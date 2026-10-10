"""Explicit live smoke; ordinary tests and CI never invoke this module."""

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4


class SmokeFailure(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise SmokeFailure(message)


def command(report, name, argv, env=None):
    step = {"name": name}
    report["steps"].append(step)
    env = env or {
        key: value for key, value in os.environ.items() if not key.startswith("CATALOGUE_")
    }
    result = subprocess.run(argv, env=env, capture_output=True, text=True, timeout=180)
    step["exit_code"] = result.returncode
    try:
        value = json.loads(result.stdout)
    except ValueError:
        try:
            value = json.loads(result.stderr.strip().splitlines()[-1])
        except (ValueError, IndexError):
            raise SmokeFailure(name + ": missing JSON diagnostics") from None
    require(isinstance(value, dict), name + ": invalid JSON diagnostics")
    for key in ("http_attempts", "requests", "downloaded_bytes"):
        require(
            key not in value or (type(value[key]) is int and value[key] >= 0),
            name + ": invalid accounting diagnostics",
        )
    require(
        "message" not in value or isinstance(value["message"], str),
        name + ": invalid error diagnostics",
    )
    step["result"] = value
    require(result.returncode == 0, name + ": " + value.get("message", "command failed"))
    return value


def finish(output, report, error=None):
    report["status"] = "failed" if error else "passed"
    if error:
        report["error"] = str(error) if isinstance(error, SmokeFailure) else type(error).__name__
    report["finished_at"] = datetime.now(UTC).isoformat()
    report["http_attempts"] = sum(
        step.get("result", {}).get("http_attempts", step.get("result", {}).get("requests", 0))
        for step in report["steps"]
    )
    report["downloaded_bytes"] = sum(
        step.get("result", {}).get("downloaded_bytes", 0) for step in report["steps"]
    )
    report["accounting_complete"] = bool(report["steps"]) and all(
        "result" in step and step.get("exit_code") == 0 for step in report["steps"]
    )
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                "status": report["status"],
                "report": str(output / "report.json"),
                "http_attempts": report["http_attempts"],
                "downloaded_bytes": report["downloaded_bytes"],
                "error": report.get("error"),
            },
            indent=2,
        )
    )
    return 1 if error else 0


def new_output(value):
    output = (value or Path("data/smoke") / uuid4().hex).absolute()
    require(not output.exists(), "output must be a fresh directory")
    require(not any(path.is_symlink() for path in (output, *output.parents)), "symlink output")
    return output


async def reconnect_check(targets, report):
    import asyncio
    from contextlib import asynccontextmanager

    from .foundation import Limits
    from .recorder import NoRedirectConnect, record
    from .transport import Client

    streams = set()
    connections = gaps = 0
    step = {"name": "forced_reconnect"}
    report["steps"].append(step)

    @asynccontextmanager
    async def connector(*args, **kwargs):
        nonlocal connections
        async with NoRedirectConnect(*args, **kwargs) as ws:
            connections += 1

            async def close_first():
                await asyncio.sleep(2)
                await ws.close()

            task = asyncio.create_task(close_first()) if connections == 1 else None
            try:
                yield ws
            finally:
                if task:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    async def emit(updates, evidence, coverage):
        nonlocal gaps
        streams.update(row["stream"] for row in updates if row["kind"] == "book")
        gaps += evidence.get("kind") == "connection_gap"

    with Client("clob", Limits(5, 8388608, 67108864, 1048576, 67108864)) as client:
        try:
            await record(
                targets=targets,
                duration=8,
                emit=emit,
                charge_request=client.reserve_request,
                charge_download=client.count_bytes,
                connector=connector,
                max_message_bytes=1048576,
            )
            require(
                connections >= 2 and gaps >= 1 and len(streams) >= 2,
                "forced reconnect did not receive fresh snapshots on both connections",
            )
            step["exit_code"] = 0
        finally:
            step["result"] = {
                "http_attempts": client.requests,
                "downloaded_bytes": client.downloaded,
                "connections": connections,
                "gaps": gaps,
                "snapshot_streams": len(streams),
            }


def run(output, source, duration, report, events_executable):
    import asyncio

    import pyarrow.parquet as pq

    from .foundation import Store

    require(source["status"] == "passed", "events smoke did not pass")
    cli = [sys.executable, "-m", "polymarket_market_data", "--root", str(output)]
    end = datetime.now(UTC).replace(microsecond=0)
    plan = command(
        report,
        "plan",
        [
            *cli,
            "plan",
            "--metadata",
            source["metadata"],
            "--metadata-sha256",
            source["metadata_sha256"],
            "--market-id",
            source["market_id"],
            "--feed",
            "clob-history",
            "--feed",
            "clob-ws",
            "--start",
            (end - timedelta(hours=1)).isoformat(),
            "--end",
            end.isoformat(),
            "--duration",
            str(duration),
            "--max-requests",
            "10",
            "--max-download-bytes",
            "8388608",
            "--max-storage-bytes",
            "67108864",
            "--max-response-bytes",
            "1048576",
            "--max-temp-bytes",
            "67108864",
        ],
    )["plan_id"]
    prices = command(report, "prices", [*cli, "prices", "backfill", "--plan", plan])
    command(
        report,
        "prices_resume",
        [*cli, "prices", "backfill", "--plan", plan, "--resume", prices["acquisition_id"]],
    )
    books = command(
        report,
        "books",
        [
            *cli,
            "books",
            "record",
            "--plan",
            plan,
            "--events-executable",
            str(events_executable),
            "--events-root",
            str(output / ".state" / "events"),
        ],
    )
    build_args = [
        *cli,
        "build",
        "--plan",
        plan,
        "--acquisition",
        prices["acquisition_id"],
        "--acquisition",
        books["acquisition_id"],
    ]
    release = command(report, "build", build_args)["release_id"]
    require(command(report, "replay", build_args)["release_id"] == release, "semantic replay drift")
    command(report, "publish", [*cli, "publish", "--release", release])
    command(report, "verify", [*cli, "verify"])
    directory = output / "published" / release
    samples = pq.read_table(directory / "price_samples.parquet").num_rows
    states = pq.read_table(directory / "book_states.parquet").to_pylist()
    valid = sum(row["status"] == "valid" for row in states)
    require(samples > 0, "history returned no samples; acquisition alone is not a smoke pass")
    require(valid > 0, "no authoritative live book snapshot observed")
    report.update(
        plan_id=plan,
        release_id=release,
        price_samples=samples,
        valid_book_states=valid,
        coverage=json.loads((directory / "coverage.json").read_text()),
        checks=["live_history", "live_books", "resume", "offline_replay", "publish", "verify"],
        skipped=["paid_feeds", "free_archive"],
    )
    store = Store(output, "oddsfox.polymarket.market-data.v1")
    store.verify()
    asyncio.run(reconnect_check(store.load_plan(plan)["assets"], report))
    report["checks"].append("forced_websocket_reconnect")


def load_report(path):
    require(path.is_file() and not path.is_symlink(), "events report must be a regular file")
    with path.open("rb") as handle:
        raw = handle.read(1048577)
    require(len(raw) <= 1048576, "events report exceeds 1 MiB")
    source = json.loads(raw)
    require(isinstance(source, dict), "invalid events report")
    require(source.get("product") == "polymarket-events", "wrong producer report")
    require(
        source.get("status") == "passed" and source.get("accounting_complete") is True,
        "events smoke did not pass with complete accounting",
    )
    for key in ("metadata", "metadata_sha256", "market_id"):
        require(isinstance(source.get(key), str) and bool(source[key]), "invalid producer identity")
    return source


def main(argv=None):
    from .foundation import Store

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events-report", required=True, type=Path)
    parser.add_argument("--events-executable", required=True, type=Path)
    parser.add_argument("--duration", type=int, default=15)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    require(1 <= args.duration <= 60, "duration must be 1..60 seconds")
    output = new_output(args.output)
    Store(output, "oddsfox.polymarket.market-data.v1")
    report = {
        "product": "polymarket-market-data",
        "steps": [],
        "started_at": datetime.now(UTC).isoformat(),
        "limits": {
            "attempts_per_command": 10,
            "download_bytes_per_command": 8388608,
            "retained_bytes": 67108864,
            "duration_seconds": args.duration,
            "reconnect_probe_seconds": 8,
            "reconnect_probe_attempts": 5,
        },
    }
    try:
        source = load_report(args.events_report)
        run(output, source, args.duration, report, args.events_executable)
    except (ValueError, OSError, subprocess.TimeoutExpired, KeyError) as error:
        return finish(output, report, error)
    return finish(output, report)


if __name__ == "__main__":
    raise SystemExit(main())
