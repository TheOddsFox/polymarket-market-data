"""Explicit live smoke; ordinary tests and CI never invoke this module."""

import argparse
import json
import os
import resource
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4


class SmokeFailure(ValueError):
    pass


def measure_storage(output, report, stopped):
    while not stopped.is_set():
        retained = temporary = 0
        for path in output.rglob("*"):
            try:
                if path.is_file() and not path.is_symlink():
                    size = path.stat().st_size
                    retained += size
                    if any(
                        part.startswith((".archive-", ".build-", ".metadata-", ".pending-", "tmp"))
                        for part in path.relative_to(output).parts
                    ):
                        temporary += size
            except FileNotFoundError:
                pass
        report["peak_root_bytes"] = max(report.get("peak_root_bytes", 0), retained)
        report["sampled_peak_temporary_bytes"] = max(
            report.get("sampled_peak_temporary_bytes", 0), temporary
        )
        stopped.wait(0.5)


def require(condition, message):
    if not condition:
        raise SmokeFailure(message)


def command(report, name, argv, env=None):
    step = {"name": name}
    report["steps"].append(step)
    env = env or {
        key: value for key, value in os.environ.items() if not key.startswith("CATALOGUE_")
    }
    result = subprocess.run(
        argv,
        env=env,
        capture_output=True,
        text=True,
        timeout=report.get("command_timeout_seconds", 180),
    )
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
    report["wall_seconds"] = (
        datetime.now(UTC) - datetime.fromisoformat(report.get("started_at", report["finished_at"]))
    ).total_seconds()
    scale = 1 if sys.platform == "darwin" else 1024
    report["peak_process_rss_bytes"] = scale * max(
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
    )
    report["http_attempts"] = sum(
        step.get("result", {}).get("http_attempts", step.get("result", {}).get("requests", 0))
        for step in report["steps"]
    )
    report["downloaded_bytes"] = sum(
        step.get("result", {}).get("downloaded_bytes", 0) for step in report["steps"]
    )
    report["accounting_complete"] = bool(report["steps"]) and all(
        "result" in step
        and step.get("exit_code") == 0
        and step["result"].get("accounting_complete", True)
        for step in report["steps"]
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
    asset_streams = {}
    connections = gaps = 0
    started = time.monotonic()
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
        from .books import asset_key

        for row in updates:
            if row["kind"] == "book":
                streams.add(row["stream"])
                asset_streams.setdefault(asset_key(row), set()).add(row["stream"])
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
                connections >= 2
                and gaps >= 1
                and len(streams) >= 2
                and all(
                    len(
                        asset_streams.get(
                            (row.get("venue", "polymarket"), row["asset_kind"], row["asset_id"]),
                            set(),
                        )
                    )
                    >= 2
                    for row in targets
                ),
                "forced reconnect did not receive fresh snapshots on both connections",
            )
            step["exit_code"] = 0
        finally:
            step["wall_seconds"] = time.monotonic() - started
            step["result"] = {
                "http_attempts": client.requests,
                "downloaded_bytes": client.downloaded,
                "connections": connections,
                "gaps": gaps,
                "snapshot_streams": len(streams),
            }


def run(
    output,
    source,
    duration,
    report,
    events_executable,
    *,
    case="active",
    start=None,
    end=None,
    expected_kind=None,
):
    import asyncio

    import pyarrow.parquet as pq

    from .acceptance import consumer_check, restore_check
    from .books import asset_key
    from .foundation import Store
    from .metadata import load_metadata, select_assets

    baseline = select_assets(
        load_metadata(source["metadata"], source["metadata_sha256"]), [source["market_id"]]
    )
    require(
        not expected_kind or all(row["asset_kind"] == expected_kind for row in baseline),
        "selected market does not have the required native asset kind",
    )
    cli = [sys.executable, "-m", "polymarket_market_data", "--root", str(output)]
    upper = end or datetime.now(UTC).replace(microsecond=0).isoformat()
    lower = (
        start
        or (datetime.fromisoformat(upper.replace("Z", "+00:00")) - timedelta(hours=1)).isoformat()
    )
    feeds = ["clob-history", "clob-ws"] if case == "active" else ["pmxt-v1", "pmxt-v2"]
    argv = [
        *cli,
        "run",
        "--market-id",
        source["market_id"],
        "--events-executable",
        str(events_executable),
        "--start",
        lower,
        "--end",
        upper,
        "--duration",
        str(duration),
        "--max-requests",
        "100",
        "--max-download-bytes",
        str(4 * 1024**3 if case == "archive" else 32 * 1024**2),
        "--max-storage-bytes",
        str(16 * 1024**3 if case == "archive" else 256 * 1024**2),
        "--max-temp-bytes",
        str(2 * 1024**3 if case == "archive" else 128 * 1024**2),
    ]
    for feed in feeds:
        argv.extend(["--feed", feed])
    result = command(report, "run", argv)
    plan, release = result["plan_id"], result["release_id"]
    resumed = command(report, "resume_run", [*cli, "run", "--resume-run", result["run_id"]])
    require(
        resumed.get("requests", 0) == 0 and resumed.get("downloaded_bytes", 0) == 0,
        "completed run resume repeated acquisition",
    )
    require(resumed["release_id"] == release, "completed resume changed release")
    store = Store(output, "oddsfox.polymarket.market-data.v1")
    assets = store.load_plan(plan)["assets"]
    require(
        {
            tuple(
                row.get(field)
                for field in (
                    "venue",
                    "market_id",
                    "asset_kind",
                    "asset_id",
                    "condition_id",
                    "outcome_index",
                )
            )
            for row in assets
        }
        == {
            tuple(
                row.get(field)
                for field in (
                    "venue",
                    "market_id",
                    "asset_kind",
                    "asset_id",
                    "condition_id",
                    "outcome_index",
                )
            )
            for row in baseline
        },
        "runner changed the trusted producer identities",
    )
    manifest = store.verify(release)
    acquisitions = sorted(
        {
            capture["acquisition_id"]
            for capture in store.committed_captures(plan_id=plan)
            if capture["capture_id"] in manifest["inputs"]
        }
    )
    build_args = [*cli, "build", "--plan", plan]
    for acquisition in acquisitions:
        build_args.extend(["--acquisition", acquisition])
    require(command(report, "replay", build_args)["release_id"] == release, "semantic replay drift")
    directory = output / "published" / release
    samples = pq.read_table(directory / "price_samples.parquet").to_pylist()
    updates = pq.read_table(directory / "book_updates.parquet").to_pylist()
    states = pq.read_table(directory / "book_states.parquet").to_pylist()
    keys = {asset_key(row) for row in assets}
    if case == "active":
        require(
            {asset_key(row) for row in samples} == keys,
            "history missing samples for a selected asset",
        )
        require(
            {asset_key(row) for row in updates if row["kind"] == "book"} == keys,
            "missing authoritative live snapshot for a selected asset",
        )
        require(
            {asset_key(row) for row in states if row["status"] == "valid"} == keys,
            "missing valid displayed live book state for a selected asset",
        )
        asyncio.run(reconnect_check(assets, report))
    else:
        for feed in feeds:
            require(
                {
                    asset_key(row)
                    for row in updates
                    if row["source"] == feed and row["kind"] != "reset"
                }
                == keys,
                feed + " missing selected archive observations",
            )
            require(
                {
                    asset_key(row)
                    for row in states
                    if row["source"] == feed and row["status"] == "valid"
                }
                == keys,
                feed + " missing reconstructed valid book state for a selected asset",
            )
    consumer = consumer_check(store, release)
    restored = restore_check(store, output.with_name(output.name + "-restored"))
    restored_store = Store(output.with_name(output.name + "-restored"), store.contract)
    require(consumer_check(restored_store, release) == consumer, "restored consumer query drift")
    restored_args = [*cli[:-1], str(restored_store.root), *build_args[len(cli) :]]
    require(
        command(report, "restored_replay", restored_args)["release_id"] == release,
        "restored semantic replay drift",
    )
    report.update(
        plan_id=plan,
        release_id=release,
        run_id=result["run_id"],
        case=case,
        asset_kinds=sorted({row["asset_kind"] for row in assets}),
        price_samples=len(samples),
        valid_book_states=sum(row["status"] == "valid" for row in states),
        coverage=json.loads((directory / "coverage.json").read_text()),
        consumer=consumer,
        restore=restored,
        captures=len(store.committed_captures()),
        checks=[
            "installed_run",
            "completed_resume",
            "offline_replay",
            "publish",
            "verify",
            "duckdb_consumer",
            "whole_root_restore",
            "restored_semantic_replay",
        ]
        + (["forced_websocket_reconnect"] if case == "active" else ["archive_v1", "archive_v2"]),
        skipped=["paid_feeds"] + (["free_archive"] if case == "active" else ["live_sources"]),
    )


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
    parser.add_argument("--case", choices=("active", "archive"), default="active")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--expected-asset-kind", choices=("ctf_token", "poly_v2_position"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    require(1 <= args.duration <= 60, "duration must be 1..60 seconds")
    require(
        args.case != "archive" or args.start and args.end,
        "archive smoke requires explicit --start and --end",
    )
    output = new_output(args.output)
    Store(output, "oddsfox.polymarket.market-data.v1")
    report = {
        "product": "polymarket-market-data",
        "steps": [],
        "started_at": datetime.now(UTC).isoformat(),
        "command_timeout_seconds": 3600 if args.case == "archive" else 180,
        "limits": {
            "run_attempts": 100,
            "run_download_bytes": 4 * 1024**3 if args.case == "archive" else 32 * 1024**2,
            "retained_bytes": 16 * 1024**3 if args.case == "archive" else 256 * 1024**2,
            "temporary_bytes": 2 * 1024**3 if args.case == "archive" else 128 * 1024**2,
            "duration_seconds": args.duration,
            "reconnect_probe_seconds": 8,
            "reconnect_probe_attempts": 5,
        },
        "measurement": {
            "rss": "OS high-water maximum for one parent or child process; not process-tree sum",
            "temporary_storage": "sampled every 0.5 seconds; transient peaks may be higher",
        },
    }
    stopped = threading.Event()
    sampling = threading.Thread(target=measure_storage, args=(output, report, stopped), daemon=True)
    sampling.start()
    try:
        source = load_report(args.events_report)
        run(
            output,
            source,
            args.duration,
            report,
            args.events_executable,
            case=args.case,
            start=args.start,
            end=args.end,
            expected_kind=args.expected_asset_kind,
        )
    except (ValueError, OSError, subprocess.TimeoutExpired, KeyError) as error:
        stopped.set()
        sampling.join()
        return finish(output, report, error)
    stopped.set()
    sampling.join()
    return finish(output, report)


if __name__ == "__main__":
    raise SystemExit(main())
