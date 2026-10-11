"""Standalone fixture copied beside a fresh installed environment; no checkout imports."""

if not __debug__:
    raise SystemExit("installed acceptance requires Python assertions; disable optimization")

import contextlib
import io
import json
import shutil
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_market_data import acquisitions, cli
from polymarket_market_data.archives import V1_SCHEMA, V2_SCHEMA
from polymarket_market_data.books import normalize_message, resets
from polymarket_market_data.feeds import coverage
from polymarket_market_data.foundation import atomic_write, digest, json_bytes
from polymarket_market_data.transport import Client

NOW = datetime(2025, 1, 1, tzinfo=UTC)
TARGETS = [
    {
        "venue": "polymarket",
        "market_id": str(market),
        "asset_kind": kind,
        "asset_id": str(native),
        "clob_token_id": str(native) if kind == "ctf_token" else None,
        "position_id": str(native) if kind == "poly_v2_position" else None,
        "condition_id": "0x" + character * 64,
        "outcome_index": ordinal,
        "usable": True,
    }
    for market, kind, character, natives in (
        (1, "ctf_token", "a", (123, 124)),
        (2, "poly_v2_position", "b", (223, 224)),
    )
    for ordinal, native in enumerate(natives, 1)
]


def producer():
    output = Path(sys.argv[sys.argv.index("--output") + 1])
    output.mkdir()
    files = {}
    relations = {
        "markets.json": [
            {"venue": "polymarket", "market_id": str(market), "usable": True} for market in (1, 2)
        ],
        "outcomes.json": TARGETS,
        "memberships.json": [],
        "identity_history.json": [],
        "coverage.json": [{"market_id": str(market), "status": "found"} for market in (1, 2)],
    }
    for name, rows in relations.items():
        raw = json_bytes(rows)
        atomic_write(output / name, raw)
        files[name] = {"sha256": digest(raw), "size": len(raw)}
    raw = json_bytes({"contract": "oddsfox.polymarket.metadata.v1", "files": files})
    atomic_write(output / "manifest.json", raw)
    print(json.dumps({"manifest_sha256": digest(raw), "http_attempts": 2, "downloaded_bytes": 256}))


def command(root, *arguments):
    with contextlib.redirect_stdout(io.StringIO()) as output:
        status = cli.main(["--root", str(root), *arguments])
    response = json.loads(output.getvalue())
    assert status == 0, response
    return response


def workflow(environment_path):
    assert Path(cli.__file__).resolve().is_relative_to(environment_path.resolve())
    work = Path.cwd()
    assert not any((parent / ".git").exists() for parent in (work, *work.parents))
    root = work / "root"
    requests = []
    archives = {}
    for version, schema in (("v1", V1_SCHEMA), ("v2", V2_SCHEMA)):
        rows = []
        for target in TARGETS[:2]:
            if version == "v1":
                rows.append(
                    {
                        "timestamp_received": NOW,
                        "timestamp_created_at": NOW,
                        "market_id": target["condition_id"],
                        "update_type": "book_snapshot",
                        "data": json.dumps(
                            {
                                "token_id": target["asset_id"],
                                "bids": [],
                                "asks": [],
                            }
                        ),
                    }
                )
            else:
                rows.append(
                    {
                        "timestamp_received": NOW,
                        "timestamp": NOW,
                        "market": target["condition_id"].encode(),
                        "event_type": "book",
                        "asset_id": target["asset_id"],
                        "bids": "[]",
                        "asks": "[]",
                    }
                )
        path = work / (version + ".parquet")
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
        archives[version] = path.read_bytes()

    def respond(request):
        requests.append(request)
        if request.url.host == "clob.polymarket.com":
            assert request.url.params["market"] in {target["asset_id"] for target in TARGETS}
            return httpx.Response(200, json={"history": [{"t": int(NOW.timestamp()), "p": "0.37"}]})
        assert request.url.host in {"r2.pmxt.dev", "r2v2.pmxt.dev"}
        return httpx.Response(
            200, content=archives["v1" if request.url.host == "r2.pmxt.dev" else "v2"]
        )

    def client(*args, **kwargs):
        return Client(*args, **kwargs, transport=httpx.MockTransport(respond), sleep=lambda _: None)

    async def recording(**kwargs):
        for connection in (1, 2):
            kwargs["charge_request"]()
            stream = f"fixture/session-{connection}"
            await kwargs["emit"](
                resets(
                    TARGETS,
                    source="clob-ws",
                    stream=stream,
                    sequence=0,
                    received_at=NOW,
                    reason="session_start",
                ),
                {"kind": "connection_start", "connection": connection},
                [],
            )
            for ordinal, target in enumerate(TARGETS, 1):
                payload = {
                    "event_type": "book",
                    "asset_id": target["asset_id"],
                    "bids": [{"price": "0.36", "size": "2"}],
                    "asks": [{"price": "0.38", "size": "3"}],
                    "timestamp": str(int(NOW.timestamp() * 1000)),
                }
                kwargs["charge_download"](len(json_bytes(payload)))
                rows = normalize_message(
                    payload,
                    targets=TARGETS,
                    source="clob-ws",
                    stream=stream,
                    sequence=ordinal,
                    received_at=NOW,
                )
                await kwargs["emit"](rows, {"kind": "book"}, [])
        await kwargs["emit"](
            [],
            {"kind": "session_end", "connections": 2, "gaps": 1},
            [
                coverage(target, "clob-ws", NOW, NOW + timedelta(seconds=60), "success", gaps=1)
                for target in TARGETS
            ],
        )

    acquisitions.Client, acquisitions.record_messages = client, recording
    result = command(
        root,
        "run",
        "--market-id",
        "1",
        "--market-id",
        "2",
        "--feed",
        "clob-history",
        "--feed",
        "clob-ws",
        "--feed",
        "pmxt-v1",
        "--feed",
        "pmxt-v2",
        "--start",
        NOW.isoformat(),
        "--end",
        (NOW + timedelta(hours=1)).isoformat(),
        "--duration",
        "60",
        "--events-executable",
        str(Path(__file__).resolve()),
    )
    assert result["stage"] == "complete" and result["accounting_complete"]
    assert result["measured_usage"] == result["charged_usage"]
    assert set(result["sources"]) == {"clob-history", "clob-ws", "pmxt-v1", "pmxt-v2"}
    assert all(source["nonempty_observations"] for source in result["sources"].values())
    call_count = len(requests)
    resumed = command(root, "run", "--resume-run", result["run_id"])
    assert resumed["requests"] == resumed["downloaded_bytes"] == 0
    assert len(requests) == call_count == 6
    assert (
        command(
            root,
            "build",
            "--plan",
            result["plan_id"],
            *[
                item
                for identifier in result["acquisition_ids"]
                for item in ("--acquisition", identifier)
            ],
        )["release_id"]
        == result["release_id"]
    )
    release = root / "published" / result["release_id"]
    with duckdb.connect() as connection:
        for name in ("instruments", "price_samples", "sampled_ohlc", "book_states", "coverage"):
            connection.read_parquet(str(release / (name + ".parquet"))).create_view(name)
        joined = connection.execute("""
            SELECT count(*) FROM sampled_ohlc AS samples
            JOIN instruments AS assets USING (venue, asset_kind, asset_id)
            WHERE samples.market_id=assets.market_id
              AND samples.condition_id=assets.condition_id
              AND samples.outcome_index=assets.outcome_index
        """).fetchone()[0]
        assert joined == 4
        assert (
            connection.execute("SELECT count(*) FROM price_samples WHERE price=0.37").fetchone()[0]
            == 4
        )
        assert (
            connection.execute(
                "SELECT count(DISTINCT stream) FROM book_states WHERE source='clob-ws' AND status='valid'"
            ).fetchone()[0]
            == 2
        )
        assert (
            connection.execute(
                "SELECT count(*) FROM coverage WHERE asset_kind='poly_v2_position' "
                "AND source IN ('pmxt-v1','pmxt-v2') AND status='capability_unknown'"
            ).fetchone()[0]
            == 4
        )
    restored = work / "restored"
    shutil.copytree(root, restored)
    restored_result = command(restored, "run", "--resume-run", result["run_id"])
    assert restored_result["release_id"] == result["release_id"]
    assert Path(restored_result["run_path"]).is_relative_to(restored)
    assert restored_result["requests"] == restored_result["downloaded_bytes"] == 0
    assert command(restored, "verify")["releases"] == [result["release_id"]]
    assert (
        command(
            restored,
            "build",
            "--plan",
            result["plan_id"],
            *[
                item
                for identifier in reversed(result["acquisition_ids"])
                for item in ("--acquisition", identifier)
            ],
        )["release_id"]
        == result["release_id"]
    )
    subprocess.run(
        [
            str(environment_path / "bin" / "polymarket-market-data"),
            "--root",
            str(restored),
            "status",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    print(json.dumps({"markets": 2, "typed_assets": 4, "sources": 4, "restore": "passed"}))


if __name__ == "__main__":
    if sys.argv[1:3] == ["metadata", "refresh"]:
        producer()
    else:
        workflow(Path(sys.argv[1]))
