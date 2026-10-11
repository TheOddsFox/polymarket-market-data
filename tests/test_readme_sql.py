import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb

from polymarket_market_data.books import UPDATE_SCHEMA, normalize_message
from polymarket_market_data.build import build
from polymarket_market_data.cli import CONTRACT
from polymarket_market_data.feeds import PRICE_SCHEMA, coverage
from polymarket_market_data.foundation import Limits, Store

NOW = datetime(2025, 1, 1, tzinfo=UTC)
CTF = {
    "venue": "polymarket",
    "market_id": "ctf-market",
    "asset_kind": "ctf_token",
    "asset_id": "123",
    "clob_token_id": "123",
    "condition_id": "0x" + "a" * 64,
    "outcome_index": 1,
}
POSITION = CTF | {
    "market_id": "position-market",
    "asset_kind": "poly_v2_position",
    "clob_token_id": None,
    "position_id": "123",
    "condition_id": "0x" + "b" * 64,
    "outcome_index": 2,
}


def test_documented_duckdb_queries_preserve_revision_identity_and_streams(tmp_path):
    store = Store(tmp_path / "root", CONTRACT)
    plan_id = store.create_plan(
        {
            "limits": Limits(30, 10**7, 10**8, 10**6, 10**7).as_dict(),
            "assets": [CTF, POSITION],
            "feeds": ["clob-history", "pmxt-v2", "clob-ws"],
        }
    )
    acquisitions = []

    def acquisition(source):
        identifier = store.new_acquisition(plan_id, source)
        acquisitions.append(identifier)
        return identifier

    def prices(target, values):
        return [
            target
            | {
                "source": "clob-history",
                "source_at": NOW + timedelta(seconds=offset),
                "received_at": NOW,
                "ingested_at": NOW,
                "price": Decimal(value),
            }
            for offset, value in enumerate(values)
        ]

    selected_acquisition = acquisition("clob-history")
    selected_capture = store.capture(
        selected_acquisition,
        "selected",
        {"price_samples": prices(CTF, ["0.40", "0.43"]) + prices(POSITION, ["0.90"])},
        schemas={"price_samples": PRICE_SCHEMA},
    )
    store.capture(
        selected_acquisition,
        "other-capture",
        {"price_samples": prices(CTF, ["0.55"])},
        schemas={"price_samples": PRICE_SCHEMA},
    )
    store.capture(
        acquisition("clob-history"),
        "other-acquisition",
        {"price_samples": prices(CTF, ["0.66"])},
        schemas={"price_samples": PRICE_SCHEMA},
    )

    def snapshot(target, source, stream, sequence, bid):
        return normalize_message(
            {
                "event_type": "book",
                "asset_id": target["asset_id"],
                "bids": [{"price": bid, "size": "2"}],
                "asks": [{"price": "0.95", "size": "3"}],
            },
            targets=[target],
            source=source,
            stream=stream,
            sequence=sequence,
            received_at=NOW + timedelta(seconds=sequence),
        )

    selected_stream = "object/selected-hour"
    archive_acquisition = acquisition("pmxt-v2")
    store.capture(
        archive_acquisition,
        "selected-book",
        {
            "book_updates": snapshot(CTF, "pmxt-v2", selected_stream, 1, "0.40")
            + snapshot(CTF, "pmxt-v2", selected_stream, 2, "0.41")
            + snapshot(POSITION, "pmxt-v2", selected_stream, 1, "0.80")
            + snapshot(CTF, "pmxt-v2", "object/other-hour", 1, "0.70")
        },
        coverage=[
            coverage(
                CTF, "pmxt-v2", NOW, NOW + timedelta(hours=1), "success", observations=2, gaps=0
            ),
            coverage(
                POSITION,
                "pmxt-v2",
                NOW,
                NOW + timedelta(hours=1),
                "success",
                observations=7,
                gaps=0,
            ),
        ],
        schemas={"book_updates": UPDATE_SCHEMA},
    )
    store.capture(
        acquisition("pmxt-v2"),
        "overlapping-receipt",
        {},
        coverage=[
            coverage(
                CTF, "pmxt-v2", NOW, NOW + timedelta(hours=1), "success", observations=3, gaps=0
            )
        ],
    )
    store.capture(
        acquisition("clob-ws"),
        "other-source",
        {"book_updates": snapshot(CTF, "clob-ws", selected_stream, 1, "0.60")},
        coverage=[
            coverage(
                CTF, "clob-ws", NOW, NOW + timedelta(hours=1), "success", observations=11, gaps=0
            )
        ],
        schemas={"book_updates": UPDATE_SCHEMA},
    )
    for identifier in acquisitions:
        store.capture(
            identifier,
            "acquisition_complete",
            {},
            evidence={
                "capture_ids": sorted(
                    capture["capture_id"]
                    for capture in store.committed_captures(acquisition_id=identifier)
                ),
                "acquisition_revision": 1,
                "schema_revision": 1,
            },
        )
    release_id = build(store, plan_id, acquisitions)

    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text()
    fences = re.findall(r"```sql\n(.*?)\n```", readme, flags=re.DOTALL)
    assert len(fences) == 1
    sql = fences[0]
    replacements = {
        "/absolute/root": str(store.root),
        "RELEASE": release_id,
        "ACQUISITION": selected_acquisition,
        "CAPTURE": selected_capture["capture_id"],
        "ASSET": CTF["asset_id"],
        "STREAM": selected_stream,
    }
    for placeholder, value in replacements.items():
        assert placeholder in sql
        sql = sql.replace(placeholder, value)
    queries = [query.strip() for query in sql.split(";") if query.strip()]
    assert len(queries) == 4
    with duckdb.connect() as connection:
        joined, ohlc, states, receipts = [
            [tuple(row.values()) for row in connection.execute(query).to_arrow_table().to_pylist()]
            for query in queries
        ]
    assert sorted(joined) == [
        ("ctf-market", 1, NOW, Decimal("0.40")),
        ("ctf-market", 1, NOW + timedelta(seconds=1), Decimal("0.43")),
        ("position-market", 2, NOW, Decimal("0.90")),
    ]
    assert ohlc == [(NOW, Decimal("0.40"), Decimal("0.43"), Decimal("0.40"), Decimal("0.43"), 2)]
    assert states == [
        (NOW + timedelta(seconds=sequence), sequence, 0, Decimal(bid), Decimal("0.95"), "valid")
        for sequence, bid in ((1, "0.40"), (2, "0.41"))
    ]
    assert sorted(receipts) == [
        ("polymarket", kind, "123", NOW, NOW + timedelta(hours=1), "success", count, 0)
        for kind, count in (("ctf_token", 2), ("ctf_token", 3), ("poly_v2_position", 7))
    ]
