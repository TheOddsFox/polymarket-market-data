"""Deterministic source-specific projections from named committed acquisitions."""

from __future__ import annotations

from collections import defaultdict
from contextlib import closing
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pyarrow as pa

from .books import IDENTITY_FIELDS, STATE_SCHEMA, UPDATE_SCHEMA, Book, asset_key
from .feeds import COVERAGE_SCHEMA, OHLC_SCHEMA, PRICE_SCHEMA, TRADE_SCHEMA, utc
from .foundation import BudgetExceeded, Limits, json_bytes


def _ordered(store, captures, relation, order):
    capture_by_path = {
        str(store.capture_path(capture) / (relation + ".parquet")): capture
        for capture in captures
        if relation in capture["tables"] and capture["tables"][relation]["rows"]
    }
    paths = list(capture_by_path)
    if not paths:
        return
    limits = store.limits
    with TemporaryDirectory(prefix=".build-", dir=store.root / ".state") as temporary:
        allowance = min(limits.max_temp_bytes // 4, store.available_bytes())
        if allowance <= 0:
            raise BudgetExceeded("build spill requires retained headroom")
        try:
            with store.external_write([Path(temporary)], allowance), duckdb.connect() as connection:
                connection.execute("SET threads=1")
                connection.execute("SET memory_limit=?", [f"{limits.max_temp_bytes // 2}B"])
                connection.execute("SET temp_directory=?", [temporary])
                connection.execute("SET max_temp_directory_size=?", [f"{allowance}B"])
                connection.read_parquet(paths, union_by_name=True, filename=True).create_view(
                    "source_rows"
                )
                for batch in connection.execute(
                    "SELECT * FROM source_rows ORDER BY " + order + ", filename"
                ).to_arrow_reader(batch_size=1024):
                    for row in batch.to_pylist():
                        capture = capture_by_path[row.pop("filename")]
                        if relation == "price_samples":
                            row["acquisition_id"] = capture["acquisition_id"]
                            row["capture_id"] = capture["capture_id"]
                        yield row
        finally:
            store.remove(Path(temporary))


def build(store, plan_id, acquisition_ids):
    plan = store.load_plan(plan_id)
    store.limits = Limits.from_dict(plan["limits"])
    if not acquisition_ids or len(set(acquisition_ids)) != len(acquisition_ids):
        raise ValueError("build requires distinct explicit acquisition IDs")
    captures, covered = [], []
    for acquisition_id in sorted(acquisition_ids):
        acquisition = store.acquisition(acquisition_id)
        if acquisition["plan_id"] != plan_id:
            raise ValueError("acquisition belongs to another plan")
        selected = store.committed_captures(acquisition_id=acquisition_id)
        terminal = [capture for capture in selected if capture["unit_id"] == "acquisition_complete"]
        if len(terminal) != 1:
            raise ValueError("unfinished acquisition cannot build")
        completion = store.read_evidence(terminal[0])
        if completion.get("acquisition_revision") != 1 or completion.get("schema_revision") != 1:
            raise ValueError("unsupported acquisition/schema completion revision")
        expected = completion["capture_ids"]
        actual = sorted(capture["capture_id"] for capture in selected if capture not in terminal)
        if expected != actual:
            raise ValueError("acquisition inventory differs from completion evidence")
        captures.extend(selected)
        covered.extend(item for capture in selected for item in capture["coverage"])
    sources = {store.acquisition(value)["source"] for value in acquisition_ids}
    if not set(plan["feeds"]).issubset(sources):
        raise ValueError("build requires terminal evidence for every planned feed")
    relations = {
        "price_samples": [],
        "book_updates": [],
        "book_states": [],
        "trades": [],
        "sampled_ohlc": [],
    }
    consumed = 0

    def append(name, row):
        nonlocal consumed
        consumed += len(json_bytes(row)) * 4
        if consumed > store.limits.max_temp_bytes // 4:
            raise BudgetExceeded("derived rows exceed bounded build memory; use smaller plans")
        relations[name].append(row)

    groups = defaultdict(list)
    seen = {}
    with closing(
        _ordered(
            store, captures, "price_samples", "source, asset_kind, asset_id, source_at, received_at"
        )
    ) as ordered:
        for row in ordered:
            key = (
                row["source"],
                row["acquisition_id"],
                row["capture_id"],
                *asset_key(row),
                row["source_at"],
            )
            if key in seen:
                if seen[key]["price"] != row["price"]:
                    raise ValueError("conflicting prices inside one capture identity")
                if any(seen[key][name] != row[name] for name, _ in IDENTITY_FIELDS):
                    raise ValueError("conflicting sample identity inside one capture bucket")
                continue
            seen[key] = row
            append("price_samples", row)
            bucket = utc(row["source_at"]).replace(second=0, microsecond=0)
            groups[
                (row["source"], row["acquisition_id"], row["capture_id"], *asset_key(row), bucket)
            ].append(row)
    for (source, acquisition_id, capture_id, venue, asset_kind, asset_id, bucket), rows in sorted(
        groups.items()
    ):
        rows.sort(key=lambda row: row["source_at"])
        identity = {name: rows[0][name] for name, _ in IDENTITY_FIELDS}
        if any(any(row[name] != value for name, value in identity.items()) for row in rows):
            raise ValueError("conflicting sample identity inside one capture bucket")
        prices = [Decimal(row["price"]) for row in rows]
        append(
            "sampled_ohlc",
            {
                "source": source,
                "acquisition_id": acquisition_id,
                "capture_id": capture_id,
                **identity,
                "bucket_start": bucket,
                "bucket_end": bucket + timedelta(minutes=1),
                "open": prices[0],
                "high": max(prices),
                "low": min(prices),
                "close": prices[-1],
                "observed_samples": len(rows),
                "continuous_coverage": False,
            },
        )
    books, previous = {}, {}
    with closing(
        _ordered(
            store,
            captures,
            "book_updates",
            "source, asset_kind, asset_id, stream, sequence, ordinal",
        )
    ) as ordered:
        for row in ordered:
            key = row["source"], *asset_key(row), row["stream"]
            position = row["sequence"], row["ordinal"]
            duplicate = previous.get(key)
            if duplicate and duplicate[:2] == position:
                if {
                    name: value
                    for name, value in duplicate[2].items()
                    if name not in ("ingested_at", "acquisition_id", "capture_id")
                } != {
                    name: value
                    for name, value in row.items()
                    if name not in ("ingested_at", "acquisition_id", "capture_id")
                }:
                    raise ValueError("conflicting book receive position")
                continue
            previous[key] = (*position, row)
            # Parquet decimal types preserve exact values; metadata remains typed.
            if row.get("price") is not None:
                row["price"] = Decimal(row["price"])
            if row.get("quantity") is not None:
                row["quantity"] = Decimal(row["quantity"])
            for side in ("bids", "asks"):
                if row.get(side) is not None:
                    row[side] = [
                        {"price": Decimal(level["price"]), "quantity": Decimal(level["quantity"])}
                        for level in row[side]
                    ]
            append("book_updates", row)
            append("book_states", books.setdefault(key, Book()).apply(row))
    seen = {}
    with closing(
        _ordered(store, captures, "trades", "source, asset_kind, asset_id, source_at, trade_id")
    ) as ordered:
        for row in ordered:
            key = row["source"], *asset_key(row), row["trade_id"]
            semantic = {
                name: value
                for name, value in row.items()
                if name not in ("received_at", "ingested_at")
            }
            if key in seen:
                if seen[key] != semantic:
                    raise ValueError("conflicting trade identity in selected captures")
                continue
            seen[key] = semantic
            append("trades", row)
    relations["coverage"] = [
        row | {"start": utc(row["start"]), "end": utc(row["end"])} for row in covered
    ]
    relations["instruments"] = plan["assets"]
    schemas = {
        "price_samples": PRICE_SCHEMA,
        "book_updates": UPDATE_SCHEMA,
        "book_states": STATE_SCHEMA,
        "trades": TRADE_SCHEMA,
        "sampled_ohlc": OHLC_SCHEMA,
        "coverage": COVERAGE_SCHEMA,
        "instruments": pa.schema(
            [
                *IDENTITY_FIELDS,
                ("clob_token_id", pa.string()),
                ("position_id", pa.string()),
                ("outcome_label", pa.string()),
            ]
        ),
    }
    return store.build_release(
        plan_id,
        captures,
        relations,
        covered,
        schemas=schemas,
        versions={"schema": 1, "normalizer": 1, "acquisition": 1},
    )
