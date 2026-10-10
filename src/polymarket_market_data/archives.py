"""Bounded free PMXT hourly archive parsing (CC BY 4.0 source data)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .books import normalize_message, parse_json, resets
from .feeds import CAPABILITIES, coverage, window
from .foundation import BudgetExceeded, json_bytes

ATTRIBUTION = {
    "creator": "PMXT",
    "source": "https://archive.pmxt.dev",
    "license": "CC BY 4.0",
    "license_url": "https://creativecommons.org/licenses/by/4.0/",
    "changes": "Selected native assets and normalized displayed-book replay",
}

V1_SCHEMA = pa.schema(
    [
        pa.field("timestamp_received", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("timestamp_created_at", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("market_id", pa.string(), nullable=False),
        pa.field("update_type", pa.string(), nullable=False),
        pa.field("data", pa.string(), nullable=False),
    ]
)
V2_SCHEMA = pa.schema(
    [
        pa.field("timestamp_received", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("timestamp", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("market", pa.binary(66), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("asset_id", pa.string(), nullable=False),
        ("bids", pa.string()),
        ("asks", pa.string()),
        ("price", pa.decimal128(9, 4)),
        ("size", pa.decimal128(18, 6)),
        ("side", pa.string()),
        ("best_bid", pa.decimal128(9, 4)),
        ("best_ask", pa.decimal128(9, 4)),
        ("fee_rate_bps", pa.uint16()),
        ("transaction_hash", pa.string()),
        ("old_tick_size", pa.decimal128(9, 4)),
        ("new_tick_size", pa.decimal128(9, 4)),
    ]
)


def archive_hours(start, end, version):
    start, end = window(start, end)
    if version not in ("v1", "v2"):
        raise ValueError("archive version must be v1 or v2")
    hour = start.replace(minute=0, second=0, microsecond=0)
    host = "r2.pmxt.dev" if version == "v1" else "r2v2.pmxt.dev"
    while hour < end:
        yield {
            "version": version,
            "hour": hour,
            "url": f"https://{host}/polymarket_orderbook_{hour:%Y-%m-%dT%H}.parquet",
        }
        hour += timedelta(hours=1)


def archive_message(row, version):
    if version == "v1":
        data = parse_json(row["data"])
        if (
            not isinstance(data, dict)
            or data.get("market_id", row["market_id"]) != row["market_id"]
        ):
            raise ValueError("archive condition envelope conflicts")
        kind = data.get("update_type", row["update_type"])
        message = {
            "event_type": "book" if kind == "book_snapshot" else kind,
            "asset_id": data.get("token_id"),
            "market": row["market_id"],
            "timestamp": (row["timestamp_created_at"] - datetime(1970, 1, 1, tzinfo=UTC))
            // timedelta(milliseconds=1),
        }
        if message["event_type"] == "book":
            message.update(bids=data.get("bids"), asks=data.get("asks"))
        elif message["event_type"] == "price_change":
            message["price_changes"] = [
                {
                    "asset_id": data.get("token_id"),
                    "price": data.get("change_price"),
                    "size": data.get("change_size"),
                    "side": data.get("change_side"),
                }
            ]
        elif message["event_type"] == "tick_size_change":
            message["new_tick_size"] = data.get("new_tick_size")
        return message
    message = {
        "event_type": row["event_type"],
        "asset_id": row["asset_id"],
        "market": row["market"].decode("ascii"),
        "timestamp": (row["timestamp"] - datetime(1970, 1, 1, tzinfo=UTC))
        // timedelta(milliseconds=1),
    }
    if row["event_type"] == "book":
        message.update(bids=parse_json(row["bids"]), asks=parse_json(row["asks"]))
    elif row["event_type"] == "price_change":
        message["price_changes"] = [
            {
                "asset_id": row["asset_id"],
                "price": row["price"],
                "size": row["size"],
                "side": row["side"],
            }
        ]
    elif row["event_type"] == "tick_size_change":
        message["new_tick_size"] = row["new_tick_size"]
    return message


def iter_archive(
    path: Path,
    *,
    version,
    targets,
    start,
    end,
    hour,
    object_sha256,
    batch_size=2048,
    max_decoded_bytes=512 * 1024**2,
    max_row_bytes=16 * 1024**2,
):
    """Yield selected updates in original file order, never timestamp-sort deltas."""
    start, end = window(start, end)
    source = f"pmxt-{version}"
    supported = [
        target
        for target in targets
        if CAPABILITIES[source].get(target["asset_kind"]) == "supported"
    ]
    conditions = {target.get("condition_id") for target in supported}
    parquet = pq.ParquetFile(path)
    schema = V1_SCHEMA if version == "v1" else V2_SCHEMA
    if not parquet.schema_arrow.equals(schema, check_metadata=False):
        raise ValueError(f"unsupported PMXT {version} archive schema")
    if not all(
        isinstance(value, int) and not isinstance(value, bool) and value > 0
        for value in (max_decoded_bytes, max_row_bytes, batch_size)
    ):
        raise ValueError("archive decode limits must be positive integers")
    metadata = parquet.metadata
    group_sizes = [
        sum(
            metadata.row_group(group).column(column).total_uncompressed_size
            for column in range(metadata.num_columns)
        )
        for group in range(metadata.num_row_groups)
    ]
    uncompressed = sum(group_sizes)
    if (
        any(size < 0 for size in group_sizes)
        or uncompressed > max_decoded_bytes
        or metadata.num_rows > max_decoded_bytes // 128
    ):
        raise BudgetExceeded("archive uncompressed footer exceeds decoded-byte/row budget")
    maximum_group = max(group_sizes, default=0)
    if maximum_group * 4 > max_decoded_bytes or metadata.num_rows and not maximum_group:
        raise BudgetExceeded("archive row-group decode exceeds temporary budget")
    row_limit = min(max_row_bytes, max_decoded_bytes // 16)
    # Dictionary entries can expand once per row; bound that expansion before Arrow allocates.
    batch_size = min(
        batch_size,
        max(1, max_decoded_bytes // 4096),
        max_decoded_bytes // (4 * max(1, maximum_group)),
    )
    stream = f"{source}/{hour.isoformat()}/{object_sha256}"
    offset = 0
    for batch in parquet.iter_batches(batch_size=batch_size):
        if batch.nbytes > max_decoded_bytes // 4:
            raise BudgetExceeded("archive decoded batch exceeds temporary budget")
        if any(batch.column(field.name).null_count for field in schema if not field.nullable):
            raise ValueError("archive has null required fields")
        updates = []
        for index, row in enumerate(batch.to_pylist()):
            stamp = row["timestamp_received"]
            condition = row["market_id"] if version == "v1" else row["market"].decode("ascii")
            if condition not in conditions or not start <= stamp < end:
                continue
            for field in ("data",) if version == "v1" else ("bids", "asks"):
                if row.get(field) is not None and len(row[field].encode()) > row_limit:
                    raise BudgetExceeded("archive source row exceeds decode budget")
            message = archive_message(row, version)
            relevant = [target for target in supported if target.get("condition_id") == condition]
            native_id = str(message.get("asset_id", ""))
            if native_id not in {target["asset_id"] for target in relevant} and message[
                "event_type"
            ] in ("book", "price_change", "tick_size_change"):
                selected = resets(
                    relevant,
                    source=source,
                    stream=stream,
                    sequence=offset + index,
                    received_at=stamp,
                    reason="unknown_archive_identity",
                )
            else:
                selected = normalize_message(
                    message,
                    targets=relevant,
                    source=source,
                    stream=stream,
                    sequence=offset + index,
                    received_at=stamp,
                )
            for update in selected:
                update["raw_identity"] = f"{object_sha256}:{offset + index}:{update['ordinal']}"
                update["ingested_at"] = datetime.now(UTC)
            updates.extend(selected)
        if sum(len(json_bytes(update)) * 4 for update in updates) > max_decoded_bytes // 4:
            raise BudgetExceeded("archive normalized batch exceeds temporary budget")
        yield updates, {"first_row": offset, "source_rows": batch.num_rows}
        offset += batch.num_rows


def archive_coverage(targets, *, version, start, end, selected_count, present=True):
    source = f"pmxt-{version}"
    return [
        coverage(
            target,
            source,
            start,
            end,
            "capability_unknown"
            if CAPABILITIES[source].get(target["asset_kind"]) != "supported"
            else (
                "success"
                if (
                    selected_count.get((target["asset_kind"], target["asset_id"]), 0)
                    if isinstance(selected_count, dict)
                    else selected_count
                )
                else "unobserved"
            )
            if present
            else "missing_object",
            object_present=present,
            continuous_coverage=False,
        )
        for target in targets
    ]
