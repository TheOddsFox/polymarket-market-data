"""Bounded free PMXT hourly archive parsing (CC BY 4.0 source data)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
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
DECODER_REVISION = 2

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
    schema = V1_SCHEMA if version == "v1" else V2_SCHEMA
    physical = pq.ParquetFile(path, pre_buffer=False)
    if not physical.schema_arrow.equals(schema, check_metadata=False):
        raise ValueError(f"unsupported PMXT {version} archive schema")
    if not all(
        isinstance(value, int) and not isinstance(value, bool) and value > 0
        for value in (max_decoded_bytes, max_row_bytes, batch_size)
    ):
        raise ValueError("archive decode limits must be positive integers")
    metadata = physical.metadata
    dictionary_columns = [
        [
            field.name
            for index, field in enumerate(schema)
            if pa.types.is_string(field.type)
            and {"RLE_DICTIONARY", "PLAIN_DICTIONARY"}.intersection(
                metadata.row_group(group).column(index).encodings
            )
        ]
        for group in range(metadata.num_row_groups)
    ]
    group_sizes = [
        sum(
            metadata.row_group(group).column(column).total_uncompressed_size
            for column in range(metadata.num_columns)
        )
        for group in range(metadata.num_row_groups)
    ]
    if any(size < 0 for size in group_sizes) or metadata.num_rows > max_decoded_bytes // 16:
        raise BudgetExceeded("archive source rows exceed work budget")
    maximum_group = max(group_sizes, default=0)
    # Dictionary readers also encode PLAIN fallback pages; reserve for their hash tables.
    decode_sizes = [
        sum(
            metadata.row_group(group).column(column).total_uncompressed_size
            * (16 if schema[column].name in dictionary_columns[group] else 4)
            for column in range(metadata.num_columns)
        )
        for group in range(metadata.num_row_groups)
    ]
    if max(decode_sizes, default=0) > max_decoded_bytes or metadata.num_rows and not maximum_group:
        raise BudgetExceeded("archive row-group decode exceeds temporary budget")
    row_limit = min(max_row_bytes, max_decoded_bytes // 16)
    batch_size = min(batch_size, 2048)
    condition_field = "market_id" if version == "v1" else "market"
    condition_values = pa.array(
        sorted(condition for condition in conditions if condition is not None)
        if version == "v1"
        else sorted(condition.encode("ascii") for condition in conditions if condition is not None),
        type=schema.field(condition_field).type,
    )
    stream = f"{source}/{hour.isoformat()}/{object_sha256}"
    offset = 0
    for group in range(metadata.num_row_groups):
        # Preserve source dictionaries; encoding unique PLAIN JSON adds an unbounded hash table.
        parquet = pq.ParquetFile(path, read_dictionary=dictionary_columns[group], pre_buffer=False)
        for batch in parquet.iter_batches(
            batch_size=batch_size, row_groups=[group], use_threads=False
        ):
            yield from _archive_batch(
                batch,
                version=version,
                schema=schema,
                supported=supported,
                conditions=condition_values,
                condition_field=condition_field,
                start=start,
                end=end,
                source=source,
                stream=stream,
                offset=offset,
                object_sha256=object_sha256,
                max_decoded_bytes=max_decoded_bytes,
                row_limit=row_limit,
            )
            offset += batch.num_rows


def _archive_batch(
    batch,
    *,
    version,
    schema,
    supported,
    conditions,
    condition_field,
    start,
    end,
    source,
    stream,
    offset,
    object_sha256,
    max_decoded_bytes,
    row_limit,
):
    """Select in Arrow before materializing any source strings in Python."""
    if batch.nbytes > max_decoded_bytes // 4:
        raise BudgetExceeded("archive decoded batch exceeds temporary budget")
    if any(batch.column(field.name).null_count for field in schema if not field.nullable):
        raise ValueError("archive has null required fields")
    stamps = batch.column("timestamp_received")
    mask = pc.and_(
        pc.is_in(batch.column(condition_field), value_set=conditions),
        pc.and_(
            pc.greater_equal(stamps, pa.scalar(start, type=stamps.type)),
            pc.less(stamps, pa.scalar(end, type=stamps.type)),
        ),
    )
    updates, normalized_bytes = [], 0
    for index in pc.indices_nonzero(mask).to_pylist():
        row = {}
        for field in schema:
            scalar = batch.column(field.name)[index]
            value = scalar.value if pa.types.is_dictionary(scalar.type) else scalar
            if value.is_valid and pa.types.is_string(value.type):
                if value.as_buffer().size > row_limit:
                    raise BudgetExceeded("archive source row exceeds decode budget")
            row[field.name] = value.as_py()
        stamp = row["timestamp_received"]
        condition = row["market_id"] if version == "v1" else row["market"].decode("ascii")
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
            normalized_bytes += len(json_bytes(update)) * 4
            if normalized_bytes > max_decoded_bytes // 4:
                raise BudgetExceeded("archive normalized batch exceeds temporary budget")
            updates.append(update)
    yield (
        updates,
        {
            "decoder_revision": DECODER_REVISION,
            "first_row": offset,
            "source_rows": batch.num_rows,
        },
    )


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
