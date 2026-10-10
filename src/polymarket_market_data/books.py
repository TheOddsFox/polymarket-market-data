"""Exact native-asset book normalization and receive-order replay.

Adapted from oddsfox-pipeline sports_data/books.py (MIT); no sports scope or quotes.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext

import pyarrow as pa

IDENTITY_FIELDS = [
    ("venue", pa.string()),
    ("asset_kind", pa.string()),
    ("asset_id", pa.string()),
    ("market_id", pa.string()),
    ("condition_id", pa.string()),
    ("outcome_index", pa.int32()),
]
TIME_FIELDS = [
    (name, pa.timestamp("us", tz="UTC")) for name in ("source_at", "received_at", "ingested_at")
]
EXACT = pa.decimal128(38, 18)
DEPTH = pa.list_(pa.struct([("price", EXACT), ("quantity", EXACT)]))
UPDATE_SCHEMA = pa.schema(
    [
        *IDENTITY_FIELDS,
        ("source", pa.string()),
        *TIME_FIELDS,
        ("stream", pa.string()),
        ("sequence", pa.int64()),
        ("ordinal", pa.int32()),
        ("kind", pa.string()),
        ("reason", pa.string()),
        ("source_hash", pa.string()),
        ("snapshot_id", pa.string()),
        ("raw_identity", pa.string()),
        ("bids", DEPTH),
        ("asks", DEPTH),
        ("side", pa.string()),
        ("price", EXACT),
        ("quantity", EXACT),
        ("tick_size", EXACT),
    ]
)
STATE_SCHEMA = pa.schema(
    [
        *IDENTITY_FIELDS,
        ("source", pa.string()),
        *TIME_FIELDS,
        ("stream", pa.string()),
        ("sequence", pa.int64()),
        ("ordinal", pa.int32()),
        ("snapshot_at", pa.timestamp("us", tz="UTC")),
        ("status", pa.string()),
        ("best_bid", EXACT),
        ("best_ask", EXACT),
        ("spread", EXACT),
        ("midpoint", pa.decimal128(38, 19)),
        ("bid_quantity", pa.decimal256(76, 18)),
        ("ask_quantity", pa.decimal256(76, 18)),
    ]
)


def number(value, *, price=False) -> Decimal:
    if isinstance(value, (float, bool)) or value is None:
        raise ValueError("numeric fields require exact decimal values")
    try:
        result = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("invalid decimal") from exc
    if not result.is_finite() or result < 0 or (price and result > 1):
        raise ValueError("numeric field outside supported range")
    _, digits, exponent = result.as_tuple()
    while digits and digits[-1] == 0:
        digits, exponent = digits[:-1], exponent + 1
    if max(len(digits) + exponent, 1) > 20 or max(-exponent, 0) > 18:
        raise ValueError("numeric field does not fit DECIMAL(38,18)")
    return result


def parse_json(payload: bytes | str):
    return json.loads(
        payload,
        parse_float=Decimal,
        parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
    )


def timestamp_ms(value) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("timestamp requires a timezone")
        return value.astimezone(UTC)
    value = number(value)
    if value != value.to_integral_value():
        raise ValueError("timestamp requires whole milliseconds")
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=int(value))


def levels(raw, *, bids: bool) -> list[dict]:
    if not isinstance(raw, list):
        raise ValueError("snapshot depth must be an array")
    result, seen = [], set()
    for item in raw:
        if isinstance(item, dict):
            price, quantity = item.get("price"), item.get("size", item.get("quantity"))
        elif isinstance(item, (list, tuple)) and len(item) == 2:
            price, quantity = item
        else:
            raise ValueError("malformed book level")
        price, quantity = number(price, price=True), number(quantity)
        if price in seen:
            raise ValueError("duplicate book price")
        seen.add(price)
        if quantity:
            result.append({"price": price, "quantity": quantity})
    return sorted(result, key=lambda item: item["price"], reverse=bids)


def asset_key(target: dict) -> tuple[str, str, str]:
    return target.get("venue", "polymarket"), target["asset_kind"], target["asset_id"]


def resets(targets, *, source, stream, sequence, received_at, reason):
    return [
        {
            **target,
            "source": source,
            "stream": stream,
            "sequence": sequence,
            "ordinal": ordinal,
            "received_at": received_at,
            "ingested_at": datetime.now(UTC),
            "source_at": None,
            "kind": "reset",
            "reason": reason,
        }
        for ordinal, target in enumerate(targets)
    ]


def normalize_message(
    payload,
    *,
    targets: list[dict],
    source: str,
    stream: str,
    sequence: int,
    received_at: datetime,
) -> list[dict]:
    if payload in (b"PING", b"PONG", "PING", "PONG"):
        return []
    value = parse_json(payload) if isinstance(payload, (str, bytes)) else payload
    messages = value if isinstance(value, list) else [value]
    result = []
    for message in messages:
        if not isinstance(message, dict):
            raise ValueError("book message must be an object")
        kind = message.get("event_type")
        if kind == "new_market":
            affected = [
                target
                for target in targets
                if target.get("condition_id") is not None
                and target.get("condition_id") == message.get("market")
            ]
            for reset in resets(
                affected,
                source=source,
                stream=stream,
                sequence=sequence,
                received_at=received_at,
                reason="metadata_refresh_needed",
            ):
                reset["ordinal"] = len(result)
                result.append(reset)
            continue
        if kind in ("last_trade_price", "best_bid_ask"):
            continue
        if kind not in ("book", "price_change", "tick_size_change", "market_resolved"):
            raise ValueError("unsupported market-stream event")
        changes = message.get("price_changes") if kind == "price_change" else [message]
        if kind == "market_resolved":
            ids = message.get("assets_ids", [message.get("asset_id")])
            if not isinstance(ids, list) or not ids or len(set(ids)) != len(ids):
                raise ValueError("invalid resolution identities")
            changes = [{"asset_id": item} for item in ids]
        if not isinstance(changes, list):
            raise ValueError("price_changes must be an array")
        for item in changes:
            if not isinstance(item, dict):
                raise ValueError("book update must be an object")
            native_id = str(item.get("asset_id", ""))
            candidates = [
                target
                for target in targets
                if target["asset_id"] == native_id
                and (
                    message.get("market") is None
                    or target.get("condition_id") is None
                    or target.get("condition_id") == message.get("market")
                )
            ]
            if len(candidates) != 1:
                raise ValueError("unknown or ambiguous subscribed identity")
            row = {
                **candidates[0],
                "source": source,
                "stream": stream,
                "sequence": sequence,
                "ordinal": len(result),
                "received_at": received_at,
                "ingested_at": datetime.now(UTC),
                "source_at": timestamp_ms(message["timestamp"])
                if message.get("timestamp") is not None
                else None,
                "kind": kind,
                "source_hash": item.get("hash", message.get("hash")),
            }
            if kind == "book":
                row.update(
                    bids=levels(item.get("bids"), bids=True),
                    asks=levels(item.get("asks"), bids=False),
                )
            elif kind == "price_change":
                if item.get("side") not in ("BUY", "SELL"):
                    raise ValueError("invalid book side")
                row.update(
                    side=item["side"],
                    price=number(item.get("price"), price=True),
                    quantity=number(item.get("size")),
                )
            elif kind == "tick_size_change":
                row["tick_size"] = number(item.get("new_tick_size"), price=True)
                if row["tick_size"] <= 0:
                    raise ValueError("tick size must be positive")
            else:
                row["reason"] = "market_resolved"
            result.append(row)
    return result


class Book:
    def __init__(self):
        self.bids, self.asks = {}, {}
        self.initialized = False
        self.reason = "missing_initial_snapshot"
        self.snapshot_at = None
        self.last_position = None
        self.last_archive_time = None
        self.last_archive_row = None
        self.ambiguous_time = None
        self.terminal = False

    def apply(self, row: dict) -> dict:
        position = row["sequence"], row["ordinal"]
        if self.last_position is not None and position <= self.last_position:
            raise ValueError("non-increasing receive order")
        self.last_position = position
        kind = row["kind"]
        if row["source"].startswith("pmxt-v"):
            stamp = row["received_at"]
            semantic = {
                key: value
                for key, value in row.items()
                if key not in ("sequence", "ordinal", "raw_identity", "ingested_at")
            }
            conflict = self.last_archive_time is not None and (
                stamp < self.last_archive_time
                or stamp == self.last_archive_time
                and semantic != self.last_archive_row
            )
            if conflict:
                self.ambiguous_time = stamp
            if conflict or stamp == self.ambiguous_time:
                kind = "reset"
                row = row | {"reason": "ambiguous_archive_order"}
            self.last_archive_time, self.last_archive_row = stamp, semantic
        if kind in ("reset", "market_resolved"):
            self.bids.clear()
            self.asks.clear()
            self.initialized = False
            self.reason = row.get("reason", "gap")
            self.terminal |= kind == "market_resolved"
        elif kind == "book" and not self.terminal:
            self.bids = {item["price"]: item["quantity"] for item in row["bids"]}
            self.asks = {item["price"]: item["quantity"] for item in row["asks"]}
            self.initialized = True
            self.snapshot_at = row["received_at"]
        elif kind == "price_change" and self.initialized:
            side = self.bids if row["side"] == "BUY" else self.asks
            if row["quantity"] == 0:
                side.pop(row["price"], None)
            else:
                side[row["price"]] = row["quantity"]
        bid, ask = max(self.bids, default=None), min(self.asks, default=None)
        status = self.reason
        if self.initialized:
            status = "empty" if not self.bids and not self.asks else "one_sided"
            if self.bids and self.asks:
                status = "crossed" if bid >= ask else "valid"
        with localcontext() as context:
            context.prec = 80
            return {
                **{
                    key: row.get(key)
                    for key in (
                        "venue",
                        "asset_kind",
                        "asset_id",
                        "market_id",
                        "condition_id",
                        "outcome_index",
                        "source",
                        "stream",
                        "sequence",
                        "ordinal",
                        "received_at",
                        "source_at",
                        "ingested_at",
                    )
                },
                "snapshot_at": self.snapshot_at,
                "status": status,
                "best_bid": bid,
                "best_ask": ask,
                "midpoint": (bid + ask) / 2 if status == "valid" else None,
                "spread": ask - bid if status == "valid" else None,
                "bid_quantity": sum(self.bids.values(), Decimal(0)),
                "ask_quantity": sum(self.asks.values(), Decimal(0)),
            }
