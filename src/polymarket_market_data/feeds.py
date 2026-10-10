"""Bounded source acquisition without catalogue, sports or framework coupling."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta

import pyarrow as pa

from .books import EXACT, IDENTITY_FIELDS, TIME_FIELDS, levels, number, timestamp_ms

OBSERVATION_FIELDS = [("acquisition_id", pa.string()), ("capture_id", pa.string())]
PRICE_SCHEMA = pa.schema(
    [*IDENTITY_FIELDS, *OBSERVATION_FIELDS, ("source", pa.string()), *TIME_FIELDS, ("price", EXACT)]
)
TRADE_SCHEMA = pa.schema(
    [
        *IDENTITY_FIELDS,
        ("source", pa.string()),
        *TIME_FIELDS,
        ("trade_id", pa.string()),
        ("price", EXACT),
        ("amount", EXACT),
    ]
)
OHLC_SCHEMA = pa.schema(
    [
        *IDENTITY_FIELDS,
        *OBSERVATION_FIELDS,
        ("source", pa.string()),
        ("bucket_start", pa.timestamp("us", tz="UTC")),
        ("bucket_end", pa.timestamp("us", tz="UTC")),
        *[(name, EXACT) for name in ("open", "high", "low", "close")],
        ("observed_samples", pa.int64()),
        ("continuous_coverage", pa.bool_()),
    ]
)
COVERAGE_SCHEMA = pa.schema(
    [
        *IDENTITY_FIELDS,
        ("source", pa.string()),
        ("start", pa.timestamp("us", tz="UTC")),
        ("end", pa.timestamp("us", tz="UTC")),
        ("status", pa.string()),
        ("observations", pa.int64()),
        ("observed_samples", pa.int64()),
        ("gaps", pa.int64()),
        ("object_present", pa.bool_()),
        ("continuous_coverage", pa.bool_()),
    ]
)

CLOB_HISTORY = "https://clob.polymarket.com/prices-history"
PMXT_BOOKS = "https://api.pmxt.dev/api/polymarket/fetchOrderBook"
PMXT_TRADES = "https://api.pmxt.dev/api/polymarket/fetchTrades"
CAPABILITIES = {
    "clob-history": {"ctf_token": "supported", "poly_v2_position": "supported"},
    "clob-ws": {"ctf_token": "supported", "poly_v2_position": "supported"},
    "pmxt-v1": {"ctf_token": "supported", "poly_v2_position": "capability_unknown"},
    "pmxt-v2": {"ctf_token": "supported", "poly_v2_position": "capability_unknown"},
    "pmxt-books": {"ctf_token": "supported", "poly_v2_position": "capability_unknown"},
    "pmxt-trades": {"ctf_token": "supported", "poly_v2_position": "capability_unknown"},
}


def fingerprint(value) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def utc(value) -> datetime:
    result = (
        datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    )
    if not isinstance(result, datetime) or result.tzinfo is None:
        raise ValueError("bounds require timezone-aware timestamps")
    return result.astimezone(UTC)


def window(start, end):
    start, end = utc(start), utc(end)
    if start >= end:
        raise ValueError("window must have positive duration")
    return start, end


def coverage(target, source, start, end, status, **extra):
    return {**target, "source": source, "start": start, "end": end, "status": status, **extra}


def received_response(value):
    if isinstance(value, tuple) and len(value) == 2:
        return value[0], utc(value[1])
    return value, datetime.now(UTC)


def history_points(payload, *, target, start, end, received_at):
    if not isinstance(payload, dict) or not isinstance(payload.get("history"), list):
        raise ValueError("malformed CLOB history envelope")
    rows, seen = [], {}
    for point in payload["history"]:
        if not isinstance(point, dict) or "t" not in point or "p" not in point:
            raise ValueError("malformed price sample")
        stamp = number(point["t"])
        if stamp != stamp.to_integral_value():
            raise ValueError("sample time must be whole seconds")
        source_at = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=int(stamp))
        price = number(point["p"], price=True)
        if not start <= source_at < end:
            continue
        if source_at in seen and seen[source_at] != price:
            raise ValueError("conflicting prices at one sample time")
        if source_at in seen:
            continue
        seen[source_at] = price
        rows.append(
            {
                **target,
                "source": "clob-history",
                "source_at": source_at,
                "received_at": received_at,
                "ingested_at": datetime.now(UTC),
                "price": price,
            }
        )
    return sorted(rows, key=lambda row: row["source_at"])


def acquire_history(
    request, *, target, start, end, fidelity=1, max_window_seconds=86400, emit=None
):
    start, end = window(start, end)
    if not isinstance(fidelity, int) or isinstance(fidelity, bool) or fidelity <= 0:
        raise ValueError("fidelity must be positive minutes")
    if (
        isinstance(max_window_seconds, bool)
        or not isinstance(max_window_seconds, int)
        or max_window_seconds <= 0
    ):
        raise ValueError("history request window must be positive seconds")
    rows, evidence, covered = [], [], []
    cursor = start
    while cursor < end:
        stop = min(end, cursor + timedelta(seconds=max_window_seconds))
        payload = request(
            "GET",
            CLOB_HISTORY,
            params={
                "market": target["asset_id"],
                "startTs": int(cursor.timestamp()),
                "endTs": int(stop.timestamp()),
                "fidelity": fidelity,
            },
        )
        payload, received = received_response(payload)
        part = history_points(payload, target=target, start=cursor, end=stop, received_at=received)
        info = coverage(
            target,
            "clob-history",
            cursor,
            stop,
            "success" if part else "confirmed_empty",
            observed_samples=len(part),
            continuous_coverage=False,
        )
        raw = {
            "request_fingerprint": fingerprint(
                {"asset": target, "start": cursor, "end": stop, "fidelity": fidelity}
            ),
            "received_at": received,
            "payload": payload,
        }
        if emit:
            emit(part, raw, [info])
        else:
            rows.extend(part)
            evidence.append(raw)
            covered.append(info)
        cursor = stop
    return rows, {"responses": evidence}, covered


def pmxt_rows(payload):
    if not isinstance(payload, dict):
        raise ValueError("malformed PMXT envelope")
    if payload.get("success") is not True or payload.get("error"):
        raise ValueError("PMXT rejected request")
    data = payload.get("data")
    if not isinstance(data, list):
        raise ValueError("PMXT response must contain a data array")
    return data


def normalize_paid(item, *, target, source, start_ms, end_ms, received_at, sequence):
    if not isinstance(item, dict):
        raise ValueError("PMXT observation must be an object")
    stamp = number(item.get("timestamp"))
    if stamp != stamp.to_integral_value() or not start_ms <= stamp <= end_ms:
        raise ValueError("PMXT observation outside requested range")
    if item.get("outcomeId") is not None and str(item["outcomeId"]) != target["asset_id"]:
        raise ValueError("PMXT native asset identity changed")
    base = {
        **target,
        "source": source,
        "source_at": timestamp_ms(stamp),
        "received_at": received_at,
        "ingested_at": datetime.now(UTC),
    }
    if source == "pmxt-trades":
        identity = item.get("id")
        if (
            not isinstance(identity, (str, int))
            or isinstance(identity, bool)
            or not str(identity).strip()
        ):
            raise ValueError("trade requires a provider identity")
        amount = number(item.get("amount"))
        if amount <= 0:
            raise ValueError("trade amount must be positive")
        return base | {
            "trade_id": str(identity),
            "price": number(item.get("price"), price=True),
            "amount": amount,
        }
    bids, asks = levels(item.get("bids"), bids=True), levels(item.get("asks"), bids=False)
    identity = fingerprint({"asset": target, "timestamp": stamp, "bids": bids, "asks": asks})
    return base | {
        "kind": "book",
        "stream": "paid",
        "sequence": sequence,
        "ordinal": 0,
        "snapshot_id": identity,
        "bids": bids,
        "asks": asks,
    }


def acquire_paid(request, *, target, source, start, end, api_key, limit=1000, emit=None):
    """The injected transport reserves each attempt before I/O; no hidden retry here."""
    if source not in ("pmxt-books", "pmxt-trades"):
        raise ValueError("unknown paid source")
    start, end = window(start, end)
    if not api_key:
        raise ValueError("paid source requires an API key")
    if not isinstance(limit, int) or limit < 1:
        raise ValueError("PMXT limit must be positive")
    if CAPABILITIES[source].get(target["asset_kind"]) != "supported":
        return [], {"requests": []}, [coverage(target, source, start, end, "capability_unknown")]
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    if start.microsecond % 1000 or end.microsecond % 1000:
        raise ValueError("paid bounds require whole milliseconds")
    first = (start - epoch) // timedelta(milliseconds=1)
    final = (end - epoch) // timedelta(milliseconds=1) - 1
    windows, rows, evidence, covered, seen = [(first, final)], [], [], [], {}
    sequence = 0
    while windows:
        lower, upper = windows.pop()
        headers = {"Authorization": f"Bearer {api_key}"}
        if source == "pmxt-books":
            if not target.get("condition_id"):
                raise ValueError("PMXT books require a condition identity")
            payload = request(
                "POST",
                PMXT_BOOKS,
                headers=headers,
                json={
                    "args": [
                        target["condition_id"],
                        None,
                        {
                            "since": lower,
                            "until": upper,
                            "outcome": target["asset_id"],
                            "limit": limit,
                        },
                    ]
                },
            )
        else:
            payload = request(
                "GET",
                PMXT_TRADES,
                headers=headers,
                params={
                    "outcomeId": target["asset_id"],
                    "start": timestamp_ms(lower).isoformat(),
                    "end": timestamp_ms(upper).isoformat(),
                    "limit": limit,
                },
            )
        payload, received = received_response(payload)
        raw_rows = pmxt_rows(payload)
        if len(raw_rows) > limit:
            raise ValueError("PMXT exceeded requested result limit")
        receipt = {"start_ms": lower, "end_ms": upper, "received_at": received, "payload": payload}
        if len(raw_rows) == limit:
            if lower == upper:
                raise ValueError("irreducibly saturated PMXT millisecond")
            middle = (lower + upper) // 2
            windows.extend([(middle + 1, upper), (lower, middle)])
            evidence.append(receipt | {"status": "saturated_split"})
            continue
        part = []
        for item in raw_rows:
            row = normalize_paid(
                item,
                target=target,
                source=source,
                start_ms=lower,
                end_ms=upper,
                received_at=received,
                sequence=sequence,
            )
            sequence += 1
            key = row.get("trade_id", row.get("snapshot_id"))
            semantic = {
                name: value
                for name, value in row.items()
                if name not in ("received_at", "ingested_at", "sequence")
            }
            if key in seen:
                if seen[key] != semantic:
                    raise ValueError("conflicting PMXT provider identity")
                continue
            seen[key] = semantic
            part.append(row)
        info = coverage(
            target,
            source,
            timestamp_ms(lower),
            timestamp_ms(upper + 1),
            "success" if part else "confirmed_empty",
            observations=len(part),
        )
        if emit:
            emit(part, {"responses": [*evidence, receipt]}, [info])
            evidence.clear()
        else:
            rows.extend(part)
            evidence.append(receipt)
            covered.append(info)
    return rows, {"responses": evidence}, covered
