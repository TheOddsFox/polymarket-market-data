"""Shared finite acquisitions used by standalone commands and the durable runner."""

from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from . import producer
from .archives import ATTRIBUTION, DECODER_REVISION, archive_coverage, archive_hours, iter_archive
from .books import UPDATE_SCHEMA
from .budget import BudgetLedger
from .feeds import (
    CAPABILITIES,
    PRICE_SCHEMA,
    TRADE_SCHEMA,
    acquire_history,
    acquire_paid,
    fingerprint,
    utc,
)
from .foundation import Limits
from .metadata import select_assets
from .recorder import record as record_messages
from .transport import Client, SourceError


def _finish(store, acquisition_id):
    captures = store.committed_captures(acquisition_id=acquisition_id)
    ids = sorted(
        capture["capture_id"]
        for capture in captures
        if capture["unit_id"] != "acquisition_complete"
    )
    store.capture(
        acquisition_id,
        "acquisition_complete",
        {},
        evidence={"capture_ids": ids, "acquisition_revision": 1, "schema_revision": 1},
    )
    return {"acquisition_id": acquisition_id, "captures": len(ids), "complete": True}


def _sanitized(source, payload):
    if source == "clob-history":
        if not isinstance(payload, dict):
            return payload
        points = payload.get("history")
        return {
            "history": [
                {key: row[key] for key in ("t", "p") if key in row}
                if isinstance(row, dict)
                else row
                for row in points
            ]
            if isinstance(points, list)
            else points
        }
    fields = ("id", "timestamp", "outcomeId", "price", "amount", "bids", "asks")
    if isinstance(payload, list):
        return [
            {key: row[key] for key in fields if key in row} if isinstance(row, dict) else row
            for row in payload
        ]
    if isinstance(payload, dict):
        return {
            "success": payload.get("success"),
            "error": "provider_error" if payload.get("error") else None,
            "data": _sanitized(source, payload.get("data")),
        }
    return payload


def _cached_request(store, acquisition_id, client, source):
    existing = {
        capture["unit_id"]: capture
        for capture in store.committed_captures(acquisition_id=acquisition_id)
    }

    def request(method, url, **kwargs):
        key = "http-" + fingerprint(
            {
                "method": method,
                "url": url,
                **{name: value for name, value in kwargs.items() if name != "headers"},
            }
        )
        if key in existing:
            cached = store.read_evidence(existing[key])
            return cached["payload"], utc(cached["received_at"])
        payload = client.json(method, url, **kwargs)
        received = datetime.now(UTC)
        sanitized = _sanitized(source, payload)
        existing[key] = store.capture(
            acquisition_id,
            key,
            {},
            evidence={
                "request_fingerprint": key[5:],
                "payload": sanitized,
                "received_at": received,
            },
        )
        return sanitized, received

    return request


def backfill(
    store,
    *,
    plan_id,
    source,
    resume_id=None,
    enable_paid=False,
    account=None,
    period=None,
    allowance_id=None,
    allowance_cap=None,
):
    plan = store.load_plan(plan_id)
    store.limits = Limits.from_dict(plan["limits"])
    if source not in ("clob-history", "pmxt-books", "pmxt-trades"):
        raise ValueError("unsupported historical source")
    if source not in plan["feeds"]:
        raise ValueError("source is not in the sealed plan")
    paid = source != "clob-history"
    ledger = None
    if paid:
        if not enable_paid or not os.environ.get("PMXT_API_KEY"):
            raise ValueError("paid acquisition needs --enable-paid and PMXT_API_KEY")
        ledger = BudgetLedger(store.root, store=store)
        ledger.configure(account, period, allowance_id, allowance_cap)
    acquisition_id = store.new_acquisition(plan_id, source, resume_id)
    if any(
        capture["unit_id"] == "acquisition_complete"
        for capture in store.committed_captures(acquisition_id=acquisition_id)
    ):
        return _finish(store, acquisition_id)
    with Client(
        "pmxt" if paid else "clob",
        store.limits,
        storage_root=store.root,
        store=store,
        paid_ledger=ledger,
        account=account,
        period=period,
        allowance_id=allowance_id,
        run_meter=getattr(store, "run_meter", None),
    ) as client:
        request = _cached_request(store, acquisition_id, client, source)
        for target in plan["assets"]:

            def emit(rows, evidence, covered, target=target):
                if source == "pmxt-books":
                    for row in rows:
                        row["stream"] = acquisition_id
                unit = "rows-" + fingerprint(
                    {
                        "target": target,
                        "coverage": [
                            {
                                key: value
                                for key, value in item.items()
                                if key not in ("observations", "observed_samples")
                            }
                            for item in covered
                        ],
                    }
                )
                relation = (
                    "book_updates"
                    if source == "pmxt-books"
                    else "trades"
                    if paid
                    else "price_samples"
                )
                schema = (
                    UPDATE_SCHEMA
                    if relation == "book_updates"
                    else TRADE_SCHEMA
                    if relation == "trades"
                    else PRICE_SCHEMA
                )
                store.capture(
                    acquisition_id,
                    unit,
                    {relation: rows},
                    evidence=evidence,
                    coverage=covered,
                    schemas={relation: schema},
                )

            if paid:
                rows, evidence, covered = acquire_paid(
                    request,
                    target=target,
                    source=source,
                    start=plan["start"],
                    end=plan["end"],
                    api_key=os.environ["PMXT_API_KEY"],
                    emit=emit,
                )
                if covered:
                    emit(rows, evidence, covered)
            else:
                acquire_history(
                    request,
                    target=target,
                    start=plan["start"],
                    end=plan["end"],
                    fidelity=plan["fidelity"],
                    emit=emit,
                )
    result = _finish(store, acquisition_id)
    return result | {"requests": client.requests, "downloaded_bytes": client.downloaded}


def archive(store, *, plan_id, version, resume_id=None):
    plan = store.load_plan(plan_id)
    store.limits = Limits.from_dict(plan["limits"])
    source = "pmxt-" + version
    if source not in plan["feeds"]:
        raise ValueError("archive source is not in the sealed plan")
    acquisition_id = store.new_acquisition(plan_id, source, resume_id)
    existing = {
        capture["unit_id"]: capture
        for capture in store.committed_captures(acquisition_id=acquisition_id)
    }
    with Client(
        "archive",
        store.limits,
        storage_root=store.root,
        store=store,
        run_meter=getattr(store, "run_meter", None),
    ) as client:
        for item in archive_hours(plan["start"], plan["end"], version):
            unit = "hour-" + item["hour"].strftime("%Y%m%dT%H")
            if unit in existing:
                continue
            lower = max(utc(plan["start"]), item["hour"])
            upper = min(utc(plan["end"]), item["hour"] + timedelta(hours=1))
            if not any(
                CAPABILITIES[source].get(target["asset_kind"]) == "supported"
                for target in plan["assets"]
            ):
                store.capture(
                    acquisition_id,
                    unit,
                    {},
                    coverage=archive_coverage(
                        plan["assets"],
                        version=version,
                        start=lower,
                        end=upper,
                        selected_count=0,
                    ),
                )
                continue
            temporary = Path(tempfile.mkdtemp(prefix=".archive-", dir=store.root / ".state"))
            try:
                path = temporary / "source.parquet"
                pinned = existing.get("source-object-" + unit)
                previous = [
                    capture for key, capture in existing.items() if key.startswith(unit + "-batch-")
                ]
                if pinned or previous:
                    objects = [
                        store.read_evidence(capture)["source_object"]
                        for capture in ([pinned] if pinned else []) + previous
                    ]
                    if any(value != objects[0] for value in objects):
                        raise ValueError("partial archive batches disagree on pinned source object")
                    object_info = objects[0]
                    store.object_path(object_info)
                    receipt = {"resumed_original_object": True}
                else:
                    try:
                        receipt = client.download(item["url"], path)
                    except SourceError as exc:
                        if exc.status != "missing":
                            raise
                        store.capture(
                            acquisition_id,
                            unit,
                            {},
                            evidence={"url": item["url"], "missing": True},
                            coverage=archive_coverage(
                                plan["assets"],
                                version=version,
                                start=lower,
                                end=upper,
                                selected_count=0,
                                present=False,
                            ),
                        )
                        continue
                    object_info = store.retain_object(path)
                store.capture(
                    acquisition_id,
                    "source-object-" + unit,
                    {},
                    evidence={
                        "source_object": object_info,
                        "url": item["url"],
                        "etag": receipt.get("etag"),
                        "attribution": ATTRIBUTION,
                    },
                    objects=[object_info],
                )
                counts = {}
                for updates, accounting in iter_archive(
                    store.object_path(object_info),
                    version=version,
                    targets=plan["assets"],
                    start=lower,
                    end=upper,
                    hour=item["hour"],
                    object_sha256=object_info["sha256"],
                    max_decoded_bytes=store.limits.max_temp_bytes,
                    max_row_bytes=store.limits.max_response_bytes,
                ):
                    for update in updates:
                        key = update["asset_kind"], update["asset_id"]
                        if update["kind"] != "reset":
                            counts[key] = counts.get(key, 0) + 1
                    if not updates:
                        continue
                    first_row = accounting["first_row"]
                    stop_row = first_row + accounting["source_rows"]
                    store.capture(
                        acquisition_id,
                        f"{unit}-batch-decoder{DECODER_REVISION}-rows-{first_row}-{stop_row}",
                        {"book_updates": updates},
                        evidence={
                            "source_object": object_info,
                            "url": item["url"],
                            "attribution": ATTRIBUTION,
                            **accounting,
                        },
                        objects=[object_info],
                        schemas={"book_updates": UPDATE_SCHEMA},
                    )
                store.capture(
                    acquisition_id,
                    unit,
                    {},
                    evidence={
                        "source_object": object_info,
                        "url": item["url"],
                        "etag": receipt.get("etag"),
                        "resumed_original_object": receipt.get("resumed_original_object", False),
                        "selected_updates": sum(counts.values()),
                        "attribution": ATTRIBUTION,
                    },
                    coverage=archive_coverage(
                        plan["assets"],
                        version=version,
                        start=lower,
                        end=upper,
                        selected_count=counts,
                    ),
                    objects=[object_info],
                )
            finally:
                store.remove(temporary)
    return _finish(store, acquisition_id) | {
        "requests": client.requests,
        "downloaded_bytes": client.downloaded,
    }


def _refresh_live_metadata(store, plan, events_executable, events_root=None, *, client=None):
    store.limits = Limits.from_dict(plan["limits"])
    bundle = producer.refresh_metadata(
        store, plan["market_ids"], events_executable, events_root, client=client
    )
    assets = select_assets(bundle, plan["market_ids"])
    fields = ("venue", "market_id", "asset_kind", "asset_id", "condition_id", "outcome_index")
    if {tuple(row.get(field) for field in fields) for row in assets} != {
        tuple(row.get(field) for field in fields) for row in plan["assets"]
    }:
        raise ValueError("selected identities changed; create a new plan")
    return bundle


def record(
    store,
    *,
    plan_id,
    events_executable,
    events_root=None,
    acquisition_id=None,
    resume_id=None,
):
    if resume_id:
        raise ValueError("live recording cannot resume; start a new session with an explicit gap")
    plan = store.load_plan(plan_id)
    if "clob-ws" not in plan["feeds"]:
        raise ValueError("live feed is not in the sealed plan")
    store.limits = Limits.from_dict(plan["limits"])
    with Client(
        "clob", store.limits, store=store, run_meter=getattr(store, "run_meter", None)
    ) as client:
        metadata = _refresh_live_metadata(
            store, plan, events_executable, events_root, client=client
        )
        acquisition_id = acquisition_id or store.new_acquisition(plan_id, "clob-ws")
        if (
            store.acquisition(acquisition_id)["plan_id"] != plan_id
            or store.acquisition(acquisition_id)["source"] != "clob-ws"
        ):
            raise ValueError("live acquisition mismatch")
        counter = 0

        async def emit(rows, evidence, covered):
            nonlocal counter
            evidence["metadata_sha256"] = metadata.path.name
            store.capture(
                acquisition_id,
                f"message-{counter:012d}",
                {"book_updates": rows},
                evidence=evidence,
                coverage=covered,
                schemas={"book_updates": UPDATE_SCHEMA},
            )
            counter += 1

        async def refresh_metadata():
            nonlocal metadata
            # Reconnect after refresh; the next connection requires fresh snapshots.
            metadata = _refresh_live_metadata(
                store, plan, events_executable, events_root, client=client
            )

        asyncio.run(
            record_messages(
                targets=plan["assets"],
                duration=plan["duration"],
                emit=emit,
                charge_request=client.reserve_request,
                charge_download=client.count_bytes,
                max_message_bytes=min(
                    store.limits.max_response_bytes, store.limits.max_temp_bytes // 64
                ),
                refresh_metadata=refresh_metadata,
            )
        )
    return _finish(store, acquisition_id) | {
        "requests": client.requests,
        "downloaded_bytes": client.downloaded,
    }
