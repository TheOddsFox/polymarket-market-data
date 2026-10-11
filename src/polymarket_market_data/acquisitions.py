"""Shared finite acquisitions used by standalone commands and the durable runner."""

from __future__ import annotations

import asyncio
import os
import re
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq

from . import producer
from .archives import (
    ARCHIVE_BATCH_SIZE,
    ATTRIBUTION,
    DECODER_REVISION,
    PROGRESS_REVISION,
    archive_coverage,
    archive_hours,
    archive_preflight,
    iter_archive,
)
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
from .foundation import Limits, digest, json_bytes, read_bytes
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


def _capture_reference(store, capture):
    return {
        "capture_id": capture["capture_id"],
        "manifest_sha256": digest(read_bytes(store.capture_path(capture) / "manifest.json")),
    }


def _observation_counts(counts):
    return [
        {"asset_kind": kind, "asset_id": asset_id, "observations": count}
        for (kind, asset_id), count in sorted(counts.items())
        if count
    ]


def _add_counts(total, counts):
    for key, count in counts.items():
        total[key] = total.get(key, 0) + count


def _archive_child_counts(
    store,
    capture,
    *,
    plan,
    plan_id,
    acquisition_id,
    unit,
    object_info,
    span,
    source,
    stream,
    lower,
    upper,
):
    """Check persisted selected rows without materializing depth arrays or the whole capture."""
    evidence = store.read_evidence(capture)
    first, stop = span
    child_first, rows = evidence.get("first_row"), evidence.get("source_rows")
    if (
        capture["acquisition_id"] != acquisition_id
        or capture["plan_id"] != plan_id
        or capture["source"] != source
        or capture["coverage"]
        or capture["objects"] != [object_info]
        or set(capture["tables"]) != {"book_updates"}
        or evidence.get("source_object") != object_info
        or type(evidence.get("decoder_revision")) is not int
        or evidence.get("decoder_revision") != DECODER_REVISION
        or not isinstance(child_first, int)
        or isinstance(child_first, bool)
        or not isinstance(rows, int)
        or isinstance(rows, bool)
        or not first <= child_first < stop
        or (child_first - first) % ARCHIVE_BATCH_SIZE
        or rows != min(ARCHIVE_BATCH_SIZE, stop - child_first)
        or capture["unit_id"]
        != f"{unit}-batch-decoder{DECODER_REVISION}-rows-{child_first}-{child_first + rows}"
    ):
        raise ValueError("archive data capture identity or physical span mismatch")
    targets = {
        (target.get("venue"), target["asset_kind"], target["asset_id"]): target
        for target in plan["assets"]
        if CAPABILITIES[source].get(target["asset_kind"]) == "supported"
    }
    path = store.capture_path(capture) / "book_updates.parquet"
    parquet = pq.ParquetFile(path, pre_buffer=False)
    count = parquet.metadata.num_rows
    if (
        not parquet.schema_arrow.equals(UPDATE_SCHEMA, check_metadata=False)
        or type(capture["tables"]["book_updates"].get("rows")) is not int
        or capture["tables"]["book_updates"] != {"rows": count, "schema": str(UPDATE_SCHEMA)}
        or not 0 < count <= rows * len(targets)
    ):
        raise ValueError("archive data capture schema or row count mismatch")
    fields = [
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
        "raw_identity",
        "received_at",
        "kind",
    ]
    for group in range(parquet.metadata.num_row_groups):
        decoded = sum(
            parquet.metadata.row_group(group)
            .column(UPDATE_SCHEMA.get_field_index(field))
            .total_uncompressed_size
            for field in fields
        )
        if decoded < 0 or decoded * 4 > store.limits.max_temp_bytes:
            raise ValueError("archive child row-group validation exceeds temporary budget")
    counts, previous, seen = {}, None, 0
    for batch in parquet.iter_batches(
        batch_size=ARCHIVE_BATCH_SIZE, columns=fields, use_threads=False
    ):
        if batch.nbytes > store.limits.max_temp_bytes // 4:
            raise ValueError("archive child validation exceeds temporary budget")
        for row in batch.to_pylist():
            target = targets.get((row["venue"], row["asset_kind"], row["asset_id"]))
            sequence, ordinal = row["sequence"], row["ordinal"]
            position = sequence, ordinal
            if (
                target is None
                or any(row[field] != target.get(field) for field in fields[:6])
                or row["source"] != source
                or row["stream"] != stream
                or sequence is None
                or not child_first <= sequence < child_first + rows
                or ordinal is None
                or not 0 <= ordinal < len(targets)
                or previous is not None
                and position <= previous
                or row["raw_identity"] != f"{object_info['sha256']}:{sequence}:{ordinal}"
                or row["received_at"] is None
                or not lower <= row["received_at"] < upper
                or row["kind"]
                not in ("book", "price_change", "tick_size_change", "market_resolved", "reset")
            ):
                raise ValueError("archive persisted observation identity or receive order mismatch")
            previous, seen = position, seen + 1
            if row["kind"] != "reset":
                key = row["asset_kind"], row["asset_id"]
                counts[key] = counts.get(key, 0) + 1
    if seen != count:
        raise ValueError("archive child validation row count mismatch")
    return counts


def _archive_progress_prefix(
    store,
    existing,
    *,
    plan,
    plan_id,
    acquisition_id,
    unit,
    object_info,
    spans,
    source,
    stream,
    lower,
    upper,
):
    """Prove a contiguous group chain and its exact group-local durable child inventory."""
    progress, children = {}, {}
    child_pattern = re.compile(
        re.escape(unit) + rf"-batch-decoder{DECODER_REVISION}-rows-([0-9]+)-([0-9]+)"
    )
    for name, capture in existing.items():
        if name.startswith(unit + "-progress"):
            evidence = store.read_evidence(capture)
            group = evidence.get("row_group")
            if (
                not isinstance(group, int)
                or isinstance(group, bool)
                or not 0 <= group < len(spans)
                or group in progress
            ):
                raise ValueError("conflicting archive progress chain")
            progress[group] = capture, evidence
        elif name.startswith(f"{unit}-batch-decoder{DECODER_REVISION}-"):
            match = child_pattern.fullmatch(name)
            if match is None:
                raise ValueError("invalid canonical archive data capture identity")
            first, stop = map(int, match.groups())
            group = next(
                (index for index, (a, b) in enumerate(spans) if a <= first < stop <= b), None
            )
            if group is None:
                raise ValueError("archive data capture lies outside a row group")
            children.setdefault(group, []).append(capture)
    counts, previous = {}, None
    for group in range(len(progress)):
        if group not in progress:
            raise ValueError("noncontiguous archive progress chain")
        capture, evidence = progress[group]
        first, stop = spans[group]
        expected_children = sorted(children.get(group, []), key=lambda c: c["unit_id"])
        references = [_capture_reference(store, child) for child in expected_children]
        if (
            capture["unit_id"]
            != f"{unit}-progress{PROGRESS_REVISION}-decoder{DECODER_REVISION}-group-{group}-rows-{first}-{stop}"
            or capture["acquisition_id"] != acquisition_id
            or capture["plan_id"] != plan_id
            or capture["source"] != source
            or capture["tables"]
            or capture["coverage"]
            or capture["objects"] != [object_info]
            or evidence.get("source_object") != object_info
            or type(evidence.get("decoder_revision")) is not int
            or evidence.get("decoder_revision") != DECODER_REVISION
            or type(evidence.get("progress_revision")) is not int
            or evidence.get("progress_revision") != PROGRESS_REVISION
            or type(evidence.get("batch_size")) is not int
            or evidence.get("batch_size") != ARCHIVE_BATCH_SIZE
            or type(evidence.get("first_row")) is not int
            or evidence.get("first_row") != first
            or type(evidence.get("stop_row")) is not int
            or evidence.get("stop_row") != stop
            or evidence.get("previous_progress") != previous
            or evidence.get("data_captures") != references
        ):
            raise ValueError("archive progress receipt proof mismatch")
        group_counts = {}
        for child in expected_children:
            _add_counts(
                group_counts,
                _archive_child_counts(
                    store,
                    child,
                    plan=plan,
                    plan_id=plan_id,
                    acquisition_id=acquisition_id,
                    unit=unit,
                    object_info=object_info,
                    span=spans[group],
                    source=source,
                    stream=stream,
                    lower=lower,
                    upper=upper,
                ),
            )
        if json_bytes(evidence.get("observation_counts")) != json_bytes(
            _observation_counts(group_counts)
        ):
            raise ValueError("archive progress observation counts mismatch")
        _add_counts(counts, group_counts)
        previous = _capture_reference(store, capture)
    return len(progress), counts, previous, children


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
            lower = max(utc(plan["start"]), item["hour"])
            upper = min(utc(plan["end"]), item["hour"] + timedelta(hours=1))
            progress_present = any(key.startswith(unit + "-progress") for key in existing)
            completed = existing.get(unit)
            completed_evidence = store.read_evidence(completed) if completed else {}
            if completed and "progress_revision" not in completed_evidence:
                if progress_present:
                    raise ValueError("legacy archive hour conflicts with current progress receipts")
                continue
            if not any(
                CAPABILITIES[source].get(target["asset_kind"]) == "supported"
                for target in plan["assets"]
            ):
                if progress_present or completed:
                    raise ValueError(
                        "archive progress conflicts with unsupported sealed identities"
                    )
                existing[unit] = store.capture(
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
                if progress_present and not pinned or completed and not pinned:
                    raise ValueError("archive progress lacks its original pinned source checkpoint")
                if pinned or previous:
                    objects = [
                        store.read_evidence(capture)["source_object"]
                        for capture in ([pinned] if pinned else []) + previous
                    ]
                    if any(value != objects[0] for value in objects):
                        raise ValueError("partial archive batches disagree on pinned source object")
                    object_info = objects[0]
                    receipt = {"resumed_original_object": True}
                else:
                    try:
                        receipt = client.download(item["url"], path)
                    except SourceError as exc:
                        if exc.status != "missing":
                            raise
                        existing[unit] = store.capture(
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
                object_path = store.object_path(object_info)
                existing["source-object-" + unit] = store.capture(
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
                source_pin = existing["source-object-" + unit]
                if (
                    source_pin["acquisition_id"] != acquisition_id
                    or source_pin["plan_id"] != plan_id
                    or source_pin["source"] != source
                    or source_pin["tables"]
                    or source_pin["coverage"]
                    or source_pin["objects"] != [object_info]
                    or store.read_evidence(source_pin).get("source_object") != object_info
                ):
                    raise ValueError("archive source checkpoint identity mismatch")
                spans, _ = archive_preflight(
                    object_path, version=version, max_decoded_bytes=store.limits.max_temp_bytes
                )
                stream = f"{source}/{item['hour'].isoformat()}/{object_info['sha256']}"
                next_group, counts, tail, group_inventory = _archive_progress_prefix(
                    store,
                    existing,
                    plan=plan,
                    plan_id=plan_id,
                    acquisition_id=acquisition_id,
                    unit=unit,
                    object_info=object_info,
                    spans=spans,
                    source=source,
                    stream=stream,
                    lower=lower,
                    upper=upper,
                )
                if completed:
                    if (
                        completed["acquisition_id"] != acquisition_id
                        or completed["plan_id"] != plan_id
                        or completed["source"] != source
                        or completed["tables"]
                        or type(completed_evidence.get("progress_revision")) is not int
                        or completed_evidence.get("progress_revision") != PROGRESS_REVISION
                        or completed_evidence.get("progress_tail") != tail
                        or next_group != len(spans)
                        or completed_evidence.get("source_object") != object_info
                        or type(completed_evidence.get("selected_updates")) is not int
                        or completed_evidence.get("selected_updates") != sum(counts.values())
                        or completed["objects"] != [object_info]
                        or json_bytes(completed["coverage"])
                        != json_bytes(
                            archive_coverage(
                                plan["assets"],
                                version=version,
                                start=lower,
                                end=upper,
                                selected_count=counts,
                            )
                        )
                    ):
                        raise ValueError("archive completed hour progress proof mismatch")
                    continue
                group_children, group_counts = [], {}
                cursor = spans[next_group][0] if next_group < len(spans) else 0
                for updates, accounting in iter_archive(
                    object_path,
                    version=version,
                    targets=plan["assets"],
                    start=lower,
                    end=upper,
                    hour=item["hour"],
                    object_sha256=object_info["sha256"],
                    start_row_group=next_group,
                    max_decoded_bytes=store.limits.max_temp_bytes,
                    max_row_bytes=store.limits.max_response_bytes,
                ):
                    if (
                        next_group >= len(spans)
                        or accounting.get("decoder_revision") != DECODER_REVISION
                    ):
                        raise ValueError("archive decoder group accounting mismatch")
                    first, stop = spans[next_group]
                    if accounting.get("group_complete"):
                        if (
                            updates
                            or accounting.get("source_rows") != 0
                            or accounting.get("row_group") != next_group
                            or accounting.get("first_row") != stop
                            or cursor != stop
                            or accounting.get("group_first_row") != first
                            or accounting.get("group_stop_row") != stop
                        ):
                            raise ValueError("archive decoder completed group accounting mismatch")
                        if {child["capture_id"] for child in group_children} != {
                            child["capture_id"] for child in group_inventory.get(next_group, [])
                        }:
                            raise ValueError(
                                "archive completed group omitted canonical data captures"
                            )
                        progress_unit = (
                            f"{unit}-progress{PROGRESS_REVISION}-decoder{DECODER_REVISION}"
                            f"-group-{next_group}-rows-{first}-{stop}"
                        )
                        evidence = {
                            "source_object": object_info,
                            "decoder_revision": DECODER_REVISION,
                            "progress_revision": PROGRESS_REVISION,
                            "batch_size": ARCHIVE_BATCH_SIZE,
                            "row_group": next_group,
                            "first_row": first,
                            "stop_row": stop,
                            "previous_progress": tail,
                            "data_captures": [
                                _capture_reference(store, child)
                                for child in sorted(group_children, key=lambda c: c["unit_id"])
                            ],
                            "observation_counts": _observation_counts(group_counts),
                        }
                        progress_capture = store.capture(
                            acquisition_id,
                            progress_unit,
                            {},
                            evidence=evidence,
                            objects=[object_info],
                        )
                        if store.read_evidence(progress_capture) != evidence:
                            raise ValueError("archive reused progress receipt proof mismatch")
                        existing[progress_unit] = progress_capture
                        tail = _capture_reference(store, progress_capture)
                        _add_counts(counts, group_counts)
                        group_children, group_counts = [], {}
                        next_group += 1
                        cursor = spans[next_group][0] if next_group < len(spans) else stop
                        continue
                    source_rows = accounting.get("source_rows")
                    if (
                        accounting.get("first_row") != cursor
                        or not isinstance(source_rows, int)
                        or isinstance(source_rows, bool)
                        or source_rows != min(ARCHIVE_BATCH_SIZE, stop - cursor)
                    ):
                        raise ValueError("archive decoder data batch accounting mismatch")
                    child_unit = f"{unit}-batch-decoder{DECODER_REVISION}-rows-{cursor}-{cursor + source_rows}"
                    cursor += source_rows
                    if not updates:
                        if child_unit in existing:
                            raise ValueError(
                                "archive replay omitted an existing canonical data batch"
                            )
                        continue
                    child = store.capture(
                        acquisition_id,
                        child_unit,
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
                    if child_unit not in existing:
                        group_inventory.setdefault(next_group, []).append(child)
                    existing[child_unit] = child
                    group_children.append(child)
                    _add_counts(
                        group_counts,
                        _archive_child_counts(
                            store,
                            child,
                            plan=plan,
                            plan_id=plan_id,
                            acquisition_id=acquisition_id,
                            unit=unit,
                            object_info=object_info,
                            span=spans[next_group],
                            source=source,
                            stream=stream,
                            lower=lower,
                            upper=upper,
                        ),
                    )
                if next_group != len(spans):
                    raise ValueError("archive decoder ended before completing its row groups")
                existing[unit] = store.capture(
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
                        "progress_revision": PROGRESS_REVISION,
                        "progress_tail": tail,
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


def _refresh_live_metadata(store, plan, events_executable, *, client=None):
    store.limits = Limits.from_dict(plan["limits"])
    bundle = producer.refresh_metadata(store, plan["market_ids"], events_executable, client=client)
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
        metadata = _refresh_live_metadata(store, plan, events_executable, client=client)
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
            metadata = _refresh_live_metadata(store, plan, events_executable, client=client)

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
