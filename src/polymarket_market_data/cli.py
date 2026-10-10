"""Installed, finite local operator commands; no ambient checkout dependencies."""

from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .archives import ATTRIBUTION, DECODER_REVISION, archive_coverage, archive_hours, iter_archive
from .books import UPDATE_SCHEMA
from .budget import BudgetLedger
from .build import build
from .feeds import (
    CAPABILITIES,
    PRICE_SCHEMA,
    TRADE_SCHEMA,
    acquire_history,
    acquire_paid,
    fingerprint,
    utc,
    window,
)
from .foundation import Limits, Store, json_bytes
from .metadata import pin_metadata, select_assets
from .recorder import record
from .transport import Client, SourceError

CONTRACT = "oddsfox.polymarket.market-data.v1"


def parser():
    root = argparse.ArgumentParser(description="Bounded Polymarket market data")
    root.add_argument("--root", type=Path, default=Path("data"))
    commands = root.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--metadata", type=Path, required=True)
    plan.add_argument("--metadata-sha256", required=True)
    plan.add_argument("--market-id", action="append", required=True)
    plan.add_argument("--feed", choices=CAPABILITIES, action="append", required=True)
    plan.add_argument("--start", required=True)
    plan.add_argument("--end", required=True)
    plan.add_argument("--fidelity", type=int, default=1)
    plan.add_argument("--duration", type=int, default=60)
    for name, default in (
        ("requests", 1000),
        ("download-bytes", 512 * 1024**2),
        ("storage-bytes", 10 * 1024**3),
        ("response-bytes", 16 * 1024**2),
        ("temp-bytes", 512 * 1024**2),
    ):
        plan.add_argument("--max-" + name, type=int, default=default)
    workflow = commands.add_parser("run", help="Collect, verify and publish selected free feeds")
    workflow.add_argument("--resume-run")
    workflow.add_argument("--market-id", action="append")
    workflow.add_argument(
        "--feed", choices=("clob-history", "clob-ws", "pmxt-v1", "pmxt-v2"), action="append"
    )
    workflow.add_argument("--start")
    workflow.add_argument("--end")
    workflow.add_argument("--fidelity", type=int)
    workflow.add_argument("--duration", type=int)
    workflow.add_argument("--events-executable", type=Path)
    for name in ("requests", "download-bytes", "storage-bytes", "response-bytes", "temp-bytes"):
        workflow.add_argument("--max-" + name, type=int)
    for name in ("prices", "books", "archive", "trades"):
        parent = commands.add_parser(name)
        actions = parent.add_subparsers(dest="action", required=True)
        if name == "books":
            live = actions.add_parser("record")
            _acquisition_flags(live)
            live.add_argument("--events-executable", required=True)
            live.add_argument("--events-root", type=Path, required=True)
        backfill = actions.add_parser("backfill")
        _acquisition_flags(backfill)
        if name == "archive":
            backfill.add_argument("--version", choices=("v1", "v2"), required=True)
        if name in ("books", "trades"):
            backfill.add_argument("--enable-paid", action="store_true")
            backfill.add_argument("--account", required=True)
            backfill.add_argument("--period", required=True)
            backfill.add_argument("--allowance-id", required=True)
            backfill.add_argument("--allowance-cap", type=int, required=True)
    derived = commands.add_parser("build")
    derived.add_argument("--plan", required=True)
    derived.add_argument("--acquisition", action="append", required=True)
    publish = commands.add_parser("publish")
    publish.add_argument("--release", required=True)
    verify = commands.add_parser("verify")
    verify.add_argument("--release")
    commands.add_parser("status")
    return root


def _acquisition_flags(command):
    command.add_argument("--plan", required=True)
    command.add_argument("--resume")


def _plan(store, args):
    start, end = window(args.start, args.end)
    if start.microsecond or end.microsecond:
        raise ValueError("plan bounds require whole UTC seconds")
    if args.fidelity <= 0 or not 1 <= args.duration <= 86400:
        raise ValueError("positive fidelity and duration 1..86400 required")
    limits = Limits(
        args.max_requests,
        args.max_download_bytes,
        args.max_storage_bytes,
        args.max_response_bytes,
        args.max_temp_bytes,
    )
    store.limits = limits
    bundle = pin_metadata(store.root, args.metadata, args.metadata_sha256, store=store)
    assets = select_assets(bundle, args.market_id)
    feeds = sorted(set(args.feed))
    return {
        "plan_id": store.create_plan(
            {
                "metadata_sha256": args.metadata_sha256,
                "market_ids": sorted(set(args.market_id)),
                "assets": assets,
                "feeds": feeds,
                "start": start,
                "end": end,
                "fidelity": args.fidelity,
                "duration": args.duration,
                "limits": limits.as_dict(),
            }
        ),
        "capabilities": {feed: CAPABILITIES[feed] for feed in feeds},
    }


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


def _backfill(store, args):
    plan = store.load_plan(args.plan)
    store.limits = Limits.from_dict(plan["limits"])
    source = "clob-history" if args.command == "prices" else "pmxt-" + args.command
    if source not in plan["feeds"]:
        raise ValueError("source is not in the sealed plan")
    paid = args.command in ("books", "trades")
    ledger = None
    if paid:
        if not args.enable_paid or not os.environ.get("PMXT_API_KEY"):
            raise ValueError("paid acquisition needs --enable-paid and PMXT_API_KEY")
        ledger = BudgetLedger(store.root, store=store)
        ledger.configure(args.account, args.period, args.allowance_id, args.allowance_cap)
    acquisition_id = store.new_acquisition(args.plan, source, args.resume)
    if any(
        capture["unit_id"] == "acquisition_complete"
        for capture in store.committed_captures(acquisition_id=acquisition_id)
    ):
        return _finish(store, acquisition_id)
    with Client(
        "pmxt" if paid else "clob",
        store.limits,
        storage_root=store.root,
        paid_ledger=ledger,
        account=getattr(args, "account", None),
        period=getattr(args, "period", None),
        allowance_id=getattr(args, "allowance_id", None),
        run_meter=getattr(store, "run_meter", None),
    ) as client:
        request = _cached_request(store, acquisition_id, client, source)
        for target in plan["assets"]:

            def emit(rows, evidence, covered, target=target):
                if paid and args.command == "books":
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
                    if args.command == "books"
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


def _archive(store, args):
    plan = store.load_plan(args.plan)
    store.limits = Limits.from_dict(plan["limits"])
    source = "pmxt-" + args.version
    if source not in plan["feeds"]:
        raise ValueError("archive source is not in the sealed plan")
    acquisition_id = store.new_acquisition(args.plan, source, args.resume)
    existing = {
        capture["unit_id"]: capture
        for capture in store.committed_captures(acquisition_id=acquisition_id)
    }
    with Client(
        "archive",
        store.limits,
        storage_root=store.root,
        run_meter=getattr(store, "run_meter", None),
    ) as client:
        for item in archive_hours(plan["start"], plan["end"], args.version):
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
                        version=args.version,
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
                                version=args.version,
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
                    version=args.version,
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
                        version=args.version,
                        start=lower,
                        end=upper,
                        selected_count=counts,
                    ),
                    objects=[object_info],
                )
            finally:
                shutil.rmtree(temporary)
    return _finish(store, acquisition_id) | {
        "requests": client.requests,
        "downloaded_bytes": client.downloaded,
    }


def _refresh_live_metadata(store, plan, args, client):
    from .runner import refresh_metadata

    store.limits = Limits.from_dict(plan["limits"])
    bundle = refresh_metadata(
        store, plan["market_ids"], args.events_executable, args.events_root, client=client
    )
    assets = select_assets(bundle, plan["market_ids"])
    fields = ("venue", "market_id", "asset_kind", "asset_id", "condition_id", "outcome_index")
    if {tuple(row.get(field) for field in fields) for row in assets} != {
        tuple(row.get(field) for field in fields) for row in plan["assets"]
    }:
        raise ValueError("selected identities changed; create a new plan")
    return bundle


def _record(store, args):
    if args.resume:
        raise ValueError("live recording cannot resume; start a new session with an explicit gap")
    plan = store.load_plan(args.plan)
    if "clob-ws" not in plan["feeds"]:
        raise ValueError("live feed is not in the sealed plan")
    store.limits = Limits.from_dict(plan["limits"])
    with Client(
        "clob", store.limits, storage_root=store.root, run_meter=getattr(store, "run_meter", None)
    ) as client:
        metadata = _refresh_live_metadata(store, plan, args, client)
        acquisition_id = getattr(args, "_acquisition_id", None) or store.new_acquisition(
            args.plan, "clob-ws"
        )
        if (
            store.acquisition(acquisition_id)["plan_id"] != args.plan
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
            metadata = _refresh_live_metadata(store, plan, args, client)

        asyncio.run(
            record(
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


def _run_locked(store, args):
    if args.command == "run":
        from .runner import run as workflow

        return workflow(store, args)
    if args.command == "plan":
        return _plan(store, args)
    if (
        args.command in ("prices", "trades")
        or args.command == "books"
        and args.action == "backfill"
    ):
        return _backfill(store, args)
    if args.command == "archive":
        return _archive(store, args)
    if args.command == "books":
        return _record(store, args)
    if args.command == "build":
        return {"release_id": build(store, args.plan, args.acquisition)}
    if args.command == "publish":
        return store.publish(args.release)
    if args.command == "verify":
        return store.verify(args.release)
    return store.status()


def run(args):
    store = Store(args.root, CONTRACT)
    with store.lock:
        return _run_locked(store, args)


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        result = run(args)
    except (ValueError, OSError, KeyError, subprocess.TimeoutExpired) as exc:
        # Provider and credential-bearing subprocess output is deliberately omitted.
        print(
            json_bytes(
                {"error": str(exc) if isinstance(exc, ValueError) else "local operation failed"}
            ).decode()
        )
        return 2
    print(json_bytes(result).decode())
    return 0
