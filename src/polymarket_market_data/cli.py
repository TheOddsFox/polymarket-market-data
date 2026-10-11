"""Installed, finite local operator commands; no ambient checkout dependencies."""

from __future__ import annotations

import argparse
import signal
import subprocess
import threading
from pathlib import Path

from . import acquisitions
from .build import build
from .feeds import CAPABILITIES, window
from .foundation import Limits, Store, json_bytes
from .metadata import pin_metadata, select_assets

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
        return acquisitions.backfill(
            store,
            plan_id=args.plan,
            source="clob-history" if args.command == "prices" else "pmxt-" + args.command,
            resume_id=args.resume,
            enable_paid=getattr(args, "enable_paid", False),
            account=getattr(args, "account", None),
            period=getattr(args, "period", None),
            allowance_id=getattr(args, "allowance_id", None),
            allowance_cap=getattr(args, "allowance_cap", None),
        )
    if args.command == "archive":
        return acquisitions.archive(
            store, plan_id=args.plan, version=args.version, resume_id=args.resume
        )
    if args.command == "books":
        return acquisitions.record(
            store,
            plan_id=args.plan,
            events_executable=args.events_executable,
            events_root=args.events_root,
            resume_id=args.resume,
        )
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

    def terminate(signum, frame):
        raise KeyboardInterrupt("workflow terminated")

    previous = None
    if threading.current_thread() is threading.main_thread():
        previous = signal.signal(signal.SIGTERM, terminate)
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
    finally:
        if previous is not None:
            signal.signal(signal.SIGTERM, previous)
    print(json_bytes(result).decode())
    return 0
