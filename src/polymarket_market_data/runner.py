"""One finite free-source workflow with durable, conservative lifetime accounting."""

from __future__ import annotations

import os
import re
import shutil
import tempfile
import uuid
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq

from .foundation import (
    BudgetExceeded,
    Limits,
    atomic_write,
    digest,
    fsync_dir,
    json_bytes,
    json_loads,
    now,
    read_bytes,
    read_json,
)
from .metadata import MAX_BUNDLE, pin_metadata, select_assets
from .producer import bounded_run, environment

FREE_FEEDS = ("clob-history", "clob-ws", "pmxt-v1", "pmxt-v2")
DEFAULTS = {
    "fidelity": 1,
    "duration": 60,
    "max_requests": 1000,
    "max_download_bytes": 4 * 1024**3,
    "max_storage_bytes": 16 * 1024**3,
    "max_response_bytes": 16 * 1024**2,
    "max_temp_bytes": 2 * 1024**3,
}


class RunMeter:
    """A reservation remains charged until its owner proves its measured consumption."""

    def __init__(self, limits, data, save=lambda: None):
        self.limits, self.data, self.save = limits, data, save
        if not data:
            data.update(
                measured_requests=0,
                measured_downloaded_bytes=0,
                charged_requests=0,
                charged_downloaded_bytes=0,
                reservations={},
            )
        for key in (
            "measured_requests",
            "measured_downloaded_bytes",
            "charged_requests",
            "charged_downloaded_bytes",
        ):
            self._nonnegative(data.get(key))
        if not isinstance(data.get("reservations"), dict):
            raise ValueError("invalid run accounting journal")
        for value in data["reservations"].values():
            self._nonnegative(value["requests"])
            self._nonnegative(value["downloaded_bytes"])

    @staticmethod
    def _nonnegative(value):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("invalid run resource accounting")

    @property
    def charged(self):
        reservations = self.data["reservations"].values()
        return (
            self.data["charged_requests"] + sum(value["requests"] for value in reservations),
            self.data["charged_downloaded_bytes"]
            + sum(value["downloaded_bytes"] for value in reservations),
        )

    @property
    def remaining(self):
        requests, downloaded = self.charged
        return (
            max(0, self.limits.max_requests - requests),
            max(0, self.limits.max_download_bytes - downloaded),
        )

    def reserve(self, requests, downloaded, label):
        self._nonnegative(requests)
        self._nonnegative(downloaded)
        remaining = self.remaining
        if requests > remaining[0] or downloaded > remaining[1]:
            raise BudgetExceeded("run request/download allowance exhausted")
        token = uuid.uuid4().hex
        self.data["reservations"][token] = {
            "requests": requests,
            "downloaded_bytes": downloaded,
            "label": label,
            "reserved_at": now(),
        }
        self.save()
        return token

    def settle(self, token, requests, downloaded, *, measured=None):
        self._nonnegative(requests)
        self._nonnegative(downloaded)
        measurement = measured or (requests, downloaded)
        for value in measurement:
            self._nonnegative(value)
        reservation = self.data["reservations"].pop(token)
        self.data["charged_requests"] += requests
        self.data["charged_downloaded_bytes"] += downloaded
        self.data["measured_requests"] += measurement[0]
        self.data["measured_downloaded_bytes"] += measurement[1]
        self.save()
        # Preserve actual crossing bytes before surfacing the exceeded limit.
        if (
            requests > reservation["requests"]
            or downloaded > reservation["downloaded_bytes"]
            or self.charged[0] > self.limits.max_requests
            or self.charged[1] > self.limits.max_download_bytes
        ):
            raise BudgetExceeded("source exceeded reserved run allowance")

    def charge_bytes(self, size):
        self._nonnegative(size)
        self.data["charged_downloaded_bytes"] += size
        self.data["measured_downloaded_bytes"] += size
        self.save()
        if self.charged[1] > self.limits.max_download_bytes:
            raise BudgetExceeded("run downloaded-byte limit exhausted")


def refresh_metadata(store, market_ids, events_executable, events_root=None, *, client=None):
    """Use a fresh managed producer root so ambient configuration cannot choose sources or paths."""
    meter = getattr(store, "run_meter", None)
    remaining = (
        meter.remaining
        if meter
        else (
            store.limits.max_requests - (client.requests if client else 0),
            store.limits.max_download_bytes - (client.downloaded if client else 0),
        )
    )
    if min(remaining) <= 0:
        raise BudgetExceeded("metadata refresh request/download limit exhausted")
    store._budget()
    retained = sum(path.stat().st_size for path in store.root.rglob("*") if path.is_file())
    output_budget = min(
        MAX_BUNDLE,
        store.limits.max_temp_bytes // 4,
        (store.limits.max_storage_bytes - retained) // 4,
    )
    if output_budget <= 0:
        raise BudgetExceeded("metadata refresh requires available temporary and retained storage")
    reservation = meter.reserve(*remaining, "metadata producer") if meter else None
    with tempfile.TemporaryDirectory(prefix=".metadata-", dir=store.root / ".state") as temporary:
        directory = Path(temporary) / "bundle"
        producer_root = Path(temporary) / "events"
        producer_root.mkdir()
        argv = [
            str(events_executable),
            "metadata",
            "refresh",
            "--output",
            str(directory),
            "--max-output-bytes",
            str(output_budget),
            "--max-response-bytes",
            str(min(store.limits.max_response_bytes, 8 * 1024**2)),
            "--max-requests",
            str(remaining[0]),
            "--max-download-bytes",
            str(remaining[1]),
        ]
        for market_id in market_ids:
            argv.extend(["--market-id", market_id])
        result = bounded_run(
            argv,
            env=environment(
                producer_root,
                replace(
                    store.limits,
                    max_requests=remaining[0],
                    max_download_bytes=remaining[1],
                    max_storage_bytes=store.limits.max_storage_bytes - retained - output_budget,
                    max_temp_bytes=store.limits.max_temp_bytes - output_budget,
                ),
            ),
            directory=temporary,
            store=store,
            max_diagnostics=min(64 * 1024, store.limits.max_temp_bytes),
        )
        try:
            reply = json_loads(result.stdout) if result.stdout.strip() else {}
            if not isinstance(reply, dict):
                raise ValueError
            attempts, downloaded = reply["http_attempts"], reply["downloaded_bytes"]
            RunMeter._nonnegative(attempts)
            RunMeter._nonnegative(downloaded)
        except (ValueError, UnicodeError, KeyError):
            raise ValueError("selected metadata refresh lacks valid resource accounting") from None
        if client:
            client.requests += attempts
            client.downloaded += downloaded
        if meter:
            meter.settle(reservation, attempts, downloaded)
        elif attempts > remaining[0] or downloaded > remaining[1]:
            raise BudgetExceeded("metadata refresh exceeded invocation request/download budget")
        if result.returncode:
            raise ValueError("selected metadata refresh failed or busy")
        if not isinstance(reply.get("manifest_sha256"), str):
            raise ValueError("selected metadata refresh lacks manifest digest")
        return pin_metadata(store.root, directory, reply["manifest_sha256"], store=store)


class RunJournal:
    def __init__(self, store, run_id=None, settings=None):
        self.store = store
        self.run_id = store._name(run_id) if run_id else uuid.uuid4().hex
        self.path = store.root / ".state" / "runs" / (self.run_id + ".json")
        if self.path.parent.is_symlink():
            raise ValueError("symlink run journal directory")
        if run_id:
            self.data = read_json(self.path)
            if (
                self.data.get("contract") != store.contract + ".run"
                or self.data.get("run_id") != run_id
                or self.data.get("settings_sha256") != digest(json_bytes(self.data["settings"]))
            ):
                raise ValueError("run journal contract/settings mismatch")
        else:
            self.data = {
                "contract": store.contract + ".run",
                "run_id": self.run_id,
                "created_at": now(),
                "settings": settings,
                "settings_sha256": digest(json_bytes(settings)),
                "stage": "metadata",
                "plan_id": None,
                "acquisitions": {},
                "interrupted_live_acquisitions": [],
                "completed_acquisitions": {},
                "release_id": None,
                "usage": {},
            }
        store.limits = Limits.from_dict(self.data["settings"]["limits"])
        self.meter = RunMeter(store.limits, self.data["usage"], self.write)
        if not run_id:
            self.write()

    def write(self):
        self.data["updated_at"] = now()
        raw = json_bytes(self.data)
        if len(raw) > self.store.limits.max_temp_bytes:
            raise BudgetExceeded("run journal temporary budget exhausted")
        self.store._budget(len(raw))
        atomic_write(self.path, raw)

    def stage(self, value):
        self.data["stage"] = value
        self.write()


def _executable(value):
    candidate = str(value)
    path = Path(candidate) if "/" in candidate else Path(shutil.which(candidate) or candidate)
    try:
        path = path.resolve(strict=True)
    except OSError:
        raise ValueError("events executable must be an existing executable file") from None
    if not path.is_file() or not os.access(path, os.X_OK):
        raise ValueError("events executable must be an existing executable file")
    return str(path)


def _settings(args):
    from .feeds import window

    if not args.market_id or not args.feed or not args.start or not args.end:
        raise ValueError("new run requires --market-id, --feed, --start and --end")
    if not args.events_executable:
        raise ValueError("new run requires explicit --events-executable")
    if any(not re.fullmatch(r"[1-9][0-9]{0,19}", value) for value in args.market_id):
        raise ValueError("invalid market ID")
    if any(value not in FREE_FEEDS for value in args.feed):
        raise ValueError("run accepts only explicitly selected free feeds")
    start, end = window(args.start, args.end)
    if start.microsecond or end.microsecond:
        raise ValueError("run bounds require whole UTC seconds")
    values = {
        name: getattr(args, name, None) if getattr(args, name, None) is not None else default
        for name, default in DEFAULTS.items()
    }
    if values["fidelity"] <= 0 or not 1 <= values["duration"] <= 86400:
        raise ValueError("positive fidelity and duration 1..86400 required")
    limits = Limits(**{name: value for name, value in values.items() if name.startswith("max_")})
    return {
        "market_ids": sorted(set(args.market_id)),
        "feeds": sorted(set(args.feed)),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "events_executable": _executable(args.events_executable),
        "fidelity": values["fidelity"],
        "duration": values["duration"],
        "limits": limits.as_dict(),
    }


def _check_resume(args, settings):
    for name, key in (("market_id", "market_ids"), ("feed", "feeds")):
        value = getattr(args, name, None)
        if value is not None and sorted(set(value)) != settings[key]:
            raise ValueError("resume selections differ from sealed run")
    for name in ("start", "end", "events_executable", *DEFAULTS):
        value = getattr(args, name, None)
        if value is None:
            continue
        if name in ("start", "end"):
            from .feeds import utc

            value = utc(value).isoformat()
        if name == "events_executable":
            value = _executable(value)
        expected = settings["limits"][name] if name.startswith("max_") else settings[name]
        if value != expected:
            raise ValueError("resume settings or limits differ from sealed run")


def _completed(store, acquisition_id):
    return any(
        capture["unit_id"] == "acquisition_complete"
        for capture in store.committed_captures(acquisition_id=acquisition_id)
    )


def _report(store, journal, manifest=None, before=(0, 0)):
    sources = {}
    for source, acquisition_id in journal.data["completed_acquisitions"].items():
        captures = store.committed_captures(acquisition_id=acquisition_id)
        counts = {}
        observed = {}
        for capture in captures:
            for relation, table in capture["tables"].items():
                counts[relation] = counts.get(relation, 0) + table["rows"]
                if not table["rows"] or relation not in ("price_samples", "book_updates", "trades"):
                    continue
                columns = ["venue", "asset_kind", "asset_id"]
                if relation == "book_updates":
                    columns.append("kind")
                for batch in pq.ParquetFile(
                    store.capture_path(capture) / (relation + ".parquet")
                ).iter_batches(batch_size=2048, columns=columns, use_threads=False):
                    for row in batch.to_pylist():
                        if relation == "book_updates" and row["kind"] not in (
                            "book",
                            "price_change",
                            "tick_size_change",
                        ):
                            continue
                        key = row["venue"], row["asset_kind"], row["asset_id"]
                        receipt = observed.setdefault(key, {"observations": 0, "snapshots": 0})
                        receipt["observations"] += 1
                        receipt["snapshots"] += row.get("kind") == "book"
        sources[source] = {
            "acquisition_id": acquisition_id,
            "captures": len(captures),
            "relation_rows": counts,
            "nonempty_observations": bool(observed),
            "observed_assets": [
                {"venue": key[0], "asset_kind": key[1], "asset_id": key[2], **receipt}
                for key, receipt in sorted(observed.items())
            ],
            "complete": True,
        }
    path = journal.path.with_name(journal.run_id + ".report.json")
    report = {
        "run_id": journal.run_id,
        "run_path": str(journal.path),
        "report_path": str(path),
        "stage": journal.data["stage"],
        "plan_id": journal.data["plan_id"],
        "acquisition_ids": list(journal.data["completed_acquisitions"].values()),
        "interrupted_live_acquisitions": journal.data["interrupted_live_acquisitions"],
        "release_id": journal.data["release_id"],
        "sources": sources,
        "requests": journal.data["usage"]["measured_requests"] - before[0],
        "downloaded_bytes": journal.data["usage"]["measured_downloaded_bytes"] - before[1],
        "measured_usage": {
            "requests": journal.data["usage"]["measured_requests"],
            "downloaded_bytes": journal.data["usage"]["measured_downloaded_bytes"],
        },
        "charged_usage": {
            "requests": journal.meter.charged[0],
            "downloaded_bytes": journal.meter.charged[1],
        },
        "accounting_complete": not journal.data["usage"]["reservations"]
        and journal.meter.charged
        == (
            journal.data["usage"]["measured_requests"],
            journal.data["usage"]["measured_downloaded_bytes"],
        ),
        "coverage": manifest.get("coverage", []) if manifest else [],
        "usage": {
            **journal.data["usage"],
            "charged_requests": journal.meter.charged[0],
            "charged_downloaded_bytes": journal.meter.charged[1],
            "remaining_requests": journal.meter.remaining[0],
            "remaining_downloaded_bytes": journal.meter.remaining[1],
            "root_storage_bytes": sum(
                value.stat().st_size for value in store.root.rglob("*") if value.is_file()
            ),
        },
        "limits": store.limits.as_dict(),
        "recovery": {
            "argv": [
                "polymarket-market-data",
                "--root",
                str(store.root),
                "run",
                "--resume-run",
                journal.run_id,
            ],
            "uncertain_reservations_remain_charged": True,
            "live_resume_starts_new_session": True,
        },
        "readiness": "inspect per-asset nonempty evidence; completion alone is not acceptance",
    }
    raw = json_bytes(report)
    store._budget(len(raw))
    atomic_write(path, raw)
    return report


def run(store, args):
    from . import cli

    with store.lock:
        resume_id = getattr(args, "resume_run", None)
        journal = RunJournal(store, resume_id, None if resume_id else _settings(args))
        settings = journal.data["settings"]
        _check_resume(args, settings)
        before = (
            journal.data["usage"]["measured_requests"],
            journal.data["usage"]["measured_downloaded_bytes"],
        )
        store.run_meter = journal.meter
        try:
            if journal.data["stage"] == "complete":
                manifest = store.verify(journal.data["release_id"])
                store.verify()
                return _report(store, journal, manifest, before)
            if not journal.data["plan_id"]:
                bundle = refresh_metadata(
                    store, settings["market_ids"], settings["events_executable"]
                )
                assets = select_assets(bundle, settings["market_ids"])
                journal.data["metadata_sha256"] = bundle.path.name
                journal.data["plan_id"] = store.create_plan(
                    {
                        key: settings[key]
                        for key in (
                            "market_ids",
                            "feeds",
                            "start",
                            "end",
                            "fidelity",
                            "duration",
                            "limits",
                        )
                    }
                    | {"assets": assets, "metadata_sha256": bundle.path.name}
                )
                journal.write()
            plan_id = journal.data["plan_id"]
            for source in settings["feeds"]:
                journal.stage("acquire:" + source)
                acquisition_id = journal.data["acquisitions"].get(source)
                if acquisition_id and _completed(store, acquisition_id):
                    journal.data["completed_acquisitions"][source] = acquisition_id
                    journal.write()
                    continue
                if acquisition_id and source == "clob-ws":
                    if acquisition_id not in journal.data["interrupted_live_acquisitions"]:
                        journal.data["interrupted_live_acquisitions"].append(acquisition_id)
                    acquisition_id = None
                    journal.write()
                if source == "clob-ws" and min(journal.meter.remaining) <= 0:
                    raise BudgetExceeded("live session requires remaining run allowance")
                if not acquisition_id:
                    acquisition_id = store.new_acquisition(plan_id, source)
                    journal.data["acquisitions"][source] = acquisition_id
                    journal.write()
                stage_args = SimpleNamespace(
                    plan=plan_id,
                    action="backfill",
                    resume=acquisition_id,
                    events_executable=settings["events_executable"],
                    events_root=store.root / ".state",
                )
                if source == "clob-history":
                    stage_args.command = "prices"
                    cli._backfill(store, stage_args)
                elif source.startswith("pmxt-"):
                    stage_args.command, stage_args.version = "archive", source[5:]
                    cli._archive(store, stage_args)
                else:
                    # shortcut: a crashed live session consumes its entire grant; upgrade only
                    # when a transport can durably report closure and partial ingress after crashes.
                    allowance = journal.meter.remaining
                    if min(allowance) <= 0:
                        raise BudgetExceeded("live session requires remaining run allowance")
                    token = journal.meter.reserve(*allowance, "live session")
                    stage_meter = RunMeter(
                        replace(
                            store.limits,
                            max_requests=allowance[0],
                            max_download_bytes=allowance[1],
                        ),
                        {},
                    )
                    store.run_meter = stage_meter
                    stage_args.command, stage_args.action = "books", "record"
                    stage_args.resume, stage_args._acquisition_id = None, acquisition_id
                    known_termination = False
                    try:
                        cli._record(store, stage_args)
                        known_termination = True
                    except (ValueError, OSError):
                        known_termination = True
                        raise
                    finally:
                        store.run_meter = journal.meter
                        if known_termination:
                            journal.meter.settle(
                                token,
                                *stage_meter.charged,
                                measured=(
                                    stage_meter.data["measured_requests"],
                                    stage_meter.data["measured_downloaded_bytes"],
                                ),
                            )
                if not _completed(store, acquisition_id):
                    raise ValueError("source stage returned without committed completion")
                journal.data["completed_acquisitions"][source] = acquisition_id
                journal.write()
            if not journal.data["release_id"]:
                journal.stage("build")
                journal.data["release_id"] = cli.build(
                    store, plan_id, list(journal.data["completed_acquisitions"].values())
                )
                journal.write()
            journal.stage("verify")
            manifest = store.verify(journal.data["release_id"])
            journal.stage("publish")
            store.verify()
            pointer = store.root / "published" / (plan_id + ".current.json")
            previous = read_bytes(pointer) if pointer.exists() else None
            try:
                store.publish(journal.data["release_id"])
                journal.stage("verify-root")
                store.verify()
            except BaseException:
                if previous is not None:
                    atomic_write(pointer, previous)
                elif pointer.exists():
                    pointer.unlink()
                    fsync_dir(pointer.parent)
                raise
            journal.stage("complete")
            return _report(store, journal, manifest, before)
        except (ValueError, OSError) as exc:
            _report(store, journal, before=before)
            raise ValueError(
                f"{exc if isinstance(exc, ValueError) else 'local operation failed'}; "
                f"resume run {journal.run_id}; journal {journal.path}"
            ) from None
        finally:
            store.run_meter = None
