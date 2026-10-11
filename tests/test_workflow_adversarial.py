"""Adversarial identity, process, accounting, consumer and restore boundaries."""

import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import httpx
import pyarrow.parquet as pq
import pytest

from polymarket_market_data import acceptance, cli, metadata, producer, runner, smoke
from polymarket_market_data import acquisitions as acquisition_operations
from polymarket_market_data import producer as metadata_producer
from polymarket_market_data.books import UPDATE_SCHEMA, normalize_message
from polymarket_market_data.build import build
from polymarket_market_data.feeds import PRICE_SCHEMA, coverage
from polymarket_market_data.foundation import (
    BudgetExceeded,
    Limits,
    Store,
    digest,
    file_info,
    json_bytes,
    read_json,
)
from polymarket_market_data.metadata import pin_metadata
from polymarket_market_data.runner import RunJournal, RunMeter
from polymarket_market_data.transport import Client

NOW = datetime(2026, 4, 14, 8, tzinfo=UTC)
LIMITS = Limits(8, 1024, 64 * 1024**2, 128, 16 * 1024**2)
CTF = {
    "venue": "polymarket",
    "market_id": "1",
    "asset_kind": "ctf_token",
    "asset_id": "123",
    "clob_token_id": "123",
    "position_id": None,
    "condition_id": "0x" + "a" * 64,
    "outcome_index": 1,
    "outcome_label": "Yes",
}
V2 = CTF | {
    "market_id": "2",
    "asset_kind": "poly_v2_position",
    "clob_token_id": None,
    "position_id": "123",
    "condition_id": "0x" + "b" * 64,
    "outcome_label": "No",
}


def arguments(**changes):
    return SimpleNamespace(
        **{
            "resume_run": None,
            "market_id": ["1", "2"],
            "feed": ["clob-history"],
            "start": NOW.isoformat(),
            "end": (NOW + timedelta(minutes=1)).isoformat(),
            "events_executable": sys.executable,
            **dict.fromkeys(runner.DEFAULTS),
            **LIMITS.as_dict(),
            **changes,
        }
    )


def bundle(assets):
    return SimpleNamespace(
        markets=[{"market_id": market_id} for market_id in ("1", "2")],
        outcomes=assets,
        coverage=[{"market_id": market_id, "status": "found"} for market_id in ("1", "2")],
    )


def pinned(store, directory):
    directory.mkdir()
    metadata = bundle([CTF, V2])
    relations = {
        "markets": [row | {"venue": "polymarket"} for row in metadata.markets],
        "outcomes": metadata.outcomes,
        "coverage": metadata.coverage,
        "identity_history": [],
        "memberships": [],
    }
    files = {}
    for name, rows in relations.items():
        path = directory / (name + ".json")
        path.write_bytes(json_bytes(rows))
        files[path.name] = file_info(path)
    manifest = json_bytes({"contract": "oddsfox.polymarket.metadata.v1", "files": files})
    (directory / "manifest.json").write_bytes(manifest)
    return pin_metadata(store.root, directory, digest(manifest), store=store)


@pytest.mark.parametrize(
    "field,value",
    [
        ("venue", "other"),
        ("market_id", "2"),
        ("asset_kind", "poly_v2_position"),
        ("asset_id", "124"),
        ("condition_id", "0x" + "c" * 64),
        ("outcome_index", 2),
    ],
)
def test_live_refresh_rejects_every_identity_reassignment(tmp_path, monkeypatch, field, value):
    store = Store(tmp_path / "root", cli.CONTRACT, LIMITS)
    refreshed = bundle([CTF | {field: value}, V2])
    monkeypatch.setattr(metadata_producer, "refresh_metadata", lambda *args, **kwargs: refreshed)
    plan = {"assets": [CTF, V2], "market_ids": ["1", "2"], "limits": LIMITS.as_dict()}
    args = SimpleNamespace(events_executable="events")
    with pytest.raises(ValueError, match="identities changed|lacks validated assets"):
        acquisition_operations._refresh_live_metadata(
            store,
            plan,
            args.events_executable,
            client=SimpleNamespace(requests=0, downloaded=0),
        )


def test_live_refresh_accepts_labels_and_order_but_preserves_equal_typed_native_ids(
    tmp_path, monkeypatch
):
    store = Store(tmp_path / "root", cli.CONTRACT, LIMITS)
    refreshed = bundle([V2 | {"outcome_label": "Renamed"}, CTF | {"outcome_label": "Changed"}])
    monkeypatch.setattr(metadata_producer, "refresh_metadata", lambda *args, **kwargs: refreshed)
    plan = {"assets": [CTF, V2], "market_ids": ["1", "2"], "limits": LIMITS.as_dict()}
    args = SimpleNamespace(events_executable="events")
    assert (
        acquisition_operations._refresh_live_metadata(
            store, plan, args.events_executable, client=None
        )
        is refreshed
    )
    assert plan["assets"] == [CTF, V2]


def test_failed_response_close_cannot_refund_unknown_transport_allowance(tmp_path):
    store = Store(tmp_path / "root", cli.CONTRACT, LIMITS)
    journal = RunJournal(store, settings=runner._settings(arguments()))

    class UnclosedResponse(httpx.SyncByteStream):
        def __iter__(self):
            yield b"[]"

        def close(self):
            raise RuntimeError("transport closure failed")

    transport = httpx.MockTransport(lambda _: httpx.Response(200, stream=UnclosedResponse()))
    with Client("clob", LIMITS, run_meter=journal.meter, transport=transport) as client:
        with pytest.raises(RuntimeError, match="closure"):
            client.get_json("https://clob.polymarket.com/prices-history")
    restarted = RunJournal(Store(store.root, cli.CONTRACT), journal.run_id)
    assert restarted.meter.charged == (1, LIMITS.max_response_bytes)
    assert len(restarted.data["usage"]["reservations"]) == 1


def test_closed_partial_response_retry_uses_the_same_durable_lifetime_budget(tmp_path):
    store = Store(tmp_path / "root", cli.CONTRACT, LIMITS)
    journal = RunJournal(store, settings=runner._settings(arguments(max_requests=2)))
    attempts = []

    class PartialResponse(httpx.SyncByteStream):
        def __iter__(self):
            yield b"partial"
            raise httpx.ReadError("interrupted response")

    def dispatch(request):
        persisted = RunJournal(Store(store.root, cli.CONTRACT), journal.run_id)
        attempts.append(persisted.meter.charged)
        if len(attempts) == 1:
            return httpx.Response(200, stream=PartialResponse())
        return httpx.Response(200, content=b"[]")

    with Client(
        "clob",
        LIMITS,
        run_meter=journal.meter,
        transport=httpx.MockTransport(dispatch),
        sleep=lambda _: None,
    ) as client:
        assert client.get_json("https://clob.polymarket.com/prices-history") == []
    assert attempts == [(1, 128), (2, 135)]
    restarted = RunJournal(Store(store.root, cli.CONTRACT), journal.run_id)
    assert restarted.meter.charged == (2, 9)
    assert restarted.data["usage"]["measured_downloaded_bytes"] == 9
    with Client(
        "archive",
        LIMITS,
        run_meter=restarted.meter,
        transport=httpx.MockTransport(lambda _: pytest.fail("reset lifetime request allowance")),
    ) as client:
        with pytest.raises(BudgetExceeded):
            client.get_bytes("https://archive.pmxt.dev/another-source")


@pytest.mark.parametrize("value", [-1, True, 0.5, "1", None])
def test_reservation_rejects_invalid_accounting_before_journal_write(value):
    writes = []
    meter = RunMeter(LIMITS, {}, lambda: writes.append(True))
    with pytest.raises(ValueError):
        meter.reserve(1, value, "invalid")
    assert meter.charged == (0, 0)
    assert writes == []


def test_source_returning_success_without_terminal_checkpoint_cannot_promote(
    released, tmp_path, monkeypatch
):
    store, previous_plan, _, previous_release = released
    pointer = store.root / "published" / (previous_plan + ".current.json")
    previous_pointer = pointer.read_bytes()
    metadata = pinned(store, tmp_path / "metadata")
    monkeypatch.setattr(metadata_producer, "refresh_metadata", lambda *args, **kwargs: metadata)
    calls = []

    def unfinished(store, *, resume_id, **kwargs):
        calls.append(resume_id)
        store.capture(resume_id, "partial", {}, evidence={"observation": "incomplete"})
        return {"complete": True, "acquisition_id": resume_id}

    monkeypatch.setattr(acquisition_operations, "backfill", unfinished)
    with pytest.raises(ValueError, match="without committed completion"):
        runner.run(store, arguments())
    journal_path = next(
        path
        for path in (store.root / ".state" / "runs").glob("*.json")
        if not path.name.endswith(".report.json")
    )
    journal = read_json(journal_path)
    assert journal["completed_acquisitions"] == {}
    assert journal["release_id"] is None
    assert list((store.root / "published").glob("*.current.json")) == [pointer]
    assert pointer.read_bytes() == previous_pointer
    assert store.verify(previous_release)["release_id"] == previous_release
    with pytest.raises(ValueError, match="without committed completion"):
        runner.run(Store(store.root, cli.CONTRACT), arguments(resume_run=journal["run_id"]))
    assert calls == [journal["acquisitions"]["clob-history"]] * 2
    assert pointer.read_bytes() == previous_pointer


@pytest.mark.parametrize("overflow", [False, True])
def test_producer_diagnostics_are_bounded_across_stdout_and_stderr(tmp_path, overflow):
    store = Store(tmp_path / "root", cli.CONTRACT, LIMITS)
    code = (
        "import os; os.write(1, b'a' * 64); os.write(2, b'b' * " + str(65 if overflow else 64) + ")"
    )
    kwargs = {
        "env": producer.environment(tmp_path),
        "directory": store.root / ".state",
        "store": store,
        "timeout": 5,
        "max_diagnostics": 128,
    }
    if overflow:
        with pytest.raises(ValueError, match="diagnostics"):
            producer.bounded_run([sys.executable, "-c", code], **kwargs)
    else:
        result = producer.bounded_run([sys.executable, "-c", code], **kwargs)
        assert result.returncode == 0 and result.stdout == b"a" * 64


def test_timeout_reaps_metadata_child_before_return(tmp_path, monkeypatch):
    store = Store(tmp_path / "root", cli.CONTRACT, LIMITS)
    processes = []
    original = subprocess.Popen

    def tracked(*args, **kwargs):
        child = original(*args, **kwargs)
        processes.append(child)
        return child

    monkeypatch.setattr(producer.subprocess, "Popen", tracked)
    with pytest.raises(ValueError, match="deadline"):
        producer.bounded_run(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            env=producer.environment(tmp_path),
            directory=store.root / ".state",
            store=store,
            timeout=0.1,
        )
    assert len(processes) == 1 and processes[0].returncode is not None


@pytest.mark.parametrize("failure", ["oversized", "symlink"])
def test_producer_output_cannot_escape_temporary_limits(tmp_path, failure):
    store = Store(tmp_path / "root", cli.CONTRACT, LIMITS)
    store.limits = Limits(8, 1024, 1024**2, 128, 64)
    output = store.root / ".state" / "producer"
    output.mkdir()
    action = (
        "target.write_bytes(b'x' * 65)"
        if failure == "oversized"
        else "target.symlink_to(sys.argv[2], target_is_directory=True)"
    )
    script = (
        "import sys, time; from pathlib import Path; "
        "target=Path(sys.argv[1])/'output'; " + action + "; time.sleep(1)"
    )
    with pytest.raises(ValueError, match="temporary storage|symlink"):
        producer.bounded_run(
            [sys.executable, "-c", script, str(output), str(tmp_path)],
            env=producer.environment(tmp_path),
            directory=output,
            store=store,
            timeout=5,
        )


@pytest.fixture
def released(tmp_path):
    store = Store(tmp_path / "root", cli.CONTRACT, LIMITS)
    plan_id = store.create_plan(
        {
            "limits": LIMITS.as_dict(),
            "assets": [CTF, V2],
            "market_ids": ["1", "2"],
            "feeds": ["clob-history", "clob-ws"],
            "start": NOW,
            "end": NOW + timedelta(minutes=1),
        }
    )
    historical = store.new_acquisition(plan_id, "clob-history")
    for unit, prices in (
        ("first", [(1, "0.123456789012345678"), (2, "0.6")]),
        ("revision", [(1, "0.9")]),
    ):
        rows = [
            target
            | {
                "source": "clob-history",
                "source_at": NOW + timedelta(seconds=second),
                "received_at": NOW,
                "ingested_at": NOW,
                "price": Decimal(price),
            }
            for target in (CTF, V2)
            for second, price in prices
        ]
        store.capture(
            historical,
            unit,
            {"price_samples": rows},
            schemas={"price_samples": PRICE_SCHEMA},
            coverage=[
                coverage(target, "clob-history", NOW, NOW + timedelta(minutes=1), "success")
                for target in (CTF, V2)
            ],
        )
    acquisition_operations._finish(store, historical)
    live = store.new_acquisition(plan_id, "clob-ws")
    updates = []
    for stream in ("first", "reconnect"):
        for target in (CTF, V2):
            for sequence, payload in enumerate(
                [
                    {
                        "event_type": "book",
                        "asset_id": "123",
                        "bids": [["0.3", "1"]],
                        "asks": [["0.7", "2"]],
                    },
                    {
                        "event_type": "price_change",
                        "price_changes": [
                            {"asset_id": "123", "side": "BUY", "price": "0.35", "size": "2"}
                        ],
                    },
                ],
                start=1,
            ):
                updates.extend(
                    normalize_message(
                        payload | {"market": target["condition_id"]},
                        targets=[CTF, V2],
                        source="clob-ws",
                        stream=stream,
                        sequence=sequence,
                        received_at=NOW + timedelta(seconds=sequence),
                    )
                )
    store.capture(
        live,
        "streams",
        {"book_updates": updates},
        schemas={"book_updates": UPDATE_SCHEMA},
        coverage=[
            coverage(target, "clob-ws", NOW, NOW + timedelta(minutes=1), "success")
            for target in (CTF, V2)
        ],
    )
    acquisition_operations._finish(store, live)
    release_id = build(store, plan_id, [historical, live])
    store.publish(release_id)
    return store, plan_id, [historical, live], release_id


def test_duckdb_consumers_preserve_typed_ids_capture_revisions_and_coverage_receipts(released):
    store, _, _, release_id = released
    report = acceptance.consumer_check(store, release_id)
    assert report["relations"]["instruments"] == 2
    assert report["relations"]["price_samples"] == 6
    assert report["relations"]["sampled_ohlc"] == 4
    assert report["relations"]["book_updates"] == report["relations"]["book_states"] == 8
    assert report["coverage_receipts"] == [
        ["clob-history", "success", 4],
        ["clob-ws", "success", 2],
    ]
    assert report["coverage_is_additive"] is False


@pytest.mark.parametrize(
    "failure",
    [
        "after-replacement-oserror",
        "after-replacement-interrupt",
        "verify-root",
        "complete",
        "complete-after-write",
        "complete-after-write-rollback-journal",
        "complete-after-write-interrupt-rollback-journal",
        "report",
    ],
)
@pytest.mark.parametrize("prior_pointer", [True, False])
def test_post_publication_failure_restores_same_plan_pointer_and_resumes_without_io(
    released, monkeypatch, failure, prior_pointer
):
    store, plan_id, acquisitions, original_release = released
    pointer = store.root / "published" / (plan_id + ".current.json")
    original_pointer = pointer.read_bytes() if prior_pointer else None
    if not prior_pointer:
        store.remove(pointer)
    historical = store.new_acquisition(plan_id, "clob-history")
    store.capture(
        historical,
        "new-revision",
        {
            "price_samples": [
                target
                | {
                    "source": "clob-history",
                    "source_at": NOW,
                    "received_at": NOW,
                    "ingested_at": NOW,
                    "price": Decimal("0.8"),
                }
                for target in (CTF, V2)
            ]
        },
        schemas={"price_samples": PRICE_SCHEMA},
    )
    acquisition_operations._finish(store, historical)
    queued = build(store, plan_id, [historical, acquisitions[1]])
    assert queued != original_release
    args = arguments(feed=["clob-history", "clob-ws"])
    journal = RunJournal(store, settings=runner._settings(args))
    journal.data.update(
        plan_id=plan_id,
        acquisitions={"clob-history": historical, "clob-ws": acquisitions[1]},
        completed_acquisitions={"clob-history": historical, "clob-ws": acquisitions[1]},
        release_id=queued,
        stage="verify",
    )
    journal.write()
    token = journal.meter.reserve(2, 100, "completed source work")
    journal.meter.settle(token, 2, 100)
    usage = read_json(journal.path)["usage"]
    inventory = {path.name for path in (store.root / "acquisitions").iterdir()}

    def forbidden(*args, **kwargs):
        pytest.fail("publication recovery repeated metadata, acquisition or build work")

    monkeypatch.setattr(metadata_producer, "refresh_metadata", forbidden)
    for name in ("backfill", "archive", "record"):
        monkeypatch.setattr(acquisition_operations, name, forbidden)
    monkeypatch.setattr(runner, "build", forbidden)
    monkeypatch.setattr(store, "new_acquisition", forbidden)
    args.resume_run = journal.run_id

    class Interrupted(BaseException):
        pass

    error = Interrupted if "interrupt" in failure else OSError
    publish, verify = store.publish, store.verify
    atomic_write = store.atomic_write
    completion_written = False

    def failed_final_write(path, raw):
        nonlocal completion_written
        is_complete = path == journal.path and json.loads(raw)["stage"] == "complete"
        is_report = (
            path == journal.path.with_name(journal.run_id + ".report.json")
            and json.loads(raw)["stage"] == "complete"
        )
        if (failure.startswith("complete") and is_complete) or (failure == "report" and is_report):
            if failure.startswith("complete-after-write"):
                atomic_write(path, raw)
                completion_written = True
            raise error("finalization write failed")
        if failure.endswith("rollback-journal") and completion_written and path == journal.path:
            raise OSError("rollback journal write failed")
        return atomic_write(path, raw)

    def failed_publish(release_id):
        publish(release_id)
        assert read_json(pointer)["release_id"] == queued
        raise error("failure after pointer replacement")

    def failed_root_verify(release_id=None):
        result = verify(release_id)
        if release_id is None and pointer.exists() and read_json(pointer)["release_id"] == queued:
            raise OSError("root verification failed after pointer replacement")
        return result

    with monkeypatch.context() as fault:
        if failure == "verify-root":
            fault.setattr(store, "verify", failed_root_verify)
        elif failure.startswith("complete") or failure == "report":
            fault.setattr(store, "atomic_write", failed_final_write)
        else:
            fault.setattr(store, "publish", failed_publish)
        with pytest.raises(Interrupted if error is Interrupted else ValueError):
            runner.run(store, args)

    assert (pointer.read_bytes() if pointer.exists() else None) == original_pointer
    assert store.verify(original_release)["release_id"] == original_release
    interrupted = read_json(journal.path)
    assert interrupted["stage"] == (
        "complete"
        if failure.endswith("rollback-journal")
        else "verify-root"
        if failure in ("verify-root", "complete", "complete-after-write", "report")
        else "publish"
    )
    assert interrupted["release_id"] == queued
    assert interrupted["usage"] == usage
    assert store.run_meter is None
    if error is not Interrupted or failure.endswith("rollback-journal"):
        report_path = journal.path.with_name(journal.run_id + ".report.json")
        assert read_json(report_path)["stage"] == (
            "verify-root" if failure.endswith("rollback-journal") else interrupted["stage"]
        )

    report = runner.run(store, args)
    assert report["stage"] == "complete" and report["release_id"] == queued
    assert report["requests"] == report["downloaded_bytes"] == 0
    assert read_json(pointer)["release_id"] == queued
    assert read_json(journal.path)["usage"] == usage
    assert {path.name for path in (store.root / "acquisitions").iterdir()} == inventory
    assert store.verify()["releases"]


@pytest.mark.parametrize(
    "active_pointer",
    ["later-release", "absent", "missing-report", "mismatched-report", "invalid-report"],
)
def test_completed_resume_does_not_change_subsequent_publication(
    released, monkeypatch, active_pointer
):
    store, plan_id, acquisitions, original_release = released
    args = arguments(feed=["clob-history", "clob-ws"])
    journal = RunJournal(store, settings=runner._settings(args))
    journal.data.update(
        plan_id=plan_id,
        acquisitions=dict(zip(args.feed, acquisitions, strict=True)),
        completed_acquisitions=dict(zip(args.feed, acquisitions, strict=True)),
        release_id=original_release,
        stage="complete",
    )
    journal.write()
    runner._report(store, journal, store.verify(original_release), (0, 0))
    report_path = journal.path.with_name(journal.run_id + ".report.json")
    if active_pointer == "missing-report":
        store.remove(report_path)
    elif active_pointer == "mismatched-report":
        store.atomic_write(
            report_path, json_bytes(read_json(report_path) | {"run_id": "another-run"})
        )
    if active_pointer == "invalid-report":
        store.atomic_write(report_path, b"[]")
    expected_report = report_path.read_bytes() if report_path.exists() else None
    pointer = store.root / "published" / (plan_id + ".current.json")
    if active_pointer != "absent":
        later_acquisition = store.new_acquisition(plan_id, "clob-history")
        first = next(
            capture
            for capture in store.committed_captures(acquisition_id=acquisitions[0])
            if "price_samples.parquet" in capture["files"]
        )
        store.capture(
            later_acquisition,
            "later",
            {"price_samples": store.read_relation(first, "price_samples")},
            schemas={"price_samples": PRICE_SCHEMA},
        )
        acquisition_operations._finish(store, later_acquisition)
        later_release = build(store, plan_id, [later_acquisition, acquisitions[1]])
        assert later_release != original_release
        store.publish(later_release)
        expected_pointer = pointer.read_bytes()
    else:
        store.remove(pointer)
        expected_pointer = None

    def forbidden(*args, **kwargs):
        pytest.fail("completed resume must only verify and report its immutable result")

    monkeypatch.setattr(store, "publish", forbidden)
    monkeypatch.setattr(store, "new_acquisition", forbidden)
    monkeypatch.setattr(metadata_producer, "refresh_metadata", forbidden)
    monkeypatch.setattr(runner, "build", forbidden)
    args.resume_run = journal.run_id
    if active_pointer in ("missing-report", "mismatched-report", "invalid-report"):
        for _ in range(2):
            with pytest.raises(ValueError):
                runner.run(store, args)
            assert pointer.read_bytes() == expected_pointer
            assert (report_path.read_bytes() if report_path.exists() else None) == expected_report
            assert read_json(journal.path)["stage"] == "complete"
        return
    report = runner.run(store, args)
    assert report["stage"] == "complete" and report["release_id"] == original_release
    assert report["requests"] == report["downloaded_bytes"] == 0
    assert (pointer.read_bytes() if pointer.exists() else None) == expected_pointer


def modified_release(store, release_id, modify):
    manifest = store.verify(release_id)
    directory = store.root / "published" / release_id
    tables = {path.stem: pq.read_table(path) for path in directory.glob("*.parquet")}
    rows = {name: table.to_pylist() for name, table in tables.items()}
    modify(rows)
    captures = [
        capture
        for capture in store.committed_captures()
        if capture["capture_id"] in manifest["inputs"]
    ]
    return store.build_release(
        manifest["plan_id"],
        captures,
        rows,
        manifest["coverage"],
        schemas={name: table.schema for name, table in tables.items()},
    )


def inconsistent_release(store, release_id, relation, field, value):
    return modified_release(
        store, release_id, lambda rows: rows[relation][0].update({field: value})
    )


@pytest.mark.parametrize(
    "relation,field,value,match",
    [
        ("price_samples", "market_id", "99", "identity"),
        ("sampled_ohlc", "outcome_index", 2, "identity"),
        ("book_updates", "asset_kind", "other", "identity"),
        ("book_states", "condition_id", "0x" + "c" * 64, "identity"),
        ("coverage", "asset_id", "124", "identity"),
        ("sampled_ohlc", "open", Decimal("0.123456789012345679"), "OHLC"),
        ("sampled_ohlc", "open", None, "OHLC"),
        ("sampled_ohlc", "high", None, "OHLC"),
        ("sampled_ohlc", "low", None, "OHLC"),
        ("sampled_ohlc", "close", None, "OHLC"),
        ("sampled_ohlc", "observed_samples", 999, "OHLC"),
        ("sampled_ohlc", "observed_samples", None, "OHLC"),
        ("sampled_ohlc", "bucket_end", NOW + timedelta(seconds=61), "OHLC"),
        ("sampled_ohlc", "bucket_end", None, "OHLC"),
        ("book_states", "stream", "other", "alignment"),
    ],
)
def test_consumer_queries_reject_sealed_but_semantically_wrong_relations(
    released, relation, field, value, match
):
    store, _, _, release_id = released
    wrong = inconsistent_release(store, release_id, relation, field, value)
    with pytest.raises(ValueError, match=match):
        acceptance.consumer_check(store, wrong)


@pytest.mark.parametrize(
    "relations,match",
    [
        (("sampled_ohlc",), "OHLC grain"),
        (("book_updates",), "position is duplicated"),
        (("book_states",), "position is duplicated"),
        (("book_updates", "book_states"), "position is duplicated"),
    ],
)
def test_consumer_queries_reject_duplicate_derived_grain(released, relations, match):
    store, _, _, release_id = released

    def duplicate(rows):
        for relation in relations:
            rows[relation].append(dict(rows[relation][0]))

    wrong = modified_release(store, release_id, duplicate)
    with pytest.raises(ValueError, match=match):
        acceptance.consumer_check(store, wrong)


def smoke_readiness(monkeypatch, released, case):
    store, plan_id, _, release_id = released
    assets = store.load_plan(plan_id)["assets"]
    directory = store.root / "published" / release_id
    tables = {
        name: pq.read_table(directory / (name + ".parquet")).to_pylist()
        for name in ("price_samples", "book_updates", "book_states")
    }
    if case == "archive":
        for name in ("book_updates", "book_states"):
            tables[name] = [
                row | {"source": feed} for feed in ("pmxt-v1", "pmxt-v2") for row in tables[name]
            ]
    monkeypatch.setattr(metadata, "load_metadata", lambda *args: bundle(assets))
    monkeypatch.setattr(metadata, "select_assets", lambda *args: assets)
    monkeypatch.setattr(
        pq,
        "read_table",
        lambda path: SimpleNamespace(to_pylist=lambda: tables[Path(path).stem]),
    )
    commands, checks = [], []

    def command(report, name, argv):
        commands.append(name)
        if name == "restored_replay":
            assert Path(argv[argv.index("--root") + 1]) == store.root.with_name(
                store.root.name + "-restored"
            )
            assert argv[argv.index("--plan") + 1] == plan_id
        return {
            "plan_id": plan_id,
            "release_id": release_id,
            "run_id": "synthetic",
            "requests": 0,
            "downloaded_bytes": 0,
            "limits": store.load_plan(plan_id)["limits"],
        }

    def consumer(*args):
        assert args[0].limits == Limits.from_dict(store.load_plan(plan_id)["limits"])
        checks.append("consumer")
        return {"checked": True}

    def restore(*args):
        checks.append("restore")
        return {"checked": True}

    async def reconnect(*args):
        checks.append("reconnect")

    monkeypatch.setattr(smoke, "command", command)
    monkeypatch.setattr(acceptance, "consumer_check", consumer)
    monkeypatch.setattr(acceptance, "restore_check", restore)
    monkeypatch.setattr(smoke, "reconnect_check", reconnect)
    source = {"metadata": "synthetic", "metadata_sha256": "synthetic", "market_id": "1"}

    def run():
        report = {"steps": []}
        smoke.run(store.root, source, 1, report, sys.executable, case=case)
        return report

    return run, tables, commands, checks


@pytest.mark.parametrize("case", ["active", "archive"])
def test_smoke_readiness_accepts_nonempty_valid_evidence_for_every_typed_asset(
    released, monkeypatch, case
):
    run, _, commands, checks = smoke_readiness(monkeypatch, released, case)
    report = run()
    assert report["case"] == case
    assert commands == ["run", "resume_run", "replay", "restored_replay"]
    assert checks == (["reconnect"] if case == "active" else []) + [
        "consumer",
        "restore",
        "consumer",
    ]


@pytest.mark.parametrize("case", ["active", "archive"])
def test_smoke_rejects_different_release_from_replay_in_restored_root(released, monkeypatch, case):
    run, _, commands, _ = smoke_readiness(monkeypatch, released, case)
    command = smoke.command

    def drifted(report, name, argv):
        result = command(report, name, argv)
        return result | {"release_id": "different"} if name == "restored_replay" else result

    monkeypatch.setattr(smoke, "command", drifted)
    with pytest.raises(smoke.SmokeFailure, match="restored semantic replay drift"):
        run()
    assert commands == ["run", "resume_run", "replay", "restored_replay"]


@pytest.mark.parametrize(
    "relation,match",
    [
        ("price_samples", "history missing samples"),
        ("book_updates", "missing authoritative live snapshot"),
        ("book_states", "missing valid displayed live book state"),
    ],
)
def test_active_smoke_readiness_cannot_hide_one_typed_asset_behind_another(
    released, monkeypatch, relation, match
):
    run, tables, _, checks = smoke_readiness(monkeypatch, released, "active")
    for row in tables[relation]:
        if row["asset_kind"] == "poly_v2_position":
            if relation == "book_states":
                row["status"] = "crossed"
            elif relation == "book_updates" and row["kind"] == "book":
                row["kind"] = "reset"
    if relation == "price_samples":
        tables[relation] = [row for row in tables[relation] if row["asset_kind"] == "ctf_token"]
    with pytest.raises(smoke.SmokeFailure, match=match):
        run()
    assert checks == []


@pytest.mark.parametrize("feed", ["pmxt-v1", "pmxt-v2"])
@pytest.mark.parametrize("failure", ["resets_only", "no_valid_state"])
def test_archive_smoke_readiness_requires_nonreset_updates_and_valid_states_per_asset_and_feed(
    released, monkeypatch, feed, failure
):
    run, tables, _, checks = smoke_readiness(monkeypatch, released, "archive")
    relation = "book_updates" if failure == "resets_only" else "book_states"
    for row in tables[relation]:
        if row["source"] == feed and row["asset_kind"] == "poly_v2_position":
            if failure == "resets_only":
                row["kind"] = "reset"
            else:
                row["status"] = "unknown"
    message = (
        "missing selected archive observations"
        if failure == "resets_only"
        else "missing reconstructed valid book state"
    )
    with pytest.raises(smoke.SmokeFailure, match=feed + " " + message):
        run()
    assert checks == []


def test_whole_root_restore_verifies_and_replays_exact_consumer_evidence(released, tmp_path):
    store, plan_id, acquisitions, release_id = released
    original = acceptance.consumer_check(store, release_id)
    journal = RunJournal(store, settings=runner._settings(arguments()))
    token = journal.meter.reserve(1, 128, "interrupted request")
    destination = tmp_path / "restored"
    report = acceptance.restore_check(store, destination)
    assert report["status"]["releases"] == [release_id]
    restored = Store(destination, cli.CONTRACT)
    assert acceptance.consumer_check(restored, release_id) == original
    assert build(restored, plan_id, acquisitions) == release_id
    assert (
        RunJournal(restored, journal.run_id).data["usage"]["reservations"][token][
            "downloaded_bytes"
        ]
        == 128
    )
    assert store.verify(release_id)["release_id"] == release_id


@pytest.mark.parametrize("existing", ["directory", "symlink", "symlink-parent"])
def test_restore_refuses_unsafe_or_existing_destination(released, tmp_path, existing):
    store, _, _, _ = released
    destination = tmp_path / "restore"
    if existing == "directory":
        destination.mkdir()
    else:
        destination.symlink_to(store.root, target_is_directory=True)
        if existing == "symlink-parent":
            destination /= "nested"
    with pytest.raises(ValueError, match="fresh regular destination"):
        acceptance.restore_check(store, destination)


def test_restore_detects_copy_corruption_before_reporting_success(released, tmp_path, monkeypatch):
    store, _, _, release_id = released
    original = acceptance.shutil.copytree

    def corrupt(*args, **kwargs):
        output = Path(original(*args, **kwargs))
        if Path(args[0]) == store.root:
            (output / "published" / release_id / "price_samples.parquet").write_bytes(
                b"corrupt copy"
            )
        return output

    monkeypatch.setattr(acceptance.shutil, "copytree", corrupt)
    with pytest.raises(ValueError, match="inventory differs"):
        acceptance.restore_check(store, tmp_path / "restore")
    assert store.verify(release_id)["release_id"] == release_id
