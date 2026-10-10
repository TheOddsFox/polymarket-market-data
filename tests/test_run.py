"""Whole-run grants survive retries, resumptions, subprocess failures and publication faults."""

import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_market_data import cli, runner
from polymarket_market_data.archives import V1_SCHEMA, V2_SCHEMA
from polymarket_market_data.books import normalize_message
from polymarket_market_data.feeds import coverage
from polymarket_market_data.foundation import (
    BudgetExceeded,
    Limits,
    Store,
    atomic_write,
    digest,
    json_bytes,
    read_json,
)
from polymarket_market_data.producer import bounded_run, environment
from polymarket_market_data.runner import RunJournal, RunMeter
from polymarket_market_data.transport import Client, SourceError

NOW = datetime(2025, 1, 1, tzinfo=UTC)
TARGET = {
    "venue": "polymarket",
    "market_id": "1",
    "asset_kind": "ctf_token",
    "asset_id": "123",
    "clob_token_id": "123",
    "position_id": None,
    "condition_id": "0x" + "a" * 64,
    "outcome_index": 1,
    "usable": True,
}


def arguments(**overrides):
    return SimpleNamespace(
        **{
            "market_id": ["1"],
            "feed": ["clob-history"],
            "start": NOW.isoformat(),
            "end": (NOW + timedelta(hours=1)).isoformat(),
            "events_executable": sys.executable,
            "resume_run": None,
            **dict.fromkeys(runner.DEFAULTS),
            **overrides,
        }
    )


def write_metadata(path):
    path.mkdir()
    relations = {
        "markets.json": [{"venue": "polymarket", "market_id": "1", "usable": True}],
        "outcomes.json": [TARGET],
        "memberships.json": [],
        "identity_history.json": [],
        "coverage.json": [{"market_id": "1", "status": "found"}],
    }
    files = {}
    for name, rows in relations.items():
        raw = json_bytes(rows)
        atomic_write(path / name, raw)
        files[name] = {"size": len(raw), "sha256": digest(raw)}
    raw = json_bytes({"contract": "oddsfox.polymarket.metadata.v1", "files": files})
    atomic_write(path / "manifest.json", raw)
    return digest(raw)


@pytest.fixture
def producer(monkeypatch):
    calls = []

    def invoke(argv, **kwargs):
        calls.append((argv, kwargs))
        sha = write_metadata(Path(argv[argv.index("--output") + 1]))
        return subprocess.CompletedProcess(
            argv,
            0,
            json_bytes({"manifest_sha256": sha, "http_attempts": 1, "downloaded_bytes": 100}),
        )

    monkeypatch.setattr(runner, "bounded_run", invoke)
    return calls


def mock_client(monkeypatch, handler):
    monkeypatch.setattr(
        cli,
        "Client",
        lambda *args, **kwargs: Client(
            *args, **kwargs, transport=httpx.MockTransport(handler), sleep=lambda _: None
        ),
    )


def test_run_all_free_sources_then_completed_resume_is_offline(tmp_path, monkeypatch, producer):
    archives = {}
    for version, schema in (("v1", V1_SCHEMA), ("v2", V2_SCHEMA)):
        source = (
            {
                "timestamp_received": NOW,
                "timestamp_created_at": NOW,
                "market_id": TARGET["condition_id"],
                "update_type": "book_snapshot",
                "data": '{"token_id":"123","bids":[],"asks":[]}',
            }
            if version == "v1"
            else {
                "timestamp_received": NOW,
                "timestamp": NOW,
                "market": TARGET["condition_id"].encode(),
                "event_type": "book",
                "asset_id": "123",
                "bids": "[]",
                "asks": "[]",
            }
        )
        path = tmp_path / (version + ".parquet")
        pq.write_table(pa.Table.from_pylist([source], schema=schema), path)
        archives[version] = path.read_bytes()
    requests = []

    def respond(request):
        requests.append(request)
        if request.url.host == "clob.polymarket.com":
            return httpx.Response(200, json={"history": [{"t": int(NOW.timestamp()), "p": "0.4"}]})
        return httpx.Response(
            200, content=archives["v2" if request.url.host == "r2v2.pmxt.dev" else "v1"]
        )

    mock_client(monkeypatch, respond)

    async def recording(**kwargs):
        kwargs["charge_request"]()
        payload = {
            "event_type": "book",
            "asset_id": "123",
            "timestamp": str(int(NOW.timestamp() * 1000)),
            "bids": [],
            "asks": [],
        }
        kwargs["charge_download"](len(json_bytes(payload)))
        rows = normalize_message(
            payload,
            targets=kwargs["targets"],
            source="clob-ws",
            stream="test-session",
            sequence=1,
            received_at=NOW,
        )
        await kwargs["emit"](
            rows,
            {"kind": "book"},
            [coverage(TARGET, "clob-ws", NOW, NOW + timedelta(seconds=60), "success")],
        )

    monkeypatch.setattr(cli, "record", recording)
    store = Store(tmp_path / "root", cli.CONTRACT)
    report = runner.run(store, arguments(feed=list(runner.FREE_FEEDS)))
    assert set(report["sources"]) == set(runner.FREE_FEEDS)
    assert all(value["nonempty_observations"] for value in report["sources"].values())
    assert report["stage"] == "complete" and report["requests"] == 6
    assert report["measured_usage"] == report["charged_usage"]
    assert len(requests) == 3 and len(producer) == 2
    resume = arguments(
        **dict.fromkeys(("market_id", "feed", "start", "end", "events_executable")),
        resume_run=report["run_id"],
    )
    resumed = runner.run(Store(store.root, cli.CONTRACT), resume)
    assert resumed["release_id"] == report["release_id"]
    assert resumed["requests"] == resumed["downloaded_bytes"] == 0
    assert len(requests) == 3 and len(producer) == 2
    store.verify()


def test_journal_unknown_reservations_survive_restart_without_reset(tmp_path):
    store = Store(tmp_path / "root", cli.CONTRACT)
    journal = RunJournal(store, settings=runner._settings(arguments(max_requests=3)))
    first = journal.meter.reserve(1, 100, "HTTP attempt")
    journal.meter.settle(first, 1, 12)
    journal.meter.reserve(2, 300, "interrupted producer")
    restarted = RunJournal(Store(store.root, cli.CONTRACT), journal.run_id)
    assert restarted.meter.remaining[0] == 0
    assert restarted.meter.charged == (3, 312)
    assert restarted.data["usage"]["measured_requests"] == 1
    with pytest.raises(BudgetExceeded):
        restarted.meter.reserve(1, 0, "extra")


def test_http_retry_attempts_and_failed_bytes_share_run_accounting():
    data = {}
    meter = RunMeter(Limits(3, 1000, 10000, 100, 100), data)
    seen = []

    def respond(request):
        seen.append(meter.charged)
        return httpx.Response(503, content=b"failed")

    with Client(
        "clob",
        meter.limits,
        run_meter=meter,
        transport=httpx.MockTransport(respond),
        sleep=lambda _: None,
    ) as client:
        with pytest.raises(SourceError):
            client.get_json("https://clob.polymarket.com/prices-history")
    assert seen == [(1, 100), (2, 106), (3, 112)]
    assert meter.charged == (3, 18)
    assert data["measured_downloaded_bytes"] == 18


def test_unknown_http_failure_keeps_download_grant_charged():
    meter = RunMeter(Limits(3, 1000, 10000, 100, 100), {})

    def crash(request):
        raise RuntimeError("unknown transport state")

    with Client(
        "clob", meter.limits, run_meter=meter, transport=httpx.MockTransport(crash)
    ) as client:
        with pytest.raises(RuntimeError):
            client.get_json("https://clob.polymarket.com/prices-history")
    assert meter.charged == (1, 100)
    assert meter.data["measured_requests"] == 0


def test_download_limit_crossing_is_recorded_without_clamping():
    meter = RunMeter(Limits(3, 5, 10000, 100, 100), {})
    with Client(
        "clob",
        meter.limits,
        run_meter=meter,
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b"0123456789")),
    ) as client:
        with pytest.raises(BudgetExceeded):
            client.get_bytes("https://clob.polymarket.com/prices-history")
    assert meter.charged == (1, 10)
    assert meter.data["measured_downloaded_bytes"] == 10


@pytest.mark.parametrize("change", [{"feed": ["pmxt-v1"]}, {"duration": 2}, {"max_requests": 2}])
def test_resume_rejects_changed_inputs_before_io(tmp_path, monkeypatch, change):
    store = Store(tmp_path / "root", cli.CONTRACT)
    journal = RunJournal(store, settings=runner._settings(arguments()))
    monkeypatch.setattr(runner, "refresh_metadata", lambda *args, **kwargs: pytest.fail("IO"))
    with pytest.raises(ValueError, match="resume"):
        runner.run(store, arguments(resume_run=journal.run_id, **change))


def test_history_completion_recovery_does_not_repeat_http(tmp_path, monkeypatch, producer):
    requests = []
    mock_client(
        monkeypatch,
        lambda request: requests.append(request) or httpx.Response(200, json={"history": []}),
    )
    original = RunJournal.write
    crashed = False

    def crash_after_completion(journal):
        nonlocal crashed
        if journal.data["completed_acquisitions"] and not crashed:
            crashed = True
            raise RuntimeError("process stopped before journal settlement")
        original(journal)

    monkeypatch.setattr(RunJournal, "write", crash_after_completion)
    store = Store(tmp_path / "root", cli.CONTRACT)
    with pytest.raises(RuntimeError):
        runner.run(store, arguments())
    run_id = next((store.root / ".state" / "runs").glob("*.json")).stem
    monkeypatch.setattr(RunJournal, "write", original)
    report = runner.run(store, arguments(resume_run=run_id))
    assert report["stage"] == "complete" and len(requests) == len(producer) == 1
    assert report["requests"] == 0
    assert not report["sources"]["clob-history"]["nonempty_observations"]


def test_failed_metadata_without_diagnostics_keeps_full_grant(tmp_path, monkeypatch):
    store = Store(tmp_path / "root", cli.CONTRACT)
    journal = RunJournal(store, settings=runner._settings(arguments(max_requests=7)))
    store.run_meter = journal.meter
    monkeypatch.setattr(
        runner, "bounded_run", lambda *args, **kwargs: subprocess.CompletedProcess([], 2, b"")
    )
    with pytest.raises(ValueError, match="accounting"):
        runner.refresh_metadata(store, ["1"], "events")
    assert journal.meter.charged == (7, 4 * 1024**3)


def test_failed_metadata_with_diagnostics_settles_measured_usage(tmp_path, monkeypatch):
    store = Store(tmp_path / "root", cli.CONTRACT)
    journal = RunJournal(store, settings=runner._settings(arguments(max_requests=7)))
    store.run_meter = journal.meter
    monkeypatch.setattr(
        runner,
        "bounded_run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 2, json_bytes({"http_attempts": 2, "downloaded_bytes": 17})
        ),
    )
    with pytest.raises(ValueError, match="failed or busy"):
        runner.refresh_metadata(store, ["1"], "events")
    assert journal.meter.charged == (2, 17)


def test_metadata_environment_clears_ambient_configuration_and_paid_key(tmp_path, monkeypatch):
    monkeypatch.setenv("CATALOGUE_CONFIG", "/unsafe.toml")
    monkeypatch.setenv("CATALOGUE_GAMMA_BASE_URL", "https://unsafe.example")
    monkeypatch.setenv("CATALOGUE_FAULT", "kill")
    monkeypatch.setenv("PMXT_API_KEY", "paid-secret")
    env = environment(tmp_path)
    assert "PMXT_API_KEY" not in env
    assert set(key for key in env if key.startswith("CATALOGUE_")) == {
        "CATALOGUE_ROOT",
        "CATALOGUE_PATHS_DATA_DIR",
        "CATALOGUE_PATHS_STATE_DIR",
    }
    assert env["CATALOGUE_ROOT"] == str(tmp_path)


@pytest.mark.parametrize(
    "script,timeout,match",
    [
        ("import sys; sys.stdout.write('x'*100000)", 5, "diagnostics"),
        ("import time; time.sleep(3)", 0.1, "deadline"),
    ],
)
def test_bounded_producer_stops_on_output_or_deadline(tmp_path, script, timeout, match):
    store = Store(tmp_path / "root", cli.CONTRACT, Limits(3, 1000, 1000000, 10000, 100000))
    with pytest.raises(ValueError, match=match):
        bounded_run(
            [sys.executable, "-c", script],
            env=os.environ.copy(),
            directory=store.root / ".state",
            store=store,
            timeout=timeout,
            max_diagnostics=1024,
        )


def test_publication_failure_is_resumable_without_new_acquisition(tmp_path, monkeypatch, producer):
    requests = []
    mock_client(
        monkeypatch,
        lambda request: requests.append(request) or httpx.Response(200, json={"history": []}),
    )
    store = Store(tmp_path / "root", cli.CONTRACT)
    original = Store.publish
    monkeypatch.setattr(Store, "publish", lambda *args: (_ for _ in ()).throw(OSError("fault")))
    with pytest.raises(ValueError, match="resume run"):
        runner.run(store, arguments())
    run_id = next((store.root / ".state" / "runs").glob("*.report.json")).name.split(".")[0]
    before = read_json(store.root / ".state" / "runs" / (run_id + ".json"))
    assert before["release_id"] and before["stage"] == "publish"
    assert not list((store.root / "published").glob("*.current.json"))
    monkeypatch.setattr(Store, "publish", original)
    report = runner.run(store, arguments(resume_run=run_id))
    assert len(requests) == len(producer) == 1
    assert report["release_id"] == before["release_id"]
    assert report["requests"] == 0


def test_paid_feed_is_rejected_before_metadata(tmp_path, monkeypatch):
    store = Store(tmp_path / "root", cli.CONTRACT)
    monkeypatch.setattr(runner, "refresh_metadata", lambda *args, **kwargs: pytest.fail("IO"))
    with pytest.raises(ValueError, match="free feeds"):
        runner.run(store, arguments(feed=["pmxt-trades"]))


def test_lifetime_request_exhaustion_cannot_be_reset_by_resume(tmp_path, monkeypatch, producer):
    requests = []
    mock_client(
        monkeypatch,
        lambda request: requests.append(request) or httpx.Response(200, json={"history": []}),
    )
    store = Store(tmp_path / "root", cli.CONTRACT)
    args = arguments(max_requests=2, end=(NOW + timedelta(days=2)).isoformat())
    with pytest.raises(ValueError, match="allowance exhausted"):
        runner.run(store, args)
    run_id = next((store.root / ".state" / "runs").glob("*.report.json")).name.split(".")[0]
    with pytest.raises(ValueError, match="allowance exhausted"):
        runner.run(
            store,
            arguments(
                **dict.fromkeys(("market_id", "feed", "start", "end", "events_executable")),
                resume_run=run_id,
            ),
        )
    report = read_json(store.root / ".state" / "runs" / (run_id + ".report.json"))
    assert report["charged_usage"]["requests"] == 2
    assert len(requests) == len(producer) == 1
    assert not list((store.root / "published").glob("*.current.json"))


def test_known_live_interruption_preserves_session_and_starts_new_id(
    tmp_path, monkeypatch, producer
):
    sessions = []

    def recording(store, args):
        sessions.append(args._acquisition_id)
        token = store.run_meter.reserve(1, 0, "WS attempt")
        store.run_meter.settle(token, 1, 0)
        store.run_meter.charge_bytes(10)
        if len(sessions) == 1:
            store.capture(args._acquisition_id, "partial-session", {})
            raise OSError("connection terminated")
        return cli._finish(store, args._acquisition_id)

    monkeypatch.setattr(cli, "_record", recording)
    store = Store(tmp_path / "root", cli.CONTRACT)
    with pytest.raises(ValueError, match="resume run"):
        runner.run(store, arguments(feed=["clob-ws"]))
    run_id = next((store.root / ".state" / "runs").glob("*.report.json")).name.split(".")[0]
    report = runner.run(
        store,
        arguments(
            **dict.fromkeys(("market_id", "feed", "start", "end", "events_executable")),
            resume_run=run_id,
        ),
    )
    assert len(sessions) == 2 and sessions[0] != sessions[1]
    assert report["interrupted_live_acquisitions"] == sessions[:1]
    assert report["acquisition_ids"] == sessions[1:]
    assert (
        report["measured_usage"]
        == report["charged_usage"]
        == {
            "requests": 3,
            "downloaded_bytes": 120,
        }
    )
    assert len(producer) == 1


def test_unknown_live_interruption_retains_full_grant(tmp_path, monkeypatch, producer):
    def crash(store, args):
        store.run_meter.charge_bytes(10)
        raise RuntimeError("unobserved transport termination")

    monkeypatch.setattr(cli, "_record", crash)
    store = Store(tmp_path / "root", cli.CONTRACT)
    with pytest.raises(RuntimeError):
        runner.run(store, arguments(feed=["clob-ws"], max_requests=3))
    run_id = next((store.root / ".state" / "runs").glob("*.json")).stem
    with pytest.raises(ValueError, match="remaining run allowance"):
        runner.run(
            store,
            arguments(
                **dict.fromkeys(("market_id", "feed", "start", "end", "events_executable")),
                resume_run=run_id,
            ),
        )
    report = read_json(store.root / ".state" / "runs" / (run_id + ".report.json"))
    assert not report["accounting_complete"]
    assert report["charged_usage"] == {"requests": 3, "downloaded_bytes": 4 * 1024**3}
    assert report["measured_usage"] == {"requests": 1, "downloaded_bytes": 100}
    assert len(producer) == 1


def test_metadata_limit_crossing_records_actual_reported_usage(tmp_path, monkeypatch):
    store = Store(tmp_path / "root", cli.CONTRACT)
    journal = RunJournal(store, settings=runner._settings(arguments(max_requests=7)))
    store.run_meter = journal.meter
    monkeypatch.setattr(
        runner,
        "bounded_run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 2, json_bytes({"http_attempts": 9, "downloaded_bytes": 17})
        ),
    )
    with pytest.raises(BudgetExceeded, match="exceeded reserved"):
        runner.refresh_metadata(store, ["1"], "events")
    restarted = RunJournal(Store(store.root, cli.CONTRACT), journal.run_id)
    assert restarted.meter.charged == (9, 17)
    assert restarted.data["usage"]["measured_requests"] == 9


def test_reset_only_live_capture_is_complete_without_observation_readiness(
    tmp_path, monkeypatch, producer
):
    from polymarket_market_data.books import UPDATE_SCHEMA, resets

    def recording(store, args):
        rows = resets(
            [TARGET],
            source="clob-ws",
            stream="session",
            sequence=0,
            received_at=NOW,
            reason="session_start",
        )
        store.capture(
            args._acquisition_id,
            "reset",
            {"book_updates": rows},
            schemas={"book_updates": UPDATE_SCHEMA},
        )
        return cli._finish(store, args._acquisition_id)

    monkeypatch.setattr(cli, "_record", recording)
    store = Store(tmp_path / "root", cli.CONTRACT)
    report = runner.run(store, arguments(feed=["clob-ws"]))
    source = report["sources"]["clob-ws"]
    assert source["complete"] and source["relation_rows"]["book_updates"] == 1
    assert not source["nonempty_observations"] and source["observed_assets"] == []
