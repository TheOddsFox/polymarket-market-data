import json
import signal
import subprocess
import sys
import time

import pytest

from polymarket_market_data import smoke


def mock_process(monkeypatch, result):
    class Child:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        @property
        def returncode(self):
            return result.returncode

        def communicate(self, timeout=None):
            return result.stdout, result.stderr

    monkeypatch.setattr(smoke.subprocess, "Popen", lambda *a, **k: Child())


def test_failed_command_keeps_stdout_accounting(monkeypatch, tmp_path):
    result = subprocess.CompletedProcess(
        [], 2, '{"http_attempts": 2, "downloaded_bytes": 17}', "warning"
    )
    mock_process(monkeypatch, result)
    report = {"steps": []}
    with pytest.raises(smoke.SmokeFailure):
        smoke.command(report, "lookup", ["unused"])
    assert smoke.finish(tmp_path, report, smoke.SmokeFailure("lookup failed")) == 1
    saved = json.loads((tmp_path / "report.json").read_text())
    assert saved["http_attempts"] == 2
    assert saved["downloaded_bytes"] == 17
    assert saved["accounting_complete"] is False


def test_last_stderr_json_and_invalid_diagnostics(monkeypatch):
    result = subprocess.CompletedProcess([], 1, "", 'started\n{"message":"rejected"}\n')
    mock_process(monkeypatch, result)
    report = {"steps": []}
    with pytest.raises(smoke.SmokeFailure, match="rejected"):
        smoke.command(report, "scan", ["unused"])
    result.stdout, result.stderr, result.returncode = "[]", "", 0
    with pytest.raises(smoke.SmokeFailure, match="invalid JSON"):
        smoke.command(report, "scan", ["unused"])


def test_timeout_does_not_claim_complete_accounting(tmp_path):
    report = {"steps": [], "command_timeout_seconds": 0.1}
    with pytest.raises(subprocess.TimeoutExpired) as error:
        smoke.command(report, "scan", [sys.executable, "-c", "import time; time.sleep(30)"])
    smoke.finish(tmp_path, report, error.value)
    assert report["accounting_complete"] is False
    assert report["error"] == "TimeoutExpired"


def test_timeout_unwinds_cli_and_reaps_isolated_producer(tmp_path):
    marker = tmp_path / "late-write"
    pid = tmp_path / "producer.pid"
    script = tmp_path / "workflow.py"
    script.write_text("""
import sys
from pathlib import Path
from polymarket_market_data import cli, producer
from polymarket_market_data.foundation import Store, Limits
root = Path(sys.argv[1])
def run(args):
    store = Store(root / 'root', 'timeout.test.v1', Limits(10, 100000, 1000000, 100000, 100000))
    workspace = store.root / '.state' / 'producer'
    workspace.mkdir()
    code = 'import os,time; from pathlib import Path; Path(%r).write_text(str(os.getpid())); time.sleep(2); Path(%r).write_text("leaked")' % (str(root / 'producer.pid'), str(root / 'late-write'))
    with store.lock:
        producer.bounded_run([sys.executable, '-c', code], env=producer.environment(workspace, store.limits), directory=workspace, store=store, timeout=30)
cli.run = run
cli.main(['status'])
""")
    report = {"steps": [], "command_timeout_seconds": 1}
    with pytest.raises(subprocess.TimeoutExpired):
        smoke.command(report, "workflow", [sys.executable, str(script), str(tmp_path)])
    assert pid.exists(), "fixture must reach the independently sessioned producer"
    with pytest.raises(ProcessLookupError):
        smoke.os.kill(int(pid.read_text()), 0)
    time.sleep(1.5)
    assert not marker.exists()


def test_timeout_force_kills_and_reaps_sigterm_ignoring_child(tmp_path, monkeypatch):
    pid = tmp_path / "unresponsive.pid"
    code = (
        "import os,signal,time; from pathlib import Path; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        f"Path({str(pid)!r}).write_text(str(os.getpid())); time.sleep(30)"
    )
    signals = []
    killpg = smoke.os.killpg

    def observed_killpg(group, sig):
        signals.append(sig)
        killpg(group, sig)

    monkeypatch.setattr(smoke.os, "killpg", observed_killpg)
    report = {"steps": [], "command_timeout_seconds": 1}
    with pytest.raises(subprocess.TimeoutExpired) as error:
        smoke.command(report, "unresponsive", [sys.executable, "-c", code])
    assert pid.exists(), "fixture must install its SIGTERM handler before timeout"
    assert signals == [signal.SIGTERM, signal.SIGKILL]
    with pytest.raises(ProcessLookupError):
        smoke.os.kill(int(pid.read_text()), 0)
    smoke.finish(tmp_path, report, error.value)
    assert report["accounting_complete"] is False


@pytest.mark.parametrize("outcome", ["terminated", "success", "error"])
def test_cli_restores_sigterm_handler(monkeypatch, outcome):
    from polymarket_market_data import cli

    previous = signal.getsignal(signal.SIGTERM)

    def interrupted(args):
        if outcome == "terminated":
            signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
        if outcome == "error":
            raise ValueError("expected local failure")
        return {"ok": True}

    monkeypatch.setattr(cli, "run", interrupted)
    if outcome == "terminated":
        with pytest.raises(KeyboardInterrupt):
            cli.main(["status"])
    else:
        assert cli.main(["status"]) == (2 if outcome == "error" else 0)
    assert signal.getsignal(signal.SIGTERM) == previous


def test_cancelled_smoke_reaps_session_child_before_propagating(monkeypatch):
    children = []
    process = subprocess.Popen

    class CancelledProcess(process):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.cancelled = False
            children.append(self)

        def communicate(self, *args, **kwargs):
            if not self.cancelled:
                self.cancelled = True
                raise KeyboardInterrupt("operator cancelled smoke")
            return super().communicate(*args, **kwargs)

    monkeypatch.setattr(smoke.subprocess, "Popen", CancelledProcess)
    report = {"steps": []}
    with pytest.raises(KeyboardInterrupt, match="operator cancelled"):
        smoke.command(report, "cancelled", [sys.executable, "-c", "import time; time.sleep(30)"])
    assert len(children) == 1 and children[0].returncode is not None
    with pytest.raises(ProcessLookupError):
        smoke.os.kill(children[0].pid, 0)
    assert "result" not in report["steps"][0]


def test_managed_smoke_report_checks_atomic_peak_and_preserves_existing_report(tmp_path):
    from dataclasses import replace

    from polymarket_market_data.foundation import BudgetExceeded, Limits, Store

    store = Store(tmp_path / "root", "smoke.test.v1", Limits(10, 10**6, 10**6, 10**5, 10**5))
    path = store.root / "report.json"
    store.atomic_write(path, b"previous report")
    with store.lock:
        before = store.retained_bytes()
        store.limits = replace(store.limits, max_storage_bytes=before)
        with pytest.raises(BudgetExceeded, match="retained storage"):
            smoke.finish(store.root, {"steps": []}, store=store)
        assert path.read_bytes() == b"previous report"
        assert store.retained_bytes() == before


def test_output_never_reuses_existing_or_symlink_root(tmp_path):
    with pytest.raises(smoke.SmokeFailure, match="fresh"):
        smoke.new_output(tmp_path)
    link = tmp_path / "link"
    link.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(smoke.SmokeFailure, match="symlink"):
        smoke.new_output(link / "new")
    assert smoke.new_output(tmp_path / "new") == tmp_path / "new"


def test_consumer_report_is_bounded_and_has_correct_producer(tmp_path):
    path = tmp_path / "report.json"
    path.write_bytes(b" " * 1048577)
    with pytest.raises(smoke.SmokeFailure, match="exceeds"):
        smoke.load_report(path)
    path.write_text('{"product":"another-product","status":"passed"}')
    with pytest.raises(smoke.SmokeFailure, match="producer"):
        smoke.load_report(path)


def test_reconnect_requires_new_snapshot_stream(monkeypatch):
    import asyncio

    from polymarket_market_data import recorder

    async def no_snapshot(**kwargs):
        kwargs["charge_request"]()
        kwargs["charge_download"](19)
        await kwargs["emit"]([], {"kind": "connection_gap"}, [])

    monkeypatch.setattr(recorder, "record", no_snapshot)
    report = {"steps": []}
    with pytest.raises(smoke.SmokeFailure, match="fresh snapshots"):
        asyncio.run(smoke.reconnect_check([], report))
    assert report["steps"][0]["result"]["http_attempts"] == 1
    assert report["steps"][0]["result"]["downloaded_bytes"] == 19
    assert "exit_code" not in report["steps"][0]


def test_consumer_rejects_incomplete_and_untyped_identity(tmp_path):
    path = tmp_path / "report.json"
    source = {"product": "polymarket-events", "status": "passed", "accounting_complete": False}
    path.write_text(json.dumps(source))
    with pytest.raises(smoke.SmokeFailure, match="accounting"):
        smoke.load_report(path)
    source["accounting_complete"] = True
    source.update(metadata=[], metadata_sha256="a", market_id="1")
    path.write_text(json.dumps(source))
    with pytest.raises(smoke.SmokeFailure, match="identity"):
        smoke.load_report(path)
