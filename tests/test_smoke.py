import json
import subprocess

import pytest

from polymarket_market_data import smoke


def test_failed_command_keeps_stdout_accounting(monkeypatch, tmp_path):
    result = subprocess.CompletedProcess(
        [], 2, '{"http_attempts": 2, "downloaded_bytes": 17}', "warning"
    )
    monkeypatch.setattr(smoke.subprocess, "run", lambda *a, **k: result)
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
    monkeypatch.setattr(smoke.subprocess, "run", lambda *a, **k: result)
    report = {"steps": []}
    with pytest.raises(smoke.SmokeFailure, match="rejected"):
        smoke.command(report, "scan", ["unused"])
    result.stdout, result.stderr, result.returncode = "[]", "", 0
    with pytest.raises(smoke.SmokeFailure, match="invalid JSON"):
        smoke.command(report, "scan", ["unused"])


def test_timeout_does_not_claim_complete_accounting(monkeypatch, tmp_path):
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired("unused", 180)

    monkeypatch.setattr(smoke.subprocess, "run", timeout)
    report = {"steps": []}
    with pytest.raises(subprocess.TimeoutExpired) as error:
        smoke.command(report, "scan", ["unused"])
    smoke.finish(tmp_path, report, error.value)
    assert report["accounting_complete"] is False
    assert report["error"] == "TimeoutExpired"


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
