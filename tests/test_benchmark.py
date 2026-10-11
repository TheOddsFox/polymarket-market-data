"""Benchmark evidence must prove work and must never turn an incomplete run green."""

import importlib.util
import json
import os
import signal
import subprocess
import sys
import time
import types
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_market_data import foundation

REPOSITORY = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def benchmark():
    spec = importlib.util.spec_from_file_location(
        "offline_benchmark_under_test", REPOSITORY / "scripts" / "benchmark-offline.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_storage_milestones_independently_keep_capture_inventory_constant(
    benchmark, tmp_path, monkeypatch
):
    measure, capture = foundation.Store._measure, foundation.Store.capture
    observed = {"inside_capture": 0, "capture_walks": 0, "captures": 0}

    def measured(store, path):
        if observed["inside_capture"] and Path(path) == store.root:
            observed["capture_walks"] += 1
        return measure(store, path)

    def captured(store, *args, **kwargs):
        observed["inside_capture"] += 1
        try:
            result = capture(store, *args, **kwargs)
            observed["captures"] += 1
            return result
        finally:
            observed["inside_capture"] -= 1

    monkeypatch.setattr(foundation.Store, "_measure", measured)
    monkeypatch.setattr(foundation.Store, "capture", captured)
    result = benchmark.storage_benchmark(tmp_path / "storage")
    assert observed["captures"] == 400 and observed["capture_walks"] <= 1
    assert result["cold_inventory_root_walks"] == result["operation_root_walks"] == 1
    assert result["next_operation_root_walks"] == 1
    assert result["cold_inventory_seconds"] >= 0
    assert [item["captures"] for item in result["milestones"]] == [0, 100, 200, 400]
    assert [item["new_captures"] for item in result["milestones"]] == [0, 100, 100, 200]
    for item in result["milestones"]:
        assert item["capture_root_walks"] == item["warm_root_walks"] == 0
        assert len(item["warm_inventory_seconds"]) == 7
        assert all(value >= 0 for value in item["warm_inventory_seconds"])
        assert item["capture_seconds"] >= 0 and item["accounting_seconds"] >= 0
        if item["new_captures"]:
            assert item["accounting_calls"] > 0


def test_json_matrix_has_real_mock_requests_and_no_recursive_walks(
    benchmark, tmp_path, monkeypatch
):
    from polymarket_market_data.transport import Client

    request, measure, rglob = Client._request, foundation.Store._measure, Path.rglob
    observed = {"inside_request": 0, "requests": 0, "walks": 0}

    def requested(client, *args, **kwargs):
        observed["inside_request"] += 1
        observed["requests"] += 1
        try:
            return request(client, *args, **kwargs)
        finally:
            observed["inside_request"] -= 1

    def measured(store, path):
        if observed["inside_request"] and Path(path) == store.root:
            observed["walks"] += 1
        return measure(store, path)

    def globbed(path, *args, **kwargs):
        if observed["inside_request"] and path.parent == tmp_path / "http":
            observed["walks"] += 1
        return rglob(path, *args, **kwargs)

    monkeypatch.setattr(Client, "_request", requested)
    monkeypatch.setattr(foundation.Store, "_measure", measured)
    monkeypatch.setattr(Path, "rglob", globbed)
    result = benchmark.http_benchmark(tmp_path / "http")
    assert [(item["mode"], item["unrelated_files"]) for item in result] == [
        ("no-root", 0),
        *((mode, count) for mode in ("unmanaged", "managed") for count in (0, 1200, 5000)),
    ]
    assert observed["requests"] == 7 * 51 and observed["walks"] == 0
    for item in result:
        assert item["root_walks"] == 0 and item["mock_requests"] == 51
        assert item["mock_downloaded_bytes"] == 51 * len(b'{"history":[]}')
        assert len(item["seconds_per_request"]) == 5
        assert all(value >= 0 for value in item["seconds_per_request"])


def test_synthetic_continuation_proves_semantics_rebuild_and_restored_resume(benchmark, tmp_path):
    result = benchmark.synthetic_archive(tmp_path / "synthetic")
    assert result["completed_groups_before_resume"] == [0, 1]
    assert result["resumed_groups"] == [2, 3]
    assert result["decoded_source_rows_on_resume"] == 2
    assert result["resumed_requests"] == result["resumed_downloaded_bytes"] == 0
    assert result["mock_setup_requests"] == 1
    assert result["semantic_equal"] and result["deterministic_rebuild"]
    assert result["completed_resume_without_network_or_decode"]
    assert result["restored_resume_without_network_or_decode"]
    assert set(result["semantic_relations"]) == set(benchmark.RELATIONS)
    assert result["semantic_relations"]["book_updates"]["rows"] == 4
    assert result["semantic_relations"]["book_states"]["rows"] == 4


def test_semantic_hash_keeps_duplicates_and_schema_but_ignores_capture_provenance(
    benchmark, tmp_path
):
    def snapshot(name, values, *, width=64, offset=0):
        directory = tmp_path / name
        directory.mkdir()
        schema = pa.schema(
            [
                ("value", pa.int64() if width == 64 else pa.int32()),
                ("ingested_at", pa.timestamp("ms", tz="UTC")),
                ("acquisition_id", pa.string()),
                ("capture_id", pa.string()),
            ]
        )
        rows = [
            {
                "value": value,
                "ingested_at": datetime(2025, 1, 1, tzinfo=UTC) + timedelta(days=offset),
                "acquisition_id": str(offset),
                "capture_id": str(index + offset),
            }
            for index, value in enumerate(values)
        ]
        for relation in benchmark.RELATIONS:
            pq.write_table(
                pa.Table.from_pylist(rows, schema=schema), directory / f"{relation}.parquet"
            )
        return benchmark.semantic_snapshot(directory)

    first = snapshot("first", [1, 2, 1])
    assert first == snapshot("reordered", [2, 1, 1], offset=1)
    missing_duplicate = snapshot("missing-duplicate", [1, 2])
    different_schema = snapshot("different-schema", [1, 2, 1], width=32)
    for relation in benchmark.RELATIONS:
        assert first[relation]["rows"] == 3
        assert first[relation]["semantic_sha256"] != missing_duplicate[relation]["semantic_sha256"]
        assert first[relation]["schema"] != different_schema[relation]["schema"]


def fake_worker(benchmark, monkeypatch, tmp_path, body):
    script = tmp_path / "fake_worker.py"
    script.write_text(body)
    monkeypatch.setattr(benchmark, "__file__", str(script))
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    payload = {"kind": "http", "root": str(workspace / "root")}
    return workspace, payload


def test_worker_environment_replaces_ambient_source_and_removes_paid_configuration(
    benchmark, tmp_path, monkeypatch
):
    workspace, payload = fake_worker(
        benchmark,
        monkeypatch,
        tmp_path,
        "import json,os\nprint(json.dumps({'peak_rss_bytes':0,'network_attempts':0,'result':dict(os.environ)}))\n",
    )
    for name in ("PYTHONPATH", "PYTHONHOME", "PMXT_API_KEY", "CATALOGUE_ROOT", "CATALOGUE_CONFIG"):
        monkeypatch.setenv(name, "ambient configuration must not reach worker")
    result = benchmark.run_worker(REPOSITORY, workspace, payload, time.monotonic() + 10)
    environment = result["result"]
    assert environment["PYTHONPATH"] == str(REPOSITORY / "src")
    assert not {"PYTHONHOME", "PMXT_API_KEY"} & environment.keys()
    assert not any(name.startswith("CATALOGUE_") for name in environment)


def test_import_verification_rejects_foreign_revision(benchmark, tmp_path, monkeypatch):
    foreign = types.ModuleType("polymarket_market_data.foreign")
    foreign.__file__ = str(tmp_path / "foreign.py")
    monkeypatch.setitem(sys.modules, foreign.__name__, foreign)
    with pytest.raises(ValueError, match="wrong revision"):
        benchmark.verify_imports(REPOSITORY)


@pytest.mark.parametrize("swallow", [False, True])
def test_private_worker_rejects_real_socket_attempt_even_when_operation_catches_it(
    benchmark, tmp_path, swallow
):
    code = f"""
import importlib.util, json, socket
from pathlib import Path
spec = importlib.util.spec_from_file_location('benchmark', {str(REPOSITORY / "scripts" / "benchmark-offline.py")!r})
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
def attempted(root):
    try:
        socket.create_connection(('127.0.0.1', 9), timeout=0.1)
    except ValueError:
        if not {swallow!r}:
            raise
    return {{}}
module.storage_benchmark = attempted
try:
    module.worker({{'kind':'storage','root':{str(tmp_path / "root")!r},'source':{str(REPOSITORY)!r}}})
except ValueError as error:
    print(json.dumps({{'error':str(error)}}))
    raise SystemExit(1)
raise SystemExit(0)
"""
    environment = dict(os.environ, PYTHONPATH=str(REPOSITORY / "src"))
    environment.pop("PYTHONHOME", None)
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 1, result.stderr
    assert "network access" in json.loads(result.stdout)["error"]


def test_private_worker_blocks_network_before_package_dependency_imports(benchmark, tmp_path):
    code = f"""
import builtins, importlib.util, json, socket
spec = importlib.util.spec_from_file_location('benchmark', {str(REPOSITORY / "scripts" / "benchmark-offline.py")!r})
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
original = builtins.__import__
def attempted(name, *args, **kwargs):
    if name == 'polymarket_market_data':
        socket.getaddrinfo('127.0.0.1', 9)
    return original(name, *args, **kwargs)
builtins.__import__ = attempted
try:
    module.worker({{'kind':'storage','root':{str(tmp_path / "root")!r},'source':{str(REPOSITORY)!r}}})
except ValueError as error:
    print(json.dumps({{'error':str(error), 'network_attempts':error.network_attempts}}))
    raise SystemExit(1)
raise SystemExit(0)
"""
    environment = dict(os.environ, PYTHONPATH=str(REPOSITORY / "src"))
    environment.pop("PYTHONHOME", None)
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 1, result.stderr
    report = json.loads(result.stdout)
    assert "network access" in report["error"] and report["network_attempts"] == 1


def test_private_worker_fails_if_package_imports_from_another_selected_revision(tmp_path):
    payload = {
        "kind": "storage",
        "root": str(tmp_path / "root"),
        "source": str(tmp_path / "different-revision"),
    }
    environment = dict(os.environ, PYTHONPATH=str(REPOSITORY / "src"))
    environment.pop("PYTHONHOME", None)
    result = subprocess.run(
        [
            sys.executable,
            str(REPOSITORY / "scripts" / "benchmark-offline.py"),
            "--worker",
            json.dumps(payload),
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 1, result.stderr
    report = json.loads(result.stdout)
    assert report["status"] == "incomplete" and "wrong revision" in report["error"]
    assert not (tmp_path / "root").exists()


@pytest.mark.parametrize("channel", ["stdout", "stderr"])
def test_worker_diagnostics_are_bounded_after_fast_exit(benchmark, tmp_path, monkeypatch, channel):
    workspace, payload = fake_worker(
        benchmark,
        monkeypatch,
        tmp_path,
        f"import sys\nsys.{channel}.write('x' * {benchmark.DIAGNOSTIC_LIMIT + 1})\n",
    )
    with pytest.raises(ValueError, match="diagnostics exceed"):
        benchmark.run_worker(REPOSITORY, workspace, payload, time.monotonic() + 10)


def test_worker_rejects_exhausted_deadline_before_launch(benchmark, tmp_path, monkeypatch):
    monkeypatch.setattr(
        benchmark.subprocess, "Popen", lambda *a, **k: pytest.fail("worker launched")
    )
    with pytest.raises(ValueError, match="deadline exhausted"):
        benchmark.run_worker(
            REPOSITORY,
            tmp_path,
            {"kind": "http", "root": str(tmp_path / "root")},
            time.monotonic() - 1,
        )


@pytest.mark.parametrize("limit", ["retained", "temporary", "rss"])
def test_worker_resource_bounds_apply_without_peak_metric_collection(
    benchmark, tmp_path, monkeypatch, limit
):
    monkeypatch.setattr(benchmark, "STORAGE_LIMIT", 4096)
    monkeypatch.setattr(benchmark, "TEMP_LIMIT", 1024)
    filename = "tmp-excess" if limit == "temporary" else "excess"
    size = 4097 if limit == "retained" else 1025
    body = "import json,pathlib\n"
    if limit != "rss":
        body += f"pathlib.Path({filename!r}).write_bytes(b'x' * {size})\n"
    body += f"print(json.dumps({{'peak_rss_bytes':{1025 if limit == 'rss' else 0},'network_attempts':0,'result':{{}}}}))\n"
    workspace, payload = fake_worker(benchmark, monkeypatch, tmp_path, body)
    with pytest.raises(ValueError, match="exceeds"):
        benchmark.run_worker(REPOSITORY, workspace, payload, time.monotonic() + 10)


@pytest.mark.parametrize("kind", ["http", "comparison"])
def test_timeout_reaps_worker_and_its_descendant(benchmark, tmp_path, monkeypatch, kind):
    workspace, payload = fake_worker(
        benchmark,
        monkeypatch,
        tmp_path,
        "import os,pathlib,subprocess,sys,time\n"
        "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'])\n"
        "pathlib.Path('pids').write_text(str(os.getpid())+' '+str(child.pid))\n"
        "time.sleep(60)\n",
    )
    payload["kind"] = kind
    pids = []
    try:
        with pytest.raises(ValueError, match="timed out"):
            benchmark.run_worker(REPOSITORY, workspace, payload, time.monotonic() + 3)
        pids = list(map(int, (workspace / "pids").read_text().split()))
        for pid in pids:
            status = subprocess.run(
                ["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True, check=False
            ).stdout.strip()
            assert not status or status.startswith("Z"), f"descendant still running: {pid} {status}"
    finally:
        if not pids and (workspace / "pids").exists():
            pids = list(map(int, (workspace / "pids").read_text().split()))
        for pid in pids:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_workspace_accounting_uses_apparent_bytes_and_rejects_symlinks(
    benchmark, tmp_path, monkeypatch
):
    assert benchmark.STORAGE_LIMIT == 16 * 1024**3 and benchmark.TEMP_LIMIT == 2 * 1024**3
    monkeypatch.setattr(benchmark, "STORAGE_LIMIT", 100)
    monkeypatch.setattr(benchmark, "TEMP_LIMIT", 20)
    with (tmp_path / "retained").open("wb") as output:
        output.truncate(80)
    temporary = tmp_path / ".build-1"
    temporary.mkdir()
    (temporary / "spill").write_bytes(b"x" * 20)
    assert benchmark.workspace_usage(tmp_path) == (100, 20)
    (temporary / "spill").write_bytes(b"x" * 21)
    with pytest.raises(ValueError, match="exceeds"):
        benchmark.workspace_usage(tmp_path)
    (temporary / "spill").write_bytes(b"x" * 20)
    (tmp_path / "link").symlink_to(tmp_path / "retained")
    with pytest.raises(ValueError, match="symlink"):
        benchmark.workspace_usage(tmp_path)


@pytest.mark.parametrize(
    "arguments,error",
    [
        (["--repetitions", "0"], "repetitions"),
        (["--cached-repetitions", "1"], "cached repetitions"),
        (["--deadline-seconds", "0"], "deadline"),
        (["--deadline-seconds", "601"], "deadline"),
        (["--deadline-seconds", "nan"], "deadline"),
        (["--cached-root", "missing"], "baseline-ref"),
        (["--cached-root", "missing", "--baseline-ref", "HEAD"], "fixture is missing"),
    ],
)
def test_invalid_cli_inputs_save_incomplete_report_before_workers(
    benchmark, tmp_path, monkeypatch, capsys, arguments, error
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(benchmark, "run_worker", lambda *a, **k: pytest.fail("worker launched"))
    output = tmp_path / "report.json"
    assert benchmark.main([*arguments, "--output", str(output)]) == 1
    report = json.loads(capsys.readouterr().out)
    assert report == json.loads(output.read_text())
    assert report["status"] == "incomplete" and error in report["error"]
    assert report["storage"] == report["http"] == {}


def test_cli_worker_failure_keeps_partial_evidence_and_cannot_report_passed(
    benchmark, tmp_path, monkeypatch, capsys
):
    calls = []

    def interrupted(source, workspace, payload, deadline, **kwargs):
        calls.append(payload["kind"])
        Path(payload["root"]).mkdir()
        if payload["kind"] == "http":
            raise ValueError("checkpoint incomplete")
        return {"result": {"milestones": []}, "network_attempts": 0}

    monkeypatch.setattr(benchmark, "run_worker", interrupted)
    assert benchmark.main(["--repetitions", "1", "--output", str(tmp_path / "report.json")]) == 1
    report = json.loads(capsys.readouterr().out)
    assert calls == ["storage", "http"]
    assert report["status"] == "incomplete" and report["error"] == "checkpoint incomplete"
    assert len(report["storage"]["candidate"]) == 1
    assert report["http"]["candidate"] == []
    assert "synthetic" not in report


def test_cached_deadline_failure_preserves_partial_checkpoint_and_cleans_workspace(
    benchmark, tmp_path, monkeypatch
):
    now = [100.0]
    workspaces = []
    checkpoint = {"status": "incomplete", "versions": [{"version": "v1", "seed": {"groups": 3}}]}

    def interrupted(source, workspace, payload, deadline, **kwargs):
        workspaces.append(workspace)
        assert payload["kind"] == "comparison" and deadline == 108.0
        assert payload["cached_root"] == str(tmp_path.absolute())
        (workspace / "comparison-progress.json").write_text(json.dumps(checkpoint))
        kwargs["peaks"]["sampled_peak_retained_bytes"] = 100
        now[0] = deadline
        raise ValueError("benchmark worker timed out")

    monkeypatch.setattr(benchmark.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(benchmark, "run_worker", interrupted)
    report = benchmark.cached_comparison(tmp_path, "HEAD", 2, 10)
    assert report["status"] == "incomplete" and "timed out" in report["error"]
    assert report["versions"] == checkpoint["versions"]
    assert report["deadline_seconds"] == 10 and report["wall_seconds_including_cleanup"] == 8
    assert report["sampled_peak_retained_bytes"] == 100
    assert len(workspaces) == 1 and not workspaces[0].exists()


def test_cached_completed_worker_cannot_hide_exceeded_complete_deadline(
    benchmark, tmp_path, monkeypatch
):
    now = [100.0]

    def completed(*args, **kwargs):
        now[0] = 111.0
        return {
            "result": {"status": "passed", "versions": []},
            "peak_rss_bytes": 1,
            "network_attempts": 0,
        }

    monkeypatch.setattr(benchmark.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(benchmark, "run_worker", completed)
    report = benchmark.cached_comparison(tmp_path, "HEAD", 2, 10)
    assert report["status"] == "incomplete" and "exceeded deadline" in report["error"]
    assert report["wall_seconds_including_cleanup"] == 11


@pytest.mark.parametrize("location", ["cached", "symlink"])
def test_cli_report_path_cannot_overwrite_cached_evidence_or_follow_links(
    benchmark, tmp_path, monkeypatch, capsys, location
):
    cached = tmp_path / "cached"
    cached.mkdir()
    evidence = cached / "evidence.json"
    evidence.write_bytes(b"immutable source evidence")
    if location == "cached":
        output = evidence
    else:
        output = tmp_path / "report.json"
        output.symlink_to(evidence)
    monkeypatch.setattr(benchmark, "run_worker", lambda *a, **k: pytest.fail("worker launched"))
    assert (
        benchmark.main(
            ["--cached-root", str(cached), "--baseline-ref", "HEAD", "--output", str(output)]
        )
        == 1
    )
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "incomplete"
    assert evidence.read_bytes() == b"immutable source evidence"
    assert "symlink" in report["error"] if location == "symlink" else "outside" in report["error"]


@pytest.mark.parametrize("failure", ["benchmark worker timed out", "semantic equality failed"])
def test_cli_cannot_report_success_when_cached_comparison_is_incomplete(
    benchmark, tmp_path, monkeypatch, capsys, failure
):
    cached = tmp_path / "cached"
    cached.mkdir()
    completed = []

    def baseline(workspace, ref, deadline):
        directory = workspace / "baseline-source"
        directory.mkdir()
        return directory, "0" * 40

    def successful(source, workspace, payload, deadline, **kwargs):
        completed.append(payload["kind"])
        Path(payload["root"]).mkdir()
        return {"result": {}, "network_attempts": 0}

    partial = {"status": "incomplete", "versions": [{"version": "v1"}], "error": failure}
    monkeypatch.setattr(benchmark, "export_baseline", baseline)
    monkeypatch.setattr(benchmark, "run_worker", successful)
    monkeypatch.setattr(benchmark, "cached_comparison", lambda *args: partial)
    output = tmp_path / "report.json"
    assert (
        benchmark.main(
            [
                "--repetitions",
                "1",
                "--cached-root",
                str(cached),
                "--baseline-ref",
                "HEAD",
                "--output",
                str(output),
            ]
        )
        == 1
    )
    report = json.loads(capsys.readouterr().out)
    assert report == json.loads(output.read_text())
    assert completed == ["storage", "http", "storage", "http", "synthetic"]
    assert report["status"] == "incomplete" and report["error"] == failure
    assert report["cached"] == partial
