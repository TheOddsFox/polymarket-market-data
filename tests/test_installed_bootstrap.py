import hashlib
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "relative_path", ["scripts/check-installed.py", "tests/fakes/installed_workflow.py"]
)
@pytest.mark.parametrize("optimization", ["-O", "-OO", "1", "2"])
def test_installed_acceptance_refuses_optimization_before_setup(
    tmp_path, relative_path, optimization
):
    repository = Path(__file__).resolve().parents[1]
    script = tmp_path / Path(relative_path).name
    script.write_bytes((repository / relative_path).read_bytes())
    environment = {
        name: value
        for name, value in os.environ.items()
        if name not in {"PYTHONPATH", "PYTHONHOME", "PYTHONOPTIMIZE"}
    }
    flags = []
    if optimization.startswith("-"):
        flags.append(optimization)
    else:
        environment["PYTHONOPTIMIZE"] = optimization
    result = subprocess.run(
        [sys.executable, "-S", *flags, str(script), "unused-environment"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert result.stderr.strip() == (
        "installed acceptance requires Python assertions; disable optimization"
    )
    assert result.stdout == ""
    assert list(tmp_path.iterdir()) == [script]


def test_installed_bootstrap_uses_locked_cache_and_isolated_environment(tmp_path, monkeypatch):
    script = Path(__file__).resolve().parents[1] / "scripts" / "check-installed.py"
    spec = importlib.util.spec_from_file_location("check_installed", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    repository = tmp_path / "checkout"
    (repository / "dist").mkdir(parents=True)
    (repository / "tests" / "fakes").mkdir(parents=True)
    wheel = repository / "dist" / "polymarket_market_data-0.1.0-py3-none-any.whl"
    wheel.write_bytes(b"wheel fixture")
    fixture_text = "assert 'installed fixture'\n"
    (repository / "tests" / "fakes" / "installed_workflow.py").write_text(fixture_text)
    monkeypatch.setattr(module, "__file__", str(repository / "scripts" / "check-installed.py"))
    monkeypatch.setattr(module.shutil, "which", lambda name: "/tools/uv")
    for name in (
        "PYTHONPATH",
        "PYTHONHOME",
        "PMXT_API_KEY",
        "CATALOGUE_ROOT",
        "UV_PROJECT",
        "UV_WORKING_DIR",
    ):
        monkeypatch.setenv(name, "ambient value must not reach the child")
    monkeypatch.setenv("UV_PROJECT_ENVIRONMENT", str(repository / "ambient-venv"))
    monkeypatch.setenv("UV_CACHE_DIR", str(tmp_path / "artifact-only-cache"))
    calls = []

    def checked(argv, **kwargs):
        calls.append((argv, kwargs))
        if "--requirements" in argv:
            requirement = Path(argv[argv.index("--requirements") + 1]).read_text()
            assert requirement == (
                f"polymarket-market-data @ {wheel.as_uri()} "
                f"--hash=sha256:{hashlib.sha256(wheel.read_bytes()).hexdigest()}\n"
            )
        if len(argv) == 3 and argv[1].endswith("installed_workflow.py"):
            copied_fixture = Path(argv[1])
            assert copied_fixture.read_text() == f"#!{argv[0]}\n{fixture_text}"
            assert os.access(copied_fixture, os.X_OK)

    monkeypatch.setattr(module, "checked", checked)
    module.main()
    assert len(calls) == 4
    fresh_venv = Path(calls[0][0][-1])
    sync, sync_options = calls[1]
    assert sync == [
        "/tools/uv",
        "sync",
        "--locked",
        "--offline",
        "--no-dev",
        "--no-install-project",
    ]
    assert sync_options["cwd"] == repository
    assert sync_options["env"]["UV_PROJECT_ENVIRONMENT"] == str(fresh_venv)
    install = calls[2][0]
    assert install[:5] == ["/tools/uv", "pip", "install", "--offline", "--no-deps"]
    assert install[install.index("--python") + 1] == str(fresh_venv / "bin" / "python")
    assert "--require-hashes" in install
    command, options = calls[3]
    assert command[0] == str(fresh_venv / "bin" / "python")
    assert options["cwd"] == fresh_venv.parent
    assert not fresh_venv.is_relative_to(repository)
    for _, options in calls:
        environment = options["env"]
        assert (
            not {
                "PYTHONPATH",
                "PYTHONHOME",
                "PMXT_API_KEY",
                "UV_PROJECT",
                "UV_WORKING_DIR",
            }
            & environment.keys()
        )
        assert not any(name.startswith("CATALOGUE_") for name in environment)
        assert environment["UV_CACHE_DIR"] == str(tmp_path / "artifact-only-cache")
