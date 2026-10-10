"""Install the built wheel and prove the free workflow outside every checkout, offline."""

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def checked(argv, **kwargs):
    subprocess.run(argv, check=True, **kwargs)


def main():
    repository = Path(__file__).resolve().parent.parent
    wheels = sorted((repository / "dist").glob("*.whl"))
    if len(wheels) != 1:
        raise ValueError("exactly one freshly built wheel required")
    wheel = wheels[0]
    uv = shutil.which("uv")
    if uv is None:
        raise ValueError("uv required for the locked installed-package check")
    with tempfile.TemporaryDirectory(prefix="market-data-installed-") as temporary:
        directory = Path(temporary)
        environment = {
            key: value
            for key, value in os.environ.items()
            if key
            not in {"PYTHONPATH", "PYTHONHOME", "PMXT_API_KEY", "UV_PROJECT", "UV_WORKING_DIR"}
            and not key.startswith("CATALOGUE_")
        }
        venv = directory / "venv"
        checked([uv, "venv", "--python", sys.executable, str(venv)], env=environment)
        python = venv / "bin" / "python"
        checked(
            [
                uv,
                "sync",
                "--locked",
                "--offline",
                "--no-dev",
                "--no-install-project",
            ],
            cwd=repository,
            env={**environment, "UV_PROJECT_ENVIRONMENT": str(venv)},
        )
        built_requirement = directory / "wheel.txt"
        built_requirement.write_text(
            f"polymarket-market-data @ {wheel.as_uri()} "
            f"--hash=sha256:{hashlib.sha256(wheel.read_bytes()).hexdigest()}\n"
        )
        checked(
            [
                uv,
                "pip",
                "install",
                "--offline",
                "--no-deps",
                "--python",
                str(python),
                "--require-hashes",
                "--requirements",
                str(built_requirement),
            ],
            env=environment,
        )
        fixture = directory / "installed_workflow.py"
        source = repository / "tests" / "fakes" / "installed_workflow.py"
        fixture.write_text(f"#!{python}\n" + source.read_text())
        fixture.chmod(0o700)
        checked([str(python), str(fixture), str(venv)], cwd=directory, env=environment)
    print(json.dumps({"installed_workflow": "passed", "sources": 4, "live_source_requests": 0}))


if __name__ == "__main__":
    main()
