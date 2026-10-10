import subprocess
from pathlib import Path


def test_operator_roots_are_ignored_and_sdist_has_explicit_inventory():
    root = Path(__file__).resolve().parents[1]
    paths = [
        "data/capture.json",
        ".state/paid.sqlite",
        "artifacts/output.csv",
        "captures/frame.json",
        ".env",
        "probe.parquet",
        "dist/wheel.whl",
    ]
    result = subprocess.run(
        ["git", "check-ignore", "--no-index", *paths],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    assert set(result.stdout.splitlines()) == set(paths)
    import tomllib

    config = tomllib.loads((root / "pyproject.toml").read_text())
    assert set(config["tool"]["hatch"]["build"]["targets"]["sdist"]["include"]) == {
        "src",
        "tests",
        "README.md",
        "LICENSE",
        "pyproject.toml",
        "uv.lock",
        "THIRD_PARTY_NOTICES.md",
    }
    workflow = (root / ".github/workflows/ci.yml").read_text()
    assert "persist-credentials: false" in workflow and "contents: read" in workflow
