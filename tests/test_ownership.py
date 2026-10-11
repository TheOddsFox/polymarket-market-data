import os
import subprocess
import sys
from pathlib import Path

import pytest

from polymarket_market_data import acquisitions, cli


def test_live_record_rejects_removed_events_root_flag(capsys):
    with pytest.raises(SystemExit) as rejected:
        cli.parser().parse_args(
            [
                "books",
                "record",
                "--plan",
                "sealed-plan",
                "--events-executable",
                "events",
                "--events-root",
                "events-root",
            ]
        )
    assert rejected.value.code == 2
    assert "unrecognized arguments: --events-root events-root" in capsys.readouterr().err


@pytest.mark.parametrize("owner", ["acquisitions", "producer"])
def test_acquisition_owners_import_without_cli_or_runner(tmp_path, owner):
    code = """
import importlib.abc
import sys

forbidden = {"polymarket_market_data.cli", "polymarket_market_data.runner"}

class RejectOperatorImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in forbidden:
            raise RuntimeError("acquisition owner imported an operator: " + fullname)

sys.meta_path.insert(0, RejectOperatorImports())
__import__("polymarket_market_data." + sys.argv[1])
if forbidden.intersection(sys.modules):
    raise RuntimeError("operator module was loaded")
"""
    environment = dict(os.environ)
    environment.pop("PYTHONOPTIMIZE", None)
    environment.pop("PYTHONHOME", None)
    # Explicitly identify source under test, without depending on the subprocess cwd.
    environment["PYTHONPATH"] = str(Path(acquisitions.__file__).resolve().parents[1])
    result = subprocess.run(
        [sys.executable, "-c", code, owner],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""


@pytest.mark.parametrize(
    "command,owner,expected",
    [
        (
            ["prices", "backfill"],
            "backfill",
            {"source": "clob-history", "enable_paid": False},
        ),
        (
            ["books", "backfill"],
            "backfill",
            {"source": "pmxt-books", "enable_paid": True},
        ),
        (
            ["trades", "backfill"],
            "backfill",
            {"source": "pmxt-trades", "enable_paid": True},
        ),
        (["archive", "backfill", "--version", "v1"], "archive", {"version": "v1"}),
        (["archive", "backfill", "--version", "v2"], "archive", {"version": "v2"}),
        (
            ["books", "record", "--events-executable", "events"],
            "record",
            {"events_executable": "events"},
        ),
    ],
)
def test_standalone_commands_dispatch_explicit_sealed_inputs(monkeypatch, command, owner, expected):
    if owner == "backfill":
        paid = expected["enable_paid"]
        account = {
            "account": "account",
            "period": "2026-10",
            "allowance_id": "allocation",
            "allowance_cap": 17,
        }
        if paid:
            command = command + [
                "--enable-paid",
                "--account",
                account["account"],
                "--period",
                account["period"],
                "--allowance-id",
                account["allowance_id"],
                "--allowance-cap",
                str(account["allowance_cap"]),
            ]
        expected = expected | (account if paid else dict.fromkeys(account))
    expected = {"plan_id": "sealed-plan", "resume_id": "prior-acquisition"} | expected
    store = object()
    result = {"acquisition_id": "prior-acquisition", "complete": True}
    calls = []

    def acquire(actual_store, **kwargs):
        calls.append((actual_store, kwargs))
        return result

    monkeypatch.setattr(acquisitions, owner, acquire)
    args = cli.parser().parse_args(
        command + ["--plan", "sealed-plan", "--resume", "prior-acquisition"]
    )
    assert cli._run_locked(store, args) is result
    assert calls == [(store, expected)]
