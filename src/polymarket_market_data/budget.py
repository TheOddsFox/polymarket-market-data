"""Financial attempt reservations are never reconstructed from capture files."""

from __future__ import annotations

import os
import sqlite3
from contextlib import nullcontext
from pathlib import Path

from filelock import FileLock

from .foundation import BudgetExceeded, atomic_write, json_bytes, now, read_json


class BudgetLedger:
    def __init__(self, root, *, store=None):
        if store is not None and Path(root).absolute() != store.root:
            raise ValueError("paid budget store/root disagreement")
        self.store = store
        self.state = Path(root) / ".state"
        self.state.mkdir(parents=True, exist_ok=True)
        self.path = self.state / "paid.sqlite"
        self.sentinel = self.state / "paid.initialized.json"
        self.lock = FileLock(self.state / "paid.lock", timeout=0)
        if any(
            path.is_symlink()
            for path in (self.state, self.path, self.sentinel, self.state / "paid.lock")
        ):
            raise ValueError("unsafe paid ledger path")
        root_marker = self.state / "root.json"
        established = (
            root_marker.exists() and read_json(root_marker).get("paid_initialized") is True
        )
        if established and (not self.sentinel.exists() or not self.path.exists()):
            raise ValueError("paid ledger missing in established root; reconcile allocation")
        if self.sentinel.exists() and not self.path.exists():
            raise ValueError("paid ledger missing; reconcile allocation before enabling calls")
        if self.path.exists():
            self._check()

    def _check(self):
        try:
            with sqlite3.connect(f"file:{self.path}?mode=ro", uri=True) as db:
                if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise ValueError("paid ledger corrupted")
                db.execute(
                    "SELECT account,period,allowance_id,cap,used FROM allowances LIMIT 1"
                ).fetchall()
        except sqlite3.Error:
            raise ValueError(
                "paid ledger corrupted; reconcile allocation before enabling calls"
            ) from None

    def _bound_write(self, db):
        if self.store is None or self.store.limits is None:
            return
        self.store._budget()
        size = sum(path.stat().st_size for path in self.store.root.rglob("*") if path.is_file())
        available = self.store.limits.max_storage_bytes - size
        page_size = db.execute("PRAGMA page_size").fetchone()[0]
        page_count = db.execute("PRAGMA page_count").fetchone()[0]
        # A transaction may journal existing pages and grow both database and journal.
        journal = page_count * page_size + 16 * 1024
        growth = (min(available, self.store.limits.max_temp_bytes) - journal - 512) // (
            2 * page_size
        )
        if growth < 4:
            raise BudgetExceeded("paid accounting disk budget exhausted")
        db.execute(f"PRAGMA max_page_count={page_count + growth}")

    def configure(self, account, period, allowance_id, cap):
        if (
            not all(
                isinstance(v, str) and 0 < len(v) <= 160 for v in (account, period, allowance_id)
            )
            or isinstance(cap, bool)
            or not isinstance(cap, int)
            or not 0 < cap < 2**63
        ):
            raise ValueError("explicit account/period/allocation and positive cap required")
        with self.store.lock if self.store is not None else nullcontext(), self.lock:
            if self.sentinel.exists() and not self.path.exists():
                raise ValueError("paid ledger lost")
            if self.path.exists():
                self._check()
            new_database = not self.path.exists()
            try:
                with sqlite3.connect(self.path) as db:
                    db.execute("PRAGMA synchronous=FULL")
                    self._bound_write(db)
                    db.execute("BEGIN IMMEDIATE")
                    db.execute(
                        "CREATE TABLE IF NOT EXISTS allowances(account TEXT,period TEXT,allowance_id TEXT,cap INTEGER NOT NULL,used INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(account,period))"
                    )
                    db.execute(
                        "CREATE TABLE IF NOT EXISTS attempts(id INTEGER PRIMARY KEY,account TEXT,period TEXT,allowance_id TEXT,reserved_at TEXT)"
                    )
                    existing = db.execute(
                        "SELECT allowance_id,cap FROM allowances WHERE account=? AND period=?",
                        (account, period),
                    ).fetchone()
                    if existing and existing != (allowance_id, cap):
                        raise ValueError(
                            "existing account-period allocation cannot be reset or replaced"
                        )
                    db.execute(
                        "INSERT OR IGNORE INTO allowances(account,period,allowance_id,cap) VALUES(?,?,?,?)",
                        (account, period, allowance_id, cap),
                    )
                    db.commit()
            except (BudgetExceeded, sqlite3.Error) as exc:
                if new_database and not self.sentinel.exists():
                    self.path.unlink(missing_ok=True)
                if isinstance(exc, sqlite3.Error):
                    raise BudgetExceeded("paid accounting transaction could not commit") from None
                raise
            fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            if not self.sentinel.exists():
                atomic_write(self.sentinel, json_bytes({"initialized_at": now()}))
            root_marker = self.state / "root.json"
            if root_marker.exists():
                marker = read_json(root_marker)
                if marker.get("paid_initialized") is not True:
                    updated = json_bytes({**marker, "paid_initialized": True})
                    if self.store is not None:
                        self.store._budget(max(0, len(updated) - root_marker.stat().st_size))
                    atomic_write(root_marker, updated)
        return self.status()

    def reserve(self, account, period, allowance_id):
        with self.store.lock if self.store is not None else nullcontext(), self.lock:
            if not self.sentinel.exists() or not self.path.exists():
                raise ValueError("paid allowance not configured or ledger lost")
            read_json(self.sentinel)
            self._check()
            try:
                with sqlite3.connect(self.path) as db:
                    db.execute("PRAGMA synchronous=FULL")
                    self._bound_write(db)
                    db.execute("BEGIN IMMEDIATE")
                    row = db.execute(
                        "SELECT allowance_id,cap,used FROM allowances WHERE account=? AND period=?",
                        (account, period),
                    ).fetchone()
                    if not row or row[0] != allowance_id:
                        raise ValueError("unknown paid allocation")
                    if row[2] >= row[1]:
                        raise BudgetExceeded("allocated paid request allowance exhausted")
                    db.execute(
                        "UPDATE allowances SET used=used+1 WHERE account=? AND period=?",
                        (account, period),
                    )
                    cursor = db.execute(
                        "INSERT INTO attempts(account,period,allowance_id,reserved_at) VALUES(?,?,?,?)",
                        (account, period, allowance_id, now()),
                    )
                    db.commit()
                    return cursor.lastrowid
            except sqlite3.Error:
                raise BudgetExceeded("paid accounting transaction could not commit") from None

    def status(self):
        if not self.path.exists():
            return {"enabled": False, "allocations": []}
        self._check()
        with sqlite3.connect(f"file:{self.path}?mode=ro", uri=True) as db:
            rows = db.execute(
                "SELECT account,period,allowance_id,cap,used FROM allowances ORDER BY account,period"
            ).fetchall()
        return {
            "enabled": True,
            "allocations": [
                dict(zip(("account", "period", "allowance_id", "cap", "used"), row, strict=True))
                for row in rows
            ],
        }
