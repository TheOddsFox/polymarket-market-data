import importlib
import os
import sqlite3
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pyarrow as pa
import pytest

from polymarket_market_data import producer
from polymarket_market_data.budget import BudgetLedger
from polymarket_market_data.feeds import PRICE_SCHEMA
from polymarket_market_data.foundation import BudgetExceeded, Limits, Store
from polymarket_market_data.transport import Client

build_module = importlib.import_module("polymarket_market_data.build")
transport_module = importlib.import_module("polymarket_market_data.transport")
CONTRACT = "oddsfox.polymarket.storage-integration.v1"
NOW = datetime(2025, 1, 1, tzinfo=UTC)
LIMITS = Limits(10, 10**6, 10**7, 10**5, 128 * 1024)
URL = "https://clob.polymarket.com/prices-history"


def actual_bytes(store):
    return sum(path.stat().st_size for path in store.root.rglob("*") if path.is_file())


def measurements(monkeypatch, store):
    paths = []
    original = Store._measure

    def measured(self, path):
        if self is store:
            paths.append(Path(path))
        return original(self, path)

    monkeypatch.setattr(Store, "_measure", measured)
    return paths


@pytest.mark.parametrize("managed", [False, True])
def test_json_responses_and_retries_never_inventory_retained_files(tmp_path, monkeypatch, managed):
    store = Store(tmp_path / "root", CONTRACT, LIMITS)
    unrelated = store.root / "objects" / "unrelated"
    unrelated.mkdir()
    for index in range(30):
        (unrelated / str(index)).write_bytes(b"retained source")
    paths = measurements(monkeypatch, store)
    original_rglob = Path.rglob

    def reject_root_walk(path, pattern):
        if path == store.root:
            pytest.fail("JSON response scanned retained storage")
        return original_rglob(path, pattern)

    monkeypatch.setattr(Path, "rglob", reject_root_walk)
    responses = iter(
        [httpx.Response(503, content=b"retry"), httpx.Response(200, json={"ok": True})]
    )
    with Client(
        "clob",
        LIMITS,
        storage_root=store.root,
        store=store if managed else None,
        transport=httpx.MockTransport(lambda request: next(responses)),
        sleep=lambda _: None,
    ) as client:
        assert client.get_json(URL) == {"ok": True}
        assert client.requests == 2
    assert store.root not in paths
    assert paths == []


class InterruptedBody(httpx.SyncByteStream):
    def __iter__(self):
        yield b"12345"
        raise httpx.ReadError("interrupted response")


def test_managed_download_retry_truncates_partial_bytes_and_reuses_inventory(tmp_path, monkeypatch):
    store = Store(tmp_path / "root", CONTRACT, LIMITS)
    destination = store.root / "objects" / "download"
    destination.write_bytes(b"previous")
    baseline = actual_bytes(store)
    store.limits = replace(LIMITS, max_storage_bytes=baseline)
    paths = measurements(monkeypatch, store)
    responses = iter(
        [httpx.Response(200, stream=InterruptedBody()), httpx.Response(200, content=b"ok")]
    )
    with store.lock:
        with Client(
            "clob",
            store.limits,
            store=store,
            transport=httpx.MockTransport(lambda request: next(responses)),
            sleep=lambda _: None,
        ) as client:
            assert client.download(URL, destination)["size"] == 2
            assert (client.requests, client.downloaded) == (2, 7)
        assert destination.read_bytes() == b"ok"
        assert store.retained_bytes() == actual_bytes(store) == baseline - len(b"previous") + 2
        assert store.lock.storage["reserved"] == 0
    assert paths.count(store.root) == 1


class FiveBytes(httpx.SyncByteStream):
    def __iter__(self):
        yield b"1234"
        yield b"5"


def test_managed_download_rejects_crossing_chunk_without_clamping_ingress(tmp_path):
    store = Store(tmp_path / "root", CONTRACT, LIMITS)
    baseline = actual_bytes(store)
    store.limits = replace(LIMITS, max_storage_bytes=baseline + 4)
    destination = store.root / "objects" / "download"
    with store.lock:
        with Client(
            "clob",
            store.limits,
            store=store,
            transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=FiveBytes())),
        ) as client:
            with pytest.raises(BudgetExceeded, match="retained storage"):
                client.download(URL, destination)
            assert (client.requests, client.downloaded) == (1, 5)
        assert destination.read_bytes() == b"1234"
        assert store.retained_bytes() == actual_bytes(store) == baseline + 4
        assert store.lock.storage["reserved"] == 0


def test_managed_transport_rejects_store_root_mismatch_before_client_creation(
    tmp_path, monkeypatch
):
    store = Store(tmp_path / "root", CONTRACT, LIMITS)
    monkeypatch.setattr(
        transport_module.httpx, "Client", lambda **kwargs: pytest.fail("transport created")
    )
    with pytest.raises(ValueError, match="store/root disagreement"):
        Client("clob", LIMITS, store=store, storage_root=tmp_path / "other")


def test_producer_polling_inventories_only_its_workspace_and_settles_after_reaping(
    tmp_path, monkeypatch
):
    store = Store(tmp_path / "root", CONTRACT, LIMITS)
    directory = store.root / ".state" / "producer"
    directory.mkdir()
    unrelated = store.root / "objects" / "unrelated"
    unrelated.mkdir()
    (unrelated / "source").write_bytes(b"retained source")
    baseline = actual_bytes(store)
    paths = measurements(monkeypatch, store)
    children = []
    popen = subprocess.Popen

    def child(*args, **kwargs):
        process = popen(*args, **kwargs)
        children.append(process)
        return process

    monkeypatch.setattr(producer.subprocess, "Popen", child)
    script = (
        "import pathlib,sys,time; p=pathlib.Path(sys.argv[1]); "
        "p.write_bytes(b'x'*100); time.sleep(0.25); p.write_bytes(b'x'*150); print('done')"
    )
    with store.lock:
        result = producer.bounded_run(
            [sys.executable, "-c", script, str(directory / "result")],
            env=os.environ.copy(),
            directory=directory,
            store=store,
            timeout=5,
        )
        assert result.returncode == 0 and result.stdout.strip() == b"done"
        assert all(process.poll() is not None for process in children)
        assert store.retained_bytes() == actual_bytes(store) == baseline + 150
        assert store.lock.storage["reserved"] == 0
        assert paths.count(store.root) == paths.count(unrelated) == 1
        assert paths.count(directory) > 2
        store.remove(directory)
        assert store.retained_bytes() == baseline


def test_producer_final_reconciliation_keeps_actual_over_limit_bytes(tmp_path, monkeypatch):
    store = Store(tmp_path / "root", CONTRACT, LIMITS)
    directory = store.root / ".state" / "producer"
    directory.mkdir()
    baseline = actual_bytes(store)
    store.limits = replace(LIMITS, max_storage_bytes=baseline + 5)
    children = []
    popen = subprocess.Popen

    def child(*args, **kwargs):
        process = popen(*args, **kwargs)
        children.append(process)
        return process

    monkeypatch.setattr(producer.subprocess, "Popen", child)
    with store.lock:
        with pytest.raises(BudgetExceeded):
            producer.bounded_run(
                [
                    sys.executable,
                    "-c",
                    "import pathlib,sys; pathlib.Path(sys.argv[1]).write_bytes(b'123456')",
                    str(directory / "result"),
                ],
                env=os.environ.copy(),
                directory=directory,
                store=store,
                timeout=5,
            )
        assert children and all(process.poll() is not None for process in children)
        assert store.retained_bytes() == actual_bytes(store) == baseline + 6
        assert store.lock.storage["reserved"] == 0
        store.remove(directory)
        assert store.retained_bytes() == baseline


def test_sqlite_growth_and_failed_transaction_reconcile_database_and_sidecars(
    tmp_path, monkeypatch
):
    store = Store(tmp_path / "root", CONTRACT, LIMITS)
    ledger = BudgetLedger(store.root, store=store)
    paths = measurements(monkeypatch, store)
    with store.lock:
        ledger.configure("account", "period", "allocation", 10)
        ledger.reserve("account", "period", "allocation")
        before = store.retained_bytes()
        assert before == actual_bytes(store)
        with pytest.raises(RuntimeError, match="transaction stopped"):
            with ledger._connection() as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute("UPDATE allowances SET used=9")
                db.execute("CREATE TABLE uncommitted(payload BLOB)")
                db.execute("INSERT INTO uncommitted VALUES(?)", (b"x" * 16384,))
                assert Path(str(ledger.path) + "-journal").is_file()
                raise RuntimeError("transaction stopped")
        assert ledger.status()["allocations"][0]["used"] == 1
        assert store.retained_bytes() == actual_bytes(store) == before
        assert store.lock.storage["reserved"] == 0
        assert not any(
            Path(str(ledger.path) + suffix).exists() for suffix in ("-journal", "-wal", "-shm")
        )
    assert paths.count(store.root) == 1


def test_sqlite_rejects_wal_without_changing_paid_usage(tmp_path):
    store = Store(tmp_path / "root", CONTRACT, LIMITS)
    ledger = BudgetLedger(store.root, store=store)
    ledger.configure("account", "period", "allocation", 10)
    with sqlite3.connect(ledger.path) as db:
        assert db.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    db.close()
    with store.lock:
        with pytest.raises(ValueError, match="rollback-journal"):
            ledger.reserve("account", "period", "allocation")
        assert ledger.status()["allocations"][0]["used"] == 0
        assert store.retained_bytes() == actual_bytes(store)
        assert store.lock.storage["reserved"] == 0


def capture_prices(store):
    plan = store.create_plan({"limits": LIMITS.as_dict(), "feeds": ["clob-history"]})
    acquisition = store.new_acquisition(plan, "clob-history")
    return store.capture(
        acquisition,
        "prices",
        {
            "price_samples": [
                {
                    "venue": "polymarket",
                    "market_id": "1",
                    "asset_kind": "ctf_token",
                    "asset_id": "123",
                    "condition_id": "0x" + "a" * 64,
                    "outcome_index": 1,
                    "source": "clob-history",
                    "source_at": NOW,
                    "received_at": NOW,
                    "ingested_at": NOW,
                    "price": Decimal("0.4"),
                }
            ]
        },
        schemas={"price_samples": PRICE_SCHEMA},
    )


@pytest.mark.parametrize("failure", [False, True])
def test_duckdb_spill_reservation_is_released_after_error_or_early_iterator_close(
    tmp_path, monkeypatch, failure
):
    store = Store(tmp_path / "root", CONTRACT, LIMITS)
    capture = capture_prices(store)
    filename = str(store.capture_path(capture) / "price_samples.parquet")
    instances = []

    class SpillingConnection:
        def __init__(self):
            self.closed = False
            self.directory = None
            instances.append(self)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.closed = True

        def execute(self, statement, parameters=None):
            if statement.startswith("SET temp_directory="):
                self.directory = Path(parameters[0])
            if statement.startswith("SELECT"):
                (self.directory / "spill").write_bytes(b"x" * 64)
                if failure:
                    raise RuntimeError("sort failed")
            return self

        def read_parquet(self, paths, **kwargs):
            assert paths == [filename]
            return self

        def create_view(self, name):
            return None

        def to_arrow_reader(self, batch_size):
            return pa.Table.from_pylist(
                [{"filename": filename, "price": Decimal("0.4")}]
            ).to_batches()

    monkeypatch.setattr(build_module.duckdb, "connect", SpillingConnection)
    with store.lock:
        before = store.retained_bytes()
        ordered = build_module._ordered(store, [capture], "price_samples", "source_at")
        if failure:
            with pytest.raises(RuntimeError, match="sort failed"):
                next(ordered)
        else:
            assert next(ordered)["price"] == Decimal("0.4")
            assert store.lock.storage["reserved"] > 0
            assert instances[0].directory.is_dir()
            ordered.close()
        assert instances[0].closed
        assert not list((store.root / ".state").glob(".build-*"))
        assert store.retained_bytes() == actual_bytes(store) == before
        assert store.lock.storage["reserved"] == 0


def test_duckdb_sort_requires_headroom_before_opening_connection(tmp_path, monkeypatch):
    store = Store(tmp_path / "root", CONTRACT, LIMITS)
    capture = capture_prices(store)
    with store.lock:
        before = store.retained_bytes()
        store.limits = replace(LIMITS, max_storage_bytes=before)
        monkeypatch.setattr(build_module.duckdb, "connect", lambda: pytest.fail("DuckDB opened"))
        with pytest.raises(BudgetExceeded, match="spill requires retained headroom"):
            next(build_module._ordered(store, [capture], "price_samples", "source_at"))
        assert not list((store.root / ".state").glob(".build-*"))
        assert store.retained_bytes() == actual_bytes(store) == before
