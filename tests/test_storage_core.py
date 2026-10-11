import os
from dataclasses import replace
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_market_data import foundation, runner
from polymarket_market_data.foundation import BudgetExceeded, Limits, Store, read_bytes


def store_at(tmp_path):
    return Store(
        tmp_path / "root", "test.storage.v1", Limits(100, 10_000, 1_000_000, 1000, 100_000)
    )


def actual_bytes(store):
    return sum(path.stat().st_size for path in store.root.rglob("*") if path.is_file())


@pytest.mark.parametrize("captures", [0, 10, 100])
def test_one_root_inventory_per_outer_operation(tmp_path, monkeypatch, captures):
    store = store_at(tmp_path)
    plan = store.create_plan({"limits": store.limits.as_dict()})
    acquisition = store.new_acquisition(plan, "fixture")
    roots = []
    original = store._measure

    def measure(path):
        if path == store.root:
            roots.append(path)
        return original(path)

    monkeypatch.setattr(store, "_measure", measure)
    with store.lock:
        initial = store.retained_bytes()
        for index in range(captures):
            store.capture(acquisition, f"unit-{index}", {"samples": [{"id": str(index)}]})
        with store.lock:
            store._budget()
            assert store.retained_bytes() >= initial
            assert store.retained_bytes() == actual_bytes(store)
        assert len(roots) == 1
    with store.lock:
        assert store.retained_bytes() == actual_bytes(store)
    assert len(roots) == 2


def test_next_operation_sees_external_growth_and_symlinks(tmp_path):
    store = store_at(tmp_path)
    with store.lock:
        before = store.retained_bytes()
    path = store.root / "orphan"
    path.write_bytes(b"pending work")
    with store.lock:
        assert store.retained_bytes() == before + len(b"pending work")
    link = store.root / "link"
    link.symlink_to(path)
    with pytest.raises(ValueError, match="symlink"):
        store.retained_bytes()


def test_changed_limits_use_existing_inventory(tmp_path):
    store = store_at(tmp_path)
    with store.lock:
        used = store.retained_bytes()
        store.limits = replace(store.limits, max_storage_bytes=used)
        store._budget()
        with pytest.raises(BudgetExceeded):
            store._budget(1)


def test_atomic_replace_reserves_old_plus_new_peak(tmp_path):
    store = store_at(tmp_path)
    path = store.root / "pointer"
    store.atomic_write(path, b"old data")
    with store.lock:
        before = store.retained_bytes()
        store.limits = replace(store.limits, max_storage_bytes=before + 3)
        with pytest.raises(BudgetExceeded):
            store.atomic_write(path, b"next")
        assert path.read_bytes() == b"old data"
        assert not list(store.root.glob(".pending-*"))
        store.limits = replace(store.limits, max_storage_bytes=before + 4)
        store.atomic_write(path, b"next")
        assert store.retained_bytes() == before - 4 == actual_bytes(store)
        assert store.lock.storage["reserved"] == 0


def test_failed_atomic_write_invalidates_and_counts_leftover(tmp_path, monkeypatch):
    store = store_at(tmp_path)
    path = store.root / "target"

    def interrupted(path, data):
        path.with_name(".pending-orphan").write_bytes(data[:3])
        raise OSError("interrupted")

    monkeypatch.setattr(foundation, "atomic_write", interrupted)
    with store.lock:
        before = store.retained_bytes()
        with pytest.raises(OSError, match="interrupted"):
            store.atomic_write(path, b"payload")
        assert store.lock.storage["bytes"] is None
        assert store.lock.storage["reserved"] == 0
        assert store.retained_bytes() == before + 3 == actual_bytes(store)


def test_sink_caps_growth_and_tracks_truncation_and_rename(tmp_path):
    store = store_at(tmp_path)
    source = store.root / ".state" / "download"
    with store.lock:
        before = store.retained_bytes()
        store.limits = replace(store.limits, max_storage_bytes=before + 8)
        with store.open_output(source, max_bytes=8) as sink:
            assert sink.write(b"12345678") == 8
            with pytest.raises(BudgetExceeded):
                sink.write(b"9")
            assert sink.truncate(4) == 4
            sink.seek(4)
            assert sink.write(b"abc") == 3
            assert store.retained_bytes() == before + 7
        destination = store.root / "objects" / "fixture"
        store.rename(source, destination)
        assert store.retained_bytes() == before + 7 == actual_bytes(store)
        store.remove(destination)
        assert store.retained_bytes() == before == actual_bytes(store)


def test_sink_records_partial_write_before_error(tmp_path):
    store = store_at(tmp_path)
    path = store.root / "partial"

    class PartialWrite:
        def __init__(self, stream):
            self.stream = stream

        def __getattr__(self, name):
            return getattr(self.stream, name)

        def write(self, data):
            self.stream.write(data[:2])
            raise OSError("partial write")

    with store.lock:
        before = store.retained_bytes()
        with store.open_output(path) as sink:
            sink.stream = PartialWrite(sink.stream)
            with pytest.raises(OSError, match="partial write"):
                sink.write(b"12345")
            assert store.retained_bytes() == before + 2 == actual_bytes(store)


def test_sink_is_arrow_compatible_and_preserves_existing_file_accounting(tmp_path):
    store = store_at(tmp_path)
    path = store.root / "sample.parquet"
    store.atomic_write(path, b"old placeholder")
    with store.lock:
        before = store.retained_bytes() - len(b"old placeholder")
        with store.open_output(path, max_bytes=10_000) as sink:
            pq.write_table(pa.table({"id": [1, 2]}), sink)
        assert pq.read_table(path).to_pylist() == [{"id": 1}, {"id": 2}]
        assert store.retained_bytes() == before + path.stat().st_size == actual_bytes(store)


def test_external_growth_consumes_reservation_without_double_charge(tmp_path):
    store = store_at(tmp_path)
    directory = store.root / ".state" / "producer"
    with store.lock:
        before = store.retained_bytes()
        store.limits = replace(store.limits, max_storage_bytes=before + 10)
        with store.external_write([directory], 10) as scope:
            assert store.available_bytes() == 0
            directory.mkdir()
            file = directory / "result"
            file.write_bytes(b"123456")
            assert scope.check() == 6
            assert store.retained_bytes() == before + 6
            assert store.lock.storage["reserved"] == 4
            with pytest.raises(BudgetExceeded):
                store.atomic_write(store.root / "unrelated", b"x")
            file.write_bytes(b"1234567890")
            assert scope.check() == 10
            assert store.available_bytes() == 0
        assert store.lock.storage["reserved"] == 0
        assert store.retained_bytes() == before + 10 == actual_bytes(store)
        store.remove(directory)
        assert store.retained_bytes() == before


def test_external_limit_crossing_is_recorded_without_clamping(tmp_path):
    store = store_at(tmp_path)
    path = store.root / "external"
    with store.lock:
        before = store.retained_bytes()
        store.limits = replace(store.limits, max_storage_bytes=before + 4)
        with pytest.raises(BudgetExceeded):
            with store.external_write([path], 4):
                path.write_bytes(b"1234567")
        assert store.retained_bytes() == before + 7 == actual_bytes(store)
        assert store.lock.storage["reserved"] == 0
        with pytest.raises(BudgetExceeded):
            store._budget()
        store.remove(path)
        store._budget()


def test_external_failure_settles_bytes_and_releases_unused_allowance(tmp_path):
    store = store_at(tmp_path)
    path = store.root / "external"
    with store.lock:
        before = store.retained_bytes()
        with pytest.raises(OSError, match="writer failed"):
            with store.external_write([path], 10):
                path.write_bytes(b"123")
                raise OSError("writer failed")
        assert store.retained_bytes() == before + 3 == actual_bytes(store)
        assert store.lock.storage["reserved"] == 0


def test_active_external_paths_cannot_be_managed_or_refreshed(tmp_path):
    store = store_at(tmp_path)
    path = store.root / ".state" / "producer"
    with store.external_write([path], 100):
        for operation in (
            lambda: store.atomic_write(path / "data", b"x"),
            lambda: store.remove(path),
            lambda: store.rename(path, store.root / "elsewhere"),
            lambda: store.retained_bytes(refresh=True),
        ):
            with pytest.raises(ValueError, match="external"):
                operation()
        with pytest.raises(ValueError, match="overlapping"):
            with store.external_write([path / "nested"], 0):
                pytest.fail("overlap entered")


def test_failed_cleanup_invalidates_then_recounts_remaining_bytes(tmp_path, monkeypatch):
    store = store_at(tmp_path)
    directory = store.root / "pending"
    directory.mkdir()
    store.atomic_write(directory / "a", b"aa")
    store.atomic_write(directory / "b", b"bbb")

    def partial_remove(path):
        (path / "a").unlink()
        raise OSError("cleanup failed")

    monkeypatch.setattr(foundation.shutil, "rmtree", partial_remove)
    with store.lock:
        before = store.retained_bytes()
        with pytest.raises(OSError, match="cleanup failed"):
            store.remove(directory)
        assert store.lock.storage["bytes"] is None
        assert store.retained_bytes() == before - 2 == actual_bytes(store)


@pytest.mark.parametrize("operation", ["atomic", "sink", "remove", "rename", "external"])
def test_helpers_refuse_symlink_ancestors_and_outside_paths(tmp_path, operation):
    store = store_at(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "data").write_bytes(b"preserve")
    (store.root / "link").symlink_to(outside, target_is_directory=True)
    for path in (store.root / "link" / "data", outside / "data"):
        with pytest.raises(ValueError, match="symlink|outside"):
            if operation == "atomic":
                store.atomic_write(path, b"changed")
            elif operation == "sink":
                with store.open_output(path):
                    pytest.fail("unsafe sink entered")
            elif operation == "remove":
                store.remove(path)
            elif operation == "rename":
                store.rename(path, store.root / "moved")
            else:
                with store.external_write([path], 100):
                    pytest.fail("unsafe scope entered")
    assert (outside / "data").read_bytes() == b"preserve"


def test_capture_temp_limit_includes_manifest_and_releases_pending_bytes(tmp_path):
    store = store_at(tmp_path)
    tiny = replace(store.limits, max_temp_bytes=100)
    plan = store.create_plan({"limits": tiny.as_dict()})
    acquisition = store.new_acquisition(plan, "fixture")
    with store.lock:
        before = store.retained_bytes()
        with pytest.raises(BudgetExceeded, match="temporary"):
            store.capture(acquisition, "empty", {})
        assert store.retained_bytes() == before == actual_bytes(store)
        assert not list(store.root.glob("acquisitions/*/captures/.pending-*"))


def test_duplicate_object_cleanup_and_internal_retention_are_accounted(tmp_path):
    store = store_at(tmp_path)
    first = store.root / ".state" / "first"
    second = store.root / ".state" / "second"
    with store.lock:
        before = store.retained_bytes()
        with store.open_output(first) as sink:
            sink.write(b"object")
        reference = store.retain_object(first)
        assert store.retained_bytes() == before + 6
        with store.open_output(second) as sink:
            sink.write(b"object")
        assert store.retained_bytes() == before + 12
        assert store.retain_object(second) == reference
        assert not second.exists()
        assert store.retained_bytes() == before + 6 == actual_bytes(store)


def test_fifos_are_rejected_without_opening_them(tmp_path):
    store = store_at(tmp_path)
    path = store.root / "fifo"
    os.mkfifo(path)
    with pytest.raises(ValueError, match="non-regular"):
        with store.open_output(path):
            pytest.fail("fifo output entered")


def test_runner_keeps_pointer_rollback_headroom_at_exact_retained_cap(tmp_path, monkeypatch):
    store = store_at(tmp_path)
    plan = store.create_plan({"limits": store.limits.as_dict()})
    acquisition = store.new_acquisition(plan, "clob-history")
    capture = store.capture(acquisition, "acquisition_complete", {})
    first = store.build_release(plan, [capture], {}, [], versions={"normalizer": 1})
    second = store.build_release(plan, [capture], {}, [], versions={"normalizer": 2})
    store.publish(first)
    pointer = store.root / "published" / (plan + ".current.json")
    previous = read_bytes(pointer)
    journal = runner.RunJournal(
        store, settings={"feeds": ["clob-history"], "limits": store.limits.as_dict()}
    )
    journal.data.update(
        plan_id=plan,
        acquisitions={"clob-history": acquisition},
        completed_acquisitions={"clob-history": acquisition},
        release_id=second,
    )
    journal.write()
    stage = runner.RunJournal.stage
    reached = []

    def fill_before_postpublication_journal(self, value):
        if value == "verify-root":
            assert store.verify(second)["release_id"] == second
            assert foundation.read_json(pointer)["release_id"] == second
            assert store.lock.storage["reserved"] == len(previous)
            remaining = store.available_bytes()
            store.atomic_write(store.root / "filler", b"x" * remaining)
            assert store.retained_bytes() + len(previous) == store.limits.max_storage_bytes
            reached.append(value)
        return stage(self, value)

    monkeypatch.setattr(runner.RunJournal, "stage", fill_before_postpublication_journal)
    # This case isolates rollback; a full root also cannot write its failure report.
    monkeypatch.setattr(runner, "_report", lambda *args, **kwargs: None)
    with store.lock:
        with pytest.raises(ValueError, match="retained storage budget exhausted"):
            runner.run(store, SimpleNamespace(resume_run=journal.run_id))
        assert reached == ["verify-root"]
        assert read_bytes(pointer) == previous
        assert store.lock.storage["reserved"] == 0
        assert store.lock.storage["scopes"] == []
        assert store.retained_bytes() == actual_bytes(store)
        assert store.available_bytes() == len(previous)
        assert set(store.verify()["releases"]) == {first, second}
