from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pyarrow.parquet as pq
import pytest

from polymarket_market_data import foundation
from polymarket_market_data.books import IDENTITY_FIELDS
from polymarket_market_data.build import build
from polymarket_market_data.feeds import PRICE_SCHEMA, coverage
from polymarket_market_data.foundation import Limits, Store
from polymarket_market_data.metadata import pin_metadata

NOW = datetime(2026, 4, 14, 8, tzinfo=UTC)
LIMITS = Limits(30, 10**7, 10**8, 10**6, 10**7)
IDENTITY = {
    "venue": "polymarket",
    "asset_kind": "ctf_token",
    "asset_id": "123",
    "market_id": "1",
    "condition_id": "0x" + "a" * 64,
    "outcome_index": 1,
}


def price_capture(store, plan_id, rows, *, covered=None):
    acquisition = store.new_acquisition(plan_id, "clob-history")
    capture = store.capture(
        acquisition,
        "samples",
        {"price_samples": rows},
        coverage=covered,
        schemas={"price_samples": PRICE_SCHEMA},
    )
    store.capture(
        acquisition,
        "acquisition_complete",
        {},
        evidence={
            "capture_ids": [capture["capture_id"]],
            "acquisition_revision": 1,
            "schema_revision": 1,
        },
    )
    return acquisition


def sample(identity, second, price):
    return identity | {
        "source": "clob-history",
        "source_at": NOW + timedelta(seconds=second),
        "received_at": NOW,
        "ingested_at": NOW,
        "price": Decimal(price),
    }


@pytest.mark.parametrize("kind", ["ctf_token", "poly_v2_position"])
def test_ohlc_preserves_every_native_identity_and_capture_revision(tmp_path, kind):
    identity = IDENTITY | {"asset_kind": kind}
    store = Store(tmp_path / "root", "test.v1")
    plan = store.create_plan(
        {"limits": LIMITS.as_dict(), "assets": [identity], "feeds": ["clob-history"]}
    )
    first = price_capture(
        store,
        plan,
        [sample(identity, 1, "0.123456789012345678"), sample(identity, 2, "0.5")],
    )
    second = price_capture(store, plan, [sample(identity, 1, "0.4")])
    release = build(store, plan, [first, second])
    rows = pq.read_table(store.root / "published" / release / "sampled_ohlc.parquet").to_pylist()
    assert len(rows) == 2
    for row in rows:
        assert {name: row[name] for name, _ in IDENTITY_FIELDS} == identity
    by_acquisition = {row["acquisition_id"]: row for row in rows}
    assert by_acquisition[first]["open"] == Decimal("0.123456789012345678")
    assert by_acquisition[first]["close"] == Decimal("0.5")
    assert by_acquisition[first]["observed_samples"] == 2
    assert by_acquisition[second]["open"] == Decimal("0.4")
    assert len({row["capture_id"] for row in rows}) == 2


def test_release_identity_is_invariant_to_acquisition_order_with_distinct_coverage(tmp_path):
    store = Store(tmp_path / "root", "test.v1")
    plan = store.create_plan(
        {"limits": LIMITS.as_dict(), "assets": [IDENTITY], "feeds": ["clob-history"]}
    )
    acquisitions = [
        price_capture(
            store,
            plan,
            [sample(IDENTITY, 1, price)],
            covered=[
                coverage(
                    IDENTITY,
                    "clob-history",
                    NOW,
                    NOW + timedelta(seconds=10),
                    "success",
                    observed_samples=count,
                )
            ],
        )
        for count, price in [(1, "0.4"), (2, "0.5")]
    ]
    forward = build(store, plan, acquisitions)
    reverse = build(store, plan, list(reversed(acquisitions)))
    assert forward == reverse
    manifest = store.verify(forward)
    assert len(manifest["coverage"]) == 2
    assert {row["observed_samples"] for row in manifest["coverage"]} == {1, 2}
    rows = pq.read_table(store.root / "published" / forward / "sampled_ohlc.parquet").to_pylist()
    assert len(rows) == 2
    assert {
        row["price"]
        for row in pq.read_table(
            store.root / "published" / forward / "price_samples.parquet"
        ).to_pylist()
    } == {Decimal("0.4"), Decimal("0.5")}


@pytest.mark.parametrize(
    "field,value", [("market_id", "2"), ("condition_id", "other"), ("outcome_index", 2)]
)
@pytest.mark.parametrize("second", [1, 2])
def test_ohlc_rejects_conflicting_sample_identity_even_at_duplicate_timestamp(
    tmp_path, field, value, second
):
    store = Store(tmp_path / "root", "test.v1")
    plan = store.create_plan(
        {"limits": LIMITS.as_dict(), "assets": [IDENTITY], "feeds": ["clob-history"]}
    )
    acquisition = price_capture(
        store,
        plan,
        [sample(IDENTITY, 1, "0.4"), sample(IDENTITY | {field: value}, second, "0.4")],
    )
    with pytest.raises(ValueError, match="conflicting sample identity"):
        build(store, plan, [acquisition])


def released(tmp_path, objects=None):
    store = Store(tmp_path / "root", "test.v1")
    plan = store.create_plan({"limits": LIMITS.as_dict()})
    acquisition = store.new_acquisition(plan, "test")
    capture = store.capture(acquisition, "selected", {}, objects=objects)
    release = store.build_release(plan, [capture], {}, [])
    return store, plan, capture, release


@pytest.mark.parametrize("same_plan", [True, False])
def test_named_release_ignores_unreferenced_damage_and_root_verification_finds_it(
    tmp_path, same_plan
):
    store, plan, _, release = released(tmp_path)
    if not same_plan:
        plan = store.create_plan({"limits": LIMITS.as_dict(), "unrelated": True})
    acquisition = store.new_acquisition(plan, "test")
    unrelated = store.capture(acquisition, "unrelated", {})
    (store.capture_path(unrelated) / "evidence.json").write_bytes(b"corruption")
    assert store.verify(release)["release_id"] == release
    assert store.publish(release)["release_id"] == release
    with pytest.raises(ValueError, match="checksum"):
        store.verify()


def test_named_release_rejects_missing_referenced_capture(tmp_path):
    store, _, capture, release = released(tmp_path)
    (store.capture_path(capture) / "manifest.json").unlink()
    with pytest.raises((ValueError, OSError)):
        store.verify(release)


def test_named_release_verifies_only_its_referenced_objects(tmp_path):
    store, plan, _, release = released(tmp_path)
    download = store.root / ".state" / "archive"
    download.write_bytes(b"archive")
    reference = store.retain_object(download)
    acquisition = store.new_acquisition(plan, "archive")
    capture = store.capture(acquisition, "source", {}, objects=[reference])
    referenced = store.build_release(plan, [capture], {}, [])
    store.object_path(reference).write_bytes(b"changed")
    assert store.verify(release)["release_id"] == release
    with pytest.raises(ValueError, match="reference"):
        store.verify(referenced)
    with pytest.raises(ValueError, match="reference"):
        store.verify()


@pytest.mark.parametrize("key", ["metadata_sha256", "metadata_manifest_sha256", "metadata_digest"])
def test_named_release_checks_referenced_pinned_metadata(tmp_path, key):
    producer = tmp_path / "producer"
    producer.mkdir()
    relations = {
        "markets": [{"venue": "polymarket", "market_id": "1", "usable": True}],
        "outcomes": [IDENTITY | {"clob_token_id": "123", "position_id": None}],
        "memberships": [],
        "identity_history": [],
        "coverage": [{"market_id": "1", "status": "found"}],
    }
    for relation, rows in relations.items():
        (producer / (relation + ".json")).write_bytes(foundation.json_bytes(rows))
    manifest = foundation.json_bytes(
        {
            "contract": "oddsfox.polymarket.metadata.v1",
            "files": foundation.file_inventory(producer),
        }
    )
    (producer / "manifest.json").write_bytes(manifest)
    sha = foundation.digest(manifest)
    store = Store(tmp_path / "root", "test.v1")
    pinned = pin_metadata(store.root, producer, sha, store=store)
    plan = store.create_plan({"limits": LIMITS.as_dict(), key: sha})
    acquisition = store.new_acquisition(plan, "test")
    capture = store.capture(acquisition, "selected", {})
    release = store.build_release(plan, [capture], {}, [])
    assert store.verify(release)["release_id"] == release
    (pinned.path / "outcomes.json").write_bytes(b"[]")
    with pytest.raises(ValueError, match="metadata file digest"):
        store.verify(release)


def test_independent_release_verification_rehashes_referenced_objects(tmp_path, monkeypatch):
    store, plan, _, _ = released(tmp_path)
    download = store.root / ".state" / "archive"
    download.write_bytes(b"archive")
    reference = store.retain_object(download)
    acquisition = store.new_acquisition(plan, "archive")
    captures = [
        store.capture(acquisition, f"source-{i}", {}, objects=[reference]) for i in range(3)
    ]
    release = store.build_release(plan, captures, {}, [])
    hashes = counted_hashes(monkeypatch)
    path = store.root / reference["path"]
    store.verify(release)
    assert hashes.count(path) == 1
    store.verify(release)
    assert hashes.count(path) == 2


def retained(tmp_path):
    store = Store(tmp_path / "root", "test.v1")
    download = store.root / ".state" / "archive"
    download.write_bytes(b"archive")
    return store, store.retain_object(download)


def counted_hashes(monkeypatch):
    original = foundation.file_info
    hashes = []

    def counted(path):
        hashes.append(path)
        return original(path)

    monkeypatch.setattr(foundation, "file_info", counted)
    return hashes


def test_object_hash_cache_is_scoped_to_outer_lock_and_exact_reference(tmp_path, monkeypatch):
    store, reference = retained(tmp_path)
    hashes = counted_hashes(monkeypatch)
    with store.lock:
        path = store.object_path(reference)
        assert store.object_path(reference) == path
        with store.lock:
            store.object_path(reference)
        assert len(hashes) == 1
        with pytest.raises(ValueError, match="reference"):
            store.object_path(reference | {"size": reference["size"] + 1})
        assert len(hashes) == 2
    store.object_path(reference)
    assert len(hashes) == 3


def test_object_hash_cache_detects_in_place_and_inode_changes(tmp_path, monkeypatch):
    store, reference = retained(tmp_path)
    hashes = counted_hashes(monkeypatch)
    with store.lock:
        path = store.object_path(reference)
        path.write_bytes(b"changed")
        with pytest.raises(ValueError, match="reference"):
            store.object_path(reference)
        replacement = store.root / ".state" / "replacement"
        replacement.write_bytes(b"archive")
        replacement.replace(path)
        assert store.object_path(reference) == path
        assert len(hashes) == 3


def test_object_hash_cache_rejects_fingerprint_drift_during_hash(tmp_path, monkeypatch):
    store, reference = retained(tmp_path)
    original = foundation.file_info

    def mutate(path):
        result = original(path)
        path.write_bytes(b"changed")
        return result

    monkeypatch.setattr(foundation, "file_info", mutate)
    with pytest.raises(ValueError, match="changed during verification"):
        store.object_path(reference)


def test_retaining_object_avoids_rehash_within_locked_operation(tmp_path, monkeypatch):
    store = Store(tmp_path / "root", "test.v1")
    download = store.root / ".state" / "archive"
    download.write_bytes(b"archive")
    hashes = counted_hashes(monkeypatch)
    with store.lock:
        reference = store.retain_object(download)
        store.object_path(reference)
        store.object_path(reference)
        assert len(hashes) == 1
    store.object_path(reference)
    assert len(hashes) == 2
