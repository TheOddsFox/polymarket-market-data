from decimal import Decimal

import httpx
import pytest

from polymarket_market_data.foundation import (
    BudgetExceeded,
    Limits,
    Store,
    digest,
    file_info,
    json_bytes,
    read_json,
)
from polymarket_market_data.metadata import (
    MAX_BUNDLE,
    MAX_FILE,
    load_metadata,
    pin_metadata,
    select_assets,
)
from polymarket_market_data.transport import Client, SourceError

CONTRACT = "oddsfox.polymarket.TEST.v1"


def limits(**changes):
    return Limits(
        **{
            **dict(
                max_requests=6,
                max_download_bytes=1_000_000,
                max_storage_bytes=10_000_000,
                max_response_bytes=100_000,
                max_temp_bytes=1_000_000,
            ),
            **changes,
        }
    )


def metadata(tmp_path, *, history=None):
    path = tmp_path / "producer"
    path.mkdir()
    tables = {
        "markets": [{"venue": "polymarket", "market_id": "1", "usable": True}],
        "outcomes": [
            {
                "venue": "polymarket",
                "market_id": "1",
                "condition_id": "0x" + "1" * 64,
                "outcome_index": 1,
                "outcome_label": "A",
                "asset_kind": "ctf_token",
                "asset_id": "10",
                "clob_token_id": "10",
                "position_id": None,
            }
        ],
        "memberships": [],
        "identity_history": history or [],
        "coverage": [{"market_id": "1", "status": "found"}],
    }
    inventory = {}
    for name, rows in tables.items():
        raw = json_bytes(rows)
        (path / (name + ".json")).write_bytes(raw)
        inventory[name + ".json"] = {"size": len(raw), "sha256": digest(raw)}
    raw = json_bytes(
        {
            "contract": "oddsfox.polymarket.metadata.v1",
            "source_release_id": "synthetic",
            "files": inventory,
        }
    )
    (path / "manifest.json").write_bytes(raw)
    return path, digest(raw)


def test_pin_survives_producer_removal_and_detects_tamper(tmp_path):
    source, sha = metadata(tmp_path)
    root = tmp_path / "consumer"
    bundle = pin_metadata(root, source, sha)
    assert select_assets(bundle)[0]["asset_id"] == "10"
    import shutil

    shutil.rmtree(source)
    assert load_metadata(bundle.path, sha).outcomes[0]["outcome_index"] == 1
    (bundle.path / "outcomes.json").write_text("[]")
    with pytest.raises(ValueError, match="digest"):
        load_metadata(bundle.path, sha)


def test_large_identity_history_is_pinned_and_still_verified(tmp_path):
    history = [
        {"market_id": "1", "observation_id": str(index), "raw_outcomes": ["A", "B" * 1024**2]}
        for index in range(33)
    ]
    source, sha = metadata(tmp_path, history=history)
    assert 32 * 1024**2 < (source / "identity_history.json").stat().st_size < MAX_BUNDLE
    bundle = pin_metadata(tmp_path / "consumer", source, sha)
    assert bundle.identity_history == history
    assert select_assets(bundle)[0]["asset_id"] == "10"
    with (bundle.path / "identity_history.json").open("r+b") as stream:
        stream.seek(-2, 2)
        stream.write(b" ")
    with pytest.raises(ValueError, match="digest"):
        load_metadata(bundle.path, sha)


@pytest.mark.parametrize("limit", ["file", "bundle"])
def test_metadata_byte_limits_reject_before_pinning(tmp_path, limit):
    source, sha = metadata(tmp_path)
    assert MAX_FILE == MAX_BUNDLE == 128 * 1024**2
    if limit == "file":
        with (source / "identity_history.json").open("r+b") as stream:
            stream.truncate(MAX_FILE + 1)
        error = "bounded regular files"
    else:
        manifest = read_json(source / "manifest.json")
        for name in ("identity_history.json", "memberships.json"):
            path = source / name
            with path.open("wb") as stream:
                stream.write(b"[]" + b" " * (1024**2 - 2))
                for _ in range(63):
                    stream.write(b" " * 1024**2)
            manifest["files"][name] = file_info(path)
        raw = json_bytes(manifest)
        (source / "manifest.json").write_bytes(raw)
        sha = digest(raw)
        error = "bundle limit"
    root = tmp_path / "consumer"
    with pytest.raises(ValueError, match=error):
        pin_metadata(root, source, sha)
    assert not list(root.rglob("manifest.json"))


@pytest.mark.parametrize("attack", ["symlink", "unexpected", "manifest"])
def test_metadata_trust_boundary(tmp_path, attack):
    source, sha = metadata(tmp_path)
    if attack == "symlink":
        target = tmp_path / "outside"
        target.write_bytes((source / "outcomes.json").read_bytes())
        (source / "outcomes.json").unlink()
        (source / "outcomes.json").symlink_to(target)
    elif attack == "unexpected":
        (source / "extra.json").write_text("[]")
    else:
        sha = "0" * 64
    with pytest.raises((ValueError, OSError)):
        pin_metadata(tmp_path / "consumer", source, sha)
    assert not list((tmp_path / "consumer").rglob("manifest.json"))


def test_capture_release_resume_and_no_global_replacement(tmp_path):
    store = Store(tmp_path / "data", CONTRACT)
    plan = store.create_plan({"limits": limits().as_dict(), "targets": ["native"]})
    acquisition = store.new_acquisition(plan, "test")
    capture = store.capture(
        acquisition,
        "leaf",
        {"samples": [{"price": Decimal("0.123456789012345678"), "id": "1"}]},
        coverage=[{"status": "success"}],
    )
    assert store.new_acquisition(plan, "test", acquisition) == acquisition
    assert store.capture(acquisition, "leaf", {}, coverage=[]) == capture
    assert store.read_relation(capture, "samples")[0]["price"] == Decimal("0.123456789012345678")
    release = store.build_release(
        plan, [capture], {"samples": store.read_relation(capture, "samples")}, capture["coverage"]
    )
    pointer = store.publish(release)
    other = store.create_plan({"limits": limits().as_dict(), "targets": ["other"]})
    assert other != plan
    assert store.verify(release)["inputs"] == [capture["capture_id"]]
    assert read_json(store.root / "published" / (plan + ".current.json")) == pointer
    (store.capture_path(capture) / "evidence.json").write_text("tamper")
    with pytest.raises(ValueError, match="checksum"):
        store.verify(release)


@pytest.mark.parametrize("status", ["failed", "invalid", "incomplete", "saturated"])
def test_unresolved_cannot_replace_pointer(tmp_path, status):
    store = Store(tmp_path / "data", CONTRACT)
    plan = store.create_plan({"limits": limits().as_dict()})
    acquisition = store.new_acquisition(plan, "test")
    c = store.capture(
        acquisition, "empty", {"samples": []}, coverage=[{"status": "confirmed_empty"}]
    )
    r = store.build_release(plan, [c], {"samples": []}, c["coverage"])
    before = store.publish(r)
    with pytest.raises(ValueError, match="coverage"):
        store.build_release(plan, [c], {"samples": []}, [{"status": status}])
    assert read_json(store.root / "published" / (plan + ".current.json")) == before


def test_invalidated_release_withdraws_pointer_without_deleting_bytes(tmp_path):
    store = Store(tmp_path / "data", CONTRACT)
    plan = store.create_plan({"limits": limits().as_dict()})
    a = store.new_acquisition(plan, "test")
    c = store.capture(a, "empty", {}, coverage=[{"status": "confirmed_empty"}])
    r = store.build_release(plan, [c], {}, c["coverage"])
    store.publish(r)
    store.invalidate(r, "block hash mismatch")
    assert (store.root / "published" / r / "manifest.json").exists()
    assert not (store.root / "published" / (plan + ".current.json")).exists()
    with pytest.raises(ValueError, match="invalidated"):
        store.publish(r)


def test_retained_object_is_verified(tmp_path):
    store = Store(tmp_path / "data", CONTRACT)
    file = store.root / ".state" / "download"
    file.write_bytes(b"synthetic")
    obj = store.retain_object(file)
    plan = store.create_plan({"limits": limits().as_dict()})
    a = store.new_acquisition(plan, "archive")
    store.capture(a, "object", {}, objects=[obj])
    store.object_path(obj).write_bytes(b"tampered!")
    with pytest.raises(ValueError, match="reference"):
        store.committed_captures()


def test_float_and_invalid_limits_rejected(tmp_path):
    with pytest.raises(ValueError):
        Limits(1, 1, 1, 1, False)
    store = Store(tmp_path / "data", CONTRACT)
    plan = store.create_plan({"limits": limits().as_dict()})
    a = store.new_acquisition(plan, "test")
    with pytest.raises(ValueError, match="float"):
        store.capture(a, "leaf", {"samples": [{"price": 0.4}]})
    assert store.committed_captures() == []


@pytest.mark.parametrize("status", [302, 404, 413])
def test_transport_refuses_redirect_or_classifies_source_failure(status):
    client = Client(
        "clob",
        limits(),
        transport=httpx.MockTransport(lambda r: httpx.Response(status)),
        sleep=lambda _: None,
    )
    with pytest.raises(SourceError):
        client.get_json("https://clob.polymarket.com/prices-history")
    assert client.requests == 1
    client.close()


def test_transient_attempts_count_and_secret_errors_are_redacted():
    def fail(request):
        raise httpx.ConnectError("private-RPC-token", request=request)

    client = Client("clob", limits(), transport=httpx.MockTransport(fail), sleep=lambda _: None)
    with pytest.raises(SourceError) as error:
        client.get_json("https://clob.polymarket.com/prices-history")
    assert client.requests == 3 and "private-RPC-token" not in str(error.value)
    client.close()


def test_response_bounds_and_read_only_endpoint():
    cap = Limits(2, 100, 1000, 4, 1000)
    with Client(
        "clob",
        cap,
        transport=httpx.MockTransport(lambda r: httpx.Response(200, content=b"0123456789")),
    ) as client:
        with pytest.raises(BudgetExceeded):
            client.get_json("https://clob.polymarket.com/prices-history")
        assert client.downloaded == 10
        with pytest.raises(ValueError, match="read-only"):
            client.post_json("https://clob.polymarket.com/order", {})


def test_metadata_pin_respects_root_and_temporary_limits_before_copy(tmp_path):
    source, sha = metadata(tmp_path)
    for key in ("max_storage_bytes", "max_temp_bytes"):
        store = Store(tmp_path / key, CONTRACT, limits(**{key: 100}))
        with pytest.raises(BudgetExceeded):
            pin_metadata(store.root, source, sha, store=store)
        assert list((store.root / "inputs").iterdir()) == []


def test_unsupported_capture_revision_fails_closed(tmp_path):
    store = Store(tmp_path / "root", CONTRACT, limits())
    plan = store.create_plan({"limits": limits().as_dict()})
    acquisition = store.new_acquisition(plan, "test")
    capture = store.capture(acquisition, "unit", {})
    path = store.capture_path(capture) / "manifest.json"
    changed = read_json(path)
    changed["schema_version"] = 2
    path.write_bytes(json_bytes(changed))
    with pytest.raises(ValueError, match="unsupported capture"):
        store.committed_captures(acquisition_id=acquisition)


def test_writer_lock_symlink_is_rejected_without_touching_target(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    target = tmp_path / "private"
    target.write_text("preserve")
    (root / ".writer.lock").symlink_to(target)
    with pytest.raises(ValueError, match="symlink writer lock"):
        Store(root, CONTRACT, limits())
    assert target.read_text() == "preserve"


def test_installed_store_outside_checkout_does_not_require_git(tmp_path, monkeypatch):
    def no_process(*args, **kwargs):
        raise AssertionError("operator root outside Git should not invoke Git")

    monkeypatch.setattr("polymarket_market_data.foundation.subprocess.run", no_process)
    store = Store(tmp_path / "standalone", CONTRACT, limits())
    assert store.status()["plans"] == 0


@pytest.mark.parametrize("damage", ["digest", "plan", "missing", "invalid", "malformed"])
def test_root_verification_rejects_damaged_active_pointer(tmp_path, damage):
    store = Store(tmp_path / "data", CONTRACT, limits())
    plan = store.create_plan({"limits": limits().as_dict()})
    acquisition = store.new_acquisition(plan, "test")
    capture = store.capture(acquisition, "empty", {})
    release = store.build_release(plan, [capture], {}, [])
    pointer = store.publish(release)
    path = store.root / "published" / (plan + ".current.json")
    assert store.verify()["releases"] == [release]
    if damage == "digest":
        pointer["manifest_sha256"] = "0" * 64
    elif damage == "plan":
        path.rename(path.with_name("0" * 64 + ".current.json"))
    elif damage == "missing":
        pointer["release_id"] = "0" * 64
    elif damage == "invalid":
        (store.root / "published" / (release + ".invalid.json")).write_text("{}")
    else:
        pointer = []
    if damage != "plan":
        path.write_bytes(json_bytes(pointer))
    with pytest.raises(ValueError, match="publication pointer"):
        store.verify()
    with pytest.raises(ValueError, match="publication pointer"):
        store.status()
