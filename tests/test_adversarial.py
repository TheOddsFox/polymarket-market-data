"""Failures that must not weaken a pinned handoff or commit boundary."""

from decimal import Decimal

import httpx
import pytest

from polymarket_market_data import foundation
from polymarket_market_data.foundation import (
    BudgetExceeded,
    Limits,
    Store,
    digest,
    file_info,
    json_bytes,
    read_json,
)
from polymarket_market_data.metadata import load_metadata, pin_metadata, select_assets
from polymarket_market_data.transport import Client


def limits(**changes):
    return Limits(
        **{
            "max_requests": 8,
            "max_download_bytes": 100_000,
            "max_storage_bytes": 10_000_000,
            "max_response_bytes": 100_000,
            "max_temp_bytes": 1_000_000,
            **changes,
        }
    )


def bundle(tmp_path, *, outcomes=None, coverage=None):
    directory = tmp_path / "metadata"
    directory.mkdir()
    relations = {
        "markets": [{"venue": "polymarket", "market_id": "1", "usable": True}],
        "outcomes": outcomes
        or [
            {
                "venue": "polymarket",
                "market_id": "1",
                "asset_kind": "ctf_token",
                "asset_id": "10",
                "clob_token_id": "10",
                "position_id": None,
                "outcome_index": 1,
                "condition_id": "0x" + "a" * 64,
            }
        ],
        "memberships": [],
        "identity_history": [],
        "coverage": [{"market_id": "1", "status": "found"}] if coverage is None else coverage,
    }
    files = {}
    for name, rows in relations.items():
        raw = json_bytes(rows)
        (directory / (name + ".json")).write_bytes(raw)
        files[name + ".json"] = {"size": len(raw), "sha256": digest(raw)}
    manifest = json_bytes({"contract": "oddsfox.polymarket.metadata.v1", "files": files})
    (directory / "manifest.json").write_bytes(manifest)
    return directory, digest(manifest)


def captured(tmp_path, status="success"):
    store = Store(tmp_path / "data", "oddsfox.polymarket.TEST.v1")
    plan = store.create_plan({"limits": limits().as_dict()})
    acquisition = store.new_acquisition(plan, "test")
    capture = store.capture(
        acquisition,
        "unit",
        {"samples": [{"price": Decimal("0.5")}]},
        evidence={"provider_revision": "one"},
        coverage=[{"status": status}],
    )
    return store, plan, capture


@pytest.mark.parametrize("native", ["0", "00", "01", "٠١", str(2**256)])
def test_contract_rejects_noncanonical_native_ids(tmp_path, native):
    outcome = {
        "venue": "polymarket",
        "market_id": "1",
        "asset_kind": "ctf_token",
        "asset_id": native,
        "clob_token_id": native,
        "position_id": None,
        "outcome_index": 1,
    }
    directory, sha = bundle(tmp_path, outcomes=[outcome])
    with pytest.raises(ValueError):
        select_assets(load_metadata(directory, sha), ["1"])


@pytest.mark.parametrize(
    "coverage",
    [
        [],
        [{"market_id": "2", "status": "found"}],
        [{"market_id": "1", "status": "failed"}, {"market_id": "1", "status": "found"}],
    ],
)
def test_selected_market_requires_unambiguous_coverage_accounting(tmp_path, coverage):
    directory, sha = bundle(tmp_path, coverage=coverage)
    with pytest.raises(ValueError):
        select_assets(load_metadata(directory, sha), ["1"])


def test_failed_pin_does_not_replace_existing_pinned_input(tmp_path):
    directory, sha = bundle(tmp_path)
    root = tmp_path / "root"
    pinned = pin_metadata(root, directory, sha)
    (directory / "outcomes.json").write_bytes(b"[]")
    with pytest.raises(ValueError):
        pin_metadata(root, directory, sha)
    assert select_assets(load_metadata(pinned.path, sha), ["1"])[0]["asset_id"] == "10"
    assert not list((root / "inputs").glob(".pending-*"))


def test_unresolved_input_cannot_be_hidden_by_replacement_coverage(tmp_path):
    store, plan, capture = captured(tmp_path, "failed")
    with pytest.raises(ValueError, match="coverage"):
        store.build_release(plan, [capture], {}, [{"status": "success"}])
    assert not list((store.root / "published").glob("*.current.json"))


def test_release_verifies_pinned_capture_manifest_not_only_current_checksums(tmp_path):
    store, plan, capture = captured(tmp_path)
    release = store.build_release(plan, [capture], {}, capture["coverage"])
    store.publish(release)
    directory = store.capture_path(capture)
    (directory / "evidence.json").write_bytes(json_bytes({"provider_revision": "two"}))
    revised = read_json(directory / "manifest.json")
    revised["files"]["evidence.json"] = file_info(directory / "evidence.json")
    (directory / "manifest.json").write_bytes(json_bytes(revised))
    with pytest.raises(ValueError, match="input|capture|manifest"):
        store.verify(release)


def test_failed_pointer_replacement_keeps_previous_release(tmp_path, monkeypatch):
    store, plan, capture = captured(tmp_path)
    first = store.build_release(plan, [capture], {"samples": []}, capture["coverage"])
    previous = store.publish(first)
    second = store.build_release(plan, [capture], {}, capture["coverage"])
    pointer = store.root / "published" / (plan + ".current.json")
    replace = foundation.os.replace

    def crash(source, destination):
        if destination == pointer:
            raise OSError("synthetic crash before pointer commit")
        return replace(source, destination)

    monkeypatch.setattr(foundation.os, "replace", crash)
    with pytest.raises(OSError, match="synthetic crash"):
        store.publish(second)
    assert read_json(pointer) == previous
    assert store.verify(first)["release_id"] == first
    assert not list((store.root / "published").glob(".pending-*"))


def test_failed_capture_manifest_commit_leaves_no_resumable_unit(tmp_path, monkeypatch):
    store, plan, capture = captured(tmp_path)
    original = foundation.atomic_write

    def crash(path, data):
        if path.name == "manifest.json":
            raise OSError("synthetic manifest failure")
        return original(path, data)

    monkeypatch.setattr(foundation, "atomic_write", crash)
    with pytest.raises(OSError, match="synthetic"):
        store.capture(capture["acquisition_id"], "second-unit", {})
    assert store.committed_captures(plan_id=plan) == [capture]
    assert not list((store.root / "acquisitions").glob("*/captures/.pending-*"))


def test_retained_archive_object_loss_blocks_release_promotion(tmp_path):
    store, plan, original = captured(tmp_path)
    temporary = store.root / ".state" / "download"
    temporary.write_bytes(b"synthetic archive")
    obj = store.retain_object(temporary)
    capture = store.capture(
        original["acquisition_id"],
        "archive-hour",
        {},
        objects=[obj],
        coverage=[{"status": "unobserved"}],
    )
    release = store.build_release(plan, [capture], {}, capture["coverage"])
    store.object_path(obj).unlink()
    with pytest.raises((ValueError, OSError)):
        store.publish(release)
    assert not list((store.root / "published").glob("*.current.json"))


class PartialFailure(httpx.SyncByteStream):
    def __iter__(self):
        yield b"abc"
        raise httpx.ReadError("credential-shaped-provider-error")


def test_partial_response_bytes_count_across_retry():
    calls = []

    def respond(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(200, stream=PartialFailure())
        return httpx.Response(200, content=b"{}")

    with Client(
        "clob",
        limits(max_download_bytes=4),
        transport=httpx.MockTransport(respond),
        retries=2,
        sleep=lambda _: None,
    ) as client:
        with pytest.raises(BudgetExceeded):
            client.get_json("https://clob.polymarket.com/prices-history")
        assert len(calls) == 2
        assert client.downloaded == 5


def test_request_limit_stops_retry_before_http():
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(503)

    with Client(
        "clob", limits(max_requests=1), transport=httpx.MockTransport(respond), sleep=lambda _: None
    ) as client:
        with pytest.raises(BudgetExceeded):
            client.get_json("https://clob.polymarket.com/prices-history")
        assert len(calls) == 1 and client.requests == 1
