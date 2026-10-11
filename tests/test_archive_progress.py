"""A row group may be skipped only when its immutable observations are proved."""

import copy
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_market_data import acquisitions, archives, cli
from polymarket_market_data.archives import V1_SCHEMA, V2_SCHEMA, archive_coverage, iter_archive
from polymarket_market_data.books import UPDATE_SCHEMA
from polymarket_market_data.build import build
from polymarket_market_data.foundation import (
    BudgetExceeded,
    Limits,
    Store,
    digest,
    file_inventory,
    json_bytes,
    read_bytes,
)
from polymarket_market_data.transport import Client

NOW = datetime(2026, 4, 14, 8, tzinfo=UTC)
LIMITS = Limits(30, 10**7, 128 * 1024**2, 1024**2, 32 * 1024**2)
TARGET = {
    "venue": "polymarket",
    "market_id": "1",
    "asset_kind": "ctf_token",
    "asset_id": "123",
    "condition_id": "0x" + "a" * 64,
    "outcome_index": 1,
}
HOUR = "hour-20260414T08"


def observation(*, quantity="1", asset_id="123", condition=None, stamp=NOW, version="v1"):
    condition = condition or TARGET["condition_id"]
    levels = [{"price": "0.4", "size": quantity}]
    if version == "v1":
        return {
            "timestamp_received": stamp,
            "timestamp_created_at": stamp,
            "market_id": condition,
            "update_type": "book_snapshot",
            "data": json.dumps({"token_id": asset_id, "bids": levels, "asks": []}),
        }
    return {
        "timestamp_received": stamp,
        "timestamp": stamp,
        "market": condition.encode(),
        "event_type": "book",
        "asset_id": asset_id,
        "bids": json.dumps(levels),
        "asks": "[]",
    }


def source_file(path, groups, version):
    schema = V1_SCHEMA if version == "v1" else V2_SCHEMA
    with pq.ParquetWriter(path, schema, compression="zstd") as writer:
        for group in groups:
            table = pa.Table.from_pylist(group, schema=schema)
            writer.write_table(table, row_group_size=max(1, len(group)))


def setup_archive(tmp_path, monkeypatch, groups=None, *, version="v1", targets=None, hours=None):
    tmp_path.mkdir(parents=True, exist_ok=True)
    store = Store(tmp_path / "root", cli.CONTRACT)
    targets = targets or [TARGET]
    hours = hours or {NOW: groups or [[observation(quantity=str(index))] for index in range(1, 5)]}
    plan_id = store.create_plan(
        {
            "limits": LIMITS.as_dict(),
            "assets": targets,
            "feeds": ["pmxt-" + version],
            "start": min(hours),
            "end": max(hours) + timedelta(hours=1),
        }
    )
    payloads, files = {}, {}
    for hour, rows in hours.items():
        path = tmp_path / (hour.strftime("%Y%m%dT%H") + ".parquet")
        source_file(path, rows, version)
        payloads[hour.strftime("%Y-%m-%dT%H")] = path.read_bytes()
        files[hour] = path
    requests = []

    def handler(request):
        requests.append(str(request.url))
        hour = Path(request.url.path).stem.removeprefix("polymarket_orderbook_")
        return httpx.Response(200, content=payloads[hour])

    monkeypatch.setattr(
        acquisitions,
        "Client",
        lambda *args, **kwargs: Client(
            *args, **kwargs, transport=httpx.MockTransport(handler), sleep=lambda _: None
        ),
    )
    return SimpleNamespace(
        store=store,
        plan_id=plan_id,
        version=version,
        requests=requests,
        payloads=payloads,
        files=files,
    )


def acquire(case, resume_id=None):
    with case.store.lock:
        return acquisitions.archive(
            case.store, plan_id=case.plan_id, version=case.version, resume_id=resume_id
        )


def captures(case, acquisition_id):
    return case.store.committed_captures(acquisition_id=acquisition_id)


def progress(case, acquisition_id):
    return sorted(
        [
            capture
            for capture in captures(case, acquisition_id)
            if "-progress1-" in capture["unit_id"]
        ],
        key=lambda capture: case.store.read_evidence(capture)["row_group"],
    )


def trace_decode(monkeypatch, case):
    original = archives.pq.ParquetFile
    decoded = []

    class TracedFile:
        def __init__(self, path, **kwargs):
            self.reader = original(path, **kwargs)
            self.schema_arrow, self.metadata = self.reader.schema_arrow, self.reader.metadata
            self.source_object = Path(path).parent == case.store.root / "objects"

        def iter_batches(self, **kwargs):
            if self.source_object:
                decoded.extend(kwargs["row_groups"])
            yield from self.reader.iter_batches(**kwargs)

        def __getattr__(self, name):
            return getattr(self.reader, name)

    monkeypatch.setattr(archives.pq, "ParquetFile", TracedFile)
    return decoded


def interrupt(monkeypatch, case, predicate, *, after=True, occurrence=1):
    original = Store.capture
    seen = 0

    def checkpoint(store, acquisition_id, unit_id, *args, **kwargs):
        nonlocal seen
        stop = False
        if store is case.store and predicate(unit_id):
            seen += 1
            stop = seen == occurrence
        if stop and not after:
            raise RuntimeError("checkpoint interruption")
        result = original(store, acquisition_id, unit_id, *args, **kwargs)
        if stop:
            raise RuntimeError("checkpoint interruption")
        return result

    monkeypatch.setattr(Store, "capture", checkpoint)
    try:
        with pytest.raises(RuntimeError, match="checkpoint interruption"):
            acquire(case)
    finally:
        monkeypatch.setattr(Store, "capture", original)
    return next((case.store.root / "acquisitions").iterdir()).name


def capture_manifest_hash(store, capture):
    return digest(read_bytes(store.capture_path(capture) / "manifest.json"))


def rewrite_evidence(store, capture, change):
    """Keep file hashes valid so tests exercise receipt semantics, not file corruption."""
    evidence = copy.deepcopy(store.read_evidence(capture))
    change(evidence)
    directory = store.capture_path(capture)
    store.atomic_write(directory / "evidence.json", json_bytes(evidence))
    manifest = copy.deepcopy(capture)
    manifest["files"] = file_inventory(directory)
    store.atomic_write(directory / "manifest.json", json_bytes(manifest))


def test_two_completed_groups_resume_only_remaining_groups_and_same_inventory_rebuilds(
    tmp_path, monkeypatch
):
    case = setup_archive(tmp_path, monkeypatch)
    decoded = trace_decode(monkeypatch, case)
    acquisition_id = interrupt(monkeypatch, case, lambda unit: "-progress1-" in unit, occurrence=2)
    assert decoded == [0, 1]
    assert len(progress(case, acquisition_id)) == 2
    decoded.clear()
    case.payloads[next(iter(case.payloads))] = b"remote object changed"
    result = acquire(case, acquisition_id)
    assert decoded == [2, 3]
    assert len(case.requests) == 1 and result["requests"] == result["downloaded_bytes"] == 0
    receipts = progress(case, acquisition_id)
    assert len(receipts) == 4
    assert all(not receipt["tables"] and not receipt["coverage"] for receipt in receipts)
    first = build(case.store, case.plan_id, [acquisition_id])
    assert build(case.store, case.plan_id, [acquisition_id]) == first
    rows = pq.read_table(case.store.root / "published" / first / "book_updates.parquet").to_pylist()
    assert [row["sequence"] for row in rows] == list(range(4))
    assert [row["bids"][0]["quantity"] for row in rows] == list(map(Decimal, ("1", "2", "3", "4")))
    decoded.clear()
    before = {
        capture["capture_id"]: capture_manifest_hash(case.store, capture)
        for capture in captures(case, acquisition_id)
    }
    assert acquire(case, acquisition_id)["requests"] == 0
    assert decoded == [] and len(case.requests) == 1
    assert before == {
        capture["capture_id"]: capture_manifest_hash(case.store, capture)
        for capture in captures(case, acquisition_id)
    }


def test_incomplete_large_group_replays_from_start_and_reuses_original_child(tmp_path, monkeypatch):
    unselected = observation(condition="0x" + "b" * 64)
    groups = [
        [observation()] + [unselected] * 2047 + [observation(quantity="2")] + [unselected] * 1024,
        [observation(quantity="3")],
    ]
    case = setup_archive(tmp_path, monkeypatch, groups)
    decoded = trace_decode(monkeypatch, case)
    acquisition_id = interrupt(monkeypatch, case, lambda unit: "-batch-decoder2-" in unit)
    assert progress(case, acquisition_id) == []
    child = next(
        capture
        for capture in captures(case, acquisition_id)
        if "-batch-decoder2-" in capture["unit_id"]
    )
    original_hash = capture_manifest_hash(case.store, child)
    decoded.clear()
    result = acquire(case, acquisition_id)
    assert result["requests"] == 0 and len(case.requests) == 1
    assert decoded == [0, 1]
    assert capture_manifest_hash(case.store, child) == original_hash
    units = {capture["unit_id"] for capture in captures(case, acquisition_id)}
    assert {
        HOUR + suffix
        for suffix in (
            "-batch-decoder2-rows-0-2048",
            "-batch-decoder2-rows-2048-3073",
            "-batch-decoder2-rows-3073-3074",
        )
    } <= units
    assert [
        case.store.read_evidence(receipt)["observation_counts"]
        for receipt in progress(case, acquisition_id)
    ] == [
        [{"asset_kind": "ctf_token", "asset_id": "123", "observations": 2}],
        [{"asset_kind": "ctf_token", "asset_id": "123", "observations": 1}],
    ]


def test_group_offsets_are_physical_not_globally_aligned_batches(tmp_path, monkeypatch):
    unselected = observation(condition="0x" + "b" * 64)
    groups = [
        [observation()] * 3,
        [unselected] * 2048 + [observation(quantity="2")] * 2,
        [observation(quantity="3")] * 2,
    ]
    case = setup_archive(tmp_path, monkeypatch, groups)
    acquisition_id = interrupt(monkeypatch, case, lambda unit: "-progress1-" in unit)
    decoded = trace_decode(monkeypatch, case)
    acquire(case, acquisition_id)
    assert decoded == [1, 2]
    children = [
        capture
        for capture in captures(case, acquisition_id)
        if "-batch-decoder2-" in capture["unit_id"]
    ]
    positions = sorted(
        row["sequence"]
        for child in children
        for row in case.store.read_relation(child, "book_updates")
    )
    assert positions == [0, 1, 2, 2051, 2052, 2053, 2054]
    assert {capture["unit_id"] for capture in children} == {
        HOUR + suffix
        for suffix in (
            "-batch-decoder2-rows-0-3",
            "-batch-decoder2-rows-2051-2053",
            "-batch-decoder2-rows-2053-2055",
        )
    }


def test_missing_unreferenced_tail_replays_last_group_with_existing_child(tmp_path, monkeypatch):
    case = setup_archive(tmp_path, monkeypatch)
    acquisition_id = interrupt(monkeypatch, case, lambda unit: unit == HOUR, after=False)
    tail = progress(case, acquisition_id)[-1]
    child = next(
        capture
        for capture in captures(case, acquisition_id)
        if capture["unit_id"].endswith("batch-decoder2-rows-3-4")
    )
    original_hash = capture_manifest_hash(case.store, child)
    case.store.remove(case.store.capture_path(tail))
    decoded = trace_decode(monkeypatch, case)
    acquire(case, acquisition_id)
    assert decoded == [3] and len(case.requests) == 1
    assert capture_manifest_hash(case.store, child) == original_hash


@pytest.mark.parametrize(
    "damage",
    [
        "referenced_child_missing",
        "referenced_child_corrupt",
        "previous_missing",
        "hour_tail_missing",
    ],
)
def test_missing_or_corrupt_referenced_evidence_fails_before_decode_or_http(
    tmp_path, monkeypatch, damage
):
    case = setup_archive(tmp_path, monkeypatch)
    acquisition_id = interrupt(
        monkeypatch, case, lambda unit: unit == "acquisition_complete", after=False
    )
    receipts = progress(case, acquisition_id)
    if damage.startswith("referenced_child"):
        identifier = case.store.read_evidence(receipts[0])["data_captures"][0]["capture_id"]
        child = next(
            capture
            for capture in captures(case, acquisition_id)
            if capture["capture_id"] == identifier
        )
        if damage.endswith("missing"):
            case.store.remove(case.store.capture_path(child))
        else:
            case.store.atomic_write(
                case.store.capture_path(child) / "book_updates.parquet", b"corrupt child"
            )
    else:
        case.store.remove(
            case.store.capture_path(receipts[1] if damage == "previous_missing" else receipts[-1])
        )
    decoded = trace_decode(monkeypatch, case)
    with pytest.raises((ValueError, OSError)):
        acquire(case, acquisition_id)
    assert decoded == [] and len(case.requests) == 1
    if damage != "referenced_child_corrupt":
        assert not any(
            capture["unit_id"] == "acquisition_complete"
            for capture in captures(case, acquisition_id)
        )


@pytest.mark.parametrize(
    "field",
    [
        "counts",
        "first_row",
        "stop_row",
        "row_group",
        "decoder_revision",
        "progress_revision",
        "batch_size",
        "previous_hash",
        "previous_absent",
        "omitted_child",
        "source_object",
    ],
)
def test_semantically_invalid_progress_receipts_fail_closed(tmp_path, monkeypatch, field):
    case = setup_archive(tmp_path, monkeypatch)
    acquisition_id = interrupt(monkeypatch, case, lambda unit: unit == HOUR, after=False)
    tail = progress(case, acquisition_id)[-1]

    def damage(evidence):
        if field == "counts":
            evidence["observation_counts"][0]["observations"] += 1
        elif field in (
            "first_row",
            "stop_row",
            "row_group",
            "decoder_revision",
            "progress_revision",
            "batch_size",
        ):
            evidence[field] += 1
        elif field == "previous_hash":
            evidence["previous_progress"]["manifest_sha256"] = "0" * 64
        elif field == "previous_absent":
            evidence["previous_progress"] = None
        elif field == "omitted_child":
            evidence["data_captures"] = []
            evidence["observation_counts"] = []
        else:
            evidence["source_object"]["size"] += 1

    rewrite_evidence(case.store, tail, damage)
    decoded = trace_decode(monkeypatch, case)
    with pytest.raises(ValueError):
        acquire(case, acquisition_id)
    assert decoded == [] and len(case.requests) == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("row_group", False),
        ("first_row", False),
        ("stop_row", True),
        ("progress_revision", True),
        ("decoder_revision", 2.0),
        ("batch_size", 2048.0),
        ("observations", True),
    ],
)
def test_receipt_numbers_require_integers_even_when_numeric_equality_matches(
    tmp_path, monkeypatch, field, value
):
    case = setup_archive(tmp_path, monkeypatch, [[observation()]])
    acquisition_id = interrupt(monkeypatch, case, lambda unit: unit == HOUR, after=False)
    receipt = progress(case, acquisition_id)[0]

    def damage(evidence):
        if field == "observations":
            evidence["observation_counts"][0][field] = value
        else:
            evidence[field] = value

    rewrite_evidence(case.store, receipt, damage)
    decoded = trace_decode(monkeypatch, case)
    with pytest.raises(ValueError):
        acquire(case, acquisition_id)
    assert decoded == [] and len(case.requests) == 1


@pytest.mark.parametrize(
    "field",
    [
        "asset_kind",
        "market_id",
        "condition_id",
        "outcome_index",
        "source",
        "stream",
        "sequence",
        "raw_identity",
        "received_at",
        "receive_order",
    ],
)
def test_valid_hashes_do_not_prove_wrong_persisted_identity_or_receive_position(
    tmp_path, monkeypatch, field
):
    groups = [[observation(quantity=str(index))] * 2 for index in range(1, 5)]
    case = setup_archive(tmp_path, monkeypatch, groups)
    acquisition_id = interrupt(monkeypatch, case, lambda unit: unit == HOUR, after=False)
    tail = progress(case, acquisition_id)[-1]
    identifier = case.store.read_evidence(tail)["data_captures"][0]["capture_id"]
    child = next(
        capture for capture in captures(case, acquisition_id) if capture["capture_id"] == identifier
    )
    rows = case.store.read_relation(child, "book_updates")
    if field == "receive_order":
        rows.reverse()
    else:
        rows[0][field] = {
            "asset_kind": "poly_v2_position",
            "market_id": "reassigned-market",
            "condition_id": "0x" + "b" * 64,
            "outcome_index": 2,
            "source": "pmxt-v2",
            "stream": "different-object-hour",
            "sequence": 8,
            "raw_identity": "different-object:6:0",
            "received_at": NOW - timedelta(milliseconds=1),
        }[field]
    directory = case.store.capture_path(child)
    with case.store.open_output(directory / "book_updates.parquet") as output:
        pq.write_table(pa.Table.from_pylist(rows, schema=UPDATE_SCHEMA), output, compression="zstd")
    manifest = copy.deepcopy(child)
    manifest["files"] = file_inventory(directory)
    case.store.atomic_write(directory / "manifest.json", json_bytes(manifest))

    def rebind(evidence):
        evidence["data_captures"][0]["manifest_sha256"] = capture_manifest_hash(case.store, child)

    rewrite_evidence(case.store, tail, rebind)
    decoded = trace_decode(monkeypatch, case)
    with pytest.raises(ValueError, match="persisted observation identity or receive order"):
        acquire(case, acquisition_id)
    assert decoded == [] and len(case.requests) == 1


@pytest.mark.parametrize("change", ["missing", "null", "hash", "capture_id"])
def test_new_hour_receipt_must_bind_its_complete_progress_tail(tmp_path, monkeypatch, change):
    case = setup_archive(tmp_path, monkeypatch)
    acquisition_id = interrupt(
        monkeypatch, case, lambda unit: unit == "acquisition_complete", after=False
    )
    hour = next(capture for capture in captures(case, acquisition_id) if capture["unit_id"] == HOUR)

    def damage(evidence):
        if change == "missing":
            del evidence["progress_tail"]
        elif change == "null":
            evidence["progress_tail"] = None
        else:
            evidence["progress_tail"]["manifest_sha256" if change == "hash" else "capture_id"] = (
                "0" * 64
            )

    rewrite_evidence(case.store, hour, damage)
    decoded = trace_decode(monkeypatch, case)
    with pytest.raises(ValueError):
        acquire(case, acquisition_id)
    assert decoded == [] and len(case.requests) == 1


@pytest.mark.parametrize("field", ["tables", "plan_id", "source"])
def test_completed_hour_requires_metadata_only_matching_plan_and_source(
    tmp_path, monkeypatch, field
):
    case = setup_archive(tmp_path, monkeypatch)
    acquisition_id = interrupt(
        monkeypatch, case, lambda unit: unit == "acquisition_complete", after=False
    )
    hour = next(capture for capture in captures(case, acquisition_id) if capture["unit_id"] == HOUR)
    directory = case.store.capture_path(hour)
    manifest = copy.deepcopy(hour)
    if field == "tables":
        with case.store.open_output(directory / "book_updates.parquet") as output:
            pq.write_table(pa.Table.from_pylist([], schema=UPDATE_SCHEMA), output)
        manifest["tables"] = {"book_updates": {"rows": 0, "schema": str(UPDATE_SCHEMA)}}
        manifest["files"] = file_inventory(directory)
    elif field == "plan_id":
        manifest[field] = case.store.create_plan(
            {
                "limits": LIMITS.as_dict(),
                "assets": [TARGET | {"market_id": "different-market"}],
                "feeds": ["pmxt-v1"],
                "start": NOW,
                "end": NOW + timedelta(hours=1),
            }
        )
        assert manifest[field] != case.plan_id
    else:
        manifest[field] = "pmxt-v2"
    case.store.atomic_write(directory / "manifest.json", json_bytes(manifest))
    # The bundle and its file hashes remain valid; the new-hour proof must reject its header.
    assert (
        next(capture for capture in captures(case, acquisition_id) if capture["unit_id"] == HOUR)
        == manifest
    )
    decoded = trace_decode(monkeypatch, case)
    with pytest.raises(ValueError):
        acquire(case, acquisition_id)
    assert decoded == [] and len(case.requests) == 1
    assert not any(
        capture["unit_id"] == "acquisition_complete" for capture in captures(case, acquisition_id)
    )


@pytest.mark.parametrize(
    "stage,after",
    [
        ("pin", False),
        ("pin", True),
        ("data", False),
        ("data", True),
        ("progress", False),
        ("progress", True),
        ("hour", False),
        ("hour", True),
        ("complete", False),
        ("complete", True),
    ],
)
def test_checkpoint_crashes_recover_without_redownloading_a_committed_pin(
    tmp_path, monkeypatch, stage, after
):
    case = setup_archive(tmp_path, monkeypatch, [[observation()]])
    predicates = {
        "pin": lambda unit: unit.startswith("source-object-"),
        "data": lambda unit: "-batch-decoder2-" in unit,
        "progress": lambda unit: "-progress1-" in unit,
        "hour": lambda unit: unit == HOUR,
        "complete": lambda unit: unit == "acquisition_complete",
    }
    acquisition_id = interrupt(monkeypatch, case, predicates[stage], after=after)
    result = acquire(case, acquisition_id)
    assert result["complete"]
    assert len(case.requests) == (2 if stage == "pin" and not after else 1)
    assert result["requests"] == (1 if stage == "pin" and not after else 0)
    assert len(progress(case, acquisition_id)) == 1


def test_empty_zero_row_and_reset_only_groups_prove_scanning_without_observations(
    tmp_path, monkeypatch
):
    groups = [
        [],
        [observation(condition="0x" + "b" * 64)],
        [observation(asset_id="999")],
        [observation(quantity="2")],
    ]
    case = setup_archive(tmp_path, monkeypatch, groups)
    acquisition_id = interrupt(monkeypatch, case, lambda unit: "-progress1-" in unit, occurrence=3)
    receipts = progress(case, acquisition_id)
    assert len(receipts) == 3
    first, filtered, reset = [case.store.read_evidence(receipt) for receipt in receipts]
    assert (first["first_row"], first["stop_row"]) == (0, 0)
    assert first["data_captures"] == filtered["data_captures"] == []
    assert (
        first["observation_counts"]
        == filtered["observation_counts"]
        == reset["observation_counts"]
        == []
    )
    assert len(reset["data_captures"]) == 1
    decoded = trace_decode(monkeypatch, case)
    acquire(case, acquisition_id)
    assert decoded == [3]
    final = case.store.read_evidence(progress(case, acquisition_id)[-1])
    assert final["observation_counts"] == [
        {"asset_kind": "ctf_token", "asset_id": "123", "observations": 1}
    ]
    hour = next(capture for capture in captures(case, acquisition_id) if capture["unit_id"] == HOUR)
    assert case.store.read_evidence(hour)["selected_updates"] == 1
    assert hour["coverage"][0]["status"] == "success"


def test_multiple_hours_keep_independent_offsets_streams_and_typed_coverage(tmp_path, monkeypatch):
    second = TARGET | {"asset_id": "124", "outcome_index": 2}
    position = TARGET | {
        "asset_kind": "poly_v2_position",
        "market_id": "2",
        "condition_id": "0x" + "b" * 64,
    }
    later = NOW + timedelta(hours=1)
    case = setup_archive(
        tmp_path,
        monkeypatch,
        version="v2",
        targets=[TARGET, second, position],
        hours={
            NOW: [[observation(version="v2")], [observation(asset_id="999", version="v2")]],
            later: [
                [observation(asset_id="124", stamp=later, version="v2")],
                [
                    observation(
                        asset_id="123",
                        condition=position["condition_id"],
                        stamp=later,
                        version="v2",
                    )
                ],
            ],
        },
    )
    acquisition_id = interrupt(monkeypatch, case, lambda unit: "-progress1-" in unit, occurrence=3)
    result = acquire(case, acquisition_id)
    assert result["requests"] == 0 and len(case.requests) == 2
    children = [
        capture
        for capture in captures(case, acquisition_id)
        if "-batch-decoder2-" in capture["unit_id"]
    ]
    rows = [row for child in children for row in case.store.read_relation(child, "book_updates")]
    assert {row["asset_kind"] for row in rows} == {"ctf_token"}
    assert len({row["stream"] for row in rows}) == 2
    assert sorted(row["sequence"] for row in rows if row["kind"] != "reset") == [0, 0]
    hours = [
        capture
        for capture in captures(case, acquisition_id)
        if capture["unit_id"] in (HOUR, "hour-20260414T09")
    ]
    assert all(
        next(item for item in hour["coverage"] if item["asset_kind"] == "poly_v2_position")[
            "status"
        ]
        == "capability_unknown"
        for hour in hours
    )
    for hour in hours:
        matching = [
            receipt
            for receipt in progress(case, acquisition_id)
            if receipt["unit_id"].startswith(hour["unit_id"] + "-")
        ]
        assert case.store.read_evidence(matching[0])["previous_progress"] is None


def test_legacy_partial_replays_original_object_and_completed_legacy_is_unchanged(
    tmp_path, monkeypatch
):
    case = setup_archive(tmp_path, monkeypatch)
    store = case.store
    acquisition_id = store.new_acquisition(case.plan_id, "pmxt-v1")
    source = store.root / ".state" / "legacy.parquet"
    store.atomic_write(source, case.files[NOW].read_bytes())
    object_info = store.retain_object(source)
    events = list(
        iter_archive(
            store.object_path(object_info),
            version="v1",
            targets=[TARGET],
            start=NOW,
            end=NOW + timedelta(hours=1),
            hour=NOW,
            object_sha256=object_info["sha256"],
        )
    )
    updates = [rows for rows, accounting in events if not accounting.get("group_complete")]
    store.capture(
        acquisition_id,
        HOUR + "-batch-0",
        {"book_updates": updates[0]},
        evidence={"source_object": object_info},
        objects=[object_info],
        schemas={"book_updates": UPDATE_SCHEMA},
    )
    decoded = trace_decode(monkeypatch, case)
    acquire(case, acquisition_id)
    assert decoded == [0, 1, 2, 3] and case.requests == []
    release = build(store, case.plan_id, [acquisition_id])
    assert pq.read_table(store.root / "published" / release / "book_updates.parquet").num_rows == 4

    complete = store.new_acquisition(case.plan_id, "pmxt-v1")
    for index, rows in enumerate(updates):
        store.capture(
            complete,
            HOUR + f"-batch-{index}",
            {"book_updates": rows},
            evidence={"source_object": object_info},
            objects=[object_info],
            schemas={"book_updates": UPDATE_SCHEMA},
        )
    store.capture(
        complete,
        HOUR,
        {},
        evidence={"source_object": object_info, "selected_updates": 4},
        coverage=archive_coverage(
            [TARGET], version="v1", start=NOW, end=NOW + timedelta(hours=1), selected_count=4
        ),
        objects=[object_info],
    )
    acquisitions._finish(store, complete)
    before = {
        capture["capture_id"]: capture_manifest_hash(store, capture)
        for capture in captures(case, complete)
    }
    decoded.clear()
    assert acquire(case, complete)["requests"] == 0
    assert decoded == [] and case.requests == []
    assert before == {
        capture["capture_id"]: capture_manifest_hash(store, capture)
        for capture in captures(case, complete)
    }


@pytest.mark.parametrize("start_group", [-1, True, 1.5, "1", None, 5])
def test_invalid_start_group_is_rejected_before_deserialization(tmp_path, monkeypatch, start_group):
    path = tmp_path / "groups.parquet"
    source_file(path, [[observation()]] * 4, "v1")
    original = archives.pq.ParquetFile

    class NoDecode:
        def __init__(self, filename, **kwargs):
            self.reader = original(filename, **kwargs)
            self.schema_arrow, self.metadata = self.reader.schema_arrow, self.reader.metadata

        def iter_batches(self, **kwargs):
            pytest.fail("invalid continuation reached decode")

    monkeypatch.setattr(archives.pq, "ParquetFile", NoDecode)
    with pytest.raises(ValueError):
        list(
            iter_archive(
                path,
                version="v1",
                targets=[TARGET],
                start=NOW,
                end=NOW + timedelta(hours=1),
                hour=NOW,
                object_sha256="digest",
                start_row_group=start_group,
            )
        )


def test_skipped_groups_still_obey_whole_object_predecode_guards(tmp_path, monkeypatch):
    path = tmp_path / "groups.parquet"
    oversized = observation() | {"data": "x" * 100000}
    source_file(path, [[oversized], [observation()]], "v1")
    with pytest.raises(BudgetExceeded, match="row-group decode"):
        list(
            iter_archive(
                path,
                version="v1",
                targets=[TARGET],
                start=NOW,
                end=NOW + timedelta(hours=1),
                hour=NOW,
                object_sha256="digest",
                start_row_group=1,
                max_decoded_bytes=100000,
            )
        )


def test_interrupted_and_independent_uninterrupted_replays_have_equal_semantics(
    tmp_path, monkeypatch
):
    interrupted = setup_archive(tmp_path / "interrupted", monkeypatch)
    identifier = interrupt(
        monkeypatch, interrupted, lambda unit: "-progress1-" in unit, occurrence=2
    )
    acquire(interrupted, identifier)
    first = build(interrupted.store, interrupted.plan_id, [identifier])
    uninterrupted = setup_archive(tmp_path / "uninterrupted", monkeypatch)
    other = acquire(uninterrupted)["acquisition_id"]
    second = build(uninterrupted.store, uninterrupted.plan_id, [other])
    excluded = {"ingested_at", "acquisition_id", "capture_id"}
    for relation in ("book_updates", "book_states", "coverage"):
        left = pq.read_table(
            interrupted.store.root / "published" / first / (relation + ".parquet")
        ).to_pylist()
        right = pq.read_table(
            uninterrupted.store.root / "published" / second / (relation + ".parquet")
        ).to_pylist()
        assert [
            {key: value for key, value in row.items() if key not in excluded} for row in left
        ] == [{key: value for key, value in row.items() if key not in excluded} for row in right]
