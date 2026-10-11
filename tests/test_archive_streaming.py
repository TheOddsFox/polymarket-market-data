"""Offline regressions for streaming decode and pinned archive recovery."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_market_data import acquisitions, archives, cli
from polymarket_market_data.archives import DECODER_REVISION, V1_SCHEMA, V2_SCHEMA, iter_archive
from polymarket_market_data.books import UPDATE_SCHEMA
from polymarket_market_data.build import build
from polymarket_market_data.foundation import BudgetExceeded, Limits, Store
from polymarket_market_data.transport import Client

NOW = datetime(2026, 4, 14, 8, tzinfo=UTC)
TARGET = {
    "venue": "polymarket",
    "market_id": "1",
    "asset_kind": "ctf_token",
    "asset_id": "123",
    "condition_id": "0x" + "a" * 64,
    "outcome_index": 1,
}
LIMITS = Limits(30, 10**7, 10**8, 10**6, 10**7)


def row(condition=TARGET["condition_id"], *, quantity="1", stamp=NOW, padding=""):
    return {
        "timestamp_received": stamp,
        "timestamp_created_at": stamp,
        "market_id": condition,
        "update_type": "book_snapshot",
        "data": json.dumps(
            {
                "token_id": "123",
                "bids": [{"price": "0.4", "size": quantity}],
                "asks": [],
                "padding": padding,
            }
        ),
    }


def decode(path, **kwargs):
    return iter_archive(
        path,
        version="v1",
        targets=[TARGET],
        start=NOW,
        end=NOW + timedelta(hours=1),
        hour=NOW,
        object_sha256="digest",
        **kwargs,
    )


def setup_cli(tmp_path, monkeypatch, rows):
    store = Store(tmp_path / "data", cli.CONTRACT)
    plan_id = store.create_plan(
        {
            "limits": LIMITS.as_dict(),
            "assets": [TARGET],
            "feeds": ["pmxt-v1"],
            "start": NOW,
            "end": NOW + timedelta(hours=1),
        }
    )
    path = tmp_path / "source.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=V1_SCHEMA), path, row_group_size=1)
    payload = path.read_bytes()
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, content=payload)

    monkeypatch.setattr(
        acquisitions,
        "Client",
        lambda *args, **kwargs: Client(
            *args, **kwargs, transport=httpx.MockTransport(handler), sleep=lambda _: None
        ),
    )
    args = cli.parser().parse_args(
        ["--root", str(store.root), "archive", "backfill", "--plan", plan_id, "--version", "v1"]
    )
    return store, plan_id, args, requests, path


def test_many_row_groups_exceed_whole_file_budget_but_stream_safely(tmp_path):
    path = tmp_path / "groups.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [row("0x" + "b" * 64, padding="x" * 250_000)] * 12 + [row()], schema=V1_SCHEMA
        ),
        path,
        row_group_size=1,
        compression="zstd",
        use_dictionary=["market_id", "update_type"],
    )
    metadata = pq.ParquetFile(path).metadata
    assert sum(metadata.row_group(i).total_byte_size for i in range(13)) > 2 * 1024**2
    result = list(decode(path, max_decoded_bytes=2 * 1024**2, max_row_bytes=1024))
    assert sum(len(updates) for updates, _ in result) == 1
    assert result[-1][0][0]["sequence"] == 12
    assert result[-1][0][0]["raw_identity"] == "digest:12:0"
    assert result[-1][1] == {
        "decoder_revision": DECODER_REVISION,
        "first_row": 12,
        "source_rows": 1,
    }


def test_unselected_rows_are_filtered_before_python_strings_and_json(tmp_path, monkeypatch):
    path = tmp_path / "selection.parquet"
    rejected = row("0x" + "b" * 64, padding="x" * 2048)
    late = row(stamp=NOW + timedelta(hours=1), padding="x" * 2048)
    pq.write_table(pa.Table.from_pylist([rejected, late, row()], schema=V1_SCHEMA), path)
    original = archives.archive_message
    observed = []

    def selected(row, version):
        observed.append(row)
        return original(row, version)

    monkeypatch.setattr(archives, "archive_message", selected)
    result = list(decode(path, max_row_bytes=1024))
    assert len(observed) == 1 and result[0][0][0]["sequence"] == 2


def test_selected_string_limit_precedes_parser_for_every_string_field(tmp_path, monkeypatch):
    path = tmp_path / "strings.parquet"
    oversized = row() | {"update_type": "x" * 4096}
    pq.write_table(pa.Table.from_pylist([oversized], schema=V1_SCHEMA), path)
    monkeypatch.setattr(
        archives, "archive_message", lambda *args: pytest.fail("oversized string materialized")
    )
    with pytest.raises(BudgetExceeded, match="source row"):
        list(decode(path, max_row_bytes=1024))


def test_source_row_work_guard_precedes_decode(tmp_path, monkeypatch):
    path = tmp_path / "work.parquet"
    pq.write_table(pa.Table.from_pylist([row()], schema=V1_SCHEMA), path)
    original = pq.ParquetFile

    class TooManyRows:
        def __init__(self, path, **kwargs):
            reader = original(path, **kwargs)
            self.schema_arrow = reader.schema_arrow
            self.metadata = SimpleNamespace(
                num_rows=1024**2 // 16 + 1,
                num_columns=reader.metadata.num_columns,
                num_row_groups=reader.metadata.num_row_groups,
                row_group=reader.metadata.row_group,
            )

        def iter_batches(self, **kwargs):
            pytest.fail("source-row guard must precede decode")

    monkeypatch.setattr(archives.pq, "ParquetFile", TooManyRows)
    with pytest.raises(BudgetExceeded, match="source rows"):
        list(decode(path, max_decoded_bytes=1024**2))


def test_required_null_in_unselected_row_is_not_hidden(tmp_path, monkeypatch):
    path = tmp_path / "null.parquet"
    pq.write_table(pa.Table.from_pylist([row()], schema=V1_SCHEMA), path)
    invalid = row("0x" + "b" * 64) | {"data": None}
    original = pq.ParquetFile

    class NullReader:
        def __init__(self, path, **kwargs):
            reader = original(path, **kwargs)
            self.schema_arrow, self.metadata = reader.schema_arrow, reader.metadata

        def iter_batches(self, **kwargs):
            yield pa.RecordBatch.from_pylist([invalid], schema=V1_SCHEMA)

    monkeypatch.setattr(archives.pq, "ParquetFile", NullReader)
    with pytest.raises(ValueError, match="null required"):
        list(decode(path))


def test_batches_are_capped_and_each_row_group_is_read_separately(tmp_path, monkeypatch):
    path = tmp_path / "batches.parquet"
    pq.write_table(
        pa.Table.from_pylist([row()] * 5000, schema=V1_SCHEMA), path, row_group_size=3000
    )
    original = pq.ParquetFile
    calls = []

    class Reader:
        def __init__(self, path, **kwargs):
            self.reader = original(path, **kwargs)
            self.schema_arrow, self.metadata = self.reader.schema_arrow, self.reader.metadata

        def iter_batches(self, **kwargs):
            calls.append(kwargs)
            yield from self.reader.iter_batches(**kwargs)

    monkeypatch.setattr(archives.pq, "ParquetFile", Reader)
    result = list(decode(path, batch_size=50_000))
    assert calls == [
        {"batch_size": 2048, "row_groups": [0], "use_threads": False},
        {"batch_size": 2048, "row_groups": [1], "use_threads": False},
    ]
    assert [accounting["first_row"] for _, accounting in result] == [0, 2048, 3000]
    assert sum(len(updates) for updates, _ in result) == 5000


@pytest.mark.parametrize("source_dictionary", [False, True])
def test_only_source_encoded_dictionary_strings_are_preserved(
    tmp_path, monkeypatch, source_dictionary
):
    path = tmp_path / "encoding.parquet"
    observations = [
        row("0x" + "b" * 64, padding="x" * 4096 + ("" if source_dictionary else str(index)))
        for index in range(2500)
    ]
    pq.write_table(
        pa.Table.from_pylist(observations, schema=V1_SCHEMA),
        path,
        row_group_size=1250,
        use_dictionary=True if source_dictionary else ["market_id", "update_type"],
    )
    original = pq.ParquetFile
    dictionaries, data_types = [], []

    class EncodedReader:
        def __init__(self, path, **kwargs):
            self.reader = original(path, **kwargs)
            self.schema_arrow, self.metadata = self.reader.schema_arrow, self.reader.metadata
            if "read_dictionary" in kwargs:
                dictionaries.append(kwargs["read_dictionary"])

        def iter_batches(self, **kwargs):
            for batch in self.reader.iter_batches(**kwargs):
                data_types.append(batch.column("data").type)
                yield batch

    monkeypatch.setattr(archives.pq, "ParquetFile", EncodedReader)
    result = list(decode(path, max_decoded_bytes=32 * 1024**2))
    assert sum(accounting["source_rows"] for _, accounting in result) == 2500
    assert all(not updates for updates, _ in result)
    assert len(dictionaries) == 2
    assert all(("data" in columns) == source_dictionary for columns in dictionaries)
    assert all(pa.types.is_dictionary(kind) == source_dictionary for kind in data_types)


def test_dictionary_plain_fallback_reserves_hash_expansion_before_decode(tmp_path, monkeypatch):
    path = tmp_path / "fallback.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [row("0x" + "b" * 64, padding="x" * 1280 + str(index)) for index in range(1000)],
            schema=V1_SCHEMA,
        ),
        path,
        use_dictionary=True,
        dictionary_pagesize_limit=1024,
        write_batch_size=32,
    )
    original = pq.ParquetFile
    group = original(path).metadata.row_group(0)
    assert {"RLE_DICTIONARY", "PLAIN"}.issubset(group.column(4).encodings)
    assert group.total_byte_size * 4 < 8 * 1024**2
    assert group.column(4).total_uncompressed_size * 16 > 8 * 1024**2

    class UnreadReader:
        def __init__(self, path, **kwargs):
            reader = original(path, **kwargs)
            self.schema_arrow, self.metadata = reader.schema_arrow, reader.metadata

        def iter_batches(self, **kwargs):
            pytest.fail("dictionary fallback decoded before expansion reservation")

    monkeypatch.setattr(archives.pq, "ParquetFile", UnreadReader)
    with pytest.raises(BudgetExceeded, match="row-group decode"):
        list(decode(path, max_decoded_bytes=8 * 1024**2))
    monkeypatch.setattr(archives.pq, "ParquetFile", original)
    result = list(decode(path, max_decoded_bytes=32 * 1024**2))
    assert sum(accounting["source_rows"] for _, accounting in result) == 1000


def test_v2_dictionary_null_optional_fields_and_unknown_assets(tmp_path):
    path = tmp_path / "v2.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "timestamp_received": NOW,
                    "timestamp": NOW,
                    "market": TARGET["condition_id"].encode(),
                    "event_type": "book",
                    "asset_id": asset_id,
                    "bids": "[]",
                    "asks": "[]",
                }
                for asset_id in ("999", "123")
            ],
            schema=V2_SCHEMA,
        ),
        path,
    )
    result = list(
        iter_archive(
            path,
            version="v2",
            targets=[TARGET],
            start=NOW,
            end=NOW + timedelta(hours=1),
            hour=NOW,
            object_sha256="digest",
        )
    )
    assert [update["kind"] for update in result[0][0]] == ["reset", "book"]


def test_pin_precedes_first_decode_and_resume_does_not_fetch_changed_remote(tmp_path, monkeypatch):
    store, _, args, requests, _ = setup_cli(tmp_path, monkeypatch, [row()])
    original = acquisitions.iter_archive

    def crash(*args, **kwargs):
        raise RuntimeError("before first parsed row")

    monkeypatch.setattr(acquisitions, "iter_archive", crash)
    with pytest.raises(RuntimeError, match="first parsed"):
        cli.run(args)
    acquisition = next((store.root / "acquisitions").iterdir()).name
    captures = store.committed_captures(acquisition_id=acquisition)
    assert len(captures) == 1 and captures[0]["unit_id"].startswith("source-object-hour-")
    monkeypatch.setattr(acquisitions, "iter_archive", original)
    args.resume = acquisition
    result = cli.run(args)
    assert len(requests) == 1 and result["requests"] == 0
    completed = store.committed_captures(acquisition_id=acquisition)
    assert any(f"batch-decoder{DECODER_REVISION}-rows-0-1" in row["unit_id"] for row in completed)


def test_empty_selected_archive_has_pin_and_hour_without_empty_data_captures(tmp_path, monkeypatch):
    store, _, args, requests, _ = setup_cli(tmp_path, monkeypatch, [row("0x" + "b" * 64)])
    result = cli.run(args)
    captures = store.committed_captures(acquisition_id=result["acquisition_id"])
    assert len(requests) == 1
    assert all("book_updates" not in capture["tables"] for capture in captures)
    hour = next(capture for capture in captures if capture["unit_id"] == "hour-20260414T08")
    assert hour["coverage"][0]["status"] == "unobserved"
    assert any(capture["unit_id"].startswith("source-object-") for capture in captures)


def test_legacy_partial_batches_replay_new_namespace_and_deduplicate(tmp_path, monkeypatch):
    store, plan_id, args, requests, path = setup_cli(
        tmp_path, monkeypatch, [row(), row(quantity="2")]
    )
    acquisition = store.new_acquisition(plan_id, "pmxt-v1")
    source = store.root / ".state" / "legacy.parquet"
    source.write_bytes(path.read_bytes())
    object_info = store.retain_object(source)
    updates, _ = next(
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
    store.capture(
        acquisition,
        "hour-20260414T08-batch-0",
        {"book_updates": updates},
        evidence={"source_object": object_info},
        objects=[object_info],
        schemas={"book_updates": UPDATE_SCHEMA},
    )
    args.resume = acquisition
    cli.run(args)
    assert not requests
    captures = store.committed_captures(acquisition_id=acquisition)
    assert any("batch-decoder" in capture["unit_id"] for capture in captures)
    release = build(store, plan_id, [acquisition])
    updates = pq.read_table(store.root / "published" / release / "book_updates.parquet").to_pylist()
    assert len(updates) == 2
    assert [update["bids"][0]["quantity"] for update in updates] == [Decimal("1"), Decimal("2")]
