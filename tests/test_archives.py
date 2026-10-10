import json
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_market_data import archives
from polymarket_market_data.archives import (
    V1_SCHEMA,
    V2_SCHEMA,
    archive_coverage,
    archive_hours,
    iter_archive,
)
from polymarket_market_data.foundation import BudgetExceeded

TARGET = {
    "venue": "polymarket",
    "market_id": "1",
    "asset_kind": "ctf_token",
    "asset_id": "123",
    "condition_id": "0x" + "a" * 64,
}
NOW = datetime(2025, 1, 1, tzinfo=UTC)


def test_v1_selected_native_identity_and_original_row_order(tmp_path):
    def observation(stamp, condition=TARGET["condition_id"]):
        return {
            "timestamp_received": stamp,
            "timestamp_created_at": stamp,
            "market_id": condition,
            "update_type": "book_snapshot",
            "data": json.dumps({"token_id": "123", "bids": [], "asks": []}),
        }

    path = tmp_path / "v1.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                observation(NOW + timedelta(milliseconds=1)),
                observation(NOW),
                observation(NOW, "0x" + "b" * 64),
            ],
            schema=V1_SCHEMA,
        ),
        path,
    )
    batches = list(
        iter_archive(
            path,
            version="v1",
            targets=[TARGET],
            start=NOW,
            end=NOW + timedelta(hours=1),
            hour=NOW,
            object_sha256="digest",
            batch_size=1,
        )
    )
    updates = [row for rows, _ in batches for row in rows]
    assert [row["sequence"] for row in updates] == [0, 1]
    assert updates[0]["received_at"] > updates[1]["received_at"]
    assert updates[0]["asset_kind"] == "ctf_token"
    assert sum(batch[1]["source_rows"] for batch in batches) == 3


def test_v2_empty_snapshot_is_observation_and_zero_not_missing(tmp_path):
    path = tmp_path / "v2.parquet"
    rows = [
        {
            "timestamp_received": NOW,
            "timestamp": NOW,
            "market": TARGET["condition_id"].encode(),
            "event_type": "book",
            "asset_id": "123",
            "bids": "[]",
            "asks": "[]",
        }
    ]
    pq.write_table(pa.Table.from_pylist(rows, schema=V2_SCHEMA), path)
    updates = list(
        iter_archive(
            path,
            version="v2",
            targets=[TARGET],
            start=NOW,
            end=NOW + timedelta(hours=1),
            hour=NOW,
            object_sha256="digest",
        )
    )[0][0]
    assert updates[0]["bids"] == []
    assert (
        archive_coverage(
            [TARGET], version="v2", start=NOW, end=NOW + timedelta(hours=1), selected_count=0
        )[0]["status"]
        == "unobserved"
    )
    assert (
        archive_coverage(
            [TARGET],
            version="v2",
            start=NOW,
            end=NOW + timedelta(hours=1),
            selected_count=0,
            present=False,
        )[0]["status"]
        == "missing_object"
    )


def test_wrong_schema_and_null_required_are_rejected(tmp_path):
    path = tmp_path / "bad.parquet"
    pq.write_table(pa.Table.from_pylist([{"foo": 1}]), path)
    with pytest.raises(ValueError, match="schema"):
        list(
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


def test_hour_inventory_is_half_open_and_version_pinned():
    hours = list(archive_hours(NOW + timedelta(minutes=30), NOW + timedelta(hours=2), "v2"))
    assert len(hours) == 2
    assert hours[0]["url"].startswith("https://r2v2.pmxt.dev/")


def test_archive_unknown_asset_resets_related_known_books(tmp_path):
    path = tmp_path / "v2.parquet"
    row = {
        "timestamp_received": NOW,
        "timestamp": NOW,
        "market": TARGET["condition_id"].encode(),
        "event_type": "book",
        "asset_id": "999",
        "bids": "[]",
        "asks": "[]",
    }
    pq.write_table(pa.Table.from_pylist([row], schema=V2_SCHEMA), path)
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
    )[0][0]
    assert result[0]["kind"] == "reset"
    assert result[0]["reason"] == "unknown_archive_identity"


def test_compressed_archive_footer_is_checked_before_deserialization(tmp_path):
    path = tmp_path / "compressed.parquet"
    data = json.dumps({"token_id": "123", "bids": [], "asks": [], "padding": "a" * 2**21})
    row = {
        "timestamp_received": NOW,
        "timestamp_created_at": NOW,
        "market_id": TARGET["condition_id"],
        "update_type": "book_snapshot",
        "data": data,
    }
    pq.write_table(pa.Table.from_pylist([row], schema=V1_SCHEMA), path, compression="zstd")
    assert path.stat().st_size < 64 * 1024
    with pytest.raises(BudgetExceeded, match="uncompressed footer"):
        list(
            iter_archive(
                path,
                version="v1",
                targets=[TARGET],
                start=NOW,
                end=NOW + timedelta(hours=1),
                hour=NOW,
                object_sha256="digest",
                max_decoded_bytes=64 * 1024,
            )
        )


def test_source_json_row_limit_precedes_message_parser(tmp_path):
    path = tmp_path / "large-row.parquet"
    row = {
        "timestamp_received": NOW,
        "timestamp_created_at": NOW,
        "market_id": TARGET["condition_id"],
        "update_type": "book_snapshot",
        "data": "x" * 2048,
    }
    pq.write_table(pa.Table.from_pylist([row], schema=V1_SCHEMA), path)
    with pytest.raises(BudgetExceeded, match="source row"):
        list(
            iter_archive(
                path,
                version="v1",
                targets=[TARGET],
                start=NOW,
                end=NOW + timedelta(hours=1),
                hour=NOW,
                object_sha256="digest",
                max_decoded_bytes=1024**2,
                max_row_bytes=1024,
            )
        )


def test_dictionary_repetition_is_bounded_before_arrow_deserialization(tmp_path, monkeypatch):
    path = tmp_path / "dictionary.parquet"
    data = json.dumps(
        {"token_id": "123", "bids": [], "asks": [], "padding": "a" * (1024**2 - 4096)}
    )
    row = {
        "timestamp_received": NOW,
        "timestamp_created_at": NOW,
        "market_id": TARGET["condition_id"],
        "update_type": "book_snapshot",
        "data": data,
    }
    pq.write_table(
        pa.Table.from_pylist([row] * 20, schema=V1_SCHEMA),
        path,
        compression="zstd",
        use_dictionary=True,
        dictionary_pagesize_limit=4 * 1024**2,
    )
    original = pq.ParquetFile
    inspected = original(path)
    uncompressed = sum(
        inspected.metadata.row_group(0).column(column).total_uncompressed_size
        for column in range(inspected.metadata.num_columns)
    )
    assert uncompressed < 2 * 1024**2
    assert len(data) * 20 > 16 * 1024**2
    batches = []

    class TracedFile:
        def __init__(self, filename):
            self.file = original(filename)
            self.metadata, self.schema_arrow = self.file.metadata, self.file.schema_arrow

        def iter_batches(self, *, batch_size):
            batches.append(batch_size)
            assert batch_size <= (16 * 1024**2) // (4 * uncompressed)
            yield from self.file.iter_batches(batch_size=batch_size)

    monkeypatch.setattr(archives.pq, "ParquetFile", TracedFile)
    result = list(
        iter_archive(
            path,
            version="v1",
            targets=[TARGET],
            start=NOW,
            end=NOW + timedelta(hours=1),
            hour=NOW,
            object_sha256="digest",
            max_decoded_bytes=16 * 1024**2,
        )
    )
    assert batches and sum(len(rows) for rows, _ in result) == 20
    assert all(accounting["source_rows"] <= batches[0] for _, accounting in result)


def test_oversized_dictionary_group_is_rejected_before_arrow_decode(tmp_path, monkeypatch):
    path = tmp_path / "dictionary.parquet"
    row = {
        "timestamp_received": NOW,
        "timestamp_created_at": NOW,
        "market_id": TARGET["condition_id"],
        "update_type": "book_snapshot",
        "data": "a" * (5 * 1024**2),
    }
    pq.write_table(
        pa.Table.from_pylist([row] * 2, schema=V1_SCHEMA),
        path,
        compression="zstd",
        use_dictionary=True,
        dictionary_pagesize_limit=8 * 1024**2,
    )
    original = pq.ParquetFile

    class UnreadFile:
        def __init__(self, filename):
            file = original(filename)
            self.metadata, self.schema_arrow = file.metadata, file.schema_arrow

        def iter_batches(self, **kwargs):
            pytest.fail("source deserialized before row-group budget rejection")

    monkeypatch.setattr(archives.pq, "ParquetFile", UnreadFile)
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
                max_decoded_bytes=16 * 1024**2,
            )
        )
