from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from polymarket_market_data.books import Book, normalize_message, resets

TARGET = {
    "venue": "polymarket",
    "market_id": "1",
    "asset_kind": "ctf_token",
    "asset_id": "123",
    "condition_id": "0x" + "a" * 64,
}
NOW = datetime(2025, 1, 1, tzinfo=UTC)


def row(message, sequence, source="clob-ws", stamp=NOW, targets=None):
    return normalize_message(
        message,
        targets=targets or [TARGET],
        source=source,
        stream="test",
        sequence=sequence,
        received_at=stamp,
    )[0]


def snapshot(sequence=1, source="clob-ws", stamp=NOW):
    return row(
        {
            "event_type": "book",
            "asset_id": "123",
            "market": TARGET["condition_id"],
            "bids": [{"price": "0.4", "size": "2"}],
            "asks": [{"price": "0.6", "size": "3"}],
        },
        sequence,
        source,
        stamp,
    )


def test_snapshot_zero_delete_and_gap_require_new_snapshot():
    book = Book()
    assert book.apply(snapshot())["midpoint"] == Decimal("0.5")
    change = row(
        {
            "event_type": "price_change",
            "market": TARGET["condition_id"],
            "price_changes": [{"asset_id": "123", "side": "BUY", "price": "0.4", "size": "0"}],
        },
        2,
    )
    assert book.apply(change)["status"] == "one_sided"
    reset = resets(
        [TARGET],
        source="clob-ws",
        stream="test",
        sequence=3,
        received_at=NOW,
        reason="connection_gap",
    )[0]
    assert book.apply(reset)["status"] == "connection_gap"
    assert book.apply(change | {"sequence": 4})["status"] == "connection_gap"
    assert book.apply(snapshot(5))["status"] == "valid"


def test_archive_ambiguous_same_timestamp_cannot_heal_with_snapshot():
    book = Book()
    assert book.apply(snapshot(source="pmxt-v2"))["status"] == "valid"
    other = snapshot(2, "pmxt-v2")
    other["bids"][0]["quantity"] = Decimal("4")
    assert book.apply(other)["status"] == "ambiguous_archive_order"
    assert book.apply(snapshot(3, "pmxt-v2"))["status"] == "ambiguous_archive_order"
    assert book.apply(snapshot(4, "pmxt-v2", NOW + timedelta(milliseconds=1)))["status"] == "valid"


def test_same_numeric_id_in_two_ledgers_not_guessed():
    with pytest.raises(ValueError, match="ambiguous"):
        row(
            {"event_type": "book", "asset_id": "123", "bids": [], "asks": []},
            1,
            targets=[TARGET, TARGET | {"asset_kind": "poly_v2_position"}],
        )


def test_position_asset_uses_native_identity():
    position = TARGET | {"asset_kind": "poly_v2_position"}
    assert (
        row(
            {"event_type": "book", "asset_id": "123", "bids": [], "asks": []}, 1, targets=[position]
        )["asset_kind"]
        == "poly_v2_position"
    )


def test_duplicate_level_and_binary_float_rejected():
    for bids in (
        [{"price": "0.4", "size": "1"}, {"price": "0.4", "size": "2"}],
        [{"price": 0.4, "size": "1"}],
    ):
        with pytest.raises(ValueError):
            row({"event_type": "book", "asset_id": "123", "bids": bids, "asks": []}, 1)
