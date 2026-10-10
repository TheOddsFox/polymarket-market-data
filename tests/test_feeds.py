from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from polymarket_market_data.books import parse_json
from polymarket_market_data.feeds import acquire_history, acquire_paid, history_points

TARGET = {
    "venue": "polymarket",
    "market_id": "1",
    "asset_kind": "ctf_token",
    "asset_id": "123",
    "condition_id": "0x" + "a" * 64,
}
START = datetime(2025, 1, 1, tzinfo=UTC)


def test_history_exact_half_open_and_duplicate_conflicts():
    t = int(START.timestamp())
    payload = parse_json(
        f'{{"history":[{{"t":{t},"p":0.123456789012345678}},{{"t":{t},"p":"0.123456789012345678"}},{{"t":{t + 1},"p":1}}]}}'
    )
    rows = history_points(
        payload, target=TARGET, start=START, end=START + timedelta(seconds=1), received_at=START
    )
    assert len(rows) == 1
    assert rows[0]["price"] == Decimal("0.123456789012345678")
    payload["history"][1]["p"] = "0.2"
    with pytest.raises(ValueError, match="conflicting"):
        history_points(
            payload, target=TARGET, start=START, end=START + timedelta(seconds=1), received_at=START
        )


@pytest.mark.parametrize(
    "payload",
    [{}, {"history": None}, {"history": [{}]}, {"history": [{"t": 0, "p": float("nan")}]}],
)
def test_malformed_history_is_never_empty(payload):
    with pytest.raises(ValueError):
        history_points(
            payload, target=TARGET, start=START, end=START + timedelta(seconds=1), received_at=START
        )


def test_history_empty_is_accounted_and_requests_are_bounded_windows():
    requests = []

    def request(*args, **kwargs):
        requests.append(kwargs["params"])
        return {"history": []}

    rows, evidence, covered = acquire_history(
        request, target=TARGET, start=START, end=START + timedelta(seconds=3), max_window_seconds=2
    )
    assert rows == []
    assert len(requests) == 2
    assert len(evidence["responses"]) == 2
    assert [item["status"] for item in covered] == ["confirmed_empty", "confirmed_empty"]


def test_paid_positions_do_not_probe():
    def forbidden(*args, **kwargs):
        pytest.fail("unknown capability made a paid request")

    rows, _, covered = acquire_paid(
        forbidden,
        target=TARGET | {"asset_kind": "poly_v2_position"},
        source="pmxt-trades",
        start=START,
        end=START + timedelta(seconds=1),
        api_key="test",
    )
    assert not rows
    assert covered[0]["status"] == "capability_unknown"


def test_paid_split_exact_empty_and_trade_identity_conflict():
    requests = []

    def request(*args, **kwargs):
        requests.append(kwargs["params"])
        return (
            {
                "success": True,
                "data": [
                    {
                        "id": "same",
                        "timestamp": int(START.timestamp() * 1000),
                        "price": "0.4",
                        "amount": "1",
                    }
                ],
            }
            if len(requests) == 1
            else {"success": True, "data": []}
        )

    rows, _, covered = acquire_paid(
        request,
        target=TARGET,
        source="pmxt-trades",
        start=START,
        end=START + timedelta(milliseconds=4),
        api_key="test",
        limit=1,
    )
    assert rows == []
    assert len(requests) == 3
    assert all(item["status"] == "confirmed_empty" for item in covered)


def test_paid_irreducible_saturation_blocks():
    def request(*args, **kwargs):
        return {"success": True, "data": [{}]}

    with pytest.raises(ValueError, match="irreducibly saturated"):
        acquire_paid(
            request,
            target=TARGET,
            source="pmxt-trades",
            start=START,
            end=START + timedelta(milliseconds=1),
            api_key="test",
            limit=1,
        )


def test_paid_duplicate_trade_conflicting_values_rejected():
    stamp = int(START.timestamp() * 1000)

    def request(*args, **kwargs):
        return {
            "success": True,
            "data": [
                {"id": "same", "timestamp": stamp, "price": value, "amount": "1"}
                for value in ("0.4", "0.5")
            ],
        }

    with pytest.raises(ValueError, match="conflicting"):
        acquire_paid(
            request,
            target=TARGET,
            source="pmxt-trades",
            start=START,
            end=START + timedelta(seconds=1),
            api_key="test",
        )
