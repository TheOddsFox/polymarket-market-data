"""Paid account reservations survive failure, adapters and process restarts."""

import httpx
import pytest

from polymarket_market_data.budget import BudgetLedger
from polymarket_market_data.foundation import BudgetExceeded, Limits
from polymarket_market_data.transport import Client, SourceError


def limits(requests=10):
    return Limits(requests, 100_000, 1_000_000, 100_000, 100_000)


def configured(tmp_path, cap=3):
    ledger = BudgetLedger(tmp_path)
    ledger.configure("allocated-account", "2026-10", "manual-allocation", cap)
    return ledger


def paid_client(ledger, transport, **kwargs):
    return Client(
        "pmxt",
        limits(),
        paid_ledger=ledger,
        account="allocated-account",
        period="2026-10",
        allowance_id="manual-allocation",
        transport=transport,
        sleep=lambda _: None,
        **kwargs,
    )


def test_every_retry_is_reserved_durably_before_transport_io(tmp_path):
    ledger = configured(tmp_path)
    observed = []

    def failed(request):
        observed.append(BudgetLedger(tmp_path).status()["allocations"][0]["used"])
        raise httpx.ReadError("API-key-shaped-sensitive-text", request=request)

    with paid_client(ledger, httpx.MockTransport(failed)) as client:
        with pytest.raises(SourceError) as error:
            client.get_json("https://api.pmxt.dev/api/polymarket/fetchTrades")
    assert observed == [1, 2, 3]
    assert "sensitive" not in str(error.value)
    with pytest.raises(BudgetExceeded):
        BudgetLedger(tmp_path).reserve("allocated-account", "2026-10", "manual-allocation")


def test_books_and_trades_share_one_account_period_allocation(tmp_path):
    first = configured(tmp_path, cap=2)
    second = BudgetLedger(tmp_path)
    called = []

    def respond(request):
        called.append(request.url.path)
        return httpx.Response(200, json={"success": True, "data": []})

    transport = httpx.MockTransport(respond)
    with paid_client(first, transport) as books, paid_client(second, transport) as trades:
        books.post_json("https://api.pmxt.dev/api/polymarket/fetchOrderBook", {"args": []})
        trades.get_json("https://api.pmxt.dev/api/polymarket/fetchTrades")
        with pytest.raises(BudgetExceeded):
            books.post_json("https://api.pmxt.dev/api/polymarket/fetchOrderBook", {"args": []})
    assert called == ["/api/polymarket/fetchOrderBook", "/api/polymarket/fetchTrades"]
    assert second.status()["allocations"][0]["used"] == 2


def test_unknown_transport_failure_does_not_refund_reserved_attempt(tmp_path):
    ledger = configured(tmp_path, cap=1)

    def crash(request):
        raise RuntimeError("process stopped after dispatch")

    with paid_client(ledger, httpx.MockTransport(crash)) as client:
        with pytest.raises(RuntimeError):
            client.get_json("https://api.pmxt.dev/api/polymarket/fetchTrades")
    restarted = BudgetLedger(tmp_path)
    assert restarted.status()["allocations"][0]["used"] == 1
    with pytest.raises(BudgetExceeded):
        restarted.reserve("allocated-account", "2026-10", "manual-allocation")


def test_same_allocation_reconfiguration_cannot_reset_spend(tmp_path):
    ledger = configured(tmp_path, cap=2)
    ledger.reserve("allocated-account", "2026-10", "manual-allocation")
    ledger.configure("allocated-account", "2026-10", "manual-allocation", 2)
    assert ledger.status()["allocations"][0]["used"] == 1
    with pytest.raises(ValueError, match="reset|replace"):
        ledger.configure("allocated-account", "2026-10", "replacement", 100)
    with pytest.raises(ValueError, match="reset|replace"):
        ledger.configure("allocated-account", "2026-10", "manual-allocation", 100)


@pytest.mark.parametrize("failure", ["missing", "corrupt"])
def test_lost_or_corrupt_ledger_blocks_existing_and_new_clients(tmp_path, failure):
    ledger = configured(tmp_path)
    if failure == "missing":
        ledger.path.unlink()
    else:
        ledger.path.write_bytes(b"not a SQLite database")
    with pytest.raises(ValueError):
        ledger.reserve("allocated-account", "2026-10", "manual-allocation")
    with pytest.raises(ValueError):
        BudgetLedger(tmp_path)
    with pytest.raises(ValueError):
        ledger.configure("allocated-account", "2026-10", "manual-allocation", 3)


def test_account_period_rotation_does_not_rewrite_previous_spend(tmp_path):
    ledger = configured(tmp_path, cap=1)
    ledger.reserve("allocated-account", "2026-10", "manual-allocation")
    ledger.configure("allocated-account", "2026-11", "next-period", 1)
    ledger.reserve("allocated-account", "2026-11", "next-period")
    assert [(row["period"], row["used"]) for row in ledger.status()["allocations"]] == [
        ("2026-10", 1),
        ("2026-11", 1),
    ]


def test_paid_accounting_obeys_retained_budget_before_attempt(tmp_path):
    from polymarket_market_data.foundation import Limits, Store

    store = Store(
        tmp_path / "root", "oddsfox.polymarket.TEST.v1", Limits(3, 100000, 200000, 100000, 100000)
    )
    ledger = BudgetLedger(store.root, store=store)
    ledger.configure("acct", "period", "allocation", 3)
    ledger.reserve("acct", "period", "allocation")
    used = ledger.status()["allocations"][0]["used"]
    retained = sum(p.stat().st_size for p in store.root.rglob("*") if p.is_file())
    store.limits = Limits(3, 100000, retained + 1000, 100000, 100000)
    with pytest.raises(BudgetExceeded, match="accounting disk budget"):
        ledger.reserve("acct", "period", "allocation")
    assert ledger.status()["allocations"][0]["used"] == used == 1
    assert (
        sum(p.stat().st_size for p in store.root.rglob("*") if p.is_file())
        <= store.limits.max_storage_bytes
    )


def test_paid_configuration_disk_cap_fails_before_credit_is_reserved(tmp_path):
    from polymarket_market_data.foundation import Limits, Store

    store = Store(
        tmp_path / "root", "oddsfox.polymarket.TEST.v1", Limits(3, 100000, 1000, 100000, 100000)
    )
    ledger = BudgetLedger(store.root, store=store)
    with pytest.raises(BudgetExceeded, match="accounting disk budget"):
        ledger.configure("acct", "period", "allocation", 3)
    assert not ledger.sentinel.exists()


def test_established_root_cannot_reset_after_ledger_and_marker_loss(tmp_path):
    from polymarket_market_data.foundation import Limits, Store

    store = Store(
        tmp_path / "root", "oddsfox.polymarket.TEST.v1", Limits(3, 100000, 200000, 100000, 100000)
    )
    ledger = BudgetLedger(store.root, store=store)
    ledger.configure("acct", "period", "allocation", 3)
    ledger.reserve("acct", "period", "allocation")
    ledger.path.unlink()
    ledger.sentinel.unlink()
    with pytest.raises(ValueError, match="established root"):
        BudgetLedger(store.root, store=store)
