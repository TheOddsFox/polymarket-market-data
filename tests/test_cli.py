from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_market_data import acquisitions, cli
from polymarket_market_data import producer as metadata_producer
from polymarket_market_data.archives import V1_SCHEMA
from polymarket_market_data.build import build
from polymarket_market_data.feeds import PRICE_SCHEMA, coverage
from polymarket_market_data.foundation import (
    BudgetExceeded,
    Limits,
    Store,
    atomic_write,
    digest,
    json_bytes,
    json_loads,
)
from polymarket_market_data.transport import Client, SourceError

TARGET = {
    "venue": "polymarket",
    "market_id": "1",
    "asset_kind": "ctf_token",
    "asset_id": "123",
    "clob_token_id": "123",
    "position_id": None,
    "condition_id": "0x" + "a" * 64,
    "outcome_index": 1,
    "usable": True,
}
NOW = datetime(2025, 1, 1, tzinfo=UTC)
LIMITS = Limits(30, 10**7, 10**8, 10**6, 10**7)


def meter(limits=LIMITS):
    return SimpleNamespace(limits=limits, requests=0, downloaded=0)


def metadata(path):
    path.mkdir()
    records = {
        "markets.json": [{"venue": "polymarket", "market_id": "1", "usable": True}],
        "outcomes.json": [TARGET],
        "memberships.json": [],
        "identity_history.json": [],
        "coverage.json": [{"market_id": "1", "status": "found"}],
    }
    files = {}
    for name, rows in records.items():
        raw = json_bytes(rows)
        atomic_write(path / name, raw)
        files[name] = {"sha256": digest(raw), "size": len(raw)}
    raw = json_bytes({"contract": "oddsfox.polymarket.metadata.v1", "files": files})
    atomic_write(path / "manifest.json", raw)
    return digest(raw)


def plan(store, start=NOW, end=NOW + timedelta(seconds=10)):
    return store.create_plan(
        {
            "limits": LIMITS.as_dict(),
            "assets": [TARGET],
            "market_ids": ["1"],
            "feeds": ["clob-history"],
            "start": start,
            "end": end,
            "fidelity": 1,
            "duration": 1,
        }
    )


def capture_complete(store, plan_id, price):
    acquisition = store.new_acquisition(plan_id, "clob-history")
    row = TARGET | {
        "source": "clob-history",
        "source_at": NOW,
        "received_at": NOW,
        "ingested_at": NOW,
        "price": Decimal(price),
    }
    capture = store.capture(
        acquisition,
        "sample",
        {"price_samples": [row]},
        coverage=[coverage(TARGET, "clob-history", NOW, NOW + timedelta(seconds=10), "success")],
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


def test_capture_scoped_source_revisions_and_stable_empty_relations(tmp_path):
    store = Store(tmp_path / "data", cli.CONTRACT)
    plan_id = plan(store)
    first, second = capture_complete(store, plan_id, "0.4"), capture_complete(store, plan_id, "0.5")
    release = build(store, plan_id, [first, second])
    rows = pq.read_table(store.root / "published" / release / "price_samples.parquet").to_pylist()
    assert {row["price"] for row in rows} == {Decimal("0.4"), Decimal("0.5")}
    assert len({row["capture_id"] for row in rows}) == 2
    trades = pq.read_table(store.root / "published" / release / "trades.parquet")
    assert trades.num_rows == 0 and "trade_id" in trades.schema.names
    assert pq.read_table(store.root / "published" / release / "sampled_ohlc.parquet").num_rows == 2
    store.publish(release)


def test_partial_capture_inventory_blocks_release(tmp_path):
    store = Store(tmp_path / "data", cli.CONTRACT)
    plan_id = plan(store)
    acquisition = store.new_acquisition(plan_id, "clob-history")
    store.capture(acquisition, "partial", {})
    with pytest.raises(ValueError, match="unfinished"):
        build(store, plan_id, [acquisition])


def test_cli_plan_capture_build_outside_checkout(tmp_path, monkeypatch, capsys):
    bundle = tmp_path / "metadata"
    pinned = metadata(bundle)
    monkeypatch.chdir(tmp_path)
    assert (
        cli.main(
            [
                "--root",
                "output",
                "plan",
                "--metadata",
                str(bundle),
                "--metadata-sha256",
                pinned,
                "--market-id",
                "1",
                "--feed",
                "clob-history",
                "--start",
                NOW.isoformat(),
                "--end",
                (NOW + timedelta(seconds=1)).isoformat(),
            ]
        )
        == 0
    )
    plan_id = json_loads(capsys.readouterr().out)["plan_id"]
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, text='{"history": []}')

    monkeypatch.setattr(
        acquisitions,
        "Client",
        lambda *args, **kwargs: Client(
            *args, **kwargs, transport=httpx.MockTransport(handler), sleep=lambda _: None
        ),
    )
    assert cli.main(["--root", "output", "prices", "backfill", "--plan", plan_id]) == 0
    acquisition = json_loads(capsys.readouterr().out)["acquisition_id"]
    assert len(calls) == 1
    assert (
        cli.main(
            ["--root", "output", "prices", "backfill", "--plan", plan_id, "--resume", acquisition]
        )
        == 0
    )
    capsys.readouterr()
    assert len(calls) == 1
    assert (
        cli.main(["--root", "output", "build", "--plan", plan_id, "--acquisition", acquisition])
        == 0
    )
    release = json_loads(capsys.readouterr().out)["release_id"]
    assert cli.main(["--root", "output", "publish", "--release", release]) == 0


def test_resume_uses_committed_http_without_repeating_request(tmp_path, monkeypatch):
    store = Store(tmp_path / "data", cli.CONTRACT)
    plan_id = plan(store, end=NOW + timedelta(days=2))
    calls = []

    def handler(request):
        calls.append(request)
        if len(calls) == 2:
            raise SourceError("failed")
        return httpx.Response(200, text='{"history": []}')

    monkeypatch.setattr(
        acquisitions,
        "Client",
        lambda *args, **kwargs: Client(
            *args, **kwargs, transport=httpx.MockTransport(handler), sleep=lambda _: None
        ),
    )
    args = cli.parser().parse_args(
        ["--root", str(store.root), "prices", "backfill", "--plan", plan_id]
    )
    with pytest.raises(SourceError):
        cli.run(args)
    acquisition = next((store.root / "acquisitions").iterdir()).name
    args.resume = acquisition
    result = cli.run(args)
    assert result["complete"] is True
    assert len(calls) == 3


def test_paid_disabled_before_http(tmp_path, monkeypatch):
    store = Store(tmp_path / "data", cli.CONTRACT)
    plan_id = store.create_plan(
        {
            "limits": LIMITS.as_dict(),
            "assets": [TARGET],
            "feeds": ["pmxt-trades"],
            "start": NOW,
            "end": NOW + timedelta(seconds=1),
        }
    )
    monkeypatch.setattr(
        acquisitions,
        "Client",
        lambda *args, **kwargs: pytest.fail("made client before paid opt-in"),
    )
    args = cli.parser().parse_args(
        [
            "--root",
            str(store.root),
            "trades",
            "backfill",
            "--plan",
            plan_id,
            "--account",
            "acct",
            "--period",
            "2025-01",
            "--allowance-id",
            "allocation",
            "--allowance-cap",
            "10",
        ]
    )
    with pytest.raises(ValueError, match="enable-paid"):
        cli.run(args)


def test_plan_enforces_metadata_storage_budget_before_pin(tmp_path):
    bundle = tmp_path / "metadata"
    pinned = metadata(bundle)
    store = Store(tmp_path / "data", cli.CONTRACT)
    args = cli.parser().parse_args(
        [
            "plan",
            "--metadata",
            str(bundle),
            "--metadata-sha256",
            pinned,
            "--market-id",
            "1",
            "--feed",
            "clob-history",
            "--start",
            NOW.isoformat(),
            "--end",
            (NOW + timedelta(seconds=1)).isoformat(),
            "--max-storage-bytes",
            "1",
        ]
    )
    with pytest.raises(BudgetExceeded):
        cli._plan(store, args)
    assert list((store.root / "inputs").iterdir()) == []
    assert list((store.root / "plans").iterdir()) == []


def test_live_metadata_output_is_temporary_and_removed_on_failure(tmp_path, monkeypatch):
    store = Store(tmp_path / "data", cli.CONTRACT)
    plan_data = store.load_plan(plan(store))
    seen = []

    def producer(argv, **kwargs):
        output = Path(argv[argv.index("--output") + 1])
        seen.append(output)
        output.mkdir()
        (output / "unfinished").write_text("source data")
        return SimpleNamespace(returncode=2, stdout="")

    monkeypatch.setattr(metadata_producer, "bounded_run", producer)
    args = SimpleNamespace(events_executable="catalogue")
    with pytest.raises(ValueError, match="valid resource accounting"):
        acquisitions._refresh_live_metadata(
            store, plan_data, args.events_executable, client=meter()
        )
    assert seen and not seen[0].exists()
    assert list((store.root / "inputs").iterdir()) == []


def test_live_metadata_success_leaves_only_one_pinned_copy(tmp_path, monkeypatch):
    store = Store(tmp_path / "data", cli.CONTRACT)
    plan_data = store.load_plan(plan(store))
    outputs = []

    def producer(argv, **kwargs):
        output = Path(argv[argv.index("--output") + 1])
        outputs.append(output)
        sha = metadata(output)
        return SimpleNamespace(
            returncode=0,
            stdout=json_bytes(
                {"manifest_sha256": sha, "http_attempts": 1, "downloaded_bytes": 64}
            ).decode(),
        )

    monkeypatch.setattr(metadata_producer, "bounded_run", producer)
    bundle = acquisitions._refresh_live_metadata(
        store,
        plan_data,
        "catalogue",
        client=meter(),
    )
    assert bundle.path.exists()
    assert list((store.root / "inputs").iterdir()) == [bundle.path]
    assert not outputs[0].exists()


def test_backfill_uses_sealed_invocation_request_cap(tmp_path, monkeypatch):
    store = Store(tmp_path / "data", cli.CONTRACT)
    second = TARGET | {"asset_id": "124", "clob_token_id": "124", "outcome_index": 2}
    limits = Limits(1, 10**7, 10**8, 10**6, 10**7)
    plan_id = store.create_plan(
        {
            "limits": limits.as_dict(),
            "assets": [TARGET, second],
            "feeds": ["clob-history"],
            "start": NOW,
            "end": NOW + timedelta(seconds=10),
            "fidelity": 1,
        }
    )
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"history": []})

    monkeypatch.setattr(
        acquisitions,
        "Client",
        lambda *args, **kwargs: Client(
            *args, **kwargs, transport=httpx.MockTransport(handler), sleep=lambda _: None
        ),
    )
    with pytest.raises(BudgetExceeded, match="request limit"):
        cli.run(
            cli.parser().parse_args(
                ["--root", str(store.root), "prices", "backfill", "--plan", plan_id]
            )
        )
    assert len(requests) == 1


def test_archive_cli_retained_object_and_repeated_ingestion_semantics(tmp_path, monkeypatch):
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
    source = {
        "timestamp_received": NOW,
        "timestamp_created_at": NOW,
        "market_id": TARGET["condition_id"],
        "update_type": "book_snapshot",
        "data": '{"token_id":"123","bids":[],"asks":[]}',
    }
    pq.write_table(pa.Table.from_pylist([source], schema=V1_SCHEMA), path)
    payload = path.read_bytes()
    monkeypatch.setattr(
        acquisitions,
        "Client",
        lambda *args, **kwargs: Client(
            *args,
            **kwargs,
            transport=httpx.MockTransport(lambda request: httpx.Response(200, content=payload)),
            sleep=lambda _: None,
        ),
    )
    args = cli.parser().parse_args(
        ["--root", str(store.root), "archive", "backfill", "--plan", plan_id, "--version", "v1"]
    )
    first, second = cli.run(args), cli.run(args)
    release = build(store, plan_id, [first["acquisition_id"], second["acquisition_id"]])
    updates = pq.read_table(store.root / "published" / release / "book_updates.parquet").to_pylist()
    states = pq.read_table(store.root / "published" / release / "book_states.parquet").to_pylist()
    assert len(updates) == len(states) == 1
    assert states[0]["outcome_index"] == 1 and states[0]["ingested_at"] is not None
    assert updates[0]["ingested_at"] is not None
    assert list((store.root / "objects").iterdir())


def test_changed_archive_objects_are_independent_replay_streams(tmp_path, monkeypatch):
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
    objects = []
    for quantity in ("1", "2"):
        path = tmp_path / (quantity + ".parquet")
        source = {
            "timestamp_received": NOW,
            "timestamp_created_at": NOW,
            "market_id": TARGET["condition_id"],
            "update_type": "book_snapshot",
            "data": '{"token_id":"123","bids":[{"price":"0.4","size":"'
            + quantity
            + '"}],"asks":[{"price":"0.6","size":"1"}]}',
        }
        pq.write_table(pa.Table.from_pylist([source], schema=V1_SCHEMA), path)
        objects.append(path.read_bytes())
    monkeypatch.setattr(
        acquisitions,
        "Client",
        lambda *args, **kwargs: Client(
            *args,
            **kwargs,
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, content=objects.pop(0))
            ),
            sleep=lambda _: None,
        ),
    )
    args = cli.parser().parse_args(
        ["--root", str(store.root), "archive", "backfill", "--plan", plan_id, "--version", "v1"]
    )
    first, second = cli.run(args), cli.run(args)
    release = build(store, plan_id, [first["acquisition_id"], second["acquisition_id"]])
    states = pq.read_table(store.root / "published" / release / "book_states.parquet").to_pylist()
    assert len(states) == 2 and len({row["stream"] for row in states}) == 2
    assert {row["status"] for row in states} == {"valid"}


def test_crash_after_http_commit_preserves_actual_receive_timestamp(tmp_path, monkeypatch):
    store = Store(tmp_path / "data", cli.CONTRACT)
    plan_id = plan(store)
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"history": [{"t": int(NOW.timestamp()), "p": "0.4"}]})

    monkeypatch.setattr(
        acquisitions,
        "Client",
        lambda *args, **kwargs: Client(
            *args, **kwargs, transport=httpx.MockTransport(handler), sleep=lambda _: None
        ),
    )
    original = Store.capture

    def crash(self, acquisition_id, unit_id, relations, **kwargs):
        if unit_id.startswith("rows-"):
            raise RuntimeError("crash after HTTP commit")
        return original(self, acquisition_id, unit_id, relations, **kwargs)

    monkeypatch.setattr(Store, "capture", crash)
    args = cli.parser().parse_args(
        ["--root", str(store.root), "prices", "backfill", "--plan", plan_id]
    )
    with pytest.raises(RuntimeError):
        cli.run(args)
    acquisition = next((store.root / "acquisitions").iterdir()).name
    committed = store.committed_captures(acquisition_id=acquisition)
    assert len(committed) == 1 and committed[0]["unit_id"].startswith("http-")
    received = acquisitions.utc(store.read_evidence(committed[0])["received_at"])
    monkeypatch.setattr(Store, "capture", original)
    args.resume = acquisition
    cli.run(args)
    rows = [
        row
        for capture in store.committed_captures(acquisition_id=acquisition)
        for row in store.read_relation(capture, "price_samples")
    ]
    assert len(requests) == 1
    assert rows[0]["received_at"] == received
    assert rows[0]["ingested_at"] >= received


def test_published_live_book_preserves_outcome_ordinal_and_ingestion(tmp_path):
    from polymarket_market_data.books import UPDATE_SCHEMA, normalize_message

    store = Store(tmp_path / "data", cli.CONTRACT)
    plan_id = store.create_plan(
        {
            "limits": LIMITS.as_dict(),
            "assets": [TARGET],
            "feeds": ["clob-ws"],
            "start": NOW,
            "end": NOW + timedelta(seconds=10),
        }
    )
    acquisition = store.new_acquisition(plan_id, "clob-ws")
    updates = normalize_message(
        {
            "event_type": "book",
            "asset_id": "123",
            "bids": [],
            "asks": [],
            "timestamp": str(int(NOW.timestamp() * 1000)),
        },
        targets=[TARGET],
        source="clob-ws",
        stream="fixture",
        sequence=1,
        received_at=NOW,
    )
    store.capture(
        acquisition,
        "message",
        {"book_updates": updates},
        schemas={"book_updates": UPDATE_SCHEMA},
        coverage=[coverage(TARGET, "clob-ws", NOW, NOW + timedelta(seconds=10), "success")],
    )
    acquisitions._finish(store, acquisition)
    release = build(store, plan_id, [acquisition])
    for name in ("book_updates", "book_states"):
        rows = pq.read_table(store.root / "published" / release / (name + ".parquet")).to_pylist()
        assert rows[0]["outcome_index"] == 1
        assert rows[0]["source_at"] == rows[0]["received_at"] == NOW
        assert rows[0]["ingested_at"] >= NOW


def test_future_completion_revision_is_rejected(tmp_path):
    store = Store(tmp_path / "data", cli.CONTRACT)
    plan_id = plan(store)
    acquisition = store.new_acquisition(plan_id, "clob-history")
    store.capture(
        acquisition,
        "acquisition_complete",
        {},
        evidence={"capture_ids": [], "acquisition_revision": 2, "schema_revision": 1},
    )
    with pytest.raises(ValueError, match="unsupported acquisition"):
        build(store, plan_id, [acquisition])


def test_partial_archive_resume_keeps_original_object_after_remote_change(tmp_path, monkeypatch):
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
    payloads = []
    for quantity in ("1", "2"):
        path = tmp_path / (quantity + ".parquet")
        source = {
            "timestamp_received": NOW,
            "timestamp_created_at": NOW,
            "market_id": TARGET["condition_id"],
            "update_type": "book_snapshot",
            "data": '{"token_id":"123","bids":[{"price":"0.4","size":"'
            + quantity
            + '"}],"asks":[{"price":"0.6","size":"1"}]}',
        }
        pq.write_table(pa.Table.from_pylist([source], schema=V1_SCHEMA), path)
        payloads.append(path.read_bytes())
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, content=payloads[min(len(requests) - 1, 1)])

    monkeypatch.setattr(
        acquisitions,
        "Client",
        lambda *args, **kwargs: Client(
            *args, **kwargs, transport=httpx.MockTransport(handler), sleep=lambda _: None
        ),
    )
    original = Store.capture

    def crash(self, acquisition_id, unit_id, relations, **kwargs):
        if unit_id.startswith("hour-") and "-batch-" not in unit_id:
            raise RuntimeError("crash after first committed archive batch")
        return original(self, acquisition_id, unit_id, relations, **kwargs)

    monkeypatch.setattr(Store, "capture", crash)
    args = cli.parser().parse_args(
        ["--root", str(store.root), "archive", "backfill", "--plan", plan_id, "--version", "v1"]
    )
    with pytest.raises(RuntimeError):
        cli.run(args)
    acquisition = next((store.root / "acquisitions").iterdir()).name
    assert len(store.committed_captures(acquisition_id=acquisition)) == 2
    monkeypatch.setattr(Store, "capture", original)
    args.resume = acquisition
    result = cli.run(args)
    assert len(requests) == 1 and result["requests"] == 0
    release = build(store, plan_id, [acquisition])
    updates = pq.read_table(store.root / "published" / release / "book_updates.parquet").to_pylist()
    assert updates[0]["bids"][0]["quantity"] == Decimal("1")


def test_live_metadata_producer_receives_budget_caps(tmp_path, monkeypatch):
    store = Store(tmp_path / "data", cli.CONTRACT)
    plan_data = store.load_plan(plan(store))

    def producer(argv, **kwargs):
        assert int(argv[argv.index("--max-output-bytes") + 1]) <= LIMITS.max_temp_bytes // 2
        assert int(argv[argv.index("--max-response-bytes") + 1]) == LIMITS.max_response_bytes
        output = Path(argv[argv.index("--output") + 1])
        sha = metadata(output)
        return SimpleNamespace(
            returncode=0,
            stdout=json_bytes(
                {"manifest_sha256": sha, "http_attempts": 1, "downloaded_bytes": 64}
            ).decode(),
        )

    monkeypatch.setattr(metadata_producer, "bounded_run", producer)
    acquisitions._refresh_live_metadata(
        store,
        plan_data,
        "catalogue",
        client=meter(),
    )


def test_delegated_metadata_refreshes_share_one_invocation_budget(tmp_path, monkeypatch):
    store = Store(tmp_path / "data", cli.CONTRACT)
    limits = Limits(3, 1024, 10**8, 10**6, 10**7)
    plan_data = store.load_plan(plan(store)) | {"limits": limits.as_dict()}
    client = meter(limits)
    requested = []

    def producer(argv, **kwargs):
        requested.append(
            (
                int(argv[argv.index("--max-requests") + 1]),
                int(argv[argv.index("--max-download-bytes") + 1]),
            )
        )
        output = Path(argv[argv.index("--output") + 1])
        sha = metadata(output)
        return SimpleNamespace(
            returncode=0,
            stdout=json_bytes(
                {"manifest_sha256": sha, "http_attempts": 1, "downloaded_bytes": 64}
            ).decode(),
        )

    monkeypatch.setattr(metadata_producer, "bounded_run", producer)
    args = SimpleNamespace(events_executable="catalogue")
    for _ in range(3):
        acquisitions._refresh_live_metadata(store, plan_data, args.events_executable, client=client)
    with pytest.raises(BudgetExceeded, match="limit exhausted"):
        acquisitions._refresh_live_metadata(store, plan_data, args.events_executable, client=client)
    assert requested == [(3, 1024), (2, 960), (1, 896)]
    assert client.requests == 3 and client.downloaded == 192


def test_failed_delegated_metadata_accounts_attempts_and_partial_bytes(tmp_path, monkeypatch):
    store = Store(tmp_path / "data", cli.CONTRACT)
    plan_data = store.load_plan(plan(store))
    client = meter()
    monkeypatch.setattr(
        metadata_producer,
        "bounded_run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=2,
            stdout=json_bytes(
                {"http_attempts": 2, "downloaded_bytes": 19, "failed": ["1"]}
            ).decode(),
        ),
    )
    with pytest.raises(ValueError, match="failed or busy"):
        acquisitions._refresh_live_metadata(
            store,
            plan_data,
            "catalogue",
            client=client,
        )
    assert client.requests == 2 and client.downloaded == 19
    assert list((store.root / "inputs").iterdir()) == []


def test_startup_metadata_consumes_last_request_before_ws_connection(tmp_path, monkeypatch):
    store = Store(tmp_path / "data", cli.CONTRACT)
    limits = Limits(1, 10**7, 10**8, 10**6, 10**7)
    plan_id = store.create_plan(
        {
            "limits": limits.as_dict(),
            "assets": [TARGET],
            "market_ids": ["1"],
            "feeds": ["clob-ws"],
            "start": NOW,
            "end": NOW + timedelta(seconds=10),
            "duration": 1,
        }
    )
    clients = []

    def client_factory(*args, **kwargs):
        client = Client(
            *args,
            **kwargs,
            transport=httpx.MockTransport(lambda _: pytest.fail("unexpected direct HTTP")),
        )
        clients.append(client)
        return client

    monkeypatch.setattr(acquisitions, "Client", client_factory)

    def producer(argv, **kwargs):
        assert int(argv[argv.index("--max-requests") + 1]) == 1
        output = Path(argv[argv.index("--output") + 1])
        sha = metadata(output)
        return SimpleNamespace(
            returncode=0,
            stdout=json_bytes(
                {"manifest_sha256": sha, "http_attempts": 1, "downloaded_bytes": 64}
            ).decode(),
        )

    monkeypatch.setattr(metadata_producer, "bounded_run", producer)

    async def record(**kwargs):
        kwargs["charge_request"]()
        pytest.fail("WS connection would exceed the shared invocation cap")

    monkeypatch.setattr(acquisitions, "record_messages", record)
    args = cli.parser().parse_args(
        [
            "--root",
            str(store.root),
            "books",
            "record",
            "--plan",
            plan_id,
            "--events-executable",
            "catalogue",
        ]
    )
    with pytest.raises(BudgetExceeded, match="request limit"):
        cli.run(args)
    assert clients[0].requests == 1 and clients[0].downloaded == 64
