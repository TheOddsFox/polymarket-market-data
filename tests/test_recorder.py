import asyncio
import json
from datetime import UTC, datetime

import pytest
from websockets.client import ClientProtocol
from websockets.frames import OP_TEXT, Frame
from websockets.protocol import OPEN
from websockets.uri import parse_uri

from polymarket_market_data import recorder
from polymarket_market_data.foundation import BudgetExceeded

TARGET = {
    "venue": "polymarket",
    "market_id": "1",
    "asset_kind": "ctf_token",
    "asset_id": "123",
    "condition_id": "0x" + "a" * 64,
}


class Socket:
    def __init__(self, payload):
        self.payload = payload
        self.sent = []

    async def send(self, value):
        self.sent.append(value)

    async def recv(self, **kwargs):
        if self.payload is not None:
            value, self.payload = self.payload, None
            return value
        raise OSError("simulated disconnect")


class Context:
    def __init__(self, socket):
        self.socket = socket

    async def __aenter__(self):
        return self.socket

    async def __aexit__(self, *args):
        pass


def test_finite_recording_preserves_reconnect_gap_and_native_identity(monkeypatch):
    clock = [0]

    def tick():
        clock[0] += 0.001
        return clock[0]

    monkeypatch.setattr(recorder.time, "monotonic", tick)

    async def no_sleep(_):
        pass

    monkeypatch.setattr(recorder.asyncio, "sleep", no_sleep)
    sockets, captures, attempts, bytes_seen = [], [], [], []
    message = json.dumps(
        {
            "event_type": "book",
            "asset_id": "123",
            "market": TARGET["condition_id"],
            "bids": [],
            "asks": [],
        }
    ).encode()

    def connect(*args, **kwargs):
        assert kwargs["proxy"] is None and kwargs["max_queue"] == 32
        assert kwargs["compression"] is None
        socket = Socket(message)
        sockets.append(socket)
        return Context(socket)

    async def emit(rows, evidence, covered):
        captures.append((rows, evidence, covered))

    asyncio.run(
        recorder.record(
            targets=[TARGET],
            duration=0.08,
            emit=emit,
            charge_request=lambda: attempts.append(True),
            charge_download=bytes_seen.append,
            connector=connect,
        )
    )
    assert len(attempts) == len(sockets) >= 2
    assert any(
        row["reason"] == "connection_gap"
        for rows, _, _ in captures
        for row in rows
        if row["kind"] == "reset"
    )
    assert captures[-1][2][0]["gaps"] >= 1
    assert captures[-1][2][0]["continuous_coverage"] is False
    assert all(json.loads(socket.sent[0])["assets_ids"] == ["123"] for socket in sockets)


def test_oversized_message_cannot_complete_session(monkeypatch):
    captures = []

    async def emit(rows, evidence, covered):
        captures.append((rows, evidence, covered))

    def full(_):
        raise BudgetExceeded("byte limit exhausted")

    with pytest.raises(BudgetExceeded):
        asyncio.run(
            recorder.record(
                targets=[TARGET],
                duration=1,
                emit=emit,
                charge_request=lambda: None,
                charge_download=full,
                connector=lambda *args, **kwargs: Context(Socket(b"{}")),
            )
        )
    assert captures[-1][2][0]["status"] == "invalid"
    assert not any(evidence.get("kind") == "session_end" for _, evidence, _ in captures)


def test_equal_numeric_assets_refuse_ambiguous_subscription():
    async def emit(*args):
        pytest.fail("should fail before recording")

    with pytest.raises(ValueError, match="ambiguous"):
        asyncio.run(
            recorder.record(
                targets=[TARGET, TARGET | {"asset_kind": "poly_v2_position"}],
                duration=1,
                emit=emit,
                charge_request=lambda: pytest.fail("network"),
                charge_download=lambda _: None,
            )
        )


def test_resolved_book_remains_terminal():
    from polymarket_market_data.books import Book, normalize_message

    book = Book()

    def row(message, seq):
        return normalize_message(
            message,
            targets=[TARGET],
            source="clob-ws",
            stream="test",
            sequence=seq,
            received_at=datetime.now(UTC),
        )[0]

    book.apply(row({"event_type": "market_resolved", "assets_ids": ["123"]}, 1))
    result = book.apply(row({"event_type": "book", "asset_id": "123", "bids": [], "asks": []}, 2))
    assert result["status"] == "market_resolved"


class WireTransport:
    def __init__(self):
        self.aborted = False
        self.writes = []

    def pause_reading(self):
        pass

    def resume_reading(self):
        pass

    def set_write_buffer_limits(self, *args):
        pass

    def write(self, data):
        self.writes.append(data)

    def abort(self):
        self.aborted = True

    def is_closing(self):
        return self.aborted


def open_wire_connection(factory):
    protocol = ClientProtocol(parse_uri(recorder.WS_URL), state=OPEN)
    connection = factory(protocol, ping_interval=None)
    connection.connection_made(WireTransport())
    connection.response = object()
    connection.response_rcvd.set_result(None)
    return connection


def test_partial_fragment_disconnect_counts_ingress_before_protocol_parse():
    async def scenario():
        charges = []
        connection = open_wire_connection(
            lambda protocol, **kwargs: recorder.MeteredConnection(
                protocol,
                charge_download=charges.append,
                report_error=lambda _: pytest.fail("budget failure"),
                **kwargs,
            )
        )
        frame = Frame(OP_TEXT, b"fragment", fin=False).serialize(mask=False)
        partial_frame = frame[:4]
        connection.data_received(partial_frame)
        connection.connection_lost(None)
        assert sum(charges) == len(partial_frame)
        assert connection.ingress_error is None
        assert connection.recv_messages.closed

    asyncio.run(scenario())


def test_ingress_overflow_closes_transport_and_preserves_budget_error():
    async def scenario():
        seen, errors = [], []

        def charge(size):
            seen.append(size)
            if sum(seen) > 4:
                raise BudgetExceeded("wire byte budget exhausted")

        connection = open_wire_connection(
            lambda protocol, **kwargs: recorder.MeteredConnection(
                protocol, charge_download=charge, report_error=errors.append, **kwargs
            )
        )
        frame = Frame(OP_TEXT, b"fragment", fin=False).serialize(mask=False)
        connection.data_received(frame[:3])
        connection.data_received(frame[3:])
        assert sum(seen) == len(frame)
        assert connection.transport.aborted
        assert len(errors) == 1 and connection.ingress_error is errors[0]
        connection.connection_lost(None)
        assert connection.recv_exc is errors[0]

    asyncio.run(scenario())


def test_ingress_budget_failure_cannot_reconnect_or_complete_recording():
    async def scenario():
        attempts, captures, received = [], [], []

        def charge(size):
            received.append(size)
            raise BudgetExceeded("wire byte budget exhausted")

        def connect(*args, **kwargs):
            assert kwargs["compression"] is None
            connection = open_wire_connection(kwargs["create_connection"])

            async def receive(**kwargs):
                connection.data_received(
                    Frame(OP_TEXT, b"partial", fin=False).serialize(mask=False)
                )
                connection.connection_lost(None)
                return await recorder.MeteredConnection.recv(connection, **kwargs)

            connection.recv = receive
            return Context(connection)

        async def emit(rows, evidence, covered):
            captures.append((rows, evidence, covered))

        with pytest.raises(BudgetExceeded, match="wire byte"):
            await recorder.record(
                targets=[TARGET],
                duration=1,
                emit=emit,
                charge_request=lambda: attempts.append(True),
                charge_download=charge,
                connector=connect,
            )
        assert len(attempts) == 1
        assert received
        assert captures[-1][2][0]["status"] == "invalid"
        assert not any(evidence.get("kind") == "session_end" for _, evidence, _ in captures)

    asyncio.run(scenario())


def test_metered_complete_message_is_not_charged_again_by_recorder():
    async def scenario():
        charges, captures = [], []
        payload = json.dumps(
            {"event_type": "book", "asset_id": "123", "bids": [], "asks": []}
        ).encode()
        wire = Frame(OP_TEXT, payload).serialize(mask=False)

        def connect(*args, **kwargs):
            connection = open_wire_connection(kwargs["create_connection"])
            pending = [True]

            async def receive(**kwargs):
                if pending:
                    pending.pop()
                    connection.data_received(wire)
                    return await recorder.MeteredConnection.recv(connection, **kwargs)
                connection.connection_lost(None)
                raise OSError("end of synthetic connection")

            connection.recv = receive
            return Context(connection)

        async def emit(rows, evidence, covered):
            captures.append((rows, evidence, covered))

        await recorder.record(
            targets=[TARGET],
            duration=0.01,
            emit=emit,
            charge_request=lambda: None,
            charge_download=charges.append,
            connector=connect,
        )
        observations = sum(row["kind"] == "book" for rows, _, _ in captures for row in rows)
        assert observations >= 1
        assert sum(charges) == observations * len(wire)
        assert len(charges) == observations

    asyncio.run(scenario())
