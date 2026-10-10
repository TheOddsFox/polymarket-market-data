"""Finite public market WebSocket sessions with explicit connection gaps."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime
from functools import partial
from uuid import uuid4

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from .books import normalize_message, resets
from .feeds import coverage
from .foundation import BudgetExceeded

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class NoRedirectConnect(connect):
    def process_redirect(self, exc):
        return exc


class MeteredConnection(ClientConnection):
    """Count ingress before framing, including incomplete messages and handshakes."""

    def __init__(self, protocol, *, charge_download, report_error, **kwargs):
        super().__init__(protocol, **kwargs)
        self.charge_download, self.report_error = charge_download, report_error
        self.ingress_error = None

    def data_received(self, data):
        try:
            self.charge_download(len(data))
        except BudgetExceeded as exc:
            if self.ingress_error is None:
                self.ingress_error = exc
                self.report_error(exc)
            self.set_recv_exc(self.ingress_error)
            self.transport.abort()
            return
        if self.ingress_error is not None:
            self.transport.abort()
            return
        super().data_received(data)


async def record(
    *,
    targets,
    duration,
    emit,
    charge_request,
    charge_download,
    connector=NoRedirectConnect,
    max_message_bytes=4 * 1024**2,
    heartbeat_seconds=10,
    pong_timeout=35,
    refresh_metadata=None,
):
    if duration <= 0 or duration > 86400:
        raise ValueError("recording duration must be 1..86400 seconds")
    if not targets or len(targets) > 500:
        raise ValueError("one bounded session requires 1..500 native assets")
    # Raw subscribe messages cannot disambiguate equal numeric IDs across ledgers.
    if len({target["asset_id"] for target in targets}) != len(targets):
        raise ValueError("ambiguous wire subscription identities")
    started = datetime.now(UTC)
    deadline, sequence, connection = time.monotonic() + duration, 0, 0
    gaps, snapshots = 0, set()
    ingress_errors = []
    while time.monotonic() < deadline:
        connection += 1
        stream = f"ws/{uuid4().hex}"
        charge_request()
        try:
            async with connector(
                WS_URL,
                open_timeout=min(15, max(0.1, deadline - time.monotonic())),
                max_size=max_message_bytes,
                max_queue=32,
                ping_interval=None,
                proxy=None,
                compression=None,
                create_connection=partial(
                    MeteredConnection,
                    charge_download=charge_download,
                    report_error=ingress_errors.append,
                ),
            ) as ws:
                await ws.send(
                    json.dumps(
                        {
                            "assets_ids": [target["asset_id"] for target in targets],
                            "type": "market",
                            "custom_feature_enabled": True,
                        }
                    )
                )
                await emit(
                    resets(
                        targets,
                        source="clob-ws",
                        stream=stream,
                        sequence=sequence,
                        received_at=datetime.now(UTC),
                        reason="session_start",
                    ),
                    {"connection": connection, "kind": "connection_start"},
                    [],
                )
                sequence += 1
                next_ping, pending_ping = time.monotonic() + heartbeat_seconds, None
                while time.monotonic() < deadline:
                    now = time.monotonic()
                    if pending_ping is not None and now - pending_ping >= pong_timeout:
                        raise TimeoutError("heartbeat expired")
                    if now >= next_ping:
                        await ws.send("PING")
                        pending_ping = now if pending_ping is None else pending_ping
                        next_ping = now + heartbeat_seconds
                    timeout = max(
                        0.001,
                        min(
                            deadline - now,
                            next_ping - now,
                            pong_timeout
                            if pending_ping is None
                            else pong_timeout - (now - pending_ping),
                        ),
                    )
                    try:
                        payload = await asyncio.wait_for(ws.recv(decode=False), timeout)
                    except TimeoutError:
                        continue
                    if isinstance(payload, str):
                        payload = payload.encode()
                    if ingress_errors:
                        raise ingress_errors[0]
                    if not isinstance(ws, MeteredConnection):
                        # Injected test connectors account complete payloads explicitly.
                        charge_download(len(payload))
                    if payload == b"PONG":
                        pending_ping = None
                        continue
                    if payload == b"PING":
                        await ws.send("PONG")
                        continue
                    received = datetime.now(UTC)
                    updates = normalize_message(
                        payload,
                        targets=targets,
                        source="clob-ws",
                        stream=stream,
                        sequence=sequence,
                        received_at=received,
                    )
                    snapshots.update(
                        (row["asset_kind"], row["asset_id"])
                        for row in updates
                        if row["kind"] == "book"
                    )
                    await emit(
                        updates,
                        {
                            "connection": connection,
                            "received_at": received,
                            "payload": payload.decode(),
                        },
                        [],
                    )
                    sequence += 1
                    if refresh_metadata and any(
                        row["kind"] == "market_resolved"
                        or row.get("reason") == "metadata_refresh_needed"
                        for row in updates
                    ):
                        await refresh_metadata()
                        raise ConnectionError("reconnect after metadata refresh")
            if ingress_errors:
                raise ingress_errors[0]
        except (
            ConnectionClosed,
            InvalidHandshake,
            OSError,
            TimeoutError,
            ValueError,
            UnicodeError,
        ) as exc:
            failure = ingress_errors[0] if ingress_errors else exc
            fatal = isinstance(failure, (ValueError, UnicodeError))
            reason = "invalid_source_message" if fatal else "connection_gap"
            await emit(
                resets(
                    targets,
                    source="clob-ws",
                    stream=stream,
                    sequence=sequence,
                    received_at=datetime.now(UTC),
                    reason=reason,
                ),
                {"connection": connection, "kind": reason},
                [
                    coverage(target, "clob-ws", started, datetime.now(UTC), "invalid")
                    for target in targets
                ]
                if fatal
                else [],
            )
            if fatal:
                raise failure
            gaps += 1
            sequence += 1
            await asyncio.sleep(min(1, max(0, deadline - time.monotonic())))
    stopped = datetime.now(UTC)
    await emit(
        [],
        {"kind": "session_end", "connections": connection, "gaps": gaps},
        [
            coverage(
                target,
                "clob-ws",
                started,
                stopped,
                "success"
                if (target["asset_kind"], target["asset_id"]) in snapshots
                else "unobserved",
                gaps=gaps,
                continuous_coverage=False,
            )
            for target in targets
        ],
    )
