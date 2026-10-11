"""Bounded producer diagnostics and isolated metadata process configuration."""

from __future__ import annotations

import os
import selectors
import signal
import subprocess
import tempfile
import time
from dataclasses import replace
from pathlib import Path

from .foundation import BudgetExceeded, json_loads
from .metadata import MAX_BUNDLE, pin_metadata


def environment(root, limits=None):
    root = Path(root).absolute()
    result = {
        **{
            key: value
            for key, value in os.environ.items()
            if not key.startswith("CATALOGUE_") and key != "PMXT_API_KEY"
        },
        "CATALOGUE_ROOT": str(root),
        "CATALOGUE_PATHS_DATA_DIR": str(root / "data"),
        "CATALOGUE_PATHS_STATE_DIR": str(root / ".state"),
    }
    if limits:
        result.update(
            {
                "CATALOGUE_CAPTURE_MAX_REQUESTS": str(limits.max_requests),
                "CATALOGUE_CAPTURE_MAX_DOWNLOAD_BYTES": str(limits.max_download_bytes),
                "CATALOGUE_CAPTURE_MAX_RESPONSE_BYTES": str(limits.max_response_bytes),
                "CATALOGUE_CAPTURE_MAX_RETAINED_BYTES": str(limits.max_storage_bytes),
                "CATALOGUE_CAPTURE_MAX_TEMP_BYTES": str(limits.max_temp_bytes),
                "CATALOGUE_CAPTURE_MAX_DURATION_S": "120",
                "CATALOGUE_CAPTURE_WORKERS": "1",
            }
        )
    return result


def bounded_run(argv, *, env, directory, store, timeout=120, max_diagnostics=64 * 1024):
    """Drain pipes without unbounded capture_output or leaving a timed-out child alive."""
    deadline = time.monotonic() + timeout
    stdout = bytearray()
    diagnostic_bytes = 0
    allowance = min(store.limits.max_temp_bytes, store.available_bytes())
    if allowance <= 0:
        raise BudgetExceeded("metadata temporary storage limit exhausted")
    try:
        with store.external_write([Path(directory)], allowance) as workspace:
            with subprocess.Popen(
                argv,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            ) as child:
                try:
                    with selectors.DefaultSelector() as selector:
                        selector.register(child.stdout, selectors.EVENT_READ, True)
                        selector.register(child.stderr, selectors.EVENT_READ, False)
                        while selector.get_map() or child.poll() is None:
                            if time.monotonic() >= deadline:
                                raise ValueError("selected metadata refresh deadline exhausted")
                            size = workspace.check()
                            if size > store.limits.max_temp_bytes:
                                raise BudgetExceeded("metadata temporary storage limit exhausted")
                            for key, _ in selector.select(min(0.1, deadline - time.monotonic())):
                                data = os.read(key.fileobj.fileno(), 8192)
                                if not data:
                                    selector.unregister(key.fileobj)
                                    continue
                                diagnostic_bytes += len(data)
                                if diagnostic_bytes > max_diagnostics:
                                    raise ValueError(
                                        "selected metadata refresh diagnostics limit exhausted"
                                    )
                                if key.data:
                                    stdout.extend(data)
                    return subprocess.CompletedProcess(argv, child.wait(), bytes(stdout), b"")
                finally:
                    try:
                        os.killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    child.wait()
    except BudgetExceeded:
        raise BudgetExceeded(
            "metadata temporary storage or retained storage limit exhausted"
        ) from None


def nonnegative(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("invalid run resource accounting")


def refresh_metadata(store, market_ids, events_executable, events_root=None, *, client=None):
    """Use a fresh managed producer root so ambient configuration cannot choose sources or paths."""
    meter = getattr(store, "run_meter", None)
    remaining = (
        meter.remaining
        if meter
        else (
            store.limits.max_requests - (client.requests if client else 0),
            store.limits.max_download_bytes - (client.downloaded if client else 0),
        )
    )
    if min(remaining) <= 0:
        raise BudgetExceeded("metadata refresh request/download limit exhausted")
    store._budget()
    retained = store.retained_bytes()
    output_budget = min(
        MAX_BUNDLE,
        store.limits.max_temp_bytes // 4,
        (store.limits.max_storage_bytes - retained) // 4,
    )
    if output_budget <= 0:
        raise BudgetExceeded("metadata refresh requires available temporary and retained storage")
    reservation = meter.reserve(*remaining, "metadata producer") if meter else None
    with tempfile.TemporaryDirectory(prefix=".metadata-", dir=store.root / ".state") as temporary:
        try:
            directory = Path(temporary) / "bundle"
            producer_root = Path(temporary) / "events"
            producer_root.mkdir()
            argv = [
                str(events_executable),
                "metadata",
                "refresh",
                "--output",
                str(directory),
                "--max-output-bytes",
                str(output_budget),
                "--max-response-bytes",
                str(min(store.limits.max_response_bytes, 8 * 1024**2)),
                "--max-requests",
                str(remaining[0]),
                "--max-download-bytes",
                str(remaining[1]),
            ]
            for market_id in market_ids:
                argv.extend(["--market-id", market_id])
            result = bounded_run(
                argv,
                env=environment(
                    producer_root,
                    replace(
                        store.limits,
                        max_requests=remaining[0],
                        max_download_bytes=remaining[1],
                        max_storage_bytes=store.limits.max_storage_bytes - retained - output_budget,
                        max_temp_bytes=store.limits.max_temp_bytes - output_budget,
                    ),
                ),
                directory=temporary,
                store=store,
                max_diagnostics=min(64 * 1024, store.limits.max_temp_bytes),
            )
            try:
                reply = json_loads(result.stdout) if result.stdout.strip() else {}
                if not isinstance(reply, dict):
                    raise ValueError
                attempts, downloaded = reply["http_attempts"], reply["downloaded_bytes"]
                nonnegative(attempts)
                nonnegative(downloaded)
            except (ValueError, UnicodeError, KeyError):
                raise ValueError(
                    "selected metadata refresh lacks valid resource accounting"
                ) from None
            if client:
                client.requests += attempts
                client.downloaded += downloaded
            if meter:
                meter.settle(reservation, attempts, downloaded)
            elif attempts > remaining[0] or downloaded > remaining[1]:
                raise BudgetExceeded("metadata refresh exceeded invocation request/download budget")
            if result.returncode:
                raise ValueError("selected metadata refresh failed or busy")
            if not isinstance(reply.get("manifest_sha256"), str):
                raise ValueError("selected metadata refresh lacks manifest digest")
            return pin_metadata(store.root, directory, reply["manifest_sha256"], store=store)
        finally:
            store.remove(Path(temporary))
