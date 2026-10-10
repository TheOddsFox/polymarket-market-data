"""Bounded producer diagnostics and isolated metadata process configuration."""

from __future__ import annotations

import os
import selectors
import signal
import subprocess
import time
from pathlib import Path

from .foundation import BudgetExceeded


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
                    store._budget()
                    size = 0
                    for path in Path(directory).rglob("*"):
                        if path.is_symlink():
                            raise ValueError("symlink in metadata producer output")
                        if path.is_file():
                            size += path.stat().st_size
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
