"""Repeatable offline operation counts and optional pinned-object continuation timings."""

if not __debug__:
    raise SystemExit("offline benchmark requires Python assertions; disable optimization")

import argparse
import hashlib
import importlib
import inspect
import json
import os
import platform
import resource
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from statistics import median

REPOSITORY = Path(__file__).resolve().parent.parent
GIB = 1024**3
STORAGE_LIMIT = 16 * GIB
TEMP_LIMIT = 2 * GIB
DIAGNOSTIC_LIMIT = 1024**2
RELATIONS = (
    "instruments",
    "price_samples",
    "sampled_ohlc",
    "book_updates",
    "book_states",
    "trades",
    "coverage",
)
OBJECTS = {
    "v1": "8288ae3834bdffb3d403b93d86a56f883a2d38b3fc5070f5b6c85f444be81497",
    "v2": "16f11c8f09d1781565b49d2ba101427f66c2e981ea3a1ba0697dd2da3d0a532e",
}
_COMPARISON_WORKER = False


def require(condition, message):
    if not condition:
        raise ValueError(message)


def limits():
    from polymarket_market_data.foundation import Limits

    return Limits(1000, 4 * GIB, STORAGE_LIMIT, 16 * 1024**2, TEMP_LIMIT)


def runtime():
    import duckdb
    import httpx
    import pyarrow

    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "pyarrow": pyarrow.__version__,
        "duckdb": duckdb.__version__,
        "httpx": httpx.__version__,
    }


def peak_rss():
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


@contextmanager
def root_walks(store=None, root=None):
    """Count inventory entry points, excluding their recursive subtree calls."""
    count = {"walks": 0}
    root = store.root if store else Path(root).absolute()
    if store is not None and hasattr(store, "_measure"):
        original = store._measure

        def measured(path):
            if Path(path) == root:
                count["walks"] += 1
            return original(path)

        store._measure = measured
        try:
            yield count
        finally:
            store._measure = original
    else:
        original = Path.rglob

        def measured(path, pattern, *args, **kwargs):
            if path == root:
                count["walks"] += 1
            return original(path, pattern, *args, **kwargs)

        Path.rglob = measured
        try:
            yield count
        finally:
            Path.rglob = original


def storage_benchmark(root):
    import pyarrow as pa

    from polymarket_market_data.foundation import Store

    store = Store(root, "benchmark.v1", limits())
    plan = store.create_plan({"limits": limits().as_dict()})
    acquisition = store.new_acquisition(plan, "clob-ws")
    schema = pa.schema([("price", pa.decimal128(38, 18)), ("id", pa.string())])
    calls, durations = [], []
    original = store._budget

    def accounted(additional=0):
        started = time.perf_counter()
        try:
            return original(additional)
        finally:
            calls.append(additional)
            durations.append(time.perf_counter() - started)

    store._budget = accounted
    results = []
    try:
        with store.lock, root_walks(store) as walks:
            started = time.perf_counter()
            store._budget()
            cold = time.perf_counter() - started
            cold_walks = walks["walks"]
            previous = 0
            for count in (0, 100, 200, 400):
                calls.clear()
                durations.clear()
                before = walks["walks"]
                started = time.perf_counter()
                for index in range(previous, count):
                    store.capture(
                        acquisition,
                        f"message/{index}",
                        {"samples": [{"price": Decimal("0.4"), "id": "1"}]},
                        evidence={"kind": "message"},
                        schemas={"samples": schema},
                    )
                elapsed = time.perf_counter() - started
                capture_walks = walks["walks"] - before
                accounting_seconds, accounting_calls = sum(durations), len(calls)
                durations.clear()
                before = walks["walks"]
                for _ in range(7):
                    store._budget()
                results.append(
                    {
                        "captures": count,
                        "new_captures": count - previous,
                        "capture_seconds": elapsed,
                        "accounting_calls": accounting_calls,
                        "accounting_seconds": accounting_seconds,
                        "capture_root_walks": capture_walks,
                        "warm_root_walks": walks["walks"] - before,
                        "warm_inventory_seconds": durations.copy(),
                        "warm_inventory_median_seconds": median(durations),
                    }
                )
                previous = count
            managed_walks = walks["walks"]
            if hasattr(store, "retained_bytes"):
                require(
                    managed_walks == 1, "candidate capture operation repeated its root inventory"
                )
        # A separate operation must start from a fresh inventory.
        with store.lock, root_walks(store) as fresh:
            store._budget()
            fresh_walks = fresh["walks"]
        return {
            "cold_inventory_seconds": cold,
            "cold_inventory_root_walks": cold_walks,
            "operation_root_walks": managed_walks,
            "next_operation_root_walks": fresh_walks,
            "milestones": results,
        }
    finally:
        store._budget = original


def http_benchmark(root):
    import httpx

    from polymarket_market_data.foundation import Store
    from polymarket_market_data.transport import Client

    results = []
    supports_store = "store" in inspect.signature(Client).parameters
    modes = ("no-root", "unmanaged", "managed") if supports_store else ("no-root", "unmanaged")
    for mode in modes:
        for count in (0,) if mode == "no-root" else (0, 1200, 5000):
            directory = Path(root) / f"{mode}-{count}"
            directory.mkdir(parents=True)
            unrelated = directory / "unrelated"
            unrelated.mkdir()
            for index in range(count):
                (unrelated / str(index)).touch()
            store = Store(directory, "benchmark.v1", limits()) if mode == "managed" else None
            kwargs = {} if mode == "no-root" else {"storage_root": directory}
            if store:
                kwargs["store"] = store
            transport = httpx.MockTransport(
                lambda request: httpx.Response(200, content=b'{"history":[]}')
            )
            with (
                root_walks(store, directory) as walks,
                Client("clob", limits(), transport=transport, **kwargs) as client,
            ):
                require(
                    client.get_json("https://clob.polymarket.com/prices-history")
                    == {"history": []},
                    "mocked JSON response differs",
                )
                samples = []
                for _ in range(5):
                    started = time.perf_counter()
                    for _ in range(10):
                        client.get_json("https://clob.polymarket.com/prices-history")
                    samples.append((time.perf_counter() - started) / 10)
                results.append(
                    {
                        "mode": mode,
                        "unrelated_files": count,
                        "seconds_per_request": samples,
                        "median_seconds_per_request": median(samples),
                        "root_walks": walks["walks"],
                        "mock_requests": client.requests,
                        "mock_downloaded_bytes": client.downloaded,
                    }
                )
                if supports_store:
                    require(walks["walks"] == 0, "JSON response walked the retained root")
    if not supports_store:
        results.append(
            {
                "mode": "managed",
                "status": "unsupported",
                "reason": "baseline transport has no Store argument",
            }
        )
    return results


def semantic_snapshot(directory):
    """Schemas and sorted row hashes retain multiplicity without retaining Python rows."""
    import pyarrow.parquet as pq

    from polymarket_market_data.foundation import digest, json_bytes

    results = {}
    for name in RELATIONS:
        reader = pq.ParquetFile(Path(directory) / (name + ".parquet"), pre_buffer=False)
        row_hashes, encoded_bytes = [], 0
        require(
            reader.metadata.num_rows <= 100_000, "semantic row inventory exceeds benchmark bound"
        )
        for batch in reader.iter_batches(batch_size=2048, use_threads=False):
            require(
                batch.nbytes <= TEMP_LIMIT // 4, "semantic batch exceeds benchmark memory bound"
            )
            for index in range(batch.num_rows):
                row = batch.slice(index, 1).to_pylist()[0]
                encoded = json_bytes(
                    {
                        key: value
                        for key, value in row.items()
                        if key not in {"ingested_at", "acquisition_id", "capture_id"}
                    }
                )
                require(len(encoded) <= 16 * 1024**2, "semantic row exceeds benchmark bound")
                encoded_bytes += len(encoded)
                row_hashes.append(digest(encoded))
        results[name] = {
            "schema": str(reader.schema_arrow),
            "rows": len(row_hashes),
            "semantic_sha256": digest(json_bytes(sorted(row_hashes))),
            "canonical_bytes": encoded_bytes,
        }
    return results


@contextmanager
def trace_source(path):
    from polymarket_market_data import archives

    original = archives.pq.ParquetFile
    result = {"decoded_groups": [], "decoded_rows": 0}

    class TracedFile:
        def __init__(self, filename, **kwargs):
            self.reader = original(filename, **kwargs)
            self.source = Path(filename).absolute() == Path(path).absolute()

        def iter_batches(self, **kwargs):
            if self.source:
                result["decoded_groups"].extend(kwargs["row_groups"])
            for batch in self.reader.iter_batches(**kwargs):
                if self.source:
                    result["decoded_rows"] += batch.num_rows
                yield batch

        def __getattr__(self, name):
            return getattr(self.reader, name)

    archives.pq.ParquetFile = TracedFile
    try:
        yield result
    finally:
        archives.pq.ParquetFile = original


def synthetic_archive(root):
    import pytest

    sys.path.insert(0, str(REPOSITORY / "tests"))
    import test_archive_progress as fixture

    from polymarket_market_data.acceptance import consumer_check, restore_check
    from polymarket_market_data.build import build
    from polymarket_market_data.foundation import Store

    with pytest.MonkeyPatch.context() as patch:
        case = fixture.setup_archive(Path(root) / "interrupted", patch)
        decoded = fixture.trace_decode(patch, case)
        acquisition = fixture.interrupt(
            patch, case, lambda unit: "-progress1-" in unit, occurrence=2
        )
        before = decoded.copy()
        require(before == [0, 1], "synthetic interruption did not commit two complete groups")
        decoded.clear()
        resumed = fixture.acquire(case, acquisition)
        groups = decoded.copy()
        require(groups == [2, 3], "synthetic continuation decoded a verified prefix")
        require(resumed["requests"] == resumed["downloaded_bytes"] == 0, "resume made a request")
        release = build(case.store, case.plan_id, [acquisition])
        require(
            build(case.store, case.plan_id, [acquisition]) == release, "rebuild identity differs"
        )
        left = semantic_snapshot(case.store.root / "published" / release)
        decoded.clear()
        require(
            fixture.acquire(case, acquisition)["requests"] == 0, "complete resume made a request"
        )
        require(decoded == [], "complete resume decoded source rows")
        other = fixture.setup_archive(Path(root) / "uninterrupted", patch)
        other_id = fixture.acquire(other)["acquisition_id"]
        other_release = build(other.store, other.plan_id, [other_id])
        right = semantic_snapshot(other.store.root / "published" / other_release)
        require(left == right, "synthetic interrupted and uninterrupted semantics differ")
        case.store.publish(release)
        restored_path = Path(root) / "restored"
        restored = restore_check(case.store, restored_path)
        restored_store = Store(restored_path, case.store.contract)
        require(
            build(restored_store, case.plan_id, [acquisition]) == release,
            "synthetic restored replay identity differs",
        )
        consumer_check(restored_store, release)
        with trace_source(
            restored_store.root / "objects" / next(iter(restored_store.root.glob("objects/*"))).name
        ) as trace:
            with restored_store.lock:
                complete = invoke_archive(restored_store, case.plan_id, case.version, acquisition)
        require(
            complete["requests"] == complete["downloaded_bytes"] == 0
            and trace["decoded_groups"] == [],
            "synthetic restored resume performed I/O",
        )
        require(
            semantic_snapshot(restored_path / "published" / release) == left,
            "synthetic restored semantics differ",
        )
        return {
            "completed_groups_before_resume": before,
            "resumed_groups": groups,
            "decoded_source_rows_on_resume": 2,
            "resumed_requests": resumed["requests"],
            "resumed_downloaded_bytes": resumed["downloaded_bytes"],
            "mock_setup_requests": len(case.requests),
            "semantic_relations": left,
            "semantic_equal": True,
            "deterministic_rebuild": True,
            "completed_resume_without_network_or_decode": True,
            "restore": restored,
            "restored_resume_without_network_or_decode": True,
        }


def copy_checked(source, destination, workspace, deadline):
    """Copy regular files with preflight headroom and bounded, deadline-checked chunks."""
    source, destination, workspace = Path(source), Path(destination), Path(workspace)
    require(not destination.exists(), "benchmark clone destination already exists")
    require(destination.absolute().is_relative_to(workspace.absolute()), "clone outside workspace")
    for path in (source, *source.parents, destination, *destination.parents):
        require(not path.is_symlink(), "symlink in benchmark clone path")
    files, directories = [], []
    pending = [source]
    while pending:
        require(time.monotonic() < deadline, "benchmark copy deadline exhausted")
        path = pending.pop()
        info = path.stat(follow_symlinks=False)
        if stat.S_ISDIR(info.st_mode):
            directories.append(path)
            with os.scandir(path) as entries:
                pending.extend(Path(entry.path) for entry in entries)
        elif stat.S_ISREG(info.st_mode):
            files.append((path, info))
        else:
            raise ValueError("non-regular source in benchmark clone")
    retained, temporary = workspace_usage(workspace)
    added = sum(info.st_size for _, info in files)
    require(retained + added <= STORAGE_LIMIT, "clone exceeds aggregate retained headroom")
    temporary_added = sum(
        info.st_size
        for path, info in files
        if any(
            part.startswith((".archive-", ".build-", ".metadata-", ".pending-", "tmp"))
            for part in (destination / path.relative_to(source) if directories else destination)
            .relative_to(workspace)
            .parts
        )
    )
    require(
        temporary + temporary_added <= TEMP_LIMIT,
        "clone exceeds temporary headroom",
    )
    for path in sorted(directories, key=lambda value: len(value.parts)):
        require(time.monotonic() < deadline, "benchmark copy deadline exhausted")
        target = destination / path.relative_to(source)
        target.mkdir(mode=0o700)
    for path, expected in files:
        target = destination / path.relative_to(source) if directories else destination
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as original, target.open("xb") as copied:
            actual = os.fstat(original.fileno())
            fingerprint = (expected.st_dev, expected.st_ino, expected.st_size, expected.st_mtime_ns)
            require(
                stat.S_ISREG(actual.st_mode)
                and (actual.st_dev, actual.st_ino, actual.st_size, actual.st_mtime_ns)
                == fingerprint,
                "benchmark clone source changed before copy",
            )
            remaining = expected.st_size
            while remaining:
                require(time.monotonic() < deadline, "benchmark copy deadline exhausted")
                chunk = original.read(min(1024**2, remaining))
                require(chunk, "benchmark clone source shrank")
                copied.write(chunk)
                remaining -= len(chunk)
            require(not original.read(1), "benchmark clone source grew")
            actual = os.fstat(original.fileno())
            require(
                (actual.st_dev, actual.st_ino, actual.st_size, actual.st_mtime_ns) == fingerprint,
                "benchmark clone source changed during copy",
            )
        target.chmod(0o600)
    require(time.monotonic() < deadline, "benchmark copy deadline exhausted")
    workspace_usage(workspace)


def invoke_archive(store, plan_id, version, acquisition_id):
    if importlib.util.find_spec("polymarket_market_data.acquisitions") is not None:
        from polymarket_market_data.acquisitions import archive

        return archive(store, plan_id=plan_id, version=version, resume_id=acquisition_id)
    from polymarket_market_data import cli

    args = cli.parser().parse_args(
        [
            "--root",
            str(store.root),
            "archive",
            "backfill",
            "--plan",
            plan_id,
            "--version",
            version,
            "--resume",
            acquisition_id,
        ]
    )
    return cli._archive(store, args)


def cached_seed(payload):
    import pyarrow.parquet as pq

    from polymarket_market_data import archives, cli
    from polymarket_market_data.foundation import Store, read_json

    cached, root = Path(payload["cached_root"]), Path(payload["root"])
    version, object_info = payload["version"], payload["object"]
    started = time.perf_counter()
    store = Store(root, cli.CONTRACT, limits())
    plan = read_json(cached / "plans" / (payload["plan_id"] + ".json"))
    plan["feeds"] = ["pmxt-" + version]
    # The original metadata pin is immutable and copied while the parent holds the source root lock.
    metadata = plan["metadata_sha256"]
    copy_checked(
        cached / "inputs" / metadata,
        root / "inputs" / metadata,
        payload["workspace"],
        payload["deadline"],
    )
    copy_checked(
        cached / object_info["path"],
        root / object_info["path"],
        payload["workspace"],
        payload["deadline"],
    )
    copy_seconds = time.perf_counter() - started
    with store.lock:
        plan_id = store.create_plan(plan)
        acquisition = store.new_acquisition(plan_id, "pmxt-" + version)
        pin_started = time.perf_counter()
        store.object_path(object_info)
        pin_seconds = time.perf_counter() - pin_started
        reader = pq.ParquetFile(root / object_info["path"], pre_buffer=False)
        total_groups = reader.metadata.num_row_groups
        stop_after = total_groups * 3 // 4
        require(0 < stop_after < total_groups, "cached comparison needs multiple source groups")
        unit = "hour-20260414T08"
        store.capture(
            acquisition,
            "source-object-" + unit,
            {},
            evidence=payload["source_evidence"],
            objects=[object_info],
        )
        original = store.capture
        receipts = 0

        class Interrupted(Exception):
            pass

        def capture(identifier, name, *args, **kwargs):
            nonlocal receipts
            result = original(identifier, name, *args, **kwargs)
            if "-progress1-" in name:
                receipts += 1
                if receipts == stop_after:
                    raise Interrupted
            return result

        store.capture = capture
        seeded = time.perf_counter()
        with trace_source(root / object_info["path"]) as trace:
            try:
                invoke_archive(store, plan_id, version, acquisition)
            except Interrupted:
                pass
            else:
                raise ValueError("cached seed completed without the requested interruption")
        store.capture = original
        seed_seconds = time.perf_counter() - seeded
        require(trace["decoded_groups"] == list(range(stop_after)), "seed prefix differs")
        store.verify()
    return {
        "plan_id": plan_id,
        "acquisition_id": acquisition,
        "object": object_info,
        "decoder_revision": archives.DECODER_REVISION,
        "progress_revision": archives.PROGRESS_REVISION,
        "total_groups": total_groups,
        "total_rows": reader.metadata.num_rows,
        "completed_groups": stop_after,
        "decoded_groups": trace["decoded_groups"],
        "decoded_rows": trace["decoded_rows"],
        "phases": {
            "copy_seconds": copy_seconds,
            "pin_seconds": pin_seconds,
            "seed_seconds": seed_seconds,
        },
    }


def cached_resume(payload):
    from polymarket_market_data import cli
    from polymarket_market_data.acceptance import consumer_check
    from polymarket_market_data.build import build
    from polymarket_market_data.foundation import Store

    store = Store(Path(payload["root"]), cli.CONTRACT, limits())
    object_path = store.root / payload["object"]["path"]
    phases = {}
    with store.lock:
        started = time.perf_counter()
        with trace_source(object_path) as trace:
            acquired = invoke_archive(
                store, payload["plan_id"], payload["version"], payload["acquisition_id"]
            )
        phases["resume_seconds"] = time.perf_counter() - started
        require(
            acquired["requests"] == acquired["downloaded_bytes"] == 0,
            "cached resume used transport",
        )
        expected = (
            list(range(payload["completed_groups"], payload["total_groups"]))
            if payload["candidate"]
            else list(range(payload["total_groups"]))
        )
        require(
            trace["decoded_groups"] == expected,
            "cached source groups differ from continuation contract",
        )
        started = time.perf_counter()
        release = build(store, payload["plan_id"], [payload["acquisition_id"]])
        require(
            build(store, payload["plan_id"], [payload["acquisition_id"]]) == release,
            "same-inventory release identity differs",
        )
        phases["build_and_rebuild_seconds"] = time.perf_counter() - started
        started = time.perf_counter()
        store.verify(release)
        store.publish(release)
        store.verify()
        consumer = consumer_check(store, release)
        phases["verification_consumer_seconds"] = time.perf_counter() - started
        started = time.perf_counter()
        snapshot = semantic_snapshot(store.root / "published" / release)
        observations = archive_evidence(
            store.root / "published" / release, store.load_plan(payload["plan_id"])["assets"]
        )
        phases["semantic_snapshot_seconds"] = time.perf_counter() - started
        counts = {"data": 0, "progress": 0, "metadata": 0}
        for capture in store.committed_captures(acquisition_id=payload["acquisition_id"]):
            category = (
                "data"
                if capture["tables"]
                else "progress"
                if "-progress1-" in capture["unit_id"]
                else "metadata"
            )
            counts[category] += 1
        with trace_source(object_path) as complete_trace:
            complete = invoke_archive(
                store, payload["plan_id"], payload["version"], payload["acquisition_id"]
            )
        require(
            complete["requests"] == complete["downloaded_bytes"] == 0
            and complete_trace["decoded_groups"] == [],
            "completed cached resume performed I/O",
        )
    restored = None
    if payload.get("restore"):
        started = time.perf_counter()
        destination = Path(payload["restore"])
        with store.lock:
            store.verify()
            copy_checked(store.root, destination, payload["workspace"], payload["deadline"])
        other = Store(destination, cli.CONTRACT, limits())
        other.verify()
        restored = {"status": other.status(), "completed_resume_without_network_or_decode": True}
        require(
            build(other, payload["plan_id"], [payload["acquisition_id"]]) == release,
            "restored inventory replay differs",
        )
        require(consumer_check(other, release) == consumer, "restored consumer result differs")
        require(
            semantic_snapshot(destination / "published" / release) == snapshot,
            "restored semantic result differs",
        )
        with other.lock, trace_source(other.root / payload["object"]["path"]) as restored_trace:
            complete = invoke_archive(
                other, payload["plan_id"], payload["version"], payload["acquisition_id"]
            )
        require(
            complete["requests"] == complete["downloaded_bytes"] == 0
            and restored_trace["decoded_groups"] == [],
            "restored completed archive resume performed I/O",
        )
        phases["restore_seconds"] = time.perf_counter() - started
    return {
        "release_id": release,
        "decoded_groups": trace["decoded_groups"],
        "decoded_rows": trace["decoded_rows"],
        "selected_updates": snapshot["book_updates"]["rows"],
        "observations": observations,
        "requests": acquired["requests"],
        "downloaded_bytes": acquired["downloaded_bytes"],
        "captures": counts,
        "consumer": consumer,
        "semantic_relations": snapshot,
        "deterministic_rebuild": True,
        "completed_resume_without_network_or_decode": True,
        "restore": restored,
        "phases": phases,
    }


def archive_evidence(directory, assets):
    import pyarrow.parquet as pq

    expected = {(asset["venue"], asset["asset_kind"], asset["asset_id"]) for asset in assets}
    counts = {
        key: {"non_reset_observations": 0, "resets": 0, "valid_states": 0} for key in expected
    }
    for relation, field in (("book_updates", "kind"), ("book_states", "status")):
        reader = pq.ParquetFile(Path(directory) / (relation + ".parquet"), pre_buffer=False)
        for batch in reader.iter_batches(
            batch_size=2048,
            use_threads=False,
            columns=["venue", "asset_kind", "asset_id", field],
        ):
            require(batch.nbytes <= TEMP_LIMIT // 4, "archive evidence batch exceeds memory bound")
            for row in batch.to_pylist():
                key = row["venue"], row["asset_kind"], row["asset_id"]
                require(key in expected, "unselected typed asset in archive evidence")
                if field == "kind":
                    name = "resets" if row[field] == "reset" else "non_reset_observations"
                    counts[key][name] += 1
                elif row[field] == "valid":
                    counts[key]["valid_states"] += 1
    for key, count in counts.items():
        require(
            count["non_reset_observations"] > 0 and count["valid_states"] > 0,
            "cached fixture lacks non-reset updates or valid states for " + "/".join(key),
        )
    return [
        {"venue": key[0], "asset_kind": key[1], "asset_id": key[2], **counts[key]}
        for key in sorted(counts)
    ]


def verify_imports(source):
    expected = Path(source).absolute() / "src"
    for name, module in tuple(sys.modules.items()):
        if name == "polymarket_market_data" or name.startswith("polymarket_market_data."):
            filename = getattr(module, "__file__", None)
            require(
                filename and Path(filename).absolute().is_relative_to(expected),
                "worker imported the wrong revision: " + name,
            )


def worker(payload):
    global _COMPARISON_WORKER

    _COMPARISON_WORKER = payload["kind"] == "comparison"
    network = {"attempts": 0}

    def audit(event, args):
        if event in {
            "socket.connect",
            "socket.sendto",
            "socket.sendmsg",
            "socket.getaddrinfo",
            "socket.gethostbyname",
            "socket.gethostbyaddr",
        }:
            network["attempts"] += 1
            error = ValueError("offline benchmark forbids network access")
            error.network_attempts = network["attempts"]
            raise error

    sys.addaudithook(audit)
    from polymarket_market_data import foundation, transport

    verify_imports(payload["source"])
    original = transport.Client._request
    if payload["kind"] in {"seed", "resume"}:

        def denied(*args, **kwargs):
            network["attempts"] += 1
            error = ValueError("pinned-object benchmark attempted an HTTP request")
            error.network_attempts = network["attempts"]
            raise error

        transport.Client._request = denied
    operations = {
        "storage": storage_benchmark,
        "http": http_benchmark,
        "synthetic": synthetic_archive,
    }
    started = time.perf_counter()
    try:
        result = (
            operations[payload["kind"]](Path(payload["root"]))
            if payload["kind"] in operations
            else cached_seed(payload)
            if payload["kind"] == "seed"
            else _cached_comparison(payload)
            if payload["kind"] == "comparison"
            else cached_resume(payload)
        )
        verify_imports(payload["source"])
        require(network["attempts"] == 0, "offline benchmark attempted network access")
        return {
            "result": result,
            "runtime": runtime(),
            "module_path": str(Path(foundation.__file__).absolute()),
            "wall_seconds": time.perf_counter() - started,
            "peak_rss_bytes": peak_rss(),
            "network_attempts": network["attempts"],
        }
    except Exception as exc:
        exc.network_attempts = network["attempts"]
        raise
    finally:
        transport.Client._request = original


def workspace_usage(directory):
    """No-follow sampled apparent bytes, including all clones and diagnostic files."""
    retained = temporary = 0
    pending = [Path(directory)]
    while pending:
        path = pending.pop()
        try:
            with os.scandir(path) as entries:
                for entry in entries:
                    info = entry.stat(follow_symlinks=False)
                    require(not stat.S_ISLNK(info.st_mode), "symlink in benchmark workspace")
                    if stat.S_ISDIR(info.st_mode):
                        pending.append(Path(entry.path))
                    elif stat.S_ISREG(info.st_mode):
                        retained += info.st_size
                        if any(
                            part.startswith(
                                (".archive-", ".build-", ".metadata-", ".pending-", "tmp")
                            )
                            for part in Path(entry.path).relative_to(directory).parts
                        ):
                            temporary += info.st_size
                    else:
                        raise ValueError("non-regular benchmark workspace path")
        except FileNotFoundError:
            pass
    require(retained <= STORAGE_LIMIT, "benchmark aggregate retained storage exceeds 16 GiB")
    require(temporary <= TEMP_LIMIT, "benchmark temporary storage exceeds 2 GiB")
    return retained, temporary


def run_worker(source, workspace, payload, deadline, *, peaks=None):
    require(time.monotonic() < deadline, "benchmark deadline exhausted before worker launch")
    workspace, source = Path(workspace), Path(source)
    require(
        Path(payload["root"]).absolute().is_relative_to(workspace.absolute()),
        "worker root outside benchmark workspace",
    )
    workspace_usage(workspace)
    payload = {
        **payload,
        "source": str(source.absolute()),
        "workspace": str(workspace.absolute()),
        "deadline": deadline,
    }
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"PYTHONPATH", "PYTHONHOME", "PYTHONOPTIMIZE", "PMXT_API_KEY"}
        and not key.startswith("CATALOGUE_")
    }
    environment["PYTHONPATH"] = str(source / "src")
    stdout_fd, stdout_name = tempfile.mkstemp(prefix="worker-stdout-", dir=workspace)
    stderr_fd, stderr_name = tempfile.mkstemp(prefix="worker-stderr-", dir=workspace)
    stdout, stderr = Path(stdout_name), Path(stderr_name)
    group_owner = not _COMPARISON_WORKER
    with os.fdopen(stdout_fd, "wb") as out, os.fdopen(stderr_fd, "wb") as err:
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).absolute()), "--worker", json.dumps(payload)],
            cwd=workspace,
            env=environment,
            stdout=out,
            stderr=err,
            start_new_session=group_owner,
        )
        last_sample = 0
        try:
            while process.poll() is None:
                require(time.monotonic() < deadline, "benchmark worker timed out")
                require(
                    stdout.stat().st_size <= DIAGNOSTIC_LIMIT
                    and stderr.stat().st_size <= DIAGNOSTIC_LIMIT,
                    "benchmark worker diagnostics exceed 1 MiB",
                )
                if peaks is not None and time.monotonic() - last_sample >= 0.5:
                    retained, temporary = workspace_usage(workspace)
                    peaks["sampled_peak_retained_bytes"] = max(
                        peaks.get("sampled_peak_retained_bytes", 0), retained
                    )
                    peaks["sampled_peak_temporary_bytes"] = max(
                        peaks.get("sampled_peak_temporary_bytes", 0), temporary
                    )
                    last_sample = time.monotonic()
                time.sleep(0.025)
        except BaseException:
            if group_owner:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            else:
                process.kill()
            process.wait()
            raise
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
            if group_owner:
                # Nested comparison workers inherit this group, including their descendants.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
    require(
        stdout.stat().st_size <= DIAGNOSTIC_LIMIT and stderr.stat().st_size <= DIAGNOSTIC_LIMIT,
        "benchmark worker diagnostics exceed 1 MiB",
    )
    try:
        result = json.loads(stdout.read_text())
    except (ValueError, UnicodeError):
        raise ValueError("benchmark worker returned invalid JSON diagnostics") from None
    require(process.returncode == 0, result.get("error", "benchmark worker failed"))
    require(result["peak_rss_bytes"] <= TEMP_LIMIT, "benchmark worker peak RSS exceeds 2 GiB")
    require(result["network_attempts"] == 0, "benchmark worker attempted network access")
    retained, temporary = workspace_usage(workspace)
    if peaks is not None:
        peaks["sampled_peak_retained_bytes"] = max(
            peaks.get("sampled_peak_retained_bytes", 0), retained
        )
        peaks["sampled_peak_temporary_bytes"] = max(
            peaks.get("sampled_peak_temporary_bytes", 0), temporary
        )
    stdout.unlink()
    stderr.unlink()
    return result


def revision(repository, ref="HEAD"):
    result = subprocess.run(
        [
            "git",
            "-C",
            str(repository),
            "rev-parse",
            "--verify",
            "--end-of-options",
            ref + "^{commit}",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    value = result.stdout.strip()
    require(len(value) == 40 and all(c in "0123456789abcdef" for c in value), "invalid Git commit")
    return value


def export_baseline(workspace, ref, deadline):
    commit = revision(REPOSITORY, ref)
    destination = Path(workspace) / "baseline-source"
    archive = Path(workspace) / "baseline.tar"
    with archive.open("wb") as stream:
        subprocess.run(
            ["git", "-C", str(REPOSITORY), "archive", commit],
            stdout=stream,
            check=True,
            timeout=max(0.001, deadline - time.monotonic()),
        )
    require(
        archive.stat().st_size <= 16 * 1024**2, "baseline source export exceeds benchmark bound"
    )
    destination.mkdir()
    with tarfile.open(archive) as source:
        require(
            all(member.isfile() or member.isdir() for member in source.getmembers()),
            "baseline export contains links or special files",
        )
        source.extractall(destination, filter="data")
    require(time.monotonic() < deadline, "baseline export deadline exhausted")
    workspace_usage(workspace)
    archive.unlink()
    return destination, commit


def _cached_comparison(payload):
    from polymarket_market_data import cli
    from polymarket_market_data.foundation import Store

    workspace = Path(payload["workspace"])
    deadline = payload["deadline"]
    result = {"status": "incomplete", "versions": [], "phase": "export"}
    progress = workspace / "comparison-progress.json"

    def checkpoint(phase):
        result["phase"] = phase
        encoded = json.dumps(result)
        require(len(encoded.encode()) <= DIAGNOSTIC_LIMIT, "comparison report exceeds 1 MiB")
        progress.write_text(encoded)

    try:
        baseline, commit = export_baseline(workspace, payload["baseline_ref"], deadline)
        result["baseline_revision"] = commit
        checkpoint("validate-canonical-root")
        canonical = Store(Path(payload["cached_root"]), cli.CONTRACT)
        with canonical.lock:
            started = time.perf_counter()
            canonical.verify()
            result["canonical_verify_seconds"] = time.perf_counter() - started
            plans = list((canonical.root / "plans").glob("*.json"))
            require(len(plans) == 1, "cached comparison requires the pinned single-plan fixture")
            plan_id = plans[0].stem
            plan = canonical.load_plan(plan_id)
            require(
                plan["market_ids"] == ["1975743"]
                and plan["start"] == "2026-04-14T08:00:00+00:00"
                and plan["end"] == "2026-04-14T09:00:00+00:00",
                "cached fixture selection differs from approved archived case",
            )
            captures = canonical.committed_captures(plan_id=plan_id)
            pins = {
                capture["source"]: canonical.read_evidence(capture)
                for capture in captures
                if capture["unit_id"] == "source-object-hour-20260414T08"
            }
            for version in ("v1", "v2"):
                evidence = pins.get("pmxt-" + version)
                require(evidence is not None, "missing cached " + version + " source checkpoint")
                object_info = evidence["source_object"]
                require(
                    object_info["sha256"] == OBJECTS[version],
                    "cached object hash differs from approved fixture",
                )
                item = {"version": version, "object": object_info, "repetitions": []}
                result["versions"].append(item)
                checkpoint(version + "-seed")
                seed_root = workspace / (version + "-seed")
                seed = run_worker(
                    REPOSITORY,
                    workspace,
                    {
                        "kind": "seed",
                        "root": str(seed_root),
                        "cached_root": str(canonical.root),
                        "plan_id": plan_id,
                        "version": version,
                        "object": object_info,
                        "source_evidence": evidence,
                    },
                    deadline,
                )
                item["seed"] = seed
                for repetition in range(payload["repetitions"]):
                    pair = {"repetition": repetition}
                    item["repetitions"].append(pair)
                    for label, source in (("baseline", baseline), ("candidate", REPOSITORY)):
                        checkpoint(f"{version}-{label}-{repetition}-clone")
                        root = workspace / f"{version}-{label}-{repetition}"
                        copied = time.perf_counter()
                        copy_checked(seed_root, root, workspace, deadline)
                        copy_seconds = time.perf_counter() - copied
                        worker_payload = {
                            "kind": "resume",
                            "root": str(root),
                            "version": version,
                            "candidate": label == "candidate",
                            **{
                                key: seed["result"][key]
                                for key in (
                                    "plan_id",
                                    "acquisition_id",
                                    "object",
                                    "completed_groups",
                                    "total_groups",
                                )
                            },
                        }
                        if label == "candidate" and repetition == 0:
                            worker_payload["restore"] = str(workspace / (version + "-restored"))
                        checkpoint(f"{version}-{label}-{repetition}-resume")
                        pair[label] = run_worker(source, workspace, worker_payload, deadline)
                        pair[label]["result"]["phases"]["clone_seconds"] = copy_seconds
                        checkpoint(f"{version}-{label}-{repetition}-cleanup")
                        shutil.rmtree(root)
                        if worker_payload.get("restore"):
                            shutil.rmtree(worker_payload["restore"])
                    require(
                        pair["baseline"]["result"]["semantic_relations"]
                        == pair["candidate"]["result"]["semantic_relations"],
                        "cached baseline/candidate semantic equality failed for " + version,
                    )
                    pair["semantic_equal"] = True
                    checkpoint(version + "-equal-" + str(repetition))
                shutil.rmtree(seed_root)
        workspace_usage(workspace)
        result["status"] = "passed"
        checkpoint("complete")
    except Exception as exc:
        result["error"] = str(exc)
        checkpoint(result["phase"])
    return result


def cached_comparison(cached_root, baseline_ref, repetitions, deadline_seconds):
    started = time.monotonic()
    deadline = started + deadline_seconds
    result = {"status": "incomplete", "versions": []}
    peaks = {}
    # The supervisor bounds verification, copying and cleanup as well as decoding children.
    with tempfile.TemporaryDirectory(
        prefix="market-data-cached-benchmark-", dir=Path(tempfile.gettempdir()).resolve()
    ) as temporary:
        workspace = Path(temporary)
        try:
            measured = run_worker(
                REPOSITORY,
                workspace,
                {
                    "kind": "comparison",
                    "root": str(workspace / "comparison"),
                    "cached_root": str(Path(cached_root).absolute()),
                    "baseline_ref": baseline_ref,
                    "repetitions": repetitions,
                },
                deadline - 2,
                peaks=peaks,
            )
            result = measured["result"]
            result["supervisor_peak_rss_bytes"] = measured["peak_rss_bytes"]
            result["network_attempts"] = measured["network_attempts"]
        except Exception as exc:
            progress = workspace / "comparison-progress.json"
            if progress.is_file() and progress.stat().st_size <= DIAGNOSTIC_LIMIT:
                result = json.loads(progress.read_text())
            result["status"] = "incomplete"
            result["error"] = str(exc)
        result.update(peaks)
    result["deadline_seconds"] = deadline_seconds
    result["wall_seconds_including_cleanup"] = time.monotonic() - started
    if result["wall_seconds_including_cleanup"] > deadline_seconds:
        result["status"] = "incomplete"
        result["error"] = "complete cached comparison exceeded deadline"
    return result


def parser():
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--repetitions", type=int, default=3)
    value.add_argument("--cached-repetitions", type=int, default=2)
    value.add_argument("--cached-root", type=Path)
    value.add_argument("--baseline-ref")
    value.add_argument("--deadline-seconds", type=float, default=600)
    value.add_argument("--output", type=Path)
    value.add_argument("--worker", help=argparse.SUPPRESS)
    return value


def validate_output(output, cached_root):
    if output is None:
        return
    path = Path(output).absolute()
    require(
        not any(parent.is_symlink() for parent in (path, *path.parents)),
        "symlink in benchmark output path",
    )
    require(
        cached_root is None or not path.resolve().is_relative_to(Path(cached_root).resolve()),
        "benchmark output must be outside the cached evidence root",
    )


def main(argv=None):
    args = parser().parse_args(argv)
    if args.worker:
        try:
            result = worker(json.loads(args.worker))
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "status": "incomplete",
                        "error": str(exc),
                        "network_attempts": getattr(exc, "network_attempts", 0),
                    }
                )
            )
            return 1
        print(json.dumps(result))
        return 0
    report = {
        "benchmark_revision": 1,
        "status": "incomplete",
        "started_at": datetime.now(UTC).isoformat(),
        "storage": {},
        "http": {},
        "cached": {"status": "not-requested"},
    }
    output_allowed = False
    try:
        validate_output(args.output, args.cached_root)
        output_allowed = True
        require(1 <= args.repetitions <= 10, "repetitions must be between 1 and 10")
        require(2 <= args.cached_repetitions <= 10, "cached repetitions must be between 2 and 10")
        require(
            0 < args.deadline_seconds <= 600, "deadline must be positive and at most 600 seconds"
        )
        require(
            not args.cached_root or args.baseline_ref, "cached comparison requires --baseline-ref"
        )
        if args.cached_root:
            require(args.cached_root.is_dir(), "cached fixture is missing")
        report["candidate_revision"] = revision(REPOSITORY)
        report["benchmark_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        diff = subprocess.run(
            ["git", "-C", str(REPOSITORY), "diff", "HEAD", "--", "src"],
            capture_output=True,
            check=True,
            timeout=10,
        ).stdout
        report["candidate_source_diff_sha256"] = hashlib.sha256(diff).hexdigest()
        report["repetitions"] = args.repetitions
        with tempfile.TemporaryDirectory(
            prefix="market-data-offline-benchmark-", dir=Path(tempfile.gettempdir()).resolve()
        ) as temporary:
            workspace = Path(temporary)
            sources = {"candidate": REPOSITORY}
            if args.baseline_ref:
                baseline, commit = export_baseline(
                    workspace, args.baseline_ref, time.monotonic() + 60
                )
                sources["baseline"] = baseline
                report["baseline_revision"] = commit
            for label, source in sources.items():
                report["storage"][label], report["http"][label] = [], []
                for repetition in range(args.repetitions):
                    for kind in ("storage", "http"):
                        root = workspace / f"{label}-{kind}-{repetition}"
                        result = run_worker(
                            source,
                            workspace,
                            {"kind": kind, "root": str(root)},
                            time.monotonic() + 180,
                        )
                        report[kind][label].append(result)
                        shutil.rmtree(root)
            report["synthetic"] = run_worker(
                REPOSITORY,
                workspace,
                {"kind": "synthetic", "root": str(workspace / "synthetic")},
                time.monotonic() + 180,
            )
        if args.cached_root:
            report["cached"] = cached_comparison(
                args.cached_root, args.baseline_ref, args.cached_repetitions, args.deadline_seconds
            )
            require(
                report["cached"]["status"] == "passed",
                report["cached"].get("error", "cached comparison incomplete"),
            )
        report["limitations"] = [
            "Timings depend on local filesystem caches; correctness gates operation counts, not timing thresholds.",
            "Storage peaks are apparent-byte samples at 0.5-second intervals; Store reservations enforce managed writes.",
            "Semantic hashes exclude only ingestion/acquisition/capture provenance and retain schemas, exact values and duplicate rows.",
        ]
        report["status"] = "passed"
    except Exception as exc:
        report["error"] = str(exc)
    if args.output and output_allowed:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
