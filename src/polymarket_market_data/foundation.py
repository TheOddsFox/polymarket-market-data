"""Immutable local evidence; a committed manifest is the recovery boundary."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
import threading
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from filelock import FileLock

UNRESOLVED = {"failed", "invalid", "incomplete", "saturated", "unresolved"}
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\Z")


def now():
    return datetime.now(UTC).isoformat()


def _encode(value):
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("non-finite decimal")
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def json_bytes(value):
    return json.dumps(
        value, default=_encode, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def json_loads(value):
    def invalid(value):
        raise ValueError("non-finite JSON number")

    return json.loads(value, parse_float=Decimal, parse_constant=invalid)


def digest(value):
    return hashlib.sha256(value).hexdigest()


def read_bytes(path, maximum=128 * 1024**2):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > maximum:
            raise ValueError("invalid or oversized regular file")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            data = stream.read(maximum + 1)
        if len(data) > maximum:
            raise ValueError("oversized file")
        return data
    finally:
        os.close(fd)


def read_json(path):
    return json_loads(read_bytes(path))


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write(path, data):
    path = Path(path)
    if path.is_symlink() or path.parent.is_symlink():
        raise ValueError("symlink destination")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fsync_dir(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


@dataclass(frozen=True)
class Limits:
    max_requests: int
    max_download_bytes: int
    max_storage_bytes: int
    max_response_bytes: int
    max_temp_bytes: int

    def __post_init__(self):
        for value in self.__dict__.values():
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError("all resource limits must be positive integers")

    @classmethod
    def from_dict(cls, value):
        return cls(**value)

    def as_dict(self):
        return dict(self.__dict__)


class BudgetExceeded(ValueError):
    pass


def _no_float(value):
    if isinstance(value, float):
        raise ValueError("binary float forbidden in financial records")
    if isinstance(value, dict):
        for child in value.values():
            _no_float(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _no_float(child)


def write_parquet(path, rows, schema=None):
    _no_float(rows)
    # Decimal inference preserves source precision; fixed schemas can be supplied by each dataset.
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path, compression="zstd")
    with open(path, "rb") as stream:
        os.fsync(stream.fileno())
    return {"rows": len(rows), "schema": str(table.schema)}


def file_info(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("expected regular evidence file")
        hasher, size = hashlib.sha256(), 0
        with os.fdopen(fd, "rb", closefd=False) as stream:
            while chunk := stream.read(1024**2):
                hasher.update(chunk)
                size += len(chunk)
        return {"sha256": hasher.hexdigest(), "size": size}
    finally:
        os.close(fd)


def file_inventory(directory):
    return {p.name: file_info(p) for p in sorted(directory.iterdir()) if p.name != "manifest.json"}


def verify_directory(directory):
    directory = Path(directory)
    if directory.is_symlink():
        raise ValueError("symlink bundle")
    manifest = read_json(directory / "manifest.json")
    files = manifest.get("files")
    if not isinstance(files, dict) or set(p.name for p in directory.iterdir()) != set(files) | {
        "manifest.json"
    }:
        raise ValueError("unexpected bundle inventory")
    for name, expected in files.items():
        if not NAME.fullmatch(name):
            raise ValueError("unsafe manifest path")
        actual = file_info(directory / name)
        if actual != {"sha256": expected["sha256"], "size": expected["size"]}:
            raise ValueError("bundle checksum mismatch")
    return manifest


class _EvidenceLock(FileLock):
    """Verified source objects live only as long as the outer writer operation."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._object_state = threading.local()

    def acquire(self, *args, **kwargs):
        result = super().acquire(*args, **kwargs)
        if self.lock_counter == 1:
            self._object_state.cache = {}
        return result

    def release(self, force=False):
        try:
            super().release(force)
        finally:
            if not self.is_locked:
                self._object_state.cache = None

    @property
    def object_cache(self):
        return getattr(self._object_state, "cache", None) if self.is_locked else None


def _fingerprint(path):
    info = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("expected regular evidence file")
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


class Store:
    def __init__(self, root, contract, limits=None):
        root = Path(root).absolute()
        for parent in (root, *root.parents):
            if parent.is_symlink():
                raise ValueError("symlink data root")
        # An operator root inside any Git checkout must be ignored before acquisition.
        probe = root
        while not probe.exists():
            probe = probe.parent
        checkout = next(
            (parent for parent in (probe, *probe.parents) if (parent / ".git").exists()), None
        )
        if checkout is not None:
            try:
                ignored = subprocess.run(
                    ["git", "-C", str(checkout), "check-ignore", "--no-index", str(root)],
                    capture_output=True,
                )
            except FileNotFoundError:
                raise ValueError("Git is required to verify a root inside a checkout") from None
            if ignored.returncode != 0:
                raise ValueError("data root inside a checkout must be gitignored")
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root, self.contract, self.limits = root, contract, limits
        if (root / ".writer.lock").is_symlink():
            raise ValueError("symlink writer lock")
        self.lock = _EvidenceLock(root / ".writer.lock", timeout=0)
        with self.lock:
            for name in ("inputs", "plans", "acquisitions", "published", "objects", ".state"):
                path = root / name
                if path.is_symlink():
                    raise ValueError("symlink store directory")
                path.mkdir(exist_ok=True)
            sentinel = root / ".state" / "root.json"
            if not sentinel.exists():
                atomic_write(sentinel, json_bytes({"contract": contract, "created_at": now()}))
            if read_json(sentinel).get("contract") != contract:
                raise ValueError("wrong data-root contract")

    def _name(self, value):
        if not isinstance(value, str) or not NAME.fullmatch(value):
            raise ValueError("invalid local identifier")
        return value

    def _budget(self, additional=0):
        if not self.limits:
            return
        size = 0
        for path in self.root.rglob("*"):
            if path.is_symlink():
                raise ValueError("symlink in data root")
            if path.is_file():
                size += path.stat().st_size
        if size + additional > self.limits.max_storage_bytes:
            raise BudgetExceeded("retained storage budget exhausted")

    def create_plan(self, data):
        Limits.from_dict(data["limits"])
        payload = {**data, "contract": self.contract + ".plan"}
        plan_id = digest(json_bytes(payload))
        with self.lock:
            path = self.root / "plans" / (plan_id + ".json")
            if not path.exists():
                self._budget(len(json_bytes(payload)))
                atomic_write(path, json_bytes(payload))
            elif read_bytes(path) != json_bytes(payload):
                raise ValueError("immutable plan collision")
        return plan_id

    def load_plan(self, plan_id):
        path = self.root / "plans" / (self._name(plan_id) + ".json")
        value = read_json(path)
        if digest(json_bytes(value)) != plan_id or value["contract"] != self.contract + ".plan":
            raise ValueError("plan integrity mismatch")
        return value

    def new_acquisition(self, plan_id, source, resume_id=None):
        plan = self.load_plan(plan_id)
        self.limits = Limits.from_dict(plan["limits"])
        with self.lock:
            if resume_id:
                value = read_json(
                    self.root / "acquisitions" / self._name(resume_id) / "acquisition.json"
                )
                if value["plan_id"] != plan_id or value["source"] != source:
                    raise ValueError("resume acquisition mismatch")
                return resume_id
            acquisition_id = uuid.uuid4().hex
            directory = self.root / "acquisitions" / acquisition_id
            descriptor = json_bytes(
                {"id": acquisition_id, "plan_id": plan_id, "source": source, "created_at": now()}
            )
            self._budget(len(descriptor))
            directory.mkdir()
            (directory / "captures").mkdir()
            atomic_write(directory / "acquisition.json", descriptor)
            return acquisition_id

    def acquisition(self, acquisition_id):
        return read_json(
            self.root / "acquisitions" / self._name(acquisition_id) / "acquisition.json"
        )

    def capture(
        self,
        acquisition_id,
        unit_id,
        relations,
        evidence=None,
        coverage=None,
        schemas=None,
        objects=None,
    ):
        acquisition = self.acquisition(acquisition_id)
        for obj in objects or []:
            self.object_path(obj)
        capture_id = digest(json_bytes([acquisition_id, unit_id]))
        parent = self.root / "acquisitions" / acquisition_id / "captures"
        destination = parent / capture_id
        with self.lock:
            if destination.exists():
                existing = verify_directory(destination)
                if existing["unit_id"] != unit_id:
                    raise ValueError("capture identity collision")
                return existing
            self._budget()
            pending = Path(tempfile.mkdtemp(prefix=".pending-", dir=parent))
            try:
                atomic_write(pending / "evidence.json", json_bytes(evidence or {}))
                table_info = {}
                for name, rows in relations.items():
                    self._name(name)
                    table_info[name] = write_parquet(
                        pending / (name + ".parquet"), rows, (schemas or {}).get(name)
                    )
                files = file_inventory(pending)
                size = sum(f["size"] for f in files.values())
                if self.limits and size > self.limits.max_temp_bytes:
                    raise BudgetExceeded("temporary capture budget exhausted")
                self._budget()  # Includes pending files, without double-counting.
                manifest = {
                    "contract": self.contract + ".capture",
                    "capture_id": capture_id,
                    "acquisition_id": acquisition_id,
                    "plan_id": acquisition["plan_id"],
                    "source": acquisition["source"],
                    "unit_id": unit_id,
                    "created_at": now(),
                    "schema_version": 1,
                    "tables": table_info,
                    "coverage": coverage or [],
                    "objects": objects or [],
                    "files": files,
                }
                manifest_bytes = json_bytes(manifest)
                self._budget(len(manifest_bytes))
                atomic_write(pending / "manifest.json", manifest_bytes)
                os.rename(pending, destination)
                fsync_dir(parent)
                return manifest
            finally:
                if pending.exists():
                    shutil.rmtree(pending)

    def committed_captures(self, acquisition_id=None, plan_id=None):
        with self.lock:
            captures = []
            roots = (
                [self.root / "acquisitions" / self._name(acquisition_id)]
                if acquisition_id
                else sorted((self.root / "acquisitions").iterdir())
            )
            for directory in roots:
                for capture in sorted((directory / "captures").iterdir()):
                    if capture.name.startswith(".pending-"):
                        continue
                    value = self._verify_capture(capture)
                    if plan_id is None or value["plan_id"] == plan_id:
                        captures.append(value)
            return captures

    def _verify_capture(self, path):
        if path.parent.is_symlink() or path.parent.parent.is_symlink():
            raise ValueError("symlink capture parent")
        value = verify_directory(path)
        if value.get("contract") != self.contract + ".capture" or value.get("schema_version") != 1:
            raise ValueError("unsupported capture contract/schema revision")
        if (
            value["capture_id"] != path.name
            or value["acquisition_id"] != path.parent.parent.name
            or digest(json_bytes([value["acquisition_id"], value["unit_id"]])) != path.name
        ):
            raise ValueError("capture identity mismatch")
        for obj in value.get("objects", []):
            self.object_path(obj)
        return value

    def retain_object(self, path):
        path = Path(path)
        with self.lock:
            before = _fingerprint(path)
            info = file_info(path)
            if _fingerprint(path) != before:
                raise ValueError("source object changed during verification")
            reference = {"path": "objects/" + info["sha256"], **info}
            destination = self.root / reference["path"]
            if destination.exists():
                self.object_path(reference)
            else:
                self._budget(0 if path.is_relative_to(self.root) else info["size"])
                if not path.is_relative_to(self.root / ".state"):
                    raise ValueError("source object must be downloaded inside root state")
                os.rename(path, destination)
                fsync_dir(destination.parent)
                retained = _fingerprint(destination)
                if retained[:4] != before[:4]:
                    raise ValueError("source object changed during retention")
                self.lock.object_cache[(info["sha256"], info["size"])] = retained
        return reference

    def object_path(self, info):
        if info.get("path") != "objects/" + info.get("sha256", "") or not re.fullmatch(
            r"[0-9a-f]{64}", info.get("sha256", "")
        ):
            raise ValueError("unsafe source object reference")
        if isinstance(info.get("size"), bool) or not isinstance(info.get("size"), int):
            raise ValueError("unsafe source object size")
        path = self.root / "objects" / info["sha256"]
        with self.lock:
            if path.parent.is_symlink():
                raise ValueError("source object reference mismatch")
            before = _fingerprint(path)
            key = info["sha256"], info["size"]
            cache = self.lock.object_cache
            if cache.get(key) != before:
                cache.pop(key, None)
                actual = file_info(path)
                if _fingerprint(path) != before:
                    raise ValueError("source object changed during verification")
                if actual != {"sha256": info["sha256"], "size": info["size"]}:
                    raise ValueError("source object reference mismatch")
                cache[key] = before
        return path

    def capture_path(self, capture):
        return (
            self.root
            / "acquisitions"
            / self._name(capture["acquisition_id"])
            / "captures"
            / self._name(capture["capture_id"])
        )

    def read_relation(self, capture, relation):
        verify_directory(self.capture_path(capture))
        path = self.capture_path(capture) / (self._name(relation) + ".parquet")
        return pq.read_table(path).to_pylist() if path.exists() else []

    def read_evidence(self, capture):
        verify_directory(self.capture_path(capture))
        return read_json(self.capture_path(capture) / "evidence.json")

    def build_release(self, plan_id, captures, relations, coverage, schemas=None, versions=None):
        plan = self.load_plan(plan_id)
        self.limits = Limits.from_dict(plan["limits"])
        if not captures:
            raise ValueError("release requires committed captures")
        for capture in captures:
            verified = verify_directory(self.capture_path(capture))
            if verified != capture or capture["plan_id"] != plan_id:
                raise ValueError("release input mismatch")
            if any(row.get("status") in UNRESOLVED for row in verified["coverage"]):
                raise ValueError("unresolved capture coverage cannot publish")
        if any(c.get("status") in UNRESOLVED for c in coverage):
            raise ValueError("unresolved coverage cannot publish")
        identity = {
            "plan_id": plan_id,
            "inputs": sorted(c["capture_id"] for c in captures),
            "input_manifests": {
                c["capture_id"]: digest(read_bytes(self.capture_path(c) / "manifest.json"))
                for c in captures
            },
            "relations": digest(json_bytes(relations)),
            "coverage": coverage,
            "versions": versions or {"schema": 1, "normalizer": 1},
        }
        parent = self.root / "published"
        with self.lock:
            pending = Path(tempfile.mkdtemp(prefix=".pending-", dir=parent))
            try:
                for name, rows in relations.items():
                    self._name(name)
                    write_parquet(pending / (name + ".parquet"), rows, (schemas or {}).get(name))
                atomic_write(pending / "coverage.json", json_bytes(coverage))
                atomic_write(pending / "plan.json", json_bytes(plan))
                self._budget()
                if (
                    self.limits
                    and sum(p.stat().st_size for p in pending.iterdir())
                    > self.limits.max_temp_bytes
                ):
                    raise BudgetExceeded("temporary release budget exhausted")
                identity["files"] = file_inventory(pending)
                release_id = digest(json_bytes(identity))
                destination = parent / release_id
                if destination.exists():
                    self.verify(release_id)
                    return release_id
                manifest = {
                    "contract": self.contract,
                    "release_id": release_id,
                    "capture_complete": True,
                    **identity,
                    "created_at": now(),
                }
                manifest_bytes = json_bytes(manifest)
                self._budget(len(manifest_bytes))
                atomic_write(pending / "manifest.json", manifest_bytes)
                os.rename(pending, destination)
                fsync_dir(parent)
                return release_id
            finally:
                if pending.exists():
                    shutil.rmtree(pending)

    def verify(self, release_id=None):
        with self.lock:
            return self._verify(release_id)

    def _verify(self, release_id=None):
        if release_id:
            directory = self.root / "published" / self._name(release_id)
            manifest = verify_directory(directory)
            if manifest["contract"] != self.contract:
                raise ValueError("release contract mismatch")
            identity = {
                key: manifest[key]
                for key in (
                    "plan_id",
                    "inputs",
                    "input_manifests",
                    "relations",
                    "coverage",
                    "versions",
                    "files",
                )
            }
            if (
                manifest.get("release_id") != release_id
                or digest(json_bytes(identity)) != release_id
            ):
                raise ValueError("immutable release identity mismatch")
            plan = self.load_plan(manifest["plan_id"])
            if read_json(directory / "plan.json") != plan:
                raise ValueError("release plan input mismatch")
            for key in ("metadata_sha256", "metadata_manifest_sha256", "metadata_digest"):
                if key in plan:
                    from .metadata import load_metadata

                    load_metadata(self.root / "inputs" / plan[key], plan[key])
            if set(manifest["inputs"]) != set(manifest["input_manifests"]):
                raise ValueError("release capture input inventory mismatch")
            for capture_id, expected in manifest["input_manifests"].items():
                paths = list(
                    (self.root / "acquisitions").glob("*/captures/" + self._name(capture_id))
                )
                if len(paths) != 1:
                    raise ValueError("release capture input missing or ambiguous")
                capture = self._verify_capture(paths[0])
                if capture["plan_id"] != manifest["plan_id"]:
                    raise ValueError("release capture plan mismatch")
                if digest(read_bytes(paths[0] / "manifest.json")) != expected:
                    raise ValueError("release capture manifest drift")
            if any(c.get("status") in UNRESOLVED for c in manifest["coverage"]):
                raise ValueError("unresolved published coverage")
            return manifest
        with self.lock:
            published = self.root / "published"
            manifests = {
                p.name: self.verify(p.name)
                for p in published.iterdir()
                if p.is_dir() and not p.name.startswith(".pending-")
            }
            for path in published.glob("*.current.json"):
                try:
                    pointer = read_json(path)
                except (ValueError, OSError):
                    raise ValueError("malformed active publication pointer") from None
                if not isinstance(pointer, dict) or not isinstance(pointer.get("release_id"), str):
                    raise ValueError("malformed active publication pointer")
                manifest = manifests.get(pointer["release_id"])
                if manifest is None:
                    raise ValueError("active publication pointer references missing release")
                release_id, plan_id = manifest["release_id"], manifest["plan_id"]
                expected = {
                    "release_id": release_id,
                    "manifest_sha256": digest(read_bytes(published / release_id / "manifest.json")),
                }
                if (
                    pointer != expected
                    or path.name != plan_id + ".current.json"
                    or (published / (release_id + ".invalid.json")).exists()
                    or (self.root / ".state" / (plan_id + ".invalid.json")).exists()
                ):
                    raise ValueError("active publication pointer integrity mismatch")
            return {"captures": len(self.committed_captures()), "releases": list(manifests)}

    def publish(self, release_id):
        with self.lock:
            manifest = self.verify(release_id)
            if (self.root / "published" / (release_id + ".invalid.json")).exists():
                raise ValueError("invalidated release")
            pointer = {
                "release_id": release_id,
                "manifest_sha256": digest(
                    read_bytes(self.root / "published" / release_id / "manifest.json")
                ),
            }
            path = self.root / "published" / (manifest["plan_id"] + ".current.json")
            encoded = json_bytes(pointer)
            self.limits = Limits.from_dict(self.load_plan(manifest["plan_id"])["limits"])
            self._budget(max(0, len(encoded) - (path.stat().st_size if path.exists() else 0)))
            atomic_write(path, encoded)
        return pointer

    def invalidate(self, release_id, reason):
        with self.lock:
            manifest = self.verify(release_id)
            atomic_write(
                self.root / "published" / (release_id + ".invalid.json"),
                json_bytes({"reason": reason, "at": now()}),
            )
            pointer = self.root / "published" / (manifest["plan_id"] + ".current.json")
            if pointer.exists() and read_json(pointer)["release_id"] == release_id:
                pointer.unlink()
                fsync_dir(pointer.parent)

    def status(self):
        return {
            "contract": self.contract,
            "root": str(self.root),
            "plans": len(list((self.root / "plans").glob("*.json"))),
            "acquisitions": len(list((self.root / "acquisitions").iterdir())),
            **self.verify(),
        }
