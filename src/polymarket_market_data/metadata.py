"""Pin selected metadata without trusting paths or producer retention."""

from __future__ import annotations

import os
import re
import shutil
import stat
import tempfile
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path

from .foundation import BudgetExceeded, atomic_write, digest, fsync_dir, json_loads, read_bytes

CONTRACT = "oddsfox.polymarket.metadata.v1"
FILES = {
    "markets.json",
    "outcomes.json",
    "memberships.json",
    "identity_history.json",
    "coverage.json",
}
MAX_FILE = 32 * 1024**2
MAX_BUNDLE = 128 * 1024**2
KINDS = {"ctf_token", "poly_v2_position"}


@dataclass(frozen=True)
class MetadataBundle:
    path: Path
    manifest: dict
    markets: list
    outcomes: list
    memberships: list
    identity_history: list
    coverage: list


def _file(fd, name):
    child = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=fd)
    try:
        info = os.fstat(child)
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_FILE:
            raise ValueError("metadata requires bounded regular files")
        with os.fdopen(child, "rb", closefd=False) as stream:
            result = stream.read(MAX_FILE + 1)
        if len(result) > MAX_FILE:
            raise ValueError("metadata file limit")
        return result
    finally:
        os.close(child)


def _read(directory, expected_digest):
    if not re.fullmatch(r"[0-9a-f]{64}", expected_digest or ""):
        raise ValueError("operator-pinned metadata manifest SHA256 required")
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        raw_manifest = _file(fd, "manifest.json")
        if digest(raw_manifest) != expected_digest:
            raise ValueError("metadata manifest digest mismatch")
        manifest = json_loads(raw_manifest)
        if manifest.get("contract") != CONTRACT or set(manifest.get("files", {})) != FILES:
            raise ValueError("unsupported metadata contract or inventory")
        if set(os.listdir(fd)) != FILES | {"manifest.json"}:
            raise ValueError("unexpected metadata file")
        blobs = {"manifest.json": raw_manifest}
        total = len(raw_manifest)
        for name in sorted(FILES):
            raw = _file(fd, name)
            total += len(raw)
            expected = manifest["files"][name]
            if len(raw) != expected["size"] or digest(raw) != expected["sha256"]:
                raise ValueError("metadata file digest mismatch")
            if total > MAX_BUNDLE:
                raise ValueError("metadata bundle limit")
            rows = json_loads(raw)
            if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                raise ValueError("metadata relation must contain objects")
            blobs[name] = raw
        return manifest, blobs
    finally:
        os.close(fd)


def load_metadata(directory, expected_digest):
    manifest, blobs = _read(directory, expected_digest)
    relations = {name[:-5]: json_loads(blobs[name]) for name in FILES}
    bundle = MetadataBundle(Path(directory), manifest, **relations)
    _validate(bundle)
    return bundle


def _validate(bundle):
    markets = {}
    coverage_ids = set()
    for row in bundle.coverage:
        market_id = row.get("market_id")
        if (
            not isinstance(market_id, str)
            or not re.fullmatch(r"[1-9][0-9]{0,19}", market_id)
            or market_id in coverage_ids
        ):
            raise ValueError("invalid or duplicate metadata coverage")
        coverage_ids.add(market_id)
    for market in bundle.markets:
        key = (market.get("venue"), market.get("market_id"))
        if (
            key[0] != "polymarket"
            or not isinstance(key[1], str)
            or not re.fullmatch(r"[0-9]+", key[1])
            or key in markets
        ):
            raise ValueError("invalid/duplicate metadata market")
        markets[key] = market
    identities, ordinals = {}, set()
    for row in bundle.outcomes:
        if row.get("usable") is False or row.get("identity_error"):
            continue
        key = (row.get("venue"), row.get("asset_kind"), row.get("asset_id"))
        native = row.get("asset_id")
        ordinal = row.get("outcome_index")
        if (
            key[0] != "polymarket"
            or key[1] not in KINDS
            or not isinstance(native, str)
            or not re.fullmatch(r"[0-9]+", native)
            or not 0 < int(native) < 2**256
            or str(int(native)) != native
        ):
            raise ValueError("invalid typed native metadata identity")
        if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 1:
            raise ValueError("outcome_index must be one-based integer")
        market_key = (key[0], row.get("market_id"))
        if market_key not in markets:
            raise ValueError("outcome without market")
        ordinal_key = (*market_key, ordinal)
        if ordinal_key in ordinals or key in identities:
            raise ValueError("duplicate/conflicting metadata identity")
        ordinals.add(ordinal_key)
        identities[key] = row
        field = "clob_token_id" if key[1] == "ctf_token" else "position_id"
        if row.get(field) != native:
            raise ValueError("asset kind/native ID disagreement")
        condition = row.get("condition_id")
        if condition is not None and not re.fullmatch(r"0x[0-9a-fA-F]{64}", condition):
            raise ValueError("invalid condition identity")


def pin_metadata(root, directory, expected_digest, *, store=None):
    root = Path(root).absolute()
    if any(parent.is_symlink() for parent in (root, *root.parents)):
        raise ValueError("symlink metadata root")
    if store is not None and root.absolute() != store.root:
        raise ValueError("metadata budget store/root disagreement")
    with store.lock if store is not None else nullcontext():
        manifest, blobs = _read(directory, expected_digest)
        # Validate before any committed input appears.
        load_metadata(directory, expected_digest)
        parent = root / "inputs"
        if parent.is_symlink():
            raise ValueError("symlink metadata destination")
        parent.mkdir(parents=True, exist_ok=True)
        target = parent / expected_digest
        if target.exists():
            return load_metadata(target, expected_digest)
        incoming = sum(map(len, blobs.values()))
        if store is not None:
            store._budget(incoming)
            if store.limits and incoming > store.limits.max_temp_bytes:
                raise BudgetExceeded("metadata temporary byte budget exhausted")
        temporary = Path(tempfile.mkdtemp(prefix=".pending-", dir=parent))
        try:
            for name, raw in blobs.items():
                (store.atomic_write if store is not None else atomic_write)(temporary / name, raw)
            load_metadata(temporary, expected_digest)
            (store.rename if store is not None else os.rename)(temporary, target)
            fsync_dir(parent)
        finally:
            if temporary.exists():
                (store.remove if store is not None else shutil.rmtree)(temporary)
        return load_metadata(target, expected_digest)


def select_assets(bundle, market_ids=None):
    selected = set(market_ids or [row["market_id"] for row in bundle.markets])
    if not selected:
        raise ValueError("explicit nonempty market selection required")
    markets = {row["market_id"]: row for row in bundle.markets}
    coverage = {row["market_id"]: row for row in bundle.coverage}
    for market_id in selected:
        if (
            market_id not in markets
            or markets[market_id].get("usable") is False
            or markets[market_id].get("identity_error")
        ):
            raise ValueError("selected market missing or unusable")
        if market_id not in coverage or coverage[market_id].get("status") != "found":
            raise ValueError("selected metadata coverage unresolved")
    assets = [
        dict(row)
        for row in bundle.outcomes
        if row["market_id"] in selected
        and row.get("usable") is not False
        and not row.get("identity_error")
    ]
    if {row["market_id"] for row in assets} != selected:
        raise ValueError("selected market lacks validated assets")
    return sorted(assets, key=lambda row: (row["market_id"], row["outcome_index"]))


def manifest_digest(directory):
    return digest(read_bytes(Path(directory) / "manifest.json"))
