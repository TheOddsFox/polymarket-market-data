"""Bounded local consumer queries and stopped-writer restore acceptance."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import duckdb

from .foundation import Store, file_info


def consumer_check(store, release_id):
    manifest = store.verify(release_id)
    names = {
        "instruments",
        "price_samples",
        "sampled_ohlc",
        "book_updates",
        "book_states",
        "trades",
        "coverage",
    }
    if {
        name.removesuffix(".parquet") for name in manifest["files"] if name.endswith(".parquet")
    } != names:
        raise ValueError("unsupported consumer relation inventory")
    directory = store.root / "published" / release_id
    with tempfile.TemporaryDirectory(dir=store.root / ".state") as temporary:
        with duckdb.connect(
            config={"autoload_known_extensions": False, "autoinstall_known_extensions": False}
        ) as connection:
            connection.execute("SET threads=1")
            maximum = (
                min(512 * 1024**2, store.limits.max_temp_bytes // 2)
                if store.limits
                else 512 * 1024**2
            )
            connection.execute("SET memory_limit=?", [f"{maximum}B"])
            connection.execute("SET max_temp_directory_size=?", [f"{maximum}B"])
            connection.execute("SET temp_directory=?", [temporary])
            counts = {}
            for name in sorted(names):
                connection.read_parquet(str(directory / (name + ".parquet"))).create_view(name)
                counts[name] = connection.execute("SELECT count(*) FROM " + name).fetchone()[0]
            if connection.execute(
                "SELECT count(*) - count(DISTINCT (venue, asset_kind, asset_id)) FROM instruments"
            ).fetchone()[0]:
                raise ValueError("duplicate consumer instrument identity")
            key = "venue, asset_kind, asset_id"
            for name in (
                "price_samples",
                "sampled_ohlc",
                "book_updates",
                "book_states",
                "trades",
                "coverage",
            ):
                bad = connection.execute(
                    f"SELECT count(*) FROM {name} f LEFT JOIN instruments i USING ({key}) "
                    "WHERE i.asset_id IS NULL OR f.market_id IS DISTINCT FROM i.market_id "
                    "OR f.condition_id IS DISTINCT FROM i.condition_id "
                    "OR f.outcome_index IS DISTINCT FROM i.outcome_index"
                ).fetchone()[0]
                if bad:
                    raise ValueError(name + " consumer identity mismatch")
            connection.execute("""
                CREATE TEMP VIEW expected_ohlc AS
                SELECT source, acquisition_id, capture_id, venue, asset_kind, asset_id,
                       date_trunc('minute', source_at) AS bucket_start,
                       count(*) AS n, arg_min(price, source_at) AS o,
                       max(price) AS h, min(price) AS l, arg_max(price, source_at) AS c
                FROM price_samples GROUP BY ALL
            """)
            if (
                counts["sampled_ohlc"]
                != connection.execute("SELECT count(*) FROM expected_ohlc").fetchone()[0]
            ):
                raise ValueError("consumer sampled OHLC grain differs from its selected capture")
            bad = connection.execute("""
                SELECT count(*) FROM sampled_ohlc a FULL JOIN expected_ohlc e
                USING (source, acquisition_id, capture_id, venue, asset_kind, asset_id, bucket_start)
                WHERE a.asset_id IS NULL OR e.asset_id IS NULL
                   OR a.observed_samples IS DISTINCT FROM e.n
                   OR a.open IS DISTINCT FROM e.o OR a.high IS DISTINCT FROM e.h
                   OR a.low IS DISTINCT FROM e.l OR a.close IS DISTINCT FROM e.c
                   OR a.bucket_end IS DISTINCT FROM a.bucket_start + INTERVAL 1 MINUTE
            """).fetchone()[0]
            if bad:
                raise ValueError("consumer sampled OHLC differs from its selected capture")
            for name in ("book_updates", "book_states"):
                if connection.execute(
                    f"SELECT count(*) - count(DISTINCT "
                    f"(source, venue, asset_kind, asset_id, stream, sequence, ordinal)) FROM {name}"
                ).fetchone()[0]:
                    raise ValueError(name + " consumer stream position is duplicated")
            bad = connection.execute("""
                SELECT count(*) FROM book_updates u FULL JOIN book_states s
                USING (source, venue, asset_kind, asset_id, stream, sequence, ordinal)
                WHERE u.asset_id IS NULL OR s.asset_id IS NULL
            """).fetchone()[0]
            if bad or counts["book_updates"] != counts["book_states"]:
                raise ValueError("consumer book update/state alignment differs")
            receipts = connection.execute(
                "SELECT source, status, count(*) FROM coverage GROUP BY ALL ORDER BY 1,2"
            ).fetchall()
            return {
                "relations": counts,
                "coverage_receipts": [list(row) for row in receipts],
                "coverage_is_additive": False,
            }


def restore_check(store, destination: Path):
    destination = Path(destination)
    if destination.exists() or any(
        path.is_symlink() for path in (destination, *destination.parents)
    ):
        raise ValueError("restore acceptance needs a fresh regular destination")
    with store.lock:
        store.verify()
        inventory = {}
        for path in store.root.rglob("*"):
            if path.is_symlink():
                raise ValueError("symlink in backup source")
            if path.is_file():
                inventory[str(path.relative_to(store.root))] = file_info(path)
        shutil.copytree(store.root, destination)
        actual = {
            str(path.relative_to(destination)): file_info(path)
            for path in destination.rglob("*")
            if path.is_file()
        }
        if actual != inventory:
            raise ValueError("restored root inventory differs")
    restored = Store(destination, store.contract)
    status = restored.status()
    return {
        "files": len(inventory),
        "bytes": sum(value["size"] for value in inventory.values()),
        "status": status,
    }
