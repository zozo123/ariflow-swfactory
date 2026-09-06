"""Storage abstraction for crash-reconciling multi-replica backend state."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


class BackendStore(Protocol):
    def put_if_version(self, key: str, expected_version: int | None, value: dict[str, Any]) -> int: ...
    def get(self, key: str) -> tuple[int, dict[str, Any]] | None: ...
    def pending(self, prefix: str) -> list[tuple[str, int, dict[str, Any]]]: ...


class VersionConflict(RuntimeError):
    pass


@dataclass(frozen=True)
class Lease:
    key: str
    owner: str
    epoch: int
    expires_at: float


class SQLiteBackendStore:
    """Local implementation of the same optimistic-CAS contract intended for Postgres."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS backend_state(
              key TEXT PRIMARY KEY,
              version INTEGER NOT NULL,
              value_json TEXT NOT NULL,
              updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS backend_leases(
              key TEXT PRIMARY KEY,
              owner TEXT NOT NULL,
              epoch INTEGER NOT NULL,
              expires_at REAL NOT NULL
            );
            """
        )

    def get(self, key: str) -> tuple[int, dict[str, Any]] | None:
        row = self.db.execute("SELECT version,value_json FROM backend_state WHERE key=?", (key,)).fetchone()
        if row is None:
            return None
        return row["version"], json.loads(row["value_json"])

    def put_if_version(self, key: str, expected_version: int | None, value: dict[str, Any]) -> int:
        payload = json.dumps(value, sort_keys=True, separators=(",", ":"))
        now = time.time()
        with self.db:
            if expected_version is None:
                try:
                    self.db.execute(
                        "INSERT INTO backend_state(key,version,value_json,updated_at) VALUES(?,1,?,?)",
                        (key, payload, now),
                    )
                    return 1
                except sqlite3.IntegrityError as exc:
                    raise VersionConflict(key) from exc
            next_version = expected_version + 1
            cur = self.db.execute(
                "UPDATE backend_state SET version=?,value_json=?,updated_at=? WHERE key=? AND version=?",
                (next_version, payload, now, key, expected_version),
            )
            if cur.rowcount != 1:
                raise VersionConflict(key)
            return next_version

    def acquire(self, key: str, owner: str, *, ttl_s: float = 30.0) -> Lease:
        now = time.time()
        with self.db:
            row = self.db.execute("SELECT owner,epoch,expires_at FROM backend_leases WHERE key=?", (key,)).fetchone()
            if row is not None and row["expires_at"] > now and row["owner"] != owner:
                raise VersionConflict(f"lease busy: {key}")
            epoch = 1 if row is None else int(row["epoch"]) + 1
            expires = now + ttl_s
            self.db.execute(
                "INSERT INTO backend_leases(key,owner,epoch,expires_at) VALUES(?,?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET owner=excluded.owner,epoch=excluded.epoch,expires_at=excluded.expires_at",
                (key, owner, epoch, expires),
            )
        return Lease(key, owner, epoch, expires)

    def pending(self, prefix: str) -> list[tuple[str, int, dict[str, Any]]]:
        rows = self.db.execute(
            "SELECT key,version,value_json FROM backend_state WHERE key LIKE ? ORDER BY updated_at",
            (prefix + "%",),
        ).fetchall()
        return [(r["key"], r["version"], json.loads(r["value_json"])) for r in rows]
