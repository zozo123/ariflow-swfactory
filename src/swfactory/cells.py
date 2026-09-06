"""Durable Factory Cell identity, fencing and append-only evidence timeline.

Airflow remains the scheduler. This module only owns durable identity and mutation authority.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
TERMINAL_STATES = frozenset({"success", "failed", "cancelled", "rejected", "cleaned"})


class CellError(RuntimeError):
    pass


class StaleEpoch(CellError):
    pass


class DuplicateOperation(CellError):
    pass


class CellBusy(CellError):
    pass


@dataclass(frozen=True)
class CellIdentity:
    repo: str
    target: str
    issue: str

    def stable_id(self) -> str:
        raw = f"{self.repo}\0{self.target}\0{self.issue}".encode()
        return "cell_" + hashlib.sha256(raw).hexdigest()[:24]


@dataclass(frozen=True)
class Mutation:
    cell_id: str
    epoch: int
    operation_key: str
    kind: str
    payload: dict[str, Any]


class CellStore:
    """SQLite-backed local durable cell store with expected-epoch fencing.

    The API is intentionally narrow so the same semantics can later be implemented by Postgres.
    One store may be shared by the backend's request threads; an in-process RLock protects the
    single SQLite connection while SQLite transactions remain the durable concurrency boundary.
    No process-local lock is treated as ownership authority: epochs in the database are.
    """

    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(
            path,
            timeout=30,
            isolation_level=None,
            check_same_thread=False,
        )
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self._migrate()

    def close(self) -> None:
        with self.lock:
            self.db.close()

    def _migrate(self) -> None:
        with self.lock:
            self.db.executescript(
                """
                CREATE TABLE IF NOT EXISTS cells (
                    cell_id TEXT PRIMARY KEY,
                    schema_version INTEGER NOT NULL,
                    repo TEXT NOT NULL,
                    target TEXT NOT NULL,
                    issue TEXT NOT NULL,
                    epoch INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    airflow_dag_id TEXT,
                    airflow_run_id TEXT,
                    map_index INTEGER,
                    factory_generation TEXT,
                    policy_digest TEXT,
                    base_sha TEXT,
                    observed_target_sha TEXT,
                    compute_json TEXT,
                    cleanup_json TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS cells_identity
                    ON cells(repo, target, issue);
                CREATE TABLE IF NOT EXISTS cell_events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    cell_id TEXT NOT NULL,
                    epoch INTEGER NOT NULL,
                    operation_key TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    UNIQUE(cell_id, operation_key),
                    FOREIGN KEY(cell_id) REFERENCES cells(cell_id)
                );
                """
            )

    def ensure(self, identity: CellIdentity, **fields: Any) -> dict[str, Any]:
        cell_id = identity.stable_id()
        now = time.time()
        with self.lock, self.db:
            self._insert_identity(identity, now)
            if fields:
                self.patch(cell_id, 1, "ensure:init", **fields)
            return self.get(cell_id)

    def activate(self, identity: CellIdentity, actor: str) -> dict[str, Any]:
        """Atomically claim an issue×target cell for one lifecycle activation.

        A fresh cell keeps epoch 1. Re-activating a terminal cell advances the epoch, which fences
        every stale actor from the previous lifecycle. An active/dispatching cell is never silently
        duplicated or taken over by ordinary submission.
        """

        cell_id = identity.stable_id()
        now = time.time()
        with self.lock, self.db:
            self._insert_identity(identity, now)
            row = self.db.execute("SELECT * FROM cells WHERE cell_id=?", (cell_id,)).fetchone()
            if row is None:  # defensive: INSERT OR IGNORE above must make this impossible
                raise KeyError(cell_id)
            current = self._decode(row)
            fresh = current["state"] == "created" and current["airflow_run_id"] is None
            if not fresh and current["state"] not in TERMINAL_STATES:
                raise CellBusy(
                    f"{cell_id} is already active at epoch {current['epoch']} "
                    f"in state {current['state']}"
                )
            next_epoch = int(current["epoch"]) if fresh else int(current["epoch"]) + 1
            cur = self.db.execute(
                """UPDATE cells SET
                    epoch=?, state='dispatching', airflow_dag_id=NULL, airflow_run_id=NULL,
                    map_index=NULL, compute_json=NULL, cleanup_json=NULL, updated_at=?
                    WHERE cell_id=? AND epoch=? AND state=?""",
                (next_epoch, now, cell_id, current["epoch"], current["state"]),
            )
            if cur.rowcount != 1:
                raise StaleEpoch(f"{cell_id}: activation lost its compare-and-swap")
            self._append(
                Mutation(
                    cell_id=cell_id,
                    epoch=next_epoch,
                    operation_key=f"activation:{next_epoch}",
                    kind="activated",
                    payload={"actor": actor},
                )
            )
            return self.get(cell_id)

    def _insert_identity(self, identity: CellIdentity, now: float) -> None:
        self.db.execute(
            """INSERT OR IGNORE INTO cells(
                cell_id,schema_version,repo,target,issue,epoch,state,created_at,updated_at
            ) VALUES(?,?,?,?,?,1,'created',?,?)""",
            (
                identity.stable_id(),
                SCHEMA_VERSION,
                identity.repo,
                identity.target,
                identity.issue,
                now,
                now,
            ),
        )

    def get(self, cell_id: str) -> dict[str, Any]:
        with self.lock:
            row = self.db.execute("SELECT * FROM cells WHERE cell_id=?", (cell_id,)).fetchone()
            if row is None:
                raise KeyError(cell_id)
            return self._decode(row)

    def list(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.db.execute(
                "SELECT * FROM cells ORDER BY updated_at DESC LIMIT ?",
                (max(1, min(limit, 1000)),),
            ).fetchall()
            return [self._decode(r) for r in rows]

    def history(self, cell_id: str) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.db.execute(
                "SELECT seq,epoch,operation_key,kind,payload_json,created_at "
                "FROM cell_events WHERE cell_id=? ORDER BY seq",
                (cell_id,),
            ).fetchall()
            return [
                {
                    "seq": r["seq"],
                    "epoch": r["epoch"],
                    "operation_key": r["operation_key"],
                    "kind": r["kind"],
                    "payload": json.loads(r["payload_json"]),
                    "created_at": r["created_at"],
                }
                for r in rows
            ]

    def take_epoch(self, cell_id: str, expected_epoch: int, actor: str) -> int:
        """Advance the mutation fence atomically and record an explicit takeover."""
        next_epoch = expected_epoch + 1
        now = time.time()
        with self.lock, self.db:
            cur = self.db.execute(
                "UPDATE cells SET epoch=?, updated_at=? WHERE cell_id=? AND epoch=?",
                (next_epoch, now, cell_id, expected_epoch),
            )
            if cur.rowcount != 1:
                raise StaleEpoch(f"{cell_id}: expected epoch {expected_epoch}")
            self._append(
                Mutation(
                    cell_id,
                    next_epoch,
                    f"epoch:{next_epoch}",
                    "epoch_advanced",
                    {"actor": actor},
                )
            )
            return next_epoch

    def patch(
        self,
        cell_id: str,
        expected_epoch: int,
        operation_key: str,
        **fields: Any,
    ) -> dict[str, Any]:
        allowed = {
            "state",
            "airflow_dag_id",
            "airflow_run_id",
            "map_index",
            "factory_generation",
            "policy_digest",
            "base_sha",
            "observed_target_sha",
            "compute",
            "cleanup",
        }
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"unsupported cell fields: {sorted(unknown)}")
        with self.lock:
            row = self.get(cell_id)
            if row["epoch"] != expected_epoch:
                raise StaleEpoch(
                    f"{cell_id}: expected {expected_epoch}, current {row['epoch']}"
                )
            encoded: dict[str, Any] = {}
            for key, value in fields.items():
                encoded[key + "_json" if key in {"compute", "cleanup"} else key] = (
                    json.dumps(value, sort_keys=True, separators=(",", ":"))
                    if key in {"compute", "cleanup"}
                    else value
                )
            now = time.time()
            with self.db:
                self._append(Mutation(cell_id, expected_epoch, operation_key, "patch", fields))
                if encoded:
                    set_clause = ",".join(f"{k}=?" for k in encoded)
                    values = list(encoded.values()) + [now, cell_id, expected_epoch]
                    cur = self.db.execute(
                        f"UPDATE cells SET {set_clause},updated_at=? WHERE cell_id=? AND epoch=?",
                        values,
                    )
                    if cur.rowcount != 1:
                        raise StaleEpoch(cell_id)
            return self.get(cell_id)

    def record(self, mutation: Mutation) -> None:
        with self.lock:
            row = self.get(mutation.cell_id)
            if row["epoch"] != mutation.epoch:
                raise StaleEpoch(mutation.cell_id)
            with self.db:
                self._append(mutation)

    def _append(self, mutation: Mutation) -> None:
        try:
            self.db.execute(
                "INSERT INTO cell_events(cell_id,epoch,operation_key,kind,payload_json,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (
                    mutation.cell_id,
                    mutation.epoch,
                    mutation.operation_key,
                    mutation.kind,
                    json.dumps(mutation.payload, sort_keys=True, separators=(",", ":")),
                    time.time(),
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise DuplicateOperation(mutation.operation_key) from exc

    @staticmethod
    def _decode(row: sqlite3.Row) -> dict[str, Any]:
        out = dict(row)
        out["compute"] = json.loads(out.pop("compute_json")) if out.get("compute_json") else None
        out["cleanup"] = json.loads(out.pop("cleanup_json")) if out.get("cleanup_json") else None
        return out


def operation_key(kind: str, *parts: str) -> str:
    raw = "\0".join((kind, *parts)).encode()
    return f"{kind}:" + hashlib.sha256(raw).hexdigest()[:24]
