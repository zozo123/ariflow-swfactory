"""Bounded repair coordination for unresolved factory mutations.

A reconciler lease prevents replicas from doing duplicate *repair work*.  It never grants Factory
Cell mutation authority: every repair still has to present the current cell epoch to the domain
store before changing authoritative state.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from swfactory.idempotency import MutationOutcome, OperationJournal, OperationRef


@dataclass(frozen=True)
class ReconcileLease:
    operation_key: str
    owner: str
    lease_epoch: int
    expires_at: float


class ReconcileLeaseStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, timeout=30, isolation_level="IMMEDIATE", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        with self.db:
            self.db.execute(
                """CREATE TABLE IF NOT EXISTS reconcile_leases(
                    operation_key TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    lease_epoch INTEGER NOT NULL,
                    expires_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )"""
            )

    def close(self) -> None:
        with self.lock:
            self.db.close()

    def acquire(self, operation_key: str, owner: str, *, ttl_s: float = 30.0) -> ReconcileLease | None:
        if not owner.strip():
            raise ValueError("reconciler owner must be nonempty")
        now = time.time()
        expires = now + max(1.0, float(ttl_s))
        with self.lock, self.db:
            row = self.db.execute("SELECT * FROM reconcile_leases WHERE operation_key=?", (operation_key,)).fetchone()
            if row is None:
                self.db.execute(
                    "INSERT INTO reconcile_leases VALUES(?,?,?,?,?)",
                    (operation_key, owner, 1, expires, now),
                )
                return ReconcileLease(operation_key, owner, 1, expires)
            if float(row["expires_at"]) > now and row["owner"] != owner:
                return None
            epoch = int(row["lease_epoch"]) + (0 if row["owner"] == owner else 1)
            self.db.execute(
                """UPDATE reconcile_leases SET owner=?, lease_epoch=?, expires_at=?, updated_at=?
                   WHERE operation_key=?""",
                (owner, epoch, expires, now, operation_key),
            )
            return ReconcileLease(operation_key, owner, epoch, expires)

    def release(self, lease: ReconcileLease) -> bool:
        with self.lock, self.db:
            cur = self.db.execute(
                "DELETE FROM reconcile_leases WHERE operation_key=? AND owner=? AND lease_epoch=?",
                (lease.operation_key, lease.owner, lease.lease_epoch),
            )
            return cur.rowcount == 1


class BoundedReconciler:
    """A small repair runner. Callers supply kind-specific observation functions."""

    def __init__(self, journal: OperationJournal, leases: ReconcileLeaseStore, owner: str):
        self.journal = journal
        self.leases = leases
        self.owner = owner

    def pass_once(
        self,
        observers: dict[str, Callable[[dict[str, Any]], MutationOutcome]],
        *,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        repaired: list[dict[str, Any]] = []
        for row in self.journal.unresolved(limit=limit, due_only=True):
            observer = observers.get(str(row["kind"]))
            if observer is None:
                continue
            lease = self.leases.acquire(str(row["operation_key"]), self.owner)
            if lease is None:
                continue
            try:
                outcome = observer(row)
                ref = OperationRef(
                    str(row["cell_id"]),
                    int(row["epoch"]),
                    str(row["kind"]),
                    str(row["operation_key"]),
                )
                self.journal.mark_observation(ref, outcome)
                if outcome.status == "committed":
                    self.journal.commit(ref, outcome.result)
                repaired.append(
                    {
                        "operation_key": ref.key,
                        "kind": ref.kind,
                        "outcome": outcome.status,
                        "lease_epoch": lease.lease_epoch,
                    }
                )
            finally:
                self.leases.release(lease)
        return repaired
