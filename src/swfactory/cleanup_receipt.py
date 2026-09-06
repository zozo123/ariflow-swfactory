"""Versioned cleanup receipts and bounded repair leases.

Cleanup is a convergent external mutation, not a best-effort `close()` side effect.  Providers report
what they observed; callers decide whether that observation is authoritative for the current cell
epoch.
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

CleanupStatus = Literal["converged", "already_absent", "refused", "ambiguous", "failed"]


@dataclass(frozen=True)
class CleanupReceipt:
    schema_version: int
    cell_id: str
    epoch: int
    operation_key: str
    provider: str
    resource_id: str
    status: CleanupStatus
    requested_at: float
    observed_at: float
    attempts: int = 1
    detail: str = ""

    @classmethod
    def build(
        cls,
        *,
        cell_id: str,
        epoch: int,
        operation_key: str,
        provider: str,
        resource_id: str,
        status: CleanupStatus,
        requested_at: float,
        attempts: int = 1,
        detail: str = "",
    ) -> "CleanupReceipt":
        if epoch < 1 or attempts < 1:
            raise ValueError("cleanup epoch/attempts must be positive")
        return cls(
            schema_version=1,
            cell_id=cell_id,
            epoch=epoch,
            operation_key=operation_key,
            provider=provider,
            resource_id=resource_id,
            status=status,
            requested_at=requested_at,
            observed_at=time.time(),
            attempts=attempts,
            detail=detail[:2000],
        )

    @property
    def converged(self) -> bool:
        return self.status in {"converged", "already_absent"}

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class RepairLease:
    key: str
    owner: str
    epoch: int
    expires_at: float


class RepairLeaseStore:
    """SQLite CAS leases for reconciliation workers.

    The lease only chooses one repair worker.  It never grants Factory Cell mutation authority;
    callers must still validate the current cell epoch before recording a repair.
    """

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30, isolation_level="IMMEDIATE", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS repair_leases(
                lease_key TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                lease_epoch INTEGER NOT NULL,
                expires_at REAL NOT NULL,
                metadata_json TEXT NOT NULL
            )"""
        )

    def acquire(
        self,
        key: str,
        owner: str,
        *,
        ttl_s: float = 30.0,
        metadata: dict | None = None,
    ) -> RepairLease | None:
        if ttl_s <= 0:
            raise ValueError("ttl_s must be positive")
        now = time.time()
        expires = now + ttl_s
        meta = json.dumps(metadata or {}, sort_keys=True, separators=(",", ":"))
        with self.db:
            row = self.db.execute(
                "SELECT owner,lease_epoch,expires_at FROM repair_leases WHERE lease_key=?",
                (key,),
            ).fetchone()
            if row is None:
                self.db.execute(
                    "INSERT INTO repair_leases VALUES(?,?,?,?,?)",
                    (key, owner, 1, expires, meta),
                )
                return RepairLease(key, owner, 1, expires)
            if float(row["expires_at"]) > now and row["owner"] != owner:
                return None
            epoch = int(row["lease_epoch"]) + 1
            self.db.execute(
                "UPDATE repair_leases SET owner=?,lease_epoch=?,expires_at=?,metadata_json=? "
                "WHERE lease_key=?",
                (owner, epoch, expires, meta, key),
            )
            return RepairLease(key, owner, epoch, expires)

    def renew(self, lease: RepairLease, *, ttl_s: float = 30.0) -> RepairLease | None:
        now = time.time()
        expires = now + ttl_s
        with self.db:
            cur = self.db.execute(
                "UPDATE repair_leases SET expires_at=? WHERE lease_key=? AND owner=? AND lease_epoch=?",
                (expires, lease.key, lease.owner, lease.epoch),
            )
        if cur.rowcount != 1:
            return None
        return RepairLease(lease.key, lease.owner, lease.epoch, expires)

    def release(self, lease: RepairLease) -> bool:
        with self.db:
            cur = self.db.execute(
                "DELETE FROM repair_leases WHERE lease_key=? AND owner=? AND lease_epoch=?",
                (lease.key, lease.owner, lease.epoch),
            )
        return cur.rowcount == 1
