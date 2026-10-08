"""Versioned cleanup receipts and bounded repair leases.

Cleanup is a convergent external mutation, not a best-effort `close()` side effect. Providers
report what they observed; callers decide whether that observation is authoritative for the current
cell
epoch.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from swfactory.store_schema import connect_write, ensure_named_schema, guard_before_ddl

if TYPE_CHECKING:
    from swfactory.idempotency import OperationRef

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
    ) -> CleanupReceipt:
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

    @classmethod
    def for_operation(
        cls,
        ref: OperationRef,
        resource_id: str,
        status: CleanupStatus,
        *,
        provider: str,
        requested_at: float,
        detail: str = "",
    ) -> CleanupReceipt:
        """The receipt for the journaled cleanup operation ``ref`` on ``resource_id``."""
        return cls.build(
            cell_id=ref.cell_id,
            epoch=ref.epoch,
            operation_key=ref.key,
            provider=provider,
            resource_id=resource_id,
            status=status,
            requested_at=requested_at,
            detail=detail,
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
        self.db = connect_write(path)
        guard_before_ddl(self.db, "repairs")
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS repair_leases(
                lease_key TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                lease_epoch INTEGER NOT NULL,
                expires_at REAL NOT NULL,
                metadata_json TEXT NOT NULL
            )"""
        )
        # Cleanup debt is authoritative state: a restore that silently drops it leaks provider
        # resources nobody is left accountable for. Version it like the other three stores.
        with self.db:
            ensure_named_schema(self.db, "repairs")

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
                "UPDATE repair_leases SET owner=?,lease_epoch=?,expires_at=?,metadata_json=? WHERE lease_key=?",
                (owner, epoch, expires, meta, key),
            )
            return RepairLease(key, owner, epoch, expires)

    def release(self, lease: RepairLease) -> bool:
        with self.db:
            cur = self.db.execute(
                "DELETE FROM repair_leases WHERE lease_key=? AND owner=? AND lease_epoch=?",
                (lease.key, lease.owner, lease.epoch),
            )
        return cur.rowcount == 1
