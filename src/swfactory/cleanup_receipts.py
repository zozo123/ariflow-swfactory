"""Versioned, provider-neutral cleanup receipts.

Cleanup is a convergence protocol, not a fire-and-forget call.  A receipt says what the factory
requested, what the provider actually observed, and whether the current cell epoch may treat the
cleanup as complete.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

CleanupStatus = Literal["converged", "already_absent", "refused", "ambiguous", "failed"]


@dataclass(frozen=True)
class CleanupReceipt:
    cell_id: str
    epoch: int
    operation_key: str
    resource_kind: str
    provider: str
    resource_id: str
    status: CleanupStatus
    requested_state: str = "absent"
    observed_state: str | None = None
    attempts: int = 1
    detail: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)
    schema_version: int = 1
    observed_at: float = field(default_factory=time.time)

    @property
    def authoritative(self) -> bool:
        return self.status in {"converged", "already_absent"}

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def digest(self) -> str:
        payload = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False)
        return hashlib.sha256(payload.encode()).hexdigest()


def cleanup_operation_key(cell_id: str, epoch: int, resource_kind: str, resource_id: str) -> str:
    raw = "\0".join((cell_id, str(epoch), resource_kind, resource_id)).encode()
    return "cleanup:" + hashlib.sha256(raw).hexdigest()[:24]


def converged_receipt(
    *,
    cell_id: str,
    epoch: int,
    resource_kind: str,
    provider: str,
    resource_id: str,
    already_absent: bool = False,
    attempts: int = 1,
    evidence: dict[str, Any] | None = None,
) -> CleanupReceipt:
    return CleanupReceipt(
        cell_id=cell_id,
        epoch=epoch,
        operation_key=cleanup_operation_key(cell_id, epoch, resource_kind, resource_id),
        resource_kind=resource_kind,
        provider=provider,
        resource_id=resource_id,
        status="already_absent" if already_absent else "converged",
        observed_state="absent",
        attempts=attempts,
        evidence=evidence or {},
    )
