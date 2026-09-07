"""Canonical epoch-fenced identity for every external factory mutation.

Airflow remains the lifecycle scheduler.  This module only names side effects so every writer uses
one `(cell_id, epoch, operation_key)` contract and stale epochs fail before an external call.
"""

from __future__ import annotations

from enum import StrEnum

from swfactory.idempotency import OperationRef


class ExternalMutation(StrEnum):
    SANDBOX_ALLOCATE = "sandbox_allocate"
    SANDBOX_RECLAIM = "sandbox_reclaim"
    GITHUB_PUBLISH = "github_publish"
    EVIDENCE_SEAL = "evidence_seal"
    APPROVAL_ANSWER = "approval_answer"
    CLEANUP_RECONCILE = "cleanup_reconcile"
    GENERATION_PROMOTE = "generation_promote"


class StaleMutation(RuntimeError):
    pass


def mutation_ref(kind: ExternalMutation, cell_id: str, epoch: int, *parts: str) -> OperationRef:
    """Build the deterministic identity used by the durable mutation journal."""
    if not isinstance(kind, ExternalMutation):
        raise TypeError("kind must be an ExternalMutation")
    if not isinstance(cell_id, str) or not cell_id.startswith("cell_"):
        raise ValueError("cell_id must be a canonical cell_ identity")
    if not isinstance(epoch, int) or isinstance(epoch, bool) or epoch <= 0:
        raise ValueError("epoch must be a positive integer")
    if not parts or any(not isinstance(part, str) or not part for part in parts):
        raise ValueError("operation identity requires non-empty stable parts")
    return OperationRef.build(cell_id, epoch, kind.value, *parts)


def require_current_epoch(ref: OperationRef, current_epoch: int) -> None:
    """Fence stale and future writers before they reach an external mutation adapter."""
    if not isinstance(current_epoch, int) or isinstance(current_epoch, bool) or current_epoch <= 0:
        raise ValueError("current_epoch must be a positive integer")
    if ref.epoch != current_epoch:
        raise StaleMutation(
            f"{ref.kind} for {ref.cell_id} is fenced: requested epoch {ref.epoch}, current epoch {current_epoch}"
        )


def external_mutation_kinds() -> tuple[ExternalMutation, ...]:
    """Machine-checkable registry; adding a side-effect family requires extending this contract."""
    return tuple(ExternalMutation)
