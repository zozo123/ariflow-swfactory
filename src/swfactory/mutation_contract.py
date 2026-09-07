"""Canonical external-mutation identities shared by property tests and runtime adapters.

Airflow remains the lifecycle scheduler. This module only names side effects that must obey the
Factory Cell fencing contract: `(cell_id, epoch, operation_key)`.
"""

from __future__ import annotations

from enum import StrEnum

from swfactory.cells import Mutation
from swfactory.idempotency import OperationRef


class ExternalMutationKind(StrEnum):
    SANDBOX_ALLOCATE = "sandbox_allocate"
    SANDBOX_RECLAIM = "sandbox_reclaim"
    GITHUB_PUBLISH = "github_publish"
    EVIDENCE_SEAL = "evidence_seal"
    APPROVAL_ANSWER = "approval_answer"
    CLEANUP_RECONCILE = "cleanup_reconcile"
    GENERATION_PROMOTE = "generation_promote"


EXTERNAL_MUTATION_KINDS = tuple(ExternalMutationKind)


def mutation_ref(cell_id: str, epoch: int, kind: ExternalMutationKind, *parts: str) -> OperationRef:
    """Build the deterministic operation identity for one fenced side effect."""
    if not cell_id.startswith("cell_") or len(cell_id) != 29:
        raise ValueError("external mutation requires a canonical Factory Cell id")
    if type(epoch) is not int or epoch < 1:
        raise ValueError("external mutation epoch must be a positive integer")
    if not parts or any(not part for part in parts):
        raise ValueError("external mutation requires non-empty operation-key parts")
    return OperationRef.build(cell_id, epoch, kind.value, *parts)


def cell_mutation(
    cell_id: str,
    epoch: int,
    kind: ExternalMutationKind,
    *parts: str,
    payload: dict[str, object] | None = None,
) -> Mutation:
    """Translate a canonical external mutation identity into CellStore evidence."""
    ref = mutation_ref(cell_id, epoch, kind, *parts)
    return Mutation(
        cell_id=cell_id,
        epoch=epoch,
        operation_key=ref.key,
        kind=kind.value,
        payload=payload or {},
    )
