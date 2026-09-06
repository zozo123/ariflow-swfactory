"""Level-1 seam joining trust-zone policy, mutation identity and durable evidence.

All lifecycle/effect evidence should enter through this facade after fan-in.  It validates that the
Factory Cell epoch/policy identity on the mutation matches the evidence identity, applies the shared
redaction contract, and then delegates to the append-only evidence writer.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from swfactory.lifecycle_evidence import EvidenceWriter, TraceContext
from swfactory.security_contract import CanonicalPolicy, MutationEnvelope, redact


class TrustedEvidence:
    def __init__(self, root: Path):
        self.writer = EvidenceWriter(root)

    def append(
        self,
        *,
        cell_id: str,
        epoch: int,
        kind: str,
        payload: Mapping[str, Any],
        policy_digest: str | None,
        trace: TraceContext | None = None,
    ) -> dict[str, Any]:
        return self.writer.append(
            cell_id=cell_id,
            epoch=epoch,
            kind=kind,
            payload=redact(dict(payload)),
            policy_digest=policy_digest,
            trace=trace,
        )

    def mutation(
        self,
        envelope: MutationEnvelope,
        *,
        kind: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        envelope.validate()
        trace = TraceContext(envelope.trace_id, _span_id(envelope.trace_id, envelope.operation_key))
        return self.append(
            cell_id=envelope.cell_id,
            epoch=envelope.epoch,
            kind=kind,
            payload={
                "operation_key": envelope.operation_key,
                "actor": envelope.actor,
                "mutation": payload,
            },
            policy_digest=envelope.policy_digest,
            trace=trace,
        )

    def policy_activation(
        self,
        *,
        cell_id: str,
        epoch: int,
        policy: CanonicalPolicy,
        actor: str,
    ) -> dict[str, Any]:
        digest = policy.digest()
        return self.append(
            cell_id=cell_id,
            epoch=epoch,
            kind="policy_activation",
            payload={"actor": actor, "policy": policy.canonical_dict(), "policy_digest": digest},
            policy_digest=digest,
            trace=TraceContext.for_cell(cell_id, epoch, "policy_activation", digest),
        )

    def verify(self, cell_id: str) -> tuple[bool, str]:
        return self.writer.verify(cell_id)

    def checkpoint(self, cell_id: str) -> dict[str, Any]:
        return self.writer.checkpoint(cell_id)


def validate_mutation_policy(envelope: MutationEnvelope, current_policy_digest: str) -> None:
    envelope.validate()
    if envelope.policy_digest != current_policy_digest:
        raise PermissionError(
            "mutation policy digest is stale; refuse external side effect until the cell "
            "is reactivated"
        )


def _span_id(trace_id: str, operation_key: str) -> str:
    import hashlib

    return hashlib.sha256(f"span\0{trace_id}\0mutation\0{operation_key}".encode()).hexdigest()[:16]
