"""Canonical core capability path for externally visible software-factory effects.

This module deliberately does not schedule work. Apache Airflow remains the only lifecycle
scheduler. The purpose of this facade is to make the core cross-cutting invariant executable:

    one Factory Cell + one epoch + one operation key

must be carried consistently through Airflow ownership, authorization, policy fencing, durable
idempotency, Cell history, and retained evidence.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from swfactory.authority import MutationAuthority, ResourceKind
from swfactory.cells import (
    CellBusy,
    CellIdentity,
    CellStore,
    DuplicateOperation,
    Mutation,
    StaleEpoch,
    operation_key,
)
from swfactory.idempotency import MutationOutcome, OperationJournal, OperationRef, RetryBudget
from swfactory.liquid_security_runtime import Capability, SecurityContext
from swfactory.liquid_security_runtime import authorize as authorize_capability
from swfactory.liquid_workgraph_runtime import WorkNode, WorkPlan, compile_workgraph
from swfactory.security_contract import CanonicalPolicy, MutationEnvelope, redact
from swfactory.trust_evidence import TrustedEvidence, validate_mutation_policy


class CoreCapabilityError(RuntimeError):
    """The canonical core capability contract was violated."""


@dataclass(frozen=True)
class AirflowBinding:
    cell_id: str
    epoch: int
    dag_id: str
    run_id: str
    map_index: int | None = None

    def validate(self) -> None:
        if not self.cell_id.startswith("cell_"):
            raise ValueError("Airflow binding requires a Factory Cell id")
        if self.epoch < 1:
            raise ValueError("Airflow binding epoch must be positive")
        if not self.dag_id.strip() or not self.run_id.strip():
            raise ValueError("Airflow binding requires dag_id and run_id")
        if self.map_index is not None and self.map_index < 0:
            raise ValueError("Airflow map_index cannot be negative")


@dataclass(frozen=True)
class CoreMutationRequest:
    airflow: AirflowBinding
    security: SecurityContext
    resource: ResourceKind
    capability: Capability
    actor: str
    kind: str
    expected_policy_digest: str
    target_tenant: str
    parts: tuple[str, ...] = ()
    secret_scope: str | None = None
    replay_safe: bool = False

    def validate(self) -> None:
        self.airflow.validate()
        self.security.validate()
        if self.security.cell_id != self.airflow.cell_id or self.security.epoch != self.airflow.epoch:
            raise CoreCapabilityError("security and Airflow identities must name the same Cell epoch")
        if not self.actor.strip():
            raise ValueError("mutation actor is required")
        if not self.kind.strip() or len(self.kind) > 128:
            raise ValueError("mutation kind must be nonempty and bounded")
        if not self.expected_policy_digest.startswith("policy:"):
            raise ValueError("mutation requires a canonical expected policy digest")
        if not self.target_tenant.strip():
            raise ValueError("target tenant is required")


@dataclass(frozen=True)
class CoreMutationResult:
    operation_key: str
    result: Any
    evidence_digest: str
    replayed: bool


class CoreCapabilityRuntime:
    """Join the canonical core contracts without introducing a second scheduler or store."""

    def __init__(self, *, cells: CellStore, journal: OperationJournal, evidence: TrustedEvidence) -> None:
        self.cells = cells
        self.journal = journal
        self.evidence = evidence

    def activate(
        self,
        *,
        repo: str,
        target: str,
        issue: str,
        actor: str,
        policy: CanonicalPolicy,
    ) -> dict[str, Any]:
        """Activate or deterministically recover the same already-active Cell."""
        canonical = policy.canonical_dict()
        if canonical["repo"] != repo.strip() or canonical["target"] != target.strip():
            raise CoreCapabilityError("policy repo/target must match the Factory Cell identity")

        identity = CellIdentity(repo=repo, target=target, issue=issue)
        digest = policy.digest()
        try:
            cell = self.cells.activate(identity, actor)
        except CellBusy:
            cell = self.cells.get(identity.stable_id())
            active_digest = cell.get("policy_digest")
            if active_digest not in {None, digest}:
                raise CoreCapabilityError("active Cell already carries a different policy digest") from None

        key = operation_key("policy_activation", cell["cell_id"], str(cell["epoch"]), digest)
        try:
            cell = self.cells.patch(cell["cell_id"], int(cell["epoch"]), key, policy_digest=digest)
        except DuplicateOperation:
            cell = self.cells.get(cell["cell_id"])
            if cell.get("policy_digest") != digest:
                raise CoreCapabilityError("policy activation key was reused for a different policy") from None
        self._policy_evidence_once(cell=cell, policy=policy, actor=actor)
        return cell

    def bind_airflow(self, binding: AirflowBinding) -> dict[str, Any]:
        """Bind the authoritative Airflow run to the durable Cell epoch exactly once."""
        binding.validate()
        cell = self._current_cell(binding.cell_id, binding.epoch)
        key = operation_key(
            "airflow_bind",
            binding.cell_id,
            str(binding.epoch),
            binding.dag_id,
            binding.run_id,
            str(binding.map_index if binding.map_index is not None else -1),
        )
        MutationAuthority(
            resource=ResourceKind.AIRFLOW_RUN,
            actor="airflow",
            cell_id=binding.cell_id,
            epoch=binding.epoch,
            operation_key=key,
        ).validate(current_epoch=int(cell["epoch"]))

        existing = (cell.get("airflow_dag_id"), cell.get("airflow_run_id"), cell.get("map_index"))
        desired = (binding.dag_id, binding.run_id, binding.map_index)
        if existing[1] is not None:
            if existing != desired:
                raise CoreCapabilityError(f"Cell is already bound to a different Airflow run: {existing!r}")
            self._airflow_evidence_once(cell=cell, binding=binding, operation_key_value=key)
            return cell

        cell = self.cells.patch(
            binding.cell_id,
            binding.epoch,
            key,
            airflow_dag_id=binding.dag_id,
            airflow_run_id=binding.run_id,
            map_index=binding.map_index,
            state="running",
        )
        self._airflow_evidence_once(cell=cell, binding=binding, operation_key_value=key)
        return cell

    def compile_work(self, nodes: Iterable[WorkNode], *, max_width: int = 7) -> WorkPlan:
        """Compile bounded issue-local work. The returned graph is not a lifecycle scheduler."""
        return compile_workgraph(nodes, max_width=max_width)

    def execute_external(
        self,
        request: CoreMutationRequest,
        fn: Callable[[], Any],
        *,
        reconcile: Callable[[], MutationOutcome] | None = None,
        budget: RetryBudget | None = None,
    ) -> CoreMutationResult:
        """Execute one fenced external effect and converge all durable records on one identity.

        The callable must return a durable receipt, not a credential. Results are redacted before
        they enter the operation journal, so secret-shaped values cannot become replay material.
        """
        request.validate()
        cell = self._current_cell(request.airflow.cell_id, request.airflow.epoch)
        self._require_airflow_owner(cell, request.airflow)
        current_policy_digest = self._require_policy(cell)

        key = operation_key(
            request.kind,
            request.airflow.cell_id,
            str(request.airflow.epoch),
            *request.parts,
        )
        envelope = MutationEnvelope(
            cell_id=request.airflow.cell_id,
            epoch=request.airflow.epoch,
            operation_key=key,
            policy_digest=request.expected_policy_digest,
            trace_id=self._trace_id(request.airflow.cell_id, request.airflow.epoch),
            actor=request.actor,
        )
        validate_mutation_policy(envelope, current_policy_digest)

        if not authorize_capability(
            request.security,
            capability=request.capability,
            tenant=request.target_tenant,
            secret_scope=request.secret_scope,
        ):
            raise PermissionError(
                f"role {request.security.role!r} lacks {request.capability.value!r} "
                f"for tenant {request.target_tenant!r}"
            )

        MutationAuthority(
            resource=request.resource,
            actor=request.actor,
            cell_id=request.airflow.cell_id,
            epoch=request.airflow.epoch,
            operation_key=key,
        ).validate(current_epoch=int(cell["epoch"]))

        ref = OperationRef(request.airflow.cell_id, request.airflow.epoch, request.kind, key)
        replayed = self._already_committed(key)

        def durable_call() -> Any:
            return redact(fn())

        wrapped_reconcile: Callable[[], MutationOutcome] | None = None
        if reconcile is not None:

            def reconcile_redacted() -> MutationOutcome:
                outcome = reconcile()
                return MutationOutcome(
                    status=outcome.status,
                    result=redact(outcome.result),
                    evidence=redact(outcome.evidence),
                    detail=outcome.detail,
                )

            wrapped_reconcile = reconcile_redacted

        result = self.journal.execute(
            ref,
            durable_call,
            replay_safe=request.replay_safe,
            reconcile=wrapped_reconcile,
            budget=budget,
        )
        receipt_digest = self._result_digest(result)
        cell_payload = {
            "resource": request.resource.value,
            "capability": request.capability.value,
            "status": "committed",
            "result_digest": receipt_digest,
        }
        self._cell_effect_once(
            Mutation(
                cell_id=request.airflow.cell_id,
                epoch=request.airflow.epoch,
                operation_key=key,
                kind="external_mutation",
                payload=cell_payload,
            )
        )
        evidence = self._mutation_evidence_once(
            envelope,
            kind="external_mutation",
            payload={**cell_payload, "result": result},
        )
        return CoreMutationResult(
            operation_key=key,
            result=result,
            evidence_digest=str(evidence["digest"]),
            replayed=replayed,
        )

    def inspect(self, cell_id: str) -> dict[str, Any]:
        """Return the operator-visible core state without adding business logic."""
        cell = self.cells.get(cell_id)
        unresolved = [
            row
            for row in self.journal.unresolved(limit=1000)
            if row.get("cell_id") == cell_id
        ]
        verified, evidence_tail = self.evidence.verify(cell_id)
        return {
            "schema_version": 1,
            "cell": cell,
            "history": self.cells.history(cell_id),
            "unresolved_operations": unresolved,
            "evidence": {"verified": verified, "tail_digest": evidence_tail},
        }

    def _current_cell(self, cell_id: str, epoch: int) -> dict[str, Any]:
        cell = self.cells.get(cell_id)
        if int(cell["epoch"]) != epoch:
            raise StaleEpoch(f"{cell_id}: expected epoch {epoch}, current {cell['epoch']}")
        return cell

    @staticmethod
    def _require_airflow_owner(cell: dict[str, Any], binding: AirflowBinding) -> None:
        observed = (cell.get("airflow_dag_id"), cell.get("airflow_run_id"), cell.get("map_index"))
        expected = (binding.dag_id, binding.run_id, binding.map_index)
        if observed != expected:
            raise CoreCapabilityError(
                f"external mutation is not owned by the bound Airflow run: {observed!r} != {expected!r}"
            )

    @staticmethod
    def _require_policy(cell: dict[str, Any]) -> str:
        digest = str(cell.get("policy_digest") or "")
        if not digest.startswith("policy:"):
            raise CoreCapabilityError("Cell has no canonical policy digest")
        return digest

    def _already_committed(self, key: str) -> bool:
        try:
            return self.journal.get(key)["state"] == "committed"
        except KeyError:
            return False

    def _cell_effect_once(self, mutation: Mutation) -> None:
        try:
            self.cells.record(mutation)
            return
        except DuplicateOperation:
            pass
        for event in reversed(self.cells.history(mutation.cell_id)):
            if event["operation_key"] != mutation.operation_key:
                continue
            if event["kind"] != mutation.kind or event["payload"] != mutation.payload:
                raise CoreCapabilityError("operation key already names a different Cell effect")
            return
        raise CoreCapabilityError("duplicate Cell operation was reported but no matching event exists")

    def _policy_evidence_once(
        self,
        *,
        cell: dict[str, Any],
        policy: CanonicalPolicy,
        actor: str,
    ) -> dict[str, Any]:
        digest = policy.digest()
        for record in reversed(self.evidence.writer.read(cell["cell_id"])):
            if record.get("kind") != "policy_activation":
                continue
            if int(record.get("epoch", -1)) != int(cell["epoch"]):
                continue
            payload = record.get("payload") or {}
            if payload.get("policy_digest") == digest:
                return record
        return self.evidence.policy_activation(
            cell_id=cell["cell_id"],
            epoch=int(cell["epoch"]),
            policy=policy,
            actor=actor,
        )

    def _airflow_evidence_once(
        self,
        *,
        cell: dict[str, Any],
        binding: AirflowBinding,
        operation_key_value: str,
    ) -> dict[str, Any]:
        policy_digest = self._require_policy(cell)
        envelope = MutationEnvelope(
            cell_id=binding.cell_id,
            epoch=binding.epoch,
            operation_key=operation_key_value,
            policy_digest=policy_digest,
            trace_id=self._trace_id(binding.cell_id, binding.epoch),
            actor="airflow",
        )
        return self._mutation_evidence_once(
            envelope,
            kind="airflow_bound",
            payload={
                "dag_id": binding.dag_id,
                "run_id": binding.run_id,
                "map_index": binding.map_index,
            },
        )

    def _mutation_evidence_once(
        self,
        envelope: MutationEnvelope,
        *,
        kind: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        for record in reversed(self.evidence.writer.read(envelope.cell_id)):
            record_payload = record.get("payload") or {}
            if record_payload.get("operation_key") != envelope.operation_key:
                continue
            if (
                int(record.get("epoch", -1)) != envelope.epoch
                or record.get("policy_digest") != envelope.policy_digest
            ):
                raise CoreCapabilityError(
                    "operation key already has evidence for a different Cell epoch or policy"
                )
            if record.get("kind") != kind:
                raise CoreCapabilityError(
                    "operation key already has evidence for a different mutation kind"
                )
            return record
        return self.evidence.mutation(envelope, kind=kind, payload=payload)

    @staticmethod
    def _trace_id(cell_id: str, epoch: int) -> str:
        return hashlib.sha256(f"trace\0{cell_id}\0{epoch}".encode()).hexdigest()[:32]

    @staticmethod
    def _result_digest(result: Any) -> str:
        payload = json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return "result:" + hashlib.sha256(payload.encode()).hexdigest()
