"""Canonical core capability path for externally visible software-factory effects.

This module deliberately does not schedule work. Apache Airflow remains the only lifecycle
scheduler. The purpose of this facade is to make the core cross-cutting invariant executable:

    one Factory Cell + one epoch + one operation key + one immutable intent

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
    TERMINAL_STATES,
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


class CellNotActive(CoreCapabilityError):
    """A terminal Cell was asked to create a new external effect."""


# Resources a terminal Cell may still act on. Cancelling work is worse than useless if it also
# strands the sandbox and the branches that work created, so cleanup and evidence stay reachable
# after the Cell is done. Everything else -- publication above all -- is fenced: a current epoch
# proves the caller is not stale, and that is a different question from whether the work it belongs
# to is still authorised to reach the outside world.
TERMINAL_EFFECT_RESOURCES = frozenset({ResourceKind.CLEANUP, ResourceKind.EVIDENCE})


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
    external_operation_key: str | None = None
    intent_digest: str | None = None

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
        if self.external_operation_key is not None and (
            not self.external_operation_key.strip() or len(self.external_operation_key) > 256
        ):
            raise ValueError("external operation key must be nonempty and bounded")
        if self.intent_digest is not None:
            _validate_sha256_digest(self.intent_digest)


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
        self._refuse_terminal_effect(cell, ResourceKind.AIRFLOW_RUN)
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
        observe_before_first_attempt: bool = False,
    ) -> CoreMutationResult:
        """Execute one fenced external effect and converge all durable records on one identity.

        The callable must return a durable receipt, not a credential. Results are redacted before
        they enter the operation journal, so secret-shaped values cannot become replay material.
        If the caller supplies a logical operation key, the journal binds it to one immutable
        ``intent_digest``; changing the requested content behind the same key fails closed.
        """
        request.validate()
        cell = self._current_cell(request.airflow.cell_id, request.airflow.epoch)
        self._require_airflow_owner(cell, request.airflow)
        current_policy_digest = self._require_policy(cell)

        key = request.external_operation_key or operation_key(
            request.kind,
            request.airflow.cell_id,
            str(request.airflow.epoch),
            *request.parts,
        )
        intent_digest = request.intent_digest or self._request_digest(request, key)
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
        # Ordered deliberately: reading back a receipt the journal already holds is observation, not
        # a new effect, and it must keep working on a terminal Cell. Refusing it would break the
        # idempotent replay that stops a redelivered Airflow task from opening a second pull
        # request. Only a call that would actually reach the provider is fenced.
        if not replayed:
            self._refuse_terminal_effect(cell, request.resource)

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
            intent_digest=intent_digest,
            # Set by the restore contract: a Cell recovered from a snapshot must observe the remote
            # before its first attempt, because "no journal row" is not evidence of "no effect".
            observe_before_first_attempt=observe_before_first_attempt,
        )
        receipt_digest = self._result_digest(result)
        cell_payload = {
            "resource": request.resource.value,
            "capability": request.capability.value,
            "status": "committed",
            "intent_digest": intent_digest,
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
        unresolved = [row for row in self.journal.unresolved(limit=1000) if row.get("cell_id") == cell_id]
        verified, evidence_tail = self.evidence.verify(cell_id)
        return {
            "schema_version": 1,
            "cell": cell,
            "history": self.cells.history(cell_id),
            "unresolved_operations": unresolved,
            "evidence": {"verified": verified, "tail_digest": evidence_tail},
        }

    def _current_cell(self, cell_id: str, epoch: int) -> dict[str, Any]:
        """Resolve the Cell and prove the caller's epoch is still the current one.

        Epoch is the only question answered here. Lifecycle state is checked separately, at the
        point where a new effect would actually be created, because replaying a receipt the journal
        has already committed must stay possible on a terminal Cell.
        """
        cell = self.cells.get(cell_id)
        if int(cell["epoch"]) != epoch:
            raise StaleEpoch(f"{cell_id}: expected epoch {epoch}, current {cell['epoch']}")
        return cell

    @staticmethod
    def _refuse_terminal_effect(cell: dict[str, Any], resource: ResourceKind) -> None:
        """Refuse a NEW external effect from a Cell whose lifecycle has already concluded.

        A real runtime accepted a Cell already in ``cancelled`` and ran its publication callback,
        because identity, binding and policy all still checked out. They would: none of them asks
        whether the work is still live. Cancelled and rejected work must not publish, and a Cell
        that already succeeded must not publish a second time.
        """
        state = str(cell.get("state") or "")
        if state in TERMINAL_STATES and resource not in TERMINAL_EFFECT_RESOURCES:
            raise CellNotActive(
                f"{cell['cell_id']} is {state}: {resource.value} may not create a new external "
                f"effect (permitted on a terminal Cell: "
                f"{', '.join(sorted(r.value for r in TERMINAL_EFFECT_RESOURCES))})"
            )

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
            payload={"dag_id": binding.dag_id, "run_id": binding.run_id, "map_index": binding.map_index},
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
            if int(record.get("epoch", -1)) != envelope.epoch or record.get("policy_digest") != envelope.policy_digest:
                raise CoreCapabilityError("operation key already has evidence for a different Cell epoch or policy")
            if record.get("kind") != kind:
                raise CoreCapabilityError("operation key already has evidence for a different mutation kind")
            return record
        return self.evidence.mutation(envelope, kind=kind, payload=payload)

    @staticmethod
    def _trace_id(cell_id: str, epoch: int) -> str:
        return hashlib.sha256(f"trace\0{cell_id}\0{epoch}".encode()).hexdigest()[:32]

    @staticmethod
    def _result_digest(result: Any) -> str:
        payload = json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return "result:" + hashlib.sha256(payload.encode()).hexdigest()

    @staticmethod
    def _request_digest(request: CoreMutationRequest, key: str) -> str:
        payload = {
            "cell_id": request.airflow.cell_id,
            "epoch": request.airflow.epoch,
            "dag_id": request.airflow.dag_id,
            "run_id": request.airflow.run_id,
            "map_index": request.airflow.map_index,
            "resource": request.resource.value,
            "capability": request.capability.value,
            "actor": request.actor,
            "kind": request.kind,
            "policy_digest": request.expected_policy_digest,
            "target_tenant": request.target_tenant,
            "parts": list(request.parts),
            "operation_key": key,
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        return "sha256:" + hashlib.sha256(raw).hexdigest()


def _validate_sha256_digest(value: str) -> None:
    if not value.startswith("sha256:"):
        raise ValueError("intent digest must use sha256:<hex>")
    digest = value.removeprefix("sha256:")
    if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
        raise ValueError("intent digest must be a lowercase SHA-256 digest")
