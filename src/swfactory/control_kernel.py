"""Durable backend control seam for admission, mutations, reconciliation and cleanup.

This module deliberately contains no Airflow scheduling logic. Apache Airflow remains the only
lifecycle scheduler. ``ControlKernel`` owns local durable control state and, when given the
Factory's Cell/evidence stores, exposes exactly one managed external-mutation path through
:class:`CoreCapabilityRuntime`.

Raw ``mutate`` remains for control effects that necessarily happen before an Airflow run can be
bound to a Cell (for example deterministic Airflow admission/dispatch). Managed provider/GitHub
writes after binding must use ``mutate_core``.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from swfactory.admission import Limits, Priority
from swfactory.cells import CellStore
from swfactory.cleanup_receipt import CleanupReceipt, RepairLeaseStore
from swfactory.core_capabilities import CoreCapabilityRuntime, CoreMutationRequest, CoreMutationResult
from swfactory.durable_admission import AdmissionDecision, DispatchIntent, DurableAdmission, MemberSpec
from swfactory.idempotency import MutationOutcome, OperationJournal, OperationRef, RetryBudget
from swfactory.trust_evidence import TrustedEvidence


class ControlKernel:
    def __init__(
        self,
        root: Path,
        *,
        limits: Limits | None = None,
        cells: CellStore | None = None,
        evidence: TrustedEvidence | None = None,
    ):
        limits = limits or Limits()
        root.mkdir(parents=True, exist_ok=True)
        self.root = root
        self.operations = OperationJournal(root / "operations.sqlite3")
        self.repairs = RepairLeaseStore(root / "repairs.sqlite3")
        self.admission = DurableAdmission(root / "admission.sqlite3", limits)
        if (cells is None) != (evidence is None):
            raise ValueError("cells and evidence must be supplied together")
        self.core = (
            CoreCapabilityRuntime(cells=cells, journal=self.operations, evidence=evidence)
            if cells is not None and evidence is not None
            else None
        )

    def close(self) -> None:
        self.operations.close()
        self.repairs.db.close()
        self.admission.close()

    def submit(
        self,
        *,
        work_id: str,
        actor: str,
        blueprint: str,
        order: dict[str, Any],
        members: Sequence[MemberSpec],
        priority: Priority = Priority.NORMAL,
    ) -> AdmissionDecision:
        return self.admission.submit(
            work_id=work_id,
            actor=actor,
            blueprint=blueprint,
            order=order,
            members=members,
            priority=priority,
        )

    def claim_dispatch(self, work_id: str) -> DispatchIntent | None:
        return self.admission.claim_dispatch(work_id)

    def pending_dispatch(self, *, limit: int = 32) -> list[str]:
        return self.admission.pending_dispatch(limit=limit)

    def cancel_reservation(self, work_id: str, *, reason: str, state: str = "cancelled") -> list[str]:
        """Close a reservation whose work order will never be delivered, and release its units."""
        return self.admission.cancel(work_id, reason=reason, state=state)

    def release_for_terminal_cell(
        self,
        work_id: str,
        *,
        cell_id: str,
        epoch: int,
        state: str,
    ) -> list[str]:
        return self.admission.complete(work_id, cell_id=cell_id, epoch=epoch, state=state)

    def release_cell(self, cell_id: str, *, epoch: int, state: str) -> list[str]:
        """Release every membership held against this exact cell epoch.

        One terminal Cell releases one unit.  A submission that fanned out to several Cells keeps
        its reservation until its last member is terminal, because releasing on the first one is
        what used to strand the siblings still running.
        """
        admitted: list[str] = []
        for work_id in self.admission.members_for_cell(cell_id, epoch):
            admitted.extend(
                self.admission.complete(
                    work_id,
                    cell_id=cell_id,
                    epoch=epoch,
                    state=state,
                )
            )
        return admitted

    def mutate(
        self,
        ref: OperationRef,
        fn: Callable[[], Any],
        *,
        replay_safe: bool = False,
        reconcile: Callable[[], MutationOutcome] | None = None,
        budget: RetryBudget | None = None,
        intent_digest: str | None = None,
    ) -> Any:
        """Journal a pre-binding/control effect.

        Managed external effects after Airflow binding use :meth:`mutate_core` so Cell authority,
        policy, capability, operation identity and evidence cannot drift apart.
        """
        return self.operations.execute(
            ref,
            fn,
            replay_safe=replay_safe,
            reconcile=reconcile,
            budget=budget,
            intent_digest=intent_digest,
        )

    def mutate_core(
        self,
        request: CoreMutationRequest,
        fn: Callable[[], Any],
        *,
        reconcile: Callable[[], MutationOutcome] | None = None,
        budget: RetryBudget | None = None,
    ) -> CoreMutationResult:
        if self.core is None:
            raise RuntimeError("canonical mutation runtime is not configured")
        return self.core.execute_external(request, fn, reconcile=reconcile, budget=budget)

    def inspect_core(self, cell_id: str) -> dict[str, Any]:
        if self.core is None:
            raise RuntimeError("canonical mutation runtime is not configured")
        return self.core.inspect(cell_id)

    def record_cleanup(self, receipt: CleanupReceipt) -> dict[str, Any]:
        """Return the canonical receipt document; persistence belongs to operation/evidence."""
        return receipt.to_dict()

    def snapshot(self, *, limit: int = 100) -> dict[str, Any]:
        unresolved = self.operations.unresolved(limit=limit)
        queue = self.admission.snapshot(limit=limit)
        return {
            "schema_version": 1,
            "queue": queue,
            "operations": unresolved,
            "repair_debt": {
                "count": len(unresolved),
                "states": _counts(row.get("state", "unknown") for row in unresolved),
                "kinds": _counts(row.get("kind", "unknown") for row in unresolved),
            },
        }


def _counts(values) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        key = str(value)
        result[key] = result.get(key, 0) + 1
    return dict(sorted(result.items()))
