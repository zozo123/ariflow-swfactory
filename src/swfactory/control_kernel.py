"""Level-1 control-plane seam for durable mutations and admission.

This module deliberately contains no Airflow scheduling logic.  It owns local durable control state
that must survive backend restarts: admission decisions, side-effect intent/results, reconciliation
leases and cleanup receipts.  The final backend fan-in depends on this seam instead of constructing
feature-specific SQLite helpers.
"""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable

from swfactory.admission import Limits, Priority
from swfactory.cleanup_receipt import CleanupReceipt, RepairLeaseStore
from swfactory.durable_admission import AdmissionDecision, DurableAdmission
from swfactory.idempotency import MutationOutcome, OperationJournal, OperationRef, RetryBudget


class ControlKernel:
    def __init__(self, root: Path, *, limits: Limits = Limits()):
        root.mkdir(parents=True, exist_ok=True)
        self.root = root
        self.operations = OperationJournal(root / "operations.sqlite3")
        self.repairs = RepairLeaseStore(root / "repairs.sqlite3")
        self.admission = DurableAdmission(root / "admission.sqlite3", limits)

    def close(self) -> None:
        self.operations.close()
        self.repairs.db.close()
        self.admission.close()

    def submit(
        self,
        *,
        work_id: str,
        repo: str,
        actor: str,
        blueprint: str,
        priority: Priority = Priority.NORMAL,
    ) -> AdmissionDecision:
        return self.admission.submit(
            work_id=work_id,
            repo=repo,
            actor=actor,
            blueprint=blueprint,
            priority=priority,
        )

    def bind_cell(self, work_id: str, cell_id: str, epoch: int) -> None:
        self.admission.bind_cell(work_id, cell_id, epoch)

    def release_for_terminal_cell(
        self,
        work_id: str,
        *,
        cell_id: str,
        epoch: int,
        state: str,
    ) -> list[str]:
        return self.admission.complete(
            work_id,
            cell_id=cell_id,
            epoch=epoch,
            state=state,
        )

    def mutate(
        self,
        ref: OperationRef,
        fn: Callable[[], Any],
        *,
        replay_safe: bool = False,
        reconcile: Callable[[], MutationOutcome] | None = None,
        budget: RetryBudget | None = None,
    ) -> Any:
        return self.operations.execute(
            ref,
            fn,
            replay_safe=replay_safe,
            reconcile=reconcile,
            budget=budget,
        )

    def record_cleanup(self, receipt: CleanupReceipt) -> dict[str, Any]:
        """Return the canonical receipt document; persistence is the operation/evidence layer's job."""
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
