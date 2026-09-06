"""Level-1 control-plane seam for durable mutations and admission.

This module deliberately contains no Airflow scheduling logic. It owns local durable control state
that must survive backend restarts: admission decisions, side-effect intent/results, reconciliation
leases and cleanup receipts. The final backend fan-in depends on this seam instead of constructing
feature-specific SQLite helpers.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from swfactory.admission import Limits, Priority
from swfactory.cleanup_receipt import CleanupReceipt, RepairLeaseStore
from swfactory.durable_admission import AdmissionDecision, DurableAdmission
from swfactory.idempotency import MutationOutcome, OperationJournal, OperationRef, RetryBudget


class ControlKernel:
    def __init__(self, root: Path, *, limits: Limits | None = None):
        limits = limits or Limits()
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

    def cancel_reservation(self, work_id: str, *, reason: str) -> list[str]:
        """Release an unbound admission reservation after pre-dispatch setup fails.

        Once a reservation is bound to a Factory Cell, only a matching authoritative cell epoch may
        release it. This method therefore refuses to cancel a bound active record and prevents an
        exception in a stale request from freeing someone else's capacity.
        """
        now = time.time()
        with self.admission.db:
            cur = self.admission.db.execute(
                """UPDATE admission_work
                   SET state='cancelled',reason=?,terminal_at=?,updated_at=?
                   WHERE work_id=? AND state IN ('active','queued') AND cell_id IS NULL""",
                (reason[:512], now, now, work_id),
            )
        return self.admission.drain() if cur.rowcount == 1 else []

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

    def release_cell(self, cell_id: str, *, epoch: int, state: str) -> list[str]:
        """Release every admission record authoritatively bound to this exact cell epoch.

        Normally there is one submission reservation. Querying by cell identity keeps the lifecycle
        callback independent of request-local work ids and makes duplicate terminal callbacks safe.
        """
        rows = self.admission.db.execute(
            """SELECT work_id FROM admission_work
               WHERE state='active' AND cell_id=? AND cell_epoch=? ORDER BY sequence""",
            (cell_id, epoch),
        ).fetchall()
        admitted: list[str] = []
        for row in rows:
            admitted.extend(
                self.admission.complete(
                    str(row["work_id"]),
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
