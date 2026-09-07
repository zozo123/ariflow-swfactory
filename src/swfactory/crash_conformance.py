"""Crash/restart conformance for Cell takeover and ambiguous external mutations.

This is intentionally not a scheduler.  It repeatedly closes and reopens the durable stores to
model scheduler/worker process loss while Airflow remains the lifecycle authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from swfactory.cells import CellIdentity, CellStore, StaleEpoch
from swfactory.idempotency import MutationOutcome, OperationJournal, OperationRef


@dataclass(frozen=True)
class CrashCycleResult:
    cell_id: str
    old_epoch: int
    new_epoch: int
    stale_writer_fenced: bool
    ambiguous_observed: bool
    current_write_calls: int
    history_events: int


def run_crash_cycle(root: Path, cycle: int) -> CrashCycleResult:
    root.mkdir(parents=True, exist_ok=True)
    cell_path = root / f"cells-{cycle}.sqlite3"
    ops_path = root / f"ops-{cycle}.sqlite3"
    identity = CellIdentity("owner/repo", "target", f"issue-{cycle}")

    # First scheduler/worker incarnation owns epoch 1 and has already dispatched the Airflow run.
    cells = CellStore(cell_path)
    cell = cells.activate(identity, actor="worker:before-crash")
    old_epoch = int(cell["epoch"])
    cells.patch(
        cell["cell_id"],
        old_epoch,
        f"bind:{cycle}",
        state="running",
        airflow_dag_id="factory",
        airflow_run_id=f"manual__crash_{cycle}",
        map_index=cycle,
    )
    operations = OperationJournal(ops_path)
    old_ref = OperationRef.build(cell["cell_id"], old_epoch, "github_publish", f"delivery-{cycle}")
    operations.begin(old_ref)
    operations.mark_in_doubt(old_ref, "worker process disappeared after send")

    # Process death: no in-memory lock or object survives this point.
    cells.close()
    operations.close()

    cells = CellStore(cell_path)
    operations = OperationJournal(ops_path)
    try:
        recovered = cells.get(identity.stable_id())
        assert recovered["airflow_run_id"] == f"manual__crash_{cycle}"
        new_epoch = cells.take_epoch(recovered["cell_id"], old_epoch, actor="worker:after-crash")

        stale_writer_fenced = False
        try:
            cells.patch(recovered["cell_id"], old_epoch, f"stale:{cycle}", state="success")
        except StaleEpoch:
            stale_writer_fenced = True

        unresolved = operations.get(old_ref.key)
        ambiguous_observed = unresolved["state"] == "in_doubt"
        operations.mark_observation(
            old_ref,
            MutationOutcome(
                "ambiguous",
                evidence={"cycle": cycle, "observed_after_restart": True},
                detail="external publication cannot be proven absent after crash",
            ),
        )

        # A new owner receives a new operation identity. Replaying the committed operation does
        # not invoke the external adapter again.
        new_ref = OperationRef.build(recovered["cell_id"], new_epoch, "github_publish", f"delivery-{cycle}")
        calls = 0

        def publish() -> dict[str, object]:
            nonlocal calls
            calls += 1
            return {"published": True, "cycle": cycle}

        first = operations.execute(new_ref, publish)
        second = operations.execute(new_ref, publish)
        assert first == second
        cells.patch(recovered["cell_id"], new_epoch, f"complete:{cycle}", state="success")
        history = cells.history(recovered["cell_id"])
        return CrashCycleResult(
            cell_id=recovered["cell_id"],
            old_epoch=old_epoch,
            new_epoch=new_epoch,
            stale_writer_fenced=stale_writer_fenced,
            ambiguous_observed=ambiguous_observed,
            current_write_calls=calls,
            history_events=len(history),
        )
    finally:
        cells.close()
        operations.close()
