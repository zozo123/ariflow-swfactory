"""Deterministic Factory Cell <-> Airflow lifecycle binding.

The helpers in this module produce identities and reconcile state.  They never dispatch tasks;
Airflow remains the sole lifecycle scheduler.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any

_DAG_RE = re.compile(r"[^a-zA-Z0-9_.-]+")


class LifecycleConflict(RuntimeError):
    """Airflow and Cell state cannot be reconciled without operator action."""


class LifecycleState(StrEnum):
    QUEUED = "queued"
    DISPATCHING = "dispatching"
    RUNNING = "running"
    WAITING_APPROVAL = "waiting_approval"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    CLEANED = "cleaned"


_TERMINAL = {
    LifecycleState.FAILED,
    LifecycleState.CANCELLED,
    LifecycleState.REJECTED,
    LifecycleState.CLEANED,
}


@dataclass(frozen=True)
class AirflowBinding:
    cell_id: str
    epoch: int
    dag_id: str
    dag_run_id: str
    blueprint_digest: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def bind_cell(
    *,
    cell_id: str,
    epoch: int,
    line_name: str,
    blueprint_digest: str,
) -> AirflowBinding:
    if not cell_id.startswith("cell_"):
        raise ValueError("Airflow binding requires a Factory Cell id")
    if epoch < 1:
        raise ValueError("Airflow binding epoch must be positive")
    digest = blueprint_digest.removeprefix("sha256:").lower()
    if len(digest) < 16 or any(ch not in "0123456789abcdef" for ch in digest):
        raise ValueError("blueprint digest must be hexadecimal")

    dag_id = _DAG_RE.sub("-", line_name.strip()).strip("-.")
    if not dag_id:
        raise ValueError("production line name cannot normalize to an empty DAG id")
    dag_id = f"swf__{dag_id}"[:250]
    identity = "\0".join((cell_id, str(epoch), dag_id, digest)).encode()
    run_hash = hashlib.sha256(identity).hexdigest()[:32]
    run_id = f"swf_cell__{cell_id[5:]}__e{epoch}__{run_hash}"
    return AirflowBinding(cell_id, epoch, dag_id, run_id, digest)


def reconcile_state(
    cell_state: str,
    airflow_state: str | None,
    *,
    approval_pending: bool = False,
) -> LifecycleState:
    """Converge observed Airflow state into the durable lifecycle projection.

    The function is deliberately monotonic for terminal Cell states.  A later scheduler
    observation cannot resurrect a terminal Cell.
    """
    current = LifecycleState(cell_state)
    if current in _TERMINAL:
        return current
    if current == LifecycleState.SUCCESS and airflow_state not in {None, "success"}:
        raise LifecycleConflict(
            f"successful Cell cannot be reconciled with Airflow state {airflow_state}"
        )
    if approval_pending:
        return LifecycleState.WAITING_APPROVAL
    if airflow_state is None:
        return current

    observed = airflow_state.casefold()
    if observed in {"queued", "scheduled", "deferred", "up_for_reschedule"}:
        return LifecycleState.QUEUED
    if observed in {"running", "restarting", "up_for_retry"}:
        return LifecycleState.RUNNING
    if observed == "success":
        return LifecycleState.SUCCESS
    if observed in {"failed", "upstream_failed"}:
        return LifecycleState.FAILED
    if observed in {"removed", "shutdown"}:
        return LifecycleState.CANCELLED
    raise LifecycleConflict(f"unsupported Airflow state: {airflow_state}")


def compatibility_receipt(binding: AirflowBinding, *, dag_digest: str) -> dict[str, Any]:
    current = dag_digest.removeprefix("sha256:").lower()
    return {
        "schema_version": 1,
        "cell_id": binding.cell_id,
        "epoch": binding.epoch,
        "dag_id": binding.dag_id,
        "dag_run_id": binding.dag_run_id,
        "blueprint_digest": binding.blueprint_digest,
        "dag_digest": current,
        "compatible": current == binding.blueprint_digest,
    }
