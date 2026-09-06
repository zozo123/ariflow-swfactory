"""Rough lifecycle runtime for Airflow-owned Liquid400 issue families.

Airflow remains the only scheduler. This module only produces deterministic lifecycle intents and
restart-safe decisions for callers that already execute inside the Airflow DAG.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


TERMINAL = frozenset({"success", "failed", "cancelled", "rejected", "cleaned"})


@dataclass(frozen=True)
class AirflowLifecycleIntent:
    cell_id: str
    epoch: int
    dag_id: str
    run_id: str
    map_index: int | None = None
    cancelled: bool = False
    approval: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        if not self.cell_id.startswith("cell_"):
            raise ValueError("invalid Factory Cell id")
        if self.epoch < 1:
            raise ValueError("epoch must be positive")
        if not self.dag_id or not self.run_id:
            raise ValueError("Airflow dag_id and run_id are required")
        if self.approval not in {None, "pending", "approved", "rejected"}:
            raise ValueError("invalid approval state")


@dataclass(frozen=True)
class RecoveryDecision:
    action: str
    reason: str


def restart_decision(*, cell_state: str, airflow_state: str, cancelled: bool) -> RecoveryDecision:
    if cancelled and cell_state not in TERMINAL:
        return RecoveryDecision("cancel", "durable cancellation wins after restart")
    if cell_state in TERMINAL:
        return RecoveryDecision("observe", "cell is terminal; do not restart lifecycle")
    if airflow_state in {"running", "queued", "scheduled", "deferred"}:
        return RecoveryDecision("observe", "Airflow still owns the active lifecycle")
    if airflow_state in {"failed", "upstream_failed"}:
        return RecoveryDecision("repair", "Airflow failed while durable Cell remains active")
    return RecoveryDecision("resume", "reconstruct Airflow execution from durable Cell intent")


def approval_decision(*, stored: str | None, incoming: str | None) -> str:
    if stored in {"approved", "rejected"}:
        return stored
    if incoming in {"approved", "rejected"}:
        return incoming
    return "pending"


def bounded_admission(*, active: int, limit: int, priority: int, age_seconds: float) -> bool:
    if limit < 1:
        raise ValueError("limit must be positive")
    if active < limit:
        return True
    # Aged high-priority work is allowed to request preemption/rebalancing from the caller,
    # but this function never schedules anything itself.
    return priority > 0 and age_seconds >= 300.0
