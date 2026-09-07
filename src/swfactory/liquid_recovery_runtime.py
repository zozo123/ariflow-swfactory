"""Rough repair/recovery implementation for ambiguous external state and disaster paths."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class RecoveryAction(StrEnum):
    OBSERVE = "observe"
    RETRY = "retry"
    REPAIR = "repair"
    RECLAIM = "reclaim"
    ROLLBACK = "rollback"
    REFUSE = "refuse"


@dataclass(frozen=True)
class RecoveryInput:
    cell_id: str
    epoch: int
    current_epoch: int
    desired_state: str
    observed_state: str | None
    operation_status: str | None
    cleanup_complete: bool = False


@dataclass(frozen=True)
class RecoveryPlan:
    action: RecoveryAction
    reason: str
    destructive: bool = False


def plan_recovery(value: RecoveryInput) -> RecoveryPlan:
    if value.epoch != value.current_epoch:
        return RecoveryPlan(RecoveryAction.REFUSE, "stale epoch cannot repair current state")
    if value.operation_status == "unknown":
        return RecoveryPlan(RecoveryAction.OBSERVE, "observe external state before retry")
    if value.desired_state in {"cancelled", "cleaned"} and not value.cleanup_complete:
        return RecoveryPlan(RecoveryAction.RECLAIM, "terminal intent requires resource cleanup", True)
    if value.observed_state is None:
        return RecoveryPlan(RecoveryAction.RETRY, "no external observation exists")
    if value.observed_state == value.desired_state:
        return RecoveryPlan(RecoveryAction.OBSERVE, "external state already matches durable intent")
    if value.desired_state in {"running", "dispatching"}:
        return RecoveryPlan(RecoveryAction.REPAIR, "active Cell diverged from external state")
    if value.desired_state in {"success", "failed", "rejected"}:
        return RecoveryPlan(RecoveryAction.RECLAIM, "terminal Cell must not retain active resources", True)
    return RecoveryPlan(RecoveryAction.ROLLBACK, "unknown divergence requires known-good generation", True)


def cleanup_debt_score(*, age_seconds: float, attempts: int, cost_estimate: float) -> float:
    age = max(age_seconds, 0.0) / 60.0
    retry_pressure = max(attempts, 0) * 10.0
    cost_pressure = max(cost_estimate, 0.0)
    return age + retry_pressure + cost_pressure


def migration_action(*, schema_from: int, schema_to: int, reversible: bool) -> RecoveryPlan:
    if schema_to < schema_from:
        return RecoveryPlan(RecoveryAction.ROLLBACK, "schema target is older than current", True)
    if schema_to == schema_from:
        return RecoveryPlan(RecoveryAction.OBSERVE, "schema already current")
    if not reversible:
        return RecoveryPlan(RecoveryAction.REFUSE, "online migration lacks rollback contract")
    return RecoveryPlan(RecoveryAction.REPAIR, "apply forward migration with rollback point")
