"""Deterministic recovery decisions for durable external mutations.

Recovery is driven from durable operation rows and current Cell authority.  This module does not
poll or schedule; a bounded reconciler calls it and executes the returned action.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any


class RecoveryAction(StrEnum):
    COMMITTED = "committed"
    OBSERVE = "observe"
    RETRY = "retry"
    WAIT = "wait"
    REFUSE = "refuse"
    DEAD = "dead"


@dataclass(frozen=True)
class RecoveryTarget:
    kind: str
    identity: dict[str, str]

    def canonical(self) -> dict[str, Any]:
        if not self.kind.strip():
            raise ValueError("recovery target kind must be nonempty")
        identity = {
            str(key): str(value)
            for key, value in sorted(self.identity.items())
            if str(key).strip() and str(value).strip()
        }
        if not identity:
            raise ValueError("recovery target must have stable identity fields")
        return {"kind": self.kind.strip(), "identity": identity}

    def digest(self) -> str:
        payload = json.dumps(
            self.canonical(),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return "target:" + hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class RecoveryDecision:
    operation_key: str
    action: RecoveryAction
    reason: str
    target: RecoveryTarget | None = None
    retry_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["action"] = self.action.value
        if self.target is not None:
            value["target"] = self.target.canonical()
        return value


def plan_recovery(
    operation: dict[str, Any],
    *,
    current_epoch: int,
    now: float | None = None,
) -> RecoveryDecision:
    """Classify one durable operation without performing external I/O."""
    clock = time.time() if now is None else now
    key = str(operation.get("operation_key", "")).strip()
    if not key:
        raise ValueError("operation row is missing operation_key")
    epoch = operation.get("epoch")
    if type(epoch) is not int or epoch < 1:
        return RecoveryDecision(key, RecoveryAction.DEAD, "invalid_epoch")
    if epoch != current_epoch:
        return RecoveryDecision(key, RecoveryAction.REFUSE, "stale_epoch")

    state = str(operation.get("state") or operation.get("status") or "pending").casefold()
    if state in {"committed", "success", "done"}:
        return RecoveryDecision(key, RecoveryAction.COMMITTED, "already_committed")
    if state in {"refused", "divergent", "dead"}:
        return RecoveryDecision(key, RecoveryAction.DEAD, state)

    next_attempt = operation.get("next_attempt_at")
    if isinstance(next_attempt, (int, float)) and next_attempt > clock:
        return RecoveryDecision(
            key,
            RecoveryAction.WAIT,
            "backoff",
            retry_at=float(next_attempt),
        )

    target = _target(operation)
    attempts = operation.get("attempts", 0)
    max_attempts = operation.get("max_attempts", 8)
    if type(attempts) is not int or type(max_attempts) is not int:
        return RecoveryDecision(key, RecoveryAction.DEAD, "invalid_retry_budget", target)
    if attempts >= max_attempts:
        return RecoveryDecision(key, RecoveryAction.DEAD, "retry_budget_exhausted", target)

    outcome = str(operation.get("outcome") or "").casefold()
    if state in {"ambiguous", "observing"} or outcome == "ambiguous":
        return RecoveryDecision(key, RecoveryAction.OBSERVE, "must_observe_before_retry", target)
    return RecoveryDecision(key, RecoveryAction.RETRY, "retryable_pending_operation", target)


def _target(operation: dict[str, Any]) -> RecoveryTarget | None:
    raw = operation.get("target")
    if not isinstance(raw, dict):
        return None
    kind = str(raw.get("kind", "")).strip()
    identity = raw.get("identity")
    if not kind or not isinstance(identity, dict):
        return None
    target = RecoveryTarget(kind, {str(key): str(value) for key, value in identity.items()})
    target.canonical()
    return target
