from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum


class Outcome(StrEnum):
    COMMITTED = "committed"
    ABSENT = "definitely_absent"
    IN_DOUBT = "in_doubt"
    REFUSED = "refused"
    EXHAUSTED = "exhausted"


class RecoveryAction(StrEnum):
    ADOPT = "adopt"
    RETRY = "retry"
    OBSERVE = "observe"
    WAIT = "wait"
    REFUSE = "refuse"
    REPAIR = "repair"


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


@dataclass(frozen=True)
class RemoteIdentity:
    kind: str
    repository: str
    logical_key: str
    immutable_content: str

    @property
    def digest(self) -> str:
        return _digest(self.__dict__)


@dataclass(frozen=True)
class Observation:
    status: Outcome
    identity_digest: str | None = None
    receipt: Mapping[str, object] | None = None
    detail: str = ""


def classify_observation(expected: RemoteIdentity, observed: Observation) -> RecoveryAction:
    if observed.status == Outcome.COMMITTED:
        if observed.identity_digest != expected.digest:
            return RecoveryAction.REFUSE
        return RecoveryAction.ADOPT
    if observed.status == Outcome.ABSENT:
        return RecoveryAction.RETRY
    if observed.status == Outcome.IN_DOUBT:
        return RecoveryAction.OBSERVE
    if observed.status in {Outcome.REFUSED, Outcome.EXHAUSTED}:
        return RecoveryAction.REFUSE
    return RecoveryAction.REFUSE


@dataclass(frozen=True)
class PublicationReceipt:
    repository: str
    base_revision: str
    head_revision: str
    content_digest: str
    branch: str
    pr_number: int | None
    pr_state: str
    url: str | None = None

    def verify(self, expected: RemoteIdentity) -> bool:
        if self.repository != expected.repository:
            return False
        if self.content_digest != expected.immutable_content:
            return False
        return self.pr_state in {"open", "closed", "merged", "branch_only"}


@dataclass(frozen=True)
class CallbackDebt:
    cell_id: str
    epoch: int
    dag_run_id: str
    task_id: str
    desired_state: str
    attempt: int = 0

    @property
    def key(self) -> str:
        return _digest(
            {
                "cell": self.cell_id,
                "epoch": self.epoch,
                "run": self.dag_run_id,
                "task": self.task_id,
                "state": self.desired_state,
            }
        )[:32]


def reconcile_callback(current_epoch: int, debt: CallbackDebt, observed_state: str | None) -> RecoveryAction:
    if debt.epoch != current_epoch:
        return RecoveryAction.REFUSE
    if observed_state == debt.desired_state:
        return RecoveryAction.ADOPT
    if observed_state is None:
        return RecoveryAction.OBSERVE
    if observed_state in {"success", "failed", "cancelled"} and debt.desired_state != observed_state:
        return RecoveryAction.REFUSE
    return RecoveryAction.RETRY
