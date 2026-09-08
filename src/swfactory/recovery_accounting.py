from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Mapping


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
class RetryPolicy:
    attempts: int
    max_attempts: int
    retry_at: datetime | None
    replay_safe: bool
    authority_current: bool

    def next_action(self, observation: Observation) -> RecoveryAction:
        if not self.authority_current:
            return RecoveryAction.REFUSE
        if observation.status == Outcome.IN_DOUBT:
            return RecoveryAction.OBSERVE
        if self.attempts >= self.max_attempts or observation.status == Outcome.EXHAUSTED:
            return RecoveryAction.REFUSE
        if self.retry_at is not None and self.retry_at > datetime.now(timezone.utc):
            return RecoveryAction.WAIT
        if observation.status == Outcome.ABSENT and self.replay_safe:
            return RecoveryAction.RETRY
        if observation.status == Outcome.COMMITTED:
            return RecoveryAction.ADOPT
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

    def verify(self, expected: RemoteIdentity) -> bool:
        if self.repository != expected.repository:
            return False
        if self.content_digest != expected.immutable_content:
            return False
        return self.pr_state in {"open", "closed", "merged", "branch_only"}


@dataclass
class AttemptLedger:
    operation_id: str
    max_attempts: int
    attempts: dict[str, Mapping[str, object]] = field(default_factory=dict)

    def reserve(self, attempt_id: str, *, input_head: str, budget_usd: float) -> None:
        if attempt_id in self.attempts:
            raise ValueError("attempt already exists")
        if len(self.attempts) >= self.max_attempts:
            raise RuntimeError("attempt budget exhausted")
        if budget_usd < 0:
            raise ValueError("budget must be non-negative")
        self.attempts[attempt_id] = {
            "state": "reserved",
            "input_head": input_head,
            "budget_usd": float(budget_usd),
            "output_head": None,
            "receipt": None,
        }

    def settle(self, attempt_id: str, *, output_head: str | None, receipt: Mapping[str, object]) -> None:
        row = dict(self.attempts[attempt_id])
        row.update(state="settled", output_head=output_head, receipt=dict(receipt))
        self.attempts[attempt_id] = row

    def mark_unknown(self, attempt_id: str, detail: str) -> None:
        row = dict(self.attempts[attempt_id])
        row.update(state="unknown", detail=detail)
        self.attempts[attempt_id] = row


@dataclass
class SpendLedger:
    ceiling_usd: float
    reserved: dict[str, float] = field(default_factory=dict)
    settled: dict[str, float] = field(default_factory=dict)
    unknown: set[str] = field(default_factory=set)

    @property
    def committed_usd(self) -> float:
        return sum(self.settled.values())

    @property
    def reserved_usd(self) -> float:
        return sum(self.reserved.values())

    def reserve(self, call_id: str, amount: float) -> None:
        if call_id in self.reserved or call_id in self.settled or call_id in self.unknown:
            raise ValueError("duplicate call identity")
        if amount < 0:
            raise ValueError("reservation must be non-negative")
        if self.committed_usd + self.reserved_usd + amount > self.ceiling_usd:
            raise RuntimeError("budget ceiling exceeded")
        self.reserved[call_id] = amount

    def settle(self, call_id: str, amount: float) -> None:
        reserved = self.reserved.pop(call_id)
        if amount < 0 or amount > reserved:
            raise ValueError("settled spend must fit reservation")
        self.settled[call_id] = amount

    def lose_result(self, call_id: str) -> None:
        self.reserved.pop(call_id)
        self.unknown.add(call_id)

    @property
    def available_usd(self) -> float:
        # Unknown spend stays conservatively unavailable until explicit reconciliation.
        blocked_unknown = 0.0
        return max(0.0, self.ceiling_usd - self.committed_usd - self.reserved_usd - blocked_unknown)


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


@dataclass(frozen=True)
class BackupManifest:
    schema_version: int
    created_at: datetime
    stores: Mapping[str, str]
    evidence_digest: str

    def validate(self, *, expected_schema: int, required_stores: set[str]) -> None:
        if self.schema_version != expected_schema:
            raise RuntimeError("unsupported backup schema")
        missing = required_stores - set(self.stores)
        if missing:
            raise RuntimeError("backup missing stores: " + ",".join(sorted(missing)))
        if len(self.evidence_digest) != 64:
            raise RuntimeError("backup evidence digest is invalid")
