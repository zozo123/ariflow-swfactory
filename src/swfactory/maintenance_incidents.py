from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


@dataclass(frozen=True)
class IncidentIdentity:
    repository: str
    source_run: str
    evidence_digest: str
    metric: str
    policy_version: str

    @property
    def key(self) -> str:
        return "incident_" + _digest(self.__dict__)[:32]


class IncidentState(StrEnum):
    PROPOSED = "proposed"
    CREATING = "creating"
    OPEN = "open"
    IN_DOUBT = "in_doubt"
    CLOSED = "closed"


@dataclass(frozen=True)
class IncidentReceipt:
    key: str
    issue_number: int
    issue_url: str
    content_digest: str


@dataclass
class IncidentLedger:
    states: dict[str, IncidentState] = field(default_factory=dict)
    receipts: dict[str, IncidentReceipt] = field(default_factory=dict)

    def propose(self, identity: IncidentIdentity, content: Mapping[str, object]) -> tuple[str, bool]:
        key = identity.key
        if key in self.states:
            return key, False
        self.states[key] = IncidentState.PROPOSED
        return key, True

    def begin_create(self, identity: IncidentIdentity) -> str:
        key = identity.key
        current = self.states.get(key)
        if current not in {IncidentState.PROPOSED, IncidentState.IN_DOUBT}:
            raise RuntimeError(f"incident {key} cannot create from {current}")
        self.states[key] = IncidentState.CREATING
        return key

    def record_created(
        self, identity: IncidentIdentity, issue_number: int, issue_url: str, content: Mapping[str, object]
    ) -> IncidentReceipt:
        key = identity.key
        if issue_number <= 0 or not issue_url:
            raise ValueError("created incident requires an issue identity")
        receipt = IncidentReceipt(key, issue_number, issue_url, _digest(dict(content)))
        existing = self.receipts.get(key)
        if existing is not None and existing != receipt:
            raise RuntimeError("incident identity resolved to a different GitHub issue")
        self.receipts[key] = receipt
        self.states[key] = IncidentState.OPEN
        return receipt

    def mark_unknown(self, identity: IncidentIdentity) -> None:
        key = identity.key
        if self.states.get(key) not in {IncidentState.CREATING, IncidentState.PROPOSED}:
            raise RuntimeError("only an unsettled incident creation can become in-doubt")
        self.states[key] = IncidentState.IN_DOUBT

    def adopt_observed(self, identity: IncidentIdentity, receipt: IncidentReceipt) -> IncidentReceipt:
        if receipt.key != identity.key:
            raise RuntimeError("observed GitHub issue belongs to another incident")
        existing = self.receipts.get(identity.key)
        if existing is not None and existing != receipt:
            raise RuntimeError("conflicting observed incident receipt")
        self.receipts[identity.key] = receipt
        self.states[identity.key] = IncidentState.OPEN
        return receipt


def should_roll_incident(previous: IncidentIdentity, current: IncidentIdentity) -> bool:
    """Return true only when the source evidence or policy scope describes a new regression."""
    return previous.key != current.key
