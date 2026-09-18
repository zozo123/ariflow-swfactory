from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
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


@dataclass
class DurableIncidentLedger(IncidentLedger):
    """JSON-backed incident ledger used across maintenance restarts.

    The file is replaced atomically so a crash cannot leave a partially written identity map.
    """

    path: Path = field(default_factory=lambda: Path(".factory/maintenance-incidents.json"))

    @classmethod
    def load(cls, path: Path) -> "DurableIncidentLedger":
        path = Path(path)
        if not path.is_file():
            return cls(path=path)
        data = json.loads(path.read_text(encoding="utf-8"))
        states = {str(key): IncidentState(value) for key, value in data.get("states", {}).items()}
        receipts = {
            str(key): IncidentReceipt(
                key=str(value["key"]),
                issue_number=int(value["issue_number"]),
                issue_url=str(value["issue_url"]),
                content_digest=str(value["content_digest"]),
            )
            for key, value in data.get("receipts", {}).items()
        }
        return cls(states=states, receipts=receipts, path=path)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": 1,
            "states": {key: value.value for key, value in sorted(self.states.items())},
            "receipts": {key: asdict(value) for key, value in sorted(self.receipts.items())},
        }
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
        tmp.replace(self.path)

    def propose(self, identity: IncidentIdentity, content: Mapping[str, object]) -> tuple[str, bool]:
        result = super().propose(identity, content)
        self.save()
        return result

    def begin_create(self, identity: IncidentIdentity) -> str:
        result = super().begin_create(identity)
        self.save()
        return result

    def record_created(
        self, identity: IncidentIdentity, issue_number: int, issue_url: str, content: Mapping[str, object]
    ) -> IncidentReceipt:
        result = super().record_created(identity, issue_number, issue_url, content)
        self.save()
        return result

    def mark_unknown(self, identity: IncidentIdentity) -> None:
        super().mark_unknown(identity)
        self.save()

    def adopt_observed(self, identity: IncidentIdentity, receipt: IncidentReceipt) -> IncidentReceipt:
        result = super().adopt_observed(identity, receipt)
        self.save()
        return result
