from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum


def _hash(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


@dataclass(frozen=True)
class SandboxIdentity:
    provider: str
    cell_id: str
    epoch: int
    attempt_id: str

    def validate(self) -> None:
        if not self.provider:
            raise ValueError("provider is required")
        if not self.cell_id:
            raise ValueError("cell_id is required")
        if self.epoch <= 0:
            raise ValueError("epoch must be positive")
        if not self.attempt_id:
            raise ValueError("attempt_id is required")

    @property
    def stable_name(self) -> str:
        self.validate()
        suffix = _hash(self.__dict__)[:20]
        prefix = "swf-" + "".join(ch for ch in self.cell_id.lower() if ch.isalnum())[:20]
        return f"{prefix}-{suffix}"[:63]

    @property
    def labels(self) -> Mapping[str, str]:
        return {
            "swfactory.owned": "true",
            "swfactory.cell": self.cell_id,
            "swfactory.epoch": str(self.epoch),
            "swfactory.attempt": self.attempt_id,
        }

    @classmethod
    def from_labels(cls, provider: str, labels: Mapping[str, str]) -> SandboxIdentity | None:
        """The identity a resource's labels *claim*, or ``None`` when they are not a complete factory
        stamp. A claim confers nothing: ``authorize_cleanup`` decides whether it may be acted on."""
        if labels.get("swfactory.owned") != "true":
            return None
        try:
            identity = cls(
                provider, labels["swfactory.cell"], int(labels["swfactory.epoch"]), labels["swfactory.attempt"]
            )
            identity.validate()
        except (KeyError, ValueError):
            return None
        return identity


class CleanupDecision(StrEnum):
    REMOVE = "remove"
    KEEP = "keep"
    OBSERVE = "observe"
    REFUSE = "refuse"


@dataclass(frozen=True)
class ResourceObservation:
    provider_id: str
    labels: Mapping[str, str]
    running: bool
    reachable: bool = True


def authorize_cleanup(
    identity: SandboxIdentity,
    observation: ResourceObservation,
    *,
    current_epoch: int | None,
    active: bool,
) -> CleanupDecision:
    identity.validate()
    if not observation.reachable:
        return CleanupDecision.OBSERVE
    if observation.labels.get("swfactory.owned") != "true":
        return CleanupDecision.REFUSE
    if observation.labels.get("swfactory.cell") != identity.cell_id:
        return CleanupDecision.REFUSE
    if observation.labels.get("swfactory.epoch") != str(identity.epoch):
        return CleanupDecision.REFUSE
    if observation.labels.get("swfactory.attempt") != identity.attempt_id:
        return CleanupDecision.REFUSE
    if current_epoch is None:
        return CleanupDecision.OBSERVE
    if current_epoch != identity.epoch:
        return CleanupDecision.REMOVE if not active else CleanupDecision.REFUSE
    if active:
        return CleanupDecision.KEEP
    return CleanupDecision.REMOVE


@dataclass
class CleanupDebt:
    outstanding: dict[str, SandboxIdentity] = field(default_factory=dict)

    def record(self, provider_id: str, identity: SandboxIdentity) -> None:
        identity.validate()
        existing = self.outstanding.get(provider_id)
        if existing is not None and existing != identity:
            raise RuntimeError("provider identity reused across Factory Cells")
        self.outstanding[provider_id] = identity

    def settle(self, provider_id: str, identity: SandboxIdentity) -> None:
        if self.outstanding.get(provider_id) != identity:
            raise RuntimeError("cleanup receipt does not match outstanding debt")
        del self.outstanding[provider_id]
