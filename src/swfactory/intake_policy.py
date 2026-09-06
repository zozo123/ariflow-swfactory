"""Typed managed GitHub intake and deduplication policy."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum


class WorkKind(StrEnum):
    BUG = "bug"
    FEATURE = "feature"
    REFACTOR = "refactor"
    SECURITY = "security"
    DOCS = "docs"
    MAINTENANCE = "maintenance"


@dataclass(frozen=True)
class WorkProposal:
    title: str
    body: str
    kind: WorkKind
    source_urls: tuple[str, ...] = ()
    acceptance: tuple[str, ...] = ()
    labels: tuple[str, ...] = ()
    blueprint: str = "default"
    priority: int = 50

    def fingerprint(self) -> str:
        normalized = " ".join((self.title + " " + self.body).lower().split())
        return hashlib.sha256(normalized.encode()).hexdigest()[:24]

    def executable(self) -> bool:
        return bool(self.title.strip() and self.body.strip() and self.acceptance)


@dataclass(frozen=True)
class ExistingWork:
    number: int
    title: str
    body: str
    state: str

    def fingerprint(self) -> str:
        normalized = " ".join((self.title + " " + self.body).lower().split())
        return hashlib.sha256(normalized.encode()).hexdigest()[:24]


def dedupe(proposal: WorkProposal, existing: Iterable[ExistingWork]) -> int | None:
    fp = proposal.fingerprint()
    for item in existing:
        if item.state == "open" and item.fingerprint() == fp:
            return item.number
    return None


def triage(
    proposal: WorkProposal, *, auto_create: bool, allowed_kinds: set[WorkKind] | None = None
) -> dict:
    allowed_kinds = allowed_kinds or set(WorkKind)
    if proposal.kind not in allowed_kinds:
        return {"decision": "reject", "reason": "kind_not_allowed"}
    if not proposal.executable():
        return {"decision": "groom", "reason": "missing_acceptance_or_body"}
    return {
        "decision": "create" if auto_create else "review",
        "reason": "policy_allows" if auto_create else "human_gate_required",
        "fingerprint": proposal.fingerprint(),
        "blueprint": proposal.blueprint,
        "priority": proposal.priority,
    }
