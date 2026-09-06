"""Bounded factory-of-factories generation and promotion policy."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum


class Dimension(StrEnum):
    CORRECTNESS = "correctness"
    EVIDENCE = "evidence"
    RELIABILITY = "reliability"
    SECURITY = "security"
    LATENCY = "latency"
    COST = "cost"
    OPERATOR_UX = "operator_ux"


@dataclass(frozen=True)
class Generation:
    source_sha: str
    image_digest: str
    blueprint_digest: str
    policy_digest: str
    schema_version: str
    parent_id: str | None = None

    @property
    def id(self) -> str:
        raw = "\0".join(
            (
                self.source_sha,
                self.image_digest,
                self.blueprint_digest,
                self.policy_digest,
                self.schema_version,
                self.parent_id or "",
            )
        ).encode()
        return "gen_" + hashlib.sha256(raw).hexdigest()[:24]


@dataclass(frozen=True)
class Evaluation:
    dimension: Dimension
    result: str
    evidence: str


@dataclass(frozen=True)
class CampaignBudget:
    max_depth: int = 1
    max_candidates: int = 4
    max_cost_usd: float = 100.0
    max_wall_s: int = 3600

    def admits(self, *, depth: int, candidates: int, cost_usd: float, wall_s: int) -> bool:
        return (
            0 <= depth <= self.max_depth
            and candidates < self.max_candidates
            and cost_usd <= self.max_cost_usd
            and wall_s <= self.max_wall_s
        )


def promotable(
    evaluations: list[Evaluation], *, required: set[Dimension], human_approved: bool
) -> tuple[bool, tuple[str, ...]]:
    by_dimension = {item.dimension: item for item in evaluations}
    failures = []
    for dimension in sorted(required, key=lambda d: d.value):
        item = by_dimension.get(dimension)
        if item is None:
            failures.append(f"missing:{dimension.value}")
        elif item.result != "pass":
            failures.append(f"{item.result}:{dimension.value}")
    if not human_approved:
        failures.append("human_gate")
    return not failures, tuple(failures)


def candidate_credentials() -> tuple[str, ...]:
    """Explicitly documents the only credential class candidates may receive."""
    return ("synthetic_repo_read_write", "ephemeral_airflow", "ephemeral_backend")
