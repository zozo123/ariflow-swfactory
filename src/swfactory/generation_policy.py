"""Bounded factory-of-factories experiments with parent-only promotion authority."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Mapping

from swfactory.idempotency import OperationRef


class GenerationRefused(RuntimeError):
    pass


@dataclass(frozen=True)
class GenerationLimits:
    max_depth: int = 3
    max_budget: int = 100

    def __post_init__(self) -> None:
        if self.max_depth < 0 or self.max_budget <= 0:
            raise ValueError("generation limits must be non-negative depth and positive budget")


@dataclass(frozen=True)
class ChildExperiment:
    parent_generation: str
    child_generation: str
    depth: int
    budget: int
    experiment_id: str
    artifact_digest: str
    inputs_digest: str

    def validate(self, limits: GenerationLimits) -> None:
        if not self.parent_generation or not self.child_generation or self.parent_generation == self.child_generation:
            raise GenerationRefused("child generation must have a distinct parent")
        if self.depth <= 0 or self.depth > limits.max_depth:
            raise GenerationRefused(f"generation depth {self.depth} exceeds bound {limits.max_depth}")
        if self.budget <= 0 or self.budget > limits.max_budget:
            raise GenerationRefused(f"generation budget {self.budget} exceeds bound {limits.max_budget}")
        for name, value in (
            ("experiment_id", self.experiment_id),
            ("artifact_digest", self.artifact_digest),
            ("inputs_digest", self.inputs_digest),
        ):
            if not value:
                raise GenerationRefused(f"{name} is required")


@dataclass(frozen=True)
class PromotionDecision:
    operation: OperationRef
    evidence: Mapping[str, object]


def promotion_decision(
    experiment: ChildExperiment,
    *,
    requester_generation: str,
    cell_id: str,
    epoch: int,
    metrics: Mapping[str, float],
    thresholds: Mapping[str, float],
    limits: GenerationLimits = GenerationLimits(),
) -> PromotionDecision:
    """Authorize only the parent after all explicit metric thresholds pass."""
    experiment.validate(limits)
    if requester_generation != experiment.parent_generation:
        raise GenerationRefused("only the parent generation may promote a child experiment")
    missing = sorted(set(thresholds) - set(metrics))
    if missing:
        raise GenerationRefused(f"promotion evidence is missing metrics: {', '.join(missing)}")
    failed = {name: metrics[name] for name, minimum in thresholds.items() if metrics[name] < minimum}
    if failed:
        raise GenerationRefused(f"promotion thresholds failed: {failed}")
    evidence = {
        "parent_generation": experiment.parent_generation,
        "child_generation": experiment.child_generation,
        "experiment_id": experiment.experiment_id,
        "depth": experiment.depth,
        "budget": experiment.budget,
        "inputs_digest": experiment.inputs_digest,
        "artifact_digest": experiment.artifact_digest,
        "metrics": dict(sorted(metrics.items())),
        "thresholds": dict(sorted(thresholds.items())),
        "decision": "promote",
        "authority": "parent",
    }
    evidence_digest = hashlib.sha256(json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    operation = OperationRef.build(cell_id, epoch, "generation_promote", experiment.experiment_id, evidence_digest)
    return PromotionDecision(operation, evidence)
