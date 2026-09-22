"""Phase-aware population dynamics for Liquid Software Factory search.

The unit of scaling is not "an agent". It is useful independent information.

This module turns observable search state into a bounded population/compute plan:
cheap heterogeneous exploration when entropy is useful, selective deep compute at
high-value disagreements, exact replay/verification when a candidate crystallizes,
and draining instead of spawning when the factory is jammed.

It is deliberately search-only. The output may shape candidate population,
model/compute tier, context posture and verifier intensity. It cannot schedule
Airflow, mutate durable Cell state, relax evidence, mint credentials, approve,
publish, merge or promote.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any

from swfactory.phase_control import ControlMode, PhaseAssessment

SWARM_DYNAMICS_SCHEMA_VERSION = 1
SWARM_DYNAMICS_AUTHORITY = "search-only"


class AgentRole(StrEnum):
    EXPLORER = "explorer"
    MUTATOR = "mutator"
    SYNTHESIZER = "synthesizer"
    CRITIC = "critic"
    VERIFIER = "verifier"
    RED_TEAM = "red-team"
    MEMORY = "memory"


class ComputeTier(StrEnum):
    CHEAP = "cheap"
    STANDARD = "standard"
    DEEP = "deep"
    EXACT_REPLAY = "exact-replay"


class ContextPolicy(StrEnum):
    FRESH = "fresh"
    INHERIT = "inherit"
    COMPACT = "compact"
    FROZEN = "frozen"


@dataclass(frozen=True)
class SwarmBudget:
    """Hard search envelope; a planner may spend less but never more."""

    max_agents: int = 32
    max_parallel: int = 16
    max_deep_agents: int = 4
    max_exact_replays: int = 2
    max_compute_units: float = 100.0

    def validate(self) -> None:
        if self.max_agents < 1:
            raise ValueError("max_agents must be positive")
        if self.max_parallel < 1 or self.max_parallel > self.max_agents:
            raise ValueError("max_parallel must be in [1, max_agents]")
        if self.max_deep_agents < 0 or self.max_deep_agents > self.max_agents:
            raise ValueError("max_deep_agents must be in [0, max_agents]")
        if self.max_exact_replays < 0 or self.max_exact_replays > self.max_agents:
            raise ValueError("max_exact_replays must be in [0, max_agents]")
        if not math.isfinite(self.max_compute_units) or self.max_compute_units < 1.0:
            raise ValueError("max_compute_units must be finite and at least one cheap compute unit")


@dataclass(frozen=True)
class DisagreementHotspot:
    """A place where extra compute can discriminate between live hypotheses."""

    hotspot_id: str
    topic_digest: str
    disagreement: float
    evidence_gap: float
    impact: float
    candidate_digests: tuple[str, ...] = ()

    def validate(self) -> None:
        if not self.hotspot_id.strip() or not self.topic_digest.strip():
            raise ValueError("hotspot identity and topic digest must be nonempty")
        for name, value in {
            "disagreement": self.disagreement,
            "evidence_gap": self.evidence_gap,
            "impact": self.impact,
        }.items():
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be finite and in [0, 1]")

    @property
    def value(self) -> float:
        self.validate()
        # Disagreement is useful only when the question still lacks evidence and matters.
        return round(self.disagreement * self.evidence_gap * (0.5 + 0.5 * self.impact), 6)


@dataclass(frozen=True)
class CandidateCrystal:
    """A frozen local basin that is worth exact independent verification."""

    candidate_digest: str
    source_digest: str
    recipe_digest: str
    policy_digest: str
    evidence_digest: str
    coherence: float
    evidence_completeness: float
    verifier_disagreement: float
    independent_verifiers: int
    contradictions: int = 0
    frozen: bool = True

    def validate(self) -> None:
        for name, value in {
            "candidate_digest": self.candidate_digest,
            "source_digest": self.source_digest,
            "recipe_digest": self.recipe_digest,
            "policy_digest": self.policy_digest,
            "evidence_digest": self.evidence_digest,
        }.items():
            if not value.strip():
                raise ValueError(f"{name} must be nonempty")
        for name, value in {
            "coherence": self.coherence,
            "evidence_completeness": self.evidence_completeness,
            "verifier_disagreement": self.verifier_disagreement,
        }.items():
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be finite and in [0, 1]")
        if self.independent_verifiers < 0 or self.contradictions < 0:
            raise ValueError("crystal verifier counts must be non-negative")

    @property
    def verify_ready(self) -> bool:
        self.validate()
        return (
            self.frozen
            and self.coherence >= 0.85
            and self.evidence_completeness >= 0.80
            and self.verifier_disagreement <= 0.25
            and self.contradictions == 0
        )

    @property
    def authority_request_ready(self) -> bool:
        """Still only a request precondition; this property grants no authority."""

        return (
            self.verify_ready
            and self.evidence_completeness >= 0.95
            and self.verifier_disagreement <= 0.10
            and self.independent_verifiers >= 2
        )

    def invariant_digest(self) -> str:
        self.validate()
        return _digest(
            {
                "candidate_digest": self.candidate_digest,
                "source_digest": self.source_digest,
                "recipe_digest": self.recipe_digest,
                "policy_digest": self.policy_digest,
                "evidence_digest": self.evidence_digest,
                "frozen": self.frozen,
            }
        )


@dataclass(frozen=True)
class SwarmObservation:
    """Search-state measurements used for population control."""

    effective_independent_search: float
    mean_correlation: float
    novelty: float
    verifier_disagreement: float
    evidence_completeness: float
    resource_pressure: float
    context_pressure: float
    branching_ratio: float
    progress_rate: float
    expected_information: float

    def validate(self) -> None:
        if not math.isfinite(self.effective_independent_search) or self.effective_independent_search < 0.0:
            raise ValueError("effective_independent_search must be finite and non-negative")
        for name, value in {
            "mean_correlation": self.mean_correlation,
            "novelty": self.novelty,
            "verifier_disagreement": self.verifier_disagreement,
            "evidence_completeness": self.evidence_completeness,
            "resource_pressure": self.resource_pressure,
            "context_pressure": self.context_pressure,
            "progress_rate": self.progress_rate,
            "expected_information": self.expected_information,
        }.items():
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be finite and in [0, 1]")
        if not math.isfinite(self.branching_ratio) or not 0.0 <= self.branching_ratio <= 4.0:
            raise ValueError("branching_ratio must be finite and in [0, 4]")


@dataclass(frozen=True)
class PopulationLane:
    role: AgentRole
    compute_tier: ComputeTier
    count: int
    context: ContextPolicy
    temperature: float
    independent_verification: bool
    diversity_axes: tuple[str, ...]
    focus_hotspots: tuple[str, ...] = ()

    def validate(self) -> None:
        if self.count < 0:
            raise ValueError("population lane count must be non-negative")
        if not math.isfinite(self.temperature) or not 0.0 <= self.temperature <= 2.0:
            raise ValueError("lane temperature must be finite and in [0, 2]")
        if self.compute_tier == ComputeTier.EXACT_REPLAY and self.temperature != 0.0:
            raise ValueError("exact replay must have zero stochastic temperature")
        if self.independent_verification and self.role not in {AgentRole.VERIFIER, AgentRole.RED_TEAM}:
            raise ValueError("only verifier/red-team lanes may claim independent verification")


@dataclass(frozen=True)
class SearchProvenance:
    """Replayable explanation for why a candidate question was spawned."""

    posture: str
    phase: str
    plan_digest: str
    blackboard_digest: str | None = None
    artifact_digests: tuple[str, ...] = ()
    law_digests: tuple[str, ...] = ()
    hotspot_ids: tuple[str, ...] = ()
    authority: str = SWARM_DYNAMICS_AUTHORITY
    schema_version: int = SWARM_DYNAMICS_SCHEMA_VERSION

    def validate(self) -> None:
        if self.schema_version != SWARM_DYNAMICS_SCHEMA_VERSION:
            raise ValueError("unsupported search provenance schema")
        if self.authority != SWARM_DYNAMICS_AUTHORITY:
            raise ValueError("search provenance must be search-only")
        if not self.posture.strip() or not self.phase.strip() or not self.plan_digest.strip():
            raise ValueError("search provenance posture, phase and plan digest must be nonempty")

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema_version": self.schema_version,
            "authority": self.authority,
            "posture": self.posture,
            "phase": self.phase,
            "plan_digest": self.plan_digest,
            "blackboard_digest": self.blackboard_digest,
            "artifact_digests": list(self.artifact_digests),
            "law_digests": list(self.law_digests),
            "hotspot_ids": list(self.hotspot_ids),
        }

    def digest(self) -> str:
        return _digest(self.canonical_dict())


@dataclass(frozen=True)
class SwarmPlan:
    """Bounded search plan. Airflow remains responsible for scheduling it."""

    phase: str
    mode: str
    lanes: tuple[PopulationLane, ...]
    selected_hotspots: tuple[DisagreementHotspot, ...]
    crystals_to_verify: tuple[CandidateCrystal, ...]
    stop_new_work: bool
    estimated_compute_units: float
    reason: str
    authority: str = SWARM_DYNAMICS_AUTHORITY
    schema_version: int = SWARM_DYNAMICS_SCHEMA_VERSION

    def validate(self, budget: SwarmBudget) -> None:
        budget.validate()
        if self.schema_version != SWARM_DYNAMICS_SCHEMA_VERSION:
            raise ValueError("unsupported swarm plan schema")
        if self.authority != SWARM_DYNAMICS_AUTHORITY:
            raise ValueError("swarm plans must be search-only")
        if not self.phase.strip() or not self.mode.strip() or not self.reason.strip():
            raise ValueError("swarm plan phase, mode and reason must be nonempty")
        for lane in self.lanes:
            lane.validate()
        for hotspot in self.selected_hotspots:
            hotspot.validate()
        for crystal in self.crystals_to_verify:
            crystal.validate()
        total = sum(lane.count for lane in self.lanes)
        deep_tiers = {ComputeTier.DEEP, ComputeTier.EXACT_REPLAY}
        deep = sum(lane.count for lane in self.lanes if lane.compute_tier in deep_tiers)
        exact = sum(lane.count for lane in self.lanes if lane.compute_tier == ComputeTier.EXACT_REPLAY)
        if total > budget.max_agents:
            raise ValueError("swarm plan exceeds max_agents")
        if deep - exact > budget.max_deep_agents:
            raise ValueError("swarm plan exceeds max_deep_agents")
        if exact > budget.max_exact_replays:
            raise ValueError("swarm plan exceeds max_exact_replays")
        if self.estimated_compute_units > budget.max_compute_units + 1e-9:
            raise ValueError("swarm plan exceeds compute-unit budget")
        if self.stop_new_work and any(
            lane.role in {AgentRole.EXPLORER, AgentRole.MUTATOR} and lane.count > 0 for lane in self.lanes
        ):
            raise ValueError("stop_new_work plan cannot spawn explorer/mutator lanes")

    def canonical_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "authority": self.authority,
            "phase": self.phase,
            "mode": self.mode,
            "lanes": [
                {
                    **asdict(lane),
                    "role": lane.role.value,
                    "compute_tier": lane.compute_tier.value,
                    "context": lane.context.value,
                }
                for lane in self.lanes
            ],
            "selected_hotspots": [
                {
                    **asdict(hotspot),
                    "value": hotspot.value,
                }
                for hotspot in self.selected_hotspots
            ],
            "crystals_to_verify": [
                {
                    **asdict(crystal),
                    "verify_ready": crystal.verify_ready,
                    "authority_request_ready": crystal.authority_request_ready,
                    "invariant_digest": crystal.invariant_digest(),
                }
                for crystal in self.crystals_to_verify
            ],
            "stop_new_work": self.stop_new_work,
            "estimated_compute_units": self.estimated_compute_units,
            "reason": self.reason,
        }

    def digest(self) -> str:
        return _digest(self.canonical_dict())

    def provenance(
        self,
        *,
        posture: str,
        blackboard_digest: str | None = None,
        artifact_digests: Iterable[str] = (),
        law_digests: Iterable[str] = (),
    ) -> SearchProvenance:
        return SearchProvenance(
            posture=posture,
            phase=self.phase,
            plan_digest=self.digest(),
            blackboard_digest=blackboard_digest,
            artifact_digests=tuple(artifact_digests),
            law_digests=tuple(law_digests),
            hotspot_ids=tuple(hotspot.hotspot_id for hotspot in self.selected_hotspots),
        )


def effective_independent_search(signatures: Sequence[Iterable[str]]) -> float:
    """Estimate effective independent trajectories instead of counting agents.

    Each behavior signature is a set of retained decisions/observations. Perfectly
    duplicated trajectories contribute almost one effective search lane; diverse
    trajectories approach the raw population count.
    """

    sets = [set(signature) for signature in signatures]
    if not sets:
        return 0.0
    if len(sets) == 1:
        return 1.0
    correlations: list[float] = []
    for index, left in enumerate(sets):
        for right in sets[index + 1 :]:
            if not left and not right:
                correlations.append(1.0)
            elif not left or not right:
                correlations.append(0.0)
            else:
                correlations.append(len(left & right) / len(left | right))
    mean = sum(correlations) / len(correlations)
    # A simple participation-ratio analogue: N discounted by shared correlation.
    n = float(len(sets))
    return round(n / (1.0 + (n - 1.0) * mean), 6)


def mean_pairwise_correlation(signatures: Sequence[Iterable[str]]) -> float:
    sets = [set(signature) for signature in signatures]
    if len(sets) < 2:
        return 0.0
    values: list[float] = []
    for index, left in enumerate(sets):
        for right in sets[index + 1 :]:
            if not left and not right:
                values.append(1.0)
            elif not left or not right:
                values.append(0.0)
            else:
                values.append(len(left & right) / len(left | right))
    return round(sum(values) / len(values), 6)


def prioritize_hotspots(
    hotspots: Sequence[DisagreementHotspot],
    *,
    limit: int,
) -> tuple[DisagreementHotspot, ...]:
    if limit < 0:
        raise ValueError("hotspot limit must be non-negative")
    for hotspot in hotspots:
        hotspot.validate()
    return tuple(sorted(hotspots, key=lambda item: (-item.value, item.hotspot_id))[:limit])


def allocate_population(
    assessment: PhaseAssessment,
    observation: SwarmObservation,
    *,
    budget: SwarmBudget | None = None,
    hotspots: Sequence[DisagreementHotspot] = (),
    crystals: Sequence[CandidateCrystal] = (),
) -> SwarmPlan:
    """Map phase + information state into a bounded heterogeneous population.

    Deep compute is not spread uniformly. It is reserved for disagreement hotspots
    and frozen candidate verification. Cheap compute is used to create diversity.
    """

    observation.validate()
    budget = budget or SwarmBudget()
    budget.validate()

    # Resource pressure shrinks the search envelope before the factory reaches a jammed regime.
    # The caller's budget remains the hard outer ceiling; this inner envelope may only narrow it.
    resource_factor = max(0.25, 1.0 - 0.75 * observation.resource_pressure)
    available_agents = max(1, int(round(budget.max_agents * resource_factor)))
    planning_budget = SwarmBudget(
        max_agents=min(budget.max_agents, available_agents),
        max_parallel=min(budget.max_parallel, available_agents),
        max_deep_agents=min(budget.max_deep_agents, available_agents),
        max_exact_replays=min(budget.max_exact_replays, available_agents),
        max_compute_units=max(1.0, budget.max_compute_units * resource_factor),
    )
    planning_budget.validate()

    selected_hotspots = prioritize_hotspots(
        hotspots,
        limit=max(0, planning_budget.max_deep_agents),
    )
    ready_crystals = tuple(
        sorted(
            (crystal for crystal in crystals if crystal.verify_ready),
            key=lambda item: (
                item.verifier_disagreement,
                -item.evidence_completeness,
                item.candidate_digest,
            ),
        )[: planning_budget.max_exact_replays]
    )

    mode = assessment.recommendation.mode
    phase = assessment.phase
    axes = ("strategy", "model", "prompt", "runtime", "context", "mutation")
    lanes: list[PopulationLane] = []
    stop_new_work = False

    # Correlated populations do not deserve more copies. A low effective population increases
    # pressure to spend the remaining envelope on genuinely different search coordinates.
    independence_ratio = min(
        1.0,
        observation.effective_independent_search / max(1.0, float(planning_budget.max_agents)),
    )
    independence_deficit = 1.0 - independence_ratio
    collapse_pressure = max(
        observation.mean_correlation,
        1.0 - observation.novelty,
        independence_deficit,
    )
    cheap_width = max(
        1,
        int(round(planning_budget.max_agents * (0.45 + 0.35 * collapse_pressure))),
    )
    cheap_width = min(planning_budget.max_agents, cheap_width)

    if mode == ControlMode.DIVERGE:
        explorer = max(1, int(round(cheap_width * 0.70)))
        mutator = max(1, cheap_width - explorer)
        lanes.extend(
            [
                PopulationLane(
                    AgentRole.EXPLORER,
                    ComputeTier.CHEAP,
                    explorer,
                    ContextPolicy.FRESH,
                    1.25,
                    False,
                    axes,
                ),
                PopulationLane(
                    AgentRole.MUTATOR,
                    ComputeTier.CHEAP,
                    mutator,
                    ContextPolicy.FRESH,
                    1.10,
                    False,
                    ("strategy", "mutation", "prompt", "context"),
                ),
            ]
        )
        if planning_budget.max_agents - cheap_width > 0:
            lanes.append(
                PopulationLane(
                    AgentRole.CRITIC,
                    ComputeTier.STANDARD,
                    min(2, planning_budget.max_agents - cheap_width),
                    ContextPolicy.COMPACT,
                    0.35,
                    False,
                    ("role", "model"),
                )
            )
        reason = "gas/diverge: maximize independent search; correlated trajectories buy diversity, not copies"

    elif mode == ControlMode.COORDINATE:
        total = min(
            planning_budget.max_agents,
            max(4, int(round(planning_budget.max_agents * 0.65))),
        )
        explore = max(1, int(round(total * 0.40)))
        synth = max(1, int(round(total * 0.20)))
        critic = max(1, int(round(total * 0.20)))
        verifier = max(1, total - explore - synth - critic)
        lanes.extend(
            [
                PopulationLane(
                    AgentRole.EXPLORER,
                    ComputeTier.CHEAP,
                    explore,
                    ContextPolicy.INHERIT,
                    0.95,
                    False,
                    axes,
                ),
                PopulationLane(
                    AgentRole.SYNTHESIZER,
                    ComputeTier.STANDARD,
                    synth,
                    ContextPolicy.COMPACT,
                    0.30,
                    False,
                    ("role", "model"),
                ),
                PopulationLane(
                    AgentRole.CRITIC,
                    ComputeTier.STANDARD,
                    critic,
                    ContextPolicy.COMPACT,
                    0.25,
                    False,
                    ("role", "model", "verifier"),
                ),
                PopulationLane(
                    AgentRole.VERIFIER,
                    ComputeTier.STANDARD,
                    verifier,
                    ContextPolicy.FRESH,
                    0.0,
                    True,
                    ("model", "runtime", "verifier"),
                ),
            ]
        )
        reason = "liquid/coordinate: preserve motion while coupling candidate worlds through compact evidence"

    elif mode in {ControlMode.MEASURE, ControlMode.ANNEAL}:
        deep = min(planning_budget.max_deep_agents, max(1, len(selected_hotspots)))
        standard = min(max(2, planning_budget.max_agents - deep), max(2, planning_budget.max_parallel))
        lanes.extend(
            [
                # Measurement is the point of this regime. Put the independent verifier first so
                # a severely narrowed resource envelope drops commentary before it drops evidence.
                PopulationLane(
                    AgentRole.VERIFIER,
                    ComputeTier.DEEP if deep else ComputeTier.STANDARD,
                    deep if deep else max(1, standard // 2),
                    ContextPolicy.FRESH,
                    0.0,
                    True,
                    ("model", "runtime", "verifier"),
                    tuple(item.hotspot_id for item in selected_hotspots),
                ),
                PopulationLane(
                    AgentRole.CRITIC,
                    ComputeTier.STANDARD,
                    max(1, standard // 2),
                    ContextPolicy.COMPACT,
                    0.15,
                    False,
                    ("model", "role", "verifier"),
                    tuple(item.hotspot_id for item in selected_hotspots),
                ),
                PopulationLane(
                    AgentRole.RED_TEAM,
                    ComputeTier.STANDARD,
                    max(1, standard // 3),
                    ContextPolicy.FRESH,
                    0.20,
                    True,
                    ("model", "role", "attack-surface"),
                    tuple(item.hotspot_id for item in selected_hotspots),
                ),
            ]
        )
        reason = "critical/anneal: concentrate expensive reasoning on disagreements that can change the decision"

    elif mode == ControlMode.VERIFY:
        exact = min(planning_budget.max_exact_replays, len(ready_crystals))
        deep = min(planning_budget.max_deep_agents, max(1, len(ready_crystals)))
        if exact:
            lanes.append(
                PopulationLane(
                    AgentRole.VERIFIER,
                    ComputeTier.EXACT_REPLAY,
                    exact,
                    ContextPolicy.FROZEN,
                    0.0,
                    True,
                    ("runtime", "verifier"),
                )
            )
        if deep:
            lanes.append(
                PopulationLane(
                    AgentRole.RED_TEAM,
                    ComputeTier.DEEP,
                    deep,
                    ContextPolicy.FRESH,
                    0.0,
                    True,
                    ("model", "runtime", "verifier", "attack-surface"),
                )
            )
        reason = "crystal/verify: stop broad search and spend compute proving or breaking exact frozen candidates"

    elif mode == ControlMode.PERTURB:
        total = min(
            planning_budget.max_agents,
            max(3, int(round(planning_budget.max_agents * 0.50))),
        )
        lanes.extend(
            [
                PopulationLane(
                    AgentRole.EXPLORER,
                    ComputeTier.CHEAP,
                    max(2, total // 2),
                    ContextPolicy.FRESH,
                    1.50,
                    False,
                    axes,
                ),
                PopulationLane(
                    AgentRole.MUTATOR,
                    ComputeTier.CHEAP,
                    max(1, total // 3),
                    ContextPolicy.FRESH,
                    1.35,
                    False,
                    ("strategy", "mutation", "runtime", "context"),
                ),
                PopulationLane(
                    AgentRole.CRITIC,
                    ComputeTier.STANDARD,
                    1,
                    ContextPolicy.FRESH,
                    0.25,
                    False,
                    ("role", "model"),
                ),
            ]
        )
        reason = (
            "glass/perturb: abandon correlated context and inject new coordinates instead of thinking harder in place"
        )

    else:  # DRAIN
        stop_new_work = True
        lanes.extend(
            [
                PopulationLane(
                    AgentRole.MEMORY,
                    ComputeTier.CHEAP,
                    1,
                    ContextPolicy.COMPACT,
                    0.0,
                    False,
                    ("evidence",),
                ),
                PopulationLane(
                    AgentRole.VERIFIER,
                    ComputeTier.STANDARD,
                    min(2, planning_budget.max_agents - 1),
                    ContextPolicy.FROZEN,
                    0.0,
                    True,
                    ("runtime", "verifier"),
                ),
            ]
        )
        reason = "jammed/drain: stop feeding the queue; consolidate evidence, finish verification and reclaim debt"

    lanes = _fit_budget(tuple(lanes), planning_budget)
    compute = round(sum(_lane_compute_units(lane) for lane in lanes), 6)
    plan = SwarmPlan(
        phase=phase,
        mode=mode.value,
        lanes=lanes,
        selected_hotspots=selected_hotspots,
        crystals_to_verify=ready_crystals,
        stop_new_work=stop_new_work,
        estimated_compute_units=compute,
        reason=(
            f"{reason}; effective-independent-search={observation.effective_independent_search:.3f}, "
            f"correlation={observation.mean_correlation:.3f}, "
            f"independence-ratio={independence_ratio:.3f}, "
            f"resource-cap={planning_budget.max_agents}/{budget.max_agents}"
        ),
    )
    plan.validate(budget)
    return plan


def observation_from_signatures(
    signatures: Sequence[Iterable[str]],
    *,
    novelty: float,
    verifier_disagreement: float,
    evidence_completeness: float,
    resource_pressure: float,
    context_pressure: float,
    branching_ratio: float,
    progress_rate: float,
    expected_information: float,
) -> SwarmObservation:
    return SwarmObservation(
        effective_independent_search=effective_independent_search(signatures),
        mean_correlation=mean_pairwise_correlation(signatures),
        novelty=novelty,
        verifier_disagreement=verifier_disagreement,
        evidence_completeness=evidence_completeness,
        resource_pressure=resource_pressure,
        context_pressure=context_pressure,
        branching_ratio=branching_ratio,
        progress_rate=progress_rate,
        expected_information=expected_information,
    )


def hotspot_from_disagreement(
    hotspot_id: str,
    payload: Mapping[str, Any],
    *,
    disagreement: float,
    evidence_gap: float,
    impact: float,
    candidate_digests: Sequence[str] = (),
) -> DisagreementHotspot:
    return DisagreementHotspot(
        hotspot_id=hotspot_id,
        topic_digest=_digest(payload),
        disagreement=disagreement,
        evidence_gap=evidence_gap,
        impact=impact,
        candidate_digests=tuple(candidate_digests),
    )


def _fit_budget(lanes: tuple[PopulationLane, ...], budget: SwarmBudget) -> tuple[PopulationLane, ...]:
    """Deterministically trim lanes without changing their semantic order."""

    remaining_agents = budget.max_agents
    remaining_deep = budget.max_deep_agents
    remaining_exact = budget.max_exact_replays
    remaining_compute = budget.max_compute_units
    fitted: list[PopulationLane] = []

    for lane in lanes:
        max_count = min(lane.count, remaining_agents)
        if lane.compute_tier == ComputeTier.DEEP:
            max_count = min(max_count, remaining_deep)
        elif lane.compute_tier == ComputeTier.EXACT_REPLAY:
            max_count = min(max_count, remaining_exact)
        per = _tier_cost(lane.compute_tier)
        max_count = min(max_count, int(remaining_compute // per))
        if max_count <= 0:
            continue
        fitted_lane = PopulationLane(
            role=lane.role,
            compute_tier=lane.compute_tier,
            count=max_count,
            context=lane.context,
            temperature=lane.temperature,
            independent_verification=lane.independent_verification,
            diversity_axes=lane.diversity_axes,
            focus_hotspots=lane.focus_hotspots,
        )
        fitted.append(fitted_lane)
        remaining_agents -= max_count
        remaining_compute -= max_count * per
        if lane.compute_tier == ComputeTier.DEEP:
            remaining_deep -= max_count
        elif lane.compute_tier == ComputeTier.EXACT_REPLAY:
            remaining_exact -= max_count

    if not fitted:
        fitted.append(
            PopulationLane(
                AgentRole.MEMORY,
                ComputeTier.CHEAP,
                1,
                ContextPolicy.COMPACT,
                0.0,
                False,
                ("evidence",),
            )
        )
    return tuple(fitted)


def _tier_cost(tier: ComputeTier) -> float:
    return {
        ComputeTier.CHEAP: 1.0,
        ComputeTier.STANDARD: 2.0,
        ComputeTier.DEEP: 8.0,
        ComputeTier.EXACT_REPLAY: 3.0,
    }[tier]


def _lane_compute_units(lane: PopulationLane) -> float:
    return lane.count * _tier_cost(lane.compute_tier)


def _digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return "sha256:" + hashlib.sha256(payload).hexdigest()
