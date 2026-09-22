"""Recursive search over the factory's own experiment process.

This is the search-side counterpart to deterministic promotion.

A normal candidate campaign explores implementations. Recursive search also observes how the
campaign behaved, compresses repeated outcomes into small search laws, and uses those laws to
reshape the next campaign. The shared medium between rounds is content-addressed artifacts rather
than hidden agent conversation.

Nothing in this module grants authority. It may choose which declared search strategy runs next,
how much sibling width to spend, and how much parallelism to expose. It may not relax evidence,
approve a gate, publish, merge, mint credentials, or promote a candidate.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from swfactory.evolution import (
    DEFAULT_STRATEGIES,
    REQUIRED_DIMENSIONS,
    CampaignError,
    CampaignReport,
    CandidateOutcome,
    CandidateRunner,
    Strategy,
    plan_requests,
    run_campaign,
)
from swfactory.generations import CampaignBudget, Dimension
from swfactory.phase_control import Phase, PhaseObservation, assess
from swfactory.swarm_dynamics import (
    DisagreementHotspot,
    SwarmBudget,
    SwarmObservation,
    allocate_population,
)
from swfactory.work_executor import Cancellation

RECURSIVE_SEARCH_SCHEMA_VERSION = 1
RECURSIVE_SEARCH_AUTHORITY = "exploration-only"


class SearchPosture(StrEnum):
    DIVERSIFY = "diversify"
    COORDINATE = "coordinate"
    EXPLOIT = "exploit"
    MEASURE = "measure"
    VERIFY = "verify"
    PERTURB = "perturb"
    STOP = "stop"


class ArtifactKind(StrEnum):
    HYPOTHESIS = "hypothesis"
    FAILURE = "failure"
    EVIDENCE = "evidence"
    COUNTEREXAMPLE = "counterexample"
    LAW = "law"
    VERIFIER = "verifier"


class LawKind(StrEnum):
    PREFER = "prefer"
    DEPRIORITIZE = "deprioritize"
    DIVERSIFY = "diversify"
    MEASURE = "measure"
    VERIFY = "verify"
    PERTURB = "perturb"


@dataclass(frozen=True)
class ResearchArtifact:
    """One content-addressed fact shared between otherwise independent trajectories."""

    artifact_id: str
    kind: ArtifactKind
    producer: str
    payload_digest: str
    candidate_id: str | None = None
    parents: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    weight: float = 1.0

    def validate(self) -> None:
        for name, value in {
            "artifact_id": self.artifact_id,
            "producer": self.producer,
            "payload_digest": self.payload_digest,
        }.items():
            if not value.strip():
                raise CampaignError(f"research artifact {name} must be nonempty")
        if not math.isfinite(self.weight) or self.weight < 0.0:
            raise CampaignError("research artifact weight must be finite and non-negative")

    def digest(self) -> str:
        self.validate()
        return _digest(
            {
                "artifact_id": self.artifact_id,
                "kind": self.kind.value,
                "producer": self.producer,
                "payload_digest": self.payload_digest,
                "candidate_id": self.candidate_id,
                "parents": list(self.parents),
                "tags": list(self.tags),
                "weight": self.weight,
            }
        )


@dataclass(frozen=True)
class ArtifactBlackboard:
    """Immutable artifact-mediated coordination surface for the swarm."""

    artifacts: tuple[ResearchArtifact, ...] = ()

    def validate(self) -> None:
        ids: set[str] = set()
        for artifact in self.artifacts:
            artifact.validate()
            if artifact.artifact_id in ids:
                raise CampaignError(f"duplicate research artifact {artifact.artifact_id}")
            ids.add(artifact.artifact_id)
        for artifact in self.artifacts:
            unknown = set(artifact.parents) - ids
            if unknown:
                raise CampaignError(
                    f"{artifact.artifact_id}: unknown parent artifacts {', '.join(sorted(unknown))}"
                )

    def digest(self) -> str:
        self.validate()
        return _digest([artifact.digest() for artifact in self.artifacts])

    def append(self, additions: Iterable[ResearchArtifact]) -> ArtifactBlackboard:
        merged = {artifact.artifact_id: artifact for artifact in self.artifacts}
        for artifact in additions:
            artifact.validate()
            incumbent = merged.get(artifact.artifact_id)
            if incumbent is not None and incumbent != artifact:
                raise CampaignError(f"artifact identity collision for {artifact.artifact_id}")
            merged[artifact.artifact_id] = artifact
        result = ArtifactBlackboard(tuple(merged[key] for key in sorted(merged)))
        result.validate()
        return result

    def select(self, *, tags: Iterable[str] = (), limit: int = 16) -> tuple[ResearchArtifact, ...]:
        required = set(tags)
        values = [
            artifact
            for artifact in self.artifacts
            if not required or required.intersection(artifact.tags)
        ]
        return tuple(
            sorted(values, key=lambda item: (-item.weight, item.artifact_id))[: max(0, limit)]
        )

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema_version": RECURSIVE_SEARCH_SCHEMA_VERSION,
            "authority": RECURSIVE_SEARCH_AUTHORITY,
            "digest": self.digest(),
            "artifacts": [_artifact_dict(artifact) for artifact in self.artifacts],
        }


def artifact_from_payload(
    *,
    kind: ArtifactKind,
    producer: str,
    payload: object,
    candidate_id: str | None = None,
    parents: Iterable[str] = (),
    tags: Iterable[str] = (),
    weight: float = 1.0,
) -> ResearchArtifact:
    """Turn a JSON-like payload into an immutable blackboard artifact."""

    payload_digest = _digest(payload)
    identity = _digest(
        {
            "kind": kind.value,
            "producer": producer,
            "payload_digest": payload_digest,
            "candidate_id": candidate_id,
            "parents": sorted(set(parents)),
            "tags": sorted(set(tags)),
        }
    )
    return ResearchArtifact(
        artifact_id=f"artifact_{identity.removeprefix('sha256:')[:24]}",
        kind=kind,
        producer=producer,
        payload_digest=payload_digest,
        candidate_id=candidate_id,
        parents=tuple(sorted(set(parents))),
        tags=tuple(sorted(set(tags))),
        weight=weight,
    )


def write_blackboard(path: Path, blackboard: ArtifactBlackboard) -> str:
    """Persist the artifact-mediated coordination surface without any authority material."""

    document = blackboard.to_dict()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return str(document["digest"])


def load_blackboard(path: Path) -> ArtifactBlackboard:
    """Load a blackboard and refuse any digest or authority drift."""

    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise CampaignError("recursive blackboard must be a JSON object")
    if int(raw.get("schema_version", -1)) != RECURSIVE_SEARCH_SCHEMA_VERSION:
        raise CampaignError("unsupported recursive blackboard schema")
    if raw.get("authority") != RECURSIVE_SEARCH_AUTHORITY:
        raise CampaignError("recursive blackboard is not exploration-only")
    rows = raw.get("artifacts")
    if not isinstance(rows, list):
        raise CampaignError("recursive blackboard has no artifacts array")
    artifacts: list[ResearchArtifact] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise CampaignError("recursive blackboard artifact must be an object")
        artifact = ResearchArtifact(
            artifact_id=str(row["artifact_id"]),
            kind=ArtifactKind(str(row["kind"])),
            producer=str(row["producer"]),
            payload_digest=str(row["payload_digest"]),
            candidate_id=(str(row["candidate_id"]) if row.get("candidate_id") is not None else None),
            parents=tuple(str(value) for value in row.get("parents", ())),
            tags=tuple(str(value) for value in row.get("tags", ())),
            weight=float(row.get("weight", 1.0)),
        )
        if row.get("digest") is not None and str(row["digest"]) != artifact.digest():
            raise CampaignError(f"recursive artifact {artifact.artifact_id} digest mismatch")
        artifacts.append(artifact)
    blackboard = ArtifactBlackboard(tuple(artifacts))
    blackboard.validate()
    if raw.get("digest") is not None and str(raw["digest"]) != blackboard.digest():
        raise CampaignError("recursive blackboard digest mismatch")
    return blackboard


@dataclass(frozen=True)
class StrategySignal:
    strategy: Strategy
    attempts: int
    answered: int
    evidence_complete: int
    required_passes: int
    unique_outputs: int
    cost_usd: float

    @property
    def answer_rate(self) -> float:
        return _ratio(self.answered, self.attempts)

    @property
    def evidence_rate(self) -> float:
        return _ratio(self.evidence_complete, self.attempts)

    @property
    def required_pass_rate(self) -> float:
        return _ratio(self.required_passes, self.attempts)

    @property
    def novelty_rate(self) -> float:
        return _ratio(self.unique_outputs, self.answered)

    @property
    def cost_per_answer(self) -> float:
        return self.cost_usd / self.answered if self.answered else self.cost_usd


@dataclass(frozen=True)
class RoundSignal:
    campaign_id: str
    depth: int
    attempts: int
    answered: int
    evidence_complete: int
    required_passes: int
    unique_outputs: int
    disagreement: float
    novelty: float
    cost_usd: float
    winner_strategy: Strategy | None
    strategies: tuple[StrategySignal, ...]

    @property
    def evidence_rate(self) -> float:
        return _ratio(self.evidence_complete, self.attempts)

    @property
    def required_pass_rate(self) -> float:
        return _ratio(self.required_passes, self.attempts)


@dataclass(frozen=True)
class SearchLaw:
    """A compact rule extracted from repeated executable experiments."""

    law_id: str
    kind: LawKind
    statement: str
    confidence: float
    support: int
    strategies: tuple[Strategy, ...] = ()
    evidence_digests: tuple[str, ...] = ()
    authority: str = RECURSIVE_SEARCH_AUTHORITY

    def validate(self) -> None:
        if not self.law_id.strip() or not self.statement.strip():
            raise CampaignError("search law identity and statement must be nonempty")
        if not math.isfinite(self.confidence) or not 0.0 <= self.confidence <= 1.0:
            raise CampaignError("search law confidence must be finite and in [0, 1]")
        if self.support < 1:
            raise CampaignError("search law support must be positive")
        if self.authority != RECURSIVE_SEARCH_AUTHORITY:
            raise CampaignError("search laws are exploration-only")

    def digest(self) -> str:
        self.validate()
        return _digest(
            {
                "law_id": self.law_id,
                "kind": self.kind.value,
                "statement": self.statement,
                "confidence": self.confidence,
                "support": self.support,
                "strategies": [strategy.value for strategy in self.strategies],
                "evidence_digests": list(self.evidence_digests),
                "authority": self.authority,
            }
        )


@dataclass(frozen=True)
class RecursiveRoundPlan:
    depth: int
    input_head: str
    posture: SearchPosture
    strategies: tuple[Strategy, ...]
    max_parallel: int
    artifact_digests: tuple[str, ...]
    law_digests: tuple[str, ...]
    reason: str
    phase: str | None = None
    phase_assessment_digest: str | None = None
    swarm_plan_digest: str | None = None
    search_provenance_digest: str | None = None
    estimated_compute_units: float = 0.0
    authority: str = RECURSIVE_SEARCH_AUTHORITY

    def validate(self) -> None:
        if self.depth < 0:
            raise CampaignError("recursive round depth must be nonnegative")
        if not self.input_head.strip():
            raise CampaignError("recursive round input head must be nonempty")
        if self.posture != SearchPosture.STOP and not self.strategies:
            raise CampaignError("a live recursive round needs at least one strategy")
        if len(set(self.strategies)) != len(self.strategies):
            raise CampaignError("recursive round strategies must be distinct")
        if self.max_parallel < 1:
            raise CampaignError("recursive round max_parallel must be positive")
        if self.authority != RECURSIVE_SEARCH_AUTHORITY:
            raise CampaignError("recursive search plans are exploration-only")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema_version": RECURSIVE_SEARCH_SCHEMA_VERSION,
            "authority": self.authority,
            "depth": self.depth,
            "input_head": self.input_head,
            "posture": self.posture.value,
            "strategies": [strategy.value for strategy in self.strategies],
            "max_parallel": self.max_parallel,
            "artifact_digests": list(self.artifact_digests),
            "law_digests": list(self.law_digests),
            "reason": self.reason,
            "phase": self.phase,
            "phase_assessment_digest": self.phase_assessment_digest,
            "swarm_plan_digest": self.swarm_plan_digest,
            "search_provenance_digest": self.search_provenance_digest,
            "estimated_compute_units": self.estimated_compute_units,
        }

    def digest(self) -> str:
        return _digest(self.to_dict())


@dataclass(frozen=True)
class RecursiveSearchReport:
    loop_id: str
    cell_id: str
    epoch: int
    root_head: str
    rounds: tuple[CampaignReport, ...]
    plans: tuple[RecursiveRoundPlan, ...]
    signals: tuple[RoundSignal, ...]
    laws: tuple[SearchLaw, ...]
    blackboard: ArtifactBlackboard
    stop_reason: str
    total_cost_usd: float
    total_wall_s: int
    authority: str = RECURSIVE_SEARCH_AUTHORITY

    @property
    def exploration_winner(self) -> CandidateOutcome | None:
        if not self.rounds:
            return None
        winner_id = self.rounds[-1].exploration_selection.winner
        if winner_id is None:
            return None
        return next(
            (outcome for outcome in self.rounds[-1].outcomes if outcome.logical_id == winner_id),
            None,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": RECURSIVE_SEARCH_SCHEMA_VERSION,
            "authority": self.authority,
            "scheduler": "airflow",
            "loop_id": self.loop_id,
            "cell_id": self.cell_id,
            "epoch": self.epoch,
            "root_head": self.root_head,
            "stop_reason": self.stop_reason,
            "total_cost_usd": self.total_cost_usd,
            "total_wall_s": self.total_wall_s,
            "plans": [plan.to_dict() for plan in self.plans],
            "signals": [_signal_dict(signal) for signal in self.signals],
            "laws": [_law_dict(law) for law in self.laws],
            "blackboard_digest": self.blackboard.digest(),
            "artifacts": [_artifact_dict(artifact) for artifact in self.blackboard.artifacts],
            "rounds": [report.to_dict() for report in self.rounds],
        }


def campaign_signal(
    report: CampaignReport,
    *,
    required: Iterable[Dimension] = REQUIRED_DIMENSIONS,
) -> RoundSignal:
    """Reduce one full campaign into order parameters for the next search round."""

    required_set = frozenset(required)
    strategy_rows: dict[Strategy, list[CandidateOutcome]] = defaultdict(list)
    answered: list[CandidateOutcome] = []
    evidence_complete: list[CandidateOutcome] = []
    required_passes: list[CandidateOutcome] = []

    for outcome in report.outcomes:
        strategy_rows[outcome.strategy].append(outcome)
        if _answered(outcome):
            answered.append(outcome)
        if _answered(outcome) and outcome.evidence_digest:
            evidence_complete.append(outcome)
        if _answered(outcome) and required_set.issubset(outcome.passed):
            required_passes.append(outcome)

    unique_outputs = {outcome.output_head for outcome in answered if outcome.output_head}
    disagreement = (
        0.0
        if len(answered) < 2
        else (len(unique_outputs) - 1) / max(1, len(answered) - 1)
    )
    novelty = _ratio(len(unique_outputs), len(answered))

    winner_strategy: Strategy | None = None
    winner_id = report.exploration_selection.winner
    if winner_id is not None:
        winner = next((outcome for outcome in report.outcomes if outcome.logical_id == winner_id), None)
        winner_strategy = winner.strategy if winner is not None else None

    strategy_signals: list[StrategySignal] = []
    for strategy in sorted(strategy_rows, key=lambda item: item.value):
        outcomes = strategy_rows[strategy]
        answered_rows = [outcome for outcome in outcomes if _answered(outcome)]
        strategy_signals.append(
            StrategySignal(
                strategy=strategy,
                attempts=len(outcomes),
                answered=len(answered_rows),
                evidence_complete=sum(1 for outcome in answered_rows if outcome.evidence_digest),
                required_passes=sum(
                    1 for outcome in answered_rows if required_set.issubset(outcome.passed)
                ),
                unique_outputs=len(
                    {outcome.output_head for outcome in answered_rows if outcome.output_head}
                ),
                cost_usd=round(sum(outcome.cost_usd for outcome in outcomes), 6),
            )
        )

    depth = report.experiment_round.depth if report.experiment_round is not None else 0
    return RoundSignal(
        campaign_id=report.campaign_id,
        depth=depth,
        attempts=len(report.outcomes),
        answered=len(answered),
        evidence_complete=len(evidence_complete),
        required_passes=len(required_passes),
        unique_outputs=len(unique_outputs),
        disagreement=round(disagreement, 6),
        novelty=round(novelty, 6),
        cost_usd=round(sum(outcome.cost_usd for outcome in report.outcomes), 6),
        winner_strategy=winner_strategy,
        strategies=tuple(strategy_signals),
    )


def extract_search_laws(
    signals: Sequence[RoundSignal],
    *,
    min_support: int = 2,
) -> tuple[SearchLaw, ...]:
    """Compress many campaign observations into a small reusable rule set.

    The rules affect only future exploration. They are intentionally lossy summaries: raw campaign
    and evidence artifacts remain the source of truth.
    """

    if min_support < 1:
        raise CampaignError("min_support must be positive")
    if not signals:
        return ()

    aggregate: dict[Strategy, dict[str, float]] = defaultdict(
        lambda: {
            "attempts": 0.0,
            "answered": 0.0,
            "evidence": 0.0,
            "required": 0.0,
            "unique": 0.0,
            "cost": 0.0,
        }
    )
    for signal in signals:
        for row in signal.strategies:
            target = aggregate[row.strategy]
            target["attempts"] += row.attempts
            target["answered"] += row.answered
            target["evidence"] += row.evidence_complete
            target["required"] += row.required_passes
            target["unique"] += row.unique_outputs
            target["cost"] += row.cost_usd

    laws: list[SearchLaw] = []
    for strategy in sorted(aggregate, key=lambda item: item.value):
        row = aggregate[strategy]
        attempts = int(row["attempts"])
        if attempts < min_support:
            continue
        required_rate = _ratio(row["required"], attempts)
        evidence_rate = _ratio(row["evidence"], attempts)
        novelty_rate = _ratio(row["unique"], row["answered"])

        if required_rate >= 0.60 and evidence_rate >= 0.60:
            confidence = min(1.0, (required_rate + evidence_rate) / 2.0)
            laws.append(
                _law(
                    LawKind.PREFER,
                    f"{strategy.value} repeatedly converts search into evidence-complete required-dimension passes",
                    confidence,
                    attempts,
                    (strategy,),
                    signals,
                )
            )
        if row["answered"] >= min_support and novelty_rate < 0.50:
            laws.append(
                _law(
                    LawKind.DEPRIORITIZE,
                    f"{strategy.value} is producing correlated outputs; spend width on a different search direction",
                    min(1.0, 1.0 - novelty_rate),
                    attempts,
                    (strategy,),
                    signals,
                )
            )

    last = signals[-1]
    if last.answered == 0:
        laws.append(
            _law(
                LawKind.PERTURB,
                "the latest search basin produced no answered world; change search coordinates before spending depth",
                1.0,
                max(1, last.attempts),
                (),
                (last,),
            )
        )
    elif last.disagreement >= 0.66 and last.required_pass_rate < 0.66:
        laws.append(
            _law(
                LawKind.MEASURE,
                "candidate worlds disagree materially; allocate the next round to discriminating evidence",
                min(1.0, last.disagreement),
                max(1, last.answered),
                (),
                (last,),
            )
        )
    elif last.evidence_rate >= 0.80 and last.required_pass_rate >= 0.80 and last.disagreement <= 0.25:
        laws.append(
            _law(
                LawKind.VERIFY,
                "search has compressed to an evidence-complete low-disagreement basin; narrow and verify exact descendants",
                min(1.0, (last.evidence_rate + last.required_pass_rate + (1.0 - last.disagreement)) / 3.0),
                max(1, last.answered),
                ((last.winner_strategy,) if last.winner_strategy is not None else ()),
                (last,),
            )
        )
    elif last.novelty < 0.50:
        laws.append(
            _law(
                LawKind.DIVERSIFY,
                "the latest population collapsed onto too few distinct outputs; restore independent search directions",
                min(1.0, 1.0 - last.novelty),
                max(1, last.answered),
                (),
                (last,),
            )
        )

    unique = {law.digest(): law for law in laws}
    return tuple(unique[key] for key in sorted(unique))


def plan_next_round(
    signals: Sequence[RoundSignal],
    *,
    depth: int,
    input_head: str,
    blackboard: ArtifactBlackboard | None = None,
    allowed_strategies: Sequence[Strategy] = DEFAULT_STRATEGIES,
    max_candidates: int = 4,
    max_parallel: int = 3,
) -> RecursiveRoundPlan:
    """Mutate the next search space from evidence about the search process itself."""

    if depth < 0:
        raise CampaignError("recursive search depth must be nonnegative")
    if max_candidates < 1 or max_parallel < 1:
        raise CampaignError("recursive search width and parallelism must be positive")
    if not allowed_strategies:
        raise CampaignError("recursive search needs at least one declared strategy")
    if len(set(allowed_strategies)) != len(allowed_strategies):
        raise CampaignError("recursive search strategies must be distinct")

    board = blackboard or ArtifactBlackboard()
    board.validate()
    laws = extract_search_laws(signals)
    scores = _strategy_scores(signals, allowed_strategies)
    ordered = tuple(sorted(allowed_strategies, key=lambda s: (-scores[s], s.value)))

    if not signals:
        posture = SearchPosture.DIVERSIFY
        width = min(max_candidates, len(ordered))
        strategies = ordered[:width]
        reason = "no search history yet: maximize independent initial hypotheses"
    else:
        last = signals[-1]
        if last.answered == 0:
            posture = SearchPosture.PERTURB
            ordered = _least_tried_first(signals, allowed_strategies)
            width = min(max_candidates, len(ordered))
            strategies = ordered[:width]
            reason = "the previous basin yielded no answer; change coordinates rather than deepen it"
        elif last.evidence_rate >= 0.80 and last.required_pass_rate >= 0.80 and last.disagreement <= 0.25:
            posture = SearchPosture.VERIFY
            winner = last.winner_strategy
            counterfactual = tuple(strategy for strategy in ordered if strategy != winner)
            preferred = ((winner,) if winner is not None else ()) + counterfactual
            width = min(max_candidates, max(1, min(2, len(preferred))))
            strategies = preferred[:width]
            reason = "high evidence with low disagreement: freeze most breadth and challenge the apparent basin"
        elif last.disagreement >= 0.66:
            posture = SearchPosture.MEASURE
            width = min(max_candidates, max(2, min(len(ordered), 3)))
            strategies = ordered[:width]
            reason = "worlds disagree: preserve competing explanations until discriminating evidence resolves them"
        elif last.novelty < 0.50:
            posture = SearchPosture.DIVERSIFY
            preferred = _novelty_first(signals, allowed_strategies)
            width = min(max_candidates, len(preferred))
            strategies = preferred[:width]
            reason = "population collapse detected: spend the next budget on less-correlated strategies"
        elif last.winner_strategy is not None:
            posture = SearchPosture.EXPLOIT
            preferred = (last.winner_strategy,) + tuple(
                strategy for strategy in ordered if strategy != last.winner_strategy
            )
            width = min(max_candidates, max(1, min(2, len(preferred))))
            strategies = preferred[:width]
            reason = "a useful basin exists: descend locally while retaining one counterfactual search direction"
        else:
            posture = SearchPosture.COORDINATE
            width = min(max_candidates, len(ordered))
            strategies = ordered[:width]
            reason = "retain broad coordination until a stable evidence-bearing basin emerges"

    if posture in {SearchPosture.VERIFY, SearchPosture.EXPLOIT}:
        parallelism = min(max_parallel, max(1, len(strategies)))
    elif posture == SearchPosture.MEASURE:
        parallelism = min(max_parallel, max(2, len(strategies)))
    else:
        parallelism = min(max_parallel, max(1, len(strategies)))

    selected_artifacts = board.select(
        tags=("evidence", "counterexample", "failure", "law"),
        limit=max(4, 2 * len(strategies)),
    )
    plan = RecursiveRoundPlan(
        depth=depth,
        input_head=input_head,
        posture=posture,
        strategies=tuple(strategies),
        max_parallel=parallelism,
        artifact_digests=tuple(artifact.digest() for artifact in selected_artifacts),
        law_digests=tuple(law.digest() for law in laws),
        reason=reason,
    )
    plan.validate()
    return plan


def plan_adaptive_round(
    signals: Sequence[RoundSignal],
    *,
    depth: int,
    input_head: str,
    blackboard: ArtifactBlackboard | None = None,
    allowed_strategies: Sequence[Strategy] = DEFAULT_STRATEGIES,
    max_candidates: int = 4,
    max_parallel: int = 3,
    previous_phase: Phase | None = None,
    resource_pressure: float = 0.0,
    context_pressure: float = 0.0,
    debt_pressure: float = 0.0,
    swarm_budget: SwarmBudget | None = None,
) -> RecursiveRoundPlan:
    """Join recursive search, phase metacognition and population allocation.

    Search determines *what* experiment to ask. Phase control determines *what kind of thinking*
    is useful. Swarm dynamics determines *where compute goes*. The resulting provenance digest is
    bound into descendant candidate identity by :func:`plan_requests`.
    """

    base = plan_next_round(
        signals,
        depth=depth,
        input_head=input_head,
        blackboard=blackboard,
        allowed_strategies=allowed_strategies,
        max_candidates=max_candidates,
        max_parallel=max_parallel,
    )
    observation = _phase_observation(
        signals,
        resource_pressure=resource_pressure,
        context_pressure=context_pressure,
        debt_pressure=debt_pressure,
    )
    phase = assess(observation, previous_phase=previous_phase)
    swarm_observation = _swarm_observation(signals, observation)
    hotspots: tuple[DisagreementHotspot, ...] = ()
    if signals and signals[-1].disagreement > 0.0:
        last = signals[-1]
        hotspots = (
            DisagreementHotspot(
                hotspot_id=f"campaign:{last.campaign_id}:disagreement",
                topic_digest=_digest(_signal_dict(last)),
                disagreement=last.disagreement,
                evidence_gap=max(0.0, 1.0 - last.evidence_rate),
                impact=max(0.5, last.required_pass_rate),
            ),
        )
    effective_budget = swarm_budget or SwarmBudget(
        max_agents=max(1, max_candidates * 4),
        max_parallel=max(1, max_parallel),
        max_deep_agents=max(1, min(max_parallel, 4)),
        max_exact_replays=min(2, max(1, max_parallel)),
        max_compute_units=max(16.0, float(max_candidates * 12)),
    )
    swarm = allocate_population(
        phase,
        swarm_observation,
        budget=effective_budget,
        hotspots=hotspots,
    )
    board = blackboard or ArtifactBlackboard()
    provenance = swarm.provenance(
        posture=base.posture.value,
        blackboard_digest=board.digest(),
        artifact_digests=base.artifact_digests,
        law_digests=base.law_digests,
    )
    active_population = max(1, sum(lane.count for lane in swarm.lanes))
    return RecursiveRoundPlan(
        depth=base.depth,
        input_head=base.input_head,
        posture=base.posture,
        strategies=base.strategies,
        max_parallel=min(base.max_parallel, active_population),
        artifact_digests=base.artifact_digests,
        law_digests=base.law_digests,
        reason=f"{base.reason}; {swarm.reason}",
        phase=phase.phase,
        phase_assessment_digest=_digest(phase.as_dict()),
        swarm_plan_digest=swarm.digest(),
        search_provenance_digest=provenance.digest(),
        estimated_compute_units=swarm.estimated_compute_units,
    )


def _phase_observation(
    signals: Sequence[RoundSignal],
    *,
    resource_pressure: float,
    context_pressure: float,
    debt_pressure: float,
) -> PhaseObservation:
    for name, value in {
        "resource_pressure": resource_pressure,
        "context_pressure": context_pressure,
        "debt_pressure": debt_pressure,
    }.items():
        if not math.isfinite(value) or not 0.0 <= value <= 1.0:
            raise CampaignError(f"{name} must be finite and in [0, 1]")

    if not signals:
        return PhaseObservation(
            candidate_entropy=1.0,
            coherence=0.0,
            mobility=1.0,
            queue_pressure=0.0,
            queue_acceleration=0.0,
            resource_pressure=resource_pressure,
            branching_ratio=0.0,
            evidence_completeness=0.0,
            context_pressure=context_pressure,
            debt_pressure=debt_pressure,
            verifier_disagreement=1.0,
        )

    last = signals[-1]
    previous = signals[-2] if len(signals) > 1 else None
    answer_rate = _ratio(last.answered, last.attempts)
    previous_answer_rate = _ratio(previous.answered, previous.attempts) if previous is not None else answer_rate
    queue_acceleration = max(-1.0, min(1.0, previous_answer_rate - answer_rate))
    failure_branching = _ratio(max(0, last.attempts - last.answered), max(1, last.attempts))
    return PhaseObservation(
        candidate_entropy=max(0.0, min(1.0, last.novelty)),
        coherence=max(0.0, min(1.0, 1.0 - last.disagreement)),
        mobility=answer_rate,
        queue_pressure=0.0,
        queue_acceleration=queue_acceleration,
        resource_pressure=resource_pressure,
        branching_ratio=min(4.0, failure_branching),
        evidence_completeness=last.evidence_rate,
        context_pressure=context_pressure,
        debt_pressure=debt_pressure,
        verifier_disagreement=last.disagreement,
    )


def _swarm_observation(
    signals: Sequence[RoundSignal],
    phase_observation: PhaseObservation,
) -> SwarmObservation:
    if not signals:
        return SwarmObservation(
            effective_independent_search=1.0,
            mean_correlation=0.0,
            novelty=1.0,
            verifier_disagreement=1.0,
            evidence_completeness=0.0,
            resource_pressure=phase_observation.resource_pressure,
            context_pressure=phase_observation.context_pressure,
            branching_ratio=phase_observation.branching_ratio,
            progress_rate=0.0,
            expected_information=1.0,
        )
    last = signals[-1]
    return SwarmObservation(
        effective_independent_search=float(max(1, last.unique_outputs)),
        mean_correlation=max(0.0, min(1.0, 1.0 - last.novelty)),
        novelty=last.novelty,
        verifier_disagreement=last.disagreement,
        evidence_completeness=last.evidence_rate,
        resource_pressure=phase_observation.resource_pressure,
        context_pressure=phase_observation.context_pressure,
        branching_ratio=phase_observation.branching_ratio,
        progress_rate=_ratio(last.answered, last.attempts),
        expected_information=max(
            0.0,
            min(1.0, last.disagreement * (1.0 - last.evidence_rate) + (1.0 - last.required_pass_rate) * 0.25),
        ),
    )


def run_recursive_search(
    runner: CandidateRunner,
    *,
    loop_id: str,
    cell_id: str,
    epoch: int,
    input_head: str,
    budget: CampaignBudget | None = None,
    strategies: Sequence[Strategy] = DEFAULT_STRATEGIES,
    max_parallel: int = 3,
    parallel: bool = True,
    human_approved: bool = False,
    required: Iterable[Dimension] = REQUIRED_DIMENSIONS,
    cancellation: Cancellation | None = None,
) -> RecursiveSearchReport:
    """Run campaigns whose next search policy is learned from prior campaign evidence."""

    budget = budget or CampaignBudget()
    cancellation = cancellation or Cancellation()
    if not loop_id.strip() or not cell_id.strip() or not input_head.strip():
        raise CampaignError("recursive search loop, cell and input identities must be nonempty")
    if epoch < 0:
        raise CampaignError("recursive search epoch must be nonnegative")
    if budget.max_depth < 0 or budget.max_candidates < 1:
        raise CampaignError("recursive search budget has invalid depth or candidate count")
    if budget.max_cost_usd < 0 or budget.max_wall_s < 0:
        raise CampaignError("recursive search budget has negative cost or wall limit")

    started = time.monotonic()
    reports: list[CampaignReport] = []
    plans: list[RecursiveRoundPlan] = []
    signals: list[RoundSignal] = []
    blackboard = ArtifactBlackboard()
    current_head = input_head
    parent_candidate: str | None = None
    total_cost = 0.0
    stop_reason = "max_depth"

    for depth in range(budget.max_depth + 1):
        if cancellation.cancelled:
            stop_reason = "cancelled"
            break

        elapsed = int(time.monotonic() - started)
        remaining_cost = round(budget.max_cost_usd - total_cost, 6)
        remaining_wall = budget.max_wall_s - elapsed
        if remaining_cost <= 0 or remaining_wall <= 0:
            stop_reason = "budget_exhausted"
            break

        previous_phase = plans[-1].phase if plans else None
        plan = plan_adaptive_round(
            signals,
            depth=depth,
            input_head=current_head,
            blackboard=blackboard,
            allowed_strategies=strategies,
            max_candidates=budget.max_candidates,
            max_parallel=max_parallel,
            previous_phase=previous_phase,
        )
        plans.append(plan)

        round_budget = CampaignBudget(
            max_depth=budget.max_depth,
            max_candidates=budget.max_candidates,
            max_cost_usd=remaining_cost,
            max_wall_s=remaining_wall,
        )
        requests = plan_requests(
            campaign_id=f"{loop_id}:recursive-{depth}",
            cell_id=cell_id,
            epoch=epoch,
            input_head=current_head,
            strategies=plan.strategies,
            budget=round_budget,
            parent_candidate=parent_candidate,
            search_provenance_digest=plan.search_provenance_digest,
            depth=depth,
        )
        report = run_campaign(
            runner,
            requests,
            budget=round_budget,
            max_parallel=plan.max_parallel,
            parallel=parallel,
            human_approved=human_approved,
            required=required,
            cancellation=cancellation,
        )
        reports.append(report)
        signal = campaign_signal(report, required=required)
        signals.append(signal)
        blackboard = blackboard.append(_campaign_artifacts(report, signal))

        total_cost = round(total_cost + signal.cost_usd, 6)
        if report.cancelled or cancellation.cancelled:
            stop_reason = "cancelled"
            break

        winner_id = report.exploration_selection.winner
        if winner_id is None:
            stop_reason = "no_exploration_candidate"
            break
        winner = next(outcome for outcome in report.outcomes if outcome.logical_id == winner_id)
        if not winner.output_head:
            raise CampaignError(f"recursive search winner {winner_id} has no output head")

        current_head = winner.output_head
        parent_candidate = winner.logical_id

        elapsed = int(time.monotonic() - started)
        if total_cost >= budget.max_cost_usd or elapsed >= budget.max_wall_s:
            stop_reason = "budget_exhausted"
            break
        if depth == budget.max_depth:
            stop_reason = "max_depth"
            break

    if not reports:
        raise CampaignError(f"recursive search produced no campaign: {stop_reason}")

    laws = extract_search_laws(signals)
    blackboard = blackboard.append(_law_artifacts(laws))
    return RecursiveSearchReport(
        loop_id=loop_id,
        cell_id=cell_id,
        epoch=epoch,
        root_head=input_head,
        rounds=tuple(reports),
        plans=tuple(plans),
        signals=tuple(signals),
        laws=laws,
        blackboard=blackboard,
        stop_reason=stop_reason,
        total_cost_usd=total_cost,
        total_wall_s=int(time.monotonic() - started),
    )


def signal_from_document(
    document: Mapping[str, Any],
    *,
    required: Iterable[Dimension] = REQUIRED_DIMENSIONS,
) -> RoundSignal:
    """Recover search order parameters from a persisted CampaignReport JSON document."""

    required_values = {dimension.value for dimension in required}
    strategy_rows: dict[Strategy, list[Mapping[str, Any]]] = defaultdict(list)
    outcomes = document.get("outcomes")
    if not isinstance(outcomes, list):
        raise CampaignError("campaign document has no outcomes array")

    answered_rows: list[Mapping[str, Any]] = []
    evidence_complete = 0
    required_passes = 0
    for raw in outcomes:
        if not isinstance(raw, Mapping):
            raise CampaignError("campaign outcome must be an object")
        try:
            strategy = Strategy(str(raw["strategy"]))
        except (KeyError, ValueError) as error:
            raise CampaignError("campaign outcome has an unknown strategy") from error
        strategy_rows[strategy].append(raw)
        if _document_answered(raw):
            answered_rows.append(raw)
            if raw.get("evidence_digest"):
                evidence_complete += 1
            passed = {
                str(item.get("dimension"))
                for item in raw.get("evaluations", [])
                if isinstance(item, Mapping) and str(item.get("result")) == "pass"
            }
            if required_values.issubset(passed):
                required_passes += 1

    unique_outputs = {str(row.get("output_head")) for row in answered_rows if row.get("output_head")}
    disagreement = (
        0.0
        if len(answered_rows) < 2
        else (len(unique_outputs) - 1) / max(1, len(answered_rows) - 1)
    )
    winner_id = None
    selection = document.get("exploration_selection")
    if isinstance(selection, Mapping) and selection.get("winner") is not None:
        winner_id = str(selection["winner"])
    winner_strategy = None
    if winner_id is not None:
        winner = next((row for row in answered_rows if str(row.get("logical_id")) == winner_id), None)
        if winner is not None:
            winner_strategy = Strategy(str(winner["strategy"]))

    per_strategy: list[StrategySignal] = []
    for strategy in sorted(strategy_rows, key=lambda item: item.value):
        rows = strategy_rows[strategy]
        answered = [row for row in rows if _document_answered(row)]
        evidence = sum(1 for row in answered if row.get("evidence_digest"))
        required_count = 0
        for row in answered:
            passed = {
                str(item.get("dimension"))
                for item in row.get("evaluations", [])
                if isinstance(item, Mapping) and str(item.get("result")) == "pass"
            }
            required_count += int(required_values.issubset(passed))
        per_strategy.append(
            StrategySignal(
                strategy=strategy,
                attempts=len(rows),
                answered=len(answered),
                evidence_complete=evidence,
                required_passes=required_count,
                unique_outputs=len({str(row.get("output_head")) for row in answered if row.get("output_head")}),
                cost_usd=round(sum(float(row.get("cost_usd") or 0.0) for row in rows), 6),
            )
        )

    experiment_round = document.get("experiment_round")
    depth = int(experiment_round.get("depth", 0)) if isinstance(experiment_round, Mapping) else 0
    return RoundSignal(
        campaign_id=str(document.get("campaign_id") or ""),
        depth=depth,
        attempts=len(outcomes),
        answered=len(answered_rows),
        evidence_complete=evidence_complete,
        required_passes=required_passes,
        unique_outputs=len(unique_outputs),
        disagreement=round(disagreement, 6),
        novelty=round(_ratio(len(unique_outputs), len(answered_rows)), 6),
        cost_usd=round(sum(float(row.get("cost_usd") or 0.0) for row in outcomes if isinstance(row, Mapping)), 6),
        winner_strategy=winner_strategy,
        strategies=tuple(per_strategy),
    )


def _campaign_artifacts(report: CampaignReport, signal: RoundSignal) -> tuple[ResearchArtifact, ...]:
    artifacts: list[ResearchArtifact] = []
    winner_id = report.exploration_selection.winner
    for outcome in report.outcomes:
        if outcome.evidence_digest:
            artifacts.append(
                ResearchArtifact(
                    artifact_id=f"evidence:{outcome.logical_id}",
                    kind=ArtifactKind.EVIDENCE,
                    producer=outcome.logical_id,
                    payload_digest=outcome.evidence_digest,
                    candidate_id=outcome.logical_id,
                    tags=("evidence", outcome.strategy.value),
                    weight=2.0 if outcome.logical_id == winner_id else 1.0,
                )
            )
        if outcome.state != "ok" or not _answered(outcome):
            detail_digest = _digest(
                {
                    "candidate": outcome.logical_id,
                    "state": outcome.state,
                    "detail": outcome.detail,
                    "strategy": outcome.strategy.value,
                }
            )
            artifacts.append(
                ResearchArtifact(
                    artifact_id=f"failure:{outcome.logical_id}",
                    kind=ArtifactKind.FAILURE,
                    producer=outcome.logical_id,
                    payload_digest=detail_digest,
                    candidate_id=outcome.logical_id,
                    tags=("failure", outcome.strategy.value),
                    weight=1.0,
                )
            )
    if signal.disagreement >= 0.66:
        artifacts.append(
            ResearchArtifact(
                artifact_id=f"counterexample:{report.campaign_id}",
                kind=ArtifactKind.COUNTEREXAMPLE,
                producer=report.campaign_id,
                payload_digest=_digest(_signal_dict(signal)),
                tags=("counterexample", "disagreement"),
                weight=1.5,
            )
        )
    return tuple(artifacts)


def _law_artifacts(laws: Sequence[SearchLaw]) -> tuple[ResearchArtifact, ...]:
    return tuple(
        ResearchArtifact(
            artifact_id=f"law:{law.law_id}",
            kind=ArtifactKind.LAW,
            producer="recursive-search",
            payload_digest=law.digest(),
            tags=("law", law.kind.value, *(strategy.value for strategy in law.strategies)),
            weight=1.0 + law.confidence,
        )
        for law in laws
    )


def _strategy_scores(
    signals: Sequence[RoundSignal],
    allowed: Sequence[Strategy],
) -> dict[Strategy, float]:
    totals: dict[Strategy, list[StrategySignal]] = defaultdict(list)
    for signal in signals:
        for row in signal.strategies:
            totals[row.strategy].append(row)

    scores: dict[Strategy, float] = {}
    for strategy in allowed:
        rows = totals.get(strategy, [])
        if not rows:
            scores[strategy] = 0.25
            continue
        attempts = sum(row.attempts for row in rows)
        answered = sum(row.answered for row in rows)
        evidence = sum(row.evidence_complete for row in rows)
        required = sum(row.required_passes for row in rows)
        unique = sum(row.unique_outputs for row in rows)
        cost = sum(row.cost_usd for row in rows)
        answer_rate = _ratio(answered, attempts)
        evidence_rate = _ratio(evidence, attempts)
        required_rate = _ratio(required, attempts)
        novelty_rate = _ratio(unique, answered)
        cost_penalty = min(1.0, (cost / answered) / 10.0) if answered else min(1.0, cost / 10.0)
        scores[strategy] = round(
            0.20 * answer_rate
            + 0.35 * evidence_rate
            + 0.35 * required_rate
            + 0.20 * novelty_rate
            - 0.10 * cost_penalty,
            6,
        )
    return scores


def _least_tried_first(
    signals: Sequence[RoundSignal],
    allowed: Sequence[Strategy],
) -> tuple[Strategy, ...]:
    attempts = {strategy: 0 for strategy in allowed}
    for signal in signals:
        for row in signal.strategies:
            if row.strategy in attempts:
                attempts[row.strategy] += row.attempts
    return tuple(sorted(allowed, key=lambda strategy: (attempts[strategy], strategy.value)))


def _novelty_first(
    signals: Sequence[RoundSignal],
    allowed: Sequence[Strategy],
) -> tuple[Strategy, ...]:
    novelty: dict[Strategy, tuple[float, int]] = {strategy: (1.0, 0) for strategy in allowed}
    totals: dict[Strategy, tuple[int, int]] = {strategy: (0, 0) for strategy in allowed}
    for signal in signals:
        for row in signal.strategies:
            answered, unique = totals.get(row.strategy, (0, 0))
            totals[row.strategy] = (answered + row.answered, unique + row.unique_outputs)
    for strategy, (answered, unique) in totals.items():
        novelty[strategy] = (_ratio(unique, answered) if answered else 1.0, answered)
    return tuple(
        sorted(
            allowed,
            key=lambda strategy: (-novelty[strategy][0], novelty[strategy][1], strategy.value),
        )
    )


def _law(
    kind: LawKind,
    statement: str,
    confidence: float,
    support: int,
    strategies: tuple[Strategy, ...],
    signals: Sequence[RoundSignal],
) -> SearchLaw:
    evidence = tuple(
        _digest(_signal_dict(signal))
        for signal in signals
    )
    raw = {
        "kind": kind.value,
        "statement": statement,
        "strategies": [strategy.value for strategy in strategies],
        "evidence": list(evidence),
    }
    return SearchLaw(
        law_id="law_" + _digest(raw)[:24],
        kind=kind,
        statement=statement,
        confidence=round(max(0.0, min(1.0, confidence)), 6),
        support=max(1, support),
        strategies=strategies,
        evidence_digests=evidence,
    )


def _answered(outcome: CandidateOutcome) -> bool:
    return (
        outcome.state == "ok"
        and bool(outcome.output_head)
        and outcome.output_head != outcome.input_head
    )


def _document_answered(outcome: Mapping[str, Any]) -> bool:
    return (
        str(outcome.get("state")) == "ok"
        and bool(outcome.get("output_head"))
        and str(outcome.get("output_head")) != str(outcome.get("input_head"))
    )


def _ratio(numerator: float | int, denominator: float | int) -> float:
    if not denominator:
        return 0.0
    return round(float(numerator) / float(denominator), 6)


def _digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _artifact_dict(artifact: ResearchArtifact) -> dict[str, Any]:
    return {
        "artifact_id": artifact.artifact_id,
        "kind": artifact.kind.value,
        "producer": artifact.producer,
        "payload_digest": artifact.payload_digest,
        "candidate_id": artifact.candidate_id,
        "parents": list(artifact.parents),
        "tags": list(artifact.tags),
        "weight": artifact.weight,
        "digest": artifact.digest(),
    }


def _law_dict(law: SearchLaw) -> dict[str, Any]:
    return {
        "law_id": law.law_id,
        "kind": law.kind.value,
        "statement": law.statement,
        "confidence": law.confidence,
        "support": law.support,
        "strategies": [strategy.value for strategy in law.strategies],
        "evidence_digests": list(law.evidence_digests),
        "authority": law.authority,
        "digest": law.digest(),
    }


def _signal_dict(signal: RoundSignal) -> dict[str, Any]:
    return {
        "campaign_id": signal.campaign_id,
        "depth": signal.depth,
        "attempts": signal.attempts,
        "answered": signal.answered,
        "evidence_complete": signal.evidence_complete,
        "required_passes": signal.required_passes,
        "unique_outputs": signal.unique_outputs,
        "disagreement": signal.disagreement,
        "novelty": signal.novelty,
        "evidence_rate": signal.evidence_rate,
        "required_pass_rate": signal.required_pass_rate,
        "cost_usd": signal.cost_usd,
        "winner_strategy": signal.winner_strategy.value if signal.winner_strategy is not None else None,
        "strategies": [
            {
                **asdict(row),
                "strategy": row.strategy.value,
                "answer_rate": row.answer_rate,
                "evidence_rate": row.evidence_rate,
                "required_pass_rate": row.required_pass_rate,
                "novelty_rate": row.novelty_rate,
                "cost_per_answer": row.cost_per_answer,
            }
            for row in signal.strategies
        ],
    }
