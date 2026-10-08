"""Bounded candidate campaigns: many weak attempts at one stage, one promoted on evidence.

A sequence of repair iterations is one learner asked the same question repeatedly, each attempt
anchored to the last one's mistakes.  A campaign asks several independent learners at once and
keeps the one the evidence prefers.  Both spend the same budget; only the second can be wrong in
more than one direction at a time.

This module is not a scheduler and it is not an authority.  It plans the exact questions a
campaign may ask and defines the report a campaign stores; Apache Airflow still owns the
lifecycle, and :func:`swfactory.generations.promotable` still requires the human gate before
anything is promoted.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any, Literal

from swfactory.experiment_tree import ExperimentRound
from swfactory.generations import CampaignBudget, Dimension, Evaluation

CandidateState = Literal["ok", "failed", "cancelled", "skipped", "refused"]


class Strategy(StrEnum):
    """The independent ways one failed attempt can be answered.

    Named for what they keep, not for how hard they try: each discards a different amount of the
    previous attempt, so a failure caused by a bad plan and a failure caused by a bad patch are
    not both handed to the same fix loop.
    """

    REPAIR = "repair"  # keep the patch; correct it against the observed failure
    RETHINK = "rethink"  # keep the specification; plan the work again
    SCRATCH = "scratch"  # keep only the issue; start the implementation over


DEFAULT_STRATEGIES: tuple[Strategy, ...] = (Strategy.REPAIR, Strategy.RETHINK, Strategy.SCRATCH)

# Correctness is not tradeable. A campaign that promoted a cheaper candidate over a correct one
# would be optimising the measurement.
REQUIRED_DIMENSIONS: frozenset[Dimension] = frozenset({Dimension.CORRECTNESS, Dimension.EVIDENCE})


class CampaignError(ValueError):
    """A campaign was asked for something its budget or inputs do not permit."""


@dataclass(frozen=True)
class CandidateRequest:
    """One learner's whole question: an immutable parent, a strategy, and a bounded allowance."""

    campaign_id: str
    cell_id: str
    epoch: int
    strategy: Strategy
    input_head: str
    parent_generation: str | None = None
    parent_candidate: str | None = None
    parent_decision_digest: str | None = None
    search_provenance_digest: str | None = None
    depth: int = 0
    budget_usd: float = 0.0
    timeout_s: int = 1800

    @property
    def logical_id(self) -> str:
        """Stable identity for this exact question, so a replay is recognisable as the same one."""
        identity_parts = [
            self.campaign_id,
            self.cell_id,
            str(self.epoch),
            self.strategy.value,
            self.input_head,
            self.parent_generation or "",
            self.parent_candidate or "",
            self.parent_decision_digest or "",
        ]
        if self.search_provenance_digest is not None:
            identity_parts.append(self.search_provenance_digest)
        identity_parts.append(str(self.depth))
        raw = "\0".join(identity_parts).encode()
        return "cand_" + hashlib.sha256(raw).hexdigest()[:24]


@dataclass(frozen=True)
class CandidateOutcome:
    """What one learner produced, including when it produced nothing."""

    logical_id: str
    strategy: Strategy
    state: CandidateState
    input_head: str
    output_head: str | None = None
    evaluations: tuple[Evaluation, ...] = ()
    cost_usd: float = 0.0
    duration_s: float = 0.0
    detail: str = ""
    candidate_ref: str | None = None
    evidence_bundle_path: str | None = None
    evidence_digest: str | None = None
    inherited_recipe_digest: str | None = None


@dataclass(frozen=True)
class Selection:
    """The proposal a campaign makes, and every reason it refused the alternatives."""

    winner: str | None
    reason: str
    ranking: tuple[str, ...] = ()
    refusals: tuple[str, ...] = ()


@dataclass
class CampaignReport:
    campaign_id: str
    cell_id: str
    epoch: int
    input_head: str
    strategies: tuple[str, ...]
    parallel: bool
    outcomes: tuple[CandidateOutcome, ...] = ()
    exploration_selection: Selection = field(default_factory=lambda: Selection(None, "not_selected"))
    selection: Selection = field(default_factory=lambda: Selection(None, "not_selected"))
    independence: tuple[str, ...] = ()
    cancelled: bool = False
    experiment_round: ExperimentRound | None = None

    def to_dict(self) -> dict[str, Any]:
        document = asdict(self)
        if self.experiment_round is not None:
            document["experiment_round"] = self.experiment_round.to_dict()
        document["schema_version"] = 3
        document["scheduler"] = "airflow"
        return document


def plan_requests(
    *,
    campaign_id: str,
    cell_id: str,
    epoch: int,
    input_head: str,
    strategies: Sequence[Strategy] = DEFAULT_STRATEGIES,
    budget: CampaignBudget | None = None,
    parent_generation: str | None = None,
    parent_candidate: str | None = None,
    parent_decision_digest: str | None = None,
    search_provenance_digest: str | None = None,
    depth: int = 0,
) -> tuple[CandidateRequest, ...]:
    """Turn a budget and a list of strategies into the exact questions a campaign may ask."""
    budget = budget or CampaignBudget()
    if not strategies:
        raise CampaignError("a campaign needs at least one strategy")
    if len(set(strategies)) != len(strategies):
        raise CampaignError("a campaign must not run the same strategy twice")
    # `admits` takes the count already spent, so the last admissible index is max_candidates - 1.
    if not budget.admits(depth=depth, candidates=len(strategies) - 1, cost_usd=0.0, wall_s=0):
        raise CampaignError(
            f"campaign budget admits at most {budget.max_candidates} candidates to depth "
            f"{budget.max_depth}; asked for {len(strategies)} at depth {depth}"
        )
    if depth == 0 and parent_candidate is not None:
        raise CampaignError("the first experiment round cannot name a parent candidate")
    if depth == 0 and parent_decision_digest is not None:
        raise CampaignError("the first experiment round cannot name a parent decision")
    if depth > 0 and not parent_candidate:
        raise CampaignError("a descendant experiment round requires the previous winner as parent_candidate")
    for name, digest in {
        "parent_decision_digest": parent_decision_digest,
        "search_provenance_digest": search_provenance_digest,
    }.items():
        if digest is None:
            continue
        prefix = "sha256:"
        suffix = digest.removeprefix(prefix)
        if not digest.startswith(prefix) or len(suffix) != 64 or any(char not in "0123456789abcdef" for char in suffix):
            raise CampaignError(f"{name} must be a canonical sha256 digest")
    share = round(budget.max_cost_usd / len(strategies), 6)
    return tuple(
        CandidateRequest(
            campaign_id=campaign_id,
            cell_id=cell_id,
            epoch=epoch,
            strategy=strategy,
            input_head=input_head,
            parent_generation=parent_generation,
            parent_candidate=parent_candidate,
            parent_decision_digest=parent_decision_digest,
            search_provenance_digest=search_provenance_digest,
            depth=depth,
            budget_usd=share,
            timeout_s=budget.max_wall_s,
        )
        for strategy in strategies
    )


def evaluation(dimension: Dimension, *, passed: bool, evidence: str) -> Evaluation:
    """Small helper so a runner records a dimension the same way everywhere."""
    return Evaluation(dimension=dimension, result="pass" if passed else "fail", evidence=evidence)
