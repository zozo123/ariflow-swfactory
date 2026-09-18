"""Bounded candidate campaigns: many weak attempts at one stage, one promoted on evidence.

A sequence of repair iterations is one learner asked the same question repeatedly, each attempt
anchored to the last one's mistakes.  A campaign asks several independent learners at once and
keeps the one the evidence prefers.  Both spend the same budget; only the second can be wrong in
more than one direction at a time.

This module is not a scheduler and it is not an authority.  Apache Airflow still owns the
lifecycle, the campaign runs inside one governed stage, and selection *proposes* a winner --
:func:`swfactory.generations.promotable` still requires the human gate before anything is
promoted.  Candidates are dependency-injected through :class:`CandidateRunner` so the kernel can
be tested, and so the workspace isolation a real runner needs stays the provider's problem.

Two properties matter more than throughput and are asserted rather than assumed:

* **Selection is completion-order independent.** If the winner changed with which candidate
  happened to finish first, the campaign would be a race, not an experiment.
* **Candidates are independent.** Two candidates reporting the same output head, or a candidate
  reporting the campaign's own input head, did not explore anything; the campaign says so instead
  of promoting a result that only looks like a choice.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Iterable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field, replace
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Protocol

from swfactory.experiment_tree import ExperimentNode, ExperimentRound, NodeState
from swfactory.git_worktree import (
    create_candidate_worktree,
    recorded_candidate_head,
    remove_candidate_worktree,
)
from swfactory.generations import CampaignBudget, Dimension, Evaluation, promotable
from swfactory.work_executor import Cancellation

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
    depth: int = 0
    budget_usd: float = 0.0
    timeout_s: int = 1800

    @property
    def logical_id(self) -> str:
        """Stable identity for this exact question, so a replay is recognisable as the same one."""
        raw = "\0".join(
            (
                self.campaign_id,
                self.cell_id,
                str(self.epoch),
                self.strategy.value,
                self.input_head,
                self.parent_generation or "",
                self.parent_candidate or "",
                str(self.depth),
            )
        ).encode()
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
    workspace_key: str | None = None

    @property
    def passed(self) -> frozenset[Dimension]:
        return frozenset(item.dimension for item in self.evaluations if item.result == "pass")


class CandidateRunner(Protocol):
    """Run one candidate to completion in its own workspace and score it."""

    def __call__(self, request: CandidateRequest) -> CandidateOutcome: ...


class WorkspaceCandidateRunner(Protocol):
    """Run one candidate inside a factory-owned isolated Git worktree."""

    def __call__(self, request: CandidateRequest, workspace: Path) -> CandidateOutcome: ...


def worktree_candidate_runner(
    repo: Path,
    worktree_root: Path,
    runner: WorkspaceCandidateRunner,
) -> CandidateRunner:
    """Adapt a workspace-aware runner to the campaign interface.

    Every invocation starts from the request's exact input head in a detached
    worktree. A successful candidate must leave a clean Git state; the observed
    HEAD is authoritative and replaces any untrusted output-head claim returned
    by the worker. The disposable worktree is removed after its receipt is
    captured, including on runner failure.
    """

    def isolated(request: CandidateRequest) -> CandidateOutcome:
        worktree = create_candidate_worktree(
            repo,
            request.logical_id,
            request.input_head,
            worktree_root,
        )
        try:
            outcome = runner(request, Path(worktree.path))
            observed_head = recorded_candidate_head(worktree)
            if outcome.output_head is not None and outcome.output_head != observed_head:
                raise CampaignError(
                    f"candidate {request.logical_id} claimed output {outcome.output_head} "
                    f"but worktree recorded {observed_head}"
                )
            return replace(
                outcome,
                output_head=observed_head,
                workspace_key=worktree.workspace_key,
            )
        finally:
            remove_candidate_worktree(repo, worktree, force=True)

    return isolated


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
    selection: Selection = field(default_factory=lambda: Selection(None, "not_selected"))
    independence: tuple[str, ...] = ()
    cancelled: bool = False
    experiment_round: ExperimentRound | None = None

    def to_dict(self) -> dict[str, Any]:
        document = asdict(self)
        if self.experiment_round is not None:
            document["experiment_round"] = self.experiment_round.to_dict()
        document["schema_version"] = 2
        document["scheduler"] = "airflow"
        return document


def rank_key(outcome: CandidateOutcome, required: Iterable[Dimension]) -> tuple[Any, ...]:
    """Order candidates by evidence, then by what they cost to obtain.

    Every component is a property of the candidate itself.  Nothing here reads a clock, a thread
    identity or an arrival position, which is what makes the ranking reproducible from the stored
    report alone -- and what makes :func:`select` independent of completion order.
    """
    passed = outcome.passed
    required_hits = sum(1 for dimension in required if dimension in passed)
    return (
        outcome.state != "ok",  # False (0) sorts first
        -required_hits,
        -len(passed),
        round(outcome.cost_usd, 6),
        round(outcome.duration_s, 3),
        outcome.logical_id,
    )


def independence_findings(outcomes: Sequence[CandidateOutcome], *, input_head: str) -> tuple[str, ...]:
    """Ways a set of candidates can only look independent.

    A campaign whose members all land on the same head explored one idea several times.  Saying so
    is the difference between an experiment and an expensive way to run the same attempt.
    """
    findings: list[str] = []
    heads: dict[str, list[str]] = {}
    for outcome in outcomes:
        if outcome.state != "ok":
            continue
        if not outcome.output_head:
            findings.append(f"{outcome.logical_id}: succeeded without an output head")
            continue
        if outcome.output_head == input_head:
            findings.append(f"{outcome.logical_id}: output head equals the campaign input head")
        heads.setdefault(outcome.output_head, []).append(outcome.logical_id)
    for head, owners in sorted(heads.items()):
        if len(owners) > 1:
            findings.append(f"{', '.join(sorted(owners))}: identical output head {head}")
    return tuple(findings)


def select(
    outcomes: Sequence[CandidateOutcome],
    *,
    required: Iterable[Dimension] = REQUIRED_DIMENSIONS,
    human_approved: bool = False,
) -> Selection:
    """Propose one candidate, and record why each of the others is not it.

    ``human_approved`` is threaded to :func:`swfactory.generations.promotable` rather than decided
    here: a campaign that could approve its own winner would be a promotion authority, and the
    methodology allows exactly one of those.
    """
    required = frozenset(required)
    ordered = sorted(outcomes, key=lambda item: rank_key(item, required))
    ranking = tuple(item.logical_id for item in ordered)
    refusals: list[str] = []
    for outcome in ordered:
        if outcome.state != "ok":
            refusals.append(f"{outcome.logical_id}: {outcome.state}")
            continue
        if not outcome.output_head:
            refusals.append(f"{outcome.logical_id}: no_output_head")
            continue
        if outcome.output_head == outcome.input_head:
            refusals.append(f"{outcome.logical_id}: unchanged_output_head")
            continue
        ok, failures = promotable(list(outcome.evaluations), required=set(required), human_approved=human_approved)
        if ok:
            return Selection(outcome.logical_id, f"promotable:{outcome.strategy.value}", ranking, tuple(refusals))
        refusals.append(f"{outcome.logical_id}: {','.join(failures)}")
    return Selection(None, "no_promotable_candidate", ranking, tuple(refusals))


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
    if depth > 0 and not parent_candidate:
        raise CampaignError("a descendant experiment round requires the previous winner as parent_candidate")
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
            depth=depth,
            budget_usd=share,
            timeout_s=budget.max_wall_s,
        )
        for strategy in strategies
    )


def run_campaign(
    runner: CandidateRunner,
    requests: Sequence[CandidateRequest],
    *,
    budget: CampaignBudget | None = None,
    max_parallel: int = 3,
    parallel: bool = True,
    human_approved: bool = False,
    required: Iterable[Dimension] = REQUIRED_DIMENSIONS,
    cancellation: Cancellation | None = None,
) -> CampaignReport:
    """Run every candidate, then choose one.

    A failing candidate does not cancel its siblings.  That is the whole point: the campaign is
    buying the information that this strategy loses on this failure, and a sibling's success is
    what makes that information worth having.
    """
    if not requests:
        raise CampaignError("a campaign needs at least one candidate request")
    budget = budget or CampaignBudget()
    if len({request.logical_id for request in requests}) != len(requests):
        raise CampaignError("candidate requests must be distinct")
    head = requests[0].input_head
    if any(request.input_head != head for request in requests):
        raise CampaignError("every candidate in one campaign starts from the same input head")
    if len({request.depth for request in requests}) != 1:
        raise CampaignError("every candidate in one campaign must have the same tree depth")
    if len({request.parent_candidate for request in requests}) != 1:
        raise CampaignError("every candidate in one campaign must have the same parent candidate")
    cancellation = cancellation or Cancellation()

    started = time.monotonic()
    concurrent = parallel and max_parallel > 1 and len(requests) > 1
    outcomes = (
        _concurrent(runner, requests, max_parallel, cancellation)
        if concurrent
        else _sequential(runner, requests, cancellation)
    )

    spent = round(sum(outcome.cost_usd for outcome in outcomes), 6)
    elapsed = int(time.monotonic() - started)
    over_budget = not budget.admits(
        depth=requests[0].depth, candidates=len(requests) - 1, cost_usd=spent, wall_s=elapsed
    )
    report = CampaignReport(
        campaign_id=requests[0].campaign_id,
        cell_id=requests[0].cell_id,
        epoch=requests[0].epoch,
        input_head=head,
        strategies=tuple(request.strategy.value for request in requests),
        parallel=concurrent,
        outcomes=tuple(outcomes),
        independence=independence_findings(outcomes, input_head=head),
        cancelled=cancellation.cancelled,
    )
    if over_budget:
        report.selection = Selection(
            None,
            "campaign_exceeded_budget",
            tuple(outcome.logical_id for outcome in outcomes),
            (f"spent {spent} usd over {elapsed}s",),
        )
    else:
        report.selection = select(outcomes, required=required, human_approved=human_approved)
    report.experiment_round = _experiment_round(requests, outcomes, report.selection)
    return report


def _experiment_round(
    requests: Sequence[CandidateRequest],
    outcomes: Sequence[CandidateOutcome],
    selection: Selection,
) -> ExperimentRound:
    """Project one campaign into a frozen/provisional sibling bush.

    A successful execution with a distinct recorded head answered its question,
    even when its verification dimensions lost. Such a node is frozen evidence.
    Runner/infrastructure failures and unchanged/missing heads remain provisional
    and can be repaired without inventing a new branch in the experiment tree.
    """
    request_by_id = {request.logical_id: request for request in requests}
    nodes: list[ExperimentNode] = []
    for outcome in outcomes:
        request = request_by_id[outcome.logical_id]
        answered = outcome.state == "ok" and bool(outcome.output_head) and outcome.output_head != outcome.input_head
        evidence = tuple(f"{item.dimension.value}:{item.result}:{item.evidence}" for item in outcome.evaluations)
        nodes.append(
            ExperimentNode(
                id=outcome.logical_id,
                parent_id=request.parent_candidate,
                depth=request.depth,
                strategy=request.strategy.value,
                input_head=request.input_head,
                result=outcome.state,
                state=NodeState.ANSWERED if answered else NodeState.PROVISIONAL,
                recorded_head=outcome.output_head if answered else None,
                selected=selection.winner == outcome.logical_id,
                evidence=evidence,
                detail=outcome.detail,
            )
        )
    round_ = ExperimentRound(
        round_id=requests[0].campaign_id,
        input_head=requests[0].input_head,
        depth=requests[0].depth,
        parent_candidate=requests[0].parent_candidate,
        nodes=tuple(nodes),
        winner_id=selection.winner,
    )
    round_.validate()
    return round_


def _sequential(
    runner: CandidateRunner, requests: Sequence[CandidateRequest], cancellation: Cancellation
) -> list[CandidateOutcome]:
    return [_guarded(runner, request, cancellation) for request in requests]


def _concurrent(
    runner: CandidateRunner,
    requests: Sequence[CandidateRequest],
    max_parallel: int,
    cancellation: Cancellation,
) -> list[CandidateOutcome]:
    collected: dict[str, CandidateOutcome] = {}
    with ThreadPoolExecutor(max_workers=min(max_parallel, len(requests)), thread_name_prefix="swf-cand") as pool:
        futures: dict[Future[CandidateOutcome], CandidateRequest] = {
            pool.submit(_guarded, runner, request, cancellation): request for request in requests
        }
        for future in as_completed(futures):
            request = futures[future]
            collected[request.logical_id] = future.result()
    # Restore request order so the stored report does not encode who finished first.
    return [collected[request.logical_id] for request in requests]


def _guarded(runner: CandidateRunner, request: CandidateRequest, cancellation: Cancellation) -> CandidateOutcome:
    """Run one candidate; a raising runner is data, not a campaign failure."""
    if cancellation.cancelled:
        return CandidateOutcome(
            request.logical_id, request.strategy, "cancelled", request.input_head, detail="cancelled before start"
        )
    started = time.monotonic()
    try:
        outcome = runner(request)
    except BaseException as error:  # noqa: BLE001 - a losing candidate is the campaign's product.
        return CandidateOutcome(
            request.logical_id,
            request.strategy,
            "failed",
            request.input_head,
            duration_s=round(time.monotonic() - started, 3),
            detail=f"{type(error).__name__}: {error}"[:2000],
        )
    if outcome.logical_id != request.logical_id or outcome.strategy != request.strategy:
        raise CampaignError("candidate runner returned an outcome for a different request")
    if outcome.input_head != request.input_head:
        raise CampaignError("candidate runner returned an outcome for a different input head")
    if outcome.duration_s:
        return outcome
    return replace(outcome, duration_s=round(time.monotonic() - started, 3))


def evaluation(dimension: Dimension, *, passed: bool, evidence: str) -> Evaluation:
    """Small helper so a runner records a dimension the same way everywhere."""
    return Evaluation(dimension=dimension, result="pass" if passed else "fail", evidence=evidence)
