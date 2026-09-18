"""Immutable fan-in evidence for bounded candidate campaigns.

A campaign report explains how candidates were ranked. Candidate evidence bundles
retain the exact source, frozen output, diff, and artifacts for individual
answered siblings. This module joins those two evidence layers into one canonical
decision manifest.

The manifest is evidence only. It cannot schedule work, approve a candidate,
publish a ref, or promote a generation.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from swfactory.candidate_evidence import CandidateEvidenceBundle
from swfactory.evolution import (
    DEFAULT_STRATEGIES,
    CampaignError,
    CampaignReport,
    CandidateOutcome,
    CandidateRequest,
    Selection,
    Strategy,
    plan_requests,
)
from swfactory.generations import CampaignBudget, Dimension, Evaluation

_DIGEST_PREFIX = "sha256:"
_ALLOWED_STATES = {"ok", "failed", "cancelled", "skipped", "refused"}


class CampaignDecisionError(RuntimeError):
    """Campaign fan-in evidence is incomplete, inconsistent, or changed."""


@dataclass(frozen=True)
class DecisionEvaluation:
    dimension: str
    result: str
    evidence: str

    def validate(self) -> None:
        if not self.dimension.strip():
            raise CampaignDecisionError("evaluation dimension must be nonempty")
        if not self.result.strip():
            raise CampaignDecisionError(f"evaluation {self.dimension!r} result must be nonempty")
        if not self.evidence.strip():
            raise CampaignDecisionError(f"evaluation {self.dimension!r} evidence must be nonempty")


@dataclass(frozen=True)
class DecisionCandidate:
    candidate_id: str
    strategy: str
    state: str
    input_head: str
    output_head: str | None
    candidate_ref: str | None
    evidence_bundle_digest: str | None
    evaluations: tuple[DecisionEvaluation, ...]
    cost_usd: float
    duration_s: float

    @property
    def answered(self) -> bool:
        return self.state == "ok" and bool(self.output_head) and self.output_head != self.input_head

    def validate(self) -> None:
        if not self.candidate_id.strip():
            raise CampaignDecisionError("candidate id must be nonempty")
        if not self.strategy.strip():
            raise CampaignDecisionError(f"{self.candidate_id}: strategy must be nonempty")
        if self.state not in _ALLOWED_STATES:
            raise CampaignDecisionError(f"{self.candidate_id}: unknown state {self.state!r}")
        if not self.input_head.strip():
            raise CampaignDecisionError(f"{self.candidate_id}: input head must be nonempty")
        if self.cost_usd < 0 or self.duration_s < 0:
            raise CampaignDecisionError(f"{self.candidate_id}: cost and duration must be nonnegative")
        dimensions = [item.dimension for item in self.evaluations]
        if len(dimensions) != len(set(dimensions)):
            raise CampaignDecisionError(f"{self.candidate_id}: duplicate evaluation dimension")
        for evaluation in self.evaluations:
            evaluation.validate()
        if self.answered:
            if not self.candidate_ref:
                raise CampaignDecisionError(f"{self.candidate_id}: answered candidate has no frozen ref")
            if not _valid_digest(self.evidence_bundle_digest):
                raise CampaignDecisionError(
                    f"{self.candidate_id}: answered candidate has no valid evidence bundle digest"
                )
        elif self.evidence_bundle_digest is not None:
            raise CampaignDecisionError(
                f"{self.candidate_id}: provisional/failed candidate cannot claim answered evidence bundle"
            )


@dataclass(frozen=True)
class DecisionSelection:
    winner: str | None
    reason: str
    ranking: tuple[str, ...]
    refusals: tuple[str, ...]

    def validate(self, candidate_ids: tuple[str, ...]) -> None:
        if not self.reason.strip():
            raise CampaignDecisionError("selection reason must be nonempty")
        if len(self.ranking) != len(set(self.ranking)):
            raise CampaignDecisionError("selection ranking contains duplicates")
        if set(self.ranking) != set(candidate_ids):
            raise CampaignDecisionError("selection ranking does not contain exactly the campaign candidates")
        if self.winner is not None and self.winner not in candidate_ids:
            raise CampaignDecisionError("selection winner is not a campaign candidate")


@dataclass(frozen=True)
class DescendantCampaignPlan:
    """A verified fork from one immutable campaign decision into the next round."""

    parent_campaign_id: str
    parent_decision_digest: str
    parent_candidate: str
    parent_evidence_digest: str
    input_head: str
    depth: int
    requests: tuple[CandidateRequest, ...]
    schema_version: int = 1

    def canonical_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "parent_campaign_id": self.parent_campaign_id,
            "parent_decision_digest": self.parent_decision_digest,
            "parent_candidate": self.parent_candidate,
            "parent_evidence_digest": self.parent_evidence_digest,
            "input_head": self.input_head,
            "depth": self.depth,
            "requests": [
                {
                    "campaign_id": request.campaign_id,
                    "cell_id": request.cell_id,
                    "epoch": request.epoch,
                    "strategy": request.strategy.value,
                    "input_head": request.input_head,
                    "parent_generation": request.parent_generation,
                    "parent_candidate": request.parent_candidate,
                    "parent_decision_digest": request.parent_decision_digest,
                    "depth": request.depth,
                    "budget_usd": request.budget_usd,
                    "timeout_s": request.timeout_s,
                    "logical_id": request.logical_id,
                }
                for request in self.requests
            ],
        }

    def digest(self) -> str:
        payload = json.dumps(self.canonical_dict(), sort_keys=True, separators=(",", ":")).encode()
        return _DIGEST_PREFIX + hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class CampaignDecisionManifest:
    campaign_id: str
    cell_id: str
    epoch: int
    input_head: str
    candidates: tuple[DecisionCandidate, ...]
    selection: DecisionSelection
    independence: tuple[str, ...]
    cancelled: bool
    schema_version: int = 1

    def validate(self) -> None:
        if self.schema_version != 1:
            raise CampaignDecisionError(f"unsupported campaign decision schema {self.schema_version}")
        if not self.campaign_id.strip() or not self.cell_id.strip():
            raise CampaignDecisionError("campaign and cell ids must be nonempty")
        if self.epoch < 0:
            raise CampaignDecisionError("campaign epoch must be nonnegative")
        if not self.input_head.strip():
            raise CampaignDecisionError("campaign input head must be nonempty")
        if not self.candidates:
            raise CampaignDecisionError("campaign decision needs at least one candidate")
        ids = tuple(candidate.candidate_id for candidate in self.candidates)
        if len(ids) != len(set(ids)):
            raise CampaignDecisionError("campaign decision contains duplicate candidates")
        for candidate in self.candidates:
            candidate.validate()
            if candidate.input_head != self.input_head:
                raise CampaignDecisionError(
                    f"{candidate.candidate_id}: input {candidate.input_head} != campaign input {self.input_head}"
                )
        self.selection.validate(ids)
        if self.selection.winner is not None:
            winner = next(candidate for candidate in self.candidates if candidate.candidate_id == self.selection.winner)
            if not winner.answered or not winner.evidence_bundle_digest:
                raise CampaignDecisionError("selection winner is not backed by answered candidate evidence")

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema_version": self.schema_version,
            "campaign_id": self.campaign_id,
            "cell_id": self.cell_id,
            "epoch": self.epoch,
            "input_head": self.input_head,
            "candidates": [
                {
                    **asdict(candidate),
                    "evaluations": [asdict(item) for item in candidate.evaluations],
                }
                for candidate in self.candidates
            ],
            "selection": asdict(self.selection),
            "independence": list(self.independence),
            "cancelled": self.cancelled,
        }

    def digest(self) -> str:
        payload = json.dumps(self.canonical_dict(), sort_keys=True, separators=(",", ":")).encode()
        return _DIGEST_PREFIX + hashlib.sha256(payload).hexdigest()


def plan_descendant_campaign(
    report: CampaignReport,
    *,
    repo: Path,
    campaign_id: str,
    strategies: tuple[Strategy, ...] = DEFAULT_STRATEGIES,
    budget: CampaignBudget | None = None,
    parent_generation: str | None = None,
) -> DescendantCampaignPlan:
    """Plan the next sibling bush only from a fully verified parent decision."""

    from swfactory.candidate_evidence import CandidateEvidenceError, verify_candidate_evidence_bundle

    if report.experiment_round is None:
        raise CampaignDecisionError("cannot descend from a campaign without an experiment round")
    winner_id = report.exploration_selection.winner
    if winner_id is None:
        raise CampaignDecisionError("cannot descend from a campaign without an exploration winner")
    winners = [outcome for outcome in report.outcomes if outcome.logical_id == winner_id]
    if len(winners) != 1:
        raise CampaignDecisionError("exploration winner must identify exactly one campaign outcome")
    winner = winners[0]
    if winner.state != "ok" or not winner.output_head or winner.output_head == winner.input_head:
        raise CampaignDecisionError("exploration winner is not an answered candidate")
    if not winner.candidate_ref or not winner.evidence_bundle_path or not winner.evidence_digest:
        raise CampaignDecisionError("exploration winner is missing frozen candidate evidence")

    bundles: dict[str, CandidateEvidenceBundle] = {}
    for outcome in report.outcomes:
        answered = outcome.state == "ok" and bool(outcome.output_head) and outcome.output_head != outcome.input_head
        if not answered:
            continue
        if not outcome.evidence_bundle_path or not outcome.evidence_digest:
            raise CampaignDecisionError(f"{outcome.logical_id}: answered sibling is missing retained evidence")
        try:
            bundle = verify_candidate_evidence_bundle(Path(outcome.evidence_bundle_path), repo=repo)
        except CandidateEvidenceError as error:
            raise CampaignDecisionError(
                f"{outcome.logical_id}: candidate evidence verification failed: {error}"
            ) from error
        if bundle.digest() != outcome.evidence_digest:
            raise CampaignDecisionError(f"{outcome.logical_id}: recorded evidence digest differs from retained bundle")
        bundles[outcome.logical_id] = bundle

    # Fan-in is rebuilt from immutable sibling evidence rather than trusted from
    # an in-memory winner pointer.
    decision_report = report
    if report.selection.winner != winner_id:
        decision_report = CampaignReport(
            campaign_id=report.campaign_id,
            cell_id=report.cell_id,
            epoch=report.epoch,
            input_head=report.input_head,
            strategies=report.strategies,
            parallel=report.parallel,
            outcomes=report.outcomes,
            selection=report.exploration_selection,
            exploration_selection=report.exploration_selection,
            independence=report.independence,
            cancelled=report.cancelled,
            experiment_round=report.experiment_round,
        )
    decision = build_campaign_decision(decision_report, bundles)

    parent_round = report.experiment_round
    if parent_round.winner_id != winner_id:
        raise CampaignDecisionError("experiment-tree winner differs from exploration selection")
    winner_nodes = [node for node in parent_round.nodes if node.id == winner_id]
    if len(winner_nodes) != 1:
        raise CampaignDecisionError("experiment tree must contain exactly one exploration winner node")
    winner_node = winner_nodes[0]
    if not winner_node.selected or winner_node.recorded_head != winner.output_head:
        raise CampaignDecisionError("experiment-tree winner does not bind the selected output head")

    parent_digest = decision.digest()
    try:
        requests = plan_requests(
            campaign_id=campaign_id,
            cell_id=report.cell_id,
            epoch=report.epoch,
            input_head=winner.output_head,
            strategies=strategies,
            budget=budget,
            parent_generation=parent_generation,
            parent_candidate=winner_id,
            parent_decision_digest=parent_digest,
            depth=parent_round.depth + 1,
        )
    except CampaignError as error:
        raise CampaignDecisionError(f"descendant campaign is inadmissible: {error}") from error

    return DescendantCampaignPlan(
        parent_campaign_id=report.campaign_id,
        parent_decision_digest=parent_digest,
        parent_candidate=winner_id,
        parent_evidence_digest=winner.evidence_digest,
        input_head=winner.output_head,
        depth=parent_round.depth + 1,
        requests=requests,
    )


def build_campaign_decision(
    report: CampaignReport,
    evidence_bundles: Mapping[str, CandidateEvidenceBundle],
) -> CampaignDecisionManifest:
    """Bind deterministic selection to every answered candidate's retained evidence."""

    known_ids = {outcome.logical_id for outcome in report.outcomes}
    extras = set(evidence_bundles) - known_ids
    if extras:
        raise CampaignDecisionError("evidence supplied for unknown candidates: " + ",".join(sorted(extras)))

    candidates = tuple(
        sorted(
            (
                _decision_candidate(outcome, evidence_bundles.get(outcome.logical_id))
                for outcome in report.outcomes
            ),
            key=lambda candidate: candidate.candidate_id,
        )
    )
    manifest = CampaignDecisionManifest(
        campaign_id=report.campaign_id,
        cell_id=report.cell_id,
        epoch=report.epoch,
        input_head=report.input_head,
        candidates=candidates,
        selection=DecisionSelection(
            winner=report.selection.winner,
            reason=report.selection.reason,
            ranking=tuple(report.selection.ranking),
            refusals=tuple(report.selection.refusals),
        ),
        independence=tuple(report.independence),
        cancelled=report.cancelled,
    )
    manifest.validate()
    return manifest


def build_campaign_decision_from_document(
    document: Mapping[str, Any],
    evidence_bundles: Mapping[str, CandidateEvidenceBundle],
) -> CampaignDecisionManifest:
    """Rehydrate the stored campaign report fields needed for immutable fan-in evidence."""

    try:
        outcomes = tuple(
            CandidateOutcome(
                logical_id=str(item["logical_id"]),
                strategy=Strategy(str(item["strategy"])),
                state=str(item["state"]),  # type: ignore[arg-type]
                input_head=str(item["input_head"]),
                output_head=str(item["output_head"]) if item.get("output_head") is not None else None,
                evaluations=tuple(
                    Evaluation(
                        dimension=Dimension(str(row["dimension"])),
                        result=str(row["result"]),
                        evidence=str(row["evidence"]),
                    )
                    for row in item.get("evaluations", ())
                ),
                cost_usd=float(item.get("cost_usd", 0.0)),
                duration_s=float(item.get("duration_s", 0.0)),
                detail=str(item.get("detail", "")),
                candidate_ref=str(item["candidate_ref"]) if item.get("candidate_ref") is not None else None,
            )
            for item in document["outcomes"]
        )
        selection_doc = document["selection"]
        report = CampaignReport(
            campaign_id=str(document["campaign_id"]),
            cell_id=str(document["cell_id"]),
            epoch=int(document["epoch"]),
            input_head=str(document["input_head"]),
            strategies=tuple(str(item) for item in document["strategies"]),
            parallel=bool(document["parallel"]),
            outcomes=outcomes,
            selection=Selection(
                winner=str(selection_doc["winner"]) if selection_doc.get("winner") is not None else None,
                reason=str(selection_doc["reason"]),
                ranking=tuple(str(item) for item in selection_doc.get("ranking", ())),
                refusals=tuple(str(item) for item in selection_doc.get("refusals", ())),
            ),
            independence=tuple(str(item) for item in document.get("independence", ())),
            cancelled=bool(document.get("cancelled", False)),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise CampaignDecisionError(f"cannot parse campaign report: {error}") from error
    return build_campaign_decision(report, evidence_bundles)


def write_campaign_decision(path: Path, manifest: CampaignDecisionManifest) -> None:
    document = manifest.canonical_dict()
    document["manifest_digest"] = manifest.digest()
    _atomic_json(path, document)


def load_campaign_decision(path: Path) -> CampaignDecisionManifest:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
        candidates = tuple(
            DecisionCandidate(
                candidate_id=str(item["candidate_id"]),
                strategy=str(item["strategy"]),
                state=str(item["state"]),
                input_head=str(item["input_head"]),
                output_head=str(item["output_head"]) if item.get("output_head") is not None else None,
                candidate_ref=str(item["candidate_ref"]) if item.get("candidate_ref") is not None else None,
                evidence_bundle_digest=(
                    str(item["evidence_bundle_digest"]) if item.get("evidence_bundle_digest") is not None else None
                ),
                evaluations=tuple(DecisionEvaluation(**row) for row in item.get("evaluations", ())),
                cost_usd=float(item["cost_usd"]),
                duration_s=float(item["duration_s"]),
            )
            for item in document["candidates"]
        )
        selection_doc = document["selection"]
        manifest = CampaignDecisionManifest(
            campaign_id=str(document["campaign_id"]),
            cell_id=str(document["cell_id"]),
            epoch=int(document["epoch"]),
            input_head=str(document["input_head"]),
            candidates=candidates,
            selection=DecisionSelection(
                winner=str(selection_doc["winner"]) if selection_doc.get("winner") is not None else None,
                reason=str(selection_doc["reason"]),
                ranking=tuple(str(item) for item in selection_doc.get("ranking", ())),
                refusals=tuple(str(item) for item in selection_doc.get("refusals", ())),
            ),
            independence=tuple(str(item) for item in document.get("independence", ())),
            cancelled=bool(document.get("cancelled", False)),
            schema_version=int(document.get("schema_version", 1)),
        )
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise CampaignDecisionError(f"cannot load campaign decision manifest: {error}") from error

    manifest.validate()
    expected = str(document.get("manifest_digest", ""))
    observed = manifest.digest()
    if expected != observed:
        raise CampaignDecisionError(
            f"campaign decision manifest digest mismatch: expected {expected!r}, observed {observed}"
        )
    return manifest


def verify_campaign_decision(
    path: Path,
    evidence_paths: Mapping[str, Path],
    *,
    repo: Path | None = None,
) -> CampaignDecisionManifest:
    """Re-load the decision and prove every answered candidate still has the bound bundle."""

    from swfactory.candidate_evidence import verify_candidate_evidence_bundle

    manifest = load_campaign_decision(path)
    answered = {candidate.candidate_id for candidate in manifest.candidates if candidate.answered}
    if set(evidence_paths) != answered:
        missing = answered - set(evidence_paths)
        extra = set(evidence_paths) - answered
        raise CampaignDecisionError(
            f"candidate evidence membership mismatch missing={sorted(missing)} extra={sorted(extra)}"
        )
    for candidate in manifest.candidates:
        if not candidate.answered:
            continue
        bundle = verify_candidate_evidence_bundle(evidence_paths[candidate.candidate_id], repo=repo)
        observed = bundle.digest()
        if observed != candidate.evidence_bundle_digest:
            raise CampaignDecisionError(
                f"{candidate.candidate_id}: evidence digest {observed} != decision {candidate.evidence_bundle_digest}"
            )
        _assert_bundle_matches(candidate, bundle)
    return manifest


def _decision_candidate(
    outcome: CandidateOutcome,
    bundle: CandidateEvidenceBundle | None,
) -> DecisionCandidate:
    answered = outcome.state == "ok" and bool(outcome.output_head) and outcome.output_head != outcome.input_head
    if answered and bundle is None:
        raise CampaignDecisionError(f"{outcome.logical_id}: answered candidate is missing retained evidence")
    if not answered and bundle is not None:
        raise CampaignDecisionError(f"{outcome.logical_id}: evidence bundle supplied for unanswered candidate")
    if bundle is not None:
        candidate = DecisionCandidate(
            candidate_id=outcome.logical_id,
            strategy=outcome.strategy.value,
            state=outcome.state,
            input_head=outcome.input_head,
            output_head=outcome.output_head,
            candidate_ref=outcome.candidate_ref,
            evidence_bundle_digest=bundle.digest(),
            evaluations=_evaluations(outcome),
            cost_usd=outcome.cost_usd,
            duration_s=outcome.duration_s,
        )
        _assert_bundle_matches(candidate, bundle)
        return candidate
    return DecisionCandidate(
        candidate_id=outcome.logical_id,
        strategy=outcome.strategy.value,
        state=outcome.state,
        input_head=outcome.input_head,
        output_head=outcome.output_head,
        candidate_ref=outcome.candidate_ref,
        evidence_bundle_digest=None,
        evaluations=_evaluations(outcome),
        cost_usd=outcome.cost_usd,
        duration_s=outcome.duration_s,
    )


def _evaluations(outcome: CandidateOutcome) -> tuple[DecisionEvaluation, ...]:
    return tuple(
        DecisionEvaluation(item.dimension.value, item.result, item.evidence)
        for item in sorted(outcome.evaluations, key=lambda item: item.dimension.value)
    )


def _assert_bundle_matches(candidate: DecisionCandidate, bundle: CandidateEvidenceBundle) -> None:
    if bundle.candidate_id != candidate.candidate_id:
        raise CampaignDecisionError(f"{candidate.candidate_id}: evidence bundle belongs to {bundle.candidate_id}")
    if bundle.input_head != candidate.input_head or bundle.output_head != candidate.output_head:
        raise CampaignDecisionError(f"{candidate.candidate_id}: evidence bundle Git lineage differs from campaign")
    if bundle.candidate_ref != candidate.candidate_ref:
        raise CampaignDecisionError(f"{candidate.candidate_id}: evidence bundle frozen ref differs from campaign")


def _valid_digest(value: str | None) -> bool:
    if value is None or not value.startswith(_DIGEST_PREFIX):
        return False
    suffix = value.removeprefix(_DIGEST_PREFIX)
    return len(suffix) == 64 and all(char in "0123456789abcdef" for char in suffix)


def _atomic_json(path: Path, document: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}-", dir=path.parent)
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        temporary.write_text(json.dumps(dict(document), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        if os.name == "posix":
            temporary.chmod(0o600)
        os.replace(temporary, path)
        if os.name == "posix":
            path.chmod(0o600)
    finally:
        temporary.unlink(missing_ok=True)
