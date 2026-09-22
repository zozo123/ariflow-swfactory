from __future__ import annotations

import hashlib
from pathlib import Path

from swfactory.evolution import Strategy, plan_requests
from swfactory.generations import CampaignBudget
from swfactory.recursive_search import (
    ArtifactBlackboard,
    ArtifactKind,
    RoundSignal,
    StrategySignal,
    artifact_from_payload,
    extract_search_laws,
    load_blackboard,
    plan_adaptive_round,
    plan_next_round,
    write_blackboard,
)


def _strategy_signal(
    strategy: Strategy,
    *,
    attempts: int = 4,
    answered: int = 4,
    evidence: int = 4,
    required: int = 4,
    unique: int = 4,
) -> StrategySignal:
    return StrategySignal(
        strategy=strategy,
        attempts=attempts,
        answered=answered,
        evidence_complete=evidence,
        required_passes=required,
        unique_outputs=unique,
        cost_usd=1.0,
    )


def _round(
    *,
    depth: int = 0,
    disagreement: float = 0.9,
    novelty: float = 1.0,
    evidence: int = 2,
    required: int = 1,
    winner: Strategy | None = Strategy.REPAIR,
) -> RoundSignal:
    return RoundSignal(
        campaign_id=f"round-{depth}",
        depth=depth,
        attempts=4,
        answered=4,
        evidence_complete=evidence,
        required_passes=required,
        unique_outputs=4,
        disagreement=disagreement,
        novelty=novelty,
        cost_usd=2.0,
        winner_strategy=winner,
        strategies=(
            _strategy_signal(Strategy.REPAIR, evidence=evidence, required=required),
            _strategy_signal(Strategy.RETHINK, evidence=evidence, required=required),
            _strategy_signal(Strategy.SCRATCH, evidence=evidence, required=required),
        ),
    )


def test_blackboard_is_content_addressed_and_round_trips(tmp_path: Path) -> None:
    first = artifact_from_payload(
        kind=ArtifactKind.HYPOTHESIS,
        producer="agent-a",
        payload={"claim": "cache identity is not approval identity"},
        tags=("hypothesis",),
    )
    second = artifact_from_payload(
        kind=ArtifactKind.EVIDENCE,
        producer="verifier-a",
        payload={"result": "pass"},
        parents=(first.artifact_id,),
        tags=("evidence",),
    )
    board = ArtifactBlackboard((first, second))
    path = tmp_path / "blackboard.json"

    written = write_blackboard(path, board)
    loaded = load_blackboard(path)

    assert written == board.digest()
    assert loaded == board
    assert loaded.select(tags=("evidence",)) == (second,)


def test_search_laws_turn_disagreement_into_measurement() -> None:
    signal = _round(disagreement=0.9, evidence=1, required=1)

    laws = extract_search_laws((signal,), min_support=1)

    assert any(law.kind.value == "measure" for law in laws)
    assert all(law.authority == "exploration-only" for law in laws)


def test_next_round_preserves_competing_explanations_under_disagreement() -> None:
    plan = plan_next_round(
        (_round(disagreement=0.9, evidence=1, required=1),),
        depth=1,
        input_head="abc123",
        max_candidates=3,
        max_parallel=3,
    )

    assert plan.posture.value == "measure"
    assert len(plan.strategies) >= 2
    assert plan.max_parallel >= 2
    assert plan.authority == "exploration-only"


def test_adaptive_round_binds_phase_swarm_and_search_provenance() -> None:
    board = ArtifactBlackboard()
    plan = plan_adaptive_round(
        (_round(disagreement=0.9, evidence=1, required=1),),
        depth=1,
        input_head="abc123",
        blackboard=board,
        max_candidates=3,
        max_parallel=3,
    )

    assert plan.phase is not None
    assert plan.phase_assessment_digest is not None
    assert plan.phase_assessment_digest.startswith("sha256:")
    assert plan.swarm_plan_digest is not None
    assert plan.swarm_plan_digest.startswith("sha256:")
    assert plan.swarm_plan is not None
    assert plan.swarm_plan.digest() == plan.swarm_plan_digest
    assert plan.search_provenance_digest is not None
    assert plan.search_provenance_digest.startswith("sha256:")
    assert plan.search_provenance is not None
    assert plan.search_provenance.digest() == plan.search_provenance_digest
    assert plan.population_manifest_digest is not None
    assert plan.population_manifest is not None
    assert plan.population_manifest.digest() == plan.population_manifest_digest
    assert plan.population_manifest.swarm_plan_digest == plan.swarm_plan_digest
    assert plan.population_manifest.search_provenance_digest == plan.search_provenance_digest
    document = plan.to_dict()
    assert document["swarm_plan"]["authority"] == "search-only"
    assert document["search_provenance"]["authority"] == "search-only"
    assert document["population_manifest"]["authority"] == "search-only"
    assert len(document["population_manifest"]["tasks"]) >= plan.max_parallel
    assert plan.estimated_compute_units > 0


def test_search_provenance_changes_candidate_question_identity() -> None:
    budget = CampaignBudget(max_candidates=3)
    first = plan_requests(
        campaign_id="campaign",
        cell_id="cell_0123456789abcdef01234567",
        epoch=1,
        input_head="abc123",
        strategies=(Strategy.REPAIR,),
        budget=budget,
        search_provenance_digest="sha256:" + "1" * 64,
    )[0]
    second = plan_requests(
        campaign_id="campaign",
        cell_id="cell_0123456789abcdef01234567",
        epoch=1,
        input_head="abc123",
        strategies=(Strategy.REPAIR,),
        budget=budget,
        search_provenance_digest="sha256:" + "2" * 64,
    )[0]

    assert first.logical_id != second.logical_id


def test_legacy_candidate_identity_is_unchanged_without_search_provenance() -> None:
    request = plan_requests(
        campaign_id="campaign",
        cell_id="cell_0123456789abcdef01234567",
        epoch=1,
        input_head="abc123",
        strategies=(Strategy.REPAIR,),
        budget=CampaignBudget(max_candidates=3),
    )[0]
    raw = "\0".join(
        (
            "campaign",
            "cell_0123456789abcdef01234567",
            "1",
            Strategy.REPAIR.value,
            "abc123",
            "",
            "",
            "",
            "0",
        )
    ).encode()

    assert request.logical_id == "cand_" + hashlib.sha256(raw).hexdigest()[:24]


def test_adaptive_round_clamps_parallelism_to_its_default_population_budget() -> None:
    plan = plan_adaptive_round(
        (),
        depth=0,
        input_head="abc123",
        max_candidates=1,
        max_parallel=99,
    )

    assert plan.max_parallel == 1


def test_future_factory_contract_keeps_one_root_search_authority() -> None:
    root = Path(__file__).resolve().parents[1]
    lines = (root / "config" / "future-factory.yaml").read_text(encoding="utf-8").splitlines()

    assert [line for line in lines if line.startswith("authority:")] == ["authority: search-only"]
    assert "authority_envelope:" in lines
