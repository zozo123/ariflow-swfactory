from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from swfactory.cli import app
from swfactory.population_manifest import (
    BehaviorReceipt,
    PopulationManifestError,
    build_population_manifest,
    summarize_population,
)
from swfactory.swarm_dynamics import (
    AgentRole,
    ComputeTier,
    ContextPolicy,
    PopulationLane,
    SwarmPlan,
)


def _plan() -> SwarmPlan:
    return SwarmPlan(
        phase="gas",
        mode="diverge",
        lanes=(
            PopulationLane(
                role=AgentRole.EXPLORER,
                compute_tier=ComputeTier.CHEAP,
                count=2,
                context=ContextPolicy.FRESH,
                temperature=1.2,
                independent_verification=False,
                diversity_axes=("model", "prompt", "runtime"),
            ),
            PopulationLane(
                role=AgentRole.VERIFIER,
                compute_tier=ComputeTier.DEEP,
                count=1,
                context=ContextPolicy.FRESH,
                temperature=0.0,
                independent_verification=True,
                diversity_axes=("model", "runtime", "verifier"),
                focus_hotspots=("hotspot-a",),
            ),
        ),
        selected_hotspots=(),
        crystals_to_verify=(),
        stop_new_work=False,
        estimated_compute_units=8.0,
        reason="test population",
    )


def test_population_manifest_materializes_every_lane_deterministically() -> None:
    plan = _plan()
    provenance = "sha256:" + "a" * 64

    first = build_population_manifest(plan, search_provenance_digest=provenance)
    second = build_population_manifest(plan, search_provenance_digest=provenance)

    assert first == second
    assert first.authority == "search-only"
    assert first.scheduler == "airflow"
    assert len(first.tasks) == 3
    assert len({task.task_id for task in first.tasks}) == 3
    assert len({task.variant_digest for task in first.tasks}) == 3
    assert len({task.diversity_coordinates for task in first.tasks}) == 3
    for task in first.tasks:
        assert tuple(axis for axis, _seed in task.diversity_coordinates) == task.diversity_axes
        assert all(0 <= seed <= 0x7FFFFFFF for _axis, seed in task.diversity_coordinates)
    assert first.digest().startswith("sha256:")
    verifier = next(task for task in first.tasks if task.independent_verification)
    assert verifier.role == AgentRole.VERIFIER
    assert verifier.context == ContextPolicy.FRESH
    assert verifier.focus_hotspots == ("hotspot-a",)


def test_population_telemetry_measures_effective_independence_not_agent_count() -> None:
    manifest = build_population_manifest(
        _plan(),
        search_provenance_digest="sha256:" + "b" * 64,
    )
    first, second, verifier = manifest.tasks
    receipts = (
        BehaviorReceipt(
            task_id=first.task_id,
            state="answered",
            behavior_signature=("same-plan", "same-tool"),
            candidate_digest="sha256:" + "1" * 64,
            cost_usd=0.1,
            duration_s=2.0,
        ),
        BehaviorReceipt(
            task_id=second.task_id,
            state="answered",
            behavior_signature=("same-plan", "same-tool"),
            candidate_digest="sha256:" + "1" * 64,
            cost_usd=0.1,
            duration_s=2.0,
        ),
        BehaviorReceipt(
            task_id=verifier.task_id,
            state="answered",
            behavior_signature=("independent-verifier", "fresh-runtime"),
            candidate_digest="sha256:" + "2" * 64,
            evidence_digest="sha256:" + "e" * 64,
            cost_usd=1.0,
            duration_s=5.0,
        ),
    )

    telemetry = summarize_population(manifest, receipts, require_complete=True)

    assert telemetry.total_tasks == 3
    assert telemetry.answered == 3
    assert telemetry.independent_verifier_answers == 1
    assert telemetry.unique_candidates == 2
    assert 1.0 < telemetry.effective_independent_search < 3.0
    assert 0.0 < telemetry.mean_correlation < 1.0
    assert telemetry.candidate_disagreement == 0.5
    assert telemetry.total_cost_usd == 1.2
    assert telemetry.manifest_digest == manifest.digest()


def test_population_receipts_must_belong_to_the_manifest() -> None:
    manifest = build_population_manifest(
        _plan(),
        search_provenance_digest="sha256:" + "c" * 64,
    )
    foreign = BehaviorReceipt(
        task_id="pop_0123456789abcdef01234567",
        state="answered",
        behavior_signature=("foreign",),
    )

    with pytest.raises(PopulationManifestError, match="unknown population task"):
        summarize_population(manifest, (foreign,))


def test_independent_verification_cannot_inherit_context() -> None:
    bad = SwarmPlan(
        phase="critical",
        mode="measure",
        lanes=(
            PopulationLane(
                role=AgentRole.VERIFIER,
                compute_tier=ComputeTier.DEEP,
                count=1,
                context=ContextPolicy.INHERIT,
                temperature=0.0,
                independent_verification=True,
                diversity_axes=("model", "runtime"),
            ),
        ),
        selected_hotspots=(),
        crystals_to_verify=(),
        stop_new_work=False,
        estimated_compute_units=4.0,
        reason="invalid verifier context",
    )

    with pytest.raises(PopulationManifestError, match="cannot inherit"):
        build_population_manifest(
            bad,
            search_provenance_digest="sha256:" + "d" * 64,
        )


def test_diversity_coordinates_change_across_replicas_but_replay_exactly() -> None:
    plan = _plan()
    provenance = "sha256:" + "f" * 64

    manifest = build_population_manifest(plan, search_provenance_digest=provenance)
    replay = build_population_manifest(plan, search_provenance_digest=provenance)

    explorers = [task for task in manifest.tasks if task.role == AgentRole.EXPLORER]
    replay_explorers = [task for task in replay.tasks if task.role == AgentRole.EXPLORER]

    assert len(explorers) == 2
    assert explorers[0].diversity_coordinates != explorers[1].diversity_coordinates
    assert [task.diversity_coordinates for task in explorers] == [
        task.diversity_coordinates for task in replay_explorers
    ]



def test_population_summarize_cli_emits_reusable_telemetry(tmp_path) -> None:
    manifest = build_population_manifest(
        _plan(),
        search_provenance_digest="sha256:" + "9" * 64,
    )
    plan_path = tmp_path / "plan.json"
    receipts_path = tmp_path / "receipts.json"
    plan_path.write_text(
        json.dumps({"plan": {"population_manifest": manifest.canonical_dict()}}),
        encoding="utf-8",
    )
    receipts = [
        {
            "task_id": task.task_id,
            "state": "answered",
            "behavior_signature": [task.task_id, task.role.value],
            "candidate_digest": "sha256:" + str(index + 1) * 64,
            "cost_usd": 0.1,
            "duration_s": 1.0,
        }
        for index, task in enumerate(manifest.tasks)
    ]
    receipts_path.write_text(json.dumps(receipts), encoding="utf-8")

    result = CliRunner().invoke(
        app,
        [
            "population-summarize",
            str(plan_path),
            str(receipts_path),
            "--require-complete",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    document = json.loads(result.stdout)
    assert document["manifest_digest"] == manifest.digest()
    assert document["answered"] == len(manifest.tasks)
    assert document["telemetry_digest"].startswith("sha256:")
    assert document["authority"] == "search-only"


def test_population_summarize_cli_refuses_incomplete_receipts(tmp_path) -> None:
    manifest = build_population_manifest(
        _plan(),
        search_provenance_digest="sha256:" + "8" * 64,
    )
    plan_path = tmp_path / "plan.json"
    receipts_path = tmp_path / "receipts.json"
    plan_path.write_text(
        json.dumps({"plan": {"population_manifest": manifest.canonical_dict()}}),
        encoding="utf-8",
    )
    receipts_path.write_text(
        json.dumps(
            [
                {
                    "task_id": manifest.tasks[0].task_id,
                    "state": "answered",
                    "behavior_signature": ["partial"],
                }
            ]
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "population-summarize",
            str(plan_path),
            str(receipts_path),
            "--require-complete",
            "--json",
        ],
    )

    assert result.exit_code == 2
    assert "population receipts incomplete" in result.output
