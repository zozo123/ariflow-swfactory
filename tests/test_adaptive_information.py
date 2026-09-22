from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from swfactory.adaptive_information import (
    InformationBudgetPolicy,
    budget_from_manifest,
    evaluate_information_budget,
    load_information_budget,
    write_information_budget,
)
from swfactory.cli import app
from swfactory.phase_control import ControlMode
from swfactory.population_execution import (
    PopulationExecutionReport,
    write_population_execution_report,
)
from swfactory.population_manifest import (
    BehaviorReceipt,
    build_population_manifest,
    summarize_population,
)
from swfactory.swarm_dynamics import (
    AgentRole,
    ComputeTier,
    ContextPolicy,
    PopulationLane,
    SwarmBudget,
    SwarmPlan,
)


def _plan(
    *,
    explorer_count: int = 2,
    verifier_count: int = 0,
    explorer_tier: ComputeTier = ComputeTier.CHEAP,
) -> SwarmPlan:
    lanes = [
        PopulationLane(
            role=AgentRole.EXPLORER,
            compute_tier=explorer_tier,
            count=explorer_count,
            context=ContextPolicy.FRESH,
            temperature=1.0,
            independent_verification=False,
            diversity_axes=("model", "prompt", "runtime"),
        )
    ]
    if verifier_count:
        lanes.append(
            PopulationLane(
                role=AgentRole.VERIFIER,
                compute_tier=ComputeTier.DEEP,
                count=verifier_count,
                context=ContextPolicy.FRESH,
                temperature=0.0,
                independent_verification=True,
                diversity_axes=("model", "runtime", "verifier"),
            )
        )
    return SwarmPlan(
        phase="liquid",
        mode="coordinate",
        lanes=tuple(lanes),
        selected_hotspots=(),
        crystals_to_verify=(),
        stop_new_work=False,
        estimated_compute_units=float(explorer_count) + 8.0 * verifier_count,
        reason="adaptive information test",
    )


def _report(
    *,
    explorer_count: int = 2,
    verifier_count: int = 0,
    duplicate_explorers: bool = False,
    verifier_answered: bool = True,
    cancelled: bool = False,
):
    manifest = build_population_manifest(
        _plan(explorer_count=explorer_count, verifier_count=verifier_count),
        search_provenance_digest="sha256:" + "a" * 64,
    )
    receipts = []
    for task in manifest.tasks:
        if task.role == AgentRole.VERIFIER and not verifier_answered:
            receipts.append(
                BehaviorReceipt(
                    task_id=task.task_id,
                    state="failed",
                    provider="provider",
                    model="verifier",
                    runtime="runtime",
                )
            )
            continue
        candidate_char = (
            "1"
            if duplicate_explorers and task.role == AgentRole.EXPLORER
            else f"{(task.replica_index + task.lane_index + 1) % 10}"
        )
        signature = (
            ("same-path", "same-tool")
            if duplicate_explorers and task.role == AgentRole.EXPLORER
            else (task.task_id, task.role.value)
        )
        receipts.append(
            BehaviorReceipt(
                task_id=task.task_id,
                state="answered",
                behavior_signature=signature,
                candidate_digest="sha256:" + candidate_char * 64,
                evidence_digest=(
                    "sha256:" + "e" * 64
                    if task.independent_verification
                    else None
                ),
                provider="provider",
                model=task.role.value,
                runtime="runtime",
                cost_usd=0.1 if task.compute_tier == ComputeTier.CHEAP else 0.8,
                duration_s=1.0,
            )
        )
    telemetry = summarize_population(
        manifest,
        tuple(receipts),
        require_complete=True,
    )
    report = PopulationExecutionReport(
        population_manifest_digest=manifest.digest(),
        provider_binding_digest="sha256:" + "b" * 64,
        receipts=tuple(receipts),
        telemetry=telemetry,
        cancelled=cancelled,
        started_tasks=len(manifest.tasks),
    )
    report.validate()
    return manifest, report


def test_correlated_settled_population_stops_buying_duplicate_search() -> None:
    manifest, report = _report(duplicate_explorers=True)
    base = SwarmBudget(
        max_agents=8,
        max_parallel=4,
        max_deep_agents=2,
        max_exact_replays=1,
        max_compute_units=32.0,
    )

    decision = evaluate_information_budget(
        report,
        base_budget=base,
        manifest=manifest,
    )

    assert decision.mode_override == ControlMode.DRAIN
    assert decision.stop_new_work is True
    assert decision.next_budget.max_agents == 1
    assert decision.next_budget.max_compute_units == 1.0
    assert decision.lanes[0].mean_correlation == 1.0
    assert decision.lanes[0].recommended_count == 0
    assert decision.lanes[0].marginal_information_value == 0.0


def test_unresolved_disagreement_reserves_independent_verification() -> None:
    manifest, report = _report(explorer_count=2, verifier_count=0)
    assert report.telemetry.candidate_disagreement == 1.0
    base = SwarmBudget(
        max_agents=6,
        max_parallel=4,
        max_deep_agents=2,
        max_exact_replays=1,
        max_compute_units=24.0,
    )

    decision = evaluate_information_budget(
        report,
        base_budget=base,
        manifest=manifest,
    )

    assert decision.mode_override == ControlMode.MEASURE
    assert decision.verifier_reserve == 1
    assert decision.next_budget.max_deep_agents >= 1
    assert decision.next_budget.max_compute_units >= 8.0
    assert dict(decision.role_caps)[AgentRole.VERIFIER] >= 1
    assert decision.stop_new_work is False


def test_high_correlation_caps_a_measured_role_at_one_replica() -> None:
    manifest, report = _report(
        explorer_count=3,
        verifier_count=1,
        duplicate_explorers=True,
    )
    base = budget_from_manifest(manifest)

    decision = evaluate_information_budget(
        report,
        base_budget=base,
        manifest=manifest,
    )

    explorer = next(lane for lane in decision.lanes if lane.role == AgentRole.EXPLORER)
    assert explorer.recommended_count <= 1
    assert dict(decision.role_caps)[AgentRole.EXPLORER] <= 1
    assert decision.next_budget.max_agents <= base.max_agents


def test_cancelled_population_drains_before_new_search() -> None:
    manifest, report = _report(cancelled=True)
    decision = evaluate_information_budget(
        report,
        base_budget=budget_from_manifest(manifest),
        manifest=manifest,
    )

    assert decision.mode_override == ControlMode.DRAIN
    assert decision.stop_new_work is True
    assert decision.lanes == ()
    assert decision.next_budget.max_agents == 1


def test_information_budget_never_widens_outer_human_budget() -> None:
    manifest, report = _report(explorer_count=2)
    base = SwarmBudget(
        max_agents=2,
        max_parallel=1,
        max_deep_agents=0,
        max_exact_replays=0,
        max_compute_units=2.0,
    )

    decision = evaluate_information_budget(
        report,
        base_budget=base,
        manifest=manifest,
    )

    assert decision.next_budget.max_agents <= 2
    assert decision.next_budget.max_parallel <= 1
    assert decision.next_budget.max_deep_agents == 0
    assert decision.next_budget.max_exact_replays == 0
    assert decision.next_budget.max_compute_units <= 2.0


def test_information_budget_round_trips_and_rejects_digest_tampering(tmp_path) -> None:
    manifest, report = _report(verifier_count=1)
    decision = evaluate_information_budget(
        report,
        base_budget=budget_from_manifest(manifest),
        manifest=manifest,
        policy=InformationBudgetPolicy(min_value_per_compute_unit=0.02),
    )
    path = tmp_path / "information-budget.json"

    digest = write_information_budget(path, decision)
    restored = load_information_budget(path)

    assert restored == decision
    assert digest == decision.digest()

    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["decision_digest"] = "sha256:" + "0" * 64
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="decision digest mismatch"):
        load_information_budget(path)



def test_population_budget_cli_replays_lane_economics(tmp_path) -> None:
    manifest, report = _report(explorer_count=3, verifier_count=1)
    plan_path = tmp_path / "plan.json"
    report_path = tmp_path / "execution.json"
    decision_path = tmp_path / "decision.json"
    plan_path.write_text(
        json.dumps({"plan": {"population_manifest": manifest.canonical_dict()}}),
        encoding="utf-8",
    )
    write_population_execution_report(report_path, report)

    result = CliRunner().invoke(
        app,
        [
            "population-budget",
            str(plan_path),
            str(report_path),
            "--output",
            str(decision_path),
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    document = json.loads(result.stdout)
    assert document["authority"] == "search-only"
    assert document["source_manifest_digest"] == manifest.digest()
    assert document["source_execution_report_digest"] == report.digest()
    assert document["decision_digest"].startswith("sha256:")
    assert document["next_budget"]["max_agents"] <= len(manifest.tasks)
    assert decision_path.is_file()
    assert load_information_budget(decision_path).digest() == document["decision_digest"]
