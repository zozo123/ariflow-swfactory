from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from swfactory.cli import app
from swfactory.population_manifest import PopulationManifestError, build_population_manifest
from swfactory.provider_binding import (
    ProviderChoiceSet,
    bind_population_manifest,
    provider_choices_from_document,
)
from swfactory.swarm_dynamics import (
    AgentRole,
    ComputeTier,
    ContextPolicy,
    PopulationLane,
    SwarmPlan,
)


def _manifest():
    plan = SwarmPlan(
        phase="gas",
        mode="diverge",
        lanes=(
            PopulationLane(
                role=AgentRole.EXPLORER,
                compute_tier=ComputeTier.CHEAP,
                count=3,
                context=ContextPolicy.FRESH,
                temperature=1.1,
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
            ),
        ),
        selected_hotspots=(),
        crystals_to_verify=(),
        stop_new_work=False,
        estimated_compute_units=12.0,
        reason="binding test",
    )
    return build_population_manifest(
        plan,
        search_provenance_digest="sha256:" + "a" * 64,
    )


def _choices() -> ProviderChoiceSet:
    return ProviderChoiceSet(
        model=("fast", "deep", "critic"),
        prompt=("direct", "counterfactual", "decompose"),
        runtime=("linux-a", "linux-b"),
        verifier=("unit", "property", "adversarial"),
    )


def test_provider_binding_is_deterministic_and_preserves_task_identity() -> None:
    manifest = _manifest()

    first = bind_population_manifest(manifest, choices=_choices())
    second = bind_population_manifest(manifest, choices=_choices())

    assert first == second
    assert first.authority == "search-only"
    assert first.scheduler == "airflow"
    assert first.population_manifest_digest == manifest.digest()
    assert [task.task_id for task in first.tasks] == [task.task_id for task in manifest.tasks]
    assert [task.variant_digest for task in first.tasks] == [task.variant_digest for task in manifest.tasks]
    assert first.digest().startswith("sha256:")
    assert all(task.binding_digest.startswith("sha256:") for task in first.tasks)


def test_replica_coordinates_drive_real_provider_variation() -> None:
    bound = bind_population_manifest(_manifest(), choices=_choices())
    explorers = bound.tasks[:3]

    combinations = {(task.model, task.prompt_variant, task.runtime) for task in explorers}
    assert len(combinations) > 1


def test_declared_required_axis_without_choices_is_refused() -> None:
    manifest = _manifest()
    choices = ProviderChoiceSet(
        prompt=("direct",),
        runtime=("linux",),
    )

    with pytest.raises(PopulationManifestError, match="no provider choices: model"):
        bind_population_manifest(manifest, choices=choices)


def test_binding_does_not_require_axes_a_lane_never_declared() -> None:
    manifest = _manifest()

    bound = bind_population_manifest(
        manifest,
        choices=_choices(),
        required_axes=("provider", "model", "runtime"),
    )

    assert all(task.provider is None for task in bound.tasks)
    assert all(task.model is not None for task in bound.tasks)
    assert all(task.runtime is not None for task in bound.tasks)


def test_cli_binds_persisted_population_manifest(tmp_path) -> None:
    manifest = _manifest()
    plan_path = tmp_path / "plan.json"
    choices_path = tmp_path / "choices.json"
    plan_path.write_text(
        json.dumps({"plan": {"population_manifest": manifest.canonical_dict()}}),
        encoding="utf-8",
    )
    choices_path.write_text(
        json.dumps(
            {
                "model": ["fast", "deep", "critic"],
                "prompt": ["direct", "counterfactual", "decompose"],
                "runtime": ["linux-a", "linux-b"],
                "verifier": ["unit", "property", "adversarial"],
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        ["population-bind", str(plan_path), str(choices_path), "--json"],
    )

    assert result.exit_code == 0, result.output
    document = json.loads(result.stdout)
    assert document["authority"] == "search-only"
    assert document["scheduler"] == "airflow"
    assert document["population_manifest_digest"] == manifest.digest()
    assert document["provider_binding_digest"].startswith("sha256:")
    assert len(document["binding"]["tasks"]) == len(manifest.tasks)


def test_cli_refuses_missing_required_provider_choice(tmp_path) -> None:
    manifest = _manifest()
    plan_path = tmp_path / "plan.json"
    choices_path = tmp_path / "choices.json"
    plan_path.write_text(
        json.dumps({"plan": {"population_manifest": manifest.canonical_dict()}}),
        encoding="utf-8",
    )
    choices_path.write_text(
        json.dumps({"runtime": ["linux-a"]}),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        ["population-bind", str(plan_path), str(choices_path), "--json"],
    )

    assert result.exit_code == 2
    assert "no provider choices: model" in result.output


def test_programmatic_provider_choices_reject_duplicate_values() -> None:
    choices = ProviderChoiceSet(
        model=("same", "same"),
        runtime=("linux",),
    )

    with pytest.raises(PopulationManifestError, match="must be distinct"):
        bind_population_manifest(_manifest(), choices=choices)


def test_persisted_provider_choices_reject_non_string_values() -> None:
    with pytest.raises(PopulationManifestError, match="strings only"):
        provider_choices_from_document(
            {
                "model": ["fast", 7],
                "runtime": ["linux"],
            }
        )
