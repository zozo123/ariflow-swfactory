from __future__ import annotations

import time

import pytest

from swfactory.population_execution import (
    PopulationCancellation,
    PopulationExecutionPolicy,
    PopulationExecutor,
)
from swfactory.population_manifest import (
    BehaviorReceipt,
    PopulationManifestError,
    build_population_manifest,
)
from swfactory.provider_binding import ProviderChoiceSet, bind_population_manifest
from swfactory.swarm_dynamics import (
    AgentRole,
    ComputeTier,
    ContextPolicy,
    PopulationLane,
    SwarmPlan,
)


def _plan(*, verifier_count: int = 1) -> SwarmPlan:
    return SwarmPlan(
        phase="liquid",
        mode="coordinate",
        lanes=(
            PopulationLane(
                role=AgentRole.EXPLORER,
                compute_tier=ComputeTier.CHEAP,
                count=2,
                context=ContextPolicy.FRESH,
                temperature=1.1,
                independent_verification=False,
                diversity_axes=("provider", "model", "runtime", "prompt"),
            ),
            PopulationLane(
                role=AgentRole.VERIFIER,
                compute_tier=ComputeTier.DEEP,
                count=verifier_count,
                context=ContextPolicy.FRESH,
                temperature=0.0,
                independent_verification=True,
                diversity_axes=("provider", "model", "runtime", "verifier"),
            ),
        ),
        selected_hotspots=(),
        crystals_to_verify=(),
        stop_new_work=False,
        estimated_compute_units=12.0,
        reason="managed population test",
    )


def _bound(*, verifier_count: int = 1, diverse: bool = True):
    manifest = build_population_manifest(
        _plan(verifier_count=verifier_count),
        search_provenance_digest="sha256:" + "a" * 64,
    )
    choices = ProviderChoiceSet(
        provider=("provider-a", "provider-b") if diverse else ("provider-a",),
        model=("fast", "deep", "critic") if diverse else ("same-model",),
        runtime=("linux-a", "linux-b") if diverse else ("linux-a",),
        prompt=("direct", "counterfactual"),
        verifier=("unit", "adversarial") if diverse else ("unit",),
    )
    binding = bind_population_manifest(manifest, choices=choices)
    return manifest, binding


def test_population_executor_preserves_manifest_order_and_emits_telemetry() -> None:
    manifest, binding = _bound()

    def runner(task):
        # Complete in a different order; fan-in must still follow manifest order.
        if task.task_id == manifest.tasks[0].task_id:
            time.sleep(0.01)
        return BehaviorReceipt(
            task_id=task.task_id,
            state="answered",
            behavior_signature=(task.task_id, task.model or "-", task.runtime or "-"),
            candidate_digest="sha256:" + ("1" if task.task_id == manifest.tasks[0].task_id else "2") * 64,
            cost_usd=0.1,
        )

    report = PopulationExecutor(
        runner,
        PopulationExecutionPolicy(max_parallel=3),
    ).execute(manifest=manifest, binding=binding)

    assert report.authority == "search-only"
    assert report.scheduler == "airflow"
    assert [receipt.task_id for receipt in report.receipts] == [task.task_id for task in manifest.tasks]
    assert report.telemetry.receipts == len(manifest.tasks)
    assert report.telemetry.answered == len(manifest.tasks)
    assert report.population_manifest_digest == manifest.digest()
    assert report.provider_binding_digest == binding.digest()
    assert report.digest().startswith("sha256:")
    for receipt, task in zip(report.receipts, binding.tasks, strict=True):
        assert receipt.provider == task.provider
        assert receipt.model == task.model
        assert receipt.runtime == task.runtime


def test_population_executor_rejects_provider_identity_drift() -> None:
    manifest, binding = _bound()

    def runner(task):
        return BehaviorReceipt(
            task_id=task.task_id,
            state="answered",
            behavior_signature=("drift",),
            provider="not-the-bound-provider",
        )

    with pytest.raises(PopulationManifestError, match="does not match bound"):
        PopulationExecutor(runner).execute(manifest=manifest, binding=binding)


def test_population_executor_converts_runner_exceptions_to_failed_receipts() -> None:
    manifest, binding = _bound()

    def runner(_task):
        raise RuntimeError("provider crashed")

    report = PopulationExecutor(runner).execute(manifest=manifest, binding=binding)

    assert report.telemetry.receipts == len(manifest.tasks)
    assert report.telemetry.answered == 0
    assert all(receipt.state == "failed" for receipt in report.receipts)


def test_population_executor_refuses_collapsed_independent_verifiers() -> None:
    manifest, binding = _bound(verifier_count=2, diverse=False)

    def runner(task):
        return BehaviorReceipt(
            task_id=task.task_id,
            state="answered",
            behavior_signature=("same-verifier",),
        )

    with pytest.raises(PopulationManifestError, match="independent verifier bindings collapsed"):
        PopulationExecutor(runner).execute(manifest=manifest, binding=binding)


def test_population_executor_can_cancel_before_start_without_inventing_receipts() -> None:
    manifest, binding = _bound()
    cancellation = PopulationCancellation()
    cancellation.cancel()

    report = PopulationExecutor(lambda _task: pytest.fail("runner must not be called")).execute(
        manifest=manifest,
        binding=binding,
        cancellation=cancellation,
    )

    assert report.cancelled is True
    assert report.started_tasks == 0
    assert report.receipts == ()
    assert report.telemetry.receipts == 0
    assert report.telemetry.total_tasks == len(manifest.tasks)
