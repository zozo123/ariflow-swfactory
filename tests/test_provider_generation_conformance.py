from __future__ import annotations

import pytest

from swfactory.generation_policy import ChildExperiment, GenerationLimits, GenerationRefused, promotion_decision
from swfactory.provider_conformance import (
    LIFECYCLE_CHECKS,
    PROVIDER_CONTRACT_VERSION,
    ProviderCapabilityDrift,
    ProviderContract,
    compare_provider_contract,
    contract_document,
)


def _provider(**overrides: object) -> ProviderContract:
    capabilities = {name: True for name in LIFECYCLE_CHECKS}
    capabilities.update(overrides)
    return ProviderContract("fixture-provider", PROVIDER_CONTRACT_VERSION, capabilities)


def test_matching_provider_fixture_is_admitted() -> None:
    expected = _provider()
    observed = _provider()
    compare_provider_contract(expected, observed)
    document = contract_document(expected)
    assert document["provider"] == "fixture-provider"
    assert document["contract_version"] == 1


@pytest.mark.parametrize("capability", LIFECYCLE_CHECKS)
def test_every_required_capability_drift_fails_closed(capability: str) -> None:
    expected = _provider()
    observed = _provider(**{capability: False})
    with pytest.raises(ProviderCapabilityDrift) as error:
        compare_provider_contract(expected, observed)
    message = str(error.value)
    assert "fixture-provider" in message
    assert capability in message
    assert "v1" in message
    assert "False" in message


def test_provider_contract_version_drift_is_explicit() -> None:
    expected = _provider()
    observed = ProviderContract("fixture-provider", 2, expected.capabilities)
    with pytest.raises(ProviderCapabilityDrift, match="contract version drift"):
        compare_provider_contract(expected, observed)


def _experiment(**overrides: object) -> ChildExperiment:
    values: dict[str, object] = {
        "parent_generation": "gen_parent",
        "child_generation": "gen_child",
        "depth": 1,
        "budget": 20,
        "experiment_id": "exp_42",
        "artifact_digest": "sha256:artifact",
        "inputs_digest": "sha256:inputs",
    }
    values.update(overrides)
    return ChildExperiment(**values)  # type: ignore[arg-type]


def test_child_cannot_self_promote() -> None:
    with pytest.raises(GenerationRefused, match="only the parent"):
        promotion_decision(
            _experiment(),
            requester_generation="gen_child",
            cell_id="cell_generation",
            epoch=3,
            metrics={"quality": 0.95},
            thresholds={"quality": 0.9},
        )


@pytest.mark.parametrize(
    "experiment",
    [_experiment(depth=4), _experiment(budget=101)],
)
def test_depth_and_budget_are_bounded(experiment: ChildExperiment) -> None:
    with pytest.raises(GenerationRefused):
        promotion_decision(
            experiment,
            requester_generation="gen_parent",
            cell_id="cell_generation",
            epoch=3,
            metrics={"quality": 0.95},
            thresholds={"quality": 0.9},
            limits=GenerationLimits(max_depth=3, max_budget=100),
        )


def test_parent_promotion_is_deterministic_idempotent_evidence() -> None:
    kwargs = dict(
        requester_generation="gen_parent",
        cell_id="cell_generation",
        epoch=3,
        metrics={"quality": 0.95, "reliability": 0.99},
        thresholds={"quality": 0.9, "reliability": 0.98},
    )
    first = promotion_decision(_experiment(), **kwargs)
    second = promotion_decision(_experiment(), **kwargs)
    assert first == second
    assert first.evidence["authority"] == "parent"
    assert first.evidence["artifact_digest"] == "sha256:artifact"
    assert first.operation.kind == "generation_promote"


def test_failed_evaluation_cannot_promote() -> None:
    with pytest.raises(GenerationRefused, match="thresholds failed"):
        promotion_decision(
            _experiment(),
            requester_generation="gen_parent",
            cell_id="cell_generation",
            epoch=3,
            metrics={"quality": 0.5},
            thresholds={"quality": 0.9},
        )
