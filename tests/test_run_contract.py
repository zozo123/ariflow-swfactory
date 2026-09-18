"""Fixed run-contract semantics for comparable candidate experiments."""

from __future__ import annotations

from dataclasses import replace

import pytest

from swfactory.evolution import CampaignError, Strategy, plan_requests, run_campaign
from swfactory.experiment_tree import ExperimentTreeError, stack_rounds
from swfactory.run_contract import RunContract, RunContractError


def _contract(*, runtime: str = "image:sha256:abc", env: str = "sha256:env-a") -> RunContract:
    return RunContract(
        argv=("uv", "run", "pytest", "-q"),
        runtime=runtime,
        env_fingerprint=env,
        cwd=".",
    )


def _outcome(request, head: str):
    from swfactory.evolution import CandidateOutcome, evaluation
    from swfactory.generations import Dimension

    return CandidateOutcome(
        logical_id=request.logical_id,
        strategy=request.strategy,
        state="ok",
        input_head=request.input_head,
        output_head=head,
        evaluations=(
            evaluation(Dimension.CORRECTNESS, passed=True, evidence="tests"),
            evaluation(Dimension.EVIDENCE, passed=True, evidence="bundle"),
        ),
    )


def test_contract_digest_is_canonical_and_round_trips() -> None:
    contract = _contract()
    document = contract.to_dict()

    assert len(contract.digest) == 64
    assert document["digest"] == contract.digest
    assert RunContract.from_dict(document) == contract


def test_contract_refuses_unsafe_working_directory_and_tampered_digest() -> None:
    with pytest.raises(RunContractError, match="inside the repository"):
        RunContract(("pytest",), "runtime", "env", cwd="../other").validate()

    document = _contract().to_dict()
    document["digest"] = "0" * 64
    with pytest.raises(RunContractError, match="digest"):
        RunContract.from_dict(document)


def test_contract_never_contains_raw_environment_values() -> None:
    contract = _contract(env="sha256:trusted-environment")
    document = contract.to_dict()

    assert set(document) == {
        "argv",
        "cwd",
        "runtime",
        "env_fingerprint",
        "schema_version",
        "digest",
    }
    assert "environment" not in document
    assert "env" not in document


def test_run_contract_changes_candidate_identity() -> None:
    first = plan_requests(
        campaign_id="round-0",
        cell_id="cell",
        epoch=1,
        input_head="base",
        strategies=(Strategy.REPAIR,),
        run_contract=_contract(env="sha256:env-a"),
    )[0]
    second = plan_requests(
        campaign_id="round-0",
        cell_id="cell",
        epoch=1,
        input_head="base",
        strategies=(Strategy.REPAIR,),
        run_contract=_contract(env="sha256:env-b"),
    )[0]

    assert first.logical_id != second.logical_id


def test_one_campaign_refuses_mixed_measurement_contracts() -> None:
    requests = plan_requests(
        campaign_id="round-0",
        cell_id="cell",
        epoch=1,
        input_head="base",
        strategies=(Strategy.REPAIR, Strategy.RETHINK),
        run_contract=_contract(),
    )
    drifted = (
        requests[0],
        replace(requests[1], run_contract=_contract(runtime="image:sha256:different")),
    )

    with pytest.raises(CampaignError, match="same run contract"):
        run_campaign(lambda request: _outcome(request, f"head-{request.strategy.value}"), drifted)


def test_report_carries_the_exact_contract_and_round_digest() -> None:
    contract = _contract()
    requests = plan_requests(
        campaign_id="round-0",
        cell_id="cell",
        epoch=1,
        input_head="base",
        strategies=(Strategy.REPAIR,),
        run_contract=contract,
    )
    report = run_campaign(
        lambda request: _outcome(request, "head-0"),
        requests,
        human_approved=True,
        parallel=False,
    )
    document = report.to_dict()

    assert document["schema_version"] == 3
    assert document["run_contract"]["digest"] == contract.digest
    assert document["experiment_round"]["contract_digest"] == contract.digest


def test_descendant_round_must_inherit_the_same_run_contract() -> None:
    first_contract = _contract()
    first_requests = plan_requests(
        campaign_id="round-0",
        cell_id="cell",
        epoch=1,
        input_head="base",
        strategies=(Strategy.REPAIR,),
        run_contract=first_contract,
    )
    first = run_campaign(
        lambda request: _outcome(request, "head-0"),
        first_requests,
        human_approved=True,
        parallel=False,
    )
    assert first.experiment_round is not None
    assert first.selection.winner is not None

    second_requests = plan_requests(
        campaign_id="round-1",
        cell_id="cell",
        epoch=1,
        input_head="head-0",
        strategies=(Strategy.REPAIR,),
        parent_candidate=first.selection.winner,
        depth=1,
        run_contract=_contract(env="sha256:drifted"),
    )
    second = run_campaign(
        lambda request: _outcome(request, "head-1"),
        second_requests,
        human_approved=True,
        parallel=False,
    )
    assert second.experiment_round is not None

    with pytest.raises(ExperimentTreeError, match="run contract changed"):
        stack_rounds((first.experiment_round, second.experiment_round))


def test_contract_presence_cannot_appear_or_disappear_mid_tree() -> None:
    contract = _contract()
    first_requests = plan_requests(
        campaign_id="round-0",
        cell_id="cell",
        epoch=1,
        input_head="base",
        strategies=(Strategy.REPAIR,),
        run_contract=contract,
    )
    first = run_campaign(
        lambda request: _outcome(request, "head-0"),
        first_requests,
        human_approved=True,
        parallel=False,
    )
    assert first.experiment_round is not None
    assert first.selection.winner is not None

    second_requests = plan_requests(
        campaign_id="round-1",
        cell_id="cell",
        epoch=1,
        input_head="head-0",
        strategies=(Strategy.REPAIR,),
        parent_candidate=first.selection.winner,
        depth=1,
    )
    second = run_campaign(
        lambda request: _outcome(request, "head-1"),
        second_requests,
        human_approved=True,
        parallel=False,
    )
    assert second.experiment_round is not None

    with pytest.raises(ExperimentTreeError, match="run contract changed"):
        stack_rounds((first.experiment_round, second.experiment_round))
