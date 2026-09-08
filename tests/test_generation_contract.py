"""The contract behind the `factory.generations` capability claim.

The claim is experimental and its follow_up demands a parent-approved adoption/refusal/rollback
experiment run with no parent production credentials.  Until that exists, the only thing the claim
may rest on is this contract: a child generation has a stable identity, a bounded campaign, no
promotion without every required dimension *and* a human, and a credential set that cannot reach
the parent's production authority.  The inventory cited "generation contract tests" for as long as
no such test existed, which is exactly the rot the reference check now refuses.
"""

from __future__ import annotations

from swfactory.generations import (
    CampaignBudget,
    Dimension,
    Evaluation,
    Generation,
    candidate_credentials,
    promotable,
)


def _generation(**overrides: str) -> Generation:
    fields = {
        "source_sha": "a" * 40,
        "image_digest": "sha256:" + "b" * 64,
        "blueprint_digest": "sha256:" + "c" * 64,
        "policy_digest": "sha256:" + "d" * 64,
        "schema_version": "1",
    }
    return Generation(**{**fields, **overrides})


def _passing() -> list[Evaluation]:
    return [Evaluation(dimension, "pass", f"evidence/{dimension.value}") for dimension in Dimension]


def test_generation_identity_is_derived_from_everything_that_governs_it() -> None:
    """A child that changed its policy or blueprint must not inherit the parent's identity."""
    base = _generation()

    assert base.id == _generation().id
    assert base.id.startswith("gen_")
    assert base.id != _generation(policy_digest="sha256:" + "e" * 64).id
    assert base.id != _generation(blueprint_digest="sha256:" + "e" * 64).id
    assert base.id != Generation(**{**vars(base), "parent_id": base.id}).id


def test_promotion_requires_every_declared_dimension_and_a_human() -> None:
    required = {Dimension.CORRECTNESS, Dimension.EVIDENCE, Dimension.SECURITY}

    ok, failures = promotable(_passing(), required=required, human_approved=True)

    assert (ok, failures) == (True, ())


def test_a_child_cannot_promote_itself_even_with_a_clean_scorecard() -> None:
    """`Child factories ... cannot self-promote into the parent production authority`."""
    required = {Dimension.CORRECTNESS}

    ok, failures = promotable(_passing(), required=required, human_approved=False)

    assert ok is False
    assert failures == ("human_gate",)


def test_a_missing_or_failed_dimension_is_named_rather_than_averaged() -> None:
    """An operator has to see which dimension refused, not a score that hides it."""
    evaluations = [
        Evaluation(Dimension.CORRECTNESS, "pass", "evidence/correctness"),
        Evaluation(Dimension.SECURITY, "fail", "evidence/security"),
    ]

    ok, failures = promotable(
        evaluations,
        required={Dimension.CORRECTNESS, Dimension.SECURITY, Dimension.EVIDENCE},
        human_approved=True,
    )

    assert ok is False
    assert failures == ("missing:evidence", "fail:security")


def test_the_campaign_budget_bounds_depth_candidates_cost_and_wall_clock() -> None:
    """Recursion is the risk: an unbounded generation campaign spends the parent's budget."""
    budget = CampaignBudget()

    assert budget.admits(depth=1, candidates=3, cost_usd=100.0, wall_s=3600)
    assert not budget.admits(depth=2, candidates=0, cost_usd=1.0, wall_s=1)
    assert not budget.admits(depth=0, candidates=4, cost_usd=1.0, wall_s=1)
    assert not budget.admits(depth=0, candidates=0, cost_usd=100.01, wall_s=1)
    assert not budget.admits(depth=0, candidates=0, cost_usd=1.0, wall_s=3601)
    assert not budget.admits(depth=-1, candidates=0, cost_usd=1.0, wall_s=1)


def test_candidate_credentials_never_include_a_production_publishing_secret() -> None:
    credentials = candidate_credentials()

    assert credentials == ("synthetic_repo_read_write", "ephemeral_airflow", "ephemeral_backend")
    assert all("ephemeral" in name or name.startswith("synthetic") for name in credentials)
    assert not any("github" in name or "token" in name for name in credentials)
