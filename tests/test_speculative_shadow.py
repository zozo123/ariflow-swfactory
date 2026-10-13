"""Shadow campaign evidence is deterministic, complete, and never an authority gate."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest
from typer.testing import CliRunner

from swfactory.campaign_decision import (
    CampaignDecisionManifest,
    DecisionCandidate,
    DecisionSelection,
)
from swfactory.cli import app
from swfactory.speculative_shadow import (
    ShadowCampaignError,
    ShadowObservations,
    ShadowVerifierObservation,
    evaluate_shadow_campaign,
    load_shadow_observations,
    write_shadow_observations,
    write_shadow_report,
)

HASH = "sha256:" + "a" * 64
VERIFIER = "sha256:" + "b" * 64
EVIDENCE = "sha256:" + "c" * 64


def campaign(*, cancelled: bool = False, failed_sibling: bool = False, epoch: int = 7) -> CampaignDecisionManifest:
    repair = DecisionCandidate(
        candidate_id="repair",
        strategy="repair",
        state="ok",
        input_head="base",
        output_head="output-repair",
        candidate_ref="refs/swfactory/candidates/repair",
        evidence_bundle_digest=HASH,
        evaluations=(),
        cost_usd=1.0,
        duration_s=10.0,
    )
    rethink = DecisionCandidate(
        candidate_id="rethink",
        strategy="rethink",
        state="failed" if failed_sibling else "ok",
        input_head="base",
        output_head=None if failed_sibling else "output-rethink",
        candidate_ref=None if failed_sibling else "refs/swfactory/candidates/rethink",
        evidence_bundle_digest=None if failed_sibling else HASH,
        evaluations=(),
        cost_usd=2.0,
        duration_s=20.0,
    )
    return CampaignDecisionManifest(
        campaign_id="campaign-1",
        cell_id="cell-1",
        epoch=epoch,
        input_head="base",
        candidates=(repair, rethink),
        selection=DecisionSelection(
            winner="repair",
            reason="selected by original policy",
            ranking=("repair", "rethink"),
            refusals=(),
        ),
        independence=(),
        cancelled=cancelled,
    )


def observation(
    candidate: str,
    result: str = "passed",
    *,
    verifier_digest: str = VERIFIER,
    observation_id: str | None = None,
) -> ShadowVerifierObservation:
    return ShadowVerifierObservation(
        candidate_id=candidate,
        output_head=f"output-{candidate}",
        observation_id=observation_id or f"check-{candidate}",
        verifier_digest=verifier_digest,
        evidence_digest=EVIDENCE,
        result=result,
        cost_usd=0.25 if candidate == "repair" else 0.75,
        duration_s=3.0 if candidate == "repair" else 5.0,
    )


def observations(
    decision: CampaignDecisionManifest,
    *receipts: ShadowVerifierObservation,
) -> ShadowObservations:
    return ShadowObservations(campaign_decision_digest=decision.digest(), receipts=tuple(receipts))


def test_shadow_comparison_preserves_ranking_but_cannot_change_winner() -> None:
    decision = campaign()
    observed = observations(decision, observation("repair", "failed"), observation("rethink", "passed"))

    report = evaluate_shadow_campaign(decision, observed)

    assert report.suggested_candidate == "rethink"
    assert report.existing_selection == "repair"
    assert report.agrees_with_existing is False
    assert decision.selection.winner == "repair"
    assert report.authority == "advisory-only"
    assert report.reported_passes == ("rethink",)
    assert report.observation_cost_usd == 1.0
    assert report.observation_work_seconds == 8.0


def test_ranking_not_verifier_arrival_order_and_digest_is_stable() -> None:
    decision = campaign()
    left = observations(decision, observation("rethink"), observation("repair"))
    right = observations(decision, observation("repair"), observation("rethink"))

    a = evaluate_shadow_campaign(decision, left)
    b = evaluate_shadow_campaign(decision, right)

    assert left.digest() == right.digest()
    assert a.digest() == b.digest()
    assert a.suggested_candidate == "repair"
    assert a.agrees_with_existing is True


def test_missing_verification_causes_explicit_abstention() -> None:
    decision = campaign()
    report = evaluate_shadow_campaign(decision, observations(decision, observation("rethink")))

    assert report.suggested_candidate is None
    assert report.agrees_with_existing is None
    assert report.missing_observations == ("repair",)
    assert report.candidate_states == (("repair", "missing"), ("rethink", "passed"))


def test_cancelled_campaign_cannot_claim_shadow_winner() -> None:
    decision = campaign(cancelled=True)
    report = evaluate_shadow_campaign(
        decision, observations(decision, observation("repair"), observation("rethink"))
    )
    assert report.cancelled is True
    assert report.suggested_candidate is None
    assert report.agrees_with_existing is None


def test_failed_candidate_is_retained_but_must_not_claim_verifier_result() -> None:
    decision = campaign(failed_sibling=True)
    report = evaluate_shadow_campaign(decision, observations(decision, observation("repair")))

    assert report.candidate_states == (("repair", "passed"), ("rethink", "failed"))
    assert report.missing_observations == ()
    assert report.suggested_candidate == "repair"

    with pytest.raises(ShadowCampaignError, match="provisional candidate"):
        evaluate_shadow_campaign(
            decision, observations(decision, observation("repair"), observation("rethink"))
        )


def test_no_reported_passes_does_not_select_a_winner() -> None:
    decision = campaign()
    report = evaluate_shadow_campaign(
        decision, observations(decision, observation("repair", "failed"), observation("rethink", "refused"))
    )
    assert report.suggested_candidate is None
    assert report.agrees_with_existing is False


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        ({"output_head": "wrong"}, "output head differs"),
        ({"candidate_id": "stranger"}, "unknown candidate"),
    ],
)
def test_candidate_binding_refuses_cross_candidate_claims(replacement: dict[str, str], message: str) -> None:
    decision = campaign()
    altered = replace(observation("repair"), **replacement)
    with pytest.raises(ShadowCampaignError, match=message):
        evaluate_shadow_campaign(decision, observations(decision, altered))


def test_epoch_or_decision_drift_invalidates_all_observations() -> None:
    earlier = campaign(epoch=7)
    later = campaign(epoch=8)
    with pytest.raises(ShadowCampaignError, match="different campaign decision"):
        evaluate_shadow_campaign(later, observations(earlier, observation("repair")))


@pytest.mark.parametrize(
    ("receipts", "message"),
    [
        ((observation("repair"), observation("repair", observation_id="new")), "duplicate candidate"),
        ((observation("repair"), observation("rethink", observation_id="check-repair")), "duplicate observation"),
        ((observation("repair"), observation("rethink", verifier_digest=HASH)), "one pinned verifier"),
    ],
)
def test_duplicate_or_unpinned_observations_are_refused(
    receipts: tuple[ShadowVerifierObservation, ...], message: str
) -> None:
    decision = campaign()
    with pytest.raises(ShadowCampaignError, match=message):
        evaluate_shadow_campaign(decision, observations(decision, *receipts))


@pytest.mark.parametrize("cost", [float("nan"), float("inf"), -1.0, True])
def test_invalid_costs_are_refused(cost: float) -> None:
    decision = campaign()
    with pytest.raises(ShadowCampaignError, match="cost_usd"):
        evaluate_shadow_campaign(decision, observations(decision, replace(observation("repair"), cost_usd=cost)))


def test_tampered_observations_are_detected(tmp_path: Path) -> None:
    decision = campaign()
    path = tmp_path / "observations.json"
    original = observations(decision, observation("repair", "failed"), observation("rethink"))
    write_shadow_observations(path, original)

    assert load_shadow_observations(path).digest() == original.digest()

    document = json.loads(path.read_text(encoding="utf-8"))
    document["receipts"][0]["result"] = "passed"
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ShadowCampaignError, match="digest mismatch"):
        load_shadow_observations(path)


def test_writes_digest_bound_advisory_report(tmp_path: Path) -> None:
    decision = campaign()
    report = evaluate_shadow_campaign(
        decision, observations(decision, observation("repair"), observation("rethink"))
    )
    destination = tmp_path / "report.json"

    write_shadow_report(destination, report)

    document = json.loads(destination.read_text(encoding="utf-8"))
    assert document["report_digest"] == report.digest()
    assert document["authority"] == "advisory-only"
    assert document["campaign_decision_digest"] == decision.digest()


def test_cli_refuses_missing_bound_candidate_evidence(tmp_path: Path) -> None:
    decision = campaign()
    source = tmp_path / "decision.json"
    source.write_text(json.dumps(decision.canonical_dict()), encoding="utf-8")
    observations_path = tmp_path / "observations.json"
    write_shadow_observations(observations_path, observations(decision, observation("repair")))
    destination = tmp_path / "advisory.json"

    result = CliRunner().invoke(
        app, ["campaign-shadow", "compare", str(source), str(observations_path), str(destination)]
    )

    assert result.exit_code == 2
    assert "campaign shadow:" in result.output
    assert not destination.exists()
