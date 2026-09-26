from __future__ import annotations

import hashlib

import pytest

from swfactory.formal_claims import (
    FORMAL_CLAIMS_AUTHORITY,
    Claim,
    EvidenceMethod,
    EvidenceReceipt,
    EvidenceVerdict,
    FormalClaimError,
    FormalizationAssessment,
    Formalizability,
    FormalQuench,
    UncertaintyAxis,
    derive_certificate,
)


def _digest(label: str) -> str:
    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


def _quench(*, statement: str = "stale epochs cannot mutate") -> FormalQuench:
    return FormalQuench(
        artifact_digest=_digest("artifact"),
        assumptions_digest=_digest("assumptions"),
        model_digest=_digest("authority-model"),
        policy_digest=_digest("policy:formal-test"),
        claims=(
            Claim(
                claim_id="authority.stale-epoch",
                statement=statement,
                method=EvidenceMethod.MODEL_CHECK,
                min_independent_receipts=2,
                formalization=FormalizationAssessment(
                    status=Formalizability.MACHINE_CHECKABLE,
                    uncertainty_axes=(UncertaintyAxis.IMPLEMENTATION_REFINEMENT,),
                    rationale=(
                        "The invariant is precise in the bounded authority model; "
                        "implementation refinement remains open."
                    ),
                    next_step="Project trusted runtime traces into the model action vocabulary.",
                ),
            ),
            Claim(
                claim_id="behavior.no-known-crash",
                statement="no crash was observed in the declared fuzz campaign",
                method=EvidenceMethod.FUZZ,
                formalization=FormalizationAssessment(
                    status=Formalizability.EMPIRICAL_ONLY,
                    uncertainty_axes=(UncertaintyAxis.OPEN_ENVIRONMENT, UncertaintyAxis.VERIFIER_SCOPE),
                    rationale="This claim reports one fuzz campaign, not a universal theorem.",
                    next_step="Retain the campaign inputs, runtime, and coverage boundary.",
                ),
            ),
        ),
    )


def _receipt(
    quench: FormalQuench,
    claim_id: str,
    *,
    verifier: str,
    verdict: EvidenceVerdict = EvidenceVerdict.SUPPORTS,
) -> EvidenceReceipt:
    claim = quench.claim_map()[claim_id]
    return EvidenceReceipt(
        quench_digest=quench.digest(),
        claim_id=claim_id,
        claim_digest=claim.digest(),
        method=claim.method,
        verdict=verdict,
        verifier=verifier,
        evidence_digest=_digest(f"{claim_id}:{verifier}:{verdict.value}"),
    )


def test_certificate_requires_the_frozen_claims_and_declared_independence() -> None:
    quench = _quench()
    certificate = derive_certificate(
        quench,
        [
            _receipt(quench, "authority.stale-epoch", verifier="tlc-a"),
            _receipt(quench, "authority.stale-epoch", verifier="tlc-b"),
            _receipt(quench, "behavior.no-known-crash", verifier="fuzzer"),
        ],
        trusted_verifiers=frozenset({"tlc-a", "tlc-b", "fuzzer"}),
    )
    assert certificate.supported_claims == (
        "authority.stale-epoch",
        "behavior.no-known-crash",
    )
    assert certificate.refuted_claims == ()
    assert certificate.unresolved_required_claims == ()
    assert certificate.meets_claim_policy is True
    assert certificate.authority == FORMAL_CLAIMS_AUTHORITY
    assert certificate.unresolved_formalization_uncertainties == {
        "authority.stale-epoch": ["implementation-refinement"],
        "behavior.no-known-crash": ["open-environment", "verifier-scope"],
    }



def test_certificate_exposes_uncertainty_and_blocks_unassessed_required_claims() -> None:
    quench = FormalQuench(
        artifact_digest=_digest("artifact"),
        assumptions_digest=_digest("assumptions"),
        model_digest=_digest("model"),
        policy_digest=_digest("policy"),
        claims=(
            Claim(
                claim_id="behavior.unassessed",
                statement="the operation always completes",
                method=EvidenceMethod.TEST,
            ),
        ),
    )
    certificate = derive_certificate(
        quench,
        [_receipt(quench, "behavior.unassessed", verifier="test-runner")],
        trusted_verifiers=frozenset({"test-runner"}),
    )

    assert "behavior.unassessed" in certificate.unassessed_formalization_claims
    assert certificate.meets_claim_policy is False
    assert (
        certificate.as_dict()["formalization_register"]["behavior.unassessed"]["status"]
        == "unassessed"
    )


def test_formalization_uncertainty_is_bound_into_the_frozen_claim() -> None:
    first = _quench()
    original = first.claim_map()["authority.stale-epoch"]
    changed = Claim(
        claim_id=original.claim_id,
        statement=original.statement,
        method=original.method,
        min_independent_receipts=original.min_independent_receipts,
        formalization=FormalizationAssessment(
            status=Formalizability.MACHINE_CHECKABLE,
            uncertainty_axes=(UncertaintyAxis.MODEL_FIDELITY,),
            rationale="The model may omit persisted operation-journal behavior.",
            next_step="Add journal states to the model and compare.",
        ),
    )
    second = FormalQuench(
        artifact_digest=first.artifact_digest,
        assumptions_digest=first.assumptions_digest,
        model_digest=first.model_digest,
        policy_digest=first.policy_digest,
        claims=(changed, first.claim_map()["behavior.no-known-crash"]),
    )

    assert first.digest() != second.digest()


def test_search_can_propose_evidence_but_cannot_mint_truth() -> None:
    quench = _quench()
    search_receipt = _receipt(quench, "behavior.no-known-crash", verifier="agent-17")
    certificate = derive_certificate(
        quench,
        [search_receipt],
        trusted_verifiers=frozenset({"independent-fuzzer"}),
    )
    assert "behavior.no-known-crash" not in certificate.supported_claims
    assert "behavior.no-known-crash" in certificate.unresolved_required_claims
    assert search_receipt.evidence_digest in certificate.ignored_evidence
    assert certificate.meets_claim_policy is False


def test_trusted_refutation_dominates_support() -> None:
    quench = _quench()
    certificate = derive_certificate(
        quench,
        [
            _receipt(quench, "behavior.no-known-crash", verifier="fuzzer-a"),
            _receipt(
                quench,
                "behavior.no-known-crash",
                verifier="counterexample-checker",
                verdict=EvidenceVerdict.REFUTES,
            ),
        ],
        trusted_verifiers=frozenset({"fuzzer-a", "counterexample-checker"}),
    )
    assert "behavior.no-known-crash" in certificate.refuted_claims
    assert "behavior.no-known-crash" not in certificate.supported_claims
    assert certificate.meets_claim_policy is False


def test_evidence_cannot_cross_a_formal_quench() -> None:
    quench = _quench()
    changed = _quench(statement="stale epochs cannot publish")
    receipt = _receipt(quench, "authority.stale-epoch", verifier="tlc")
    with pytest.raises(FormalClaimError, match="different Formal Quench"):
        derive_certificate(changed, [receipt], trusted_verifiers=frozenset({"tlc"}))


def test_changing_the_claim_changes_both_claim_and_quench_identity() -> None:
    first = _quench()
    second = _quench(statement="stale epochs cannot publish")
    assert first.claim_map()["authority.stale-epoch"].digest() != second.claim_map()["authority.stale-epoch"].digest()
    assert first.digest() != second.digest()


def test_evidence_method_is_property_specific_not_globally_ranked() -> None:
    quench = _quench()
    claim = quench.claim_map()["behavior.no-known-crash"]
    wrong_method = EvidenceReceipt(
        quench_digest=quench.digest(),
        claim_id=claim.claim_id,
        claim_digest=claim.digest(),
        method=EvidenceMethod.THEOREM,
        verdict=EvidenceVerdict.SUPPORTS,
        verifier="proof-assistant",
        evidence_digest=_digest("wrong-method"),
    )
    with pytest.raises(FormalClaimError, match="evidence method"):
        derive_certificate(quench, [wrong_method], trusted_verifiers=frozenset({"proof-assistant"}))


def test_duplicate_verifier_identity_does_not_fake_independence() -> None:
    quench = _quench()
    certificate = derive_certificate(
        quench,
        [
            _receipt(quench, "authority.stale-epoch", verifier="tlc"),
            _receipt(quench, "authority.stale-epoch", verifier="tlc"),
        ],
        trusted_verifiers=frozenset({"tlc"}),
    )
    assert "authority.stale-epoch" in certificate.unresolved_required_claims
    assert certificate.meets_claim_policy is False


def test_policy_digest_is_content_bound_and_changes_quench_identity() -> None:
    first = _quench()
    second = FormalQuench(
        artifact_digest=first.artifact_digest,
        assumptions_digest=first.assumptions_digest,
        model_digest=first.model_digest,
        policy_digest=_digest("policy:formal-test-v2"),
        claims=first.claims,
    )

    assert first.digest() != second.digest()

    malformed = FormalQuench(
        artifact_digest=first.artifact_digest,
        assumptions_digest=first.assumptions_digest,
        model_digest=first.model_digest,
        policy_digest="policy:formal-test",
        claims=first.claims,
    )
    with pytest.raises(FormalClaimError, match="policy_digest must be sha256"):
        malformed.validate()


def test_trusted_counterexample_may_refute_with_its_actual_method() -> None:
    quench = _quench()
    claim = quench.claim_map()["authority.stale-epoch"]
    counterexample = EvidenceReceipt(
        quench_digest=quench.digest(),
        claim_id=claim.claim_id,
        claim_digest=claim.digest(),
        method=EvidenceMethod.FUZZ,
        verdict=EvidenceVerdict.REFUTES,
        verifier="counterexample-checker",
        evidence_digest=_digest("trusted-fuzz-counterexample"),
    )

    certificate = derive_certificate(
        quench,
        [counterexample],
        trusted_verifiers=frozenset({"counterexample-checker"}),
    )

    assert "authority.stale-epoch" in certificate.refuted_claims
    assert "authority.stale-epoch" not in certificate.supported_claims
    assert certificate.meets_claim_policy is False
