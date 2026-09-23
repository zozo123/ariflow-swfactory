from __future__ import annotations

import hashlib

import pytest

from swfactory.formal_claims import (
    FORMAL_CLAIMS_AUTHORITY,
    SEARCH_AUTHORITY,
    TRUSTED_VERIFIER_AUTHORITY,
    Claim,
    EvidenceMethod,
    EvidenceReceipt,
    EvidenceVerdict,
    FormalClaimError,
    FormalQuench,
    derive_certificate,
)


def _digest(label: str) -> str:
    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


def _quench(*, statement: str = "stale epochs cannot mutate") -> FormalQuench:
    return FormalQuench(
        artifact_digest=_digest("artifact"),
        assumptions_digest=_digest("assumptions"),
        model_digest=_digest("authority-model"),
        policy_digest="policy:formal-test",
        claims=(
            Claim(
                claim_id="authority.stale-epoch",
                statement=statement,
                method=EvidenceMethod.MODEL_CHECK,
                min_independent_receipts=2,
            ),
            Claim(
                claim_id="behavior.no-known-crash",
                statement="no crash was observed in the declared fuzz campaign",
                method=EvidenceMethod.FUZZ,
            ),
        ),
    )


def _receipt(
    quench: FormalQuench,
    claim_id: str,
    *,
    verifier: str,
    verdict: EvidenceVerdict = EvidenceVerdict.SUPPORTS,
    authority: str = TRUSTED_VERIFIER_AUTHORITY,
) -> EvidenceReceipt:
    claim = quench.claim_map()[claim_id]
    return EvidenceReceipt(
        quench_digest=quench.digest(),
        claim_id=claim_id,
        claim_digest=claim.digest(),
        method=claim.method,
        verdict=verdict,
        verifier=verifier,
        evidence_digest=_digest(f"{claim_id}:{verifier}:{verdict.value}:{authority}"),
        authority=authority,
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
    )
    assert certificate.supported_claims == (
        "authority.stale-epoch",
        "behavior.no-known-crash",
    )
    assert certificate.refuted_claims == ()
    assert certificate.unresolved_required_claims == ()
    assert certificate.meets_claim_policy is True
    assert certificate.authority == FORMAL_CLAIMS_AUTHORITY


def test_search_can_propose_evidence_but_cannot_mint_truth() -> None:
    quench = _quench()
    search_receipt = _receipt(
        quench,
        "behavior.no-known-crash",
        verifier="agent-17",
        authority=SEARCH_AUTHORITY,
    )
    certificate = derive_certificate(quench, [search_receipt])
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
    )
    assert "behavior.no-known-crash" in certificate.refuted_claims
    assert "behavior.no-known-crash" not in certificate.supported_claims
    assert certificate.meets_claim_policy is False


def test_evidence_cannot_cross_a_formal_quench() -> None:
    quench = _quench()
    changed = _quench(statement="stale epochs cannot publish")
    receipt = _receipt(quench, "authority.stale-epoch", verifier="tlc")
    with pytest.raises(FormalClaimError, match="different Formal Quench"):
        derive_certificate(changed, [receipt])


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
        authority=TRUSTED_VERIFIER_AUTHORITY,
    )
    with pytest.raises(FormalClaimError, match="evidence method"):
        derive_certificate(quench, [wrong_method])


def test_duplicate_verifier_identity_does_not_fake_independence() -> None:
    quench = _quench()
    certificate = derive_certificate(
        quench,
        [
            _receipt(quench, "authority.stale-epoch", verifier="tlc"),
            _receipt(quench, "authority.stale-epoch", verifier="tlc"),
        ],
    )
    assert "authority.stale-epoch" in certificate.unresolved_required_claims
    assert certificate.meets_claim_policy is False
