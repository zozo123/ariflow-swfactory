"""Claim-scoped correctness for the software factory.

Arbitrary program correctness is not a capability this module claims. Instead it represents an
indexed judgment:

    Gamma |-_{M} artifact : claim

where the artifact, assumptions (Gamma), model (M), policy, and claim set are frozen by a Formal
Quench. Evidence can justify a claim only when it is bound to that exact quench and exact claim.

The module is deliberately authority-free. A complete ClaimCertificate says that the evidence
policy for a frozen artifact is satisfied; it does not approve, publish, merge, or promote anything.
Those remain duties of the existing authority plane.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum

FORMAL_CLAIMS_SCHEMA_VERSION = 1
FORMAL_CLAIMS_AUTHORITY = "evidence-only"
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")


class FormalClaimError(ValueError):
    """A formal-claim object is malformed or crosses an evidence boundary."""


class EvidenceMethod(StrEnum):
    TEST = "test"
    FUZZ = "fuzz"
    STATIC_ANALYSIS = "static-analysis"
    REPLAY = "replay"
    BOUNDED_EXHAUSTIVE = "bounded-exhaustive"
    MODEL_CHECK = "model-check"
    THEOREM = "theorem"
    BENCHMARK = "benchmark"
    OBSERVATION = "observation"


class EvidenceVerdict(StrEnum):
    SUPPORTS = "supports"
    REFUTES = "refutes"
    INCONCLUSIVE = "inconclusive"


def _require_digest(value: str, field: str) -> None:
    if not _SHA256.fullmatch(value):
        raise FormalClaimError(f"{field} must be sha256:<64 lowercase hex characters>")


def _canonical_digest(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class Claim:
    """One bounded proposition about one frozen correctness context.

    method names the evidence semantics required for this claim. Methods are intentionally not
    globally ranked: a theorem over the wrong abstraction is not automatically stronger than a
    runtime test against the actual binary.
    """

    claim_id: str
    statement: str
    method: EvidenceMethod
    required: bool = True
    min_independent_receipts: int = 1

    def validate(self) -> None:
        if not self.claim_id.strip() or len(self.claim_id) > 160:
            raise FormalClaimError("claim_id must be nonempty and bounded")
        if not self.statement.strip():
            raise FormalClaimError(f"{self.claim_id}: statement must be nonempty")
        if self.min_independent_receipts < 1:
            raise FormalClaimError(f"{self.claim_id}: min_independent_receipts must be positive")

    def digest(self) -> str:
        self.validate()
        return _canonical_digest(
            {
                "claim_id": self.claim_id,
                "statement": self.statement,
                "method": self.method.value,
                "required": self.required,
                "min_independent_receipts": self.min_independent_receipts,
            }
        )


@dataclass(frozen=True)
class FormalQuench:
    """Freeze the question before evidence is interpreted.

    A quench binds exact artifact bytes, assumptions, model, promotion policy, and claims. Changing
    any one of them changes the digest and invalidates evidence issued for the previous question.
    """

    artifact_digest: str
    assumptions_digest: str
    model_digest: str
    policy_digest: str
    claims: tuple[Claim, ...]

    def validate(self) -> None:
        _require_digest(self.artifact_digest, "artifact_digest")
        _require_digest(self.assumptions_digest, "assumptions_digest")
        _require_digest(self.model_digest, "model_digest")
        if not self.policy_digest.strip():
            raise FormalClaimError("policy_digest must be nonempty")
        if not self.claims:
            raise FormalClaimError("a Formal Quench requires at least one claim")
        seen: set[str] = set()
        for claim in self.claims:
            claim.validate()
            if claim.claim_id in seen:
                raise FormalClaimError(f"duplicate claim_id {claim.claim_id!r}")
            seen.add(claim.claim_id)

    def digest(self) -> str:
        self.validate()
        return _canonical_digest(
            {
                "schema_version": FORMAL_CLAIMS_SCHEMA_VERSION,
                "artifact_digest": self.artifact_digest,
                "assumptions_digest": self.assumptions_digest,
                "model_digest": self.model_digest,
                "policy_digest": self.policy_digest,
                "claims": [
                    {"claim_id": claim.claim_id, "claim_digest": claim.digest()}
                    for claim in sorted(self.claims, key=lambda value: value.claim_id)
                ],
            }
        )

    def claim_map(self) -> dict[str, Claim]:
        self.validate()
        return {claim.claim_id: claim for claim in self.claims}


@dataclass(frozen=True)
class EvidenceReceipt:
    """A verifier result for exactly one claim in exactly one quench.

    Search systems may emit receipts, but a receipt cannot self-declare trust. The authority plane
    supplies the trusted verifier set to certificate derivation. A search-produced counterexample
    must be checked by one of those verifiers before it changes the justified claim set. This is the
    epistemic analogue of "compute cannot mint authority".
    """

    quench_digest: str
    claim_id: str
    claim_digest: str
    method: EvidenceMethod
    verdict: EvidenceVerdict
    verifier: str
    evidence_digest: str

    def validate(self) -> None:
        _require_digest(self.quench_digest, "quench_digest")
        _require_digest(self.claim_digest, "claim_digest")
        _require_digest(self.evidence_digest, "evidence_digest")
        if not self.claim_id.strip():
            raise FormalClaimError("evidence claim_id must be nonempty")
        if not self.verifier.strip():
            raise FormalClaimError("evidence verifier must be nonempty")


@dataclass(frozen=True)
class ClaimCertificate:
    """Derived claim status for one Formal Quench.

    This is evidence state, not lifecycle authority. meets_claim_policy is therefore a predicate an
    authority gate may consume; it is never itself permission to promote.
    """

    quench_digest: str
    supported_claims: tuple[str, ...]
    refuted_claims: tuple[str, ...]
    unresolved_required_claims: tuple[str, ...]
    ignored_evidence: tuple[str, ...]
    evidence_digests: tuple[str, ...]
    authority: str = FORMAL_CLAIMS_AUTHORITY

    @property
    def meets_claim_policy(self) -> bool:
        return not self.refuted_claims and not self.unresolved_required_claims

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": FORMAL_CLAIMS_SCHEMA_VERSION,
            "authority": self.authority,
            "quench_digest": self.quench_digest,
            "supported_claims": list(self.supported_claims),
            "refuted_claims": list(self.refuted_claims),
            "unresolved_required_claims": list(self.unresolved_required_claims),
            "ignored_evidence": list(self.ignored_evidence),
            "evidence_digests": list(self.evidence_digests),
            "meets_claim_policy": self.meets_claim_policy,
        }

    def digest(self) -> str:
        return _canonical_digest(self.as_dict())


def derive_certificate(
    quench: FormalQuench,
    receipts: Iterable[EvidenceReceipt],
    *,
    trusted_verifiers: frozenset[str],
) -> ClaimCertificate:
    """Derive the justified claim set without allowing evidence to drift across questions.

    Trust is an input from the authority boundary; a receipt cannot self-declare itself trusted.
    A refutation by a trusted verifier dominates supporting receipts for the same claim. Otherwise
    a claim is supported only after its configured number of distinct trusted verifier identities
    have produced supporting receipts using the claim declared method.

    No global ordering between evidence methods is assumed.
    """

    quench.validate()
    if any(not verifier.strip() for verifier in trusted_verifiers):
        raise FormalClaimError("trusted verifier identities must be nonempty")
    quench_digest = quench.digest()
    claims = quench.claim_map()
    grouped: dict[str, list[EvidenceReceipt]] = {claim_id: [] for claim_id in claims}
    ignored: list[str] = []
    all_evidence: list[str] = []

    for receipt in receipts:
        receipt.validate()
        all_evidence.append(receipt.evidence_digest)
        if receipt.quench_digest != quench_digest:
            raise FormalClaimError(f"{receipt.claim_id}: evidence belongs to a different Formal Quench")
        claim = claims.get(receipt.claim_id)
        if claim is None:
            raise FormalClaimError(f"evidence references unknown claim {receipt.claim_id!r}")
        if receipt.claim_digest != claim.digest():
            raise FormalClaimError(f"{receipt.claim_id}: evidence claim digest does not match frozen claim")
        if receipt.method != claim.method:
            raise FormalClaimError(
                f"{receipt.claim_id}: evidence method {receipt.method.value!r} does not match "
                f"frozen method {claim.method.value!r}"
            )
        if receipt.verifier not in trusted_verifiers:
            ignored.append(receipt.evidence_digest)
            continue
        grouped[receipt.claim_id].append(receipt)

    supported: list[str] = []
    refuted: list[str] = []
    unresolved_required: list[str] = []

    for claim in sorted(quench.claims, key=lambda value: value.claim_id):
        trusted = grouped[claim.claim_id]
        if any(receipt.verdict == EvidenceVerdict.REFUTES for receipt in trusted):
            refuted.append(claim.claim_id)
            continue
        supporters = {receipt.verifier for receipt in trusted if receipt.verdict == EvidenceVerdict.SUPPORTS}
        if len(supporters) >= claim.min_independent_receipts:
            supported.append(claim.claim_id)
        elif claim.required:
            unresolved_required.append(claim.claim_id)

    return ClaimCertificate(
        quench_digest=quench_digest,
        supported_claims=tuple(supported),
        refuted_claims=tuple(refuted),
        unresolved_required_claims=tuple(unresolved_required),
        ignored_evidence=tuple(sorted(set(ignored))),
        evidence_digests=tuple(sorted(set(all_evidence))),
    )
