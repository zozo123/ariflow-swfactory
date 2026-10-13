"""Advisory-only fan-in for speculative campaign verifier observations.

This module neither runs a verifier nor schedules agents. It compares an existing,
digest-bound campaign decision to separately recorded verifier observations without
changing campaign selection, publication, or Cell authority.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from swfactory.campaign_decision import CampaignDecisionManifest
from swfactory.canonical import atomic_json

_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_RESULT = Literal["passed", "failed", "timed_out", "refused"]
_AUTHORITY = "advisory-only"


class ShadowCampaignError(ValueError):
    """Shadow input is incomplete, inconsistent, or changed."""


def _digest(document: Mapping[str, Any]) -> str:
    payload = json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _require_digest(name: str, value: str) -> None:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ShadowCampaignError(f"{name} must be a canonical sha256 digest")


@dataclass(frozen=True)
class ShadowVerifierObservation:
    """A record supplied by an external checker, not proof of checker authenticity."""

    candidate_id: str
    output_head: str
    observation_id: str
    verifier_digest: str
    evidence_digest: str
    result: _RESULT
    cost_usd: float = 0.0
    duration_s: float = 0.0

    def validate(self) -> None:
        for name in ("candidate_id", "output_head", "observation_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ShadowCampaignError(f"observation {name} must be nonempty")
        _require_digest("verifier_digest", self.verifier_digest)
        _require_digest("evidence_digest", self.evidence_digest)
        if self.result not in ("passed", "failed", "timed_out", "refused"):
            raise ShadowCampaignError(f"{self.candidate_id}: invalid verifier result {self.result!r}")
        for name in ("cost_usd", "duration_s"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ShadowCampaignError(f"{self.candidate_id}: {name} must be finite and nonnegative")


@dataclass(frozen=True)
class ShadowObservations:
    campaign_decision_digest: str
    receipts: tuple[ShadowVerifierObservation, ...]
    schema_version: int = 1

    def validate(self) -> None:
        if self.schema_version != 1:
            raise ShadowCampaignError("unsupported shadow observations schema")
        _require_digest("campaign_decision_digest", self.campaign_decision_digest)
        ids: set[str] = set()
        tokens: set[str] = set()
        verifiers: set[str] = set()
        for receipt in self.receipts:
            receipt.validate()
            if receipt.candidate_id in ids:
                raise ShadowCampaignError(f"duplicate candidate observation: {receipt.candidate_id}")
            if receipt.observation_id in tokens:
                raise ShadowCampaignError(f"duplicate observation identity: {receipt.observation_id}")
            ids.add(receipt.candidate_id)
            tokens.add(receipt.observation_id)
            verifiers.add(receipt.verifier_digest)
        if len(verifiers) > 1:
            raise ShadowCampaignError("shadow comparison requires one pinned verifier digest")

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema_version": self.schema_version,
            "campaign_decision_digest": self.campaign_decision_digest,
            "receipts": [asdict(receipt) for receipt in sorted(self.receipts, key=lambda r: r.candidate_id)],
        }

    def digest(self) -> str:
        return _digest(self.canonical_dict())


@dataclass(frozen=True)
class ShadowCampaignReport:
    """Read-only comparison; suggested_candidate is never permission to promote."""

    campaign_id: str
    campaign_decision_digest: str
    observations_digest: str
    verifier_digest: str | None
    candidate_states: tuple[tuple[str, str], ...]
    missing_observations: tuple[str, ...]
    reported_passes: tuple[str, ...]
    suggested_candidate: str | None
    existing_selection: str | None
    agrees_with_existing: bool | None
    cancelled: bool
    observation_cost_usd: float
    observation_work_seconds: float
    authority: str = _AUTHORITY
    schema_version: int = 1

    def canonical_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "authority": self.authority,
            "campaign_id": self.campaign_id,
            "campaign_decision_digest": self.campaign_decision_digest,
            "observations_digest": self.observations_digest,
            "verifier_digest": self.verifier_digest,
            "candidate_states": [
                {"candidate_id": candidate_id, "result": result} for candidate_id, result in self.candidate_states
            ],
            "missing_observations": list(self.missing_observations),
            "reported_passes": list(self.reported_passes),
            "suggested_candidate": self.suggested_candidate,
            "existing_selection": self.existing_selection,
            "agrees_with_existing": self.agrees_with_existing,
            "cancelled": self.cancelled,
            "observation_cost_usd": self.observation_cost_usd,
            "observation_work_seconds": self.observation_work_seconds,
        }

    def digest(self) -> str:
        return _digest(self.canonical_dict())


def evaluate_shadow_campaign(
    campaign: CampaignDecisionManifest, observations: ShadowObservations
) -> ShadowCampaignReport:
    """Compare exact candidate identities; abstain if coverage or authority is incomplete.

    The existing campaign rank order is reused, never inferred from completion time.
    This is an observational experiment, not a trained acceptance predictor.
    """
    campaign.validate()
    observations.validate()
    if observations.campaign_decision_digest != campaign.digest():
        raise ShadowCampaignError("shadow observations belong to a different campaign decision")

    candidates = {candidate.candidate_id: candidate for candidate in campaign.candidates}
    by_id = {receipt.candidate_id: receipt for receipt in observations.receipts}
    for candidate_id, receipt in by_id.items():
        candidate = candidates.get(candidate_id)
        if candidate is None:
            raise ShadowCampaignError(f"unknown candidate in shadow observations: {candidate_id}")
        if not candidate.answered:
            raise ShadowCampaignError(f"{candidate_id}: provisional candidate cannot claim verifier observation")
        if receipt.output_head != candidate.output_head:
            raise ShadowCampaignError(f"{candidate_id}: verifier output head differs from frozen candidate")

    answered = {candidate_id for candidate_id, candidate in candidates.items() if candidate.answered}
    missing = tuple(sorted(answered - set(by_id)))
    reported_passes = tuple(sorted(cid for cid, receipt in by_id.items() if receipt.result == "passed"))

    # All answered candidates must be observed before comparing winners: never
    # let the first finished verifier silently determine the recommendation.
    suggested: str | None = None
    agreement: bool | None = None
    if not missing and not campaign.cancelled:
        suggested = next((cid for cid in campaign.selection.ranking if cid in reported_passes), None)
        agreement = suggested == campaign.selection.winner

    states = tuple(
        sorted(
            (
                candidate_id,
                by_id[candidate_id].result
                if candidate_id in by_id
                else ("missing" if candidate.answered else candidate.state),
            )
            for candidate_id, candidate in candidates.items()
        )
    )
    ordered_receipts = sorted(observations.receipts, key=lambda r: r.candidate_id)
    return ShadowCampaignReport(
        campaign_id=campaign.campaign_id,
        campaign_decision_digest=campaign.digest(),
        observations_digest=observations.digest(),
        verifier_digest=ordered_receipts[0].verifier_digest if ordered_receipts else None,
        candidate_states=states,
        missing_observations=missing,
        reported_passes=reported_passes,
        suggested_candidate=suggested,
        existing_selection=campaign.selection.winner,
        agrees_with_existing=agreement,
        cancelled=campaign.cancelled,
        observation_cost_usd=math.fsum(r.cost_usd for r in ordered_receipts),
        # Additive verifier work, NOT end-to-end latency or critical-path time.
        observation_work_seconds=math.fsum(r.duration_s for r in ordered_receipts),
    )


def write_shadow_observations(path: Path, observations: ShadowObservations) -> None:
    document = observations.canonical_dict()
    document["observations_digest"] = observations.digest()
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_json(path, document)


def load_shadow_observations(path: Path) -> ShadowObservations:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
        observations = ShadowObservations(
            campaign_decision_digest=document["campaign_decision_digest"],
            receipts=tuple(ShadowVerifierObservation(**row) for row in document["receipts"]),
            schema_version=document["schema_version"],
        )
        observations.validate()
    except (OSError, KeyError, TypeError, ValueError) as error:
        raise ShadowCampaignError(f"cannot load shadow observations: {error}") from error

    if document.get("observations_digest") != observations.digest():
        raise ShadowCampaignError("shadow observations digest mismatch")
    return observations


def write_shadow_report(path: Path, report: ShadowCampaignReport) -> None:
    document = report.canonical_dict()
    document["report_digest"] = report.digest()
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_json(path, document)
