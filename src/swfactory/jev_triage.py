"""Deterministic related-work shortlist and advisory shadow view.

The adviser is deliberately downstream of retrieval and upstream of an operator decision. Nothing
here files, closes, suppresses, enrolls, executes, approves, or promotes work.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable
from dataclasses import asdict, dataclass

_TOKEN = re.compile(r"[a-z0-9_]+")


@dataclass(frozen=True)
class WorkItem:
    key: str
    title: str
    acceptance_criteria: tuple[str, ...] = ()

    @property
    def has_acceptance_criteria(self) -> bool:
        return any(item.strip() for item in self.acceptance_criteria)


@dataclass(frozen=True)
class RelatedItem:
    item: WorkItem
    lexical_score: float


@dataclass(frozen=True)
class Advisory:
    existing_issue: int
    relationship: str
    confidence: float
    probability: float
    needs_review: bool = False
    available: bool = True
    rationale: str = ""


_ALLOWED_RELATIONSHIPS = frozenset(
    {
        "potential_duplicate",
        "extension",
        "dependency",
        "conflict",
        "related",
        "unrelated",
        "insufficient_evidence",
    }
)


def _tokens(item: WorkItem) -> set[str]:
    text = " ".join((item.title, *item.acceptance_criteria)).lower()
    return set(_TOKEN.findall(text))


def related_issues(proposal: WorkItem, existing: Iterable[WorkItem], *, limit: int = 5) -> tuple[RelatedItem, ...]:
    """Return a deterministic lexical shortlist; Jev never controls retrieval membership."""

    proposal_tokens = _tokens(proposal)
    ranked: list[RelatedItem] = []
    for item in existing:
        candidate_tokens = _tokens(item)
        union = proposal_tokens | candidate_tokens
        score = len(proposal_tokens & candidate_tokens) / len(union) if union else 0.0
        ranked.append(RelatedItem(item=item, lexical_score=score))
    ranked.sort(key=lambda row: (-row.lexical_score, row.item.key))
    return tuple(ranked[: max(0, limit)])


def normalize_advisory(proposal: WorkItem, existing: WorkItem, advisory: Advisory | None) -> Advisory:
    """Validate an untrusted suggestion and force review when acceptance evidence is incomplete."""

    if advisory is None:
        return Advisory(
            existing_issue=_issue_number(existing.key),
            relationship="insufficient_evidence",
            confidence=0.0,
            probability=0.0,
            needs_review=True,
            available=False,
            rationale="adviser unavailable",
        )
    if advisory.relationship not in _ALLOWED_RELATIONSHIPS:
        raise ValueError("unknown advisory relationship")
    if any(
        not math.isfinite(value) or not 0.0 <= value <= 1.0
        for value in (advisory.confidence, advisory.probability)
    ):
        raise ValueError("advisory confidence/probability must be finite and within [0,1]")

    missing = not proposal.has_acceptance_criteria or not existing.has_acceptance_criteria
    return Advisory(
        existing_issue=advisory.existing_issue,
        relationship=advisory.relationship,
        confidence=advisory.confidence,
        probability=advisory.probability,
        needs_review=advisory.needs_review or missing or advisory.relationship == "insufficient_evidence",
        available=advisory.available,
        rationale=advisory.rationale,
    )


def shadow_view(
    proposal: WorkItem,
    related: Iterable[RelatedItem],
    advisories: dict[str, Advisory | None],
) -> dict:
    """Render the complete proposal plus related work. No output field is an execution decision."""

    rows = []
    for relation in related:
        item = relation.item
        advisory = normalize_advisory(proposal, item, advisories.get(item.key))
        rows.append(
            {
                "issue": item.key,
                "title": item.title,
                "acceptance_criteria": list(item.acceptance_criteria),
                "acceptance_criteria_missing": not item.has_acceptance_criteria,
                "lexical_score": relation.lexical_score,
                "advisory": asdict(advisory),
            }
        )

    return {
        "proposal": {
            "key": proposal.key,
            "title": proposal.title,
            "acceptance_criteria": list(proposal.acceptance_criteria),
            "acceptance_criteria_missing": not proposal.has_acceptance_criteria,
        },
        "related": rows,
        "operator_decision_required": True,
        "proposal_preserved": True,
        "authority": "none",
    }


def evaluation_summary(expected: Iterable[str], observed: Iterable[Advisory]) -> dict:
    """Small honest summary for shadow experiments; raw cases stay the evidence."""

    expected_rows = list(expected)
    observed_rows = list(observed)
    if len(expected_rows) != len(observed_rows):
        raise ValueError("expected and observed lengths differ")
    correct = sum(exp == obs.relationship for exp, obs in zip(expected_rows, observed_rows, strict=True))
    review = sum(obs.needs_review or not obs.available for obs in observed_rows)
    false_duplicate = sum(
        obs.relationship == "potential_duplicate" and exp != "potential_duplicate"
        for exp, obs in zip(expected_rows, observed_rows, strict=True)
    )
    return {
        "cases": len(expected_rows),
        "correct": correct,
        "accuracy": correct / len(expected_rows) if expected_rows else None,
        "needs_review_or_unavailable": review,
        "false_duplicate_suggestions": false_duplicate,
    }


def _issue_number(key: str) -> int:
    text = key.removeprefix("#")
    return int(text) if text.isdigit() else 0
