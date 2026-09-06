"""Optimistic coordination for concurrent factory jobs targeting one repository."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import StrEnum


class StalePolicy(StrEnum):
    BLOCK = "block"
    WARN = "warn"
    REBASE = "rebase"
    SERIALIZE = "serialize"


@dataclass(frozen=True)
class RepoObservation:
    repo: str
    target: str
    base_sha: str
    observed_target_sha: str
    touched_files: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class CoordinationDecision:
    action: str
    stale: bool
    overlaps: tuple[str, ...] = ()
    require_reverify: bool = False
    reason: str = ""


def coordinate(
    job: RepoObservation,
    *,
    current_target_sha: str,
    concurrent: Iterable[RepoObservation],
    policy: StalePolicy = StalePolicy.BLOCK,
) -> CoordinationDecision:
    peers = [
        peer
        for peer in concurrent
        if peer.repo == job.repo and peer.target == job.target and peer.base_sha == job.base_sha
    ]
    overlaps = sorted(
        {path for peer in peers for path in job.touched_files.intersection(peer.touched_files)}
    )
    stale = current_target_sha != job.observed_target_sha

    if overlaps and policy == StalePolicy.SERIALIZE:
        return CoordinationDecision(
            "serialize", stale, tuple(overlaps), False, "overlapping concurrent mutation set"
        )
    if stale:
        if policy == StalePolicy.BLOCK:
            return CoordinationDecision(
                "block", True, tuple(overlaps), False, "target moved since verification"
            )
        if policy == StalePolicy.REBASE:
            return CoordinationDecision(
                "rebase",
                True,
                tuple(overlaps),
                True,
                "rebase requires complete policy verification",
            )
        if policy == StalePolicy.WARN:
            return CoordinationDecision(
                "publish_stale",
                True,
                tuple(overlaps),
                False,
                "publish only with explicit stale evidence",
            )
    if overlaps:
        return CoordinationDecision(
            "review_overlap",
            False,
            tuple(overlaps),
            True,
            "overlap must not silently last-writer-win",
        )
    return CoordinationDecision("proceed", False, (), False, "disjoint and current")


def merge_order(work_ids: Iterable[str]) -> tuple[str, ...]:
    """Stable order for conflict handling/evidence independent of completion timing."""
    return tuple(sorted(set(work_ids)))
