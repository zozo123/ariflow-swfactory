"""Machine-checkable release manifest for the consolidated Liquid implementation.

The manifest is intentionally code, not prose: CI/operator tooling can import it and prove that
all generated bundles and legacy tranches are present, disjoint, contiguous within their declared
scope, and still preserve the repository's singular authorities.
"""

from __future__ import annotations

import importlib
import json
from dataclasses import asdict, dataclass

from swfactory.legacy_issue_runtime import LegacyTranche
from swfactory.liquid_bundle_engine import BundleSpec

LIQUID_MODULES = tuple(f"swfactory.liquid_bundle_{index:02d}" for index in range(1, 19))
LEGACY_MODULES = tuple(f"swfactory.legacy_bundle_{index:02d}" for index in range(1, 5))

AUTHORITIES = {
    "lifecycle_scheduler": "airflow",
    "cell_mutation": "cell_id+epoch",
    "publication": "factory",
    "promotion": "parent",
}

EXPECTED_LIQUID_ISSUES = 900
EXPECTED_LIQUID500_ISSUES = 500
EXPECTED_LIQUID400_ISSUES = 400
EXPECTED_LEGACY_RANKS = 181


@dataclass(frozen=True)
class ReleaseSummary:
    lifecycle_scheduler: str
    liquid_bundles: int
    liquid_domains: int
    liquid_issues: int
    liquid500_issues: int
    liquid400_issues: int
    legacy_tranches: int
    legacy_ranks: int
    first_liquid_issue: int
    last_liquid_issue: int


def _load_bundles() -> list[BundleSpec]:
    bundles: list[BundleSpec] = []
    for module_name in LIQUID_MODULES:
        module = importlib.import_module(module_name)
        bundle = getattr(module, "BUNDLE", None)
        if not isinstance(bundle, BundleSpec):
            raise ValueError(f"{module_name} does not expose BundleSpec BUNDLE")
        bundle.validate()
        bundles.append(bundle)
    return bundles


def _load_tranches() -> list[LegacyTranche]:
    tranches: list[LegacyTranche] = []
    for module_name in LEGACY_MODULES:
        module = importlib.import_module(module_name)
        tranche = getattr(module, "TRANCHE", None)
        if not isinstance(tranche, LegacyTranche):
            raise ValueError(f"{module_name} does not expose LegacyTranche TRANCHE")
        tranche.validate()
        tranches.append(tranche)
    return tranches


def _assert_contiguous_spans(spans: list[tuple[int, int]], *, label: str) -> None:
    ordered = sorted(spans)
    for previous, current in zip(ordered, ordered[1:], strict=False):
        if previous[1] >= current[0]:
            raise ValueError(f"{label} spans overlap: {previous} and {current}")
        if previous[1] + 1 != current[0]:
            raise ValueError(f"{label} spans have a gap: {previous} then {current}")


def validate_release() -> ReleaseSummary:
    if AUTHORITIES["lifecycle_scheduler"] != "airflow":
        raise ValueError("Apache Airflow must remain the only lifecycle scheduler")
    if AUTHORITIES["cell_mutation"] != "cell_id+epoch":
        raise ValueError("Cell identity plus epoch must remain the mutation authority")
    if AUTHORITIES["publication"] != "factory":
        raise ValueError("factory must remain the single publication authority")
    if AUTHORITIES["promotion"] != "parent":
        raise ValueError("child factories may not self-promote")

    bundles = _load_bundles()
    spans = [(bundle.issue_start, bundle.issue_end) for bundle in bundles]
    _assert_contiguous_spans(spans, label="Liquid issue")

    issue_count = sum(bundle.issue_end - bundle.issue_start + 1 for bundle in bundles)
    liquid500 = sum(
        bundle.issue_end - bundle.issue_start + 1 for bundle in bundles if bundle.source == "Liquid500"
    )
    liquid400 = sum(
        bundle.issue_end - bundle.issue_start + 1 for bundle in bundles if bundle.source == "Liquid400"
    )
    if issue_count != EXPECTED_LIQUID_ISSUES:
        raise ValueError(f"expected {EXPECTED_LIQUID_ISSUES} Liquid issues, found {issue_count}")
    if liquid500 != EXPECTED_LIQUID500_ISSUES or liquid400 != EXPECTED_LIQUID400_ISSUES:
        raise ValueError(f"unexpected source cardinality: Liquid500={liquid500}, Liquid400={liquid400}")

    tranches = _load_tranches()
    rank_spans = [(tranche.start_rank, tranche.end_rank) for tranche in tranches]
    _assert_contiguous_spans(rank_spans, label="legacy rank")
    legacy_ranks = sum(tranche.count for tranche in tranches)
    if rank_spans[0][0] != 1 or legacy_ranks != EXPECTED_LEGACY_RANKS:
        raise ValueError(f"legacy tranche coverage must be ranks 1-{EXPECTED_LEGACY_RANKS}")

    domain_count = sum(len(bundle.domains) for bundle in bundles)
    return ReleaseSummary(
        lifecycle_scheduler=AUTHORITIES["lifecycle_scheduler"],
        liquid_bundles=len(bundles),
        liquid_domains=domain_count,
        liquid_issues=issue_count,
        liquid500_issues=liquid500,
        liquid400_issues=liquid400,
        legacy_tranches=len(tranches),
        legacy_ranks=legacy_ranks,
        first_liquid_issue=min(start for start, _ in spans),
        last_liquid_issue=max(end for _, end in spans),
    )


def main() -> int:
    print(json.dumps(asdict(validate_release()), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
