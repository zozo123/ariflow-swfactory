"""Machine-checkable issue-to-code traceability for the Liquid release.

Closing an issue is not evidence that it has an implementation.  This ledger resolves every
canonical generated Liquid issue to the exact executable bundle/domain/concern that owns it and
resolves every pre-Liquid backlog rank to the executable legacy tranche that absorbed it.
Superseded/duplicate issue identities are recorded explicitly instead of silently widening the
canonical ranges.
"""

from __future__ import annotations

import importlib
import json
from dataclasses import asdict, dataclass

from swfactory.legacy_issue_runtime import LegacyTranche
from swfactory.liquid_bundle_engine import BundleSpec, Concern, ExecutionIntent
from swfactory.liquid_release import (
    EXPECTED_LEGACY_RANKS,
    EXPECTED_LIQUID400_ISSUES,
    EXPECTED_LIQUID500_ISSUES,
    EXPECTED_LIQUID_ISSUES,
    LEGACY_MODULES,
    LIQUID_MODULES,
)


@dataclass(frozen=True)
class LiquidIssueBinding:
    issue_number: int
    source: str
    bundle_id: str
    module: str
    domain: str
    owner: str
    concern: Concern
    anchor: str


@dataclass(frozen=True)
class LegacyRankBinding:
    rank: int
    tranche_id: str
    module: str


@dataclass(frozen=True)
class SupersededIssue:
    issue_number: int
    disposition: str
    canonical_issue: int | None
    reason: str


# #1155 was a duplicate second seed of Liquid400/D40-C10.  The canonical 400-issue series is
# #755-#1154 inclusive, so the duplicate is a tombstone rather than an excuse to change cardinality.
SUPERSEDED_ISSUES = (
    SupersededIssue(
        issue_number=1155,
        disposition="duplicate",
        canonical_issue=1154,
        reason="duplicate Liquid400 D40-C10 seed; canonical Liquid400 ends at #1154",
    ),
)


def _bundle(module_name: str) -> BundleSpec:
    module = importlib.import_module(module_name)
    bundle = getattr(module, "BUNDLE", None)
    if not isinstance(bundle, BundleSpec):
        raise ValueError(f"{module_name} does not expose BundleSpec BUNDLE")
    bundle.validate()
    return bundle


def _tranche(module_name: str) -> LegacyTranche:
    module = importlib.import_module(module_name)
    tranche = getattr(module, "TRANCHE", None)
    if not isinstance(tranche, LegacyTranche):
        raise ValueError(f"{module_name} does not expose LegacyTranche TRANCHE")
    tranche.validate()
    return tranche


def liquid_bindings() -> tuple[LiquidIssueBinding, ...]:
    concerns = tuple(Concern)
    bindings: list[LiquidIssueBinding] = []
    for module_name in LIQUID_MODULES:
        bundle = _bundle(module_name)
        for domain_index, domain in enumerate(bundle.domains):
            for concern_index, concern in enumerate(concerns):
                issue_number = bundle.issue_start + domain_index * len(concerns) + concern_index
                if issue_number > bundle.issue_end:
                    raise ValueError(f"{bundle.bundle_id} generated issue outside declared span")
                bindings.append(
                    LiquidIssueBinding(
                        issue_number=issue_number,
                        source=bundle.source,
                        bundle_id=bundle.bundle_id,
                        module=module_name,
                        domain=domain.slug,
                        owner=domain.owner,
                        concern=concern,
                        anchor=domain.anchor,
                    )
                )
    return tuple(sorted(bindings, key=lambda item: item.issue_number))


def legacy_rank_bindings() -> tuple[LegacyRankBinding, ...]:
    bindings: list[LegacyRankBinding] = []
    for module_name in LEGACY_MODULES:
        tranche = _tranche(module_name)
        for rank in range(tranche.start_rank, tranche.end_rank + 1):
            bindings.append(LegacyRankBinding(rank=rank, tranche_id=tranche.tranche_id, module=module_name))
    return tuple(sorted(bindings, key=lambda item: item.rank))


def binding_for_issue(issue_number: int) -> LiquidIssueBinding:
    for binding in liquid_bindings():
        if binding.issue_number == issue_number:
            return binding
    raise KeyError(f"#{issue_number} is not a canonical generated Liquid issue")


def execute_issue(issue_number: int, *, cell_id: str, epoch: int, **flags: bool) -> ExecutionIntent:
    """Resolve an issue identity all the way to its canonical executable bundle path."""

    binding = binding_for_issue(issue_number)
    module = importlib.import_module(binding.module)
    run = getattr(module, "run", None)
    if not callable(run):
        raise ValueError(f"{binding.module} does not expose callable run")
    result = run(binding.domain, binding.concern.value, cell_id=cell_id, epoch=epoch, **flags)
    if not isinstance(result, ExecutionIntent):
        raise ValueError(f"{binding.module}.run did not return ExecutionIntent")
    if result.domain != binding.domain or result.concern is not binding.concern or result.anchor != binding.anchor:
        raise ValueError(f"#{issue_number} resolved to divergent executable intent")
    return result


def validate_ledger() -> dict[str, int]:
    liquid = liquid_bindings()
    issue_numbers = [item.issue_number for item in liquid]
    if len(liquid) != EXPECTED_LIQUID_ISSUES or len(set(issue_numbers)) != EXPECTED_LIQUID_ISSUES:
        raise ValueError("generated Liquid issue ledger must contain 900 unique canonical issues")
    if issue_numbers != list(range(issue_numbers[0], issue_numbers[-1] + 1)):
        raise ValueError("canonical generated Liquid issue identities must be contiguous")

    liquid500 = sum(item.source == "Liquid500" for item in liquid)
    liquid400 = sum(item.source == "Liquid400" for item in liquid)
    if liquid500 != EXPECTED_LIQUID500_ISSUES or liquid400 != EXPECTED_LIQUID400_ISSUES:
        raise ValueError(f"unexpected source cardinality: Liquid500={liquid500}, Liquid400={liquid400}")

    legacy = legacy_rank_bindings()
    ranks = [item.rank for item in legacy]
    if ranks != list(range(1, EXPECTED_LEGACY_RANKS + 1)):
        raise ValueError(f"legacy ledger must cover snapshot ranks 1-{EXPECTED_LEGACY_RANKS}")

    canonical = set(issue_numbers)
    for tombstone in SUPERSEDED_ISSUES:
        if tombstone.issue_number in canonical:
            raise ValueError(f"superseded issue #{tombstone.issue_number} leaked into canonical coverage")
        if tombstone.canonical_issue is not None and tombstone.canonical_issue not in canonical:
            raise ValueError(f"superseded issue #{tombstone.issue_number} points outside canonical coverage")

    return {
        "liquid_issues": len(liquid),
        "liquid500_issues": liquid500,
        "liquid400_issues": liquid400,
        "legacy_ranks": len(legacy),
        "superseded_issues": len(SUPERSEDED_ISSUES),
        "first_liquid_issue": issue_numbers[0],
        "last_liquid_issue": issue_numbers[-1],
    }


def main() -> int:
    summary = validate_ledger()
    print(json.dumps({"summary": summary, "superseded": [asdict(item) for item in SUPERSEDED_ISSUES]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
