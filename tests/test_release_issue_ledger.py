from swfactory.liquid_bundle_engine import Concern
from swfactory.release_issue_ledger import (
    SUPERSEDED_ISSUES,
    binding_for_issue,
    execute_issue,
    legacy_rank_bindings,
    liquid_bindings,
    validate_ledger,
)


def test_release_issue_ledger_has_exact_cardinality() -> None:
    summary = validate_ledger()
    assert summary == {
        "liquid_issues": 900,
        "liquid500_issues": 500,
        "liquid400_issues": 400,
        "legacy_ranks": 181,
        "superseded_issues": 1,
        "first_liquid_issue": 255,
        "last_liquid_issue": 1154,
    }


def test_every_generated_issue_resolves_to_executable_code() -> None:
    bindings = liquid_bindings()
    assert len(bindings) == 900
    for binding in bindings:
        intent = execute_issue(binding.issue_number, cell_id="cell_release_ledger", epoch=1)
        assert intent.domain == binding.domain
        assert intent.concern is binding.concern
        assert intent.anchor == binding.anchor


def test_boundary_issue_identity_is_stable() -> None:
    first = binding_for_issue(255)
    last = binding_for_issue(1154)
    assert first.source == "Liquid500"
    assert first.concern is Concern.INVARIANT
    assert last.source == "Liquid400"
    assert last.domain == "stabilization-health"
    assert last.concern is Concern.STABILIZE


def test_duplicate_seed_is_explicitly_superseded() -> None:
    assert len(SUPERSEDED_ISSUES) == 1
    duplicate = SUPERSEDED_ISSUES[0]
    assert duplicate.issue_number == 1155
    assert duplicate.disposition == "duplicate"
    assert duplicate.canonical_issue == 1154
    try:
        binding_for_issue(1155)
    except KeyError:
        pass
    else:
        raise AssertionError("duplicate issue #1155 must never become canonical coverage")


def test_legacy_backlog_is_bound_to_executable_tranches() -> None:
    bindings = legacy_rank_bindings()
    assert [item.rank for item in bindings] == list(range(1, 182))
    assert bindings[0].tranche_id == "legacy-01"
    assert bindings[-1].tranche_id == "legacy-04"
