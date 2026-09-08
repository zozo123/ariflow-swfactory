"""The Liquid matrix must stay falsifiable.

These tests are the reason the matrix is allowed to be data instead of Python: they prove the
shipped spec parses, that every runtime anchor points at code that actually exists, that no row
can claim support the capability inventory does not back, and that the physics vocabulary stays
out of the product's cognitive path.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest

from swfactory.capability_inventory import load_inventory
from swfactory.liquid_spec import (
    CANONICAL_OWNERS,
    AnchorError,
    LiquidSpecError,
    load_spec,
    resolve_anchor,
    validate_spec,
)

SPEC_PATH = Path("config/liquid-spec.yaml")
INVENTORY_PATH = Path("config/capability-inventory.json")

# The ten boring, measurable production signals that replaced the physics scalars.
EXPECTED_METRICS = {
    "queue_depth",
    "failure_rate",
    "retry_rate",
    "in_doubt_operations",
    "cleanup_debt",
    "cost",
    "time_to_evidence",
    "time_to_merge",
    "sandbox_saturation",
    "cancellation_lag",
}

# Vocabulary that belongs in docs/research, never in a domain row a developer reads.
BANNED_VOCABULARY = (
    "jarzynski",
    "crooks",
    "nucleation",
    "onsager",
    "statistical mechanics",
    "statmech",
    "ocean120",
    "phase240",
    "free energy",
    "entropy production",
)


def _minimal_document() -> dict[str, Any]:
    """A tiny but fully valid spec, so each rejection test changes exactly one thing."""
    return {
        "schema_version": 1,
        "doctrine": {"phases": ["exploration", "stabilization", "convergence"]},
        "owners": list(CANONICAL_OWNERS),
        "concerns": [
            {
                "id": f"C{index:02d}",
                "name": f"concern{index}",
                "action": f"action{index}",
                "metric": f"metric{index}",
                "question": "Does it hold?",
            }
            for index in range(1, 11)
        ],
        "families": [
            {
                "id": "Liquid500",
                "kind": "liquid",
                "state": "declared",
                "domains": 1,
                "concerns_per_domain": 10,
            }
        ],
        "domains": [
            {
                "id": "liquid500.cell-authority",
                "slug": "cell-authority",
                "family": "Liquid500",
                "owner": "authority",
                "state": "declared",
                "support": "unsupported",
                "invariant": "Every managed run is bound to one durable Cell identity.",
                "runtime_anchor": "swfactory.cells:CellIdentity",
                "evidence_requirement": "A journal record showing a stale-epoch write refused.",
            }
        ],
    }


def _rejects(document: dict[str, Any], match: str) -> None:
    with pytest.raises(LiquidSpecError, match=match):
        validate_spec(document)


# ---------------------------------------------------------------------------------------------
# the shipped spec
# ---------------------------------------------------------------------------------------------


def test_shipped_spec_validates_and_is_the_whole_matrix() -> None:
    spec = load_spec(SPEC_PATH)

    assert spec.owners == CANONICAL_OWNERS
    assert len(spec.concerns) == 10
    assert set(spec.metrics) == EXPECTED_METRICS
    assert len(spec.domains) == 90
    assert spec.matrix_cells == 900
    assert spec.summary()["matrix_cells"] == 900

    liquid = {family.id: family for family in spec.families if family.kind == "liquid"}
    assert liquid["Liquid500"].domains == 50
    assert liquid["Liquid400"].domains == 40
    assert sum(family.domains for family in liquid.values()) == len(spec.domains)


def test_every_runtime_anchor_resolves_to_real_code() -> None:
    """The point of the whole exercise: the matrix is checkable, so it cannot be decorative."""
    spec = load_spec(SPEC_PATH)

    for row in spec.domains:
        path = resolve_anchor(row.runtime_anchor)
        assert path.is_file(), f"{row.id}: {row.runtime_anchor}"


def test_no_row_claims_more_support_than_the_capability_inventory_backs() -> None:
    spec = load_spec(SPEC_PATH)
    claims = {str(row["id"]): row for row in load_inventory(INVENTORY_PATH)["claims"]}

    for row in spec.domains:
        if row.state == "validated":
            assert row.capability_claim in claims, row.id
        if row.support == "supported":
            assert claims[row.capability_claim]["support"] == "supported", row.id
        if row.state == "experimental":
            assert row.follow_up, row.id

    # The default must stay aspirational: most of the matrix is a backlog, not a promise.
    assert spec.counts("support")["unsupported"] > len(spec.domains) // 2


def test_every_owner_is_one_of_the_seven_canonical_roles() -> None:
    spec = load_spec(SPEC_PATH)

    assert set(spec.by_owner()) == set(CANONICAL_OWNERS)
    assert all(row.owner in CANONICAL_OWNERS for row in spec.domains)
    assert all(spec.by_owner()[owner] for owner in CANONICAL_OWNERS)


def test_physics_vocabulary_stays_in_research_and_out_of_the_domain_rows() -> None:
    spec = load_spec(SPEC_PATH)

    for row in spec.domains:
        prose = f"{row.id} {row.invariant} {row.evidence_requirement}".lower()
        for banned in BANNED_VOCABULARY:
            assert banned not in prose, f"{row.id} still carries {banned!r}"

    research = [family for family in spec.families if family.kind == "research"]
    assert {family.id for family in research} == {"Ocean120", "Phase240", "StatMech360"}
    assert all(family.state == "research" for family in research)


def test_duplicate_slugs_are_reported_rather_than_silently_merged() -> None:
    spec = load_spec(SPEC_PATH)

    duplicates = spec.duplicate_slugs()
    assert duplicates, "the generators produced cross-family slug collisions; keep them visible"
    for slug in duplicates:
        rows = [row for row in spec.domains if row.slug == slug]
        assert len({row.family for row in rows}) == len(rows)
        assert all(row.note for row in rows), slug
        assert all(row.support != "supported" for row in rows), slug


# ---------------------------------------------------------------------------------------------
# anchor resolution
# ---------------------------------------------------------------------------------------------


def test_resolve_anchor_accepts_modules_packages_and_attributes() -> None:
    assert resolve_anchor("swfactory.cells").name == "cells.py"
    assert resolve_anchor("swfactory.idempotency:OperationJournal").name == "idempotency.py"
    assert resolve_anchor("swfactory.backend.service:Factory").name == "service.py"


@pytest.mark.parametrize(
    ("anchor", "match"),
    [
        ("swfactory.does_not_exist", "no module"),
        ("swfactory.cells:NoSuchClass", "no top-level"),
        ("os.path", "under 'swfactory'"),
        ("swfactory.cells:a:b", "at most one"),
        ("   ", "nonempty"),
    ],
)
def test_resolve_anchor_rejects_anything_that_is_not_real(anchor: str, match: str) -> None:
    with pytest.raises(AnchorError, match=match):
        resolve_anchor(anchor)


# ---------------------------------------------------------------------------------------------
# rejections -- one changed field per test, so the message is unambiguous
# ---------------------------------------------------------------------------------------------


def test_minimal_document_is_valid_so_the_rejection_tests_isolate_one_change() -> None:
    spec = validate_spec(_minimal_document())

    assert spec.matrix_cells == 10
    assert spec.domain("liquid500.cell-authority").owner == "authority"


def test_unknown_schema_version_is_rejected() -> None:
    document = _minimal_document()
    document["schema_version"] = 2
    _rejects(document, "unsupported liquid spec schema")


def test_owner_outside_the_canonical_seven_is_rejected() -> None:
    document = _minimal_document()
    document["domains"][0]["owner"] = "platform"
    _rejects(document, "owner 'platform' is not one of the canonical roles")


def test_duplicate_domain_id_is_rejected() -> None:
    document = _minimal_document()
    document["domains"].append(copy.deepcopy(document["domains"][0]))
    document["families"][0]["domains"] = 2
    _rejects(document, "duplicate domain id 'liquid500.cell-authority'")


def test_dangling_runtime_anchor_is_rejected() -> None:
    document = _minimal_document()
    document["domains"][0]["runtime_anchor"] = "swfactory.liquid_bundle_99"
    _rejects(document, "no module 'swfactory.liquid_bundle_99'")


def test_dangling_attribute_on_a_real_module_is_rejected() -> None:
    document = _minimal_document()
    document["domains"][0]["runtime_anchor"] = "swfactory.cells:PhaseTransition"
    _rejects(document, "defines no top-level 'PhaseTransition'")


def test_missing_required_field_is_rejected() -> None:
    document = _minimal_document()
    del document["domains"][0]["invariant"]
    _rejects(document, r"domain missing fields: \['invariant'\]")


def test_empty_invariant_is_rejected() -> None:
    document = _minimal_document()
    document["domains"][0]["invariant"] = "   "
    _rejects(document, "invariant must be nonempty")


def test_id_must_be_family_dot_slug() -> None:
    document = _minimal_document()
    document["domains"][0]["id"] = "cell-authority"
    _rejects(document, "must be '<family>.<slug>'")


def test_unknown_family_is_rejected() -> None:
    document = _minimal_document()
    document["domains"][0]["family"] = "Liquid900"
    _rejects(document, "unknown family 'Liquid900'")


def test_support_cannot_outrun_state() -> None:
    document = _minimal_document()
    document["domains"][0]["support"] = "supported"
    _rejects(document, "supported rows must be validated")


def test_validated_row_must_cite_a_capability_claim() -> None:
    document = _minimal_document()
    document["domains"][0]["state"] = "validated"
    _rejects(document, "must cite a capability_claim")


def test_experimental_row_must_carry_a_follow_up() -> None:
    document = _minimal_document()
    document["domains"][0]["state"] = "experimental"
    document["domains"][0]["support"] = "experimental"
    _rejects(document, "require an explicit follow_up")


def test_unknown_capability_claim_is_rejected_against_the_real_inventory() -> None:
    document = _minimal_document()
    document["domains"][0]["capability_claim"] = "physics.nucleation"
    with pytest.raises(LiquidSpecError, match="unknown capability_claim 'physics.nucleation'"):
        validate_spec(document, claims={"airflow.lifecycle": {"state": "validated", "support": "supported"}})


def test_row_cannot_be_supported_when_its_claim_is_not() -> None:
    document = _minimal_document()
    document["domains"][0]["state"] = "validated"
    document["domains"][0]["support"] = "supported"
    document["domains"][0]["capability_claim"] = "sandbox.srt"
    with pytest.raises(LiquidSpecError, match="so this row cannot be supported"):
        validate_spec(document, claims={"sandbox.srt": {"state": "experimental", "support": "experimental"}})


def test_concern_axis_must_be_declared_once_with_unique_ids_and_metrics() -> None:
    document = _minimal_document()
    document["concerns"][1]["id"] = "C01"
    _rejects(document, "duplicate concern id 'C01'")

    document = _minimal_document()
    document["concerns"][1]["metric"] = "metric1"
    _rejects(document, "metric 'metric1' is already used")


def test_family_domain_count_must_match_the_rows_that_exist() -> None:
    document = _minimal_document()
    document["families"][0]["domains"] = 50
    _rejects(document, "declares 50 domains but 1 rows exist")


def test_doctrine_keeps_the_three_intuitive_phases_only() -> None:
    document = _minimal_document()
    document["doctrine"]["phases"] = ["gas", "liquid", "crystallized"]
    _rejects(document, "doctrine.phases must be")
