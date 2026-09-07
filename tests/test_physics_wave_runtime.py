from __future__ import annotations

import pytest

from swfactory.liquid_bundle_engine import BundleAction, Concern
from swfactory.physics_wave_runtime import (
    ISSUE_COUNTS,
    PhaseState,
    PhysicsWave,
    WaveBundle,
    execute_wave,
    wave_issue_slots,
)


def test_wave_cardinality_matches_generated_backlogs() -> None:
    assert len(wave_issue_slots(PhysicsWave.OCEAN120)) == ISSUE_COUNTS[PhysicsWave.OCEAN120] == 120
    assert len(wave_issue_slots(PhysicsWave.PHASE240)) == ISSUE_COUNTS[PhysicsWave.PHASE240] == 240
    assert len(wave_issue_slots(PhysicsWave.STATMECH360)) == ISSUE_COUNTS[PhysicsWave.STATMECH360] == 360


def test_bundle_is_exactly_four_domains_and_40_issues() -> None:
    bundle = WaveBundle("ocean-01", PhysicsWave.OCEAN120, 1, 4)
    assert bundle.issue_count == 40
    assert bundle.contains(1)
    assert bundle.contains(4)
    assert not bundle.contains(5)


def test_airflow_is_the_only_scheduler() -> None:
    with pytest.raises(ValueError, match="only lifecycle scheduler"):
        execute_wave(
            PhysicsWave.OCEAN120,
            domain_ordinal=1,
            domain_slug="pressure-routing",
            owner="authority",
            concern=Concern.RUNTIME,
            cell_id="cell_demo",
            epoch=1,
            scheduler="temporal",
        )


def test_non_crystallized_phase_cannot_stabilize_to_promotion() -> None:
    intent = execute_wave(
        PhysicsWave.PHASE240,
        domain_ordinal=1,
        domain_slug="phase-observation",
        owner="evidence",
        concern=Concern.STABILIZE,
        cell_id="cell_demo",
        epoch=2,
        phase_state=PhaseState.SUPERCRITICAL,
    )
    assert intent.action is BundleAction.REFUSE
    assert not intent.promotable


def test_statmech_operator_algebra_cannot_mutate_external_authority() -> None:
    intent = execute_wave(
        PhysicsWave.STATMECH360,
        domain_ordinal=1,
        domain_slug="creation-annihilation",
        owner="security",
        concern=Concern.RUNTIME,
        cell_id="cell_demo",
        epoch=3,
        operator_external_mutation=True,
    )
    assert intent.action is BundleAction.REFUSE
    assert not intent.promotable
