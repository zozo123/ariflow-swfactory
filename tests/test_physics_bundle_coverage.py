from __future__ import annotations

from swfactory.physics_bundle_01 import BUNDLE as B01
from swfactory.physics_bundle_02 import BUNDLE as B02
from swfactory.physics_bundle_03 import BUNDLE as B03
from swfactory.physics_bundle_04 import BUNDLE as B04
from swfactory.physics_bundle_05 import BUNDLE as B05
from swfactory.physics_bundle_06 import BUNDLE as B06
from swfactory.physics_bundle_07 import BUNDLE as B07
from swfactory.physics_bundle_08 import BUNDLE as B08
from swfactory.physics_bundle_09 import BUNDLE as B09
from swfactory.physics_bundle_10 import BUNDLE as B10
from swfactory.physics_bundle_11 import BUNDLE as B11
from swfactory.physics_bundle_12 import BUNDLE as B12
from swfactory.physics_bundle_13 import BUNDLE as B13
from swfactory.physics_bundle_14 import BUNDLE as B14
from swfactory.physics_bundle_15 import BUNDLE as B15
from swfactory.physics_bundle_16 import BUNDLE as B16
from swfactory.physics_bundle_17 import BUNDLE as B17
from swfactory.physics_bundle_18 import BUNDLE as B18
from swfactory.physics_wave_runtime import DOMAIN_COUNTS, ISSUE_COUNTS, PhysicsWave

BUNDLES = (B01, B02, B03, B04, B05, B06, B07, B08, B09, B10, B11, B12, B13, B14, B15, B16, B17, B18)


def test_all_720_generated_physics_slots_are_covered_once() -> None:
    covered: set[tuple[PhysicsWave, int]] = set()
    total_slots = 0

    for bundle in BUNDLES:
        bundle.validate()
        assert bundle.issue_count == 40
        total_slots += bundle.issue_count
        for ordinal in range(bundle.domain_start, bundle.domain_end + 1):
            key = (bundle.wave, ordinal)
            assert key not in covered
            covered.add(key)

    assert total_slots == 720
    assert sum(ISSUE_COUNTS.values()) == 720
    for wave, domain_count in DOMAIN_COUNTS.items():
        assert {ordinal for candidate, ordinal in covered if candidate is wave} == set(range(1, domain_count + 1))
