from __future__ import annotations

from pathlib import Path

import pytest

from swfactory.harness_conformance import assert_scenario, execute_scenario, load_scenario


FIXTURES = Path(__file__).parent / "fixtures" / "harness-scale"
SCENARIOS = sorted(FIXTURES.glob("*.json"))


def test_harness_scale_catalog_is_not_empty() -> None:
    assert SCENARIOS, "add at least one harness-scale scenario fixture"


@pytest.mark.parametrize("path", SCENARIOS, ids=lambda path: path.stem)
def test_harness_scale_scenario(path: Path, tmp_path: Path) -> None:
    document = load_scenario(path)
    result = execute_scenario(document, tmp_path / path.stem)
    assert_scenario(document, result)
