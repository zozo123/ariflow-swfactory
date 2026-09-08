from __future__ import annotations

import pytest

from swfactory.stage_registry import names, resolve


def test_managed_build_stage_is_the_canonical_build_and_test_implementation() -> None:
    from swfactory.work_stage import build_and_test

    assert resolve("build_and_test") is build_and_test


def test_registry_preserves_other_canonical_stages() -> None:
    from swfactory.stages import STAGES

    assert names() == tuple(STAGES)
    for stage, implementation in STAGES.items():
        if stage != "build_and_test":
            assert resolve(stage) is implementation


def test_unknown_stage_fails_closed() -> None:
    with pytest.raises(KeyError, match="unknown factory stage"):
        resolve("not-a-stage")
