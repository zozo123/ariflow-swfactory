from __future__ import annotations

import pytest

from swfactory.stage_registry import names, resolve


def test_managed_build_stage_is_the_canonical_build_and_test_implementation() -> None:
    from swfactory.work_stage import build_and_test

    assert resolve("build_and_test") is build_and_test


def test_registry_preserves_unoverridden_canonical_stages() -> None:
    from swfactory.stages import STAGES

    assert names() == tuple(STAGES)
    for stage, implementation in STAGES.items():
        if stage not in {"build_and_test", "review"}:
            assert resolve(stage) is implementation


def test_unknown_stage_fails_closed() -> None:
    with pytest.raises(KeyError, match="unknown factory stage"):
        resolve("not-a-stage")


def test_blueprint_pipeline_uses_the_same_build_stage_as_managed_airflow() -> None:
    from swfactory.blueprint import load
    from swfactory.stages import Gate
    from swfactory.work_stage import build_and_test

    pipeline = load("blueprints/default.toml").pipeline()
    stages = [item for item in pipeline if not isinstance(item, Gate)]
    build = next(item for item in stages if getattr(item, "__name__", "") == "build_and_test")

    assert build is build_and_test
    assert build is resolve("build_and_test")


def test_legacy_stage_registry_and_default_pipeline_share_managed_build() -> None:
    from swfactory import stages
    from swfactory.stage_registry import resolve

    assert stages.STAGES["build_and_test"] is stages.build_and_test
    assert resolve("build_and_test") is stages.build_and_test
    assert stages.build_and_test in stages.PIPELINE
    assert not hasattr(stages, "_legacy_build_and_test")
