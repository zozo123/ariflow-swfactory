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


def test_blueprint_replay_and_managed_runtime_resolve_the_same_build_callable() -> None:
    from swfactory.blueprint import load
    from swfactory.work_stage import build_and_test

    blueprint = load("factory")
    build = next(item for item in blueprint.pipeline() if getattr(item, "__name__", "") == "build_and_test")

    assert build is resolve("build_and_test")
    assert build is build_and_test


def test_one_registry_resolution_controls_every_blueprint_stage(monkeypatch: pytest.MonkeyPatch) -> None:
    from swfactory import stage_registry
    from swfactory.blueprint import load

    seen: list[str] = []
    original = stage_registry.resolve

    def recording(stage: str):
        seen.append(stage)
        return original(stage)

    monkeypatch.setattr(stage_registry, "resolve", recording)
    blueprint = load("hotfix")
    pipeline = blueprint.pipeline()

    assert seen == blueprint.order
    assert len([item for item in pipeline if callable(item)]) == len(blueprint.order)
