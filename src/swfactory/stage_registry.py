from __future__ import annotations

from collections.abc import Callable
from typing import Any

StageCallable = Callable[[Any], Any]


def resolve(stage: str) -> StageCallable:
    """Resolve one canonical stage implementation for managed and replay callers.

    `build_and_test` is the managed Plan.work implementation. Every other stage remains sourced
    from the canonical stages registry. Keeping this decision here removes the DAG-only exception
    and gives replay/CLI code one import to use as it migrates.
    """
    if stage == "build_and_test":
        from swfactory.work_stage import build_and_test

        return build_and_test
    from swfactory.stages import STAGES

    try:
        return STAGES[stage]
    except KeyError as exc:
        raise KeyError(f"unknown factory stage {stage!r}") from exc


def names() -> tuple[str, ...]:
    from swfactory.stages import STAGES

    return tuple(STAGES)
