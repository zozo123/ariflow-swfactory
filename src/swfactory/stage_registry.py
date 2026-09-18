from __future__ import annotations

from collections.abc import Callable
from typing import Any

StageCallable = Callable[[Any], Any]


def _review(ctx: Any) -> Any:
    """Dispatch the managed review stage without creating a second lifecycle surface.

    The ordinary line keeps the canonical single-review implementation.  A line that explicitly
    selects ``REVIEW_LIQUID.md`` opts into the Liquid annealer: the policy path and its content are
    already part of accepted inputs, so a Cell epoch cannot quietly switch review semantics between
    Airflow tasks.  Both implementations still execute inside the one Airflow-owned ``review`` task.
    """

    blueprint = getattr(ctx, "blueprint", None)
    if blueprint is not None and blueprint.review.policy == "REVIEW_LIQUID.md":
        from swfactory.liquid_annealing import review

        return review(ctx)
    from swfactory.stages import review

    return review(ctx)


_review.__name__ = "review"


def resolve(stage: str) -> StageCallable:
    """Resolve one canonical implementation for a managed factory stage.

    ``build_and_test`` is the managed Plan.work implementation. ``review`` has one explicit policy
    switch for the Liquid line, but remains one Airflow stage; the annealer may fan review evidence
    out internally, never lifecycle work. Everything else comes from the canonical stages registry.
    """
    if stage == "build_and_test":
        from swfactory.work_stage import build_and_test

        return build_and_test
    if stage == "review":
        return _review
    from swfactory.stages import STAGES

    try:
        return STAGES[stage]
    except KeyError as exc:
        raise KeyError(f"unknown factory stage {stage!r}") from exc


def names() -> tuple[str, ...]:
    from swfactory.stages import STAGES

    return tuple(STAGES)
