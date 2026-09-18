"""Orthogonal exploration-space construction.

The search phase may vary several independent axes at once. Variants are immutable
descriptions only; they cannot rank, merge, publish, or promote anything.
"""

from __future__ import annotations

import hashlib
import itertools
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from swfactory.exploration_entropy import ExplorationOrder, permute_exploration


@dataclass(frozen=True)
class ExplorationVariant:
    axes: tuple[tuple[str, str], ...]

    @property
    def logical_id(self) -> str:
        payload = json.dumps(self.axes, separators=(",", ":"), ensure_ascii=True).encode()
        return "variant_" + hashlib.sha256(payload).hexdigest()[:20]

    def to_dict(self) -> dict[str, object]:
        return {
            "logical_id": self.logical_id,
            "authority": "exploration-only",
            "axes": {name: value for name, value in self.axes},
        }


def build_exploration_space(
    axes: Mapping[str, Sequence[str]],
    *,
    max_variants: int,
) -> tuple[ExplorationOrder, tuple[ExplorationVariant, ...]]:
    """Build the cartesian hypothesis space, then sample an entropy-ordered bounded slice."""
    if max_variants < 1:
        raise ValueError("max_variants must be positive")
    if not axes:
        raise ValueError("exploration requires at least one axis")

    names = tuple(sorted(axes))
    values: list[tuple[str, ...]] = []
    for name in names:
        options = tuple(axes[name])
        if not options or any(not item.strip() for item in options):
            raise ValueError(f"axis {name!r} must contain nonempty options")
        if len(set(options)) != len(options):
            raise ValueError(f"axis {name!r} contains duplicate options")
        values.append(options)

    variants = tuple(
        ExplorationVariant(tuple(zip(names, combination, strict=True))) for combination in itertools.product(*values)
    )
    ids = tuple(variant.logical_id for variant in variants)
    order = permute_exploration(ids)
    by_id = {variant.logical_id: variant for variant in variants}
    selected = tuple(by_id[item] for item in order.values[:max_variants])
    return order, selected
