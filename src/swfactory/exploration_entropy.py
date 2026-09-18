"""High-entropy exploration contract.

Exploration may deliberately vary ordering and lane selection. The entropy token is
retained as evidence, but never participates in final candidate ranking or promotion.
"""

from __future__ import annotations

import secrets
from collections.abc import Sequence
from dataclasses import dataclass
from random import Random
from typing import Protocol, TypeVar

T = TypeVar("T")


class _Sampler(Protocol):
    def sample(self, population: Sequence[T], k: int) -> list[T]: ...


@dataclass(frozen=True)
class ExplorationOrder:
    entropy_token: str
    values: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "authority": "exploration-only",
            "entropy_token": self.entropy_token,
            "values": list(self.values),
        }


def permute_exploration(
    values: Sequence[str],
    *,
    sampler: _Sampler | None = None,
    entropy_token: str | None = None,
) -> ExplorationOrder:
    """Return one non-authoritative permutation and the token that identifies it."""
    if not values:
        raise ValueError("exploration requires at least one value")
    if len(set(values)) != len(values):
        raise ValueError("exploration values must be distinct")

    active = sampler or secrets.SystemRandom()
    ordered = tuple(active.sample(tuple(values), k=len(values)))
    token = entropy_token or secrets.token_hex(16)
    if not token.strip():
        raise ValueError("entropy token must be nonempty")
    return ExplorationOrder(token, ordered)


def deterministic_test_sampler(seed: int) -> Random:
    """Seeded sampler for tests only; production exploration should use SystemRandom."""
    return Random(seed)
