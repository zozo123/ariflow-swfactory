"""Seeded fault schedules and retained benchmark evidence plans."""

from __future__ import annotations

import json
import random
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class Fault:
    at_step: int
    kind: str
    target: str
    duration_s: float = 0.0


@dataclass(frozen=True)
class FaultPlan:
    seed: int
    faults: tuple[Fault, ...]

    def to_json(self) -> dict:
        return {"schema_version": 1, "seed": self.seed, "faults": [asdict(f) for f in self.faults]}


def generate(seed: int, *, steps: int, count: int, targets: Iterable[str]) -> FaultPlan:
    rng = random.Random(seed)
    targets = tuple(targets)
    kinds = (
        "kill_backend",
        "freeze_worker",
        "sandbox_terminate",
        "api_429",
        "api_500",
        "stale_epoch",
    )
    faults = []
    for _ in range(max(0, count)):
        faults.append(
            Fault(
                at_step=rng.randrange(max(1, steps)),
                kind=rng.choice(kinds),
                target=rng.choice(targets),
                duration_s=round(rng.random() * 5, 3),
            )
        )
    return FaultPlan(seed, tuple(sorted(faults, key=lambda f: (f.at_step, f.kind, f.target))))


def write_evidence(root: Path, *, plan: FaultPlan, verdicts: dict, metrics: dict, timeline: list[dict]) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    bundle = {
        "schema_version": 1,
        "plan": plan.to_json(),
        "verdicts": verdicts,
        "metrics": metrics,
        "timeline": timeline,
    }
    path = root / f"fault-run-{plan.seed}.json"
    path.write_text(json.dumps(bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def invariant_verdict(
    *,
    duplicate_mutations: int,
    stale_epoch_mutations: int,
    lost_evidence: int,
    cleanup_pending: int,
) -> dict[str, bool]:
    return {
        "no_duplicate_mutation": duplicate_mutations == 0,
        "stale_epoch_rejected": stale_epoch_mutations == 0,
        "no_lost_evidence": lost_evidence == 0,
        "cleanup_converged": cleanup_pending == 0,
    }
