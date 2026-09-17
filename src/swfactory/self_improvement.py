"""Close the loop: turn the factory's own evidence into work orders it can drain.

The liquid line already schedules, isolates, gates and delivers. What it never had is a reason to
run: something must write the issues it drains, and a person choosing them by hand is the step that
does not scale. This module reads what the factory has already measured about itself and proposes
the work, ranked.

The whole risk of a self-improving loop is that it proposes things nobody can check, then reports
success against its own prose. One invariant prevents that:

    a proposal is refused unless its done-condition is a command an existing gate already runs.

So every work order here ends in `uv run pytest tests/test_module_reachability.py`, or
`python -m swfactory.capability_inventory`, or `swfactory metrics` -- checks that were already
load-bearing before this module existed. A proposal that cannot be falsified is not emitted at all,
which is the difference between a loop that converges and one that congratulates itself.

It proposes. It does not promote: the liquid line's two human gates and the merge button are
unchanged, and nothing here can reach them.
"""

from __future__ import annotations

import json
import shlex
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

# The checks a done-condition may cite. Each already gates the repository; this module adds no new
# authority, it only points existing ones at the debt they can already see.
VERIFIABLE_CHECKS: frozenset[str] = frozenset(
    {
        "uv run pytest tests/test_module_reachability.py",
        "uv run python -m swfactory.capability_inventory",
        "uv run swfactory metrics --root .",
    }
)


class Source(StrEnum):
    """Where a signal was measured. Ordering is the tie-break, so it is deliberate."""

    REACHABILITY = "reachability"  # code that runs nothing
    CAPABILITY = "capability"  # a claim the inventory cannot call validated
    DELIVERY = "delivery"  # how the line actually performs


SOURCE_ORDER = {source: index for index, source in enumerate(Source)}


class ProposalError(ValueError):
    """A proposal was asked for in a shape the loop refuses to emit."""


@dataclass(frozen=True)
class DoneWhen:
    """The condition that retires a work order, and the command that decides it."""

    check: str
    predicate: str

    def validate(self) -> None:
        if self.check not in VERIFIABLE_CHECKS:
            raise ProposalError(
                f"done-condition cites {self.check!r}, which is not a check this repository runs; "
                f"known: {sorted(VERIFIABLE_CHECKS)}"
            )
        if not self.predicate.strip():
            raise ProposalError("done-condition has no predicate")


@dataclass(frozen=True)
class Signal:
    """One measured weakness. ``weight`` is the size of the debt in its own units."""

    source: Source
    key: str
    weight: float
    detail: str


@dataclass(frozen=True)
class WorkOrder:
    source: Source
    key: str
    title: str
    rationale: str
    done_when: DoneWhen
    weight: float
    labels: tuple[str, ...] = ("liquid",)

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "source": self.source.value}

    def as_issue(self) -> str:
        """The issue body the liquid line would drain, done-condition first."""
        return (
            f"{self.rationale}\n\n"
            f"**Done when:** {self.done_when.predicate}\n\n"
            f"Verified by:\n\n```sh\n{self.done_when.check}\n```\n"
        )


@dataclass
class Assessment:
    signals: tuple[Signal, ...] = ()
    orders: tuple[WorkOrder, ...] = ()
    refused: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "signals": [{**asdict(s), "source": s.source.value} for s in self.signals],
            "orders": [order.to_dict() for order in self.orders],
            "refused": list(self.refused),
        }


# ---------------------------------------------------------------- reading the factory's evidence


def ledger_signals(ledger: Mapping[str, str], sizes: Mapping[str, int]) -> list[Signal]:
    """Unreachable modules, weighted by the lines that run nothing."""
    return [
        Signal(Source.REACHABILITY, module, float(sizes.get(module, 0)), reason)
        for module, reason in sorted(ledger.items())
    ]


def capability_signals(document: Mapping[str, Any]) -> list[Signal]:
    """Claims the inventory cannot call validated.

    Weighted by how far the claim is from validated rather than by size: an ``experimental`` claim
    with a live runtime is closer to done than a ``declared`` one with none, and a loop that cannot
    tell them apart will keep proposing the hardest thing first.
    """
    distance = {"declared": 3.0, "integrated": 2.0, "experimental": 1.0, "unsupported": 0.0}
    signals = []
    for claim in document.get("claims", []):
        state = str(claim.get("state", ""))
        if state == "validated":
            continue
        signals.append(
            Signal(
                Source.CAPABILITY,
                str(claim.get("id", "")),
                distance.get(state, 1.0),
                f"state={state}, support={claim.get('support')}",
            )
        )
    return sorted(signals, key=lambda s: s.key)


DELIVERY_TARGETS: dict[str, tuple[float, str]] = {
    # metric -> (target, direction) where direction is "min" (at least) or "max" (at most)
    "first_pass_rate": (0.8, "min"),
    "tests_pass_rate": (1.0, "min"),
    "mean_iterations": (1.5, "max"),
}


def delivery_signals(summary: Mapping[str, Any]) -> list[Signal]:
    """Measured line performance that misses its target. A met target emits nothing."""
    signals = []
    if not summary.get("runs"):
        return signals
    for metric, (target, direction) in sorted(DELIVERY_TARGETS.items()):
        value = float(summary.get(metric, 0.0))
        missed = value < target if direction == "min" else value > target
        if missed:
            gap = abs(target - value)
            signals.append(Signal(Source.DELIVERY, metric, gap, f"{metric}={value:.3f}, target {direction} {target}"))
    if int(summary.get("blockers", 0)):
        signals.append(
            Signal(Source.DELIVERY, "blockers", float(summary["blockers"]), f"{summary['blockers']} blocking findings")
        )
    return signals


# ---------------------------------------------------------------- turning evidence into work


def _order_for(signal: Signal) -> WorkOrder:
    if signal.source is Source.REACHABILITY:
        return WorkOrder(
            source=signal.source,
            key=signal.key,
            title=f"Wire or retire `swfactory.{signal.key}`",
            rationale=(
                f"`src/swfactory/{signal.key.replace('.', '/')}.py` is {int(signal.weight)} lines that no "
                f"entrypoint reaches ({signal.detail}). Either give it a caller or delete it; carrying it "
                f"unreached is the cost without the benefit."
            ),
            done_when=DoneWhen(
                "uv run pytest tests/test_module_reachability.py",
                f"`{signal.key}` is gone from NOT_YET_WIRED and the ledger still passes",
            ),
            weight=signal.weight,
        )
    if signal.source is Source.CAPABILITY:
        return WorkOrder(
            source=signal.source,
            key=signal.key,
            title=f"Validate or withdraw the `{signal.key}` capability claim",
            rationale=(
                f"`config/capability-inventory.json` carries `{signal.key}` as {signal.detail}. A claim that "
                f"never reaches `validated` reads to a user exactly like one that did."
            ),
            done_when=DoneWhen(
                "uv run python -m swfactory.capability_inventory",
                f"`{signal.key}` is `validated`, or withdrawn from the inventory",
            ),
            weight=signal.weight,
        )
    return WorkOrder(
        source=signal.source,
        key=signal.key,
        title=f"Move `{signal.key}` back to target",
        rationale=f"The line's own metrics say {signal.detail}.",
        done_when=DoneWhen(
            "uv run swfactory metrics --root .",
            f"`{signal.key}` meets its target across committed runs",
        ),
        weight=signal.weight,
    )


def rank_key(order: WorkOrder) -> tuple[Any, ...]:
    """Deterministic order: nothing here reads a clock or an arrival position."""
    return (SOURCE_ORDER[order.source], -round(order.weight, 6), order.key)


def propose(signals: Iterable[Signal], *, budget: int = 5) -> Assessment:
    """Rank the evidence and emit the work the factory can verify it finished.

    A work order whose done-condition does not name a check this repository runs is refused and
    recorded, never emitted. That refusal is the loop's safety property: it cannot ask for something
    whose completion it would have to take on trust.
    """
    if budget < 1:
        raise ProposalError("a proposal budget must admit at least one work order")
    ordered = tuple(signals)
    orders: list[WorkOrder] = []
    refused: list[str] = []
    for signal in ordered:
        order = _order_for(signal)
        try:
            order.done_when.validate()
        except ProposalError as error:
            refused.append(f"{signal.source.value}:{signal.key}: {error}")
            continue
        orders.append(order)
    orders.sort(key=rank_key)
    return Assessment(signals=ordered, orders=tuple(interleave(orders, budget)), refused=tuple(refused))


def interleave(orders: Sequence[WorkOrder], budget: int) -> list[WorkOrder]:
    """Take the heaviest debt from each source in turn, rather than the heaviest overall.

    Sorting by weight alone lets one source own the whole budget: the reachability ledger is 24
    entries deep, so a straight ranking proposes twenty-four module chores and never once mentions
    that a capability claim is stuck or that the first-pass rate slipped. A loop that only ever
    descends its steepest gradient stops improving the moment that gradient flattens.

    Within a source the order is still weight-first, so the round-robin costs nothing but the
    monopoly.
    """
    by_source: dict[Source, list[WorkOrder]] = {}
    for order in orders:
        by_source.setdefault(order.source, []).append(order)
    queues = [by_source[source] for source in Source if source in by_source]
    taken: list[WorkOrder] = []
    while queues and len(taken) < budget:
        for queue in list(queues):
            if len(taken) == budget:
                break
            taken.append(queue.pop(0))
            if not queue:
                queues.remove(queue)
    return taken


# ---------------------------------------------------------------- the repository's own evidence


def module_sizes(root: Path) -> dict[str, int]:
    src = root / "src" / "swfactory"
    sizes = {}
    for path in src.rglob("*.py"):
        if "prompts" in path.parts:
            continue
        module = path.relative_to(src).with_suffix("").as_posix().replace("/", ".").removesuffix(".__init__")
        sizes[module] = len(path.read_text(encoding="utf-8").splitlines())
    return sizes


def assess(root: Path, *, ledger: Mapping[str, str], summary: Mapping[str, Any] | None = None) -> list[Signal]:
    """Every signal the repository can currently produce about itself."""
    inventory = json.loads((root / "config" / "capability-inventory.json").read_text(encoding="utf-8"))
    signals = ledger_signals(ledger, module_sizes(root))
    signals += capability_signals(inventory)
    signals += delivery_signals(summary or {})
    return signals


def report(orders: Sequence[WorkOrder]) -> str:
    """One line per work order, widest debt first, with the check that retires it."""
    if not orders:
        return "no work proposed: every measured signal is at target"
    lines = []
    for index, order in enumerate(orders, start=1):
        lines.append(f"{index}. [{order.source.value}] {order.title}  (weight {order.weight:g})")
        lines.append(f"     done when: {order.done_when.predicate}")
        lines.append(f"     verify:    {order.done_when.check}")
    return "\n".join(lines)


def issue_plan(orders: Sequence[WorkOrder], *, label: str = "liquid") -> list[dict[str, Any]]:
    """The issues the liquid line would drain, one per work order.

    Rendered, never filed. Filing is an outward effect on a real repository, and a loop that opens
    issues on its own behalf has crossed from proposing into acting -- the one boundary this module
    exists to hold. The caller decides.

    `label` is the line's `trigger.backlog.label`: an issue carrying it is enrolled for the next
    tick, which is what makes this the last mechanical gap between measuring and running.
    """
    return [
        {
            "title": order.title,
            "body": order.as_issue(),
            "labels": sorted({label, *order.labels}),
            "source": order.source.value,
            "key": order.key,
        }
        for order in orders
    ]


def issue_commands(orders: Sequence[WorkOrder], *, label: str = "liquid") -> list[str]:
    """`gh issue create` lines for a person to read before any of them runs."""
    lines = []
    for issue in issue_plan(orders, label=label):
        labels = ",".join(issue["labels"])
        title = shlex.quote(issue["title"])
        body = shlex.quote(issue["body"])
        lines.append(f"gh issue create --title {title} --label {labels} --body {body}")
    return lines
