"""Git-native lineage for bounded candidate exploration.

A campaign is one *round*: co-equal candidates fan out from the same immutable
input revision. Several rounds form a tree by descending only from the selected
answered candidate of the previous round. That gives the Liquid factory the
same useful shape as a disciplined experiment workflow: width explores one
decision, depth accumulates decisions that actually won.

This module is deliberately advisory. It schedules nothing, mutates no
repository and grants no promotion authority. It records which revision a
candidate answered on and makes invalid lineage fail closed.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterable


class ExperimentTreeError(ValueError):
    """The supplied lineage cannot describe one deterministic experiment tree."""


class NodeState(StrEnum):
    PROVISIONAL = "provisional"
    ANSWERED = "answered"


@dataclass(frozen=True)
class ExperimentNode:
    """One candidate in one round.

    ``answered`` means the candidate produced a result tied to a distinct recorded
    revision. Answered nodes are frozen evidence: later work must branch from one,
    never rewrite it in place. Infrastructure failures remain provisional.
    """

    id: str
    parent_id: str | None
    depth: int
    strategy: str
    input_head: str
    result: str
    state: NodeState
    recorded_head: str | None = None
    selected: bool = False
    evidence: tuple[str, ...] = ()
    detail: str = ""

    @property
    def frozen(self) -> bool:
        return self.state == NodeState.ANSWERED

    def validate(self) -> None:
        if not self.id.strip():
            raise ExperimentTreeError("experiment node id must be nonempty")
        if self.depth < 0:
            raise ExperimentTreeError(f"{self.id}: depth must be nonnegative")
        if not self.input_head.strip():
            raise ExperimentTreeError(f"{self.id}: input head must be nonempty")
        if self.state == NodeState.ANSWERED and not self.recorded_head:
            raise ExperimentTreeError(f"{self.id}: answered node has no recorded head")
        if self.state == NodeState.PROVISIONAL and self.selected:
            raise ExperimentTreeError(f"{self.id}: provisional node cannot be selected")
        if self.selected and self.state != NodeState.ANSWERED:
            raise ExperimentTreeError(f"{self.id}: selected node must be answered")

    def to_dict(self) -> dict[str, Any]:
        document = asdict(self)
        document["state"] = self.state.value
        document["frozen"] = self.frozen
        return document

    @classmethod
    def from_dict(cls, document: dict[str, Any]) -> ExperimentNode:
        return cls(
            id=str(document["id"]),
            parent_id=(str(document["parent_id"]) if document.get("parent_id") is not None else None),
            depth=int(document["depth"]),
            strategy=str(document["strategy"]),
            input_head=str(document["input_head"]),
            result=str(document["result"]),
            state=NodeState(str(document["state"])),
            recorded_head=(
                str(document["recorded_head"]) if document.get("recorded_head") is not None else None
            ),
            selected=bool(document.get("selected", False)),
            evidence=tuple(str(item) for item in document.get("evidence", ())),
            detail=str(document.get("detail", "")),
        )


@dataclass(frozen=True)
class ExperimentRound:
    """One sibling bush: several co-equal candidates from one parent revision."""

    round_id: str
    input_head: str
    depth: int
    parent_candidate: str | None
    nodes: tuple[ExperimentNode, ...]
    winner_id: str | None = None

    def validate(self) -> None:
        if not self.round_id.strip():
            raise ExperimentTreeError("round id must be nonempty")
        if not self.input_head.strip():
            raise ExperimentTreeError(f"{self.round_id}: input head must be nonempty")
        if self.depth < 0:
            raise ExperimentTreeError(f"{self.round_id}: depth must be nonnegative")
        if self.depth == 0 and self.parent_candidate is not None:
            raise ExperimentTreeError(f"{self.round_id}: first round cannot name a parent candidate")
        if self.depth > 0 and not self.parent_candidate:
            raise ExperimentTreeError(f"{self.round_id}: depth {self.depth} requires a parent candidate")
        if not self.nodes:
            raise ExperimentTreeError(f"{self.round_id}: round needs at least one candidate")

        ids = [node.id for node in self.nodes]
        if len(ids) != len(set(ids)):
            raise ExperimentTreeError(f"{self.round_id}: duplicate candidate id")

        selected = []
        for node in self.nodes:
            node.validate()
            if node.depth != self.depth:
                raise ExperimentTreeError(f"{node.id}: depth differs from round")
            if node.parent_id != self.parent_candidate:
                raise ExperimentTreeError(f"{node.id}: parent differs from round")
            if node.input_head != self.input_head:
                raise ExperimentTreeError(f"{node.id}: input head differs from round")
            if node.selected:
                selected.append(node.id)

        if self.winner_id is None:
            if selected:
                raise ExperimentTreeError(f"{self.round_id}: selected node exists without winner")
        else:
            if self.winner_id not in ids:
                raise ExperimentTreeError(f"{self.round_id}: winner is not a candidate in this round")
            if selected != [self.winner_id]:
                raise ExperimentTreeError(f"{self.round_id}: winner and selected node disagree")

    @property
    def winner(self) -> ExperimentNode | None:
        if self.winner_id is None:
            return None
        return next(node for node in self.nodes if node.id == self.winner_id)

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema_version": 1,
            "round_id": self.round_id,
            "input_head": self.input_head,
            "depth": self.depth,
            "parent_candidate": self.parent_candidate,
            "winner_id": self.winner_id,
            "nodes": [node.to_dict() for node in self.nodes],
        }

    @classmethod
    def from_dict(cls, document: dict[str, Any]) -> ExperimentRound:
        round_ = cls(
            round_id=str(document["round_id"]),
            input_head=str(document["input_head"]),
            depth=int(document["depth"]),
            parent_candidate=(
                str(document["parent_candidate"]) if document.get("parent_candidate") is not None else None
            ),
            winner_id=(str(document["winner_id"]) if document.get("winner_id") is not None else None),
            nodes=tuple(ExperimentNode.from_dict(item) for item in document.get("nodes", ())),
        )
        round_.validate()
        return round_


@dataclass(frozen=True)
class ExperimentTree:
    """A sequence of sibling bushes stacked on confirmed winners."""

    root_head: str
    rounds: tuple[ExperimentRound, ...]

    def validate(self) -> None:
        if not self.root_head.strip():
            raise ExperimentTreeError("tree root head must be nonempty")
        if not self.rounds:
            raise ExperimentTreeError("experiment tree needs at least one round")

        ordered = tuple(sorted(self.rounds, key=lambda round_: (round_.depth, round_.round_id)))
        if ordered != self.rounds:
            raise ExperimentTreeError("rounds must be stored in deterministic depth order")

        node_ids: set[str] = set()
        previous: ExperimentRound | None = None
        for index, round_ in enumerate(self.rounds):
            round_.validate()
            if round_.depth != index:
                raise ExperimentTreeError(
                    f"{round_.round_id}: expected depth {index}, got {round_.depth}; stacked bushes cannot skip depth"
                )
            overlap = node_ids.intersection(node.id for node in round_.nodes)
            if overlap:
                raise ExperimentTreeError(f"candidate reused across rounds: {', '.join(sorted(overlap))}")
            node_ids.update(node.id for node in round_.nodes)

            if previous is None:
                if round_.input_head != self.root_head:
                    raise ExperimentTreeError("first round does not start at tree root")
            else:
                winner = previous.winner
                if winner is None:
                    raise ExperimentTreeError(
                        f"{round_.round_id}: cannot descend because {previous.round_id} has no winner"
                    )
                if round_.parent_candidate != winner.id:
                    raise ExperimentTreeError(
                        f"{round_.round_id}: parent must be previous winner {winner.id}, got {round_.parent_candidate}"
                    )
                if round_.input_head != winner.recorded_head:
                    raise ExperimentTreeError(
                        f"{round_.round_id}: input head must be winner revision {winner.recorded_head}"
                    )
            previous = round_

    @property
    def winner(self) -> ExperimentNode | None:
        self.validate()
        return self.rounds[-1].winner

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema_version": 1,
            "root_head": self.root_head,
            "rounds": [round_.to_dict() for round_ in self.rounds],
        }


def stack_rounds(rounds: Iterable[ExperimentRound]) -> ExperimentTree:
    ordered = tuple(sorted(rounds, key=lambda round_: (round_.depth, round_.round_id)))
    if not ordered:
        raise ExperimentTreeError("cannot stack an empty round set")
    tree = ExperimentTree(root_head=ordered[0].input_head, rounds=ordered)
    tree.validate()
    return tree


def render(tree: ExperimentTree) -> str:
    tree.validate()
    lines = [f"root  {tree.root_head}"]
    for round_ in tree.rounds:
        indent = "  " * round_.depth
        parent = round_.parent_candidate or "root"
        lines.append(f"{indent}round {round_.depth}  {round_.round_id}  parent={parent}")
        for node in round_.nodes:
            mark = "*" if node.selected else " "
            frozen = "frozen" if node.frozen else "provisional"
            head = node.recorded_head or "-"
            lines.append(
                f"{indent}  {mark} {node.id}  {node.strategy:<8} {frozen:<11} result={node.result:<9} head={head}"
            )
    return "\n".join(lines)


def load_round(path: Path) -> ExperimentRound:
    document = json.loads(path.read_text(encoding="utf-8"))
    payload = document.get("experiment_round", document)
    if not isinstance(payload, dict):
        raise ExperimentTreeError(f"{path}: missing experiment_round object")
    return ExperimentRound.from_dict(payload)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate and render stacked candidate experiment rounds.")
    parser.add_argument("reports", nargs="+", type=Path, help="campaign report JSON files, oldest round first")
    parser.add_argument("--json", action="store_true", help="emit the validated combined tree as JSON")
    args = parser.parse_args(argv)
    tree = stack_rounds(load_round(path) for path in args.reports)
    if args.json:
        print(json.dumps(tree.to_dict(), indent=2, sort_keys=True))
    else:
        print(render(tree))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
