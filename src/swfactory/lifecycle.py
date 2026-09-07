"""Managed SDLC graph: fixed scheduler topology, dynamic per-issue work as validated data.

The factory deliberately does *not* synthesize Airflow DAG files per GitHub issue. Airflow owns a
small stable set of production-line DAGs; ``Plan.work`` captures the issue-specific graph. This
module projects that graph onto explicit software-factory roles for inspection, policy and future
fork-capable executors without claiming a sandbox provider can clone a live cell today.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from swfactory.models import AgentRole, BoundaryModel, Plan, PlanTask
from swfactory.paths import validate_identifier

Execution = Literal["external", "airflow_stage", "inside_stage"]


class ManagedNode(BoundaryModel):
    id: str = Field(pattern=r"^[a-z][a-z0-9_:-]{0,95}$")
    role: AgentRole
    title: str = Field(min_length=1, max_length=240)
    stage: Literal["intent", "spec", "plan", "build_and_test", "review", "deliver"]
    execution: Execution
    depends_on: list[str] = Field(default_factory=list)
    condition: str | None = None
    parallel_safe: bool = False


class ManagedGraph(BoundaryModel):
    version: Literal[1] = 1
    issue_id: str
    scheduler: Literal["fixed-airflow-dag"] = "fixed-airflow-dag"
    fork_semantics: Literal["hint-only"] = "hint-only"
    fork_evidence: Literal["lineage-required"] = "lineage-required"
    nodes: list[ManagedNode] = Field(min_length=1, max_length=80)

    @model_validator(mode="after")
    def _valid_dag(self) -> ManagedGraph:
        validate_identifier(self.issue_id, field="issue_id")
        ids = [node.id for node in self.nodes]
        if len(ids) != len(set(ids)):
            raise ValueError("managed graph node ids must be unique")
        known = set(ids)
        by_id = {node.id: node for node in self.nodes}
        for node in self.nodes:
            unknown = sorted(set(node.depends_on) - known)
            if unknown:
                raise ValueError(f"managed node {node.id!r} has unknown dependencies {unknown}")
            if node.id in node.depends_on:
                raise ValueError(f"managed node {node.id!r} cannot depend on itself")

        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(node_id: str) -> None:
            if node_id in visited:
                return
            if node_id in visiting:
                raise ValueError(f"managed graph contains a cycle at {node_id!r}")
            visiting.add(node_id)
            for parent in by_id[node_id].depends_on:
                visit(parent)
            visiting.remove(node_id)
            visited.add(node_id)

        for node_id in ids:
            visit(node_id)
        return self

    def layers(self) -> list[list[ManagedNode]]:
        remaining = {node.id: node for node in self.nodes}
        done: set[str] = set()
        layers: list[list[ManagedNode]] = []
        while remaining:
            layer = [node for node in self.nodes if node.id in remaining and set(node.depends_on) <= done]
            if not layer:
                raise ValueError("managed graph is cyclic")
            layers.append(layer)
            done.update(node.id for node in layer)
            for node in layer:
                remaining.pop(node.id, None)
        return layers

    def fork_candidates(self) -> list[list[str]]:
        """Parallel-safe inside-stage nodes in the same wave; runtime support is not implied."""
        groups: list[list[str]] = []
        for layer in self.layers():
            candidates = [node.id for node in layer if node.execution == "inside_stage" and node.parallel_safe]
            if len(candidates) > 1:
                groups.append(candidates)
        return groups

    def to_mermaid(self) -> str:
        lines = ["flowchart LR"]
        for node in self.nodes:
            label = f"{node.role}\\n{node.title}".replace('"', "'")
            lines.append(f'  {node.id.replace(":", "_")}["{label}"]')
        for node in self.nodes:
            for parent in node.depends_on:
                condition = f"|{node.condition}|" if node.condition else ""
                lines.append(f"  {parent.replace(':', '_')} -->{condition} {node.id.replace(':', '_')}")
        return "\n".join(lines) + "\n"


def _work_nodes(plan: Plan) -> list[PlanTask]:
    if plan.work:
        return plan.work
    title = plan.steps[0] if plan.steps else "Implement the approved plan"
    return [PlanTask(id="change", title=title, files=plan.files, tests=plan.tests)]


def compile_graph(issue_id: str, plan: Plan) -> ManagedGraph:
    """Project an issue plan onto the managed lifecycle roles.

    Work nodes are namespaced with ``work:``. Root work depends on the fixed planner node;
    terminal work feeds the fixed reviewer. Review repair is represented as the real bounded
    improver loop inside the review stage. ``parallel_safe`` is only a fork hint until a sandbox
    backend advertises clone/fork semantics and the executor gains a deterministic merge protocol.
    Native forks must preserve evidence identifiers and parent lineage; sibling results are not
    independent merely because they ran in separate sandboxes.
    """
    validate_identifier(issue_id, field="issue_id")
    work = _work_nodes(plan)
    work_ids = {node.id for node in work}
    children = {node_id: set() for node_id in work_ids}
    for node in work:
        for parent in node.depends_on:
            children[parent].add(node.id)

    nodes = [
        ManagedNode(
            id="issue",
            role="issue_maker",
            title="GitHub work order",
            stage="intent",
            execution="external",
        ),
        ManagedNode(
            id="groom",
            role="groomer",
            title="Bound the specification",
            stage="spec",
            execution="airflow_stage",
            depends_on=["issue"],
        ),
        ManagedNode(
            id="plan",
            role="planner",
            title="Validate the issue work graph",
            stage="plan",
            execution="airflow_stage",
            depends_on=["groom"],
        ),
    ]
    for node in work:
        parents = [f"work:{parent}" for parent in node.depends_on] or ["plan"]
        nodes.append(
            ManagedNode(
                id=f"work:{node.id}",
                role=node.role,
                title=node.title,
                stage="build_and_test",
                execution="inside_stage",
                depends_on=parents,
                parallel_safe=node.parallel_safe,
            )
        )

    terminals = [f"work:{node.id}" for node in work if not children[node.id]]
    nodes.extend(
        [
            ManagedNode(
                id="review",
                role="reviewer",
                title="Review code, tests and plan fidelity",
                stage="review",
                execution="airflow_stage",
                depends_on=terminals,
            ),
            ManagedNode(
                id="improve",
                role="improver",
                title="Repair blocking review findings",
                stage="review",
                execution="inside_stage",
                depends_on=["review"],
                condition="review=request_changes",
            ),
            ManagedNode(
                id="deliver",
                role="deliverer",
                title="Publish evidence and pull request",
                stage="deliver",
                execution="airflow_stage",
                depends_on=["review"],
                condition="review_stage=complete",
            ),
        ]
    )
    return ManagedGraph(issue_id=issue_id, nodes=nodes)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Render a plan.json as the managed lifecycle DAG")
    parser.add_argument("plan", type=Path, help="path to a plan.json artifact")
    parser.add_argument("--issue", required=True, help="safe factory issue id")
    parser.add_argument("--format", choices=("json", "mermaid"), default="json")
    return parser


def main() -> None:
    args = _parser().parse_args()
    plan = Plan.model_validate_json(args.plan.read_text(encoding="utf-8"))
    graph = compile_graph(args.issue, plan)
    if args.format == "mermaid":
        print(graph.to_mermaid(), end="")
    else:
        print(json.dumps(graph.model_dump(mode="json"), indent=2))


if __name__ == "__main__":
    main()
