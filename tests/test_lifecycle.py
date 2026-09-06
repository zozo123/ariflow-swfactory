from __future__ import annotations

import pytest
from pydantic import ValidationError

from swfactory.lifecycle import compile_graph
from swfactory.models import Plan, PlanTask


def _plan(*work: PlanTask) -> Plan:
    return Plan(
        files=["src/a.py", "src/b.py"],
        steps=["write tests", "implement"],
        tests=["R1: regression"],
        risks=[],
        work=list(work),
    )


def test_plan_work_graph_rejects_unknown_dependency() -> None:
    with pytest.raises(ValidationError, match="unknown dependencies"):
        _plan(PlanTask(id="impl", title="Implement", depends_on=["missing"]))


def test_plan_work_graph_rejects_cycles() -> None:
    with pytest.raises(ValidationError, match="cycle"):
        _plan(
            PlanTask(id="a", title="A", depends_on=["b"]),
            PlanTask(id="b", title="B", depends_on=["a"]),
        )


def test_plan_work_graph_rejects_undeclared_files() -> None:
    with pytest.raises(ValidationError, match="not declared"):
        _plan(PlanTask(id="impl", title="Implement", files=["src/c.py"]))


def test_work_layers_preserve_parallelism_and_fork_hints() -> None:
    plan = _plan(
        PlanTask(id="tests", title="Add tests", files=["src/a.py"]),
        PlanTask(id="api", title="Implement API", files=["src/a.py"], parallel_safe=True),
        PlanTask(id="docs", title="Update docs", files=["src/b.py"], parallel_safe=True),
        PlanTask(id="wire", title="Wire integration", depends_on=["api", "docs"]),
    )
    assert [[node.id for node in layer] for layer in plan.work_layers()] == [
        ["tests", "api", "docs"],
        ["wire"],
    ]
    assert plan.fork_candidates() == [["api", "docs"]]


def test_compile_graph_uses_fixed_roles_and_dynamic_work() -> None:
    plan = _plan(
        PlanTask(id="tests", title="Add tests", files=["src/a.py"]),
        PlanTask(
            id="impl",
            title="Implement",
            depends_on=["tests"],
            files=["src/b.py"],
        ),
    )
    graph = compile_graph("42", plan)
    by_id = {node.id: node for node in graph.nodes}

    assert graph.scheduler == "fixed-airflow-dag"
    assert by_id["issue"].role == "issue_maker"
    assert by_id["groom"].role == "groomer"
    assert by_id["plan"].role == "planner"
    assert by_id["work:impl"].depends_on == ["work:tests"]
    assert by_id["review"].depends_on == ["work:impl"]
    assert by_id["improve"].condition == "review=request_changes"
    assert by_id["deliver"].role == "deliverer"
    assert by_id["deliver"].depends_on == ["review"]


def test_compile_graph_falls_back_for_legacy_plan() -> None:
    graph = compile_graph("DEMO-1", _plan())
    work = [node for node in graph.nodes if node.id.startswith("work:")]
    assert len(work) == 1
    assert work[0].id == "work:change"
    assert work[0].role == "code_writer"


def test_managed_graph_fork_candidates_are_hint_only() -> None:
    graph = compile_graph(
        "42",
        _plan(
            PlanTask(id="a", title="A", files=["src/a.py"], parallel_safe=True),
            PlanTask(id="b", title="B", files=["src/b.py"], parallel_safe=True),
        ),
    )
    assert graph.fork_semantics == "hint-only"
    assert graph.fork_evidence == "lineage-required"
    assert graph.fork_candidates() == [["work:a", "work:b"]]


def test_mermaid_contains_role_labels() -> None:
    mermaid = compile_graph("42", _plan()).to_mermaid()
    assert "flowchart LR" in mermaid
    assert "issue_maker" in mermaid
    assert "reviewer" in mermaid
    assert "deliverer" in mermaid
