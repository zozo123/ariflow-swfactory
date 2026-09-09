"""Acceptance for the `workgraph.serial` claim, at the entrypoint Airflow actually calls.

``dags/blueprints.py`` selects ``swfactory.work_stage.build_and_test`` for the managed build task,
so that function — not the legacy loop in ``swfactory.stages`` — is what the claim's invariant is
about: *Plan.work executes only inside the Airflow-owned build stage, honors dependencies and
conflicts, and fans in deterministically.*

These tests drive the whole stage: host-owned run state, workspace-head fencing, one commit per
node, the declared-scope refusal, the execution report, the bounded repair loop, and the legacy
fallback.  Only the sandbox is a fake — an in-memory git whose HEAD really does move per commit —
because the point is the stage's behaviour, not git's.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from swfactory import work_stage
from swfactory.call_accounting import CallLedger
from swfactory.config import Config
from swfactory.models import AgentResult, Issue, Plan, PlanTask, RunResult, StageError, StageResult
from swfactory.stages import Ctx

ISSUE_ID = "WG-1"
ART = f"docs/factory/{ISSUE_ID}"
JUNIT = ".factory/junit.xml"
FACTORY_TOML = f"""\
[commands]
test = "pytest -q --junitxml={JUNIT}"
[paths]
source = "src"
tests = "tests"
protected = ["factory.toml"]
"""
GREEN = '<testsuite tests="3" failures="0" errors="0" skipped="0"/>'
RED = '<testsuite tests="3" failures="1" errors="0" skipped="0"/>'


class GitSandbox:
    """An in-memory workspace whose HEAD advances on commit, so node receipts mean something."""

    name = "fake:workgraph"
    workdir = "/work"

    def __init__(self, *, junit: list[str] | None = None) -> None:
        self.files: dict[str, str] = {"factory.toml": FACTORY_TOML, JUNIT: GREEN}
        self.head = "head0000"
        self.staged: list[str] = []
        self.diffs: dict[tuple[str, str], list[str]] = {}
        self.commits: list[str] = []
        self.junit = list(junit or [])
        self.test_runs = 0

    # -- sandbox protocol ------------------------------------------------
    def ensure(self) -> None:
        return None

    def close(self) -> None:
        return None

    def read(self, path: str) -> str:
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]

    def write(self, path: str, content: str) -> None:
        self.files[path] = content
        if path not in self.staged:
            self.staged.append(path)

    def exists(self, path: str) -> bool:
        return path in self.files

    def run_agent(self, cmd: str, *, timeout_s: int = 1800) -> RunResult:  # pragma: no cover
        return self.run(cmd, timeout_s=timeout_s)

    def run(self, cmd: str, *, cwd: str | None = None, timeout_s: int = 1800) -> RunResult:
        del cwd, timeout_s
        if cmd.startswith("git rev-parse HEAD"):
            return RunResult(0, self.head + "\n", "", 0.0)
        if "git reset -q --" in cmd:
            self.staged = [path for path in self.staged if not path.startswith(f"{ART}/")]
            return RunResult(0, "", "", 0.0)
        if cmd.startswith("git reset --hard"):
            self.head = cmd.split()[-1].strip("'\"")
            self.staged.clear()
            return RunResult(0, "", "", 0.0)
        if cmd == "git diff --cached --quiet":
            return RunResult(1 if self.staged else 0, "", "", 0.0)
        if match := re.match(r"git diff --name-only (\S+)\.\.(\S+)", cmd):
            return RunResult(0, "\n".join(self.diffs.get(match.groups(), [])) + "\n", "", 0.0)
        if "commit -q -m" in cmd:
            before, self.head = self.head, f"{self.head}-{len(self.commits) + 1}"
            self.diffs[(before, self.head)] = sorted(self.staged)
            self.commits.append(cmd)
            self.staged.clear()
            return RunResult(0, "", "", 0.0)
        if cmd.startswith("git status --porcelain"):
            return RunResult(0, "", "", 0.0)
        if cmd.startswith(f"rm -f {JUNIT}"):
            return RunResult(0, "", "", 0.0)
        if cmd.startswith("pytest"):
            self.test_runs += 1
            report = self.junit.pop(0) if self.junit else GREEN
            self.files[JUNIT] = report
            return RunResult(0 if report == GREEN else 1, "1 failed", "", 0.0)
        return RunResult(0, "", "", 0.0)


class NodeAgent:
    """An agent that edits exactly the files it is told to, so scope can be violated on purpose."""

    kind = "scripted"

    def __init__(self, sandbox: GitSandbox, edits: dict[str, list[str]], *, cost: float = 0.0) -> None:
        self.sandbox = sandbox
        self.edits = edits
        self.cost = cost
        self.calls: list[tuple[str, int, str]] = []

    def run(self, sb: Any, *, stage: str, iteration: int, prompt: str = "", **kwargs: Any) -> AgentResult:
        del sb, kwargs
        node = match.group(1) if (match := re.search(r"Node: `([^`]+)`", prompt)) else stage
        self.calls.append((stage, iteration, node))
        for path in self.edits.get(node, []):
            self.sandbox.write(path, f"# written by {node}\n")
        return AgentResult(agent="scripted", data={"summary": f"{node} done"}, cost_usd=self.cost)


class NoScm:
    kind = "local"


def _plan(*tasks: PlanTask) -> Plan:
    declared = sorted({path for task in tasks for path in task.files} or {"src/a.py"})
    return Plan(files=declared, steps=["do it"], tests=["pytest"], work=list(tasks))


def _ctx(tmp_path: Path, sandbox: GitSandbox, agent: NodeAgent, plan: Plan) -> Ctx:
    ctx = Ctx(
        cfg=Config(issue=ISSUE_ID, run_id="wg000001", max_build_iterations=3),
        sb=sandbox,  # type: ignore[arg-type]
        agent=agent,  # type: ignore[arg-type]
        scm=NoScm(),  # type: ignore[arg-type]
        issue=Issue(id=ISSUE_ID, title="workgraph", body="body"),
        run_dir=tmp_path / "run",
    )
    ctx.state.write_control("workspace-head", sandbox.head + "\n")
    ctx.state.write_artifact(f"{ART}/spec.md", "# spec\n")
    ctx.state.write_artifact(f"{ART}/plan.md", "# plan\n")
    ctx.state.write_artifact(f"{ART}/plan.json", plan.model_dump_json())
    return ctx


def _report(ctx: Ctx) -> dict[str, Any]:
    return json.loads(ctx.state.read_artifact(f"{ART}/workgraph-execution.json"))


def test_a_two_node_graph_runs_in_dependency_order_with_one_commit_per_node(tmp_path: Path) -> None:
    plan = _plan(
        PlanTask(id="b", title="B", depends_on=["a"], files=["src/b.py"]),
        PlanTask(id="a", title="A", files=["src/a.py"]),
    )
    sandbox = GitSandbox()
    agent = NodeAgent(sandbox, {"a": ["src/a.py"], "b": ["src/b.py"]})
    ctx = _ctx(tmp_path, sandbox, agent, plan)

    result = work_stage.build_and_test(ctx)
    report = _report(ctx)

    assert [node for _, _, node in agent.calls] == ["a", "b"]
    assert len(sandbox.commits) == 2
    assert "work a:" in sandbox.commits[0] and "work b:" in sandbox.commits[1]
    assert result.numbers["work_nodes"] == 2
    assert result.numbers["parallel_nodes"] == 0
    assert result.numbers["repair_iterations"] == 0
    assert result.numbers["tests_passed"] == 1
    assert report["mode"] == "shared_workspace_serial"
    assert report["scheduler"] == "airflow"
    assert report["parallel"] is False
    assert [row["node_id"] for row in report["nodes"]] == ["b", "a"], "receipts follow Plan.work order"
    assert report["final_head"] == sandbox.head
    assert {row["node_id"]: row["input_head"] for row in report["nodes"]}["b"] == "head0000-1"


def test_declared_file_conflicts_are_reported_rather_than_run_in_parallel(tmp_path: Path) -> None:
    """``parallel_safe`` is a hint from the plan, not permission: the shared workspace serializes."""
    plan = _plan(
        PlanTask(id="a", title="A", files=["src/shared.py"], parallel_safe=True),
        PlanTask(id="b", title="B", files=["src/shared.py"], parallel_safe=True),
    )
    sandbox = GitSandbox()
    ctx = _ctx(tmp_path, sandbox, NodeAgent(sandbox, {"a": ["src/shared.py"], "b": ["src/shared.py"]}), plan)

    work_stage.build_and_test(ctx)
    report = _report(ctx)

    assert report["parallel"] is False
    assert report["declared_conflicts"] == [{"left": "a", "right": "b", "files": ["src/shared.py"]}]


def test_a_node_that_edits_outside_its_declared_files_is_refused_and_rolled_back(tmp_path: Path) -> None:
    """A node's declared files are the review contract; a silent widening would defeat the plan."""
    plan = _plan(PlanTask(id="a", title="A", files=["src/a.py"]))
    sandbox = GitSandbox()
    ctx = _ctx(tmp_path, sandbox, NodeAgent(sandbox, {"a": ["src/a.py", "src/elsewhere.py"]}), plan)

    with pytest.raises(StageError, match="outside its declared scope") as error:
        work_stage.build_and_test(ctx)

    assert error.value.kind == "policy"
    assert sandbox.head == "head0000", "the workspace is restored to the node's input head"
    assert not ctx.state.has_artifact(f"{ART}/workgraph-execution.json")


def test_a_failing_suite_after_fan_in_is_repaired_within_the_build_budget(tmp_path: Path) -> None:
    plan = _plan(PlanTask(id="a", title="A", files=["src/a.py"]))
    sandbox = GitSandbox(junit=[RED, GREEN])
    agent = NodeAgent(sandbox, {"a": ["src/a.py"], "fix": ["src/a.py"]})
    ctx = _ctx(tmp_path, sandbox, agent, plan)

    result = work_stage.build_and_test(ctx)

    assert [stage for stage, _, _ in agent.calls] == ["build", "fix"]
    assert result.numbers["repair_iterations"] == 1
    assert result.numbers["first_pass_ci"] == 0
    assert result.numbers["tests_passed"] == 1


def test_every_node_and_repair_attempt_is_journalled_as_its_own_paid_call(tmp_path: Path) -> None:
    """A work graph spends per node and again per repair. Accounting only the stage total would
    leave those calls invisible to the ceiling the next task rebuilds, and would lose whichever of
    them was in flight when the cell died -- the graph is where most of a run's money goes."""
    plan = _plan(
        PlanTask(id="b", title="B", depends_on=["a"], files=["src/b.py"]),
        PlanTask(id="a", title="A", files=["src/a.py"]),
    )
    sandbox = GitSandbox(junit=[RED, GREEN])
    agent = NodeAgent(sandbox, {"a": ["src/a.py"], "b": ["src/b.py"], "fix": ["src/a.py"]}, cost=0.5)
    ctx = _ctx(tmp_path, sandbox, agent, plan)

    work_stage.build_and_test(ctx)

    ledger = CallLedger(ctx.state)
    records = ledger.records()
    assert [(row.stage, row.iteration) for row in records] == [("build", 1), ("build", 2), ("fix", 3)]
    assert [row.state for row in records] == ["committed"] * 3
    assert [row.settled_usd for row in records] == [0.5, 0.5, 0.5]
    assert ledger.charged_usd() == 1.5 and ledger.unreconciled() == []


def test_a_suite_that_never_goes_green_stops_the_stage_within_its_bound(tmp_path: Path) -> None:
    plan = _plan(PlanTask(id="a", title="A", files=["src/a.py"]))
    sandbox = GitSandbox(junit=[RED, RED, RED, RED])
    agent = NodeAgent(sandbox, {"a": ["src/a.py"], "fix": ["src/a.py"]})
    ctx = _ctx(tmp_path, sandbox, agent, plan)

    with pytest.raises(StageError, match="bounded repair"):
        work_stage.build_and_test(ctx)

    assert [stage for stage, _, _ in agent.calls] == ["build", "fix", "fix"]


def test_a_plan_without_work_nodes_still_runs_the_legacy_build_loop(tmp_path: Path) -> None:
    """The managed stage serves every plan; a plan with no work graph must not lose its build."""
    plan = _plan()
    sandbox = GitSandbox()
    agent = NodeAgent(sandbox, {"build": ["src/a.py"]})
    ctx = _ctx(tmp_path, sandbox, agent, plan)

    result = work_stage.build_and_test(ctx)

    assert [stage for stage, _, _ in agent.calls] == ["build"]
    assert result.numbers["iterations"] == 1
    assert "work_nodes" not in result.numbers
    assert not ctx.state.has_artifact(f"{ART}/workgraph-execution.json")


def test_an_airflow_retry_of_a_completed_stage_re_runs_no_node(tmp_path: Path) -> None:
    """Airflow retries tasks; the host stage log is the only thing allowed to skip the work."""
    plan = _plan(PlanTask(id="a", title="A", files=["src/a.py"]))
    sandbox = GitSandbox()
    agent = NodeAgent(sandbox, {"a": ["src/a.py"]})
    ctx = _ctx(tmp_path, sandbox, agent, plan)
    first = work_stage.build_and_test(ctx)

    again = work_stage.build_and_test(ctx)

    assert again.status == "skipped"
    assert again.numbers == first.numbers
    assert len(agent.calls) == 1
    assert len(sandbox.commits) == 1


def test_the_stage_result_is_journalled_for_the_next_airflow_task(tmp_path: Path) -> None:
    plan = _plan(PlanTask(id="a", title="A", files=["src/a.py"]))
    sandbox = GitSandbox()
    ctx = _ctx(tmp_path, sandbox, NodeAgent(sandbox, {"a": ["src/a.py"]}), plan)

    work_stage.build_and_test(ctx)

    records = [StageResult.model_validate(row) for row in ctx.state.read_jsonl("stages.jsonl")]
    assert [record.stage for record in records] == ["build_and_test"]
    assert records[0].artifacts == [f"{ART}/workgraph-execution.json"]
