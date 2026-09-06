"""Typed data that crosses stage boundaries. Prose artifacts (intent/spec) stay markdown."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, NamedTuple

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from swfactory.paths import validate_identifier, validate_run_id

Severity = Literal["blocker", "major", "minor", "nit"]
AgentKind = Literal["claude", "scripted"]
AgentRole = Literal[
    "issue_maker",
    "groomer",
    "planner",
    "code_writer",
    "reviewer",
    "improver",
    "deliverer",
]


class BoundaryModel(BaseModel):
    """Strict base for values crossing an orchestration or trust boundary."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class Issue(BoundaryModel):
    id: str
    title: str = Field(min_length=1)
    body: str  # verbatim originator text
    labels: list[str] = Field(default_factory=list)
    url: str | None = None

    @field_validator("id")
    @classmethod
    def _safe_id(cls, value: str) -> str:
        return validate_identifier(value, field="issue.id")


class RunResult(NamedTuple):
    exit_code: int
    stdout: str
    stderr: str
    duration_s: float
    timed_out: bool = False

    @property
    def ok(self) -> bool:
        return self.exit_code == 0 and not self.timed_out


class TestResult(BoundaryModel):
    passed: int = Field(default=0, ge=0)
    failed: int = Field(default=0, ge=0)
    errors: int = Field(default=0, ge=0)
    skipped: int = Field(default=0, ge=0)
    exit_code: int
    junit_path: str | None = None
    report_valid: bool = True

    @property
    def total(self) -> int:
        return self.passed + self.failed + self.errors + self.skipped

    @property
    def ok(self) -> bool:
        return (
            self.report_valid
            and self.total > 0
            and self.exit_code == 0
            and self.failed == 0
            and self.errors == 0
        )


class Finding(BoundaryModel):
    severity: Severity
    file: str
    line: int | None = Field(default=None, ge=1)
    title: str = Field(min_length=1)
    detail: str = Field(min_length=1)


class Review(BoundaryModel):
    """Reviewer output contract (REVIEW.md); fed to `claude --json-schema`."""

    verdict: Literal["approve", "request_changes"]
    findings: list[Finding] = Field(default_factory=list)

    @property
    def blockers(self) -> list[Finding]:
        return [f for f in self.findings if f.severity == "blocker"]

    @model_validator(mode="after")
    def _verdict_matches_findings(self) -> Review:
        expected = "request_changes" if self.blockers else "approve"
        if self.verdict != expected:
            raise ValueError(f"review verdict must be {expected!r} for its findings")
        return self


class PlanTask(BoundaryModel):
    """One node in an issue-specific work graph produced by the planner.

    The fixed Airflow DAG remains the scheduler. These nodes describe dependencies *inside* the
    governed build cell so the plan can preserve parallelism/fork opportunities without creating
    per-issue Airflow DAG definitions at runtime.
    """

    id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,62}$")
    title: str = Field(min_length=1, max_length=240)
    role: Literal["code_writer"] = "code_writer"
    depends_on: list[str] = Field(default_factory=list, max_length=32)
    files: list[str] = Field(default_factory=list, max_length=128)
    tests: list[str] = Field(default_factory=list, max_length=128)
    parallel_safe: bool = False

    @field_validator("depends_on")
    @classmethod
    def _unique_dependencies(cls, values: list[str]) -> list[str]:
        values = [value.strip() for value in values]
        if any(not value for value in values):
            raise ValueError("work dependencies must not be empty")
        if len(values) != len(set(values)):
            raise ValueError("work dependencies must not contain duplicates")
        return values


class Plan(BoundaryModel):
    """plan.md content, typed so plan-fidelity can be checked by code.

    ``work`` is the issue-specific DAG. It is optional for backward compatibility with stored
    plans; an empty graph means the build stage treats ``steps`` as one serial code-writer node.
    """

    files: list[str]
    steps: list[str]
    tests: list[str]
    risks: list[str] = Field(default_factory=list)
    work: list[PlanTask] = Field(default_factory=list, max_length=64)

    @model_validator(mode="after")
    def _validate_work_graph(self) -> Plan:
        if not self.work:
            return self

        ids = [node.id for node in self.work]
        if len(ids) != len(set(ids)):
            raise ValueError("work graph node ids must be unique")
        known = set(ids)
        declared_files = set(self.files)
        by_id = {node.id: node for node in self.work}

        for node in self.work:
            if node.id in node.depends_on:
                raise ValueError(f"work graph node {node.id!r} cannot depend on itself")
            unknown = sorted(set(node.depends_on) - known)
            if unknown:
                raise ValueError(f"work graph node {node.id!r} has unknown dependencies {unknown}")
            undeclared = sorted(set(node.files) - declared_files)
            if undeclared:
                raise ValueError(
                    f"work graph node {node.id!r} references files not declared in plan.files: "
                    f"{undeclared}"
                )

        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(node_id: str) -> None:
            if node_id in visited:
                return
            if node_id in visiting:
                raise ValueError(f"work graph contains a cycle at {node_id!r}")
            visiting.add(node_id)
            for parent in by_id[node_id].depends_on:
                visit(parent)
            visiting.remove(node_id)
            visited.add(node_id)

        for node_id in ids:
            visit(node_id)
        return self

    def work_layers(self) -> list[list[PlanTask]]:
        """Stable topological waves; independent nodes in one wave may be fork candidates."""
        if not self.work:
            return []
        remaining = {node.id: node for node in self.work}
        done: set[str] = set()
        layers: list[list[PlanTask]] = []
        while remaining:
            layer = [
                node for node in self.work if node.id in remaining and set(node.depends_on) <= done
            ]
            if not layer:
                raise ValueError("work graph is cyclic")
            layers.append(layer)
            done.update(node.id for node in layer)
            for node in layer:
                remaining.pop(node.id, None)
        return layers

    def fork_candidates(self) -> list[list[str]]:
        """Parallel-safe nodes sharing a topological wave; a hint, never a capability claim."""
        return [
            [node.id for node in layer if node.parallel_safe]
            for layer in self.work_layers()
            if sum(node.parallel_safe for node in layer) > 1
        ]

    def to_markdown(self, issue_id: str) -> str:
        def section(title: str, items: list[str]) -> str:
            body = "\n".join(f"- {i}" for i in items) or "- (none)"
            return f"## {title}\n{body}\n"

        work = [
            f"`{node.id}` [{node.role}] after "
            f"{', '.join(f'`{dep}`' for dep in node.depends_on) or '`plan`'}"
            f"{' · forkable' if node.parallel_safe else ''}: {node.title}"
            for node in self.work
        ]
        text = (
            f"# Plan — {issue_id}\n\n"
            + section("Files", self.files)
            + "\n"
            + section("Order", self.steps)
            + "\n"
            + section("Tests", self.tests)
            + "\n"
            + section("Risks", self.risks)
        )
        if work:
            text += "\n" + section("Work graph", work)
        return text


class BuildSummary(BoundaryModel):
    summary: str
    files_changed: list[str] = Field(default_factory=list)


class Diagnosis(BoundaryModel):
    metric: str
    hypothesis: str
    evidence: list[str] = Field(default_factory=list)
    proposed_intent: str | None = None


class AgentResult(BoundaryModel):
    agent: AgentKind
    text: str = ""
    data: dict | None = None  # structured output when a schema was requested
    cost_usd: float = Field(default=0.0, ge=0)
    num_turns: int = Field(default=0, ge=0)
    duration_ms: int = Field(default=0, ge=0)
    session_id: str | None = None
    is_error: bool = False
    subtype: str = "success"  # e.g. error_max_turns, error_max_budget_usd


class Approval(BoundaryModel):
    gate: Literal["intent", "plan"]
    decision: Literal["approve", "reject"]
    actor: str = Field(min_length=1)  # os user, Airflow responded_by_user, or "auto"
    at: datetime
    artifact_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")


class StageResult(BoundaryModel):
    stage: str = Field(min_length=1)
    status: Literal["ok", "skipped", "blocked", "failed"] = "ok"
    artifacts: list[str] = Field(default_factory=list)  # repo-relative paths written
    numbers: dict[str, float] = Field(default_factory=dict)
    cost_usd: float = Field(default=0.0, ge=0)
    duration_s: float = Field(default=0.0, ge=0)
    preview: str = ""  # gate artifact head (intent.md / plan.md), shown by the ApprovalOperator
    error: str | None = None


class RunReport(BoundaryModel):
    run_id: str
    issue_id: str
    agent: AgentKind
    sandbox: str
    scm: str
    stages: list[StageResult]
    approvals: list[Approval]
    pr_url: str | None = None
    tests_passed: bool = False
    total_cost_usd: float = Field(default=0.0, ge=0)

    @field_validator("run_id")
    @classmethod
    def _safe_run_id(cls, value: str) -> str:
        return validate_run_id(value)

    @field_validator("issue_id")
    @classmethod
    def _safe_issue_id(cls, value: str) -> str:
        return validate_identifier(value, field="issue_id")

    def table(self) -> str:
        rows = [
            ("run", self.run_id),
            ("issue", self.issue_id),
            ("agent / sandbox / scm", f"{self.agent} / {self.sandbox} / {self.scm}"),
            ("stages", " → ".join(f"{s.stage}:{s.status}" for s in self.stages)),
            ("approvals", ", ".join(f"{a.gate}={a.decision} by {a.actor}" for a in self.approvals)),
            ("tests passed", str(self.tests_passed)),
            ("pr", self.pr_url or "-"),
            ("cost usd", f"{self.total_cost_usd:.4f}"),
        ]
        for s in self.stages:
            if s.numbers:
                rows.append((f"  {s.stage}", ", ".join(f"{k}={v:g}" for k, v in s.numbers.items())))
        width = max(len(k) for k, _ in rows)
        return "\n".join(f"{k.ljust(width)}  {v}" for k, v in rows)


class StageError(RuntimeError):
    def __init__(
        self,
        kind: Literal["agent", "sandbox", "scm", "policy"],
        msg: str,
        *,
        retryable: bool = False,
    ) -> None:
        super().__init__(f"[{kind}] {msg}")
        self.kind = kind
        self.retryable = retryable
