"""Typed data that crosses stage boundaries. Prose artifacts (intent/spec) stay markdown."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, NamedTuple

from pydantic import BaseModel, Field

Severity = Literal["blocker", "major", "minor", "nit"]
AgentKind = Literal["claude", "scripted"]


class Issue(BaseModel):
    id: str
    title: str
    body: str  # verbatim originator text
    labels: list[str] = Field(default_factory=list)
    url: str | None = None


class RunResult(NamedTuple):
    exit_code: int
    stdout: str
    stderr: str
    duration_s: float
    timed_out: bool = False

    @property
    def ok(self) -> bool:
        return self.exit_code == 0 and not self.timed_out


class TestResult(BaseModel):
    passed: int = 0
    failed: int = 0
    errors: int = 0
    skipped: int = 0
    exit_code: int
    junit_path: str | None = None

    @property
    def ok(self) -> bool:
        return self.exit_code == 0 and self.failed == 0 and self.errors == 0


class Finding(BaseModel):
    severity: Severity
    file: str
    line: int | None = None
    title: str
    detail: str


class Review(BaseModel):
    """Reviewer output contract (REVIEW.md); fed to `claude --json-schema`."""

    verdict: Literal["approve", "request_changes"]
    findings: list[Finding] = Field(default_factory=list)

    @property
    def blockers(self) -> list[Finding]:
        return [f for f in self.findings if f.severity == "blocker"]


class Plan(BaseModel):
    """plan.md content, typed so plan-fidelity can be checked by code."""

    files: list[str]
    steps: list[str]
    tests: list[str]
    risks: list[str] = Field(default_factory=list)

    def to_markdown(self, issue_id: str) -> str:
        def section(title: str, items: list[str]) -> str:
            body = "\n".join(f"- {i}" for i in items) or "- (none)"
            return f"## {title}\n{body}\n"

        return (
            f"# Plan — {issue_id}\n\n"
            + section("Files", self.files)
            + "\n"
            + section("Order", self.steps)
            + "\n"
            + section("Tests", self.tests)
            + "\n"
            + section("Risks", self.risks)
        )


class BuildSummary(BaseModel):
    summary: str
    files_changed: list[str] = Field(default_factory=list)


class Diagnosis(BaseModel):
    metric: str
    hypothesis: str
    evidence: list[str] = Field(default_factory=list)
    proposed_intent: str | None = None


class AgentResult(BaseModel):
    agent: AgentKind
    text: str = ""
    data: dict | None = None  # structured output when a schema was requested
    cost_usd: float = 0.0
    num_turns: int = 0
    duration_ms: int = 0
    session_id: str | None = None
    is_error: bool = False
    subtype: str = "success"  # e.g. error_max_turns, error_max_budget_usd


class Approval(BaseModel):
    gate: Literal["intent", "plan"]
    decision: Literal["approve", "reject"]
    actor: str  # os user, Airflow responded_by_user, or "auto"
    at: datetime


class StageResult(BaseModel):
    stage: str
    status: Literal["ok", "skipped", "blocked"] = "ok"
    artifacts: list[str] = Field(default_factory=list)  # repo-relative paths written
    numbers: dict[str, float] = Field(default_factory=dict)
    cost_usd: float = 0.0
    duration_s: float = 0.0


class RunReport(BaseModel):
    run_id: str
    issue_id: str
    agent: AgentKind
    sandbox: str
    scm: str
    stages: list[StageResult]
    approvals: list[Approval]
    pr_url: str | None = None
    tests_passed: bool = False
    total_cost_usd: float = 0.0

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
