"""Typed data that crosses stage boundaries. Prose artifacts (intent/spec) stay markdown."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, NamedTuple

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from swfactory.paths import validate_identifier, validate_run_id

Severity = Literal["blocker", "major", "minor", "nit"]
AgentKind = Literal["claude", "scripted"]


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


class Plan(BoundaryModel):
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
