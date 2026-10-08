"""Checked-in authority for unattended work, never agent-selected permissions."""

from __future__ import annotations

import fnmatch
import hashlib
import json
import sqlite3
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator

from swfactory.config import FACTORY_ROOT
from swfactory.models import Approval, BoundaryModel, Issue, StageError
from swfactory.paths import normalize_relative_path

LINE = "autonomous"


class AutonomousPolicy(BoundaryModel):
    schema_version: Literal[1]
    enabled: bool
    repository: str
    base_branch: str
    required_labels: list[str] = Field(min_length=1)
    denied_labels: list[str]
    allowed_paths: list[str] = Field(min_length=1)
    protected_paths: list[str]
    budget_usd: float = Field(gt=0)
    required_checks: list[str] = Field(min_length=1)
    checks_app_id: int = Field(gt=0)
    max_files: int = Field(gt=0, le=100)
    merge_timeout_s: int = Field(gt=0, le=86400)
    revision: str = ""

    @field_validator("allowed_paths", "protected_paths")
    @classmethod
    def safe_patterns(cls, values: list[str]) -> list[str]:
        for value in values:
            normalize_relative_path(value.rstrip("/"), field="policy path")
        return values

    def issue_reason(self, issue: Issue, repository: str) -> str | None:
        if not self.enabled:
            return "policy_disabled"
        if repository != self.repository:
            return "repository_outside_policy"
        if issue.state != "open":
            return "issue_not_open"
        if not set(self.required_labels) <= set(issue.labels):
            return "required_labels_missing"
        if set(self.denied_labels) & set(issue.labels):
            return "denied_label"
        if not issue.title.strip() or not issue.body.strip():
            return "missing_intent"
        return None

    def check_paths(self, paths: list[str], *, artifact_prefix: str | None = None) -> None:
        if not paths or len(set(paths)) > self.max_files:
            raise StageError("policy", "empty or oversized autonomous change")
        for path in paths:
            normalized = normalize_relative_path(path, field="autonomous change path")
            if normalized != path:
                raise StageError("policy", "autonomous paths must be canonical")
            # Only the host-generated chain for this issue may be included in publication.
            if artifact_prefix and path.startswith(artifact_prefix + "/"):
                continue
            if any(_matches(path, pattern) for pattern in self.protected_paths):
                raise StageError("policy", f"protected path: {path}")
            if not any(_matches(path, pattern) for pattern in self.allowed_paths):
                raise StageError("policy", f"path outside autonomous policy: {path}")

    def check_budget(self, cost: float, ceiling: float) -> None:
        if not 0 <= cost <= self.budget_usd or not 0 < ceiling <= self.budget_usd:
            raise StageError("policy", "autonomous budget exceeds checked-in authority")


def _matches(path: str, pattern: str) -> bool:
    return (
        path == pattern.rstrip("/") or path.startswith(pattern.rstrip("/") + "/") or fnmatch.fnmatchcase(path, pattern)
    )


def load_policy(root: Path = FACTORY_ROOT) -> AutonomousPolicy:
    raw = (root / "config/autonomous.toml").read_bytes()
    document = tomllib.loads(raw.decode())
    # A policy revision binds the limits, protection contract, review skill and line itself.
    contract = tomllib.loads((root / "factory.toml").read_text())
    protected = contract["paths"]["protected"]
    authority = [raw, json.dumps(protected, sort_keys=True).encode()]
    authority.extend((root / name).read_bytes() for name in ("REVIEW.md", "blueprints/autonomous.toml"))
    document["protected_paths"] = list(dict.fromkeys(document["protected_paths"] + protected))
    document["revision"] = hashlib.sha256(b"\0".join(authority)).hexdigest()
    return AutonomousPolicy.model_validate(document)


class AutonomyStore:
    """Immutable decisions per Cell epoch, with durable blocked triage outcomes."""

    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute("CREATE TABLE IF NOT EXISTS decisions (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS decision_times (key TEXT PRIMARY KEY, created_at TEXT NOT NULL)")

    def connect(self):
        return sqlite3.connect(self.path, timeout=30)

    def bind(self, key: str, value: dict) -> dict:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"))
        with self.connect() as db:
            inserted = db.execute("INSERT OR IGNORE INTO decisions VALUES (?, ?)", (key, encoded))
            if inserted.rowcount:
                db.execute(
                    "INSERT OR IGNORE INTO decision_times VALUES (?, ?)",
                    (key, datetime.now(UTC).isoformat(timespec="milliseconds")),
                )
            previous = db.execute("SELECT value FROM decisions WHERE key = ?", (key,)).fetchone()[0]
        if previous != encoded:
            raise StageError("policy", "autonomous decision changed at the same Cell epoch")
        return value

    def get(self, key: str) -> dict | None:
        with self.connect() as db:
            row = db.execute("SELECT value FROM decisions WHERE key = ?", (key,)).fetchone()
        return json.loads(row[0]) if row else None


def record_merge_timing(ctx, result: dict | None = None) -> None:
    """Host-only observational timing; never an input to a gate or managed mutation."""
    name = "merge-timing.json"
    now = datetime.now(UTC)
    prior = json.loads(ctx.state.read_control(name)) if ctx.state.has_control(name) else {}
    if prior.get("state") == "merged":
        return
    data = {
        "started_at": prior.get("started_at", now.isoformat()),
        "observed_at": now.isoformat(),
        "state": result.get("state", "checking") if result else "checking",
        "ci_checks_wait_s": prior.get("ci_checks_wait_s", 0.0),
    }
    if prior.get("state") == "pending" and "check" in prior.get("reason", ""):
        data["ci_checks_wait_s"] += max(0.0, (now - datetime.fromisoformat(prior["observed_at"])).total_seconds())
    if result:
        data["reason"] = result.get("reason", "")
    if data["state"] == "merged":
        data["finished_at"] = now.isoformat()
    ctx.state.write_control(name, json.dumps(data, sort_keys=True) + "\n")


def gate_key(cell_id: str, epoch: int, gate: str) -> str:
    return f"{cell_id}:{epoch}:gate:{gate}"


def policy_approval(ctx, gate: str) -> Approval:
    """Ask the backend to approve current host-owned evidence, never a model's yes/no."""
    from swfactory import accepted_inputs
    from swfactory.backend_scm import BackendScm
    from swfactory.models import Plan
    from swfactory.stages import _artifact_sha256, _gate_artifact, cell_evidence, persisted_cost

    if not isinstance(ctx.scm, BackendScm) or not cell_evidence(ctx)[2]:
        raise StageError("policy", "policy gates require a backend-managed Cell")
    policy = load_policy()
    artifact_sha256 = _artifact_sha256(ctx, _gate_artifact(ctx, gate))
    plan = Plan.model_validate_json(ctx.read_artifact(f"{ctx.art}/plan.json")) if gate == "plan" else None
    result = ctx.scm.autonomous_gate(
        gate=gate,
        revision=policy.revision,
        artifact_sha256=artifact_sha256,
        inputs_digest=accepted_inputs.require(ctx.state).digest,
        paths=plan.files if plan else [],
        plan_sha256=_artifact_sha256(ctx, f"{ctx.art}/plan.json") if plan else None,
        cost_usd=persisted_cost(ctx),
        budget_usd=ctx.cfg.max_budget_usd,
    )
    return Approval.model_validate(result)


def enforce_runtime_policy(cfg, blueprint, binding: dict | None) -> None:
    """Refuse widened worker settings before creating a sandbox or spending on the first stage."""
    if not any(gate.mode == "policy" for gate in blueprint.gates):
        return
    from swfactory.security_contract import CanonicalPolicy

    policy = load_policy()
    if blueprint.name != LINE or not binding or not binding.get("managed") or cfg.scm != "github":
        raise StageError("policy", "autonomous work requires a backend-managed line")
    if not policy.enabled or cfg.repo != policy.repository or cfg.target_dir or cfg.base_branch != policy.base_branch:
        raise StageError("policy", "runtime target is outside autonomous policy")
    policy.check_budget(0, cfg.max_budget_usd)
    if not 0 < cfg.max_budget_usd_per_stage <= cfg.max_budget_usd:
        raise StageError("policy", "stage budget exceeds autonomous job authority")
    expected = CanonicalPolicy.for_factory_job(
        LINE, {"repo": cfg.repo, "dir": cfg.target_dir, "base_branch": cfg.base_branch}
    ).digest()
    if binding.get("policy_digest") != expected:
        raise StageError("policy", "autonomous policy moved before execution; a new epoch is required")
