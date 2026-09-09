"""Blueprints: the unit of deployment. ``blueprints/<name>.toml`` = one DAG = one CLI line.

A blueprint is data: stage order, gates, limits, targets, sandbox profile, PR labels and additive
per-stage tool policy. Stage *semantics* stay code in ``swfactory.stages``. The DAG generator
shape-reads the TOML with ``tomllib`` at parse time; everything else goes through ``load`` and the
pydantic ``Blueprint`` model so the file is validated exactly once, in one place.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from swfactory.agent import POLICIES
from swfactory.approval_policy import GateMode, declared_mode
from swfactory.config import FACTORY_ROOT, Config
from swfactory.paths import (
    normalize_absolute_posix_path,
    normalize_relative_path,
    validate_git_ref,
    validate_repo,
)

if TYPE_CHECKING:
    from swfactory.stages import Gate, Stage

BLUEPRINTS_DIR = FACTORY_ROOT / "blueprints"
DEFAULT_BLUEPRINT = "factory"
CANONICAL_ORDER: tuple[str, ...] = ("intent", "spec", "plan", "build_and_test", "review", "deliver")
# Stages a human gate may follow: ``models.Approval.gate`` (approvals.json) is typed to these.
GATE_STAGES: tuple[str, ...] = ("intent", "plan")
NAME_PATTERN = r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$"
# TOML top-level tables the schema knows; anything else is a typo, not an extension point.
_SECTIONS = frozenset(
    ("blueprint", "trigger", "targets", "stages", "gates", "limits", "policy", "review") + ("sandbox", "deliver")
)
# ``blueprints/<name>.toml`` file names that map to a different ``blueprint.name``.
_FILE_ALIASES = {DEFAULT_BLUEPRINT: "default"}
# Airflow ``retries`` of the job tasks that retry (every other stage task gets 0). ``Blueprint.
# worst_case_s`` sizes the sandbox TTL from these; ``dags/blueprints.py`` carries its own copy
# because DAG parsing must not import swfactory, and ``tests/test_dag_parity.py`` pins the two.
TASK_RETRIES: dict[str, int] = {"setup": 2, "deliver": 2}
_EXTRA_TOOL_RE = re.compile(r"^(Read|Grep|Glob|Edit|Write|MultiEdit|NotebookEdit)(?:\([^,\r\n]*\))?$")


class Target(BaseModel):
    """One repository the line operates on; jobs = issues x targets."""

    model_config = ConfigDict(extra="forbid")

    repo: str  # owner/name
    dir: str = ""  # subdir the factory operates on; "" = repo root
    base_branch: str = "main"

    @field_validator("repo")
    @classmethod
    def _repo(cls, value: str) -> str:
        return validate_repo(value)

    @field_validator("dir")
    @classmethod
    def _directory(cls, value: str) -> str:
        return normalize_relative_path(value, field="targets.dir", allow_empty=True)

    @field_validator("base_branch")
    @classmethod
    def _branch(cls, value: str) -> str:
        return validate_git_ref(value, field="targets.base_branch")


class GateSpec(BaseModel):
    """An approval point after ``after``; ``artifact`` is shown to the approver.

    ``mode`` is the whole contract: ``"human"`` (the default) means an identified person must
    answer this gate, and nothing outside the blueprint may downgrade that — no environment
    variable, no missing response. ``"auto"`` is a deliberately unattended demo/stress line, where
    the ApprovalOperator's own default answers and the decision is recorded as automatic.
    """

    model_config = ConfigDict(extra="forbid")

    after: str
    artifact: str
    timeout_h: int = Field(default=24, ge=1)
    assigned: list[str] = Field(default_factory=list)
    mode: GateMode = "human"

    @model_validator(mode="before")
    @classmethod
    def _mode(cls, data: Any) -> Any:
        """Fold the legacy ``auto = true|false`` spelling into ``mode``, so a gate states its
        authority once. Two spellings that disagree are rejected rather than silently ranked."""
        if not isinstance(data, dict) or "auto" not in data:
            return data
        rest = {k: v for k, v in data.items() if k != "auto"}
        return {**rest, "mode": declared_mode(data)}

    @field_validator("artifact")
    @classmethod
    def _artifact(cls, value: str) -> str:
        return normalize_relative_path(value, field="gates.artifact")

    @property
    def auto(self) -> bool:
        """Read-only compatibility view of ``mode``; the declaration itself lives in ``mode``."""
        return self.mode == "auto"


class Limits(BaseModel):
    """Every bounded loop and budget of one job (issue x target)."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    max_build_iterations: int = Field(default=3, ge=1)
    max_review_fixes: int = Field(default=1, ge=0)
    max_turns: int = Field(default=40, ge=1)
    budget_usd_per_stage: float = Field(default=2.0, gt=0)
    budget_usd: float = Field(default=8.0, gt=0)  # per JOB, not per DAG run
    stage_timeout_h: int = Field(default=3, ge=1)
    max_parallel_jobs: int = Field(default=4, ge=1)


class PolicyOverride(BaseModel):
    """Additive per-stage tool policy: extra allowed tools and/or a model. Nothing else."""

    model_config = ConfigDict(extra="forbid")

    model: str | None = None
    extra_allowed_tools: list[str] = Field(default_factory=list)

    @field_validator("extra_allowed_tools")
    @classmethod
    def _tools(cls, value: list[str]) -> list[str]:
        tools = [tool.strip() for tool in value]
        if any(not _EXTRA_TOOL_RE.fullmatch(tool) for tool in tools):
            raise ValueError(
                "policy tools must be path-scoped file/search tools; shell, task, web, MCP, "
                "commas, and malformed matchers are forbidden"
            )
        return list(dict.fromkeys(tools))


class ReviewSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    policy: str = "REVIEW.md"
    nit_cap: int = Field(default=3, ge=0)

    @field_validator("policy")
    @classmethod
    def _policy(cls, value: str) -> str:
        return normalize_relative_path(value, field="review.policy")


class SandboxSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["local", "srt", "islo", "docker", "toolset"] = "islo"
    gateway_profile: str = "swfactory"
    environment: str = "swfactory"
    ttl_s: int = Field(default=172_800, ge=1)  # --delete-after
    idle_s: int = Field(default=900, ge=1)  # --pause-after-idle
    snapshot: str | None = None  # --snapshot warm start (islo only)
    backend: str = "sbx"  # Airflow SandboxBackend name or ``package.module:Class``
    workdir: str = "/workspace/repo"

    @field_validator("backend")
    @classmethod
    def _backend(cls, value: str) -> str:
        value = value.strip()
        if not value or any(ch.isspace() for ch in value):
            raise ValueError("sandbox.backend must be a name or package.module:Class")
        return value

    @field_validator("workdir")
    @classmethod
    def _workdir(cls, value: str) -> str:
        return normalize_absolute_posix_path(value, field="sandbox.workdir")


class Trigger(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["manual", "cron"] = "manual"
    cron: str | None = None
    issues: list[str] = Field(default_factory=list)

    @field_validator("issues")
    @classmethod
    def _issues(cls, values: list[str]) -> list[str]:
        issues: list[str] = []
        for item in values:
            value = item.strip()
            if not value:
                raise ValueError("trigger.issues entries must not be empty")
            issues.append(value if value.isdigit() else normalize_relative_path(value, field="trigger.issues"))
        return list(dict.fromkeys(issues))

    @model_validator(mode="after")
    def _cron_present(self) -> Trigger:
        if self.kind == "cron" and not (self.cron or "").strip():
            raise ValueError("trigger.kind='cron' requires trigger.cron")
        return self


class Blueprint(BaseModel):
    """A validated ``blueprints/<name>.toml``."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=NAME_PATTERN)
    version: Literal[1] = 1
    description: str = ""
    trigger: Trigger = Field(default_factory=Trigger)
    targets: list[Target] = Field(min_length=1)
    order: list[str]
    gates: list[GateSpec] = Field(default_factory=list)
    limits: Limits = Field(default_factory=Limits)
    policy: dict[str, PolicyOverride] = Field(default_factory=dict)
    review: ReviewSpec = Field(default_factory=ReviewSpec)
    sandbox: SandboxSpec = Field(default_factory=SandboxSpec)
    labels: list[str] = Field(default_factory=lambda: ["factory", "agent-authored"])

    # ------------------------------------------------------------ validation

    @model_validator(mode="after")
    def _shape(self) -> Blueprint:
        self._check_order()
        self._check_gates()
        if self.limits.budget_usd_per_stage > self.limits.budget_usd:
            raise ValueError("limits.budget_usd_per_stage must not exceed limits.budget_usd")
        if self.sandbox.ttl_s <= self.worst_case_s:
            raise ValueError(
                f"sandbox.ttl_s ({self.sandbox.ttl_s}) must exceed the line's worst case of {self.worst_case_s} s: "
                f"{sum(g.timeout_h for g in self.gates)} h of gates plus {self._stage_tries()} stage tries x "
                f"{self.limits.stage_timeout_h} h; a cell deleted mid-line loses every uncommitted change"
            )
        unknown = sorted(set(self.policy) - set(POLICIES))
        if unknown:
            raise ValueError(f"policy overrides for unknown stages {unknown}; known: {list(POLICIES)}")
        for stage, override in self.policy.items():
            unsafe = [
                tool
                for tool in override.extra_allowed_tools
                if tool.split("(", 1)[0] == "Bash"
                or (
                    not POLICIES[stage].writes
                    and tool.split("(", 1)[0] in {"Edit", "MultiEdit", "NotebookEdit", "Write"}
                )
            ]
            if unsafe:
                raise ValueError(f"policy.{stage} cannot add shell access or escalate a read-only stage: {unsafe}")
        return self

    def _check_order(self) -> None:
        if not self.order:
            raise ValueError("stages.order must not be empty")
        unknown = [s for s in self.order if s not in CANONICAL_ORDER]
        if unknown:
            raise ValueError(f"stages.order has unknown stages {unknown}; known: {CANONICAL_ORDER}")
        positions = [CANONICAL_ORDER.index(s) for s in self.order]
        if positions != sorted(set(positions)):
            raise ValueError(
                f"stages.order {self.order} must be a subsequence of {list(CANONICAL_ORDER)} "
                "(canonical order, no repeats)"
            )
        if self.order[0] != "intent":
            raise ValueError("stages.order must start with 'intent'")
        if self.order[-1] != "deliver":
            raise ValueError("stages.order must end with 'deliver'")
        needs_plan = any(stage in self.order for stage in ("build_and_test", "review"))
        if needs_plan and "plan" not in self.order:
            raise ValueError("stages.order needs 'plan' before build_and_test or review")
        if "review" in self.order and "build_and_test" not in self.order:
            raise ValueError("stages.order needs 'build_and_test' before review")

    def _check_gates(self) -> None:
        after = [gate.after for gate in self.gates]
        duplicate = next((stage for stage in after if after.count(stage) > 1), None)
        if duplicate is not None:
            raise ValueError(f"more than one gate after {duplicate!r}")
        stage_artifacts = {"intent": {"intent.md"}, "plan": {"plan.md", "plan.json"}}
        for g in self.gates:
            if g.after not in self.order:
                raise ValueError(f"gate after {g.after!r} is not in stages.order {self.order}")
            if g.after not in GATE_STAGES:
                raise ValueError(f"gates may only follow {list(GATE_STAGES)}, not {g.after!r}")
            if g.artifact not in stage_artifacts[g.after]:
                raise ValueError(
                    f"gate after {g.after!r} must show one of {sorted(stage_artifacts[g.after])}, not {g.artifact!r}"
                )

    # ------------------------------------------------------------ derived

    @property
    def gate_timeout_h(self) -> int:
        """Longest gate timeout (0 when the line has no gates); ``Config.gate_timeout_h``."""
        return max((g.timeout_h for g in self.gates), default=0)

    def _stage_tries(self) -> int:
        """Airflow tries a job can spend in stage tasks: setup and every stage, retries included."""
        return sum(1 + TASK_RETRIES.get(task, 0) for task in ("setup", *self.order))

    @property
    def worst_case_s(self) -> int:
        """Longest a job can keep its cell: every gate waiting to its timeout, plus every stage
        task (setup included) exhausting ``stage_timeout_h`` on each of its tries.

        The cell's ``--delete-after`` clock starts at setup and never pauses, so THIS is what
        ``sandbox.ttl_s`` has to outlive -- not the longest single gate, which is what the floor
        used to be: ``liquid.toml`` carried 24 h over 16 h of gates and 66 h of stage tries and
        loaded fine. Scheduler latency and ``retry_delay`` are not modelled; leave headroom.
        """
        return 3600 * (sum(g.timeout_h for g in self.gates) + self.limits.stage_timeout_h * self._stage_tries())

    def gate_after(self, stage: str) -> GateSpec | None:
        """The gate following ``stage``, if any."""
        return next((g for g in self.gates if g.after == stage), None)

    def pipeline(self) -> tuple[Stage | Gate, ...]:
        """``STAGES[s]`` for every stage in order, each gate inserted right after its stage."""
        from swfactory.stages import STAGES, Gate

        items: list[Stage | Gate] = []
        for name in self.order:
            items.append(STAGES[name])
            gate = self.gate_after(name)
            if gate is not None:
                items.append(Gate(gate.after, gate.artifact, gate.mode))  # type: ignore[arg-type]
        return tuple(items)

    def jobs(self, conf: dict[str, Any] | None) -> list[dict[str, Any]]:
        """Fan-out of one DAG run: runtime issues, else ``trigger.issues``, x targets.

        ``conf["targets"]`` (list of ``owner/name``) restricts the blueprint's targets.
        Result items: ``{"issue", "repo", "dir", "base_branch", "job_idx"}``.
        """
        conf = conf or {}
        # The UI/params form sends every param, so ``issues`` arrives as its default ``[]`` next
        # to a filled ``issue``: fall back on emptiness, not on absence.
        raw = conf.get("issues") or None
        if raw is None and conf.get("issue") not in (None, ""):
            raw = [conf["issue"]]
        if raw is None:
            raw = self.trigger.issues
        if raw is not None and not isinstance(raw, list | tuple):
            raw = [raw]
        issues: list[str] = []
        for item in raw or []:
            value = str(item).strip()
            if not value:
                continue
            issues.append(value if value.isdigit() else normalize_relative_path(value, field="conf.issues"))
        issues = list(dict.fromkeys(issues))
        if not issues:
            raise ValueError('run needs conf {"issues": [...]} (or {"issue": N}), or trigger.issues')
        targets = self.targets
        if conf.get("targets"):
            selected = conf["targets"]
            if not isinstance(selected, list | tuple | set):
                selected = [selected]
            wanted = {str(t) for t in selected}
            targets = [t for t in self.targets if t.repo in wanted]
            missing = wanted - {t.repo for t in targets}
            if missing:
                raise ValueError(f"conf.targets {sorted(missing)} not in blueprint {self.name!r}")
        return [
            {
                "issue": issue,
                "repo": t.repo,
                "dir": t.dir,
                "base_branch": t.base_branch,
                "job_idx": idx,
            }
            for idx, (issue, t) in enumerate((i, t) for i in issues for t in targets)
        ]

    def config(self, job: dict[str, Any], *, run_id: str, **overrides: Any) -> Config:
        """Runtime ``Config`` for one job: limits/sandbox/target mapped onto Config fields, then
        ``overrides`` (CLI flags; ``None`` values ignored). Operational ``SWF_*`` settings still
        win; job identity is explicit and cannot come from ambient worker state."""
        values: dict[str, Any] = {
            "issue": str(job["issue"]),
            "repo": job.get("repo", self.targets[0].repo),
            "target_dir": job.get("dir", self.targets[0].dir),
            "base_branch": job.get("base_branch", self.targets[0].base_branch),
            "run_id": run_id,
            "blueprint": self.name,
            "sandbox": self.sandbox.kind,
            "gateway_profile": self.sandbox.gateway_profile,
            "islo_environment": self.sandbox.environment,
            "sandbox_ttl_s": self.sandbox.ttl_s,
            "sandbox_idle_s": self.sandbox.idle_s,
            "islo_snapshot": self.sandbox.snapshot,
            "toolset_backend": self.sandbox.backend,
            "toolset_workdir": self.sandbox.workdir,
            "max_build_iterations": self.limits.max_build_iterations,
            "max_review_fixes": self.limits.max_review_fixes,
            "max_turns": self.limits.max_turns,
            "max_budget_usd_per_stage": self.limits.budget_usd_per_stage,
            "max_budget_usd": self.limits.budget_usd,
            "stage_timeout_h": self.limits.stage_timeout_h,
            "max_parallel_jobs": self.limits.max_parallel_jobs,
            "gate_timeout_h": self.gate_timeout_h,
        }
        values.update({k: v for k, v in overrides.items() if v is not None})
        return Config(**values)


# ---------------------------------------------------------------- loading


def loads(text: str) -> Blueprint:
    """Parse and validate blueprint TOML text."""
    data = tomllib.loads(text)
    unknown = sorted(set(data) - _SECTIONS)
    if unknown:
        raise ValueError(f"unknown blueprint sections {unknown}; known: {sorted(_SECTIONS)}")
    return Blueprint.model_validate(_flatten(data))


def _flatten(data: dict[str, Any]) -> dict[str, Any]:
    """TOML tables -> ``Blueprint`` fields (``[blueprint]`` is inlined, ``[stages].order`` and
    ``[deliver].labels`` are lifted)."""
    blueprint = data.get("blueprint", {})
    if not isinstance(blueprint, dict):
        raise ValueError("[blueprint] must be a table")
    out: dict[str, Any] = dict(blueprint)
    for key in ("trigger", "targets", "gates", "limits", "policy", "review", "sandbox"):
        if key in data:
            out[key] = data[key]
    stages = data.get("stages", {})
    if not isinstance(stages, dict):
        raise ValueError("[stages] must be a table")
    unknown_stages = sorted(set(stages) - {"order"})
    if unknown_stages:
        raise ValueError(f"unknown [stages] keys {unknown_stages}; known: ['order']")
    if "order" in stages:
        out["order"] = stages["order"]
    deliver = data.get("deliver", {})
    if not isinstance(deliver, dict):
        raise ValueError("[deliver] must be a table")
    unknown_deliver = sorted(set(deliver) - {"labels"})
    if unknown_deliver:
        raise ValueError(f"unknown [deliver] keys {unknown_deliver}; known: ['labels']")
    if "labels" in deliver:
        out["labels"] = deliver["labels"]
    return out


def resolve(name_or_path: str) -> Path:
    """``blueprints/<name>.toml`` relative to cwd, then to the factory root; or a ``.toml`` path.

    ``factory`` (the default blueprint's name) lives in ``blueprints/default.toml``.
    """
    candidate = Path(name_or_path)
    if candidate.suffix == ".toml":
        if candidate.is_file():
            return candidate
        raise FileNotFoundError(f"blueprint file not found: {name_or_path}")
    safe_name_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
    if not name_or_path or any(ch not in safe_name_chars for ch in name_or_path):
        raise ValueError(f"invalid blueprint name: {name_or_path!r}")
    stems = [name_or_path, *filter(None, [_FILE_ALIASES.get(name_or_path)])]
    tried: list[Path] = []
    for root in (Path.cwd() / "blueprints", BLUEPRINTS_DIR):
        for stem in stems:
            path = root / f"{stem}.toml"
            if path.is_file():
                return path
            tried.append(path)
    raise FileNotFoundError(f"no blueprint {name_or_path!r}; tried " + ", ".join(str(p) for p in tried))


def load(name_or_path: str = DEFAULT_BLUEPRINT) -> Blueprint:
    """Load ``blueprints/<name>.toml`` (or a ``.toml`` path) and validate it. A name must match
    the file's ``blueprint.name`` (the DAG id)."""
    path = resolve(name_or_path)
    try:
        bp = loads(path.read_text(encoding="utf-8"))
    except (ValueError, tomllib.TOMLDecodeError) as e:
        raise ValueError(f"{path}: {e}") from e
    if Path(name_or_path).suffix != ".toml" and bp.name != name_or_path:
        raise ValueError(f"{path}: blueprint.name is {bp.name!r}, expected {name_or_path!r}")
    return bp


def blueprint_paths(root: Path | None = None) -> list[Path]:
    """Every ``*.toml`` under ``blueprints/`` (the factory root's by default), sorted."""
    return sorted((root or BLUEPRINTS_DIR).glob("*.toml"))
