"""Blueprints: the unit of deployment. ``blueprints/<name>.toml`` = one DAG = one CLI line.

A blueprint is data: stage order, gates, limits, targets, sandbox profile, PR labels and additive
per-stage tool policy. Stage *semantics* stay code in ``swfactory.stages``. The DAG generator
shape-reads the TOML with ``tomllib`` at parse time; everything else goes through ``load`` and the
pydantic ``Blueprint`` model so the file is validated exactly once, in one place.
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from swfactory.agent import POLICIES
from swfactory.config import FACTORY_ROOT, Config

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
    ("blueprint", "trigger", "targets", "stages", "gates", "limits", "policy", "review")
    + ("sandbox", "deliver")
)
# ``blueprints/<name>.toml`` file names that map to a different ``blueprint.name``.
_FILE_ALIASES = {DEFAULT_BLUEPRINT: "default"}


class Target(BaseModel):
    """One repository the line operates on; jobs = issues x targets."""

    model_config = ConfigDict(extra="forbid")

    repo: str  # owner/name
    dir: str = ""  # subdir the factory operates on; "" = repo root
    base_branch: str = "main"


class GateSpec(BaseModel):
    """A human approval point after ``after``; ``artifact`` is shown to the approver."""

    model_config = ConfigDict(extra="forbid")

    after: str
    artifact: str
    timeout_h: int = Field(default=24, ge=1)
    assigned: list[str] = Field(default_factory=list)
    auto: bool = False  # True -> ApprovalOperator defaults="Approve" (actor "auto")


class Limits(BaseModel):
    """Every bounded loop and budget of one job (issue x target)."""

    model_config = ConfigDict(extra="forbid")

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


class ReviewSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    policy: str = "REVIEW.md"
    nit_cap: int = Field(default=3, ge=0)


class SandboxSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["local", "srt", "islo", "docker", "toolset"] = "islo"
    gateway_profile: str = "swfactory"
    environment: str = "swfactory"
    ttl_s: int = Field(default=172_800, ge=1)  # --delete-after
    idle_s: int = Field(default=900, ge=1)  # --pause-after-idle
    snapshot: str | None = None  # --snapshot warm start (islo only)


class Trigger(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["manual", "cron"] = "manual"
    cron: str | None = None

    @model_validator(mode="after")
    def _cron_present(self) -> Trigger:
        if self.kind == "cron" and not (self.cron or "").strip():
            raise ValueError("trigger.kind='cron' requires trigger.cron")
        return self


class Blueprint(BaseModel):
    """A validated ``blueprints/<name>.toml``."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=NAME_PATTERN)
    version: int = 1
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
        if self.sandbox.ttl_s <= self.gate_timeout_h * 3600:
            raise ValueError(
                f"sandbox.ttl_s ({self.sandbox.ttl_s}) must exceed the longest gate timeout "
                f"({self.gate_timeout_h} h)"
            )
        unknown = sorted(set(self.policy) - set(POLICIES))
        if unknown:
            raise ValueError(
                f"policy overrides for unknown stages {unknown}; known: {list(POLICIES)}"
            )
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

    def _check_gates(self) -> None:
        seen: set[str] = set()
        for g in self.gates:
            if g.after not in self.order:
                raise ValueError(f"gate after {g.after!r} is not in stages.order {self.order}")
            if g.after not in GATE_STAGES:
                raise ValueError(f"gates may only follow {list(GATE_STAGES)}, not {g.after!r}")
            if g.after in seen:
                raise ValueError(f"more than one gate after {g.after!r}")
            seen.add(g.after)

    # ------------------------------------------------------------ derived

    @property
    def gate_timeout_h(self) -> int:
        """Longest gate timeout (0 when the line has no gates); ``Config.gate_timeout_h``."""
        return max((g.timeout_h for g in self.gates), default=0)

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
                items.append(Gate(gate.after, gate.artifact, gate.auto))  # type: ignore[arg-type]
        return tuple(items)

    def jobs(self, conf: dict[str, Any] | None) -> list[dict[str, Any]]:
        """Fan-out of one DAG run: ``conf.issues`` (or ``conf.issue``) x targets.

        ``conf["targets"]`` (list of ``owner/name``) restricts the blueprint's targets.
        Result items: ``{"issue", "repo", "dir", "base_branch", "job_idx"}``.
        """
        conf = conf or {}
        # The UI/params form sends every param, so ``issues`` arrives as its default ``[]`` next
        # to a filled ``issue``: fall back on emptiness, not on absence.
        raw = conf.get("issues") or None
        if raw is None and conf.get("issue") not in (None, ""):
            raw = [conf["issue"]]
        issues = [str(i).strip() for i in (raw or []) if str(i).strip()]
        if not issues:
            raise ValueError('conf needs {"issues": [...]} (or {"issue": N})')
        targets = self.targets
        if conf.get("targets"):
            wanted = {str(t) for t in conf["targets"]}
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
        ``overrides`` (CLI flags; ``None`` values ignored). ``SWF_*`` env vars still win over both
        (``Config.settings_customise_sources`` orders env before init)."""
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
    out: dict[str, Any] = dict(data.get("blueprint", {}))
    for key in ("trigger", "targets", "gates", "limits", "policy", "review", "sandbox"):
        if key in data:
            out[key] = data[key]
    stages = data.get("stages", {})
    if "order" in stages:
        out["order"] = stages["order"]
    deliver = data.get("deliver", {})
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
    stems = [name_or_path, *filter(None, [_FILE_ALIASES.get(name_or_path)])]
    tried: list[Path] = []
    for root in (Path.cwd() / "blueprints", BLUEPRINTS_DIR):
        for stem in stems:
            path = root / f"{stem}.toml"
            if path.is_file():
                return path
            tried.append(path)
    raise FileNotFoundError(
        f"no blueprint {name_or_path!r}; tried " + ", ".join(str(p) for p in tried)
    )


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
