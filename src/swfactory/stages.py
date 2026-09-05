"""The SDLC stages. ``STAGES`` (keyed by ``CANONICAL_ORDER``) is the registry a blueprint walks;
``PIPELINE`` is the default blueprint's walk and drives ``swfactory run`` and the Airflow DAG.

Every stage is a function ``Ctx -> StageResult`` that is idempotent: when the orchestrator's stage
log (``<run_dir>/state/stages.jsonl``, written by ``_timed`` on the orchestrator filesystem)
already holds a completed record for it, it returns ``status="skipped"``. Skip decisions never
read the sandbox: generated code and the target's test command execute there, so a compromised
checkout must not be able to skip a stage. The run-level budget is seeded from the same host log.
Loops (build/fix, review/fix) live *inside* stage functions and are bounded by ``Config``. The
agent never commits: stages commit with the bot identity and provenance trailers, and
``deliver`` hands the commits to the Scm as a patch stream. A blueprint (``Ctx.blueprint``) only
changes the walk order and knobs (tool policy additions, nit cap, PR labels); with
``blueprint=None`` every stage behaves exactly as v1.

Agent calls are named ``<stage>.<iteration>`` (envelopes ``{art}/agent/<stage>.<iteration>.json``,
scripted fixtures ``<stage>.<iteration>.{patch|json|md}``): ``build.1`` then ``fix.2`` ..
``fix.<max_build_iterations>`` for the build loop, ``review.k`` for the k-th review, and review
fixes continue the ``fix`` numbering at ``fix.<max_build_iterations + k>`` (``fix.4`` for the
first review fix with the default of 3 build iterations) so no envelope or fixture name is reused.
"""

from __future__ import annotations

import contextlib
import dataclasses
import functools
import getpass
import hashlib
import json
import shlex
import shutil
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NamedTuple

import typer
import yaml
from pydantic import BaseModel, ValidationError

from swfactory import metrics as metrics_mod
from swfactory.agent import POLICIES, Agent, Policy, render_prompt
from swfactory.config import (
    FACTORY_ROOT,
    Config,
    TargetContract,
    load_target_contract,
    protected_for,
)
from swfactory.models import (
    AgentResult,
    Approval,
    BuildSummary,
    Finding,
    Issue,
    Plan,
    Review,
    RunReport,
    StageError,
    StageResult,
    TestResult,
)
from swfactory.sandbox import LocalSandbox, Sandbox
from swfactory.scm import BOT_EMAIL, BOT_NAME, Scm
from swfactory.state import RunState

if TYPE_CHECKING:
    from swfactory.blueprint import Blueprint

CANONICAL_ORDER: tuple[str, ...] = ("intent", "spec", "plan", "build_and_test", "review", "deliver")
NIT_CAP = 3  # REVIEW.md: at most 3 nits per review (blueprint.review.nit_cap overrides)
DEFAULT_LABELS: tuple[str, ...] = ("factory", "agent-authored")  # blueprint.labels overrides
_GIT_BOT = f"git -c user.name={shlex.quote(BOT_NAME)} -c user.email={shlex.quote(BOT_EMAIL)}"
PREVIEW_CHARS = 4000  # StageResult.preview: head of the gate artifact shown to the approver
# crabbox providers that run in place: no file download step exists (or is needed) for junit.
IN_PLACE_PROVIDERS = frozenset(
    {"srt", "anthropic-sandbox-runtime", "docker-sandbox", "apple-machine"}
)
BASE_FILE = ".factory/base"
STARTED_FILE = ".factory/started"
STAGES_LOG = ".factory/stages.jsonl"  # sandbox COPY of the stage log (audit trail, never trusted)
RUN_STAGES_LOG = "stages.jsonl"  # authoritative log: <run_dir>/state/stages.jsonl (orchestrator)
HOOKS_LOG = ".factory/hooks.jsonl"  # swf_guard.py decisions, appended by the hook in the sandbox
SEVERITIES: tuple[str, ...] = ("blocker", "major", "minor", "nit")
# `git add -A` that skips special files: the Anthropic Sandbox Runtime on Linux binds /dev/null
# style stubs over shell/git rc names in the cwd, which git refuses to stage.
GIT_ADD_ALL = (
    "git ls-files -z -o -m --exclude-standard | while IFS= read -r -d '' f; do "
    '[ -f "$f" ] || [ -L "$f" ] || [ -d "$f" ] && printf "%s\\0" "$f"; done '
    "| xargs -0 -r git add -- 2>/dev/null; git add -u -- . 2>/dev/null || true"
)

# Scratch the factory never commits, added to .git/info/exclude by setup (the target may lack a
# .gitignore, as the demo copy does), plus the shell-rc stubs the Anthropic Sandbox Runtime
# (Linux) binds into the cwd as special files.
NEVER_COMMITTED = (
    ".factory/",
    ".venv/",
    "__pycache__/",
    "*.pyc",
    ".pytest_cache/",
    ".ruff_cache/",
    ".bash_profile",
    ".bashrc",
    ".profile",
    ".gitconfig",
    ".npmrc",
    ".zshrc",
    ".inputrc",
)


# ---------------------------------------------------------------- context


@dataclass
class Ctx:
    """Everything a stage needs. Built once per run by the CLI, or per task by the DAG."""

    cfg: Config
    sb: Sandbox
    agent: Agent
    scm: Scm
    issue: Issue
    run_dir: Path
    blueprint: Blueprint | None = None  # None => v1 defaults (PIPELINE, NIT_CAP, DEFAULT_LABELS)
    contract: TargetContract | None = None  # loaded by setup(); lazily on demand otherwise
    stages: list[StageResult] = field(default_factory=list)  # accumulated by run_pipeline
    spent_usd: float = 0.0  # run guard; seeded from <run_dir>/state/stages.jsonl by _agent
    budget_seeded: bool = False  # spent_usd includes earlier tasks/processes of this run
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def art(self) -> str:
        """Committed artifact dir, relative to the target dir: ``docs/factory/<issue_id>``."""
        return Config.artifacts_dir(self.issue.id)

    @property
    def branch(self) -> str:
        """Branch the sandbox works on and the PR is opened from."""
        return f"factory/{self.issue.id}-{self.cfg.run_id}"

    @property
    def state(self) -> RunState:
        """Orchestrator-owned state for this run; never writable by the agent."""

        return RunState(self.run_dir)

    def write_artifact(self, path: str, content: str) -> None:
        """Persist an authoritative artifact, then mirror it into the delivery checkout."""

        self.state.write_artifact(path, content)
        self.sb.write(path, content)

    def read_artifact(self, path: str) -> str:
        return self.state.read_artifact(path)


class Gate(NamedTuple):
    """A human approval point; ``artifact`` is shown to the approver. ``auto`` (blueprint
    ``gates[].auto``) approves without asking, in the CLI and the DAG alike (actor "auto")."""

    name: Literal["intent", "plan"]
    artifact: str
    auto: bool = False


Stage = Callable[[Ctx], StageResult]
Approver = Callable[[Gate, Ctx], Approval]


# ---------------------------------------------------------------- helpers


def _contract(ctx: Ctx) -> TargetContract:
    if ctx.contract is None:
        if ctx.state.has_control("target-contract.json"):
            try:
                ctx.contract = TargetContract.model_validate_json(
                    ctx.state.read_control("target-contract.json")
                )
            except (OSError, ValueError) as e:
                raise StageError("policy", f"stored target contract is invalid: {e}") from e
        else:
            try:
                ctx.contract = load_target_contract(ctx.sb)
            except ValueError as e:
                raise StageError("policy", str(e)) from e
            ctx.state.write_control(
                "target-contract.json", ctx.contract.model_dump_json(indent=2) + "\n"
            )
    return ctx.contract


def _sh(ctx: Ctx, cmd: str, *, timeout_s: int = 600) -> str:
    """Run a command that must succeed; stdout is returned, failure is a sandbox StageError."""
    res = ctx.sb.run(cmd, timeout_s=timeout_s)
    if not res.ok:
        raise StageError(
            "sandbox", f"`{cmd}` failed (rc={res.exit_code}): {res.stderr.strip()[-800:]}"
        )
    return res.stdout


def _read_or(ctx: Ctx, path: str, default: str = "") -> str:
    if path.startswith(f"{ctx.art}/"):
        try:
            return ctx.read_artifact(path)
        except FileNotFoundError:
            return default
    try:
        return ctx.sb.read(path)
    except FileNotFoundError:
        return default


def _read_json(ctx: Ctx, path: str, default: object) -> object:
    """A JSON artifact from the sandbox, or ``default`` when it is absent, empty or not JSON.

    The sandbox is untrusted provider state, so a forged or truncated artifact must degrade to the
    default instead of raising a bare ``JSONDecodeError`` out of a stage: every caller already
    type-checks what comes back.
    """
    text = _read_or(ctx, path)
    if not text.strip():
        return default
    try:
        return json.loads(text)
    except ValueError:
        return default


def _dumps(obj: object) -> str:
    return json.dumps(obj, indent=2) + "\n"


def _exclude(ctx: Ctx) -> str:
    """Pathspec that keeps the artifact chain out of the reviewed diff."""
    return shlex.quote(f":!{ctx.art}")


def _skipped(prior: StageResult) -> StageResult:
    """The idempotent re-run of a stage: the prior record's artifacts/numbers/preview, no cost."""
    return prior.model_copy(update={"status": "skipped", "duration_s": 0.0, "cost_usd": 0.0})


def _persisted(ctx: Ctx) -> list[StageResult]:
    """Every record of ``<run_dir>/state/stages.jsonl``, the host log ``_timed`` appends to.

    This file is the ONLY evidence a stage may be skipped on: the sandbox is agent-writable.
    """
    try:
        return [
            StageResult.model_validate(record)
            for record in ctx.state.read_jsonl(RUN_STAGES_LOG)
        ]
    except (OSError, ValueError) as e:
        raise StageError("policy", f"run journal is corrupt: {e}") from e


def _done(ctx: Ctx, stage: str) -> StageResult | None:
    """The latest completed (non-skipped) record of ``stage`` in this run, if any."""
    recs = [r for r in _persisted(ctx) if r.stage == stage and r.status != "skipped"]
    latest = recs[-1] if recs else None
    return latest if latest is not None and latest.status in {"ok", "blocked"} else None


def _nit_cap(ctx: Ctx) -> int:
    return ctx.blueprint.review.nit_cap if ctx.blueprint else NIT_CAP


def _pipeline(ctx: Ctx) -> tuple[Stage | Gate, ...]:
    return ctx.blueprint.pipeline() if ctx.blueprint else PIPELINE


def _timed(fn: Stage) -> Stage:
    """Fill ``duration_s``/``cost_usd`` and persist the result: authoritatively to
    ``<run_dir>/state/stages.jsonl`` on the orchestrator, plus a copy in the sandbox
    (``STAGES_LOG``) that ``deliver`` carries into the committed artifact chain."""

    @functools.wraps(fn)
    def wrapper(ctx: Ctx) -> StageResult:
        # Seed before taking the delta. On a fresh Airflow task, seeding inside the first agent
        # call would otherwise attribute every earlier task's cost to this stage a second time.
        seed_budget(ctx)
        t0, spent0 = time.monotonic(), ctx.spent_usd
        try:
            result = fn(ctx)
        except Exception as error:
            failed = StageResult(
                stage=fn.__name__,
                status="failed",
                duration_s=round(time.monotonic() - t0, 3),
                cost_usd=round(max(ctx.spent_usd - spent0, 0.0), 6),
                error=str(error)[:2000],
            )
            _append_stage(ctx, failed)
            raise
        result.duration_s = round(time.monotonic() - t0, 3)
        result.cost_usd = round(ctx.spent_usd - spent0, 6)
        _append_stage(ctx, result)
        return result

    return wrapper


def _append_stage(ctx: Ctx, result: StageResult) -> None:
    line = ctx.state.append_json(RUN_STAGES_LOG, result.model_dump(mode="json"))
    # The host journal is authoritative. A dead cell must not turn completed work into a failure;
    # delivery rebuilds its mirror when a cell is available again.
    with contextlib.suppress(Exception):
        ctx.sb.write(STAGES_LOG, _read_or(ctx, STAGES_LOG) + line)


def load_stage_results(ctx: Ctx) -> list[StageResult]:
    """Stage results of this run so far, from ``<run_dir>/state/stages.jsonl``
    (DAG tasks are separate processes; in-process results are already in the log). Per stage
    the latest non-skipped record wins, in canonical order."""
    latest: dict[str, StageResult] = {}
    for rec in _persisted(ctx):
        prev = latest.get(rec.stage)
        if prev is None or rec.status != "skipped" or prev.status == "skipped":
            latest[rec.stage] = rec
    order = CANONICAL_ORDER  # unknown stages (a custom blueprint) sort first
    return sorted(latest.values(), key=lambda r: order.index(r.stage) if r.stage in order else -1)


def seed_budget(ctx: Ctx) -> float:
    """Once per process: add what earlier tasks/processes of this run spent (every record in
    ``<run_dir>/state/stages.jsonl``) to ``ctx.spent_usd`` so ``Config.max_budget_usd`` is a RUN
    ceiling under the DAG too, not a per-task one. Returns the seeded total."""
    if not ctx.budget_seeded:
        ctx.spent_usd += sum(r.cost_usd for r in _persisted(ctx))
        ctx.budget_seeded = True
    return ctx.spent_usd


def _agent(
    ctx: Ctx, stage: str, iteration: int, prompt: str, schema: type[BaseModel] | None
) -> AgentResult:
    """Run the agent for one stage call, enforcing the run-level budget and surfacing errors.
    Under srt the kernel ``denyWrite`` set follows the stage (``protected_for``): ``fix`` calls
    lose write access to the tests dir that ``build`` needed, matching the ``Edit(...)`` rules."""
    seed_budget(ctx)
    protected = protected_for(_contract(ctx), stage)
    if hasattr(ctx.sb, "set_protected"):
        ctx.sb.set_protected(protected)
    res = ctx.agent.run(
        ctx.sb,
        stage=stage,
        iteration=iteration,
        prompt=prompt,
        policy=_policy(ctx, stage),
        schema=schema,
        cfg=ctx.cfg,
        issue_id=ctx.issue.id,
        protected=protected,
    )
    envelope = f"{ctx.art}/agent/{stage}.{iteration}.json"
    if ctx.sb.exists(envelope):
        ctx.state.write_artifact(envelope, ctx.sb.read(envelope))
    ctx.spent_usd += res.cost_usd
    if ctx.spent_usd > ctx.cfg.max_budget_usd:
        raise StageError(
            "policy",
            f"run budget exceeded: {ctx.spent_usd:.2f} > {ctx.cfg.max_budget_usd:.2f} USD "
            f"after {stage}.{iteration}",
        )
    if res.is_error:
        raise StageError("agent", f"{stage}.{iteration} failed: {res.subtype}: {res.text[-800:]}")
    if schema is not None and res.data is None:
        raise StageError("agent", f"{stage}.{iteration} returned no structured output")
    return res


def _policy(ctx: Ctx, stage: str) -> Policy:
    """``POLICIES[stage]`` plus the blueprint's additive override (extra allowed tools, model).
    ``disallowed_tools`` and ``writes`` cannot be changed from a blueprint."""
    policy = POLICIES[stage]
    override = ctx.blueprint.policy.get(stage) if ctx.blueprint else None
    if override is None:
        return policy
    extra = tuple(t for t in override.extra_allowed_tools if t not in policy.allowed_tools)
    return dataclasses.replace(
        policy, allowed_tools=policy.allowed_tools + extra, model=override.model or policy.model
    )


def _protected(ctx: Ctx, stage: str) -> str:
    return ", ".join(protected_for(_contract(ctx), stage)) or "(none)"


def _workspace_head(ctx: Ctx) -> str:
    return _sh(ctx, "git rev-parse HEAD").strip()


def _record_workspace_head(ctx: Ctx, sha: str) -> None:
    ctx.state.write_control("workspace-head", sha + "\n")


def _assert_workspace_head(ctx: Ctx, phase: str) -> str:
    """Refuse a resumed or hostile cell whose checked-out commit escaped host-owned state."""

    if not ctx.state.has_control("workspace-head"):
        raise StageError("policy", "run state has no workspace head; setup must complete first")
    expected = ctx.state.read_control("workspace-head").strip()
    actual = _workspace_head(ctx)
    if actual != expected:
        raise StageError(
            "policy",
            f"{phase} workspace drifted from recorded HEAD {expected} to {actual}",
        )
    return actual


def commit(
    ctx: Ctx, *, stage: str, msg: str, paths: Sequence[str] | None = None
) -> str:
    """Commit everything under the target dir as the bot with provenance trailers (``.factory``
    is excluded by setup). Returns the HEAD sha; a no-op when there is nothing to commit."""
    _assert_workspace_head(ctx, f"{stage} commit")
    if paths:
        _sh(ctx, "git add -A -- " + " ".join(shlex.quote(path) for path in paths))
    else:
        _sh(ctx, GIT_ADD_ALL)
        # Intent, gates and agent receipts are committed once by deliver, after the review stream.
        _sh(ctx, f"git reset -q -- {shlex.quote(ctx.art)}")
    if ctx.sb.run("git diff --cached --quiet").ok:
        sha = _workspace_head(ctx)
        _record_workspace_head(ctx, sha)
        return sha
    q = shlex.quote
    cmd = (
        f"{_GIT_BOT} -c commit.gpgsign=false "
        f"commit -q -m {q(msg)} "
        f"--trailer {q(f'Factory-Run={ctx.cfg.run_id}')} "
        f"--trailer {q(f'Factory-Stage={stage}')} "
        f"--trailer {q(f'Agent={ctx.agent.kind}')} "
        f"--trailer {q('Co-Authored-By: Claude <noreply@anthropic.com>')}"
    )
    _sh(ctx, cmd)
    sha = _workspace_head(ctx)
    _record_workspace_head(ctx, sha)
    return sha


def run_tests(ctx: Ctx) -> tuple[TestResult, str]:
    """Run the target's test command (or the crabbox wrapper) in the sandbox; returns the parsed
    junit result and the tail of its output (the failure text a fix prompt gets)."""
    contract = _contract(ctx)
    cmd = contract.test
    if ctx.cfg.tests == "crabbox":
        cmd = crabbox_command(ctx.cfg.crabbox_provider, contract.junit, contract.test)
    removed = ctx.sb.run(f"rm -f {shlex.quote(contract.junit)}")
    if not removed.ok:
        raise StageError("sandbox", f"could not clear stale JUnit report: {removed.stderr[-800:]}")
    res = ctx.sb.run(cmd)
    output = (res.stdout[-6000:] + "\n" + res.stderr[-2000:]).strip()
    try:
        counts = _parse_junit(ctx.sb.read(contract.junit))
    except (FileNotFoundError, ET.ParseError, ValueError):
        result = TestResult(
            exit_code=res.exit_code or 1, junit_path=None, report_valid=False
        )
    else:
        result = TestResult(
            **counts,
            exit_code=res.exit_code,
            junit_path=contract.junit,
            report_valid=True,
        )
    _assert_workspace_head(ctx, "verification")
    _ensure_clean(ctx, "verification", allow_artifacts=True)
    return result, output


def crabbox_command(provider: str, junit: str, test_cmd: str) -> str:
    """``crabbox run`` wrapper for the target's test command. The junit file comes back through
    ``-download <junit>=<junit>`` (never ``-artifact-glob``: SSH-lease only); in-place providers
    (``IN_PLACE_PROVIDERS``) already leave it in the working tree, so no download is requested."""
    q = shlex.quote
    download = "" if provider in IN_PLACE_PROVIDERS else f"-download {q(f'{junit}={junit}')} "
    return (
        f"crabbox run -provider {q(provider)} -junit {q(junit)} {download}"
        f"-ttl 45m -idle-timeout 15m -- {test_cmd}"
    )


def _parse_junit(xml_text: str) -> dict[str, int]:
    root = ET.fromstring(xml_text)

    def local_name(element: ET.Element) -> str:
        return element.tag.rsplit("}", 1)[-1]

    if local_name(root) == "testsuite":
        suites = [root]
    elif local_name(root) == "testsuites":
        # Count top-level aggregates once. ``iter`` double-counts nested suite totals.
        suites = [element for element in root if local_name(element) == "testsuite"]
    else:
        suites = []
    if not suites:
        raise ValueError("JUnit report contains no testsuite")
    total = failed = errors = skipped = 0
    for s in suites:
        total += int(s.get("tests", 0))
        failed += int(s.get("failures", 0))
        errors += int(s.get("errors", 0))
        skipped += int(s.get("skipped", 0))
    if total <= 0 or min(failed, errors, skipped) < 0:
        raise ValueError("JUnit report contains invalid test counts")
    if failed + errors + skipped > total:
        raise ValueError("JUnit report result counts exceed its test count")
    return {
        "passed": max(total - failed - errors - skipped, 0),
        "failed": failed,
        "errors": errors,
        "skipped": skipped,
    }


def _ensure_clean(ctx: Ctx, phase: str, *, allow_artifacts: bool = False) -> None:
    """Refuse uncommitted workspace changes outside the trusted artifact chain."""

    exclude = f" {_exclude(ctx)}" if allow_artifacts else ""
    dirty = _sh(ctx, f"git status --porcelain=v1 --untracked-files=all -- .{exclude}").strip()
    if dirty:
        raise StageError(
            "policy",
            f"{phase} changed files outside the reviewed commit stream:\n{dirty[-2000:]}",
        )


def _test_numbers(tr: TestResult) -> dict[str, float]:
    return {
        "tests_passed": float(tr.ok),
        "tests_failed": float(tr.failed + tr.errors),
        "tests_count": float(tr.passed + tr.failed + tr.errors + tr.skipped),
    }


# ---------------------------------------------------------------- stages


COPY_IGNORE = shutil.ignore_patterns(".venv", ".factory", ".git", "__pycache__", ".pytest_cache")


def seed_local_workdir(workdir: Path, target_dir: str) -> bool:
    """Copy a declared target into an empty host workdir, never into the source tree.

    A root target is only meaningful when the command is launched from that target's checkout.
    Missing sources fail before Git can manufacture an empty baseline that looks like the target.
    """
    workdir.mkdir(parents=True, exist_ok=True)
    if any(workdir.iterdir()):
        return False
    src = Path.cwd() if not target_dir else Path(target_dir)
    if target_dir and not src.is_dir():
        src = FACTORY_ROOT / target_dir
    if not src.is_dir():
        raise ValueError(f"target directory does not exist: {target_dir or src}")
    if not target_dir and not (src / "factory.toml").is_file():
        raise ValueError(
            "a local root target must run from the target checkout containing factory.toml"
        )
    shutil.copytree(src, workdir, ignore=COPY_IGNORE, dirs_exist_ok=True)
    return True


def _seed_exclude(ctx: Ctx) -> None:
    """Append NEVER_COMMITTED to ``.git/info/exclude`` through ``sb.write`` (host-side for local,
    srt and docker; ``islo cp`` for islo) — never through a confined shell, which srt may deny
    for anything under ``.git/``."""
    git_dir = ctx.sb.run("git rev-parse --git-dir").stdout.strip() or ".git"
    path = f"{git_dir}/info/exclude"
    try:
        current = ctx.sb.read(path)
    except FileNotFoundError:
        current = ""
    have = set(current.splitlines())
    missing = [p for p in NEVER_COMMITTED if p not in have]
    if missing:
        sep = "" if not current or current.endswith("\n") else "\n"
        ctx.sb.write(path, current + sep + "\n".join(missing) + "\n")


def setup(ctx: Ctx) -> StageResult:
    """Prepare the sandbox: repo, bot identity, baseline, work branch, deps, base sha, contract."""
    t0 = time.monotonic()
    sb = ctx.sb
    policy = {
        "config": {
            name: getattr(ctx.cfg, name)
            for name in (
                "sandbox",
                "agent",
                "scm",
                "approve",
                "tests",
                "crabbox_provider",
                "max_build_iterations",
                "max_review_fixes",
                "max_turns",
                "max_budget_usd_per_stage",
                "max_budget_usd",
                "gate_timeout_h",
                "stage_timeout_h",
                "max_parallel_jobs",
                "gateway_profile",
                "islo_environment",
                "sandbox_ttl_s",
                "sandbox_idle_s",
                "islo_snapshot",
                "toolset_backend",
                "toolset_workdir",
                "srt_allowed_domains",
                "docker_image",
                "docker_credentials",
                "docker_network",
                "docker_user",
                "allow_local_agent",
            )
        },
        "blueprint": ctx.blueprint.model_dump(mode="json") if ctx.blueprint else None,
    }
    policy_sha256 = hashlib.sha256(
        json.dumps(policy, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    identity = _dumps(
        {
            "schema": 2,
            "run_id": ctx.cfg.run_id,
            "blueprint": ctx.cfg.blueprint,
            "issue_id": ctx.issue.id,
            "repo": ctx.cfg.repo,
            "target_dir": ctx.cfg.target_dir,
            "base_branch": ctx.cfg.base_branch,
            "policy_sha256": policy_sha256,
        }
    )
    if ctx.state.has_control("identity.json"):
        if ctx.state.read_control("identity.json") != identity:
            raise StageError("policy", "run id is already bound to a different job identity")
    else:
        ctx.state.write_control("identity.json", identity)
    if isinstance(sb, LocalSandbox):
        seed_local_workdir(sb.root, ctx.cfg.target_dir)
    sb.ensure()
    _seed_exclude(ctx)
    # Identity travels as `git -c` (here and in commit()): srt forbids writes to .git/config.
    if not sb.run("git rev-parse --verify -q HEAD").ok:
        _sh(ctx, GIT_ADD_ALL)
        _sh(ctx, f"{_GIT_BOT} -c commit.gpgsign=false commit -q --allow-empty -m baseline")
    if not ctx.state.has_control("base"):
        base = _workspace_head(ctx)
        ctx.state.write_control("base", base + "\n")
        ctx.state.write_control(
            "started", datetime.now(UTC).isoformat(timespec="seconds") + "\n"
        )
        _record_workspace_head(ctx, base)
    base = ctx.state.read_control("base").strip()
    base_commit = shlex.quote(f"{base}^{{commit}}")
    if not sb.run(f"git cat-file -e {base_commit}").ok:
        raise StageError("policy", f"recorded base commit is unavailable: {base}")
    sb.write(BASE_FILE, ctx.state.read_control("base"))
    sb.write(STARTED_FILE, ctx.state.read_control("started"))
    q = shlex.quote(ctx.branch)
    branch_ref = shlex.quote(f"refs/heads/{ctx.branch}")
    if sb.run(f"git rev-parse --verify -q {branch_ref}").ok:
        _sh(ctx, f"git checkout -q {q}")
    else:
        recorded_head = (
            ctx.state.read_control("workspace-head").strip()
            if ctx.state.has_control("workspace-head")
            else ""
        )
        progressed = any(
            record.stage in {"build_and_test", "review", "deliver"}
            for record in _persisted(ctx)
        )
        if (recorded_head and recorded_head != base) or progressed:
            raise StageError(
                "policy",
                "factory branch disappeared after work began; refusing to recreate lost work",
            )
        _sh(ctx, f"git checkout -q -b {q} {shlex.quote(base)}")
        _record_workspace_head(ctx, base)
    head = _assert_workspace_head(ctx, "setup")
    if not sb.run(f"git merge-base --is-ancestor {shlex.quote(base)} {shlex.quote(head)}").ok:
        raise StageError("policy", "recorded base is not an ancestor of the factory branch")
    if sb.exists("pyproject.toml"):
        res = sb.run("uv sync --group dev", timeout_s=1200)
        if not res.ok:
            raise StageError(
                "sandbox", f"uv sync failed: {res.stderr.strip()[-800:]}", retryable=True
            )
    _contract(ctx)
    _review_policy(ctx)
    return StageResult(stage="setup", duration_s=round(time.monotonic() - t0, 3))


@_timed
def intent(ctx: Ctx) -> StageResult:
    """No agent: the originator's words, verbatim, under a small front matter."""
    path = f"{ctx.art}/intent.md"
    if prior := _done(ctx, "intent"):
        return _skipped(prior)
    meta = {
        "id": ctx.issue.id,
        "title": ctx.issue.title,
        "labels": list(ctx.issue.labels),
        "url": ctx.issue.url,
        "run_id": ctx.cfg.run_id,
    }
    front = yaml.safe_dump(meta, sort_keys=False, allow_unicode=True).rstrip()
    text = f"---\n{front}\n---\n{ctx.issue.body.rstrip()}\n"
    ctx.write_artifact(path, text)
    return StageResult(stage="intent", artifacts=[path], preview=text[:PREVIEW_CHARS])


def _document_only(text: str) -> str:
    """Drop any chatty preamble before the first markdown heading (the artifact is a document)."""
    lines = text.rstrip().splitlines()
    for i, line in enumerate(lines):
        if line.startswith("# "):
            return "\n".join(lines[i:])
    return text.rstrip()


@_timed
def spec(ctx: Ctx) -> StageResult:
    """Agent (read-only) turns intent.md into spec.md."""
    path = f"{ctx.art}/spec.md"
    if prior := _done(ctx, "spec"):
        return _skipped(prior)
    prompt = render_prompt(
        "spec", issue_id=ctx.issue.id, intent=ctx.read_artifact(f"{ctx.art}/intent.md")
    )
    res = _agent(ctx, "spec", 1, prompt, None)
    if not res.text.strip():
        raise StageError("agent", "spec returned empty text")
    ctx.write_artifact(path, _document_only(res.text) + "\n")
    return StageResult(stage="spec", artifacts=[path])


@_timed
def plan(ctx: Ctx) -> StageResult:
    """Agent (read-only) produces a typed Plan -> plan.json + rendered plan.md."""
    json_path, md_path = f"{ctx.art}/plan.json", f"{ctx.art}/plan.md"
    if prior := _done(ctx, "plan"):
        return _skipped(prior)
    prompt = render_prompt(
        "plan",
        issue_id=ctx.issue.id,
        intent=ctx.read_artifact(f"{ctx.art}/intent.md"),
        spec=_read_or(ctx, f"{ctx.art}/spec.md"),  # "(none)" when the line has no spec stage
        protected=_protected(ctx, "plan"),
    )
    res = _agent(ctx, "plan", 1, prompt, Plan)
    p = Plan.model_validate(res.data)
    md = p.to_markdown(ctx.issue.id)
    ctx.write_artifact(md_path, md)
    ctx.write_artifact(json_path, _dumps(p.model_dump()))
    gate = ctx.blueprint.gate_after("plan") if ctx.blueprint else None
    preview = ctx.read_artifact(f"{ctx.art}/{gate.artifact}") if gate else md
    return StageResult(
        stage="plan",
        artifacts=[md_path, json_path],
        numbers={"files": float(len(p.files))},
        preview=preview[:PREVIEW_CHARS],
    )


def _summary_line(res: AgentResult, fallback: str) -> str:
    try:
        text = BuildSummary.model_validate(res.data).summary.strip().splitlines()[0]
    except (ValueError, IndexError):
        text = ""
    return (text or fallback)[:72]


@_timed
def build_and_test(ctx: Ctx) -> StageResult:
    """Bounded build -> commit -> test loop; iterations >= 2 use the ``fix`` stage."""
    if prior := _done(ctx, "build_and_test"):
        return _skipped(prior)
    spec_text = _read_or(ctx, f"{ctx.art}/spec.md")  # "(none)" when the line has no spec stage
    plan_text = ctx.read_artifact(f"{ctx.art}/plan.md")
    failures = ""
    for i in range(1, ctx.cfg.max_build_iterations + 1):
        stage = "build" if i == 1 else "fix"
        prompt = render_prompt(
            stage,
            issue_id=ctx.issue.id,
            spec=spec_text,
            plan=plan_text,
            failures=failures,
            protected=_protected(ctx, stage),
        )
        res = _agent(ctx, stage, i, prompt, BuildSummary)
        commit(ctx, stage=stage, msg=f"{stage}: {_summary_line(res, f'iteration {i}')}")
        tr, output = run_tests(ctx)
        if tr.ok:
            numbers = {
                "iterations": float(i),
                "first_pass_ci": float(i == 1),
                **_test_numbers(tr),
            }
            return StageResult(stage="build_and_test", numbers=numbers)
        failures = f"exit code {tr.exit_code}; failed={tr.failed} errors={tr.errors}\n\n{output}"
    raise StageError(
        "policy",
        f"tests still failing after {ctx.cfg.max_build_iterations} build iterations; "
        f"last failure:\n{failures[-1500:]}",
    )


def cap_nits(review: Review, cap: int = NIT_CAP) -> tuple[Review, int]:
    """Keep the first ``cap`` nits in order of appearance; return (review, dropped count)."""
    kept: list[Finding] = []
    nits = 0
    for f in review.findings:
        if f.severity == "nit":
            nits += 1
            if nits > cap:
                continue
        kept.append(f)
    return Review(verdict=review.verdict, findings=kept), max(nits - cap, 0)


def _plan_fidelity(ctx: Ctx, base: str) -> list[Finding]:
    """Deterministic pass 4 of REVIEW.md, both halves: files touched by the diff but absent from
    plan.json (major), and planned files the diff never touched (minor; major under the target's
    tests dir, since a dropped test file is untested behaviour)."""
    plan_data = _read_json(ctx, f"{ctx.art}/plan.json", None)
    if not isinstance(plan_data, dict):
        return []
    planned = {str(f).strip() for f in plan_data.get("files") or []} - {""}
    changed = set(
        _sh(ctx, f"git diff --name-only --relative {base}..HEAD -- . {_exclude(ctx)}").split()
    )
    tests_dir = _contract(ctx).tests_dir.rstrip("/") + "/"
    findings = [
        Finding(
            severity="major",
            file=f,
            title="Plan fidelity: file not listed in plan.md",
            detail=f"`{f}` was changed but plan.md does not list it. Add it to the plan or "
            "revert the change.",
        )
        for f in sorted(changed - planned)
    ]
    findings += [
        Finding(
            severity="major" if f.startswith(tests_dir) else "minor",
            file=f,
            title="Plan fidelity: planned file not touched",
            detail=f"plan.md lists `{f}` but the diff never touches it. Do the planned work or "
            "drop it from the plan.",
        )
        for f in sorted(planned - changed)
    ]
    return findings


def _review_policy(ctx: Ctx) -> str:
    if ctx.state.has_control("review-policy.md"):
        return ctx.state.read_control("review-policy.md")
    policy = ctx.blueprint.review.policy if ctx.blueprint else "REVIEW.md"
    text = _read_or(ctx, policy)
    if not text.strip():
        packaged = FACTORY_ROOT / policy
        if not packaged.is_file():
            raise StageError("policy", f"review policy not found: {policy}")
        text = packaged.read_text(encoding="utf-8")
    ctx.state.write_control("review-policy.md", text)
    return text


def _format_findings(findings: list[Finding]) -> str:
    return "\n".join(
        f"- [{f.severity}] {f.file}:{f.line or '-'} {f.title} — {f.detail}" for f in findings
    )


def _tests_blocker(ctx: Ctx, tr: TestResult, output: str) -> Finding:
    """Synthetic blocker for a review fix that broke the target's test suite."""
    return Finding(
        severity="blocker",
        file=_contract(ctx).tests_dir,
        title="Tests failing after review fix",
        detail=(
            f"exit code {tr.exit_code}; failed={tr.failed} errors={tr.errors}. The review fix "
            f"must keep the suite green. Tail:\n{output[-1500:]}"
        ),
    )


@_timed
def review(ctx: Ctx) -> StageResult:
    """Agent review (Review schema) with the nit cap and plan fidelity enforced in code; blockers
    trigger at most ``max_review_fixes`` fix + test + re-review rounds. A fix that leaves the
    suite red is itself a blocker (``_tests_blocker``): the stage cannot end ``ok`` on red tests.
    Review fixes are agent calls ``fix.<max_build_iterations + k>`` (see module docstring)."""
    path = f"{ctx.art}/review.json"
    if prior := _done(ctx, "review"):
        return _skipped(prior)
    _assert_workspace_head(ctx, "review")
    base = ctx.state.read_control("base").strip()
    spec_text = _read_or(ctx, f"{ctx.art}/spec.md")  # "(none)" when the line has no spec stage
    plan_text = ctx.read_artifact(f"{ctx.art}/plan.md")
    numbers: dict[str, float] = {}
    rv, dropped, fixes = Review(verdict="approve"), 0, 0
    tests_blocker: Finding | None = None
    for k in range(ctx.cfg.max_review_fixes + 1):
        diff = _sh(ctx, f"git diff {base}..HEAD -- . {_exclude(ctx)}")
        prompt = render_prompt(
            "review",
            issue_id=ctx.issue.id,
            review_policy=_review_policy(ctx),
            spec=spec_text,
            plan=plan_text,
            diff=diff,
        )
        res = _agent(ctx, "review", k + 1, prompt, Review)
        rv, dropped = cap_nits(Review.model_validate(res.data), _nit_cap(ctx))
        rv.findings.extend(_plan_fidelity(ctx, base))
        if tests_blocker is not None:
            rv.findings.append(tests_blocker)
        if not rv.blockers or k == ctx.cfg.max_review_fixes:
            break
        fixes = k + 1
        fix_prompt = render_prompt(
            "fix",
            issue_id=ctx.issue.id,
            plan=plan_text,
            failures="Review blockers:\n" + _format_findings(rv.blockers),
            protected=_protected(ctx, "fix"),
        )
        iteration = ctx.cfg.max_build_iterations + fixes
        fix_res = _agent(ctx, "fix", iteration, fix_prompt, BuildSummary)
        commit(ctx, stage="fix", msg=f"fix: {_summary_line(fix_res, 'address review blockers')}")
        tr, output = run_tests(ctx)
        numbers.update(_test_numbers(tr))
        tests_blocker = None if tr.ok else _tests_blocker(ctx, tr, output)
    record = {
        "verdict": "request_changes" if rv.blockers else "approve",  # REVIEW.md contract
        "findings": [f.model_dump() for f in rv.findings],
        "dropped_nits": dropped,
        "fixes": fixes,
    }
    ctx.write_artifact(path, _dumps(record))
    counts = {s: sum(1 for f in rv.findings if f.severity == s) for s in SEVERITIES}
    numbers.update(
        {
            "blockers": float(counts["blocker"]),
            "findings": float(len(rv.findings)),
            "dropped_nits": float(dropped),
            "fixes": float(fixes),
            **{k: float(v) for k, v in counts.items()},
        }
    )
    status = "blocked" if rv.blockers else "ok"
    return StageResult(stage="review", status=status, artifacts=[path], numbers=numbers)


def _load_approvals(ctx: Ctx) -> list[Approval]:
    data = _read_json(ctx, f"{ctx.art}/approvals.json", [])
    if not isinstance(data, list):
        raise StageError("policy", "approvals.json must contain a JSON array")
    try:
        return [Approval.model_validate(a) for a in data]
    except ValidationError as e:
        raise StageError("policy", f"approvals.json is invalid: {e}") from e


def _gate_artifact(ctx: Ctx, gate_name: str) -> str:
    gate = next(
        (item for item in _pipeline(ctx) if isinstance(item, Gate) and item.name == gate_name),
        None,
    )
    if gate is None:
        raise StageError("policy", f"approval names unknown gate {gate_name!r}")
    return f"{ctx.art}/{gate.artifact}"


def record_approval(ctx: Ctx, approval: Approval) -> None:
    """Append one approval to ``{art}/approvals.json`` (committed by deliver)."""
    path = f"{ctx.art}/approvals.json"
    artifact = _gate_artifact(ctx, approval.gate)
    digest = hashlib.sha256(ctx.read_artifact(artifact).encode("utf-8")).hexdigest()
    approval = approval.model_copy(update={"artifact_sha256": digest})
    data = _read_json(ctx, path, [])
    if not isinstance(data, list):
        raise StageError("policy", "approvals.json must contain a JSON array")
    data = [
        item
        for item in data
        if not isinstance(item, dict) or item.get("gate") != approval.gate
    ]
    data.append(approval.model_dump(mode="json"))
    ctx.write_artifact(path, _dumps(data))


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    head = "| " + " | ".join(headers) + " |\n|" + "|".join(" --- " for _ in headers) + "|"
    body = "\n".join("| " + " | ".join(r) + " |" for r in rows)
    return head + ("\n" + body if body else "")


def pr_body(
    ctx: Ctx,
    *,
    findings: list[Finding],
    dropped_nits: int,
    approvals: list[Approval],
    stages: list[StageResult],
) -> str:
    """Render the PR description: banner, findings (blockers first), approvals, stages."""
    parts: list[str] = []
    if ctx.agent.kind == "scripted":
        parts.append(
            "> **SCRIPTED REPLAY** — agent=scripted, no model calls; fixtures replayed from "
            f"`{ctx.cfg.fixtures_dir}`. Not real work: do not merge."
        )
    url = f" ({ctx.issue.url})" if ctx.issue.url else ""
    parts.append(f"## Intent\n{ctx.issue.title}{url}")
    lines = [f"## Review — {len(findings)} finding(s)"]
    for sev in SEVERITIES:
        group = [f for f in findings if f.severity == sev]
        if not group:
            continue
        note = ""
        if sev == "nit" and dropped_nits:
            note = f", {dropped_nits} more dropped by the cap of {_nit_cap(ctx)}"
        lines.append(f"### {sev.capitalize()} ({len(group)}{note})")
        lines.extend(f"- `{f.file}:{f.line or '-'}` **{f.title}** — {f.detail}" for f in group)
    parts.append("\n".join(lines))
    approval_rows = [
        [a.gate, a.decision, a.actor, a.at.isoformat(timespec="seconds")] for a in approvals
    ]
    parts.append("## Approvals\n" + _md_table(["gate", "decision", "actor", "at"], approval_rows))
    stage_rows = [
        [
            s.stage,
            s.status,
            f"{s.duration_s:.1f}",
            f"{s.cost_usd:.4f}",
            ", ".join(f"{k}={v:g}" for k, v in s.numbers.items()),
        ]
        for s in stages
    ]
    parts.append(
        "## Stages\n"
        + _md_table(["stage", "status", "duration s", "cost usd", "numbers"], stage_rows)
    )
    parts.append(
        "## Provenance\n"
        f"- run `{ctx.cfg.run_id}` · agent `{ctx.agent.kind}` · sandbox `{ctx.sb.name}`\n"
        f"- artifact chain: `{ctx.art}/` (intent, spec, plan, review, approvals, metrics, agent/)\n"
        f"- commits authored by `{BOT_NAME}` with Factory-Run / Factory-Stage / Agent trailers"
    )
    return "\n\n".join(parts) + "\n"


def denied_tool_calls(ctx: Ctx) -> int:
    """Number of tool calls ``swf_guard.py`` refused in this run (``decision == "deny"`` lines of
    the sandbox's ``.factory/hooks.jsonl``); 0 when the hook never ran (scripted agent)."""
    count = 0
    for line in _read_or(ctx, HOOKS_LOG).splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        count += isinstance(entry, dict) and entry.get("decision") == "deny"
    return count


def _copy_audit_logs(ctx: Ctx) -> None:
    """Carry the audit logs into the committed chain (``{art}/agent/``): the orchestrator's stage
    log and the hook's decisions, which otherwise die with the sandbox."""
    run_log = ctx.state.root / RUN_STAGES_LOG
    if run_log.is_file():
        ctx.write_artifact(
            f"{ctx.art}/agent/stages.jsonl", run_log.read_text(encoding="utf-8")
        )
    if ctx.sb.exists(HOOKS_LOG):
        ctx.write_artifact(f"{ctx.art}/agent/hooks.jsonl", ctx.sb.read(HOOKS_LOG))


def _validate_approvals(ctx: Ctx, approvals: list[Approval]) -> tuple[bool, str | None]:
    """Bind every required gate decision to the exact artifact that was shown."""

    gates = [item for item in _pipeline(ctx) if isinstance(item, Gate)]
    by_name: dict[str, Approval] = {}
    for approval in approvals:
        if approval.gate in by_name:
            raise StageError("policy", f"duplicate approval for gate {approval.gate!r}")
        by_name[approval.gate] = approval
    known = {gate.name for gate in gates}
    if unknown := sorted(set(by_name) - known):
        raise StageError("policy", f"approvals name unknown gates: {unknown}")

    processed: set[str] = set()
    rejected_gate: str | None = None
    for gate in gates:
        approval = by_name.get(gate.name)
        if approval is None:
            raise StageError("policy", f"missing decision for required gate {gate.name!r}")
        processed.add(gate.name)
        artifact = f"{ctx.art}/{gate.artifact}"
        try:
            content = ctx.read_artifact(artifact)
        except FileNotFoundError as e:
            raise StageError("policy", f"approved artifact is missing: {artifact}") from e
        expected = hashlib.sha256(content.encode("utf-8")).hexdigest()
        if approval.artifact_sha256 != expected:
            raise StageError("policy", f"approval for {gate.name} does not match its artifact")
        if approval.decision == "reject":
            rejected_gate = gate.name
            break
    if extra := sorted(set(by_name) - processed):
        raise StageError("policy", f"approvals are out of sequence after rejection: {extra}")
    return rejected_gate is not None, rejected_gate


def _validated_review(
    ctx: Ctx, required: list[str], completed: dict[str, StageResult]
) -> tuple[Review, dict[str, object], int]:
    """Load host-owned review evidence and fail closed on any inconsistency."""

    data: dict[str, object] = {}
    review = Review(verdict="approve")
    logged = completed.get("review")
    if "review" in required:
        try:
            parsed = json.loads(ctx.read_artifact(f"{ctx.art}/review.json"))
            if not isinstance(parsed, dict):
                raise ValueError("review record is not an object")
            data = parsed
            review = Review.model_validate(
                {"verdict": parsed.get("verdict"), "findings": parsed.get("findings")}
            )
            dropped_nits = parsed.get("dropped_nits", 0)
            if (
                not isinstance(dropped_nits, int)
                or isinstance(dropped_nits, bool)
                or dropped_nits < 0
            ):
                raise ValueError("dropped_nits must be a non-negative integer")
        except (FileNotFoundError, ValueError, ValidationError) as e:
            raise StageError("policy", f"review evidence is invalid: {e}") from e
    logged_blockers = int(logged.numbers.get("blockers", 0)) if logged else 0
    if logged is not None and logged.status == "blocked":
        logged_blockers = max(logged_blockers, 1)
    return review, data, max(len(review.blockers), logged_blockers)


@_timed
def deliver(ctx: Ctx) -> StageResult:
    """Write metrics, commit the artifact chain, extract the patch stream and publish the PR.
    Never skipped: publishing is the observable output of the run. A rejected gate (any
    ``decision == "reject"`` in approvals.json) still publishes, as ``[REJECTED]`` +
    ``factory:rejected``, so the refusal and its actor land in git; blockers left by review
    publish as ``[BLOCKED]`` + ``factory:blocked``. Both return ``status="blocked"``."""
    _assert_workspace_head(ctx, "delivery")
    approvals = _load_approvals(ctx)
    if not ctx.state.has_artifact(f"{ctx.art}/approvals.json"):
        ctx.write_artifact(f"{ctx.art}/approvals.json", "[]\n")
    rejected, rejected_gate = _validate_approvals(ctx, approvals)
    stages = load_stage_results(ctx)
    completed = {stage.stage: stage for stage in stages if stage.status in {"ok", "blocked"}}
    order = list(ctx.blueprint.order) if ctx.blueprint else list(CANONICAL_ORDER)
    required = order[: order.index(rejected_gate) + 1] if rejected_gate else order[:-1]
    if missing := [stage for stage in required if stage not in completed]:
        raise StageError("policy", f"delivery lacks completed stage evidence: {missing}")
    rv, data, blockers = _validated_review(ctx, required, completed)
    denied = denied_tool_calls(ctx)
    metrics_mod.write_run_metrics(
        ctx,
        stages,
        approvals,
        total_cost_usd=sum(record.cost_usd for record in _persisted(ctx)),
    )
    _copy_audit_logs(ctx)
    cleared = ctx.sb.run(f"rm -rf -- {shlex.quote(ctx.art)}")
    if not cleared.ok:
        raise StageError("sandbox", f"could not rebuild artifact chain: {cleared.stderr[-800:]}")
    ctx.state.mirror_all(ctx.sb)
    _ensure_clean(ctx, "delivery", allow_artifacts=True)
    commit(
        ctx,
        stage="deliver",
        msg=f"docs(factory): artifact chain for {ctx.issue.id}",
        paths=[ctx.art],
    )
    base = ctx.state.read_control("base").strip()
    patch = _sh(ctx, f"git format-patch --stdout {base}..HEAD")
    commits = int(_sh(ctx, f"git rev-list --count {base}..HEAD").strip() or 0)
    # Patch paths are repo-relative; the sandbox cwd may be a subdir of the checkout (islo) or
    # the target copy itself (local/srt, prefix ""). Only that target subtree may land.
    prefix = _sh(ctx, "git rev-parse --show-prefix").strip()
    blocked = rejected or bool(blockers)
    labels = list(ctx.blueprint.labels if ctx.blueprint else DEFAULT_LABELS)
    if rejected:
        labels.append("factory:rejected")
    elif blocked:
        labels.append("factory:blocked")
    title = f"{ctx.issue.id}: {ctx.issue.title}"
    banner = "[REJECTED] " if rejected else "[BLOCKED] " if blocked else ""
    url = ctx.scm.publish(
        branch=ctx.branch,
        patch=patch.encode("utf-8"),
        title=banner + title,
        body=pr_body(
            ctx,
            findings=rv.findings,
            dropped_nits=int(data.get("dropped_nits", 0)),
            approvals=approvals,
            stages=stages,
        ),
        labels=labels,
        allowed_prefixes=[prefix],
    )
    return StageResult(
        stage="deliver",
        status="blocked" if blocked else "ok",
        artifacts=[url],
        numbers={
            "blockers": float(blockers),
            "commits": float(commits),
            "rejected": float(rejected),
            "denied_tool_calls": float(denied),
        },
    )


STAGES: dict[str, Stage] = {
    "intent": intent,
    "spec": spec,
    "plan": plan,
    "build_and_test": build_and_test,
    "review": review,
    "deliver": deliver,
}

# The default blueprint's walk (blueprints/default.toml); ``Ctx.blueprint=None`` uses it.
PIPELINE: tuple[Stage | Gate, ...] = (
    intent,
    Gate("intent", "intent.md"),
    spec,
    plan,
    Gate("plan", "plan.md"),
    build_and_test,
    review,
    deliver,
)


# ---------------------------------------------------------------- driver


def run_pipeline(ctx: Ctx, approver: Approver) -> RunReport:
    """CLI driver: walk the blueprint's pipeline (``PIPELINE`` without one); gates go through
    ``approver``; a rejection skips straight to ``deliver`` (a ``[REJECTED]`` PR carrying the
    artifact chain and the refusal) and stops the run."""
    approvals: list[Approval] = []
    for item in _pipeline(ctx):
        if isinstance(item, Gate):
            approval = approver(item, ctx)
            record_approval(ctx, approval)
            approvals.append(approval)
            print(f"{'gate:' + item.name:<16} {approval.decision} by {approval.actor}")
            if approval.decision == "reject":
                _run_stage(ctx, deliver)
                break
            continue
        _run_stage(ctx, item)
    return build_report(ctx, approvals)


def _run_stage(ctx: Ctx, stage: Stage) -> StageResult:
    result = stage(ctx)
    ctx.stages.append(result)
    nums = ", ".join(f"{k}={v:g}" for k, v in result.numbers.items())
    print(f"{result.stage:<16} {result.status:<8} {result.duration_s:6.1f}s  {nums}")
    return result


def build_report(ctx: Ctx, approvals: list[Approval]) -> RunReport:
    """Summarise ``ctx.stages`` into a RunReport."""
    stages = ctx.stages
    pr_url = next((s.artifacts[0] for s in stages if s.stage == "deliver" and s.artifacts), None)
    tests = [s.numbers["tests_passed"] for s in stages if "tests_passed" in s.numbers]
    return RunReport(
        run_id=ctx.cfg.run_id,
        issue_id=ctx.issue.id,
        agent=ctx.agent.kind,
        sandbox=ctx.sb.name,
        scm=ctx.scm.kind,
        stages=list(stages),
        approvals=approvals,
        pr_url=pr_url,
        tests_passed=bool(tests) and tests[-1] == 1.0,
        total_cost_usd=round(sum(s.cost_usd for s in _persisted(ctx)), 6),
    )


def cli_approver(gate: Gate, ctx: Ctx) -> Approval:
    """``approve=auto`` or a blueprint gate with ``auto = true`` records actor "auto"; otherwise
    show the artifact and ask."""
    if gate.auto or ctx.cfg.approve == "auto":
        return Approval(gate=gate.name, decision="approve", actor="auto", at=datetime.now(UTC))
    print(f"\n===== {gate.artifact} =====\n{ctx.read_artifact(f'{ctx.art}/{gate.artifact}')}\n")
    ok = typer.confirm(f"Approve gate '{gate.name}'?", default=False)
    return Approval(
        gate=gate.name,
        decision="approve" if ok else "reject",
        actor=getpass.getuser(),
        at=datetime.now(UTC),
    )
