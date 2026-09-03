"""The SDLC stages. ``STAGES`` (keyed by ``CANONICAL_ORDER``) is the registry a blueprint walks;
``PIPELINE`` is the default blueprint's walk and drives ``swfactory run`` and the Airflow DAG.

Every stage is a function ``Ctx -> StageResult`` that is idempotent: when the orchestrator's stage
log (``<run_dir>/stages.jsonl``, written by ``_timed`` on the orchestrator's own filesystem)
already holds a completed record for it, it returns ``status="skipped"``. Skip decisions never
read the sandbox: the agent can write anything there (``docs/factory/**``, ``.factory/**``), so a
forged artifact must not be able to skip a stage, and the run-level budget is seeded from the same
log. Loops (build/fix, review/fix) live *inside* stage functions and are bounded by ``Config``.
The agent never commits: stages commit with the bot identity and provenance trailers, and
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

import dataclasses
import functools
import getpass
import json
import shlex
import shutil
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NamedTuple

import typer
import yaml
from pydantic import BaseModel

from swfactory import metrics as metrics_mod
from swfactory.agent import POLICIES, Agent, Policy, render_prompt
from swfactory.config import Config, TargetContract, load_target_contract, protected_for
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

if TYPE_CHECKING:
    from swfactory.blueprint import Blueprint

FACTORY_ROOT = Path(__file__).resolve().parents[2]
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
RUN_STAGES_LOG = "stages.jsonl"  # authoritative stage log: <run_dir>/stages.jsonl (orchestrator)
HOOKS_LOG = ".factory/hooks.jsonl"  # swf_guard.py decisions, appended by the hook in the sandbox
SEVERITIES: tuple[str, ...] = ("blocker", "major", "minor", "nit")
# Scratch the factory never commits (added to .git/info/exclude by setup; the target may lack a
# .gitignore, as the demo copy does).
NEVER_COMMITTED = (".factory/", ".venv/", "__pycache__/", "*.pyc", ".pytest_cache/", ".ruff_cache/")


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
    target: dict | None = None  # the job's {"repo", "dir", "base_branch"} (provenance only)
    contract: TargetContract | None = None  # loaded by setup(); lazily on demand otherwise
    stages: list[StageResult] = field(default_factory=list)  # accumulated by run_pipeline
    spent_usd: float = 0.0  # run-level cost guard; seeded from <run_dir>/stages.jsonl by _agent
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
        try:
            ctx.contract = load_target_contract(ctx.sb)
        except ValueError as e:
            raise StageError("policy", str(e)) from e
    return ctx.contract


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _sh(ctx: Ctx, cmd: str, *, timeout_s: int = 600) -> str:
    """Run a command that must succeed; stdout is returned, failure is a sandbox StageError."""
    res = ctx.sb.run(cmd, timeout_s=timeout_s)
    if not res.ok:
        raise StageError(
            "sandbox", f"`{cmd}` failed (rc={res.exit_code}): {res.stderr.strip()[-800:]}"
        )
    return res.stdout


def _read_or(ctx: Ctx, path: str, default: str = "") -> str:
    try:
        return ctx.sb.read(path)
    except FileNotFoundError:
        return default


def _read_json(ctx: Ctx, path: str, default: object) -> object:
    text = _read_or(ctx, path)
    return json.loads(text) if text.strip() else default


def _dumps(obj: object) -> str:
    return json.dumps(obj, indent=2) + "\n"


def _exclude(ctx: Ctx) -> str:
    """Pathspec that keeps the artifact chain out of the reviewed diff."""
    return shlex.quote(f":!{ctx.art}")


def _skipped(prior: StageResult) -> StageResult:
    """The idempotent re-run of a stage: the prior record's artifacts/numbers/preview, no cost."""
    return prior.model_copy(update={"status": "skipped", "duration_s": 0.0, "cost_usd": 0.0})


def _persisted(ctx: Ctx) -> list[StageResult]:
    """Every record of ``<run_dir>/stages.jsonl``, the orchestrator-side log ``_timed`` appends
    to. This file is the ONLY evidence a stage may be skipped on: the sandbox is agent-writable."""
    path = ctx.run_dir / RUN_STAGES_LOG
    if not path.is_file():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    return [StageResult.model_validate_json(line) for line in lines if line.strip()]


def _done(ctx: Ctx, stage: str) -> StageResult | None:
    """The latest completed (non-skipped) record of ``stage`` in this run, if any."""
    recs = [r for r in _persisted(ctx) if r.stage == stage and r.status != "skipped"]
    return recs[-1] if recs else None


def _preview(text: str) -> str:
    return text[:PREVIEW_CHARS]


def _nit_cap(ctx: Ctx) -> int:
    return ctx.blueprint.review.nit_cap if ctx.blueprint else NIT_CAP


def _pipeline(ctx: Ctx) -> tuple[Stage | Gate, ...]:
    return ctx.blueprint.pipeline() if ctx.blueprint else PIPELINE


def _timed(fn: Stage) -> Stage:
    """Fill ``duration_s``/``cost_usd`` and persist the result: authoritatively to
    ``<run_dir>/stages.jsonl`` on the orchestrator, plus a copy in the sandbox (``STAGES_LOG``)
    that ``deliver`` carries into the committed artifact chain."""

    @functools.wraps(fn)
    def wrapper(ctx: Ctx) -> StageResult:
        t0, spent0 = time.monotonic(), ctx.spent_usd
        result = fn(ctx)
        result.duration_s = round(time.monotonic() - t0, 3)
        result.cost_usd = round(ctx.spent_usd - spent0, 6)
        line = result.model_dump_json() + "\n"
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        with (ctx.run_dir / RUN_STAGES_LOG).open("a", encoding="utf-8") as fh:
            fh.write(line)
        ctx.sb.write(STAGES_LOG, _read_or(ctx, STAGES_LOG) + line)
        return result

    return wrapper


def load_stage_results(ctx: Ctx) -> list[StageResult]:
    """Stage results of this run so far, from the orchestrator's ``<run_dir>/stages.jsonl``
    (DAG tasks are separate processes; in-process results are already in the log). Per stage
    the latest non-skipped record wins, in canonical order."""
    latest: dict[str, StageResult] = {}
    for rec in _persisted(ctx):
        prev = latest.get(rec.stage)
        if prev is None or rec.status != "skipped" or prev.status == "skipped":
            latest[rec.stage] = rec
    order = CANONICAL_ORDER
    return sorted(latest.values(), key=lambda r: order.index(r.stage) if r.stage in order else -1)


def seed_budget(ctx: Ctx) -> float:
    """Once per process: add what earlier tasks/processes of this run spent (every record in
    ``<run_dir>/stages.jsonl``) to ``ctx.spent_usd`` so ``Config.max_budget_usd`` is a RUN
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
    if hasattr(ctx.sb, "set_protected"):
        ctx.sb.set_protected(protected_for(_contract(ctx), stage))
    res = ctx.agent.run(
        ctx.sb,
        stage=stage,
        iteration=iteration,
        prompt=prompt,
        policy=_policy(ctx, stage),
        schema=schema,
        cfg=ctx.cfg,
        issue_id=ctx.issue.id,
    )
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


def commit(ctx: Ctx, *, stage: str, msg: str) -> str:
    """Commit everything under the target dir as the bot with provenance trailers (``.factory``
    is excluded by setup). Returns the HEAD sha; a no-op when there is nothing to commit."""
    _sh(ctx, "git add -A")
    if ctx.sb.run("git diff --cached --quiet").ok:
        return _sh(ctx, "git rev-parse HEAD").strip()
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
    return _sh(ctx, "git rev-parse HEAD").strip()


def _run_tests(ctx: Ctx) -> tuple[TestResult, str]:
    """Run the target's test command; returns the parsed junit result and the output tail."""
    contract = _contract(ctx)
    cmd = contract.test
    if ctx.cfg.tests == "crabbox":
        cmd = crabbox_command(ctx.cfg.crabbox_provider, contract.junit, contract.test)
    ctx.sb.run(f"rm -f {shlex.quote(contract.junit)}")
    res = ctx.sb.run(cmd)
    output = (res.stdout[-6000:] + "\n" + res.stderr[-2000:]).strip()
    try:
        counts = _parse_junit(ctx.sb.read(contract.junit))
    except FileNotFoundError:
        return TestResult(exit_code=res.exit_code, junit_path=None), output
    return TestResult(**counts, exit_code=res.exit_code, junit_path=contract.junit), output


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


def run_tests(ctx: Ctx) -> TestResult:
    """Run the target's tests in the sandbox (or through the crabbox wrapper) and parse junit."""
    return _run_tests(ctx)[0]


def _parse_junit(xml_text: str) -> dict[str, int]:
    root = ET.fromstring(xml_text)
    suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
    total = failed = errors = skipped = 0
    for s in suites:
        total += int(s.get("tests", 0))
        failed += int(s.get("failures", 0))
        errors += int(s.get("errors", 0))
        skipped += int(s.get("skipped", 0))
    return {
        "passed": max(total - failed - errors - skipped, 0),
        "failed": failed,
        "errors": errors,
        "skipped": skipped,
    }


def _test_numbers(tr: TestResult) -> dict[str, float]:
    return {
        "tests_passed": float(tr.ok),
        "tests_failed": float(tr.failed + tr.errors),
        "tests_count": float(tr.passed + tr.failed + tr.errors + tr.skipped),
    }


# ---------------------------------------------------------------- stages


FACTORY_ROOT = Path(__file__).resolve().parents[2]
COPY_IGNORE = shutil.ignore_patterns(".venv", ".factory", ".git", "__pycache__", ".pytest_cache")


def seed_local_workdir(workdir: Path, target_dir: str) -> bool:
    """Copy the target dir into an EMPTY local workdir (demo/CLI/DAG share this; never touches
    the source tree). Returns True when a copy happened."""
    workdir.mkdir(parents=True, exist_ok=True)
    if any(workdir.iterdir()) or not target_dir:
        return False
    src = Path(target_dir)
    if not src.is_dir():
        src = FACTORY_ROOT / target_dir
    if not src.is_dir():
        return False
    shutil.copytree(src, workdir, ignore=COPY_IGNORE, dirs_exist_ok=True)
    return True


def setup(ctx: Ctx) -> StageResult:
    """Prepare the sandbox: repo, bot identity, baseline, work branch, deps, base sha, contract."""
    t0 = time.monotonic()
    sb = ctx.sb
    if isinstance(sb, LocalSandbox):
        seed_local_workdir(sb.root, ctx.cfg.target_dir)
    sb.ensure()
    info = '"$(git rev-parse --git-dir)/info"'
    patterns = " ".join(shlex.quote(p) for p in NEVER_COMMITTED)
    _sh(
        ctx,
        f"mkdir -p {info} && for p in {patterns}; do "
        f'grep -qxF -- "$p" {info}/exclude 2>/dev/null || echo "$p" >> {info}/exclude; done',
    )
    # Identity travels as `git -c` (here and in commit()): srt forbids writes to .git/config.
    if not sb.run("git rev-parse --verify -q HEAD").ok:
        _sh(ctx, "git add -A")
        _sh(ctx, f"{_GIT_BOT} -c commit.gpgsign=false commit -q --allow-empty -m baseline")
    if not sb.exists(BASE_FILE):
        sb.write(BASE_FILE, _sh(ctx, "git rev-parse HEAD").strip() + "\n")
        sb.write(STARTED_FILE, _now() + "\n")
    q = shlex.quote(ctx.branch)
    if sb.run(f"git rev-parse --verify -q refs/heads/{q}").ok:
        _sh(ctx, f"git checkout -q {q}")
    else:
        _sh(ctx, f"git checkout -q -b {q}")
    if sb.exists("pyproject.toml"):
        res = sb.run("uv sync --group dev", timeout_s=1200)
        if not res.ok:
            raise StageError(
                "sandbox", f"uv sync failed: {res.stderr.strip()[-800:]}", retryable=True
            )
    _contract(ctx)
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
    ctx.sb.write(path, text)
    return StageResult(stage="intent", artifacts=[path], preview=_preview(text))


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
        "spec", issue_id=ctx.issue.id, intent=ctx.sb.read(f"{ctx.art}/intent.md")
    )
    res = _agent(ctx, "spec", 1, prompt, None)
    if not res.text.strip():
        raise StageError("agent", "spec returned empty text")
    ctx.sb.write(path, _document_only(res.text) + "\n")
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
        intent=ctx.sb.read(f"{ctx.art}/intent.md"),
        spec=_read_or(ctx, f"{ctx.art}/spec.md"),  # "(none)" when the line has no spec stage
        protected=_protected(ctx, "plan"),
    )
    res = _agent(ctx, "plan", 1, prompt, Plan)
    p = Plan.model_validate(res.data)
    md = p.to_markdown(ctx.issue.id)
    ctx.sb.write(md_path, md)
    ctx.sb.write(json_path, _dumps(p.model_dump()))
    return StageResult(
        stage="plan",
        artifacts=[md_path, json_path],
        numbers={"files": float(len(p.files))},
        preview=_preview(md),
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
    plan_text = ctx.sb.read(f"{ctx.art}/plan.md")
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
        tr, output = _run_tests(ctx)
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
    text = _read_or(ctx, "REVIEW.md")
    if not text.strip():
        text = (FACTORY_ROOT / "REVIEW.md").read_text(encoding="utf-8")
    return text


def _by_severity(findings: list[Finding]) -> dict[str, int]:
    return {s: sum(1 for f in findings if f.severity == s) for s in SEVERITIES}


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
    base = ctx.sb.read(BASE_FILE).strip()
    spec_text = _read_or(ctx, f"{ctx.art}/spec.md")  # "(none)" when the line has no spec stage
    plan_text = ctx.sb.read(f"{ctx.art}/plan.md")
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
        tr, output = _run_tests(ctx)
        numbers.update(_test_numbers(tr))
        tests_blocker = None if tr.ok else _tests_blocker(ctx, tr, output)
    record = {
        "verdict": "request_changes" if rv.blockers else "approve",  # REVIEW.md contract
        "findings": [f.model_dump() for f in rv.findings],
        "dropped_nits": dropped,
        "fixes": fixes,
    }
    ctx.sb.write(path, _dumps(record))
    counts = _by_severity(rv.findings)
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
    return [Approval.model_validate(a) for a in data] if isinstance(data, list) else []


def record_approval(ctx: Ctx, approval: Approval) -> None:
    """Append one approval to ``{art}/approvals.json`` (committed by deliver)."""
    path = f"{ctx.art}/approvals.json"
    data = _read_json(ctx, path, [])
    if not isinstance(data, list):
        data = []
    data.append(approval.model_dump(mode="json"))
    ctx.sb.write(path, _dumps(data))


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
    run_log = ctx.run_dir / RUN_STAGES_LOG
    if run_log.is_file():
        ctx.sb.write(f"{ctx.art}/agent/stages.jsonl", run_log.read_text(encoding="utf-8"))
    if ctx.sb.exists(HOOKS_LOG):
        ctx.sb.write(f"{ctx.art}/agent/hooks.jsonl", ctx.sb.read(HOOKS_LOG))


@_timed
def deliver(ctx: Ctx) -> StageResult:
    """Write metrics, commit the artifact chain, extract the patch stream and publish the PR.
    Never skipped: publishing is the observable output of the run. A rejected gate (any
    ``decision == "reject"`` in approvals.json) still publishes, as ``[REJECTED]`` +
    ``factory:rejected``, so the refusal and its actor land in git; blockers left by review
    publish as ``[BLOCKED]`` + ``factory:blocked``. Both return ``status="blocked"``."""
    approvals = _load_approvals(ctx)
    if not ctx.sb.exists(f"{ctx.art}/approvals.json"):
        ctx.sb.write(f"{ctx.art}/approvals.json", "[]\n")
    stages = load_stage_results(ctx)
    denied = denied_tool_calls(ctx)
    metrics_mod.write_run_metrics(ctx, stages, approvals)
    _copy_audit_logs(ctx)
    commit(ctx, stage="deliver", msg=f"docs(factory): artifact chain for {ctx.issue.id}")
    base = ctx.sb.read(BASE_FILE).strip()
    patch = _sh(ctx, f"git format-patch --stdout {base}..HEAD")
    commits = int(_sh(ctx, f"git rev-list --count {base}..HEAD").strip() or 0)
    # Patch paths are repo-relative; the sandbox cwd may be a subdir of the checkout (islo) or
    # the target copy itself (local/srt, prefix ""). Only that subtree (and docs/factory) may land.
    prefix = _sh(ctx, "git rev-parse --show-prefix").strip()
    data = _read_json(ctx, f"{ctx.art}/review.json", {})
    data = data if isinstance(data, dict) else {}
    rv = Review.model_validate(data) if data else Review(verdict="approve")
    rejected = any(a.decision == "reject" for a in approvals)
    blocked = rejected or bool(rv.blockers)
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
        allowed_prefixes=[prefix, "docs/factory/"],
    )
    return StageResult(
        stage="deliver",
        status="blocked" if blocked else "ok",
        artifacts=[url],
        numbers={
            "blockers": float(len(rv.blockers)),
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
        total_cost_usd=round(sum(s.cost_usd for s in stages), 6),
    )


def cli_approver(gate: Gate, ctx: Ctx) -> Approval:
    """``approve=auto`` or a blueprint gate with ``auto = true`` records actor "auto"; otherwise
    show the artifact and ask."""
    if gate.auto or ctx.cfg.approve == "auto":
        return Approval(gate=gate.name, decision="approve", actor="auto", at=datetime.now(UTC))
    print(f"\n===== {gate.artifact} =====\n{ctx.sb.read(f'{ctx.art}/{gate.artifact}')}\n")
    ok = typer.confirm(f"Approve gate '{gate.name}'?", default=False)
    return Approval(
        gate=gate.name,
        decision="approve" if ok else "reject",
        actor=getpass.getuser(),
        at=datetime.now(UTC),
    )
