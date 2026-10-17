"""The SDLC stages. ``STAGES`` (keyed by ``CANONICAL_ORDER``) is the registry a blueprint walks;
``PIPELINE`` is the default blueprint's walk and drives ``swfactory run`` and the Airflow DAG.

Every stage is a function ``Ctx -> StageResult`` that is idempotent: when the orchestrator's stage
log (``<run_dir>/state/stages.jsonl``, written by ``_timed`` on the orchestrator filesystem)
already holds a completed record for it, it returns ``status="skipped"``. Skip decisions never
read the sandbox: generated code and the target's test command execute there, so a compromised
checkout must not be able to skip a stage. The run-level budget is rebuilt from the per-call ledger
(``swfactory.call_accounting``, written around every provider call) with that host log as a floor:
a stage record only exists once a stage returns, and money is spent long before that.
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
import re
import shlex
import shutil
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, NamedTuple

import typer
import yaml
from pydantic import BaseModel, ValidationError

from swfactory import accepted_inputs
from swfactory import metrics as metrics_mod
from swfactory.agent import POLICIES, Agent, Invocation, Policy, render_prompt
from swfactory.approval_policy import GateMode, check_recorded, replay_approval
from swfactory.blueprint import CANONICAL_ORDER, Blueprint, ReviewSpec
from swfactory.call_accounting import CallLedger
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
from swfactory.publication_identity import PublicationIdentity, publication_key, this_instance
from swfactory.sandbox import SRT_RUNTIME_PROTECTED, DockerSandbox, LocalSandbox, Sandbox, SrtSandbox
from swfactory.scm import BOT_EMAIL, BOT_NAME, Scm
from swfactory.state import JournalCorruption, RunBusyError, RunState

if TYPE_CHECKING:
    from swfactory.harness_efficiency import PackedReviewDiff

# The ``blueprint=None`` defaults are the blueprint schema's own; a blueprint overrides each.
NIT_CAP = ReviewSpec().nit_cap  # REVIEW.md: at most 3 nits per review
DEFAULT_LABELS: tuple[str, ...] = tuple(Blueprint.model_fields["labels"].get_default(call_default_factory=True))
_GIT_BOT = f"git -c user.name={shlex.quote(BOT_NAME)} -c user.email={shlex.quote(BOT_EMAIL)}"
PREVIEW_CHARS = 4000  # StageResult.preview: head of the gate artifact shown to the approver
# crabbox providers that run in place: no file download step exists (or is needed) for junit.
IN_PLACE_PROVIDERS = frozenset({"srt", "anthropic-sandbox-runtime", "docker-sandbox", "apple-machine"})
BASE_FILE = ".factory/base"
STARTED_FILE = ".factory/started"
STAGES_LOG = ".factory/stages.jsonl"  # sandbox COPY of the stage log (audit trail, never trusted)
RUN_STAGES_LOG = "stages.jsonl"  # authoritative log: <run_dir>/state/stages.jsonl (orchestrator)
HOOKS_LOG = ".factory/hooks.jsonl"  # swf_guard.py decisions, appended by the hook in the sandbox
# `git add -A` that skips special files: the Anthropic Sandbox Runtime on Linux binds /dev/null
# style stubs over shell/git rc names in the cwd, which git refuses to stage.
GIT_ADD_ALL = (
    "git ls-files -z -o -m --exclude-standard | while IFS= read -r -d '' f; do "
    '[ -f "$f" ] || [ -L "$f" ] || [ -d "$f" ] && printf "%s\\0" "$f"; done '
    "| xargs -0 -r git add -- 2>/dev/null; git add -u -- . 2>/dev/null || true"
)

# Scratch the factory never commits, added to .git/info/exclude by setup (the target may lack a
# .gitignore, as the demo copy does). Sandbox Runtime's conditional placeholders are added by
# ``_seed_exclude`` only when the corresponding project paths do not exist.
NEVER_COMMITTED = (
    ".factory/",
    ".venv/",
    "__pycache__/",
    "*.pyc",
    ".pytest_cache/",
    ".ruff_cache/",
)
SPECIAL_DOTFILE_SCAN = (
    "find . -maxdepth 1 -mindepth 1 -name '.*' ! -type f ! -type d ! -type l -exec basename {} \\; 2>/dev/null"
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
    spent_usd: float = 0.0  # run guard; seeded from the durable call ledger + stage log by _agent
    budget_seeded: bool = False  # spent_usd includes earlier tasks/processes of this run
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def art(self) -> str:
        """Committed artifact dir, relative to the target dir: ``docs/factory/<issue_id>``."""
        return Config.artifacts_dir(self.issue.id)

    @property
    def instance(self) -> str:
        """This factory instance, as stamped on every commit and pull request it publishes."""
        return this_instance()

    @property
    def branch(self) -> str:
        """Branch the sandbox works on and the PR is opened from.

        Keyed on the WORK, not the run: `sha256(repo, target, issue)` -- the same inputs
        `CellIdentity.stable_id` uses -- rather than `run_id`. Two independently operated factory
        instances working one issue x target used to push `factory/<issue>-<run_a>` and
        `factory/<issue>-<run_b>`, which meant two branches, two pull requests and no way for
        either to notice the other. One ref makes git's own compare-and-swap the arbiter.

        A retry of the same run lands on the same branch as before, so the existing force-push
        behaviour is unchanged for the single-instance case.
        """
        key = publication_key(self.cfg.repo, self.cfg.target_dir, self.issue.id)
        return f"factory/{self.issue.id}-{key}"

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
    """An approval point; ``artifact`` is shown to the approver. ``mode`` is the blueprint's
    declaration (``gates[].mode``): ``"human"`` needs an identified answer, ``"auto"`` is a
    deliberately unattended line. The CLI walk and the DAG read the same declaration."""

    name: Literal["intent", "plan"]
    artifact: str
    mode: GateMode = "human"


Stage = Callable[[Ctx], StageResult]
Approver = Callable[[Gate, Ctx], Approval]


# ---------------------------------------------------------------- helpers


def _contract(ctx: Ctx) -> TargetContract:
    if ctx.contract is None:
        if ctx.state.has_control("target-contract.json"):
            try:
                ctx.contract = TargetContract.model_validate_json(ctx.state.read_control("target-contract.json"))
            except (OSError, ValueError) as e:
                raise StageError("policy", f"stored target contract is invalid: {e}") from e
        else:
            try:
                ctx.contract = load_target_contract(ctx.sb)
            except ValueError as e:
                raise StageError("policy", str(e)) from e
            ctx.state.write_control("target-contract.json", ctx.contract.model_dump_json(indent=2) + "\n")
    return ctx.contract


def _sh(ctx: Ctx, cmd: str, *, timeout_s: int = 600) -> str:
    """Run a command that must succeed; stdout is returned, failure is a sandbox StageError."""
    res = ctx.sb.run(cmd, timeout_s=timeout_s)
    if not res.ok:
        raise StageError("sandbox", f"`{cmd}` failed (rc={res.exit_code}): {res.stderr.strip()[-800:]}")
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
        return [StageResult.model_validate(record) for record in ctx.state.read_jsonl(RUN_STAGES_LOG)]
    except (OSError, ValueError) as e:
        raise StageError("policy", f"run journal is corrupt: {e}") from e


def persisted_cost(ctx: Ctx) -> float:
    """What the stage log says this run spent: the sum of every record's ``cost_usd``."""
    if ctx.cfg.agent == "external":
        return _observed_cost(ctx)
    return sum(record.cost_usd for record in _persisted(ctx))


def _observed_cost(ctx: Ctx) -> float:
    ledger = CallLedger(ctx.state)
    return (ledger.floor_usd() or 0.0) + sum(record.settled_usd or 0.0 for record in ledger.records())


def _artifact_sha256(ctx: Ctx, path: str) -> str:
    return hashlib.sha256(ctx.read_artifact(path).encode("utf-8")).hexdigest()


def _done(ctx: Ctx, stage: str) -> StageResult | None:
    """The latest completed (non-skipped) record of ``stage`` in this run, if any."""
    recs = [r for r in _persisted(ctx) if r.stage == stage and r.status != "skipped"]
    latest = recs[-1] if recs else None
    return latest if latest is not None and latest.status in {"ok", "blocked"} else None


def _nit_cap(ctx: Ctx) -> int:
    return ctx.blueprint.review.nit_cap if ctx.blueprint else NIT_CAP


def _pipeline(ctx: Ctx) -> tuple[Stage | Gate, ...]:
    return ctx.blueprint.pipeline() if ctx.blueprint else PIPELINE


@contextlib.contextmanager
def owned(state: RunState, operation: str) -> Iterator[None]:
    """Own ``operation`` on the run: a busy run is a retryable sandbox error, a corrupt journal a
    policy error."""
    try:
        with state.exclusive(operation):
            yield
    except RunBusyError as error:
        raise StageError("sandbox", str(error), retryable=True) from error
    except JournalCorruption as error:
        raise StageError("policy", str(error)) from error


def _owned[OwnedResult](fn: Callable[..., OwnedResult]) -> Callable[..., OwnedResult]:
    """Acquire ownership before any stage check, budget read or sandbox mutation."""

    @functools.wraps(fn)
    def wrapper(ctx: Ctx, *args: Any, **kwargs: Any) -> OwnedResult:
        with owned(ctx.state, fn.__name__):
            return fn(ctx, *args, **kwargs)

    return wrapper


def _timed(fn: Stage) -> Stage:
    """Fill ``duration_s``/``cost_usd`` and persist the result: authoritatively to
    ``<run_dir>/state/stages.jsonl`` on the orchestrator, plus a copy in the sandbox
    (``STAGES_LOG``) that ``deliver`` carries into the committed artifact chain."""

    @functools.wraps(fn)
    def wrapper(ctx: Ctx) -> StageResult:
        # Seed before taking the delta. On a fresh Airflow task, seeding inside the first agent
        # call would otherwise attribute every earlier task's cost to this stage a second time.
        seed_budget(ctx, refresh=True)

        def observed() -> float:
            return _observed_cost(ctx) if ctx.cfg.agent == "external" else ctx.spent_usd

        t0, spent0 = time.monotonic(), observed()
        try:
            result = fn(ctx)
        except Exception as error:
            failed = StageResult(
                stage=fn.__name__,
                status="failed",
                duration_s=round(time.monotonic() - t0, 3),
                cost_usd=round(max(observed() - spent0, 0.0), 6),
                error=str(error)[:2000],
            )
            _append_stage(ctx, failed)
            raise
        result.duration_s = round(time.monotonic() - t0, 3)
        result.cost_usd = round(max(observed() - spent0, 0.0), 6)
        _append_stage(ctx, result)
        return result

    return _owned(wrapper)


def _resumable(fn: Stage) -> Stage:
    """``_timed``, plus the idempotent re-run: a stage this run's host log already holds as
    completed returns ``_skipped`` of that record instead of running again."""

    @functools.wraps(fn)
    def resume(ctx: Ctx) -> StageResult:
        if prior := _done(ctx, fn.__name__):
            return _skipped(prior)
        return fn(ctx)

    return _timed(resume)


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


def seed_budget(ctx: Ctx, *, refresh: bool = False) -> float:
    """Seed persisted spend, then refresh under stage ownership when a retained context resumes.

    Another worker can advance this run while a CLI waits for approval. Once-per-process caching
    alone would let that CLI reuse an obsolete budget. Retain any in-process, unjournaled charge.
    """
    if not ctx.budget_seeded:
        ctx.spent_usd += _persisted_spend(ctx)
        ctx.budget_seeded = True
    elif refresh:
        persisted = _persisted_spend(ctx)
        # Every external charge is journalled. A trusted no-start reconciliation can
        # return funds even when this same context previously cached the reservation.
        ctx.spent_usd = persisted if ctx.cfg.agent == "external" else max(ctx.spent_usd, persisted)
    return ctx.spent_usd


def _persisted_spend(ctx: Ctx) -> float:
    """The most this run can be shown to have committed, from the two durable records of it.

    The call ledger is the authority: it holds every call, including the ones no stage record will
    ever describe because their stage died mid-call. It is therefore never below the stage log for
    a run that this code accounted for. The stage log stays a floor because a run that began before
    per-call accounting existed has cost only there, and dropping that would hand a resumed run
    money it already spent.
    """
    ledger = CallLedger(ctx.state)
    floor = ledger.floor_usd()
    if floor is None:
        # First contact between this ledger and the run. Everything the stage log holds right now
        # was spent by calls the ledger never saw, so it is adopted once as the floor. If the ledger
        # already has call rows (a run that predates the floor row itself), the ledger governs and
        # the floor is zero: summing the stage log on top would double-count those very calls.
        floor = ledger.adopt_floor(0.0 if ledger.records() else persisted_cost(ctx))
    return round(floor + ledger.charged_usd(), 6)


def unreconciled_spend(ctx: Ctx) -> float:
    """Money this run has committed to calls whose usage nobody has observed yet."""
    return CallLedger(ctx.state).unreconciled_usd()


def _agent(ctx: Ctx, stage: str, iteration: int, prompt: str, schema: type[BaseModel] | None) -> AgentResult:
    """Run the agent for one stage call, enforcing the run-level budget and surfacing errors.
    Under srt the kernel ``denyWrite`` set follows the stage (``protected_for``): ``fix`` calls
    lose write access to the tests dir that ``build`` needed, matching the ``Edit(...)`` rules."""
    if ctx.cfg.agent == "external" and isinstance(ctx.sb, DockerSandbox):
        # Reclaim an interrupted paid process even when its held grant exhausts the run.
        # Cleanup proves execution stopped; it does not release unknown provider usage.
        ctx.sb.recover_external()
        # A worker may have died after persisting a no-start proof but before reconciling the
        # ledger. Observe it before the ceiling check so a full reservation cannot hide recovery.
        recovering = CallLedger(ctx.state)
        for record in recovering.unreconciled():
            observation = ctx.sb.external_no_start_observation(record.call_id)
            if observation is not None:
                recovering.reconcile(record.call_id, observation)
    # Refresh, not seed: a call that died in THIS process left its reservation in the ledger and
    # nothing in `ctx.spent_usd`, so the cached counter would under-count until a restart.
    seed_budget(ctx, refresh=True)
    remaining = ctx.cfg.max_budget_usd - ctx.spent_usd
    if remaining <= 0:
        # Name the in-doubt money in the refusal: an operator who only sees "exhausted" has no way
        # to know the ceiling is held by calls nobody has observed, or that reconciling frees it.
        unknown = unreconciled_spend(ctx)
        detail = f" ({unknown:.2f} USD of it unreconciled)" if unknown else ""
        raise StageError("policy", f"run budget exhausted before {stage}.{iteration}{detail}")
    granted = min(ctx.cfg.max_budget_usd_per_stage, remaining)
    call_cfg = ctx.cfg.model_copy(update={"max_budget_usd_per_stage": granted})
    protected = protected_for(_contract(ctx), stage)
    policy = _policy(ctx, stage)
    external = ctx.agent.kind == "external"
    writable: tuple[str, ...] = ()
    admitted_digest = None
    if external:
        if ctx.blueprint and any(gate.mode == "policy" for gate in ctx.blueprint.gates):
            raise StageError("policy", "external agents are not qualified for autonomous policy gates")
        # Qualification and immutable binding are checked before reserving money or launching.
        from swfactory.external_agent import ExternalAgent

        if not isinstance(ctx.agent, ExternalAgent):
            raise StageError("policy", "external execution requires the factory's qualified launcher")
        ctx.agent.validate_binding()
        ctx.agent.validate_call(policy, stage)
        admitted = accepted_inputs.stored(ctx.state)
        if admitted is not None and admitted.policy_sha256 != accepted_inputs.policy_sha256(ctx.cfg, ctx.blueprint):
            raise StageError("policy", "external execution policy differs from accepted inputs; open a new epoch")
        contract = _contract(ctx)
        if policy.writes:
            writable = (contract.source, contract.tests_dir) if stage == "build" else (contract.source,)
        ctx.agent.validate_execution(ctx.sb, policy, writable, protected)
        admitted_digest = accepted_inputs.digest_of(ctx.state)
    if hasattr(ctx.sb, "set_protected"):
        ctx.sb.set_protected(protected)
    # Reserve BEFORE the provider can be paid. Everything after this line may be lost to a kill;
    # this row is what makes the next process account for a call it never saw answer.
    ledger = CallLedger(ctx.state)
    if (
        external
        and ctx.agent.profile.budget_mode == "hard_cap"
        and any(record.stage == stage and record.iteration == iteration for record in ledger.unreconciled())
    ):
        raise StageError("policy", "external paid attempt is unresolved; observe and reconcile it before retrying")
    attempt = ledger.reserve(stage=stage, iteration=iteration, reserved_usd=granted)
    invocation_args = {}
    if external:
        invocation_args["invocation"] = Invocation(
            call_id=attempt.call_id,
            accepted_inputs_digest=admitted_digest,
            writable_paths=writable,
        )
    try:
        if external:
            binding = ctx.agent.binding
            binding_json = json.dumps(
                binding, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
            )
            descriptor = {
                "schema_version": 1,
                "authority": "factory",
                "run_id": ctx.cfg.run_id,
                "call_id": attempt.call_id,
                "accepted_inputs_digest": admitted_digest,
                "profile": binding,
                "profile_binding_sha256": hashlib.sha256(binding_json.encode()).hexdigest(),
                "stage": stage,
                "iteration": iteration,
                "reserved_usd": granted,
                "sandbox": ctx.sb.name,
                "workdir": str(Path(ctx.sb.workdir).resolve()),
                "sandbox_labels": dict(ctx.sb.identity.labels) if isinstance(ctx.sb, DockerSandbox) else {},
                "created_at": datetime.now(UTC).isoformat(timespec="milliseconds"),
            }
            descriptor_path = "external-calls/" + hashlib.sha256(attempt.call_id.encode()).hexdigest() + ".json"
            descriptor_content = json.dumps(descriptor, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n"
            if ctx.state.has_control(descriptor_path):
                if ctx.state.read_control(descriptor_path) != descriptor_content:
                    raise StageError("policy", "external call descriptor is immutable and differs from this attempt")
            else:
                # The host descriptor survives a killed launcher before any result receipt
                # exists. It never enters agent-writable checkout or artifact state.
                ctx.state.write_control(descriptor_path, descriptor_content)
        res = ctx.agent.run(
            ctx.sb,
            stage=stage,
            iteration=iteration,
            prompt=prompt,
            policy=policy,
            schema=schema,
            cfg=call_cfg,
            issue_id=ctx.issue.id,
            protected=protected,
            **invocation_args,
        )
    except BaseException as error:
        # A timeout or a dead cell says nothing about the provider's ledger. The request left, so
        # the reservation stands as unknown spend until an observation resolves it; releasing it
        # here would let the run buy the same money twice.
        observation = (
            ctx.sb.external_no_start_observation(attempt.call_id)
            if external and isinstance(ctx.sb, DockerSandbox)
            else None
        )
        if observation is not None:
            ledger.reconcile(attempt.call_id, observation)
        else:
            ledger.mark_unknown(attempt, error)
        # Deliberately NOT `ctx.spent_usd += granted`. The ledger already carries the reservation as
        # unknown spend at full value, and `seed_budget(refresh=True)` at the top of the next call
        # reads it back. Adding it here as well pushed the phantom amount into the stage log via the
        # `_timed` wrapper's `spent_usd - spent0`, and that record has no reconciliation: after an
        # observation proved the provider never saw the request, the ledger charge fell to zero while
        # the stage log kept the run pinned at the reservation -- forever. Money that reconciliation
        # cannot return is not a reservation, it is a fine.
        raise
    receipt = {
        "session_id": res.session_id,
        "num_turns": res.num_turns,
        "duration_ms": res.duration_ms,
        "subtype": res.subtype,
        "is_error": res.is_error,
        "agent": res.agent,
    }
    # Candidate output cannot author billing. Paid external calls retain their full reservation
    # until an operator supplies a trusted provider observation through the existing reconciler.
    if res.cost_usd is None:
        ledger.mark_unknown(attempt, "external usage requires a trusted provider observation")
    else:
        ledger.settle(attempt, cost_usd=res.cost_usd, receipt=receipt)
        ctx.spent_usd += res.cost_usd
    if ctx.spent_usd > ctx.cfg.max_budget_usd:
        raise StageError(
            "policy",
            f"run budget exceeded: {ctx.spent_usd:.2f} > {ctx.cfg.max_budget_usd:.2f} USD after {stage}.{iteration}",
        )
    envelope = f"{ctx.art}/agent/{stage}.{iteration}.json"
    if external:
        host_receipt = {
            "schema_version": 1,
            "authority": "factory",
            "call_id": attempt.call_id,
            "accepted_inputs_digest": accepted_inputs.digest_of(ctx.state),
            "profile": ctx.agent.binding,
            "profile_id": ctx.agent.binding["manifest"]["id"],
            "stage": stage,
            "iteration": iteration,
            **res.model_dump(mode="json", exclude={"text", "data"}),
            "candidate": {k: v for k, v in (res.raw_envelope or {}).items() if k != "text"},
        }
        content = json.dumps(host_receipt, sort_keys=True, indent=2) + "\n"
        receipt_id = hashlib.sha256(attempt.call_id.encode()).hexdigest()
        ctx.state.write_control(f"external-agent/{receipt_id}.json", content)
        ctx.state.write_artifact(envelope, content)
        ctx.sb.write(envelope, content)
    elif ctx.sb.exists(envelope):
        ctx.state.write_artifact(envelope, ctx.sb.read(envelope))
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
    return dataclasses.replace(policy, allowed_tools=policy.allowed_tools + extra, model=override.model or policy.model)


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


def commit(ctx: Ctx, *, stage: str, msg: str, paths: Sequence[str] | None = None) -> str:
    """Commit everything under the target dir as the bot with provenance trailers (``.factory``
    is excluded by setup). Returns the HEAD sha; a no-op when there is nothing to commit."""
    _assert_workspace_head(ctx, f"{stage} commit")
    sha = commit_unfenced(ctx, stage=stage, msg=msg, paths=paths)
    _record_workspace_head(ctx, sha)
    return sha


def commit_unfenced(ctx: Ctx, *, stage: str, msg: str, paths: Sequence[str] | None = None) -> str:
    """``commit`` without the run's workspace-head fence, for a cell whose HEAD is not that head.

    A fork child (``swfactory.fork_wave``) is the one caller: its checkout starts at the parent's
    checkpoint and is fenced against that, and recording its HEAD as the run's would move the
    parent's fence to a commit the parent does not have.
    """
    if paths:
        _sh(ctx, "git add -A -- " + " ".join(shlex.quote(path) for path in paths))
    else:
        _sh(ctx, GIT_ADD_ALL)
        # Intent, gates and agent receipts are committed once by deliver, after the review stream.
        _sh(ctx, f"git reset -q -- {shlex.quote(ctx.art)}")
    if ctx.sb.run("git diff --cached --quiet").ok:
        return _workspace_head(ctx)
    q = shlex.quote
    cmd = (
        f"{_GIT_BOT} -c commit.gpgsign=false "
        f"commit -q -m {q(msg)} "
        f"--trailer {q(f'Factory-Run={ctx.cfg.run_id}')} "
        f"--trailer {q(f'Factory-Stage={stage}')} "
        # Which factory instance produced this commit. It travels with the commit, so the REMOTE
        # can answer "is this mine" when several instances share one branch -- no bookkeeping on
        # either side, and the answer survives a restart because it was never in memory.
        f"--trailer {q(f'Factory-Instance={ctx.instance}')} "
        f"--trailer {q(f'Agent={ctx.agent.kind}')} "
    )
    if ctx.agent.kind != "external":
        cmd += f"--trailer {q('Co-Authored-By: Claude <noreply@anthropic.com>')}"
    _sh(ctx, cmd)
    return _workspace_head(ctx)


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
    legacy_output = (res.stdout[-6000:] + "\n" + res.stderr[-2000:]).strip()
    cases: dict[str, str] | None = None
    try:
        report = ctx.sb.read(contract.junit)
        counts = _parse_junit(report)
    except (FileNotFoundError, ET.ParseError, ValueError):
        result = TestResult(exit_code=res.exit_code or 1, junit_path=None, report_valid=False)
    else:
        cases = _junit_cases(report)
        result = _with_cases(
            TestResult(
                **counts,
                exit_code=res.exit_code,
                junit_path=contract.junit,
                report_valid=True,
            ),
            cases,
        )
    output = legacy_output
    if not result.ok:
        from swfactory.harness_efficiency import pack_failure_observation

        packed = pack_failure_observation(
            ctx,
            command=cmd,
            exit_code=res.exit_code,
            timed_out=res.timed_out,
            stdout=res.stdout,
            stderr=res.stderr,
        )
        if packed is not None:
            output = packed.prompt_text
    head = _assert_workspace_head(ctx, "verification")
    _ensure_clean(ctx, "verification", allow_artifacts=True)
    # Host-owned and bound to the HEAD it ran on: a reader holding a different HEAD has stale
    # evidence and must say so rather than judge the wrong commit.
    ctx.state.write_control(
        JUNIT_CASES,
        json.dumps({"head": head, "report_valid": result.report_valid, "cases": cases}, sort_keys=True) + "\n",
    )
    return result, output


# Control record of the latest suite run: ``{head, report_valid, cases}`` (cases may be null).
JUNIT_CASES = "junit-cases.json"


def junit_cases_for(ctx: Ctx, head: str) -> dict[str, str] | None:
    """The per-test outcomes ``run_tests`` recorded for exactly ``head``, or None if it did not."""
    if not ctx.state.has_control(JUNIT_CASES):
        return None
    record = json.loads(ctx.state.read_control(JUNIT_CASES))
    if record.get("head") != head or not record.get("report_valid"):
        return None
    cases = record.get("cases")
    return dict(cases) if isinstance(cases, dict) else None


def crabbox_command(provider: str, junit: str, test_cmd: str) -> str:
    """``crabbox run`` wrapper for the target's test command. The junit file comes back through
    ``-download <junit>=<junit>`` (never ``-artifact-glob``: SSH-lease only); in-place providers
    (``IN_PLACE_PROVIDERS``) already leave it in the working tree, so no download is requested."""
    q = shlex.quote
    download = "" if provider in IN_PLACE_PROVIDERS else f"-download {q(f'{junit}={junit}')} "
    return f"crabbox run -provider {q(provider)} -junit {q(junit)} {download}-ttl 45m -idle-timeout 15m -- {test_cmd}"


def _parse_junit(xml_text: str) -> dict[str, int]:
    root = ET.fromstring(xml_text)
    summary_keys = ("tests", "failures", "errors", "skipped")
    outcomes = ("passed", "failed", "errors", "skipped")
    containers = {"testsuite", "testsuites"}
    statuses = {"failure", "error", "skipped"}
    evidence_tags = containers | statuses | {"testcase"}

    def local_name(element: ET.Element) -> str:
        return element.tag.rsplit("}", 1)[-1]

    def count(element: ET.Element) -> tuple[dict[str, int], dict[str, int]]:
        is_case = local_name(element) == "testcase"
        allowed = statuses if is_case else containers | {"testcase"}
        children = []
        for child in element:
            if local_name(child) in allowed:
                children.append(child)
            elif any(local_name(descendant) in evidence_tags for descendant in child.iter()):
                raise ValueError("JUnit report contains misplaced test evidence")

        if is_case:
            if any(
                local_name(descendant) in evidence_tags
                for child in children
                for descendant in child.iter()
                if descendant is not child
            ):
                raise ValueError("JUnit testcase contains nested test evidence")
            present = {local_name(child) for child in children}
            if (statuses & element.attrib.keys()) - present or {"failure", "error"} <= present:
                raise ValueError("JUnit testcase contains ambiguous outcomes")
            failed, errors, skipped = (int(tag in present) for tag in ("failure", "error", "skipped"))
            raw = {"tests": 1, "failures": failed, "errors": errors, "skipped": skipped}
            # Node TODO failures also carry <skipped>; one failed case is not two tests.
            result = {
                "passed": int(not present),
                "failed": failed,
                "errors": errors,
                "skipped": int(skipped and not failed and not errors),
            }
            return result, raw

        declared = {key: int(element.attrib[key]) for key in summary_keys if key in element.attrib}
        if any(value < 0 for value in declared.values()):
            raise ValueError("JUnit report contains invalid test counts")
        if children:
            parsed = [count(child) for child in children]
            result = {key: sum(counts[key] for counts, _ in parsed) for key in outcomes}
            raw = {key: sum(summary[key] for _, summary in parsed) for key in summary_keys}
            # Reporters summarize either all descendant cases or immediate children, as Node does.
            direct = {key: sum(bool(summary[key]) for _, summary in parsed) for key in summary_keys}
            direct["tests"] = len(children)
            if not any(all(summary[key] == value for key, value in declared.items()) for summary in (raw, direct)):
                raise ValueError("JUnit summary disagrees with its test evidence")
            return result, raw

        if local_name(element) != "testsuite" or "tests" not in declared:
            raise ValueError("JUnit report contains no test evidence")
        raw = {key: declared.get(key, 0) for key in summary_keys}
        total, failed, errors, skipped = (raw[key] for key in summary_keys)
        if failed + errors + skipped > total:
            raise ValueError("JUnit report result counts exceed its test count")
        return dict(zip(outcomes, (total - failed - errors - skipped, failed, errors, skipped), strict=True)), raw

    if local_name(root) not in containers:
        raise ValueError("JUnit report contains no testsuite or testsuites root")
    result, _ = count(root)
    if sum(result.values()) <= 0:
        raise ValueError("JUnit report contains no tests")
    return result


# Beyond this many non-passing ids a result keeps counts only: a suite that failed wholesale (an
# import error) says nothing a per-test comparison could use, and receipts stay small.
MAX_NONPASSING_CASES = 1000


def _junit_cases(xml_text: str) -> dict[str, str] | None:
    """Every test case of an already-validated report, ``id -> passed|failed|error|skipped``.

    The id is ``<suite>/.../<classname>::<name>``: Node reporters reuse one classname across nested
    suites, so the suite path is what keeps two cases apart. Outcomes follow ``_parse_junit``'s
    rules (a Node TODO failure that also carries ``<skipped>`` is a failure). None when the report
    is summary-only or two cases share an id -- a partial or ambiguous set must never be compared as
    if it were the whole suite.
    """
    root = ET.fromstring(xml_text)
    cases: dict[str, str] = {}

    def local_name(element: ET.Element) -> str:
        return element.tag.rsplit("}", 1)[-1]

    def walk(element: ET.Element, path: tuple[str, ...]) -> bool:
        for child in element:
            kind = local_name(child)
            if kind in ("testsuite", "testsuites"):
                if not walk(child, (*path, child.attrib.get("name", ""))):
                    return False
            elif kind == "testcase":
                scope = "/".join((*path, child.attrib.get("classname", "")))
                case_id = f"{scope}::{child.attrib.get('name', '')}"
                present = {local_name(status) for status in child}
                outcome = (
                    "failed"
                    if "failure" in present
                    else "error"
                    if "error" in present
                    else "skipped"
                    if "skipped" in present
                    else "passed"
                )
                if case_id in cases:
                    return False
                cases[case_id] = outcome
        return True

    start = (root.attrib.get("name", ""),) if local_name(root) == "testsuite" else ()
    if not walk(root, start) or not cases:
        return None
    return cases


def case_name(case_id: str) -> str:
    """The test's own name in a ``_junit_cases`` id, without a pytest ``[param]`` suffix."""
    return case_id.rsplit("::", 1)[-1].split("[", 1)[0]


def cases_digest(cases: dict[str, str]) -> str:
    """The identity of a test universe: which ids exist, regardless of how they came out."""
    return hashlib.sha256("\n".join(sorted(cases)).encode()).hexdigest()


def _with_cases(result: TestResult, cases: dict[str, str] | None) -> TestResult:
    if cases is None:
        return result
    nonpassing = tuple(sorted(case for case, outcome in cases.items() if outcome != "passed"))
    if len(nonpassing) > MAX_NONPASSING_CASES:
        return result
    return result.model_copy(update={"nonpassing_cases": nonpassing, "cases_digest": cases_digest(cases)})


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
        "tests_count": float(tr.total),
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
        raise ValueError("a local root target must run from the target checkout containing factory.toml")
    shutil.copytree(src, workdir, ignore=COPY_IGNORE, dirs_exist_ok=True)
    return True


def _seed_exclude(ctx: Ctx) -> None:
    """Append NEVER_COMMITTED to ``.git/info/exclude`` through ``sb.write`` (host-side for local,
    srt and docker; ``islo cp`` for islo) — never through a confined shell, which srt may deny
    for anything under ``.git/``.

    Sandbox Runtime bind-mounts special placeholders over missing root dotfiles on Linux. Discover
    only non-regular entries before any agent runs, then exclude their safe basenames. Regular
    project files, directories and symlinks never enter this dynamic list.
    """
    git_dir = ctx.sb.run("git rev-parse --git-dir").stdout.strip() or ".git"
    path = f"{git_dir}/info/exclude"
    try:
        current = ctx.sb.read(path)
    except FileNotFoundError:
        current = ""
    scanned = ctx.sb.run(SPECIAL_DOTFILE_SCAN)
    special = [
        name
        for name in scanned.stdout.splitlines()
        if scanned.ok and name.startswith(".") and name not in {".", ".."} and "/" not in name and "\\" not in name
    ]
    if isinstance(ctx.sb, SrtSandbox):
        special.extend(path for path in SRT_RUNTIME_PROTECTED if not ctx.sb.exists(path))
    have = set(current.splitlines())
    missing = [p for p in dict.fromkeys((*NEVER_COMMITTED, *special)) if p not in have]
    if missing:
        sep = "" if not current or current.endswith("\n") else "\n"
        ctx.sb.write(path, current + sep + "\n".join(missing) + "\n")


@_owned
def setup(ctx: Ctx) -> StageResult:
    """Prepare the sandbox: repo, bot identity, baseline, work branch, deps, base sha, contract."""
    t0 = time.monotonic()
    sb = ctx.sb
    # One definition of "the execution policy", shared with the epoch pin: two lists here and in
    # accepted_inputs would drift, and a knob present in only one of them is fenced by neither.
    identity = _dumps(
        {
            "schema": 3,  # bumped with the policy document: schema 2 hashed a narrower policy
            "run_id": ctx.cfg.run_id,
            "blueprint": ctx.cfg.blueprint,
            "issue_id": ctx.issue.id,
            "repo": ctx.cfg.repo,
            "target_dir": ctx.cfg.target_dir,
            "base_branch": ctx.cfg.base_branch,
            "policy_sha256": accepted_inputs.policy_sha256(ctx.cfg, ctx.blueprint),
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
        ctx.state.write_control("started", datetime.now(UTC).isoformat(timespec="seconds") + "\n")
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
            ctx.state.read_control("workspace-head").strip() if ctx.state.has_control("workspace-head") else ""
        )
        progressed = any(record.stage in {"build_and_test", "review", "deliver"} for record in _persisted(ctx))
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
            raise StageError("sandbox", f"uv sync failed: {res.stderr.strip()[-800:]}", retryable=True)
    _contract(ctx)
    _review_policy(ctx)
    duration = round(time.monotonic() - t0, 3)
    ctx.state.write_control("setup-timing.json", _dumps({"duration_s": duration}))
    return StageResult(stage="setup", duration_s=duration)


@_resumable
def intent(ctx: Ctx) -> StageResult:
    """No agent: the originator's words, verbatim, under a small front matter."""
    path = f"{ctx.art}/intent.md"
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


@_resumable
def spec(ctx: Ctx) -> StageResult:
    """Agent (read-only) turns intent.md into spec.md."""
    path = f"{ctx.art}/spec.md"
    prompt = render_prompt("spec", issue_id=ctx.issue.id, intent=ctx.read_artifact(f"{ctx.art}/intent.md"))
    res = _agent(ctx, "spec", 1, prompt, None)
    if not res.text.strip():
        raise StageError("agent", "spec returned empty text")
    ctx.write_artifact(path, _document_only(res.text) + "\n")
    return StageResult(stage="spec", artifacts=[path])


@_resumable
def plan(ctx: Ctx) -> StageResult:
    """Agent (read-only) produces a typed Plan -> plan.json + rendered plan.md."""
    json_path, md_path = f"{ctx.art}/plan.json", f"{ctx.art}/plan.md"
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
    # Advisory and refusing nothing: the approver sees which requirement no planned test proves
    # before any code is written. The table is kept whole and the artifact head shortened, so a
    # long plan cannot push the coverage out of the bounded preview.
    rows = requirement_trace(_read_or(ctx, f"{ctx.art}/spec.md"), p.model_dump(), None)
    coverage = coverage_table(rows)[: PREVIEW_CHARS // 2]
    trace = trace_numbers(rows)
    return StageResult(
        stage="plan",
        artifacts=[md_path, json_path],
        numbers={
            "files": float(len(p.files)),
            "requirements": trace["requirements"],
            "untested_requirements": trace["untested_requirements"],
        },
        preview=preview[: PREVIEW_CHARS - len(coverage) - 1].rstrip("\n") + "\n" + coverage,
    )


def _summary_line(res: AgentResult, fallback: str) -> str:
    try:
        text = BuildSummary.model_validate(res.data).summary.strip().splitlines()[0]
    except (ValueError, IndexError):
        text = ""
    return (text or fallback)[:72]


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
    changed = set(_sh(ctx, f"git diff --name-only --relative {base}..HEAD -- . {_exclude(ctx)}").split())
    tests_dir = _contract(ctx).tests_dir.rstrip("/") + "/"
    findings = [
        Finding(
            severity="major",
            file=f,
            title="Plan fidelity: file not listed in plan.md",
            detail=f"`{f}` was changed but plan.md does not list it. Add it to the plan or revert the change.",
        )
        for f in sorted(changed - planned)
    ]
    findings += [
        Finding(
            severity="major" if f.startswith(tests_dir) else "minor",
            file=f,
            title="Plan fidelity: planned file not touched",
            detail=f"plan.md lists `{f}` but the diff never touches it. Do the planned work or drop it from the plan.",
        )
        for f in sorted(planned - changed)
    ]
    return findings


# ---------------------------------------------------------------- requirement trace
#
# VOLO's step 2: acceptance tests derived from the requirements must exist, run and pass -- and
# that is checked by code, not left to the reviewer's judgement. The spec numbers requirements, the
# plan maps each to the exact test that proves it (``R2: test_increase — ...``), and ``run_tests``
# records per-case JUnit outcomes bound to the HEAD they ran on. ``requirement_trace`` joins the
# three; review turns every planned acceptance test that is not passing into a blocker. A
# requirement the plan maps to no test is advisory only (recorded, never a finding): every recorded
# fixture leaves its docstring requirement untested, and a missing test is a planning gap the plan
# gate shows the approver, not a defect of the diff under review.

TRACE = "trace.json"  # {art}/trace.json: requirement -> planned acceptance tests -> JUnit outcome

_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")
_REQUIREMENTS_TITLE = re.compile(r"\brequirements?\b", re.IGNORECASE)
# ``R3. ...`` / ``R3: ...`` / ``- **R3** — ...`` as prompts/spec.md asks, or a bare ``3. ...``
# ordered-list item, which is what every recorded fixture writes. Both name requirement R3.
_SPEC_TAGGED = re.compile(
    r"^\s{0,3}(?:[-*+]\s+)?(?:\*\*|__)?R(\d+)(?:\*\*|__)?\s*(?:[.:)]|\s[—–-])?(?:\*\*|__)?\s+(\S.*)$"
)
_SPEC_NUMBERED = re.compile(r"^\s{0,3}(\d+)[.)]\s+(\S.*)$")
# A plan test entry: ``R2: test_x — ...``, ``R2, R3: test_x``, ``R1-R3: test_x``, ``R2 - test_x``.
_PLAN_TAGS = re.compile(
    r"^\s*(?:[-*+]\s+)?\(?(R\d+(?:\s*(?:,|/|&|\+|\band\b|\.\.|[-–])\s*R\d+)*)\)?\s*(?::|\)|\]|\s[—–-])\s*(.*)$"
)
_PLAN_TAG = re.compile(r"R(\d+)(?:\s*(?:\.\.|[-–])\s*R(\d+))?")
_TEST_TOKEN = re.compile(r"^[\w./:\[\]-]+")
# A bare name counts as a planned test only when it is shaped like one; prose after a tag
# (``R1: percent_change is exported``) must not turn into a "missing test" blocker. Free-form
# runner names (Node) are written qualified and quoted: ``R1: `suite::adds two numbers` ``.
_TEST_IDENTIFIER = re.compile(r"(?:[Tt]est[A-Z0-9_]\w*|\w+_test|\w+Test)")
# Worst first: a requirement's status is the worst status among its planned tests.
TRACE_STATUSES: tuple[str, ...] = ("failed", "missing", "skipped", "unverified", "passed")
_TRACE_BLOCKING = frozenset({"failed", "missing", "skipped"})
_TRACE_MAX_CASES = 20  # case ids kept per planned test in trace.json (parametrized tests fan out)


def _requirement_id(number: str) -> str:
    return f"R{int(number)}"


def spec_requirements(spec_text: str) -> dict[str, str]:
    """``{"R1": text, ...}`` in spec order, read from every heading whose title names requirements.

    Continuation lines (wrapped or indented under an item) join their item's text; the first
    occurrence of an id wins, so a nested ordered sub-list cannot rename a requirement.
    """
    found: dict[str, str] = {}
    level = 0  # > 0 while inside a requirements section opened by a heading of that depth
    current: str | None = None
    after_blank = False
    for line in spec_text.splitlines():
        heading = _HEADING.match(line)
        if heading:
            depth = len(heading.group(1))
            current = None
            if level and depth > level:
                continue  # a sub-heading inside the requirements section
            level = depth if _REQUIREMENTS_TITLE.search(heading.group(2)) else 0
            continue
        if not level:
            continue
        if not line.strip():
            after_blank = True
            continue
        item = _SPEC_TAGGED.match(line) or _SPEC_NUMBERED.match(line)
        if item:
            rid = _requirement_id(item.group(1))
            current = None if rid in found else rid
            if current is not None:
                found[current] = item.group(2).strip()
        elif current is not None and (not after_blank or line[:1].isspace()):
            found[current] = f"{found[current]} {line.strip()}"
        else:
            current = None
        after_blank = False
    return found


def _planned_test_name(rest: str) -> str | None:
    """The test named first after a requirement tag, or None when the entry names no test."""
    rest = rest.strip()
    if rest.startswith("`"):
        end = rest.find("`", 1)
        token = rest[1:end].strip() if end > 0 else ""
    else:
        match = _TEST_TOKEN.match(rest)
        token = match.group(0).rstrip(".:-") if match else ""
    token = token.removesuffix("()")
    name = token.rpartition("::")[2].split("[", 1)[0].strip()
    if not name:
        return None
    return token if "::" in token or _TEST_IDENTIFIER.fullmatch(name) else None


def plan_test_map(plan_data: object) -> dict[str, list[str]]:
    """``{"R2": ["test_increase", ...]}`` from plan.json ``tests`` and every work node's ``tests``.

    Entries are ``R<n>: <exact test name> — <what it proves>`` (prompts/plan.md); several
    requirements may share one test (``R2, R3: test_x``) and ranges expand (``R1-R3``). Entries
    without a tag, or whose first word is not a test name, map nothing.
    """
    if not isinstance(plan_data, dict):
        return {}
    entries = list(plan_data.get("tests") or [])
    for node in plan_data.get("work") or []:
        if isinstance(node, dict):
            entries += list(node.get("tests") or [])
    mapped: dict[str, list[str]] = {}
    for entry in entries:
        if not isinstance(entry, str) or not (tagged := _PLAN_TAGS.match(entry)):
            continue
        test = _planned_test_name(tagged.group(2))
        if test is None:
            continue
        for tag in _PLAN_TAG.finditer(tagged.group(1)):
            first = int(tag.group(1))
            last = int(tag.group(2)) if tag.group(2) else first
            numbers = range(first, last + 1) if 0 <= last - first <= 50 else (first, last)
            for number in numbers:
                tests = mapped.setdefault(_requirement_id(str(number)), [])
                if test not in tests:
                    tests.append(test)
    return mapped


def _name_tokens(text: str) -> list[str]:
    """Path-ish segments of a test qualifier or case scope: ``tests/test_x.py::TestA`` and the
    pytest scope ``pytest/tests.test_x.TestA`` both end in ``tests, test_x, TestA``."""
    text = re.sub(r"\.(?:py|[cm]?[jt]sx?)(?=$|::|/)", "", text)
    return [part.strip() for part in re.split(r"::|[/\\.]", text) if part.strip()]


def matching_cases(test: str, cases: dict[str, str]) -> list[str]:
    """JUnit case ids (``_junit_cases``) that a planned test name denotes.

    The name part must equal the case's name exactly -- without its pytest ``[param]`` suffix
    (``case_name``) unless the plan names a parameter itself. A ``file::name`` or
    ``module::Class::name`` qualifier must be a suffix of the case's scope, so
    ``tests/test_x.py::test_y`` matches ``pytest/tests.test_x::test_y``.
    """
    qualifier, _, name = test.rpartition("::")
    want = _name_tokens(qualifier)
    found = []
    for case_id in sorted(cases):
        scope, _, raw = case_id.rpartition("::")
        if (raw if "[" in name else case_name(case_id)) != name:
            continue
        if want:
            have = _name_tokens(scope)
            if len(want) > len(have) or have[-len(want) :] != want:
                continue
        found.append(case_id)
    return found


def _planned_outcome(test: str, cases: dict[str, str] | None) -> dict[str, Any]:
    if cases is None:
        return {"test": test, "status": "unverified", "cases": [], "case_count": 0}
    matched = matching_cases(test, cases)
    outcomes = {cases[case_id] for case_id in matched}
    status = (
        "missing"
        if not matched
        else "failed"
        if outcomes & {"failed", "error"}
        else "skipped"
        if "skipped" in outcomes
        else "passed"
    )
    return {"test": test, "status": status, "cases": matched[:_TRACE_MAX_CASES], "case_count": len(matched)}


def requirement_trace(spec_text: str, plan_data: object, cases: dict[str, str] | None) -> list[dict[str, Any]]:
    """One row per requirement: ``{requirement, text, in_spec, planned_tests, status}``.

    Pure. Requirements come from the spec in its order, then any the plan tags that the spec does
    not name. ``planned_tests`` holds ``{test, status, cases, case_count}`` per planned test, where
    status is ``passed|failed|skipped|missing`` against ``cases`` (a failure or error is
    ``failed``; one non-passing parametrized case makes the test non-passing) or ``unverified``
    when ``cases`` is None. The row's status is its worst test's (``TRACE_STATUSES``), or
    ``untested`` when the plan maps no test to it.
    """
    requirements = spec_requirements(spec_text)
    planned = plan_test_map(plan_data)
    ids = list(requirements) + sorted(set(planned) - set(requirements), key=lambda rid: int(rid[1:]))
    rows: list[dict[str, Any]] = []
    for rid in ids:
        tests = [_planned_outcome(test, cases) for test in planned.get(rid, [])]
        status = min((t["status"] for t in tests), key=TRACE_STATUSES.index) if tests else "untested"
        rows.append(
            {
                "requirement": rid,
                "text": requirements.get(rid, ""),
                "in_spec": rid in requirements,
                "planned_tests": tests,
                "status": status,
            }
        )
    return rows


def _planned_by_test(rows: list[dict[str, Any]]) -> dict[str, tuple[str, list[str]]]:
    """``test -> (status, [requirement ids])``: one entry per planned test, however many it proves."""
    by_test: dict[str, tuple[str, list[str]]] = {}
    for row in rows:
        for planned in row["planned_tests"]:
            by_test.setdefault(planned["test"], (planned["status"], []))[1].append(row["requirement"])
    return by_test


def trace_numbers(rows: list[dict[str, Any]]) -> dict[str, float]:
    by_test = _planned_by_test(rows)
    return {
        "requirements": float(len(rows)),
        "untested_requirements": float(sum(row["status"] == "untested" for row in rows)),
        "acceptance_tests": float(len(by_test)),
        # planned acceptance tests without a passing case for the HEAD (missing/skipped/failed/unverified)
        "missing_acceptance_tests": float(sum(status != "passed" for status, _ in by_test.values())),
    }


def trace_findings(rows: list[dict[str, Any]], *, head: str, tests_dir: str, evidence: bool) -> list[Finding]:
    """Blockers for planned acceptance tests that did not run and pass at ``head``.

    Without per-test evidence for ``head`` (``evidence=False``) nothing planned can be verified,
    so the one finding is that the evidence is missing: fail closed, never "no news is good news".
    A plan that maps no test has nothing to verify and yields nothing here (its rows are advisory).
    """
    by_test = _planned_by_test(rows)
    if not by_test:
        return []
    if not evidence:
        return [
            Finding(
                severity="blocker",
                file=tests_dir,
                title="No per-test JUnit evidence for the reviewed HEAD",
                detail=(
                    f"plan.json maps {len(by_test)} acceptance test(s) to requirements, but no per-test "
                    f"JUnit results were recorded for HEAD {head[:12]} (missing, stale, summary-only or "
                    "ambiguous report). Acceptance tests cannot be verified, so review fails closed."
                ),
            )
        ]
    findings = []
    for test, (status, requirements) in by_test.items():
        if status not in _TRACE_BLOCKING:
            continue
        what = {
            "missing": "has no such test case",
            "skipped": "reports it skipped, and a skipped test proves nothing",
            "failed": "reports it failed or errored",
        }[status]
        findings.append(
            Finding(
                severity="blocker",
                file=tests_dir,
                title=f"Acceptance test {status}: {test}",
                detail=(
                    f"{', '.join(requirements)} is planned to be proven by `{test}`, but the JUnit report "
                    f"for HEAD {head[:12]} {what}. The requirement stays unproven until that test runs "
                    "and passes."
                ),
            )
        )
    return findings


def contract_checklist(rows: list[dict[str, Any]], plan_data: object = None) -> str:
    """The review prompt's ``## Contract checklist``: each requirement with its trace status."""
    if not rows:
        tests = [str(t) for t in (plan_data or {}).get("tests") or []] if isinstance(plan_data, dict) else []
        listed = "".join(f"\n- {t}" for t in tests)
        return "No numbered requirements were found in the spec or the plan." + (
            f"\nThe plan's tests:{listed}" if listed else ""
        )
    lines = []
    for row in rows:
        text = (row["text"] or "(not in the spec; tagged by the plan)")[:400]
        evidence = "; ".join(f"`{t['test']}` {t['status']}" for t in row["planned_tests"]) or "no planned test"
        lines.append(f"- [{row['status']}] {row['requirement']}: {text} — {evidence}")
    return "\n".join(lines)


def coverage_table(rows: list[dict[str, Any]]) -> str:
    """The plan gate's advisory coverage: which planned test proves each requirement."""
    lines = ["", "## Requirement coverage (advisory)"]
    if not rows:
        lines.append("- no numbered requirements found in the spec or in the plan's tests")
    for row in rows:
        tests = ", ".join(t["test"] for t in row["planned_tests"]) or "no planned test"
        note = "" if row["in_spec"] else " (not in the spec)"
        lines.append(f"- {row['requirement']}: {tests}{note}")
    return "\n".join(lines) + "\n"


class ContractTrace(NamedTuple):
    """One review round's trace: the findings, numbers, prompt checklist and trace.json record."""

    findings: list[Finding]
    numbers: dict[str, float]
    checklist: str
    record: dict[str, Any]


def contract_trace(ctx: Ctx, spec_text: str, plan_data: object) -> ContractTrace:
    """The requirement trace for the HEAD under review, from the JUnit cases ``run_tests``
    recorded for exactly that HEAD (``junit_cases_for``); any other evidence is stale."""
    head = _assert_workspace_head(ctx, "requirement trace")
    cases = junit_cases_for(ctx, head)
    rows = requirement_trace(spec_text, plan_data, cases)
    numbers = trace_numbers(rows)
    record = {
        "schema_version": 1,
        "head": head,
        "evidence": "junit" if cases is not None else "missing",
        "summary": {key: int(value) for key, value in numbers.items()},
        "requirements": rows,
    }
    findings = trace_findings(rows, head=head, tests_dir=_contract(ctx).tests_dir, evidence=cases is not None)
    return ContractTrace(findings, numbers, contract_checklist(rows, plan_data), record)


def _review_policy(ctx: Ctx) -> str:
    if ctx.state.has_control("review-policy.md"):
        return ctx.state.read_control("review-policy.md")
    policy = ctx.blueprint.review.policy if ctx.blueprint else ReviewSpec().policy
    text = _read_or(ctx, policy)
    if not text.strip():
        packaged = FACTORY_ROOT / policy
        if not packaged.is_file():
            raise StageError("policy", f"review policy not found: {policy}")
        text = packaged.read_text(encoding="utf-8")
    ctx.state.write_control("review-policy.md", text)
    return text


def _format_findings(findings: list[Finding]) -> str:
    return "\n".join(f"- [{f.severity}] {f.file}:{f.line or '-'} {f.title} — {f.detail}" for f in findings)


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


def review_context(
    ctx: Ctx, base: str, numbers: dict[str, float], *, fanout: int = 1, saved_key: str
) -> tuple[str, PackedReviewDiff | None]:
    """The diff one review round reads, packed when the harness can, and the packed diff itself.

    Adds the round's byte counters to ``numbers``; ``saved_key`` names the saving, which is
    ``saved_bytes_per_prompt x fanout`` (the per-prompt saving at the default fanout of 1).
    """
    diff = _sh(ctx, f"git diff {base}..HEAD -- . {_exclude(ctx)}")
    from swfactory.harness_efficiency import pack_review_diff

    packed = pack_review_diff(
        ctx,
        diff=diff,
        base_sha=base,
        head_sha=_assert_workspace_head(ctx, "review context"),
        fanout=fanout,
    )
    if packed is None:
        return diff, None
    for key, attribute in (
        ("review_context_source_bytes", "source_bytes"),
        ("review_context_prompt_bytes", "prompt_bytes"),
        (saved_key, "estimated_replayed_bytes_avoided"),
    ):
        numbers[key] = numbers.get(key, 0.0) + float(getattr(packed, attribute))
    return packed.prompt_text, packed


def fix_and_retest(ctx: Ctx, prompt: str, *, fixes: int, fallback: str, numbers: dict[str, float]) -> Finding | None:
    """Review fix ``fixes``: agent call ``fix.<max_build_iterations + fixes>``, commit, re-run the
    suite. Returns the ``_tests_blocker`` a red suite leaves, None when it is green."""
    res = _agent(ctx, "fix", ctx.cfg.max_build_iterations + fixes, prompt, BuildSummary)
    commit(ctx, stage="fix", msg=f"fix: {_summary_line(res, fallback)}")
    tr, output = run_tests(ctx)
    numbers.update(_test_numbers(tr))
    return None if tr.ok else _tests_blocker(ctx, tr, output)


@_resumable
def review(ctx: Ctx) -> StageResult:
    """Agent review (Review schema) with the nit cap and plan fidelity enforced in code; blockers
    trigger at most ``max_review_fixes`` fix + test + re-review rounds. A fix that leaves the
    suite red is itself a blocker (``_tests_blocker``): the stage cannot end ``ok`` on red tests.
    Review fixes are agent calls ``fix.<max_build_iterations + k>`` (see module docstring)."""
    path = f"{ctx.art}/review.json"
    _assert_workspace_head(ctx, "review")
    base = ctx.state.read_control("base").strip()
    spec_text = _read_or(ctx, f"{ctx.art}/spec.md")  # "(none)" when the line has no spec stage
    plan_text = ctx.read_artifact(f"{ctx.art}/plan.md")
    plan_data = _read_json(ctx, f"{ctx.art}/plan.json", None)
    trace_path = f"{ctx.art}/{TRACE}"
    numbers: dict[str, float] = {}
    rv, dropped, fixes = Review(verdict="approve"), 0, 0
    tests_blocker: Finding | None = None
    trace: ContractTrace | None = None
    for k in range(ctx.cfg.max_review_fixes + 1):
        review_diff, _packed = review_context(ctx, base, numbers, saved_key="review_context_saved_bytes")
        # Recomputed every round: a review fix moves HEAD, and only that HEAD's cases count.
        trace = contract_trace(ctx, spec_text, plan_data)
        prompt = render_prompt(
            "review",
            issue_id=ctx.issue.id,
            review_policy=_review_policy(ctx),
            spec=spec_text,
            plan=plan_text,
            diff=review_diff,
            checklist=trace.checklist,
        )
        res = _agent(ctx, "review", k + 1, prompt, Review)
        rv, dropped = cap_nits(Review.model_validate(res.data), _nit_cap(ctx))
        rv.findings.extend(_plan_fidelity(ctx, base))
        rv.findings.extend(trace.findings)
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
        tests_blocker = fix_and_retest(
            ctx, fix_prompt, fixes=fixes, fallback="address review blockers", numbers=numbers
        )
    record = {
        "verdict": "request_changes" if rv.blockers else "approve",  # REVIEW.md contract
        "findings": [f.model_dump() for f in rv.findings],
        "dropped_nits": dropped,
        "fixes": fixes,
    }
    ctx.write_artifact(path, _dumps(record))
    if trace is None:  # max_review_fixes >= 0, so the loop always runs; never publish untraced
        raise StageError("policy", "review ran no round, so it has no requirement trace")
    ctx.write_artifact(trace_path, _dumps(trace.record))
    numbers.update(trace.numbers)
    counts = metrics_mod.severity_counts(rv.findings)
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
    return StageResult(stage="review", status=status, artifacts=[path, trace_path], numbers=numbers)


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


def cell_evidence(ctx: Ctx) -> tuple[str | None, int | None, bool]:
    """``(cell_id, epoch, managed)`` from host-owned control state; all-``None`` when unbound.

    ``runtime.build_ctx`` writes ``cell.json`` outside the sandbox, so this is the run's own view of
    which Cell epoch it is executing as -- not something a mapped job dict or an agent can restate.
    """
    if not ctx.state.has_control("cell.json"):
        return None, None, False
    try:
        data = json.loads(ctx.state.read_control("cell.json"))
    except ValueError as error:
        raise StageError("policy", f"stored Factory Cell binding is invalid: {error}") from error
    if not isinstance(data, dict):
        raise StageError("policy", "stored Factory Cell binding is invalid")
    cell_id, epoch = data.get("cell_id"), data.get("epoch")
    return (
        str(cell_id) if isinstance(cell_id, str) else None,
        int(epoch) if isinstance(epoch, int) else None,
        bool(data.get("managed")),
    )


@_owned
def record_approval(ctx: Ctx, approval: Approval) -> None:
    """Record the latest decision for one gate in ``approvals.json`` (committed by deliver).

    Airflow may retry a record task, so one gate owns one entry rather than accumulating duplicate
    decisions from task attempts.

    Recording is where the answer is fenced: the host stamps the artifact digest the approver was
    shown, the Cell epoch the run is executing as, and the inputs that epoch was admitted with. An
    answer that already claims a different Cell/epoch is refused instead of being restamped, so a
    response cannot be carried across epochs and re-recorded as current.
    """
    path = f"{ctx.art}/approvals.json"
    artifact = _gate_artifact(ctx, approval.gate)
    digest = _artifact_sha256(ctx, artifact)
    cell_id, cell_epoch, _managed = cell_evidence(ctx)
    # `require`, not `digest_of`: an unpinned run returns None, and `None == None` used to
    # validate a whole approval set with null digests. Publication must fail closed.
    inputs_digest = accepted_inputs.require(ctx.state).digest
    if (approval.cell_id is not None and approval.cell_id != cell_id) or (
        approval.cell_epoch is not None and approval.cell_epoch != cell_epoch
    ):
        raise StageError(
            "policy",
            f"decision for gate {approval.gate!r} is bound to Cell "
            f"{approval.cell_id}/{approval.cell_epoch}, not {cell_id}/{cell_epoch}",
        )
    # Symmetric with the Cell check above: an answer that already carries a digest is CHECKED
    # against the artifact, never restamped. Overwriting it let a decision be re-recorded over a
    # changed artifact -- an approver's yes to one plan silently becoming a yes to another -- and
    # left the digest defended only at continuation, one enforcement point instead of two.
    if approval.artifact_sha256 is not None and approval.artifact_sha256 != digest:
        raise StageError(
            "policy",
            f"decision for gate {approval.gate!r} was given for artifact "
            f"{approval.artifact_sha256[:12]}, but {artifact} now digests to {digest[:12]}",
        )
    # Same rule as the artifact digest above: an answer that already names accepted inputs is
    # CHECKED, never restamped. Restamping would let a decision given before a deliberate
    # re-accept be re-recorded as if it had been given for the inputs that replaced them.
    # H1: the pin binds the approval RECORD, not the approval CHANNEL. `record_<stage>` builds its
    # Approval from the raw operator response, so it arrives with no inputs digest and recording
    # stamps whatever is pinned now. After a re-accept, an ordinary Airflow task clear or retry
    # therefore re-recorded a stale answer against the new inputs and it validated cleanly. The
    # answer's own timestamp is what distinguishes a person answering again from an old XCom being
    # read twice.
    # Human answers only. An "auto" default and a replay-fixture decision are produced by the
    # runtime at record time, not pulled from an operator's XCom, so a retry re-derives them under
    # whatever is pinned now -- there is no stale answer to launder. Applying the check to them
    # would refuse every automatic gate, which is the over-fencing that gets a check switched off.
    if approval.responded_at is not None and accepted_inputs.answered_before_admission(
        ctx.state, approval.responded_at
    ):
        raise StageError(
            "policy",
            f"the answer recorded for gate {approval.gate!r} was given before this run admitted "
            f"{inputs_digest}, so it was not given for these inputs; answer the gate again",
        )
    if approval.inputs_digest is not None and approval.inputs_digest != inputs_digest:
        raise StageError(
            "policy",
            f"decision for gate {approval.gate!r} was given for accepted inputs "
            f"{approval.inputs_digest}, but this run executes {inputs_digest}",
        )
    approval = approval.model_copy(
        update={
            "artifact_sha256": digest,
            "cell_id": cell_id,
            "cell_epoch": cell_epoch,
            "inputs_digest": inputs_digest,
        }
    )
    data = _read_json(ctx, path, [])
    if not isinstance(data, list):
        raise StageError("policy", "approvals.json must contain a JSON array")
    data = [item for item in data if not isinstance(item, dict) or item.get("gate") != approval.gate]
    data.append(approval.model_dump(mode="json"))
    ctx.write_artifact(path, _dumps(data))


@_owned
def teardown(ctx: Ctx) -> None:
    """Cleanup must not terminate a work cell currently owned by another task attempt.

    Fork children first: a build task that died mid-wave is not necessarily retried, and its
    journalled children would otherwise bill until their lease. The parent closes either way.
    """
    from swfactory import fork_wave  # fork_wave builds on this module

    try:
        fork_wave.teardown(ctx)
    finally:
        ctx.sb.close()


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
    for sev in metrics_mod.SEVERITIES:
        group = [f for f in findings if f.severity == sev]
        if not group:
            continue
        note = ""
        if sev == "nit" and dropped_nits:
            note = f", {dropped_nits} more dropped by the cap of {_nit_cap(ctx)}"
        lines.append(f"### {sev.capitalize()} ({len(group)}{note})")
        lines.extend(f"- `{f.file}:{f.line or '-'}` **{f.title}** — {f.detail}" for f in group)
    parts.append("\n".join(lines))
    approval_rows = [[a.gate, a.decision, a.actor, a.at.isoformat(timespec="seconds")] for a in approvals]
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
    parts.append("## Stages\n" + _md_table(["stage", "status", "duration s", "cost usd", "numbers"], stage_rows))
    if ctx.agent.kind == "external" and (unknown := unreconciled_spend(ctx)):
        parts.append(f"Observed charges: ${persisted_cost(ctx):.4f}; unresolved reserved spend: ${unknown:.4f}.")
    pinned = accepted_inputs.stored(ctx.state)
    parts.append(
        "## Provenance\n"
        f"- run `{ctx.cfg.run_id}` · agent `{ctx.agent.kind}` · sandbox `{ctx.sb.name}`\n"
        f"- accepted inputs `{pinned.digest if pinned else 'unpinned'}` "
        f"(policy `{pinned.policy_sha256[:12] if pinned else '-'}`) — every approval above was "
        "given for these\n"
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
        ctx.write_artifact(f"{ctx.art}/agent/stages.jsonl", run_log.read_text(encoding="utf-8"))
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

    cell_id, cell_epoch, managed = cell_evidence(ctx)
    # `require`, not `digest_of`: an unpinned run returns None, and `None == None` used to
    # validate a whole approval set with null digests. Publication must fail closed.
    inputs_digest = accepted_inputs.require(ctx.state).digest
    processed: set[str] = set()
    rejected_gate: str | None = None
    for gate in gates:
        approval = by_name.get(gate.name)
        if approval is None:
            raise StageError("policy", f"missing decision for required gate {gate.name!r}")
        processed.add(gate.name)
        artifact = f"{ctx.art}/{gate.artifact}"
        try:
            expected = _artifact_sha256(ctx, artifact)
        except FileNotFoundError as e:
            raise StageError("policy", f"approved artifact is missing: {artifact}") from e
        if approval.artifact_sha256 != expected:
            raise StageError("policy", f"approval for {gate.name} does not match its artifact")
        # Continuation is the third enforcement point: the recorded chain is re-checked against the
        # authority the blueprint declared and the Cell epoch this run is actually executing as, so
        # a decision recorded elsewhere (older epoch, automatic default, replay fixture) cannot
        # publish here.
        check_recorded(
            gate=gate.name,
            gate_mode=gate.mode,
            approval=approval,
            managed=managed,
            cell_id=cell_id,
            cell_epoch=cell_epoch,
            inputs_digest=inputs_digest,
        )
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
            review = Review.model_validate({"verdict": parsed.get("verdict"), "findings": parsed.get("findings")})
            dropped_nits = parsed.get("dropped_nits", 0)
            if not isinstance(dropped_nits, int) or isinstance(dropped_nits, bool) or dropped_nits < 0:
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
    if ctx.agent.kind == "external" and ctx.blueprint and any(gate.mode == "policy" for gate in ctx.blueprint.gates):
        raise StageError("policy", "external agents are not qualified for autonomous publication")
    metrics_mod.write_run_metrics(
        ctx,
        stages,
        approvals,
        total_cost_usd=persisted_cost(ctx),
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
    cell_id, epoch, _managed = cell_evidence(ctx)
    if ctx.blueprint and any(gate.mode == "policy" for gate in ctx.blueprint.gates):
        from swfactory.autonomy import load_policy
        from swfactory.backend_scm import BackendScm

        if not isinstance(ctx.scm, BackendScm) or blocked:
            raise StageError("policy", "autonomous publication requires passing managed evidence")
        ctx.scm.autonomous_evidence = {
            "revision": load_policy().revision,
            "agent": ctx.agent.kind,
            "tests_passed": metrics_mod.tests_passed(stages),
            "review_verdict": rv.verdict,
            "blockers": blockers,
            "cost_usd": persisted_cost(ctx),
            "budget_usd": ctx.cfg.max_budget_usd,
            "approvals": [approval.model_dump(mode="json") for approval in approvals],
            "artifact_digests": {
                name: _artifact_sha256(ctx, f"{ctx.art}/{name}")
                for name in ("intent.md", "plan.md", "plan.json", "review.json", "metrics.json", "approvals.json")
            },
        }
    url = ctx.scm.publish(
        branch=ctx.branch,
        patch=patch.encode("utf-8"),
        title=banner + title,
        # The same instance id the commits carry in their trailer: the remote compares the two.
        identity=PublicationIdentity(
            key=publication_key(ctx.cfg.repo, ctx.cfg.target_dir, ctx.issue.id),
            instance=ctx.instance,
            cell_id=cell_id,
            epoch=epoch,
        ),
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


def build_and_test(ctx: Ctx) -> StageResult:
    """Compatibility entrypoint for the one managed build implementation."""
    from swfactory.work_stage import build_and_test as managed_build_and_test

    return managed_build_and_test(ctx)


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
    return RunReport(
        run_id=ctx.cfg.run_id,
        issue_id=ctx.issue.id,
        inputs_digest=accepted_inputs.digest_of(ctx.state),
        agent=ctx.agent.kind,
        sandbox=ctx.sb.name,
        scm=ctx.scm.kind,
        stages=list(stages),
        approvals=approvals,
        pr_url=pr_url,
        tests_passed=metrics_mod.tests_passed(stages),
        total_cost_usd=round(persisted_cost(ctx), 6),
        unreconciled_cost_usd=round(unreconciled_spend(ctx), 6),
    )


def cli_approver(gate: Gate, ctx: Ctx) -> Approval:
    """Local replay's approver, holding the same policy the DAG's ``record_<stage>`` holds.

    A gate declared ``mode = "auto"`` answers itself. Otherwise the only thing allowed to answer
    without a person is the explicitly declared replay fixture (``SWF_GATE_REPLAY``). ``--approve
    auto`` / ``SWF_APPROVE=auto`` is refused on a human gate: an environment variable that can
    switch a declared human gate off is not a gate, and this is the surface where that used to work.
    """
    if gate.mode == "policy":
        from swfactory.autonomy import policy_approval

        return policy_approval(ctx, gate.name)
    if gate.mode == "auto":
        return Approval(gate=gate.name, decision="approve", actor="auto", mode="auto", at=datetime.now(UTC))
    _cell_id, _epoch, managed = cell_evidence(ctx)
    replay = replay_approval(
        gate.name,
        fixture_path=ctx.cfg.gate_replay,
        managed=managed,
        scm=ctx.cfg.scm,
        agent=ctx.cfg.agent,
    )
    if replay is not None:
        return replay
    if ctx.cfg.approve == "auto":
        raise StageError(
            "policy",
            f"gate {gate.name!r} is declared human and cannot be approved by configuration; "
            'answer it, declare the gate mode = "auto", or point SWF_GATE_REPLAY at a replay fixture',
        )
    print(f"\n===== {gate.artifact} =====\n{ctx.read_artifact(f'{ctx.art}/{gate.artifact}')}\n")
    ok = typer.confirm(f"Approve gate '{gate.name}'?", default=False)
    return Approval(
        gate=gate.name,
        decision="approve" if ok else "reject",
        actor=getpass.getuser(),
        mode="human",
        at=datetime.now(UTC),
    )
