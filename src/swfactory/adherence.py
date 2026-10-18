"""Post-run adherence audit: the AUDIT step of declare -> hold -> record -> audit.

swfactory already DECLARES what every role may do (``agent.POLICIES``, ``protected_for``, the
blueprint's gates and budget, ``accepted_inputs.STAGE_PROMPTS`` as the who-may-invoke-whom map),
HOLDS each call to it (tool allowlists, deny rules and the guard hook, kernel ``denyWrite``,
per-call budget grants, the read-only worktree fingerprint in ``stages._agent``) and RECORDS what
happened in host-owned state: the stage journal, the operations journal, the per-call ledger, the
result envelopes, the gate approvals and the commit trailers. Nothing turned those records into a
verdict. :func:`audit` does, deterministically and without a model call:

* ``adherent``     every check that applies passed;
* ``deviant``      at least one check found a role outside its declaration;
* ``unverifiable`` nothing failed, but some evidence a check needs was never recorded -- a run
  that predates the ledger, an unreconciled call, a missing envelope.

Absence of evidence is never read as adherence: a check that cannot see is ``unknown``, and only a
line or role this module does not model (replica-fusion children keep their own ledgers) is
``n/a``. The audit is RECORD-ONLY: ``stages.deliver`` writes ``{art}/adherence.json`` into the
committed artifact chain and ``adherence`` into ``metrics.json``, and publishes either way. Letting
``deviant`` block publication is an authority change reserved for a separate decision.

The six checks, and what each one reads:

participation   every non-deliver stage of the line has an ok/blocked record; every ledger role is
                one the line's stages may invoke; every settled call left its envelope.
delegation      every call was reserved inside an operations-journal window owned by a stage that
                may invoke its role (same process, same clock: no cross-host skew).
role_boundary   every commit base..HEAD carries one Factory-Stage trailer of a writing role and
                touches nothing ``protected_for`` that role or the fixed guarded set; deliver
                commits touch only the artifact dir; every read-only call's settle receipt (or, for
                an answer still awaiting its bill, the host's own record of it) says it left the
                worktree unchanged or ran on a read-only checkout mount.
tool_boundary   Claude envelopes: no permission denials, turns within the cap, the declared model;
                guard-hook denials (attributed through ``SWF_CALL``).
budget          settled <= reserved for every call; the run total within its ceiling.
information_flow every gate's approval matches the host artifact and the accepted inputs digest,
                and every call of a post-gate stage ran in an operation that started after the
                gate was recorded -- by operations-journal ORDER, which survives re-recording.
"""

from __future__ import annotations

import hashlib
import json
import shlex
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from fnmatch import fnmatch
from typing import TYPE_CHECKING, Any, Literal

from swfactory import accepted_inputs
from swfactory.agent import _GUARDED, POLICIES, call_max_turns
from swfactory.blueprint import CANONICAL_ORDER
from swfactory.call_accounting import CALLS_LOG, CallLedger, CallRecord
from swfactory.config import protected_for
from swfactory.models import Approval, StageResult

if TYPE_CHECKING:
    from swfactory.stages import Ctx

SCHEMA_VERSION = 1
ARTIFACT = "adherence.json"
Verdict = Literal["adherent", "deviant", "unverifiable"]
Status = Literal["pass", "fail", "unknown", "n/a"]
CHECKS = ("participation", "delegation", "role_boundary", "tool_boundary", "budget", "information_flow")
# ``StageResult.numbers`` is float-typed, so the deliver record carries the verdict as a signed
# score: above zero adheres, zero cannot be verified, below zero deviates.
VERDICT_SCORE: dict[str, float] = {"adherent": 1.0, "unverifiable": 0.0, "deviant": -1.0}
# ``accepted_inputs.STAGE_PROMPTS`` names TEMPLATES; the ledger names ROLES (the ``stage`` argument
# of ``stages._agent``). ``build_node`` is the build role's per-node template; ``fusion`` runs its
# roles in child ledgers and charges the parent one campaign reservation (``CAMPAIGN_ROLES``).
TEMPLATE_ROLES: dict[str, tuple[str, ...]] = {
    "spec": ("spec",),
    "plan": ("plan",),
    "build": ("build",),
    "build_node": ("build",),
    "fix": ("fix",),
    "review": ("review",),
    "fusion": (),
}
FUSION_ROLE = "replica_fusion"
# One ledger row reserving a whole campaign whose individual calls are not ``_agent`` calls of this
# run: replica-fusion children keep their own ledgers, and the optional population search runs
# host-owned provider tasks. Both belong to build_and_test, leave no envelope and no Claude tool
# record here, and are audited for participation, delegation and budget only.
CAMPAIGN_ROLES = frozenset({FUSION_ROLE, "population_search"})
# Factory-Stage trailer -> the role whose boundary the commit is held to. ``fusion`` adopts a
# candidate whose author was build (round 1) or fix (later rounds); it is held to build, the weaker
# of the two, and says so in its evidence rather than guessing.
TRAILER_ROLES = {"build": "build", "fix": "fix", "fusion": "build", "deliver": "deliver"}
MAX_EVIDENCE = 40
_EPSILON = 1e-9
_STATUS_RANK = {"n/a": 0, "pass": 1, "unknown": 2, "fail": 3}


# ---------------------------------------------------------------- result plumbing


@dataclass
class _Check:
    name: str
    statuses: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)

    def add(self, status: Status, note: str) -> None:
        self.statuses.append(status)
        self.evidence.append(f"{status}: {note}")

    def result(self) -> dict[str, Any]:
        status = max(self.statuses, key=_STATUS_RANK.__getitem__, default="n/a")
        # Failures and gaps first: a reader of a long list must not have to dig for the finding.
        ordered = sorted(self.evidence, key=lambda line: -_STATUS_RANK[line.split(":", 1)[0]])
        if len(ordered) > MAX_EVIDENCE:
            ordered = [*ordered[:MAX_EVIDENCE], f"... {len(ordered) - MAX_EVIDENCE} more"]
        return {"name": self.name, "status": status, "evidence": ordered}


def verdict_of(checks: Sequence[dict[str, Any]]) -> Verdict:
    """Any fail -> deviant; else any unknown -> unverifiable; else adherent."""
    statuses = {check["status"] for check in checks}
    if "fail" in statuses:
        return "deviant"
    if "unknown" in statuses:
        return "unverifiable"
    return "adherent"


# ---------------------------------------------------------------- the run, as recorded


@dataclass(frozen=True)
class _Call:
    record: CallRecord
    at: datetime | None  # the reservation time: when the call was committed to, before the provider

    @property
    def role(self) -> str:
        return self.record.stage

    @property
    def name(self) -> str:
        return f"{self.record.stage}.{self.record.iteration}"


@dataclass(frozen=True)
class _Window:
    """One owned operation attempt from ``operations.jsonl``: ``index`` is its journal position."""

    index: int
    operation: str
    start: datetime
    end: datetime | None


@dataclass
class _Line:
    order: list[str]
    required: list[str]
    gates: list[Any]  # stages.Gate
    gates_before: dict[str, list[str]]  # stage -> names of the gates that precede it in the walk
    fusion: bool

    def roles_of(self, stage: str) -> set[str]:
        roles = {role for name in accepted_inputs.STAGE_PROMPTS.get(stage, ()) for role in TEMPLATE_ROLES.get(name, ())}
        if stage == "build_and_test":
            roles.add("population_search")  # enabled by host control state, on any line
            if self.fusion:
                roles.add(FUSION_ROLE)
        return roles

    def line_roles(self) -> set[str]:
        return {role for stage in self.order for role in self.roles_of(stage)}


def _parse_ts(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else None


def _calls(ctx: Ctx) -> list[_Call]:
    reserved_at: dict[str, datetime | None] = {}
    for row in ctx.state.read_jsonl(CALLS_LOG):
        if isinstance(row, dict) and row.get("event") == "reserved" and isinstance(row.get("call_id"), str):
            reserved_at.setdefault(row["call_id"], _parse_ts(row.get("at")))
    return [_Call(record, reserved_at.get(record.call_id)) for record in CallLedger(ctx.state).records()]


def _windows(ctx: Ctx) -> tuple[list[_Window], list[dict[str, Any]]]:
    records = ctx.state.operation_records()
    started: dict[str, tuple[int, dict[str, Any]]] = {}
    ends: dict[str, datetime | None] = {}
    for index, record in enumerate(records):
        if record["event"] == "started":
            started.setdefault(record["attempt_id"], (index, record))
        else:
            ends.setdefault(record["attempt_id"], _parse_ts(record.get("finished_at")))
    windows = []
    for attempt, (index, record) in started.items():
        start = _parse_ts(record.get("started_at"))
        if start is not None:
            windows.append(_Window(index, record["operation"], start, ends.get(attempt)))
    return sorted(windows, key=lambda window: window.index), records


def _owner(call: _Call, windows: Sequence[_Window]) -> _Window | None:
    """The owned operation that was running when ``call`` was reserved. Ownership is exclusive per
    run, so at most one window can contain a reservation; the latest start wins on a tie."""
    if call.at is None:
        return None
    containing = [w for w in windows if w.start <= call.at and (w.end is None or call.at <= w.end)]
    return containing[-1] if containing else None


def _line(ctx: Ctx, approvals: Sequence[Approval]) -> _Line:
    from swfactory import stages

    order = list(ctx.blueprint.order) if ctx.blueprint else list(CANONICAL_ORDER)
    gates: list[Any] = []
    gates_before: dict[str, list[str]] = {}
    for item in stages._pipeline(ctx):
        if isinstance(item, stages.Gate):
            gates.append(item)
        else:
            gates_before[getattr(item, "__name__", "")] = [gate.name for gate in gates]
    rejected = next((a.gate for a in approvals if a.decision == "reject" and a.gate in order), None)
    required = order[: order.index(rejected) + 1] if rejected else order[:-1]
    fusion = ctx.blueprint is not None and ctx.blueprint.fusion is not None
    return _Line(order, required, gates, gates_before, fusion)


def _envelope_path(ctx: Ctx, call: _Call) -> str:
    return f"{ctx.art}/agent/{call.role}.{call.record.iteration}.json"


def _expects_envelope(call: _Call) -> bool:
    """A call that answered: settled from its own receipt. A call settled by an observation that it
    never reached the provider (``definitely_absent``) has no envelope to leave."""
    return call.record.settled_usd is not None and call.record.receipt is not None and call.role not in CAMPAIGN_ROLES


def _hits(path: str, rule: str) -> bool:
    """``swf_guard.py``'s rule semantics, so the audit holds a commit to the boundary the hold used."""
    rule = rule.strip().rstrip("/")
    return bool(rule) and (
        path == rule or path.startswith(rule + "/") or fnmatch(path, rule) or fnmatch(path, "*/" + rule)
    )


# ---------------------------------------------------------------- checks


def _participation(ctx: Ctx, line: _Line, stages_: Sequence[StageResult], calls: Sequence[_Call]) -> _Check:
    check = _Check("participation")
    completed = {s.stage: s for s in stages_ if s.status in {"ok", "blocked"}}
    for stage in line.required:
        if stage in completed:
            check.add("pass", f"stage {stage} recorded {completed[stage].status}")
        else:
            check.add("fail", f"stage {stage} of the line has no ok/blocked record")
    allowed = line.line_roles()
    for call in calls:
        if call.role not in allowed:
            check.add("fail", f"ledger call {call.name}: role {call.role!r} is not one this line's stages invoke")
    for stage in line.required:
        roles = line.roles_of(stage)
        if roles and not any(call.role in roles for call in calls):
            check.add("unknown", f"stage {stage}: the call ledger holds none of its calls ({sorted(roles)})")
    settled = [call for call in calls if _expects_envelope(call)]
    missing = [call for call in settled if not ctx.state.has_artifact(_envelope_path(ctx, call))]
    for call in missing:
        check.add("unknown", f"settled call {call.name} left no envelope at {_envelope_path(ctx, call)}")
    if settled and not missing:
        check.add("pass", f"{len(settled)} settled call(s) each left an envelope")
    return check


def _delegation(line: _Line, calls: Sequence[_Call], windows: Sequence[_Window]) -> _Check:
    check = _Check("delegation")
    held = 0
    for call in calls:
        owner = _owner(call, windows)
        if owner is None:
            check.add("unknown", f"{call.name} was reserved outside every recorded operation window")
        elif call.role in line.roles_of(owner.operation):
            held += 1
        else:
            check.add(
                "fail", f"{call.name} ran inside operation {owner.operation!r}, which may not invoke {call.role!r}"
            )
    if held:
        check.add("pass", f"{held} call(s) ran inside a stage operation allowed to invoke their role")
    return check


def _commits(ctx: Ctx, base: str) -> list[tuple[str, list[str], list[str]]]:
    """``(sha, Factory-Stage values, target-relative files)`` for every commit base..HEAD; files
    outside the target dir keep a leading ``../`` marker so the boundary check can name them."""
    from swfactory import stages

    prefix = stages._sh(ctx, "git rev-parse --show-prefix").strip()
    fmt = shlex.quote("--format=%x1e%H%x1f%(trailers:key=Factory-Stage,valueonly)")
    # No pathspec: a pathspec would hide exactly the commits that strayed outside the target.
    raw = stages._sh(
        ctx,
        f"git -c core.quotepath=off log -z --no-renames --name-only {fmt} {shlex.quote(base + '..HEAD')}",
    )
    commits = []
    for record in raw.split("\x1e")[1:]:
        header, _, names = record.partition("\0")
        sha, _, trailers = header.partition("\x1f")
        values = [value.strip() for value in trailers.splitlines() if value.strip()]
        files = []
        for name in names.split("\0"):
            name = name.strip("\n")
            if not name:
                continue
            files.append(name[len(prefix) :] if name.startswith(prefix) else f"../{name}")
        commits.append((sha.strip(), values, files))
    return commits


def _role_boundary(ctx: Ctx, calls: Sequence[_Call]) -> _Check:
    from swfactory import stages

    check = _Check("role_boundary")
    contract = stages._contract(ctx)
    base = ctx.state.read_control("base").strip() if ctx.state.has_control("base") else ""
    if not base:
        check.add("unknown", "run state records no base commit, so base..HEAD cannot be read")
    else:
        try:
            commits = _commits(ctx, base)
        except Exception as error:  # noqa: BLE001 - a sandbox that cannot answer is missing evidence
            check.add("unknown", f"could not read the commit stream {base[:12]}..HEAD: {error}")
            commits = []
        held = 0
        for sha, trailers, files in commits:
            short = sha[:12]
            if len(trailers) != 1:
                check.add("fail", f"commit {short} carries {len(trailers)} Factory-Stage trailers, not exactly one")
                continue
            trailer = trailers[0]
            role = "build" if trailer.startswith("work:") else TRAILER_ROLES.get(trailer)
            if role is None:
                writes = POLICIES[trailer].writes if trailer in POLICIES else None
                why = "a read-only role" if writes is False else "no writing role"
                check.add("fail", f"commit {short} is attributed to {trailer!r}, {why}; only the factory commits")
                continue
            outside = [f for f in files if f.startswith("../")]
            if outside:
                check.add("fail", f"commit {short} ({trailer}) touches files outside the target: {outside[:5]}")
                continue
            if role == "deliver":
                stray = [f for f in files if not f.startswith(f"{ctx.art}/")]
                if stray:
                    check.add("fail", f"deliver commit {short} touches files outside {ctx.art}/: {stray[:5]}")
                else:
                    held += 1
                continue
            guarded = [f for f in files if any(_hits(f, rule) for rule in _GUARDED)]
            protected = [f for f in files if any(_hits(f, rule) for rule in protected_for(contract, role))]
            if guarded:
                check.add("fail", f"commit {short} ({trailer}) touches guarded paths: {guarded[:5]}")
            if protected:
                check.add("fail", f"commit {short} ({trailer}) touches paths protected for {role}: {protected[:5]}")
            if not guarded and not protected:
                held += 1
                if trailer == "fusion":
                    check.add("pass", f"fusion commit {short} held to build's boundary (author round not recorded)")
        if held:
            check.add("pass", f"{held} commit(s) base..HEAD stayed inside their role's boundary")
    for call in calls:
        policy = POLICIES.get(call.role)
        if policy is None or policy.writes or call.role in CAMPAIGN_ROLES:
            continue
        # The settle receipt, or -- for an answer whose bill awaits an observation -- the facts the
        # host recorded on the in-doubt row. Neither: the call never answered here.
        seen = call.record.receipt or call.record.observed
        if seen is None:
            if (call.record.observation or {}).get("status") == "definitely_absent":
                continue  # proven never to have started: nothing ran that could edit
            check.add("unknown", f"read-only call {call.name} never answered; its effect on the worktree is unseen")
            continue
        unchanged = seen.get("worktree_unchanged")
        if unchanged is None and seen.get("worktree_hold") == stages.READ_ONLY_MOUNT:
            check.add("pass", f"read-only call {call.name} ran on a read-only checkout mount")
        elif unchanged is True:
            check.add("pass", f"read-only call {call.name} left the worktree unchanged")
        elif unchanged is False:
            check.add("fail", f"read-only call {call.name} changed the worktree")
        else:
            check.add("unknown", f"read-only call {call.name} has no worktree fingerprint in its receipt")
    return check


def _model_matches(model: str, usage: object) -> bool | None:
    if not isinstance(usage, dict) or not usage:
        return None
    wanted = model.casefold()
    for key, entry in usage.items():
        canonical = str(entry.get("canonicalModel", "")) if isinstance(entry, dict) else ""
        if wanted in {str(key).casefold(), canonical.casefold()} or (canonical and wanted in canonical.casefold()):
            return True
    return False


def _tool_boundary(ctx: Ctx, calls: Sequence[_Call]) -> _Check:
    from swfactory import stages

    check = _Check("tool_boundary")
    for call in calls:
        receipt = call.record.receipt
        if receipt is None or call.role in CAMPAIGN_ROLES:
            continue
        agent = receipt.get("agent")
        if agent != "claude":
            check.add("n/a", f"{call.name}: agent {agent!r} leaves no Claude tool envelope")
            continue
        try:
            envelope = json.loads(ctx.state.read_artifact(_envelope_path(ctx, call)))
        except (FileNotFoundError, ValueError):
            envelope = None
        if not isinstance(envelope, dict):
            check.add("unknown", f"{call.name}: no readable envelope to inspect")
            continue
        denials = envelope.get("permission_denials")
        if not isinstance(denials, list):
            check.add("unknown", f"{call.name}: envelope records no permission_denials")
        elif denials:
            tools = sorted({str(d.get("tool_name", "?")) if isinstance(d, dict) else "?" for d in denials})
            check.add("fail", f"{call.name}: {len(denials)} tool call(s) refused by permission rules ({tools})")
        else:
            check.add("pass", f"{call.name}: no permission denials")
        policy = stages._policy(ctx, call.role) if call.role in POLICIES else None
        # Exactly the cap the call was launched under: a ``[policy.<stage>] max_turns`` override
        # only tightens, and a run limit lowered after the blueprint loaded still wins over it.
        cap = ctx.cfg.max_turns if policy is None else call_max_turns(policy, ctx.cfg)
        turns = envelope.get("num_turns")
        if isinstance(turns, int) and turns > cap and envelope.get("subtype") != "error_max_turns":
            check.add("fail", f"{call.name}: {turns} turns past the cap of {cap} without a max-turns stop")
        if policy is not None and policy.model:
            matched = _model_matches(policy.model, envelope.get("modelUsage"))
            if matched is None:
                check.add("unknown", f"{call.name}: envelope records no modelUsage for declared model {policy.model!r}")
            elif not matched:
                check.add("fail", f"{call.name}: ran without the declared model {policy.model!r}")
    denied: dict[str, int] = {}
    for line in stages._read_or(ctx, stages.HOOKS_LOG).splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if isinstance(entry, dict) and entry.get("decision") == "deny":
            who = str(entry.get("call") or "unattributed")
            denied[who] = denied.get(who, 0) + 1
    for who, count in sorted(denied.items()):
        check.add("fail", f"guard hook refused {count} tool call(s) of {who}")
    return check


def _budget(ctx: Ctx, line: _Line, calls: Sequence[_Call]) -> _Check:
    check = _Check("budget")
    ledger = CallLedger(ctx.state)
    floor = ledger.floor_usd() or 0.0
    if not calls:
        agent_stages = [stage for stage in line.required if line.roles_of(stage)]
        if agent_stages:
            check.add("unknown", f"no per-call ledger rows for agent stages {agent_stages} (pre-ledger run)")
        return check
    within = 0
    for call in calls:
        record = call.record
        if record.settled_usd is None:
            check.add("unknown", f"{call.name}: {record.reserved_usd:.4f} USD reserved, usage never reconciled")
        elif record.settled_usd > record.reserved_usd + _EPSILON:
            check.add(
                "fail", f"{call.name}: settled {record.settled_usd:.4f} USD over its {record.reserved_usd:.4f} grant"
            )
        else:
            within += 1
    if within:
        check.add("pass", f"{within} call(s) settled within their reservation")
    total = round(floor + ledger.charged_usd(), 6)
    if total > ctx.cfg.max_budget_usd + _EPSILON:
        check.add("fail", f"run charged {total:.4f} USD against a {ctx.cfg.max_budget_usd:.4f} USD ceiling")
    else:
        check.add("pass", f"run charged {total:.4f} of {ctx.cfg.max_budget_usd:.4f} USD")
    return check


def _information_flow(
    ctx: Ctx,
    line: _Line,
    approvals: Sequence[Approval],
    calls: Sequence[_Call],
    windows: Sequence[_Window] | None,
    records: Sequence[dict[str, Any]],
) -> _Check:
    check = _Check("information_flow")
    by_gate = {approval.gate: approval for approval in approvals}
    try:
        pinned: str | None = accepted_inputs.require(ctx.state).digest
    except Exception as error:  # noqa: BLE001 - an unpinned run has no digest to compare against
        pinned = None
        check.add("unknown", f"accepted inputs are not pinned: {error}")
    reached = set(line.required)
    for gate in line.gates:
        if gate.name not in reached:
            continue  # after the rejected gate: never asked
        approval = by_gate.get(gate.name)
        if approval is None:
            check.add("fail", f"gate {gate.name} has no recorded decision")
            continue
        path = f"{ctx.art}/{gate.artifact}"
        try:
            digest = hashlib.sha256(ctx.state.read_artifact(path).encode("utf-8")).hexdigest()
        except FileNotFoundError:
            check.add("fail", f"gate {gate.name}: the approved artifact {path} is missing")
        else:
            if approval.artifact_sha256 == digest:
                check.add("pass", f"gate {gate.name}: decision bound to {gate.artifact} as published")
            else:
                check.add("fail", f"gate {gate.name}: decision digest does not match {path}")
        if pinned is not None:
            if approval.inputs_digest == pinned:
                check.add("pass", f"gate {gate.name}: decision given for the accepted inputs")
            else:
                check.add("fail", f"gate {gate.name}: decision names inputs {approval.inputs_digest}, not {pinned}")
    if windows is None:
        if calls:
            check.add("unknown", "no readable operations journal: gate-before-call order cannot be shown")
        return check
    rejected = {approval.gate for approval in approvals if approval.decision == "reject"}
    for call in calls:
        owner = _owner(call, windows)
        if owner is None:
            check.add("unknown", f"{call.name}: no owning operation, so its order against the gates is unknown")
            continue
        before = records[: owner.index]
        named = {
            r["operation"].split(":", 1)[1]
            for r in before
            if r["event"] == "completed" and r["operation"].startswith("record_approval:")
        }
        # Runs recorded before gates were named in the journal: count the anonymous recordings.
        legacy = sum(1 for r in before if r["event"] == "completed" and r["operation"] == "record_approval")
        needed = line.gates_before.get(owner.operation, [])
        # A run upgraded mid-flight may hold both: each anonymous recording covers one unnamed gate.
        unnamed = [gate for gate in needed if gate not in named]
        held = True
        for gate in needed:
            if gate in rejected:
                held = False
                check.add("fail", f"{call.name} ran in {owner.operation!r} although gate {gate} was rejected")
            elif gate in unnamed and legacy < len(unnamed):
                held = False
                check.add("fail", f"{call.name} ran in {owner.operation!r}, started before gate {gate} was recorded")
        if held:
            check.add("pass", f"{call.name} ran after its {len(needed)} preceding gate(s) were recorded")
    return check


# ---------------------------------------------------------------- audit


def _per_role(calls: Sequence[_Call]) -> dict[str, dict[str, Any]]:
    """Spend by ROLE from the ledger, never from stage records: the review stage's record also
    carries the fix calls it makes, and the declared cap is per call, not per stage."""
    roles: dict[str, dict[str, Any]] = {}
    for call in calls:
        row = roles.setdefault(call.role, {"calls": 0, "reserved_usd": 0.0, "cost_usd": 0.0, "unreconciled_usd": 0.0})
        row["calls"] += 1
        row["reserved_usd"] += call.record.reserved_usd
        if call.record.settled_usd is None:
            row["unreconciled_usd"] += call.record.reserved_usd
        else:
            row["cost_usd"] += call.record.settled_usd
    return {
        role: {k: round(v, 6) if isinstance(v, float) else v for k, v in row.items()}
        for role, row in sorted(roles.items())
    }


def _guarded(name: str, run: Callable[[], _Check]) -> dict[str, Any]:
    try:
        return run().result()
    except Exception as error:  # noqa: BLE001 - one unreadable record must not hide the other checks
        failed = _Check(name)
        failed.add("unknown", f"audit could not evaluate this check: {type(error).__name__}: {error}")
        return failed.result()


def audit(
    ctx: Ctx,
    *,
    stages: Sequence[StageResult] | None = None,
    approvals: Sequence[Approval] | None = None,
) -> dict[str, Any]:
    """Re-derive, from host-owned records and the delivered commit stream, whether every role
    stayed inside its declaration. Reads only; never raises on missing or malformed evidence.

    ``stages`` and ``approvals`` default to what the run state holds (``load_stage_results`` and
    the host ``approvals.json``); ``deliver`` passes the ones it has already validated.
    """
    from swfactory import stages as stages_mod

    notes: list[str] = []
    try:
        stage_results = list(stages) if stages is not None else stages_mod.load_stage_results(ctx)
        decisions = list(approvals) if approvals is not None else stages_mod._load_approvals(ctx)
        line = _line(ctx, decisions)
        calls = _calls(ctx)
    except Exception as error:  # noqa: BLE001 - the audit reports, it does not gate
        check = _Check("audit")
        check.add("unknown", f"run records are unreadable: {type(error).__name__}: {error}")
        return {
            "schema_version": SCHEMA_VERSION,
            "verdict": "unverifiable",
            "checks": [check.result()],
            "per_role": {},
            "line": {},
        }
    windows: list[_Window] | None
    records: list[dict[str, Any]]
    try:
        windows, records = _windows(ctx)
    except Exception as error:  # noqa: BLE001 - a corrupt journal is missing evidence here
        windows, records = None, []
        notes.append(f"operations journal unreadable: {error}")
    if line.fusion:
        notes.append(f"replica-fusion roles run in child ledgers; the parent audits the {FUSION_ROLE!r} reservation")
    if any(call.role == "population_search" for call in calls):
        notes.append("population search runs host-owned provider tasks; audited as one campaign reservation")

    def delegation() -> _Check:
        if windows is None:
            check = _Check("delegation")
            if calls:
                check.add("unknown", "no readable operations journal")
            return check
        return _delegation(line, calls, windows)

    checks = [
        _guarded("participation", lambda: _participation(ctx, line, stage_results, calls)),
        _guarded("delegation", delegation),
        _guarded("role_boundary", lambda: _role_boundary(ctx, calls)),
        _guarded("tool_boundary", lambda: _tool_boundary(ctx, calls)),
        _guarded("budget", lambda: _budget(ctx, line, calls)),
        _guarded("information_flow", lambda: _information_flow(ctx, line, decisions, calls, windows, records)),
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "verdict": verdict_of(checks),
        "checks": checks,
        "per_role": _per_role(calls),
        "line": {
            "blueprint": ctx.cfg.blueprint,
            "order": line.order,
            "gates": [gate.name for gate in line.gates],
            "required": line.required,
            "notes": notes,
        },
    }


def render(report: dict[str, Any]) -> str:
    """The committed artifact, keys sorted so two audits of one record differ only where it did."""
    return json.dumps(report, indent=2, sort_keys=True) + "\n"
