"""Forkable work cells: one parallel-safe ``Plan.work`` wave fanned out over provider forks.

Airflow still owns the lifecycle task. This runs inside ``work_stage.build_and_test``, schedules
nothing, and decides nothing about the lifecycle: it turns one topological wave into N forked
child cells, lets ``WorkExecutor`` classify their writes and fan them back in by node id, and hands
every merged patch to the stage's own journal. A fork hint is always safe to ignore -- a wave
forks only when every gate in :func:`fork_decision` holds, and otherwise runs in the one shared
workspace exactly as before.

The native-fork contract in ``docs/lifecycle.md``, rule by rule:

1. **Immutable parent.** The parent is parked at the wave's checkpoint SHA, and every child proves
   ``HEAD == checkpoint`` on a clean tree before an agent touches it. A provider's periodic
   snapshot can be older than HEAD; a child that came from one is refused as a stale fork.
2. **Own lease.** Each child has an explicit TTL (never longer than the stage), its own agent call
   number and budget reservation, and an idempotency key bound to run, wave, node, call and
   checkpoint. A wave forks only when the run budget can pay every sibling's full grant at once.
3. **No publishing credentials.** Children are forks of a credential-free cell; commits that are
   published, and publication itself, happen only in the parent.
4. **Lineage.** The journal records the parent, the checkpoint, each child id, its key, its input
   and output HEAD and the digest of its patch -- the key before the fork call, the id the moment
   the provider answers.
5. **No false agreement.** Siblings share one parent and do different work; receipts name the
   shared parent (``fork_parent``) and nothing here counts siblings as independent evidence.
6. **Deterministic fan-in.** Conflicts are classified before any merge; patches are applied in node
   id order, and each applied result is checked blob-for-blob against the child that produced it.
7. **Full verification.** Nothing is verified in a child. The stage's test/repair loop runs on the
   merged head, as it does for serial work.
8. **Teardown.** Every child is reaped in its runner's ``finally``; a node failure, a timeout or a
   signal halts live siblings at the provider; a retried task (:func:`recover`) and the teardown
   task (:func:`teardown`) reap whatever the journal names before anything else. A child whose
   fork answer was lost is found again through its idempotency key, and until then the parent
   stays parked -- a running parent could fork anew from a periodic snapshot.

A provider's definite refusal to fork (boat's start-rate limit) creates nothing; a wave whose only
failures are refusals raises :class:`ForkUnavailable`, and the stage runs it serially instead.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import shlex
import threading
import time
from collections.abc import Callable, Sequence
from typing import Any, Protocol, runtime_checkable

from swfactory import stages
from swfactory.config import protected_for
from swfactory.models import BuildSummary, PlanTask, StageError
from swfactory.sandbox import Sandbox
from swfactory.sandbox_contract import ForkRefused
from swfactory.work_executor import (
    Cancellation,
    ExecutionReport,
    ExecutorPolicy,
    MergeReceipt,
    NodeRequest,
    NodeResult,
    WorkExecutor,
)
from swfactory.workgraph import WorkNode, conflict_set

PROGRESS_FILE = "workgraph-progress.json"  # work_stage's journal; this module owns its fork keys
PATCH_DIR = ".factory/fork-patches"  # below .factory/, which stages.NEVER_COMMITTED excludes
LEASE_GRACE_S = 300  # past a child's TTL by this much, the provider has stopped it for us


@runtime_checkable
class ForkableSandbox(Protocol):
    """A cell that can be parked and forked. Only an adapter that implements this AND whose
    capability document says ``fork`` is ever fanned out: a document alone is a claim, and an
    adapter alone is untested."""

    def fork_parent(self) -> str:
        """Park the cell at its current disk so it can be forked; returns the parent's id."""
        ...

    def fork_child(self, *, key: str, ttl_s: int, record: Callable[[str], None]) -> Sandbox:
        """A ready child forked from the parked parent; ``record`` gets its id before any wait."""
        ...

    def reclaim_fork(self, *, key: str, ttl_s: int) -> str | None:
        """Re-issue an idempotent fork whose answer was lost; the child it made, or ``None``."""
        ...

    def halt(self, provider_id: str) -> None:
        """Ask the provider to stop one child now, without waiting (cancellation)."""
        ...

    def reap(self, provider_id: str) -> None:
        """Stop one child and confirm it; raises unless the provider confirms."""
        ...

    def resume_parent(self) -> None:
        """Bring the parked parent back into service (idempotent)."""
        ...


@dataclasses.dataclass(frozen=True)
class Harvest:
    """What one child produced, read back out of it before it is reaped."""

    node_id: str
    child: str
    key: str
    agent_call: int
    input_head: str
    output_head: str
    message: str
    touched: tuple[str, ...]
    raw_diff: str  # `git diff --raw --no-abbrev`: mode and blob id per path, the fan-in check
    patch: str
    patch_blob: str  # `git hash-object` of the patch, computed in the child and again here

    def lineage(self, parent: str, checkpoint: str) -> dict[str, Any]:
        return {
            "fork_parent": parent,
            "checkpoint": checkpoint,
            "child": self.child,
            "key": self.key,
            "child_input_head": self.input_head,
            "child_output_head": self.output_head,
            "patch_blob": self.patch_blob,
        }


Merge = Callable[[PlanTask, Harvest, str], str]


class ForkUnavailable(StageError):
    """Every failure in the wave was the provider refusing to fork. Nothing was merged, every
    child is confirmed gone and the parent is back at the checkpoint: the wave may run serially."""

    def __init__(self, message: str) -> None:
        super().__init__("sandbox", message, retryable=True)


def fork_decision(ctx: stages.Ctx, pending: Sequence[PlanTask], *, provider_forks: bool) -> tuple[bool, str]:
    """Whether this wave forks, and the reason either way (both are retained as evidence)."""
    # Cheapest facts first: a single-node wave or a provider without fork never reads the config.
    if len(pending) < 2:
        return False, "single_node_wave"
    if not provider_forks:
        return False, "provider_cannot_fork"
    if ctx.cfg.max_parallel_forks < 2:
        return False, "max_parallel_forks_is_1"
    if not isinstance(ctx.sb, ForkableSandbox):
        return False, "sandbox_has_no_fork_adapter"
    if ctx.agent.kind == "external":
        # The external launcher binds one descriptor to one workdir and owns its own recovery.
        return False, "external_agent_not_qualified_for_forks"
    if not all(node.parallel_safe for node in pending):
        return False, "wave_not_parallel_safe"
    if conflict_set(_work_nodes(pending)):
        return False, "declared_conflict"
    remaining = ctx.cfg.max_budget_usd - stages.seed_budget(ctx, refresh=True)
    if remaining < len(pending) * ctx.cfg.max_budget_usd_per_stage:
        # Each sibling may be granted a full stage budget before any of them is billed; only a
        # run that can pay all of them at once may fork, so concurrency never outspends serial.
        return False, "budget_cannot_cover_siblings"
    return True, "parallel_safe_wave_on_forkable_provider"


def run_wave(
    ctx: stages.Ctx,
    pending: Sequence[PlanTask],
    progress: dict[str, Any],
    *,
    layer: int,
    checkpoint: str,
    prompts: dict[str, str],
    store: Callable[[], None],
    merge: Merge,
    deadline: float | None = None,
) -> dict[str, Any]:
    """Fork one child per pending node off ``checkpoint``, run them, fan in, reap; the receipt.

    Raises the first primary node failure (by node id), a ``policy`` error for a conflict found
    before fan-in, or a retryable ``sandbox`` error when a child could not be confirmed stopped --
    in which case the in-flight record stays in the journal for :func:`recover` and
    :func:`teardown`. ``deadline`` (``time.monotonic``) bounds every child's lease to what is left
    of the stage, so no child is meant to outlive the task that forked it.
    """
    parent = ctx.sb
    if not isinstance(parent, ForkableSandbox):  # fork_decision already refused this
        raise StageError("policy", "fork wave requested on a sandbox that cannot fork")
    nodes = {node.id: node for node in pending}
    lock = threading.RLock()
    left = ctx.cfg.stage_timeout_h * 3600 if deadline is None else int(deadline - time.monotonic())
    ttl_s = max(60, min(ctx.cfg.sandbox_ttl_s, ctx.cfg.stage_timeout_h * 3600, left))
    stages._ensure_clean(ctx, f"fork wave {layer}", allow_artifacts=True)
    # Everything that can fail on bad local state is settled before the parent is parked.
    cell_id, epoch, _ = stages.cell_evidence(ctx)
    protected = tuple(protected_for(stages._contract(ctx), "build"))
    wave: dict[str, Any] = {
        "layer": layer,
        "checkpoint": checkpoint,
        "parent": None,
        "parked": True,  # intent first: a death inside the stop still leaves a parent to unpark
        "children": [],
        "started_at": time.time(),
    }
    harvests: dict[str, Harvest] = {}
    primary: dict[str, BaseException] = {}
    live: dict[str, dict[str, Any]] = {}
    merges: list[dict[str, Any]] = []
    resumed = False

    def journal(entry: dict[str, Any], **update: Any) -> None:
        with lock:
            entry.update(update)
            store()

    def halt_live() -> None:
        with lock:
            targets = [entry["child"] for entry in live.values() if entry.get("child")]
        for child in targets:
            try:
                parent.halt(child)
            except Exception as error:  # noqa: BLE001 - the runner's own reap still confirms
                print(f"sandbox: fork {child} could not be halted: {error}")

    cancellation = _HaltingCancellation(halt_live)

    def runner(request: NodeRequest) -> NodeResult:
        node = nodes[request.node.id]
        with lock:
            progress["agent_calls"] = int(progress["agent_calls"]) + 1
            call = progress["agent_calls"]
            entry: dict[str, Any] = {
                "node_id": node.id,
                "agent_call": call,
                "key": _fork_key(ctx, layer, node.id, call, checkpoint),
                "ttl_s": ttl_s,
                "child": None,
                "reaped": False,
            }
            wave["children"].append(entry)
            store()
        started = time.monotonic()
        try:
            if cancellation.cancelled:
                # Halted before it asked the provider for anything: nothing exists to reap, and the
                # row must say so rather than read like a fork whose fate is unknown.
                journal(entry, cancelled_before_fork=True, reaped=True)
                raise _Cancelled()
            # Durable before the call: a fork whose answer is lost is reclaimed by this key, and a
            # key whose fork was never requested must not be "reclaimed" into a brand-new VM.
            journal(entry, fork_requested=True, requested_at=time.time())
            try:
                child = parent.fork_child(
                    key=entry["key"], ttl_s=ttl_s, record=lambda child_id: journal(entry, child=child_id)
                )
            except ForkRefused:
                journal(entry, fork_refused=True, reaped=True)  # refused outright: nothing to reap
                raise
            with lock:
                live[node.id] = entry
            if cancellation.cancelled:
                raise _Cancelled()
            child_ctx = dataclasses.replace(ctx, sb=child)
            observed = _prove_lineage(child_ctx, checkpoint, node.id)
            result = stages._agent(child_ctx, "build", call, prompts[node.id], BuildSummary)
            if stages._workspace_head(child_ctx) != checkpoint:
                raise StageError("policy", f"fork {node.id}: the agent moved HEAD; only the factory commits")
            message = f"work {node.id}: {stages._summary_line(result, node.title)}"
            output = stages.commit_unfenced(child_ctx, stage=f"work:{node.id}", msg=message)
            harvest = _harvest(child_ctx, node, entry, checkpoint=checkpoint, output=output, message=message)
            with lock:
                harvests[node.id] = harvest
            return NodeResult(
                logical_id=request.logical_id,
                node_id=node.id,
                state="ok",
                input_head=observed,
                output_head=output,
                touched_files=harvest.touched,
                artifacts=(harvest.patch_blob,),
                provider_incarnation=entry["child"],
                duration_s=time.monotonic() - started,
            )
        except BaseException as error:
            collateral = isinstance(error, _Cancelled) or cancellation.cancelled
            with lock:
                if not collateral:
                    primary[node.id] = error
            return NodeResult(
                logical_id=request.logical_id,
                node_id=node.id,
                state="cancelled" if collateral else "failed",
                input_head=checkpoint,
                output_head=None,
                provider_incarnation=entry.get("child"),
                duration_s=time.monotonic() - started,
                detail=f"{type(error).__name__}: {error}"[:2000],
            )
        finally:
            with lock:
                live.pop(node.id, None)
            _reap_entry(parent, entry, journal)

    def unpark() -> None:
        nonlocal resumed
        if resumed:
            return
        parent.resume_parent()
        resumed = True
        journal(wave, parked=False)

    def merger(result: NodeResult, target_head: str, index: int) -> MergeReceipt:
        if not resumed:
            unpark()
            # The parent sat parked through the whole wave; it must come back exactly where it left.
            stages._assert_workspace_head(ctx, f"fork wave {layer} fan-in")
        harvest = harvests[result.node_id]
        after = merge(nodes[result.node_id], harvest, target_head)
        receipt = MergeReceipt(
            index=index,
            node_id=result.node_id,
            source_head=harvest.output_head,
            target_before=target_head,
            target_after=after,
            transform="patch_apply",
            requires_reverify=True,
        )
        merges.append(dataclasses.asdict(receipt))
        return receipt

    executor = WorkExecutor(
        runner,
        merger,
        ExecutorPolicy(
            max_parallel=ctx.cfg.max_parallel_forks,
            max_attempts=1,  # a retry is a new fork, a new call and a new key: the task retry's job
            protected_paths=protected,
        ),
    )

    with lock:
        progress["fork_wave"] = wave
        store()
    report: ExecutionReport | None = None
    outcome: BaseException | None = None
    try:
        wave["parent"] = parent.fork_parent()
        journal(wave)
        report = executor.execute(
            cell_id=cell_id or f"run:{ctx.cfg.run_id}",
            epoch=epoch or 0,
            input_head=checkpoint,
            nodes=_work_nodes(pending),
            supports_fork=True,
            cancellation=cancellation,
        )
    except BaseException as error:  # a refused park or merge, a timeout, a signal: teardown first
        outcome = error
    debt: list[str] = []
    try:
        # Runners reap their own children; this catches any whose runner never reached `finally`,
        # and -- while the parent is still parked -- any child whose fork answer never arrived.
        if not resumed:
            _reclaim_lost(parent, wave, journal)
        debt = _reap_all(parent, wave, journal)
    except BaseException as error:  # noqa: BLE001 - a journal that cannot be written is debt too
        debt.append(f"teardown journal: {str(error)[:200]}")
    finally:
        # A lost fork can be reclaimed only from a parked parent -- a running one might fork anew
        # from a periodic snapshot -- so it stays parked until recover or teardown finds the child.
        if not resumed and not _lost(wave, journal):
            try:
                unpark()
            except Exception as error:  # noqa: BLE001 - `parked` stays true; recover/teardown resume it
                debt.append(f"parent {wave['parent']} not resumed: {str(error)[:200]}")

    receipt = {
        "layer": layer,
        "checkpoint": checkpoint,
        "fork_parent": wave["parent"],
        "parallel": bool(report and report.parallel),
        "max_parallel_forks": ctx.cfg.max_parallel_forks,
        "ttl_s": ttl_s,
        "children": [_child_receipt(entry, harvests.get(entry["node_id"])) for entry in _by_node(wave)],
        "results": [
            {"node_id": r.node_id, "state": r.state, "duration_s": round(r.duration_s, 3), "detail": r.detail}
            for r in (report.results if report else ())
        ],
        "merges": merges,
        "conflicts": [dataclasses.asdict(conflict) for conflict in (report.conflicts if report else ())],
        "error": f"{type(outcome).__name__}: {outcome}"[:2000] if outcome else None,
        "teardown": "confirmed" if not debt else "debt",
        "duration_s": round(time.time() - wave["started_at"], 3),
    }
    with lock:
        progress.setdefault("fork_waves", []).append(receipt)
        if not debt:
            progress["fork_wave"] = None
        store()
    first_failure = primary[min(primary)] if primary else None
    if debt:
        cause = outcome or first_failure
        detail = f"; the wave itself failed first: {cause}" if cause else ""
        raise StageError(
            "sandbox",
            f"fork wave {layer}: teardown not confirmed: {debt}; a retry or the teardown task reaps them{detail}"[
                :4000
            ],
            retryable=True,
        ) from cause
    if outcome is not None:
        raise outcome
    assert report is not None
    if first_failure is not None:
        if not merges and all(isinstance(error, ForkRefused) for error in primary.values()):
            raise ForkUnavailable(f"fork wave {layer}: the provider refused to fork: {first_failure}"[:2000])
        raise first_failure
    if report.conflicts:
        detail = "; ".join(f"{c.kind} {c.left}/{c.right} {list(c.files)} {c.detail}".strip() for c in report.conflicts)
        raise StageError("policy", f"fork wave {layer} refused before fan-in: {detail}")
    if len(merges) != len(pending):
        states = {r.node_id: r.state for r in report.results}
        raise StageError("policy", f"fork wave {layer} did not fan in every node: {states}")
    return receipt


def recover(ctx: stages.Ctx) -> None:
    """Settle a fork wave a dead process left in flight, before anything fences the parent.

    Reaps every journalled child (reclaiming, by idempotency key, any whose id never reached the
    journal) and unparks the parent. Children that had not fanned in are simply run again, as
    new forks with new keys: nothing a dead wave produced is a candidate.
    """
    loaded = _in_flight(ctx)
    if loaded is None:
        return
    progress, wave, store, journal = loaded
    parent = ctx.sb
    if not isinstance(parent, ForkableSandbox):
        raise StageError("policy", "a fork wave is in flight but the configured sandbox cannot fork")
    if wave.get("parked"):
        _reclaim_lost(parent, wave, journal)
    debt = _reap_all(parent, wave, journal)
    if wave.get("parked") and not _lost(wave, journal):
        parent.resume_parent()
        journal(wave, parked=False)
    if debt:
        raise StageError("sandbox", f"recovered fork wave left children unconfirmed: {debt}", retryable=True)
    progress.setdefault("fork_waves", []).append({**wave, "recovered": True, "teardown": "confirmed"})
    progress["fork_wave"] = None
    store()


def teardown(ctx: stages.Ctx) -> None:
    """Reap what an unfinished fork wave left, for the teardown task, before it closes the parent.

    The build task may never be retried (a killed worker, a teardown-debt error), so ``recover``
    alone would leave children billing until their lease. The parent is not resumed: the teardown
    task closes it next. Raises when a child cannot be confirmed stopped, after recording it.
    """
    loaded = _in_flight(ctx)
    if loaded is None or not isinstance(ctx.sb, ForkableSandbox):
        return
    progress, wave, store, journal = loaded
    if wave.get("parked"):
        _reclaim_lost(ctx.sb, wave, journal)
    debt = _reap_all(ctx.sb, wave, journal)
    if debt:
        raise StageError("sandbox", f"teardown could not confirm fork children stopped: {debt}")
    progress.setdefault("fork_waves", []).append({**wave, "torn_down": True, "teardown": "confirmed"})
    progress["fork_wave"] = None
    store()


# ---------------------------------------------------------------- internals


class _Cancelled(Exception):
    """A sibling failed first; this node's work is not a candidate."""


class _HaltingCancellation(Cancellation):
    """``WorkExecutor`` cancellation bound to real termination: the first cancel halts every live
    child at the provider, so an in-flight sibling stops spending instead of finishing for nothing."""

    def __init__(self, on_cancel: Callable[[], None]) -> None:
        super().__init__()
        self._on_cancel = on_cancel
        self._once = threading.Lock()

    def cancel(self) -> None:
        with self._once:
            first = not self.cancelled
            super().cancel()
        if first:
            self._on_cancel()


def _in_flight(
    ctx: stages.Ctx,
) -> tuple[dict[str, Any], dict[str, Any], Callable[[], None], Callable[..., None]] | None:
    if not ctx.state.has_control(PROGRESS_FILE):
        return None
    try:
        progress = json.loads(ctx.state.read_control(PROGRESS_FILE))
    except ValueError as error:
        raise StageError("policy", f"workgraph progress is corrupt: {error}") from error
    wave = progress.get("fork_wave") if isinstance(progress, dict) else None
    if not wave:
        return None

    def store() -> None:
        ctx.state.write_control(PROGRESS_FILE, json.dumps(progress, indent=2, sort_keys=True) + "\n")

    def journal(entry: dict[str, Any], **update: Any) -> None:
        entry.update(update)
        store()

    return progress, wave, store, journal


def _by_node(wave: dict[str, Any]) -> list[dict[str, Any]]:
    return sorted(wave.get("children", []), key=lambda row: row["node_id"])


def _child_receipt(entry: dict[str, Any], harvest: Harvest | None) -> dict[str, Any]:
    fields = (
        "node_id",
        "agent_call",
        "key",
        "child",
        "reaped",
        "fork_refused",
        "cancelled_before_fork",
        "teardown_error",
    )
    row = {k: entry.get(k) for k in fields}
    if harvest is not None:
        row |= {
            "child_input_head": harvest.input_head,
            "child_output_head": harvest.output_head,
            "touched": list(harvest.touched),
            "patch_blob": harvest.patch_blob,
        }
    return {k: v for k, v in row.items() if v is not None or k == "child"}


def _work_nodes(pending: Sequence[PlanTask]) -> tuple[WorkNode, ...]:
    # Dependencies are already merged into the checkpoint, so within a wave every node is a root.
    return tuple(WorkNode(id=node.id, files=tuple(node.files), parallel_safe=node.parallel_safe) for node in pending)


def _fork_key(ctx: stages.Ctx, layer: int, node_id: str, call: int, checkpoint: str) -> str:
    raw = "\0".join((ctx.cfg.run_id, ctx.issue.id, str(layer), node_id, str(call), checkpoint)).encode()
    return "swf-fork-" + hashlib.sha256(raw).hexdigest()[:40]


def _prove_lineage(child_ctx: stages.Ctx, checkpoint: str, node_id: str) -> str:
    head = stages._workspace_head(child_ctx)
    if head != checkpoint:
        raise StageError(
            "policy",
            f"fork {node_id}: child is at {head} but the parent was parked at {checkpoint}; "
            "a stale snapshot is never a parent",
        )
    stages._ensure_clean(child_ctx, f"fork {node_id} lineage", allow_artifacts=True)
    return head


def _git_blob(content: str) -> str:
    data = content.encode("utf-8")
    return hashlib.sha1(b"blob %d\0" % len(data) + data, usedforsecurity=False).hexdigest()


def _harvest(
    child_ctx: stages.Ctx,
    node: PlanTask,
    entry: dict[str, Any],
    *,
    checkpoint: str,
    output: str,
    message: str,
) -> Harvest:
    q = shlex.quote
    span = f"{q(checkpoint)}..{q(output)}"
    # --no-renames: a rename reports only its new path, which would hide the deletion from the
    # declared-scope, overlap and protected-path checks.
    touched_text = stages._sh(child_ctx, f"git diff --name-only --relative {span} --no-renames -- .")
    raw_diff = stages._sh(child_ctx, f"git diff --raw --no-abbrev --relative {span} --no-renames -- .")
    patch_rel = f"{PATCH_DIR}/{node.id}.patch"
    # Repository-root paths (no --relative), limited to the target: `git apply` in the parent runs
    # from the top level, which is right for a git-init target and a full clone alike.
    stages._sh(
        child_ctx,
        f"mkdir -p {q(PATCH_DIR)} && "
        f"git diff --binary --full-index --no-renames {q(checkpoint)} {q(output)} -- . > {q(patch_rel)}",
    )
    patch = child_ctx.sb.read(patch_rel)
    in_child = stages._sh(child_ctx, f"git hash-object --no-filters {q(patch_rel)}").strip()
    if in_child != _git_blob(patch):
        raise StageError("sandbox", f"fork {node.id}: the patch read back differs from the one in the child")
    return Harvest(
        node_id=node.id,
        child=str(entry["child"]),
        key=str(entry["key"]),
        agent_call=int(entry["agent_call"]),
        input_head=checkpoint,
        output_head=output,
        message=message,
        touched=tuple(sorted(line.strip() for line in touched_text.splitlines() if line.strip())),
        raw_diff=raw_diff,
        patch=patch,
        patch_blob=in_child,
    )


def _reclaim_lost(parent: ForkableSandbox, wave: dict[str, Any], journal: Callable[..., None]) -> None:
    """Find, by idempotency key, every requested child whose id never reached the journal."""
    for entry in wave.get("children", []):
        if entry.get("fork_requested") and not entry.get("child") and not entry.get("reaped"):
            try:
                found = parent.reclaim_fork(key=entry["key"], ttl_s=int(entry.get("ttl_s") or 3600))
            except Exception as error:  # noqa: BLE001 - unknown is debt, recorded on the entry
                journal(entry, teardown_error=f"reclaim: {str(error)[:300]}")
                continue
            journal(entry, child=found, reaped=found is None)


def _reap_entry(parent: ForkableSandbox, entry: dict[str, Any], journal: Callable[..., None]) -> str | None:
    child = entry.get("child")
    if not child or entry.get("reaped"):
        return None
    try:
        parent.reap(child)
    except Exception as error:  # noqa: BLE001 - recorded as teardown debt, never swallowed
        journal(entry, teardown_error=str(error)[:400])
        return child
    journal(entry, reaped=True)
    return None


def _reap_all(parent: ForkableSandbox, wave: dict[str, Any], journal: Callable[..., None]) -> list[str]:
    debt = [_reap_entry(parent, entry, journal) for entry in wave.get("children", [])]
    return [child for child in debt if child] + [f"key {entry['key']}" for entry in _lost(wave, journal)]


def _lost(wave: dict[str, Any], journal: Callable[..., None]) -> list[dict[str, Any]]:
    """Requested forks whose child was never identified: unknown, not absent -- until the lease
    they were requested with has certainly run out, after which the provider has stopped them."""
    lost = []
    for entry in wave.get("children", []):
        if not entry.get("fork_requested") or entry.get("child") or entry.get("reaped"):
            continue
        expires = float(entry.get("requested_at") or 0.0) + int(entry.get("ttl_s") or 0) + LEASE_GRACE_S
        if entry.get("requested_at") and time.time() >= expires:
            journal(entry, reaped=True, lease_expired=True)
            continue
        lost.append(entry)
    return lost
