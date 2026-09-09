"""Money is spent at the provider, not when a stage returns.

Every test here breaks the window between "the provider was paid" and "the orchestrator wrote it
down": a call that dies inside the provider, a call whose receipt cannot be read back, a hard kill
(SIGKILL, no handlers) between the two, a failed call, and a restart that has to rebuild the run
ceiling from what survived. The invariant under test is one-directional: a call whose usage is
unknown keeps its reservation charged until an *observation* says otherwise. Money that might
already be spent is never available again on a guess -- that is how a run walks past its ceiling.

Hermetic: an in-memory sandbox and scripted agents, no provider, no network.
"""

from __future__ import annotations

import json
import os
import signal
from pathlib import Path
from typing import Any

import pytest

from swfactory.call_accounting import CallLedger, ReconciliationRefused
from swfactory.config import Config
from swfactory.idempotency import MutationOutcome
from swfactory.models import AgentResult, Issue, RunResult, StageError
from swfactory.stages import Ctx, _agent, seed_budget, unreconciled_spend

ISSUE_ID = "X-1"
ART = f"docs/factory/{ISSUE_ID}"
FACTORY_TOML = '[commands]\ntest = "pytest"\n[paths]\nsource = "src"\ntests = "tests"\n'


class MemSandbox:
    """An agent-writable workdir; the envelope read can be broken on purpose."""

    name = "mem"
    workdir = "/work"

    def __init__(self, files: dict[str, str] | None = None, *, unreadable: str | None = None) -> None:
        self.files = {"factory.toml": FACTORY_TOML, **(files or {})}
        self.unreadable = unreadable

    def ensure(self) -> None: ...

    def close(self) -> None: ...

    def run(self, cmd: str, *, cwd: str | None = None, timeout_s: int = 1800) -> RunResult:
        del cmd, cwd, timeout_s
        return RunResult(0, "", "", 0.0)

    def run_agent(self, cmd: str, *, timeout_s: int = 1800) -> RunResult:
        return self.run(cmd, timeout_s=timeout_s)

    def read(self, path: str) -> str:
        if path == self.unreadable:
            raise OSError("the cell died while the envelope was being read back")
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]

    def write(self, path: str, content: str) -> None:
        self.files[path] = content

    def exists(self, path: str) -> bool:
        return path in self.files or path == self.unreadable


class PeekAgent:
    """Reports what the durable ledger held at the instant the provider was invoked."""

    kind = "scripted"

    def __init__(self, run_dir: Path, *, cost: float = 0.0) -> None:
        self.run_dir = run_dir
        self.cost = cost
        self.at_call: list[dict[str, Any]] = []
        self.granted: list[float] = []

    def run(self, sb: Any, *, cfg: Config, **kw: Any) -> AgentResult:
        del sb
        self.at_call = rows(self.run_dir)
        self.granted.append(cfg.max_budget_usd_per_stage)
        return AgentResult(agent="scripted", text="# spec\n", cost_usd=self.cost, session_id="sess-9", num_turns=4)


class DyingAgent:
    """A provider call that never comes back: the request left, the answer did not."""

    kind = "scripted"

    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.calls = 0

    def run(self, sb: Any, **kw: Any) -> AgentResult:
        del sb, kw
        self.calls += 1
        raise self.error


class SuicidalAgent:
    """A hard kill inside the provider call: no except branch, no finally, no atexit."""

    kind = "scripted"

    def run(self, sb: Any, **kw: Any) -> AgentResult:  # pragma: no cover - the process dies here
        del sb, kw
        os.kill(os.getpid(), signal.SIGKILL)
        raise AssertionError("unreachable")


class ErroringAgent:
    """A call the provider charged for and then reported as an error."""

    kind = "scripted"

    def run(self, sb: Any, **kw: Any) -> AgentResult:
        del sb, kw
        return AgentResult(
            agent="scripted",
            cost_usd=0.42,
            is_error=True,
            subtype="error_max_turns",
            session_id="sess-err",
            text="limit reached",
        )


def ctx_on(tmp_path: Path, sb: MemSandbox, agent: Any = None, **cfg: Any) -> Ctx:
    return Ctx(
        cfg=Config(issue="x", run_id="acct0001", **cfg),
        sb=sb,  # type: ignore[arg-type]
        agent=agent,  # type: ignore[arg-type]
        scm=None,  # type: ignore[arg-type]
        issue=Issue(id=ISSUE_ID, title="t", body="b"),
        run_dir=tmp_path / "run",
    )


def rows(run_dir: Path) -> list[dict[str, Any]]:
    path = run_dir / "state" / "agent_calls.jsonl"
    if not path.is_file():
        return []
    # Call rows only. The ledger also holds one `floor_usd` row -- the money a run spent before per-call
    # accounting existed, adopted once -- and these tests reason about calls.
    parsed = (json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    return [row for row in parsed if "call_id" in row]


# ================================================================ 1. reservation before the call


def test_a_conservative_reservation_is_journalled_before_the_provider_is_invoked(tmp_path: Path) -> None:
    """The reservation has to be durable *before* the money can be spent, and it has to be the
    most the call could cost -- the very ceiling handed to the provider -- because that is the
    only bound that still holds when nobody comes back to write down the truth."""
    agent = PeekAgent(tmp_path / "run")
    ctx = ctx_on(tmp_path, MemSandbox(), agent, max_budget_usd=8.0, max_budget_usd_per_stage=2.0)

    _agent(ctx, "spec", 1, "prompt", None)

    assert len(agent.at_call) == 1, "the provider ran with nothing durable behind it"
    (reserved,) = agent.at_call
    assert (reserved["event"], reserved["state"]) == ("reserved", "intent")
    assert (reserved["stage"], reserved["iteration"]) == ("spec", 1)
    assert reserved["reserved_usd"] == agent.granted[0] == 2.0  # exactly the ceiling the call got
    assert reserved["owner"] and reserved["pid"] == os.getpid() and reserved["lease_until"] > 0


def test_the_reservation_shrinks_to_what_the_run_ceiling_still_allows(tmp_path: Path) -> None:
    """A reservation larger than the remaining ceiling would over-book the run; smaller than the
    granted per-call ceiling would under-book the one call that is about to spend it."""
    agent = PeekAgent(tmp_path / "run")
    ctx = ctx_on(tmp_path, MemSandbox(), agent, max_budget_usd=1.25, max_budget_usd_per_stage=2.0)

    _agent(ctx, "spec", 1, "prompt", None)

    assert agent.at_call[0]["reserved_usd"] == agent.granted[0] == 1.25


# ================================================================ 2. settlement and receipt


def test_settled_usage_and_its_receipt_land_before_any_further_sandbox_io(tmp_path: Path) -> None:
    """The envelope download is sandbox I/O that can fail or hang. The charge and the receipt are
    already durable by then, so a cell that dies one line later still leaves the money written."""
    envelope = f"{ART}/agent/spec.1.json"
    sb = MemSandbox(unreadable=envelope)
    ctx = ctx_on(tmp_path, sb, PeekAgent(tmp_path / "run", cost=0.75), max_budget_usd=8.0)

    with pytest.raises(OSError, match="the cell died"):
        _agent(ctx, "spec", 1, "prompt", None)

    settled = rows(tmp_path / "run")[-1]
    assert (settled["event"], settled["state"]) == ("settled", "committed")
    assert settled["cost_usd"] == 0.75
    assert settled["receipt"]["session_id"] == "sess-9" and settled["receipt"]["num_turns"] == 4
    assert not (tmp_path / "run" / "state" / "stages.jsonl").exists(), "no stage returned to write it"
    ledger = CallLedger(ctx.state)
    assert ledger.charged_usd() == 0.75 and ledger.unreconciled() == []


def test_a_failed_call_is_charged_at_what_the_provider_billed(tmp_path: Path) -> None:
    """A refusal is not a refund: the turns were bought. The receipt keeps the subtype so an
    operator can see which money bought an error."""
    ctx = ctx_on(tmp_path, MemSandbox(), ErroringAgent(), max_budget_usd=8.0)

    with pytest.raises(StageError, match="spec.1 failed: error_max_turns"):
        _agent(ctx, "spec", 1, "prompt", None)

    settled = rows(tmp_path / "run")[-1]
    assert settled["state"] == "committed" and settled["cost_usd"] == 0.42
    assert settled["receipt"]["subtype"] == "error_max_turns" and settled["receipt"]["is_error"] is True
    assert seed_budget(ctx_on(tmp_path, MemSandbox(), max_budget_usd=8.0)) == 0.42


# ================================================================ 3. unknown spend


def test_a_call_that_never_answers_is_tracked_as_unknown_and_keeps_its_reservation(tmp_path: Path) -> None:
    """A timeout says nothing about the provider's ledger, only about ours. The reservation stays
    charged and stays visible as unreconciled: an unknown call is money that might be gone."""
    died = StageError("sandbox", "claude wrote no output (timed_out=True)", retryable=True)
    ctx = ctx_on(tmp_path, MemSandbox(), DyingAgent(died), max_budget_usd=8.0, max_budget_usd_per_stage=2.0)

    with pytest.raises(StageError, match="timed_out=True"):
        _agent(ctx, "spec", 1, "prompt", None)

    unknown = rows(tmp_path / "run")[-1]
    assert (unknown["event"], unknown["state"]) == ("unknown", "in_doubt")
    assert unknown["cost_usd"] is None and "timed_out=True" in unknown["reason"]
    # Through the guard's own path, not the raw counter. The first version asserted `ctx.spent_usd
    # == 2.0` here, which required `_agent` to add the reservation to the in-memory counter -- and
    # that is how the phantom charge reached the stage log via `_timed`, where no reconciliation
    # could ever remove it. The ledger holds the unknown money; the guard refreshes from the ledger.
    assert seed_budget(ctx, refresh=True) == 2.0, "the in-process guard must see the unknown charge too"
    restarted = ctx_on(tmp_path, MemSandbox(), max_budget_usd=8.0)
    assert seed_budget(restarted) == 2.0 and unreconciled_spend(restarted) == 2.0


def test_a_hard_kill_between_the_call_and_its_receipt_still_costs_the_run(tmp_path: Path) -> None:
    """SIGKILL inside the provider call runs no handler at all, so only what was written *before*
    the call survives. Without a durable reservation the next process reads a budget with more
    money in it than the account has."""
    child = os.fork()
    if child == 0:  # pragma: no cover - killed mid-call, never reports coverage
        ctx = ctx_on(tmp_path, MemSandbox(), SuicidalAgent(), max_budget_usd=8.0, max_budget_usd_per_stage=2.0)
        _agent(ctx, "build", 1, "prompt", None)
        os._exit(97)  # unreachable: reaching it would mean the kill did not land
    _, status = os.waitpid(child, 0)

    assert os.WIFSIGNALED(status) and os.WTERMSIG(status) == signal.SIGKILL
    (reserved,) = rows(tmp_path / "run")
    assert reserved["state"] == "intent" and reserved["reserved_usd"] == 2.0
    restarted = ctx_on(tmp_path, MemSandbox(), max_budget_usd=8.0)
    assert seed_budget(restarted) == 2.0 and unreconciled_spend(restarted) == 2.0


def test_the_run_ceiling_survives_the_task_that_lost_its_answer(tmp_path: Path) -> None:
    """Two Airflow tasks, two processes, one run ceiling. The second task inherits the first
    task's unknown call as spent money and refuses rather than buying one more."""
    died = StageError("sandbox", "cell terminated during the call", retryable=True)
    first = ctx_on(tmp_path, MemSandbox(), DyingAgent(died), max_budget_usd=2.0, max_budget_usd_per_stage=2.0)
    with pytest.raises(StageError, match="cell terminated"):
        _agent(first, "build", 1, "prompt", None)

    second = PeekAgent(tmp_path / "run")
    task2 = ctx_on(tmp_path, MemSandbox(), second, max_budget_usd=2.0, max_budget_usd_per_stage=2.0)
    with pytest.raises(StageError, match=r"run budget exhausted before build\.2 \(2\.00 USD of it unreconciled\)"):
        _agent(task2, "build", 2, "prompt", None)

    assert second.granted == [], "the provider must not be invoked past the ceiling"


# ================================================================ 4. reconciliation


def test_an_unknown_charge_is_released_only_by_a_recorded_observation(tmp_path: Path) -> None:
    """Reuses the operation journal's outcome vocabulary: only ``committed`` (with the usage the
    provider actually reports) or ``definitely_absent`` may move money back. Anything short of
    that -- including a claim with no usage attached -- leaves the reservation standing."""
    ledger = CallLedger(ctx_on(tmp_path, MemSandbox()).state)
    ambiguous = ledger.reserve(stage="build", iteration=1, reserved_usd=2.0)
    absent = ledger.reserve(stage="build", iteration=2, reserved_usd=2.0)
    billed = ledger.reserve(stage="build", iteration=3, reserved_usd=2.0)
    for attempt in (ambiguous, absent, billed):
        ledger.mark_unknown(attempt, "the cell died mid-call")
    assert ledger.charged_usd() == 6.0

    ledger.reconcile(ambiguous.call_id, MutationOutcome("ambiguous", detail="provider has no usage yet"))
    assert ledger.charged_usd() == 6.0, "an inconclusive look is not an observation"
    with pytest.raises(ReconciliationRefused, match="usage"):
        ledger.reconcile(billed.call_id, MutationOutcome("committed"))
    assert ledger.charged_usd() == 6.0

    ledger.reconcile(absent.call_id, MutationOutcome("definitely_absent", detail="no such request"))
    ledger.reconcile(billed.call_id, MutationOutcome("committed", 0.37, {"invoice": "inv-1"}))
    assert ledger.charged_usd() == pytest.approx(2.37)
    assert [record.call_id for record in ledger.unreconciled()] == [ambiguous.call_id]
    settled = ledger.record(billed.call_id)
    assert settled.observation == {"status": "committed", "evidence": {"invoice": "inv-1"}, "detail": None}


def test_a_settled_receipt_is_immutable(tmp_path: Path) -> None:
    """A committed charge is evidence, not a running total; overwriting it would let a later,
    cheaper observation erase money the provider already took."""
    ledger = CallLedger(ctx_on(tmp_path, MemSandbox()).state)
    attempt = ledger.reserve(stage="build", iteration=1, reserved_usd=2.0)
    ledger.settle(attempt, cost_usd=1.5, receipt={"session_id": "s1"})

    with pytest.raises(ReconciliationRefused, match="already settled"):
        ledger.reconcile(attempt.call_id, MutationOutcome("definitely_absent"))
    assert ledger.charged_usd() == 1.5


def test_money_spent_before_the_ledger_existed_is_added_to_it_not_maxed_with_it(tmp_path: Path) -> None:
    """A run resumed across the deploy that introduced the ledger has cost in two DISJOINT records.

    The first version seeded `max(stage_log, ledger)`. With 5.50 in the pre-ledger stage log and
    three settled 1.50 calls, that returned 5.50 every time while real spend climbed to 10.00 --
    past an 8.00 ceiling -- and the provider was invoked. The stage-log money is adopted once as a
    floor and the ledger's calls are added on top.
    """
    from swfactory.models import StageResult
    from swfactory.stages import RUN_STAGES_LOG

    ctx = ctx_on(tmp_path, MemSandbox(), max_budget_usd=8.0, max_budget_usd_per_stage=2.0)
    ctx.state.append_json(
        RUN_STAGES_LOG,
        StageResult(stage="spec", status="ok", duration_s=1.0, cost_usd=5.5).model_dump(mode="json"),
    )
    assert seed_budget(ctx) == 5.5  # adopts the floor

    ledger = CallLedger(ctx.state)
    for i in range(1, 4):
        ledger.settle(ledger.reserve(stage="build", iteration=i, reserved_usd=2.0), cost_usd=1.5)
    assert seed_budget(ctx, refresh=True) == 10.0, "pre-ledger money and ledger money are disjoint"

    with pytest.raises(StageError, match="run budget exhausted"):
        _agent(ctx, "build", 4, "prompt", None)


def test_reconciling_a_dead_call_as_never_sent_returns_its_money(tmp_path: Path) -> None:
    """The point of a reservation is that an observation can release it.

    The first version pushed the reservation into `ctx.spent_usd`, so the `_timed` wrapper wrote it
    into the stage log as the failed stage's cost. After `definitely_absent` -- proof the provider
    never saw the request -- the ledger charge fell to 0.00 but the stage-log floor kept the run
    pinned at 2.00. Forever. Money reconciliation cannot return is not a reservation, it is a fine.
    """
    from swfactory.stages import _timed

    died = StageError("sandbox", "cell terminated during the call", retryable=True)
    ctx = ctx_on(tmp_path, MemSandbox(), DyingAgent(died), max_budget_usd=8.0, max_budget_usd_per_stage=2.0)

    # Through the stage wrapper, deliberately. `_timed` writes `spent_usd - spent0` into the stage
    # log as the failed stage's cost; calling `_agent` directly bypasses that and the test passes
    # against the very code it is meant to catch.
    def dying_stage(ctx: Ctx) -> Any:
        return _agent(ctx, "spec", 1, "prompt", None)

    with pytest.raises(StageError, match="cell terminated"):
        _timed(dying_stage)(ctx)  # type: ignore[arg-type]
    assert seed_budget(ctx, refresh=True) == 2.0 and unreconciled_spend(ctx) == 2.0

    ledger = CallLedger(ctx.state)
    (unknown,) = ledger.unreconciled()
    ledger.reconcile(
        unknown.call_id,
        MutationOutcome(status="definitely_absent", result=None, evidence=None, detail="not in provider log"),
    )
    restarted = ctx_on(tmp_path, MemSandbox(), max_budget_usd=8.0)
    assert seed_budget(restarted) == 0.0, "an observed-absent call must free its reservation"
    assert unreconciled_spend(restarted) == 0.0
