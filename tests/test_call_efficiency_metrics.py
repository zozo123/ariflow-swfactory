"""``metrics.json`` ``agent_calls`` and ``model_turns``, summed from the host-owned call ledger.

The harness-efficiency contract asks for model turns per run; before these keys the numbers lived
only in each call's settle receipt, so a change meant to cut turns (the dialect card, a per-stage
``max_turns``) had nothing in the run metrics to move.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from swfactory.agent import ScriptedAgent
from swfactory.approval_policy import SCRIPTED_REPLAY_FIXTURE
from swfactory.blueprint import load
from swfactory.call_accounting import CallLedger
from swfactory.cli import execute
from swfactory.metrics import call_efficiency
from swfactory.replica_fusion import CAMPAIGN_STAGE
from swfactory.state import RunState

ROOT = Path(__file__).resolve().parents[1]


def _ctx(run_dir: Path) -> SimpleNamespace:
    return SimpleNamespace(state=RunState(run_dir), run_dir=run_dir)


def test_calls_and_turns_are_summed_from_settle_receipts(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    ledger = CallLedger(RunState(run_dir))
    for stage, turns in (("plan", 5), ("build", 7)):
        ledger.settle(
            ledger.reserve(stage=stage, iteration=1, reserved_usd=1.0), cost_usd=0.5, receipt={"num_turns": turns}
        )
    # A call that never answered: it left the process, so it counts as a call, but its turns are
    # unknown and are not guessed.
    ledger.mark_unknown(ledger.reserve(stage="fix", iteration=2, reserved_usd=1.0), "killed")
    # A receipt whose turn count is not a non-negative integer degrades to zero, never a crash.
    for junk in ("9", -1, True, None):
        ledger.settle(
            ledger.reserve(stage="review", iteration=1, reserved_usd=1.0), cost_usd=0.0, receipt={"num_turns": junk}
        )

    assert call_efficiency(_ctx(run_dir)) == {"agent_calls": 7, "model_turns": 12}


def test_a_fusion_campaign_counts_its_workers_calls_not_its_reservation(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    parent = CallLedger(RunState(run_dir))
    parent.settle(parent.reserve(stage="plan", iteration=1, reserved_usd=1.0), cost_usd=0.1, receipt={"num_turns": 4})
    campaign = parent.reserve(stage=CAMPAIGN_STAGE, iteration=1, reserved_usd=5.0)
    parent.settle(campaign, cost_usd=2.0, receipt={"fusion_pin": "sha256:x"})
    for identity, turns in (("repair-a", 6), ("repair-b", 9)):
        worker = CallLedger(RunState(run_dir / "fusion" / "workers" / identity))
        worker.settle(
            worker.reserve(stage="build", iteration=1, reserved_usd=1.0), cost_usd=1.0, receipt={"num_turns": turns}
        )

    assert call_efficiency(_ctx(run_dir)) == {"agent_calls": 3, "model_turns": 19}


def test_a_run_without_calls_reports_zero(tmp_path: Path) -> None:
    assert call_efficiency(_ctx(tmp_path / "run")) == {"agent_calls": 0, "model_turns": 0}


def test_delivered_metrics_carry_the_ledger_totals(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The scripted demo: spec, plan, build.1, fix.2 and review.1 are five provider calls (the
    ScriptedAgent reports zero turns), and every one of them is in the delivered metrics.json."""
    monkeypatch.chdir(ROOT)
    bp = load("factory")
    (job,) = bp.jobs({"issues": ["demo/issue.md"]})
    cfg = bp.config(
        job,
        run_id="ca115000",
        gate_replay=str(SCRIPTED_REPLAY_FIXTURE),
        agent="scripted",
        sandbox="local",
        scm="local",
        workdir=str(tmp_path / "work"),
    )
    report = execute(cfg, run_dir=tmp_path / "run", agent=ScriptedAgent([ROOT / "demo" / "scripted"]), blueprint=bp)
    assert report.tests_passed is True

    metrics = json.loads((tmp_path / "work" / "docs" / "factory" / "DEMO-1" / "metrics.json").read_text())
    records = CallLedger(RunState(tmp_path / "run")).records()
    assert [(row.stage, row.iteration) for row in records] == [
        ("spec", 1),
        ("plan", 1),
        ("build", 1),
        ("fix", 2),
        ("review", 1),
    ]
    assert metrics["agent_calls"] == 5
    assert metrics["model_turns"] == 0
