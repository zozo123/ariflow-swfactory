"""The winning-harness mechanisms working together on one scripted run.

Each mechanism is pinned on its own elsewhere (``test_ratchet``, ``test_regression_guard``,
``test_adherence``, ``test_target_dialect``, ``test_stages_blocker`` for the trace). These tests pin
the seams between them, where one mechanism's output is another's input:

* the ratchet moves HEAD back to the best candidate; the delivery guard must judge exactly the head
  that was tested last (``regression.json`` ``tested_head``), never the reverted attempt;
* a reverted call still ran and still cost: ``metrics.agent_calls`` counts it, the adherence audit
  accounts for it without calling the run deviant or unverifiable;
* the fix prompt after a revert carries both the ratchet note (in ``{failures}``) and the target's
  dialect card (``{dialect}``).

Hermetic: ScriptedAgent + LocalSandbox + local SCM, no provider.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from support import git

from swfactory.agent import ScriptedAgent
from swfactory.approval_policy import SCRIPTED_REPLAY_FIXTURE
from swfactory.blueprint import load
from swfactory.cli import execute
from swfactory.models import RunReport

ROOT = Path(__file__).resolve().parents[1]
ART = "docs/factory/DEMO-1"
SCRIPTED = ROOT / "demo" / "scripted"
FIXTURES = ROOT / "tests" / "fixtures"


@pytest.fixture(autouse=True)
def _at_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(ROOT)


def _run(tmp_path: Path, fixtures: list[Path]) -> RunReport:
    bp = load("factory")
    (job,) = bp.jobs({"issues": ["demo/issue.md"]})
    cfg = bp.config(
        job,
        run_id="w1nn1ng0",
        gate_replay=str(SCRIPTED_REPLAY_FIXTURE),
        agent="scripted",
        sandbox="local",
        scm="local",
        workdir=str(tmp_path / "work"),
    )
    return execute(cfg, run_dir=tmp_path / "run", agent=ScriptedAgent(fixtures), blueprint=bp)


def _calls(tmp_path: Path) -> list[str]:
    rows = [json.loads(line) for line in (tmp_path / "run" / "state" / "agent_calls.jsonl").read_text().splitlines()]
    return sorted({f"{row['stage']}.{row['iteration']}" for row in rows if "stage" in row and "iteration" in row})


def test_a_ratcheted_build_is_guarded_audited_and_carded_at_the_best_head(tmp_path: Path) -> None:
    """fix.2 is reverted by the ratchet, fix.3 goes green from build.1's tree, review approves."""
    report = _run(tmp_path, [FIXTURES / "ratchet", SCRIPTED])
    by = {stage.stage: stage for stage in report.stages}
    assert by["deliver"].status == "ok" and by["build_and_test"].numbers["ratchet_reverts"] == 1
    work = tmp_path / "work"
    ratchet = json.loads((tmp_path / "run" / "state" / "artifacts" / ART / "build-ratchet.json").read_text())
    best, reverted = ratchet["best"]["head"], ratchet["iterations"][1]["head"]

    # The guard judged fix.3's head -- the one the last test run saw -- by per-test ids.
    regression = json.loads((work / ART / "regression.json").read_text())
    assert regression["tested_head"] == best != reverted
    assert regression["mode"] == "cases" and regression["regressions"] == 0
    assert by["deliver"].numbers["regressions"] == 0 and by["deliver"].numbers["base_tests_passed"] == 3

    # The reverted call is counted and audited, and does not make the run deviant or unverifiable.
    assert "fix.2" in _calls(tmp_path)
    metrics = json.loads((work / ART / "metrics.json").read_text())
    assert metrics["agent_calls"] == 6  # spec, plan, build.1, fix.2 (reverted), fix.3, review.1
    assert metrics["ratchet_reverts"] == 1 and metrics["iterations"] == 3
    adherence = json.loads(git(work, "show", f"HEAD:{ART}/adherence.json"))
    assert adherence["verdict"] == "adherent" and metrics["adherence"] == "adherent"
    assert reverted not in git(work, "log", "--format=%H").split()

    # The fix after the revert sees the ratchet note and the target's dialect card.
    prompt = (work / ".factory" / "prompt.fix.3.md").read_text()
    assert "NOTE: fix.2 was reverted and the workspace is back at build.1" in prompt
    assert "## Target dialect" in prompt
    assert prompt.index("NOTE: fix.2 was reverted") < prompt.index("## Target dialect")


def test_a_reverted_review_fix_is_guarded_and_traced_on_the_restored_head(tmp_path: Path) -> None:
    """blocker_breaks/fix.4 turns the suite red; review reverts it and re-tests. The guard, the trace
    and the audit all read the restored head's evidence, and the run delivers [BLOCKED]."""
    fixtures = [FIXTURES / "blocker_breaks", FIXTURES / "blocker", SCRIPTED]
    report = _run(tmp_path, fixtures)
    by = {stage.stage: stage for stage in report.stages}
    assert by["review"].status == "blocked" and by["deliver"].status == "blocked"
    work = tmp_path / "work"
    (reverted,) = json.loads((work / ART / "review.json").read_text())["reverted_fixes"]
    restored = reverted["restored_head"]

    regression = json.loads((work / ART / "regression.json").read_text())
    assert regression["tested_head"] == restored != reverted["reverted_sha"]
    assert regression["regressions"] == 0 and by["deliver"].numbers["regressions"] == 0
    trace = json.loads((work / ART / "trace.json").read_text())
    assert trace["head"] == restored and trace["summary"]["missing_acceptance_tests"] == 0

    assert "fix.4" in _calls(tmp_path)
    metrics = json.loads((work / ART / "metrics.json").read_text())
    assert metrics["agent_calls"] == 7 and metrics["review_reverted_fixes"] == 1
    assert json.loads(git(work, "show", f"HEAD:{ART}/adherence.json"))["verdict"] == "adherent"

    prompt = (work / ".factory" / "prompt.fix.4.md").read_text()
    assert "Review blockers:" in prompt and "## Target dialect" in prompt
    pr = (tmp_path / "run" / "pr.md").read_text()
    assert pr.startswith("# [BLOCKED] DEMO-1:") and "Review fix 1 reverted: it broke the suite" in pr
