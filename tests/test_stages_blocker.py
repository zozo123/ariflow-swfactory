"""Review fix path, the factory:blocked path and the [REJECTED] gate path, driven by
``tests/fixtures/blocker`` (a blocker whose fix lands) and ``tests/fixtures/blocker_breaks`` (a
blocker whose fix breaks the suite). Review fixes are ``fix.<max_build_iterations + k>``: with the
default of 3 build iterations the first review fix is ``fix.4``."""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from swfactory.agent import ScriptedAgent
from swfactory.blueprint import load
from swfactory.cli import execute
from swfactory.models import Approval, RunReport
from swfactory.stages import Ctx, Gate

ROOT = Path(__file__).resolve().parents[1]
ART = "docs/factory/DEMO-1"
FIXTURES = [ROOT / "tests" / "fixtures" / "blocker", ROOT / "demo" / "scripted"]
BREAKS = [ROOT / "tests" / "fixtures" / "blocker_breaks", *FIXTURES]


@pytest.fixture(autouse=True)
def _isolated_git(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(tmp_path / "empty-gitconfig"))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.chdir(ROOT)


def _run(
    tmp_path: Path, fixtures: list[Path] = FIXTURES, approver=None, **overrides: object
) -> tuple[RunReport, Path]:
    bp = load("factory")
    (job,) = bp.jobs({"issues": ["demo/issue.md"]})
    defaults: dict[str, object] = {
        "approve": "auto",
        "agent": "scripted",
        "sandbox": "local",
        "scm": "local",
        "workdir": str(tmp_path / "work"),
    }
    cfg = bp.config(job, run_id="b10ck3r1", **{**defaults, **overrides})
    kw = {"approver": approver} if approver else {}
    report = execute(
        cfg, run_dir=tmp_path / "run", agent=ScriptedAgent(fixtures), blueprint=bp, **kw
    )
    return report, tmp_path


def test_blocker_is_fixed_and_re_reviewed(tmp_path: Path) -> None:
    report, tmp = _run(tmp_path)  # max_review_fixes defaults to 1
    by = {s.stage: s for s in report.stages}
    assert by["review"].status == "ok" and by["deliver"].status == "ok"
    assert by["review"].numbers["fixes"] == 1 and by["review"].numbers["blockers"] == 0
    assert by["review"].numbers["tests_passed"] == 1
    assert report.tests_passed is True
    art = tmp / "work" / ART
    review = json.loads((art / "review.json").read_text())
    assert review["verdict"] == "approve" and review["fixes"] == 1
    assert [f["severity"] for f in review["findings"]] == ["nit"]
    envelopes = sorted(p.name for p in (art / "agent").glob("*.json"))
    # build loop: build.1, fix.2; review fix: fix.4 (= 3 build iterations + 1), never fix.1/fix.2
    assert envelopes == [
        "build.1.json",
        "fix.2.json",
        "fix.4.json",
        "plan.1.json",
        "review.1.json",
        "review.2.json",
        "spec.1.json",
    ]
    assert (tmp / "work" / ".factory" / "prompt.fix.4.md").is_file()
    core = (tmp / "work" / "src" / "calc" / "core.py").read_text()
    assert "A decrease is negative" in core  # blocker/fix.4.patch landed
    pr = (tmp / "run" / "pr.md").read_text()
    assert "factory:blocked" not in pr and "[BLOCKED]" not in pr
    assert "| fix | " in pr or "fixes=1" in pr


def test_exhausted_fixes_deliver_blocked(tmp_path: Path) -> None:
    report, tmp = _run(tmp_path, max_review_fixes=0)
    by = {s.stage: s for s in report.stages}
    assert by["review"].status == "blocked" and by["deliver"].status == "blocked"
    assert by["review"].numbers["blockers"] == 1 and by["review"].numbers["fixes"] == 0
    assert report.tests_passed is True  # build passed; the block is a review verdict
    review = json.loads((tmp / "work" / ART / "review.json").read_text())
    assert review["verdict"] == "request_changes"
    pr = (tmp / "run" / "pr.md").read_text()
    assert pr.startswith("# [BLOCKED] DEMO-1:")
    assert "labels: factory, agent-authored, factory:blocked" in pr
    assert "### Blocker (1)" in pr
    assert pr.index("### Blocker") < pr.index("### Nit")


def test_review_fix_that_breaks_tests_is_a_blocker(tmp_path: Path) -> None:
    """blocker_breaks/fix.4.patch reverts the sign fix: the re-review approves (scripted), but the
    red suite becomes a synthetic blocker and the run delivers [BLOCKED] instead of a green PR."""
    report, tmp = _run(tmp_path, fixtures=BREAKS)
    by = {s.stage: s for s in report.stages}
    assert by["review"].status == "blocked" and by["deliver"].status == "blocked"
    assert by["review"].numbers["fixes"] == 1 and by["review"].numbers["blockers"] == 1
    assert by["review"].numbers["tests_passed"] == 0 and by["review"].numbers["tests_failed"] == 2
    assert report.tests_passed is False
    review = json.loads((tmp / "work" / ART / "review.json").read_text())
    assert review["verdict"] == "request_changes"
    (blocker,) = [f for f in review["findings"] if f["severity"] == "blocker"]
    assert blocker["title"] == "Tests failing after review fix" and blocker["file"] == "tests"
    assert "failed=2" in blocker["detail"] and "test_decrease" in blocker["detail"]
    pr = (tmp / "run" / "pr.md").read_text()
    assert pr.startswith("# [BLOCKED] DEMO-1:") and "factory:blocked" in pr
    assert "Tests failing after review fix" in pr


def _reject_intent(gate: Gate, ctx: Ctx) -> Approval:
    decision = "reject" if gate.name == "intent" else "approve"
    return Approval(gate=gate.name, decision=decision, actor="alice", at=datetime.now(UTC))


def test_rejected_gate_publishes_rejected_pr_with_durable_approval(tmp_path: Path) -> None:
    """A refusal is part of the audit chain: approvals.json + metrics.json are committed and the
    PR is titled [REJECTED] with label factory:rejected; nothing after the gate runs."""
    report, tmp = _run(tmp_path, approve="prompt", approver=_reject_intent)
    assert [s.stage for s in report.stages] == ["intent", "deliver"]
    assert report.stages[-1].status == "blocked" and report.stages[-1].numbers["rejected"] == 1
    assert [(a.gate, a.decision, a.actor) for a in report.approvals] == [
        ("intent", "reject", "alice")
    ]
    assert report.tests_passed is False and report.pr_url
    art = tmp / "work" / ART
    approvals = json.loads((art / "approvals.json").read_text())
    assert [(a["gate"], a["decision"], a["actor"]) for a in approvals] == [
        ("intent", "reject", "alice")
    ]
    assert json.loads((art / "metrics.json").read_text())["approvers"] == ["alice"]
    assert not (art / "spec.md").exists() and not (art / "plan.json").exists()
    pr = (tmp / "run" / "pr.md").read_text()
    assert pr.startswith("# [REJECTED] DEMO-1:")
    assert "labels: factory, agent-authored, factory:rejected" in pr
    assert "| intent | reject | alice |" in pr
    remote, branch = tmp / "run" / "remote.git", "factory/DEMO-1-b10ck3r1"
    files = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", branch],
        cwd=remote,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    assert f"{ART}/approvals.json" in files and f"{ART}/metrics.json" in files
    assert f"{ART}/intent.md" in files and "tests/test_percent_change.py" not in files
    assert json.loads((tmp / "run" / "report.json").read_text())["stages"][-1]["status"] == (
        "blocked"
    )
