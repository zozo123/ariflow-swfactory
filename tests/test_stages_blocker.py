"""Review fix path, the factory:blocked path and the [REJECTED] gate path, driven by
``tests/fixtures/blocker`` (a blocker whose fix lands) and ``tests/fixtures/blocker_breaks`` (a
blocker whose fix breaks the suite). Review fixes are ``fix.<max_build_iterations + k>``: with the
default of 3 build iterations the first review fix is ``fix.4``."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from support import git

from swfactory.agent import ScriptedAgent
from swfactory.approval_policy import SCRIPTED_REPLAY_FIXTURE
from swfactory.blueprint import load
from swfactory.cli import execute
from swfactory.models import Approval, RunReport, StageError
from swfactory.stages import Ctx, Gate

ROOT = Path(__file__).resolve().parents[1]
ART = "docs/factory/DEMO-1"
FIXTURES = [ROOT / "tests" / "fixtures" / "blocker", ROOT / "demo" / "scripted"]
BREAKS = [ROOT / "tests" / "fixtures" / "blocker_breaks", *FIXTURES]


@pytest.fixture(autouse=True)
def _at_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(ROOT)


def _run(tmp_path: Path, fixtures: list[Path] = FIXTURES, approver=None, **overrides: object) -> tuple[RunReport, Path]:
    bp = load("factory")
    (job,) = bp.jobs({"issues": ["demo/issue.md"]})
    defaults: dict[str, object] = {
        "gate_replay": str(SCRIPTED_REPLAY_FIXTURE),
        "agent": "scripted",
        "sandbox": "local",
        "scm": "local",
        "workdir": str(tmp_path / "work"),
    }
    cfg = bp.config(job, run_id="b10ck3r1", **{**defaults, **overrides})
    kw = {"approver": approver} if approver else {}
    report = execute(cfg, run_dir=tmp_path / "run", agent=ScriptedAgent(fixtures), blueprint=bp, **kw)
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


def _delivered_branch(tmp: Path) -> tuple[Path, str]:
    from swfactory.config import Config
    from swfactory.publication_identity import publication_key

    defaults = Config(issue="demo/issue.md")
    return tmp / "run" / "remote.git", f"factory/DEMO-1-{publication_key(defaults.repo, defaults.target_dir, 'DEMO-1')}"


def test_review_fix_that_breaks_tests_is_reverted_and_still_a_blocker(tmp_path: Path) -> None:
    """blocker_breaks/fix.4.patch reverts the sign fix: the re-review approves (scripted), but the
    red suite is reverted to the green pre-fix head and becomes a synthetic blocker, so the run
    still delivers [BLOCKED] -- now carrying green bytes rather than the broken attempt.

    This pinned the red delivery before (tests_failed == 2, report.tests_passed False, and the
    requirement trace's R2/R4 acceptance tests failing at the red HEAD). The new assertions are
    strictly stronger: the block, the label and the blocker all remain, and on top the delivered
    tree is the one that passed -- its trace, re-derived from the restored HEAD's own JUnit cases,
    has every planned acceptance test passing -- and the reverted commit stays auditable by SHA."""
    report, tmp = _run(tmp_path, fixtures=BREAKS)
    by = {s.stage: s for s in report.stages}
    assert by["review"].status == "blocked" and by["deliver"].status == "blocked"
    assert by["review"].numbers["fixes"] == 1 and by["review"].numbers["blockers"] == 1
    assert by["review"].numbers["reverted_fixes"] == 1
    # The numbers describe the delivered head, re-tested after the revert: green.
    assert by["review"].numbers["tests_passed"] == 1 and by["review"].numbers["tests_failed"] == 0
    assert by["review"].numbers["missing_acceptance_tests"] == 0
    assert report.tests_passed is True
    review = json.loads((tmp / "work" / ART / "review.json").read_text())
    assert review["verdict"] == "request_changes"
    (blocker,) = [f for f in review["findings"] if f["severity"] == "blocker"]
    assert blocker["title"] == "Review fix 1 reverted: it broke the suite" and blocker["file"] == "tests"
    assert "failed=2" in blocker["detail"] and "test_decrease" in blocker["detail"]
    (reverted,) = review["reverted_fixes"]
    assert reverted["agent_call"] == "fix.4" and reverted["failed"] == 2 and reverted["restored_tests_passed"]
    assert reverted["reverted_sha"] in blocker["detail"]
    work = tmp / "work"
    assert git(work, "cat-file", "-t", reverted["reverted_sha"]).strip() == "commit"  # still auditable
    core = (work / "src" / "calc" / "core.py").read_text()
    assert "return (new - old) / old" in core and "A decrease is negative" not in core  # fix.2's tree
    remote, branch = _delivered_branch(tmp)
    assert reverted["reverted_sha"] not in git(remote, "log", "--format=%H", branch).split()
    delivered = git(remote, "show", f"{branch}:src/calc/core.py")
    assert delivered == core.rstrip("\n")  # `git` strips its output
    assert json.loads((work / ART / "metrics.json").read_text())["review_reverted_fixes"] == 1
    trace = json.loads((work / ART / "trace.json").read_text())
    assert trace["head"] == reverted["restored_head"] == git(work, "rev-parse", "HEAD")
    assert trace["evidence"] == "junit" and trace["summary"]["missing_acceptance_tests"] == 0
    pr = (tmp / "run" / "pr.md").read_text()
    assert pr.startswith("# [BLOCKED] DEMO-1:") and "factory:blocked" in pr
    assert "Review fix 1 reverted: it broke the suite" in pr


def test_a_second_review_fix_lands_on_the_restored_tree(tmp_path: Path) -> None:
    """max_review_fixes=2: fix.4 breaks the suite and is reverted; fix.5 is authored against the
    restored (green) tree -- it applies only because the revert really happened -- and goes green,
    so the third review approves and the run delivers an unblocked PR."""
    fixtures = [ROOT / "tests" / "fixtures" / "blocker_breaks_then_fixed", *BREAKS]
    report, tmp = _run(tmp_path, fixtures=fixtures, max_review_fixes=2)
    by = {s.stage: s for s in report.stages}
    assert by["review"].status == "ok" and by["deliver"].status == "ok"
    assert by["review"].numbers["fixes"] == 2 and by["review"].numbers["blockers"] == 0
    assert by["review"].numbers["reverted_fixes"] == 1 and by["review"].numbers["tests_passed"] == 1
    review = json.loads((tmp / "work" / ART / "review.json").read_text())
    assert review["verdict"] == "approve"
    assert [row["agent_call"] for row in review["reverted_fixes"]] == ["fix.4"]
    prompt = (tmp / "work" / ".factory" / "prompt.fix.5.md").read_text()
    assert "Review fix 1 reverted: it broke the suite" in prompt
    core = (tmp / "work" / "src" / "calc" / "core.py").read_text()
    assert "A decrease is negative" in core and "return (new - old) / old" in core
    pr = (tmp / "run" / "pr.md").read_text()
    assert "[BLOCKED]" not in pr and "factory:blocked" not in pr


def test_a_review_fix_that_edits_tests_is_refused_on_a_local_backend(tmp_path: Path) -> None:
    """The fix scope holds in fix_and_retest too: a review fix that edits tests/ is refused as
    policy and the workspace goes back to the pre-fix head, whatever the sandbox enforces."""
    fixtures = [ROOT / "tests" / "fixtures" / "review_fix_weakens_tests", *FIXTURES]
    with pytest.raises(StageError) as raised:
        _run(tmp_path, fixtures=fixtures)
    assert raised.value.kind == "policy" and "protected paths ['tests/test_percent_change.py']" in str(raised.value)
    work = tmp_path / "work"
    # HEAD is back on the build loop's fix.2 commit: the review fix.4 commit is no candidate.
    assert git(work, "log", "-1", "--format=%s") == "fix: applied scripted fixture fix.2.patch"
    assert "assert True" not in (work / "tests" / "test_percent_change.py").read_text()


def _reject_intent(gate: Gate, ctx: Ctx) -> Approval:
    decision = "reject" if gate.name == "intent" else "approve"
    return Approval(gate=gate.name, decision=decision, actor="alice", at=datetime.now(UTC))


def test_rejected_gate_publishes_rejected_pr_with_durable_approval(tmp_path: Path) -> None:
    """A refusal is part of the audit chain: approvals.json + metrics.json are committed and the
    PR is titled [REJECTED] with label factory:rejected; nothing after the gate runs."""
    report, tmp = _run(tmp_path, approve="prompt", approver=_reject_intent)
    assert [s.stage for s in report.stages] == ["intent", "deliver"]
    assert report.stages[-1].status == "blocked" and report.stages[-1].numbers["rejected"] == 1
    assert [(a.gate, a.decision, a.actor) for a in report.approvals] == [("intent", "reject", "alice")]
    assert report.tests_passed is False and report.pr_url
    art = tmp / "work" / ART
    approvals = json.loads((art / "approvals.json").read_text())
    assert [(a["gate"], a["decision"], a["actor"]) for a in approvals] == [("intent", "reject", "alice")]
    assert json.loads((art / "metrics.json").read_text())["approvers"] == ["alice"]
    assert not (art / "spec.md").exists() and not (art / "plan.json").exists()
    pr = (tmp / "run" / "pr.md").read_text()
    assert pr.startswith("# [REJECTED] DEMO-1:")
    assert "labels: factory, agent-authored, factory:rejected" in pr
    assert "| intent | reject | alice |" in pr
    from swfactory.config import Config
    from swfactory.publication_identity import publication_key

    # Derived, not hardcoded to the run id: the publish ref is keyed on the work so two factory
    # instances converge on one branch and one pull request.
    defaults = Config(issue="demo/issue.md")
    remote = tmp / "run" / "remote.git"
    branch = f"factory/DEMO-1-{publication_key(defaults.repo, defaults.target_dir, 'DEMO-1')}"
    files = git(remote, "ls-tree", "-r", "--name-only", branch).split()
    assert f"{ART}/approvals.json" in files and f"{ART}/metrics.json" in files
    assert f"{ART}/intent.md" in files and "tests/test_percent_change.py" not in files
    assert json.loads((tmp / "run" / "report.json").read_text())["stages"][-1]["status"] == ("blocked")
