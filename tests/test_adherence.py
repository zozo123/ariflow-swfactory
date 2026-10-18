"""The post-run adherence audit (``swfactory.adherence``) and the hold/record it reads.

One real scripted run is the adherent baseline. Every deviation is then applied to a COPY of that
run -- its checkout and its host state -- so each test changes exactly one recorded fact and shows
the verdict the audit derives from it. A second scripted run whose fix fixture also writes under
``tests/`` shows the same verdict end to end: LocalSandbox has no kernel ``denyWrite``, so the edit
lands and only the audit can see that fix crossed its boundary.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from support import git

from swfactory import adherence, stages
from swfactory.agent import ScriptedAgent
from swfactory.approval_policy import SCRIPTED_REPLAY_FIXTURE
from swfactory.blueprint import Blueprint, FusionSpec, PolicyOverride, load
from swfactory.call_accounting import CALLS_LOG, CallLedger
from swfactory.cli import execute
from swfactory.config import Config
from swfactory.models import AgentResult, Approval, Issue, RunReport, StageError
from swfactory.sandbox import LocalSandbox
from swfactory.state import OPERATIONS_LOG

ROOT = Path(__file__).resolve().parents[1]
ART = "docs/factory/DEMO-1"
RUN_ID = "adh3r3nt"
EXTRA_TEST = """diff --git a/tests/test_extra.py b/tests/test_extra.py
new file mode 100644
--- /dev/null
+++ b/tests/test_extra.py
@@ -0,0 +1,2 @@
+def test_extra() -> None:
+    assert True
"""


@pytest.fixture(autouse=True)
def _at_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(ROOT)


def _scripted_run(tmp: Path, fixtures: str, *, fusion: bool = False) -> tuple[RunReport, Blueprint, Config]:
    bp = load("factory")
    if fusion:  # exactly what `swfactory demo --fusion` runs
        bp = bp.model_copy(update={"fusion": FusionSpec()})
    (job,) = bp.jobs({"issues": ["demo/issue.md"]})
    cfg = bp.config(
        job,
        run_id=RUN_ID,
        gate_replay=str(SCRIPTED_REPLAY_FIXTURE),
        agent="scripted",
        sandbox="local",
        scm="local",
        workdir=str(tmp / "work"),
        fixtures_dir=fixtures,
    )
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(ROOT)
        report = execute(cfg, run_dir=tmp / "run", blueprint=bp)
    return report, bp, cfg


@pytest.fixture(scope="module")
def run(tmp_path_factory: pytest.TempPathFactory) -> tuple[RunReport, Path, Blueprint, Config]:
    tmp = tmp_path_factory.mktemp("adherent")
    report, bp, cfg = _scripted_run(tmp, "demo/scripted")
    return report, tmp, bp, cfg


def _copy(run: tuple[RunReport, Path, Blueprint, Config], dst: Path) -> stages.Ctx:
    """An independent copy of the delivered run -- checkout and host state -- as a ``Ctx``."""
    _, tmp, bp, cfg = run
    shutil.copytree(tmp / "work", dst / "work", ignore=shutil.ignore_patterns(".venv"), symlinks=True)
    shutil.copytree(tmp / "run", dst / "run", ignore=shutil.ignore_patterns("remote.git"))
    return stages.Ctx(
        cfg=cfg.model_copy(update={"workdir": str(dst / "work")}),
        sb=LocalSandbox(dst / "work"),
        agent=ScriptedAgent([ROOT / "demo" / "scripted"]),
        scm=None,  # type: ignore[arg-type]  # the audit never publishes
        issue=Issue(id="DEMO-1", title="add percent_change", body="body"),
        run_dir=dst / "run",
        blueprint=bp,
    )


def _check(report: dict[str, Any], name: str) -> dict[str, Any]:
    return next(check for check in report["checks"] if check["name"] == name)


def _evidence(report: dict[str, Any], name: str, status: str) -> list[str]:
    return [line for line in _check(report, name)["evidence"] if line.startswith(f"{status}:")]


def _commit(work: Path, files: dict[str, str], trailer: str | None) -> None:
    for name, text in files.items():
        (work / name).parent.mkdir(parents=True, exist_ok=True)
        (work / name).write_text(text, encoding="utf-8")
    git(work, "add", "-A")
    args = ["commit", "-qm", "sneaky"]
    if trailer is not None:
        args += ["--trailer", f"Factory-Stage={trailer}"]
    git(work, *args)


def _call(ctx: stages.Ctx, operation: str, role: str, iteration: int, *, cost: float | None, envelope: dict | None):
    """One more call the way ``_agent`` records it: reserved inside an owned operation, settled with
    a receipt, its envelope in host artifacts."""
    ledger = CallLedger(ctx.state)
    with ctx.state.exclusive(operation):
        attempt = ledger.reserve(stage=role, iteration=iteration, reserved_usd=0.5)
        if cost is not None:
            receipt = {"agent": (envelope or {}).get("agent", "claude"), "worktree_unchanged": True}
            ledger.settle(attempt, cost_usd=cost, receipt=receipt)
    if envelope is not None:
        ctx.state.write_artifact(f"{ctx.art}/agent/{role}.{iteration}.json", json.dumps(envelope))
    return attempt


# ---------------------------------------------------------------- adherent baseline


def test_the_scripted_demo_is_adherent_and_the_verdict_is_committed(run) -> None:
    report, tmp, _, _ = run
    work = tmp / "work"
    committed = json.loads(git(work, "show", f"HEAD:{ART}/adherence.json"))
    assert committed["schema_version"] == 1 and committed["verdict"] == "adherent"
    assert [check["name"] for check in committed["checks"]] == list(adherence.CHECKS)
    statuses = {check["name"]: check["status"] for check in committed["checks"]}
    assert statuses == {
        "participation": "pass",
        "delegation": "pass",
        "role_boundary": "pass",
        "tool_boundary": "n/a",  # scripted envelopes record no tool use
        "budget": "pass",
        "information_flow": "pass",
    }
    assert committed["per_role"] == {
        role: {"calls": 1, "cost_usd": 0.0, "reserved_usd": 2.0, "unreconciled_usd": 0.0}
        for role in ("build", "fix", "plan", "review", "spec")
    }
    # The verdict reaches every receipt of the run: metrics, the deliver record and the PR body.
    assert json.loads((work / ART / "metrics.json").read_text())["adherence"] == "adherent"
    assert report.stages[-1].numbers["adherence"] == adherence.VERDICT_SCORE["adherent"]
    assert "adherence audit: `adherent`" in (tmp / "run" / "pr.md").read_text()


def test_every_settle_receipt_records_the_prompt_digest_and_read_only_calls_the_worktree_hold(run) -> None:
    _, tmp, _, _ = run
    ctx = _copy(run, tmp / "receipts")
    for record in CallLedger(ctx.state).records():
        receipt = record.receipt or {}
        assert len(receipt["prompt_sha256"]) == 64 and receipt["prompt_bytes"] > 0, record.call_id
        if record.stage in {"spec", "plan", "review"}:
            assert receipt["worktree_unchanged"] is True, record.call_id
        else:
            assert "worktree_unchanged" not in receipt, record.call_id


def test_a_re_audit_after_delivery_holds_the_deliver_commit_to_the_artifact_dir(run, tmp_path: Path) -> None:
    """A retried deliver re-audits a stream that now holds the first deliver commit; it is a
    role-bound commit like any other, so the verdict and every check status are unchanged."""
    ctx = _copy(run, tmp_path)
    first = json.loads((tmp_path / "work" / ART / "adherence.json").read_text())
    again = adherence.audit(ctx)
    assert [(c["name"], c["status"]) for c in again["checks"]] == [(c["name"], c["status"]) for c in first["checks"]]
    assert (
        "pass: 3 commit(s) base..HEAD stayed inside their role's boundary" in _check(again, "role_boundary")["evidence"]
    )


def test_liquid_review_lanes_and_fork_wave_nodes_are_modelled_not_misread_as_deviant(run, tmp_path: Path) -> None:
    """The lines whose naming differs from the plain demo: liquid annealing runs its specialist
    lanes as ``review.<round*10+lane>`` inside the review operation, and Plan.work / fork waves run
    numbered build calls inside build_and_test and land ``work:<node>`` commits. None of that is a
    deviation, and none of it may degrade the verdict to unverifiable either."""
    ctx = _copy(run, tmp_path)
    for lane in (11, 12, 13):
        _call(ctx, "review", "review", lane, cost=0.0, envelope={"agent": "scripted"})
    for call in (2, 3):
        _call(ctx, "build_and_test", "build", call, cost=0.0, envelope={"agent": "scripted"})
    _commit(tmp_path / "work", {"src/calc/node.py": "N = 1\n"}, "work:n2")
    report = adherence.audit(ctx)
    assert report["verdict"] == "adherent", report
    assert report["per_role"]["review"]["calls"] == 4 and report["per_role"]["build"]["calls"] == 3


def test_the_fusion_demo_audits_its_campaign_reservation_and_stays_adherent(tmp_path: Path) -> None:
    """Replica-fusion children keep their own ledgers; the parent sees one ``replica_fusion``
    campaign reservation and a ``fusion`` adoption commit. Both are modelled: participation,
    delegation and budget read the reservation, role_boundary holds the commit to build."""
    report, _, _ = _scripted_run(tmp_path, "demo/fusion", fusion=True)
    assert report.stages[-1].status == "ok"
    committed = json.loads(git(tmp_path / "work", "show", f"HEAD:{ART}/adherence.json"))
    assert committed["verdict"] == "adherent", committed
    assert committed["per_role"]["replica_fusion"]["calls"] == 1
    assert any("replica-fusion roles run in child ledgers" in note for note in committed["line"]["notes"])
    assert any(line.startswith("pass: fusion commit") for line in _check(committed, "role_boundary")["evidence"])


# ---------------------------------------------------------------- deviant


def test_a_fix_commit_that_touches_tests_is_deviant_on_role_boundary(run, tmp_path: Path) -> None:
    ctx = _copy(run, tmp_path)
    _commit(tmp_path / "work", {"tests/test_core.py": "def test_weakened() -> None:\n    pass\n"}, "fix")
    report = adherence.audit(ctx)
    assert report["verdict"] == "deviant"
    assert _check(report, "role_boundary")["status"] == "fail"
    (finding,) = _evidence(report, "role_boundary", "fail")
    assert "(fix) touches paths protected for fix" in finding and "tests/test_core.py" in finding


def test_a_build_commit_may_write_tests_but_not_the_guarded_set(run, tmp_path: Path) -> None:
    ctx = _copy(run, tmp_path)
    _commit(tmp_path / "work", {"tests/test_more.py": "def test_more() -> None:\n    pass\n"}, "work:n1")
    assert adherence.audit(ctx)["verdict"] == "adherent"  # build owns tests
    _commit(tmp_path / "work", {"REVIEW.md": "anything goes\n"}, "build")
    report = adherence.audit(ctx)
    assert report["verdict"] == "deviant"
    assert any("guarded paths: ['REVIEW.md']" in line for line in _evidence(report, "role_boundary", "fail"))


@pytest.mark.parametrize("trailer", ["review", "spec", None])
def test_a_commit_attributed_to_a_read_only_role_or_to_nobody_is_deviant(run, tmp_path: Path, trailer) -> None:
    ctx = _copy(run, tmp_path)
    _commit(tmp_path / "work", {"src/calc/extra.py": "X = 1\n"}, trailer)
    report = adherence.audit(ctx)
    assert report["verdict"] == "deviant"
    (finding,) = _evidence(report, "role_boundary", "fail")
    assert ("read-only role" in finding) if trailer else ("0 Factory-Stage trailers" in finding)


def test_a_deliver_commit_outside_the_artifact_dir_is_deviant(run, tmp_path: Path) -> None:
    ctx = _copy(run, tmp_path)
    _commit(tmp_path / "work", {"src/calc/extra.py": "X = 1\n"}, "deliver")
    report = adherence.audit(ctx)
    assert report["verdict"] == "deviant"
    assert any(f"outside {ART}/" in line for line in _evidence(report, "role_boundary", "fail"))


def test_a_call_settled_above_its_reservation_is_deviant_on_budget(run, tmp_path: Path) -> None:
    ctx = _copy(run, tmp_path)
    _call(ctx, "review", "fix", 9, cost=0.75, envelope={"agent": "scripted"})
    report = adherence.audit(ctx)
    assert report["verdict"] == "deviant"
    assert _evidence(report, "budget", "fail") == ["fail: fix.9: settled 0.7500 USD over its 0.5000 grant"]
    assert report["per_role"]["fix"] == {"calls": 2, "cost_usd": 0.75, "reserved_usd": 2.5, "unreconciled_usd": 0.0}


def test_a_run_charged_past_its_ceiling_is_deviant_on_budget(run, tmp_path: Path) -> None:
    ctx = _copy(run, tmp_path)
    ctx.cfg = ctx.cfg.model_copy(update={"max_budget_usd": 0.25})
    _call(ctx, "review", "review", 9, cost=0.4, envelope={"agent": "scripted"})
    report = adherence.audit(ctx)
    assert any("against a 0.2500 USD ceiling" in line for line in _evidence(report, "budget", "fail"))


def test_permission_denials_in_a_claude_envelope_are_deviant_and_attributed(run, tmp_path: Path) -> None:
    ctx = _copy(run, tmp_path)
    denial = {"tool_name": "Edit", "tool_use_id": "t1", "tool_input": {"file_path": "tests/test_core.py"}}
    _call(ctx, "review", "fix", 9, cost=0.1, envelope={"permission_denials": [denial], "num_turns": 3})
    report = adherence.audit(ctx)
    assert report["verdict"] == "deviant"
    assert _evidence(report, "tool_boundary", "fail") == [
        "fail: fix.9: 1 tool call(s) refused by permission rules (['Edit'])"
    ]


def test_turns_past_the_cap_and_a_foreign_model_are_deviant(run, tmp_path: Path) -> None:
    ctx = _copy(run, tmp_path)
    ctx.blueprint = ctx.blueprint.model_copy(update={"policy": {"fix": PolicyOverride(model="claude-opus-5")}})
    usage = {"claude-haiku-4": {"canonicalModel": "claude-haiku-4", "costUSD": 0.1}}
    envelope = {"permission_denials": [], "num_turns": ctx.cfg.max_turns + 5, "modelUsage": usage}
    _call(ctx, "review", "fix", 9, cost=0.1, envelope=envelope)
    findings = _evidence(adherence.audit(ctx), "tool_boundary", "fail")
    assert any("turns past the cap" in line for line in findings)
    assert any("without the declared model 'claude-opus-5'" in line for line in findings)


def test_a_guard_hook_denial_is_deviant_and_names_its_call(run, tmp_path: Path) -> None:
    ctx = _copy(run, tmp_path)
    line = {"ts": "t", "tool": "Edit", "path_or_cmd": "tests/test_core.py", "decision": "deny", "call": "fix.2"}
    (tmp_path / "work" / ".factory" / "hooks.jsonl").write_text(json.dumps(line) + "\n")
    report = adherence.audit(ctx)
    assert report["verdict"] == "deviant"
    assert _evidence(report, "tool_boundary", "fail") == ["fail: guard hook refused 1 tool call(s) of fix.2"]


def test_a_tampered_approval_digest_is_deviant_on_information_flow(run, tmp_path: Path) -> None:
    ctx = _copy(run, tmp_path)
    path = f"{ART}/approvals.json"
    rows = json.loads(ctx.state.read_artifact(path))
    rows[1]["artifact_sha256"] = hashlib.sha256(b"a different plan").hexdigest()
    ctx.state.write_artifact(path, json.dumps(rows))
    report = adherence.audit(ctx)
    assert report["verdict"] == "deviant"
    assert _evidence(report, "information_flow", "fail") == [
        f"fail: gate plan: decision digest does not match {ART}/plan.md"
    ]


def test_a_call_whose_stage_started_before_its_gate_was_recorded_is_deviant(run, tmp_path: Path) -> None:
    """Order comes from the operations journal, not from clocks on two hosts: a plan call that ran in
    a plan operation journalled before ``record_approval:intent`` completed preceded the gate."""
    ctx = _copy(run, tmp_path)
    early = [
        {"attempt_id": "e" * 32, "operation": "plan", "pid": 1, "host": "h", "event": "started"},
        {"attempt_id": "e" * 32, "operation": "plan", "pid": 1, "host": "h", "event": "completed"},
    ]
    early[0]["started_at"] = early[1]["started_at"] = "2000-01-01T00:00:00.000+00:00"
    early[1]["finished_at"] = "2000-01-01T00:00:01.000+00:00"
    journal = ctx.state.root / OPERATIONS_LOG
    journal.write_text("".join(json.dumps(r) + "\n" for r in early) + journal.read_text())
    at = "2000-01-01T00:00:00.500+00:00"
    base = {"call_id": "plan.2:early", "stage": "plan", "iteration": 2, "at": at, "reserved_usd": 0.5}
    ctx.state.append_json(CALLS_LOG, {**base, "event": "reserved", "state": "intent", "cost_usd": None})
    receipt = {"agent": "scripted", "worktree_unchanged": True}
    ctx.state.append_json(
        CALLS_LOG, {**base, "event": "settled", "state": "committed", "cost_usd": 0.0, "receipt": receipt}
    )
    ctx.state.write_artifact(f"{ART}/agent/plan.2.json", json.dumps({"agent": "scripted"}))
    report = adherence.audit(ctx)
    assert report["verdict"] == "deviant"
    assert _evidence(report, "information_flow", "fail") == [
        "fail: plan.2 ran in 'plan', started before gate intent was recorded"
    ]
    assert _check(report, "delegation")["status"] == "pass"  # the right stage, at the wrong time


def test_gate_recordings_journalled_before_gates_were_named_still_count(run, tmp_path: Path) -> None:
    """Runs journalled ``record_approval`` without the gate name: each anonymous recording covers
    one gate, so the full run still holds, and one recording short of a stage's gates does not."""
    ctx = _copy(run, tmp_path)
    journal = ctx.state.root / OPERATIONS_LOG
    named = journal.read_text()
    anonymous = named.replace('"record_approval:intent"', '"record_approval"')
    journal.write_text(anonymous.replace('"record_approval:plan"', '"record_approval"'))
    assert adherence.audit(ctx)["verdict"] == "adherent"
    # Drop the plan recording entirely: build, fix and review now ran with one gate unaccounted for.
    rows = [json.loads(line) for line in named.splitlines()]
    journal.write_text("".join(json.dumps(r) + "\n" for r in rows if r["operation"] != "record_approval:plan"))
    findings = _evidence(adherence.audit(ctx), "information_flow", "fail")
    assert "fail: build.1 ran in 'build_and_test', started before gate plan was recorded" in findings
    assert not any(line.startswith(("fail: spec.1", "fail: plan.1")) for line in findings)


def test_a_call_made_by_a_stage_that_may_not_invoke_its_role_is_deviant(run, tmp_path: Path) -> None:
    ctx = _copy(run, tmp_path)
    _call(ctx, "plan", "build", 9, cost=0.0, envelope={"agent": "scripted"})
    report = adherence.audit(ctx)
    assert report["verdict"] == "deviant"
    assert _evidence(report, "delegation", "fail") == [
        "fail: build.9 ran inside operation 'plan', which may not invoke 'build'"
    ]


def test_a_read_only_receipt_that_saw_the_worktree_change_is_deviant(run, tmp_path: Path) -> None:
    ctx = _copy(run, tmp_path)
    ledger = CallLedger(ctx.state)
    with ctx.state.exclusive("review"):
        attempt = ledger.reserve(stage="review", iteration=9, reserved_usd=0.5)
        ledger.settle(attempt, cost_usd=0.0, receipt={"agent": "scripted", "worktree_unchanged": False})
    ctx.state.write_artifact(f"{ART}/agent/review.9.json", json.dumps({"agent": "scripted"}))
    report = adherence.audit(ctx)
    assert _evidence(report, "role_boundary", "fail") == ["fail: read-only call review.9 changed the worktree"]


# ---------------------------------------------------------------- unverifiable


def test_an_unreconciled_call_is_unverifiable_not_adherent(run, tmp_path: Path) -> None:
    ctx = _copy(run, tmp_path)
    _call(ctx, "review", "review", 9, cost=None, envelope=None)
    report = adherence.audit(ctx)
    assert report["verdict"] == "unverifiable"
    assert _evidence(report, "budget", "unknown") == ["unknown: review.9: 0.5000 USD reserved, usage never reconciled"]
    assert report["per_role"]["review"]["unreconciled_usd"] == 0.5


def test_a_settled_call_without_its_envelope_is_unverifiable(run, tmp_path: Path) -> None:
    ctx = _copy(run, tmp_path)
    (ctx.state.artifacts / ART / "agent" / "build.1.json").unlink()
    report = adherence.audit(ctx)
    assert report["verdict"] == "unverifiable"
    assert _evidence(report, "participation", "unknown") == [
        f"unknown: settled call build.1 left no envelope at {ART}/agent/build.1.json"
    ]


def test_a_run_that_predates_the_ledger_is_unverifiable(run, tmp_path: Path) -> None:
    ctx = _copy(run, tmp_path)
    (ctx.state.root / CALLS_LOG).unlink()
    report = adherence.audit(ctx)
    assert report["verdict"] == "unverifiable"
    assert _check(report, "budget")["status"] == "unknown"
    assert report["per_role"] == {}


def test_a_legacy_read_only_receipt_without_the_fingerprint_is_unverifiable(run, tmp_path: Path) -> None:
    ctx = _copy(run, tmp_path)
    ledger = CallLedger(ctx.state)
    with ctx.state.exclusive("review"):
        attempt = ledger.reserve(stage="review", iteration=9, reserved_usd=0.5)
        ledger.settle(attempt, cost_usd=0.0, receipt={"agent": "scripted"})
    ctx.state.write_artifact(f"{ART}/agent/review.9.json", json.dumps({"agent": "scripted"}))
    report = adherence.audit(ctx)
    assert report["verdict"] == "unverifiable"
    assert _evidence(report, "role_boundary", "unknown") == [
        "unknown: read-only call review.9 has no worktree fingerprint in its receipt"
    ]


def test_unreadable_records_never_raise_out_of_the_audit(run, tmp_path: Path) -> None:
    ctx = _copy(run, tmp_path)
    (ctx.state.root / OPERATIONS_LOG).write_text('{"event": 1}\n')
    report = adherence.audit(ctx)
    assert report["verdict"] == "unverifiable"
    assert _check(report, "delegation")["status"] == "unknown"


def test_the_verdict_rule() -> None:
    def checks(*statuses: str) -> list[dict[str, Any]]:
        return [{"name": str(i), "status": s, "evidence": []} for i, s in enumerate(statuses)]

    assert adherence.verdict_of(checks("pass", "n/a")) == "adherent"
    assert adherence.verdict_of(checks("pass", "unknown", "n/a")) == "unverifiable"
    assert adherence.verdict_of(checks("unknown", "fail", "pass")) == "deviant"
    assert adherence.verdict_of(checks("n/a")) == "adherent"


# ---------------------------------------------------------------- end to end


def test_a_re_walk_that_re_records_both_gates_stays_adherent(run, tmp_path: Path) -> None:
    """A second walk of the same run re-answers every gate, replacing each ``approvals.json`` entry
    -- and its ``at`` -- with one given AFTER every call of the first walk. Gate order is read from
    the operations journal, which kept the first recordings, so the re-walk is not a false
    "call before approval"."""
    _, tmp, bp, cfg = run
    ctx = _copy(run, tmp_path)
    git(tmp_path, "clone", "--quiet", "--mirror", str(tmp / "run" / "remote.git"), str(tmp_path / "run" / "remote.git"))
    first = {a["gate"]: a["at"] for a in json.loads(ctx.state.read_artifact(f"{ART}/approvals.json"))}
    again = execute(
        cfg.model_copy(update={"workdir": str(tmp_path / "work"), "fixtures_dir": str(tmp_path / "none")}),
        run_dir=tmp_path / "run",
        blueprint=bp,
    )
    later = {a["gate"]: a["at"] for a in json.loads(ctx.state.read_artifact(f"{ART}/approvals.json"))}
    assert all(later[gate] > first[gate] for gate in first)  # the re-walk really re-recorded them
    assert again.stages[-1].numbers["adherence"] == adherence.VERDICT_SCORE["adherent"]
    committed = json.loads(git(tmp_path / "work", "show", f"HEAD:{ART}/adherence.json"))
    assert committed["verdict"] == "adherent"


def test_a_run_rejected_at_its_first_gate_is_audited_only_up_to_the_refusal(tmp_path: Path) -> None:
    """The stages after a rejected gate never ran and are not owed: no call is missing, no budget
    is unknown, and the refusal itself is checked against the artifact it refused."""

    def reject_intent(gate: stages.Gate, ctx: stages.Ctx) -> Approval:
        decision = "reject" if gate.name == "intent" else "approve"
        return Approval(gate=gate.name, decision=decision, actor="alice", at=datetime.now(UTC))

    bp = load("factory")
    (job,) = bp.jobs({"issues": ["demo/issue.md"]})
    cfg = bp.config(
        job,
        run_id="r3j3ct0a",
        approve="prompt",
        agent="scripted",
        sandbox="local",
        scm="local",
        workdir=str(tmp_path / "work"),
        fixtures_dir=str(tmp_path / "no-fixtures"),
    )
    report = execute(cfg, run_dir=tmp_path / "run", blueprint=bp, approver=reject_intent)
    assert [(s.stage, s.status) for s in report.stages] == [("intent", "ok"), ("deliver", "blocked")]
    committed = json.loads(git(tmp_path / "work", "show", f"HEAD:{ART}/adherence.json"))
    assert committed["verdict"] == "adherent" and committed["per_role"] == {}
    assert committed["line"]["required"] == ["intent"]
    assert (
        "pass: gate intent: decision bound to intent.md as published"
        in _check(committed, "information_flow")["evidence"]
    )


def test_a_fix_fixture_that_writes_tests_publishes_a_deviant_verdict(tmp_path: Path) -> None:
    """Scripted + local has no kernel hold on ``tests/`` during fix, so the edit lands, the suite
    stays green and the run publishes -- record-only -- with the deviation in its committed chain."""
    fixtures = tmp_path / "fixtures"
    shutil.copytree(ROOT / "demo" / "scripted", fixtures)
    patch = fixtures / "fix.2.patch"
    patch.write_text(patch.read_text() + EXTRA_TEST)
    report, _, _ = _scripted_run(tmp_path, str(fixtures))
    assert report.stages[-1].status == "ok"  # published: the audit does not gate
    assert report.stages[-1].numbers["adherence"] == adherence.VERDICT_SCORE["deviant"]
    committed = json.loads(git(tmp_path / "work", "show", f"HEAD:{ART}/adherence.json"))
    assert committed["verdict"] == "deviant"
    (finding,) = _evidence(committed, "role_boundary", "fail")
    assert "(fix) touches paths protected for fix: ['tests/test_extra.py']" in finding
    assert json.loads((tmp_path / "work" / ART / "metrics.json").read_text())["adherence"] == "deviant"


# ---------------------------------------------------------------- the read-only hold


class _Writer:
    """A stub agent that edits the checkout during its call (or not)."""

    kind = "scripted"

    def __init__(self, root: Path, files: dict[str, str]) -> None:
        self.root = root
        self.files = files

    def run(self, sb: Any, **kw: Any) -> AgentResult:
        for name, text in self.files.items():
            (self.root / name).parent.mkdir(parents=True, exist_ok=True)
            with (self.root / name).open("a", encoding="utf-8") as handle:
                handle.write(text)
        return AgentResult(agent="scripted", text="# doc\n", data={"summary": "ok", "files_changed": []})


def _hold_ctx(tmp_path: Path, files: dict[str, str]) -> stages.Ctx:
    work = tmp_path / "work"
    work.mkdir()
    (work / "factory.toml").write_text('[commands]\ntest = "true"\n[paths]\nprotected = ["factory.toml"]\n')
    (work / "src").mkdir()
    (work / "src" / "a.py").write_text("A = 1\n")
    (work / "uv.lock").write_text("lock\n")
    git(work, "init", "-q", "-b", "main")
    (work / ".git" / "info" / "exclude").write_text(".factory/\n")
    git(work, "add", "-A")
    git(work, "commit", "-qm", "base")
    return stages.Ctx(
        cfg=Config(issue="x", run_id="h0ld0001", workdir=str(work)),
        sb=LocalSandbox(work),
        agent=_Writer(work, files),  # type: ignore[arg-type]
        scm=None,  # type: ignore[arg-type]
        issue=Issue(id="X-1", title="t", body="b"),
        run_dir=tmp_path / "run",
    )


def test_a_read_only_call_that_writes_a_file_is_refused_after_its_cost_is_settled(tmp_path: Path) -> None:
    ctx = _hold_ctx(tmp_path, {"src/extra.py": "B = 2\n"})
    with pytest.raises(StageError) as raised:
        stages._agent(ctx, "spec", 1, "the prompt", None)
    assert raised.value.kind == "policy"
    assert str(raised.value) == "[policy] spec.1 (read-only) changed the worktree"
    (record,) = CallLedger(ctx.state).records()
    assert record.settled_usd == 0.0 and record.receipt["worktree_unchanged"] is False


def test_a_read_only_call_that_edits_an_already_dirty_file_is_refused(tmp_path: Path) -> None:
    """The fingerprint covers content, not just porcelain state: setup may leave uv.lock modified,
    and a second edit to it changes no status letter."""
    ctx = _hold_ctx(tmp_path, {"uv.lock": "agent\n"})
    (tmp_path / "work" / "uv.lock").write_text("rewritten by uv sync\n")
    with pytest.raises(StageError, match=r"review\.1 \(read-only\) changed the worktree"):
        stages._agent(ctx, "review", 1, "p", None)


def test_a_read_only_call_that_edits_an_already_untracked_file_is_refused(tmp_path: Path) -> None:
    """Porcelain status names an untracked file once (``??``) however often it changes, and ``git
    diff`` never shows it: only the content hash of untracked files sees the second edit."""
    ctx = _hold_ctx(tmp_path, {"notes.txt": "agent\n"})
    (tmp_path / "work" / "notes.txt").write_text("left by setup\n")
    with pytest.raises(StageError, match=r"plan\.1 \(read-only\) changed the worktree"):
        stages._agent(ctx, "plan", 1, "p", None)


def test_a_read_only_call_that_changes_nothing_passes_and_a_dirty_tree_it_found_is_tolerated(
    tmp_path: Path,
) -> None:
    ctx = _hold_ctx(tmp_path, {f"{Config.artifacts_dir('X-1')}/agent/plan.1.json": "{}"})
    (tmp_path / "work" / "uv.lock").write_text("rewritten by uv sync\n")
    stages._agent(ctx, "plan", 1, "the prompt", None)
    (record,) = CallLedger(ctx.state).records()
    assert record.receipt["worktree_unchanged"] is True
    assert record.receipt["prompt_sha256"] == hashlib.sha256(b"the prompt").hexdigest()
    assert record.receipt["prompt_bytes"] == len(b"the prompt")


def test_write_roles_are_not_fingerprinted(tmp_path: Path) -> None:
    ctx = _hold_ctx(tmp_path, {"src/a.py": "B = 2\n"})
    stages._agent(ctx, "build", 1, "p", None)
    (record,) = CallLedger(ctx.state).records()
    assert "worktree_unchanged" not in record.receipt


def test_the_reservation_time_is_what_delegation_reads(run, tmp_path: Path) -> None:
    """Guard on the record the audit relies on: every call row carries an ISO reservation time."""
    ctx = _copy(run, tmp_path)
    rows = [row for row in ctx.state.read_jsonl(CALLS_LOG) if row.get("event") == "reserved"]
    assert rows and all(datetime.fromisoformat(row["at"]).tzinfo == UTC for row in rows)
