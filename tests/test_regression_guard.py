"""Guarded delivery (VOLO step 4): the base's suite is replayed once by ``setup`` and ``deliver``
holds the candidate to it, and a delivered diff that touches a protected path never reaches a
remote. Both are model-free. Scripted end to end (ScriptedAgent + LocalSandbox + LocalGitScm) from
``tests/fixtures/regression_guard/<case>/build.1.patch``: each is the demo's build with the sign
already right (so build passes on iteration 1), plus the one change under test."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from support import git

from swfactory import stages
from swfactory.agent import ScriptedAgent
from swfactory.approval_policy import SCRIPTED_REPLAY_FIXTURE
from swfactory.blueprint import load
from swfactory.cli import execute
from swfactory.models import RunReport, StageError, StageResult
from swfactory.models import TestResult as SuiteResult  # not collected as a test class
from swfactory.runtime import ctx_for
from swfactory.stages import BASE_TESTS, JUNIT_CASES, regression_findings

ROOT = Path(__file__).resolve().parents[1]
ART = "docs/factory/DEMO-1"
GUARD = ROOT / "tests" / "fixtures" / "regression_guard"
DEMO = ROOT / "demo" / "scripted"
BASE_PASSING = [
    "pytest/tests.test_core::test_compound_annual",
    "pytest/tests.test_core::test_negative_rejected",
    "pytest/tests.test_core::test_simple_interest",
]


@pytest.fixture(autouse=True)
def _at_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(ROOT)


def _config(tmp_path: Path):
    bp = load("factory")
    (job,) = bp.jobs({"issues": ["demo/issue.md"]})
    cfg = bp.config(
        job,
        run_id="gu4rd001",
        gate_replay=str(SCRIPTED_REPLAY_FIXTURE),
        agent="scripted",
        sandbox="local",
        scm="local",
        workdir=str(tmp_path / "work"),
    )
    return bp, cfg


def _run(tmp_path: Path, case: str | None = None) -> RunReport:
    bp, cfg = _config(tmp_path)
    fixtures = [GUARD / case, DEMO] if case else [DEMO]
    return execute(cfg, run_dir=tmp_path / "run", agent=ScriptedAgent(fixtures), blueprint=bp)


def _ctx(tmp_path: Path) -> stages.Ctx:
    bp, cfg = _config(tmp_path)
    return ctx_for(cfg, blueprint=bp, run_dir=tmp_path / "run", agent=ScriptedAgent([DEMO]))


def _factory_branches(tmp_path: Path) -> list[str]:
    """The publication refs in the LocalGitScm bare remote (it also holds the base branch)."""
    remote = tmp_path / "run" / "remote.git"
    if not remote.exists():
        return []
    return git(remote, "for-each-ref", "--format=%(refname:short)", "refs/heads/factory/").split()


def _regression(tmp_path: Path) -> dict:
    return json.loads((tmp_path / "work" / ART / "regression.json").read_text())


# ---------------------------------------------------------------- the pure rule


def _result(passed: int, *, valid: bool = True) -> SuiteResult:
    return SuiteResult(passed=passed, exit_code=0 if valid else 1, report_valid=valid)


BASE = {"base": "b" * 40, "report_valid": True, "passed": 3, "passing": ["s::a", "s::b", "s::c"]}
NO_CANDIDATE_IDS = "Regression guard: the delivered head's report carries no unique per-test ids"


@pytest.mark.parametrize(
    ("base", "cases", "result", "expected"),
    [
        pytest.param(BASE, {"s::a": "passed", "s::b": "passed", "s::c": "passed"}, _result(3), [], id="all-kept"),
        pytest.param(
            BASE,
            {"s::a": "passed", "s::b": "passed", "s::c": "passed", "s::new": "failed", "s::d": "passed"},
            _result(4),
            [],
            id="new-failing-test-is-not-a-regression",
        ),
        pytest.param(
            {**BASE, "passing": ["s::a"], "passed": 1},
            {"s::a": "passed", "s::b": "failed", "s::c": "error"},
            _result(1),
            [],
            id="already-failing-at-base-is-not-a-regression",
        ),
        pytest.param(
            BASE,
            {"s::a": "passed", "s::b": "passed", "s::trivial": "passed", "s::trivial2": "passed"},
            _result(4),
            ["Regression guard: s::c is missing"],
            id="deleted-test-despite-more-passing",
        ),
        pytest.param(
            BASE,
            {"s::a": "passed", "s::b": "passed", "s::c_renamed": "passed"},
            _result(3),
            ["Regression guard: s::c is missing"],
            id="renamed-test",
        ),
        pytest.param(
            BASE,
            {"s::a": "skipped", "s::b": "failed", "s::c": "error"},
            _result(0),
            [
                "Regression guard: s::a is now skipped",
                "Regression guard: s::b now fails",
                "Regression guard: s::c now errors",
            ],
            id="skipped-failed-errored",
        ),
        pytest.param(
            {**BASE, "passing": None},
            None,
            _result(2),
            ["Regression guard: 1 fewer passing test(s) than the base"],
            id="fallback-both-summary-only-fewer",
        ),
        pytest.param({**BASE, "passing": None}, None, _result(3), [], id="fallback-both-summary-only-equal"),
        # Stronger than the count fallback this used to take: a base with ids cannot be confirmed
        # against a candidate without them, however many tests the candidate passes.
        pytest.param(
            BASE,
            None,
            _result(2),
            [NO_CANDIDATE_IDS],
            id="candidate-without-ids-fewer",
        ),
        pytest.param(
            BASE,
            None,
            _result(5),
            [NO_CANDIDATE_IDS],
            id="candidate-without-ids-cannot-hide-a-deleted-test-behind-more-passing",
        ),
        pytest.param({**BASE, "passing": [], "passed": 0}, None, _result(1), [], id="base-passed-nothing-by-id"),
        pytest.param(
            {**BASE, "passing": None},
            {"s::x": "passed", "s::y": "passed", "s::z": "passed", "s::w": "passed"},
            _result(4),
            [],
            id="fallback-base-has-no-ids-more-passing",
        ),
        pytest.param(
            BASE,
            None,
            _result(3, valid=False),
            [NO_CANDIDATE_IDS],
            id="invalid-candidate-report-confirms-nothing",
        ),
        pytest.param(
            {**BASE, "passing": None},
            None,
            _result(3, valid=False),
            ["Regression guard: 3 fewer passing test(s) than the base"],
            id="fallback-invalid-candidate-report-passed-nothing",
        ),
        pytest.param(
            {"base": "b" * 40, "report_valid": False, "passed": 0, "passing": None},
            None,
            _result(0, valid=False),
            [],
            id="new-project-without-tests-has-nothing-to-regress",
        ),
        pytest.param({**BASE, "passing": [], "passed": 0}, {"s::a": "failed"}, _result(0), [], id="empty-base-set"),
    ],
)
def test_regression_findings_table(base, cases, result, expected) -> None:
    findings = regression_findings(base, cases, result, tests_dir="tests")
    assert [f.title for f in findings] == expected
    assert all(f.severity == "blocker" and f.file == "tests" for f in findings)


def test_regression_findings_name_the_base_revision() -> None:
    (finding,) = regression_findings(BASE, {"s::a": "passed", "s::b": "passed"}, _result(2))
    assert "`s::c` passed on the base revision bbbbbbbbbbbb" in finding.detail


def test_bounded_findings_keep_the_pr_body_small_and_count_the_rest() -> None:
    many = regression_findings({**BASE, "passing": [f"s::t{i:03}" for i in range(120)], "passed": 120}, {}, _result(0))
    assert len(many) == 120
    shown = stages._bounded_findings(many)
    assert len(shown) == stages.MAX_REGRESSION_FINDINGS + 1
    assert shown[-1].title == "Regression guard: 70 more base-passing test(s) regressed"
    assert stages._bounded_findings(many[:3]) == many[:3]


# ---------------------------------------------------------------- scripted end to end


def test_plain_demo_records_the_base_and_delivers_ok(tmp_path: Path) -> None:
    """The base suite ran once in setup, the candidate kept all three base tests, and the build's
    new tests file (a change under tests/) is allowed: tests stay writable during build."""
    report = _run(tmp_path)
    by = {s.stage: s for s in report.stages}
    assert by["deliver"].status == "ok"
    assert by["deliver"].numbers["regressions"] == 0 and by["deliver"].numbers["base_tests_passed"] == 3
    state = tmp_path / "run" / "state"
    base = json.loads((state / BASE_TESTS).read_text())
    assert base["passing"] == BASE_PASSING and base["passed"] == 3 and base["report_valid"] is True
    assert base["base"] == (state / "base").read_text().strip()
    regression = _regression(tmp_path)
    assert regression["regressions"] == 0 and regression["mode"] == "cases" and regression["findings"] == []
    assert regression["base_passed"] == 3 and regression["candidate_passed"] == 7
    pr = (tmp_path / "run" / "pr.md").read_text()
    assert "[BLOCKED]" not in pr and "Regression guard" not in pr
    remote = tmp_path / "run" / "remote.git"
    (branch,) = _factory_branches(tmp_path)
    delivered = git(remote, "ls-tree", "-r", "--name-only", branch).split()
    assert "tests/test_percent_change.py" in delivered and f"{ART}/regression.json" in delivered


@pytest.mark.parametrize(
    ("case", "title"),
    [
        ("deletes_base_test", "Regression guard: pytest/tests.test_core::test_negative_rejected is missing"),
        ("skips_base_test", "Regression guard: pytest/tests.test_core::test_compound_annual is now skipped"),
    ],
)
def test_a_build_that_drops_a_base_test_delivers_blocked(tmp_path: Path, case: str, title: str) -> None:
    """The suite is green (more tests pass than on the base) and review approves, but a test the
    base passed is gone or skipped: deliver routes it through the existing [BLOCKED] path."""
    report = _run(tmp_path, case)
    by = {s.stage: s for s in report.stages}
    assert by["build_and_test"].status == "ok" and by["build_and_test"].numbers["tests_passed"] == 1
    assert by["review"].status == "ok" and by["review"].numbers["blockers"] == 0
    assert by["deliver"].status == "blocked"
    assert by["deliver"].numbers["regressions"] == 1 and by["deliver"].numbers["blockers"] == 1
    regression = _regression(tmp_path)
    assert [f["title"] for f in regression["findings"]] == [title]
    assert regression["candidate_passed"] >= regression["base_passed"] == 3  # counts alone would pass
    pr = (tmp_path / "run" / "pr.md").read_text()
    assert pr.startswith("# [BLOCKED] DEMO-1:")
    assert "labels: factory, agent-authored, factory:blocked" in pr
    assert "### Blocker (1)" in pr and title in pr


@pytest.mark.parametrize(
    ("case", "path"),
    [("edits_factory_toml", "factory.toml"), ("adds_github_workflow", ".github/workflows/ci.yml")],
)
def test_a_build_that_touches_a_protected_path_is_refused_before_publication(
    tmp_path: Path, case: str, path: str
) -> None:
    with pytest.raises(StageError) as error:
        _run(tmp_path, case)
    assert error.value.kind == "policy"
    assert str(error.value) == f"[policy] delivered diff touches protected path(s): {path}"
    assert _factory_branches(tmp_path) == []  # nothing was pushed
    assert not (tmp_path / "run" / "pr.md").exists()
    log = [json.loads(line) for line in (tmp_path / "run" / "state" / "stages.jsonl").read_text().splitlines()]
    assert (log[-1]["stage"], log[-1]["status"]) == ("deliver", "failed")


def test_setup_retry_does_not_rerun_the_base_suite(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    real = stages.run_tests

    def counting(ctx: stages.Ctx):
        calls.append(ctx.state.read_control("workspace-head").strip())
        return real(ctx)

    monkeypatch.setattr(stages, "run_tests", counting)
    ctx = _ctx(tmp_path)
    stages.setup(ctx)
    base = ctx.state.read_control("base").strip()
    assert calls == [base]
    recorded = ctx.state.read_control(BASE_TESTS)
    assert json.loads(ctx.state.read_control(JUNIT_CASES))["head"] == base

    stages.setup(_ctx(tmp_path))  # a retry of the same run: same base, record already there
    assert calls == [base] and ctx.state.read_control(BASE_TESTS) == recorded


def test_setup_that_died_before_recording_measures_the_base_on_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = stages.run_tests

    def dies(ctx: stages.Ctx):
        raise StageError("sandbox", "cell lost mid-suite", retryable=True)

    monkeypatch.setattr(stages, "run_tests", dies)
    with pytest.raises(StageError):
        stages.setup(_ctx(tmp_path))
    assert not (tmp_path / "run" / "state" / BASE_TESTS).exists()
    monkeypatch.setattr(stages, "run_tests", real)
    ctx = _ctx(tmp_path)
    stages.setup(ctx)
    assert json.loads(ctx.state.read_control(BASE_TESTS))["passing"] == BASE_PASSING


def test_a_target_without_tests_records_an_explicit_empty_baseline(tmp_path: Path) -> None:
    """A new project legitimately has no suite yet: the base record says so instead of failing
    setup, and nothing then exists that a candidate could regress."""
    work = tmp_path / "work"
    work.mkdir()  # a non-empty workdir is used as the target as it is (seed_local_workdir)
    (work / "factory.toml").write_text('[commands]\ntest = "true"\n[paths]\nprotected = ["factory.toml"]\n')
    ctx = _ctx(tmp_path)
    stages.setup(ctx)
    base = ctx.state.read_control("base").strip()
    assert json.loads(ctx.state.read_control(BASE_TESTS)) == {
        "base": base,
        "passed": 0,
        "passing": None,
        "report_valid": False,
    }


def test_a_base_that_provisioning_left_dirty_is_recorded_as_unmeasured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A dependency sync that rewrites a tracked lockfile leaves a tree that is not the base, so it
    cannot be measured as one -- but a run like that worked before the guard existed, so setup goes
    on and the record says, in words, that no baseline was taken and why."""
    real = stages._review_policy

    def dirties(ctx: stages.Ctx) -> str:
        (tmp_path / "work" / "uv.lock").write_text("rewritten by a sync\n")
        return real(ctx)

    monkeypatch.setattr(stages, "_review_policy", dirties)
    stages.setup(_ctx(tmp_path))
    record = json.loads((tmp_path / "run" / "state" / BASE_TESTS).read_text())
    assert record["report_valid"] is False and record["passing"] is None and record["passed"] == 0
    assert "uv.lock" in record["unmeasured"]
    # An unmeasured baseline names no test a candidate could lose.
    assert regression_findings(record, {"t::a": "failed"}, _result(0, valid=False), tests_dir="tests") == []


def test_a_progressed_run_without_a_base_record_never_measures_the_candidate_as_base(tmp_path: Path) -> None:
    """A run that predates the guard (HEAD already moved past the base) records nothing, and
    deliver then refuses instead of publishing unguarded."""
    _run(tmp_path)
    (tmp_path / "run" / "state" / BASE_TESTS).unlink()
    ctx = _ctx(tmp_path)
    stages.setup(ctx)
    assert not ctx.state.has_control(BASE_TESTS)
    with pytest.raises(StageError, match="run has no base test record") as error:
        stages.deliver(ctx)
    assert error.value.kind == "policy"


def test_delivery_retry_after_the_artifact_commit_reuses_the_candidate_evidence(tmp_path: Path) -> None:
    """A retried deliver starts from its own artifact-chain commit; the suite ran on its parent,
    whose code is identical, so the evidence still describes what is published."""
    _run(tmp_path)
    ctx = _ctx(tmp_path)
    tested = json.loads(ctx.state.read_control(JUNIT_CASES))["head"]
    assert stages._workspace_head(ctx) != tested
    again = stages.deliver(ctx)
    assert again.status == "ok" and again.numbers["regressions"] == 0


def test_code_changed_after_the_suite_ran_is_stale_evidence(tmp_path: Path) -> None:
    _run(tmp_path)
    ctx = _ctx(tmp_path)
    work = tmp_path / "work"
    core = work / "src" / "calc" / "core.py"
    core.write_text(core.read_text() + "\n# changed after the suite ran\n")
    head = stages.commit(ctx, stage="build", msg="late edit")
    with pytest.raises(StageError, match="candidate test evidence is stale") as error:
        stages.deliver(ctx)
    assert error.value.kind == "policy"
    assert head == stages._workspace_head(ctx)


def test_missing_candidate_evidence_fails_closed(tmp_path: Path) -> None:
    _run(tmp_path)
    (tmp_path / "run" / "state" / JUNIT_CASES).unlink()
    with pytest.raises(StageError, match="no candidate test evidence") as error:
        stages.deliver(_ctx(tmp_path))
    assert error.value.kind == "policy"


def test_base_record_for_another_base_fails_closed(tmp_path: Path) -> None:
    _run(tmp_path)
    path = tmp_path / "run" / "state" / BASE_TESTS
    path.write_text(json.dumps({**json.loads(path.read_text()), "base": "0" * 40}))
    with pytest.raises(StageError, match="does not describe base") as error:
        stages.deliver(_ctx(tmp_path))
    assert error.value.kind == "policy"


def test_a_rename_out_of_a_protected_path_is_still_a_change_to_it(tmp_path: Path) -> None:
    """``--no-renames``: moving CLAUDE.md elsewhere reports the protected path as deleted, so a
    rename cannot launder a protected file out of its place. Editing the tests dir stays allowed."""
    ctx = _ctx(tmp_path)
    stages.setup(ctx)
    base = ctx.state.read_control("base").strip()
    work = tmp_path / "work"
    (work / "tests" / "test_extra.py").write_text("def test_extra():\n    assert True\n")
    stages.commit(ctx, stage="build", msg="tests only")
    stages._assert_unprotected_delivery(ctx, base)  # the tests dir is writable during build

    git(work, "mv", "CLAUDE.md", "NOTES.md")
    stages.commit(ctx, stage="build", msg="rename a protected file")
    with pytest.raises(StageError) as error:
        stages._assert_unprotected_delivery(ctx, base)
    assert error.value.kind == "policy"
    assert str(error.value) == "[policy] delivered diff touches protected path(s): CLAUDE.md"


def test_a_candidate_whose_report_lost_its_unique_ids_cannot_hide_a_deleted_test(tmp_path: Path) -> None:
    """The build deletes a base-passing test and adds two cases sharing one JUnit id, so the
    candidate's report has no usable ids and more tests pass than on the base. Counts alone would
    publish it green; the base recorded ids, so the guard refuses to judge by counts. (Review's
    requirement trace has no per-test evidence either and blocks too; no fix round is scripted.)"""
    bp, cfg = _config(tmp_path)
    cfg = cfg.model_copy(update={"max_review_fixes": 0})
    agent = ScriptedAgent([GUARD / "duplicate_ids", DEMO])
    report = execute(cfg, run_dir=tmp_path / "run", agent=agent, blueprint=bp)
    by = {s.stage: s for s in report.stages}
    assert by["build_and_test"].status == "ok" and by["build_and_test"].numbers["tests_passed"] == 1
    assert by["deliver"].status == "blocked" and by["deliver"].numbers["regressions"] == 1
    regression = _regression(tmp_path)
    assert regression["mode"] == "unconfirmed"
    assert regression["candidate_passed"] > regression["base_passed"] == 3  # counts alone would pass
    assert [f["title"] for f in regression["findings"]] == [NO_CANDIDATE_IDS]
    pr = (tmp_path / "run" / "pr.md").read_text()
    assert pr.startswith("# [BLOCKED] DEMO-1:") and NO_CANDIDATE_IDS in pr


def test_a_failed_base_measurement_is_not_saved_as_an_empty_baseline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The base holds tests, but its run left no valid report (a timeout, a lost download): an
    invalid report is a failed measurement, not a base that passed nothing. Saved as zero it would
    turn the guard off for the whole run, so setup fails retryable and measures again."""
    real = stages.run_tests

    def lost_report(ctx: stages.Ctx):
        return SuiteResult(exit_code=124, report_valid=False), "timed out before JUnit was written"

    monkeypatch.setattr(stages, "run_tests", lost_report)
    with pytest.raises(StageError, match="left no valid JUnit report although `tests` holds tests") as error:
        stages.setup(_ctx(tmp_path))
    assert error.value.kind == "sandbox" and error.value.retryable is True
    assert not (tmp_path / "run" / "state" / BASE_TESTS).exists()

    monkeypatch.setattr(stages, "run_tests", real)
    ctx = _ctx(tmp_path)
    stages.setup(ctx)  # the retry measures the base for real
    assert json.loads(ctx.state.read_control(BASE_TESTS))["passing"] == BASE_PASSING


def test_a_base_that_never_reports_is_recorded_unmeasured_after_bounded_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A base whose suite cannot report at all (a syntax error, a missing dependency) fails the same
    way on every attempt. Refusing setup forever would stop the very run that could repair it, so
    after ``MAX_BASE_MEASUREMENTS`` failed attempts the baseline is recorded as unmeasured."""

    def broken_base(ctx: stages.Ctx):
        return SuiteResult(exit_code=2, report_valid=False), "SyntaxError: invalid syntax"

    monkeypatch.setattr(stages, "run_tests", broken_base)
    for _ in range(stages.MAX_BASE_MEASUREMENTS - 1):
        with pytest.raises(StageError, match="left no valid JUnit report") as error:
            stages.setup(_ctx(tmp_path))
        assert error.value.retryable is True
        assert not (tmp_path / "run" / "state" / BASE_TESTS).exists()

    ctx = _ctx(tmp_path)
    stages.setup(ctx)  # the last allowed attempt records instead of refusing
    record = json.loads(ctx.state.read_control(BASE_TESTS))
    assert record["report_valid"] is False and record["passing"] is None and record["passed"] == 0
    assert f"in {stages.MAX_BASE_MEASUREMENTS} attempts" in record["unmeasured"]


def _break_the_sign(tmp_path: Path) -> None:
    core = tmp_path / "work" / "src" / "calc" / "core.py"
    text = core.read_text()
    assert "return (new - old) / old" in text
    core.write_text(text.replace("return (new - old) / old", "return (old - new) / old"))


def test_a_resumed_review_re_establishes_stale_test_evidence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A review fix X was committed and tested, then reverted, and the worker died while the
    restored head was being re-tested: the evidence names X, not HEAD. The re-run review re-tests
    HEAD instead of leaving deliver to refuse the stale record on every retry."""
    _run(tmp_path)
    ctx = _ctx(tmp_path)
    before = stages._workspace_head(ctx)
    _break_the_sign(tmp_path)
    fixed = stages.commit(ctx, stage="fix", msg="fix: a review fix that breaks the sign")
    tested, _ = stages.run_tests(ctx)
    assert not tested.ok and json.loads(ctx.state.read_control(JUNIT_CASES))["head"] == fixed
    stages.restore_head(ctx, before)  # ... and the worker dies before the restored head is re-tested
    with pytest.raises(StageError, match="candidate test evidence is stale"):
        stages.deliver(_ctx(tmp_path))

    # The killed review left no completed record, so the operator's re-run executes it again.
    ctx.state.append_json(stages.RUN_STAGES_LOG, StageResult(stage="review", status="failed").model_dump(mode="json"))
    calls: list[str] = []
    real = stages.run_tests

    def counting(ctx: stages.Ctx):
        calls.append(stages._workspace_head(ctx))
        return real(ctx)

    monkeypatch.setattr(stages, "run_tests", counting)
    reviewed = stages.review(_ctx(tmp_path))
    assert reviewed.status == "ok" and reviewed.numbers["tests_passed"] == 1.0
    assert calls == [before]
    assert json.loads(ctx.state.read_control(JUNIT_CASES))["head"] == before
    assert stages.refresh_test_evidence(_ctx(tmp_path), {}) is None and calls == [before]  # current: no re-run
    assert stages.deliver(_ctx(tmp_path)).status == "ok"


def test_a_stale_head_that_is_red_is_a_review_blocker(tmp_path: Path) -> None:
    """The kill landed between a fix commit and its first test run: HEAD is the untested fix. The
    re-tested head is red, so the review cannot end ok on it."""
    _run(tmp_path)
    ctx = _ctx(tmp_path)
    _break_the_sign(tmp_path)
    head = stages.commit(ctx, stage="fix", msg="fix: committed, never tested")
    numbers: dict[str, float] = {}
    blocker = stages.refresh_test_evidence(ctx, numbers)
    assert blocker is not None and blocker.severity == "blocker"
    assert blocker.title == "Tests failing on the head review resumed from"
    assert numbers["tests_passed"] == 0.0
    assert json.loads(ctx.state.read_control(JUNIT_CASES))["head"] == head


def test_the_run_declares_the_protected_set_it_enforces(tmp_path: Path) -> None:
    """Deliver refuses ``FIXED_PROTECTED`` plus the target's build list; the dialect card and the
    prompts name that same set, so no refusal arrives unannounced after every call is paid for."""
    from swfactory.campaign_fusion import FIXED_PROTECTED

    ctx = _ctx(tmp_path)
    stages.setup(ctx)
    card = stages.target_dialect(ctx)
    (line,) = [row for row in card.splitlines() if row.startswith("- Protected paths")]
    for path in ("AGENTS.md", "CLAUDE.md", "REVIEW_LIQUID.md", ".gitmodules", "factory.toml"):
        assert f"`{path}`" in line
    declared = stages.declared_protected(stages._contract(ctx), "build")
    assert set(FIXED_PROTECTED) <= set(declared) and "tests" not in declared
    assert "tests" in stages.declared_protected(stages._contract(ctx), "fix")
    for stage in ("plan", "build", "fix"):
        assert "AGENTS.md" in stages._protected(ctx, stage) and "CLAUDE.md" in stages._protected(ctx, stage)


def test_a_plan_that_lists_a_protected_path_is_refused_before_build(tmp_path: Path) -> None:
    with pytest.raises(StageError) as error:
        _run(tmp_path, "plans_agents_md")
    assert error.value.kind == "policy"
    assert str(error.value) == "[policy] plan lists protected path(s) the run may not change: AGENTS.md"
    log = [json.loads(line) for line in (tmp_path / "run" / "state" / "stages.jsonl").read_text().splitlines()]
    assert (log[-1]["stage"], log[-1]["status"]) == ("plan", "failed")
    assert "build_and_test" not in {row["stage"] for row in log}
    assert _factory_branches(tmp_path) == []
