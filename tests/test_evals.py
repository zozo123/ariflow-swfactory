"""The eval suite's own tests: front-matter parsing, ``check``, ``score``, ``baseline_diff``.

Everything here is hermetic — synthetic ``RunReport``s and tmp workdirs — except the last test,
which really runs two of the committed evals end to end with the scripted agent (~10 s).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from swfactory.evals import (
    Eval,
    EvalOutcome,
    Expect,
    baseline_diff,
    check,
    delivered_labels,
    load_baseline,
    load_suite,
    parse_eval,
    run_suite,
    score,
)
from swfactory.models import RunReport, StageResult

ROOT = Path(__file__).resolve().parents[1]
SUITE = ROOT / "demo" / "evals"
CLEAN_STAGES = ("intent", "spec", "plan", "build_and_test", "review", "deliver")


# ---------------------------------------------------------------- helpers


def _stage(stage: str, status: str = "ok", **numbers: float) -> StageResult:
    return StageResult(stage=stage, status=status, numbers=numbers)


def _report(
    *stages: StageResult, tests_passed: bool = True, pr_url: str | None = None
) -> RunReport:
    return RunReport(
        run_id="e0000001",
        issue_id="EVAL-X",
        agent="scripted",
        sandbox="local:work",
        scm="local",
        stages=list(stages),
        approvals=[],
        pr_url=pr_url,
        tests_passed=tests_passed,
    )


def _clean_report(**kw: object) -> RunReport:
    """A run that did everything right: every stage ok, one build iteration, no blockers."""
    return _report(
        _stage("intent"),
        _stage("spec"),
        _stage("plan", files=3),
        _stage("build_and_test", iterations=1, tests_passed=1),
        _stage("review", blockers=0, fixes=0),
        _stage("deliver", blockers=0, commits=2, rejected=0),
        **kw,
    )


def _eval(**expect: object) -> Eval:
    return Eval(
        id="EVAL-X",
        title="synthetic",
        slug="00-synthetic",
        issue_path=Path("issue.md"),
        fixtures_dir=Path("."),
        expect=Expect.model_validate(expect),
    )


@pytest.fixture
def workdir(tmp_path: Path) -> Path:
    """A finished run's target checkout: contract, package, and the committed artifact chain."""
    (tmp_path / "factory.toml").write_text(
        '[commands]\ntest = "pytest"\n\n[paths]\nsource = "src"\ntests = "tests"\n',
        encoding="utf-8",
    )
    pkg = tmp_path / "src" / "calc"
    pkg.mkdir(parents=True)
    pkg.joinpath("__init__.py").write_text(
        'from calc.core import average\n\n__all__ = ["average"]\n', encoding="utf-8"
    )
    art = tmp_path / "docs" / "factory" / "EVAL-X"
    art.mkdir(parents=True)
    art.joinpath("spec.md").write_text("# Spec\n\n## Open questions\nHow many places?\n", "utf-8")
    return tmp_path


# ---------------------------------------------------------------- front matter


def test_parse_eval_reads_the_expect_block() -> None:
    ev = parse_eval(SUITE / "01-average" / "issue.md")
    assert (ev.id, ev.slug) == ("EVAL-AVERAGE", "01-average")
    assert ev.expect.stages == list(CLEAN_STAGES)
    assert (ev.expect.tests_pass, ev.expect.max_build_iterations) == (True, 1)
    assert ev.expect.exports == ["calc.average"]
    assert ev.artifacts == "docs/factory/EVAL-AVERAGE"


def test_load_suite_loads_every_committed_eval() -> None:
    evals = load_suite(SUITE)
    assert 6 <= len(evals) <= 50  # the playbook's range; 20-50 is the goal, not today's truth
    assert len({e.id for e in evals}) == len(evals)
    for ev in evals:
        assert (ev.fixtures_dir / "build.1.patch").is_file(), ev.slug
        assert (ev.fixtures_dir / "plan.json").is_file(), ev.slug


def test_committed_baseline_covers_the_committed_suite() -> None:
    """A new eval must land with its baseline entry, or the gate silently ignores it."""
    baseline = load_baseline(SUITE / "baseline.json")
    assert set(baseline["evals"]) == {e.id for e in load_suite(SUITE)}
    assert baseline["passed"] == baseline["total"] == len(baseline["evals"])


@pytest.mark.parametrize(
    ("name", "text"),
    [
        ("no front matter", "just a body\n"),
        ("unterminated", "---\nid: E\ntitle: T\nbody without a closing fence\n"),
        ("no id", "---\ntitle: T\nexpect: {}\n---\nbody\n"),
        ("no expect", "---\nid: E\ntitle: T\n---\nbody\n"),
        ("bad yaml", "---\nid: [E\n---\nbody\n"),
        ("unknown expect key", "---\nid: E\ntitle: T\nexpect:\n  stagez: [spec]\n---\nbody\n"),
        ("wrong expect type", "---\nid: E\ntitle: T\nexpect:\n  blockers: many\n---\nbody\n"),
    ],
)
def test_parse_eval_rejects_a_bad_file(tmp_path: Path, name: str, text: str) -> None:
    path = tmp_path / "issue.md"
    path.write_text(text, encoding="utf-8")
    with pytest.raises(ValueError, match=str(path.name)):
        parse_eval(path)


def test_load_suite_without_evals_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="no evals found"):
        load_suite(tmp_path)


# ---------------------------------------------------------------- check


def test_check_passes_a_good_run(workdir: Path) -> None:
    ev = _eval(
        stages=list(CLEAN_STAGES),
        tests_pass=True,
        max_build_iterations=1,
        max_review_fixes=1,
        blockers=0,
        exports=["calc.average"],
        artifacts_contain={"spec.md": ["## Open questions", "?"]},
    )
    assert check(ev, _clean_report(), workdir) == []


def test_check_flags_a_stage_that_did_not_reach_ok(workdir: Path) -> None:
    report = _report(_stage("intent"), _stage("spec", "blocked"), _stage("deliver", blockers=0))
    failures = check(_eval(stages=["spec", "review"]), report, workdir)
    assert failures == [
        "stage spec: expected ok, got blocked",
        "stage review: expected ok, got no record",
    ]


def test_check_flags_failed_tests(workdir: Path) -> None:
    failures = check(_eval(tests_pass=True), _clean_report(tests_passed=False), workdir)
    assert failures == ["tests_passed: expected True, got False"]


def test_check_bounds_are_upper_bounds_not_equalities(workdir: Path) -> None:
    """Reaching the same outcome in fewer iterations is an improvement, never a failure."""
    slow = _report(_stage("build_and_test", iterations=3), _stage("review", fixes=2))
    assert check(_eval(max_build_iterations=2, max_review_fixes=1), slow, workdir) == [
        "build_and_test.iterations: expected <= 2, got 3",
        "review.fixes: expected <= 1, got 2",
    ]
    assert check(_eval(max_build_iterations=3, max_review_fixes=2), slow, workdir) == []


def test_check_flags_a_stage_that_recorded_no_counter(workdir: Path) -> None:
    report = _report(_stage("intent"), _stage("deliver", blockers=0))
    assert check(_eval(max_build_iterations=1), report, workdir) == [
        "build_and_test.iterations: expected <= 1, stage did not record it"
    ]


def test_check_flags_the_wrong_blocker_count(workdir: Path) -> None:
    blocked = _report(
        _stage("review", "blocked", blockers=2), _stage("deliver", "blocked", blockers=2)
    )
    assert check(_eval(blockers=0), blocked, workdir) == ["blockers: expected 0, got 2"]
    assert check(_eval(blockers=2), blocked, workdir) == []
    assert check(_eval(blockers=1), _report(_stage("intent")), workdir) == [
        "blockers: expected 1, no review or deliver record"
    ]


def test_check_reads_the_expected_label_off_the_published_pr(workdir: Path, tmp_path: Path) -> None:
    pr = tmp_path / "pr.md"
    pr.write_text(
        "# [BLOCKED] EVAL-X\n\nlabels: factory, agent-authored, factory:blocked\n", "utf-8"
    )
    report = _report(
        _stage("review", "blocked", blockers=1),
        _stage("deliver", "blocked", blockers=1),
        pr_url=f"file://{pr}",
    )
    assert check(_eval(blockers=1, label="factory:blocked"), report, workdir) == []
    assert check(_eval(label="factory:rejected"), report, workdir) == [
        "label factory:rejected: PR carries ['factory', 'agent-authored', 'factory:blocked']"
    ]


def test_check_flags_a_blocked_label_on_a_clean_pr(workdir: Path, tmp_path: Path) -> None:
    pr = tmp_path / "pr.md"
    pr.write_text("# EVAL-X\n\nlabels: factory, agent-authored\n", encoding="utf-8")
    report = _clean_report(pr_url=f"file://{pr}")
    assert check(_eval(label="factory:blocked"), report, workdir) == [
        "label factory:blocked: PR carries ['factory', 'agent-authored']"
    ]


def test_delivered_labels_falls_back_to_the_deliver_counters() -> None:
    """No readable pr.md (a GitHub url needs a token): the deliver counters stand in."""
    assert delivered_labels(_clean_report()) == []
    assert delivered_labels(_report(_stage("deliver", "blocked", blockers=1))) == [
        "factory:blocked"
    ]
    assert delivered_labels(_report(_stage("deliver", "blocked", rejected=1))) == [
        "factory:rejected"
    ]
    assert delivered_labels(_report(_stage("intent"))) is None


def test_check_flags_a_missing_export(workdir: Path) -> None:
    failures = check(_eval(exports=["calc.median", "average"]), _clean_report(), workdir)
    assert failures == [
        "export calc.median: 'median' is not defined or imported in calc/__init__.py",
        "export 'average': must be dotted, e.g. calc.average",
    ]


def test_check_flags_a_symbol_missing_from_dunder_all(workdir: Path) -> None:
    pkg = workdir / "src" / "calc" / "__init__.py"
    pkg.write_text('from calc.core import average, median\n\n__all__ = ["average"]\n', "utf-8")
    assert check(_eval(exports=["calc.median"]), _clean_report(), workdir) == [
        "export calc.median: 'median' is missing from __all__ in calc/__init__.py"
    ]


def test_check_flags_an_unknown_module(workdir: Path) -> None:
    assert check(_eval(exports=["nope.thing"]), _clean_report(), workdir) == [
        "export nope.thing: no module nope under src/"
    ]


def test_check_flags_a_missing_or_thin_artifact(workdir: Path) -> None:
    ev = _eval(artifacts_contain={"spec.md": ["## Risks"], "plan.md": ["anything"]})
    assert check(ev, _clean_report(), workdir) == [
        "artifact spec.md: does not contain '## Risks'",
        "artifact plan.md: missing from docs/factory/EVAL-X/",
    ]


# ---------------------------------------------------------------- score and baseline


def test_score_counts_passes_and_keeps_the_detail() -> None:
    results = [
        EvalOutcome(id="A", slug="01-a"),
        EvalOutcome(id="B", slug="02-b", failures=["tests_passed: expected True, got False"]),
    ]
    assert score(results) == {
        "passed": 1,
        "total": 2,
        "evals": {
            "A": {"slug": "01-a", "passed": True, "failures": []},
            "B": {
                "slug": "02-b",
                "passed": False,
                "failures": ["tests_passed: expected True, got False"],
            },
        },
    }


def test_baseline_diff_reports_only_regressions() -> None:
    baseline = score([EvalOutcome(id="A"), EvalOutcome(id="B", failures=["was already broken"])])
    current = score(
        [
            EvalOutcome(id="A", failures=["blockers: expected 0, got 1"]),  # regression
            EvalOutcome(id="B"),  # a new pass: never a failure
            EvalOutcome(id="C", failures=["stage spec: expected ok, got blocked"]),  # new gap
        ]
    )
    assert baseline_diff(current, baseline) == ["A: regressed — blockers: expected 0, got 1"]
    assert baseline_diff(baseline, baseline) == []


def test_baseline_diff_flags_an_eval_that_left_the_suite() -> None:
    baseline = score([EvalOutcome(id="A"), EvalOutcome(id="B")])
    current = score([EvalOutcome(id="A")])
    assert baseline_diff(current, baseline) == [
        "B: passed in the baseline but is no longer in the suite"
    ]


def test_load_baseline_rejects_a_non_score_file(tmp_path: Path) -> None:
    path = tmp_path / "baseline.json"
    path.write_text(json.dumps({"passed": 1}), encoding="utf-8")
    with pytest.raises(ValueError, match="not a suite score"):
        load_baseline(path)


# ---------------------------------------------------------------- the real thing


@pytest.mark.slow
def test_two_evals_run_end_to_end_with_the_scripted_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One clean eval and the blocked one, through the same pipeline the CLI walks (~10 s).

    This is the test that would catch a prompt, guard or stage change the synthetic reports
    above cannot see: the fixtures really apply, the target's suite really runs, and the blocked
    eval really ends as a ``factory:blocked`` PR.
    """
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(tmp_path / "empty-gitconfig"))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.setenv("GIT_TERMINAL_PROMPT", "0")
    monkeypatch.chdir(ROOT)
    result = run_suite(
        SUITE,
        only=["01-average", "05-blocked-missing-tests"],
        work_root=tmp_path / "runs",
        echo=lambda _line: None,
    )
    assert result["evals"]["EVAL-AVERAGE"]["failures"] == []
    assert result["evals"]["EVAL-MEDIAN"]["failures"] == []
    assert (result["passed"], result["total"]) == (2, 2)
    pr = (tmp_path / "runs" / "05-blocked-missing-tests" / "run" / "pr.md").read_text()
    assert "factory:blocked" in pr and "[BLOCKED]" in pr
