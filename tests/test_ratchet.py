"""Bounded repair with a ratchet (VOLO step 3), and the host-side fix scope.

``stages.ratchet`` is a pure rule: keep a repair iff it lost no passing test, or gained at least
``RATCHET_GAIN_PER_LOSS`` per test it lost; a changed test universe or a candidate without a valid
report is refused; without per-test ids it falls back to counts. The scripted end-to-end runs drive
the legacy build loop over ``demo/target`` with real git, real pytest and real JUnit:

* ``tests/fixtures/ratchet``: fix.2 trades test_zero_old_raises for test_increase + test_decrease
  (2 < 3 x 1) and is reverted; fix.3 is authored against build.1's tree and goes green.
* ``tests/fixtures/fix_weakens_tests``: fix.2 rewrites tests and is refused as policy on the local
  backend, where no kernel rule stops it.
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
from swfactory.models import RunReport, StageError
from swfactory.models import TestResult as SwfTestResult
from swfactory.stages import RATCHET_EVIDENCE_IDS, RATCHET_GAIN_PER_LOSS, cases_digest, ratchet, ratchet_note

ROOT = Path(__file__).resolve().parents[1]
ART = "docs/factory/DEMO-1"
SCRIPTED = ROOT / "demo" / "scripted"
UNIVERSE = {f"pytest/t::test_{index}": "passed" for index in range(10)}
DIGEST = cases_digest(UNIVERSE)


@pytest.fixture(autouse=True)
def _at_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(ROOT)


def _result(failing: list[int], *, digest: str | None = DIGEST, valid: bool = True, total: int = 10) -> SwfTestResult:
    nonpassing = tuple(sorted(f"pytest/t::test_{index}" for index in failing))
    return SwfTestResult(
        passed=total - len(failing),
        failed=len(failing),
        exit_code=1 if failing else 0,
        report_valid=valid,
        nonpassing_cases=nonpassing if digest else (),
        cases_digest=digest,
    )


# ---------------------------------------------------------------- the rule


@pytest.mark.parametrize(
    ("best", "cand", "keep", "lost", "gained"),
    [
        pytest.param([1, 2], [1], True, 0, 1, id="gain-only"),
        pytest.param([1, 2], [1, 2], True, 0, 0, id="tie-keeps"),
        pytest.param([1, 2, 3], [0], True, 1, 3, id="lose1-gain3-kept"),
        pytest.param([1, 2], [0], False, 1, 2, id="lose1-gain2-reverted"),
        pytest.param([1], [1, 2], False, 1, 0, id="pure-regression"),
        pytest.param([1, 2, 3, 4, 5, 6], [7, 8], True, 2, 6, id="lose2-gain6-kept"),
        pytest.param([1, 2, 3, 4, 5], [7, 8], False, 2, 5, id="lose2-gain5-reverted"),
        pytest.param([1, 2, 3], [], True, 0, 3, id="all-green"),
    ],
)
def test_the_ratchet_keeps_only_a_net_gain(
    best: list[int], cand: list[int], keep: bool, lost: int, gained: int
) -> None:
    kept, evidence = ratchet(_result(best), _result(cand))

    assert kept is keep
    assert evidence["verdict"] == ("kept" if keep else "reverted")
    assert (evidence["lost_count"], evidence["gained_count"]) == (lost, gained)
    assert evidence["lost"] == sorted(f"pytest/t::test_{index}" for index in set(cand) - set(best))


def test_the_gain_per_loss_is_volos_three() -> None:
    assert RATCHET_GAIN_PER_LOSS == 3


@pytest.mark.parametrize(("cand_passed", "keep"), [(7, False), (8, False), (9, True)])
def test_a_changed_test_universe_must_buy_strictly_more_passing_tests(cand_passed: int, keep: bool) -> None:
    """Different ids cannot be compared set-wise. Equal or fewer passing tests over a moved
    universe are refused (fail closed); strictly more is kept, because code alone moves it -- a
    fixed collection error turns one error case into the module's real cases."""
    cand = SwfTestResult(
        passed=cand_passed,
        failed=10 - cand_passed,
        exit_code=1,
        nonpassing_cases=tuple(f"pytest/u::test_{index}" for index in range(10 - cand_passed)),
        cases_digest="0" * 64,
    )

    kept, evidence = ratchet(_result([1, 2]), cand)

    assert kept is keep and "universe changed" in evidence["reason"] and evidence["lost"] == []


def test_a_fixed_collection_error_is_kept() -> None:
    """Real JUnit: build.1's test module fails to import (one error case); the fix makes it
    collectable, so three real cases replace it -- one of them still failing."""
    from swfactory.stages import _junit_cases, _parse_junit, _with_cases

    broken = (
        '<testsuites><testsuite name="pytest" tests="2" errors="1">'
        '<testcase classname="tests.test_core" name="test_old"/>'
        '<testcase classname="" name="tests.test_new"><error message="ImportError"/></testcase>'
        "</testsuite></testsuites>"
    )
    fixed = (
        '<testsuites><testsuite name="pytest" tests="4" failures="1">'
        '<testcase classname="tests.test_core" name="test_old"/>'
        '<testcase classname="tests.test_new" name="test_a"/>'
        '<testcase classname="tests.test_new" name="test_b"/>'
        '<testcase classname="tests.test_new" name="test_c"><failure message="x"/></testcase>'
        "</testsuite></testsuites>"
    )

    def parsed(xml: str) -> SwfTestResult:
        return _with_cases(SwfTestResult(**_parse_junit(xml), exit_code=1), _junit_cases(xml))

    kept, evidence = ratchet(parsed(broken), parsed(fixed))

    assert kept is True and "universe changed" in evidence["reason"]


def test_a_candidate_without_a_valid_report_is_refused() -> None:
    kept, evidence = ratchet(_result([1]), _result([], digest=None, valid=False))

    assert kept is False and "no valid JUnit report" in evidence["reason"]


def test_an_invalid_best_never_blocks_a_valid_candidate() -> None:
    best = SwfTestResult(exit_code=1, report_valid=False)

    kept, _ = ratchet(best, _result([1, 2, 3]))

    assert kept is True


@pytest.mark.parametrize(("cand_failing", "keep"), [([1], True), ([1, 2], True), ([1, 2, 3], False)])
def test_without_case_ids_the_rule_falls_back_to_counts(cand_failing: list[int], keep: bool) -> None:
    kept, evidence = ratchet(_result([1, 2], digest=None), _result(cand_failing, digest=None))

    assert kept is keep and evidence["lost"] == [] and "no per-test ids" in evidence["reason"]


def test_one_side_without_ids_also_falls_back_to_counts() -> None:
    kept, _ = ratchet(_result([1, 2]), _result([3], digest=None))

    assert kept is True


def test_a_passing_test_turned_skip_counts_as_lost() -> None:
    """``TestResult.ok`` tolerates skips; the ratchet does not let a fix skip its way to green."""
    best = _result([1])
    cand = SwfTestResult(
        passed=8,
        skipped=2,
        exit_code=0,
        nonpassing_cases=("pytest/t::test_2", "pytest/t::test_3"),
        cases_digest=DIGEST,
    )

    kept, evidence = ratchet(best, cand)

    assert cand.ok and kept is False and evidence["lost_count"] == 2 and evidence["gained_count"] == 1


def test_evidence_lists_are_capped_but_counts_are_not() -> None:
    universe = {f"pytest/t::test_{index}": "passed" for index in range(200)}
    digest = cases_digest(universe)
    best = SwfTestResult(passed=200, exit_code=0, nonpassing_cases=(), cases_digest=digest)
    lost = tuple(sorted(f"pytest/t::test_{index}" for index in range(100)))
    cand = SwfTestResult(passed=100, failed=100, exit_code=1, nonpassing_cases=lost, cases_digest=digest)

    _, evidence = ratchet(best, cand)

    assert len(evidence["lost"]) == RATCHET_EVIDENCE_IDS and evidence["lost_count"] == 100


def test_the_revert_note_names_tests_not_ids() -> None:
    _, evidence = ratchet(_result([1, 2]), _result([0]))

    note = ratchet_note("fix.2", "build.1", evidence)

    assert note.startswith("NOTE: fix.2 was reverted and the workspace is back at build.1")
    assert "broke test_0" in note and "fixed test_1, test_2" in note and "pytest/t::" not in note


# ---------------------------------------------------------------- the legacy loop, end to end


def _run(tmp_path: Path, fixtures: list[Path], **overrides: object) -> RunReport:
    bp = load("factory")
    (job,) = bp.jobs({"issues": ["demo/issue.md"]})
    cfg = bp.config(
        job,
        run_id="r4tch3t1",
        gate_replay=str(SCRIPTED_REPLAY_FIXTURE),
        agent="scripted",
        sandbox="local",
        scm="local",
        workdir=str(tmp_path / "work"),
        **overrides,
    )
    return execute(cfg, run_dir=tmp_path / "run", agent=ScriptedAgent(fixtures), blueprint=bp)


def test_a_regressing_fix_is_reverted_and_the_next_fix_starts_from_the_best_tree(tmp_path: Path) -> None:
    report = _run(tmp_path, [ROOT / "tests" / "fixtures" / "ratchet", SCRIPTED])
    by = {stage.stage: stage for stage in report.stages}
    build = by["build_and_test"]

    assert build.status == "ok" and by["deliver"].status == "ok" and report.tests_passed
    assert build.numbers["iterations"] == 3 and build.numbers["first_pass_ci"] == 0
    assert build.numbers["ratchet_reverts"] == 1 and build.numbers["best_iteration"] == 3
    assert f"{ART}/build-ratchet.json" in build.artifacts

    evidence = json.loads((tmp_path / "run" / "state" / "artifacts" / ART / "build-ratchet.json").read_text())
    rows = evidence["iterations"]
    assert [(row["agent_call"], row["verdict"]) for row in rows] == [
        ("build.1", "kept"),
        ("fix.2", "reverted"),
        ("fix.3", "kept"),
    ]
    assert [row["failed"] for row in rows] == [2, 1, 0]
    assert [name.rsplit("::", 1)[-1] for name in rows[1]["lost"]] == ["test_zero_old_raises"]
    assert sorted(name.rsplit("::", 1)[-1] for name in rows[1]["gained"]) == ["test_decrease", "test_increase"]
    assert evidence["best"]["agent_call"] == "fix.3" and evidence["reverts"] == 1

    work = tmp_path / "work"
    # fix.3 was applied on build.1's commit: the reverted fix.2 commit is not in the history.
    history = git(work, "log", "--format=%H", f"{rows[0]['head']}..{rows[2]['head']}").split()
    assert history == [rows[2]["head"]]
    assert rows[1]["head"] not in git(work, "log", "--format=%H").split()
    assert git(work, "cat-file", "-t", rows[1]["head"]) == "commit"  # still auditable by SHA
    prompt = (work / ".factory" / "prompt.fix.3.md").read_text()
    assert "NOTE: fix.2 was reverted and the workspace is back at build.1" in prompt
    assert "broke test_zero_old_raises" in prompt
    core = (work / "src" / "calc" / "core.py").read_text()
    assert 'raise ValueError("old must be non-zero")' in core and "return (new - old) / old" in core

    metrics = json.loads((work / ART / "metrics.json").read_text())
    assert metrics["iterations"] == 3 and metrics["ratchet_reverts"] == 1 and metrics["review_reverted_fixes"] == 0


def test_legacy_exhaustion_leaves_the_workspace_and_evidence_at_the_best_candidate(tmp_path: Path) -> None:
    """Delivery on exhaustion is unchanged -- a policy refusal, nothing published -- but the
    workspace, the run's fence and build-ratchet.json end on build.1, not on the reverted fix.2."""
    with pytest.raises(StageError) as raised:
        _run(tmp_path, [ROOT / "tests" / "fixtures" / "ratchet", SCRIPTED], max_build_iterations=2)

    assert raised.value.kind == "policy" and "(1 reverted by the ratchet)" in str(raised.value)
    assert "the workspace is left at the best candidate, build.1" in str(raised.value)
    evidence = json.loads((tmp_path / "run" / "state" / "artifacts" / ART / "build-ratchet.json").read_text())
    assert [(row["agent_call"], row["verdict"]) for row in evidence["iterations"]] == [
        ("build.1", "kept"),
        ("fix.2", "reverted"),
    ]
    assert evidence["best"]["agent_call"] == "build.1" and evidence["reverts"] == 1
    work = tmp_path / "work"
    head = git(work, "rev-parse", "HEAD")
    assert head == evidence["best"]["head"] == evidence["iterations"][0]["head"]
    assert (tmp_path / "run" / "state" / "workspace-head").read_text().strip() == head
    assert 'raise ValueError("old must be non-zero")' in (work / "src" / "calc" / "core.py").read_text()
    assert not (tmp_path / "run" / "pr.md").exists()


def test_a_fix_that_edits_tests_is_refused_and_the_workspace_restored(tmp_path: Path) -> None:
    """Unchecked, this fix would go green by weakening the tests (on the sign bug). The local
    backend has no kernel rule against it; the host check refuses the commit as policy."""
    with pytest.raises(StageError) as raised:
        _run(tmp_path, [ROOT / "tests" / "fixtures" / "fix_weakens_tests", SCRIPTED])

    assert raised.value.kind == "policy" and raised.value.retryable is False
    assert "fix commit" in str(raised.value) and "tests/test_percent_change.py" in str(raised.value)
    work = tmp_path / "work"
    assert git(work, "log", "-1", "--format=%s") == "build: applied scripted fixture build.1.patch"
    recorded = (tmp_path / "run" / "state" / "workspace-head").read_text().strip()
    assert recorded == git(work, "rev-parse", "HEAD"), "the run's fence moved back with the workspace"
    assert "assert True" not in (work / "tests" / "test_percent_change.py").read_text()
    assert git(work, "status", "--porcelain", "--", "src", "tests") == ""
