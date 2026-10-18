"""The requirement -> acceptance-test trace (VOLO step 2): pure parsing and matching, the review
blockers it produces, the plan-gate coverage table and the review prompt's contract checklist,
then end to end with the scripted agent: a planned test that was renamed, skipped, or whose JUnit
evidence is stale blocks review in code, whatever the scripted reviewer says."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from swfactory import stages
from swfactory.agent import ScriptedAgent, render_prompt
from swfactory.approval_policy import SCRIPTED_REPLAY_FIXTURE
from swfactory.blueprint import load
from swfactory.cli import execute
from swfactory.models import RunReport, StageError
from swfactory.stages import (
    contract_checklist,
    coverage_table,
    matching_cases,
    plan_test_map,
    requirement_trace,
    spec_requirements,
    trace_findings,
    trace_numbers,
)

ROOT = Path(__file__).resolve().parents[1]
ART = "docs/factory/DEMO-1"
DEMO = ROOT / "demo" / "scripted"

SPEC_BARE = """# Spec — X

## Requirements
1. `f()` is exported.
2. Returns the mean,
   rounded to two places.
3. Empty input raises `ValueError`.

## API
1. not a requirement: outside the section
"""

SPEC_TAGGED = """# Spec — X

## Requirements
- R1: exported from `calc`.
- **R2.** returns the mean
R3. empty raises
- **R4** — docstring says so

## Open questions
None.
"""


# ---------------------------------------------------------------- parsing


def test_spec_requirements_accept_bare_numbering_and_join_continuations() -> None:
    assert spec_requirements(SPEC_BARE) == {
        "R1": "`f()` is exported.",
        "R2": "Returns the mean, rounded to two places.",
        "R3": "Empty input raises `ValueError`.",
    }


def test_spec_requirements_accept_r_prefixed_numbering() -> None:
    assert spec_requirements(SPEC_TAGGED) == {
        "R1": "exported from `calc`.",
        "R2": "returns the mean",
        "R3": "empty raises",
        "R4": "docstring says so",
    }


def test_spec_without_a_requirements_section_names_nothing() -> None:
    assert spec_requirements("(none)") == {}
    assert spec_requirements("# Spec\n\n## API\n1. f\n") == {}


def test_every_recorded_fixture_spec_parses_five_requirements() -> None:
    specs = [DEMO / "spec.md", ROOT / "demo" / "selfhost-scripted" / "spec.md"]
    specs += sorted((ROOT / "demo" / "evals").glob("*/spec.md"))
    for spec in specs:
        assert list(spec_requirements(spec.read_text())) == ["R1", "R2", "R3", "R4", "R5"], spec


def test_plan_test_map_reads_tags_lists_ranges_and_work_nodes() -> None:
    plan = {
        "tests": [
            "R1: test_exported — f is importable",
            "R2, R3: test_shared — one test, two requirements",
            "R4-R5: test_range",
            "R6 - test_dash_separated - selfhost fixture style",
            "R7: `tests/test_x.py::test_qualified` — qualified and quoted",
            "R8: `suite name::adds two numbers` — a free-form runner name",
            "R1: percent_change is exported — prose, not a test name",
            "R9: tests cover the docstring",
            "test_untagged — no requirement",
            42,
        ],
        "work": [{"id": "api", "tests": ["R2: test_node_level — from a work node", "R1: test_exported"]}],
    }
    assert plan_test_map(plan) == {
        "R1": ["test_exported"],
        "R2": ["test_shared", "test_node_level"],
        "R3": ["test_shared"],
        "R4": ["test_range"],
        "R5": ["test_range"],
        "R6": ["test_dash_separated"],
        "R7": ["tests/test_x.py::test_qualified"],
        "R8": ["suite name::adds two numbers"],
    }
    assert plan_test_map(None) == {} and plan_test_map(["R1: test_x"]) == {}


def test_every_recorded_fixture_plan_maps_its_tests() -> None:
    plan = json.loads((DEMO / "plan.json").read_text())
    assert plan_test_map(plan) == {
        "R1": ["test_exported"],
        "R2": ["test_increase"],
        "R3": ["test_zero_old_raises"],
        "R4": ["test_decrease"],
    }
    for path in sorted((ROOT / "demo" / "evals").glob("*/plan.json")):
        mapped = plan_test_map(json.loads(path.read_text()))
        listed = [t for t in json.loads(path.read_text())["tests"] if t.startswith("R")]
        assert sum(len(tests) for tests in mapped.values()) == len(listed), path


# ---------------------------------------------------------------- matching and statuses


CASES = {
    "pytest/tests.test_core::test_simple_interest": "passed",
    "pytest/tests.test_x::test_exported": "passed",
    "pytest/tests.test_x::test_failing": "failed",
    "pytest/tests.test_x::test_erroring": "error",
    "pytest/tests.test_x::test_skipped": "skipped",
    "pytest/tests.test_x.TestGroup::test_in_class": "passed",
    "pytest/tests.test_y::test_exported": "passed",
    "pytest/tests.test_p::test_param[a]": "passed",
    "pytest/tests.test_p::test_param[b-c]": "failed",
    "pytest/tests.test_q::test_all_good[1]": "passed",
    "pytest/tests.test_q::test_all_good[2]": "passed",
    "jest/calc/math::adds two numbers": "passed",
}


def test_matching_by_exact_name_and_by_qualifier_suffix() -> None:
    assert matching_cases("test_exported", CASES) == [
        "pytest/tests.test_x::test_exported",
        "pytest/tests.test_y::test_exported",
    ]
    assert matching_cases("tests/test_x.py::test_exported", CASES) == ["pytest/tests.test_x::test_exported"]
    assert matching_cases("test_x::test_exported", CASES) == ["pytest/tests.test_x::test_exported"]
    assert matching_cases("TestGroup::test_in_class", CASES) == ["pytest/tests.test_x.TestGroup::test_in_class"]
    assert matching_cases("tests/test_z.py::test_exported", CASES) == []
    assert matching_cases("test_export", CASES) == []  # exact, never a prefix
    assert matching_cases("math::adds two numbers", CASES) == ["jest/calc/math::adds two numbers"]


def test_case_name_matching_with_params() -> None:
    assert matching_cases("test_param", CASES) == [
        "pytest/tests.test_p::test_param[a]",
        "pytest/tests.test_p::test_param[b-c]",
    ]
    assert matching_cases("test_param[a]", CASES) == ["pytest/tests.test_p::test_param[a]"]


def _statuses(plan_tests: list[str], cases: dict[str, str] | None = CASES) -> dict[str, str]:
    rows = requirement_trace("(none)", {"tests": plan_tests}, cases)
    return {row["requirement"]: row["status"] for row in rows}


def test_planned_test_statuses() -> None:
    assert _statuses(
        [
            "R1: test_exported",
            "R2: test_failing",
            "R3: test_erroring",
            "R4: test_skipped",
            "R5: test_never_written",
            "R6: test_param",  # one failing parametrization fails the test
            "R7: test_param[a]",
            "R8: test_all_good",
        ]
    ) == {
        "R1": "passed",
        "R2": "failed",
        "R3": "failed",
        "R4": "skipped",
        "R5": "missing",
        "R6": "failed",
        "R7": "passed",
        "R8": "passed",
    }


def test_a_requirement_takes_its_worst_planned_test() -> None:
    plan = ["R1: test_exported", "R1: test_skipped", "R2: test_exported", "R2: test_never_written"]
    assert _statuses(plan) == {"R1": "skipped", "R2": "missing"}


def test_without_evidence_every_planned_test_is_unverified() -> None:
    assert _statuses(["R1: test_exported"], None) == {"R1": "unverified"}


def test_rows_follow_spec_order_then_plan_only_tags_and_keep_untested_requirements() -> None:
    rows = requirement_trace(SPEC_BARE, {"tests": ["R3: test_exported", "R9: test_failing"]}, CASES)
    assert [(r["requirement"], r["status"], r["in_spec"]) for r in rows] == [
        ("R1", "untested", True),
        ("R2", "untested", True),
        ("R3", "passed", True),
        ("R9", "failed", False),
    ]
    (planned,) = rows[2]["planned_tests"]
    assert planned == {
        "test": "test_exported",
        "status": "passed",
        "cases": ["pytest/tests.test_x::test_exported", "pytest/tests.test_y::test_exported"],
        "case_count": 2,
    }
    assert trace_numbers(rows) == {
        "requirements": 4.0,
        "untested_requirements": 2.0,
        "acceptance_tests": 2.0,
        "missing_acceptance_tests": 1.0,
    }


# ---------------------------------------------------------------- findings


def test_one_blocker_per_planned_test_that_is_not_passing() -> None:
    rows = requirement_trace(
        SPEC_TAGGED,
        {"tests": ["R1: test_exported", "R2, R3: test_never_written", "R4: test_skipped", "R9: test_failing"]},
        CASES,
    )
    findings = trace_findings(rows, head="a" * 40, tests_dir="tests", evidence=True)
    assert [(f.severity, f.file, f.title) for f in findings] == [
        ("blocker", "tests", "Acceptance test missing: test_never_written"),
        ("blocker", "tests", "Acceptance test skipped: test_skipped"),
        ("blocker", "tests", "Acceptance test failed: test_failing"),
    ]
    assert findings[0].detail.startswith("R2, R3 is planned to be proven by `test_never_written`")
    assert "aaaaaaaaaaaa" in findings[0].detail


def test_untested_requirements_are_advisory_not_findings() -> None:
    rows = requirement_trace(SPEC_BARE, {"tests": ["R1: test_exported"]}, CASES)
    assert [r["status"] for r in rows] == ["passed", "untested", "untested"]
    assert trace_findings(rows, head="h", tests_dir="tests", evidence=True) == []


def test_missing_evidence_with_planned_tests_is_one_blocker() -> None:
    rows = requirement_trace(SPEC_BARE, {"tests": ["R1: test_exported", "R2: test_other"]}, None)
    (finding,) = trace_findings(rows, head="b" * 40, tests_dir="tests", evidence=False)
    assert finding.severity == "blocker" and finding.title == "No per-test JUnit evidence for the reviewed HEAD"
    assert "2 acceptance test(s)" in finding.detail and "bbbbbbbbbbbb" in finding.detail
    # A plan that maps no test has nothing to verify, so absent evidence is not a finding.
    assert (
        trace_findings(requirement_trace(SPEC_BARE, {"tests": []}, None), head="h", tests_dir="t", evidence=False) == []
    )


# ---------------------------------------------------------------- rendering


def test_contract_checklist_and_coverage_table() -> None:
    rows = requirement_trace(SPEC_BARE, {"tests": ["R1: test_exported", "R7: test_skipped"]}, CASES)
    checklist = contract_checklist(rows)
    assert checklist.splitlines() == [
        "- [passed] R1: `f()` is exported. — `test_exported` passed",
        "- [untested] R2: Returns the mean, rounded to two places. — no planned test",
        "- [untested] R3: Empty input raises `ValueError`. — no planned test",
        "- [skipped] R7: (not in the spec; tagged by the plan) — `test_skipped` skipped",
    ]
    table = coverage_table(requirement_trace(SPEC_BARE, {"tests": ["R1: test_exported", "R7: test_x"]}, None))
    assert "## Requirement coverage (advisory)" in table
    assert "- R1: test_exported\n" in table and "- R2: no planned test\n" in table
    assert "- R7: test_x (not in the spec)\n" in table
    assert "no numbered requirements" in coverage_table([])
    assert contract_checklist([], {"tests": ["test_a"]}).endswith("The plan's tests:\n- test_a")


def test_review_prompt_places_the_contract_checklist_after_the_diff() -> None:
    prompt = render_prompt(
        "review",
        issue_id="DEMO-1",
        review_policy="POLICY",
        spec="SPEC-BODY",
        plan="PLAN-BODY",
        diff="DIFF-BODY",
        checklist="- [missing] R4: CHECKLIST-ROW",
    )
    order = [
        prompt.index("SPEC-BODY"),
        prompt.index("## Diff under review"),
        prompt.index("DIFF-BODY"),
        prompt.index("## Contract checklist"),
        prompt.index("CHECKLIST-ROW"),
        prompt.index("## Output"),
    ]
    assert order == sorted(order)
    assert "cite each requirement the change does not meet as a finding" in prompt


def test_plan_prompt_requires_the_traceable_mapping_form() -> None:
    assert "R<n>: <exact test function name> — <what it proves>" in render_prompt("plan", issue_id="DEMO-1")


# ---------------------------------------------------------------- end to end (scripted)


@pytest.fixture(autouse=True)
def _at_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(ROOT)


def _run(tmp_path: Path, fixtures: list[Path], **overrides: object) -> tuple[RunReport, Path]:
    bp = load("factory")
    (job,) = bp.jobs({"issues": ["demo/issue.md"]})
    defaults: dict[str, object] = {
        "gate_replay": str(SCRIPTED_REPLAY_FIXTURE),
        "agent": "scripted",
        "sandbox": "local",
        "scm": "local",
        "workdir": str(tmp_path / "work"),
        # No review-fix fixture exists for these runs, and none could help: tests are read-only
        # in fix, so a missing or skipped acceptance test survives every fix round.
        "max_review_fixes": 0,
    }
    cfg = bp.config(job, run_id="tr4c3001", **{**defaults, **overrides})
    report = execute(cfg, run_dir=tmp_path / "run", agent=ScriptedAgent(fixtures), blueprint=bp)
    return report, tmp_path / "work" / ART


def _blocked_by(report: RunReport, art: Path, title: str) -> dict:
    by = {s.stage: s for s in report.stages}
    assert by["build_and_test"].status == "ok" and report.tests_passed is True
    assert by["review"].status == "blocked" and by["deliver"].status == "blocked"
    review = json.loads((art / "review.json").read_text())
    blockers = [f for f in review["findings"] if f["severity"] == "blocker"]
    assert [f["title"] for f in blockers] == [title]  # the scripted reviewer itself only approves
    assert review["verdict"] == "request_changes"
    return blockers[0]


def test_a_renamed_acceptance_test_blocks_review(tmp_path: Path) -> None:
    """The build wrote ``test_fall`` where the plan promised ``test_decrease`` for R4: the suite is
    green, the reviewer approves, and code still refuses -- R4 is unproven."""
    report, art = _run(tmp_path, [ROOT / "tests" / "fixtures" / "trace_renamed", DEMO])
    blocker = _blocked_by(report, art, "Acceptance test missing: test_decrease")
    assert blocker["detail"].startswith("R4 is planned to be proven by `test_decrease`")
    trace = json.loads((art / "trace.json").read_text())
    assert {r["requirement"]: r["status"] for r in trace["requirements"]}["R4"] == "missing"
    assert report.stages[-2].numbers["missing_acceptance_tests"] == 1
    assert "factory:blocked" in (tmp_path / "run" / "pr.md").read_text()


def test_a_skipped_acceptance_test_blocks_review(tmp_path: Path) -> None:
    """A skipped test keeps ``TestResult.ok`` green, so only the trace can see R4 proves nothing."""
    report, art = _run(tmp_path, [ROOT / "tests" / "fixtures" / "trace_skipped", DEMO])
    _blocked_by(report, art, "Acceptance test skipped: test_decrease")
    trace = json.loads((art / "trace.json").read_text())
    (r4,) = [r for r in trace["requirements"] if r["requirement"] == "R4"]
    assert r4["status"] == "skipped"
    assert r4["planned_tests"][0]["cases"] == ["pytest/tests.test_percent_change::test_decrease"]


def test_stale_junit_evidence_blocks_review(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Evidence recorded for another HEAD is no evidence: review fails closed."""
    original = stages.run_tests

    def stale(ctx: stages.Ctx):
        result = original(ctx)
        record = json.loads(ctx.state.read_control(stages.JUNIT_CASES))
        ctx.state.write_control(stages.JUNIT_CASES, json.dumps({**record, "head": "0" * 40}) + "\n")
        return result

    monkeypatch.setattr(stages, "run_tests", stale)
    # Review blocks on the stale trace; deliver's regression guard then refuses outright, since the
    # same stale record cannot vouch for what would be published (not even as a [BLOCKED] PR).
    with pytest.raises(StageError, match="candidate test evidence is stale") as error:
        _run(tmp_path, [DEMO])
    assert error.value.kind == "policy"
    art = tmp_path / "work" / ART
    log = [json.loads(line) for line in (tmp_path / "run" / "state" / "stages.jsonl").read_text().splitlines()]
    assert [(row["stage"], row["status"]) for row in log][-2:] == [("review", "blocked"), ("deliver", "failed")]
    review = json.loads((art / "review.json").read_text())
    (blocker,) = [f for f in review["findings"] if f["severity"] == "blocker"]
    assert blocker["title"] == "No per-test JUnit evidence for the reviewed HEAD"
    assert "4 acceptance test(s)" in blocker["detail"]
    assert not (tmp_path / "run" / "pr.md").exists()
    trace = json.loads((art / "trace.json").read_text())
    assert trace["evidence"] == "missing"
    assert {r["status"] for r in trace["requirements"]} == {"unverified", "untested"}
