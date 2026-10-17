from pathlib import Path

import pytest

from swfactory import models
from swfactory.stages import _parse_junit

FIXTURES = Path(__file__).parent / "fixtures" / "junit"


@pytest.mark.parametrize(
    ("fixture", "expected"),
    [
        ("node22-standalone.xml", {"passed": 1, "failed": 1, "errors": 0, "skipped": 1}),
        ("node22-suite.xml", {"passed": 1, "failed": 1, "errors": 0, "skipped": 1}),
        ("node22-nested.xml", {"passed": 3, "failed": 1, "errors": 0, "skipped": 1}),
    ],
)
def test_node22_todo_failure_remains_failed_on_exit_zero(fixture: str, expected: dict[str, int]) -> None:
    counts = _parse_junit((FIXTURES / fixture).read_text())

    assert counts == expected
    result = models.TestResult(**counts, exit_code=0)
    assert result.total == sum(expected.values())
    assert result.ok is False


@pytest.mark.parametrize(
    ("report", "expected"),
    [
        ('<testsuites><testcase name="pass"/></testsuites>', {"passed": 1, "failed": 0, "errors": 0, "skipped": 0}),
        (
            '<testsuites xmlns="urn:junit"><testcase/><testcase><error/></testcase></testsuites>',
            {"passed": 1, "failed": 0, "errors": 1, "skipped": 0},
        ),
        (
            '<testsuites><testcase/><testsuite tests="2" failures="1"/></testsuites>',
            {"passed": 2, "failed": 1, "errors": 0, "skipped": 0},
        ),
        (
            '<testsuite tests="3" failures="1" errors="0" skipped="1">'
            '<testsuite tests="3" failures="1" errors="0" skipped="1">'
            "<testcase/><testcase><failure/></testcase><testcase><skipped/></testcase>"
            "</testsuite></testsuite>",
            {"passed": 1, "failed": 1, "errors": 0, "skipped": 1},
        ),
        (
            '<testsuite tests="3" failures="1" errors="1" skipped="1"/>',
            {"passed": 0, "failed": 1, "errors": 1, "skipped": 1},
        ),
        (
            '<testsuites><testsuite tests="0"/><testsuite tests="1"/></testsuites>',
            {"passed": 1, "failed": 0, "errors": 0, "skipped": 0},
        ),
        (
            "<testsuite><testcase/><testcase><skipped/><error/></testcase></testsuite>",
            {"passed": 1, "failed": 0, "errors": 1, "skipped": 0},
        ),
    ],
)
def test_junit_counts_each_case_once(report: str, expected: dict[str, int]) -> None:
    assert _parse_junit(report) == expected


@pytest.mark.parametrize(
    "report",
    [
        "<testsuites/>",
        '<testsuites tests="1"/>',
        '<testsuite tests="0"/>',
        '<not-junit><testsuite tests="1"/></not-junit>',
        '<testsuite tests="many"/>',
        '<testsuite tests="1" failures="2"/>',
        '<testsuite tests="1" failures="1" skipped="1"/>',
        '<testsuites><testsuite tests="2" failures="1"/><testsuite tests="2" failures="-1"/></testsuites>',
        '<testsuites><testsuite tests="-1"/><testsuite tests="2"/></testsuites>',
        "<testsuites><testcase/><testsuite/></testsuites>",
        '<testsuite tests="2"><testcase/></testsuite>',
        '<testsuite tests="1" failures="0"><testcase><failure/></testcase></testsuite>',
        '<testsuite tests="1" failures="1"><testcase/></testsuite>',
        '<testsuite tests="1" errors="1"><testcase/></testsuite>',
        '<testsuites tests="2"><testcase/></testsuites>',
        "<testsuites><testcase><failure/><error/></testcase></testsuites>",
        "<testsuites><testcase><testcase/></testcase></testsuites>",
        '<testsuites><testcase failure="lost failure"/></testsuites>',
        "<testsuites><testcase><failure><testcase/></failure></testcase></testsuites>",
        '<testsuite tests="1"><failure/></testsuite>',
        '<testsuite tests="1"><wrapper><testcase><failure/></testcase></wrapper></testsuite>',
    ],
)
def test_junit_rejects_impossible_or_ambiguous_evidence(report: str) -> None:
    with pytest.raises(ValueError):
        _parse_junit(report)


def test_all_skipped_test_report_is_not_successful() -> None:
    assert models.TestResult(skipped=3, exit_code=0, report_valid=True).ok is False


def test_passing_tests_with_legitimate_skips_remain_successful() -> None:
    assert models.TestResult(passed=2, skipped=1, exit_code=0, report_valid=True).ok is True


def test_case_ids_keep_nested_node_suites_apart_and_match_the_counts() -> None:
    from swfactory.stages import _junit_cases

    report = (FIXTURES / "node22-nested.xml").read_text()
    cases = _junit_cases(report)

    assert cases is not None
    assert cases["test::standalone pass"] == "passed"
    assert cases["outer/test::suite pass"] == "passed"
    assert cases["outer/inner/test::inner skip"] == "skipped"
    # A Node TODO failure also carries <skipped>; it is a failure, as _parse_junit counts it.
    assert cases["outer/inner/test::inner TODO"] == "failed"
    counts = _parse_junit(report)
    by_outcome = {outcome: sum(1 for value in cases.values() if value == outcome) for outcome in set(cases.values())}
    assert by_outcome == {"passed": counts["passed"], "failed": counts["failed"], "skipped": counts["skipped"]}


def test_pytest_ids_name_the_module_and_the_test() -> None:
    from swfactory.stages import _junit_cases, case_name

    report = (
        '<testsuites><testsuite name="pytest" tests="3" failures="1" errors="0" skipped="0">'
        '<testcase classname="tests.test_core" name="test_add"/>'
        '<testcase classname="tests.test_core" name="test_div[zero]"><failure/></testcase>'
        '<testcase classname="tests.test_core" name="test_div[one]"/>'
        "</testsuite></testsuites>"
    )
    cases = _junit_cases(report)

    assert cases == {
        "pytest/tests.test_core::test_add": "passed",
        "pytest/tests.test_core::test_div[zero]": "failed",
        "pytest/tests.test_core::test_div[one]": "passed",
    }
    assert case_name("pytest/tests.test_core::test_div[zero]") == "test_div"


@pytest.mark.parametrize(
    "report",
    [
        '<testsuite tests="3" failures="1" errors="0" skipped="0"/>',
        '<testsuites><testcase classname="a" name="x"/><testcase classname="a" name="x"/></testsuites>',
    ],
    ids=["summary-only", "duplicate-ids"],
)
def test_a_partial_or_ambiguous_universe_has_no_case_ids(report: str) -> None:
    from swfactory.stages import _junit_cases

    assert _junit_cases(report) is None


def test_a_result_carries_its_nonpassing_ids_and_a_universe_digest() -> None:
    from swfactory.stages import _with_cases, cases_digest

    cases = {"/s/c::a": "passed", "/s/c::b": "failed", "/s/c::c": "skipped"}
    result = _with_cases(models.TestResult(passed=1, failed=1, skipped=1, exit_code=1), cases)

    assert result.nonpassing_cases == ("/s/c::b", "/s/c::c")
    assert result.cases_digest == cases_digest({"/s/c::c": "error", "/s/c::a": "x", "/s/c::b": "y"})
    assert _with_cases(result, None) is result
    # An old receipt without the new fields still validates.
    legacy = models.TestResult.model_validate({"passed": 1, "exit_code": 0})
    assert legacy.nonpassing_cases == () and legacy.cases_digest is None
