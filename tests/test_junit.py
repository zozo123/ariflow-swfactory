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
