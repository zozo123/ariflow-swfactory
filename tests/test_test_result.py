"""Test result semantics used by the governed build stage."""

from swfactory.models import TestResult


def test_all_skipped_test_report_is_not_successful() -> None:
    result = TestResult(skipped=3, exit_code=0, report_valid=True)

    assert result.ok is False


def test_passing_tests_with_legitimate_skips_remain_successful() -> None:
    result = TestResult(passed=2, skipped=1, exit_code=0, report_valid=True)

    assert result.ok is True
