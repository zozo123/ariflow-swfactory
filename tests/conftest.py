"""Shared pytest configuration (markers only; fixtures live next to their tests)."""

from __future__ import annotations

import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "slow: end-to-end tests that spawn subprocesses (Airflow dag.test, srt)"
    )
