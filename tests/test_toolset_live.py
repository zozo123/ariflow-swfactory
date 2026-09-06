"""Opt-in real microVM roundtrip; needs a running, authenticated sbx daemon.

SWF_TEST_LIVE_TOOLSET=1 uv run --no-sync pytest tests/test_toolset_live.py
This test deliberately requests open networking, performs no network commands, and never changes
host policies. It verifies transport/lifecycle, not network confinement or a full factory run.
"""

from __future__ import annotations

import os

import pytest

from swfactory.sandbox import ToolsetSandbox, load_toolset_backend

pytestmark = pytest.mark.skipif(
    os.environ.get("SWF_TEST_LIVE_TOOLSET") != "1", reason="opt-in real sbx microVM test"
)


def test_real_sbx_roundtrip_and_teardown():
    backend = load_toolset_backend("sbx")
    sandbox = ToolsetSandbox(backend, workdir="/tmp/factory", block_network=False)
    try:
        sandbox.ensure()
        sandbox.write("nested/hello.txt", "hello from Airflow main\n")
        assert sandbox.exists("nested/hello.txt")
        assert sandbox.read("nested/hello.txt") == "hello from Airflow main\n"
        result = sandbox.run("pwd && cat nested/hello.txt")
        assert result.ok, result.stderr
        assert result.stdout == "/tmp/factory\nhello from Airflow main\n"
        assert sandbox.run("exit 7").exit_code == 7
    finally:
        sandbox.close()
    assert sandbox.sandbox_id is None, "backend failed to destroy the test microVM"
