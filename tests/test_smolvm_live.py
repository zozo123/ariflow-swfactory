"""Opt-in Linux/KVM checks. Never confused with the hermetic HTTP contract tests.

SWF_TEST_LIVE_SMOLVM=1 uv run --no-sync pytest tests/test_smolvm_live.py
Requires a host-owned smolvm serve socket and a factory image with bash/coreutils/Python.
"""

from __future__ import annotations

import os
import shlex
import uuid

import pytest

from swfactory.sandbox import ToolsetSandbox
from swfactory.smolvm_backend import SmolvmSandboxBackend
from swfactory.state import RunState

pytestmark = pytest.mark.skipif(os.environ.get("SWF_TEST_LIVE_SMOLVM") != "1", reason="opt-in real SmolVM test")


def new_backend(tmp_path, name):
    return SmolvmSandboxBackend(
        socket_path=os.environ.get("SWF_TOOLSET_SMOLVM_SOCKET", "/run/smolvm/api.sock"),
        image=os.environ.get("SWF_TOOLSET_SMOLVM_IMAGE", "ghcr.io/zozo123/swfactory-sandbox:latest"),
        state=RunState(tmp_path),
        machine_name=name,
    )


def test_real_files_reconnect_and_network_block(tmp_path):
    name = "swf-smol-" + uuid.uuid4().hex
    be = new_backend(tmp_path, name)
    sandbox = ToolsetSandbox(be, workdir="/tmp/swf-smoke", state=RunState(tmp_path))
    try:
        sandbox.ensure()
        sandbox.write("nested/hello.txt", "hello SmolVM\n")
        assert sandbox.read("nested/hello.txt") == "hello SmolVM\n"
        result = sandbox.run("cat nested/hello.txt")
        assert result.ok and result.stdout == "hello SmolVM\n", result
        assert sandbox.run("exit 7").exit_code == 7
        restarted = ToolsetSandbox(new_backend(tmp_path, name), workdir="/tmp/swf-smoke", state=RunState(tmp_path))
        restarted.ensure()
        assert restarted.read("nested/hello.txt") == "hello SmolVM\n"
        # Direct IP avoids mistaking DNS failure for outbound network enforcement.
        probe = (
            "import socket\ntry:\n socket.create_connection(('1.1.1.1',443),2)\n"
            "except OSError:\n pass\nelse:\n raise SystemExit(1)"
        )
        assert restarted.run("python -c " + shlex.quote(probe), timeout_s=10).ok
    finally:
        be.destroy(name)  # Raises if cleanup is not confirmed; do not hide live-test cleanup debt.
    assert be._load(name)["phase"] == "deleted"


def test_real_timeout_destroys_incarnation(tmp_path):
    name = "swf-smol-" + uuid.uuid4().hex
    be = new_backend(tmp_path, name)
    try:
        be.create()
        result = be.run_command(name, "sleep 60", timeout=1, max_output_bytes=1024)
        assert result.timed_out and result.sandbox_terminated
        assert be._load(name)["phase"] == "deleted"
    finally:
        be.destroy(name)
