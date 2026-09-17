"""The demo must test current source even after a same-size edit within one second."""

from __future__ import annotations

import os
import py_compile
import shutil
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_demo_test_command_replaces_stale_bytecode(tmp_path):
    target = tmp_path / "target"
    shutil.copytree(ROOT / "demo" / "target", target, ignore=shutil.ignore_patterns("__pycache__", ".venv", ".factory"))
    source = target / "src" / "calc" / "core.py"
    correct = source.read_text()
    broken = correct.replace("return principal * rate * years", "return principal + rate * years")
    assert broken != correct and len(broken) == len(correct)
    source.write_text(broken)
    timestamp = source.stat().st_mtime_ns
    bytecode = Path(
        py_compile.compile(str(source), doraise=True, invalidation_mode=py_compile.PycInvalidationMode.TIMESTAMP)
    )
    source.write_text(correct)
    os.utime(source, ns=(timestamp, timestamp))
    stale = bytecode.read_bytes()

    command = tomllib.loads((target / "factory.toml").read_text())["commands"]["test"]
    result = subprocess.run(["bash", "-c", command], cwd=target, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr
    assert bytecode.read_bytes() != stale
    assert (target / ".factory" / "junit.xml").is_file()


def test_the_demo_target_pins_the_interpreter_the_suite_uses() -> None:
    """Without this pin the bytecode test above passes only by accident of what is installed.

    `demo/target/pyproject.toml` asks for `>=3.12` and pinned nothing, so `uv run` inside the
    target picked the newest interpreter on the machine -- 3.14 on a developer box, 3.12 on CI.
    The test compiles `core.cpython-312.pyc` with the suite's interpreter and then asserts the
    demo's own test command rewrote it; under 3.14 that command writes `core.cpython-314.pyc`
    instead, so the watched file never changes and the failure reads as stale-bytecode logic
    rather than as two different Pythons.
    """
    pinned = (ROOT / "demo" / "target" / ".python-version").read_text().strip()

    assert pinned == (ROOT / ".python-version").read_text().strip()
