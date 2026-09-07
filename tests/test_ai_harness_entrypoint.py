from __future__ import annotations

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAUNCHER = ROOT / "scripts" / "swf-harness.sh"


def _fake_swf(tmp_path: Path) -> tuple[Path, Path]:
    output = tmp_path / "called.txt"
    fake = tmp_path / "swf"
    fake.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"printf 'harness=%s\\nfactory=%s\\nargs=' \"$SWF_HARNESS\" \"$SWF_FACTORY_ID\" > {output!s}\n"
        f"printf '%q ' \"$@\" >> {output!s}\n"
    )
    fake.chmod(0o755)
    return fake, output


def test_launcher_is_valid_shell() -> None:
    subprocess.run(["bash", "-n", str(LAUNCHER)], check=True)


def test_launcher_forwards_explicit_harness_session_and_submit_args(tmp_path: Path) -> None:
    fake, output = _fake_swf(tmp_path)
    env = os.environ.copy()
    env["SWF_BIN"] = str(fake)
    subprocess.run(
        [
            "bash",
            str(LAUNCHER),
            "--harness",
            "claude",
            "--factory-id",
            "claude-session-17",
            "--issue",
            "1204",
            "--blueprint",
            "factory",
        ],
        cwd=ROOT,
        env=env,
        check=True,
        text=True,
        capture_output=True,
    )
    text = output.read_text()
    assert "harness=claude" in text
    assert "factory=claude-session-17" in text
    assert "args=submit --issue 1204 --blueprint factory" in text


def test_launcher_reuses_environment_identity(tmp_path: Path) -> None:
    fake, output = _fake_swf(tmp_path)
    env = os.environ.copy()
    env.update(
        {
            "SWF_BIN": str(fake),
            "SWF_HARNESS": "codex",
            "SWF_FACTORY_ID": "codex-session-a",
        }
    )
    subprocess.run(
        ["bash", str(LAUNCHER), "--issue", "1201"],
        cwd=ROOT,
        env=env,
        check=True,
        text=True,
        capture_output=True,
    )
    text = output.read_text()
    assert "harness=codex" in text
    assert "factory=codex-session-a" in text
    assert "args=submit --issue 1201" in text
