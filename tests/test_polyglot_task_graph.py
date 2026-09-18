from __future__ import annotations

import json
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _turbo() -> dict[str, object]:
    return json.loads((ROOT / "turbo.json").read_text(encoding="utf-8"))


def test_turbo_is_an_accelerator_not_an_authority_layer() -> None:
    turbo = _turbo()

    assert turbo["remoteCache"] == {"enabled": False}

    flags = turbo["futureFlags"]
    assert flags["experimentalPythonWorkspaces"] is True
    assert flags["experimentalTaskCommand"] is True
    assert "experimentalCargoWorkspaces" not in flags

    tasks = turbo["tasks"]
    assert tasks["//#rust-build"]["cache"] is False
    assert tasks["//#rust-domain-contract"]["cache"] is False
    assert tasks["swfactory-python#test"]["dependsOn"] == ["//#rust-build"]
    assert set(tasks["//#polyglot-contract"]["dependsOn"]) == {
        "swfactory-python#test",
        "//#rust-domain-contract",
    }


def test_uv_root_has_a_stable_turbo_workspace_identity() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert project["tool"]["turbo"]["name"] == "swfactory-python"
    assert project["tool"]["uv"]["workspace"]["members"] == []
