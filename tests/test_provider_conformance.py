"""Provider-contract checks for lost compute: an expired cell and a missing workspace.

Each adapter is driven to the two loss conditions and must answer ``alive() is False`` without
creating anything. The report is the retained evidence behind the ``ttl`` claim each provider
document makes in ``swfactory.sandbox_contract``. Hermetic: no islo, no Airflow, no network.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

from swfactory import sandbox as sandbox_mod
from swfactory.provider_conformance import CORE_CHECKS, CheckResult, capability_report, classify
from swfactory.sandbox import IsloSandbox, LocalSandbox, ToolsetSandbox
from swfactory.sandbox_contract import compute_lost, provider_documents

LOSS_CHECKS = ("expiration", "missing_workspace")


class _Backend:
    def __init__(self) -> None:
        self.present = True
        self.workdir = True

    def create(self, *, spec=None):
        return "sbx-1"

    def run_command(self, sandbox, command, *, timeout, max_output_bytes):
        class R:
            exit_code = 0 if (self.present and self.workdir) else 1
            stdout = ""
            stderr = "" if self.present else "no such sandbox"
            timed_out = False
            sandbox_terminated = not self.present

        return R()


def _islo(monkeypatch, tmp_path: Path) -> tuple[IsloSandbox, dict[str, bool]]:
    present = {"listed": True, "workdir": True}

    def fake_run(argv, **kwargs):
        if "--source" in argv:  # create-if-needed is exactly what a probe must never do
            raise AssertionError(f"alive() tried to create: {argv}")
        if argv[:2] == ["islo", "ls"]:
            rows = [{"name": "swf-x", "status": "running"}] if present["listed"] else []
            return subprocess.CompletedProcess(argv, 0, stdout=json.dumps(rows), stderr="")
        return subprocess.CompletedProcess(argv, 0 if present["workdir"] else 1, stdout="", stderr="")

    monkeypatch.setattr(sandbox_mod.subprocess, "run", fake_run)
    sb = IsloSandbox(
        "swf-x",
        source="github://o/r:main",
        gateway_profile="p",
        environment="e",
        ttl_s=3600,
        idle_s=60,
        target_dir="",
        factory_root=tmp_path,
    )
    return sb, present


def _results(*, expired: bool, workspace_gone: bool) -> list[CheckResult]:
    return [
        CheckResult("expiration", classify(passed=expired), "alive() is False once the provider dropped the cell"),
        CheckResult("missing_workspace", classify(passed=workspace_gone), "alive() is False once the checkout is gone"),
    ]


def test_loss_checks_are_core_not_optional() -> None:
    """A provider that cannot say its cell is gone would let a stage run against an empty VM."""
    assert set(LOSS_CHECKS) <= set(CORE_CHECKS)


def test_local_reports_loss(tmp_path: Path) -> None:
    sb = LocalSandbox(tmp_path / "w")
    sb.ensure()
    assert sb.alive()
    shutil.rmtree(tmp_path / "w")
    report = capability_report("local", _results(expired=not sb.alive(), workspace_gone=not sb.alive()))
    assert {k: report["core"][k] for k in LOSS_CHECKS} == dict.fromkeys(LOSS_CHECKS, "supported")


def test_islo_reports_loss(monkeypatch, tmp_path: Path) -> None:
    sb, present = _islo(monkeypatch, tmp_path)
    assert sb.alive()
    present["workdir"] = False
    workspace_gone = not sb.alive()
    present["workdir"], present["listed"] = True, False
    expired = not sb.alive()
    report = capability_report("islo", _results(expired=expired, workspace_gone=workspace_gone))
    assert {k: report["core"][k] for k in LOSS_CHECKS} == dict.fromkeys(LOSS_CHECKS, "supported")


def test_toolset_reports_loss() -> None:
    be = _Backend()
    sb = ToolsetSandbox(be, workdir="/workspace/target")
    sb.ensure()
    assert sb.alive()
    be.workdir = False
    workspace_gone = not sb.alive()
    be.workdir, be.present = True, False
    expired = not sb.alive()
    report = capability_report("toolset:fake", _results(expired=expired, workspace_gone=workspace_gone))
    assert {k: report["core"][k] for k in LOSS_CHECKS} == dict.fromkeys(LOSS_CHECKS, "supported")


def test_every_ttl_claim_has_an_adapter_probe() -> None:
    """A document may claim ``ttl=True`` only for an implementation whose ``alive`` is exercised
    above; the claim is otherwise a label, which the contract forbids."""
    probed = {"IsloSandbox", "ToolsetSandbox"}
    for doc in provider_documents():
        if doc.capabilities.ttl:
            assert doc.implementation in probed, doc.provider


def test_compute_lost_is_one_named_reason_for_every_adapter() -> None:
    lost = compute_lost("islo", "swf-x", detail="not listed by `islo ls`")
    assert lost.reason == "infrastructure_lost" and not lost.ok
    assert lost.summary().startswith("infrastructure_lost: islo sandbox 'swf-x'")


@pytest.mark.parametrize("name", LOSS_CHECKS)
def test_an_unprobed_loss_check_blocks_core_readiness(name: str) -> None:
    results = [CheckResult(check, "supported") for check in CORE_CHECKS if check != name]
    assert capability_report("x", results)["core_ready"] is False
