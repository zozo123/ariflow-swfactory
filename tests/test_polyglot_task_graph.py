"""Contracts for the native Cargo + uv task graph and its authority boundary."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _turbo() -> dict[str, object]:
    """Load the repository's Turbo task-graph configuration."""
    return json.loads((ROOT / "turbo.json").read_text(encoding="utf-8"))


def test_turbo_is_an_accelerator_not_an_authority_layer() -> None:
    """Keep Turbo local, uncached, and limited to verification work."""
    turbo = _turbo()

    assert turbo["remoteCache"] == {"enabled": False}

    flags = turbo["futureFlags"]
    assert flags["experimentalPythonWorkspaces"] is True
    assert flags["experimentalCargoWorkspaces"] is True
    assert flags["experimentalTaskCommand"] is True
    assert flags["affectedUsingTaskInputs"] is True

    tasks = turbo["tasks"]
    assert not any(name.startswith("//#rust-") for name in tasks)
    assert tasks["//#polyglot-verification"]["cache"] is False
    assert set(tasks["//#polyglot-verification"]["dependsOn"]) == {
        "swfactory-python#test",
        "swfactory-rust#test",
        "swf-cli#build",
    }


def test_root_cargo_workspace_is_native_and_single() -> None:
    """Require one native Cargo workspace rooted at the repository."""
    root = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))

    assert not (ROOT / "rust" / "Cargo.toml").exists()
    assert root["workspace"]["members"] == ["rust/crates/*"]
    assert root["workspace"]["metadata"]["name"] == "swfactory-rust"
    assert (ROOT / "Cargo.lock").is_file()
    assert (ROOT / "rust-toolchain.toml").is_file()
    assert (ROOT / "rustfmt.toml").is_file()
    for name, spec in root["workspace"]["dependencies"].items():
        if isinstance(spec, dict) and "path" in spec:
            assert spec["path"].startswith("rust/crates/"), (name, spec)


def test_uv_workspace_has_a_real_contract_member_and_stable_aggregate_identity() -> None:
    """Require native uv members and stable Turbo package identities."""
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    member = tomllib.loads((ROOT / "tests/fixtures/contract/pyproject.toml").read_text(encoding="utf-8"))
    lock = tomllib.loads((ROOT / "uv.lock").read_text(encoding="utf-8"))

    assert project["tool"]["turbo"]["name"] == "swfactory-python"
    assert project["tool"]["uv"]["workspace"]["members"] == ["tests/fixtures/contract"]
    assert member["project"]["name"] == "swfactory-contract-fixtures"
    assert set(lock["manifest"]["members"]) == {"swfactory", "swfactory-contract-fixtures"}


def test_polyglot_job_is_advisory_and_never_part_of_candidate_readiness() -> None:
    """Keep the polyglot CI job advisory and outside promotion readiness."""
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    candidate = workflow.split("  candidate-readiness:", 1)[1]

    assert "  polyglot-task-graph:" in workflow
    polyglot = workflow.split("  polyglot-task-graph:", 1)[1].split("  candidate-readiness:", 1)[0]
    assert "continue-on-error: true" in polyglot
    assert "polyglot-task-graph" not in candidate.split("steps:", 1)[0]


def test_turbo_does_not_enter_airflow_or_promotion_authority() -> None:
    """Prevent Turbo from entering lifecycle or promotion authority paths."""
    authority_paths = [
        ROOT / "src/swfactory/candidate_readiness.py",
        ROOT / "scripts/promotion_policy.py",
        ROOT / ".github/promotion-policy.yml",
        *sorted((ROOT / "dags").glob("*.py")),
    ]
    for path in authority_paths:
        assert "turbo" not in path.read_text(encoding="utf-8").lower(), path


def test_turbo_cache_environment_cannot_change_candidate_evidence(monkeypatch, tmp_path: Path) -> None:
    """Prove Turbo cache settings cannot alter candidate evidence."""
    from swfactory.candidate_readiness import build_manifest

    artifact = tmp_path / "required.txt"
    artifact.write_text("same exact evidence\n", encoding="utf-8")
    kwargs = {
        "head_sha": "a" * 40,
        "base_sha": "b" * 40,
        "tested_sha": "c" * 40,
        "required": [("test", artifact)],
    }

    monkeypatch.setenv("TURBO_CACHE_DIR", str(tmp_path / "cache-a"))
    monkeypatch.setenv("TURBO_REMOTE_ONLY", "false")
    first = build_manifest(**kwargs)

    monkeypatch.setenv("TURBO_CACHE_DIR", str(tmp_path / "cache-b"))
    monkeypatch.setenv("TURBO_REMOTE_ONLY", "true")
    second = build_manifest(**kwargs)

    assert first.canonical_dict() == second.canonical_dict()
    assert first.digest() == second.digest()


def test_workspace_graph_inputs_are_protected_even_when_tests_are_writable() -> None:
    """Protect graph inputs without changing stage-level test permissions."""
    from swfactory.config import TargetContract, protected_for

    contract = TargetContract.parse((ROOT / "factory.toml").read_text(encoding="utf-8"))
    always_protected = {
        "Cargo.toml",
        "Cargo.lock",
        "rust-toolchain.toml",
        "rustfmt.toml",
        "turbo.json",
        "tests/fixtures/contract/pyproject.toml",
    }
    for stage in ("build", "fix"):
        protected = set(protected_for(contract, stage))
        assert always_protected <= protected
    assert "tests" not in set(protected_for(contract, "build"))
    assert "tests" in set(protected_for(contract, "fix"))
