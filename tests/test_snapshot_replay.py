"""Committed source + committed recipe replay contracts."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from typer.testing import CliRunner

from swfactory.cli import app
from swfactory.execution_recipe import (
    BoundExecutionRecipe,
    ExecutionRecipe,
    load_execution_recipe,
)
from swfactory.snapshot_replay import (
    SnapshotReplayError,
    run_snapshot_recipe,
    verify_snapshot_run,
)
from swfactory.source_snapshot import create_source_snapshot


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def _write_recipe(repo: Path, *, secret_env: list[str] | None = None) -> None:
    recipe_dir = repo / ".swfactory"
    recipe_dir.mkdir(exist_ok=True)
    document = {
        "schema_version": 1,
        "argv": [
            sys.executable,
            "-c",
            "from pathlib import Path; print(Path('value.txt').read_text().strip())",
        ],
        "cwd": ".",
        "timeout_s": 30,
        "resources": {"cpus": 1, "memory_mb": 128},
        "environment": {"PYTHONHASHSEED": "0"},
        "secret_env": secret_env or [],
    }
    (recipe_dir / "candidate-run.json").write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _repo(tmp_path: Path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.name", "Factory Test")
    _git(repo, "config", "user.email", "factory@example.test")
    (repo / "value.txt").write_text("committed\n", encoding="utf-8")
    _write_recipe(repo)
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "base")
    commit = _git(repo, "rev-parse", "HEAD")
    snapshot = create_source_snapshot(repo, commit, tmp_path / "snapshots")
    recipe = load_execution_recipe(repo, commit)
    return repo, snapshot, recipe


def test_replay_uses_committed_source_and_recipe_not_dirty_checkout(tmp_path: Path) -> None:
    repo, snapshot, recipe = _repo(tmp_path)
    (repo / "value.txt").write_text("dirty source\n", encoding="utf-8")
    _write_recipe(repo)
    document = json.loads((repo / ".swfactory" / "candidate-run.json").read_text())
    document["argv"][-1] = "print('dirty recipe')"
    (repo / ".swfactory" / "candidate-run.json").write_text(json.dumps(document), encoding="utf-8")

    receipt = run_snapshot_recipe(snapshot, recipe, tmp_path / "run")

    assert receipt.exit_code == 0
    assert receipt.timed_out is False
    assert (tmp_path / "run" / "stdout.bin").read_text() == "committed\n"
    assert receipt.execution_recipe_commit_sha == snapshot.commit_sha
    assert receipt.execution_recipe_sha256 == recipe.digest
    assert receipt.resource_enforcement == "declared-not-enforced-local"
    verify_snapshot_run(tmp_path / "run", snapshot=snapshot, repo=repo)


def test_snapshot_and_recipe_must_name_same_commit(tmp_path: Path) -> None:
    repo, snapshot, recipe = _repo(tmp_path)
    (repo / "value.txt").write_text("second\n", encoding="utf-8")
    _git(repo, "add", "value.txt")
    _git(repo, "commit", "-q", "-m", "second")
    second = create_source_snapshot(repo, "HEAD", tmp_path / "snapshots")

    with pytest.raises(SnapshotReplayError, match="execution recipe commit"):
        run_snapshot_recipe(second, recipe, tmp_path / "run")

    assert snapshot.commit_sha != second.commit_sha


def test_local_replay_refuses_secret_injection(tmp_path: Path) -> None:
    _, snapshot, recipe = _repo(tmp_path)
    secret_recipe = ExecutionRecipe(
        argv=recipe.recipe.argv,
        cwd=recipe.recipe.cwd,
        timeout_s=recipe.recipe.timeout_s,
        cpus=recipe.recipe.cpus,
        memory_mb=recipe.recipe.memory_mb,
        environment=recipe.recipe.environment,
        secret_env=("GH_TOKEN",),
    )
    bound = BoundExecutionRecipe(
        commit_sha=snapshot.commit_sha,
        path=recipe.path,
        recipe=secret_recipe,
    )

    with pytest.raises(SnapshotReplayError, match="does not inject secret_env"):
        run_snapshot_recipe(snapshot, bound, tmp_path / "run")


def test_tampered_output_fails_verification(tmp_path: Path) -> None:
    repo, snapshot, recipe = _repo(tmp_path)
    run_snapshot_recipe(snapshot, recipe, tmp_path / "run")
    (tmp_path / "run" / "stdout.bin").write_bytes(b"tampered")

    with pytest.raises(SnapshotReplayError, match="stream changed"):
        verify_snapshot_run(tmp_path / "run", snapshot=snapshot, repo=repo)


def test_tampered_recipe_manifest_fails_verification(tmp_path: Path) -> None:
    repo, snapshot, recipe = _repo(tmp_path)
    run_snapshot_recipe(snapshot, recipe, tmp_path / "run")
    path = tmp_path / "run" / "recipe.json"
    document = json.loads(path.read_text())
    document["recipe"]["timeout_s"] = 31
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(SnapshotReplayError, match="recipe digest mismatch"):
        verify_snapshot_run(tmp_path / "run", snapshot=snapshot, repo=repo)


def test_cli_loads_recipe_from_source_commit_and_verifies(tmp_path: Path) -> None:
    repo, snapshot, _ = _repo(tmp_path)
    source_receipt = tmp_path / "source.json"
    source_receipt.write_text(json.dumps(snapshot.to_dict()) + "\n", encoding="utf-8")
    destination = tmp_path / "run"
    runner = CliRunner()

    run = runner.invoke(
        app,
        ["snapshot-replay", str(repo), str(source_receipt), str(destination)],
    )
    assert run.exit_code == 0, run.output
    assert "timed_out=false" in run.output
    assert (destination / "stdout.bin").read_text() == "committed\n"

    verify = runner.invoke(
        app,
        [
            "snapshot-replay-verify",
            str(repo),
            str(source_receipt),
            str(destination),
            "--json",
        ],
    )
    assert verify.exit_code == 0, verify.output
    document = json.loads(verify.stdout)
    assert document["commit_sha"] == snapshot.commit_sha
    assert document["receipt_digest"]
