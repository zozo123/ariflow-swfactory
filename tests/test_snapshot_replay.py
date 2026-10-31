"""Committed source + committed recipe replay contracts."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from support import git, make_repo
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

RECIPE = {
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
    "secret_env": [],
}


def _repo(tmp_path: Path):
    recipe_json = json.dumps(RECIPE, indent=2, sort_keys=True) + "\n"
    repo, commit = make_repo(tmp_path, {"value.txt": "committed\n", ".swfactory/candidate-run.json": recipe_json})
    snapshot = create_source_snapshot(repo, commit, tmp_path / "snapshots")
    recipe = load_execution_recipe(repo, commit)
    return repo, snapshot, recipe


def test_replay_uses_committed_source_and_recipe_not_dirty_checkout(tmp_path: Path) -> None:
    repo, snapshot, recipe = _repo(tmp_path)
    (repo / "value.txt").write_text("dirty source\n", encoding="utf-8")
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
    git(repo, "add", "value.txt")
    git(repo, "commit", "-q", "-m", "second")
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


@pytest.mark.parametrize("stream_name", ["stdout", "stderr"])
@pytest.mark.parametrize("redirected", [False, True])
def test_retained_stream_parent_must_not_redirect_outside_bundle(
    tmp_path: Path, stream_name: str, redirected: bool
) -> None:
    from dataclasses import asdict, replace

    repo, snapshot, recipe = _repo(tmp_path)
    destination = tmp_path / "run"
    receipt = run_snapshot_recipe(snapshot, recipe, destination)
    stream = getattr(receipt, stream_name)
    logs = destination / "logs"
    if redirected:
        outside = tmp_path / "outside"
        outside.mkdir()
        logs.symlink_to(outside, target_is_directory=True)
    else:
        logs.mkdir()
    (destination / stream.path).rename(logs / stream.path)
    receipt = replace(receipt, **{stream_name: replace(stream, path=f"logs/{stream.path}")})
    document = {**asdict(receipt), "receipt_digest": receipt.digest}
    (destination / "receipt.json").write_text(json.dumps(document), encoding="utf-8")

    if redirected:
        with pytest.raises(SnapshotReplayError, match="redirected"):
            verify_snapshot_run(destination, snapshot=snapshot, repo=repo)
    else:
        _, observed = verify_snapshot_run(destination, snapshot=snapshot, repo=repo)
        assert observed == receipt


def _rewrite_stream_path(destination: Path, receipt, stream_name: str, path: str):
    from dataclasses import asdict, replace

    stream = getattr(receipt, stream_name)
    receipt = replace(receipt, **{stream_name: replace(stream, path=path)})
    document = {**asdict(receipt), "receipt_digest": receipt.digest}
    (destination / "receipt.json").write_text(json.dumps(document), encoding="utf-8")
    return receipt


@pytest.mark.parametrize("stream_name", ["stdout", "stderr"])
@pytest.mark.parametrize("redirected", [False, True])
def test_retained_stream_ancestor_must_not_redirect_outside_bundle(
    tmp_path: Path, stream_name: str, redirected: bool
) -> None:
    repo, snapshot, recipe = _repo(tmp_path)
    destination = tmp_path / "run"
    receipt = run_snapshot_recipe(snapshot, recipe, destination)
    stream = getattr(receipt, stream_name)
    ancestor = destination / "logs"
    if redirected:
        outside = tmp_path / "outside"
        (outside / "nested").mkdir(parents=True)
        ancestor.symlink_to(outside, target_is_directory=True)
    else:
        (ancestor / "nested").mkdir(parents=True)
    (destination / stream.path).rename(ancestor / "nested" / stream.path)
    receipt = _rewrite_stream_path(destination, receipt, stream_name, f"logs/nested/{stream.path}")

    if redirected:
        with pytest.raises(SnapshotReplayError, match="redirected"):
            verify_snapshot_run(destination, snapshot=snapshot, repo=repo)
    else:
        _, observed = verify_snapshot_run(destination, snapshot=snapshot, repo=repo)
        assert observed == receipt


@pytest.mark.parametrize("stream_name", ["stdout", "stderr"])
def test_retained_stream_leaf_must_not_be_a_symlink(tmp_path: Path, stream_name: str) -> None:
    repo, snapshot, recipe = _repo(tmp_path)
    destination = tmp_path / "run"
    receipt = run_snapshot_recipe(snapshot, recipe, destination)
    stream = getattr(receipt, stream_name)
    outside = tmp_path / "outside.bin"
    (destination / stream.path).rename(outside)
    (destination / stream.path).symlink_to(outside)

    with pytest.raises(SnapshotReplayError, match="redirected"):
        verify_snapshot_run(destination, snapshot=snapshot, repo=repo)


@pytest.mark.parametrize("stream_name", ["stdout", "stderr"])
@pytest.mark.parametrize("unsafe", ["../outside.bin", "logs/../../outside.bin", "/tmp/outside.bin"])
def test_retained_stream_path_must_stay_inside_bundle(tmp_path: Path, stream_name: str, unsafe: str) -> None:
    from dataclasses import asdict

    repo, snapshot, recipe = _repo(tmp_path)
    destination = tmp_path / "run"
    receipt = run_snapshot_recipe(snapshot, recipe, destination)
    stream = getattr(receipt, stream_name)
    (tmp_path / "outside.bin").write_bytes((destination / stream.path).read_bytes())
    document = asdict(receipt)
    document[stream_name]["path"] = unsafe
    document["receipt_digest"] = "0" * 64
    (destination / "receipt.json").write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(SnapshotReplayError, match="unsafe retained stream path"):
        verify_snapshot_run(destination, snapshot=snapshot, repo=repo)
