"""Replay capsules execute only verified snapshot bytes and retain digest-bound outputs."""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from swfactory.cli import app
from swfactory.snapshot_replay import (
    SnapshotReplayError,
    SnapshotRunRecipe,
    run_snapshot_recipe,
    verify_snapshot_run,
)
from swfactory.source_snapshot import SourceSnapshot, create_source_snapshot


def _git(repo: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def _repo(tmp_path: Path) -> tuple[Path, SourceSnapshot]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.name", "Factory Test")
    _git(repo, "config", "user.email", "factory@example.test")
    (repo / "value.txt").write_text("committed\n", encoding="utf-8")
    _git(repo, "add", "value.txt")
    _git(repo, "commit", "-q", "-m", "base")
    snapshot = create_source_snapshot(repo, "HEAD", tmp_path / "snapshots")
    return repo, snapshot


def test_replay_executes_snapshot_bytes_not_dirty_checkout(tmp_path: Path) -> None:
    repo, snapshot = _repo(tmp_path)
    (repo / "value.txt").write_text("dirty\n", encoding="utf-8")
    recipe = SnapshotRunRecipe(
        argv=(
            sys.executable,
            "-c",
            "from pathlib import Path; print(Path('value.txt').read_text().strip())",
        ),
    )

    receipt = run_snapshot_recipe(snapshot, recipe, tmp_path / "run")

    assert receipt.exit_code == 0
    assert receipt.timed_out is False
    assert (tmp_path / "run" / "stdout.bin").read_text() == "committed\n"
    loaded_recipe, loaded_receipt = verify_snapshot_run(tmp_path / "run", snapshot=snapshot)
    assert loaded_recipe.digest == recipe.digest
    assert loaded_receipt.digest == receipt.digest


def test_recipe_environment_is_explicit_and_retained(tmp_path: Path) -> None:
    _, snapshot = _repo(tmp_path)
    recipe = SnapshotRunRecipe(
        argv=(sys.executable, "-c", "import os; print(os.environ.get('TOKEN', 'missing'))"),
        env=(("TOKEN", "recorded-value"),),
    )

    run_snapshot_recipe(snapshot, recipe, tmp_path / "run")

    assert (tmp_path / "run" / "stdout.bin").read_text() == "recorded-value\n"
    document = json.loads((tmp_path / "run" / "recipe.json").read_text())
    assert document["env"] == [["TOKEN", "recorded-value"]]


def test_replay_retains_nonzero_exit_and_stderr(tmp_path: Path) -> None:
    _, snapshot = _repo(tmp_path)
    recipe = SnapshotRunRecipe(
        argv=(sys.executable, "-c", "import sys; print('bad', file=sys.stderr); raise SystemExit(7)"),
    )

    receipt = run_snapshot_recipe(snapshot, recipe, tmp_path / "run")

    assert receipt.exit_code == 7
    assert receipt.timed_out is False
    assert (tmp_path / "run" / "stderr.bin").read_text() == "bad\n"


def test_timeout_is_recorded_without_forging_exit_code(tmp_path: Path) -> None:
    _, snapshot = _repo(tmp_path)
    recipe = SnapshotRunRecipe(
        argv=(sys.executable, "-c", "import time; time.sleep(2)"),
        timeout_s=0.05,
    )

    receipt = run_snapshot_recipe(snapshot, recipe, tmp_path / "run")

    assert receipt.timed_out is True
    assert receipt.exit_code is None
    verify_snapshot_run(tmp_path / "run", snapshot=snapshot)


def test_tampered_output_fails_verification(tmp_path: Path) -> None:
    _, snapshot = _repo(tmp_path)
    recipe = SnapshotRunRecipe(argv=(sys.executable, "-c", "print('ok')"))
    run_snapshot_recipe(snapshot, recipe, tmp_path / "run")
    (tmp_path / "run" / "stdout.bin").write_bytes(b"tampered")

    with pytest.raises(SnapshotReplayError, match="stream changed"):
        verify_snapshot_run(tmp_path / "run", snapshot=snapshot)


def test_receipt_cannot_be_verified_against_different_snapshot(tmp_path: Path) -> None:
    repo, snapshot = _repo(tmp_path)
    recipe = SnapshotRunRecipe(argv=(sys.executable, "-c", "print('ok')"))
    run_snapshot_recipe(snapshot, recipe, tmp_path / "run")
    (repo / "value.txt").write_text("second\n", encoding="utf-8")
    _git(repo, "add", "value.txt")
    _git(repo, "commit", "-q", "-m", "second")
    other = create_source_snapshot(repo, "HEAD", tmp_path / "snapshots")

    with pytest.raises(SnapshotReplayError, match="different source snapshot"):
        verify_snapshot_run(tmp_path / "run", snapshot=other)


def test_archive_links_are_refused_before_execution(tmp_path: Path) -> None:
    archive = tmp_path / "unsafe.tar"
    with tarfile.open(archive, "w") as tar:
        info = tarfile.TarInfo("escape")
        info.type = tarfile.SYMTYPE
        info.linkname = "/etc/passwd"
        tar.addfile(info)
    payload = archive.read_bytes()
    snapshot = SourceSnapshot(
        commit_sha="a" * 40,
        sha256=hashlib.sha256(payload).hexdigest(),
        size_bytes=len(payload),
        archive_path=str(archive),
        cache_hit=False,
    )
    recipe = SnapshotRunRecipe(argv=(sys.executable, "-c", "print('never')"))

    with pytest.raises(SnapshotReplayError, match="non-regular archive member"):
        run_snapshot_recipe(snapshot, recipe, tmp_path / "run")


def test_archive_path_traversal_is_refused(tmp_path: Path) -> None:
    archive = tmp_path / "unsafe.tar"
    with tarfile.open(archive, "w") as tar:
        data = b"escape"
        info = tarfile.TarInfo("../escape.txt")
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    payload = archive.read_bytes()
    snapshot = SourceSnapshot(
        commit_sha="a" * 40,
        sha256=hashlib.sha256(payload).hexdigest(),
        size_bytes=len(payload),
        archive_path=str(archive),
        cache_hit=False,
    )
    recipe = SnapshotRunRecipe(argv=(sys.executable, "-c", "print('never')"))

    with pytest.raises(SnapshotReplayError, match="unsafe archive member path"):
        run_snapshot_recipe(snapshot, recipe, tmp_path / "run")


def test_recipe_refuses_escaping_cwd_and_duplicate_env() -> None:
    with pytest.raises(SnapshotReplayError, match="cwd"):
        SnapshotRunRecipe(argv=("/bin/true",), cwd="../outside").validate()
    with pytest.raises(SnapshotReplayError, match="duplicate environment key"):
        SnapshotRunRecipe(argv=("/bin/true",), env=(("A", "1"), ("A", "2"))).validate()



def test_recipe_refuses_secret_like_environment_keys() -> None:
    with pytest.raises(SnapshotReplayError, match="secret-like"):
        SnapshotRunRecipe(
            argv=("/bin/true",),
            env=(("SERVICE_API_KEY", "do-not-retain"),),
        ).validate()


def test_cli_runs_and_verifies_replay_capsule(tmp_path: Path) -> None:
    _, snapshot = _repo(tmp_path)
    source_receipt = tmp_path / "source.json"
    source_receipt.write_text(json.dumps(snapshot.to_dict()) + "\n", encoding="utf-8")
    recipe_path = tmp_path / "recipe.json"
    recipe_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "argv": [sys.executable, "-c", "print('cli-replay')"],
                "cwd": ".",
                "env": [],
                "timeout_s": 30,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    destination = tmp_path / "run"

    runner = CliRunner()
    run = runner.invoke(
        app,
        ["snapshot-replay", "run", str(source_receipt), str(recipe_path), str(destination)],
    )
    assert run.exit_code == 0, run.output
    assert "timed_out=false" in run.output
    assert (destination / "stdout.bin").read_text() == "cli-replay\n"

    verify = runner.invoke(
        app,
        [
            "snapshot-replay",
            "verify",
            str(destination),
            "--source-receipt",
            str(source_receipt),
            "--json",
        ],
    )
    assert verify.exit_code == 0, verify.output
    document = json.loads(verify.stdout)
    assert document["commit_sha"] == snapshot.commit_sha
    assert document["receipt_digest"].startswith("sha256:")
