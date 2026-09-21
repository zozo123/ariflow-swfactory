"""Commit-bound candidate execution recipe contracts."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from swfactory.evidence_release import CandidateManifest, bind_execution_recipe
from swfactory.execution_recipe import ExecutionRecipeError, load_execution_recipe


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def _repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.name", "Recipe Test")
    _git(repo, "config", "user.email", "recipe@example.test")
    path = repo / ".swfactory"
    path.mkdir()
    (path / "candidate-run.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "argv": ["uv", "run", "pytest", "-q"],
                "cwd": ".",
                "timeout_s": 1800,
                "resources": {"cpus": 4, "memory_mb": 8192},
                "environment": {"PYTHONHASHSEED": "0"},
                "secret_env": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "recipe")
    return repo, _git(repo, "rev-parse", "HEAD")


def test_recipe_is_read_from_recorded_commit_not_dirty_checkout(tmp_path: Path) -> None:
    repo, commit = _repo(tmp_path)
    recipe_path = repo / ".swfactory" / "candidate-run.json"
    recipe_path.write_text('{"schema_version":1,"argv":["evil"]}\n', encoding="utf-8")

    bound = load_execution_recipe(repo, commit)

    assert bound.commit_sha == commit
    assert bound.recipe.argv == ("uv", "run", "pytest", "-q")
    assert bound.recipe.cpus == 4
    assert bound.recipe.memory_mb == 8192
    assert bound.recipe.environment == (("PYTHONHASHSEED", "0"),)
    assert bound.recipe.secret_env == ()
    assert len(bound.digest) == 64


def test_recipe_digest_changes_when_committed_execution_contract_changes(tmp_path: Path) -> None:
    repo, first_commit = _repo(tmp_path)
    first = load_execution_recipe(repo, first_commit)
    path = repo / ".swfactory" / "candidate-run.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["resources"]["cpus"] = 8
    path.write_text(json.dumps(document) + "\n", encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "more cpu")
    second_commit = _git(repo, "rev-parse", "HEAD")
    second = load_execution_recipe(repo, second_commit)

    assert second.digest != first.digest
    assert second.commit_sha != first.commit_sha


def test_secret_values_are_refused_from_committed_public_environment(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    path = repo / ".swfactory" / "candidate-run.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["environment"]["API_KEY"] = "do-not-commit-this"
    path.write_text(json.dumps(document) + "\n", encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "bad secret")

    with pytest.raises(ExecutionRecipeError, match="looks secret"):
        load_execution_recipe(repo, "HEAD")


@pytest.mark.parametrize("cwd", ["/tmp", "../outside", "safe/../../outside"])
def test_recipe_cwd_cannot_escape_repository(tmp_path: Path, cwd: str) -> None:
    repo, _ = _repo(tmp_path)
    path = repo / ".swfactory" / "candidate-run.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["cwd"] = cwd
    path.write_text(json.dumps(document) + "\n", encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "bad cwd")

    with pytest.raises(ExecutionRecipeError, match="escapes repository"):
        load_execution_recipe(repo, "HEAD")


def test_recipe_rejects_non_string_cwd(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    recipe_path = repo / ".swfactory" / "candidate-run.json"
    document = json.loads(recipe_path.read_text(encoding="utf-8"))
    document["cwd"] = 123
    recipe_path.write_text(json.dumps(document), encoding="utf-8")
    _git(repo, "add", ".swfactory/candidate-run.json")
    _git(repo, "commit", "-q", "-m", "bad cwd type")

    with pytest.raises(ExecutionRecipeError, match="cwd must be a string"):
        load_execution_recipe(repo, "HEAD")


def test_recipe_rejects_unknown_fields_instead_of_silently_ignoring_policy(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    path = repo / ".swfactory" / "candidate-run.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["privileged"] = True
    path.write_text(json.dumps(document) + "\n", encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "unknown policy")

    with pytest.raises(ExecutionRecipeError, match="unknown execution recipe fields"):
        load_execution_recipe(repo, "HEAD")


def test_candidate_manifest_digest_binds_recipe_and_exact_source_commit(tmp_path: Path) -> None:
    repo, commit = _repo(tmp_path)
    recipe = load_execution_recipe(repo, commit)
    manifest = CandidateManifest(commit, "base", (), {})
    bound = bind_execution_recipe(manifest, recipe)

    assert bound.execution_recipe_sha256 == recipe.digest
    assert bound.execution_recipe_commit_sha == commit
    assert bound.execution_recipe_path == ".swfactory/candidate-run.json"
    bound.validate(set())
    assert bound.digest != manifest.digest

    other = CandidateManifest("f" * 40, "base", (), {})
    with pytest.raises(RuntimeError, match="recipe commit"):
        bind_execution_recipe(other, recipe)


def test_option_like_revision_and_escaping_recipe_path_are_refused(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    with pytest.raises(ExecutionRecipeError, match="invalid Git revision"):
        load_execution_recipe(repo, "--help")
    with pytest.raises(ExecutionRecipeError, match="escapes repository"):
        load_execution_recipe(repo, "HEAD", path="../candidate-run.json")


def test_public_recipe_rejects_secret_env_injection(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    path = repo / ".swfactory" / "candidate-run.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["secret_env"] = ["GH_TOKEN"]
    path.write_text(json.dumps(document) + "\n", encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "forbidden secret injection")

    with pytest.raises(ExecutionRecipeError, match="secret_env is retired"):
        load_execution_recipe(repo, "HEAD")
