"""Optional Compose wiring, worker-only provider keys and deployment input refusal."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "deploy" / "docker"
LAUNCHER = DEPLOY / "compose-external-agent.sh"
PROVIDER_KEYS = {
    "ANTHROPIC_API_KEY",
    "OPENAI_API_KEY",
    "PRIME_API_KEY",
    "GOOGLE_API_KEY",
    "GEMINI_API_KEY",
    "MISTRAL_API_KEY",
    "COHERE_API_KEY",
    "GROQ_API_KEY",
    "TOGETHER_API_KEY",
    "FIREWORKS_API_KEY",
    "OPENROUTER_API_KEY",
    "DEEPSEEK_API_KEY",
    "XAI_API_KEY",
}


def _env(profile: Path) -> dict[str, str]:
    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(("SWF_", "COMPOSE_")) and name not in PROVIDER_KEYS | {"GH_TOKEN", "GITHUB_TOKEN"}
    }
    return {
        **env,
        "PWD": str(ROOT),
        "SWF_EXTERNAL_PROFILE_DIR": str(profile),
        "SWF_EXTERNAL_IMAGE": "registry.example/harness@sha256:" + "a" * 64,
        "OPENAI_API_KEY": "provider-test-sentinel",
        "PRIME_API_KEY": "prime-test-sentinel",
        "SWF_BACKEND_TOKEN": "backend-test-sentinel" * 2,
    }


def _operator_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "operator"
    directory.mkdir()
    (directory / "profile.json").write_text("{}\n", encoding="utf-8")
    return directory


def _launcher(env: dict[str, str], *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(LAUNCHER), *args], cwd=ROOT, env=env, capture_output=True, text=True, check=False, timeout=30
    )


@pytest.fixture(scope="module")
def compose_available() -> None:
    if not shutil.which("docker"):
        pytest.skip("Docker CLI is needed for actual Compose rendering")
    result = subprocess.run(["docker", "compose", "version"], capture_output=True, check=False, timeout=15)
    if result.returncode != 0:
        pytest.skip("Docker Compose is unavailable")


def test_overlay_only_adds_explicit_provider_keys_to_airflow() -> None:
    overlay = yaml.safe_load((DEPLOY / "compose.external-agent.yml").read_text(encoding="utf-8"))
    assert set(overlay["services"]) == {"airflow"}
    airflow = overlay["services"]["airflow"]
    env = airflow["environment"]
    assert set(env) == PROVIDER_KEYS | {
        "SWF_AGENT",
        "SWF_SANDBOX",
        "SWF_AGENT_PROFILE",
        "SWF_DOCKER_IMAGE",
        "SWF_DOCKER_CREDENTIALS",
    }
    assert env["SWF_AGENT"] == "external" and env["SWF_DOCKER_CREDENTIALS"] == "env"
    mount = airflow["volumes"][0]
    assert mount["target"] == "/opt/swfactory-external-profiles"
    assert mount["read_only"] and not mount["bind"]["create_host_path"]


def test_actual_compose_merge_preserves_native_stack_and_isolates_keys(tmp_path: Path, compose_available) -> None:
    profile = _operator_dir(tmp_path)
    env = _env(profile)
    base_result = subprocess.run(
        ["docker", "compose", "-f", str(DEPLOY / "compose.yml"), "config", "--format", "json"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert base_result.returncode == 0, base_result.stderr
    base = json.loads(base_result.stdout)
    assert base["services"]["airflow"]["environment"]["SWF_AGENT"] == "scripted"
    assert "SWF_AGENT_PROFILE" not in base["services"]["airflow"]["environment"]
    result = _launcher(env, "config", "--format", "json")
    assert result.returncode == 0, result.stderr
    merged = json.loads(result.stdout)
    assert set(merged["services"]) == set(base["services"])
    for service in set(base["services"]) - {"airflow"}:
        assert merged["services"][service] == base["services"][service]
        assert not PROVIDER_KEYS.intersection(merged["services"][service].get("environment", {}))
    airflow = merged["services"]["airflow"]
    worker_env = airflow["environment"]
    assert worker_env["SWF_AGENT_PROFILE"] == "/opt/swfactory-external-profiles/profile.json"
    assert worker_env["SWF_DOCKER_IMAGE"] == env["SWF_EXTERNAL_IMAGE"]
    assert worker_env["OPENAI_API_KEY"] == env["OPENAI_API_KEY"]
    assert worker_env["PRIME_API_KEY"] == env["PRIME_API_KEY"]
    assert worker_env["SWF_BACKEND_URL"] == "http://backend:8082"
    mount = next(volume for volume in airflow["volumes"] if volume["target"] == "/opt/swfactory-external-profiles")
    assert mount["source"] == str(profile.resolve()) and mount["read_only"]
    # Compose omits this false value in its normalized JSON; true is rendered explicitly.
    assert not mount.get("bind", {}).get("create_host_path", False)
    native_mounts = {volume["target"]: volume for volume in base["services"]["airflow"]["volumes"]}
    merged_mounts = {volume["target"]: volume for volume in airflow["volumes"]}
    assert all(merged_mounts[target] == volume for target, volume in native_mounts.items())


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("SWF_EXTERNAL_PROFILE_DIR", "", "absolute directory"),
        ("SWF_EXTERNAL_PROFILE_DIR", "relative", "absolute directory"),
        ("SWF_EXTERNAL_PROFILE_DIR", "/missing/operator/profile", "must already exist"),
        ("SWF_EXTERNAL_IMAGE", "", "must use image@sha256"),
        ("SWF_EXTERNAL_IMAGE", "image:latest", "must use image@sha256"),
        ("SWF_EXTERNAL_IMAGE", "image@sha256:abc", "must use image@sha256"),
    ],
)
def test_launcher_refuses_invalid_inputs_before_docker(tmp_path: Path, field: str, value: str, reason: str) -> None:
    env = _env(_operator_dir(tmp_path))
    env[field] = value
    env["PATH"] = "/usr/bin:/bin"  # no dependency on a Docker daemon or the operator's PATH
    result = _launcher(env, "config", "--quiet")
    assert result.returncode == 2 and reason in result.stderr
    assert "provider-test-sentinel" not in result.stdout + result.stderr


def test_launcher_refuses_missing_profile_and_symlink(tmp_path: Path) -> None:
    directory = _operator_dir(tmp_path)
    profile = directory / "profile.json"
    profile.unlink()
    env = _env(directory)
    result = _launcher(env, "config", "--quiet")
    assert result.returncode == 2 and "readable profile.json" in result.stderr
    other = tmp_path / "unowned-profile.json"
    other.write_text("{}\n")
    profile.symlink_to(other)
    result = _launcher(env, "config", "--quiet")
    assert result.returncode == 2 and "not a symlink" in result.stderr


def test_launcher_refuses_candidate_directory_and_requires_operation(tmp_path: Path) -> None:
    env = _env(_operator_dir(tmp_path))
    result = _launcher(env)
    assert result.returncode == 2 and "pass a Compose operation" in result.stderr
    # Rehearse a candidate below a separate factory checkout, without changing the real repo.
    repository = tmp_path / "factory"
    deployment = repository / "deploy" / "docker"
    deployment.mkdir(parents=True)
    launcher = deployment / LAUNCHER.name
    shutil.copyfile(LAUNCHER, launcher)
    candidate = repository / ".factory" / "run" / "work" / "operator"
    candidate.mkdir(parents=True)
    env["SWF_EXTERNAL_PROFILE_DIR"] = str(candidate)
    result = subprocess.run(
        ["bash", str(launcher), "config", "--quiet"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 2 and "outside candidate workdirs" in result.stderr
