"""Gateway routing and credential projection must agree across preflight and execution."""

import json

import pytest

from swfactory import sandbox
from swfactory.config import Config
from swfactory.doctor import _check_model_credential
from swfactory.models import RunResult, StageError

GATEWAY = "https://workspace.cloud.databricks.com/serving-endpoints/anthropic"


@pytest.fixture
def gateway(monkeypatch):
    for key, value in {
        "ANTHROPIC_BASE_URL": GATEWAY,
        "ANTHROPIC_AUTH_TOKEN": "gateway-secret",
        "ANTHROPIC_API_KEY": "unrelated-anthropic-key",
        "ANTHROPIC_MODEL": "databricks-claude-sonnet-4-6",
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
        "GH_TOKEN": "github-secret",
        "SWF_BACKEND_TOKEN": "backend-secret",
        "DATABRICKS_TOKEN": "workspace-admin-secret",
    }.items():
        monkeypatch.setenv(key, value)


@pytest.mark.parametrize("kind", ["srt", "docker"])
def test_gateway_only_reaches_model_process(kind, gateway, tmp_path, monkeypatch):
    seen = []

    def run(argv, **kwargs):
        seen.append((argv, kwargs["env"]))
        return RunResult(0, "", "", 0.0)

    monkeypatch.setattr(sandbox, "_run_subprocess", run)
    sb = sandbox.make_sandbox(Config(issue="42", agent="claude", sandbox=kind, workdir=str(tmp_path)), "42")
    sb.run("pytest")
    sb.run_agent("claude -p")
    lifecycle_argv, lifecycle_env = seen[0]
    agent_argv, agent_env = seen[1]
    assert not any(k.startswith(("ANTHROPIC_", "DATABRICKS_", "GH_", "SWF_")) for k in lifecycle_env)
    assert agent_env["ANTHROPIC_AUTH_TOKEN"] == "gateway-secret"
    assert agent_env["ANTHROPIC_BASE_URL"] == GATEWAY
    assert agent_env["ANTHROPIC_MODEL"] == "databricks-claude-sonnet-4-6"
    for key in ("ANTHROPIC_API_KEY", "GH_TOKEN", "SWF_BACKEND_TOKEN", "DATABRICKS_TOKEN"):
        assert key not in agent_env
    assert "gateway-secret" not in json.dumps(agent_argv)
    assert "ANTHROPIC_AUTH_TOKEN" not in lifecycle_argv
    if kind == "srt":
        assert "workspace.cloud.databricks.com" in sb.settings()["network"]["allowedDomains"]
    else:
        assert "ANTHROPIC_AUTH_TOKEN" in agent_argv
        assert "ANTHROPIC_API_KEY" not in agent_argv


@pytest.mark.parametrize(
    "url",
    [
        "",
        "http://workspace.example/anthropic",
        "https://user:private@workspace.example",
        "https://@workspace.example",
        "https://*.example",
        "https://workspace.example?token=private",
        "https://workspace.example#private",
        "https://workspace.example:8443",
        "https://workspace.example:bad",
        "https://workspace.example/\nprivate",
        "https://workspace.example\\private",
        "https://",
    ],
)
def test_bad_gateway_fails_before_launch_without_disclosing_value(url, monkeypatch, tmp_path):
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "private-token")
    monkeypatch.setenv("ANTHROPIC_BASE_URL", url)
    with pytest.raises(StageError) as error:
        sandbox.make_sandbox(Config(issue="42", sandbox="docker", agent="claude", workdir=str(tmp_path)), "42")
    assert "private" not in str(error.value)
    check = _check_model_credential({"ANTHROPIC_AUTH_TOKEN": "private-token", "ANTHROPIC_BASE_URL": url})
    assert not check.ok
    assert "private" not in str(check)


def test_doctor_accepts_gateway_without_anthropic_key():
    check = _check_model_credential({"ANTHROPIC_AUTH_TOKEN": "private-token", "ANTHROPIC_BASE_URL": GATEWAY})
    assert check.ok
    assert "private-token" not in str(check)


def test_toolset_gets_same_scoped_gateway_and_egress(gateway, monkeypatch):
    captured = {}
    monkeypatch.setattr(sandbox, "load_toolset_backend", lambda *a, **kw: object())
    monkeypatch.setattr(sandbox, "ToolsetSandbox", lambda backend, **kw: captured.update(kw))
    sandbox.make_sandbox(Config(issue="42", agent="claude", sandbox="toolset"), "42")
    assert captured["env"]["ANTHROPIC_AUTH_TOKEN"] == "gateway-secret"
    assert "ANTHROPIC_API_KEY" not in captured["env"]
    assert "DATABRICKS_TOKEN" not in captured["env"]
    assert "workspace.cloud.databricks.com" in captured["allow_egress_to"]


def test_scripted_local_and_host_login_ignore_gateway(gateway, monkeypatch, tmp_path):
    monkeypatch.setenv("ANTHROPIC_BASE_URL", "invalid")
    for cfg in (
        Config(issue="42", sandbox="srt", workdir=str(tmp_path)),
        Config(issue="42", agent="claude", sandbox="local", workdir=str(tmp_path), allow_local_agent=True),
        Config(issue="42", agent="claude", sandbox="docker", docker_credentials="host", workdir=str(tmp_path)),
        Config(issue="42", agent="claude", sandbox="islo"),
    ):
        sandbox.make_sandbox(cfg, "42")
