"""Strict external contracts, immutable bindings and candidate-only accounting."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from swfactory import doctor
from swfactory.agent import POLICIES, Invocation, make_agent
from swfactory.config import Config
from swfactory.external_agent import MAX_CANDIDATE_BYTES, ExternalAgent, profile_document
from swfactory.models import BuildSummary, RunResult, StageError

_INVOCATION = Invocation("call-1", "inputs:" + "a" * 64, ("src", "tests"))


class ExternalSandbox:
    """Only the executable boundary is available; no sandbox receipt writes are possible."""

    def __init__(self, workdir: Path, response: RunResult) -> None:
        self.workdir = str(workdir)
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def run_external(self, argv: tuple[str, ...], **kwargs: Any) -> RunResult:
        self.calls.append({"argv": argv, **kwargs})
        return self.response

    def preflight_external(self, argv: tuple[str, ...], **kwargs: Any) -> list[str]:
        return []


def _profile(tmp_path: Path, **changes: Any) -> tuple[Path, Path]:
    executable = tmp_path / "trusted-wrapper"
    executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    executable.chmod(0o700)
    document = {
        "schema_version": 1,
        "id": "fixture",
        "version": "1.0",
        "argv": [str(executable)],
        "model": "fixture-model",
        "credential_env": [],
        "budget_mode": "no_charge",
        "enforces_usd_limit": False,
        **changes,
    }
    profile = tmp_path / "profile.json"
    profile.write_text(json.dumps(document), encoding="utf-8")
    return profile, executable


def _cfg(tmp_path: Path, profile: Path, **changes: Any) -> Config:
    return Config(
        issue="DEMO-1",
        agent="external",
        agent_profile=str(profile),
        sandbox="local",
        allow_local_agent=True,
        workdir=str(tmp_path / "candidate"),
        **changes,
    )


def _candidate(**changes: Any) -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "call_id": "call-1",
            "profile_id": "fixture",
            "status": "success",
            "text": "candidate prose",
            "data": {"summary": "edited source", "files_changed": ["src/a.py"]},
            "num_turns": 3,
            "session_id": "native-session",
            **changes,
        }
    )


def _run(
    agent: ExternalAgent,
    cfg: Config,
    sb: ExternalSandbox,
    *,
    schema: Any = BuildSummary,
    invocation: Invocation | None = _INVOCATION,
    policy: Any = POLICIES["build"],
) -> Any:
    return agent.run(
        sb,
        stage="build",
        iteration=2,
        prompt="Implement the accepted plan.",
        policy=policy,
        schema=schema,
        cfg=cfg,
        issue_id="DEMO-1",
        protected=(".git/**", "docs/factory/**"),
        invocation=invocation,
    )


def test_external_factory_request_and_candidate_receipt(tmp_path: Path) -> None:
    path, executable = _profile(tmp_path, credential_env=["OPENAI_API_KEY"])
    cfg = _cfg(tmp_path, path, max_turns=7, max_budget_usd_per_stage=1.25)
    agent = make_agent(cfg)
    assert isinstance(agent, ExternalAgent)
    sb = ExternalSandbox(Path(cfg.workdir), RunResult(0, _candidate(), "", 0.125))
    result = _run(agent, cfg, sb)
    assert result.agent == "external" and not result.is_error
    assert result.cost_usd == 0  # admitted no-charge profile, never candidate accounting
    assert result.duration_ms == 125 and result.num_turns == 3
    assert result.data == {"summary": "edited source", "files_changed": ["src/a.py"]}
    assert result.session_id == "native-session"
    assert result.raw_envelope is not None and "text" not in result.raw_envelope
    assert "raw_envelope" not in result.model_dump()
    call = sb.calls[0]
    assert call["argv"] == (str(executable),)
    assert call["writes"] == ("src", "tests")
    assert call["credential_env"] == ("OPENAI_API_KEY",)
    assert call["protected"] == (".git/**", "docs/factory/**")
    request = json.loads(call["request"])
    assert request["schema_version"] == 1
    assert request["call_id"] == "call-1" and request["profile_id"] == "fixture"
    assert request["accepted_inputs_digest"] == "inputs:" + "a" * 64
    assert request["stage"] == "build" and request["iteration"] == 2
    assert request["model"] == "fixture-model"
    assert request["budget"] == {"usd": 1.25, "max_turns": 7, "timeout_s": 1800}
    assert request["policy"]["writable_paths"] == ["src", "tests"]
    assert request["output_schema"] == BuildSummary.model_json_schema()


def test_hard_cap_does_not_fabricate_known_cost(tmp_path: Path) -> None:
    path, _ = _profile(tmp_path, budget_mode="hard_cap", enforces_usd_limit=True)
    cfg = _cfg(tmp_path, path)
    agent = ExternalAgent(cfg)
    result = _run(agent, cfg, ExternalSandbox(Path(cfg.workdir), RunResult(0, _candidate(), "", 0)))
    assert result.cost_usd is None and not result.is_error


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_version": True},
        {"schema_version": "1"},
        {"schema_version": 2},
        {"argv": ["relative-wrapper"]},
        {"argv": []},
        {"argv": ["/opt/../wrapper"]},
        {"argv": ["/opt/wrapper", ""]},
        {"argv": ["/opt/wrapper", "arg\nnext"]},
        {"argv": ["/opt/agent", "--request=attacker.json"]},
        {"model": ""},
        {"model": "  "},
        {"enforces_usd_limit": "true"},
        {"budget_mode": "hard_cap", "enforces_usd_limit": False},
        {"budget_mode": "reported_cost"},
        {"unknown": "value"},
    ],
)
def test_profile_is_a_strict_versioned_contract(tmp_path: Path, changes: dict[str, Any]) -> None:
    path, _ = _profile(tmp_path, **changes)
    with pytest.raises(StageError, match="invalid external profile"):
        profile_document(_cfg(tmp_path, path))


@pytest.mark.parametrize(
    "credential",
    ["GH_TOKEN", "GITHUB_TOKEN", "SWF_AGENT", "AIRFLOW_HOME", "AWS_SECRET_ACCESS_KEY", "SCM_TOKEN", "PATH"],
)
def test_factory_and_service_credentials_are_refused(tmp_path: Path, credential: str) -> None:
    path, _ = _profile(tmp_path, credential_env=[credential])
    with pytest.raises(StageError, match="model-provider keys"):
        profile_document(_cfg(tmp_path, path))


def test_duplicate_keys_and_nonfinite_json_are_refused(tmp_path: Path) -> None:
    path, _ = _profile(tmp_path)
    cfg = _cfg(tmp_path, path)
    valid = path.read_text()
    path.write_text(valid[:-1] + ', "model":"second"}')
    with pytest.raises(StageError, match="duplicate JSON key"):
        profile_document(cfg)
    path.write_text(valid[:-1] + ', "unknown":NaN}')
    with pytest.raises(StageError, match="non-finite JSON"):
        profile_document(cfg)


@pytest.mark.parametrize("change", ["manifest", "executable"])
def test_binding_changes_are_refused_before_execution(tmp_path: Path, change: str) -> None:
    path, executable = _profile(tmp_path)
    cfg = _cfg(tmp_path, path)
    agent = ExternalAgent(cfg)
    sb = ExternalSandbox(Path(cfg.workdir), RunResult(0, _candidate(), "", 0))
    if change == "manifest":
        document = json.loads(path.read_text())
        document["version"] = "2.0"
        path.write_text(json.dumps(document))
    else:
        executable.write_text("#!/bin/sh\necho changed\n")
    with pytest.raises(StageError, match="binding changed"):
        _run(agent, cfg, sb)
    assert not sb.calls


def test_binding_is_canonical_and_defensively_copied(tmp_path: Path) -> None:
    path, _ = _profile(tmp_path)
    cfg = _cfg(tmp_path, path)
    agent = ExternalAgent(cfg)
    binding = agent.binding
    assert len(binding["manifest_sha256"]) == len(binding["executable_sha256"]) == 64
    binding["manifest"]["argv"][0] = "/different"
    document = json.loads(path.read_text())
    path.write_text(json.dumps(document, indent=2, sort_keys=True))
    agent.validate_binding()
    assert agent.binding["manifest"]["argv"][0] != "/different"


def test_interpreter_argument_file_is_pinned_before_execution(tmp_path: Path) -> None:
    script = tmp_path / "wrapper.py"
    script.write_text("print('first')\n")
    path, executable = _profile(tmp_path)
    document = json.loads(path.read_text())
    document["argv"] = [str(executable), str(script)]
    path.write_text(json.dumps(document))
    cfg = _cfg(tmp_path, path)
    agent = ExternalAgent(cfg)
    assert len(agent.binding["argument_files_sha256"][str(script.resolve())]) == 64
    sb = ExternalSandbox(Path(cfg.workdir), RunResult(0, _candidate(), "", 0))
    script.write_text("print('changed')\n")
    with pytest.raises(StageError, match="binding changed"):
        _run(agent, cfg, sb)
    assert not sb.calls


def test_local_argument_file_cannot_come_from_candidate_checkout(tmp_path: Path) -> None:
    checkout = tmp_path / "candidate"
    checkout.mkdir()
    script = checkout / "wrapper.py"
    script.write_text("print('candidate-controlled')\n")
    path, executable = _profile(tmp_path)
    document = json.loads(path.read_text())
    document["argv"] = [str(executable), str(script)]
    path.write_text(json.dumps(document))
    with pytest.raises(StageError, match="argument file must be outside"):
        ExternalAgent(_cfg(tmp_path, path))


@pytest.mark.parametrize("inside", ["profile", "executable"])
def test_local_operator_inputs_must_be_outside_checkout(tmp_path: Path, inside: str) -> None:
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    path, executable = _profile(tmp_path)
    if inside == "profile":
        moved = candidate / "profile.json"
        path.rename(moved)
        path = moved
    else:
        moved = candidate / "wrapper"
        executable.rename(moved)
        document = json.loads(path.read_text())
        document["argv"] = [str(moved)]
        path.write_text(json.dumps(document))
    with pytest.raises(StageError, match="outside the candidate checkout"):
        profile_document(_cfg(tmp_path, path))


def test_docker_binding_uses_image_digest_and_container_executable(tmp_path: Path) -> None:
    path, _ = _profile(tmp_path, argv=["/opt/harness/wrapper"])
    cfg = _cfg(tmp_path, path).model_copy(
        update={"sandbox": "docker", "docker_image": "registry/wrapper@sha256:" + "a" * 64}
    )
    binding = profile_document(cfg)
    assert binding["docker_image"] == cfg.docker_image
    assert binding["executable_sha256"] is None
    with pytest.raises(StageError, match="digest-pinned"):
        profile_document(cfg.model_copy(update={"docker_image": "registry/wrapper:latest"}))


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_version": True},
        {"schema_version": "1"},
        {"call_id": "another-call"},
        {"profile_id": "another-profile"},
        {"status": "running"},
        {"text": {"arbitrary": "object"}},
        {"data": []},
        {"num_turns": True},
        {"num_turns": -1},
        {"session_id": 42},
        {"cost_usd": 0},
        {"usage": {"total_cost_usd": 0}},
    ],
)
def test_candidate_cannot_supply_identity_accounting_or_untyped_fields(tmp_path: Path, changes: dict[str, Any]) -> None:
    path, _ = _profile(tmp_path, budget_mode="hard_cap", enforces_usd_limit=True)
    cfg = _cfg(tmp_path, path)
    sb = ExternalSandbox(Path(cfg.workdir), RunResult(0, _candidate(**changes), "", 0))
    result = _run(ExternalAgent(cfg), cfg, sb)
    assert result.is_error and result.subtype == "error_external_output"
    assert result.cost_usd is None


@pytest.mark.parametrize(
    ("response", "subtype"),
    [
        (RunResult(0, _candidate(status="error"), "", 0), "error_external_agent"),
        (RunResult(12, _candidate(), "", 0), "error_external_exit"),
        (RunResult(124, "", "", 0, True), "error_external_timeout"),
        (RunResult(0, "not json", "", 0), "error_external_output"),
        (RunResult(0, '{"deep":' * 2000 + "0" + "}" * 2000, "", 0), "error_external_output"),
        (RunResult(0, _candidate(data=None), "", 0), "error_schema_validation"),
        (RunResult(0, _candidate(data={"summary": "x", "extra": 1}), "", 0), "error_schema_validation"),
        (RunResult(0, "{}" + " " * MAX_CANDIDATE_BYTES, "", 0), "error_external_output"),
    ],
)
def test_runtime_and_schema_failures_cannot_be_success(tmp_path: Path, response: RunResult, subtype: str) -> None:
    path, _ = _profile(tmp_path)
    cfg = _cfg(tmp_path, path)
    result = _run(ExternalAgent(cfg), cfg, ExternalSandbox(Path(cfg.workdir), response))
    assert result.is_error and result.subtype == subtype


def test_policy_and_call_identity_are_checked_before_launch(tmp_path: Path) -> None:
    path, _ = _profile(tmp_path)
    cfg = _cfg(tmp_path, path)
    agent = ExternalAgent(cfg)
    sb = ExternalSandbox(Path(cfg.workdir), RunResult(0, _candidate(), "", 0))
    for policy in (
        replace(POLICIES["build"], allowed_tools=("Bash",)),
        replace(POLICIES["build"], disallowed_tools=()),
        replace(POLICIES["build"], model="different-model"),
        replace(POLICIES["build"], writes=False),
    ):
        with pytest.raises(StageError, match="overrides|override"):
            _run(agent, cfg, sb, policy=policy)
    with pytest.raises(StageError, match="host-owned call identity"):
        _run(agent, cfg, sb, invocation=None)
    with pytest.raises(StageError, match="read-only.*writable"):
        agent.run(
            sb,
            stage="spec",
            iteration=1,
            prompt="x",
            policy=POLICIES["spec"],
            schema=None,
            cfg=cfg,
            issue_id="DEMO-1",
            invocation=Invocation("call-1", writable_paths=("src",)),
        )
    assert not sb.calls


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"agent_profile": None}, "requires SWF_AGENT_PROFILE"),
        ({"sandbox": "srt"}, "currently require sandbox=docker"),
        ({"sandbox": "islo"}, "currently require sandbox=docker"),
        ({"sandbox": "toolset"}, "currently require sandbox=docker"),
        ({"sandbox": "boat"}, "currently require sandbox=docker"),
        ({"sandbox": "local", "allow_local_agent": False}, "requires --allow-local-agent"),
        ({"record_dir": str(Path("/tmp/recordings"))}, "fixture recording is not supported"),
        ({"sandbox": "docker", "docker_image": "image:latest"}, "digest-pinned"),
        (
            {
                "sandbox": "docker",
                "docker_image": "image@sha256:" + "a" * 64,
                "docker_credentials": "host",
            },
            "cannot mount host login",
        ),
    ],
)
def test_config_refuses_unsupported_external_boundaries(tmp_path: Path, changes: dict[str, Any], reason: str) -> None:
    path, _ = _profile(tmp_path)
    values = _cfg(tmp_path, path).model_dump()
    with pytest.raises(ValidationError, match=reason):
        Config(**{**values, **changes})


def test_external_doctor_validates_without_executing_and_blocks_missing_profile(tmp_path: Path) -> None:
    path, _ = _profile(tmp_path)
    cfg = _cfg(tmp_path, path)

    def refuse_execution(argv: Any) -> str:
        pytest.fail(f"external doctor must not execute the wrapper: {argv}")

    checks = doctor.sandbox_checks(cfg, runner=refuse_execution)
    assert doctor.blocking(checks) == []
    external = next(check for check in checks if check.name == "external agent")
    assert external.ok and "fixture 1.0" in external.detail
    path.unlink()
    checks = doctor.sandbox_checks(cfg, runner=refuse_execution)
    blocked = doctor.blocking(checks)
    assert len(blocked) == 1 and blocked[0].name == "external agent"
    assert "verify SWF_AGENT_PROFILE" in blocked[0].fix


def test_candidate_rejects_overflowing_json_number(tmp_path: Path) -> None:
    path, _ = _profile(tmp_path)
    cfg = _cfg(tmp_path, path)
    raw = _candidate().replace('"num_turns": 3', '"num_turns": 1e400')
    result = _run(ExternalAgent(cfg), cfg, ExternalSandbox(Path(cfg.workdir), RunResult(0, raw, "", 0)))
    assert result.is_error and result.subtype == "error_external_output"
    assert "non-finite JSON number" in result.text
