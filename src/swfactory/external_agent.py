"""Versioned executable boundary for operator-installed agent harnesses.

The factory owns admission, reservations, filesystem confinement, validation and receipts.
A wrapper owns its harness dependencies and converts this request to its native interface.
Its output remains candidate evidence: a self-reported charge cannot settle a reservation.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from swfactory.agent import POLICIES, Invocation, Policy
from swfactory.config import Config
from swfactory.models import AgentKind, AgentResult, RunResult, StageError
from swfactory.paths import normalize_absolute_posix_path

if TYPE_CHECKING:
    from swfactory.sandbox import Sandbox

MAX_CANDIDATE_BYTES = 1024 * 1024
_MAX_PROFILE_BYTES = 64 * 1024
_DIGEST_IMAGE = re.compile(r".+@sha256:[0-9a-f]{64}$")
# Only model-provider credentials may enter this execution path. Extending this list is an
# operator-maintained control-plane change, rather than a work-order supplied environment name.
_PROVIDER_CREDENTIALS = frozenset(
    {
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
)


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON value: {value}")


def _finite_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("non-finite JSON number")
    return number


def _json_object(raw: str) -> dict[str, Any]:
    try:
        value = json.loads(
            raw, object_pairs_hook=_unique_object, parse_constant=_reject_constant, parse_float=_finite_float
        )
    except RecursionError as error:
        raise ValueError("JSON nesting exceeds supported depth") from error
    if not isinstance(value, dict):
        raise ValueError("expected one JSON object")
    return value


class ExternalProfile(BaseModel):
    """Strict operator manifest; the executable is never selected by candidate content."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    schema_version: Literal[1]
    id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._-]+$")
    version: str = Field(min_length=1, max_length=128)
    argv: tuple[str, ...] = Field(min_length=1)
    model: str = Field(min_length=1, max_length=256)
    credential_env: tuple[str, ...]
    budget_mode: Literal["no_charge", "hard_cap"]
    enforces_usd_limit: bool

    @field_validator("schema_version", mode="before")
    @classmethod
    def _version_number(cls, value: Any) -> Any:
        if type(value) is not int:
            raise ValueError("schema_version must be integer 1")
        return value

    @field_validator("version", "model")
    @classmethod
    def _nonblank(cls, value: str) -> str:
        if not value.strip() or any(c in value for c in "\n\r\x00"):
            raise ValueError("version and model must be nonblank single-line strings")
        return value

    @field_validator("argv")
    @classmethod
    def _executable_argv(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        executable = normalize_absolute_posix_path(value[0], field="argv[0]")
        if any(not arg or any(c in arg for c in "\x00\n\r") for arg in value):
            raise ValueError("argv requires nonempty single-line strings without NUL")
        if any(arg == "--request" or arg.startswith("--request=") for arg in value[1:]):
            raise ValueError("--request is reserved for the factory")
        return (executable, *value[1:])

    @field_validator("credential_env")
    @classmethod
    def _credentials(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("credential_env cannot contain duplicates")
        if any(name not in _PROVIDER_CREDENTIALS for name in value):
            raise ValueError("credential_env may contain only allowlisted model-provider keys")
        return value

    @model_validator(mode="after")
    def _budget_contract(self) -> ExternalProfile:
        if self.budget_mode == "hard_cap" and not self.enforces_usd_limit:
            raise ValueError("hard_cap requires enforces_usd_limit=true")
        return self


def _profile(cfg: Config) -> tuple[ExternalProfile, Path]:
    if not cfg.agent_profile:
        raise StageError("policy", "external agent requires an operator-owned profile")
    path = Path(cfg.agent_profile).resolve()
    try:
        raw = path.read_bytes()
        if len(raw) > _MAX_PROFILE_BYTES:
            raise ValueError("profile exceeds 64 KiB")
        document = _json_object(raw.decode("utf-8"))
        profile = ExternalProfile.model_validate_json(_canonical(document))
    except (OSError, UnicodeError, ValueError, ValidationError) as error:
        raise StageError("policy", f"invalid external profile: {error}") from error
    return profile, path


def _outside_checkout(path: Path, checkout: Path, *, label: str) -> None:
    if path.resolve().is_relative_to(checkout.resolve()):
        raise StageError("policy", f"external {label} must be outside the candidate checkout")


def profile_document(cfg: Config) -> dict[str, Any]:
    """Return the canonical binding admitted with this run, without credential values."""
    profile, path = _profile(cfg)
    manifest = profile.model_dump(mode="json")
    executable_sha256 = None
    argument_files_sha256: dict[str, str] = {}
    docker_image = None
    checkout = Path(cfg.workdir)
    _outside_checkout(path, checkout, label="profile")
    _outside_checkout(Path(profile.argv[0]), checkout, label="executable")
    if cfg.sandbox == "local":
        executable = Path(profile.argv[0]).resolve()
        try:
            if not executable.is_file() or not os.access(executable, os.X_OK):
                raise ValueError("argv[0] must be an executable file")
            executable_sha256 = hashlib.sha256(executable.read_bytes()).hexdigest()
            # Pin interpreter scripts and file arguments as well as argv[0]. Otherwise
            # changing wrapper.py behind a stable Python executable bypasses admission.
            for argument in profile.argv[1:]:
                argument_path = Path(argument)
                if argument_path.is_absolute() and argument_path.is_file():
                    _outside_checkout(argument_path, checkout, label="argument file")
                    argument_files_sha256[str(argument_path.resolve())] = hashlib.sha256(
                        argument_path.read_bytes()
                    ).hexdigest()
        except (OSError, ValueError) as error:
            raise StageError("policy", f"invalid external executable: {error}") from error
    elif cfg.sandbox == "docker":
        if not _DIGEST_IMAGE.fullmatch(cfg.docker_image):
            raise StageError("policy", "external Docker execution requires a digest-pinned image")
        docker_image = cfg.docker_image
    else:
        raise StageError("policy", "external agents require Docker or explicit local development execution")
    return {
        "manifest": manifest,
        "manifest_sha256": hashlib.sha256(_canonical(manifest).encode()).hexdigest(),
        "executable_sha256": executable_sha256,
        "argument_files_sha256": argument_files_sha256,
        "docker_image": docker_image,
    }


class _Candidate(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal[1]
    call_id: str = Field(min_length=1)
    profile_id: str = Field(min_length=1)
    status: Literal["success", "error"]
    text: str = ""
    data: dict[str, Any] | None = None
    num_turns: int = Field(default=0, ge=0)
    session_id: str | None = None

    @field_validator("schema_version", mode="before")
    @classmethod
    def _version_number(cls, value: Any) -> Any:
        if type(value) is not int:
            raise ValueError("schema_version must be integer 1")
        return value


class ExternalAgent:
    """Launch one external wrapper under the sandbox's explicit execution boundary."""

    kind: AgentKind = "external"

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        self._binding = profile_document(cfg)
        self.profile = ExternalProfile.model_validate_json(_canonical(self._binding["manifest"]))

    @property
    def binding(self) -> dict[str, Any]:
        # Receipts may serialize this value; callers cannot mutate the admitted binding.
        return json.loads(_canonical(self._binding))

    def validate_binding(self) -> None:
        """Fail before reserving/launching if the operator manifest or executable changed."""
        if profile_document(self._cfg) != self._binding:
            raise StageError("policy", "external agent binding changed after admission")

    def validate_call(self, policy: Policy, stage: str) -> None:
        """Native tool patterns are informative; untranslated native overrides are refused."""
        baseline = POLICIES.get(stage)
        if baseline is None:
            raise StageError("policy", f"external agent has no factory policy for stage {stage}")
        if policy.allowed_tools != baseline.allowed_tools or policy.disallowed_tools != baseline.disallowed_tools:
            raise StageError("policy", "external agents do not support native tool-policy overrides")
        if policy.writes != baseline.writes:
            raise StageError("policy", "external agents do not support write-policy overrides")
        if policy.model is not None and policy.model != self.profile.model:
            raise StageError("policy", "external model override differs from the admitted profile model")

    def validate_execution(
        self, sb: Sandbox, policy: Policy, writable_paths: Sequence[str], protected: Sequence[str]
    ) -> None:
        """Refuse known launch/configuration failures before reserving provider money."""
        checkout = Path(sb.workdir)
        _outside_checkout(Path(self.profile.argv[0]), checkout, label="executable")
        _outside_checkout(Path(self._cfg.agent_profile or ""), checkout, label="profile")
        for argument in self._binding["argument_files_sha256"]:
            _outside_checkout(Path(argument), checkout, label="argument file")
        if self._cfg.record_dir:
            raise StageError("policy", "external fixture recording is not supported")
        if not policy.writes and writable_paths:
            raise StageError("policy", "a read-only external call cannot receive writable paths")
        sb.preflight_external(
            self.profile.argv,
            writes=writable_paths,
            protected=protected,
            timeout_s=policy.timeout_s,
            credential_env=self.profile.credential_env,
        )

    def run(
        self,
        sb: Sandbox,
        *,
        stage: str,
        iteration: int,
        prompt: str,
        policy: Policy,
        schema: type[BaseModel] | None,
        cfg: Config,
        issue_id: str,
        protected: Sequence[str] = (),
        invocation: Invocation | None = None,
    ) -> AgentResult:
        self.validate_binding()
        self.validate_call(policy, stage)
        if invocation is None or not invocation.call_id:
            raise StageError("policy", "external execution requires a host-owned call identity")
        self.validate_execution(sb, policy, invocation.writable_paths, protected)
        if cfg.record_dir:
            raise StageError("policy", "external fixture recording is not supported")
        request = {
            "schema_version": 1,
            "call_id": invocation.call_id,
            "accepted_inputs_digest": invocation.accepted_inputs_digest,
            "profile_id": self.profile.id,
            "profile_version": self.profile.version,
            "stage": stage,
            "iteration": iteration,
            "issue_id": issue_id,
            "model": self.profile.model,
            "prompt": prompt,
            "output_schema": schema.model_json_schema() if schema is not None else None,
            "budget": {
                "usd": cfg.max_budget_usd_per_stage,
                "max_turns": cfg.max_turns,
                "timeout_s": policy.timeout_s,
            },
            "policy": {
                "allowed_tools": list(policy.allowed_tools),
                "disallowed_tools": list(policy.disallowed_tools),
                "writes": policy.writes,
                "protected_paths": list(protected),
                "writable_paths": list(invocation.writable_paths),
            },
        }
        result = sb.run_external(
            self.profile.argv,
            request=_canonical(request),
            call_id=invocation.call_id,
            writes=invocation.writable_paths,
            protected=tuple(protected),
            timeout_s=policy.timeout_s,
            credential_env=self.profile.credential_env,
        )
        return self._result(result, invocation.call_id, schema)

    def _result(self, run: RunResult, call_id: str, schema: type[BaseModel] | None) -> AgentResult:
        cost = 0.0 if self.profile.budget_mode == "no_charge" else None
        common: dict[str, Any] = {"agent": self.kind, "cost_usd": cost, "duration_ms": int(run.duration_s * 1000)}
        envelope: dict[str, Any] = {}
        try:
            if len(run.stdout.encode("utf-8")) > MAX_CANDIDATE_BYTES:
                raise ValueError("candidate exceeds 1 MiB")
            envelope = _json_object(run.stdout)
            candidate = _Candidate.model_validate(envelope)
            if candidate.call_id != call_id or candidate.profile_id != self.profile.id:
                raise ValueError("candidate does not match the host call and profile identity")
        except (ValueError, ValidationError) as error:
            subtype = "error_external_output"
            if run.timed_out:
                subtype = "error_external_timeout"
            elif run.exit_code != 0:
                subtype = "error_external_exit"
            return AgentResult(
                **common,
                text=f"invalid external candidate: {error}",
                is_error=True,
                subtype=subtype,
                raw_envelope={key: value for key, value in envelope.items() if key != "text"},
            )
        common["raw_envelope"] = {key: value for key, value in envelope.items() if key != "text"}
        if run.timed_out or run.exit_code != 0 or candidate.status == "error":
            subtype = "error_external_timeout" if run.timed_out else "error_external_exit"
            if run.exit_code == 0 and not run.timed_out:
                subtype = "error_external_agent"
            return AgentResult(
                **common,
                text=candidate.text,
                num_turns=candidate.num_turns,
                session_id=candidate.session_id,
                is_error=True,
                subtype=subtype,
            )
        if schema is not None:
            try:
                if candidate.data is None:
                    raise ValueError("external candidate omitted required structured data")
                data = schema.model_validate(candidate.data).model_dump()
            except (ValueError, ValidationError) as error:
                return AgentResult(**common, text=str(error), is_error=True, subtype="error_schema_validation")
        else:
            data = candidate.data
        return AgentResult(
            **common,
            text=candidate.text,
            data=data,
            num_turns=candidate.num_turns,
            session_id=candidate.session_id,
        )
