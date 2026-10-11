"""Operator-attested external billing observations, never candidate-reported usage.

This imports a human observation; it does not contact or certify a provider. Docker teardown
and the factory's pre-launch descriptor must already exist before funds can be returned.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import stat
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from swfactory.call_accounting import CALLS_LOG, CallLedger, CallRecord, ReconciliationRefused
from swfactory.external_agent import ExternalProfile, _json_object
from swfactory.idempotency import MutationOutcome
from swfactory.paths import confined_path, validate_run_id
from swfactory.sandbox_governance import SandboxIdentity
from swfactory.state import RunState

_MAX_BYTES = 64 * 1024
_SHA = r"^[a-f0-9]{64}$"


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True, allow_inf_nan=False)

    @field_validator("schema_version", mode="before", check_fields=False)
    @classmethod
    def _version(cls, value: Any) -> Any:
        if type(value) is not int:
            raise ValueError("schema_version must be integer 1")
        return value


class _Binding(_Strict):
    manifest: ExternalProfile
    manifest_sha256: str = Field(pattern=_SHA)
    executable_sha256: str | None = Field(pattern=_SHA)
    argument_files_sha256: dict[str, str]
    docker_image: str | None


class _Descriptor(_Strict):
    schema_version: Literal[1]
    authority: Literal["factory"]
    run_id: str
    call_id: str
    accepted_inputs_digest: str | None
    profile: _Binding
    profile_binding_sha256: str = Field(pattern=_SHA)
    stage: str
    iteration: int = Field(ge=1)
    reserved_usd: float = Field(ge=0)
    sandbox: str
    workdir: str
    sandbox_labels: dict[str, str]
    created_at: str


class _Observation(_Strict):
    schema_version: Literal[1]
    run_id: str
    call_id: str
    accepted_inputs_digest: str = Field(pattern=r"^inputs:[a-f0-9]{64}$")
    profile_binding_sha256: str = Field(pattern=_SHA)
    provider: str = Field(min_length=1, max_length=128)
    account_id: str = Field(min_length=1, max_length=256)
    request_id: str = Field(min_length=1, max_length=256)
    evidence_reference: str = Field(min_length=1, max_length=1024)
    observed_at: str
    status: Literal["committed", "definitely_absent", "ambiguous"]
    final: bool
    cost_usd: float | None = Field(default=None, ge=0)

    @field_validator("cost_usd", mode="before")
    @classmethod
    def _cost(cls, value: Any) -> Any:
        if value is not None and type(value) not in (int, float):
            raise ValueError("cost_usd must be a finite USD number")
        return value

    @field_validator("provider", "account_id", "request_id", "evidence_reference")
    @classmethod
    def _label(cls, value: str) -> str:
        if not value.strip() or any(ord(char) < 32 for char in value):
            raise ValueError("billing references must be nonblank single-line strings")
        return value

    @field_validator("observed_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        timestamp = datetime.fromisoformat(value)
        if timestamp.tzinfo is None:
            raise ValueError("observed_at requires a timezone")
        return value

    @model_validator(mode="after")
    def _outcome(self) -> _Observation:
        if self.status == "committed":
            if not self.final or self.cost_usd is None:
                raise ValueError("committed requires final=true and the final aggregate USD bill")
        elif self.cost_usd is not None:
            raise ValueError("only committed observations may contain cost_usd")
        if self.status == "definitely_absent" and not self.final:
            raise ValueError("definitely_absent requires final=true")
        if self.status == "ambiguous" and self.final:
            raise ValueError("ambiguous observations cannot claim final=true")
        return self


def _parse(raw: bytes, model: type[_Strict]) -> Any:
    if len(raw) > _MAX_BYTES:
        raise ValueError("billing evidence exceeds 64 KiB")
    try:
        document = _json_object(raw.decode("utf-8"))
        return model.model_validate_json(_canonical(document))
    except ValidationError as error:
        # Observations may be private. Do not echo rejected input values into a CLI error.
        fields = ", ".join(".".join(map(str, item["loc"])) or "document" for item in error.errors())
        raise ValueError(f"invalid billing evidence fields: {fields}") from None


def _state(root: Path, run_id: str) -> RunState:
    directory = confined_path(root.expanduser().resolve(), validate_run_id(run_id))
    state = RunState(directory)
    if not state.root.is_dir():
        raise FileNotFoundError(f"no saved state for run {run_id!r}")
    return state


def _records(state: RunState) -> list[CallRecord]:
    # The historical ledger reader has permissive defaults. Operator reporting must not turn
    # malformed money into zero, and reconciliation must not append to such a ledger.
    for row in state.read_jsonl(CALLS_LOG):
        if not isinstance(row, dict):
            raise ValueError("invalid agent call journal record")
        if "call_id" not in row:
            names = ("floor_usd",)
        else:
            if not isinstance(row["call_id"], str) or not row["call_id"]:
                raise ValueError("invalid agent call identity")
            names = ("reserved_usd", "cost_usd")
        for name in names:
            value = row.get(name)
            if name == "cost_usd" and value is None:
                continue
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise ValueError(f"invalid {name} in agent call journal")
    return CallLedger(state).records()


def _descriptor(state: RunState, record: CallRecord) -> _Descriptor:
    key = hashlib.sha256(record.call_id.encode()).hexdigest()
    name = f"external-calls/{key}.json"
    if not state.has_control(name):
        raise ReconciliationRefused("call lacks a factory pre-launch descriptor; legacy evidence cannot be inferred")
    descriptor = _parse(state.read_control(name).encode(), _Descriptor)
    binding = descriptor.profile.model_dump(mode="json")
    if (
        descriptor.call_id != record.call_id
        or descriptor.stage != record.stage
        or descriptor.iteration != record.iteration
        or descriptor.reserved_usd != record.reserved_usd
        or descriptor.profile_binding_sha256 != _hash(binding)
        or descriptor.profile.manifest_sha256 != _hash(binding["manifest"])
        or not re.fullmatch(r"inputs:[a-f0-9]{64}", descriptor.accepted_inputs_digest or "")
    ):
        raise ReconciliationRefused("factory call descriptor does not match the call ledger and admitted binding")
    return descriptor


def list_calls(root: Path, run_id: str) -> dict[str, Any]:
    """Read call identities and reservations without exposing candidate output or repairing state."""
    state = _state(root, run_id)
    records = _records(state)
    calls = []
    for record in records:
        row = {
            "call_id": record.call_id,
            "stage": record.stage,
            "iteration": record.iteration,
            "state": record.state,
            "reserved_usd": record.reserved_usd,
            "settled_usd": record.settled_usd,
            "unreconciled": record.unreconciled,
            "binding": None,
        }
        key = hashlib.sha256(record.call_id.encode()).hexdigest()
        if state.has_control(f"external-calls/{key}.json"):
            descriptor = _descriptor(state, record)
            row["binding"] = {
                "run_id": descriptor.run_id,
                "accepted_inputs_digest": descriptor.accepted_inputs_digest,
                "profile_binding_sha256": descriptor.profile_binding_sha256,
                "profile_id": descriptor.profile.manifest.id,
                "model": descriptor.profile.manifest.model,
                "sandbox": descriptor.sandbox,
            }
        calls.append(row)
    ledger = CallLedger(state)
    return {
        "run_id": run_id,
        "calls": calls,
        "charged_usd": ledger.charged_usd(),
        "unreconciled_usd": ledger.unreconciled_usd(),
        "legacy_floor_usd": ledger.floor_usd(),
    }


def _cleanup_time(state: RunState, descriptor: _Descriptor) -> float:
    if descriptor.sandbox.partition(":")[0] != "docker":
        raise ReconciliationRefused(
            "billing imports require factory-observed Docker teardown; local dev is unqualified"
        )
    journal = state.read_jsonl("external-processes.jsonl")
    if any(not isinstance(row, dict) for row in journal):
        raise ReconciliationRefused("external process journal contains an invalid record")
    rows = [row for row in journal if row.get("call_id") == descriptor.call_id]
    if SandboxIdentity.from_labels("docker", descriptor.sandbox_labels) is None:
        raise ReconciliationRefused("factory descriptor lacks complete Docker ownership labels")
    expected = {
        **descriptor.sandbox_labels,
        "swfactory.external": "true",
        "swfactory.external-call": hashlib.sha256(descriptor.call_id.encode()).hexdigest()[:16],
    }
    if (
        not rows
        or rows[0].get("event") != "intent"
        or rows[0].get("container_id_required") is not True
        or rows[-1].get("event") != "cleaned"
        or any(row.get("labels") != expected or row.get("container") != rows[0].get("container") for row in rows)
    ):
        raise ReconciliationRefused("call has no matching terminal Docker cleanup; recover execution before billing")
    ids = [row.get("container_id") for row in rows if row.get("event") in {"created", "start_intent"}]
    if not ids or any(not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value) for value in ids):
        raise ReconciliationRefused("Docker cleanup lacks the acknowledged creation identity")
    if len(set(ids)) != 1:
        raise ReconciliationRefused("Docker cleanup lacks the acknowledged creation identity")
    cleaned_at = rows[-1].get("at")
    if type(cleaned_at) not in (int, float) or not math.isfinite(cleaned_at) or cleaned_at < 0:
        raise ReconciliationRefused("Docker cleanup has an invalid observation time")
    return cleaned_at


def reconcile_call(
    root: Path, run_id: str, call_id: str, *, observation: Path, actor: str, reason: str
) -> dict[str, Any]:
    """Import one explicit operator attestation under the same lock that fences stage execution."""
    if not actor.strip() or not reason.strip() or len(actor) > 256 or len(reason) > 2000:
        raise ValueError("billing reconciliation requires a nonblank bounded actor and reason")
    state = _state(root, run_id)
    with state.exclusive("reconcile-call"):
        records = {record.call_id: record for record in _records(state)}
        if call_id not in records:
            raise ReconciliationRefused("call is not recorded in this run")
        record = records[call_id]
        descriptor = _descriptor(state, record)
        source = observation.expanduser()
        path = source.resolve()
        if (
            path.is_relative_to(root.expanduser().resolve())
            or path.is_relative_to(Path(descriptor.workdir).resolve())
            or source.is_symlink()
        ):
            raise ReconciliationRefused(
                "billing observations must be operator files outside factory state and checkout"
            )
        # Reject pipes/devices before reading, without blocking the run lock on open().
        # Open the original expanded spelling so O_NOFOLLOW still sees a final symlink.
        fd = os.open(source, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
                raise ReconciliationRefused(
                    "billing observation must be an operator-owned regular file without hardlinks"
                )
            raw = handle.read(_MAX_BYTES + 1)
        observed = _parse(raw, _Observation)
        if (
            observed.run_id != run_id
            or descriptor.run_id != run_id
            or observed.call_id != call_id
            or observed.accepted_inputs_digest != descriptor.accepted_inputs_digest
            or observed.profile_binding_sha256 != descriptor.profile_binding_sha256
        ):
            raise ReconciliationRefused("observation does not match this run, call, admitted inputs and profile")
        digest = hashlib.sha256(raw).hexdigest()
        previous = (record.observation or {}).get("evidence") or {}
        if record.settled_usd is not None:
            if previous.get("observation_sha256") != digest:
                raise ReconciliationRefused("settled charges are immutable; conflicting or unrelated replay refused")
            return {"call_id": call_id, "already_applied": True, "settled_usd": record.settled_usd}
        if observed.status != "ambiguous":
            stopped_at = _cleanup_time(state, descriptor)
            if datetime.fromisoformat(observed.observed_at).timestamp() < stopped_at:
                raise ReconciliationRefused("final billing observation predates terminal Docker cleanup")
        evidence = {
            "schema_version": 1,
            "authority": "operator_attestation",
            "actor": actor.strip(),
            "reason": reason.strip(),
            "imported_at": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "source": str(path),
            "observation_sha256": digest,
            "observation": observed.model_dump(mode="json"),
        }
        key = hashlib.sha256(call_id.encode()).hexdigest()
        archive = f"external-observations/{key}/{digest}.json"
        if state.has_control(archive):
            # Preserve the first import's provenance across a crash before the ledger append.
            retained = _json_object(state.read_control(archive))
            if retained.get("observation_sha256") != digest or retained.get("observation") != evidence["observation"]:
                raise ReconciliationRefused("archived observation does not match the imported evidence")
            evidence = retained
        else:
            state.write_control(archive, _canonical(evidence) + "\n")
        outcome = MutationOutcome(
            observed.status, observed.cost_usd, evidence, detail="explicit operator billing attestation"
        )
        settled = CallLedger(state).reconcile(call_id, outcome)
        return {
            "call_id": call_id,
            "already_applied": False,
            "settled_usd": settled.settled_usd,
            "unreconciled": settled.unreconciled,
            "observation_sha256": digest,
        }
