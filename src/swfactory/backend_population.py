"""Airflow-worker client for backend-owned population provider execution.

The worker knows the exact search invocation and the factory backend bearer token. It never knows the
provider/model credential. Every call is routed to /v1/population/execute, where the trusted
backend performs lease projection, provider execution, artifact retention, journaling, and evidence.
"""

from __future__ import annotations

import hashlib
import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from swfactory.models import StageError
from swfactory.population_adapter import PopulationInvocation
from swfactory.population_manifest import (
    BehaviorReceipt,
    PopulationManifestError,
    behavior_receipt_from_document,
)
from swfactory.provider_binding import BoundPopulationTask
from swfactory.webhook import _safe_backend_base

MAX_BACKEND_POPULATION_RESPONSE = 2 * 1024 * 1024


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, _req, _fp, _code, _msg, _headers, _newurl):
        return None


@dataclass(frozen=True)
class PopulationTaskInput:
    instruction: str
    context_artifact_digests: tuple[str, ...] = ()
    objective_digest: str | None = None


class BackendPopulationRunner:
    """Callable consumed by PopulationExecutor inside an already-scheduled Airflow task."""

    def __init__(
        self,
        *,
        backend_url: str,
        backend_token: str,
        cell_id: str,
        epoch: int,
        policy_digest: str,
        population_manifest_digest: str,
        provider_binding_digest: str,
        inputs: dict[str, PopulationTaskInput],
        timeout_s: float = 180.0,
        opener=None,
    ) -> None:
        try:
            self.backend_url = _safe_backend_base(backend_url)
        except ValueError as error:
            raise StageError("policy", f"managed population backend URL is invalid: {error}") from error
        if len(backend_token) < 32 or any(char.isspace() for char in backend_token):
            raise StageError("policy", "managed population execution requires a valid SWF_BACKEND_TOKEN")
        if epoch < 1 or not cell_id.strip():
            raise StageError("policy", "managed population execution requires a valid Cell epoch")
        if not policy_digest.startswith("policy:"):
            raise StageError("policy", "managed population execution requires a Factory Cell policy digest")
        self.backend_token = backend_token
        self.cell_id = cell_id
        self.epoch = epoch
        self.policy_digest = policy_digest
        self.population_manifest_digest = population_manifest_digest
        self.provider_binding_digest = provider_binding_digest
        self.inputs = dict(inputs)
        self.timeout_s = timeout_s
        self._open = opener or urllib.request.build_opener(_NoRedirect()).open

    def __call__(self, task: BoundPopulationTask) -> BehaviorReceipt:
        try:
            task_input = self.inputs[task.task_id]
        except KeyError as error:
            raise StageError(
                "provider",
                f"population task {task.task_id} has no immutable invocation input",
            ) from error
        invocation = PopulationInvocation(
            task=task,
            population_manifest_digest=self.population_manifest_digest,
            provider_binding_digest=self.provider_binding_digest,
            instruction=task_input.instruction,
            context_artifact_digests=task_input.context_artifact_digests,
            objective_digest=task_input.objective_digest,
        )
        invocation.validate()
        operation_key = "population_model_call:" + hashlib.sha256(
            (task.task_id + "\0" + invocation.digest()).encode()
        ).hexdigest()[:24]
        body = {
            "cell_id": self.cell_id,
            "epoch": self.epoch,
            "policy_digest": self.policy_digest,
            "operation_key": operation_key,
            "population_manifest_digest": self.population_manifest_digest,
            "provider_binding_digest": self.provider_binding_digest,
            "task": task.canonical_dict() | {"binding_digest": task.binding_digest},
            "instruction": task_input.instruction,
            "context_artifact_digests": list(task_input.context_artifact_digests),
            "objective_digest": task_input.objective_digest,
        }
        response = self._post(body)
        if response.get("invocation_digest") != invocation.digest():
            raise StageError("provider", "backend population receipt is bound to another invocation")
        if response.get("population_manifest_digest") != self.population_manifest_digest:
            raise StageError("provider", "backend population receipt is bound to another manifest")
        if response.get("provider_binding_digest") != self.provider_binding_digest:
            raise StageError("provider", "backend population receipt is bound to another provider binding")
        raw_receipt = response.get("receipt")
        if not isinstance(raw_receipt, dict):
            raise StageError("provider", "backend population response contains no behavior receipt")
        try:
            receipt = behavior_receipt_from_document(raw_receipt)
        except (KeyError, TypeError, ValueError, PopulationManifestError) as error:
            raise StageError("provider", "backend returned an invalid population behavior receipt") from error
        digest = response.get("receipt_digest")
        if digest != receipt.digest():
            raise StageError("provider", "backend population receipt digest mismatch")
        if receipt.task_id != task.task_id:
            raise StageError("provider", "backend returned a receipt for another population task")
        return receipt

    def _post(self, body: dict[str, Any]) -> dict[str, Any]:
        payload = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        request = urllib.request.Request(
            self.backend_url + "/v1/population/execute",
            data=payload,
            method="POST",
            headers={
                "Authorization": "Bearer " + self.backend_token,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            response = self._open(request, timeout=self.timeout_s)
        except urllib.error.HTTPError as error:
            response = error
        except OSError as error:
            raise StageError(
                "provider",
                f"factory backend unavailable during population execution: {error}",
                retryable=True,
            ) from error
        with response:
            status = int(response.code)
            raw = response.read(MAX_BACKEND_POPULATION_RESPONSE + 1)
        if len(raw) > MAX_BACKEND_POPULATION_RESPONSE:
            raise StageError("provider", "factory backend population response exceeds limit")
        try:
            value = json.loads(raw) if raw else {}
        except ValueError as error:
            raise StageError("provider", "factory backend population route returned invalid JSON") from error
        if not isinstance(value, dict):
            raise StageError("provider", "factory backend population route returned a non-object")
        if status >= 300:
            detail = str(value.get("detail") or f"HTTP {status}")[:500]
            raise StageError(
                "provider",
                f"factory backend population operation refused: {detail}",
                retryable=status >= 500,
            )
        return value
