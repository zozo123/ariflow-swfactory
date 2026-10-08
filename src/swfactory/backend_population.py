"""Airflow-worker client for backend-owned population provider execution.

The worker knows the exact search invocation and the factory backend bearer token. It never knows the
provider/model credential. Every call is routed to /v1/population/execute, where the trusted
backend performs lease projection, provider execution, artifact retention, journaling, and evidence.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from swfactory.backend_http import ResponseTooLarge, no_redirect_open, post_json, valid_backend_token
from swfactory.models import StageError
from swfactory.population_adapter import PopulationInvocation
from swfactory.population_execution import PopulationExecutionAbort
from swfactory.population_manifest import (
    BehaviorReceipt,
    PopulationManifestError,
    behavior_receipt_from_document,
)
from swfactory.provider_binding import BoundPopulationTask
from swfactory.webhook import _safe_backend_base

MAX_BACKEND_POPULATION_RESPONSE = 2 * 1024 * 1024


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
    ) -> None:
        try:
            self.backend_url = _safe_backend_base(backend_url)
        except ValueError as error:
            raise StageError("policy", f"managed population backend URL is invalid: {error}") from error
        if not valid_backend_token(backend_token):
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
        self._candidate_artifacts: dict[str, tuple[str, str]] = {}

    def _invocation_and_key(self, task: BoundPopulationTask) -> tuple[PopulationInvocation, str]:
        """The exact invocation for ``task`` and the operation key a fresh process re-derives from it."""
        try:
            task_input = self.inputs[task.task_id]
        except KeyError as error:
            raise PopulationExecutionAbort(
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
        operation_key = (
            "population_model_call:"
            + hashlib.sha256((task.task_id + "\0" + invocation.digest()).encode()).hexdigest()[:24]
        )
        return invocation, operation_key

    def __call__(self, task: BoundPopulationTask) -> BehaviorReceipt:
        invocation, operation_key = self._invocation_and_key(task)
        body = {
            "cell_id": self.cell_id,
            "epoch": self.epoch,
            "policy_digest": self.policy_digest,
            "operation_key": operation_key,
            "population_manifest_digest": self.population_manifest_digest,
            "provider_binding_digest": self.provider_binding_digest,
            "task": task.canonical_dict() | {"binding_digest": task.binding_digest},
            "instruction": invocation.instruction,
            "context_artifact_digests": list(invocation.context_artifact_digests),
            "objective_digest": invocation.objective_digest,
        }
        response = self._post("/population/execute", body)
        if response.get("invocation_digest") != invocation.digest():
            raise PopulationExecutionAbort("backend population receipt is bound to another invocation")
        if response.get("population_manifest_digest") != self.population_manifest_digest:
            raise PopulationExecutionAbort("backend population receipt is bound to another manifest")
        if response.get("provider_binding_digest") != self.provider_binding_digest:
            raise PopulationExecutionAbort("backend population receipt is bound to another provider binding")
        raw_receipt = response.get("receipt")
        if not isinstance(raw_receipt, dict):
            raise PopulationExecutionAbort("backend population response contains no behavior receipt")
        try:
            receipt = behavior_receipt_from_document(raw_receipt)
        except (KeyError, TypeError, ValueError, PopulationManifestError) as error:
            raise PopulationExecutionAbort("backend returned an invalid population behavior receipt") from error
        digest = response.get("receipt_digest")
        if digest != receipt.digest():
            raise PopulationExecutionAbort("backend population receipt digest mismatch")
        if receipt.task_id != task.task_id:
            raise PopulationExecutionAbort("backend returned a receipt for another population task")
        artifact_digest = response.get("candidate_artifact_digest")
        if artifact_digest is not None:
            if not isinstance(artifact_digest, str) or not artifact_digest.startswith("sha256:"):
                raise PopulationExecutionAbort("backend returned an invalid candidate artifact digest")
            self._candidate_artifacts[task.task_id] = (operation_key, artifact_digest)
        return receipt

    def candidate_excerpt(
        self,
        task: BoundPopulationTask,
        *,
        artifact_digest: str | None = None,
        max_chars: int = 8192,
    ) -> str:
        """Read a bounded excerpt only from the committed operation that produced this task.

        A fresh process can reconstruct the operation key from the exact task + immutable input and
        use the artifact digest retained in the BehaviorReceipt. This makes Airflow task retries
        read committed search evidence instead of re-running the provider.
        """

        cached = self._candidate_artifacts.get(task.task_id)
        if cached is not None:
            operation_key, cached_digest = cached
            if artifact_digest is not None and artifact_digest != cached_digest:
                raise PopulationExecutionAbort(
                    f"population task {task.task_id} artifact digest changed across one runner"
                )
            artifact_digest = cached_digest
        else:
            if artifact_digest is None:
                raise PopulationExecutionAbort(f"population task {task.task_id} has no retained candidate artifact")
            _, operation_key = self._invocation_and_key(task)

        value = self._post(
            "/population/artifact",
            {
                "cell_id": self.cell_id,
                "epoch": self.epoch,
                "policy_digest": self.policy_digest,
                "operation_key": operation_key,
                "artifact_digest": artifact_digest,
                "max_chars": max_chars,
            },
        )
        if value.get("artifact_digest") != artifact_digest:
            raise PopulationExecutionAbort("backend returned another population artifact")
        excerpt = value.get("output_excerpt")
        if not isinstance(excerpt, str):
            raise PopulationExecutionAbort("backend population artifact has no text excerpt")
        return excerpt

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        try:
            status, value = post_json(
                self.backend_url,
                self.backend_token,
                "/v1" + path,
                body,
                timeout=self.timeout_s,
                limit=MAX_BACKEND_POPULATION_RESPONSE,
                opener=no_redirect_open,
            )
        except ResponseTooLarge:
            raise PopulationExecutionAbort("factory backend population response exceeds limit") from None
        except ValueError as error:
            raise PopulationExecutionAbort("factory backend population route returned invalid JSON") from error
        except OSError as error:
            raise PopulationExecutionAbort(
                f"factory backend unavailable during population execution: {error}",
                retryable=True,
            ) from error
        if status >= 300:
            raise PopulationExecutionAbort(
                f"factory backend population operation refused: {value[:500]}",
                retryable=status >= 500,
            )
        if not isinstance(value, dict):
            raise PopulationExecutionAbort("factory backend population route returned a non-object")
        return value
