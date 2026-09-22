"""Backend-owned managed population provider execution.

Airflow workers may carry search-only population task identity and instruction bytes, but they never
receive provider credentials. This module validates the current Factory Cell/Airflow/policy binding,
journals the paid provider call through the canonical core capability runtime, projects a short-lived
credential lease only inside this backend process, retains provider output in a content-addressed
host store, and returns only sanitized receipt metadata.
"""

from __future__ import annotations

from typing import Any

from swfactory.authority import ResourceKind
from swfactory.core_capabilities import CoreMutationRequest
from swfactory.credential_lease import LeaseBinding
from swfactory.liquid_security_runtime import Capability, SecurityContext
from swfactory.population_adapter import (
    PopulationInvocation,
    population_adapter_identity,
)
from swfactory.population_manifest import BehaviorReceipt, PopulationManifestError
from swfactory.provider_binding import bound_population_task_from_document

from .core_service import airflow_binding, ensure_core, intent_digest
from .service import Factory, Refused, text


def operation(factory: Factory, path: str, body: dict[str, Any]) -> Any:
    if path != "/population/execute":
        raise Refused(404, "unknown backend population operation")
    return _execute(factory, body)


def _managed_identity(
    factory: Factory,
    body: dict[str, Any],
) -> tuple[dict[str, Any], str, int, str, str]:
    cell_id = text(body, "cell_id")
    epoch = body.get("epoch")
    if type(epoch) is not int or epoch < 1:
        raise ValueError("epoch must be a positive integer")
    policy_digest = text(body, "policy_digest")
    operation_key = text(body, "operation_key", max_len=256)
    cell = factory._cell(cell_id)
    if int(cell["epoch"]) != epoch:
        raise Refused(409, f"stale Factory Cell epoch {epoch}; current epoch is {cell['epoch']}")
    if cell.get("policy_digest") != policy_digest:
        raise Refused(409, "Factory Cell policy digest changed; population call is stale")
    airflow_binding(factory, cell_id, epoch)
    ensure_core(factory)
    return cell, cell_id, epoch, policy_digest, operation_key


def _invocation(
    body: dict[str, Any],
) -> PopulationInvocation:
    raw_task = body.get("task")
    if not isinstance(raw_task, dict):
        raise ValueError("task must be a bound population task object")
    task = bound_population_task_from_document(raw_task)
    context = body.get("context_artifact_digests") or []
    if not isinstance(context, list) or any(not isinstance(value, str) for value in context):
        raise ValueError("context_artifact_digests must be an array of strings")
    objective = body.get("objective_digest")
    if objective is not None and not isinstance(objective, str):
        raise ValueError("objective_digest must be a string or null")
    invocation = PopulationInvocation(
        task=task,
        population_manifest_digest=text(body, "population_manifest_digest", max_len=80),
        provider_binding_digest=text(body, "provider_binding_digest", max_len=80),
        instruction=text(body, "instruction", max_len=256 * 1024),
        context_artifact_digests=tuple(context),
        objective_digest=objective,
    )
    invocation.validate()
    return invocation


def _lease_binding(
    factory: Factory,
    *,
    cell: dict[str, Any],
    operation_key: str,
    invocation: PopulationInvocation,
) -> LeaseBinding:
    try:
        row = factory.control.operations.get(operation_key)
        attempt = max(1, int(row.get("attempts") or 0))
    except KeyError:
        attempt = 1
    airflow = airflow_binding(factory, str(cell["cell_id"]), int(cell["epoch"]))
    sandbox_id = ":".join(
        value
        for value in (
            invocation.task.provider or "provider",
            invocation.task.runtime or "runtime",
            invocation.task.task_id,
        )
        if value
    )[:512]
    return LeaseBinding(
        factory_run_id=f"{airflow.dag_id}:{airflow.run_id}:{airflow.map_index if airflow.map_index is not None else 0}",
        dag_run_id=airflow.run_id,
        task_instance_id=(
            f"job[{airflow.map_index if airflow.map_index is not None else 0}].population.{invocation.task.task_id}"
        ),
        stage_id="population",
        sandbox_id=sandbox_id,
        attempt_number=attempt,
        cell_id=airflow.cell_id,
        epoch=airflow.epoch,
        operation_key=operation_key,
        policy_digest=str(cell["policy_digest"]),
    )


def _execute(factory: Factory, body: dict[str, Any]) -> dict[str, Any]:
    cell, cell_id, epoch, policy_digest, operation_key = _managed_identity(factory, body)
    invocation = _invocation(body)
    provider = invocation.task.provider
    if provider is None:
        raise Refused(400, "managed population execution requires a concrete bound provider")
    adapter = factory.population_adapters.get(provider)
    if adapter is None:
        raise Refused(503, f"population provider {provider!r} is not configured on the backend")

    adapter_digest = population_adapter_identity(adapter)
    invocation_digest = invocation.digest()
    request_intent = intent_digest(
        {
            "kind": "population_model_call",
            "invocation_digest": invocation_digest,
            "adapter_digest": adapter_digest,
            "population_manifest_digest": invocation.population_manifest_digest,
            "provider_binding_digest": invocation.provider_binding_digest,
            "task_id": invocation.task.task_id,
        }
    )
    tenant = factory.repo or str(cell.get("repo") or "factory")
    request = CoreMutationRequest(
        airflow=airflow_binding(factory, cell_id, epoch),
        security=SecurityContext(
            tenant=tenant,
            cell_id=cell_id,
            epoch=epoch,
            role="population-provider",
            capabilities=frozenset({Capability.USE_MODEL}),
        ),
        resource=ResourceKind.MODEL_CALL,
        capability=Capability.USE_MODEL,
        actor="python-backend",
        kind="population_model_call",
        expected_policy_digest=policy_digest,
        target_tenant=tenant,
        parts=(invocation.task.task_id, invocation_digest, adapter_digest),
        replay_safe=False,
        external_operation_key=operation_key,
        intent_digest=request_intent,
    )

    def provider_call() -> dict[str, Any]:
        credential: str | None = None
        handle = None
        capability = adapter.credential_capability
        if capability is not None:
            binding = _lease_binding(
                factory,
                cell=cell,
                operation_key=operation_key,
                invocation=invocation,
            )
            factory.leases.revoke_prior_attempts(
                binding.cell_id,
                binding.epoch,
                binding.attempt_number,
            )
            handle = factory.leases.mint(
                binding,
                capability=capability,
                purpose="population-search",
                ttl_s=900.0,
            )
            credential = factory.leases.redeem(
                handle,
                binding,
                process_nonce=factory.lease_process_nonce,
            )

        try:
            result = adapter.execute(invocation, credential=credential)
            result.validate()
        finally:
            credential = None
            if handle is not None:
                factory.leases.revoke(handle.lease_id)

        candidate_digest = None
        if result.output:
            candidate_digest, _path = factory.population_artifacts.retain(
                invocation_digest=invocation_digest,
                output=result.output,
            )
        receipt = BehaviorReceipt(
            task_id=invocation.task.task_id,
            state=result.state,
            behavior_signature=result.behavior_signature,
            candidate_digest=candidate_digest,
            evidence_digest=result.evidence_digest,
            provider=invocation.task.provider,
            model=invocation.task.model,
            runtime=invocation.task.runtime,
            cost_usd=result.cost_usd,
            duration_s=result.duration_s,
        )
        receipt.validate()
        return {
            "schema_version": 1,
            "authority": "search-only",
            "task_id": invocation.task.task_id,
            "invocation_digest": invocation_digest,
            "adapter_digest": adapter_digest,
            "population_manifest_digest": invocation.population_manifest_digest,
            "provider_binding_digest": invocation.provider_binding_digest,
            "candidate_artifact_digest": candidate_digest,
            "receipt": receipt.canonical_dict(),
            "receipt_digest": receipt.digest(),
        }

    outcome = factory.control.mutate_core(request, provider_call)
    result = outcome.result
    if not isinstance(result, dict):
        raise PopulationManifestError("population model call journal returned an invalid receipt")
    return {
        **result,
        "operation_key": outcome.operation_key,
        "evidence_digest": outcome.evidence_digest,
        "replayed": outcome.replayed,
    }
