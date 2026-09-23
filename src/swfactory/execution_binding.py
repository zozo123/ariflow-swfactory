"""Level-1 seam between sandbox capability truth and bounded Plan.work execution.

The lifecycle scheduler remains Airflow.  This seam selects a provider from explicit capability
facts and tells the in-stage executor whether lineage-preserving parallelism is safe.  It never
creates a second scheduling authority.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

from swfactory.backend_population import BackendPopulationRunner, PopulationTaskInput
from swfactory.population_execution import (
    PopulationCancellation,
    PopulationExecutionPolicy,
    PopulationExecutionReport,
    PopulationExecutor,
    write_population_execution_report,
)
from swfactory.population_manifest import PopulationManifest
from swfactory.provider_binding import ProviderBindingManifest
from swfactory.sandbox_contract import CapabilityRequirement, ProviderDocument, select_provider
from swfactory.work_executor import Cancellation, ExecutionReport, WorkExecutor
from swfactory.workgraph import WorkNode, conflict_set


@dataclass(frozen=True)
class ExecutionDecision:
    provider: str
    parallel: bool
    reason: str
    required_capabilities: tuple[str, ...]


def choose_execution(
    nodes: Iterable[WorkNode],
    providers: Iterable[ProviderDocument],
    *,
    preferred: Iterable[str] = (),
    require_network_policy: bool = False,
) -> tuple[ProviderDocument, ExecutionDecision]:
    ordered = tuple(nodes)
    has_parallel_wave = any(node.parallel_safe for node in ordered) and len(ordered) > 1
    conflicts = conflict_set(ordered)
    want_fork = has_parallel_wave and not conflicts
    requirement = CapabilityRequirement(
        fork=want_fork,
        network_policy=require_network_policy,
        filesystem_isolation=True,
        exact_teardown=True,
    )
    try:
        provider = select_provider(providers, requirement, preferred=preferred)
        parallel = want_fork and provider.capabilities.fork
        reason = "capabilities_allow_parallel" if parallel else "serial_by_graph"
    except ValueError:
        # Fork is an optimization.  If no provider can preserve fork lineage, retry selection for
        # the same graph in deterministic serial mode rather than inventing provider behavior.
        serial_requirement = CapabilityRequirement(
            fork=False,
            network_policy=require_network_policy,
            filesystem_isolation=True,
            exact_teardown=True,
        )
        provider = select_provider(providers, serial_requirement, preferred=preferred)
        parallel = False
        reason = "serial_fallback_missing_fork"
    required = tuple(name for name, needed in requirement.__dict__.items() if bool(needed))
    return provider, ExecutionDecision(provider.provider, parallel, reason, required)


def execute_bound_work(
    executor: WorkExecutor,
    *,
    cell_id: str,
    epoch: int,
    input_head: str,
    nodes: Iterable[WorkNode],
    provider: ProviderDocument,
    decision: ExecutionDecision,
    cancellation: Cancellation | None = None,
) -> ExecutionReport:
    if decision.provider != provider.provider:
        raise ValueError("execution decision/provider mismatch")
    return executor.execute(
        cell_id=cell_id,
        epoch=epoch,
        input_head=input_head,
        nodes=tuple(nodes),
        supports_fork=decision.parallel and provider.capabilities.fork,
        cancellation=cancellation,
    )


def execute_bound_population(
    executor: PopulationExecutor,
    *,
    manifest: PopulationManifest,
    binding: ProviderBindingManifest,
    cancellation: PopulationCancellation | None = None,
) -> PopulationExecutionReport:
    """Execute one search-only population inside an already-scheduled Airflow lifecycle task."""

    if binding.population_manifest_digest != manifest.digest():
        raise ValueError("population execution binding/manifest mismatch")
    return executor.execute(
        manifest=manifest,
        binding=binding,
        cancellation=cancellation,
    )


def execute_managed_population(
    *,
    manifest: PopulationManifest,
    binding: ProviderBindingManifest,
    inputs: Mapping[str, PopulationTaskInput],
    cell_id: str,
    epoch: int,
    policy_digest: str,
    env: Mapping[str, str] | None = None,
    max_parallel: int = 8,
    report_path: Path | None = None,
    cancellation: PopulationCancellation | None = None,
) -> PopulationExecutionReport:
    """Run a provider-bound population through the trusted backend from one Airflow stage.

    The Airflow worker receives only the factory backend URL/token. Provider credentials stay on the
    backend and are projected there per population task.
    """

    environment = os.environ if env is None else env
    backend_url = environment.get("SWF_BACKEND_URL", "")
    backend_token = environment.get("SWF_BACKEND_TOKEN", "")
    runner = BackendPopulationRunner(
        backend_url=backend_url,
        backend_token=backend_token,
        cell_id=cell_id,
        epoch=epoch,
        policy_digest=policy_digest,
        population_manifest_digest=manifest.digest(),
        provider_binding_digest=binding.digest(),
        inputs=dict(inputs),
    )
    executor = PopulationExecutor(
        runner,
        PopulationExecutionPolicy(
            max_parallel=max_parallel,
            require_complete=True,
            require_distinct_independent_verifiers=True,
        ),
    )
    report = execute_bound_population(
        executor,
        manifest=manifest,
        binding=binding,
        cancellation=cancellation,
    )
    if report_path is not None:
        write_population_execution_report(report_path, report)
    return report
