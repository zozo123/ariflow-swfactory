"""Level-1 seam between sandbox capability truth and bounded Plan.work execution.

The lifecycle scheduler remains Airflow.  This seam selects a provider from explicit capability
facts and tells the in-stage executor whether lineage-preserving parallelism is safe.  It never
creates a second scheduling authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

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
    required = tuple(
        name
        for name, needed in requirement.__dict__.items()
        if bool(needed)
    )
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
