"""Bounded in-stage execution for provider-bound search populations.

Airflow remains the lifecycle scheduler. This executor is analogous to WorkExecutor: it runs inside
one already-scheduled task, fans out bounded search trajectories, captures immutable behavior
receipts, and reduces them to population telemetry. It has no Cell mutation, publication, approval,
credential, or promotion authority.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from collections.abc import Callable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

from swfactory.models import StageError
from swfactory.population_manifest import (
    BehaviorReceipt,
    PopulationManifest,
    PopulationManifestError,
    PopulationTelemetry,
    behavior_receipt_from_document,
    population_telemetry_from_document,
    summarize_population,
)
from swfactory.provider_binding import BoundPopulationTask, ProviderBindingManifest

POPULATION_EXECUTION_SCHEMA_VERSION = 1
POPULATION_EXECUTION_AUTHORITY = "search-only"


def _digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return "sha256:" + hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class PopulationExecutionPolicy:
    max_parallel: int = 8
    require_complete: bool = True
    require_distinct_independent_verifiers: bool = True

    def validate(self) -> None:
        if self.max_parallel < 1:
            raise PopulationManifestError("population execution max_parallel must be positive")


class PopulationExecutionAbort(StageError):
    """A managed population stage must abort rather than downgrade this boundary failure."""

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__("agent", message, retryable=retryable)


class PopulationCancellation:
    def __init__(self) -> None:
        self._event = threading.Event()

    def cancel(self) -> None:
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()


class PopulationRunner(Protocol):
    def __call__(self, task: BoundPopulationTask) -> BehaviorReceipt: ...


@dataclass(frozen=True)
class PopulationExecutionReport:
    population_manifest_digest: str
    provider_binding_digest: str
    receipts: tuple[BehaviorReceipt, ...]
    telemetry: PopulationTelemetry
    cancelled: bool
    started_tasks: int
    authority: str = POPULATION_EXECUTION_AUTHORITY
    scheduler: str = "airflow"
    schema_version: int = POPULATION_EXECUTION_SCHEMA_VERSION

    def validate(self) -> None:
        if self.schema_version != POPULATION_EXECUTION_SCHEMA_VERSION:
            raise PopulationManifestError("unsupported population execution report schema")
        if self.authority != POPULATION_EXECUTION_AUTHORITY:
            raise PopulationManifestError("population execution reports must remain search-only")
        if self.scheduler != "airflow":
            raise PopulationManifestError("population execution cannot introduce another scheduler")
        if self.started_tasks < 0 or self.started_tasks > self.telemetry.total_tasks:
            raise PopulationManifestError("population execution started-task count is inconsistent")
        if self.population_manifest_digest != self.telemetry.manifest_digest:
            raise PopulationManifestError("population execution telemetry belongs to another manifest")
        if len(self.receipts) != self.telemetry.receipts:
            raise PopulationManifestError("population execution receipt count disagrees with telemetry")

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema_version": self.schema_version,
            "authority": self.authority,
            "scheduler": self.scheduler,
            "population_manifest_digest": self.population_manifest_digest,
            "provider_binding_digest": self.provider_binding_digest,
            "receipts": [receipt.canonical_dict() for receipt in self.receipts],
            "telemetry": self.telemetry.canonical_dict(),
            "cancelled": self.cancelled,
            "started_tasks": self.started_tasks,
        }

    def digest(self) -> str:
        return _digest(self.canonical_dict())


def population_execution_report_from_document(
    document: Mapping[str, Any],
) -> PopulationExecutionReport:
    """Rehydrate one retained managed-population report and re-check every binding."""

    raw_receipts = document.get("receipts")
    raw_telemetry = document.get("telemetry")
    if not isinstance(raw_receipts, list):
        raise PopulationManifestError("population execution receipts must be an array")
    if not isinstance(raw_telemetry, Mapping):
        raise PopulationManifestError("population execution telemetry must be an object")

    receipts = tuple(
        behavior_receipt_from_document(row)
        for row in raw_receipts
        if isinstance(row, Mapping)
    )
    if len(receipts) != len(raw_receipts):
        raise PopulationManifestError("every population execution receipt must be an object")
    report = PopulationExecutionReport(
        population_manifest_digest=str(document["population_manifest_digest"]),
        provider_binding_digest=str(document["provider_binding_digest"]),
        receipts=receipts,
        telemetry=population_telemetry_from_document(raw_telemetry),
        cancelled=bool(document["cancelled"]),
        started_tasks=int(document["started_tasks"]),
        authority=str(document.get("authority", POPULATION_EXECUTION_AUTHORITY)),
        scheduler=str(document.get("scheduler", "airflow")),
        schema_version=int(document.get("schema_version", POPULATION_EXECUTION_SCHEMA_VERSION)),
    )
    report.validate()
    return report


def write_population_execution_report(
    path: Path,
    report: PopulationExecutionReport,
) -> str:
    """Atomically retain a canonical report plus its digest."""

    document = report.canonical_dict()
    digest = report.digest()
    document["report_digest"] = digest
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp.replace(path)
    return digest


def load_population_execution_report(path: Path) -> PopulationExecutionReport:
    """Load and verify a retained population execution report."""

    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise PopulationManifestError("population execution report must be a JSON object")
    expected = raw.get("report_digest")
    report = population_execution_report_from_document(raw)
    if expected is not None and str(expected) != report.digest():
        raise PopulationManifestError("population execution report digest mismatch")
    return report


class PopulationExecutor:
    """Bounded fan-out/fan-in executor for one provider-bound population manifest."""

    def __init__(
        self,
        runner: PopulationRunner,
        policy: PopulationExecutionPolicy | None = None,
    ) -> None:
        self.runner = runner
        self.policy = policy or PopulationExecutionPolicy()
        self.policy.validate()

    def execute(
        self,
        *,
        manifest: PopulationManifest,
        binding: ProviderBindingManifest,
        cancellation: PopulationCancellation | None = None,
    ) -> PopulationExecutionReport:
        manifest.validate()
        binding.validate()
        self._validate_binding(manifest, binding)
        if self.policy.require_distinct_independent_verifiers:
            self._validate_verifier_independence(manifest, binding)

        cancellation = cancellation or PopulationCancellation()
        bound_by_id = {task.task_id: task for task in binding.tasks}
        ordered = tuple(bound_by_id[task.task_id] for task in manifest.tasks)
        receipts_by_id: dict[str, BehaviorReceipt] = {}
        started = 0

        with ThreadPoolExecutor(max_workers=min(self.policy.max_parallel, max(1, len(ordered)))) as pool:
            futures: dict[Future[BehaviorReceipt], BoundPopulationTask] = {}
            for task in ordered:
                if cancellation.cancelled:
                    break
                futures[pool.submit(self._run_one, task)] = task
                started += 1

            for future in as_completed(futures):
                task = futures[future]
                if cancellation.cancelled and future.cancel():
                    continue
                try:
                    receipt = future.result()
                except (PopulationExecutionAbort, PopulationManifestError):
                    cancellation.cancel()
                    for pending in futures:
                        pending.cancel()
                    raise
                except BaseException as exc:
                    receipt = BehaviorReceipt(
                        task_id=task.task_id,
                        state="failed",
                        provider=task.provider,
                        model=task.model,
                        runtime=task.runtime,
                        duration_s=0.0,
                    )
                    # Preserve failure identity without leaking arbitrary exception payloads into
                    # promotion-adjacent evidence. The task id and failed state are enough here.
                    _ = exc
                receipt = self._normalize_receipt(task, receipt)
                receipts_by_id[task.task_id] = receipt

        receipts = tuple(receipts_by_id[task.task_id] for task in ordered if task.task_id in receipts_by_id)
        telemetry = summarize_population(
            manifest,
            receipts,
            require_complete=self.policy.require_complete and not cancellation.cancelled,
        )
        report = PopulationExecutionReport(
            population_manifest_digest=manifest.digest(),
            provider_binding_digest=binding.digest(),
            receipts=receipts,
            telemetry=telemetry,
            cancelled=cancellation.cancelled,
            started_tasks=started,
        )
        report.validate()
        return report

    def _run_one(self, task: BoundPopulationTask) -> BehaviorReceipt:
        started = time.monotonic()
        receipt = self.runner(task)
        elapsed = time.monotonic() - started
        if receipt.duration_s >= elapsed:
            return receipt
        return BehaviorReceipt(
            **{
                **asdict(receipt),
                "duration_s": elapsed,
            }
        )

    @staticmethod
    def _validate_binding(
        manifest: PopulationManifest,
        binding: ProviderBindingManifest,
    ) -> None:
        if binding.population_manifest_digest != manifest.digest():
            raise PopulationManifestError("provider binding belongs to another population manifest")
        manifest_ids = tuple(task.task_id for task in manifest.tasks)
        binding_ids = tuple(task.task_id for task in binding.tasks)
        if binding_ids != manifest_ids:
            raise PopulationManifestError("provider binding task order/identity does not match population manifest")
        variants = {task.task_id: task.variant_digest for task in manifest.tasks}
        for task in binding.tasks:
            if variants[task.task_id] != task.variant_digest:
                raise PopulationManifestError(f"{task.task_id}: provider binding changed variant identity")

    @staticmethod
    def _validate_verifier_independence(
        manifest: PopulationManifest,
        binding: ProviderBindingManifest,
    ) -> None:
        manifest_by_id = {task.task_id: task for task in manifest.tasks}
        seen: dict[tuple[str | None, str | None, str | None, str | None], str] = {}
        for task in binding.tasks:
            source = manifest_by_id[task.task_id]
            if not source.independent_verification:
                continue
            signature = (
                task.provider,
                task.model,
                task.runtime,
                task.verifier_variant,
            )
            previous = seen.get(signature)
            if previous is not None:
                raise PopulationManifestError(
                    f"independent verifier bindings collapsed: {previous} and {task.task_id} share {signature!r}"
                )
            seen[signature] = task.task_id

    @staticmethod
    def _normalize_receipt(
        task: BoundPopulationTask,
        receipt: BehaviorReceipt,
    ) -> BehaviorReceipt:
        receipt.validate()
        if receipt.task_id != task.task_id:
            raise PopulationManifestError("population runner returned a receipt for another task")

        expected: Mapping[str, str | None] = {
            "provider": task.provider,
            "model": task.model,
            "runtime": task.runtime,
        }
        observed: Mapping[str, str | None] = {
            "provider": receipt.provider,
            "model": receipt.model,
            "runtime": receipt.runtime,
        }
        for field in expected:
            if observed[field] is not None and observed[field] != expected[field]:
                raise PopulationManifestError(
                    f"{task.task_id}: receipt {field} {observed[field]!r} does not match bound {expected[field]!r}"
                )

        return BehaviorReceipt(
            task_id=receipt.task_id,
            state=receipt.state,
            behavior_signature=receipt.behavior_signature,
            candidate_digest=receipt.candidate_digest,
            evidence_digest=receipt.evidence_digest,
            provider=expected["provider"],
            model=expected["model"],
            runtime=expected["runtime"],
            cost_usd=receipt.cost_usd,
            duration_s=receipt.duration_s,
        )


def execute_population(
    manifest: PopulationManifest,
    binding: ProviderBindingManifest,
    runner: Callable[[BoundPopulationTask], BehaviorReceipt],
    *,
    max_parallel: int = 8,
    require_complete: bool = True,
    require_distinct_independent_verifiers: bool = True,
    cancellation: PopulationCancellation | None = None,
) -> PopulationExecutionReport:
    """Convenience entry point for one already-scheduled Airflow task."""

    return PopulationExecutor(
        runner,
        PopulationExecutionPolicy(
            max_parallel=max_parallel,
            require_complete=require_complete,
            require_distinct_independent_verifiers=require_distinct_independent_verifiers,
        ),
    ).execute(
        manifest=manifest,
        binding=binding,
        cancellation=cancellation,
    )
