"""Cell-scoped evidence, tracing, cost and SLO contracts.

High-cardinality truth lives in durable evidence. Aggregate telemetry stays deliberately bounded.
The writer accepts a redactor dependency so the evidence leaf can remain independent until the
security and evidence streams fan in.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from swfactory.evidence_store import EvidenceEvent, EvidenceStore

Redactor = Callable[[Any], Any]


def cell_trace_id(cell_id: str, epoch: int) -> str:
    raw = f"trace\0{cell_id}\0{epoch}".encode()
    return hashlib.sha256(raw).hexdigest()[:32]


def span_id(trace_id: str, *parts: str) -> str:
    raw = "\0".join((trace_id, *parts)).encode()
    return hashlib.sha256(raw).hexdigest()[:16]


@dataclass(frozen=True)
class EvidenceContext:
    cell_id: str
    epoch: int
    policy_digest: str | None = None
    trace_id: str | None = None
    generation: str | None = None

    def resolved_trace_id(self) -> str:
        return self.trace_id or cell_trace_id(self.cell_id, self.epoch)


class CellEvidenceWriter:
    def __init__(
        self,
        store: EvidenceStore,
        context: EvidenceContext,
        *,
        redact: Redactor | None = None,
    ) -> None:
        self.store = store
        self.context = context
        self.redact = redact or (lambda value: value)

    def event(self, kind: str, data: Mapping[str, Any], *, span: Iterable[str] = ()) -> Path:
        trace = self.context.resolved_trace_id()
        payload = {
            "schema_version": 1,
            "trace_id": trace,
            "span_id": span_id(trace, kind, *tuple(span)),
            "policy_digest": self.context.policy_digest,
            "generation": self.context.generation,
            "data": self.redact(dict(data)),
        }
        return self.store.append(
            EvidenceEvent(
                cell_id=self.context.cell_id,
                epoch=self.context.epoch,
                kind=kind,
                at=time.time(),
                data=payload,
            )
        )

    def manifest(self, **metadata: Any) -> dict[str, Any]:
        return self.store.manifest(
            self.context.cell_id,
            epoch=self.context.epoch,
            policy_digest=self.context.policy_digest,
            trace_id=self.context.resolved_trace_id(),
            generation=self.context.generation,
            **self.redact(metadata),
        )


@dataclass(frozen=True)
class CostEvent:
    cell_id: str
    epoch: int
    source: str
    quantity: float
    unit: str
    amount: float
    currency: str = "USD"
    stage: str | None = None
    node_id: str | None = None
    generation: str | None = None
    at: float = field(default_factory=time.time)
    schema_version: int = 1


class CostLedger:
    def __init__(self, root: Path):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)

    def append(self, event: CostEvent) -> Path:
        if event.quantity < 0 or event.amount < 0:
            raise ValueError("cost quantity and amount must be non-negative")
        path = self.root / event.cell_id / "cost.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(asdict(event), sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
        return path

    def total(self, cell_id: str, *, currency: str = "USD") -> float:
        path = self.root / cell_id / "cost.jsonl"
        if not path.exists():
            return 0.0
        total = 0.0
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line:
                continue
            event = json.loads(line)
            if event.get("currency") == currency:
                total += float(event.get("amount", 0.0))
        return round(total, 8)


@dataclass(frozen=True)
class SLODefinition:
    name: str
    description: str
    success_event: str
    start_event: str
    objective: float
    latency_s: float | None = None
    schema_version: int = 1


DEFAULT_SLOS: tuple[SLODefinition, ...] = (
    SLODefinition(
        "dispatch",
        "admitted cells obtain an authoritative Airflow run binding",
        "airflow_bound",
        "admitted",
        0.995,
        60.0,
    ),
    SLODefinition(
        "completion",
        "started cells reach a terminal governed outcome",
        "cell_terminal",
        "airflow_bound",
        0.99,
        7200.0,
    ),
    SLODefinition(
        "publication",
        "approved verified cells publish or expose actionable repair debt",
        "publication_terminal",
        "delivery_started",
        0.995,
        300.0,
    ),
    SLODefinition(
        "cleanup",
        "terminal cells converge provider compute to absent",
        "cleanup_converged",
        "cell_terminal",
        0.999,
        600.0,
    ),
    SLODefinition(
        "recovery",
        "in-doubt external mutations become committed/refused/divergent with evidence",
        "reconciliation_terminal",
        "mutation_in_doubt",
        0.99,
        900.0,
    ),
)


ALLOWED_METRIC_LABELS = frozenset(
    {
        "repo",
        "blueprint",
        "provider",
        "generation",
        "state",
        "reason",
        "priority",
        "operation_kind",
        "slo",
    }
)
FORBIDDEN_CARDINALITY_LABELS = frozenset(
    {"cell_id", "run_id", "operation_key", "node_id", "sandbox_id", "issue", "trace_id", "span_id"}
)


def validate_metric_labels(labels: Mapping[str, str]) -> None:
    forbidden = set(labels) & FORBIDDEN_CARDINALITY_LABELS
    unknown = set(labels) - ALLOWED_METRIC_LABELS
    if forbidden:
        raise ValueError(f"high-cardinality metric labels are forbidden: {sorted(forbidden)}")
    if unknown:
        raise ValueError(f"unknown metric labels: {sorted(unknown)}")


def repair_debt_summary(rows: Iterable[Mapping[str, Any]], *, now: float | None = None) -> dict[str, Any]:
    now = time.time() if now is None else now
    counts: dict[str, int] = {}
    oldest = 0.0
    total = 0
    for row in rows:
        total += 1
        kind = str(row.get("kind") or "unknown")
        counts[kind] = counts.get(kind, 0) + 1
        updated = float(row.get("updated_at") or now)
        oldest = max(oldest, max(0.0, now - updated))
    return {"count": total, "oldest_age_s": oldest, "by_kind": dict(sorted(counts.items()))}


def checkpoint_manifest(manifest: Mapping[str, Any], previous_digest: str | None = None) -> dict[str, Any]:
    canonical = json.dumps(manifest, sort_keys=True, separators=(",", ":"), allow_nan=False)
    digest = hashlib.sha256(
        ((previous_digest or "") + "\0" + canonical).encode()
    ).hexdigest()
    return {
        "schema_version": 1,
        "previous_digest": previous_digest,
        "manifest_sha256": hashlib.sha256(canonical.encode()).hexdigest(),
        "chain_digest": digest,
    }


def offline_bundle_manifest(
    *,
    cell_id: str,
    epoch: int,
    files: Iterable[Path],
    root: Path,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    entries = []
    for path in sorted(files, key=lambda value: str(value)):
        data = path.read_bytes()
        entries.append(
            {
                "path": str(path.relative_to(root)),
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        )
    return {
        "schema_version": 1,
        "cell_id": cell_id,
        "epoch": epoch,
        "metadata": dict(metadata or {}),
        "files": entries,
    }
