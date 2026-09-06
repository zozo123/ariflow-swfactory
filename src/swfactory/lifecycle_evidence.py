"""Cell-scoped lifecycle evidence, deterministic tracing and cost ledger.

This is the single write facade lifecycle code should converge on.  High-cardinality identifiers live
in append-only evidence, not aggregate metric labels.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from swfactory.security_boundary import redact


@dataclass(frozen=True)
class TraceContext:
    trace_id: str
    span_id: str

    @classmethod
    def for_cell(cls, cell_id: str, epoch: int, kind: str, identity: str = "") -> TraceContext:
        trace = hashlib.sha256(f"trace\0{cell_id}\0{epoch}".encode()).hexdigest()[:32]
        span = hashlib.sha256(f"span\0{trace}\0{kind}\0{identity}".encode()).hexdigest()[:16]
        return cls(trace, span)

    def child(self, kind: str, identity: str = "") -> TraceContext:
        span = hashlib.sha256(
            f"span\0{self.trace_id}\0{self.span_id}\0{kind}\0{identity}".encode()
        ).hexdigest()[:16]
        return TraceContext(self.trace_id, span)


@dataclass(frozen=True)
class EvidenceEnvelope:
    schema_version: int
    cell_id: str
    epoch: int
    policy_digest: str | None
    trace_id: str
    span_id: str
    kind: str
    payload: dict[str, Any]
    at: float


@dataclass(frozen=True)
class CostEvent:
    schema_version: int
    cell_id: str
    epoch: int
    source: str
    quantity: float
    unit: str
    currency: str | None
    amount: float | None
    stage: str | None
    node: str | None
    generation: str | None
    at: float


@dataclass(frozen=True)
class SLOContract:
    name: str
    population: str
    success: str
    latency_s: float | None = None
    schema_version: int = 1


DEFAULT_SLOS: tuple[SLOContract, ...] = (
    SLOContract("dispatch", "accepted work", "authoritative Airflow run bound", 30.0),
    SLOContract("completion", "started cells", "terminal lifecycle state", 3600.0),
    SLOContract("publication", "verified candidates", "PR publication converged", 120.0),
    SLOContract("cleanup", "terminal cells with compute", "provider resource absent", 300.0),
    SLOContract("recovery", "in-doubt operations", "reconciled or operator-visible", 300.0),
)


class EvidenceWriter:
    """Append-only, fsync-backed cell evidence with hash chaining and artifact manifests."""

    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def append(
        self,
        *,
        cell_id: str,
        epoch: int,
        kind: str,
        payload: Mapping[str, Any],
        policy_digest: str | None = None,
        trace: TraceContext | None = None,
    ) -> dict[str, Any]:
        if epoch < 1:
            raise ValueError("evidence epoch must be positive")
        trace = trace or TraceContext.for_cell(cell_id, epoch, kind)
        safe_payload = redact(dict(payload))
        envelope = EvidenceEnvelope(
            1,
            cell_id,
            epoch,
            policy_digest,
            trace.trace_id,
            trace.span_id,
            kind,
            safe_payload,
            time.time(),
        )
        path = self._events_path(cell_id)
        previous = self._tail_digest(path)
        body = asdict(envelope)
        canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False)
        digest = hashlib.sha256((previous + "\0" + canonical).encode()).hexdigest()
        record = {**body, "previous_digest": previous or None, "digest": digest}
        self._append_jsonl(path, record)
        return record

    def artifact(
        self,
        *,
        cell_id: str,
        epoch: int,
        logical_name: str,
        path: Path,
        policy_digest: str | None = None,
        trace: TraceContext | None = None,
    ) -> dict[str, Any]:
        data = path.read_bytes()
        sha = hashlib.sha256(data).hexdigest()
        return self.append(
            cell_id=cell_id,
            epoch=epoch,
            kind="artifact",
            policy_digest=policy_digest,
            trace=trace,
            payload={
                "logical_name": logical_name,
                "path": str(path),
                "sha256": sha,
                "bytes": len(data),
            },
        )

    def cost(self, event: CostEvent, *, policy_digest: str | None = None) -> dict[str, Any]:
        if event.quantity < 0 or (event.amount is not None and event.amount < 0):
            raise ValueError("cost values must be non-negative")
        return self.append(
            cell_id=event.cell_id,
            epoch=event.epoch,
            kind="cost",
            policy_digest=policy_digest,
            trace=TraceContext.for_cell(event.cell_id, event.epoch, "cost", event.source),
            payload=asdict(event),
        )

    def checkpoint(self, cell_id: str) -> dict[str, Any]:
        events = self.read(cell_id)
        digest = events[-1]["digest"] if events else hashlib.sha256(b"").hexdigest()
        checkpoint = {
            "schema_version": 1,
            "cell_id": cell_id,
            "events": len(events),
            "tail_digest": digest,
            "created_at": time.time(),
        }
        path = self.root / cell_id / "checkpoint.json"
        self._atomic_json(path, checkpoint)
        return checkpoint

    def bundle_manifest(self, cell_id: str, extra_paths: Iterable[Path] = ()) -> dict[str, Any]:
        directory = self.root / cell_id
        files: list[dict[str, Any]] = []
        candidates = list(directory.glob("**/*")) if directory.exists() else []
        candidates += list(extra_paths)
        seen: set[str] = set()
        for path in sorted((p for p in candidates if p.is_file()), key=lambda p: str(p)):
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            data = path.read_bytes()
            files.append(
                {"path": str(path), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
            )
        manifest = {
            "schema_version": 1,
            "cell_id": cell_id,
            "files": files,
            "manifest_digest": hashlib.sha256(
                json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        }
        self._atomic_json(directory / "bundle-manifest.json", manifest)
        return manifest

    def read(self, cell_id: str) -> list[dict[str, Any]]:
        path = self._events_path(cell_id)
        if not path.is_file():
            return []
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
        return rows

    def verify(self, cell_id: str) -> tuple[bool, str]:
        previous = ""
        for index, record in enumerate(self.read(cell_id)):
            body = {k: v for k, v in record.items() if k not in {"previous_digest", "digest"}}
            canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False)
            expected = hashlib.sha256((previous + "\0" + canonical).encode()).hexdigest()
            if record.get("previous_digest") != (previous or None):
                return False, f"event {index}: previous digest mismatch"
            if record.get("digest") != expected:
                return False, f"event {index}: digest mismatch"
            previous = expected
        return True, previous

    def _events_path(self, cell_id: str) -> Path:
        directory = self.root / cell_id
        directory.mkdir(parents=True, exist_ok=True)
        return directory / "events.jsonl"

    @staticmethod
    def _tail_digest(path: Path) -> str:
        if not path.is_file():
            return ""
        last = ""
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    last = line
        return str(json.loads(last).get("digest") or "") if last else ""

    @staticmethod
    def _append_jsonl(path: Path, value: Mapping[str, Any]) -> None:
        payload = (
            json.dumps(dict(value), sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
        )
        with path.open("a", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())

    @staticmethod
    def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(dict(value), sort_keys=True, indent=2) + "\n", encoding="utf-8")
        os.replace(tmp, path)


ALLOWED_METRIC_LABELS = frozenset(
    {"repo", "blueprint", "generation", "provider", "stage", "state", "kind", "result"}
)


def validate_metric_labels(labels: Mapping[str, str]) -> None:
    forbidden = set(labels) - ALLOWED_METRIC_LABELS
    if forbidden:
        raise ValueError(
            f"high-cardinality/unknown metric labels are forbidden: {sorted(forbidden)}"
        )
    for key, value in labels.items():
        lower = value.lower()
        if key in {
            "repo",
            "blueprint",
            "generation",
            "provider",
            "stage",
            "state",
            "kind",
            "result",
        }:
            if "cell_" in lower or "run_" in lower or len(value) > 128:
                raise ValueError(f"metric label {key} appears high-cardinality")
