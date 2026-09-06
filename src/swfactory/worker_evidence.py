"""Durable evidence ledger for bounded seven-worker fan-out/fan-in batches."""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

_ALLOWED_STATES = {"success", "failed", "cancelled"}
_ALLOWED_ROLES = {
    "authority",
    "airflow",
    "workgraph",
    "recovery",
    "security",
    "evidence",
    "operator",
}


class WorkerEvidenceSink:
    """Append-only worker receipts plus an atomically replaced batch manifest."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def append_batch(
        self,
        *,
        cell_id: str,
        epoch: int,
        batch_id: str,
        receipts: Iterable[Mapping[str, Any]],
    ) -> dict[str, Any]:
        if not cell_id.startswith("cell_") or epoch < 1:
            raise ValueError("worker evidence requires a valid Cell id and positive epoch")
        if not batch_id.strip() or len(batch_id) > 256:
            raise ValueError("worker batch id must be nonempty and at most 256 characters")

        normalized = tuple(self._normalize(receipt) for receipt in receipts)
        normalized = tuple(sorted(normalized, key=lambda row: (row["role"], row["task_id"])))
        cell = self.root / cell_id
        cell.mkdir(parents=True, exist_ok=True)
        timeline = cell / "workers.jsonl"
        written_at = time.time()
        with timeline.open("a", encoding="utf-8") as handle:
            for receipt in normalized:
                event = {
                    "schema_version": 1,
                    "cell_id": cell_id,
                    "epoch": epoch,
                    "batch_id": batch_id,
                    "recorded_at": written_at,
                    "receipt": receipt,
                }
                handle.write(
                    json.dumps(event, sort_keys=True, separators=(",", ":"), allow_nan=False)
                    + "\n"
                )
            handle.flush()
            os.fsync(handle.fileno())

        manifest = self._manifest(cell_id, epoch, batch_id, normalized, timeline)
        self._atomic_json(cell / "workers-manifest.json", manifest)
        return manifest

    def inspect(self, cell_id: str) -> dict[str, Any]:
        cell = self.root / cell_id
        timeline_path = cell / "workers.jsonl"
        manifest_path = cell / "workers-manifest.json"
        timeline: list[dict[str, Any]] = []
        if timeline_path.exists():
            for line in timeline_path.read_text(encoding="utf-8").splitlines():
                if line:
                    timeline.append(json.loads(line))
        manifest = None
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        return {"cell_id": cell_id, "timeline": timeline, "manifest": manifest}

    @staticmethod
    def _normalize(receipt: Mapping[str, Any]) -> dict[str, Any]:
        role = str(receipt.get("role", ""))
        state = str(receipt.get("state", ""))
        task_id = str(receipt.get("task_id", ""))
        if role not in _ALLOWED_ROLES:
            raise ValueError(f"unknown worker evidence role: {role}")
        if state not in _ALLOWED_STATES:
            raise ValueError(f"unknown worker evidence state: {state}")
        if not task_id.strip():
            raise ValueError("worker receipt is missing task_id")
        return {
            "task_id": task_id,
            "role": role,
            "state": state,
            "started_at": float(receipt.get("started_at", 0.0)),
            "finished_at": float(receipt.get("finished_at", 0.0)),
            "error": receipt.get("error"),
            "result_digest": _result_digest(receipt.get("result")),
        }

    @staticmethod
    def _manifest(
        cell_id: str,
        epoch: int,
        batch_id: str,
        receipts: tuple[dict[str, Any], ...],
        timeline: Path,
    ) -> dict[str, Any]:
        payload = json.dumps(receipts, sort_keys=True, separators=(",", ":")).encode()
        counts = {state: 0 for state in sorted(_ALLOWED_STATES)}
        for row in receipts:
            counts[row["state"]] += 1
        return {
            "schema_version": 1,
            "cell_id": cell_id,
            "epoch": epoch,
            "batch_id": batch_id,
            "workers": len({row["role"] for row in receipts}),
            "tasks": len(receipts),
            "counts": counts,
            "batch_sha256": hashlib.sha256(payload).hexdigest(),
            "timeline_sha256": hashlib.sha256(timeline.read_bytes()).hexdigest(),
        }

    @staticmethod
    def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(dict(value), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        with tmp.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(tmp, path)


def _result_digest(result: Any) -> str | None:
    if result is None:
        return None
    payload = json.dumps(result, sort_keys=True, separators=(",", ":"), default=str).encode()
    return "sha256:" + hashlib.sha256(payload).hexdigest()
