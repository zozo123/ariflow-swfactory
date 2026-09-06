"""Durable, operator-readable Factory Cell evidence bundles."""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class EvidenceEvent:
    cell_id: str
    epoch: int
    kind: str
    at: float
    data: dict[str, Any]


class EvidenceStore:
    def __init__(self, root: Path):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)

    def append(self, event: EvidenceEvent) -> Path:
        cell = self.root / event.cell_id
        cell.mkdir(parents=True, exist_ok=True)
        line = json.dumps(asdict(event), sort_keys=True, separators=(",", ":")) + "\n"
        path = cell / "timeline.jsonl"
        with path.open("a", encoding="utf-8") as fh:
            fh.write(line)
            fh.flush()
            os.fsync(fh.fileno())
        return path

    def manifest(self, cell_id: str, **metadata: Any) -> dict[str, Any]:
        cell = self.root / cell_id
        artifacts = []
        for path in sorted(cell.glob("**/*")):
            if not path.is_file() or path.name == "manifest.json":
                continue
            data = path.read_bytes()
            artifacts.append(
                {
                    "path": str(path.relative_to(cell)),
                    "bytes": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
            )
        manifest = {
            "schema_version": 1,
            "cell_id": cell_id,
            "generated_at": time.time(),
            "metadata": metadata,
            "artifacts": artifacts,
        }
        self._atomic_json(cell / "manifest.json", manifest)
        return manifest

    def inspect(self, cell_id: str) -> dict[str, Any]:
        cell = self.root / cell_id
        timeline = []
        path = cell / "timeline.jsonl"
        if path.exists():
            timeline = [
                json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line
            ]
        manifest_path = cell / "manifest.json"
        manifest = (
            json.loads(manifest_path.read_text(encoding="utf-8"))
            if manifest_path.exists()
            else None
        )
        return {"cell_id": cell_id, "timeline": timeline, "manifest": manifest}

    @staticmethod
    def _atomic_json(path: Path, value: Any) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(tmp, path)
