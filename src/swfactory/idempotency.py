"""Exactly-once boundary primitives for retryable factory side effects."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


@dataclass(frozen=True)
class OperationRef:
    cell_id: str
    epoch: int
    kind: str
    key: str

    @classmethod
    def build(cls, cell_id: str, epoch: int, kind: str, *parts: str) -> "OperationRef":
        digest = hashlib.sha256("\0".join((cell_id, str(epoch), kind, *parts)).encode()).hexdigest()[:24]
        return cls(cell_id, epoch, kind, f"{kind}:{digest}")


class OperationJournal:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS operations(
              operation_key TEXT PRIMARY KEY,
              cell_id TEXT NOT NULL,
              epoch INTEGER NOT NULL,
              kind TEXT NOT NULL,
              state TEXT NOT NULL,
              result_json TEXT,
              updated_at REAL NOT NULL
            );
            """
        )

    def begin(self, ref: OperationRef) -> str:
        now = time.time()
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO operations VALUES(?,?,?,?,?,?,?)",
                (ref.key, ref.cell_id, ref.epoch, ref.kind, "intent", None, now),
            )
        return self.get(ref.key)["state"]

    def commit(self, ref: OperationRef, result: Any) -> None:
        payload = json.dumps(result, sort_keys=True, separators=(",", ":"))
        with self.db:
            self.db.execute(
                "UPDATE operations SET state='committed', result_json=?, updated_at=? WHERE operation_key=?",
                (payload, time.time(), ref.key),
            )

    def get(self, key: str) -> dict[str, Any]:
        row = self.db.execute("SELECT * FROM operations WHERE operation_key=?", (key,)).fetchone()
        if row is None:
            raise KeyError(key)
        out = dict(row)
        out["result"] = json.loads(out.pop("result_json")) if out.get("result_json") else None
        return out

    def execute(self, ref: OperationRef, fn: Callable[[], Any]) -> Any:
        state = self.begin(ref)
        row = self.get(ref.key)
        if state == "committed":
            return row["result"]
        result = fn()
        self.commit(ref, result)
        return result

    def unresolved(self) -> list[dict[str, Any]]:
        rows = self.db.execute("SELECT * FROM operations WHERE state!='committed' ORDER BY updated_at").fetchall()
        return [dict(r) for r in rows]
