"""Host-owned run state.

The sandbox is where untrusted work happens, so it cannot also be the source of truth for gates,
review verdicts, cost accounting, or the baseline used to create a delivery patch. ``RunState``
keeps those values below the orchestrator's run directory and mirrors publishable artifacts into
the sandbox only when needed.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

from swfactory.paths import confined_path, normalize_relative_path

if TYPE_CHECKING:
    from swfactory.sandbox import Sandbox

try:
    import fcntl
except ImportError:  # pragma: no cover - swfactory targets Linux/macOS, kept import-safe elsewhere
    fcntl = None  # type: ignore[assignment]


class RunState:
    """Atomic files and an append-only journal owned by the orchestrator."""

    def __init__(self, run_dir: Path) -> None:
        self.root = Path(run_dir).resolve() / "state"
        self.artifacts = self.root / "artifacts"

    def _path(self, relative: str, *, artifacts: bool = False) -> Path:
        clean = normalize_relative_path(relative, field="run state path")
        return confined_path(self.artifacts if artifacts else self.root, clean)

    @staticmethod
    def _atomic_write(path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        tmp = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path)
        finally:
            if tmp.exists():
                tmp.unlink()

    def write_control(self, name: str, content: str) -> None:
        self._atomic_write(self._path(name), content)

    def read_control(self, name: str) -> str:
        return self._path(name).read_text(encoding="utf-8")

    def has_control(self, name: str) -> bool:
        return self._path(name).is_file()

    def clear_control(self, name: str) -> None:
        self._path(name).unlink(missing_ok=True)

    def write_artifact(self, relative: str, content: str) -> None:
        self._atomic_write(self._path(relative, artifacts=True), content)

    def read_artifact(self, relative: str) -> str:
        return self._path(relative, artifacts=True).read_text(encoding="utf-8")

    def has_artifact(self, relative: str) -> bool:
        return self._path(relative, artifacts=True).is_file()

    def mirror_artifact(self, sandbox: Sandbox, relative: str) -> None:
        sandbox.write(relative, self.read_artifact(relative))

    def mirror_all(self, sandbox: Sandbox) -> None:
        if not self.artifacts.is_dir():
            return
        for path in sorted(p for p in self.artifacts.rglob("*") if p.is_file()):
            sandbox.write(path.relative_to(self.artifacts).as_posix(), path.read_text("utf-8"))

    def append_json(self, name: str, value: Any) -> str:
        """Append one durable JSONL record under an advisory process lock."""

        line = json.dumps(value, separators=(",", ":"), ensure_ascii=False) + "\n"
        path = self._path(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a+", encoding="utf-8") as handle:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return line

    def read_jsonl(self, name: str) -> list[Any]:
        """Read valid records; tolerate only a torn final line from an interrupted append."""

        path = self._path(name)
        if not path.is_file():
            return []
        raw = path.read_text(encoding="utf-8")
        lines = raw.splitlines()
        records: list[Any] = []
        for index, line in enumerate(lines):
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except ValueError:
                if index == len(lines) - 1 and not raw.endswith("\n"):
                    break
                raise ValueError(f"corrupt run journal at {path}:{index + 1}") from None
        return records
