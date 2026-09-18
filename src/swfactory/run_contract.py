"""Canonical fixed run contracts for comparable experiment rounds.

The run contract records *how* a candidate is evaluated without storing secret
environment values. It is immutable experiment metadata, not an execution engine:
Airflow still owns lifecycle and the trusted runner still executes commands.

A contract intentionally contains only:
- argv: exact command tokens, never a shell string;
- cwd: repository-relative working directory;
- runtime: a stable runtime/harness identity (for example image or toolchain digest);
- env_fingerprint: a trusted digest over the non-secret environment identity.

Secret values never belong in this document.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import PurePosixPath
from typing import Any


class RunContractError(ValueError):
    """The experiment run contract is malformed or unsafe to compare."""


@dataclass(frozen=True)
class RunContract:
    argv: tuple[str, ...]
    cwd: str = "."
    runtime: str = "unspecified"
    env_fingerprint: str = "unspecified"
    schema_version: int = 1

    def validate(self) -> None:
        if not self.argv or any(not token or "\x00" in token for token in self.argv):
            raise RunContractError("run contract argv must contain nonempty NUL-free tokens")
        if any("\n" in token or "\r" in token for token in self.argv):
            raise RunContractError("run contract argv tokens must not contain line breaks")
        if not self.cwd or "\x00" in self.cwd:
            raise RunContractError("run contract cwd must be nonempty and NUL-free")
        path = PurePosixPath(self.cwd.replace("\\", "/"))
        if path.is_absolute() or ".." in path.parts:
            raise RunContractError("run contract cwd must stay inside the repository")
        if not self.runtime.strip():
            raise RunContractError("run contract runtime identity must be nonempty")
        if not self.env_fingerprint.strip():
            raise RunContractError("run contract environment fingerprint must be nonempty")

    @property
    def digest(self) -> str:
        self.validate()
        payload = {
            "schema_version": self.schema_version,
            "argv": list(self.argv),
            "cwd": str(PurePosixPath(self.cwd.replace("\\", "/"))),
            "runtime": self.runtime,
            "env_fingerprint": self.env_fingerprint,
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
        return hashlib.sha256(raw).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        document = asdict(self)
        document["argv"] = list(self.argv)
        document["digest"] = self.digest
        return document

    @classmethod
    def from_dict(cls, document: dict[str, Any]) -> "RunContract":
        contract = cls(
            argv=tuple(str(item) for item in document.get("argv", ())),
            cwd=str(document.get("cwd", ".")),
            runtime=str(document.get("runtime", "unspecified")),
            env_fingerprint=str(document.get("env_fingerprint", "unspecified")),
            schema_version=int(document.get("schema_version", 1)),
        )
        contract.validate()
        supplied = document.get("digest")
        if supplied is not None and str(supplied) != contract.digest:
            raise RunContractError("run contract digest does not match canonical content")
        return contract
