"""Provider-neutral sandbox conformance contract and generated capability report."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class SandboxProvider(Protocol):
    name: str

    def create(self) -> str: ...
    def exec(self, sandbox_id: str, command: list[str]) -> tuple[int, str, str]: ...
    def remove(self, sandbox_id: str) -> None: ...


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: str
    detail: str = ""


CORE_CHECKS = (
    "create",
    "exec",
    "timeout",
    "env_scrub",
    "protected_paths",
    "output_bounds",
    "termination_reporting",
    "exact_teardown",
)
OPTIONAL_CHECKS = ("attach", "snapshot", "fork", "pause_resume", "network_policy", "ttl")


def capability_report(provider: str, results: list[CheckResult]) -> dict:
    by_name = {result.name: result for result in results}

    def state(name: str) -> str:
        result = by_name.get(name)
        return "not_tested" if result is None else result.status

    return {
        "schema_version": 1,
        "provider": provider,
        "core": {name: state(name) for name in CORE_CHECKS},
        "optional": {name: state(name) for name in OPTIONAL_CHECKS},
        "core_ready": all(state(name) == "supported" for name in CORE_CHECKS),
        "results": [result.__dict__ for result in results],
    }


def classify(*, passed: bool | None, blocking: bool = True) -> str:
    if passed is None:
        return "not_tested"
    if passed:
        return "supported"
    return "not_supported" if blocking else "experimental"
