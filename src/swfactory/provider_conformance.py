"""Provider-neutral sandbox conformance contract and generated capability report."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol


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
LIFECYCLE_CHECKS = (
    "allocate",
    "fixed_identity_replay",
    "execute",
    "inspect",
    "heartbeat",
    "teardown",
    "cancel",
    "evidence",
)
PROVIDER_CONTRACT_VERSION = 1


class ProviderCapabilityDrift(RuntimeError):
    pass


@dataclass(frozen=True)
class ProviderContract:
    provider: str
    contract_version: int
    capabilities: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not self.provider.strip():
            raise ValueError("provider is required")
        if self.contract_version <= 0:
            raise ValueError("contract_version must be positive")
        missing = [name for name in LIFECYCLE_CHECKS if name not in self.capabilities]
        if missing:
            raise ValueError(f"provider contract omits required capabilities: {', '.join(missing)}")


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


def compare_provider_contract(expected: ProviderContract, observed: ProviderContract) -> None:
    """Fail closed on drift and name the exact provider/capability/value that changed."""
    if observed.provider != expected.provider:
        raise ProviderCapabilityDrift(
            f"provider identity drift: expected {expected.provider!r}, observed {observed.provider!r}"
        )
    if observed.contract_version != expected.contract_version:
        raise ProviderCapabilityDrift(
            f"provider {expected.provider}: contract version drift; expected {expected.contract_version}, "
            f"observed {observed.contract_version}"
        )
    for capability in sorted(expected.capabilities):
        expected_value = expected.capabilities[capability]
        observed_value = observed.capabilities.get(capability, "<missing>")
        if observed_value != expected_value:
            raise ProviderCapabilityDrift(
                f"provider {expected.provider}: capability {capability!r} drift under contract "
                f"v{expected.contract_version}; expected {expected_value!r}, observed {observed_value!r}"
            )


def contract_document(contract: ProviderContract) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "provider": contract.provider,
        "contract_version": contract.contract_version,
        "capabilities": dict(contract.capabilities),
    }
