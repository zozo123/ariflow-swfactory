"""Versioned compatibility contracts for backend, cells, artifacts and operators."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class ContractVersion:
    major: int
    minor: int = 0

    @classmethod
    def parse(cls, value: str) -> ContractVersion:
        major, _, minor = value.partition(".")
        return cls(int(major), int(minor or 0))

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}"


@dataclass(frozen=True)
class CompatibilityWindow:
    component: str
    minimum: ContractVersion
    current: ContractVersion

    def accepts(self, other: ContractVersion) -> bool:
        return self.minimum.major == other.major == self.current.major and self.minimum <= other <= self.current

    def negotiate(self, offered: ContractVersion) -> ContractVersion:
        if not self.accepts(offered):
            raise IncompatibleContract(f"{self.component}: offered {offered}, supported {self.minimum}..{self.current}")
        return min(offered, self.current)


class IncompatibleContract(RuntimeError):
    pass


@dataclass(frozen=True)
class GenerationStamp:
    factory_generation: str
    backend: ContractVersion
    cell: ContractVersion
    artifact: ContractVersion
    operator: ContractVersion

    def to_dict(self) -> dict[str, str]:
        return {
            "factory_generation": self.factory_generation,
            "backend": str(self.backend),
            "cell": str(self.cell),
            "artifact": str(self.artifact),
            "operator": str(self.operator),
        }


def adjacent_generation_compatible(parent: GenerationStamp, child: GenerationStamp) -> tuple[bool, tuple[str, ...]]:
    failures = []
    for field in ("backend", "cell", "artifact", "operator"):
        a = getattr(parent, field)
        b = getattr(child, field)
        if a.major != b.major or abs(a.minor - b.minor) > 1:
            failures.append(field)
    return not failures, tuple(failures)
