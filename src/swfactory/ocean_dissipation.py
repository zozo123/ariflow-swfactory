"""Cache-locality scoring and entropy dissipation for Ocean120 D11-D12."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CacheCandidate:
    tenant: str
    lineage_digest: str
    warm_hits: int
    cold_misses: int
    age_s: float

    def locality_score(self) -> float:
        if self.warm_hits < 0 or self.cold_misses < 0 or self.age_s < 0:
            raise ValueError("cache counters cannot be negative")
        total = self.warm_hits + self.cold_misses
        reuse = self.warm_hits / total if total else 0.0
        freshness = 1.0 / (1.0 + self.age_s)
        return reuse + freshness


@dataclass(frozen=True, slots=True)
class EntropyCandidate:
    name: str
    live_references: int
    authority_overlap: bool = False
    superseded: bool = False


def eviction_order(candidates: list[CacheCandidate]) -> list[CacheCandidate]:
    """Evict lowest locality first; tenant and lineage remain part of identity."""

    return sorted(candidates, key=lambda item: (item.locality_score(), item.tenant, item.lineage_digest))


def collapse_entropy(candidates: list[EntropyCandidate]) -> tuple[list[str], list[str]]:
    """Return (delete, retain) sets without mutating the repository."""

    delete: list[str] = []
    retain: list[str] = []
    for candidate in candidates:
        if candidate.live_references < 0:
            raise ValueError("live references cannot be negative")
        if candidate.authority_overlap:
            retain.append(candidate.name)
            continue
        if candidate.superseded and candidate.live_references == 0:
            delete.append(candidate.name)
        else:
            retain.append(candidate.name)
    return sorted(delete), sorted(retain)
