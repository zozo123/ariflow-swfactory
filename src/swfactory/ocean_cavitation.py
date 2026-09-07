"""Retry cavitation collapse and event-wave coalescing for Ocean120 D09-D10."""

from dataclasses import dataclass, field
from time import monotonic


@dataclass(slots=True)
class CavitationGuard:
    max_ambiguous: int = 3
    ambiguous: dict[tuple[str, int, str], int] = field(default_factory=dict)

    def observe(self, cell_id: str, epoch: int, operation_key: str, *, ambiguous: bool) -> tuple[bool, str]:
        if not cell_id.startswith("cell_") or epoch < 1 or not operation_key:
            raise ValueError("canonical mutation identity required")
        key = (cell_id, epoch, operation_key)
        if not ambiguous:
            self.ambiguous.pop(key, None)
            return True, "external result observed"
        count = self.ambiguous.get(key, 0) + 1
        self.ambiguous[key] = count
        if count >= self.max_ambiguous:
            return False, "retry cavitation collapsed; require observation or repair"
        return False, "ambiguous result held; blind retry refused"


@dataclass(slots=True)
class EventWaveCoalescer:
    window_s: float = 0.5
    seen: dict[tuple[str, int, str], float] = field(default_factory=dict)

    def admit(self, cell_id: str, epoch: int, event_key: str, *, now: float | None = None) -> bool:
        if not cell_id.startswith("cell_") or epoch < 1 or not event_key:
            raise ValueError("canonical event identity required")
        if self.window_s <= 0:
            raise ValueError("coalescing window must be positive")
        stamp = monotonic() if now is None else now
        key = (cell_id, epoch, event_key)
        previous = self.seen.get(key)
        self.seen[key] = stamp
        return previous is None or stamp - previous >= self.window_s
