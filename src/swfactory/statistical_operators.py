"""Statistical-physics-inspired operators for high-entropy development.

These operators manipulate an abstract development state only. They do not schedule lifecycle work,
mutate GitHub, or bypass Factory Cell/epoch authority. Apache Airflow remains the sole scheduler.
"""

from dataclasses import dataclass, replace


@dataclass(frozen=True, slots=True)
class DevelopmentQuantum:
    quantum_id: str
    cell_id: str
    epoch: int
    lineage: tuple[str, ...] = ()
    live_references: int = 0
    superseded: bool = False

    def validate(self) -> None:
        if not self.quantum_id:
            raise ValueError("quantum_id required")
        if not self.cell_id.startswith("cell_") or self.epoch < 1:
            raise ValueError("canonical Cell identity and positive epoch required")
        if self.live_references < 0:
            raise ValueError("live_references cannot be negative")


@dataclass(frozen=True, slots=True)
class FockState:
    quanta: tuple[DevelopmentQuantum, ...] = ()

    @property
    def number(self) -> int:
        return len(self.quanta)

    def by_id(self, quantum_id: str) -> DevelopmentQuantum | None:
        return next((q for q in self.quanta if q.quantum_id == quantum_id), None)


def create(state: FockState, quantum: DevelopmentQuantum) -> FockState:
    """Creation operator: add one explicit branch/work quantum with preserved Cell lineage."""

    quantum.validate()
    if state.by_id(quantum.quantum_id) is not None:
        raise ValueError("creation operator refuses duplicate quantum identity")
    return FockState((*state.quanta, quantum))


def annihilate(state: FockState, quantum_id: str) -> FockState:
    """Annihilation operator: remove only a superseded, unreferenced quantum."""

    quantum = state.by_id(quantum_id)
    if quantum is None:
        return state
    quantum.validate()
    if not quantum.superseded or quantum.live_references:
        raise ValueError("annihilation requires superseded state with zero live references")
    return FockState(tuple(q for q in state.quanta if q.quantum_id != quantum_id))


def reference(state: FockState, quantum_id: str, delta: int) -> FockState:
    quantum = state.by_id(quantum_id)
    if quantum is None:
        raise KeyError(quantum_id)
    updated = replace(quantum, live_references=quantum.live_references + delta)
    updated.validate()
    return FockState(tuple(updated if q.quantum_id == quantum_id else q for q in state.quanta))


def mark_superseded(state: FockState, quantum_id: str) -> FockState:
    quantum = state.by_id(quantum_id)
    if quantum is None:
        raise KeyError(quantum_id)
    updated = replace(quantum, superseded=True)
    return FockState(tuple(updated if q.quantum_id == quantum_id else q for q in state.quanta))


def number_operator(state: FockState, *, cell_id: str | None = None) -> int:
    if cell_id is None:
        return state.number
    return sum(1 for q in state.quanta if q.cell_id == cell_id)
