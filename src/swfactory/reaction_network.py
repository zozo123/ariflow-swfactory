"""Protein-network-inspired coordination model for concurrent factory work.

The analogy is deliberately structural: components bind into complexes, compete for scarce partners,
undergo cooperative/allosteric regulation, require chaperone capacity, and can phase-separate into
specialized compartments. The model never schedules work; Airflow remains the sole scheduler.
"""

from dataclasses import dataclass
from math import exp


@dataclass(frozen=True, slots=True)
class Species:
    name: str
    concentration: float
    capacity: float = 1.0
    compartment: str = "default"

    def validate(self) -> None:
        if not self.name:
            raise ValueError("species name required")
        if self.concentration < 0 or self.capacity <= 0:
            raise ValueError("invalid species concentration/capacity")


@dataclass(frozen=True, slots=True)
class Reaction:
    reaction_id: str
    reactants: tuple[tuple[str, int], ...]
    products: tuple[tuple[str, int], ...]
    rate: float
    activation_barrier: float = 0.0
    cooperativity: float = 1.0
    inhibitor: float = 0.0

    def validate(self) -> None:
        if not self.reaction_id:
            raise ValueError("reaction_id required")
        if self.rate < 0 or self.activation_barrier < 0 or self.cooperativity <= 0:
            raise ValueError("invalid reaction kinetics")
        if self.inhibitor < 0:
            raise ValueError("inhibitor cannot be negative")
        if any(stoich <= 0 for _, stoich in (*self.reactants, *self.products)):
            raise ValueError("stoichiometry must be positive")


@dataclass(frozen=True, slots=True)
class ComplexState:
    complex_id: str
    members: tuple[str, ...]
    occupancy: float
    stable: bool


@dataclass(frozen=True, slots=True)
class NetworkFlux:
    reaction_id: str
    flux: float
    limited_by: str | None


def reaction_flux(
    reaction: Reaction,
    species: tuple[Species, ...],
    *,
    temperature: float = 1.0,
    chaperone_capacity: float = 1.0,
) -> NetworkFlux:
    """Mass-action-like flux with barrier, cooperativity, inhibition, and chaperone limits."""

    reaction.validate()
    if temperature <= 0 or chaperone_capacity < 0:
        raise ValueError("temperature must be positive and chaperone capacity non-negative")
    pool = {item.name: item for item in species}
    for item in species:
        item.validate()

    availability = 1.0
    limited_by: str | None = None
    limiting_ratio = float("inf")
    for name, stoich in reaction.reactants:
        item = pool.get(name)
        if item is None:
            return NetworkFlux(reaction.reaction_id, 0.0, name)
        ratio = item.concentration / stoich
        if ratio < limiting_ratio:
            limiting_ratio = ratio
            limited_by = name
        availability *= max(item.concentration / item.capacity, 0.0) ** (stoich * reaction.cooperativity)

    barrier = exp(-reaction.activation_barrier / temperature)
    inhibition = 1.0 / (1.0 + reaction.inhibitor)
    chaperone = min(chaperone_capacity, 1.0)
    return NetworkFlux(reaction.reaction_id, reaction.rate * availability * barrier * inhibition * chaperone, limited_by)


def cooperative_occupancy(ligand: float, *, kd: float, hill: float = 1.0) -> float:
    """Hill-style cooperative occupancy for allosteric/admission effects."""

    if ligand < 0 or kd <= 0 or hill <= 0:
        raise ValueError("invalid cooperative binding inputs")
    numerator = ligand**hill
    return numerator / (kd**hill + numerator) if numerator else 0.0


def kinetic_proofreading(error_probability: float, checkpoints: int) -> float:
    """Independent proofreading checkpoints suppress erroneous promotion exponentially."""

    if not 0 <= error_probability <= 1 or checkpoints < 0:
        raise ValueError("invalid proofreading inputs")
    return error_probability ** (checkpoints + 1)


def phase_separation_score(*, affinity: float, concentration: float, crowding: float, disorder: float) -> float:
    """Heuristic propensity for forming a specialized condensate/compartment."""

    if min(affinity, concentration, crowding, disorder) < 0:
        raise ValueError("phase-separation inputs cannot be negative")
    return affinity * concentration * (1.0 + crowding) / (1.0 + disorder)


def assemble_complex(complex_id: str, members: tuple[Species, ...], *, threshold: float = 0.5) -> ComplexState:
    if not complex_id or not members or not 0 <= threshold <= 1:
        raise ValueError("invalid complex definition")
    for member in members:
        member.validate()
    occupancy = min(member.concentration / member.capacity for member in members)
    return ComplexState(complex_id, tuple(member.name for member in members), occupancy, occupancy >= threshold)
