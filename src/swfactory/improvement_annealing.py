"""Annealing for the improvement loop's own schedule: when to exploit, when to explore.

`liquid_annealing` anneals a review: defect evidence raises temperature, and a candidate
crystallizes only when the ordinary invariants already hold. `exploration_entropy` lets
exploration vary its ordering while keeping the entropy token as evidence that never reaches
promotion. Both govern how one candidate is examined.

Neither governs what the factory chooses to work on. `self_improvement` proposes the same way
whether it is retiring debt every cycle or has retired none in twenty: it demotes whatever has
stalled and proposes the next-heaviest thing, forever. That is a loop with no escape from a local
minimum -- it will circle the easy half of its backlog and never take on the item that actually
blocks it.

Annealing is the escape. The trajectory is the observation:

* debt retiring steadily -> cool -> EXPLOIT: a narrow budget spent on the closest-to-done work,
  and anything stalled stays demoted.
* nothing retired for several cycles -> hot -> EXPLORE: a wider budget spread across sources, and
  above the re-admission threshold the stalled items come back -- because when a loop is stuck, the
  thing it keeps sidestepping is usually the thing in its way. They return to be re-scoped, not
  re-proposed unchanged.

Heat is not the only thing a loop must read. Annealing widens the budget when nothing is
retiring, and that is right when the bottleneck is the loop's own search. It is exactly wrong when
the bottleneck is the queue the loop has already handed to humans: a backlog of open work orders
nobody has drained is evidence that proposing is not the scarce thing. So enrolment pressure is a
second, opposing term -- it scales the annealed budget DOWN, and it can take it below the base
budget, because a flooded loop should ask for less than a healthy one, not the same. At the cap the
budget floors at one: a loop that proposes nothing has gone dark, which is not restraint.

Pressure governs the proposal and the enrolment path only. It never touches measurement -- every
signal is still assessed, recorded and reported at full detail under any pressure, because a loop
that stops measuring when its queue is full is one that cannot see the queue draining either.

The same discipline as the modules it borrows from: these numbers are dimensionless diagnostics,
not physical units, and they carry no authority. Temperature shapes a proposal. It cannot approve,
promote, merge, or weaken a gate -- every done-condition still names a check the repository already
runs, and the liquid line's two human gates are untouched. A hotter loop asks for different work;
it never lowers the bar for finishing it.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

# Above this the loop is exploring rather than exploiting.
EXPLORING_AT = 0.5
# Above this it re-admits what it has been avoiding. Deliberately higher than EXPLORING_AT: widening
# the search is cheap, and returning to a known-stuck item should need real evidence of a stall.
READMIT_AT = 0.75
# Cycles of no retirement at which stagnation is half-saturated. Small, because three cycles with
# nothing retired is already a signal.
STAGNATION_HALFLIFE = 3.0
# Open work orders the loop may have enrolled before enrolment needs a human to acknowledge the
# queue. A soft cap: it refuses the mechanical enrolment path, never the measurement behind it.
DEFAULT_ENROL_CAP = 10


@dataclass(frozen=True)
class LoopObservation:
    """What the trajectory says about the loop, not about any one candidate."""

    cycles: int
    cycles_since_retirement: int
    stalled: int
    carried: int
    sources: int = 1
    # Work orders this loop enrolled that are still open. Counted outside, not here: `evaluate` does
    # no I/O, and the queue lives on a forge. Zero is the honest default -- an operator who has not
    # said how full the queue is has told the loop nothing, and a loop must not invent pressure.
    enrolled: int = 0
    enrol_cap: int = DEFAULT_ENROL_CAP

    def validate(self) -> None:
        if min(self.cycles, self.cycles_since_retirement, self.stalled, self.carried, self.sources) < 0:
            raise ValueError("annealing observations are counts and cannot be negative")
        if self.enrolled < 0:
            raise ValueError("annealing observations are counts and cannot be negative")
        if self.enrol_cap < 1:
            raise ValueError("an enrolment cap below one would refuse every proposal the loop makes")


@dataclass(frozen=True)
class LoopTemperature:
    temperature: float
    phase: str
    budget: int
    readmit_stalled: bool
    spread_sources: int
    reason: str
    # What the heat alone asked for, before enrolment pressure was applied. Kept so the echoed line
    # can say the budget was cut and by how much -- a governor that silently narrows the proposal is
    # indistinguishable from a loop that ran out of things to say.
    annealed_budget: int = 0
    pressure: float = 0.0
    enrolled: int = 0
    enrol_cap: int = DEFAULT_ENROL_CAP
    # False once the queue is at or over the cap. Refuses the mechanical enrolment path only; the
    # assessment behind it is still measured, ranked, recorded and reported in full.
    enrol_allowed: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 2, "authority": "proposal-shaping-only", **asdict(self)}


def evaluate(observation: LoopObservation, *, base_budget: int = 5) -> LoopTemperature:
    """Map a trajectory to a proposal policy. Deterministic: no clock, no randomness, no I/O.

    Stagnation dominates and blockage modulates. A loop can carry a lot of stalled debt and still be
    cool as long as it is retiring something each cycle -- progress is the evidence that matters, and
    a backlog full of hard items is not itself a reason to thrash.

    Enrolment pressure then opposes the result. It is applied AFTER the annealed widening, never
    folded into the temperature, for two reasons. Temperature is a statement about the search, and a
    full queue is a statement about the humans downstream -- collapsing them would make the reported
    reason unauditable. And a hot, flooded loop must land BELOW its base budget; a pressure term
    inside the temperature could at most cancel the widening and leave it at base, which is the
    wrong answer to "you have already asked for more than anyone has drained".
    """
    observation.validate()
    if base_budget < 1:
        raise ValueError("base budget must admit at least one work order")

    # Saturating, so twenty barren cycles are not twenty times hotter than four: past a point the
    # loop is simply stuck, and more heat buys nothing.
    stagnation = observation.cycles_since_retirement / (observation.cycles_since_retirement + STAGNATION_HALFLIFE)
    blockage = observation.stalled / observation.carried if observation.carried else 0.0
    temperature = round(min(1.0, 0.7 * stagnation + 0.3 * min(blockage, 1.0)), 6)

    exploring = temperature >= EXPLORING_AT
    readmit = temperature >= READMIT_AT
    # Widen with heat, never past double: an unbounded budget is not exploration, it is the whole
    # backlog proposed at once, which is the same as proposing nothing.
    annealed = base_budget + round(temperature * base_budget)
    spread = max(1, observation.sources if exploring else 1)

    # Linear relief rather than a cliff: the loop should narrow as the queue fills, not carry on at
    # full width until one issue tips it over. Floors at one -- see the module docstring on why a
    # governor that can reach zero is worse than the flood it is governing.
    pressure = round(min(1.0, observation.enrolled / observation.enrol_cap), 6)
    budget = max(1, round(annealed * (1.0 - pressure)))
    enrol_allowed = observation.enrolled < observation.enrol_cap

    if observation.cycles < 2:
        # One recorded assessment has no predecessor to compare against, so nothing can be known
        # about retirement yet. Saying "debt is being retired" there was a guess wearing a fact.
        reason = "no trajectory yet: exploit until there is evidence to explore on"
    elif readmit:
        reason = (
            f"nothing retired in {observation.cycles_since_retirement} cycles with "
            f"{observation.stalled}/{observation.carried} stalled: re-admit what is being avoided, re-scoped"
        )
    elif exploring:
        reason = f"nothing retired in {observation.cycles_since_retirement} cycles: widen the search"
    elif observation.cycles_since_retirement:
        # Warming, not yet exploring. Saying "debt is being retired" here was simply false, and a
        # policy that misreports why it chose what it chose is one nobody can audit.
        reason = f"nothing retired in {observation.cycles_since_retirement} cycles: warming, budget widened"
    else:
        reason = "debt is being retired: keep the budget narrow and finish what is closest"

    if budget < annealed:
        # Said second and joined, not substituted: the heat reason is still true, and an operator
        # reading "widen the search / budget cut to 2" learns something a single sentence cannot say.
        reason += (
            f"; {observation.enrolled}/{observation.enrol_cap} enrolled orders still open, "
            f"budget cut {annealed}->{budget}"
        )
        if not enrol_allowed:
            reason += " and enrolment needs a human to acknowledge the queue"

    return LoopTemperature(
        temperature=temperature,
        phase="exploring" if exploring else "exploiting",
        budget=budget,
        readmit_stalled=readmit,
        spread_sources=spread,
        reason=reason,
        annealed_budget=annealed,
        pressure=pressure,
        enrolled=observation.enrolled,
        enrol_cap=observation.enrol_cap,
        enrol_allowed=enrol_allowed,
    )


def observe(
    history: list[dict[str, Any]],
    *,
    carried: int,
    stalled: int,
    sources: int,
    enrolled: int = 0,
    enrol_cap: int = DEFAULT_ENROL_CAP,
) -> LoopObservation:
    """Read a recorded trajectory into an observation.

    ``cycles_since_retirement`` counts back to the last assessment that carried strictly more debt
    than the one after it -- that is what retirement looks like from the outside, and it needs no
    extra bookkeeping in the trajectory files.

    ``enrolled`` cannot be read from the trajectory: it is how many work orders are still OPEN on the
    forge, which the trajectory has no way to know -- the loop records what it proposed, and an order
    can be closed by anyone without the loop ever proposing again. It is passed in by the caller.
    """
    counts = [len(entry.get("signals", [])) for entry in history]
    since = 0
    for older, newer in zip(reversed(counts[:-1]), reversed(counts[1:]), strict=False):
        if older > newer:
            break
        since += 1
    return LoopObservation(
        cycles=len(history),
        cycles_since_retirement=since,
        stalled=stalled,
        carried=carried,
        sources=sources,
        enrolled=enrolled,
        enrol_cap=enrol_cap,
    )
