"""Per-call spend accounting for provider (agent) calls.

An agent call spends real money at the provider. The orchestrator learns *how much* only when the
call answers, so the window between "the request left this process" and "the receipt is durable"
is money the factory owes but cannot yet name. Counting a stage's spend in memory and journalling
it when the stage returns leaves that whole window unaccounted: a hard kill after a paid call loses
the charge, and the next process reconstructs a budget with more money in it than the account has.

So the order here is: reserve, then call, then settle.

* ``reserve`` writes a durable attempt row *before* the provider is invoked, holding the most that
  call could possibly cost -- the ceiling actually handed to the provider. Conservative on purpose:
  it is the only bound that still holds when nobody comes back to write the truth down.
* ``settle`` replaces the reservation with the billed usage and its receipt as soon as the answer
  exists, before any other I/O that could fail.
* ``mark_unknown`` records that an attempt died with its outcome unknown. The reservation stays
  charged. **Unknown usage never returns to available funds without reconciliation** -- an
  unreconciled call is money that might already be spent, and treating it as available is exactly
  how a run walks past its ceiling.
* ``reconcile`` is the only way out of unknown, and it takes an observation, not a claim.

There is deliberately no new vocabulary for "unknown": the states are ``idempotency``'s
``OperationState`` and the observations are its ``MutationOutcome`` (#2042), which already models an
attempt owner/lease and in-doubt outcomes, and which ``restore_contract`` (#2074) already reads as
"state that must be observed before it is trusted". A second notion of unknown would be a second
source of truth, and the two would disagree at exactly the moment one of them mattered.

The rows live in ``<run_dir>/state/agent_calls.jsonl`` through :class:`swfactory.state.RunState`,
so they inherit the orchestrator's crash-safe append: fsync, torn-tail recovery and POSIX locking.
The sandbox is never consulted -- it is agent-writable, and money is not a thing the agent gets to
report on.
"""

from __future__ import annotations

import os
import socket
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from swfactory.idempotency import MutationOutcome, OperationState
from swfactory.state import RunState

CALLS_LOG = "agent_calls.jsonl"
DEFAULT_LEASE_S = 3600.0  # an agent call is long; the lease only labels an owner, it never releases
# The only outcomes that may move money back out of a reservation: the provider's own usage, or
# proof the request never reached it. Everything else -- including "we looked and could not tell"
# -- leaves the charge standing.
RELEASING = frozenset({"committed", "definitely_absent"})


class ReconciliationRefused(RuntimeError):
    """A reservation was asked to release on something that is not an observation of its outcome."""


@dataclass(frozen=True)
class CallAttempt:
    """The durable handle for one provider call, returned by :meth:`CallLedger.reserve`."""

    call_id: str
    owner: str
    stage: str
    iteration: int
    reserved_usd: float


@dataclass(frozen=True)
class CallRecord:
    """One call folded over its journal rows: what it may cost, and what is known about it."""

    call_id: str
    stage: str
    iteration: int
    state: OperationState
    reserved_usd: float
    settled_usd: float | None = None
    receipt: dict[str, Any] | None = None
    observation: dict[str, Any] | None = None
    reason: str | None = None
    owner: str | None = None
    lease_until: float = 0.0

    @property
    def unreconciled(self) -> bool:
        """No receipt and no observation: this call's usage is unknown."""
        return self.settled_usd is None

    @property
    def charge(self) -> float:
        """What this call takes out of the run ceiling.

        An unsettled call is charged its full reservation whether it is still in flight or was
        orphaned by a kill, because the two are indistinguishable from here and only one of them
        is safe to assume.
        """
        return self.reserved_usd if self.settled_usd is None else self.settled_usd


class CallLedger:
    """Append-only per-call spend for one run."""

    def __init__(self, state: RunState) -> None:
        self._state = state

    # -- writes ----------------------------------------------------------
    def reserve(
        self, *, stage: str, iteration: int, reserved_usd: float, lease_s: float = DEFAULT_LEASE_S
    ) -> CallAttempt:
        """Journal the attempt and its conservative reservation. Call this before the provider."""
        if reserved_usd < 0:
            raise ValueError("a reservation cannot be negative")
        # A retry after a lost attempt is a *new* call: reusing the identity would let the retry's
        # settlement overwrite the dead attempt's reservation, which is the charge we are keeping.
        attempt = CallAttempt(
            call_id=f"{stage}.{iteration}:{uuid.uuid4().hex[:12]}",
            owner=uuid.uuid4().hex,
            stage=stage,
            iteration=iteration,
            reserved_usd=float(reserved_usd),
        )
        self._append(
            attempt,
            event="reserved",
            state="intent",
            reserved_usd=attempt.reserved_usd,
            cost_usd=None,
            owner=attempt.owner,
            lease_until=self._now() + max(1.0, lease_s),
            pid=os.getpid(),
            host=socket.gethostname(),
        )
        return attempt

    def settle(self, attempt: CallAttempt, *, cost_usd: float, receipt: dict[str, Any] | None = None) -> None:
        """Record billed usage and its receipt. The reservation is replaced by the truth."""
        if cost_usd < 0:
            raise ValueError("settled usage cannot be negative")
        self._append(
            attempt,
            event="settled",
            state="committed",
            reserved_usd=attempt.reserved_usd,
            cost_usd=float(cost_usd),
            receipt=dict(receipt or {}),
        )

    def mark_unknown(self, attempt: CallAttempt, reason: BaseException | str) -> None:
        """Record that the attempt died with its usage unknown; the reservation stays charged."""
        self._append(
            attempt,
            event="unknown",
            state="in_doubt",
            reserved_usd=attempt.reserved_usd,
            cost_usd=None,
            reason=str(reason)[:2000],
        )

    def reconcile(self, call_id: str, outcome: MutationOutcome) -> CallRecord:
        """Apply one observation of a call's real outcome.

        Only ``committed`` (carrying the provider's own usage) and ``definitely_absent`` settle the
        row. Every other status is recorded as evidence and leaves the reservation charged, so a
        look that could not tell cannot be mistaken for a look that found nothing.
        """
        record = self.record(call_id)
        if record.settled_usd is not None:
            raise ReconciliationRefused(f"call {call_id!r} is already settled at {record.settled_usd} USD")
        usage: float | None = None
        if outcome.status in RELEASING:
            usage = 0.0 if outcome.status == "definitely_absent" else _usage_of(outcome)
            if usage is None:
                raise ReconciliationRefused(f"observation of {call_id!r} claims a charge with no usage attached")
        self._append(
            CallAttempt(call_id, record.owner or "", record.stage, record.iteration, record.reserved_usd),
            event="observed",
            state=_state_for(outcome, usage),
            reserved_usd=record.reserved_usd,
            cost_usd=usage,
            observation={"status": outcome.status, "evidence": outcome.evidence, "detail": outcome.detail},
        )
        return self.record(call_id)

    # -- reads -----------------------------------------------------------
    def records(self) -> list[CallRecord]:
        """Every call of this run, folded in journal order."""
        folded: dict[str, CallRecord] = {}
        for row in self._state.read_jsonl(CALLS_LOG):
            if not isinstance(row, dict) or not isinstance(row.get("call_id"), str):
                continue  # a foreign row cannot silently become a charge of zero
            prior = folded.get(row["call_id"])
            folded[row["call_id"]] = CallRecord(
                call_id=row["call_id"],
                stage=str(row.get("stage", "")),
                iteration=int(row.get("iteration", 0)),
                state=str(row.get("state", "intent")),  # type: ignore[arg-type]
                reserved_usd=float(row.get("reserved_usd", 0.0)),
                settled_usd=None if row.get("cost_usd") is None else float(row["cost_usd"]),
                receipt=row.get("receipt") or (prior.receipt if prior else None),
                observation=row.get("observation") or (prior.observation if prior else None),
                reason=row.get("reason") or (prior.reason if prior else None),
                owner=row.get("owner") or (prior.owner if prior else None),
                lease_until=float(row.get("lease_until") or (prior.lease_until if prior else 0.0)),
            )
        return list(folded.values())

    def record(self, call_id: str) -> CallRecord:
        for record in self.records():
            if record.call_id == call_id:
                return record
        raise KeyError(call_id)

    def charged_usd(self) -> float:
        """Everything this run has committed: billed usage plus every unreconciled reservation."""
        return round(sum(record.charge for record in self.records()), 6)

    def adopt_floor(self, usd: float) -> float:
        """Record, once, the money a run spent before per-call accounting existed.

        A run resumed across the deploy that introduced this ledger has cost in two places: the
        stage log for the calls made before, and this ledger for the calls made after. Those are
        DISJOINT sets of money. Taking ``max()`` of the two records -- the first version -- made the
        new calls free up to the old total: a 5.50 stage log and three settled 1.50 calls seeded
        5.50 every time while real spend reached 10.00 against an 8.00 ceiling, and the provider was
        invoked. Capturing the pre-ledger amount exactly once, the first time this ledger touches the
        run, and then adding what the ledger sees is the only arithmetic that is right in both eras.

        Idempotent: the first adoption wins, so a later, larger stage log -- which the stage wrapper
        keeps appending to for the operator's report -- cannot re-inflate the floor.
        """
        existing = self.floor_usd()
        if existing is not None:
            return existing
        usd = round(max(usd, 0.0), 6)
        self._state.append_json(
            CALLS_LOG,
            {"floor_usd": usd, "at": datetime.now(UTC).isoformat(timespec="milliseconds")},
        )
        return usd

    def floor_usd(self) -> float | None:
        """The adopted pre-ledger floor, or None when this run never adopted one."""
        for row in self._state.read_jsonl(CALLS_LOG):
            if isinstance(row, dict) and "floor_usd" in row and "call_id" not in row:
                return float(row["floor_usd"])
        return None

    def unreconciled(self) -> list[CallRecord]:
        """Calls whose usage nobody has observed. Their money is gone until proven otherwise."""
        return [record for record in self.records() if record.unreconciled]

    def unreconciled_usd(self) -> float:
        return round(sum(record.charge for record in self.unreconciled()), 6)

    # -- internals -------------------------------------------------------
    def _append(self, attempt: CallAttempt, **row: Any) -> None:
        self._state.append_json(
            CALLS_LOG,
            {
                "call_id": attempt.call_id,
                "stage": attempt.stage,
                "iteration": attempt.iteration,
                "at": datetime.now(UTC).isoformat(timespec="milliseconds"),
                **row,
            },
        )

    @staticmethod
    def _now() -> float:
        return datetime.now(UTC).timestamp()


def _state_for(outcome: MutationOutcome, usage: float | None) -> OperationState:
    """The same transition ``OperationJournal.mark_observation`` makes, so one reader serves both."""
    if usage is not None:
        return "committed"
    return "refused" if outcome.status == "refused" else "reconciling"


def _usage_of(outcome: MutationOutcome) -> float | None:
    """The provider's own usage from an observation, or None when it carries no number."""
    for candidate in (outcome.result, (outcome.evidence or {}).get("cost_usd")):
        if isinstance(candidate, (int, float)) and not isinstance(candidate, bool) and candidate >= 0:
            return float(candidate)
    return None
