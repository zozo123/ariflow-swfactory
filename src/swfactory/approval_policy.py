"""Who may satisfy a release gate, and with what evidence.

One policy, three enforcement points, because a gate enforced in only one of them is enforced
nowhere:

1. **DAG construction** — ``dags/blueprints.py`` gives the ApprovalOperator a default answer only
   for a gate whose blueprint declares ``mode = "auto"``. Nothing ambient may widen that.
2. **Response recording** — ``record_<stage>`` (Airflow) and ``stages.cli_approver`` (local replay)
   both turn a response into an :class:`~swfactory.models.Approval` through this module, so the two
   surfaces cannot drift apart.
3. **Managed continuation** — ``stages.deliver`` re-checks the recorded chain against the gates the
   blueprint declared and against the Cell/epoch the run is actually bound to, before publishing.

Two properties this file exists to hold:

* A gate a blueprint declares as human is satisfied only by an identified answer to *that* gate, on
  *that* Cell epoch, over *that* artifact digest, for *those* accepted inputs
  (``swfactory.accepted_inputs``). ``SWF_APPROVE=auto`` is not an approver.
* "Nobody answered" is never "approved". The only thing that may stand in for a missing response is
  an explicitly declared replay fixture (``SWF_GATE_REPLAY``), which is refused outright for
  backend-managed work.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from swfactory.config import FACTORY_ROOT
from swfactory.models import Approval, StageError

GateMode = Literal["human", "auto"]
GATE_MODES: tuple[GateMode, ...] = ("human", "auto")

APPROVE = "Approve"
REJECT = "Reject"

# Actor names the runtime issues to itself. A response that claims one is forging provenance, so
# these can never arrive from an approver: "auto" would read as a policy-sanctioned automatic
# decision, "replay:*" as a fixture the release path already refuses for managed work.
AUTO_ACTOR = "auto"
REPLAY_ACTOR_PREFIX = "replay:"

# The one replay fixture that ships with the factory, for the scripted-agent harnesses (evals, the
# DAG smoke and stress runs). It is a file in the tree rather than a code path so "who answered this
# gate" stays a thing a reader can look at, and so pointing at it is a deliberate act.
SCRIPTED_REPLAY_FIXTURE = FACTORY_ROOT / "demo" / "gate-replay.json"


def declared_mode(spec: Mapping[str, Any]) -> GateMode:
    """The mode one ``[[gates]]`` table declares.

    ``mode`` is the contract; ``auto = true|false`` is the legacy spelling of the same statement and
    is folded into it here so the TOML has exactly one meaning. A file that says both and disagrees
    with itself is a bug, not a precedence puzzle to resolve silently.
    """
    mode = spec.get("mode")
    auto = spec.get("auto")
    if auto is not None and not isinstance(auto, bool):
        raise ValueError(f"gate auto must be a boolean, not {auto!r}")
    if mode is None:
        return "auto" if auto else "human"
    if mode not in GATE_MODES:
        raise ValueError(f"gate mode must be one of {list(GATE_MODES)}, not {mode!r}")
    if auto is not None and auto is not (mode == "auto"):
        raise ValueError(f"gate declares mode {mode!r} and auto {auto!r}, which contradict each other")
    return mode  # type: ignore[return-value]


def actor_of(responded_by_user: Any) -> str | None:
    """The identity behind an Airflow HITL response, or ``None`` when it carries none."""
    value: Any = responded_by_user
    if isinstance(responded_by_user, Mapping):
        value = responded_by_user.get("name") or responded_by_user.get("id")
    if not isinstance(value, str):
        return None
    return value.strip() or None


def load_replay_fixture(path: str | None) -> dict[str, dict[str, str]] | None:
    """Parse the declared replay fixture, or return ``None`` when none is declared.

    A malformed fixture is an error rather than an empty one: falling back to "no fixture" would
    reintroduce the missing-response hole through a typo.
    """
    if not path:
        return None
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise StageError("policy", f"gate replay fixture {path!r} is unreadable: {error}") from error
    if not isinstance(raw, dict):
        raise StageError("policy", f"gate replay fixture {path!r} must be a JSON object keyed by gate")
    fixture: dict[str, dict[str, str]] = {}
    for gate, entry in raw.items():
        decision = entry.get("decision") if isinstance(entry, dict) else None
        actor = entry.get("actor") if isinstance(entry, dict) else None
        if decision not in ("approve", "reject") or not isinstance(actor, str) or not actor.strip():
            raise StageError("policy", f"gate replay fixture entry for {gate!r} needs decision and actor")
        fixture[str(gate)] = {"decision": decision, "actor": actor.strip()}
    return fixture


def replay_refusal(*, managed: bool, scm: str, agent: str = "scripted") -> str | None:
    """Why this run may not use a replay fixture, or ``None`` when it may.

    The fixture has to prove it is NOT production, rather than merely that it is not *managed*.
    Guarding on ``managed`` alone reopened the very hole this module exists to close: ``managed`` is
    false for every direct CLI run, including ``--scm github``, so the shipped fixture could approve
    a declared human gate and publish a real pull request on the command line the self-hosting guide
    documents. One environment variable had simply replaced another.

    So each capability that lets a run reach the outside world disqualifies the fixture on its own.
    """
    if managed:
        # Managed work publishes through backend-owned credentials against a real Cell epoch.
        return "a backend-managed Cell"
    if scm != "local":
        return f"a run publishing through scm {scm!r}"
    # `agent` deliberately does NOT disqualify. The question is whether a run can produce an effect
    # outside itself, and the agent decides what gets written, not where it goes: a scripted agent
    # with `--scm github` is far more dangerous than a real one with `--scm local`, and the check
    # above already refuses it. Disqualifying on agent would only break the unattended evals, which
    # exist to run the real agent against a local remote -- no credentials, nothing published.
    return None


def replay_approval(
    gate: str, *, fixture_path: str | None, managed: bool, scm: str = "local", agent: str = "scripted"
) -> Approval | None:
    """The fixture's answer for ``gate``, or ``None`` when no fixture declares one."""
    fixture = load_replay_fixture(fixture_path)
    if fixture is None:
        return None
    if refusal := replay_refusal(managed=managed, scm=scm, agent=agent):
        raise StageError("policy", f"a gate replay fixture cannot answer gate {gate!r} for {refusal}")
    entry = fixture.get(gate)
    if entry is None:
        return None
    return Approval(
        gate=gate,  # type: ignore[arg-type]
        decision=entry["decision"],  # type: ignore[arg-type]
        actor=REPLAY_ACTOR_PREFIX + entry["actor"],
        mode="replay",
        at=datetime.now(UTC),
    )


def approval_from_response(
    *,
    gate: str,
    gate_mode: GateMode,
    response: Any,
    fixture_path: str | None = None,
    managed: bool = False,
    scm: str = "local",
    agent: str = "scripted",
) -> Approval:
    """Turn one recorded gate response into an Approval, or refuse to invent one."""
    if not response:
        replay = replay_approval(gate, fixture_path=fixture_path, managed=managed, scm=scm, agent=agent)
        if replay is not None:
            return replay
        # The hole this replaces recorded actor "auto" here, so a gate marked successful with no
        # answer at all authorized the rest of the line.
        raise StageError("policy", f"gate {gate!r} has no recorded response; a gate nobody answered is not approved")
    if not isinstance(response, Mapping):
        kind = type(response).__name__
        raise StageError("policy", f"gate {gate!r} response is malformed: expected an object, got {kind}")
    chosen = response.get("chosen_options")
    if not isinstance(chosen, list) or len(chosen) != 1 or chosen[0] not in (APPROVE, REJECT):
        raise StageError("policy", f"gate {gate!r} response has no single {APPROVE}/{REJECT} option: {chosen!r}")
    decision = "approve" if chosen[0] == APPROVE else "reject"
    actor = actor_of(response.get("responded_by_user"))
    if actor is None:
        # No identity means the operator's own declared default fired instead of a person. Only a
        # gate whose blueprint declares that mode is allowed to accept it.
        if gate_mode != "auto":
            raise StageError(
                "policy", f"gate {gate!r} is declared human; a response without an identity cannot satisfy it"
            )
        return Approval(gate=gate, decision=decision, actor=AUTO_ACTOR, mode="auto", at=datetime.now(UTC))  # type: ignore[arg-type]
    # Case-folded: "AUTO" recorded actor='AUTO', mode='human' and impersonated the runtime's own
    # actor name in approvals.json, which is the record an auditor reads to see who approved.
    folded = actor.casefold()
    if folded == AUTO_ACTOR.casefold() or folded.startswith(REPLAY_ACTOR_PREFIX.casefold()):
        raise StageError("policy", f"gate {gate!r} response claims the reserved actor {actor!r}")
    responded_at = _responded_at(response)
    if responded_at is None:
        # Fail closed here rather than downstream. This function parses a STORED response, so the
        # answer time is what later separates a person answering again from an old XCom being read
        # twice after a re-accept. The HITL event always carries it; a response shape that omits it
        # is the shape a replay would have.
        raise StageError(
            "policy", f"gate {gate!r} response carries no answer time, so it cannot be dated to these inputs"
        )
    return Approval(
        gate=gate,  # type: ignore[arg-type]
        decision=decision,
        actor=actor,
        mode="human",
        at=datetime.now(UTC),
        responded_at=responded_at,
    )


def _responded_at(response: Mapping[str, Any]) -> datetime | None:
    """The HITL event's own answer time, when it carries one.

    Carried through so recording can refuse an answer given before the current admission. Parsed
    leniently and left as None on anything unrecognised: the check treats a missing timestamp as
    stale, so a shape this cannot read fails closed rather than open.
    """
    raw = response.get("responded_at")
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def check_recorded(
    *,
    gate: str,
    gate_mode: GateMode,
    approval: Approval,
    managed: bool,
    cell_id: str | None,
    cell_epoch: int | None,
    inputs_digest: str | None,
) -> None:
    """Re-check one recorded decision at continuation time. Raises on anything short of authority.

    ``inputs_digest`` has no default on purpose: a caller that forgets it would silently publish on
    an approval given for other inputs, which is the whole failure this parameter exists to stop.
    """
    if approval.gate != gate:
        raise StageError("policy", f"approval for gate {gate!r} is recorded against gate {approval.gate!r}")
    if approval.inputs_digest != inputs_digest:
        # The artifact digest proves the approver saw this plan.md; it says nothing about the issue
        # text, blueprint and policy the remaining stages will execute. An answer given for other
        # accepted inputs is not authority over these -- it has to be answered again.
        raise StageError(
            "policy",
            f"approval for gate {gate!r} was given for accepted inputs {approval.inputs_digest}, not {inputs_digest}",
        )
    if approval.cell_id != cell_id or approval.cell_epoch != cell_epoch:
        # An answer is authority over one Cell epoch only: without this an approvals.json carried
        # into a fenced-off epoch (or another Cell's workspace) would still read as approved.
        raise StageError(
            "policy",
            f"approval for gate {gate!r} is bound to Cell {approval.cell_id}/{approval.cell_epoch}, "
            f"not {cell_id}/{cell_epoch}",
        )
    if gate_mode == "human" and approval.mode == "auto":
        raise StageError("policy", f"gate {gate!r} is declared human; an automatic decision cannot satisfy it")
    if approval.mode == "replay" and managed:
        raise StageError(
            "policy", f"gate {gate!r} carries a replay-fixture decision, which cannot authorize managed work"
        )
    if approval.mode == "human" and (approval.actor == AUTO_ACTOR or approval.actor.startswith(REPLAY_ACTOR_PREFIX)):
        raise StageError(
            "policy", f"gate {gate!r} records a human decision under the reserved actor {approval.actor!r}"
        )
