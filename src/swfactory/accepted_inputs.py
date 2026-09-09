"""What one Factory Cell epoch was admitted with, pinned so no later task can reinterpret it.

Airflow rebuilds a fresh ``Ctx`` for every task of an epoch: ``dags/blueprints.py`` reloads the
named blueprint, ``runtime._prepare_ctx`` re-fetches the issue, and ``Config`` re-reads the
worker's ``SWF_*`` environment. Every one of those is a live input read at task time, so without a
pin the issue body, the blueprint on the worker's disk or a policy variable could differ between
two tasks of the same epoch and nothing would notice. A person approves one plan; the factory
builds a different one.

So the first context of a run *admits* one immutable snapshot (:class:`AcceptedInputs`) into
host-owned run state, and every later context recomputes it and compares. The comparison happens
in ``runtime.ctx_for``, before any stage function, agent call or publication: refusing after the
model has written code is not refusing.

Three settings buckets, because conflating them either fences nothing or fences the wrong thing:

* **identity** (``config.IDENTITY_SETTINGS``) -- which issue, repo, subtree, branch and run. Already
  reasserted by ``runtime.job_config`` and carried in the snapshot's own fields.
* **policy** (:data:`POLICY_SETTINGS`) -- what the approver was told the run may do: which sandbox
  and agent, which loop bounds and budgets, which egress allowlist, which credential *mode*. These
  may not differ between two tasks of one epoch.
* **operational** (:data:`OPERATIONAL_SETTINGS`) -- per-worker paths and ownership that say nothing
  about what the run may do. These may differ; fencing them would only make a second worker
  unusable while protecting nothing anyone approved.

``SWF_GATE_REPLAY`` sits across that line and is split deliberately: the fixture *path* is
operational (every checkout puts it somewhere else), its *content* is policy, because the file
answers release gates. So the snapshot pins ``sha256(fixture)`` and ignores where it lives.

When inputs genuinely changed, the answer is a new epoch, never a quiet re-read: see
:func:`reaccept` and ``docs/run-recovery.md``.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from swfactory.models import Issue, StageError

if TYPE_CHECKING:
    from swfactory.blueprint import Blueprint
    from swfactory.config import Config
    from swfactory.state import RunState

SCHEMA_VERSION = 1
PIN_FILE = "accepted-inputs.json"  # host-owned control state, next to identity.json and cell.json
ADMITTED_AT_FILE = "accepted-inputs-at.txt"
HISTORY_LOG = "accepted-inputs.jsonl"  # append-only: every admission and every deliberate supersede

# Settings that ARE the execution policy an approval covers. Kept here rather than in
# ``stages.setup`` so the setup identity hash and the epoch pin can never drift into two different
# opinions of what "policy" means.
POLICY_SETTINGS: tuple[str, ...] = (
    "sandbox",
    "agent",
    "scm",
    "approve",
    "tests",
    "crabbox_provider",
    "max_build_iterations",
    "max_review_fixes",
    "max_turns",
    "max_budget_usd_per_stage",
    "max_budget_usd",
    "gate_timeout_h",
    "stage_timeout_h",
    "max_parallel_jobs",
    "gateway_profile",
    "islo_environment",
    "sandbox_ttl_s",
    "sandbox_idle_s",
    "islo_snapshot",
    "toolset_backend",
    "toolset_sbx_host_network_policy",  # whether the sandbox may reach the network at all
    "toolset_sbx_image",  # what the generated code executes inside
    "toolset_workdir",
    "srt_allowed_domains",
    "docker_image",
    "docker_credentials",  # env vs. bind-mounting the operator's own ~/.claude: a policy choice
    "docker_network",
    "docker_user",
    "allow_local_agent",
)

# Settings a second worker may legitimately hold differently. ``gate_replay`` is here for its PATH
# only; its content is folded into the policy document below.
OPERATIONAL_SETTINGS: frozenset[str] = frozenset(
    {
        "fixtures_dir",  # absolute after runtime.locate(): differs per checkout
        "workdir",  # per-run host scratch, derived from the run directory
        "record_dir",  # where a worker dumps agent output for later fixture authoring
        "sandbox_owner",  # the worker identity allowed to sweep its own sandboxes
        "gate_replay",  # path only; the answers it contains are policy (see policy_document)
    }
)

REAPPROVAL_HINT = (
    "an accepted epoch is never reinterpreted: open a new epoch and re-answer every gate "
    "(backend-managed: activate the Cell again; local: `swfactory state reaccept <run_id> "
    "--actor NAME --reason TEXT`)"
)


class AcceptedInputs(BaseModel):
    """The versioned, immutable snapshot admitted for one Cell epoch.

    Every field is something a change of which changes what the factory would build, so the whole
    model -- not a chosen subset -- is what :attr:`digest` covers.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = SCHEMA_VERSION
    cell_id: str | None = None
    cell_epoch: int | None = Field(default=None, ge=1)
    # Who owns this epoch. Outside the snapshot it was unpinned, so a re-run that dropped the
    # Airflow bindings was admitted with no refusal, `cell_evidence` then reported unmanaged, and
    # both the local re-accept refusal and the "a replay fixture cannot authorize managed work"
    # ban came off with it.
    managed: bool = False
    issue_id: str
    issue_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    # Deliberately no issue URL: for a file issue it is ``file://<this worker's absolute path>``,
    # so pinning it would fence two workers apart over where a checkout lives. Which issue this is
    # comes from ``issue_id`` + ``repo``; what it says comes from ``issue_sha256``.
    blueprint: str
    blueprint_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    policy_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    repo: str
    target_dir: str
    base_branch: str

    @property
    def digest(self) -> str:
        """``inputs:<sha256>`` -- the one value approvals, reports and receipts all quote."""
        return "inputs:" + _sha256(self.model_dump(mode="json"))


# what a mismatching field means to a person reading the refusal
_FIELD_NAMES = {
    "cell_id": "Cell id",
    "cell_epoch": "cell epoch",
    "managed": "who owns this Cell epoch",
    "issue_id": "issue id",
    "issue_sha256": "issue content",
    "blueprint": "blueprint name",
    "blueprint_sha256": "resolved blueprint",
    "policy_sha256": "effective policy",
    "repo": "target repo",
    "target_dir": "target directory",
    "base_branch": "base branch",
}


def _dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _sha256(value: Any) -> str:
    return hashlib.sha256(_dumps(value).encode("utf-8")).hexdigest()


def issue_document(issue: Issue) -> dict[str, Any]:
    """Everything of the issue the run acts on. The body is the originator's verbatim text, so an
    edit to it is a different instruction even when the title and labels are untouched."""
    return {
        "id": issue.id,
        "title": issue.title,
        "body": issue.body,
        "labels": list(issue.labels),
    }


def _replay_digest(path: str | None) -> str | None:
    """``sha256`` of the declared gate-replay fixture, or ``None`` when none is declared.

    Content, not path: two workers pointing at their own copy of the same fixture accepted the same
    answers, while swapping the answers under a running epoch is a policy change. An unreadable
    fixture hashes to nothing here on purpose -- ``approval_policy.load_replay_fixture`` is the one
    place that decides a broken fixture is fatal, and it does so when a gate is actually answered.
    """
    if not path:
        return None
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except OSError:
        return "unreadable"


def policy_document(cfg: Config, blueprint: Blueprint | None) -> dict[str, Any]:
    """The effective execution policy: the policy-bucket settings plus the resolved blueprint."""
    return {
        "schema_version": SCHEMA_VERSION,
        "config": {name: getattr(cfg, name) for name in POLICY_SETTINGS},
        "gate_replay_sha256": _replay_digest(cfg.gate_replay),
        "blueprint": blueprint.model_dump(mode="json") if blueprint is not None else None,
    }


def snapshot(
    cfg: Config,
    blueprint: Blueprint | None,
    issue: Issue,
    *,
    cell_id: str | None = None,
    cell_epoch: int | None = None,
    managed: bool = False,
) -> AcceptedInputs:
    """The snapshot the caller's inputs would be admitted as, without admitting it."""
    return AcceptedInputs(
        cell_id=cell_id,
        cell_epoch=cell_epoch,
        managed=managed,
        issue_id=issue.id,
        issue_sha256=_sha256(issue_document(issue)),
        blueprint=cfg.blueprint,
        blueprint_sha256=_sha256(blueprint.model_dump(mode="json") if blueprint is not None else None),
        policy_sha256=_sha256(policy_document(cfg, blueprint)),
        repo=cfg.repo,
        target_dir=cfg.target_dir,
        base_branch=cfg.base_branch,
    )


def stored(state: RunState) -> AcceptedInputs | None:
    """The snapshot this run was admitted with, or ``None`` when it has none yet."""
    if not state.has_control(PIN_FILE):
        return None
    try:
        return AcceptedInputs.model_validate_json(state.read_control(PIN_FILE))
    except (OSError, ValueError) as error:
        # A pin that cannot be read is not an absent pin: treating it as absent would let a
        # corrupted (or truncated) control file re-admit whatever the current worker happens to see.
        raise StageError("policy", f"the accepted inputs of this run are unreadable: {error}") from error


def digest_of(state: RunState) -> str | None:
    """This run's accepted-inputs digest, or ``None`` when it has no pin.

    ``None`` is a real answer, not a missing one: a Ctx built outside ``runtime.ctx_for`` (unit
    tests, the workgraph harness) has no epoch, and its approvals record ``None`` and are checked
    against ``None`` -- consistently unpinned rather than accidentally matching a pinned run.
    """
    pinned = stored(state)
    return pinned.digest if pinned is not None else None


def require(state: RunState) -> AcceptedInputs:
    """The snapshot, or a refusal. Used where evidence must quote it (approvals, delivery)."""
    pinned = stored(state)
    if pinned is None:
        raise StageError("policy", f"this run has no accepted inputs to execute against; {REAPPROVAL_HINT}")
    return pinned


def describe_mismatch(accepted: AcceptedInputs, current: AcceptedInputs) -> list[str]:
    """Which accepted inputs the current task disagrees with, in words an operator can act on."""
    old, new = accepted.model_dump(mode="json"), current.model_dump(mode="json")
    changed = []
    for field, label in _FIELD_NAMES.items():
        if old.get(field) != new.get(field):
            changed.append(f"{label} ({_short(old.get(field))} -> {_short(new.get(field))})")
    return changed


def _short(value: Any) -> str:
    text = "none" if value is None else str(value)
    return text[:12] + "…" if len(text) > 13 else text


def admit(state: RunState, current: AcceptedInputs) -> AcceptedInputs:
    """Pin ``current`` on first admission; afterwards return it only if it still matches.

    This is the fence. It runs while the run's exclusive lock is held and before the Ctx exists, so
    a task whose inputs drifted never reaches a stage body, an agent or the publication path.
    """
    accepted = stored(state)
    if accepted is None:
        state.write_control(PIN_FILE, current.model_dump_json() + "\n")
        # When this epoch accepted its inputs, kept beside the pin rather than inside it: the digest
        # has to stay a pure function of the inputs, or re-admitting identical inputs would produce
        # a different digest. `answered_before_admission` is what uses it.
        state.write_control(ADMITTED_AT_FILE, datetime.now(UTC).isoformat() + "\n")
        record = {"event": "accepted", **current.model_dump(mode="json"), "digest": current.digest}
        state.append_json(HISTORY_LOG, record)
        return current
    if accepted.digest != current.digest:
        changed = ", ".join(describe_mismatch(accepted, current)) or "an unclassified field"
        raise StageError(
            "policy",
            f"this Cell epoch was admitted with {accepted.digest}, but this task resolved "
            f"{current.digest}; changed: {changed}. {REAPPROVAL_HINT}",
        )
    return accepted


def reaccept(state: RunState, *, actor: str, reason: str) -> AcceptedInputs:
    """The deliberate route for genuinely changed inputs: retire the pin, on the record.

    It records who re-opened the epoch and why, then clears the pin so the NEXT context admits
    whatever the inputs now are and appends its own ``accepted`` record. Deliberately not a
    re-admission from here: this command reads no issue, no blueprint and no environment, so it
    cannot itself become a quiet way to accept inputs nobody looked at.

    Every earlier approval keeps the digest it was given for, so ``stages._validate_approvals``
    refuses to publish on it and every gate must be answered again. That is the point: this
    re-opens the epoch's inputs, it does not re-authorize them.

    A backend-managed Cell has no business being re-pinned by whichever worker happens to hold the
    run directory -- its epoch is the fence, and advancing it is the backend's job.
    """
    if not actor.strip() or not reason.strip():
        raise StageError("policy", "re-accepting changed inputs needs an actor and a reason")
    if _managed(state):
        raise StageError(
            "policy",
            "this run executes a backend-managed Cell: re-accept by advancing the Cell epoch "
            "through the backend, not by re-pinning one worker's run directory",
        )
    accepted = require(state)
    state.append_json(
        HISTORY_LOG,
        {
            "event": "superseded",
            "actor": actor.strip(),
            "reason": reason.strip(),
            "superseded_digest": accepted.digest,
        },
    )
    state.clear_control(PIN_FILE)
    state.clear_control(ADMITTED_AT_FILE)
    return accepted


def admitted_at(state: RunState) -> datetime | None:
    """When this run pinned the inputs it is currently executing, if it has pinned any."""
    if not state.has_control(ADMITTED_AT_FILE):
        return None
    try:
        return datetime.fromisoformat(state.read_control(ADMITTED_AT_FILE).strip())
    except (OSError, ValueError):
        # An unreadable stamp must not read as "no admission": that would waive the check below.
        return datetime.now(UTC)


def answered_before_admission(state: RunState, responded_at: datetime | None) -> bool:
    """Whether a gate answer was given before the inputs this run now holds were admitted.

    The pin binds the approval RECORD, not the approval CHANNEL: `record_<stage>` builds its
    Approval from the raw operator response, so re-running it stamps whatever is pinned *now*.
    After a re-accept, an ordinary Airflow task clear or retry therefore re-recorded a stale
    answer against the new inputs and it validated cleanly -- alice's yes to one plan silently
    becoming her yes to another. No attacker needed.

    `responded_at` comes from the HITL event, so it is the operator's own answer time. An answer
    that predates the admission cannot have been given for these inputs. An answer with no
    timestamp is refused rather than trusted: a response shape that omits it is exactly the shape
    a replay would have.
    """
    stamped = admitted_at(state)
    if stamped is None:
        return False
    if responded_at is None:
        # Only reachable for an in-process decision, which is made now by definition. A stored
        # response with no answer time is refused earlier, at `approval_from_response`.
        return False
    if responded_at.tzinfo is None:
        responded_at = responded_at.replace(tzinfo=UTC)
    return responded_at < stamped


def _managed(state: RunState) -> bool:
    if not state.has_control("cell.json"):
        return False
    try:
        data = json.loads(state.read_control("cell.json"))
    except (OSError, ValueError):
        return True  # an unreadable binding is not a licence to re-pin locally
    return bool(isinstance(data, dict) and data.get("managed"))
