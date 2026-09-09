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

The blueprint names its stages; ``prompts/<stage>.md`` is what those stages actually *say* to the
model. Pinning the blueprint and not the templates left the instruction itself loose: two workers
on different swfactory builds, holding a byte-identical ``blueprints/factory.toml``, admitted the
same digest and rendered different instructions -- a plan approved under one build prompt and code
produced under another, with every digest matching. So the templates the resolved blueprint
REFERENCES are pinned by content too (:data:`STAGE_PROMPTS`, :func:`prompt_document`), on the same
path/content split as the replay fixture.

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

from swfactory import agent
from swfactory.config import FACTORY_ROOT
from swfactory.models import Issue, StageError

if TYPE_CHECKING:
    from swfactory.blueprint import Blueprint
    from swfactory.config import Config
    from swfactory.state import RunState

SCHEMA_VERSION = 2  # 2: prompt templates, packaged review policy, agent policies, persisted digest
PIN_FILE = "accepted-inputs.json"  # host-owned control state, next to identity.json and cell.json
DIGEST_FILE = "accepted-inputs.digest"  # the digest AS WRITTEN at admission; never recomputed for receipts
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

# Which ``prompts/<name>.md`` each blueprint stage instructs the agent with. The blueprint names
# stages; ``stages.py`` (and ``work_stage.py`` on a liquid line) turns each into ``render_prompt``
# calls, and those templates are the instruction. Kept as data here rather than imported from
# ``stages`` so the pin stays importable without the stage bodies; ``test_accepted_inputs``
# re-derives it from the stage source by AST and fails when a new call site escapes this map.
STAGE_PROMPTS: dict[str, tuple[str, ...]] = {
    "intent": (),  # deterministic front-matter over the issue body; no agent call
    "spec": ("spec",),
    "plan": ("plan",),
    "build_and_test": ("build", "fix", "build_node"),  # iteration 1 builds; later ones fix; workgraph nodes
    "review": ("review", "fix"),  # a blocker round re-enters the fix template
    "deliver": (),  # PR body and receipt are code
}

# Templates no stage of a line can reach: ``diagnose`` belongs to ``maintain``, which runs outside
# any epoch. Named rather than ignored so adding a template is a deliberate placement -- digesting
# the whole directory instead would fence every running epoch over a prompt it never renders.
UNREFERENCED_PROMPTS: frozenset[str] = frozenset({"diagnose"})

# WHY the templates and not the swfactory version. Pinning ``swfactory.__version__`` would be
# stricter and simpler, and it was the obvious alternative: it fences the whole build, so nothing
# an upgrade changes can slip through. It was rejected because it fences the wrong set. Every
# upgrade -- a bug fix in ``state.py``, a new CLI flag, a docs typo -- would refuse every in-flight
# epoch, so operators would learn to re-accept reflexively, and a re-accept that has become routine
# stops being a decision about the inputs. The templates are the part of the build that changes
# what the model is told, and a digest over them refuses exactly when that changed and stays quiet
# otherwise, which is what keeps the refusal worth reading. The cost is bounded and known: a build
# that changes agent behaviour WITHOUT touching a referenced template (the guard rules in
# ``agent.py``, a stage body) is not fenced here. Those are pinned by their own evidence
# (``harness_conformance``, the setup identity hash), not by this snapshot.

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

    # Both spellings parse: a pin written by the previous build is read, described and refused --
    # never rejected as malformed, which would read as 'no pin' one layer up.
    schema_version: Literal[1, 2] = SCHEMA_VERSION
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
    # Per template rather than one rolled-up hash, because a refusal that cannot say WHICH
    # instruction changed is not actionable: the operator has to diff two swfactory builds to find
    # out what they are being asked to re-approve. A tuple, not a dict, so the field is as
    # immutable as ``frozen=True`` claims -- the digest is recomputed from it on every comparison.
    prompt_templates: tuple[tuple[str, str], ...] = ()
    # The review prompt interpolates a policy document. When the target ships none, it comes from
    # swfactory's own packaged REVIEW.md -- an instruction asset that differs between builds exactly
    # the way a template does, and two trees differing only there admitted the same digest. The
    # target's own copy is already fenced by target identity; this pins the packaged fallback.
    review_policy_sha256: str | None = None
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
    "prompt_templates": "prompt templates",  # replaced by _describe_templates, which names them
    "policy_sha256": "effective policy",
    "review_policy_sha256": "packaged review policy (REVIEW.md)",
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


def referenced_prompts(blueprint: Blueprint | None) -> tuple[str, ...]:
    """The templates this blueprint's stages actually render, sorted.

    The REFERENCED set, not the directory: a line without a ``spec`` stage never renders
    ``spec.md``, so a change to it cannot have changed what that line was approved to do, and
    fencing it would stop unrelated work over an unrelated prompt.

    A context with no blueprint (unit tests, the workgraph harness) references nothing, exactly as
    its ``blueprint_sha256`` pins ``None``: consistently unpinned, not accidentally matching.
    """
    if blueprint is None:
        return ()
    return tuple(sorted({name for stage in blueprint.order for name in STAGE_PROMPTS.get(stage, ())}))


def agent_policy_document() -> dict[str, dict[str, Any]]:
    """``agent.POLICIES`` as a stable JSON document, one entry per stage template."""
    from dataclasses import asdict

    from swfactory.agent import POLICIES

    return {name: asdict(policy) for name, policy in sorted(POLICIES.items())}


def packaged_review_policy_digest(blueprint: Blueprint | None) -> str | None:
    """sha256 of the packaged review policy the review prompt falls back to, or None if absent.

    Resolved the way ``stages._review_policy`` resolves it -- ``blueprint.review.policy``, default
    ``REVIEW.md``, under ``FACTORY_ROOT`` -- but read directly and without that function's cache
    side effect, because admission happens before any run state exists.
    """
    name = blueprint.review.policy if blueprint is not None else "REVIEW.md"
    path = FACTORY_ROOT / name
    if not path.is_file():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def prompt_document(blueprint: Blueprint | None) -> tuple[tuple[str, str], ...]:
    """``(name, sha256)`` for every referenced template, read from the templates installed HERE.

    Content, not path -- the ``gate_replay`` split again: two checkouts install swfactory in
    different directories, which is not something anyone approved, while shipping different text
    under the same template name is exactly the mid-epoch change this pins.

    A template that cannot be read pins ``"missing"`` rather than being skipped: a worker that
    lacks the file would otherwise agree with one that has it, right up until the stage renders it.
    """
    return tuple((name, _template_digest(name)) for name in referenced_prompts(blueprint))


def _template_digest(name: str) -> str:
    try:
        return hashlib.sha256((agent.PROMPTS_DIR / f"{name}.md").read_bytes()).hexdigest()
    except OSError:
        return "missing"


def policy_document(cfg: Config, blueprint: Blueprint | None) -> dict[str, Any]:
    """The effective execution policy: the policy-bucket settings, the resolved blueprint, and the
    prompt templates that blueprint names.

    The templates are POLICY, not operational: they are what the approver was told the run may do,
    one layer below the blueprint. So they belong in the document every receipt quotes
    (``metrics.policy_sha256``), and they are also carried as their own snapshot field so a refusal
    can name the file -- the same shape ``blueprint``/``blueprint_sha256`` already has.
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "config": {name: getattr(cfg, name) for name in POLICY_SETTINGS},
        "gate_replay_sha256": _replay_digest(cfg.gate_replay),
        "blueprint": blueprint.model_dump(mode="json") if blueprint is not None else None,
        "prompt_templates": [list(entry) for entry in prompt_document(blueprint)],
        "review_policy_sha256": packaged_review_policy_digest(blueprint),
        # The tool surface, model and timeout the agent is called with, per stage. Unpinned, a model
        # swap between two tasks of one epoch was not a mismatch. The module used to claim these were
        # "pinned by their own evidence"; nothing pinned them.
        "agent_policies": agent_policy_document(),
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
        prompt_templates=prompt_document(blueprint),
        review_policy_sha256=packaged_review_policy_digest(blueprint),
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
    """The digest this run was admitted under, or None when it has no pin.

    Read from the sidecar written at admission, not recomputed from the pin. A pin admitted under
    the current schema and lacking a sidecar (a run admitted by the build immediately before this
    one) is recomputed once, which is stable because the schema did not change under it; a pin
    from an OLDER schema with no sidecar cannot be trusted to reproduce its receipts and is refused
    by `admit`, so nothing downstream ever quotes a digest the code cannot stand behind.
    """
    pinned = stored(state)
    if pinned is None:
        return None
    if state.has_control(DIGEST_FILE):
        return state.read_control(DIGEST_FILE).strip()
    if pinned.schema_version == SCHEMA_VERSION:
        return pinned.digest
    raise StageError(
        "policy",
        f"this run was admitted by an earlier swfactory build (accepted-inputs schema "
        f"{pinned.schema_version}, now {SCHEMA_VERSION}) that recorded no stable digest; "
        f"its receipts cannot be re-derived. {REAPPROVAL_HINT}",
    )


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
    # `policy_sha256` covers the templates and the packaged review policy as well as the settings.
    # When one of those named sub-causes moved, "effective policy changed" beside "prompts/build.md
    # changed" reports one change twice and tells the operator nothing the named line did not.
    named_causes = ("prompt_templates", "review_policy_sha256", "blueprint_sha256")
    policy_explained = any(old.get(f) != new.get(f) for f in named_causes)
    for field, label in _FIELD_NAMES.items():
        if old.get(field) == new.get(field):
            continue
        if field == "policy_sha256" and policy_explained:
            continue
        if field == "prompt_templates":
            changed.extend(_describe_templates(old.get(field) or [], new.get(field) or []))
        else:
            changed.append(f"{label} ({_short(old.get(field))} -> {_short(new.get(field))})")
    return changed


def _describe_templates(accepted: Any, current: Any) -> list[str]:
    """Name the template files themselves.

    "prompt templates changed" is unactionable -- the operator cannot tell whether the build
    instruction, the review instruction or both moved, and so cannot judge what re-approving would
    mean. ``prompts/build.md`` is a file they can diff between the two swfactory builds.
    """
    before, after = dict(map(tuple, accepted)), dict(map(tuple, current))
    lines = []
    for name in sorted(set(before) | set(after)):
        was, now = before.get(name), after.get(name)
        if was == now:
            continue
        if was is None:
            lines.append(f"prompt template prompts/{name}.md is newly referenced by this line")
        elif now is None:
            lines.append(f"prompt template prompts/{name}.md is no longer referenced by this line")
        else:
            lines.append(f"prompt template prompts/{name}.md changed ({_short(was)} -> {_short(now)})")
    return lines


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
        # The digest is recorded at the moment of admission and read back verbatim from here on.
        # Recomputing it from the stored model is not stable across a schema change: adding a field
        # changed the digest of pins an earlier build had already written, so every published
        # receipt quoted a value this code could no longer reproduce.
        state.write_control(DIGEST_FILE, current.digest + "\n")
        # When this epoch accepted its inputs, kept beside the pin rather than inside it: the digest
        # has to stay a pure function of the inputs, or re-admitting identical inputs would produce
        # a different digest. `answered_before_admission` is what uses it.
        state.write_control(ADMITTED_AT_FILE, datetime.now(UTC).isoformat() + "\n")
        record = {"event": "accepted", **current.model_dump(mode="json"), "digest": current.digest}
        state.append_json(HISTORY_LOG, record)
        return current
    if accepted.schema_version != current.schema_version:
        # Not a drift in the inputs -- a drift in what this build PINS. Describing it field by field
        # would say "prompts/build.md is newly referenced", which is false: it was always rendered,
        # it just was not pinned. Say what actually happened and point at the route.
        raise StageError(
            "policy",
            f"this Cell epoch was admitted by an earlier swfactory build (accepted-inputs schema "
            f"{accepted.schema_version}); this build (schema {current.schema_version}) also pins "
            f"prompt templates, the packaged review policy and the agent tool policies, which that "
            f"admission never covered. {REAPPROVAL_HINT}",
        )
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
    state.clear_control(DIGEST_FILE)
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
