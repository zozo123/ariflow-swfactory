"""Maintain: detect metric band breaches over committed run metrics and respond by tier.

Detection is deterministic (``statistics.mean``/``statistics.stdev`` over a window of the committed
run metrics, read through the single reader ``metrics.load_all``); the model is involved only at
the ``diagnose``/``propose`` tiers, read-only, through the normal ``Agent`` seam. Also owns the
nightly sweep of orphaned ``swf-*`` sandboxes.
"""

from __future__ import annotations

import json
import math
import os
import re
import statistics
import subprocess
import time
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol

import yaml
from pydantic import BaseModel

from swfactory import cell_callback
from swfactory.agent import POLICIES, Agent, render_prompt
from swfactory.cells import TERMINAL_STATES, CellIdentity
from swfactory.cleanup_receipt import CleanupReceipt, CleanupStatus
from swfactory.config import Config
from swfactory.idempotency import MutationOutcome, OperationError, OperationJournal, OperationRef
from swfactory.metrics import CREATED_KEYS, first_timestamp, load_all
from swfactory.models import Diagnosis
from swfactory.runtime import run_id_for
from swfactory.sandbox import Sandbox
from swfactory.sandbox_governance import (
    CleanupDebt,
    CleanupDecision,
    ResourceObservation,
    SandboxIdentity,
    authorize_cleanup,
)
from swfactory.scm import Scm

Action = Literal["log", "diagnose", "propose"]

MIN_SAMPLES = 3
INCIDENTS_DIR = "docs/factory/incidents"
INCIDENT_LABELS = ("maintain", "incident")
SANDBOX_PREFIX = "swf-"
# A checkout of the target repo to read metrics from (else the DAG shallow-clones the base branch).
MAINTAIN_ROOT_ENV = "SWF_MAINTAIN_ROOT"

# bands.yaml metric name -> keys tried in metrics.json (first hit wins, after the name itself).
METRIC_ALIASES: dict[str, tuple[str, ...]] = {
    "first_pass_test_rate": ("first_pass_ci",),
    "build_iterations": ("iterations",),
    "review_blockers": ("blockers",),
}


class Breach(BaseModel):
    metric: str
    sigma: int
    value: float
    mean: float
    stdev: float
    action: Action


# ---------------------------------------------------------------- loading


def load_bands(path: Path) -> dict:
    """Parse ``bands.yaml`` (window_runs, metrics{direction}, tiers[{sigma, action}])."""
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    for key in ("metrics", "tiers"):
        if key not in data:
            raise ValueError(f"{path}: missing '{key}'")
    return data


def load_runs(root: Path, window: int, *, include_scripted: bool = False) -> list[dict]:
    """The newest ``window`` runs under ``root``, newest first — the band check's history.

    ``metrics.load_all`` is the only glob over committed metrics; scripted (replay) runs are
    excluded unless asked for, so demo runs never pollute real bands.
    """
    return load_all(root, include_scripted=include_scripted, newest_first=True)[: max(window, 0)]


def metric_value(run: dict, metric: str, *, key: str | None = None) -> float | None:
    """Look ``metric`` up in a metrics.json dict (top level, ``numbers``, ``metrics``)."""
    names = (key,) if key else (metric, *METRIC_ALIASES.get(metric, ()))
    for scope in (run, run.get("numbers"), run.get("metrics")):
        if not isinstance(scope, dict):
            continue
        for name in names:
            value = scope.get(name)
            if isinstance(value, bool):
                return float(value)
            if isinstance(value, int | float):
                return float(value)
    return None


# ---------------------------------------------------------------- detection


def detect(runs: list[dict], bands: dict) -> list[Breach]:
    """Classify the newest run's metrics against the history (``runs[1:]``) by sigma tier.

    Deterministic: sample mean/stdev over the history; a metric is skipped when fewer than
    ``MIN_SAMPLES`` history values exist or the latest run lacks it. The highest tier whose sigma
    the deviation (signed by ``direction``) reaches wins. A flat history (stdev 0 — the normal
    shape for booleans and small counts such as ``first_pass_ci`` or ``blockers``) makes any
    move in the bad direction an infinite deviation, i.e. a top-tier breach reported with
    ``stdev=0``; a move in the good direction or no move is not a breach.
    """
    if len(runs) < 2:
        return []
    latest, history = runs[0], runs[1:]
    tiers = sorted(
        ((int(t["sigma"]), str(t["action"])) for t in bands.get("tiers", [])),
        key=lambda t: t[0],
    )
    breaches: list[Breach] = []
    for metric, spec in bands.get("metrics", {}).items():
        spec = spec or {}
        key = spec.get("key")
        value = metric_value(latest, metric, key=key)
        samples = [v for r in history if (v := metric_value(r, metric, key=key)) is not None]
        if value is None or len(samples) < MIN_SAMPLES:
            continue
        mean, stdev = statistics.mean(samples), statistics.stdev(samples)
        if value == mean:
            continue
        deviation = (value - mean) / stdev if stdev else math.copysign(math.inf, value - mean)
        if spec.get("direction", "higher_is_bad") == "lower_is_bad":
            deviation = -deviation
        hit = [t for t in tiers if deviation >= t[0]]
        if not hit:
            continue
        sigma, action = hit[-1]
        breaches.append(Breach(metric=metric, sigma=sigma, value=value, mean=mean, stdev=stdev, action=action))
    return breaches


# ---------------------------------------------------------------- response


def run(
    cfg: Config,
    *,
    scm: Scm,
    agent: Agent | None,
    sb: Sandbox | None,
    bands_path: Path,
    root: Path | None = None,
    now: datetime | None = None,
) -> list[Breach]:
    """Detect breaches under ``root`` (default ``cfg.target_dir``) and act per tier.

    ``log``: print. ``diagnose``: read-only agent run (``Diagnosis`` schema), an incident record
    at ``docs/factory/incidents/<date>-<metric>.md`` *and* an issue carrying that record
    (labels ``INCIDENT_LABELS``) so the diagnosis outlives the checkout it was written to.
    ``propose``: draft an intent and open an issue labeled ``factory`` (incident record
    appended) so the factory re-enters through dispatch.yml. The agent is skipped (not failed)
    when ``agent``/``sb`` are not provided.
    """
    root = Path(root) if root is not None else Path(cfg.target_dir)
    now = now or datetime.now(UTC)
    bands = load_bands(bands_path)
    runs = load_runs(root, int(bands.get("window_runs", 20)))
    breaches = detect(runs, bands)
    print(f"maintain: {len(runs)} runs in window, {len(breaches)} breach(es)")
    for breach in breaches:
        print(_log_line(breach))
        if breach.action == "log":
            continue
        diagnosis = _diagnose(breach, runs, cfg=cfg, agent=agent, sb=sb)
        record = incident_markdown(breach, diagnosis, now)
        incident = root / INCIDENTS_DIR / f"{now:%Y-%m-%d}-{breach.metric}.md"
        incident.parent.mkdir(parents=True, exist_ok=True)
        incident.write_text(record, encoding="utf-8")
        print(f"maintain: wrote {incident}")
        if breach.action == "propose":
            url = scm.open_issue(
                title=f"[maintain] {breach.metric} breached the {breach.sigma}σ band",
                body=f"{draft_intent(breach, diagnosis)}\n{record}",
                labels=["factory"],
            )
        else:
            url = scm.open_issue(
                title=f"[incident] {breach.metric} {breach.sigma}σ",
                body=record,
                labels=list(INCIDENT_LABELS),
            )
        print(f"maintain: opened issue {url}")
    return breaches


def clone_target(url: str, branch: str, dest: Path) -> Path:
    """Shallow, read-only clone of ``branch`` at ``url`` into ``dest`` (no credential needed for
    a public repo; a private one relies on the orchestrator's own git credential setup)."""
    proc = subprocess.run(
        ["git", "clone", "--quiet", "--depth", "1", "--branch", branch, url, str(dest)],
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"git clone {url}@{branch} failed rc={proc.returncode}: {proc.stderr.strip()}")
    return dest


def metrics_root(cfg: Config, scratch: Path, *, env: Mapping[str, str] | None = None) -> Path:
    """The directory whose ``docs/factory/*/metrics.json`` the band check reads.

    ``$SWF_MAINTAIN_ROOT`` (a checkout of the target repo) when set, else a shallow clone of
    ``cfg.repo``@``cfg.base_branch`` under ``scratch``; ``cfg.target_dir`` is appended. Raises
    ``FileNotFoundError`` when the result has no ``docs/factory``: a worker whose cwd happens to
    lack the target must fail, not report "0 runs in window".
    """
    env = os.environ if env is None else env
    base = env.get(MAINTAIN_ROOT_ENV)
    root = (
        Path(base) if base else clone_target(f"https://github.com/{cfg.repo}.git", cfg.base_branch, scratch / "target")
    )
    if cfg.target_dir:
        root = root / cfg.target_dir
    if not (root / "docs" / "factory").is_dir():
        raise FileNotFoundError(
            f"maintain: {root} has no docs/factory (committed metrics live there); "
            f"point {MAINTAIN_ROOT_ENV} at a checkout of {cfg.repo}"
        )
    return root


def _diagnose(
    breach: Breach, runs: list[dict], *, cfg: Config, agent: Agent | None, sb: Sandbox | None
) -> Diagnosis | None:
    if agent is None or sb is None:
        print(f"maintain: no agent/sandbox; skipping diagnosis of {breach.metric}")
        return None
    evidence = "\n".join(
        f"- run {r.get('run_id', '?')}: {breach.metric}={metric_value(r, breach.metric)}" for r in runs
    )
    prompt = render_prompt("diagnose", metric=f"{breach.metric}: {_log_line(breach)}", evidence=evidence)
    sb.ensure()
    result = agent.run(
        sb,
        stage="diagnose",
        iteration=1,
        prompt=prompt,
        policy=POLICIES["diagnose"],
        schema=Diagnosis,
        cfg=cfg,
        issue_id="maintain",
    )
    if result.is_error or not result.data:
        return Diagnosis(
            metric=breach.metric,
            hypothesis=f"diagnosis unavailable: agent returned {result.subtype}",
        )
    return Diagnosis.model_validate(result.data)


def incident_markdown(breach: Breach, diagnosis: Diagnosis | None, now: datetime) -> str:
    """Incident record body (committed under ``docs/factory/incidents``)."""
    lines = [
        f"# Incident — {breach.metric} ({breach.sigma}σ, {breach.action})",
        "",
        f"- date: {now:%Y-%m-%d}",
        f"- value: {breach.value:g}",
        f"- mean: {breach.mean:.4g}",
        f"- stdev: {breach.stdev:.4g}",
        "",
        "## Hypothesis",
        diagnosis.hypothesis if diagnosis else "(no diagnosis: agent not available)",
        "",
        "## Evidence",
    ]
    lines += [f"- {e}" for e in (diagnosis.evidence if diagnosis else [])] or ["- (none)"]
    return "\n".join(lines) + "\n"


def draft_intent(breach: Breach, diagnosis: Diagnosis | None) -> str:
    """Intent text (originator's voice) for the issue opened at the ``propose`` tier."""
    proposed = (diagnosis.proposed_intent if diagnosis else None) or (
        f"As a maintainer of the factory I want `{breach.metric}` back inside its band. "
        f"The latest run measured {breach.value:g} against a window mean of {breach.mean:.4g} "
        f"(stdev {breach.stdev:.4g}), a {breach.sigma}σ deviation."
    )
    hypothesis = diagnosis.hypothesis if diagnosis else "(no diagnosis available)"
    return (
        f"{proposed}\n\n"
        f"## Why now\n{_log_line(breach)}\n\n"
        f"## Hypothesis\n{hypothesis}\n\n"
        "## Acceptance\n"
        f"- `{breach.metric}` returns to within 1σ of the window mean over the next runs.\n"
        "- No gate, hook, or test is disabled.\n"
    )


def _log_line(breach: Breach) -> str:
    return (
        f"maintain: {breach.metric} {breach.value:g} is {breach.sigma}σ off the window "
        f"(mean {breach.mean:.4g}, stdev {breach.stdev:.4g}) -> {breach.action}"
    )


# ---------------------------------------------------------------- sandbox sweep

SANDBOX_NAME_RE = re.compile(r"^swf-[a-z0-9][a-z0-9_-]*-[0-9a-f]{8}$")  # what THIS factory names
OWNER_ENV = "SWF_SANDBOX_OWNER"
CLEANUP_KIND = "sandbox_cleanup"  # the journal kind whose retry budget idempotency.py already declares
SWEEP_LIMIT = 50  # removals per sweep: one nightly pass must stay bounded and observable


class SandboxProvider(Protocol):
    """What the sweep needs from ``islo`` (``control.IsloClient`` in production)."""

    def listing(self) -> str: ...

    def remove(self, name: str) -> Any: ...


class CleanupControl(Protocol):
    """The backend's ``ControlKernel``: the durable journal every removal intent is written to."""

    operations: OperationJournal

    def mutate(
        self,
        ref: OperationRef,
        fn: Callable[[], Any],
        *,
        replay_safe: bool = False,
        reconcile: Callable[[], MutationOutcome] | None = None,
    ) -> Any: ...


def owned_sandboxes(list_json: str, owner: str) -> list[dict]:
    """Entries of an ``islo ls --output json`` listing whose ``created_by`` is ``owner``.

    Pure. Tolerates a JSON array or an object wrapping one; drops deleted entries and anything
    whose creator is missing or different. Never call ``islo ls --all`` to feed this.
    """
    owner = (owner or "").strip().lower()
    if not owner:
        return []
    try:
        data = json.loads(list_json)
    except ValueError:
        return []
    if isinstance(data, dict):
        data = next((v for v in data.values() if isinstance(v, list)), [])
    if not isinstance(data, list):
        return []
    mine: list[dict] = []
    for item in data:
        if not isinstance(item, dict) or item.get("status") == "deleted":
            continue
        if str(item.get("created_by") or "").strip().lower() != owner:
            continue
        mine.append(item)
    return mine


def sweep_orphans(list_json: str, ttl_s: int, now: datetime, *, owner: str) -> list[str]:
    """Names of factory sandboxes (``swf-<slug>-<run8>``) created by ``owner`` older than ``ttl_s``.

    Two independent filters, both required: the entry's ``created_by`` equals ``owner`` and the
    name matches the factory's own naming pattern. Without an owner nothing is ever returned.
    Age and naming only *nominate*: whether a nominee is an orphan is the Cell's call (below).
    """
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    names: list[str] = []
    for item in owned_sandboxes(list_json, owner):
        name = str(item.get("name") or "")
        if not SANDBOX_NAME_RE.match(name):
            continue
        created = first_timestamp(item, CREATED_KEYS)
        if created is not None and (now - created).total_seconds() > ttl_s:
            names.append(name)
    return names


def cell_for_sandbox(name: str, cells: Iterable[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    """The Cell whose bound Airflow run named this sandbox, else ``None``.

    The link is the one ``dags/blueprints.py`` builds the name from: ``swf-<slug>-<run8>`` with
    ``run8 = run_id_for(airflow_run_id, map_index)``. A re-activated Cell clears its run binding,
    so a sandbox from an earlier epoch is unowned here -- that lifecycle is over, which is the only
    reason the Cell could re-arm at all.
    """
    run8 = name.rsplit("-", 1)[-1]
    for cell in cells:
        run_id = cell.get("airflow_run_id")
        if run_id and run_id_for(str(run_id), int(cell.get("map_index") or 0)) == run8:
            return cell
    return None


def sandbox_identity(item: Mapping[str, Any], cell: Mapping[str, Any] | None, *, owner: str) -> SandboxIdentity:
    """Durable identity a removal is journaled under.

    Owned: the Cell and epoch that bound the run. Unowned: the deterministic identity of the
    resource itself, so a lost ``rm`` on a Cell-less sandbox is still remembered. ``attempt_id`` is
    the provider's creation stamp: a later sandbox reusing the name is a different resource and
    must not inherit this one's receipt.
    """
    name = str(item.get("name") or "")
    created = first_timestamp(item, CREATED_KEYS)
    attempt = created.isoformat() if created is not None else name.rsplit("-", 1)[-1]
    if cell is None:
        return SandboxIdentity("islo", CellIdentity(owner, "islo", name).stable_id(), 1, attempt)
    return SandboxIdentity("islo", str(cell["cell_id"]), int(cell["epoch"]), attempt)


def authorize_removal(
    identity: SandboxIdentity, item: Mapping[str, Any], cell: Mapping[str, Any] | None
) -> CleanupDecision:
    """``sandbox_governance.authorize_cleanup`` over durable state: islo carries no labels, so the
    Cell row (or the resource's own identity) is the label authority. A live Cell at its current
    epoch keeps its sandbox however old it is."""
    observation = ResourceObservation(str(item.get("name") or ""), identity.labels, item.get("status") == "running")
    if cell is None:
        return authorize_cleanup(identity, observation, current_epoch=1, active=False)
    return authorize_cleanup(
        identity, observation, current_epoch=int(cell["epoch"]), active=cell.get("state") not in TERMINAL_STATES
    )


def sweep_sandboxes(
    ttl_s: int,
    *,
    owner: str,
    islo: SandboxProvider,
    cells: Iterable[Mapping[str, Any]],
    control: CleanupControl,
    now: datetime | None = None,
    limit: int = SWEEP_LIMIT,
) -> dict[str, list[str]]:
    """Remove this owner's orphaned factory sandboxes older than ``ttl_s``; the backend's sweep.

    ``owner`` is REQUIRED: the sweep refuses to run when it cannot prove whose sandboxes it is
    looking at. Lists with plain ``islo ls`` (own scope, never ``--all``), filters by
    ``created_by`` and factory naming, then asks the Cell store before every ``rm``: a sandbox a
    live Cell owns at its current epoch is kept. Each removal is a ``sandbox_cleanup`` operation in
    the journal, so a lost ``rm`` reply is observed, retried or settled -- never a print.
    ``debt`` names the removals still unresolved after this pass; ``reconciled`` the earlier
    in-doubt removals this pass could settle.
    """
    report: dict[str, list[str]] = {"removed": [], "kept": [], "debt": [], "reconciled": []}
    if not owner:
        print(f"maintain: {OWNER_ENV} not set; refusing to sweep sandboxes")
        return report
    now = now or datetime.now(UTC)
    listing = islo.listing()
    items = {str(it.get("name") or ""): it for it in owned_sandboxes(listing, owner)}
    debt = CleanupDebt()
    for name in sweep_orphans(listing, ttl_s, now, owner=owner)[: max(limit, 0)]:
        item, cell = items[name], cell_for_sandbox(name, cells)
        identity = sandbox_identity(item, cell, owner=owner)
        decision = authorize_removal(identity, item, cell)
        if decision != CleanupDecision.REMOVE:
            print(f"maintain: keeping {name}: {decision} (owned by {identity.cell_id}@{identity.epoch})")
            report["kept"].append(name)
            continue
        debt.record(name, identity)
        ref = OperationRef.build(identity.cell_id, identity.epoch, CLEANUP_KIND, name, identity.attempt_id)
        if _remove(ref, name, islo=islo, control=control, owner=owner) is not None:
            debt.settle(name, identity)
            print(f"maintain: removed orphan sandbox {name}")
            report["removed"].append(name)
    report["debt"] = sorted(debt.outstanding)
    report["reconciled"] = _settle_absent(control.operations, present=set(items))
    return report


def _receipt(ref: OperationRef, name: str, status: CleanupStatus, *, requested_at: float) -> dict[str, Any]:
    return CleanupReceipt.build(
        cell_id=ref.cell_id,
        epoch=ref.epoch,
        operation_key=ref.key,
        provider="islo",
        resource_id=name,
        status=status,
        requested_at=requested_at,
    ).to_dict()


def _remove(ref: OperationRef, name: str, *, islo: SandboxProvider, control: CleanupControl, owner: str) -> Any:
    """One journaled ``islo rm``; the committed receipt, or ``None`` while the outcome is unresolved.

    The journal fails closed on a row it already holds, so a retry first *observes*: still listed
    means the effect is absent and ``rm`` may run again; unlisted means it converged and only the
    reply was lost. When ``rm`` itself raises, the same observation runs at once, so the common
    lost-reply case settles in the sweep that caused it.
    """
    requested_at = time.time()

    def observe() -> MutationOutcome:
        evidence = {"resource": name}
        try:
            present = any(it.get("name") == name for it in owned_sandboxes(islo.listing(), owner))
        except Exception as e:  # noqa: BLE001 - an unreadable provider is an unknown outcome, not a verdict
            return MutationOutcome("ambiguous", evidence=evidence, detail=str(e)[:500])
        if present:
            return MutationOutcome("definitely_absent", evidence=evidence, detail="sandbox still listed")
        return MutationOutcome("committed", _receipt(ref, name, "already_absent", requested_at=requested_at), evidence)

    def rm() -> dict[str, Any]:
        islo.remove(name)
        return _receipt(ref, name, "converged", requested_at=requested_at)

    try:
        return control.mutate(ref, rm, replay_safe=True, reconcile=observe)
    except Exception as e:  # noqa: BLE001 - one unresolved rm must not abort the sweep; the journal holds it
        print(f"maintain: islo rm {name} unresolved ({e}); observing")
    try:
        outcome = control.operations.observe(ref, observe)
    except (KeyError, OperationError) as e:  # refused before an intent existed, or the row moved under us
        print(f"maintain: {name} left as cleanup debt: {e}")
        return None
    return outcome.result if outcome.status == "committed" else None


def _settle_absent(journal: OperationJournal, *, present: set[str]) -> list[str]:
    """Settle earlier in-doubt removals whose sandbox no longer appears in the own-scope listing.

    A lost ``rm`` whose follow-up listing also failed leaves a row that names its resource in the
    observation; once that resource is gone the effect converged, and the row must say so or the
    fleet carries phantom cleanup debt forever.
    """
    settled: list[str] = []
    for row in journal.unresolved(limit=1000):
        if row.get("kind") != CLEANUP_KIND:
            continue
        name = str(((row.get("observation") or {}).get("evidence") or {}).get("resource") or "")
        if not name or name in present:
            continue
        ref = OperationRef(str(row["cell_id"]), int(row["epoch"]), CLEANUP_KIND, str(row["operation_key"]))
        receipt = _receipt(ref, name, "already_absent", requested_at=float(row.get("updated_at") or time.time()))
        absent = MutationOutcome("committed", receipt, {"resource": name})
        if journal.observe(ref, lambda outcome=absent: outcome).status == "committed":
            print(f"maintain: reconciled lost removal of {name}")
            settled.append(name)
    return settled


def request_sweep(ttl_s: int, *, env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """The DAG/CLI side: ask the backend to run ``sweep_sandboxes``.

    A worker never runs ``islo rm`` itself. The Cell store that says who owns a sandbox and the
    journal that remembers a removal live with the backend; a sweep that guessed from age alone is
    the failure this exists to prevent, so without a backend it refuses rather than falls back.
    """
    env = os.environ if env is None else env
    if not env.get("SWF_BACKEND_URL"):
        print("maintain: SWF_BACKEND_URL not set; refusing to sweep sandboxes without the Cell store")
        return {"removed": [], "kept": [], "debt": [], "reconciled": [], "refused": "SWF_BACKEND_URL not set"}
    # Removals are serial and each one is a provider round trip; a bounded sweep still needs minutes.
    return cell_callback.post("/workers/sweep", {"ttl_s": int(ttl_s)}, env=env, timeout=600)
