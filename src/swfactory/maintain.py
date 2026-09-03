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
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel

from swfactory.agent import POLICIES, Agent, render_prompt
from swfactory.config import Config
from swfactory.metrics import CREATED_KEYS, first_timestamp, load_all
from swfactory.models import Diagnosis
from swfactory.sandbox import Sandbox
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
        breaches.append(
            Breach(metric=metric, sigma=sigma, value=value, mean=mean, stdev=stdev, action=action)
        )
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
        raise RuntimeError(
            f"git clone {url}@{branch} failed rc={proc.returncode}: {proc.stderr.strip()}"
        )
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
        Path(base)
        if base
        else clone_target(f"https://github.com/{cfg.repo}.git", cfg.base_branch, scratch / "target")
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
        f"- run {r.get('run_id', '?')}: {breach.metric}={metric_value(r, breach.metric)}"
        for r in runs
    )
    prompt = render_prompt(
        "diagnose", metric=f"{breach.metric}: {_log_line(breach)}", evidence=evidence
    )
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

Runner = Callable[[Sequence[str]], Any]


SANDBOX_NAME_RE = re.compile(r"^swf-[a-z0-9][a-z0-9_-]*-[0-9a-f]{8}$")  # what THIS factory names
OWNER_ENV = "SWF_SANDBOX_OWNER"


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


def remove_orphans(names: Sequence[str], runner: Runner) -> list[str]:
    """Best-effort ``islo rm`` for each factory-named sandbox via ``runner``; returns removed."""
    removed: list[str] = []
    for name in names:
        if not SANDBOX_NAME_RE.match(name):  # defense in depth: never rm a foreign name
            print(f"maintain: refusing to remove non-factory sandbox {name!r}")
            continue
        try:
            runner(["islo", "rm", name, "--output", "plain"])
        except Exception as e:  # noqa: BLE001 - one failed rm must not abort the sweep
            print(f"maintain: failed to remove {name}: {e}")
            continue
        print(f"maintain: removed orphan sandbox {name}")
        removed.append(name)
    return removed


def sweep_sandboxes(
    ttl_s: int, *, owner: str | None = None, runner: Runner | None = None
) -> list[str]:
    """Remove this owner's orphaned factory sandboxes older than ``ttl_s``.

    ``owner`` (or ``$SWF_SANDBOX_OWNER``) is REQUIRED: the sweep refuses to run when it cannot
    prove whose sandboxes it is looking at. Lists with plain ``islo ls`` (own scope, never
    ``--all``) and still filters by ``created_by``.
    """
    owner = owner or os.environ.get(OWNER_ENV, "")
    if not owner:
        print(f"maintain: {OWNER_ENV} not set; refusing to sweep sandboxes")
        return []
    runner = runner or _islo
    listing = runner(["islo", "ls", "--output", "json"])
    names = sweep_orphans(str(listing or ""), ttl_s, datetime.now(UTC), owner=owner)
    return remove_orphans(names, runner)


def _islo(argv: Sequence[str]) -> str:
    proc = subprocess.run(list(argv), capture_output=True, text=True, timeout=300, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"{' '.join(argv)} failed rc={proc.returncode}: {proc.stderr.strip()}")
    return proc.stdout
