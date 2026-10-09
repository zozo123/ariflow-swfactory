"""The one table of authenticated ``POST /v1/...`` factory backend operations.

Every route takes the Factory and the JSON request body. The paths are a wire contract -- the Rust
console (``swf-adapters``), Airflow workers and the Python CLI call them by name -- so none is
renamed here alone.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Callable
from functools import partial
from typing import Any

from swfactory import blueprint, maintain
from swfactory.cells import SCHEMA_VERSION
from swfactory.control import ControlError, GitHubClient, IsloClient, MetricsSource
from swfactory.doctor import _check_managed_workers
from swfactory.inspection import inspect_run, list_runs
from swfactory.models import StageError
from swfactory.product_surface import build_preview

from . import autonomous_service as autonomous
from . import core_service as core
from . import population_service as population
from . import scm_service as scm
from .service import Factory, Refused, text

Route = Callable[[Factory, dict[str, Any]], Any]


def _limit(body: dict[str, Any]) -> int:
    limit = body.get("limit", 30)
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("limit must be between 1 and 1000")
    return limit


def _pr_number(body: dict[str, Any]) -> int:
    number = body.get("number")
    if type(number) is not int or number <= 0:
        raise ValueError("number must be a positive integer")
    return number


def _row(
    name: str,
    ok: bool,
    *,
    detail: str | None = None,
    fix: str | None = None,
    required: bool = True,
    bad: str = "fail",
) -> dict[str, Any]:
    """One ``/doctor`` row: ``ok`` as a bool, a lowercase ``status`` (``warn`` when not required)."""
    status = "ok" if ok else bad if required else "warn"
    row = {"name": name, "ok": bool(ok), "status": status, "detail": detail, "fix": fix, "required": required}
    return {key: value for key, value in row.items() if value is not None}


def _doctor(factory: Factory, _body: dict[str, Any]) -> list[dict[str, Any]]:
    caps = factory.capabilities()
    # Every row carries `ok` as well as `status`. The console deserializes into
    # `swf_domain::doctor::Check`, whose `ok: bool` has no default and no alias — so a row
    # with only `status` fails to parse, the whole response is discarded, and `swf doctor`
    # prints one fabricated failure blaming the operator's token. `doctor` is the command
    # someone runs when nothing else works; it must not be the thing that lies to them.
    # `status` is kept alongside for the Python CLI, which reads it.
    checks = [
        _row("factory backend", True, detail="Python API v1"),
        _row("factory cells", True, detail=f"durable CellStore schema v{SCHEMA_VERSION}"),
        # A capability document rendered as text: `detail` is a string on both sides, and an
        # object here failed to parse even once `ok` was present.
        _row(
            "mutation readiness",
            caps["mutation_ready"],
            detail=", ".join(f"{k}={v}" for k, v in sorted(caps.items())),
            bad="warn",
        ),
    ]
    # The row the console path was missing (#2050 added it to the Python doctor only): a
    # managed cell fails closed in its FIRST stage without SWF_BACKEND_URL/SWF_BACKEND_TOKEN,
    # and from `swf doctor` that looked like a healthy backend. One honesty caveat, written
    # into `detail`: this reads THIS process's environment. The workers carry their own copy
    # (Compose passes the pair to the airflow service separately), so a green row here means
    # the backend host is configured, not that every worker is -- the compose guard in
    # tests/test_doctor.py is what pins the worker side.
    workers = _check_managed_workers(os.environ)
    # Never `required` on the backend host. The host does not call itself, so it legitimately
    # has no SWF_BACKEND_URL of its own -- the e2e harness exports that pair into the Airflow
    # process and nowhere else -- and a required row here failed `swf doctor` against every
    # healthy backend, which is #1217 by another route. Informational: a red row still shows
    # the operator that THIS host's copy is unwired, without claiming to know the workers'.
    checks.append(
        _row(
            "managed worker callback",
            workers.ok,
            detail=workers.detail + " (as seen from the backend host's environment, which is not the workers')",
            fix=workers.fix,
            required=False,
        )
    )
    try:
        health = factory._checked_airflow("GET", "/monitor/health")
        for name in ("metadatabase", "scheduler"):
            healthy = (health.get(name) or {}).get("status") == "healthy"
            fix = "" if healthy else "restore the Airflow service"
            checks.append(_row(name, healthy, detail="Airflow health", fix=fix))
        factory._checked_airflow("GET", "/dags?limit=1")
        checks.append(_row("airflow auth", True))
    except (Refused, ControlError, OSError):
        checks.append(
            _row(
                "airflow",
                False,
                detail="Airflow is unavailable or authentication failed",
                fix="check AIRFLOW_URL and credentials on the backend",
            )
        )
    for tool, configured in (("gh", bool(factory.repo)), ("islo", bool(factory.owner))):
        present = bool(shutil.which(tool))
        checks.append(
            _row(
                tool,
                present,
                detail=f"backend tool installed={present}, configured={configured}; credentials not probed",
                fix="" if present else f"install {tool} on the backend if needed",
                required=False,
            )
        )
    return checks


def _queue_cancel(factory: Factory, body: dict[str, Any]) -> dict[str, Any]:
    work_id = text(body, "work_id")
    admission = factory.control.admission
    state = admission.state_of(work_id)
    if state is None:
        raise Refused(404, f"no admission work {work_id}")
    if state == "bound":
        # The Airflow run already exists; cancelling it here would release capacity while
        # the compute keeps going. Its Factory Cells own that decision.
        raise Refused(409, "a dispatched work order is cancelled through its Factory Cells")
    # An order can already have activated its Cells and still be cancellable: a delivery
    # that failed after activation leaves it admitted with live Cells behind it. Closing
    # the reservation without closing those Cells leaves a live Cell no work order owns --
    # and because the Cell's epoch is part of the deterministic work id, the identity then
    # answers 409 to every resubmission forever. That is #2058 with the arrows reversed.
    reason = text(body, "reason", max_len=512)
    cancelled_cells = factory._cancel_activations(work_id, f"queue-cancel:{work_id}")
    released = admission.cancel(work_id, reason=reason)
    return {
        "work_id": work_id,
        "was": state,
        "state": admission.state_of(work_id),
        "cancelled_cells": cancelled_cells,
        "released_work": released,
        "resumed_dispatch": factory.resume_dispatch(),
        "dispatch": admission.dispatch_row(work_id),
    }


def _queue_inspect(factory: Factory, body: dict[str, Any]) -> dict[str, Any]:
    work_id = text(body, "work_id")
    snapshot = factory.control.admission.snapshot(limit=1000)
    for section in ("active", "queued"):
        for row in snapshot[section]:
            if row["work_id"] == work_id:
                return row
    raise Refused(404, f"no admission work {work_id}")


def _operation(factory: Factory, body: dict[str, Any]) -> dict[str, Any]:
    try:
        return factory.control.operations.get(text(body, "operation_key"))
    except KeyError as error:
        raise Refused(404, "no such operation") from error


def _lease(factory: Factory, body: dict[str, Any]) -> dict[str, Any]:
    try:
        return factory.leases.inspect(text(body, "lease_id"))
    except KeyError as error:
        raise Refused(404, "no such credential lease") from error


def _preview(factory: Factory, body: dict[str, Any]) -> dict[str, Any]:
    line = factory._line(text(body, "line"))
    issues = body.get("issues") or []
    targets = body.get("targets") or []
    conf = {"issues": issues, **({"targets": targets} if targets else {})}
    return build_preview(line=line.name, jobs=list(line.jobs(conf))).to_dict()


def _history(factory: Factory, body: dict[str, Any]) -> list[dict[str, Any]]:
    cell_id = text(body, "cell_id")
    factory._cell(cell_id)
    return factory.cell_store.history(cell_id)


def _verify(factory: Factory, body: dict[str, Any]) -> dict[str, Any]:
    cell_id = text(body, "cell_id")
    ok, tail = factory.evidence.verify(cell_id)
    return {"cell_id": cell_id, "verified": ok, "tail_digest": tail}


def _deliveries(factory: Factory, body: dict[str, Any], *, kind: str) -> Any:
    """The factory-labelled ``prs`` or ``issues``; no repository means no credential, so none."""
    if not factory.repo:
        return []
    listing = getattr(GitHubClient(factory.repo), kind)
    return listing(label=text(body, "label", default="factory"), limit=_limit(body))


def _pull_request(factory: Factory, read: str, key: str | int) -> Any:
    """One ``GitHubClient`` PR read through the backend's own ``gh`` credential and repository."""
    if not factory.repo:
        raise Refused(503, "SWF_REPO is not configured on the backend")
    try:
        return getattr(GitHubClient(factory.repo), read)(key)
    except ControlError as error:
        raise Refused(502, "GitHub operation failed; check backend credentials and repository") from error


def _sweep(factory: Factory, body: dict[str, Any]) -> dict[str, Any]:
    # The nightly orphan sweep runs HERE, next to the Cell store and the operation journal:
    # a worker deciding from age alone is #2075. Cells say who still owns a sandbox; the
    # journal keeps every removal intent so a lost ``islo rm`` reply is reconciled, not printed.
    # A docker factory (``SWF_SANDBOX=docker``) sweeps its labelled work containers the same
    # way (#2052); any other factory never runs ``docker`` here.
    ttl_s = body.get("ttl_s")
    if type(ttl_s) is not int or ttl_s < 1:
        raise ValueError("ttl_s must be a positive integer")
    return maintain.sweep_sandboxes(
        ttl_s,
        owner=factory.owner,
        islo=IsloClient(factory.owner),
        cells=factory.cell_store.list(limit=1000),
        control=factory.control,
        docker=maintain.select_containers(os.environ),
    )


def _lines(_factory: Factory, _body: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "name": bp.name,
            "targets": [t.repo for t in bp.targets],
            "route": list(bp.order),
            "gates": [g.model_dump() for g in bp.gates],
        }
        for bp in (blueprint.load(str(p)) for p in blueprint.blueprint_paths())
    ]


def _on_repository(route: Callable[[Factory, dict[str, Any], str], Any]) -> Route:
    """An SCM route acting on the configured repository: refused without one, base branch checked first."""

    def scoped(factory: Factory, body: dict[str, Any]) -> Any:
        if not factory.repo:
            raise Refused(503, "SWF_REPO is not configured on the backend")
        return route(factory, body, text(body, "base_branch", default="main"))

    return scoped


def _autonomous(decide: Route) -> Route:
    """An autonomous decision on the configured repository; a ``StageError`` answers as a refusal."""

    def refusing(factory: Factory, body: dict[str, Any], _base_branch: str) -> Any:
        try:
            return decide(factory, body)
        except StageError as error:
            raise Refused(503 if error.retryable else 403, str(error)) from error

    return _on_repository(refusing)


ROUTES: dict[str, Route] = {
    "/doctor": _doctor,
    "/compatibility": lambda factory, _body: factory.capabilities(),
    "/fleet": lambda factory, _body: factory.fleet(),
    "/queue": lambda factory, body: factory.control.admission.snapshot(limit=_limit(body)),
    # The explicit operator handle on the same redelivery the request paths pump, for the case
    # where a backend restarted and nothing has submitted or transitioned since.
    "/queue/resume": lambda factory, body: {"resumed": factory.resume_dispatch(limit=_limit(body))},
    "/queue/cancel": _queue_cancel,
    "/queue/inspect": _queue_inspect,
    "/operations": lambda factory, body: factory.control.operations.unresolved(limit=_limit(body)),
    "/operations/inspect": _operation,
    "/work-orders": lambda factory, body: factory.submit(body),
    "/blueprints/preview": _preview,
    "/cells": lambda factory, body: factory.cell_store.list(limit=_limit(body)),
    "/cells/inspect": lambda factory, body: factory._cell(text(body, "cell_id")),
    "/cells/history": _history,
    "/cells/transition": lambda factory, body: factory._transition(body),
    "/leases/denials": lambda factory, body: factory.leases.denials(limit=_limit(body)),
    "/leases/inspect": _lease,
    "/evidence/verify": _verify,
    "/evidence/checkpoint": lambda factory, body: factory.evidence.checkpoint(text(body, "cell_id")),
    "/deliveries/prs": partial(_deliveries, kind="prs"),
    "/deliveries/issues": partial(_deliveries, kind="issues"),
    "/deliveries/head": lambda factory, body: _pull_request(factory, "pr_for_head", text(body, "branch")),
    "/deliveries/checks": lambda factory, body: _pull_request(factory, "pr_checks", _pr_number(body)),
    "/deliveries/url": lambda factory, body: _pull_request(factory, "pr_url", _pr_number(body)),
    "/workers": lambda factory, _body: IsloClient(factory.owner).own_sandboxes(),
    "/workers/remove": lambda factory, body: IsloClient(factory.owner).remove(text(body, "name")),
    "/workers/sweep": _sweep,
    "/metrics/runs": lambda factory, _body: MetricsSource(factory.root).runs(),
    "/metrics/summary": lambda factory, _body: MetricsSource(factory.root).summary(),
    "/state/runs": lambda factory, body: list_runs(factory.state_root, limit=_limit(body)),
    "/state/inspect": lambda factory, body: inspect_run(factory.state_root, text(body, "run_id")),
    "/lines": _lines,
    "/core/inspect": lambda factory, body: core.operator_projection(factory, text(body, "cell_id")),
    "/core/recovery-plan": lambda factory, body: core.recovery_plan(factory, text(body, "operation_key")),
    "/population/execute": population.execute,
    "/population/artifact": population.artifact,
    "/scm/autonomy-status": scm.autonomy_status,
    "/scm/linear-source": scm.linear_source,
    "/scm/issue": _on_repository(scm.fetch_issue),
    "/scm/publish": _on_repository(scm.publish),
    "/scm/open-issue": _on_repository(scm.open_issue),
    "/scm/triage": _autonomous(autonomous.triage),
    "/scm/policy-gate": _autonomous(autonomous.policy_gate),
    "/scm/merge": _autonomous(autonomous.merge),
}
