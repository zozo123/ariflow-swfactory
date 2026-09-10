"""Shared Factory Cell binding helpers for backend and Airflow runtime.

The durable cell is one issue x target lifecycle. Airflow remains the scheduler; these helpers only
make cell identity/epoch/policy explicit in mapped-job data so every runtime surface agrees on the
same authority/evidence root.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from swfactory.cells import CellIdentity
from swfactory.intake_governance import require_complete_bindings

# The actor a cron tick submits itself as. The backend accepts ``airflow_run_id`` from this actor
# alone, so a run created by Airflow's scheduler is the only thing that can be attached to.
SCHEDULE_ACTOR = "airflow-schedule"
_BINDING_KEYS = frozenset({"job_idx", "cell_id", "epoch", "repo", "snapshot_digest"})


def target_identity(job: dict[str, Any]) -> str:
    directory = str(job.get("dir", "")).strip() or "."
    base_branch = str(job.get("base_branch", "main")).strip() or "main"
    return f"{directory}@{base_branch}"


def identity_for_job(job: dict[str, Any]) -> CellIdentity:
    return CellIdentity(
        repo=str(job["repo"]).strip(),
        target=target_identity(job),
        issue=str(job["issue"]).strip(),
    )


def admit_scheduled_run(line: str, dag_run_id: str, jobs: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Admit an Airflow-scheduled run through the backend's work orders; return its Cell bindings.

    A cron tick creates the run before anyone has admitted the work, so the run asks for itself:
    ``POST /v1/work-orders`` with ``airflow_run_id`` naming the run that already exists. The backend
    reserves per-repository capacity, activates the Cells and binds them to *this* run instead of
    dispatching a second copy of the schedule. The run id is the trigger identity, so a retried
    ``fan_out`` is the same order, not a new one. Anything but a bound answer -- queued behind
    capacity, refused, no backend configured -- raises: a governed line never falls back to running
    unmanaged (#2067).
    """
    from swfactory.cell_callback import CellCallbackError, post

    jobs = list(jobs)
    order = {
        "line": line,
        "actor": SCHEDULE_ACTOR,
        "airflow_run_id": dag_run_id,
        "issues": list(dict.fromkeys(str(job["issue"]) for job in jobs)),
        "targets": sorted({str(job["repo"]) for job in jobs}),
    }
    document = post("/work-orders", order)  # `post` owns the /v1 prefix
    state = document.get("state")
    if state != "submitted":
        limiting = document.get("limiting")
        raise CellCallbackError(
            f"scheduled run {dag_run_id} deferred: work order {state} ({document.get('reason')}"
            + (f", limited by {limiting})" if limiting else ")")
        )
    if document.get("run_id") != dag_run_id:
        raise CellCallbackError("backend bound the work order to a different Airflow run")
    bindings = document.get("bindings")
    if not isinstance(bindings, list):
        raise CellCallbackError("backend returned no Factory Cell bindings for the scheduled run")
    return bindings


def bind_jobs(jobs: Iterable[dict[str, Any]], bindings: Iterable[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Attach verified cell metadata to mapped jobs.

    Backend-managed runs carry explicit bindings (in Airflow run conf, or answered to a scheduled
    run's own admission). A bindings list is all or nothing: ``require_complete_bindings`` refuses a
    partial one, because a run half managed lets the other half execute with no admission behind it.
    Direct/legacy Airflow runs (no list at all) still receive the deterministic cell id, but are
    marked unmanaged and use epoch 1 only as descriptive evidence; mutation authority must be
    checked by the backend before side effects.
    """
    out = [dict(job) for job in jobs]
    if bindings is None:
        for job in out:
            job.update(
                cell_id=identity_for_job(job).stable_id(),
                cell_epoch=1,
                cell_managed=False,
                cell_policy_digest=None,
                cell_generation=None,
            )
        return out

    rows = list(bindings)
    if not all(isinstance(raw, dict) and raw.keys() >= _BINDING_KEYS for raw in rows):
        raise ValueError(f"factory cell bindings must be objects naming {sorted(_BINDING_KEYS)}")
    parsed = {item.job_idx: item for item in require_complete_bindings(rows, len(out))}
    if len({item.snapshot_digest for item in parsed.values()}) != 1:
        raise ValueError("factory cell bindings must come from one work order")
    raw_by_index = {int(raw["job_idx"]): raw for raw in rows}
    for job in out:
        idx = int(job["job_idx"])
        binding, raw = parsed[idx], raw_by_index[idx]
        identity = identity_for_job(job)
        if binding.cell_id != identity.stable_id() or binding.repo != identity.repo:
            raise ValueError(f"factory cell binding mismatch for mapped job {idx}")
        policy_digest = raw.get("policy_digest")
        if policy_digest is not None and (
            not isinstance(policy_digest, str) or not policy_digest.startswith("policy:")
        ):
            raise ValueError(f"invalid factory cell policy digest for mapped job {idx}")
        generation = raw.get("factory_generation")
        if generation is not None and not isinstance(generation, str):
            raise ValueError(f"invalid factory generation for mapped job {idx}")
        job.update(
            cell_id=binding.cell_id,
            cell_epoch=binding.epoch,
            cell_managed=True,
            cell_policy_digest=policy_digest,
            cell_generation=generation,
        )
    return out
