"""Shared Factory Cell binding helpers for backend and Airflow runtime.

The durable cell is one issue x target lifecycle. Airflow remains the scheduler; these helpers only
make cell identity/epoch/policy explicit in mapped-job data so every runtime surface agrees on the
same authority/evidence root.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from swfactory.cells import CellIdentity, CellStore


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


def ensure_bindings(store: CellStore, jobs: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    bindings: list[dict[str, Any]] = []
    for job in jobs:
        row = store.ensure(identity_for_job(job))
        bindings.append(
            {
                "job_idx": int(job["job_idx"]),
                "cell_id": row["cell_id"],
                "epoch": int(row["epoch"]),
                "policy_digest": row.get("policy_digest"),
                "factory_generation": row.get("factory_generation"),
            }
        )
    return bindings


def bind_jobs(
    jobs: Iterable[dict[str, Any]], bindings: Iterable[dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    """Attach verified cell metadata to mapped jobs.

    Backend-managed runs carry explicit bindings in Airflow run conf. Direct/legacy Airflow runs
    still receive the deterministic cell id, but are marked unmanaged and use epoch 1 only as
    descriptive evidence; mutation authority must be checked by the backend before side effects.
    """
    by_index: dict[int, dict[str, Any]] = {}
    for raw in bindings or ():
        if not isinstance(raw, dict):
            raise ValueError("factory cell bindings must be objects")
        idx = raw.get("job_idx")
        if type(idx) is not int or idx < 0 or idx in by_index:
            raise ValueError("factory cell binding job_idx must be a unique non-negative integer")
        by_index[idx] = raw

    out: list[dict[str, Any]] = []
    for raw_job in jobs:
        job = dict(raw_job)
        idx = int(job["job_idx"])
        expected = identity_for_job(job).stable_id()
        binding = by_index.get(idx)
        if binding is None:
            job.update(
                cell_id=expected,
                cell_epoch=1,
                cell_managed=False,
                cell_policy_digest=None,
                cell_generation=None,
            )
        else:
            if binding.get("cell_id") != expected:
                raise ValueError(f"factory cell binding mismatch for mapped job {idx}")
            epoch = binding.get("epoch")
            if type(epoch) is not int or epoch < 1:
                raise ValueError(
                    f"factory cell binding epoch must be positive for mapped job {idx}"
                )
            policy_digest = binding.get("policy_digest")
            if policy_digest is not None and (
                not isinstance(policy_digest, str) or not policy_digest.startswith("policy:")
            ):
                raise ValueError(f"invalid factory cell policy digest for mapped job {idx}")
            generation = binding.get("factory_generation")
            if generation is not None and not isinstance(generation, str):
                raise ValueError(f"invalid factory generation for mapped job {idx}")
            job.update(
                cell_id=expected,
                cell_epoch=epoch,
                cell_managed=True,
                cell_policy_digest=policy_digest,
                cell_generation=generation,
            )
        out.append(job)

    unknown = sorted(set(by_index) - {int(job["job_idx"]) for job in out})
    if unknown:
        raise ValueError(f"factory cell bindings reference unknown mapped jobs {unknown}")
    return out
