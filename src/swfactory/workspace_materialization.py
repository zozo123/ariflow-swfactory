"""Large-repository workspace materialization and cache planning."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class MaterializationPlan:
    repo: str
    base_sha: str
    mode: str
    sparse_paths: tuple[str, ...]
    shallow: bool
    partial_clone: bool
    cache_key: str


def plan_materialization(
    repo: str,
    base_sha: str,
    *,
    required_paths: Iterable[str],
    allow_sparse: bool = True,
    require_full_checkout: bool = False,
) -> MaterializationPlan:
    paths = tuple(sorted({_normalize(path) for path in required_paths if path.strip()}))
    mode = "full" if require_full_checkout or not allow_sparse else "sparse"
    sparse = () if mode == "full" else paths
    raw = "\0".join((repo, base_sha, mode, *sparse)).encode()
    return MaterializationPlan(
        repo=repo,
        base_sha=base_sha,
        mode=mode,
        sparse_paths=sparse,
        shallow=True,
        partial_clone=mode == "sparse",
        cache_key="workspace:" + hashlib.sha256(raw).hexdigest()[:24],
    )


def verify_sparse_coverage(plan: MaterializationPlan, required_paths: Iterable[str]) -> tuple[bool, tuple[str, ...]]:
    if plan.mode == "full":
        return True, ()
    missing = []
    for path in map(_normalize, required_paths):
        if not any(path == root or path.startswith(root.rstrip("/") + "/") for root in plan.sparse_paths):
            missing.append(path)
    return not missing, tuple(sorted(set(missing)))


def _normalize(path: str) -> str:
    return path.replace("\\", "/").lstrip("./")
