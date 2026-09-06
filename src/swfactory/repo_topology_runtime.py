"""Deterministic repository topology discovery and verified materialization planning.

This module converts repository facts into read-only planning inputs.  Unsupported syntax broadens
validation; it never narrows safety.  Mutable worktrees remain cell/node-local.
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class ModuleInfo:
    name: str
    root: str
    manifests: tuple[str, ...] = ()
    dependencies: tuple[str, ...] = ()
    owners: tuple[str, ...] = ()


@dataclass(frozen=True)
class WorkflowImpact:
    workflow: str
    paths: tuple[str, ...] = ()
    paths_ignore: tuple[str, ...] = ()
    required_checks: tuple[str, ...] = ()
    conservative: bool = False


@dataclass(frozen=True)
class DiscoverySnapshot:
    schema_version: int
    modules: tuple[ModuleInfo, ...]
    workflows: tuple[WorkflowImpact, ...]
    digest: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class MaterializationRequest:
    source: str
    commit: str
    required_paths: tuple[str, ...]
    protected_paths: tuple[str, ...] = ()
    control_paths: tuple[str, ...] = ("factory.toml", ".github/workflows")
    allow_sparse: bool = True
    allow_partial: bool = True


@dataclass(frozen=True)
class MaterializationReceipt:
    mode: str
    source: str
    commit: str
    requested_paths: tuple[str, ...]
    present_paths: tuple[str, ...]
    missing_paths: tuple[str, ...]
    fallback_reason: str | None
    cache_key: str

    @property
    def verified(self) -> bool:
        return not self.missing_paths


def discover_modules(root: Path) -> tuple[ModuleInfo, ...]:
    """Discover common package/build manifests without executing repository code."""
    manifests = {
        "pyproject.toml",
        "package.json",
        "Cargo.toml",
        "go.mod",
        "pom.xml",
        "build.gradle",
        "build.gradle.kts",
        "WORKSPACE",
        "WORKSPACE.bazel",
        "MODULE.bazel",
    }
    found: list[ModuleInfo] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name not in manifests:
            continue
        rel = path.relative_to(root).as_posix()
        module_root = path.parent.relative_to(root).as_posix() or "."
        found.append(ModuleInfo(name=module_root, root=module_root, manifests=(rel,)))
    merged: dict[str, ModuleInfo] = {}
    for item in found:
        prior = merged.get(item.root)
        manifests_for_root = tuple(sorted(set((prior.manifests if prior else ()) + item.manifests)))
        merged[item.root] = ModuleInfo(name=item.root, root=item.root, manifests=manifests_for_root)
    return tuple(merged[key] for key in sorted(merged))


def apply_codeowners(modules: Iterable[ModuleInfo], text: str) -> tuple[ModuleInfo, ...]:
    """Apply GitHub CODEOWNERS last-match semantics to module roots."""
    rules: list[tuple[str, tuple[str, ...]]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        rules.append((parts[0].lstrip("/"), tuple(parts[1:])))
    out: list[ModuleInfo] = []
    for module in modules:
        owners: tuple[str, ...] = ()
        probe = module.root.rstrip("/") + "/"
        for pattern, candidate in rules:
            normalized = pattern.rstrip("/")
            if (
                fnmatch.fnmatch(module.root, normalized)
                or fnmatch.fnmatch(probe, normalized + "/")
                or normalized.endswith("/**")
                and probe.startswith(normalized[:-3].rstrip("/") + "/")
            ):
                owners = candidate
        out.append(ModuleInfo(**{**asdict(module), "owners": owners}))
    return tuple(out)


def snapshot(
    modules: Iterable[ModuleInfo], workflows: Iterable[WorkflowImpact]
) -> DiscoverySnapshot:
    modules = tuple(sorted(modules, key=lambda m: (m.root, m.name)))
    workflows = tuple(sorted(workflows, key=lambda w: w.workflow))
    payload = {"modules": [asdict(m) for m in modules], "workflows": [asdict(w) for w in workflows]}
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return DiscoverySnapshot(1, modules, workflows, digest)


def impacted_workflows(
    changed_paths: Iterable[str], workflows: Iterable[WorkflowImpact]
) -> tuple[str, ...]:
    changed = tuple(sorted(set(changed_paths)))
    impacted: list[str] = []
    for workflow in sorted(workflows, key=lambda w: w.workflow):
        if workflow.conservative or not workflow.paths:
            impacted.append(workflow.workflow)
            continue
        matched = any(
            any(fnmatch.fnmatch(path, pattern) for pattern in workflow.paths) for path in changed
        )
        ignored = (
            all(
                any(fnmatch.fnmatch(path, pattern) for pattern in workflow.paths_ignore)
                for path in changed
            )
            if changed and workflow.paths_ignore
            else False
        )
        if matched and not ignored:
            impacted.append(workflow.workflow)
    return tuple(impacted)


def materialization_cache_key(
    request: MaterializationRequest, *, toolchain: Mapping[str, str] | None = None
) -> str:
    payload = {
        "source": request.source,
        "commit": request.commit,
        "required_paths": sorted(set(request.required_paths)),
        "protected_paths": sorted(set(request.protected_paths)),
        "control_paths": sorted(set(request.control_paths)),
        "allow_sparse": request.allow_sparse,
        "allow_partial": request.allow_partial,
        "toolchain": dict(sorted((toolchain or {}).items())),
    }
    return (
        "mat:"
        + hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )


def verify_materialization(
    request: MaterializationRequest,
    checkout: Path,
    *,
    mode: str,
    fallback_reason: str | None = None,
) -> MaterializationReceipt:
    required = tuple(
        sorted(set(request.required_paths + request.protected_paths + request.control_paths))
    )
    present: list[str] = []
    missing: list[str] = []
    for rel in required:
        if (checkout / rel).exists():
            present.append(rel)
        else:
            missing.append(rel)
    return MaterializationReceipt(
        mode=mode,
        source=request.source,
        commit=request.commit,
        requested_paths=required,
        present_paths=tuple(present),
        missing_paths=tuple(missing),
        fallback_reason=fallback_reason,
        cache_key=materialization_cache_key(request),
    )


def choose_mode(request: MaterializationRequest, *, coverage_provable: bool) -> str:
    if request.allow_sparse and coverage_provable:
        return "sparse-partial" if request.allow_partial else "sparse"
    if request.allow_partial:
        return "partial"
    return "full"
