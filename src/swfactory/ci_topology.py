"""Deterministic repository/CI topology model for targeted verification."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Iterable


@dataclass(frozen=True)
class Module:
    name: str
    roots: tuple[str, ...]
    depends_on: tuple[str, ...] = ()
    checks: tuple[str, ...] = ()


@dataclass(frozen=True)
class Topology:
    modules: tuple[Module, ...]
    full_suite_checks: tuple[str, ...] = ()

    def impacted(self, changed: Iterable[str]) -> tuple[str, ...]:
        changed_paths = tuple(_norm(p) for p in changed)
        directly = {
            module.name
            for module in self.modules
            if any(any(_under(path, root) for root in module.roots) for path in changed_paths)
        }
        impacted = set(directly)
        changed_any = True
        while changed_any:
            changed_any = False
            for module in self.modules:
                if module.name in impacted:
                    continue
                if any(dep in impacted for dep in module.depends_on):
                    impacted.add(module.name)
                    changed_any = True
        return tuple(module.name for module in self.modules if module.name in impacted)

    def required_checks(self, changed: Iterable[str], *, require_full_suite: bool = False) -> tuple[str, ...]:
        impacted = set(self.impacted(changed))
        checks: list[str] = []
        for module in self.modules:
            if module.name in impacted:
                checks.extend(module.checks)
        if require_full_suite:
            checks.extend(self.full_suite_checks)
        return tuple(dict.fromkeys(checks))

    def explain(self, changed: Iterable[str], *, require_full_suite: bool = False) -> dict:
        changed_tuple = tuple(changed)
        impacted = self.impacted(changed_tuple)
        checks = self.required_checks(changed_tuple, require_full_suite=require_full_suite)
        return {
            "changed": changed_tuple,
            "impacted_modules": impacted,
            "required_checks": checks,
            "full_suite_required": require_full_suite,
        }


def _norm(path: str) -> str:
    return str(PurePosixPath(path.replace("\\", "/")))


def _under(path: str, root: str) -> bool:
    root = _norm(root).rstrip("/")
    return path == root or path.startswith(root + "/")
