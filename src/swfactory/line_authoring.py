"""Offline production-line validation, graphing and semantic diff helpers."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class StageSpec:
    name: str
    gate: bool = False
    concurrency: int = 1
    sandbox: str = "default"
    permissions: tuple[str, ...] = ()


@dataclass(frozen=True)
class LineSpec:
    name: str
    stages: tuple[StageSpec, ...]

    def validate(self) -> tuple[str, ...]:
        errors = []
        names = [stage.name for stage in self.stages]
        if not self.name:
            errors.append("line name is empty")
        if not self.stages:
            errors.append("line has no stages")
        if len(names) != len(set(names)):
            errors.append("duplicate stage names")
        for stage in self.stages:
            if stage.concurrency < 1:
                errors.append(f"{stage.name}: concurrency must be >= 1")
        return tuple(errors)

    def graph(self) -> str:
        lines = ["flowchart LR"]
        for index, stage in enumerate(self.stages):
            shape = f"{{{{{stage.name}}}}}" if stage.gate else f"[{stage.name}]"
            lines.append(f"  s{index}{shape}")
            if index:
                lines.append(f"  s{index - 1} --> s{index}")
        return "\n".join(lines)


def semantic_diff(before: LineSpec, after: LineSpec) -> dict:
    old = {stage.name: stage for stage in before.stages}
    new = {stage.name: stage for stage in after.stages}
    added = sorted(set(new) - set(old))
    removed = sorted(set(old) - set(new))
    changed = []
    for name in sorted(set(old).intersection(new)):
        if old[name] != new[name]:
            changed.append({"stage": name, "before": asdict(old[name]), "after": asdict(new[name])})
    return {
        "line": after.name,
        "added_stages": added,
        "removed_stages": removed,
        "changed_stages": changed,
        "risk_flags": _risk_flags(old, new, removed),
    }


def preview_fanout(issues: Iterable[str], targets: Iterable[str]) -> dict:
    issues = tuple(issues)
    targets = tuple(targets) or ("default",)
    return {
        "jobs": len(issues) * len(targets),
        "issues": issues,
        "targets": targets,
    }


def _risk_flags(
    old: dict[str, StageSpec], new: dict[str, StageSpec], removed: list[str]
) -> list[str]:
    flags = [f"removed_stage:{name}" for name in removed]
    for name in set(old).intersection(new):
        if old[name].gate and not new[name].gate:
            flags.append(f"gate_removed:{name}")
        if new[name].concurrency > old[name].concurrency:
            flags.append(f"concurrency_increased:{name}")
        if set(new[name].permissions) - set(old[name].permissions):
            flags.append(f"permissions_expanded:{name}")
    return sorted(flags)
