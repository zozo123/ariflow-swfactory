#!/usr/bin/env python3
"""Apply small semantic fixes discovered during the liquid fan-in stabilization wave."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def replace(path: str, old: str, new: str) -> None:
    target = ROOT / path
    text = target.read_text(encoding="utf-8")
    if old not in text:
        if new in text:
            return
        raise RuntimeError(f"stabilization pattern not found in {path}: {old[:80]!r}")
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


def main() -> None:
    replace(
        "src/swfactory/admission.py",
        "def __init__(self, limits: Limits = Limits()):\n        self.limits = limits",
        "def __init__(self, limits: Limits | None = None):\n        self.limits = limits or Limits()",
    )
    replace(
        "src/swfactory/control_kernel.py",
        "def __init__(self, root: Path, *, limits: Limits = Limits()):\n        root.mkdir(parents=True, exist_ok=True)",
        "def __init__(self, root: Path, *, limits: Limits | None = None):\n        limits = limits or Limits()\n        root.mkdir(parents=True, exist_ok=True)",
    )
    replace(
        "src/swfactory/durable_admission.py",
        "def __init__(self, path: Path, limits: Limits = Limits()):\n        path.parent.mkdir(parents=True, exist_ok=True)\n        self.limits = limits",
        "def __init__(self, path: Path, limits: Limits | None = None):\n        path.parent.mkdir(parents=True, exist_ok=True)\n        self.limits = limits or Limits()",
    )
    replace(
        "src/swfactory/work_executor.py",
        "self, runner: NodeRunner, merger: NodeMerger, policy: ExecutorPolicy = ExecutorPolicy()\n    ):\n        self.runner = runner\n        self.merger = merger\n        self.policy = policy",
        "self, runner: NodeRunner, merger: NodeMerger, policy: ExecutorPolicy | None = None\n    ):\n        self.runner = runner\n        self.merger = merger\n        self.policy = policy or ExecutorPolicy()",
    )
    replace(
        "src/swfactory/repo_runtime.py",
        "def _glob_under(root: str, paths: Iterable[str], ignored: Iterable[str]) -> bool:\n",
        "def _glob_root(pattern: str) -> str:\n    if pattern.endswith(\"/**\"):\n        return pattern[:-3].rstrip(\"/\")\n    return pattern.rstrip(\"/\")\n\n\ndef _glob_under(root: str, paths: Iterable[str], ignored: Iterable[str]) -> bool:\n",
    )
    replace(
        "src/swfactory/repo_runtime.py",
        "fnmatch.fnmatchcase(root, pattern.rstrip(\"/**\")) for pattern in path_patterns",
        "fnmatch.fnmatchcase(root, _glob_root(pattern)) for pattern in path_patterns",
    )
    replace(
        "src/swfactory/repo_runtime.py",
        "fnmatch.fnmatchcase(root, pattern.rstrip(\"/**\")) for pattern in ignore_patterns",
        "fnmatch.fnmatchcase(root, _glob_root(pattern)) for pattern in ignore_patterns",
    )
    for path, class_name in (
        ("src/swfactory/generations.py", "Dimension"),
        ("src/swfactory/intake_policy.py", "WorkKind"),
        ("src/swfactory/repo_coordination.py", "StalePolicy"),
    ):
        replace(path, "from enum import Enum", "from enum import StrEnum")
        replace(path, f"class {class_name}(str, Enum):", f"class {class_name}(StrEnum):")

    replace(
        "tests/test_dag_stress.py",
        "def test_fan_out_returned_issues_x_targets(stress: dict) -> None:\n    \"\"\"``fan_out``'s XCom is exactly ``Blueprint.jobs(conf)``: the DAG expands over nothing else.\"\"\"\n    assert stress[\"fan_out\"] == list(_expected_jobs())",
        "def test_fan_out_returned_issues_x_targets(stress: dict) -> None:\n    \"\"\"``fan_out`` is issues x targets enriched only by the durable Cell envelope.\"\"\"\n    expected = list(_expected_jobs())\n    actual = stress[\"fan_out\"]\n    assert len(actual) == len(expected)\n    for enriched, core in zip(actual, expected, strict=True):\n        assert {key: enriched[key] for key in core} == core\n        assert enriched[\"cell_id\"].startswith(\"cell_\")\n        assert enriched[\"cell_epoch\"] == 1\n        assert enriched[\"cell_managed\"] is False\n        assert enriched[\"cell_policy_digest\"] is None\n        assert enriched[\"cell_generation\"] is None\n    assert len({job[\"cell_id\"] for job in actual}) == len(expected)",
    )


if __name__ == "__main__":
    main()
