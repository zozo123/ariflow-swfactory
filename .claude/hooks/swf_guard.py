#!/usr/bin/env python3
"""swfactory PreToolUse guard: the deterministic gate the model cannot argue with.

Reads the Claude Code PreToolUse payload ({"tool_name", "tool_input"}) on stdin.

* Edit/Write/MultiEdit/NotebookEdit are denied when ``file_path`` (``notebook_path`` for
  NotebookEdit) matches a protected rule. Rules come from ``SWF_PROTECTED`` (colon-separated
  globs or path prefixes, set by the factory from the target's factory.toml) plus a fixed list
  (REVIEW.md, bands.yaml, .claude/, .github/, factory.toml).
* Bash is denied when the command matches the factory denylist (push / PR / commit / egress):
  the factory commits and delivers, never the agent.

Every decision is appended to ``.factory/hooks.jsonl``. Exit 2 (+ stderr) denies, 0 allows.
Stdlib only: this file is copied verbatim into target checkouts by ``swfactory.agent``.
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import UTC, datetime
from fnmatch import fnmatch

FIXED_PROTECTED = ("REVIEW.md", "bands.yaml", ".claude/", ".github/", "factory.toml")
WRITE_TOOLS = frozenset({"Edit", "Write", "MultiEdit", "NotebookEdit"})
BASH_DENY = re.compile(r"git push|gh pr|git commit|--amend|curl |wget ")
LOG_PATH = ".factory/hooks.jsonl"


def protected_rules() -> list[str]:
    """Fixed rules plus the colon-separated SWF_PROTECTED list, empty entries dropped."""
    extra = os.environ.get("SWF_PROTECTED", "").split(":")
    return [r for r in (*FIXED_PROTECTED, *extra) if r]


def _rule_hits(candidate: str, rule: str) -> bool:
    rule = rule.rstrip("/")
    return (
        candidate == rule
        or candidate.startswith(rule + "/")
        or fnmatch(candidate, rule)
        or fnmatch(candidate, "*/" + rule)
    )


def path_matches(path: str, rule: str, cwd: str) -> bool:
    """True when ``path`` (absolute or cwd-relative) falls under ``rule``."""
    if not path:
        return False
    norm = os.path.normpath(path)
    candidates = {norm}
    if os.path.isabs(norm):
        candidates.add(os.path.relpath(norm, cwd))
    return any(_rule_hits(c, rule) for c in candidates)


def decide(tool: str, tool_input: dict, cwd: str) -> tuple[str, str]:
    """Return (path_or_cmd, deny_reason); an empty reason means allow."""
    if tool in WRITE_TOOLS:
        path = str(tool_input.get("file_path") or tool_input.get("notebook_path") or "")
        for rule in protected_rules():
            if path_matches(path, rule, cwd):
                return path, f"'{path}' is protected ({rule}). Fix the code, not the gate."
        return path, ""
    if tool == "Bash":
        cmd = str(tool_input.get("command") or "")
        m = BASH_DENY.search(cmd)
        if m:
            return cmd, (
                f"command matches the factory denylist ({m.group(0).strip()!r}); "
                "the factory commits, pushes and opens PRs — the agent does not."
            )
        return cmd, ""
    return "", ""


def log_decision(tool: str, target: str, decision: str) -> None:
    try:
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        entry = {
            "ts": datetime.now(UTC).isoformat(timespec="seconds"),
            "tool": tool,
            "path_or_cmd": target[:500],
            "decision": decision,
        }
        with open(LOG_PATH, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")
    except OSError:
        pass  # logging must never change the decision


def main() -> int:
    try:
        payload = json.load(sys.stdin)
        tool = str(payload.get("tool_name") or "")
        tool_input = payload.get("tool_input") or {}
    except (ValueError, AttributeError):
        print("swf_guard: malformed hook payload; denying by default", file=sys.stderr)
        return 2
    target, reason = decide(tool, tool_input, os.getcwd())
    decision = "deny" if reason else "allow"
    log_decision(tool, target, decision)
    if reason:
        print(f"blocked by swf_guard: {reason}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
