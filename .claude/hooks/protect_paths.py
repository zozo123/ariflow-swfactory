#!/usr/bin/env python3
"""Deterministic gate (playbook 'hard block'): refuse edits to protected paths.

Reads the Claude Code PreToolUse payload on stdin. Protected paths come from
SWF_PROTECTED_PATHS (colon-separated, set by the factory for fix tasks) plus a fixed list.
Exit 2 blocks the tool call and shows the message to the agent.
"""
import json
import os
import sys

FIXED = ["REVIEW.md", "bands.yaml", ".claude/settings.json"]


def main() -> int:
    payload = json.load(sys.stdin)
    path = (payload.get("tool_input") or {}).get("file_path", "")
    protected = FIXED + [p for p in os.environ.get("SWF_PROTECTED_PATHS", "").split(":") if p]
    for rule in protected:
        if path.endswith(rule) or f"/{rule.rstrip('/')}/" in path or path.startswith(rule):
            print(f"blocked: '{path}' is protected ({rule}). Fix the code, not the gate.", file=sys.stderr)
            return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
