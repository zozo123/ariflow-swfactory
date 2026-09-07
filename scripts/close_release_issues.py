#!/usr/bin/env python3
"""Close exactly the issues declared by a checked-in release manifest.

The manifest is the authority. This script never infers a numeric upper bound and never
walks "all open issues", so a future issue cannot be swept into an old release by accident.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, NoReturn


def fail(message: str) -> NoReturn:
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(2)


def gh_json(*args: str) -> dict[str, Any]:
    proc = subprocess.run(
        ["gh", "api", *args],
        check=False,
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        fail(proc.stderr.strip() or f"gh api failed: {' '.join(args)}")
    try:
        value = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        fail(f"GitHub returned invalid JSON: {exc}")
    if not isinstance(value, dict):
        fail("GitHub response was not an object")
    return value


def load_manifest(path: Path) -> tuple[str, list[int]]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        fail(f"manifest does not exist: {path}")
    except json.JSONDecodeError as exc:
        fail(f"invalid manifest JSON: {exc}")

    if not isinstance(raw, dict):
        fail("manifest root must be an object")
    if raw.get("version") != 1:
        fail("manifest version must be 1")

    release_id = raw.get("release_id")
    if not isinstance(release_id, str) or not release_id.strip():
        fail("release_id must be a non-empty string")

    issues = raw.get("issues")
    if not isinstance(issues, list) or not issues:
        fail("issues must be a non-empty explicit list")
    if any(type(number) is not int or number <= 0 for number in issues):
        fail("every issue number must be a positive integer")
    if len(set(issues)) != len(issues):
        fail("issue list contains duplicates")

    expected_count = raw.get("expected_count")
    if type(expected_count) is not int or expected_count != len(issues):
        fail(f"expected_count={expected_count!r} does not match explicit issue count={len(issues)}")

    return release_id.strip(), sorted(issues)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    if not args.repo or "/" not in args.repo:
        fail("--repo owner/name (or GITHUB_REPOSITORY) is required")

    release_id, issue_numbers = load_manifest(args.manifest)
    selected: list[int] = []
    already_closed: list[int] = []

    for number in issue_numbers:
        issue = gh_json(f"repos/{args.repo}/issues/{number}")
        if issue.get("pull_request") is not None:
            fail(f"#{number} is a pull request, not an issue")
        if issue.get("state") == "closed":
            already_closed.append(number)
        elif issue.get("state") == "open":
            selected.append(number)
        else:
            fail(f"#{number} has unexpected state {issue.get('state')!r}")

    closed: list[int] = []
    if args.apply:
        for number in selected:
            issue = gh_json(
                "-X",
                "PATCH",
                f"repos/{args.repo}/issues/{number}",
                "-f",
                "state=closed",
                "-f",
                "state_reason=completed",
            )
            if issue.get("state") != "closed":
                fail(f"#{number} did not become closed")
            closed.append(number)

    remaining: list[int] = []
    if args.apply:
        for number in issue_numbers:
            issue = gh_json(f"repos/{args.repo}/issues/{number}")
            if issue.get("state") != "closed":
                remaining.append(number)
        if remaining:
            fail(f"release manifest still has open issues: {remaining}")

    summary = {
        "release_id": release_id,
        "repo": args.repo,
        "manifest_count": len(issue_numbers),
        "selected_open": len(selected),
        "already_closed": len(already_closed),
        "closed_now": len(closed),
        "remaining": remaining,
        "applied": bool(args.apply),
    }
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
