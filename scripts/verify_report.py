#!/usr/bin/env python3
"""Assert one `swf deliveries verify --json` report reached the strongest verdict.

The three verdicts are deliberately separate, and only the third one is evidence:

* ``workflow_succeeded`` — Airflow says the tasks are green. That is a statement about the
  orchestrator, not about the code.
* ``branch_published`` — a branch exists with the expected commits on it. Still nobody has run it.
* ``independently_verified`` — this process cloned that branch into a directory of its own, read
  the contract out of the CHECKOUT, and re-ran the target's own test command there.

A release scenario that accepts the first two has only proved that the factory did not crash.

    scripts/verify_report.py <report.json> <job index>
"""

from __future__ import annotations

import json
import sys


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    with open(argv[1], encoding="utf-8") as fh:
        report = json.load(fh)
    idx = argv[2]
    verdicts = report.get("verdicts") or {}
    # `tests` is the re-run's own one-line summary ("7 passed, 0 failed"), not a structure.
    tests = report.get("tests") or "-"
    print(
        f"  job {idx}: workflow={verdicts.get('workflow_succeeded')} "
        f"published={verdicts.get('branch_published')} "
        f"verified={verdicts.get('independently_verified')} "
        f"tests={tests}"
    )
    if verdicts.get("independently_verified"):
        return 0
    # Say WHY, with the evidence rows that did not pass: "it failed" is not a diagnosis.
    for check in report.get("checks") or []:
        if check.get("status") not in ("pass", "skipped"):
            print(
                f"    {check.get('level', '?')}/{check.get('name', '?')}: "
                f"{check.get('status')} — {check.get('detail', '')}",
                file=sys.stderr,
            )
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
