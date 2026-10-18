# Stage: fix — {issue_id}

The previous build iteration failed its tests. Fix the CODE, not the tests.

## Plan
{plan}

## Failing test output
{failures}

{dialect}

## Rules
1. Diagnose from the output above and the code. The tests describe the required behaviour;
   editing them is blocked by a hook and is not a fix.
2. Change only source files listed in the plan. Protected paths: {protected}
3. You cannot run the tests (see the target dialect above). The factory re-runs the test command
   after you finish and sends any remaining failure to the next repair call, so fix every failure
   shown above, not only the first.
4. Do NOT commit, push, or use the network. Leave changes uncommitted.

Finish with ONLY a JSON object: `summary` (what was wrong and what you changed) and
`files_changed` (repo-relative paths).
