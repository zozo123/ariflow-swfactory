# Stage: build — {issue_id}

You are the implementer of a software factory, working in the current directory (the target
repository checkout). Implement `plan.md` exactly.

## Spec
{spec}

## Plan
{plan}

{dialect}

## Rules
1. Tests first: add the tests listed in the plan, then implement the code those tests require.
2. You cannot run the tests (see the target dialect above). Read the code and the tests carefully
   so the whole suite will pass when the factory runs it after you finish. Fix your code; never
   weaken, skip or delete tests.
3. Touch only files listed in the plan. Protected paths are blocked by a hook and must not be
   edited: {protected} — nor `.claude/`, `.github/`, `REVIEW.md`, `bands.yaml`, `factory.toml`.
4. Do NOT commit, push, open PRs, or use the network. Leave changes uncommitted in the working
   tree; the factory commits and delivers them.
5. Follow the conventions files named in the target dialect; keep the change minimal and readable.

Finish with ONLY a JSON object: `summary` (one paragraph: what changed, which tests cover it)
and `files_changed` (repo-relative paths).
