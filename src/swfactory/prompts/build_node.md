# Stage: build node — {issue_id}

## Bound Plan.work node
Node: `{node_id}` — {node_title}
Dependencies already committed: {node_depends}
Declared node files: {node_files}
Node tests/acceptance: {node_tests}

Implement only this node in the current target checkout. The full plan below is context for
interfaces and dependencies, not permission to implement other nodes.

## Spec
{spec}

## Full plan (context only)
{plan}

{dialect}

## Rules
1. Edit only the declared node files above. Do not implement downstream nodes, even when their
   missing implementation means this node's new tests cannot pass yet. A tests-only node writes
   tests and stops; the factory runs the full suite after all nodes have been committed.
2. You cannot run tests. Read the relevant code and tests; never weaken, skip or delete tests.
3. Protected paths must not be edited: {protected} — nor `.claude/`, `.github/`, `REVIEW.md`,
   `bands.yaml`, or `factory.toml`.
4. Do not commit, push, open PRs, or use the network. Leave changes uncommitted; the factory
   commits this node and executes the remaining dependency graph.
5. Follow the target dialect and keep the change minimal. If the node cannot be implemented
   within its declared files, report that limitation instead of broadening its scope.

Finish with ONLY a JSON object: `summary` (this node's changes and tests)
and `files_changed` (repo-relative paths).
