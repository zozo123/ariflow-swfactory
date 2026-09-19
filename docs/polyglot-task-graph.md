# Polyglot task graph: motion, not authority

Turborepo 2.11 adds experimental native understanding of uv and Cargo workspaces. That is useful
here, but only if it stays on the correct side of the factory boundary.

The software factory has two very different kinds of state:

- **matter**: durable Cell identity, accepted input digests, approval revisions, mutation journals,
  evidence, and publication state;
- **motion**: builds, tests, agents, sandboxes, worktrees, task executors, and caches.

Turborepo optimizes motion. Airflow and the Factory Cell control plane govern matter.

That distinction is the whole integration.

## The boundary

```text
GitHub work order
      |
      v
Airflow lifecycle + Cell authority
      |
      +---- stage -------------------------------+
      |                                          |
      |  local polyglot task graph               |
      |                                          |
      |  native Cargo tasks ----+                 |
      |                        |                   |
      |  native uv package ----+--> verification  |
      |       + factory verify |    fan-in         |
      |                        |                   |
      +------------------------------------------+
      |
      v
digest-bound evidence -> human gates -> promotion
```

Turbo is never allowed to:

- create or transfer Cell authority;
- answer a human gate;
- retry an ambiguous external mutation;
- decide publication or promotion;
- turn a cache hit into verification evidence.

It may decide that a content-identical local task does not need to execute again. That is a speed
decision, not a truth decision.

## Current graph

Cargo is a real repository-root workspace: `Cargo.toml`, `Cargo.lock`,
`rust-toolchain.toml`, and `rustfmt.toml` share one root identity while crate sources remain
under `rust/crates/`. `[workspace.metadata] name = "swfactory-rust"` gives the native Cargo
aggregate a stable Turbo package identity.

uv is also discovered natively. The root `pyproject.toml` names the synthetic workspace package
`swfactory-python`, and `tests/fixtures/contract` is a real virtual uv member named
`swfactory-contract-fixtures`.

One subtlety matters for affectedness: Turborepo intentionally treats its native **root pytest**
task (`swfactory-python#test`) as repository-wide because pytest controls collection. Native uv
tasks also carry toolchain-generated inputs in a separate JIT input channel, so a startup exclusion
cannot honestly narrow that built-in task to "everything except Rust source."

The factory therefore defines one explicit root task, `//#python-verify`, which still executes the
root uv workspace with `uv run --active --frozen --all-packages pytest`. Its startup inputs are
`$TURBO_DEFAULT# Polyglot task graph: motion, not authority

Turborepo 2.11 adds experimental native understanding of uv and Cargo workspaces. That is useful
here, but only if it stays on the correct side of the factory boundary.

The software factory has two very different kinds of state:

- **matter**: durable Cell identity, accepted input digests, approval revisions, mutation journals,
  evidence, and publication state;
- **motion**: builds, tests, agents, sandboxes, worktrees, task executors, and caches.

Turborepo optimizes motion. Airflow and the Factory Cell control plane govern matter.

That distinction is the whole integration.

## The boundary

```text
GitHub work order
      |
      v
Airflow lifecycle + Cell authority
      |
      +---- stage -------------------------------+
      |                                          |
      |  local polyglot task graph               |
      |                                          |
      |  native Cargo tasks ----+                 |
      |                        |                   |
      |  native uv package ----+--> verification  |
      |       + factory verify |    fan-in         |
      |                        |                   |
      +------------------------------------------+
      |
      v
digest-bound evidence -> human gates -> promotion
```

Turbo is never allowed to:

- create or transfer Cell authority;
- answer a human gate;
- retry an ambiguous external mutation;
- decide publication or promotion;
- turn a cache hit into verification evidence.

It may decide that a content-identical local task does not need to execute again. That is a speed
decision, not a truth decision.

## Current graph

Cargo is a real repository-root workspace: `Cargo.toml`, `Cargo.lock`,
`rust-toolchain.toml`, and `rustfmt.toml` share one root identity while crate sources remain
under `rust/crates/`. `[workspace.metadata] name = "swfactory-rust"` gives the native Cargo
aggregate a stable Turbo package identity.

uv is also discovered natively. The root `pyproject.toml` names the synthetic workspace package
`swfactory-python`, and `tests/fixtures/contract` is a real virtual uv member named
`swfactory-contract-fixtures`.

 minus `rust/**/*.rs`. Native uv discovery remains enabled and visible in the
package graph; the explicit task exists only to state this repository's narrower incremental
verification contract instead of overriding Turbo's generic native contract.

```text
//#python-verify -----------+
                           |
swfactory-rust#test --------+----> //#polyglot-verification
                           |
swf-cli#build --------------+
```

The Rust tasks are native Cargo tasks. The Python verification task is an explicit root task that
executes the natively discovered uv workspace. The root fan-in is uncached and has no authority
outside this local execution graph.

Run the graph with Turborepo 2.11.1:

```sh
npx --yes turbo@2.11.1 ls
npx --yes turbo@2.11.1 run '//#polyglot-verification' --dry-run=json
npx --yes turbo@2.11.1 run '//#polyglot-verification' --summarize
```

The CI integration remains advisory while the native Rust/Python support and affectedness policy
are being proven.

## Hashes have two jobs here

Turborepo and the factory both use content-derived identity, but they make different claims.

| Mechanism | Question | Consequence |
| --- | --- | --- |
| Turbo task hash | Have these inputs already produced these outputs? | Skip redundant work |
| Factory digest / approval revision | Are these exactly the bytes a person or policy approved? | Permit or refuse authority |

A Turbo cache hit may reduce latency. It can never authorize a transition.

Remote Turbo cache is disabled in `turbo.json`. Native Cargo and uv package tasks may use the
local content-addressed cache; Turborepo derives their toolchain and dependency identity from the
root workspace manifests and lockfiles. The repo-wide `//#polyglot-verification` fan-in stays
`cache: false` because it is an execution barrier, not evidence.

## Promotion rule

The experiment graduates from advisory to required only after all of these are true:

1. Turbo's native Cargo and uv graphs match the existing CI dependency intent;
2. affectedness is proven for Python-only, Rust-only, docs-only, and shared-contract changes;
3. cold and warm runs are benchmarked on representative changes;
4. candidate evidence still comes from the factory's independent verification path;
5. disabling Turbo produces the same promotion decision, only more slowly.

That last condition is the invariant: **an accelerator may change time-to-answer, never the answer.**


## Reproducible benchmark and affectedness evidence

The advisory CI job also runs:

```sh
uv run python scripts/polyglot_benchmark.py --out .factory/turbo/benchmark.json
```

The script creates detached local worktrees and measures four synthetic changes without touching the
candidate checkout. Affectedness queries only the three full-name tasks the executable fan-in
actually depends on: `//#python-verify`, `swfactory-rust#test`, and `swf-cli#build`.
This matters because Turbo's `affectedTasks` resolver also schedules prerequisites of whatever
affected tasks it selects; including the root fan-in in the query would therefore make unchanged
sibling leaves appear in the execution set. Generic native tasks, including Turbo's intentionally
repository-wide root pytest task, remain available but are outside this factory graph's contract.

- Python-only: Python verification must be affected; Rust work must not be.
- Rust-only: Rust work must be affected; Python verification must not be.
- Docs-only: Python verification must run because the shipped site/doc contract is part of pytest;
  Rust work must not be selected.
- Shared contract fixture: both Python and Rust verification must be affected.

It then removes the repository Rust `target/`, runs verification once without Turbo, removes
`target/` again plus the local Turbo cache, runs a cold native polyglot verification, and
immediately repeats it as the no-op warm run. Tool download/registry caches stay warm on both sides,
so the cold comparison resets build outputs rather than benchmarking the network. The JSON report retains wall/CPU time,
attempted tasks, cache hits, executed-task count, summary sizes, and the verdict-equivalence result.
Remote cache stays disabled.

The benchmark fails if the non-Turbo and Turbo verification verdicts differ or if the warm run
produces no additional local Turbo cache hits. This is still advisory evidence: it is deliberately
absent from candidate readiness until the graph and measurements are stable across representative
changes.
