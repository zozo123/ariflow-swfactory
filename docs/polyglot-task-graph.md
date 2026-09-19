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
      |  Rust build ----+                         |
      |                 +--> Python tests         |
      |  Rust contract -+                         |
      |                                          |
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

The repository root is a uv project, so `pyproject.toml` now declares a uv workspace and gives the
synthetic Turbo workspace package the stable name `swfactory-python`.

The Rust workspace still lives at `Cargo.toml`. Turborepo 2.11 native Cargo discovery requires
the Cargo workspace at the repository root, so this change deliberately does **not** pretend the
nested workspace is native. Instead, Rust is represented by explicit root tasks:

```text
swf-cli#build
      |
      +---------------------> swfactory-python#test
      |
      +--> swfactory-rust#test
                    |
                    +--------> //#polyglot-verification
swfactory-python#test -------+
```

Run the graph with Turborepo 2.11.1:

```sh
npx --yes turbo@2.11.1 run '//#polyglot-verification'
```

The CI integration is advisory while Turborepo's Rust/Python support is experimental.

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
candidate checkout. Affectedness is measured on native leaf `build`/`test` work, separately from
the full `//#polyglot-verification` barrier. That distinction matters: deliberately invoking the
full fan-in executes its prerequisites by definition, while an incremental planner should select
only leaf work made stale by the change.

- Python-only: Python verification must be affected; Rust work must not be.
- Rust-only: Rust work must be affected; Python verification must not be.
- Docs-only: Python verification must run because the shipped site/doc contract is part of pytest;
  Rust work must not be selected.
- Shared contract fixture: both Python and Rust verification must be affected.

It then runs the verification once without Turbo, clears the local Turbo cache, runs a cold native
polyglot verification, and immediately repeats it warm. The JSON report retains wall/CPU time,
attempted tasks, cache hits, executed-task count, summary sizes, and the verdict-equivalence result.
Remote cache stays disabled.

The benchmark fails if the non-Turbo and Turbo verification verdicts differ or if the warm run
produces no additional local Turbo cache hits. This is still advisory evidence: it is deliberately
absent from candidate readiness until the graph and measurements are stable across representative
changes.
