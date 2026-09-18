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

The Rust workspace still lives at `rust/Cargo.toml`. Turborepo 2.11 native Cargo discovery requires
the Cargo workspace at the repository root, so this change deliberately does **not** pretend the
nested workspace is native. Instead, Rust is represented by explicit root tasks:

```text
//#rust-build
      |
      +---------------------> swfactory-python#test
      |
      +--> //#rust-domain-contract
                    |
                    +--------> //#polyglot-contract
swfactory-python#test -------+
```

Run the graph with Turborepo 2.11.1:

```sh
npx --yes turbo@2.11.1 run '//#polyglot-contract'
```

The CI integration is advisory while Turborepo's Rust/Python support is experimental.

## Hashes have two jobs here

Turborepo and the factory both use content-derived identity, but they make different claims.

| Mechanism | Question | Consequence |
| --- | --- | --- |
| Turbo task hash | Have these inputs already produced these outputs? | Skip redundant work |
| Factory digest / approval revision | Are these exactly the bytes a person or policy approved? | Permit or refuse authority |

A Turbo cache hit may reduce latency. It can never authorize a transition.

Remote Turbo cache is disabled in `turbo.json`. The Python native task may use the local
content-addressed cache. The explicit Rust tasks are currently `cache: false` because, until Cargo
is natively discovered, their task hash would not automatically include the complete Rust compiler
identity and Cargo dependency semantics.

## Why not move Cargo to the root in this change?

That is the clean end state for native multi-language discovery, but it changes a much larger
surface:

- every `--manifest-path rust/Cargo.toml` call;
- CI and release scripts;
- contributor documentation;
- Rust cache paths;
- packaging and release assumptions;
- any code that treats `rust/` as a protected subtree.

The migration should be measured as its own change. Once it lands, enable
`experimentalCargoWorkspaces`, delete the explicit Rust root tasks, and let Turborepo derive Cargo
inputs and toolchain identity natively.

## Promotion rule

The experiment graduates from advisory to required only after all of these are true:

1. the root-Cargo migration is complete;
2. Turbo's native Cargo and uv graphs match the existing CI dependency intent;
3. cold and warm runs are benchmarked on representative changes;
4. candidate evidence still comes from the factory's independent verification path;
5. disabling Turbo produces the same promotion decision, only more slowly.

That last condition is the invariant: **an accelerator may change time-to-answer, never the answer.**
