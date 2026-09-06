# rust/ — the `swf` operator binary

This workspace builds one native executable, `swf`: install it, connect a factory environment,
submit work, watch it, read the evidence, answer the gates and verify what shipped — from a laptop
with no Python on it. It is the operator's application code and nothing else. Airflow keeps
scheduling, retries, mapped jobs and the HITL gates; `swf` holds no authority of its own and
re-reads every decision from the API before acting on it.

The product documentation lives in [docs/swf.md](../docs/swf.md). This page is for someone about to
build the thing.

## The crates

Dependency direction is strictly `cli → tui → app → adapters → domain`. A lower crate never names a
higher one, which is what keeps the pure logic testable and the transports swappable.

| Crate | What it is | The boundary it defends |
| --- | --- | --- |
| `swf-domain` | Contracts, identities and the pure roll-ups ported from `swfactory.control` | No I/O, no tokio, no reqwest — so equivalence against the Python fixtures is a unit test |
| `swf-adapters` | Airflow REST v2, GitHub through `gh`, islo, artifact metrics | One trait per source; nothing above it ever sees a transport or an HTTP status |
| `swf-app` | The operations both interfaces call: contexts, doctor, submit, gates, deliveries | Every rule that must not exist twice — gate readiness, re-validation, per-source degradation |
| `swf-tui` | The Ratatui factory: attention, jobs, evidence, approvals | Renders and dispatches; it never awaits, never blocks and never touches the network |
| `swf-cli` | The `swf` binary: clap, text and JSON renderers, exit codes | The only crate that decides how a failure is spelled to a script |

`swf tui` is a Ratatui interface over the *same* operations layer as the commands, never a second
implementation. That is why `swf-app` exists as its own crate.

## Build and test

The workspace root is `rust/` because the repository root is the Python package, so every command
either runs from here or carries `--manifest-path rust/Cargo.toml`.

```sh
cargo build --release                                   # target/release/swf
cargo fmt --all --check                                 # max_width = 100, same as ruff
cargo clippy --all-targets --all-features -- -D warnings # a warning is a failure
cargo test --workspace                                   # the hermetic suite, no network
cargo test -p swf-domain --test contract                 # the Rust half of the equivalence harness
```

`--locked` is used everywhere CI builds, mirroring `uv.lock`'s role on the Python side:
`Cargo.lock` is committed and authoritative, and CI must never silently update it. Adding a
dependency means `cargo add` **and** committing the lock in the same change.

Contract equivalence is the migration's safety net, and it has two halves that must be run
together: `uv run pytest tests/test_contract_fixtures.py` from the repository root asserts the
Python implementation still produces the recorded answers, and `cargo test -p swf-domain --test
contract` asserts the Rust one does. The `contract-equivalence` job in `.github/workflows/ci.yml`
runs both so neither language can drift alone.

## The end-to-end run

`scripts/swf_e2e.sh` is the acceptance test: a live Airflow, two issues across two targets, eight
authenticated approvals and four verified deliveries, where the only thing that talks to Airflow
after the boot is `swf`. It is the twin of `scripts/stress_airflow.sh` — same boot, same blueprint,
same gates — and the difference is who drives.

```sh
scripts/swf_e2e.sh                                      # from the repository root
SWF_E2E_KEEP=1 scripts/swf_e2e.sh                       # keep the work dir for a post-mortem
SWF_BIN=rust/target/release/swf scripts/swf_e2e.sh      # skip the build, use one you already have
```

It exits non-zero if a gate could not be answered through `swf`, a job's evidence is missing, a
delivery fails independent verification, or the snapshot `swf` renders disagrees with the one
`swfactory herd` renders from the same live server.

## Exit codes

The codes are part of the public surface, under the same semantic-versioning policy as the Python
CLI: scripts are allowed to branch on them. With `--json`, a failure also prints one JSON document
to stdout — `{"error": {"kind": …, "message": …, "exit_code": …, "hint": …}}` — whose `kind` maps
one-to-one onto this table, so a consumer never has to inspect `$?`.

| Code | `kind` | Meaning |
| --- | --- | --- |
| 0 | — | success |
| 1 | `operational` | a check is red, a gate answer was refused by policy, verification failed |
| 2 | `usage` | a clap parse failure, an unparseable job or gate id, a mutation without `--yes` on a non-TTY |
| 3 | `not_found` | no such context, run, job, gate, delivery or sandbox |
| 4 | `auth` | HTTP 401, an invalid JWT, or a credential env var that is unset |
| 5 | `unreachable` | connect, DNS, TLS or timeout; `gh` or `islo` missing from `PATH` |
| 6 | `conflict` | the gate was already answered, or the evidence moved under the operator |

Diagnostics always go to stderr, so `swf … --json | jq` is safe in a pipeline.

## Conventions

Doc comments say **why**, not what: a `//!` paragraph opens every module naming its job and the
boundary it defends, and an inline comment names the external fact that forces the code's shape.
No `unwrap()` or `expect()` outside tests. No `panic!` on data that came from a service. Test
names are sentences stating the invariant — `fn removing_a_foreign_sandbox_surfaces_permission_error()`
— not `fn test_remove()`.

`rust/target/` is gitignored; nothing in it is ever committed.
