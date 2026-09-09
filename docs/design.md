# Design: the factory, its DAGs, and its boundaries

Reference for the parts the README only names: how a blueprint declares a route, what the generated
Airflow DAG does, how metrics and bands close the loop, and the decisions (and accepted risks)
behind all of it.

Validation status: the 2.0 rewrite was audited statically before tagging. The v2.0.1 workflow
then gates publication on lint, the hermetic suite, scripted e2e demo, Airflow parity and smoke,
and package build. Live hosted-provider runs remain deployment validation; the section explicitly
labeled as prior 1.1 evidence records older compatibility runs.

## Scope and authority

swfactory turns a declared work order into a reviewable pull request. It keeps routes, approvals,
evidence, budgets, and the source-control credential in a trusted control plane.

The control plane owns the route, job identity, stage transitions, approvals, sandbox lifecycle,
trace record, patch validation, and publication. A coding agent performs the assigned work inside
one sandbox. The target's `factory.toml` supplies its test command and protected paths. A person
retains merge authority. Astronomer Blueprint can place this route inside a larger workflow while
the route keeps its own approvals and limits.

The project covers admitted software work from intent through pull request. Backlog ranking,
product decisions, production monitoring, and final merge remain with the systems and people that
already own them.

```text
issue x target -> capture baseline + contract -> intent -> human gate -> spec -> plan
               -> human gate -> bounded edit / trusted verify -> independent review
               -> validate exact patch + evidence -> PR or explicit blocked/rejected PR
               -> human merge
```

## How the factory runs

1. **Work order.** A person or upstream product system creates a GitHub issue describing the
   desired change.
2. **Production route.** A label, trusted comment, Airflow trigger, schedule, or CLI command selects
   a versioned blueprint. The blueprint fixes the target, stages, approvals, limits, sandbox, and
   delivery labels. The target's `factory.toml` fixes the test command and working scope.
3. **Plant scheduler.** Airflow manages mapped jobs, dependencies, retries, timeouts, approvals,
   and run history. The CLI uses the same stage functions and `(blueprint, job, run id)` identity.
4. **Work cell.** One coding agent receives one bounded job in one sandbox and returns edits plus
   evidence.
5. **Quality checks.** Typed plans, approval hashes, fresh JUnit, independent review, commit
   history, and exact-patch validation decide whether the work can advance.
6. **Release.** People approve intent and plan, review the pull request, and merge through the
   repository's normal branch policy.
7. **Continuous improvement.** `metrics.json` and `bands.yaml` record drift, open incidents, or
   propose new issues that return as work orders.

```text
work order -> production route -> Airflow -> work cell -> quality checks
                       |             |                           |
                       v             v                           v
                  approvals      run history              pull request
                       |                                         |
                       +----------------> human merge <-----------+
                                              |
                                              v
new work order <- incident / proposal <- metric bands <- merged run evidence
```

The built-in metrics describe factory execution: cycle time, iterations, first-pass verification,
review findings, denied tools, and cost. `maintain` reads the configured checkout; with the default
base-branch checkout, it sees run evidence after merge. Production health, customer impact,
security, and business results can join the cycle through integrations that create ordinary
GitHub work orders.

## Blueprints

`blueprints/default.toml` (name `factory`) is the default SDLC line; `blueprints/hotfix.toml` is a
second line with zero Python: no `spec` stage, an intent gate that approves itself, a 4 h plan gate,
and an extra `hotfix` PR label. A blueprint is validated by `swfactory.blueprint.Blueprint`
(pydantic) and shape-read by the DAG generator with stdlib `tomllib`:

| Section | What it fixes | Rules |
| --- | --- | --- |
| `[blueprint]` | `name` = DAG id = CLI name | `^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$`, must equal the CLI name |
| `[trigger]` | manual or cron admission | cron requires `cron`; a scheduled execution needs non-empty `issues`; runtime run configuration overrides those defaults |
| `[[targets]]` | `repo`, `dir`, `base_branch` — jobs per run = issues x targets | >= 1 |
| `[stages] order` | which stage functions run | subsequence of `intent spec plan build_and_test review deliver`, first `intent`, last `deliver`; omitted inputs render `(none)` |
| `[[gates]]` | `after`, `artifact`, `timeout_h`, `assigned`, `mode` | `after` in `{intent, plan}` and in `order`; `mode="human"` (default) -> no operator default, an unanswered gate times out; `mode="auto"` -> defaults to Approve (actor `auto`). Legacy `auto=true/false` is folded into `mode`; declaring both and disagreeing is an error |
| `[limits]` | build/review iterations, turns, USD per stage / per **job**, `stage_timeout_h`, `max_parallel_jobs` | `budget_usd_per_stage <= budget_usd` |
| `[policy.<stage>]` | `extra_allowed_tools`, `model` | additive path-scoped file/search tools only; no shell, web, task, or MCP tools; no writes in read-only stages |
| `[review]` / `[deliver]` | `nit_cap`; PR `labels` | |
| `[sandbox]` | `kind` (`local\|srt\|docker\|islo\|toolset`), provider settings, `ttl_s`, `idle_s`, `snapshot`, toolset `backend` / absolute `workdir` | `ttl_s > max gate timeout`; unsupported policy is an error |

Operational `SWF_*` env vars override blueprint values and CLI flags. Job identity does not:
`issue`, `repo`, `target_dir`, `base_branch`, `run_id`, and `blueprint` are rebound from the mapped
job after settings load so ambient worker configuration cannot collapse jobs onto one workspace.
Without `--agent claude` a run is a scripted replay and uses `LocalSandbox` unless `--sandbox` says
otherwise.

## Airflow

The dependency groups pin `apache-airflow==3.3.1` and standard provider 1.18.0. The optional CI
job `airflow-main` is configured to run DAG parity and smoke against upstream
`apache/airflow@main` so API drift in the task SDK or HITL operators can surface before a release.

```sh
uv sync --group airflow                              # apache-airflow 3.3.1 + standard provider
export AIRFLOW_HOME=$PWD/airflow_home                # gitignored
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags AIRFLOW__CORE__LOAD_EXAMPLES=False
uv run airflow standalone                            # UI on http://localhost:8080
```

`dags/blueprints.py` emits one DAG per `blueprints/*.toml` (dag_id = `blueprint.name`). A run fans
`{"issues": [...]}` ({`"issue": N`} accepted, optional `"targets": [...]` filter) x the blueprint's
`[[targets]]` out into a mapped `job` task group — `setup > intent > [approve_intent >
record_intent] > spec > plan > [approve_plan > record_plan] > build_and_test > review > deliver >
metrics ; teardown` — one sandbox and one addressable approval per (issue, target),
`max_parallel_jobs` at a time. `deliver` publishes the asset `swf.metrics.<blueprint>`, so
`dags/maintain.py` runs after every delivery as well as nightly at 03:00 UTC.

Trigger: label an issue `factory` (or `factory:<name>`) so the receiver or `dispatch.yml` POSTs
`/api/v2/dags/<name>/dagRuns`, or `uv run airflow dags trigger factory --conf '{"issues": ["42"]}'`.
Answer gates in the UI (Required Actions shows the head of intent.md / plan.md) or from the shell:

```sh
export AIRFLOW_TOKEN=$(curl -s -X POST localhost:8080/auth/token -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"<airflow_home/simple_auth_manager_passwords.json.generated>"}' | jq -r .access_token)
uv run swfactory approve <dag_run_id> intent          # or: plan, --reject, --blueprint <name>, --map-index <job>
```

Workers read operational `SWF_*` env vars; mapped job identity always wins. `airflow dags test`
never resolves HITL tasks, so mark the gates:

```sh
uv run airflow dags test factory --conf '{"issues":["demo/issue.md"]}' --mark-success-pattern 'job\.approve_.*'
```

A gate declares its own authority with `gates[].mode`. `mode = "auto"` makes the gate default to
Approve once its `timeout_h` elapses; `mode = "human"` (the default) never gets a default, so an
unanswered gate times out and fails. **No environment variable widens this** — `SWF_APPROVE=auto` is
not an approver, and a missing or empty response is refused rather than recorded as actor `auto`
(#2066). The only thing that may stand in for a missing response is the explicitly declared replay
fixture `SWF_GATE_REPLAY` (see `demo/gate-replay.json`), which `swfactory.approval_policy` refuses
outright for backend-managed cells, so a smoke run cannot authorize managed work by marking a gate
successful. A recorded decision carries actor, `mode`, the Cell id/epoch and the approved artifact's
sha256, and `deliver` re-checks all four before publishing. A gate's `assigned` users become the
HITL `assigned_users`. Gates are a
`GateOperator` (an `ApprovalOperator` that never skips on its own): the response — Approve or
Reject, with `responded_by_user` — lands in XCom, `record_<stage>` writes it to `approvals.json`,
and on Reject raises `AirflowSkipException`; the work stages skip while `deliver`/`metrics` run with
`trigger_rule="none_failed"` and publish the `[REJECTED]` PR. Every task rebuilds its `Ctx`; the run
id is `sha1(dag_run_id:job_idx)[:8]`, so retries share the sandbox and the orchestrator's stage log
`.factory/<run_id>/state/stages.jsonl` on the worker. Host-owned artifacts live beside it under
`state/artifacts/` and are mirrored into the sandbox only when needed. New DAGs start paused in
standalone: unpause once in the UI or with
`PATCH /api/v2/dags/factory {"is_paused": false}`.

The repository's DAG coverage is defined in `tests/test_dag_parity.py` (blueprint shape) and
`tests/test_dag_smoke.py` (the recipe above end to end):

```sh
uv run --group airflow pytest tests/test_dag_parity.py tests/test_dag_smoke.py
```

## Astronomer Blueprint composition

Astronomer's `airflow-blueprint` is an optional outer composition plane, not a replacement for a
factory line. The `SoftwareFactory` template is advertised through the
`airflow_blueprint.blueprints` package entry point as `software_factory`. It returns a deferrable
`TriggerDagRunOperator` pointed at an existing line DAG:

```text
Astro IDE / DAG YAML -> software_factory -> swfactory line DAG -> mapped jobs + gates + PR
```

This child-DAG boundary is deliberate. It preserves dynamic task mapping, native HITL task
addresses, independent retries and run history, and prevents an outer visual composition from
editing away a gate or changing delivery authority. A rejected line can complete operationally
after publishing rejection evidence, so parent-DAG success means “the line finished,” not “the
change was approved.” See [astronomer-blueprint.md](astronomer-blueprint.md). The same boundary
serves [dag-factory](dag-factory.md): YAML composes, the line governs, and neither can edit the other.

## Metrics and bands

`bands.yaml` defines a window (20 runs) and three response tiers over the `docs/factory/*/
metrics.json` of a checkout:

| Breach | Tier | Action |
| --- | --- | --- |
| 1σ | `log` | record it and move on |
| 2σ | `diagnose` | read-only agent, `Diagnosis` schema, incident record at `docs/factory/incidents/<date>-<metric>.md` **and** an issue labeled `maintain, incident` carrying it, so the diagnosis outlives the checkout |
| 3σ | `propose` | a drafted intent + the incident record in one `gh issue create --label factory`, which re-enters the factory |

Detection is `statistics.mean`/`stdev`, needs at least 3 history samples, and ignores
`agent=scripted` runs so the demo never moves the bands. A flat history (stdev 0 — the normal shape
for `first_pass_ci` or `blockers`) makes any move in the bad direction a top-tier breach, reported
with `stdev=0`; no move, or a move in the good direction, is not a breach.

The `maintain` DAG never reads relative to the worker's cwd: it uses `$SWF_MAINTAIN_ROOT` (a
checkout of the target) when set, else a shallow read-only clone of the target's base branch made
for the task, and fails loudly when the result has no `docs/factory/`. `bands.yaml` defaults to the
factory checkout's own copy (`$SWF_BANDS` overrides).

```sh
uv run swfactory metrics --root .       # first-pass rate, mean iterations, p50 cycle, findings, cost
uv run swfactory maintain --root . --sweep-ttl-s 172800   # omit the flag for the configured TTL
```

## crabbox

crabbox is a command wrapper, not a sandbox. With `--tests crabbox` (local sandbox only) `run_tests`
wraps the target's test command in

```
crabbox run -provider <provider> -junit .factory/junit.xml \
  -download .factory/junit.xml=.factory/junit.xml -ttl 45m -idle-timeout 15m -- <cmd>
```

(no `-download` for in-place providers such as `srt`, `docker-sandbox`, `apple-machine`) and reads
the junit file it brings back. Never `-allow-env`, `-env-from-profile` or `-keep`. v2 fixes three v1
bugs: `-artifact-glob` (SSH-lease providers only) -> `-download`; default provider `islo` ->
`local-container` (`scrub_env` strips the `ISLO_API_KEY` islo needs); `.crabbox.yaml` jobs are maps
so `crabbox doctor` parses it. Human inner loop:
`crabbox run --provider local-container -- uv run pytest`.

## Airflow's own sandbox abstraction (experimental)

Airflow grew a `SandboxToolset` in the `common.ai` provider, whose `SandboxBackend` is the same
shape as this factory's `Sandbox`: create and destroy, run a command, read and write files. Rather
than reimplement per vendor, `ToolsetSandbox` adapts it, so every backend the Airflow community
ships becomes a swfactory sandbox:

```sh
uv run swfactory run --issue 1 --sandbox toolset            # backend sbx (released provider)
SWF_TOOLSET_BACKEND=islo uv run swfactory run --issue 1 --sandbox toolset
```

Hosted providers such as Daytona, E2B, Tensorlake, and Box by ASCII use the same seam through a
custom `package.module:Class` backend. The repository does not ship those vendor adapters. An
adapter must implement the Airflow-compatible lifecycle and prove reconnectability, output and
timeout bounds, confined file access, policy enforcement, cleanup, and expiry before production
use.

| backend | status |
|---|---|
| `sbx` (Docker Sandboxes) | ships in the released `apache-airflow-providers-common-ai` |
| `islo` | pending upstream: [apache/airflow#71672](https://github.com/apache/airflow/pull/71672) |
| `opensandbox` | pending upstream: [apache/airflow#71676](https://github.com/apache/airflow/pull/71676) |
| `asciibox` | pending upstream: [apache/airflow#71725](https://github.com/apache/airflow/pull/71725) |

A pending backend fails with a message naming its pull request. Install the development stack with
Node 22+, pnpm 10.28.1 and uv available:

```sh
./scripts/airflow_main.sh          # all packages + both UIs from one apache/airflow@main commit
./scripts/airflow_main.sh --islo   # explicitly overlay the experimental common.ai islo fork
./scripts/airflow_main.sh --pypi   # upstream core with the released common.ai provider
uv run --no-sync pytest
SWF_AIRFLOW_NO_SYNC=1 scripts/stress_airflow.sh
uv sync --group airflow           # intentionally return to the pinned release
```

`AIRFLOW_REF=<commit>` reproduces a snapshot. The installer checks installed Git provenance and
package dependencies so a same-version release wheel cannot silently stand in for main. The islo
overlay also resolves `AI_PROVIDER_REF` once and verifies both its repository URL and commit. Both CI
jobs invoke this same script. The main job runs the full suite and a live scheduler with eight
admin-attributed HITL approvals, per-job test results, and delivery artifacts. It then clones
each published branch, verifies its approvals and branch isolation, and reruns the target tests. The islo fork job
remains experimental. The live scheduler uses the local sandbox and local Git remotes.

`SWF_TEST_LIVE_TOOLSET=1 uv run --no-sync pytest tests/test_toolset_live.py` separately tests a real
sbx microVM's command transport, file roundtrip, failure exit codes and teardown. It explicitly
requests open networking and does not test confinement or full factory execution inside the VM.
Normal factory runs still send restrictive `SandboxSpec` requirements. The sbx provider needs
`SWF_TOOLSET_SBX_HOST_NETWORK_POLICY=deny-all` to acknowledge an already-configured host policy;
this setting does not configure the host. Set `SWF_TOOLSET_SBX_IMAGE` to a factory-ready image
with Git, test tools and the agent; the upstream Python-only image is insufficient for those jobs.

The supported stack stays pinned to `apache-airflow==3.3.1`: production should not track a dev
branch, while the `airflow-main` canary is configured to expose upstream drift. The GitHub delivery
boundary is unchanged either way — the orchestrator still holds the GitHub credential and still
applies the patch. Model-credential isolation remains a property of the selected backend.

`blueprints/toolset.toml` defines the configuration and DAG wiring, rather than proving a real
backend: the default six-stage order over one target, `[sandbox] kind = "toolset"`, a
self-approving intent gate (an experiment must finish unattended) and a two-hour plan gate,
`max_parallel_jobs = 1` and modest budgets. Every blueprint test and the DAG parity suite
enumerate `blueprints/*.toml`. A line may pin `[sandbox] backend` and absolute `workdir`;
`SWF_TOOLSET_BACKEND` and `SWF_TOOLSET_WORKDIR` are operational overrides. The shipped toolset
line declares `sbx` and `/workspace/repo` explicitly. It is deliberately not the
default: `factory` stays on islo.

### Prior 1.1 compatibility evidence

The following checks were recorded for the 1.1 line against `apache/airflow` main built from
source, with the `common.ai` provider overlaid from the islo backend PR branch. They are historical
evidence and do not validate the 2.0 changes in this delivery:

| what | result |
|---|---|
| versions | airflow **3.4.0**, task-sdk 1.4.0, providers-standard 1.18.0 from `apache/airflow` main; common-ai 0.7.0 from the islo PR branch |
| sandbox backends resolved | `islo` → `IsloSandboxBackend`, `sbx` → `SbxSandboxBackend` (opensandbox and asciibox are separate PRs, and report themselves unavailable by name) |
| Airflow test suite | 31 passed (`tests/test_dag_parity.py`, `tests/test_dag_smoke.py`) |
| a real DAG run | `airflow dags test factory --mark-success-pattern 'job\.approve_.*'` → all 14 tasks, `state=success` |

To re-establish compatibility evidence for the current revision, run
`./scripts/airflow_main.sh`, point `AIRFLOW_HOME` at a scratch directory, and execute the listed
DAG checks. Return to the supported pin with `uv sync --group airflow`.


## The swf binary

`swf` is the operator interface: one native executable that connects to a factory environment,
submits governed work, shows every mapped job, answers approval gates, verifies deliveries and
removes owned sandboxes, with a Ratatui screen (`swf tui`) over the same operations. It is phases B
through E of a migration — CLI, TUI, operations, packaging — and **not** a Rust runtime: the
execution engine is still Python on Airflow and stays there.

The workspace lives in `rust/` because the repository root is the Python package. Dependency
direction is strictly `swf-cli -> swf-tui -> swf-app -> swf-adapters -> swf-domain`; `swf-domain`
compiles with no async runtime at all, which is what makes the ported roll-ups testable against
recorded fixtures. `swf` reads Airflow's public API only — never the metadata database — and writes
exactly four things through it: a triggered DAG run, a gate response, a run marked failed, and a DAG
unpaused.

Two CI jobs keep the two implementations honest rather than merely both green. `rust` runs
`cargo fmt --check`, `cargo clippy … -D warnings`, `cargo test --workspace` and the release build.
`contract-equivalence` runs both halves of `tests/fixtures/contract/`: the pytest half asserts the
Python function still produces the recorded answer, the cargo half asserts the Rust one does, in one
job over one checkout — so regenerating a fixture to match a drifting implementation turns the other
language red in the same run. `scripts/swf_e2e.sh` is the acceptance test: a live Airflow, two
issues across two targets, eight authenticated approvals and four independently verified
deliveries, with `swf` as the only thing that talks to Airflow after the boot.

Full reference, including the exit-code table, the JSON contract, the key map and the security
posture: [swf.md](swf.md).

## Versioning and release

The distribution (`pyproject.toml` `version`, the git tag `vX.Y.Z`, the CHANGELOG heading) follows
[semver](https://semver.org/spec/v2.0.0.html) over the surface a *user of the factory* depends on:
the blueprint schema, the `SWF_*` / `Config` knobs, the `Sandbox` / `Agent` / `Scm` protocols, the
CLI verbs and their flags — `swfactory`'s and `swf`'s alike, including `swf`'s `--json` document
keys and its exit-code table — and the shape of the committed artifact chain (`plan.json`,
`review.json`, `approvals.json`, `metrics.json`). A **breaking change** is therefore concrete: an
existing `blueprints/*.toml` no longer loads, a `SWF_*` env var or `Config` field is removed or
changes meaning, or a protocol method is added, removed or re-signatured so a third-party
`Sandbox`/`Agent`/`Scm` stops satisfying it. Adding a stage function, a sandbox kind, a CLI flag or
an optional blueprint key is a minor release; a fix that keeps every one of those intact is a patch.

`[blueprint] version` inside a blueprint file is a **separate, independent** integer: it versions
the TOML schema, not the package, and moves only when a blueprint written for an older schema can
no longer be read. `swfactory 2.0.0` reads `version = 1`; the two numbers are not expected to
track each other, and neither implies the other's compatibility.

Airflow's own pin (`apache-airflow==3.3.1`) is a dependency, not part of the public surface —
moving it is a minor release unless a DAG a user has triggered stops working, which it would be.

Releasing is a tag push: bump `version`, write the CHANGELOG section, tag `vX.Y.Z`, push the tag.
`.github/workflows/release.yml` refuses a tag that does not match `pyproject.toml` or has no
CHANGELOG section, then runs lint, the hermetic suite, the scripted demo and the DAG tests before
`uv build` and `gh release create` with the wheel, the sdist and that CHANGELOG section as the body.
See [CONTRIBUTING.md](../CONTRIBUTING.md#release) for the exact commands.

## Design decisions

- Blueprints are data in the **factory** repo, never in the target: gates, budgets and tool policy
  must not be agent-editable. Stage semantics stay Python (`stages.py`); a TOML file only chooses
  the walk, the knobs and the targets. Dynamic task mapping fans out over jobs and nothing else —
  nested expansion (e.g. review lenses) is unsupported in Airflow 3.3.1 and loops stay inside stage
  functions.
- **Airflow, not islo Factory lines, is the spine.** The human gates need an approver identity in
  the audit trail, and the HITL response carries `responded_by_user`. `GateOperator` subclasses
  `ApprovalOperator` only to stop it skipping its own child on Reject, so the refusal can be
  recorded and delivered. One state machine, no DAG cycles, no blueprint -> `line.toml` compiler.
- **Delivery is a `git format-patch` stream applied on the orchestrator.** The sandbox never holds a
  GitHub credential, so "the agent never pushes or merges" is structural, not a prompt. It also
  keeps the credential out of the one place model-written code runs, which is why there is no
  per-stage `SandboxExecutor`: running stage tasks as workers *inside* sandboxes would invert the
  trust boundary and require remote logging to get the audit trail back. The `Sandbox` protocol
  stays; the orchestrator stays the only actor with tokens.
- **The operator's client is Rust; the work cell is not.** This bullet used to read "No Rust", and
  it argued exactly one case: a *compiled replacement for `.claude/hooks/swf_guard.py` inside an
  islo sandbox*. That case is unchanged and still decided. A live probe of an islo sandbox found
  `/usr/bin/python3` and no pip or uv, so the Python hook runs where it matters; Claude Code's
  explicit tool inventory and native path-scoped deny rules are the primary gate (checked before
  hooks and not bypassable by hook output); and a compiled guard would add a musl cross-compile
  release pipeline and a binary download step for no new capability. What the old bullet never
  asked is a different question: what an **operator** runs on their own laptop to drive the
  factory. That is now `swf` ([swf.md](swf.md), `rust/`) — a native client of the same Airflow
  `/api/v2`, `gh` and `islo` interfaces `control.py` drives, so connecting to a factory no longer
  requires Python, uv or a virtualenv on the operator's machine. Nothing else moved: `stages.py`,
  `runtime.py`, `agent.py` and the guard hook stay Python, because they run where Airflow and the
  sandbox image already are, and a second implementation of stage semantics is the one migration
  this project will not make.
- **No `CrabboxSandbox`.** crabbox's `-artifact-glob` is SSH-lease-only, islo `-download` caps at
  one file <= 64 KiB, and rsync `sync.delete` would clobber the agent's remote edits, so "every
  provider as a Sandbox" cannot implement `read/write/exists/run`. crabbox stays the test-command
  wrapper.
- **Docker's agent-only sandbox CLI is not the Airflow `sbx` backend.** The former still lacks the
  full exec/file lifecycle this project needs; the latter is wired through `ToolsetSandbox` and
  the `common.ai` `SandboxBackend` contract ([docker.md](docker.md)).
- The default demo's scripted fixtures can be captured with `--record`; eval fixtures are authored
  acceptance contracts. Both are marked `agent=scripted` in `metrics.json`, and the terminal and
  PR body identify scripted execution.
- The orchestrator is Python + uv, stdlib where possible (`subprocess`, `tomllib`, `statistics`,
  `xml.etree`): the factory is glue around `claude`, `islo`, `srt`, `docker`, `gh` and `git`. Every
  loop is bounded; exhaustion is `StageError(kind="policy")` or a `factory:blocked` PR, never a
  retry. The operator binary is Rust for the reason above, and it is glue too — over the same
  interfaces, with no engine of its own.
- The `control.py` / `herd.py` split is deliberate: one module owns the clients (Airflow, `gh`,
  `islo`), the other is pure presentation. Fake-based coverage can exercise the whole TUI without
  a network ([herd.md](herd.md)). `swf` repeats the split with a layer more: `swf-adapters` owns
  the clients, `swf-app` owns the operations, and both `swf-cli` and `swf-tui` are renderers over
  it — so a command and a keystroke cannot answer a gate differently.

## Known limits / accepted risks

- The trusted stage runner executes the target's declared verification command in the same
  sandbox after the write agent exits. Claude's write stages receive file tools, not Bash, but the
  generated tree is still untrusted code and the selected sandbox remains the containment layer.
- `scrub_env` removes common credential families, exact secret variables, and names ending in
  `_API_KEY`, `_ACCESS_TOKEN`, `_AUTH_TOKEN`, `_PASSWORD`, or `_SECRET`. A novel credential name
  can still evade pattern-based scrubbing; prefer a deny-by-default provider gateway and minimal
  explicit pass-through.
- Issue text is untrusted input to every prompt (`intent.md` quotes it verbatim). Mitigations are
  structural rather than textual: the native deny rules, `validate_patch` + the secret scan before
  publish, `allowed_prefixes` confinement, the human gates, and a PR instead of a merge.
- `srt` and `docker` are defense-in-depth, not the production trust boundary. Their real model
  credential is scoped to the agent process/container, but generated files execute later during
  credential-free verification; only islo has gateway-backed phantom tokens.
- `budget_usd` is a ceiling per job, not per run.
- Python's bytecode cache invalidates on mtime seconds + size: a fix that changes a source file to
  the same byte length within the same second as the previous test run can be masked by a stale
  `__pycache__` entry. Real agent edits practically never hit this; the scripted fixtures avoid it.
- Airflow retries and `tasks clear` are safe only as long as `.factory/<run_id>/state/` survives on
  the worker — that directory is the run's authoritative memory (journal, budget inputs, artifact
  copies, baseline, approvals, review, and sandbox identity).

## Stress test

`blueprints/stress.toml` is a third line whose purpose is to exercise the spine under fan-out:
the default stage order, both gates `mode = "auto"` (the unattended backstop), `max_parallel_jobs =
2`, and **two** targets — `demo/target` plus `demo/target-b`, a copy the harness materialises into
the run's cwd rather than a byte-identical second copy committed to the repo (the recorded patches
carry blob hashes, so a second target has to *be* that copy). With `demo/issue2.md` (DEMO-2, whose
acceptance criteria the existing `demo/scripted` fixtures already satisfy) one run is 2 issues x 2
targets = **4 mapped jobs and no new fixtures**: fixtures are keyed by stage, and every job seeds
its own workdir.

```sh
uv run --group airflow pytest tests/test_dag_stress.py    # dag.test(), gates marked success
scripts/stress_airflow.sh                                 # live standalone, gates answered as admin
```

`tests/test_dag_stress.py` is designed to assert, per `map_index`, that a job owns its run id, run
dir, workdir, bare remote, `pr.md`, `approvals.json` and `metrics.json`
(`blueprint == "stress"`), that no
workdir holds another job's `docs/factory/<issue>/`, that `fan_out` returned exactly
`Blueprint.jobs(conf)`, and that `max_active_tis_per_dagrun` on the stage tasks is the blueprint's
`max_parallel_jobs`. `scripts/stress_airflow.sh` boots `airflow standalone` in a throwaway
`AIRFLOW_HOME` on a free port, unpauses the DAG (new DAGs start paused), triggers it over REST,
answers all 8 gates with `swfactory approve <run> <gate> --map-index <i>`, prints a per-job table
and exits non-zero on any failed task — the half `dag.test()` cannot show, since it only marks a
HITL task success: the committed actor is `replay:scripted-replay` under `dag.test()` (marking the
gate successful leaves no response, so the declared `SWF_GATE_REPLAY` fixture answers it) and
`admin` under the script.
One live-only caveat the script encodes: a gate is only answered once its task instance has been
`awaiting_input` since the previous poll. Answering in the sub-second window between the operator
creating the HITL detail and the task parking makes the scheduler see a stale executor event
("finished with state success, but the task instance's state attribute is queued") and fail the
gate — a race a human cannot hit and a polling script hits about once per dozen gates.
