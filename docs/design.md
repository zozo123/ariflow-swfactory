# Design: the line, the DAG, and why they are shaped this way

Reference for the parts the README only names: how a blueprint declares a line, what the generated
Airflow DAG does, how metrics and bands close the loop, and the decisions (and accepted risks)
behind all of it.

## Blueprints

`blueprints/default.toml` (name `factory`) is the default SDLC line; `blueprints/hotfix.toml` is a
second line with zero Python: no `spec` stage, an intent gate that approves itself, a 4 h plan gate,
and an extra `hotfix` PR label. A blueprint is validated by `swfactory.blueprint.Blueprint`
(pydantic) and shape-read by the DAG generator with stdlib `tomllib`:

| Section | What it fixes | Rules |
| --- | --- | --- |
| `[blueprint]` | `name` = DAG id = CLI name | `^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$`, must equal the CLI name |
| `[[targets]]` | `repo`, `dir`, `base_branch` — jobs per run = issues x targets | >= 1 |
| `[stages] order` | which stage functions run | subsequence of `intent spec plan build_and_test review deliver`, first `intent`, last `deliver`; omitted inputs render `(none)` |
| `[[gates]]` | `after`, `artifact`, `timeout_h`, `assigned`, `auto` | `after` in `{intent, plan}` and in `order`; `auto=true` -> the gate defaults to Approve (actor `auto`) |
| `[limits]` | build/review iterations, turns, USD per stage / per **job**, `stage_timeout_h`, `max_parallel_jobs` | `budget_usd_per_stage <= budget_usd` |
| `[policy.<stage>]` | `extra_allowed_tools`, `model` | additive only: `disallowed_tools` cannot be touched |
| `[review]` / `[deliver]` | `nit_cap`; PR `labels` | |
| `[sandbox]` | `kind` (`local\|srt\|docker\|islo`), islo gateway/environment, `ttl_s`, `idle_s`, `snapshot` | `ttl_s > max gate timeout`; describes where the **real** agent runs |

`SWF_*` env vars override blueprint values and CLI flags alike (the dev/smoke escape hatch).
Without `--agent claude` a run is a scripted replay and uses `LocalSandbox` unless `--sandbox` says
otherwise.

## Airflow

Pinned to the latest release (`apache-airflow==3.3.1`, standard provider 1.18.0). The optional CI
job `airflow-main` also runs DAG parity and smoke against upstream `apache/airflow@main` (built
from source) so API drift in the task SDK or the HITL operators shows up early.

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

Workers read the `SWF_*` env vars, which override the blueprint. `airflow dags test` never resolves
HITL tasks, so mark the gates:

```sh
uv run airflow dags test factory --conf '{"issues":["demo/issue.md"]}' --mark-success-pattern 'job\.approve_.*'
```

`SWF_APPROVE=auto` (or a gate's `auto = true`) only makes a real run's gate default to Approve once
its `timeout_h` elapses; a gate's `assigned` users become the HITL `assigned_users`. Gates are a
`GateOperator` (an `ApprovalOperator` that never skips on its own): the response — Approve or
Reject, with `responded_by_user` — lands in XCom, `record_<stage>` writes it to `approvals.json`,
and on Reject raises `AirflowSkipException`; the work stages skip while `deliver`/`metrics` run with
`trigger_rule="none_failed"` and publish the `[REJECTED]` PR. Every task rebuilds its `Ctx`; the run
id is `sha1(dag_run_id#job_idx)[:8]`, so retries share the sandbox and the orchestrator's stage log
`.factory/<run_id>/stages.jsonl` on the worker. New DAGs start paused in standalone: unpause once in
the UI or with `PATCH /api/v2/dags/factory {"is_paused": false}`.

`tests/test_dag_parity.py` asserts every blueprint's DAG mirrors its stage order and gates, and
`tests/test_dag_smoke.py` runs the recipe above end to end:

```sh
uv run --group airflow pytest tests/test_dag_parity.py tests/test_dag_smoke.py
```

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
uv run swfactory maintain --root . [--sweep-ttl-s 172800]   # band check (+ orphan swf-* sandbox sweep)
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

## Versioning and release

The distribution (`pyproject.toml` `version`, the git tag `vX.Y.Z`, the CHANGELOG heading) follows
[semver](https://semver.org/spec/v2.0.0.html) over the surface a *user of the factory* depends on:
the blueprint schema, the `SWF_*` / `Config` knobs, the `Sandbox` / `Agent` / `Scm` protocols, the
CLI verbs and their flags, and the shape of the committed artifact chain (`plan.json`,
`review.json`, `approvals.json`, `metrics.json`). A **breaking change** is therefore concrete: an
existing `blueprints/*.toml` no longer loads, a `SWF_*` env var or `Config` field is removed or
changes meaning, or a protocol method is added, removed or re-signatured so a third-party
`Sandbox`/`Agent`/`Scm` stops satisfying it. Adding a stage function, a sandbox kind, a CLI flag or
an optional blueprint key is a minor release; a fix that keeps every one of those intact is a patch.

`[blueprint] version` inside a blueprint file is a **separate, independent** integer: it versions
the TOML schema, not the package, and moves only when a blueprint written for an older schema can
no longer be read. `swfactory 1.0.0` reads `version = 1`; the two numbers are not expected to
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
- **No Rust.** A live probe of an islo sandbox found `/usr/bin/python3`, so the Python hook runs
  where it matters; Claude Code's native `Edit(...)`/`Bash(...)` deny rules are the primary gate
  anyway (checked before hooks, not bypassable by hook output). A compiled guard would add a musl
  cross-compile release pipeline and a binary download step for no new capability.
- **No `CrabboxSandbox`.** crabbox's `-artifact-glob` is SSH-lease-only, islo `-download` caps at
  one file <= 64 KiB, and rsync `sync.delete` would clobber the agent's remote edits, so "every
  provider as a Sandbox" cannot implement `read/write/exists/run`. crabbox stays the test-command
  wrapper.
- **Docker Sandboxes stay documented, not wired** — they run an agent only, with no `exec` for
  tests and git ([docker.md](docker.md)).
- Scripted fixtures are recordings (`--record`), not hand-written theater; `metrics.json` marks them
  `agent=scripted` and a banner says so in the terminal and in the PR body.
- Python + uv, stdlib where possible (`subprocess`, `tomllib`, `statistics`, `xml.etree`): the
  factory is glue around `claude`, `islo`, `srt`, `docker`, `gh` and `git`. Every loop is bounded;
  exhaustion is `StageError(kind="policy")` or a `factory:blocked` PR, never a retry.
- The `control.py` / `herd.py` split is deliberate: one module owns the clients (Airflow, `gh`,
  `islo`), the other is pure presentation, so the whole TUI is unit-tested with fakes and no
  network ([herd.md](herd.md)).

## Known limits / accepted risks

- `Bash(uv run *)` is arbitrary code execution inside the sandbox, by design: the agent has to run
  the target's tests. The sandbox **is** the trust boundary (islo MicroVM in production, srt or a
  container on a dev box); the deny rules and the hook shape what the agent does, they do not
  contain it.
- `scrub_env` is a prefix denylist (`ANTHROPIC_*`, `GH_TOKEN`, `GITHUB_TOKEN`, `AWS_*`,
  `ISLO_API*`), so a credential under another name in the orchestrator's environment reaches a
  `local`/`srt`/`docker` child. Pass secrets through the islo gateway (phantom tokens), not the
  environment.
- Issue text is untrusted input to every prompt (`intent.md` quotes it verbatim). Mitigations are
  structural rather than textual: the native deny rules, `validate_patch` + the secret scan before
  publish, `allowed_prefixes` confinement, the human gates, and a PR instead of a merge.
- `srt` and `docker` are defense-in-depth, not the production trust boundary: a real
  `ANTHROPIC_API_KEY` (or your OAuth session) is present, and only islo has phantom tokens.
- `budget_usd` is a ceiling per job, not per run.
- Python's bytecode cache invalidates on mtime seconds + size: a fix that changes a source file to
  the same byte length within the same second as the previous test run can be masked by a stale
  `__pycache__` entry. Real agent edits practically never hit this; the scripted fixtures avoid it.
- Airflow retries and `tasks clear` are safe only as long as `.factory/<run_id>/` survives on the
  worker — that directory is the run's memory (stage log, budget, sandbox identity).

## Stress test

`blueprints/stress.toml` is a third line whose only purpose is to prove the spine under fan-out:
the default stage order, both gates `auto = true` (the unattended backstop), `max_parallel_jobs =
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

`tests/test_dag_stress.py` asserts, per `map_index`, that a job owns its run id, run dir, workdir,
bare remote, `pr.md`, `approvals.json` and `metrics.json` (`blueprint == "stress"`), that no
workdir holds another job's `docs/factory/<issue>/`, that `fan_out` returned exactly
`Blueprint.jobs(conf)`, and that `max_active_tis_per_dagrun` on the stage tasks is the blueprint's
`max_parallel_jobs`. `scripts/stress_airflow.sh` boots `airflow standalone` in a throwaway
`AIRFLOW_HOME` on a free port, unpauses the DAG (new DAGs start paused), triggers it over REST,
answers all 8 gates with `swfactory approve <run> <gate> --map-index <i>`, prints a per-job table
and exits non-zero on any failed task — the half `dag.test()` cannot show, since it only marks a
HITL task success: the committed actor is `auto` under `dag.test()` and `admin` under the script.
One live-only caveat the script encodes: a gate is only answered once its task instance has been
`awaiting_input` since the previous poll. Answering in the sub-second window between the operator
creating the HITL detail and the task parking makes the scheduler see a stale executor event
("finished with state success, but the task instance's state attribute is queued") and fail the
gate — a race a human cannot hit and a polling script hits about once per dozen gates.
