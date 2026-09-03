# swfactory

An AI-native software factory: a GitHub issue goes in, a reviewed pull request with a committed
artifact chain (intent, spec, plan, review, approvals, metrics) comes out. A **blueprint**
(`blueprints/<name>.toml`) declares the line — stage order, human gates, limits, target repos,
sandbox profile, PR labels — and becomes one Airflow 3 DAG and one `swfactory run --blueprint`
command. Claude Code does the stage work inside a sandbox that holds no GitHub credential; the
orchestrator alone talks to GitHub, and a human merges. A keyless scripted replay of a recorded run
doubles as the end-to-end test.

```
 GitHub issue --label factory[:<name>]--> dispatch.yml --POST /api/v2/dags/<name>/dagRuns--> Airflow 3
                                                                                              |
  ORCHESTRATOR (trusted: Airflow worker or your shell)             holds ISLO_API_KEY, GH_TOKEN
  +---------------------------------------------------------------------------------------------+
  | dags/blueprints.py: one DAG per blueprints/*.toml, jobs = issues x targets (task mapping)    |
  |  fan_out > job[ setup > intent > [approve_intent] > spec > plan > [approve_plan]             |
  |                 > build_and_test > review > deliver > metrics ; teardown ]  [..] = HITL gate |
  |  scm.py (gh, git am, push factory/<issue>-<run>, gh pr create)  <-- format-patch ----------+ |
  +-------------------------------------------------------------------------------------------|-+
        | islo use swf-<issue>-<run> --source github://<repo>:main ... | or srt on a host dir  |
  ======|===================== trust boundary (no --env, no tokens) ==========================|===
        v                                                                                     |
  SANDBOX (untrusted compute: islo MicroVM, or srt-confined directory)                        |
  +-------------------------------------------------------------------------------------------|-+
  | clone of the target  ->  claude -p (per-stage --allowedTools, --max-turns, budget)         | |
  | .claude/settings.local.json: Edit(...)/Bash(...) deny rules + swf_guard.py hook (audit)    | |
  | tests run here (or via `crabbox run` on the LOCAL path)  -> bot-authored commits ----------+ |
  | islo: deny-by-default gateway, phantom ANTHROPIC_API_KEY; srt: domain allowlist, real key    |
  +---------------------------------------------------------------------------------------------+
```

## Quickstart (no keys, no network, ~10 s)

```sh
uv sync
uv run pytest            # 160 passed (hermetic: fake subprocess, tmp git repos)
uv run swfactory demo    # scripted replay of a recorded run on demo/target
```

The demo copies `demo/target` to `.factory/<run>/work`, runs every stage of the default blueprint
with fixtures from `demo/scripted`, and "publishes" to a bare git remote at
`.factory/<run>/remote.git`, printing the would-be PR. The build loop really loops:
`build.1.patch` gets the sign wrong, `fix.2.patch` fixes it. The tail of the output is the
`RunReport` table:

```
run                    25d35305
issue                  DEMO-1
agent / sandbox / scm  scripted / local:work / local
stages                 intent:ok → spec:ok → plan:ok → build_and_test:ok → review:ok → deliver:ok
approvals              intent=approve by auto, plan=approve by auto
tests passed           True
pr                     file:///.../.factory/25d35305/pr.md
cost usd               0.0000
  plan                 files=3
  build_and_test       iterations=2, first_pass_ci=0, tests_passed=1, tests_failed=0, tests_count=7
  review               blockers=0, findings=4, dropped_nits=1, fixes=0, blocker=0, major=1, minor=0, nit=3
  deliver              blockers=0, commits=3
```

Use `--approve prompt` to answer the two gates yourself. Exit code is 1 if any job is blocked or
its tests did not pass. `uv run swfactory demo --sandbox srt` runs the same replay with every
command confined by the Anthropic Sandbox Runtime (needs `npx`; ~1 min).

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
| `[sandbox]` | `kind` (`local\|srt\|islo`), islo gateway/environment, `ttl_s`, `idle_s`, `snapshot` | `ttl_s > max gate timeout`; describes where the **real** agent runs |

```sh
uv run swfactory run --blueprint hotfix --issue demo/issue.md --approve auto   # scripted, local
uv run swfactory run --blueprint deps-bump --issue 42 --issue 43 --agent claude --sandbox islo --scm github
```

One issue applied to N `[[targets]]` is N jobs (`<run>-<job_idx>` run ids on the CLI, one map
index per job in Airflow), each with its own sandbox, PR and approvals. `SWF_*` env vars override
blueprint values and CLI flags alike (dev/smoke escape hatch). Without `--agent claude` the run is
a scripted replay and uses `LocalSandbox` unless `--sandbox` says otherwise.

## Artifact chain and human gates

Everything is committed under `docs/factory/<issue>/` in the target, by `swfactory-bot`, with
`Factory-Run` / `Factory-Stage` / `Agent` commit trailers.

| Playbook stage | Artifact | Who approves | Where it runs |
| --- | --- | --- | --- |
| Intent | `intent.md` (issue body verbatim + front matter) | gate 1 (Airflow `job.approve_intent` / CLI confirm) -> `approvals.json` | orchestrator, no agent |
| Spec | `spec.md` | - | agent, read-only tools, in sandbox |
| Plan | `plan.json` (typed) + `plan.md` | gate 2 (`job.approve_plan`) -> `approvals.json` | agent, read-only tools, in sandbox |
| Build + test | bot commits, `agent/build.1.json`, `agent/fix.N.json`, junit | none; bounded by `max_build_iterations` | agent with Edit/Write/`uv run`, tests in sandbox |
| Review | `review.json` (REVIEW.md contract, nit cap, plan fidelity checked in code) | none; one fix + re-review max (`max_review_fixes`) | agent, read-only, in sandbox |
| Deliver | `metrics.json` (incl. `blueprint`), PR labeled per blueprint (+ `factory:blocked`) | human merges: CODEOWNERS + branch protection | orchestrator (`git am`, push, `gh pr create`) |

`Scm` has no merge method and the sandbox has nothing to push with; `deliver` pulls
`git format-patch <base>..HEAD` out and applies it on the orchestrator. Every stage is idempotent
(status `skipped` when its artifact exists) so Airflow retries and `tasks clear` are safe.

## Sandboxes

| `--sandbox` | Isolation | Credentials the agent can see | Needs | Use |
| --- | --- | --- | --- | --- |
| `local` | none (`scrub_env` only) | none — `agent=claude` refused unless `--allow-local-agent` | nothing | demo, pytest, CI |
| `srt` | OS-level (macOS Seatbelt / Linux bubblewrap): writes limited to the workdir + Claude/uv caches, `factory.toml` `protected` globs and `.claude`/`.github` kernel read-only, egress domain allowlist, `~/.ssh` `~/.aws` `~/.config/gh` unreadable | the real `ANTHROPIC_API_KEY` (or the host's Claude OAuth login); shares the host kernel; proxy-based egress | `srt` on PATH or `npx` (`@anthropic-ai/sandbox-runtime`); Linux: bubblewrap + socat | cloudless real agent on a keyed dev box |
| `islo` | Firecracker-class MicroVM, deny-by-default gateway | phantom `ANTHROPIC_API_KEY` swapped on egress; never a GitHub token | `islo login`, gateway profile + environment (below) | production (Airflow), `demo --real`, evals |

Honest limits: phantom tokens exist only on islo — `srt` is defense-in-depth for your own machine,
not the production trust boundary; tools that ignore `HTTP_PROXY` bypass its allowlist; srt
forbids writes to `.git/config`/`.git/hooks`, so git identity travels as `git -c user.name=...`;
pause/resume and `--snapshot` exist only on islo; `budget_usd` is per job, not per run.

The cloudless real-agent path (Claude logged in or `ANTHROPIC_API_KEY` in your shell; no sandbox
vendor account):

```sh
uv run swfactory run --issue <n|path> --agent claude --sandbox srt --scm local --approve prompt   # or --scm github
```

Inside the sandbox the agent runs with `.claude/settings.local.json` written by `install_guard`:
native `permissions.deny` rules (`Edit(REVIEW.md)`, `Edit(factory.toml)`, `Edit(.claude/**)`,
`Edit(.github/**)`, `Edit(<protected glob>)`, `Bash(git push*)`, `Bash(gh pr *)`, `Bash(git commit*)`,
`Bash(curl *)`, `Bash(wget *)`, `Read(.env*)`) are the primary gate; the PreToolUse hook
`.claude/hooks/swf_guard.py` (Python 3, present in the islo image) is defense-in-depth and writes the
`.factory/hooks.jsonl` audit log.

## Real run (islo)

Prerequisites on the orchestrator: `uv`, `git`, `gh`, `islo` (0.48+), a GitHub token for
`swfactory-bot` in `GH_TOKEN`. No `ANTHROPIC_API_KEY` on the host: it lives in the islo
environment and reaches the sandbox only as a phantom token.

```sh
islo login && islo login --tool github && islo login --tool claude
islo gateway create --name swfactory --default-action deny --internet-access true
#   allow on the profile (console): api.anthropic.com github.com pypi.org files.pythonhosted.org astral.sh
islo environment create --name swfactory \
  --gateway-secret 'ANTHROPIC_API_KEY=<real key>;host=api.anthropic.com;auth=bearer'
export GH_TOKEN=<fine-grained PAT for swfactory-bot: contents+pull_requests write, issues write>
gh api -X PUT repos/zozo123/ariflow-swfactory/branches/main/protection --input - <<'JSON'
{"required_status_checks":{"strict":true,"contexts":["test"]},"enforce_admins":false,
 "required_pull_request_reviews":{"require_code_owner_reviews":true,"required_approving_review_count":1},
 "restrictions":null}
JSON
uv run swfactory demo --real     # = run --issue demo/issue.md --agent claude --sandbox islo --scm github --approve prompt
uv run swfactory run --issue 42 --agent claude --sandbox islo --scm github
```

`.github/CODEOWNERS` (`* @zozo123`) makes the human the required reviewer. The sandbox
`swf-<issue>-<run>` is created with `--auto-resume on_activity --pause-after-idle 900
--delete-after <ttl>`; `islo cp` does not resume a paused VM, so file transfers retry after
`islo resume`. Add `--record demo/scripted` to rewrite the demo fixtures from real agent outputs.

Warm start: bake a snapshot once and set it in the blueprint (`[sandbox] snapshot`) or
`SWF_ISLO_SNAPSHOT`; `islo.yaml`'s setup script (uv only) does not re-run from a snapshot.

```sh
islo use swf-golden --source github://zozo123/ariflow-swfactory:main --gateway-profile swfactory \
  --environment swfactory --init minimal --output plain -- \
  bash -lc 'cd /workspace/ariflow-swfactory/demo/target && uv sync --group dev && claude --version'
islo snapshot save swf-golden --name swf-golden-$(date +%Y%m%d) && islo rm swf-golden
export SWF_ISLO_SNAPSHOT=swf-golden-$(date +%Y%m%d)
```

Dev escape hatch, honestly: `--agent claude --sandbox local --allow-local-agent` runs the real
agent and its code unconfined on your machine in `.factory/<run>/work`; `LocalSandbox` scrubs
`ANTHROPIC_*`, so `claude` must be logged in on the host. Prefer `--sandbox srt`.

## Airflow

```sh
uv sync --group airflow                              # apache-airflow 3.3.1 + standard provider
export AIRFLOW_HOME=$PWD/airflow_home                # gitignored
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags AIRFLOW__CORE__LOAD_EXAMPLES=False
uv run airflow standalone                            # UI on http://localhost:8080
```

`dags/blueprints.py` emits one DAG per `blueprints/*.toml` (dag_id = `blueprint.name`). A run fans
`{"issues": [...]}` ({`"issue": N`} accepted, optional `"targets": [...]` filter) x the blueprint's
`[[targets]]` out into a mapped `job` task group — `setup > intent > [approve_intent > record_intent]
> spec > plan > [approve_plan > record_plan] > build_and_test > review > deliver > metrics ;
teardown` — one sandbox and one addressable approval per (issue, target), `max_parallel_jobs`
at a time. `deliver` publishes the asset `swf.metrics.<blueprint>`, so `dags/maintain.py` runs after
every delivery as well as nightly at 03:00 UTC.

Trigger: label an issue `factory` (or `factory:<name>`; `dispatch.yml` POSTs
`/api/v2/dags/<name>/dagRuns` with the `AIRFLOW_URL` / `AIRFLOW_TOKEN` repo secrets), or
`uv run airflow dags trigger factory --conf '{"issues": ["42"]}'`. Approve gates in the UI (Required
Actions shows the head of intent.md / plan.md) or from the shell:

```sh
export AIRFLOW_TOKEN=$(curl -s -X POST localhost:8080/auth/token -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"<airflow_home/simple_auth_manager_passwords.json.generated>"}' | jq -r .access_token)
uv run swfactory approve <dag_run_id> intent          # or: plan, --reject, --blueprint <name>, --map-index <job>
```

Workers read `SWF_*` env vars (`SWF_AGENT`, `SWF_SANDBOX`, `SWF_SCM`, ...), which override the
blueprint. `airflow dags test` never resolves HITL tasks, so mark the gates:

```sh
uv run airflow dags test factory --conf '{"issues":["demo/issue.md"]}' --mark-success-pattern 'job\.approve_.*'
```

`SWF_APPROVE=auto` (or a gate's `auto = true`) only makes a real run's gate default to Approve once
its `timeout_h` elapses. `tests/test_dag_parity.py` asserts every blueprint's DAG mirrors its stage
order and gates; `tests/test_dag_smoke.py` runs the recipe above end to end:
`uv run --group airflow pytest tests/test_dag_parity.py tests/test_dag_smoke.py` (17 passed).

## crabbox

crabbox is a command wrapper, not a sandbox. With `--tests crabbox` (local sandbox only)
`run_tests` wraps the target's test command in
`crabbox run -provider <provider> -junit .factory/junit.xml -download .factory/junit.xml=.factory/junit.xml -ttl 45m -idle-timeout 15m -- <cmd>`
(no `-download` for in-place providers such as `srt`, `docker-sandbox`, `apple-machine`) and reads
the junit file it brings back. Never `-allow-env`, `-env-from-profile` or `-keep`. v2 fixes three v1
bugs: `-artifact-glob` (SSH-lease providers only) -> `-download`; default provider `islo` ->
`local-container` (`scrub_env` strips the `ISLO_API_KEY` islo needs); `.crabbox.yaml` jobs are maps
so `crabbox doctor` parses it. Human inner loop: `crabbox run --provider local-container -- uv run pytest`.

## Maintain

`bands.yaml` defines a window (20 runs) and three response tiers over `docs/factory/*/metrics.json`
of a checkout: 1σ `log`, 2σ `diagnose` (read-only agent, `Diagnosis` schema, incident record at
`docs/factory/incidents/<date>-<metric>.md`), 3σ `propose` (drafted intent + `gh issue create
--label factory`, which re-enters the factory). Detection is `statistics.mean`/`stdev`, needs at
least 3 history samples, and ignores `agent=scripted` runs so the demo never moves the bands.

```sh
uv run swfactory metrics --root .       # first-pass rate, mean iterations, p50 cycle, findings, cost
uv run swfactory maintain --root . [--sweep-ttl-s 172800]   # band check (+ orphan swf-* sandbox sweep)
```

## Layout

```
blueprints/default.toml      the `factory` line; hotfix.toml: a second line (no spec, auto intent gate)
src/swfactory/blueprint.py   Blueprint models, load/loads/resolve, pipeline(), jobs(conf), config(job)
src/swfactory/config.py      Config (SWF_* env > init), TargetContract from factory.toml
src/swfactory/models.py      Issue, Plan, Review, Finding, Approval, StageResult (+preview), RunReport
src/swfactory/sandbox.py     Sandbox protocol, LocalSandbox, SrtSandbox, IsloSandbox, make_sandbox
src/swfactory/agent.py       Agent protocol, POLICIES, ClaudeAgent, ScriptedAgent, install_guard
src/swfactory/scm.py         Scm protocol, LocalGitScm (bare remote + pr.md), GitHubScm (gh)
src/swfactory/stages.py      Ctx, CANONICAL_ORDER/STAGES, stage functions, run_tests, commit, run_pipeline
src/swfactory/metrics.py     write_run_metrics, load_all, summarize, table
src/swfactory/maintain.py    load_runs, detect, run, sweep_sandboxes
src/swfactory/cli.py         typer: run, demo, metrics, approve, maintain
src/swfactory/prompts/*.md   spec, plan, build, fix, review, diagnose templates
dags/blueprints.py           one mapped-task-group DAG per blueprint (ApprovalOperator gates)
dags/maintain.py             nightly + after every delivery (AssetOrTimeSchedule) band check + sweep
.claude/hooks/swf_guard.py   PreToolUse audit hook installed into every target before write stages
.claude/skills/swfactory/    spec/plan shape + review contract for the agent
REVIEW.md  bands.yaml        review policy; maintain tiers
islo.yaml  .crabbox.yaml     sandbox setup (uv only); crabbox profile
demo/                        issue.md (DEMO-1), target/ (`calc` + factory.toml), scripted/ fixtures
tests/                       hermetic; test_dag_*.py need the airflow group
.github/workflows/           ci (ruff, pytest, demo, airflow parity+smoke, srt smoke), dispatch, evals
```

## Design decisions

- Blueprints are data in the **factory** repo, never in the target: gates, budgets and tool policy
  must not be agent-editable. Stage semantics stay Python (`stages.py`); a TOML file only chooses
  the walk, the knobs and the targets. Dynamic task mapping fans out over jobs and nothing else —
  nested expansion (e.g. review lenses) is unsupported in Airflow 3.3.1 and loops stay inside stage
  functions.
- Airflow, not islo Factory lines, is the spine: the human gates need an approver identity in the
  audit trail, and `ApprovalOperator` records `responded_by_user`. One state machine, no DAG cycles.
- Delivery is a `git format-patch` stream applied on the orchestrator: the sandbox never holds a
  GitHub credential, so "the agent never pushes or merges" is structural, not a prompt.
- **No Rust.** A live probe of an islo sandbox found `/usr/bin/python3`, so the Python hook runs
  where it matters; Claude Code's native `Edit(...)`/`Bash(...)` deny rules are the primary gate
  anyway (checked before hooks, not bypassable by hook output). A compiled guard would add a musl
  cross-compile release pipeline and a binary download step for no new capability.
- **No CrabboxSandbox.** crabbox's `-artifact-glob` is SSH-lease-only, islo `-download` caps at one
  file <= 64 KiB, and rsync `sync.delete` would clobber the agent's remote edits, so "every provider
  as a Sandbox" cannot implement `read/write/exists/run`. crabbox stays the test-command wrapper.
- Scripted fixtures are recordings (`--record`), not hand-written theater; `metrics.json` marks
  them `agent=scripted` and a banner says so in the terminal and the PR body.
- Python + uv, stdlib where possible (`subprocess`, `tomllib`, `statistics`, `xml.etree`): the
  factory is glue around `claude`, `islo`, `srt`, `gh`, `git`. Every loop is bounded; exhaustion is
  `StageError(kind="policy")` or a `factory:blocked` PR.

## License

Apache-2.0
