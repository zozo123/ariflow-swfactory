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
  ORCHESTRATOR (trusted: islo orchestrator sandbox / Airflow worker / your shell)  ISLO_API_KEY, GH_TOKEN
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
uv run pytest            # 235 passed (hermetic: fake subprocess, tmp git repos)
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
  deliver              blockers=0, commits=3, rejected=0, denied_tool_calls=0
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
| Review | `review.json` (REVIEW.md contract, nit cap, plan fidelity — both halves — checked in code) | none; `max_review_fixes` fix + test + re-review rounds (default 1); a fix that leaves the suite red is itself a blocker | review: agent, read-only; fix: agent with Edit/Write, tests dir protected |
| Deliver | `metrics.json` (incl. `blueprint`, `denied_tool_calls`), `agent/stages.jsonl` + `agent/hooks.jsonl` audit copies, PR labeled per blueprint (+ `factory:blocked` / `factory:rejected`) | human merges: CODEOWNERS + branch protection | orchestrator (`git am`, push, `gh pr create`) |

`Scm` has no merge method and the sandbox has nothing to push with; `deliver` pulls
`git format-patch <base>..HEAD` out and applies it on the orchestrator. Before anything touches
git or the network the patch is policy-checked (`scm.validate_patch`: no absolute paths, no `..`,
nothing under `.git/`, no symlink modes, every path under the target dir or `docs/factory/`) and
scanned for secret-shaped tokens (AWS, Anthropic, GitHub, Google, Slack keys, private-key blocks);
a hit is `StageError("policy")` and nothing is published. Delivery is retry-safe: `factory/*` is
the bot-owned branch namespace and is force-updated on a retried `deliver`, and an open PR for the
branch is edited in place instead of duplicated.

Run state lives on the **orchestrator**: `_timed` appends every `StageResult` to
`.factory/<run_id>/stages.jsonl` (a copy goes into the sandbox for the audit trail, but is never
trusted). A stage is `skipped` only when that log holds a completed record for it — an artifact the
agent forged in the sandbox cannot skip a stage — and the run budget (`budget_usd`) is seeded from
the same log, so it is a ceiling per run even when Airflow splits the run over tasks and workers.
Airflow retries and `tasks clear` are safe as long as `.factory/<run_id>/` survives on the worker.

A rejected gate is part of the audit chain, not a dead end: the CLI (and the DAG, via
`trigger_rule="none_failed"` on `deliver`) records the refusal and its actor in `approvals.json`,
skips the remaining work stages, and still delivers — a `[REJECTED]` PR labeled `factory:rejected`
carrying `intent.md`, `approvals.json` and `metrics.json`, `status="blocked"`, exit code 1.

## Sandboxes

| `--sandbox` | Isolation | Credentials the agent can see | Needs | Use |
| --- | --- | --- | --- | --- |
| `local` | none (`scrub_env` only) | none — `agent=claude` refused unless `--allow-local-agent` | nothing | demo, pytest, CI |
| `srt` | OS-level (macOS Seatbelt / Linux bubblewrap): writes limited to the workdir + Claude/uv caches; `factory.toml` `protected` globs are kernel `denyWrite` **per stage** (tests dir writable for `build`, denied for `fix`, re-synced before every agent call) plus `.claude`/`.github`; egress domain allowlist; `~/.ssh` `~/.aws` `~/.config` `~/.gnupg` `~/.netrc` `~/.docker` `~/.kube` unreadable | the real `ANTHROPIC_API_KEY` (or the host's Claude OAuth login); shares the host kernel; proxy-based egress | `srt` on PATH or `npx` (`@anthropic-ai/sandbox-runtime`); Linux: bubblewrap + socat | cloudless real agent on a keyed dev box; `evals.yml` |
| `islo` | Firecracker-class MicroVM, deny-by-default gateway | phantom `ANTHROPIC_API_KEY` swapped on egress; never a GitHub token | `islo login` + `--tool github/claude`, gateway profile + environment (`deploy/islo/bootstrap.sh`), `swfactory doctor` green | production: the two-tier topology below, `demo --real`, `evals-islo` |

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
`Edit(.github/**)`, `Edit(docs/factory/**)`, `Edit(.factory/**)`, `Edit(<protected glob>)`,
`Bash(git push*)`, `Bash(gh pr *)`, `Bash(git commit*)`, `Bash(curl *)`, `Bash(wget *)`,
`Read(.env*)`) are the primary gate — the artifact chain and the stage scratch belong to the
orchestrator, so the agent cannot forge `review.json`, `approvals.json` or its own audit log; the
PreToolUse hook `.claude/hooks/swf_guard.py` (Python 3, present in the islo image) is
defense-in-depth and writes the `.factory/hooks.jsonl` audit log (denied calls are counted as
`denied_tool_calls` in `metrics.json` and the PR's deliver row). `install_guard` also ships the
factory's `.claude/skills/swfactory/` skill into the target, so the agent reads the same
spec/plan/review contract wherever its cwd is.

## Run the factory on islo (production)

Two tiers, both islo sandboxes, one trust boundary between them. Nothing that can push to GitHub
ever shares a VM with model-generated code, and nothing that can call Anthropic ever sees a GitHub
token:

| Tier | Runs | Credentials (all phantom: swapped by the gateway on egress) | Egress |
| --- | --- | --- | --- |
| **Orchestrator** — one sandbox, `swf-orchestrator` (trusted) | Airflow 3 (`airflow standalone`, UI `:8080`), `swfactory webhook serve --port 8081 --airflow-url http://localhost:8080` (GitHub issue events -> `POST /api/v2/dags/<name>/dagRuns`), and `deliver`: `git am` of the agent's format-patch stream, push `factory/*`, `gh pr create` | `GH_TOKEN` (`islo login --tool github`), `ISLO_API_KEY` to spawn agent VMs; **no** Anthropic key | `swfactory-orchestrator` gateway: github.com, api.github.com, the islo API |
| **Agents** — one MicroVM per (issue, target), `swf-<issue>-<run>` (untrusted) | clone of the target (`--source`), `claude -p` per stage, the target's tests, bot-authored commits | `ANTHROPIC_API_KEY` (`islo login --tool claude`); never a GitHub token, never `--env` | `swfactory` gateway, deny-by-default: api.anthropic.com, pypi.org, files.pythonhosted.org, astral.sh |

The orchestrator spawns agent VMs with the same `IsloSandbox.argv` the CLI uses (`--gateway-profile
swfactory --environment swfactory --init minimal --delete-after --pause-after-idle --auto-resume
on_activity`), reads artifacts out with `islo cp`, and applies the patch on its own side — the
"agent never pushes" property is structural (see "Artifact chain"), not a prompt. Commands, in order:

```sh
islo login && islo login --tool github && islo login --tool claude   # once per org (phantom tokens)
deploy/islo/bootstrap.sh    # one-time: agent gateway/environment, optional snapshot, knowledge items
export GITHUB_WEBHOOK_SECRET=$(openssl rand -hex 32)  # generate once; store/reuse on every redeploy
deploy/islo/deploy.sh       # orchestrator sandbox from deploy/islo/orchestrator/{islo.yaml,start.sh}:
                            #   Airflow + webhook receiver; prints the shared Airflow UI URL
uv run swfactory doctor     # preflight: islo/gh/claude CLIs, login + tool integrations, gateway profile,
                            #   environment, snapshot, gh repo; --json for CI (evals-islo runs it first)
gh issue edit 42 --add-label factory        # GitHub -> islo incoming webhook -> :8081 -> DAG `factory`
uv run swfactory approve <dag_run_id> intent        # or approve both gates in the Airflow UI at the
uv run swfactory approve <dag_run_id> plan          #   shared URL (islo share swf-orchestrator 8080)
# -> PR on the target, labeled per blueprint (factory[:blocked|:rejected]); a human merges
```

`deploy/islo/knowledge.sh [owner/repo]` (called by bootstrap, safe to rerun) publishes `CLAUDE.md`
and `REVIEW.md` as `rule` items and `.claude/skills/swfactory/SKILL.md` as a `skill`, tagged
`swfactory` and linked to the repo (`islo knowledge get` -> `update`, else `create`), so
`islo knowledge render --repo <owner/repo> --tag swfactory` gives every sandbox agent the same
contract that `install_guard` ships into the target. `dispatch.yml` (a GitHub Action posting to the
Airflow API with the `AIRFLOW_URL`/`AIRFLOW_TOKEN` secrets) stays as the alternative trigger when
the orchestrator's `:8080` is shared instead of `:8081`. `.github/CODEOWNERS` (`* @zozo123`) plus
branch protection (`gh api -X PUT repos/<owner/repo>/branches/main/protection`: 1 review, code
owners, `test` status check) make the human the required reviewer. `demo --real`
(`run --issue demo/issue.md --agent claude --sandbox islo --scm github --approve prompt`) is the
same path from your shell; add `--record demo/scripted` to rewrite the demo fixtures from real
agent outputs. Agent VMs are created with `--auto-resume on_activity --pause-after-idle 900
--delete-after <ttl>`; `islo cp` does not resume a paused VM, so file transfers retry once after
`islo resume`.

Warm start: bake a snapshot once and set it in the blueprint (`[sandbox] snapshot`) or
`SWF_ISLO_SNAPSHOT` (the repo variable of the same name feeds `evals-islo`); `islo.yaml`'s setup
script (uv only) does not re-run from a snapshot.

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
its `timeout_h` elapses; a gate's `assigned` users become the HITL `assigned_users`. Gates are a
`GateOperator` (an `ApprovalOperator` that never skips on its own): the response — Approve or
Reject, with `responded_by_user` — lands in XCom, `record_<stage>` writes it to `approvals.json`,
and on Reject raises `AirflowSkipException`; the work stages skip, `deliver`/`metrics` run with
`trigger_rule="none_failed"` and publish the `[REJECTED]` PR. Every task rebuilds its `Ctx`; the
run id is `sha1(dag_run_id#job_idx)[:8]`, so retries share the sandbox and the orchestrator log
`.factory/<run_id>/stages.jsonl` on the worker. `tests/test_dag_parity.py` asserts every
blueprint's DAG mirrors its stage order and gates; `tests/test_dag_smoke.py` runs the recipe above
end to end:
`uv run --group airflow pytest tests/test_dag_parity.py tests/test_dag_smoke.py` (21 passed).

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
`docs/factory/incidents/<date>-<metric>.md` **and** an issue labeled `maintain, incident` carrying
it, so the diagnosis outlives the checkout), 3σ `propose` (drafted intent + incident record in one
`gh issue create --label factory`, which re-enters the factory). Detection is
`statistics.mean`/`stdev`, needs at least 3 history samples, and ignores `agent=scripted` runs so
the demo never moves the bands. A flat history (stdev 0 — the normal shape for `first_pass_ci` or
`blockers`) makes any move in the bad direction a top-tier breach reported with `stdev=0`; no move,
or a move in the good direction, is not a breach.

The `maintain` DAG never reads relative to the worker's cwd: it uses `$SWF_MAINTAIN_ROOT` (a
checkout of the target) when set, else a shallow read-only clone of the target's base branch made
for the task, and fails loudly when the result has no `docs/factory/`. `bands.yaml` defaults to the
factory checkout's own copy (`$SWF_BANDS` overrides).

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
src/swfactory/cli.py         typer: run, demo, metrics, approve, maintain, doctor, webhook serve
src/swfactory/prompts/*.md   spec, plan, build, fix, review, diagnose templates
dags/blueprints.py           one mapped-task-group DAG per blueprint (GateOperator gates, record_<stage>)
dags/maintain.py             nightly + after every delivery (AssetOrTimeSchedule) band check + sweep
.claude/hooks/swf_guard.py   PreToolUse audit hook installed into every target before write stages
.claude/skills/swfactory/    spec/plan shape + review contract for the agent
REVIEW.md  bands.yaml        review policy; maintain tiers
islo.yaml  .crabbox.yaml     agent sandbox setup (uv only); crabbox profile
deploy/islo/                 bootstrap.sh (gateways, environments, webhook, knowledge), deploy.sh
                             (orchestrator sandbox: orchestrator/{islo.yaml,start.sh}), knowledge.sh
demo/                        issue.md (DEMO-1), target/ (`calc` + factory.toml), scripted/ fixtures
tests/                       hermetic; test_dag_*.py need the airflow group
.github/workflows/           ci (ruff, pytest, demo, airflow parity+smoke, srt smoke), dispatch,
                             evals: real-demo (claude under srt) + evals-islo (claude in an islo
                             MicroVM, phantom key, ISLO_API_KEY only); both assert report.json
```

## Proof (real runs on this repo)

Two unattended runs of `swfactory run --issue 1 --agent claude --sandbox srt --scm github` against
[issue #1](https://github.com/zozo123/ariflow-swfactory/issues/1):

| run | outcome | what it shows |
|---|---|---|
| [PR #2](https://github.com/zozo123/ariflow-swfactory/pull/2) `factory:blocked` | build passed, review found **1 blocker** (no tests), 1 fix attempt could not resolve it | the factory ships a labeled, blocked PR instead of merging; the reviewer diagnosed the root cause (tests/ was deny-listed in the build stage — fixed in `e94ed37`) and caught a real spec error (R6 sign) |
| [PR #3](https://github.com/zozo123/ariflow-swfactory/pull/3) | 14 tests, first-pass CI, **0 findings**, $1.94 | the clean path: intent → spec → plan → build+test → review → PR, every artifact committed under `docs/factory/1/` |

Both PRs carry bot-authored commits with `Factory-Run` / `Factory-Stage` / `Agent` trailers and
per-stage `agent/*.json` (cost, turns, session id). `main` is branch-protected (1 review, code
owners); a human merges. Those runs used `srt`; the production pieces (`deploy/islo/*`,
`swfactory doctor`, the webhook receiver, knowledge items) are exercised by `evals-islo`, which
reruns the demo issue in an islo MicroVM weekly and whenever the institutional knowledge changes,
with no Anthropic key on the runner — the RunReport it asserts on is the same `report.json`.

> **Why the two factory PRs fail this repo's CI while `main` is green:** their branches already
> contain the agent's real `percent_change` in `demo/target`, so the keyless demo's recorded
> fixtures (`demo/scripted/build.1.patch`, recorded against the baseline target) no longer apply.
> That is the intended signal: merging a factory PR that changes the demo target means re-recording
> the fixtures (`swfactory run --record demo/scripted ...`) in the same PR, or closing the PR and
> keeping it as the audit record. Either is a human decision; the factory never merges.

**Live human gate (real Airflow API):** `airflow standalone` + `POST /api/v2/dags/factory/dagRuns`,
then `swfactory approve <run> intent` / `plan` as user `admin`: all 14 tasks succeeded and the
committed `approvals.json` records actor `admin` for both gates (from Airflow's HITL
`responded_by_user`). New DAGs start paused in standalone; unpause once via the UI or
`PATCH /api/v2/dags/factory {"is_paused": false}`.

### Sandbox safety

The factory only ever removes sandboxes it can prove are its own: every `islo rm` is preceded by a
plain `islo ls` (own scope, never `--all`), the name must match the factory pattern
`swf-<slug>-<run8>`, and `created_by` must equal `SWF_SANDBOX_OWNER` when set. The nightly sweep
refuses to run without an owner. Teammates' sandboxes are never touched.

### Known limits / accepted risks

- `Bash(uv run *)` is arbitrary code execution inside the sandbox, by design: the agent has to run
  the target's tests. The sandbox **is** the trust boundary (islo MicroVM in production, srt on a
  dev box); the deny rules and the hook shape what the agent does, they do not contain it.
- `scrub_env` is a prefix denylist (`ANTHROPIC_*`, `GH_TOKEN`, `GITHUB_TOKEN`, `AWS_*`,
  `ISLO_API*`), so a credential under another name in the orchestrator's environment reaches a
  `local`/`srt` child. Pass secrets through the islo gateway (phantom tokens), not the environment.
- Issue text is untrusted input to every prompt (intent.md is quoted verbatim). Mitigations are
  structural rather than textual: the native deny rules, `validate_patch` + the secret scan before
  publish, `allowed_prefixes` confinement, the human gates, and a PR instead of a merge.
- Python's bytecode cache invalidates on mtime seconds + size: a fix that changes a source file to
  the same byte length within the same second as the previous test run can be masked by a stale
  `__pycache__` entry. Real agent edits practically never hit this; the scripted fixtures avoid it.
- `srt` is defense-in-depth, not the production trust boundary (see "Honest limits" above).

## Design decisions

- Blueprints are data in the **factory** repo, never in the target: gates, budgets and tool policy
  must not be agent-editable. Stage semantics stay Python (`stages.py`); a TOML file only chooses
  the walk, the knobs and the targets. Dynamic task mapping fans out over jobs and nothing else —
  nested expansion (e.g. review lenses) is unsupported in Airflow 3.3.1 and loops stay inside stage
  functions.
- Airflow, not islo Factory lines, is the spine: the human gates need an approver identity in the
  audit trail, and the HITL response carries `responded_by_user`. `GateOperator` subclasses
  `ApprovalOperator` only to stop it skipping its own child on Reject, so the refusal can be
  recorded and delivered. One state machine, no DAG cycles.
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
