# swfactory

[![CI](https://github.com/zozo123/ariflow-swfactory/actions/workflows/ci.yml/badge.svg)](https://github.com/zozo123/ariflow-swfactory/actions/workflows/ci.yml)
[![Airflow 3.3.1](https://img.shields.io/badge/Airflow-3.3.1-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/docs/apache-airflow/3.3.1/)
[![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/downloads/release/python-3120/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-D22128)](LICENSE)
[![Project site](https://img.shields.io/badge/Project_site-Live-61DBFF)](https://zozo123.github.io/ariflow-swfactory/)

An AI-native software factory: a GitHub issue goes in, a reviewed pull request with a committed
artifact chain (intent, spec, plan, review, approvals, metrics) comes out. A **blueprint**
(`blueprints/<name>.toml`) declares the line — stage order, human gates, limits, target repos,
sandbox profile, PR labels — and becomes one Airflow 3 DAG and one `swfactory run --blueprint`
command. Claude Code does the stage work inside a sandbox that holds no GitHub credential; the
orchestrator alone talks to GitHub, and a human merges. A keyless scripted replay of a recorded run
doubles as the end-to-end test. Overview and diagrams: the
[project site](https://zozo123.github.io/ariflow-swfactory/).

```
 GitHub issue --label factory[:<name>]--> webhook/dispatch --POST /api/v2/dags/<name>/dagRuns--> Airflow 3
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
  SANDBOX (untrusted compute: islo MicroVM, srt-confined directory, or docker container)      |
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
uv run pytest            # 404 tests without the airflow group, 438 with it, hermetic: fake subprocess, tmp git repos, no network
uv run swfactory demo    # scripted replay of a recorded run on demo/target
```

The demo copies `demo/target` to `.factory/<run>/work`, runs every stage of the default blueprint
with fixtures from `demo/scripted`, and "publishes" to a bare git remote at
`.factory/<run>/remote.git`, printing the would-be PR. The build loop really loops:
`build.1.patch` gets the sign wrong, `fix.2.patch` fixes it. The tail of the output is the
`RunReport`:

```
run                    21543040
issue                  DEMO-1
agent / sandbox / scm  scripted / local:work / local
stages                 intent:ok → spec:ok → plan:ok → build_and_test:ok → review:ok → deliver:ok
approvals              intent=approve by auto, plan=approve by auto
tests passed           True
pr                     file:///.../.factory/21543040/pr.md
cost usd               0.0000
  plan                 files=3
  build_and_test       iterations=2, first_pass_ci=0, tests_passed=1, tests_failed=0, tests_count=7
  review               blockers=0, findings=4, dropped_nits=1, fixes=0, blocker=0, major=1, minor=0, nit=3
  deliver              blockers=0, commits=3, rejected=0, denied_tool_calls=0
```

Use `--approve prompt` to answer the two gates yourself. Exit code is 1 if any job is blocked or
its tests did not pass. Four lines ship: `blueprints/default.toml` (DAG id and CLI name
`factory`), `blueprints/hotfix.toml` (no `spec` stage, self-approving intent gate, extra `hotfix`
label), `blueprints/stress.toml` (two targets, `max_parallel_jobs = 2`, the fan-out harness behind
`scripts/stress_airflow.sh`) and `blueprints/toolset.toml` (the default order on Airflow's own
`SandboxBackend`, via `--sandbox toolset`) — zero Python between them. One issue applied to N
`[[targets]]` is N jobs, each with its own sandbox, PR and approvals; `SWF_*` env vars override
blueprint values and CLI flags alike.

```sh
uv run swfactory run --blueprint hotfix --issue demo/issue.md --approve auto     # scripted, local
uv run swfactory run --issue 42 --agent claude --sandbox srt --scm github --approve prompt
```

Blueprint schema, the generated DAG and how to run Airflow locally: [docs/design.md](docs/design.md).

## Artifact chain and human gates

Everything is committed under `docs/factory/<issue>/` in the target, by `swfactory-bot`, with
`Factory-Run` / `Factory-Stage` / `Agent` commit trailers.

| Stage | Artifact | Who approves | Where it runs |
| --- | --- | --- | --- |
| Intent | `intent.md` (issue body verbatim + front matter) | gate 1 (Airflow `job.approve_intent` / CLI confirm) -> `approvals.json` | orchestrator, no agent |
| Spec | `spec.md` | - | agent, read-only tools, in sandbox |
| Plan | `plan.json` (typed) + `plan.md` | gate 2 (`job.approve_plan`) -> `approvals.json` | agent, read-only tools, in sandbox |
| Build + test | bot commits, `agent/build.1.json`, `agent/fix.N.json`, junit | none; bounded by `max_build_iterations` | agent with Edit/Write/`uv run`, tests in sandbox |
| Review | `review.json` (REVIEW.md contract, nit cap, plan fidelity — both halves — checked in code) | none; `max_review_fixes` fix + test + re-review rounds (default 1); a fix that leaves the suite red is itself a blocker | review: agent, read-only; fix: agent with Edit/Write, tests dir protected |
| Deliver | `metrics.json` (incl. `blueprint`, `denied_tool_calls`), `agent/stages.jsonl` + `agent/hooks.jsonl` audit copies, PR labeled per blueprint (+ `factory:blocked` / `factory:rejected`) | human merges: CODEOWNERS + branch protection | orchestrator (`git am`, push, `gh pr create`) |

`Scm` has no merge method and the sandbox has nothing to push with: `deliver` pulls
`git format-patch <base>..HEAD` out and applies it on the orchestrator, after a policy check
(`scm.validate_patch`: no absolute paths, no `..`, nothing under `.git/`, no symlink modes, every
path under the target dir or `docs/factory/`) and a secret scan. A hit is `StageError("policy")`
and nothing is published. Delivery is retry-safe: `factory/*` is the bot-owned branch namespace,
force-updated on a retried `deliver`, and an open PR for the branch is edited in place.

Run state lives on the **orchestrator**: every `StageResult` is appended to
`.factory/<run_id>/stages.jsonl` (a copy goes into the sandbox for the audit trail, never trusted).
A stage is `skipped` only when that log holds a completed record for it — an artifact the agent
forged in the sandbox cannot skip a stage — and `budget_usd` is seeded from the same log, so it is
a ceiling per run even when Airflow splits the run over tasks and workers. A rejected gate is part
of the chain, not a dead end: the refusal and its actor land in `approvals.json`, the work stages
skip, and `deliver` still publishes a `[REJECTED]` PR labeled `factory:rejected` with
`status="blocked"` and exit code 1.

## CLI

| Command | Does |
| --- | --- |
| `swfactory run --blueprint <name> --issue <n\|path> ...` | run one line over issues x targets: `--agent claude\|scripted`, `--sandbox local\|srt\|docker\|islo`, `--scm local\|github`, `--approve auto\|prompt`, `--record <dir>` |
| `swfactory demo [--real] [--sandbox srt\|docker]` | the default line on `demo/issue.md`: keyless scripted replay, or `--real` = claude in an islo sandbox against a real repo |
| `swfactory doctor [--json] [--blueprint <name>]` | preflight the real path: islo/gh/claude/srt CLIs, `islo login` + tool integrations, gateway profile, environment, snapshot, `gh auth` + repo, blueprint, `factory.toml`; exit 1 on a required red row, each with its `fix:` |
| `swfactory herd [--airflow-url ...] [--owner ...]` | control-room TUI: pending gates (approve/reject), runs and their jobs, factory PRs, your own sandboxes, metrics; `t` triggers any blueprint ([docs/herd.md](docs/herd.md)) |
| `swfactory herd --once [--json]` / `--approve-all [--reject]` | the same clients headless: one snapshot (per-job rows, gates, PRs, sandboxes, metrics) for CI, or answer every pending gate; exit 1 if an answer failed |
| `swfactory webhook serve [--port 8081] [--airflow-url ...]` | GitHub issue/comment receiver on the orchestrator -> `POST /api/v2/dags/<name>/dagRuns`; `GET /healthz` |
| `swfactory webhook route <event> <payload.json>` | dry run: print the DAG run a payload would trigger, exit 1 if it would be ignored |
| `swfactory metrics --root <dir>` | summarise committed runs: first-pass rate, mean iterations, p50 cycle, findings, cost |
| `python -m swfactory.evals [--suite demo/evals] [--agent claude] [--update-baseline]` | run the eval suite — issues with a machine-checkable expected outcome — and fail on any regression against `demo/evals/baseline.json` ([docs/evals.md](docs/evals.md)) |
| `swfactory maintain --root <dir> [--sweep-ttl-s N]` | band check per `bands.yaml` (log / diagnose / propose) + orphan `swf-*` sandbox sweep |
| `swfactory approve <dag_run_id> intent\|plan [--reject]` | answer a running DAG's gate through the Airflow HITL API (`--blueprint`, `--map-index`) |

## Sandboxes

`--sandbox` picks where the agent, its edits and the target's tests run. The agent never holds a
GitHub credential in any of them.

| kind | Isolation | Agent's credentials | Needs | Honest limit |
| --- | --- | --- | --- | --- |
| `local` | none (`scrub_env` only) | none — `agent=claude` refused unless `--allow-local-agent` | nothing | not a boundary at all; demo, pytest, CI |
| `srt` | OS-level (macOS Seatbelt / Linux bubblewrap): writes limited to the workdir + caches, `factory.toml` `protected` globs as kernel `denyWrite` per stage, egress domain allowlist, `~/.ssh` `~/.aws` `~/.config` `~/.gnupg` `~/.netrc` `~/.docker` `~/.kube` unreadable | the real `ANTHROPIC_API_KEY` (or the host's Claude login) | `srt` or `npx` (`@anthropic-ai/sandbox-runtime`); Linux: bubblewrap + socat | shares the host kernel and holds a real key; tools that ignore `HTTP_PROXY` bypass the allowlist. Defense-in-depth for your own machine, not the production boundary |
| `docker` | Linux container per command (`docker run --rm`): workdir bind-mounted rw at its own path, protected prefixes and `.claude`/`.github` `:ro` per stage, `--network bridge\|none` | `ANTHROPIC_API_KEY` by `-e NAME`, or your `~/.claude` login (`credentials=host`) | docker CLI + daemon; `deploy/docker/sandbox.Dockerfile` | shared kernel, `docker.sock` is root-equivalent, no phantom tokens, no domain allowlist. Testing deployment ([docs/docker.md](docs/docker.md)) |
| `islo` | Firecracker-class MicroVM, deny-by-default gateway | phantom `ANTHROPIC_API_KEY` swapped on egress; never a GitHub token | `islo login` + `--tool github/claude`, gateway profile + environment, `swfactory doctor` green | the production boundary; pause/resume and `--snapshot` exist only here ([docs/islo.md](docs/islo.md)) |
| `toolset` | Airflow's own `SandboxBackend` (provider `common.ai`) adapted behind our protocol | experimental; `sbx` released, islo/opensandbox/asciibox are pending upstream PRs — see [docs/design.md](docs/design.md) |

`budget_usd` is per job, not per run. Inside every sandbox the agent runs under
`.claude/settings.local.json` written by `install_guard`: native `permissions.deny` rules
(`Edit(REVIEW.md)`, `Edit(factory.toml)`, `Edit(.claude/**)`, `Edit(.github/**)`,
`Edit(docs/factory/**)`, `Edit(.factory/**)`, `Edit(<protected glob>)`, `Bash(git push*)`,
`Bash(gh pr *)`, `Bash(git commit*)`, `Bash(curl *)`, `Bash(wget *)`, `Read(.env*)`) are the
primary gate — the artifact chain and the stage scratch belong to the orchestrator, so the agent
cannot forge `review.json`, `approvals.json` or its own audit log. The PreToolUse hook
`.claude/hooks/swf_guard.py` is defense-in-depth and writes the `.factory/hooks.jsonl` audit log;
denied calls are counted as `denied_tool_calls` in `metrics.json` and the PR's deliver row.
`install_guard` also ships `.claude/skills/swfactory/` into the target, so the agent reads the same
spec/plan/review contract wherever its cwd is.

## Proof (real runs on this repo)

Two unattended runs of `swfactory run --issue 1 --agent claude --sandbox srt --scm github` against
[issue #1](https://github.com/zozo123/ariflow-swfactory/issues/1):

| run | outcome | what it shows |
|---|---|---|
| [PR #2](https://github.com/zozo123/ariflow-swfactory/pull/2) `factory:blocked` | build passed, review found **1 blocker** (no tests), 1 fix attempt could not resolve it | the factory ships a labeled, blocked PR instead of merging; the reviewer diagnosed the root cause (tests/ was deny-listed in the build stage — fixed in `e94ed37`) and caught a real spec error (R6 sign) |
| [PR #3](https://github.com/zozo123/ariflow-swfactory/pull/3) | 14 tests, first-pass CI, **0 findings**, $1.94 | the clean path: intent → spec → plan → build+test → review → PR, every artifact committed under `docs/factory/1/` |

Both PRs carry bot-authored commits with `Factory-Run` / `Factory-Stage` / `Agent` trailers and
per-stage `agent/*.json` (cost, turns, session id); `main` is branch-protected (1 review, code
owners) and a human merges. Those two branches fail this repo's CI on purpose: they contain the
agent's real `demo/target` change, so the recorded demo fixtures no longer apply — merging one
means re-recording them (`--record demo/scripted`) in the same PR.

**Live human gate (real Airflow API):** `airflow standalone` + `POST /api/v2/dags/factory/dagRuns`,
then `swfactory approve <run> intent` / `plan` as user `admin`: all 14 tasks succeeded and the
committed `approvals.json` records actor `admin` for both gates, from Airflow's HITL
`responded_by_user`. `scripts/stress_airflow.sh` is that run under fan-out: `stress.toml` over 2
issues x 2 targets, **53/53 task instances green** and 8 gates answered as `admin`, each of the 4
jobs with its own run id, workdir, remote and chain. `swfactory herd --approve-all` answers the
same gates through the TUI's own clients.

CI (`.github/workflows/`): `test` (ruff + pytest + demo) and `airflow-parity` (parity + smoke +
stress) are the required checks. Advisory: `srt-smoke`, `docker-smoke`, `airflow-main` (upstream
canary) and `airflow-main-sandbox-toolset`. `evals.yml` gates every
change to CLAUDE.md, a prompt, a blueprint or `.claude/**` on the keyless `eval-suite`, and runs
the real agent weekly — `real-demo` under srt, `evals-islo` in an islo MicroVM with no key on the
runner.

## Layout

```
blueprints/*.toml            default.toml = the `factory` line; hotfix, stress, toolset = 3 more
src/swfactory/blueprint.py   Blueprint models, load/loads/resolve, pipeline(), jobs(conf), config(job)
src/swfactory/config.py      Config (SWF_* env > init), TargetContract from factory.toml
src/swfactory/runtime.py     the one (blueprint, job, run id) -> Ctx assembly, CLI and DAG alike
src/swfactory/models.py      Issue, Plan, Review, Finding, Approval, StageResult (+preview), RunReport
src/swfactory/sandbox.py     Sandbox protocol, Local/Srt/Docker/IsloSandbox, make_sandbox
src/swfactory/agent.py       Agent protocol, POLICIES, ClaudeAgent, ScriptedAgent, install_guard
src/swfactory/scm.py         Scm protocol, LocalGitScm (bare remote + pr.md), GitHubScm (gh)
src/swfactory/stages.py      Ctx, CANONICAL_ORDER/STAGES, stage functions, run_tests, commit, run_pipeline
src/swfactory/metrics.py     write_run_metrics, load_all, summarize, table
src/swfactory/maintain.py    load_runs, detect, run, sweep_sandboxes
src/swfactory/control.py     Airflow /api/v2, gh and islo clients behind one Snapshot/Actions pair
src/swfactory/herd.py        the Textual TUI over control.py (presentation only)
src/swfactory/doctor.py      read-only preflight checks with a `fix:` per row
src/swfactory/webhook.py     stdlib GitHub -> Airflow receiver (route is pure and unit-tested)
src/swfactory/cli.py         typer app: the nine verbs in the CLI table above
src/swfactory/evals.py       eval suite over demo/evals/**: load_suite, check, score, baseline_diff
src/swfactory/prompts/*.md   spec, plan, build, fix, review, diagnose templates
dags/blueprints.py           one mapped-task-group DAG per blueprint (GateOperator, record_<stage>)
dags/maintain.py             nightly + after every delivery (AssetOrTimeSchedule) band check + sweep
.claude/hooks/swf_guard.py   PreToolUse audit hook installed into every target before write stages
.claude/skills/swfactory/    spec/plan shape + review contract for the agent
REVIEW.md  bands.yaml        review policy; maintain tiers
islo.yaml  .crabbox.yaml     agent sandbox setup (uv only); crabbox test-wrapper profile
deploy/islo/                 bootstrap.sh, deploy.sh, knowledge.sh, orchestrator/ (docs/islo.md)
deploy/docker/               compose.yml + Dockerfiles for the local stack (docs/docker.md)
demo/                        issue.md (DEMO-1), issue2.md (DEMO-2), target/, scripted/ fixtures
scripts/stress_airflow.sh    live `airflow standalone`: stress.toml over 2 issues x 2 targets, 8
                             gates answered as `admin` through the HITL API (docs/design.md)
tests/                       hermetic; test_dag_*.py need the airflow group
.github/workflows/           ci, dispatch (issue label -> Airflow), evals
```

## Docs

- [docs/design.md](docs/design.md) — blueprint schema, the generated DAG, Airflow locally, metrics
  and bands, crabbox, design decisions, known limits and accepted risks.
- [docs/islo.md](docs/islo.md) — production: the two-tier islo topology, bootstrap order, gateway /
  environment / snapshot, webhook wiring, `evals-islo`, knowledge items.
- [docs/docker.md](docs/docker.md) — the fully local Docker stack and its honest limits.
- [docs/herd.md](docs/herd.md) — the control-room TUI: tabs, keys, who acts, sandbox safety.
- [CLAUDE.md](CLAUDE.md) — the one-page contract every agent (and human) works under.
- [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), [CHANGELOG.md](CHANGELOG.md).

## License

Apache-2.0
