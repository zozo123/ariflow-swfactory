# swfactory

An AI-native software factory: a GitHub issue goes in, a reviewed pull request with a committed
artifact chain (intent, spec, plan, review, approvals, metrics) comes out. Airflow 3 runs one
linear pipeline with two human gates; Claude Code does the stage work inside an islo MicroVM that
holds no credentials; the orchestrator alone talks to GitHub, and a human merges. The same stage
functions run from the `swfactory` CLI and from the Airflow DAG, and a keyless scripted replay of a
recorded run doubles as the end-to-end test.

```
 GitHub issue --label factory--> dispatch.yml --POST /api/v2/dags/factory/dagRuns--> Airflow 3
                                                                                        |
  ORCHESTRATOR (trusted: Airflow worker or your shell)          holds ISLO_API_KEY, GH_TOKEN
  +---------------------------------------------------------------------------------------+
  | dags/factory.py == swfactory.stages.PIPELINE                                          |
  |  setup > intent > [approve_intent] > spec > plan > [approve_plan] > build_and_test     |
  |        > review > deliver > metrics ; teardown            [..] = ApprovalOperator /    |
  |                                                                  typer.confirm         |
  |  scm.py (gh, git am, push factory/<issue>-<run>, gh pr create)  <-- format-patch ----+ |
  +-------------------------------------------------------------------------------------|-+
        | islo use swf-<issue>-<run> --source github://<repo>:main --gateway-profile ... |
  ======|===================== trust boundary (no --env, no tokens) ====================|===
        v                                                                               |
  ISLO SANDBOX (untrusted compute)                                                      |
  +-------------------------------------------------------------------------------------|-+
  | clone of the target  ->  claude -p (per-stage --allowedTools, --max-turns, budget)   | |
  | .claude/hooks/swf_guard.py denies protected paths + git push/commit/gh pr/curl/wget  | |
  | tests run here (or via `crabbox run` on the LOCAL path)  -> bot-authored commits ----+ |
  | gateway: deny-by-default; ANTHROPIC_API_KEY is a phantom token swapped on egress only  |
  +---------------------------------------------------------------------------------------+
```

## Quickstart (no keys, no network, ~5 s)

```sh
uv sync
uv run pytest            # 94 passed
uv run swfactory demo    # scripted replay of a recorded run on demo/target
```

The demo copies `demo/target` to `.factory/<run>/work`, runs every pipeline stage with fixtures
from `demo/scripted`, and "publishes" to a bare git remote at `.factory/<run>/remote.git`,
printing the would-be PR. The build loop really loops: `build.1.patch` gets the sign wrong,
`fix.2.patch` fixes it. The tail of the output is the `RunReport` table:

```
run                    ac2ca241
issue                  DEMO-1
agent / sandbox / scm  scripted / local:work / local
stages                 intent:ok → spec:ok → plan:ok → build_and_test:ok → review:ok → deliver:ok
approvals              intent=approve by auto, plan=approve by auto
tests passed           True
pr                     file:///.../.factory/ac2ca241/pr.md
cost usd               0.0000
  plan                 files=3
  build_and_test       iterations=2, first_pass_ci=0, tests_passed=1, tests_failed=0, tests_count=7
  review               blockers=0, findings=4, dropped_nits=1, fixes=0, blocker=0, major=1, minor=0, nit=3
  deliver              blockers=0, commits=3
```

Use `--approve prompt` to answer the two gates yourself. Exit code is 1 if any stage is blocked or
tests did not pass.

## Artifact chain and human gates

Everything is committed under `docs/factory/<issue>/` in the target, by `swfactory-bot`, with
`Factory-Run` / `Factory-Stage` / `Agent` commit trailers.

| Playbook stage | Artifact | Who approves | Where it runs |
| --- | --- | --- | --- |
| Intent | `intent.md` (issue body verbatim + front matter) | human gate 1 (Airflow `approve_intent` / CLI confirm) -> `approvals.json` | orchestrator, no agent |
| Spec | `spec.md` | - | agent, read-only tools, in sandbox |
| Plan | `plan.json` (typed) + `plan.md` | human gate 2 (`approve_plan`) -> `approvals.json` | agent, read-only tools, in sandbox |
| Build + test | bot commits, `agent/build.1.json`, `agent/fix.N.json`, junit | none; bounded by `max_build_iterations` (3) | agent with Edit/Write/`uv run`, tests in sandbox |
| Review | `review.json` (REVIEW.md contract, nit cap 3, plan fidelity checked in code) | none; one fix + re-review max (`max_review_fixes`) | agent, read-only, in sandbox |
| Deliver | `metrics.json`, PR labeled `factory`, `agent-authored` (+ `factory:blocked`) | human merges: CODEOWNERS + branch protection | orchestrator (`git am`, push, `gh pr create`) |

`Scm` has no merge method and the sandbox has nothing to push with; `deliver` pulls
`git format-patch <base>..HEAD` out and applies it on the orchestrator. Every stage is idempotent
(status `skipped` when its artifact exists) so Airflow retries and `tasks clear` are safe.

## Real run

Prerequisites on the orchestrator: `uv`, `git`, `gh`, `islo` (0.48+), a GitHub token for
`swfactory-bot` in `GH_TOKEN`. No `ANTHROPIC_API_KEY` on the host: it lives in the islo
environment and reaches the sandbox only as a phantom token.

One-time bootstrap:

```sh
islo login                       # islo account
islo login --tool github         # lets `--source github://...` clone the target
islo login --tool claude         # Claude Code inside the sandbox image
islo gateway create --name swfactory --default-action deny --internet-access true
#   then allow these hosts on the profile (islo console; the 0.48 CLI has no rule subcommand):
#   api.anthropic.com  github.com  pypi.org  files.pythonhosted.org  astral.sh
islo environment create --name swfactory \
  --gateway-secret 'ANTHROPIC_API_KEY=<real key>;host=api.anthropic.com;auth=bearer'
#   never --secret for this key: only --gateway-secret keeps the real value out of the VM

export GH_TOKEN=<fine-grained PAT for swfactory-bot: contents+pull_requests write, issues write>
gh api -X PUT repos/zozo123/ariflow-swfactory/branches/main/protection --input - <<'JSON'
{"required_status_checks":{"strict":true,"contexts":["test"]},"enforce_admins":false,
 "required_pull_request_reviews":{"require_code_owner_reviews":true,"required_approving_review_count":1},
 "restrictions":null}
JSON
```

`.github/CODEOWNERS` (`* @zozo123`) makes the human the required reviewer of every factory PR.
Then:

```sh
uv run swfactory demo --real     # = run --issue demo/issue.md --agent claude --sandbox islo --scm github --approve prompt
uv run swfactory run --issue 42 --agent claude --sandbox islo --scm github   # a real issue
```

This creates `swf-demo-1-<run>` from `github://zozo123/ariflow-swfactory:main`, runs `claude -p`
per stage inside it, asks you to approve intent and plan in the terminal, and opens a PR from
`factory/DEMO-1-<run>`. Add `--record demo/scripted` to rewrite the demo fixtures from the real
agent outputs. `--tests crabbox` is only valid with `--sandbox local`.

Dev escape hatch, honestly: `--agent claude --sandbox local --allow-local-agent` runs the real
agent and its code on your machine in `.factory/<run>/work`. The guard hook is still installed,
but model-written code executes on the host, and `LocalSandbox` scrubs `ANTHROPIC_*` from the
child environment, so `claude` must be logged in on the host (not driven by an API-key env var).
The `Config` validator refuses `agent=claude` + `sandbox=local` without this flag.

## Airflow

```sh
uv sync --group airflow                              # apache-airflow 3.3.1 + standard provider
export AIRFLOW_HOME=$PWD/airflow_home                # gitignored
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags AIRFLOW__CORE__LOAD_EXAMPLES=False
uv run airflow standalone                            # UI on http://localhost:8080
```

Trigger: label an issue `factory` (dispatch.yml POSTs `/api/v2/dags/factory/dagRuns` with the
`AIRFLOW_URL` / `AIRFLOW_TOKEN` repo secrets), or
`uv run airflow dags trigger factory --conf '{"issue": "42"}'`. Approve gates in the UI
(Required Actions) or from the shell:

```sh
export AIRFLOW_TOKEN=$(curl -s -X POST localhost:8080/auth/token -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"<airflow_home/simple_auth_manager_passwords.json.generated>"}' | jq -r .access_token)
uv run swfactory approve <dag_run_id> intent          # or: plan, --reject
```

Config reaches the worker as `SWF_*` env vars (`SWF_AGENT`, `SWF_SANDBOX`, `SWF_SCM`, ...).
`SWF_APPROVE=auto` makes both gates default to Approve after `SWF_AUTO_GATE_S` (10 s), which is
how `airflow dags test factory --conf '{"issue":"demo/issue.md"}'` completes.
`tests/test_dag_parity.py` asserts the DAG's task ids equal `PIPELINE` and the gates are
`ApprovalOperator`s with a response timeout:
`uv run --group airflow pytest tests/test_dag_parity.py`.

## crabbox

crabbox is a command wrapper, not a sandbox. With `--tests crabbox` (local sandbox only)
`run_tests` wraps the target's test command in
`crabbox run -provider <provider> -junit .factory/junit.xml -artifact-glob .factory/junit.xml -ttl 45m -idle-timeout 15m -- <cmd>`
and reads the junit file crabbox brings back. Never `-allow-env`, `-env-from-profile` or `-keep`.
The human inner loop for this repo is the same idea: `crabbox run --provider islo -- uv run pytest`
(`.crabbox.yaml` excludes `.venv`, `.factory`, `airflow_home`, `.env*`). Try the wrapper with
`uv run swfactory demo --tests crabbox --crabbox-provider local-container`.

## Maintain

`bands.yaml` defines a window (20 runs) and three response tiers over `docs/factory/*/metrics.json`
of a checkout: 1σ `log`, 2σ `diagnose` (read-only agent, `Diagnosis` schema, incident record at
`docs/factory/incidents/<date>-<metric>.md`), 3σ `propose` (drafted intent + `gh issue create
--label factory`, which re-enters the factory). Detection is `statistics.mean`/`stdev`, needs at
least 3 history samples, and ignores `agent=scripted` runs so the demo never moves the bands.

```sh
uv run swfactory metrics --root .       # first-pass rate, mean iterations, p50 cycle, findings, cost
```

`dags/maintain.py` runs `maintain.run()` daily and then sweeps orphan `swf-*` sandboxes older than
`SWF_SANDBOX_TTL_S`. There is no `swfactory maintain` CLI verb; call `swfactory.maintain.run`
from Python or the DAG.

## Layout

```
src/swfactory/config.py      Config (SWF_* env / flags), TargetContract from factory.toml
src/swfactory/models.py      Issue, Plan, Review, Finding, Approval, StageResult, RunReport, StageError
src/swfactory/sandbox.py     Sandbox protocol, LocalSandbox (env scrub), IsloSandbox (islo use/cp/rm)
src/swfactory/agent.py       Agent protocol, POLICIES, ClaudeAgent, ScriptedAgent, install_guard
src/swfactory/scm.py         Scm protocol, LocalGitScm (bare remote + pr.md), GitHubScm (gh)
src/swfactory/stages.py      Ctx, PIPELINE, stage functions, run_tests, commit, run_pipeline
src/swfactory/metrics.py     write_run_metrics, load_all, summarize, table
src/swfactory/maintain.py    load_runs, detect, run, sweep_sandboxes
src/swfactory/cli.py         typer: run, demo, metrics, approve
src/swfactory/prompts/*.md   spec, plan, build, fix, review, diagnose templates
dags/factory.py              linear DAG with ApprovalOperator gates, setup/teardown
dags/maintain.py             @daily band check + sandbox sweep
.claude/hooks/swf_guard.py   PreToolUse guard installed into every target before write stages
.claude/skills/swfactory/    spec/plan shape + review contract for the agent
REVIEW.md  bands.yaml        review policy; maintain tiers
islo.yaml  .crabbox.yaml     sandbox setup (uv only); crabbox profile
demo/issue.md                DEMO-1, a front-matter issue
demo/target/                 `calc` package + factory.toml (the target contract)
demo/scripted/               recorded fixtures replayed by `swfactory demo`
tests/                       hermetic: fake subprocess, tmp git repos, no network
.github/workflows/           ci (ruff, pytest, demo), dispatch (issue -> DAG), evals (weekly real run)
```

## Design decisions

- Airflow, not islo Factory lines, is the spine: the two human gates need an approver identity in
  the audit trail, and `ApprovalOperator` records `responded_by_user`. One state machine, no DAG cycles.
- Delivery is a `git format-patch` stream applied on the orchestrator: the sandbox never holds a
  GitHub credential, so "the agent never pushes or merges" is structural, not a prompt.
- Scripted fixtures are recordings (`--record`), not hand-written theater; `metrics.json` marks
  them `agent=scripted` and a banner says so in the terminal and the PR body.
- Python + uv, stdlib where possible (`subprocess`, `tomllib`, `statistics`, `xml.etree`): the
  factory is glue around `claude`, `islo`, `gh`, `git`; a Rust CLI would buy nothing here.
- Every loop is bounded (`max_build_iterations`, `max_review_fixes`, `--max-turns`, per-stage and
  per-run USD ceilings). Exhaustion is `StageError(kind="policy")` or a `factory:blocked` PR.

## License

Apache-2.0
