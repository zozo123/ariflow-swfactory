# herd — the factory's driver's seat

`herd` is one terminal screen over the whole software factory — and, behind two flags, the same
screen without a terminal. It shows the human gates waiting for an answer, every job of every
Airflow run, the factory PRs, the sandboxes this operator owns and the aggregate metrics, and it
drives the line: answer a gate, trigger a blueprint, stop a run, remove your own sandbox.

It is built with [Textual](https://textual.textualize.io) on top of `swfactory.control`, which is
the only code that talks to Airflow (`/api/v2`), GitHub (`gh`) and islo (`islo`). `herd` itself is
presentation and key maps: it reads a `Snapshot` from a collector and mutates through an `Actions`
object, so the whole screen — and both headless flags — are unit-tested with fakes and no network.

```sh
uv run swfactory herd                                   # the TUI
uv run swfactory herd --once --json                      # one snapshot, exit 0, no TUI
uv run swfactory herd --approve-all                      # answer every pending gate, then exit
```

`swfactory.herd.main(...)` is that whole command (TUI and headless), `make_clients(...)` builds
the one client stack both use, `make_app(*, airflow_url, repo, owner, token=None, username=None,
password=None, metrics_root=".", refresh_s=5.0, dag_ids=None) -> HerdApp` returns the app over the
real clients, and `run_herd(collector, actions, *, info=None, refresh_s=5.0)` runs any
collector/actions pair (the tests use fakes).

## The unit is the job, not the run

`dags/blueprints.py` expands the `job` task group over `fan_out` — one job per (issue x target) —
so a gate, a sandbox and a PR all belong to one `(run_id, map_index)` pair. `herd` shows that
unit: **one Runs row per job**, and every gate names the job it belongs to.
`AirflowClient.job_rows(dag, run)` builds those rows from two bounded reads —
`GET .../taskInstances` grouped by `map_index`, plus `fan_out`'s `return_value` XCom for the issue.
No XCom (a run that has not fanned out yet, or an API user who cannot read XComs) is not an error:
the issue falls back to the run's `conf` when that is unambiguous (one issue x N targets gives
every job the same issue) and otherwise reads `-`.

## What it shows

```
 repo zozo123/ariflow-swfactory  ·  airflow http://localhost:8080  ·  owner me@x.io  ·  refreshed 12:00:05  ·  [ github ]
┌ Gates (1) ┬ Runs (3) ┬ PRs (1) ┬ Sandboxes (1) ┬ Metrics ┐
│ dag      run_id                            job  issue  stage             state    │
│ factory  manual__2026-09-03T08:04:02+00:00  0    42     build_and_test    running  │
│ factory  manual__2026-09-03T08:04:02+00:00  1    43     approve_plan      running  │
│ hotfix   manual__2026-09-02T11:00:00+00:00  0    9      intent:failed     failed   │
├───────────────────────────────────────────────────────────────────────────────────┤
│ 12:00:05 github: gh: not logged in                                                │
│ 12:00:41 -> approve factory/manual__...[1] approve_plan                           │
│ 12:00:42 ok approve factory/manual__...[1] approve_plan                           │
├───────────────────────────────────────────────────────────────────────────────────┤
│ actions are recorded as admin (the user the Airflow token belongs to)             │
│ t trigger  s stop  o open  q quit  ? help                                        │
└───────────────────────────────────────────────────────────────────────────────────┘
```

| Tab | Rows | Source |
| --- | --- | --- |
| **Gates** | every unanswered HITL gate across all DAGs: dag, run, gate (`approve_intent` / `approve_plan`), job index, that job's issue, subject, age | `GET /api/v2/dags/~/dagRuns/~/hitlDetails?response_received=false` |
| **Runs** | one row **per job**: dag, run, `map_index`, issue, current stage (`build_and_test`, `intent:failed`), rolled-up state | `GET /dags/{dag}/dagRuns` + `job_rows` (task instances and `fan_out`'s XCom) |
| **PRs** | PRs labelled `factory` in the target repo with their check rollup (`2 pass / 0 fail / 1 pending`) | `gh pr list --json ...` |
| **Sandboxes** | islo sandboxes **created by the configured owner** — nobody else's ever appear | plain `islo ls --output json`, filtered by `created_by` |
| **Metrics** | `swfactory metrics` table (first-pass rate, iterations, p50 cycle, findings, cost) over `metrics_root` | `**/docs/factory/*/metrics.json` |

Per-job rows cost two reads per run, so `collect` fetches them for every active run plus the
`jobs_per_dag` (default 5) newest runs of each DAG. Older runs — and any run whose read failed —
still contribute exactly one collapsed row (`job` `-`, the run's own state), so the table never
lies by omission. The header carries the repo, the Airflow URL, the owner and the time of the last
refresh; a source that fails (Airflow down, `gh` not logged in, no `islo` binary) shows up as a red
badge there and one line in the log pane while the other tabs keep working. Collection runs in a
worker thread every `refresh_s` seconds (and on `r`), so the screen never blocks on the network.

## Keys

| Where | Key | Does |
| --- | --- | --- |
| anywhere | `r` / `f5` | refresh now (`f5` only on the Gates tab, where `r` rejects) |
| anywhere | `q` | quit |
| anywhere | `?` | show this key map |
| Gates | `a` | approve the selected gate — right `map_index`, asks `y`/`n` |
| Gates | `r` | reject the selected gate (asks `y`/`n`) |
| Runs | `t` | trigger: pick a blueprint (`1`-`9` / arrows + `enter`), then a comma-separated list of issue numbers or `.md` paths |
| Runs | `s` | stop (fail) the selected job's **whole run** (asks `y`/`n`) |
| Runs | `o` | open that run in the Airflow UI |
| PRs | `o` | open the PR in the browser (`gh pr view --web`) |
| Sandboxes | `x` | remove the selected sandbox (asks `y`/`n`; own factory sandboxes only) |

`t` is blueprint-aware: the choices are the `blueprints/*.toml` names (which *are* the DAG ids,
via `swfactory.blueprint.blueprint_paths`), the row under the cursor only preselects one, and the
run is posted to the DAG that was picked — `AirflowClient.trigger(dag_id, issues)` is still the
single call that starts a line. The picker is skipped when the factory ships a single blueprint.
The new run appears immediately as an optimistic row (`job` `-`, state `queued`) with its Airflow
UI link in the log pane and a toast; the row is replaced by the real jobs on the next poll.

Every mutation asks a one-line confirmation, is written to the log pane (`-> ... / ok ... /
refused ... / failed ...`), and triggers a refresh when it succeeds. A failing action is a
notification, never a crash.

## Headless drive mode (CI and scripts)

Both flags reuse the same clients, collector and actions as the TUI — there is no second code
path, which is the point: what CI exercises is what the keys do.

| Flag | Does | Exit code |
| --- | --- | --- |
| `--once` | collect once, print a short text digest (runs, their jobs, gates, errors), exit | 0 |
| `--json` | the same single snapshot as JSON — implies `--once` | 0 |
| `--approve-all` | answer **every** pending gate of the configured blueprints, echoing one line per answer (`approve factory/manual__…[1] approve_plan as admin`) | 1 if any answer failed (or the gate list could not be read), else 0 |
| `--approve-all --reject` | the same, rejecting instead of approving | as above |

The JSON is one object with the keys `collected_at`, `runs` (each with its `jobs`: `map_index`,
`issue`, `stage`, `state`), `gates`, `prs`, `sandboxes`, `metrics` and `errors`. A failing source
fills `errors` rather than the exit code — `--once` is a read and always exits 0, so a CI step can
snapshot the factory without becoming a health check.

`--approve-all` is what proves the action layer against a real Airflow: stand up `airflow
standalone`, trigger a run, then

```sh
AIRFLOW_USER=admin AIRFLOW_PASSWORD=... uv run swfactory herd --approve-all
```

answers both gates of every job and prints who answered. Note that `airflow dags test` /
`dag.test()` never resolves HITL tasks (use `--mark-success-pattern 'job\.approve_.*'` there);
answering a gate for real needs the REST API, which is exactly what this flag drives.

## Who acts

Gate answers go out as `PATCH .../taskInstances/{task_id}/{map_index}/hitlDetails` with
`{"chosen_options": ["Approve"|"Reject"], "params_input": {}}`. Airflow records the responder
from the API credential (`responded_by_user`), and `record_<gate>` writes that identity into the
committed `approvals.json`. **The actor is therefore the Airflow API user the token belongs to**
(or the `username` exchanged for a token at `POST /auth/token`) — never a name typed into the TUI,
and never the `as <actor>` string `--approve-all` echoes, which is only there so the operator can
see whose token is answering. The footer says so on every screen. Use a personal token; a shared
token makes every approval look like the same person.

## Sandbox safety rule

`herd` shows and removes only sandboxes it can prove are the operator's own:

1. listing is plain `islo ls --output json` — **never** `--all`;
2. only entries whose `created_by` equals the configured `owner` (case-insensitive) are shown;
3. `x` refuses (`PermissionError`, shown as a warning toast) unless the name matches the factory
   pattern `swf-<slug>-<run8>` (`swfactory.maintain.SANDBOX_NAME_RE`) **and** a fresh listing
   taken right before the `islo rm` still says `created_by == owner`
   (`swfactory.sandbox.owns_sandbox`).

Without an `owner` the Sandboxes tab is empty and every removal is refused. Teammates' sandboxes
are never touched — the same rule the nightly `maintain` sweep follows.
