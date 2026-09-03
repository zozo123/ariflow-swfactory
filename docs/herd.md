# herd — the factory's management TUI

`herd` is one terminal screen over the whole software factory: the human gates waiting for an
answer, the Airflow runs of every blueprint line, the factory PRs, the sandboxes this operator
owns, and the aggregate metrics. It is built with [Textual](https://textual.textualize.io) on top
of `swfactory.control`, which is the only code that talks to Airflow (`/api/v2`), GitHub (`gh`)
and islo (`islo`). The TUI itself is pure presentation: it reads a `Snapshot` from a collector and
mutates through an `Actions` object, so the whole screen is unit-tested with fakes and no network.

```
$ uv run python -c "from swfactory.herd import make_app; make_app(airflow_url='http://localhost:8080', repo='zozo123/ariflow-swfactory', owner='me@x.io', token='...', username=None, password=None, metrics_root='.', refresh_s=5).run()"
```

`make_app(*, airflow_url, repo, owner, token=None, username=None, password=None, metrics_root=".",
refresh_s=5.0, dag_ids=None) -> HerdApp` wires the real clients; `run_herd(collector, actions,
*, info=None, refresh_s=5.0)` runs any collector/actions pair (the tests use fakes).

## What it shows

```
 repo zozo123/ariflow-swfactory  ·  airflow http://localhost:8080  ·  owner me@x.io  ·  refreshed 12:00:05  ·  [ github ]
┌ Gates (1) ┬ Runs (2) ┬ PRs (1) ┬ Sandboxes (1) ┬ Metrics ┐
│ dag      run                                   gate           map  subject                            age │
│ factory  manual__2026-09-03T08:04:02+00:00     approve_plan   0    [factory] approve plan.md for 42   5m  │
│                                                                                                          │
├──────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ 12:00:05 github: gh: not logged in                                                                       │
│ 12:00:41 -> approve factory/manual__...[0] approve_plan                                                  │
│ 12:00:42 ok approve factory/manual__...[0] approve_plan                                                  │
├──────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ actions are recorded as admin (the user the Airflow token belongs to)                                    │
│ a approve  r reject  q quit  ? help                                                                      │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

| Tab | Rows | Source |
| --- | --- | --- |
| **Gates** | every unanswered HITL gate across all DAGs: dag, run, gate (`approve_intent` / `approve_plan`), job index, subject, age | `GET /api/v2/dags/~/dagRuns/~/hitlDetails?response_received=false` |
| **Runs** | the last 20 runs of every `swfactory`-tagged DAG (one per `blueprints/*.toml`): state, start, and the current stage per job (`build_and_test`, `intent:failed`, `spec, approve_intent` for two jobs) | `GET /dags/{dag}/dagRuns`, task instances for active runs only |
| **PRs** | PRs labelled `factory` in the target repo with their check rollup (`2 pass / 0 fail / 1 pending`) | `gh pr list --json ...` |
| **Sandboxes** | islo sandboxes **created by the configured owner** — nobody else's ever appear | plain `islo ls --output json`, filtered by `created_by` |
| **Metrics** | `swfactory metrics` table (first-pass rate, iterations, p50 cycle, findings, cost) over `metrics_root` | `**/docs/factory/*/metrics.json` |

The header carries the repo, the Airflow URL, the owner and the time of the last refresh. A
source that fails (Airflow down, `gh` not logged in, no `islo` binary) shows up as a red badge in
the header and one line in the log pane; the other tabs keep working. Collection runs in a worker
thread every `refresh_s` seconds (and on `r`), so the screen never blocks on the network.

## Keys

| Where | Key | Does |
| --- | --- | --- |
| anywhere | `r` / `f5` | refresh now (`f5` only on the Gates tab, where `r` rejects) |
| anywhere | `q` | quit |
| anywhere | `?` | show this key map |
| Gates | `a` | approve the selected gate (asks `y`/`n`) |
| Gates | `r` | reject the selected gate (asks `y`/`n`) |
| Runs | `t` | trigger the selected run's DAG with a comma-separated list of issue ids |
| Runs | `s` | stop (fail) the selected run (asks `y`/`n`) |
| Runs | `o` | open the run in the Airflow UI |
| PRs | `o` | open the PR in the browser (`gh pr view --web`) |
| Sandboxes | `x` | remove the selected sandbox (asks `y`/`n`; own factory sandboxes only) |

Every mutation asks a one-line confirmation, is written to the log pane (`-> ... / ok ... /
refused ... / failed ...`), and triggers a refresh when it succeeds. A failing action is a
notification, never a crash.

## Who acts

Gate answers go out as `PATCH .../taskInstances/{task_id}/{map_index}/hitlDetails` with
`{"chosen_options": ["Approve"|"Reject"], "params_input": {}}`. Airflow records the responder
from the API credential (`responded_by_user`), and `record_<gate>` writes that identity into the
committed `approvals.json`. **The actor is therefore the Airflow API user the token belongs to**
(or the `username` exchanged for a token at `POST /auth/token`) — never a name typed into the TUI.
The footer says so on every screen. Use a personal token; a shared token makes every approval look
like the same person.

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
