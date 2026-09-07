from __future__ import annotations

from pathlib import Path


def replace(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    if old not in text:
        raise SystemExit(f"{path}: expected stale fragment not found: {old[:100]!r}")
    file.write_text(text.replace(old, new, 1))


replace("docs/factory-backend.md", "`src/swfactory/backend.py` is the network boundary.", "`src/swfactory/backend/` is the network boundary.")
replace(
    "docs/factory-backend.md",
    "The API always requires\n`Authorization: Bearer <SWF_BACKEND_TOKEN>`, including health.",
    "Only `GET /v1/liveness` and `GET /v1/readiness` are deliberately unauthenticated deployment probes.\nEvery factory-state/control API, including `GET /v1/health`, requires\n`Authorization: Bearer <SWF_BACKEND_TOKEN>`.",
)
replace(
    "docs/factory-backend.md",
    "uv run swfactory backend\n```",
    "uv run swfactory backend\n```\n\nBackend-submitted Cells also call the backend from the Airflow scheduler/worker process at every\nmanaged stage boundary. Give that process the same token plus a network-reachable backend URL:\n\n```sh\n# In the Airflow scheduler/worker environment, not in the agent sandbox:\nexport SWF_BACKEND_URL=http://factory-backend.internal:8082\nexport SWF_BACKEND_TOKEN=\"$CONTROL_PLANE_BACKEND_TOKEN\"\n```\n\nThe Docker `console` profile wires these variables into the `airflow` service automatically. They\nare control-plane credentials and must remain scrubbed from model-written work processes.",
)
old_table = """| Route | Request | Response |
| --- | --- | --- |
| `GET /v1/health` | none | service name and API version |
| `POST /v1/doctor` | `{}` | readiness checks from the backend host |
| `POST /v1/lines` | `{}` | installed line names, routes, gates and targets |
| `POST /v1/work-orders` | `line`, `issues`, optional `targets` | run identity, validated blueprint and mapped job count |
| `POST /v1/workers` | `{}` | configured owner's active worker references |
| `POST /v1/workers/remove` | `name` | argv executed after fresh server-side ownership checks |
| `POST /v1/deliveries/prs` / `issues` | optional `label`, `limit` | normalized repository records |
| `POST /v1/deliveries/head` | `branch` | PR publication evidence, or null |
| `POST /v1/deliveries/checks` / `url` | `number` | check summary / browser URL |
| `POST /v1/metrics/runs` / `summary` | `{}` | committed metrics / aggregate |
| `POST /v1/state/runs` | optional `limit` | saved run summaries |
| `POST /v1/state/inspect` | `run_id` | ownership, journal health and interruption evidence |"""
new_table = """| Route | Authority / request | Response |
| --- | --- | --- |
| `GET /v1/liveness` | public deployment probe | process liveness only |
| `GET /v1/readiness` | public deployment probe | read/mutation readiness and serving generation |
| `GET /v1/health` | authenticated read | service/API/Cell schema and mutation readiness |
| `POST /v1/doctor` | authenticated read | readiness checks from the backend host |
| `POST /v1/compatibility` | authenticated read | versioned backend/cell/features contract |
| `POST /v1/fleet` | authenticated read | Cell/generation/repair/queue roll-up |
| `POST /v1/queue` / `queue/inspect` | authenticated read | admission state / one work item |
| `POST /v1/operations` / `operations/inspect` | authenticated read | unresolved mutation journal / one operation |
| `POST /v1/lines` | authenticated read | installed line names, routes, gates and targets |
| `POST /v1/work-orders` | mutation: `line`, `issues`, optional `targets` | admission, Cell binding and Airflow run identity |
| `POST /v1/blueprints/preview` | authenticated read | read-only mapped work preview |
| `POST /v1/cells` / `cells/inspect` / `cells/history` | authenticated read | durable Cell projections/history |
| `POST /v1/cells/transition` | mutation, epoch + operation-key fenced | lifecycle transition and released admission work |
| `POST /v1/evidence/verify` | authenticated read | Cell evidence-chain verification |
| `POST /v1/evidence/checkpoint` | mutation, Cell-scoped | sealed evidence checkpoint |
| `POST /v1/workers` | authenticated read | configured owner's active worker references |
| `POST /v1/workers/remove` | mutation: `name` | argv executed after fresh server-side ownership checks |
| `POST /v1/deliveries/prs` / `issues` | authenticated read | normalized repository records |
| `POST /v1/deliveries/head` | authenticated read: `branch` | PR publication evidence, or null |
| `POST /v1/deliveries/checks` / `url` | authenticated read: `number` | check summary / browser URL |
| `POST /v1/metrics/runs` / `summary` | authenticated read | committed metrics / aggregate |
| `POST /v1/state/runs` / `state/inspect` | authenticated read | saved run / journal health and interruption evidence |
| `POST /v1/scm/issue` | authenticated read; numeric issue only | normalized issue document |
| `POST /v1/scm/publish` | mutation, Cell/epoch/policy fenced | apply patch, push `factory/*` branch, publication evidence |
| `POST /v1/scm/open-issue` | mutation, Cell/epoch/policy fenced | create repository issue and evidence |
| `/v1/airflow/api/v2/...` | compatibility read/mutation subset | Airflow-compatible documents/status codes |"""
replace("docs/factory-backend.md", old_table, new_table)
replace(
    "docs/factory-backend.md",
    "It allows the console's reads\nand only four mutations:",
    "Inside this compatibility mount only, it allows the console's reads\nand exactly four Airflow mutations:",
)
replace(
    "docs/factory-backend.md",
    "Requests are bounded to 64 KiB and upstream responses to 16 MiB, with deadlines and redirects\ndisabled.",
    "Requests are bounded to 64 KiB by default; authenticated SCM patch publication/open-issue\nrequests alone may use up to 16 MiB. Upstream responses are bounded to 16 MiB, with deadlines and\nredirects disabled.",
)
replace("CLAUDE.md", "`src/swfactory/backend.py`", "`src/swfactory/backend/`")

replace(
    "docs/swf.md",
    "The file is `config.toml` under `$SWF_CONFIG`, else `$XDG_CONFIG_HOME/swf/`, else the platform's own\nconfig directory",
    "`$SWF_CONFIG`, when set, is the path to the config **file** itself. Otherwise the file is\n`$XDG_CONFIG_HOME/swf/config.toml`, or `config.toml` under the platform's own config directory",
)
replace(
    "docs/swf.md",
    "That fallback is never written to disk, and `swf doctor` says so:\n\n```\nwarn context       local -> http://localhost:8080 (built-in fallback; nothing is configured)\n                   fix: swf context add local --airflow-url http://localhost:8080\n```",
    "That fallback is never written to disk. `swf context show` reports the fallback in its `note`\nfield; `swf doctor` reports the checks it can actually perform for the resolved context. With no\nbackend token configured, for example, its required backend-auth row is a failure rather than an\ninvented context warning.",
)
replace("docs/swf.md", "`--yes` is required for the five mutations", "`--yes` is required for the six mutations")
replace(
    "docs/swf.md",
    "| `swf gates list` | FILTERS_GATES |\n| `swf jobs list` | FILTERS_JOBS |\n| `swf runs list` | FILTERS_RUNS |\n\nEXAMPLES_BLOCK",
    "| `swf gates list` | `--dag`, `--blueprint`, `--issue`, `--gate`, `--ready`, `--limit` |\n| `swf jobs list` | `--dag`, `--state`, `--issue`, `--attention`, `--limit` |\n| `swf runs list` | `--dag`, `--state`, `--limit` |\n\nFor example:\n\n```sh\nswf gates list --dag factory --issue 142 --ready\nswf jobs list --dag factory --state failed --attention\nswf runs list --dag factory --state running --limit 50\n```",
)
replace(
    "docs/swf.md",
    "DRYRUN_PARA",
    "Bulk gate answers use the same filters as `gates list`. Add `--all --dry-run` first to print the\nexact ready set without writing anything; after review, repeat the identical command without\n`--dry-run` to perform the answers. Example: `swf gates approve --all --dag factory --issue 142\n--ready --dry-run`.",
)

print("patched docs/factory-backend.md, docs/swf.md and CLAUDE.md")
