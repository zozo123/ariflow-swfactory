from __future__ import annotations

from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}: {old[:80]!r}")
    file.write_text(text.replace(old, new, 1))


def replace_between(path: str, start: str, end: str, replacement: str) -> None:
    file = Path(path)
    text = file.read_text()
    i = text.index(start)
    j = text.index(end, i)
    file.write_text(text[:i] + replacement + text[j:])


# ---------------------------------------------------------------- backend runtime/doctor
replace_once(
    "src/swfactory/backend/service.py",
    '''            ]
            try:
                health = self._checked_airflow("GET", "/monitor/health")
''',
    '''            ]
            worker_url = (os.getenv("SWF_BACKEND_URL") or "").strip()
            worker_token = os.getenv("SWF_BACKEND_TOKEN") or ""
            worker_token_ok = len(worker_token) >= 32 and not any(c.isspace() for c in worker_token)
            callback_ready = bool(worker_url) and worker_token_ok
            checks.append(
                {
                    "name": "managed worker callback",
                    "ok": callback_ready,
                    "status": "ok" if callback_ready else "fail",
                    "detail": (
                        f"SWF_BACKEND_URL={'set' if worker_url else 'missing'}, "
                        f"SWF_BACKEND_TOKEN={'valid' if worker_token_ok else 'missing/invalid'}"
                    ),
                    "required": True,
                    "fix": (
                        ""
                        if callback_ready
                        else "export SWF_BACKEND_URL and the same SWF_BACKEND_TOKEN before starting Airflow scheduler/workers"
                    ),
                }
            )
            try:
                health = self._checked_airflow("GET", "/monitor/health")
''',
)

replace_once(
    "deploy/docker/compose.yml",
    '''      AIRFLOW_TOKEN: ${AIRFLOW_TOKEN:-}
      SWF_BACKEND_TOKEN: ${SWF_BACKEND_TOKEN:-}
      SWF_REPO: ${SWF_REPO:-}
''',
    '''      AIRFLOW_TOKEN: ${AIRFLOW_TOKEN:-}
      # Doctor validates the callback contract the Airflow service receives above. Keep the
      # declared callback URL/token on the backend service too so one compose environment is the
      # source of truth for managed workers.
      SWF_BACKEND_URL: ${SWF_BACKEND_URL:-http://backend:8082}
      SWF_BACKEND_TOKEN: ${SWF_BACKEND_TOKEN:-}
      SWF_REPO: ${SWF_REPO:-}
''',
)

# ---------------------------------------------------------------- backend tests
Path("tests/test_backend_service_contract.py").write_text(
    '''from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from swfactory.backend.service import Factory, Refused

TOKEN = "t" * 32


def factory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Factory:
    monkeypatch.setenv("AIRFLOW_TOKEN", "airflow-test-token")
    return Factory(
        token=TOKEN,
        airflow_url="http://localhost:8080",
        root=tmp_path,
        state_root=tmp_path / ".factory",
    )


def test_compatibility_rejects_encoded_path_traversal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    backend = factory(tmp_path, monkeypatch)
    try:
        with pytest.raises(Refused, match="invalid path segment") as caught:
            backend.compatibility("GET", "/dags/%2e%2e/dagRuns", None)
        assert caught.value.status == 400
    finally:
        backend.close()


def test_work_orders_refuse_drain_before_submit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    backend = factory(tmp_path, monkeypatch)
    monkeypatch.setattr(backend, "capabilities", lambda: {"mutation_ready": False})
    monkeypatch.setattr(backend, "submit", lambda body: pytest.fail(f"submit reached during drain: {body}"))
    try:
        with pytest.raises(Refused, match="draining or not mutation-ready") as caught:
            backend.operation("/work-orders", {"line": "factory", "issues": ["1"]})
        assert caught.value.status == 503
    finally:
        backend.close()


def test_doctor_reports_managed_worker_callback_contract(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    backend = factory(tmp_path, monkeypatch)
    monkeypatch.delenv("SWF_BACKEND_URL", raising=False)
    monkeypatch.setattr(shutil, "which", lambda _tool: "/bin/true")

    def airflow(method: str, path: str, body=None):
        assert method == "GET"
        if path == "/monitor/health":
            return {"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"}}
        if path == "/dags?limit=1":
            return {"dags": []}
        raise AssertionError(path)

    monkeypatch.setattr(backend, "_checked_airflow", airflow)
    try:
        rows = backend.operation("/doctor", {})
        callback = next(row for row in rows if row["name"] == "managed worker callback")
        assert callback["ok"] is False
        assert callback["required"] is True
        assert "SWF_BACKEND_URL" in callback["fix"]
        assert "SWF_BACKEND_TOKEN" in callback["fix"]

        monkeypatch.setenv("SWF_BACKEND_URL", "http://backend:8082")
        rows = backend.operation("/doctor", {})
        callback = next(row for row in rows if row["name"] == "managed worker callback")
        assert callback["ok"] is True
        assert callback["fix"] == ""
    finally:
        backend.close()
'''
)

# ---------------------------------------------------------------- Rust backend adapter boundary tests
Path("rust/crates/swf-adapters/tests/factory.rs").write_text(
    '''use std::time::Duration;

use serde_json::{json, Value};
use swf_adapters::error::AdapterError;
use swf_adapters::factory::FactoryApi;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

fn client(server: &MockServer) -> FactoryApi {
    FactoryApi::new(&server.uri(), TOKEN.to_string(), Duration::from_secs(5)).expect("factory client")
}

#[tokio::test]
async fn call_sends_bearer_json_and_decodes_api_v1() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/compatibility"))
        .and(header("authorization", format!("Bearer {TOKEN}")))
        .and(body_json(json!({"probe": true})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"api": 1, "ready": true})))
        .expect(1)
        .mount(&server)
        .await;

    let value: Value = client(&server)
        .call("/compatibility", json!({"probe": true}), &CancellationToken::new())
        .await
        .expect("valid response");
    assert_eq!(value, json!({"api": 1, "ready": true}));
}

#[tokio::test]
async fn non_json_backend_response_fails_closed_as_decode() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/doctor"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not-json"))
        .mount(&server)
        .await;

    let error = client(&server)
        .call::<Value>("/doctor", json!({}), &CancellationToken::new())
        .await
        .expect_err("non-json must be rejected");
    assert!(matches!(error, AdapterError::Decode { .. }));
}

#[tokio::test]
async fn backend_status_is_classified_before_schema_decode() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/doctor"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({"detail": "bad token"})))
        .mount(&server)
        .await;

    let error = client(&server)
        .call::<Value>("/doctor", json!({}), &CancellationToken::new())
        .await
        .expect_err("401 must be auth");
    assert!(matches!(error, AdapterError::Auth { .. }));
}
'''
)

# ---------------------------------------------------------------- operator docs
replace_once(
    "CLAUDE.md",
    "  `backend.py` API; service credentials and work-order validation live there.",
    "  `backend/` API package; service credentials and work-order validation live there.",
)

replace_once(
    "docs/factory-backend.md",
    "`src/swfactory/backend.py` is the network boundary. It loads the installed Python blueprints,",
    "`src/swfactory/backend/` is the network boundary package. It loads the installed Python blueprints,",
)

replace_once(
    "docs/factory-backend.md",
    '''export SWF_BACKEND_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
export AIRFLOW_URL=http://localhost:8080
''',
    '''export SWF_BACKEND_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
# Managed Airflow stages call the backend at every Cell boundary. Export these SAME values in the
# scheduler/worker environment before Airflow starts (use the workers' reachable backend URL).
export SWF_BACKEND_URL=http://localhost:8082
export AIRFLOW_URL=http://localhost:8080
''',
)

replace_once(
    "docs/factory-backend.md",
    '''The default bind is `127.0.0.1:8082`. For remote operators, put an authenticated deployment behind
HTTPS and run `swfactory backend --host 0.0.0.0` on its private network. The API always requires
`Authorization: Bearer <SWF_BACKEND_TOKEN>`, including health. The token must be at least 32
non-whitespace characters. It grants factory operator authority, including approvals and cleanup;
this is a single trusted-operator deployment, not a multi-tenant authorization system.
''',
    '''The default bind is `127.0.0.1:8082`. For remote operators, put an authenticated deployment behind
HTTPS and run `swfactory backend --host 0.0.0.0` on its private network. `GET /v1/liveness` and
`GET /v1/readiness` are deliberately unauthenticated probe endpoints; every other route, including
`GET /v1/health`, requires `Authorization: Bearer <SWF_BACKEND_TOKEN>`. The token must be at least
32 non-whitespace characters. It grants factory operator authority, including approvals, Cell
transitions and publication; this is a single trusted-operator deployment, not a multi-tenant
authorization system.

For a host deployment, start Airflow with `SWF_BACKEND_URL` and the same `SWF_BACKEND_TOKEN` in its
scheduler/worker environment. `swf doctor` reports `managed worker callback` red if this declared
callback contract is missing. In Compose the `airflow` service receives both values automatically;
the backend container keeps the same declarations so its doctor report and worker configuration
come from one environment.
''',
)

api_table = '''| Route | Authority / response |
| --- | --- |
| `GET /v1/liveness` | public process liveness probe |
| `GET /v1/readiness` | public read/mutation readiness and serving generation probe |
| `GET /v1/health` | authenticated service/API/Cell schema versions and mutation readiness |
| `POST /v1/doctor` | backend, Airflow, managed-worker callback and optional-tool checks |
| `POST /v1/compatibility` | versioned capability/contract document |
| `POST /v1/lines` | installed production lines, targets, stages and gates |
| `POST /v1/blueprints/preview` | deterministic mapping preview for a validated line |
| `POST /v1/work-orders` | admit and submit a governed line; creates/owns Factory Cells |
| `POST /v1/fleet` | configured provider/fleet capability document |
| `POST /v1/queue` / `queue/inspect` | admission state and one admitted/queued work item |
| `POST /v1/operations` / `operations/inspect` | unresolved mutation journal and one operation |
| `POST /v1/cells` / `cells/inspect` / `cells/history` | durable Cell projections and history |
| `POST /v1/cells/transition` | **mutation:** epoch-fenced Cell lifecycle transition/cleanup |
| `POST /v1/evidence/verify` / `evidence/checkpoint` | evidence-chain verification/checkpoint |
| `POST /v1/scm/issue` | read one numeric GitHub issue through backend credentials |
| `POST /v1/scm/publish` | **mutation:** epoch/policy-fenced patch push + PR publication |
| `POST /v1/scm/open-issue` | **mutation:** epoch/policy-fenced GitHub issue creation |
| `POST /v1/deliveries/prs` / `deliveries/issues` | normalized repository records |
| `POST /v1/deliveries/head` | publication evidence for a deterministic branch |
| `POST /v1/deliveries/checks` / `deliveries/url` | check summary / browser URL |
| `POST /v1/workers` | configured owner's active worker references |
| `POST /v1/workers/remove` | **mutation:** ownership-checked worker removal |
| `POST /v1/metrics/runs` / `metrics/summary` | committed metrics / aggregate |
| `POST /v1/state/runs` / `state/inspect` | saved run summaries / journal evidence |

'''
replace_between(
    "docs/factory-backend.md",
    "| Route | Request | Response |\n",
    "The compatibility mount",
    api_table,
)

replace_once(
    "docs/factory-backend.md",
    '''The compatibility mount `/v1/airflow/api/v2/...` preserves Airflow's published JSON, pagination,
log continuation and conflict responses for the existing console. It allows the console's reads
and only four mutations: submit an installed line, unpause it, mark its batch failed, and answer
an approval. It cannot change variables, connections, users or DAG code. Read access covers the
''',
    '''The compatibility mount `/v1/airflow/api/v2/...` preserves Airflow's published JSON, pagination,
log continuation and conflict responses for the existing console. **Inside that compatibility
mount only**, the allow-list contains four mutations: submit an installed line, unpause it, mark
its batch failed, and answer an approval. The backend API also has the explicitly documented Cell,
SCM and worker mutations in the table above. The compatibility mount cannot change variables,
connections, users or DAG code. Read access covers the
''',
)

replace_once(
    "docs/factory-backend.md",
    '''Requests are bounded to 64 KiB and upstream responses to 16 MiB, with deadlines and redirects
disabled. Airflow authentication can refresh once after an explicit 401. A timeout or server error
''',
    '''Ordinary requests are bounded to 64 KiB. The two SCM publication routes
`/v1/scm/publish` and `/v1/scm/open-issue` allow up to 16 MiB so bounded patches and issue bodies can
cross the trusted boundary; upstream responses are capped at 16 MiB. Deadlines are enforced and
redirects are disabled. Airflow authentication can refresh once after an explicit 401. A timeout or server error
''',
)

# docs/swf.md factual fixes + generated placeholders
replace_once(
    "docs/swf.md",
    '''The file is `config.toml` under `$SWF_CONFIG`, else `$XDG_CONFIG_HOME/swf/`, else the platform's own
config directory (`~/Library/Application Support/swf/` on macOS). It is written atomically at mode
''',
    '''`$SWF_CONFIG`, when set, is the **path to the config file itself**. Otherwise the file is
`$XDG_CONFIG_HOME/swf/config.toml`, or the platform config directory (for example
`~/Library/Application Support/swf/config.toml` on macOS). It is written atomically at mode
''',
)

replace_once(
    "docs/swf.md",
    '''That fallback is never written to disk, and `swf doctor` says so:

```
warn context       local -> http://localhost:8080 (built-in fallback; nothing is configured)
                   fix: swf context add local --airflow-url http://localhost:8080
```
''',
    '''That fallback is never written to disk. `swf context show` identifies it as the built-in
fallback; `swf doctor` reports the connectivity/authentication checks it can actually perform for
that context (and, in normal backend mode, the backend's own required checks).
''',
)

replace_once(
    "docs/swf.md",
    "`--yes` is required for the five mutations that answer or destroy — `gates approve`, `gates reject`,",
    "`--yes` is required for the six mutations that answer or destroy — `gates approve`, `gates reject`,",
)

replace_once(
    "docs/swf.md",
    '''| `swf gates list` | FILTERS_GATES |
| `swf jobs list` | FILTERS_JOBS |
| `swf runs list` | FILTERS_RUNS |

EXAMPLES_BLOCK
''',
    '''| `swf gates list` | `--dag`, `--blueprint`, `--issue`, `--gate`, `--ready`, `--limit` |
| `swf jobs list` | `--dag`, `--state`, `--issue`, `--attention`, `--limit` |
| `swf runs list` | `--dag`, `--state`, `--limit` |

For example:

```sh
swf gates list --dag factory --issue 42 --ready --limit 50
swf jobs list --dag factory --state failed --issue 42 --attention --limit 50
swf runs list --dag factory --state running --limit 50
```
''',
)

replace_once(
    "docs/swf.md",
    "DRYRUN_PARA\n",
    '''`--dry-run` applies to bulk `gates approve --all` and `gates reject --all`. It runs the same
selection logic as the real batch, reports which ready gates would be answered (and which are
skipped), and performs no mutation. For example, on a non-interactive shell:

```sh
swf gates approve --all --dag factory --issue 42 --ready --yes --dry-run
swf gates approve --all --dag factory --issue 42 --ready --yes
```

''',
)

# ---------------------------------------------------------------- live backend E2E
replace_once(
    "scripts/swf_e2e.sh",
    '''#   scripts/swf_e2e.sh [issue ...]          # default: demo/issue.md demo/issue2.md
#
# Env: SWF_E2E_KEEP=1''',
    '''#   scripts/swf_e2e.sh [--backend] [issue ...]  # default: demo/issue.md demo/issue2.md
#
# `--backend` proves the normal Rust -> Python backend -> Airflow path. Without it the script keeps
# the explicit direct-Airflow compatibility path for local diagnosis.
#
# Env: SWF_E2E_KEEP=1''',
)

replace_once(
    "scripts/swf_e2e.sh",
    '''CONTEXT="e2e"

if [ $# -eq 0 ]; then set -- demo/issue.md demo/issue2.md; fi
''',
    '''CONTEXT="e2e"
BACKEND_MODE=0
if [ "${1:-}" = "--backend" ]; then
  BACKEND_MODE=1
  shift
fi

if [ $# -eq 0 ]; then set -- demo/issue.md demo/issue2.md; fi
''',
)

replace_once(
    "scripts/swf_e2e.sh",
    '''STANDALONE_LOG="$WORK/standalone.log"
STANDALONE_PID=""
''',
    '''STANDALONE_LOG="$WORK/standalone.log"
STANDALONE_PID=""
BACKEND_LOG="$WORK/backend.log"
BACKEND_PID=""
''',
)

replace_once(
    "scripts/swf_e2e.sh",
    '''  if [ "$rc" -ne 0 ] && [ -f "$STANDALONE_LOG" ]; then
    say "standalone log (tail)"
    tail -40 "$STANDALONE_LOG" || true
  fi
  if [ "${SWF_E2E_KEEP:-}" = "1" ]; then echo "work dir kept: $WORK"; else rm -rf "$WORK"; fi
''',
    '''  if [ -n "$BACKEND_PID" ]; then
    kill -TERM "$BACKEND_PID" 2>/dev/null || true
    wait "$BACKEND_PID" 2>/dev/null || true
  fi
  if [ "$rc" -ne 0 ] && [ -f "$STANDALONE_LOG" ]; then
    say "standalone log (tail)"
    tail -40 "$STANDALONE_LOG" || true
  fi
  if [ "$rc" -ne 0 ] && [ -f "$BACKEND_LOG" ]; then
    say "backend log (tail)"
    tail -80 "$BACKEND_LOG" || true
  fi
  if [ "${SWF_E2E_KEEP:-}" = "1" ]; then echo "work dir kept: $WORK"; else rm -rf "$WORK"; fi
''',
)

replace_once(
    "scripts/swf_e2e.sh",
    '''BASE="http://localhost:$PORT"

export AIRFLOW__CORE__DAGS_FOLDER="$REPO/dags"
''',
    '''BASE="http://localhost:$PORT"
BACKEND_BASE=""
BACKEND_PORT=""
if [ "$BACKEND_MODE" -eq 1 ]; then
  BACKEND_PORT="$("$PY" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
  BACKEND_BASE="http://127.0.0.1:$BACKEND_PORT"
  export SWF_BACKEND_URL="$BACKEND_BASE"
  export SWF_BACKEND_TOKEN="${SWF_E2E_BACKEND_TOKEN:-swf-e2e-backend-token-0123456789abcdef}"
fi

export AIRFLOW__CORE__DAGS_FOLDER="$REPO/dags"
''',
)

replace_once(
    "scripts/swf_e2e.sh",
    '''PASSWORD="$("$PY" -c "
import json

users = json.load(open('$PASSWORDS'))
print(users.get('admin') or next(iter(users.values())))
")"

# ---------------------------------------------------------------- 1. connect
''',
    '''PASSWORD="$("$PY" -c "
import json

users = json.load(open('$PASSWORDS'))
print(users.get('admin') or next(iter(users.values())))
")"

if [ "$BACKEND_MODE" -eq 1 ]; then
  say "factory backend on $BACKEND_BASE (log: $BACKEND_LOG)"
  export AIRFLOW_URL="$BASE" AIRFLOW_USER=admin AIRFLOW_PASSWORD="$PASSWORD"
  export SWF_STATE_ROOT="$WORK/backend-state" SWF_METRICS_ROOT="$WORK"
  "$BIN/swfactory" backend --host 127.0.0.1 --port "$BACKEND_PORT" >"$BACKEND_LOG" 2>&1 &
  BACKEND_PID=$!
  i=0
  until curl -fsS "$BACKEND_BASE/v1/liveness" >/dev/null 2>&1; do
    kill -0 "$BACKEND_PID" 2>/dev/null || fail "backend died during startup"
    [ $i -lt 60 ] || fail "backend liveness never went green"
    sleep 1
    i=$((i + 1))
  done
fi

# ---------------------------------------------------------------- 1. connect
''',
)

replace_once(
    "scripts/swf_e2e.sh",
    '''say "swf context add $CONTEXT"
"$SWF" context add "$CONTEXT" --direct \\
  --airflow-url "$BASE" \\
  --repo "zozo123/ariflow-swfactory" \\
  --user admin --password-env SWF_E2E_PASSWORD \\
  --metrics-root "$WORK" \\
  --use
''',
    '''say "swf context add $CONTEXT"
if [ "$BACKEND_MODE" -eq 1 ]; then
  "$SWF" context add "$CONTEXT" \\
    --backend-url "$BACKEND_BASE" \\
    --airflow-url "$BASE" \\
    --repo "zozo123/ariflow-swfactory" \\
    --use
else
  "$SWF" context add "$CONTEXT" --direct \\
    --airflow-url "$BASE" \\
    --repo "zozo123/ariflow-swfactory" \\
    --user admin --password-env SWF_E2E_PASSWORD \\
    --metrics-root "$WORK" \\
    --use
fi
''',
)

replace_once(
    "scripts/swf_e2e.sh",
    '''PY

say "waiting for the dag-processor to parse $DAG_ID"
''',
    '''PY

if [ "$BACKEND_MODE" -eq 1 ]; then
  "$PY" - "$WORK/doctor.json" <<'PY' || fail "doctor did not validate managed worker callbacks"
import json
import sys

rows = json.load(open(sys.argv[1]))
row = next((item for item in rows if item.get("name") == "managed worker callback"), None)
if not row or not row.get("ok"):
    print(f"managed worker callback doctor row: {row}", file=sys.stderr)
    raise SystemExit(1)
print("managed worker callback: ok")
PY
fi

say "waiting for the dag-processor to parse $DAG_ID"
''',
)

replace_once(
    "scripts/swf_e2e.sh",
    '''SUBMIT_ARGS=()
for issue in "${ISSUES[@]}"; do SUBMIT_ARGS+=(--issue "$issue"); done
"$SWF" submit --blueprint "$DAG_ID" "${SUBMIT_ARGS[@]}" --json >"$WORK/submit.json"
''',
    '''SUBMIT_ARGS=()
for issue in "${ISSUES[@]}"; do SUBMIT_ARGS+=(--issue "$issue"); done
SUBMIT_ID_ARGS=()
if [ "$BACKEND_MODE" -eq 1 ]; then
  # This proves the normal harness identity crosses Rust -> backend -> Cell admission -> Airflow.
  SUBMIT_ID_ARGS=(--harness codex --factory-id e2e-codex)
fi
"$SWF" submit --blueprint "$DAG_ID" "${SUBMIT_ARGS[@]}" "${SUBMIT_ID_ARGS[@]}" --json >"$WORK/submit.json"
''',
)

# ---------------------------------------------------------------- CI: backend mode is the main acceptance path; make the job blocking
replace_once(
    ".github/workflows/ci.yml",
    '''  live-gate-e2e:
    if: github.event_name == 'push' || github.base_ref == 'main'
    runs-on: ubuntu-latest
    continue-on-error: true
    timeout-minutes: 45
''',
    '''  live-gate-e2e:
    if: github.event_name == 'push' || github.base_ref == 'main'
    runs-on: ubuntu-latest
    timeout-minutes: 45
''',
)
replace_once(
    ".github/workflows/ci.yml",
    '''      - name: swf drives a live Airflow end to end (2 issues x 2 targets, 8 gates, 4 deliveries)
        id: rust_harness
        continue-on-error: true
        run: bash scripts/swf_e2e.sh
''',
    '''      - name: swf drives backend + live Airflow end to end (2 issues x 2 targets, 8 gates, 4 deliveries)
        id: rust_harness
        continue-on-error: true
        run: bash scripts/swf_e2e.sh --backend
''',
)
replace_once(
    ".github/workflows/ci.yml",
    '''      - run: uv run ruff check . && uv run ruff format --check .
      - run: uv run pytest
''',
    '''      - run: uv run ruff check . && uv run ruff format --check .
      - name: Reject unresolved documentation template tokens
        run: '! grep -R -n -E "FILTERS_[A-Z]+|EXAMPLES_BLOCK|DRYRUN_PARA" docs'
      - run: uv run pytest
''',
)

# Remove an earlier formatter helper accidentally fanned into the integration branch. One-shot
# workflow helpers must never reach main.
Path(".github/workflows/hardening-format-once.yml").unlink(missing_ok=True)
