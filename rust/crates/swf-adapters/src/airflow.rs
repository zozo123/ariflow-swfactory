//! Airflow's public REST API, and nothing else — never its metadata database.
//!
//! Non-negotiable 1 of the architecture. `/api/v2` is the contract Airflow keeps across minor
//! versions; the database schema is an implementation detail that has changed under every major
//! release. Reading it would make `swf` a second scheduler with a private opinion about what the
//! factory is doing, which is precisely the failure this binary replaces.
//!
//! Four things in here are load-bearing and easy to get wrong:
//!
//! * **Pagination.** `limit` is clamped server-side to `[api] maximum_page_limit` (default 100),
//!   silently (`03-airflow-rest.md` §9). The Python client asks for 500 task instances and is
//!   quietly truncated today. So every collection read loops, advances `offset` by the number of
//!   rows *actually returned*, and stops on an empty page, on `total_entries`, or at
//!   [`MAX_PAGES`] — and in that last case it sets [`Page::truncated`](crate::traits::Page) rather
//!   than pretending it saw everything.
//! * **Percent-encoding.** A manual run id is `manual__2026-09-03T08:18:24.904858+00:00`: it
//!   carries `:` and `+`. Every dynamic path segment goes through [`seg`], which encodes down to
//!   the unreserved set exactly as Python's `quote(v, safe="")` does. An unencoded `+` in a query
//!   string decodes to a space and addresses a run that does not exist.
//! * **One re-mint, then stop.** The default JWT lives 24 h, so a TUI left open overnight *will*
//!   see a 401. A single silent re-mint keeps that invisible; a retry loop would turn a wrong
//!   password into a login flood, so after one failure the error is the operator's to read.
//! * **Cancellation.** Every call takes a [`CancellationToken`]. When the operator switches
//!   context, the in-flight refresh is abandoned rather than allowed to land on top of the newer
//!   one — a stale answer arriving second is worse than no answer at all.
//!
//! Log text is sanitised here, at the point it enters the process, rather than at each renderer.
//! A log line is written by an agent and can contain `ESC [ 2 J`; making every future call site
//! remember to scrub it is a security bug waiting for its first forgetful caller (non-negotiable
//! 7).

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, Method, StatusCode};
use serde_json::{json, Map, Value};
use swf_domain::ids::{GateId, JobId, RunRef};
use swf_domain::model::{Gate, JobRow, Run, TaskState};
use swf_domain::rollup::group_jobs;
use swf_domain::sanitize::sanitize_line;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::error::{truncate, AdapterError, Result};
use crate::traits::{parse_ts, LogPage, Page, Runs, DEFAULT_HTTP_TIMEOUT};

/// The stable public prefix. Everything below it is versioned and backward-compatible.
pub const API_PREFIX: &str = "/api/v2";

/// The auth manager's own route. It is **not** under `/api/v2` — it is a separately mounted app,
/// and a client that prefixes it will 404 on every login (`03-airflow-rest.md` §0).
pub const TOKEN_PATH: &str = "/auth/token";

/// The tag the shipped blueprints carry.
pub const DEFAULT_DAG_TAG: &str = "swfactory";

/// The task whose XCom lists the jobs a run fanned out into.
pub const FAN_OUT_TASK_ID: &str = "fan_out";

/// The XCom key a task's return value is stored under.
pub const XCOM_RETURN_KEY: &str = "return_value";

/// The answer Airflow's `ApprovalOperator` accepts for "yes".
pub const GATE_APPROVE: &str = "Approve";

/// The answer Airflow's `ApprovalOperator` accepts for "no".
pub const GATE_REJECT: &str = "Reject";

/// What to ask for per page. The server clamps anything larger to `[api] maximum_page_limit`
/// (default 100) without saying so, so asking for more is a lie the client tells itself.
pub const PAGE_LIMIT: usize = 100;

/// How many pages one collection read may fetch before it stops and admits it stopped.
///
/// A hundred pages is ten thousand rows: past that, something is wrong with the filter and the
/// honest answer is a truncated table with a visible note, not a request storm.
pub const MAX_PAGES: usize = 100;

/// How the client proves who it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Auth {
    /// A server configured for anonymous admin access. The probe in `09-live-api-probe.md` shows
    /// a *default* standalone answers 401 to this, so it is a deliberate choice and not a default.
    None,
    /// A token supplied by the operator's environment. It cannot be re-minted, so its expiry is
    /// reported rather than papered over.
    Token(String),
    /// A username and password to exchange for a JWT at [`TOKEN_PATH`]. An empty password is
    /// allowed — the simple auth manager accepts one.
    Basic {
        /// The account name.
        username: String,
        /// The secret. It lives in memory only; it is never written to the config file (rule 6).
        password: String,
    },
}

/// The `Runs` implementation: Airflow's REST API over `reqwest` + rustls.
#[derive(Debug)]
pub struct AirflowApi {
    base: String,
    http: Client,
    auth: Auth,
    timeout: Duration,
    /// The minted JWT. A `Mutex` and not an `RwLock` because the point is to serialise *minting*:
    /// twenty panes refreshing at once must produce one token request, not twenty.
    token: Arc<Mutex<Option<String>>>,
}

impl AirflowApi {
    /// Build a client for one environment.
    ///
    /// `base_url` is the whole base, not an origin: `[api] base_url` can put Airflow behind a path
    /// prefix, and both `/api/v2` and `/auth/token` inherit it. The trailing `/` is stripped once,
    /// here, so no call site has to think about it again.
    pub fn new(base_url: &str, auth: Auth, timeout: Duration) -> Result<Self> {
        let http = Client::builder()
            .timeout(timeout)
            .user_agent(concat!("swf/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| AdapterError::Unreachable {
                what: "HTTP client".to_string(),
                detail: truncate(&e.to_string()),
            })?;
        Ok(Self {
            base: base_url.trim_end_matches('/').to_string(),
            http,
            auth,
            timeout,
            token: Arc::new(Mutex::new(None)),
        })
    }

    /// A client with the Python default timeout of 15 s.
    pub fn with_defaults(base_url: &str, auth: Auth) -> Result<Self> {
        Self::new(base_url, auth, DEFAULT_HTTP_TIMEOUT)
    }

    /// The base URL, already stripped of its trailing slash.
    pub fn base_url(&self) -> &str {
        &self.base
    }

    /// True when a fresh credential can be obtained without asking the operator.
    ///
    /// Only [`Auth::Basic`] qualifies: a static token that expired is a fact to report, not a
    /// problem to retry around.
    fn can_remint(&self) -> bool {
        matches!(self.auth, Auth::Basic { .. })
    }

    /// The bearer token to present, minting one if that is possible and not yet done.
    async fn token(&self, cancel: &CancellationToken) -> Result<Option<String>> {
        match &self.auth {
            Auth::None => Ok(None),
            Auth::Token(token) => Ok(Some(token.clone())),
            Auth::Basic { username, password } => {
                let mut cached = self.token.lock().await;
                if let Some(token) = cached.as_ref() {
                    return Ok(Some(token.clone()));
                }
                let minted = self.mint(username, password, cancel).await?;
                *cached = Some(minted.clone());
                Ok(Some(minted))
            }
        }
    }

    /// Forget the cached token so the next call mints a fresh one.
    async fn forget_token(&self) {
        *self.token.lock().await = None;
    }

    /// Exchange a username and password for a JWT.
    ///
    /// Any 2xx is accepted deliberately. The route is decorated `201`, an earlier reading of the
    /// live server recorded `200`, and the two disagreed once already
    /// (`09-live-api-probe.md` §3) — gating on an exact status would make login a coin flip.
    async fn mint(
        &self,
        username: &str,
        password: &str,
        cancel: &CancellationToken,
    ) -> Result<String> {
        let url = format!("{}{TOKEN_PATH}", self.base);
        let body = json!({"username": username, "password": password});
        let data = self
            .send(Method::POST, &url, &[], Some(&body), None, cancel)
            .await?;
        let token = data
            .as_ref()
            .and_then(|v| v.get("access_token"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        if token.is_empty() {
            return Err(AdapterError::Auth {
                detail: format!("{TOKEN_PATH} returned no access_token"),
            });
        }
        Ok(token.to_string())
    }

    /// One authenticated `/api/v2` call, with exactly one silent re-mint on an auth failure.
    async fn api(
        &self,
        method: Method,
        path: &str,
        query: &[(String, String)],
        body: Option<&Value>,
        cancel: &CancellationToken,
    ) -> Result<Option<Value>> {
        let url = format!("{}{API_PREFIX}{path}", self.base);
        let token = self.token(cancel).await?;
        let first = self
            .send(method.clone(), &url, query, body, token.as_deref(), cancel)
            .await;
        match first {
            // The token expired under a long-lived TUI, or the server rotated its key. Drop the
            // cached one, mint once, try once. A second failure is the operator's to read.
            Err(err) if err.is_auth() && self.can_remint() => {
                self.forget_token().await;
                let fresh = self.token(cancel).await?;
                self.send(method, &url, query, body, fresh.as_deref(), cancel)
                    .await
            }
            other => other,
        }
    }

    /// One unauthenticated `/api/v2` call, for the probes that sit outside the auth router.
    async fn api_unauth(
        &self,
        method: Method,
        path: &str,
        cancel: &CancellationToken,
    ) -> Result<Option<Value>> {
        let url = format!("{}{API_PREFIX}{path}", self.base);
        self.send(method, &url, &[], None, None, cancel).await
    }

    /// Send one request and read one answer. This is the only place a status code exists.
    async fn send(
        &self,
        method: Method,
        url: &str,
        query: &[(String, String)],
        body: Option<&Value>,
        token: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<Option<Value>> {
        let mut builder = self
            .http
            .request(method, url)
            .header(reqwest::header::ACCEPT, "application/json")
            .timeout(self.timeout);
        if !query.is_empty() {
            builder = builder.query(query);
        }
        if let Some(body) = body {
            builder = builder.json(body);
        }
        if let Some(token) = token {
            builder = builder.bearer_auth(token);
        }
        let request = builder.build().map_err(|e| AdapterError::Unreachable {
            what: url.to_string(),
            detail: truncate(&e.to_string()),
        })?;
        let what = format!("{} {}", request.method(), request.url());

        let call = async {
            let response = self
                .http
                .execute(request)
                .await
                .map_err(|e| self.transport_error(&what, &e))?;
            let status = response.status();
            let text = response
                .text()
                .await
                .map_err(|e| self.transport_error(&what, &e))?;
            if !status.is_success() {
                return Err(AdapterError::from_status(
                    status.as_u16(),
                    &what,
                    &detail_of(&text),
                ));
            }
            // A blank body is a legitimate success (a 204, a PATCH that answers nothing).
            if text.trim().is_empty() {
                return Ok(None);
            }
            match serde_json::from_str::<Value>(&text) {
                Ok(Value::Null) => Ok(None),
                Ok(value) => Ok(Some(value)),
                Err(_) => Err(AdapterError::Decode {
                    what,
                    detail: truncate(&format!("{:?}", excerpt(&text))),
                }),
            }
        };
        guard(cancel, call).await
    }

    /// Turn a transport failure into this crate's vocabulary, stamping the real timeout budget.
    fn transport_error(&self, what: &str, error: &reqwest::Error) -> AdapterError {
        match AdapterError::from_reqwest(what, error) {
            AdapterError::Timeout { what, .. } => AdapterError::Timeout {
                what,
                after: self.timeout,
            },
            other => other,
        }
    }

    /// Read a whole collection, page by page.
    ///
    /// `want` bounds the number of rows the caller asked for (`list_runs(limit)`); `None` means
    /// "all of them". `offset` advances by the rows actually returned, never by the requested
    /// limit, because the server clamps the limit without telling anyone.
    async fn paged(
        &self,
        path: &str,
        rows_key: &str,
        base_query: &[(String, String)],
        want: Option<usize>,
        cancel: &CancellationToken,
    ) -> Result<Page<Value>> {
        let mut rows: Vec<Value> = Vec::new();
        let mut offset = 0usize;
        let mut pages = 0usize;

        loop {
            if want.is_some_and(|w| rows.len() >= w) {
                break;
            }
            if pages >= MAX_PAGES {
                // Something above us asked a question with an unbounded answer. Stop, keep what
                // we have, and let the caller render the fact that this is not everything.
                return Ok(Page::partial(rows));
            }
            let limit = match want {
                Some(w) => PAGE_LIMIT.min(w - rows.len()),
                None => PAGE_LIMIT,
            };
            let mut query = base_query.to_vec();
            query.push(("limit".to_string(), limit.to_string()));
            query.push(("offset".to_string(), offset.to_string()));

            let body = self
                .api(Method::GET, path, &query, None, cancel)
                .await?
                .unwrap_or(Value::Null);
            let page: Vec<Value> = body
                .get(rows_key)
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();

            let got = page.len();
            rows.extend(page);
            offset += got;
            pages += 1;

            // An empty page is the real terminator: `total_entries` is `int | null` on `dagRuns`
            // and `taskInstances` and must never be the only stop condition (gotcha 21).
            if got == 0 {
                break;
            }
            if let Some(total) = body.get("total_entries").and_then(Value::as_i64) {
                if offset as i64 >= total {
                    break;
                }
            }
        }
        Ok(Page::whole(rows))
    }

    /// The UI deep link for a run, built from ids so a just-triggered run can be linked at once.
    pub fn deep_link(&self, dag_id: &str, run_id: &str) -> String {
        format!("{}/dags/{}/runs/{}", self.base, seg(dag_id), seg(run_id))
    }
}

/// Percent-encode one path segment down to the unreserved set, as Python's `quote(v, safe="")`.
///
/// Nothing survives but `A-Za-z0-9_.-~`, so `:` becomes `%3A` and `+` becomes `%2B`. A "path
/// segment" encode set that spares `+` looks right and is wrong: the same id is also used in query
/// strings, where a raw `+` decodes to a space and quietly addresses a different run.
pub fn seg(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' | b'~' => {
                out.push(*byte as char);
            }
            other => {
                // Writing to a String cannot fail; the result is discarded rather than unwrapped.
                let _ = write!(out, "%{other:02X}");
            }
        }
    }
    out
}

/// Run a future unless the caller has already stopped caring, and abandon it if they stop midway.
///
/// `biased` so an already-cancelled token wins without a poll of the request — a context switch
/// must not cost a round trip nobody will read.
async fn guard<T, F>(cancel: &CancellationToken, fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    tokio::select! {
        biased;
        () = cancel.cancelled() => Err(AdapterError::Cancelled),
        out = fut => out,
    }
}

/// Pull the human-readable half out of an error body.
///
/// `detail` is `string | object | list` across this API — the 409 from the unique-constraint
/// handler is an object, a 422 is a list (gotcha 12) — so it is read as a `Value` and rendered,
/// never deserialized into a `String` that would fail exactly when the text matters most.
fn detail_of(text: &str) -> String {
    match serde_json::from_str::<Value>(text) {
        Ok(value) => match value.get("detail") {
            Some(Value::String(s)) => s.clone(),
            Some(other) => other.to_string(),
            None => excerpt(text),
        },
        Err(_) => excerpt(text),
    }
}

/// The first 200 characters of a body, for an error message that has to fit on a line.
fn excerpt(text: &str) -> String {
    text.trim().chars().take(200).collect()
}

/// Python's `str(d.get(key, ""))`: a *missing* key reads as `""`, but a key present as JSON `null`
/// reads as `"None"`. It looks like a bug and it is the contract `_run_from` was written against.
fn py_get_str(object: &Value, key: &str) -> String {
    match object.get(key) {
        None => String::new(),
        Some(Value::Null) => "None".to_string(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

/// Python's `str(x or "")` over a falsy chain: the first value that is neither absent, `null` nor
/// an empty string.
fn or_str<'a>(object: &'a Value, keys: &[&str]) -> &'a str {
    for key in keys {
        if let Some(Value::String(s)) = object.get(*key) {
            if !s.is_empty() {
                return s;
            }
        }
    }
    ""
}

/// Read one `DAGRunResponse` (`03-airflow-rest.md` §4).
pub fn run_from(d: &Value) -> Run {
    let state = or_str(d, &["state"]);
    let mut run = Run::new(
        py_get_str(d, "dag_id"),
        or_str(d, &["dag_run_id", "run_id"]),
        if state.is_empty() { "unknown" } else { state },
    );
    run.start = d.get("start_date").and_then(parse_ts);
    run.end = d.get("end_date").and_then(parse_ts);
    if let Some(Value::Object(conf)) = d.get("conf") {
        run.conf = conf.clone();
    }
    run
}

/// Read one `TaskInstanceResponse` down to the three fields a roll-up needs (§5).
///
/// `state` is `Option<String>` and unknown states pass through untouched: the enum gained
/// `awaiting_input` in 3.3 and will gain more, and a client that hard-fails on a state it has not
/// heard of is a client that breaks on the next upgrade (gotcha 8).
pub fn task_state_from(ti: &Value) -> TaskState {
    TaskState::new(
        py_get_str(ti, "task_id"),
        ti.get("map_index").and_then(Value::as_i64).unwrap_or(-1) as i32,
        ti.get("state")
            .and_then(Value::as_str)
            .map(str::to_string)
            .filter(|s| !s.is_empty()),
    )
}

/// Read one `HITLDetail` (§7a).
///
/// The nested `task_instance` wins for identity and the flat keys are a fallback for older builds.
/// `map_index` is read with a *default* chain rather than a falsy one, because a job index of `0`
/// is a real job and an `or` chain would skip straight past it.
pub fn gate_from(h: &Value) -> Gate {
    let empty = Value::Object(Map::new());
    let ti = match h.get("task_instance") {
        Some(v) if v.is_object() => v,
        _ => &empty,
    };
    let dag_id = {
        let from_ti = or_str(ti, &["dag_id"]);
        if from_ti.is_empty() {
            or_str(h, &["dag_id"])
        } else {
            from_ti
        }
    };
    let run_id = {
        let from_ti = or_str(ti, &["dag_run_id", "run_id"]);
        if from_ti.is_empty() {
            or_str(h, &["run_id"])
        } else {
            from_ti
        }
    };
    let task_id = {
        let from_ti = or_str(ti, &["task_id"]);
        if from_ti.is_empty() {
            or_str(h, &["task_id"])
        } else {
            from_ti
        }
    };
    let map_index = ti
        .get("map_index")
        .and_then(Value::as_i64)
        .or_else(|| h.get("map_index").and_then(Value::as_i64))
        .unwrap_or(-1) as i32;
    let options = h
        .get("options")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|o| match o {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .collect()
        })
        .unwrap_or_default();

    let mut gate = Gate::new(
        dag_id,
        run_id,
        task_id,
        map_index,
        or_str(h, &["subject"]),
        or_str(h, &["body"]),
        None,
        options,
    );
    // Assigned rather than passed: the constructor is UTC-shaped, and the offset a source reported
    // is part of the snapshot document. `2026-09-03T14:00:00+02:00` must come back out spelled the
    // way it came in, or the byte-diff against `herd --once --json` fails on the same instant.
    gate.created_at = h.get("created_at").and_then(parse_ts);
    gate
}

/// Render one entry of a log page's `content`.
///
/// `content` is `list[StructuredLogMessage]` **or** `list[str]` — the handler falls back to raw
/// lines when it cannot parse the file (gotcha 28) — so both arms are handled rather than one
/// being assumed. Every result is sanitised: this is the boundary, and past it a log line is text.
fn log_line(entry: &Value) -> String {
    let raw = match entry {
        Value::String(s) => s.clone(),
        Value::Object(map) => {
            let event = map.get("event").and_then(Value::as_str).unwrap_or_default();
            match map.get("timestamp").and_then(Value::as_str) {
                Some(ts) if !ts.is_empty() => format!("{ts} {event}"),
                _ => event.to_string(),
            }
        }
        other => other.to_string(),
    };
    sanitize_line(&raw)
}

#[async_trait]
impl Runs for AirflowApi {
    async fn list_dags(&self, tag: &str, cancel: &CancellationToken) -> Result<Page<String>> {
        let mut query = Vec::new();
        if !tag.is_empty() {
            query.push(("tags".to_string(), tag.to_string()));
        }
        let page = self.paged("/dags", "dags", &query, None, cancel).await?;
        // A row without a `dag_id` is not a DAG; dropping it is what the Python does, and it keeps
        // one malformed entry from poisoning the whole list.
        Ok(Page {
            rows: page
                .rows
                .iter()
                .map(|d| py_get_str(d, "dag_id"))
                .filter(|id| !id.is_empty())
                .collect(),
            truncated: page.truncated,
        })
    }

    async fn list_runs(
        &self,
        dag_id: &str,
        limit: usize,
        cancel: &CancellationToken,
    ) -> Result<Page<Run>> {
        let path = format!("/dags/{}/dagRuns", seg(dag_id));
        let query = vec![("order_by".to_string(), "-run_after".to_string())];
        let page = self
            .paged(&path, "dag_runs", &query, Some(limit), cancel)
            .await?;
        Ok(page.map(|d| run_from(&d)))
    }

    async fn task_states(
        &self,
        run: &RunRef,
        cancel: &CancellationToken,
    ) -> Result<Page<TaskState>> {
        let path = format!(
            "/dags/{}/dagRuns/{}/taskInstances",
            seg(&run.dag_id),
            seg(&run.run_id)
        );
        let page = self
            .paged(&path, "task_instances", &[], None, cancel)
            .await?;
        Ok(page.map(|ti| task_state_from(&ti)))
    }

    async fn fan_out_jobs(&self, run: &RunRef, cancel: &CancellationToken) -> Result<Vec<Value>> {
        let path = format!(
            "/dags/{}/dagRuns/{}/taskInstances/{}/xcomEntries/{}",
            seg(&run.dag_id),
            seg(&run.run_id),
            seg(FAN_OUT_TASK_ID),
            seg(XCOM_RETURN_KEY)
        );
        let query = vec![("map_index".to_string(), "-1".to_string())];
        let body = match self.api(Method::GET, &path, &query, None, cancel).await {
            Ok(body) => body.unwrap_or(Value::Null),
            // A run that has not fanned out yet has no XCom. That is a state of the world, not a
            // failure, and the job rows degrade to their fallback issues (§6, gotcha 7).
            Err(AdapterError::NotFound { .. }) => return Ok(Vec::new()),
            Err(err) => return Err(err),
        };

        // The default (`deserialize=false`) mode hands back the raw DB column: a JSON *string*
        // containing the payload. Parse it a second time, but tolerate a value that already
        // arrived structured — values written through the Task Execution API do.
        let value = match body.get("value") {
            Some(Value::String(raw)) => match serde_json::from_str::<Value>(raw) {
                Ok(parsed) => parsed,
                Err(_) => return Ok(Vec::new()),
            },
            Some(other) => other.clone(),
            None => return Ok(Vec::new()),
        };
        Ok(match value {
            Value::Array(items) => items.into_iter().filter(Value::is_object).collect(),
            _ => Vec::new(),
        })
    }

    async fn job_rows(
        &self,
        run: &RunRef,
        fallback: &[String],
        cancel: &CancellationToken,
    ) -> Result<Page<JobRow>> {
        let tasks = self.task_states(run, cancel).await?;
        // Exactly two bounded reads per run, and only the *first* may fail the row: a missing or
        // unreadable XCom is degradation, not an outage.
        let fan_out = match self.fan_out_jobs(run, cancel).await {
            Ok(jobs) => jobs,
            Err(err) if err.is_cancelled() => return Err(err),
            Err(_) => Vec::new(),
        };
        let rows = group_jobs(&run.dag_id, &run.run_id, &tasks.rows, &fan_out, fallback);
        Ok(Page {
            rows,
            truncated: tasks.truncated,
        })
    }

    async fn pending_gates(&self, cancel: &CancellationToken) -> Result<Page<Gate>> {
        // `~` is a real wildcard on this route and must go out unencoded (gotcha 16).
        let query = vec![("response_received".to_string(), "false".to_string())];
        let page = self
            .paged(
                "/dags/~/dagRuns/~/hitlDetails",
                "hitl_details",
                &query,
                None,
                cancel,
            )
            .await?;
        Ok(page.map(|h| gate_from(&h)))
    }

    async fn logs(
        &self,
        job: &JobId,
        task: &str,
        attempt: u32,
        token: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<LogPage> {
        let path = format!(
            "/dags/{}/dagRuns/{}/taskInstances/{}/logs/{attempt}",
            seg(&job.dag_id),
            seg(&job.run_id),
            seg(task)
        );
        let mut query = vec![("map_index".to_string(), job.map_index.to_string())];
        if let Some(token) = token {
            query.push(("token".to_string(), token.to_string()));
        }
        let body = self
            .api(Method::GET, &path, &query, None, cancel)
            .await?
            .unwrap_or(Value::Null);
        let lines = body
            .get("content")
            .and_then(Value::as_array)
            .map(|items| items.iter().map(log_line).collect())
            .unwrap_or_default();
        Ok(LogPage {
            lines,
            // `continuation_token == null` is the only end-of-log signal on the wire (gotcha 19).
            continuation_token: body
                .get("continuation_token")
                .and_then(Value::as_str)
                .filter(|t| !t.is_empty())
                .map(str::to_string),
        })
    }

    async fn respond(
        &self,
        gate: &GateId,
        approve: bool,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let choice = if approve { GATE_APPROVE } else { GATE_REJECT };
        // `map_index` is a required *path* segment here, not a query parameter (gotcha 10).
        let path = format!(
            "/dags/{}/dagRuns/{}/taskInstances/{}/{}/hitlDetails",
            seg(&gate.job.dag_id),
            seg(&gate.job.run_id),
            seg(&gate.task_id),
            gate.job.map_index
        );
        let body = json!({"chosen_options": [choice], "params_input": {}});
        self.api(Method::PATCH, &path, &[], Some(&body), cancel)
            .await?;
        Ok(())
    }

    async fn trigger(
        &self,
        dag_id: &str,
        issues: &[String],
        cancel: &CancellationToken,
    ) -> Result<String> {
        let refs: Vec<String> = issues
            .iter()
            .map(|i| i.trim().to_string())
            .filter(|i| !i.is_empty())
            .collect();
        if refs.is_empty() {
            return Err(AdapterError::refused("trigger needs at least one issue"));
        }
        let path = format!("/dags/{}/dagRuns", seg(dag_id));
        // `logical_date` has no default and the body forbids extra keys, so the key must be
        // present and explicitly null or the request is a 422 (gotcha 4).
        let body = json!({"logical_date": Value::Null, "conf": {"issues": refs}});
        let data = self
            .api(Method::POST, &path, &[], Some(&body), cancel)
            .await?
            .unwrap_or(Value::Null);
        let run_id = or_str(&data, &["dag_run_id", "run_id"]);
        if run_id.is_empty() {
            return Err(AdapterError::Decode {
                what: format!("trigger {dag_id}"),
                detail: "response carried no dag_run_id".to_string(),
            });
        }
        Ok(run_id.to_string())
    }

    async fn stop_run(&self, run: &RunRef, cancel: &CancellationToken) -> Result<()> {
        let path = format!("/dags/{}/dagRuns/{}", seg(&run.dag_id), seg(&run.run_id));
        // Only the keys present in the body are applied, so `update_mask` is omitted on purpose
        // (§8b). And `running` is not a settable state — `failed` is what "stop" means here.
        let body = json!({"state": "failed"});
        self.api(Method::PATCH, &path, &[], Some(&body), cancel)
            .await?;
        Ok(())
    }

    async fn unpause_dag(&self, dag_id: &str, cancel: &CancellationToken) -> Result<()> {
        let path = format!("/dags/{}", seg(dag_id));
        let body = json!({"is_paused": false});
        self.api(Method::PATCH, &path, &[], Some(&body), cancel)
            .await?;
        Ok(())
    }

    async fn health(&self, cancel: &CancellationToken) -> Result<Value> {
        // Unauthenticated on purpose: this is the probe that tells "wrong URL" from "wrong token",
        // and sending a credential would collapse the two answers into one.
        Ok(self
            .api_unauth(Method::GET, "/monitor/health", cancel)
            .await?
            .unwrap_or(Value::Null))
    }

    fn run_url(&self, run: &RunRef) -> String {
        self.deep_link(&run.dag_id, &run.run_id)
    }
}

/// Whether a status is one this client re-mints for. Exposed for the tests that pin §11's table.
pub fn is_auth_status(status: StatusCode, detail: &str) -> bool {
    status == StatusCode::UNAUTHORIZED
        || (status == StatusCode::FORBIDDEN && detail.contains("Invalid JWT token"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_run_id_survives_its_colons_and_pluses() {
        let run = "manual__2026-09-03T08:18:24.904858+00:00";
        assert_eq!(seg(run), "manual__2026-09-03T08%3A18%3A24.904858%2B00%3A00");
        assert_eq!(seg("a/b"), "a%2Fb");
        assert_eq!(seg("a b"), "a%20b");
        assert_eq!(seg("~"), "~", "unreserved characters stay literal");
        assert_eq!(seg("añ"), "a%C3%B1", "non-ASCII is encoded per UTF-8 byte");
    }

    #[test]
    fn the_base_url_loses_exactly_one_trailing_slash() {
        let api =
            AirflowApi::with_defaults("https://host/airflow/", Auth::None).expect("client builds");
        assert_eq!(api.base_url(), "https://host/airflow");
        assert_eq!(
            api.deep_link("factory", "manual__x:1"),
            "https://host/airflow/dags/factory/runs/manual__x%3A1"
        );
    }

    #[test]
    fn run_from_reproduces_the_falsy_chains_and_the_none_string() {
        let run = run_from(&json!({
            "dag_id": "factory", "dag_run_id": "r1", "state": "running",
            "conf": {"issues": ["42"]}
        }));
        assert_eq!(run.dag_id, "factory");
        assert_eq!(run.run_id, "r1");
        assert_eq!(run.state, "running");
        assert_eq!(run.issues(), vec!["42"]);

        // `state` falsy -> "unknown"; `run_id` falls through to the legacy key.
        let odd = run_from(&json!({"dag_id": "f", "run_id": "r2", "state": null}));
        assert_eq!(odd.state, "unknown");
        assert_eq!(odd.run_id, "r2");

        // The documented Python quirk: a present-but-null dag_id stringifies to "None".
        assert_eq!(run_from(&json!({"dag_id": null})).dag_id, "None");
        assert_eq!(run_from(&json!({})).dag_id, "");
        assert!(run_from(&json!({"conf": "not an object"})).conf.is_empty());
    }

    #[test]
    fn a_task_instance_keeps_a_state_nobody_has_heard_of() {
        let ti = task_state_from(&json!({"task_id": "job.build", "map_index": 3, "state": "moon"}));
        assert_eq!(ti.task_id, "job.build");
        assert_eq!(ti.map_index, 3);
        assert_eq!(ti.state.as_deref(), Some("moon"));

        let unstarted = task_state_from(&json!({"task_id": "fan_out", "state": null}));
        assert_eq!(unstarted.map_index, -1, "a missing map_index is unmapped");
        assert_eq!(unstarted.state_or_none(), "none");
    }

    #[test]
    fn a_gate_takes_its_identity_from_the_nested_task_instance() {
        let gate = gate_from(&json!({
            "subject": "[factory] approve intent.md",
            "body": "Run manual__x · job 0",
            "options": ["Approve", "Reject"],
            "created_at": "2026-09-03T08:19:12.857207Z",
            "task_instance": {
                "dag_id": "factory", "dag_run_id": "manual__x",
                "task_id": "job.approve_intent", "map_index": 0
            }
        }));
        assert_eq!(gate.dag_id, "factory");
        assert_eq!(gate.run_id, "manual__x");
        assert_eq!(gate.task_id, "job.approve_intent");
        assert_eq!(
            gate.map_index, 0,
            "job 0 is a job, not a falsy value to skip"
        );
        assert_eq!(gate.short_name(), "approve_intent");
        assert!(gate.created_at.is_some());
        assert!(gate.accepts(GATE_APPROVE) && gate.accepts(GATE_REJECT));
        assert!(
            !gate.ready,
            "readiness is a two-poll decision, not a field of the payload"
        );
    }

    #[test]
    fn a_gate_falls_back_to_the_flat_keys_of_an_older_build() {
        let gate = gate_from(&json!({
            "dag_id": "hotfix", "run_id": "r9", "task_id": "job.approve_plan",
            "map_index": 2, "subject": "s"
        }));
        assert_eq!(gate.dag_id, "hotfix");
        assert_eq!(gate.run_id, "r9");
        assert_eq!(gate.map_index, 2);
        assert_eq!(gate.body, "");
        assert!(gate.options.is_empty());
        assert!(
            gate.accepts("anything"),
            "no declared options means no local veto"
        );
    }

    #[test]
    fn a_gate_with_nothing_in_it_still_produces_a_value() {
        let gate = gate_from(&json!({}));
        assert_eq!(gate.map_index, -1);
        assert_eq!(gate.subject, "");
        assert_eq!(gate.revision, Gate::revision_of("", ""));
    }

    #[test]
    fn detail_is_read_from_every_shape_the_api_uses_for_it() {
        assert_eq!(
            detail_of(r#"{"detail":"Not authenticated"}"#),
            "Not authenticated"
        );
        assert_eq!(
            detail_of(r#"{"detail":{"reason":"Unique constraint violation"}}"#),
            r#"{"reason":"Unique constraint violation"}"#
        );
        assert!(detail_of(r#"{"detail":[{"msg":"field required"}]}"#).contains("field required"));
        assert_eq!(detail_of("plain text body"), "plain text body");
        assert_eq!(detail_of(""), "");
    }

    #[test]
    fn a_log_entry_is_scrubbed_whichever_shape_it_arrives_in() {
        let structured = log_line(&json!({"timestamp": "2026-09-03T08:00:00Z", "event": "hello"}));
        assert_eq!(structured, "2026-09-03T08:00:00Z hello");
        assert_eq!(log_line(&json!({"event": "bare"})), "bare");
        assert_eq!(log_line(&json!("raw line")), "raw line");

        // The reason this happens at the boundary and not at the renderer.
        let hostile = log_line(&json!({"event": "safe\u{1b}[2Jcleared\u{1b}]0;retitle\u{7}"}));
        assert_eq!(hostile, "safecleared");
    }

    #[tokio::test]
    async fn an_already_cancelled_token_stops_the_call_before_it_starts() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let err = guard(&cancel, async { Ok::<u8, AdapterError>(1) })
            .await
            .expect_err("a cancelled caller gets no answer");
        assert!(err.is_cancelled());
    }

    #[test]
    fn only_a_password_can_be_re_minted() {
        let basic = AirflowApi::with_defaults(
            "http://x",
            Auth::Basic {
                username: "admin".into(),
                password: String::new(),
            },
        )
        .expect("client builds");
        assert!(basic.can_remint(), "an empty password is still a password");

        let static_token =
            AirflowApi::with_defaults("http://x", Auth::Token("t".into())).expect("client builds");
        assert!(
            !static_token.can_remint(),
            "an expired static token is a fact to report, not a retry to hide"
        );
        assert!(!AirflowApi::with_defaults("http://x", Auth::None)
            .expect("client builds")
            .can_remint());
    }

    #[test]
    fn the_auth_statuses_are_the_ones_section_eleven_names() {
        assert!(is_auth_status(StatusCode::UNAUTHORIZED, "Token Expired"));
        assert!(is_auth_status(StatusCode::FORBIDDEN, "Invalid JWT token"));
        assert!(!is_auth_status(StatusCode::FORBIDDEN, "Forbidden"));
        assert!(!is_auth_status(StatusCode::NOT_FOUND, "gone"));
    }
}
