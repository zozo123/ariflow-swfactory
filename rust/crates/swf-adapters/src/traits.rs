//! The vocabulary the rest of the product is written against, so no transport ever leaks upward.
//!
//! One trait per source. `swf-app` holds `Arc<dyn Runs>`, `Arc<dyn Deliveries>` and friends
//! (`00-architecture.md` §D-A), which is what lets the TUI swap a whole environment at runtime and
//! lets a test drive the entire product with a five-line fake and no network. The boundary this
//! module defends is that direction: nothing above here may name `reqwest`, an argv, or a status
//! code.
//!
//! Two rules are baked into the signatures rather than left to discipline:
//!
//! * **Every I/O method takes a [`CancellationToken`].** A context switch or a change of selected
//!   run cancels the token and issues a fresh one, so an in-flight refresh cannot land after — and
//!   overwrite — a newer one. An abandoned call answers [`AdapterError::Cancelled`], which is not
//!   a failure.
//! * **Every collection read answers a [`Page`], not a bare `Vec`.** Non-negotiable 3 of the
//!   architecture: a read that stopped at its bound must *say so*. Returning `Vec` would make
//!   silently hiding jobs the path of least resistance, and hiding jobs is the failure mode this
//!   whole binary exists to replace.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use serde_json::Value;
use swf_domain::ids::{GateId, JobId, RunRef};
use swf_domain::metrics::{MetricsSummary, RunMetrics};
use swf_domain::model::{
    parse_timestamp, Gate, IssueRef, JobRow, PullRequest, Run, SandboxRef, TaskState, Timestamp,
};
use tokio_util::sync::CancellationToken;

use crate::error::Result;

/// How long a subprocess may run before it is a hang. `_SUBPROCESS_TIMEOUT_S` in the Python, and
/// deliberately unrelated to the HTTP timeout: `gh` waiting on a slow API is normal, an HTTP call
/// taking two minutes is not.
pub const SUBPROCESS_TIMEOUT: Duration = Duration::from_secs(120);

/// The HTTP default, `DEFAULT_TIMEOUT_S = 15.0` in `01-domain-control.md` §1. `--timeout`
/// overrides this one and never the subprocess bound.
pub const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_millis(15_000);

/// A collection read, and whether it is the whole collection.
///
/// `truncated` is the honest half. A read that hit its page bound has *some* of the rows and no
/// way to know how many it does not have; rendering those rows without the flag would be the
/// table lying by omission, which is the one thing the jobs table may never do.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Page<T> {
    /// The rows actually read, in the order the service returned them.
    pub rows: Vec<T>,
    /// True when the loop stopped at [`crate::airflow::MAX_PAGES`] with more rows outstanding.
    pub truncated: bool,
}

impl<T> Page<T> {
    /// A complete read.
    pub fn whole(rows: Vec<T>) -> Self {
        Self {
            rows,
            truncated: false,
        }
    }

    /// A read that stopped early, and admits it.
    pub fn partial(rows: Vec<T>) -> Self {
        Self {
            rows,
            truncated: true,
        }
    }

    /// How many rows were read.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// True when nothing was read.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Map the rows, keeping the truncation flag — losing it in a `map` is exactly the mistake
    /// this type exists to prevent.
    pub fn map<U>(self, f: impl FnMut(T) -> U) -> Page<U> {
        Page {
            rows: self.rows.into_iter().map(f).collect(),
            truncated: self.truncated,
        }
    }
}

impl<T> From<Page<T>> for Vec<T> {
    fn from(page: Page<T>) -> Self {
        page.rows
    }
}

/// One poll of one task attempt's log.
///
/// Airflow has no streaming log endpoint (`03-airflow-rest.md` §10, gotchas 18–19): `--follow` is a
/// poll loop, and [`LogPage::continuation_token`] is the whole of its state. A `None` token is the
/// *only* end-of-log signal on the wire, so it is modelled as `Option` and not as a bool that
/// could be computed wrongly.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogPage {
    /// The newly appended lines, already sanitised — see [`crate::airflow`] on why the scrubbing
    /// happens here and not at the renderer.
    pub lines: Vec<String>,
    /// The opaque token to send on the next poll, or `None` once the log is complete. Never parse,
    /// synthesise or persist it across servers: it is signed with the API server's secret key.
    pub continuation_token: Option<String>,
}

impl LogPage {
    /// True when Airflow says there will be nothing more for this attempt.
    pub fn complete(&self) -> bool {
        self.continuation_token.is_none()
    }
}

/// Everything `swf` reads and writes through Airflow's public REST API.
///
/// The unit is the job, so the identity types are [`JobId`]/[`GateId`]/[`RunRef`] and never a row
/// index. `run_url` is not async because it builds a link and asks nobody.
#[async_trait]
pub trait Runs: Send + Sync {
    /// The DAG ids carrying `tag`. An empty tag means "every DAG the server will show".
    async fn list_dags(&self, tag: &str, cancel: &CancellationToken) -> Result<Page<String>>;

    /// The newest `limit` runs of one DAG, newest first (`order_by=-run_after`).
    async fn list_runs(
        &self,
        dag_id: &str,
        limit: usize,
        cancel: &CancellationToken,
    ) -> Result<Page<Run>>;

    /// Every task instance of one run. Paged: a run with more than one page of tasks is normal.
    async fn task_states(
        &self,
        run: &RunRef,
        cancel: &CancellationToken,
    ) -> Result<Page<TaskState>>;

    /// The `fan_out` XCom, i.e. the job list this run was expanded into.
    ///
    /// A run that has not fanned out yet has no XCom, and that 404 is a normal state of the world,
    /// not a failure: this answers an empty `Vec` (`03-airflow-rest.md` §6, gotcha 7).
    async fn fan_out_jobs(&self, run: &RunRef, cancel: &CancellationToken) -> Result<Vec<Value>>;

    /// The job rows of one run: exactly two bounded reads, rolled up by `swf_domain::rollup`.
    ///
    /// `fallback` is the run's own `conf` issue list, so a row can still name its issue when the
    /// XCom is missing.
    async fn job_rows(
        &self,
        run: &RunRef,
        fallback: &[String],
        cancel: &CancellationToken,
    ) -> Result<Page<JobRow>>;

    /// Every HITL detail still waiting for an answer, across every DAG.
    async fn pending_gates(&self, cancel: &CancellationToken) -> Result<Page<Gate>>;

    /// One poll of one task attempt's log. `token` continues a previous poll; `None` starts one.
    async fn logs(
        &self,
        job: &JobId,
        task: &str,
        attempt: u32,
        token: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<LogPage>;

    /// Answer one gate. A `409` here means someone else answered first — a normal outcome that
    /// surfaces as [`AdapterError::Conflict`](crate::error::AdapterError::Conflict), never a crash.
    async fn respond(&self, gate: &GateId, approve: bool, cancel: &CancellationToken)
        -> Result<()>;

    /// Trigger a run for `issues` and return the run id the server assigned.
    async fn trigger(
        &self,
        dag_id: &str,
        issues: &[String],
        cancel: &CancellationToken,
    ) -> Result<String>;

    /// Mark the run failed.
    ///
    /// This is all Airflow offers, and the name says it: nothing is killed, no sandbox is cleaned
    /// up, and any task already running keeps running. Non-negotiable 9.
    async fn stop_run(&self, run: &RunRef, cancel: &CancellationToken) -> Result<()>;

    /// Unpause a DAG. `[core] dags_are_paused_at_creation` defaults to true, so a freshly parsed
    /// blueprint sits `queued` forever until something does this (`03-airflow-rest.md` §12a).
    async fn unpause_dag(&self, dag_id: &str, cancel: &CancellationToken) -> Result<()>;

    /// The unauthenticated health probe `swf doctor` uses to tell "wrong URL" from "wrong token".
    async fn health(&self, cancel: &CancellationToken) -> Result<Value>;

    /// The UI deep link for a run. Not an API call, so a just-triggered run can be linked before
    /// it appears in any snapshot.
    fn run_url(&self, run: &RunRef) -> String;
}

/// One pull request as `gh` reports it for a branch — the "does this delivery exist?" answer.
///
/// Narrower than [`PullRequest`] on purpose: `06-delivery-evidence.md` §4.3 asks a different
/// question here (`baseRefName`, `headRefOid`) than the herd table does.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Deserialize)]
pub struct PrHead {
    /// The PR's web URL.
    pub url: String,
    /// `OPEN` / `MERGED` / `CLOSED`.
    pub state: String,
    /// Untrusted: render through `swf_domain::sanitize`.
    pub title: String,
    /// Label names, flattened from `gh`'s objects.
    pub labels: Vec<String>,
    /// The head commit sha the PR points at.
    pub head_sha: String,
    /// The branch this PR merges into — checked against the blueprint's `base_branch`.
    pub base_ref: String,
}

/// What the factory published, as GitHub knows it.
#[async_trait]
pub trait Deliveries: Send + Sync {
    /// Pull requests carrying `label`, newest `limit`.
    async fn prs(
        &self,
        label: &str,
        limit: u32,
        cancel: &CancellationToken,
    ) -> Result<Vec<PullRequest>>;

    /// Issues carrying `label`, newest `limit`.
    async fn issues(
        &self,
        label: &str,
        limit: u32,
        cancel: &CancellationToken,
    ) -> Result<Vec<IssueRef>>;

    /// The PR whose head is `branch`, if there is one. `None` is a real answer — a delivery that
    /// published a branch but never opened a PR.
    async fn pr_for_branch(
        &self,
        branch: &str,
        cancel: &CancellationToken,
    ) -> Result<Option<PrHead>>;

    /// The rolled-up check summary for one PR, in `summarize_checks` form.
    async fn checks(&self, number: i64, cancel: &CancellationToken) -> Result<String>;

    /// Open one PR in the operator's browser, and answer the argv that was run so the CLI can
    /// print what it did rather than claim something it cannot observe.
    async fn pr_view(&self, number: i64, cancel: &CancellationToken) -> Result<Vec<String>>;
}

/// The sandboxes the factory may have created — and, much more importantly, the ones it did not.
#[async_trait]
pub trait Sandboxes: Send + Sync {
    /// The sandboxes the configured owner created, excluding ones already deleted.
    async fn list(&self, cancel: &CancellationToken) -> Result<Vec<SandboxRef>>;

    /// Remove one sandbox, after re-reading the provider's listing to confirm it may be removed.
    ///
    /// The guard is the point of this method; see [`crate::islo`] for why it re-lists rather than
    /// trusting the snapshot the operator was looking at.
    async fn remove(&self, name: &str, cancel: &CancellationToken) -> Result<Vec<String>>;
}

/// The committed metrics history, read from a checkout rather than from a service.
#[async_trait]
pub trait MetricsStore: Send + Sync {
    /// Every `metrics.json` under the root, oldest first.
    async fn runs(&self, cancel: &CancellationToken) -> Result<Vec<RunMetrics>>;

    /// The aggregate over [`MetricsStore::runs`].
    async fn summary(&self, cancel: &CancellationToken) -> Result<MetricsSummary>;
}

/// What one subprocess produced.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CommandOutput {
    /// The exit status, or `-1` when the process was killed by a signal.
    pub code: i32,
    /// Standard output, verbatim — the caller parses it, so nothing is trimmed here.
    pub stdout: String,
    /// Standard error, verbatim.
    pub stderr: String,
}

impl CommandOutput {
    /// The error text the Python client uses: stderr if it has anything, else stdout.
    pub fn message(&self) -> String {
        let err = self.stderr.trim();
        if err.is_empty() {
            self.stdout.trim().to_string()
        } else {
            err.to_string()
        }
    }
}

/// How `gh` and `islo` are actually run — injectable so a test can assert the argv without a fork.
///
/// The argv is a `Vec<String>` and never a shell string. Delivery branch names, issue ids and
/// sandbox names all come from outside this process; one of them containing `; rm -rf` must be a
/// nonsense argument and not a command.
#[async_trait]
pub trait CommandRunner: Send + Sync {
    /// Run `argv` to completion, or fail with
    /// [`AdapterError::Timeout`](crate::error::AdapterError::Timeout) after `timeout`.
    async fn run(
        &self,
        argv: &[String],
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<CommandOutput>;
}

/// The runner everything but a test uses: a real process, with a real deadline.
///
/// It lives beside the trait rather than inside `gh` or `islo` because both need it and neither
/// owns it. Killing the child on timeout matters more than it looks: `gh` waiting on a hung proxy
/// would otherwise hold the operator's terminal open with no way back.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemRunner;

#[async_trait]
impl CommandRunner for SystemRunner {
    async fn run(
        &self,
        argv: &[String],
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<CommandOutput> {
        let Some((program, args)) = argv.split_first() else {
            return Err(crate::error::AdapterError::refused("empty command"));
        };
        let mut command = tokio::process::Command::new(program);
        command.args(args).kill_on_drop(true);

        let spawned = async {
            let output = command.output().await.map_err(|e| {
                // A tool that is not installed is unreachable, not a failed request: the fix is
                // "install it", and `swf doctor` says so only if the classification is right.
                crate::error::AdapterError::Unreachable {
                    what: program.clone(),
                    detail: crate::error::truncate(&e.to_string()),
                }
            })?;
            Ok(CommandOutput {
                code: output.status.code().unwrap_or(-1),
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            })
        };

        tokio::select! {
            biased;
            () = cancel.cancelled() => Err(crate::error::AdapterError::Cancelled),
            elapsed = tokio::time::sleep(timeout) => {
                let () = elapsed;
                Err(crate::error::AdapterError::Timeout { what: program.clone(), after: timeout })
            }
            out = spawned => out,
        }
    }
}

/// Read a timestamp the way `metrics.parse_ts` does, because half the services spell it
/// differently and none of them is worth failing a whole snapshot over.
///
/// The string arm is delegated to `swf_domain::model::parse_timestamp` on purpose: two ISO parsers
/// in one binary is two chances to disagree about what `2026-09-03T14:00:00+02:00` means, and the
/// snapshot document is a byte-diff target. What is added here is the numeric arm — `metrics.parse_ts`
/// also accepts epoch **seconds**, which is how some `islo` builds report `created_at`.
///
/// A bool is deliberately not a timestamp: Python excludes it explicitly, because `True` is not
/// epoch 1 and a provider that returns `"created": true` is telling us nothing.
pub fn parse_ts(value: &Value) -> Option<Timestamp> {
    match value {
        Value::Number(number) => epoch_seconds(number.as_f64()?),
        other => parse_timestamp(other),
    }
}

/// The string arm alone, for a caller that already has a `&str`.
pub fn parse_ts_str(text: &str) -> Option<Timestamp> {
    parse_timestamp(&Value::String(text.to_string()))
}

/// Epoch seconds, fractional part included, as an offset-aware instant.
fn epoch_seconds(seconds: f64) -> Option<Timestamp> {
    if !seconds.is_finite() {
        return None;
    }
    let whole = seconds.trunc();
    let nanos = ((seconds - whole) * 1e9).round().clamp(0.0, 999_999_999.0) as u32;
    match Utc.timestamp_opt(whole as i64, nanos) {
        chrono::LocalResult::Single(dt) => Some(dt.fixed_offset()),
        _ => None,
    }
}

/// The first of `keys` whose value parses as a timestamp.
///
/// A present-but-unparseable key does **not** stop the scan (`01-domain-control.md` §7.3): `islo`
/// and `gh` have each spelled "created" differently over the years, and one of them writing
/// garbage is not a reason to give up on the others.
pub fn first_timestamp(scope: &Value, keys: &[&str]) -> Option<Timestamp> {
    let object = scope.as_object()?;
    keys.iter()
        .filter_map(|key| object.get(*key))
        .find_map(parse_ts)
}

/// The spellings of "created" seen in the wild (`metrics.CREATED_KEYS`), in priority order.
pub const CREATED_KEYS: &[&str] = &["created_at", "createdAt", "created-at", "created"];

/// The spellings of "finished" (`metrics._FINISHED_KEYS`), in priority order.
pub const FINISHED_KEYS: &[&str] = &["finished", "finished_at", "end", "ended_at"];

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_partial_page_stays_partial_through_a_map() {
        let page = Page::partial(vec![1, 2, 3]).map(|n| n * 2);
        assert!(
            page.truncated,
            "losing the flag in a map is how the table starts lying"
        );
        assert_eq!(page.rows, vec![2, 4, 6]);
        assert_eq!(page.len(), 3);
        assert!(!page.is_empty());
    }

    #[test]
    fn a_log_page_is_complete_only_when_the_token_is_gone() {
        let more = LogPage {
            lines: vec!["a".into()],
            continuation_token: Some("t".into()),
        };
        assert!(!more.complete());
        assert!(LogPage::default().complete());
    }

    #[test]
    fn parse_ts_accepts_both_spellings_of_utc() {
        let z = parse_ts(&json!("2026-09-03T08:18:24.904858Z"));
        let offset = parse_ts(&json!("2026-09-03T08:18:24.904858+00:00"));
        assert_eq!(z, offset);
        assert!(z.is_some());
    }

    #[test]
    fn parse_ts_reads_epoch_seconds_but_never_a_bool() {
        let epoch = parse_ts(&json!(1_756_886_304)).map(|d| d.timestamp());
        assert_eq!(epoch, Some(1_756_886_304));
        assert!(parse_ts(&json!(true)).is_none(), "True is not epoch 1");
        assert!(parse_ts(&json!(false)).is_none());
    }

    #[test]
    fn a_naive_stamp_is_read_as_utc_and_a_date_as_midnight() {
        let naive = parse_ts(&json!("2026-09-03T08:18:24")).map(|d| d.to_rfc3339());
        assert_eq!(naive.as_deref(), Some("2026-09-03T08:18:24+00:00"));
        let date = parse_ts(&json!("2026-09-03")).map(|d| d.to_rfc3339());
        assert_eq!(date.as_deref(), Some("2026-09-03T00:00:00+00:00"));
    }

    #[test]
    fn unparseable_and_wrong_typed_stamps_answer_none_rather_than_erroring() {
        for bad in [
            json!(""),
            json!("not a date"),
            json!(null),
            json!([]),
            json!({}),
        ] {
            assert!(parse_ts(&bad).is_none(), "{bad:?} must degrade, not fail");
        }
    }

    #[test]
    fn first_timestamp_skips_a_broken_key_and_keeps_looking() {
        let scope = json!({"created_at": "nonsense", "createdAt": "2026-09-03T08:18:24Z"});
        let found = first_timestamp(&scope, CREATED_KEYS).map(|d| d.timestamp());
        assert_eq!(found, Some(1_788_423_504));
        assert!(first_timestamp(&json!({}), CREATED_KEYS).is_none());
        assert!(first_timestamp(&json!("not an object"), CREATED_KEYS).is_none());
    }

    #[test]
    fn a_command_reports_stderr_when_it_has_any_and_stdout_otherwise() {
        let out = CommandOutput {
            code: 1,
            stdout: "some output\n".into(),
            stderr: "  boom  \n".into(),
        };
        assert_eq!(out.message(), "boom");
        let quiet = CommandOutput {
            code: 1,
            stdout: " fell over ".into(),
            stderr: "   ".into(),
        };
        assert_eq!(quiet.message(), "fell over");
    }
}
