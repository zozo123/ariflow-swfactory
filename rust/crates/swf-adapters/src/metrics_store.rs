//! The committed metrics history, read off a checkout instead of asked of a service.
//!
//! Every factory run commits `docs/factory/<issue_id>/metrics.json` as part of its delivery, so
//! the history of the fleet is in git and not in a database somebody has to keep alive. That is
//! why this adapter exists at all: `swf metrics` on a laptop with no network still answers.
//!
//! The boundary defended here is *tolerance*. These files were written by past versions of the
//! factory and will be read by future ones. A file that is unreadable, is not JSON, is not an
//! object, or has no `run_id` is skipped in silence; a file whose numbers arrive as strings is
//! read leniently by `swf_domain::metrics`. One bad artifact from a run that failed halfway must
//! never be able to make `swf metrics` refuse to answer at all.
//!
//! The discovery order is not a detail either. `load_all` sorts candidate *paths*
//! lexicographically, applies the [`MAX_RUNS`] bound to that sorted list — so a malformed file
//! still consumes a slot — and then orders the accepted runs by `(finished, issue_id)`. Any other
//! order produces a different `p50_cycle_s` on the same checkout, and the aggregate is supposed to
//! be reproducible.

use std::fs;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde_json::Value;
use swf_domain::metrics::{summarize, MetricsSummary, RunMetrics};
use tokio_util::sync::CancellationToken;

use crate::error::{AdapterError, Result};
use crate::traits::{first_timestamp, MetricsStore, FINISHED_KEYS};

/// How many candidate files one scan will look at. `metrics._MAX_RUNS`.
pub const MAX_RUNS: usize = 10_000;

/// The directory a run's artifacts are committed under, relative to a target checkout.
pub const ARTIFACT_DIR: &str = "docs/factory";

/// The file name every run writes.
pub const METRICS_FILE: &str = "metrics.json";

/// How deep the `**` of `**/docs/factory/*/metrics.json` will descend.
///
/// Python's `Path.glob` is unbounded; a bound is added here because `swf metrics --root /` is a
/// typo an operator can make, and walking a whole filesystem is not a useful answer to it.
pub const MAX_DEPTH: usize = 24;

/// The committed metrics tree of one checkout.
#[derive(Debug, Clone)]
pub struct FsMetrics {
    root: PathBuf,
    include_scripted: bool,
    newest_first: bool,
}

impl FsMetrics {
    /// Read every run under `root`, scripted replays included, oldest first — what `swf metrics`
    /// aggregates.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            include_scripted: true,
            newest_first: false,
        }
    }

    /// Drop `agent == "scripted"` records: demo replays are real files but not real work, and a
    /// fleet report that counts them flatters itself.
    pub fn without_scripted(mut self) -> Self {
        self.include_scripted = false;
        self
    }

    /// Order newest first, for a "what happened lately" view rather than an aggregate.
    pub fn newest_first(mut self) -> Self {
        self.newest_first = true;
        self
    }

    /// The checkout being read.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Every `metrics.json` under the root, in `**/docs/factory/*/metrics.json` order.
    ///
    /// Returned sorted by full path string, because that is the list [`MAX_RUNS`] truncates and a
    /// different sort would truncate a different set of runs.
    pub fn discover(&self) -> Vec<PathBuf> {
        let mut found = Vec::new();
        collect(&self.root, 0, &mut found);
        found.sort();
        found.truncate(MAX_RUNS);
        found
    }

    /// Read and order the runs. Blocking: call it through [`MetricsStore::runs`] in async code.
    pub fn load(&self) -> Vec<RunMetrics> {
        let mut rows: Vec<(f64, String, RunMetrics)> = Vec::new();
        for path in self.discover() {
            let Ok(text) = fs::read_to_string(&path) else {
                continue;
            };
            let Ok(data) = serde_json::from_str::<Value>(&text) else {
                continue;
            };
            // A JSON document that is not an object, or an object with no `run_id`, is some other
            // file that happens to be called metrics.json. It is not this factory's.
            if !data.is_object() || data.get("run_id").is_none() {
                continue;
            }
            let agent = data
                .get("agent")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !self.include_scripted && agent == "scripted" {
                continue;
            }
            let issue_id = path
                .parent()
                .and_then(|p| p.file_name())
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let epoch = finished_epoch(&data, &path);
            // Every field defaults and the numbers deserialize leniently, so this only fails on a
            // structurally wrong document — and even then the run still counts, at zero, because
            // dropping it would quietly change `runs` and every rate derived from it.
            let run: RunMetrics = serde_json::from_value(data).unwrap_or_default();
            rows.push((epoch, issue_id, run));
        }

        // `(finished, issue_id)` is the total order; the issue id is the deterministic tie-break
        // for the many runs that finish inside the same second.
        rows.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        });
        if self.newest_first {
            rows.reverse();
        }
        rows.into_iter().map(|(_, _, run)| run).collect()
    }
}

/// Walk for `**/docs/factory/*/metrics.json`.
///
/// `**` is zero or more directories, so `root/docs/factory/42/metrics.json` matches as well as
/// `root/demo/target/docs/factory/42/metrics.json`. Symlinked directories are not followed, which
/// is both what `Path.glob` does and the only way this terminates on a checkout with a self-link.
fn collect(dir: &Path, depth: usize, found: &mut Vec<PathBuf>) {
    if depth > MAX_DEPTH {
        return;
    }
    let artifacts = dir.join(ARTIFACT_DIR);
    if artifacts.is_dir() {
        if let Ok(entries) = fs::read_dir(&artifacts) {
            for entry in entries.flatten() {
                let candidate = entry.path().join(METRICS_FILE);
                // `*` matches exactly one component, so only a direct child of `factory/` counts.
                if entry.path().is_dir() && candidate.is_file() {
                    found.push(candidate);
                }
            }
        }
    }
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(meta) = entry.file_type() else {
            continue;
        };
        if meta.is_dir() && !meta.is_symlink() {
            collect(&path, depth + 1, found);
        }
    }
}

/// When this run finished, as an epoch, for the sort.
///
/// The run's own keys win, then a nested `timestamps` object, then the file's mtime, then `0.0`.
/// The mtime fallback is what keeps a run that never wrote a `finished` stamp — because it was
/// killed — in a sensible place in the history rather than at the very beginning of time.
fn finished_epoch(data: &Value, path: &Path) -> f64 {
    if let Some(ts) = first_timestamp(data, FINISHED_KEYS) {
        return ts.timestamp() as f64 + f64::from(ts.timestamp_subsec_nanos()) / 1e9;
    }
    if let Some(nested) = data.get("timestamps") {
        if let Some(ts) = first_timestamp(nested, FINISHED_KEYS) {
            return ts.timestamp() as f64 + f64::from(ts.timestamp_subsec_nanos()) / 1e9;
        }
    }
    fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0.0, |d| d.as_secs_f64())
}

#[async_trait]
impl MetricsStore for FsMetrics {
    async fn runs(&self, cancel: &CancellationToken) -> Result<Vec<RunMetrics>> {
        let store = self.clone();
        // A directory walk is blocking work. Doing it on the runtime would stall every other
        // refresh in the TUI for as long as the checkout is large.
        let work = tokio::task::spawn_blocking(move || store.load());
        tokio::select! {
            biased;
            () = cancel.cancelled() => Err(AdapterError::Cancelled),
            joined = work => joined.map_err(|e| AdapterError::Decode {
                what: "metrics scan".to_string(),
                detail: crate::error::truncate(&e.to_string()),
            }),
        }
    }

    async fn summary(&self, cancel: &CancellationToken) -> Result<MetricsSummary> {
        Ok(summarize(&self.runs(cancel).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write(root: &Path, relative: &str, body: &str) {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("test fixture dirs");
        }
        fs::write(path, body).expect("test fixture file");
    }

    fn record(run_id: &str, finished: &str, cycle: f64) -> String {
        format!(
            r#"{{"run_id": "{run_id}", "issue_id": "42", "agent": "claude",
                 "finished": "{finished}", "cycle_s": {cycle}, "iterations": 2,
                 "first_pass_ci": true, "tests_passed": true, "total_cost_usd": 1.5,
                 "findings_by_severity": {{"blocker": 0, "major": 1, "minor": 0, "nit": 2}}}}"#
        )
    }

    #[test]
    fn the_glob_matches_at_the_root_and_at_any_depth() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        write(
            root,
            "docs/factory/1/metrics.json",
            &record("r1", "2026-01-01T00:00:00Z", 10.0),
        );
        write(
            root,
            "demo/target/docs/factory/2/metrics.json",
            &record("r2", "2026-01-02T00:00:00Z", 20.0),
        );
        // `*` matches exactly one component: a deeper nesting under `factory/` is not a run.
        write(
            root,
            "docs/factory/3/sub/metrics.json",
            &record("r3", "2026-01-03T00:00:00Z", 30.0),
        );
        // The file name is part of the pattern.
        write(
            root,
            "docs/factory/4/other.json",
            &record("r4", "2026-01-04T00:00:00Z", 40.0),
        );

        let found = FsMetrics::new(root).discover();
        assert_eq!(found.len(), 2, "found {found:?}");
        let runs = FsMetrics::new(root).load();
        let ids: Vec<&str> = runs.iter().map(|r| r.run_id.as_str()).collect();
        assert_eq!(ids, vec!["r1", "r2"], "oldest first by `finished`");
    }

    #[test]
    fn ordering_is_by_finished_then_issue_id_and_reverses_wholesale() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        // Same instant, different issue directories: the directory name is the tie-break.
        write(
            root,
            "docs/factory/b/metrics.json",
            &record("second", "2026-01-01T00:00:00Z", 1.0),
        );
        write(
            root,
            "docs/factory/a/metrics.json",
            &record("first", "2026-01-01T00:00:00Z", 1.0),
        );
        write(
            root,
            "docs/factory/c/metrics.json",
            &record("third", "2026-06-01T00:00:00Z", 1.0),
        );

        let oldest: Vec<String> = FsMetrics::new(root)
            .load()
            .into_iter()
            .map(|r| r.run_id)
            .collect();
        assert_eq!(oldest, vec!["first", "second", "third"]);

        let newest: Vec<String> = FsMetrics::new(root)
            .newest_first()
            .load()
            .into_iter()
            .map(|r| r.run_id)
            .collect();
        assert_eq!(
            newest,
            vec!["third", "second", "first"],
            "both key parts reverse"
        );
    }

    #[test]
    fn a_run_with_no_finished_stamp_falls_back_to_the_nested_timestamps_object() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        write(
            root,
            "docs/factory/1/metrics.json",
            r#"{"run_id": "nested", "timestamps": {"ended_at": "2020-01-01T00:00:00Z"}}"#,
        );
        write(
            root,
            "docs/factory/2/metrics.json",
            &record("stamped", "2026-01-01T00:00:00Z", 1.0),
        );
        let ids: Vec<String> = FsMetrics::new(root)
            .load()
            .into_iter()
            .map(|r| r.run_id)
            .collect();
        assert_eq!(ids, vec!["nested", "stamped"]);
    }

    #[test]
    fn junk_files_are_skipped_in_silence_rather_than_failing_the_report() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        write(root, "docs/factory/1/metrics.json", "{not json");
        write(root, "docs/factory/2/metrics.json", "[1, 2, 3]");
        write(
            root,
            "docs/factory/3/metrics.json",
            r#"{"agent": "claude"}"#,
        );
        write(
            root,
            "docs/factory/4/metrics.json",
            &record("good", "2026-01-01T00:00:00Z", 5.0),
        );

        let runs = FsMetrics::new(root).load();
        assert_eq!(runs.len(), 1, "one real run among four files");
        assert_eq!(runs[0].run_id, "good");
    }

    #[test]
    fn scripted_replays_can_be_excluded_from_a_fleet_report() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        write(
            root,
            "docs/factory/1/metrics.json",
            &record("real", "2026-01-01T00:00:00Z", 5.0),
        );
        write(
            root,
            "docs/factory/2/metrics.json",
            r#"{"run_id": "demo", "agent": "scripted", "finished": "2026-01-02T00:00:00Z"}"#,
        );

        assert_eq!(FsMetrics::new(root).load().len(), 2);
        let real = FsMetrics::new(root).without_scripted().load();
        assert_eq!(real.len(), 1);
        assert_eq!(real[0].run_id, "real");
    }

    #[test]
    fn a_missing_root_is_an_empty_history_and_not_a_crash() {
        let runs = FsMetrics::new("/nonexistent/path/for/swf/tests").load();
        assert!(runs.is_empty());
    }

    #[tokio::test]
    async fn the_summary_is_the_domain_aggregate_over_what_was_found() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        write(
            root,
            "docs/factory/1/metrics.json",
            &record("a", "2026-01-01T00:00:00Z", 10.0),
        );
        write(
            root,
            "docs/factory/2/metrics.json",
            &record("b", "2026-01-02T00:00:00Z", 20.0),
        );

        let store = FsMetrics::new(root);
        let summary = store
            .summary(&CancellationToken::new())
            .await
            .expect("a summary");
        assert_eq!(summary.runs, 2);
        assert_eq!(summary.scripted_runs, 0);
        assert_eq!(
            summary.p50_cycle_s, 15.0,
            "the mean of the two middle values"
        );
        assert_eq!(summary.mean_iterations, 2.0);
        assert_eq!(summary.total_cost_usd, 3.0);
        assert_eq!(summary.findings_by_severity.major, 2);
        assert_eq!(summary.blockers, 0);
    }

    #[tokio::test]
    async fn a_cancelled_scan_answers_cancelled_rather_than_an_empty_history() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let err = FsMetrics::new(".")
            .runs(&cancel)
            .await
            .expect_err("cancelled");
        assert!(
            err.is_cancelled(),
            "an empty list here would read as 'no runs'"
        );
    }

    #[test]
    fn numbers_written_as_strings_by_an_older_factory_still_read() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        write(
            root,
            "docs/factory/1/metrics.json",
            r#"{"run_id": "old", "cycle_s": "12.5", "iterations": "3",
                "first_pass_ci": 1, "total_cost_usd": "0.25"}"#,
        );
        let runs = FsMetrics::new(root).load();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].cycle_s, 12.5);
        assert_eq!(runs[0].iterations, 3);
        assert!(runs[0].first_pass_ci);
        assert_eq!(runs[0].total_cost_usd, 0.25);
    }
}
