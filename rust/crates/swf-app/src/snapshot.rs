//! One pass over every source, and the rule that a dead source only blanks its own pane.
//!
//! This is the fan-out `swfactory herd` performs, ported so the two snapshots agree value for
//! value (`01-domain-control.md` §4). The boundary it defends is non-negotiable 4: each source is
//! read inside its own failure boundary, and a failure is recorded against *that source's* key
//! rather than thrown. `collect` therefore has no error return at all — a `Snapshot` with three
//! populated panes and one error string is a better answer than no answer, and it is the only
//! answer that lets an operator see that `gh` is missing while Airflow is fine.
//!
//! Two error boundaries are deliberately coarse and one is deliberately fine, exactly as the
//! Python has them:
//!
//! * listing the DAGs and listing every DAG's runs share **one** boundary (`airflow`), because a
//!   server that fails halfway through has not given us a run list we can reason about;
//! * each run's job rows get **their own** key (`airflow:{dag}/{run}`), and a failing run still
//!   contributes a collapsed row. The table never lies by omission.

use chrono::{DateTime, Utc};
use swf_adapters::error::AdapterError;
use swf_adapters::traits::{Deliveries, MetricsStore, Runs, Sandboxes};
use swf_domain::model::{
    Run, Snapshot, SourceHealth, SOURCE_AIRFLOW, SOURCE_GATES, SOURCE_GITHUB, SOURCE_ISLO,
    SOURCE_METRICS,
};
use swf_domain::rollup::collapsed_job;
use swf_domain::sanitize::sanitize_line;
use tokio_util::sync::CancellationToken;

use crate::context::DEFAULT_DAG_TAG;
use crate::gates;

/// How many runs of each DAG a pass reads. `herd`'s default, and the reason the Runs pane is
/// bounded work no matter how long the factory has been up.
pub const DEFAULT_RUNS_PER_DAG: usize = 20;

/// How many runs of each DAG get real job rows before the rest collapse to one line each.
pub const DEFAULT_JOBS_PER_DAG: usize = 5;

/// The label `gh pr list` filters on when nothing says otherwise.
pub const DEFAULT_PR_LABEL: &str = "factory";

/// How many pull requests a pass reads.
pub const DEFAULT_PR_LIMIT: u32 = 30;

/// What one pass is allowed to cost.
///
/// The two numbers are a cost policy, not a display preference: `runs_per_dag` bounds the reads,
/// and `jobs_per_dag` bounds the *expensive* ones, since every non-collapsed run costs two more
/// round trips. An active run is always expanded regardless of position — a running job is the
/// thing an operator opened the tool to look at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectOpts {
    /// The DAGs to read. `Some(list)` skips `list_dags` entirely, even when the list is empty.
    pub dag_ids: Option<Vec<String>>,
    /// The tag `list_dags` filters on when `dag_ids` is `None`.
    pub dag_tag: String,
    /// Runs per DAG.
    pub runs_per_dag: usize,
    /// Runs per DAG that get real job rows.
    pub jobs_per_dag: usize,
    /// The pull-request label.
    pub pr_label: String,
    /// How many pull requests to read.
    pub pr_limit: u32,
    /// Whether to establish gate readiness in the same pass (`00-architecture.md` §5).
    ///
    /// On by default: a gate list where `ready` is always false is worse than no gate list, because
    /// the operator cannot tell "not yet" from "we did not look".
    pub with_readiness: bool,
}

impl Default for CollectOpts {
    fn default() -> Self {
        Self {
            dag_ids: None,
            dag_tag: DEFAULT_DAG_TAG.to_string(),
            runs_per_dag: DEFAULT_RUNS_PER_DAG,
            jobs_per_dag: DEFAULT_JOBS_PER_DAG,
            pr_label: DEFAULT_PR_LABEL.to_string(),
            pr_limit: DEFAULT_PR_LIMIT,
            with_readiness: true,
        }
    }
}

impl CollectOpts {
    /// Discover DAGs by this tag when none are pinned.
    pub fn tagged(mut self, tag: &str) -> Self {
        if !tag.trim().is_empty() {
            self.dag_tag = tag.trim().to_string();
        }
        self
    }

    /// The DAG ids a context pins, or `None` to discover them by tag.
    pub fn for_dags(mut self, dag_ids: &[String]) -> Self {
        self.dag_ids = if dag_ids.is_empty() {
            None
        } else {
            Some(dag_ids.to_vec())
        };
        self
    }
}

/// The sources one pass may read. Any of them may be absent, and absence is not an error.
///
/// A context with no `repo` has no GitHub pane and no `github` error — there is nothing wrong,
/// there is simply nothing configured, and those two states must not render the same way.
#[derive(Default)]
pub struct Sources<'a> {
    /// Airflow. Runs, jobs and gates all come from here.
    pub airflow: Option<&'a dyn Runs>,
    /// GitHub, through `gh`.
    pub github: Option<&'a dyn Deliveries>,
    /// The sandbox provider.
    pub islo: Option<&'a dyn Sandboxes>,
    /// The committed metrics history.
    pub metrics: Option<&'a dyn MetricsStore>,
}

/// Read every source once and answer what was learned, plus what was not.
///
/// Never returns `Err`. The order of operations is the Python's, because `Snapshot.errors` is an
/// insertion-ordered document that a CI job byte-diffs: reordering the sources here would change
/// the JSON without changing a single fact.
pub async fn collect(
    sources: Sources<'_>,
    opts: &CollectOpts,
    now: DateTime<Utc>,
    cancel: &CancellationToken,
) -> Snapshot {
    let mut snap = Snapshot::new(now);

    if let Some(airflow) = sources.airflow {
        // One boundary around the DAG listing *and* every `list_runs`: a server that died between
        // two DAGs has not told us what the run list is, and pretending we have half of it is how
        // an operator concludes that four runs vanished.
        let listing = async {
            let ids = match &opts.dag_ids {
                Some(ids) => (ids.clone(), false),
                None => {
                    let page = airflow.list_dags(&opts.dag_tag, cancel).await?;
                    (page.rows, page.truncated)
                }
            };
            let mut runs: Vec<Run> = Vec::new();
            let mut truncated = ids.1;
            for dag_id in &ids.0 {
                let page = airflow.list_runs(dag_id, opts.runs_per_dag, cancel).await?;
                truncated = truncated || page.truncated;
                runs.extend(page.rows);
            }
            Ok::<(Vec<Run>, bool), AdapterError>((runs, truncated))
        }
        .await;

        match listing {
            Ok((runs, truncated)) => {
                snap.runs = runs;
                let mut health = SourceHealth::fresh(now);
                health.truncated = truncated;
                snap.health.insert(SOURCE_AIRFLOW.to_string(), health);
            }
            Err(err) => degrade(&mut snap, SOURCE_AIRFLOW, &err, now),
        }

        // Outside the boundary above on purpose: with an empty run list this is a no-op, and with
        // a partial one it still fills in what it can.
        expand_jobs(airflow, &mut snap, opts, now, cancel).await;

        // Gates are fetched even when the run listing failed. They are the only pane where a
        // human is *blocked*, and a broken `dagRuns` route is no reason to hide an approval.
        match airflow.pending_gates(cancel).await {
            Ok(page) => {
                snap.gates = page.rows;
                let mut health = SourceHealth::fresh(now);
                health.truncated = page.truncated;
                snap.health.insert(SOURCE_GATES.to_string(), health);
                if opts.with_readiness {
                    gates::mark_ready(airflow, &mut snap, cancel).await;
                }
            }
            Err(err) => degrade(&mut snap, SOURCE_GATES, &err, now),
        }
    }

    if let Some(github) = sources.github {
        match github.prs(&opts.pr_label, opts.pr_limit, cancel).await {
            Ok(prs) => {
                snap.prs = prs;
                snap.health
                    .insert(SOURCE_GITHUB.to_string(), SourceHealth::fresh(now));
            }
            Err(err) => degrade(&mut snap, SOURCE_GITHUB, &err, now),
        }
    }

    if let Some(islo) = sources.islo {
        match islo.list(cancel).await {
            Ok(sandboxes) => {
                snap.sandboxes = sandboxes;
                snap.health
                    .insert(SOURCE_ISLO.to_string(), SourceHealth::fresh(now));
            }
            Err(err) => degrade(&mut snap, SOURCE_ISLO, &err, now),
        }
    }

    if let Some(metrics) = sources.metrics {
        match metrics.summary(cancel).await {
            Ok(summary) => {
                snap.metrics = serde_json::to_value(&summary).unwrap_or_default();
                snap.health
                    .insert(SOURCE_METRICS.to_string(), SourceHealth::fresh(now));
            }
            Err(err) => degrade(&mut snap, SOURCE_METRICS, &err, now),
        }
    }

    snap
}

/// Give each run its job rows, under the cost policy `CollectOpts` describes.
async fn expand_jobs(
    airflow: &dyn Runs,
    snap: &mut Snapshot,
    opts: &CollectOpts,
    now: DateTime<Utc>,
    cancel: &CancellationToken,
) {
    // How many runs of each DAG have been seen so far, in API order (newest first).
    let mut position: Vec<(String, usize)> = Vec::new();
    for index in 0..snap.runs.len() {
        let (dag_id, run_ref, issues, active) = {
            let run = &snap.runs[index];
            (run.dag_id.clone(), run.id(), run.issues(), run.active())
        };
        let pos = match position.iter_mut().find(|(dag, _)| *dag == dag_id) {
            Some((_, seen)) => {
                let pos = *seen;
                *seen += 1;
                pos
            }
            None => {
                position.push((dag_id.clone(), 1));
                0
            }
        };

        if !(active || pos < opts.jobs_per_dag) {
            let collapsed = collapsed_job(&snap.runs[index]);
            snap.runs[index].jobs = vec![collapsed];
            continue;
        }

        match airflow.job_rows(&run_ref, &issues, cancel).await {
            Ok(page) if !page.rows.is_empty() => {
                snap.runs[index].jobs = page.rows;
                if page.truncated {
                    let key = run_key(&run_ref.dag_id, &run_ref.run_id);
                    let mut health = SourceHealth::fresh(now);
                    health.truncated = true;
                    snap.health.insert(key, health);
                    if let Some(airflow_health) = snap.health.get_mut(SOURCE_AIRFLOW) {
                        airflow_health.truncated = true;
                    }
                }
            }
            // An empty read is not an empty run: it means the tasks are not there yet, and one
            // honest line beats a run that disappears from the table.
            Ok(_) => {
                let collapsed = collapsed_job(&snap.runs[index]);
                snap.runs[index].jobs = vec![collapsed];
            }
            Err(err) => {
                let collapsed = collapsed_job(&snap.runs[index]);
                snap.runs[index].jobs = vec![collapsed];
                if !err.is_cancelled() {
                    let key = run_key(&run_ref.dag_id, &run_ref.run_id);
                    snap.set_error(&key, message(&err));
                    snap.health
                        .insert(key, SourceHealth::failed(now, message(&err)));
                }
            }
        }
    }
}

/// The error key one failing run is recorded under: `airflow:{dag_id}/{run_id}`.
pub fn run_key(dag_id: &str, run_id: &str) -> String {
    format!("{SOURCE_AIRFLOW}:{dag_id}/{run_id}")
}

/// Record a source failure without disturbing any other pane.
///
/// A cancelled read is not a failure and leaves no trace: the operator changed context, and the
/// answer is simply no longer wanted (`00-architecture.md` §D-A).
fn degrade(snap: &mut Snapshot, source: &str, err: &AdapterError, now: DateTime<Utc>) {
    if err.is_cancelled() {
        return;
    }
    let text = message(err);
    snap.set_error(source, text.clone());
    snap.health
        .insert(source.to_string(), SourceHealth::failed(now, text));
}

/// One line of service words, control characters removed (rule 7): these strings are rendered.
fn message(err: &AdapterError) -> String {
    sanitize_line(&err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_per_run_error_key_is_the_one_the_python_writes() {
        assert_eq!(run_key("factory", "r-running"), "airflow:factory/r-running");
    }

    #[test]
    fn a_cancelled_read_leaves_no_error_behind() {
        let now = Utc::now();
        let mut snap = Snapshot::new(now);
        degrade(&mut snap, SOURCE_GITHUB, &AdapterError::Cancelled, now);
        assert!(snap.healthy(), "cancellation is the caller's own doing");
        assert!(snap.health.is_empty());
    }

    #[test]
    fn an_explicit_dag_list_disables_discovery_even_when_empty() {
        let opts = CollectOpts::default().for_dags(&[]);
        assert_eq!(opts.dag_ids, None, "an empty context list means 'by tag'");
        let opts = CollectOpts::default().for_dags(&["hotfix".to_string()]);
        assert_eq!(opts.dag_ids.as_deref(), Some(&["hotfix".to_string()][..]));
    }

    #[test]
    fn the_defaults_are_herds() {
        let opts = CollectOpts::default();
        assert_eq!(opts.runs_per_dag, 20);
        assert_eq!(opts.jobs_per_dag, 5);
        assert_eq!(opts.pr_label, "factory");
        assert_eq!(opts.pr_limit, 30);
        assert_eq!(opts.dag_tag, "swfactory");
    }
}
