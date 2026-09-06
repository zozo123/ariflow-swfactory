//! The roll-ups: how a pile of task instances becomes the one word an operator reads.
//!
//! Every function here is a port of a Python function that already decides what the factory
//! *looks like* — `control.job_state`, `control.group_jobs`, `herd.stage_progress` and friends.
//! The boundary this module defends is equivalence: a fixture generated from the Python is
//! replayed against these functions (`tests/contract.rs`), so a tie-break that looks arbitrary
//! here is load-bearing there. Simplifying one is not a cleanup, it is a behaviour change.
//!
//! Two rules recur and are worth stating once. Failure always wins a roll-up, because a job with
//! one failed task is a job someone has to look at. And a state Airflow has not reported is the
//! literal word `"none"`, not an absence — `states::or_none` makes the set-membership tests below
//! total, which is why an unknown state falls through to "queued" instead of crashing.

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::model::{JobRow, Run, TaskState, NO_ISSUE};
use crate::states;

/// The stage order the shipped DAGs create tasks in, used to pick a job's *frontier*.
///
/// It is a display order, not a schedule: Airflow decides what runs when. Its job here is to
/// answer "which of these stages is the one to name?" deterministically, so two operators looking
/// at the same run read the same word. Stages a blueprint never declares simply do not appear.
pub const TASK_ORDER: &[&str] = &[
    "setup",
    "intent",
    "approve_intent",
    "record_intent",
    "spec",
    "plan",
    "approve_plan",
    "record_plan",
    "build_and_test",
    "review",
    "deliver",
    "metrics",
    "teardown",
];

/// The label `stage_progress` uses when a job has started nothing and finished nothing.
pub const PENDING_STAGE: &str = "pending";

/// The label `stage_progress` returns for a job with no task instances at all.
pub const NO_STAGE: &str = "-";

/// GitHub check verdicts that count as green. `NEUTRAL` and `SKIPPED` are here because a check
/// that declined to run has not failed, and colouring it red trains operators to ignore red.
const CHECK_PASS: &[&str] = &["SUCCESS", "NEUTRAL", "SKIPPED"];

/// GitHub check verdicts that count as red. Everything unrecognised is *pending*, not failed —
/// guessing "failed" for a verdict GitHub invented last week would block a good delivery.
const CHECK_FAIL: &[&str] = &[
    "FAILURE",
    "ERROR",
    "TIMED_OUT",
    "CANCELLED",
    "ACTION_REQUIRED",
    "STARTUP_FAILURE",
    "STALE",
];

/// Roll a job's task instances up into one word.
///
/// The precedence is the whole point: failure beats activity beats partial progress. A job whose
/// `review` failed while `deliver` is still queued is `failed`, because that is the thing a human
/// has to act on; showing `running` would hide it behind the jobs that are merely slow.
///
/// Returns exactly one of `queued`, `running`, `failed`, `skipped`, `success`.
pub fn job_state(tasks: &[TaskState]) -> String {
    let states: Vec<&str> = tasks.iter().map(TaskState::state_or_none).collect();
    if states.is_empty() {
        return "queued".to_string();
    }
    if states.iter().copied().any(states::is_failed) {
        return "failed".to_string();
    }
    if states.iter().copied().any(states::is_active) {
        return "running".to_string();
    }
    // Nothing failed and nothing is moving, so what is left unfinished is `none` and states
    // Airflow reported that this build does not know about.
    let unfinished = states.iter().filter(|s| !states::is_final(s)).count();
    if unfinished > 0 {
        // Some task did finish, so the job is between stages rather than not started.
        return if unfinished < states.len() {
            "running".to_string()
        } else {
            "queued".to_string()
        };
    }
    if states.iter().all(|s| *s == "skipped") {
        "skipped".to_string()
    } else {
        "success".to_string()
    }
}

/// Split a run's task instances into one row per mapped job.
///
/// Buckets keep the order the API returned tasks in, because `JobRow::tasks` is what a detail
/// pane renders and re-sorting it would scramble the stage list. Rows come out ordered by job
/// index; the unmapped bucket (`fan_out` itself, and anything Airflow has not expanded) is
/// dropped as soon as *any* mapped task exists, so a fanned-out run does not grow a phantom row.
/// A run still fanning out keeps its single placeholder, which is honest: there is one job-shaped
/// thing there, we just do not know its index yet.
pub fn group_jobs(
    dag_id: &str,
    run_id: &str,
    tasks: &[TaskState],
    fan_out: &[Value],
    fallback_issues: &[String],
) -> Vec<JobRow> {
    let mut buckets: Vec<(i32, Vec<TaskState>)> = Vec::new();
    for task in tasks {
        match buckets.iter_mut().find(|(idx, _)| *idx == task.map_index) {
            Some((_, bucket)) => bucket.push(task.clone()),
            None => buckets.push((task.map_index, vec![task.clone()])),
        }
    }

    let mut order: Vec<i32> = buckets
        .iter()
        .map(|(idx, _)| *idx)
        .filter(|idx| *idx >= 0)
        .collect();
    if order.is_empty() {
        order = buckets.iter().map(|(idx, _)| *idx).collect();
    }
    order.sort_unstable();

    order
        .into_iter()
        .filter_map(|idx| {
            let tasks = buckets
                .iter()
                .find(|(key, _)| *key == idx)
                .map(|(_, v)| v)?;
            Some(JobRow {
                dag_id: dag_id.to_string(),
                run_id: run_id.to_string(),
                map_index: idx,
                issue: issue_of(idx, fan_out, fallback_issues),
                state: job_state(tasks),
                tasks: tasks.clone(),
            })
        })
        .collect()
}

/// Which issue a job index answers.
///
/// Jobs are issues x targets, so a run carrying exactly one issue gives every job that issue
/// without consulting the `fan_out` XCom at all. Two or more issues and the mapping genuinely
/// needs the XCom; until it arrives, saying `-` is the honest answer and guessing the first issue
/// is not.
fn issue_of(idx: i32, fan_out: &[Value], fallback: &[String]) -> String {
    if idx >= 0 {
        if let Some(entry) = usize::try_from(idx).ok().and_then(|i| fan_out.get(i)) {
            // A `null` entry is treated as `{}`, matching `(fan_out[idx] or {}).get("issue")`.
            if let Some(issue) = entry.as_object().and_then(|o| o.get("issue")) {
                if py_truthy(issue) {
                    return py_str(issue);
                }
            }
        }
    }
    match fallback {
        [only] => only.clone(),
        _ => NO_ISSUE.to_string(),
    }
}

/// The single row that stands in for a run whose task instances were not fetched.
///
/// Note `state` is the DAG **run** state verbatim (`unknown` included), not the `job_state`
/// vocabulary. A collapsed row is a statement about the run, and pretending otherwise would let a
/// reader believe the jobs had been inspected.
pub fn collapsed_job(run: &Run) -> JobRow {
    let issues = run.issues();
    let issue = if issues.is_empty() {
        NO_ISSUE.to_string()
    } else {
        issues.join(", ")
    };
    JobRow {
        dag_id: run.dag_id.clone(),
        run_id: run.run_id.clone(),
        map_index: -1,
        issue,
        state: run.state.clone(),
        tasks: Vec::new(),
    }
}

/// Name the stage each job of a run is *at*, joined into one cell.
///
/// The frontier is the first active stage in display order, or else the last finished one. The
/// `stage:state` spelling on the second branch is deliberate: `deliver` and `deliver:skipped` are
/// very different facts, and a bare stage name for a skipped or failed task would read as
/// progress. `success` is the only state that goes unannotated, because it is the only one that
/// means "and then the next thing started".
///
/// Duplicate labels collapse, so a fanned-out run whose eight jobs are all building says
/// `build_and_test` once rather than eight times.
pub fn stage_progress(tasks: &[TaskState]) -> String {
    // Insertion-ordered, because the label order follows the order jobs were first seen.
    let mut by_job: Vec<(i32, Vec<(String, String)>)> = Vec::new();
    for task in tasks {
        let stage = task
            .task_id
            .rsplit('.')
            .next()
            .unwrap_or(&task.task_id)
            .to_string();
        let state = task.state_or_none().to_string();
        let entry = match by_job.iter_mut().find(|(idx, _)| *idx == task.map_index) {
            Some(entry) => entry,
            None => {
                by_job.push((task.map_index, Vec::new()));
                match by_job.last_mut() {
                    Some(entry) => entry,
                    None => continue,
                }
            }
        };
        match entry.1.iter_mut().find(|(name, _)| *name == stage) {
            Some(slot) => slot.1 = state,
            None => entry.1.push((stage, state)),
        }
    }

    let mut frontiers: Vec<String> = Vec::new();
    for (_, states) in &by_job {
        let ordered = order_stages(states);
        let label = match ordered
            .iter()
            .find(|(_, state)| states::is_active(state))
            .map(|(name, _)| (*name).to_string())
        {
            Some(active) => active,
            None => match ordered
                .iter()
                .rev()
                .find(|(_, state)| states::is_final(state))
            {
                None => PENDING_STAGE.to_string(),
                Some((name, state)) if *state == "success" => (*name).to_string(),
                Some((name, state)) => format!("{name}:{state}"),
            },
        };
        if !frontiers.contains(&label) {
            frontiers.push(label);
        }
    }
    if frontiers.is_empty() {
        NO_STAGE.to_string()
    } else {
        frontiers.join(", ")
    }
}

/// Known stages in display order, then everything else alphabetically.
///
/// The alphabetical tail is what keeps a blueprint that invents a stage name from making the
/// frontier depend on Airflow's task-listing order.
fn order_stages(states: &[(String, String)]) -> Vec<(&str, &str)> {
    let mut ordered: Vec<(&str, &str)> = Vec::with_capacity(states.len());
    for known in TASK_ORDER {
        if let Some((name, state)) = states.iter().find(|(name, _)| name == known) {
            ordered.push((name.as_str(), state.as_str()));
        }
    }
    let mut extra: Vec<(&str, &str)> = states
        .iter()
        .filter(|(name, _)| !TASK_ORDER.contains(&name.as_str()))
        .map(|(name, state)| (name.as_str(), state.as_str()))
        .collect();
    extra.sort_unstable_by(|a, b| a.0.cmp(b.0));
    extra.dedup_by(|a, b| a.0 == b.0);
    ordered.extend(extra);
    ordered
}

/// Reduce a GitHub check rollup to `N pass / N fail / N pending`.
///
/// Handles both shapes `gh` emits — `CheckRun` (`status` + `conclusion`) and `StatusContext`
/// (`state`) — and it deliberately resolves every doubt toward *pending*. A check whose verdict
/// this build does not recognise is not evidence of failure, and reporting it as one would block
/// a delivery on a vocabulary change.
///
/// `None`, a non-list, and an empty list are all the literal `"none"`: there is no check data.
/// A list of nothing but junk is `"0 pass / 0 fail / 0 pending"`, because there *was* data.
pub fn summarize_checks(rollup: Option<&Value>) -> String {
    let Some(Value::Array(items)) = rollup else {
        return "none".to_string();
    };
    if items.is_empty() {
        return "none".to_string();
    }
    let (mut passed, mut failed, mut pending) = (0u32, 0u32, 0u32);
    for item in items {
        let Some(item) = item.as_object() else {
            continue;
        };
        let conclusion = item.get("conclusion").filter(|v| py_truthy(v));
        let verdict = conclusion
            .or_else(|| item.get("state").filter(|v| py_truthy(v)))
            .map(py_str)
            .unwrap_or_default()
            .to_uppercase();
        let status = item
            .get("status")
            .filter(|v| py_truthy(v))
            .map(py_str)
            .unwrap_or_default()
            .to_uppercase();
        if !status.is_empty() && status != "COMPLETED" && conclusion.is_none() {
            pending += 1;
        } else if CHECK_PASS.contains(&verdict.as_str()) {
            passed += 1;
        } else if CHECK_FAIL.contains(&verdict.as_str()) {
            failed += 1;
        } else {
            pending += 1;
        }
    }
    format!("{passed} pass / {failed} fail / {pending} pending")
}

/// Read the comma-separated issue list an operator typed into a trigger prompt.
///
/// Blank segments are dropped rather than rejected, because `42,,43` is a typo with an obvious
/// meaning and refusing it mid-trigger costs more than it protects.
pub fn parse_issues(text: &str) -> Vec<String> {
    text.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect()
}

/// Render a job index for a table cell: `-1` is "no index yet", so it prints as `-`.
///
/// This is the display half of the rule that `map_index == -1` is not a job number. The JSON
/// keeps the raw integer; only humans get the dash.
pub fn job_index(map_index: i32) -> String {
    if map_index < 0 {
        NO_STAGE.to_string()
    } else {
        map_index.to_string()
    }
}

/// How long ago something happened, in one cell's worth of characters.
///
/// One unit only, truncated, never rounded up: `90s` is `1m`, not `2m`. A clock skew that puts an
/// event in the future clamps to `0s` rather than printing a negative age, because a negative age
/// says "your data is broken" when the truth is "the two clocks disagree by a second".
pub fn age(then: Option<DateTime<Utc>>, now: DateTime<Utc>) -> String {
    let Some(then) = then else {
        return NO_STAGE.to_string();
    };
    let seconds = (now - then).num_seconds().max(0);
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3600 {
        format!("{}m", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h", seconds / 3600)
    } else {
        format!("{}d", seconds / 86_400)
    }
}

/// Python's truthiness, which is what the ported `or` chains actually test.
///
/// `0`, `0.0`, `""`, `[]`, `{}`, `false` and `null` are all falsy there, and several roll-ups
/// lean on it — a `fan_out` entry with `issue: 0` falls through to the fallback rule.
pub(crate) fn py_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// Python's `str()` for the values that reach a rendered cell.
///
/// Containers are the one divergence, and a deliberate one: Python would print a repr with single
/// quotes, this prints JSON. Nothing real nests a container where a stringified scalar belongs,
/// and JSON is the more useful thing for an operator to see.
pub(crate) fn py_str(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Bool(true) => "True".to_string(),
        Value::Bool(false) => "False".to_string(),
        Value::Null => "None".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    fn ts(task_id: &str, map_index: i32, state: Option<&str>) -> TaskState {
        TaskState::new(task_id, map_index, state.map(str::to_string))
    }

    fn states_of(list: &[Option<&str>]) -> Vec<TaskState> {
        list.iter()
            .enumerate()
            .map(|(i, s)| ts(&format!("job.t{i}"), 0, *s))
            .collect()
    }

    #[test]
    fn job_state_matches_every_pinned_python_example() {
        let cases: &[(&[Option<&str>], &str)] = &[
            (&[], "queued"),
            (&[None, None], "queued"),
            (&[Some("success"), None], "running"),
            (&[Some("success"), Some("running")], "running"),
            (&[Some("upstream_failed"), Some("running")], "failed"),
            (&[Some("success"), Some("success")], "success"),
            (&[Some("skipped"), Some("skipped")], "skipped"),
            (&[Some("skipped"), Some("success")], "success"),
            (
                &[
                    Some("success"),
                    Some("success"),
                    Some("awaiting_input"),
                    None,
                ],
                "running",
            ),
            (&[Some("banana")], "queued"),
            (&[Some("banana"), Some("success")], "running"),
            (&[Some("")], "queued"),
        ];
        for (input, expected) in cases {
            assert_eq!(job_state(&states_of(input)), *expected, "input={input:?}");
        }
    }

    #[test]
    fn failure_outranks_an_active_task() {
        let tasks = vec![
            ts("job.a", 0, Some("running")),
            ts("job.b", 0, Some("failed")),
        ];
        assert_eq!(job_state(&tasks), "failed");
    }

    #[test]
    fn group_jobs_sorts_by_index_and_keeps_task_order() {
        let tasks = vec![
            ts("job.setup", 2, Some("success")),
            ts("job.intent", 0, None),
            ts("job.setup", 0, Some("success")),
        ];
        let rows = group_jobs("factory", "r1", &tasks, &[], &[]);
        assert_eq!(
            rows.iter().map(|r| r.map_index).collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert_eq!(
            rows[0]
                .tasks
                .iter()
                .map(|t| t.task_id.as_str())
                .collect::<Vec<_>>(),
            vec!["job.intent", "job.setup"],
        );
    }

    #[test]
    fn group_jobs_drops_the_unmapped_bucket_once_a_job_exists() {
        let tasks = vec![
            ts("fan_out", -1, Some("success")),
            ts("job.setup", 0, Some("running")),
        ];
        let rows = group_jobs("factory", "r1", &tasks, &[], &[]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].map_index, 0);
    }

    #[test]
    fn group_jobs_keeps_one_placeholder_while_a_run_fans_out() {
        let tasks = vec![ts("fan_out", -1, Some("running"))];
        let rows = group_jobs("factory", "r1", &tasks, &[], &[]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].map_index, -1);
        assert_eq!(rows[0].issue, NO_ISSUE);
    }

    #[test]
    fn group_jobs_on_no_tasks_is_empty_not_a_placeholder() {
        assert!(group_jobs("factory", "r1", &[], &[], &[]).is_empty());
    }

    #[test]
    fn one_fallback_issue_reaches_every_row_but_two_reach_none() {
        let tasks = vec![ts("job.setup", 0, None), ts("job.setup", 1, None)];
        let one = vec!["42".to_string()];
        let rows = group_jobs("factory", "r1", &tasks, &[], &one);
        assert!(rows.iter().all(|r| r.issue == "42"));

        let two = vec!["42".to_string(), "43".to_string()];
        let rows = group_jobs("factory", "r1", &tasks, &[], &two);
        assert!(rows.iter().all(|r| r.issue == NO_ISSUE));
    }

    #[test]
    fn fan_out_wins_over_the_fallback_but_only_when_truthy() {
        let tasks = vec![ts("job.setup", 0, None), ts("job.setup", 1, None)];
        let fan_out = vec![json!({"issue": 7}), json!({"issue": 0})];
        let fallback = vec!["99".to_string()];
        let rows = group_jobs("factory", "r1", &tasks, &fan_out, &fallback);
        assert_eq!(rows[0].issue, "7");
        // `issue: 0` is falsy in Python, so the row falls through to the single fallback.
        assert_eq!(rows[1].issue, "99");
    }

    #[test]
    fn a_null_fan_out_entry_is_an_empty_mapping() {
        let tasks = vec![ts("job.setup", 0, None)];
        let rows = group_jobs("factory", "r1", &tasks, &[Value::Null], &[]);
        assert_eq!(rows[0].issue, NO_ISSUE);
    }

    #[test]
    fn collapsed_job_reports_the_run_state_verbatim() {
        let mut run = Run::new("factory", "r1", "success");
        run.conf = match json!({"issues": ["42", "43"]}) {
            Value::Object(map) => map,
            _ => Default::default(),
        };
        let row = collapsed_job(&run);
        assert_eq!(
            (row.map_index, row.issue.as_str(), row.state.as_str()),
            (-1, "42, 43", "success")
        );
        assert!(row.tasks.is_empty());

        let bare = collapsed_job(&Run::new("f", "r", "queued"));
        assert_eq!(bare.issue, NO_ISSUE);
        assert_eq!(bare.state, "queued");

        // `unknown` is a run state, not a job state — it must survive the roll-up untouched.
        assert_eq!(
            collapsed_job(&Run::new("f", "r", "unknown")).state,
            "unknown"
        );
    }

    #[test]
    fn stage_progress_names_the_active_frontier() {
        let mut tasks: Vec<TaskState> = [
            "setup",
            "intent",
            "approve_intent",
            "record_intent",
            "spec",
            "plan",
            "approve_plan",
            "record_plan",
        ]
        .iter()
        .map(|s| ts(&format!("job.{s}"), 0, Some("success")))
        .collect();
        tasks.push(ts("job.build_and_test", 0, Some("running")));
        assert_eq!(stage_progress(&tasks), "build_and_test");
    }

    #[test]
    fn awaiting_input_is_the_frontier_or_gated_jobs_render_the_wrong_stage() {
        let tasks = vec![
            ts("job.setup", 0, Some("success")),
            ts("job.intent", 0, Some("success")),
            ts("job.approve_intent", 0, Some("awaiting_input")),
        ];
        assert_eq!(stage_progress(&tasks), "approve_intent");

        let deferred = vec![
            ts("job.plan", 0, Some("success")),
            ts("job.approve_plan", 0, Some("deferred")),
        ];
        assert_eq!(stage_progress(&deferred), "approve_plan");
    }

    #[test]
    fn a_finished_non_success_stage_is_annotated() {
        let tasks = vec![
            ts("job.setup", 0, Some("success")),
            ts("job.intent", 0, Some("failed")),
        ];
        assert_eq!(stage_progress(&tasks), "intent:failed");

        let skipped = vec![
            ts("job.review", 0, Some("success")),
            ts("job.deliver", 0, Some("skipped")),
        ];
        assert_eq!(stage_progress(&skipped), "deliver:skipped");
    }

    #[test]
    fn stage_progress_edge_cases() {
        assert_eq!(stage_progress(&[]), "-");
        assert_eq!(
            stage_progress(&[ts("fan_out", -1, Some("success"))]),
            "fan_out"
        );
        assert_eq!(
            stage_progress(&[ts("job.setup", 0, Some("none"))]),
            "pending"
        );
        assert_eq!(stage_progress(&[ts("job.setup", 0, None)]), "pending");
    }

    #[test]
    fn stage_progress_lists_one_frontier_per_job_in_first_seen_order() {
        let tasks = vec![
            ts("job.setup", 0, Some("success")),
            ts("job.setup", 1, Some("success")),
            ts("job.spec", 0, Some("running")),
            ts("job.approve_intent", 1, Some("deferred")),
        ];
        assert_eq!(stage_progress(&tasks), "spec, approve_intent");
    }

    #[test]
    fn identical_frontiers_collapse_to_one_label() {
        let tasks = vec![
            ts("job.build_and_test", 0, Some("running")),
            ts("job.build_and_test", 1, Some("running")),
            ts("job.build_and_test", 2, Some("running")),
        ];
        assert_eq!(stage_progress(&tasks), "build_and_test");
    }

    #[test]
    fn unknown_stage_names_sort_after_the_known_ones() {
        let tasks = vec![
            ts("job.zebra", 0, Some("success")),
            ts("job.alpha", 0, Some("success")),
            ts("job.setup", 0, Some("success")),
        ];
        // setup (known) first, then alpha, zebra alphabetically; the LAST final one wins.
        assert_eq!(stage_progress(&tasks), "zebra");
    }

    #[test]
    fn a_later_task_overwrites_an_earlier_state_for_the_same_stage() {
        let tasks = vec![
            ts("job.intent", 0, Some("running")),
            ts("job.intent", 0, Some("failed")),
        ];
        assert_eq!(stage_progress(&tasks), "intent:failed");
    }

    #[test]
    fn summarize_checks_says_none_only_when_there_is_no_data() {
        assert_eq!(summarize_checks(None), "none");
        assert_eq!(summarize_checks(Some(&json!([]))), "none");
        assert_eq!(summarize_checks(Some(&json!({}))), "none");
        assert_eq!(summarize_checks(Some(&json!("nope"))), "none");
        // A list of junk *is* data, so the counters render even though nothing counted.
        assert_eq!(
            summarize_checks(Some(&json!([1, "x", null]))),
            "0 pass / 0 fail / 0 pending"
        );
    }

    #[test]
    fn summarize_checks_reads_both_github_shapes() {
        let rollup = json!([
            {"status": "COMPLETED", "conclusion": "SUCCESS"},
            {"status": "IN_PROGRESS"},
            {"state": "FAILURE"},
            {"conclusion": "neutral"},
            {"conclusion": "banana"},
        ]);
        assert_eq!(
            summarize_checks(Some(&rollup)),
            "2 pass / 1 fail / 2 pending"
        );
    }

    #[test]
    fn an_incomplete_status_with_a_conclusion_is_not_pending() {
        let rollup = json!([{"status": "QUEUED", "conclusion": "CANCELLED"}]);
        assert_eq!(
            summarize_checks(Some(&rollup)),
            "0 pass / 1 fail / 0 pending"
        );
    }

    #[test]
    fn parse_issues_drops_blanks_and_trims() {
        assert_eq!(
            parse_issues(" 42, 43,,demo/issue.md "),
            vec!["42", "43", "demo/issue.md"]
        );
        assert!(parse_issues("").is_empty());
        assert!(parse_issues(" , , ").is_empty());
    }

    #[test]
    fn job_index_prints_a_dash_for_no_index_yet() {
        assert_eq!(job_index(0), "0");
        assert_eq!(job_index(3), "3");
        assert_eq!(job_index(-1), "-");
        assert_eq!(job_index(-7), "-");
    }

    #[test]
    fn age_truncates_to_one_unit_and_clamps_the_future() {
        let now = match Utc.timestamp_opt(1_000_000, 0) {
            chrono::LocalResult::Single(dt) => dt,
            _ => Utc::now(),
        };
        let ago = |secs: i64| age(Some(now - chrono::Duration::seconds(secs)), now);
        assert_eq!(ago(0), "0s");
        assert_eq!(ago(30), "30s");
        assert_eq!(ago(59), "59s");
        assert_eq!(ago(60), "1m");
        assert_eq!(ago(90), "1m");
        assert_eq!(ago(300), "5m");
        assert_eq!(ago(3600), "1h");
        assert_eq!(ago(10_800), "3h");
        assert_eq!(ago(172_800), "2d");
        assert_eq!(ago(-500), "0s");
        assert_eq!(age(None, now), "-");
    }
}
