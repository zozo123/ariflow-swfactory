//! What a person reads.
//!
//! The rule this module defends is the one from `00-architecture.md` §7: status must be legible
//! without colour. Every state gets a word, and colour only ever repeats what the word already
//! said — so the same output is readable in a pipe, in a CI log and by someone whose terminal
//! renders red as brown. Columns are sized to the rows actually present, because a fixed layout
//! either truncates a real run id or prints a field of blank space, and both make a table harder
//! to scan than the data justified.
//!
//! Nothing here decides anything. Every value has already been sanitised where it crossed out of a
//! service (`swf_domain::sanitize`), and every verdict was reached in `swf-app`.

use std::fmt::Write as _;

use chrono::{DateTime, Utc};
use swf_app::attention::Attention;
use swf_app::context::Context;
use swf_app::delivery::Delivery;
use swf_app::gates::{BatchOutcome, BatchReport, GateAnswer, GateReview};
use swf_app::stack::StackStatus;
use swf_app::submit::Submission;
use swf_domain::evidence::DeliveryReport;
use swf_domain::model::{Gate, JobRow, Run, SandboxRef};
use swf_domain::rollup::{age, job_index, stage_progress};

use crate::term::Term;

/// A column-aligned table sized to its own rows.
#[derive(Debug, Default)]
pub struct Table {
    head: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl Table {
    /// Start a table with these column names.
    pub fn new<I, S>(head: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            head: head.into_iter().map(Into::into).collect(),
            rows: Vec::new(),
        }
    }

    /// Append one row. Short rows are padded, long ones are not truncated.
    pub fn row<I, S>(&mut self, cells: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.rows.push(cells.into_iter().map(Into::into).collect());
    }

    /// True when nothing was appended.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Render, with the header dimmed and the last column left unpadded.
    pub fn render(&self, term: &Term) -> String {
        let columns = self
            .rows
            .iter()
            .map(Vec::len)
            .chain(std::iter::once(self.head.len()))
            .max()
            .unwrap_or(0);
        let mut width = vec![0usize; columns];
        for row in std::iter::once(&self.head).chain(self.rows.iter()) {
            for (index, cell) in row.iter().enumerate() {
                width[index] = width[index].max(cell.chars().count());
            }
        }
        let line = |row: &Vec<String>| {
            let mut out = String::new();
            for (index, cell) in row.iter().enumerate() {
                if index + 1 == row.len() {
                    out.push_str(cell);
                } else {
                    let _ = write!(out, "{cell:<pad$}  ", pad = width[index]);
                }
            }
            out.trim_end().to_string()
        };
        let mut lines = Vec::with_capacity(self.rows.len() + 1);
        if !self.head.is_empty() {
            lines.push(term.dim(&line(&self.head)));
        }
        lines.extend(self.rows.iter().map(line));
        lines.join("\n")
    }
}

/// A key/value block: `key` padded to the widest key, then two spaces, then the value.
pub fn fields(pairs: &[(&str, String)]) -> String {
    let width = pairs
        .iter()
        .map(|(key, _)| key.chars().count())
        .max()
        .unwrap_or(0);
    pairs
        .iter()
        .map(|(key, value)| format!("{key:<width$}  {value}").trim_end().to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

/// The word for "nothing to show", so an empty table never renders as an empty screen.
pub fn nothing(what: &str) -> String {
    format!("(no {what})")
}

/// `swf context list`
pub fn contexts(all: &[Context], active: &str, term: &Term) -> String {
    let mut table = Table::new(["", "name", "airflow", "auth", "repo"]);
    for context in all {
        let marker = if context.name == active { "*" } else { " " };
        table.row([
            marker.to_string(),
            context.name.clone(),
            context.airflow_url.clone(),
            context.auth.redacted(),
            context.repo.clone().unwrap_or_else(|| "-".into()),
        ]);
    }
    table.render(term)
}

/// `swf runs list`
pub fn runs(rows: &[Run], now: DateTime<Utc>, term: &Term) -> String {
    if rows.is_empty() {
        return nothing("runs");
    }
    let mut table = Table::new(["run", "state", "age", "jobs", "issues"]);
    for run in rows {
        table.row([
            format!("{}/{}", run.dag_id, run.run_id),
            state_word(&run.state, term),
            age(run.start.map(|at| at.with_timezone(&Utc)), now),
            run.jobs.len().to_string(),
            joined(&run.issues()),
        ]);
    }
    table.render(term)
}

/// `swf jobs list`
pub fn jobs(rows: &[JobRow], term: &Term) -> String {
    if rows.is_empty() {
        return nothing("jobs");
    }
    let mut table = Table::new(["job", "issue", "state", "stage"]);
    for job in rows {
        table.row([
            job.id().to_string(),
            job.issue.clone(),
            state_word(&job.state, term),
            stage_progress(&job.tasks),
        ]);
    }
    table.render(term)
}

/// `swf runs inspect`
pub fn run_detail(run: &Run, url: &str, now: DateTime<Utc>, term: &Term) -> String {
    let head = fields(&[
        ("run", format!("{}/{}", run.dag_id, run.run_id)),
        ("state", state_word(&run.state, term)),
        ("age", age(run.start.map(|at| at.with_timezone(&Utc)), now)),
        ("issues", joined(&run.issues())),
        ("url", url.to_string()),
    ]);
    format!("{head}\n\n{}", jobs(&run.jobs, term))
}

/// `swf jobs inspect`
pub fn job_detail(job: &JobRow, gates: &[Gate], url: &str, term: &Term) -> String {
    let head = fields(&[
        ("job", job.id().to_string()),
        ("index", job_index(job.map_index)),
        ("issue", job.issue.clone()),
        ("state", state_word(&job.state, term)),
        ("stage", stage_progress(&job.tasks)),
        ("url", url.to_string()),
    ]);
    let mut tasks = Table::new(["task", "state"]);
    for task in &job.tasks {
        tasks.row([task.task_id.clone(), state_word(task.state_or_none(), term)]);
    }
    let mut out = format!("{head}\n\n{}", tasks.render(term));
    if !gates.is_empty() {
        let _ = write!(out, "\n\n{}", self::gates(gates, Utc::now(), term));
    }
    out
}

/// `swf gates list`
pub fn gates(rows: &[Gate], now: DateTime<Utc>, term: &Term) -> String {
    if rows.is_empty() {
        return nothing("gates waiting for an answer");
    }
    let mut table = Table::new(["gate", "ready", "age", "subject"]);
    for gate in rows {
        table.row([
            gate.id().to_string(),
            ready_word(gate.ready, term),
            age(gate.created_at.map(|at| at.with_timezone(&Utc)), now),
            swf_domain::sanitize::sanitize_line(&gate.subject),
        ]);
    }
    table.render(term)
}

/// `swf gates review`
///
/// The revision is printed because it is the argument to `--expect`: an operator who read this
/// evidence can prove they answered *this* evidence and not whatever replaced it.
pub fn gate_review(review: &GateReview, term: &Term) -> String {
    let head = fields(&[
        ("gate", review.id.to_string()),
        ("job", review.id.job.to_string()),
        ("ready", ready_word(review.ready, term)),
        ("task state", review.task_state.clone()),
        ("job state", state_word(&review.job_state, term)),
        ("stage", review.stage.clone()),
        ("options", joined(&review.options)),
        ("revision", review.revision.clone()),
        ("url", review.url.clone()),
    ]);
    let body = if review.gate.body.trim().is_empty() {
        "(no evidence body)".to_string()
    } else {
        review.gate.body.clone()
    };
    format!(
        "{head}\n\n{}\n\n{body}",
        term.head(&review.gate.subject.clone())
    )
}

/// What `swf gates approve` echoes.
///
/// The first line is `herd`'s wording verbatim (`02-herd-tui.md` §6) — the same operator reads
/// both, and two spellings of one event is two events as far as a reader is concerned.
pub fn gate_answer(answer: &GateAnswer, actor: &str, term: &Term) -> String {
    let job = &answer.id.job;
    let where_ = format!(
        "{}/{}[{}] {}",
        job.dag_id,
        job.run_id,
        job.map_index,
        answer.id.short_name()
    );
    let mut out = format!("{} {where_} as {actor}", verb(answer.decision));
    let _ = write!(
        out,
        "\n{}",
        fields(&[
            ("revision", answer.revision.clone()),
            ("sightings", answer.sightings.to_string()),
        ])
    );
    if answer.forced {
        let _ = write!(
            out,
            "\n{}",
            term.warn("forced: readiness was not confirmed, and the scheduler may fail this gate")
        );
    }
    out
}

/// `swf gates approve --all` / `swf gates reject --all`, and the dry run that precedes it.
///
/// The filter is echoed on the first line and the counts on the last, because those are the two
/// facts that make a batch checkable after the event: what was asked for, and what it did. Every
/// gate keeps its own line — a summary alone would hide which gate was the one that failed.
pub fn batch(report: &BatchReport, actor: &str, term: &Term) -> String {
    let verb = verb(report.decision);
    let head = if report.dry_run {
        term.head(&format!("dry run: nothing was written ({verb} as {actor})"))
    } else {
        term.head(&format!("{verb} as {actor}"))
    };
    let mut out = format!("{head}\nfilter  {}", report.filter);

    if report.items.is_empty() {
        let _ = write!(out, "\n\n{}", nothing("gates matched that filter"));
    } else {
        let show_issue = report.items.iter().any(|item| item.issue.is_some());
        let mut table = if show_issue {
            Table::new(["gate", "issue", "outcome", "why"])
        } else {
            Table::new(["gate", "outcome", "why"])
        };
        for item in &report.items {
            let mut cells = vec![item.id.to_string()];
            if show_issue {
                cells.push(item.issue.clone().unwrap_or_else(|| "-".into()));
            }
            cells.push(outcome_word(item.outcome, term));
            cells.push(item.detail.clone());
            table.row(cells);
        }
        let _ = write!(out, "\n\n{}", table.render(term));
    }

    let counts = if report.dry_run {
        format!(
            "{} matched · {} would be answered · {} skipped",
            report.matched(),
            report.count(BatchOutcome::Planned),
            report.count(BatchOutcome::Skipped)
        )
    } else {
        format!(
            "{} matched · {} answered · {} skipped · {} conflict · {} failed",
            report.matched(),
            report.count(BatchOutcome::Answered),
            report.count(BatchOutcome::Skipped),
            report.count(BatchOutcome::Conflict),
            report.count(BatchOutcome::Failed)
        )
    };
    let _ = write!(out, "\n\n{counts}");
    if report.truncated {
        // Its own line, and never folded into the counts: a batch over a shortened selection
        // answered *a* set rather than *the* set, and that is the one fact a reader must not skim.
        let _ = write!(
            out,
            "\n{}",
            term.warn("the selection was truncated; matching gates outside it were not answered")
        );
    }
    out
}

/// A batch outcome as a word first, coloured only to repeat what the word already says.
fn outcome_word(outcome: BatchOutcome, term: &Term) -> String {
    match outcome {
        BatchOutcome::Answered => term.good(outcome.as_str()),
        BatchOutcome::Planned => outcome.as_str().to_string(),
        BatchOutcome::Skipped => term.dim(outcome.as_str()),
        BatchOutcome::Conflict => term.warn(outcome.as_str()),
        BatchOutcome::Failed => term.bad(outcome.as_str()),
    }
}

/// `1 gate`, `12 gates` — a count that reads as a sentence in a question.
pub fn count_of(n: usize, what: &str) -> String {
    if n == 1 {
        format!("{n} {what}")
    } else {
        format!("{n} {what}s")
    }
}

/// `swf attention`
pub fn attention(att: &Attention, term: &Term) -> String {
    let mut blocks: Vec<String> = Vec::new();

    if !att.gates.is_empty() {
        let mut table = Table::new(["gate", "ready", "age", "subject"]);
        for item in &att.gates {
            table.row([
                item.id.clone(),
                ready_word(item.ready, term),
                item.age.clone(),
                item.subject.clone(),
            ]);
        }
        blocks.push(format!(
            "{}\n{}",
            term.head(&format!("gates ({})", att.gates.len())),
            table.render(term)
        ));
    }
    if !att.failures.is_empty() {
        let mut table = Table::new(["job", "issue", "state", "stage"]);
        for item in &att.failures {
            table.row([
                item.id.clone(),
                item.issue.clone(),
                term.bad(&item.state),
                item.stage.clone(),
            ]);
        }
        blocks.push(format!(
            "{}\n{}",
            term.head(&format!("failures ({})", att.failures.len())),
            table.render(term)
        ));
    }
    if !att.blocked.is_empty() {
        let mut table = Table::new(["pr", "branch", "reason", "checks", "title"]);
        for item in &att.blocked {
            table.row([
                format!("#{}", item.number),
                item.branch.clone(),
                item.reason.clone(),
                item.checks.clone(),
                item.title.clone(),
            ]);
        }
        blocks.push(format!(
            "{}\n{}",
            term.head(&format!("blocked deliveries ({})", att.blocked.len())),
            table.render(term)
        ));
    }
    if !att.orphans.is_empty() {
        let mut table = Table::new(["sandbox", "status", "age", "why"]);
        for item in &att.orphans {
            table.row([
                item.name.clone(),
                item.status.clone(),
                item.age.clone(),
                item.reason.clone(),
            ]);
        }
        blocks.push(format!(
            "{}\n{}",
            term.head(&format!("orphan sandboxes ({})", att.orphans.len())),
            table.render(term)
        ));
    }
    for error in &att.errors {
        // A source that failed is shown, never hidden: a quiet screen has to mean "nothing is
        // wrong", not "we could not tell" (non-negotiable 4).
        blocks.push(term.warn(&format!(
            "source {} is unavailable: {}",
            error.source, error.message
        )));
    }
    if blocks.is_empty() {
        return term.good("nothing needs a person");
    }
    blocks.join("\n\n")
}

/// `swf deliveries list`
pub fn deliveries(rows: &[Delivery], term: &Term) -> String {
    if rows.is_empty() {
        return nothing("deliveries");
    }
    let mut table = Table::new(["pr", "branch", "state", "checks", "title"]);
    for row in rows {
        let state = if row.blocked {
            term.warn(&row.state)
        } else {
            row.state.clone()
        };
        table.row([
            format!("#{}", row.number),
            row.branch.clone(),
            state,
            row.checks.clone(),
            row.title.clone(),
        ]);
    }
    table.render(term)
}

/// `swf deliveries verify`
///
/// The three claims stay three columns (non-negotiable 10). Collapsing them into one "verified?"
/// is the mistake this command exists to make impossible: a run that says it succeeded, a branch
/// that exists and a test suite that was re-run here are three different facts.
pub fn verifications(reports: &[DeliveryReport], term: &Term) -> String {
    if reports.is_empty() {
        return nothing("deliveries to verify");
    }
    let mut table = Table::new([
        "delivery",
        "verdict",
        "workflow",
        "published",
        "verified",
        "tests",
    ]);
    for report in reports {
        table.row([
            report.delivery.branch.clone(),
            verdict_word(report, term),
            yes_no(report.workflow_succeeded, term),
            yes_no(report.branch_published, term),
            yes_no(report.independently_verified, term),
            tests_word(report),
        ]);
    }
    let mut out = table.render(term);
    for report in reports {
        let why = why_not(report);
        if !why.is_empty() {
            let _ = write!(
                out,
                "\n{}",
                term.bad(&format!("  {}: {why}", report.delivery.branch))
            );
        }
    }
    out
}

/// One short sentence saying what stopped a report short, or nothing when it did not.
pub fn why_not(report: &DeliveryReport) -> String {
    if report.verdict.is_verified() {
        return String::new();
    }
    let refuted: Vec<String> = report
        .refutations()
        .iter()
        .map(|check| format!("{}: {}", check.id, check.detail))
        .collect();
    if !refuted.is_empty() {
        return refuted.join("; ");
    }
    let unproven: Vec<String> = report
        .checks
        .iter()
        .filter(|check| !check.status.passed() && !check.status.refutes())
        .map(|check| format!("{}: {}", check.id, check.detail))
        .collect();
    if unproven.is_empty() {
        format!("nothing above {} could be established", report.attained)
    } else {
        unproven.join("; ")
    }
}

/// The re-run test result as a cell.
pub fn tests_word(report: &DeliveryReport) -> String {
    match &report.tests {
        None => "-".to_string(),
        Some(tests) if tests.timed_out => "timed out".to_string(),
        Some(tests) => format!(
            "{} passed, {} failed",
            tests.passed,
            tests.failed + tests.errors
        ),
    }
}

/// `swf sandboxes list`
pub fn sandboxes(rows: &[SandboxRef], now: DateTime<Utc>, term: &Term) -> String {
    if rows.is_empty() {
        return nothing("sandboxes");
    }
    let mut table = Table::new(["name", "status", "owner", "age", "factory"]);
    for row in rows {
        table.row([
            row.name.clone(),
            row.status.clone(),
            row.created_by.clone(),
            age(row.created_at.map(|at| at.with_timezone(&Utc)), now),
            if row.factory_named() { "yes" } else { "no" }.to_string(),
        ]);
    }
    table.render(term)
}

/// `swf submit`
pub fn submission(sub: &Submission) -> String {
    let blueprint = match (&sub.blueprint.path, sub.blueprint.resolved) {
        (Some(path), true) => format!("{} ({path})", sub.blueprint.name),
        // A blueprint that is not on this laptop is the normal case for a remote operator: the DAG
        // lives on the server. Saying so beats implying the submit was somehow partial.
        _ => format!(
            "{} (not on this machine; the server has it)",
            sub.blueprint.name
        ),
    };
    fields(&[
        ("run", format!("{}/{}", sub.dag_id, sub.run_id)),
        ("issues", joined(&sub.issues)),
        ("blueprint", blueprint),
        (
            "jobs",
            sub.jobs
                .map(|n| n.to_string())
                .unwrap_or_else(|| "-".into()),
        ),
        ("url", sub.url.clone()),
    ])
}

/// `swf stack up | down | status`
pub fn stack(status: &StackStatus, term: &Term) -> String {
    let mut table = Table::new(["what", "state", "detail"]);
    for row in &status.rows {
        let state = if row.ok {
            term.good(&row.state)
        } else {
            term.bad(&row.state)
        };
        table.row([row.name.clone(), state, row.detail.clone()]);
    }
    let body = if table.is_empty() {
        nothing("services")
    } else {
        table.render(term)
    };
    let ran = status
        .ran
        .iter()
        .map(|cmd| term.dim(&format!("$ {cmd}")))
        .collect::<Vec<_>>()
        .join("\n");
    if ran.is_empty() {
        body
    } else {
        format!("{ran}\n\n{body}")
    }
}

/// The lower-case verb `herd` echoes, which is not the capitalised option Airflow records.
pub fn verb(decision: swf_app::gates::Decision) -> &'static str {
    if decision.approves() {
        "approve"
    } else {
        "reject"
    }
}

/// A state word, coloured only to repeat what it already says.
pub fn state_word(state: &str, term: &Term) -> String {
    match state {
        "success" => term.good(state),
        "failed" | "upstream_failed" => term.bad(state),
        "awaiting_input" | "deferred" | "up_for_retry" => term.warn(state),
        _ => state.to_string(),
    }
}

/// Readiness, as a word first and a colour second (`00-architecture.md` §7).
pub fn ready_word(ready: bool, term: &Term) -> String {
    if ready {
        term.good("ready")
    } else {
        term.dim("waiting")
    }
}

/// A boolean claim, as a word.
pub fn yes_no(value: bool, term: &Term) -> String {
    if value {
        term.good("yes")
    } else {
        term.bad("no")
    }
}

/// The verdict word, coloured by whether it means "proven".
fn verdict_word(report: &DeliveryReport, term: &Term) -> String {
    let word = report.verdict.as_str();
    if report.verdict.is_verified() {
        term.good(word)
    } else if report.verdict == swf_domain::evidence::Verdict::Refuted {
        term.bad(word)
    } else {
        term.warn(word)
    }
}

/// A list as a cell: `-` when empty, so a column never goes blank for two different reasons.
fn joined(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_string()
    } else {
        values.join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use swf_domain::model::TaskState;

    fn plain() -> Term {
        Term::plain()
    }

    #[test]
    fn a_table_sizes_its_columns_to_the_rows_it_actually_has() {
        let mut table = Table::new(["a", "bbbb"]);
        table.row(["x", "y"]);
        table.row(["longer", "z"]);
        assert_eq!(table.render(&plain()), "a       bbbb\nx       y\nlonger  z");
    }

    #[test]
    fn the_last_column_is_never_padded_so_a_line_has_no_trailing_space() {
        let mut table = Table::new(["a", "b"]);
        table.row(["x", "yy"]);
        table.row(["x", "y"]);
        for line in table.render(&plain()).lines() {
            assert_eq!(line, line.trim_end(), "{line:?}");
        }
    }

    #[test]
    fn every_state_is_legible_with_no_colour_at_all() {
        let term = plain();
        for state in ["success", "failed", "awaiting_input", "queued"] {
            assert_eq!(state_word(state, &term), state);
        }
        assert_eq!(ready_word(true, &term), "ready");
        assert_eq!(ready_word(false, &term), "waiting");
        assert_eq!(yes_no(false, &term), "no");
    }

    #[test]
    fn an_empty_table_says_so_rather_than_printing_nothing() {
        assert_eq!(jobs(&[], &plain()), "(no jobs)");
        assert_eq!(
            gates(&[], Utc::now(), &plain()),
            "(no gates waiting for an answer)"
        );
    }

    #[test]
    fn a_job_row_renders_its_identity_not_its_position() {
        let mut job = JobRow::new("factory", "manual__2026", 1);
        job.issue = "42".into();
        job.state = "running".into();
        job.tasks = vec![TaskState::new("job.setup", 1, Some("success".into()))];
        let text = jobs(&[job], &plain());
        assert!(text.contains("factory/manual__2026#1"), "{text}");
        assert!(text.contains("setup"), "{text}");
    }

    #[test]
    fn the_gate_echo_is_herds_wording() {
        use swf_app::gates::Decision;
        use swf_domain::ids::{GateId, JobId};

        let answer = GateAnswer {
            id: GateId::new(JobId::new("factory", "manual__1", 1), "job.approve_plan"),
            decision: Decision::Approve,
            revision: "abc123".into(),
            forced: false,
            sightings: 2,
        };
        let text = gate_answer(&answer, "admin", &plain());
        assert!(
            text.starts_with("approve factory/manual__1[1] approve_plan as admin"),
            "{text}"
        );
    }

    fn batch_item(
        id: &str,
        outcome: swf_app::gates::BatchOutcome,
        detail: &str,
    ) -> swf_app::gates::BatchItem {
        use swf_domain::ids::GateId;
        swf_app::gates::BatchItem {
            id: GateId::parse(id).expect("a gate id"),
            gate: "approve_plan".into(),
            issue: None,
            ready: outcome != swf_app::gates::BatchOutcome::Skipped,
            outcome,
            detail: detail.into(),
            revision: "abc123".into(),
            sightings: 2,
            kind: None,
        }
    }

    #[test]
    fn a_batch_report_says_what_it_did_to_every_gate_and_what_selected_them() {
        use swf_app::gates::{BatchOutcome, BatchReport, Decision};
        let report = BatchReport {
            decision: Decision::Approve,
            dry_run: false,
            filter: "--dag factory --gate plan".into(),
            truncated: false,
            items: vec![
                batch_item("factory/r1#0:plan", BatchOutcome::Answered, ""),
                batch_item("factory/r1#1:plan", BatchOutcome::Skipped, "is not ready"),
                batch_item(
                    "factory/r1#2:plan",
                    BatchOutcome::Conflict,
                    "answered first",
                ),
            ],
        };
        let text = batch(&report, "admin", &plain());
        assert!(text.contains("approve as admin"), "{text}");
        assert!(text.contains("--dag factory --gate plan"), "{text}");
        // Every gate keeps its own line: a summary alone would hide which one was which.
        for id in ["factory/r1#0", "factory/r1#1", "factory/r1#2"] {
            assert!(text.contains(id), "{id} is missing from {text}");
        }
        assert!(text.contains("is not ready"), "a skip says why: {text}");
        assert!(
            text.contains("3 matched · 1 answered · 1 skipped · 1 conflict · 0 failed"),
            "{text}"
        );
        // Legible with no colour at all (§7): the words carry the state, not the escape codes.
        assert!(!text.contains('\u{1b}'), "{text}");
    }

    #[test]
    fn a_dry_run_says_it_wrote_nothing_and_a_shortened_batch_says_so_on_its_own_line() {
        use swf_app::gates::{BatchOutcome, BatchReport, Decision};
        let report = BatchReport {
            decision: Decision::Reject,
            dry_run: true,
            filter: "--limit 1".into(),
            truncated: true,
            items: vec![batch_item("factory/r1#0:plan", BatchOutcome::Planned, "")],
        };
        let text = batch(&report, "admin", &plain());
        assert!(text.starts_with("dry run: nothing was written"), "{text}");
        assert!(text.contains("1 matched · 1 would be answered"), "{text}");
        let last = text.lines().last().unwrap_or_default();
        assert!(
            last.contains("truncated") && last.contains("were not answered"),
            "the truncation is its own line, not a clause in the counts: {text}"
        );
    }

    #[test]
    fn a_batch_that_matched_nothing_says_so_rather_than_printing_a_bare_zero() {
        use swf_app::gates::{BatchReport, Decision};
        let report = BatchReport {
            decision: Decision::Approve,
            dry_run: false,
            filter: "--dag nope".into(),
            truncated: false,
            items: Vec::new(),
        };
        let text = batch(&report, "admin", &plain());
        assert!(text.contains("(no gates matched that filter)"), "{text}");
        assert_eq!(count_of(1, "gate"), "1 gate");
        assert_eq!(count_of(0, "gate"), "0 gates");
        assert_eq!(count_of(12, "gate"), "12 gates");
    }

    #[test]
    fn forcing_a_gate_says_what_it_risked() {
        use swf_app::gates::Decision;
        use swf_domain::ids::{GateId, JobId};

        let answer = GateAnswer {
            id: GateId::new(JobId::new("factory", "manual__1", 0), "job.approve_intent"),
            decision: Decision::Reject,
            revision: "d".into(),
            forced: true,
            sightings: 0,
        };
        let text = gate_answer(&answer, "admin", &plain());
        assert!(text.contains("forced"), "{text}");
        assert!(text.contains("fail this gate"), "{text}");
    }
}
