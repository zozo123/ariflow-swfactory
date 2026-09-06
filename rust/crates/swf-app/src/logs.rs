//! One task attempt's log, and a follow loop that only ever emits what is new.
//!
//! Airflow has no streaming log endpoint (`03-airflow-rest.md` §10): `--follow` is a poll loop and
//! the continuation token is the whole of its state. The boundary this module defends is
//! *idempotence of output* — a poll loop that re-reads the same page and prints it again is worse
//! than no follow at all, because the operator cannot tell a repeated line from a repeated event.
//! So every line that goes out is counted, and a server that hands back the log from the start
//! (which it does when it declines to issue a token) has its prefix skipped rather than reprinted.
//!
//! Everything is passed through [`swf_domain::sanitize`] on the way out, again. The adapter
//! already scrubs at the point the bytes enter the process; doing it here too costs nothing and
//! means a fake adapter, a cached page or a future source cannot reintroduce an escape sequence
//! into somebody's terminal. Rule 7 is not a place to be clever about deduplicating work.

use std::time::Duration;

use swf_adapters::traits::Runs;
use swf_domain::ids::JobId;
use swf_domain::sanitize::sanitize_line;
use swf_domain::states::{is_active, is_failed};
use tokio_util::sync::CancellationToken;

use crate::ops::{OpsError, Result};

/// How long to wait between polls when following.
pub const DEFAULT_POLL: Duration = Duration::from_secs(2);

/// The attempt an operator means when they do not say: the first one.
pub const DEFAULT_ATTEMPT: u32 = 1;

/// Which log to read, and how hard to keep reading it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogOpts {
    /// The task id. `None` means "pick the one worth looking at" — see [`resolve_task`].
    pub task: Option<String>,
    /// The try number. Airflow numbers attempts from 1.
    pub attempt: u32,
    /// How long to wait between polls when following.
    pub poll: Duration,
    /// A bound on the number of polls, so a test — and a `--follow` in a script — terminates.
    pub max_polls: Option<usize>,
}

impl Default for LogOpts {
    fn default() -> Self {
        Self {
            task: None,
            attempt: DEFAULT_ATTEMPT,
            poll: DEFAULT_POLL,
            max_polls: None,
        }
    }
}

impl LogOpts {
    /// Read this task rather than choosing one.
    pub fn for_task(mut self, task: impl Into<String>) -> Self {
        self.task = Some(task.into());
        self
    }

    /// Read this attempt.
    pub fn attempt(mut self, attempt: u32) -> Self {
        self.attempt = attempt;
        self
    }
}

/// One poll's worth of log.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogStream {
    /// Which task was actually read — it may have been chosen for the operator.
    pub task: String,
    /// The attempt that was read.
    pub attempt: u32,
    /// The lines, sanitised.
    pub lines: Vec<String>,
    /// The token to continue with, or `None` when Airflow says the log is complete.
    pub continuation_token: Option<String>,
}

impl LogStream {
    /// True when there will be nothing more for this attempt.
    pub fn complete(&self) -> bool {
        self.continuation_token.is_none()
    }
}

/// One poll of one task attempt's log.
pub async fn fetch(
    runs: &dyn Runs,
    job: &JobId,
    opts: &LogOpts,
    cancel: &CancellationToken,
) -> Result<LogStream> {
    let task = task_for(runs, job, opts, cancel).await?;
    let page = runs.logs(job, &task, opts.attempt, None, cancel).await?;
    Ok(LogStream {
        task,
        attempt: opts.attempt,
        lines: page.lines.iter().map(|line| sanitize_line(line)).collect(),
        continuation_token: page.continuation_token,
    })
}

/// Poll until the log is complete, handing every *new* line to `sink`.
///
/// Answers how many lines were emitted. Cancellation is not a failure: the operator pressed a key,
/// and the lines already handed over stay handed over.
pub async fn follow(
    runs: &dyn Runs,
    job: &JobId,
    opts: &LogOpts,
    sink: &mut dyn FnMut(&str),
    cancel: &CancellationToken,
) -> Result<usize> {
    let task = task_for(runs, job, opts, cancel).await?;
    let mut token: Option<String> = None;
    let mut emitted = 0usize;
    let mut polls = 0usize;

    loop {
        if cancel.is_cancelled() {
            return Ok(emitted);
        }
        let page = match runs
            .logs(job, &task, opts.attempt, token.as_deref(), cancel)
            .await
        {
            Ok(page) => page,
            Err(err) if err.is_cancelled() => return Ok(emitted),
            Err(err) => return Err(err.into()),
        };
        polls += 1;

        // A server that answered without a token has started the log again from the top. Skipping
        // what has already been handed over is the difference between a follow and an echo.
        let skip = if token.is_none() && emitted > 0 {
            emitted
        } else {
            0
        };
        for line in page.lines.iter().skip(skip) {
            sink(&sanitize_line(line));
            emitted += 1;
        }

        token = page.continuation_token.clone();
        if token.is_none() {
            return Ok(emitted);
        }
        if opts.max_polls.is_some_and(|max| polls >= max) {
            return Ok(emitted);
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(emitted),
            () = tokio::time::sleep(opts.poll) => {}
        }
    }
}

/// The task the operator named, or the one they meant.
async fn task_for(
    runs: &dyn Runs,
    job: &JobId,
    opts: &LogOpts,
    cancel: &CancellationToken,
) -> Result<String> {
    match &opts.task {
        Some(task) if !task.trim().is_empty() => Ok(task.trim().to_string()),
        _ => resolve_task(runs, job, cancel).await,
    }
}

/// Choose the task worth reading for a job: the failure, else what is running, else the last one.
///
/// The order is the order a person would look. A failed task is why they ran the command; a
/// running one is what they are waiting for; and with neither, the most recently reported task is
/// the end of the story so far.
pub async fn resolve_task(
    runs: &dyn Runs,
    job: &JobId,
    cancel: &CancellationToken,
) -> Result<String> {
    let tasks = runs.task_states(&job.run(), cancel).await?;
    let mine: Vec<_> = tasks
        .rows
        .iter()
        .filter(|task| task.map_index == job.map_index)
        .collect();
    if mine.is_empty() {
        return Err(
            OpsError::not_found(format!("{job} has no task instances yet"))
                .with_hint("swf jobs inspect the run first, or pass --task"),
        );
    }
    let pick = mine
        .iter()
        .find(|task| is_failed(task.state_or_none()))
        .or_else(|| mine.iter().find(|task| is_active(task.state_or_none())))
        .or_else(|| mine.last())
        .map(|task| task.task_id.clone());
    pick.ok_or_else(|| OpsError::not_found(format!("{job} has no task instances yet")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_defaults_read_the_first_attempt_and_choose_the_task() {
        let opts = LogOpts::default();
        assert_eq!(opts.attempt, 1);
        assert!(opts.task.is_none());
        assert_eq!(opts.poll, Duration::from_secs(2));
        let named = LogOpts::default()
            .for_task(" job.build_and_test ")
            .attempt(3);
        assert_eq!(named.attempt, 3);
        assert_eq!(named.task.as_deref(), Some(" job.build_and_test "));
    }

    #[test]
    fn a_stream_is_complete_only_when_there_is_no_token() {
        let stream = LogStream {
            task: "job.build_and_test".into(),
            attempt: 1,
            lines: vec!["a".into()],
            continuation_token: Some("t".into()),
        };
        assert!(!stream.complete());
        assert!(LogStream::default().complete());
    }
}
