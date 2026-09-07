//! Everything the interface wants done, done off the render thread and reported back as a message.
//!
//! The boundary this module defends is the frame. `update` decides *what* should happen and hands
//! back an [`Effect`]; nothing on the path from a keystroke to a repaint is allowed to await a
//! socket, so an Airflow that has stopped answering slows down exactly one pane and not the
//! cursor. Every result comes back through the same bounded channel the keyboard uses, which is
//! what keeps there being one state-update path.
//!
//! It is also where obsolete work is abandoned. When the operator moves to another run — or
//! switches context, which rebuilds the whole runtime — the in-flight token is cancelled and
//! replaced. A cancelled read is not a failure and is never reported as one: the operator simply
//! stopped wanting the answer (`00-architecture.md` §D-A).

use std::process::Stdio;
use std::sync::{Arc, Mutex};

use swf_app::delivery::VerifyOpts;
use swf_app::gates::{AnswerOpts, Decision};
use swf_app::logs::LogOpts;
use swf_app::ops::{Ops, OpsError};
use swf_app::submit::SubmitRequest;
use swf_domain::ids::{DeliveryId, GateId, JobId, RunRef};
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

use crate::model::Msg;

/// How many messages may queue between the workers and the update loop.
///
/// Bounded on purpose (§7). A refresh that produces messages faster than the screen can absorb
/// them should make the *effect* wait, not grow a queue that eventually is the whole snapshot
/// history in memory.
pub const CHANNEL_CAPACITY: usize = 64;

/// One piece of work the update function wants performed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Abandon whatever is in flight. Issued when the selected run changes.
    Cancel,
    /// Read every source once.
    Refresh,
    /// Fetch one gate's evidence, and the revision to answer against.
    Review(GateId),
    /// Answer one gate, re-validating first.
    Answer {
        /// Which gate.
        id: GateId,
        /// Yes or no.
        decision: Decision,
        /// The revision the operator was shown.
        expect: Option<String>,
        /// What the activity log calls this.
        label: String,
    },
    /// Submit issues to a blueprint.
    Trigger {
        /// The blueprint DAG.
        dag_id: String,
        /// The issue refs.
        issues: Vec<String>,
        /// What the activity log calls this.
        label: String,
    },
    /// Mark one Airflow run failed.
    Stop {
        /// Which run.
        run: RunRef,
        /// What the activity log calls this.
        label: String,
    },
    /// Open a URL in the operator's browser.
    Open(String),
    /// Resolve a run's UI link, then open it.
    OpenRun(RunRef),
    /// Remove one sandbox, through the adapter's ownership guard.
    RemoveSandbox {
        /// The sandbox name.
        name: String,
        /// What the activity log calls this.
        label: String,
    },
    /// Verify one delivery, keeping its three claims apart.
    Verify(String),
    /// Fetch one task attempt's log.
    Logs {
        /// Whose log.
        job: JobId,
        /// Which task, or the one worth looking at.
        task: Option<String>,
    },
    /// Nothing to do here — the loop exits on its own.
    Quit,
}

/// The bridge between [`Effect`] and the operations layer.
///
/// Holds the `Ops` the whole session runs against and the sender every worker reports through.
/// Switching context means building a new `Ops` and a new `Runtime`, which cancels every request
/// the old environment had outstanding — there is no path by which an answer from the previous
/// factory can land in the new one's screen.
pub struct Runtime {
    ops: Arc<Ops>,
    tx: Sender<Msg>,
    token: Mutex<CancellationToken>,
}

impl Runtime {
    /// Wire an operations layer to a message channel.
    pub fn new(ops: Arc<Ops>, tx: Sender<Msg>) -> Self {
        Self {
            ops,
            tx,
            token: Mutex::new(CancellationToken::new()),
        }
    }

    /// The token this moment's work should carry.
    pub fn token(&self) -> CancellationToken {
        match self.token.lock() {
            Ok(guard) => guard.clone(),
            // A poisoned lock means a worker panicked mid-swap. Handing back a live token loses
            // one cancellation; refusing to hand one back loses the interface.
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    /// Abandon everything outstanding and issue a fresh token for what comes next.
    pub fn cancel_in_flight(&self) {
        let mut guard = match self.token.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.cancel();
        *guard = CancellationToken::new();
    }

    /// Cancel everything for good, on the way out.
    pub fn shutdown(&self) {
        if let Ok(guard) = self.token.lock() {
            guard.cancel();
        }
    }

    /// Start one effect. Returns immediately; the answer arrives as a [`Msg`].
    pub fn dispatch(&self, effect: Effect) {
        match effect {
            Effect::Cancel => self.cancel_in_flight(),
            Effect::Quit => self.shutdown(),
            other => {
                let ops = Arc::clone(&self.ops);
                let tx = self.tx.clone();
                let token = self.token();
                tokio::spawn(async move { perform(ops, tx, token, other).await });
            }
        }
    }
}

/// Run one effect and report what happened.
async fn perform(ops: Arc<Ops>, tx: Sender<Msg>, cancel: CancellationToken, effect: Effect) {
    let msg = match effect {
        // `snapshot` never fails: per-source errors ride inside it, which is what keeps one dead
        // service from blanking the other three panes (rule 4).
        Effect::Refresh => Some(Msg::Snapshot(Box::new(ops.snapshot_default(&cancel).await))),
        Effect::Review(id) => match ops.gate_review(&id, &cancel).await {
            Ok(review) => Some(Msg::Review(Box::new(review))),
            Err(err) => failed(format!("review {id}"), err),
        },
        Effect::Answer {
            id,
            decision,
            expect,
            label,
        } => {
            let opts = AnswerOpts {
                expect,
                force: false,
                confirm_delay: None,
            };
            match ops.gate_answer(&id, decision, &opts, &cancel).await {
                Ok(answer) => Some(Msg::Answered {
                    id: answer.id,
                    decision: answer.decision,
                    forced: answer.forced,
                }),
                // A gate someone else answered first, or evidence that moved under the operator,
                // is a normal outcome of two people working the same queue — a refusal to report,
                // not a failure to retry (rule 5).
                Err(err) => Some(classify(label, err)),
            }
        }
        Effect::Trigger {
            dag_id,
            issues,
            label,
        } => {
            let request = SubmitRequest {
                issues,
                blueprint: dag_id,
                targets: Vec::new(),
                harness: None,
                factory_id: None,
            };
            match ops.submit(&request, &cancel).await {
                Ok(submission) => Some(Msg::Ok(format!(
                    "{label} -> {}/{} {}",
                    submission.dag_id, submission.run_id, submission.url
                ))),
                Err(err) => Some(classify(label, err)),
            }
        }
        Effect::Stop { run, label } => match ops.stop_run(&run, &cancel).await {
            // Named for what Airflow actually did. Nothing was killed and no sandbox was cleaned
            // up, and saying otherwise would be the lie non-negotiable 9 exists to prevent.
            Ok(()) => Some(Msg::Ok(format!("{label}: the run is marked failed"))),
            Err(err) => Some(classify(label, err)),
        },
        Effect::RemoveSandbox { name, label } => match ops.remove_sandbox(&name, &cancel).await {
            Ok(_) => Some(Msg::Ok(label)),
            Err(err) => Some(classify(label, err)),
        },
        Effect::OpenRun(run) => match ops.run_url(&run) {
            Ok(url) => {
                open_url(&url);
                Some(Msg::Note(format!("open {url}")))
            }
            Err(err) => Some(classify(format!("open {run}"), err)),
        },
        Effect::Open(url) => {
            open_url(&url);
            Some(Msg::Note(format!("open {url}")))
        }
        Effect::Verify(id) => match DeliveryId::parse(&id) {
            Ok(delivery) => {
                let opts = VerifyOpts::default();
                match ops.verify_delivery(&delivery, &opts, &cancel).await {
                    // The three claims stay three claims: the verdict word is the whole point of
                    // the command, so it is what the log line says (non-negotiable 10).
                    Ok(report) => Some(Msg::Ok(format!(
                        "verify {id}: {} (workflow {}, published {}, verified {})",
                        report.verdict.as_str(),
                        report.workflow_succeeded,
                        report.branch_published,
                        report.independently_verified,
                    ))),
                    Err(err) => Some(classify(format!("verify {id}"), err)),
                }
            }
            Err(err) => Some(Msg::Failed {
                label: format!("verify {id}"),
                message: err.to_string(),
            }),
        },
        Effect::Logs { job, task } => {
            let opts = LogOpts {
                task,
                ..LogOpts::default()
            };
            match ops.logs(&job, &opts, &cancel).await {
                Ok(stream) => Some(Msg::LogLines(
                    std::iter::once(format!("--- {job} {} #{} ---", stream.task, stream.attempt))
                        .chain(stream.lines)
                        .collect(),
                )),
                Err(err) => failed(format!("logs {job}"), err),
            }
        }
        Effect::Cancel | Effect::Quit => None,
    };

    if let Some(msg) = msg {
        // The send awaits rather than dropping: back-pressure belongs on the worker, and losing
        // the result of a write the operator authorised is worse than making it wait a frame.
        let _ = tx.send(msg).await;
    }
}

/// A read that failed, unless the caller had already stopped wanting it.
fn failed(label: String, err: OpsError) -> Option<Msg> {
    if err.is_cancelled() {
        return None;
    }
    Some(Msg::Failed {
        label,
        message: err.message,
    })
}

/// Split an operation's failure into "the factory said no" and "something broke".
///
/// A refusal is a decision that has already been made — a gate answered by someone else, a
/// sandbox that is not the operator's, a policy that says no. Retrying is wrong and repainting
/// over the reason is worse, so these two go back as different messages.
fn classify(label: String, err: OpsError) -> Msg {
    if err.is_cancelled() {
        return Msg::Note(format!("{label}: abandoned"));
    }
    let message = match &err.hint {
        Some(hint) => format!("{} (fix: {hint})", err.message),
        None => err.message.clone(),
    };
    match err.exit_code() {
        1 | 6 => Msg::Refused { label, message },
        _ => Msg::Failed { label, message },
    }
}

/// Hand a URL to whatever the platform uses to open one.
///
/// Argv is a vector and the URL is an argument, never part of a shell string: it came from a
/// service, and a link is a nicety that must never become a way to run a command. A failure is
/// swallowed on purpose — a headless box has no browser, and the URL has already been logged
/// where the operator can copy it (`02-herd-tui.md` §9.13).
fn open_url(url: &str) {
    let (program, leading): (&str, &[&str]) = if cfg!(target_os = "macos") {
        ("open", &[])
    } else if cfg!(target_os = "windows") {
        ("cmd", &["/C", "start", ""])
    } else {
        ("xdg-open", &[])
    };
    let _ = std::process::Command::new(program)
        .args(leading)
        .arg(url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
}

#[cfg(test)]
mod tests {
    use super::*;
    use swf_app::ops::ErrorKind;

    #[test]
    fn a_cancelled_read_is_never_shown_to_the_operator_as_a_failure() {
        assert!(failed("review x".into(), OpsError::cancelled()).is_none());
        assert!(matches!(
            classify("approve x".into(), OpsError::cancelled()),
            Msg::Note(_)
        ));
    }

    #[test]
    fn a_conflict_is_a_refusal_and_an_outage_is_a_failure() {
        assert!(matches!(
            classify("approve x".into(), OpsError::conflict("answered first")),
            Msg::Refused { .. }
        ));
        assert!(matches!(
            classify("approve x".into(), OpsError::operational("policy said no")),
            Msg::Refused { .. }
        ));
        for kind in [ErrorKind::Auth, ErrorKind::Unreachable, ErrorKind::NotFound] {
            assert!(
                matches!(
                    classify("approve x".into(), OpsError::new(kind, "boom")),
                    Msg::Failed { .. }
                ),
                "{kind:?} is not a policy decision"
            );
        }
    }

    #[test]
    fn a_hint_travels_with_the_message_so_the_fix_is_on_screen() {
        let err = OpsError::conflict("someone answered it first").with_hint("swf gates list");
        match classify("approve x".into(), err) {
            Msg::Refused { message, .. } => assert!(message.contains("fix: swf gates list")),
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cancelling_replaces_the_token_so_the_next_request_is_not_born_dead() {
        let ctx = swf_app::context::Context::builtin();
        let ops = Arc::new(Ops::builder(ctx).build());
        let (tx, _rx) = tokio::sync::mpsc::channel(CHANNEL_CAPACITY);
        let runtime = Runtime::new(ops, tx);
        let first = runtime.token();
        runtime.cancel_in_flight();
        assert!(first.is_cancelled());
        assert!(!runtime.token().is_cancelled());
    }
}
