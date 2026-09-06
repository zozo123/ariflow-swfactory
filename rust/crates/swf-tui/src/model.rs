//! The whole interface as data, and the one function allowed to change it.
//!
//! Elm's shape, for a reason that is specific to this product: an operator is looking at a factory
//! that keeps moving while they read it, so *when* the screen changed and *why* has to be
//! answerable. Every change to what is on screen arrives as a [`Msg`] and goes through [`update`],
//! which returns the work it wants done rather than doing it. Nothing here awaits, opens a socket
//! or reads a clock of its own — hand it the same messages and it produces the same screen, which
//! is what makes a rendering test worth writing.
//!
//! The boundary this module defends is the cursor. Selection is keyed by [`JobId`]/[`GateId`] and
//! never by a row number, so a refresh that reorders the table cannot move an operator's cursor
//! onto a different job between the moment they read a row and the moment they press `a`. That is
//! a deliberate departure from the Python `herd`, which preserved the row *index*
//! (`02-herd-tui.md` §10.17); approving the wrong job because the table sorted itself is not a
//! risk worth compatibility.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use chrono::{DateTime, Utc};
use crossterm::event::KeyEvent;
use swf_app::attention::Attention;
use swf_app::context::{Auth, Context};
use swf_app::delivery::{from_pr, Delivery};
use swf_app::gates::{Decision, GateReview};
use swf_domain::ids::{GateId, JobId, RunRef};
use swf_domain::metrics::table_value;
use swf_domain::model::{Snapshot, SourceHealth};
use swf_domain::rollup::{job_index, stage_progress};
use swf_domain::sanitize::{sanitize_block, sanitize_line};

use crate::effect::Effect;
use crate::event::{map_key, Action};
use crate::theme::{state_tone, Tone};

/// How many activity lines the log pane keeps.
///
/// Bounded because a failing source logs once per poll for as long as it is broken, and an
/// unbounded buffer turns a service outage into a memory leak on the operator's laptop.
pub const LOG_CAPACITY: usize = 500;

/// A source is stale once its data is older than this many refresh intervals.
pub const STALE_FACTOR: u32 = 3;

/// Below this width the right-hand detail pane is folded away.
pub const NARROW_COLS: u16 = 100;

/// Below this width the left navigation goes too, and the centre gets the whole screen.
pub const VERY_NARROW_COLS: u16 = 72;

/// How often the screen re-reads the factory when nobody has pressed anything.
pub const DEFAULT_REFRESH: Duration = Duration::from_secs(5);

/// The DAG a trigger falls back to when the context names none.
pub const FALLBACK_DAG: &str = "factory";

/// The seven screens, in the order `1`–`7` select them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum View {
    /// What needs a human right now.
    Attention,
    /// Every job the factory is working on.
    Jobs,
    /// One job: identity, progress, evidence, problems.
    JobDetail,
    /// One gate, with the evidence and the actor, before it is answered.
    Review,
    /// What the factory has published.
    Deliveries,
    /// Sandboxes and the health of each source.
    Infrastructure,
    /// The committed metrics history.
    History,
}

impl View {
    /// Every view, in navigation order.
    pub const ALL: [View; 7] = [
        View::Attention,
        View::Jobs,
        View::JobDetail,
        View::Review,
        View::Deliveries,
        View::Infrastructure,
        View::History,
    ];

    /// The name in the navigation and in the command palette.
    pub fn title(self) -> &'static str {
        match self {
            View::Attention => "attention",
            View::Jobs => "jobs",
            View::JobDetail => "job detail",
            View::Review => "review",
            View::Deliveries => "deliveries",
            View::Infrastructure => "infrastructure",
            View::History => "history",
        }
    }

    /// Its position in `1`–`7`.
    pub fn number(self) -> usize {
        View::ALL.iter().position(|v| *v == self).unwrap_or(0) + 1
    }

    /// The view a digit selects, or `None` for a digit outside `1`–`7`.
    pub fn from_number(n: usize) -> Option<View> {
        View::ALL.get(n.checked_sub(1)?).copied()
    }

    /// The next view along, wrapping.
    pub fn next(self) -> View {
        View::from_number(self.number() % View::ALL.len() + 1).unwrap_or(View::Attention)
    }

    /// The previous view, wrapping.
    pub fn prev(self) -> View {
        let n = self.number();
        let back = if n == 1 { View::ALL.len() } else { n - 1 };
        View::from_number(back).unwrap_or(View::Attention)
    }
}

/// What a row stands for, which decides what the detail pane shows and what a key does to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKind {
    /// An approval waiting for an answer.
    Gate,
    /// One mapped job of a run.
    Job,
    /// One task instance of a job.
    Task,
    /// A job that failed.
    Failure,
    /// A published branch or PR.
    Delivery,
    /// A sandbox.
    Sandbox,
    /// One source's freshness and last error.
    Source,
    /// A line of the metrics table.
    Metric,
}

/// One line of whatever table the active view shows.
///
/// `key` is the identity the selection holds — a `JobId`, a `GateId`, a delivery id, a sandbox
/// name. It is never a row number, which is the whole point (see the module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// The identity this row addresses.
    pub key: String,
    /// What it is.
    pub kind: RowKind,
    /// The cells, already sanitised, in column order.
    pub cells: Vec<String>,
    /// How the row should feel, so the renderer never has to interpret a state itself.
    pub tone: Tone,
}

impl Row {
    fn new(key: impl Into<String>, kind: RowKind, cells: Vec<String>, tone: Tone) -> Self {
        Self {
            key: key.into(),
            kind,
            cells: cells.iter().map(|c| sanitize_line(c)).collect(),
            tone,
        }
    }

    /// Everything in this row as one lowercase haystack, for `/`.
    fn haystack(&self) -> String {
        self.cells.join(" ").to_lowercase()
    }
}

/// A bounded activity log that would rather lose history than memory.
///
/// The drop count is public and rendered. A ring that silently forgets is indistinguishable from
/// a source that stopped reporting, and telling those two apart at 2 a.m. is the entire job.
#[derive(Debug, Clone)]
pub struct LogRing {
    lines: VecDeque<String>,
    capacity: usize,
    dropped: usize,
}

impl Default for LogRing {
    fn default() -> Self {
        Self::new(LOG_CAPACITY)
    }
}

impl LogRing {
    /// A ring holding at most `capacity` lines.
    pub fn new(capacity: usize) -> Self {
        Self {
            lines: VecDeque::new(),
            capacity: capacity.max(1),
            dropped: 0,
        }
    }

    /// Add one line, dropping the oldest if the ring is full.
    pub fn push(&mut self, line: impl AsRef<str>) {
        while self.lines.len() >= self.capacity {
            self.lines.pop_front();
            self.dropped = self.dropped.saturating_add(1);
        }
        self.lines.push_back(sanitize_line(line.as_ref()));
    }

    /// The lines held, oldest first.
    pub fn lines(&self) -> impl DoubleEndedIterator<Item = &str> {
        self.lines.iter().map(String::as_str)
    }

    /// How many lines were dropped to make room. Rendered whenever it is not zero.
    pub fn dropped(&self) -> usize {
        self.dropped
    }

    /// How many lines are held.
    pub fn len(&self) -> usize {
        self.lines.len()
    }

    /// True while nothing has been logged.
    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    /// The most recent line, which is what the status bar echoes.
    pub fn last(&self) -> Option<&str> {
        self.lines.back().map(String::as_str)
    }
}

/// The `/` filter over the active table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Search {
    /// True while the operator is typing into it.
    pub open: bool,
    /// The filter, which stays in force after `enter` closes the box.
    pub query: String,
}

/// One thing the command palette can run.
#[derive(Debug, Clone, Copy)]
pub struct Command {
    /// What the operator types.
    pub name: &'static str,
    /// One line saying what it does.
    pub help: &'static str,
    /// What it actually runs — the same action the key map produces.
    pub action: Action,
}

/// Everything `:` offers. It is exactly the key map, spelled out, so that a key nobody remembers
/// is still reachable by name.
pub const COMMANDS: &[Command] = &[
    Command {
        name: "attention",
        help: "what needs a human right now",
        action: Action::GoView(View::Attention),
    },
    Command {
        name: "jobs",
        help: "every job the factory is working on",
        action: Action::GoView(View::Jobs),
    },
    Command {
        name: "review",
        help: "the selected gate, with its evidence",
        action: Action::GoView(View::Review),
    },
    Command {
        name: "deliveries",
        help: "what the factory has published",
        action: Action::GoView(View::Deliveries),
    },
    Command {
        name: "infrastructure",
        help: "sandboxes and per-source health",
        action: Action::GoView(View::Infrastructure),
    },
    Command {
        name: "history",
        help: "the committed metrics history",
        action: Action::GoView(View::History),
    },
    Command {
        name: "refresh",
        help: "re-read every source now",
        action: Action::Refresh,
    },
    Command {
        name: "approve",
        help: "answer the selected gate yes",
        action: Action::Approve,
    },
    Command {
        name: "reject",
        help: "answer the selected gate no",
        action: Action::Reject,
    },
    Command {
        name: "trigger",
        help: "submit issues to a blueprint",
        action: Action::Trigger,
    },
    Command {
        name: "stop",
        help: "mark the selected Airflow run failed",
        action: Action::Stop,
    },
    Command {
        name: "open",
        help: "open the selected run or delivery in a browser",
        action: Action::Open,
    },
    Command {
        name: "verify",
        help: "verify the selected delivery",
        action: Action::Verify,
    },
    Command {
        name: "logs",
        help: "fetch the selected job's current task log",
        action: Action::Logs,
    },
    Command {
        name: "remove-sandbox",
        help: "remove the selected sandbox",
        action: Action::Remove,
    },
    Command {
        name: "log",
        help: "show or hide the activity pane",
        action: Action::ToggleLog,
    },
    Command {
        name: "help",
        help: "the key map",
        action: Action::Help,
    },
    Command {
        name: "quit",
        help: "leave; remote work keeps running",
        action: Action::Quit,
    },
];

/// The `:` palette's state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Palette {
    /// True while it is on screen.
    pub open: bool,
    /// What has been typed.
    pub input: String,
    /// Which match is highlighted.
    pub index: usize,
}

impl Palette {
    /// The commands matching what has been typed, in declaration order.
    pub fn matches(&self) -> Vec<&'static Command> {
        let needle = self.input.trim().to_lowercase();
        COMMANDS
            .iter()
            .filter(|c| needle.is_empty() || c.name.contains(&needle))
            .collect()
    }

    /// The highlighted command, if the filter matches anything.
    pub fn selected(&self) -> Option<&'static Command> {
        let matches = self.matches();
        matches
            .get(self.index.min(matches.len().saturating_sub(1)))
            .copied()
    }
}

/// A mutation that has been described to the operator and is waiting for `y`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pending {
    /// Answer a gate. `expect` is the revision the operator was shown, and a mismatch at write
    /// time is a conflict rather than a silent overwrite.
    Answer {
        /// Which gate.
        id: GateId,
        /// Yes or no.
        decision: Decision,
        /// The evidence revision as reviewed.
        expect: Option<String>,
    },
    /// Mark an Airflow run failed.
    Stop(RunRef),
    /// Remove one sandbox.
    RemoveSandbox(String),
    /// Submit issues to a blueprint.
    Trigger {
        /// The blueprint DAG.
        dag_id: String,
        /// The issue refs, already parsed.
        issues: Vec<String>,
    },
}

/// A confirmation modal: the sentence, the log label, and what happens on `y`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Confirm {
    /// The question, naming every identity involved.
    pub question: String,
    /// What the activity log calls this, whichever way it is answered.
    pub label: String,
    /// What `y` does.
    pub pending: Pending,
}

/// What a text prompt is collecting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromptKind {
    /// The issue refs for a trigger against this blueprint.
    TriggerIssues {
        /// The blueprint DAG the issues will be submitted to.
        dag_id: String,
    },
}

/// A one-line text prompt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prompt {
    /// The question above the input.
    pub title: String,
    /// The example under it.
    pub placeholder: String,
    /// What has been typed.
    pub input: String,
    /// What to do with it.
    pub kind: PromptKind,
}

/// Which key map is in force.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Navigating.
    Normal,
    /// Typing into `/`.
    Search,
    /// Typing into `:`.
    Palette,
    /// Typing into a prompt.
    Prompt,
    /// Answering `y`/`n`.
    Confirm,
    /// Reading the key map.
    Help,
}

/// The environment line across the top: which factory, and as whom.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Env {
    /// The context name.
    pub context: String,
    /// The Airflow base URL.
    pub airflow_url: String,
    /// `owner/name`, or `-`.
    pub repo: String,
    /// The sandbox owner, or `-`.
    pub owner: String,
    /// Who an approval will be recorded as.
    pub actor: String,
    /// The blueprints this context knows about, for `t`.
    pub dag_ids: Vec<String>,
}

impl Env {
    /// Read a context's identity, without reading a single secret.
    pub fn from_context(ctx: &Context) -> Self {
        Self {
            context: ctx.name.clone(),
            airflow_url: ctx.airflow_url.clone(),
            repo: ctx.repo.clone().unwrap_or_else(|| "-".into()),
            owner: ctx.owner.clone().unwrap_or_else(|| "-".into()),
            actor: actor_of(&ctx.auth),
            dag_ids: ctx.dag_ids.clone(),
        }
    }

    /// The blueprint a trigger defaults to.
    pub fn default_dag(&self) -> String {
        self.dag_ids
            .first()
            .cloned()
            .unwrap_or_else(|| FALLBACK_DAG.to_string())
    }
}

/// Who an approval will be recorded as, derived from the auth the context names.
///
/// Cosmetic and labelled as such: Airflow records the credential's own user, so a shared token
/// makes every approval look like the same person (`02-herd-tui.md` §10.30). Showing the name of
/// the variable the token comes from is the most honest thing this side of the wire can say.
pub fn actor_of(auth: &Auth) -> String {
    match auth {
        Auth::None => "anonymous".to_string(),
        Auth::TokenEnv { var } => format!("${var}"),
        Auth::Basic { user, .. } => user.clone(),
    }
}

/// Which identity is selected in each view.
///
/// One field per kind rather than one cursor, so moving to the Deliveries tab and back does not
/// lose the job that was being watched.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Selection {
    /// The Attention row.
    pub attention: Option<String>,
    /// The job, in every view that has one.
    pub job: Option<JobId>,
    /// The task, inside the job detail.
    pub task: Option<String>,
    /// The gate under review.
    pub gate: Option<GateId>,
    /// The delivery.
    pub delivery: Option<String>,
    /// The sandbox or source row.
    pub infra: Option<String>,
    /// The metrics row.
    pub metric: Option<String>,
}

/// Everything on screen.
pub struct Model {
    /// The active view.
    pub view: View,
    /// Where `esc` returns to from a detail view.
    pub back: View,
    /// The last completed collection pass.
    pub snapshot: Snapshot,
    /// The roll-up of that pass into "what needs a human".
    pub attention: Attention,
    /// The gate currently being reviewed, if the Review view has been opened.
    pub review: Option<GateReview>,
    /// Per-source freshness, kept across passes so a failed read still shows the last good time.
    pub sources: BTreeMap<String, SourceHealth>,
    /// What is selected, by identity.
    pub selection: Selection,
    /// The activity log.
    pub log: LogRing,
    /// Whether the log pane is showing.
    pub log_open: bool,
    /// Whether the key map is showing.
    pub help_open: bool,
    /// The command palette.
    pub palette: Palette,
    /// The table filter.
    pub search: Search,
    /// The confirmation waiting for `y`.
    pub confirm: Option<Confirm>,
    /// The text prompt waiting for `enter`.
    pub prompt: Option<Prompt>,
    /// The one-line message under the tables.
    pub status: String,
    /// Which factory, and as whom.
    pub env: Env,
    /// The clock, injected so a rendering test is a pure function of its inputs.
    pub now: DateTime<Utc>,
    /// When the last pass completed.
    pub last_refresh: Option<DateTime<Utc>>,
    /// How often to re-read.
    pub refresh: Duration,
    /// How many reads are outstanding.
    pub in_flight: usize,
    /// The terminal size, so the layout decisions are testable without a terminal.
    pub size: (u16, u16),
    /// Set once the operator has asked to leave.
    pub quit: bool,
}

impl Model {
    /// An empty screen pointed at one environment.
    pub fn new(env: Env, refresh: Duration, now: DateTime<Utc>) -> Self {
        Self {
            view: View::Attention,
            back: View::Attention,
            snapshot: Snapshot::new(now),
            attention: Attention::default(),
            review: None,
            sources: BTreeMap::new(),
            selection: Selection::default(),
            log: LogRing::default(),
            log_open: true,
            help_open: false,
            palette: Palette::default(),
            search: Search::default(),
            confirm: None,
            prompt: None,
            status: String::new(),
            env,
            now,
            last_refresh: None,
            refresh,
            in_flight: 0,
            size: (120, 40),
            quit: false,
        }
    }

    /// Which key map is in force. Overlays shadow each other in the order they can be opened.
    pub fn mode(&self) -> Mode {
        if self.confirm.is_some() {
            Mode::Confirm
        } else if self.prompt.is_some() {
            Mode::Prompt
        } else if self.palette.open {
            Mode::Palette
        } else if self.help_open {
            Mode::Help
        } else if self.search.open {
            Mode::Search
        } else {
            Mode::Normal
        }
    }

    /// True once the terminal is too narrow for the detail pane.
    pub fn narrow(&self) -> bool {
        self.size.0 < NARROW_COLS
    }

    /// True once the terminal is too narrow for the navigation as well.
    pub fn very_narrow(&self) -> bool {
        self.size.0 < VERY_NARROW_COLS
    }

    /// How old data has to be before it is called stale: three refresh intervals (§7).
    pub fn stale_after(&self) -> chrono::Duration {
        chrono::Duration::from_std(self.refresh * STALE_FACTOR)
            .unwrap_or_else(|_| chrono::Duration::seconds(15))
    }

    /// The sources that are stale or failing, by name, sorted — the header's badges.
    pub fn unhealthy(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .snapshot
            .errors
            .iter()
            .map(|e| e.source.clone())
            .collect();
        names.sort();
        names.dedup();
        names
    }

    /// True when any source's data is older than three refresh intervals.
    pub fn stale(&self) -> bool {
        let threshold = self.stale_after();
        !self.sources.is_empty()
            && self
                .sources
                .values()
                .any(|h| h.is_stale(self.now, threshold))
    }

    /// True when a collection read stopped at its page bound. Rule 3: this must reach the screen.
    pub fn truncated(&self) -> bool {
        self.sources.values().any(|h| h.truncated)
    }

    /// The column headings of the active view.
    pub fn columns(&self) -> &'static [&'static str] {
        match self.view {
            View::Attention => &["kind", "id", "detail", "status", "age"],
            View::Jobs => &["dag", "run", "job", "issue", "stage", "state"],
            View::JobDetail => &["task", "job", "state"],
            View::Review => &[],
            View::Deliveries => &["#", "title", "checks", "state"],
            View::Infrastructure => &["kind", "name", "status", "detail", "age"],
            View::History => &["metric", "value"],
        }
    }

    /// Every row of the active view, already filtered by `/`.
    ///
    /// Built on demand from the snapshot rather than cached, so there is exactly one description
    /// of what a table contains and the update function and the renderer cannot disagree about it.
    pub fn rows(&self) -> Vec<Row> {
        let all = self.all_rows();
        let needle = self.search.query.trim().to_lowercase();
        if needle.is_empty() {
            return all;
        }
        all.into_iter()
            .filter(|row| row.haystack().contains(&needle))
            .collect()
    }

    fn all_rows(&self) -> Vec<Row> {
        match self.view {
            View::Attention => self.attention_rows(),
            View::Jobs => self.job_rows(),
            View::JobDetail => self.task_rows(),
            View::Review => Vec::new(),
            View::Deliveries => self.delivery_rows(),
            View::Infrastructure => self.infra_rows(),
            View::History => self.metric_rows(),
        }
    }

    fn attention_rows(&self) -> Vec<Row> {
        let mut rows = Vec::new();
        for gate in &self.attention.gates {
            let status = if gate.ready {
                "◆ ready".to_string()
            } else {
                "⋯ arming".to_string()
            };
            rows.push(Row::new(
                &gate.id,
                RowKind::Gate,
                vec![
                    "gate".into(),
                    gate.id.clone(),
                    gate.subject.clone(),
                    status,
                    gate.age.clone(),
                ],
                Tone::Waiting,
            ));
        }
        for job in &self.attention.failures {
            rows.push(Row::new(
                &job.id,
                RowKind::Failure,
                vec![
                    "failure".into(),
                    job.id.clone(),
                    format!("issue {} · {}", job.issue, job.stage),
                    crate::theme::state_label(&job.state),
                    "-".into(),
                ],
                Tone::Bad,
            ));
        }
        for pr in &self.attention.blocked {
            rows.push(Row::new(
                format!("pr#{}", pr.number),
                RowKind::Delivery,
                vec![
                    "delivery".into(),
                    format!("#{}", pr.number),
                    pr.title.clone(),
                    pr.reason.clone(),
                    "-".into(),
                ],
                Tone::Bad,
            ));
        }
        for sandbox in &self.attention.orphans {
            rows.push(Row::new(
                &sandbox.name,
                RowKind::Sandbox,
                vec![
                    "sandbox".into(),
                    sandbox.name.clone(),
                    sandbox.reason.clone(),
                    sandbox.status.clone(),
                    sandbox.age.clone(),
                ],
                Tone::Warn,
            ));
        }
        for err in &self.attention.errors {
            rows.push(Row::new(
                format!("source:{}", err.source),
                RowKind::Source,
                vec![
                    "source".into(),
                    err.source.clone(),
                    err.message.clone(),
                    "✗ unavailable".into(),
                    "-".into(),
                ],
                Tone::Bad,
            ));
        }
        rows
    }

    fn job_rows(&self) -> Vec<Row> {
        self.snapshot
            .jobs()
            .map(|job| {
                Row::new(
                    job.id().to_string(),
                    RowKind::Job,
                    vec![
                        job.dag_id.clone(),
                        job.run_id.clone(),
                        job_index(job.map_index),
                        job.issue.clone(),
                        stage_progress(&job.tasks),
                        crate::theme::state_label(&job.state),
                    ],
                    state_tone(&job.state),
                )
            })
            .collect()
    }

    fn task_rows(&self) -> Vec<Row> {
        let Some(job) = self.selected_job_row() else {
            return Vec::new();
        };
        job.tasks
            .iter()
            .map(|task| {
                let state = task.state_or_none();
                Row::new(
                    format!("{}#{}", task.task_id, task.map_index),
                    RowKind::Task,
                    vec![
                        task.task_id.clone(),
                        job_index(task.map_index),
                        crate::theme::state_label(state),
                    ],
                    state_tone(state),
                )
            })
            .collect()
    }

    /// The deliveries this pass saw, as the operations layer describes them.
    pub fn deliveries(&self) -> Vec<Delivery> {
        self.snapshot.prs.iter().map(from_pr).collect()
    }

    fn delivery_rows(&self) -> Vec<Row> {
        self.deliveries()
            .into_iter()
            .map(|d| {
                let tone = if d.blocked { Tone::Bad } else { Tone::Plain };
                Row::new(
                    d.id.clone(),
                    RowKind::Delivery,
                    vec![
                        format!("#{}", d.number),
                        d.title.clone(),
                        d.checks.clone(),
                        d.state.clone(),
                    ],
                    tone,
                )
            })
            .collect()
    }

    fn infra_rows(&self) -> Vec<Row> {
        let mut rows = Vec::new();
        for sandbox in &self.snapshot.sandboxes {
            rows.push(Row::new(
                format!("sandbox:{}", sandbox.name),
                RowKind::Sandbox,
                vec![
                    "sandbox".into(),
                    sandbox.name.clone(),
                    sandbox.status.clone(),
                    sandbox.created_by.clone(),
                    swf_domain::rollup::age(
                        sandbox.created_at.map(|t| t.with_timezone(&Utc)),
                        self.now,
                    ),
                ],
                Tone::Plain,
            ));
        }
        let threshold = self.stale_after();
        for (name, health) in &self.sources {
            let (status, tone) = source_status(health, self.now, threshold);
            let detail = match (&health.error, health.truncated) {
                (Some(err), _) => err.clone(),
                (None, true) => "read stopped at its page bound — some rows are hidden".into(),
                (None, false) => "-".into(),
            };
            rows.push(Row::new(
                format!("source:{name}"),
                RowKind::Source,
                vec![
                    "source".into(),
                    name.clone(),
                    status,
                    detail,
                    swf_domain::rollup::age(health.fetched_at, self.now),
                ],
                tone,
            ));
        }
        rows
    }

    fn metric_rows(&self) -> Vec<Row> {
        table_value(&self.snapshot.metrics)
            .lines()
            .map(|line| {
                let (label, value) = line.split_once("  ").unwrap_or((line, ""));
                Row::new(
                    label.trim(),
                    RowKind::Metric,
                    vec![label.trim().to_string(), value.trim().to_string()],
                    Tone::Plain,
                )
            })
            .collect()
    }

    /// The index of the selected row among the visible ones.
    ///
    /// `None` when the selection has scrolled out of the filter or vanished from the snapshot;
    /// the renderer then shows no cursor rather than inventing one on an unrelated row.
    pub fn selected_index(&self) -> Option<usize> {
        let key = self.current_key()?;
        self.rows().iter().position(|row| row.key == key)
    }

    /// The identity selected in the active view, as the string a row carries.
    pub fn current_key(&self) -> Option<String> {
        match self.view {
            View::Attention => self.selection.attention.clone(),
            View::Jobs => self.selection.job.as_ref().map(ToString::to_string),
            View::JobDetail => self.selection.task.clone(),
            View::Review => self.selection.gate.as_ref().map(ToString::to_string),
            View::Deliveries => self.selection.delivery.clone(),
            View::Infrastructure => self.selection.infra.clone(),
            View::History => self.selection.metric.clone(),
        }
    }

    /// The selected row itself.
    pub fn current_row(&self) -> Option<Row> {
        let key = self.current_key()?;
        self.rows().into_iter().find(|row| row.key == key)
    }

    /// The job row behind the current selection, wherever the selection came from.
    pub fn selected_job_row(&self) -> Option<&swf_domain::model::JobRow> {
        let id = self.selection.job.as_ref()?;
        self.snapshot.job(id)
    }

    /// The gate behind the current selection.
    pub fn selected_gate(&self) -> Option<&swf_domain::model::Gate> {
        let id = self.selection.gate.as_ref()?;
        self.snapshot.gate(id)
    }

    /// Point the active view's selection at one key, cancelling in-flight work if that changed
    /// which run is being watched (`00-architecture.md` §D-A).
    pub fn select(&mut self, key: &str) -> Vec<Effect> {
        let before = self.selection.job.as_ref().map(JobId::run);
        match self.view {
            View::Attention => {
                self.selection.attention = Some(key.to_string());
                self.adopt_attention_key(key);
            }
            View::Jobs => {
                if let Ok(id) = key.parse::<JobId>() {
                    self.selection.job = Some(id);
                    self.selection.task = None;
                }
            }
            View::JobDetail => self.selection.task = Some(key.to_string()),
            View::Review => {}
            View::Deliveries => self.selection.delivery = Some(key.to_string()),
            View::Infrastructure => self.selection.infra = Some(key.to_string()),
            View::History => self.selection.metric = Some(key.to_string()),
        }
        let after = self.selection.job.as_ref().map(JobId::run);
        if before != after {
            vec![Effect::Cancel]
        } else {
            Vec::new()
        }
    }

    /// An Attention row names a job or a gate; adopt whichever it is so `a`, `s`, `o` and `enter`
    /// all act on the thing the operator is looking at rather than on a stale selection.
    fn adopt_attention_key(&mut self, key: &str) {
        if let Ok(gate) = key.parse::<GateId>() {
            self.selection.job = Some(gate.job.clone());
            self.selection.gate = Some(gate);
        } else if let Ok(job) = key.parse::<JobId>() {
            self.selection.job = Some(job);
        } else if let Some(name) = key.strip_prefix("pr#") {
            self.selection.delivery = self
                .deliveries()
                .into_iter()
                .find(|d| d.number.to_string() == name)
                .map(|d| d.id);
        } else if !key.starts_with("source:") {
            self.selection.infra = Some(format!("sandbox:{key}"));
        }
    }

    /// Move the cursor by `delta` rows, clamped, and select whatever lands under it.
    pub fn move_by(&mut self, delta: isize) -> Vec<Effect> {
        let rows = self.rows();
        if rows.is_empty() {
            return Vec::new();
        }
        let current = self.selected_index().map(|i| i as isize).unwrap_or(-1);
        let next = (current + delta).clamp(0, rows.len() as isize - 1) as usize;
        let key = rows[next].key.clone();
        self.select(&key)
    }

    /// Put the cursor on the first row, or the last if `end`.
    pub fn move_to_edge(&mut self, end: bool) -> Vec<Effect> {
        let rows = self.rows();
        let Some(row) = (if end { rows.last() } else { rows.first() }) else {
            return Vec::new();
        };
        let key = row.key.clone();
        self.select(&key)
    }

    /// Put the cursor on the first row when nothing is selected, so a fresh table is usable
    /// without pressing a key first.
    fn ensure_selection(&mut self) {
        if self.selected_index().is_some() {
            return;
        }
        let Some(first) = self.rows().first().map(|row| row.key.clone()) else {
            return;
        };
        let _ = self.select(&first);
    }

    /// Note one line in the activity log, stamped with the injected clock.
    pub fn note(&mut self, line: impl AsRef<str>) {
        let stamp = self.now.format("%H:%M:%S");
        let line = sanitize_line(line.as_ref());
        self.status = line.clone();
        self.log.push(format!("{stamp} {line}"));
    }

    /// Adopt a completed collection pass without moving the cursor.
    pub fn apply_snapshot(&mut self, snapshot: Snapshot) {
        let mut snapshot = snapshot;
        sanitize_snapshot(&mut snapshot);
        self.attention = Attention::from_snapshot(&snapshot, self.now);
        for (name, health) in &snapshot.health {
            self.sources.insert(name.clone(), health.clone());
        }
        // A source that has stopped answering keeps its last successful stamp so the header can
        // say how old the data on screen actually is, rather than blanking the age.
        for (name, health) in self.sources.iter_mut() {
            health.in_flight = false;
            if !snapshot.health.contains_key(name) {
                health.error = Some("this source was not read in the last pass".into());
            }
        }
        self.snapshot = snapshot;
        self.last_refresh = Some(self.now);
        self.in_flight = self.in_flight.saturating_sub(1);
        // Every source failure is echoed once per pass, on purpose: the badge is permanent, the
        // log line is the heartbeat that says the failure is still happening (§9.6, gotcha 19).
        let failures: Vec<String> = self
            .snapshot
            .errors
            .iter()
            .map(|e| format!("{}: {}", e.source, e.message))
            .collect();
        for line in failures {
            self.note(line);
        }
        self.ensure_selection();
    }
}

/// Sanitise every string that arrived from a service, once, at the point it enters the model.
///
/// Rule 7 of the architecture. Doing it here rather than in the renderer means a string cannot
/// reach a widget by a path that forgot to call it — and a control sequence in a PR title that
/// repositions the operator's cursor is a security bug, not a cosmetic one.
pub fn sanitize_snapshot(snapshot: &mut Snapshot) {
    for run in &mut snapshot.runs {
        run.dag_id = sanitize_line(&run.dag_id);
        run.run_id = sanitize_line(&run.run_id);
        run.state = sanitize_line(&run.state);
        for job in &mut run.jobs {
            job.dag_id = sanitize_line(&job.dag_id);
            job.run_id = sanitize_line(&job.run_id);
            job.issue = sanitize_line(&job.issue);
            job.state = sanitize_line(&job.state);
            for task in &mut job.tasks {
                task.task_id = sanitize_line(&task.task_id);
                task.state = task.state.as_deref().map(sanitize_line);
            }
        }
    }
    for gate in &mut snapshot.gates {
        gate.dag_id = sanitize_line(&gate.dag_id);
        gate.run_id = sanitize_line(&gate.run_id);
        gate.task_id = sanitize_line(&gate.task_id);
        gate.subject = sanitize_line(&gate.subject);
        gate.body = sanitize_block(&gate.body);
        gate.options = gate.options.iter().map(|o| sanitize_line(o)).collect();
    }
    for pr in &mut snapshot.prs {
        pr.title = sanitize_line(&pr.title);
        pr.url = sanitize_line(&pr.url);
        pr.head = sanitize_line(&pr.head);
        pr.state = sanitize_line(&pr.state);
        pr.checks = sanitize_line(&pr.checks);
        pr.labels = pr.labels.iter().map(|l| sanitize_line(l)).collect();
    }
    for sandbox in &mut snapshot.sandboxes {
        sandbox.name = sanitize_line(&sandbox.name);
        sandbox.status = sanitize_line(&sandbox.status);
        sandbox.created_by = sanitize_line(&sandbox.created_by);
    }
    for error in &mut snapshot.errors {
        error.source = sanitize_line(&error.source);
        error.message = sanitize_line(&error.message);
    }
    for health in snapshot.health.values_mut() {
        health.error = health.error.as_deref().map(sanitize_line);
    }
}

/// One source's freshness, as a glyph, a word and a feeling.
pub fn source_status(
    health: &SourceHealth,
    now: DateTime<Utc>,
    threshold: chrono::Duration,
) -> (String, Tone) {
    if health.error.is_some() {
        ("✗ failing".to_string(), Tone::Bad)
    } else if health.in_flight {
        ("↻ loading".to_string(), Tone::Active)
    } else if health.is_stale(now, threshold) {
        ("◔ stale".to_string(), Tone::Warn)
    } else if health.truncated {
        ("⋯ truncated".to_string(), Tone::Warn)
    } else {
        ("✓ fresh".to_string(), Tone::Good)
    }
}

/// Everything that can change the screen.
#[derive(Debug, Clone)]
pub enum Msg {
    /// A key was pressed.
    Key(KeyEvent),
    /// The terminal was resized.
    Resize(u16, u16),
    /// The clock moved. Carries `now` so the model never reads one itself.
    Tick(DateTime<Utc>),
    /// Ctrl-C, SIGINT or SIGTERM. Leaves; remote work keeps running.
    Interrupt,
    /// A collection pass finished.
    Snapshot(Box<Snapshot>),
    /// A gate's evidence arrived.
    Review(Box<GateReview>),
    /// A gate was answered.
    Answered {
        /// Which gate.
        id: GateId,
        /// What was chosen.
        decision: Decision,
        /// True when readiness was overridden.
        forced: bool,
    },
    /// A mutation succeeded. The string is what the log should say.
    Ok(String),
    /// A mutation was refused by policy. Not a crash, and not a reason to refresh.
    Refused {
        /// What was attempted.
        label: String,
        /// Why it was refused.
        message: String,
    },
    /// A mutation or a read failed.
    Failed {
        /// What was attempted.
        label: String,
        /// What went wrong.
        message: String,
    },
    /// Something worth one log line happened.
    Note(String),
    /// Log lines fetched for a task attempt.
    LogLines(Vec<String>),
    /// Leave.
    Quit,
}

/// The only function allowed to change the model, and the only one that decides what to do next.
///
/// It returns effects rather than performing them so that the whole interface can be driven from a
/// test with no runtime, no terminal and no network — which is what makes "the cursor survived a
/// refresh" a thing that can be asserted rather than hoped for.
pub fn update(model: &mut Model, msg: Msg) -> Vec<Effect> {
    match msg {
        Msg::Key(key) => match map_key(model.mode(), model.view, &key) {
            Some(action) => apply(model, action),
            None => Vec::new(),
        },
        Msg::Resize(w, h) => {
            model.size = (w, h);
            Vec::new()
        }
        Msg::Tick(now) => {
            model.now = now;
            let due = match model.last_refresh {
                Some(last) => now - last >= model.stale_after() / STALE_FACTOR as i32,
                None => true,
            };
            if due && model.in_flight == 0 {
                start_refresh(model)
            } else {
                Vec::new()
            }
        }
        Msg::Interrupt | Msg::Quit => {
            model.quit = true;
            vec![Effect::Quit]
        }
        Msg::Snapshot(snapshot) => {
            model.apply_snapshot(*snapshot);
            Vec::new()
        }
        Msg::Review(review) => {
            model.selection.gate = Some(review.id.clone());
            model.selection.job = Some(review.id.job.clone());
            model.review = Some(*review);
            model.back = if model.view == View::Review {
                model.back
            } else {
                model.view
            };
            model.view = View::Review;
            Vec::new()
        }
        Msg::Answered {
            id,
            decision,
            forced,
        } => {
            let forced = if forced { " (forced)" } else { "" };
            model.note(format!("ok {} {id}{forced}", decision.word()));
            model.review = None;
            model.view = model.back;
            start_refresh(model)
        }
        Msg::Ok(label) => {
            model.note(format!("ok {label}"));
            start_refresh(model)
        }
        // A refusal is a policy decision that has already been made: re-reading cannot change it,
        // and a refresh here would only hide the message under a repaint (§9.9, gotcha 20).
        Msg::Refused { label, message } => {
            model.note(format!("refused {label}: {message}"));
            Vec::new()
        }
        Msg::Failed { label, message } => {
            model.in_flight = model.in_flight.saturating_sub(1);
            model.note(format!("failed {label}: {message}"));
            Vec::new()
        }
        Msg::Note(line) => {
            model.note(line);
            Vec::new()
        }
        Msg::LogLines(lines) => {
            for line in lines {
                model.log.push(line);
            }
            model.log_open = true;
            Vec::new()
        }
    }
}

fn start_refresh(model: &mut Model) -> Vec<Effect> {
    model.in_flight = model.in_flight.saturating_add(1);
    for health in model.sources.values_mut() {
        health.in_flight = true;
    }
    vec![Effect::Refresh]
}

/// Apply one action in whichever mode the model is in.
fn apply(model: &mut Model, action: Action) -> Vec<Effect> {
    match model.mode() {
        Mode::Confirm => confirm_action(model, action),
        Mode::Prompt => prompt_action(model, action),
        Mode::Palette => palette_action(model, action),
        Mode::Help => {
            model.help_open = false;
            Vec::new()
        }
        Mode::Search => search_action(model, action),
        Mode::Normal => normal_action(model, action),
    }
}

fn confirm_action(model: &mut Model, action: Action) -> Vec<Effect> {
    match action {
        Action::Accept => {
            let Some(confirm) = model.confirm.take() else {
                return Vec::new();
            };
            model.note(format!("-> {}", confirm.label));
            model.in_flight = model.in_flight.saturating_add(1);
            vec![match confirm.pending {
                Pending::Answer {
                    id,
                    decision,
                    expect,
                } => Effect::Answer {
                    id,
                    decision,
                    expect,
                    label: confirm.label,
                },
                Pending::Stop(run) => Effect::Stop {
                    run,
                    label: confirm.label,
                },
                Pending::RemoveSandbox(name) => Effect::RemoveSandbox {
                    name,
                    label: confirm.label,
                },
                Pending::Trigger { dag_id, issues } => Effect::Trigger {
                    dag_id,
                    issues,
                    label: confirm.label,
                },
            }]
        }
        Action::Cancel => {
            if let Some(confirm) = model.confirm.take() {
                model.note(format!("cancelled: {}", confirm.label));
            }
            Vec::new()
        }
        _ => Vec::new(),
    }
}

fn prompt_action(model: &mut Model, action: Action) -> Vec<Effect> {
    let Some(prompt) = model.prompt.as_mut() else {
        return Vec::new();
    };
    match action {
        Action::Char(c) => {
            prompt.input.push(c);
            Vec::new()
        }
        Action::Backspace => {
            prompt.input.pop();
            Vec::new()
        }
        Action::Cancel => {
            model.prompt = None;
            model.note("trigger cancelled (no issues)");
            Vec::new()
        }
        Action::Accept => {
            let Some(prompt) = model.prompt.take() else {
                return Vec::new();
            };
            let PromptKind::TriggerIssues { dag_id } = prompt.kind;
            let issues = swf_domain::rollup::parse_issues(&prompt.input);
            if issues.is_empty() {
                model.note("trigger cancelled (no issues)");
                return Vec::new();
            }
            model.confirm = Some(Confirm {
                question: format!(
                    "submit {} to {dag_id} as {}?",
                    issues.join(", "),
                    model.env.actor
                ),
                label: format!("trigger {dag_id} {}", issues.join(", ")),
                pending: Pending::Trigger { dag_id, issues },
            });
            Vec::new()
        }
        _ => Vec::new(),
    }
}

fn palette_action(model: &mut Model, action: Action) -> Vec<Effect> {
    match action {
        Action::Char(c) => {
            model.palette.input.push(c);
            model.palette.index = 0;
            Vec::new()
        }
        Action::Backspace => {
            model.palette.input.pop();
            model.palette.index = 0;
            Vec::new()
        }
        Action::Down => {
            let len = model.palette.matches().len();
            if len > 0 {
                model.palette.index = (model.palette.index + 1).min(len - 1);
            }
            Vec::new()
        }
        Action::Up => {
            model.palette.index = model.palette.index.saturating_sub(1);
            Vec::new()
        }
        Action::Cancel => {
            model.palette = Palette::default();
            Vec::new()
        }
        Action::Accept => {
            let chosen = model.palette.selected().map(|c| c.action);
            model.palette = Palette::default();
            match chosen {
                Some(action) => normal_action(model, action),
                None => Vec::new(),
            }
        }
        _ => Vec::new(),
    }
}

fn search_action(model: &mut Model, action: Action) -> Vec<Effect> {
    match action {
        Action::Char(c) => {
            model.search.query.push(c);
            model.ensure_selection();
            Vec::new()
        }
        Action::Backspace => {
            model.search.query.pop();
            model.ensure_selection();
            Vec::new()
        }
        Action::Accept => {
            model.search.open = false;
            model.ensure_selection();
            Vec::new()
        }
        Action::Cancel => {
            model.search = Search::default();
            model.ensure_selection();
            Vec::new()
        }
        _ => Vec::new(),
    }
}

fn normal_action(model: &mut Model, action: Action) -> Vec<Effect> {
    match action {
        Action::Quit => {
            model.quit = true;
            vec![Effect::Quit]
        }
        Action::Refresh => {
            model.note("refresh requested");
            start_refresh(model)
        }
        Action::Help => {
            model.help_open = true;
            Vec::new()
        }
        Action::ToggleLog => {
            model.log_open = !model.log_open;
            Vec::new()
        }
        Action::Palette => {
            model.palette = Palette {
                open: true,
                ..Palette::default()
            };
            Vec::new()
        }
        Action::Search => {
            model.search.open = true;
            Vec::new()
        }
        Action::GoView(view) => go(model, view),
        Action::NextView => go(model, model.view.next()),
        Action::PrevView => go(model, model.view.prev()),
        Action::Up => model.move_by(-1),
        Action::Down => model.move_by(1),
        Action::PageUp => model.move_by(-10),
        Action::PageDown => model.move_by(10),
        Action::Top => model.move_to_edge(false),
        Action::Bottom => model.move_to_edge(true),
        Action::Accept => enter(model),
        Action::Cancel => {
            if !model.search.query.is_empty() {
                model.search = Search::default();
                model.ensure_selection();
                return Vec::new();
            }
            go(model, model.back)
        }
        Action::Approve => answer(model, Decision::Approve),
        Action::Reject => answer(model, Decision::Reject),
        Action::Trigger => trigger(model),
        Action::Stop => stop(model),
        Action::Open => open(model),
        Action::Verify => verify(model),
        Action::Remove => remove_sandbox(model),
        Action::Logs => logs(model),
        Action::Char(_) | Action::Backspace => Vec::new(),
    }
}

fn go(model: &mut Model, view: View) -> Vec<Effect> {
    if view == model.view {
        return Vec::new();
    }
    if !matches!(view, View::JobDetail | View::Review) {
        model.back = view;
    }
    model.view = view;
    model.search = Search::default();
    let mut effects = Vec::new();
    if view == View::Review {
        if let Some(id) = model.selection.gate.clone() {
            effects.push(Effect::Cancel);
            effects.push(Effect::Review(id));
        }
    }
    model.ensure_selection();
    effects
}

fn enter(model: &mut Model) -> Vec<Effect> {
    let Some(row) = model.current_row() else {
        model.note("nothing selected");
        return Vec::new();
    };
    match row.kind {
        RowKind::Gate => match row.key.parse::<GateId>() {
            Ok(id) => {
                model.selection.gate = Some(id.clone());
                model.back = model.view;
                model.view = View::Review;
                vec![Effect::Cancel, Effect::Review(id)]
            }
            Err(err) => {
                model.note(format!("failed review {}: {err}", row.key));
                Vec::new()
            }
        },
        RowKind::Job | RowKind::Failure => {
            model.back = model.view;
            model.view = View::JobDetail;
            model.selection.task = None;
            model.ensure_selection();
            Vec::new()
        }
        _ => {
            model.note(format!("no detail view for {}", row.key));
            Vec::new()
        }
    }
}

/// Build the confirmation for answering a gate.
///
/// The question names the gate, the job and the actor because those three are what an operator
/// gets wrong when several runs are on screen; the revision goes with the answer so the write
/// re-validates against exactly the evidence that was read (rule 5).
fn answer(model: &mut Model, decision: Decision) -> Vec<Effect> {
    let (id, revision, ready) = match (&model.review, model.current_row()) {
        (Some(review), _) if model.view == View::Review => (
            review.id.clone(),
            Some(review.revision.clone()),
            review.ready,
        ),
        (_, Some(row)) if row.kind == RowKind::Gate => match row.key.parse::<GateId>() {
            Ok(id) => {
                let gate = model.snapshot.gate(&id);
                let revision = gate.map(|g| g.current_revision());
                let ready = gate.map(|g| g.ready).unwrap_or(false);
                (id, revision, ready)
            }
            Err(err) => {
                model.note(format!("failed {}: {err}", decision.word()));
                return Vec::new();
            }
        },
        _ => {
            model.note("no gate selected");
            return Vec::new();
        }
    };
    if !ready {
        model.note(format!(
            "{} {id} is not answerable yet: its task is not parked",
            id.short_name()
        ));
    }
    model.confirm = Some(Confirm {
        question: format!(
            "{} {} of {} as {}? (evidence {})",
            decision.word(),
            id.short_name(),
            id.job,
            model.env.actor,
            revision.clone().unwrap_or_else(|| "unknown".into()),
        ),
        label: format!("{} {id}", decision.word()),
        pending: Pending::Answer {
            id,
            decision,
            expect: revision,
        },
    });
    Vec::new()
}

fn trigger(model: &mut Model) -> Vec<Effect> {
    let dag_id = model
        .selection
        .job
        .as_ref()
        .map(|job| job.dag_id.clone())
        .filter(|dag| model.env.dag_ids.is_empty() || model.env.dag_ids.contains(dag))
        .unwrap_or_else(|| model.env.default_dag());
    model.prompt = Some(Prompt {
        title: format!("trigger {dag_id}: issue ids or paths, comma separated"),
        placeholder: "42, 43".into(),
        input: String::new(),
        kind: PromptKind::TriggerIssues { dag_id },
    });
    Vec::new()
}

fn stop(model: &mut Model) -> Vec<Effect> {
    let Some(job) = model.selection.job.clone() else {
        model.note("no run selected");
        return Vec::new();
    };
    let run = job.run();
    let state = model
        .snapshot
        .run(&run)
        .map(|r| r.state.clone())
        .unwrap_or_else(|| "unknown".into());
    model.confirm = Some(Confirm {
        // Non-negotiable 9: this marks the Airflow run failed. It does not kill a process, it does
        // not clean up a sandbox, and a confirmation that implies otherwise is a lie.
        question: format!(
            "mark the run {run} failed — the whole run, every job (state {state})? \
             Work already running is not killed."
        ),
        label: format!("stop {run}"),
        pending: Pending::Stop(run),
    });
    Vec::new()
}

fn open(model: &mut Model) -> Vec<Effect> {
    if model.view == View::Deliveries {
        if let Some(delivery) = model
            .deliveries()
            .into_iter()
            .find(|d| Some(d.id.as_str()) == model.selection.delivery.as_deref())
        {
            model.note(format!("open {}", delivery.url));
            return vec![Effect::Open(delivery.url)];
        }
    }
    match model.selection.job.as_ref() {
        Some(job) => {
            let run = job.run();
            model.note(format!("open {run}"));
            vec![Effect::OpenRun(run)]
        }
        None => {
            model.note("nothing to open");
            Vec::new()
        }
    }
}

fn verify(model: &mut Model) -> Vec<Effect> {
    let Some(id) = model.selection.delivery.clone() else {
        model.note("no delivery selected");
        return Vec::new();
    };
    model.note(format!("-> verify {id}"));
    model.in_flight = model.in_flight.saturating_add(1);
    vec![Effect::Verify(id)]
}

fn remove_sandbox(model: &mut Model) -> Vec<Effect> {
    let name = model
        .current_row()
        .filter(|row| row.kind == RowKind::Sandbox)
        .map(|row| row.cells.get(1).cloned().unwrap_or_default());
    let Some(name) = name.filter(|n| !n.is_empty()) else {
        model.note("no sandbox selected");
        return Vec::new();
    };
    let created_by = model
        .snapshot
        .sandboxes
        .iter()
        .find(|s| s.name == name)
        .map(|s| s.created_by.clone())
        .unwrap_or_else(|| "?".into());
    model.confirm = Some(Confirm {
        question: format!("remove sandbox {name} (created by {created_by})?"),
        label: format!("remove sandbox {name}"),
        pending: Pending::RemoveSandbox(name),
    });
    Vec::new()
}

fn logs(model: &mut Model) -> Vec<Effect> {
    let Some(job) = model.selection.job.clone() else {
        model.note("no job selected");
        return Vec::new();
    };
    let task = model
        .current_row()
        .filter(|row| row.kind == RowKind::Task)
        .and_then(|row| row.cells.first().cloned());
    model.note(format!("-> logs {job}"));
    vec![Effect::Logs { job, task }]
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use crossterm::event::{KeyCode, KeyModifiers};
    use swf_domain::model::{JobRow, Run, TaskState};

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + seconds, 0)
            .single()
            .unwrap_or_else(Utc::now)
    }

    fn model() -> Model {
        let env = Env {
            context: "local".into(),
            airflow_url: "http://localhost:8080".into(),
            repo: "acme/widgets".into(),
            owner: "me@example.com".into(),
            actor: "admin".into(),
            dag_ids: vec!["factory".into()],
        };
        Model::new(env, DEFAULT_REFRESH, at(0))
    }

    fn job(dag: &str, run: &str, index: i32, state: &str) -> JobRow {
        let mut row = JobRow::new(dag, run, index);
        row.state = state.into();
        row.issue = "42".into();
        row.tasks = vec![TaskState::new("spec", index, Some(state.into()))];
        row
    }

    fn snapshot(order: &[(&str, &str, i32)]) -> Snapshot {
        let mut snap = Snapshot::new(at(0));
        for (dag, run_id, index) in order {
            let mut run = Run::new(*dag, *run_id, "running");
            run.jobs = vec![job(dag, run_id, *index, "running")];
            snap.runs.push(run);
        }
        snap
    }

    fn key(code: KeyCode) -> Msg {
        Msg::Key(KeyEvent::new(code, KeyModifiers::NONE))
    }

    #[test]
    fn the_cursor_stays_on_the_job_it_was_on_when_a_refresh_reorders_the_table() {
        let mut model = model();
        model.view = View::Jobs;
        update(
            &mut model,
            Msg::Snapshot(Box::new(snapshot(&[
                ("factory", "a", 0),
                ("factory", "b", 0),
                ("factory", "c", 0),
            ]))),
        );
        update(&mut model, key(KeyCode::Down));
        update(&mut model, key(KeyCode::Down));
        let chosen = model.current_key();
        assert_eq!(chosen.as_deref(), Some("factory/c#0"));
        assert_eq!(model.selected_index(), Some(2));

        // The same three jobs come back in a different order, which is what Airflow does whenever
        // a run finishes. A cursor kept by index would now be pointing at `factory/a`.
        update(
            &mut model,
            Msg::Snapshot(Box::new(snapshot(&[
                ("factory", "c", 0),
                ("factory", "a", 0),
                ("factory", "b", 0),
            ]))),
        );
        assert_eq!(model.current_key(), chosen);
        assert_eq!(model.selected_index(), Some(0));
    }

    #[test]
    fn a_selection_that_vanished_lands_on_a_row_and_not_on_a_ghost() {
        let mut model = model();
        model.view = View::Jobs;
        update(
            &mut model,
            Msg::Snapshot(Box::new(snapshot(&[
                ("factory", "a", 0),
                ("factory", "b", 0),
            ]))),
        );
        update(&mut model, key(KeyCode::Down));
        assert_eq!(model.current_key().as_deref(), Some("factory/b#0"));
        update(
            &mut model,
            Msg::Snapshot(Box::new(snapshot(&[("factory", "a", 0)]))),
        );
        assert_eq!(model.selected_index(), Some(0));
        assert_eq!(model.current_key().as_deref(), Some("factory/a#0"));
    }

    #[test]
    fn the_log_ring_drops_the_oldest_and_says_how_many() {
        let mut ring = LogRing::new(3);
        for i in 0..10 {
            ring.push(format!("line {i}"));
        }
        assert_eq!(ring.len(), 3);
        assert_eq!(ring.dropped(), 7);
        assert_eq!(
            ring.lines().collect::<Vec<_>>(),
            vec!["line 7", "line 8", "line 9"]
        );
    }

    #[test]
    fn a_log_line_from_a_service_cannot_reposition_the_cursor() {
        let mut ring = LogRing::new(4);
        ring.push("gh says \u{1b}[2Jnothing\u{7}");
        assert_eq!(ring.last(), Some("gh says nothing"));
    }

    #[test]
    fn moving_between_runs_cancels_whatever_was_in_flight_for_the_old_one() {
        let mut model = model();
        model.view = View::Jobs;
        update(
            &mut model,
            Msg::Snapshot(Box::new(snapshot(&[
                ("factory", "a", 0),
                ("factory", "b", 0),
            ]))),
        );
        let effects = update(&mut model, key(KeyCode::Down));
        assert!(matches!(effects.as_slice(), [Effect::Cancel]));
        // Moving inside the same run has nothing to abandon.
        let mut same = model;
        same.view = View::JobDetail;
        assert!(same.move_by(1).is_empty());
    }

    #[test]
    fn answering_a_gate_asks_first_and_names_everything_that_could_be_confused() {
        let mut model = model();
        let mut snap = snapshot(&[("factory", "r1", 1)]);
        let mut gate = swf_domain::model::Gate::new(
            "factory",
            "r1",
            "job.approve_plan",
            1,
            "ship it",
            "the plan",
            Some(at(0)),
            vec!["approve".into(), "reject".into()],
        );
        gate.ready = true;
        snap.gates.push(gate);
        update(&mut model, Msg::Snapshot(Box::new(snap)));
        model.view = View::Attention;
        model.ensure_selection();
        let effects = update(&mut model, key(KeyCode::Char('a')));
        assert!(effects.is_empty(), "an answer must be confirmed first");
        let confirm = model.confirm.clone().expect("a confirmation");
        assert!(confirm.question.contains("approve_plan"));
        assert!(confirm.question.contains("factory/r1#1"));
        assert!(confirm.question.contains("admin"));
        assert!(matches!(
            confirm.pending,
            Pending::Answer {
                decision: Decision::Approve,
                expect: Some(_),
                ..
            }
        ));

        // `y` produces exactly one write, carrying the revision that was on screen.
        let effects = update(&mut model, key(KeyCode::Char('y')));
        assert!(matches!(effects.as_slice(), [Effect::Answer { .. }]));
    }

    #[test]
    fn a_refusal_does_not_repaint_over_the_reason() {
        let mut model = model();
        let effects = update(
            &mut model,
            Msg::Refused {
                label: "approve factory/r1#1:job.approve_plan".into(),
                message: "not yours".into(),
            },
        );
        assert!(effects.is_empty(), "a policy decision is not retried");
        assert!(model.status.contains("refused"));
    }

    #[test]
    fn search_filters_the_table_and_keeps_the_cursor_on_something_visible() {
        let mut model = model();
        model.view = View::Jobs;
        update(
            &mut model,
            Msg::Snapshot(Box::new(snapshot(&[
                ("factory", "alpha", 0),
                ("hotfix", "beta", 0),
            ]))),
        );
        update(&mut model, key(KeyCode::Char('/')));
        for c in "hotfix".chars() {
            update(&mut model, key(KeyCode::Char(c)));
        }
        assert_eq!(model.rows().len(), 1);
        assert_eq!(model.current_key().as_deref(), Some("hotfix/beta#0"));
        update(&mut model, key(KeyCode::Esc));
        assert_eq!(model.rows().len(), 2);
    }

    #[test]
    fn the_palette_runs_the_same_actions_the_keys_do() {
        let mut model = model();
        update(&mut model, key(KeyCode::Char(':')));
        assert_eq!(model.mode(), Mode::Palette);
        for c in "deliv".chars() {
            update(&mut model, key(KeyCode::Char(c)));
        }
        assert_eq!(model.palette.matches().len(), 1);
        update(&mut model, key(KeyCode::Enter));
        assert_eq!(model.view, View::Deliveries);
        assert!(!model.palette.open);
    }

    #[test]
    fn quitting_asks_for_nothing_and_leaves_the_factory_running() {
        let mut model = model();
        let effects = update(&mut model, key(KeyCode::Char('q')));
        assert!(model.quit);
        assert!(matches!(effects.as_slice(), [Effect::Quit]));
    }

    #[test]
    fn stopping_a_run_says_what_it_actually_does() {
        let mut model = model();
        model.view = View::Jobs;
        update(
            &mut model,
            Msg::Snapshot(Box::new(snapshot(&[("factory", "r1", 0)]))),
        );
        update(&mut model, key(KeyCode::Char('s')));
        let confirm = model.confirm.clone().expect("a confirmation");
        assert!(confirm.question.contains("mark the run"));
        assert!(confirm.question.contains("not killed"));
        assert!(!confirm.question.to_lowercase().contains("clean"));
    }

    #[test]
    fn untrusted_text_is_stripped_before_it_can_reach_a_widget() {
        let mut model = model();
        let mut snap = snapshot(&[("factory", "r1", 0)]);
        snap.prs.push(swf_domain::model::PullRequest {
            number: 7,
            title: "fix \u{1b}]0;pwned\u{7}things".into(),
            url: "https://example.test/7".into(),
            labels: vec!["factory".into()],
            state: "OPEN".into(),
            checks: "1 pass / 0 fail / 0 pending".into(),
            head: "factory/42-run".into(),
        });
        update(&mut model, Msg::Snapshot(Box::new(snap)));
        model.view = View::Deliveries;
        let rows = model.rows();
        assert_eq!(rows[0].cells[1], "fix things");
    }

    #[test]
    fn the_views_are_numbered_one_to_seven_and_wrap_both_ways() {
        assert_eq!(View::Attention.number(), 1);
        assert_eq!(View::History.number(), 7);
        assert_eq!(View::from_number(4), Some(View::Review));
        assert_eq!(View::from_number(8), None);
        assert_eq!(View::History.next(), View::Attention);
        assert_eq!(View::Attention.prev(), View::History);
    }
}
