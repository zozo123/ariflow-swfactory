//! The argument surface, and nothing else.
//!
//! Every verb, flag and help string a script may depend on lives here so the whole public
//! interface can be read in one file — the command names, the `--json` keys and the exit codes are
//! semver surface (`00-architecture.md` §D-E), and surface that is scattered is surface that
//! drifts. There is deliberately no validation in this module beyond what clap itself can do: the
//! rules belong to `swf-app`, where the TUI takes the same path, and a check written here would be
//! a rule one of the two interfaces could skip.

use clap::{Args, Parser, Subcommand};

/// The exit-code table, printed under `swf --help` because scripts branch on it.
pub const EXIT_CODES: &str = "\
Exit codes:
  0  success
  1  operational failure (a check is red, a gate answer was refused, verification failed)
  2  usage error (a bad flag, an unparseable id, a mutation without --yes on a non-TTY)
  3  not found (no such context, run, job, cell, gate, delivery or sandbox)
  4  authentication or authorisation failure
  5  service unreachable (DNS, connect, TLS, timeout, or a missing gh/islo)
  6  conflict (the gate was already answered, or the evidence moved under you)

With --json a failure also prints one document to stdout:
  {\"error\": {\"kind\": …, \"message\": …, \"exit_code\": …, \"hint\": …}}
Diagnostics always go to stderr, so `swf … --json | jq` is safe in a pipeline.";

/// `swf` — one command over the whole factory.
#[derive(Debug, Parser)]
#[command(
    name = "swf",
    version,
    propagate_version = true,
    about = "Operate the software factory: connect, submit, inspect, approve, verify",
    after_help = EXIT_CODES,
    after_long_help = EXIT_CODES
)]
pub struct Cli {
    /// The environment to act against. Overrides $SWF_CONTEXT and the config's `default`.
    #[arg(long, global = true, value_name = "NAME")]
    pub context: Option<String>,

    /// Print exactly one JSON document on stdout. Every diagnostic still goes to stderr.
    #[arg(long, global = true)]
    pub json: bool,

    /// Never emit colour, even on a terminal. Colour is off automatically when stdout is a pipe.
    #[arg(long = "no-color", global = true)]
    pub no_color: bool,

    /// HTTP deadline in seconds. It never bounds a subprocess: `gh` on a slow API is not a hang.
    #[arg(long, global = true, value_name = "S")]
    pub timeout: Option<f64>,

    /// Say more on stderr. Repeat for more.
    #[arg(short = 'v', long, global = true, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Answer yes to the confirmation. Required for a mutation when stdin is not a terminal.
    #[arg(short = 'y', long, global = true)]
    pub yes: bool,

    /// What to do.
    #[command(subcommand)]
    pub command: Command,
}

/// Every verb the binary answers.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// The factory environments this machine knows about.
    #[command(subcommand)]
    Context(ContextCmd),

    /// Is this machine able to drive a factory? One line per check, a `fix:` for every failure.
    Doctor,

    /// Send governed work to Airflow.
    Submit(SubmitArgs),

    /// Approvals waiting, failures, blocked deliveries and orphan sandboxes.
    Attention,

    /// DAG runs.
    #[command(subcommand)]
    Runs(RunsCmd),

    /// Mapped jobs — the unit a gate, a sandbox and a delivery all belong to.
    #[command(subcommand)]
    Jobs(JobsCmd),

    /// Durable issue×target Factory Cells and their ownership/evidence history.
    #[command(subcommand)]
    Cells(CellsCmd),

    /// One task attempt's log.
    Logs(LogsArgs),

    /// Approval gates.
    #[command(subcommand)]
    Gates(GatesCmd),

    /// What the factory published, and whether it is true.
    #[command(subcommand)]
    Deliveries(DeliveriesCmd),

    /// Agent sandboxes.
    #[command(subcommand)]
    Sandboxes(SandboxesCmd),

    /// The committed metrics history.
    Metrics(MetricsArgs),

    /// One pass over every source, in the same document `swfactory herd --once --json` prints.
    Snapshot,

    /// The local development stack.
    #[command(subcommand)]
    Stack(StackCmd),

    /// The interactive factory.
    Tui,

    /// Print a shell completion script.
    Completions(CompletionsArgs),

    /// Print the version.
    Version,
}

/// `swf context …`
//
// The variants differ in size because `add` carries a dozen flags and `list` carries none. That
// is the command line's shape, not a data-structure choice: boxing it to even the variants out
// would buy nothing — exactly one of these is built, once, per process.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Subcommand)]
pub enum ContextCmd {
    /// Every configured environment, with the active one marked.
    List,

    /// Make one environment the default for later commands.
    Use {
        /// The name to make default.
        name: String,
    },

    /// Record an environment. The config file never holds a secret — only the name of a variable.
    Add(ContextAddArgs),

    /// Show one environment, with every credential redacted to the variable name.
    Show {
        /// The environment to show. Defaults to the active one.
        name: Option<String>,
    },

    /// Forget one environment.
    Remove {
        /// The name to forget.
        name: String,
    },
}

/// `swf context add`
#[derive(Debug, Args)]
pub struct ContextAddArgs {
    /// What to call it.
    pub name: String,

    /// Public Airflow UI URL; also the API URL in explicit --direct mode.
    #[arg(long, value_name = "URL")]
    pub airflow_url: String,

    /// Python factory backend URL. Credentials use SWF_BACKEND_TOKEN.
    #[arg(long, default_value = "http://localhost:8082", value_name = "URL")]
    pub backend_url: String,

    /// Explicit compatibility mode: connect directly to Airflow, gh and islo.
    #[arg(long)]
    pub direct: bool,

    /// `owner/name` of the repository deliveries land in.
    #[arg(long, value_name = "OWNER/NAME")]
    pub repo: Option<String>,

    /// The sandbox owner islo records. Without it, sandbox removal is refused outright.
    #[arg(long, value_name = "WHO")]
    pub owner: Option<String>,

    /// Where the committed metrics.json files are.
    #[arg(long, value_name = "PATH")]
    pub metrics_root: Option<String>,

    /// A DAG to read. Repeatable. Absent means "ask Airflow for everything with the tag".
    #[arg(long = "dag", value_name = "ID")]
    pub dags: Vec<String>,

    /// The tag that identifies factory DAGs on this server.
    #[arg(long, value_name = "TAG")]
    pub dag_tag: Option<String>,

    /// Basic auth username. Needs --password-env.
    #[arg(long, value_name = "NAME")]
    pub user: Option<String>,

    /// The NAME of the environment variable holding the password. Never the password.
    #[arg(long, value_name = "VAR")]
    pub password_env: Option<String>,

    /// The NAME of the environment variable holding a bearer token. Never the token.
    #[arg(long, value_name = "VAR")]
    pub token_env: Option<String>,

    /// Make it the default straight away.
    #[arg(long = "use")]
    pub use_it: bool,

    /// Replace an environment of the same name.
    #[arg(long)]
    pub force: bool,
}

/// `swf submit`
#[derive(Debug, Args)]
pub struct SubmitArgs {
    /// An issue for the factory to answer. Repeatable; one run answers each issue once.
    #[arg(long = "issue", value_name = "REF", required = true)]
    pub issues: Vec<String>,

    /// The blueprint, by name or by path to its .toml.
    #[arg(long, value_name = "NAME", default_value = "factory")]
    pub blueprint: String,

    /// A target repository to override the blueprint's. Repeatable.
    #[arg(long = "target", value_name = "OWNER/NAME")]
    pub targets: Vec<String>,

    /// Poll until the run reaches a final state before answering.
    #[arg(long)]
    pub wait: bool,
}

/// `swf runs …`
#[derive(Debug, Subcommand)]
pub enum RunsCmd {
    /// The newest runs, newest first.
    List {
        /// One DAG. Absent means every DAG this context reads.
        #[arg(long, value_name = "ID")]
        dag: Option<String>,

        /// Only runs in this state: queued, running, success, failed.
        #[arg(long, value_name = "STATE")]
        state: Option<String>,

        /// How many runs to read per DAG. --state narrows what was read.
        #[arg(long, default_value_t = 20, value_name = "N")]
        limit: usize,
    },

    /// One run: its state, its issues and its jobs.
    Inspect {
        /// `dag/run`.
        run: String,
    },

    /// MARK THE AIRFLOW RUN FAILED. It does not stop processes or clean up sandboxes.
    ///
    /// This is all Airflow offers and the name says it: tasks already running keep running, the
    /// agent keeps working, and any sandbox the run created stays up. Remove sandboxes with
    /// `swf sandboxes rm`.
    Stop {
        /// `dag/run`.
        run: String,
    },

    /// Unpause a DAG. A freshly parsed blueprint is paused, so its runs sit queued forever.
    Unpause {
        /// The DAG id.
        dag: String,
    },
}

/// `swf jobs …`
#[derive(Debug, Subcommand)]
pub enum JobsCmd {
    /// Every mapped job this context can see.
    List(JobListArgs),

    /// One job: identity, progress, tasks and the gates it is waiting on.
    Inspect {
        /// `dag/run#index`.
        job: String,
    },
}

/// `swf cells …`
#[derive(Debug, Subcommand)]
pub enum CellsCmd {
    /// Newest durable Factory Cells, newest mutation first.
    List {
        /// Maximum cells to return.
        #[arg(long, default_value_t = 100, value_name = "N")]
        limit: usize,
    },

    /// One cell's durable projection: epoch, Airflow identity, compute and cleanup state.
    Inspect {
        /// `cell_` followed by 24 hexadecimal characters.
        cell_id: String,
    },

    /// One cell's append-only authority/evidence history.
    History {
        /// `cell_` followed by 24 hexadecimal characters.
        cell_id: String,
    },
}

/// `swf logs`
#[derive(Debug, Args)]
pub struct LogsArgs {
    /// `dag/run#index`.
    pub job: String,

    /// The task to read. Absent picks the failure, else what is running, else the last one.
    #[arg(long, value_name = "TASK")]
    pub task: Option<String>,

    /// Which try. Airflow numbers them from 1.
    #[arg(long, default_value_t = 1, value_name = "N")]
    pub attempt: u32,

    /// Keep polling until the attempt's log is complete. Airflow has no streaming endpoint.
    #[arg(long)]
    pub follow: bool,
}

/// `swf gates …`
#[derive(Debug, Subcommand)]
pub enum GatesCmd {
    /// Every gate waiting for an answer, and whether it can be answered yet.
    List(GateListArgs),

    /// The evidence for one gate, and the revision to hand back to `approve`.
    Review {
        /// `dag/run#index:gate`.
        gate: String,
    },

    /// Approve one gate, or every gate a filter selects with --all.
    ///
    /// A batch answers only gates that are ready: one that has not parked yet is reported as
    /// skipped, and there is no --force for a set. Run it with --dry-run first — that prints the
    /// whole selection and writes nothing — then re-run the identical line with --dry-run removed.
    Approve(AnswerArgs),

    /// Reject one gate, or every gate a filter selects with --all.
    ///
    /// The same rules as approve, and the same advice: --dry-run first, writes nothing, then the
    /// identical line without it.
    Reject(AnswerArgs),
}

/// The filters that narrow a set of gates, shared by the listing and the bulk answer.
///
/// One struct for both so a listing an operator narrowed **is** the batch they are about to run.
/// Two copies of these flags would eventually disagree, and the day they did, the disagreement
/// would be discovered by a batch answering a gate nobody had read.
#[derive(Debug, Args)]
pub struct GateFilterArgs {
    /// Only gates of this DAG.
    #[arg(long, value_name = "ID")]
    pub dag: Option<String>,

    /// Only gates of this blueprint — the same set as --dag, named the way you submitted it.
    #[arg(long, value_name = "NAME")]
    pub blueprint: Option<String>,

    /// Only gates whose job answers this issue. Costs an extra read per matched run.
    #[arg(long, value_name = "REF")]
    pub issue: Option<String>,

    /// Only this gate: `intent`, `plan`, or a full `job.approve_*` task id.
    #[arg(long = "gate", value_name = "NAME")]
    pub gate_name: Option<String>,

    /// Only gates that can be answered now, dropping the ones still arming.
    #[arg(long)]
    pub ready: bool,

    /// At most this many gates. A set the limit cut says so, and never silently.
    #[arg(long, value_name = "N")]
    pub limit: Option<usize>,
}

/// `swf gates list`
#[derive(Debug, Args)]
pub struct GateListArgs {
    /// Which gates to show.
    #[command(flatten)]
    pub filter: GateFilterArgs,
}

/// `swf jobs list`
#[derive(Debug, Args)]
pub struct JobListArgs {
    /// Only jobs that need a person: failed, or waiting on a gate.
    #[arg(long)]
    pub attention: bool,

    /// Only jobs of this DAG. The other DAGs are never read.
    #[arg(long, value_name = "ID")]
    pub dag: Option<String>,

    /// Only jobs in this state: queued, running, success, failed, skipped.
    #[arg(long, value_name = "STATE")]
    pub state: Option<String>,

    /// Only jobs answering this issue.
    #[arg(long, value_name = "REF")]
    pub issue: Option<String>,

    /// At most this many jobs. A listing the limit cut says so.
    #[arg(long, value_name = "N")]
    pub limit: Option<usize>,
}

/// `swf gates approve|reject`
///
/// Either one gate by identity, or `--all` over a filtered set. `--dry-run` prints the whole set
/// and writes nothing; run it first, read what it selected, then re-run the identical line without
/// `--dry-run`.
#[derive(Debug, Args)]
pub struct AnswerArgs {
    /// `dag/run#index:gate`. Leave it out and pass --all to answer a whole set.
    #[arg(value_name = "GATE")]
    pub gate: Option<String>,

    /// The evidence revision `swf gates review` printed. A mismatch is a conflict, not a warning.
    #[arg(long, value_name = "REV")]
    pub expect: Option<String>,

    /// Answer a gate whose task has not parked yet. Doing so can make the scheduler fail it.
    /// One gate only: it cannot be combined with --all.
    #[arg(long)]
    pub force: bool,

    /// Answer every gate the filters select. Only gates that are ready are answered; the rest are
    /// reported as skipped, and there is no --force for a set.
    #[arg(long)]
    pub all: bool,

    /// Print exactly what --all would answer and write nothing. Run this first.
    #[arg(long = "dry-run")]
    pub dry_run: bool,

    /// Which gates --all means.
    #[command(flatten)]
    pub filter: GateFilterArgs,
}

/// `swf deliveries …`
#[derive(Debug, Subcommand)]
pub enum DeliveriesCmd {
    /// What the factory published.
    List,

    /// Check a delivery, keeping "the workflow said so", "a branch exists" and "we re-derived it"
    /// apart.
    Verify(VerifyArgs),
}

/// `swf deliveries verify`
#[derive(Debug, Args)]
pub struct VerifyArgs {
    /// A branch, a PR number, a URL or a local checkout.
    #[arg(value_name = "DELIVERY")]
    pub delivery: Option<String>,

    /// Verify every delivery this context can list. Answers a JSON array.
    #[arg(long, conflicts_with = "delivery")]
    pub all: bool,

    /// Clone the published branch and re-run its own tests. Nothing above `published` is
    /// attempted without it.
    #[arg(long = "clone")]
    pub clone_it: bool,

    /// The repository, when the context does not name one.
    #[arg(long, value_name = "OWNER/NAME")]
    pub repo: Option<String>,

    /// The branch the PR must merge into.
    #[arg(long, value_name = "REF")]
    pub base_branch: Option<String>,

    /// The directory inside the repo to test, when it is not to be discovered.
    #[arg(long, value_name = "DIR")]
    pub target_dir: Option<String>,

    /// Keep the checkout afterwards, for someone who wants to look at it.
    #[arg(long)]
    pub keep: bool,

    /// Where the published branch lives, when it is not a GitHub repository.
    ///
    /// `scm = "local"` publishes to a bare repository in the run directory. That branch is no less
    /// delivered for never reaching a forge, so it must be verifiable the same way.
    #[arg(long = "from", value_name = "REMOTE")]
    pub origin: Option<String>,

    /// The published branch, when there is no pull request to read it from.
    #[arg(long, value_name = "REF")]
    pub branch: Option<String>,
}

/// `swf sandboxes …`
#[derive(Debug, Subcommand)]
pub enum SandboxesCmd {
    /// The sandboxes this context's owner created.
    List,

    /// One sandbox: ownership, status and age.
    Inspect {
        /// The sandbox name.
        name: String,
    },

    /// Remove one sandbox. The provider's listing is re-read first to confirm it may be removed.
    Rm {
        /// The sandbox name.
        name: String,
    },
}

/// `swf metrics`
#[derive(Debug, Args)]
pub struct MetricsArgs {
    /// Read the committed history from here instead of the context's metrics_root.
    #[arg(long, value_name = "PATH")]
    pub root: Option<String>,
}

/// `swf stack …`
#[derive(Debug, Subcommand)]
pub enum StackCmd {
    /// Bring the local stack up and wait for it to answer.
    Up {
        /// Build the sandbox image first.
        #[arg(long)]
        build: bool,
    },

    /// Take the local stack down.
    Down {
        /// Also drop the volumes: the metadata DB, the venv and the generated password.
        #[arg(long)]
        volumes: bool,
    },

    /// What is actually running.
    Status,
}

/// `swf completions`
#[derive(Debug, Args)]
pub struct CompletionsArgs {
    /// The shell to generate for.
    #[arg(value_enum)]
    pub shell: clap_complete::Shell,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn the_argument_tree_is_internally_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn stop_never_claims_to_have_stopped_anything() {
        let mut cmd = Cli::command();
        let help = cmd.render_long_help().to_string();
        assert!(help.contains("swf"), "{help}");
        let stop = Cli::command()
            .find_subcommand("runs")
            .and_then(|runs| runs.find_subcommand("stop").cloned())
            .map(|mut stop| stop.render_long_help().to_string())
            .unwrap_or_default();
        assert!(stop.to_lowercase().contains("mark"), "{stop}");
        assert!(stop.contains("does not stop processes"), "{stop}");
        assert!(stop.contains("sandbox"), "{stop}");
    }

    #[test]
    fn global_flags_are_accepted_after_the_subcommand() {
        let cli =
            Cli::try_parse_from(["swf", "gates", "approve", "a/b#0:intent", "--yes", "--json"])
                .expect("parse");
        assert!(cli.yes);
        assert!(cli.json);
    }

    #[test]
    fn submit_requires_an_issue() {
        assert!(Cli::try_parse_from(["swf", "submit"]).is_err());
        assert!(Cli::try_parse_from(["swf", "submit", "--issue", "42"]).is_ok());
    }

    #[test]
    fn cells_have_stable_read_only_shapes() {
        let list = Cli::try_parse_from(["swf", "cells", "list", "--limit", "25"])
            .expect("cells list parses");
        let Command::Cells(CellsCmd::List { limit }) = list.command else {
            panic!("expected cells list");
        };
        assert_eq!(limit, 25);

        let inspect =
            Cli::try_parse_from(["swf", "cells", "inspect", "cell_0123456789abcdef01234567"])
                .expect("cells inspect parses");
        assert!(matches!(
            inspect.command,
            Command::Cells(CellsCmd::Inspect { .. })
        ));
    }

    #[test]
    fn a_gate_answer_takes_either_an_identity_or_a_whole_filtered_set() {
        let one = Cli::try_parse_from(["swf", "gates", "approve", "a/b#0:plan"]).expect("parse");
        let Command::Gates(GatesCmd::Approve(args)) = one.command else {
            panic!("expected gates approve");
        };
        assert_eq!(args.gate.as_deref(), Some("a/b#0:plan"));
        assert!(!args.all);

        let many = Cli::try_parse_from([
            "swf",
            "gates",
            "reject",
            "--all",
            "--dag",
            "factory",
            "--blueprint",
            "factory",
            "--issue",
            "42",
            "--gate",
            "plan",
            "--limit",
            "20",
            "--dry-run",
        ])
        .expect("parse");
        let Command::Gates(GatesCmd::Reject(args)) = many.command else {
            panic!("expected gates reject");
        };
        assert!(args.all && args.dry_run);
        assert!(args.gate.is_none());
        assert_eq!(args.filter.dag.as_deref(), Some("factory"));
        assert_eq!(args.filter.blueprint.as_deref(), Some("factory"));
        assert_eq!(args.filter.issue.as_deref(), Some("42"));
        assert_eq!(args.filter.gate_name.as_deref(), Some("plan"));
        assert_eq!(args.filter.limit, Some(20));
    }

    #[test]
    fn the_listings_take_the_filters_the_batch_takes() {
        let gates = Cli::try_parse_from([
            "swf", "gates", "list", "--dag", "factory", "--gate", "intent", "--ready", "--limit",
            "5",
        ])
        .expect("parse");
        let Command::Gates(GatesCmd::List(args)) = gates.command else {
            panic!("expected gates list");
        };
        assert!(args.filter.ready);
        assert_eq!(args.filter.limit, Some(5));

        let jobs = Cli::try_parse_from([
            "swf",
            "jobs",
            "list",
            "--dag",
            "factory",
            "--state",
            "failed",
            "--issue",
            "42",
            "--limit",
            "10",
            "--attention",
        ])
        .expect("parse");
        let Command::Jobs(JobsCmd::List(args)) = jobs.command else {
            panic!("expected jobs list");
        };
        assert!(args.attention);
        assert_eq!(args.state.as_deref(), Some("failed"));
        assert_eq!(args.issue.as_deref(), Some("42"));
        assert_eq!(args.limit, Some(10));

        let runs =
            Cli::try_parse_from(["swf", "runs", "list", "--state", "running"]).expect("parse");
        let Command::Runs(RunsCmd::List { state, limit, .. }) = runs.command else {
            panic!("expected runs list");
        };
        assert_eq!(state.as_deref(), Some("running"));
        assert_eq!(
            limit, 20,
            "the read stays bounded when nothing says otherwise"
        );
    }

    #[test]
    fn the_bulk_help_sends_a_careful_operator_to_the_dry_run_first() {
        let approve = Cli::command()
            .find_subcommand("gates")
            .and_then(|gates| gates.find_subcommand("approve").cloned())
            .map(|mut cmd| cmd.render_long_help().to_string())
            .unwrap_or_default();
        assert!(approve.contains("--dry-run"), "{approve}");
        assert!(approve.contains("writes nothing"), "{approve}");
        assert!(approve.contains("ready"), "{approve}");
    }

    #[test]
    fn verify_refuses_an_identity_and_all_at_once() {
        assert!(
            Cli::try_parse_from(["swf", "deliveries", "verify", "factory/a-b", "--all"]).is_err()
        );
    }
}
