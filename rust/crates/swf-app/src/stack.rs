//! The local development stack: start it, stop it, and say what is *actually* running.
//!
//! `swf stack` drives `deploy/docker/compose.yml` and nothing else. The boundary it defends is the
//! difference between a request and a fact: `up` does not report success because `docker compose`
//! exited zero — it re-reads the stack afterwards and reports what came back. A command that says
//! "started" while the scheduler is crash-looping is how an operator spends twenty minutes
//! debugging their own client.
//!
//! Two mechanical details are load-bearing (`08-local-stack.md` §A.1). Compose mounts the repo at
//! `${PWD}:${PWD}` and sets `working_dir: ${PWD}`, because the Airflow worker bind-mounts each
//! run's workdir into sibling containers at the *same absolute path* — the path has to mean the
//! same thing to the daemon and to the worker. So every invocation exports `PWD` explicitly and
//! passes `--project-directory`, rather than relying on whatever directory `swf` happens to have
//! been started in. And the argv is a `Vec<String>` with an `env` prefix rather than a shell
//! string, because a repo path with a space in it must be a path and never two arguments.
//!
//! This module never touches `deploy/islo/*`. That is a production deployment with its own script,
//! its own credentials and its own blast radius.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use swf_adapters::traits::{CommandRunner, Runs, SUBPROCESS_TIMEOUT};
use swf_domain::sanitize::sanitize_line;
use tokio_util::sync::CancellationToken;

use crate::doctor::HEALTH_COMPONENTS;
use crate::ops::{OpsError, Result};

/// The compose file, relative to the repo root.
pub const COMPOSE_FILE: &str = "deploy/docker/compose.yml";

/// The sandbox image's Dockerfile, relative to the repo root.
pub const SANDBOX_DOCKERFILE: &str = "deploy/docker/sandbox.Dockerfile";

/// The tag the sandbox image is built under.
pub const SANDBOX_IMAGE: &str = "swfactory-sandbox:local";

/// The services `compose.yml` runs by default. `sandbox-image` is a build target (profile
/// `build`) and never runs. `backend` is here because it is no longer profile-gated: it is the
/// console's control plane and every managed work cell calls it, so a stack whose backend died
/// still accepts work orders and then fails each of them in its first stage. Omitting it from
/// this list is what lets `swf stack up` report a healthy stack over exactly that failure.
/// `tests/test_doctor.py::test_stack_status_covers_every_service_the_default_stack_starts`
/// keeps this list and `compose.yml` from drifting apart.
pub const SERVICES: &[&str] = &["airflow", "backend", "webhook"];

/// How long a cold `up` may take before it is a hang: the first run does `uv sync` into a volume.
pub const UP_TIMEOUT: Duration = Duration::from_secs(900);

/// How long to keep polling for health after `up` returns.
pub const READY_TIMEOUT: Duration = Duration::from_secs(360);

/// How often to poll while waiting for health.
pub const READY_POLL: Duration = Duration::from_secs(5);

/// What to do to the stack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StackAction {
    /// Bring it up and wait for it to be healthy.
    Up {
        /// Also build the sandbox image first.
        build: bool,
    },
    /// Take it down.
    Down {
        /// Also drop the named volumes — the metadata DB, the venv and the generated password.
        volumes: bool,
    },
    /// Report what is running.
    Status,
}

impl StackAction {
    /// The word the result prints.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Up { .. } => "up",
            Self::Down { .. } => "down",
            Self::Status => "status",
        }
    }
}

/// Where the stack lives and how long to wait for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StackOpts {
    /// The repo root. Compose is run with this as `--project-directory` *and* as `$PWD`.
    pub repo_root: PathBuf,
    /// The compose file.
    pub compose_file: PathBuf,
    /// How long to wait for the stack to report healthy after `up`.
    pub ready_timeout: Duration,
    /// How often to poll while waiting.
    pub poll: Duration,
}

impl Default for StackOpts {
    fn default() -> Self {
        let root = find_repo_root().unwrap_or_else(|| PathBuf::from("."));
        Self {
            compose_file: root.join(COMPOSE_FILE),
            repo_root: root,
            ready_timeout: READY_TIMEOUT,
            poll: READY_POLL,
        }
    }
}

impl StackOpts {
    /// Point at one checkout.
    pub fn at(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            compose_file: root.join(COMPOSE_FILE),
            repo_root: root,
            ..Self::default()
        }
    }
}

/// One line of the report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StackRow {
    /// What is being reported: a service name, `api`, or a health component.
    pub name: String,
    /// One word: `running`, `healthy`, `down`, `unreachable`.
    pub state: String,
    /// What was actually observed.
    pub detail: String,
    /// Whether this row is how it should be.
    pub ok: bool,
}

impl StackRow {
    /// A row that is how it should be.
    pub fn up(
        name: impl Into<String>,
        state: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            state: state.into(),
            detail: sanitize_line(&detail.into()),
            ok: true,
        }
    }

    /// A row that is not.
    pub fn down(
        name: impl Into<String>,
        state: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            state: state.into(),
            detail: sanitize_line(&detail.into()),
            ok: false,
        }
    }
}

/// What the stack looks like now — never what was asked for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StackStatus {
    /// Which action produced this report.
    pub action: String,
    /// One row per thing that was looked at.
    pub rows: Vec<StackRow>,
    /// True when every row is how it should be.
    pub ok: bool,
    /// The commands that were run, so the output can say what was done rather than claim it.
    pub ran: Vec<String>,
}

impl StackStatus {
    /// Build a report and derive `ok` from the rows rather than from anybody's intent.
    fn new(action: StackAction, rows: Vec<StackRow>, ran: Vec<String>) -> Self {
        Self {
            action: action.as_str().to_string(),
            ok: rows.iter().all(|row| row.ok),
            rows,
            ran,
        }
    }
}

/// Drive the stack and report what came back.
pub async fn run(
    commands: &dyn CommandRunner,
    runs: Option<&dyn Runs>,
    action: StackAction,
    opts: &StackOpts,
    cancel: &CancellationToken,
) -> Result<StackStatus> {
    let mut ran: Vec<String> = Vec::new();

    match action {
        StackAction::Up { build } => {
            if build {
                let argv = build_argv(opts);
                ran.push(argv.join(" "));
                exec(commands, &argv, UP_TIMEOUT, cancel).await?;
            }
            let argv = compose_argv(opts, &["up", "-d"]);
            ran.push(argv.join(" "));
            exec(commands, &argv, UP_TIMEOUT, cancel).await?;
            wait_healthy(runs, opts, cancel).await;
        }
        StackAction::Down { volumes } => {
            let mut args = vec!["down"];
            if volumes {
                args.push("--volumes");
            }
            let argv = compose_argv(opts, &args);
            ran.push(argv.join(" "));
            exec(commands, &argv, UP_TIMEOUT, cancel).await?;
        }
        StackAction::Status => {}
    }

    let argv = compose_argv(opts, &["ps", "--format", "json"]);
    ran.push(argv.join(" "));
    let mut rows = service_rows(commands, &argv, action, cancel).await;
    rows.extend(api_rows(runs, action, cancel).await);
    Ok(StackStatus::new(action, rows, ran))
}

/// The compose invocation, `PWD` and project directory included.
///
/// `env` rather than a shell: the repo root is a path, and a path is one argument no matter what
/// characters it contains.
pub fn compose_argv(opts: &StackOpts, args: &[&str]) -> Vec<String> {
    let root = opts.repo_root.display().to_string();
    let mut argv = vec![
        "env".to_string(),
        format!("PWD={root}"),
        "docker".to_string(),
        "compose".to_string(),
        "--project-directory".to_string(),
        root,
        "-f".to_string(),
        opts.compose_file.display().to_string(),
    ];
    argv.extend(args.iter().map(|arg| (*arg).to_string()));
    argv
}

/// The sandbox image build, which is a plain `docker build` and not a compose service.
pub fn build_argv(opts: &StackOpts) -> Vec<String> {
    vec![
        "docker".to_string(),
        "build".to_string(),
        "-t".to_string(),
        SANDBOX_IMAGE.to_string(),
        "-f".to_string(),
        opts.repo_root
            .join(SANDBOX_DOCKERFILE)
            .display()
            .to_string(),
        opts.repo_root.display().to_string(),
    ]
}

/// Run one command, turning "docker is not installed" into a sentence an operator can act on.
async fn exec(
    commands: &dyn CommandRunner,
    argv: &[String],
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<String> {
    let out = commands.run(argv, timeout, cancel).await.map_err(|err| {
        OpsError::from(err).with_hint("install Docker Desktop, or start the Docker daemon")
    })?;
    if out.code != 0 {
        return Err(OpsError::operational(format!(
            "{} exited {}: {}",
            argv.first().map(String::as_str).unwrap_or("command"),
            out.code,
            out.message()
        )));
    }
    Ok(out.stdout)
}

/// One row per compose service, from `docker compose ps`.
async fn service_rows(
    commands: &dyn CommandRunner,
    argv: &[String],
    action: StackAction,
    cancel: &CancellationToken,
) -> Vec<StackRow> {
    let expect_up = !matches!(action, StackAction::Down { .. });
    let listing = match commands.run(argv, SUBPROCESS_TIMEOUT, cancel).await {
        Ok(out) if out.code == 0 => out.stdout,
        Ok(out) => {
            return vec![StackRow::down("compose", "unknown", out.message())];
        }
        Err(err) => {
            return vec![StackRow::down("compose", "unknown", err.to_string())];
        }
    };

    let services = parse_ps(&listing);
    let mut rows = Vec::new();
    for name in SERVICES {
        match services.iter().find(|svc| svc.service == *name) {
            Some(svc) => {
                let running = svc.state.eq_ignore_ascii_case("running");
                let unhealthy = svc.health.eq_ignore_ascii_case("unhealthy");
                let detail = if svc.health.is_empty() {
                    svc.status.clone()
                } else {
                    format!("{} ({})", svc.status, svc.health)
                };
                let ok = if expect_up {
                    running && !unhealthy
                } else {
                    !running
                };
                rows.push(StackRow {
                    name: (*name).to_string(),
                    state: svc.state.clone(),
                    detail: sanitize_line(&detail),
                    ok,
                });
            }
            None if expect_up => rows.push(StackRow::down(*name, "down", "not running")),
            None => rows.push(StackRow::up(*name, "down", "not running")),
        }
    }
    rows
}

/// The API rows: is Airflow answering, and are its components healthy?
async fn api_rows(
    runs: Option<&dyn Runs>,
    action: StackAction,
    cancel: &CancellationToken,
) -> Vec<StackRow> {
    let Some(runs) = runs else {
        return Vec::new();
    };
    let expect_up = !matches!(action, StackAction::Down { .. });
    match runs.health(cancel).await {
        Ok(health) => {
            let mut rows = vec![StackRow {
                name: "api".to_string(),
                state: "answering".to_string(),
                detail: "monitor/health returned 200".to_string(),
                ok: expect_up,
            }];
            for component in HEALTH_COMPONENTS {
                let status = health
                    .get(*component)
                    .and_then(|value| value.get("status"))
                    .and_then(Value::as_str)
                    .unwrap_or("-");
                rows.push(StackRow {
                    name: (*component).to_string(),
                    state: status.to_string(),
                    detail: String::new(),
                    // `dag_processor` and `triggerer` may legitimately never have run, so an
                    // absent component is reported and not counted against the stack.
                    ok: !expect_up || status == "healthy" || status == "-",
                });
            }
            rows
        }
        Err(err) => vec![StackRow {
            name: "api".to_string(),
            state: "unreachable".to_string(),
            detail: sanitize_line(&err.to_string()),
            ok: !expect_up,
        }],
    }
}

/// Poll `monitor/health` until the stack is up, or until the budget runs out.
///
/// A timeout here is not an error: the report that follows says exactly which component is still
/// unhealthy, which is more useful than an exception that says the word "timeout".
async fn wait_healthy(runs: Option<&dyn Runs>, opts: &StackOpts, cancel: &CancellationToken) {
    let Some(runs) = runs else {
        return;
    };
    let deadline = std::time::Instant::now() + opts.ready_timeout;
    loop {
        if cancel.is_cancelled() || std::time::Instant::now() >= deadline {
            return;
        }
        if let Ok(health) = runs.health(cancel).await {
            let healthy = ["metadatabase", "scheduler"].iter().all(|component| {
                health
                    .get(*component)
                    .and_then(|value| value.get("status"))
                    .and_then(Value::as_str)
                    == Some("healthy")
            });
            if healthy {
                return;
            }
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(opts.poll) => {}
        }
    }
}

/// One service, as `docker compose ps` describes it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ComposeService {
    /// The container name.
    pub name: String,
    /// The compose service name.
    pub service: String,
    /// `running`, `exited`, `created`, …
    pub state: String,
    /// `healthy`, `unhealthy`, `starting`, or empty when the service has no healthcheck.
    pub health: String,
    /// The human status line, e.g. `Up 4 minutes (healthy)`.
    pub status: String,
}

/// Read `docker compose ps --format json`, in both spellings it has had.
///
/// Modern compose emits one JSON object per line; older versions emit a single array. Guessing
/// wrong means reporting an empty stack while everything is running, so both are read.
pub fn parse_ps(text: &str) -> Vec<ComposeService> {
    let mut out = Vec::new();
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return out;
    }
    if let Ok(Value::Array(items)) = serde_json::from_str::<Value>(trimmed) {
        for item in items {
            out.push(service_from(&item));
        }
        return out;
    }
    for line in trimmed.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(value) = serde_json::from_str::<Value>(line) {
            out.push(service_from(&value));
        }
    }
    out
}

/// One `ps` row, tolerating the capitalisation compose has used over the years.
fn service_from(value: &Value) -> ComposeService {
    let text = |keys: &[&str]| -> String {
        keys.iter()
            .filter_map(|key| value.get(*key).and_then(Value::as_str))
            .find(|found| !found.is_empty())
            .unwrap_or_default()
            .to_string()
    };
    ComposeService {
        name: text(&["Name", "name"]),
        service: text(&["Service", "service"]),
        state: text(&["State", "state"]),
        health: text(&["Health", "health"]),
        status: text(&["Status", "status"]),
    }
}

/// Walk up from the working directory looking for the compose file.
pub fn find_repo_root() -> Option<PathBuf> {
    let mut dir = std::env::current_dir().ok()?;
    loop {
        if dir.join(COMPOSE_FILE).is_file() {
            return Some(dir);
        }
        if !dir.pop() {
            return None;
        }
    }
}

/// True when this path looks like a factory checkout — used by callers that want to explain why
/// `swf stack` has nothing to drive.
pub fn is_repo_root(path: &Path) -> bool {
    path.join(COMPOSE_FILE).is_file()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts() -> StackOpts {
        StackOpts::at("/src/my repo")
    }

    #[test]
    fn compose_is_run_with_pwd_and_a_project_directory_and_never_through_a_shell() {
        let argv = compose_argv(&opts(), &["up", "-d"]);
        assert_eq!(
            argv,
            vec![
                "env",
                "PWD=/src/my repo",
                "docker",
                "compose",
                "--project-directory",
                "/src/my repo",
                "-f",
                "/src/my repo/deploy/docker/compose.yml",
                "up",
                "-d",
            ]
        );
        assert!(
            argv.iter().all(|arg| !arg.contains("&&")),
            "a path with a space must be one argument, not a shell string"
        );
    }

    #[test]
    fn the_sandbox_build_names_its_dockerfile_and_its_context() {
        let argv = build_argv(&opts());
        assert_eq!(argv[0], "docker");
        assert!(argv.contains(&SANDBOX_IMAGE.to_string()));
        assert!(argv
            .iter()
            .any(|arg| arg.ends_with("deploy/docker/sandbox.Dockerfile")));
        assert_eq!(argv.last().map(String::as_str), Some("/src/my repo"));
    }

    #[test]
    fn ps_is_read_as_ndjson_and_as_an_array() {
        let ndjson = r#"{"Name":"swfactory-airflow-1","Service":"airflow","State":"running","Health":"healthy","Status":"Up 4 minutes (healthy)"}
{"Name":"swfactory-webhook-1","Service":"webhook","State":"running","Health":"","Status":"Up 4 minutes"}"#;
        let rows = parse_ps(ndjson);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].service, "airflow");
        assert_eq!(rows[0].health, "healthy");

        let array = r#"[{"Service":"airflow","State":"exited","Status":"Exited (1)"}]"#;
        let rows = parse_ps(array);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, "exited");

        assert!(parse_ps("").is_empty());
        assert!(parse_ps("not json at all").is_empty());
    }

    #[test]
    fn a_status_report_derives_ok_from_the_rows_and_not_from_the_request() {
        let status = StackStatus::new(
            StackAction::Up { build: false },
            vec![
                StackRow::up("airflow", "running", "Up 4 minutes (healthy)"),
                StackRow::down("webhook", "down", "not running"),
            ],
            vec!["docker compose up -d".into()],
        );
        assert_eq!(status.action, "up");
        assert!(!status.ok, "one dead service means the stack is not up");
        assert_eq!(status.ran.len(), 1);
    }

    #[test]
    fn a_row_detail_from_a_container_cannot_repaint_the_terminal() {
        let row = StackRow::down("airflow", "exited", "boom\u{1b}[2Jgone");
        assert_eq!(row.detail, "boomgone");
    }
}
