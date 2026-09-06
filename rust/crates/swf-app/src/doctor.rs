//! Readiness, from a machine that has no Python on it.
//!
//! `swf doctor` is the command someone runs when nothing works, so the boundary it defends is
//! *actionability*: every check that can fail carries the literal command that fixes it, and
//! nothing is marked required unless the product genuinely needs it. A report that shouts about an
//! optional integration teaches people to ignore the report.
//!
//! Only checks a no-Python client can honestly perform live here (`05-doctor-config.md` §4.1):
//! HTTP against Airflow, a subprocess for `gh` and `islo`, a TOML parse for the blueprint, and a
//! stat for the metrics root. The Python orchestrator's own checks — the toolset backend, the
//! Config trust boundary — are *not* reimplemented and are not silently dropped either; they are
//! simply not this client's to answer, and the rows below say what they do say and no more.
//!
//! Two probes are kept separate on purpose. `airflow api` is unauthenticated and `airflow auth`
//! carries the credential, because collapsing them makes "wrong URL" and "wrong token" produce the
//! same red line — and those two have completely different fixes.

use std::time::Duration;

use serde_json::Value;
use swf_adapters::error::AdapterError;
use swf_adapters::traits::{CommandRunner, Runs, SUBPROCESS_TIMEOUT};
use swf_domain::doctor::Check;
use swf_domain::sanitize::sanitize_line;
use tokio_util::sync::CancellationToken;

use crate::context::{Auth, Context};
use crate::submit::{find_blueprint, BLUEPRINTS_DIR};

/// The Airflow health components a local stack must report healthy (`08-local-stack.md` §A.9).
pub const HEALTH_COMPONENTS: &[&str] = &["metadatabase", "scheduler", "dag_processor", "triggerer"];

/// The components whose failure means the API cannot be trusted at all. `dag_processor` and
/// `triggerer` may legitimately be `null` on a server that has never run one.
pub const REQUIRED_COMPONENTS: &[&str] = &["metadatabase", "scheduler"];

/// Everything this client can check for itself, in the order an operator would fix them.
///
/// Never fails: a failing check *is* the answer, and a doctor that returns `Err` is a doctor that
/// cannot report the one thing it was run to report.
pub async fn checks(
    context: &Context,
    runs: Option<&dyn Runs>,
    commands: &dyn CommandRunner,
    cancel: &CancellationToken,
) -> Vec<Check> {
    let mut out = Vec::new();
    out.push(context_check(context));
    out.push(credential_check(context));

    match runs {
        Some(runs) => {
            out.push(api_check(runs, context, cancel).await);
            let (auth, dags) = auth_check(runs, context, cancel).await;
            out.push(auth);
            out.push(dags);
        }
        None => {
            out.push(Check::fail(
                "airflow api",
                "no Airflow client is configured",
                "swf context add <name> --airflow-url http://localhost:8080",
            ));
        }
    }

    out.push(blueprint_check());
    out.push(gh_check(context, commands, cancel).await);
    out.push(islo_check(context, commands, cancel).await);
    out.push(metrics_check(context));
    out
}

/// Is there a context at all, and is it one someone chose?
fn context_check(context: &Context) -> Check {
    let detail = format!("{} -> {}", context.name, context.airflow_url);
    if context.is_builtin() {
        // Not required: a fresh machine pointed at localhost is a *state*, not a fault, and the
        // Airflow checks below will say plainly whether anything is listening there.
        return Check::fail(
            "context",
            format!("{detail} (built-in fallback; nothing is configured)"),
            "swf context add local --airflow-url http://localhost:8080",
        )
        .optional();
    }
    Check::pass("context", detail)
}

/// Does the credential this context names actually exist in the environment?
///
/// Checked separately from the request it is used for, because "the variable is unset" and "the
/// server rejected the token" are different problems with different fixes, and only the first one
/// can be diagnosed without the network.
fn credential_check(context: &Context) -> Check {
    match &context.auth {
        Auth::None => Check::pass("credential", "none (anonymous access)"),
        auth => {
            let missing: Vec<&str> = auth
                .env_vars()
                .into_iter()
                .filter(|var| std::env::var(var).is_err())
                .collect();
            if missing.is_empty() {
                Check::pass("credential", auth.redacted())
            } else {
                Check::fail(
                    "credential",
                    format!("{} is not set", missing.join(", ")),
                    format!("export {}=…", missing.join("=… ")),
                )
            }
        }
    }
}

/// Is something Airflow-shaped listening at this URL? Asked without a credential on purpose.
async fn api_check(runs: &dyn Runs, context: &Context, cancel: &CancellationToken) -> Check {
    match runs.health(cancel).await {
        Ok(health) => {
            let broken: Vec<String> = REQUIRED_COMPONENTS
                .iter()
                .filter(|name| component(&health, name).as_deref() != Some("healthy"))
                .map(|name| {
                    format!(
                        "{name} {}",
                        component(&health, name).unwrap_or_else(|| "unknown".into())
                    )
                })
                .collect();
            if broken.is_empty() {
                Check::pass("airflow api", summarise_health(&health))
            } else {
                Check::fail(
                    "airflow api",
                    broken.join(", "),
                    "swf stack status  # or check the scheduler on that host",
                )
            }
        }
        Err(err) => Check::fail(
            "airflow api",
            format!("{} is unreachable: {}", context.airflow_url, one_line(&err)),
            "check airflow_url, then: swf stack up",
        ),
    }
}

/// Does the credential get us a DAG list, and is anything tagged for the factory?
async fn auth_check(
    runs: &dyn Runs,
    context: &Context,
    cancel: &CancellationToken,
) -> (Check, Check) {
    match runs.list_dags(&context.dag_tag, cancel).await {
        Ok(page) => {
            let count = page.rows.len();
            let auth = Check::pass("airflow auth", format!("{} accepted", auth_word(context)));
            let tag = &context.dag_tag;
            let dags = if count == 0 {
                // Informational: a server with no factory DAGs may simply be tagged differently,
                // and failing the whole report over a naming choice helps nobody.
                Check::fail(
                    "dags",
                    format!("no DAG carries the tag {tag:?}"),
                    "check dag_tag, or deploy the blueprints to this server",
                )
                .optional()
            } else {
                Check::pass("dags", format!("{count} tagged {tag:?}"))
            };
            (auth, dags)
        }
        Err(err) if err.is_auth() => (
            Check::fail("airflow auth", one_line(&err), credential_fix(context)),
            Check::fail("dags", "not read: authentication failed", "").optional(),
        ),
        Err(err) => (
            Check::fail(
                "airflow auth",
                one_line(&err),
                "swf doctor --json  # and check the airflow api row above",
            ),
            Check::fail("dags", "not read", "").optional(),
        ),
    }
}

/// The literal fix for a rejected credential, named for the kind of credential it is.
fn credential_fix(context: &Context) -> String {
    match &context.auth {
        Auth::None => "this context sends no credential; add auth to it".to_string(),
        Auth::TokenEnv { var } => format!("export {var}=<a fresh token>"),
        Auth::Basic { user, password_env } => {
            format!("check {user}'s password in ${password_env}")
        }
    }
}

/// How this context proves who it is, in one word.
fn auth_word(context: &Context) -> &'static str {
    match &context.auth {
        Auth::None => "anonymous access",
        Auth::TokenEnv { .. } => "bearer token",
        Auth::Basic { .. } => "minted token",
    }
}

/// Does the default blueprint parse, if there is one here to parse?
///
/// Informational, because a remote operator has no checkout: the DAG lives on the server, and a
/// laptop without `blueprints/` is a perfectly working laptop.
fn blueprint_check() -> Check {
    let name = swf_domain::blueprint::DEFAULT_BLUEPRINT;
    let Some(path) = find_blueprint(name) else {
        return Check::fail(
            "blueprint",
            format!("skipped: no {BLUEPRINTS_DIR}/ directory here"),
            "run from a factory checkout, or set SWF_BLUEPRINTS_DIR",
        )
        .optional();
    };
    match std::fs::read_to_string(&path) {
        Ok(text) => match swf_domain::blueprint::Blueprint::from_toml(&text) {
            Ok(blueprint) => Check::pass(
                "blueprint",
                format!(
                    "{} ({} stage(s), {} target(s))",
                    path.display(),
                    blueprint.order.len(),
                    blueprint.targets.len()
                ),
            ),
            Err(err) => Check::fail(
                "blueprint",
                format!("{}: {err}", path.display()),
                format!("edit {}", path.display()),
            ),
        },
        Err(err) => Check::fail(
            "blueprint",
            format!("{}: {err}", path.display()),
            format!("check the permissions on {}", path.display()),
        ),
    }
}

/// Is `gh` installed and logged in? Required only when this context has a repo to reach.
async fn gh_check(
    context: &Context,
    commands: &dyn CommandRunner,
    cancel: &CancellationToken,
) -> Check {
    let argv = vec!["gh".to_string(), "auth".to_string(), "status".to_string()];
    let check = match commands.run(&argv, SUBPROCESS_TIMEOUT, cancel).await {
        Ok(out) if out.code == 0 => Check::pass("gh auth", first_line(&out.stdout, &out.stderr)),
        Ok(out) => Check::fail(
            "gh auth",
            first_line(&out.stderr, &out.stdout),
            "gh auth login",
        ),
        Err(AdapterError::Unreachable { .. }) => Check::fail(
            "gh auth",
            "gh is not on PATH",
            "install the GitHub CLI: https://cli.github.com",
        ),
        Err(err) => Check::fail("gh auth", one_line(&err), "gh auth login"),
    };
    if context.repo.is_some() {
        check
    } else {
        check.optional()
    }
}

/// Is the sandbox CLI installed? Required only when this context owns sandboxes.
async fn islo_check(
    context: &Context,
    commands: &dyn CommandRunner,
    cancel: &CancellationToken,
) -> Check {
    let argv = vec!["islo".to_string(), "--version".to_string()];
    let check = match commands.run(&argv, SUBPROCESS_TIMEOUT, cancel).await {
        Ok(out) if out.code == 0 => Check::pass("islo cli", first_line(&out.stdout, &out.stderr)),
        Ok(out) => Check::fail(
            "islo cli",
            first_line(&out.stderr, &out.stdout),
            "reinstall the islo CLI",
        ),
        Err(AdapterError::Unreachable { .. }) => Check::fail(
            "islo cli",
            "islo is not on PATH",
            "install the islo CLI, or drop `owner` from this context",
        ),
        Err(err) => Check::fail("islo cli", one_line(&err), "reinstall the islo CLI"),
    };
    if context.owner.is_some() {
        check
    } else {
        check.optional()
    }
}

/// Is the metrics root a directory that exists? Informational: `swf metrics` is the only reader.
fn metrics_check(context: &Context) -> Check {
    let path = context.metrics_path();
    if path.is_dir() {
        Check::pass("metrics root", path.display().to_string())
    } else {
        Check::fail(
            "metrics root",
            format!("{} is not a directory", path.display()),
            "swf context add … --metrics-root <path to a factory checkout>",
        )
        .optional()
    }
}

/// One component's status out of the health document, tolerating `null` and a missing key.
fn component(health: &Value, name: &str) -> Option<String> {
    health
        .get(name)?
        .get("status")
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// The health document as one line, in the order `08-local-stack.md` §A.9 lists the rows.
fn summarise_health(health: &Value) -> String {
    HEALTH_COMPONENTS
        .iter()
        .map(|name| {
            format!(
                "{name} {}",
                component(health, name).unwrap_or_else(|| "-".into())
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The first non-empty line of `primary`, falling back to `secondary`. Untrusted: sanitised.
fn first_line(primary: &str, secondary: &str) -> String {
    let pick = |text: &str| {
        text.lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .map(str::to_string)
    };
    sanitize_line(
        &pick(primary)
            .or_else(|| pick(secondary))
            .unwrap_or_default(),
    )
}

/// An adapter error as one sanitised line.
fn one_line(err: &AdapterError) -> String {
    sanitize_line(&err.to_string())
}

/// The subprocess deadline these checks use, re-exported so a caller can say what it waited for.
pub const CHECK_TIMEOUT: Duration = SUBPROCESS_TIMEOUT;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use swf_domain::doctor::{exit_code, failed};

    #[test]
    fn a_fresh_machine_is_told_what_to_add_rather_than_failing() {
        let check = context_check(&Context::builtin());
        assert!(!check.ok);
        assert!(!check.required, "a fresh machine is not a broken machine");
        assert!(check.fix.contains("swf context add"), "{}", check.fix);
        assert_eq!(exit_code(&[check]), 0);
    }

    #[test]
    fn a_configured_context_passes_and_shows_where_it_points() {
        let mut ctx = Context::new("prod", "https://airflow.example.com/");
        ctx.auth = Auth::TokenEnv {
            var: "SWF_DOCTOR_TEST_TOKEN".into(),
        };
        let check = context_check(&ctx);
        assert!(check.ok);
        assert_eq!(check.detail, "prod -> https://airflow.example.com");
    }

    #[test]
    fn a_missing_credential_variable_is_named_and_never_read() {
        let mut ctx = Context::new("prod", "http://h:8080");
        ctx.auth = Auth::Basic {
            user: "admin".into(),
            password_env: "SWF_DOCTOR_UNSET_PASSWORD".into(),
        };
        let check = credential_check(&ctx);
        assert!(!check.ok);
        assert!(check.detail.contains("SWF_DOCTOR_UNSET_PASSWORD"));
        assert_eq!(failed(std::slice::from_ref(&check)).len(), 1);
        assert!(!check.detail.contains("admin's password"));
    }

    #[test]
    fn health_is_summarised_and_a_null_component_is_not_a_failure() {
        let health = json!({
            "metadatabase": {"status": "healthy"},
            "scheduler": {"status": "healthy"},
            "dag_processor": null,
            "triggerer": {"status": null},
        });
        assert_eq!(
            component(&health, "metadatabase").as_deref(),
            Some("healthy")
        );
        assert_eq!(component(&health, "dag_processor"), None);
        let line = summarise_health(&health);
        assert!(
            line.starts_with("metadatabase healthy, scheduler healthy"),
            "{line}"
        );
        assert!(line.contains("triggerer -"), "{line}");
    }

    #[test]
    fn subprocess_output_is_sanitised_before_it_reaches_a_terminal() {
        let line = first_line("\n  \u{1b}[31mgh version 2.40\u{1b}[0m\n", "");
        assert_eq!(line, "gh version 2.40");
        assert_eq!(first_line("", "  fell over  "), "fell over");
        assert_eq!(first_line("", ""), "");
    }
}
