//! The process contract, exercised the way a script exercises it.
//!
//! Everything here runs the real binary and looks only at what a caller can see: the exit code,
//! stdout, stderr. That is deliberate — the exit-code table and the `--json` documents are public
//! surface under the project's semver policy (`00-architecture.md` §D-E), and a test that reached
//! inside the process could not tell whether the promise survived to the process boundary.
//!
//! Nothing here touches a network it did not start. Where a service is needed, the address points
//! at a closed port, because "nothing answered" is itself one of the six outcomes.

use std::process::Command as StdCommand;

use assert_cmd::Command;
use serde_json::Value;
use tempfile::TempDir;

/// A closed port on the loopback interface: connecting to it fails immediately and locally.
const NOWHERE: &str = "http://127.0.0.1:1";

/// Every subcommand path, so `--help` can be asserted for all of them and none is forgotten.
const PATHS: &[&[&str]] = &[
    &[],
    &["context"],
    &["context", "list"],
    &["context", "use"],
    &["context", "add"],
    &["context", "show"],
    &["context", "remove"],
    &["doctor"],
    &["submit"],
    &["attention"],
    &["runs"],
    &["runs", "list"],
    &["runs", "inspect"],
    &["runs", "stop"],
    &["runs", "unpause"],
    &["jobs"],
    &["jobs", "list"],
    &["jobs", "inspect"],
    &["cells"],
    &["cells", "list"],
    &["cells", "inspect"],
    &["cells", "history"],
    &["queue"],
    &["queue", "list"],
    &["queue", "inspect"],
    &["operations"],
    &["operations", "list"],
    &["operations", "inspect"],
    &["fleet"],
    &["compatibility"],
    &["logs"],
    &["gates"],
    &["gates", "list"],
    &["gates", "review"],
    &["gates", "approve"],
    &["gates", "reject"],
    &["deliveries"],
    &["deliveries", "list"],
    &["deliveries", "verify"],
    &["sandboxes"],
    &["sandboxes", "list"],
    &["sandboxes", "inspect"],
    &["sandboxes", "rm"],
    &["metrics"],
    &["snapshot"],
    &["stack"],
    &["stack", "up"],
    &["stack", "down"],
    &["stack", "status"],
    &["tui"],
    &["completions"],
    &["version"],
];

/// A `swf` pointed at a config file of its own, with the operator's environment left out of it.
fn swf(home: &TempDir) -> Command {
    let mut cmd = Command::cargo_bin("swf").expect("the binary builds");
    cmd.env("SWF_CONFIG", home.path().join("config.toml"));
    cmd.env_remove("SWF_CONTEXT");
    cmd.env_remove("NO_COLOR");
    cmd.env_remove("XDG_CONFIG_HOME");
    // The backend variables override a context for one process, so an operator's own shell must
    // not decide whether a test is talking to a backend or is in direct mode.
    cmd.env_remove("SWF_BACKEND_URL");
    cmd.env_remove("SWF_BACKEND_TOKEN");
    cmd
}

/// A backend token the client will accept, so a test exercises transport and not the guard.
///
/// `FactoryApi` refuses a short or whitespace-bearing token before it opens a socket, which is
/// the right thing for an operator and the wrong thing for a test that means to reach the wire.
const TOKEN: &str = "0123456789abcdef0123456789abcdef";

/// Every command group the binary answers, spelled the way an operator types it.
///
/// The whole list, not a sample: the defect this guards against is one verb going missing from the
/// discovery surfaces, and a verb that nobody thought to list here is exactly the one that would.
const GROUPS: &[&str] = &[
    "context",
    "doctor",
    "submit",
    "attention",
    "runs",
    "jobs",
    "cells",
    "queue",
    "operations",
    "fleet",
    "compatibility",
    "logs",
    "gates",
    "deliveries",
    "sandboxes",
    "metrics",
    "snapshot",
    "stack",
    "tui",
    "completions",
    "version",
    "help",
];

/// The groups only the Python factory backend can serve.
const BACKEND_ONLY: &[&[&str]] = &[
    &["cells", "list"],
    &["queue", "list"],
    &["operations", "list"],
    &["fleet"],
    &["compatibility"],
];

/// A home whose only context reaches a factory backend at `url`.
fn backed(url: &str) -> TempDir {
    let home = TempDir::new().expect("tempdir");
    swf(&home)
        .args([
            "context",
            "add",
            "t",
            "--airflow-url",
            url,
            "--backend-url",
            url,
            "--dag",
            "factory",
            "--use",
        ])
        .assert()
        .success();
    home
}

/// A home with one context, pointed wherever the caller says.
fn connected(url: &str) -> TempDir {
    let home = TempDir::new().expect("tempdir");
    swf(&home)
        .args([
            "context",
            "add",
            "t",
            "--direct",
            "--airflow-url",
            url,
            "--use",
        ])
        .assert()
        .success();
    home
}

/// stdout, parsed as the one document `--json` promised.
fn document(output: &[u8]) -> Value {
    serde_json::from_slice(output).unwrap_or_else(|err| {
        panic!(
            "stdout was not one JSON document ({err}): {}",
            String::from_utf8_lossy(output)
        )
    })
}

#[test]
fn every_subcommand_explains_itself() {
    let home = TempDir::new().expect("tempdir");
    for path in PATHS {
        let output = swf(&home)
            .args(path.iter().copied())
            .arg("--help")
            .output()
            .expect("run");
        assert!(
            output.status.success(),
            "swf {} --help exited {:?}",
            path.join(" "),
            output.status.code()
        );
        assert!(
            !output.stdout.is_empty(),
            "swf {} --help printed nothing",
            path.join(" ")
        );
    }
}

#[test]
fn the_exit_code_table_is_documented_where_a_script_author_will_look() {
    let home = TempDir::new().expect("tempdir");
    let output = swf(&home).arg("--help").output().expect("run");
    let help = String::from_utf8_lossy(&output.stdout).to_string();
    for line in [
        "0  success",
        "2  usage error",
        "3  not found",
        "4  authentication",
        "5  service unreachable",
        "6  conflict",
    ] {
        assert!(help.contains(line), "--help is missing {line:?}:\n{help}");
    }
}

#[test]
fn stop_tells_the_truth_about_what_airflow_can_do() {
    // Non-negotiable 9: the help must correct the verb before an operator believes their sandbox
    // was cleaned up.
    let home = TempDir::new().expect("tempdir");
    let output = swf(&home)
        .args(["runs", "stop", "--help"])
        .output()
        .expect("run");
    let help = String::from_utf8_lossy(&output.stdout).to_lowercase();
    assert!(help.contains("mark"), "{help}");
    assert!(help.contains("does not stop processes"), "{help}");
    assert!(help.contains("sandbox"), "{help}");
}

#[test]
fn the_version_is_the_one_product_version() {
    let home = TempDir::new().expect("tempdir");
    let expected = env!("CARGO_PKG_VERSION");
    let output = swf(&home).arg("--version").output().expect("run");
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        format!("swf {expected}")
    );

    let output = swf(&home)
        .args(["version", "--json"])
        .output()
        .expect("run");
    let doc = document(&output.stdout);
    assert_eq!(doc["name"], "swf");
    assert_eq!(doc["version"], expected);
}

#[test]
fn completions_are_generated_from_the_parser_itself() {
    let home = TempDir::new().expect("tempdir");
    for shell in ["bash", "zsh", "fish"] {
        let output = swf(&home)
            .args(["completions", shell])
            .output()
            .expect("run");
        assert!(output.status.success(), "{shell}");
        let script = String::from_utf8_lossy(&output.stdout).to_string();
        assert!(script.contains("swf"), "{shell}: {script}");
        assert!(
            script.contains("doctor"),
            "{shell} completion is missing a verb"
        );
    }
    swf(&home).args(["completions", "klingon"]).assert().code(2);
}

#[test]
fn a_bad_flag_is_a_usage_error_and_still_answers_a_json_consumer() {
    let home = TempDir::new().expect("tempdir");
    swf(&home).args(["doctor", "--nope"]).assert().code(2);

    let output = swf(&home)
        .args(["doctor", "--nope", "--json"])
        .output()
        .expect("run");
    assert_eq!(output.status.code(), Some(2));
    let doc = document(&output.stdout);
    assert_eq!(doc["error"]["kind"], "usage");
    assert_eq!(doc["error"]["exit_code"], 2);
}

#[test]
fn an_identity_that_could_never_name_anything_is_a_usage_error() {
    let home = connected(NOWHERE);
    let output = swf(&home)
        .args(["jobs", "inspect", "not-an-identity", "--json"])
        .output()
        .expect("run");
    assert_eq!(output.status.code(), Some(2), "{:?}", output);
    assert_eq!(document(&output.stdout)["error"]["kind"], "usage");
}

#[test]
fn a_context_that_does_not_exist_is_not_found_and_never_a_silent_fallback() {
    // An operator who typos `--context prd` must not be quietly pointed at production.
    let home = connected(NOWHERE);
    let output = swf(&home)
        .args(["--context", "prd", "doctor", "--json"])
        .output()
        .expect("run");
    assert_eq!(output.status.code(), Some(3));
    let doc = document(&output.stdout);
    assert_eq!(doc["error"]["kind"], "not_found");
    assert_eq!(doc["error"]["exit_code"], 3);

    swf(&home).args(["context", "show", "prd"]).assert().code(3);
}

#[test]
fn a_mutation_with_no_terminal_to_ask_is_refused_rather_than_assumed() {
    // stdin is not a tty here, which is exactly the cron-job case: answering a gate because
    // nobody was there to object is the failure this rule exists to prevent.
    let home = connected(NOWHERE);
    let output = swf(&home)
        .args(["gates", "approve", "factory/manual__1#0:intent"])
        .output()
        .expect("run");
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(stderr.contains("--yes"), "{stderr}");
    assert!(stderr.contains("no terminal"), "{stderr}");
    assert!(
        output.stdout.is_empty(),
        "stdout must stay clean without --json"
    );
}

#[test]
fn the_same_refusal_reaches_a_json_consumer_as_a_document() {
    let home = connected(NOWHERE);
    let output = swf(&home)
        .args(["runs", "stop", "factory/manual__1", "--json"])
        .output()
        .expect("run");
    assert_eq!(output.status.code(), Some(2));
    let doc = document(&output.stdout);
    assert_eq!(doc["error"]["kind"], "usage");
    assert_eq!(doc["error"]["hint"], "pass --yes if you mean it");
}

#[test]
fn nothing_answering_is_its_own_exit_code() {
    let home = connected(NOWHERE);
    let output = swf(&home)
        .args(["runs", "list", "--dag", "factory", "--json"])
        .output()
        .expect("run");
    assert_eq!(output.status.code(), Some(5), "{:?}", output);
    let doc = document(&output.stdout);
    assert_eq!(doc["error"]["kind"], "unreachable");
    assert_eq!(doc["error"]["exit_code"], 5);
}

#[test]
fn a_credential_variable_that_is_not_set_is_an_auth_failure_before_any_request() {
    let home = TempDir::new().expect("tempdir");
    swf(&home)
        .args([
            "context",
            "add",
            "t",
            "--airflow-url",
            NOWHERE,
            "--user",
            "admin",
            "--password-env",
            "SWF_TEST_PASSWORD_THAT_IS_NOT_SET",
            "--use",
        ])
        .assert()
        .success();
    let output = swf(&home)
        .args(["runs", "list", "--dag", "factory", "--json"])
        .env_remove("SWF_TEST_PASSWORD_THAT_IS_NOT_SET")
        .output()
        .expect("run");
    assert_eq!(output.status.code(), Some(4));
    assert_eq!(document(&output.stdout)["error"]["kind"], "auth");
}

#[test]
fn a_context_never_stores_a_secret_and_never_shows_one() {
    let home = TempDir::new().expect("tempdir");
    swf(&home)
        .args([
            "context",
            "add",
            "e2e",
            "--airflow-url",
            "http://localhost:8080/",
            "--repo",
            "zozo123/ariflow-swfactory",
            "--user",
            "admin",
            "--password-env",
            "SWF_E2E_PASSWORD",
            "--metrics-root",
            ".",
            "--use",
        ])
        .env("SWF_E2E_PASSWORD", "hunter2")
        .assert()
        .success();

    let config = std::fs::read_to_string(home.path().join("config.toml")).expect("config written");
    assert!(
        !config.contains("hunter2"),
        "the password reached the file:\n{config}"
    );
    assert!(config.contains("SWF_E2E_PASSWORD"), "{config}");

    let output = swf(&home)
        .args(["context", "show", "--json"])
        .env("SWF_E2E_PASSWORD", "hunter2")
        .output()
        .expect("run");
    let doc = document(&output.stdout);
    assert!(!String::from_utf8_lossy(&output.stdout).contains("hunter2"));
    assert_eq!(doc["name"], "e2e");
    // The trailing slash is stripped once, on load, so no call site has to think about it.
    assert_eq!(doc["airflow_url"], "http://localhost:8080");
    assert_eq!(doc["auth"]["kind"], "basic");
    assert_eq!(doc["auth"]["password_env"], "SWF_E2E_PASSWORD");
    assert!(doc["auth"].get("password").is_none());

    let output = swf(&home)
        .args(["context", "list", "--json"])
        .output()
        .expect("run");
    let doc = document(&output.stdout);
    assert_eq!(doc.as_array().map(Vec::len), Some(1));
    assert_eq!(doc[0]["active"], true);
}

#[test]
fn a_password_flag_with_no_user_is_refused_before_the_file_is_written() {
    let home = TempDir::new().expect("tempdir");
    swf(&home)
        .args([
            "context",
            "add",
            "t",
            "--airflow-url",
            NOWHERE,
            "--password-env",
            "P",
        ])
        .assert()
        .code(2);
    assert!(!home.path().join("config.toml").exists());
}

#[test]
fn doctor_reports_rows_rather_than_refusing_to_report() {
    // A doctor that returns an error envelope is a doctor that cannot report the one thing it was
    // run to report. Red rows are the answer, and the e2e reads them out of this document.
    let home = connected(NOWHERE);
    let output = swf(&home).args(["doctor", "--json"]).output().expect("run");
    assert_eq!(output.status.code(), Some(1), "{:?}", output);
    let doc = document(&output.stdout);
    let rows = doc.as_array().expect("an array of checks");
    assert!(!rows.is_empty());
    for row in rows {
        for key in ["name", "ok", "detail", "fix", "required", "status"] {
            assert!(row.get(key).is_some(), "check row is missing {key}: {row}");
        }
    }
    let airflow: Vec<&Value> = rows
        .iter()
        .filter(|row| {
            row["name"]
                .as_str()
                .unwrap_or_default()
                .starts_with("airflow")
        })
        .collect();
    assert!(
        !airflow.is_empty(),
        "doctor produced no airflow check at all"
    );

    let output = swf(&home).arg("doctor").output().expect("run");
    let text = String::from_utf8_lossy(&output.stdout).to_string();
    assert!(
        text.contains("fix:"),
        "a failure must carry a fix line:\n{text}"
    );
    assert!(text.contains("checks,"), "{text}");
}

#[test]
fn a_snapshot_survives_every_source_being_down() {
    // Per-source degradation (non-negotiable 4): one dead service never blanks the document, and
    // the failure is said out loud on stderr rather than rendered as an empty factory.
    let home = connected(NOWHERE);
    let output = swf(&home)
        .args(["snapshot", "--json"])
        .output()
        .expect("run");
    assert_eq!(output.status.code(), Some(0), "{:?}", output);
    let doc = document(&output.stdout);
    for key in [
        "collected_at",
        "runs",
        "gates",
        "prs",
        "sandboxes",
        "metrics",
        "errors",
    ] {
        assert!(doc.get(key).is_some(), "snapshot is missing {key}");
    }
    assert!(doc["errors"].as_object().is_some_and(|e| !e.is_empty()));
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(stderr.contains("unavailable"), "{stderr}");
}

#[test]
fn colour_is_never_written_into_a_pipe() {
    let home = connected(NOWHERE);
    let output = swf(&home).arg("doctor").output().expect("run");
    let text = String::from_utf8_lossy(&output.stdout).to_string();
    assert!(
        !text.contains('\u{1b}'),
        "an escape sequence reached a pipe:\n{text}"
    );
}

#[test]
fn the_binary_runs_at_all() {
    // `assert_cmd::cargo_bin` finds the artifact; this proves the artifact is the one the e2e
    // script will invoke as `swf`.
    let path = assert_cmd::cargo::cargo_bin("swf");
    let output = StdCommand::new(path)
        .arg("--version")
        .output()
        .expect("run");
    assert!(output.status.success());
}

#[test]
fn a_batch_refusal_happens_before_anything_is_read_let_alone_written() {
    // Every one of these is refused by the process itself: the address points at a closed port,
    // so a refusal that needed a server would show up here as exit 5 instead of 2.
    let home = connected(NOWHERE);
    let cases: &[&[&str]] = &[
        &["gates", "approve", "--all", "--force", "--yes"],
        &["gates", "approve", "--all", "--yes", "factory/r1#0:plan"],
        &["gates", "approve", "--all", "--yes", "--expect", "abc123"],
        &[
            "gates",
            "approve",
            "--dag",
            "factory",
            "factory/r1#0:plan",
            "--yes",
        ],
        &["gates", "approve", "--dry-run", "factory/r1#0:plan"],
        &["gates", "approve"],
        &["gates", "reject", "--all", "--force", "--yes"],
    ];
    for case in cases {
        let output = swf(&home)
            .args(case.iter().copied())
            .arg("--json")
            .output()
            .expect("run");
        assert_eq!(
            output.status.code(),
            Some(2),
            "swf {} should be a usage error: {}",
            case.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
        let doc = document(&output.stdout);
        assert_eq!(doc["error"]["kind"], "usage", "swf {}", case.join(" "));
    }
}

#[test]
fn the_bulk_answer_documents_the_dry_run_where_an_operator_will_look() {
    let home = TempDir::new().expect("tempdir");
    let output = swf(&home)
        .args(["gates", "approve", "--help"])
        .output()
        .expect("run");
    let help = String::from_utf8_lossy(&output.stdout);
    assert!(help.contains("--dry-run"), "{help}");
    assert!(help.contains("--all"), "{help}");
    assert!(
        help.contains("write nothing") || help.contains("writes nothing"),
        "{help}"
    );
}

/// The verbs clap actually lists under `Commands:`, as an operator would read them off the screen.
///
/// The first token of a row and nothing else. A plain substring search over the help text is not
/// this test: `swf fleet` is described as "cells, queue depth and repair debt", so a search for
/// "queue" or "cells" succeeds out of *another* command's prose and would keep succeeding after
/// the verb itself had been deleted.
fn commands_listed_in(help: &str) -> Vec<String> {
    help.lines()
        .skip_while(|line| line.trim_start() != "Commands:")
        .skip(1)
        .take_while(|line| !line.trim().is_empty())
        .filter(|line| line.starts_with("  ") && !line.starts_with("      "))
        .filter_map(|line| line.split_whitespace().next())
        .map(str::to_string)
        .collect()
}

/// How each shell's script dispatches on a *top-level* verb, as clap_complete emits it.
///
/// Anchored on the generated structure rather than on the word appearing somewhere, for the same
/// reason as above: `swf runs --state` is documented as "queued, running, …", which contains
/// "queue", and the fleet description contains both "queue" and "cells". Matching those is
/// matching prose. If clap_complete ever changes these shapes the assertion fails for every verb
/// at once, which is a loud failure and the one worth having.
fn top_level_completion_markers(shell: &str, group: &str) -> Vec<String> {
    match shell {
        "bash" => vec![format!("swf,{group})")],
        "fish" => vec![format!("__fish_swf_needs_command\" -f -a \"{group}\"")],
        "zsh" => vec![format!("\n'{group}:")],
        other => panic!("no marker known for {other}"),
    }
}

#[test]
fn every_command_group_is_in_the_help_and_in_the_completions() {
    // A verb that the binary answers but the parser behind `--help` and `completions` has never
    // heard of is a verb no operator can discover and no shell can finish (#197). The two are one
    // assertion on purpose: they are the same defect seen from two directions.
    let home = TempDir::new().expect("tempdir");
    let help = String::from_utf8_lossy(&swf(&home).arg("--help").output().expect("run").stdout)
        .to_string();
    let listed = commands_listed_in(&help);
    assert!(
        listed.len() >= GROUPS.len(),
        "the Commands: block was not parsed, only {listed:?} came out of:\n{help}"
    );
    for group in GROUPS {
        assert!(
            listed.iter().any(|found| found == group),
            "--help lists no `{group}` command, only {listed:?}:\n{help}"
        );
    }
    for shell in ["bash", "zsh", "fish"] {
        let out = swf(&home)
            .args(["completions", shell])
            .output()
            .expect("run");
        assert!(out.status.success(), "{shell}");
        let script = String::from_utf8_lossy(&out.stdout).to_string();
        for group in GROUPS {
            for marker in top_level_completion_markers(shell, group) {
                assert!(
                    script.contains(&marker),
                    "the {shell} completion cannot finish `{group}`: no {marker:?} in it"
                );
            }
        }
    }
}

#[test]
fn a_malformed_argument_answers_the_same_one_document_whatever_the_group() {
    // The usage envelope is one promise, not one promise per command group: a script that branches
    // on `.error.kind` must not have to know which parser its verb happened to reach.
    let home = connected(NOWHERE);
    let cases: &[&[&str]] = &[
        &["doctor", "--nope"],
        &["runs", "list", "--nope"],
        &["cells", "list", "--nope"],
        &["queue", "list", "--nope"],
        &["operations", "list", "--nope"],
        &["fleet", "--nope"],
        &["compatibility", "--nope"],
    ];
    for case in cases {
        let output = swf(&home)
            .args(case.iter().copied())
            .arg("--json")
            .output()
            .expect("run");
        let named = case.join(" ");
        assert_eq!(
            output.status.code(),
            Some(2),
            "swf {named} should be a usage error: {output:?}"
        );
        let doc = document(&output.stdout);
        assert_eq!(doc["error"]["kind"], "usage", "swf {named}");
        assert_eq!(doc["error"]["exit_code"], 2, "swf {named}");
        assert_eq!(doc["error"]["hint"], "swf --help", "swf {named}");
        assert!(
            doc["error"]["message"]
                .as_str()
                .is_some_and(|m| !m.is_empty()),
            "swf {named} explained nothing"
        );
    }
}

#[test]
fn context_timeout_and_verbosity_answer_the_same_way_whatever_the_group() {
    // One command from the original tree and one from the group that used to have a parser of its
    // own, asked the same three questions. Any answer that differs is drift an operator pays for.
    let old: &[&str] = &["runs", "list", "--dag", "factory"];
    let new: &[&str] = &["queue", "list"];

    for case in [old, new] {
        let named = case.join(" ");
        let home = connected(NOWHERE);
        let output = swf(&home)
            .args(["--context", "prd"])
            .args(case.iter().copied())
            .arg("--json")
            .output()
            .expect("run");
        assert_eq!(output.status.code(), Some(3), "swf {named}: {output:?}");
        assert_eq!(document(&output.stdout)["error"]["kind"], "not_found");
    }

    for case in [old, new] {
        let named = case.join(" ");
        let home = backed(NOWHERE);
        let output = swf(&home)
            .args(case.iter().copied())
            .args(["--timeout", "0", "--json"])
            .env("SWF_BACKEND_TOKEN", TOKEN)
            .output()
            .expect("run");
        assert_eq!(output.status.code(), Some(2), "swf {named}: {output:?}");
        assert_eq!(document(&output.stdout)["error"]["kind"], "usage");
    }

    for case in [old, new] {
        let named = case.join(" ");
        let home = backed(NOWHERE);
        let output = swf(&home)
            .args(case.iter().copied())
            .arg("-v")
            .env("SWF_BACKEND_TOKEN", TOKEN)
            .output()
            .expect("run");
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        assert!(
            stderr.contains("context t ->"),
            "swf {named} -v never said which context it resolved:\n{stderr}"
        );
    }
}

#[test]
fn an_explicitly_direct_context_is_told_why_and_never_quietly_widened() {
    // `--direct` is a promise that this context uses its own Airflow credentials, local gh/islo
    // and local metrics, and is never widened beyond them (docs/factory-backend.md). A
    // backend-only group therefore has to refuse in words: answered with a gh or Airflow failure
    // an operator reads a factory problem, and answered with an empty document they believe they
    // went through the backend when they did not.
    let home = connected(NOWHERE);
    for case in BACKEND_ONLY {
        let named = case.join(" ");
        let output = swf(&home)
            .args(case.iter().copied())
            .arg("--json")
            .output()
            .expect("run");
        assert_eq!(output.status.code(), Some(1), "swf {named}: {output:?}");
        let doc = document(&output.stdout);
        assert_eq!(doc["error"]["kind"], "operational", "swf {named}");
        let message = doc["error"]["message"].as_str().unwrap_or_default();
        assert!(
            message.contains("\"t\""),
            "swf {named} did not name the context: {message}"
        );
        assert!(message.contains("direct"), "swf {named}: {message}");
        assert!(
            message.contains("local credentials"),
            "swf {named} never says it refused to widen: {message}"
        );
        let hint = doc["error"]["hint"].as_str().unwrap_or_default();
        assert!(hint.contains("--backend-url"), "swf {named}: {hint}");
        assert!(hint.contains("SWF_BACKEND_TOKEN"), "swf {named}: {hint}");
        assert!(
            output.stderr.windows(6).any(|w| w == b"error:"),
            "swf {named} said nothing on stderr"
        );
    }
}
