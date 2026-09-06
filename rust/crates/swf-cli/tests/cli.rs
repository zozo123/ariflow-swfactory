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
    cmd
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
