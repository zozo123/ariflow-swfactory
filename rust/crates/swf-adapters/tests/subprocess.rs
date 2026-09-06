//! The subprocess adapters as `swf-app` will actually hold them: behind `Arc<dyn _>`.
//!
//! The unit tests in `gh.rs` and `islo.rs` pin the argv and the refusals from inside the crate.
//! This file asks the different question the operations layer cares about — that these traits are
//! object-safe, that a fake runner can stand in for a real one from outside, and that the guard
//! that protects a teammate's sandbox still holds when the adapter is reached through a trait
//! object rather than a concrete type. No process is ever spawned.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use swf_adapters::gh::GhCli;
use swf_adapters::islo::IsloCli;
use swf_adapters::metrics_store::FsMetrics;
use swf_adapters::traits::{CommandOutput, CommandRunner, Deliveries, MetricsStore, Sandboxes};
use swf_adapters::Result;
use tokio_util::sync::CancellationToken;

/// A runner that answers each command from a table keyed on its second word (`ls`, `rm`, `list`,
/// `view`), so a test can script `gh` and `islo` without caring what order the adapter calls them.
#[derive(Default)]
struct Fake {
    calls: Mutex<Vec<Vec<String>>>,
    stdout: Mutex<Vec<(String, String)>>,
    code: i32,
}

impl Fake {
    fn answering(pairs: &[(&str, &str)]) -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            stdout: Mutex::new(
                pairs
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect(),
            ),
            code: 0,
        })
    }

    fn seen(&self) -> Vec<Vec<String>> {
        self.calls.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

#[async_trait]
impl CommandRunner for Fake {
    async fn run(
        &self,
        argv: &[String],
        _timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<CommandOutput> {
        if cancel.is_cancelled() {
            return Err(swf_adapters::AdapterError::Cancelled);
        }
        self.calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(argv.to_vec());
        let verb = argv.get(1).cloned().unwrap_or_default();
        let table = self.stdout.lock().unwrap_or_else(|e| e.into_inner());
        let stdout = table
            .iter()
            .find(|(k, _)| *k == verb)
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        Ok(CommandOutput {
            code: self.code,
            stdout,
            stderr: String::new(),
        })
    }
}

#[tokio::test]
async fn the_delivery_adapter_works_through_a_trait_object() {
    let runner = Fake::answering(&[
        (
            "pr",
            r#"[{"number": 7, "title": "42: add a thing", "url": "https://x/7",
                 "labels": [{"name": "factory"}], "state": "OPEN",
                 "headRefName": "factory/42-r1",
                 "statusCheckRollup": [{"conclusion": "SUCCESS"}]}]"#,
        ),
        (
            "issue",
            r#"[{"number": 42, "title": "the thing", "url": "https://x/i/42", "labels": []}]"#,
        ),
    ]);
    // This is the shape `Ops` holds. If the trait were not object-safe, this line would not build.
    let deliveries: Arc<dyn Deliveries> = Arc::new(GhCli::new("acme/widgets", runner.clone()));
    let cancel = CancellationToken::new();

    let prs = deliveries
        .prs("factory", 30, &cancel)
        .await
        .expect("one PR");
    assert_eq!(prs[0].number, 7);
    assert_eq!(prs[0].checks, "1 pass / 0 fail / 0 pending");

    let issues = deliveries
        .issues("factory", 30, &cancel)
        .await
        .expect("one issue");
    assert_eq!(issues[0].number, 42);

    assert_eq!(runner.seen().len(), 2);
    assert!(runner.seen().iter().all(|argv| argv[0] == "gh"));
}

#[tokio::test]
async fn a_pr_that_was_never_opened_is_none_and_not_a_not_found_error() {
    let runner = Fake::answering(&[("pr", "[]")]);
    let deliveries: Arc<dyn Deliveries> = Arc::new(GhCli::new("acme/widgets", runner));
    let found = deliveries
        .pr_for_branch("factory/42-r1", &CancellationToken::new())
        .await
        .expect("an answered question");
    assert!(
        found.is_none(),
        "a delivery can publish a branch and never open a PR; that is a state, not a failure"
    );
}

#[tokio::test]
async fn the_removal_guard_holds_through_a_trait_object_too() {
    const LISTING: &str = r#"[
        {"name": "swf-mine-1a2b3c4d", "status": "running", "created_by": "me@corp.com"},
        {"name": "swf-theirs-deadbeef", "status": "running", "created_by": "amy@corp.com"}
    ]"#;
    let runner = Fake::answering(&[("ls", LISTING)]);
    let sandboxes: Arc<dyn Sandboxes> = Arc::new(IsloCli::new("me@corp.com", runner.clone()));
    let cancel = CancellationToken::new();

    let mine = sandboxes.list(&cancel).await.expect("a listing");
    assert_eq!(mine.len(), 1);
    assert_eq!(mine[0].name, "swf-mine-1a2b3c4d");

    let refused = sandboxes
        .remove("swf-theirs-deadbeef", &cancel)
        .await
        .expect_err("not mine to remove");
    assert_eq!(
        refused.exit_code(),
        1,
        "a refusal is operational, not an auth failure"
    );

    let ran = sandboxes
        .remove("swf-mine-1a2b3c4d", &cancel)
        .await
        .expect("mine, factory-named, and still mine on a fresh listing");
    assert_eq!(
        ran,
        vec!["islo", "rm", "swf-mine-1a2b3c4d", "--output", "plain"]
    );

    // list, (refused: list), (allowed: list), rm — the guard re-reads before every removal.
    let verbs: Vec<String> = runner.seen().iter().map(|a| a[1].clone()).collect();
    assert_eq!(verbs, vec!["ls", "ls", "ls", "rm"]);
}

#[tokio::test]
async fn the_metrics_store_works_through_a_trait_object_and_answers_an_empty_checkout() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn MetricsStore> = Arc::new(FsMetrics::new(tmp.path()));
    let cancel = CancellationToken::new();

    assert!(store.runs(&cancel).await.expect("no runs").is_empty());
    let summary = store.summary(&cancel).await.expect("an empty aggregate");
    assert_eq!(summary.runs, 0);
    assert_eq!(
        summary.first_pass_rate, 0.0,
        "a rate over no runs is zero, never a division by zero"
    );
}
