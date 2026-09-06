//! GitHub, reached through the operator's own `gh` — never through a token this process holds.
//!
//! `swf` deliberately owns no GitHub credential. `gh` already has one, already knows about SSO and
//! enterprise hosts, and is already the thing the operator authenticated by hand; borrowing it
//! means there is no second secret to store, rotate or leak, and rule 6 (no secrets in the config
//! file) stays true for GitHub without any special case.
//!
//! The boundary this module defends is the shell. Every command is a `Vec<String>` handed to
//! `exec`, never a string handed to `sh`. Branch names, issue ids and labels all arrive from
//! outside — from a delivery report, from a PR title someone else wrote — and a `;` in one of them
//! has to be a nonsense argument rather than a second command. The argv is also the *contract*:
//! the `--json` field lists below are the ones `01-domain-control.md` §6 and
//! `06-delivery-evidence.md` §4.3 pin, and the tests assert them literally, because a silently
//! dropped field would show up as an empty column rather than as an error.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use swf_domain::model::{IssueRef, PullRequest};
use swf_domain::rollup::summarize_checks;
use swf_domain::sanitize::sanitize_line;
use tokio_util::sync::CancellationToken;

use crate::error::{AdapterError, Result};
use crate::traits::{CommandRunner, Deliveries, PrHead, SUBPROCESS_TIMEOUT};

/// The label the factory puts on everything it opens.
pub const DEFAULT_LABEL: &str = "factory";

/// How many pull requests or issues one listing asks for. The herd table shows far fewer; the
/// margin is there so a busy repo does not hide the row an operator is looking for.
pub const DEFAULT_LIST_LIMIT: u32 = 30;

/// The fields `gh pr list` must return for the herd table. Order is part of the contract only in
/// the sense that the tests pin it — `gh` itself does not care — but a *missing* field silently
/// becomes an empty column, so the list is written once, here.
pub const PR_FIELDS: &str = "number,title,url,labels,state,headRefName,statusCheckRollup";

/// The fields `gh issue list` must return.
pub const ISSUE_FIELDS: &str = "number,title,url,labels";

/// The fields the delivery verifier needs about the PR on one branch
/// (`06-delivery-evidence.md` §4.3, check `pr.exists`).
pub const PR_HEAD_FIELDS: &str = "url,state,title,labels,headRefOid,baseRefName";

/// GitHub as seen through `gh`.
pub struct GhCli {
    repo: String,
    runner: Arc<dyn CommandRunner>,
}

impl GhCli {
    /// Build a client for one `owner/name` repository.
    pub fn new(repo: impl Into<String>, runner: Arc<dyn CommandRunner>) -> Self {
        Self {
            repo: repo.into(),
            runner,
        }
    }

    /// The repository every command is scoped to.
    pub fn repo(&self) -> &str {
        &self.repo
    }

    /// `gh pr list` for one label — the argv, built so a test can read it.
    pub fn prs_argv(&self, label: &str, limit: u32) -> Vec<String> {
        argv([
            "gh",
            "pr",
            "list",
            "--repo",
            &self.repo,
            "--label",
            label,
            "--state",
            "all",
            "--limit",
            &limit.to_string(),
            "--json",
            PR_FIELDS,
        ])
    }

    /// `gh issue list` for one label.
    pub fn issues_argv(&self, label: &str, limit: u32) -> Vec<String> {
        argv([
            "gh",
            "issue",
            "list",
            "--repo",
            &self.repo,
            "--label",
            label,
            "--limit",
            &limit.to_string(),
            "--json",
            ISSUE_FIELDS,
        ])
    }

    /// `gh pr list --head` — the one PR whose head is this branch, if any.
    pub fn pr_for_branch_argv(&self, branch: &str) -> Vec<String> {
        argv([
            "gh",
            "pr",
            "list",
            "--repo",
            &self.repo,
            "--head",
            branch,
            "--state",
            "all",
            "--json",
            PR_HEAD_FIELDS,
            "--limit",
            "1",
        ])
    }

    /// `gh pr view --json statusCheckRollup` for one PR.
    pub fn checks_argv(&self, number: i64) -> Vec<String> {
        argv([
            "gh",
            "pr",
            "view",
            &number.to_string(),
            "--repo",
            &self.repo,
            "--json",
            "statusCheckRollup",
        ])
    }

    /// `gh pr view --web` — hand the PR to the operator's browser.
    pub fn pr_view_argv(&self, number: i64) -> Vec<String> {
        argv([
            "gh",
            "pr",
            "view",
            &number.to_string(),
            "--repo",
            &self.repo,
            "--web",
        ])
    }

    /// Run one `gh` command and return its stdout, or classify why it failed.
    async fn run(&self, argv: Vec<String>, cancel: &CancellationToken) -> Result<String> {
        let output = self
            .runner
            .run(&argv, SUBPROCESS_TIMEOUT, cancel)
            .await
            .map_err(|err| annotate_missing_tool(err, &argv))?;
        if output.code != 0 {
            // The Python's exact phrasing, because this text reaches `Snapshot.errors`.
            return Err(AdapterError::refused(format!(
                "{} failed rc={}: {}",
                argv.join(" "),
                output.code,
                output.message()
            )));
        }
        Ok(output.stdout)
    }

    /// Parse `gh`'s stdout as a JSON array. An empty or `null` body is an empty list, not an
    /// error: a repo with no factory PRs is a normal repo.
    fn rows(text: &str, what: &str) -> Result<Vec<Value>> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }
        match serde_json::from_str::<Value>(trimmed) {
            Ok(Value::Array(rows)) => Ok(rows),
            Ok(Value::Null) => Ok(Vec::new()),
            Ok(_) => Ok(Vec::new()),
            Err(e) => Err(AdapterError::Decode {
                what: what.to_string(),
                detail: crate::error::truncate(&e.to_string()),
            }),
        }
    }
}

/// Build an argv without repeating `.to_string()` at every element.
fn argv<'a>(parts: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    parts.into_iter().map(str::to_string).collect()
}

/// A tool that is not installed is *unreachable*, not a failure of the request.
///
/// The distinction is the difference between `swf doctor` saying "install gh" (exit 5, fixable)
/// and saying "the PR list is broken" (exit 1, mysterious).
fn annotate_missing_tool(err: AdapterError, argv: &[String]) -> AdapterError {
    match err {
        AdapterError::Unreachable { detail, .. } => AdapterError::Unreachable {
            what: argv.first().cloned().unwrap_or_default(),
            detail,
        },
        other => other,
    }
}

/// `gh` reports labels as objects; the herd table wants names.
///
/// A non-list is `[]` and a bare string passes through — `gh` has spelled this both ways and one
/// odd shape must not cost the whole listing.
fn label_names(labels: Option<&Value>) -> Vec<String> {
    match labels {
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| match item {
                Value::Object(map) => map
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// A field `gh` promises is a string, read defensively and scrubbed.
///
/// PR and issue titles are written by whoever opened them. They reach a terminal, so they go
/// through [`sanitize_line`] here rather than at each of the four places that render them.
fn text(row: &Value, key: &str) -> String {
    sanitize_line(row.get(key).and_then(Value::as_str).unwrap_or_default())
}

#[async_trait]
impl Deliveries for GhCli {
    async fn prs(
        &self,
        label: &str,
        limit: u32,
        cancel: &CancellationToken,
    ) -> Result<Vec<PullRequest>> {
        let out = self.run(self.prs_argv(label, limit), cancel).await?;
        let rows = Self::rows(&out, "gh pr list")?;
        Ok(rows
            .iter()
            .filter_map(|row| {
                // A row without a number cannot be addressed, so it is dropped rather than
                // rendered as PR #0 — a row an operator cannot act on is worse than no row.
                let number = row.get("number").and_then(Value::as_i64)?;
                Some(PullRequest {
                    number,
                    title: text(row, "title"),
                    url: text(row, "url"),
                    labels: label_names(row.get("labels")),
                    state: text(row, "state"),
                    checks: summarize_checks(row.get("statusCheckRollup")),
                    head: text(row, "headRefName"),
                })
            })
            .collect())
    }

    async fn issues(
        &self,
        label: &str,
        limit: u32,
        cancel: &CancellationToken,
    ) -> Result<Vec<IssueRef>> {
        let out = self.run(self.issues_argv(label, limit), cancel).await?;
        let rows = Self::rows(&out, "gh issue list")?;
        Ok(rows
            .iter()
            .filter_map(|row| {
                let number = row.get("number").and_then(Value::as_i64)?;
                Some(IssueRef {
                    number,
                    title: text(row, "title"),
                    url: text(row, "url"),
                    labels: label_names(row.get("labels")),
                })
            })
            .collect())
    }

    async fn pr_for_branch(
        &self,
        branch: &str,
        cancel: &CancellationToken,
    ) -> Result<Option<PrHead>> {
        let out = self.run(self.pr_for_branch_argv(branch), cancel).await?;
        let rows = Self::rows(&out, "gh pr list --head")?;
        // No PR is a real answer: a delivery can publish a branch and never open one. Saying
        // "not found" here would make an honest state look like a broken query.
        Ok(rows.first().map(|row| PrHead {
            url: text(row, "url"),
            state: text(row, "state"),
            title: text(row, "title"),
            labels: label_names(row.get("labels")),
            head_sha: text(row, "headRefOid"),
            base_ref: text(row, "baseRefName"),
        }))
    }

    async fn checks(&self, number: i64, cancel: &CancellationToken) -> Result<String> {
        let out = self.run(self.checks_argv(number), cancel).await?;
        let trimmed = out.trim();
        if trimmed.is_empty() {
            return Ok(summarize_checks(None));
        }
        let body: Value = serde_json::from_str(trimmed).map_err(|e| AdapterError::Decode {
            what: format!("gh pr view {number}"),
            detail: crate::error::truncate(&e.to_string()),
        })?;
        Ok(summarize_checks(body.get("statusCheckRollup")))
    }

    async fn pr_view(&self, number: i64, cancel: &CancellationToken) -> Result<Vec<String>> {
        let argv = self.pr_view_argv(number);
        self.run(argv.clone(), cancel).await?;
        // Answer the argv rather than "opened": whether a browser actually appeared is not
        // something this process can observe, and claiming it would be a small lie.
        Ok(argv)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;

    use crate::traits::CommandOutput;

    /// A runner that answers from a script and records every argv it was given.
    #[derive(Default)]
    struct FakeRunner {
        calls: Mutex<Vec<Vec<String>>>,
        reply: Mutex<Vec<Result<CommandOutput>>>,
    }

    impl FakeRunner {
        fn ok(stdout: &str) -> Arc<Self> {
            let runner = Self::default();
            runner
                .reply
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(Ok(CommandOutput {
                    code: 0,
                    stdout: stdout.to_string(),
                    stderr: String::new(),
                }));
            Arc::new(runner)
        }

        fn failing(code: i32, stderr: &str) -> Arc<Self> {
            let runner = Self::default();
            runner
                .reply
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(Ok(CommandOutput {
                    code,
                    stdout: String::new(),
                    stderr: stderr.to_string(),
                }));
            Arc::new(runner)
        }

        fn seen(&self) -> Vec<Vec<String>> {
            self.calls.lock().unwrap_or_else(|e| e.into_inner()).clone()
        }
    }

    #[async_trait]
    impl CommandRunner for FakeRunner {
        async fn run(
            &self,
            argv: &[String],
            _timeout: Duration,
            cancel: &CancellationToken,
        ) -> Result<CommandOutput> {
            if cancel.is_cancelled() {
                return Err(AdapterError::Cancelled);
            }
            self.calls
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(argv.to_vec());
            let mut replies = self.reply.lock().unwrap_or_else(|e| e.into_inner());
            if replies.is_empty() {
                return Ok(CommandOutput::default());
            }
            replies.remove(0)
        }
    }

    fn gh(runner: Arc<FakeRunner>) -> GhCli {
        GhCli::new("acme/widgets", runner)
    }

    #[tokio::test]
    async fn pr_list_argv_is_the_one_the_spec_pins() {
        let runner = FakeRunner::ok("[]");
        let client = gh(runner.clone());
        let prs = client
            .prs(DEFAULT_LABEL, DEFAULT_LIST_LIMIT, &CancellationToken::new())
            .await
            .expect("an empty repo is not an error");
        assert!(prs.is_empty());
        assert_eq!(
            runner.seen()[0],
            vec![
                "gh",
                "pr",
                "list",
                "--repo",
                "acme/widgets",
                "--label",
                "factory",
                "--state",
                "all",
                "--limit",
                "30",
                "--json",
                "number,title,url,labels,state,headRefName,statusCheckRollup",
            ]
        );
    }

    #[tokio::test]
    async fn issue_list_argv_is_the_one_the_spec_pins() {
        let runner = FakeRunner::ok("[]");
        gh(runner.clone())
            .issues("factory", 30, &CancellationToken::new())
            .await
            .expect("an empty repo is not an error");
        assert_eq!(
            runner.seen()[0],
            vec![
                "gh",
                "issue",
                "list",
                "--repo",
                "acme/widgets",
                "--label",
                "factory",
                "--limit",
                "30",
                "--json",
                "number,title,url,labels",
            ]
        );
    }

    #[tokio::test]
    async fn pr_view_web_answers_the_argv_it_ran_rather_than_claiming_success() {
        let runner = FakeRunner::ok("");
        let ran = gh(runner.clone())
            .pr_view(1234, &CancellationToken::new())
            .await
            .expect("gh exits zero");
        assert_eq!(
            ran,
            vec![
                "gh",
                "pr",
                "view",
                "1234",
                "--repo",
                "acme/widgets",
                "--web"
            ]
        );
        assert_eq!(runner.seen()[0], ran);
    }

    #[tokio::test]
    async fn a_branch_name_is_an_argument_and_never_a_command() {
        let runner = FakeRunner::ok("[]");
        let hostile = "factory/42-r1; rm -rf /";
        gh(runner.clone())
            .pr_for_branch(hostile, &CancellationToken::new())
            .await
            .expect("no PR is a real answer");
        let seen = &runner.seen()[0];
        assert!(
            seen.contains(&hostile.to_string()),
            "passed verbatim as one argv element"
        );
        assert_eq!(seen.iter().filter(|a| a.contains("rm -rf")).count(), 1);
    }

    #[tokio::test]
    async fn a_pull_request_row_is_flattened_and_scrubbed() {
        let runner = FakeRunner::ok(
            r#"[{"number": 7, "title": "fix\u001b[2J thing", "url": "https://x/7",
                 "labels": [{"name": "factory"}, {"name": "agent-authored"}],
                 "state": "OPEN", "headRefName": "factory/42-r1",
                 "statusCheckRollup": [{"conclusion": "SUCCESS"}, {"status": "IN_PROGRESS"}]}]"#,
        );
        let prs = gh(runner)
            .prs("factory", 30, &CancellationToken::new())
            .await
            .expect("one PR");
        assert_eq!(prs.len(), 1);
        assert_eq!(prs[0].number, 7);
        assert_eq!(prs[0].title, "fix thing", "a PR title reaches a terminal");
        assert_eq!(prs[0].labels, vec!["factory", "agent-authored"]);
        assert_eq!(prs[0].head, "factory/42-r1");
        assert_eq!(prs[0].checks, "1 pass / 0 fail / 1 pending");
    }

    #[tokio::test]
    async fn a_row_without_a_number_is_dropped_rather_than_rendered_as_pr_zero() {
        let runner = FakeRunner::ok(r#"[{"title": "no number"}, {"number": 3, "title": "ok"}]"#);
        let prs = gh(runner)
            .prs("factory", 30, &CancellationToken::new())
            .await
            .expect("the good row survives");
        assert_eq!(prs.len(), 1);
        assert_eq!(prs[0].number, 3);
    }

    #[tokio::test]
    async fn a_non_zero_exit_carries_the_trimmed_stderr() {
        let runner = FakeRunner::failing(1, "  gh: could not resolve to a Repository.  \n");
        let err = gh(runner)
            .prs("factory", 30, &CancellationToken::new())
            .await
            .expect_err("a broken repo is an error");
        assert_eq!(err.exit_code(), 1);
        let text = err.to_string();
        assert!(text.contains("rc=1"), "{text}");
        assert!(
            text.contains("could not resolve to a Repository."),
            "{text}"
        );
        assert!(!text.contains("  gh:"), "stderr is trimmed: {text}");
    }

    #[tokio::test]
    async fn an_empty_stdout_is_an_empty_list_and_bad_json_is_an_error() {
        let empty = gh(FakeRunner::ok("   \n"))
            .issues("factory", 30, &CancellationToken::new())
            .await
            .expect("silence means nothing matched");
        assert!(empty.is_empty());

        let err = gh(FakeRunner::ok("{not json"))
            .issues("factory", 30, &CancellationToken::new())
            .await
            .expect_err("garbage is not silence");
        assert_eq!(err.kind(), "operational");
    }

    #[tokio::test]
    async fn labels_survive_every_shape_gh_has_used_for_them() {
        assert_eq!(label_names(None), Vec::<String>::new());
        assert_eq!(
            label_names(Some(&serde_json::json!("not a list"))),
            Vec::<String>::new()
        );
        assert_eq!(
            label_names(Some(
                &serde_json::json!([{"name": "a"}, "b", {"colour": "red"}])
            )),
            vec!["a", "b", ""]
        );
    }

    #[tokio::test]
    async fn checks_reads_the_rollup_out_of_a_single_pr_view() {
        let runner = FakeRunner::ok(
            r#"{"statusCheckRollup": [{"conclusion": "FAILURE"}, {"conclusion": "SUCCESS"}]}"#,
        );
        let summary = gh(runner)
            .checks(7, &CancellationToken::new())
            .await
            .expect("a rollup");
        assert_eq!(summary, "1 pass / 1 fail / 0 pending");
    }

    #[tokio::test]
    async fn a_cancelled_caller_gets_no_subprocess_answer() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let err = gh(FakeRunner::ok("[]"))
            .prs("factory", 30, &cancel)
            .await
            .expect_err("cancelled");
        assert!(err.is_cancelled());
    }
}
