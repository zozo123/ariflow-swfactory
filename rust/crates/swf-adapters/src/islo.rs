//! Sandboxes through `islo`, and the three guards that stand between `swf` and a teammate's work.
//!
//! Listing is the easy half: `islo ls --output json`, **never** `--all`. The `--all` flag widens
//! the listing to every sandbox on the account, and a tool that can *see* a colleague's sandbox is
//! one refactor away from removing it. Staying inside the caller's own scope means the dangerous
//! case never appears in the data in the first place.
//!
//! Removal is the half that has to be right. `swf sandboxes rm` refuses unless **all three** hold,
//! checked in this order (`01-domain-control.md` §6):
//!
//! 1. an owner is configured — with nobody to compare against, nothing is safe to delete;
//! 2. the name matches `^swf-[a-z0-9][a-z0-9_-]*-[0-9a-f]{8}$` — someone's `prod-db` must survive
//!    a fat-fingered argument;
//! 3. a **fresh** listing, fetched in this call, still shows the sandbox as created by that owner.
//!
//! The third guard re-lists on purpose. The snapshot the operator was looking at may be minutes
//! old, and "it was mine when the screen was drawn" is not a fact about now. All three refusals
//! are operational (exit 1), not authentication failures: `swf` decided, and no credential would
//! change the answer.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use swf_domain::model::{is_factory_name, SandboxRef};
use tokio_util::sync::CancellationToken;

use crate::error::{AdapterError, Result};
use crate::traits::{first_timestamp, CommandRunner, Sandboxes, CREATED_KEYS, SUBPROCESS_TIMEOUT};

/// The status `islo` reports for a sandbox that is already gone.
pub const DELETED_STATUS: &str = "deleted";

/// The environment variable the Python reads the owner from, kept so an operator's existing shell
/// still works.
pub const OWNER_ENV: &str = "SWF_SANDBOX_OWNER";

/// Sandboxes as seen through `islo`.
pub struct IsloCli {
    owner: String,
    runner: Arc<dyn CommandRunner>,
}

impl IsloCli {
    /// Build a client for one owner. A blank owner is allowed and disables removal entirely.
    pub fn new(owner: impl Into<String>, runner: Arc<dyn CommandRunner>) -> Self {
        Self {
            owner: owner.into().trim().to_string(),
            runner,
        }
    }

    /// The configured owner, already trimmed.
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// `islo ls --output json` — the listing argv. Never `--all`: see the module docs.
    pub fn list_argv(&self) -> Vec<String> {
        ["islo", "ls", "--output", "json"]
            .into_iter()
            .map(str::to_string)
            .collect()
    }

    /// `islo rm <name> --output plain` — only ever reached past all three guards.
    pub fn remove_argv(&self, name: &str) -> Vec<String> {
        vec![
            "islo".to_string(),
            "rm".to_string(),
            name.to_string(),
            "--output".to_string(),
            "plain".to_string(),
        ]
    }

    /// Fetch the provider's listing as raw text.
    async fn listing(&self, cancel: &CancellationToken) -> Result<String> {
        let argv = self.list_argv();
        let output = self
            .runner
            .run(&argv, SUBPROCESS_TIMEOUT, cancel)
            .await
            .map_err(|err| match err {
                AdapterError::Unreachable { detail, .. } => AdapterError::Unreachable {
                    what: "islo".to_string(),
                    detail,
                },
                other => other,
            })?;
        if output.code != 0 {
            return Err(AdapterError::refused(format!(
                "{} failed rc={}: {}",
                argv.join(" "),
                output.code,
                output.message()
            )));
        }
        Ok(output.stdout)
    }
}

/// Find the list of sandboxes inside whatever `islo ls` wrapped it in.
///
/// A bare array is used as-is; an object yields its **first** value that is a list, because the
/// CLI has wrapped the array under a key more than once and the key has not always been the same.
/// Anything else is an empty listing — which, for the removal guard, means "refuse".
fn rows(listing: &str) -> Vec<Value> {
    let Ok(data) = serde_json::from_str::<Value>(listing.trim()) else {
        return Vec::new();
    };
    match data {
        Value::Array(items) => items,
        Value::Object(map) => map
            .values()
            .find_map(|v| v.as_array().cloned())
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

/// The provider's `created_by`, normalised the way both sides of the comparison must be.
///
/// `.strip().lower()` on both sides, exactly as the Python: an owner configured as `Me@Corp.com `
/// and a provider reporting `me@corp.com` are the same person, and treating them as different
/// would refuse a legitimate removal — which trains the operator to reach for `--force`.
fn creator(item: &Value) -> String {
    item.get("created_by")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_lowercase()
}

/// The provider's status, lowercased for the `deleted` comparison.
fn status(item: &Value) -> String {
    item.get("status")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// The sandboxes `owner` created and that still exist.
///
/// A blank owner yields nothing at all rather than everything — the failure mode of the opposite
/// choice is a list of other people's sandboxes with a delete key next to each one.
pub fn owned_sandboxes(listing: &str, owner: &str) -> Vec<SandboxRef> {
    let owner = owner.trim().to_lowercase();
    if owner.is_empty() {
        return Vec::new();
    }
    rows(listing)
        .iter()
        .filter(|item| item.is_object())
        .filter(|item| status(item) != DELETED_STATUS)
        .filter(|item| creator(item) == owner)
        .map(|item| {
            let mut sandbox = SandboxRef::new(
                item.get("name").and_then(Value::as_str).unwrap_or_default(),
                status(item),
                creator(item),
                None,
            );
            // Assigned rather than passed, so a provider that reports a local offset keeps it: the
            // snapshot prints the stamp it was given, not this machine's idea of the same instant.
            sandbox.created_at = first_timestamp(item, CREATED_KEYS);
            sandbox
        })
        .collect()
}

/// Whether `name` is present in this listing, not deleted, and created by `owner`.
///
/// Only the **first** entry with that name is consulted, and a name that is absent answers `false`.
/// Both are deliberate: absence is not permission, and a provider that returns a name twice has a
/// problem `swf` must not resolve by picking the convenient row.
pub fn owns_sandbox(listing: &str, name: &str, owner: &str) -> bool {
    let Some(item) = rows(listing)
        .into_iter()
        .find(|item| item.get("name").and_then(Value::as_str) == Some(name))
    else {
        return false;
    };
    if status(&item) == DELETED_STATUS {
        return false;
    }
    let owner = owner.trim().to_lowercase();
    owner.is_empty() || creator(&item) == owner
}

#[async_trait]
impl Sandboxes for IsloCli {
    async fn list(&self, cancel: &CancellationToken) -> Result<Vec<SandboxRef>> {
        if self.owner.is_empty() {
            return Ok(Vec::new());
        }
        let listing = self.listing(cancel).await?;
        Ok(owned_sandboxes(&listing, &self.owner))
    }

    async fn remove(&self, name: &str, cancel: &CancellationToken) -> Result<Vec<String>> {
        // Guard 1: with nobody to compare against, nothing is safe to delete.
        if self.owner.is_empty() {
            return Err(AdapterError::refused(
                "no sandbox owner configured; refusing to remove anything",
            ));
        }
        // Guard 2: the name policy. Someone's `prod-db` is not a factory sandbox.
        if !is_factory_name(name) {
            return Err(AdapterError::refused(format!(
                "{name:?} is not a factory-named sandbox (swf-<slug>-<run8>)"
            )));
        }
        // Guard 3: re-read the provider *now*. The screen the operator acted from may be minutes
        // old, and ownership is a fact about the present.
        let listing = self.listing(cancel).await?;
        if !owns_sandbox(&listing, name, &self.owner) {
            return Err(AdapterError::refused(format!(
                "{name:?} was not created by {:?}; refusing",
                self.owner
            )));
        }

        let argv = self.remove_argv(name);
        let output = self.runner.run(&argv, SUBPROCESS_TIMEOUT, cancel).await?;
        if output.code != 0 {
            return Err(AdapterError::refused(format!(
                "{} failed rc={}: {}",
                argv.join(" "),
                output.code,
                output.message()
            )));
        }
        Ok(argv)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;

    use crate::traits::CommandOutput;

    /// A runner that replays a script and remembers every argv, so a refusal can be proven to have
    /// run nothing rather than merely to have returned an error.
    #[derive(Default)]
    struct FakeRunner {
        calls: Mutex<Vec<Vec<String>>>,
        replies: Mutex<Vec<CommandOutput>>,
    }

    impl FakeRunner {
        fn new(replies: Vec<CommandOutput>) -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
                replies: Mutex::new(replies),
            })
        }

        fn listing(json: &str) -> Arc<Self> {
            Self::new(vec![
                CommandOutput {
                    code: 0,
                    stdout: json.to_string(),
                    stderr: String::new(),
                },
                CommandOutput::default(),
            ])
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
            let mut replies = self.replies.lock().unwrap_or_else(|e| e.into_inner());
            if replies.is_empty() {
                return Ok(CommandOutput::default());
            }
            Ok(replies.remove(0))
        }
    }

    const MINE: &str = r#"[
        {"name": "swf-demo-1a2b3c4d", "status": "running", "created_by": "Me@Corp.com",
         "created_at": "2026-09-03T08:00:00Z"},
        {"name": "swf-old-00112233", "status": "deleted", "created_by": "me@corp.com"},
        {"name": "prod-db", "status": "running", "created_by": "me@corp.com"},
        {"name": "swf-theirs-deadbeef", "status": "running", "created_by": "amy@corp.com"}
    ]"#;

    fn islo(runner: Arc<FakeRunner>) -> IsloCli {
        IsloCli::new("  me@corp.com ", runner)
    }

    #[tokio::test]
    async fn the_listing_is_never_widened_to_everyone() {
        let runner = FakeRunner::listing(MINE);
        islo(runner.clone())
            .list(&CancellationToken::new())
            .await
            .expect("a listing");
        assert_eq!(runner.seen()[0], vec!["islo", "ls", "--output", "json"]);
        assert!(
            !runner.seen()[0].iter().any(|a| a == "--all"),
            "--all makes a colleague's sandbox visible, and visible is one refactor from deletable"
        );
    }

    #[tokio::test]
    async fn listing_keeps_only_live_sandboxes_this_owner_created() {
        let boxes = islo(FakeRunner::listing(MINE))
            .list(&CancellationToken::new())
            .await
            .expect("a listing");
        let names: Vec<&str> = boxes.iter().map(|b| b.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["swf-demo-1a2b3c4d", "prod-db"],
            "ownership, not the name policy, decides what is listed"
        );
        assert!(boxes[0].created_at.is_some());
        assert_eq!(
            boxes[0].created_by, "me@corp.com",
            "both sides are normalised"
        );
    }

    #[tokio::test]
    async fn an_unconfigured_owner_lists_nothing_and_removes_nothing() {
        let runner = FakeRunner::listing(MINE);
        let client = IsloCli::new("   ", runner.clone());
        assert!(client
            .list(&CancellationToken::new())
            .await
            .expect("no owner, no sandboxes")
            .is_empty());
        let err = client
            .remove("swf-demo-1a2b3c4d", &CancellationToken::new())
            .await
            .expect_err("refused");
        assert_eq!(
            err.exit_code(),
            1,
            "a refusal is operational, not an auth failure"
        );
        assert!(err.to_string().contains("no sandbox owner configured"));
        assert!(
            runner.seen().is_empty(),
            "a refusal must not spawn anything"
        );
    }

    #[tokio::test]
    async fn a_sandbox_that_is_not_factory_named_is_refused_without_a_listing() {
        let runner = FakeRunner::listing(MINE);
        let err = islo(runner.clone())
            .remove("prod-db", &CancellationToken::new())
            .await
            .expect_err("refused");
        assert!(err.to_string().contains("is not a factory-named sandbox"));
        assert!(
            runner.seen().is_empty(),
            "the name check comes first, so a typo costs no round trip"
        );
    }

    #[tokio::test]
    async fn a_teammates_sandbox_is_refused_even_though_it_is_factory_named() {
        let runner = FakeRunner::listing(MINE);
        let err = islo(runner.clone())
            .remove("swf-theirs-deadbeef", &CancellationToken::new())
            .await
            .expect_err("refused");
        assert!(err.to_string().contains("was not created by"));
        assert_eq!(runner.seen().len(), 1, "it listed, then stopped");
        assert_eq!(runner.seen()[0][1], "ls");
    }

    #[tokio::test]
    async fn removal_re_lists_immediately_before_acting() {
        let runner = FakeRunner::listing(MINE);
        let argv = islo(runner.clone())
            .remove("swf-demo-1a2b3c4d", &CancellationToken::new())
            .await
            .expect("owned and factory-named");
        assert_eq!(
            argv,
            vec!["islo", "rm", "swf-demo-1a2b3c4d", "--output", "plain"]
        );
        let seen = runner.seen();
        assert_eq!(seen.len(), 2);
        assert_eq!(
            seen[0][1], "ls",
            "the decision is made on data fetched in this call"
        );
        assert_eq!(seen[1], argv);
    }

    #[tokio::test]
    async fn a_sandbox_deleted_between_the_screen_and_the_key_press_is_refused() {
        let listing = r#"[{"name": "swf-demo-1a2b3c4d", "status": "deleted",
                           "created_by": "me@corp.com"}]"#;
        let err = islo(FakeRunner::listing(listing))
            .remove("swf-demo-1a2b3c4d", &CancellationToken::new())
            .await
            .expect_err("already gone");
        assert!(err.to_string().contains("was not created by"));
    }

    #[tokio::test]
    async fn an_unreadable_listing_refuses_rather_than_assuming_ownership() {
        for listing in ["not json", "null", "42", "[]", r#"{"a": 1}"#] {
            let err = islo(FakeRunner::listing(listing))
                .remove("swf-demo-1a2b3c4d", &CancellationToken::new())
                .await
                .expect_err("silence is not permission");
            assert!(err.to_string().contains("was not created by"), "{listing}");
        }
    }

    #[test]
    fn a_wrapped_listing_is_unwrapped_by_shape_and_not_by_key_name() {
        let wrapped = r#"{"count": 1, "sandboxes": [{"name": "swf-a-0000abcd",
                          "status": "running", "created_by": "me"}]}"#;
        assert!(owns_sandbox(wrapped, "swf-a-0000abcd", "me"));
        assert!(!owns_sandbox(wrapped, "swf-a-0000abcd", "amy"));
        assert!(!owns_sandbox(wrapped, "missing", "me"));
    }

    #[test]
    fn only_the_first_row_with_a_given_name_is_consulted() {
        let duplicated = r#"[{"name": "swf-a-0000abcd", "status": "deleted", "created_by": "me"},
                             {"name": "swf-a-0000abcd", "status": "running", "created_by": "me"}]"#;
        assert!(
            !owns_sandbox(duplicated, "swf-a-0000abcd", "me"),
            "a provider returning a name twice is a problem swf must not resolve conveniently"
        );
    }

    #[tokio::test]
    async fn a_failing_islo_rm_reports_its_stderr() {
        let runner = FakeRunner::new(vec![
            CommandOutput {
                code: 0,
                stdout: MINE.to_string(),
                stderr: String::new(),
            },
            CommandOutput {
                code: 2,
                stdout: String::new(),
                stderr: "sandbox is locked".to_string(),
            },
        ]);
        let err = islo(runner)
            .remove("swf-demo-1a2b3c4d", &CancellationToken::new())
            .await
            .expect_err("islo said no");
        assert!(err.to_string().contains("rc=2"));
        assert!(err.to_string().contains("sandbox is locked"));
    }
}
