//! What the factory published, and what "verified" is allowed to mean.
//!
//! Three different claims get called "it worked": the workflow *reported* success, a branch really
//! was *published*, and an independent process *re-derived* the facts from that branch. They have
//! different forgers — the run itself, the orchestrator credential, and nobody — so this module
//! keeps them apart end to end and hands [`DeliveryReport::resolve`] the raw findings rather than
//! a conclusion. Non-negotiable 10 exists because collapsing them into one green tick is how a
//! self-reported `tests_passed` ends up on a dashboard as proof.
//!
//! The boundary this module defends is the difference between *not applicable* and *could not
//! tell*. A check that does not apply is `Skipped` and proves nothing either way; a check that
//! applies and could not be run is `Unavailable`, and a laptop with no credentials must never be
//! able to verify anything at all by simply asking no questions. That distinction is why
//! `--clone` skips the re-run (`PublishedOnly`, an honest ceiling) while a *failed* clone is
//! unavailable (`Inconclusive`, an admission).
//!
//! With `--clone`, the target's own test command is re-run from a fresh checkout of the published
//! branch, read out of that checkout's `factory.toml`. Never from anywhere else: the point of the
//! exercise is that the branch describes how to test itself, and reading the command from the
//! operator's working copy — or guessing it — would verify a different thing than the one being
//! shipped.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::Serialize;
use swf_adapters::metrics_store::FsMetrics;
use swf_adapters::traits::{CommandRunner, Deliveries, MetricsStore, PrHead};
use swf_domain::blueprint::validate_git_ref;
use swf_domain::evidence::{DeliveryRef, DeliveryReport, Evidence, EvidenceLevel, TestEvidence};
use swf_domain::ids::{DeliveryId, DELIVERY_BRANCH_PREFIX};
use swf_domain::metrics::RunMetrics;
use swf_domain::model::PullRequest;
use swf_domain::sanitize::sanitize_line;
use tokio_util::sync::CancellationToken;

use crate::attention::{BLOCKED_BANNERS, BLOCKED_LABELS};
use crate::context::Context;
use crate::ops::{OpsError, Result};

/// The labels every factory delivery carries (`stages.DEFAULT_LABELS`).
pub const REQUIRED_LABELS: &[&str] = &["factory", "agent-authored"];

/// Where a target's contract lives, relative to the target directory.
pub const CONTRACT_FILE: &str = "factory.toml";

/// Where the committed artifact chain lives, relative to the target directory.
pub const ARTIFACT_DIR: &str = "docs/factory";

/// The default JUnit path, and the prefix every JUnit path must start with.
pub const DEFAULT_JUNIT: &str = ".factory/junit.xml";

/// The prefix `[paths].junit` must live under.
pub const JUNIT_PREFIX: &str = ".factory/";

/// How deep to look for a `factory.toml` when the target directory was not named.
pub const CONTRACT_SEARCH_DEPTH: usize = 3;

/// How long a re-run test suite may take before it is a hang rather than a slow suite.
pub const TEST_TIMEOUT: Duration = Duration::from_secs(1800);

/// How much history a verification clone needs.
pub const CLONE_DEPTH: &str = "50";

/// One published delivery, as the forge knows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Delivery {
    /// The identity `swf deliveries verify` takes.
    pub id: String,
    /// `factory/<issue_id>-<run_id>`.
    pub branch: String,
    /// The issue this delivery answers, when the branch name yields one.
    pub issue_id: String,
    /// The run that produced it.
    pub run_id: String,
    /// The pull request number.
    pub number: i64,
    /// The web URL.
    pub url: String,
    /// The title, sanitised.
    pub title: String,
    /// `OPEN` / `MERGED` / `CLOSED`.
    pub state: String,
    /// The labels, verbatim.
    pub labels: Vec<String>,
    /// The check roll-up, in `summarize_checks` form.
    pub checks: String,
    /// True when the factory stopped this delivery on purpose.
    pub blocked: bool,
}

/// Read one pull request as a delivery.
pub fn from_pr(pr: &PullRequest) -> Delivery {
    let (issue_id, run_id) = split_branch(&pr.head);
    Delivery {
        id: if pr.head.starts_with(DELIVERY_BRANCH_PREFIX) {
            pr.head.clone()
        } else {
            format!("#{}", pr.number)
        },
        branch: pr.head.clone(),
        issue_id,
        run_id,
        number: pr.number,
        url: pr.url.clone(),
        title: sanitize_line(&pr.title),
        state: pr.state.clone(),
        labels: pr.labels.clone(),
        checks: pr.checks.clone(),
        blocked: is_blocked(&pr.title, &pr.labels),
    }
}

/// Everything the factory has published under `label`.
pub async fn list(
    github: &dyn Deliveries,
    label: &str,
    limit: u32,
    cancel: &CancellationToken,
) -> Result<Vec<Delivery>> {
    let prs = github.prs(label, limit, cancel).await?;
    Ok(prs.iter().map(from_pr).collect())
}

/// True when a delivery legitimately carries a `[BLOCKED]`/`[REJECTED]` banner or label.
pub fn is_blocked(title: &str, labels: &[String]) -> bool {
    BLOCKED_LABELS
        .iter()
        .any(|blocked| labels.iter().any(|have| have == *blocked))
        || BLOCKED_BANNERS
            .iter()
            .any(|banner| title.starts_with(*banner))
}

/// Split `factory/<issue_id>-<run_id>` on the **last** hyphen.
///
/// Issue ids may contain `-` and `.`, so the branch is not splittable from the left. Getting this
/// backwards produces an issue id that is a prefix of the real one, which then fails to match the
/// artifact directory for reasons nobody can see.
pub fn split_branch(branch: &str) -> (String, String) {
    match DeliveryId::parse(branch) {
        Ok(DeliveryId::Branch { issue_id, run_id }) => (issue_id, run_id),
        _ => (String::new(), String::new()),
    }
}

/// How far verification is allowed to go, and where it is allowed to work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyOpts {
    /// Re-run the target's own tests from a fresh checkout of the published branch.
    pub clone: bool,
    /// The repository, when the context does not name one.
    pub repo: Option<String>,
    /// The branch the PR must merge into.
    pub base_branch: Option<String>,
    /// The target directory inside the repo, when it is not to be discovered.
    pub target_dir: Option<String>,
    /// How long the re-run suite may take.
    pub test_timeout: Duration,
    /// Where to clone. `None` uses a scratch directory that is removed afterwards.
    pub workdir: Option<PathBuf>,
    /// Keep the checkout after the run, for someone who wants to look at it.
    pub keep_checkout: bool,
    /// Where the published branch actually lives, when it is not a GitHub repository.
    ///
    /// `scm = "local"` publishes to a bare repository in the run directory rather than to a forge,
    /// and that branch is no less delivered for it: the stress line, the demo and every hermetic
    /// test produce exactly this shape. Without a way to name it, `verify --clone` could only ever
    /// attest deliveries that reached GitHub, which would quietly redefine "independently
    /// verified" as "verified if you paid for a forge".
    pub origin: Option<String>,
    /// The published branch, when there is no pull request to read it from.
    pub branch: Option<String>,
}

impl Default for VerifyOpts {
    fn default() -> Self {
        Self {
            clone: false,
            repo: None,
            base_branch: None,
            target_dir: None,
            test_timeout: TEST_TIMEOUT,
            workdir: None,
            keep_checkout: false,
            origin: None,
            branch: None,
        }
    }
}

/// Verify one delivery, keeping the three claims apart.
#[allow(clippy::too_many_arguments)]
pub async fn verify(
    github: Option<&dyn Deliveries>,
    metrics: Option<&dyn MetricsStore>,
    commands: &dyn CommandRunner,
    context: &Context,
    id: &DeliveryId,
    opts: &VerifyOpts,
    cancel: &CancellationToken,
) -> Result<DeliveryReport> {
    let mut target = resolve(github, id, cancel).await?;
    let mut checks: Vec<Evidence> = Vec::new();

    // Ask the forge once, before anything is judged: the answer decides both what `Published` can
    // attest and whether this delivery is one the factory blocked on purpose.
    let lookup = look_up_pr(github, &target, cancel).await;
    if let PrLookup::Found(pr) = &lookup {
        target.pr_url = Some(pr.url.clone());
        target.head_sha = Some(pr.head_sha.clone()).filter(|sha| !sha.is_empty());
        target.blocked = is_blocked(&pr.title, &pr.labels);
        target.pr = Some(pr.clone());
    }

    // --- Reported: what the run said about itself, in files the run itself committed. ---
    //
    // A local checkout is read from *itself*, not from the context's metrics root: verifying a
    // directory someone handed you against a different directory's history would answer a question
    // nobody asked.
    let committed: Vec<RunMetrics> = match &target.local {
        Some(dir) => FsMetrics::new(dir.clone()).load(),
        None => match metrics {
            Some(store) => store.runs(cancel).await.unwrap_or_default(),
            None => Vec::new(),
        },
    };
    let record = pick_record(&committed, &target);
    if let (Some(found), true) = (&record, target.run_id.is_empty()) {
        // A checkout with exactly one run in it names itself; nothing else could have.
        target.run_id = found.run_id.clone();
        target.issue_id = found.issue_id.clone();
    }
    let blocked = target.blocked;
    checks.extend(reported_checks(record.as_ref(), &target, blocked));

    // --- Published: what the forge attests. ---
    checks.extend(published_checks(&lookup, &target, opts));

    // --- Verified: what this process re-derived from the branch itself. ---
    let mut tests = None;
    if opts.clone {
        // A named origin already says where to clone from, so it stands in for the repo: a
        // local-remote delivery has no `owner/name` and does not need one.
        let repo = match (
            &opts.origin,
            opts.repo.clone().or_else(|| context.repo.clone()),
        ) {
            (Some(_), named) => named.unwrap_or_default(),
            (None, Some(named)) => named,
            (None, None) => {
                return Err(OpsError::operational(
                    "verification needs a repo; this context does not name one",
                )
                .with_hint("swf deliveries verify … --repo owner/name (or --from <remote>)"))
            }
        };
        let (rerun, evidence) = rerun_tests(commands, &repo, &target, opts, cancel).await;
        checks.extend(rerun);
        tests = evidence;
    } else {
        checks.push(Evidence::skipped(
            "tests.rerun",
            EvidenceLevel::Verified,
            "--clone was not requested, so nothing above `published` was attempted",
        ));
    }

    Ok(DeliveryReport::resolve(
        DeliveryRef {
            branch: target.branch.clone(),
            pr_url: target.pr_url.clone(),
            head_sha: target.head_sha.clone(),
            base_sha: None,
            issue_id: target.issue_id.clone(),
            run_id: target.run_id.clone(),
        },
        checks,
        tests,
        blocked,
    ))
}

/// What a delivery identity resolves to before any evidence is gathered.
#[derive(Debug, Clone, Default)]
struct Target {
    branch: String,
    issue_id: String,
    run_id: String,
    pr: Option<PrHead>,
    pr_url: Option<String>,
    head_sha: Option<String>,
    blocked: bool,
    local: Option<PathBuf>,
}

/// Turn whichever spelling the operator used into a branch.
async fn resolve(
    github: Option<&dyn Deliveries>,
    id: &DeliveryId,
    cancel: &CancellationToken,
) -> Result<Target> {
    match id {
        DeliveryId::Branch { issue_id, run_id } => Ok(Target {
            branch: format!("{DELIVERY_BRANCH_PREFIX}{issue_id}-{run_id}"),
            issue_id: issue_id.clone(),
            run_id: run_id.clone(),
            ..Target::default()
        }),
        DeliveryId::Pr(number) => {
            let github = github.ok_or_else(|| {
                OpsError::operational(
                    "a pull request number needs a repo to look it up in; this context has none",
                )
                .with_hint("verify by branch instead: swf deliveries verify factory/<issue>-<run>")
            })?;
            let prs = github.prs("factory", 100, cancel).await?;
            let pr = prs
                .iter()
                .find(|pr| pr.number == i64::try_from(*number).unwrap_or(i64::MAX))
                .ok_or_else(|| {
                    OpsError::not_found(format!("no factory pull request #{number}"))
                        .with_hint("swf deliveries list")
                })?;
            let (issue_id, run_id) = split_branch(&pr.head);
            Ok(Target {
                branch: pr.head.clone(),
                issue_id,
                run_id,
                pr_url: Some(pr.url.clone()),
                blocked: is_blocked(&pr.title, &pr.labels),
                ..Target::default()
            })
        }
        DeliveryId::Url(url) => match pr_number_in(url) {
            Some(number) => Box::pin(resolve(github, &DeliveryId::Pr(number), cancel)).await,
            None => Err(OpsError::usage(format!("{url} is not a pull request URL"))),
        },
        DeliveryId::Path(path) => {
            let local = PathBuf::from(path);
            if !local.is_dir() {
                return Err(OpsError::not_found(format!(
                    "{path} is not a delivery: no such branch, pull request or directory"
                )));
            }
            Ok(Target {
                local: Some(local),
                ..Target::default()
            })
        }
    }
}

/// The PR number in a GitHub URL, if it names one.
fn pr_number_in(url: &str) -> Option<u64> {
    let tail = url.split("/pull/").nth(1)?;
    let digits: String = tail.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

/// The committed record this delivery is about.
///
/// Matching is on the run id, which is the half of the branch name that is unambiguous. A local
/// checkout that was handed over without an identity falls back to "the only record there is",
/// and to nothing at all when there is more than one — guessing which run a directory means is how
/// a report ends up describing the wrong delivery.
fn pick_record(committed: &[RunMetrics], target: &Target) -> Option<RunMetrics> {
    if !target.run_id.is_empty() {
        return committed
            .iter()
            .find(|run| run.run_id == target.run_id)
            .cloned();
    }
    match committed {
        [only] => Some(only.clone()),
        _ => None,
    }
}

/// What the run committed about itself. Nothing here can ever satisfy a `Verified` check.
fn reported_checks(record: Option<&RunMetrics>, target: &Target, blocked: bool) -> Vec<Evidence> {
    let mut out = Vec::new();
    let Some(record) = record else {
        out.push(Evidence::skipped(
            "metrics.present",
            EvidenceLevel::Reported,
            "no committed metrics.json for this run is readable here",
        ));
        return out;
    };
    out.push(Evidence::pass(
        "metrics.present",
        EvidenceLevel::Reported,
        format!(
            "metrics.json for run {} ({} stage(s), {} blocker(s))",
            record.run_id,
            record.stage_status.len(),
            record.blockers
        ),
    ));
    if !target.issue_id.is_empty()
        && !record.issue_id.is_empty()
        && record.issue_id != target.issue_id
    {
        out.push(Evidence::fail(
            "metrics.issue_matches",
            EvidenceLevel::Reported,
            "the committed metrics answer a different issue than the branch names",
            target.issue_id.clone(),
            record.issue_id.clone(),
        ));
    }
    if blocked {
        out.push(Evidence::skipped(
            "metrics.tests_passed",
            EvidenceLevel::Reported,
            "the delivery is blocked, so its own test verdict is not the question",
        ));
    } else if record.tests_passed {
        out.push(Evidence::pass(
            "metrics.tests_passed",
            EvidenceLevel::Reported,
            "the run reports its tests green (self-reported, not verified)",
        ));
    } else {
        out.push(Evidence::fail(
            "metrics.tests_passed",
            EvidenceLevel::Reported,
            "the run reports its own tests as failing",
            "true",
            "false",
        ));
    }
    out
}

/// What asking the forge about a branch came back with.
///
/// Four answers, not two. "There is no repo configured" and "the query failed" are different facts
/// about *us*, and "there is no PR" is a fact about the delivery. Folding them together is how a
/// laptop with no network reports a published delivery as unpublished.
#[derive(Debug, Clone)]
enum PrLookup {
    /// This context names no repository, so nothing was asked.
    NotConfigured,
    /// The forge answered, and no pull request has this branch as its head.
    Missing,
    /// The forge answered.
    Found(PrHead),
    /// The question was asked and could not be answered.
    Failed(String),
}

/// Ask the forge about one branch, once.
async fn look_up_pr(
    github: Option<&dyn Deliveries>,
    target: &Target,
    cancel: &CancellationToken,
) -> PrLookup {
    let Some(github) = github else {
        return PrLookup::NotConfigured;
    };
    if target.branch.is_empty() {
        return PrLookup::NotConfigured;
    }
    match github.pr_for_branch(&target.branch, cancel).await {
        Ok(Some(pr)) => PrLookup::Found(pr),
        Ok(None) => PrLookup::Missing,
        Err(err) => PrLookup::Failed(sanitize_line(&err.to_string())),
    }
}

/// What the forge attests: the branch name, the PR, its title, labels and base.
fn published_checks(lookup: &PrLookup, target: &Target, opts: &VerifyOpts) -> Vec<Evidence> {
    let mut out = Vec::new();

    // A branch name that is not a safe git ref is a finding, never a refusal to look: `swf` is a
    // client, and declining to inspect a branch the factory happily created would make the tool
    // less useful than the defect it is reporting (`00-architecture.md` §D-B).
    //
    // Only the *failing* case is recorded. A well-formed branch name is not evidence that a branch
    // exists — it is evidence that a string was typed correctly — and letting it settle the
    // `Published` level would let an offline laptop attest publication by parsing its own argument.
    if !target.branch.is_empty() {
        if let Err(detail) = validate_git_ref(&target.branch, "branch") {
            out.push(Evidence::fail(
                "branch.name_invalid",
                EvidenceLevel::Published,
                detail,
                "a safe git ref",
                target.branch.clone(),
            ));
        }
        if target.issue_id.is_empty() || target.run_id.is_empty() {
            out.push(Evidence::fail(
                "branch.name_parses",
                EvidenceLevel::Published,
                "the branch does not split into an issue id and a run id",
                format!("{DELIVERY_BRANCH_PREFIX}<issue>-<run>"),
                target.branch.clone(),
            ));
        }
    }

    let pr = match lookup {
        // Deliberately absent, not broken: a context with no repo chose not to ask GitHub, and
        // `Skipped` is what caps the verdict at what the self-report alone can support — while a
        // question that *was* asked and could not be answered is `Unavailable`, which cannot.
        PrLookup::NotConfigured => {
            out.push(Evidence::skipped(
                "pr.exists",
                EvidenceLevel::Published,
                "no repo is configured for this context, so the forge was not asked",
            ));
            return out;
        }
        PrLookup::Failed(detail) => {
            out.push(Evidence::unavailable(
                "pr.exists",
                EvidenceLevel::Published,
                format!("the forge could not be asked: {detail}"),
            ));
            return out;
        }
        PrLookup::Missing => {
            // A delivery published to a named remote rather than to a forge is still published,
            // and `scm = "local"` produces exactly that: the demo, the stress line and every
            // hermetic run put the branch in a bare repository instead of on GitHub. Asking the
            // forge about it and calling the "no" a failure would report every local delivery as
            // unpublished — a verdict about where the branch is not, dressed up as a verdict about
            // whether it exists. So when the operator named the origin, the forge's answer is
            // simply not the question, and `branch.in_origin` (recorded by the clone, below)
            // settles the level instead.
            if opts.origin.is_some() {
                out.push(Evidence::skipped(
                    "pr.exists",
                    EvidenceLevel::Published,
                    "this delivery was published to a named remote, not to a forge",
                ));
            } else {
                out.push(Evidence::fail(
                    "pr.exists",
                    EvidenceLevel::Published,
                    "no pull request has this branch as its head",
                    format!("a pull request whose head is {}", target.branch),
                    "none",
                ));
            }
            return out;
        }
        PrLookup::Found(pr) => pr,
    };

    out.push(Evidence::pass(
        "pr.exists",
        EvidenceLevel::Published,
        format!("{} ({})", pr.url, pr.state),
    ));

    let expected_prefix = format!("{}: ", target.issue_id);
    let stripped = BLOCKED_BANNERS
        .iter()
        .find_map(|banner| pr.title.strip_prefix(*banner))
        .unwrap_or(&pr.title);
    if target.issue_id.is_empty() || stripped.starts_with(&expected_prefix) {
        out.push(Evidence::pass(
            "pr.title_shape",
            EvidenceLevel::Published,
            sanitize_line(&pr.title),
        ));
    } else {
        out.push(Evidence::fail(
            "pr.title_shape",
            EvidenceLevel::Published,
            "the title does not name the issue this branch answers",
            format!("{expected_prefix}…"),
            sanitize_line(&pr.title),
        ));
    }

    let missing: Vec<&str> = REQUIRED_LABELS
        .iter()
        .filter(|want| !pr.labels.iter().any(|have| have == *want))
        .copied()
        .collect();
    let both_banners = pr.labels.iter().any(|l| l == "factory:blocked")
        && pr.labels.iter().any(|l| l == "factory:rejected");
    if !missing.is_empty() {
        out.push(Evidence::fail(
            "pr.labels",
            EvidenceLevel::Published,
            "the delivery is missing a label the factory always applies",
            missing.join(", "),
            pr.labels.join(", "),
        ));
    } else if both_banners {
        out.push(Evidence::fail(
            "pr.labels",
            EvidenceLevel::Published,
            "a delivery cannot be both blocked and rejected",
            "one of factory:blocked, factory:rejected",
            pr.labels.join(", "),
        ));
    } else {
        out.push(Evidence::pass(
            "pr.labels",
            EvidenceLevel::Published,
            pr.labels.join(", "),
        ));
    }

    match &opts.base_branch {
        Some(base) if pr.base_ref == *base => out.push(Evidence::pass(
            "pr.base",
            EvidenceLevel::Published,
            format!("merges into {base}"),
        )),
        Some(base) => out.push(Evidence::fail(
            "pr.base",
            EvidenceLevel::Published,
            "the delivery targets a different branch than the blueprint's base",
            base.clone(),
            pr.base_ref.clone(),
        )),
        None => out.push(Evidence::skipped(
            "pr.base",
            EvidenceLevel::Published,
            "no base branch was given to check against",
        )),
    }
    out
}

/// Clone the published branch and re-run the target's own test command from it.
///
/// Every failure mode below answers `Unavailable`, never `Pass` and never `Fail`: a clone that did
/// not happen has not verified anything and has not refuted anything either.
async fn rerun_tests(
    commands: &dyn CommandRunner,
    repo: &str,
    target: &Target,
    opts: &VerifyOpts,
    cancel: &CancellationToken,
) -> (Vec<Evidence>, Option<TestEvidence>) {
    let mut out = Vec::new();
    // `--branch` names the ref when there is no pull request to read it from; a forge delivery
    // still takes it from the PR head, which is the only trustworthy source when one exists.
    let branch = if target.branch.is_empty() {
        opts.branch.clone().unwrap_or_default()
    } else {
        target.branch.clone()
    };
    if branch.is_empty() {
        out.push(Evidence::unavailable(
            "tests.rerun",
            EvidenceLevel::Verified,
            "this delivery has no published branch to clone",
        ));
        return (out, None);
    }

    let scratch = match opts.workdir.clone() {
        Some(dir) => {
            if let Err(err) = std::fs::create_dir_all(&dir) {
                out.push(Evidence::unavailable(
                    "branch.exists",
                    EvidenceLevel::Verified,
                    format!("cannot use {}: {err}", dir.display()),
                ));
                return (out, None);
            }
            Scratch::borrowed(dir)
        }
        None => match Scratch::temporary(opts.keep_checkout) {
            Ok(scratch) => scratch,
            Err(err) => {
                out.push(Evidence::unavailable(
                    "branch.exists",
                    EvidenceLevel::Verified,
                    format!("cannot create a scratch directory: {err}"),
                ));
                return (out, None);
            }
        },
    };
    let checkout = scratch.path().join("checkout");
    let checkout_str = checkout.display().to_string();
    let url = opts
        .origin
        .clone()
        .unwrap_or_else(|| format!("https://github.com/{repo}.git"));

    let mut plan: Vec<Vec<String>> = vec![argv(&[
        "git",
        "clone",
        "--quiet",
        "--depth",
        CLONE_DEPTH,
        &url,
        &checkout_str,
    ])];
    plan.push(argv(&[
        "git",
        "-C",
        &checkout_str,
        "fetch",
        "--quiet",
        "--depth",
        CLONE_DEPTH,
        "origin",
        &branch,
    ]));
    plan.push(argv(&[
        "git",
        "-C",
        &checkout_str,
        "checkout",
        "--quiet",
        "FETCH_HEAD",
    ]));

    for step in plan {
        match commands.run(&step, opts.test_timeout, cancel).await {
            Ok(out_step) if out_step.code == 0 => {}
            Ok(out_step) => {
                out.push(Evidence::unavailable(
                    "branch.exists",
                    EvidenceLevel::Verified,
                    format!("{}: {}", step.join(" "), out_step.message()),
                ));
                return (out, None);
            }
            Err(err) => {
                out.push(Evidence::unavailable(
                    "branch.exists",
                    EvidenceLevel::Verified,
                    format!("{}: {err}", step.join(" ")),
                ));
                return (out, None);
            }
        }
    }
    out.push(Evidence::pass(
        "branch.exists",
        EvidenceLevel::Verified,
        format!("{branch} was cloned from {url}"),
    ));
    if opts.origin.is_some() {
        // Fetching the ref out of the remote is a stronger answer than any forge API gives: the
        // branch is not merely reported to exist, its commits are now on this disk.
        out.push(Evidence::pass(
            "branch.in_origin",
            EvidenceLevel::Published,
            format!("{branch} was fetched from {url}"),
        ));
    }

    // The contract comes out of the checkout, never out of the operator's working copy: the point
    // of the exercise is that the branch says how to test itself.
    let dir = match opts.target_dir.as_deref() {
        Some(target_dir) => checkout.join(target_dir),
        None => match find_contract(&checkout) {
            Some(dir) => dir,
            None => {
                out.push(Evidence::unavailable(
                    "tests.rerun",
                    EvidenceLevel::Verified,
                    "target has no factory.toml; the factory refuses to guess commands",
                ));
                return (out, None);
            }
        },
    };
    let contract_path = dir.join(CONTRACT_FILE);
    let text = match std::fs::read_to_string(&contract_path) {
        Ok(text) => text,
        Err(_) => {
            out.push(Evidence::unavailable(
                "tests.rerun",
                EvidenceLevel::Verified,
                "target has no factory.toml; the factory refuses to guess commands",
            ));
            return (out, None);
        }
    };
    let contract = match parse_contract(&text) {
        Ok(contract) => contract,
        Err(detail) => {
            out.push(Evidence::fail(
                "target.contract",
                EvidenceLevel::Verified,
                detail,
                "a factory.toml the factory can read",
                contract_path.display().to_string(),
            ));
            return (out, None);
        }
    };

    // A stale report would be read as this run's result, so it goes before the run and not after.
    let junit = dir.join(&contract.junit);
    if let Err(err) = remove_if_present(&junit) {
        out.push(Evidence::unavailable(
            "tests.rerun",
            EvidenceLevel::Verified,
            format!("could not clear stale JUnit report: {err}"),
        ));
        return (out, None);
    }

    // The command is evaluated by a shell, as the factory runs it — but the directory and the
    // command are passed as *arguments*, so neither can inject the other.
    let shell = argv(&[
        "sh",
        "-c",
        "cd \"$1\" || exit 1; eval \"$2\"",
        "swf",
        &dir.display().to_string(),
        &contract.test,
    ]);
    let started = Instant::now();
    let (code, timed_out) = match commands.run(&shell, opts.test_timeout, cancel).await {
        Ok(result) => (result.code, false),
        Err(swf_adapters::error::AdapterError::Timeout { .. }) => (-1, true),
        Err(err) => {
            out.push(Evidence::unavailable(
                "tests.rerun",
                EvidenceLevel::Verified,
                format!("the test command could not be run: {err}"),
            ));
            return (out, None);
        }
    };
    let duration_s = started.elapsed().as_secs_f64();

    let counts = std::fs::read_to_string(&junit)
        .map_err(|err| err.to_string())
        .and_then(|xml| parse_junit(&xml));
    let (passed, failed, errors, skipped, report_valid, detail) = match &counts {
        Ok(counts) => (
            counts.passed,
            counts.failed,
            counts.errors,
            counts.skipped,
            true,
            format!(
                "{} passed, {} failed, {} errored, {} skipped",
                counts.passed, counts.failed, counts.errors, counts.skipped
            ),
        ),
        Err(detail) => (0, 0, 0, 0, false, detail.clone()),
    };
    let ok = code == 0 && report_valid && failed == 0 && errors == 0 && !timed_out;

    let evidence = TestEvidence {
        command: contract.test.clone(),
        cwd: dir.display().to_string(),
        junit_path: contract.junit.clone(),
        exit_code: code,
        passed,
        failed,
        errors,
        skipped,
        report_valid,
        ok,
        duration_s,
        timed_out,
    };
    if ok {
        out.push(Evidence::pass(
            "tests.rerun",
            EvidenceLevel::Verified,
            format!("{} -> {detail}", contract.test),
        ));
    } else if timed_out {
        out.push(Evidence::unavailable(
            "tests.rerun",
            EvidenceLevel::Verified,
            format!("{} did not finish within the budget", contract.test),
        ));
    } else {
        out.push(Evidence::fail(
            "tests.rerun",
            EvidenceLevel::Verified,
            format!("the target's own tests do not pass on this branch: {detail}"),
            "a green run and a coherent report",
            format!("exit {code}; {detail}"),
        ));
    }
    (out, Some(evidence))
}

/// Build an argv without ever building a shell string.
fn argv(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|part| (*part).to_string()).collect()
}

/// Remove a file that may not be there.
fn remove_if_present(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

/// A working directory that cleans up after itself unless it was asked not to.
enum Scratch {
    Owned(tempfile::TempDir),
    Kept(PathBuf),
}

impl Scratch {
    fn temporary(keep: bool) -> std::io::Result<Self> {
        let dir = tempfile::Builder::new().prefix("swf-verify-").tempdir()?;
        Ok(if keep {
            Self::Kept(dir.keep())
        } else {
            Self::Owned(dir)
        })
    }

    fn borrowed(path: PathBuf) -> Self {
        Self::Kept(path)
    }

    fn path(&self) -> &Path {
        match self {
            Self::Owned(dir) => dir.path(),
            Self::Kept(path) => path.as_path(),
        }
    }
}

/// The target's own contract: what it says its tests are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetContract {
    /// `[commands].test`. Required — the factory refuses to guess.
    pub test: String,
    /// `[commands].lint`, if there is one.
    pub lint: Option<String>,
    /// `[paths].junit`, which must live below `.factory/`.
    pub junit: String,
    /// `[paths].source`.
    pub source: String,
    /// `[paths].tests`.
    pub tests_dir: String,
    /// `[paths].protected`, de-duplicated with order preserved.
    pub protected: Vec<String>,
}

/// Parse a `factory.toml` with the same rules the factory applies to it.
///
/// The error strings are the Python's, verbatim, because they are what an operator has already
/// seen once in a run log and will search for.
pub fn parse_contract(text: &str) -> std::result::Result<TargetContract, String> {
    let table: toml::Table = toml::from_str(text).map_err(|e| e.message().to_string())?;
    let string_at = |section: &str, key: &str| -> Option<String> {
        table
            .get(section)?
            .as_table()?
            .get(key)?
            .as_str()
            .map(str::to_string)
    };
    let command = |key: &str| -> std::result::Result<Option<String>, String> {
        match string_at("commands", key) {
            Some(raw) => {
                let value = raw.trim().to_string();
                if value.is_empty() || value.contains('\0') {
                    return Err(format!("commands.{key} must be a non-empty command"));
                }
                Ok(Some(value))
            }
            None => Ok(None),
        }
    };

    let test = command("test")?.ok_or("factory.toml must define [commands].test")?;
    let lint = command("lint")?;
    let junit = string_at("paths", "junit").unwrap_or_else(|| DEFAULT_JUNIT.to_string());
    if !junit.starts_with(JUNIT_PREFIX) {
        return Err("paths.junit must live below .factory/".to_string());
    }
    let mut protected: Vec<String> = Vec::new();
    if let Some(values) = table
        .get("paths")
        .and_then(toml::Value::as_table)
        .and_then(|paths| paths.get("protected"))
    {
        let items = values
            .as_array()
            .ok_or("paths.protected must be an array of strings")?;
        for item in items {
            let value = item
                .as_str()
                .ok_or("paths.protected must be an array of strings")?;
            if !protected.iter().any(|seen| seen == value) {
                protected.push(value.to_string());
            }
        }
    }
    Ok(TargetContract {
        test,
        lint,
        junit,
        source: string_at("paths", "source").unwrap_or_else(|| "src".to_string()),
        tests_dir: string_at("paths", "tests").unwrap_or_else(|| "tests".to_string()),
        protected,
    })
}

/// Find the directory holding a `factory.toml`, shallowest first, deterministically.
pub fn find_contract(root: &Path) -> Option<PathBuf> {
    let mut frontier = vec![root.to_path_buf()];
    for _ in 0..=CONTRACT_SEARCH_DEPTH {
        if let Some(found) = frontier
            .iter()
            .find(|dir| dir.join(CONTRACT_FILE).is_file())
        {
            return Some(found.clone());
        }
        let mut next = Vec::new();
        for dir in &frontier {
            let Ok(entries) = std::fs::read_dir(dir) else {
                continue;
            };
            let mut children: Vec<PathBuf> = entries
                .filter_map(std::result::Result::ok)
                .map(|entry| entry.path())
                .filter(|path| {
                    path.is_dir()
                        && path
                            .file_name()
                            .is_some_and(|name| !name.to_string_lossy().starts_with('.'))
                })
                .collect();
            children.sort();
            next.extend(children);
        }
        if next.is_empty() {
            return None;
        }
        frontier = next;
    }
    None
}

/// What a JUnit report actually says, recomputed rather than believed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct JunitCounts {
    /// Tests that passed, derived rather than read.
    pub passed: u32,
    /// Tests that failed.
    pub failed: u32,
    /// Tests that errored.
    pub errors: u32,
    /// Tests that were skipped.
    pub skipped: u32,
    /// The suite total the report claims.
    pub total: u32,
}

/// Parse a JUnit report the way `stages._parse_junit` does, and refuse an incoherent one.
///
/// The two rules that matter: element names are read namespace-insensitively, and a `testsuites`
/// root contributes only its **direct** `testsuite` children. Recursing would double-count a
/// nested suite's totals against its parent's, which turns a green run into an impossible one.
pub fn parse_junit(xml: &str) -> std::result::Result<JunitCounts, String> {
    let elements = scan_elements(xml);
    let root = elements
        .first()
        .ok_or("JUnit report contains no testsuite")?;
    let suites: Vec<&Element> = match root.name.as_str() {
        "testsuite" => vec![root],
        "testsuites" => elements
            .iter()
            .filter(|el| el.depth == 1 && el.name == "testsuite")
            .collect(),
        _ => Vec::new(),
    };
    if suites.is_empty() {
        return Err("JUnit report contains no testsuite".to_string());
    }
    let sum = |key: &str| -> i64 {
        suites
            .iter()
            .map(|suite| {
                suite
                    .attrs
                    .iter()
                    .find(|(name, _)| name == key)
                    .and_then(|(_, value)| value.trim().parse::<i64>().ok())
                    .unwrap_or(0)
            })
            .sum()
    };
    let total = sum("tests");
    let failed = sum("failures");
    let errors = sum("errors");
    let skipped = sum("skipped");
    if total <= 0 || failed < 0 || errors < 0 || skipped < 0 {
        return Err("JUnit report contains invalid test counts".to_string());
    }
    if failed + errors + skipped > total {
        return Err("JUnit report result counts exceed its test count".to_string());
    }
    let passed = (total - failed - errors - skipped).max(0);
    Ok(JunitCounts {
        passed: passed as u32,
        failed: failed as u32,
        errors: errors as u32,
        skipped: skipped as u32,
        total: total as u32,
    })
}

/// One XML element, flattened: local name, nesting depth and attributes.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Element {
    name: String,
    depth: usize,
    attrs: Vec<(String, String)>,
}

/// Walk the tags of an XML document without pulling in a parser.
///
/// A JUnit report is a flat, attribute-only document, and the only structural question asked of it
/// is "which `testsuite` elements are direct children of the root?". A scanner that answers that
/// and nothing else is easier to reason about than a dependency, and it cannot be talked into
/// expanding an entity.
fn scan_elements(xml: &str) -> Vec<Element> {
    let bytes: Vec<char> = xml.chars().collect();
    let mut out: Vec<Element> = Vec::new();
    let mut depth = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] != '<' {
            i += 1;
            continue;
        }
        // Declarations, comments and doctypes carry no structure worth reading.
        if bytes.get(i + 1) == Some(&'!') || bytes.get(i + 1) == Some(&'?') {
            let rest: String = bytes[i..].iter().collect();
            let skip = if rest.starts_with("<!--") {
                rest.find("-->").map(|at| at + 3)
            } else {
                rest.find('>').map(|at| at + 1)
            };
            i += skip.unwrap_or(bytes.len() - i);
            continue;
        }
        let close = match bytes[i..].iter().position(|c| *c == '>') {
            Some(at) => i + at,
            None => break,
        };
        let tag: String = bytes[i + 1..close].iter().collect();
        i = close + 1;
        if tag.starts_with('/') {
            depth = depth.saturating_sub(1);
            continue;
        }
        let self_closing = tag.trim_end().ends_with('/');
        let body = tag.trim_end().trim_end_matches('/');
        let mut parts = body.splitn(2, |c: char| c.is_ascii_whitespace());
        let raw_name = parts.next().unwrap_or_default();
        let name = raw_name.rsplit(':').next().unwrap_or(raw_name).to_string();
        if name.is_empty() {
            continue;
        }
        out.push(Element {
            name,
            depth,
            attrs: scan_attrs(parts.next().unwrap_or_default()),
        });
        if !self_closing {
            depth += 1;
        }
    }
    out
}

/// `name="value"` pairs, in either quoting style.
fn scan_attrs(text: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0usize;
    while i < chars.len() {
        while i < chars.len() && (chars[i].is_ascii_whitespace() || chars[i] == '/') {
            i += 1;
        }
        let start = i;
        while i < chars.len() && chars[i] != '=' && !chars[i].is_ascii_whitespace() {
            i += 1;
        }
        if start == i {
            break;
        }
        let raw_name: String = chars[start..i].iter().collect();
        let name = raw_name.rsplit(':').next().unwrap_or(&raw_name).to_string();
        while i < chars.len() && chars[i].is_ascii_whitespace() {
            i += 1;
        }
        if chars.get(i) != Some(&'=') {
            continue;
        }
        i += 1;
        while i < chars.len() && chars[i].is_ascii_whitespace() {
            i += 1;
        }
        let quote = match chars.get(i) {
            Some(&q @ ('"' | '\'')) => {
                i += 1;
                q
            }
            _ => continue,
        };
        let start = i;
        while i < chars.len() && chars[i] != quote {
            i += 1;
        }
        let value: String = chars[start..i.min(chars.len())].iter().collect();
        i += 1;
        out.push((name, value));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_branch_splits_on_the_last_hyphen_because_issue_ids_contain_them() {
        assert_eq!(
            split_branch("factory/PROJ-17-a1b2c3d4"),
            ("PROJ-17".to_string(), "a1b2c3d4".to_string())
        );
        assert_eq!(
            split_branch("factory/42-abcd1234"),
            ("42".to_string(), "abcd1234".to_string())
        );
        assert_eq!(split_branch("main"), (String::new(), String::new()));
    }

    #[test]
    fn a_blocked_delivery_is_recognised_by_either_a_label_or_a_banner() {
        assert!(is_blocked("42: x", &["factory:blocked".to_string()]));
        assert!(is_blocked("[REJECTED] 42: x", &[]));
        assert!(!is_blocked("42: x", &["factory".to_string()]));
    }

    #[test]
    fn a_pull_request_url_yields_its_number() {
        assert_eq!(
            pr_number_in("https://github.com/acme/widgets/pull/17"),
            Some(17)
        );
        assert_eq!(
            pr_number_in("https://github.com/acme/widgets/pull/17/files"),
            Some(17)
        );
        assert_eq!(pr_number_in("https://github.com/acme/widgets"), None);
    }

    #[test]
    fn a_contract_without_a_test_command_is_refused_rather_than_guessed_at() {
        let err = parse_contract("[paths]\nsource = \"src\"\n").expect_err("no test");
        assert_eq!(err, "factory.toml must define [commands].test");
        let err = parse_contract("[commands]\ntest = \"  \"\n").expect_err("blank");
        assert!(err.contains("non-empty"), "{err}");
    }

    #[test]
    fn a_junit_path_outside_dot_factory_is_refused() {
        let err = parse_contract("[commands]\ntest = \"pytest\"\n[paths]\njunit = \"out.xml\"\n")
            .expect_err("escape");
        assert_eq!(err, "paths.junit must live below .factory/");
    }

    #[test]
    fn the_reference_contract_parses_with_its_defaults() {
        let contract = parse_contract(
            r#"
[commands]
test = "uv run --group dev pytest --junitxml=.factory/junit.xml"
lint = "uv run --group dev python -m compileall -q src"
[paths]
source = "src"
tests  = "tests"
protected = ["factory.toml", "tests/", "factory.toml"]
"#,
        )
        .expect("the shipped demo contract");
        assert_eq!(contract.junit, DEFAULT_JUNIT);
        assert_eq!(contract.tests_dir, "tests");
        assert_eq!(contract.protected, vec!["factory.toml", "tests/"]);
        assert!(contract.lint.is_some());
    }

    #[test]
    fn a_testsuites_root_counts_only_its_direct_children() {
        let xml = r#"<?xml version="1.0"?>
<testsuites tests="99" failures="99">
  <testsuite name="a" tests="3" failures="1" errors="0" skipped="1">
    <testsuite name="nested" tests="50" failures="50"/>
  </testsuite>
  <testsuite name="b" tests="2" failures="0" errors="0" skipped="0"/>
</testsuites>"#;
        let counts = parse_junit(xml).expect("parse");
        assert_eq!(
            counts.total, 5,
            "the nested suite must not be counted twice"
        );
        assert_eq!(counts.failed, 1);
        assert_eq!(counts.skipped, 1);
        assert_eq!(counts.passed, 3);
    }

    #[test]
    fn a_bare_testsuite_root_is_one_suite_and_missing_attributes_are_zero() {
        let counts = parse_junit(r#"<testsuite tests="4" failures="1"/>"#).expect("parse");
        assert_eq!(counts.total, 4);
        assert_eq!(counts.errors, 0);
        assert_eq!(counts.passed, 3);
    }

    #[test]
    fn namespaced_element_names_are_read_the_same_as_bare_ones() {
        let counts = parse_junit(r#"<ns:testsuite tests="2" ns:failures="0"/>"#).expect("parse");
        assert_eq!(counts.total, 2);
        assert_eq!(counts.passed, 2);
    }

    #[test]
    fn an_incoherent_report_is_refused_and_never_rounded_into_shape() {
        assert_eq!(
            parse_junit("<other/>").expect_err("no suite"),
            "JUnit report contains no testsuite"
        );
        assert_eq!(
            parse_junit(r#"<testsuite tests="0"/>"#).expect_err("empty"),
            "JUnit report contains invalid test counts"
        );
        assert_eq!(
            parse_junit(r#"<testsuite tests="2" failures="3"/>"#).expect_err("impossible"),
            "JUnit report result counts exceed its test count"
        );
        assert!(parse_junit("").is_err());
    }

    #[test]
    fn the_contract_is_found_at_the_shallowest_depth_and_deterministically() {
        let dir = tempfile::tempdir().expect("tempdir");
        let deep = dir.path().join("demo").join("target");
        std::fs::create_dir_all(&deep).expect("mkdir");
        std::fs::write(deep.join(CONTRACT_FILE), "").expect("write");
        assert_eq!(find_contract(dir.path()).as_deref(), Some(deep.as_path()));

        std::fs::write(dir.path().join(CONTRACT_FILE), "").expect("write");
        assert_eq!(
            find_contract(dir.path()).as_deref(),
            Some(dir.path()),
            "the root wins over a nested target"
        );
        assert!(find_contract(Path::new("/nope/nope")).is_none());
    }

    #[test]
    fn a_delivery_row_carries_the_identity_the_verify_command_takes() {
        let pr = PullRequest {
            number: 7,
            title: "[BLOCKED] 42: do the thing".into(),
            url: "https://example.com/7".into(),
            labels: vec!["factory".into(), "factory:blocked".into()],
            state: "OPEN".into(),
            checks: "1 pass / 0 fail / 0 pending".into(),
            head: "factory/42-abcd1234".into(),
        };
        let delivery = from_pr(&pr);
        assert_eq!(delivery.id, "factory/42-abcd1234");
        assert_eq!(delivery.issue_id, "42");
        assert_eq!(delivery.run_id, "abcd1234");
        assert!(delivery.blocked);
    }
}
