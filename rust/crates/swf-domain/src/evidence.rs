//! What "verified" is allowed to mean, and the machinery that refuses to let it mean less.
//!
//! Three different claims get called "it worked": the workflow reported success, a branch really
//! was published, and an independent process re-derived the facts from that branch. They have
//! different forgers — the run itself, the orchestrator credential, and nobody — so this module
//! keeps them as three separate booleans and three separate levels. Non-negotiable 10 of the
//! architecture exists because collapsing them into one green tick is how a self-reported
//! `tests_passed` ends up on a dashboard as proof.
//!
//! The committed artifact chain is written by the run that produced it. Nothing at
//! [`EvidenceLevel::Reported`] may ever satisfy a [`EvidenceLevel::Verified`] check, and the
//! resolver below has no path that lets it.

use serde::{Deserialize, Serialize};

/// Who could forge the claim a piece of evidence makes.
///
/// Ordered weakest to strongest, and `Ord` is derived so `attained` can be a max — the ordering
/// is the whole semantics, not a convenience.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceLevel {
    /// The run said so, in files the run itself committed.
    Reported,
    /// The forge says so: a ref, a PR, labels, a title. Forgeable by the orchestrator credential.
    Published,
    /// Re-derived from the branch by this verifier: fresh clone, recomputed hashes, re-run tests.
    Verified,
}

impl EvidenceLevel {
    /// The word a report prints.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reported => "reported",
            Self::Published => "published",
            Self::Verified => "verified",
        }
    }
}

impl std::fmt::Display for EvidenceLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What one check found.
///
/// `Skipped` and `Unavailable` are deliberately different. Skipped means the check does not apply
/// — this blueprint has no `spec` stage, so there is no `spec.md` to look for, and its absence
/// proves nothing is wrong. Unavailable means it *does* apply and could not be evaluated: no
/// network, no `gh`, no test runner. Folding the second into the first would let a laptop with no
/// credentials verify anything at all.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum EvidenceStatus {
    /// The claim held.
    Pass,
    /// The claim is contradicted by what was actually found.
    Fail,
    /// Not applicable to this delivery.
    Skipped {
        /// Why it does not apply.
        reason: String,
    },
    /// Applicable, but it could not be evaluated.
    Unavailable {
        /// What was missing.
        reason: String,
    },
}

impl EvidenceStatus {
    /// True only for `Pass`. Nothing else is evidence of anything.
    pub fn passed(&self) -> bool {
        matches!(self, Self::Pass)
    }

    /// True for `Fail` — the one status that refutes a delivery outright.
    pub fn refutes(&self) -> bool {
        matches!(self, Self::Fail)
    }

    /// True when the check could not be evaluated, which is not the same as passing.
    pub fn unavailable(&self) -> bool {
        matches!(self, Self::Unavailable { .. })
    }

    /// The word a report prints.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Skipped { .. } => "skipped",
            Self::Unavailable { .. } => "unavailable",
        }
    }
}

/// One atomic thing the verifier looked at.
///
/// `expected` and `actual` are separate from `detail` because a human reading a refutation needs
/// the two values side by side, and a machine consuming `--json` should not have to parse a
/// sentence to get them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Evidence {
    /// A stable machine id, e.g. `commit.trailers`. Never renamed once shipped.
    pub id: String,
    /// Which claim this check can support.
    pub level: EvidenceLevel,
    /// What it found.
    #[serde(flatten)]
    pub status: EvidenceStatus,
    /// One human sentence.
    #[serde(default)]
    pub detail: String,
    /// What the self-report or the schema said should be true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected: Option<String>,
    /// What the verifier actually found.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual: Option<String>,
}

impl Evidence {
    /// A check that held.
    pub fn pass(id: impl Into<String>, level: EvidenceLevel, detail: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            level,
            status: EvidenceStatus::Pass,
            detail: detail.into(),
            expected: None,
            actual: None,
        }
    }

    /// A check that is contradicted, with the two values that disagree.
    pub fn fail(
        id: impl Into<String>,
        level: EvidenceLevel,
        detail: impl Into<String>,
        expected: impl Into<String>,
        actual: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            level,
            status: EvidenceStatus::Fail,
            detail: detail.into(),
            expected: Some(expected.into()),
            actual: Some(actual.into()),
        }
    }

    /// A check that does not apply to this delivery.
    pub fn skipped(id: impl Into<String>, level: EvidenceLevel, reason: impl Into<String>) -> Self {
        let reason = reason.into();
        Self {
            id: id.into(),
            level,
            detail: reason.clone(),
            status: EvidenceStatus::Skipped { reason },
            expected: None,
            actual: None,
        }
    }

    /// A check that applies and could not be run.
    pub fn unavailable(
        id: impl Into<String>,
        level: EvidenceLevel,
        reason: impl Into<String>,
    ) -> Self {
        let reason = reason.into();
        Self {
            id: id.into(),
            level,
            detail: reason.clone(),
            status: EvidenceStatus::Unavailable { reason },
            expected: None,
            actual: None,
        }
    }
}

/// The delivery a report is about, named every way it can be named.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryRef {
    /// `factory/<issue_id>-<run_id>`.
    #[serde(default)]
    pub branch: String,
    /// The pull request URL, when one was found.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr_url: Option<String>,
    /// The commit the verifier actually inspected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_sha: Option<String>,
    /// The merge base the diff was taken from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_sha: Option<String>,
    /// The issue this delivery answers.
    #[serde(default)]
    pub issue_id: String,
    /// The factory run that produced it.
    #[serde(default)]
    pub run_id: String,
}

/// The outcome of re-running the target's own test command from a clean checkout.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TestEvidence {
    /// `factory.toml [commands].test`, verbatim.
    pub command: String,
    /// Where it ran: `<checkout>/<target_dir>`.
    pub cwd: String,
    /// `factory.toml [paths].junit`.
    pub junit_path: String,
    /// The command's exit code.
    pub exit_code: i32,
    /// Counts recomputed from the JUnit report, not from the self-report.
    pub passed: u32,
    /// Tests that failed.
    pub failed: u32,
    /// Tests that errored.
    pub errors: u32,
    /// Tests that were skipped.
    pub skipped: u32,
    /// False when the report was missing, unparseable, or internally inconsistent.
    pub report_valid: bool,
    /// The overall verdict: a green exit *and* a coherent report.
    pub ok: bool,
    /// Wall time.
    pub duration_s: f64,
    /// True when the runner was killed rather than finishing.
    pub timed_out: bool,
}

/// What the whole body of evidence adds up to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Every applicable `Verified`-level check passed, tests included.
    Verified,
    /// The same, for a delivery that legitimately carries `[BLOCKED]` / `[REJECTED]`.
    ///
    /// A blocked delivery is a *correct* outcome, not a verification failure: the factory found
    /// blockers and said so. What is verified is that it said so consistently everywhere.
    VerifiedBlocked,
    /// The branch, PR and trailers check out; nothing above `Published` could be established.
    PublishedOnly,
    /// Only the run's own committed report could be read.
    ReportedOnly,
    /// At least one check is contradicted. One `Fail` is enough, whatever else passed.
    Refuted,
    /// No contradiction, but something that had to be checked could not be.
    Inconclusive,
}

impl Verdict {
    /// The word a report prints.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::VerifiedBlocked => "verified_blocked",
            Self::PublishedOnly => "published_only",
            Self::ReportedOnly => "reported_only",
            Self::Refuted => "refuted",
            Self::Inconclusive => "inconclusive",
        }
    }

    /// True only for the two verdicts that mean an independent process re-derived the facts.
    pub fn is_verified(self) -> bool {
        matches!(self, Self::Verified | Self::VerifiedBlocked)
    }

    /// The process exit code: `0` verified, `1` refuted, `2` for everything that is neither.
    ///
    /// "Not proven" is not "proven false", and a CI job that treats them the same will either
    /// ship refuted work or block on a missing credential.
    pub fn exit_code(self) -> i32 {
        match self {
            Self::Verified | Self::VerifiedBlocked => 0,
            Self::Refuted => 1,
            _ => 2,
        }
    }
}

impl std::fmt::Display for Verdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Everything `swf deliveries verify` learned, and the three claims it keeps apart.
///
/// The booleans are not derived from `verdict` and `verdict` is not derived from them alone: a
/// reader who only wants "did the branch actually get published?" must be able to read that
/// without decoding a six-way enum, and must not be able to read it as anything else.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeliveryReport {
    /// Which delivery this is about.
    pub delivery: DeliveryRef,
    /// The overall outcome.
    pub verdict: Verdict,
    /// The highest level at which every applicable check passed.
    pub attained: EvidenceLevel,
    /// *The workflow says it succeeded.* Self-reported; the run wrote this about itself.
    pub workflow_succeeded: bool,
    /// *A branch/PR really exists with this shape.* Attested by the forge.
    pub branch_published: bool,
    /// *An independent process re-derived the facts from the branch.* Attested by this verifier.
    pub independently_verified: bool,
    /// Every check, in the order they were run.
    #[serde(default)]
    pub checks: Vec<Evidence>,
    /// The re-run test result, when tests were attempted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tests: Option<TestEvidence>,
}

impl DeliveryReport {
    /// Resolve a set of checks into a report.
    ///
    /// `blocked` says the delivery legitimately carries a `[BLOCKED]`/`[REJECTED]` banner, which
    /// changes `Verified` into `VerifiedBlocked` and nothing else — a blocked delivery whose
    /// evidence is internally consistent is a success of the process, and reporting it as a
    /// failure trains people to ignore the verifier.
    ///
    /// The resolution order is fixed: any `Fail` refutes, whatever else is green. Then a level
    /// counts as attained only if it was *attempted* and every check at it passed, so a run with
    /// no network cannot reach `Published` by having asked no questions.
    pub fn resolve(
        delivery: DeliveryRef,
        checks: Vec<Evidence>,
        tests: Option<TestEvidence>,
        blocked: bool,
    ) -> Self {
        // A level is attained only when something at it actually *passed* and nothing at it
        // failed or went unevaluated. `Skipped` neither proves nor blocks — it is the honest
        // answer for a check that does not apply — so a level made of nothing but skips proves
        // nothing, and a level with one pass and three skips does.
        let settled = |level: EvidenceLevel| {
            let at = || checks.iter().filter(|c| c.level == level);
            at().any(|c| c.status.passed())
                && !at().any(|c| c.status.refutes() || c.status.unavailable())
        };
        let unavailable = |level: EvidenceLevel| {
            checks
                .iter()
                .any(|c| c.level == level && c.status.unavailable())
        };

        let refuted = checks.iter().any(|c| c.status.refutes());
        let reported = settled(EvidenceLevel::Reported);
        let published = settled(EvidenceLevel::Published);
        // Tests are the only Verified-level evidence a self-report can be checked against, so a
        // report that ran them must have them green before anything claims `Verified`.
        let tests_ok = tests.as_ref().map(|t| t.ok).unwrap_or(true);
        let verified = settled(EvidenceLevel::Verified) && tests_ok;

        let verdict = if refuted {
            Verdict::Refuted
        } else if verified {
            if blocked {
                Verdict::VerifiedBlocked
            } else {
                Verdict::Verified
            }
        } else if unavailable(EvidenceLevel::Verified) || unavailable(EvidenceLevel::Published) {
            Verdict::Inconclusive
        } else if published {
            Verdict::PublishedOnly
        } else if reported {
            Verdict::ReportedOnly
        } else {
            Verdict::Inconclusive
        };

        let attained = if verified {
            EvidenceLevel::Verified
        } else if published {
            EvidenceLevel::Published
        } else {
            EvidenceLevel::Reported
        };

        Self {
            delivery,
            verdict,
            attained,
            workflow_succeeded: reported,
            branch_published: published,
            independently_verified: verified,
            checks,
            tests,
        }
    }

    /// Every check that refuted the delivery — what a `--json` consumer shows first.
    pub fn refutations(&self) -> Vec<&Evidence> {
        self.checks.iter().filter(|c| c.status.refutes()).collect()
    }

    /// The process exit code for this report.
    pub fn exit_code(&self) -> i32 {
        self.verdict.exit_code()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use EvidenceLevel::{Published, Reported, Verified};

    fn delivery() -> DeliveryRef {
        DeliveryRef {
            branch: "factory/42-r1".into(),
            issue_id: "42".into(),
            run_id: "r1".into(),
            ..DeliveryRef::default()
        }
    }

    fn green_tests() -> TestEvidence {
        TestEvidence {
            command: "pytest".into(),
            cwd: "/tmp/co/demo/target".into(),
            junit_path: ".factory/junit.xml".into(),
            exit_code: 0,
            passed: 12,
            failed: 0,
            errors: 0,
            skipped: 1,
            report_valid: true,
            ok: true,
            duration_s: 3.5,
            timed_out: false,
        }
    }

    #[test]
    fn the_three_claims_are_three_separate_booleans() {
        let checks = vec![
            Evidence::pass("chain.metrics_parses", Reported, "metrics.json parses"),
            Evidence::pass("pr.exists", Published, "PR #7"),
            Evidence::unavailable("commit.trailers", Verified, "no network"),
        ];
        let report = DeliveryReport::resolve(delivery(), checks, None, false);
        assert!(report.workflow_succeeded);
        assert!(report.branch_published);
        assert!(!report.independently_verified);
        assert_eq!(report.verdict, Verdict::Inconclusive);
        assert_eq!(report.attained, Published);
    }

    #[test]
    fn a_self_report_can_never_reach_the_verified_level() {
        let checks = vec![Evidence::pass(
            "chain.present",
            Reported,
            "all files present",
        )];
        let report = DeliveryReport::resolve(delivery(), checks, None, false);
        assert!(report.workflow_succeeded);
        assert!(!report.independently_verified);
        assert_eq!(report.verdict, Verdict::ReportedOnly);
        assert_eq!(report.attained, Reported);
        assert_eq!(report.exit_code(), 2);
    }

    #[test]
    fn one_failure_refutes_whatever_else_passed() {
        let checks = vec![
            Evidence::pass("pr.exists", Published, "PR #7"),
            Evidence::pass("commit.identity", Verified, "swfactory-bot"),
            Evidence::fail(
                "chain.plan_roundtrip",
                Verified,
                "plan.md does not match plan.json",
                "sha:aaa",
                "sha:bbb",
            ),
        ];
        let report = DeliveryReport::resolve(delivery(), checks, Some(green_tests()), false);
        assert_eq!(report.verdict, Verdict::Refuted);
        assert_eq!(report.exit_code(), 1);
        assert_eq!(report.refutations().len(), 1);
        assert!(!report.independently_verified);
    }

    #[test]
    fn a_full_pass_with_green_tests_is_verified() {
        let checks = vec![
            Evidence::pass("chain.present", Reported, "all files present"),
            Evidence::pass("pr.exists", Published, "PR #7"),
            Evidence::pass("commit.trailers", Verified, "every commit carries them"),
            Evidence::skipped("chain.spec", Verified, "no spec stage in this blueprint"),
        ];
        let report = DeliveryReport::resolve(delivery(), checks, Some(green_tests()), false);
        assert_eq!(report.verdict, Verdict::Verified);
        assert_eq!(report.attained, Verified);
        assert!(report.independently_verified);
        assert_eq!(report.exit_code(), 0);
    }

    #[test]
    fn a_skipped_check_does_not_block_a_level_but_an_unavailable_one_does() {
        let mixed = vec![
            Evidence::pass("commit.trailers", Verified, "ok"),
            Evidence::skipped("chain.spec", Verified, "no spec stage"),
        ];
        assert!(DeliveryReport::resolve(delivery(), mixed, None, false).independently_verified);

        let unavailable = vec![
            Evidence::pass("commit.trailers", Verified, "ok"),
            Evidence::unavailable("tests.rerun", Verified, "no runner"),
        ];
        let report = DeliveryReport::resolve(delivery(), unavailable, None, false);
        assert!(!report.independently_verified);
        assert_eq!(report.verdict, Verdict::Inconclusive);
    }

    #[test]
    fn a_level_made_of_nothing_but_skips_proves_nothing() {
        let checks = vec![Evidence::skipped("chain.spec", Verified, "no spec stage")];
        let report = DeliveryReport::resolve(delivery(), checks, None, false);
        assert!(!report.independently_verified);
        assert_eq!(report.verdict, Verdict::Inconclusive);
    }

    #[test]
    fn red_tests_stop_a_verified_verdict_even_when_every_check_passed() {
        let checks = vec![Evidence::pass("commit.trailers", Verified, "ok")];
        let mut tests = green_tests();
        tests.ok = false;
        tests.failed = 2;
        let report = DeliveryReport::resolve(delivery(), checks, Some(tests), false);
        assert!(!report.independently_verified);
        assert_ne!(report.verdict, Verdict::Verified);
    }

    #[test]
    fn a_blocked_delivery_verifies_as_blocked_not_as_a_failure() {
        let checks = vec![
            Evidence::pass("pr.labels", Published, "factory:blocked present"),
            Evidence::pass("state.blocked_consistent", Verified, "blockers agree"),
        ];
        let report = DeliveryReport::resolve(delivery(), checks, Some(green_tests()), true);
        assert_eq!(report.verdict, Verdict::VerifiedBlocked);
        assert!(report.verdict.is_verified());
        assert_eq!(report.exit_code(), 0);
    }

    #[test]
    fn a_verifier_that_asked_nothing_attains_nothing() {
        let report = DeliveryReport::resolve(delivery(), Vec::new(), None, false);
        assert!(!report.workflow_succeeded);
        assert!(!report.branch_published);
        assert!(!report.independently_verified);
        assert_eq!(report.verdict, Verdict::Inconclusive);
    }

    #[test]
    fn levels_order_weakest_to_strongest() {
        assert!(Reported < Published);
        assert!(Published < Verified);
        assert_eq!(Verified.as_str(), "verified");
    }

    #[test]
    fn a_report_round_trips_through_json_keeping_the_three_claims_apart() {
        let checks = vec![
            Evidence::pass("pr.exists", Published, "PR #7"),
            Evidence::unavailable("tests.rerun", Verified, "no runner"),
        ];
        let report = DeliveryReport::resolve(delivery(), checks, None, false);
        let text = match serde_json::to_string(&report) {
            Ok(text) => text,
            Err(e) => panic!("{e}"),
        };
        assert!(text.contains("\"workflow_succeeded\""), "{text}");
        assert!(text.contains("\"branch_published\""), "{text}");
        assert!(text.contains("\"independently_verified\""), "{text}");
        assert!(text.contains("\"status\":\"unavailable\""), "{text}");
        assert_eq!(
            serde_json::from_str::<DeliveryReport>(&text).ok(),
            Some(report)
        );
    }
}
