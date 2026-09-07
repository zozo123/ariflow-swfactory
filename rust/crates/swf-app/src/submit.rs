//! Sending work to the factory, and everything that is checked before anything is posted.
//!
//! `swf submit` is the one operation that *creates* work, so the boundary it defends is the
//! moment of no return: a run that has been triggered exists, consumes budget and has to be
//! stopped by hand. Everything that can be checked locally — the blueprint parses, the issue
//! references are identifiers the factory can turn into branch names and artifact directories, the
//! targets are `owner/name` pairs — is checked *before* the POST, because a 422 from Airflow
//! arrives after the decision and a validation error arrives before it.
//!
//! The blueprint is resolved the way the Python resolves it (`04-dags-blueprints.md` §4.2–4.5):
//! `./blueprints/` first, then the factory root, with `factory` also trying the file stem
//! `default`. A remote operator will often have no checkout at all, and that is not an error —
//! the DAG lives on the server, not on the laptop. A blueprint that cannot be found locally is
//! reported as unresolved and the submission still goes ahead by name; a blueprint that *is* found
//! and does not parse stops the submission, because then we know something is wrong.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use swf_adapters::traits::Runs;
use swf_domain::blueprint::{is_blueprint_name, validate_repo, Blueprint, DEFAULT_BLUEPRINT};
use swf_domain::ids::RunRef;
use tokio_util::sync::CancellationToken;

use crate::ops::{OpsError, Result};

/// The directory blueprints live in, under each search root.
pub const BLUEPRINTS_DIR: &str = "blueprints";

/// Points directly at a blueprints directory, for an operator whose checkout is elsewhere.
pub const BLUEPRINTS_ENV: &str = "SWF_BLUEPRINTS_DIR";

/// Points at a factory checkout; its `blueprints/` is searched after the working directory's.
pub const FACTORY_ROOT_ENV: &str = "SWF_FACTORY_ROOT";

/// Names whose file stem differs from the blueprint name (`_FILE_ALIASES`).
pub const FILE_ALIASES: &[(&str, &str)] = &[("factory", "default")];

/// The longest an issue reference may be, and the character set it may use.
const MAX_ISSUE_CHARS: usize = 128;
const MAX_HARNESS_CHARS: usize = 48;
const MAX_FACTORY_ID_CHARS: usize = 64;

/// What an operator asked the factory to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitRequest {
    /// The issue references. One run, many issues, `issues x targets` jobs.
    pub issues: Vec<String>,
    /// The blueprint name, or a path to a `.toml` file.
    pub blueprint: String,
    /// Repositories to override the blueprint's targets with.
    pub targets: Vec<String>,
    /// AI harness that opened this factory session (codex, claude, grok, custom).
    pub harness: Option<String>,
    /// Stable id for one outer harness session. Reuse it for retries from that session.
    pub factory_id: Option<String>,
}

impl Default for SubmitRequest {
    fn default() -> Self {
        Self {
            issues: Vec::new(),
            blueprint: DEFAULT_BLUEPRINT.to_string(),
            targets: Vec::new(),
            harness: None,
            factory_id: None,
        }
    }
}

impl SubmitRequest {
    /// A request for these issues against the default blueprint.
    pub fn for_issues<I, S>(issues: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            issues: issues.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
    }

    /// Backend admission actor for an AI-harness-owned factory session.
    ///
    /// The pair is all-or-nothing: accepting half an identity would make replay/dedupe ambiguous.
    /// It is deliberately encoded into the existing backend actor field, so old backends keep
    /// working while the current backend gains distinct admission/idempotency identity per session.
    pub fn origin_actor(&self) -> Result<Option<String>> {
        let harness = clean_origin_component(
            self.harness.as_deref(),
            "harness",
            MAX_HARNESS_CHARS,
        )?;
        let factory_id = clean_origin_component(
            self.factory_id.as_deref(),
            "factory id",
            MAX_FACTORY_ID_CHARS,
        )?;
        match (harness, factory_id) {
            (None, None) => Ok(None),
            (Some(harness), Some(factory_id)) => {
                Ok(Some(format!("harness:{harness}:{factory_id}")))
            }
            _ => Err(OpsError::usage(
                "--harness and --factory-id must be supplied together",
            )
            .with_hint(
                "swf submit --harness claude --factory-id claude-session-1 --issue 42",
            )),
        }
    }
}

fn clean_origin_component(
    raw: Option<&str>,
    field: &str,
    max_chars: usize,
) -> Result<Option<String>> {
    let Some(raw) = raw else { return Ok(None) };
    let value = raw.trim();
    if value.is_empty() || value.chars().count() > max_chars {
        return Err(OpsError::usage(format!(
            "{field} must be 1-{max_chars} characters",
        )));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return Err(OpsError::usage(format!(
            "{field} may contain only ASCII letters, digits, dot, underscore and hyphen",
        )));
    }
    Ok(Some(value.to_ascii_lowercase()))
}

/// Which blueprint a submission ran against, and whether it could be read here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlueprintRef {
    /// The name, which is also the DAG id.
    pub name: String,
    /// The file it was read from, when there was one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// False when no local copy was found and the name was taken on trust.
    pub resolved: bool,
}

/// A run that now exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Submission {
    /// The DAG the run belongs to.
    pub dag_id: String,
    /// The run id Airflow assigned. Half of the identity of everything this run produces.
    pub run_id: String,
    /// The UI deep link.
    pub url: String,
    /// The issues, as they were posted.
    pub issues: Vec<String>,
    /// The blueprint, and whether it could be read locally.
    pub blueprint: BlueprintRef,
    /// How many jobs this run should fan out into, when the blueprint could be read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jobs: Option<usize>,
    /// Originating AI harness, when this came through the governed backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    /// Stable outer factory-session id, when this came through the governed backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub factory_id: Option<String>,
}

impl Submission {
    /// The run this submission created.
    pub fn run(&self) -> RunRef {
        RunRef::new(self.dag_id.clone(), self.run_id.clone())
    }
}

/// Validate everything, then trigger exactly one run.
pub async fn submit(
    runs: &dyn Runs,
    request: &SubmitRequest,
    cancel: &CancellationToken,
) -> Result<Submission> {
    if request.origin_actor()?.is_some() {
        return Err(OpsError::usage(
            "AI harness identity requires factory-backend mode; direct Airflow cannot preserve it",
        )
        .with_hint("configure a context with --backend-url and SWF_BACKEND_TOKEN"));
    }
    let issues = clean_issues(&request.issues)?;
    check_targets(&request.targets)?;

    let (blueprint, loaded) = resolve_blueprint(&request.blueprint)?;
    let dag_id = blueprint.name.clone();
    let jobs = loaded.as_ref().map(|bp| bp.job_count(issues.len()));

    let run_id = runs.trigger(&dag_id, &issues, cancel).await?;
    let run = RunRef::new(dag_id.clone(), run_id.clone());
    Ok(Submission {
        dag_id,
        run_id,
        url: runs.run_url(&run),
        issues,
        blueprint,
        jobs,
        harness: None,
        factory_id: None,
    })
}

/// Trim, drop blanks, reject duplicates, and validate what is left.
///
/// Duplicates are rejected rather than de-duplicated: two of the same issue in one run means the
/// operator's list is not what they think it is, and quietly fixing it hides that.
fn clean_issues(raw: &[String]) -> Result<Vec<String>> {
    let mut out: Vec<String> = Vec::new();
    for value in raw {
        let issue = value.trim();
        if issue.is_empty() {
            continue;
        }
        validate_issue(issue).map_err(OpsError::usage)?;
        if out.iter().any(|seen| seen == issue) {
            return Err(OpsError::usage(format!(
                "issue {issue:?} was given twice; one run answers each issue once"
            )));
        }
        out.push(issue.to_string());
    }
    if out.is_empty() {
        return Err(
            OpsError::usage("submit needs at least one issue").with_hint("swf submit --issue 42")
        );
    }
    Ok(out)
}

/// Targets, checked and then refused by this build.
///
/// The trigger route this client speaks posts `conf.issues` and nothing else, so a `--target`
/// would be silently dropped on the way to Airflow. Hiding an operator's override is precisely the
/// failure mode this product exists to replace, so it is refused loudly instead — and the check
/// still runs first, so a malformed repo is reported as a malformed repo.
fn check_targets(targets: &[String]) -> Result<()> {
    for target in targets {
        validate_repo(target).map_err(OpsError::usage)?;
    }
    if targets.is_empty() {
        return Ok(());
    }
    Err(OpsError::usage(
        "--target cannot be honoured: this client's trigger posts conf.issues only, \
         and dropping the override silently would be worse than refusing it",
    )
    .with_hint("edit the blueprint's [[targets]], or trigger with a blueprint that has them"))
}

/// Read the blueprint if it is here, and say honestly whether it was.
fn resolve_blueprint(name_or_path: &str) -> Result<(BlueprintRef, Option<Blueprint>)> {
    let name = name_or_path.trim();
    if name.is_empty() {
        return Err(OpsError::usage("blueprint name must not be empty"));
    }
    let by_path = Path::new(name)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("toml"));

    match find_blueprint(name) {
        Some(path) => {
            let blueprint = load_file(&path, if by_path { None } else { Some(name) })?;
            Ok((
                BlueprintRef {
                    name: blueprint.name.clone(),
                    path: Some(path.display().to_string()),
                    resolved: true,
                },
                Some(blueprint),
            ))
        }
        // A named blueprint that is not on this machine is the normal case for a remote operator:
        // the DAG lives on the server. A *path* that is not there is a typo, and is refused.
        None if by_path => Err(OpsError::not_found(format!(
            "blueprint file not found: {name}"
        ))),
        None => {
            if !is_blueprint_name(name) {
                return Err(OpsError::usage(format!("invalid blueprint name: {name:?}")));
            }
            Ok((
                BlueprintRef {
                    name: name.to_string(),
                    path: None,
                    resolved: false,
                },
                None,
            ))
        }
    }
}

/// Parse one blueprint file, prefixing every complaint with the path that caused it.
fn load_file(path: &Path, expected_name: Option<&str>) -> Result<Blueprint> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| OpsError::operational(format!("{}: {e}", path.display())))?;
    let blueprint = Blueprint::from_toml(&text)
        .map_err(|e| OpsError::operational(format!("{}: {e}", path.display())))?;
    if let Some(expected) = expected_name {
        if blueprint.name != expected {
            return Err(OpsError::operational(format!(
                "{}: blueprint.name is {:?}, expected {:?}",
                path.display(),
                blueprint.name,
                expected
            )));
        }
    }
    Ok(blueprint)
}

/// Where a blueprint might be, in the order the Python looks: cwd first, then the factory root.
pub fn blueprint_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(dir) = std::env::var_os(BLUEPRINTS_ENV).filter(|v| !v.is_empty()) {
        roots.push(PathBuf::from(dir));
    }
    if let Ok(cwd) = std::env::current_dir() {
        roots.push(cwd.join(BLUEPRINTS_DIR));
    }
    if let Some(root) = std::env::var_os(FACTORY_ROOT_ENV).filter(|v| !v.is_empty()) {
        roots.push(PathBuf::from(root).join(BLUEPRINTS_DIR));
    }
    roots
}

/// Every blueprint file directly under one root, sorted by path (`blueprint_paths`).
pub fn blueprint_paths(root: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    let mut paths: Vec<PathBuf> = entries
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_file()
                && path
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("toml"))
        })
        .collect();
    paths.sort();
    paths
}

/// Find the file for a name or a path: root-major, stem-minor, first hit wins.
pub fn find_blueprint(name_or_path: &str) -> Option<PathBuf> {
    let direct = Path::new(name_or_path);
    if direct
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("toml"))
    {
        return direct.is_file().then(|| direct.to_path_buf());
    }
    if name_or_path.is_empty()
        || !name_or_path
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return None;
    }
    let mut stems = vec![name_or_path.to_string()];
    for (name, stem) in FILE_ALIASES {
        if *name == name_or_path {
            stems.push((*stem).to_string());
        }
    }
    for root in blueprint_roots() {
        for stem in &stems {
            let candidate = root.join(format!("{stem}.toml"));
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

/// What `conf.issues` may hold, mirroring `Blueprint.jobs`:
///
/// ```python
/// issues.append(value if value.isdigit() else normalize_relative_path(value, field="conf.issues"))
/// ```
///
/// An earlier version of this applied `paths.validate_identifier` here — the rule that governs the
/// *derived* issue id, the one that becomes `factory/<issue>-<run>` and `docs/factory/<issue>/`.
/// That rule forbids `/`, which silently made the factory's own documented entry point
/// (`--issue demo/issue.md`, a path to a front-matter file) unsubmittable. The identifier rule
/// still applies, but three stages later and to a value the factory derives, not to the reference
/// an operator types. Validating the wrong layer is how a client refuses work the server would
/// have accepted.
pub fn validate_issue(value: &str) -> std::result::Result<(), String> {
    if value.is_empty() || value.chars().count() > MAX_ISSUE_CHARS {
        return Err(format!("issue must be 1-{MAX_ISSUE_CHARS} characters"));
    }
    // A bare issue number is the common case and needs no path reasoning at all.
    if value.chars().all(|c| c.is_ascii_digit()) {
        return Ok(());
    }
    // Otherwise it is a path to an issue file, held to `normalize_relative_path`: it has to stay
    // inside the checkout, and it has to be unambiguous on every platform that will read it.
    let bad = |why: &str| format!("issue {value:?} must be an issue number or a path {why}");
    if value.chars().any(|c| c.is_control()) {
        return Err(bad("without control characters"));
    }
    if value.trim() != value || value.contains('\\') {
        return Err(bad("that is a clean POSIX relative path"));
    }
    if value.starts_with('/') || value.chars().nth(1) == Some(':') {
        return Err(bad("that is relative, not absolute"));
    }
    if value.split('/').any(|part| part == "..") {
        return Err(bad("that stays inside its root"));
    }
    if value == "." || value.split('/').all(|p| p.is_empty() || p == ".") {
        return Err(bad("that names a file"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_issue_reference_is_a_number_or_a_path_to_an_issue_file() {
        // The factory's own documented entry point is a path, and the demo and the stress line
        // both submit one. A client that refuses these refuses the product.
        for good in [
            "42",
            "demo/issue.md",
            "demo/issue2.md",
            "PROJ-17",
            "a.b_c-9",
            "docs/issues/deep/nested.md",
        ] {
            assert!(validate_issue(good).is_ok(), "{good} must be accepted");
        }
        // Traversal and platform ambiguity stay refused: this value is joined onto a checkout.
        for bad in [
            "",
            ".",
            "..",
            "../etc/passwd",
            "demo/../../etc/passwd",
            "/etc/passwd",
            "C:\\Windows",
            "demo\\issue.md",
            " demo/issue.md",
            "demo/issue.md ",
        ] {
            assert!(validate_issue(bad).is_err(), "{bad:?} must be refused");
        }
        assert!(validate_issue(&"a".repeat(128)).is_ok());
        assert!(validate_issue(&"a".repeat(129)).is_err());
    }

    /// The Python is the authority on this, so pin the two branches of its own expression:
    /// `value if value.isdigit() else normalize_relative_path(value, field="conf.issues")`.
    #[test]
    fn the_digit_branch_and_the_path_branch_match_the_python_expression() {
        assert!(
            validate_issue("007").is_ok(),
            "a digit string never sees path rules"
        );
        assert!(
            validate_issue("1/2").is_ok(),
            "not all-digits, so it is judged as a path"
        );
        assert!(
            validate_issue("..").is_err(),
            "not all-digits, and traversal is refused"
        );
    }

    #[test]
    fn issues_are_trimmed_blanks_dropped_and_duplicates_refused() {
        let cleaned = clean_issues(&[" 42 ".into(), "".into(), "43".into()]).expect("clean");
        assert_eq!(cleaned, vec!["42".to_string(), "43".to_string()]);

        let err = clean_issues(&["42".into(), "42".into()]).expect_err("duplicate");
        assert_eq!(err.exit_code(), 2);
        assert!(err.message.contains("twice"), "{}", err.message);

        let empty = clean_issues(&["  ".into()]).expect_err("nothing to do");
        assert_eq!(empty.exit_code(), 2);
        assert!(empty.hint.is_some());
    }

    #[test]
    fn a_target_is_validated_first_and_then_refused_loudly() {
        assert!(check_targets(&[]).is_ok());
        let malformed = check_targets(&["not-a-repo".into()]).expect_err("shape");
        assert!(malformed.message.contains("owner/name"), "{malformed}");
        let refused = check_targets(&["acme/widgets".into()]).expect_err("unsupported");
        assert_eq!(refused.exit_code(), 2);
        assert!(
            refused.message.contains("conf.issues"),
            "the refusal must say why: {refused}"
        );
    }

    #[test]
    fn a_missing_blueprint_file_is_not_found_but_a_missing_name_is_taken_on_trust() {
        let err = resolve_blueprint("/nowhere/at/all.toml").expect_err("path");
        assert_eq!(err.exit_code(), 3);

        let (reference, blueprint) =
            resolve_blueprint("no-such-blueprint-here").expect("a name is not a promise");
        assert!(!reference.resolved);
        assert_eq!(reference.name, "no-such-blueprint-here");
        assert!(blueprint.is_none());

        let bad_name = resolve_blueprint("../etc/passwd").expect_err("not a name");
        assert_eq!(bad_name.exit_code(), 2);
    }

    #[test]
    fn a_blueprint_that_is_here_and_does_not_parse_stops_the_submission() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("broken.toml");
        std::fs::write(&path, "this is not toml =").expect("write");
        let err = resolve_blueprint(&path.display().to_string()).expect_err("broken");
        assert_eq!(err.exit_code(), 1);
        assert!(err.message.contains("broken.toml"), "{err}");
    }

    #[test]
    fn blueprint_paths_lists_only_toml_files_directly_under_the_root() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("b.toml"), "").expect("write");
        std::fs::write(dir.path().join("a.toml"), "").expect("write");
        std::fs::write(dir.path().join("notes.md"), "").expect("write");
        std::fs::create_dir(dir.path().join("nested")).expect("mkdir");
        std::fs::write(dir.path().join("nested").join("c.toml"), "").expect("write");

        let paths = blueprint_paths(dir.path());
        let names: Vec<String> = paths
            .iter()
            .filter_map(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
            .collect();
        assert_eq!(names, vec!["a.toml", "b.toml"], "sorted, non-recursive");
        assert!(blueprint_paths(Path::new("/nope/nope")).is_empty());
    }

    #[test]
    fn harness_origin_is_paired_validated_and_stable() {
        let mut request = SubmitRequest::for_issues(["42"]);
        assert_eq!(request.origin_actor().expect("legacy"), None);

        request.harness = Some("Claude".into());
        let missing = request.origin_actor().expect_err("pair required");
        assert_eq!(missing.exit_code(), 2);

        request.factory_id = Some("Session-17".into());
        assert_eq!(
            request.origin_actor().expect("origin").as_deref(),
            Some("harness:claude:session-17")
        );
        assert_eq!(
            request.origin_actor().expect("replay").as_deref(),
            Some("harness:claude:session-17")
        );

        request.factory_id = Some("not/a/session".into());
        assert!(request.origin_actor().is_err());
    }

}
