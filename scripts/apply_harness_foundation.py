#!/usr/bin/env python3
from pathlib import Path


def replace(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    if new in text:
        return
    if old not in text:
        raise SystemExit(f"missing expected snippet in {path}: {old[:80]!r}")
    p.write_text(text.replace(old, new, 1))


# swf-app submit contract: optional harness + factory session identity.
replace(
    "rust/crates/swf-app/src/submit.rs",
    'const MAX_ISSUE_CHARS: usize = 128;\n',
    'const MAX_ISSUE_CHARS: usize = 128;\nconst MAX_HARNESS_CHARS: usize = 48;\nconst MAX_FACTORY_ID_CHARS: usize = 64;\n',
)
replace(
    "rust/crates/swf-app/src/submit.rs",
    '    /// Repositories to override the blueprint\'s targets with.\n    pub targets: Vec<String>,\n',
    '    /// Repositories to override the blueprint\'s targets with.\n    pub targets: Vec<String>,\n    /// AI harness that opened this factory session (codex, claude, grok, custom).\n    pub harness: Option<String>,\n    /// Stable id for one outer harness session. Reuse it for retries from that session.\n    pub factory_id: Option<String>,\n',
)
replace(
    "rust/crates/swf-app/src/submit.rs",
    '            blueprint: DEFAULT_BLUEPRINT.to_string(),\n            targets: Vec::new(),\n',
    '            blueprint: DEFAULT_BLUEPRINT.to_string(),\n            targets: Vec::new(),\n            harness: None,\n            factory_id: None,\n',
)
replace(
    "rust/crates/swf-app/src/submit.rs",
    '    pub fn for_issues<I, S>(issues: I) -> Self\n    where\n        I: IntoIterator<Item = S>,\n        S: Into<String>,\n    {\n        Self {\n            issues: issues.into_iter().map(Into::into).collect(),\n            ..Self::default()\n        }\n    }\n}\n',
    '    pub fn for_issues<I, S>(issues: I) -> Self\n    where\n        I: IntoIterator<Item = S>,\n        S: Into<String>,\n    {\n        Self {\n            issues: issues.into_iter().map(Into::into).collect(),\n            ..Self::default()\n        }\n    }\n\n    /// Backend admission actor for an AI-harness-owned factory session.\n    ///\n    /// The pair is all-or-nothing: accepting half an identity would make replay/dedupe ambiguous.\n    /// It is deliberately encoded into the existing backend actor field, so old backends keep\n    /// working while the current backend gains distinct admission/idempotency identity per session.\n    pub fn origin_actor(&self) -> Result<Option<String>> {\n        let harness = clean_origin_component(\n            self.harness.as_deref(),\n            "harness",\n            MAX_HARNESS_CHARS,\n        )?;\n        let factory_id = clean_origin_component(\n            self.factory_id.as_deref(),\n            "factory id",\n            MAX_FACTORY_ID_CHARS,\n        )?;\n        match (harness, factory_id) {\n            (None, None) => Ok(None),\n            (Some(harness), Some(factory_id)) => {\n                Ok(Some(format!("harness:{harness}:{factory_id}")))\n            }\n            _ => Err(OpsError::usage(\n                "--harness and --factory-id must be supplied together",\n            )\n            .with_hint(\n                "swf submit --harness claude --factory-id claude-session-1 --issue 42",\n            )),\n        }\n    }\n}\n\nfn clean_origin_component(\n    raw: Option<&str>,\n    field: &str,\n    max_chars: usize,\n) -> Result<Option<String>> {\n    let Some(raw) = raw else { return Ok(None) };\n    let value = raw.trim();\n    if value.is_empty() || value.chars().count() > max_chars {\n        return Err(OpsError::usage(format!(\n            "{field} must be 1-{max_chars} characters",\n        )));\n    }\n    if !value\n        .chars()\n        .all(|c| c.is_ascii_alphanumeric() || matches!(c, \'.\' | \'_\' | \'-\'))\n    {\n        return Err(OpsError::usage(format!(\n            "{field} may contain only ASCII letters, digits, dot, underscore and hyphen",\n        )));\n    }\n    Ok(Some(value.to_ascii_lowercase()))\n}\n',
)
replace(
    "rust/crates/swf-app/src/submit.rs",
    '    /// How many jobs this run should fan out into, when the blueprint could be read.\n    #[serde(skip_serializing_if = "Option::is_none")]\n    pub jobs: Option<usize>,\n',
    '    /// How many jobs this run should fan out into, when the blueprint could be read.\n    #[serde(skip_serializing_if = "Option::is_none")]\n    pub jobs: Option<usize>,\n    /// Originating AI harness, when this came through the governed backend.\n    #[serde(default, skip_serializing_if = "Option::is_none")]\n    pub harness: Option<String>,\n    /// Stable outer factory-session id, when this came through the governed backend.\n    #[serde(default, skip_serializing_if = "Option::is_none")]\n    pub factory_id: Option<String>,\n',
)
replace(
    "rust/crates/swf-app/src/submit.rs",
    'pub async fn submit(\n    runs: &dyn Runs,\n    request: &SubmitRequest,\n    cancel: &CancellationToken,\n) -> Result<Submission> {\n    let issues = clean_issues(&request.issues)?;\n',
    'pub async fn submit(\n    runs: &dyn Runs,\n    request: &SubmitRequest,\n    cancel: &CancellationToken,\n) -> Result<Submission> {\n    if request.origin_actor()?.is_some() {\n        return Err(OpsError::usage(\n            "AI harness identity requires factory-backend mode; direct Airflow cannot preserve it",\n        )\n        .with_hint("configure a context with --backend-url and SWF_BACKEND_TOKEN"));\n    }\n    let issues = clean_issues(&request.issues)?;\n',
)
replace(
    "rust/crates/swf-app/src/submit.rs",
    '        blueprint,\n        jobs,\n    })\n',
    '        blueprint,\n        jobs,\n        harness: None,\n        factory_id: None,\n    })\n',
)

# Add focused contract tests before the test module closes.
p = Path("rust/crates/swf-app/src/submit.rs")
text = p.read_text()
needle = '    fn harness_origin_is_paired_validated_and_stable()'
if needle not in text:
    insert = '''\n\n    #[test]\n    fn harness_origin_is_paired_validated_and_stable() {\n        let mut request = SubmitRequest::for_issues(["42"]);\n        assert_eq!(request.origin_actor().expect("legacy"), None);\n\n        request.harness = Some("Claude".into());\n        let missing = request.origin_actor().expect_err("pair required");\n        assert_eq!(missing.exit_code(), 2);\n\n        request.factory_id = Some("Session-17".into());\n        assert_eq!(\n            request.origin_actor().expect("origin").as_deref(),\n            Some("harness:claude:session-17")\n        );\n        assert_eq!(\n            request.origin_actor().expect("replay").as_deref(),\n            Some("harness:claude:session-17")\n        );\n\n        request.factory_id = Some("not/a/session".into());\n        assert!(request.origin_actor().is_err());\n    }\n'''
    head, tail = text.rsplit('\n}', 1)
    p.write_text(head + insert + '\n}' + tail)

# Backend-aware Ops uses the first-class /work-orders route and encodes session identity as actor.
replace(
    "rust/crates/swf-app/src/ops.rs",
    '        if let Some(backend) = &self.backend {\n            let mut submitted: Submission = backend\n                .call(\n                    "/work-orders",\n                    serde_json::json!({\n                        "line": request.blueprint,\n                        "issues": request.issues,\n                        "targets": request.targets,\n                    }),\n                    cancel,\n                )\n                .await?;\n            // The backend may see an internal Airflow hostname. Browser links use the context.\n            submitted.url = self.runs()?.run_url(&submitted.run());\n            return Ok(submitted);\n        }\n',
    '        if let Some(backend) = &self.backend {\n            let mut body = serde_json::json!({\n                "line": request.blueprint,\n                "issues": request.issues,\n                "targets": request.targets,\n            });\n            if let Some(actor) = request.origin_actor()? {\n                body["actor"] = serde_json::Value::String(actor);\n            }\n            let mut submitted: Submission = backend.call("/work-orders", body, cancel).await?;\n            // The backend may see an internal Airflow hostname. Browser links use the context.\n            submitted.url = self.runs()?.run_url(&submitted.run());\n            submitted.harness = request.harness.as_ref().map(|v| v.trim().to_ascii_lowercase());\n            submitted.factory_id = request\n                .factory_id\n                .as_ref()\n                .map(|v| v.trim().to_ascii_lowercase());\n            return Ok(submitted);\n        }\n',
)

# CLI flags plus environment fallbacks used by outer AI harnesses.
replace(
    "rust/crates/swf-cli/src/cli.rs",
    '    /// Poll until the run reaches a final state before answering.\n    #[arg(long)]\n    pub wait: bool,\n',
    '    /// AI harness that owns the outer agentic session (codex, claude, grok, custom).\n    #[arg(long, value_name = "NAME")]\n    pub harness: Option<String>,\n\n    /// Stable id for this outer factory session. Pair with --harness; reuse on retries.\n    #[arg(long, value_name = "ID")]\n    pub factory_id: Option<String>,\n\n    /// Poll until the run reaches a final state before answering.\n    #[arg(long)]\n    pub wait: bool,\n',
)
replace(
    "rust/crates/swf-cli/src/exec.rs",
    '    let request = SubmitRequest {\n        issues: args.issues.clone(),\n        blueprint: args.blueprint.clone(),\n        targets: args.targets.clone(),\n    };\n',
    '    let request = SubmitRequest {\n        issues: args.issues.clone(),\n        blueprint: args.blueprint.clone(),\n        targets: args.targets.clone(),\n        harness: args.harness.clone().or_else(|| nonempty_env("SWF_HARNESS")),\n        factory_id: args\n            .factory_id\n            .clone()\n            .or_else(|| nonempty_env("SWF_FACTORY_ID")),\n    };\n',
)
replace(
    "rust/crates/swf-cli/src/exec.rs",
    '// ---------------------------------------------------------------------------- submit\n\nasync fn submit_cmd',
    '// ---------------------------------------------------------------------------- submit\n\nfn nonempty_env(name: &str) -> Option<String> {\n    std::env::var(name).ok().filter(|value| !value.trim().is_empty())\n}\n\nasync fn submit_cmd',
)

# TUI remains an operator surface and therefore uses no AI harness identity by default.
replace(
    "rust/crates/swf-tui/src/effect.rs",
    '            let request = SubmitRequest {\n                issues,\n                blueprint: dag_id,\n                targets: Vec::new(),\n            };\n',
    '            let request = SubmitRequest {\n                issues,\n                blueprint: dag_id,\n                targets: Vec::new(),\n                harness: None,\n                factory_id: None,\n            };\n',
)

# Human output makes origin visible; JSON gets it from Submission serde automatically.
replace(
    "rust/crates/swf-cli/src/render.rs",
    '    fields(&[\n        ("run", format!("{}/{}", sub.dag_id, sub.run_id)),\n        ("issues", joined(&sub.issues)),\n        ("blueprint", blueprint),\n        (\n            "jobs",\n            sub.jobs\n                .map(|n| n.to_string())\n                .unwrap_or_else(|| "-".into()),\n        ),\n        ("url", sub.url.clone()),\n    ])\n',
    '    let mut pairs = vec![\n        ("run", format!("{}/{}", sub.dag_id, sub.run_id)),\n        ("issues", joined(&sub.issues)),\n        ("blueprint", blueprint),\n    ];\n    if let Some(harness) = &sub.harness {\n        pairs.push(("harness", harness.clone()));\n    }\n    if let Some(factory_id) = &sub.factory_id {\n        pairs.push(("factory", factory_id.clone()));\n    }\n    pairs.extend([\n        (\n            "jobs",\n            sub.jobs\n                .map(|n| n.to_string())\n                .unwrap_or_else(|| "-".into()),\n        ),\n        ("url", sub.url.clone()),\n    ]);\n    fields(&pairs)\n',
)

print("harness foundation patch applied")
