//! The `blueprints/*.toml` schema, and the rules a blueprint must satisfy before it is a DAG.
//!
//! A blueprint is the only place a human declares what the factory will do to a repository, so
//! this module's boundary is *refusal*: an unknown key is an error rather than a silent no-op, a
//! stage order that skips a prerequisite is rejected before Airflow ever builds a task, and a
//! `[policy]` block cannot hand an agent shell access or turn a read-only stage into a writing
//! one. Every one of those is a rule someone would otherwise discover at 3 a.m. with a half-built
//! pull request.
//!
//! The messages are the Python's, word for word, because operators grep for them and the two
//! implementations have to be interchangeable while both exist.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The stages a blueprint may declare, in the only order they may appear in.
pub const CANONICAL_ORDER: &[&str] = &[
    "intent",
    "spec",
    "plan",
    "build_and_test",
    "review",
    "deliver",
];

/// The only stages a human gate may follow.
///
/// Gates exist to catch a wrong *direction* early, while a course correction is cheap. A gate
/// after `build_and_test` would ask a human to approve work that has already been done, which is
/// theatre, so the schema does not allow one.
pub const GATE_STAGES: &[&str] = &["intent", "plan"];

/// The agent-policy stage names — deliberately not [`CANONICAL_ORDER`].
///
/// `build` and `fix` are two policies of the one `build_and_test` stage, and `diagnose` has no
/// stage of its own at all. Conflating the two vocabularies is the mistake this constant exists
/// to make impossible.
pub const POLICY_STAGES: &[&str] = &["spec", "plan", "build", "fix", "review", "diagnose"];

/// The policy stages whose agents may write files. Everything else is read-only, and a blueprint
/// may not promote it.
const POLICY_WRITE_STAGES: &[&str] = &["build", "fix"];

/// File-editing tools a `[policy]` block is allowed to name at all.
const POLICY_TOOLS: &[&str] = &[
    "Read",
    "Grep",
    "Glob",
    "Edit",
    "Write",
    "MultiEdit",
    "NotebookEdit",
];

/// Tools that only a writing stage may be granted.
const WRITE_TOOLS: &[&str] = &["Edit", "MultiEdit", "NotebookEdit", "Write"];

/// The top-level TOML tables a blueprint may contain, sorted as the error message prints them.
const SECTIONS: &[&str] = &[
    "blueprint",
    "deliver",
    "gates",
    "limits",
    "policy",
    "review",
    "sandbox",
    "stages",
    "targets",
    "trigger",
];

/// Which artifact each gate is allowed to show. A gate that displays the wrong file is a gate
/// that asks for approval of something other than what it names.
const GATE_ARTIFACTS: &[(&str, &[&str])] = &[
    ("intent", &["intent.md"]),
    ("plan", &["plan.json", "plan.md"]),
];

/// The blueprint loaded when no `--blueprint` is given. Its *file* is `default.toml`.
pub const DEFAULT_BLUEPRINT: &str = "factory";

/// The name a blueprint (and therefore a DAG, and therefore an islo sandbox) may carry.
pub const NAME_PATTERN: &str = "^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$";

/// Why a blueprint was refused.
///
/// Both variants render as the bare message: a blueprint error is read by a human editing a TOML
/// file, and prefixing it with a Rust type name helps nobody.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BlueprintError {
    /// The file is not valid TOML.
    #[error("{0}")]
    Syntax(String),
    /// The file parsed but says something the factory will not do.
    #[error("{0}")]
    Invalid(String),
}

impl BlueprintError {
    fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }
}

/// How a DAG run gets started.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TriggerKind {
    /// A human or `swf submit` triggers it.
    #[default]
    Manual,
    /// Airflow's scheduler triggers it, which is why `cron` becomes required.
    Cron,
}

/// `[trigger]` — when the line runs, and on what if nobody says.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Trigger {
    /// `manual` or `cron`.
    pub kind: TriggerKind,
    /// The cron expression, required when `kind = "cron"`.
    pub cron: Option<String>,
    /// Issues a scheduled run works on when the run conf names none. Runtime conf always wins.
    pub issues: Vec<String>,
}

/// `[[targets]]` — one repository (and directory inside it) the line works on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    /// `owner/name`, validated against GitHub's own character rules.
    pub repo: String,
    /// The directory inside the repo, `""` for the root.
    #[serde(default)]
    pub dir: String,
    /// The branch a delivery is cut from and opened against.
    #[serde(default = "default_base_branch")]
    pub base_branch: String,
}

fn default_base_branch() -> String {
    "main".to_string()
}

/// `[[gates]]` — one human approval, after one stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateSpec {
    /// The stage this gate follows. Must be in `stages.order` and in [`GATE_STAGES`].
    pub after: String,
    /// The artifact the approver is shown.
    pub artifact: String,
    /// How long Airflow holds the gate open before falling back.
    #[serde(default = "default_timeout_h")]
    pub timeout_h: u32,
    /// HITL user ids; empty means anyone may answer.
    #[serde(default)]
    pub assigned: Vec<String>,
    /// True for an unattended line: the gate answers itself, recorded as actor `auto`.
    #[serde(default)]
    pub auto: bool,
}

fn default_timeout_h() -> u32 {
    24
}

/// `[limits]` — the budget a single job may spend before the line gives up.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    /// How many times `build_and_test` may loop.
    pub max_build_iterations: u32,
    /// How many review-driven fix rounds are allowed.
    pub max_review_fixes: u32,
    /// The agent's per-call turn cap.
    pub max_turns: u32,
    /// Dollars one stage may spend.
    pub budget_usd_per_stage: f64,
    /// Dollars one JOB (issue x target) may spend — not one DAG run.
    pub budget_usd: f64,
    /// `execution_timeout` of every stage task.
    pub stage_timeout_h: u32,
    /// `max_active_tis_per_dagrun`, i.e. concurrent sandboxes.
    pub max_parallel_jobs: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_build_iterations: 3,
            max_review_fixes: 1,
            max_turns: 40,
            budget_usd_per_stage: 2.0,
            budget_usd: 8.0,
            stage_timeout_h: 3,
            max_parallel_jobs: 4,
        }
    }
}

/// `[policy.<stage>]` — an **additive** relaxation of one agent policy.
///
/// Only additive: there is no way to remove a denied tool from a blueprint, because the deny list
/// is the sandbox's floor and a file in a repository must not be able to lower it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PolicyOverride {
    /// A model override for this stage.
    pub model: Option<String>,
    /// Extra path-scoped file/search tools. Shell, web and MCP tools are refused.
    pub extra_allowed_tools: Vec<String>,
}

/// `[review]` — where the review policy lives and how many nits survive it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReviewSpec {
    /// A path in the factory repo rendered into the review prompt; the target's own file wins.
    pub policy: String,
    /// How many `nit` findings survive into the review report.
    pub nit_cap: u32,
}

impl Default for ReviewSpec {
    fn default() -> Self {
        Self {
            policy: "REVIEW.md".to_string(),
            nit_cap: 3,
        }
    }
}

/// Where a job's agent actually runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxKind {
    /// The worker's own filesystem — harness use only.
    Local,
    /// The `srt` npm sandbox.
    Srt,
    /// islo MicroVMs. The production boundary, hence the default.
    #[default]
    Islo,
    /// A local container.
    Docker,
    /// Airflow's own `SandboxBackend` abstraction.
    Toolset,
}

/// `[sandbox]` — the isolation boundary and its lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SandboxSpec {
    /// Which sandbox implementation to use.
    pub kind: SandboxKind,
    /// The islo gateway profile the sandbox's egress policy comes from.
    pub gateway_profile: String,
    /// The islo environment.
    pub environment: String,
    /// Seconds until the provider deletes the sandbox. Must outlive the longest gate.
    pub ttl_s: u64,
    /// Seconds of idleness before the provider pauses it.
    pub idle_s: u64,
    /// An islo snapshot to warm-start from.
    pub snapshot: Option<String>,
    /// The `toolset` backend name, or `package.module:Class`.
    pub backend: String,
    /// Where the repository is exposed inside the sandbox.
    pub workdir: String,
}

impl Default for SandboxSpec {
    fn default() -> Self {
        Self {
            kind: SandboxKind::default(),
            gateway_profile: "swfactory".to_string(),
            environment: "swfactory".to_string(),
            ttl_s: 172_800,
            idle_s: 900,
            snapshot: None,
            backend: "sbx".to_string(),
            workdir: "/workspace/repo".to_string(),
        }
    }
}

fn default_labels() -> Vec<String> {
    vec!["factory".to_string(), "agent-authored".to_string()]
}

fn default_version() -> u32 {
    1
}

/// One blueprint: one TOML file, one Airflow DAG, one `--blueprint` name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Blueprint {
    /// The DAG id and the CLI name.
    pub name: String,
    /// The schema version. Only `1` exists.
    #[serde(default = "default_version")]
    pub version: u32,
    /// One line for a human reading `swf submit --help`.
    #[serde(default)]
    pub description: String,
    /// When the line runs.
    #[serde(default)]
    pub trigger: Trigger,
    /// The repositories the line works on. At least one.
    pub targets: Vec<Target>,
    /// `[stages] order` — lifted to the top level, as the Python model has it.
    pub order: Vec<String>,
    /// The human gates.
    #[serde(default)]
    pub gates: Vec<GateSpec>,
    /// The per-job budget.
    #[serde(default)]
    pub limits: Limits,
    /// Additive agent-policy relaxations, keyed by [`POLICY_STAGES`] name.
    #[serde(default)]
    pub policy: BTreeMap<String, PolicyOverride>,
    /// The review policy.
    #[serde(default)]
    pub review: ReviewSpec,
    /// The isolation boundary.
    #[serde(default)]
    pub sandbox: SandboxSpec,
    /// `[deliver] labels` — lifted to the top level. `factory:blocked` is appended at runtime.
    #[serde(default = "default_labels")]
    pub labels: Vec<String>,
}

impl Blueprint {
    /// Parse and fully validate a blueprint from TOML text.
    ///
    /// There is no "parse without validating" entry point on purpose. A half-checked blueprint is
    /// the thing that builds a DAG which then fails on its ninth task, and the whole reason this
    /// schema is strict is to move that failure to the moment someone edits the file.
    pub fn from_toml(text: &str) -> Result<Self, BlueprintError> {
        let root: toml::Table =
            toml::from_str(text).map_err(|e| BlueprintError::Syntax(e.to_string()))?;
        let flat = flatten(root)?;
        let mut blueprint: Self = toml::Value::Table(flat)
            .try_into()
            .map_err(|e: toml::de::Error| BlueprintError::invalid(e.message().to_string()))?;
        blueprint.normalize_and_check()?;
        Ok(blueprint)
    }

    /// The longest gate this blueprint declares, in hours. `0` when it declares none.
    ///
    /// The sandbox has to outlive this or a job's MicroVM is deleted while a human is still
    /// deciding, and the job resumes into nothing.
    pub fn gate_timeout_h(&self) -> u32 {
        self.gates.iter().map(|g| g.timeout_h).max().unwrap_or(0)
    }

    /// The gate that follows one stage, if any.
    pub fn gate_after(&self, stage: &str) -> Option<&GateSpec> {
        self.gates.iter().find(|g| g.after == stage)
    }

    /// How many jobs one run of this blueprint produces for `issues` issues: issues x targets.
    pub fn job_count(&self, issues: usize) -> usize {
        issues * self.targets.len()
    }

    /// Normalise the path-shaped fields in place, then run every cross-field rule.
    ///
    /// Order matters and mirrors the Python: stage order, then gates, then budgets, then the
    /// sandbox lifetime, then policy. The first failure is the one reported, and a reader fixing
    /// their file top-down meets them in the order they wrote them.
    fn normalize_and_check(&mut self) -> Result<(), BlueprintError> {
        if !is_blueprint_name(&self.name) {
            return Err(BlueprintError::invalid(format!(
                "blueprint.name must match {NAME_PATTERN}, not {}",
                py_repr(&self.name)
            )));
        }
        if self.version != 1 {
            return Err(BlueprintError::invalid(format!(
                "blueprint.version must be 1, not {}",
                self.version
            )));
        }
        self.normalize_trigger()?;
        self.normalize_targets()?;
        for gate in &mut self.gates {
            gate.artifact = normalize_relative_path(&gate.artifact, "gates.artifact", false)
                .map_err(BlueprintError::invalid)?;
            if gate.timeout_h < 1 {
                return Err(BlueprintError::invalid("gates.timeout_h must be >= 1"));
            }
        }
        self.review.policy = normalize_relative_path(&self.review.policy, "review.policy", false)
            .map_err(BlueprintError::invalid)?;
        self.normalize_sandbox()?;

        self.check_order()?;
        self.check_gates()?;
        self.check_limits()?;
        self.check_ttl()?;
        self.check_policy()?;
        Ok(())
    }

    fn normalize_trigger(&mut self) -> Result<(), BlueprintError> {
        if self.trigger.kind == TriggerKind::Cron
            && self
                .trigger
                .cron
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .is_empty()
        {
            return Err(BlueprintError::invalid(
                "trigger.kind='cron' requires trigger.cron",
            ));
        }
        let mut seen: Vec<String> = Vec::new();
        for raw in &self.trigger.issues {
            let value = raw.trim();
            if value.is_empty() {
                return Err(BlueprintError::invalid(
                    "trigger.issues entries must not be empty",
                ));
            }
            // A bare issue number is a GitHub reference, not a path, so it is kept verbatim.
            let issue = if value.bytes().all(|b| b.is_ascii_digit()) {
                value.to_string()
            } else {
                normalize_relative_path(value, "trigger.issues", false)
                    .map_err(BlueprintError::invalid)?
            };
            if !seen.contains(&issue) {
                seen.push(issue);
            }
        }
        self.trigger.issues = seen;
        Ok(())
    }

    fn normalize_targets(&mut self) -> Result<(), BlueprintError> {
        if self.targets.is_empty() {
            return Err(BlueprintError::invalid(
                "targets must contain at least 1 item",
            ));
        }
        for target in &mut self.targets {
            validate_repo(&target.repo).map_err(BlueprintError::invalid)?;
            target.dir = normalize_relative_path(&target.dir, "targets.dir", true)
                .map_err(BlueprintError::invalid)?;
            validate_git_ref(&target.base_branch, "targets.base_branch")
                .map_err(BlueprintError::invalid)?;
        }
        Ok(())
    }

    fn normalize_sandbox(&mut self) -> Result<(), BlueprintError> {
        let backend = self.sandbox.backend.trim();
        if backend.is_empty() || backend.chars().any(char::is_whitespace) {
            return Err(BlueprintError::invalid(
                "sandbox.backend must be a name or package.module:Class",
            ));
        }
        self.sandbox.backend = backend.to_string();
        self.sandbox.workdir =
            normalize_absolute_posix_path(&self.sandbox.workdir, "sandbox.workdir")
                .map_err(BlueprintError::invalid)?;
        if self.sandbox.ttl_s < 1 || self.sandbox.idle_s < 1 {
            return Err(BlueprintError::invalid(
                "sandbox.ttl_s and sandbox.idle_s must be >= 1",
            ));
        }
        Ok(())
    }

    /// `stages.order` must be a strictly increasing subsequence of [`CANONICAL_ORDER`].
    ///
    /// Not a permutation and not a multiset: the stages feed each other, so `plan` before `spec`
    /// is not a preference, it is a line that cannot work. Omitting a stage is allowed — that is
    /// what makes `hotfix` a real line and not a special case in code.
    fn check_order(&self) -> Result<(), BlueprintError> {
        if self.order.is_empty() {
            return Err(BlueprintError::invalid("stages.order must not be empty"));
        }
        let unknown: Vec<&String> = self
            .order
            .iter()
            .filter(|s| !CANONICAL_ORDER.contains(&s.as_str()))
            .collect();
        if !unknown.is_empty() {
            return Err(BlueprintError::invalid(format!(
                "stages.order has unknown stages {}; known: {}",
                py_list(unknown.iter().map(|s| s.as_str())),
                py_tuple(CANONICAL_ORDER.iter().copied()),
            )));
        }
        let positions: Vec<usize> = self
            .order
            .iter()
            .filter_map(|s| CANONICAL_ORDER.iter().position(|c| c == s))
            .collect();
        let strictly_increasing = positions.windows(2).all(|w| w[0] < w[1]);
        if !strictly_increasing {
            return Err(BlueprintError::invalid(format!(
                "stages.order {} must be a subsequence of {} (canonical order, no repeats)",
                py_list(self.order.iter().map(String::as_str)),
                py_list(CANONICAL_ORDER.iter().copied()),
            )));
        }
        if self.order.first().map(String::as_str) != Some("intent") {
            return Err(BlueprintError::invalid(
                "stages.order must start with 'intent'",
            ));
        }
        if self.order.last().map(String::as_str) != Some("deliver") {
            return Err(BlueprintError::invalid(
                "stages.order must end with 'deliver'",
            ));
        }
        let has = |stage: &str| self.order.iter().any(|s| s == stage);
        if (has("build_and_test") || has("review")) && !has("plan") {
            return Err(BlueprintError::invalid(
                "stages.order needs 'plan' before build_and_test or review",
            ));
        }
        if has("review") && !has("build_and_test") {
            return Err(BlueprintError::invalid(
                "stages.order needs 'build_and_test' before review",
            ));
        }
        Ok(())
    }

    fn check_gates(&self) -> Result<(), BlueprintError> {
        let mut seen: Vec<&str> = Vec::new();
        for gate in &self.gates {
            if seen.contains(&gate.after.as_str()) {
                return Err(BlueprintError::invalid(format!(
                    "more than one gate after {}",
                    py_repr(&gate.after)
                )));
            }
            seen.push(&gate.after);
            if !self.order.contains(&gate.after) {
                return Err(BlueprintError::invalid(format!(
                    "gate after {} is not in stages.order {}",
                    py_repr(&gate.after),
                    py_list(self.order.iter().map(String::as_str)),
                )));
            }
            if !GATE_STAGES.contains(&gate.after.as_str()) {
                return Err(BlueprintError::invalid(format!(
                    "gates may only follow {}, not {}",
                    py_list(GATE_STAGES.iter().copied()),
                    py_repr(&gate.after),
                )));
            }
            let allowed = GATE_ARTIFACTS
                .iter()
                .find(|(stage, _)| *stage == gate.after)
                .map(|(_, artifacts)| *artifacts)
                .unwrap_or(&[]);
            if !allowed.contains(&gate.artifact.as_str()) {
                return Err(BlueprintError::invalid(format!(
                    "gate after {} must show one of {}, not {}",
                    py_repr(&gate.after),
                    py_list(allowed.iter().copied()),
                    py_repr(&gate.artifact),
                )));
            }
        }
        Ok(())
    }

    fn check_limits(&self) -> Result<(), BlueprintError> {
        let l = &self.limits;
        if l.max_build_iterations < 1 || l.max_turns < 1 || l.stage_timeout_h < 1 {
            return Err(BlueprintError::invalid(
                "limits.max_build_iterations, limits.max_turns and limits.stage_timeout_h \
                 must be >= 1",
            ));
        }
        if l.max_parallel_jobs < 1 {
            return Err(BlueprintError::invalid(
                "limits.max_parallel_jobs must be >= 1",
            ));
        }
        if !(l.budget_usd_per_stage.is_finite() && l.budget_usd.is_finite()) {
            return Err(BlueprintError::invalid("limits budgets must be finite"));
        }
        if l.budget_usd_per_stage <= 0.0 || l.budget_usd <= 0.0 {
            return Err(BlueprintError::invalid("limits budgets must be > 0"));
        }
        if l.budget_usd_per_stage > l.budget_usd {
            return Err(BlueprintError::invalid(
                "limits.budget_usd_per_stage must not exceed limits.budget_usd",
            ));
        }
        Ok(())
    }

    /// The sandbox must outlive the longest gate, strictly.
    ///
    /// Equal is not enough: a sandbox deleted at the exact hour the gate expires races the
    /// scheduler, and the job resumes into a MicroVM that no longer exists.
    fn check_ttl(&self) -> Result<(), BlueprintError> {
        let gate_h = self.gate_timeout_h();
        if self.sandbox.ttl_s <= u64::from(gate_h) * 3600 {
            return Err(BlueprintError::invalid(format!(
                "sandbox.ttl_s ({}) must exceed the longest gate timeout ({gate_h} h)",
                self.sandbox.ttl_s,
            )));
        }
        Ok(())
    }

    fn check_policy(&self) -> Result<(), BlueprintError> {
        let unknown: Vec<&str> = self
            .policy
            .keys()
            .map(String::as_str)
            .filter(|k| !POLICY_STAGES.contains(k))
            .collect();
        if !unknown.is_empty() {
            return Err(BlueprintError::invalid(format!(
                "policy overrides for unknown stages {}; known: {}",
                py_list(unknown.iter().copied()),
                py_list(POLICY_STAGES.iter().copied()),
            )));
        }
        for (stage, override_) in &self.policy {
            for tool in &override_.extra_allowed_tools {
                if !is_path_scoped_tool(tool.trim()) {
                    return Err(BlueprintError::invalid(
                        "policy tools must be path-scoped file/search tools; shell, task, web, \
                         MCP, commas, and malformed matchers are forbidden",
                    ));
                }
            }
            let writes = POLICY_WRITE_STAGES.contains(&stage.as_str());
            let unsafe_tools: Vec<&str> = override_
                .extra_allowed_tools
                .iter()
                .map(|t| t.trim())
                .filter(|t| {
                    let head = t.split('(').next().unwrap_or(t);
                    head == "Bash" || (!writes && WRITE_TOOLS.contains(&head))
                })
                .collect();
            if !unsafe_tools.is_empty() {
                return Err(BlueprintError::invalid(format!(
                    "policy.{stage} cannot add shell access or escalate a read-only stage: {}",
                    py_list(unsafe_tools.iter().copied()),
                )));
            }
        }
        Ok(())
    }
}

/// Check the top-level sections and lift `[blueprint]`, `[stages]` and `[deliver]` into the shape
/// the model expects.
///
/// The allow-list runs before anything else so a typo'd table name (`[limit]`) is reported as
/// exactly that, instead of surfacing later as "limits has the default values" — a silent wrong
/// answer being the failure this whole schema is built to avoid.
fn flatten(root: toml::Table) -> Result<toml::Table, BlueprintError> {
    let mut unknown: Vec<String> = root
        .keys()
        .filter(|k| !SECTIONS.contains(&k.as_str()))
        .cloned()
        .collect();
    if !unknown.is_empty() {
        unknown.sort();
        return Err(BlueprintError::invalid(format!(
            "unknown blueprint sections {}; known: {}",
            py_list(unknown.iter().map(String::as_str)),
            py_list(SECTIONS.iter().copied()),
        )));
    }

    let mut flat = toml::Table::new();
    for (key, value) in root {
        match key.as_str() {
            "blueprint" => {
                let toml::Value::Table(fields) = value else {
                    return Err(BlueprintError::invalid("[blueprint] must be a table"));
                };
                for (field, entry) in fields {
                    flat.insert(field, entry);
                }
            }
            "stages" => {
                flat.insert("order".into(), lift(value, "stages", "order")?);
            }
            "deliver" => {
                flat.insert("labels".into(), lift(value, "deliver", "labels")?);
            }
            _ => {
                flat.insert(key, value);
            }
        }
    }
    Ok(flat)
}

/// Pull the single allowed key out of a one-key section, rejecting anything else in it.
fn lift(value: toml::Value, section: &str, key: &str) -> Result<toml::Value, BlueprintError> {
    let toml::Value::Table(mut fields) = value else {
        return Err(BlueprintError::invalid(format!(
            "[{section}] must be a table"
        )));
    };
    let mut unknown: Vec<String> = fields.keys().filter(|k| *k != key).cloned().collect();
    if !unknown.is_empty() {
        unknown.sort();
        return Err(BlueprintError::invalid(format!(
            "unknown [{section}] keys {}; known: {}",
            py_list(unknown.iter().map(String::as_str)),
            py_list([key]),
        )));
    }
    fields
        .remove(key)
        .ok_or_else(|| BlueprintError::invalid(format!("[{section}] must define {key}")))
}

/// `^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$` — an islo-compatible id, hand-rolled so the domain crate
/// does not pull in a regex engine to check four names.
pub fn is_blueprint_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 63 {
        return false;
    }
    let Some((first, rest)) = bytes.split_first() else {
        return false;
    };
    if !first.is_ascii_alphanumeric() {
        return false;
    }
    rest.iter()
        .all(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'-')
}

/// `^(Read|Grep|Glob|Edit|Write|MultiEdit|NotebookEdit)(?:\([^,\r\n]*\))?$`.
///
/// The comma ban is the point: a matcher containing one would be read as two tools by the agent
/// runtime, and the second could be anything at all.
fn is_path_scoped_tool(tool: &str) -> bool {
    for name in POLICY_TOOLS {
        let Some(rest) = tool.strip_prefix(name) else {
            continue;
        };
        if rest.is_empty() {
            return true;
        }
        if let Some(inner) = rest.strip_prefix('(').and_then(|r| r.strip_suffix(')')) {
            if !inner.contains([',', '\r', '\n']) {
                return true;
            }
        }
    }
    false
}

/// `owner/name`, using only characters GitHub itself allows, and no reserved path component.
pub fn validate_repo(value: &str) -> Result<(), String> {
    let parts: Vec<&str> = value.split('/').collect();
    let shape = "repo must be an owner/name pair using GitHub-safe characters".to_string();
    if parts.len() != 2 {
        return Err(shape);
    }
    for part in &parts {
        let bytes = part.as_bytes();
        if bytes.is_empty() || bytes.len() > 100 {
            return Err(shape);
        }
        let Some((first, rest)) = bytes.split_first() else {
            return Err(shape);
        };
        if !first.is_ascii_alphanumeric() {
            return Err(shape);
        }
        if !rest
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        {
            return Err(shape);
        }
    }
    if parts
        .iter()
        .any(|p| *p == "." || *p == ".." || p.ends_with(".lock"))
    {
        return Err("repo contains a reserved path component".to_string());
    }
    Ok(())
}

/// The characters `_INVALID_REF_RE` actually rejects — see [`validate_git_ref`].
const REF_SUSPECT: &[char] = &['~', '^', ':', '?', '*', '\\', '['];

/// Refuse a branch name that git or a shell would read as something else.
///
/// **This mirrors a bug in the Python on purpose.** `_INVALID_REF_RE` there reads as "reject any
/// of `\x00-\x20~^:?*\[]`", but Python's `re` closes the character class early, so the compiled
/// pattern only fires on one of those characters *immediately followed by `]`*. `a:b` and
/// `has space` are accepted by `swfactory` today. Tightening it here would make `swf` reject
/// blueprints the Python accepts, which is a worse failure than the loose check while both
/// implementations are live; the explicit clauses below carry the real protection.
pub fn validate_git_ref(value: &str, field: &str) -> Result<(), String> {
    let bad = || format!("{field} is not a safe Git ref: {}", py_repr(value));
    if value.is_empty() || value == "@" {
        return Err(bad());
    }
    let chars: Vec<char> = value.chars().collect();
    if chars
        .windows(2)
        .any(|w| (w[0] <= '\u{20}' || REF_SUSPECT.contains(&w[0])) && w[1] == ']')
    {
        return Err(bad());
    }
    if value.starts_with('-')
        || value.starts_with('/')
        || value.ends_with('/')
        || value.ends_with('.')
        || value.contains("..")
        || value.contains("//")
        || value.contains("@{")
    {
        return Err(bad());
    }
    if value
        .split('/')
        .any(|part| part.is_empty() || part.starts_with('.') || part.ends_with(".lock"))
    {
        return Err(bad());
    }
    Ok(())
}

/// Normalise a path that must stay inside its root.
///
/// `..` is rejected rather than resolved: a blueprint is a repository file, and the moment it can
/// name a path outside its checkout, cloning a repository becomes a way to read the machine that
/// clones it.
pub fn normalize_relative_path(
    value: &str,
    field: &str,
    allow_empty: bool,
) -> Result<String, String> {
    if has_control(value) {
        return Err(format!("{field} contains control characters"));
    }
    if value.is_empty() {
        return if allow_empty {
            Ok(String::new())
        } else {
            Err(format!("{field} must not be empty"))
        };
    }
    if value != value.trim() || value.contains('\\') || has_drive_letter(value) {
        return Err(format!("{field} must be a clean POSIX relative path"));
    }
    if value.starts_with('/') || value.split('/').any(|p| p == "..") {
        return Err(format!(
            "{field} must stay inside its root: {}",
            py_repr(value)
        ));
    }
    let normalized = join_components(value.split('/'));
    if normalized.is_empty() {
        return if allow_empty {
            Ok(String::new())
        } else {
            Err(format!("{field} must name a path"))
        };
    }
    Ok(normalized)
}

/// Normalise a path inside a sandbox, which must be absolute and must not be the root itself.
pub fn normalize_absolute_posix_path(value: &str, field: &str) -> Result<String, String> {
    if has_control(value) {
        return Err(format!("{field} contains control characters"));
    }
    if value != value.trim() || value.contains('\\') || has_drive_letter(value) {
        return Err(format!("{field} must be a clean absolute POSIX path"));
    }
    if !value.starts_with('/') || value.split('/').any(|p| p == "..") || value == "/" {
        return Err(format!("{field} must be an absolute sandbox path below /"));
    }
    let normalized = join_components(value.split('/'));
    if normalized.is_empty() {
        return Err(format!("{field} must be an absolute sandbox path below /"));
    }
    Ok(format!("/{normalized}"))
}

/// Drop empty and `.` components the way `PurePosixPath` does — without resolving `..`, which
/// callers have already refused.
fn join_components<'a>(parts: impl Iterator<Item = &'a str>) -> String {
    parts
        .filter(|p| !p.is_empty() && *p != ".")
        .collect::<Vec<_>>()
        .join("/")
}

fn has_control(value: &str) -> bool {
    value.chars().any(|c| c < '\u{20}' || c == '\u{7f}')
}

fn has_drive_letter(value: &str) -> bool {
    let mut chars = value.chars();
    matches!((chars.next(), chars.next()), (Some(c), Some(':')) if c.is_ascii_alphabetic())
}

/// Python's `repr()` of a string, so a quoted value in an error message looks the same in both
/// implementations.
fn py_repr(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// Python's `repr()` of a list of strings.
fn py_list<'a>(items: impl IntoIterator<Item = &'a str>) -> String {
    let body = items
        .into_iter()
        .map(py_repr)
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{body}]")
}

/// Python's `repr()` of a tuple of strings, trailing comma and all for a single element.
fn py_tuple<'a>(items: impl IntoIterator<Item = &'a str>) -> String {
    let parts: Vec<String> = items.into_iter().map(py_repr).collect();
    if parts.len() == 1 {
        format!("({},)", parts[0])
    } else {
        format!("({})", parts.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
[blueprint]
name = "min"

[[targets]]
repo = "owner/name"

[stages]
order = ["intent", "deliver"]
"#;

    fn parse(text: &str) -> Result<Blueprint, BlueprintError> {
        Blueprint::from_toml(text)
    }

    fn err(text: &str) -> String {
        match parse(text) {
            Ok(bp) => panic!("expected a rejection, got {}", bp.name),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn the_minimal_legal_blueprint_takes_every_default() {
        let bp = match parse(MINIMAL) {
            Ok(bp) => bp,
            Err(e) => panic!("{e}"),
        };
        assert_eq!(bp.name, "min");
        assert_eq!(bp.version, 1);
        assert_eq!(bp.order, vec!["intent", "deliver"]);
        assert_eq!(bp.trigger.kind, TriggerKind::Manual);
        assert_eq!(bp.targets[0].dir, "");
        assert_eq!(bp.targets[0].base_branch, "main");
        assert_eq!(bp.limits, Limits::default());
        assert_eq!(bp.sandbox, SandboxSpec::default());
        assert_eq!(bp.review, ReviewSpec::default());
        assert_eq!(bp.labels, vec!["factory", "agent-authored"]);
        assert_eq!(bp.gate_timeout_h(), 0);
        assert_eq!(bp.job_count(3), 3);
    }

    #[test]
    fn an_unknown_section_names_itself_and_the_known_ones() {
        let text = format!("{MINIMAL}\n[limit]\nmax_turns = 1\n");
        assert_eq!(
            err(&text),
            "unknown blueprint sections ['limit']; known: ['blueprint', 'deliver', 'gates', \
             'limits', 'policy', 'review', 'sandbox', 'stages', 'targets', 'trigger']"
        );
    }

    #[test]
    fn one_key_sections_reject_anything_else_in_them() {
        let stages = MINIMAL.replace(
            "order = [\"intent\", \"deliver\"]",
            "order = [\"intent\", \"deliver\"]\nextra = 1",
        );
        assert_eq!(
            err(&stages),
            "unknown [stages] keys ['extra']; known: ['order']"
        );
        let deliver = format!("{MINIMAL}\n[deliver]\nlabels = []\nother = 1\n");
        assert_eq!(
            err(&deliver),
            "unknown [deliver] keys ['other']; known: ['labels']"
        );
    }

    #[test]
    fn an_unknown_key_inside_a_known_section_is_still_an_error() {
        let text = format!("{MINIMAL}\n[limits]\nmax_turnips = 3\n");
        assert!(err(&text).contains("max_turnips"), "{}", err(&text));
    }

    #[test]
    fn stage_order_must_be_a_subsequence_of_the_canonical_order() {
        let with = |order: &str| MINIMAL.replace("[\"intent\", \"deliver\"]", order);
        assert_eq!(err(&with("[]")), "stages.order must not be empty");
        assert!(err(&with("[\"intent\", \"banana\", \"deliver\"]"))
            .starts_with("stages.order has unknown stages ['banana']; known: ('intent', 'spec',"));
        assert!(
            err(&with("[\"intent\", \"plan\", \"spec\", \"deliver\"]")).contains(
                "must be a subsequence of ['intent', 'spec', 'plan', 'build_and_test', \
                       'review', 'deliver'] (canonical order, no repeats)"
            )
        );
        assert!(err(&with("[\"intent\", \"intent\", \"deliver\"]")).contains("no repeats"));
        assert_eq!(
            err(&with("[\"spec\", \"deliver\"]")),
            "stages.order must start with 'intent'"
        );
        assert_eq!(
            err(&with("[\"intent\", \"spec\"]")),
            "stages.order must end with 'deliver'"
        );
        assert_eq!(
            err(&with("[\"intent\", \"build_and_test\", \"deliver\"]")),
            "stages.order needs 'plan' before build_and_test or review"
        );
        assert_eq!(
            err(&with("[\"intent\", \"plan\", \"review\", \"deliver\"]")),
            "stages.order needs 'build_and_test' before review"
        );
    }

    #[test]
    fn gates_may_only_follow_intent_and_plan_and_only_once_each() {
        let gated = |after: &str, artifact: &str| {
            format!(
                "{}\n[[gates]]\nafter = \"{after}\"\nartifact = \"{artifact}\"\ntimeout_h = 1\n",
                MINIMAL.replace(
                    "[\"intent\", \"deliver\"]",
                    "[\"intent\", \"spec\", \"plan\", \"build_and_test\", \"review\", \"deliver\"]"
                )
            )
        };
        assert!(parse(&gated("intent", "intent.md")).is_ok());
        assert_eq!(
            err(&gated("spec", "intent.md")),
            "gates may only follow ['intent', 'plan'], not 'spec'"
        );
        assert_eq!(
            err(&gated("plan", "spec.md")),
            "gate after 'plan' must show one of ['plan.json', 'plan.md'], not 'spec.md'"
        );
        let twice = format!(
            "{}\n[[gates]]\nafter = \"intent\"\nartifact = \"intent.md\"\ntimeout_h = 1\n",
            gated("intent", "intent.md")
        );
        assert_eq!(err(&twice), "more than one gate after 'intent'");
    }

    #[test]
    fn a_gate_after_a_stage_the_line_does_not_run_is_refused() {
        let text = format!(
            "{MINIMAL}\n[[gates]]\nafter = \"plan\"\nartifact = \"plan.md\"\ntimeout_h = 1\n"
        );
        assert_eq!(
            err(&text),
            "gate after 'plan' is not in stages.order ['intent', 'deliver']"
        );
    }

    #[test]
    fn the_sandbox_must_strictly_outlive_the_longest_gate() {
        let text = |ttl: u64| {
            format!(
                "{MINIMAL}\n[[gates]]\nafter = \"intent\"\nartifact = \"intent.md\"\n\
                 timeout_h = 1\n\n[sandbox]\nttl_s = {ttl}\n"
            )
        };
        assert_eq!(
            err(&text(3600)),
            "sandbox.ttl_s (3600) must exceed the longest gate timeout (1 h)"
        );
        assert!(parse(&text(3601)).is_ok());
    }

    #[test]
    fn budgets_are_per_job_and_the_stage_budget_cannot_exceed_them() {
        let text = format!("{MINIMAL}\n[limits]\nbudget_usd_per_stage = 9.0\nbudget_usd = 8.0\n");
        assert_eq!(
            err(&text),
            "limits.budget_usd_per_stage must not exceed limits.budget_usd"
        );
    }

    #[test]
    fn policy_keys_are_agent_stages_not_pipeline_stages() {
        let text = format!("{MINIMAL}\n[policy.build_and_test]\nextra_allowed_tools = []\n");
        assert_eq!(
            err(&text),
            "policy overrides for unknown stages ['build_and_test']; known: ['spec', 'plan', \
             'build', 'fix', 'review', 'diagnose']"
        );
    }

    #[test]
    fn a_policy_cannot_grant_shell_or_promote_a_read_only_stage() {
        let with = |stage: &str, tool: &str| {
            format!("{MINIMAL}\n[policy.{stage}]\nextra_allowed_tools = [\"{tool}\"]\n")
        };
        // Shell never passes the tool matcher at all.
        assert!(err(&with("build", "Bash(ls)")).starts_with("policy tools must be path-scoped"));
        assert!(err(&with("review", "WebFetch")).starts_with("policy tools must be path-scoped"));
        assert!(err(&with("review", "Read(a,b)")).starts_with("policy tools must be path-scoped"));
        // A write tool is legal only on a stage that already writes.
        assert_eq!(
            err(&with("review", "Write")),
            "policy.review cannot add shell access or escalate a read-only stage: ['Write']"
        );
        assert!(parse(&with("build", "Write(src/**)")).is_ok());
        assert!(parse(&with("review", "Read(docs/**)")).is_ok());
    }

    #[test]
    fn paths_must_stay_inside_their_root() {
        assert_eq!(
            normalize_relative_path("../etc/passwd", "targets.dir", true),
            Err("targets.dir must stay inside its root: '../etc/passwd'".to_string())
        );
        assert_eq!(
            normalize_relative_path("/abs", "targets.dir", true),
            Err("targets.dir must stay inside its root: '/abs'".to_string())
        );
        assert_eq!(
            normalize_relative_path("a\\b", "targets.dir", true),
            Err("targets.dir must be a clean POSIX relative path".to_string())
        );
        assert_eq!(
            normalize_relative_path("C:/x", "targets.dir", true),
            Err("targets.dir must be a clean POSIX relative path".to_string())
        );
        assert_eq!(
            normalize_relative_path("a\u{1}b", "targets.dir", true),
            Err("targets.dir contains control characters".to_string())
        );
        assert_eq!(
            normalize_relative_path("", "gates.artifact", false),
            Err("gates.artifact must not be empty".to_string())
        );
        assert_eq!(
            normalize_relative_path("", "targets.dir", true),
            Ok(String::new())
        );
        assert_eq!(
            normalize_relative_path("./demo//target/", "targets.dir", true),
            Ok("demo/target".to_string())
        );
        assert_eq!(
            normalize_relative_path(".", "targets.dir", true),
            Ok(String::new())
        );
    }

    #[test]
    fn a_sandbox_workdir_must_be_absolute_and_not_the_root() {
        assert_eq!(
            normalize_absolute_posix_path("/workspace//repo/", "sandbox.workdir"),
            Ok("/workspace/repo".to_string())
        );
        for bad in ["repo", "/", "/a/../b"] {
            assert_eq!(
                normalize_absolute_posix_path(bad, "sandbox.workdir"),
                Err("sandbox.workdir must be an absolute sandbox path below /".to_string()),
                "{bad}"
            );
        }
    }

    #[test]
    fn repo_must_be_an_owner_name_pair() {
        assert!(validate_repo("zozo123/ariflow-swfactory").is_ok());
        for bad in ["nameonly", "a/b/c", "-bad/name", "", "own er/name"] {
            assert!(validate_repo(bad).is_err(), "{bad}");
        }
        assert_eq!(
            validate_repo("owner/name.lock"),
            Err("repo contains a reserved path component".to_string())
        );
    }

    #[test]
    fn git_refs_reject_the_shapes_that_actually_bite() {
        assert!(validate_git_ref("main", "targets.base_branch").is_ok());
        assert!(validate_git_ref("release/1.0", "targets.base_branch").is_ok());
        for bad in [
            "", "@", "-x", "/x", "x/", "x.", "a..b", "a//b", "a@{1}", ".hidden", "a.lock", "x~]",
        ] {
            assert!(
                validate_git_ref(bad, "targets.base_branch").is_err(),
                "{bad} should be rejected"
            );
        }
        // Mirrored bug: these are accepted by the Python and must be accepted here too.
        for loose in ["a:b", "has space", "bad~name", "st*r", "br[ack"] {
            assert!(
                validate_git_ref(loose, "targets.base_branch").is_ok(),
                "{loose} must stay accepted while both implementations are live"
            );
        }
    }

    #[test]
    fn blueprint_names_are_islo_compatible_ids() {
        let long = "a".repeat(63);
        for good in ["factory", "a", "A1", "a-b_c", long.as_str()] {
            assert!(is_blueprint_name(good), "{good}");
        }
        let too_long = "a".repeat(64);
        for bad in ["", "-a", "_a", "a.b", "a/b", too_long.as_str()] {
            assert!(!is_blueprint_name(bad), "{bad}");
        }
        let text = MINIMAL.replace("\"min\"", "\"bad.name\"");
        assert!(
            err(&text).starts_with("blueprint.name must match"),
            "{}",
            err(&text)
        );
    }

    #[test]
    fn cron_triggers_must_carry_a_cron_expression() {
        let text = format!("{MINIMAL}\n[trigger]\nkind = \"cron\"\n");
        assert_eq!(err(&text), "trigger.kind='cron' requires trigger.cron");
        let ok = format!("{MINIMAL}\n[trigger]\nkind = \"cron\"\ncron = \"0 6 * * 1\"\n");
        assert!(parse(&ok).is_ok());
    }

    #[test]
    fn trigger_issues_keep_numbers_verbatim_and_deduplicate() {
        let text =
            format!("{MINIMAL}\n[trigger]\nissues = [\"42\", \"./demo/issue.md\", \"42\"]\n");
        let bp = match parse(&text) {
            Ok(bp) => bp,
            Err(e) => panic!("{e}"),
        };
        assert_eq!(bp.trigger.issues, vec!["42", "demo/issue.md"]);
        let blank = format!("{MINIMAL}\n[trigger]\nissues = [\"  \"]\n");
        assert_eq!(err(&blank), "trigger.issues entries must not be empty");
    }

    #[test]
    fn every_shipped_blueprint_parses_and_says_what_the_spec_says() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../blueprints")
            .canonicalize();
        let Ok(dir) = dir else {
            eprintln!("SKIP: blueprints/ not found next to the workspace");
            return;
        };
        let mut seen: Vec<(String, Blueprint)> = Vec::new();
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) => panic!("cannot read {}: {e}", dir.display()),
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("toml") {
                continue;
            }
            let text = match std::fs::read_to_string(&path) {
                Ok(text) => text,
                Err(e) => panic!("cannot read {}: {e}", path.display()),
            };
            let bp = match Blueprint::from_toml(&text) {
                Ok(bp) => bp,
                Err(e) => panic!("{}: {e}", path.display()),
            };
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_string();
            seen.push((stem, bp));
        }
        seen.sort_by(|a, b| a.0.cmp(&b.0));
        let names: Vec<&str> = seen.iter().map(|(stem, _)| stem.as_str()).collect();
        assert_eq!(names, vec!["default", "hotfix", "stress", "toolset"]);

        let by_stem = |stem: &str| {
            seen.iter()
                .find(|(s, _)| s == stem)
                .map(|(_, bp)| bp.clone())
        };
        let Some(default) = by_stem("default") else {
            panic!("default.toml missing");
        };
        // default.toml is the `factory` DAG — the file stem and the name differ on purpose.
        assert_eq!(default.name, DEFAULT_BLUEPRINT);
        assert_eq!(default.order, CANONICAL_ORDER.to_vec());
        assert_eq!(default.gate_timeout_h(), 24);
        assert_eq!(default.sandbox.kind, SandboxKind::Islo);
        assert_eq!(default.targets.len(), 1);
        assert_eq!(default.targets[0].dir, "demo/target");
        assert_eq!(default.labels, vec!["factory", "agent-authored"]);
        assert!(default.policy.contains_key("build"));

        let Some(hotfix) = by_stem("hotfix") else {
            panic!("hotfix.toml missing");
        };
        assert_eq!(hotfix.name, "hotfix");
        assert_eq!(
            hotfix.order,
            vec!["intent", "plan", "build_and_test", "review", "deliver"]
        );
        assert_eq!(hotfix.gate_after("intent").map(|g| g.auto), Some(true));
        assert_eq!(hotfix.gate_after("plan").map(|g| g.timeout_h), Some(4));
        assert_eq!(hotfix.gate_timeout_h(), 4);

        let Some(stress) = by_stem("stress") else {
            panic!("stress.toml missing");
        };
        assert_eq!(stress.targets.len(), 2);
        assert_eq!(stress.sandbox.kind, SandboxKind::Local);
        assert_eq!(stress.limits.max_parallel_jobs, 2);
        assert!(stress.gates.iter().all(|g| g.auto));
        assert_eq!(stress.job_count(2), 4);

        let Some(toolset) = by_stem("toolset") else {
            panic!("toolset.toml missing");
        };
        assert_eq!(toolset.sandbox.kind, SandboxKind::Toolset);
        assert_eq!(toolset.sandbox.backend, "sbx");
        assert_eq!(toolset.sandbox.workdir, "/workspace/repo");
        assert_eq!(toolset.limits.max_build_iterations, 2);
        assert_eq!(toolset.sandbox.ttl_s, 10_800);
    }
}
