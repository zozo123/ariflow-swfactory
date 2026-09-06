//! Which factory `swf` is talking to, and the promise that the file never holds a secret.
//!
//! A context is the whole environment in one name: the Airflow base URL, the repository the
//! deliveries land in, the sandbox owner, where the committed metrics live, and how to prove who
//! we are. The boundary this module defends is rule 6 of the architecture — **no secrets in the
//! config file**. A context stores the *name* of an environment variable, never its value, and
//! [`show`] redacts on the way out, so a screen share, a `cat`, a support ticket or a
//! `swf context show --json` piped into a bug report can never leak a token.
//!
//! The second thing it defends is the fresh machine. A missing config is not an error: it yields
//! the built-in `local` context, because the first command a new operator runs is `swf doctor`
//! and that command has to *explain what to add* rather than crash on a file that was never
//! written.

use std::collections::BTreeMap;
use std::env;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use swf_adapters::airflow::Auth as WireAuth;

/// The name of the context that exists even when nothing is configured.
pub const BUILTIN_CONTEXT: &str = "local";

/// Where a stock local stack listens (`08-local-stack.md` §A.2).
pub const DEFAULT_AIRFLOW_URL: &str = "http://localhost:8080";

/// The tag the shipped DAGs carry, and the one `list_dags` filters on by default.
pub const DEFAULT_DAG_TAG: &str = "swfactory";

/// Where `metrics.json` files are looked for when a context does not say.
pub const DEFAULT_METRICS_ROOT: &str = ".";

/// The only schema version that exists. An unknown one is refused rather than guessed at: a file
/// written by a newer `swf` may mean something different by the same key.
pub const CONFIG_VERSION: u32 = 1;

/// Keys that may never appear in the file, at any depth.
///
/// Not a warning — a load error. A warning teaches people that a token in the config is a thing
/// you can do if you accept a little yellow text, and it is not.
pub const SECRET_KEYS: &[&str] = &["password", "token", "secret"];

/// Overrides the active context, below `--context` and above the file's `default`.
pub const CONTEXT_ENV: &str = "SWF_CONTEXT";

/// Points at a different config file entirely. Not part of C.1's schema; it exists so a test, a
/// sandbox or a second identity on one machine does not have to write the operator's real file.
pub const CONFIG_ENV: &str = "SWF_CONFIG";

/// The XDG variable [`ContextStore::config_path`] honours before asking `directories`.
pub const XDG_CONFIG_HOME: &str = "XDG_CONFIG_HOME";

/// The file name under the config directory.
pub const CONFIG_FILE: &str = "config.toml";

/// Why a context could not be read, written or chosen.
///
/// The variants exist to carry the exit code (`00-architecture.md` §6): a context that is not
/// there is a `3`, a credential variable that is not set is a `4`, and a file that says something
/// impossible is a `1`.
#[derive(Debug, thiserror::Error)]
pub enum ContextError {
    /// No context by that name, and the operator named it explicitly.
    #[error("no context named {name:?}")]
    NotFound {
        /// What was asked for.
        name: String,
    },

    /// A context by that name already exists and `add` was not told to replace it.
    #[error("context {name:?} already exists")]
    Exists {
        /// The name that is taken.
        name: String,
    },

    /// The file could not be read or written.
    #[error("{path}: {detail}")]
    Io {
        /// The file involved.
        path: String,
        /// The OS's words.
        detail: String,
    },

    /// The file is not the shape this version understands.
    #[error("{path}: {detail}")]
    Invalid {
        /// The file involved.
        path: String,
        /// One sentence saying what is wrong with it.
        detail: String,
    },

    /// A secret was found in the file. Named separately because the fix is different in kind.
    #[error(
        "{path}: {key} must never be stored in the config; store the name of an env var instead"
    )]
    SecretInFile {
        /// The file involved.
        path: String,
        /// The offending key, with its table path.
        key: String,
    },

    /// The context names an environment variable that is not set.
    #[error("${var} is not set")]
    MissingEnv {
        /// The variable the context named.
        var: String,
    },

    /// There is nowhere to put the config file — no `$HOME`, no `$XDG_CONFIG_HOME`.
    #[error("cannot locate a config directory; set ${CONFIG_ENV} to a path")]
    NoConfigDir,
}

impl ContextError {
    /// The `kind` of the `--json` error envelope (`00-architecture.md` §C.2).
    pub fn kind(&self) -> &'static str {
        match self {
            Self::NotFound { .. } => "not_found",
            Self::MissingEnv { .. } => "auth",
            _ => "operational",
        }
    }

    /// The process exit code this error implies.
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::NotFound { .. } => 3,
            Self::MissingEnv { .. } => 4,
            _ => 1,
        }
    }

    /// The `fix:` line, when there is an obvious one.
    pub fn hint(&self) -> Option<String> {
        match self {
            Self::NotFound { .. } => Some("swf context list".to_string()),
            Self::MissingEnv { var } => Some(format!("export {var}=…")),
            Self::SecretInFile { .. } => Some(
                "replace the key with `token_env`/`password_env` naming an env var".to_string(),
            ),
            _ => None,
        }
    }
}

/// How `swf` proves who it is to Airflow.
///
/// Every variant that involves a secret names an environment variable and stops there. There is
/// deliberately no `Token(String)` arm: if the type cannot hold a secret, no future edit can
/// accidentally serialise one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[derive(Default)]
pub enum Auth {
    /// A server configured for anonymous admin access. A deliberate choice, never a default guess.
    #[default]
    None,
    /// A bearer token read from the environment at use time.
    TokenEnv {
        /// The variable holding the token, e.g. `AIRFLOW_TOKEN`.
        var: String,
    },
    /// A username, plus a password read from the environment, exchanged for a JWT at
    /// `POST {base}/auth/token`.
    Basic {
        /// The account name. Not a secret, so it is stored.
        user: String,
        /// The variable holding the password, e.g. `AIRFLOW_PASSWORD`.
        password_env: String,
    },
}

impl Auth {
    /// The word the table and the JSON print.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::TokenEnv { .. } => "token_env",
            Self::Basic { .. } => "basic",
        }
    }

    /// The environment variables this auth needs, so `doctor` can report a missing one by name.
    pub fn env_vars(&self) -> Vec<&str> {
        match self {
            Self::None => Vec::new(),
            Self::TokenEnv { var } => vec![var.as_str()],
            Self::Basic { password_env, .. } => vec![password_env.as_str()],
        }
    }

    /// One redacted line: what kind, and which variables it reads. Never a value.
    pub fn redacted(&self) -> String {
        match self {
            Self::None => "none".to_string(),
            Self::TokenEnv { var } => format!("token_env (${var})"),
            Self::Basic { user, password_env } => format!("basic (user {user}, ${password_env})"),
        }
    }

    /// Read the secrets the environment holds and hand the adapter something it can use.
    ///
    /// This is the only function in the crate that looks at the values, and it returns them
    /// straight into [`WireAuth`] without storing them anywhere a `Debug` or a `Serialize` could
    /// reach.
    pub fn resolve(&self) -> Result<WireAuth, ContextError> {
        match self {
            Self::None => Ok(WireAuth::None),
            Self::TokenEnv { var } => Ok(WireAuth::Token(read_env(var)?)),
            Self::Basic { user, password_env } => Ok(WireAuth::Basic {
                username: user.clone(),
                // An empty password is legal — the simple auth manager accepts one — but an
                // *unset* variable is not: it is the difference between "no password" and
                // "the operator forgot", and only the second is worth a message.
                password: read_env(password_env)?,
            }),
        }
    }
}

/// Read one variable, treating "set but empty" as set.
fn read_env(var: &str) -> Result<String, ContextError> {
    env::var(var).map_err(|_| ContextError::MissingEnv {
        var: var.to_string(),
    })
}

/// One environment `swf` can be pointed at.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Context {
    /// The key under `[contexts]`. Not stored inside the table — it *is* the table's name.
    #[serde(skip)]
    pub name: String,
    /// The full base URL, path prefix included. `/api/v2` and `/auth/token` are appended to it.
    pub airflow_url: String,
    /// Python factory API. Empty explicitly selects legacy direct service access.
    #[serde(default)]
    pub backend_url: String,
    /// `owner/name` of the repository deliveries land in. Absent means `gh` is not wired up.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    /// The islo sandbox owner. Absent means sandbox removal is refused outright.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// Where the committed `metrics.json` files are.
    #[serde(default = "default_metrics_root")]
    pub metrics_root: String,
    /// The DAGs to read. Empty means "ask Airflow for everything tagged [`Context::dag_tag`]".
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dag_ids: Vec<String>,
    /// The tag that identifies factory DAGs on this server.
    #[serde(default = "default_dag_tag")]
    pub dag_tag: String,
    /// How to authenticate. See [`Auth`] — it cannot hold a secret.
    #[serde(default)]
    pub auth: Auth,
}

fn default_metrics_root() -> String {
    DEFAULT_METRICS_ROOT.to_string()
}

fn default_dag_tag() -> String {
    DEFAULT_DAG_TAG.to_string()
}

impl Context {
    /// A context with everything but the URL defaulted.
    pub fn new(name: impl Into<String>, airflow_url: &str) -> Self {
        Self {
            name: name.into(),
            airflow_url: normalize_url(airflow_url),
            backend_url: String::new(),
            repo: None,
            owner: None,
            metrics_root: default_metrics_root(),
            dag_ids: Vec::new(),
            dag_tag: default_dag_tag(),
            auth: Auth::None,
        }
    }

    /// The context a machine with no config file gets.
    ///
    /// It is materialised in memory and never written to disk: writing it would turn "you have
    /// not configured anything yet" into "you configured localhost", and `swf doctor` would stop
    /// being able to tell the operator which of the two they are in.
    pub fn builtin() -> Self {
        let mut context = Self::new(BUILTIN_CONTEXT, DEFAULT_AIRFLOW_URL);
        context.backend_url = "http://localhost:8082".to_string();
        context
    }

    /// True for the in-memory fallback: no file said any of this.
    pub fn is_builtin(&self) -> bool {
        *self == Self::builtin()
    }

    /// Where the committed metrics live, as a path.
    pub fn metrics_path(&self) -> PathBuf {
        PathBuf::from(&self.metrics_root)
    }

    /// The credential, read from the environment at the moment it is needed.
    pub fn airflow_auth(&self) -> Result<WireAuth, ContextError> {
        self.auth.resolve()
    }
}

/// Strip the trailing `/` exactly once, at the edge, so no call site has to think about it.
fn normalize_url(url: &str) -> String {
    url.trim().trim_end_matches('/').to_string()
}

/// The file, as it is on disk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ConfigFile {
    version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    default: Option<String>,
    #[serde(default)]
    contexts: BTreeMap<String, Context>,
}

impl Default for ConfigFile {
    fn default() -> Self {
        Self {
            version: CONFIG_VERSION,
            default: None,
            contexts: BTreeMap::new(),
        }
    }
}

/// The contexts on this machine, and the rules for choosing between them.
#[derive(Debug, Clone)]
pub struct ContextStore {
    path: PathBuf,
    file: ConfigFile,
    exists: bool,
}

impl ContextStore {
    /// Where the config file lives on this machine.
    ///
    /// `$SWF_CONFIG` wins outright. Otherwise `$XDG_CONFIG_HOME/swf/config.toml` when that is set
    /// — on every platform, because an operator who exports XDG_CONFIG_HOME means it — and only
    /// then the platform's own answer via `directories`.
    pub fn config_path() -> Result<PathBuf, ContextError> {
        if let Some(explicit) = env::var_os(CONFIG_ENV).filter(|v| !v.is_empty()) {
            return Ok(PathBuf::from(explicit));
        }
        if let Some(xdg) = env::var_os(XDG_CONFIG_HOME).filter(|v| !v.is_empty()) {
            return Ok(PathBuf::from(xdg).join("swf").join(CONFIG_FILE));
        }
        directories::ProjectDirs::from("", "", "swf")
            .map(|dirs| dirs.config_dir().join(CONFIG_FILE))
            .ok_or(ContextError::NoConfigDir)
    }

    /// Load the config file for this machine, or the empty store when there is none.
    pub fn open() -> Result<Self, ContextError> {
        Self::open_at(Self::config_path()?)
    }

    /// Load one specific file. A missing file is not an error — see the module docs.
    pub fn open_at(path: impl Into<PathBuf>) -> Result<Self, ContextError> {
        let path = path.into();
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self {
                    path,
                    file: ConfigFile::default(),
                    exists: false,
                })
            }
            Err(err) => {
                return Err(ContextError::Io {
                    path: path.display().to_string(),
                    detail: err.to_string(),
                })
            }
        };
        let file = parse(&text, &path)?;
        Ok(Self {
            path,
            file,
            exists: true,
        })
    }

    /// The file this store reads and writes.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// True when the file is really there. False means every context below is the built-in one.
    pub fn exists(&self) -> bool {
        self.exists
    }

    /// Every configured context, by name. Empty configuration answers the built-in `local`, so a
    /// `swf context list` on a fresh machine shows the operator what they are actually pointed at.
    pub fn list(&self) -> Vec<Context> {
        if self.file.contexts.is_empty() {
            return vec![Context::builtin()];
        }
        self.file
            .contexts
            .iter()
            .map(|(name, ctx)| named(name, ctx))
            .collect()
    }

    /// One context by name, or `None`.
    pub fn get(&self, name: &str) -> Option<Context> {
        match self.file.contexts.get(name) {
            Some(ctx) => Some(named(name, ctx)),
            None if name == BUILTIN_CONTEXT && self.file.contexts.is_empty() => {
                Some(Context::builtin())
            }
            None => None,
        }
    }

    /// The `default` key, if the file names one that exists.
    pub fn default_name(&self) -> Option<&str> {
        self.file
            .default
            .as_deref()
            .filter(|name| self.file.contexts.contains_key(*name))
    }

    /// Choose the active context.
    ///
    /// Precedence is `--context` > `$SWF_CONTEXT` > the file's `default` > the only context there
    /// is > the built-in `local` (`00-architecture.md` §C.1). The first two are explicit requests,
    /// so naming a context that does not exist is an error and never a silent fallback: an
    /// operator who typoed `--context prd` must not be quietly pointed at production.
    pub fn resolve(&self, requested: Option<&str>) -> Result<Context, ContextError> {
        let asked = requested
            .map(str::to_string)
            .or_else(|| env::var(CONTEXT_ENV).ok().filter(|v| !v.trim().is_empty()));
        if let Some(name) = asked {
            let name = name.trim().to_string();
            return self.get(&name).ok_or(ContextError::NotFound { name });
        }
        if let Some(name) = self.default_name() {
            if let Some(ctx) = self.get(name) {
                return Ok(ctx);
            }
        }
        if self.file.contexts.len() == 1 {
            if let Some((name, ctx)) = self.file.contexts.iter().next() {
                return Ok(named(name, ctx));
            }
        }
        Ok(Context::builtin())
    }

    /// Add or replace a context and write the file.
    pub fn add(&mut self, context: Context, replace: bool) -> Result<(), ContextError> {
        if !replace && self.file.contexts.contains_key(&context.name) {
            return Err(ContextError::Exists { name: context.name });
        }
        let mut stored = context.clone();
        stored.airflow_url = normalize_url(&stored.airflow_url);
        // The first context added becomes the default: a store with exactly one context and no
        // `default` key resolves to it anyway, and saying so in the file is less surprising.
        if self.file.contexts.is_empty() && self.file.default.is_none() {
            self.file.default = Some(stored.name.clone());
        }
        self.file.contexts.insert(stored.name.clone(), stored);
        self.save()
    }

    /// Make one context the default.
    pub fn use_context(&mut self, name: &str) -> Result<(), ContextError> {
        if !self.file.contexts.contains_key(name) {
            return Err(ContextError::NotFound {
                name: name.to_string(),
            });
        }
        self.file.default = Some(name.to_string());
        self.save()
    }

    /// Forget one context. The `default` key follows it out rather than dangling.
    pub fn remove(&mut self, name: &str) -> Result<(), ContextError> {
        if self.file.contexts.remove(name).is_none() {
            return Err(ContextError::NotFound {
                name: name.to_string(),
            });
        }
        if self.file.default.as_deref() == Some(name) {
            self.file.default = None;
        }
        self.save()
    }

    /// Write the file: same-directory temp file, `0600`, then rename.
    ///
    /// Atomic because a half-written config is a machine that cannot reach its factory, and
    /// `0600` because even a file that holds no secrets holds the shape of someone's estate.
    pub fn save(&self) -> Result<(), ContextError> {
        let io = |detail: String| ContextError::Io {
            path: self.path.display().to_string(),
            detail,
        };
        let text = toml::to_string_pretty(&self.file)
            .map_err(|e| io(format!("cannot render config: {e}")))?;
        // Refuse to write anything a re-read would reject. The check is on the rendered bytes on
        // purpose: it is the file, not the struct, that has to keep the promise.
        parse(&text, &self.path)?;

        let dir = self.path.parent().unwrap_or(Path::new("."));
        std::fs::create_dir_all(dir).map_err(|e| io(e.to_string()))?;
        let temp = dir.join(format!(".{}.{}.tmp", CONFIG_FILE, std::process::id()));
        std::fs::write(&temp, text.as_bytes()).map_err(|e| io(e.to_string()))?;
        restrict(&temp).map_err(|e| io(e.to_string()))?;
        std::fs::rename(&temp, &self.path).map_err(|e| {
            let _ = std::fs::remove_file(&temp);
            io(e.to_string())
        })?;
        Ok(())
    }
}

/// Give a context the name of the table it was read from.
fn named(name: &str, context: &Context) -> Context {
    let mut out = context.clone();
    out.name = name.to_string();
    out.airflow_url = normalize_url(&out.airflow_url);
    out
}

/// Owner-only permissions, where the platform has such a thing.
#[cfg(unix)]
fn restrict(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn restrict(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

/// Parse and vet the file text.
///
/// The secret scan runs before deserialization, so a file with a `token = "ghp_…"` in a table
/// `swf` does not even know about is still refused. That is the point: the rule is about the
/// file, not about the fields this version happens to read.
fn parse(text: &str, path: &Path) -> Result<ConfigFile, ContextError> {
    let shown = path.display().to_string();
    let table: toml::Table = toml::from_str(text).map_err(|e| ContextError::Invalid {
        path: shown.clone(),
        detail: e.message().to_string(),
    })?;
    if let Some(found) = find_secret(&toml::Value::Table(table.clone()), "") {
        return Err(ContextError::SecretInFile {
            path: shown,
            key: found,
        });
    }
    let version = table.get("version").and_then(toml::Value::as_integer);
    match version {
        Some(v) if v == i64::from(CONFIG_VERSION) => {}
        Some(v) => {
            return Err(ContextError::Invalid {
                path: shown,
                detail: format!("version {v} is not supported; this build understands version 1"),
            })
        }
        None => {
            return Err(ContextError::Invalid {
                path: shown,
                detail: "missing `version = 1`".to_string(),
            })
        }
    }
    let file: ConfigFile = toml::Value::Table(table)
        .try_into()
        .map_err(|e: toml::de::Error| ContextError::Invalid {
            path: shown.clone(),
            detail: e.message().to_string(),
        })?;
    if let Some(name) = &file.default {
        if !file.contexts.contains_key(name) {
            return Err(ContextError::Invalid {
                path: shown,
                detail: format!("default = {name:?} names no context"),
            });
        }
    }
    for (name, ctx) in &file.contexts {
        if ctx.airflow_url.trim().is_empty() {
            return Err(ContextError::Invalid {
                path: shown,
                detail: format!("contexts.{name}.airflow_url must not be empty"),
            });
        }
    }
    Ok(file)
}

/// The first forbidden key, with its dotted path, or `None`.
fn find_secret(value: &toml::Value, prefix: &str) -> Option<String> {
    let table = value.as_table()?;
    for (key, child) in table {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        if SECRET_KEYS.contains(&key.to_ascii_lowercase().as_str()) {
            return Some(path);
        }
        if let Some(found) = find_secret(child, &path) {
            return Some(found);
        }
    }
    None
}

/// The human rendering of one context, with every secret already gone.
///
/// There is no unredacted counterpart anywhere in the crate. A "show the real value" flag would
/// be used once in anger, in a screen share, and that is the whole threat model.
pub fn show(context: &Context) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "name          {}", context.name);
    let _ = writeln!(out, "airflow_url   {}", context.airflow_url);
    let _ = writeln!(
        out,
        "repo          {}",
        context.repo.as_deref().unwrap_or("-")
    );
    let _ = writeln!(
        out,
        "owner         {}",
        context.owner.as_deref().unwrap_or("-")
    );
    let _ = writeln!(out, "metrics_root  {}", context.metrics_root);
    let dags = if context.dag_ids.is_empty() {
        format!("(tag {})", context.dag_tag)
    } else {
        context.dag_ids.join(", ")
    };
    let _ = writeln!(out, "dags          {dags}");
    let _ = writeln!(out, "auth          {}", context.auth.redacted());
    if context.is_builtin() {
        let _ = writeln!(
            out,
            "note          built-in fallback; nothing is configured yet"
        );
    }
    out
}

/// The same, as the `--json` document. Same redaction, same absence of any value.
pub fn show_json(context: &Context) -> serde_json::Value {
    let mut auth = serde_json::Map::new();
    auth.insert("kind".into(), context.auth.kind().into());
    match &context.auth {
        Auth::None => {}
        Auth::TokenEnv { var } => {
            auth.insert("token_env".into(), var.clone().into());
        }
        Auth::Basic { user, password_env } => {
            auth.insert("user".into(), user.clone().into());
            auth.insert("password_env".into(), password_env.clone().into());
        }
    }
    serde_json::json!({
        "name": context.name,
        "airflow_url": context.airflow_url,
        "backend_url": context.backend_url,
        "backend_token_env": "SWF_BACKEND_TOKEN",
        "repo": context.repo,
        "owner": context.owner,
        "metrics_root": context.metrics_root,
        "dag_ids": context.dag_ids,
        "dag_tag": context.dag_tag,
        "auth": serde_json::Value::Object(auth),
        "builtin": context.is_builtin(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(text: &str) -> (tempfile::TempDir, ContextStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(CONFIG_FILE);
        std::fs::write(&path, text).expect("write");
        let store = ContextStore::open_at(&path).expect("open");
        (dir, store)
    }

    const SAMPLE: &str = r#"
version = 1
default = "prod"

[contexts.prod]
airflow_url = "https://airflow.example.com/airflow/"
repo = "acme/widgets"
[contexts.prod.auth]
kind = "token_env"
var = "AIRFLOW_TOKEN"

[contexts.staging]
airflow_url = "https://staging.example.com"
[contexts.staging.auth]
kind = "basic"
user = "admin"
password_env = "AIRFLOW_PASSWORD"
"#;

    #[test]
    fn a_missing_file_is_the_builtin_local_context_and_is_not_written() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nowhere").join(CONFIG_FILE);
        let store = ContextStore::open_at(&path).expect("a missing config must not be an error");
        assert!(!store.exists());
        let ctx = store.resolve(None).expect("resolve");
        assert_eq!(ctx.name, BUILTIN_CONTEXT);
        assert_eq!(ctx.airflow_url, DEFAULT_AIRFLOW_URL);
        assert_eq!(ctx.dag_tag, DEFAULT_DAG_TAG);
        assert!(ctx.is_builtin());
        assert!(!path.exists(), "the fallback must never be materialised");
        assert_eq!(store.list().len(), 1);
    }

    #[test]
    fn the_trailing_slash_is_stripped_once_at_the_edge() {
        let (_dir, store) = store(SAMPLE);
        let prod = store.get("prod").expect("prod");
        assert_eq!(prod.airflow_url, "https://airflow.example.com/airflow");
    }

    #[test]
    fn precedence_is_argument_then_env_then_default() {
        let (_dir, store) = store(SAMPLE);
        assert_eq!(store.resolve(Some("staging")).expect("ask").name, "staging");
        assert_eq!(store.resolve(None).expect("default").name, "prod");
        assert_eq!(store.default_name(), Some("prod"));
    }

    #[test]
    fn a_named_context_that_does_not_exist_is_never_silently_replaced() {
        let (_dir, store) = store(SAMPLE);
        let err = store
            .resolve(Some("prd"))
            .expect_err("typo must not resolve");
        assert_eq!(err.exit_code(), 3);
        assert_eq!(err.kind(), "not_found");
    }

    #[test]
    fn one_context_and_no_default_resolves_to_it() {
        let (_dir, store) =
            store("version = 1\n[contexts.only]\nairflow_url = \"http://h:8080\"\n");
        assert_eq!(store.resolve(None).expect("only").name, "only");
    }

    #[test]
    fn a_secret_anywhere_in_the_file_is_a_load_error() {
        for bad in [
            "version = 1\n[contexts.p]\nairflow_url = \"http://h\"\ntoken = \"ghp_xxx\"\n",
            "version = 1\n[contexts.p]\nairflow_url = \"http://h\"\n[contexts.p.auth]\nkind = \"basic\"\nuser = \"a\"\npassword = \"hunter2\"\n",
            "version = 1\nsecret = 1\n[contexts.p]\nairflow_url = \"http://h\"\n",
        ] {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join(CONFIG_FILE);
            std::fs::write(&path, bad).expect("write");
            let err = ContextStore::open_at(&path).expect_err("a secret must refuse the load");
            assert!(matches!(err, ContextError::SecretInFile { .. }), "{err}");
            assert!(err.hint().is_some());
        }
    }

    #[test]
    fn an_unknown_version_is_refused_rather_than_guessed_at() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(CONFIG_FILE);
        std::fs::write(&path, "version = 2\n").expect("write");
        let err = ContextStore::open_at(&path).expect_err("version 2");
        assert_eq!(err.exit_code(), 1);
        std::fs::write(&path, "default = \"x\"\n").expect("write");
        assert!(ContextStore::open_at(&path).is_err(), "version is required");
    }

    #[test]
    fn show_prints_the_variable_name_and_never_its_value() {
        let (_dir, store) = store(SAMPLE);
        let staging = store.get("staging").expect("staging");
        let text = show(&staging);
        assert!(text.contains("AIRFLOW_PASSWORD"), "{text}");
        assert!(text.contains("basic"), "{text}");
        assert!(!text.contains("hunter2"), "{text}");
        let json = show_json(&staging).to_string();
        assert!(json.contains("password_env"), "{json}");
        assert!(!json.contains("hunter2"), "{json}");
        // The redaction is structural: no arm of the type can carry a secret at all.
        assert_eq!(
            staging.auth.redacted(),
            "basic (user admin, $AIRFLOW_PASSWORD)"
        );
    }

    #[test]
    fn a_saved_file_round_trips_and_holds_no_secret() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("swf").join(CONFIG_FILE);
        let mut store = ContextStore::open_at(&path).expect("open");
        let mut ctx = Context::new("prod", "https://airflow.example.com/");
        ctx.repo = Some("acme/widgets".into());
        ctx.auth = Auth::Basic {
            user: "admin".into(),
            password_env: "AIRFLOW_PASSWORD".into(),
        };
        store.add(ctx, false).expect("add");

        let text = std::fs::read_to_string(&path).expect("read back");
        assert!(text.contains("password_env"), "{text}");
        assert!(!text.contains("\npassword ="), "{text}");
        let reopened = ContextStore::open_at(&path).expect("reopen");
        let prod = reopened.get("prod").expect("prod");
        assert_eq!(prod.airflow_url, "https://airflow.example.com");
        assert_eq!(prod.repo.as_deref(), Some("acme/widgets"));
        assert_eq!(reopened.default_name(), Some("prod"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).expect("stat").permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "config must be owner-only");
        }
    }

    #[test]
    fn use_and_remove_keep_the_default_key_honest() {
        let (dir, mut store) = store(SAMPLE);
        let _ = &dir;
        store.use_context("staging").expect("use");
        assert_eq!(store.default_name(), Some("staging"));
        store.remove("staging").expect("remove");
        assert_eq!(store.default_name(), None, "a dangling default is a trap");
        assert!(store.get("staging").is_none());
        assert!(store.remove("staging").is_err());
    }

    #[test]
    fn adding_a_name_twice_needs_an_explicit_replace() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut store = ContextStore::open_at(dir.path().join(CONFIG_FILE)).expect("open");
        store
            .add(Context::new("a", "http://one"), false)
            .expect("first");
        assert!(store.add(Context::new("a", "http://two"), false).is_err());
        store
            .add(Context::new("a", "http://two"), true)
            .expect("replace");
        assert_eq!(store.get("a").expect("a").airflow_url, "http://two");
    }

    #[test]
    fn auth_resolution_names_the_variable_that_is_missing() {
        let auth = Auth::TokenEnv {
            var: "SWF_TEST_TOKEN_THAT_IS_UNSET".into(),
        };
        let err = auth.resolve().expect_err("unset");
        assert_eq!(err.exit_code(), 4, "a missing credential is exit 4");
        assert!(err.to_string().contains("SWF_TEST_TOKEN_THAT_IS_UNSET"));
        assert_eq!(auth.env_vars(), vec!["SWF_TEST_TOKEN_THAT_IS_UNSET"]);
        assert!(matches!(Auth::None.resolve(), Ok(WireAuth::None)));
    }
}
