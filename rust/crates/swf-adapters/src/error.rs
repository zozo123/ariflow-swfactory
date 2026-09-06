//! One error vocabulary for every service the binary talks to, so an exit code can be promised.
//!
//! `swf`'s exit-code table is documented public surface (`00-architecture.md` §D-E): a script that
//! sees `4` must know it needs a credential and not a retry, and a script that sees `6` must know
//! another operator got there first. That promise can only be kept if the classification happens
//! **once**, at the boundary where the status code is still visible — a `String` error thrown here
//! and pattern-matched in the CLI would be a promise made out of prose.
//!
//! So every adapter answers in these eight words, and the mapping is fixed:
//! `401`/`403 "Invalid JWT token"` → [`AdapterError::Auth`], `404` → [`AdapterError::NotFound`],
//! `409` → [`AdapterError::Conflict`], connect/DNS/TLS → [`AdapterError::Unreachable`], an elapsed
//! deadline → [`AdapterError::Timeout`], and everything the service rejected on its own terms —
//! including a `403 "Forbidden"`, a non-zero `gh` exit and a refused `islo rm` — →
//! [`AdapterError::Status`], which is the *operational* bucket and never invites a retry.
//!
//! [`AdapterError::Cancelled`] is not a failure. It means the operator changed context and this
//! answer is no longer wanted; showing it as an error would train people to distrust the screen.

use std::time::Duration;

/// Every way a source call can end other than with data.
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    /// Nothing answered: DNS, connect, TLS, or a tool that is not on `PATH`. Exit 5.
    #[error("{what} is unreachable: {detail}")]
    Unreachable {
        /// Which service or command was being reached, for a message that names the culprit.
        what: String,
        /// The transport's own words, already trimmed.
        detail: String,
    },

    /// The credential is missing, expired or undecodable. Exit 4, and worth re-minting once.
    #[error("{detail}")]
    Auth {
        /// What the service said, or which env var was unset.
        detail: String,
    },

    /// The thing addressed does not exist. Exit 3.
    #[error("{what}")]
    NotFound {
        /// The identity that was looked up, so the operator can see their own typo.
        what: String,
    },

    /// Someone else got there first, or the evidence moved. Exit 6, and never a crash.
    #[error("{detail}")]
    Conflict {
        /// The service's account of the collision.
        detail: String,
    },

    /// The service understood the request and refused it. Exit 1.
    ///
    /// This is the operational bucket: a `400`, a `403 "Forbidden"`, a `422`, a `5xx`, a non-zero
    /// `gh` exit, and the local refusals in [`crate::islo`] all land here. They have one thing in
    /// common that matters to a caller — repeating the request unchanged will fail again.
    #[error("{detail}")]
    Status {
        /// The HTTP status, when there was one. `None` for a subprocess or a local refusal.
        code: Option<u16>,
        /// One sentence naming what was refused and why.
        detail: String,
    },

    /// The answer arrived and could not be read as what it claimed to be. Exit 1.
    ///
    /// The wording is the Python client's, because these strings reach `Snapshot.errors`.
    #[error("{what} returned non-JSON: {detail}")]
    Decode {
        /// Which response or file failed to parse.
        what: String,
        /// The parser's complaint, or a truncated excerpt of the body.
        detail: String,
    },

    /// The deadline elapsed before the answer did. Exit 5, like any other silence.
    #[error("{what} timed out after {}s", after.as_secs_f64())]
    Timeout {
        /// Which call ran out of time.
        what: String,
        /// The budget that elapsed.
        after: Duration,
    },

    /// The caller stopped wanting the answer. Not a failure — see the module docs.
    #[error("cancelled")]
    Cancelled,
}

/// How long an error message may be before it stops being a message and starts being a body dump.
///
/// The Python client truncates service `detail` at 300 characters (`01-domain-control.md` §5) and
/// these strings end up in `Snapshot.errors`, which is a byte-compatibility surface.
pub const MAX_DETAIL_CHARS: usize = 300;

impl AdapterError {
    /// The `kind` string of the `--json` error envelope (`00-architecture.md` §C.2).
    ///
    /// The CLI renders this verbatim, so it is spelled here once rather than re-derived from a
    /// match arm in another crate that could drift.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Unreachable { .. } | Self::Timeout { .. } => "unreachable",
            Self::Auth { .. } => "auth",
            Self::NotFound { .. } => "not_found",
            Self::Conflict { .. } => "conflict",
            Self::Status { .. } | Self::Decode { .. } | Self::Cancelled => "operational",
        }
    }

    /// The process exit code this error implies.
    ///
    /// `Cancelled` answers `1` only because every code has to be *something*; a cancelled request
    /// is the caller's own doing and must never reach a process boundary.
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Unreachable { .. } | Self::Timeout { .. } => 5,
            Self::Auth { .. } => 4,
            Self::NotFound { .. } => 3,
            Self::Conflict { .. } => 6,
            Self::Status { .. } | Self::Decode { .. } | Self::Cancelled => 1,
        }
    }

    /// True when re-presenting the same request with a fresh credential could plausibly work.
    ///
    /// Only [`AdapterError::Auth`] qualifies, and only once — see [`crate::airflow::AirflowApi`].
    /// A `403 "Forbidden"` is deliberately excluded: it is a permission decision, and retrying a
    /// permission decision is how a client gets itself rate-limited.
    pub fn is_auth(&self) -> bool {
        matches!(self, Self::Auth { .. })
    }

    /// True when the caller abandoned this call.
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }

    /// A local refusal: the adapter itself declined to act. Exit 1, no HTTP status.
    ///
    /// The removal guard in [`crate::islo`] is the reason this exists — refusing to delete a
    /// teammate's sandbox is an operational outcome, not an authentication failure.
    pub fn refused(detail: impl Into<String>) -> Self {
        Self::Status {
            code: None,
            detail: truncate(&detail.into()),
        }
    }

    /// Classify one HTTP status the way `03-airflow-rest.md` §11 says it must be classified.
    ///
    /// `detail` is the service's own `detail` field, already stringified — it is `string | object`
    /// across this API and a `403` can only be told from an expired token by reading it.
    ///
    /// The rendered text mirrors the Python client's `"{method} {url} -> HTTP {code} {detail}"`,
    /// because these strings become `Snapshot.errors` values and that document is diffed in CI.
    pub fn from_status(code: u16, what: &str, detail: &str) -> Self {
        let detail = truncate(detail);
        let full = truncate(&format!("{what} -> HTTP {code} {detail}"));
        match code {
            401 => Self::Auth { detail: full },
            // An undecodable JWT is a 403 with this exact text; every other 403 is a permission
            // decision and must not provoke a re-mint (§11, gotcha 13).
            403 if detail.contains("Invalid JWT token") => Self::Auth { detail: full },
            404 => Self::NotFound { what: full },
            409 => Self::Conflict { detail: full },
            _ => Self::Status {
                code: Some(code),
                detail: full,
            },
        }
    }

    /// Classify a transport failure. `what` names the call so the message is actionable.
    pub fn from_reqwest(what: &str, error: &reqwest::Error) -> Self {
        if error.is_timeout() {
            return Self::Timeout {
                what: what.to_string(),
                after: Duration::ZERO,
            };
        }
        if error.is_decode() || error.is_body() {
            return Self::Decode {
                what: what.to_string(),
                detail: truncate(&error.to_string()),
            };
        }
        Self::Unreachable {
            what: what.to_string(),
            detail: truncate(&error.to_string()),
        }
    }
}

/// Cut a service's words down to a message, on a character boundary, with a visible ellipsis.
///
/// Slicing a `String` by byte index is how a well-meaning truncation panics on the first UTF-8
/// error message it meets, which is exactly when the operator needs it most.
pub fn truncate(detail: &str) -> String {
    let trimmed = detail.trim();
    if trimmed.chars().count() <= MAX_DETAIL_CHARS {
        return trimmed.to_string();
    }
    let mut out: String = trimmed.chars().take(MAX_DETAIL_CHARS).collect();
    out.push('\u{2026}');
    out
}

/// The result every adapter method returns.
pub type Result<T> = std::result::Result<T, AdapterError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_status_map_is_the_one_the_exit_table_promises() {
        assert_eq!(
            AdapterError::from_status(401, "GET /dags", "Not authenticated").exit_code(),
            4
        );
        assert_eq!(
            AdapterError::from_status(401, "x", "Token Expired").kind(),
            "auth"
        );
        assert_eq!(AdapterError::from_status(404, "run", "gone").exit_code(), 3);
        assert_eq!(
            AdapterError::from_status(409, "gate", "answered").exit_code(),
            6
        );
        assert_eq!(
            AdapterError::from_status(422, "trigger", "bad body").exit_code(),
            1
        );
        assert_eq!(
            AdapterError::from_status(500, "x", "boom").kind(),
            "operational"
        );
    }

    #[test]
    fn an_invalid_jwt_is_auth_but_a_plain_forbidden_is_not() {
        let bad_token = AdapterError::from_status(403, "GET /dags", "Invalid JWT token");
        assert!(
            bad_token.is_auth(),
            "an undecodable JWT must provoke exactly one re-mint"
        );

        let forbidden = AdapterError::from_status(403, "PATCH gate", "Forbidden");
        assert!(
            !forbidden.is_auth(),
            "retrying a permission decision is never right"
        );
        assert_eq!(forbidden.exit_code(), 1);

        let respondent = AdapterError::from_status(
            403,
            "PATCH gate",
            "User=amy (id=7) is not a respondent for the task.",
        );
        assert!(!respondent.is_auth());
    }

    #[test]
    fn a_local_refusal_is_operational_and_carries_no_status() {
        let err = AdapterError::refused("'prod-db' was not created by 'me'; refusing");
        assert_eq!(err.exit_code(), 1);
        assert_eq!(err.kind(), "operational");
        assert!(matches!(err, AdapterError::Status { code: None, .. }));
    }

    #[test]
    fn cancellation_is_not_shown_as_a_service_failure() {
        let err = AdapterError::Cancelled;
        assert!(err.is_cancelled());
        assert!(!err.is_auth());
    }

    #[test]
    fn truncation_never_splits_a_character() {
        let wide = "é".repeat(MAX_DETAIL_CHARS + 50);
        let cut = truncate(&wide);
        assert_eq!(cut.chars().count(), MAX_DETAIL_CHARS + 1);
        assert!(cut.ends_with('\u{2026}'));
        assert_eq!(truncate("  spaced  "), "spaced");
    }

    #[test]
    fn a_timeout_reads_as_silence_not_as_a_refusal() {
        let err = AdapterError::Timeout {
            what: "GET /dags".into(),
            after: Duration::from_secs(15),
        };
        assert_eq!(err.exit_code(), 5);
        assert_eq!(err.kind(), "unreachable");
        assert!(err.to_string().contains("15"));
    }
}
