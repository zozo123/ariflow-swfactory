//! Durable Factory Cell contracts shared by the operator surfaces.
//!
//! The Python backend is authoritative for persistence/fencing.  These types deliberately mirror
//! its versioned JSON documents so Rust can inspect the same identity without reinterpreting it.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `cell_` plus the first 96 bits of the identity digest, lowercase hex.
pub const CELL_ID_PREFIX: &str = "cell_";
pub const CELL_ID_DIGEST_LEN: usize = 24;
pub const CELL_ID_LEN: usize = CELL_ID_PREFIX.len() + CELL_ID_DIGEST_LEN;

/// One answer to "is this a Factory Cell id", for every reader in this workspace.
///
/// Python's `CellIdentity.stable_id` is the only minter and it produces `cell_` plus 24 lowercase
/// hex characters. That was previously checked ten different ways across the two languages: the
/// operator surface here already required the full shape, while `StageInvocation` and the worker
/// batch tested only the prefix, and the Python readers split between prefix-only and prefix-plus-
/// length. So `cell_`, `cell_zzz` and `cell_` followed by two hundred characters were each valid to
/// some readers and invalid to others -- on the identity every epoch fence is keyed to.
///
/// Lowercase only, deliberately: `is_ascii_hexdigit` would also admit `cell_ABC…`, which the minter
/// cannot produce and which Python's `[0-9a-f]` rejects. A case the two languages disagree about is
/// exactly what the ABI gate in #2256 exists to prevent.
pub fn is_cell_id(value: &str) -> bool {
    value.len() == CELL_ID_LEN
        && value.starts_with(CELL_ID_PREFIX)
        && value[CELL_ID_PREFIX.len()..]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// One durable issue×target lifecycle projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CellRecord {
    pub cell_id: String,
    pub schema_version: u32,
    pub repo: String,
    pub target: String,
    pub issue: String,
    pub epoch: u64,
    pub state: String,
    pub airflow_dag_id: Option<String>,
    pub airflow_run_id: Option<String>,
    pub map_index: Option<i64>,
    pub factory_generation: Option<String>,
    pub policy_digest: Option<String>,
    pub base_sha: Option<String>,
    pub observed_target_sha: Option<String>,
    pub compute: Option<Value>,
    pub cleanup: Option<Value>,
    pub created_at: f64,
    pub updated_at: f64,
}

impl CellRecord {
    /// Whether this projection is in a lifecycle-terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state.as_str(),
            "success" | "failed" | "cancelled" | "rejected" | "cleaned"
        )
    }

    /// Stable `dag/run#index` identity when Airflow has accepted the cell.
    pub fn airflow_identity(&self) -> Option<String> {
        Some(format!(
            "{}/{}#{}",
            self.airflow_dag_id.as_ref()?,
            self.airflow_run_id.as_ref()?,
            self.map_index?
        ))
    }
}

/// One append-only mutation/evidence event from a Factory Cell history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CellEvent {
    pub seq: u64,
    pub epoch: u64,
    pub operation_key: String,
    pub kind: String,
    pub payload: Value,
    pub created_at: f64,
}

#[cfg(test)]
mod cell_id_tests {
    use super::*;

    /// The shape Python's `CellIdentity.stable_id` actually mints.
    const MINTED: &str = "cell_2d711642b726b04401627ca9";

    #[test]
    fn a_minted_id_is_accepted() {
        assert!(is_cell_id(MINTED));
        assert_eq!(MINTED.len(), CELL_ID_LEN);
    }

    #[test]
    fn the_shapes_the_prefix_only_check_used_to_admit_are_refused() {
        // Each of these passed `starts_with("cell_")` and was refused by Python. That split is the
        // whole reason this function exists; if any of them starts passing again the two languages
        // have diverged on cell identity, which is what #2256 is open about.
        for probe in [
            "cell_",
            "cell_1",
            "cell_zzz",
            "cell_abcdefabcdefabcdefabcdefabcdef",
            "cell_0123456789abcdef0123456",   // 23 -- one short
            "cell_0123456789abcdef012345678", // 25 -- one long
        ] {
            assert!(
                probe.starts_with(CELL_ID_PREFIX),
                "probe must test the tightening"
            );
            assert!(!is_cell_id(probe), "{probe} must be refused");
        }
    }

    #[test]
    fn the_alphabet_is_lowercase_hex_and_nothing_else() {
        // `is_ascii_hexdigit` would admit the uppercase form; Python's `[0-9a-f]` does not, and the
        // minter cannot produce it. Admitting it here would be a silent cross-language disagreement.
        assert!(!is_cell_id("cell_0123456789ABCDEF01234567"));
        assert!(!is_cell_id("cell_0123456789abcdef0123456g"));
        assert!(!is_cell_id("cell_0123456789abcdef0123456-"));
        assert!(!is_cell_id("CELL_0123456789abcdef01234567"));
    }

    #[test]
    fn a_prefix_only_implementation_would_fail_these_tests() {
        // Guards the guard: if someone replaces the body with the old check, this is what breaks.
        let prefix_only = |v: &str| v.starts_with(CELL_ID_PREFIX);
        let disagreements = ["cell_", "cell_zzz", "cell_0123456789ABCDEF01234567"];
        for probe in disagreements {
            assert!(prefix_only(probe) && !is_cell_id(probe));
        }
    }
}
