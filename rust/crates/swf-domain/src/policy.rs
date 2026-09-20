//! Canonical Factory Cell policy identity.
//!
//! Python owns policy construction today; Rust owns the same digest ABI so authority can migrate
//! without changing the coordinate system underneath live Cells.
//!
//! The v1 byte contract is UTF-8 canonical JSON: sorted object keys, compact separators, raw
//! Unicode/DEL where JSON permits it, and numbers constrained to the exact cross-language range.
//! The numeric bound prevents a large Python integer from collapsing through an f64 representation
//! before Rust hashes it.

use std::fmt::Write as _;

use ring::digest::{digest, SHA256};
use serde_json::Value;

pub const POLICY_SCHEMA_VERSION: u64 = 1;
pub const POLICY_DIGEST_FAMILY: &str = "v1";
pub const POLICY_MAX_EXACT_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("policy number exceeds cross-language exact range +/-{POLICY_MAX_EXACT_INTEGER}")]
    InvalidNumber,
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Digest an already-canonical JSON policy mapping using the same domain separation as Python.
pub fn policy_digest(policy: &Value) -> Result<String, PolicyError> {
    let canonical = canonical_value(policy)?;
    let payload = serde_json::to_string(&canonical)?;
    let domain = format!("{POLICY_DIGEST_FAMILY}\0{payload}");
    let hash = digest(&SHA256, domain.as_bytes());
    let mut hex = String::with_capacity(64);
    for byte in hash.as_ref() {
        write!(&mut hex, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(format!("policy:{POLICY_DIGEST_FAMILY}:{hex}"))
}

fn canonical_value(value: &Value) -> Result<Value, PolicyError> {
    match value {
        Value::Object(fields) => {
            let mut keys: Vec<&String> = fields.keys().collect();
            keys.sort();
            let mut out = serde_json::Map::new();
            for key in keys {
                out.insert(key.clone(), canonical_value(&fields[key])?);
            }
            Ok(Value::Object(out))
        }
        Value::Array(items) => items
            .iter()
            .map(canonical_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Number(number) => {
            let valid = if let Some(value) = number.as_i64() {
                value.unsigned_abs() <= POLICY_MAX_EXACT_INTEGER
            } else if let Some(value) = number.as_u64() {
                value <= POLICY_MAX_EXACT_INTEGER
            } else if let Some(value) = number.as_f64() {
                value.is_finite() && value.abs() <= POLICY_MAX_EXACT_INTEGER as f64
            } else {
                false
            };
            if !valid {
                return Err(PolicyError::InvalidNumber);
            }
            Ok(Value::Number(number.clone()))
        }
        other => Ok(other.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_key_order_never_changes_policy_identity() {
        let left: Value = serde_json::from_str(r#"{"b":2,"a":{"y":2,"x":1}}"#).unwrap();
        let right: Value = serde_json::from_str(r#"{"a":{"x":1,"y":2},"b":2}"#).unwrap();
        assert_eq!(
            policy_digest(&left).unwrap(),
            policy_digest(&right).unwrap()
        );
    }

    #[test]
    fn unicode_and_del_use_utf8_json_bytes() {
        let unicode: Value = serde_json::from_str(r#"{"schema_version":1,"target":"src@früh"}"#).unwrap();
        assert_eq!(
            policy_digest(&unicode).unwrap(),
            "policy:v1:1a980255174b80d62ed78b13ef57bf458480f5bbf254af514a6f6e3fc2d4a39d"
        );

        let del = serde_json::json!({"repo": "acme/re\u{7f}po", "schema_version": 1});
        assert_eq!(
            policy_digest(&del).unwrap(),
            "policy:v1:4bde36b2ab958acb2bb7fb8f416be23a8dda25fe91763d444638d201a6ce8d8b"
        );
    }

    #[test]
    fn numbers_outside_the_exact_cross_language_range_are_refused() {
        assert!(policy_digest(&serde_json::json!({"n": POLICY_MAX_EXACT_INTEGER})).is_ok());
        assert!(policy_digest(&serde_json::json!({"n": POLICY_MAX_EXACT_INTEGER + 1})).is_err());
        assert!(
            policy_digest(&serde_json::json!({"n": -(POLICY_MAX_EXACT_INTEGER as i64) - 1}))
                .is_err()
        );
    }
}
