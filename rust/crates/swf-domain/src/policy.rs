//! Canonical Factory Cell policy identity.
//!
//! Python owns policy construction today; Rust owns the same digest ABI so authority can migrate
//! without changing the coordinate system underneath live Cells.

use std::fmt::Write as _;

use ring::digest::{digest, SHA256};
use serde_json::Value;

pub const POLICY_SCHEMA_VERSION: u64 = 1;
pub const POLICY_DIGEST_FAMILY: &str = "v1";

/// Digest an already-canonical JSON policy mapping using the same domain separation as Python.
pub fn policy_digest(policy: &Value) -> Result<String, serde_json::Error> {
    let canonical = canonical_value(policy);
    let payload = serde_json::to_string(&canonical)?;
    let domain = format!("{POLICY_DIGEST_FAMILY}\0{payload}");
    let hash = digest(&SHA256, domain.as_bytes());
    let mut hex = String::with_capacity(64);
    for byte in hash.as_ref() {
        write!(&mut hex, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(format!("policy:{POLICY_DIGEST_FAMILY}:{hex}"))
}

fn canonical_value(value: &Value) -> Value {
    match value {
        Value::Object(fields) => {
            let mut keys: Vec<&String> = fields.keys().collect();
            keys.sort();
            let mut out = serde_json::Map::new();
            for key in keys {
                out.insert(key.clone(), canonical_value(&fields[key]));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonical_value).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_key_order_never_changes_policy_identity() {
        let left: Value = serde_json::from_str(r#"{"b":2,"a":{"y":2,"x":1}}"#).unwrap();
        let right: Value = serde_json::from_str(r#"{"a":{"x":1,"y":2},"b":2}"#).unwrap();
        assert_eq!(policy_digest(&left).unwrap(), policy_digest(&right).unwrap());
    }
}
