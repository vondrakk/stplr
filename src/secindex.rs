// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Secondary indexes — query by a value field, maintained synchronously as posting lists.
//!
//! When a collection has indexed fields, the coordinator keeps an inverted index for each: a set of
//! document keys per field-value. The index *is* a stplr set (a posting list — exactly the substrate
//! primitive), stored in a reserved collection `_idx:<coll>:<field>` keyed by the field value. So a
//! lookup is just reading that set, and the index inherits sharding/replication for free.
//!
//! This module is pure: it computes which index set-ops a write/delete implies. The coordinator
//! applies them via `Cluster::set_add` / `set_remove` in the same request, so the index is updated
//! synchronously and is immediately consistent with the data.

use std::collections::HashMap;

use serde_json::Value;

/// Which fields are indexed, per collection.
#[derive(Default, Clone, Debug)]
pub struct IndexConfig {
    fields: HashMap<String, Vec<String>>,
}

impl IndexConfig {
    /// Parse `coll:field,coll:field2,coll2:field` (comma-separated `collection:field` pairs).
    pub fn parse(spec: &str) -> IndexConfig {
        let mut fields: HashMap<String, Vec<String>> = HashMap::new();
        for entry in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            if let Some((coll, field)) = entry.split_once(':') {
                let (coll, field) = (coll.trim(), field.trim());
                if !coll.is_empty() && !field.is_empty() {
                    fields.entry(coll.to_string()).or_default().push(field.to_string());
                }
            }
        }
        IndexConfig { fields }
    }
    pub fn fields_for(&self, coll: &str) -> &[String] {
        self.fields.get(coll).map(Vec::as_slice).unwrap_or(&[])
    }
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

/// The reserved collection holding the posting list for (coll, field): field-value -> set of keys.
pub fn idx_coll(coll: &str, field: &str) -> String {
    format!("_idx:{coll}:{field}")
}

/// A field's value rendered as an index key. Only scalar fields are indexed.
pub fn field_value(doc: &Value, field: &str) -> Option<String> {
    match doc.get(field) {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Number(n)) => Some(n.to_string()),
        Some(Value::Bool(b)) => Some(b.to_string()),
        _ => None,
    }
}

/// Index ops a write implies: `(removes, adds)`, each `(idx_coll, field_value)` to set_remove /
/// set_add the doc key from/to. Only changed fields produce ops (idempotent re-writes are no-ops).
pub fn diff_write(fields: &[String], coll: &str, old: Option<&Value>, new: &Value) -> (Vec<(String, String)>, Vec<(String, String)>) {
    let mut removes = Vec::new();
    let mut adds = Vec::new();
    for f in fields {
        let oldv = old.and_then(|d| field_value(d, f));
        let newv = field_value(new, f);
        if oldv != newv {
            if let Some(ov) = oldv {
                removes.push((idx_coll(coll, f), ov));
            }
            if let Some(nv) = newv {
                adds.push((idx_coll(coll, f), nv));
            }
        }
    }
    (removes, adds)
}

/// Index ops a delete implies: remove the doc key from every indexed field's posting list.
pub fn diff_delete(fields: &[String], coll: &str, old: &Value) -> Vec<(String, String)> {
    fields.iter().filter_map(|f| field_value(old, f).map(|v| (idx_coll(coll, f), v))).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn config_parse_and_lookup() {
        let c = IndexConfig::parse("users:status, users:region , orders:state");
        assert_eq!(c.fields_for("users"), &["status".to_string(), "region".to_string()]);
        assert_eq!(c.fields_for("orders"), &["state".to_string()]);
        assert!(c.fields_for("other").is_empty());
    }

    #[test]
    fn field_value_scalars_only() {
        let d = json!({"status": "active", "n": 7, "ok": true, "obj": {"x": 1}});
        assert_eq!(field_value(&d, "status"), Some("active".into()));
        assert_eq!(field_value(&d, "n"), Some("7".into()));
        assert_eq!(field_value(&d, "ok"), Some("true".into()));
        assert_eq!(field_value(&d, "obj"), None, "objects aren't indexed");
        assert_eq!(field_value(&d, "missing"), None);
    }

    #[test]
    fn diff_for_insert_update_delete() {
        let fields = vec!["status".to_string()];
        // insert: just an add
        let (rm, add) = diff_write(&fields, "users", None, &json!({"status": "active"}));
        assert!(rm.is_empty());
        assert_eq!(add, vec![("_idx:users:status".into(), "active".into())]);

        // update status active -> banned: remove old, add new
        let (rm, add) = diff_write(&fields, "users", Some(&json!({"status": "active"})), &json!({"status": "banned"}));
        assert_eq!(rm, vec![("_idx:users:status".into(), "active".into())]);
        assert_eq!(add, vec![("_idx:users:status".into(), "banned".into())]);

        // unchanged field -> no index ops
        let (rm, add) = diff_write(&fields, "users", Some(&json!({"status": "active"})), &json!({"status": "active", "other": 1}));
        assert!(rm.is_empty() && add.is_empty());

        // delete: remove from the posting list
        assert_eq!(diff_delete(&fields, "users", &json!({"status": "active"})), vec![("_idx:users:status".into(), "active".into())]);
    }
}
