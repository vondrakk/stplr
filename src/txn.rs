// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Single-shard multi-key transactions ("transaction-lite").
//!
//! A [`Txn`] is a set of **preconditions** ([`TxnCheck`] — a key must currently equal an expected
//! value, or be absent) plus a set of **ops** ([`TxnOp`]) applied **all-or-nothing**: if every
//! precondition holds, every op is applied atomically; otherwise nothing changes. Because every
//! operation on a shard runs under the shard's exclusive lock (and, on the durable backend, in one
//! LMDB write transaction), this is genuine **single-shard ACID** — atomic, isolated, durable.
//!
//! Scope, stated plainly: the keys a txn touches must all route to the **same shard**. This is
//! single-shard ACID, not distributed two-phase commit. To force keys onto one shard, give them a
//! shared **hash-tag** `{tag}` — only the substring inside the braces is hashed for routing (the
//! Redis Cluster convention), so `{order:42}:line:1` and `{order:42}:total` co-locate.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A mutation within a transaction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum TxnOp {
    Put { coll: String, key: String, value: Value },
    Delete { coll: String, key: String },
    SetAdd { coll: String, key: String, member: String },
    SetRemove { coll: String, key: String, member: String },
}

impl TxnOp {
    pub fn coll(&self) -> &str {
        match self {
            TxnOp::Put { coll, .. } | TxnOp::Delete { coll, .. } | TxnOp::SetAdd { coll, .. } | TxnOp::SetRemove { coll, .. } => coll,
        }
    }
    pub fn key(&self) -> &str {
        match self {
            TxnOp::Put { key, .. } | TxnOp::Delete { key, .. } | TxnOp::SetAdd { key, .. } | TxnOp::SetRemove { key, .. } => key,
        }
    }
}

/// A precondition: at apply time, `(coll, key)` must equal `expect` (`None` = the key must be absent).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TxnCheck {
    pub coll: String,
    pub key: String,
    #[serde(default)]
    pub expect: Option<Value>,
}

/// An atomic, conditional, multi-key transaction over keys on a single shard.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Txn {
    #[serde(default)]
    pub checks: Vec<TxnCheck>,
    pub ops: Vec<TxnOp>,
}

impl Txn {
    /// Every key the transaction reads or writes — used to verify co-location (all on one shard).
    pub fn keys(&self) -> Vec<&str> {
        let mut ks: Vec<&str> = self.checks.iter().map(|c| c.key.as_str()).collect();
        ks.extend(self.ops.iter().map(|o| o.key()));
        ks
    }
}

/// The routing token for a key: the substring inside the first `{...}` if present and non-empty
/// (Redis-style hash-tag), else the whole key. Keys sharing a tag route to the same shard, so a
/// transaction over them is single-shard.
pub fn route_token(key: &str) -> &str {
    if let Some(open) = key.find('{') {
        if let Some(close_rel) = key[open + 1..].find('}') {
            let inner = &key[open + 1..open + 1 + close_rel];
            if !inner.is_empty() {
                return inner;
            }
        }
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_tag_extraction() {
        assert_eq!(route_token("{order:42}:line:1"), "order:42");
        assert_eq!(route_token("{order:42}:total"), "order:42");
        assert_eq!(route_token("plainkey"), "plainkey"); // no tag -> whole key
        assert_eq!(route_token("{}:x"), "{}:x"); // empty tag -> whole key
    }

    #[test]
    fn txn_keys_collects_checks_and_ops() {
        let t = Txn {
            checks: vec![TxnCheck { coll: "kv".into(), key: "{o}:a".into(), expect: Some(Value::from(1)) }],
            ops: vec![
                TxnOp::Put { coll: "kv".into(), key: "{o}:b".into(), value: Value::from(2) },
                TxnOp::Delete { coll: "kv".into(), key: "{o}:c".into() },
            ],
        };
        assert_eq!(t.keys(), vec!["{o}:a", "{o}:b", "{o}:c"]);
        // all share the hash-tag -> one route token -> co-located
        let tokens: std::collections::HashSet<&str> = t.keys().iter().map(|k| route_token(k)).collect();
        assert_eq!(tokens.len(), 1, "co-located on one shard");
    }

    #[test]
    fn wire_format_round_trips() {
        let t = Txn {
            checks: vec![TxnCheck { coll: "kv".into(), key: "a".into(), expect: None }],
            ops: vec![TxnOp::SetAdd { coll: "tags".into(), key: "a".into(), member: "x".into() }],
        };
        let j = serde_json::to_string(&t).unwrap();
        assert_eq!(serde_json::from_str::<Txn>(&j).unwrap(), t);
    }
}
