// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Asynchronous multi-DC geo-replication.
//!
//! Ships a stream of mutations from one cluster to a remote one and applies them with a conflict
//! policy. The pieces:
//!
//! - **Wire format** — [`ReplBatch`] of timestamped [`ReplOp`]s, JSON-serializable; the unit a source
//!   ships and a sink applies. `last_seq` lets the source commit its progress (it tails the change
//!   feed via a [consumer group](crate::changefeed), so shipping is resumable).
//! - **Source** — [`tail`] reads new change-feed events for a replication group and packages them.
//!   [`ship_batch`] POSTs a batch to a peer.
//! - **Sink + conflict policy** — [`apply_lww`] applies a batch to a store: KV `Put`/`Delete` use
//!   last-writer-wins by timestamp (a per-key sidecar tracks the last applied time); set ops are
//!   additive (a join/leave always applies — sets converge by union, the right merge for posting
//!   lists). This gives eventual consistency for active-passive *and* active-active topologies.
//!
//! Designed to compose: the live mutation source is the PITR write-ahead log (every mutation,
//! timestamped) or the value-level change feed; the resumable cursor is a change-feed consumer group.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::changefeed::{ChangeEvent, ChangeLog};
use crate::store::IndexStore;

/// Sidecar collection holding the last-applied timestamp per replicated KV key (for LWW).
pub const GEO_TS: &str = "__georep_ts";

/// A replicated mutation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum ReplOp {
    Put { coll: String, key: String, value: Value },
    Delete { coll: String, key: String },
    SetAdd { coll: String, key: String, member: String },
    SetRemove { coll: String, key: String, member: String },
}

/// A mutation plus the wall-clock time it happened at the source (drives LWW).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ReplEntry {
    pub ts_ms: u64,
    pub op: ReplOp,
}

/// A shippable batch: where it came from, the ops, and the source's high-water change-feed seq (so
/// the source can commit its consumer-group offset once the batch is acknowledged).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ReplBatch {
    pub origin: String,
    pub last_seq: u64,
    pub entries: Vec<ReplEntry>,
}

fn ts_key(coll: &str, key: &str) -> String {
    format!("{coll}\u{1}{key}")
}

/// Apply a batch to `store` with last-writer-wins for KV and additive merge for sets. Returns the
/// number of ops applied (an op dropped as stale doesn't count). Safe to replay: a re-applied batch
/// converges to the same state.
pub fn apply_lww(store: &mut dyn IndexStore, batch: &ReplBatch) -> usize {
    let mut applied = 0;
    for e in &batch.entries {
        match &e.op {
            ReplOp::Put { coll, key, value } => {
                if lww_admit(store, coll, key, e.ts_ms) {
                    store.put_object(coll, key, value.clone());
                    applied += 1;
                }
            }
            ReplOp::Delete { coll, key } => {
                if lww_admit(store, coll, key, e.ts_ms) {
                    store.delete_object(coll, key);
                    applied += 1;
                }
            }
            // Set membership converges by union — a join/leave always applies (idempotent).
            ReplOp::SetAdd { coll, key, member } => {
                store.set_add(coll, key, member);
                applied += 1;
            }
            ReplOp::SetRemove { coll, key, member } => {
                store.set_remove(coll, key, member);
                applied += 1;
            }
        }
    }
    applied
}

/// LWW gate: admit a KV write for (coll,key) at `ts_ms` iff it's not older than the last applied,
/// updating the sidecar timestamp when admitted.
fn lww_admit(store: &mut dyn IndexStore, coll: &str, key: &str, ts_ms: u64) -> bool {
    let tk = ts_key(coll, key);
    let last = store.get_object(GEO_TS, &tk).and_then(|v| v.as_u64()).unwrap_or(0);
    if ts_ms < last {
        return false;
    }
    store.put_object(GEO_TS, &tk, Value::from(ts_ms));
    true
}

/// Convert value-level change-feed events into replicated set ops, stamped with `ts_ms`.
pub fn from_change_events(events: &[ChangeEvent], ts_ms: u64) -> Vec<ReplEntry> {
    events
        .iter()
        .map(|e| {
            let op = if e.op == "remove" {
                ReplOp::SetRemove { coll: e.set.clone(), key: e.doc_id.clone(), member: e.value.clone() }
            } else {
                ReplOp::SetAdd { coll: e.set.clone(), key: e.doc_id.clone(), member: e.value.clone() }
            };
            ReplEntry { ts_ms, op }
        })
        .collect()
}

/// Source side: read this group's un-shipped change-feed events into a batch (None when caught up).
/// After the batch is acknowledged by the peer, commit `batch.last_seq` to the group so the next
/// `tail` resumes after it.
pub fn tail(log: &dyn ChangeLog, group: &str, limit: usize, ts_ms: u64, origin: &str) -> Option<ReplBatch> {
    let events = log.read_group(group, limit);
    if events.is_empty() {
        return None;
    }
    let last_seq = events.last().map(|e| e.seq).unwrap_or(0);
    Some(ReplBatch { origin: origin.to_string(), last_seq, entries: from_change_events(&events, ts_ms) })
}

/// Ship a batch to a peer's geo-apply endpoint (`POST {peer}/geo/apply`). Returns Ok on a 2xx.
pub async fn ship_batch(client: &reqwest::Client, peer: &str, batch: &ReplBatch) -> Result<(), String> {
    let resp = client
        .post(format!("{}/geo/apply", peer.trim_end_matches('/')))
        .json(batch)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if resp.status().is_success() {
        Ok(())
    } else {
        Err(format!("peer returned {}", resp.status()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changefeed::MemoryChangeLog;
    use crate::store::MemoryStore;

    fn batch(origin: &str, entries: Vec<ReplEntry>) -> ReplBatch {
        ReplBatch { origin: origin.into(), last_seq: 0, entries }
    }
    fn put(key: &str, v: &str, ts: u64) -> ReplEntry {
        ReplEntry { ts_ms: ts, op: ReplOp::Put { coll: "kv".into(), key: key.into(), value: Value::from(v) } }
    }

    #[test]
    fn lww_newer_wins_older_dropped() {
        let mut s = MemoryStore::new();
        apply_lww(&mut s, &batch("dc1", vec![put("a", "v2", 200)]));
        // an older write for the same key is rejected
        let n = apply_lww(&mut s, &batch("dc2", vec![put("a", "v1", 100)]));
        assert_eq!(n, 0, "older write dropped");
        assert_eq!(s.get_object("kv", "a"), Some(Value::from("v2")));
        // a newer write wins
        apply_lww(&mut s, &batch("dc2", vec![put("a", "v3", 300)]));
        assert_eq!(s.get_object("kv", "a"), Some(Value::from("v3")));
        // replay is convergent (state unchanged)
        apply_lww(&mut s, &batch("dc1", vec![put("a", "v2", 200)]));
        assert_eq!(s.get_object("kv", "a"), Some(Value::from("v3")));
    }

    #[test]
    fn lww_delete_respects_time() {
        let mut s = MemoryStore::new();
        apply_lww(&mut s, &batch("dc1", vec![put("a", "v", 100)]));
        // a delete older than the write does not remove it
        let del_old = ReplEntry { ts_ms: 50, op: ReplOp::Delete { coll: "kv".into(), key: "a".into() } };
        apply_lww(&mut s, &batch("dc2", vec![del_old]));
        assert_eq!(s.get_object("kv", "a"), Some(Value::from("v")), "stale delete ignored");
        // a newer delete removes it
        let del_new = ReplEntry { ts_ms: 200, op: ReplOp::Delete { coll: "kv".into(), key: "a".into() } };
        apply_lww(&mut s, &batch("dc2", vec![del_new]));
        assert_eq!(s.get_object("kv", "a"), None);
    }

    #[test]
    fn sets_replicate_additively() {
        let mut s = MemoryStore::new();
        let e = |m: &str| ReplEntry { ts_ms: 1, op: ReplOp::SetAdd { coll: "tags".into(), key: "d1".into(), member: m.into() } };
        apply_lww(&mut s, &batch("dc1", vec![e("a"), e("b")]));
        let mut members = s.set_members("tags", "d1");
        members.sort();
        assert_eq!(members, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn wire_format_round_trips() {
        let b = batch("dc1", vec![put("a", "v", 100)]);
        let json = serde_json::to_string(&b).unwrap();
        let back: ReplBatch = serde_json::from_str(&json).unwrap();
        assert_eq!(b, back);
    }

    #[test]
    fn tail_change_feed_then_apply_to_remote() {
        // source DC records set changes in its change feed
        let log = MemoryChangeLog::new();
        log.append("add", "ACME", "orders", "d1");
        log.append("add", "EU", "orders", "d1");
        log.append("remove", "ACME", "orders", "d1");

        // replicator tails the group, ships, commits the offset
        let b = tail(&log, "geo-dc2", 100, 500, "dc1").expect("a batch");
        assert_eq!(b.entries.len(), 3);
        log.commit_offset("geo-dc2", b.last_seq);
        assert!(tail(&log, "geo-dc2", 100, 600, "dc1").is_none(), "caught up after commit");

        // remote DC applies it
        let mut remote = MemoryStore::new();
        apply_lww(&mut remote, &b);
        // ACME was added then removed; EU remains
        assert_eq!(remote.set_members("orders", "d1"), vec!["EU".to_string()]);
    }
}
