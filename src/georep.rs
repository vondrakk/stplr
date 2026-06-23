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

impl ReplOp {
    pub fn coll(&self) -> &str {
        match self {
            ReplOp::Put { coll, .. } | ReplOp::Delete { coll, .. } | ReplOp::SetAdd { coll, .. } | ReplOp::SetRemove { coll, .. } => coll,
        }
    }
    pub fn key(&self) -> &str {
        match self {
            ReplOp::Put { key, .. } | ReplOp::Delete { key, .. } | ReplOp::SetAdd { key, .. } | ReplOp::SetRemove { key, .. } => key,
        }
    }
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

/// Convert PITR write-ahead-log entries into replicated ops (the comprehensive mutation source for
/// active-passive geo-replication — the WAL captures every mutation, not just set ops). A `PutTtl`
/// replicates as a plain `Put` (the destination applies it as a live value).
pub fn from_wal_entries(entries: &[crate::pitr::WalEntry], skip_origin: Option<&str>) -> Vec<ReplEntry> {
    use crate::pitr::WalOp;
    entries
        .iter()
        // Never replicate the internal LWW sidecar (per-DC bookkeeping, not user data).
        .filter(|e| e.op.coll() != GEO_TS)
        // Active-active loop break: don't ship a write back to the DC it came from.
        .filter(|e| skip_origin.map_or(true, |so| e.origin.as_deref() != Some(so)))
        .map(|e| {
            let op = match &e.op {
                WalOp::Put { coll, key, value } => ReplOp::Put { coll: coll.clone(), key: key.clone(), value: value.clone() },
                WalOp::PutTtl { coll, key, value, .. } => ReplOp::Put { coll: coll.clone(), key: key.clone(), value: value.clone() },
                WalOp::Delete { coll, key } => ReplOp::Delete { coll: coll.clone(), key: key.clone() },
                WalOp::SetAdd { coll, key, member } => ReplOp::SetAdd { coll: coll.clone(), key: key.clone(), member: member.clone() },
                WalOp::SetRemove { coll, key, member } => ReplOp::SetRemove { coll: coll.clone(), key: key.clone(), member: member.clone() },
            };
            ReplEntry { ts_ms: e.ts_ms, op }
        })
        .collect()
}

/// One replication cycle: tail the WAL past `group`'s cursor, ship the batch to `peer`, and (only on
/// success) advance the cursor so the next cycle resumes after it. Returns the number of ops shipped.
pub async fn replicate_once(
    wal: &dyn crate::pitr::Wal,
    client: &reqwest::Client,
    peer: &str,
    origin: &str,
    peer_id: Option<&str>,
    group: &str,
) -> Result<usize, String> {
    let cursor = wal.read_cursor(group);
    let entries = wal.entries_after(cursor);
    if entries.is_empty() {
        return Ok(0);
    }
    let last_seq = entries.last().map(|e| e.seq).unwrap_or(cursor);
    // `peer_id` set (active-active) -> drop the peer's own writes so they don't echo back.
    let repl = from_wal_entries(&entries, peer_id);
    if repl.is_empty() {
        wal.commit_cursor(group, last_seq); // nothing to ship, but advance past the skipped entries
        return Ok(0);
    }
    let batch = ReplBatch { origin: origin.to_string(), last_seq, entries: repl };
    let n = batch.entries.len();
    ship_batch(client, peer, &batch).await?;
    wal.commit_cursor(group, last_seq);
    Ok(n)
}

/// Replicator loop: forever, tail this node's PITR WAL and ship new mutations to `peer`'s
/// `/geo/apply`. Resumable via the durable WAL cursor (`group`). `peer_id` set = **active-active**
/// (skip the peer's own writes so they don't echo); `None` = active-passive (ship everything).
pub async fn replicate_loop(
    wal: std::sync::Arc<dyn crate::pitr::Wal>,
    peer: String,
    origin: String,
    peer_id: Option<String>,
    group: String,
    interval_ms: u64,
) {
    let client = reqwest::Client::new();
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(interval_ms.max(50))).await;
        match replicate_once(wal.as_ref(), &client, &peer, &origin, peer_id.as_deref(), &group).await {
            Ok(n) if n > 0 => eprintln!("geo: replicated {n} op(s) to {peer}"),
            Ok(_) => {}
            Err(e) => eprintln!("geo: replication to {peer} failed: {e}"),
        }
    }
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

    #[tokio::test]
    async fn replicate_from_wal_to_peer_endpoint() {
        use crate::client::ShardClient;
        use crate::cluster::Cluster;
        use crate::net::{serve_ephemeral, shared, HttpShardClient};
        use crate::partitioner::NodeId;
        use crate::pitr::{MemoryWal, Wal};
        use crate::shard::Shard;
        use std::collections::HashMap;
        use std::sync::Arc;

        // Active source: a shard recording its mutations to a PITR WAL.
        let wal = Arc::new(MemoryWal::new());
        let mut src = Shard::new("src", MemoryStore::new()).with_wal(wal.clone());
        src.write_object("kv", "a", Value::from("v1"));
        src.set_add("tags", "d1", "x");
        src.write_object("kv", "a", Value::from("v2"));

        // Passive destination cluster, with the coordinator /geo/apply endpoint.
        let mut clients: HashMap<NodeId, Arc<dyn ShardClient>> = HashMap::new();
        for id in ["d0", "d1"] {
            let addr = serve_ephemeral(shared(Shard::new(id, MemoryStore::new()))).await;
            clients.insert(id.to_string(), Arc::new(HttpShardClient::new(id, &format!("http://{addr}"))));
        }
        let cluster = Arc::new(Cluster::new(clients, 1, vec!["kv".into()]));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let caddr = listener.local_addr().unwrap();
        let router = crate::coord::app(cluster.clone());
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let peer = format!("http://{caddr}");

        // One replication cycle ships the whole WAL and advances the cursor.
        let client = reqwest::Client::new();
        let n = replicate_once(wal.as_ref(), &client, &peer, "src", None, "geo->dst").await.unwrap();
        assert_eq!(n, 3, "shipped all 3 recorded mutations");
        assert_eq!(wal.read_cursor("geo->dst"), 3, "cursor advanced to the WAL head");

        // The destination now holds the replicated state (latest value + the set member).
        assert_eq!(cluster.get("kv", "a").await, Some(Value::from("v2")));
        let tags = cluster.get("tags", "d1").await.unwrap();
        assert!(tags.as_array().unwrap().iter().any(|v| v == "x"), "set member replicated");

        // Caught up: a second cycle ships nothing.
        assert_eq!(replicate_once(wal.as_ref(), &client, &peer, "src", None, "geo->dst").await.unwrap(), 0);
    }

    #[test]
    fn active_active_does_not_echo_the_peers_own_writes() {
        use crate::pitr::{MemoryWal, Wal, WalOp};
        let wal = MemoryWal::new();
        // a local write (origin None) + one that arrived from DC "B" (origin Some("B")) + the internal
        // LWW sidecar (must never replicate)
        wal.append(100, WalOp::Put { coll: "kv".into(), key: "local".into(), value: Value::from(1) });
        wal.append_with_origin(200, WalOp::Put { coll: "kv".into(), key: "fromB".into(), value: Value::from(2) }, Some("B".into()));
        wal.append(300, WalOp::Put { coll: GEO_TS.into(), key: "kv\u{1}fromB".into(), value: Value::from(200) });
        let entries = wal.entries();

        // shipping toward DC "B": skip B-origin writes (no echo) AND the sidecar -> only the local write
        let to_b = from_wal_entries(&entries, Some("B"));
        assert_eq!(to_b.len(), 1);
        assert_eq!(to_b[0].op, ReplOp::Put { coll: "kv".into(), key: "local".into(), value: Value::from(1) });

        // active-passive (no peer id): still drops the sidecar, keeps both data writes
        let all = from_wal_entries(&entries, None);
        assert_eq!(all.len(), 2);
    }
}
