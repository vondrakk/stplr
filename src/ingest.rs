// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Durable ingest queue — a write-ahead log that makes ingest crash-safe.
//!
//! An incoming write is appended to a durable LMDB log and acknowledged *before* it is applied to
//! the shards (the `enqueue` is the durable accept point). A background drain loop applies queued
//! ops to a [`WriteSink`] (the cluster) and advances a durably-persisted **committed cursor**, so a
//! coordinator/ingest crash replays only the un-applied tail rather than losing accepted writes —
//! **at-least-once** delivery. The queued ops (put / set-add / set-remove / delete) are all
//! idempotent, so at-least-once replay is safe (a re-applied op is a no-op or an overwrite).
//!
//! Follow-ups: trim the log behind the committed cursor (it's append-only today); a stricter
//! `WriteSink` that fails (so the queue retries) when a write doesn't reach a quorum of replicas.

use std::path::Path;
use std::sync::Mutex;

use anyhow::Result;
use async_trait::async_trait;
use heed::types::Str;
use heed::{Database, Env, EnvOpenOptions};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One queued write. Tagged JSON on the wire/disk. All variants are idempotent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum IngestOp {
    Put { coll: String, key: String, value: Value },
    SetAdd { coll: String, key: String, member: String },
    SetRemove { coll: String, key: String, member: String },
    Delete { coll: String, key: String },
}

/// Where drained ops are applied — the cluster's write path (or a test double).
#[async_trait]
pub trait WriteSink: Send + Sync {
    async fn apply(&self, op: &IngestOp) -> Result<(), String>;
}

const COMMITTED: &str = "committed";

/// 20-digit zero-padded key so lexical order == numeric order for range scans.
fn key(seq: u64) -> String {
    format!("{seq:020}")
}

/// Durable, crash-resumable write-ahead queue on LMDB (two dbs: `queue` seq->op JSON, `meta` for the
/// committed cursor). Recovers both `next` and `committed` from disk on open.
pub struct IngestQueue {
    env: Env,
    queue: Database<Str, Str>,
    meta: Database<Str, Str>,
    next: Mutex<u64>,
    committed: Mutex<u64>,
}

impl IngestQueue {
    pub fn open(path: &Path, map_size: usize) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        // SAFETY: single-process open; the mmap is not aliased elsewhere.
        let env = unsafe { EnvOpenOptions::new().max_dbs(2).map_size(map_size).open(path)? };
        let (queue, meta): (Database<Str, Str>, Database<Str, Str>) = {
            let mut wtxn = env.write_txn()?;
            let q = env.create_database(&mut wtxn, Some("queue"))?;
            let m = env.create_database(&mut wtxn, Some("meta"))?;
            wtxn.commit()?;
            (q, m)
        };
        let (next, committed) = {
            let rtxn = env.read_txn()?;
            let next = queue.last(&rtxn)?.and_then(|(k, _)| k.parse::<u64>().ok()).unwrap_or(0) + 1;
            let committed = meta.get(&rtxn, COMMITTED)?.and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
            (next, committed)
        };
        Ok(Self { env, queue, meta, next: Mutex::new(next), committed: Mutex::new(committed) })
    }

    /// Durably append an op; returns its sequence number. The commit here is the at-least-once
    /// acknowledgement point — once this returns, the write survives a crash.
    pub fn enqueue(&self, op: &IngestOp) -> u64 {
        let mut next = self.next.lock().unwrap();
        let seq = *next;
        let mut wtxn = self.env.write_txn().expect("ingest write_txn");
        self.queue.put(&mut wtxn, &key(seq), &serde_json::to_string(op).unwrap()).expect("ingest put");
        wtxn.commit().expect("ingest commit");
        *next = seq + 1;
        seq
    }

    /// Highest sequence accepted (0 if empty).
    pub fn latest_seq(&self) -> u64 {
        self.next.lock().unwrap().saturating_sub(1)
    }

    /// Last sequence successfully applied + persisted.
    pub fn committed(&self) -> u64 {
        *self.committed.lock().unwrap()
    }

    /// Accepted-but-not-yet-applied count.
    pub fn pending(&self) -> u64 {
        self.latest_seq().saturating_sub(self.committed())
    }

    fn read_uncommitted(&self, from: u64, limit: usize) -> Vec<(u64, IngestOp)> {
        let rtxn = match self.env.read_txn() {
            Ok(t) => t,
            Err(_) => return Vec::new(),
        };
        let iter = match self.queue.iter(&rtxn) {
            Ok(i) => i,
            Err(_) => return Vec::new(),
        };
        let mut out = Vec::new();
        for item in iter.flatten() {
            if let Ok(seq) = item.0.parse::<u64>() {
                if seq > from {
                    if let Ok(op) = serde_json::from_str::<IngestOp>(item.1) {
                        out.push((seq, op));
                        if out.len() >= limit {
                            break;
                        }
                    }
                }
            }
        }
        out
    }

    fn persist_committed(&self, seq: u64) {
        let mut wtxn = self.env.write_txn().expect("ingest write_txn");
        self.meta.put(&mut wtxn, COMMITTED, &seq.to_string()).expect("ingest meta put");
        wtxn.commit().expect("ingest meta commit");
        *self.committed.lock().unwrap() = seq;
    }

    /// Apply up to `limit` accepted-but-unapplied ops to `sink`, in order, advancing the durable
    /// committed cursor to the last success. Stops at the first failure (that op is retried on the
    /// next drain). Returns how many were applied. Idempotent replay makes a partial drain safe.
    pub async fn drain(&self, sink: &dyn WriteSink, limit: usize) -> usize {
        let from = self.committed();
        let ops = self.read_uncommitted(from, limit);
        let mut last_ok = from;
        let mut applied = 0usize;
        for (seq, op) in ops {
            match sink.apply(&op).await {
                Ok(()) => {
                    last_ok = seq;
                    applied += 1;
                }
                Err(_) => break, // leave it (and the rest) for the next drain
            }
        }
        if last_ok > from {
            self.persist_committed(last_ok);
        }
        applied
    }
}

/// The cluster IS a write sink: applying a queued op runs its routed/replicated write.
#[async_trait]
impl WriteSink for crate::cluster::Cluster {
    async fn apply(&self, op: &IngestOp) -> Result<(), String> {
        match op {
            IngestOp::Put { coll, key, value } => self.put(coll, key, value.clone()).await,
            IngestOp::SetAdd { coll, key, member } => self.set_add(coll, key, member).await,
            IngestOp::SetRemove { coll, key, member } => self.set_remove(coll, key, member).await,
            IngestOp::Delete { coll, key } => self.delete(coll, key).await,
        }
        Ok(())
    }
}

/// Spawn a background loop that drains the queue to `sink` every `interval_ms` (the applier).
pub fn spawn_drainer(queue: std::sync::Arc<IngestQueue>, sink: std::sync::Arc<dyn WriteSink>, interval_ms: u64) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(interval_ms.max(20))).await;
            let _ = queue.drain(sink.as_ref(), 1024).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;

    fn tmp(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("stplr-ingest-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    // A sink that records applied ops and can be told to fail at/after a given op index.
    #[derive(Default)]
    struct MockSink {
        applied: Mutex<Vec<IngestOp>>,
        fail_after: Mutex<usize>, // fail once this many have been applied (usize::MAX = never)
    }
    #[async_trait]
    impl WriteSink for MockSink {
        async fn apply(&self, op: &IngestOp) -> Result<(), String> {
            let mut a = self.applied.lock().unwrap();
            if a.len() >= *self.fail_after.lock().unwrap() {
                return Err("sink down".into());
            }
            a.push(op.clone());
            Ok(())
        }
    }

    fn put(k: &str) -> IngestOp {
        IngestOp::Put { coll: "kv".into(), key: k.into(), value: json!(k) }
    }

    #[tokio::test]
    async fn enqueue_then_drain_in_order() {
        let dir = tmp("order");
        let q = IngestQueue::open(&dir, 16 * 1024 * 1024).unwrap();
        assert_eq!(q.enqueue(&put("a")), 1);
        assert_eq!(q.enqueue(&put("b")), 2);
        assert_eq!(q.enqueue(&put("c")), 3);
        assert_eq!(q.pending(), 3);

        let sink = MockSink { fail_after: Mutex::new(usize::MAX), ..Default::default() };
        assert_eq!(q.drain(&sink, 1024).await, 3);
        assert_eq!(q.committed(), 3);
        assert_eq!(q.pending(), 0);
        assert_eq!(*sink.applied.lock().unwrap(), vec![put("a"), put("b"), put("c")]);
        // draining again is a no-op (nothing uncommitted)
        assert_eq!(q.drain(&sink, 1024).await, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn crash_resume_replays_uncommitted_without_loss() {
        let dir = tmp("resume");
        {
            let q = IngestQueue::open(&dir, 16 * 1024 * 1024).unwrap();
            for k in ["a", "b", "c", "d"] {
                q.enqueue(&put(k));
            }
            // applier dies after committing 2 (sink fails on the 3rd)
            let sink = MockSink { fail_after: Mutex::new(2), ..Default::default() };
            assert_eq!(q.drain(&sink, 1024).await, 2);
            assert_eq!(q.committed(), 2);
            assert_eq!(q.pending(), 2, "c,d still un-applied");
        } // drop = "crash"

        // restart: cursor + un-applied tail recovered from disk
        let q = IngestQueue::open(&dir, 16 * 1024 * 1024).unwrap();
        assert_eq!(q.committed(), 2, "committed cursor survived");
        assert_eq!(q.latest_seq(), 4, "queue survived");
        assert_eq!(q.pending(), 2);
        let sink = MockSink { fail_after: Mutex::new(usize::MAX), ..Default::default() };
        assert_eq!(q.drain(&sink, 1024).await, 2, "replays exactly the un-applied tail");
        assert_eq!(*sink.applied.lock().unwrap(), vec![put("c"), put("d")], "no loss, no earlier replay");
        assert_eq!(q.pending(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn drains_into_a_real_cluster() {
        use crate::client::{InProcessShardClient, ShardClient};
        use crate::cluster::Cluster;
        use crate::net::shared;
        use crate::partitioner::NodeId;
        use crate::shard::Shard;
        use crate::store::MemoryStore;
        use std::collections::HashMap;

        let mut clients: HashMap<NodeId, Arc<dyn ShardClient>> = HashMap::new();
        for id in ["s0", "s1"] {
            clients.insert(id.to_string(), Arc::new(InProcessShardClient::new(id, shared(Shard::new(id, MemoryStore::new())))));
        }
        let cluster = Cluster::new(clients, 1, vec!["kv".into()]);

        let dir = tmp("cluster");
        let q = IngestQueue::open(&dir, 16 * 1024 * 1024).unwrap();
        q.enqueue(&put("alpha"));
        q.enqueue(&IngestOp::SetAdd { coll: "tags".into(), key: "t".into(), member: "m1".into() });

        // Cluster IS a WriteSink — draining applies the queued ops to the routed owners.
        assert_eq!(q.drain(&cluster, 1024).await, 2);
        assert_eq!(cluster.get("kv", "alpha").await, Some(json!("alpha")), "queued put landed in the cluster");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn enqueue_survives_reopen_before_any_apply() {
        let dir = tmp("durable");
        {
            let q = IngestQueue::open(&dir, 16 * 1024 * 1024).unwrap();
            q.enqueue(&put("x"));
            q.enqueue(&put("y"));
        } // crash before draining
        let q = IngestQueue::open(&dir, 16 * 1024 * 1024).unwrap();
        assert_eq!(q.pending(), 2, "accepted writes survived a crash with zero applied");
        let sink = Arc::new(MockSink { fail_after: Mutex::new(usize::MAX), ..Default::default() });
        assert_eq!(q.drain(sink.as_ref(), 1024).await, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
