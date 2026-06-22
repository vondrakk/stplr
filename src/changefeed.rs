// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Durable, replayable value-level change feed. Every value that joins or leaves a set (via
//! ingest/remove) is appended to a monotonic log; consumers pull from any offset
//! (`read(since, limit)`) and resume after downtime. Emitted at the coordinator (the write choke
//! point). The LMDB-backed log survives restart; an in-memory one is used for tests.
//!
//! NOTE (follow-ups): the log is unbounded (no retention/trim yet), and ordering is per
//! coordinator node — a globally-ordered feed across multiple coordinators needs a shared
//! sequencer.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use anyhow::Result;
use heed::types::Str;
use heed::{Database, Env, EnvOpenOptions};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeEvent {
    pub seq: u64,
    pub op: String, // "add" | "remove"
    pub value: String,
    pub set: String,
    pub doc_id: String,
}

pub trait ChangeLog: Send + Sync {
    /// Append a value-level change; assigns and returns the new sequence number.
    fn append(&self, op: &str, value: &str, set: &str, doc_id: &str) -> u64;
    /// Events with seq > `since`, up to `limit`, in order.
    fn read(&self, since: u64, limit: usize) -> Vec<ChangeEvent>;
    /// Highest sequence number written so far (0 if empty).
    fn latest_seq(&self) -> u64;
    /// Drop all events with seq < `before` (retention/compaction). Returns how many were removed.
    /// Consumers that already read past `before` lose nothing; ones that haven't skip the gap.
    fn compact(&self, before: u64) -> usize;
    /// Keep only the most recent `max` events, compacting everything older. Returns removed count.
    fn retain_latest(&self, max: u64) -> usize {
        let latest = self.latest_seq();
        if latest > max {
            self.compact(latest - max + 1)
        } else {
            0
        }
    }
    /// A consumer group's committed read offset (0 = never committed; the group starts at the beginning).
    fn committed_offset(&self, group: &str) -> u64;
    /// Persist a consumer group's committed offset — acks every event up to and including `seq`, so the
    /// next [`read_group`](Self::read_group) resumes after it. Durable for the LMDB log.
    fn commit_offset(&self, group: &str, seq: u64);
    /// The next batch for a consumer group: events after its committed offset, up to `limit`. Lets
    /// many independent consumers share one feed, each tracking its own position (at-least-once).
    fn read_group(&self, group: &str, limit: usize) -> Vec<ChangeEvent> {
        self.read(self.committed_offset(group), limit)
    }
}

/// 20-digit zero-padded key so lexical order == numeric order for range scans.
fn key(seq: u64) -> String {
    format!("{seq:020}")
}

/// Non-durable log for dev/tests. `next` is a monotonic seq counter independent of the buffer
/// length, so sequence numbers stay unique and increasing across compaction (which shrinks `inner`).
#[derive(Default)]
pub struct MemoryChangeLog {
    inner: Mutex<Vec<ChangeEvent>>,
    next: AtomicU64,
    offsets: Mutex<std::collections::HashMap<String, u64>>,
}

impl MemoryChangeLog {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ChangeLog for MemoryChangeLog {
    fn append(&self, op: &str, value: &str, set: &str, doc_id: &str) -> u64 {
        let seq = self.next.fetch_add(1, Ordering::SeqCst) + 1; // monotonic, survives compaction
        self.inner.lock().unwrap().push(ChangeEvent {
            seq,
            op: op.into(),
            value: value.into(),
            set: set.into(),
            doc_id: doc_id.into(),
        });
        seq
    }
    fn read(&self, since: u64, limit: usize) -> Vec<ChangeEvent> {
        let v = self.inner.lock().unwrap();
        v.iter().filter(|e| e.seq > since).take(limit).cloned().collect()
    }
    fn latest_seq(&self) -> u64 {
        self.next.load(Ordering::SeqCst)
    }
    fn compact(&self, before: u64) -> usize {
        let mut v = self.inner.lock().unwrap();
        let n = v.len();
        v.retain(|e| e.seq >= before);
        n - v.len()
    }
    fn committed_offset(&self, group: &str) -> u64 {
        self.offsets.lock().unwrap().get(group).copied().unwrap_or(0)
    }
    fn commit_offset(&self, group: &str, seq: u64) {
        self.offsets.lock().unwrap().insert(group.to_string(), seq);
    }
}

/// Durable log on LMDB (one db, seq -> JSON event). Recovers its counter from the last key on
/// open, so it resumes after a restart.
pub struct LmdbChangeLog {
    env: Env,
    db: Database<Str, Str>,
    offsets: Database<Str, Str>, // consumer-group committed offsets: group -> seq
    next: Mutex<u64>,
}

impl LmdbChangeLog {
    pub fn open(path: &Path, map_size: usize) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        // SAFETY: single-process open; the mmap is not aliased elsewhere.
        let env = unsafe { EnvOpenOptions::new().max_dbs(4).map_size(map_size).open(path)? };
        let (db, offsets): (Database<Str, Str>, Database<Str, Str>) = {
            let mut wtxn = env.write_txn()?;
            let db = env.create_database(&mut wtxn, Some("changefeed"))?;
            let offsets = env.create_database(&mut wtxn, Some("offsets"))?;
            wtxn.commit()?;
            (db, offsets)
        };
        let next = {
            let rtxn = env.read_txn()?;
            let last = db.last(&rtxn)?.and_then(|(k, _)| k.parse::<u64>().ok()).unwrap_or(0);
            last + 1
        };
        Ok(Self { env, db, offsets, next: Mutex::new(next) })
    }
}

impl ChangeLog for LmdbChangeLog {
    fn append(&self, op: &str, value: &str, set: &str, doc_id: &str) -> u64 {
        let mut next = self.next.lock().unwrap();
        let seq = *next;
        let ev = ChangeEvent { seq, op: op.into(), value: value.into(), set: set.into(), doc_id: doc_id.into() };
        let mut wtxn = self.env.write_txn().expect("changefeed write_txn");
        self.db.put(&mut wtxn, &key(seq), &serde_json::to_string(&ev).unwrap()).expect("changefeed put");
        wtxn.commit().expect("changefeed commit");
        *next = seq + 1;
        seq
    }
    fn read(&self, since: u64, limit: usize) -> Vec<ChangeEvent> {
        let rtxn = match self.env.read_txn() {
            Ok(t) => t,
            Err(_) => return Vec::new(),
        };
        // Keys are zero-padded, so iteration is in seq order. Skipping to `since` is O(offset);
        // a numeric-keyed range scan would make it O(result) — a later optimization.
        let iter = match self.db.iter(&rtxn) {
            Ok(i) => i,
            Err(_) => return Vec::new(),
        };
        let mut out = Vec::new();
        for item in iter {
            if let Ok((_, v)) = item {
                if let Ok(ev) = serde_json::from_str::<ChangeEvent>(v) {
                    if ev.seq > since {
                        out.push(ev);
                        if out.len() >= limit {
                            break;
                        }
                    }
                }
            }
        }
        out
    }
    fn latest_seq(&self) -> u64 {
        self.next.lock().unwrap().saturating_sub(1)
    }
    fn compact(&self, before: u64) -> usize {
        if before <= 1 {
            return 0;
        }
        // Collect keys for events with seq < before (sorted; stop once we reach it), then delete.
        let to_delete: Vec<String> = {
            let rtxn = match self.env.read_txn() {
                Ok(t) => t,
                Err(_) => return 0,
            };
            let iter = match self.db.iter(&rtxn) {
                Ok(i) => i,
                Err(_) => return 0,
            };
            let mut ks = Vec::new();
            for item in iter.flatten() {
                match item.0.parse::<u64>() {
                    Ok(seq) if seq < before => ks.push(item.0.to_string()),
                    Ok(_) => break,
                    Err(_) => continue,
                }
            }
            ks
        };
        if to_delete.is_empty() {
            return 0;
        }
        let mut wtxn = match self.env.write_txn() {
            Ok(t) => t,
            Err(_) => return 0,
        };
        let mut removed = 0;
        for k in &to_delete {
            if self.db.delete(&mut wtxn, k).unwrap_or(false) {
                removed += 1;
            }
        }
        if wtxn.commit().is_err() {
            return 0;
        }
        removed
    }
    fn committed_offset(&self, group: &str) -> u64 {
        let Ok(rtxn) = self.env.read_txn() else {
            return 0;
        };
        self.offsets.get(&rtxn, group).ok().flatten().and_then(|v| v.parse().ok()).unwrap_or(0)
    }
    fn commit_offset(&self, group: &str, seq: u64) {
        if let Ok(mut wtxn) = self.env.write_txn() {
            if self.offsets.put(&mut wtxn, group, &seq.to_string()).is_ok() {
                let _ = wtxn.commit();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exercise(log: &dyn ChangeLog) {
        assert_eq!(log.latest_seq(), 0);
        assert_eq!(log.append("add", "ACME-001", "orders", "O-1"), 1);
        assert_eq!(log.append("add", "EU", "orders", "O-1"), 2);
        assert_eq!(log.append("remove", "ACME-001", "logs", "L-1"), 3);
        assert_eq!(log.latest_seq(), 3);

        // replay from offset
        let from1 = log.read(1, 100);
        assert_eq!(from1.len(), 2);
        assert_eq!(from1[0].seq, 2);
        assert_eq!(from1[0].value, "EU");
        assert_eq!(from1[1].op, "remove");

        // from the start, with a limit
        let first = log.read(0, 2);
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].seq, 1);

        // caught up
        assert!(log.read(3, 100).is_empty());
    }

    fn exercise_retention(log: &dyn ChangeLog) {
        for i in 0..10 {
            log.append("add", &format!("v{i}"), "s", "d");
        }
        assert_eq!(log.latest_seq(), 10);
        // keep the last 3 -> drops seq 1..=7
        assert_eq!(log.retain_latest(3), 7);
        let all = log.read(0, 100);
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].seq, 8, "oldest surviving event is seq 8");
        // explicit compact below 9 -> drops seq 8
        assert_eq!(log.compact(9), 1);
        assert_eq!(log.read(0, 100).len(), 2);
        // retain more than present is a no-op; latest_seq unchanged (monotonic)
        assert_eq!(log.retain_latest(100), 0);
        assert_eq!(log.latest_seq(), 10);
    }

    #[test]
    fn memory_retention() {
        exercise_retention(&MemoryChangeLog::new());
    }

    #[test]
    fn lmdb_retention() {
        let dir = std::env::temp_dir().join(format!("stplr-cf-ret-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        {
            let log = LmdbChangeLog::open(&dir, 16 * 1024 * 1024).unwrap();
            exercise_retention(&log);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn exercise_groups(log: &dyn ChangeLog) {
        for i in 0..5 {
            log.append("add", &format!("v{i}"), "s", "d"); // seq 1..=5
        }
        // a fresh group starts from the beginning
        assert_eq!(log.committed_offset("g1"), 0);
        let first = log.read_group("g1", 2);
        assert_eq!(first.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1, 2]);

        // commit advances g1; g2 is independent (still sees everything)
        log.commit_offset("g1", 2);
        assert_eq!(log.read_group("g1", 10).iter().map(|e| e.seq).collect::<Vec<_>>(), vec![3, 4, 5]);
        assert_eq!(log.read_group("g2", 10).len(), 5);

        // drain g1
        log.commit_offset("g1", 5);
        assert!(log.read_group("g1", 10).is_empty());
        assert_eq!(log.committed_offset("g1"), 5);
        assert_eq!(log.committed_offset("g2"), 0);
    }

    #[test]
    fn memory_consumer_groups() {
        exercise_groups(&MemoryChangeLog::new());
    }

    #[test]
    fn lmdb_consumer_groups_durable() {
        let dir = std::env::temp_dir().join(format!("stplr-cf-grp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        {
            let log = LmdbChangeLog::open(&dir, 16 * 1024 * 1024).unwrap();
            exercise_groups(&log);
        }
        // reopen: the committed offset survived the restart
        let log = LmdbChangeLog::open(&dir, 16 * 1024 * 1024).unwrap();
        assert_eq!(log.committed_offset("g1"), 5, "offset recovered from disk");
        assert!(log.read_group("g1", 10).is_empty(), "g1 still drained after restart");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn memory_changelog() {
        exercise(&MemoryChangeLog::new());
    }

    #[test]
    fn lmdb_changelog_durable_and_resumable() {
        let dir = std::env::temp_dir().join(format!("stitch-cf-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        {
            let log = LmdbChangeLog::open(&dir, 16 * 1024 * 1024).unwrap();
            exercise(&log);
        }
        // reopen: counter recovered from disk, replay still works
        let log = LmdbChangeLog::open(&dir, 16 * 1024 * 1024).unwrap();
        assert_eq!(log.latest_seq(), 3, "seq recovered from disk");
        assert_eq!(log.read(0, 100).len(), 3, "events survived reopen");
        assert_eq!(log.append("add", "X", "s", "d"), 4, "appends continue from recovered seq");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
