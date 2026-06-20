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
}

/// 20-digit zero-padded key so lexical order == numeric order for range scans.
fn key(seq: u64) -> String {
    format!("{seq:020}")
}

/// Non-durable log for dev/tests.
#[derive(Default)]
pub struct MemoryChangeLog {
    inner: Mutex<Vec<ChangeEvent>>,
}

impl MemoryChangeLog {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ChangeLog for MemoryChangeLog {
    fn append(&self, op: &str, value: &str, set: &str, doc_id: &str) -> u64 {
        let mut v = self.inner.lock().unwrap();
        let seq = v.len() as u64 + 1;
        v.push(ChangeEvent { seq, op: op.into(), value: value.into(), set: set.into(), doc_id: doc_id.into() });
        seq
    }
    fn read(&self, since: u64, limit: usize) -> Vec<ChangeEvent> {
        let v = self.inner.lock().unwrap();
        v.iter().filter(|e| e.seq > since).take(limit).cloned().collect()
    }
    fn latest_seq(&self) -> u64 {
        self.inner.lock().unwrap().len() as u64
    }
}

/// Durable log on LMDB (one db, seq -> JSON event). Recovers its counter from the last key on
/// open, so it resumes after a restart.
pub struct LmdbChangeLog {
    env: Env,
    db: Database<Str, Str>,
    next: Mutex<u64>,
}

impl LmdbChangeLog {
    pub fn open(path: &Path, map_size: usize) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        // SAFETY: single-process open; the mmap is not aliased elsewhere.
        let env = unsafe { EnvOpenOptions::new().max_dbs(2).map_size(map_size).open(path)? };
        let db: Database<Str, Str> = {
            let mut wtxn = env.write_txn()?;
            let db = env.create_database(&mut wtxn, Some("changefeed"))?;
            wtxn.commit()?;
            db
        };
        let next = {
            let rtxn = env.read_txn()?;
            let last = db.last(&rtxn)?.and_then(|(k, _)| k.parse::<u64>().ok()).unwrap_or(0);
            last + 1
        };
        Ok(Self { env, db, next: Mutex::new(next) })
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
