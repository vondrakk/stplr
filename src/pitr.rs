// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Point-in-time restore (PITR).
//!
//! A [`Wal`] records every mutation a shard applies, stamped with wall-clock time. To restore the
//! store to how it looked at some past instant, replay the log into a fresh store, applying only the
//! ops with `ts_ms <= target` ([`restore_into`]). Because conditional ops (CAS, INCR) are recorded
//! as their resulting `Put`, replay is deterministic and needs no re-evaluation.
//!
//! The WAL is opt-in (a shard records to it only when one is attached) and supports
//! [`Wal::compact_before`] to bound the retained PITR window.

use std::path::Path;
use std::sync::Mutex;

use anyhow::Result;
use heed::types::Str;
use heed::{Database, Env, EnvOpenOptions};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::store::IndexStore;

/// A single recorded mutation. CAS/INCR are normalized to the `Put` they produced.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum WalOp {
    Put { coll: String, key: String, value: Value },
    PutTtl { coll: String, key: String, value: Value, expire_at_ms: u64 },
    Delete { coll: String, key: String },
    SetAdd { coll: String, key: String, member: String },
    SetRemove { coll: String, key: String, member: String },
}

impl WalOp {
    pub fn coll(&self) -> &str {
        match self {
            WalOp::Put { coll, .. }
            | WalOp::PutTtl { coll, .. }
            | WalOp::Delete { coll, .. }
            | WalOp::SetAdd { coll, .. }
            | WalOp::SetRemove { coll, .. } => coll,
        }
    }
}

/// A WAL record: a monotonic sequence, the wall-clock time it was applied, and the op.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WalEntry {
    pub seq: u64,
    pub ts_ms: u64,
    pub op: WalOp,
    /// Where this mutation came from: `None` = a local write; `Some(dc)` = applied from peer `dc` via
    /// geo-replication. The replicator skips entries whose origin is the peer it's shipping to, so an
    /// active-active write doesn't echo back to the DC it came from. `#[serde(default)]` keeps old
    /// WAL records (written before this field existed) readable.
    #[serde(default)]
    pub origin: Option<String>,
}

pub trait Wal: Send + Sync {
    /// Append a locally-applied op — `append_with_origin` with no origin.
    fn append(&self, ts_ms: u64, op: WalOp) -> u64 {
        self.append_with_origin(ts_ms, op, None)
    }
    /// Append an op stamped with where it came from (`None` = local). Returns its sequence number.
    fn append_with_origin(&self, ts_ms: u64, op: WalOp, origin: Option<String>) -> u64;
    /// All records in sequence order.
    fn entries(&self) -> Vec<WalEntry>;
    /// Drop records with `ts_ms < before` (PITR-window retention). Returns how many were removed.
    fn compact_before(&self, before: u64) -> usize;
    /// Records with `seq > after`, in order — for resumable tailing (e.g. geo-replication). The
    /// default filters `entries`; durable backends may override with a range scan.
    fn entries_after(&self, after: u64) -> Vec<WalEntry> {
        self.entries().into_iter().filter(|e| e.seq > after).collect()
    }
    /// A named cursor's committed seq (0 if never set) — lets a consumer resume where it left off.
    fn read_cursor(&self, name: &str) -> u64;
    /// Persist a named cursor's committed seq.
    fn commit_cursor(&self, name: &str, seq: u64);
}

/// Apply one op to a store (used during replay).
pub fn apply(op: &WalOp, store: &mut dyn IndexStore) {
    match op {
        WalOp::Put { coll, key, value } => store.put_object(coll, key, value.clone()),
        WalOp::PutTtl { coll, key, value, expire_at_ms } => store.put_object_at(coll, key, value.clone(), *expire_at_ms),
        WalOp::Delete { coll, key } => store.delete_object(coll, key),
        WalOp::SetAdd { coll, key, member } => {
            store.set_add(coll, key, member);
        }
        WalOp::SetRemove { coll, key, member } => {
            store.set_remove(coll, key, member);
        }
    }
}

/// Replay the WAL into `store`, applying every op recorded at or before `target_ms`, in order. The
/// store ends up in the state it had as of `target_ms`.
pub fn restore_into(wal: &dyn Wal, target_ms: u64, store: &mut dyn IndexStore) -> usize {
    let mut applied = 0;
    for e in wal.entries() {
        if e.ts_ms <= target_ms {
            apply(&e.op, store);
            applied += 1;
        }
    }
    applied
}

/// 20-digit zero-padded key so lexical order == numeric order.
fn key(seq: u64) -> String {
    format!("{seq:020}")
}

/// Non-durable WAL for dev/tests.
#[derive(Default)]
pub struct MemoryWal {
    inner: Mutex<Vec<WalEntry>>,
    cursors: Mutex<std::collections::HashMap<String, u64>>,
}

impl MemoryWal {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Wal for MemoryWal {
    fn append_with_origin(&self, ts_ms: u64, op: WalOp, origin: Option<String>) -> u64 {
        let mut v = self.inner.lock().unwrap();
        let seq = v.len() as u64 + 1;
        v.push(WalEntry { seq, ts_ms, op, origin });
        seq
    }
    fn entries(&self) -> Vec<WalEntry> {
        self.inner.lock().unwrap().clone()
    }
    fn compact_before(&self, before: u64) -> usize {
        let mut v = self.inner.lock().unwrap();
        let n = v.len();
        v.retain(|e| e.ts_ms >= before);
        n - v.len()
    }
    fn read_cursor(&self, name: &str) -> u64 {
        self.cursors.lock().unwrap().get(name).copied().unwrap_or(0)
    }
    fn commit_cursor(&self, name: &str, seq: u64) {
        self.cursors.lock().unwrap().insert(name.to_string(), seq);
    }
}

/// Durable WAL on LMDB (one db, seq -> JSON record). Recovers its counter from the last key on open.
pub struct LmdbWal {
    env: Env,
    db: Database<Str, Str>,
    cursors: Database<Str, Str>, // consumer cursors: name -> committed seq
    next: Mutex<u64>,
}

impl LmdbWal {
    pub fn open(path: &Path, map_size: usize) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        // SAFETY: single-process open; the mmap is not aliased elsewhere.
        let env = unsafe { EnvOpenOptions::new().max_dbs(4).map_size(map_size).open(path)? };
        let (db, cursors): (Database<Str, Str>, Database<Str, Str>) = {
            let mut wtxn = env.write_txn()?;
            let db = env.create_database(&mut wtxn, Some("wal"))?;
            let cursors = env.create_database(&mut wtxn, Some("cursors"))?;
            wtxn.commit()?;
            (db, cursors)
        };
        let next = {
            let rtxn = env.read_txn()?;
            db.last(&rtxn)?.and_then(|(k, _)| k.parse::<u64>().ok()).unwrap_or(0) + 1
        };
        Ok(Self { env, db, cursors, next: Mutex::new(next) })
    }
}

impl Wal for LmdbWal {
    fn append_with_origin(&self, ts_ms: u64, op: WalOp, origin: Option<String>) -> u64 {
        let mut next = self.next.lock().unwrap();
        let seq = *next;
        let entry = WalEntry { seq, ts_ms, op, origin };
        let mut wtxn = self.env.write_txn().expect("wal write_txn");
        self.db.put(&mut wtxn, &key(seq), &serde_json::to_string(&entry).unwrap()).expect("wal put");
        wtxn.commit().expect("wal commit");
        *next = seq + 1;
        seq
    }
    fn entries(&self) -> Vec<WalEntry> {
        let rtxn = match self.env.read_txn() {
            Ok(t) => t,
            Err(_) => return Vec::new(),
        };
        let iter = match self.db.iter(&rtxn) {
            Ok(i) => i,
            Err(_) => return Vec::new(),
        };
        iter.flatten().filter_map(|(_, v)| serde_json::from_str::<WalEntry>(v).ok()).collect()
    }
    fn compact_before(&self, before: u64) -> usize {
        let to_delete: Vec<String> = {
            let rtxn = match self.env.read_txn() {
                Ok(t) => t,
                Err(_) => return 0,
            };
            let iter = match self.db.iter(&rtxn) {
                Ok(i) => i,
                Err(_) => return 0,
            };
            iter.flatten()
                .filter_map(|(k, v)| {
                    serde_json::from_str::<WalEntry>(v).ok().filter(|e| e.ts_ms < before).map(|_| k.to_string())
                })
                .collect()
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
    fn read_cursor(&self, name: &str) -> u64 {
        let Ok(rtxn) = self.env.read_txn() else {
            return 0;
        };
        self.cursors.get(&rtxn, name).ok().flatten().and_then(|v| v.parse().ok()).unwrap_or(0)
    }
    fn commit_cursor(&self, name: &str, seq: u64) {
        if let Ok(mut wtxn) = self.env.write_txn() {
            if self.cursors.put(&mut wtxn, name, &seq.to_string()).is_ok() {
                let _ = wtxn.commit();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    fn exercise(wal: &dyn Wal) {
        // a key's life over time: created @100, updated @200, deleted @300
        wal.append(100, WalOp::Put { coll: "kv".into(), key: "a".into(), value: Value::from(1) });
        wal.append(200, WalOp::Put { coll: "kv".into(), key: "a".into(), value: Value::from(2) });
        wal.append(250, WalOp::SetAdd { coll: "tags".into(), key: "a".into(), member: "x".into() });
        wal.append(300, WalOp::Delete { coll: "kv".into(), key: "a".into() });

        // restore to 150 -> a == 1, no tag yet
        let mut s = MemoryStore::new();
        assert_eq!(restore_into(wal, 150, &mut s), 1);
        assert_eq!(s.get_object("kv", "a"), Some(Value::from(1)));

        // restore to 260 -> a == 2, tag present
        let mut s = MemoryStore::new();
        restore_into(wal, 260, &mut s);
        assert_eq!(s.get_object("kv", "a"), Some(Value::from(2)));
        assert!(s.set_members("tags", "a").contains(&"x".to_string()));

        // restore to 999 -> a deleted
        let mut s = MemoryStore::new();
        restore_into(wal, 999, &mut s);
        assert_eq!(s.get_object("kv", "a"), None);
    }

    #[test]
    fn memory_pitr() {
        exercise(&MemoryWal::new());
    }

    #[test]
    fn shard_records_then_restores() {
        use crate::shard::Shard;
        use std::sync::Arc;
        let wal = Arc::new(MemoryWal::new());
        let mut shard = Shard::new("s0", MemoryStore::new()).with_wal(wal.clone());
        shard.write_object("kv", "a", Value::from("hi"));
        shard.incr("kv", "n", 5); // recorded as Put(5)
        shard.set_add("tags", "a", "x");
        shard.cas("kv", "c", None, Value::from(1)); // recorded as Put(1)
        shard.delete_object("kv", "a"); // after the put -> a is gone

        let mut s = MemoryStore::new();
        restore_into(wal.as_ref(), u64::MAX, &mut s);
        assert_eq!(s.get_object("kv", "a"), None, "delete replayed in order");
        assert_eq!(s.get_object("kv", "n"), Some(Value::from(5)), "incr recorded as its resulting put");
        assert_eq!(s.get_object("kv", "c"), Some(Value::from(1)), "cas recorded as its resulting put");
        assert!(s.set_members("tags", "a").contains(&"x".to_string()));
    }

    #[test]
    fn lmdb_pitr_durable_and_compactable() {
        let dir = std::env::temp_dir().join(format!("stplr-wal-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        {
            let wal = LmdbWal::open(&dir, 16 * 1024 * 1024).unwrap();
            exercise(&wal);
            // retention: drop everything before ts 300 -> only the delete @300 remains
            assert_eq!(wal.compact_before(300), 3);
            assert_eq!(wal.entries().len(), 1);
        }
        // reopen: records survived, counter recovered
        let wal = LmdbWal::open(&dir, 16 * 1024 * 1024).unwrap();
        assert_eq!(wal.entries().len(), 1);
        assert_eq!(wal.append(400, WalOp::Delete { coll: "kv".into(), key: "z".into() }), 5, "seq continues from disk");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
