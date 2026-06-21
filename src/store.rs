// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Storage seam — the substrate's two shard-local roles: `DataSource` (a generic tabular row
//! source) and `IndexStore` (a generic object + set store). Synchronous (LMDB is sync); async lives
//! at the network layer. A `MemoryStore` fills both roles for dev/tests/single-node. No correlation
//! types cross this boundary — it is pure key/value/set storage.

use std::collections::HashMap;

use serde_json::Value;

#[derive(Debug, Clone)]
pub struct Row {
    pub row_id: String,
    pub doc: Value,
}

#[derive(Debug, Clone, Copy)]
pub struct Capabilities {
    /// The store can compute AND/OR/NOT over location sets itself (pushdown).
    pub native_set_logic: bool,
}

/// Read-only access to the user's source records.
pub trait DataSource {
    fn list_tables(&self) -> Vec<String>;
    fn scan_table(&self, table: &str) -> Vec<Row>;
    fn get_row(&self, table: &str, row_id: &str) -> Option<Value>;
}

/// Persists the engine's index/stitch/map structures, optionally with native set ops.
pub trait IndexStore {
    fn put_object(&mut self, coll: &str, id: &str, obj: Value);
    /// Apply many object puts at once. The default loops `put_object`; durable backends override
    /// this to apply the whole batch in ONE transaction (group commit) — the write-throughput win,
    /// since N writes then pay one commit instead of N.
    fn put_batch(&mut self, items: Vec<(String, String, Value)>) {
        for (coll, id, obj) in items {
            self.put_object(&coll, &id, obj);
        }
    }
    fn get_object(&self, coll: &str, id: &str) -> Option<Value>;
    fn scan_objects(&self, coll: &str) -> Vec<Value>;
    /// (id, obj) pairs — needed for resharding, where the routing key (= id) must be known.
    fn scan_entries(&self, coll: &str) -> Vec<(String, Value)>;
    /// Ascending key iteration within a collection, bounded three ways (all optional): keys strictly
    /// greater than `after` (the pagination cursor), starting with `prefix`, and strictly less than
    /// `end`. Up to `limit`, skipping expired keys. The default collects + sorts via `scan_entries`;
    /// durable stores override with an LMDB cursor range scan (no full load).
    fn scan_range(&self, coll: &str, after: Option<&str>, prefix: Option<&str>, end: Option<&str>, limit: usize) -> Vec<String> {
        let mut keys: Vec<String> = self.scan_entries(coll).into_iter().map(|(k, _)| k).collect();
        keys.sort();
        keys.into_iter()
            .filter(|k| after.map_or(true, |a| k.as_str() > a))
            .filter(|k| prefix.map_or(true, |p| k.starts_with(p)))
            .filter(|k| end.map_or(true, |e| k.as_str() < e))
            .take(limit)
            .collect()
    }
    /// Paginated key iteration (cursor only) — `scan_range` with no prefix/end bound.
    fn scan_keys(&self, coll: &str, after: Option<&str>, limit: usize) -> Vec<String> {
        self.scan_range(coll, after, None, None, limit)
    }
    /// Fetch many keys at once, returning a value (or `None`) per key, in input order. The default
    /// loops `get_object`; the network win is at the cluster layer (one request per shard).
    fn mget(&self, coll: &str, keys: &[String]) -> Vec<Option<Value>> {
        keys.iter().map(|k| self.get_object(coll, k)).collect()
    }
    fn clear_collection(&mut self, coll: &str);
    fn delete_object(&mut self, coll: &str, id: &str);

    /// Write an object with an **absolute** expiry (epoch ms); after that time `get_object` treats it
    /// as absent (lazy expiry) and `sweep_expired` reclaims it. The default ignores the expiry (no
    /// TTL support) so other backends still compile. A plain `put_object` clears any prior expiry.
    fn put_object_at(&mut self, coll: &str, id: &str, obj: Value, _expire_at_ms: u64) {
        self.put_object(coll, id, obj);
    }
    /// Absolute expiry (epoch ms) recorded for a key, if any (not necessarily in the future).
    fn expiry_of(&self, _coll: &str, _id: &str) -> Option<u64> {
        None
    }
    /// Physically drop entries whose expiry is <= `now_ms`; returns how many. Default no-op.
    fn sweep_expired(&mut self, _now_ms: u64) -> usize {
        0
    }

    /// Atomic compare-and-set: write `new` iff the current value equals `expect` (`expect == None`
    /// means "only if the key is absent"). Returns whether the write happened. The read-modify-write
    /// is atomic because every caller (the shard) holds the exclusive store lock for the whole op.
    fn cas(&mut self, coll: &str, key: &str, expect: Option<Value>, new: Value) -> bool {
        if self.get_object(coll, key) == expect {
            self.put_object(coll, key, new);
            true
        } else {
            false
        }
    }

    /// Atomic integer add (absent key counts as 0). Returns the new value, or `None` if the existing
    /// value isn't an integer. Atomic for the same reason as `cas`.
    fn incr(&mut self, coll: &str, key: &str, delta: i64) -> Option<i64> {
        let cur = match self.get_object(coll, key) {
            None => 0,
            Some(v) => v.as_i64()?,
        };
        let next = cur.wrapping_add(delta);
        self.put_object(coll, key, Value::from(next));
        Some(next)
    }

    /// Hot-copy the durable store to `dest` — a consistent point-in-time backup taken WITHOUT
    /// quiescing the shard. The default errors (in-memory/non-durable stores have nothing on disk
    /// to copy); durable backends override it. Restore = point a fresh node's data dir at the copy.
    fn snapshot(&self, _dest: &std::path::Path) -> Result<(), String> {
        Err("snapshot not supported by this store (durable backend only)".into())
    }

    // --- generic posting-list set ops ---
    // A "set" at (coll, key) is a collection of opaque string members, stored as a JSON array.
    // Mutations are a single read-modify-write of one object, so callers that hold the store lock
    // (every shard does) get atomic add/remove with no cross-key coordination. Backends with a
    // native set type (e.g. Redis SADD/SREM/SMEMBERS) override these for pushdown.

    /// Add `member`; returns true if it was not already present.
    fn set_add(&mut self, coll: &str, key: &str, member: &str) -> bool {
        let mut arr: Vec<String> = self.get_object(coll, key).and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default();
        if arr.iter().any(|m| m == member) {
            return false;
        }
        arr.push(member.to_string());
        self.put_object(coll, key, serde_json::to_value(arr).unwrap());
        true
    }

    /// Remove `member`; returns true if it was present. Deletes the key when the set empties.
    fn set_remove(&mut self, coll: &str, key: &str, member: &str) -> bool {
        let mut arr: Vec<String> = self.get_object(coll, key).and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default();
        let before = arr.len();
        arr.retain(|m| m != member);
        if arr.len() == before {
            return false;
        }
        if arr.is_empty() {
            self.delete_object(coll, key);
        } else {
            self.put_object(coll, key, serde_json::to_value(arr).unwrap());
        }
        true
    }

    /// The set's members (empty if the key is absent).
    fn set_members(&self, coll: &str, key: &str) -> Vec<String> {
        self.get_object(coll, key).and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default()
    }

    fn capabilities(&self) -> Capabilities;
}

/// Reference adapter: source tables + engine collections in memory. Not persistent.
#[derive(Default)]
pub struct MemoryStore {
    tables: HashMap<String, Vec<Row>>,
    objects: HashMap<String, HashMap<String, Value>>,
    expiry: HashMap<String, HashMap<String, u64>>, // coll -> id -> absolute expiry (epoch ms)
}

impl MemoryStore {
    fn is_expired(&self, coll: &str, id: &str) -> bool {
        self.expiry.get(coll).and_then(|m| m.get(id)).is_some_and(|exp| *exp <= crate::lease::now_epoch_ms())
    }
    fn clear_expiry(&mut self, coll: &str, id: &str) {
        if let Some(m) = self.expiry.get_mut(coll) {
            m.remove(id);
        }
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed helper (fixtures/tests).
    pub fn add_row(&mut self, table: &str, row_id: &str, doc: Value) {
        self.tables.entry(table.to_string()).or_default().push(Row {
            row_id: row_id.to_string(),
            doc,
        });
    }
}

impl DataSource for MemoryStore {
    fn list_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }
    fn scan_table(&self, table: &str) -> Vec<Row> {
        self.tables.get(table).cloned().unwrap_or_default()
    }
    fn get_row(&self, table: &str, row_id: &str) -> Option<Value> {
        self.tables.get(table)?.iter().find(|r| r.row_id == row_id).map(|r| r.doc.clone())
    }
}

impl IndexStore for MemoryStore {
    fn put_object(&mut self, coll: &str, id: &str, obj: Value) {
        self.clear_expiry(coll, id); // a plain write clears any prior TTL
        self.objects.entry(coll.to_string()).or_default().insert(id.to_string(), obj);
    }
    fn get_object(&self, coll: &str, id: &str) -> Option<Value> {
        if self.is_expired(coll, id) {
            return None;
        }
        self.objects.get(coll)?.get(id).cloned()
    }
    fn scan_objects(&self, coll: &str) -> Vec<Value> {
        self.objects
            .get(coll)
            .map(|m| m.iter().filter(|(id, _)| !self.is_expired(coll, id)).map(|(_, v)| v.clone()).collect())
            .unwrap_or_default()
    }
    fn scan_entries(&self, coll: &str) -> Vec<(String, Value)> {
        self.objects
            .get(coll)
            .map(|m| m.iter().filter(|(id, _)| !self.is_expired(coll, id)).map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default()
    }
    fn clear_collection(&mut self, coll: &str) {
        self.objects.remove(coll);
        self.expiry.remove(coll);
    }
    fn delete_object(&mut self, coll: &str, id: &str) {
        if let Some(m) = self.objects.get_mut(coll) {
            m.remove(id);
        }
        self.clear_expiry(coll, id);
    }
    fn put_object_at(&mut self, coll: &str, id: &str, obj: Value, expire_at_ms: u64) {
        self.objects.entry(coll.to_string()).or_default().insert(id.to_string(), obj);
        self.expiry.entry(coll.to_string()).or_default().insert(id.to_string(), expire_at_ms);
    }
    fn expiry_of(&self, coll: &str, id: &str) -> Option<u64> {
        self.expiry.get(coll).and_then(|m| m.get(id)).copied()
    }
    fn sweep_expired(&mut self, now_ms: u64) -> usize {
        let mut dead: Vec<(String, String)> = Vec::new();
        for (coll, m) in &self.expiry {
            for (id, exp) in m {
                if *exp <= now_ms {
                    dead.push((coll.clone(), id.clone()));
                }
            }
        }
        for (coll, id) in &dead {
            if let Some(m) = self.objects.get_mut(coll) {
                m.remove(id);
            }
            if let Some(m) = self.expiry.get_mut(coll) {
                m.remove(id);
            }
        }
        dead.len()
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities { native_set_logic: false }
    }
}
