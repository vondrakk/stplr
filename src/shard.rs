// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! One cluster node's local data — Rust port of `cluster/shard.ts`. A generic object + set store:
//! holds opaque objects by (collection, key), applies incremental set-membership writes, answers
//! object reads, and supports the reshard primitives (export/import/drop a bucket's entries). It
//! has no knowledge of how the objects it stores were built — the correlation construction
//! (US 11,151,112) lives in the engine layer, above the substrate.

use std::collections::HashSet;

use serde_json::Value;

use crate::lease::{evaluate, now_epoch_ms, Lease, LeaseOutcome, LEASE_COLL};
use crate::partitioner::bucket_of;
use crate::store::IndexStore;

pub struct Shard<St> {
    pub id: String,
    store: St,
}

impl<St: IndexStore> Shard<St> {
    pub fn new(id: &str, store: St) -> Self {
        Self { id: id.to_string(), store }
    }

    pub fn object(&self, coll: &str, key: &str) -> Option<Value> {
        self.store.get_object(coll, key)
    }
    pub fn scan_range(&self, coll: &str, after: Option<&str>, prefix: Option<&str>, end: Option<&str>, limit: usize) -> Vec<String> {
        self.store.scan_range(coll, after, prefix, end, limit)
    }

    // --- generic posting-list set ops (cluster-routed) ---
    pub fn set_add(&mut self, coll: &str, key: &str, member: &str) -> bool {
        self.store.set_add(coll, key, member)
    }
    pub fn set_remove(&mut self, coll: &str, key: &str, member: &str) -> bool {
        self.store.set_remove(coll, key, member)
    }
    pub fn set_members(&self, coll: &str, key: &str) -> Vec<String> {
        self.store.set_members(coll, key)
    }

    pub fn write_object(&mut self, coll: &str, key: &str, obj: Value) {
        self.store.put_object(coll, key, obj);
    }
    /// Write `obj` that expires `ttl_ms` from now (the shard stamps the absolute time, so all
    /// replicas use a consistent clock source). `ttl_ms == 0` writes with no expiry.
    pub fn write_object_ttl(&mut self, coll: &str, key: &str, obj: Value, ttl_ms: u64) {
        if ttl_ms == 0 {
            self.store.put_object(coll, key, obj);
        } else {
            self.store.put_object_at(coll, key, obj, now_epoch_ms() + ttl_ms);
        }
    }
    /// Reclaim expired keys; returns how many were dropped.
    pub fn sweep_expired(&mut self) -> usize {
        self.store.sweep_expired(now_epoch_ms())
    }

    /// Atomic compare-and-set (see [`IndexStore::cas`]).
    pub fn cas(&mut self, coll: &str, key: &str, expect: Option<Value>, new: Value) -> bool {
        self.store.cas(coll, key, expect, new)
    }
    /// Atomic integer add (see [`IndexStore::incr`]).
    pub fn incr(&mut self, coll: &str, key: &str, delta: i64) -> Option<i64> {
        self.store.incr(coll, key, delta)
    }
    /// Group-commit many object writes (one txn on durable backends).
    pub fn write_batch(&mut self, items: Vec<(String, String, Value)>) {
        self.store.put_batch(items);
    }
    pub fn delete_object(&mut self, coll: &str, key: &str) {
        self.store.delete_object(coll, key);
    }

    /// Hot-copy this node's durable store to `dest` (Err if the backend isn't durable).
    pub fn snapshot(&self, dest: &std::path::Path) -> Result<(), String> {
        self.store.snapshot(dest)
    }

    /// Atomically acquire or renew a TTL lease at `key` (leader election). The whole read→decide→
    /// write happens under the caller's exclusive shard lock, so concurrent acquires from rival
    /// coordinators are serialized here — exactly one wins per term. Returns the outcome (granted +
    /// the authoritative lease record). See [`crate::lease`] for the correctness scope.
    pub fn lease_acquire(&mut self, key: &str, holder: &str, ttl_ms: u64) -> LeaseOutcome {
        let current: Option<Lease> =
            self.store.get_object(LEASE_COLL, key).and_then(|v| serde_json::from_value(v).ok());
        let outcome = evaluate(current.as_ref(), holder, ttl_ms, now_epoch_ms());
        if outcome.granted {
            self.store.put_object(LEASE_COLL, key, serde_json::to_value(&outcome.lease).unwrap());
        }
        outcome
    }

    /// Every (id, obj) this node holds in `coll` whose id falls in one of `buckets`.
    pub fn export_entries(&self, coll: &str, buckets: &[usize]) -> Vec<(String, Value)> {
        let want: HashSet<usize> = buckets.iter().copied().collect();
        self.store
            .scan_entries(coll)
            .into_iter()
            .filter(|(id, _)| want.contains(&bucket_of(id)))
            .collect()
    }

    pub fn import_entries(&mut self, coll: &str, entries: Vec<(String, Value)>) {
        for (id, obj) in entries {
            self.store.put_object(coll, &id, obj);
        }
    }

    /// Delete this node's entries in `coll` that fall in `buckets`; returns how many.
    pub fn drop_buckets(&mut self, coll: &str, buckets: &[usize]) -> usize {
        let want: HashSet<usize> = buckets.iter().copied().collect();
        let ids: Vec<String> = self
            .store
            .scan_entries(coll)
            .into_iter()
            .filter(|(id, _)| want.contains(&bucket_of(id)))
            .map(|(id, _)| id)
            .collect();
        for id in &ids {
            self.store.delete_object(coll, id);
        }
        ids.len()
    }
}
