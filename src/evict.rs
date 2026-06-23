// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Capacity-bounded eviction (cache mode) as a store decorator.
//!
//! [`EvictingStore`] caps how many keys a store holds and, on overflow, evicts one by **LRU** (least
//! recently used) or **LFU** (least frequently used). It's a *cache* mode — for the in-memory backend
//! you want bounded, not unbounded; a durable backend is normally left unbounded (the disk is the
//! database), so reach for this on `--store memory`.
//!
//! Access is tracked on point reads/writes (`get_object`/`put_object`, and the set/CAS/INCR ops that
//! build on them); bulk scans don't disturb recency. Eviction is O(n) over tracked keys per insert
//! (a fine default for cache-sized working sets; an intrusive-list LRU is a possible follow-up).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde_json::Value;

use crate::store::{Capabilities, IndexStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictPolicy {
    Lru,
    Lfu,
}

impl EvictPolicy {
    /// Parse `lru` / `lfu` (case-insensitive); anything else is `None`.
    pub fn parse(s: &str) -> Option<EvictPolicy> {
        match s.trim().to_ascii_lowercase().as_str() {
            "lru" => Some(EvictPolicy::Lru),
            "lfu" => Some(EvictPolicy::Lfu),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
struct Meta {
    freq: u64,
    last: u64,
}

/// Wraps a store with a key-count ceiling + eviction policy.
pub struct EvictingStore<S> {
    inner: S,
    capacity: usize,
    policy: EvictPolicy,
    meta: Mutex<HashMap<(String, String), Meta>>,
    clock: AtomicU64,
}

impl<S: IndexStore> EvictingStore<S> {
    pub fn new(inner: S, capacity: usize, policy: EvictPolicy) -> Self {
        Self { inner, capacity: capacity.max(1), policy, meta: Mutex::new(HashMap::new()), clock: AtomicU64::new(0) }
    }
    pub fn inner(&self) -> &S {
        &self.inner
    }
    /// Keys currently tracked (== held, for a cache that started empty).
    pub fn len(&self) -> usize {
        self.meta.lock().unwrap().len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn touch(&self, coll: &str, id: &str) {
        let now = self.clock.fetch_add(1, Ordering::Relaxed);
        let mut m = self.meta.lock().unwrap();
        let e = m.entry((coll.to_string(), id.to_string())).or_insert(Meta { freq: 0, last: now });
        e.freq += 1;
        e.last = now;
    }

    /// Record a write; if it newly pushes the key count over capacity, pick a victim to evict
    /// (returns it for the caller to delete from the inner store outside the meta lock).
    fn note_write(&self, coll: &str, id: &str) -> Option<(String, String)> {
        let now = self.clock.fetch_add(1, Ordering::Relaxed);
        let key = (coll.to_string(), id.to_string());
        let mut m = self.meta.lock().unwrap();
        let is_new = !m.contains_key(&key);
        let e = m.entry(key.clone()).or_insert(Meta { freq: 0, last: now });
        e.freq += 1;
        e.last = now;
        if is_new && m.len() > self.capacity {
            let victim = m
                .iter()
                .filter(|(k, _)| **k != key)
                .min_by_key(|(_, meta)| match self.policy {
                    EvictPolicy::Lru => meta.last,
                    EvictPolicy::Lfu => meta.freq,
                })
                .map(|(k, _)| k.clone());
            if let Some(v) = &victim {
                m.remove(v);
            }
            return victim;
        }
        None
    }

    fn forget(&self, coll: &str, id: &str) {
        self.meta.lock().unwrap().remove(&(coll.to_string(), id.to_string()));
    }
}

impl<S: IndexStore> IndexStore for EvictingStore<S> {
    fn put_object(&mut self, coll: &str, id: &str, obj: Value) {
        if let Some((vc, vk)) = self.note_write(coll, id) {
            self.inner.delete_object(&vc, &vk);
        }
        self.inner.put_object(coll, id, obj);
    }
    fn put_object_at(&mut self, coll: &str, id: &str, obj: Value, expire_at_ms: u64) {
        if let Some((vc, vk)) = self.note_write(coll, id) {
            self.inner.delete_object(&vc, &vk);
        }
        self.inner.put_object_at(coll, id, obj, expire_at_ms);
    }
    fn put_batch(&mut self, items: Vec<(String, String, Value)>) {
        let mut victims = Vec::new();
        for (c, k, _) in &items {
            if let Some(v) = self.note_write(c, k) {
                victims.push(v);
            }
        }
        for (vc, vk) in victims {
            self.inner.delete_object(&vc, &vk);
        }
        self.inner.put_batch(items);
    }
    fn get_object(&self, coll: &str, id: &str) -> Option<Value> {
        let v = self.inner.get_object(coll, id);
        if v.is_some() {
            self.touch(coll, id);
        }
        v
    }
    fn delete_object(&mut self, coll: &str, id: &str) {
        self.forget(coll, id);
        self.inner.delete_object(coll, id);
    }
    fn clear_collection(&mut self, coll: &str) {
        self.meta.lock().unwrap().retain(|(c, _), _| c != coll);
        self.inner.clear_collection(coll);
    }
    // pass-through (no recency effect / no value transform)
    fn scan_objects(&self, coll: &str) -> Vec<Value> {
        self.inner.scan_objects(coll)
    }
    fn scan_entries(&self, coll: &str) -> Vec<(String, Value)> {
        self.inner.scan_entries(coll)
    }
    fn scan_range(&self, coll: &str, after: Option<&str>, prefix: Option<&str>, end: Option<&str>, limit: usize) -> Vec<String> {
        self.inner.scan_range(coll, after, prefix, end, limit)
    }
    fn expiry_of(&self, coll: &str, id: &str) -> Option<u64> {
        self.inner.expiry_of(coll, id)
    }
    fn sweep_expired(&mut self, now_ms: u64) -> usize {
        self.inner.sweep_expired(now_ms)
    }
    fn snapshot(&self, dest: &std::path::Path) -> Result<(), String> {
        self.inner.snapshot(dest)
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;
    use serde_json::json;

    #[test]
    fn lru_evicts_least_recently_used() {
        let mut s = EvictingStore::new(MemoryStore::new(), 2, EvictPolicy::Lru);
        s.put_object("kv", "a", json!(1));
        s.put_object("kv", "b", json!(2));
        // touch a so b is the LRU
        let _ = s.get_object("kv", "a");
        // inserting c (capacity 2) evicts b
        s.put_object("kv", "c", json!(3));
        assert_eq!(s.len(), 2);
        assert!(s.get_object("kv", "b").is_none(), "LRU victim b evicted");
        assert_eq!(s.get_object("kv", "a"), Some(json!(1)));
        assert_eq!(s.get_object("kv", "c"), Some(json!(3)));
    }

    #[test]
    fn lfu_evicts_least_frequently_used() {
        let mut s = EvictingStore::new(MemoryStore::new(), 2, EvictPolicy::Lfu);
        s.put_object("kv", "a", json!(1));
        s.put_object("kv", "b", json!(2));
        // hammer a's frequency; b stays least-frequent
        for _ in 0..5 {
            let _ = s.get_object("kv", "a");
        }
        s.put_object("kv", "c", json!(3));
        assert!(s.get_object("kv", "b").is_none(), "LFU victim b evicted");
        assert_eq!(s.get_object("kv", "a"), Some(json!(1)));
    }

    #[test]
    fn delete_frees_capacity() {
        let mut s = EvictingStore::new(MemoryStore::new(), 2, EvictPolicy::Lru);
        s.put_object("kv", "a", json!(1));
        s.put_object("kv", "b", json!(2));
        s.delete_object("kv", "a");
        assert_eq!(s.len(), 1);
        // now c fits without eviction
        s.put_object("kv", "c", json!(3));
        assert_eq!(s.get_object("kv", "b"), Some(json!(2)), "b not evicted — delete freed room");
        assert_eq!(s.get_object("kv", "c"), Some(json!(3)));
    }

    #[test]
    fn policy_parse() {
        assert_eq!(EvictPolicy::parse("LRU"), Some(EvictPolicy::Lru));
        assert_eq!(EvictPolicy::parse("lfu"), Some(EvictPolicy::Lfu));
        assert_eq!(EvictPolicy::parse("nope"), None);
    }
}
