// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Transparent value compression as a store decorator.
//!
//! [`CompressedStore`] wraps any [`IndexStore`] and lz4-compresses values above a size threshold on
//! the way in, decompressing on the way out. It only overrides the value-touching trait methods, so
//! everything built on `get_object`/`put_object` — set ops, CAS, INCR, scan — gets compression for
//! free. It touches none of the underlying store, so it's zero-risk and opt-in (wrap, or don't).
//!
//! Compressed values are stored as `{"<MARKER>": "<base64 lz4>"}`; decode is fail-safe (a value that
//! merely looks like the wrapper but doesn't decompress is returned verbatim). The base64 wrapper
//! adds ~33% to the *compressed* bytes — for typical JSON, lz4 still nets a large saving, and values
//! that wouldn't shrink are stored uncompressed. (A raw-bytes codec with no base64 is a follow-up.)

use base64::Engine;
use serde_json::{json, Value};

use crate::store::{Capabilities, IndexStore};

/// Marker key identifying a compressed value envelope.
const MARKER: &str = "__stplr_z";

fn encode(v: &Value, min_size: usize) -> Value {
    let bytes = match serde_json::to_vec(v) {
        Ok(b) => b,
        Err(_) => return v.clone(),
    };
    if bytes.len() < min_size {
        return v.clone();
    }
    let compressed = lz4_flex::compress_prepend_size(&bytes);
    let b64 = base64::engine::general_purpose::STANDARD.encode(&compressed);
    // Only wrap if it actually saves space (account for the JSON wrapper overhead).
    if b64.len() + MARKER.len() + 8 >= bytes.len() {
        return v.clone();
    }
    json!({ MARKER: b64 })
}

fn decode(v: Value) -> Value {
    if let Value::Object(m) = &v {
        if m.len() == 1 {
            if let Some(Value::String(b64)) = m.get(MARKER) {
                if let Ok(comp) = base64::engine::general_purpose::STANDARD.decode(b64) {
                    if let Ok(bytes) = lz4_flex::decompress_size_prepended(&comp) {
                        if let Ok(orig) = serde_json::from_slice::<Value>(&bytes) {
                            return orig;
                        }
                    }
                }
            }
        }
    }
    v
}

/// Wraps a store to compress values larger than `min_size` bytes.
pub struct CompressedStore<S> {
    inner: S,
    min_size: usize,
}

impl<S> CompressedStore<S> {
    pub fn new(inner: S, min_size: usize) -> Self {
        Self { inner, min_size }
    }
    /// The wrapped store (values are in their compressed-on-disk form here).
    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S: IndexStore> IndexStore for CompressedStore<S> {
    // value-touching: encode in, decode out
    fn put_object(&mut self, coll: &str, id: &str, obj: Value) {
        self.inner.put_object(coll, id, encode(&obj, self.min_size));
    }
    fn put_object_at(&mut self, coll: &str, id: &str, obj: Value, expire_at_ms: u64) {
        self.inner.put_object_at(coll, id, encode(&obj, self.min_size), expire_at_ms);
    }
    fn put_batch(&mut self, items: Vec<(String, String, Value)>) {
        let min = self.min_size;
        let enc = items.into_iter().map(|(c, k, v)| (c, k, encode(&v, min))).collect();
        self.inner.put_batch(enc);
    }
    fn get_object(&self, coll: &str, id: &str) -> Option<Value> {
        self.inner.get_object(coll, id).map(decode)
    }
    fn scan_objects(&self, coll: &str) -> Vec<Value> {
        self.inner.scan_objects(coll).into_iter().map(decode).collect()
    }
    fn scan_entries(&self, coll: &str) -> Vec<(String, Value)> {
        self.inner.scan_entries(coll).into_iter().map(|(k, v)| (k, decode(v))).collect()
    }

    // key-only / bookkeeping: straight delegation (no value involved)
    fn scan_range(&self, coll: &str, after: Option<&str>, prefix: Option<&str>, end: Option<&str>, limit: usize) -> Vec<String> {
        self.inner.scan_range(coll, after, prefix, end, limit)
    }
    fn clear_collection(&mut self, coll: &str) {
        self.inner.clear_collection(coll);
    }
    fn delete_object(&mut self, coll: &str, id: &str) {
        self.inner.delete_object(coll, id);
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
    // set ops / cas / incr / mget intentionally use the trait defaults, which route through our
    // get_object/put_object above — so they operate on decompressed values transparently.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    #[test]
    fn round_trips_and_actually_compresses() {
        let mut s = CompressedStore::new(MemoryStore::new(), 64);

        // a large, compressible value
        let big = Value::String("stplr-".repeat(200)); // 1200 bytes, very compressible
        s.put_object("kv", "big", big.clone());
        assert_eq!(s.get_object("kv", "big"), Some(big.clone()), "decompresses to the original");

        // it's stored compressed in the inner store (marker present)
        let raw = s.inner().get_object("kv", "big").unwrap();
        assert!(raw.get(MARKER).is_some(), "large value stored compressed");
        assert!(
            serde_json::to_vec(&raw).unwrap().len() < serde_json::to_vec(&big).unwrap().len(),
            "compressed form is smaller"
        );

        // a small value is stored as-is (compression wouldn't help)
        s.put_object("kv", "small", Value::from(42));
        assert_eq!(s.get_object("kv", "small"), Some(Value::from(42)));
        assert!(s.inner().get_object("kv", "small").unwrap().get(MARKER).is_none(), "small value not wrapped");
    }

    #[test]
    fn set_ops_cas_incr_scan_work_through_decorator() {
        let mut s = CompressedStore::new(MemoryStore::new(), 64);

        assert!(s.set_add("tags", "x", "a"));
        assert!(s.set_add("tags", "x", "b"));
        assert_eq!(s.set_members("tags", "x"), vec!["a".to_string(), "b".to_string()]);
        assert!(s.set_remove("tags", "x", "a"));
        assert_eq!(s.set_members("tags", "x"), vec!["b".to_string()]);

        assert_eq!(s.incr("kv", "n", 5), Some(5));
        assert_eq!(s.incr("kv", "n", 3), Some(8));

        assert!(s.cas("kv", "c", None, Value::from("v")));
        assert!(!s.cas("kv", "c", None, Value::from("w")));
        assert_eq!(s.get_object("kv", "c"), Some(Value::from("v")));

        // a large value survives scan + delete
        let big = json!({"blob": "x".repeat(500)});
        s.put_object("kv", "big", big.clone());
        assert!(s.scan_entries("kv").iter().any(|(k, v)| k == "big" && *v == big));
        s.delete_object("kv", "big");
        assert_eq!(s.get_object("kv", "big"), None);
    }

    #[test]
    fn decode_is_fail_safe_on_lookalike_values() {
        let mut s = CompressedStore::new(MemoryStore::new(), 1 << 30); // never compress
        // a user value that happens to look like the envelope must round-trip untouched
        let lookalike = json!({ MARKER: "not-actually-base64-lz4" });
        s.put_object("kv", "k", lookalike.clone());
        assert_eq!(s.get_object("kv", "k"), Some(lookalike));
    }
}
