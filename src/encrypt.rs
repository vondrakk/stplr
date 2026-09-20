// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Encryption at rest as a store decorator.
//!
//! [`EncryptedStore`] wraps any [`IndexStore`] and encrypts each value with **AES-256-GCM** (a fresh
//! random nonce per write, authenticated) before it touches disk, decrypting on read. Like the
//! compression decorator it overrides only the value-touching trait methods, so set ops, CAS, INCR,
//! and scan — all built on `get_object`/`put_object` — are encrypted transparently, and it touches
//! none of the underlying store (zero-risk, opt-in). Keys are never written in clear: only the value
//! payloads are encrypted; collection + key names are not (they're the routing/index surface).
//!
//! The data key is derived as SHA-256 of the supplied key material (a passphrase or a 32-byte hex
//! key), so any string works as the secret. Decrypt is fail-safe: a value that isn't a well-formed
//! ciphertext envelope is returned verbatim (so plaintext written before encryption was enabled
//! still reads back — migrate by rewriting).

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use base64::Engine;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::store::{Capabilities, IndexStore};

/// Marker key identifying an encrypted value envelope.
const MARKER: &str = "__enc";

/// Derive a 32-byte AES key from arbitrary key material (passphrase or hex).
fn derive_key(material: &str) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(material.as_bytes());
    h.finalize().into()
}

fn encrypt(cipher: &Aes256Gcm, v: &Value) -> Value {
    let plaintext = match serde_json::to_vec(v) {
        Ok(b) => b,
        Err(_) => return v.clone(),
    };
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng); // 96-bit, unique per write
    match cipher.encrypt(&nonce, plaintext.as_ref()) {
        Ok(ct) => {
            let mut blob = nonce.to_vec();
            blob.extend(ct);
            json!({ MARKER: base64::engine::general_purpose::STANDARD.encode(blob) })
        }
        Err(_) => v.clone(),
    }
}

fn decrypt(cipher: &Aes256Gcm, v: Value) -> Value {
    if let Value::Object(m) = &v {
        if m.len() == 1 {
            if let Some(Value::String(b64)) = m.get(MARKER) {
                if let Ok(blob) = base64::engine::general_purpose::STANDARD.decode(b64) {
                    if blob.len() > 12 {
                        let (nonce, ct) = blob.split_at(12);
                        if let Ok(pt) = cipher.decrypt(Nonce::from_slice(nonce), ct) {
                            if let Ok(orig) = serde_json::from_slice::<Value>(&pt) {
                                return orig;
                            }
                        }
                    }
                }
            }
        }
    }
    v
}

/// Wraps a store to encrypt values at rest with AES-256-GCM.
pub struct EncryptedStore<S> {
    inner: S,
    cipher: Aes256Gcm,
}

impl<S> EncryptedStore<S> {
    /// `key_material` is any secret string (a passphrase or 32-byte hex key); the data key is its
    /// SHA-256.
    pub fn new(inner: S, key_material: &str) -> Self {
        let key = derive_key(key_material);
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key));
        Self { inner, cipher }
    }
    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S: IndexStore> IndexStore for EncryptedStore<S> {
    fn put_object(&mut self, coll: &str, id: &str, obj: Value) {
        self.inner.put_object(coll, id, encrypt(&self.cipher, &obj));
    }
    fn put_object_at(&mut self, coll: &str, id: &str, obj: Value, expire_at_ms: u64) {
        self.inner.put_object_at(coll, id, encrypt(&self.cipher, &obj), expire_at_ms);
    }
    fn put_batch(&mut self, items: Vec<(String, String, Value)>) {
        let enc = items.into_iter().map(|(c, k, v)| (c, k, encrypt(&self.cipher, &v))).collect();
        self.inner.put_batch(enc);
    }
    fn get_object(&self, coll: &str, id: &str) -> Option<Value> {
        self.inner.get_object(coll, id).map(|v| decrypt(&self.cipher, v))
    }
    fn scan_objects(&self, coll: &str) -> Vec<Value> {
        self.inner.scan_objects(coll).into_iter().map(|v| decrypt(&self.cipher, v)).collect()
    }
    fn scan_entries(&self, coll: &str) -> Vec<(String, Value)> {
        self.inner.scan_entries(coll).into_iter().map(|(k, v)| (k, decrypt(&self.cipher, v))).collect()
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
    // set ops / cas / incr use the trait defaults, which route through our get/put — so they operate
    // on decrypted values and re-encrypt transparently.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    #[test]
    fn encrypts_at_rest_and_round_trips() {
        let mut s = EncryptedStore::new(MemoryStore::new(), "correct horse battery staple");
        let v = json!({"ssn": "123-45-6789", "balance": 4200});
        s.put_object("kv", "acct", v.clone());
        assert_eq!(s.get_object("kv", "acct"), Some(v.clone()), "decrypts to original");

        // the inner store holds ciphertext, not the plaintext
        let raw = s.inner().get_object("kv", "acct").unwrap();
        assert!(raw.get(MARKER).is_some(), "stored as an encrypted envelope");
        let stored = serde_json::to_string(&raw).unwrap();
        assert!(!stored.contains("123-45-6789"), "plaintext is NOT on disk");
    }

    #[test]
    fn wrong_key_cannot_decrypt() {
        let mut a = EncryptedStore::new(MemoryStore::new(), "key-A");
        a.put_object("kv", "x", json!("secret"));
        let envelope = a.inner().get_object("kv", "x").unwrap();
        // a store with a different key, fed the same ciphertext, fails to recover the plaintext
        let b = EncryptedStore::new(MemoryStore::new(), "key-B");
        let recovered = decrypt(&b.cipher, envelope.clone());
        assert_eq!(recovered, envelope, "wrong key -> ciphertext returned, not plaintext");
    }

    #[test]
    fn set_ops_and_cas_work_through_encryption() {
        let mut s = EncryptedStore::new(MemoryStore::new(), "k");
        assert!(s.set_add("tags", "x", "a"));
        assert!(s.set_add("tags", "x", "b"));
        assert_eq!(s.set_members("tags", "x"), vec!["a".to_string(), "b".to_string()]);
        assert!(s.cas("kv", "c", None, json!(1)));
        assert!(!s.cas("kv", "c", None, json!(2)));
        assert_eq!(s.get_object("kv", "c"), Some(json!(1)));
        // the set is stored encrypted too
        assert!(s.inner().get_object("tags", "x").unwrap().get(MARKER).is_some());
    }

    #[test]
    fn plaintext_written_before_encryption_still_reads() {
        // a value that isn't an encrypted envelope is returned verbatim (smooth migration)
        let s = EncryptedStore::new(MemoryStore::new(), "k");
        let plain = json!({"hello": "world"});
        assert_eq!(decrypt(&s.cipher, plain.clone()), plain);
    }
}
