// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! The coordinator's transport seam — Rust port of `cluster/shardClient.ts`. One async trait,
//! two impls: `InProcessShardClient` (co-located shard, no network) and `HttpShardClient` (remote,
//! in net.rs). The coordinator only ever talks to `ShardClient`s, so in-process and networked
//! clusters share one code path.

use async_trait::async_trait;
use serde_json::Value;

use crate::lease::LeaseOutcome;
use crate::net::SharedShard;

/// A client call result: Ok, or Err(reason) — the coordinator fails over to the next replica.
pub type CResult<T> = Result<T, String>;

/// The coordinator's transport to a shard. Generic key/value + set-membership + reshard operations
/// only — no correlation vocabulary (no `Occurrence`, no `Location`). The engine layer builds its
/// structures above this and stores them as opaque objects.
#[async_trait]
pub trait ShardClient: Send + Sync {
    fn id(&self) -> &str;
    async fn object(&self, coll: &str, key: &str) -> CResult<Option<Value>>;
    /// Key iteration on this shard with optional cursor/prefix/end bounds. Default returns empty
    /// (backends that can't enumerate); shard-backed clients override it.
    async fn scan_range(&self, _coll: &str, _after: Option<&str>, _prefix: Option<&str>, _end: Option<&str>, _limit: usize) -> CResult<Vec<String>> {
        Ok(Vec::new())
    }
    /// Bucket-filtered key scan on this shard (partition-aware reads). Default returns empty.
    async fn scan_buckets(&self, _coll: &str, _buckets: &[usize], _after: Option<&str>, _limit: usize) -> CResult<Vec<String>> {
        Ok(Vec::new())
    }
    /// Batch get on this shard — a value (or None) per key, in order. Default loops `object`.
    async fn mget(&self, coll: &str, keys: &[String]) -> CResult<Vec<Option<Value>>> {
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            out.push(self.object(coll, k).await?);
        }
        Ok(out)
    }
    async fn set_add(&self, coll: &str, key: &str, member: &str) -> CResult<bool>;
    async fn set_remove(&self, coll: &str, key: &str, member: &str) -> CResult<bool>;
    async fn write_object(&self, coll: &str, key: &str, obj: Value) -> CResult<()>;
    /// Write with a time-to-live (ms). Default ignores the TTL (writes without expiry) so backends
    /// that don't support it still compile; shard-backed clients override it.
    async fn write_object_ttl(&self, coll: &str, key: &str, obj: Value, _ttl_ms: u64) -> CResult<()> {
        self.write_object(coll, key, obj).await
    }
    async fn delete_object(&self, coll: &str, key: &str) -> CResult<()>;
    /// Atomic compare-and-set; default unsupported (shard-backed clients override).
    async fn cas(&self, _coll: &str, _key: &str, _expect: Option<Value>, _new: Value) -> CResult<bool> {
        Err("cas not supported by this client".into())
    }
    /// Atomic integer add; default unsupported. `Ok(None)` = value wasn't an integer.
    async fn incr(&self, _coll: &str, _key: &str, _delta: i64) -> CResult<Option<i64>> {
        Err("incr not supported by this client".into())
    }
    async fn export_entries(&self, coll: &str, buckets: Vec<usize>) -> CResult<Vec<(String, Value)>>;
    async fn import_entries(&self, coll: &str, entries: Vec<(String, Value)>) -> CResult<()>;
    async fn drop_buckets(&self, coll: &str, buckets: Vec<usize>) -> CResult<usize>;
    async fn health(&self) -> bool;
    /// The shard's base URL (so a direct-to-shard client can be told where to connect). Empty for
    /// in-process clients, which can't be reached over the network.
    fn endpoint(&self) -> &str {
        ""
    }
    /// Atomically acquire/renew a TTL lease at `key` (leader election). Default: unsupported — only
    /// shard-backed clients implement it. See [`crate::lease`].
    async fn lease_acquire(&self, _key: &str, _holder: &str, _ttl_ms: u64) -> CResult<LeaseOutcome> {
        Err("lease_acquire not supported by this client".into())
    }
}

/// In-process client — used for co-located shards. Locks the shared shard and calls it directly;
/// no serialization, no network. The lock is held only for the (synchronous) op, never across await.
pub struct InProcessShardClient {
    id: String,
    shard: SharedShard,
}

impl InProcessShardClient {
    pub fn new(id: &str, shard: SharedShard) -> Self {
        Self { id: id.to_string(), shard }
    }
}

#[async_trait]
impl ShardClient for InProcessShardClient {
    fn id(&self) -> &str {
        &self.id
    }
    async fn object(&self, coll: &str, key: &str) -> CResult<Option<Value>> {
        Ok(self.shard.read().unwrap().object(coll, key))
    }
    async fn scan_range(&self, coll: &str, after: Option<&str>, prefix: Option<&str>, end: Option<&str>, limit: usize) -> CResult<Vec<String>> {
        Ok(self.shard.read().unwrap().scan_range(coll, after, prefix, end, limit))
    }
    async fn mget(&self, coll: &str, keys: &[String]) -> CResult<Vec<Option<Value>>> {
        Ok(self.shard.read().unwrap().mget(coll, keys))
    }
    async fn scan_buckets(&self, coll: &str, buckets: &[usize], after: Option<&str>, limit: usize) -> CResult<Vec<String>> {
        Ok(self.shard.read().unwrap().scan_buckets(coll, buckets, after, limit))
    }
    async fn set_add(&self, coll: &str, key: &str, member: &str) -> CResult<bool> {
        Ok(self.shard.write().unwrap().set_add(coll, key, member))
    }
    async fn set_remove(&self, coll: &str, key: &str, member: &str) -> CResult<bool> {
        Ok(self.shard.write().unwrap().set_remove(coll, key, member))
    }
    async fn write_object(&self, coll: &str, key: &str, obj: Value) -> CResult<()> {
        self.shard.write().unwrap().write_object(coll, key, obj);
        Ok(())
    }
    async fn write_object_ttl(&self, coll: &str, key: &str, obj: Value, ttl_ms: u64) -> CResult<()> {
        self.shard.write().unwrap().write_object_ttl(coll, key, obj, ttl_ms);
        Ok(())
    }
    async fn cas(&self, coll: &str, key: &str, expect: Option<Value>, new: Value) -> CResult<bool> {
        Ok(self.shard.write().unwrap().cas(coll, key, expect, new))
    }
    async fn incr(&self, coll: &str, key: &str, delta: i64) -> CResult<Option<i64>> {
        Ok(self.shard.write().unwrap().incr(coll, key, delta))
    }
    async fn delete_object(&self, coll: &str, key: &str) -> CResult<()> {
        self.shard.write().unwrap().delete_object(coll, key);
        Ok(())
    }
    async fn export_entries(&self, coll: &str, buckets: Vec<usize>) -> CResult<Vec<(String, Value)>> {
        Ok(self.shard.read().unwrap().export_entries(coll, &buckets))
    }
    async fn import_entries(&self, coll: &str, entries: Vec<(String, Value)>) -> CResult<()> {
        self.shard.write().unwrap().import_entries(coll, entries);
        Ok(())
    }
    async fn drop_buckets(&self, coll: &str, buckets: Vec<usize>) -> CResult<usize> {
        Ok(self.shard.write().unwrap().drop_buckets(coll, &buckets))
    }
    async fn health(&self) -> bool {
        true
    }
    async fn lease_acquire(&self, key: &str, holder: &str, ttl_ms: u64) -> CResult<LeaseOutcome> {
        Ok(self.shard.write().unwrap().lease_acquire(key, holder, ttl_ms))
    }
}
