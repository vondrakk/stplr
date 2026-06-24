// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! stplr's cluster front door — the generic, self-managing distributed store. Async and
//! interior-mutable so a migration runs *concurrently* with reads/writes (online rebalance). It
//! routes generic object/set operations with replication + failover, and rebalances on membership
//! change with crash-resume.
//!
//! This layer is pure substrate: it knows keys, collections, opaque `Value` objects, opaque set
//! members, and buckets — nothing about correlation. The engine (US 11,151,112) sits ON TOP of it
//! (see coordinator.rs) and never leaks back down, so this module has no dependency on the engine.
//!
//! Online migration: routing consults a per-bucket overlay (old owners authoritative until a
//! bucket is copied + cut over), and a freeze/drain gate (see guard.rs) makes in-flight ops on a
//! migrating bucket drain before the copy and new ops wait for cutover. All methods are `&self`;
//! a `migrating` lock serializes migrations without blocking traffic.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::client::ShardClient;
use crate::guard::Gates;
use crate::journal::{InMemoryJournal, MigrationJournal, MigrationRecord};
use crate::partitioner::{bucket_of, NodeId, Partitioner};
use crate::reshard::plan_reshard;

/// One read partition of the keyspace: a shard, its endpoint, and the buckets it primary-owns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partition {
    pub id: NodeId,
    pub endpoint: String,
    pub buckets: Vec<usize>,
}

const CHUNK: usize = 256;

fn clamp(target: usize, n: usize) -> usize {
    target.min(n.max(1)).max(1)
}

pub struct MigrateSummary {
    pub buckets: usize,
    pub moved_buckets: usize,
    pub copied: usize,
    pub dropped: usize,
}

pub struct RouteInfo {
    pub key_value: String,
    pub owners: Vec<NodeId>,
    pub served_by: Option<NodeId>,
}

/// Routing overlay active during a migration: a bucket resolves to its OLD owners until it has
/// been migrated, then to its NEW owners.
struct Overlay {
    old_p: Partitioner,
    r_old: usize,
    new_p: Partitioner,
    r_new: usize,
    migrated: HashSet<usize>,
}

pub struct Cluster {
    partitioner: RwLock<Partitioner>,
    clients: RwLock<HashMap<NodeId, Arc<dyn ShardClient>>>,
    replication_target: AtomicUsize,
    replication: AtomicUsize,
    chunk: usize,
    down: RwLock<HashSet<NodeId>>,
    journal: Arc<dyn MigrationJournal>,
    overlay: Mutex<Option<Overlay>>,
    gates: Arc<Gates>,
    migrating: tokio::sync::Mutex<()>,
    /// The collections this cluster manages (migrated as a unit). Opaque namespace names supplied
    /// by the caller — the cluster attaches no meaning to them.
    colls: Vec<String>,
    /// node -> rack/zone for rack-aware replica placement (empty = plain HRW). Applied to the live
    /// partitioner and to every partitioner built for a migration overlay.
    topology: RwLock<HashMap<NodeId, String>>,
    /// Membership epoch — bumps on every committed membership change. A direct-to-shard
    /// [`crate::smart::SmartClient`] polls this to know when to refresh its routing view.
    epoch: AtomicU64,
}

impl Cluster {
    pub fn new(clients: HashMap<NodeId, Arc<dyn ShardClient>>, replication: usize, colls: Vec<String>) -> Self {
        let ids: Vec<NodeId> = clients.keys().cloned().collect();
        let rt = replication.max(1);
        let r = clamp(rt, ids.len());
        Cluster {
            partitioner: RwLock::new(Partitioner::new(ids)),
            clients: RwLock::new(clients),
            replication_target: AtomicUsize::new(rt),
            replication: AtomicUsize::new(r),
            chunk: CHUNK,
            down: RwLock::new(HashSet::new()),
            journal: Arc::new(InMemoryJournal::new()),
            overlay: Mutex::new(None),
            gates: Gates::new(),
            migrating: tokio::sync::Mutex::new(()),
            colls,
            topology: RwLock::new(HashMap::new()),
            epoch: AtomicU64::new(1),
        }
    }

    pub fn with_journal(mut self, journal: Arc<dyn MigrationJournal>) -> Self {
        self.journal = journal;
        self
    }

    /// Builder form of [`set_topology`] — set the node→rack map for rack-aware replica placement.
    pub fn with_topology(self, topology: HashMap<NodeId, String>) -> Self {
        self.set_topology(topology);
        self
    }

    /// Set (or clear, if empty) the node→rack map. Replicas of a key then spread across distinct
    /// racks (best-effort), so a single rack/AZ loss can't take every copy. Applies to the live
    /// partitioner immediately; later overlays inherit it via [`Cluster::make_partitioner`].
    pub fn set_topology(&self, topology: HashMap<NodeId, String>) {
        *self.topology.write().unwrap() = topology.clone();
        self.partitioner.write().unwrap().set_topology(topology);
    }

    /// Build a partitioner over `nodes` carrying the current topology (used for migration overlays
    /// so a reshard places/relocates replicas rack-aware too).
    fn make_partitioner(&self, nodes: Vec<NodeId>) -> Partitioner {
        let mut p = Partitioner::new(nodes);
        let t = self.topology.read().unwrap();
        if !t.is_empty() {
            p.set_topology(t.clone());
        }
        p
    }

    pub fn with_chunk(mut self, chunk: usize) -> Self {
        self.chunk = chunk.max(1);
        self
    }

    pub fn mark_down(&self, id: &str) {
        self.down.write().unwrap().insert(id.to_string());
    }

    pub fn member_ids(&self) -> Vec<NodeId> {
        let mut v = self.partitioner.read().unwrap().list();
        v.sort();
        v
    }

    /// Current membership epoch (bumps on every committed change).
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }

    /// (id, endpoint) for each member — what a direct-to-shard client needs to connect to each shard
    /// itself. Endpoint is the shard's base URL (empty for in-process clients).
    pub fn members_with_endpoints(&self) -> Vec<(NodeId, String)> {
        let mut v: Vec<(NodeId, String)> =
            self.clients.read().unwrap().iter().map(|(id, c)| (id.clone(), c.endpoint().to_string())).collect();
        v.sort();
        v
    }

    pub fn add_node(&self, id: &str, client: Arc<dyn ShardClient>) {
        self.clients.write().unwrap().insert(id.to_string(), client);
    }

    fn replication(&self) -> usize {
        self.replication.load(Ordering::Relaxed)
    }

    /// Number of shards in the ring (for metrics/introspection).
    pub fn shard_count(&self) -> usize {
        self.partitioner.read().unwrap().list().len()
    }

    /// Effective replication factor (clamped to shard count).
    pub fn replication_factor(&self) -> usize {
        self.replication()
    }

    /// Whether a rebalance/migration overlay is currently active.
    pub fn is_migrating(&self) -> bool {
        self.overlay.lock().unwrap().is_some()
    }

    /// Owners of a key — consults the migration overlay (old vs new per bucket) when one is active.
    fn owners(&self, key: &str) -> Vec<NodeId> {
        {
            let ov = self.overlay.lock().unwrap();
            if let Some(o) = ov.as_ref() {
                let b = bucket_of(key);
                return if o.migrated.contains(&b) {
                    o.new_p.owners_of_bucket(b, o.r_new)
                } else {
                    o.old_p.owners_of_bucket(b, o.r_old)
                };
            }
        }
        self.partitioner.read().unwrap().replica_set(key, self.replication())
    }

    fn is_down(&self, node: &str) -> bool {
        self.down.read().unwrap().contains(node)
    }

    fn client_of(&self, node: &str) -> Option<Arc<dyn ShardClient>> {
        self.clients.read().unwrap().get(node).cloned()
    }

    fn live_owner(&self, key: &str) -> Option<NodeId> {
        self.owners(key).into_iter().find(|n| !self.is_down(n))
    }

    /// Routing for a single key: its owners and which one would currently serve it.
    pub fn route(&self, key: &str) -> RouteInfo {
        RouteInfo { owners: self.owners(key), served_by: self.live_owner(key), key_value: key.to_string() }
    }

    /// Acquire/renew a TTL lease at `key`, routed to its **primary** owner only. Single-owner on
    /// purpose: the election must have ONE authoritative arbiter so two coordinators can't both win
    /// on different replicas (no split-brain). If that owner is down, returns `None` — no leader is
    /// elected (a safe pause) rather than risking a second leader elsewhere.
    pub async fn lease_acquire(&self, key: &str, holder: &str, ttl_ms: u64) -> Option<crate::lease::LeaseOutcome> {
        let owner = self.owners(key).into_iter().next()?;
        if self.is_down(&owner) {
            return None;
        }
        let c = self.client_of(&owner)?;
        c.lease_acquire(key, holder, ttl_ms).await.ok()
    }

    /// Try to become (or stay) the coordinator leader as `holder`. Convenience over [`lease_acquire`]
    /// at the well-known leader key.
    pub async fn try_acquire_leadership(&self, holder: &str, ttl_ms: u64) -> Option<crate::lease::LeaseOutcome> {
        self.lease_acquire(crate::lease::LEADER_KEY, holder, ttl_ms).await
    }

    // --- generic data plane (object + set), routed + replicated + migration-gated ---

    /// Read an object from a live owner of `key`, gated against an in-flight migration of its
    /// bucket and failing over to the next replica.
    /// Iterate keys in `coll` across ALL shards. Each live shard returns its keys strictly greater
    /// than `after` (ascending); results are merged + de-duplicated (a key lives on R replicas) into
    /// one ascending page of at most `limit`. Returns `(page, next_cursor)` where `next_cursor` is
    /// the last key when a full page came back (pass it as the next `after`), else `None` (drained).
    pub async fn scan_range(
        &self,
        coll: &str,
        after: Option<&str>,
        prefix: Option<&str>,
        end: Option<&str>,
        limit: usize,
    ) -> (Vec<String>, Option<String>) {
        let clients: Vec<Arc<dyn ShardClient>> = self.clients.read().unwrap().values().cloned().collect();
        let mut merged = std::collections::BTreeSet::new();
        for c in clients {
            if self.is_down(c.id()) {
                continue;
            }
            if let Ok(keys) = c.scan_range(coll, after, prefix, end, limit).await {
                merged.extend(keys);
            }
        }
        let page: Vec<String> = merged.into_iter().take(limit).collect();
        let cursor = if page.len() >= limit { page.last().cloned() } else { None };
        (page, cursor)
    }

    /// Cursor-only iteration — `scan_range` with no prefix/end bound.
    pub async fn scan_keys(&self, coll: &str, after: Option<&str>, limit: usize) -> (Vec<String>, Option<String>) {
        self.scan_range(coll, after, None, None, limit).await
    }

    /// A read partition: a shard, its endpoint, and the buckets it is the PRIMARY owner of. The
    /// partitions returned by [`partition_plan`](Self::partition_plan) tile every bucket exactly once,
    /// so a parallel reader (one task per partition) covers the whole keyspace with no overlap and
    /// full data locality — each task scans only its own shard, no coordinator/merge in the path.
    // (struct lives here; see partition_plan below.)

    /// Build the partition plan: assign every bucket to its primary owner and group by shard.
    pub fn partition_plan(&self) -> Vec<Partition> {
        let (nbuckets, mut by_node) = {
            let p = self.partitioner.read().unwrap();
            let n = p.bucket_count();
            let mut by: HashMap<NodeId, Vec<usize>> = HashMap::new();
            for b in 0..n {
                if let Some(primary) = p.owners_of_bucket(b, 1).into_iter().next() {
                    by.entry(primary).or_default().push(b);
                }
            }
            (n, by)
        };
        let _ = nbuckets;
        let endpoints: HashMap<NodeId, String> = self.members_with_endpoints().into_iter().collect();
        let mut plan: Vec<Partition> = by_node
            .drain()
            .map(|(id, buckets)| Partition { endpoint: endpoints.get(&id).cloned().unwrap_or_default(), id, buckets })
            .collect();
        plan.sort_by(|a, b| a.id.cmp(&b.id));
        plan
    }

    /// Batch get: group the requested keys by their owning shard and issue ONE request per shard
    /// (instead of one per key), then reassemble into input order. Missing/failed keys come back as
    /// `None`. The round-trip-amortization win for clients reading many keys (e.g. a compute partition).
    pub async fn mget(&self, coll: &str, keys: &[String]) -> Vec<Option<Value>> {
        let mut by_node: HashMap<NodeId, Vec<usize>> = HashMap::new();
        for (i, k) in keys.iter().enumerate() {
            if let Some(owner) = self.owners(k).into_iter().find(|n| !self.is_down(n)) {
                by_node.entry(owner).or_default().push(i);
            }
        }
        let mut out = vec![None; keys.len()];
        for (node, idxs) in by_node {
            let Some(c) = self.client_of(&node) else { continue };
            let sub: Vec<String> = idxs.iter().map(|&i| keys[i].clone()).collect();
            if let Ok(vals) = c.mget(coll, &sub).await {
                for (j, &i) in idxs.iter().enumerate() {
                    out[i] = vals.get(j).cloned().flatten();
                }
            }
        }
        out
    }

    pub async fn get(&self, coll: &str, key: &str) -> Option<Value> {
        let _gate = self.gates.enter(bucket_of(key)).await;
        for node in self.owners(key) {
            if self.is_down(&node) {
                continue;
            }
            if let Some(c) = self.client_of(&node) {
                if let Ok(o) = c.object(coll, key).await {
                    return o;
                }
            }
        }
        None
    }

    /// Apply a single-shard transaction. All keys it touches must share a routing token (a
    /// `{hash-tag}`), so they co-locate on one shard; otherwise it's rejected with `Err` — this is
    /// single-shard ACID, not distributed 2PC. The transaction is applied to every live replica of
    /// that shard (replication); the result is the primary's commit decision.
    pub async fn apply_txn(&self, txn: &crate::txn::Txn) -> Result<bool, String> {
        use crate::txn::route_token;
        let keys = txn.keys();
        let Some(&first) = keys.first() else {
            return Ok(true); // empty txn trivially commits
        };
        let token = route_token(first);
        if keys.iter().any(|k| route_token(k) != token) {
            return Err("transaction keys span multiple shards — give them a shared {hash-tag} to co-locate".into());
        }
        let _gate = self.gates.enter(bucket_of(first)).await;
        let mut result: Option<bool> = None;
        for node in self.owners(first) {
            if self.is_down(&node) {
                continue;
            }
            if let Some(c) = self.client_of(&node) {
                if let Ok(committed) = c.apply_txn(txn).await {
                    result.get_or_insert(committed); // the primary (first live owner) decides
                }
            }
        }
        result.ok_or_else(|| "no live owner for the transaction's shard".to_string())
    }

    /// Apply a write to every live owner of `key`, gated against an in-flight migration of its
    /// bucket (waits for cutover, then lands on the new owner).
    async fn broadcast<'a, F, Fut>(&'a self, key: &'a str, op: F)
    where
        F: Fn(Arc<dyn ShardClient>) -> Fut,
        Fut: std::future::Future<Output = ()> + 'a,
    {
        let _gate = self.gates.enter(bucket_of(key)).await;
        for node in self.owners(key) {
            if self.is_down(&node) {
                continue;
            }
            if let Some(c) = self.client_of(&node) {
                op(c).await;
            }
        }
    }

    pub async fn put(&self, coll: &str, key: &str, obj: Value) {
        self.broadcast(key, |c| {
            let (coll, k, obj) = (coll.to_string(), key.to_string(), obj.clone());
            async move {
                let _ = c.write_object(&coll, &k, obj).await;
            }
        })
        .await;
    }

    /// Like [`put`], but the value expires `ttl_ms` from now on each replica (0 = no expiry).
    pub async fn put_ttl(&self, coll: &str, key: &str, obj: Value, ttl_ms: u64) {
        self.broadcast(key, |c| {
            let (coll, k, obj) = (coll.to_string(), key.to_string(), obj.clone());
            async move {
                let _ = c.write_object_ttl(&coll, &k, obj, ttl_ms).await;
            }
        })
        .await;
    }

    /// Atomic compare-and-set: runs on the key's **primary** owner (one authoritative arbiter — a
    /// broadcast of independent RMWs would diverge), then replicates the new value to the other
    /// owners on success. Returns whether it set.
    pub async fn cas(&self, coll: &str, key: &str, expect: Option<Value>, new: Value) -> bool {
        let _gate = self.gates.enter(bucket_of(key)).await;
        let live: Vec<NodeId> = self.owners(key).into_iter().filter(|n| !self.is_down(n)).collect();
        let Some((primary, replicas)) = live.split_first() else {
            return false;
        };
        let Some(pc) = self.client_of(primary) else {
            return false;
        };
        let set = pc.cas(coll, key, expect, new.clone()).await.unwrap_or(false);
        if set {
            for r in replicas {
                if let Some(c) = self.client_of(r) {
                    let _ = c.write_object(coll, key, new.clone()).await;
                }
            }
        }
        set
    }

    /// Atomic integer add, primary-authoritative then replicated (see [`cas`]). `None` if the value
    /// isn't an integer or no owner is reachable.
    pub async fn incr(&self, coll: &str, key: &str, delta: i64) -> Option<i64> {
        let _gate = self.gates.enter(bucket_of(key)).await;
        let live: Vec<NodeId> = self.owners(key).into_iter().filter(|n| !self.is_down(n)).collect();
        let (primary, replicas) = live.split_first()?;
        let pc = self.client_of(primary)?;
        let new = pc.incr(coll, key, delta).await.ok().flatten()?;
        for r in replicas {
            if let Some(c) = self.client_of(r) {
                let _ = c.write_object(coll, key, Value::from(new)).await;
            }
        }
        Some(new)
    }

    pub async fn delete(&self, coll: &str, key: &str) {
        self.broadcast(key, |c| {
            let (coll, k) = (coll.to_string(), key.to_string());
            async move {
                let _ = c.delete_object(&coll, &k).await;
            }
        })
        .await;
    }

    pub async fn set_add(&self, coll: &str, key: &str, member: &str) {
        self.broadcast(key, |c| {
            let (coll, k, m) = (coll.to_string(), key.to_string(), member.to_string());
            async move {
                let _ = c.set_add(&coll, &k, &m).await;
            }
        })
        .await;
    }

    pub async fn set_remove(&self, coll: &str, key: &str, member: &str) {
        self.broadcast(key, |c| {
            let (coll, k, m) = (coll.to_string(), key.to_string(), member.to_string());
            async move {
                let _ = c.set_remove(&coll, &k, &m).await;
            }
        })
        .await;
    }

    /// Bulk-load pre-built objects into `coll`, routing each key to its replica set (one import per
    /// owner). The fast path for a full (re)build.
    pub async fn bulk_load(&self, coll: &str, entries: Vec<(String, Value)>) {
        let r = self.replication();
        let mut per: HashMap<NodeId, Vec<(String, Value)>> = HashMap::new();
        {
            let p = self.partitioner.read().unwrap();
            for (key, obj) in &entries {
                for node in p.replica_set(key, r) {
                    per.entry(node).or_default().push((key.clone(), obj.clone()));
                }
            }
        }
        let client_list: Vec<(NodeId, Arc<dyn ShardClient>)> =
            { self.clients.read().unwrap().iter().map(|(k, v)| (k.clone(), v.clone())).collect() };
        for (id, client) in client_list {
            if let Some(es) = per.remove(&id) {
                let _ = client.import_entries(coll, es).await;
            }
        }
    }

    // --- control plane: membership + rebalancing ---

    pub async fn rebalance(&self, new_ids: Vec<NodeId>) -> MigrateSummary {
        let old_ids = self.member_ids();
        self.run_migration(old_ids, new_ids, HashSet::new(), None).await
    }

    pub async fn drain(&self, node: &str) -> MigrateSummary {
        let new_ids: Vec<NodeId> = self.member_ids().into_iter().filter(|n| n != node).collect();
        self.rebalance(new_ids).await
    }

    pub async fn apply_membership(&self, target: HashMap<NodeId, Arc<dyn ShardClient>>) -> MigrateSummary {
        {
            let mut cl = self.clients.write().unwrap();
            for (id, client) in &target {
                cl.entry(id.clone()).or_insert_with(|| client.clone());
            }
        }
        let target_ids: Vec<NodeId> = target.keys().cloned().collect();
        self.rebalance(target_ids).await
    }

    pub async fn resume_migration(&self) -> Option<MigrateSummary> {
        let rec = self.journal.load()?;
        self.replication_target.store(rec.replication.max(1), Ordering::Relaxed);
        let done: HashSet<usize> = rec.done.iter().copied().collect();
        Some(self.run_migration(rec.old_nodes.clone(), rec.new_nodes.clone(), done, None).await)
    }

    pub async fn rebalance_partial(&self, new_ids: Vec<NodeId>, max_chunks: usize) -> MigrateSummary {
        let old_ids = self.member_ids();
        self.run_migration(old_ids, new_ids, HashSet::new(), Some(max_chunks)).await
    }

    async fn run_migration(&self, old: Vec<NodeId>, new: Vec<NodeId>, mut done: HashSet<usize>, stop_after: Option<usize>) -> MigrateSummary {
        let _mlock = self.migrating.lock().await; // one migration at a time; does NOT block reads
        let buckets_total = self.partitioner.read().unwrap().bucket_count();
        let same = old.len() == new.len() && old.iter().all(|n| new.contains(n));
        if same {
            self.journal.clear();
            self.commit_membership(&new);
            return MigrateSummary { buckets: buckets_total, moved_buckets: 0, copied: 0, dropped: 0 };
        }

        let rt = self.replication_target.load(Ordering::Relaxed);
        let plan = plan_reshard(&old, &new, rt, &self.topology.read().unwrap());
        {
            let mut ov = self.overlay.lock().unwrap();
            *ov = Some(Overlay {
                old_p: self.make_partitioner(old.clone()),
                r_old: clamp(rt, old.len()),
                new_p: self.make_partitioner(new.clone()),
                r_new: clamp(rt, new.len()),
                migrated: done.iter().copied().collect(),
            });
        }

        let mut actions: BTreeMap<usize, (Vec<(NodeId, NodeId)>, Vec<NodeId>)> = BTreeMap::new();
        for c in &plan.copies {
            if !done.contains(&c.bucket) {
                actions.entry(c.bucket).or_default().0.push((c.from.clone(), c.to.clone()));
            }
        }
        for d in &plan.drops {
            if !done.contains(&d.bucket) {
                actions.entry(d.bucket).or_default().1.push(d.node.clone());
            }
        }
        let bucket_list: Vec<usize> = actions.keys().copied().collect();
        self.save_record(&old, &new, &done);

        let mut copied = 0;
        let mut dropped = 0;
        for (i, chunk) in bucket_list.chunks(self.chunk).enumerate() {
            if let Some(max) = stop_after {
                if i >= max {
                    // simulate a crash: leave overlay + journal, do not commit
                    return MigrateSummary { buckets: buckets_total, moved_buckets: plan.moved_buckets, copied, dropped };
                }
            }
            self.gates.freeze(chunk);
            self.gates.drain(chunk).await;

            // Batch the chunk's moves: all buckets for a (from,to) pair travel in ONE export+
            // import per collection (the shard filters by the bucket set), and all of a node's
            // dropped buckets in one drop — turning thousands of per-bucket round-trips into a
            // handful per chunk.
            let mut pair_buckets: BTreeMap<(NodeId, NodeId), Vec<usize>> = BTreeMap::new();
            let mut node_drops: BTreeMap<NodeId, Vec<usize>> = BTreeMap::new();
            for &bk in chunk {
                let (copies, drops) = &actions[&bk];
                for (from, to) in copies {
                    pair_buckets.entry((from.clone(), to.clone())).or_default().push(bk);
                }
                for node in drops {
                    node_drops.entry(node.clone()).or_default().push(bk);
                }
            }

            for ((from, to), buckets) in &pair_buckets {
                let fc = self.client_of(from);
                let tc = self.client_of(to);
                if let (Some(fc), Some(tc)) = (fc, tc) {
                    for coll in &self.colls {
                        let entries = fc.export_entries(coll, buckets.clone()).await.unwrap_or_default();
                        copied += entries.len();
                        if !entries.is_empty() {
                            let _ = tc.import_entries(coll, entries).await;
                        }
                    }
                }
            }
            // cutover: these buckets now route to their new owners (overlay)
            {
                let mut ov = self.overlay.lock().unwrap();
                if let Some(o) = ov.as_mut() {
                    for &b in chunk {
                        o.migrated.insert(b);
                    }
                }
            }
            for (node, buckets) in &node_drops {
                if let Some(c) = self.client_of(node) {
                    for coll in &self.colls {
                        dropped += c.drop_buckets(coll, buckets.clone()).await.unwrap_or(0);
                    }
                }
            }
            self.gates.unfreeze(chunk); // waiters resume → route to the new owners
            for &bk in chunk {
                done.insert(bk);
            }
            self.save_record(&old, &new, &done);
        }

        self.journal.clear();
        self.commit_membership(&new); // partitioner := new
        *self.overlay.lock().unwrap() = None; // then drop the overlay (routing already == new)
        MigrateSummary { buckets: buckets_total, moved_buckets: plan.moved_buckets, copied, dropped }
    }

    fn save_record(&self, old: &[NodeId], new: &[NodeId], done: &HashSet<usize>) {
        self.journal.save(&MigrationRecord {
            old_nodes: old.to_vec(),
            new_nodes: new.to_vec(),
            replication: self.replication_target.load(Ordering::Relaxed),
            done: done.iter().copied().collect(),
        });
    }

    fn commit_membership(&self, new: &[NodeId]) {
        self.epoch.fetch_add(1, Ordering::Relaxed);
        self.partitioner.write().unwrap().set_nodes(new.to_vec());
        self.replication
            .store(clamp(self.replication_target.load(Ordering::Relaxed), new.len()), Ordering::Relaxed);
        let remove: Vec<NodeId> = { self.clients.read().unwrap().keys().filter(|k| !new.contains(k)).cloned().collect() };
        {
            let mut cl = self.clients.write().unwrap();
            let mut d = self.down.write().unwrap();
            for id in &remove {
                cl.remove(id);
                d.remove(id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::InProcessShardClient;
    use crate::net::shared;
    use crate::shard::Shard;
    use crate::store::MemoryStore;
    use serde_json::json;

    fn racks_map() -> HashMap<NodeId, String> {
        [("n0", "a"), ("n1", "a"), ("n2", "b"), ("n3", "b")]
            .iter()
            .map(|(n, r)| (n.to_string(), r.to_string()))
            .collect()
    }

    fn four_node_clients() -> HashMap<NodeId, Arc<dyn ShardClient>> {
        let mut clients: HashMap<NodeId, Arc<dyn ShardClient>> = HashMap::new();
        for id in ["n0", "n1", "n2", "n3"] {
            let sh = shared(Shard::new(id, MemoryStore::new()));
            clients.insert(id.to_string(), Arc::new(InProcessShardClient::new(id, sh)));
        }
        clients
    }

    #[tokio::test]
    async fn rack_aware_routing_spreads_replicas_and_serves() {
        let topo = racks_map();
        let cluster = Cluster::new(four_node_clients(), 2, vec!["kv".into()]).with_topology(topo.clone());
        for key in ["alpha", "beta", "gamma", "delta", "k-7", "zzz"] {
            let owners = cluster.route(key).owners;
            assert_eq!(owners.len(), 2, "two replicas for {key}");
            let racks: HashSet<&str> = owners.iter().map(|n| topo[n].as_str()).collect();
            assert_eq!(racks.len(), 2, "replicas of {key} span distinct racks: {owners:?}");
        }
        // a real round-trip flows through the rack-aware owners
        cluster.put("kv", "alpha", json!("A")).await;
        assert_eq!(cluster.get("kv", "alpha").await, Some(json!("A")));
    }

    #[tokio::test]
    async fn no_topology_keeps_plain_hrw() {
        let cluster = Cluster::new(four_node_clients(), 2, vec!["kv".into()]);
        let p = Partitioner::new(vec!["n0".into(), "n1".into(), "n2".into(), "n3".into()]);
        for key in ["alpha", "beta", "gamma", "k-9"] {
            assert_eq!(cluster.route(key).owners, p.replica_set(key, 2), "plain HRW unchanged for {key}");
        }
        // introspection accessors backing the coordinator /metrics gauges
        assert_eq!(cluster.shard_count(), 4);
        assert_eq!(cluster.replication_factor(), 2);
        assert!(!cluster.is_migrating(), "no migration at rest");
    }

    #[tokio::test]
    async fn cas_and_incr_through_the_cluster() {
        let cluster = Cluster::new(four_node_clients(), 2, vec!["kv".into()]);
        // CAS: set-if-absent, then match/mismatch
        assert!(cluster.cas("kv", "lock", None, json!("held")).await, "acquire when absent");
        assert!(!cluster.cas("kv", "lock", None, json!("other")).await, "fails when present");
        assert!(cluster.cas("kv", "lock", Some(json!("held")), json!("held2")).await, "swaps on match");
        assert_eq!(cluster.get("kv", "lock").await, Some(json!("held2")));
        // value replicated to BOTH owners (read each replica directly)
        for owner in cluster.route("lock").owners {
            let c = cluster.clients.read().unwrap().get(&owner).unwrap().clone();
            assert_eq!(c.object("kv", "lock").await.unwrap(), Some(json!("held2")), "replica {owner} consistent");
        }
        // INCR
        assert_eq!(cluster.incr("kv", "n", 5).await, Some(5));
        assert_eq!(cluster.incr("kv", "n", 7).await, Some(12));
        assert_eq!(cluster.get("kv", "n").await, Some(json!(12)));
    }

    #[tokio::test]
    async fn put_ttl_expires_through_the_cluster() {
        let cluster = Cluster::new(four_node_clients(), 2, vec!["kv".into()]);
        cluster.put_ttl("kv", "ephemeral", json!("x"), 1).await; // ~1ms ttl
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        assert_eq!(cluster.get("kv", "ephemeral").await, None, "key expired via TTL");
        cluster.put_ttl("kv", "lasting", json!("y"), 100_000).await;
        assert_eq!(cluster.get("kv", "lasting").await, Some(json!("y")), "long TTL still present");
    }
}
