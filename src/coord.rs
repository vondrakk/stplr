// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Coordinator server — the cluster front door over HTTP. Holds an `Arc<Cluster>` and routes each
//! client object/set op to the right shard(s) via the partitioner + replication + failover. The
//! client API shape matches a shard's (`/object`, `/write`, `/setAdd`, …), so a client points at a
//! coordinator or a single shard transparently. This is what makes a multi-shard cluster runnable:
//! N shard nodes + a coordinator that routes.
//!
//! First increment: routing + replication over HTTP. Direct-to-shard writes and a binary
//! coordinator↔shard hop (to remove the coordinator from the data path's hot loop) come later.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::cluster::Cluster;

type Co = Arc<Cluster>;

#[derive(Deserialize)]
struct RebalanceBody {
    /// The target membership (node ids) to rebalance the ring to.
    nodes: Vec<String>,
}
#[derive(Deserialize)]
struct DrainBody {
    node: String,
}

/// Live leadership state for this coordinator, maintained by the election loop and read by handlers.
pub struct Leadership {
    holder: String,
    is_leader: AtomicBool,
    token: AtomicU64,
    current_leader: Mutex<String>,
}

impl Leadership {
    fn new(holder: &str) -> Self {
        Leadership {
            holder: holder.to_string(),
            is_leader: AtomicBool::new(false),
            token: AtomicU64::new(0),
            current_leader: Mutex::new(String::new()),
        }
    }
    /// True iff THIS coordinator currently holds the lease (is the singleton driver).
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Relaxed)
    }
    /// The current leadership term (fencing token); 0 until first known.
    pub fn token(&self) -> u64 {
        self.token.load(Ordering::Relaxed)
    }
    /// The id of whoever currently holds the lease (may be another coordinator), "" if unknown.
    pub fn current_leader(&self) -> String {
        self.current_leader.lock().unwrap().clone()
    }
    /// This coordinator's own id.
    pub fn holder(&self) -> &str {
        &self.holder
    }
}

/// Spawn the election loop: `holder` acquires/renews the leader lease every ~ttl/3. On a grant it is
/// the leader; on denial it records the incumbent and steps down; if the lease owner is unreachable
/// it steps down (no leader is safer than a second one). Returns the shared, readable state.
pub fn spawn_elector(cluster: Co, holder: String, ttl_ms: u64) -> Arc<Leadership> {
    let state = Arc::new(Leadership::new(&holder));
    let st = state.clone();
    let renew = (ttl_ms / 3).max(50);
    tokio::spawn(async move {
        loop {
            match cluster.try_acquire_leadership(&holder, ttl_ms).await {
                Some(o) => {
                    st.is_leader.store(o.granted, Ordering::Relaxed);
                    st.token.store(o.lease.token, Ordering::Relaxed);
                    *st.current_leader.lock().unwrap() = o.lease.holder;
                }
                None => {
                    // lease owner unreachable — cannot assert leadership; step down.
                    st.is_leader.store(false, Ordering::Relaxed);
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(renew)).await;
        }
    });
    state
}

#[derive(Deserialize)]
struct ObjQ {
    coll: String,
    key: String,
}
#[derive(Deserialize)]
struct ScanQ {
    coll: String,
    #[serde(default)]
    after: Option<String>,
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    end: Option<String>,
    #[serde(default = "default_scan_limit")]
    limit: usize,
}
fn default_scan_limit() -> usize {
    1000
}
#[derive(Deserialize)]
struct MgetBody {
    coll: String,
    keys: Vec<String>,
}
#[derive(Deserialize)]
struct WriteBody {
    coll: String,
    key: String,
    obj: Value,
    #[serde(default, rename = "ttlMs")]
    ttl_ms: Option<u64>,
}
#[derive(Deserialize)]
struct SetBody {
    coll: String,
    key: String,
    member: String,
}
#[derive(Deserialize)]
struct DeleteBody {
    coll: String,
    key: String,
}
#[derive(Deserialize)]
struct KeyQ {
    key: String,
}
#[derive(Deserialize)]
struct CasBody {
    coll: String,
    key: String,
    #[serde(default)]
    expect: Option<Value>,
    new: Value,
}
#[derive(Deserialize)]
struct IncrBody {
    coll: String,
    key: String,
    delta: i64,
}

pub fn app(cluster: Co) -> Router {
    Router::new()
        .route("/health", get(|| async { Json(json!({"ok": true})) }))
        .route("/metrics", get(metrics))
        .route("/object", get(object))
        .route("/mget", post(mget))
        .route("/scan", get(scan))
        .route("/write", post(write))
        .route("/cas", post(cas))
        .route("/incr", post(incr))
        .route("/setAdd", post(set_add))
        .route("/setRemove", post(set_remove))
        .route("/deleteObject", post(delete_object))
        .route("/route", get(route_info))
        .route("/members", get(members))
        .route("/partitions", get(partitions))
        .with_state(cluster)
}

/// Coordinator metrics: process op counters (shared with shards) + cluster gauges.
async fn metrics(State(c): State<Co>) -> String {
    let mut out = crate::metrics::render();
    out.push_str(&crate::metrics::render_cluster_gauges(
        c.shard_count(),
        c.replication_factor(),
        c.is_migrating(),
    ));
    out
}

async fn object(State(c): State<Co>, Query(q): Query<ObjQ>) -> Json<Value> {
    crate::metrics::inc(&crate::metrics::GET);
    Json(json!({ "object": c.get(&q.coll, &q.key).await }))
}
/// Cluster-wide batch get: one value (or null) per key, in order — keys routed to their shards and
/// batched one request per shard.
async fn mget(State(c): State<Co>, Json(b): Json<MgetBody>) -> Json<Value> {
    Json(json!({ "values": c.mget(&b.coll, &b.keys).await }))
}
/// Cluster-wide paginated key iteration: fan out to all shards, merge into one ascending page +
/// a `cursor` to pass back as `?after=` for the next page (null when the collection is drained).
async fn scan(State(c): State<Co>, Query(q): Query<ScanQ>) -> Json<Value> {
    let (keys, cursor) =
        c.scan_range(&q.coll, q.after.as_deref(), q.prefix.as_deref(), q.end.as_deref(), q.limit.min(10_000)).await;
    Json(json!({ "keys": keys, "cursor": cursor }))
}
async fn write(State(c): State<Co>, Json(b): Json<WriteBody>) -> Json<Value> {
    crate::metrics::inc(&crate::metrics::SET);
    match b.ttl_ms {
        Some(ttl) if ttl > 0 => c.put_ttl(&b.coll, &b.key, b.obj, ttl).await,
        _ => c.put(&b.coll, &b.key, b.obj).await,
    }
    Json(json!({ "ok": true }))
}
async fn cas(State(c): State<Co>, Json(b): Json<CasBody>) -> Json<Value> {
    crate::metrics::inc(&crate::metrics::SET);
    Json(json!({ "set": c.cas(&b.coll, &b.key, b.expect, b.new).await }))
}
async fn incr(State(c): State<Co>, Json(b): Json<IncrBody>) -> Json<Value> {
    crate::metrics::inc(&crate::metrics::SET);
    Json(json!({ "value": c.incr(&b.coll, &b.key, b.delta).await }))
}
async fn set_add(State(c): State<Co>, Json(b): Json<SetBody>) -> Json<Value> {
    crate::metrics::inc(&crate::metrics::SADD);
    c.set_add(&b.coll, &b.key, &b.member).await;
    Json(json!({ "ok": true }))
}
async fn set_remove(State(c): State<Co>, Json(b): Json<SetBody>) -> Json<Value> {
    crate::metrics::inc(&crate::metrics::SREM);
    c.set_remove(&b.coll, &b.key, &b.member).await;
    Json(json!({ "ok": true }))
}
async fn delete_object(State(c): State<Co>, Json(b): Json<DeleteBody>) -> Json<Value> {
    crate::metrics::inc(&crate::metrics::DEL);
    c.delete(&b.coll, &b.key).await;
    Json(json!({ "ok": true }))
}
async fn route_info(State(c): State<Co>, Query(q): Query<KeyQ>) -> Json<Value> {
    let r = c.route(&q.key);
    Json(json!({ "owners": r.owners, "servedBy": r.served_by }))
}
async fn members(State(c): State<Co>) -> Json<Value> {
    let members: Vec<Value> =
        c.members_with_endpoints().into_iter().map(|(id, ep)| json!({ "id": id, "endpoint": ep })).collect();
    Json(json!({ "epoch": c.epoch(), "members": members }))
}
/// The partition plan for parallel reads: each shard + the buckets it primary-owns. A client reads
/// each partition directly from its shard's `/scanBuckets`, covering the keyspace once, in parallel.
async fn partitions(State(c): State<Co>) -> Json<Value> {
    Json(json!({ "epoch": c.epoch(), "partitions": c.partition_plan() }))
}

/// Router with a `/leader` status endpoint and leader-gated `/admin/*` driver ops (added on top of
/// [`app`]). The driver operations (rebalance, drain) are the singleton work leader election exists
/// to serialize: a non-leader replica rejects them with `409` + the current leader's id (the
/// standard leader-redirect), so you can safely fire a control-plane op at ANY coordinator.
pub fn app_with_leadership(cluster: Co, leadership: Arc<Leadership>) -> Router {
    let l_status = leadership.clone();
    let (cl_reb, ld_reb) = (cluster.clone(), leadership.clone());
    let (cl_drn, ld_drn) = (cluster.clone(), leadership.clone());
    app(cluster)
        .route(
            "/leader",
            get(move || {
                let l = l_status.clone();
                async move {
                    Json(json!({
                        "self": l.holder(),
                        "leader": l.current_leader(),
                        "isLeader": l.is_leader(),
                        "token": l.token(),
                    }))
                }
            }),
        )
        .route(
            "/admin/rebalance",
            post(move |Json(b): Json<RebalanceBody>| {
                let (cl, ld) = (cl_reb.clone(), ld_reb.clone());
                async move {
                    if let Some(redirect) = not_leader(&ld) {
                        return redirect;
                    }
                    let s = cl.rebalance(b.nodes).await;
                    (
                        StatusCode::OK,
                        Json(json!({
                            "ok": true, "leader": ld.holder(),
                            "movedBuckets": s.moved_buckets, "copied": s.copied, "dropped": s.dropped,
                        })),
                    )
                }
            }),
        )
        .route(
            "/admin/drain",
            post(move |Json(b): Json<DrainBody>| {
                let (cl, ld) = (cl_drn.clone(), ld_drn.clone());
                async move {
                    if let Some(redirect) = not_leader(&ld) {
                        return redirect;
                    }
                    let s = cl.drain(&b.node).await;
                    (
                        StatusCode::OK,
                        Json(json!({
                            "ok": true, "leader": ld.holder(),
                            "movedBuckets": s.moved_buckets, "copied": s.copied, "dropped": s.dropped,
                        })),
                    )
                }
            }),
        )
}

/// Add the durable-ingest routes: `POST /enqueue` (durably accept a write, ack with its seq, apply
/// asynchronously) and `GET /ingest/status` (accepted / committed / pending counts).
pub fn add_ingest_routes(router: Router, queue: Arc<crate::ingest::IngestQueue>) -> Router {
    let q_put = queue.clone();
    let q_stat = queue;
    router
        .route(
            "/enqueue",
            post(move |Json(b): Json<WriteBody>| {
                let q = q_put.clone();
                async move {
                    let seq = q.enqueue(&crate::ingest::IngestOp::Put { coll: b.coll, key: b.key, value: b.obj });
                    Json(json!({ "ok": true, "durable": true, "seq": seq }))
                }
            }),
        )
        .route(
            "/ingest/status",
            get(move || {
                let q = q_stat.clone();
                async move {
                    Json(json!({ "accepted": q.latest_seq(), "committed": q.committed(), "pending": q.pending() }))
                }
            }),
        )
}

/// Leader-redirect guard: `Some(409 + current leader)` if this replica is not the leader, else
/// `None` (proceed). Driver ops must only run on the one leader.
fn not_leader(l: &Leadership) -> Option<(StatusCode, Json<Value>)> {
    if l.is_leader() {
        None
    } else {
        Some((
            StatusCode::CONFLICT,
            Json(json!({ "ok": false, "error": "not leader", "leader": l.current_leader() })),
        ))
    }
}

pub async fn serve_addr(addr: std::net::SocketAddr, cluster: Co) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app(cluster)).await
}

/// Serve the coordinator with optional leader election and/or a durable ingest queue. `elect` =
/// `(holder, ttl_ms)` spawns the elector + `/leader` + gated `/admin/*`; `queue` spawns the drain
/// loop (applying queued writes to the cluster) + `/enqueue` + `/ingest/status`.
pub async fn serve_addr_full(
    addr: std::net::SocketAddr,
    cluster: Co,
    elect: Option<(String, u64)>,
    queue: Option<Arc<crate::ingest::IngestQueue>>,
    auth_token: Option<String>,
    tls: Option<crate::tls::ServerTls>,
) -> std::io::Result<()> {
    let mut router = match elect {
        Some((holder, ttl)) => app_with_leadership(cluster.clone(), spawn_elector(cluster.clone(), holder, ttl)),
        None => app(cluster.clone()),
    };
    if let Some(q) = queue {
        crate::ingest::spawn_drainer(q.clone(), cluster.clone() as Arc<dyn crate::ingest::WriteSink>, 50);
        router = add_ingest_routes(router, q);
    }
    let router = crate::net::require_bearer(router, auth_token);
    crate::tls::serve_router(addr, router, tls).await
}

/// Serve with leader election only (no ingest queue, no auth).
pub async fn serve_addr_elected(
    addr: std::net::SocketAddr,
    cluster: Co,
    holder: String,
    ttl_ms: u64,
) -> std::io::Result<()> {
    serve_addr_full(addr, cluster, Some((holder, ttl_ms)), None, None, None).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ShardClient;
    use crate::net::{serve_ephemeral, shared, HttpShardClient};
    use crate::partitioner::NodeId;
    use crate::shard::Shard;
    use crate::store::MemoryStore;
    use std::collections::HashMap;

    #[tokio::test]
    async fn coordinator_routes_across_two_shards() {
        // Two real in-process shard HTTP servers.
        let mut clients: HashMap<NodeId, Arc<dyn ShardClient>> = HashMap::new();
        for id in ["s0", "s1"] {
            let addr = serve_ephemeral(shared(Shard::new(id, MemoryStore::new()))).await;
            clients.insert(id.to_string(), Arc::new(HttpShardClient::new(id, &format!("http://{addr}"))));
        }
        let cluster = Arc::new(Cluster::new(clients, 1, vec!["kv".into()]));

        // Write + read back through the coordinator's Cluster routing.
        cluster.put("kv", "alpha", json!("A")).await;
        cluster.put("kv", "beta", json!("B")).await;
        assert_eq!(cluster.get("kv", "alpha").await, Some(json!("A")));
        assert_eq!(cluster.get("kv", "beta").await, Some(json!("B")));
        assert_eq!(cluster.get("kv", "missing").await, None);
        // Both shards are in the ring.
        let mut m = cluster.member_ids();
        m.sort();
        assert_eq!(m, vec!["s0".to_string(), "s1".to_string()]);
    }

    #[tokio::test]
    async fn scan_iterates_all_keys_across_shards_paginated() {
        let mut clients: HashMap<NodeId, Arc<dyn ShardClient>> = HashMap::new();
        for id in ["s0", "s1"] {
            let addr = serve_ephemeral(shared(Shard::new(id, MemoryStore::new()))).await;
            clients.insert(id.to_string(), Arc::new(HttpShardClient::new(id, &format!("http://{addr}"))));
        }
        let cluster = Arc::new(Cluster::new(clients, 1, vec!["kv".into()]));

        // 25 keys scatter across the two shards by rendezvous hashing.
        let mut want: Vec<String> = (0..25).map(|i| format!("k{i:02}")).collect();
        want.sort();
        for k in &want {
            cluster.put("kv", k, json!(k)).await;
        }

        // Page through with a small limit; the cursor walks the whole collection in ascending order.
        let mut got: Vec<String> = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let (page, cursor) = cluster.scan_keys("kv", after.as_deref(), 10).await;
            got.extend(page.clone());
            // each page is globally sorted and strictly after the previous cursor
            assert!(page.windows(2).all(|w| w[0] < w[1]), "page ascending");
            match cursor {
                Some(c) => after = Some(c),
                None => break,
            }
        }
        assert_eq!(got, want, "scan returns every key exactly once, in order");

        // Empty/unknown collection yields an empty drained page.
        let (page, cursor) = cluster.scan_keys("nope", None, 10).await;
        assert!(page.is_empty() && cursor.is_none());
    }

    #[tokio::test]
    async fn partition_plan_tiles_keyspace_and_reads_are_disjoint() {
        let mut clients: HashMap<NodeId, Arc<dyn ShardClient>> = HashMap::new();
        let mut by_id: HashMap<String, Arc<dyn ShardClient>> = HashMap::new();
        for id in ["s0", "s1", "s2"] {
            let addr = serve_ephemeral(shared(Shard::new(id, MemoryStore::new()))).await;
            let c: Arc<dyn ShardClient> = Arc::new(HttpShardClient::new(id, &format!("http://{addr}")));
            clients.insert(id.to_string(), c.clone());
            by_id.insert(id.to_string(), c);
        }
        let cluster = Arc::new(Cluster::new(clients, 1, vec!["kv".into()]));

        // the plan assigns every bucket exactly once
        let plan = cluster.partition_plan();
        let mut buckets: Vec<usize> = plan.iter().flat_map(|p| p.buckets.clone()).collect();
        let total = buckets.len();
        buckets.sort();
        buckets.dedup();
        assert_eq!(buckets.len(), total, "no bucket assigned to two shards");
        assert_eq!(buckets.len(), 4096, "every bucket assigned");

        // write keys, then read partition-by-partition straight from each shard
        let want: std::collections::HashSet<String> = (0..40).map(|i| format!("k{i}")).collect();
        for k in &want {
            cluster.put("kv", k, json!(k)).await;
        }
        let mut seen = std::collections::HashSet::new();
        for p in &plan {
            let c = by_id.get(&p.id).unwrap();
            for k in c.scan_buckets("kv", &p.buckets, None, 10_000).await.unwrap() {
                assert!(seen.insert(k), "a key was read on two partitions");
            }
        }
        assert_eq!(seen, want, "every key read exactly once across partitions");
    }

    #[tokio::test]
    async fn mget_batches_across_shards_in_order() {
        let mut clients: HashMap<NodeId, Arc<dyn ShardClient>> = HashMap::new();
        for id in ["s0", "s1", "s2"] {
            let addr = serve_ephemeral(shared(Shard::new(id, MemoryStore::new()))).await;
            clients.insert(id.to_string(), Arc::new(HttpShardClient::new(id, &format!("http://{addr}"))));
        }
        let cluster = Arc::new(Cluster::new(clients, 1, vec!["kv".into()]));
        for k in ["a", "b", "c", "d"] {
            cluster.put("kv", k, json!(format!("V-{k}"))).await;
        }
        // mixed present/absent, result aligned to input order
        let keys: Vec<String> = ["c", "missing", "a", "d", "b"].iter().map(|s| s.to_string()).collect();
        let got = cluster.mget("kv", &keys).await;
        assert_eq!(
            got,
            vec![Some(json!("V-c")), None, Some(json!("V-a")), Some(json!("V-d")), Some(json!("V-b"))]
        );
        // empty request -> empty result
        assert!(cluster.mget("kv", &[]).await.is_empty());
    }

    #[tokio::test]
    async fn scan_range_prefix_and_bounds() {
        let mut clients: HashMap<NodeId, Arc<dyn ShardClient>> = HashMap::new();
        for id in ["s0", "s1", "s2"] {
            let addr = serve_ephemeral(shared(Shard::new(id, MemoryStore::new()))).await;
            clients.insert(id.to_string(), Arc::new(HttpShardClient::new(id, &format!("http://{addr}"))));
        }
        let cluster = Arc::new(Cluster::new(clients, 1, vec!["kv".into()]));
        for k in ["user:01", "user:02", "user:03", "order:01", "order:02", "zzz"] {
            cluster.put("kv", k, json!(k)).await;
        }

        // prefix: only the user: keys, in order
        let (page, _) = cluster.scan_range("kv", None, Some("user:"), None, 100).await;
        assert_eq!(page, vec!["user:01", "user:02", "user:03"]);

        // half-open range [order:02, user:02): order:02, user:01
        let (page, _) = cluster.scan_range("kv", None, None, Some("user:02"), 100).await;
        assert!(page.contains(&"order:01".to_string()) && page.contains(&"user:01".to_string()));
        assert!(!page.contains(&"user:02".to_string()), "end bound is exclusive");
        assert!(!page.contains(&"zzz".to_string()), "beyond end excluded");

        // prefix + cursor pagination
        let (p1, cur) = cluster.scan_range("kv", None, Some("user:"), None, 2).await;
        assert_eq!(p1, vec!["user:01", "user:02"]);
        let (p2, _) = cluster.scan_range("kv", cur.as_deref(), Some("user:"), None, 2).await;
        assert_eq!(p2, vec!["user:03"], "cursor resumes within the prefix");
    }

    // Two coordinators over the SAME shards (same node ids → same lease arbiter shard). Exercises
    // the full wire: /leaseAcquire route + HttpShardClient::lease_acquire + single-owner routing.
    async fn two_coordinators() -> (Co, Co) {
        let mut a: HashMap<NodeId, Arc<dyn ShardClient>> = HashMap::new();
        let mut b: HashMap<NodeId, Arc<dyn ShardClient>> = HashMap::new();
        for id in ["s0", "s1"] {
            let addr = serve_ephemeral(shared(Shard::new(id, MemoryStore::new()))).await;
            a.insert(id.to_string(), Arc::new(HttpShardClient::new(id, &format!("http://{addr}"))));
            b.insert(id.to_string(), Arc::new(HttpShardClient::new(id, &format!("http://{addr}"))));
        }
        (Arc::new(Cluster::new(a, 1, vec!["kv".into()])), Arc::new(Cluster::new(b, 1, vec!["kv".into()])))
    }

    #[tokio::test]
    async fn leader_election_grants_one_and_denies_the_other() {
        let (ca, cb) = two_coordinators().await;
        let ra = ca.try_acquire_leadership("A", 10_000).await.unwrap();
        assert!(ra.granted, "A becomes leader");

        let rb = cb.try_acquire_leadership("B", 10_000).await.unwrap();
        assert!(!rb.granted, "B is denied while A holds the lease");
        assert_eq!(rb.lease.holder, "A", "B learns A is the leader");

        let ra2 = ca.try_acquire_leadership("A", 10_000).await.unwrap();
        assert!(ra2.granted, "A renews");
        assert_eq!(ra2.lease.token, ra.lease.token, "renew stays in the same term");
    }

    #[tokio::test]
    async fn elector_loop_makes_one_coordinator_leader() {
        let (ca, cb) = two_coordinators().await;
        let la = spawn_elector(ca, "A".into(), 10_000);
        // let A's elector acquire before B's starts
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        let lb = spawn_elector(cb, "B".into(), 10_000);
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;

        assert!(la.is_leader(), "A's elector holds the lease");
        assert!(!lb.is_leader(), "B's elector does not");
        assert_eq!(lb.current_leader(), "A", "B reports A as leader");
        assert!(la.token() >= 1);
    }

    async fn serve_elected(cluster: Co, leadership: Arc<Leadership>) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = app_with_leadership(cluster, leadership);
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    }

    #[tokio::test]
    async fn admin_ops_are_leader_gated() {
        let (ca, _cb) = two_coordinators().await;
        let ld = Arc::new(Leadership::new("self"));
        *ld.current_leader.lock().unwrap() = "other".to_string(); // someone else leads
        let addr = serve_elected(ca, ld.clone()).await;
        let http = reqwest::Client::new();
        let url = format!("http://{addr}/admin/rebalance");

        // not leader → 409 + who the leader is
        let r = http.post(&url).json(&json!({"nodes": ["s0", "s1"]})).send().await.unwrap();
        assert_eq!(r.status().as_u16(), 409, "non-leader rejects driver ops");
        let body: Value = r.json().await.unwrap();
        assert_eq!(body["leader"], "other", "redirects to the current leader");

        // become leader → the op runs (no-op rebalance to the same membership)
        ld.is_leader.store(true, Ordering::Relaxed);
        let r = http.post(&url).json(&json!({"nodes": ["s0", "s1"]})).send().await.unwrap();
        assert_eq!(r.status().as_u16(), 200, "leader executes driver ops");
        assert_eq!(r.json::<Value>().await.unwrap()["ok"], true);
    }
}
