// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Network transport for the shard role — Rust port of `cluster/shardServer.ts` +
//! `httpShardClient.ts`. A shard exposes its operations over JSON HTTP (axum); a coordinator
//! reaches a remote shard through `HttpShardClient` (reqwest). The object-safe `ShardOps` trait
//! lets the server hold any backing store (memory/LMDB) behind one `dyn` handle.

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use axum::extract::{Query, Request, State};
use axum::http::{header::AUTHORIZATION, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::client::{CResult, ShardClient};
use crate::shard::Shard;
use crate::store::IndexStore;

/// Wrap a router so every route except `/health` and `/metrics` requires `Authorization: Bearer
/// <token>`. `None` = no auth (dev / behind a trusted mesh). Used by both the shard and coordinator.
pub fn require_bearer(router: Router, token: Option<String>) -> Router {
    match token {
        Some(t) => router.layer(middleware::from_fn_with_state(Arc::new(t), bearer_layer)),
        None => router,
    }
}

async fn bearer_layer(State(token): State<Arc<String>>, req: Request, next: Next) -> Result<Response, StatusCode> {
    let path = req.uri().path();
    if path == "/health" || path == "/metrics" {
        return Ok(next.run(req).await); // probes + scrapers are unauthenticated
    }
    let presented = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    match presented {
        Some(v) if v == token.as_str() => Ok(next.run(req).await),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}

/// Object-safe view of a shard's operations, so the HTTP server is not generic over the store.
/// Generic store + set-membership + reshard only — no correlation types cross this boundary.
/// `Sync` so the server can hold it behind an `RwLock`: read ops (`object`/`export`/`caps`) take a
/// shared read lock and run concurrently across worker threads (LMDB reads are MVCC — they don't
/// block each other); only mutations take the exclusive write lock.
pub trait ShardOps: Send + Sync {
    fn object(&self, coll: &str, key: &str) -> Option<Value>;
    fn mget(&self, coll: &str, keys: &[String]) -> Vec<Option<Value>>;
    fn scan_range(&self, coll: &str, after: Option<&str>, prefix: Option<&str>, end: Option<&str>, limit: usize) -> Vec<String>;
    fn set_add(&mut self, coll: &str, key: &str, member: &str) -> bool;
    fn set_remove(&mut self, coll: &str, key: &str, member: &str) -> bool;
    fn write_object(&mut self, coll: &str, key: &str, obj: Value);
    fn write_object_ttl(&mut self, coll: &str, key: &str, obj: Value, ttl_ms: u64);
    fn sweep_expired(&mut self) -> usize;
    fn cas(&mut self, coll: &str, key: &str, expect: Option<Value>, new: Value) -> bool;
    fn incr(&mut self, coll: &str, key: &str, delta: i64) -> Option<i64>;
    fn write_batch(&mut self, items: Vec<(String, String, Value)>);
    fn delete_object(&mut self, coll: &str, key: &str);
    fn export_entries(&self, coll: &str, buckets: &[usize]) -> Vec<(String, Value)>;
    fn import_entries(&mut self, coll: &str, entries: Vec<(String, Value)>);
    fn drop_buckets(&mut self, coll: &str, buckets: &[usize]) -> usize;
    fn native_set_logic(&self) -> bool;
    /// Hot-copy the durable store to `dest` (Err if the backend isn't durable). A read op.
    fn snapshot(&self, dest: &str) -> Result<(), String>;
    /// Atomically acquire/renew a TTL lease at `key` (leader election). A mutation.
    fn lease_acquire(&mut self, key: &str, holder: &str, ttl_ms: u64) -> crate::lease::LeaseOutcome;
}

impl<St: IndexStore + Send + Sync> ShardOps for Shard<St> {
    fn object(&self, coll: &str, key: &str) -> Option<Value> {
        Shard::object(self, coll, key)
    }
    fn mget(&self, coll: &str, keys: &[String]) -> Vec<Option<Value>> {
        Shard::mget(self, coll, keys)
    }
    fn scan_range(&self, coll: &str, after: Option<&str>, prefix: Option<&str>, end: Option<&str>, limit: usize) -> Vec<String> {
        Shard::scan_range(self, coll, after, prefix, end, limit)
    }
    fn set_add(&mut self, coll: &str, key: &str, member: &str) -> bool {
        Shard::set_add(self, coll, key, member)
    }
    fn set_remove(&mut self, coll: &str, key: &str, member: &str) -> bool {
        Shard::set_remove(self, coll, key, member)
    }
    fn write_object(&mut self, coll: &str, key: &str, obj: Value) {
        Shard::write_object(self, coll, key, obj)
    }
    fn write_object_ttl(&mut self, coll: &str, key: &str, obj: Value, ttl_ms: u64) {
        Shard::write_object_ttl(self, coll, key, obj, ttl_ms)
    }
    fn sweep_expired(&mut self) -> usize {
        Shard::sweep_expired(self)
    }
    fn cas(&mut self, coll: &str, key: &str, expect: Option<Value>, new: Value) -> bool {
        Shard::cas(self, coll, key, expect, new)
    }
    fn incr(&mut self, coll: &str, key: &str, delta: i64) -> Option<i64> {
        Shard::incr(self, coll, key, delta)
    }
    fn write_batch(&mut self, items: Vec<(String, String, Value)>) {
        Shard::write_batch(self, items)
    }
    fn delete_object(&mut self, coll: &str, key: &str) {
        Shard::delete_object(self, coll, key)
    }
    fn export_entries(&self, coll: &str, buckets: &[usize]) -> Vec<(String, Value)> {
        Shard::export_entries(self, coll, buckets)
    }
    fn import_entries(&mut self, coll: &str, entries: Vec<(String, Value)>) {
        Shard::import_entries(self, coll, entries)
    }
    fn drop_buckets(&mut self, coll: &str, buckets: &[usize]) -> usize {
        Shard::drop_buckets(self, coll, buckets)
    }
    fn native_set_logic(&self) -> bool {
        false // memory/LMDB combine app-side; a Redis backend would override
    }
    fn snapshot(&self, dest: &str) -> Result<(), String> {
        Shard::snapshot(self, std::path::Path::new(dest))
    }
    fn lease_acquire(&mut self, key: &str, holder: &str, ttl_ms: u64) -> crate::lease::LeaseOutcome {
        Shard::lease_acquire(self, key, holder, ttl_ms)
    }
}

pub type SharedShard = Arc<RwLock<dyn ShardOps + Send + Sync>>;

/// Wrap any store-backed shard as a `SharedShard` (the return type is the unsizing coercion site).
pub fn shared<St: IndexStore + Send + Sync + 'static>(shard: Shard<St>) -> SharedShard {
    Arc::new(RwLock::new(shard))
}

// ---- wire bodies ----

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
#[derive(Serialize, Deserialize)]
struct MgetBody {
    coll: String,
    keys: Vec<String>,
}
#[derive(Serialize, Deserialize)]
struct SetBody {
    coll: String,
    key: String,
    member: String,
}
#[derive(Serialize, Deserialize)]
struct WriteBody {
    coll: String,
    key: String,
    obj: Value,
    /// Optional time-to-live in ms; absent or 0 = no expiry.
    #[serde(default, rename = "ttlMs", skip_serializing_if = "Option::is_none")]
    ttl_ms: Option<u64>,
}
#[derive(Serialize, Deserialize)]
struct DeleteBody {
    coll: String,
    key: String,
}
#[derive(Serialize, Deserialize)]
struct BucketBody {
    coll: String,
    buckets: Vec<usize>,
}
#[derive(Serialize, Deserialize)]
struct ImportBody {
    coll: String,
    entries: Vec<(String, Value)>,
}
#[derive(Serialize, Deserialize)]
struct CasBody {
    coll: String,
    key: String,
    #[serde(default)]
    expect: Option<Value>,
    new: Value,
}
#[derive(Serialize, Deserialize)]
struct IncrBody {
    coll: String,
    key: String,
    delta: i64,
}
#[derive(Deserialize)]
struct BackupQ {
    /// Destination file for the snapshot (e.g. a path on the backup volume).
    dest: String,
}
#[derive(Serialize, Deserialize)]
struct LeaseBody {
    key: String,
    holder: String,
    #[serde(rename = "ttlMs")]
    ttl_ms: u64,
}

// ---- server ----

pub fn app(state: SharedShard) -> Router {
    Router::new()
        .route("/health", get(|| async { Json(json!({"ok": true})) }))
        .route("/metrics", get(|| async { crate::metrics::render() }))
        .route("/caps", get(caps))
        .route("/object", get(object))
        .route("/mget", post(mget))
        .route("/scan", get(scan))
        .route("/setAdd", post(set_add))
        .route("/setRemove", post(set_remove))
        .route("/write", post(write))
        .route("/cas", post(cas))
        .route("/incr", post(incr))
        .route("/deleteObject", post(delete_object))
        .route("/export", post(export))
        .route("/import", post(import))
        .route("/dropBuckets", post(drop_buckets))
        .route("/backup", post(backup))
        .route("/leaseAcquire", post(lease_acquire))
        .with_state(state)
}

// Read handlers take a shared read lock — concurrent across worker threads.
async fn caps(State(s): State<SharedShard>) -> Json<Value> {
    Json(json!({ "nativeSetLogic": s.read().unwrap().native_set_logic() }))
}
async fn object(State(s): State<SharedShard>, Query(q): Query<ObjQ>) -> Json<Value> {
    crate::metrics::inc(&crate::metrics::GET);
    Json(json!({ "object": s.read().unwrap().object(&q.coll, &q.key) }))
}
/// Batch get on this shard — one value (or null) per requested key, in order (read op).
async fn mget(State(s): State<SharedShard>, Json(b): Json<MgetBody>) -> Json<Value> {
    crate::metrics::inc(&crate::metrics::GET);
    let vals = s.read().unwrap().mget(&b.coll, &b.keys);
    Json(json!({ "values": vals }))
}
/// Paginated key iteration on this shard with optional prefix/end bounds (read op). `limit` is
/// capped to keep a page bounded.
async fn scan(State(s): State<SharedShard>, Query(q): Query<ScanQ>) -> Json<Value> {
    let keys = s.read().unwrap().scan_range(&q.coll, q.after.as_deref(), q.prefix.as_deref(), q.end.as_deref(), q.limit.min(10_000));
    Json(json!({ "keys": keys }))
}
async fn export(State(s): State<SharedShard>, Json(b): Json<BucketBody>) -> Json<Value> {
    Json(json!({ "entries": s.read().unwrap().export_entries(&b.coll, &b.buckets) }))
}
/// Trigger a hot backup to `?dest=`. A read op (MVCC snapshot copy); the operator's CronJob POSTs
/// here and then ships the file off-box. `spawn_blocking` keeps the (possibly large) copy off the
/// async worker. Returns `{ok:false,error}` rather than a 5xx so the operator can log the reason.
async fn backup(State(s): State<SharedShard>, Query(q): Query<BackupQ>) -> Json<Value> {
    let dest = q.dest.clone();
    let res = tokio::task::spawn_blocking(move || s.read().unwrap().snapshot(&dest)).await;
    match res {
        Ok(Ok(())) => Json(json!({ "ok": true, "dest": q.dest })),
        Ok(Err(e)) => Json(json!({ "ok": false, "error": e })),
        Err(e) => Json(json!({ "ok": false, "error": format!("backup task panicked: {e}") })),
    }
}

// Mutating handlers take the exclusive write lock.
async fn set_add(State(s): State<SharedShard>, Json(b): Json<SetBody>) -> Json<Value> {
    crate::metrics::inc(&crate::metrics::SADD);
    let added = s.write().unwrap().set_add(&b.coll, &b.key, &b.member);
    Json(json!({ "added": added }))
}
async fn set_remove(State(s): State<SharedShard>, Json(b): Json<SetBody>) -> Json<Value> {
    crate::metrics::inc(&crate::metrics::SREM);
    let removed = s.write().unwrap().set_remove(&b.coll, &b.key, &b.member);
    Json(json!({ "removed": removed }))
}
async fn write(State(s): State<SharedShard>, Json(b): Json<WriteBody>) -> Json<Value> {
    crate::metrics::inc(&crate::metrics::SET);
    match b.ttl_ms {
        Some(ttl) if ttl > 0 => s.write().unwrap().write_object_ttl(&b.coll, &b.key, b.obj, ttl),
        _ => s.write().unwrap().write_object(&b.coll, &b.key, b.obj),
    }
    Json(json!({ "ok": true }))
}
async fn cas(State(s): State<SharedShard>, Json(b): Json<CasBody>) -> Json<Value> {
    crate::metrics::inc(&crate::metrics::SET);
    let set = s.write().unwrap().cas(&b.coll, &b.key, b.expect, b.new);
    Json(json!({ "set": set }))
}
async fn incr(State(s): State<SharedShard>, Json(b): Json<IncrBody>) -> Json<Value> {
    crate::metrics::inc(&crate::metrics::SET);
    let value = s.write().unwrap().incr(&b.coll, &b.key, b.delta);
    Json(json!({ "value": value }))
}
async fn delete_object(State(s): State<SharedShard>, Json(b): Json<DeleteBody>) -> Json<Value> {
    crate::metrics::inc(&crate::metrics::DEL);
    s.write().unwrap().delete_object(&b.coll, &b.key);
    Json(json!({ "ok": true }))
}
async fn lease_acquire(State(s): State<SharedShard>, Json(b): Json<LeaseBody>) -> Json<Value> {
    let o = s.write().unwrap().lease_acquire(&b.key, &b.holder, b.ttl_ms);
    Json(json!({
        "granted": o.granted,
        "holder": o.lease.holder,
        "expiresMs": o.lease.expires_ms,
        "token": o.lease.token,
    }))
}
async fn import(State(s): State<SharedShard>, Json(b): Json<ImportBody>) -> Json<Value> {
    s.write().unwrap().import_entries(&b.coll, b.entries);
    Json(json!({ "ok": true }))
}
async fn drop_buckets(State(s): State<SharedShard>, Json(b): Json<BucketBody>) -> Json<Value> {
    Json(json!({ "dropped": s.write().unwrap().drop_buckets(&b.coll, &b.buckets) }))
}

/// Bind an ephemeral port, spawn the server, and return its address (tests/dev).
pub async fn serve_ephemeral(state: SharedShard) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let app = app(state);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

// ---- client ----

/// Remote shard over HTTP — the coordinator's transport to a shard process.
pub struct HttpShardClient {
    id: String,
    base: String,
    http: reqwest::Client,
}

impl HttpShardClient {
    pub fn new(id: &str, base: &str) -> Self {
        Self::new_authed(id, base, None)
    }

    /// Like [`new`], but presents `Authorization: Bearer <token>` on every request (baked into the
    /// reqwest client's default headers) when a token is configured.
    pub fn new_authed(id: &str, base: &str, token: Option<&str>) -> Self {
        // Timeouts matter for the membership poller: a probe to a dead node must fail fast, not
        // hang discovery. connect_timeout bounds the dead-host case; timeout bounds large moves.
        let mut builder = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(2))
            .timeout(std::time::Duration::from_secs(15));
        if let Some(t) = token {
            let mut h = reqwest::header::HeaderMap::new();
            if let Ok(v) = reqwest::header::HeaderValue::from_str(&format!("Bearer {t}")) {
                h.insert(reqwest::header::AUTHORIZATION, v);
                builder = builder.default_headers(h);
            }
        }
        builder = crate::tls::client_tls().apply(builder); // trust the cluster CA for https shards
        let http = builder.build().unwrap_or_else(|_| reqwest::Client::new());
        Self { id: id.to_string(), base: base.trim_end_matches('/').to_string(), http }
    }
}

#[async_trait]
impl ShardClient for HttpShardClient {
    fn id(&self) -> &str {
        &self.id
    }

    fn endpoint(&self) -> &str {
        &self.base
    }

    async fn health(&self) -> bool {
        self.http.get(format!("{}/health", self.base)).send().await.map(|r| r.status().is_success()).unwrap_or(false)
    }

    async fn object(&self, coll: &str, key: &str) -> CResult<Option<Value>> {
        let v: Value = self
            .http
            .get(format!("{}/object", self.base))
            .query(&[("coll", coll), ("key", key)])
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        Ok(v.get("object").cloned().filter(|o| !o.is_null()))
    }

    async fn mget(&self, coll: &str, keys: &[String]) -> CResult<Vec<Option<Value>>> {
        let body = MgetBody { coll: coll.into(), keys: keys.to_vec() };
        let v: Value = self
            .http
            .post(format!("{}/mget", self.base))
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::from_value(v.get("values").cloned().unwrap_or(Value::Null)).unwrap_or_default())
    }

    async fn scan_range(&self, coll: &str, after: Option<&str>, prefix: Option<&str>, end: Option<&str>, limit: usize) -> CResult<Vec<String>> {
        let mut req = self.http.get(format!("{}/scan", self.base)).query(&[("coll", coll), ("limit", &limit.to_string())]);
        if let Some(a) = after {
            req = req.query(&[("after", a)]);
        }
        if let Some(p) = prefix {
            req = req.query(&[("prefix", p)]);
        }
        if let Some(e) = end {
            req = req.query(&[("end", e)]);
        }
        let v: Value = req.send().await.map_err(|e| e.to_string())?.json().await.map_err(|e| e.to_string())?;
        Ok(serde_json::from_value(v.get("keys").cloned().unwrap_or(Value::Null)).unwrap_or_default())
    }

    async fn set_add(&self, coll: &str, key: &str, member: &str) -> CResult<bool> {
        let body = SetBody { coll: coll.into(), key: key.into(), member: member.into() };
        let v: Value = self
            .http
            .post(format!("{}/setAdd", self.base))
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        Ok(v.get("added").and_then(|b| b.as_bool()).unwrap_or(false))
    }
    async fn set_remove(&self, coll: &str, key: &str, member: &str) -> CResult<bool> {
        let body = SetBody { coll: coll.into(), key: key.into(), member: member.into() };
        let v: Value = self
            .http
            .post(format!("{}/setRemove", self.base))
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        Ok(v.get("removed").and_then(|b| b.as_bool()).unwrap_or(false))
    }
    async fn write_object(&self, coll: &str, key: &str, obj: Value) -> CResult<()> {
        let body = WriteBody { coll: coll.into(), key: key.into(), obj, ttl_ms: None };
        post_ok(&self.http, &format!("{}/write", self.base), &body).await
    }
    async fn write_object_ttl(&self, coll: &str, key: &str, obj: Value, ttl_ms: u64) -> CResult<()> {
        let body = WriteBody { coll: coll.into(), key: key.into(), obj, ttl_ms: Some(ttl_ms) };
        post_ok(&self.http, &format!("{}/write", self.base), &body).await
    }
    async fn cas(&self, coll: &str, key: &str, expect: Option<Value>, new: Value) -> CResult<bool> {
        let body = CasBody { coll: coll.into(), key: key.into(), expect, new };
        let v: Value = self
            .http
            .post(format!("{}/cas", self.base))
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        Ok(v.get("set").and_then(|b| b.as_bool()).unwrap_or(false))
    }
    async fn incr(&self, coll: &str, key: &str, delta: i64) -> CResult<Option<i64>> {
        let body = IncrBody { coll: coll.into(), key: key.into(), delta };
        let v: Value = self
            .http
            .post(format!("{}/incr", self.base))
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        Ok(v.get("value").and_then(|n| n.as_i64()))
    }
    async fn delete_object(&self, coll: &str, key: &str) -> CResult<()> {
        let body = DeleteBody { coll: coll.into(), key: key.into() };
        post_ok(&self.http, &format!("{}/deleteObject", self.base), &body).await
    }

    async fn export_entries(&self, coll: &str, buckets: Vec<usize>) -> CResult<Vec<(String, Value)>> {
        let v: Value = self
            .http
            .post(format!("{}/export", self.base))
            .json(&BucketBody { coll: coll.into(), buckets })
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::from_value(v.get("entries").cloned().unwrap_or(Value::Null)).unwrap_or_default())
    }

    async fn import_entries(&self, coll: &str, entries: Vec<(String, Value)>) -> CResult<()> {
        let body = ImportBody { coll: coll.into(), entries };
        post_ok(&self.http, &format!("{}/import", self.base), &body).await
    }

    async fn drop_buckets(&self, coll: &str, buckets: Vec<usize>) -> CResult<usize> {
        let v: Value = self
            .http
            .post(format!("{}/dropBuckets", self.base))
            .json(&BucketBody { coll: coll.into(), buckets })
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        Ok(v.get("dropped").and_then(|d| d.as_u64()).unwrap_or(0) as usize)
    }

    async fn lease_acquire(&self, key: &str, holder: &str, ttl_ms: u64) -> CResult<crate::lease::LeaseOutcome> {
        let body = LeaseBody { key: key.into(), holder: holder.into(), ttl_ms };
        let v: Value = self
            .http
            .post(format!("{}/leaseAcquire", self.base))
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        Ok(crate::lease::LeaseOutcome {
            granted: v.get("granted").and_then(|b| b.as_bool()).unwrap_or(false),
            lease: crate::lease::Lease {
                holder: v.get("holder").and_then(|h| h.as_str()).unwrap_or_default().to_string(),
                expires_ms: v.get("expiresMs").and_then(|e| e.as_u64()).unwrap_or(0),
                token: v.get("token").and_then(|t| t.as_u64()).unwrap_or(0),
            },
        })
    }
}

async fn post_ok<B: Serialize>(http: &reqwest::Client, url: &str, body: &B) -> CResult<()> {
    http.post(url).json(body).send().await.and_then(|r| r.error_for_status()).map(|_| ()).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;
    const MAP: &str = "map"; // an opaque collection name; the substrate attaches no meaning to it

    #[tokio::test]
    async fn http_shard_store_roundtrip() {
        let shard: SharedShard = Arc::new(RwLock::new(Shard::new("s0", MemoryStore::new())));
        let addr = serve_ephemeral(shard).await;
        let client = HttpShardClient::new("s0", &format!("http://{addr}"));

        assert!(client.health().await, "server is up");

        // generic opaque-object write/read over the wire (the engine stores its MAP/IDX/OBJ
        // structures this way; the shard treats them as opaque values).
        let obj = json!({"keyValue":"ACME-001","locations":[{"table":"orders","rowId":"o1"}]});
        client.write_object(MAP, "ACME-001", obj.clone()).await.unwrap();
        assert_eq!(client.object(MAP, "ACME-001").await.unwrap(), Some(obj));

        // bucket export/import/drop work over the wire
        let bucket = crate::partitioner::bucket_of("ACME-001");
        let exported = client.export_entries(MAP, vec![bucket]).await.unwrap();
        assert_eq!(exported.len(), 1, "the key's MAP entry exports from its bucket");
        let dropped = client.drop_buckets(MAP, vec![bucket]).await.unwrap();
        assert_eq!(dropped, 1);
        assert!(client.object(MAP, "ACME-001").await.unwrap().is_none(), "dropped from the shard");
    }

    #[tokio::test]
    async fn http_bearer_auth() {
        let shard: SharedShard = Arc::new(RwLock::new(Shard::new("s0", MemoryStore::new())));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = require_bearer(app(shard), Some("sekret".into()));
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let http = reqwest::Client::new();
        let base = format!("http://{addr}");

        // no token -> 401
        let r = http.get(format!("{base}/object?coll=kv&key=k")).send().await.unwrap();
        assert_eq!(r.status().as_u16(), 401, "unauthenticated request rejected");
        // /health is exempt (probes)
        assert_eq!(http.get(format!("{base}/health")).send().await.unwrap().status().as_u16(), 200);
        // correct token -> 200
        let r = http.get(format!("{base}/object?coll=kv&key=k")).bearer_auth("sekret").send().await.unwrap();
        assert_eq!(r.status().as_u16(), 200, "authenticated request allowed");

        // an authed HttpShardClient round-trips end to end
        let c = HttpShardClient::new_authed("s0", &base, Some("sekret"));
        c.write_object(MAP, "k", json!("v")).await.unwrap();
        assert_eq!(c.object(MAP, "k").await.unwrap(), Some(json!("v")));
    }
}
