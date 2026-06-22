// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! stplrd — the standalone stplr substrate server.
//!
//! Runs a single shard node serving the generic object + set-ops API over HTTP (the routes defined
//! in `stplr::net`). This is the runnable face of the stplr library: `stplr` is a crate other
//! systems embed, `stplrd` is the daemon you deploy and point a client at. Runs as a shard (owns a
//! partition) or a coordinator (routes client ops across shards) — choose with `--role`.
//!
//!   stplrd --bind 0.0.0.0:8100 --store lmdb --path /var/lib/stplr --map-size 8gb
//!   stplrd --role coordinator --bind 0.0.0.0:8080 --shards s0=10.0.0.1:8100,s1=10.0.0.2:8100 --replication 2

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use stplr::client::ShardClient;
use stplr::cluster::Cluster;
use stplr::lmdb::LmdbStore;
use stplr::net::{app, shared, HttpShardClient, SharedShard};
use stplr::partitioner::NodeId;
use stplr::shard::Shard;
use stplr::store::MemoryStore;

struct Args {
    role: String,
    bind: String,
    bin_bind: Option<String>,
    resp_bind: Option<String>,
    // PITR: record mutations to a write-ahead log at this dir; restore mode replays it.
    pitr_log: Option<PathBuf>,
    pitr_restore_to: Option<u64>,
    // Active-passive geo-replication: tail the PITR WAL and ship to this peer cluster's /geo/apply.
    geo_peer: Option<String>,
    geo_interval_ms: u64,
    store: String,
    path: PathBuf,
    map_size: usize,
    id: String,
    shards: Option<String>,
    racks: Option<String>,
    verify_snapshot: Option<String>,
    coordinator_id: Option<String>,
    lease_ttl_ms: u64,
    ingest_queue: Option<PathBuf>,
    auth_token: Option<String>,
    replication: usize,
    // StatefulSet shard discovery (coordinator): pods <shard_name>-0..N at <pod>.<shard_domain>.
    shard_count: usize,
    shard_name: Option<String>,
    shard_domain: Option<String>,
    shard_port: u16,
    // In-process TLS. Server: cert+key (both required) enable TLS on the binary + HTTP transports.
    // Client (coordinator -> shard): a CA to trust, or insecure to skip verification (dev).
    tls_cert: Option<String>,
    tls_key: Option<String>,
    tls_ca: Option<String>,
    tls_insecure: bool,
    // RBAC policy spec ("token:coll:perms;..."); when set it replaces the single --auth-token.
    policy: Option<String>,
}

/// Build the server TLS materials from `--tls-cert`/`--tls-key` (None when neither is set).
fn build_server_tls(args: &Args) -> anyhow::Result<Option<stplr::tls::ServerTls>> {
    match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => Ok(Some(stplr::tls::ServerTls::from_pem(cert, key)?)),
        (None, None) => Ok(None),
        _ => anyhow::bail!("--tls-cert and --tls-key must be given together"),
    }
}

fn print_help() {
    eprintln!(
        "stplrd — standalone stplr substrate server\n\n\
         USAGE:\n  stplrd [OPTIONS]\n\n\
         OPTIONS:\n\
         \x20 --role <ROLE>       shard | coordinator (default shard)\n\
         \x20 --bind <ADDR>       HTTP listen address (default 0.0.0.0:8100)\n\
         \x20 --bin-bind <ADDR>   binary-protocol listen address (shard only; e.g. 0.0.0.0:8101)\n\
         \x20 --resp-bind <ADDR>  Redis-compatible (RESP) listen address (shard only; e.g. 0.0.0.0:6379)\n\
         \x20 --pitr-log <DIR>    record all mutations to a PITR write-ahead log at DIR        [shard]\n\
         \x20 --pitr-restore-to <MS>  rebuild --path store from --pitr-log as of epoch-ms MS, then exit\n\
         \x20 --geo-peer <URL>     active-passive: tail --pitr-log + ship mutations to peer's /geo/apply\n\
         \x20 --geo-interval-ms <N>  geo-replication poll interval (default 1000)              [shard]\n\
         \x20 --store <KIND>      memory | lmdb (default memory)            [shard]\n\
         \x20 --path <DIR>        lmdb data directory (default ./stplr-data) [shard]\n\
         \x20 --map-size <SIZE>   lmdb map ceiling, e.g. 512mb, 8gb          [shard]\n\
         \x20 --id <NODE_ID>      node id (default stplrd)                  [shard]\n\
         \x20 --shards <LIST>     id=host:port,... of the shards            [coordinator]\n\
         \x20 --racks <LIST>      id=rack,... for rack-aware replica spread  [coordinator]\n\
         \x20 --shard-count <N>   StatefulSet discovery: number of shard pods [coordinator]\n\
         \x20 --shard-name <NAME> StatefulSet name (pods <name>-0..N)        [coordinator]\n\
         \x20 --shard-domain <D>  headless service domain for per-pod DNS    [coordinator]\n\
         \x20 --shard-port <P>    shard port (default 8100)                  [coordinator]\n\
         \x20 --replication <N>   copies of each key across shards (default 2) [coordinator]\n\
         \x20 --coordinator-id <ID>  enable leader election; this coordinator's lease holder id\n\
         \x20 --lease-ttl-ms <N>  leader lease TTL in ms (default 10000)     [coordinator]\n\
         \x20 --ingest-queue <DIR>  durable write-ahead ingest queue at DIR  [coordinator]\n\
         \x20 --auth-token <TOK>  require Bearer <TOK> on the API (or env STPLR_AUTH_TOKEN)\n\
         \x20 --policy <SPEC>     RBAC: 'token:coll:perms;...' (perms r/w/a, coll or *); replaces --auth-token\n\
         \x20 --tls-cert <FILE>   PEM cert chain — enables TLS on the binary + HTTP transports (with --tls-key)\n\
         \x20 --tls-key <FILE>    PEM private key for --tls-cert\n\
         \x20 --tls-ca <FILE>     PEM CA to trust for coordinator->shard https (client side)\n\
         \x20 --tls-insecure      skip cert verification on client connections (DEV ONLY)\n\
         \x20 --verify-snapshot <FILE>  inspect a backup snapshot (read-only) and exit\n\
         \x20 -h, --help         this help"
    );
}

/// Parse a byte size: a plain integer, or with a `kb`/`mb`/`gb` suffix (case-insensitive).
fn parse_size(s: &str) -> Option<usize> {
    let s = s.trim().to_lowercase();
    let (num, mult) = if let Some(n) = s.strip_suffix("gb") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("mb") {
        (n, 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("kb") {
        (n, 1024)
    } else {
        (s.as_str(), 1)
    };
    num.trim().parse::<usize>().ok().map(|v| v * mult)
}

fn parse_args() -> Args {
    let mut a = Args {
        role: "shard".into(),
        bind: "0.0.0.0:8100".into(),
        bin_bind: None,
        resp_bind: None,
        pitr_log: None,
        pitr_restore_to: None,
        geo_peer: None,
        geo_interval_ms: 1000,
        store: "memory".into(),
        path: PathBuf::from("./stplr-data"),
        map_size: 2 * 1024 * 1024 * 1024,
        id: "stplrd".into(),
        shards: None,
        racks: None,
        verify_snapshot: None,
        coordinator_id: None,
        lease_ttl_ms: 10_000,
        ingest_queue: None,
        auth_token: std::env::var("STPLR_AUTH_TOKEN").ok().filter(|s| !s.is_empty()),
        replication: 2,
        shard_count: 0,
        shard_name: None,
        shard_domain: None,
        shard_port: 8100,
        tls_cert: None,
        tls_key: None,
        tls_ca: None,
        tls_insecure: false,
        policy: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--role" => a.role = it.next().unwrap_or(a.role),
            "--bind" => a.bind = it.next().unwrap_or(a.bind),
            "--bin-bind" => a.bin_bind = it.next(),
            "--resp-bind" => a.resp_bind = it.next(),
            "--pitr-log" => a.pitr_log = it.next().map(PathBuf::from),
            "--pitr-restore-to" => a.pitr_restore_to = it.next().and_then(|s| s.parse().ok()),
            "--geo-peer" => a.geo_peer = it.next(),
            "--geo-interval-ms" => a.geo_interval_ms = it.next().and_then(|s| s.parse().ok()).unwrap_or(a.geo_interval_ms),
            "--store" => a.store = it.next().unwrap_or(a.store),
            "--path" => a.path = it.next().map(PathBuf::from).unwrap_or(a.path),
            "--map-size" => a.map_size = it.next().as_deref().and_then(parse_size).unwrap_or(a.map_size),
            "--id" => a.id = it.next().unwrap_or(a.id),
            "--shards" => a.shards = it.next(),
            "--racks" => a.racks = it.next(),
            "--verify-snapshot" => a.verify_snapshot = it.next(),
            "--coordinator-id" => a.coordinator_id = it.next(),
            "--lease-ttl-ms" => a.lease_ttl_ms = it.next().and_then(|s| s.parse().ok()).unwrap_or(a.lease_ttl_ms),
            "--ingest-queue" => a.ingest_queue = it.next().map(PathBuf::from),
            "--auth-token" => a.auth_token = it.next(),
            "--replication" => a.replication = it.next().and_then(|s| s.parse().ok()).unwrap_or(a.replication),
            "--shard-count" => a.shard_count = it.next().and_then(|s| s.parse().ok()).unwrap_or(a.shard_count),
            "--shard-name" => a.shard_name = it.next(),
            "--shard-domain" => a.shard_domain = it.next(),
            "--shard-port" => a.shard_port = it.next().and_then(|s| s.parse().ok()).unwrap_or(a.shard_port),
            "--tls-cert" => a.tls_cert = it.next(),
            "--tls-key" => a.tls_key = it.next(),
            "--tls-ca" => a.tls_ca = it.next(),
            "--tls-insecure" => a.tls_insecure = true,
            "--policy" => a.policy = it.next(),
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other => {
                eprintln!("stplrd: unknown argument '{other}'\n");
                print_help();
                std::process::exit(2);
            }
        }
    }
    a
}

/// Coordinator role: build a Cluster from the `--shards` list and serve the routed client API.
async fn run_coordinator(args: &Args) -> anyhow::Result<()> {
    let mut clients: HashMap<NodeId, Arc<dyn ShardClient>> = HashMap::new();
    if let Some(spec) = args.shards.as_deref() {
        // Explicit list: id=host:port,...
        for part in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (id, hostport) =
                part.split_once('=').ok_or_else(|| anyhow::anyhow!("bad --shards entry '{part}' (want id=host:port)"))?;
            let base = format!("http://{}", hostport.trim());
            clients.insert(id.trim().to_string(), Arc::new(HttpShardClient::new_authed(id.trim(), &base, args.auth_token.as_deref())));
        }
    } else if args.shard_count > 0 {
        // StatefulSet discovery: <shard_name>-<i> at <shard_name>-<i>.<shard_domain>:<shard_port>.
        let name = args.shard_name.as_deref().ok_or_else(|| anyhow::anyhow!("--shard-count needs --shard-name"))?;
        let domain = args.shard_domain.as_deref().ok_or_else(|| anyhow::anyhow!("--shard-count needs --shard-domain"))?;
        for i in 0..args.shard_count {
            let id = format!("{name}-{i}");
            let base = format!("http://{id}.{domain}:{}", args.shard_port);
            clients.insert(id.clone(), Arc::new(HttpShardClient::new_authed(&id, &base, args.auth_token.as_deref())));
        }
    }
    if clients.is_empty() {
        anyhow::bail!("coordinator needs --shards id=host:port,... OR --shard-count/--shard-name/--shard-domain");
    }
    let n = clients.len();
    // Optional rack-aware replica placement: id=rack,... spreads each key's replicas across racks.
    let mut topology: HashMap<NodeId, String> = HashMap::new();
    if let Some(spec) = args.racks.as_deref() {
        for part in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (id, rack) =
                part.split_once('=').ok_or_else(|| anyhow::anyhow!("bad --racks entry '{part}' (want id=rack)"))?;
            topology.insert(id.trim().to_string(), rack.trim().to_string());
        }
    }
    let racks = topology.len();
    let mut cluster = Cluster::new(clients, args.replication, vec!["kv".to_string(), "sets".to_string()]);
    if !topology.is_empty() {
        cluster = cluster.with_topology(topology);
    }
    let cluster = Arc::new(cluster);
    // Optional durable ingest queue (write-ahead log; crash-safe at-least-once writes).
    let queue = match &args.ingest_queue {
        Some(path) => Some(Arc::new(stplr::ingest::IngestQueue::open(path, args.map_size)?)),
        None => None,
    };
    let elect = args.coordinator_id.clone().map(|id| (id, args.lease_ttl_ms));
    let server_tls = build_server_tls(args)?;
    let addr: SocketAddr = args.bind.parse().map_err(|e| anyhow::anyhow!("bad --bind '{}': {e}", args.bind))?;
    eprintln!(
        "stplrd coordinator on {}://{addr} ({n} shards, replication {}{}{}{})",
        if server_tls.is_some() { "https" } else { "http" },
        args.replication,
        if racks > 0 { format!(", rack-aware across {racks} mapped nodes") } else { String::new() },
        match &args.coordinator_id {
            Some(id) => format!(", leader election as '{id}' (lease {}ms)", args.lease_ttl_ms),
            None => String::new(),
        },
        match &args.ingest_queue {
            Some(p) => format!(", durable ingest queue at {}", p.display()),
            None => String::new(),
        }
    );
    let policy = match &args.policy {
        Some(spec) => Some(std::sync::Arc::new(stplr::rbac::Policy::parse(spec)?)),
        None => None,
    };
    stplr::coord::serve_addr_full(addr, cluster, elect, queue, args.auth_token.clone(), server_tls, policy).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = parse_args();
    stplr::metrics::init();
    // Cluster-wide client TLS (coordinator -> shard hop) is process-global; set once here.
    stplr::tls::set_client_tls(stplr::tls::ClientTls { ca_pem: args.tls_ca.clone(), insecure: args.tls_insecure });

    // Backup inspection mode: validate a snapshot file (read-only) and exit. Non-zero exit on a
    // bad/empty backup so an operator/CI check can gate on it.
    if let Some(file) = &args.verify_snapshot {
        let stats = LmdbStore::verify_snapshot(std::path::Path::new(file))?;
        let total: usize = stats.iter().map(|(_, n)| n).sum();
        eprintln!("snapshot {file}: {} collection(s), {total} entries", stats.len());
        for (name, n) in &stats {
            eprintln!("  {name}\t{n}");
        }
        if total == 0 {
            anyhow::bail!("snapshot has no entries — refusing to vouch for an empty backup");
        }
        return Ok(());
    }

    // PITR restore mode: rebuild the --path lmdb store from the --pitr-log WAL as of a timestamp, then
    // exit. Run before serving so an operator restores into a fresh data dir offline.
    if let Some(target) = args.pitr_restore_to {
        let dir = args.pitr_log.as_ref().ok_or_else(|| anyhow::anyhow!("--pitr-restore-to needs --pitr-log <DIR>"))?;
        let wal = stplr::pitr::LmdbWal::open(dir, args.map_size)?;
        let mut store = LmdbStore::open(&args.path, args.map_size)?;
        let n = stplr::pitr::restore_into(&wal, target, &mut store);
        eprintln!("PITR: replayed {n} op(s) from {} into {} as of epoch-ms {target}", dir.display(), args.path.display());
        return Ok(());
    }

    if args.role == "coordinator" {
        return run_coordinator(&args).await;
    }
    if args.role != "shard" {
        anyhow::bail!("unknown --role '{}' (expected shard | coordinator)", args.role);
    }

    // Optional PITR write-ahead log: when set, the shard records every mutation to it.
    let wal: Option<Arc<dyn stplr::pitr::Wal>> = match &args.pitr_log {
        Some(dir) => Some(Arc::new(stplr::pitr::LmdbWal::open(dir, args.map_size)?)),
        None => None,
    };
    let state: SharedShard = match args.store.as_str() {
        "memory" => {
            let mut s = Shard::new(&args.id, MemoryStore::new());
            if let Some(w) = &wal {
                s = s.with_wal(w.clone());
            }
            shared(s)
        }
        "lmdb" => {
            let mut s = Shard::new(&args.id, LmdbStore::open(&args.path, args.map_size)?);
            if let Some(w) = &wal {
                s = s.with_wal(w.clone());
            }
            shared(s)
        }
        other => anyhow::bail!("unknown store '{other}' (expected memory | lmdb)"),
    };
    if wal.is_some() {
        eprintln!("stplrd '{}' recording PITR log at {}", args.id, args.pitr_log.as_ref().unwrap().display());
    }

    // Active-passive geo-replication: tail the PITR WAL and ship mutations to the passive peer.
    if let Some(peer) = &args.geo_peer {
        let w = wal.clone().ok_or_else(|| anyhow::anyhow!("--geo-peer requires --pitr-log (the WAL is the replication source)"))?;
        let (peer, origin, interval) = (peer.clone(), args.id.clone(), args.geo_interval_ms);
        let group = format!("geo->{peer}");
        eprintln!("stplrd '{}' geo-replicating (active-passive) to {peer} every {interval}ms", args.id);
        tokio::spawn(stplr::georep::replicate_loop(w, peer, origin, group, interval));
    }

    // Background TTL sweeper: reclaim expired keys periodically (cheap no-op when no TTLs are set).
    {
        let sweep = state.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                let n = sweep.write().unwrap().sweep_expired();
                if n > 0 {
                    eprintln!("stplrd swept {n} expired key(s)");
                }
            }
        });
    }

    // TLS materials (cert+key) shared by both transports; None = plaintext (trusted mesh).
    let server_tls = build_server_tls(&args)?;

    // Optional binary protocol on its own port, sharing the same shard state.
    if let Some(b) = &args.bin_bind {
        let baddr: SocketAddr = b.parse().map_err(|e| anyhow::anyhow!("bad --bin-bind '{b}': {e}"))?;
        let bstate = state.clone();
        let btoken = args.auth_token.clone();
        let btls = server_tls.clone();
        eprintln!("stplrd '{}' binary protocol on {}{}", args.id, baddr, if btls.is_some() { " (TLS)" } else { "" });
        tokio::spawn(async move {
            let _ = stplr::proto::serve_addr_tls(baddr, bstate, btoken, btls).await;
        });
    }

    // Optional Redis-compatible (RESP) protocol, sharing the same shard state.
    if let Some(r) = &args.resp_bind {
        let raddr: SocketAddr = r.parse().map_err(|e| anyhow::anyhow!("bad --resp-bind '{r}': {e}"))?;
        let rstate = state.clone();
        let rtoken = args.auth_token.clone();
        eprintln!("stplrd '{}' RESP (Redis-compatible) on {}", args.id, raddr);
        tokio::spawn(async move {
            let _ = stplr::resp::serve_addr(raddr, rstate, rtoken).await;
        });
    }

    let addr: SocketAddr = args.bind.parse().map_err(|e| anyhow::anyhow!("bad --bind '{}': {e}", args.bind))?;
    eprintln!(
        "stplrd '{}' listening on {}://{} (store={}{}{})",
        args.id,
        if server_tls.is_some() { "https" } else { "http" },
        addr,
        args.store,
        if args.store == "lmdb" { format!(", path={}", args.path.display()) } else { String::new() },
        if args.auth_token.is_some() { ", bearer-auth" } else { "" }
    );
    let router = match &args.policy {
        Some(spec) => stplr::net::require_policy(app(state), std::sync::Arc::new(stplr::rbac::Policy::parse(spec)?)),
        None => stplr::net::require_bearer(app(state), args.auth_token),
    };
    stplr::tls::serve_router(addr, router, server_tls).await?;
    Ok(())
}
