# stplr

**A self-managing, horizontally-scaling distributed key/value + set-ops store.**

stplr is the open substrate underneath higher-level data systems. It knows keys,
collections, opaque value objects, opaque set members, and buckets — and nothing
about what's stored in them. Any query or correlation semantics layer *on top* of
stplr and never leak back down.

The pitch: **durable-tier throughput at cache-tier operational simplicity.** One
self-managing binary, started in seconds, that on a single node out-reads and
out-writes the open durable field — while you scale it by adding shards, not by
hand-tuning a cluster.

## Performance

On matched single nodes (4 vCPU, LMDB-durable, 128 clients, 64-byte values), stplr's
binary protocol leads the open durable field — Cassandra, ScyllaDB, etcd, TiKV,
FoundationDB — on read and write throughput:

| Op | stplr | ScyllaDB | etcd | Cassandra |
|----|------:|---------:|-----:|----------:|
| GET | ~204k op/s | ~75k | ~38k | ~38k |
| SET (durable) | ~129k op/s | ~77k | ~29k | ~29k |

In-memory caches (Valkey/Memcached) win raw point ops by design — different tier,
not durable. These are **single-node, per-shard** numbers; ScyllaDB is shard-per-core
and scales with cores on a fat box, where stplr scales **horizontally** (add shards).
Methodology + the load harness: see the `stplr-bench` project.

## Demo

```sh
./demo/demo.sh   # builds stplrd, starts a 3-shard durable cluster + coordinator on localhost
                 # (leader election, durable ingest queue, bearer auth) and walks every feature
                 # — write/read, set ops, TTL, CAS/INCR, auth, leader, ingest queue, metrics, backup.
```

Pitch deck: <https://stplr.org/deck/>.

## Quickstart

```sh
# a durable shard, HTTP API + the fast binary protocol
stplrd --store lmdb --path /var/lib/stplr --bind 0.0.0.0:8100 --bin-bind 0.0.0.0:8101

# a coordinator routing across shards (replication + failover)
stplrd --role coordinator --bind 0.0.0.0:8080 \
       --shards s0=10.0.0.1:8100,s1=10.0.0.2:8100 --replication 2

# rack/zone-aware replicas: spread each key's copies across distinct racks
stplrd --role coordinator --bind 0.0.0.0:8080 \
       --shards s0=10.0.0.1:8100,s1=10.0.0.2:8100,s2=10.0.1.1:8100 \
       --racks s0=zone-a,s1=zone-b,s2=zone-a --replication 2

# HA: run multiple coordinators with leader election (one drives rebalancing)
stplrd --role coordinator --bind 0.0.0.0:8080 --coordinator-id coord-a \
       --shards s0=10.0.0.1:8100,s1=10.0.0.2:8100 --replication 2
# GET /leader -> {"self","leader","isLeader","token"}
# POST /admin/rebalance {"nodes":[...]}  — runs only on the leader (else 409 + leader)
# POST /admin/drain {"node":"..."}        — same leader-gating
```

Pass `--coordinator-id` to enable a TTL-lease election: every coordinator races to
hold one lease (on the shard that owns it), and the holder is the singleton
rebalance/migration driver. The driver ops (`/admin/rebalance`, `/admin/drain`) are
leader-gated — fire them at any coordinator and a non-leader replica replies `409`
with the current leader's id (leader-redirect). A fencing `token` increments on
each change of leader so a deposed leader can detect it. It's a lease (Redis/Consul-
session model), not partition-tolerant consensus — if the lease shard is
unreachable no leader is elected (a safe pause) rather than two.

The HTTP API is `GET /object?coll=&key=`, `POST /write|/setAdd|/setRemove|/deleteObject`.
Add `"ttlMs": <n>` to a `/write` body for a per-key **TTL** — the key reads as absent once it
expires (lazy) and a background sweeper reclaims it; a plain write clears any TTL. A no-TTL
workload pays nothing for the feature (the expiry path is guarded behind a "any TTLs set?" flag).

**Atomic ops:** `POST /cas {coll,key,expect?,new}` → `{set}` is compare-and-set (omit `expect` for
set-if-absent); `POST /incr {coll,key,delta}` → `{value}` is an atomic counter (absent = 0, `null`
if the value isn't an integer). The read-modify-write runs on the key's **primary** owner under the
shard write lock, then the result replicates to the other owners — one authoritative arbiter, no
divergence.

### Authentication & transport security

Set `--auth-token <tok>` (or `STPLR_AUTH_TOKEN`) on a shard/coordinator to require
`Authorization: Bearer <tok>` on every HTTP route (except `/health` + `/metrics`, left open for
probes/scrapers) and an `OP_AUTH` handshake on the binary protocol. The coordinator presents the
same token to its shards; `HttpShardClient::new_authed` / `SmartClient::from_coordinator_authed`
present it client-side. One shared cluster secret; no token = open (dev / trusted mesh).

**TLS:** the listeners speak plaintext — terminate TLS at your mesh / ingress / load balancer (the
common in-cluster pattern, and it keeps the hot path free of a TLS dependency). Native in-process
TLS (rustls) is a roadmap item.
The **binary protocol** (`--bin-bind`) is a framed TCP wire (length-prefixed opcodes,
no HTTP/JSON envelope) — ~3.5× the HTTP throughput on reads, and the recommended hot path.

### Direct-to-shard client

`stplr::smart::SmartClient` routes reads **and writes** straight to the owning shards via the same
rendezvous placement the coordinator uses — but evaluated client-side, so the coordinator is off the
data path (it stays the control plane: membership, rebalance, leader election). One network hop
instead of two.

```rust
// discover the shards from a coordinator, then talk to them directly
let client = stplr::smart::SmartClient::from_coordinator("http://coord:8080", 2).await?;
client.put("kv", "alpha", json!("A")).await;          // -> written to alpha's replica set directly
let v = client.get("kv", "alpha").await;              // -> read from a live owner, with failover
```

It tracks the coordinator's membership **epoch** and refreshes its routing view when it advances
(`refresh()`, or `spawn_refresher(ms)`). Strongly consistent under stable membership; eventually
consistent during a rebalance until the view refreshes.

### Durable ingest queue

For crash-safe writes, run a coordinator with `--ingest-queue <dir>`: `POST /enqueue` appends the
write to a durable LMDB write-ahead log and acks with its sequence number *before* applying it. A
background drainer applies queued writes to the shards and advances a durable committed cursor, so a
coordinator crash replays only the un-applied tail — **at-least-once** (the ops are idempotent).
`GET /ingest/status` reports `accepted` / `committed` / `pending`.

```sh
stplrd --role coordinator --bind 0.0.0.0:8080 --shards s0=10.0.0.1:8100 \
       --ingest-queue /var/lib/stplr/ingest
curl -X POST localhost:8080/enqueue -H 'content-type: application/json' \
     -d '{"coll":"kv","key":"alpha","obj":"A"}'   # -> {"ok":true,"durable":true,"seq":1}
```

On Kubernetes/OpenShift, deploy with the **stplr-operator** (a `StplrCluster` CR → a
shard StatefulSet + coordinator with auto-sized PVCs).

## Architecture

- **Rendezvous (HRW) partitioning** over a fixed ring of 4096 virtual buckets, with
  **top-R replication** so every key survives node loss; deterministic placement means
  minimal data movement on membership change.
- **Zero-touch operations:** online rebalance, graceful drain, auto-heal, crash-resume,
  and live membership — no manual reshard, no downtime, no operator babysitting.
- **Generic data plane:** an object store `(collection, key) -> value` plus server-side
  posting-list set operations (`set_add` / `set_remove` / `set_members`).
- **Composable roles, one binary:** `shard` owns/serves a partition; `coordinator` routes
  and drives rebalancing. For maximum throughput, clients can route **directly to shards**
  (rendezvous-hash client-side), keeping the coordinator off the data path.
- **Durable by default:** LMDB-backed via `heed` (group-committed writes, `MDB_NOSYNC` +
  periodic sync) — the disk *is* the database. In-memory store available for dev/tests.
- **Replayable change feed:** a durable, value-level log you can tail and replay for
  recovery or downstream sync.

## Status

The substrate is built and benchmarked. Coordinator routing + horizontal sharding are in;
in flight: direct-to-shard write APIs, coordinator leader election, streaming/scheduled
backups + restore. See open PRs.

## License

stplr is licensed under the **Business Source License 1.1** (see [LICENSE](LICENSE)).

- **Licensor:** The Von Drakk Corporation
- Use, copy, modify, and run stplr in production for any purpose — **except** offering it
  (or a service deriving its value primarily from it) to third parties as a hosted/managed
  key-value, distributed-storage, or set-operation service. That requires a separate
  commercial license.
- **Change Date:** four years after each version is first published, that version converts
  to the **Apache License 2.0**.

Source-available today, true open source on a rolling per-version clock.
