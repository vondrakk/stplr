# stplr HTTP protocol

The coordinator (and each shard) exposes a small JSON-over-HTTP API. This is the contract the
official SDKs implement ([Go](../clients/go), [Java](../clients/java), [Python](../clients/python));
it's stable enough to build a client against in any language.

There is also a binary hot-path protocol for high-throughput KV (length-prefixed frames over a
persistent TCP connection); this document covers the HTTP API, which exposes the full feature set.

## Conventions

- **Base URL** — the coordinator, e.g. `http://host:8080` (or `https://` with TLS).
- **Values are opaque JSON.** stplr stores and returns whatever JSON you put in a value; it attaches
  no meaning to it.
- **Collections** (`coll`) are flat namespaces for keys. **Keys** are strings.
- All request bodies and responses are JSON. Errors are non-2xx with a plain-text body.

## Authentication

- **Bearer token:** if the server is started with a token, send `Authorization: Bearer <token>`.
- **RBAC:** if started with a policy (`token:coll:perms;…`, perms = `r`ead / `w`rite / `d`elete /
  `a`dmin), each request is authorized by token against the target collection + operation:
  `401` = unknown/missing token, `403` = authenticated but not permitted.
- `GET /health` and `GET /metrics` are always unauthenticated.
- **TLS:** the server may serve HTTPS; clients trust the system roots or a supplied CA. The binary
  protocol has its own optional TLS.

## Endpoints

### Key/value

| Method & path | Request (JSON / query) | Response |
|---|---|---|
| `GET /object?coll=&key=` | — | `{ "object": <value> \| null }` |
| `POST /write` | `{ "coll", "key", "obj": <value>, "ttlMs"?: <int> }` | `{ "ok": true }` |
| `POST /deleteObject` | `{ "coll", "key" }` | `{ "ok": true }` |
| `POST /cas` | `{ "coll", "key", "new": <value>, "expect"?: <value> }` (omit `expect` = set-if-absent) | `{ "set": <bool> }` |
| `POST /incr` | `{ "coll", "key", "delta": <int> }` | `{ "value": <int> }` |
| `POST /mget` | `{ "coll", "keys": [<string>] }` | `{ "values": [<value> \| null] }` (input order) |

### Sets (server-side posting lists)

| Method & path | Request | Response |
|---|---|---|
| `POST /setAdd` | `{ "coll", "key", "member" }` | `{ "added": <bool> }` |
| `POST /setRemove` | `{ "coll", "key", "member" }` | `{ "removed": <bool> }` |

### Iteration

| Method & path | Request (query) | Response |
|---|---|---|
| `GET /scan?coll=&after=&prefix=&end=&limit=` | `after` = cursor (exclusive); `prefix`, `end` (exclusive) optional; `limit` default 1000 | `{ "keys": [<string>], "cursor": <string> \| null }` |
| `POST /scanBuckets` *(shard)* | `{ "coll", "buckets": [<int>], "after"?, "limit"? }` | `{ "keys": [<string>] }` |

Page through `scan` by passing the returned `cursor` as the next `after`; a `null` cursor means the
collection (or range) is drained.

### Topology

| Method & path | Response |
|---|---|
| `GET /members` | `{ "epoch": <int>, "members": [{ "id", "endpoint" }] }` |
| `GET /partitions` | `{ "epoch": <int>, "partitions": [{ "id", "endpoint", "buckets": [<int>] }] }` |

`partitions` returns the keyspace tiled exactly once by primary-owned buckets — drive one reader per
partition (against its shard's `endpoint` + `/scanBuckets`) for parallel, locality-aware, exactly-once
full scans (e.g. one Spark task per shard).

### Operational

| Method & path | Response |
|---|---|
| `GET /health` | `{ "ok": true }` |
| `GET /metrics` | Prometheus text exposition |

## Example

```bash
curl -s -H "Authorization: Bearer $TOK" -H 'content-type: application/json' \
  -X POST http://host:8080/write -d '{"coll":"kv","key":"alpha","obj":{"hello":"world"}}'

curl -s -H "Authorization: Bearer $TOK" 'http://host:8080/object?coll=kv&key=alpha'
# {"object":{"hello":"world"}}
```

## Building an SDK

A client is roughly: an HTTP client with the bearer header, JSON (de)serialization of the bodies
above, and a `scan` loop that follows the cursor. See the
[Go](../clients/go/stplr.go), [Java](../clients/java/src/org/stplr/StplrClient.java), and
[Python](../clients/python/stplr/__init__.py) clients for compact reference implementations.

Licensed under BSL 1.1 (the stplr substrate's license).
