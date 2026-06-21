# stplr Python client

Dependency-free (stdlib-only) Python client for [stplr](https://stplr.org) — a self-managing,
horizontally-scaling distributed key/value + set-ops store. Values are native Python objects.

```bash
pip install stplr   # or: pip install git+https://github.com/vondrakk/stplr#subdirectory=clients/python
```

```python
from stplr import Client

c = Client("https://coord:8080", token="secret")

c.set("kv", "alpha", {"hello": "world"})
print(c.get("kv", "alpha"))            # {'hello': 'world'}   (None if absent)

c.incr("kv", "hits", 1)
c.cas("kv", "lock", "held")            # set-if-absent
c.cas("kv", "lock", "next", expect="held")

c.mget("kv", ["alpha", "beta"])        # [value, value-or-None]
list(c.scan_all("kv", prefix="user:")) # paginated key iteration
```

## TLS

```python
c = Client("https://coord:8080", token="secret", ca_cert=open("ca.pem").read())
# insecure=True is available for dev only
```

## Parallel reads (PySpark / any compute)

`partitions()` returns one entry per shard with the buckets it primary-owns — the keyspace tiled
exactly once. Drive one reader per partition for locality-aware, exactly-once parallel scans:

```python
parts = c.partitions()                 # [{'id', 'endpoint', 'buckets'}, ...]
rdd = sc.parallelize(parts, len(parts)).flatMap(read_partition)   # one Spark task per shard
```

## Test

```bash
cd clients/python && python3 -m unittest -v   # no deps, no network
```

## API

`get` · `set` / `set_ttl` · `delete` · `cas` · `incr` · `set_add` / `set_remove` · `mget` ·
`scan` / `scan_all` (cursor, prefix, end) · `members` · `partitions`.

Licensed under BSL 1.1 (the stplr substrate's license).
