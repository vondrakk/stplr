# stplr client SDKs

Official, dependency-free clients for [stplr](https://stplr.org). All implement the same
[HTTP protocol](../docs/PROTOCOL.md): KV, atomic CAS/INCR, server-side set ops, batch `mget`,
cursor/prefix/range `scan`, plus `members` / `partitions` for parallel reads. All support bearer
auth and TLS.

| Language | Path | Notes |
|---|---|---|
| **Go** | [`go/`](go) | stdlib-only; `go get github.com/vondrakk/stplr/clients/go`. Includes the [`stplrctl`](go/cmd/stplrctl) operator CLI. |
| **Java** | [`java/`](java) | JDK-only (`java.net.http` + a tiny built-in JSON); Java 17+, builds with `javac`. |
| **Python** | [`python/`](python) | stdlib-only, native-object values; `pip install stplr`. README shows PySpark partition-parallel reads. |

Each client is tested against a mock of the wire contract (no network, no third-party deps). To add
a client in another language, follow [docs/PROTOCOL.md](../docs/PROTOCOL.md).
