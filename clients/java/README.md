# stplr Java client

Dependency-free (JDK-only) Java client for [stplr](https://stplr.org) — a self-managing,
horizontally-scaling distributed key/value + set-ops store. Uses `java.net.http` and a tiny built-in
JSON util (Java has no stdlib JSON). Stored values are opaque JSON, passed/returned as JSON strings
(bind them with your own JSON library).

Requires Java 17+.

```java
import org.stplr.StplrClient;

StplrClient c = StplrClient.builder("https://coord:8080").token("secret").build();

c.set("kv", "alpha", "{\"hello\":\"world\"}");
var v = c.get("kv", "alpha");                 // Optional<String> of raw JSON

long hits = c.incr("kv", "hits", 1);
boolean locked = c.cas("kv", "lock", null, "\"held\"");   // set-if-absent

var vals = c.mget("kv", java.util.List.of("alpha", "beta"));
var keys = c.scanAll("kv", "user:");          // prefix-bounded, paginated
var parts = c.partitions();                   // parallel, locality-aware reads
```

## TLS

```java
StplrClient c = StplrClient.builder("https://coord:8080")
    .token("secret")
    .sslContext(myCustomSslContext())   // trust a private cluster CA
    .build();
// .insecureSkipVerify() is available for dev only
```

## Build & test (no build tool required)

```bash
javac -d build src/org/stplr/*.java test/org/stplr/*.java
java -cp build org.stplr.StplrClientTest
```

## API

`get` · `set` / `setTtl` · `delete` · `cas` · `incr` · `setAdd` / `setRemove` · `mget` ·
`scan` / `scanAll` (cursor, prefix, end) · `members` · `partitions`.

Licensed under BSL 1.1 (the stplr substrate's license).
