# stplr Go client

Dependency-free (stdlib-only) Go client for [stplr](https://stplr.org) — a self-managing,
horizontally-scaling distributed key/value + set-ops store. Speaks the coordinator's HTTP API.

```bash
go get github.com/vondrakk/stplr/clients/go
```

```go
package main

import (
	"context"
	"fmt"

	stplr "github.com/vondrakk/stplr/clients/go"
)

func main() {
	c := stplr.New("https://coord:8080", stplr.WithToken("secret"))
	ctx := context.Background()

	// key/value
	_ = c.Set(ctx, "kv", "alpha", map[string]any{"hello": "world"})
	var v map[string]any
	if ok, _ := c.GetInto(ctx, "kv", "alpha", &v); ok {
		fmt.Println(v["hello"])
	}

	// atomic counter + compare-and-set
	n, _ := c.Incr(ctx, "kv", "hits", 1)
	swapped, _ := c.CAS(ctx, "kv", "lock", nil, "held") // set-if-absent
	_ = n
	_ = swapped

	// batch get
	vals, _ := c.Mget(ctx, "kv", []string{"alpha", "beta"})
	_ = vals

	// iterate keys (prefix-bounded, paginated)
	keys, _ := c.ScanAll(ctx, "kv", "user:")
	_ = keys

	// parallel, locality-aware full scan: one task per partition
	parts, _ := c.Partitions(ctx)
	_ = parts
}
```

## TLS

```go
c := stplr.New("https://coord:8080",
	stplr.WithToken("secret"),
	stplr.WithCACert(caPEM)) // trust a private cluster CA
```

`WithInsecureSkipVerify()` is available for dev only.

## API

`Get` / `GetInto` · `Set` / `SetTTL` · `Delete` · `CAS` · `Incr` · `SetAdd` / `SetRemove` ·
`Mget` · `Scan` / `ScanAll` (cursor, prefix, end bounds) · `Members` · `Partitions`.

Licensed under BSL 1.1 (the stplr substrate's license).
