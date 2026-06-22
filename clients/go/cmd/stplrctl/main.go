// Command stplrctl is the operator CLI for a stplr cluster. It wraps the coordinator HTTP API via
// the Go SDK: inspect membership and the partition plan, read/write/scan keys, counters, and sets.
//
//	stplrctl --addr https://coord:8080 --token $TOK members
//	stplrctl get kv alpha
//	stplrctl scan kv --prefix user: --limit 100
//
// Connection flags can also come from the environment: STPLR_ADDR, STPLR_TOKEN, STPLR_CA, STPLR_INSECURE.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	stplr "github.com/vondrakk/stplr/clients/go"
)

func main() {
	fs := flag.NewFlagSet("stplrctl", flag.ContinueOnError)
	addr := fs.String("addr", env("STPLR_ADDR", "http://localhost:8080"), "coordinator base URL")
	token := fs.String("token", os.Getenv("STPLR_TOKEN"), "bearer token")
	ca := fs.String("ca", os.Getenv("STPLR_CA"), "PEM CA cert file to trust")
	insecure := fs.Bool("insecure", os.Getenv("STPLR_INSECURE") == "1", "skip TLS verification (DEV ONLY)")
	asJSON := fs.Bool("json", false, "emit JSON instead of human-readable output")
	fs.Usage = usage
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	args := fs.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	var opts []stplr.Option
	if *token != "" {
		opts = append(opts, stplr.WithToken(*token))
	}
	if *ca != "" {
		pem, err := os.ReadFile(*ca)
		check(err)
		opts = append(opts, stplr.WithCACert(pem))
	}
	if *insecure {
		opts = append(opts, stplr.WithInsecureSkipVerify())
	}
	c := stplr.New(*addr, opts...)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd, rest := args[0], args[1:]
	if err := run(ctx, c, cmd, rest, *asJSON); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, c *stplr.Client, cmd string, a []string, asJSON bool) error {
	switch cmd {
	case "members":
		m, err := c.Members(ctx)
		if err != nil {
			return err
		}
		if asJSON {
			return emit(m)
		}
		for _, x := range m {
			fmt.Printf("%-16s %s\n", x.ID, x.Endpoint)
		}
		return nil

	case "partitions":
		p, err := c.Partitions(ctx)
		if err != nil {
			return err
		}
		if asJSON {
			return emit(p)
		}
		for _, x := range p {
			fmt.Printf("%-16s %-28s %d buckets\n", x.ID, x.Endpoint, len(x.Buckets))
		}
		return nil

	case "get":
		if len(a) != 2 {
			return fmt.Errorf("usage: get <coll> <key>")
		}
		raw, ok, err := c.Get(ctx, a[0], a[1])
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("not found")
		}
		fmt.Println(string(raw))
		return nil

	case "set":
		if len(a) != 3 {
			return fmt.Errorf("usage: set <coll> <key> <json-value>")
		}
		var v any
		if err := json.Unmarshal([]byte(a[2]), &v); err != nil {
			return fmt.Errorf("value must be valid JSON: %w", err)
		}
		return c.Set(ctx, a[0], a[1], v)

	case "del", "delete":
		if len(a) != 2 {
			return fmt.Errorf("usage: del <coll> <key>")
		}
		return c.Delete(ctx, a[0], a[1])

	case "incr":
		if len(a) != 3 {
			return fmt.Errorf("usage: incr <coll> <key> <delta>")
		}
		d, err := strconv.ParseInt(a[2], 10, 64)
		if err != nil {
			return err
		}
		n, err := c.Incr(ctx, a[0], a[1], d)
		if err != nil {
			return err
		}
		fmt.Println(n)
		return nil

	case "mget":
		if len(a) < 2 {
			return fmt.Errorf("usage: mget <coll> <key>...")
		}
		vals, err := c.Mget(ctx, a[0], a[1:])
		if err != nil {
			return err
		}
		if asJSON {
			return emit(vals)
		}
		for i, key := range a[1:] {
			v := "(absent)"
			if i < len(vals) && vals[i] != nil {
				v = string(vals[i])
			}
			fmt.Printf("%s\t%s\n", key, v)
		}
		return nil

	case "scan":
		sf := flag.NewFlagSet("scan", flag.ContinueOnError)
		prefix := sf.String("prefix", "", "key prefix")
		end := sf.String("end", "", "exclusive upper bound")
		limit := sf.Int("limit", 0, "max keys (0 = all, paged)")
		if len(a) < 1 {
			return fmt.Errorf("usage: scan <coll> [--prefix p] [--end e] [--limit n]")
		}
		coll := a[0]
		if err := sf.Parse(a[1:]); err != nil {
			return err
		}
		if *limit == 0 {
			keys, err := c.ScanAll(ctx, coll, *prefix)
			if err != nil {
				return err
			}
			return printKeys(keys, asJSON)
		}
		keys, _, err := c.Scan(ctx, coll, stplr.ScanOptions{Prefix: *prefix, End: *end, Limit: *limit})
		if err != nil {
			return err
		}
		return printKeys(keys, asJSON)

	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func printKeys(keys []string, asJSON bool) error {
	if asJSON {
		return emit(keys)
	}
	fmt.Println(strings.Join(keys, "\n"))
	return nil
}

func emit(v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `stplrctl — operator CLI for a stplr cluster

usage: stplrctl [--addr URL] [--token T] [--ca FILE] [--insecure] [--json] <command> [args]

commands:
  members                       list cluster shards
  partitions                    show the partition plan (shard + bucket counts)
  get <coll> <key>              print a value
  set <coll> <key> <json>       write a JSON value
  del <coll> <key>              delete a key
  incr <coll> <key> <delta>     atomic counter add, prints new value
  mget <coll> <key>...          batch get
  scan <coll> [--prefix p] [--end e] [--limit n]   iterate keys

env: STPLR_ADDR, STPLR_TOKEN, STPLR_CA, STPLR_INSECURE
`)
}
