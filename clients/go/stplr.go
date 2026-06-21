// Package stplr is the Go client for stplr — a self-managing, horizontally-scaling distributed
// key/value + set-ops store. It speaks the coordinator's HTTP API and is dependency-free (stdlib
// only). Construct a Client with New and call the typed methods; values are opaque JSON.
//
// Licensed under the Business Source License 1.1 (the stplr substrate's license).
package stplr

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client talks to a stplr coordinator (or a single shard) over HTTP.
type Client struct {
	base  string
	http  *http.Client
	token string
}

// Option configures a Client.
type Option func(*config)

type config struct {
	httpClient *http.Client
	token      string
	caPEM      []byte
	insecure   bool
	timeout    time.Duration
}

// WithToken presents Authorization: Bearer <token> on every request.
func WithToken(token string) Option { return func(c *config) { c.token = token } }

// WithCACert trusts a PEM CA certificate (a private cluster CA) for TLS.
func WithCACert(pem []byte) Option { return func(c *config) { c.caPEM = pem } }

// WithInsecureSkipVerify disables TLS certificate verification. DEV ONLY.
func WithInsecureSkipVerify() Option { return func(c *config) { c.insecure = true } }

// WithHTTPClient supplies a custom *http.Client (overrides TLS/timeout options).
func WithHTTPClient(h *http.Client) Option { return func(c *config) { c.httpClient = h } }

// WithTimeout sets the per-request timeout (default 15s).
func WithTimeout(d time.Duration) Option { return func(c *config) { c.timeout = d } }

// New creates a Client for the coordinator at baseURL (e.g. "https://coord:8080").
func New(baseURL string, opts ...Option) *Client {
	cfg := config{timeout: 15 * time.Second}
	for _, o := range opts {
		o(&cfg)
	}
	hc := cfg.httpClient
	if hc == nil {
		tr := &http.Transport{}
		if cfg.insecure || len(cfg.caPEM) > 0 {
			tlsCfg := &tls.Config{InsecureSkipVerify: cfg.insecure}
			if len(cfg.caPEM) > 0 {
				pool := x509.NewCertPool()
				pool.AppendCertsFromPEM(cfg.caPEM)
				tlsCfg.RootCAs = pool
			}
			tr.TLSClientConfig = tlsCfg
		}
		hc = &http.Client{Timeout: cfg.timeout, Transport: tr}
	}
	return &Client{base: strings.TrimRight(baseURL, "/"), http: hc, token: cfg.token}
}

// ---- core operations ----

// Get returns the raw JSON value for key and whether it was present.
func (c *Client) Get(ctx context.Context, coll, key string) (json.RawMessage, bool, error) {
	q := url.Values{"coll": {coll}, "key": {key}}
	var resp struct {
		Object json.RawMessage `json:"object"`
	}
	if err := c.do(ctx, http.MethodGet, "/object?"+q.Encode(), nil, &resp); err != nil {
		return nil, false, err
	}
	if len(resp.Object) == 0 || string(resp.Object) == "null" {
		return nil, false, nil
	}
	return resp.Object, true, nil
}

// GetInto unmarshals the value for key into dest. Returns false if the key is absent.
func (c *Client) GetInto(ctx context.Context, coll, key string, dest any) (bool, error) {
	raw, ok, err := c.Get(ctx, coll, key)
	if err != nil || !ok {
		return ok, err
	}
	return true, json.Unmarshal(raw, dest)
}

// Set writes value (any JSON-serializable) at key.
func (c *Client) Set(ctx context.Context, coll, key string, value any) error {
	return c.do(ctx, http.MethodPost, "/write", map[string]any{"coll": coll, "key": key, "obj": value}, nil)
}

// SetTTL writes value with a time-to-live; it expires ttl from now (0 = no expiry).
func (c *Client) SetTTL(ctx context.Context, coll, key string, value any, ttl time.Duration) error {
	return c.do(ctx, http.MethodPost, "/write",
		map[string]any{"coll": coll, "key": key, "obj": value, "ttlMs": ttl.Milliseconds()}, nil)
}

// Delete removes key.
func (c *Client) Delete(ctx context.Context, coll, key string) error {
	return c.do(ctx, http.MethodPost, "/deleteObject", map[string]any{"coll": coll, "key": key}, nil)
}

// CAS atomically sets key to newVal only if its current value equals expect (pass nil expect to
// require the key be absent). Returns whether the swap happened.
func (c *Client) CAS(ctx context.Context, coll, key string, expect, newVal any) (bool, error) {
	body := map[string]any{"coll": coll, "key": key, "new": newVal}
	if expect != nil {
		body["expect"] = expect
	}
	var resp struct {
		Set bool `json:"set"`
	}
	err := c.do(ctx, http.MethodPost, "/cas", body, &resp)
	return resp.Set, err
}

// Incr atomically adds delta to the integer value at key and returns the new value.
func (c *Client) Incr(ctx context.Context, coll, key string, delta int64) (int64, error) {
	var resp struct {
		Value int64 `json:"value"`
	}
	err := c.do(ctx, http.MethodPost, "/incr", map[string]any{"coll": coll, "key": key, "delta": delta}, &resp)
	return resp.Value, err
}

// SetAdd adds member to the set at key; returns whether it was newly added.
func (c *Client) SetAdd(ctx context.Context, coll, key, member string) (bool, error) {
	var resp struct {
		Added bool `json:"added"`
	}
	err := c.do(ctx, http.MethodPost, "/setAdd", map[string]any{"coll": coll, "key": key, "member": member}, &resp)
	return resp.Added, err
}

// SetRemove removes member from the set at key; returns whether it was present.
func (c *Client) SetRemove(ctx context.Context, coll, key, member string) (bool, error) {
	var resp struct {
		Removed bool `json:"removed"`
	}
	err := c.do(ctx, http.MethodPost, "/setRemove", map[string]any{"coll": coll, "key": key, "member": member}, &resp)
	return resp.Removed, err
}

// Mget fetches many keys in one call; result[i] is the raw value for keys[i] (nil if absent).
func (c *Client) Mget(ctx context.Context, coll string, keys []string) ([]json.RawMessage, error) {
	var resp struct {
		Values []json.RawMessage `json:"values"`
	}
	err := c.do(ctx, http.MethodPost, "/mget", map[string]any{"coll": coll, "keys": keys}, &resp)
	return resp.Values, err
}

// ScanOptions bounds a Scan: After is the pagination cursor (exclusive), Prefix restricts to keys
// starting with it, End is an exclusive upper bound, Limit caps the page (default server-side 1000).
type ScanOptions struct {
	After  string
	Prefix string
	End    string
	Limit  int
}

// Scan returns an ascending page of keys plus a cursor; pass the cursor as opts.After for the next
// page. An empty cursor means the collection (or range) is drained.
func (c *Client) Scan(ctx context.Context, coll string, opts ScanOptions) (keys []string, cursor string, err error) {
	q := url.Values{"coll": {coll}}
	if opts.After != "" {
		q.Set("after", opts.After)
	}
	if opts.Prefix != "" {
		q.Set("prefix", opts.Prefix)
	}
	if opts.End != "" {
		q.Set("end", opts.End)
	}
	if opts.Limit > 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}
	var resp struct {
		Keys   []string `json:"keys"`
		Cursor *string  `json:"cursor"`
	}
	if err = c.do(ctx, http.MethodGet, "/scan?"+q.Encode(), nil, &resp); err != nil {
		return nil, "", err
	}
	if resp.Cursor != nil {
		cursor = *resp.Cursor
	}
	return resp.Keys, cursor, nil
}

// ScanAll walks every key in coll (optionally prefix-filtered), paging until drained.
func (c *Client) ScanAll(ctx context.Context, coll, prefix string) ([]string, error) {
	var all []string
	after := ""
	for {
		page, cursor, err := c.Scan(ctx, coll, ScanOptions{After: after, Prefix: prefix, Limit: 1000})
		if err != nil {
			return all, err
		}
		all = append(all, page...)
		if cursor == "" {
			return all, nil
		}
		after = cursor
	}
}

// Member is a shard in the cluster.
type Member struct {
	ID       string `json:"id"`
	Endpoint string `json:"endpoint"`
}

// Members lists the cluster's shards.
func (c *Client) Members(ctx context.Context) ([]Member, error) {
	var resp struct {
		Members []Member `json:"members"`
	}
	err := c.do(ctx, http.MethodGet, "/members", nil, &resp)
	return resp.Members, err
}

// Partition is a shard plus the buckets it primary-owns — a unit of parallel reads.
type Partition struct {
	ID       string `json:"id"`
	Endpoint string `json:"endpoint"`
	Buckets  []int  `json:"buckets"`
}

// Partitions returns the partition plan (tiles the keyspace once) for parallel, locality-aware reads.
func (c *Client) Partitions(ctx context.Context) ([]Partition, error) {
	var resp struct {
		Partitions []Partition `json:"partitions"`
	}
	err := c.do(ctx, http.MethodGet, "/partitions", nil, &resp)
	return resp.Partitions, err
}

// ---- transport ----

func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("stplr: %s %s -> %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}
