package stplr

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// mockServer emulates the stplr coordinator's JSON contract over an in-memory map, so the client
// can be tested end-to-end without the Rust server. It also enforces the bearer token.
func mockServer(t *testing.T, token string) *httptest.Server {
	t.Helper()
	store := map[string]json.RawMessage{} // "coll\x00key" -> raw value
	k := func(coll, key string) string { return coll + "\x00" + key }

	mux := http.NewServeMux()
	auth := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if token != "" && r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			h(w, r)
		}
	}
	writeJSON := func(w http.ResponseWriter, v any) { _ = json.NewEncoder(w).Encode(v) }

	mux.HandleFunc("/object", auth(func(w http.ResponseWriter, r *http.Request) {
		v, ok := store[k(r.URL.Query().Get("coll"), r.URL.Query().Get("key"))]
		if !ok {
			writeJSON(w, map[string]any{"object": nil})
			return
		}
		writeJSON(w, map[string]json.RawMessage{"object": v})
	}))
	mux.HandleFunc("/write", auth(func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			Coll, Key string
			Obj       json.RawMessage
		}
		_ = json.NewDecoder(r.Body).Decode(&b)
		store[k(b.Coll, b.Key)] = b.Obj
		writeJSON(w, map[string]any{"ok": true})
	}))
	mux.HandleFunc("/deleteObject", auth(func(w http.ResponseWriter, r *http.Request) {
		var b struct{ Coll, Key string }
		_ = json.NewDecoder(r.Body).Decode(&b)
		delete(store, k(b.Coll, b.Key))
		writeJSON(w, map[string]any{"ok": true})
	}))
	mux.HandleFunc("/cas", auth(func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			Coll, Key      string
			Expect, New    json.RawMessage
		}
		_ = json.NewDecoder(r.Body).Decode(&b)
		cur, has := store[k(b.Coll, b.Key)]
		ok := (!has && len(b.Expect) == 0) || (has && string(cur) == string(b.Expect))
		if ok {
			store[k(b.Coll, b.Key)] = b.New
		}
		writeJSON(w, map[string]any{"set": ok})
	}))
	mux.HandleFunc("/incr", auth(func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			Coll, Key string
			Delta     int64
		}
		_ = json.NewDecoder(r.Body).Decode(&b)
		cur := int64(0)
		if v, ok := store[k(b.Coll, b.Key)]; ok {
			_ = json.Unmarshal(v, &cur)
		}
		cur += b.Delta
		store[k(b.Coll, b.Key)], _ = json.Marshal(cur)
		writeJSON(w, map[string]any{"value": cur})
	}))
	mux.HandleFunc("/mget", auth(func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			Coll string
			Keys []string
		}
		_ = json.NewDecoder(r.Body).Decode(&b)
		vals := make([]json.RawMessage, len(b.Keys))
		for i, key := range b.Keys {
			if v, ok := store[k(b.Coll, key)]; ok {
				vals[i] = v
			} else {
				vals[i] = json.RawMessage("null")
			}
		}
		writeJSON(w, map[string]any{"values": vals})
	}))
	mux.HandleFunc("/scan", auth(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		coll, after, prefix := q.Get("coll"), q.Get("after"), q.Get("prefix")
		limit := 1000
		if l := q.Get("limit"); l != "" {
			limit, _ = strconv.Atoi(l)
		}
		var keys []string
		for kk := range store {
			parts := strings.SplitN(kk, "\x00", 2)
			if parts[0] != coll {
				continue
			}
			key := parts[1]
			if key <= after || (prefix != "" && !strings.HasPrefix(key, prefix)) {
				continue
			}
			keys = append(keys, key)
		}
		sort.Strings(keys)
		var cursor any
		if len(keys) > limit {
			keys = keys[:limit]
			cursor = keys[len(keys)-1]
		}
		writeJSON(w, map[string]any{"keys": keys, "cursor": cursor})
	}))
	mux.HandleFunc("/members", auth(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"epoch": 1, "members": []Member{{ID: "s0", Endpoint: "http://s0:8100"}}})
	}))
	mux.HandleFunc("/partitions", auth(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"partitions": []Partition{{ID: "s0", Endpoint: "http://s0:8100", Buckets: []int{0, 1, 2}}}})
	}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestClientRoundTrips(t *testing.T) {
	srv := mockServer(t, "testtoken")
	c := New(srv.URL, WithToken("testtoken"))
	ctx := context.Background()

	// set / get
	if err := c.Set(ctx, "kv", "a", map[string]any{"n": 1}); err != nil {
		t.Fatal(err)
	}
	var got map[string]int
	ok, err := c.GetInto(ctx, "kv", "a", &got)
	if err != nil || !ok || got["n"] != 1 {
		t.Fatalf("GetInto = %v %v %v", got, ok, err)
	}
	if _, ok, _ := c.Get(ctx, "kv", "missing"); ok {
		t.Fatal("missing key reported present")
	}

	// incr
	if v, _ := c.Incr(ctx, "kv", "ctr", 5); v != 5 {
		t.Fatalf("incr=%d", v)
	}
	if v, _ := c.Incr(ctx, "kv", "ctr", 3); v != 8 {
		t.Fatalf("incr=%d", v)
	}

	// cas: succeeds on match, fails on mismatch
	if ok, _ := c.CAS(ctx, "kv", "lock", nil, "held"); !ok {
		t.Fatal("CAS set-if-absent should succeed")
	}
	if ok, _ := c.CAS(ctx, "kv", "lock", nil, "stolen"); ok {
		t.Fatal("CAS set-if-absent should fail when present")
	}

	// mget aligned to input order, absent -> null
	for _, key := range []string{"m1", "m2"} {
		_ = c.Set(ctx, "kv", key, key)
	}
	vals, err := c.Mget(ctx, "kv", []string{"m2", "nope", "m1"})
	if err != nil || len(vals) != 3 {
		t.Fatalf("mget=%v err=%v", vals, err)
	}
	if string(vals[0]) != `"m2"` || string(vals[1]) != "null" || string(vals[2]) != `"m1"` {
		t.Fatalf("mget order wrong: %s | %s | %s", vals[0], vals[1], vals[2])
	}

	// delete
	if err := c.Delete(ctx, "kv", "a"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := c.Get(ctx, "kv", "a"); ok {
		t.Fatal("deleted key still present")
	}

	// members + partitions
	if m, err := c.Members(ctx); err != nil || len(m) != 1 || m[0].ID != "s0" {
		t.Fatalf("members=%v err=%v", m, err)
	}
	if p, err := c.Partitions(ctx); err != nil || len(p) != 1 || len(p[0].Buckets) != 3 {
		t.Fatalf("partitions=%v err=%v", p, err)
	}
}

func TestScanPagination(t *testing.T) {
	srv := mockServer(t, "")
	c := New(srv.URL)
	ctx := context.Background()
	for i := 0; i < 25; i++ {
		_ = c.Set(ctx, "kv", "user:"+strconv.Itoa(100+i), i)
	}
	_ = c.Set(ctx, "kv", "other", 1)

	all, err := c.ScanAll(ctx, "kv", "user:")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 25 {
		t.Fatalf("prefix scan returned %d, want 25", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i-1] >= all[i] {
			t.Fatal("scan results not ascending/unique")
		}
	}

	// small-page pagination still covers everything
	seen := map[string]bool{}
	after := ""
	for {
		page, cursor, err := c.Scan(ctx, "kv", ScanOptions{After: after, Prefix: "user:", Limit: 7})
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range page {
			if seen[k] {
				t.Fatal("duplicate key across pages")
			}
			seen[k] = true
		}
		if cursor == "" {
			break
		}
		after = cursor
	}
	if len(seen) != 25 {
		t.Fatalf("paged scan saw %d, want 25", len(seen))
	}
}

func TestAuthRejected(t *testing.T) {
	srv := mockServer(t, "right")
	c := New(srv.URL, WithToken("wrong"))
	if err := c.Set(context.Background(), "kv", "a", 1); err == nil {
		t.Fatal("expected auth failure with wrong token")
	}
}
