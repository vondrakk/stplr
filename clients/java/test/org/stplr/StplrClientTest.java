// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
package org.stplr;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Self-contained test: a JDK HttpServer emulates the stplr JSON contract over an in-memory map, the
 * client drives it, and assertions run in main (no test framework / no network). Exits non-zero on
 * failure. Run: javac ... && java org.stplr.StplrClientTest
 */
public class StplrClientTest {
    static final String TOKEN = "testtoken";
    static final TreeMap<String, String> store = new TreeMap<>(); // "coll\0key" -> raw json value

    public static void main(String[] args) throws Exception {
        HttpServer srv = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        srv.createContext("/", StplrClientTest::handle);
        srv.start();
        int port = srv.getAddress().getPort();
        String base = "http://127.0.0.1:" + port;

        try {
            roundTrips(base);
            scanPagination(base);
            authRejected(base);
            System.out.println("ALL JAVA SDK TESTS PASSED");
        } catch (Throwable t) {
            System.err.println("FAIL: " + t.getMessage());
            t.printStackTrace();
            srv.stop(0);
            System.exit(1);
        }
        srv.stop(0);
    }

    static void roundTrips(String base) throws Exception {
        StplrClient c = StplrClient.builder(base).token(TOKEN).build();

        c.set("kv", "a", "{\"n\":1}");
        Optional<String> v = c.get("kv", "a");
        check(v.isPresent() && v.get().contains("\"n\":1"), "get after set: " + v);
        check(c.get("kv", "missing").isEmpty(), "missing key should be empty");

        check(c.incr("kv", "ctr", 5) == 5, "incr 5");
        check(c.incr("kv", "ctr", 3) == 8, "incr to 8");

        check(c.cas("kv", "lock", null, "\"held\""), "cas set-if-absent should succeed");
        check(!c.cas("kv", "lock", null, "\"stolen\""), "cas set-if-absent should fail when present");

        c.set("kv", "m1", "\"m1\"");
        c.set("kv", "m2", "\"m2\"");
        List<String> vals = c.mget("kv", List.of("m2", "nope", "m1"));
        check(vals.size() == 3, "mget size");
        check("\"m2\"".equals(vals.get(0)) && vals.get(1) == null && "\"m1\"".equals(vals.get(2)), "mget order/absent: " + vals);

        c.delete("kv", "a");
        check(c.get("kv", "a").isEmpty(), "deleted key gone");

        check(c.members().size() == 1 && c.members().get(0).id().equals("s0"), "members");
        check(c.partitions().get(0).buckets().size() == 3, "partitions buckets");
    }

    static void scanPagination(String base) throws Exception {
        StplrClient c = StplrClient.builder(base).token(TOKEN).build();
        for (int i = 0; i < 25; i++) c.set("kv", "user:" + (100 + i), String.valueOf(i));
        c.set("kv", "zzz", "1");

        List<String> all = c.scanAll("kv", "user:");
        check(all.size() == 25, "prefix scanAll size: " + all.size());
        for (int i = 1; i < all.size(); i++) check(all.get(i - 1).compareTo(all.get(i)) < 0, "scan ascending/unique");

        java.util.Set<String> seen = new java.util.HashSet<>();
        String after = "";
        while (true) {
            StplrClient.ScanPage p = c.scan("kv", after, "user:", null, 7);
            for (String k : p.keys()) check(seen.add(k), "no dup across pages");
            if (p.cursor() == null) break;
            after = p.cursor();
        }
        check(seen.size() == 25, "paged scan total: " + seen.size());
    }

    static void authRejected(String base) throws Exception {
        StplrClient c = StplrClient.builder(base).token("wrong").build();
        boolean threw = false;
        try {
            c.set("kv", "x", "1");
        } catch (Exception e) {
            threw = true;
        }
        check(threw, "wrong token should be rejected");
    }

    static void check(boolean cond, String msg) {
        if (!cond) throw new AssertionError(msg);
    }

    // ---- mock server emulating the stplr JSON contract ----
    static void handle(HttpExchange ex) throws java.io.IOException {
        try {
            if (!("Bearer " + TOKEN).equals(ex.getRequestHeaders().getFirst("Authorization"))) {
                respond(ex, 401, "{}");
                return;
            }
            String path = ex.getRequestURI().getPath();
            String query = ex.getRequestURI().getQuery();
            Map<String, String> q = parseQuery(query);
            String bodyStr = new String(readAll(ex.getRequestBody()), StandardCharsets.UTF_8);
            Map<?, ?> b = bodyStr.isEmpty() ? Map.of() : (Map<?, ?>) Json.parse(bodyStr);

            switch (path) {
                case "/object": {
                    String val = store.get(k(q.get("coll"), q.get("key")));
                    respond(ex, 200, "{\"object\":" + (val == null ? "null" : val) + "}");
                    return;
                }
                case "/write": {
                    store.put(k((String) b.get("coll"), (String) b.get("key")), Json.write(b.get("obj")));
                    respond(ex, 200, "{\"ok\":true}");
                    return;
                }
                case "/deleteObject": {
                    store.remove(k((String) b.get("coll"), (String) b.get("key")));
                    respond(ex, 200, "{\"ok\":true}");
                    return;
                }
                case "/cas": {
                    String key = k((String) b.get("coll"), (String) b.get("key"));
                    String cur = store.get(key);
                    String expect = b.get("expect") == null ? null : Json.write(b.get("expect"));
                    boolean ok = (cur == null && expect == null) || (cur != null && cur.equals(expect));
                    if (ok) store.put(key, Json.write(b.get("new")));
                    respond(ex, 200, "{\"set\":" + ok + "}");
                    return;
                }
                case "/incr": {
                    String key = k((String) b.get("coll"), (String) b.get("key"));
                    long cur = store.containsKey(key) ? (long) Double.parseDouble(store.get(key)) : 0;
                    cur += ((Number) b.get("delta")).longValue();
                    store.put(key, String.valueOf(cur));
                    respond(ex, 200, "{\"value\":" + cur + "}");
                    return;
                }
                case "/mget": {
                    String coll = (String) b.get("coll");
                    StringBuilder sb = new StringBuilder("{\"values\":[");
                    List<?> keys = (List<?>) b.get("keys");
                    for (int i = 0; i < keys.size(); i++) {
                        if (i > 0) sb.append(',');
                        String val = store.get(k(coll, (String) keys.get(i)));
                        sb.append(val == null ? "null" : val);
                    }
                    sb.append("]}");
                    respond(ex, 200, sb.toString());
                    return;
                }
                case "/scan": {
                    String coll = q.get("coll"), after = q.getOrDefault("after", ""), prefix = q.getOrDefault("prefix", "");
                    int limit = q.containsKey("limit") ? Integer.parseInt(q.get("limit")) : 1000;
                    List<String> keys = new ArrayList<>();
                    for (String kk : store.keySet()) {
                        String[] parts = kk.split("\0", 2);
                        if (!parts[0].equals(coll)) continue;
                        String key = parts[1];
                        if (key.compareTo(after) <= 0) continue;
                        if (!prefix.isEmpty() && !key.startsWith(prefix)) continue;
                        keys.add(key);
                    }
                    Collections.sort(keys);
                    String cursor = "null";
                    if (keys.size() > limit) {
                        keys = keys.subList(0, limit);
                        cursor = "\"" + keys.get(keys.size() - 1) + "\"";
                    }
                    StringBuilder sb = new StringBuilder("{\"keys\":[");
                    for (int i = 0; i < keys.size(); i++) {
                        if (i > 0) sb.append(',');
                        sb.append('"').append(keys.get(i)).append('"');
                    }
                    sb.append("],\"cursor\":").append(cursor).append('}');
                    respond(ex, 200, sb.toString());
                    return;
                }
                case "/members":
                    respond(ex, 200, "{\"members\":[{\"id\":\"s0\",\"endpoint\":\"http://s0:8100\"}]}");
                    return;
                case "/partitions":
                    respond(ex, 200, "{\"partitions\":[{\"id\":\"s0\",\"endpoint\":\"http://s0:8100\",\"buckets\":[0,1,2]}]}");
                    return;
                default:
                    respond(ex, 404, "{}");
            }
        } catch (Exception e) {
            respond(ex, 500, "{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    static String k(String coll, String key) { return coll + "\0" + key; }

    static Map<String, String> parseQuery(String q) {
        Map<String, String> m = new LinkedHashMap<>();
        if (q == null) return m;
        for (String p : q.split("&")) {
            int eq = p.indexOf('=');
            if (eq > 0) m.put(p.substring(0, eq), java.net.URLDecoder.decode(p.substring(eq + 1), StandardCharsets.UTF_8));
        }
        return m;
    }

    static byte[] readAll(InputStream in) throws java.io.IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int n;
        while ((n = in.read(buf)) > 0) out.write(buf, 0, n);
        return out.toByteArray();
    }

    static void respond(HttpExchange ex, int code, String body) {
        try {
            byte[] b = body.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(code, b.length);
            ex.getResponseBody().write(b);
            ex.getResponseBody().close();
        } catch (java.io.IOException ignored) {
        }
    }
}
