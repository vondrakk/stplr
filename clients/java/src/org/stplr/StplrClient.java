// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
package org.stplr;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Java client for stplr — a self-managing, horizontally-scaling distributed key/value + set-ops
 * store. Speaks the coordinator's HTTP API; dependency-free (JDK only). Stored values are opaque
 * JSON, passed and returned as JSON strings (bind them with your own JSON library).
 *
 * <pre>{@code
 * StplrClient c = StplrClient.builder("https://coord:8080").token("secret").build();
 * c.set("kv", "alpha", "{\"hello\":\"world\"}");
 * Optional<String> v = c.get("kv", "alpha");
 * }</pre>
 */
public final class StplrClient {
    private final String base;
    private final HttpClient http;
    private final String token;

    private StplrClient(String base, HttpClient http, String token) {
        this.base = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
        this.http = http;
        this.token = token;
    }

    public static Builder builder(String baseUrl) {
        return new Builder(baseUrl);
    }

    public static final class Builder {
        private final String baseUrl;
        private String token;
        private SSLContext sslContext;
        private Duration timeout = Duration.ofSeconds(15);

        Builder(String baseUrl) { this.baseUrl = baseUrl; }

        public Builder token(String token) { this.token = token; return this; }
        public Builder sslContext(SSLContext ctx) { this.sslContext = ctx; return this; }
        public Builder timeout(Duration d) { this.timeout = d; return this; }
        /** DEV ONLY: trust any TLS certificate. */
        public Builder insecureSkipVerify() { this.sslContext = trustAllContext(); return this; }

        public StplrClient build() {
            HttpClient.Builder b = HttpClient.newBuilder().connectTimeout(timeout);
            if (sslContext != null) b.sslContext(sslContext);
            return new StplrClient(baseUrl, b.build(), token);
        }
    }

    // ---- model ----
    public record Member(String id, String endpoint) {}
    public record Partition(String id, String endpoint, List<Integer> buckets) {}
    public record ScanPage(List<String> keys, String cursor) {}

    // ---- key/value ----

    /** The raw JSON value at key, or empty if absent. */
    public Optional<String> get(String coll, String key) throws IOException, InterruptedException {
        Map<?, ?> r = (Map<?, ?>) send("GET", "/object?coll=" + enc(coll) + "&key=" + enc(key), null);
        Object o = r.get("object");
        return o == null ? Optional.empty() : Optional.of(Json.write(o));
    }

    /** Write a raw JSON value at key. */
    public void set(String coll, String key, String jsonValue) throws IOException, InterruptedException {
        send("POST", "/write", body("coll", coll, "key", key, "obj", new Json.Raw(jsonValue)));
    }

    /** Write a raw JSON value with a time-to-live (millis from now; 0 = no expiry). */
    public void setTtl(String coll, String key, String jsonValue, long ttlMillis) throws IOException, InterruptedException {
        Map<String, Object> b = body("coll", coll, "key", key, "obj", new Json.Raw(jsonValue));
        b.put("ttlMs", ttlMillis);
        send("POST", "/write", b);
    }

    public void delete(String coll, String key) throws IOException, InterruptedException {
        send("POST", "/deleteObject", body("coll", coll, "key", key));
    }

    /** Atomic compare-and-set; pass null expect to require the key be absent. Returns whether it swapped. */
    public boolean cas(String coll, String key, String expectJson, String newJson) throws IOException, InterruptedException {
        Map<String, Object> b = body("coll", coll, "key", key, "new", new Json.Raw(newJson));
        if (expectJson != null) b.put("expect", new Json.Raw(expectJson));
        Map<?, ?> r = (Map<?, ?>) send("POST", "/cas", b);
        return Boolean.TRUE.equals(r.get("set"));
    }

    /** Atomically add delta to the integer at key; returns the new value. */
    public long incr(String coll, String key, long delta) throws IOException, InterruptedException {
        Map<?, ?> r = (Map<?, ?>) send("POST", "/incr", body("coll", coll, "key", key, "delta", delta));
        return ((Number) r.get("value")).longValue();
    }

    public boolean setAdd(String coll, String key, String member) throws IOException, InterruptedException {
        Map<?, ?> r = (Map<?, ?>) send("POST", "/setAdd", body("coll", coll, "key", key, "member", member));
        return Boolean.TRUE.equals(r.get("added"));
    }

    public boolean setRemove(String coll, String key, String member) throws IOException, InterruptedException {
        Map<?, ?> r = (Map<?, ?>) send("POST", "/setRemove", body("coll", coll, "key", key, "member", member));
        return Boolean.TRUE.equals(r.get("removed"));
    }

    /** Batch get: result.get(i) is the raw JSON value for keys.get(i), or null if absent. */
    public List<String> mget(String coll, List<String> keys) throws IOException, InterruptedException {
        Map<?, ?> r = (Map<?, ?>) send("POST", "/mget", body("coll", coll, "keys", keys));
        List<?> vals = (List<?>) r.get("values");
        List<String> out = new ArrayList<>(vals.size());
        for (Object v : vals) out.add(v == null ? null : Json.write(v));
        return out;
    }

    // ---- iteration ----

    /** A page of ascending keys + a cursor (null when drained). Bounds: after/prefix/end (any may be null), limit (0 = default). */
    public ScanPage scan(String coll, String after, String prefix, String end, int limit) throws IOException, InterruptedException {
        StringBuilder q = new StringBuilder("/scan?coll=").append(enc(coll));
        if (after != null && !after.isEmpty()) q.append("&after=").append(enc(after));
        if (prefix != null && !prefix.isEmpty()) q.append("&prefix=").append(enc(prefix));
        if (end != null && !end.isEmpty()) q.append("&end=").append(enc(end));
        if (limit > 0) q.append("&limit=").append(limit);
        Map<?, ?> r = (Map<?, ?>) send("GET", q.toString(), null);
        List<String> keys = new ArrayList<>();
        for (Object k : (List<?>) r.get("keys")) keys.add((String) k);
        Object cur = r.get("cursor");
        return new ScanPage(keys, cur == null ? null : (String) cur);
    }

    /** Walk every key in coll (optionally prefix-filtered), paging until drained. */
    public List<String> scanAll(String coll, String prefix) throws IOException, InterruptedException {
        List<String> all = new ArrayList<>();
        String after = "";
        while (true) {
            ScanPage p = scan(coll, after, prefix, null, 1000);
            all.addAll(p.keys());
            if (p.cursor() == null) return all;
            after = p.cursor();
        }
    }

    // ---- topology ----

    public List<Member> members() throws IOException, InterruptedException {
        Map<?, ?> r = (Map<?, ?>) send("GET", "/members", null);
        List<Member> out = new ArrayList<>();
        for (Object o : (List<?>) r.get("members")) {
            Map<?, ?> m = (Map<?, ?>) o;
            out.add(new Member((String) m.get("id"), (String) m.get("endpoint")));
        }
        return out;
    }

    public List<Partition> partitions() throws IOException, InterruptedException {
        Map<?, ?> r = (Map<?, ?>) send("GET", "/partitions", null);
        List<Partition> out = new ArrayList<>();
        for (Object o : (List<?>) r.get("partitions")) {
            Map<?, ?> m = (Map<?, ?>) o;
            List<Integer> buckets = new ArrayList<>();
            for (Object b : (List<?>) m.get("buckets")) buckets.add(((Number) b).intValue());
            out.add(new Partition((String) m.get("id"), (String) m.get("endpoint"), buckets));
        }
        return out;
    }

    // ---- transport ----

    private Object send(String method, String path, Map<String, Object> body) throws IOException, InterruptedException {
        HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(base + path));
        if (token != null) rb.header("Authorization", "Bearer " + token);
        if (body != null) {
            rb.header("Content-Type", "application/json");
            rb.method(method, HttpRequest.BodyPublishers.ofString(Json.write(body)));
        } else {
            rb.method(method, HttpRequest.BodyPublishers.noBody());
        }
        HttpResponse<String> resp = http.send(rb.build(), HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new IOException("stplr: " + method + " " + path + " -> " + resp.statusCode() + ": " + resp.body());
        }
        return Json.parse(resp.body());
    }

    private static Map<String, Object> body(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static SSLContext trustAllContext() {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, new TrustManager[] {new X509TrustManager() {
                public void checkClientTrusted(java.security.cert.X509Certificate[] c, String a) {}
                public void checkServerTrusted(java.security.cert.X509Certificate[] c, String a) {}
                public java.security.cert.X509Certificate[] getAcceptedIssuers() { return new java.security.cert.X509Certificate[0]; }
            }}, new java.security.SecureRandom());
            return ctx;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
