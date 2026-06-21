// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
package org.stplr;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal, dependency-free JSON parser + writer — just enough for the stplr wire envelopes (Java has
 * no JSON in the stdlib, and the SDK stays dependency-free). Parses into Map/List/String/Double/
 * Boolean/null. Opaque values are passed through verbatim via {@link Raw}.
 */
final class Json {
    private Json() {}

    static Object parse(String s) {
        return new P(s).value();
    }

    static String write(Object o) {
        StringBuilder b = new StringBuilder();
        w(b, o);
        return b.toString();
    }

    /** Wraps an already-serialized JSON string so it is emitted verbatim (opaque stored values). */
    static final class Raw {
        final String json;
        Raw(String json) { this.json = json; }
    }

    // ---- parser ----
    private static final class P {
        final String s;
        int i;
        P(String s) { this.s = s; }

        Object value() {
            ws();
            char c = s.charAt(i);
            switch (c) {
                case '{': return obj();
                case '[': return arr();
                case '"': return str();
                case 't': i += 4; return Boolean.TRUE;
                case 'f': i += 5; return Boolean.FALSE;
                case 'n': i += 4; return null;
                default: return num();
            }
        }

        Map<String, Object> obj() {
            Map<String, Object> m = new LinkedHashMap<>();
            i++; // {
            ws();
            if (s.charAt(i) == '}') { i++; return m; }
            while (true) {
                ws();
                String k = str();
                ws();
                i++; // :
                m.put(k, value());
                ws();
                char c = s.charAt(i++);
                if (c == '}') break; // else ','
            }
            return m;
        }

        List<Object> arr() {
            List<Object> a = new ArrayList<>();
            i++; // [
            ws();
            if (s.charAt(i) == ']') { i++; return a; }
            while (true) {
                a.add(value());
                ws();
                char c = s.charAt(i++);
                if (c == ']') break; // else ','
            }
            return a;
        }

        String str() {
            StringBuilder b = new StringBuilder();
            i++; // opening quote
            while (true) {
                char c = s.charAt(i++);
                if (c == '"') break;
                if (c == '\\') {
                    char e = s.charAt(i++);
                    switch (e) {
                        case 'n': b.append('\n'); break;
                        case 't': b.append('\t'); break;
                        case 'r': b.append('\r'); break;
                        case 'b': b.append('\b'); break;
                        case 'f': b.append('\f'); break;
                        case 'u': b.append((char) Integer.parseInt(s.substring(i, i + 4), 16)); i += 4; break;
                        default: b.append(e); // " \ /
                    }
                } else {
                    b.append(c);
                }
            }
            return b.toString();
        }

        Object num() {
            int st = i;
            while (i < s.length() && "-+.eE0123456789".indexOf(s.charAt(i)) >= 0) i++;
            return Double.parseDouble(s.substring(st, i));
        }

        void ws() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        }
    }

    // ---- writer ----
    private static void w(StringBuilder b, Object o) {
        if (o == null) { b.append("null"); return; }
        if (o instanceof Raw) { b.append(((Raw) o).json); return; }
        if (o instanceof String) { wStr(b, (String) o); return; }
        if (o instanceof Boolean || o instanceof Number) { b.append(o.toString()); return; }
        if (o instanceof Map) {
            b.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : ((Map<?, ?>) o).entrySet()) {
                if (!first) b.append(',');
                first = false;
                wStr(b, e.getKey().toString());
                b.append(':');
                w(b, e.getValue());
            }
            b.append('}');
            return;
        }
        if (o instanceof Iterable) {
            b.append('[');
            boolean first = true;
            for (Object e : (Iterable<?>) o) {
                if (!first) b.append(',');
                first = false;
                w(b, e);
            }
            b.append(']');
            return;
        }
        wStr(b, o.toString());
    }

    private static void wStr(StringBuilder b, String s) {
        b.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\n': b.append("\\n"); break;
                case '\t': b.append("\\t"); break;
                case '\r': b.append("\\r"); break;
                default:
                    if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
                    else b.append(c);
            }
        }
        b.append('"');
    }
}
