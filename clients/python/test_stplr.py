# SPDX-License-Identifier: BUSL-1.1
# Copyright (c) 2026 The Von Drakk Corporation
"""Self-contained tests: an http.server mock emulates the stplr JSON contract over an in-memory
dict; the client drives it. No network, no third-party deps. Run: python3 -m unittest -v"""
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from stplr import Client, StplrError

TOKEN = "testtoken"
STORE: dict = {}  # (coll, key) -> value


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):  # silence
        pass

    def _auth(self) -> bool:
        if self.headers.get("Authorization") != "Bearer " + TOKEN:
            self._send(401, {})
            return False
        return True

    def _send(self, code: int, obj) -> None:
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _body(self) -> dict:
        n = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(n) or b"{}")

    def do_GET(self):
        if not self._auth():
            return
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        if u.path == "/object":
            self._send(200, {"object": STORE.get((q["coll"], q["key"]))})
        elif u.path == "/scan":
            coll, after, prefix = q["coll"], q.get("after", ""), q.get("prefix", "")
            limit = int(q.get("limit", "1000"))
            keys = sorted(k for (c, k) in STORE if c == coll and k > after and k.startswith(prefix))
            cursor = None
            if len(keys) > limit:
                keys = keys[:limit]
                cursor = keys[-1]
            self._send(200, {"keys": keys, "cursor": cursor})
        elif u.path == "/members":
            self._send(200, {"members": [{"id": "s0", "endpoint": "http://s0:8100"}]})
        elif u.path == "/partitions":
            self._send(200, {"partitions": [{"id": "s0", "endpoint": "http://s0:8100", "buckets": [0, 1, 2]}]})
        else:
            self._send(404, {})

    def do_POST(self):
        if not self._auth():
            return
        u = urlparse(self.path)
        b = self._body()
        key = (b.get("coll"), b.get("key"))
        if u.path == "/write":
            STORE[key] = b["obj"]
            self._send(200, {"ok": True})
        elif u.path == "/deleteObject":
            STORE.pop(key, None)
            self._send(200, {"ok": True})
        elif u.path == "/cas":
            has = key in STORE
            ok = (not has) if "expect" not in b else (has and STORE[key] == b["expect"])
            if ok:
                STORE[key] = b["new"]
            self._send(200, {"set": ok})
        elif u.path == "/incr":
            STORE[key] = STORE.get(key, 0) + b["delta"]
            self._send(200, {"value": STORE[key]})
        elif u.path == "/setAdd":
            self._send(200, {"added": True})
        elif u.path == "/mget":
            coll = b["coll"]
            self._send(200, {"values": [STORE.get((coll, k)) for k in b["keys"]]})
        else:
            self._send(404, {})


class StplrSDKTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        cls.port = cls.srv.server_address[1]
        cls.thread = threading.Thread(target=cls.srv.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.port}"

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def setUp(self):
        STORE.clear()
        self.c = Client(self.base, token=TOKEN)

    def test_round_trips(self):
        self.c.set("kv", "a", {"n": 1})
        self.assertEqual(self.c.get("kv", "a"), {"n": 1})
        self.assertIsNone(self.c.get("kv", "missing"))

        self.assertEqual(self.c.incr("kv", "ctr", 5), 5)
        self.assertEqual(self.c.incr("kv", "ctr", 3), 8)

        self.assertTrue(self.c.cas("kv", "lock", "held"))  # set-if-absent
        self.assertFalse(self.c.cas("kv", "lock", "stolen"))  # already present
        self.assertTrue(self.c.cas("kv", "lock", "next", expect="held"))  # matches current

        for k in ("m1", "m2"):
            self.c.set("kv", k, k)
        self.assertEqual(self.c.mget("kv", ["m2", "nope", "m1"]), ["m2", None, "m1"])

        self.c.delete("kv", "a")
        self.assertIsNone(self.c.get("kv", "a"))

        self.assertEqual(self.c.members()[0]["id"], "s0")
        self.assertEqual(len(self.c.partitions()[0]["buckets"]), 3)

    def test_scan_pagination(self):
        for i in range(25):
            self.c.set("kv", f"user:{100 + i}", i)
        self.c.set("kv", "zzz", 1)

        allk = list(self.c.scan_all("kv", prefix="user:"))
        self.assertEqual(len(allk), 25)
        self.assertEqual(allk, sorted(allk))

        seen, after = set(), None
        while True:
            page, cursor = self.c.scan("kv", after=after, prefix="user:", limit=7)
            for k in page:
                self.assertNotIn(k, seen)
                seen.add(k)
            if not cursor:
                break
            after = cursor
        self.assertEqual(len(seen), 25)

    def test_auth_rejected(self):
        bad = Client(self.base, token="wrong")
        with self.assertRaises(StplrError):
            bad.set("kv", "x", 1)


if __name__ == "__main__":
    unittest.main()
