# SPDX-License-Identifier: BUSL-1.1
# Copyright (c) 2026 The Von Drakk Corporation
"""Python client for stplr — a self-managing, horizontally-scaling distributed key/value + set-ops
store. Speaks the coordinator's HTTP API; dependency-free (stdlib only). Values are native Python
objects (dicts/lists/str/int/...), JSON-encoded on the wire.

    from stplr import Client
    c = Client("https://coord:8080", token="secret")
    c.set("kv", "alpha", {"hello": "world"})
    print(c.get("kv", "alpha"))        # {'hello': 'world'}  (None if absent)
"""
from __future__ import annotations

import json
import ssl
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Optional

__all__ = ["Client", "StplrError", "ABSENT"]

# Sentinel for cas(): expect=ABSENT means "set only if the key is absent" (omit the expect field).
ABSENT = object()


class StplrError(Exception):
    """A non-2xx response from stplr."""


class Client:
    def __init__(
        self,
        base_url: str,
        token: Optional[str] = None,
        ca_cert: Optional[str] = None,
        insecure: bool = False,
        timeout: float = 15.0,
    ):
        self.base = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        if insecure:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            self._ctx: Optional[ssl.SSLContext] = ctx
        elif ca_cert:
            self._ctx = ssl.create_default_context(cadata=ca_cert)
        else:
            self._ctx = None

    # ---- core operations ----

    def get(self, coll: str, key: str) -> Any:
        """The value at ``key`` (decoded), or ``None`` if absent."""
        r = self._req("GET", "/object?" + urllib.parse.urlencode({"coll": coll, "key": key}))
        return r.get("object")

    def set(self, coll: str, key: str, value: Any) -> None:
        self._req("POST", "/write", {"coll": coll, "key": key, "obj": value})

    def set_ttl(self, coll: str, key: str, value: Any, ttl_ms: int) -> None:
        """Write with a time-to-live (ms from now; 0 = no expiry)."""
        self._req("POST", "/write", {"coll": coll, "key": key, "obj": value, "ttlMs": ttl_ms})

    def delete(self, coll: str, key: str) -> None:
        self._req("POST", "/deleteObject", {"coll": coll, "key": key})

    def cas(self, coll: str, key: str, new: Any, expect: Any = ABSENT) -> bool:
        """Atomic compare-and-set. With ``expect=ABSENT`` (default) sets only if the key is absent;
        otherwise swaps only if the current value equals ``expect``. Returns whether it swapped."""
        body = {"coll": coll, "key": key, "new": new}
        if expect is not ABSENT:
            body["expect"] = expect
        return bool(self._req("POST", "/cas", body).get("set", False))

    def incr(self, coll: str, key: str, delta: int) -> int:
        """Atomically add ``delta`` to the integer at ``key``; returns the new value."""
        return int(self._req("POST", "/incr", {"coll": coll, "key": key, "delta": delta})["value"])

    def set_add(self, coll: str, key: str, member: str) -> bool:
        return bool(self._req("POST", "/setAdd", {"coll": coll, "key": key, "member": member}).get("added", False))

    def set_remove(self, coll: str, key: str, member: str) -> bool:
        return bool(self._req("POST", "/setRemove", {"coll": coll, "key": key, "member": member}).get("removed", False))

    def mget(self, coll: str, keys: list[str]) -> list[Any]:
        """Batch get; result[i] is the value for keys[i] (``None`` if absent)."""
        return self._req("POST", "/mget", {"coll": coll, "keys": keys}).get("values", [])

    # ---- iteration ----

    def scan(
        self,
        coll: str,
        after: Optional[str] = None,
        prefix: Optional[str] = None,
        end: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> tuple[list[str], Optional[str]]:
        """A page of ascending keys plus a cursor (``None`` when drained). Bounds: ``after`` (cursor,
        exclusive), ``prefix``, ``end`` (exclusive). Pass the cursor back as ``after`` for the next page."""
        params = {"coll": coll}
        if after:
            params["after"] = after
        if prefix:
            params["prefix"] = prefix
        if end:
            params["end"] = end
        if limit:
            params["limit"] = str(limit)
        r = self._req("GET", "/scan?" + urllib.parse.urlencode(params))
        return r.get("keys", []), r.get("cursor")

    def scan_all(self, coll: str, prefix: Optional[str] = None):
        """Yield every key in ``coll`` (optionally prefix-filtered), paging until drained."""
        after = None
        while True:
            keys, cursor = self.scan(coll, after=after, prefix=prefix, limit=1000)
            yield from keys
            if not cursor:
                return
            after = cursor

    # ---- topology ----

    def members(self) -> list[dict]:
        """Cluster shards: ``[{'id', 'endpoint'}, ...]``."""
        return self._req("GET", "/members").get("members", [])

    def partitions(self) -> list[dict]:
        """Partition plan for parallel reads: ``[{'id', 'endpoint', 'buckets'}, ...]`` (tiles once)."""
        return self._req("GET", "/partitions").get("partitions", [])

    # ---- transport ----

    def _req(self, method: str, path: str, body: Any = None) -> dict:
        data = None
        headers = {}
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        req = urllib.request.Request(self.base + path, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout, context=self._ctx) as resp:
                return json.loads(resp.read().decode() or "{}")
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace").strip()
            raise StplrError(f"{method} {path} -> {e.code}: {detail}") from None
