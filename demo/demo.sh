#!/usr/bin/env bash
# stplr demo — spins up a real 3-shard durable cluster + coordinator (leader election, durable
# ingest queue, bearer auth) on localhost and walks every headline feature with live curls.
# Self-contained: builds stplrd if needed, uses temp data dirs + ephemeral processes, cleans up.
#
#   ./demo/demo.sh            # build (if needed) + run the walkthrough
#   STPLRD=/path/to/stplrd ./demo/demo.sh
set -uo pipefail

TOKEN="demo-secret"
WORK="$(mktemp -d)"
PIDS=()
COORD="http://127.0.0.1:8080"
S0="http://127.0.0.1:8100"
AUTH=(-H "authorization: Bearer ${TOKEN}" -H "content-type: application/json")

cleanup() { kill "${PIDS[@]}" 2>/dev/null; wait 2>/dev/null; rm -rf "${WORK}"; }
trap cleanup EXIT INT TERM

bold() { printf "\n\033[1;33m== %s ==\033[0m\n" "$*"; }
note() { printf "\033[2m%s\033[0m\n" "$*"; }
# print the command (copy-pasteable: quote args with spaces/JSON/&, redact the token), then run it
c() {
  local shown=() a
  for a in "$@"; do
    case "$a" in
      *" "*|*"{"*|*"&"*|*'"'*) shown+=("'${a}'") ;;
      *) shown+=("${a}") ;;
    esac
  done
  printf "\033[2m\$ %s\033[0m\n" "$(printf '%s ' "${shown[@]}" | sed "s/Bearer ${TOKEN}/Bearer ***/g")"
  "$@"; echo
}

# --- locate or build stplrd ---
BIN="${STPLRD:-}"
if [ -z "${BIN}" ]; then
  TARGET="$(cargo metadata --no-deps --format-version 1 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])' 2>/dev/null || echo target)"
  BIN="${TARGET}/debug/stplrd"
  if [ ! -x "${BIN}" ]; then
    note "building stplrd (first run)…"; cargo build --bin stplrd >&2 || exit 1
  fi
fi
note "using ${BIN}"

bold "1. Start a 3-shard DURABLE cluster + coordinator"
note "shards on :8100-8102 (lmdb), coordinator on :8080 — leader election + durable ingest queue + bearer auth"
for i in 0 1 2; do
  "${BIN}" --store lmdb --path "${WORK}/s${i}" --bind "127.0.0.1:810${i}" --id "s${i}" \
    --auth-token "${TOKEN}" >"${WORK}/s${i}.log" 2>&1 & PIDS+=($!)
done
"${BIN}" --role coordinator --bind 127.0.0.1:8080 \
  --shards s0=127.0.0.1:8100,s1=127.0.0.1:8101,s2=127.0.0.1:8102 \
  --replication 2 --coordinator-id coord-a --ingest-queue "${WORK}/iq" \
  --auth-token "${TOKEN}" >"${WORK}/co.log" 2>&1 & PIDS+=($!)
for _ in $(seq 1 40); do curl -sf "${AUTH[@]}" "${COORD}/health" >/dev/null 2>&1 && break; sleep 0.25; done

bold "2. Cluster membership (id + endpoint + epoch)"
c curl -s "${AUTH[@]}" "${COORD}/members"

bold "3. Write + read an object — replicated to 2 shards, routed by rendezvous hashing"
c curl -s "${AUTH[@]}" -X POST "${COORD}/write" -d '{"coll":"kv","key":"alpha","obj":{"hello":"world"}}'
c curl -s "${AUTH[@]}" "${COORD}/object?coll=kv&key=alpha"
note "which shards own it:"
c curl -s "${AUTH[@]}" "${COORD}/route?key=alpha"

bold "4. Server-side set ops (posting lists, evaluated on the shard)"
c curl -s "${AUTH[@]}" -X POST "${COORD}/setAdd" -d '{"coll":"tags","key":"reptiles","member":"vypr"}'
c curl -s "${AUTH[@]}" -X POST "${COORD}/setAdd" -d '{"coll":"tags","key":"reptiles","member":"boa"}'

bold "5. Per-key TTL (lazy expiry on read + background sweep)"
c curl -s "${AUTH[@]}" -X POST "${COORD}/write" -d '{"coll":"kv","key":"session","obj":"abc","ttlMs":800}'
note "immediately:"; c curl -s "${AUTH[@]}" "${COORD}/object?coll=kv&key=session"
note "after 1s (expired):"; sleep 1; c curl -s "${AUTH[@]}" "${COORD}/object?coll=kv&key=session"

bold "6. Atomic compare-and-set + counters"
note "acquire a lock (set-if-absent):"; c curl -s "${AUTH[@]}" -X POST "${COORD}/cas" -d '{"coll":"kv","key":"lock","new":"held"}'
note "someone else tries (fails):"; c curl -s "${AUTH[@]}" -X POST "${COORD}/cas" -d '{"coll":"kv","key":"lock","new":"stolen"}'
note "atomic counter:"
c curl -s "${AUTH[@]}" -X POST "${COORD}/incr" -d '{"coll":"kv","key":"hits","delta":1}'
c curl -s "${AUTH[@]}" -X POST "${COORD}/incr" -d '{"coll":"kv","key":"hits","delta":41}'

bold "7. Bearer auth is enforced"
note "without a token (expect 401):"
c curl -s -o /dev/null -w 'HTTP %{http_code}\n' "${COORD}/object?coll=kv&key=alpha"
note "with the token (expect 200):"
c curl -s -o /dev/null -w 'HTTP %{http_code}\n' "${AUTH[@]}" "${COORD}/object?coll=kv&key=alpha"

bold "8. Coordinator leader election"
c curl -s "${AUTH[@]}" "${COORD}/leader"

bold "9. Durable ingest queue — accepted durably, applied async, at-least-once"
c curl -s "${AUTH[@]}" -X POST "${COORD}/enqueue" -d '{"coll":"kv","key":"queued","obj":"via-WAL"}'
c curl -s "${AUTH[@]}" "${COORD}/ingest/status"
note "drained to the shards:"; sleep 0.3; c curl -s "${AUTH[@]}" "${COORD}/object?coll=kv&key=queued"

bold "10. Prometheus metrics + cluster gauges"
note "\$ curl -s ${COORD}/metrics | grep -E 'stplr_ops_total|stplr_cluster_'"
curl -s "${AUTH[@]}" "${COORD}/metrics" | grep -E 'stplr_ops_total|stplr_cluster_' | grep -v '#'; echo

bold "11. Hot backup snapshot + verify (against shard s0 directly)"
c curl -s "${AUTH[@]}" -X POST "${S0}/backup?dest=${WORK}/s0.snap"
note "verify the backup is restorable (counts entries, read-only):"
c "${BIN}" --verify-snapshot "${WORK}/s0.snap"

bold "Done — tearing down the cluster"
note "stplr.org · one self-managing binary · durable, distributed, zero-touch"
