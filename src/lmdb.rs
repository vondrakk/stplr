// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Durable embedded store on LMDB (via `heed`) — the shard's "the PVC is the database" backend.
//! Mirrors the TS `store/lmdb.ts` layout: one named sub-database per collection (`o:<coll>`) and
//! per source table (`t:<table>`), plus a `__tables` registry. Values are JSON strings, so the
//! data is wire-identical to the Redis/in-memory backends.
//!
//! Synchronous, like the rest of the shard-local store. Two write-path optimizations (the durable
//! SET path was the benchmark's worst number — 3.5k op/s vs 83k in-memory):
//!   1. Named sub-database handles are CACHED (opened/created once), not re-created per op.
//!   2. The env runs with `MDB_NOSYNC`: `commit()` makes writes immediately visible but does NOT
//!      fsync. We force a sync every `SYNC_EVERY` writes and on drop. A hard crash loses at most
//!      ~that many recent writes (the redis `appendfsync everysec` tradeoff); LMDB's copy-on-write
//!      design means NOSYNC costs durability, not integrity (no corruption).
//! The IndexStore trait is infallible, so disk errors `expect`-panic here (a shard restart recovers).

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use anyhow::Result;
use heed::types::{DecodeIgnore, Str};
use heed::{CompactionOption, Database, Env, EnvFlags, EnvOpenOptions};
use serde_json::Value;

use crate::store::{Capabilities, DataSource, IndexStore, Row};

const TABLES_DB: &str = "__tables";
const EXP_DB: &str = "__expiry"; // key "coll\0id" -> absolute expiry (epoch ms, as a string)
const SYNC_EVERY: u64 = 2048;

pub struct LmdbStore {
    env: Env,
    // Cached named-db handles (heed `Database` is a Copy dbi handle). Interior-mutable so reads
    // (`&self`) can populate the cache too.
    dbs: Mutex<HashMap<String, Database<Str, Str>>>,
    writes: AtomicU64,
    // Fast path guard: when no key has ever had a TTL, every read/write skips the expiry db entirely
    // (so a no-TTL workload — e.g. the benchmark — pays nothing for the feature).
    has_any_ttl: AtomicBool,
}

impl LmdbStore {
    /// Open (creating if needed) an LMDB environment at `path` with a `map_size` byte ceiling.
    pub fn open(path: &Path, map_size: usize) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        // SAFETY: standard single-process open; the mmap is not aliased elsewhere. NO_SYNC defers
        // fsync to `force_sync()` (called on a write threshold + on drop) — see module docs.
        let env = unsafe {
            EnvOpenOptions::new()
                .max_dbs(1024)
                .map_size(map_size)
                .flags(EnvFlags::NO_SYNC)
                .open(path)?
        };
        // Recover the TTL guard: are there any expiry entries on disk?
        let has_any_ttl = {
            let rtxn = env.read_txn()?;
            match env.open_database::<Str, Str>(&rtxn, Some(EXP_DB))? {
                Some(db) => db.first(&rtxn)?.is_some(),
                None => false,
            }
        };
        Ok(Self {
            env,
            dbs: Mutex::new(HashMap::new()),
            writes: AtomicU64::new(0),
            has_any_ttl: AtomicBool::new(has_any_ttl),
        })
    }

    fn obj_db(coll: &str) -> String {
        format!("o:{coll}")
    }
    fn tbl_db(table: &str) -> String {
        format!("t:{table}")
    }
    fn exp_key(coll: &str, id: &str) -> String {
        format!("{coll}\u{0}{id}")
    }

    fn read_expiry(&self, coll: &str, id: &str) -> Option<u64> {
        let db = self.read_db(EXP_DB)?;
        let rtxn = self.env.read_txn().ok()?;
        db.get(&rtxn, &Self::exp_key(coll, id)).ok()?.and_then(|s| s.parse::<u64>().ok())
    }
    fn is_expired(&self, coll: &str, id: &str) -> bool {
        self.has_any_ttl.load(Ordering::Relaxed)
            && self.read_expiry(coll, id).is_some_and(|e| e <= crate::lease::now_epoch_ms())
    }
    /// Drop a key's expiry entry (called on a plain put / delete that clears the TTL).
    fn clear_expiry(&self, coll: &str, id: &str) {
        let db = self.db(EXP_DB);
        let mut wtxn = self.env.write_txn().expect("lmdb write_txn");
        let _ = db.delete(&mut wtxn, &Self::exp_key(coll, id));
        wtxn.commit().expect("lmdb commit");
    }

    /// Cached handle for the named sub-db, creating it on first use. The handle is stable for the
    /// life of the env, so this pays the create cost once instead of per write.
    fn db(&self, name: &str) -> Database<Str, Str> {
        if let Some(&d) = self.dbs.lock().unwrap().get(name) {
            return d;
        }
        let mut wtxn = self.env.write_txn().expect("lmdb write_txn");
        let d: Database<Str, Str> =
            self.env.create_database(&mut wtxn, Some(name)).expect("lmdb create_database");
        wtxn.commit().expect("lmdb commit");
        self.dbs.lock().unwrap().insert(name.to_string(), d);
        d
    }

    /// Cached handle for reads, WITHOUT creating the sub-db (None if it doesn't exist yet).
    /// The dbi is opened inside a COMMITTED (write) txn, not an aborted read txn: LMDB closes a
    /// handle opened in a transaction that aborts, so a read-txn open + abort yields a handle that
    /// reads empty once reused in a later txn. A freshly-written env papers over this, but a
    /// `copy_to_file` backup (restore path) exposes it — the named db is present yet reads as empty.
    /// Committing the open puts the handle in the shared env, valid across all later read txns. This
    /// costs one write txn per collection on its first read (then cached); the hot path is unchanged.
    fn read_db(&self, name: &str) -> Option<Database<Str, Str>> {
        if let Some(&d) = self.dbs.lock().unwrap().get(name) {
            return Some(d);
        }
        let wtxn = self.env.write_txn().ok()?;
        let d = self.env.open_database::<Str, Str>(&wtxn, Some(name)).ok().flatten();
        wtxn.commit().ok()?;
        let d = d?;
        self.dbs.lock().unwrap().insert(name.to_string(), d);
        Some(d)
    }

    /// Periodic durability: fsync once every SYNC_EVERY writes (NO_SYNC skips it per-commit).
    fn maybe_sync(&self) {
        if self.writes.fetch_add(1, Ordering::Relaxed) % SYNC_EVERY == SYNC_EVERY - 1 {
            let _ = self.env.force_sync();
        }
    }

    fn put_str(&self, name: &str, key: &str, val: &str) {
        let db = self.db(name);
        let mut wtxn = self.env.write_txn().expect("lmdb write_txn");
        db.put(&mut wtxn, key, val).expect("lmdb put");
        wtxn.commit().expect("lmdb commit"); // NO_SYNC => no fsync here
        self.maybe_sync();
    }

    fn entries(&self, name: &str) -> Vec<(String, Value)> {
        let Some(db) = self.read_db(name) else {
            return Vec::new();
        };
        let rtxn = match self.env.read_txn() {
            Ok(t) => t,
            Err(_) => return Vec::new(),
        };
        let iter = match db.iter(&rtxn) {
            Ok(i) => i,
            Err(_) => return Vec::new(),
        };
        let mut out = Vec::new();
        for item in iter {
            if let Ok((k, v)) = item {
                if let Ok(val) = serde_json::from_str::<Value>(v) {
                    out.push((k.to_string(), val));
                }
            }
        }
        out
    }

    /// Inspect a snapshot file produced by [`snapshot`](Self::snapshot) WITHOUT mutating it: open it
    /// read-only and count the entries in each named sub-database. Validates that a backup is a
    /// well-formed, restorable LMDB env (and shows what's in it) before you depend on it. All lookups
    /// happen inside ONE read txn (so the cross-txn dbi caveat in `read_db` doesn't apply), and the
    /// env is opened `READ_ONLY | NO_SUB_DIR` — the snapshot file is never written.
    pub fn verify_snapshot(file: &Path) -> Result<Vec<(String, usize)>> {
        // SAFETY: read-only single-file open of an existing snapshot; the mmap is not aliased.
        let env = unsafe {
            EnvOpenOptions::new()
                .max_dbs(1024)
                .flags(EnvFlags::READ_ONLY | EnvFlags::NO_SUB_DIR)
                .open(file)?
        };
        let rtxn = env.read_txn()?;
        let mut out = Vec::new();
        // The main (unnamed) db keys ARE the names of the sub-databases.
        if let Some(main) = env.open_database::<Str, DecodeIgnore>(&rtxn, None)? {
            let names: Vec<String> =
                main.iter(&rtxn)?.filter_map(|kv| kv.ok().map(|(k, _)| k.to_string())).collect();
            for name in names {
                let count = match env.open_database::<Str, Str>(&rtxn, Some(&name))? {
                    Some(db) => db.iter(&rtxn)?.count(),
                    None => 0,
                };
                out.push((name, count));
            }
        }
        out.sort();
        Ok(out)
    }

    /// Seed a source row (fixtures/tests), mirroring MemoryStore::add_row. Registers the table.
    pub fn add_row(&mut self, table: &str, row_id: &str, doc: Value) {
        self.put_str(&Self::tbl_db(table), row_id, &doc.to_string());
        self.put_str(TABLES_DB, table, "1");
    }
}

impl Drop for LmdbStore {
    fn drop(&mut self) {
        // Flush the NO_SYNC tail to disk on clean shutdown.
        let _ = self.env.force_sync();
    }
}

impl DataSource for LmdbStore {
    fn list_tables(&self) -> Vec<String> {
        self.entries(TABLES_DB).into_iter().map(|(k, _)| k).collect()
    }
    fn scan_table(&self, table: &str) -> Vec<Row> {
        self.entries(&Self::tbl_db(table))
            .into_iter()
            .map(|(row_id, doc)| Row { row_id, doc })
            .collect()
    }
    fn get_row(&self, table: &str, row_id: &str) -> Option<Value> {
        let db = self.read_db(&Self::tbl_db(table))?;
        let rtxn = self.env.read_txn().ok()?;
        let s = db.get(&rtxn, row_id).ok()??;
        serde_json::from_str(s).ok()
    }
}

impl IndexStore for LmdbStore {
    fn put_object(&mut self, coll: &str, id: &str, obj: Value) {
        self.put_str(&Self::obj_db(coll), id, &obj.to_string());
        if self.has_any_ttl.load(Ordering::Relaxed) {
            self.clear_expiry(coll, id); // a plain write clears any prior TTL
        }
    }
    /// Group commit: apply the whole batch in ONE write txn (+ one commit, one maybe_sync). With
    /// the per-write txn+commit removed from the hot path, write throughput jumps.
    fn put_batch(&mut self, items: Vec<(String, String, Value)>) {
        if items.is_empty() {
            return;
        }
        // Ensure each collection's db handle exists+cached before opening the batch txn (the
        // create path commits its own txn; we can't nest one inside the batch txn).
        for (coll, _, _) in &items {
            let _ = self.db(&Self::obj_db(coll));
        }
        let ttl = self.has_any_ttl.load(Ordering::Relaxed);
        if ttl {
            let _ = self.db(EXP_DB);
        }
        let mut wtxn = self.env.write_txn().expect("lmdb write_txn");
        for (coll, id, obj) in &items {
            let db = *self.dbs.lock().unwrap().get(&Self::obj_db(coll)).expect("db handle cached");
            db.put(&mut wtxn, id, &obj.to_string()).expect("lmdb put");
            if ttl {
                let edb = *self.dbs.lock().unwrap().get(EXP_DB).expect("exp db cached");
                let _ = edb.delete(&mut wtxn, &Self::exp_key(coll, id)); // batched put clears TTL too
            }
        }
        wtxn.commit().expect("lmdb commit"); // one commit for the whole batch
        self.maybe_sync();
    }
    fn get_object(&self, coll: &str, id: &str) -> Option<Value> {
        if self.is_expired(coll, id) {
            return None;
        }
        let db = self.read_db(&Self::obj_db(coll))?;
        let rtxn = self.env.read_txn().ok()?;
        let s = db.get(&rtxn, id).ok()??;
        serde_json::from_str(s).ok()
    }
    fn scan_objects(&self, coll: &str) -> Vec<Value> {
        self.scan_entries(coll).into_iter().map(|(_, v)| v).collect()
    }
    fn scan_entries(&self, coll: &str) -> Vec<(String, Value)> {
        let entries = self.entries(&Self::obj_db(coll));
        if self.has_any_ttl.load(Ordering::Relaxed) {
            entries.into_iter().filter(|(id, _)| !self.is_expired(coll, id)).collect()
        } else {
            entries
        }
    }

    /// Cursor range scan: seek to the lower bound and walk forward, stopping at `end`/`prefix` or
    /// `limit`. LMDB stores keys sorted, so this never loads the whole collection and breaks early
    /// once it passes the requested range — the win over the trait default.
    fn scan_range(&self, coll: &str, after: Option<&str>, prefix: Option<&str>, end: Option<&str>, limit: usize) -> Vec<String> {
        use std::ops::Bound;
        let Some(db) = self.read_db(&Self::obj_db(coll)) else {
            return Vec::new();
        };
        let rtxn = match self.env.read_txn() {
            Ok(t) => t,
            Err(_) => return Vec::new(),
        };
        // Lower bound = the tighter of the cursor (exclusive) and the prefix start (inclusive).
        let lower = match (after, prefix) {
            (Some(a), Some(p)) if a >= p => Bound::Excluded(a),
            (Some(_), Some(p)) => Bound::Included(p),
            (Some(a), None) => Bound::Excluded(a),
            (None, Some(p)) => Bound::Included(p),
            (None, None) => Bound::Unbounded,
        };
        let range: (Bound<&str>, Bound<&str>) = (lower, Bound::Unbounded);
        let iter = match db.range(&rtxn, &range) {
            Ok(i) => i,
            Err(_) => return Vec::new(),
        };
        let check_ttl = self.has_any_ttl.load(Ordering::Relaxed);
        let mut out = Vec::new();
        for item in iter {
            if out.len() >= limit {
                break;
            }
            let Ok((k, _)) = item else { continue };
            if end.is_some_and(|e| k >= e) {
                break; // sorted: past the end bound, done
            }
            if prefix.is_some_and(|p| !k.starts_with(p)) {
                break; // sorted: past the prefix range, done
            }
            if check_ttl && self.is_expired(coll, k) {
                continue;
            }
            out.push(k.to_string());
        }
        out
    }
    fn clear_collection(&mut self, coll: &str) {
        let Some(db) = self.read_db(&Self::obj_db(coll)) else {
            return;
        };
        let mut wtxn = self.env.write_txn().expect("lmdb write_txn");
        db.clear(&mut wtxn).expect("lmdb clear");
        wtxn.commit().expect("lmdb commit");
        self.maybe_sync();
    }
    fn delete_object(&mut self, coll: &str, id: &str) {
        if let Some(db) = self.read_db(&Self::obj_db(coll)) {
            let mut wtxn = self.env.write_txn().expect("lmdb write_txn");
            db.delete(&mut wtxn, id).expect("lmdb delete");
            wtxn.commit().expect("lmdb commit");
            self.maybe_sync();
        }
        if self.has_any_ttl.load(Ordering::Relaxed) {
            self.clear_expiry(coll, id);
        }
    }
    fn put_object_at(&mut self, coll: &str, id: &str, obj: Value, expire_at_ms: u64) {
        self.put_str(&Self::obj_db(coll), id, &obj.to_string());
        let edb = self.db(EXP_DB);
        let mut wtxn = self.env.write_txn().expect("lmdb write_txn");
        edb.put(&mut wtxn, &Self::exp_key(coll, id), &expire_at_ms.to_string()).expect("lmdb exp put");
        wtxn.commit().expect("lmdb commit");
        self.has_any_ttl.store(true, Ordering::Relaxed);
        self.maybe_sync();
    }
    fn expiry_of(&self, coll: &str, id: &str) -> Option<u64> {
        self.read_expiry(coll, id)
    }
    fn sweep_expired(&mut self, now_ms: u64) -> usize {
        if !self.has_any_ttl.load(Ordering::Relaxed) {
            return 0;
        }
        let Some(edb) = self.read_db(EXP_DB) else {
            return 0;
        };
        // Collect expired (full exp-key, coll, id), then delete under per-key write txns.
        let dead: Vec<(String, String, String)> = self
            .entries(EXP_DB)
            .into_iter()
            .filter_map(|(ekey, v)| {
                let exp = v.as_u64()?;
                if exp > now_ms {
                    return None;
                }
                let (coll, id) = ekey.split_once('\u{0}')?;
                Some((ekey.clone(), coll.to_string(), id.to_string()))
            })
            .collect();
        for (ekey, coll, id) in &dead {
            if let Some(odb) = self.read_db(&Self::obj_db(coll)) {
                let mut wtxn = self.env.write_txn().expect("lmdb write_txn");
                let _ = odb.delete(&mut wtxn, id);
                let _ = edb.delete(&mut wtxn, ekey);
                wtxn.commit().expect("lmdb commit");
            }
        }
        if !dead.is_empty() {
            self.maybe_sync();
        }
        dead.len()
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities { native_set_logic: false }
    }
    /// Consistent hot backup: flush the NO_SYNC tail, then copy the whole env to `dest` as ONE
    /// compacted LMDB file (free pages dropped, so it's smaller than the live env). LMDB's MVCC
    /// gives the copy a stable read-snapshot — no writer quiesce needed for correctness, though
    /// the shard's lock is held for the copy's duration (a brief write-pause; backups are rare).
    /// `dest` is a file path; restore by placing it at `<data-dir>/data.mdb` of a fresh node.
    fn snapshot(&self, dest: &std::path::Path) -> Result<(), String> {
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
        }
        let _ = self.env.force_sync();
        self.env.copy_to_file(dest, CompactionOption::Enabled).map(|_| ()).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn tmp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("stitch-lmdb-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn lmdb_object_and_set_ops_roundtrip() {
        let dir = tmp_dir("kv");
        let mut s = LmdbStore::open(&dir, 64 * 1024 * 1024).unwrap();

        // opaque object store
        s.put_object("c", "k1", json!({"a": 1}));
        assert_eq!(s.get_object("c", "k1"), Some(json!({"a": 1})));
        assert_eq!(s.scan_entries("c").len(), 1);
        s.delete_object("c", "k1");
        assert_eq!(s.get_object("c", "k1"), None);

        // generic posting-list set ops (the default IndexStore impl over the object store)
        assert!(s.set_add("s", "k", "m1"));
        assert!(s.set_add("s", "k", "m2"));
        assert!(!s.set_add("s", "k", "m1"), "members dedup");
        let mut members = s.set_members("s", "k");
        members.sort();
        assert_eq!(members, vec!["m1".to_string(), "m2".to_string()]);
        assert!(s.set_remove("s", "k", "m1"));
        assert_eq!(s.set_members("s", "k"), vec!["m2".to_string()]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn lmdb_data_source_roundtrip() {
        let dir = tmp_dir("src");
        let mut s = LmdbStore::open(&dir, 64 * 1024 * 1024).unwrap();
        s.add_row("orders", "o1", json!({"customerId": "ACME-001"}));
        assert_eq!(s.list_tables(), vec!["orders".to_string()]);
        assert_eq!(s.scan_table("orders").len(), 1);
        assert_eq!(s.get_row("orders", "o1"), Some(json!({"customerId": "ACME-001"})));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn lmdb_snapshot_is_a_restorable_copy() {
        let dir = tmp_dir("snap-src");
        let mut s = LmdbStore::open(&dir, 64 * 1024 * 1024).unwrap();
        s.put_object("c", "k1", json!({"v": 7}));
        s.set_add("s", "k", "m1");

        // Snapshot to a single .mdb file, then restore: a fresh data dir whose data.mdb IS the snap.
        let snap = std::env::temp_dir().join(format!("stitch-lmdb-snap-{}.mdb", std::process::id()));
        let _ = std::fs::remove_file(&snap);
        s.snapshot(&snap).expect("snapshot");
        assert!(snap.exists() && std::fs::metadata(&snap).unwrap().len() > 0, "snapshot non-empty");

        let restore_dir = tmp_dir("snap-dst");
        std::fs::create_dir_all(&restore_dir).unwrap();
        std::fs::copy(&snap, restore_dir.join("data.mdb")).unwrap();
        let r = LmdbStore::open(&restore_dir, 64 * 1024 * 1024).unwrap();
        assert_eq!(r.get_object("c", "k1"), Some(json!({"v": 7})), "object survived snapshot+restore");
        assert_eq!(r.set_members("s", "k"), vec!["m1".to_string()], "set survived snapshot+restore");

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&restore_dir);
        let _ = std::fs::remove_file(&snap);
    }

    #[test]
    fn verify_snapshot_reports_collection_counts() {
        let dir = tmp_dir("verify-src");
        let snap = std::env::temp_dir().join(format!("stitch-lmdb-verify-{}.mdb", std::process::id()));
        let _ = std::fs::remove_file(&snap);
        {
            let mut s = LmdbStore::open(&dir, 64 * 1024 * 1024).unwrap();
            s.put_object("c", "k1", json!({"v": 1}));
            s.put_object("c", "k2", json!({"v": 2}));
            s.set_add("s", "k", "m1"); // creates o:s with one entry
            s.snapshot(&snap).unwrap();
        }
        let stats: std::collections::HashMap<String, usize> =
            LmdbStore::verify_snapshot(&snap).unwrap().into_iter().collect();
        assert_eq!(stats.get("o:c"), Some(&2), "object collection counted");
        assert_eq!(stats.get("o:s"), Some(&1), "set collection counted");
        // a bogus / non-LMDB file must error, not panic.
        let junk = std::env::temp_dir().join(format!("stitch-not-lmdb-{}.mdb", std::process::id()));
        std::fs::write(&junk, b"not an lmdb file").unwrap();
        assert!(LmdbStore::verify_snapshot(&junk).is_err(), "corrupt file rejected");

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_file(&snap);
        let _ = std::fs::remove_file(&junk);
    }

    #[test]
    fn lmdb_ttl_expiry_sweep_and_reopen() {
        let dir = tmp_dir("ttl");
        let now = crate::lease::now_epoch_ms();
        {
            let mut s = LmdbStore::open(&dir, 64 * 1024 * 1024).unwrap();
            s.put_object_at("c", "live", json!(1), now + 100_000); // far future
            s.put_object_at("c", "dead", json!(2), now - 1); // already expired
            assert_eq!(s.get_object("c", "live"), Some(json!(1)));
            assert_eq!(s.get_object("c", "dead"), None, "expired key reads as absent");
            assert!(!s.scan_entries("c").iter().any(|(k, _)| k == "dead"), "scan skips expired");

            // a plain put clears the TTL
            s.put_object_at("c", "temp", json!(3), now - 1);
            s.put_object("c", "temp", json!(4));
            assert_eq!(s.get_object("c", "temp"), Some(json!(4)), "plain put cleared the expiry");
            assert_eq!(s.expiry_of("c", "temp"), None);

            // sweep physically reclaims the expired 'dead' key
            assert!(s.sweep_expired(now) >= 1, "swept >=1 expired");
            assert_eq!(s.expiry_of("c", "dead"), None, "expiry gone after sweep");
        }
        // TTL guard + entries survive reopen
        let s = LmdbStore::open(&dir, 64 * 1024 * 1024).unwrap();
        assert_eq!(s.get_object("c", "live"), Some(json!(1)), "live ttl key survived reopen");
        assert!(s.expiry_of("c", "live").is_some(), "expiry survived reopen");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn lmdb_cas_and_incr() {
        let dir = tmp_dir("cas");
        let mut s = LmdbStore::open(&dir, 64 * 1024 * 1024).unwrap();
        // compare-and-set
        assert!(s.cas("c", "k", None, json!("v1")), "set-if-absent succeeds when absent");
        assert!(!s.cas("c", "k", None, json!("v2")), "set-if-absent fails when present");
        assert!(s.cas("c", "k", Some(json!("v1")), json!("v2")), "cas succeeds on match");
        assert!(!s.cas("c", "k", Some(json!("v1")), json!("v3")), "cas fails on mismatch");
        assert_eq!(s.get_object("c", "k"), Some(json!("v2")));
        // atomic counters
        assert_eq!(s.incr("n", "cnt", 5), Some(5));
        assert_eq!(s.incr("n", "cnt", 3), Some(8));
        assert_eq!(s.incr("n", "cnt", -10), Some(-2));
        // incr on a non-integer value reports failure rather than clobbering
        s.put_object("n", "str", json!("hello"));
        assert_eq!(s.incr("n", "str", 1), None);
        assert_eq!(s.get_object("n", "str"), Some(json!("hello")), "non-int value left intact");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn lmdb_persists_across_reopen() {
        let dir = tmp_dir("persist");
        {
            let mut s = LmdbStore::open(&dir, 64 * 1024 * 1024).unwrap();
            s.put_object("c", "k1", json!({"v": 42}));
            s.set_add("s", "k", "m1");
        } // drop closes the env (and force_syncs the NO_SYNC tail)
        let s = LmdbStore::open(&dir, 64 * 1024 * 1024).unwrap();
        assert_eq!(s.get_object("c", "k1"), Some(json!({"v": 42})), "object survived reopen");
        assert_eq!(s.set_members("s", "k"), vec!["m1".to_string()], "set survived reopen");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
