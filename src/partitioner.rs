// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Rendezvous (HRW) hashing over virtual buckets — Rust port of `cluster/partitioner.ts`.
//! Hash functions are bit-identical to the TS (FNV-1a over UTF-16 units + the same HRW mix), so
//! a key maps to the same bucket/owners as the TS cluster.
//!
//! Optional **rack/zone-aware replication** (Cassandra NetworkTopologyStrategy style): given a
//! node→rack map, the R replicas of a key are chosen down the HRW preference list while preferring
//! distinct racks, so a single rack/AZ loss can't take out every copy. Best-effort: if there are
//! fewer racks than R, the remaining slots fill in HRW order. With NO topology set, selection is the
//! plain HRW top-R — unchanged and bit-identical to the TS cluster.

use std::collections::{HashMap, HashSet};

pub type NodeId = String;

const NUM_BUCKETS: usize = 4096;

fn fnv1a(s: &str) -> u32 {
    let mut h: u32 = 0x811c9dc5;
    for u in s.encode_utf16() {
        h ^= u as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

/// Which bucket a key id falls in. Must match the Partitioner's bucket count.
pub fn bucket_of(key: &str) -> usize {
    (fnv1a(key) % NUM_BUCKETS as u32) as usize
}

/// HRW weight for assigning a bucket to a node.
fn bucket_score(node_hash: u32, bucket: usize) -> u32 {
    let mut h = node_hash ^ (bucket as u32).wrapping_add(1).wrapping_mul(0x9e37_79b1);
    h = (h ^ (h >> 15)).wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h
}

pub struct Partitioner {
    nodes: Vec<NodeId>,
    buckets: Vec<Vec<NodeId>>, // per bucket: nodes ranked by HRW score, desc
    num_buckets: usize,
    topology: HashMap<NodeId, String>, // node -> rack/zone; empty = no topology (plain HRW top-R)
}

impl Partitioner {
    pub fn new(nodes: Vec<NodeId>) -> Self {
        let mut p = Partitioner {
            nodes: Vec::new(),
            buckets: Vec::new(),
            num_buckets: NUM_BUCKETS,
            topology: HashMap::new(),
        };
        p.set_nodes(nodes);
        p
    }

    /// Set the node→rack map for rack-aware replica placement (empty clears it → plain HRW).
    /// Preserved across `set_nodes`; only the entries for current nodes matter.
    pub fn set_topology(&mut self, topology: HashMap<NodeId, String>) {
        self.topology = topology;
    }

    /// Walk a bucket's HRW-ranked preference list and pick R owners, preferring distinct racks when
    /// a topology is set (best-effort: fill in HRW order once every rack is represented). With no
    /// topology this is exactly `pl[..R]` — the plain HRW top-R.
    fn pick(&self, pl: &[NodeId], r: usize) -> Vec<NodeId> {
        let r = r.min(self.nodes.len());
        if self.topology.is_empty() {
            return pl[..r].to_vec();
        }
        let mut chosen: Vec<NodeId> = Vec::with_capacity(r);
        let mut used: HashSet<&str> = HashSet::new();
        // pass 1: first node per rack, in HRW order (a node with no rack = its own domain via its id)
        for n in pl {
            if chosen.len() >= r {
                break;
            }
            let domain = self.topology.get(n).map(String::as_str).unwrap_or(n.as_str());
            if used.insert(domain) {
                chosen.push(n.clone());
            }
        }
        // pass 2: more replicas than racks → fill the rest in HRW order
        if chosen.len() < r {
            for n in pl {
                if chosen.len() >= r {
                    break;
                }
                if !chosen.iter().any(|c| c == n) {
                    chosen.push(n.clone());
                }
            }
        }
        chosen
    }

    pub fn set_nodes(&mut self, nodes: Vec<NodeId>) {
        self.nodes = nodes;
        let hashes: Vec<u32> = self.nodes.iter().map(|n| fnv1a(n)).collect();
        self.buckets = (0..self.num_buckets)
            .map(|b| {
                let mut ranked: Vec<(usize, u32)> =
                    (0..self.nodes.len()).map(|i| (i, bucket_score(hashes[i], b))).collect();
                // stable sort desc → ties preserve node insertion order (matches V8's stable sort)
                ranked.sort_by(|a, c| c.1.cmp(&a.1));
                ranked.into_iter().map(|(i, _)| self.nodes[i].clone()).collect()
            })
            .collect();
    }

    pub fn list(&self) -> Vec<NodeId> {
        self.nodes.clone()
    }

    pub fn bucket_count(&self) -> usize {
        self.num_buckets
    }

    pub fn preference_list(&self, key: &str) -> &[NodeId] {
        &self.buckets[bucket_of(key)]
    }

    /// The R nodes that hold this key (primary + replicas), rack-aware when a topology is set.
    pub fn replica_set(&self, key: &str, r: usize) -> Vec<NodeId> {
        self.pick(self.preference_list(key), r)
    }

    /// The R owners of a bucket index directly (no key needed) — used by the reshard planner.
    /// Rack-aware when a topology is set, so rebalancing also spreads replicas across racks.
    pub fn owners_of_bucket(&self, bucket: usize, r: usize) -> Vec<NodeId> {
        self.pick(&self.buckets[bucket], r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes(n: &[&str]) -> Vec<NodeId> {
        n.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn no_topology_is_plain_hrw_top_r() {
        // Without a topology, replica_set must be exactly the HRW preference-list prefix —
        // the bit-identical-to-TS behavior must not regress.
        let p = Partitioner::new(nodes(&["a", "b", "c", "d"]));
        for key in ["alpha", "beta", "gamma", "k-42", "x"] {
            let pl = p.preference_list(key);
            assert_eq!(p.replica_set(key, 2), pl[..2].to_vec());
            assert_eq!(p.replica_set(key, 3), pl[..3].to_vec());
            assert_eq!(p.owners_of_bucket(bucket_of(key), 2), pl[..2].to_vec());
        }
    }

    #[test]
    fn rack_aware_spreads_replicas_across_racks() {
        // 4 nodes in 2 racks; R=2 must land one replica in each rack for every key.
        let mut p = Partitioner::new(nodes(&["n0", "n1", "n2", "n3"]));
        let topo: HashMap<NodeId, String> = [("n0", "a"), ("n1", "a"), ("n2", "b"), ("n3", "b")]
            .iter()
            .map(|(n, r)| (n.to_string(), r.to_string()))
            .collect();
        p.set_topology(topo.clone());

        for key in ["alpha", "beta", "gamma", "delta", "k-7", "k-99", "zzz"] {
            let rs = p.replica_set(key, 2);
            assert_eq!(rs.len(), 2, "two replicas for {key}");
            let racks: HashSet<&str> = rs.iter().map(|n| topo[n].as_str()).collect();
            assert_eq!(racks.len(), 2, "replicas of {key} span distinct racks: {rs:?}");
            // The primary is still the HRW winner — rack-awareness only reorders the replicas.
            assert_eq!(rs[0], p.preference_list(key)[0], "primary unchanged for {key}");
        }
    }

    #[test]
    fn best_effort_when_more_replicas_than_racks() {
        // R=3 over only 2 racks: still return 3 distinct nodes, covering both racks.
        let mut p = Partitioner::new(nodes(&["n0", "n1", "n2", "n3"]));
        let topo: HashMap<NodeId, String> = [("n0", "a"), ("n1", "a"), ("n2", "b"), ("n3", "b")]
            .iter()
            .map(|(n, r)| (n.to_string(), r.to_string()))
            .collect();
        p.set_topology(topo.clone());

        for key in ["alpha", "beta", "gamma", "k-1", "k-2"] {
            let rs = p.replica_set(key, 3);
            assert_eq!(rs.len(), 3, "three replicas for {key}");
            let distinct: HashSet<&NodeId> = rs.iter().collect();
            assert_eq!(distinct.len(), 3, "no duplicate replica for {key}: {rs:?}");
            let racks: HashSet<&str> = rs.iter().map(|n| topo[n].as_str()).collect();
            assert_eq!(racks.len(), 2, "both racks represented for {key}");
        }
    }

    #[test]
    fn topology_survives_set_nodes() {
        let mut p = Partitioner::new(nodes(&["n0", "n2"]));
        let topo: HashMap<NodeId, String> = [("n0", "a"), ("n1", "a"), ("n2", "b"), ("n3", "b")]
            .iter()
            .map(|(n, r)| (n.to_string(), r.to_string()))
            .collect();
        p.set_topology(topo.clone());
        // Membership grows; the topology map (covering the new nodes) must still apply.
        p.set_nodes(nodes(&["n0", "n1", "n2", "n3"]));
        let rs = p.replica_set("alpha", 2);
        let racks: HashSet<&str> = rs.iter().map(|n| topo[n].as_str()).collect();
        assert_eq!(racks.len(), 2, "rack-aware placement holds after set_nodes: {rs:?}");
    }
}
