// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Bucket-diff reshard planner — Rust port of `cluster/reshard.ts`. Pure: diffs two memberships
//! and emits the minimal per-bucket copies + drops to move from `old` to `new` at replication R.

use std::collections::{HashMap, HashSet};

use crate::partitioner::{NodeId, Partitioner};

pub struct Copy {
    pub bucket: usize,
    pub from: NodeId, // any old owner holds a complete replica
    pub to: NodeId,
}

pub struct Drop {
    pub bucket: usize,
    pub node: NodeId,
}

pub struct ReshardPlan {
    pub copies: Vec<Copy>,
    pub drops: Vec<Drop>,
    pub buckets: usize,
    pub moved_buckets: usize,
}

pub fn plan_reshard(
    old: &[NodeId],
    new: &[NodeId],
    replication: usize,
    topology: &HashMap<NodeId, String>,
) -> ReshardPlan {
    // Same topology on both sides so the plan's target owners match the live overlay's rack-aware
    // routing (else the plan would copy/drop toward plain-HRW owners the overlay never routes to).
    let mut old_p = Partitioner::new(old.to_vec());
    let mut new_p = Partitioner::new(new.to_vec());
    if !topology.is_empty() {
        old_p.set_topology(topology.clone());
        new_p.set_topology(topology.clone());
    }
    let r_old = replication.min(old.len().max(1)).max(1);
    let r_new = replication.min(new.len().max(1)).max(1);
    let buckets = new_p.bucket_count();

    let mut copies = Vec::new();
    let mut drops = Vec::new();
    let mut moved_buckets = 0;

    for b in 0..buckets {
        let old_owners = if old.is_empty() { Vec::new() } else { old_p.owners_of_bucket(b, r_old) };
        let new_owners = new_p.owners_of_bucket(b, r_new);
        let old_set: HashSet<&NodeId> = old_owners.iter().collect();
        let new_set: HashSet<&NodeId> = new_owners.iter().collect();
        let source = old_owners.first().cloned();
        let mut changed = false;

        for to in &new_owners {
            if !old_set.contains(to) {
                if let Some(src) = &source {
                    copies.push(Copy { bucket: b, from: src.clone(), to: to.clone() });
                    changed = true;
                }
            }
        }
        for node in &old_owners {
            if !new_set.contains(node) {
                drops.push(Drop { bucket: b, node: node.clone() });
                changed = true;
            }
        }
        if changed {
            moved_buckets += 1;
        }
    }

    ReshardPlan { copies, drops, buckets, moved_buckets }
}
