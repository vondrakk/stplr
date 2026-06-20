// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Migration journal — records progress so a crashed rebalance can resume. Port of the core of
//! `cluster/journal.ts`. Only progress (the `done` buckets) is stored; the plan is recomputed
//! deterministically from (old, new, R). The durable ShardJournal-on-PVC variant lands with the
//! async transport layer; for now the in-memory journal exercises the resume algorithm.

use std::sync::Mutex;

use crate::partitioner::NodeId;

#[derive(Clone)]
pub struct MigrationRecord {
    pub old_nodes: Vec<NodeId>,
    pub new_nodes: Vec<NodeId>,
    pub replication: usize,
    pub done: Vec<usize>, // buckets fully migrated (copied + dropped)
}

/// Where the coordinator records migration progress. Interior mutability (&self) so the
/// coordinator can checkpoint while migrating; Send + Sync so it lives in an async coordinator.
pub trait MigrationJournal: Send + Sync {
    fn load(&self) -> Option<MigrationRecord>;
    fn save(&self, rec: &MigrationRecord);
    fn clear(&self);
}

#[derive(Default)]
pub struct InMemoryJournal {
    rec: Mutex<Option<MigrationRecord>>,
}

impl InMemoryJournal {
    pub fn new() -> Self {
        Self::default()
    }
}

impl MigrationJournal for InMemoryJournal {
    fn load(&self) -> Option<MigrationRecord> {
        self.rec.lock().unwrap().clone()
    }
    fn save(&self, rec: &MigrationRecord) {
        *self.rec.lock().unwrap() = Some(rec.clone());
    }
    fn clear(&self) {
        *self.rec.lock().unwrap() = None;
    }
}
