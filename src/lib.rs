// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! stplr — a self-managing, horizontally-scaling distributed key/value + set-ops store.
//!
//! Rendezvous-hashed partitioning over virtual buckets, top-R replication, and zero-touch
//! operations: online rebalance, drain, auto-heal, crash-resume, and live membership. The data
//! plane is a generic object store plus server-side posting-list set operations, with a replayable
//! change feed. Nodes compose roles (shard / coordinator / api) and run as one binary.
//!
//! This crate is the open substrate. It knows keys, collections, opaque `Value` objects, opaque
//! set members, and buckets — and nothing about what is stored in them. Any correlation or
//! query semantics (e.g. the proprietary Stitch engine) layer ON TOP of this crate and never leak
//! back down; nothing here depends on them.
//!
//! Licensed under the Business Source License 1.1 (see LICENSE).

#![allow(dead_code)]

pub mod changefeed;
pub mod client;
pub mod cluster;
pub mod config;
pub mod coord;
pub mod guard;
pub mod ingest;
pub mod journal;
pub mod lease;
pub mod lmdb;
pub mod metrics;
pub mod net;
pub mod partitioner;
pub mod proto;
pub mod reshard;
pub mod shard;
pub mod smart;
pub mod store;
pub mod tls;
