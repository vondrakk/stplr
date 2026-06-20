// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Process metrics in Prometheus exposition format, scraped at `/metrics` (shard + coordinator).
//!
//! Deliberately dependency-free and lock-free: a handful of relaxed atomic counters bumped on the
//! hot path (negligible cost) and rendered on demand. Both the HTTP and binary data paths bump the
//! same counters, so a shard's totals are protocol-agnostic.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

pub static GET: AtomicU64 = AtomicU64::new(0);
pub static SET: AtomicU64 = AtomicU64::new(0);
pub static SADD: AtomicU64 = AtomicU64::new(0);
pub static SREM: AtomicU64 = AtomicU64::new(0);
pub static DEL: AtomicU64 = AtomicU64::new(0);

fn start() -> &'static Instant {
    static S: OnceLock<Instant> = OnceLock::new();
    S.get_or_init(Instant::now)
}

/// Pin the uptime clock at process start (call once at boot).
pub fn init() {
    let _ = start();
}

#[inline]
pub fn inc(c: &AtomicU64) {
    c.fetch_add(1, Ordering::Relaxed);
}

/// Render the current metrics in Prometheus text exposition format.
pub fn render() -> String {
    let g = GET.load(Ordering::Relaxed);
    let s = SET.load(Ordering::Relaxed);
    let sa = SADD.load(Ordering::Relaxed);
    let sr = SREM.load(Ordering::Relaxed);
    let d = DEL.load(Ordering::Relaxed);
    let total = g + s + sa + sr + d;
    let up = start().elapsed().as_secs_f64();
    format!(
        "# HELP stplr_ops_total Operations served, by type.\n\
         # TYPE stplr_ops_total counter\n\
         stplr_ops_total{{op=\"get\"}} {g}\n\
         stplr_ops_total{{op=\"set\"}} {s}\n\
         stplr_ops_total{{op=\"sadd\"}} {sa}\n\
         stplr_ops_total{{op=\"srem\"}} {sr}\n\
         stplr_ops_total{{op=\"del\"}} {d}\n\
         # HELP stplr_ops_grand_total All operations served.\n\
         # TYPE stplr_ops_grand_total counter\n\
         stplr_ops_grand_total {total}\n\
         # HELP stplr_uptime_seconds Process uptime in seconds.\n\
         # TYPE stplr_uptime_seconds gauge\n\
         stplr_uptime_seconds {up:.3}\n"
    )
}

/// Render coordinator-level cluster gauges (membership / replication / migration). A shard has no
/// cluster view, so only the coordinator's `/metrics` appends these (after [`render`]).
pub fn render_cluster_gauges(shards: usize, replication: usize, migrating: bool) -> String {
    format!(
        "# HELP stplr_cluster_shards Number of shards the coordinator routes across.\n\
         # TYPE stplr_cluster_shards gauge\n\
         stplr_cluster_shards {shards}\n\
         # HELP stplr_cluster_replication Effective replication factor.\n\
         # TYPE stplr_cluster_replication gauge\n\
         stplr_cluster_replication {replication}\n\
         # HELP stplr_cluster_migrating Whether a rebalance/migration is in progress (1=yes).\n\
         # TYPE stplr_cluster_migrating gauge\n\
         stplr_cluster_migrating {}\n",
        migrating as u8
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_gauges_have_prometheus_shape() {
        let out = render_cluster_gauges(3, 2, true);
        assert!(out.contains("# TYPE stplr_cluster_shards gauge"));
        assert!(out.contains("stplr_cluster_shards 3"));
        assert!(out.contains("stplr_cluster_replication 2"));
        assert!(out.contains("stplr_cluster_migrating 1"));
        assert!(render_cluster_gauges(1, 1, false).contains("stplr_cluster_migrating 0"));
    }

    #[test]
    fn render_has_prometheus_shape() {
        init();
        inc(&GET);
        inc(&SET);
        let out = render();
        assert!(out.contains("# TYPE stplr_ops_total counter"));
        assert!(out.contains("stplr_ops_total{op=\"get\"}"));
        assert!(out.contains("stplr_uptime_seconds"));
    }
}
