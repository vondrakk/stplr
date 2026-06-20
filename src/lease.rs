// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! TTL-lease leader election — electing the singleton coordinator (the rebalance/migration driver)
//! without any external coordination service. A single lease record (holder + expiry + fencing
//! token) lives at a well-known key; coordinators race to acquire/renew it on the shard that owns
//! that key, and the current holder is the leader.
//!
//! **Correctness scope (be honest about it):** the owning shard serializes acquires under its write
//! lock and is the single clock source, so this is correct mutual exclusion *under a healthy
//! cluster* — the same model as a Redis `SET NX PX` lock or a Consul session, NOT a
//! partition-tolerant consensus. If the lease shard is unreachable, the lease can expire and be
//! re-granted elsewhere; the **fencing token** (bumped on every change of holder) lets a driver
//! detect that it was superseded and stop acting. A future increment can replicate the lease and
//! enforce the token at the write path; this increment provides the election + the token.

use serde::{Deserialize, Serialize};

/// Collection + key the coordinator lease lives at (an ordinary object, routed like any key).
pub const LEASE_COLL: &str = "__lease";
pub const LEADER_KEY: &str = "coordinator";

/// A lease record: who holds it, when it expires (epoch millis), and the fencing token (the
/// leadership term — strictly increases each time the lease changes hands).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Lease {
    pub holder: String,
    pub expires_ms: u64,
    pub token: u64,
}

/// The result of an acquire attempt: whether it was granted, and the authoritative current lease
/// (the caller's own lease if granted, or the incumbent's if denied).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseOutcome {
    pub granted: bool,
    pub lease: Lease,
}

/// Wall-clock epoch millis — the lease shard's single clock source for expiry comparisons.
pub fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Pure lease transition. Granted when the lease is vacant, expired, or already held by `holder`.
/// The fencing token bumps only when leadership changes hands — a renew by the incumbent keeps its
/// token, so a token change always means a new term (and that any prior holder is deposed).
pub fn evaluate(current: Option<&Lease>, holder: &str, ttl_ms: u64, now_ms: u64) -> LeaseOutcome {
    match current {
        // renew: incumbent re-acquires before expiry → same term, same token
        Some(l) if l.holder == holder && now_ms < l.expires_ms => LeaseOutcome {
            granted: true,
            lease: Lease { holder: holder.to_string(), expires_ms: now_ms + ttl_ms, token: l.token },
        },
        // held by someone else and still valid → denied; report the incumbent
        Some(l) if now_ms < l.expires_ms => LeaseOutcome { granted: false, lease: l.clone() },
        // expired (held by anyone, incl. a late incumbent) → new term, bump the token
        Some(l) => LeaseOutcome {
            granted: true,
            lease: Lease { holder: holder.to_string(), expires_ms: now_ms + ttl_ms, token: l.token + 1 },
        },
        // first ever acquisition
        None => LeaseOutcome {
            granted: true,
            lease: Lease { holder: holder.to_string(), expires_ms: now_ms + ttl_ms, token: 1 },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vacant_grants_term_one() {
        let o = evaluate(None, "c1", 1000, 5000);
        assert!(o.granted);
        assert_eq!(o.lease, Lease { holder: "c1".into(), expires_ms: 6000, token: 1 });
    }

    #[test]
    fn incumbent_renew_keeps_token() {
        let cur = Lease { holder: "c1".into(), expires_ms: 6000, token: 3 };
        let o = evaluate(Some(&cur), "c1", 1000, 5500); // before expiry
        assert!(o.granted);
        assert_eq!(o.lease.token, 3, "renew keeps the term token");
        assert_eq!(o.lease.expires_ms, 6500, "expiry extended");
    }

    #[test]
    fn other_holder_valid_is_denied() {
        let cur = Lease { holder: "c1".into(), expires_ms: 6000, token: 3 };
        let o = evaluate(Some(&cur), "c2", 1000, 5500); // c1 still valid
        assert!(!o.granted);
        assert_eq!(o.lease, cur, "report the incumbent unchanged");
    }

    #[test]
    fn expired_grants_new_term_and_bumps_token() {
        let cur = Lease { holder: "c1".into(), expires_ms: 6000, token: 3 };
        let o = evaluate(Some(&cur), "c2", 1000, 6001); // c1 lease expired
        assert!(o.granted);
        assert_eq!(o.lease.holder, "c2");
        assert_eq!(o.lease.token, 4, "new leader → token bumps (fences the old leader)");
    }

    #[test]
    fn late_incumbent_after_expiry_is_a_new_term() {
        let cur = Lease { holder: "c1".into(), expires_ms: 6000, token: 3 };
        let o = evaluate(Some(&cur), "c1", 1000, 7000); // same holder, but lapsed
        assert!(o.granted);
        assert_eq!(o.lease.token, 4, "a lapsed lease is a new term even for the same holder");
    }
}
