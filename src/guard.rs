// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Per-bucket freeze/drain gate for online migration — the async analogue of the TS Phase-C
//! guard. Reads/writes "enter" their key's bucket; a migration "freezes" a bucket (new entrants
//! wait), "drains" in-flight ops, copies+cuts over, then "unfreezes" (waiters resume against the
//! new owner). Wakeups go through a watch channel, which retains its latest value and so cannot
//! lose a notification (unlike a bare Notify).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

struct GateState {
    frozen: HashSet<usize>,
    in_flight: HashMap<usize, usize>,
}

pub struct Gates {
    state: Mutex<GateState>,
    tx: watch::Sender<u64>,
    gen: Mutex<u64>,
}

impl Gates {
    pub fn new() -> Arc<Self> {
        let (tx, _rx) = watch::channel(0);
        Arc::new(Self {
            state: Mutex::new(GateState { frozen: HashSet::new(), in_flight: HashMap::new() }),
            tx,
            gen: Mutex::new(0),
        })
    }

    fn bump(&self) {
        let mut g = self.gen.lock().unwrap();
        *g += 1;
        let _ = self.tx.send(*g);
    }

    /// Enter the gate for bucket `b`: wait while it's frozen, then register as in-flight. The
    /// returned token decrements the in-flight count on drop (panic-safe).
    pub async fn enter(self: &Arc<Self>, b: usize) -> GateToken {
        loop {
            let mut rx = self.tx.subscribe();
            {
                let mut s = self.state.lock().unwrap();
                if !s.frozen.contains(&b) {
                    *s.in_flight.entry(b).or_insert(0) += 1;
                    return GateToken { gates: self.clone(), b };
                }
            }
            let _ = rx.changed().await; // re-check after any gate change
        }
    }

    fn leave(&self, b: usize) {
        {
            let mut s = self.state.lock().unwrap();
            if let Some(n) = s.in_flight.get_mut(&b) {
                *n -= 1;
                if *n == 0 {
                    s.in_flight.remove(&b);
                }
            }
        }
        self.bump();
    }

    /// Freeze a set of buckets — new entrants block until `unfreeze`.
    pub fn freeze(&self, buckets: &[usize]) {
        let mut s = self.state.lock().unwrap();
        for &b in buckets {
            s.frozen.insert(b);
        }
    }

    /// Wait until no in-flight op remains on any of `buckets` (call after `freeze`, before copy).
    pub async fn drain(&self, buckets: &[usize]) {
        loop {
            let mut rx = self.tx.subscribe();
            {
                let s = self.state.lock().unwrap();
                if buckets.iter().all(|b| s.in_flight.get(b).copied().unwrap_or(0) == 0) {
                    return;
                }
            }
            let _ = rx.changed().await;
        }
    }

    /// Release frozen buckets — waiters resume (and re-route to the new owner).
    pub fn unfreeze(&self, buckets: &[usize]) {
        {
            let mut s = self.state.lock().unwrap();
            for &b in buckets {
                s.frozen.remove(&b);
            }
        }
        self.bump();
    }
}

pub struct GateToken {
    gates: Arc<Gates>,
    b: usize,
}

impl Drop for GateToken {
    fn drop(&mut self) {
        self.gates.leave(self.b);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn freeze_blocks_enter_until_unfreeze() {
        let g = Gates::new();
        g.freeze(&[7]);
        let g2 = g.clone();
        let entered = Arc::new(Mutex::new(false));
        let e2 = entered.clone();
        let h = tokio::spawn(async move {
            let _t = g2.enter(7).await; // blocks until unfreeze
            *e2.lock().unwrap() = true;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!*entered.lock().unwrap(), "blocked while frozen");
        g.unfreeze(&[7]);
        h.await.unwrap();
        assert!(*entered.lock().unwrap(), "entered after unfreeze");
    }

    #[tokio::test]
    async fn drain_waits_for_in_flight() {
        let g = Gates::new();
        let token = g.enter(3).await; // in-flight on bucket 3
        let g2 = g.clone();
        let drained = Arc::new(Mutex::new(false));
        let d2 = drained.clone();
        let h = tokio::spawn(async move {
            g2.freeze(&[3]);
            g2.drain(&[3]).await; // waits for the in-flight op to finish
            *d2.lock().unwrap() = true;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!*drained.lock().unwrap(), "drain waits while in-flight");
        drop(token); // op finishes
        h.await.unwrap();
        assert!(*drained.lock().unwrap(), "drained once in-flight cleared");
    }
}
