//! Hybrid Logical Clock (HLC) for cross-node causal ordering.
//!
//! Encodes (physical_ms, logical) into a single u64 Timestamp:
//!   bits [63..16] = wall clock milliseconds (48 bits, ~8900 years)
//!   bits [15..0]  = logical counter (16 bits, 65535 ticks per ms)
//!
//! Properties:
//! - Monotonic within a node (never goes backwards)
//! - Causal: recv(msg) always gets a timestamp > send(msg)
//! - Compatible with existing Timestamp(u64) — old monotonic counters
//!   just happen to live in the low range, HLC values are much larger

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::types::Timestamp;

const LOGICAL_BITS: u32 = 16;
const LOGICAL_MASK: u64 = (1u64 << LOGICAL_BITS) - 1;

fn physical(ts: u64) -> u64 {
    ts >> LOGICAL_BITS
}
fn logical(ts: u64) -> u64 {
    ts & LOGICAL_MASK
}
fn pack(phys: u64, log: u64) -> u64 {
    (phys << LOGICAL_BITS) | (log & LOGICAL_MASK)
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Thread-safe Hybrid Logical Clock.
pub struct HybridClock {
    state: AtomicU64,
}

impl HybridClock {
    pub fn new() -> Self {
        Self {
            state: AtomicU64::new(pack(wall_ms(), 0)),
        }
    }

    /// Allocate a new timestamp, advancing the clock.
    pub fn now(&self) -> Timestamp {
        loop {
            let old = self.state.load(Ordering::Relaxed);
            let old_phys = physical(old);
            let old_log = logical(old);
            let pt = wall_ms();

            let new = if pt > old_phys {
                pack(pt, 0)
            } else {
                // Wall clock hasn't advanced — bump logical
                pack(old_phys, old_log + 1)
            };

            if self
                .state
                .compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Timestamp(new);
            }
        }
    }

    /// Update clock on receiving a remote timestamp (causal receive).
    /// Returns a new timestamp > max(local, remote).
    pub fn recv(&self, remote: Timestamp) -> Timestamp {
        loop {
            let old = self.state.load(Ordering::Relaxed);
            let old_phys = physical(old);
            let old_log = logical(old);
            let rem_phys = physical(remote.0);
            let rem_log = logical(remote.0);
            let pt = wall_ms();

            let new = if pt > old_phys && pt > rem_phys {
                pack(pt, 0)
            } else if old_phys == rem_phys {
                pack(old_phys, old_log.max(rem_log) + 1)
            } else if old_phys > rem_phys {
                pack(old_phys, old_log + 1)
            } else {
                pack(rem_phys, rem_log + 1)
            };

            if self
                .state
                .compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Timestamp(new);
            }
        }
    }

    /// Current clock value without advancing.
    pub fn current(&self) -> Timestamp {
        Timestamp(self.state.load(Ordering::Relaxed))
    }

    /// Advance clock past a given value (e.g. after WAL recovery).
    pub fn advance_past(&self, ts: Timestamp) {
        loop {
            let old = self.state.load(Ordering::Relaxed);
            if old >= ts.0 {
                return;
            }
            if self
                .state
                .compare_exchange_weak(old, ts.0 + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Extract physical component (milliseconds since epoch).
    pub fn physical_ms(ts: Timestamp) -> u64 {
        physical(ts.0)
    }

    /// Extract logical component.
    pub fn logical_part(ts: Timestamp) -> u64 {
        logical(ts.0)
    }
}

impl Default for HybridClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic() {
        let c = HybridClock::new();
        let mut prev = c.now();
        for _ in 0..1000 {
            let t = c.now();
            assert!(t > prev, "HLC must be strictly monotonic");
            prev = t;
        }
    }

    #[test]
    fn recv_advances_past_remote() {
        let c = HybridClock::new();
        let local = c.now();
        // Simulate a remote timestamp far in the future
        let remote = Timestamp(local.0 + pack(10_000, 0));
        let after = c.recv(remote);
        assert!(after > remote, "recv must return ts > remote");
        assert!(after > local, "recv must return ts > local");
    }

    #[test]
    fn advance_past() {
        let c = HybridClock::new();
        let big = Timestamp(pack(wall_ms() + 100_000, 42));
        c.advance_past(big);
        let after = c.now();
        assert!(after > big);
    }

    #[test]
    fn pack_unpack() {
        let phys = 1_700_000_000_000u64; // ~2023 in ms
        let log = 42u64;
        let packed = pack(phys, log);
        assert_eq!(physical(packed), phys);
        assert_eq!(logical(packed), log);
    }
}
