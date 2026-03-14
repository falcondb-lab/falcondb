//! Nonce anti-replay tracker for the native protocol handshake.
//!
//! Each ClientHello carries a 16-byte nonce. The server records recently seen
//! nonces and rejects duplicates within a configurable window to prevent
//! replay attacks on the handshake.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Thread-safe nonce tracker with a sliding time window.
#[derive(Debug)]
pub struct NonceTracker {
    entries: parking_lot::Mutex<VecDeque<NonceEntry>>,
    window: Duration,
    max_entries: usize,
}

#[derive(Debug)]
struct NonceEntry {
    nonce: [u8; 16],
    seen_at: Instant,
}

impl NonceTracker {
    /// Create a new tracker with the given time window and max capacity.
    pub fn new(window: Duration, max_entries: usize) -> Self {
        Self {
            entries: parking_lot::Mutex::new(VecDeque::with_capacity(max_entries)),
            window,
            max_entries,
        }
    }

    /// Default tracker: 5-minute window, 10k entries.
    pub fn default_tracker() -> Self {
        Self::new(Duration::from_secs(300), 10_000)
    }

    /// Check if a nonce has been seen before. If not, record it and return true.
    /// Returns false if the nonce is a replay (already seen within the window).
    pub fn check_and_record(&self, nonce: &[u8; 16]) -> bool {
        // All-zero nonce is a special case: always allowed (test/dev mode)
        if nonce == &[0u8; 16] {
            return true;
        }

        let mut entries = self.entries.lock();
        let now = Instant::now();

        // Evict expired entries
        while let Some(front) = entries.front() {
            if now.duration_since(front.seen_at) > self.window {
                entries.pop_front();
            } else {
                break;
            }
        }

        // Check for duplicate
        for entry in entries.iter() {
            if entry.nonce == *nonce {
                return false; // replay detected
            }
        }

        // Record the nonce
        if entries.len() >= self.max_entries {
            entries.pop_front(); // evict oldest if at capacity
        }
        entries.push_back(NonceEntry {
            nonce: *nonce,
            seen_at: now,
        });

        true
    }

    /// Number of nonces currently tracked.
    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    /// Whether the tracker is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.lock().is_empty()
    }

    /// Clear all tracked nonces.
    pub fn clear(&self) {
        self.entries.lock().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unique_nonces_accepted() {
        let tracker = NonceTracker::new(Duration::from_secs(60), 100);
        let n1 = [1u8; 16];
        let n2 = [2u8; 16];
        assert!(tracker.check_and_record(&n1));
        assert!(tracker.check_and_record(&n2));
        assert_eq!(tracker.len(), 2);
    }

    #[test]
    fn test_duplicate_nonce_rejected() {
        let tracker = NonceTracker::new(Duration::from_secs(60), 100);
        let nonce = [0xAA; 16];
        assert!(tracker.check_and_record(&nonce));
        assert!(!tracker.check_and_record(&nonce)); // replay
    }

    #[test]
    fn test_zero_nonce_always_allowed() {
        let tracker = NonceTracker::new(Duration::from_secs(60), 100);
        let zero = [0u8; 16];
        assert!(tracker.check_and_record(&zero));
        assert!(tracker.check_and_record(&zero)); // zero is always allowed
        assert_eq!(tracker.len(), 0); // zero nonces are not recorded
    }

    #[test]
    fn test_capacity_eviction() {
        let tracker = NonceTracker::new(Duration::from_secs(60), 3);
        let n1 = [1u8; 16];
        let n2 = [2u8; 16];
        let n3 = [3u8; 16];
        let n4 = [4u8; 16];

        assert!(tracker.check_and_record(&n1));
        assert!(tracker.check_and_record(&n2));
        assert!(tracker.check_and_record(&n3));
        assert_eq!(tracker.len(), 3);

        // Adding n4 should evict n1
        assert!(tracker.check_and_record(&n4));
        assert_eq!(tracker.len(), 3);

        // n1 was evicted, so it should be accepted again
        assert!(tracker.check_and_record(&n1));
    }

    #[test]
    fn test_clear() {
        let tracker = NonceTracker::new(Duration::from_secs(60), 100);
        let nonce = [0xBB; 16];
        assert!(tracker.check_and_record(&nonce));
        assert!(!tracker.check_and_record(&nonce));

        tracker.clear();
        assert!(tracker.is_empty());
        assert!(tracker.check_and_record(&nonce)); // accepted again after clear
    }

    #[test]
    fn test_default_tracker() {
        let tracker = NonceTracker::default_tracker();
        assert!(tracker.is_empty());
        let nonce = [0xCC; 16];
        assert!(tracker.check_and_record(&nonce));
        assert!(!tracker.check_and_record(&nonce));
    }
}
