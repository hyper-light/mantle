//! Why this node answered its requests `Failed`, S3's `500 InternalError` (node.md §4.4), each
//! cause counted since the node opened. A failure's reply names its cause on either door
//! (`door::Fault`); these counts say on the node's lines (`failed NAME COUNT`) which causes
//! failed its requests, where its clients saw only what each was told.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::door::Fault;

/// The requests this node answered `Failed`, a count for each cause in [`Fault::ALL`]'s order:
/// one fixed array, each count an atomic add on a failure's path alone, shared by every host of
/// the node as its gateway's settings are (one `Arc` made with the node).
#[derive(Debug)]
pub struct Failures {
    counts: [AtomicU64; Fault::ALL.len()],
}

impl Default for Failures {
    fn default() -> Self {
        Self {
            counts: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl Failures {
    /// A request answered `Failed` for `cause`. Saturating: a count past `u64` says as much.
    pub fn count(&self, cause: Fault) {
        if let Some(count) = self.counts.get(cause.index()) {
            let _ = count.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(1))
            });
        }
    }

    /// Each cause's count, in [`Fault::ALL`]'s order.
    pub fn counts(&self) -> [u64; Fault::ALL.len()] {
        std::array::from_fn(|at| {
            self.counts
                .get(at)
                .map_or(0, |count| count.load(Ordering::Relaxed))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each cause is counted in its own place, in `Fault::ALL`'s order, and no other.
    #[test]
    fn each_cause_counts_in_its_own_place() {
        let failures = Failures::default();
        for (at, cause) in Fault::ALL.into_iter().enumerate() {
            for _ in 0..=at {
                failures.count(cause);
            }
        }
        let counts = failures.counts();
        for (at, count) in counts.into_iter().enumerate() {
            assert_eq!(count, u64::try_from(at).unwrap() + 1);
        }
    }
}
