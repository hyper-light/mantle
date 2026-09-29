//! When the collector acts (docs/design/metadata.md §2): which released files it reclaims, and
//! which buckets' creates and deletes it takes over.
//!
//! The collector runs beside each range's leader and does its work through the reclaimer
//! (reclaim.rs) and the coordinator (coordinator.rs), whose every step can be repeated. Two
//! collectors at once, across a change of leader, only repeat work. What this module decides
//! is when: a released file once its grace has passed, and an attempt once it has gone its
//! patience without progress.

use crate::record::{Bucket, BucketState};

/// How long a released file is kept before it is reclaimed, so a deletion made by mistake can
/// still be undone by a metadata change: three days, as GFS keeps a deleted file
/// (docs/design/chunk-store.md §8; docs/research/22 §1.1). A recovery-point policy, not a
/// safety bound (22 §10.4).
pub const GRACE_NS: u64 = 3 * 24 * 60 * 60 * 1_000_000_000;

/// The collector's two waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Schedule {
    /// How long a released file is kept.
    pub grace_ns: u64,
    /// How long a create or delete may go without progress before the collector takes it
    /// over. Taking over early is safe, since a range refuses the steps of an attempt older
    /// than the one that last moved its row or gate; it only aborts a slow attempt (22 §10.5).
    pub patience_ns: u64,
}

/// What the collector does with a create or delete that has gone its patience without
/// progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Takeover {
    /// A create left unfinished is taken over as a delete (`bucket::Command::Abandon`).
    Abandon,
    /// A delete left unfinished is run again under a new attempt, which deletes the bucket if
    /// it is empty and reopens its gates if not (`bucket::Command::BeginDelete`).
    Delete,
    /// A deleted bucket's cleanup is resumed from its row (`Coordinator::resume`).
    Resume,
}

impl Schedule {
    /// Files released before this time, in the range's clock, are due.
    pub fn due_before(&self, now_ns: u64) -> u64 {
        now_ns.saturating_sub(self.grace_ns)
    }

    /// When to read the queue next: when its oldest row comes due, or, when it holds none, a
    /// grace from now, before which nothing released from now on can come due. The collector
    /// waits on the queue's own times rather than polling.
    pub fn wake(&self, now_ns: u64, oldest_ns: Option<u64>) -> u64 {
        match oldest_ns {
            Some(released) => released.saturating_add(self.grace_ns).max(now_ns),
            None => now_ns.saturating_add(self.grace_ns),
        }
    }

    /// What the collector does with `row` at `now_ns`: nothing while its attempt shows
    /// progress within the patience, or once it is active.
    pub fn takeover(&self, row: &Bucket, now_ns: u64) -> Option<Takeover> {
        let action = match row.state {
            BucketState::Active => return None,
            BucketState::Creating => Takeover::Abandon,
            BucketState::Deleting => Takeover::Delete,
            BucketState::Deleted => Takeover::Resume,
        };
        if now_ns.saturating_sub(row.progress_ns) >= self.patience_ns {
            Some(action)
        } else {
            None
        }
    }

    /// When the attempt of `row` will have gone its patience, if nothing moves it first.
    pub fn deadline(&self, row: &Bucket) -> Option<u64> {
        (row.state != BucketState::Active).then(|| row.progress_ns.saturating_add(self.patience_ns))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Versioning;

    const S: Schedule = Schedule {
        grace_ns: 1_000,
        patience_ns: 100,
    };

    fn row(state: BucketState, progress_ns: u64) -> Bucket {
        Bucket {
            owner: "o".into(),
            created_ns: 1,
            location: String::new(),
            versioning: Versioning::Unversioned,
            state,
            attempt: 1,
            progress_ns,
            lock: None,
        }
    }

    #[test]
    fn released_files_come_due_a_grace_after_release() {
        assert_eq!(S.due_before(5_000), 4_000);
        assert_eq!(
            S.due_before(500),
            0,
            "nothing is due before a grace has passed"
        );
        // The next look is when the oldest row comes due, never in the past.
        assert_eq!(S.wake(5_000, Some(4_500)), 5_500);
        assert_eq!(S.wake(5_000, Some(3_000)), 5_000);
        assert_eq!(S.wake(5_000, None), 6_000);
        assert_eq!(S.wake(u64::MAX - 10, None), u64::MAX);
        assert_eq!(GRACE_NS, 259_200_000_000_000);
    }

    #[test]
    fn an_attempt_is_taken_over_only_after_its_patience_without_progress() {
        for (state, action) in [
            (BucketState::Creating, Takeover::Abandon),
            (BucketState::Deleting, Takeover::Delete),
            (BucketState::Deleted, Takeover::Resume),
        ] {
            let r = row(state, 1_000);
            assert_eq!(S.takeover(&r, 1_099), None);
            assert_eq!(S.takeover(&r, 1_100), Some(action));
            assert_eq!(S.deadline(&r), Some(1_100));
            // A clock behind the stamp is no reason to act.
            assert_eq!(S.takeover(&r, 10), None);
        }
        let active = row(BucketState::Active, 0);
        assert_eq!(S.takeover(&active, u64::MAX), None);
        assert_eq!(S.deadline(&active), None);
    }
}
