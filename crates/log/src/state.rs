//! What the log holds in memory (docs/design/raft-log.md §4): each group's retained entries,
//! hard state, start and proposals, where each was written, and how many live pieces each
//! segment still holds.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use crate::format::{HardState, Start};

/// Where a piece of the log was written: its segment's slot in the file and the file offset
/// of its encoded start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Place {
    pub slot: u32,
    pub offset: u64,
}

/// A retained entry: its term, where it is, its payload's length and, while it is recent,
/// its bytes.
#[derive(Debug, Clone)]
pub struct Slot {
    pub term: u64,
    pub place: Place,
    pub len: u32,
    pub cached: Option<Arc<[u8]>>,
}

/// An entry the group approved by itself on the fast track.
#[derive(Debug, Clone)]
pub struct Proposal {
    pub term: u64,
    pub place: Place,
    pub bytes: Arc<[u8]>,
}

/// One group's durable state.
#[derive(Debug, Clone, Default)]
pub struct Group {
    pub start: Start,
    /// Where the start was written; `None` for a group that never recorded one.
    pub start_at: Option<Place>,
    /// Entries from `start.index + 1` on.
    pub entries: VecDeque<Slot>,
    pub hard: Option<(HardState, Place)>,
    pub proposals: BTreeMap<u64, Proposal>,
    /// Payload bytes of the retained entries.
    pub bytes: u64,
    /// Payload bytes of the cached entries, which are the retained entries from
    /// `cache_from` on.
    pub cached: u64,
    pub cache_from: u64,
}

impl Group {
    /// The index of the last entry held, or the start's when none is.
    pub fn last(&self) -> Option<u64> {
        let held = u64::try_from(self.entries.len()).ok()?;
        self.start.index.checked_add(held)
    }

    /// The first entry's index.
    pub fn first(&self) -> Option<u64> {
        self.start.index.checked_add(1)
    }

    pub fn slot(&self, index: u64) -> Option<&Slot> {
        let i = index.checked_sub(self.first()?)?;
        self.entries.get(usize::try_from(i).ok()?)
    }

    /// The term of `index`, answered for the start's index too.
    pub fn term(&self, index: u64) -> Option<u64> {
        if index == self.start.index {
            return Some(self.start.term);
        }
        self.slot(index).map(|s| s.term)
    }

    /// Every live piece of the group: where it is and the bytes it takes there.
    pub fn pieces(&self) -> impl Iterator<Item = (Place, u64)> + '_ {
        self.start_at
            .map(|p| (p, START_BYTES))
            .into_iter()
            .chain(self.hard.iter().map(|(_, p)| (*p, HARD_STATE_BYTES)))
            .chain(self.entries.iter().map(|s| (s.place, entry_bytes(s.len))))
            .chain(self.proposals.values().map(|p| {
                let len = u32::try_from(p.bytes.len()).unwrap_or(u32::MAX);
                (p.place, entry_bytes(len).saturating_add(PROPOSAL_EXTRA))
            }))
    }
}

/// Bytes a record's pieces take in a payload (format.rs).
pub const START_BYTES: u64 = 33;
pub const HARD_STATE_BYTES: u64 = 41;
/// A proposal's bytes beyond an entry's: its record's kind and group, and its index.
pub const PROPOSAL_EXTRA: u64 = 25;

/// Bytes an entry of `len` payload bytes takes in a payload.
pub fn entry_bytes(len: u32) -> u64 {
    u64::from(len).saturating_add(crate::format::ENTRY_HEADER_BYTES)
}

/// The live pieces each segment slot holds, and their bytes.
#[derive(Debug, Clone, Default)]
pub struct Live {
    counts: Vec<(u64, u64)>,
}

impl Live {
    pub fn with_slots(slots: usize) -> Self {
        Self {
            counts: vec![(0, 0); slots],
        }
    }

    pub fn grow(&mut self, slots: usize) {
        if slots > self.counts.len() {
            self.counts.resize(slots, (0, 0));
        }
    }

    pub fn add(&mut self, place: Place, bytes: u64) {
        if let Some((count, total)) = self.counts.get_mut(slot_index(place)) {
            *count = count.saturating_add(1);
            *total = total.saturating_add(bytes);
        }
    }

    pub fn kill(&mut self, place: Place, bytes: u64) {
        if let Some((count, total)) = self.counts.get_mut(slot_index(place)) {
            *count = count.saturating_sub(1);
            *total = total.saturating_sub(bytes);
        }
    }

    /// The live pieces in `slot` and their bytes.
    pub fn of(&self, slot: u32) -> (u64, u64) {
        usize::try_from(slot)
            .ok()
            .and_then(|s| self.counts.get(s))
            .copied()
            .unwrap_or((0, 0))
    }
}

fn slot_index(place: Place) -> usize {
    usize::try_from(place.slot).unwrap_or(usize::MAX)
}

/// A group as replay rebuilds it: entries may arrive out of order, since relocated copies
/// of old entries follow the newer ones they sit beside (docs/design/raft-log.md §5).
#[derive(Debug, Default)]
pub struct Replayed {
    pub start: Start,
    pub start_at: Option<Place>,
    pub entries: BTreeMap<u64, Slot>,
    pub hard: Option<(HardState, Place)>,
    pub proposals: BTreeMap<u64, Proposal>,
    /// The group's last index as the records replayed so far set it. Relocated copies do
    /// not set it, so it can lag the truth before the first `Entries` replayed, but never
    /// past a proposal written after it.
    pub last: u64,
}

impl Replayed {
    /// Entries at `first` and after are replaced by what follows.
    pub fn truncate(&mut self, first: u64) {
        self.entries.split_off(&first);
    }

    /// The group's log reached `last`: proposals at or before it end, as they do when the
    /// update that reached it was applied (07 §1.4).
    pub fn reach(&mut self, last: u64) {
        self.last = last;
        match last.checked_add(1) {
            Some(after) => self.proposals = self.proposals.split_off(&after),
            None => self.proposals.clear(),
        }
    }

    /// The group as it runs, or `None` if its entries do not run unbroken from its start to
    /// its last: a record the log acknowledged is missing.
    pub fn finish(mut self) -> Option<Group> {
        let first = self.start.index.checked_add(1)?;
        let held = self.entries.split_off(&first);
        let mut entries = VecDeque::with_capacity(held.len());
        let mut bytes = 0u64;
        for (expected, (index, slot)) in (first..).zip(held) {
            if index != expected {
                return None;
            }
            bytes = bytes.checked_add(u64::from(slot.len))?;
            entries.push_back(slot);
        }
        let last = self
            .start
            .index
            .checked_add(u64::try_from(entries.len()).ok()?)?;
        // What the log has reached is no longer a proposal (07 §1.4).
        let proposals = self.proposals.split_off(&last.checked_add(1)?);
        Some(Group {
            start: self.start,
            start_at: self.start_at,
            entries,
            hard: self.hard,
            proposals,
            bytes,
            cached: 0,
            cache_from: last.checked_add(1)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(term: u64, slot: u32) -> Slot {
        Slot {
            term,
            place: Place {
                slot,
                offset: u64::from(slot) * 100,
            },
            len: 1,
            cached: None,
        }
    }

    #[test]
    fn replay_accepts_relocated_entries_behind_newer_ones_and_refuses_gaps() {
        let mut r = Replayed::default();
        r.entries.insert(6, slot(2, 3));
        r.entries.insert(7, slot(2, 3));
        // Relocated copies of 1..=5 arrive after 6 and 7.
        for i in 1..=5 {
            r.entries.insert(i, slot(1, 4));
        }
        r.start = Start { index: 2, term: 1 };
        let g = r.finish().unwrap();
        assert_eq!((g.first(), g.last()), (Some(3), Some(7)));
        assert_eq!(g.term(2), Some(1));
        assert_eq!(g.term(6), Some(2));
        assert_eq!(g.term(8), None);
        let mut gap = Replayed::default();
        gap.entries.insert(1, slot(1, 0));
        gap.entries.insert(3, slot(1, 0));
        assert!(gap.finish().is_none());
    }

    #[test]
    fn live_counts_follow_places() {
        let mut live = Live::with_slots(2);
        let p = Place { slot: 1, offset: 0 };
        live.add(p, 10);
        live.add(p, 5);
        live.kill(p, 10);
        assert_eq!(live.of(1), (1, 5));
        live.grow(4);
        live.add(Place { slot: 3, offset: 0 }, 7);
        assert_eq!(live.of(3), (1, 7));
        assert_eq!(live.of(9), (0, 0));
    }
}
