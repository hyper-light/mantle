//! One index over the memtables a shard keeps readable for gets (research/51 §7): each key's
//! newest record among the memtables it covers, where the key's hash points
//! ([`crate::kept_table`], research/63), so a get the index answers reads one place.
//!
//! A packing worker walks each key's newest entry of a memtable's copy as it builds the branch,
//! and hands back those records ([`KeyList`]), split by the top bits of their places into the
//! index's parts; a maintenance trip takes an index's parts to as many workers at once, each
//! adding its part's records of the memtables kept since ([`KeptIndex::take_part`]), the newer
//! memtable's record replacing an older one's, and the index comes back once every part has;
//! the shard only probes it, one part a key. Once both of the shard's indexes hold a memtable's
//! records, its arena is read no more.
//!
//! The parts are what let the index keep pace with the writes: each trip takes the records
//! kept since the index last went, and the two indexes take turns, so a trip's records arrive
//! over the trip before it and its own; at `x` ns a record merged and `r` records a second, the
//! trips stay bounded only while `x r <= 1/2` (the trip after next is `x r` times the two before
//! it). One worker merged a record in 150-230 ns (`kept_merge`, 10M scale), against 100 ns a
//! record at the bench's 5M records a second: split `P` ways, a trip takes `1/P` the time.
//!
//! Memtables are numbered by a counter that wraps: the memtables an index covers and the shard
//! keeps span far fewer than 2^31 numbers, so "older" is decided modulo 2^32 ([`older`]).

use crate::error::Error;
use crate::kept_table::{Answer, PartedList, RecordTable, part_of};

/// A memtable's records as its packing worker walked them, each key's newest entry once, split
/// by the index's parts.
pub type KeyList = PartedList;

/// Whether memtable `a` was numbered before `b`, the counter having wrapped or not.
pub fn older(a: u32, b: u32) -> bool {
    let d = b.wrapping_sub(a);
    d != 0 && d < 1 << 31
}

/// The index: each key's newest record among the memtables it covers, in parts split by the top
/// `bits` bits of a key's place, and the newest memtable it covers.
#[derive(Debug)]
pub struct KeptIndex {
    parts: Vec<RecordTable>,
    bits: u32,
    covers: Option<u32>,
    /// The records of given-back memtables the shard had counted (its `kept_gone`) when this
    /// index was last swept: what has gone stale in it since is the count now less this.
    swept_at: u64,
}

impl Default for KeptIndex {
    fn default() -> Self {
        Self::new(0)
    }
}

impl KeptIndex {
    /// An empty index of `2^bits` parts.
    pub fn new(bits: u32) -> Self {
        let parts = 1usize.checked_shl(bits).unwrap_or(1);
        Self {
            parts: (0..parts).map(|_| RecordTable::part(bits)).collect(),
            bits,
            covers: None,
            swept_at: 0,
        }
    }

    /// Its parts.
    pub fn parts(&self) -> usize {
        self.parts.len()
    }

    /// The newest memtable whose records the index holds, none before the first.
    pub fn covers(&self) -> Option<u32> {
        self.covers
    }

    /// Notes that every part now holds the records of memtables up to `covers`.
    pub fn set_covers(&mut self, covers: Option<u32>) {
        self.covers = covers;
    }

    /// The shard's count of given-back records when this index was last swept.
    pub fn swept_at(&self) -> u64 {
        self.swept_at
    }

    /// Notes that the sweep this index is sent for drops what the shard has given back up to
    /// `gone` records.
    pub fn set_swept_at(&mut self, gone: u64) {
        self.swept_at = gone;
    }

    /// Records held, those of memtables given back included until a sweep drops them.
    pub fn len(&self) -> usize {
        self.parts
            .iter()
            .map(RecordTable::len)
            .fold(0, usize::saturating_add)
    }

    pub fn is_empty(&self) -> bool {
        self.parts.iter().all(RecordTable::is_empty)
    }

    /// The bytes its tables take.
    pub fn bytes(&self) -> usize {
        self.parts
            .iter()
            .map(RecordTable::bytes)
            .fold(0, usize::saturating_add)
    }

    /// The newest record of `key`, of 32-bit filter hash `hash32`, among the memtables the index
    /// covers: unknown when that memtable is older than `oldest`, given back.
    #[inline]
    pub fn get(&self, hash32: u32, key: &[u8], oldest: u32) -> Answer<'_> {
        self.parts
            .get(part_of(hash32, self.bits))
            .map_or(Answer::Absent, |t| t.get(hash32, key, oldest))
    }

    /// Reads `hash32`'s home slot, so a lookup after finds it cached ([`RecordTable::touch`]).
    #[inline]
    pub fn touch(&self, hash32: u32) -> u8 {
        self.parts
            .get(part_of(hash32, self.bits))
            .map_or(0, |t| t.touch(hash32))
    }

    /// Adds each memtable's records, `lists` oldest first, passing over those it already covers,
    /// every part in turn here (a trip takes the parts to workers at once instead): a key's
    /// record in a newer memtable replaces its record in an older one. With `sweep`, records of
    /// memtables older than `oldest` (given back) are dropped first. On an error (memory
    /// refused) the index holds some of the lists' records and must be dropped, not served.
    pub fn extend(
        &mut self,
        lists: &[(u32, KeyList)],
        oldest: u32,
        sweep: bool,
    ) -> Result<(), Error> {
        let covers = self.covers;
        let lacked = || {
            lists
                .iter()
                .filter(move |(memtable, _)| covers.is_none_or(|c| older(c, *memtable)))
        };
        for (p, table) in self.parts.iter_mut().enumerate() {
            if sweep {
                table.sweep(oldest)?;
            }
            for (memtable, list) in lacked() {
                if let Some(records) = list.part(p) {
                    table.merge(*memtable, records, oldest)?;
                }
            }
        }
        if let Some((newest, _)) = lacked().last() {
            self.covers = Some(*newest);
        }
        Ok(())
    }

    /// Part `part`'s table, taken out for a worker to extend; an empty one is left until it
    /// comes back.
    pub fn take_part(&mut self, part: usize) -> RecordTable {
        let bits = self.bits;
        self.parts
            .get_mut(part)
            .map(|t| std::mem::replace(t, RecordTable::part(bits)))
            .unwrap_or_else(|| RecordTable::part(bits))
    }

    /// Part `part`'s table, back from its worker.
    pub fn put_part(&mut self, part: usize, table: RecordTable) {
        if let Some(t) = self.parts.get_mut(part) {
            *t = table;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::Op;
    use crate::kept_table::Record;
    use proptest::prelude::*;
    use std::collections::HashMap;

    #[test]
    fn older_is_decided_across_the_counter_wrapping() {
        assert!(older(1, 2));
        assert!(!older(2, 1));
        assert!(!older(5, 5));
        assert!(older(u32::MAX, 0));
        assert!(older(u32::MAX - 3, 2));
        assert!(!older(2, u32::MAX - 3));
    }

    fn h(k: &[u8]) -> u32 {
        crate::branch::filter::hash32(crate::branch::filter::hash(k))
    }

    proptest! {
        /// Against a map of each key's newest record: lists added a few at a time, in one, two or
        /// four parts, through the index's own extend or part by part as a trip takes them, the
        /// numbers running past the counter's wrap, each list taken twice (as a trip that carries
        /// it for the other index does), and memtables given back with a sweep, which drops
        /// exactly the records whose newest memtable was given back.
        #[test]
        fn it_holds_each_key_s_newest_record_among_the_memtables_it_covers(
            batches in prop::collection::vec(
                prop::collection::vec(prop::collection::vec((0u64..400, any::<u8>()), 0..60), 1..4),
                1..8,
            ),
            start in prop_oneof![Just(0u32), Just(u32::MAX - 5)],
            gone in 0u32..6,
            bits in 0u32..3,
            by_parts in any::<bool>(),
        ) {
            let mut index = KeptIndex::new(bits);
            let mut model: HashMap<Vec<u8>, (u32, Vec<u8>)> = HashMap::new();
            let mut next = start;
            for batch in &batches {
                let mut lists = Vec::new();
                for keys in batch {
                    let mut list = KeyList::new(bits);
                    // A memtable holds a key once: its newest entry.
                    let mut seen = std::collections::HashSet::new();
                    for &(k, b) in keys {
                        if seen.insert(k) {
                            let key = format!("{k:016}").into_bytes();
                            let value = vec![b; 100];
                            list.push(&Record { hash32: h(&key), op: Op::Put, key: &key, value: &value })
                                .unwrap();
                            model.insert(key, (next, value));
                        }
                    }
                    lists.push((next, list));
                    next = next.wrapping_add(1);
                }
                let before = index.len();
                if by_parts {
                    // As a trip does it: each part taken out, its records of the lists the index
                    // lacks merged, and put back; then the index covers the newest.
                    let covers = index.covers();
                    for part in 0..index.parts() {
                        let mut table = index.take_part(part);
                        for (n, list) in &lists {
                            if covers.is_none_or(|c| older(c, *n)) {
                                table.merge(*n, list.part(part).unwrap(), start).unwrap();
                            }
                        }
                        index.put_part(part, table);
                    }
                    index.set_covers(lists.last().map(|(n, _)| *n));
                } else {
                    index.extend(&lists, start, false).unwrap();
                }
                index.extend(&lists, start, false).unwrap();
                prop_assert_eq!(index.covers(), Some(next.wrapping_sub(1)));
                prop_assert!(index.len() >= before);
            }
            for (k, (_, v)) in &model {
                prop_assert_eq!(index.get(h(k), k, start), Answer::Record(Op::Put, v.as_slice()));
            }
            // Give back the oldest `gone` memtables: the sweep drops their records, no other.
            let oldest = start.wrapping_add(gone);
            index.extend(&[], oldest, true).unwrap();
            for (k, (n, v)) in &model {
                let got = index.get(h(k), k, oldest);
                if older(*n, oldest) {
                    prop_assert_eq!(got, Answer::Absent);
                } else {
                    prop_assert_eq!(got, Answer::Record(Op::Put, v.as_slice()));
                }
            }
        }
    }
}
