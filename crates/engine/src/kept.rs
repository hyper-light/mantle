//! One index over the memtables a shard keeps readable for gets (research/51 §7): each key's
//! newest record among the memtables it covers, where the key's hash points
//! ([`crate::kept_table`], research/63), so a get the index answers reads one place.
//!
//! A packing worker walks each key's newest entry of a memtable's copy as it builds the branch,
//! and hands back those records ([`KeyList`]); a maintenance worker adds the lists of the
//! memtables kept since to an index it was handed ([`KeptIndex::extend`]), the newer memtable's
//! record replacing an older one's, and hands it back; the shard only probes it. Once both of the
//! shard's indexes hold a memtable's records, its arena is read no more.
//!
//! Memtables are numbered by a counter that wraps: the memtables an index covers and the shard
//! keeps span far fewer than 2^31 numbers, so "older" is decided modulo 2^32 ([`older`]).

use crate::error::Error;
use crate::kept_table::{Answer, RecordList, RecordTable};

/// A memtable's records as its packing worker walked them: each key's newest entry once.
pub type KeyList = RecordList;

/// Whether memtable `a` was numbered before `b`, the counter having wrapped or not.
pub fn older(a: u32, b: u32) -> bool {
    let d = b.wrapping_sub(a);
    d != 0 && d < 1 << 31
}

/// The index: each key's newest record among the memtables it covers, and the newest memtable it
/// covers.
#[derive(Debug, Default)]
pub struct KeptIndex {
    table: RecordTable,
    covers: Option<u32>,
    /// The records of given-back memtables the shard had counted (its `kept_gone`) when this
    /// index was last swept: what has gone stale in it since is the count now less this.
    swept_at: u64,
}

impl KeptIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// The newest memtable whose records the index holds, none before the first.
    pub fn covers(&self) -> Option<u32> {
        self.covers
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
        self.table.len()
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// The bytes its table takes.
    pub fn bytes(&self) -> usize {
        self.table.bytes()
    }

    /// The newest record of `key`, of 32-bit filter hash `hash32`, among the memtables the index
    /// covers: unknown when that memtable is older than `oldest`, given back.
    #[inline]
    pub fn get(&self, hash32: u32, key: &[u8], oldest: u32) -> Answer<'_> {
        self.table.get(hash32, key, oldest)
    }

    /// Reads `hash32`'s home slot, so a lookup after finds it cached ([`RecordTable::touch`]).
    #[inline]
    pub fn touch(&self, hash32: u32) -> u8 {
        self.table.touch(hash32)
    }

    /// Adds each memtable's records, `lists` oldest first, passing over those it already covers: a
    /// key's record in a newer memtable replaces its record in an older one. With `sweep`, records
    /// of memtables older than `oldest` (given back) are dropped first. On an error (memory
    /// refused) the index holds some of the lists' records and must be dropped, not served.
    pub fn extend(
        &mut self,
        lists: &[(u32, KeyList)],
        oldest: u32,
        sweep: bool,
    ) -> Result<(), Error> {
        if sweep {
            self.table.sweep(oldest)?;
        }
        for (memtable, list) in lists {
            // A list it took on an earlier trip is passed over: its records are in, or replaced
            // by newer ones.
            if self.covers.is_some_and(|c| !older(c, *memtable)) {
                continue;
            }
            self.table.merge(*memtable, list, oldest)?;
            self.covers = Some(*memtable);
        }
        Ok(())
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
        /// Against a map of each key's newest record: lists added a few at a time, the numbers
        /// running past the counter's wrap, each list taken twice (as a trip that carries it for
        /// the other index does), and memtables given back with a sweep, which drops exactly the
        /// records whose newest memtable was given back.
        #[test]
        fn it_holds_each_key_s_newest_record_among_the_memtables_it_covers(
            batches in prop::collection::vec(
                prop::collection::vec(prop::collection::vec((0u64..400, any::<u8>()), 0..60), 1..4),
                1..8,
            ),
            start in prop_oneof![Just(0u32), Just(u32::MAX - 5)],
            gone in 0u32..6,
        ) {
            let mut index = KeptIndex::new();
            let mut model: HashMap<Vec<u8>, (u32, Vec<u8>)> = HashMap::new();
            let mut next = start;
            for batch in &batches {
                let mut lists = Vec::new();
                for keys in batch {
                    let mut list = KeyList::default();
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
                index.extend(&lists, start, false).unwrap();
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
