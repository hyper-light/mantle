//! One index over the memtables a shard keeps readable for gets (research/51 §7): each key's
//! newest record among the memtables it covers, where the key's hash points
//! ([`crate::kept_table`], research/63), so a get the index answers reads one place.
//!
//! A packing worker walks each key's newest entry of a memtable's copy as it builds the branch,
//! and hands back those records ([`KeyList`]), split by the top bits of their places into the
//! index's parts. The index is one copy: a part goes out on a worker alone to take its records of
//! the memtables kept since it last did ([`KeptIndex::take_part`]), the newer memtable's record
//! replacing an older one's, and comes back to serve again; while it is out, a key of that part
//! is answered by the memtables newer than its part holds and then the trunk. Once every part
//! holds a memtable's records, its arena is read no more.
//!
//! One copy holds a record in one slot: two copies taking turns held 46-60 GB at 100M against
//! main's 35-37, kept a fifth of the memtables there and sent the rest of the gets to the trunk
//! (research/63 §5). Each record is merged once, and a trip is one part's: the parts out at once
//! are as many as keep pace with the writes, `x` ns to merge a record and `r` records a second
//! needing `x r` workers busy.
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

/// The index: each key's newest record among the memtables its part covers, in parts split by
/// the top `bits` bits of a key's place, each part one table, home serving gets or out on a
/// worker.
#[derive(Debug)]
pub struct KeptIndex {
    /// Each part's table, none while it is out.
    parts: Vec<Option<RecordTable>>,
    bits: u32,
    /// The newest memtable each part's table holds the records of, none before its first.
    covers: Vec<Option<u32>>,
    /// Per part, the shard's count of given-back records (its `kept_gone`) when the part was last
    /// swept: what has gone stale in it since is the count now less this.
    swept_at: Vec<u64>,
    /// Which index this is among those the shard has made, so a part back from a worker after its
    /// index was dropped is dropped too.
    id: u64,
}

impl KeptIndex {
    /// An empty index of `2^bits` parts, the shard's `id`th.
    pub fn new(bits: u32, id: u64) -> Self {
        let parts = 1usize.checked_shl(bits).unwrap_or(1);
        Self {
            parts: (0..parts).map(|_| Some(RecordTable::part(bits))).collect(),
            bits,
            covers: vec![None; parts],
            swept_at: vec![0; parts],
            id,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Its parts.
    pub fn parts(&self) -> usize {
        self.parts.len()
    }

    /// The part a key of 32-bit filter hash `hash32` is in.
    pub fn part_of(&self, hash32: u32) -> usize {
        part_of(hash32, self.bits)
    }

    /// The newest memtable part `part` holds the records of, none before its first.
    pub fn covers(&self, part: usize) -> Option<u32> {
        self.covers.get(part).copied().flatten()
    }

    /// The newest memtable every part holds the records of: none while a part holds none.
    pub fn covers_all(&self) -> Option<u32> {
        let mut all: Option<u32> = None;
        for c in &self.covers {
            let c = (*c)?;
            all = Some(match all {
                Some(a) if older(a, c) => a,
                _ => c,
            });
        }
        all
    }

    /// The newest memtable any part holds the records of: none before the first.
    pub fn covers_newest(&self) -> Option<u32> {
        let mut newest: Option<u32> = None;
        for c in self.covers.iter().flatten() {
            newest = Some(match newest {
                Some(n) if older(*c, n) => n,
                _ => *c,
            });
        }
        newest
    }

    /// Whether part `part` is out on a worker.
    pub fn is_out(&self, part: usize) -> bool {
        self.parts.get(part).is_some_and(Option::is_none)
    }

    /// The parts out on workers.
    pub fn out(&self) -> usize {
        self.parts.iter().filter(|p| p.is_none()).count()
    }

    /// The shard's count of given-back records when part `part` was last swept.
    pub fn swept_at(&self, part: usize) -> u64 {
        self.swept_at.get(part).copied().unwrap_or(0)
    }

    /// Records held by the parts home, those of memtables given back included until a sweep
    /// drops them.
    pub fn len(&self) -> usize {
        self.parts
            .iter()
            .flatten()
            .map(RecordTable::len)
            .fold(0, usize::saturating_add)
    }

    pub fn is_empty(&self) -> bool {
        self.parts.iter().flatten().all(RecordTable::is_empty)
    }

    /// The bytes the tables of the parts home take.
    pub fn bytes(&self) -> usize {
        self.parts
            .iter()
            .flatten()
            .map(RecordTable::bytes)
            .fold(0, usize::saturating_add)
    }

    /// The newest record of `key`, of 32-bit filter hash `hash32`, among the memtables its part
    /// covers: unknown when that memtable is older than `oldest`, given back, or the part is out.
    #[inline]
    pub fn get(&self, hash32: u32, key: &[u8], oldest: u32) -> Answer<'_> {
        match self.parts.get(self.part_of(hash32)) {
            Some(Some(t)) => t.get(hash32, key, oldest),
            Some(None) => Answer::Unknown,
            None => Answer::Absent,
        }
    }

    /// Reads `hash32`'s home slot, so a lookup after finds it cached ([`RecordTable::touch`]).
    #[inline]
    pub fn touch(&self, hash32: u32) -> u8 {
        match self.parts.get(self.part_of(hash32)) {
            Some(Some(t)) => t.touch(hash32),
            _ => 0,
        }
    }

    /// The bytes a trip of part `part` that takes `lists` (memtable number and records) and,
    /// with `sweep`, sweeps first, grows its table to beside the one it took
    /// ([`RecordTable::growth`]), the part's slot size settled from its first records here, once:
    /// what the trip holds beyond the table it took, at most. None for a part out.
    pub fn growth<'a>(
        &mut self,
        part: usize,
        lists: impl Iterator<Item = &'a crate::kept_table::RecordList> + Clone,
        sweep: bool,
    ) -> usize {
        let Some(Some(table)) = self.parts.get_mut(part) else {
            return 0;
        };
        if let Some(first) = lists.clone().find(|l| !l.is_empty()) {
            table.settle_slot(first);
        }
        table.growth(sweep, lists.map(crate::kept_table::RecordList::len))
    }

    /// Part `part`'s table, taken out for a worker to extend, the part out until it is back;
    /// none for a part already out. With `sweep`, the part is noted swept at `gone`.
    pub fn take_part(&mut self, part: usize, sweep: Option<u64>) -> Option<RecordTable> {
        let table = self.parts.get_mut(part)?.take()?;
        if let (Some(gone), Some(at)) = (sweep, self.swept_at.get_mut(part)) {
            *at = gone;
        }
        Some(table)
    }

    /// Part `part`'s table, back from its worker holding the records of memtables up to `covers`.
    pub fn put_part(&mut self, part: usize, table: RecordTable, covers: Option<u32>) {
        if let Some(p) = self.parts.get_mut(part) {
            *p = Some(table);
        }
        if let (Some(c), Some(slot)) = (covers, self.covers.get_mut(part)) {
            *slot = Some(c);
        }
    }

    /// Every part home takes each memtable's records it lacks, `lists` oldest first, after a
    /// sweep with `sweep` of records of memtables older than `oldest` (given back), here rather
    /// than on workers: for a shard without them, and tests. On an error (memory refused) the
    /// index holds some of the lists' records and must be dropped, not served.
    pub fn extend(
        &mut self,
        lists: &[(u32, KeyList)],
        oldest: u32,
        sweep: bool,
    ) -> Result<(), Error> {
        for (p, slot) in self.parts.iter_mut().enumerate() {
            let Some(table) = slot.as_mut() else {
                continue;
            };
            let covers = self.covers.get(p).copied().flatten();
            if sweep {
                table.sweep(oldest)?;
            }
            for (memtable, list) in lists {
                if covers.is_some_and(|c| !older(c, *memtable)) {
                    continue;
                }
                if let Some(records) = list.part(p) {
                    table.merge(*memtable, records, oldest)?;
                }
                if let Some(c) = self.covers.get_mut(p) {
                    *c = Some(*memtable);
                }
            }
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
        /// Against a map of each key's newest record: lists added a few at a time, in one, two or
        /// four parts, through the index's own extend or part by part as trips take them (a key
        /// of a part out answered unknown), the numbers running past the counter's wrap, each
        /// list offered twice, and memtables given back with a sweep, which drops exactly the
        /// records whose newest memtable was given back.
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
            let mut index = KeptIndex::new(bits, 1);
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
                    // As trips do it: each part taken out alone, a key of it answered unknown
                    // while it is out, its records of the lists it lacks merged, and put back
                    // covering the newest.
                    for part in 0..index.parts() {
                        let covers = index.covers(part);
                        let mut table = index.take_part(part, None).unwrap();
                        prop_assert!(index.is_out(part));
                        if let Some(k) = model.keys().find(|k| index.part_of(h(k)) == part) {
                            prop_assert_eq!(index.get(h(k), k, start), Answer::Unknown);
                        }
                        for (n, list) in &lists {
                            if covers.is_none_or(|c| older(c, *n)) {
                                table.merge(*n, list.part(part).unwrap(), start).unwrap();
                            }
                        }
                        index.put_part(part, table, lists.last().map(|(n, _)| *n));
                    }
                } else {
                    index.extend(&lists, start, false).unwrap();
                }
                index.extend(&lists, start, false).unwrap();
                for part in 0..index.parts() {
                    prop_assert_eq!(index.covers(part), Some(next.wrapping_sub(1)));
                }
                prop_assert_eq!(index.covers_all(), Some(next.wrapping_sub(1)));
                prop_assert_eq!(index.out(), 0);
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
