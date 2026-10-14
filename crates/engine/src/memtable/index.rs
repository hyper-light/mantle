//! A memtable's index of each key's newest entry: a SwissTable of the entries' arena offsets in as
//! many groups as its memory buys (research/54 §5).
//!
//! `IncMap` keeps a 64-bit key and a 64-bit value a bucket, 17 bytes with its control byte, so a
//! 64 MiB memtable of 560 Ki entries spent 17 MiB on it. Here a bucket holds only the entry's
//! 32-bit offset, in its group's cache line beside the group's control word (8 bytes a bucket
//! with the line's padding): the key a probe compares is the entry's own, read from the arena.
//! Keys compare by their bytes, so two keys of one 64-bit hash are two entries, with no list of
//! clashes beside the table.
//!
//! A group is matched as `IncMap` matches one (Abseil's SwissTable: a control byte a bucket, 7 bits
//! of the hash, read eight at a time; hashbrown's portable group). A key's first group is its
//! hash's bits below the tag multiplied into the count of groups, the product's high word
//! (multiply-shift range reduction: Lemire, "Fast Random Integer Generation in an Interval", ACM
//! TOMACS 29(1), 2019), so a table holds any count of groups its memory buys; a probe goes on to
//! the next group, wrapping, so it reaches every group of any count. The table never grows: one
//! with no bucket free refuses a new key ([`KeyIndex::put`]), and its memtable is packed.

// Group matching's constants are `util::incmap`'s, derived and cited there.
use crate::util::incmap::{EMPTY, FIB, GROUP, HIGH, LOW, repeat};

/// A group of buckets: their control bytes as one word, byte `b` for bucket `b`, beside their
/// entry offsets, in one cache line, so the line a probe reads is the one an insert then writes
/// (with the words and the entries in two arrays, an insert's entry missed in a second line,
/// about two thirds of what a put spent in the index: research/54 §2). Aligned to 64 bytes, a
/// group lies in one line where lines are 64 bytes (x86-64, most aarch64) or 128 (Apple's cores).
#[derive(Clone, Copy, Debug)]
#[repr(C, align(64))]
struct Group {
    ctrl: u64,
    slots: [u32; GROUP],
}

impl Group {
    const EMPTY: Self = Self {
        ctrl: repeat(EMPTY),
        slots: [0; GROUP],
    };
}

/// One table: the groups its memory buys, at least one.
#[derive(Debug)]
struct Table {
    groups: Vec<Group>,
    full: usize,
}

impl Default for Table {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Table {
    fn new(groups: usize) -> Self {
        Self {
            groups: vec![Group::EMPTY; groups.max(1)],
            full: 0,
        }
    }

    fn buckets(&self) -> usize {
        self.groups.len().saturating_mul(GROUP)
    }

    /// Group `g`'s control word.
    #[inline]
    fn word(&self, g: usize) -> u64 {
        self.groups.get(g).map_or(0, |group| group.ctrl)
    }

    /// Bucket `i`'s entry.
    #[inline]
    fn slot(&self, i: usize) -> Option<u32> {
        self.groups
            .get(i / GROUP)
            .and_then(|group| group.slots.get(i % GROUP))
            .copied()
    }

    #[inline]
    fn slot_mut(&mut self, i: usize) -> Option<&mut u32> {
        self.groups
            .get_mut(i / GROUP)
            .and_then(|group| group.slots.get_mut(i % GROUP))
    }

    /// A hash's tag, its top 7 bits after mixing, and its first group: the 57 bits below the tag
    /// times the count of groups, the product's high word.
    #[inline]
    fn place(&self, hash: u64) -> (u8, usize) {
        let m = hash.wrapping_mul(FIB);
        let tag = u8::try_from(m >> 57).unwrap_or(0);
        let groups = u128::try_from(self.groups.len()).unwrap_or(u128::MAX);
        let group = usize::try_from(u128::from(m << 7).wrapping_mul(groups) >> 64).unwrap_or(0);
        (tag, group)
    }

    /// The bucket holding an entry whose key `same` accepts, or else the first empty bucket the
    /// probe passed.
    #[inline]
    fn seek(&self, hash: u64, same: impl Fn(u32) -> bool) -> Result<usize, Option<usize>> {
        let (tag, mut g) = self.place(hash);
        let pattern = repeat(tag);
        let mut free = None;
        // Each round reads a group, the next after it: every group once.
        for _ in 0..self.groups.len() {
            let w = self.word(g);
            let at = g.wrapping_mul(GROUP);
            let cmp = w ^ pattern;
            let mut m = cmp.wrapping_sub(LOW) & !cmp & HIGH;
            while m != 0 {
                let i = at.wrapping_add(usize::try_from(m.trailing_zeros() / 8).unwrap_or(0));
                if self.slot(i).is_some_and(&same) {
                    return Ok(i);
                }
                m &= m.wrapping_sub(1);
            }
            if free.is_none() && w & HIGH != 0 {
                let b = usize::try_from((w & HIGH).trailing_zeros() / 8).unwrap_or(0);
                free = Some(at.wrapping_add(b));
            }
            // An empty bucket ends the probe: the key would have been placed by then.
            if w & (w << 1) & HIGH != 0 {
                return Err(free);
            }
            g = g.wrapping_add(1);
            if g == self.groups.len() {
                g = 0;
            }
        }
        Err(free)
    }

    fn set_ctrl(&mut self, i: usize, c: u8) {
        let shift = u32::try_from(i % GROUP).unwrap_or(0).wrapping_mul(8);
        if let Some(group) = self.groups.get_mut(i / GROUP) {
            let w = &mut group.ctrl;
            *w = (*w & !0xFFu64.wrapping_shl(shift)) | u64::from(c).wrapping_shl(shift);
        }
    }

    fn is_full(&self, i: usize) -> bool {
        let shift = u32::try_from(i % GROUP).unwrap_or(0).wrapping_mul(8);
        self.groups
            .get(i / GROUP)
            .is_some_and(|group| group.ctrl.wrapping_shr(shift) & 0x80 == 0)
    }

    /// Puts `entry` in the bucket an entry `same` accepts holds, or a new one: the entry it
    /// replaced. `None` with no bucket free.
    fn put(&mut self, hash: u64, entry: u32, same: impl Fn(u32) -> bool) -> Option<Option<u32>> {
        match self.seek(hash, same) {
            Ok(i) => {
                let e = self.slot_mut(i)?;
                Some(Some(std::mem::replace(e, entry)))
            }
            Err(free) => {
                let at = free?;
                let (tag, _) = self.place(hash);
                self.set_ctrl(at, tag);
                *self.slot_mut(at)? = entry;
                self.full = self.full.saturating_add(1);
                Some(None)
            }
        }
    }
}

/// The index: one table, its size set when it is made.
#[derive(Debug, Default)]
pub struct KeyIndex {
    table: Table,
}

impl KeyIndex {
    /// An index of at least `entries` buckets and the groups `extra` bytes buy beside them.
    pub fn sized(entries: usize, extra: usize) -> Self {
        Self {
            table: Table::new(Self::groups_for(entries, extra)),
        }
    }

    /// The groups an index of at least `entries` buckets and `extra` bytes more takes.
    fn groups_for(entries: usize, extra: usize) -> usize {
        entries
            .div_ceil(GROUP)
            .saturating_add(extra.checked_div(size_of::<Group>()).unwrap_or(0))
    }

    /// Whether [`Self::sized`] with these would make the table this one has.
    pub fn is_sized(&self, entries: usize, extra: usize) -> bool {
        self.table.groups.len() == Self::groups_for(entries, extra).max(1)
    }

    /// Reads `hash`'s first group, the line holding its control word and its entries, so a probe
    /// made after finds it cached: several indexes' misses overlapped rather than one after
    /// another (the shard's kept memtables, research/51).
    #[inline]
    pub fn touch(&self, hash: u64) -> u64 {
        let (_, g) = self.table.place(hash);
        self.table.word(g)
    }

    /// Each key's newest entry, in no order.
    pub fn each_entry(&self, mut each: impl FnMut(u32)) {
        for i in 0..self.table.buckets() {
            if self.table.is_full(i)
                && let Some(e) = self.table.slot(i)
            {
                each(e);
            }
        }
    }

    /// Empties the index, keeping its table.
    pub fn clear(&mut self) {
        for group in &mut self.table.groups {
            group.ctrl = repeat(EMPTY);
        }
        self.table.full = 0;
    }

    /// The bytes its table takes.
    pub fn bytes(&self) -> usize {
        self.table.groups.len().saturating_mul(size_of::<Group>())
    }

    /// The buckets its table has.
    pub fn buckets(&self) -> usize {
        self.table.buckets()
    }

    /// Whether a new key has a bucket.
    pub fn has_room(&self) -> bool {
        self.table.full < self.table.buckets()
    }

    /// The newest entry of a key of hash `hash` that `same` accepts.
    #[inline]
    pub fn get(&self, hash: u64, same: impl Fn(u32) -> bool) -> Option<u32> {
        self.table
            .seek(hash, &same)
            .ok()
            .and_then(|i| self.table.slot(i))
    }

    /// Makes `entry` the newest of its key, of hash `hash`: `Some` of the entry it replaced, if
    /// the key had one; `None`, nothing changed, for a new key with no bucket free. `same` accepts
    /// the entries of the same key.
    pub fn put(
        &mut self,
        hash: u64,
        entry: u32,
        same: impl Fn(u32) -> bool,
    ) -> Option<Option<u32>> {
        self.table.put(hash, entry, &same)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Keys as offsets into a list standing for the arena; a key's hash chosen by the test, so
    /// keys of one hash are distinct entries. Tables of counts of groups that are not powers of
    /// two, from one group, filled to every bucket.
    #[test]
    fn it_answers_as_a_map_does_to_its_last_bucket_for_any_count_of_groups() {
        let hash_of_key = |k: u64| {
            if k.is_multiple_of(7) {
                42
            } else {
                k.wrapping_mul(0x2545_f491)
            }
        };
        for groups in [1usize, 3, 7, 100, 1000, 4097] {
            let buckets = groups * GROUP;
            let mut index = KeyIndex::sized(buckets, 0);
            assert_eq!(index.buckets(), buckets);
            let mut keys: Vec<u64> = Vec::new();
            let mut model: HashMap<u64, u32> = HashMap::new();
            let mut x = 0x9e37_79b9_7f4a_7c15u64 ^ groups as u64;
            while model.len() < buckets {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                // Keys drawn from twice the buckets: repeats replace, new ones take a bucket.
                let k = x % (2 * buckets as u64);
                let entry = u32::try_from(keys.len()).unwrap();
                keys.push(k);
                let was = index.put(hash_of_key(k), entry, |e| keys[e as usize] == k);
                assert_eq!(was, Some(model.insert(k, entry)), "groups {groups}");
                if x.is_multiple_of(5) {
                    let probe = (x >> 20) % (2 * buckets as u64);
                    let got = index.get(hash_of_key(probe), |e| keys[e as usize] == probe);
                    assert_eq!(got, model.get(&probe).copied(), "groups {groups}");
                }
            }
            assert!(!index.has_room());
            // Full: a new key is refused, changing nothing; a held key is still replaced.
            let absent = 2 * buckets as u64 + 1;
            keys.push(absent);
            let entry = u32::try_from(keys.len() - 1).unwrap();
            assert_eq!(
                index.put(hash_of_key(absent), entry, |e| keys[e as usize] == absent),
                None
            );
            let (&held, _) = model.iter().next().unwrap();
            keys.push(held);
            let entry = u32::try_from(keys.len() - 1).unwrap();
            assert!(
                index
                    .put(hash_of_key(held), entry, |e| keys[e as usize] == held)
                    .is_some_and(|was| was.is_some())
            );
            model.insert(held, entry);
            for (&k, &e) in &model {
                assert_eq!(
                    index.get(hash_of_key(k), |f| keys[f as usize] == k),
                    Some(e)
                );
            }
            assert_eq!(
                index.get(hash_of_key(absent), |e| keys[e as usize] == absent),
                None
            );
            let mut listed = 0;
            index.each_entry(|_| listed += 1);
            assert_eq!(listed, model.len());
            index.clear();
            assert!(index.has_room());
            assert_eq!(index.buckets(), buckets);
        }
    }

    /// The groups an index is sized to: the buckets its entries need, a group's line each, and
    /// what the extra bytes buy.
    #[test]
    fn it_takes_the_groups_its_entries_and_its_extra_bytes_buy() {
        assert_eq!(KeyIndex::sized(0, 0).buckets(), GROUP);
        assert_eq!(KeyIndex::sized(1, 0).buckets(), GROUP);
        assert_eq!(KeyIndex::sized(GROUP + 1, 0).buckets(), 2 * GROUP);
        let line = size_of::<Group>();
        assert_eq!(KeyIndex::sized(GROUP, 3 * line).buckets(), 4 * GROUP);
        assert_eq!(
            KeyIndex::sized(GROUP, 3 * line + line - 1).buckets(),
            4 * GROUP
        );
        assert_eq!(KeyIndex::sized(GROUP, 3 * line).bytes(), 4 * line);
        assert!(KeyIndex::sized(5 * GROUP, 2 * line).is_sized(5 * GROUP, 2 * line));
        assert!(!KeyIndex::sized(5 * GROUP, 2 * line).is_sized(5 * GROUP, 3 * line));
    }
}
