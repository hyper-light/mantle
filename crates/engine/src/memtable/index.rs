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
//! of the hash, read eight at a time; hashbrown's portable group). A key has two groups, each from
//! its own half of the hash's bits below the tag, multiplied into the count of groups and shifted
//! down (multiply-shift range reduction: Lemire, "Fast Random Integer Generation in an Interval",
//! ACM TOMACS 29(1), 2019), so a table holds any count of groups its memory buys. A new key takes
//! its first group while that has a bucket free, and only once it is full the emptier of two more,
//! marking the first as overflowed, in its line: as folly's F14 counts the keys that overflowed a
//! chunk so a lookup stops at one none did (Bronson and Shi, "Open-sourcing F14", Engineering at
//! Meta, 2019), and as Iceberg hashing keeps most keys in a front yard of one choice and balances
//! the overflow between two (Bender, Conway, Farach-Colton, Kuszmaul and Tagliavini, "Iceberg
//! Hashing", J. ACM 70(6), 2023). The two are the halves of a second hash, SplitMix64's finalizer
//! of the first (Steele, Lea and Flood, OOPSLA 2014). A group only fills, so a marked group is full
//! and an unmarked one sent no key away: a lookup reads a key's first group, and its two others
//! only when the first is marked, three lines at most and one while the first never overflowed
//! (research/54 §6). The table never grows: a key whose first group and both others are full is
//! refused ([`KeyIndex::put`]), and its memtable is packed.

// Group matching's constants are `util::incmap`'s, derived and cited there.
use crate::memtable::inlineskiplist::{MIX_1, MIX_2};
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
    /// A key whose first group this is went to its second: set once this group was full.
    overflowed: bool,
}

impl Group {
    const EMPTY: Self = Self {
        ctrl: repeat(EMPTY),
        slots: [0; GROUP],
        overflowed: false,
    };
}

/// The bits of the second hash each of a key's overflow groups is taken from: half its word.
const HALF: u32 = u64::BITS / 2;
/// The most groups a table holds: an overflow group's bits times the count of groups stays in a
/// word.
const MOST_GROUPS: u64 = 1 << HALF;

/// One table: the groups its memory buys, at least one and at most [`MOST_GROUPS`].
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
            groups: vec![
                Group::EMPTY;
                groups.clamp(1, usize::try_from(MOST_GROUPS).unwrap_or(usize::MAX))
            ],
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

    /// A hash's tag, its top 7 bits after mixing; its first group, the 57 bits below the tag times
    /// the count of groups, the product's high word; and its two overflow groups, each a half of
    /// the second hash times the count of groups, shifted down by the half's bits.
    #[inline]
    fn place(&self, hash: u64) -> (u8, usize, usize, usize) {
        let m = hash.wrapping_mul(FIB);
        let tag = u8::try_from(m >> 57).unwrap_or(0);
        let groups = u64::try_from(self.groups.len()).unwrap_or(0);
        let first =
            usize::try_from(u128::from(m << 7).wrapping_mul(u128::from(groups)) >> 64).unwrap_or(0);
        let mut z = hash;
        z = (z ^ (z >> 30)).wrapping_mul(MIX_1);
        z = (z ^ (z >> 27)).wrapping_mul(MIX_2);
        z ^= z >> 31;
        let half = (1u64 << HALF).wrapping_sub(1);
        let group =
            |bits: u64| usize::try_from((bits & half).wrapping_mul(groups) >> HALF).unwrap_or(0);
        (tag, first, group(z), group(z >> HALF))
    }

    /// The bucket in group `g` (control word `w`) holding an entry whose key `same` accepts.
    #[inline]
    fn found(&self, g: usize, w: u64, pattern: u64, same: &impl Fn(u32) -> bool) -> Option<usize> {
        let at = g.wrapping_mul(GROUP);
        let cmp = w ^ pattern;
        let mut m = cmp.wrapping_sub(LOW) & !cmp & HIGH;
        while m != 0 {
            let i = at.wrapping_add(usize::try_from(m.trailing_zeros() / 8).unwrap_or(0));
            if self.slot(i).is_some_and(same) {
                return Some(i);
            }
            m &= m.wrapping_sub(1);
        }
        None
    }

    /// Whether group `g` sent a key to that key's second group.
    #[inline]
    fn marked(&self, g: usize) -> bool {
        self.groups.get(g).is_some_and(|group| group.overflowed)
    }

    /// The first free bucket of group `g`, whose control word is `w`.
    #[inline]
    fn free(g: usize, w: u64) -> Option<usize> {
        (w & HIGH != 0).then(|| {
            let b = usize::try_from((w & HIGH).trailing_zeros() / 8).unwrap_or(0);
            g.wrapping_mul(GROUP).wrapping_add(b)
        })
    }

    /// The bucket holding an entry whose key `same` accepts: its first group, and its overflow
    /// groups only when the first is marked, both their words read before either is matched.
    #[inline]
    fn find(&self, hash: u64, same: impl Fn(u32) -> bool) -> Option<usize> {
        let (tag, g1, g2, g3) = self.place(hash);
        let pattern = repeat(tag);
        if let Some(i) = self.found(g1, self.word(g1), pattern, &same) {
            return Some(i);
        }
        if !self.marked(g1) {
            return None;
        }
        let (w2, w3) = (self.word(g2), self.word(g3));
        let other = |g: usize, w: u64| {
            if g == g1 {
                None
            } else {
                self.found(g, w, pattern, &same)
            }
        };
        other(g2, w2).or_else(|| if g3 == g2 { None } else { other(g3, w3) })
    }

    /// The bucket holding an entry whose key `same` accepts; or else where a new one goes, the
    /// first free bucket of its first group, or once that is full of its second with the first
    /// to be marked; none when both are full.
    #[inline]
    fn seek(
        &self,
        hash: u64,
        same: impl Fn(u32) -> bool,
    ) -> Result<usize, Option<(usize, Option<usize>)>> {
        let (tag, g1, g2, g3) = self.place(hash);
        let pattern = repeat(tag);
        let w1 = self.word(g1);
        if let Some(i) = self.found(g1, w1, pattern, &same) {
            return Ok(i);
        }
        // A first group with a bucket free was never full, so never marked: the key is nowhere.
        if let Some(at) = Self::free(g1, w1) {
            return Err(Some((at, None)));
        }
        let (w2, w3) = (self.word(g2), self.word(g3));
        // The overflow groups other than the first, each once.
        let two = [(g2, w2), (g3, w3)];
        let others = two
            .iter()
            .enumerate()
            .filter(|&(k, &(g, _))| g != g1 && (k == 0 || g != g2))
            .map(|(_, &gw)| gw);
        if self.marked(g1) {
            for (g, w) in others.clone() {
                if let Some(i) = self.found(g, w, pattern, &same) {
                    return Ok(i);
                }
            }
        }
        // The emptier of the overflow groups with a bucket free.
        let empty = |w: u64| (w & HIGH).count_ones();
        let pick = others.fold(None, |best: Option<(usize, u64)>, (g, w)| match best {
            Some((_, bw)) if empty(bw) >= empty(w) => best,
            _ => Some((g, w)),
        });
        Err(pick
            .and_then(|(g, w)| Self::free(g, w))
            .map(|at| (at, Some(g1))))
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
    /// replaced. `None` with both of the key's groups full.
    fn put(&mut self, hash: u64, entry: u32, same: impl Fn(u32) -> bool) -> Option<Option<u32>> {
        match self.seek(hash, same) {
            Ok(i) => {
                let e = self.slot_mut(i)?;
                Some(Some(std::mem::replace(e, entry)))
            }
            Err(free) => {
                let (at, overflowed) = free?;
                let (tag, _, _, _) = self.place(hash);
                self.set_ctrl(at, tag);
                *self.slot_mut(at)? = entry;
                if let Some(group) = overflowed.and_then(|g| self.groups.get_mut(g)) {
                    group.overflowed = true;
                }
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
        self.table.groups.len()
            == Self::groups_for(entries, extra)
                .clamp(1, usize::try_from(MOST_GROUPS).unwrap_or(usize::MAX))
    }

    /// Reads `hash`'s two groups, the lines holding their control words and entries, so a probe
    /// made after finds them cached: several indexes' misses overlapped rather than one after
    /// another (the shard's kept memtables, research/51).
    #[inline]
    pub fn touch(&self, hash: u64) -> u64 {
        let (_, g1, g2, g3) = self.table.place(hash);
        let w = self.table.word(g1);
        if self.table.marked(g1) {
            w ^ self.table.word(g2) ^ self.table.word(g3)
        } else {
            w
        }
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
            group.overflowed = false;
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

    /// The newest entry of a key of hash `hash` that `same` accepts.
    #[inline]
    pub fn get(&self, hash: u64, same: impl Fn(u32) -> bool) -> Option<u32> {
        self.table
            .find(hash, &same)
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
    /// keys of one hash are distinct entries, and those sharing hash 42 share both groups. Tables
    /// of counts of groups that are not powers of two, from one group, drawn into four times
    /// their buckets over: a key is taken while one of its groups has a bucket and refused once
    /// both are full, changing nothing; what is held answers as a map does, a held key is still
    /// replaced, a refused one is never found.
    #[test]
    fn it_answers_as_a_map_does_and_refuses_a_key_both_of_whose_groups_are_full() {
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
            let mut refused: std::collections::HashSet<u64> = Default::default();
            let mut x = 0x9e37_79b9_7f4a_7c15u64 ^ groups as u64;
            for _ in 0..4 * buckets {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let k = x % (2 * buckets as u64);
                let entry = u32::try_from(keys.len()).unwrap();
                keys.push(k);
                match index.put(hash_of_key(k), entry, |e| keys[e as usize] == k) {
                    Some(was) => {
                        assert!(
                            !refused.contains(&k),
                            "groups {groups}: a refused key taken"
                        );
                        assert_eq!(was, model.insert(k, entry), "groups {groups}");
                    }
                    None => {
                        assert!(
                            !model.contains_key(&k),
                            "groups {groups}: a held key refused"
                        );
                        refused.insert(k);
                    }
                }
                if x.is_multiple_of(5) {
                    let probe = (x >> 20) % (2 * buckets as u64);
                    let got = index.get(hash_of_key(probe), |e| keys[e as usize] == probe);
                    assert_eq!(got, model.get(&probe).copied(), "groups {groups}");
                }
            }
            assert!(model.len() <= buckets);
            for (&k, &e) in &model {
                assert_eq!(
                    index.get(hash_of_key(k), |f| keys[f as usize] == k),
                    Some(e)
                );
            }
            for &k in &refused {
                assert_eq!(index.get(hash_of_key(k), |e| keys[e as usize] == k), None);
            }
            if let Some((&held, _)) = model.iter().next() {
                keys.push(held);
                let entry = u32::try_from(keys.len() - 1).unwrap();
                assert!(
                    index
                        .put(hash_of_key(held), entry, |e| keys[e as usize] == held)
                        .is_some_and(|was| was.is_some())
                );
            }
            let mut listed = 0;
            index.each_entry(|_| listed += 1);
            assert_eq!(listed, model.len());
            index.clear();
            assert_eq!(index.buckets(), buckets);
            let k = 1u64;
            keys.push(k);
            let entry = u32::try_from(keys.len() - 1).unwrap();
            assert_eq!(
                index.put(hash_of_key(k), entry, |e| keys[e as usize] == k),
                Some(None)
            );
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
