//! A map of `u64` to `u64` that grows without stopping an operation to rehash it whole.
//!
//! A cache's index grows with the cache, on the read path: a table that doubles at once rehashes
//! every entry in one get, and touches a new table's pages there (measured: 546 to 2178 page
//! faults, 1.3 to 5.2 ms, in single gets of a 64 MiB record cache filling; research/37). Here a
//! full table starts a new one and each write then moves `MOVE` of the old table's buckets, as
//! Redis's dict rehashes a step an operation (`dict.c`, `_dictRehashStep`), so no single
//! operation pays for more than that.
//!
//! Open addressing with linear probing; a key's home bucket by Fibonacci hashing (Knuth, TAOCP
//! vol. 3, §6.4). Removal leaves a tombstone, so a migration or a sweep never moves an entry it has
//! not reached; tombstones are dropped when the table is rebuilt, which they count toward.

/// Buckets of the old table moved each write while a migration runs. Derived: a migration from
/// an old table of `C` buckets takes `C / MOVE` writes, each adding at most an entry and a
/// tombstone to the new table, so the new table ends it holding at most `len + C / 4` for
/// `MOVE = 8`. Built with `N >= 8/7 (len + 1 + C / 4)` buckets, it stays under its rebuild
/// threshold, `7/8` of `N`: a migration always ends before the next begins.
const MOVE: usize = 8;
/// Fibonacci hashing's multiplier, 2^64 over the golden ratio (Knuth, TAOCP vol. 3, §6.4).
const FIB: u64 = 0x9E37_79B9_7F4A_7C15;

const EMPTY: u8 = 0;
const FULL: u8 = 1;
const TOMB: u8 = 2;

/// One open-addressing table: a power-of-two count of buckets.
#[derive(Debug, Default)]
struct Table {
    ctrl: Vec<u8>,
    keys: Vec<u64>,
    vals: Vec<u64>,
    full: usize,
    tombs: usize,
    shift: u32,
}

impl Table {
    /// A table of `buckets` buckets, a power of two. Zeroed memory: its pages are touched as
    /// entries land in them, not when it is made.
    fn new(buckets: usize) -> Self {
        Self {
            ctrl: vec![EMPTY; buckets],
            keys: vec![0; buckets],
            vals: vec![0; buckets],
            full: 0,
            tombs: 0,
            shift: 64u32.saturating_sub(buckets.trailing_zeros()),
        }
    }

    fn buckets(&self) -> usize {
        self.ctrl.len()
    }

    fn home(&self, key: u64) -> usize {
        usize::try_from(key.wrapping_mul(FIB).checked_shr(self.shift).unwrap_or(0)).unwrap_or(0)
    }

    fn next(&self, i: usize) -> usize {
        i.wrapping_add(1) & self.buckets().wrapping_sub(1)
    }

    /// The bucket holding `key`.
    fn find(&self, key: u64) -> Option<usize> {
        let mut i = self.home(key);
        // Each step passes a bucket: at most all of them.
        for _ in 0..self.buckets() {
            match self.ctrl.get(i).copied()? {
                EMPTY => return None,
                FULL if self.keys.get(i).copied()? == key => return Some(i),
                _ => i = self.next(i),
            }
        }
        None
    }

    fn get(&self, key: u64) -> Option<u64> {
        self.find(key).and_then(|i| self.vals.get(i).copied())
    }

    /// Sets `key` to `val`; the value it replaced.
    fn insert(&mut self, key: u64, val: u64) -> Option<u64> {
        let mut i = self.home(key);
        let mut slot = None;
        // Each step passes a bucket: at most all of them.
        for _ in 0..self.buckets() {
            match self.ctrl.get(i).copied()? {
                EMPTY => {
                    slot.get_or_insert(i);
                    break;
                }
                FULL if self.keys.get(i).copied()? == key => {
                    let v = self.vals.get_mut(i)?;
                    return Some(std::mem::replace(v, val));
                }
                TOMB => {
                    slot.get_or_insert(i);
                    i = self.next(i);
                }
                _ => i = self.next(i),
            }
        }
        let at = slot?;
        let c = self.ctrl.get_mut(at)?;
        if *c == TOMB {
            self.tombs = self.tombs.saturating_sub(1);
        }
        *c = FULL;
        *self.keys.get_mut(at)? = key;
        *self.vals.get_mut(at)? = val;
        self.full = self.full.saturating_add(1);
        None
    }

    /// Takes the entry at bucket `i`, leaving a tombstone.
    fn take(&mut self, i: usize) -> Option<(u64, u64)> {
        let c = self.ctrl.get_mut(i)?;
        if *c != FULL {
            return None;
        }
        *c = TOMB;
        self.full = self.full.saturating_sub(1);
        self.tombs = self.tombs.saturating_add(1);
        Some((self.keys.get(i).copied()?, self.vals.get(i).copied()?))
    }

    fn remove(&mut self, key: u64) -> Option<u64> {
        let i = self.find(key)?;
        self.take(i).map(|(_, v)| v)
    }

    /// Whether one more entry passes `7/8` of the buckets, tombstones counted.
    fn crowded(&self) -> bool {
        self.full
            .saturating_add(self.tombs)
            .saturating_add(1)
            .saturating_mul(8)
            > self.buckets().saturating_mul(7)
    }
}

/// The map: its table, the old one a migration is emptying, and how far it has come.
#[derive(Debug, Default)]
pub struct IncMap {
    table: Table,
    old: Option<Table>,
    moved: usize,
    /// The next bucket a sweep looks at.
    swept: usize,
}

impl IncMap {
    /// An empty map; its first table is made with its first entry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Entries held.
    pub fn len(&self) -> usize {
        self.table
            .full
            .saturating_add(self.old.as_ref().map_or(0, |o| o.full))
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, key: u64) -> Option<u64> {
        self.table
            .get(key)
            .or_else(|| self.old.as_ref().and_then(|o| o.get(key)))
    }

    pub fn contains_key(&self, key: u64) -> bool {
        self.get(key).is_some()
    }

    /// Sets `key` to `val`; the value it replaced.
    pub fn insert(&mut self, key: u64, val: u64) -> Option<u64> {
        self.step();
        if self.table.buckets() == 0 || self.table.crowded() {
            // A migration still running here would break `MOVE`'s bound: it is finished first
            // rather than lose the entry, whatever it costs.
            while self.old.is_some() {
                self.step();
            }
            self.rebuild();
        }
        // After any rebuild: the key may be in the table that just became the old one.
        let was = self.old.as_mut().and_then(|o| o.remove(key));
        self.table.insert(key, val).or(was)
    }

    /// Removes `key`; its value.
    pub fn remove(&mut self, key: u64) -> Option<u64> {
        self.step();
        let old = self.old.as_mut().and_then(|o| o.remove(key));
        self.table.remove(key).or(old)
    }

    /// Looks at the next `n` buckets of the table, from where the last sweep stopped, and removes
    /// the entries `keep` refuses: a bounded pass a write, for entries that expire.
    pub fn sweep(&mut self, n: usize, keep: impl Fn(u64, u64) -> bool) {
        let buckets = self.table.buckets();
        if buckets == 0 {
            return;
        }
        for _ in 0..n.min(buckets) {
            let i = self.swept & buckets.wrapping_sub(1);
            self.swept = i.wrapping_add(1);
            let refused = self.table.ctrl.get(i) == Some(&FULL)
                && self
                    .table
                    .keys
                    .get(i)
                    .zip(self.table.vals.get(i))
                    .is_some_and(|(&k, &v)| !keep(k, v));
            if refused {
                self.table.take(i);
            }
        }
    }

    /// Starts a migration to a new table sized for the entries held and the writes the
    /// migration spans (see `MOVE`).
    fn rebuild(&mut self) {
        let c = self.table.buckets();
        // At most half full when built, and room for the migration's writes (see `MOVE`).
        let len = self.table.full.saturating_add(1);
        let want = len
            .saturating_mul(2)
            .max((len.saturating_add(c / 4).saturating_mul(8) / 7).saturating_add(1));
        let buckets = want.checked_next_power_of_two().unwrap_or(want);
        let old = std::mem::replace(&mut self.table, Table::new(buckets));
        self.swept = 0;
        if old.full > 0 {
            self.old = Some(old);
            self.moved = 0;
        }
    }

    /// Moves the next `MOVE` buckets of the old table, ending the migration at its last.
    fn step(&mut self) {
        let Some(old) = self.old.as_mut() else {
            return;
        };
        for _ in 0..MOVE {
            if self.moved >= old.buckets() || old.full == 0 {
                self.old = None;
                return;
            }
            if let Some((k, v)) = old.take(self.moved) {
                self.table.insert(k, v);
            }
            self.moved = self.moved.saturating_add(1);
        }
    }

    /// Every entry, in no order.
    #[cfg(test)]
    pub fn entries(&self) -> Vec<(u64, u64)> {
        let mut all = Vec::new();
        for t in std::iter::once(&self.table).chain(self.old.iter()) {
            for i in 0..t.buckets() {
                if t.ctrl.get(i) == Some(&FULL) {
                    all.push((t.keys[i], t.vals[i]));
                }
            }
        }
        all
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn it_answers_as_a_map_does_through_every_migration() {
        let mut m = IncMap::new();
        let mut model: HashMap<u64, u64> = HashMap::new();
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let mut migrations = 0;
        for step in 0..400_000u64 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            // Keys from a range that grows then shrinks: the map grows, fills with tombstones,
            // and rebuilds smaller.
            let span = 1 + (step % 100_000).min(100_000 - step % 100_000);
            let key = x % span;
            let had = m.old.is_some();
            match x >> 60 {
                0..=9 => assert_eq!(m.insert(key, step), model.insert(key, step), "{step}"),
                10..=13 => assert_eq!(m.remove(key), model.remove(&key), "{step}"),
                _ => assert_eq!(m.get(key), model.get(&key).copied(), "{step}"),
            }
            assert_eq!(m.len(), model.len(), "{step}");
            if !had && m.old.is_some() {
                migrations += 1;
            }
            // A migration ends before the table it fills is crowded.
            assert!(m.old.is_none() || !m.table.crowded(), "{step}");
        }
        // Grown from 16 buckets to 64 Ki, migrating each time while the model was checked.
        assert!(migrations >= 6, "only {migrations} migrations");
        let mut all: Vec<(u64, u64)> = m.entries();
        all.sort_unstable();
        let mut want: Vec<(u64, u64)> = model.into_iter().collect();
        want.sort_unstable();
        assert_eq!(all, want);
    }

    #[test]
    fn no_write_moves_more_than_its_share() {
        // Growing to a million entries: each insert moves at most `MOVE` old buckets, and every
        // migration has ended before the table is crowded again.
        let mut m = IncMap::new();
        for k in 0..1_000_000u64 {
            let before = m.moved;
            let migrating = m.old.is_some();
            m.insert(k, k);
            if migrating && m.old.is_some() {
                assert!(m.moved - before <= MOVE);
            }
            assert!(m.old.is_none() || !m.table.crowded(), "{k}");
        }
        assert!((0..1_000_000u64).all(|k| m.get(k) == Some(k)));
    }

    #[test]
    fn tombstones_crowding_a_table_rebuild_it_smaller() {
        let mut m = IncMap::new();
        for k in 0..100_000u64 {
            m.insert(k, k);
        }
        for k in 0..100_000u64 {
            m.remove(k);
        }
        let big = m.table.buckets();
        // A few keys churned: each new one lands on fresh buckets until tombstones crowd the
        // table, which then rebuilds for what it holds.
        for k in 100_000..400_000u64 {
            m.insert(k, k);
            m.remove(k.saturating_sub(4));
        }
        assert!(
            m.table.buckets() < big / 16,
            "{} of {big}",
            m.table.buckets()
        );
        assert_eq!(m.len(), 4);
    }

    #[test]
    fn a_sweep_removes_what_it_refuses_and_keeps_the_rest() {
        let mut m = IncMap::new();
        for k in 0..1000u64 {
            m.insert(k, k);
        }
        while m.old.is_some() {
            m.insert(5000, 5000);
            m.remove(5000);
        }
        m.sweep(m.table.buckets(), |_, v| v % 2 == 0);
        let mut left: Vec<u64> = m.entries().into_iter().map(|(k, _)| k).collect();
        left.sort_unstable();
        assert_eq!(left, (0..1000).step_by(2).collect::<Vec<_>>());
    }
}
