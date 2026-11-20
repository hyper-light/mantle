//! One entry's writes buffered over the engine (docs/design/replica.md §2): the commands of an
//! entry read what the commands before them wrote, and the engine takes the entry's writes as
//! one batch at its index.
//!
//! The writes are kept in storage the range keeps from one entry to the next ([`Writes`]), a skip
//! list (Pugh 1990, docs/research/59) over slots and links that are cleared, keeping their
//! capacity, as each entry ends: once the storage has held an entry as large, applying one
//! allocates nothing for the rows it writes beyond the rows themselves, the keys and values the
//! layer makes, moved in and on to the engine's batch.

use crate::engine::{EngineError, Row, Rows, Write};

/// The most links a row takes: Pugh's MaxLevel, log_{1/p} N, for his p = 1/4 and the N = 2^32
/// slots a `u32` link names.
const LEVELS: usize = 16;

/// Where each entry's draw of heights starts: the seed of Marsaglia's xor64 generator, with its
/// shifts 13, 7 and 17 ([`Writes::height`]). The same at every clearing, so an entry draws the
/// same heights for its slots in order, and one no larger than an entry held before takes no
/// more links.
const SEED: u64 = 88_172_645_463_325_252;

/// One row written: its key, and its value or `None` where the overlay deleted it.
#[derive(Debug)]
struct Slot {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    /// Where the slot's links begin in [`Writes::links`]: the next slot at each of its levels.
    links: u32,
    /// The levels the slot is linked at, at least one.
    height: u8,
}

/// One overlay's writes, in storage a range keeps from entry to entry: the keys written, in key
/// order, each with its value or its deletion, and the ranges cleared.
///
/// Bound: the slots, links and cleared ranges hold one entry's writes at a time, and every entry
/// is within the largest a range takes (`range.max_entry_bytes`). An entry that grows them past
/// four times the rows it wrote gives half its capacity back as it ends, at each entry until the
/// load is a quarter or more: Cormen et al.'s table contraction (Introduction to Algorithms,
/// §17.4), which keeps the amortized cost per row constant and never moves between growth and
/// contraction for entries of a steady size, so an outsized entry's room is given back over the
/// entries after it. The keys' and values' own bytes are the layer's buffers, moved in and out.
#[derive(Debug, Default)]
pub struct Writes {
    /// Every row written since the storage was last cleared. A row written again keeps its slot;
    /// one cleared stays, unlinked, until the storage is cleared.
    slots: Vec<Slot>,
    /// Each slot's links, `height` of them from its `links`.
    links: Vec<Option<u32>>,
    /// The first slot at each level.
    head: [Option<u32>; LEVELS],
    /// The ranges cleared, in order: no row beneath shows through them.
    cleared: Vec<(Vec<u8>, Vec<u8>)>,
    /// Marsaglia's xor64 state, each new slot's height drawn from it in turn (Xorshift RNGs,
    /// 2003): [`SEED`] at each clearing, zero before the first.
    draw: u64,
}

impl Writes {
    /// Empties the storage, keeping its capacity within the bound ([`Writes`]).
    fn clear(&mut self) {
        if self.slots.is_empty() && self.cleared.is_empty() {
            return;
        }
        contract(&mut self.slots);
        contract(&mut self.links);
        contract(&mut self.cleared);
        self.head = [None; LEVELS];
        self.draw = SEED;
    }

    fn slot(&self, at: u32) -> Option<&Slot> {
        self.slots.get(usize::try_from(at).ok()?)
    }

    /// Where the link of slot `from` at `level` is kept in `links`.
    fn link_at(&self, from: u32, level: usize) -> Option<usize> {
        let slot = self.slot(from)?;
        if level >= usize::from(slot.height) {
            return None;
        }
        usize::try_from(slot.links).ok()?.checked_add(level)
    }

    /// The slot after `from` (the head where `None`) at `level`.
    fn link(&self, from: Option<u32>, level: usize) -> Option<u32> {
        match from {
            None => self.head.get(level).copied().flatten(),
            Some(from) => {
                let at = self.link_at(from, level)?;
                self.links.get(at).copied().flatten()
            }
        }
    }

    fn set_link(&mut self, from: Option<u32>, level: usize, to: Option<u32>) {
        let place = match from {
            None => self.head.get_mut(level),
            Some(from) => match self.link_at(from, level) {
                Some(at) => self.links.get_mut(at),
                None => None,
            },
        };
        if let Some(place) = place {
            *place = to;
        }
    }

    /// The last slot linked at each level whose key is below `key`, `None` for the head. Each
    /// level is walked forward only, past slots of rising keys, so at most every slot once.
    fn before(&self, key: &[u8]) -> [Option<u32>; LEVELS] {
        let mut before = [None; LEVELS];
        let mut at = None;
        for level in (0..LEVELS).rev() {
            for _ in 0..=self.slots.len() {
                let Some(next) = self.link(at, level) else {
                    break;
                };
                match self.slot(next) {
                    Some(slot) if slot.key.as_slice() < key => at = Some(next),
                    _ => break,
                }
            }
            if let Some(place) = before.get_mut(level) {
                *place = at;
            }
        }
        before
    }

    /// The first slot linked whose key is `key` or after it.
    fn seek(&self, key: &[u8]) -> Option<u32> {
        let before = self.before(key);
        self.link(before.first().copied().flatten(), 0)
    }

    /// The row written at `key`, if one is.
    fn find(&self, key: &[u8]) -> Option<&Slot> {
        self.seek(key)
            .and_then(|at| self.slot(at))
            .filter(|slot| slot.key.as_slice() == key)
    }

    /// The first row written in `[from, to)` that the overlay did not delete.
    fn first_put(&self, from: &[u8], to: &[u8]) -> Option<(&[u8], &[u8])> {
        let mut at = self.seek(from);
        for _ in 0..self.slots.len() {
            let slot = self.slot(at?)?;
            if slot.key.as_slice() >= to {
                return None;
            }
            if let Some(value) = &slot.value {
                return Some((&slot.key, value));
            }
            at = self.link(at, 0);
        }
        None
    }

    /// A slot's height: one level, and one more for each pair of the draw's low bits that are
    /// zero, each level a quarter of the one below (Pugh's p = 1/4), at most [`LEVELS`].
    fn height(&mut self) -> u8 {
        if self.draw == 0 {
            self.draw = SEED;
        }
        let mut x = self.draw;
        x ^= x.wrapping_shl(13);
        x ^= x.wrapping_shr(7);
        x ^= x.wrapping_shl(17);
        self.draw = x;
        let levels = usize::try_from(x.trailing_zeros().wrapping_shr(1))
            .unwrap_or(LEVELS)
            .saturating_add(1)
            .min(LEVELS);
        u8::try_from(levels).unwrap_or(u8::MAX)
    }

    /// Writes `value` at `key`, `None` deleting it: a key written before takes the new value in
    /// its slot.
    fn put(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<(), EngineError> {
        let before = self.before(&key);
        let next = self.link(before.first().copied().flatten(), 0);
        if let Some(slot) = next
            .and_then(|at| usize::try_from(at).ok())
            .and_then(|at| self.slots.get_mut(at))
            && slot.key == key
        {
            slot.value = value;
            return Ok(());
        }
        let height = self.height();
        let at = u32::try_from(self.slots.len()).map_err(|_| EngineError::Memory)?;
        let links = u32::try_from(self.links.len()).map_err(|_| EngineError::Memory)?;
        let levels = usize::from(height);
        self.slots.try_reserve(1).map_err(|_| EngineError::Memory)?;
        self.links
            .try_reserve(levels)
            .map_err(|_| EngineError::Memory)?;
        for (level, &from) in before.iter().enumerate().take(levels) {
            let next = self.link(from, level);
            self.links.push(next);
        }
        self.slots.push(Slot {
            key,
            value,
            links,
            height,
        });
        for (level, &from) in before.iter().enumerate().take(levels) {
            self.set_link(from, level, Some(at));
        }
        Ok(())
    }

    /// Removes every row written in `[from, to)` and keeps the range, through which no row
    /// beneath shows. An empty range clears nothing.
    fn clear_range(&mut self, from: Vec<u8>, to: Vec<u8>) -> Result<(), EngineError> {
        if from >= to {
            return Ok(());
        }
        self.cleared
            .try_reserve(1)
            .map_err(|_| EngineError::Memory)?;
        let (first, last) = (self.before(&from), self.before(&to));
        // At each level the last slot below `from` now links to the first at or past `to`.
        for (level, (&below, &within)) in first.iter().zip(&last).enumerate() {
            let past = self.link(within, level);
            self.set_link(below, level, past);
        }
        self.cleared.push((from, to));
        Ok(())
    }

    /// The end of the cleared range holding `key`, if one does.
    fn cleared_past(&self, key: &[u8]) -> Option<&[u8]> {
        self.cleared
            .iter()
            .filter(|(from, to)| from.as_slice() <= key && key < to.as_slice())
            .map(|(_, to)| to.as_slice())
            .max()
    }

    /// The rows linked: written and not cleared since.
    fn rows(&self) -> usize {
        let first = self.head.first().copied().flatten();
        std::iter::successors(first, |&at| self.link(Some(at), 0))
            .take(self.slots.len())
            .count()
    }

    /// The rows linked, in key order, each moved out of its slot: `f` is given each key and its
    /// value or deletion.
    fn drain(
        &mut self,
        mut f: impl FnMut(Vec<u8>, Option<Vec<u8>>) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        let mut at = self.head.first().copied().flatten();
        for _ in 0..self.slots.len() {
            let Some(here) = at else {
                break;
            };
            at = self.link(Some(here), 0);
            let Some(slot) = usize::try_from(here)
                .ok()
                .and_then(|here| self.slots.get_mut(here))
            else {
                break;
            };
            f(std::mem::take(&mut slot.key), slot.value.take())?;
        }
        Ok(())
    }
}

/// Gives half of `v`'s capacity back where it holds less than a quarter of it, and empties it
/// ([`Writes`]'s bound).
fn contract<T>(v: &mut Vec<T>) {
    let (len, capacity) = (v.len(), v.capacity());
    v.clear();
    if len < capacity.wrapping_shr(2) {
        v.shrink_to(capacity.wrapping_shr(1));
    }
}

pub struct Overlay<'a, R> {
    rows: &'a mut R,
    /// A row's key and value at most: a batch holding a longer one is refused whole.
    bound: (usize, usize),
    /// Each key the entry wrote since it last cleared the key's range, and the ranges it cleared.
    writes: &'a mut Writes,
}

impl<'a, R: Rows> Overlay<'a, R> {
    /// An overlay over `rows` that takes no row of a key past `max_key` bytes or a value past
    /// `max_value`, its writes kept in `writes`, emptied of any an earlier pass left: a pass
    /// refused part way, its rows not in memory or no room for its batch, leaves nothing.
    pub fn new(
        rows: &'a mut R,
        (max_key, max_value): (usize, usize),
        writes: &'a mut Writes,
    ) -> Self {
        writes.clear();
        Self {
            rows,
            bound: (max_key, max_value),
            writes,
        }
    }

    /// The entry's writes: its cleared ranges, then one write per key written after, in key
    /// order, each row moved; the storage is emptied, keeping its capacity.
    pub fn into_writes(self) -> Result<Vec<Write>, EngineError> {
        let mut out = Vec::new();
        let len = self.writes.cleared.len().checked_add(self.writes.rows());
        out.try_reserve_exact(len.ok_or(EngineError::Memory)?)
            .map_err(|_| EngineError::Memory)?;
        out.extend(
            self.writes
                .cleared
                .iter_mut()
                .map(|(from, to)| Write::Clear(std::mem::take(from), std::mem::take(to))),
        );
        self.writes.drain(|k, v| {
            out.push(match v {
                Some(v) => Write::Put(k, v),
                None => Write::Delete(k),
            });
            Ok(())
        })?;
        self.writes.clear();
        Ok(out)
    }

    /// Takes `own`'s writes, one command's made over this overlay, as if they had been made here:
    /// its cleared ranges, then its rows, moved, nothing copied, and empties `own`. A command
    /// whose session keeps its answer has its writes taken so; one refused for room has them
    /// dropped (`apply::apply_entry`).
    pub fn absorb(&mut self, own: &mut Writes) -> Result<(), EngineError> {
        for (from, to) in &mut own.cleared {
            self.writes
                .clear_range(std::mem::take(from), std::mem::take(to))?;
        }
        let writes = &mut *self.writes;
        own.drain(|k, v| writes.put(k, v))?;
        own.clear();
        Ok(())
    }
}

impl<R: Rows> Rows for Overlay<'_, R> {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        match self.writes.find(key) {
            Some(slot) => Ok(slot.value.clone()),
            None if self.writes.cleared_past(key).is_some() => Ok(None),
            None => self.rows.get(key),
        }
    }

    fn next(&mut self, from: &[u8], to: &[u8]) -> Result<Option<Row>, EngineError> {
        if from >= to {
            return Ok(None);
        }
        let written = self
            .writes
            .first_put(from, to)
            .map(|(k, v)| (k.to_vec(), v.to_vec()));
        // The first row beneath that the entry has not deleted, replaced or cleared. Each step
        // past a row passes one of the entry's writes or leaves a cleared range, so the walk
        // ends.
        let mut at = from.to_vec();
        let beneath = loop {
            let Some((k, v)) = self.rows.next(&at, to)? else {
                break None;
            };
            if let Some(past) = self.writes.cleared_past(&k) {
                at = past.to_vec();
                continue;
            }
            if self.writes.find(&k).is_none() {
                break Some((k, v));
            }
            at = k;
            at.push(0);
        };
        Ok(match (written, beneath) {
            (Some(w), Some(b)) => Some(if w.0 <= b.0 { w } else { b }),
            (w, b) => w.or(b),
        })
    }

    fn apply(&mut self, _index: u64, writes: Vec<Write>) -> Result<(), EngineError> {
        let (max_key, max_value) = self.bound;
        if let Some((k, v)) = writes.iter().find_map(|w| match w {
            Write::Put(k, v) if k.len() > max_key || v.len() > max_value => Some((k, v)),
            _ => None,
        }) {
            return Err(EngineError::RowTooLarge {
                key: k.len(),
                value: v.len(),
            });
        }
        for write in writes {
            match write {
                Write::Put(k, v) => self.writes.put(k, Some(v))?,
                Write::Delete(k) => self.writes.put(k, None)?,
                Write::Clear(from, to) => self.writes.clear_range(from, to)?,
            }
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Model;
    use proptest::prelude::*;

    /// A batch holding a row past the bound on a key or a value is refused whole: none of its
    /// writes shows, and the overlay takes the next batch.
    #[test]
    fn a_batch_holding_a_row_past_its_bound_is_refused_whole() {
        let mut engine = Model::default();
        let mut writes = Writes::default();
        let mut overlay = Overlay::new(&mut engine, (2, 3), &mut writes);
        let put = |k: &[u8], v: &[u8]| Write::Put(k.to_vec(), v.to_vec());
        for row in [put(b"abc", b"v"), put(b"k", b"long")] {
            assert!(matches!(
                overlay.apply(1, vec![put(b"a", b"1"), row]),
                Err(EngineError::RowTooLarge { .. })
            ));
            assert_eq!(overlay.get(b"a").unwrap(), None);
        }
        overlay.apply(1, vec![put(b"ab", b"xyz")]).unwrap();
        assert_eq!(overlay.get(b"ab").unwrap(), Some(b"xyz".to_vec()));
    }

    fn key() -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(0u8..4, 0..3)
    }

    fn write() -> impl Strategy<Value = Write> {
        prop_oneof![
            3 => (key(), key()).prop_map(|(k, v)| Write::Put(k, v)),
            2 => key().prop_map(Write::Delete),
            1 => (key(), key()).prop_map(|(from, to)| Write::Clear(from, to)),
        ]
    }

    proptest! {
        /// A command's writes made over the entry's overlay and taken into it read as they would
        /// had the command written there itself, and give the entry the same writes.
        #[test]
        fn a_commands_writes_taken_read_as_written_in_place(
            before in prop::collection::vec(write(), 0..12),
            entry in prop::collection::vec(write(), 0..6),
            command in prop::collection::vec(prop::collection::vec(write(), 0..4), 0..3),
            from in key(),
            to in key(),
        ) {
            let mut engine = Model::default();
            engine.apply(1, before).unwrap();
            let mut other = engine.clone();
            let bound = (usize::MAX, usize::MAX);
            let (mut a, mut b, mut c) = (Writes::default(), Writes::default(), Writes::default());
            let mut direct = Overlay::new(&mut engine, bound, &mut a);
            let mut taken = Overlay::new(&mut other, bound, &mut b);
            direct.apply(2, entry.clone()).unwrap();
            taken.apply(2, entry).unwrap();
            {
                let mut own = Overlay::new(&mut taken, bound, &mut c);
                for writes in &command {
                    own.apply(2, writes.clone()).unwrap();
                }
                taken.absorb(&mut c).unwrap();
            }
            for writes in command {
                direct.apply(2, writes).unwrap();
            }
            for k in [from.clone(), to.clone()] {
                prop_assert_eq!(direct.get(&k).unwrap(), taken.get(&k).unwrap());
            }
            prop_assert_eq!(direct.next(&from, &to).unwrap(), taken.next(&from, &to).unwrap());
            let (a, b) = (direct.into_writes().unwrap(), taken.into_writes().unwrap());
            engine.apply(2, a).unwrap();
            other.apply(2, b).unwrap();
            let all = |m: &mut Model| {
                let mut rows = Vec::new();
                let mut at = Vec::new();
                while let Some((k, v)) = m.next(&at, &[0xFF]).unwrap() {
                    at = k.clone();
                    at.push(0);
                    rows.push((k, v));
                }
                rows
            };
            prop_assert_eq!(all(&mut engine), all(&mut other));
        }

        /// Reading through the overlay is reading the engine with the entry's writes
        /// applied, and the entry's writes applied as one batch give the same rows.
        #[test]
        fn an_overlay_reads_as_the_engine_would_after_the_entry(
            before in prop::collection::vec(write(), 0..12),
            entry in prop::collection::vec(prop::collection::vec(write(), 0..4), 0..4),
            from in key(),
            to in key(),
        ) {
            let mut engine = Model::default();
            // The storage holds an earlier entry's writes first, as a range's does, and the
            // overlay reads only its own.
            let mut writes = Writes::default();
            let mut earlier = Overlay::new(&mut engine, (usize::MAX, usize::MAX), &mut writes);
            earlier.apply(1, before.clone()).unwrap();
            earlier.apply(1, entry.concat()).unwrap();
            engine.apply(1, before).unwrap();
            let mut expected = engine.clone();
            let mut overlay = Overlay::new(&mut engine, (usize::MAX, usize::MAX), &mut writes);
            for (i, writes) in entry.iter().enumerate() {
                overlay.apply(2, writes.clone()).unwrap();
                expected.apply(2 + i as u64, writes.clone()).unwrap();
                for k in [from.clone(), to.clone()] {
                    prop_assert_eq!(overlay.get(&k).unwrap(), expected.get(&k).unwrap());
                }
                prop_assert_eq!(overlay.next(&from, &to).unwrap(), expected.next(&from, &to).unwrap());
            }
            let batch = overlay.into_writes().unwrap();
            engine.apply(2, batch).unwrap();
            let all = |m: &mut Model| {
                let mut rows = Vec::new();
                let mut at = Vec::new();
                while let Some((k, v)) = m.next(&at, &[0xFF]).unwrap() {
                    at = k.clone();
                    at.push(0);
                    rows.push((k, v));
                }
                rows
            };
            prop_assert_eq!(all(&mut engine), all(&mut expected));
        }
    }
}
