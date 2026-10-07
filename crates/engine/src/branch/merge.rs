//! Merging branches (docs/design/engine-structure.md §4, step E4): their entries in key order,
//! the newest branch's entry for each key and no other, as SplinterDB's merge iterator gives them
//! to a compaction (research/34 §1).

use super::filter::Keys;
use super::{Branch, Builder, Cursor, Op};
use crate::error::Error;
use crate::store::Store;
use hyper_block::block::BlockFile;

/// A merge over cursors given newest first, from a start key up to an end key (exclusive, none
/// for the end of the branches).
#[derive(Debug)]
pub struct Merge {
    cursors: Vec<Cursor>,
    end: Option<Vec<u8>>,
    /// The cursor whose entry is current, if any.
    current: Option<usize>,
}

impl Merge {
    /// A merge of `branches`, newest first, over `[from, end)`.
    pub fn new<F: BlockFile>(
        store: &mut Store<F>,
        branches: &[Branch],
        from: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Self, Error> {
        let mut cursors = Vec::with_capacity(branches.len());
        for b in branches {
            cursors.push(b.seek(store, from)?);
        }
        let mut merge = Self {
            cursors,
            end: end.map(<[u8]>::to_vec),
            current: None,
        };
        merge.pick();
        Ok(merge)
    }

    /// The cursor at the smallest key, the newest of those tied; none past the end.
    fn pick(&mut self) {
        let mut best: Option<usize> = None;
        for (i, c) in self.cursors.iter().enumerate() {
            if !c.valid() {
                continue;
            }
            let better = match best.and_then(|b| self.cursors.get(b)) {
                None => true,
                // Strictly smaller only: an equal key keeps the earlier, newer cursor.
                Some(b) => c.key() < b.key(),
            };
            if better {
                best = Some(i);
            }
        }
        self.current = best.filter(|&b| {
            self.cursors
                .get(b)
                .is_some_and(|c| self.end.as_deref().is_none_or(|end| c.key() < end))
        });
    }

    /// The current entry: key, operation, value.
    pub fn entry(&self) -> Option<(&[u8], Op, &[u8])> {
        self.current
            .and_then(|i| self.cursors.get(i))
            .map(|c| (c.key(), c.op(), c.value()))
    }

    /// Moves past the current key in every cursor that holds it; the cursors moved, each an
    /// input entry consumed.
    pub fn next<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<u64, Error> {
        let Some(at) = self.current.and_then(|i| self.cursors.get(i)) else {
            return Ok(0);
        };
        let key = at.key().to_vec();
        let mut moved = 0u64;
        for c in &mut self.cursors {
            if c.valid() && c.key() == key.as_slice() {
                c.next(store)?;
                moved = moved.saturating_add(1);
            }
        }
        self.pick();
        Ok(moved)
    }
}

/// A compaction run a slice at a time ([`Compaction::step`]): `branches` (newest first) merged
/// over `[from, end)` into branches of at most `per` entries each, the newest entry of each key,
/// tombstones dropped when `drop_tombstones` (nothing older lies below). Its inputs are not
/// changed while it runs, so it is resumed between any other reads of the store.
#[derive(Debug)]
pub struct Compaction {
    merge: Merge,
    drop_tombstones: bool,
    per: u64,
    /// The branch being built: its first key, its builder, its entries.
    building: Option<(Vec<u8>, Builder, u64)>,
    out: Vec<(Vec<u8>, Branch)>,
    /// Input entries not yet merged, at most: the inputs' counts less those consumed (a range
    /// narrower than a branch leaves some never reached).
    remaining: u64,
}

impl Compaction {
    /// A compaction of `branches`, newest first, over `[from, end)`.
    pub fn new<F: BlockFile>(
        store: &mut Store<F>,
        branches: &[Branch],
        from: &[u8],
        end: Option<&[u8]>,
        drop_tombstones: bool,
        per: u64,
    ) -> Result<Self, Error> {
        Ok(Self {
            merge: Merge::new(store, branches, from, end)?,
            drop_tombstones,
            per: per.max(1),
            building: None,
            out: Vec::new(),
            remaining: branches
                .iter()
                .map(|b| b.count)
                .fold(0, u64::saturating_add),
        })
    }

    /// Input entries left to merge, at most.
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Merges up to `budget` keys; the keys merged, fewer than `budget` only once the merge is
    /// done.
    pub fn step<F: BlockFile>(&mut self, store: &mut Store<F>, budget: u64) -> Result<u64, Error> {
        let mut done = 0u64;
        while done < budget {
            let Some((key, op, value)) = self.merge.entry() else {
                break;
            };
            if !(self.drop_tombstones && op == Op::Delete) {
                let (key, value) = (key.to_vec(), value.to_vec());
                if self
                    .building
                    .as_ref()
                    .is_some_and(|(_, _, n)| *n >= self.per)
                    && let Some((first, b, _)) = self.building.take()
                {
                    self.out.push((first, b.finish(store)?));
                }
                let (_, b, n) = match self.building.as_mut() {
                    Some(b) => b,
                    None => self.building.insert((
                        key.clone(),
                        Builder::new(store, Keys::AtMost(self.remaining.min(self.per)))?,
                        0,
                    )),
                };
                b.add(store, &key, op, &value)?;
                *n = n.saturating_add(1);
            }
            let consumed = self.merge.next(store)?;
            self.remaining = self.remaining.saturating_sub(consumed);
            done = done.saturating_add(1);
        }
        Ok(done)
    }

    /// Whether every key is merged.
    pub fn is_done(&self) -> bool {
        self.merge.entry().is_none()
    }

    /// The branches made, each with its first key, in key order; the merge must be done.
    pub fn finish<F: BlockFile>(
        mut self,
        store: &mut Store<F>,
    ) -> Result<Vec<(Vec<u8>, Branch)>, Error> {
        if !self.is_done() {
            return Err(Error::InvalidArgument {
                what: "a compaction finished before its merge",
            });
        }
        if let Some((first, b, _)) = self.building.take() {
            self.out.push((first, b.finish(store)?));
        }
        Ok(self.out)
    }
}

/// Merges `branches` (newest first) over `[from, end)` into a new branch: the newest entry of
/// each key, tombstones dropped when `drop_tombstones` (a compaction with nothing older below
/// it). None when nothing is left.
pub fn compact<F: BlockFile>(
    store: &mut Store<F>,
    branches: &[Branch],
    from: &[u8],
    end: Option<&[u8]>,
    drop_tombstones: bool,
) -> Result<Option<Branch>, Error> {
    let mut c = Compaction::new(store, branches, from, end, drop_tombstones, u64::MAX)?;
    c.step(store, u64::MAX)?;
    Ok(c.finish(store)?.into_iter().next().map(|(_, b)| b))
}

/// [`compact`] into branches of at most `per` entries each: each with its first key, in key
/// order, for a leaf's split. Empty when nothing is left.
pub fn compact_split<F: BlockFile>(
    store: &mut Store<F>,
    branches: &[Branch],
    from: &[u8],
    end: Option<&[u8]>,
    drop_tombstones: bool,
    per: u64,
) -> Result<Vec<(Vec<u8>, Branch)>, Error> {
    let mut c = Compaction::new(store, branches, from, end, drop_tombstones, per)?;
    c.step(store, u64::MAX)?;
    c.finish(store)
}
