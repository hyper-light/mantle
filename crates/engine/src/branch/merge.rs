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
    /// The key `next` moves past, its buffer reused so a step allocates nothing.
    past: Vec<u8>,
}

impl Merge {
    /// A merge of `branches`, newest first, over `[from, end)`.
    pub fn new<'a, F: BlockFile>(
        store: &mut Store<F>,
        branches: impl IntoIterator<Item = &'a Branch>,
        from: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Self, Error> {
        let mut cursors = Vec::new();
        for b in branches {
            cursors.push(b.seek(store, from)?);
        }
        let mut merge = Self {
            cursors,
            end: end.map(<[u8]>::to_vec),
            current: None,
            past: Vec::new(),
        };
        merge.pick();
        Ok(merge)
    }

    /// Gives every cursor's span back to `store`'s pool: the merge is done with them.
    pub fn give_back<F: BlockFile>(self, store: &mut Store<F>) {
        for c in self.cursors {
            c.give_back(store);
        }
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
        self.past.clear();
        self.past.extend_from_slice(at.key());
        let mut moved = 0u64;
        for c in &mut self.cursors {
            if c.valid() && c.key() == self.past.as_slice() {
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
    /// Branches sealed whose filter pages are still to write, oldest first: at most the parts
    /// the compaction makes.
    closing: std::collections::VecDeque<(Vec<u8>, Builder)>,
    out: Vec<(Vec<u8>, Branch)>,
    /// Input entries not yet merged, at most: the inputs' counts less those consumed (a range
    /// narrower than a branch leaves some never reached).
    remaining: u64,
}

impl Compaction {
    /// A compaction of `branches`, newest first, over `[from, end)`. The branches are taken by
    /// reference: a branch's descriptor carries its filter, which a compaction never reads, and
    /// planning cloned the descriptors, megabytes of filters a plan.
    pub fn new<'a, F: BlockFile>(
        store: &mut Store<F>,
        branches: impl IntoIterator<Item = &'a Branch>,
        from: &[u8],
        end: Option<&[u8]>,
        drop_tombstones: bool,
        per: u64,
    ) -> Result<Self, Error> {
        let mut remaining = 0u64;
        let counted = branches.into_iter().inspect(|b| {
            remaining = remaining.saturating_add(b.count);
        });
        let merge = Merge::new(store, counted, from, end)?;
        Ok(Self {
            merge,
            drop_tombstones,
            per: per.max(1),
            building: None,
            closing: std::collections::VecDeque::new(),
            out: Vec::new(),
            remaining,
        })
    }

    /// Input entries left to merge, at most.
    pub fn remaining(&self) -> u64 {
        // The filter pages left to write, each at its worth in keys.
        let filters = |b: &Builder| b.filter_pages_left().saturating_mul(b.page_keys());
        self.closing
            .iter()
            .map(|(_, b)| filters(b))
            .chain(self.building.iter().map(|(_, b, _)| filters(b)))
            .fold(self.remaining, u64::saturating_add)
    }

    /// Works for up to `budget`: the filter pages of branches already sealed first, each at its
    /// worth in keys ([`Builder::page_keys`]), then keys merged; a part that reaches its size, or
    /// the last once the merge is done, is sealed and its filter's pages wait their turn. Returns
    /// the budget used, less than `budget` only once the compaction is done.
    pub fn step<F: BlockFile>(&mut self, store: &mut Store<F>, budget: u64) -> Result<u64, Error> {
        let mut done = 0u64;
        while done < budget {
            if let Some((_, b)) = self.closing.front_mut() {
                let keys = b.page_keys();
                let pages = budget
                    .saturating_sub(done)
                    .checked_div(keys)
                    .unwrap_or(1)
                    .max(1);
                let before = b.filter_pages_left();
                let finished = b.write_filter(store, pages)?;
                let written = before.saturating_sub(b.filter_pages_left());
                done = done.saturating_add(written.saturating_mul(keys));
                if finished && let Some((first, b)) = self.closing.pop_front() {
                    self.out.push((first, b.into_branch(store)?));
                }
                continue;
            }
            let Self {
                merge,
                building,
                closing,
                per,
                remaining,
                drop_tombstones,
                ..
            } = self;
            let Some((key, op, value)) = merge.entry() else {
                // The merge is done: the last part is sealed, or the compaction is.
                match building.take() {
                    Some((first, mut b, _)) => {
                        b.seal(store)?;
                        closing.push_back((first, b));
                        continue;
                    }
                    None => break,
                }
            };
            if !(*drop_tombstones && op == Op::Delete) {
                if building.as_ref().is_some_and(|(_, _, n)| *n >= *per)
                    && let Some((first, mut b, _)) = building.take()
                {
                    b.seal(store)?;
                    closing.push_back((first, b));
                }
                let (_, b, n) = match building.as_mut() {
                    Some(b) => b,
                    None => building.insert((
                        key.to_vec(),
                        Builder::new(store, Keys::AtMost((*remaining).min(*per)))?,
                        0,
                    )),
                };
                b.add(store, key, op, value)?;
                *n = n.saturating_add(1);
            }
            let consumed = self.merge.next(store)?;
            self.remaining = self.remaining.saturating_sub(consumed);
            done = done.saturating_add(1);
        }
        Ok(done.min(budget))
    }

    /// Whether every key is merged and every branch made is whole, its filter written.
    pub fn is_done(&self) -> bool {
        self.merge.entry().is_none() && self.building.is_none() && self.closing.is_empty()
    }

    /// The branches made, each with its first key, in key order; the merge must be done.
    pub fn finish<F: BlockFile>(
        self,
        store: &mut Store<F>,
    ) -> Result<Vec<(Vec<u8>, Branch)>, Error> {
        if !self.is_done() {
            return Err(Error::InvalidArgument {
                what: "a compaction finished before its merge",
            });
        }
        self.merge.give_back(store);
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
