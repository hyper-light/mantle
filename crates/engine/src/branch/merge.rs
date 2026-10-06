//! Merging branches (docs/design/engine-structure.md §4, step E4): their entries in key order,
//! the newest branch's entry for each key and no other, as SplinterDB's merge iterator gives them
//! to a compaction (research/34 §1).

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

    /// Moves past the current key in every cursor that holds it.
    pub fn next<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<(), Error> {
        let Some(at) = self.current.and_then(|i| self.cursors.get(i)) else {
            return Ok(());
        };
        let key = at.key().to_vec();
        for c in &mut self.cursors {
            if c.valid() && c.key() == key.as_slice() {
                c.next(store)?;
            }
        }
        self.pick();
        Ok(())
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
    let mut merge = Merge::new(store, branches, from, end)?;
    let mut builder = Builder::new(store.page_capacity())?;
    let mut any = false;
    while let Some((key, op, value)) = merge.entry() {
        if !(drop_tombstones && op == Op::Delete) {
            let (key, value) = (key.to_vec(), value.to_vec());
            builder.add(store, &key, op, &value)?;
            any = true;
        }
        merge.next(store)?;
    }
    if any {
        builder.finish(store).map(Some)
    } else {
        Ok(None)
    }
}
