//! The merge a scan reads a leaf segment's trunk sources through (docs/design/engine-structure.md
//! §5, E6): a cursor for each lone branch and a walk for each pivot bundle with a REMIX view, given
//! newest first as `Trunk::segment_at` lists them. Each key reads from the newest source holding
//! it, a deletion too, and every source holding it moves past it.

use crate::branch::{Branch, Op, RunCursor};
use crate::error::Error;
use crate::remix::{View, Walk};
use crate::store::Store;
use crate::trunk::Source;
use hyper_block::block::BlockFile;

/// One source's head: a branch's cursor, or a bundle's walk through its view.
#[derive(Debug)]
enum Head<'a> {
    Branch(RunCursor, &'a Branch),
    View(Walk, &'a View, &'a [Branch]),
}

impl Head<'_> {
    fn key(&self) -> Option<&[u8]> {
        match self {
            Head::Branch(c, _) => c.valid().then(|| c.key()),
            Head::View(w, ..) => w.valid().then(|| w.key()),
        }
    }

    fn op(&self) -> Op {
        match self {
            Head::Branch(c, _) => c.op(),
            Head::View(w, ..) => w.op(),
        }
    }

    fn value(&self) -> &[u8] {
        match self {
            Head::Branch(c, _) => c.value(),
            Head::View(w, ..) => w.value(),
        }
    }

    fn next<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<(), Error> {
        match self {
            Head::Branch(c, b) => c.next(b, store),
            Head::View(w, view, runs) => w.step(view, store, runs),
        }
    }

    fn give_back<F: BlockFile>(self, store: &mut Store<F>) {
        match self {
            Head::Branch(c, _) => c.give_back(store),
            Head::View(w, ..) => w.give_back(store),
        }
    }
}

/// A merge of a segment's sources over `[from, end)`, its buffers kept between segments.
#[derive(Debug, Default)]
pub struct ScanMerge<'a> {
    heads: Vec<Head<'a>>,
    end: Vec<u8>,
    bounded: bool,
    /// The head whose entry is current, if any.
    current: Option<usize>,
    /// The key `next` moves past.
    past: Vec<u8>,
    /// A range filter check's buffer.
    scratch: Vec<u8>,
    /// Sources passed over at open by their range filters, and sources opened.
    skipped: u64,
    opened: u64,
}

impl<'a> ScanMerge<'a> {
    /// A merge holding no sources.
    pub fn new() -> Self {
        Self::default()
    }

    /// Opens `sources` (newest first) over `[from, end)`, the heads of a merge open before given
    /// back first. With `filter`, a source whose range filters rule `[from, end)` out is passed
    /// over unread: a branch by its own, a view when every run's does. A scan the caller bounds
    /// asks for it; a leaf's end alone almost never rules a branch above it out, so an open scan
    /// does not pay the checks.
    pub fn open<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        sources: &[Source<'a>],
        from: &[u8],
        end: Option<&[u8]>,
        filter: bool,
    ) -> Result<(), Error> {
        self.close(store);
        self.end.clear();
        self.bounded = end.is_some();
        if let Some(e) = end {
            self.end.extend_from_slice(e);
        }
        for s in sources {
            if filter && end.is_some() {
                let scratch = &mut self.scratch;
                let held = match *s {
                    Source::Branch(b) => b.range.may_hold(from, end, scratch),
                    Source::View(_, runs) => {
                        runs.iter().any(|b| b.range.may_hold(from, end, scratch))
                    }
                };
                if !held {
                    self.skipped = self.skipped.saturating_add(1);
                    continue;
                }
            }
            self.opened = self.opened.saturating_add(1);
            let head = match *s {
                Source::Branch(b) => Head::Branch(b.seek(store, from)?, b),
                Source::View(view, runs) => {
                    Head::View(Walk::seek(view, store, runs, from)?, view, runs)
                }
            };
            self.heads.push(head);
        }
        self.pick();
        Ok(())
    }

    /// Sources passed over by their range filters, and sources opened, since the merge was made.
    pub fn counts(&self) -> (u64, u64) {
        (self.skipped, self.opened)
    }

    /// Gives every head's pages and span back to `store`.
    pub fn close<F: BlockFile>(&mut self, store: &mut Store<F>) {
        for h in self.heads.drain(..) {
            h.give_back(store);
        }
        self.current = None;
    }

    /// The head at the least key before the end, the newest of those tied.
    fn pick(&mut self) {
        let mut best: Option<(usize, &[u8])> = None;
        for (i, h) in self.heads.iter().enumerate() {
            let Some(k) = h.key() else { continue };
            // Strictly less only: an equal key keeps the earlier, newer head.
            if best.is_none_or(|(_, b)| k < b) {
                best = Some((i, k));
            }
        }
        self.current = best
            .filter(|&(_, k)| !self.bounded || k < self.end.as_slice())
            .map(|(i, _)| i);
    }

    /// The current entry: key, operation, value; none past the end.
    pub fn entry(&self) -> Option<(&[u8], Op, &[u8])> {
        let h = self.heads.get(self.current?)?;
        Some((h.key()?, h.op(), h.value()))
    }

    /// Moves every head holding the current key past it.
    pub fn next<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<(), Error> {
        let Some(key) = self
            .current
            .and_then(|i| self.heads.get(i))
            .and_then(Head::key)
        else {
            return Ok(());
        };
        self.past.clear();
        self.past.extend_from_slice(key);
        for h in &mut self.heads {
            if h.key() == Some(self.past.as_slice()) {
                h.next(store)?;
            }
        }
        self.pick();
        Ok(())
    }
}
