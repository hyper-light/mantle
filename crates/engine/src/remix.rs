//! A REMIX view of a leaf's bundle (Zhong et al., FAST 2021; docs/design/engine-structure.md §5,
//! E6): the bundle's branches (its runs, newest first) read as one sorted sequence without
//! merging them. The view cuts every version of every key, in key order, into segments of at
//! most [`SEGMENT`] entries, a key's versions never split. Each segment keeps its first key (its
//! anchor), where each run stands at the anchor (a page number and an entry), and a selector byte
//! for each entry naming its run, whether a newer run shadows it, and whether it is a deletion.
//!
//! A seek finds its segment by the anchors and places each run's cursor from the offsets; a step
//! follows the next selector, so it moves one run's cursor and compares no keys. A run's pages are
//! read only once a selector names it. The bundle is the oldest a key in its leaf has (pending
//! branches and the path's bundles are newer), so a key whose newest version is a deletion has
//! nothing older to hide: the view passes over it.

use crate::branch::{Branch, Op, RunCursor};
use crate::error::{Error, Malformed};
use crate::rows::Rows;
use crate::store::Store;
use hyper_block::block::BlockFile;

/// Entries a segment holds at most, as the paper evaluates it: 2.9 bytes a key for 48-byte keys
/// and 8 runs, 3.16% of the data (research/34 §4). A bundle of more runs widens its segments to
/// its run count, so a key's versions fit one segment.
pub const SEGMENT: usize = 32;

/// Format: a selector's run bits, and the runs a view holds at most.
const RUN: u8 = 0x3f;
/// Format: a version a newer run shadows.
const OLD: u8 = 0x80;
/// Format: a deletion.
const TOMBSTONE: u8 = 0x40;

/// A run's offset that names no entry: the run has none at or past the anchor in the view.
const PAST: (u64, u16) = (u64::MAX, u16::MAX);

fn corrupt() -> Error {
    Error::Corruption {
        what: "a REMIX view",
        why: Malformed::OutOfRange,
    }
}

/// A view of runs over a key range.
#[derive(Debug, Default)]
pub struct View {
    runs: usize,
    /// The anchors back to back, and where each ends.
    anchors: Vec<u8>,
    anchor_ends: Vec<usize>,
    /// Each segment's offset for each run, `runs` a segment.
    offsets: Vec<(u64, u16)>,
    /// The selectors back to back, and where each segment's end.
    selectors: Vec<u8>,
    selector_ends: Vec<usize>,
}

impl View {
    /// The view of `runs` (newest first) over `[lo, hi)`, at most 63 runs.
    pub fn build<F: BlockFile>(
        store: &mut Store<F>,
        runs: &[&Branch],
        lo: &[u8],
        hi: Option<&[u8]>,
    ) -> Result<Self, Error> {
        if runs.len() > usize::from(RUN) {
            return Err(Error::LimitExceeded {
                what: "the runs of a REMIX view",
                limit: u64::from(RUN),
            });
        }
        let width = SEGMENT.max(runs.len());
        let mut cursors = Vec::with_capacity(runs.len());
        for b in runs {
            match b.run_at(store, lo) {
                Ok(c) => cursors.push(c),
                Err(e) => {
                    give_back(store, cursors);
                    return Err(e);
                }
            }
        }
        let built = Self::fill(store, runs, &mut cursors, hi, width);
        give_back(store, cursors);
        built
    }

    fn fill<F: BlockFile>(
        store: &mut Store<F>,
        runs: &[&Branch],
        cursors: &mut [RunCursor],
        hi: Option<&[u8]>,
        width: usize,
    ) -> Result<Self, Error> {
        let mut view = Self {
            runs: runs.len(),
            ..Self::default()
        };
        let in_range = |c: &RunCursor| c.valid() && hi.is_none_or(|h| c.key() < h);
        let mut key = Vec::new();
        let mut in_segment = 0usize;
        // Every entry of every run is taken once: the runs' entries bound the steps.
        let total: u64 = runs.iter().map(|b| b.count).sum();
        for _ in 0..=total {
            // The least key any run holds next.
            let Some(least) = cursors
                .iter()
                .filter(|c| in_range(c))
                .map(RunCursor::key)
                .min()
            else {
                if in_segment > 0 {
                    view.selector_ends.push(view.selectors.len());
                }
                return Ok(view);
            };
            key.clear();
            key.extend_from_slice(least);
            let versions = cursors
                .iter()
                .filter(|c| in_range(c) && c.key() == key.as_slice())
                .count();
            if in_segment > 0 && in_segment.saturating_add(versions) > width {
                view.selector_ends.push(view.selectors.len());
                in_segment = 0;
            }
            if in_segment == 0 {
                view.anchors.extend_from_slice(&key);
                view.anchor_ends.push(view.anchors.len());
                for c in cursors.iter() {
                    view.offsets.push(if in_range(c) {
                        let (page, index) = c.position();
                        (page, u16::try_from(index).map_err(|_| corrupt())?)
                    } else {
                        PAST
                    });
                }
            }
            // The key's versions, newest run first.
            let mut newest = true;
            for (r, c) in cursors.iter_mut().enumerate() {
                if !(in_range(c) && c.key() == key.as_slice()) {
                    continue;
                }
                let mut sel = u8::try_from(r).map_err(|_| corrupt())?;
                if !newest {
                    sel |= OLD;
                }
                if c.op() == Op::Delete {
                    sel |= TOMBSTONE;
                }
                newest = false;
                view.selectors.push(sel);
                in_segment = in_segment.saturating_add(1);
                let b = runs.get(r).ok_or(corrupt())?;
                c.next(b, store)?;
            }
        }
        Err(corrupt())
    }

    /// The segments.
    pub fn segments(&self) -> usize {
        self.anchor_ends.len()
    }

    /// Entries the view holds, every version.
    pub fn entries(&self) -> usize {
        self.selectors.len()
    }

    fn anchor(&self, s: usize) -> Option<&[u8]> {
        let start = match s.checked_sub(1) {
            Some(p) => *self.anchor_ends.get(p)?,
            None => 0,
        };
        self.anchors.get(start..*self.anchor_ends.get(s)?)
    }

    fn selectors_of(&self, s: usize) -> Option<&[u8]> {
        let start = match s.checked_sub(1) {
            Some(p) => *self.selector_ends.get(p)?,
            None => 0,
        };
        self.selectors.get(start..*self.selector_ends.get(s)?)
    }

    /// The segment whose entries a seek of `key` starts in: the last whose anchor is at most
    /// `key`, the first when `key` is before every anchor.
    fn segment_of(&self, key: &[u8]) -> usize {
        let (mut lo, mut hi) = (0usize, self.segments());
        // The last anchor at most `key`, by halving: `lo` stays at most, `hi` past.
        while hi.saturating_sub(lo) > 1 {
            let mid = lo.saturating_add(hi.saturating_sub(lo) / 2);
            match self.anchor(mid) {
                Some(a) if a <= key => lo = mid,
                _ => hi = mid,
            }
        }
        lo
    }

    /// Appends to `out` up to `limit` live entries from the first key at or past `from` (a
    /// key's newest version, deletions passed over), reading `runs`, the branches the view was
    /// built of. Returns whether the view holds more past them, with the next key into `next`.
    pub fn scan<F: BlockFile>(
        &self,
        store: &mut Store<F>,
        runs: &[&Branch],
        from: &[u8],
        limit: usize,
        out: &mut Rows,
        next: &mut Vec<u8>,
    ) -> Result<bool, Error> {
        let mut walk = Walk::seek(self, store, runs, from)?;
        let mut taken = 0usize;
        let more = loop {
            if !walk.valid() {
                break false;
            }
            if taken == limit {
                next.clear();
                next.extend_from_slice(walk.key());
                break true;
            }
            if walk.op() == Op::Put {
                out.push(walk.key(), walk.value());
                taken = taken.saturating_add(1);
            }
            walk.step(self, store, runs)?;
        };
        walk.give_back(store);
        Ok(more)
    }
}

fn give_back<F: BlockFile>(store: &mut Store<F>, cursors: Vec<RunCursor>) {
    for c in cursors {
        c.give_back(store);
    }
}

/// A walk of a view's keys in order: each key's newest version. One cursor a run, placed at its
/// segment's offset when a selector first names it and moved as the selectors name it; entries
/// passed over (shadowed versions) are counted and skipped when the run is next read.
#[derive(Debug)]
pub struct Walk {
    segment: usize,
    /// The entry's index in the segment's selectors, and its run.
    at: usize,
    run: usize,
    /// Each run's cursor once placed, and the entries it must still pass to reach its next.
    cursors: Vec<Option<RunCursor>>,
    behind: Vec<usize>,
    valid: bool,
}

impl Walk {
    /// A walk at the first key at or past `from` whose newest version holds a value.
    pub fn seek<F: BlockFile>(
        view: &View,
        store: &mut Store<F>,
        runs: &[&Branch],
        from: &[u8],
    ) -> Result<Self, Error> {
        let mut w = Self {
            segment: view.segment_of(from),
            at: 0,
            run: 0,
            cursors: (0..view.runs).map(|_| None).collect(),
            behind: vec![0; view.runs],
            valid: view.segments() > 0,
        };
        if w.valid {
            w.settle(view, store, runs)?;
        }
        // Forward to the first key at or past `from`: within the segment, at most its entries.
        for _ in 0..=view.entries() {
            if !w.valid || (w.key() >= from && w.op() == Op::Put) {
                return Ok(w);
            }
            w.step(view, store, runs)?;
        }
        Err(corrupt())
    }

    /// Whether the walk is at an entry.
    pub fn valid(&self) -> bool {
        self.valid
    }

    fn cursor(&self) -> Option<&RunCursor> {
        self.cursors.get(self.run)?.as_ref()
    }

    /// The current key; empty past the end.
    pub fn key(&self) -> &[u8] {
        self.cursor().map_or(&[][..], RunCursor::key)
    }

    /// The current key's newest operation.
    pub fn op(&self) -> Op {
        self.cursor().map_or(Op::Delete, RunCursor::op)
    }

    /// The current value.
    pub fn value(&self) -> &[u8] {
        self.cursor().map_or(&[][..], RunCursor::value)
    }

    /// Moves to the next key's newest version.
    pub fn step<F: BlockFile>(
        &mut self,
        view: &View,
        store: &mut Store<F>,
        runs: &[&Branch],
    ) -> Result<(), Error> {
        if !self.valid {
            return Ok(());
        }
        // The current entry is passed: its run's cursor moves past it when next read.
        let b = self.behind.get_mut(self.run).ok_or(corrupt())?;
        *b = b.saturating_add(1);
        self.at = self.at.saturating_add(1);
        self.settle(view, store, runs)
    }

    /// From `at`, past shadowed versions and segment ends, to an entry that is a key's newest
    /// version, its run's cursor placed on it; invalid past the view's last entry.
    fn settle<F: BlockFile>(
        &mut self,
        view: &View,
        store: &mut Store<F>,
        runs: &[&Branch],
    ) -> Result<(), Error> {
        for _ in 0..=view.entries() {
            let Some(selectors) = view.selectors_of(self.segment) else {
                self.valid = false;
                return Ok(());
            };
            let Some(&sel) = selectors.get(self.at) else {
                // The segment's end: the next one's offsets place the runs not yet read, and
                // their counts of entries passed start again from it.
                self.segment = self.segment.saturating_add(1);
                self.at = 0;
                for (c, b) in self.cursors.iter().zip(self.behind.iter_mut()) {
                    if c.is_none() {
                        *b = 0;
                    }
                }
                continue;
            };
            let r = usize::from(sel & RUN);
            if sel & OLD != 0 {
                let b = self.behind.get_mut(r).ok_or(corrupt())?;
                *b = b.saturating_add(1);
                self.at = self.at.saturating_add(1);
                continue;
            }
            self.run = r;
            return self.load(view, store, runs);
        }
        Err(corrupt())
    }

    /// Places or catches up the current run's cursor on the current entry.
    fn load<F: BlockFile>(
        &mut self,
        view: &View,
        store: &mut Store<F>,
        runs: &[&Branch],
    ) -> Result<(), Error> {
        let r = self.run;
        let b = runs.get(r).ok_or(corrupt())?;
        let slot = self.cursors.get_mut(r).ok_or(corrupt())?;
        if slot.is_none() {
            let i = self
                .segment
                .checked_mul(view.runs)
                .and_then(|i| i.checked_add(r))
                .ok_or(corrupt())?;
            let (page, index) = *view.offsets.get(i).ok_or(corrupt())?;
            if (page, index) == PAST {
                return Err(corrupt());
            }
            *slot = Some(b.run_from(store, page, usize::from(index))?);
        }
        let behind = self.behind.get_mut(r).ok_or(corrupt())?;
        if let Some(c) = slot.as_mut() {
            for _ in 0..*behind {
                c.next(b, store)?;
            }
            if !c.valid() {
                return Err(corrupt());
            }
        }
        *behind = 0;
        Ok(())
    }

    /// Gives every placed cursor's page and span back to `store`.
    pub fn give_back<F: BlockFile>(self, store: &mut Store<F>) {
        for c in self.cursors.into_iter().flatten() {
            c.give_back(store);
        }
    }
}
