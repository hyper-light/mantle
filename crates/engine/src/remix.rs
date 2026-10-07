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
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct View {
    runs: usize,
    /// The runs' roots, newest first: the branches the view describes.
    roots: Vec<u64>,
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
    /// The view of `runs` (newest first) over `[lo, hi)`, at most 63 runs, built at once: a
    /// [`Build`] run to its end.
    pub fn build<F: BlockFile>(
        store: &mut Store<F>,
        runs: &[Branch],
        lo: &[u8],
        hi: Option<&[u8]>,
    ) -> Result<Self, Error> {
        let mut job = Build::new(store, runs, lo, hi)?;
        // Every entry of every run is taken once: the runs' entries bound the steps.
        let total: u64 = runs.iter().map(|b| b.count).sum();
        for _ in 0..=total {
            match job.step(store, runs, u64::MAX) {
                Ok((_, true)) => return Ok(job.finish(store)),
                Ok((_, false)) => {}
                Err(e) => {
                    job.abandon(store);
                    return Err(e);
                }
            }
        }
        job.abandon(store);
        Err(corrupt())
    }

    /// The bytes the view's segments take in its pages: each anchor with a two-byte length, six
    /// bytes a run's offset (a four-byte page number, a two-byte entry), and each segment's
    /// selectors with a one-byte count.
    pub fn bytes(&self) -> usize {
        let segments = self.segments();
        self.anchors
            .len()
            .saturating_add(segments.saturating_mul(2))
            .saturating_add(self.offsets.len().saturating_mul(6))
            .saturating_add(self.selectors.len())
            .saturating_add(segments)
    }

    /// Appends the view to `out`: its runs and their roots, its anchors, offsets and selectors,
    /// each list with its length.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), Error> {
        let n32 = |n: usize| {
            u32::try_from(n)
                .map(u32::to_le_bytes)
                .map_err(|_| corrupt())
        };
        out.extend_from_slice(&n32(self.runs)?);
        for r in &self.roots {
            out.extend_from_slice(&r.to_le_bytes());
        }
        out.extend_from_slice(&n32(self.anchors.len())?);
        out.extend_from_slice(&self.anchors);
        out.extend_from_slice(&n32(self.anchor_ends.len())?);
        for &e in &self.anchor_ends {
            out.extend_from_slice(&n32(e)?);
        }
        out.extend_from_slice(&n32(self.offsets.len())?);
        for &(page, index) in &self.offsets {
            out.extend_from_slice(&page.to_le_bytes());
            out.extend_from_slice(&index.to_le_bytes());
        }
        out.extend_from_slice(&n32(self.selectors.len())?);
        out.extend_from_slice(&self.selectors);
        out.extend_from_slice(&n32(self.selector_ends.len())?);
        for &e in &self.selector_ends {
            out.extend_from_slice(&n32(e)?);
        }
        Ok(())
    }

    /// A view [`Self::encode`] wrote at the start of `bytes`, and the bytes it took. Its parts
    /// must agree: a segment an anchor and a selector end, `runs` offsets a segment, ends that
    /// rise within their lists, every selector naming a run the view has.
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), Error> {
        let mut r = Reader { bytes, at: 0 };
        let runs = r.n32()?;
        if runs > usize::from(RUN) {
            return Err(corrupt());
        }
        let mut roots = Vec::with_capacity(runs);
        for _ in 0..runs {
            roots.push(r.u64()?);
        }
        let n = r.n32()?;
        let anchors = r.take(n)?.to_vec();
        let n = r.n32()?;
        let mut anchor_ends = Vec::with_capacity(n.min(bytes.len()));
        for _ in 0..n {
            anchor_ends.push(r.n32()?);
        }
        let n = r.n32()?;
        let mut offsets = Vec::with_capacity(n.min(bytes.len()));
        for _ in 0..n {
            let page = r.u64()?;
            let index =
                u16::from_le_bytes(r.take(2)?.first_chunk::<2>().copied().ok_or(corrupt())?);
            offsets.push((page, index));
        }
        let n = r.n32()?;
        let selectors = r.take(n)?.to_vec();
        let n = r.n32()?;
        let mut selector_ends = Vec::with_capacity(n.min(bytes.len()));
        for _ in 0..n {
            selector_ends.push(r.n32()?);
        }
        let at = r.at;
        let rising = |ends: &[usize], len: usize| {
            ends.windows(2).all(|w| matches!(w, [a, b] if a < b))
                && ends.last().is_none_or(|&e| e == len)
                && ends.first().is_none_or(|&e| e > 0)
        };
        let segments = anchor_ends.len();
        if selector_ends.len() != segments
            || offsets.len() != segments.saturating_mul(runs)
            || !rising(&anchor_ends, anchors.len())
            || !rising(&selector_ends, selectors.len())
            || selectors.iter().any(|&s| usize::from(s & RUN) >= runs)
        {
            return Err(corrupt());
        }
        Ok((
            Self {
                runs,
                roots,
                anchors,
                anchor_ends,
                offsets,
                selectors,
                selector_ends,
            },
            at,
        ))
    }

    /// The roots of the runs the view describes, newest first.
    pub fn roots(&self) -> &[u64] {
        &self.roots
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

    /// Run `r`'s offset at segment `s`, as a position; a run with no entry there is refused.
    fn offset(&self, s: usize, r: usize) -> Result<(u64, usize), Error> {
        let i = s
            .checked_mul(self.runs)
            .and_then(|i| i.checked_add(r))
            .ok_or(corrupt())?;
        let (page, index) = *self.offsets.get(i).ok_or(corrupt())?;
        if (page, index) == PAST {
            return Err(corrupt());
        }
        Ok((page, usize::from(index)))
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
        runs: &[Branch],
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

/// A view built a slice at a time, as maintenance (docs/design/engine-structure.md §5, E6): a
/// cursor a run, merged in key order, each key's versions newest first; a segment closes where the
/// next key's versions would pass its width. The runs it reads are named by their roots, so the
/// owner checks before each step that the bundle has not changed under it ([`Build::reads`]).
#[derive(Debug)]
pub struct Build {
    roots: Vec<u64>,
    hi: Option<Vec<u8>>,
    width: usize,
    cursors: Vec<RunCursor>,
    view: View,
    in_segment: usize,
    key: Vec<u8>,
}

impl Build {
    /// A build of the view of `runs` (newest first, at most 63) over `[lo, hi)`.
    pub fn new<F: BlockFile>(
        store: &mut Store<F>,
        runs: &[Branch],
        lo: &[u8],
        hi: Option<&[u8]>,
    ) -> Result<Self, Error> {
        if runs.len() > usize::from(RUN) {
            return Err(Error::LimitExceeded {
                what: "the runs of a REMIX view",
                limit: u64::from(RUN),
            });
        }
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
        Ok(Self {
            roots: runs.iter().map(|b| b.root).collect(),
            hi: hi.map(<[u8]>::to_vec),
            width: SEGMENT.max(runs.len()),
            cursors,
            view: View {
                runs: runs.len(),
                roots: runs.iter().map(|b| b.root).collect(),
                ..View::default()
            },
            in_segment: 0,
            key: Vec::new(),
        })
    }

    /// Whether `runs` are the runs this build reads: the same branches, in the same order.
    pub fn reads(&self, runs: &[Branch]) -> bool {
        self.roots.len() == runs.len() && self.roots.iter().zip(runs).all(|(&r, b)| r == b.root)
    }

    /// Takes up to `budget` entries into the view (a key's versions together, so a step may
    /// pass it by fewer than the runs): the entries taken, and whether every entry in range is
    /// now in it.
    pub fn step<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        runs: &[Branch],
        budget: u64,
    ) -> Result<(u64, bool), Error> {
        if !self.reads(runs) {
            return Err(Error::InvalidArgument {
                what: "a view build stepped over runs it was not started on",
            });
        }
        let hi = self.hi.as_deref();
        let in_range = |c: &RunCursor| c.valid() && hi.is_none_or(|h| c.key() < h);
        let mut taken = 0u64;
        while taken < budget {
            // The least key any run holds next.
            let Some(least) = self
                .cursors
                .iter()
                .filter(|c| in_range(c))
                .map(RunCursor::key)
                .min()
            else {
                if self.in_segment > 0 {
                    self.view.selector_ends.push(self.view.selectors.len());
                    self.in_segment = 0;
                }
                return Ok((taken, true));
            };
            self.key.clear();
            self.key.extend_from_slice(least);
            let key = self.key.as_slice();
            let versions = self
                .cursors
                .iter()
                .filter(|c| in_range(c) && c.key() == key)
                .count();
            let view = &mut self.view;
            if self.in_segment > 0 && self.in_segment.saturating_add(versions) > self.width {
                view.selector_ends.push(view.selectors.len());
                self.in_segment = 0;
            }
            if self.in_segment == 0 {
                view.anchors.extend_from_slice(key);
                view.anchor_ends.push(view.anchors.len());
                for c in &self.cursors {
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
            for (r, c) in self.cursors.iter_mut().enumerate() {
                if !(in_range(c) && c.key() == key) {
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
                self.in_segment = self.in_segment.saturating_add(1);
                taken = taken.saturating_add(1);
                let b = runs.get(r).ok_or(corrupt())?;
                c.next(b, store)?;
            }
        }
        Ok((taken, false))
    }

    /// The view built, the cursors' pages and spans given back to `store`.
    pub fn finish<F: BlockFile>(self, store: &mut Store<F>) -> View {
        give_back(store, self.cursors);
        self.view
    }

    /// Drops the build, the cursors' pages and spans given back to `store`.
    pub fn abandon<F: BlockFile>(self, store: &mut Store<F>) {
        give_back(store, self.cursors);
    }
}

/// A view rebuilt for a bundle that gained a newest run (Zhong et al. FAST 2021 §4.3): the old
/// view, one sorted run, merged with the new run by generalized binary merging (Hwang and Lin).
/// For each of the new run's keys the merge point in the old view is found by the anchors and a
/// binary search over the groups of one segment (a key read a halving); the old entries before it
/// are copied without reading them, their runs moved up one, and the versions of a key the new
/// run also holds marked shadowed. The old runs' offsets at each new segment follow from the old
/// offsets and the pages' entry counts; a new segment's anchor is the old one where the segment
/// starts where an old one did, else one key read. Segments are cut as [`Build`] cuts them, so
/// the view is the one a build from scratch makes, field for field.
#[derive(Debug)]
pub struct Rebuild {
    roots: Vec<u64>,
    old: View,
    hi: Option<Vec<u8>>,
    width: usize,
    /// The new run's cursor.
    new: RunCursor,
    /// The old stream's next entry: its segment and index there.
    seg: usize,
    at: usize,
    /// Each old run's entries passed in the current old segment, and those left in the view.
    in_seg: Vec<usize>,
    left: Vec<usize>,
    /// A cursor an old run to read a key where one is needed.
    probes: Vec<Option<RunCursor>>,
    /// The old stream's position the new run's next key goes before, and whether that old
    /// group holds the same key; none until searched for the key.
    point: Option<(usize, usize, bool)>,
    out: View,
    in_segment: usize,
    key: Vec<u8>,
}

impl Rebuild {
    /// A rebuild of `old` (the view of `runs[1..]`) for `runs`, the bundle with its newest run
    /// first, over `[lo, hi)`.
    pub fn new<F: BlockFile>(
        store: &mut Store<F>,
        old: View,
        runs: &[Branch],
        lo: &[u8],
        hi: Option<&[u8]>,
    ) -> Result<Self, Error> {
        let (newest, older) = runs.split_first().ok_or(Error::InvalidArgument {
            what: "a view rebuilt for no runs",
        })?;
        if runs.len() > usize::from(RUN)
            || old.roots.len() != older.len()
            || old.roots.iter().zip(older).any(|(&r, b)| r != b.root)
        {
            return Err(Error::InvalidArgument {
                what: "a view rebuilt over runs its old view does not describe",
            });
        }
        let new = newest.run_at(store, lo)?;
        let mut left = vec![0usize; old.runs];
        for &sel in &old.selectors {
            if let Some(l) = left.get_mut(usize::from(sel & RUN)) {
                *l = l.saturating_add(1);
            }
        }
        let runs_n = runs.len();
        Ok(Self {
            roots: runs.iter().map(|b| b.root).collect(),
            in_seg: vec![0; old.runs],
            left,
            probes: (0..old.runs).map(|_| None).collect(),
            hi: hi.map(<[u8]>::to_vec),
            width: SEGMENT.max(runs_n),
            new,
            seg: 0,
            at: 0,
            point: None,
            out: View {
                runs: runs_n,
                roots: runs.iter().map(|b| b.root).collect(),
                ..View::default()
            },
            in_segment: 0,
            key: Vec::new(),
            old,
        })
    }

    /// Whether `runs` are the runs this rebuild reads: the same branches, in the same order.
    pub fn reads(&self, runs: &[Branch]) -> bool {
        self.roots.len() == runs.len() && self.roots.iter().zip(runs).all(|(&r, b)| r == b.root)
    }

    /// The old group at the stream's position: its entries.
    fn group_len(&self) -> usize {
        let sels = self.old.selectors_of(self.seg).unwrap_or(&[]);
        let mut n = 0usize;
        for (i, &sel) in sels.iter().enumerate().skip(self.at) {
            if i > self.at && sel & OLD == 0 {
                break;
            }
            n = n.saturating_add(1);
        }
        n
    }

    /// The key of the old group at `(seg, at)`, into `self.key`: the segment's anchor at its
    /// start, else read through the group's run's probe cursor.
    fn old_key<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        runs: &[Branch],
        seg: usize,
        at: usize,
    ) -> Result<(), Error> {
        self.key.clear();
        if at == 0 {
            self.key
                .extend_from_slice(self.old.anchor(seg).ok_or(corrupt())?);
            return Ok(());
        }
        let sels = self.old.selectors_of(seg).ok_or(corrupt())?;
        let r = usize::from(sels.get(at).ok_or(corrupt())? & RUN);
        let b = runs.get(r.saturating_add(1)).ok_or(corrupt())?;
        let pos = b
            .position_after(self.old.offset(seg, r)?, occurrences(sels, at, r))?
            .ok_or(corrupt())?;
        let slot = self.probes.get_mut(r).ok_or(corrupt())?;
        if slot.is_none() {
            *slot = Some(RunCursor::new(store)?);
        }
        let c = slot.as_mut().ok_or(corrupt())?;
        c.place(b, store, pos)?;
        self.key.extend_from_slice(c.key());
        Ok(())
    }

    /// The merge point of `k` from the stream's position: the first old group whose key is at
    /// least `k`, and whether it is `k`.
    fn find_point<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        runs: &[Branch],
        k: &[u8],
    ) -> Result<(usize, usize, bool), Error> {
        let segments = self.old.segments();
        // The last segment from the current one whose anchor is below `k`: the point is in it
        // or at the next one's start.
        let (mut lo, mut hi) = (self.seg, segments);
        while hi.saturating_sub(lo) > 1 {
            let mid = lo.saturating_add(hi.saturating_sub(lo) / 2);
            match self.old.anchor(mid) {
                Some(a) if a < k => lo = mid,
                _ => hi = mid,
            }
        }
        let s = lo;
        // The groups of segment `s` from the start (or the stream's position in its segment).
        let sels = self.old.selectors_of(s).ok_or(corrupt())?;
        let start = if s == self.seg { self.at } else { 0 };
        let starts: Vec<usize> = sels
            .iter()
            .enumerate()
            .skip(start)
            .filter(|&(i, &sel)| i == start || sel & OLD == 0)
            .map(|(i, _)| i)
            .collect();
        // Galloping from the stream's position (Hwang and Lin's generalized binary merging): the
        // group 1, 2, 4, ... ahead is read until one is at least `k`, then the last gap halved.
        // A key costs about twice the log of the groups it passes, so a new run much smaller
        // than the view reads little of it, and one as large reads about a key a group.
        let mut equal_at: Option<usize> = None;
        let mut passed = 0usize;
        let mut step = 1usize;
        let mut bound = starts.len();
        while passed < starts.len() {
            let probe = passed
                .saturating_add(step)
                .saturating_sub(1)
                .min(starts.len().saturating_sub(1));
            let at = *starts.get(probe).ok_or(corrupt())?;
            self.old_key(store, runs, s, at)?;
            match self.key.as_slice().cmp(k) {
                std::cmp::Ordering::Less => {
                    passed = probe.saturating_add(1);
                    step = step.saturating_mul(2);
                }
                std::cmp::Ordering::Equal => {
                    equal_at = Some(at);
                    bound = probe;
                    break;
                }
                std::cmp::Ordering::Greater => {
                    bound = probe;
                    break;
                }
            }
        }
        // The first group at least `k` lies in `[passed, bound]`.
        let (mut glo, mut ghi) = (passed, bound);
        while glo < ghi {
            let mid = glo.saturating_add(ghi.saturating_sub(glo) / 2);
            let at = *starts.get(mid).ok_or(corrupt())?;
            self.old_key(store, runs, s, at)?;
            match self.key.as_slice().cmp(k) {
                std::cmp::Ordering::Less => glo = mid.saturating_add(1),
                std::cmp::Ordering::Equal => {
                    equal_at = Some(at);
                    ghi = mid;
                }
                std::cmp::Ordering::Greater => ghi = mid,
            }
        }
        if let Some(&at) = starts.get(glo) {
            return Ok((s, at, equal_at == Some(at)));
        }
        // Past segment `s`: the next one's start, whose anchor says whether it is `k`.
        let next = s.saturating_add(1);
        let equal = self.old.anchor(next).is_some_and(|a| a == k);
        Ok((next, 0, equal))
    }

    /// Opens a segment in the output for a group of `size` entries keyed `key`, the runs'
    /// offsets taken where the streams stand, unless the group fits the one open.
    fn open<F: BlockFile>(
        &mut self,
        runs: &[Branch],
        size: usize,
        key_from_old: bool,
        store: &mut Store<F>,
    ) -> Result<(), Error> {
        if self.in_segment > 0 && self.in_segment.saturating_add(size) > self.width {
            self.out.selector_ends.push(self.out.selectors.len());
            self.in_segment = 0;
        }
        if self.in_segment > 0 {
            return Ok(());
        }
        if key_from_old {
            let (seg, at) = (self.seg, self.at);
            self.old_key(store, runs, seg, at)?;
        } else {
            self.key.clear();
            self.key.extend_from_slice(self.new.key());
        }
        self.out.anchors.extend_from_slice(&self.key);
        self.out.anchor_ends.push(self.out.anchors.len());
        let hi = self.hi.as_deref();
        let new_in = self.new.valid() && hi.is_none_or(|h| self.new.key() < h);
        self.out.offsets.push(if new_in {
            let (page, index) = self.new.position();
            (page, u16::try_from(index).map_err(|_| corrupt())?)
        } else {
            PAST
        });
        for r in 0..self.old.runs {
            let left = self.left.get(r).copied().unwrap_or(0);
            if left == 0 {
                self.out.offsets.push(PAST);
                continue;
            }
            let b = runs.get(r.saturating_add(1)).ok_or(corrupt())?;
            let passed = self.in_seg.get(r).copied().unwrap_or(0);
            let (page, index) = b
                .position_after(self.old.offset(self.seg, r)?, passed)?
                .ok_or(corrupt())?;
            self.out
                .offsets
                .push((page, u16::try_from(index).map_err(|_| corrupt())?));
        }
        Ok(())
    }

    /// Copies the old group at the stream's position, its runs moved up one; shadowed whole
    /// when `shadowed` (the new run holds its key).
    fn copy_old(&mut self, shadowed: bool) -> Result<usize, Error> {
        let n = self.group_len();
        let sels = self.old.selectors_of(self.seg).ok_or(corrupt())?;
        for &sel in sels
            .get(self.at..self.at.saturating_add(n))
            .ok_or(corrupt())?
        {
            let r = sel & RUN;
            let mut out = r.checked_add(1).ok_or(corrupt())? | (sel & (OLD | TOMBSTONE));
            if shadowed {
                out |= OLD;
            }
            self.out.selectors.push(out);
            if let Some(c) = self.in_seg.get_mut(usize::from(r)) {
                *c = c.saturating_add(1);
            }
            if let Some(l) = self.left.get_mut(usize::from(r)) {
                *l = l.saturating_sub(1);
            }
        }
        self.in_segment = self.in_segment.saturating_add(n);
        self.at = self.at.saturating_add(n);
        if self.at >= sels.len() {
            self.seg = self.seg.saturating_add(1);
            self.at = 0;
            self.in_seg.iter_mut().for_each(|c| *c = 0);
        }
        Ok(n)
    }

    /// Takes up to `budget` entries into the view: the entries taken, and whether the view is
    /// whole.
    pub fn step<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        runs: &[Branch],
        budget: u64,
    ) -> Result<(u64, bool), Error> {
        if !self.reads(runs) {
            return Err(Error::InvalidArgument {
                what: "a view rebuild stepped over runs it was not started on",
            });
        }
        let newest = runs.first().ok_or(corrupt())?;
        let mut taken = 0u64;
        while taken < budget {
            let hi = self.hi.as_deref();
            let new_in = self.new.valid() && hi.is_none_or(|h| self.new.key() < h);
            let old_done = self.seg >= self.old.segments();
            if !new_in && old_done {
                if self.in_segment > 0 {
                    self.out.selector_ends.push(self.out.selectors.len());
                    self.in_segment = 0;
                }
                return Ok((taken, true));
            }
            if !new_in {
                let size = self.group_len();
                self.open(runs, size, true, store)?;
                taken = taken.saturating_add(u64::try_from(self.copy_old(false)?).unwrap_or(0));
                continue;
            }
            if self.point.is_none() {
                let k = self.new.key().to_vec();
                self.point = Some(if old_done {
                    (self.seg, 0, false)
                } else {
                    self.find_point(store, runs, &k)?
                });
            }
            let (ps, pa, equal) = self.point.ok_or(corrupt())?;
            if !old_done && (self.seg, self.at) < (ps, pa) {
                // An old group before the new key: copied unread.
                let size = self.group_len();
                self.open(runs, size, true, store)?;
                taken = taken.saturating_add(u64::try_from(self.copy_old(false)?).unwrap_or(0));
                continue;
            }
            // The new key: alone, or first of the old group it shadows.
            let size = if equal && !old_done {
                self.group_len().saturating_add(1)
            } else {
                1
            };
            self.open(runs, size, false, store)?;
            let mut sel = 0u8;
            if self.new.op() == Op::Delete {
                sel |= TOMBSTONE;
            }
            self.out.selectors.push(sel);
            self.in_segment = self.in_segment.saturating_add(1);
            taken = taken.saturating_add(1);
            if equal && !old_done {
                taken = taken.saturating_add(u64::try_from(self.copy_old(true)?).unwrap_or(0));
            }
            self.new.next(newest, store)?;
            self.point = None;
        }
        Ok((taken, false))
    }

    /// The view rebuilt, the cursors given back to `store`.
    pub fn finish<F: BlockFile>(self, store: &mut Store<F>) -> View {
        self.new.give_back(store);
        for c in self.probes.into_iter().flatten() {
            c.give_back(store);
        }
        self.out
    }

    /// Drops the rebuild, its cursors given back to `store`.
    pub fn abandon<F: BlockFile>(self, store: &mut Store<F>) {
        drop(self.finish(store));
    }
}

/// The entries of run `r` among `selectors` before position `j`: every version counts, as each is
/// an entry of its run.
fn occurrences(selectors: &[u8], j: usize, r: usize) -> usize {
    selectors
        .get(..j)
        .unwrap_or(selectors)
        .iter()
        .filter(|&&sel| usize::from(sel & RUN) == r)
        .count()
}

/// The position in `selectors` of run `r`'s `m`th entry (from 0); none past its last.
fn nth_of(selectors: &[u8], r: usize, m: usize) -> Option<usize> {
    selectors
        .iter()
        .enumerate()
        .filter(|&(_, &sel)| usize::from(sel & RUN) == r)
        .nth(m)
        .map(|(i, _)| i)
}

/// Bytes read from the front, each read refused past the end.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], Error> {
        let end = self.at.checked_add(n).ok_or(corrupt())?;
        let s = self.bytes.get(self.at..end).ok_or(corrupt())?;
        self.at = end;
        Ok(s)
    }

    fn n32(&mut self) -> Result<usize, Error> {
        let b = self.take(4)?.first_chunk::<4>().copied().ok_or(corrupt())?;
        usize::try_from(u32::from_le_bytes(b)).map_err(|_| corrupt())
    }

    fn u64(&mut self) -> Result<u64, Error> {
        let b = self.take(8)?.first_chunk::<8>().copied().ok_or(corrupt())?;
        Ok(u64::from_le_bytes(b))
    }
}

fn give_back<F: BlockFile>(store: &mut Store<F>, cursors: Vec<RunCursor>) {
    for c in cursors {
        c.give_back(store);
    }
}

/// A walk of a view's keys in order: each key's newest version, deletions included. One cursor a
/// run, placed at its
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
    /// A walk at the first key at or past `from`, at its newest version, a deletion too (a view
    /// above older levels hides their versions with it). The
    /// segment is found by the anchors, then the entry by a binary search of the segment
    /// (Zhong et al. §3.2): the entry at `j` is in the run its selector names, at that run's
    /// offset moved on by the same selector's occurrences before `j`, so a probe reads one key.
    /// The runs are then left behind by their occurrences before the entry found, and each is
    /// placed when a selector first names it.
    pub fn seek<F: BlockFile>(
        view: &View,
        store: &mut Store<F>,
        runs: &[Branch],
        from: &[u8],
    ) -> Result<Self, Error> {
        Self::seek_with(view, store, runs, from, WalkBufs::default())
    }

    /// [`Self::seek`] in the buffers of a walk given back: a scan that keeps them seeks without
    /// allocating once they have grown to its views' runs.
    pub fn seek_with<F: BlockFile>(
        view: &View,
        store: &mut Store<F>,
        runs: &[Branch],
        from: &[u8],
        bufs: WalkBufs,
    ) -> Result<Self, Error> {
        let WalkBufs {
            mut cursors,
            mut behind,
        } = bufs;
        cursors.clear();
        cursors.resize_with(view.runs, || None);
        behind.clear();
        behind.resize(view.runs, 0);
        let mut w = Self {
            segment: view.segment_of(from),
            at: 0,
            run: 0,
            cursors,
            behind,
            valid: view.segments() > 0,
        };
        if !w.valid {
            return Ok(w);
        }
        let selectors = view.selectors_of(w.segment).ok_or(corrupt())?;
        let found = w.search(view, store, runs, selectors, from);
        let found = match found {
            Ok(at) => at,
            Err(e) => {
                w.give_back(store);
                return Err(e);
            }
        };
        w.at = found;
        // A run the search probed is placed where the walk consumes it, its page kept when it
        // is the one held; a run it did not is placed when a selector names it.
        for r in 0..view.runs {
            let k = occurrences(selectors, w.at, r);
            let slot = w.cursors.get_mut(r).ok_or(corrupt())?;
            let behind = w.behind.get_mut(r).ok_or(corrupt())?;
            *behind = k;
            let Some(c) = slot.as_mut() else { continue };
            let b = runs.get(r).ok_or(corrupt())?;
            match b.position_after(view.offset(w.segment, r)?, k)? {
                Some(at) => {
                    c.place(b, store, at)?;
                    *behind = 0;
                }
                None => {
                    if let Some(c) = slot.take() {
                        c.give_back(store);
                    }
                }
            }
        }
        w.settle(view, store, runs)?;
        Ok(w)
    }

    /// The first entry of `selectors` (the segment's) whose key is at least `from`, by halving:
    /// its length when every key is less. Each probe reads its key through its run's own cursor,
    /// so a page a probe reads is the page the run is then placed on when the walk starts there.
    /// A probe's page then narrows the search by every key on it (Zhong et al. §3.2, the I/O
    /// optimization): the run's entries in the segment that sort before `from` are counted on
    /// the page, which puts the answer after the last of them and at or before the next, so a
    /// run is probed about once.
    fn search<F: BlockFile>(
        &mut self,
        view: &View,
        store: &mut Store<F>,
        runs: &[Branch],
        selectors: &[u8],
        from: &[u8],
    ) -> Result<usize, Error> {
        let (mut lo, mut hi) = (0usize, selectors.len());
        while lo < hi {
            let mid = lo.saturating_add(hi.saturating_sub(lo) / 2);
            let r = usize::from(selectors.get(mid).ok_or(corrupt())? & RUN);
            let b = runs.get(r).ok_or(corrupt())?;
            let offset = view.offset(self.segment, r)?;
            let k = occurrences(selectors, mid, r);
            let at = b.position_after(offset, k)?.ok_or(corrupt())?;
            let slot = self.cursors.get_mut(r).ok_or(corrupt())?;
            if slot.is_none() {
                *slot = Some(RunCursor::new(store)?);
            }
            let c = slot.as_mut().ok_or(corrupt())?;
            c.place(b, store, at)?;
            // The run's occurrences in the segment: those before `from` are counted on the page.
            let (_, index) = c.position();
            let (first_at_least, n) = c.page_lower_bound(from)?;
            let total = occurrences(selectors, selectors.len(), r);
            // The occurrence the page's entry `i` is: `k` at the cursor's entry.
            let occurrence_of = |i: usize| k.checked_add(i)?.checked_sub(index);
            if first_at_least < n {
                // The page's first entry at least `from`: the run's entries from it on are at
                // least `from`; those on the page before it are less (none is known less when
                // it is the page's first, as entries before the page may be at least `from`).
                if let Some(before) = occurrence_of(first_at_least) {
                    if first_at_least > 0
                        && let Some(p) = before.checked_sub(1).and_then(|o| nth_of(selectors, r, o))
                    {
                        lo = lo.max(p.saturating_add(1));
                    }
                    if before < total
                        && let Some(p) = nth_of(selectors, r, before)
                    {
                        hi = hi.min(p);
                    }
                } else {
                    // Even the segment's first of the run is at least `from`.
                    if let Some(p) = nth_of(selectors, r, 0) {
                        hi = hi.min(p);
                    }
                }
            } else if let Some(last) = occurrence_of(n.saturating_sub(1)) {
                // Every entry on the page is less than `from`: so is the run up to its last.
                if let Some(p) = nth_of(selectors, r, last.min(total.saturating_sub(1))) {
                    lo = lo.max(p.saturating_add(1));
                }
            }
            // The probe itself decides `mid`, whatever the page could say.
            if c.key() < from {
                lo = lo.max(mid.saturating_add(1));
            } else {
                hi = hi.min(mid);
            }
        }
        Ok(lo)
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
        runs: &[Branch],
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
        runs: &[Branch],
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
        runs: &[Branch],
    ) -> Result<(), Error> {
        let r = self.run;
        let b = runs.get(r).ok_or(corrupt())?;
        let slot = self.cursors.get_mut(r).ok_or(corrupt())?;
        if slot.is_none() {
            let (page, index) = view.offset(self.segment, r)?;
            *slot = Some(b.run_from(store, page, index)?);
        }
        let behind = self.behind.get_mut(r).ok_or(corrupt())?;
        if let Some(c) = slot.as_mut() {
            c.advance(b, store, *behind)?;
            if !c.valid() {
                return Err(corrupt());
            }
        }
        *behind = 0;
        Ok(())
    }

    /// Gives every placed cursor's page and span back to `store`.
    /// Gives the runs' cursors back to `store`'s pools; the walk's own buffers come back, empty,
    /// for the next [`Self::seek_with`].
    pub fn give_back<F: BlockFile>(mut self, store: &mut Store<F>) -> WalkBufs {
        for c in self.cursors.drain(..).flatten() {
            c.give_back(store);
        }
        self.behind.clear();
        WalkBufs {
            cursors: self.cursors,
            behind: self.behind,
        }
    }
}

/// A walk's buffers, kept between walks ([`Walk::seek_with`]).
#[derive(Debug, Default)]
pub struct WalkBufs {
    cursors: Vec<Option<RunCursor>>,
    behind: Vec<usize>,
}
