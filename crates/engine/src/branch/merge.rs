//! Merging branches (docs/design/engine-structure.md §4, step E4): their entries in key order,
//! the newest branch's entry for each key and no other, as SplinterDB's merge iterator gives them
//! to a compaction (research/34 §1).

use std::cmp::Ordering;

use super::filter::Keys;
use super::{Branch, Builder, Initial, Op, RunCursor};
use crate::error::Error;
use crate::store::Store;
use hyper_block::block::BlockFile;

/// A merge over cursors given newest first, from a start key up to an end key (exclusive, none
/// for the end of the branches).
///
/// The cursors play a tournament kept as a tree of losers (Knuth, The Art of Computer
/// Programming vol. 3 §5.4.1): each internal node holds the cursor that lost the match below
/// it, the root's slot the winner, the cursor at the least key in range (of equal keys the
/// newest). A step that moves the winner plays its path again, a comparison a level, where a
/// scan of every cursor took two a cursor; the runner-up, the best of the losers on the
/// winner's path, lets a winner whose next key stays below it go on with one comparison.
#[derive(Debug)]
pub struct Merge {
    cursors: Vec<RunCursor>,
    end: Option<Vec<u8>>,
    /// The cursor whose entry is current, if any.
    current: Option<usize>,
    /// The cursor at the least key of the others, as the tournament last left it: while the
    /// current key stays below its key, no other cursor holds the current key, so a step moves
    /// the current cursor alone and compares its next key with this one only.
    second: Option<usize>,
    /// The tournament: slot 0 the winner, slots `1..k` each internal node's loser, the cursors
    /// at leaf positions `k..2k` (node `n`'s children `2n` and `2n + 1`).
    tree: Vec<usize>,
    /// Each node's winner while the tournament is played whole, kept for the next.
    wins: Vec<usize>,
    /// The key `next` moves past, its buffer reused so a step allocates nothing.
    past: Vec<u8>,
    /// While opening, `current` is the next input and `past` holds its from-key.
    opening: bool,
}

impl Merge {
    /// A merge of `branches`, newest first, over `[from, end)`.
    pub fn new<'a, F: BlockFile>(
        store: &mut Store<F>,
        branches: impl IntoIterator<Item = &'a Branch>,
        from: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Self, Error> {
        Self::with(store, branches, from, end, false)
    }

    /// [`Self::new`] for a compaction, which reads its inputs to their end: an extent a read
    /// from the first.
    pub fn sequential<'a, F: BlockFile>(
        store: &mut Store<F>,
        branches: impl IntoIterator<Item = &'a Branch>,
        from: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Self, Error> {
        Self::with(store, branches, from, end, true)
    }

    fn with<'a, F: BlockFile>(
        store: &mut Store<F>,
        branches: impl IntoIterator<Item = &'a Branch>,
        from: &[u8],
        end: Option<&[u8]>,
        sequential: bool,
    ) -> Result<Self, Error> {
        let mut cursors = Vec::new();
        for b in branches {
            let cursor = if sequential {
                b.seek_sequential(store, from)
            } else {
                b.seek(store, from)
            };
            match cursor {
                Ok(cursor) => cursors.push(cursor),
                Err(error) => {
                    for cursor in cursors {
                        cursor.give_back(store);
                    }
                    return Err(error);
                }
            }
        }
        let mut merge = Self {
            cursors,
            end: end.map(<[u8]>::to_vec),
            current: None,
            second: None,
            tree: Vec::new(),
            wins: Vec::new(),
            past: Vec::new(),
            opening: false,
        };
        merge.pick();
        Ok(merge)
    }

    fn prepared(from: Vec<u8>, end: Option<Vec<u8>>) -> Self {
        Self {
            cursors: Vec::new(),
            end,
            current: Some(0),
            second: None,
            tree: Vec::new(),
            wins: Vec::new(),
            past: from,
            opening: true,
        }
    }

    /// Opens one input leaf. Partial cursors retain only their existing span and page owners.
    fn open<'a, F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        branches: impl IntoIterator<Item = &'a Branch>,
        count: usize,
        yield_io: bool,
    ) -> Result<bool, Error> {
        if !self.opening {
            return Ok(true);
        }
        let at = self.current.ok_or(Error::InvalidArgument {
            what: "a compaction opened over inconsistent inputs",
        })?;
        if count == 0 {
            self.opening = false;
            self.past.clear();
            self.pick();
            return Ok(true);
        }
        let branch = branches.into_iter().nth(at).ok_or(Error::InvalidArgument {
            what: "a compaction opened over inconsistent inputs",
        })?;
        if self.cursors.len() == at {
            self.cursors
                .push(RunCursor::prepare(branch, store, &self.past)?);
        }
        let cursor = self.cursors.get_mut(at).ok_or(Error::InvalidArgument {
            what: "a compaction opened over inconsistent inputs",
        })?;
        match cursor.begin(branch, store, &self.past, yield_io)? {
            Initial::Waiting => return Ok(false),
            Initial::More => return Ok(true),
            Initial::Done => self.current = Some(at.saturating_add(1)),
        }
        if self.current == Some(count) {
            self.opening = false;
            self.past.clear();
            self.pick();
        }
        Ok(true)
    }

    /// An opening failure returns every partial cursor, including unclaimed read buffers.
    fn cancel_opening<F: BlockFile>(&mut self, store: &mut Store<F>) {
        for cursor in self.cursors.drain(..) {
            cursor.give_back(store);
        }
        self.current = Some(0);
        self.opening = true;
    }

    /// Gives every cursor's span back to `store`'s pool: the merge is done with them.
    pub fn give_back<F: BlockFile>(self, store: &mut Store<F>) {
        for c in self.cursors {
            c.give_back(store);
        }
    }

    /// Whether cursor `c` holds an entry in range: valid and below the end.
    fn live(&self, c: &RunCursor) -> bool {
        c.valid() && self.end.as_deref().is_none_or(|end| c.key() < end)
    }

    /// Whether cursor `a` comes before cursor `b`: one in range before one not, then the less
    /// key, then of equal keys the newer (the earlier input).
    fn before(&self, a: usize, b: usize) -> bool {
        let live = |i: usize| self.cursors.get(i).filter(|c| self.live(c));
        match (live(a), live(b)) {
            (Some(x), Some(y)) => match x.key().cmp(y.key()) {
                Ordering::Less => true,
                Ordering::Greater => false,
                Ordering::Equal => a < b,
            },
            (Some(_), None) => true,
            (None, _) => false,
        }
    }

    /// Plays the tournament whole: each node's match between its children's winners, from
    /// the bottom up, then the winner and runner-up.
    fn pick(&mut self) {
        let k = self.cursors.len();
        self.tree.clear();
        self.tree.resize(k.max(1), 0);
        self.wins.clear();
        self.wins.resize(k.saturating_mul(2), 0);
        for i in 0..k {
            if let Some(w) = self.wins.get_mut(k.saturating_add(i)) {
                *w = i;
            }
        }
        for node in (1..k).rev() {
            let left = self.wins.get(node.saturating_mul(2)).copied().unwrap_or(0);
            let right = self
                .wins
                .get(node.saturating_mul(2).saturating_add(1))
                .copied()
                .unwrap_or(0);
            let (winner, loser) = if self.before(right, left) {
                (right, left)
            } else {
                (left, right)
            };
            if let Some(w) = self.wins.get_mut(node) {
                *w = winner;
            }
            if let Some(l) = self.tree.get_mut(node) {
                *l = loser;
            }
        }
        let winner = if k > 1 {
            self.wins.get(1).copied().unwrap_or(0)
        } else {
            0
        };
        if let Some(w) = self.tree.first_mut() {
            *w = winner;
        }
        self.settle();
    }

    /// Plays cursor `i`'s path again after it moved: at each node on the way up the better of
    /// it and the node's loser goes on and the other stays.
    fn replay(&mut self, i: usize) {
        let k = self.cursors.len();
        let mut winner = i;
        let mut node = k.saturating_add(i) >> 1;
        // A level a step: at most the bits of an index.
        for _ in 0..=usize::BITS {
            if node == 0 {
                break;
            }
            if let Some(&loser) = self.tree.get(node)
                && self.before(loser, winner)
            {
                if let Some(l) = self.tree.get_mut(node) {
                    *l = winner;
                }
                winner = loser;
            }
            node >>= 1;
        }
        if let Some(w) = self.tree.first_mut() {
            *w = winner;
        }
    }

    /// The current cursor, the winner if it holds an entry in range, and the runner-up: the best
    /// of the losers on the winner's path, every one of which the winner beat, so the cursor
    /// the winner alone stands before.
    fn settle(&mut self) {
        let k = self.cursors.len();
        let winner = if k == 0 {
            None
        } else {
            self.tree.first().copied()
        };
        self.current = winner.filter(|&w| self.cursors.get(w).is_some_and(|c| self.live(c)));
        let mut second: Option<usize> = None;
        if let Some(w) = winner {
            let mut node = k.saturating_add(w) >> 1;
            for _ in 0..=usize::BITS {
                if node == 0 {
                    break;
                }
                if let Some(&loser) = self.tree.get(node)
                    && second.is_none_or(|s| self.before(loser, s))
                {
                    second = Some(loser);
                }
                node >>= 1;
            }
        }
        self.second = second.filter(|&s| self.cursors.get(s).is_some_and(|c| self.live(c)));
    }

    /// Whether the current key is below every other cursor's: no other cursor holds it.
    fn alone(&self, at: &RunCursor) -> bool {
        self.second
            .and_then(|s| self.cursors.get(s))
            .is_none_or(|s| !s.valid() || at.key() < s.key())
    }

    /// Whether moving past the current key reads no page that has not landed: every cursor
    /// holding it either stays in its leaf or has its next leaf ready ([`RunCursor::next_ready`]).
    pub fn ready<'a, F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        branches: impl IntoIterator<Item = &'a Branch>,
    ) -> Result<bool, Error> {
        if self.opening {
            return Ok(true);
        }
        let Some(i) = self.current else {
            return Ok(true);
        };
        let Some(at) = self.cursors.get(i) else {
            return Ok(true);
        };
        if self.alone(at) {
            let b = branches.into_iter().nth(i).ok_or(Error::InvalidArgument {
                what: "a merge stepped over inputs it was not opened on",
            })?;
            return match self.cursors.get_mut(i) {
                Some(c) => c.next_ready(b, store),
                None => Ok(true),
            };
        }
        self.past.clear();
        self.past.extend_from_slice(at.key());
        for (c, b) in self.cursors.iter_mut().zip(branches) {
            if c.valid() && c.key() == self.past.as_slice() && !c.next_ready(b, store)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// The current entry: key, operation, value.
    pub fn entry(&self) -> Option<(&[u8], Op, &[u8])> {
        if self.opening {
            return None;
        }
        self.current
            .and_then(|i| self.cursors.get(i))
            .map(|c| (c.key(), c.op(), c.value()))
    }

    /// Moves past the current key in every cursor that holds it; the cursors moved, each an
    /// input entry consumed. `branches` are the merge's, in the order it was opened on: a
    /// cursor moving to its next leaf reads it by its branch's page counts. The winner moves
    /// and plays its path again; an older cursor holding the same key surfaces as the next
    /// winner and moves in turn.
    pub fn next<'a, F: BlockFile, I>(
        &mut self,
        store: &mut Store<F>,
        branches: I,
    ) -> Result<u64, Error>
    where
        I: IntoIterator<Item = &'a Branch> + Clone,
    {
        if self.opening {
            return Ok(0);
        }
        let Some(i) = self.current else {
            return Ok(0);
        };
        let Some(at) = self.cursors.get(i) else {
            return Ok(0);
        };
        if self.alone(at) {
            // Only this cursor holds the key: it moves alone, and stays the winner while its
            // next key is below the runner-up's, every other loser being no better.
            self.advance(store, branches, i)?;
            let stays = self
                .cursors
                .get(i)
                .is_some_and(|c| self.live(c) && self.alone(c));
            if !stays {
                self.replay(i);
                self.settle();
            }
            return Ok(1);
        }
        self.past.clear();
        self.past.extend_from_slice(at.key());
        let mut moved = 0u64;
        let mut winner = i;
        // Each cursor holds a key once: at most every cursor moves.
        for _ in 0..self.cursors.len() {
            self.advance(store, branches.clone(), winner)?;
            moved = moved.saturating_add(1);
            self.replay(winner);
            let Some(&next) = self.tree.first() else {
                break;
            };
            let same = self
                .cursors
                .get(next)
                .is_some_and(|c| self.live(c) && c.key() == self.past.as_slice());
            if !same {
                break;
            }
            winner = next;
        }
        self.settle();
        Ok(moved)
    }

    /// Moves cursor `i` past its key, reading its next leaf by its branch's page counts.
    fn advance<'a, F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        branches: impl IntoIterator<Item = &'a Branch>,
        i: usize,
    ) -> Result<(), Error> {
        let b = branches.into_iter().nth(i).ok_or(Error::InvalidArgument {
            what: "a merge stepped over inputs it was not opened on",
        })?;
        let c = self.cursors.get_mut(i).ok_or(Error::InvalidArgument {
            what: "a merge stepped over inputs it was not opened on",
        })?;
        c.next(b, store)
    }
}

/// A compaction run a slice at a time ([`Compaction::step`]): `branches` (newest first) merged
/// over `[from, end)` into branches of at most `per` entries each, the newest entry of each key,
/// tombstones dropped when `drop_tombstones` (nothing older lies below). Its inputs are not
/// changed while it runs, so it is resumed between any other reads of the store.
#[derive(Debug)]
pub struct Compaction {
    merge: Merge,
    /// The inputs' roots, newest first: each step is given the inputs again and checks them.
    roots: Vec<u64>,
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
    /// Whether a step stops when an input's next page has not landed ([`Self::set_yield`]),
    /// and whether the last step did.
    yield_io: bool,
    waiting: bool,
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
        let mut roots = Vec::new();
        let counted = branches.into_iter().inspect(|b| {
            remaining = remaining.saturating_add(b.count);
            roots.push(b.root);
        });
        let merge = Merge::sequential(store, counted, from, end)?;
        Ok(Self {
            merge,
            roots,
            drop_tombstones,
            per: per.max(1),
            building: None,
            closing: std::collections::VecDeque::new(),
            out: Vec::new(),
            remaining,
            yield_io: false,
            waiting: false,
        })
    }

    /// The trunk's private plan: retain the existing from-key buffer and open input leaves in
    /// budgeted steps, after yielding has been selected. Public constructors remain immediate.
    pub(crate) fn prepare<'a>(
        branches: impl IntoIterator<Item = &'a Branch>,
        from: Vec<u8>,
        end: Option<Vec<u8>>,
        drop_tombstones: bool,
        per: u64,
    ) -> Self {
        let mut remaining = 0u64;
        let mut roots = Vec::new();
        for branch in branches {
            remaining = remaining.saturating_add(branch.count);
            roots.push(branch.root);
        }
        let mut merge = Merge::prepared(from, end);
        if roots.is_empty() {
            merge.opening = false;
            merge.current = None;
            merge.past.clear();
        }
        Self {
            merge,
            roots,
            drop_tombstones,
            per: per.max(1),
            building: None,
            closing: std::collections::VecDeque::new(),
            out: Vec::new(),
            remaining,
            yield_io: false,
            waiting: false,
        }
    }

    /// Whether a step stops, rather than wait, when an initial or next input page has not landed from
    /// the device ([`Store::ready`]): for a step a put paces, which the device's latency must
    /// not hold. Steps of idle time and of a stall wait, and always finish their budget.
    pub fn set_yield(&mut self, on: bool) {
        self.yield_io = on;
    }

    /// Only an unfinished private opening can restart after its owners have been returned.
    pub(crate) fn opening(&self) -> bool {
        self.merge.opening
    }

    /// Whether the last step stopped for a page still being read.
    pub fn waiting(&self) -> bool {
        self.waiting
    }

    /// The work estimate: input entries left, one unit per input still opening, and filter
    /// pages at their worth in keys. A gap may take another leaf move; its opening stays owed
    /// until the input is positioned.
    pub fn remaining(&self) -> u64 {
        let opening = if self.merge.opening {
            self.merge
                .current
                .map_or(self.roots.len(), |at| self.roots.len().saturating_sub(at))
        } else {
            0
        };
        let remaining = self
            .remaining
            .saturating_add(u64::try_from(opening).unwrap_or(u64::MAX));
        // The filter pages left to write, each at its worth in keys.
        let filters = |b: &Builder| b.filter_pages_left().saturating_mul(b.page_keys());
        self.closing
            .iter()
            .map(|(_, b)| filters(b))
            .chain(self.building.iter().map(|(_, b, _)| filters(b)))
            .fold(remaining, u64::saturating_add)
    }

    /// Works for up to `budget`: a private plan opens one input leaf per unit first, yielding
    /// before an unlanded read; then the filter pages of branches already sealed, each at its
    /// worth in keys ([`Builder::page_keys`]), then keys merged; a part that reaches its size, or
    /// the last once the merge is done, is sealed and its filter's pages wait their turn. Returns
    /// the budget used, less than `budget` only once the compaction is done or, when it yields
    /// ([`Self::set_yield`]), stopped for a page still being read ([`Self::waiting`]).
    pub fn step<'a, F: BlockFile, I>(
        &mut self,
        store: &mut Store<F>,
        inputs: I,
        budget: u64,
    ) -> Result<u64, Error>
    where
        I: IntoIterator<Item = &'a Branch> + Clone,
    {
        // The inputs are the ones the compaction was opened on, in its order.
        let mut n = 0usize;
        for b in inputs.clone() {
            if self.roots.get(n) != Some(&b.root) {
                if self.merge.opening {
                    self.merge.cancel_opening(store);
                }
                return Err(Error::InvalidArgument {
                    what: "a compaction stepped over inputs it was not opened on",
                });
            }
            n = n.saturating_add(1);
        }
        if n != self.roots.len() {
            if self.merge.opening {
                self.merge.cancel_opening(store);
            }
            return Err(Error::InvalidArgument {
                what: "a compaction stepped over inputs it was not opened on",
            });
        }
        let mut done = 0u64;
        self.waiting = false;
        while done < budget {
            if self.merge.opening {
                match self
                    .merge
                    .open(store, inputs.clone(), self.roots.len(), self.yield_io)
                {
                    Ok(true) => {
                        done = done.saturating_add(1);
                        continue;
                    }
                    Ok(false) => {
                        self.waiting = true;
                        break;
                    }
                    Err(error) => {
                        self.merge.cancel_opening(store);
                        return Err(error);
                    }
                }
            }
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
            if self.yield_io && !self.merge.ready(store, inputs.clone())? {
                self.waiting = true;
                break;
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
            let consumed = self.merge.next(store, inputs.clone())?;
            self.remaining = self.remaining.saturating_sub(consumed);
            done = done.saturating_add(1);
        }
        Ok(done.min(budget))
    }

    /// Whether every key is merged and every branch made is whole, its filter written.
    pub fn is_done(&self) -> bool {
        !self.merge.opening
            && self.merge.entry().is_none()
            && self.building.is_none()
            && self.closing.is_empty()
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
    c.step(store, branches, u64::MAX)?;
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
    c.step(store, branches, u64::MAX)?;
    c.finish(store)
}
