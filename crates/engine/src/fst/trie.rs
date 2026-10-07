//! The trie of a Fast Succinct Trie in LOUDS-DS (Zhang et al. SIGMOD 2018 §2.2–§2.4;
//! research/35): its upper levels in LOUDS-Dense, a node as a 256-bit bitmap of its labels and
//! one of the labels whose branch goes on, so a child is found by one bit test and one rank; its
//! lower levels in LOUDS-Sparse, a node's labels as bytes with a bit a label for whether its branch
//! goes on and a bit for whether it is the node's first. The levels are cut where the dense part
//! would pass 1/64 of the sparse part (the paper's R = 64).
//!
//! Where the paper marks a node whose path is itself a key with a 0xFF label (sparse) or a bit in
//! a third bitmap (dense), a bit a node marks it here at every level: a bit where the label took
//! eight, and no rule needed to tell it from a real 0xFF byte.
//!
//! Nodes are numbered breadth first, the root 0, across both parts; the branch that goes on at a
//! label leads to node `1 + (branches going on before it)`. Values are held in node order, the
//! dense levels' first: a node's own key's value, then its labels' that end a key.

use std::ops::ControlFlow;

use super::bits::{Bits, Grow};
use super::packed::{MAX_WIDTH, Packed, width_of};
use crate::error::{Error, Malformed};

fn corrupt() -> Error {
    Error::Corruption {
        what: "a succinct trie",
        why: Malformed::OutOfRange,
    }
}

/// Rank block bits for LOUDS-Sparse: a block in one cache line, 6.25% over the bits; for
/// LOUDS-Dense, a word, so a rank costs one popcount (research/35 §1).
const SPARSE_BLOCK: usize = 512;
const DENSE_BLOCK: usize = 64;

/// The dense part's bits at most this share of the sparse part's (the paper's R).
const DENSE_RATIO: usize = 64;

/// Bits a dense node takes: two 256-bit bitmaps and its own-key bit.
const DENSE_NODE_BITS: usize = 513;
/// Bits a sparse label takes: its byte, its has-child and node-start bits.
const SPARSE_LABEL_BITS: usize = 10;

/// The set bits of `bits` before position `p`.
fn before(bits: &Bits, p: usize) -> usize {
    p.checked_sub(1).map_or(0, |q| bits.rank1(q))
}

/// A static trie of byte keys, each with a value.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Trie {
    dense: Dense,
    sparse: Sparse,
    /// The dense levels' values in node order, then the sparse levels', at the bits the
    /// largest needs.
    values: Packed,
}

/// The upper levels: a node is 256 bits of each bitmap.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Dense {
    labels: Bits,
    has_child: Bits,
    /// A bit a node: whether the node's path is itself a key.
    prefix: Bits,
    nodes: usize,
    /// Branches going on, and values held, in the dense levels.
    children: usize,
    values: usize,
}

/// The lower levels.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Sparse {
    labels: Vec<u8>,
    has_child: Bits,
    louds: Bits,
    /// A bit a node, numbered from the first sparse node.
    prefix: Bits,
}

/// A node: dense, by its number; sparse, by its number less the dense nodes'.
#[derive(Clone, Copy, Debug)]
enum Node {
    Dense(usize),
    Sparse(usize),
}

/// One level of the trie as the builder fills it.
#[derive(Clone, Debug, Default)]
struct Level {
    labels: Vec<u8>,
    has_child: Grow,
    louds: Grow,
    /// A bit a node: whether its path is itself a key.
    prefix: Grow,
    /// The level's values in node order: a node's own key's, then its labels' that end keys.
    values: Packed,
}

impl Level {
    /// Empties the level, keeping its buffers, its values to be `width` bits.
    fn reset(&mut self, width: u32) -> Result<(), Error> {
        self.labels.clear();
        self.has_child.clear();
        self.louds.clear();
        self.prefix.clear();
        self.values.reset(width)
    }

    /// Adds `key`'s label at depth `d`, the first of a new node when `first`, with `value` when
    /// the label ends the key.
    fn push(&mut self, key: &[u8], d: usize, first: bool, value: u32) -> Result<(), Error> {
        let last = d.saturating_add(1) == key.len();
        self.labels.push(key.get(d).copied().unwrap_or(0));
        self.has_child.push(!last);
        self.louds.push(first);
        if last {
            self.values.push(value)?;
        }
        Ok(())
    }
}

/// Builds a trie of keys given in strictly ascending order, each with a value of at most a
/// width of bits, one pass and no key held but the last. Its levels keep their buffers when it
/// is reset, so a builder used again for a trie no larger allocates nothing as keys are added.
#[derive(Clone, Debug, Default)]
pub struct TrieBuilder {
    levels: Vec<Level>,
    /// Levels in use; those past it are kept for their buffers.
    depth: usize,
    width: u32,
    prev: Vec<u8>,
    keys: usize,
    /// The largest value given: the trie's values take the bits it needs.
    max: u32,
}

impl TrieBuilder {
    /// A builder for values of at most `width` bits.
    pub fn new(width: u32) -> Result<Self, Error> {
        let mut b = Self::default();
        b.reset(width)?;
        Ok(b)
    }

    /// Empties the builder for another trie, its values to be `width` bits, its buffers kept.
    pub fn reset(&mut self, width: u32) -> Result<(), Error> {
        if width > MAX_WIDTH {
            return Err(Error::InvalidArgument {
                what: "trie values wider than 32 bits",
            });
        }
        // Each level is emptied as it comes back into use.
        self.width = width;
        self.depth = 0;
        self.prev.clear();
        self.keys = 0;
        self.max = 0;
        // The root: a node with no labels yet.
        self.level(0)?.prefix.push(false);
        Ok(())
    }

    /// The keys added.
    pub fn len(&self) -> usize {
        self.keys
    }

    /// Whether no key has been added.
    pub fn is_empty(&self) -> bool {
        self.keys == 0
    }

    /// Level `d`, the levels down to it in use.
    fn level(&mut self, d: usize) -> Result<&mut Level, Error> {
        while self.depth <= d {
            if self.levels.len() <= self.depth {
                self.levels.push(Level::default());
            }
            let width = self.width;
            self.levels
                .get_mut(self.depth)
                .ok_or(corrupt())?
                .reset(width)?;
            self.depth = self.depth.saturating_add(1);
        }
        self.levels.get_mut(d).ok_or(corrupt())
    }

    /// Adds `key`, greater than every key added, with `value`.
    pub fn add(&mut self, key: &[u8], value: u32) -> Result<(), Error> {
        if self.keys > 0 && key <= self.prev.as_slice() {
            return Err(Error::InvalidArgument {
                what: "a trie's keys not strictly ascending",
            });
        }
        if width_of(value) > self.width {
            return Err(Error::InvalidArgument {
                what: "a trie value wider than its width",
            });
        }
        let first_key = self.keys == 0;
        self.keys = self.keys.checked_add(1).ok_or(corrupt())?;
        self.max = self.max.max(value);
        if key.is_empty() {
            // The empty key sorts first: the root's own.
            let root = self.level(0)?;
            root.prefix.set(0, true);
            return root.values.push(value);
        }
        let lcp = if first_key {
            0
        } else {
            self.prev
                .iter()
                .zip(key)
                .take_while(|(a, b)| a == b)
                .count()
        };
        if !first_key && lcp == self.prev.len() && !self.prev.is_empty() {
            // The previous key is a prefix of this one: its last label goes on, and the node
            // it now leads to holds it as its own key, its value the level's last.
            let d = self.prev.len().saturating_sub(1);
            let l = self.level(d)?;
            l.has_child.set_last(true);
            let ended = l.values.pop().ok_or(corrupt())?;
            let l = self.level(lcp)?;
            l.prefix.push(true);
            l.values.push(ended)?;
            l.push(key, lcp, true, value)?;
        } else {
            // A new label in the node the previous key's branch at this level is in, or the
            // root's first.
            let l = self.level(lcp)?;
            let first = lcp == 0 && l.labels.is_empty();
            l.push(key, lcp, first, value)?;
        }
        for d in lcp.saturating_add(1)..key.len() {
            let l = self.level(d)?;
            l.prefix.push(false);
            l.push(key, d, true, value)?;
        }
        self.prev.clear();
        self.prev.extend_from_slice(key);
        Ok(())
    }

    /// The trie of the keys added, its dense part cut where the ratio puts it.
    pub fn finish(&self) -> Result<Trie, Error> {
        self.finish_at(self.cutoff())
    }

    /// The levels in use.
    fn used(&self) -> &[Level] {
        self.levels.get(..self.depth).unwrap_or(&[])
    }

    /// The deepest level the dense part may reach: the largest `l` whose levels above it, dense,
    /// take at most 1/64 of the sparse levels from it down (Zhang et al. §2.4).
    fn cutoff(&self) -> usize {
        let dense = |l: &Level| l.prefix.len().saturating_mul(DENSE_NODE_BITS);
        let sparse = |l: &Level| l.labels.len().saturating_mul(SPARSE_LABEL_BITS);
        let mut below: usize = self.used().iter().map(sparse).sum();
        let mut above = 0usize;
        let mut cut = 0usize;
        for (l, level) in self.used().iter().enumerate() {
            above = above.saturating_add(dense(level));
            below = below.saturating_sub(sparse(level));
            if above.saturating_mul(DENSE_RATIO) <= below {
                cut = l.saturating_add(1);
            } else {
                break;
            }
        }
        cut
    }

    /// Levels above `cutoff` dense, the rest sparse, each breadth first; the values, in node
    /// order level by level, at the bits the largest needs.
    fn finish_at(&self, cutoff: usize) -> Result<Trie, Error> {
        let mut d_labels = Grow::default();
        let mut d_child = Grow::default();
        let mut d_prefix = Grow::default();
        let mut labels = Vec::new();
        let mut has_child = Grow::default();
        let mut louds = Grow::default();
        let mut prefix = Grow::default();
        let mut values = Packed::new(width_of(self.max))?;
        let mut dense_children = 0usize;
        let mut dense_values = 0usize;
        for (depth, l) in self.used().iter().enumerate() {
            if depth < cutoff {
                let mut at = 0usize;
                for node in 0..l.prefix.len() {
                    let base = d_labels.len();
                    let next = base.checked_add(256).ok_or(corrupt())?;
                    d_labels.grow_to(next);
                    d_child.grow_to(next);
                    d_prefix.push(l.prefix.get(node));
                    // The node's labels: the first, then each not starting another node.
                    let mut first = true;
                    while at < l.labels.len() && (first || !l.louds.get(at)) {
                        let label = l.labels.get(at).copied().ok_or(corrupt())?;
                        let goes_on = l.has_child.get(at);
                        let pos = base.saturating_add(usize::from(label));
                        d_labels.set(pos, true);
                        d_child.set(pos, goes_on);
                        dense_children = dense_children.saturating_add(usize::from(goes_on));
                        first = false;
                        at = at.saturating_add(1);
                    }
                }
            } else {
                labels.extend_from_slice(&l.labels);
                for i in 0..l.labels.len() {
                    has_child.push(l.has_child.get(i));
                    louds.push(l.louds.get(i));
                }
                for node in 0..l.prefix.len() {
                    prefix.push(l.prefix.get(node));
                }
            }
            for i in 0..l.values.len() {
                values.push(l.values.get(i).ok_or(corrupt())?)?;
            }
            if depth < cutoff {
                dense_values = values.len();
            }
        }
        if values.len() != self.keys {
            return Err(corrupt());
        }
        Ok(Trie {
            dense: Dense {
                nodes: d_prefix.len(),
                labels: Bits::from_grow(d_labels, DENSE_BLOCK)?,
                has_child: Bits::from_grow(d_child, DENSE_BLOCK)?,
                prefix: Bits::from_grow(d_prefix, DENSE_BLOCK)?,
                children: dense_children,
                values: dense_values,
            },
            sparse: Sparse {
                labels,
                has_child: Bits::from_grow(has_child, SPARSE_BLOCK)?,
                louds: Bits::from_grow(louds, SPARSE_BLOCK)?,
                prefix: Bits::from_grow(prefix, SPARSE_BLOCK)?,
            },
            values,
        })
    }
}

impl Trie {
    /// The trie of `keys` (strictly ascending) with `values`, one a key.
    pub fn build(keys: &[&[u8]], values: &[u32]) -> Result<Self, Error> {
        Self::builder(keys, values)?.finish()
    }

    fn builder(keys: &[&[u8]], values: &[u32]) -> Result<TrieBuilder, Error> {
        if keys.len() != values.len() {
            return Err(Error::InvalidArgument {
                what: "a trie's keys not one value a key",
            });
        }
        let mut b = TrieBuilder::new(width_of(values.iter().copied().max().unwrap_or(0)))?;
        for (k, v) in keys.iter().zip(values) {
            b.add(k, *v)?;
        }
        Ok(b)
    }

    /// Node number `n` in its part.
    fn node(&self, n: usize) -> Node {
        match n.checked_sub(self.dense.nodes) {
            Some(local) => Node::Sparse(local),
            None => Node::Dense(n),
        }
    }

    // ------------------------------------------------------------------ dense

    /// The value of the key the dense branch at `pos` ends.
    fn dense_value_at(&self, pos: usize) -> Option<u32> {
        let d = &self.dense;
        let ends_before = before(&d.labels, pos).saturating_sub(before(&d.has_child, pos));
        let prefixes = d.prefix.rank1(pos / 256);
        self.values.get(ends_before.saturating_add(prefixes))
    }

    /// The value of dense node `n`'s own key.
    fn dense_value_of(&self, n: usize) -> Option<u32> {
        let d = &self.dense;
        let start = n.checked_mul(256)?;
        let ends_before = before(&d.labels, start).saturating_sub(before(&d.has_child, start));
        self.values
            .get(ends_before.saturating_add(before(&d.prefix, n)))
    }

    /// The dense node the branch at `pos` goes on to.
    fn dense_child(&self, pos: usize) -> usize {
        before(&self.dense.has_child, pos).saturating_add(1)
    }

    /// The greatest label of dense node `n` below `pos` (exclusive), as a position.
    fn dense_below(&self, n: usize, pos: usize) -> Option<usize> {
        let d = &self.dense;
        let r = before(&d.labels, pos);
        (r > before(&d.labels, n.checked_mul(256)?))
            .then(|| d.labels.select1(r))
            .flatten()
    }

    // ------------------------------------------------------------------ sparse

    /// Sparse branches going on before label position `p`.
    fn children_before(&self, p: usize) -> usize {
        before(&self.sparse.has_child, p)
    }

    /// Sparse node `n`'s labels: its first position and the next node's.
    fn node_range(&self, n: usize) -> (usize, usize) {
        let s = &self.sparse;
        let start = n
            .checked_add(1)
            .and_then(|k| s.louds.select1(k))
            .unwrap_or(s.labels.len());
        let end = n
            .checked_add(2)
            .and_then(|k| s.louds.select1(k))
            .unwrap_or(s.labels.len());
        (start, end)
    }

    /// The node (numbered across both parts) the sparse branch at `p` goes on to.
    fn child(&self, p: usize) -> usize {
        self.dense
            .children
            .saturating_add(self.children_before(p))
            .saturating_add(1)
    }

    /// The value of the key the sparse branch at `p` ends.
    fn value_at_label(&self, p: usize) -> Option<u32> {
        let s = &self.sparse;
        let ends_before = p.saturating_sub(self.children_before(p));
        let n = s.louds.rank1(p).saturating_sub(1);
        let prefixes = s.prefix.rank1(n);
        self.values.get(
            self.dense
                .values
                .saturating_add(ends_before)
                .saturating_add(prefixes),
        )
    }

    /// The value of sparse node `n`'s own key.
    fn value_of_node(&self, n: usize) -> Option<u32> {
        let (start, _) = self.node_range(n);
        let ends_before = start.saturating_sub(self.children_before(start));
        self.values.get(
            self.dense
                .values
                .saturating_add(ends_before)
                .saturating_add(before(&self.sparse.prefix, n)),
        )
    }

    // ------------------------------------------------------------------ both

    /// Whether node `n`'s path is itself a key.
    fn is_key(&self, n: Node) -> bool {
        match n {
            Node::Dense(n) => self.dense.prefix.get(n),
            Node::Sparse(n) => self.sparse.prefix.get(n),
        }
    }

    /// The value of node `n`'s own key.
    fn value_of(&self, n: Node) -> Option<u32> {
        match n {
            Node::Dense(n) => self.dense_value_of(n),
            Node::Sparse(n) => self.value_of_node(n),
        }
    }

    /// The value `key` holds.
    pub fn get(&self, key: &[u8]) -> Option<u32> {
        let mut node = 0usize;
        for (d, &b) in key.iter().enumerate() {
            let last = d.saturating_add(1) == key.len();
            match self.node(node) {
                Node::Dense(n) => {
                    let pos = n.checked_mul(256)?.checked_add(usize::from(b))?;
                    if !self.dense.labels.get(pos) {
                        return None;
                    }
                    if !self.dense.has_child.get(pos) {
                        return if last { self.dense_value_at(pos) } else { None };
                    }
                    node = self.dense_child(pos);
                }
                Node::Sparse(n) => {
                    let (start, end) = self.node_range(n);
                    let i = self.sparse.labels.get(start..end)?.binary_search(&b).ok()?;
                    let p = start.checked_add(i)?;
                    if !self.sparse.has_child.get(p) {
                        return if last { self.value_at_label(p) } else { None };
                    }
                    node = self.child(p);
                }
            }
        }
        let n = self.node(node);
        if self.is_key(n) {
            self.value_of(n)
        } else {
            None
        }
    }

    /// The value of the greatest key at most `key`: the leaf a key falls in when the keys are
    /// the leaves' separators. One descent, remembering the nearest smaller branch passed; if
    /// the key's path ends above every key beneath it, the rightmost key under that branch.
    pub fn floor(&self, key: &[u8]) -> Option<u32> {
        let mut node = 0usize;
        // The nearest point below which every key is less than `key`.
        let mut fallback: Option<Fallback> = None;
        for &b in key {
            let n = self.node(node);
            if self.is_key(n) {
                fallback = Some(Fallback::Node(n));
            }
            match n {
                Node::Dense(n) => {
                    let pos = n.checked_mul(256)?.checked_add(usize::from(b))?;
                    if let Some(q) = self.dense_below(n, pos) {
                        fallback = Some(Fallback::Dense(q));
                    }
                    if !self.dense.labels.get(pos) {
                        return self.rightmost(fallback?);
                    }
                    if !self.dense.has_child.get(pos) {
                        // A key ending here: `key` itself, or a prefix of it.
                        return self.dense_value_at(pos);
                    }
                    node = self.dense_child(pos);
                }
                Node::Sparse(n) => {
                    let (start, end) = self.node_range(n);
                    let labels = self.sparse.labels.get(start..end).unwrap_or(&[]);
                    let (found, i) = match labels.binary_search(&b) {
                        Ok(i) => (true, i),
                        Err(i) => (false, i),
                    };
                    if let Some(smaller) = i.checked_sub(1) {
                        fallback = Some(Fallback::Sparse(start.saturating_add(smaller)));
                    }
                    if !found {
                        return self.rightmost(fallback?);
                    }
                    let p = start.saturating_add(i);
                    if !self.sparse.has_child.get(p) {
                        return self.value_at_label(p);
                    }
                    node = self.child(p);
                }
            }
        }
        // `key` ends at `node`: its own key, if any, is `key`; every other beneath is greater.
        let n = self.node(node);
        if self.is_key(n) {
            return self.value_of(n);
        }
        self.rightmost(fallback?)
    }

    /// The value of the greatest key at or under `from`.
    fn rightmost(&self, from: Fallback) -> Option<u32> {
        let mut at = from;
        // Down the last branch of each node: at most a node a step.
        for _ in 0..=self.dense.nodes.saturating_add(self.sparse.labels.len()) {
            let next = match at {
                Fallback::Node(n) => return self.value_of(n),
                Fallback::Dense(pos) => {
                    if !self.dense.has_child.get(pos) {
                        return self.dense_value_at(pos);
                    }
                    self.dense_child(pos)
                }
                Fallback::Sparse(p) => {
                    if !self.sparse.has_child.get(p) {
                        return self.value_at_label(p);
                    }
                    self.child(p)
                }
            };
            // The child's last label.
            at = match self.node(next) {
                Node::Dense(n) => {
                    Fallback::Dense(self.dense_below(n, n.checked_add(1)?.checked_mul(256)?)?)
                }
                Node::Sparse(n) => {
                    let (start, end) = self.node_range(n);
                    if end <= start {
                        return None;
                    }
                    Fallback::Sparse(end.saturating_sub(1))
                }
            };
        }
        None
    }

    /// The value of the least key at least `key`, the key itself written to `out`.
    pub fn ceil(&self, key: &[u8], out: &mut Vec<u8>) -> Option<u32> {
        match self.seek(key, out, |_, _| ControlFlow::Continue(())) {
            ControlFlow::Continue(v) => v,
            ControlFlow::Break(()) => None,
        }
    }

    /// One descent along `key`: `prefix` is given each stored key that is a proper prefix of
    /// `key`, shortest first, as its length and value, and may stop the descent; then the value
    /// of the least key at least `key`, the key written to `out`. The prefixes are what a range
    /// filter of truncated keys cannot order against `key` by the trie alone (research/35 §2).
    pub fn seek<F>(
        &self,
        key: &[u8],
        out: &mut Vec<u8>,
        mut prefix: F,
    ) -> ControlFlow<(), Option<u32>>
    where
        F: FnMut(usize, u32) -> ControlFlow<()>,
    {
        out.clear();
        let mut node = 0usize;
        // The nearest branch passed whose label is greater than the key's byte: the least key
        // beneath it is the least greater than `key` that the descent leaves behind.
        let mut greater: Option<(Fallback, usize)> = None;
        for (d, &b) in key.iter().enumerate() {
            let n = self.node(node);
            if self.is_key(n)
                && let Some(v) = self.value_of(n)
            {
                prefix(d, v)?;
            }
            let last = d.saturating_add(1) == key.len();
            match n {
                Node::Dense(n) => {
                    let Some(pos) = n
                        .checked_mul(256)
                        .and_then(|s| s.checked_add(usize::from(b)))
                    else {
                        break;
                    };
                    if let Some(q) = self.dense_above(n, pos) {
                        greater = Some((Fallback::Dense(q), d));
                    }
                    if !self.dense.labels.get(pos) {
                        break;
                    }
                    if !self.dense.has_child.get(pos) {
                        let v = self.dense_value_at(pos);
                        if last {
                            out.extend_from_slice(key);
                            return ControlFlow::Continue(v);
                        }
                        if let Some(v) = v {
                            prefix(d.saturating_add(1), v)?;
                        }
                        break;
                    }
                    node = self.dense_child(pos);
                }
                Node::Sparse(n) => {
                    let (start, end) = self.node_range(n);
                    let labels = self.sparse.labels.get(start..end).unwrap_or(&[]);
                    let (found, i) = match labels.binary_search(&b) {
                        Ok(i) => (true, i),
                        Err(i) => (false, i),
                    };
                    let above = if found { i.saturating_add(1) } else { i };
                    if above < labels.len() {
                        greater = Some((Fallback::Sparse(start.saturating_add(above)), d));
                    }
                    if !found {
                        break;
                    }
                    let p = start.saturating_add(i);
                    if !self.sparse.has_child.get(p) {
                        let v = self.value_at_label(p);
                        if last {
                            out.extend_from_slice(key);
                            return ControlFlow::Continue(v);
                        }
                        if let Some(v) = v {
                            prefix(d.saturating_add(1), v)?;
                        }
                        break;
                    }
                    node = self.child(p);
                }
            }
            if last {
                // `key` ends at the node the branch leads to: its own key is `key`, and every
                // other key beneath it is greater, the least its first label's.
                out.extend_from_slice(key);
                let n = self.node(node);
                if self.is_key(n) {
                    return ControlFlow::Continue(self.value_of(n));
                }
                let first = self.first_label(n);
                return ControlFlow::Continue(first.and_then(|f| self.leftmost(f, out)));
            }
        }
        if key.is_empty() {
            let n = self.node(0);
            if self.is_key(n) {
                return ControlFlow::Continue(self.value_of(n));
            }
            return ControlFlow::Continue(self.first_label(n).and_then(|f| self.leftmost(f, out)));
        }
        ControlFlow::Continue(greater.and_then(|(at, d)| {
            out.extend_from_slice(key.get(..d)?);
            self.leftmost(at, out)
        }))
    }

    /// The least label of dense node `n` above `pos` (exclusive), as a position.
    fn dense_above(&self, n: usize, pos: usize) -> Option<usize> {
        let q = self
            .dense
            .labels
            .select1(self.dense.labels.rank1(pos).checked_add(1)?)?;
        if q < n.checked_add(1)?.checked_mul(256)? {
            Some(q)
        } else {
            None
        }
    }

    /// Node `n`'s least label.
    fn first_label(&self, n: Node) -> Option<Fallback> {
        match n {
            Node::Dense(n) => {
                let start = n.checked_mul(256)?;
                let q = self
                    .dense
                    .labels
                    .select1(before(&self.dense.labels, start).checked_add(1)?)?;
                if q < start.checked_add(256)? {
                    Some(Fallback::Dense(q))
                } else {
                    None
                }
            }
            Node::Sparse(n) => {
                let (start, end) = self.node_range(n);
                if start < end {
                    Some(Fallback::Sparse(start))
                } else {
                    None
                }
            }
        }
    }

    /// The value of the least key at or under the label at `from`, its labels appended to `out`.
    fn leftmost(&self, from: Fallback, out: &mut Vec<u8>) -> Option<u32> {
        let mut at = from;
        // Down the first branch of each node: at most a node a step.
        for _ in 0..=self.dense.nodes.saturating_add(self.sparse.labels.len()) {
            let next = match at {
                Fallback::Node(n) => return self.value_of(n),
                Fallback::Dense(pos) => {
                    out.push(u8::try_from(pos % 256).ok()?);
                    if !self.dense.has_child.get(pos) {
                        return self.dense_value_at(pos);
                    }
                    self.dense_child(pos)
                }
                Fallback::Sparse(p) => {
                    out.push(*self.sparse.labels.get(p)?);
                    if !self.sparse.has_child.get(p) {
                        return self.value_at_label(p);
                    }
                    self.child(p)
                }
            };
            // A node's own key is the least beneath it.
            let n = self.node(next);
            at = if self.is_key(n) {
                Fallback::Node(n)
            } else {
                self.first_label(n)?
            };
        }
        None
    }

    /// Appends the trie to `out`: the dense part's three bit vectors and counts, the sparse
    /// part's labels and three bit vectors, and the values packed.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let put = |out: &mut Vec<u8>, n: usize| {
            out.extend_from_slice(&u64::try_from(n).unwrap_or(u64::MAX).to_le_bytes());
        };
        let d = &self.dense;
        d.labels.encode(out);
        d.has_child.encode(out);
        d.prefix.encode(out);
        put(out, d.nodes);
        put(out, d.children);
        put(out, d.values);
        let s = &self.sparse;
        put(out, s.labels.len());
        out.extend_from_slice(&s.labels);
        s.has_child.encode(out);
        s.louds.encode(out);
        s.prefix.encode(out);
        self.values.encode(out);
    }

    /// A trie [`Self::encode`] wrote at the start of `bytes`, and the bytes it took.
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), Error> {
        let mut at = 0usize;
        let rest = |at: usize| bytes.get(at..).ok_or(corrupt());
        let bits = |at: &mut usize| -> Result<Bits, Error> {
            let (b, used) = Bits::decode(rest(*at)?)?;
            *at = at.checked_add(used).ok_or(corrupt())?;
            Ok(b)
        };
        let labels_d = bits(&mut at)?;
        let child_d = bits(&mut at)?;
        let prefix_d = bits(&mut at)?;
        let num = |at: &mut usize| -> Result<usize, Error> {
            let v = rest(*at)?
                .first_chunk::<8>()
                .map(|b| u64::from_le_bytes(*b))
                .ok_or(corrupt())?;
            *at = at.checked_add(8).ok_or(corrupt())?;
            usize::try_from(v).map_err(|_| corrupt())
        };
        let nodes = num(&mut at)?;
        let children = num(&mut at)?;
        let dense_values = num(&mut at)?;
        let n = num(&mut at)?;
        let end = at.checked_add(n).ok_or(corrupt())?;
        let labels = bytes.get(at..end).ok_or(corrupt())?.to_vec();
        at = end;
        let bits = |at: &mut usize| -> Result<Bits, Error> {
            let (b, used) = Bits::decode(bytes.get(*at..).ok_or(corrupt())?)?;
            *at = at.checked_add(used).ok_or(corrupt())?;
            Ok(b)
        };
        let has_child = bits(&mut at)?;
        let louds = bits(&mut at)?;
        let prefix = bits(&mut at)?;
        let (values, used) = Packed::decode(bytes.get(at..).ok_or(corrupt())?)?;
        at = at.checked_add(used).ok_or(corrupt())?;
        // The parts agree: a dense node is 256 bits, the dense values and nodes are within the
        // whole, and the sparse part has a has-child and a node-start bit a label.
        if labels_d.len() != nodes.saturating_mul(256)
            || child_d.len() != labels_d.len()
            || prefix_d.len() != nodes
            || dense_values > values.len()
            || has_child.len() != labels.len()
            || louds.len() != labels.len()
        {
            return Err(corrupt());
        }
        Ok((
            Self {
                dense: Dense {
                    labels: labels_d,
                    has_child: child_d,
                    prefix: prefix_d,
                    nodes,
                    children,
                    values: dense_values,
                },
                sparse: Sparse {
                    labels,
                    has_child,
                    louds,
                    prefix,
                },
                values,
            },
            at,
        ))
    }

    /// The keys held.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether the trie holds no key.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Bytes held.
    pub fn bytes(&self) -> usize {
        let d = &self.dense;
        let s = &self.sparse;
        d.labels
            .bytes()
            .saturating_add(d.has_child.bytes())
            .saturating_add(d.prefix.bytes())
            .saturating_add(s.labels.len())
            .saturating_add(s.has_child.bytes())
            .saturating_add(s.louds.bytes())
            .saturating_add(s.prefix.bytes())
            .saturating_add(self.values.bytes())
    }
}

/// Where a floor search falls back to: a dense label position, a sparse one, or a node whose
/// own key it is.
#[derive(Clone, Copy, Debug)]
enum Fallback {
    Dense(usize),
    Sparse(usize),
    Node(Node),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn rng(x: &mut u64) -> u64 {
        *x ^= *x << 13;
        *x ^= *x >> 7;
        *x ^= *x << 17;
        *x
    }

    /// Keys from a small alphabet with 0x00 and 0xFF in it, lengths 0 to 6: prefixes of one
    /// another, shared paths, every edge.
    fn key(x: &mut u64) -> Vec<u8> {
        let len = (rng(x) % 7) as usize;
        (0..len)
            .map(|_| [0x00, 0x01, b'a', b'b', 0x7f, 0xfe, 0xff][(rng(x) % 7) as usize])
            .collect()
    }

    #[test]
    fn gets_and_floors_agree_with_an_ordered_map() {
        let mut x = 0x2545_f491_4f6c_dd1du64;
        for case in 0..400 {
            let n = (rng(&mut x) % 300) as usize;
            let map: BTreeMap<Vec<u8>, u32> = (0..n)
                .map(|i| (key(&mut x), u32::try_from(i).unwrap()))
                .collect();
            let keys: Vec<&[u8]> = map.keys().map(Vec::as_slice).collect();
            let values: Vec<u32> = map.values().copied().collect();
            let queries: Vec<Vec<u8>> = (0..300).map(|_| key(&mut x)).collect();
            // At every cut between the dense and sparse parts, all sparse to all dense.
            for cutoff in 0..=8 {
                let t = Trie::builder(&keys, &values)
                    .unwrap()
                    .finish_at(cutoff)
                    .unwrap();
                assert_eq!(t.len(), map.len());
                let mut out = Vec::new();
                t.encode(&mut out);
                assert_eq!(Trie::decode(&out).unwrap(), (t.clone(), out.len()));
                for (k, v) in &map {
                    assert_eq!(t.get(k), Some(*v), "case {case} cut {cutoff} get {k:?}");
                }
                for q in &queries {
                    assert_eq!(
                        t.get(q),
                        map.get(q).copied(),
                        "case {case} cut {cutoff} get {q:?}"
                    );
                    let want = map.range(..=q.clone()).next_back().map(|(_, v)| *v);
                    assert_eq!(t.floor(q), want, "case {case} cut {cutoff} floor {q:?}");
                }
            }
        }
    }

    #[test]
    fn ceilings_and_prefixes_agree_with_an_ordered_map() {
        let mut x = 0x5851_f42d_4c95_7f2du64;
        for case in 0..400 {
            let n = (rng(&mut x) % 300) as usize;
            let map: BTreeMap<Vec<u8>, u32> = (0..n)
                .map(|i| (key(&mut x), u32::try_from(i).unwrap()))
                .collect();
            let keys: Vec<&[u8]> = map.keys().map(Vec::as_slice).collect();
            let values: Vec<u32> = map.values().copied().collect();
            let mut queries: Vec<Vec<u8>> = (0..300).map(|_| key(&mut x)).collect();
            queries.extend(map.keys().cloned());
            for cutoff in 0..=8 {
                let t = Trie::builder(&keys, &values)
                    .unwrap()
                    .finish_at(cutoff)
                    .unwrap();
                let mut out = Vec::new();
                for q in &queries {
                    let want = map.range(q.clone()..).next();
                    let mut seen = Vec::new();
                    let got = t.seek(q, &mut out, |len, v| {
                        seen.push((len, v));
                        ControlFlow::Continue(())
                    });
                    assert_eq!(
                        got,
                        ControlFlow::Continue(want.map(|(_, v)| *v)),
                        "case {case} cut {cutoff} ceil {q:?}"
                    );
                    if let Some((k, _)) = want {
                        assert_eq!(&out, k, "case {case} cut {cutoff} ceil key {q:?}");
                    }
                    let prefixes: Vec<(usize, u32)> = (0..q.len())
                        .filter_map(|l| map.get(&q[..l]).map(|v| (l, *v)))
                        .collect();
                    assert_eq!(seen, prefixes, "case {case} cut {cutoff} prefixes {q:?}");
                    // Stopped at the first prefix, the descent says so.
                    if !prefixes.is_empty() {
                        assert_eq!(
                            t.seek(q, &mut out, |_, _| ControlFlow::Break(())),
                            ControlFlow::Break(())
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn a_builder_reset_builds_what_a_new_one_does() {
        let mut x = 0x1234_5678_9abc_def1u64;
        let mut reused = TrieBuilder::new(32).unwrap();
        for case in 0..200 {
            let n = (rng(&mut x) % 300) as usize;
            let map: BTreeMap<Vec<u8>, u32> = (0..n)
                .map(|_| (key(&mut x), (rng(&mut x) % 5_000) as u32))
                .collect();
            let keys: Vec<&[u8]> = map.keys().map(Vec::as_slice).collect();
            let values: Vec<u32> = map.values().copied().collect();
            let fresh = Trie::build(&keys, &values).unwrap();
            reused.reset(32).unwrap();
            for (k, v) in &map {
                reused.add(k, *v).unwrap();
            }
            assert_eq!(reused.finish().unwrap(), fresh, "case {case}");
            // Values packed at the bits the largest needs.
            let most = values.iter().copied().max().unwrap_or(0);
            assert_eq!(fresh.values.width(), width_of(most), "case {case}");
        }
    }

    #[test]
    fn a_truncated_trie_is_refused_typed() {
        let keys: Vec<Vec<u8>> = (0u32..500)
            .map(|n| format!("k{n:04}").into_bytes())
            .collect();
        let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        let values: Vec<u32> = (0..500).collect();
        let t = Trie::build(&refs, &values).unwrap();
        let mut out = Vec::new();
        t.encode(&mut out);
        for cut in 0..out.len() {
            assert!(Trie::decode(&out[..cut]).is_err(), "cut at {cut}");
        }
    }

    #[test]
    fn unordered_or_unpaired_keys_are_refused() {
        assert!(Trie::build(&[b"b", b"a"], &[1, 2]).is_err());
        assert!(Trie::build(&[b"a", b"a"], &[1, 2]).is_err());
        assert!(Trie::build(&[b"a"], &[]).is_err());
    }
}
