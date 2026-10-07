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

use super::bits::Bits;
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
    /// The dense levels' values in node order, then the sparse levels'.
    values: Vec<u32>,
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
#[derive(Default)]
struct Level {
    labels: Vec<u8>,
    has_child: Vec<bool>,
    louds: Vec<bool>,
    /// The key each label ends, if it ends one.
    ends: Vec<Option<usize>>,
    /// Each node's own key, if its path is one.
    prefix: Vec<Option<usize>>,
}

impl Trie {
    /// The trie of `keys` (strictly ascending) with `values`, one a key.
    pub fn build(keys: &[&[u8]], values: &[u32]) -> Result<Self, Error> {
        Self::build_at(keys, values, None)
    }

    /// [`Self::build`] with the dense part cut at `cutoff` levels, or where the ratio puts it.
    fn build_at(keys: &[&[u8]], values: &[u32], cutoff: Option<usize>) -> Result<Self, Error> {
        if keys.len() != values.len() || keys.windows(2).any(|w| matches!(w, [a, b] if a >= b)) {
            return Err(Error::InvalidArgument {
                what: "a trie's keys not strictly ascending, or not one value a key",
            });
        }
        let mut levels: Vec<Level> = Vec::new();
        let level = |levels: &mut Vec<Level>, d: usize| {
            while levels.len() <= d {
                levels.push(Level::default());
            }
        };
        // The root: a node with no labels yet.
        level(&mut levels, 0);
        if let Some(l) = levels.get_mut(0) {
            l.prefix.push(None);
        }
        let mut prev: Option<&[u8]> = None;
        for (i, &key) in keys.iter().enumerate() {
            let lcp = prev.map_or(0, |p| p.iter().zip(key).take_while(|(a, b)| a == b).count());
            let extends = prev.is_some_and(|p| lcp == p.len());
            if key.is_empty() {
                // The empty key sorts first: the root's own.
                if let Some(slot) = levels.get_mut(0).and_then(|l| l.prefix.get_mut(0)) {
                    *slot = Some(i);
                }
                prev = Some(key);
                continue;
            }
            if extends
                && let Some(p) = prev
                && !p.is_empty()
            {
                // The previous key is a prefix of this one: its last label goes on, and the
                // node it now leads to holds it as its own key.
                let d = p.len().saturating_sub(1);
                let l = levels.get_mut(d).ok_or(corrupt())?;
                if let Some(h) = l.has_child.last_mut() {
                    *h = true;
                }
                let ended = l.ends.last_mut().and_then(Option::take);
                level(&mut levels, lcp);
                let l = levels.get_mut(lcp).ok_or(corrupt())?;
                l.prefix.push(ended);
                Self::push(l, key, lcp, i, true);
            } else if lcp == 0 && levels.first().is_some_and(|l| l.labels.is_empty()) {
                // The root's first label.
                let l = levels.get_mut(0).ok_or(corrupt())?;
                Self::push(l, key, 0, i, true);
            } else {
                // A new label in the node the previous key's branch at this level is in.
                level(&mut levels, lcp);
                let l = levels.get_mut(lcp).ok_or(corrupt())?;
                Self::push(l, key, lcp, i, false);
            }
            for d in lcp.saturating_add(1)..key.len() {
                level(&mut levels, d);
                let l = levels.get_mut(d).ok_or(corrupt())?;
                l.prefix.push(None);
                Self::push(l, key, d, i, true);
            }
            prev = Some(key);
        }
        let cutoff = cutoff.unwrap_or_else(|| Self::cutoff(&levels));
        Self::assemble(&levels, values, cutoff)
    }

    /// The deepest level the dense part may reach: the largest `l` whose levels above it, dense,
    /// take at most 1/64 of the sparse levels from it down (Zhang et al. §2.4).
    fn cutoff(levels: &[Level]) -> usize {
        let dense = |l: &Level| l.prefix.len().saturating_mul(DENSE_NODE_BITS);
        let sparse = |l: &Level| l.labels.len().saturating_mul(SPARSE_LABEL_BITS);
        let mut below: usize = levels.iter().map(sparse).sum();
        let mut above = 0usize;
        let mut cut = 0usize;
        for (l, level) in levels.iter().enumerate() {
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

    /// Adds `key`'s label at depth `d` to level `l`: the first of a new node when `first`.
    fn push(l: &mut Level, key: &[u8], d: usize, i: usize, first: bool) {
        let last = d.saturating_add(1) == key.len();
        l.labels.push(key.get(d).copied().unwrap_or(0));
        l.has_child.push(!last);
        l.louds.push(first);
        l.ends.push(if last { Some(i) } else { None });
    }

    /// Levels above `cutoff` dense, the rest sparse, each breadth first, and the values in node
    /// order.
    fn assemble(levels: &[Level], values: &[u32], cutoff: usize) -> Result<Self, Error> {
        let mut d_labels = Vec::new();
        let mut d_child = Vec::new();
        let mut d_prefix = Vec::new();
        let mut labels = Vec::new();
        let mut has_child = Vec::new();
        let mut louds = Vec::new();
        let mut prefix = Vec::new();
        let mut ordered = Vec::with_capacity(values.len());
        let mut dense_children = 0usize;
        let mut dense_values = 0usize;
        for (depth, l) in levels.iter().enumerate() {
            let dense = depth < cutoff;
            // Node by node: its own key's value, then its labels' that end keys.
            let mut at = 0usize;
            for node_key in &l.prefix {
                let base = d_labels.len();
                if dense {
                    d_labels.resize(base.saturating_add(256), false);
                    d_child.resize(base.saturating_add(256), false);
                    d_prefix.push(node_key.is_some());
                } else {
                    prefix.push(node_key.is_some());
                }
                if let Some(k) = node_key {
                    ordered.push(*values.get(*k).ok_or(corrupt())?);
                }
                let mut first = true;
                while at < l.labels.len() && (first || !l.louds.get(at).copied().unwrap_or(true)) {
                    let label = l.labels.get(at).copied().ok_or(corrupt())?;
                    let goes_on = l.has_child.get(at).copied().ok_or(corrupt())?;
                    if dense {
                        let pos = base.saturating_add(usize::from(label));
                        if let Some(b) = d_labels.get_mut(pos) {
                            *b = true;
                        }
                        if let Some(b) = d_child.get_mut(pos) {
                            *b = goes_on;
                        }
                        dense_children = dense_children.saturating_add(usize::from(goes_on));
                    } else {
                        labels.push(label);
                        has_child.push(goes_on);
                        louds.push(first);
                    }
                    first = false;
                    if let Some(Some(k)) = l.ends.get(at) {
                        ordered.push(*values.get(*k).ok_or(corrupt())?);
                    }
                    at = at.saturating_add(1);
                }
            }
            if dense {
                dense_values = ordered.len();
            }
        }
        if ordered.len() != values.len() {
            return Err(corrupt());
        }
        Ok(Self {
            dense: Dense {
                labels: Bits::new(&d_labels, DENSE_BLOCK)?,
                has_child: Bits::new(&d_child, DENSE_BLOCK)?,
                nodes: d_prefix.len(),
                prefix: Bits::new(&d_prefix, DENSE_BLOCK)?,
                children: dense_children,
                values: dense_values,
            },
            sparse: Sparse {
                labels,
                has_child: Bits::new(&has_child, SPARSE_BLOCK)?,
                louds: Bits::new(&louds, SPARSE_BLOCK)?,
                prefix: Bits::new(&prefix, SPARSE_BLOCK)?,
            },
            values: ordered,
        })
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
        self.values
            .get(ends_before.saturating_add(prefixes))
            .copied()
    }

    /// The value of dense node `n`'s own key.
    fn dense_value_of(&self, n: usize) -> Option<u32> {
        let d = &self.dense;
        let start = n.checked_mul(256)?;
        let ends_before = before(&d.labels, start).saturating_sub(before(&d.has_child, start));
        self.values
            .get(ends_before.saturating_add(before(&d.prefix, n)))
            .copied()
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
        self.values
            .get(
                self.dense
                    .values
                    .saturating_add(ends_before)
                    .saturating_add(prefixes),
            )
            .copied()
    }

    /// The value of sparse node `n`'s own key.
    fn value_of_node(&self, n: usize) -> Option<u32> {
        let (start, _) = self.node_range(n);
        let ends_before = start.saturating_sub(self.children_before(start));
        self.values
            .get(
                self.dense
                    .values
                    .saturating_add(ends_before)
                    .saturating_add(before(&self.sparse.prefix, n)),
            )
            .copied()
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
            .saturating_add(self.values.len().saturating_mul(4))
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
                let t = Trie::build_at(&keys, &values, Some(cutoff)).unwrap();
                assert_eq!(t.len(), map.len());
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
    fn unordered_or_unpaired_keys_are_refused() {
        assert!(Trie::build(&[b"b", b"a"], &[1, 2]).is_err());
        assert!(Trie::build(&[b"a", b"a"], &[1, 2]).is_err());
        assert!(Trie::build(&[b"a"], &[]).is_err());
    }
}
