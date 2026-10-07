//! The trie of a Fast Succinct Trie in LOUDS-Sparse (Zhang et al. SIGMOD 2018 §2.3; research/35):
//! every node's branch labels in breadth-first order, a bit a label for whether its branch goes
//! on to a child, a bit a label for whether it is its node's first. Where the paper marks a
//! node whose path is itself a key with a 0xFF label first in the node, a bit a node marks it
//! here: a bit where the label took eight, and no rule needed to tell it from a real 0xFF byte.
//!
//! Nodes are numbered breadth first, the root 0; the branch at label position `p` that goes on
//! leads to node `1 + (branches going on before p)`. Values are held in node order: a node's
//! own key's first, then its labels' that end a key.

use super::bits::Bits;
use crate::error::{Error, Malformed};

fn corrupt() -> Error {
    Error::Corruption {
        what: "a succinct trie",
        why: Malformed::OutOfRange,
    }
}

/// Rank block bits for LOUDS-Sparse: a block in one cache line, 6.25% over the bits
/// (research/35 §1).
const SPARSE_BLOCK: usize = 512;

/// A static trie of byte keys, each with a value.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Trie {
    labels: Vec<u8>,
    has_child: Bits,
    louds: Bits,
    /// A bit a node: whether the node's path is itself a key.
    prefix: Bits,
    values: Vec<u32>,
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
        Self::assemble(&levels, values)
    }

    /// Adds `key`'s label at depth `d` to level `l`: the first of a new node when `first`.
    fn push(l: &mut Level, key: &[u8], d: usize, i: usize, first: bool) {
        let last = d.saturating_add(1) == key.len();
        l.labels.push(key.get(d).copied().unwrap_or(0));
        l.has_child.push(!last);
        l.louds.push(first);
        l.ends.push(if last { Some(i) } else { None });
    }

    /// The levels concatenated breadth first, and the values in node order.
    fn assemble(levels: &[Level], values: &[u32]) -> Result<Self, Error> {
        let mut labels = Vec::new();
        let mut has_child = Vec::new();
        let mut louds = Vec::new();
        let mut prefix = Vec::new();
        let mut ordered = Vec::with_capacity(values.len());
        for l in levels {
            labels.extend_from_slice(&l.labels);
            has_child.extend_from_slice(&l.has_child);
            louds.extend_from_slice(&l.louds);
            prefix.extend(l.prefix.iter().map(Option::is_some));
            // Node by node: its own key's value, then its labels' that end keys.
            let mut node = 0usize;
            let mut at = 0usize;
            while node < l.prefix.len() {
                if let Some(Some(k)) = l.prefix.get(node) {
                    ordered.push(*values.get(*k).ok_or(corrupt())?);
                }
                // The node's labels: from its first to the next node's.
                let mut first = true;
                while at < l.labels.len() && (first || !l.louds.get(at).copied().unwrap_or(true)) {
                    first = false;
                    if let Some(Some(k)) = l.ends.get(at) {
                        ordered.push(*values.get(*k).ok_or(corrupt())?);
                    }
                    at = at.saturating_add(1);
                }
                node = node.saturating_add(1);
            }
        }
        if ordered.len() != values.len() {
            return Err(corrupt());
        }
        Ok(Self {
            labels,
            has_child: Bits::new(&has_child, SPARSE_BLOCK)?,
            louds: Bits::new(&louds, SPARSE_BLOCK)?,
            prefix: Bits::new(&prefix, SPARSE_BLOCK)?,
            values: ordered,
        })
    }

    /// Branches going on before label position `p`.
    fn children_before(&self, p: usize) -> usize {
        match p.checked_sub(1) {
            Some(q) => self.has_child.rank1(q),
            None => 0,
        }
    }

    /// Node `n`'s labels: its first position and the next node's.
    fn node_range(&self, n: usize) -> (usize, usize) {
        let start = n
            .checked_add(1)
            .and_then(|k| self.louds.select1(k))
            .unwrap_or(self.labels.len());
        let end = n
            .checked_add(2)
            .and_then(|k| self.louds.select1(k))
            .unwrap_or(self.labels.len());
        (start, end)
    }

    /// The node the branch at `p` goes on to.
    fn child(&self, p: usize) -> usize {
        self.children_before(p).saturating_add(1)
    }

    /// The node holding label position `p`.
    fn node_of(&self, p: usize) -> usize {
        self.louds.rank1(p).saturating_sub(1)
    }

    /// Prefix keys among nodes `0..n`.
    fn prefixes_before(&self, n: usize) -> usize {
        match n.checked_sub(1) {
            Some(m) => self.prefix.rank1(m),
            None => 0,
        }
    }

    /// The value of the key the branch at `p` ends.
    fn value_at_label(&self, p: usize) -> Option<u32> {
        let ends_before = p.saturating_sub(self.children_before(p));
        let n = self.node_of(p);
        let prefixes = self
            .prefixes_before(n)
            .saturating_add(usize::from(self.prefix.get(n)));
        self.values
            .get(ends_before.saturating_add(prefixes))
            .copied()
    }

    /// The value of node `n`'s own key.
    fn value_of_node(&self, n: usize) -> Option<u32> {
        let (start, _) = self.node_range(n);
        let ends_before = start.saturating_sub(self.children_before(start));
        self.values
            .get(ends_before.saturating_add(self.prefixes_before(n)))
            .copied()
    }

    /// The value `key` holds.
    pub fn get(&self, key: &[u8]) -> Option<u32> {
        let mut node = 0usize;
        for (d, &b) in key.iter().enumerate() {
            let (start, end) = self.node_range(node);
            let labels = self.labels.get(start..end)?;
            let i = labels.binary_search(&b).ok()?;
            let p = start.checked_add(i)?;
            if !self.has_child.get(p) {
                return (d.saturating_add(1) == key.len())
                    .then(|| self.value_at_label(p))
                    .flatten();
            }
            node = self.child(p);
        }
        if self.prefix.get(node) {
            self.value_of_node(node)
        } else {
            None
        }
    }

    /// The value of the greatest key at most `key`: the leaf a key falls in when the keys are
    /// the leaves' separators. One descent, remembering the nearest smaller branch passed; if
    /// the key's path ends above every key beneath it, the rightmost key under that branch.
    pub fn floor(&self, key: &[u8]) -> Option<u32> {
        let mut node = 0usize;
        // The nearest point below which every key is less than `key`: a label position, or a
        // node whose own key is.
        let mut fallback: Option<Fallback> = None;
        for &b in key {
            if self.prefix.get(node) {
                fallback = Some(Fallback::Node(node));
            }
            let (start, end) = self.node_range(node);
            let labels = self.labels.get(start..end).unwrap_or(&[]);
            match labels.binary_search(&b) {
                Ok(i) => {
                    if let Some(smaller) = i.checked_sub(1) {
                        fallback = Some(Fallback::Label(start.saturating_add(smaller)));
                    }
                    let p = start.saturating_add(i);
                    if !self.has_child.get(p) {
                        // A key ending here: `key` itself, or a prefix of it.
                        return self.value_at_label(p);
                    }
                    node = self.child(p);
                }
                Err(i) => {
                    if let Some(smaller) = i.checked_sub(1) {
                        fallback = Some(Fallback::Label(start.saturating_add(smaller)));
                    }
                    return self.rightmost(fallback?);
                }
            }
        }
        // `key` ends at `node`: its own key, if any, is `key`; every other beneath is greater.
        if self.prefix.get(node) {
            return self.value_of_node(node);
        }
        self.rightmost(fallback?)
    }

    /// The value of the greatest key at or under `from`.
    fn rightmost(&self, from: Fallback) -> Option<u32> {
        let mut p = match from {
            Fallback::Node(n) => return self.value_of_node(n),
            Fallback::Label(p) => p,
        };
        // Down the last branch of each node: at most the labels.
        for _ in 0..=self.labels.len() {
            if !self.has_child.get(p) {
                return self.value_at_label(p);
            }
            let (start, end) = self.node_range(self.child(p));
            if end <= start {
                return None;
            }
            p = end.saturating_sub(1);
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
        self.labels
            .len()
            .saturating_add(self.has_child.bytes())
            .saturating_add(self.louds.bytes())
            .saturating_add(self.prefix.bytes())
            .saturating_add(self.values.len().saturating_mul(4))
    }
}

/// Where a floor search falls back to.
#[derive(Clone, Copy, Debug)]
enum Fallback {
    Label(usize),
    Node(usize),
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
            let t = Trie::build(&keys, &values).unwrap();
            assert_eq!(t.len(), map.len());
            for (k, v) in &map {
                assert_eq!(t.get(k), Some(*v), "case {case} get {k:?}");
            }
            for _ in 0..300 {
                let q = key(&mut x);
                assert_eq!(t.get(&q), map.get(&q).copied(), "case {case} get {q:?}");
                let want = map.range(..=q.clone()).next_back().map(|(_, v)| *v);
                assert_eq!(t.floor(&q), want, "case {case} floor {q:?} keys {keys:?}");
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
