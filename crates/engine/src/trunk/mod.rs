//! The trunk (docs/design/engine-structure.md §4, step E4): SplinterDB's size-tiered Bε-tree over
//! branches (research/34 §1), one shard's, so one worker runs it and no lock is taken.
//!
//! - A **node** has pivots, each a lower bound of the keys under it, with a child (none at a
//!   leaf) and a **pivot bundle** of branches, newest first; and **in-flight bundles** the node
//!   received, shared by its pivots, each pivot reading those from its own start on.
//! - A packed memtable enters the root as an in-flight bundle (**incorporation**).
//! - **Compaction**: a pivot with in-flight bundles merges them, clipped to its range, into one
//!   branch at the front of its pivot bundle; each entry is rewritten once a level.
//! - **Flush**: a pivot bundle of more than `fanout` branches moves to the child as in-flight
//!   bundles by reference, no entry rewritten (flush-then-compact). A node over `fanout` pivots
//!   splits.
//! - A **leaf** whose bundle passes `fanout` branches is compacted whole, tombstones dropped (no
//!   older entry lies below it), and split by entries once past `leaf_entries`.
//! - A **point read** goes root to leaf: each node's pivot for the key, its live in-flight
//!   bundles newest first, then its pivot bundle. Every branch is probed until maplets (step E5)
//!   name the branches a key may be in.
//!
//! References: a branch is named by one node at a time (a flush moves a bundle, it never copies
//! one) and holds the one reference its extents were allocated with; the node that drops it
//! releases them, so an extent is freed exactly when no node names it.

use crate::branch::merge::compact;
use crate::branch::{Branch, Op};
use crate::error::{Error, Malformed};
use crate::store::Store;
use hyper_block::block::BlockFile;

/// The trunk's shape: from measurement, given at creation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrunkConfig {
    /// Pivots a node holds, and branches a pivot bundle holds, before a split or a flush.
    pub fanout: usize,
    /// Entries a leaf holds before it splits.
    pub leaf_entries: u64,
}

#[derive(Clone, Debug)]
struct Pivot {
    key: Vec<u8>,
    child: Option<usize>,
    /// Newest first.
    bundle: Vec<Branch>,
    /// The first of the node's in-flight bundles live for this pivot.
    start: usize,
}

#[derive(Clone, Debug)]
struct Node {
    leaf: bool,
    pivots: Vec<Pivot>,
    /// Oldest first.
    inflight: Vec<Vec<Branch>>,
    /// The keys below this, if any bound them: the next node's first pivot.
    end: Option<Vec<u8>>,
}

/// One shard's trunk.
#[derive(Debug)]
pub struct Trunk {
    nodes: Vec<Node>,
    root: usize,
    config: TrunkConfig,
}

fn corrupt() -> Error {
    Error::Corruption {
        what: "a trunk",
        why: Malformed::OutOfRange,
    }
}

fn release<F: BlockFile>(store: &mut Store<F>, b: &Branch) -> Result<(), Error> {
    for &e in &b.extents {
        store.release(e)?;
    }
    Ok(())
}

impl Trunk {
    /// An empty trunk: one leaf over every key.
    pub fn new(config: TrunkConfig) -> Result<Self, Error> {
        if config.fanout < 2 || config.leaf_entries < 2 {
            return Err(Error::InvalidArgument {
                what: "a trunk of fanout below 2 or leaves below 2 entries",
            });
        }
        Ok(Self {
            nodes: vec![Node {
                leaf: true,
                pivots: vec![Pivot {
                    key: Vec::new(),
                    child: None,
                    bundle: Vec::new(),
                    start: 0,
                }],
                inflight: Vec::new(),
                end: None,
            }],
            root: 0,
            config,
        })
    }

    fn node(&self, n: usize) -> Result<&Node, Error> {
        self.nodes.get(n).ok_or(corrupt())
    }

    fn node_mut(&mut self, n: usize) -> Result<&mut Node, Error> {
        self.nodes.get_mut(n).ok_or(corrupt())
    }

    /// The pivot of `node` whose range holds `key`.
    fn pivot_of(node: &Node, key: &[u8]) -> usize {
        node.pivots
            .partition_point(|p| p.key.as_slice() <= key)
            .saturating_sub(1)
    }

    /// The end of pivot `i`'s range: the next pivot's key, or the node's end.
    fn pivot_end(node: &Node, i: usize) -> Option<Vec<u8>> {
        node.pivots
            .get(i.saturating_add(1))
            .map(|p| p.key.clone())
            .or_else(|| node.end.clone())
    }

    /// The newest entry for `key` in the trunk: its operation and value into `value`.
    pub fn get<F: BlockFile>(
        &self,
        store: &mut Store<F>,
        key: &[u8],
        value: &mut Vec<u8>,
    ) -> Result<Option<Op>, Error> {
        let hash = crate::branch::filter::hash(key);
        let mut at = self.root;
        // Every level down is a node's child: at most the trunk's node count of steps.
        for _ in 0..self.nodes.len() {
            let node = self.node(at)?;
            let p = Self::pivot_of(node, key);
            let pivot = node.pivots.get(p).ok_or(corrupt())?;
            for bundle in node.inflight.get(pivot.start..).unwrap_or(&[]).iter().rev() {
                for b in bundle {
                    if let Some(op) = b.get_hashed(store, key, hash, value)? {
                        return Ok(Some(op));
                    }
                }
            }
            for b in &pivot.bundle {
                if let Some(op) = b.get_hashed(store, key, hash, value)? {
                    return Ok(Some(op));
                }
            }
            match pivot.child {
                Some(child) => at = child,
                None => return Ok(None),
            }
        }
        Err(corrupt())
    }

    /// Takes a packed memtable into the root as an in-flight bundle, then flushes and compacts
    /// down the tree as the module describes. The branch's references are the trunk's from here.
    pub fn incorporate<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        branch: Branch,
    ) -> Result<(), Error> {
        let root = self.root;
        self.node_mut(root)?.inflight.push(vec![branch]);
        let split = self.flush_then_compact(store, root)?;
        if split.len() > 1 {
            // The root split: a new root over the parts.
            let pivots = split
                .into_iter()
                .map(|(key, child)| Pivot {
                    key,
                    child: Some(child),
                    bundle: Vec::new(),
                    start: 0,
                })
                .collect();
            self.nodes.push(Node {
                leaf: false,
                pivots,
                inflight: Vec::new(),
                end: None,
            });
            self.root = self.nodes.len().saturating_sub(1);
        }
        Ok(())
    }

    /// Compacts, flushes and splits node `n`; returns the nodes now covering its range, each with
    /// its first key (one unless it split).
    fn flush_then_compact<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        n: usize,
    ) -> Result<Vec<(Vec<u8>, usize)>, Error> {
        self.compact_pivots(store, n)?;
        let node = self.node(n)?;
        if node.leaf {
            return self.settle_leaf(store, n);
        }
        // Flush each pivot whose bundle passed the fanout, then take its child's split.
        let mut i = 0usize;
        while i < self.node(n)?.pivots.len() {
            let pivot = self.node(n)?.pivots.get(i).ok_or(corrupt())?;
            if pivot.bundle.len() <= self.config.fanout {
                i = i.saturating_add(1);
                continue;
            }
            let child = pivot.child.ok_or(corrupt())?;
            let bundle =
                std::mem::take(&mut self.node_mut(n)?.pivots.get_mut(i).ok_or(corrupt())?.bundle);
            // Oldest first into the child's in-flight list: the bundle is newest first.
            for b in bundle.into_iter().rev() {
                self.node_mut(child)?.inflight.push(vec![b]);
            }
            let parts = self.flush_then_compact(store, child)?;
            let node = self.node_mut(n)?;
            let first = node.pivots.get(i).ok_or(corrupt())?.key.clone();
            let mut replacement: Vec<Pivot> = parts
                .into_iter()
                .map(|(key, child)| Pivot {
                    key,
                    child: Some(child),
                    bundle: Vec::new(),
                    start: node.inflight.len(),
                })
                .collect();
            if let Some(p) = replacement.first_mut() {
                p.key = first;
            }
            let added = replacement.len();
            node.pivots.splice(i..=i, replacement);
            i = i.saturating_add(added);
        }
        self.split_node(n)
    }

    /// Folds each pivot's live in-flight bundles, clipped to its range, into one branch at the
    /// front of its bundle, and drops the in-flight bundles no pivot reads any more.
    fn compact_pivots<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        n: usize,
    ) -> Result<(), Error> {
        let pivots = self.node(n)?.pivots.len();
        for i in 0..pivots {
            let node = self.node(n)?;
            let pivot = node.pivots.get(i).ok_or(corrupt())?;
            let live = node.inflight.get(pivot.start..).unwrap_or(&[]);
            if live.is_empty() {
                continue;
            }
            // Newest first, the order a merge takes.
            let branches: Vec<Branch> = live.iter().rev().flatten().cloned().collect();
            let (from, end) = (pivot.key.clone(), Self::pivot_end(node, i));
            let inflight = node.inflight.len();
            let merged = compact(store, &branches, &from, end.as_deref(), false)?;
            let pivot = self.node_mut(n)?.pivots.get_mut(i).ok_or(corrupt())?;
            pivot.start = inflight;
            if let Some(b) = merged {
                pivot.bundle.insert(0, b);
            }
        }
        // In-flight bundles before every pivot's start are read by none: their references go.
        let node = self.node_mut(n)?;
        let dead = node.pivots.iter().map(|p| p.start).min().unwrap_or(0);
        let gone: Vec<Vec<Branch>> = node.inflight.drain(..dead).collect();
        for p in &mut node.pivots {
            p.start = p.start.saturating_sub(dead);
        }
        for bundle in gone {
            for b in &bundle {
                release(store, b)?;
            }
        }
        Ok(())
    }

    /// A leaf over `fanout` branches is compacted whole, tombstones dropped, into branches of at
    /// most `leaf_entries` entries; each beyond the first becomes a leaf of its own.
    fn settle_leaf<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        n: usize,
    ) -> Result<Vec<(Vec<u8>, usize)>, Error> {
        let node = self.node(n)?;
        let pivot = node.pivots.first().ok_or(corrupt())?;
        let entries: u64 = pivot.bundle.iter().map(|b| b.count).sum();
        if pivot.bundle.len() <= self.config.fanout && entries <= self.config.leaf_entries {
            return Ok(vec![(pivot.key.clone(), n)]);
        }
        let (from, end) = (pivot.key.clone(), node.end.clone());
        let branches = pivot.bundle.clone();
        let parts = crate::branch::merge::compact_split(
            store,
            &branches,
            &from,
            end.as_deref(),
            true,
            self.config.leaf_entries.div_ceil(2).max(1),
        )?;
        for b in &branches {
            release(store, b)?;
        }
        // Every compacted part's references are the leaf's that takes it.
        if parts.is_empty() {
            self.node_mut(n)?
                .pivots
                .first_mut()
                .ok_or(corrupt())?
                .bundle
                .clear();
            return Ok(vec![(from, n)]);
        }
        let mut out = Vec::with_capacity(parts.len());
        let count = parts.len();
        for (j, (first, branch)) in parts.into_iter().enumerate() {
            let key = if j == 0 { from.clone() } else { first };
            if j == 0 {
                let node = self.node_mut(n)?;
                let p = node.pivots.first_mut().ok_or(corrupt())?;
                p.bundle = vec![branch];
                out.push((key, n));
            } else {
                self.nodes.push(Node {
                    leaf: true,
                    pivots: vec![Pivot {
                        key: key.clone(),
                        child: None,
                        bundle: vec![branch],
                        start: 0,
                    }],
                    inflight: Vec::new(),
                    end: None,
                });
                out.push((key, self.nodes.len().saturating_sub(1)));
            }
        }
        // Each part ends where the next begins; the last where the leaf did.
        for j in 0..count {
            let end_key = out
                .get(j.saturating_add(1))
                .map(|(k, _)| k.clone())
                .or_else(|| end.clone());
            let (_, idx) = out.get(j).ok_or(corrupt())?;
            let idx = *idx;
            self.node_mut(idx)?.end = end_key;
        }
        Ok(out)
    }

    /// An index node over `fanout` pivots splits into nodes of at most `fanout` pivots. Its
    /// in-flight bundles are empty here: [`Self::compact_pivots`] ran first and left every pivot
    /// past them, so a split only partitions the pivots.
    fn split_node(&mut self, n: usize) -> Result<Vec<(Vec<u8>, usize)>, Error> {
        let fanout = self.config.fanout;
        let node = self.node_mut(n)?;
        let first = node
            .pivots
            .first()
            .map(|p| p.key.clone())
            .unwrap_or_default();
        let len = node.pivots.len();
        if len <= fanout {
            return Ok(vec![(first, n)]);
        }
        if !node.inflight.is_empty() {
            return Err(corrupt());
        }
        let parts = len.div_ceil(fanout);
        let size = len.div_ceil(parts);
        let end = node.end.clone();
        let mut rest = node.pivots.split_off(size);
        let mut out = vec![(first, n)];
        while !rest.is_empty() {
            let tail = rest.split_off(size.min(rest.len()));
            let key = rest.first().map(|p| p.key.clone()).unwrap_or_default();
            self.nodes.push(Node {
                leaf: false,
                pivots: rest,
                inflight: Vec::new(),
                end: None,
            });
            out.push((key, self.nodes.len().saturating_sub(1)));
            rest = tail;
        }
        for j in 0..out.len() {
            let end_key = out
                .get(j.saturating_add(1))
                .map(|(k, _)| k.clone())
                .or_else(|| end.clone());
            let idx = out.get(j).ok_or(corrupt())?.1;
            self.node_mut(idx)?.end = end_key;
        }
        Ok(out)
    }

    /// The trunk's shape: its height (1 for a lone leaf), its nodes, and its leaves.
    pub fn shape(&self) -> Result<(usize, usize, usize), Error> {
        let mut height = 1usize;
        let mut at = self.root;
        while let Some(child) = self.node(at)?.pivots.first().and_then(|p| p.child) {
            height = height.saturating_add(1);
            at = child;
            if height > self.nodes.len() {
                return Err(corrupt());
            }
        }
        let leaves = self.nodes.iter().filter(|n| n.leaf).count();
        Ok((height, self.nodes.len(), leaves))
    }

    /// The trunk's branches, for checks: every branch a node names, each naming once.
    pub fn branches(&self) -> Vec<Branch> {
        let mut out = Vec::new();
        for node in &self.nodes {
            for bundle in &node.inflight {
                out.extend(bundle.iter().cloned());
            }
            for p in &node.pivots {
                out.extend(p.bundle.iter().cloned());
            }
        }
        out
    }
}
