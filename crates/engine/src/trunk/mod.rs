//! The trunk (docs/design/engine-structure.md §4, step E4): SplinterDB's size-tiered Bε-tree over
//! branches (research/34 §1), one shard's, so one worker runs it and no lock is taken.
//!
//! - A **node** has pivots, each a lower bound of the keys under it, with a child (none at a
//!   leaf) and a **pivot bundle** of branches, newest first; and **in-flight bundles** the node
//!   received, shared by its pivots, each pivot reading those from its own start on.
//! - A packed memtable waits as **pending**, read before the tree; the next **cascade** takes
//!   every pending branch into the root, each an in-flight bundle (**incorporation**), and runs
//!   the root's flush-then-compact and every child's it sets off.
//! - A cascade runs a slice at a time ([`Trunk::step`]): an explicit stack of the nodes in
//!   progress, a compaction merging a budget of keys a step and changing its node only once it
//!   is done. Nothing else changes a node while a cascade runs (new branches wait as pending),
//!   so every read between steps sees the trunk whole.
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

use crate::branch::merge::Compaction;
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

/// What the trunk's maintenance has done since it was made or loaded.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TrunkStats {
    /// Pivots whose in-flight bundles were compacted into one branch.
    pub pivot_compactions: u64,
    /// Leaves compacted whole.
    pub leaf_compactions: u64,
    /// Pivot bundles flushed to a child.
    pub flushes: u64,
    /// Nodes made by splits, leaves and index nodes.
    pub splits: u64,
    /// Entries written into branches by compactions.
    pub entries_written: u64,
    /// Nanoseconds planning compactions (their cursors' seeks), and finishing them (the last
    /// pages and filters, and the trunk's change).
    pub plan_ns: u64,
    pub finish_ns: u64,
}

fn ns_since(t: std::time::Instant) -> u64 {
    u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// One shard's trunk.
#[derive(Debug)]
pub struct Trunk {
    nodes: Vec<Node>,
    root: usize,
    config: TrunkConfig,
    /// The extents of the last saved image, released when the next one is saved.
    saved: Vec<u64>,
    stats: TrunkStats,
    /// Packed memtables not yet in the tree, oldest first: read before it, and taken into the
    /// root together when the next cascade starts.
    pending: Vec<Branch>,
    /// The cascade in progress, a frame a node from the root down ([`Trunk::step`]).
    cascade: Vec<Frame>,
    /// A finished frame's nodes now covering its range, for the frame above it.
    returned: Option<Vec<(Vec<u8>, usize)>>,
}

/// One node's flush-then-compact in progress: the recursion the module describes, held as an
/// explicit stack so it runs a slice at a time between the shard's other work.
#[derive(Debug)]
struct Frame {
    n: usize,
    phase: Phase,
}

#[derive(Debug)]
enum Phase {
    /// Compacting pivot `i`'s live in-flight bundles: the job and the in-flight count it covers.
    Pivots {
        i: usize,
        job: Option<(Compaction, usize)>,
    },
    /// The leaf's whole compaction, and the branches it replaces.
    Settle { job: Option<(Compaction, Vec<u64>)> },
    /// Flushing the pivots from `i` on; `waiting` while the child's frame runs.
    Flush { i: usize, waiting: bool },
}

/// The saved image's format version.
const IMAGE_FORMAT: u8 = 1;
/// The saved image header's magic: "mantleTK" in ASCII, little-endian.
const IMAGE_MAGIC: u64 = u64::from_le_bytes(*b"mantleTK");
/// None, in a child or an end key's place.
const ABSENT: u32 = u32::MAX;

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
            saved: Vec::new(),
            stats: TrunkStats::default(),
            pending: Vec::new(),
            cascade: Vec::new(),
            returned: None,
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
        for b in self.pending.iter().rev() {
            if let Some(op) = b.get_hashed(store, key, hash, value)? {
                return Ok(Some(op));
            }
        }
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

    /// Takes a packed memtable into the trunk and runs every cascade it sets off to the end.
    /// The branch's references are the trunk's from here.
    pub fn incorporate<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        branch: Branch,
    ) -> Result<(), Error> {
        self.add(branch);
        self.drain(store)
    }

    /// Takes a packed memtable as pending: read before the tree, taken into the root when the
    /// next cascade starts ([`Self::step`]). Its references are the trunk's from here.
    pub fn add(&mut self, branch: Branch) {
        self.pending.push(branch);
    }

    /// Packed memtables pending.
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// Whether no cascade runs and none is pending.
    pub fn is_idle(&self) -> bool {
        self.cascade.is_empty() && self.pending.is_empty()
    }

    /// The maintenance owed, in input entries, at most: the pending branches' and what the
    /// running compactions have left. A cascade's later compactions are not yet known; each is
    /// owed once it is planned.
    pub fn debt(&self) -> u64 {
        let pending = self
            .pending
            .iter()
            .map(|b| b.count)
            .fold(0, u64::saturating_add);
        self.cascade
            .iter()
            .map(|f| match &f.phase {
                Phase::Pivots {
                    job: Some((c, _)), ..
                }
                | Phase::Settle { job: Some((c, _)) } => c.remaining(),
                _ => 0,
            })
            .fold(pending, u64::saturating_add)
    }

    /// Runs every cascade to the end.
    pub fn drain<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<(), Error> {
        while !self.is_idle() {
            self.step(store, u64::MAX)?;
        }
        Ok(())
    }

    /// Runs maintenance for up to `budget` merged keys: the cascade in progress, or a new one
    /// over every pending branch. A step between merges (planning, a flush by reference, a
    /// split) costs no budget, and each is taken once, so a step ends. Returns the keys merged.
    pub fn step<F: BlockFile>(&mut self, store: &mut Store<F>, budget: u64) -> Result<u64, Error> {
        self.run(store, budget, true)
    }

    /// Runs the cascade in progress to its end, starting none: the pending branches then enter
    /// the root at the next step.
    pub fn finish_cascade<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<(), Error> {
        self.run(store, u64::MAX, false).map(|_| ())
    }

    /// The trunk's fanout.
    pub fn fanout(&self) -> usize {
        self.config.fanout
    }

    fn run<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        budget: u64,
        start: bool,
    ) -> Result<u64, Error> {
        let mut used = 0u64;
        // A finished frame's result is taken by the frame above it in the same step, at no
        // cost to the budget: a settled or split node's new siblings are reachable only once
        // its parent splices them in, and a step ending between the two left keys a read could
        // not reach (found at fill pace by `tests/shard_db_paced.rs`).
        while used < budget || self.returned.is_some() {
            let Some(frame) = self.cascade.pop() else {
                if !start || self.pending.is_empty() {
                    break;
                }
                // A new cascade: the pending branches enter the root, oldest first.
                let root = self.root;
                for b in std::mem::take(&mut self.pending) {
                    self.node_mut(root)?.inflight.push(vec![b]);
                }
                self.cascade.push(Frame {
                    n: root,
                    phase: Phase::Pivots { i: 0, job: None },
                });
                continue;
            };
            used = used.saturating_add(self.advance(store, frame, budget.saturating_sub(used))?);
            if self.cascade.is_empty()
                && let Some(parts) = self.returned.take()
            {
                self.grow_root(parts);
            }
        }
        Ok(used)
    }

    /// The root's frame returned `parts`: more than one is a split, and a new root over them.
    fn grow_root(&mut self, parts: Vec<(Vec<u8>, usize)>) {
        if parts.len() <= 1 {
            return;
        }
        let pivots = parts
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

    /// Advances `frame` by one move, merging up to `budget` keys; pushes it back unless it
    /// finished, in which case its result is in `returned`. Returns the keys merged.
    fn advance<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        frame: Frame,
        budget: u64,
    ) -> Result<u64, Error> {
        let n = frame.n;
        let (phase, used) = match frame.phase {
            Phase::Pivots {
                i,
                job: Some((mut c, covered)),
            } => {
                let used = c.step(store, budget)?;
                if c.is_done() {
                    let t = std::time::Instant::now();
                    self.apply_pivot(store, n, i, c, covered)?;
                    self.stats.finish_ns = self.stats.finish_ns.saturating_add(ns_since(t));
                    (
                        Some(Phase::Pivots {
                            i: i.saturating_add(1),
                            job: None,
                        }),
                        used,
                    )
                } else {
                    (
                        Some(Phase::Pivots {
                            i,
                            job: Some((c, covered)),
                        }),
                        used,
                    )
                }
            }
            Phase::Pivots { i, job: None } => {
                let t = std::time::Instant::now();
                let phase = self.plan_pivot(store, n, i)?;
                self.stats.plan_ns = self.stats.plan_ns.saturating_add(ns_since(t));
                (Some(phase), 0)
            }
            Phase::Settle {
                job: Some((mut c, extents)),
            } => {
                let used = c.step(store, budget)?;
                if c.is_done() {
                    let t = std::time::Instant::now();
                    let parts = c.finish(store)?;
                    self.returned = Some(self.apply_settle(store, n, parts, &extents)?);
                    self.stats.finish_ns = self.stats.finish_ns.saturating_add(ns_since(t));
                    (None, used)
                } else {
                    (
                        Some(Phase::Settle {
                            job: Some((c, extents)),
                        }),
                        used,
                    )
                }
            }
            Phase::Settle { job: None } => {
                let t = std::time::Instant::now();
                let phase = self.plan_settle(store, n)?;
                self.stats.plan_ns = self.stats.plan_ns.saturating_add(ns_since(t));
                (phase, 0)
            }
            Phase::Flush { i, waiting } => {
                let i = if waiting { self.take_child(n, i)? } else { i };
                (self.flush_from(n, i)?, 0)
            }
        };
        if let Some(phase) = phase {
            self.cascade.push(Frame { n, phase });
        }
        Ok(used)
    }

    /// Writes the trunk's image for a checkpoint and returns its header page, which the
    /// superblock names as the root: the nodes, each pivot's key, child and branches, serialized
    /// into pages of fresh extents. The previous image's extents are released now and freed once
    /// the checkpoint naming this one is durable (the store's deferred free). Every node's
    /// in-flight list is empty between incorporations, which the image relies on.
    pub fn save<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<u64, Error> {
        if !self.is_idle() {
            return Err(Error::InvalidArgument {
                what: "a trunk saved with maintenance pending",
            });
        }
        let mut blob = Vec::new();
        blob.push(IMAGE_FORMAT);
        let put32 = |v: &mut Vec<u8>, n: usize| -> Result<(), Error> {
            v.extend_from_slice(&u32::try_from(n).map_err(|_| corrupt())?.to_le_bytes());
            Ok(())
        };
        put32(&mut blob, self.root)?;
        put32(&mut blob, self.config.fanout)?;
        blob.extend_from_slice(&self.config.leaf_entries.to_le_bytes());
        put32(&mut blob, self.nodes.len())?;
        for node in &self.nodes {
            if !node.inflight.is_empty() {
                return Err(corrupt());
            }
            blob.push(u8::from(node.leaf));
            match &node.end {
                Some(end) => {
                    put32(&mut blob, end.len())?;
                    blob.extend_from_slice(end);
                }
                None => blob.extend_from_slice(&ABSENT.to_le_bytes()),
            }
            put32(&mut blob, node.pivots.len())?;
            for p in &node.pivots {
                put32(&mut blob, p.key.len())?;
                blob.extend_from_slice(&p.key);
                match p.child {
                    Some(c) => put32(&mut blob, c)?,
                    None => blob.extend_from_slice(&ABSENT.to_le_bytes()),
                }
                put32(&mut blob, p.bundle.len())?;
                for b in &p.bundle {
                    b.encode(&mut blob)?;
                }
            }
        }
        // The header page, then the blob's pages, in fresh extents.
        let capacity = store.page_capacity();
        let per = usize::try_from(store.extent_pages()).map_err(|_| corrupt())?;
        let pages = blob.len().div_ceil(capacity).saturating_add(1);
        let extents = pages.div_ceil(per);
        let header_room = capacity.saturating_sub(20) / 8;
        if extents > header_room {
            return Err(Error::LimitExceeded {
                what: "extents a trunk image's header names",
                limit: u64::try_from(header_room).unwrap_or(u64::MAX),
            });
        }
        let mut fresh = Vec::with_capacity(extents);
        for _ in 0..extents {
            fresh.push(store.allocate_extent()?);
        }
        let address_of = |store: &Store<F>, i: usize| -> Result<u64, Error> {
            let extent = *fresh
                .get(i.checked_div(per).ok_or(corrupt())?)
                .ok_or(corrupt())?;
            store.address(
                extent,
                u32::try_from(i.checked_rem(per).ok_or(corrupt())?).map_err(|_| corrupt())?,
            )
        };
        let mut header = Vec::with_capacity(extents.saturating_mul(8).saturating_add(20));
        header.extend_from_slice(&IMAGE_MAGIC.to_le_bytes());
        header.extend_from_slice(
            &u64::try_from(blob.len())
                .map_err(|_| corrupt())?
                .to_le_bytes(),
        );
        put32(&mut header, extents)?;
        for e in &fresh {
            header.extend_from_slice(&e.to_le_bytes());
        }
        let head = address_of(store, 0)?;
        let mut run = store.run()?;
        store.queue_page(&mut run, head, &header)?;
        for (i, chunk) in blob.chunks(capacity).enumerate() {
            let address = address_of(store, i.saturating_add(1))?;
            store.queue_page(&mut run, address, chunk)?;
        }
        store.write_run(&mut run)?;
        for e in std::mem::replace(&mut self.saved, fresh) {
            store.release(e)?;
        }
        Ok(head)
    }

    /// The trunk a checkpoint's image at `head` holds, its branches' filters read back.
    pub fn load<F: BlockFile>(store: &mut Store<F>, head: u64) -> Result<Self, Error> {
        let mut header = Vec::new();
        store.read_page(head, &mut header)?;
        let u64_at = |b: &[u8], at: usize| {
            b.get(at..)
                .and_then(<[u8]>::first_chunk::<8>)
                .map(|x| u64::from_le_bytes(*x))
                .ok_or(corrupt())
        };
        let u32_at = |b: &[u8], at: usize| {
            b.get(at..)
                .and_then(<[u8]>::first_chunk::<4>)
                .map(|x| u32::from_le_bytes(*x))
                .ok_or(corrupt())
        };
        if u64_at(&header, 0)? != IMAGE_MAGIC {
            return Err(Error::Corruption {
                what: "a trunk image",
                why: Malformed::BadMagic,
            });
        }
        let len = usize::try_from(u64_at(&header, 8)?).map_err(|_| corrupt())?;
        let extents = usize::try_from(u32_at(&header, 16)?).map_err(|_| corrupt())?;
        let mut saved = Vec::with_capacity(extents.min(header.len() / 8));
        for i in 0..extents {
            saved.push(u64_at(
                &header,
                20usize.saturating_add(i.saturating_mul(8)),
            )?);
        }
        let per = usize::try_from(store.extent_pages()).map_err(|_| corrupt())?;
        let capacity = store.page_capacity();
        let mut blob = Vec::with_capacity(len);
        let pages = len.div_ceil(capacity);
        for i in 1..=pages {
            let extent = *saved
                .get(i.checked_div(per).ok_or(corrupt())?)
                .ok_or(corrupt())?;
            let address = store.address(
                extent,
                u32::try_from(i.checked_rem(per).ok_or(corrupt())?).map_err(|_| corrupt())?,
            )?;
            store.read_page(address, &mut blob)?;
        }
        if blob.len() != len {
            return Err(corrupt());
        }
        let mut r = Reader {
            bytes: &blob,
            at: 0,
        };
        let format = *r.take(1)?.first().ok_or(corrupt())?;
        if format != IMAGE_FORMAT {
            return Err(Error::Corruption {
                what: "a trunk image",
                why: Malformed::UnknownVersion(u64::from(format)),
            });
        }
        let u32_of = |s: &[u8]| {
            s.first_chunk::<4>()
                .map(|b| u32::from_le_bytes(*b))
                .ok_or(corrupt())
        };
        let usize_of = |s: &[u8]| -> Result<usize, Error> {
            usize::try_from(u32_of(s)?).map_err(|_| corrupt())
        };
        let root = usize_of(r.take(4)?)?;
        let fanout = usize_of(r.take(4)?)?;
        let leaf_entries = r
            .take(8)?
            .first_chunk::<8>()
            .map(|b| u64::from_le_bytes(*b))
            .ok_or(corrupt())?;
        let count = usize_of(r.take(4)?)?;
        let mut nodes = Vec::with_capacity(count.min(len));
        for _ in 0..count {
            let leaf = *r.take(1)?.first().ok_or(corrupt())? == 1;
            let end_len = u32_of(r.take(4)?)?;
            let end = if end_len == ABSENT {
                None
            } else {
                Some(
                    r.take(usize::try_from(end_len).map_err(|_| corrupt())?)?
                        .to_vec(),
                )
            };
            let pivot_count = usize_of(r.take(4)?)?;
            let mut pivots = Vec::with_capacity(pivot_count.min(len));
            for _ in 0..pivot_count {
                let key_len = usize_of(r.take(4)?)?;
                let key = r.take(key_len)?.to_vec();
                let child = u32_of(r.take(4)?)?;
                let child = if child == ABSENT {
                    None
                } else {
                    Some(usize::try_from(child).map_err(|_| corrupt())?)
                };
                let bundle_len = usize_of(r.take(4)?)?;
                let mut bundle = Vec::with_capacity(bundle_len.min(len));
                for _ in 0..bundle_len {
                    let rest = r.bytes.get(r.at..).ok_or(corrupt())?;
                    let (branch, used) = Branch::decode(store, rest)?;
                    r.at = r.at.checked_add(used).ok_or(corrupt())?;
                    bundle.push(branch);
                }
                pivots.push(Pivot {
                    key,
                    child,
                    bundle,
                    start: 0,
                });
            }
            nodes.push(Node {
                leaf,
                pivots,
                inflight: Vec::new(),
                end,
            });
        }
        if root >= nodes.len() {
            return Err(corrupt());
        }
        Ok(Self {
            nodes,
            root,
            config: TrunkConfig {
                fanout,
                leaf_entries,
            },
            saved,
            stats: TrunkStats::default(),
            pending: Vec::new(),
            cascade: Vec::new(),
            returned: None,
        })
    }

    /// The next pivot from `i` on with live in-flight bundles gets a compaction planned; with
    /// none left, the in-flight bundles no pivot reads go, and the node settles (a leaf) or
    /// flushes.
    fn plan_pivot<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        n: usize,
        i: usize,
    ) -> Result<Phase, Error> {
        let node = self.node(n)?;
        let next = (i..node.pivots.len()).find(|&j| {
            node.pivots
                .get(j)
                .is_some_and(|p| node.inflight.len() > p.start)
        });
        let Some(j) = next else {
            self.drop_dead(store, n)?;
            return Ok(if self.node(n)?.leaf {
                Phase::Settle { job: None }
            } else {
                Phase::Flush {
                    i: 0,
                    waiting: false,
                }
            });
        };
        let pivot = node.pivots.get(j).ok_or(corrupt())?;
        let live = node.inflight.get(pivot.start..).unwrap_or(&[]);
        let (from, end) = (pivot.key.clone(), Self::pivot_end(node, j));
        let covered = node.inflight.len();
        // Newest first, the order a merge takes.
        let branches = live.iter().rev().flatten();
        let job = Compaction::new(store, branches, &from, end.as_deref(), false, u64::MAX)?;
        Ok(Phase::Pivots {
            i: j,
            job: Some((job, covered)),
        })
    }

    /// Pivot `i`'s compaction is done: its branch goes to the front of the pivot's bundle, and
    /// the pivot reads in-flight bundles from `covered` on.
    fn apply_pivot<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        n: usize,
        i: usize,
        job: Compaction,
        covered: usize,
    ) -> Result<(), Error> {
        let merged = job.finish(store)?.into_iter().next().map(|(_, b)| b);
        self.stats.pivot_compactions = self.stats.pivot_compactions.saturating_add(1);
        if let Some(b) = &merged {
            self.stats.entries_written = self.stats.entries_written.saturating_add(b.count);
        }
        let pivot = self.node_mut(n)?.pivots.get_mut(i).ok_or(corrupt())?;
        pivot.start = covered;
        if let Some(b) = merged {
            pivot.bundle.insert(0, b);
        }
        Ok(())
    }

    /// In-flight bundles before every pivot's start are read by none: their references go.
    fn drop_dead<F: BlockFile>(&mut self, store: &mut Store<F>, n: usize) -> Result<(), Error> {
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

    /// A leaf over `fanout` branches or `leaf_entries` entries gets its whole compaction
    /// planned, tombstones dropped, into branches of at most half `leaf_entries`; any other
    /// leaf's frame finishes as it is.
    fn plan_settle<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        n: usize,
    ) -> Result<Option<Phase>, Error> {
        let node = self.node(n)?;
        let pivot = node.pivots.first().ok_or(corrupt())?;
        let entries: u64 = pivot.bundle.iter().map(|b| b.count).sum();
        if pivot.bundle.len() <= self.config.fanout && entries <= self.config.leaf_entries {
            self.returned = Some(vec![(pivot.key.clone(), n)]);
            return Ok(None);
        }
        let (from, end) = (pivot.key.clone(), node.end.clone());
        // The extents the compaction's inputs hold, released once it replaces them.
        let extents: Vec<u64> = pivot
            .bundle
            .iter()
            .flat_map(|b| b.extents.iter().copied())
            .collect();
        let job = Compaction::new(
            store,
            &pivot.bundle,
            &from,
            end.as_deref(),
            true,
            self.config.leaf_entries.div_ceil(2).max(1),
        )?;
        Ok(Some(Phase::Settle {
            job: Some((job, extents)),
        }))
    }

    /// A leaf's compaction is done: `parts` replace the branches whose `extents` it releases,
    /// each part beyond the first a leaf of its own. Returns the leaves now covering the range, each with its first key.
    fn apply_settle<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        n: usize,
        parts: Vec<(Vec<u8>, Branch)>,
        extents: &[u64],
    ) -> Result<Vec<(Vec<u8>, usize)>, Error> {
        let node = self.node(n)?;
        let pivot = node.pivots.first().ok_or(corrupt())?;
        let (from, end) = (pivot.key.clone(), node.end.clone());
        self.stats.leaf_compactions = self.stats.leaf_compactions.saturating_add(1);
        self.stats.splits = self
            .stats
            .splits
            .saturating_add(u64::try_from(parts.len().saturating_sub(1)).unwrap_or(0));
        for (_, b) in &parts {
            self.stats.entries_written = self.stats.entries_written.saturating_add(b.count);
        }
        for &e in extents {
            store.release(e)?;
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

    /// Flushes the first pivot from `i` on whose bundle passed the fanout: its branches go to
    /// the child's in-flight list by reference and the child's frame runs next. With none left,
    /// the node splits if it must and its frame finishes.
    fn flush_from(&mut self, n: usize, i: usize) -> Result<Option<Phase>, Error> {
        let fanout = self.config.fanout;
        let node = self.node(n)?;
        let next = (i..node.pivots.len())
            .find(|&j| node.pivots.get(j).is_some_and(|p| p.bundle.len() > fanout));
        let Some(j) = next else {
            self.returned = Some(self.split_node(n)?);
            return Ok(None);
        };
        let pivot = self.node_mut(n)?.pivots.get_mut(j).ok_or(corrupt())?;
        let child = pivot.child.ok_or(corrupt())?;
        let bundle = std::mem::take(&mut pivot.bundle);
        self.stats.flushes = self.stats.flushes.saturating_add(1);
        // Oldest first into the child's in-flight list: the bundle is newest first.
        for b in bundle.into_iter().rev() {
            self.node_mut(child)?.inflight.push(vec![b]);
        }
        self.cascade.push(Frame {
            n,
            phase: Phase::Flush {
                i: j,
                waiting: true,
            },
        });
        self.cascade.push(Frame {
            n: child,
            phase: Phase::Pivots { i: 0, job: None },
        });
        Ok(None)
    }

    /// The child of pivot `i` finished: the nodes now covering its range replace the pivot.
    /// Returns the pivot to flush from next.
    fn take_child(&mut self, n: usize, i: usize) -> Result<usize, Error> {
        let parts = self.returned.take().ok_or(corrupt())?;
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
        Ok(i.saturating_add(added))
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
        self.stats.splits = self
            .stats
            .splits
            .saturating_add(u64::try_from(out.len().saturating_sub(1)).unwrap_or(0));
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

    /// What the trunk's maintenance has done.
    pub fn stats(&self) -> TrunkStats {
        self.stats
    }

    /// The extents of the trunk's saved image.
    pub fn image_extents(&self) -> &[u64] {
        &self.saved
    }

    /// The trunk's branches, for checks: every branch a node names, each naming once.
    pub fn branches(&self) -> Vec<Branch> {
        let mut out = self.pending.clone();
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

/// A position in an image's bytes.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    /// The next `n` bytes.
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let end = self.at.checked_add(n).ok_or(corrupt())?;
        let s = self.bytes.get(self.at..end).ok_or(corrupt())?;
        self.at = end;
        Ok(s)
    }
}
