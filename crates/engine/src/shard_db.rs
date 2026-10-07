//! One shard's engine (docs/design/engine-structure.md §2, §6, §8): the memtable, and the trunk
//! of branches in the shard's store. A put or delete goes to the memtable; a get reads the
//! memtable, the one being packed, then the trunk.
//!
//! Maintenance runs on the shard's own worker, a slice at a time, never all at once on the put
//! that fills a memtable (SILK, Balmau et al., USENIX ATC 2019: flushes first, compaction spread
//! so no put waits on it; SplinterDB, Conway et al., USENIX ATC 2020: incorporation and bundle
//! compactions as separate tasks). A full memtable becomes the packing one, readable, and a
//! cleared one takes the puts. Each put then does its share of two debts, each over the room it
//! must be paid within:
//! - the packing memtable's entries, before the new one fills (`room`, its arena bytes left);
//! - the trunk's maintenance, before `fanout` packed memtables wait on it (`room` and a memtable
//!   for each further one that may wait).
//!
//! A put of `b` bytes does `debt · b / room` of a debt: paid at that rate, it is paid when the
//! room runs out. A cascade's compactions are owed only once planned, so a debt can still be
//! owed when a memtable fills; that put pays it whole, a stall the engine counts.
//!
//! Durability is the Raft log's (§2): the engine applies committed entries and makes its state
//! durable by checkpoints. A checkpoint packs the memtable into the trunk, writes the trunk's
//! image, and names it with the last applied Raft index in the store's superblock; recovery
//! opens the newest checkpoint and the caller replays the log from the index after it.

use crate::branch::filter::Keys;
use crate::branch::{Builder, Op};
use crate::error::Error;
use crate::memtable::btree::{BTreeMem, Walk};
use crate::rows::Rows;
use crate::store::{Config, Store};
use crate::trunk::{Trunk, TrunkConfig};
use hyper_block::block::BlockFile;

/// The time maintenance took on the put path: packing memtables into branches, the trunk's
/// steps, and the stalls, a put paying a debt whole because its room ran out; in total and at the
/// most for one slice.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FlushStats {
    /// Memtables packed.
    pub flushes: u64,
    /// Nanoseconds packing, in all and at the most a slice.
    pub pack_ns: u64,
    pub pack_max_ns: u64,
    /// Nanoseconds in the trunk's steps, in all and at the most a slice.
    pub incorporate_ns: u64,
    pub incorporate_max_ns: u64,
    /// Nanoseconds finishing packed memtables: the walk's rest, the last pages and the filter.
    pub pack_finish_ns: u64,
    /// The largest share of each debt one put was given: entries to pack, keys to merge.
    pub pack_share_most: u64,
    pub trunk_share_most: u64,
    /// Stalls, and their nanoseconds in all and at the most.
    pub stalls: u64,
    pub stall_ns: u64,
    pub stall_max_ns: u64,
}

/// A memtable read in key order from a seek, its next entry held in buffers it reuses: one
/// source of a scan.
struct MemCursor {
    walk: Walk,
    key: Vec<u8>,
    op: Op,
    value: Vec<u8>,
    valid: bool,
}

impl MemCursor {
    fn new(mem: &BTreeMem, from: &[u8]) -> Result<Self, Error> {
        let mut c = Self {
            walk: mem.walk_from(from)?,
            key: Vec::new(),
            op: Op::Put,
            value: Vec::new(),
            valid: false,
        };
        c.advance(mem)?;
        Ok(c)
    }

    fn key(&self) -> Option<&[u8]> {
        if self.valid {
            Some(self.key.as_slice())
        } else {
            None
        }
    }

    /// Moves to the next entry, none past the last.
    fn advance(&mut self, mem: &BTreeMem) -> Result<(), Error> {
        let Self {
            walk,
            key,
            op,
            value,
            valid,
        } = self;
        *valid = false;
        mem.walk_some(walk, 1, |k, o, v| {
            key.clear();
            key.extend_from_slice(k);
            value.clear();
            value.extend_from_slice(v);
            *op = o;
            *valid = true;
            Ok(())
        })?;
        Ok(())
    }
}

/// A full memtable packed into a branch a slice at a time, read until it is packed.
#[derive(Debug)]
struct Packing {
    mem: BTreeMem,
    walk: Walk,
    builder: Builder,
    packed: usize,
}

impl Packing {
    /// The keys' worth of packing left: entries to pack, then the branch's filter pages, each at
    /// its worth in keys.
    fn left(&self) -> u64 {
        let entries = u64::try_from(self.mem.len().saturating_sub(self.packed)).unwrap_or(u64::MAX);
        self.builder
            .filter_pages_left()
            .saturating_mul(self.builder.page_keys())
            .saturating_add(entries)
    }
}

/// A shard's engine over a store.
#[derive(Debug)]
pub struct ShardDb<F: BlockFile> {
    store: Store<F>,
    mem: BTreeMem,
    packing: Option<Packing>,
    /// A cleared memtable for the next fill, its arena kept.
    spare: Option<BTreeMem>,
    mem_limit: usize,
    trunk: Trunk,
    /// Work owed but not yet whole, in work·bytes over the room: the remainder of each debt's
    /// share.
    pack_carry: u128,
    trunk_carry: u128,
    forget_carry: u128,
    flush_stats: FlushStats,
    /// The key a scan is at, and the bounds of the trunk segment it reads: buffers kept
    /// between scans.
    scan_key: Vec<u8>,
    scan_from: Vec<u8>,
    scan_end: Vec<u8>,
    /// Whether maintenance is timed ([`ShardDb::set_timed`]).
    timed: bool,
}

/// Nanoseconds since `t`, none when timing is off (`ShardDb::set_timed`).
fn ns_since(t: Option<std::time::Instant>) -> u64 {
    t.map_or(0, |t| {
        u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX)
    })
}

/// A put's share of `debt`: `debt · bytes / room`, the remainder carried to the next put; the
/// whole debt when no room is left.
fn share(debt: u64, bytes: usize, room: usize, carry: &mut u128) -> u64 {
    let (bytes, room) = (
        u128::try_from(bytes).unwrap_or(u128::MAX),
        u128::try_from(room).unwrap_or(u128::MAX),
    );
    let owed = u128::from(debt)
        .saturating_mul(bytes)
        .saturating_add(*carry);
    if room == 0 {
        *carry = 0;
        return debt;
    }
    *carry = owed.checked_rem(room).unwrap_or(0);
    u64::try_from(owed.checked_div(room).unwrap_or(0))
        .unwrap_or(u64::MAX)
        .min(debt)
}

impl<F: BlockFile> ShardDb<F> {
    fn with(store: Store<F>, mem_limit: usize, trunk: Trunk) -> Result<Self, Error> {
        Ok(Self {
            store,
            mem: BTreeMem::new(mem_limit)?,
            packing: None,
            spare: None,
            mem_limit,
            trunk,
            pack_carry: 0,
            trunk_carry: 0,
            forget_carry: 0,
            flush_stats: FlushStats::default(),
            scan_key: Vec::new(),
            scan_from: Vec::new(),
            scan_end: Vec::new(),
            timed: false,
        })
    }

    /// A new engine in `file`, which must be empty: its store created, its trunk empty, its
    /// memtables bounded at `mem_limit` bytes each.
    pub fn create(
        file: F,
        store: Config,
        mem_limit: usize,
        trunk: TrunkConfig,
    ) -> Result<Self, Error> {
        Self::with(Store::create(file, store)?, mem_limit, Trunk::new(trunk)?)
    }

    /// The engine in `file` at its newest checkpoint, and the last Raft index that checkpoint
    /// holds: the log is replayed from the next. A checkpoint with no trunk yet gives an empty
    /// trunk of `trunk`'s shape.
    pub fn open(
        file: F,
        store: Config,
        mem_limit: usize,
        trunk: TrunkConfig,
    ) -> Result<(Self, u64), Error> {
        let (mut store, recovered) = Store::open(file, store)?;
        let trunk = match recovered.root {
            Some(head) => Trunk::load(&mut store, head)?,
            None => Trunk::new(trunk)?,
        };
        Ok((Self::with(store, mem_limit, trunk)?, recovered.applied))
    }

    /// Makes the engine's state through Raft index `applied` durable: the memtable packed into
    /// the trunk, the trunk's image written, and a checkpoint naming it, durable when this
    /// returns.
    pub fn checkpoint(&mut self, applied: u64) -> Result<(), Error> {
        self.flush()?;
        let head = self.trunk.save(&mut self.store)?;
        self.store.checkpoint(Some(head), applied)
    }

    /// Checks that the store holds exactly the extents the engine names, each once: the
    /// superblocks', the allocator map's, the trunk image's and every branch's. Run after
    /// recovery, it finds an extent leaked or freed while named; a difference is a typed
    /// corruption.
    pub fn check_references(&self) -> Result<(), Error> {
        let mut named = std::collections::BTreeSet::new();
        named.insert(0u64);
        let mut once = |e: u64| -> Result<(), Error> {
            if named.insert(e) {
                Ok(())
            } else {
                Err(Error::Corruption {
                    what: "the extents a shard names",
                    why: crate::error::Malformed::CountMismatch,
                })
            }
        };
        for &e in self.store.map_extents() {
            once(e)?;
        }
        for &e in self.trunk.image_extents() {
            once(e)?;
        }
        for b in self.trunk.branches() {
            for &e in &b.extents {
                once(e)?;
            }
        }
        let held = self
            .store
            .refs()
            .iter()
            .enumerate()
            .filter(|&(_, &c)| c > 0);
        let mut count = 0usize;
        for (e, &c) in held {
            let e = u64::try_from(e).map_err(|_| Error::InvalidArgument {
                what: "an extent past u64",
            })?;
            if c != 1 || !named.contains(&e) {
                return Err(Error::Corruption {
                    what: "the extents a shard holds",
                    why: crate::error::Malformed::CountMismatch,
                });
            }
            count = count.saturating_add(1);
        }
        if count != named.len() {
            return Err(Error::Corruption {
                what: "the extents a shard holds",
                why: crate::error::Malformed::CountMismatch,
            });
        }
        Ok(())
    }

    /// Times maintenance, its phases and the store's I/O into [`FlushStats`], `TrunkStats` and
    /// `IoStats`: off by default, as a clock read each slice costs every put.
    pub fn set_timed(&mut self, on: bool) {
        self.timed = on;
        self.trunk.set_timed(on);
        self.store.set_timed(on);
    }

    /// Gives point reads a page cache of `pages` pages (`Store::set_cache`).
    pub fn set_cache(&mut self, pages: usize) {
        self.store.set_cache(pages);
    }

    /// Hands the store's writes to `issuer`, the device's, with up to `batches` out at once
    /// (`Store::attach`): a put that fills an extent goes on while the device writes it.
    pub fn attach(
        &mut self,
        issuer: &hyper_block::issuer::Issuer,
        batches: usize,
    ) -> Result<(), Error>
    where
        F: 'static,
    {
        self.store.attach(issuer, batches)
    }

    /// Waits for every write handed to the device's issuer to land (`Store::drain`).
    pub fn land(&mut self) -> Result<(), Error> {
        self.store.drain()
    }

    /// The store's file, the engine's work done, and whether every write handed to the device's
    /// issuer landed (`Store::into_file`).
    pub fn into_file(self) -> (F, Result<(), Error>) {
        self.store.into_file()
    }

    fn apply(&mut self, key: &[u8], op: Op, value: &[u8]) -> Result<(), Error> {
        let mut before = self.mem.bytes();
        match self.mem.insert(key, op, value) {
            Err(Error::LimitExceeded { .. }) if !self.mem.is_empty() => {
                self.rotate()?;
                before = self.mem.bytes();
                self.mem.insert(key, op, value)?;
            }
            other => other?,
        }
        self.pace(self.mem.bytes().saturating_sub(before))
    }

    /// Records `key` holding `value`.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.apply(key, Op::Put, value)
    }

    /// Records `key` deleted.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.apply(key, Op::Delete, &[])
    }

    /// The value `key` holds, into `value`; false when it holds none.
    pub fn get(&mut self, key: &[u8], value: &mut Vec<u8>) -> Result<bool, Error> {
        let mut found = self.mem.get(key, value)?;
        if found.is_none()
            && let Some(p) = &self.packing
        {
            found = p.mem.get(key, value)?;
        }
        if found.is_none() {
            found = self.trunk.get(&mut self.store, key, value)?;
        }
        Ok(found == Some(Op::Put))
    }

    /// A page of the range `[from, end)` (to the end with no `end`): up to `limit` keys that
    /// hold values, in key order, appended to `out`; true when the page filled before the range
    /// ended, with the key to continue from in `next`. The continuation is the next key any
    /// source holds, which may be one a deletion hides: the next page then starts there and
    /// reads it as absent. As S3's listings page, so no scan state lives across writes. Each key
    /// reads its newest entry: the memtable, the one being packed, then the trunk leaf by leaf
    /// (`Trunk::segments`); a deletion hides the key. A row costs no allocation once `out`,
    /// `next` and the shard's own scan buffer have grown.
    pub fn scan(
        &mut self,
        from: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        out: &mut Rows,
        next: &mut Vec<u8>,
    ) -> Result<bool, Error> {
        next.clear();
        if limit == 0 {
            next.extend_from_slice(from);
            return Ok(true);
        }
        let mut active = MemCursor::new(&self.mem, from)?;
        let mut packing = match &self.packing {
            Some(p) => Some(MemCursor::new(&p.mem, from)?),
            None => None,
        };
        let trunk = &self.trunk;
        let store = &mut self.store;
        let key = &mut self.scan_key;
        // The trunk's leaf segments, found one at a time from the seek's key: a segment's
        // branches, and where the next one starts.
        let seg_from = &mut self.scan_from;
        let seg_end = &mut self.scan_end;
        seg_from.clear();
        seg_from.extend_from_slice(from);
        let mut branches = Vec::new();
        let mut trunk_done = false;
        let mut merge: Option<crate::branch::merge::Merge> = None;
        let before_end = |k: &[u8]| end.is_none_or(|e| k < e);
        let mut taken = 0usize;
        let more = loop {
            // The trunk's next key: from the segment open, or the next segment's first.
            while merge.as_ref().is_none_or(|m| m.entry().is_none()) {
                if let Some(m) = merge.take() {
                    m.give_back(store);
                }
                if trunk_done || !before_end(seg_from) {
                    break;
                }
                let bounded = trunk.segment_at(seg_from, &mut branches, seg_end)?;
                // Each segment ends past its start, so the walk moves a leaf a step and ends.
                if bounded && seg_end.as_slice() <= seg_from.as_slice() {
                    return Err(Error::Corruption {
                        what: "a trunk segment's bounds",
                        why: crate::error::Malformed::OutOfOrder,
                    });
                }
                let hi = match (bounded, end) {
                    (true, Some(e)) if e < seg_end.as_slice() => Some(e),
                    (true, _) => Some(seg_end.as_slice()),
                    (false, e) => e,
                };
                merge = Some(crate::branch::merge::Merge::new(
                    store,
                    branches.iter().copied(),
                    seg_from,
                    hi,
                )?);
                if bounded {
                    std::mem::swap(seg_from, seg_end);
                } else {
                    trunk_done = true;
                }
            }
            // The smallest key any source holds next, copied so the sources can move.
            let tree = merge.as_ref().and_then(|m| m.entry()).map(|(k, _, _)| k);
            let mem_a = active.key().filter(|k| before_end(k));
            let mem_p = packing
                .as_ref()
                .and_then(MemCursor::key)
                .filter(|k| before_end(k));
            let Some(least) = [mem_a, mem_p, tree].into_iter().flatten().min() else {
                break false;
            };
            key.clear();
            key.extend_from_slice(least);
            if taken == limit {
                next.extend_from_slice(key);
                break true;
            }
            // The newest source holding the key decides; every source holding it moves past it.
            let mut decided = false;
            if active.key() == Some(key.as_slice()) {
                decided = true;
                if active.op == Op::Put {
                    out.push(key, &active.value);
                    taken = taken.saturating_add(1);
                }
                active.advance(&self.mem)?;
            }
            if let (Some(c), Some(p)) = (packing.as_mut(), self.packing.as_ref())
                && c.key() == Some(key.as_slice())
            {
                if !decided {
                    decided = true;
                    if c.op == Op::Put {
                        out.push(key, &c.value);
                        taken = taken.saturating_add(1);
                    }
                }
                c.advance(&p.mem)?;
            }
            if let Some(m) = merge.as_mut()
                && let Some((k, op, v)) = m.entry()
                && k == key.as_slice()
            {
                if !decided && op == Op::Put {
                    out.push(key, v);
                    taken = taken.saturating_add(1);
                }
                m.next(store)?;
            }
        };
        if let Some(m) = merge.take() {
            m.give_back(store);
        }
        Ok(more)
    }

    /// A put that took `bytes` of the memtable does its share of each debt (the module's
    /// pacing).
    fn pace(&mut self, bytes: usize) -> Result<(), Error> {
        let room = self.mem.room();
        if let Some(p) = &self.packing {
            let w = share(p.left(), bytes, room, &mut self.pack_carry);
            self.flush_stats.pack_share_most = self.flush_stats.pack_share_most.max(w);
            self.pack_some(w)?;
        }
        let waiting = self.trunk.fanout().saturating_sub(self.trunk.pending());
        let trunk_room = room.saturating_add(waiting.saturating_mul(self.mem_limit));
        // The cache's freed pages, forgotten over the same room as the trunk's work that freed
        // them.
        let forget = self.store.forget_debt();
        if forget > 0 {
            let w = share(forget, bytes, trunk_room, &mut self.forget_carry);
            if w > 0 {
                self.store.forget_some(w);
            }
        }
        let debt = self.trunk.debt();
        if debt > 0 {
            let room = trunk_room;
            let w = share(debt, bytes, room, &mut self.trunk_carry);
            self.flush_stats.trunk_share_most = self.flush_stats.trunk_share_most.max(w);
            if w > 0 {
                let t = self.timed.then(std::time::Instant::now);
                self.trunk.step(&mut self.store, w)?;
                self.note_trunk(ns_since(t));
            }
        }
        Ok(())
    }

    /// Packs `w` keys' worth of the packing memtable: entries, then its filter pages at their
    /// worth in keys; the branch goes to the trunk once whole.
    fn pack_some(&mut self, w: u64) -> Result<(), Error> {
        let Some(p) = &mut self.packing else {
            return Ok(());
        };
        if w == 0 {
            return Ok(());
        }
        let t = self.timed.then(std::time::Instant::now);
        let Packing {
            mem,
            walk,
            builder,
            packed,
        } = p;
        let store = &mut self.store;
        let entries = u64::try_from(mem.len().saturating_sub(*packed)).unwrap_or(u64::MAX);
        let walked = w.min(entries);
        if walked > 0 {
            let limit = usize::try_from(walked).unwrap_or(usize::MAX);
            let visited = mem.walk_some(walk, limit, |k, op, v| builder.add(store, k, op, v))?;
            *packed = packed.saturating_add(visited);
        }
        let mut whole = false;
        let over = w.saturating_sub(walked);
        if *packed >= mem.len() && over > 0 {
            builder.seal(store)?;
            let pages = over.checked_div(builder.page_keys()).unwrap_or(1).max(1);
            whole = builder.write_filter(store, pages)?;
        }
        self.note_pack(ns_since(t));
        if whole {
            self.finish_packing()?;
        }
        Ok(())
    }

    /// Whether maintenance is owed: a memtable packing, the trunk's work, or freed pages the
    /// cache has yet to forget.
    pub fn owed(&self) -> bool {
        self.packing.is_some() || self.trunk.debt() > 0 || self.store.forget_debt() > 0
    }

    /// Pays up to `keys` keys' worth of maintenance owed, for the shard's idle time (SILK: the
    /// flush first, since a full memtable stops puts; then the trunk's work; then the cache's
    /// forgetting). Returns the work done, 0 once nothing is owed.
    pub fn idle_step(&mut self, keys: u64) -> Result<u64, Error> {
        if let Some(p) = &self.packing {
            // At least one: a memtable packed whole with no filter page left still has its
            // branch to finish.
            let w = keys.min(p.left()).max(1);
            self.pack_some(w)?;
            return Ok(w);
        }
        if self.trunk.debt() > 0 {
            let t = self.timed.then(std::time::Instant::now);
            let used = self.trunk.step(&mut self.store, keys)?;
            self.note_trunk(ns_since(t));
            return Ok(used);
        }
        Ok(self.store.forget_some(keys))
    }

    fn note_pack(&mut self, ns: u64) {
        let f = &mut self.flush_stats;
        f.pack_ns = f.pack_ns.saturating_add(ns);
        f.pack_max_ns = f.pack_max_ns.max(ns);
    }

    fn note_trunk(&mut self, ns: u64) {
        let f = &mut self.flush_stats;
        f.incorporate_ns = f.incorporate_ns.saturating_add(ns);
        f.incorporate_max_ns = f.incorporate_max_ns.max(ns);
    }

    /// Packs the rest of the packing memtable, gives its branch to the trunk as pending, and
    /// keeps the memtable, cleared, for the next fill.
    fn finish_packing(&mut self) -> Result<(), Error> {
        let Some(Packing {
            mut mem,
            mut walk,
            mut builder,
            ..
        }) = self.packing.take()
        else {
            return Ok(());
        };
        let t = self.timed.then(std::time::Instant::now);
        let store = &mut self.store;
        mem.walk_some(&mut walk, usize::MAX, |k, op, v| {
            builder.add(store, k, op, v)
        })?;
        let branch = builder.finish(&mut self.store)?;
        let ns = ns_since(t);
        self.note_pack(ns);
        self.flush_stats.pack_finish_ns = self.flush_stats.pack_finish_ns.saturating_add(ns);
        self.trunk.add(branch);
        mem.clear();
        self.spare = Some(mem);
        self.pack_carry = 0;
        self.flush_stats.flushes = self.flush_stats.flushes.saturating_add(1);
        Ok(())
    }

    /// The memtable is full: it becomes the packing one and a cleared one takes the puts. A debt
    /// still owed is paid first, whole, and counted a stall: the packing memtable's, and the
    /// trunk's cascade when `fanout` packed memtables wait on it.
    fn rotate(&mut self) -> Result<(), Error> {
        let t = self.timed.then(std::time::Instant::now);
        let mut stalled = false;
        if self.packing.is_some() {
            self.finish_packing()?;
            stalled = true;
        }
        if self.trunk.pending() >= self.trunk.fanout() {
            self.trunk.finish_cascade(&mut self.store)?;
            stalled = true;
        }
        if stalled {
            let ns = ns_since(t);
            let f = &mut self.flush_stats;
            f.stalls = f.stalls.saturating_add(1);
            f.stall_ns = f.stall_ns.saturating_add(ns);
            f.stall_max_ns = f.stall_max_ns.max(ns);
        }
        let fresh = match self.spare.take() {
            Some(m) => m,
            None => BTreeMem::new(self.mem_limit)?,
        };
        let full = std::mem::replace(&mut self.mem, fresh);
        self.packing = Some(Packing {
            walk: full.walk_start(),
            builder: Builder::new(
                &mut self.store,
                Keys::Exactly(u64::try_from(full.len()).unwrap_or(u64::MAX)),
            )?,
            mem: full,
            packed: 0,
        });
        Ok(())
    }

    /// Pays maintenance owed, for the shard's idle time: the packing memtable's whole, then up
    /// to `budget` keys of the trunk's. Returns the trunk's keys merged, fewer than `budget` only
    /// once nothing is owed.
    pub fn maintain(&mut self, budget: u64) -> Result<u64, Error> {
        self.finish_packing()?;
        let t = self.timed.then(std::time::Instant::now);
        let used = self.trunk.step(&mut self.store, budget)?;
        self.note_trunk(ns_since(t));
        Ok(used)
    }

    /// Packs every memtable into the trunk and runs its maintenance to the end.
    pub fn flush(&mut self) -> Result<(), Error> {
        self.finish_packing()?;
        if !self.mem.is_empty() {
            let fresh = match self.spare.take() {
                Some(m) => m,
                None => BTreeMem::new(self.mem_limit)?,
            };
            let full = std::mem::replace(&mut self.mem, fresh);
            self.packing = Some(Packing {
                walk: full.walk_start(),
                builder: Builder::new(
                    &mut self.store,
                    Keys::Exactly(u64::try_from(full.len()).unwrap_or(u64::MAX)),
                )?,
                mem: full,
                packed: 0,
            });
            self.finish_packing()?;
        }
        let t = self.timed.then(std::time::Instant::now);
        self.trunk.drain(&mut self.store)?;
        self.note_trunk(ns_since(t));
        self.trunk_carry = 0;
        Ok(())
    }

    /// The flushes' time, the trunk's maintenance, and the store's I/O since the engine started.
    pub fn stats(&self) -> (FlushStats, crate::trunk::TrunkStats, crate::store::IoStats) {
        (self.flush_stats, self.trunk.stats(), self.store.io_stats())
    }

    /// The trunk's shape: height, nodes, leaves.
    pub fn shape(&self) -> Result<(usize, usize, usize), Error> {
        self.trunk.shape()
    }
}
