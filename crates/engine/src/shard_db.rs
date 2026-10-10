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
use crate::memtable::hashed::{HashMem, Walk};
use crate::rows::Rows;
use crate::scan::{BorrowedScanMerge, ScanMerge};
use crate::store::{Config, IntoFile, Store};
use crate::trunk::pool::{self, Back, FEED_BUFFERS, Job, Owner, Pool, Spawn, Stream, Work};
use crate::trunk::{Source, Trunk, TrunkConfig};
use crate::util::reuse;
use hyper_block::block::BlockFile;
use std::collections::VecDeque;
use std::sync::mpsc::{SyncSender, sync_channel};

/// The time maintenance took on the put path: packing memtables into branches, the trunk's
/// steps, and the stalls, a put paying a debt whole because its room ran out; in total and at the
/// most for one slice.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FlushStats {
    /// Memtables packed.
    /// Of the memtables packed, those a maintenance worker built ([`ShardDb::set_workers`]).
    pub fed: u64,
    /// The most memtables frozen at once, fed whole to their workers and awaiting their branches.
    pub frozen_most: u64,
    /// Frozen memtables whose worker failed, packed again on the shard.
    pub repacked: u64,
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
    /// Nanoseconds in memtable inserts, in rotations (a full memtable handed to packing, its
    /// stall included), and in forgetting freed pages: in all, timed only as the rest are.
    /// Memtables rotated: filled, and handed to packing.
    pub rotations: u64,
    pub insert_ns: u64,
    pub rotate_ns: u64,
    pub forget_ns: u64,
    /// Nanoseconds the active memtable's order work took inside puts (its paced sorts and
    /// merges), and retiring a packed memtable for reuse (clearing it and its index): timed
    /// only as the rest are.
    pub order_ns: u64,
    pub retire_ns: u64,
    /// Puts whose trunk share waited for its input reads: the last memtable before a rotation
    /// would stall ([`ShardDb::put`]'s pacing).
    pub waited_steps: u64,
}

/// A memtable read in key order from a seek, its next entry held in buffers it reuses: one
/// source of a scan.
#[derive(Debug)]
struct MemCursor {
    walk: Walk,
    key: Vec<u8>,
    op: Op,
    value: Vec<u8>,
    valid: bool,
}

impl MemCursor {
    /// A cursor at no entry, allocating nothing.
    fn empty() -> Self {
        Self {
            walk: Walk::default(),
            key: Vec::new(),
            op: Op::Put,
            value: Vec::new(),
            valid: false,
        }
    }

    /// Moves to `mem`'s first entry at or past `from`, its buffers kept.
    fn seek(&mut self, mem: &HashMem, from: &[u8]) -> Result<(), Error> {
        mem.walk_from_into(from, &mut self.walk)?;
        self.advance(mem)
    }

    fn key(&self) -> Option<&[u8]> {
        if self.valid {
            Some(self.key.as_slice())
        } else {
            None
        }
    }

    /// Moves to the next entry, none past the last.
    fn advance(&mut self, mem: &HashMem) -> Result<(), Error> {
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

/// A full memtable packed into a branch a slice at a time, read until it is packed. The shard
/// pays its order and walks it; the branch is built by the shard's own `builder`, or, with
/// maintenance workers, by a worker the walked entries are fed to (`feed`), the shard keeping the
/// memtable to read meanwhile.
#[derive(Debug)]
struct Packing {
    mem: HashMem,
    /// The walk in key order, opened once the memtable's order work is paid: its runs do not
    /// change after.
    walk: Option<Walk>,
    builder: Option<Builder>,
    feed: Option<Feed>,
    packed: usize,
    /// When it began packing: its rotation.
    since: std::time::Instant,
}

/// A packing fed to worker `worker`: the channel its full buffers go out on, closed once every
/// entry is sent; the buffers the shard holds, the one it fills and those spare (the worker
/// gives the others back through the pool, [`Pool::buffer`]); and the extents granted the job.
#[derive(Debug)]
struct Feed {
    worker: usize,
    full: Option<SyncSender<Vec<u8>>>,
    filling: Option<Vec<u8>>,
    spare: Vec<Vec<u8>>,
    grant: Vec<u64>,
}

/// A full memtable handed to its packing worker, its entries fed to it by later puts: still
/// read until the worker's branch is back and every older memtable's branch has gone to the trunk before it,
/// since pending branches are read newest first. Its worker, the extents granted the job, the
/// worker's result once back, when it began packing, and its first failure. A failed input
/// stays readable and never retires past its older predecessors ([`ShardDb::retire`]).
#[derive(Debug)]
struct Frozen {
    mem: HashMem,
    worker: usize,
    grant: Vec<u64>,
    back: Option<Back>,
    since: std::time::Instant,
    error: Option<Error>,
    /// Its walk and feed while entries are still to send: puts go on feeding it after its
    /// rotation, oldest first, so a rotation never feeds a memtable whole ([`ShardDb::feed`]).
    walk: Option<Walk>,
    feed: Option<Feed>,
    packed: usize,
}

impl Frozen {
    /// Whether its worker result or retained failure is ready to report.
    fn ready(&self) -> bool {
        self.back.is_some() || self.error.is_some()
    }
}

impl Packing {
    /// The keys' worth of packing left: entries to pack, then the branch's filter pages, each at
    /// its worth in keys; one more while a worker's branch is still to come back.
    fn left(&self) -> u64 {
        let entries = u64::try_from(self.mem.len().saturating_sub(self.packed)).unwrap_or(u64::MAX);
        let order = u64::try_from(self.mem.debt()).unwrap_or(u64::MAX);
        let filter = self
            .builder
            .as_ref()
            .map_or(0, |b| b.filter_pages_left().saturating_mul(b.page_keys()));
        let back = u64::from(self.feed.is_some());
        filter
            .saturating_add(entries)
            .saturating_add(order)
            .saturating_add(back)
    }
}

/// Walks up to `w` of a frozen or packing memtable's entries into its feed's buffers, each
/// sent to the worker once it holds a run's bytes (`run`); with every entry sent, the feed
/// closes. Without a buffer to fill it stops: the wait for its worker to give one back is the
/// caller's ([`ShardDb::feed_from`]). The entries walked.
#[allow(clippy::too_many_arguments)]
fn feed_one<F: BlockFile>(
    store: &mut Store<F>,
    pool: &mut Pool,
    mem: &HashMem,
    walk: &mut Walk,
    f: &mut Feed,
    packed: &mut usize,
    w: u64,
    run: usize,
) -> Result<u64, Error> {
    // The mean entry's bytes in the arena: how many entries a buffer's room takes.
    let mean = mem.bytes().checked_div(mem.len()).unwrap_or(1).max(1);
    let mut budget = w;
    while *packed < mem.len() && budget > 0 && f.full.is_some() {
        if f.filling.is_none() {
            f.filling = match f.spare.pop() {
                Some(b) => Some(b),
                None => pool.buffer(store, f.worker, false)?,
            };
        }
        let Some(buf) = f.filling.as_mut() else {
            break;
        };
        let room = run.saturating_sub(buf.len());
        let fit = u64::try_from(room.checked_div(mean).unwrap_or(0).max(1)).unwrap_or(u64::MAX);
        let limit = usize::try_from(budget.min(fit)).unwrap_or(usize::MAX);
        let visited =
            mem.walk_some_hashed(walk, limit, |k, op, v, h| pool::encode(buf, k, op, v, h))?;
        *packed = packed.saturating_add(visited);
        budget = budget.saturating_sub(u64::try_from(visited).unwrap_or(u64::MAX));
        let last = *packed >= mem.len();
        if (buf.len() >= run || last || visited == 0)
            && let Some(buf) = f.filling.take()
            && let Some(full) = f.full.as_ref()
            && let Err(back) = full.send(buf)
        {
            // The worker is gone, its failure on its way back: the buffer stays.
            f.spare.push(back.0);
            f.full = None;
        }
        if visited == 0 {
            break;
        }
    }
    if *packed >= mem.len() {
        f.full = None;
    }
    Ok(w.saturating_sub(budget))
}

/// The batches a device's issuer of `depth` is started for (`Issuer::start_for`) to hold a
/// shard that keeps `shard_batches` out and its maintenance workers, each with `depth` out
/// (`ShardDb::attach`), up to the cores they may use: within it a submission never waits for
/// the issuer's inbox.
pub fn issuer_batches(shard_batches: usize, depth: usize) -> usize {
    shard_batches.saturating_add(Pool::cores().saturating_mul(depth))
}

/// A runtime task's rotation, resumed before its entry is inserted. Its overlap bound is
/// measured once, not again after waiting for a worker.
#[derive(Debug)]
struct Rotation {
    started: std::time::Instant,
    finish_packing: bool,
    most: Option<usize>,
    stalled: bool,
}

/// One retained durability operation. Its index and phases survive a canceled borrowed wait.
#[derive(Debug)]
struct Checkpoint {
    applied: u64,
    phase: CheckpointPhase,
    error: Option<Error>,
}

#[derive(Clone, Copy, Debug)]
enum CheckpointPhase {
    Flush,
    Save,
    Metadata(u64),
}

/// A shard's engine over a store.
#[derive(Debug)]
pub struct ShardDb<F: BlockFile> {
    store: Store<F>,
    mem: HashMem,
    packing: Option<Packing>,
    rotation: Option<Rotation>,
    checkpoint: Option<Checkpoint>,
    /// Terminal worker cleanup leaves active entries unflushed, as the synchronous Stop did.
    stopping: bool,
    terminal_error: Option<Error>,
    terminal_done: bool,
    /// Memtables fed whole to their packing workers, oldest first ([`Frozen`]); at most
    /// [`Self::frozen_most`].
    frozen: VecDeque<Frozen>,
    /// When the active memtable began filling, how long the last one took to fill, and how long
    /// the last packing took from its rotation to its branch: what bounds the memtables in
    /// flight ([`Self::frozen_most`]).
    rotated: Option<std::time::Instant>,
    fill_ns: u64,
    pack_ns: u64,
    /// A cleared memtable for the next fill, its arena kept.
    spare: Option<HashMem>,
    /// Packing buffers back from the last packing fed to a worker, for the next: at most
    /// [`FEED_BUFFERS`].
    feed_buffers: Vec<Vec<u8>>,
    mem_limit: usize,
    trunk: Trunk,
    /// Work owed but not yet whole, in work·bytes over the room: the remainder of each debt's
    /// share.
    pack_carry: u128,
    /// The device's issuer for the maintenance workers' stores and the batches each may have
    /// out, once the shard is attached ([`Self::attach`]).
    worker_attach: Option<(hyper_block::issuer::Attacher, usize)>,
    trunk_carry: u128,
    /// The active memtable's order debt's carry ([`share`]).
    /// Entries the active memtable leaves unsorted at most before it sorts them
    /// ([`Self::set_order_bound`]).
    order_bound: usize,
    /// Nanoseconds scans have spent on the active memtable's runs being many rather than one,
    /// not yet spent back on merging them. Idle time merges them only once this covers the merge's
    /// measured cost: the ski-rental rule, within twice the cost of the best choice made knowing
    /// every scan to come (Karlin, Manasse, Rudolph and Sleator, Algorithmica 1988). A scan is
    /// what makes the merge worth anything: with none, nothing is merged, and merging while
    /// writes keep coming would merge the large run with every new small one.
    tidy_rent_ns: u64,
    /// The nanoseconds and comparisons of the active memtables' seeks measured so far, and of
    /// idle merging and its entries moved: the prices the rule weighs, a merge's priced as a
    /// seek's comparison until one is measured.
    seek_ns: u64,
    seek_cmps: u64,
    tidy_ns: u64,
    tidy_moves: u64,
    forget_carry: u128,
    flush_stats: FlushStats,
    /// The write memory the owner spares at most, and the pages written when the last cycle
    /// began ([`ShardDb::set_write_budget`]).
    write_cap: usize,
    /// Fixed output reservations: the owner's lifetime peak, and owner plus admitted Pool
    /// capacity. Pool credit is kept until its workers and attachments have retired.
    owner_write_reserved: usize,
    write_reserved: usize,
    cycle_pages: u64,
    /// The memory budget for the cache and write memory together, write memory's share of it,
    /// and the store's counters when it was last divided ([`ShardDb::set_memory`]).
    memory: Option<usize>,
    /// The cache of hot records point reads consult before the trunk, if the owner gave one,
    /// its share of the memory budget, and the gets it missed with their trunk lookups'
    /// nanoseconds (what a record-cache hit saves), since the last division.
    records: Option<crate::records::RecordCache>,
    records_share: usize,
    /// The tuning cycle: the last write cycle's bytes written, its length in operations (a
    /// memtable's entries), and the gets counted since it began.
    cycle_written: usize,
    cycle_ops: u64,
    ops_since: u64,
    record_misses: u64,
    record_miss_ns: u64,
    record_ghost_hits: u64,
    write_share: usize,
    tuned: crate::store::IoStats,
    /// The tuner's past steps, for its Newton step.
    newton: Newton,
    /// Pages each operation trims from the page cache and the record cache while a tuning step
    /// left them above their new size.
    trim_cache: usize,
    trim_records: usize,
    /// The page cache's share in bytes, the record cache's being `records_share`.
    cache_share: usize,
    /// The page cache's and record cache's index and ghost bytes when last granted: what their
    /// shares pay for besides data.
    charged: (usize, usize),
    /// The key a scan is at, and the bounds of the trunk segment it reads: buffers kept
    /// between scans.
    scan_key: Vec<u8>,
    scan_from: Vec<u8>,
    /// A scan's memtable cursors, its segment's sources and its merge, kept between scans so a
    /// scan allocates nothing once they have grown.
    scan_active: MemCursor,
    scan_packing: MemCursor,
    /// A scan's cursors of the frozen memtables, kept between scans: at most one a frozen one.
    scan_frozen: Vec<MemCursor>,
    scan_sources: Vec<Source<'static>>,
    /// The pivots a scan's segments passed, each with its segment's measured cost to open a
    /// source: charged to the trunk once the scan ends ([`Trunk::charge`]).
    scan_passed: Vec<crate::trunk::Passed>,
    scan_charges: Vec<(crate::trunk::Passed, u64)>,
    scan_merge: ScanMerge<'static>,
    /// Trunk sources scans passed over by their range filters, and sources they opened.
    scan_skipped: u64,
    scan_opened: u64,
    scan_end: Vec<u8>,
    /// Whether maintenance is timed ([`ShardDb::set_timed`]).
    timed: bool,
}

/// Cited (Luo & Carey, PVLDB 2021 §5.4): a tuning step moves 5% of the memory budget toward
/// the region whose memory saves more, and takes at most 10% of the region giving it up, since
/// both regions' returns diminish.
const TUNE_STEP_PERCENT: usize = 5;
const TUNE_DONOR_PERCENT: usize = 10;

/// Cited (Luo & Carey, PVLDB 2021 §5.4): the Newton step fits the cost's slope to the last
/// three allocations.
const TUNE_SAMPLES: usize = 3;

/// The last allocations between one pair of regions and how much more a byte the first saved
/// than the second at each, for the Newton step: research/36, the paper's `K` of them.
#[derive(Debug, Default)]
struct Newton {
    pair: (usize, usize),
    samples: std::collections::VecDeque<(i128, i128)>,
}

impl Newton {
    /// Records that at `x` bytes in the pair's first region it saved `d` ns a MiB more than the
    /// second, and returns the bytes to move into the first region (negative: out of it) that
    /// bring `d` to zero on the line fitted to the samples, when the fit has returns
    /// diminishing (a negative slope); none otherwise, for the fixed step.
    fn step(&mut self, pair: (usize, usize), x: usize, d: i128) -> Option<i128> {
        if pair != self.pair {
            self.samples.clear();
            self.pair = pair;
        }
        if self.samples.len() >= TUNE_SAMPLES {
            self.samples.pop_front();
        }
        self.samples.push_back((i128::try_from(x).ok()?, d));
        let n = i128::try_from(self.samples.len()).ok()?;
        if n < 2 {
            return None;
        }
        // Least squares, scaled by n² so it stays in integers: slope = sxd / sxx.
        let (mut sx, mut sd, mut sxx, mut sxd) = (0i128, 0i128, 0i128, 0i128);
        for &(x, d) in &self.samples {
            sx = sx.checked_add(x)?;
            sd = sd.checked_add(d)?;
            sxx = sxx.checked_add(x.checked_mul(x)?)?;
            sxd = sxd.checked_add(x.checked_mul(d)?)?;
        }
        let sxx = n.checked_mul(sxx)?.checked_sub(sx.checked_mul(sx)?)?;
        let sxd = n.checked_mul(sxd)?.checked_sub(sx.checked_mul(sd)?)?;
        if sxx <= 0 || sxd >= 0 {
            return None;
        }
        d.checked_mul(sxx)?.checked_div(sxd)?.checked_neg()
    }
}

/// What a region of memory saved in a cycle, in nanoseconds, against the bytes it stands for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Gain {
    saved_ns: u64,
    bytes: usize,
}

/// Write memory's next share of `memory` against the page cache alone ([`tune3`] with no record
/// cache and no past steps).
#[cfg(test)]
fn tune(memory: usize, share: usize, written: usize, read: Gain, write: Gain) -> usize {
    let none = Gain {
        saved_ns: 0,
        bytes: 0,
    };
    let shares = [memory.saturating_sub(share), share, 0];
    tune3(
        memory,
        shares,
        written,
        [read, write, none],
        &mut Newton::default(),
    )[1]
}

/// The shard's memory divided again among the page cache, write memory and the record cache
/// (`shares`, in that order): a step moves toward the region whose bytes saved the most a byte,
/// from the region with memory that saved the least, the gains compared exactly by cross
/// products without dividing. The step is the Newton step on the pair's past steps (`newton`)
/// when their fit shows returns diminishing, the paper's fixed step otherwise, and takes at
/// most the donor bound either way. Write memory never passes the last cycle's writes,
/// `written`, since a queue past a cycle's writes spares no wait; the page cache takes the
/// rest. Equal gains, or nothing saved, change nothing.
fn tune3(
    memory: usize,
    shares: [usize; 3],
    written: usize,
    gains: [Gain; 3],
    newton: &mut Newton,
) -> [usize; 3] {
    let step = memory.saturating_mul(TUNE_STEP_PERCENT) / 100;
    let rate = |i: usize| {
        gains.get(i).copied().unwrap_or(Gain {
            saved_ns: 0,
            bytes: 0,
        })
    };
    // `a` saved more a byte than `b`: a.saved / a.bytes > b.saved / b.bytes.
    let more = |a: Gain, b: Gain| {
        let per = |g: Gain, other: Gain| {
            u128::from(g.saved_ns)
                .saturating_mul(u128::try_from(other.bytes.max(1)).unwrap_or(u128::MAX))
        };
        per(a, b) > per(b, a)
    };
    let mut to = 0usize;
    for i in 1..3 {
        if more(rate(i), rate(to)) {
            to = i;
        }
    }
    let mut from: Option<usize> = None;
    for i in 0..3 {
        if i == to || shares.get(i).copied().unwrap_or(0) == 0 {
            continue;
        }
        if from.is_none_or(|f| more(rate(f), rate(i))) {
            from = Some(i);
        }
    }
    let mut next = shares;
    if let Some(f) = from
        && more(rate(to), rate(f))
    {
        let donor = shares.get(f).copied().unwrap_or(0);
        // ns a MiB the region saved, signed differences of which the Newton step fits.
        let per_mib = |i: usize| {
            let g = rate(i);
            i128::from(g.saved_ns)
                .saturating_mul(1 << 20)
                .checked_div(i128::try_from(g.bytes.max(1)).unwrap_or(i128::MAX))
                .unwrap_or(0)
        };
        let (lo, hi) = (to.min(f), to.max(f));
        let toward = newton
            .step(
                (lo, hi),
                shares.get(lo).copied().unwrap_or(0),
                per_mib(lo).saturating_sub(per_mib(hi)),
            )
            .map(|m| if to == lo { m } else { m.saturating_neg() })
            .and_then(|m| usize::try_from(m).ok())
            .filter(|&m| m > 0);
        let give = toward
            .unwrap_or(step)
            .min(donor.saturating_mul(TUNE_DONOR_PERCENT) / 100);
        if let Some(d) = next.get_mut(f) {
            *d = d.saturating_sub(give);
        }
        if let Some(r) = next.get_mut(to) {
            *r = r.saturating_add(give);
        }
    }
    // Write memory within a cycle's writes and the budget; the page cache has the rest.
    let [_, write, records] = next;
    let write = write.min(written).min(memory);
    let records = records.min(memory.saturating_sub(write));
    [
        memory.saturating_sub(write).saturating_sub(records),
        write,
        records,
    ]
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

/// Entries the active memtable leaves unsorted at most by default, about 50 us of the radix
/// sort a scan inherits: the sort measured 20-30 ns an entry on the Apple M-series dev machine
/// (benches/shard_db.rs, research/40), seeks' p99.9 measured 41-165 us there, so a scan after a
/// burst of puts is held no longer than seeks' own tail (docs/design/constants.md).
pub const ORDER_BOUND: usize = 2048;

impl<F: BlockFile> ShardDb<F> {
    fn with(store: Store<F>, mem_limit: usize, trunk: Trunk) -> Result<Self, Error> {
        let write_reserved = store.write_reservation_for(None)?;
        Ok(Self {
            store,
            mem: HashMem::new(mem_limit)?,
            packing: None,
            rotation: None,
            checkpoint: None,
            stopping: false,
            terminal_error: None,
            terminal_done: false,
            frozen: VecDeque::new(),
            rotated: None,
            fill_ns: 0,
            pack_ns: 0,
            spare: None,
            feed_buffers: Vec::new(),
            mem_limit,
            trunk,
            pack_carry: 0,
            worker_attach: None,
            trunk_carry: 0,
            order_bound: ORDER_BOUND,
            tidy_rent_ns: 0,
            seek_ns: 0,
            seek_cmps: 0,
            tidy_ns: 0,
            tidy_moves: 0,
            forget_carry: 0,
            flush_stats: FlushStats::default(),
            write_cap: 0,
            owner_write_reserved: write_reserved,
            write_reserved,
            cycle_pages: 0,
            memory: None,
            records: None,
            records_share: 0,
            cycle_written: 0,
            cycle_ops: u64::MAX,
            ops_since: 0,
            record_misses: 0,
            record_miss_ns: 0,
            record_ghost_hits: 0,
            write_share: 0,
            tuned: crate::store::IoStats::default(),
            newton: Newton::default(),
            trim_cache: 0,
            trim_records: 0,
            cache_share: 0,
            charged: (0, 0),
            scan_key: Vec::new(),
            scan_from: Vec::new(),
            scan_active: MemCursor::empty(),
            scan_packing: MemCursor::empty(),
            scan_frozen: Vec::new(),
            scan_sources: Vec::new(),
            scan_passed: Vec::new(),
            scan_charges: Vec::new(),
            scan_merge: ScanMerge::new(),
            scan_skipped: 0,
            scan_opened: 0,
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
        self.no_checkpoint_mutation()?;
        self.flush()?;
        let head = self.trunk.save(&mut self.store)?;
        self.store.checkpoint(Some(head), applied)
    }

    /// Cold validation precedes every range spawn and any preparation mutation.
    pub(crate) fn validate_async_setup(&self) -> Result<(), Error> {
        if hyper_rt::registry::current_shard().is_some() {
            return Err(Error::InvalidArgument {
                what: "runtime engine preparation inside a runtime task",
            });
        }
        if !self.store.has_issuer() || self.worker_attach.is_none() {
            return Err(Error::InvalidArgument {
                what: "a runtime engine without its device issuer and maintenance workers",
            });
        }
        if self.store.fenced() {
            return Err(Error::Io {
                op: "prepare a runtime engine",
                detail: "the store was fenced by a failed write or flush".into(),
            });
        }
        if self.checkpoint.is_some() || self.packing.as_ref().is_some_and(|p| p.builder.is_some()) {
            return Err(Error::InvalidArgument {
                what: "a retained local operation before runtime preparation",
            });
        }
        self.no_worker_failure()?;
        self.trunk.validate_worker_preparation()
    }

    pub(crate) fn retirement_capacity(&self) -> usize {
        self.trunk.pool_capacity()
    }

    pub(crate) fn adopt_retirement(
        &mut self,
        lease: hyper_rt::runtime::RetirementLease,
    ) -> Result<(), Error> {
        self.trunk
            .pool_mut()
            .ok_or(Error::InvalidArgument {
                what: "native retirement without maintenance workers",
            })?
            .adopt_retirement(lease)
    }

    pub(crate) fn prepare_original_retirement_watch(
        &mut self,
    ) -> Result<crate::store::OriginalPhysical, Error> {
        self.store.prepare_original_retirement_watch()
    }

    pub(crate) fn adopt_original<W>(
        &mut self,
        adoption: &mut hyper_rt::runtime::OriginalAdoption<F, W>,
        physical: W,
    ) -> Result<(), Error>
    where
        F: Send + 'static,
        W: hyper_rt::runtime::OriginalFence,
    {
        self.store.adopt_original(adoption, physical)
    }

    /// All configured output reservations become ready before the engine leaves cold setup.
    pub(crate) fn prepare_async_backend(&mut self) -> Result<(), Error> {
        self.validate_async_setup()?;
        let pool = self.trunk.pool_mut().ok_or(Error::InvalidArgument {
            what: "runtime preparation without maintenance workers",
        })?;
        match pool.prepare() {
            Ok(()) => Ok(()),
            Err(error) => {
                // No job was admitted. Partial startup is still retired before refusal.
                self.terminal_error.get_or_insert_with(|| error.clone());
                if let Err(retirement) = pool.close_blocking(&mut self.store) {
                    self.terminal_error.get_or_insert(retirement);
                }
                Err(error)
            }
        }
    }

    fn no_worker_failure(&self) -> Result<(), Error> {
        self.frozen
            .iter()
            .find_map(|f| f.error.as_ref())
            .cloned()
            .map_or(Ok(()), Err)
    }

    /// Whether the runtime can perform required I/O through the device and maintenance workers.
    /// Checked before any range is placed on a shard: a missing backend is a setup refusal.
    pub(crate) fn require_async_backend(&self) -> Result<(), Error> {
        if !self.store.has_issuer()
            || !self.trunk.workers_prepared()
            || self.worker_attach.is_none()
        {
            return Err(Error::InvalidArgument {
                what: "a runtime engine without its device issuer and maintenance workers",
            });
        }
        Ok(())
    }

    fn no_checkpoint_mutation(&self) -> Result<(), Error> {
        if self.stopping {
            return Err(Error::InvalidArgument {
                what: "a mutation after shard finish began",
            });
        }
        if self.checkpoint.is_some() {
            return Err(Error::InvalidArgument {
                what: "a mutation during a retained checkpoint",
            });
        }
        Ok(())
    }

    /// A range's checkpoint advances only on its own receipts. The serialized trunk and
    /// allocator stay unchanged between Save and the second flush's commit.
    pub(crate) fn checkpoint_paced(&mut self, applied: u64, keys: u64) -> Result<bool, Error> {
        self.require_async_backend()?;
        if let Some(checkpoint) = &self.checkpoint {
            if checkpoint.applied != applied {
                return Err(Error::InvalidArgument {
                    what: "a different index while a checkpoint is retained",
                });
            }
            if let Some(error) = &checkpoint.error {
                return Err(error.clone());
            }
        } else {
            self.checkpoint = Some(Checkpoint {
                applied,
                phase: CheckpointPhase::Flush,
                error: None,
            });
        }
        let result = self.advance_checkpoint(keys);
        if let Err(error) = &result
            && let Some(checkpoint) = self.checkpoint.as_mut()
        {
            checkpoint.error.get_or_insert_with(|| error.clone());
        }
        if matches!(result, Ok(true)) {
            self.checkpoint = None;
        }
        result
    }

    fn advance_checkpoint(&mut self, keys: u64) -> Result<bool, Error> {
        let checkpoint = self.checkpoint.as_ref().ok_or(Error::InvalidArgument {
            what: "a checkpoint continuation without its operation",
        })?;
        let (applied, phase) = (checkpoint.applied, checkpoint.phase);
        let next = match phase {
            CheckpointPhase::Flush => {
                if !self.flush_paced(keys)? {
                    return Ok(false);
                }
                CheckpointPhase::Save
            }
            CheckpointPhase::Save => {
                let Some(head) = self.trunk.save_paced(&mut self.store, keys)? else {
                    return Ok(false);
                };
                CheckpointPhase::Metadata(head)
            }
            CheckpointPhase::Metadata(head) => {
                return self.store.checkpoint_paced(Some(head), applied, keys);
            }
        };
        self.checkpoint
            .as_mut()
            .ok_or(Error::InvalidArgument {
                what: "a checkpoint lost while advancing its phase",
            })?
            .phase = next;
        Ok(false)
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
        for &e in self.trunk.view_extents() {
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
    /// Sets the entries the active memtable leaves unsorted at most: what a scan after a burst
    /// of puts may sort before it reads, against the merge work more, smaller chunks cost puts.
    pub fn set_order_bound(&mut self, entries: usize) {
        self.order_bound = entries.max(1);
    }

    /// Runs the trunk's compactions on maintenance workers, each over its own handle on the
    /// shard's file that `open` gives (the file opened again as the shard's was): as many as
    /// the measured need asks, up to the cores the OS reports less this thread (`trunk::pool`).
    /// Puts then pay only for taking the workers' results, and wait on them only when a
    /// rotation finds the trunk's cascade still out. Refused when the workers' channel back
    /// cannot be made (`Pool::new`).
    pub fn set_workers(
        &mut self,
        mut open: impl FnMut() -> Result<F, Error> + Send + 'static,
    ) -> Result<(), Error>
    where
        F: Send + 'static,
    {
        if self.trunk.has_pool() {
            return Err(Error::InvalidArgument {
                what: "maintenance workers already configured",
            });
        }
        let per_worker = self
            .store
            .write_reservation_for(self.worker_attach.as_ref().map(|(_, n)| *n))?;
        let mut most = Pool::cores();
        if let Some(memory) = self.memory {
            let (cache, _, records) = self.memory_held();
            let used = self
                .owner_write_reserved
                .checked_add(self.queue_reservation())
                .and_then(|n| n.checked_add(cache))
                .and_then(|n| n.checked_add(records))
                .ok_or_else(|| Self::write_limit(memory))?;
            most = memory
                .checked_sub(used)
                .and_then(|n| n.checked_div(per_worker))
                .unwrap_or(0)
                .min(most);
            if most == 0 {
                return Err(Self::write_limit(memory));
            }
        }
        let reserved = per_worker
            .checked_mul(most)
            .and_then(|n| n.checked_add(self.owner_write_reserved))
            .ok_or_else(|| Self::write_limit(usize::MAX))?;
        self.check_write_reservation(reserved)?;
        let config = self.store.config();
        let spawn: Spawn = Box::new(move |seat| {
            let file = open()?;
            std::thread::Builder::new()
                .name(format!("mantle-maint-{}", seat.id))
                .spawn(move || pool::serve(file, config, seat))
                .map_err(|e| Error::Io {
                    op: "start a maintenance worker",
                    detail: e.to_string(),
                })
        });
        let mut pool = Pool::new(spawn, most)?;
        if let Some((attacher, batches)) = &self.worker_attach {
            pool.set_attach(attacher.clone(), *batches);
        }
        self.trunk.set_pool(pool);
        self.write_reserved = reserved;
        self.partition_memory();
        Ok(())
    }

    /// Warm maintenance seats held and active workers wanted, when the shard has any.
    pub fn workers(&self) -> Option<(usize, usize)> {
        self.trunk.pool_workers()
    }

    pub fn set_timed(&mut self, on: bool) {
        self.timed = on;
        self.trunk.set_timed(on);
        self.store.set_timed(on);
    }

    /// When seeks consolidate the trunk's pivots (`Trunk::set_consolidation`).
    pub fn set_consolidation(&mut self, choice: crate::trunk::Consolidation) {
        self.trunk.set_consolidation(choice);
    }

    /// How the trunk makes a bundle's view when it can rebuild it (`Trunk::set_view_choice`).
    pub fn set_view_choice(&mut self, choice: crate::trunk::ViewChoice) {
        self.trunk.set_view_choice(choice);
    }

    /// Has branches built from here keep `suffix_bits` a key in their range filters
    /// (`Store::set_range_filter`).
    pub fn set_range_filter(&mut self, suffix_bits: u32) -> Result<(), Error> {
        self.store.set_range_filter(suffix_bits)
    }

    /// Spares the shard at most `bytes` of write memory for runs waiting for the device's
    /// issuer (`Store::set_write_budget`). The store is given, from each memtable on, the
    /// lesser of that and the bytes the last memtable's cycle wrote: paced maintenance pays a
    /// cycle's debt before the next memtable rotates, so a queue longer than a cycle's writes
    /// could not spare a put a wait, only hold memory while the device is short.
    pub fn set_write_budget(&mut self, bytes: usize) -> Result<(), Error> {
        let run = self.store.write_reservation_for(None)?;
        let queued = bytes
            .checked_div(run)
            .and_then(|n| n.checked_mul(run))
            .ok_or_else(|| Self::write_limit(usize::MAX))?;
        let total = self
            .write_reserved
            .checked_add(queued.max(self.store.warm_write_bytes()))
            .ok_or_else(|| Self::write_limit(usize::MAX))?;
        if let Some(memory) = self.memory
            && total > memory
        {
            return Err(Self::write_limit(memory));
        }
        self.write_cap = bytes;
        self.store.set_write_budget(bytes);
        if self.memory.is_some() {
            self.write_share = queued.max(self.store.warm_write_bytes());
            self.partition_memory();
        }
        Ok(())
    }

    /// The write memory the store has now ([`Self::set_write_budget`]).
    pub fn write_budget(&self) -> usize {
        self.store.write_budget()
    }

    /// Gives the shard `bytes` of memory for its page cache and its write memory together,
    /// fixed output reservations charged first, then divided between cache, queued writes and
    /// records by measured marginal gains ([`tune`]; research/36). Refused unchanged when the
    /// owner, admitted worker capacity and retained output buffers cannot fit. Other Builder
    /// working runs, input spans, point/page buffers, memtables, branch metadata and packing
    /// feeds are separate, as are attachment/channel metadata and worker stacks.
    pub fn set_memory(&mut self, bytes: usize) -> Result<(), Error> {
        let needed = self
            .write_reserved
            .checked_add(self.store.warm_write_bytes())
            .ok_or_else(|| Self::write_limit(usize::MAX))?;
        let available = bytes
            .checked_sub(self.write_reserved)
            .ok_or_else(|| Self::write_limit(bytes))?;
        if bytes < needed {
            return Err(Self::write_limit(bytes));
        }
        // Validate before changing any share or cache: held output buffers cannot be reclaimed.
        self.memory = Some(bytes);
        self.write_share = self.store.warm_write_bytes();
        self.write_cap = available;
        self.store.set_write_budget(self.write_share);
        let remaining = available.saturating_sub(self.write_share);
        self.records_share = (available.saturating_mul(TUNE_STEP_PERCENT) / 100).min(remaining);
        self.records = Some(crate::records::RecordCache::new(
            self.records_share,
            self.store.page_size(),
        ));
        self.cache_share = remaining.saturating_sub(self.records_share);
        self.store.resize_cache(
            self.cache_share
                .checked_div(self.store.page_size())
                .unwrap_or(0),
        );
        self.tuned = self.store.io_stats();
        (
            self.record_misses,
            self.record_miss_ns,
            self.record_ghost_hits,
        ) = (0, 0, 0);
        self.partition_memory();
        Ok(())
    }

    fn write_limit(bytes: usize) -> Error {
        Error::LimitExceeded {
            what: "reserved write buffers within the memory budget",
            limit: u64::try_from(bytes).unwrap_or(u64::MAX),
        }
    }

    fn queue_reservation(&self) -> usize {
        let queue = if self.memory.is_some() {
            self.write_share
        } else {
            self.store.write_budget()
        };
        queue.max(self.store.warm_write_bytes())
    }

    /// Current physically held cache regions remain charged while a new fixed reservation is
    /// admitted. Setup does not borrow credit from a cache that has not returned its buffers.
    fn check_write_reservation(&self, fixed: usize) -> Result<(), Error> {
        let write = fixed
            .checked_add(self.queue_reservation())
            .ok_or_else(|| Self::write_limit(usize::MAX))?;
        if let Some(memory) = self.memory {
            let (cache, _, records) = self.memory_held();
            let total = write
                .checked_add(cache)
                .and_then(|n| n.checked_add(records))
                .ok_or_else(|| Self::write_limit(memory))?;
            if total > memory {
                return Err(Self::write_limit(memory));
            }
        }
        Ok(())
    }

    /// Only the bytes beyond fixed output reservations move between cache, queue and records.
    fn partition_memory(&mut self) {
        let Some(memory) = self.memory else {
            return;
        };
        // All reservation changes validated this subtraction before mutating the shard.
        let available = memory.saturating_sub(self.write_reserved);
        self.write_share = self.write_share.max(self.store.warm_write_bytes());
        self.records_share = self
            .records_share
            .min(available.saturating_sub(self.write_share));
        self.cache_share = available
            .saturating_sub(self.write_share)
            .saturating_sub(self.records_share);
        self.grant();
        let half = usize::try_from(self.cycle_ops / 2)
            .unwrap_or(usize::MAX)
            .max(1);
        self.trim_cache = self.store.cache_over().div_ceil(half);
        self.trim_records = self
            .records
            .as_ref()
            .map_or(0, crate::records::RecordCache::over)
            .div_ceil(half);
    }

    /// The bytes the memtables hold: the active one, the one packing and the spare kept for
    /// the next rotation, each as [`HashMem::memory`] counts it. They sit beside the budget
    /// [`Self::set_memory`] divides, as RocksDB's write buffers sit beside its block cache.
    pub fn memtable_bytes(&self) -> usize {
        self.mem
            .memory()
            .saturating_add(self.packing.as_ref().map_or(0, |p| p.mem.memory()))
            .saturating_add(
                self.frozen
                    .iter()
                    .map(|f| f.mem.memory())
                    .fold(0, usize::saturating_add),
            )
            .saturating_add(self.spare.as_ref().map_or(0, HashMem::memory))
    }

    /// The cache capacity, scoped write reservation and record capacity, in bytes. Writes
    /// include fixed owner/worker roles and retained queued buffers, not only queue admission.
    pub fn memory_split(&self) -> (usize, usize, usize) {
        let (pages, _) = self.store.cache_pages();
        (
            pages.saturating_mul(self.store.page_size()),
            self.write_reserved.saturating_add(self.queue_reservation()),
            self.records
                .as_ref()
                .map_or(0, crate::records::RecordCache::bytes),
        )
    }

    /// The memory the page cache and record cache hold now, with the index and ghost bytes
    /// their shares were last charged, and scoped write reservations, in bytes: within the budget as
    /// memory moves (`grant`). Indexes are charged at each grant, so a cache's data never takes
    /// what its index needs; between grants its index's own size is `index_bytes`.
    pub fn memory_held(&self) -> (usize, usize, usize) {
        let (cache_index, records_index) = self.charged;
        (
            self.store.cache_bytes().saturating_add(cache_index),
            self.write_reserved.saturating_add(self.queue_reservation()),
            self.records
                .as_ref()
                .map_or(0, crate::records::RecordCache::held_bytes)
                .saturating_add(records_index),
        )
    }

    /// Operations in a tuning cycle: a memtable's worth.
    pub fn cycle_ops(&self) -> u64 {
        self.cycle_ops
    }

    /// The trunk's filters as gets use them, against Monkey's allocation of the same bits
    /// (research/39).
    pub fn filter_plan(&self) -> crate::trunk::FilterPlan {
        self.trunk.filter_plan()
    }

    /// Whether a region is still being brought down to a smaller share (`trim`): until it is,
    /// the regions may hold more than the budget by what it has left to give back.
    pub fn trimming(&self) -> bool {
        self.trim_cache > 0 || self.trim_records > 0
    }

    /// The bytes the page cache's and record cache's indexes and ghosts take.
    pub fn index_bytes(&self) -> (usize, usize) {
        (
            self.store.cache_index_bytes(),
            self.records
                .as_ref()
                .map_or(0, crate::records::RecordCache::index_bytes),
        )
    }

    /// At a rotation: the store's write budget from the cycle just ended, and with a memory
    /// budget, the cache and write memory divided again by what each saved per byte in it.
    fn rebudget(&mut self) {
        let io = self.store.io_stats();
        let pages = io.pages_written.saturating_sub(self.cycle_pages);
        self.cycle_pages = io.pages_written;
        let page = self.store.page_size();
        let written = usize::try_from(pages)
            .unwrap_or(usize::MAX)
            .saturating_mul(page);
        self.cycle_written = written;
        // A cycle is a memtable's worth of operations, puts or gets: a phase of reads alone
        // is divided again as often as one of writes.
        self.cycle_ops = u64::try_from(self.mem.len()).unwrap_or(u64::MAX).max(1);
        self.ops_since = 0;
        if self.memory.is_none() {
            self.store.set_write_budget(written.min(self.write_cap));
            return;
        }
        self.retune();
    }

    /// Counts a get toward the tuning cycle, and divides the memory again at its end.
    fn count_get(&mut self) {
        if self.memory.is_none() {
            return;
        }
        self.trim();
        self.ops_since = self.ops_since.saturating_add(1);
        if self.ops_since >= self.cycle_ops {
            self.ops_since = 0;
            self.retune();
        }
    }

    /// Divides the memory budget again by what each region saved a byte since the last
    /// division (research/36), write memory held within the last write cycle's writes.
    fn retune(&mut self) {
        let Some(total) = self.memory else { return };
        let memory = total.saturating_sub(self.write_reserved);
        let io = self.store.io_stats();
        let page = self.store.page_size();
        let written = self.cycle_written;
        let last = std::mem::replace(&mut self.tuned, io);
        let reads = io.device_reads.saturating_sub(last.device_reads);
        let miss_ns = io
            .device_read_ns
            .saturating_sub(last.device_read_ns)
            .checked_div(reads)
            .unwrap_or(0);
        let (_, ghost_pages) = self.store.cache_pages();
        let read = Gain {
            saved_ns: io
                .ghost_misses
                .saturating_sub(last.ghost_misses)
                .saturating_mul(miss_ns),
            bytes: ghost_pages.saturating_mul(page),
        };
        let write = Gain {
            saved_ns: io.budget_wait_ns.saturating_sub(last.budget_wait_ns),
            bytes: self.write_share.max(self.store.run_bytes()),
        };
        // A record-cache ghost hit saves a trunk lookup, at the mean the cycle measured.
        let ghost_hits = self
            .records
            .as_ref()
            .map_or(0, crate::records::RecordCache::ghost_hits);
        let record_ghost = ghost_hits.saturating_sub(self.record_ghost_hits);
        self.record_ghost_hits = ghost_hits;
        let lookup_ns = self
            .record_miss_ns
            .checked_div(self.record_misses)
            .unwrap_or(0);
        (self.record_misses, self.record_miss_ns) = (0, 0);
        let records = Gain {
            saved_ns: record_ghost.saturating_mul(lookup_ns),
            bytes: self.records_share,
        };
        // Write memory the cycle did not use goes back at once: a share above the most queued
        // spared no wait, and the queue takes buffers only as runs wait.
        let warm = self.store.warm_write_bytes();
        self.write_share = self.write_share.min(self.store.take_queue_peak()).max(warm);
        let cache = memory
            .saturating_sub(self.write_share)
            .saturating_sub(self.records_share);
        let [cache, write_share, records_share] = tune3(
            memory,
            [cache, self.write_share, self.records_share],
            written,
            [read, write, records],
            &mut self.newton,
        );
        // Warm output buffers remain owned even when admission shrinks: the retained floor
        // cannot be reassigned to a cache before the store retires.
        let extra = warm.saturating_sub(write_share);
        let write_share = write_share.max(warm);
        let records_share = records_share.saturating_sub(extra.saturating_sub(cache));
        let cache = cache.saturating_sub(extra);
        // The record cache keeps one step: with none, its ghost would gather no evidence that
        // it should grow.
        let floor = (memory.saturating_mul(TUNE_STEP_PERCENT) / 100)
            .min(memory.saturating_sub(write_share));
        let (cache, records_share) = if records_share < floor {
            (
                cache.saturating_sub(floor.saturating_sub(records_share)),
                floor,
            )
        } else {
            (cache, records_share)
        };
        self.write_share = write_share;
        self.store.set_write_budget(write_share.min(self.write_cap));
        self.cache_share = cache;
        self.records_share = records_share;
        self.grant();
        // What a shrunk region holds above its size leaves a few pages an operation, over half
        // a cycle, so no operation pays for the step and the memory moves within the cycle.
        let half = usize::try_from(self.cycle_ops / 2)
            .unwrap_or(usize::MAX)
            .max(1);
        let per_op = |over: usize| over.div_ceil(half);
        self.trim_cache = per_op(self.store.cache_over());
        self.trim_records = per_op(
            self.records
                .as_ref()
                .map_or(0, crate::records::RecordCache::over),
        );
    }

    /// Sets the page cache's and the record cache's limits to their shares, each less the index
    /// and ghost bytes it pays for, at the ratio of index to data measured now. A region whose
    /// limit fell is brought down to it by `trim`, a few pages an operation, which outpaces a
    /// growing region's filling (a page or a record a miss): memory moving between them never
    /// holds more than it did when the step began.
    fn grant(&mut self) {
        if self.memory.is_none() {
            return;
        }
        let page = self.store.page_size().max(1);
        let net = |share: usize, data: usize, index: usize| -> usize {
            let total = u128::try_from(data.saturating_add(index)).unwrap_or(u128::MAX);
            if data == 0 || total == 0 {
                return share.saturating_sub(index);
            }
            let share = u128::try_from(share).unwrap_or(u128::MAX);
            let data = u128::try_from(data).unwrap_or(u128::MAX);
            usize::try_from(share.saturating_mul(data).checked_div(total).unwrap_or(0))
                .unwrap_or(usize::MAX)
        };
        let (cache_index, records_index) = self.index_bytes();
        self.charged = (cache_index, records_index);
        let cache = net(self.cache_share, self.store.cache_bytes(), cache_index);
        self.store
            .resize_cache(cache.checked_div(page).unwrap_or(0));
        if let Some(r) = self.records.as_mut() {
            let records = net(self.records_share, r.held_bytes(), records_index);
            r.resize(records);
        }
    }

    /// An operation's share of bringing shrunk regions down to their size.
    fn trim(&mut self) {
        if self.trim_cache > 0 && !self.store.trim_cache(self.trim_cache) {
            self.trim_cache = 0;
        }
        if self.trim_records > 0
            && !self
                .records
                .as_mut()
                .is_some_and(|r| r.trim(self.trim_records))
        {
            self.trim_records = 0;
        }
    }

    /// Gives point reads a cache of `bytes` of hot records (`records::RecordCache`), replacing
    /// any it had; none at 0.
    pub fn set_record_cache(&mut self, bytes: usize) {
        self.records =
            (bytes > 0).then(|| crate::records::RecordCache::new(bytes, self.store.page_size()));
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
        if self.trunk.pool_workers().is_some_and(|(held, _)| held != 0) {
            return Err(Error::InvalidArgument {
                what: "changing device attachment after maintenance workers started",
            });
        }
        let owner = self
            .owner_write_reserved
            .max(self.store.write_reservation_for(Some(batches))?);
        let old_workers = self
            .write_reserved
            .checked_sub(self.owner_write_reserved)
            .ok_or(Error::InvalidArgument {
                what: "write reservation state",
            })?;
        let per = self.store.write_reservation_for(Some(issuer.depth()))?;
        let workers = self
            .trunk
            .pool_mut()
            .map_or(Ok(0), |pool| {
                per.checked_mul(pool.capacity())
                    .ok_or_else(|| Self::write_limit(usize::MAX))
            })?
            .max(old_workers);
        let reserved = owner
            .checked_add(workers)
            .ok_or_else(|| Self::write_limit(usize::MAX))?;
        self.check_write_reservation(reserved)?;
        self.store.attach(issuer, batches)?;
        // The maintenance workers' stores attach too, each on its own thread, with as many
        // batches out as the device serves at once (`Issuer::depth`, from the queue the OS
        // reports and the depth calibration measured throughput to stop growing at): fewer
        // serialize a worker behind its slowest write, and under a loaded kernel's buffered
        // writes two out held a worker long enough for a rotation to wait on its feed (stalls of
        // 0.23-1.09 s against 5-51 ms with the depth, benchmark-results/
        // mantle-depth-stall-repro-20261009); more cannot be in flight. A worker then holds at
        // most that many runs in write memory. The depth is one at least (`Issuer::start_for`).
        let worker = (issuer.attacher(), issuer.depth());
        if let Some(pool) = self.trunk.pool_mut() {
            pool.set_attach(worker.0.clone(), worker.1);
        }
        self.worker_attach = Some(worker);
        self.owner_write_reserved = owner;
        self.write_reserved = reserved;
        self.partition_memory();
        Ok(())
    }

    /// Waits for every write handed to the device's issuer to land (`Store::drain`).
    pub fn land(&mut self) -> Result<(), Error> {
        self.store.drain()
    }

    /// A terminal path must close every partial worker input before stopping its pool. Both
    /// synchronous and paced cleanup retain the inputs/grants until physical quiescence.
    fn close_worker_inputs(&mut self) {
        if let Err(error) = self.no_worker_failure() {
            self.terminal_error.get_or_insert(error);
        }
        if !self.stopping {
            self.stopping = true;
            if let Some(feed) = self.packing.as_mut().and_then(|p| p.feed.as_mut()) {
                feed.full = None;
            }
            for feed in self.frozen.iter_mut().filter_map(|f| f.feed.as_mut()) {
                feed.full = None;
            }
            if let Some(pool) = self.trunk.pool_mut() {
                for back in self.frozen.iter_mut().filter_map(|f| f.back.as_mut()) {
                    if let Some(error) = back.physical_error.take() {
                        pool.note_close_error(error);
                    }
                }
            }
        }
    }

    /// Ends unfinished worker inputs without publishing partial branches. Grants and input
    /// buffers stay owned until all worker writes drain and their return channel closes.
    /// The active memtable is not flushed: stopping retains the last checkpoint's contract.
    pub(crate) fn stop_workers_paced(&mut self) -> Result<bool, Error> {
        self.close_worker_inputs();
        let Some(pool) = self.trunk.pool_mut() else {
            return Ok(true);
        };
        let closed = pool.close_paced(&mut self.store);
        if matches!(closed, Ok(false)) {
            return closed;
        }
        // EOF ends each Store/attachment, and close_paced then awaited the native
        // retirement receipt. Every thread and its TLS destructors have ended here.
        self.trunk.stop_workers();
        closed
    }

    /// A shard's terminal cleanup, driven by its runtime task. A canceled borrowed wait
    /// keeps the first error and every unfinished owner in the engine for the next wait.
    /// The result follows worker EOF and native retirement, owner drain and device-attachment retirement.
    pub async fn finish_async(&mut self) -> Result<(), Error> {
        let mut finish = std::pin::pin!(self.finish_inner());
        std::future::poll_fn(|cx| {
            let current = hyper_rt::futures::current_task()
                .is_some_and(|task| hyper_rt::waker::waker_for(task.0).will_wake(cx.waker()));
            if !current {
                return std::task::Poll::Ready(Err(Error::InvalidArgument {
                    what: "an async shard finish polled outside its runtime task",
                }));
            }
            std::future::Future::poll(finish.as_mut(), cx)
        })
        .await
    }

    async fn finish_inner(&mut self) -> Result<(), Error> {
        if self
            .trunk
            .pool_mut()
            .is_some_and(|pool| pool.workers() != 0 && !pool.retirement_owned())
        {
            return Err(Error::InvalidArgument {
                what: "runtime finish without adopted native worker retirement",
            });
        }
        self.trunk.abandon_maplet(&mut self.store);
        self.trunk.abandon_view(&mut self.store);
        if let Some(error) = self.checkpoint.as_ref().and_then(|c| c.error.as_ref()) {
            self.terminal_error.get_or_insert_with(|| error.clone());
        }
        while !self.terminal_done {
            loop {
                match self.stop_workers_paced() {
                    Ok(true) => break,
                    Err(error) => {
                        // Pool close reports its error only after EOF; it was dropped by
                        // stop_workers_paced, so no unfinished worker can reuse a grant.
                        self.terminal_error.get_or_insert(error);
                        break;
                    }
                    Ok(false) => {
                        if let Err(error) = self.wait_completion().await {
                            self.terminal_error.get_or_insert(error);
                        }
                    }
                }
            }
            loop {
                match self.store.drain_paced() {
                    Ok(true) => break,
                    Err(error) => {
                        // The Store preserves its first failure until every issued transfer
                        // has retired; detach must still run before this error is returned.
                        self.terminal_error.get_or_insert(error);
                        break;
                    }
                    Ok(false) => {
                        if self.store.io_outstanding() {
                            if let Err(error) = self.store.wait_completion().await {
                                self.terminal_error.get_or_insert(error);
                            }
                        } else {
                            hyper_rt::futures::yield_now().await;
                        }
                    }
                }
            }
            // An interrupted checkpoint's unsubmitted pages and new metadata remain
            // owned until the accepted writes above have retired. Abandoning it never
            // publishes durability or permits a late write to a reused extent.
            loop {
                match self.store.abort_checkpoint_paced() {
                    Ok(true) => break,
                    Err(error) => {
                        self.terminal_error.get_or_insert(error);
                        break;
                    }
                    Ok(false) => {
                        if self.store.io_outstanding() {
                            if let Err(error) = self.store.wait_completion().await {
                                self.terminal_error.get_or_insert(error);
                            }
                        } else {
                            hyper_rt::futures::yield_now().await;
                        }
                    }
                }
            }
            if let Err(error) = self.trunk.abort_save(&mut self.store) {
                self.terminal_error.get_or_insert(error);
            }
            if let Err(error) = self.store.finish_async().await {
                if !self.store.finished() {
                    return Err(error);
                }
                self.terminal_error.get_or_insert(error);
            }
            self.checkpoint = None;
            self.terminal_done = true;
        }
        self.terminal_error.clone().map_or(Ok(()), Err)
    }

    /// The actor retains no native file after this terminal check.
    pub(crate) fn actor_finished(&self) -> bool {
        self.terminal_done
            && self.store.finished()
            && self.store.original_retired()
            && self.trunk.pool_workers().is_none()
    }

    /// The store's file after worker retirement, and whether every submitted device write
    /// landed. Unfinished packing and the active memtable are not flushed; recovery keeps
    /// the last checkpoint's contract (`Store::into_file`).
    /// No device operation, receipt wait or native join occurs after this check.
    pub(crate) fn into_file_finished(mut self) -> IntoFile<Self, F> {
        if !self.terminal_done || !self.store.finished() || self.trunk.pool_workers().is_some() {
            return IntoFile::Refused {
                owner: self,
                error: Error::InvalidArgument {
                    what: "file extraction before shard finish",
                },
            };
        }
        match self.store.into_file_finished() {
            IntoFile::Finished { file, result } => IntoFile::Finished {
                file,
                result: self.terminal_error.take().map_or(result, Err),
            },
            IntoFile::Refused { owner, error } => {
                self.store = owner;
                IntoFile::Refused { owner: self, error }
            }
        }
    }

    pub fn into_file(mut self) -> IntoFile<Self, F> {
        if self.terminal_done {
            return self.into_file_finished();
        }
        if hyper_rt::registry::current_shard().is_some() {
            return IntoFile::Refused {
                owner: self,
                error: Error::InvalidArgument {
                    what: "unfinished shard consumed by a runtime",
                },
            };
        }
        self.close_worker_inputs();
        let mut first = self.terminal_error.take();
        if let Some(pool) = self.trunk.pool_mut()
            && let Err(error) = pool.close_blocking(&mut self.store)
        {
            first.get_or_insert(error);
        }
        self.trunk.stop_workers();
        match self.store.into_file() {
            IntoFile::Finished { file, result } => IntoFile::Finished {
                file,
                result: first.map_or(result, Err),
            },
            IntoFile::Refused { owner, error } => {
                self.store = owner;
                self.terminal_error = first;
                IntoFile::Refused { owner: self, error }
            }
        }
    }

    fn apply(&mut self, key: &[u8], op: Op, value: &[u8]) -> Result<(), Error> {
        self.no_worker_failure()?;
        self.no_checkpoint_mutation()?;
        if self.store.fenced() {
            return Err(Error::Io {
                op: "apply a shard entry",
                detail: "the store was fenced by a failed write or flush".into(),
            });
        }
        self.trim();
        // A write makes the key's cached record stale: it goes before the write is answered.
        if let Some(r) = self.records.as_mut() {
            r.invalidate(key, crate::branch::filter::hash(key));
        }
        let mut before = self.mem.bytes();
        let t = self.timed.then(std::time::Instant::now);
        match self.mem.insert(key, op, value) {
            Err(Error::LimitExceeded { .. }) if !self.mem.is_empty() => {
                let r = self.timed.then(std::time::Instant::now);
                self.rotate()?;
                let ns = ns_since(r);
                self.flush_stats.rotate_ns = self.flush_stats.rotate_ns.saturating_add(ns);
                before = self.mem.bytes();
                self.mem.insert(key, op, value)?;
            }
            other => other?,
        }
        let ns = ns_since(t);
        self.flush_stats.insert_ns = self.flush_stats.insert_ns.saturating_add(ns);
        self.pace(self.mem.bytes().saturating_sub(before), false)
    }

    /// A range's owned request remains unapplied when its rotation stops for work still out.
    /// HashMem's capacity refusal changes nothing; record invalidation follows insertion so a
    /// yielded request leaves the previous value and its cache entry reachable.
    fn apply_paced(&mut self, key: &[u8], op: Op, value: &[u8], keys: u64) -> Result<bool, Error> {
        self.no_worker_failure()?;
        self.no_checkpoint_mutation()?;
        self.require_async_backend()?;
        if self.store.fenced() {
            return Err(Error::Io {
                op: "apply a shard entry",
                detail: "the store was fenced by a failed write or flush".into(),
            });
        }
        self.trunk.clear_io_wait();
        if self.rotation.is_some() && !self.rotate_paced(keys)? {
            return Ok(false);
        }
        let mut before = self.mem.bytes();
        match self.mem.insert(key, op, value) {
            Err(Error::LimitExceeded { .. }) if !self.mem.is_empty() => {
                if !self.rotate_paced(keys)? {
                    return Ok(false);
                }
                before = self.mem.bytes();
                self.mem.insert(key, op, value)?;
            }
            other => other?,
        }
        self.trim();
        if let Some(r) = self.records.as_mut() {
            r.invalidate(key, crate::branch::filter::hash(key));
        }
        self.pace(self.mem.bytes().saturating_sub(before), true)?;
        Ok(true)
    }

    pub(crate) fn put_paced(&mut self, key: &[u8], value: &[u8], keys: u64) -> Result<bool, Error> {
        self.apply_paced(key, Op::Put, value, keys)
    }

    pub(crate) fn delete_paced(&mut self, key: &[u8], keys: u64) -> Result<bool, Error> {
        self.apply_paced(key, Op::Delete, &[], keys)
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
        self.count_get();
        self.lookup(key, value)
    }

    /// [`Self::get`] made only if it can finish without waiting for the device or for a write
    /// still out over a page it needs: `Some` of its answer, or `None` when a page it needs is
    /// not in memory now, with `value` left empty, for the caller to take the paced path
    /// ([`Self::get_async`]), which counts the get.
    pub(crate) fn get_now(
        &mut self,
        key: &[u8],
        value: &mut Vec<u8>,
    ) -> Result<Option<bool>, Error> {
        self.store.begin_no_wait();
        let found = self.lookup(key, value);
        let refused = self.store.end_no_wait();
        match found {
            Ok(found) => {
                self.count_get();
                Ok(Some(found))
            }
            Err(_) if refused => {
                value.clear();
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    fn lookup(&mut self, key: &[u8], value: &mut Vec<u8>) -> Result<bool, Error> {
        // The key's filter hash, once for the memtables and every branch on its path.
        let hash = crate::branch::filter::hash(key);
        let mut found = self.mem.get_hashed(key, hash, value)?;
        if found.is_none()
            && let Some(p) = &self.packing
        {
            found = p.mem.get_hashed(key, hash, value)?;
        }
        // The frozen memtables, newest first.
        for f in self.frozen.iter().rev() {
            if found.is_some() {
                break;
            }
            found = f.mem.get_hashed(key, hash, value)?;
        }
        if found.is_none()
            && let Some(cached) = self.records.as_mut().and_then(|r| r.get(key, hash))
        {
            value.clear();
            value.extend_from_slice(cached);
            return Ok(true);
        }
        if found.is_none() {
            // A get the record cache missed is timed: its trunk lookup is what a hit saves.
            let t = self.records.as_ref().map(|_| std::time::Instant::now());
            found = self.trunk.get_hashed(&mut self.store, key, hash, value)?;
            if let Some(r) = self.records.as_mut() {
                self.record_misses = self.record_misses.saturating_add(1);
                self.record_miss_ns = self.record_miss_ns.saturating_add(ns_since(t));
                if found == Some(Op::Put) {
                    r.insert(key, hash, value);
                }
            }
        }
        Ok(found == Some(Op::Put))
    }

    /// A runtime lookup keeps its source borrows until the owned request is answered.
    /// Memtable and record hits complete immediately; device demand reads yield.
    pub(crate) async fn get_async(
        &mut self,
        key: &[u8],
        value: &mut Vec<u8>,
    ) -> Result<bool, Error> {
        self.require_async_backend()?;
        self.count_get();
        // The key's filter hash, once for the memtables and every branch on its path.
        let hash = crate::branch::filter::hash(key);
        let mut found = self.mem.get_hashed(key, hash, value)?;
        if found.is_none()
            && let Some(p) = &self.packing
        {
            found = p.mem.get_hashed(key, hash, value)?;
        }
        // The frozen memtables, newest first.
        for f in self.frozen.iter().rev() {
            if found.is_some() {
                break;
            }
            found = f.mem.get_hashed(key, hash, value)?;
        }
        if found.is_none()
            && let Some(cached) = self.records.as_mut().and_then(|r| r.get(key, hash))
        {
            value.clear();
            value.extend_from_slice(cached);
            return Ok(true);
        }
        if found.is_none() {
            // A get the record cache missed is timed: its trunk lookup is what a hit saves.
            let t = self.records.as_ref().map(|_| std::time::Instant::now());
            found = self
                .trunk
                .get_hashed_async(&mut self.store, key, hash, value)
                .await?;
            if let Some(r) = self.records.as_mut() {
                self.record_misses = self.record_misses.saturating_add(1);
                self.record_miss_ns = self.record_miss_ns.saturating_add(ns_since(t));
                if found == Some(Op::Put) {
                    r.insert(key, hash, value);
                }
            }
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
        // A walk of a memtable sees its sorted runs: what arrived since the last scan is sorted
        // into one first, and a packing memtable's sort, if unfinished, is finished.
        self.mem.seal();
        if let Some(p) = self.packing.as_mut() {
            p.mem.seal();
        }
        let mut active = std::mem::replace(&mut self.scan_active, MemCursor::empty());
        // A seek of the runs is timed when they are many: the price of the comparisons they add.
        let (many, one) = self.mem.seek_comparisons();
        let step_extra = self.mem.step_comparisons();
        let t = (many > one).then(std::time::Instant::now);
        active.seek(&self.mem, from)?;
        let seek_ns = ns_since(t);
        let seek_timed = t.is_some() && active.walk.run_work().0 > 0;
        let mut packing = match &self.packing {
            Some(p) => {
                let mut c = std::mem::replace(&mut self.scan_packing, MemCursor::empty());
                c.seek(&p.mem, from)?;
                Some(c)
            }
            None => None,
        };
        // A cursor a frozen memtable, newest first; their order work is paid, so none changes
        // under the scan.
        let mut frozen = std::mem::take(&mut self.scan_frozen);
        frozen.resize_with(self.frozen.len(), MemCursor::empty);
        for (c, f) in frozen.iter_mut().zip(self.frozen.iter().rev()) {
            c.seek(&f.mem, from)?;
        }
        let trunk = &self.trunk;
        let store = &mut self.store;
        let key = &mut self.scan_key;
        // The trunk's leaf segments, found one at a time from the seek's key: a segment's
        // sources (branches, and pivot bundles through their views), and where the next starts.
        let seg_from = &mut self.scan_from;
        let seg_end = &mut self.scan_end;
        seg_from.clear();
        seg_from.extend_from_slice(from);
        let mut sources: Vec<Source<'_>> = reuse(std::mem::take(&mut self.scan_sources));
        let passed = &mut self.scan_passed;
        let charges = &mut self.scan_charges;
        charges.clear();
        let mut trunk_done = false;
        let mut merge: ScanMerge<'_> = std::mem::take(&mut self.scan_merge).recycle(store);
        let before_end = |k: &[u8]| end.is_none_or(|e| k < e);
        let mut taken = 0usize;
        let more = loop {
            // The trunk's next key: from the segment open, or the next segment's first.
            while merge.entry().is_none() {
                if trunk_done || !before_end(seg_from) {
                    break;
                }
                let bounded = trunk.segment_at(seg_from, &mut sources, seg_end, passed)?;
                // Each segment ends past its start, so the walk moves a leaf a step and ends.
                if bounded && seg_end.as_slice() <= seg_from.as_slice() {
                    merge.close(store);
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
                // Sources are timed only when a pivot gave more than its one: what earns rent.
                let spare = passed.iter().any(|p| {
                    let (_, n) = p.sources();
                    if p.leaf() { n > 1 } else { n > 0 }
                });
                merge.open(store, &sources, seg_from, hi, end.is_some(), spare)?;
                // Each pivot's rent: the measured time its sources took to open, all an index
                // pivot gave, all but the dearest of a leaf's, which stands for the one branch a
                // consolidated leaf keeps.
                let open_ns = merge.open_ns();
                charges.extend(passed.iter().map(|&p| {
                    let (first, n) = p.sources();
                    let own = open_ns.get(first..first.saturating_add(n)).unwrap_or(&[]);
                    let sum = own.iter().fold(0u64, |a, &b| a.saturating_add(b));
                    let rent = if p.leaf() {
                        sum.saturating_sub(own.iter().copied().max().unwrap_or(0))
                    } else {
                        sum
                    };
                    (p, rent)
                }));
                if bounded {
                    std::mem::swap(seg_from, seg_end);
                } else {
                    trunk_done = true;
                }
            }
            // The smallest key any source holds next, copied so the sources can move.
            let tree = merge.entry().map(|(k, _, _)| k);
            let mem_a = active.key().filter(|k| before_end(k));
            let mem_p = packing
                .as_ref()
                .and_then(MemCursor::key)
                .filter(|k| before_end(k));
            let mem_f = frozen
                .iter()
                .filter_map(MemCursor::key)
                .filter(|k| before_end(k))
                .min();
            let Some(least) = [mem_a, mem_p, mem_f, tree].into_iter().flatten().min() else {
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
            for (c, f) in frozen.iter_mut().zip(self.frozen.iter().rev()) {
                if c.key() != Some(key.as_slice()) {
                    continue;
                }
                if !decided {
                    decided = true;
                    if c.op == Op::Put {
                        out.push(key, &c.value);
                        taken = taken.saturating_add(1);
                    }
                }
                c.advance(&f.mem)?;
            }
            if let Some((k, op, v)) = merge.entry()
                && k == key.as_slice()
            {
                if !decided && op == Op::Put {
                    out.push(key, v);
                    taken = taken.saturating_add(1);
                }
                merge.next(store)?;
            }
        };
        let (skipped, opened) = merge.counts();
        self.scan_merge = merge.recycle(store);
        self.scan_sources = reuse(sources);
        self.scan_skipped = self.scan_skipped.saturating_add(skipped);
        self.scan_opened = self.scan_opened.saturating_add(opened);
        self.scan_active = active;
        if let Some(c) = packing {
            self.scan_packing = c;
        }
        self.scan_frozen = frozen;
        // What the walks merged from the runs stays merged for the next seek of that range.
        if !self.scan_charges.is_empty() {
            let io = self.store.io_stats();
            let pages = crate::trunk::PageCosts {
                read_ns: io.read_ns.checked_div(io.pages_read).unwrap_or(0),
                write_ns: io.write_ns.checked_div(io.pages_written).unwrap_or(0),
                extent_pages: u64::from(self.store.extent_pages()),
            };
            self.trunk.charge(&self.scan_charges, pages);
        }
        let (run_seeks, run_steps) = self.scan_active.walk.run_work();
        self.mem.adopt(&mut self.scan_active.walk);
        if let Some(p) = self.packing.as_mut() {
            p.mem.adopt(&mut self.scan_packing.walk);
        }
        if many > one {
            let seek_ns = if seek_timed { Some(seek_ns) } else { None };
            self.charge_runs(seek_ns, many, one, run_seeks, run_steps, step_extra);
        }
        Ok(more)
    }

    /// A runtime scan owns its complete page while source borrows yield for demand reads.
    /// The merge guard returns every cursor and its buffers on completion, error or cancel.
    pub(crate) async fn scan_async(
        &mut self,
        from: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        out: &mut Rows,
        next: &mut Vec<u8>,
    ) -> Result<bool, Error> {
        self.require_async_backend()?;
        next.clear();
        if limit == 0 {
            next.extend_from_slice(from);
            return Ok(true);
        }
        // A walk of a memtable sees its sorted runs: what arrived since the last scan is sorted
        // into one first, and a packing memtable's sort, if unfinished, is finished.
        self.mem.seal();
        if let Some(p) = self.packing.as_mut() {
            p.mem.seal();
        }
        let mut active = std::mem::replace(&mut self.scan_active, MemCursor::empty());
        // A seek of the runs is timed when they are many: the price of the comparisons they add.
        let (many, one) = self.mem.seek_comparisons();
        let step_extra = self.mem.step_comparisons();
        let t = (many > one).then(std::time::Instant::now);
        active.seek(&self.mem, from)?;
        let seek_ns = ns_since(t);
        let seek_timed = t.is_some() && active.walk.run_work().0 > 0;
        let mut packing = match &self.packing {
            Some(p) => {
                let mut c = std::mem::replace(&mut self.scan_packing, MemCursor::empty());
                c.seek(&p.mem, from)?;
                Some(c)
            }
            None => None,
        };
        // A cursor a frozen memtable, newest first; their order work is paid, so none changes
        // under the scan.
        let mut frozen = std::mem::take(&mut self.scan_frozen);
        frozen.resize_with(self.frozen.len(), MemCursor::empty);
        for (c, f) in frozen.iter_mut().zip(self.frozen.iter().rev()) {
            c.seek(&f.mem, from)?;
        }
        let trunk = &mut self.trunk;
        let key = &mut self.scan_key;
        // The trunk's leaf segments, found one at a time from the seek's key: a segment's
        // sources (branches, and pivot bundles through their views), and where the next starts.
        let seg_from = &mut self.scan_from;
        let seg_end = &mut self.scan_end;
        seg_from.clear();
        seg_from.extend_from_slice(from);
        let mut sources: Vec<Source<'_>> = reuse(std::mem::take(&mut self.scan_sources));
        let passed = &mut self.scan_passed;
        let charges = &mut self.scan_charges;
        charges.clear();
        let mut trunk_done = false;
        let mut merge = BorrowedScanMerge::new(&mut self.scan_merge, &mut self.store);
        let before_end = |k: &[u8]| end.is_none_or(|e| k < e);
        let mut taken = 0usize;
        let more = loop {
            // The trunk's next key: from the segment open, or the next segment's first.
            while merge.entry().is_none() {
                if trunk_done || !before_end(seg_from) {
                    break;
                }
                let bounded = trunk.segment_at(seg_from, &mut sources, seg_end, passed)?;
                // Each segment ends past its start, so the walk moves a leaf a step and ends.
                if bounded && seg_end.as_slice() <= seg_from.as_slice() {
                    merge.close();
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
                // Sources are timed only when a pivot gave more than its one: what earns rent.
                let spare = passed.iter().any(|p| {
                    let (_, n) = p.sources();
                    if p.leaf() { n > 1 } else { n > 0 }
                });
                merge
                    .open(&sources, seg_from, hi, end.is_some(), spare)
                    .await?;
                // Each pivot's rent: the measured time its sources took to open, all an index
                // pivot gave, all but the dearest of a leaf's, which stands for the one branch a
                // consolidated leaf keeps.
                let open_ns = merge.open_ns();
                charges.extend(passed.iter().map(|&p| {
                    let (first, n) = p.sources();
                    let own = open_ns.get(first..first.saturating_add(n)).unwrap_or(&[]);
                    let sum = own.iter().fold(0u64, |a, &b| a.saturating_add(b));
                    let rent = if p.leaf() {
                        sum.saturating_sub(own.iter().copied().max().unwrap_or(0))
                    } else {
                        sum
                    };
                    (p, rent)
                }));
                if bounded {
                    std::mem::swap(seg_from, seg_end);
                } else {
                    trunk_done = true;
                }
            }
            // The smallest key any source holds next, copied so the sources can move.
            let tree = merge.entry().map(|(k, _, _)| k);
            let mem_a = active.key().filter(|k| before_end(k));
            let mem_p = packing
                .as_ref()
                .and_then(MemCursor::key)
                .filter(|k| before_end(k));
            let mem_f = frozen
                .iter()
                .filter_map(MemCursor::key)
                .filter(|k| before_end(k))
                .min();
            let Some(least) = [mem_a, mem_p, mem_f, tree].into_iter().flatten().min() else {
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
            for (c, f) in frozen.iter_mut().zip(self.frozen.iter().rev()) {
                if c.key() != Some(key.as_slice()) {
                    continue;
                }
                if !decided {
                    decided = true;
                    if c.op == Op::Put {
                        out.push(key, &c.value);
                        taken = taken.saturating_add(1);
                    }
                }
                c.advance(&f.mem)?;
            }
            if let Some((k, op, v)) = merge.entry()
                && k == key.as_slice()
            {
                if !decided && op == Op::Put {
                    out.push(key, v);
                    taken = taken.saturating_add(1);
                }
                merge.next().await?;
            }
        };
        let (skipped, opened) = merge.counts();
        drop(merge);
        self.scan_sources = reuse(sources);
        self.scan_skipped = self.scan_skipped.saturating_add(skipped);
        self.scan_opened = self.scan_opened.saturating_add(opened);
        self.scan_active = active;
        if let Some(c) = packing {
            self.scan_packing = c;
        }
        self.scan_frozen = frozen;
        // What the walks merged from the runs stays merged for the next seek of that range.
        if !self.scan_charges.is_empty() {
            let io = self.store.io_stats();
            let pages = crate::trunk::PageCosts {
                read_ns: io.read_ns.checked_div(io.pages_read).unwrap_or(0),
                write_ns: io.write_ns.checked_div(io.pages_written).unwrap_or(0),
                extent_pages: u64::from(self.store.extent_pages()),
            };
            self.trunk.charge(&self.scan_charges, pages);
        }
        let (run_seeks, run_steps) = self.scan_active.walk.run_work();
        self.mem.adopt(&mut self.scan_active.walk);
        if let Some(p) = self.packing.as_mut() {
            p.mem.adopt(&mut self.scan_packing.walk);
        }
        if many > one {
            let seek_ns = if seek_timed { Some(seek_ns) } else { None };
            self.charge_runs(seek_ns, many, one, run_seeks, run_steps, step_extra);
        }
        Ok(more)
    }

    /// A put that took `bytes` of the memtable does its share of each debt (the module's
    /// pacing).
    fn pace(&mut self, bytes: usize, paced: bool) -> Result<(), Error> {
        let room = self.mem.room();
        // The active memtable sorts its own entries as it fills, in chunks of at most
        // `order_bound` entries paid by the puts that write them, so the one it rotates to is
        // nearly in order and its packing owes the walk alone, and a scan after any burst of
        // puts sorts at most two chunks (HashMem::order_put).
        let t = self.timed.then(std::time::Instant::now);
        self.mem.order_put(1, self.order_bound, room);
        let ns = ns_since(t);
        self.flush_stats.order_ns = self.flush_stats.order_ns.saturating_add(ns);
        let left = self.pack_left();
        if left > 0 {
            // The memtable rotates when a put no longer fits, before its room reaches nothing:
            // packing is paced over the room a put of this size leaves, so it ends first.
            let room = room.saturating_sub(bytes);
            let w = share(left, bytes, room, &mut self.pack_carry);
            self.flush_stats.pack_share_most = self.flush_stats.pack_share_most.max(w);
            self.pack_some(w, false)?;
        } else if !self.frozen.is_empty() {
            self.take_packs(false)?;
        }
        let waiting = self.trunk.fanout().saturating_sub(self.trunk.pending());
        let trunk_room = room.saturating_add(waiting.saturating_mul(self.mem_limit));
        // The cache's freed pages, forgotten over the same room as the trunk's work that freed
        // them.
        let forget = self.store.forget_debt();
        if forget > 0 {
            let w = share(forget, bytes, trunk_room, &mut self.forget_carry);
            if w > 0 {
                let t = self.timed.then(std::time::Instant::now);
                self.store.forget_some(w);
                let ns = ns_since(t);
                self.flush_stats.forget_ns = self.flush_stats.forget_ns.saturating_add(ns);
            }
        }
        let debt = self.trunk.debt();
        if debt > 0 {
            let room = trunk_room;
            let w = share(debt, bytes, room, &mut self.trunk_carry);
            self.flush_stats.trunk_share_most = self.flush_stats.trunk_share_most.max(w);
            if w > 0 {
                let t = self.timed.then(std::time::Instant::now);
                // A compaction may stop for an input read still in flight while a rotation can
                // still pass without a stall: the work it leaves is paid by the puts of the
                // memtables still to fill. Once the next rotation would stall (a packed
                // memtable for every slot the trunk has), nothing is left to absorb it, and
                // each put waits for its reads and pays its share, so a device that cannot
                // keep up slows puts in step rather than leave the whole debt to one rotation.
                let last = self.trunk.pending().saturating_add(1) >= self.trunk.fanout();
                if last && !paced {
                    self.trunk.step(&mut self.store, w)?;
                    self.flush_stats.waited_steps = self.flush_stats.waited_steps.saturating_add(1);
                } else {
                    self.trunk.step_paced(&mut self.store, w)?;
                }
                self.note_trunk(ns_since(t));
            }
        } else if self.trunk.cascading() && self.trunk.has_pool() {
            // The cascade's compactions are out on the workers: a step takes whatever results
            // have come and hands out what they let go, waiting for none.
            if paced {
                self.trunk.step_paced(&mut self.store, 1)?;
            } else {
                self.trunk.step(&mut self.store, 1)?;
            }
        }
        Ok(())
    }

    /// Packs `w` keys' worth of the packing memtable: its order, then entries, then its filter
    /// pages at their worth in keys; the branch goes to the trunk once whole. With workers, the
    /// entries are fed to one and its branch taken once it comes back, waited for when `wait`.
    fn pack_some(&mut self, w: u64, wait: bool) -> Result<(), Error> {
        if w == 0 {
            // No share this put: a worker's branch is taken if it is back, nothing else.
            return self.take_packs(false);
        }
        // The frozen memtables still feeding come first, oldest first.
        let mut w = w;
        if self.frozen.iter().any(|f| f.feed.is_some()) {
            let used = self.feed_from(w, wait, false)?;
            w = w.saturating_sub(used);
            self.take_packs(false)?;
        }
        let Some(p) = &mut self.packing else {
            return Ok(());
        };
        if w == 0 {
            return Ok(());
        }
        let t = self.timed.then(std::time::Instant::now);
        // The memtable's order first: its sort and merges, a slice of the budget.
        if p.mem.debt() > 0 {
            let paid = p.mem.pay(usize::try_from(w).unwrap_or(usize::MAX));
            w = w.saturating_sub(u64::try_from(paid).unwrap_or(u64::MAX));
        }
        if p.mem.debt() == 0 && p.walk.is_none() {
            p.walk = Some(p.mem.walk_start());
        }
        if p.walk.is_some() && p.builder.is_none() && p.feed.is_none() {
            self.start_packing(false)?;
        }
        let fed = self.packing.as_ref().is_some_and(|p| p.feed.is_some());
        if fed {
            self.feed(w, wait)?;
            let whole = self
                .packing
                .as_ref()
                .and_then(|p| p.feed.as_ref())
                .is_some_and(|f| f.full.is_none());
            if whole {
                self.freeze();
            }
            self.note_pack(ns_since(t));
            return self.take_packs(wait && !self.frozen.is_empty());
        }
        if w == 0 {
            return Ok(());
        }
        let Some(p) = &mut self.packing else {
            return Ok(());
        };
        let Packing {
            mem,
            walk,
            builder,
            packed,
            ..
        } = p;
        let Some(builder) = builder.as_mut() else {
            return Ok(());
        };
        let store = &mut self.store;
        let entries = u64::try_from(mem.len().saturating_sub(*packed)).unwrap_or(u64::MAX);
        let walked = if walk.is_some() { w.min(entries) } else { 0 };
        if walked > 0
            && let Some(walk) = walk.as_mut()
        {
            let limit = usize::try_from(walked).unwrap_or(usize::MAX);
            let visited = mem.walk_some_hashed(walk, limit, |k, op, v, h| {
                builder.add_hashed(store, k, op, v, h)
            })?;
            *packed = packed.saturating_add(visited);
        }
        let mut whole = false;
        let over = if walk.is_some() {
            w.saturating_sub(walked)
        } else {
            0
        };
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

    /// The packing memtable's order is paid: its branch goes to a worker, the walked entries fed
    /// to it. A packing not started is a stall the next rotation would take, so it may start a
    /// worker past the measured need, within the cores ([`Pool::can_take`]). With every core's
    /// worker busy a later put tries again, or, when the packing must end now (`now`), the
    /// shard builds it.
    fn start_packing(&mut self, now: bool) -> Result<(), Error> {
        let Some(p) = &mut self.packing else {
            return Ok(());
        };
        let entries = u64::try_from(p.mem.len()).unwrap_or(u64::MAX);
        if let Some(pool) = self.trunk.pool_mut().filter(|pool| pool.can_take(true)) {
            // The grant: the memtable's bytes in extents, and one for the last partial one; a
            // branch that needs more asks for it.
            let run = self.store.run_bytes().max(1);
            let extents = crate::util::div_ceil_usize(p.mem.bytes(), run)
                .unwrap_or(0)
                .saturating_add(1);
            let grant = self.store.grant(extents)?;
            let (full, full_rx) = sync_channel(FEED_BUFFERS);
            let job = Box::new(Job {
                work: Work::Pack(Stream {
                    entries,
                    full: full_rx,
                }),
                grant: grant.clone(),
                file_end: self.store.end(),
                generation: self.store.generation(),
            });
            let sent = match pool.send(job, Owner::Pack, true) {
                Ok(sent) => sent,
                Err((error, job)) => {
                    self.store.grant_back(&job.grant)?;
                    return Err(error);
                }
            };
            match sent {
                Ok(worker) => {
                    let mut spare = std::mem::take(&mut self.feed_buffers);
                    spare.resize_with(FEED_BUFFERS, Vec::new);
                    p.feed = Some(Feed {
                        worker,
                        full: Some(full),
                        filling: spare.pop(),
                        spare,
                        grant,
                    });
                    return Ok(());
                }
                Err(job) => self.store.grant_back(&job.grant)?,
            }
        }
        if now || !self.trunk.has_pool() {
            p.builder = Some(Builder::new(&mut self.store, Keys::Exactly(entries))?);
        } else if let Some(pool) = self.trunk.pool_mut() {
            // Every worker is occupied: a completion frees the slot this packing needs.
            pool.want_message();
        }
        Ok(())
    }

    /// Feeds `w` entries of the frozen memtables still feeding and the packing one: see
    /// [`Self::feed_from`].
    fn feed(&mut self, w: u64, wait: bool) -> Result<u64, Error> {
        self.feed_from(w, wait, true)
    }

    /// Feeds up to `w` entries, the packing memtable too only when `packing_too`: the entries
    /// fed. Every feed whose worker has a buffer free takes entries, the oldest first, so the
    /// workers build at once; one starved never holds the newer back. With `wait`, it feeds
    /// until `w` are fed or none is left to feed, and waits for a worker's message only after a
    /// pass that fed nothing with no message taken since it began: a message may bring back a
    /// buffer of a feed the pass found starved, whose worker, waiting for its feed, sends
    /// nothing more ([`Pool::wait_any`] refuses that wait). A worker's result, a failure's when
    /// it ends before its feed, closes that feed ([`Self::take_packs`]).
    fn feed_from(&mut self, w: u64, wait: bool, packing_too: bool) -> Result<u64, Error> {
        let mut fed = 0u64;
        loop {
            let seen = self.trunk.pool_mut().map(|p| p.accepted());
            let (used, open) = self.feed_pass(w.saturating_sub(fed), packing_too)?;
            fed = fed.saturating_add(used);
            if !wait || !open || fed >= w {
                return Ok(fed);
            }
            if used == 0 {
                self.take_packs(false)?;
                let Self {
                    frozen,
                    packing,
                    trunk,
                    store,
                    ..
                } = self;
                let Some(pool) = trunk.pool_mut() else {
                    return Ok(fed);
                };
                // A message taken since the pass began, by the pass or by the take after it, may
                // have brought a feed's buffer back after the pass found that feed starved: its
                // worker, waiting for the feed, sends nothing more. So the wait is only for a pass
                // that fed nothing with no message since: every open feed's buffers are then out
                // with its worker, which sends one back, or its result, without the shard.
                if Some(pool.accepted()) != seen {
                    continue;
                }
                let open = frozen
                    .iter()
                    .filter_map(|f| f.feed.as_ref().map(|feed| feed.worker))
                    .chain(
                        packing
                            .as_ref()
                            .filter(|_| packing_too)
                            .and_then(|p| p.feed.as_ref().map(|feed| feed.worker)),
                    );
                if !pool.wait_any(store, open)? {
                    return Ok(fed);
                }
            }
        }
    }

    /// One pass over the feeds, oldest first, none waited for: the entries fed, and whether a
    /// feed is still open.
    fn feed_pass(&mut self, w: u64, packing_too: bool) -> Result<(u64, bool), Error> {
        let run = self.store.run_bytes().max(1);
        let mut left = w;
        let mut open = false;
        let Self {
            frozen,
            packing,
            trunk,
            store,
            feed_buffers,
            ..
        } = self;
        let pool = trunk.pool_mut().ok_or(Error::InvalidArgument {
            what: "a packing fed to workers the shard no longer has",
        })?;
        for fz in frozen.iter_mut() {
            if left == 0 {
                return Ok((w, true));
            }
            let (Some(walk), Some(f)) = (fz.walk.as_mut(), fz.feed.as_mut()) else {
                continue;
            };
            let used = feed_one(store, pool, &fz.mem, walk, f, &mut fz.packed, left, run)?;
            left = left.saturating_sub(used);
            if f.full.is_none() {
                // Fed whole: its spare buffers for the next packing; the worker gives back
                // the rest with its result.
                f.spare.extend(f.filling.take());
                feed_buffers.append(&mut f.spare);
                feed_buffers.truncate(FEED_BUFFERS);
                fz.feed = None;
                fz.walk = None;
            } else {
                open = true;
            }
        }
        if left == 0 || !packing_too {
            return Ok((w.saturating_sub(left), open || left == 0));
        }
        let Some(p) = packing.as_mut() else {
            return Ok((w.saturating_sub(left), open));
        };
        let Packing {
            mem,
            walk,
            feed,
            packed,
            ..
        } = p;
        let (Some(walk), Some(f)) = (walk.as_mut(), feed.as_mut()) else {
            return Ok((w.saturating_sub(left), open));
        };
        let used = feed_one(store, pool, mem, walk, f, packed, left, run)?;
        Ok((
            w.saturating_sub(left).saturating_add(used),
            open || f.full.is_some(),
        ))
    }

    /// The packing memtable, handed to its worker, becomes a frozen memtable: read until its
    /// branch is back, fed by later puts if entries are left, while the next memtable's packing
    /// starts.
    fn freeze(&mut self) {
        if !self.packing.as_ref().is_some_and(|p| p.feed.is_some()) {
            return;
        }
        let Some(Packing {
            mem,
            walk,
            feed,
            packed,
            since,
            ..
        }) = self.packing.take()
        else {
            return;
        };
        let Some(mut f) = feed else {
            return;
        };
        let worker = f.worker;
        let grant = std::mem::take(&mut f.grant);
        let (walk, feed) = if f.full.is_none() {
            f.spare.extend(f.filling.take());
            self.feed_buffers.append(&mut f.spare);
            self.feed_buffers.truncate(FEED_BUFFERS);
            (None, None)
        } else {
            (walk, Some(f))
        };
        self.frozen.push_back(Frozen {
            mem,
            worker,
            grant,
            back: None,
            since,
            error: None,
            walk,
            feed,
            packed,
        });
        let held = u64::try_from(self.frozen.len()).unwrap_or(u64::MAX);
        self.flush_stats.frozen_most = self.flush_stats.frozen_most.max(held);
        self.pack_carry = 0;
    }

    /// The entries still to feed: the frozen memtables' and the packing one's, with its order
    /// and filter pages ([`Packing::left`]).
    fn pack_left(&self) -> u64 {
        let frozen = self
            .frozen
            .iter()
            .filter(|f| f.feed.is_some())
            .map(|f| u64::try_from(f.mem.len().saturating_sub(f.packed)).unwrap_or(u64::MAX))
            .fold(0u64, u64::saturating_add);
        self.packing
            .as_ref()
            .map_or(0, Packing::left)
            .saturating_add(frozen)
    }

    /// Takes the packing workers' branches that are back, waiting for one when `wait` and a
    /// memtable is frozen: each goes to its memtable, and those whose older memtables' are all
    /// in go to the trunk as pending, oldest first ([`Self::retire`]).
    fn take_packs(&mut self, wait: bool) -> Result<(), Error> {
        // A wait needs a result on its way: a frozen memtable fed whole whose worker is still
        // building. One still feeding waits on the shard, not the shard on it.
        let mut wait = wait
            && self
                .frozen
                .iter()
                .any(|f| f.feed.is_none() && f.back.is_none() && f.error.is_none());
        loop {
            // Results already in are retired whether or not another comes.
            self.retire_ready()?;
            let Some(pool) = self.trunk.pool_mut() else {
                return Ok(());
            };
            let Some(back) = pool.take(&mut self.store, Owner::Pack, wait)? else {
                if pool.out_for(Owner::Pack)
                    && self
                        .frozen
                        .iter()
                        .any(|f| f.feed.is_none() && f.back.is_none() && f.error.is_none())
                {
                    pool.want_message();
                }
                return Ok(());
            };
            wait = false;
            match self
                .frozen
                .iter_mut()
                .find(|f| f.worker == back.worker && f.back.is_none())
            {
                Some(f) => {
                    // A worker ends before its feed does only by a failure: the feed closes.
                    f.back = Some(back);
                    f.feed = None;
                    f.walk = None;
                }
                None => {
                    // A worker ended before its feed did: only by a failure, which the packing
                    // still feeding owns.
                    let feeding = self
                        .packing
                        .as_ref()
                        .and_then(|p| p.feed.as_ref())
                        .is_some_and(|f| f.worker == back.worker);
                    if !feeding {
                        return Err(Error::InvalidArgument {
                            what: "a packed branch for no packing memtable",
                        });
                    }
                    self.freeze();
                    if let Some(f) = self.frozen.back_mut() {
                        f.back = Some(back);
                        f.feed = None;
                        f.walk = None;
                    }
                }
            }
        }
    }

    /// Retires the frozen memtables at the front that are ready, oldest first.
    fn retire_ready(&mut self) -> Result<(), Error> {
        while self.frozen.front().is_some_and(Frozen::ready) {
            if let Some(f) = self.frozen.pop_front() {
                self.retire(f)?;
            }
        }
        Ok(())
    }

    /// A frozen memtable's branch goes to the trunk as pending, the memtable is kept cleared for
    /// a later fill, and the packing's latency is measured ([`Self::frozen_most`]).
    ///
    /// A worker failure is reported after its physical drain; the memtable remains readable.
    /// No shard-owned Builder repeats the failed job, and nothing younger retires past it.
    fn retire(&mut self, mut f: Frozen) -> Result<(), Error> {
        let retired = (|| {
            if let Some(error) = f.error.clone() {
                return Err(error);
            }
            let back = f.back.as_mut().ok_or(Error::InvalidArgument {
                what: "a frozen input with no worker result",
            })?;
            self.feed_buffers.append(&mut back.buffers);
            self.feed_buffers.truncate(FEED_BUFFERS);
            let failed = back
                .physical_error
                .as_ref()
                .or_else(|| back.result.as_ref().err())
                .cloned();
            if let Some(error) = failed {
                // The first worker/physical error precedes cleanup errors. Failed releases
                // remain named in their original vectors, but are not attempted again.
                let terminal = &mut self.terminal_error;
                terminal.get_or_insert_with(|| error.clone());
                let store = &mut self.store;
                f.grant.retain(|extent| match store.release(*extent) {
                    Ok(()) => false,
                    Err(cleanup) => {
                        terminal.get_or_insert(cleanup);
                        true
                    }
                });
                back.topped.retain(|extent| match store.release(*extent) {
                    Ok(()) => false,
                    Err(cleanup) => {
                        terminal.get_or_insert(cleanup);
                        true
                    }
                });
                return Err(error);
            }
            let out = back.result.as_mut().map_err(|error| error.clone())?;
            let mut cleanup_error = None;
            // Mark each unused extent only after its individual return succeeds, so a
            // partial allocator refusal cannot make an owned extent disappear.
            out.unused.retain(
                |extent| match self.store.grant_back(std::slice::from_ref(extent)) {
                    Ok(()) => {
                        f.grant.retain(|held| held != extent);
                        back.topped.retain(|held| held != extent);
                        false
                    }
                    Err(error) => {
                        cleanup_error.get_or_insert(error);
                        true
                    }
                },
            );
            if let Some(error) = cleanup_error {
                return Err(error);
            }
            if out.parts.len() != 1 {
                return Err(Error::InvalidArgument {
                    what: "a packing worker's result without exactly one branch",
                });
            }
            let (_, branch) = out.parts.pop().ok_or(Error::InvalidArgument {
                what: "a packing worker's result with no branch",
            })?;
            self.store.extend_end(out.end);
            Ok(branch)
        })();
        let branch = match retired {
            Ok(branch) => branch,
            Err(error) => {
                f.error.get_or_insert_with(|| error.clone());
                self.terminal_error.get_or_insert_with(|| error.clone());
                self.frozen.push_front(f);
                return Err(error);
            }
        };
        let Frozen { mut mem, since, .. } = f;
        self.trunk.add(branch);
        self.pack_ns = u64::try_from(
            std::time::Instant::now()
                .saturating_duration_since(since)
                .as_nanos(),
        )
        .unwrap_or(u64::MAX);
        self.flush_stats.fed = self.flush_stats.fed.saturating_add(1);
        let t = self.timed.then(std::time::Instant::now);
        mem.clear();
        let ns = ns_since(t);
        self.flush_stats.retire_ns = self.flush_stats.retire_ns.saturating_add(ns);
        self.spare = Some(mem);
        self.flush_stats.flushes = self.flush_stats.flushes.saturating_add(1);
        Ok(())
    }

    /// The frozen memtables a rotation may leave in flight: the packings whose lifetimes reach
    /// past it. A packing lasts `pack_ns` from its rotation to its branch and a memtable fills
    /// in `fill_ns` (Little's law, L = λW: λ a memtable each `fill_ns`, W `pack_ns`). A packing
    /// that lasts any part of a fill reaches the rotation that freezes it, so the overlap is
    /// rounded up, `ceil(pack_ns / fill_ns)`: rounded down, a packing a little shorter than its
    /// last measure counted none, and every rotation it outlasted fed it whole and waited for it.
    /// With no measure yet, none. At most the pool's admitted capacity, each packing a worker's.
    fn frozen_most(&self) -> usize {
        if !self.trunk.has_pool() {
            return 0;
        }
        let overlap = crate::util::div_ceil(self.pack_ns, self.fill_ns).unwrap_or(0);
        usize::try_from(overlap)
            .unwrap_or(usize::MAX)
            .min(self.trunk.pool_capacity())
    }

    /// Takes every frozen memtable's branch, waiting for each: the front retires once ready, and
    /// one not ready has its worker's result coming, which the wait takes.
    fn drain_frozen(&mut self) -> Result<(), Error> {
        loop {
            self.retire_ready()?;
            if self.frozen.is_empty() {
                return Ok(());
            }
            self.wait_oldest(self.frozen.len())?;
        }
    }

    /// Waits for a frozen memtable's branch, the oldest `n`'s entries fed first, all at once:
    /// a worker builds only what it has been sent, and fed together they build in parallel, so
    /// the wait is the slowest packing's, not their sum.
    fn wait_oldest(&mut self, n: usize) -> Result<(), Error> {
        let left = self
            .frozen
            .iter()
            .take(n)
            .filter(|f| f.feed.is_some())
            .map(|f| u64::try_from(f.mem.len().saturating_sub(f.packed)).unwrap_or(u64::MAX))
            .fold(0u64, u64::saturating_add);
        if left > 0 {
            self.feed_from(left, true, false)?;
        }
        self.take_packs(true)
    }

    /// Charges a scan's `run_seeks` seeks of the runs, each `many` comparisons where one run would
    /// make `one`, and its `run_steps` heap steps, each `step_extra` comparisons more than one
    /// run's, to the rent the runs being many have cost, at the measured price of a seek's
    /// comparison; `seek_ns`, when its first seek was of the runs, measures it again.
    fn charge_runs(
        &mut self,
        seek_ns: Option<u64>,
        many: usize,
        one: usize,
        run_seeks: u64,
        run_steps: u64,
        step_extra: usize,
    ) {
        let wide = |n: usize| u128::from(u64::try_from(n).unwrap_or(u64::MAX));
        if let Some(ns) = seek_ns {
            self.seek_ns = self.seek_ns.saturating_add(ns);
            self.seek_cmps = self
                .seek_cmps
                .saturating_add(u64::try_from(many).unwrap_or(u64::MAX));
        }
        let extra = u128::from(run_seeks)
            .saturating_mul(wide(many.saturating_sub(one)))
            .saturating_add(u128::from(run_steps).saturating_mul(wide(step_extra)));
        let rent = u128::from(self.seek_ns)
            .saturating_mul(extra)
            .checked_div(u128::from(self.seek_cmps))
            .unwrap_or(0);
        self.tidy_rent_ns = self
            .tidy_rent_ns
            .saturating_add(u64::try_from(rent).unwrap_or(u64::MAX));
    }

    /// Whether scans have paid for merging the active memtable's runs to one: their rent covers
    /// its moves at the measured price of one ([`Self::tidy_rent_ns`]).
    fn tidy_paid(&self) -> bool {
        if self.tidy_rent_ns == 0 || !self.mem.untidy() {
            return false;
        }
        let Some(moves) = self.mem.collapse_moves() else {
            return false;
        };
        let (ns, units) = if self.tidy_moves > 0 {
            (self.tidy_ns, self.tidy_moves)
        } else {
            (self.seek_ns, self.seek_cmps)
        };
        if units == 0 {
            return false;
        }
        let moves = u128::from(u64::try_from(moves).unwrap_or(u64::MAX));
        u128::from(self.tidy_rent_ns).saturating_mul(u128::from(units))
            >= moves.saturating_mul(u128::from(ns))
    }

    /// Whether maintenance is owed: a memtable packing, the trunk's work or a consolidation, a
    /// leaf's REMIX view to build, the active memtable's runs to merge, or freed pages the cache
    /// has yet to forget.
    pub fn owed(&self) -> bool {
        self.owed_with(false)
    }

    /// A range's idle work: finish admitted order jobs without repeatedly rebuilding an
    /// active memtable's larger run for each new partial tail; its runs are merged only once
    /// scans have paid for it ([`Self::tidy_paid`]).
    pub(crate) fn idle_owed(&self) -> bool {
        if self.checkpoint_frozen() {
            return false;
        }
        self.owed_with(true)
    }

    fn checkpoint_frozen(&self) -> bool {
        self.checkpoint
            .as_ref()
            .is_some_and(|c| !matches!(c.phase, CheckpointPhase::Flush))
    }

    pub(crate) fn checkpoint_failed(&self) -> bool {
        self.checkpoint.as_ref().is_some_and(|c| c.error.is_some())
    }

    fn owed_with(&self, paced: bool) -> bool {
        self.packing.is_some()
            || !self.frozen.is_empty()
            || self.trunk.debt() > 0
            || !self.trunk.is_idle()
            || self.trunk.consolidation_owed()
            || self.trunk.views_owed()
            || self.trunk.maplets_owed()
            || (if paced {
                self.mem.debt() > 0
            } else {
                self.mem.untidy()
            })
            || self.tidy_paid()
            || self.store.forget_debt() > 0
            || self.store.cache_unreserved() > 0
    }

    /// Pays up to `keys` work units of maintenance owed, for the shard's idle time (SILK: the
    /// flush first, since a full memtable stops puts; then the trunk's work; then the cache's
    /// views of leaves' bundles; then the consolidations seeks paid for; then the active
    /// memtable's order; then the cache's forgetting). Returns the work done, 0 once nothing is
    /// owed.
    pub fn idle_step(&mut self, keys: u64) -> Result<u64, Error> {
        self.idle_step_with(keys, false)
    }

    /// A runtime task's idle slice: a required trunk read that has not landed leaves its
    /// work owed, so the task can await an attachment completion or another request.
    pub(crate) fn idle_paced_step(&mut self, keys: u64) -> Result<u64, Error> {
        self.require_async_backend()?;
        if self.checkpoint_frozen() {
            return Ok(0);
        }
        self.trunk.clear_io_wait();
        self.idle_step_with(keys, true)
    }

    pub(crate) fn waiting_for_io(&self) -> bool {
        self.trunk.waiting_for_worker()
            || ((self.stopping
                || self.rotation.is_some()
                || self.checkpoint.is_some()
                || self.trunk.waiting_for_io())
                && self.store.io_outstanding())
    }

    pub(crate) async fn wait_completion(&mut self) -> Result<bool, Error> {
        let Some(pool) = self.trunk.pool_mut().filter(|p| p.waiting()) else {
            return self.store.wait_completion().await;
        };
        if self.store.io_outstanding() {
            match hyper_rt::combine::race2(pool.receive(), self.store.wait_completion()).await {
                hyper_rt::combine::Either::First(message) => match message? {
                    Some(message) => pool.accept(&mut self.store, message),
                    None => Ok(true),
                },
                hyper_rt::combine::Either::Second(answer) => answer,
            }
        } else {
            match pool.receive().await? {
                Some(message) => pool.accept(&mut self.store, message),
                None => Ok(true),
            }
        }
    }

    fn idle_step_with(&mut self, keys: u64, paced: bool) -> Result<u64, Error> {
        let left = self.pack_left();
        if left > 0 || self.packing.is_some() {
            // At least one: a memtable packed whole with no filter page left still has its
            // branch to finish.
            let w = keys.min(left).max(1);
            self.pack_some(w, !paced)?;
            return Ok(w);
        }
        if !self.frozen.is_empty() {
            // Only workers' branches owed: an idle shard that may wait waits for the oldest.
            if paced {
                self.take_packs(false)?;
            } else {
                self.wait_oldest(1)?;
            }
            return Ok(1);
        }
        if self.trunk.debt() > 0 || !self.trunk.is_idle() {
            // Idle slices are timed, a clock read a slice: the measured cost of a compacted
            // key consolidations are priced at (`Trunk::note_merge`).
            let t = Some(std::time::Instant::now());
            let used = if paced {
                self.trunk.step_paced(&mut self.store, keys)?
            } else {
                self.trunk.idle_step(&mut self.store, keys)?
            };
            let ns = ns_since(t);
            self.trunk.note_merge(ns, used);
            if self.timed {
                self.note_trunk(ns);
            }
            return Ok(used.max(1));
        }
        if self.trunk.views_owed() {
            return if paced {
                self.trunk.view_step_paced(&mut self.store, keys)
            } else {
                self.trunk.view_step(&mut self.store, keys)
            };
        }
        if self.trunk.maplets_owed() {
            return if paced {
                self.trunk.maplet_step_paced(&mut self.store, keys)
            } else {
                self.trunk.maplet_step(&mut self.store, keys)
            };
        }
        if self.trunk.consolidation_owed() {
            // Seeks paid for consolidating leaves: one cascade, in bulk, flushes and settles
            // them. After views and maplets, which make a bundle one source without rewriting
            // it: seeks pay rent only for the sources left with those built.
            let t = Some(std::time::Instant::now());
            let used = self.trunk.consolidate_step(&mut self.store, keys, paced)?;
            let ns = ns_since(t);
            self.trunk.note_merge(ns, used);
            if self.timed {
                self.note_trunk(ns);
            }
            return Ok(used.max(1));
        }
        let ordering = if paced {
            self.mem.debt() > 0
        } else {
            self.mem.untidy()
        };
        if ordering {
            // Puts admit bounded sorts and same-level counter merges. Active idle finishes
            // those jobs; explicit maintenance can still consolidate every run for seeks.
            let budget = usize::try_from(keys).unwrap_or(usize::MAX);
            let done = if paced {
                self.mem.pay(budget)
            } else {
                self.mem.tidy(budget)
            };
            return Ok(u64::try_from(done).unwrap_or(u64::MAX));
        }
        if self.tidy_paid() {
            // The active memtable's runs merged to one, so a seek searches one, once the scans
            // that searched them have paid for it; the time spent comes off what they paid.
            let budget = usize::try_from(keys).unwrap_or(usize::MAX);
            let t = std::time::Instant::now();
            let done = u64::try_from(self.mem.tidy(budget)).unwrap_or(u64::MAX);
            let ns = ns_since(Some(t));
            self.tidy_ns = self.tidy_ns.saturating_add(ns);
            self.tidy_moves = self.tidy_moves.saturating_add(done);
            self.tidy_rent_ns = self.tidy_rent_ns.saturating_sub(ns);
            return Ok(done);
        }
        if self.store.forget_debt() > 0 {
            return Ok(self.store.forget_some(keys));
        }
        // Last, the cache's slots up to its limit, a page a unit: a read that misses then takes
        // a buffer already mapped, no allocation or first-touch fault on its path.
        let pages = usize::try_from(keys).unwrap_or(usize::MAX);
        Ok(u64::try_from(self.store.reserve_cache(pages)).unwrap_or(u64::MAX))
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

    /// Packs the rest of the packing memtable and takes every frozen one's branch, all going to
    /// the trunk as pending in age order, and keeps a memtable, cleared, for the next fill.
    fn finish_packing(&mut self) -> Result<(), Error> {
        let Some(p) = self.packing.as_mut() else {
            return self.drain_frozen();
        };
        let t = self.timed.then(std::time::Instant::now);
        // The order work left, whole, then the walk from where packing left it.
        p.mem.pay(usize::MAX);
        if p.walk.is_none() {
            p.walk = Some(p.mem.walk_start());
        }
        if p.builder.is_none() && p.feed.is_none() {
            self.start_packing(true)?;
        }
        if self.packing.as_ref().is_some_and(|p| p.feed.is_some()) {
            self.freeze();
            self.drain_frozen()?;
            let ns = ns_since(t);
            self.note_pack(ns);
            self.flush_stats.pack_finish_ns = self.flush_stats.pack_finish_ns.saturating_add(ns);
            return if self.packing.is_some() {
                Err(Error::InvalidArgument {
                    what: "a packing worker that never gave its branch back",
                })
            } else {
                Ok(())
            };
        }
        // Built here: after every older memtable's branch, pending in age order.
        self.drain_frozen()?;
        let Some(Packing {
            mut mem,
            walk,
            builder: Some(mut builder),
            ..
        }) = self.packing.take()
        else {
            return Ok(());
        };
        let mut walk = match walk {
            Some(w) => w,
            None => mem.walk_start(),
        };
        let store = &mut self.store;
        mem.walk_some_hashed(&mut walk, usize::MAX, |k, op, v, h| {
            builder.add_hashed(store, k, op, v, h)
        })?;
        let branch = builder.finish(&mut self.store)?;
        let ns = ns_since(t);
        self.note_pack(ns);
        self.flush_stats.pack_finish_ns = self.flush_stats.pack_finish_ns.saturating_add(ns);
        self.trunk.add(branch);
        let t = self.timed.then(std::time::Instant::now);
        mem.clear();
        let ns = ns_since(t);
        self.flush_stats.retire_ns = self.flush_stats.retire_ns.saturating_add(ns);
        self.spare = Some(mem);
        self.pack_carry = 0;
        self.flush_stats.flushes = self.flush_stats.flushes.saturating_add(1);
        Ok(())
    }

    /// A full memtable as the packing one: its builder now when the shard packs it, or once its
    /// order is paid when a worker may ([`Self::start_packing`]).
    fn packing_of(&mut self, mem: HashMem) -> Result<Packing, Error> {
        let builder = if self.trunk.has_pool() {
            None
        } else {
            Some(Builder::new(
                &mut self.store,
                Keys::Exactly(u64::try_from(mem.len()).unwrap_or(u64::MAX)),
            )?)
        };
        Ok(Packing {
            mem,
            walk: None,
            builder,
            feed: None,
            packed: 0,
            since: std::time::Instant::now(),
        })
    }

    /// The same rotation as the blocking API, paid in the task's existing slice budget.
    /// Only old packing and cascade state changes before it returns true; the caller's entry
    /// remains unapplied, and every old memtable remains reachable by reads.
    fn rotate_paced(&mut self, keys: u64) -> Result<bool, Error> {
        self.require_async_backend()?;
        if self.rotation.is_none() {
            self.rebudget();
            self.flush_stats.rotations = self.flush_stats.rotations.saturating_add(1);
            let now = std::time::Instant::now();
            if let Some(r) = self.rotated {
                self.fill_ns =
                    u64::try_from(now.saturating_duration_since(r).as_nanos()).unwrap_or(u64::MAX);
            }
            let finish_packing = self.packing.as_ref().is_some_and(|p| p.feed.is_none());
            if self.packing.as_ref().is_some_and(|p| p.feed.is_some()) {
                self.freeze();
            }
            self.rotation = Some(Rotation {
                started: now,
                finish_packing,
                most: None,
                stalled: finish_packing,
            });
        }
        if self.rotation.as_ref().is_some_and(|r| r.finish_packing) {
            if self.packing.is_some() {
                self.pack_some(keys.max(1), false)?;
                if self.packing.is_some() {
                    return Ok(false);
                }
            }
            let left = self.pack_left();
            if left > 0 {
                self.feed_from(left.min(keys.max(1)), false, false)?;
            }
            self.take_packs(false)?;
            if !self.frozen.is_empty() {
                return Ok(false);
            }
            if let Some(r) = self.rotation.as_mut() {
                r.finish_packing = false;
            }
        }
        let most = match self.rotation.as_ref().and_then(|r| r.most) {
            Some(most) => most,
            None => {
                let most = self.frozen_most();
                if let Some(r) = self.rotation.as_mut() {
                    r.most = Some(most);
                }
                most
            }
        };
        if self.frozen.len() > most {
            if let Some(r) = self.rotation.as_mut() {
                r.stalled = true;
            }
            let n = self.frozen.len().saturating_sub(most);
            let left = self
                .frozen
                .iter()
                .take(n)
                .filter(|f| f.feed.is_some())
                .map(|f| u64::try_from(f.mem.len().saturating_sub(f.packed)).unwrap_or(u64::MAX))
                .fold(0u64, u64::saturating_add);
            if left > 0 {
                self.feed_from(left.min(keys.max(1)), false, false)?;
            }
            self.take_packs(false)?;
            if self.frozen.len() > most {
                return Ok(false);
            }
        }
        if self.trunk.pending() >= self.trunk.fanout() {
            if let Some(r) = self.rotation.as_mut() {
                r.stalled = true;
            }
            if self.trunk.packing_blocks_dispatch() {
                // A packing holds every seat and can still need owner-fed input. Pay its
                // bounded slice before waiting on the cascade's worker capacity.
                self.pack_some(keys.max(1), false)?;
                self.take_packs(false)?;
                if self.trunk.packing_blocks_dispatch() {
                    return Ok(false);
                }
            }
            if !self
                .trunk
                .finish_cascade_paced(&mut self.store, keys.max(1))?
            {
                return Ok(false);
            }
        }
        let fresh = match self.spare.take() {
            Some(m) => m,
            None => HashMem::new(self.mem_limit)?,
        };
        let mut full = std::mem::replace(&mut self.mem, fresh);
        self.tidy_rent_ns = 0;
        full.close();
        self.packing = Some(self.packing_of(full)?);
        let r = self.rotation.take().ok_or(Error::InvalidArgument {
            what: "a resumed rotation with no initial state",
        })?;
        let ns = if self.timed {
            u64::try_from(r.started.elapsed().as_nanos()).unwrap_or(u64::MAX)
        } else {
            0
        };
        self.flush_stats.rotate_ns = self.flush_stats.rotate_ns.saturating_add(ns);
        if r.stalled {
            let f = &mut self.flush_stats;
            f.stalls = f.stalls.saturating_add(1);
            f.stall_ns = f.stall_ns.saturating_add(ns);
            f.stall_max_ns = f.stall_max_ns.max(ns);
        }
        self.rotated = Some(std::time::Instant::now());
        Ok(true)
    }

    /// Packs every memtable and finishes its cascades before a range starts durability.
    /// A false result owns all unfinished work.
    pub(crate) fn flush_paced(&mut self, keys: u64) -> Result<bool, Error> {
        self.trunk.clear_io_wait();
        self.require_async_backend()?;
        if self.rotation.is_some() && !self.rotate_paced(keys)? {
            return Ok(false);
        }
        if self.packing.is_some() || !self.frozen.is_empty() {
            self.pack_some(keys.max(1), false)?;
            self.take_packs(false)?;
            return Ok(false);
        }
        if !self.mem.is_empty() {
            let fresh = match self.spare.take() {
                Some(m) => m,
                None => HashMem::new(self.mem_limit)?,
            };
            let mut full = std::mem::replace(&mut self.mem, fresh);
            self.tidy_rent_ns = 0;
            full.close();
            self.packing = Some(self.packing_of(full)?);
            return Ok(false);
        }
        if !self.trunk.is_idle() {
            self.trunk.step_paced(&mut self.store, keys.max(1))?;
            return Ok(false);
        }
        self.trunk_carry = 0;
        Ok(true)
    }

    /// The memtable is full: it becomes the packing one and a cleared one takes the puts. A debt
    /// still owed is paid first, whole, and counted a stall: the packing memtable's, and the
    /// trunk's cascade when `fanout` packed memtables wait on it.
    fn rotate(&mut self) -> Result<(), Error> {
        self.rebudget();
        self.flush_stats.rotations = self.flush_stats.rotations.saturating_add(1);
        let now = std::time::Instant::now();
        // The fill lasts from the last rotation's end: a stall there is not the fill's time,
        // which would lower the measured overlap and make the next rotation wait for more.
        if let Some(r) = self.rotated {
            self.fill_ns =
                u64::try_from(now.saturating_duration_since(r).as_nanos()).unwrap_or(u64::MAX);
        }
        let t = if self.timed { Some(now) } else { None };
        let mut stalled = false;
        if self.packing.as_ref().is_some_and(|p| p.feed.is_some()) {
            // Handed to a worker: frozen as it is, later puts feeding what is left, while the
            // next memtable fills.
            self.freeze();
        } else if self.packing.is_some() {
            self.finish_packing()?;
            stalled = true;
        }
        // Whatever froze them (this rotation, or a put's share that fed one whole), no more stay
        // frozen than the measure says overlap: the oldest are waited for.
        let most = self.frozen_most();
        while self.frozen.len() > most {
            self.wait_oldest(self.frozen.len().saturating_sub(most))?;
            stalled = true;
        }
        if self.trunk.pending() >= self.trunk.fanout() {
            if self.trunk.packing_blocks_dispatch() {
                // A whole cascade cannot admit its first job until a packing seat is free;
                // feed the oldest packing before any wait that would depend on its owner.
                self.wait_oldest(1)?;
            }
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
            None => HashMem::new(self.mem_limit)?,
        };
        let mut full = std::mem::replace(&mut self.mem, fresh);
        // Rent paid on one memtable's runs buys no merge of the next's.
        self.tidy_rent_ns = 0;
        // Its order, a debt its packing pays a slice at a time.
        full.close();
        self.packing = Some(self.packing_of(full)?);
        self.rotated = Some(std::time::Instant::now());
        Ok(())
    }

    /// Pays maintenance owed, for the shard's idle time: the packing memtable's whole, then up
    /// to `budget` work units of the trunk's (initial leaf moves, merged keys and filter pages),
    /// then REMIX views of its bundles and the cache's freed pages with the budget left.
    /// Returns the work done, less than `budget` only once nothing is owed.
    pub fn maintain(&mut self, budget: u64) -> Result<u64, Error> {
        self.finish_packing()?;
        // Timed, one clock read a drain: a compacted key's cost, which consolidations are
        // priced at (`Trunk::note_merge`).
        let t = Some(std::time::Instant::now());
        let mut used = self.trunk.step(&mut self.store, budget)?;
        let ns = ns_since(t);
        self.trunk.note_merge(ns, used);
        if self.timed {
            self.note_trunk(ns);
        }
        // Each view step does at least one unit of work while views are owed: the budget left
        // bounds the steps.
        while used < budget && self.trunk.views_owed() {
            let done = self
                .trunk
                .view_step(&mut self.store, budget.saturating_sub(used))?;
            used = used.saturating_add(done.max(1));
        }
        // Then bundles' maplets, each step at least one unit while owed.
        while used < budget && self.trunk.maplets_owed() {
            let done = self
                .trunk
                .maplet_step(&mut self.store, budget.saturating_sub(used))?;
            used = used.saturating_add(done.max(1));
        }
        // Then a consolidation of every pivot of several sources, each step at least one unit
        // while owed: a drain settles the layout so each seek opens one source, as bulk
        // preparation measured 3.6x the seeks of the layout it replaces (Codex's
        // mantle-bounded-consolidation-experiment-20261008). Then the views and maplets of the
        // bundles it changed.
        self.trunk.consolidate_all();
        while used < budget && (self.trunk.consolidation_owed() || self.trunk.cascading()) {
            // A drain's whole budget stays whole, so each step waits for the workers'
            // compactions rather than return to be called again.
            let left = if budget == u64::MAX {
                u64::MAX
            } else {
                budget.saturating_sub(used)
            };
            let done = self.trunk.consolidate_step(&mut self.store, left, false)?;
            used = used.saturating_add(done.max(1));
            while used < budget && self.trunk.views_owed() {
                let done = self
                    .trunk
                    .view_step(&mut self.store, budget.saturating_sub(used))?;
                used = used.saturating_add(done.max(1));
            }
            while used < budget && self.trunk.maplets_owed() {
                let done = self
                    .trunk
                    .maplet_step(&mut self.store, budget.saturating_sub(used))?;
                used = used.saturating_add(done.max(1));
            }
        }
        // Then the active memtable's runs merged to one, each step at least one unit while owed.
        while used < budget && self.mem.untidy() {
            let left = usize::try_from(budget.saturating_sub(used)).unwrap_or(usize::MAX);
            let done = u64::try_from(self.mem.tidy(left)).unwrap_or(u64::MAX);
            used = used.saturating_add(done.max(1));
        }
        // Then the cache's freed pages, at most the budget left.
        if used < budget && self.store.forget_debt() > 0 {
            used = used.saturating_add(self.store.forget_some(budget.saturating_sub(used)));
        }
        // Then the cache's slots reserved up to its limit, a page a unit.
        if used < budget {
            let pages = usize::try_from(budget.saturating_sub(used)).unwrap_or(usize::MAX);
            let made = self.store.reserve_cache(pages);
            used = used.saturating_add(u64::try_from(made).unwrap_or(u64::MAX));
        }
        Ok(used)
    }

    /// Packs every memtable into the trunk and runs its maintenance to the end.
    pub fn flush(&mut self) -> Result<(), Error> {
        self.no_checkpoint_mutation()?;
        self.finish_packing()?;
        if !self.mem.is_empty() {
            let fresh = match self.spare.take() {
                Some(m) => m,
                None => HashMem::new(self.mem_limit)?,
            };
            let mut full = std::mem::replace(&mut self.mem, fresh);
            // Rent paid on one memtable's runs buys no merge of the next's.
            self.tidy_rent_ns = 0;
            full.close();
            self.packing = Some(self.packing_of(full)?);
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

    /// The bytes the trunk's branches and views hold in memory.
    pub fn memory(&self) -> crate::trunk::Memory {
        self.trunk.memory()
    }

    /// Trunk sources scans have passed over by their range filters, and those they opened.
    pub fn scan_filtered(&self) -> (u64, u64) {
        (self.scan_skipped, self.scan_opened)
    }

    /// The trunk's bundles that have a REMIX view now.
    pub fn views(&self) -> usize {
        self.trunk.views()
    }

    /// The trunk's shape: height, nodes, leaves.
    pub fn shape(&self) -> Result<(usize, usize, usize), Error> {
        self.trunk.shape()
    }
}

#[cfg(test)]
mod tune_tests {
    use super::{Gain, tune};

    const MIB: usize = 1 << 20;

    fn gain(saved_ns: u64, bytes: usize) -> Gain {
        Gain { saved_ns, bytes }
    }

    #[test]
    fn memory_moves_toward_the_region_whose_bytes_saved_more() {
        let m = 100 * MIB;
        // Write memory saved 10 ns a byte where the cache saved 1: write takes a step, 5% of
        // the budget, the cache giving no more than 10% of its own.
        assert_eq!(
            tune(m, 0, m, gain(1_000, 1_000), gain(10_000, 1_000)),
            5 * MIB
        );
        // The cache saved more: write gives back at most 10% of its share.
        assert_eq!(
            tune(m, 20 * MIB, m, gain(10_000, 1_000), gain(1_000, 1_000)),
            18 * MIB
        );
        // Equal gains, or nothing saved: no change.
        assert_eq!(tune(m, 20 * MIB, m, gain(5, 10), gain(10, 20)), 20 * MIB);
        assert_eq!(tune(m, 20 * MIB, m, gain(0, 10), gain(0, 20)), 20 * MIB);
        // A small cache gives at most 10% of itself: 98 MiB of write leaves 2 MiB of cache,
        // which gives up 0.2 MiB.
        assert_eq!(
            tune(m, 98 * MIB, m, gain(0, 1), gain(1, 1)),
            98 * MIB + 2 * MIB / 10
        );
    }

    #[test]
    fn three_regions_move_memory_from_the_least_to_the_most_saved() {
        let m = 100 * MIB;
        // Records saved most, write least: write gives, at most 10% of its 20 MiB.
        assert_eq!(
            super::tune3(
                m,
                [70 * MIB, 20 * MIB, 10 * MIB],
                m,
                [gain(5, 1), gain(1, 1), gain(9, 1)],
                &mut super::Newton::default()
            ),
            [70 * MIB, 18 * MIB, 12 * MIB]
        );
        // The least saving region holds nothing: the donor is the least among the rest.
        assert_eq!(
            super::tune3(
                m,
                [90 * MIB, 0, 10 * MIB],
                m,
                [gain(1, 1), gain(0, 1), gain(9, 1)],
                &mut super::Newton::default()
            ),
            [85 * MIB, 0, 15 * MIB]
        );
        // All equal: nothing moves.
        assert_eq!(
            super::tune3(
                m,
                [50 * MIB, 25 * MIB, 25 * MIB],
                m,
                [gain(1, 1), gain(1, 1), gain(1, 1)],
                &mut super::Newton::default()
            ),
            [50 * MIB, 25 * MIB, 25 * MIB]
        );
        // Write past the cycle's writes: the excess goes to the page cache.
        assert_eq!(
            super::tune3(
                m,
                [50 * MIB, 30 * MIB, 20 * MIB],
                10 * MIB,
                [gain(1, 1), gain(1, 1), gain(1, 1)],
                &mut super::Newton::default()
            ),
            [70 * MIB, 10 * MIB, 20 * MIB]
        );
    }

    #[test]
    fn the_newton_step_moves_to_where_the_fitted_gains_meet() {
        // The first region saved 90 ns a MiB more at 10 MiB, 80 more at 20 MiB: on that line
        // the gains meet at 100 MiB, 80 MiB on from 20.
        let mut n = super::Newton::default();
        assert_eq!(n.step((0, 2), 10 * MIB, 90), None);
        assert_eq!(n.step((0, 2), 20 * MIB, 80), Some(80 * 1_048_576));
        // Another pair starts over.
        assert_eq!(n.step((1, 2), 20 * MIB, 80), None);
        // Returns that grow with memory give no Newton step: the fixed step is taken.
        assert_eq!(n.step((1, 2), 30 * MIB, 90), None);
        // A third sample keeps the last three; the fourth drops the first.
        let mut n = super::Newton::default();
        for (x, d) in [(1, 0), (10, 90), (20, 80), (30, 70)] {
            n.step((0, 1), x * MIB, d);
        }
        assert_eq!(n.samples.len(), super::TUNE_SAMPLES);
        assert_eq!(n.step((0, 1), 40 * MIB, 60), Some(60 * 1_048_576));
    }

    #[test]
    fn the_tuner_takes_the_newton_step_within_the_donor_bound() {
        let m = 1000 * MIB;
        let mut n = super::Newton::default();
        // Records at 10 MiB saved 2 ns a byte, the page cache 1: the fixed step, 5% of 1000.
        let first = super::tune3(
            m,
            [990 * MIB, 0, 10 * MIB],
            m,
            [gain(990, 990), gain(0, 1), gain(20, 10)],
            &mut n,
        );
        assert_eq!(first, [940 * MIB, 0, 60 * MIB]);
        // At 60 MiB records saved 1.5 a byte: the line meets the cache's 1 at 110 MiB, 50 on.
        let second = super::tune3(
            m,
            first,
            m,
            [gain(940, 940), gain(0, 1), gain(90, 60)],
            &mut n,
        );
        assert_eq!(second, [890 * MIB, 0, 110 * MIB]);
        // The page cache saved 1001 ns a MiB more at 99 MiB and 1000 more at 100: the line
        // meets at 1100 MiB, past the records' 10%, so the step is 90 MiB.
        let mut n = super::Newton::default();
        n.step((0, 2), 99 * MIB, 1001);
        let next = super::tune3(
            m,
            [100 * MIB, 0, 900 * MIB],
            m,
            [gain(1000, MIB), gain(0, 1), gain(0, MIB)],
            &mut n,
        );
        assert_eq!(next, [190 * MIB, 0, 810 * MIB]);
    }

    #[test]
    fn write_memory_never_passes_a_cycles_writes() {
        let m = 100 * MIB;
        // Write wins, but the last cycle wrote 3 MiB: a longer queue spares no wait.
        assert_eq!(tune(m, 0, 3 * MIB, gain(0, 1), gain(1, 1)), 3 * MIB);
        // A cycle that wrote less than the share brings it down at once.
        assert_eq!(tune(m, 50 * MIB, 8 * MIB, gain(0, 1), gain(0, 1)), 8 * MIB);
    }

    #[test]
    fn gains_compare_exactly_at_the_extremes() {
        let m = 1 << 40;
        // u64::MAX ns over two bytes against u64::MAX - 1 over one: products past u64 compared
        // in u128, never divided or overflowed; write saved slightly less a byte.
        assert_eq!(
            tune(m, m / 2, m, gain(u64::MAX - 1, 1), gain(u64::MAX, 2)),
            m / 2 - m / 20
        );
        // And exactly equal a byte across the extremes: no change.
        assert_eq!(
            tune(m, m / 2, m, gain(1, 1), gain(u64::MAX, usize::MAX)),
            m / 2
        );
    }
}

#[cfg(test)]
mod frozen_tests {
    //! Frozen memtables out of order and through failure, live: the host's own workers and the
    //! overlap the shard measures, a stall or a failure armed on the worker a packing went to.
    //! The packing whose writes stall stays frozen and every read stays exact; a younger packing
    //! another worker takes comes back behind it, and on a host whose one worker is the stalled
    //! one it waits for that worker; a starved feed holds no younger one back; released, the
    //! branches enter the trunk oldest first; a worker's failure is kept, typed and fenced,
    //! never dropping what the memtables hold; and the store reopens at its last checkpoint.
    use super::*;
    use hyper_block::DiskError;
    use hyper_block::buf::Alignment;
    use hyper_block::file::{CachingRequest, DeviceFile};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex, MutexGuard};

    /// One test's gates, statics of its own so tests running at once share none: the worker
    /// whose writes stall (`usize::MAX` names none), whether they stall and whether one waits
    /// now, whether that worker's next write fails, and the workers opened so far, each
    /// numbered as the pool seats it (seats fill in order, and none empties during a test).
    struct Gates {
        worker: AtomicUsize,
        state: Mutex<Stall>,
        changed: Condvar,
        fail: AtomicBool,
        opened: AtomicUsize,
    }

    struct Stall {
        held: bool,
        waiting: bool,
    }

    impl Gates {
        const fn new() -> Self {
            Self {
                worker: AtomicUsize::new(usize::MAX),
                state: Mutex::new(Stall {
                    held: false,
                    waiting: false,
                }),
                changed: Condvar::new(),
                fail: AtomicBool::new(false),
                opened: AtomicUsize::new(0),
            }
        }

        fn state(&self) -> MutexGuard<'_, Stall> {
            self.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }

        /// Stalls worker `worker`'s writes, and fails the first after the stall when `fail`.
        fn arm(&self, worker: usize, fail: bool) {
            self.fail.store(fail, Ordering::SeqCst);
            self.state().held = true;
            self.worker.store(worker, Ordering::SeqCst);
        }

        /// Waits until the stalled worker waits in a write.
        fn until_waiting(&self) {
            let mut s = self.state();
            while !s.waiting {
                s = self
                    .changed
                    .wait(s)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }

        fn waited(&self) -> bool {
            self.state().waiting
        }

        fn release(&self) {
            self.state().held = false;
            self.changed.notify_all();
        }
    }

    /// A file a worker writes through, numbered as the pool seats it, its writes gated by its
    /// test's [`Gates`]; the shard's own file has none.
    struct Held {
        file: DeviceFile,
        worker: usize,
        gates: Option<&'static Gates>,
    }

    impl BlockFile for Held {
        fn alignment(&self) -> Alignment {
            self.file.alignment()
        }
        fn len(&self) -> Result<u64, DiskError> {
            self.file.len()
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
            self.file.read_exact_at(buf, offset)
        }
        fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
            if let Some(g) = self
                .gates
                .filter(|g| g.worker.load(Ordering::SeqCst) == self.worker)
            {
                let mut s = g.state();
                while s.held {
                    s.waiting = true;
                    g.changed.notify_all();
                    s = g
                        .changed
                        .wait(s)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                drop(s);
                // One failure an arming: taken and cleared at once.
                if g.fail.swap(false, Ordering::SeqCst) {
                    return Err(DiskError::Io {
                        op: "a test's failed write",
                        path: std::path::PathBuf::new(),
                        source: std::io::Error::other("a test's failed write"),
                    });
                }
            }
            self.file.write_all_at(buf, offset)
        }
        fn sync_data(&self) -> Result<(), DiskError> {
            self.file.sync_data()
        }
    }

    /// Releases its gates when dropped: a failed assertion while a worker stalls must not leave
    /// the pool's drop joining a worker that waits forever. Declared after the shard, it drops
    /// before the shard's pool does.
    struct Open(&'static Gates);

    impl Drop for Open {
        fn drop(&mut self) {
            self.0.fail.store(false, Ordering::SeqCst);
            self.0.release();
        }
    }

    fn open(path: &std::path::Path, create: bool) -> DeviceFile {
        DeviceFile::open(
            path,
            create,
            CachingRequest::Buffered,
            Alignment::new(4096).unwrap(),
        )
        .unwrap()
    }

    fn plain(path: &std::path::Path, create: bool) -> Held {
        Held {
            file: open(path, create),
            worker: usize::MAX,
            gates: None,
        }
    }

    /// The workers' files, each numbered as the pool seats it and gated by `gates`.
    fn gated(
        path: &std::path::Path,
        gates: &'static Gates,
    ) -> impl FnMut() -> Result<Held, Error> + Send + 'static {
        let path = path.to_path_buf();
        move || {
            Ok(Held {
                file: open(&path, false),
                worker: gates.opened.fetch_add(1, Ordering::SeqCst),
                gates: Some(gates),
            })
        }
    }

    const STORE: Config = Config {
        page_size: 4096,
        extent_pages: 8,
        max_extents: 1 << 16,
    };
    const TRUNK: TrunkConfig = TrunkConfig {
        fanout: 3,
        leaf_entries: 96,
    };
    const MEM: usize = 4 * 1024;

    fn key(k: u64) -> Vec<u8> {
        format!("k{:05}", k % 700).into_bytes()
    }

    fn check(db: &mut ShardDb<Held>, oracle: &BTreeMap<Vec<u8>, Vec<u8>>) {
        let mut v = Vec::new();
        for k in 0..700u64 {
            let kk = key(k);
            let found = db.get(&kk, &mut v).unwrap();
            match oracle.get(&kk) {
                Some(want) => assert!(found && &v == want, "key {k}"),
                None => assert!(!found, "key {k} reads a value it does not hold"),
            }
        }
        scan_exact(db, oracle);
    }

    fn scan_exact(db: &mut ShardDb<Held>, oracle: &BTreeMap<Vec<u8>, Vec<u8>>) {
        let mut rows = Rows::new();
        let mut next = Vec::new();
        assert!(
            !db.scan(b"", None, usize::MAX, &mut rows, &mut next)
                .unwrap()
        );
        let got: Vec<(Vec<u8>, Vec<u8>)> =
            rows.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
        let want: Vec<(Vec<u8>, Vec<u8>)> =
            oracle.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(got, want);
    }

    fn put_or_delete(db: &mut ShardDb<Held>, oracle: &mut BTreeMap<Vec<u8>, Vec<u8>>, i: u64) {
        let k = key(i.wrapping_mul(2_654_435_761));
        if i.is_multiple_of(7) {
            db.delete(&k).unwrap();
            oracle.remove(&k);
        } else {
            let v = format!("v{i}").into_bytes();
            db.put(&k, &v).unwrap();
            oracle.insert(k, v);
        }
    }

    /// A put of a key no earlier put wrote, so a memtable fills with as many entries as it
    /// holds.
    fn put_new(db: &mut ShardDb<Held>, oracle: &mut BTreeMap<Vec<u8>, Vec<u8>>, i: u64) {
        let k = format!("k{i:06}").into_bytes();
        let v = format!("v{i}").into_bytes();
        db.put(&k, &v).unwrap();
        oracle.insert(k, v);
    }

    type Put = fn(&mut ShardDb<Held>, &mut BTreeMap<Vec<u8>, Vec<u8>>, u64);

    /// Puts until the shard has measured a fill and a packing, so the overlap it lets stay
    /// frozen is its own: by the second rotation, which, with no overlap measured yet, waits for
    /// the first packing a worker took and so measures it.
    fn warm(
        db: &mut ShardDb<Held>,
        oracle: &mut BTreeMap<Vec<u8>, Vec<u8>>,
        i: &mut u64,
        put: Put,
    ) {
        let rotations = db.flush_stats.rotations;
        while db.pack_ns == 0 || db.fill_ns == 0 {
            assert!(
                db.flush_stats.rotations < rotations + 2,
                "a second rotation measured no packing"
            );
            put(db, oracle, *i);
            *i += 1;
        }
    }

    /// Puts until the packing memtable is out on a worker that has none of its entries yet (its
    /// feed's buffers all still in hand), then stalls that worker's writes, failing the first
    /// after the stall when `fail`; puts through the rotation that freezes it, feeds it what its
    /// buffers take, and waits until its worker waits in a write. Returns that worker.
    fn stall_oldest(
        db: &mut ShardDb<Held>,
        oracle: &mut BTreeMap<Vec<u8>, Vec<u8>>,
        i: &mut u64,
        put: Put,
        gates: &'static Gates,
        fail: bool,
    ) -> usize {
        let rotations = db.flush_stats.rotations;
        let worker = loop {
            if let Some(f) = db.packing.as_ref().and_then(|p| p.feed.as_ref()) {
                assert_eq!(
                    f.spare.len() + usize::from(f.filling.is_some()),
                    FEED_BUFFERS,
                    "a buffer reached the worker before its stall was armed"
                );
                break f.worker;
            }
            assert_eq!(
                db.flush_stats.rotations, rotations,
                "a fill passed with no worker taking its packing"
            );
            put(db, oracle, *i);
            *i += 1;
        };
        gates.arm(worker, fail);
        while db.flush_stats.rotations == rotations {
            put(db, oracle, *i);
            *i += 1;
        }
        db.feed_from(u64::MAX, false, false).unwrap();
        gates.until_waiting();
        assert!(
            db.frozen
                .front()
                .is_some_and(|f| f.worker == worker && !f.ready()),
            "the stalled packing is the oldest frozen"
        );
        worker
    }

    /// The younger packing's share, all of it, waited for: a free worker, when the host has
    /// one, takes it and its result comes back behind the stalled one; on a host whose workers
    /// are all busy, the stalled one's among them, it waits for a worker.
    fn younger_beside(db: &mut ShardDb<Held>) {
        db.pack_some(u64::MAX, true).unwrap();
        match db.frozen.get(1) {
            Some(younger) => assert!(
                younger.back.is_some(),
                "a younger packing's result did not come back behind the stalled one"
            ),
            None => {
                assert!(
                    db.packing.as_ref().is_some_and(|p| p.feed.is_none()),
                    "a younger packing neither came back nor waits for a worker"
                );
                assert!(
                    !db.trunk.pool_mut().unwrap().can_take(true),
                    "a younger packing waits while a worker is free"
                );
            }
        }
    }

    static GATES_A: Gates = Gates::new();

    #[test]
    fn a_stalled_packing_holds_no_younger_one_back_and_retires_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let mut db = ShardDb::create(plain(&path, true), STORE, MEM, TRUNK).unwrap();
        let _open = Open(&GATES_A);
        db.set_workers(gated(&path, &GATES_A)).unwrap();
        let mut oracle = BTreeMap::new();
        let mut i = 0u64;
        warm(&mut db, &mut oracle, &mut i, put_or_delete);
        check(&mut db, &oracle);
        stall_oldest(&mut db, &mut oracle, &mut i, put_or_delete, &GATES_A, false);
        younger_beside(&mut db);
        check(&mut db, &oracle);
        // Released: every frozen branch enters the trunk, oldest first.
        GATES_A.release();
        db.drain_frozen().unwrap();
        assert!(db.frozen.is_empty());
        assert!(GATES_A.waited());
        check(&mut db, &oracle);
        // Puts on, through a cascade, at the overlap the shard measures.
        let rotations = db.flush_stats.rotations;
        let cascade = u64::try_from(TRUNK.fanout).unwrap();
        while db.flush_stats.rotations < rotations + cascade {
            put_or_delete(&mut db, &mut oracle, i);
            i += 1;
        }
        check(&mut db, &oracle);
        db.checkpoint(i).unwrap();
        check(&mut db, &oracle);
        let (file, landed) = crate::store::cold_file(db.into_file());
        landed.unwrap();
        drop(file);
        let (mut db, applied) = ShardDb::open(plain(&path, false), STORE, MEM, TRUNK).unwrap();
        assert_eq!(applied, i);
        check(&mut db, &oracle);
        db.check_references().unwrap();
    }

    static GATES_B: Gates = Gates::new();

    #[test]
    fn a_failed_packing_keeps_entries_and_reports_the_worker_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let mut db = ShardDb::create(plain(&path, true), STORE, MEM, TRUNK).unwrap();
        let _open = Open(&GATES_B);
        let mut oracle = BTreeMap::new();
        db.put(b"durable", b"before worker failure").unwrap();
        oracle.insert(b"durable".to_vec(), b"before worker failure".to_vec());
        db.checkpoint(1).unwrap();
        let durable = oracle.clone();
        db.set_workers(gated(&path, &GATES_B)).unwrap();
        let mut i = 0u64;
        warm(&mut db, &mut oracle, &mut i, put_or_delete);
        // A real worker failure is retained, with no owner-thread repacking.
        stall_oldest(&mut db, &mut oracle, &mut i, put_or_delete, &GATES_B, true);
        younger_beside(&mut db);
        GATES_B.release();
        assert!(matches!(db.drain_frozen(), Err(Error::Io { .. })));
        assert!(matches!(db.drain_frozen(), Err(Error::Io { .. })));
        check(&mut db, &oracle);
        assert!(matches!(
            db.put(b"after", b"refused"),
            Err(Error::Io { .. })
        ));
        assert!(matches!(db.delete(b"durable"), Err(Error::Io { .. })));
        check(&mut db, &oracle);
        let (file, retired) = crate::store::cold_file(db.into_file());
        assert!(matches!(retired, Err(Error::Io { .. })));
        drop(file);
        let (mut db, applied) = ShardDb::open(plain(&path, false), STORE, MEM, TRUNK).unwrap();
        assert_eq!(applied, 1);
        check(&mut db, &durable);
        db.check_references().unwrap();
    }

    /// The cap rounds the measured overlap up: a packing that lasts any part of a fill reaches the
    /// rotation that freezes it, so one a little shorter than a fill may still be out there; with
    /// no workers, or no fill measured, none ([`ShardDb::frozen_most`]).
    #[test]
    fn the_frozen_cap_rounds_the_measured_overlap_up() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let mut db = ShardDb::create(plain(&path, true), STORE, MEM, TRUNK).unwrap();
        db.fill_ns = 1_000;
        db.pack_ns = 999;
        assert_eq!(db.frozen_most(), 0, "no workers, none");
        let p = path.clone();
        db.set_workers(move || Ok(plain(&p, false))).unwrap();
        for (pack_ns, most) in [
            (0u64, 0usize),
            (1, 1),
            (999, 1),
            (1_000, 1),
            (1_001, 2),
            (2_500, 3),
        ] {
            db.pack_ns = pack_ns;
            assert_eq!(
                db.frozen_most(),
                most.min(Pool::cores()),
                "a packing of {pack_ns} ns against a fill of 1000 ns"
            );
        }
        db.fill_ns = 0;
        db.pack_ns = 5_000;
        assert_eq!(db.frozen_most(), 0, "no fill measured, none");
    }

    static GATES_C: Gates = Gates::new();

    /// A run two pages, the fewest an extent holds, so a memtable takes many of a feed's buffers
    /// and its feed starves while its worker stalls.
    const STORE_RUN_PAGE: Config = Config {
        page_size: 4096,
        extent_pages: 2,
        max_extents: 1 << 16,
    };
    const MEM_RUNS: usize = 128 * 1024;

    #[test]
    fn a_starved_oldest_feed_never_holds_a_younger_packing_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let mut db = ShardDb::create(plain(&path, true), STORE_RUN_PAGE, MEM_RUNS, TRUNK).unwrap();
        let _open = Open(&GATES_C);
        db.set_workers(gated(&path, &GATES_C)).unwrap();
        let mut oracle = BTreeMap::new();
        let mut i = 0u64;
        warm(&mut db, &mut oracle, &mut i, put_new);
        stall_oldest(&mut db, &mut oracle, &mut i, put_new, &GATES_C, false);
        assert!(
            db.frozen.front().is_some_and(|f| f.feed.is_some()),
            "the stalled packing's feed did not starve"
        );
        // Puts until the younger packing is out on another worker with a buffer in hand and
        // entries left, or, on a host whose workers are all busy, the stalled one's among them,
        // its order paid with no worker free to take it; within the fill, so no rotation waits
        // on the stalled one.
        let rotations = db.flush_stats.rotations;
        let in_hand = loop {
            let p = db.packing.as_ref().unwrap();
            if p.packed < p.mem.len() && p.feed.as_ref().is_some_and(|f| f.filling.is_some()) {
                break true;
            }
            if p.walk.is_some() && p.feed.is_none() && !db.trunk.pool_mut().unwrap().can_take(true)
            {
                break false;
            }
            assert_eq!(
                db.flush_stats.rotations, rotations,
                "a fill passed with the younger packing neither out nor waiting for a worker"
            );
            put_new(&mut db, &mut oracle, i);
            i += 1;
        };
        if in_hand {
            // One pass, waiting for none: the younger packing takes entries though the oldest
            // starves.
            let before = db.packing.as_ref().map_or(0, |p| p.packed);
            db.feed(u64::MAX, false).unwrap();
            let after = db.packing.as_ref().map_or(0, |p| p.packed);
            assert!(
                after > before,
                "the starved oldest held the younger feed back"
            );
        }
        assert!(db.frozen.front().is_some_and(|f| !f.ready()));
        scan_exact(&mut db, &oracle);
        // Released: every branch enters the trunk, oldest first, reads exact.
        GATES_C.release();
        db.drain_frozen().unwrap();
        db.maintain(u64::MAX).unwrap();
        assert!(db.frozen.is_empty());
        let mut v = Vec::new();
        for (k, want) in &oracle {
            assert!(db.get(k, &mut v).unwrap() && &v == want);
        }
        db.checkpoint(i).unwrap();
        let (file, landed) = crate::store::cold_file(db.into_file());
        landed.unwrap();
        drop(file);
        let (mut db, applied) =
            ShardDb::open(plain(&path, false), STORE_RUN_PAGE, MEM_RUNS, TRUNK).unwrap();
        assert_eq!(applied, i);
        scan_exact(&mut db, &oracle);
        db.check_references().unwrap();
    }
}
