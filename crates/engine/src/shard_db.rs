//! One shard's engine (docs/design/engine-structure.md §2, §6, §8): the memtable, and the trunk
//! of branches in the shard's store. A put or delete goes to the memtable; a get reads the
//! memtable, the one being packed, then the trunk.
//!
//! Maintenance runs on the shard's own worker, a slice at a time, never all at once on the put
//! that fills a memtable (SILK, Balmau et al., USENIX ATC 2019: flushes first, compaction spread
//! so no put waits on it; SplinterDB, Conway et al., USENIX ATC 2020: incorporation and bundle
//! compactions as separate tasks). A full memtable goes to a maintenance worker that sorts the
//! copy of its arena it kept and builds its branch, frozen and read until the branch is back
//! (research/40 §4); with no worker, or none free when one must finish, the shard packs it
//! itself as the packing memtable. A cleared one takes the puts. Each put then does its share of
//! two debts, each over the room it must be paid within:
//! - the packing memtable's entries, before the new one fills (`room`, its arena bytes left);
//! - the trunk's maintenance, before `fanout` packed memtables wait on it (`room` and a memtable
//!   for each further one that may wait).
//!
//! A put of `b` bytes does `debt · b / room` of a debt: paid at that rate, it is paid when the
//! room runs out. Maintenance's compactions are owed only once planned, so a debt can still be
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
use crate::trunk::pool::{self, Back, Growth, Job, Owner, Pool, SortPack, Spawn, Ticket, Work};
use crate::trunk::{Source, Trunk, TrunkConfig};
use crate::util::reuse;
use hyper_block::block::BlockFile;
use std::collections::VecDeque;

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
    /// Entries the active memtables' paced order work moved inside puts, counted always: none
    /// while workers sort the memtables and no scan reads them (research/40 §4).
    pub order_moves: u64,
    /// A runtime task's idle slices ([`ShardDb::idle_paced_step`]) and the work units they did.
    pub idle_slices: u64,
    pub idle_units: u64,
    /// The record cache's gets served and missed, its misses on keys its ghost named, and its
    /// bytes held and allowed, when the shard has one (set as the stats are taken).
    pub record_hits: u64,
    pub record_misses: u64,
    pub record_ghost_hits: u64,
    pub record_held: u64,
    pub record_bytes: u64,
    /// The packed memtables kept readable for gets, the bytes they hold, and the gets they
    /// answered ([`ShardDb`]'s kept memtables, research/51).
    pub kept: u64,
    pub kept_bytes: u64,
    pub kept_hits: u64,
    /// Of the kept memtables, those the index a get probes covers (research/51 §7).
    pub kept_covered: u64,
    /// Gets of keys no kept memtable held, answered without the trunk while they held every
    /// record it does (research/51 §8).
    pub kept_absent: u64,
    /// Puts whose trunk share waited for its input reads: the last memtable before a rotation
    /// would stall ([`ShardDb::put`]'s pacing).
    pub waited_steps: u64,
}

/// What the kept memtables say of a key: its newest entry's operation; that none holds it; or,
/// when the index named another key of its hash, nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeptAnswer {
    Found(Op),
    Absent,
    Unknown,
}

/// A memtable kept readable for gets: the memtable, its number among the kept ones (`crate::kept`
/// numbers them by a counter that wraps), and the entries its key list held, which giving it
/// back leaves in the indexes until a sweep.
#[derive(Debug)]
struct KeptMem {
    mem: HashMem,
    number: u32,
    keys: u64,
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

/// A full memtable being packed, read until it is: one whose arena copy waits for a worker to
/// sort and pack it (`sort`), handed to the first free; or one the shard packs itself a slice at
/// a time, its order paid and walked into its own `builder`, when the shard has no workers, the
/// memtable kept no copy, or it must be finished with no worker free.
#[derive(Debug)]
struct Packing {
    mem: HashMem,
    sort: Option<SortPack>,
    /// The walk in key order, opened once the memtable's order work is paid: its runs do not
    /// change after.
    walk: Option<Walk>,
    builder: Option<Builder>,
    packed: usize,
    /// When it began packing: its rotation.
    since: std::time::Instant,
}

/// A full memtable handed to the worker that sorts and packs it: still read until the worker's
/// branch is back and every older memtable's branch has gone to the trunk before it, since
/// pending branches are read newest first. Its job's ticket, the extents granted the job, the
/// worker's result once back, when it began packing, its first failure, and the bytes of its
/// sorted packing's buffers, out with the worker. A failed input stays readable and never
/// retires past its older predecessors ([`ShardDb::retire`]).
#[derive(Debug)]
struct Frozen {
    mem: HashMem,
    ticket: Ticket,
    grant: Vec<u64>,
    back: Option<Back>,
    since: std::time::Instant,
    error: Option<Error>,
    sorting: usize,
}

impl Frozen {
    /// Whether its worker result or retained failure is ready to report.
    fn ready(&self) -> bool {
        self.back.is_some() || self.error.is_some()
    }
}

impl Packing {
    /// The keys' worth of packing left: entries to pack, then the branch's filter pages, each at
    /// its worth in keys, for one the shard packs; one, the hand-off a put's share tries, for one
    /// waiting for a worker.
    fn left(&self) -> u64 {
        if self.sort.is_some() {
            return 1;
        }
        let entries = u64::try_from(self.mem.len().saturating_sub(self.packed)).unwrap_or(u64::MAX);
        let order = u64::try_from(self.mem.debt()).unwrap_or(u64::MAX);
        let filter = self
            .builder
            .as_ref()
            .map_or(0, |b| b.filter_pages_left().saturating_mul(b.page_keys()));
        filter.saturating_add(entries).saturating_add(order)
    }
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
    /// Packed memtables kept readable for gets, oldest first (research/51). Each one's branch is
    /// in the trunk, and every memtable packed after a kept one is kept, frozen, packing or
    /// active, so the first version a get finds newest first is the newest, and a key none of
    /// them holds is read from the trunk. They hold memory the budget leaves idle and give it
    /// back oldest first ([`Self::fit_kept`]), as F2's hot log evicts at its head.
    kept: std::collections::VecDeque<KeptMem>,
    /// The bytes the kept memtables and the key lists home hold ([`HashMem::memory`],
    /// `KeyList::bytes`).
    kept_bytes: usize,
    /// Gets a kept memtable answered, and gets of keys none held answered so while they hold
    /// every record the trunk does.
    kept_hits: u64,
    kept_absent: u64,
    /// The page cache's refusals for want of the machine's memory at the last look: one since
    /// gives a kept memtable back.
    kept_refused: u64,
    /// The most a get grows the caches by, a page cache chunk and a record cache page, taken at
    /// the last full look ([`Self::fit_kept`]): room kept memtables leave them.
    kept_step: usize,
    /// What the budget's regions held at the last look, and the kept memtables with their lists
    /// and indexes: more since, and they look again ([`Self::kept_crowded`]).
    kept_seen: usize,
    kept_seen_held: usize,
    /// The entry buffers the last kept memtable shed, for the next memtable a fill starts new.
    kept_shed: Option<crate::memtable::hashed::Bufs>,
    /// The key lists of kept memtables not yet taken by both indexes, oldest first.
    kept_lists: std::collections::VecDeque<(u32, crate::kept::KeyList)>,
    /// The index over kept memtables a get probes (research/51 §7), and the other, home while
    /// not out on a worker taking the memtables kept since it last went (`kept_out`); the two
    /// take turns, so a get never waits for an index and the shard never lends the one it reads.
    kept_index: Option<crate::kept::KeptIndex>,
    kept_spare: Option<crate::kept::KeptIndex>,
    kept_out: bool,
    /// The bytes the index out on a worker held when it went.
    kept_out_bytes: usize,
    /// The next kept memtable's number.
    kept_next: u32,
    /// Whether the kept memtables hold every record the trunk does: from a shard made empty, while
    /// every memtable packed has been kept and none given back. A key no memtable holds and no
    /// kept one holds is then held by none, and its get reads no further (research/51 §8).
    kept_whole: bool,
    /// Entries of the key lists of every memtable given back: an index swept when this much has
    /// gone stale since its last sweep is more than half of what it holds.
    kept_gone: u64,
    /// Key list buffers no index needs, for the next packings and lists.
    kept_free: Vec<crate::kept::KeyList>,
    /// Sorted packings back from their workers, their buffers for the next memtables' mirrors
    /// and sorts: no more than were ever out at once with the active memtable's, and at most a
    /// worker's packing each beside it ([`Self::keep_sort_pack`]).
    sort_spares: Vec<SortPack>,
    /// Whether the active memtable orders itself as it fills: only for scans, which read its
    /// order, since a worker sorts its mirror for its packing. A scan into it, or since the last
    /// rotation, has it ordered (research/40 §4).
    order_paced: bool,
    scans_at_rotation: u64,
    mem_limit: usize,
    trunk: Trunk,
    /// Work owed but not yet whole, in work·bytes over the room: the remainder of each debt's
    /// share.
    pack_carry: u128,
    /// The bytes paced puts wrote since their range's task last paid their shares
    /// ([`Self::take_results`]).
    turn_bytes: usize,
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
    /// The trunk lookups of the gets the record cache's ghost named since the last division, in
    /// nanoseconds: what a larger record cache would have saved.
    record_ghost_ns: u64,
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
    /// Scans made, the trunk's leaf segments they visited and the sources those listed.
    scans: u64,
    scan_segments: u64,
    scan_listed: u64,
    scan_end: Vec<u8>,
    /// Whether maintenance is timed ([`ShardDb::set_timed`]).
    timed: bool,
    /// A file kept as segments: the growth out on a worker, its segment's first extent and
    /// extents; the extents a segment adds, the largest grant asked and at least a packing's; the
    /// most a packing's grant wanted that the admitted extents could not give; the most any grant
    /// wanted that they could not give since the last segment was booked; the extents left when
    /// the growth now due first came due; and the most the allocations took while one was due,
    /// which the next segment is booked ahead of.
    growth: Option<(Ticket, u64, u64)>,
    segment_extents: u64,
    packing_wanted: u64,
    /// A growth landed at the last wait for room: a wait declared before it may now be met, so
    /// the runtime task passes again before it waits ([`Self::waiting_for_io`]); cleared as its
    /// next turn begins ([`Self::take_results`]).
    room_came: bool,
    growth_waited: u64,
    growth_due_at: Option<u64>,
    growth_lead: u64,
    /// A growth whose write failed: every look for room after it answers with that failure, so
    /// whichever call took the growth's result first, a put waiting for room or a turn's look,
    /// the range's next turn keeps it as its fault, and no growth is ever booked past it.
    growth_failed: Option<Error>,
}

/// Cited (Luo & Carey, PVLDB 2021 §5.4): a tuning step moves 5% of the memory budget toward
/// the region whose memory saves more, and takes at most 10% of the region giving it up, since
/// both regions' returns diminish.
pub(crate) const TUNE_STEP_PERCENT: usize = 5;
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
    if room == 0 {
        *carry = 0;
        return debt;
    }
    // In 64 bits while `debt · bytes` and the carry fit, as they do below 1.6·10^17 units of
    // debt for a 116-byte put: a 128-bit division is a call into the compiler's runtime, 1.8% of
    // a fill's shard thread (research/52).
    if let (Ok(b), Ok(r), Ok(c)) = (
        u64::try_from(bytes),
        u64::try_from(room),
        u64::try_from(*carry),
    ) && let Some(owed) = debt.checked_mul(b).and_then(|o| o.checked_add(c))
    {
        *carry = u128::from(owed.checked_rem(r).unwrap_or(0));
        return owed.checked_div(r).unwrap_or(0).min(debt);
    }
    let (bytes, room) = (
        u128::try_from(bytes).unwrap_or(u128::MAX),
        u128::try_from(room).unwrap_or(u128::MAX),
    );
    let owed = u128::from(debt)
        .saturating_mul(bytes)
        .saturating_add(*carry);
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
            kept: std::collections::VecDeque::new(),
            kept_bytes: 0,
            kept_hits: 0,
            kept_absent: 0,
            kept_refused: 0,
            kept_step: 0,
            kept_seen: 0,
            kept_seen_held: 0,
            kept_shed: None,
            kept_lists: std::collections::VecDeque::new(),
            kept_index: None,
            kept_spare: None,
            kept_out: false,
            kept_out_bytes: 0,
            kept_next: 0,
            kept_whole: false,
            kept_gone: 0,
            kept_free: Vec::new(),
            sort_spares: Vec::new(),
            order_paced: false,
            scans_at_rotation: 0,
            mem_limit,
            trunk,
            pack_carry: 0,
            turn_bytes: 0,
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
            record_ghost_ns: 0,
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
            scans: 0,
            scan_segments: 0,
            scan_listed: 0,
            scan_end: Vec::new(),
            timed: false,
            growth: None,
            growth_failed: None,
            segment_extents: 0,
            packing_wanted: 0,
            room_came: false,
            growth_waited: 0,
            growth_due_at: None,
            growth_lead: 0,
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
        let mut db = Self::with(Store::create(file, store)?, mem_limit, Trunk::new(trunk)?)?;
        db.grow_cold()?;
        // An empty trunk: every record it will hold comes from a packed memtable.
        db.kept_whole = true;
        Ok(db)
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
        let empty = recovered.root.is_none();
        let trunk = match recovered.root {
            Some(head) => Trunk::load(&mut store, head)?,
            None => Trunk::new(trunk)?,
        };
        let mut db = Self::with(store, mem_limit, trunk)?;
        db.grow_cold()?;
        // A trunk loaded holds records no kept memtable does.
        db.kept_whole = empty;
        Ok((db, recovered.applied))
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
        // The checkpoint's flush and its map's extents need the file's admitted extents, which
        // only this drives while the checkpoint steps.
        self.grow(false)?;
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
    /// rotation finds the trunk's root without room. Refused when the workers' channel back
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
        // The active memtable, still empty, keeps a copy for the worker that will sort it.
        if self.mem.is_empty() && !self.mem.mirrored() {
            let buf = self.mirror_buffer();
            if let Err(buf) = self.mem.mirror_into(buf) {
                self.spare_mirror(buf);
            }
        }
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

    /// The extents a store whose file grows by segments may use, and a segment being written
    /// whole past them ([`Store::segments`]); None for a file that grows by its writes.
    pub fn segments(&self) -> Option<crate::store::Segments> {
        self.store.segments()
    }

    /// The write memory the store has now ([`Self::set_write_budget`]).
    pub fn write_budget(&self) -> usize {
        self.store.write_budget()
    }

    /// Sizes the shard to the machine it runs on: one of `shares` of the host's effective
    /// capacity (its physical memory, clamped to the tightest OS, job or cgroup bound; hyper-rt
    /// `machine::facts`, §4.2) for its page cache, write memory and records together, divided
    /// between them by measured gains ([`Self::set_memory`]), and the cache taking memory only
    /// as reads want it and only while the machine has it free, asked before each chunk. The
    /// capacity is the stable bound; free memory, a fluctuating snapshot, gates each growth.
    pub fn follow_machine(&mut self, shares: usize) -> Result<(), Error> {
        let facts = hyper_rt::machine::facts::Facts::query();
        let capacity =
            hyper_rt::machine::facts::effective_capacity(facts.memory.total, facts.memory.limit);
        let budget = usize::try_from(capacity)
            .unwrap_or(usize::MAX)
            .checked_div(shares.max(1))
            .unwrap_or(0);
        self.store
            .follow_machine(hyper_rt::machine::facts::Facts::memory_available_now);
        self.set_memory(budget)
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
        self.record_ghost_ns = 0;
        self.partition_memory();
        // A budget smaller than the kept memtables hold has them give memory back now.
        self.fit_kept(true);
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
    /// the next rotation, each as [`HashMem::memory`] counts it, with their mirrors, and the
    /// sorted packings' buffers, out with workers or kept for the next. They sit beside the budget
    /// [`Self::set_memory`] divides, as RocksDB's write buffers sit beside its block cache.
    pub fn memtable_bytes(&self) -> usize {
        self.mem
            .memory()
            .saturating_add(self.packing.as_ref().map_or(0, |p| p.mem.memory()))
            .saturating_add(
                self.frozen
                    .iter()
                    .map(|f| f.mem.memory().saturating_add(f.sorting))
                    .fold(0, usize::saturating_add),
            )
            .saturating_add(self.spare.as_ref().map_or(0, HashMem::memory))
            .saturating_add(
                self.sort_spares
                    .iter()
                    .map(|p| p.memory())
                    .fold(0, usize::saturating_add),
            )
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

    /// Whether a region is still being brought down to a smaller share (`trim`), or the kept
    /// memtables hold more than the budget leaves them while an index of theirs is out on a
    /// worker, which gives its memory back only when it returns: until then the regions and the
    /// kept memtables may hold more than the budget by what is left to give back.
    pub fn trimming(&self) -> bool {
        self.trim_cache > 0
            || self.trim_records > 0
            || (self.kept_out && self.kept_held() > self.kept_limit())
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
        self.fit_kept(true);
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
        // A tuning step may have moved shares, write memory's among them: the kept memtables give
        // way at once.
        if self.kept_crowded() {
            self.fit_kept(false);
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
        // A record-cache ghost hit saves its trunk lookup: the lookups of the gets the ghost
        // named, each timed, are what a larger record cache would have saved this cycle.
        let records = Gain {
            saved_ns: std::mem::take(&mut self.record_ghost_ns),
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

    /// Sets the page cache's and the record cache's data limits from their shares, each less the
    /// index and ghost bytes it takes now, the charge `memory_held` counts until the next grant:
    /// a cache's data at its limit and that charge are within its share whatever either holds.
    /// A region whose limit fell is brought down to it by `trim`, a few pages an operation; a
    /// region whose share rose grows past what it holds only into the budget's room, which the
    /// other's trimming frees, and `trim` grants again after each of its steps. Trimming alone
    /// did not outpace a growing region's filling (a record cache took a 4 KiB page while the
    /// page cache still held 22 KiB above its share, research/50 §13), so memory moving between
    /// them now never holds more than the budget, or than it did when the move began.
    fn grant(&mut self) {
        let Some(budget) = self.memory else {
            return;
        };
        let page = self.store.page_size().max(1);
        let (cache_index, records_index) = self.index_bytes();
        self.charged = (cache_index, records_index);
        // A region grows past what it holds only into the budget's room now: while another still
        // holds what it is giving back (`trim`), the memory moves as it is freed, never held twice.
        let (cache_held, write_held, records_held) = self.memory_held();
        let mut room = budget.saturating_sub(
            cache_held
                .saturating_add(write_held)
                .saturating_add(records_held),
        );
        let cache = Self::sized(
            self.cache_share.saturating_sub(cache_index),
            cache_held.saturating_sub(cache_index),
            &mut room,
        );
        self.store
            .resize_cache(cache.checked_div(page).unwrap_or(0));
        if let Some(r) = self.records.as_mut() {
            let records = Self::sized(
                self.records_share.saturating_sub(records_index),
                r.held_bytes(),
                &mut room,
            );
            r.resize(records);
        }
    }

    /// A region's size from its `share`: the share when it holds that much or more, which it
    /// trims down to; else what it `held` and as much more toward the share as `room` has, taken
    /// from `room`.
    fn sized(share: usize, held: usize, room: &mut usize) -> usize {
        if share <= held {
            return share;
        }
        let grow = share.saturating_sub(held).min(*room);
        *room = room.saturating_sub(grow);
        held.saturating_add(grow)
    }

    /// An operation's share of bringing shrunk regions down to their size; what they gave back
    /// the growing regions may take ([`Self::grant`]).
    fn trim(&mut self) {
        // Its regions only: the kept memtables' part of `trimming` is given back by their index's
        // return, not here.
        if self.trim_cache == 0 && self.trim_records == 0 {
            return;
        }
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
        self.grant();
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
            if let Some(pool) = self.trunk.pool_mut() {
                for back in self.frozen.iter_mut().filter_map(|f| f.back.as_mut()) {
                    if let Some(error) = back.physical_error.take() {
                        pool.note_close_error(error);
                    }
                }
            }
            // A stopping shard keeps no memtable for gets: an index out on a worker comes back
            // with the pool's close and is dropped with it.
            self.give_back_kept();
            self.kept_out = false;
            self.kept_out_bytes = 0;
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
        // One hash of the key serves the record cache and the memtable.
        let h = crate::branch::filter::hash(key);
        // A write makes the key's cached record stale: it goes before the write is answered.
        if let Some(r) = self.records.as_mut() {
            r.invalidate(key, h);
        }
        let mut before = self.mem.bytes();
        let t = self.timed.then(std::time::Instant::now);
        match self.mem.insert_hashed(key, h, op, value) {
            Err(Error::LimitExceeded { .. }) if !self.mem.is_empty() => {
                let r = self.timed.then(std::time::Instant::now);
                self.rotate()?;
                let ns = ns_since(r);
                self.flush_stats.rotate_ns = self.flush_stats.rotate_ns.saturating_add(ns);
                before = self.mem.bytes();
                self.mem.insert_hashed(key, h, op, value)?;
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
        let h = crate::branch::filter::hash(key);
        let mut before = self.mem.bytes();
        match self.mem.insert_hashed(key, h, op, value) {
            Err(Error::LimitExceeded { .. }) if !self.mem.is_empty() => {
                if !self.rotate_paced(keys)? {
                    return Ok(false);
                }
                before = self.mem.bytes();
                self.mem.insert_hashed(key, h, op, value)?;
            }
            other => other?,
        }
        self.trim();
        if let Some(r) = self.records.as_mut() {
            r.invalidate(key, h);
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

    /// [`Self::scan`] made only if it can finish without waiting for the device or for a write
    /// still out over a page it needs: `Some` of its answer, or `None` when a page it needs is
    /// not in memory now, the rows it took and `next` back as they were and the scan uncounted,
    /// for the caller to take the paced path ([`Self::scan_async`]).
    pub(crate) fn scan_now(
        &mut self,
        from: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        out: &mut Rows,
        next: &mut Vec<u8>,
    ) -> Result<Option<bool>, Error> {
        let rows = out.len();
        let counted = (self.scans, self.scan_segments, self.scan_listed);
        self.store.begin_no_wait();
        let scanned = self.scan(from, end, limit, out, next);
        let refused = self.store.end_no_wait();
        match scanned {
            Ok(more) => Ok(Some(more)),
            Err(_) if refused => {
                out.truncate(rows);
                next.clear();
                (self.scans, self.scan_segments, self.scan_listed) = counted;
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
        // Then the kept memtables, packed but readable, newest first. When they hold every record
        // the trunk does, a key none holds is held by none.
        if found.is_none() && !self.kept.is_empty() {
            self.tend_kept()?;
            match self.kept_get(key, hash, value)? {
                KeptAnswer::Found(op) => {
                    found = Some(op);
                    self.kept_hits = self.kept_hits.saturating_add(1);
                }
                KeptAnswer::Absent if self.kept_whole => {
                    self.kept_absent = self.kept_absent.saturating_add(1);
                    value.clear();
                    if self.kept_crowded() {
                        self.fit_kept(false);
                    }
                    return Ok(false);
                }
                KeptAnswer::Absent | KeptAnswer::Unknown => {}
            }
        }
        let mut ghost = false;
        if found.is_none()
            && let Some(r) = self.records.as_mut()
        {
            match r.lookup(key, hash) {
                crate::records::Lookup::Hit(cached) => {
                    value.clear();
                    value.extend_from_slice(cached);
                    return Ok(true);
                }
                crate::records::Lookup::Miss { ghost: named } => ghost = named,
            }
        }
        if found.is_none() {
            // A miss the ghost named is timed: a larger record cache would have served it, so
            // its trunk lookup is what growing the cache saves (the tuner's marginal gain,
            // research/36). Other misses' lookups price nothing, and are not timed.
            let t = ghost.then(std::time::Instant::now);
            found = self.trunk.get_hashed(&mut self.store, key, hash, value)?;
            if let Some(r) = self.records.as_mut() {
                self.record_ghost_ns = self.record_ghost_ns.saturating_add(ns_since(t));
                if found == Some(Op::Put) {
                    r.insert(key, hash, value);
                }
            }
        }
        // The get may have grown the caches: the kept memtables give way before it returns.
        if self.kept_crowded() {
            self.fit_kept(false);
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
        // Then the kept memtables, packed but readable, newest first. When they hold every record
        // the trunk does, a key none holds is held by none.
        if found.is_none() && !self.kept.is_empty() {
            self.tend_kept()?;
            match self.kept_get(key, hash, value)? {
                KeptAnswer::Found(op) => {
                    found = Some(op);
                    self.kept_hits = self.kept_hits.saturating_add(1);
                }
                KeptAnswer::Absent if self.kept_whole => {
                    self.kept_absent = self.kept_absent.saturating_add(1);
                    value.clear();
                    if self.kept_crowded() {
                        self.fit_kept(false);
                    }
                    return Ok(false);
                }
                KeptAnswer::Absent | KeptAnswer::Unknown => {}
            }
        }
        let mut ghost = false;
        if found.is_none()
            && let Some(r) = self.records.as_mut()
        {
            match r.lookup(key, hash) {
                crate::records::Lookup::Hit(cached) => {
                    value.clear();
                    value.extend_from_slice(cached);
                    return Ok(true);
                }
                crate::records::Lookup::Miss { ghost: named } => ghost = named,
            }
        }
        if found.is_none() {
            // A miss the ghost named is timed: a larger record cache would have served it, so
            // its trunk lookup is what growing the cache saves (the tuner's marginal gain,
            // research/36). Other misses' lookups price nothing, and are not timed.
            let t = ghost.then(std::time::Instant::now);
            found = self
                .trunk
                .get_hashed_async(&mut self.store, key, hash, value)
                .await?;
            if let Some(r) = self.records.as_mut() {
                self.record_ghost_ns = self.record_ghost_ns.saturating_add(ns_since(t));
                if found == Some(Op::Put) {
                    r.insert(key, hash, value);
                }
            }
        }
        // The get may have grown the caches: the kept memtables give way before it returns.
        if self.kept_crowded() {
            self.fit_kept(false);
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
        let more = self.scan_inner(from, end, limit, out, next);
        // A scan may have read pages into the cache: the kept memtables give way before it
        // returns.
        if self.kept_crowded() {
            self.fit_kept(false);
        }
        more
    }

    fn scan_inner(
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
        // A frozen memtable a worker sorts kept no order of its own: its first scan sorts it,
        // once, as nothing writes it after. Scans from here have the active memtable ordered
        // as it fills, so the next seals sort little.
        for f in &mut self.frozen {
            f.mem.seal();
        }
        self.order_paced = true;
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
        // A cursor a frozen memtable, newest first; each sealed above, so none changes under the
        // scan.
        let mut frozen = std::mem::take(&mut self.scan_frozen);
        frozen.resize_with(self.frozen.len(), MemCursor::empty);
        for (c, f) in frozen.iter_mut().zip(self.frozen.iter().rev()) {
            c.seek(&f.mem, from)?;
        }
        self.scans = self.scans.saturating_add(1);
        let trunk = &self.trunk;
        let store = &mut self.store;
        let key = &mut self.scan_key;
        let scan_segments = &mut self.scan_segments;
        let scan_listed = &mut self.scan_listed;
        // The trunk's leaf segments, found one at a time from the seek's key: a segment's
        // sources (a branch each), and where the next starts.
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
        // Every error ends the walk here, so the merge gives its sources' pages and spans back to
        // the store before the scan returns it (`ScanMerge::close`): a scan refused a page that
        // would wait ([`Self::scan_now`]) holds none of the store's buffers after.
        let walked: Result<bool, Error> = 'walk: loop {
            // The trunk's next key: from the segment open, or the next segment's first.
            while merge.entry().is_none() {
                if trunk_done || !before_end(seg_from) {
                    break;
                }
                let bounded = match trunk.segment_at(seg_from, &mut sources, seg_end, passed) {
                    Ok(bounded) => bounded,
                    Err(error) => break 'walk Err(error),
                };
                *scan_segments = scan_segments.saturating_add(1);
                *scan_listed =
                    scan_listed.saturating_add(u64::try_from(sources.len()).unwrap_or(u64::MAX));
                // Each segment ends past its start, so the walk moves a leaf a step and ends.
                if bounded && seg_end.as_slice() <= seg_from.as_slice() {
                    break 'walk Err(Error::Corruption {
                        what: "a trunk segment's bounds",
                        why: crate::error::Malformed::OutOfOrder,
                    });
                }
                let hi = match (bounded, end) {
                    (true, Some(e)) if e < seg_end.as_slice() => Some(e),
                    (true, _) => Some(seg_end.as_slice()),
                    (false, e) => e,
                };
                // Sources are timed only when the descent gave more than the one a consolidated
                // path keeps: what earns rent.
                let spare = crate::trunk::earns_rent(passed);
                if let Err(error) = merge.open(store, &sources, seg_from, hi, end.is_some(), spare)
                {
                    break 'walk Err(error);
                }
                // Each pivot's rent: the measured time its sources took to open, all an index
                // pivot gave, all but the dearest of the deepest that gave any (research/50 §14).
                crate::trunk::rents(passed, merge.open_ns(), charges);
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
                break Ok(false);
            };
            key.clear();
            key.extend_from_slice(least);
            if taken == limit {
                next.extend_from_slice(key);
                break Ok(true);
            }
            // The newest source holding the key decides; every source holding it moves past it.
            let mut decided = false;
            if active.key() == Some(key.as_slice()) {
                decided = true;
                if active.op == Op::Put {
                    out.push(key, &active.value);
                    taken = taken.saturating_add(1);
                }
                if let Err(error) = active.advance(&self.mem) {
                    break Err(error);
                }
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
                if let Err(error) = c.advance(&p.mem) {
                    break Err(error);
                }
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
                if let Err(error) = c.advance(&f.mem) {
                    break 'walk Err(error);
                }
            }
            if let Some((k, op, v)) = merge.entry()
                && k == key.as_slice()
            {
                if !decided && op == Op::Put {
                    out.push(key, v);
                    taken = taken.saturating_add(1);
                }
                if let Err(error) = merge.next(store) {
                    break Err(error);
                }
            }
        };
        let more = match walked {
            Ok(more) => more,
            Err(error) => {
                merge.close(store);
                return Err(error);
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
            let (read_ns, write_ns) = self.store.page_costs();
            let pages = crate::trunk::PageCosts {
                read_ns,
                write_ns,
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
        let more = self.scan_async_inner(from, end, limit, out, next).await;
        // A scan may have read pages into the cache: the kept memtables give way before it
        // returns.
        if self.kept_crowded() {
            self.fit_kept(false);
        }
        more
    }

    async fn scan_async_inner(
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
        // A frozen memtable a worker sorts kept no order of its own: its first scan sorts it,
        // once, as nothing writes it after. Scans from here have the active memtable ordered
        // as it fills, so the next seals sort little.
        for f in &mut self.frozen {
            f.mem.seal();
        }
        self.order_paced = true;
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
        // A cursor a frozen memtable, newest first; each sealed above, so none changes under the
        // scan.
        let mut frozen = std::mem::take(&mut self.scan_frozen);
        frozen.resize_with(self.frozen.len(), MemCursor::empty);
        for (c, f) in frozen.iter_mut().zip(self.frozen.iter().rev()) {
            c.seek(&f.mem, from)?;
        }
        self.scans = self.scans.saturating_add(1);
        let trunk = &mut self.trunk;
        let key = &mut self.scan_key;
        let scan_segments = &mut self.scan_segments;
        let scan_listed = &mut self.scan_listed;
        // The trunk's leaf segments, found one at a time from the seek's key: a segment's
        // sources (a branch each), and where the next starts.
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
                *scan_segments = scan_segments.saturating_add(1);
                *scan_listed =
                    scan_listed.saturating_add(u64::try_from(sources.len()).unwrap_or(u64::MAX));
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
                // Sources are timed only when the descent gave more than the one a consolidated
                // path keeps: what earns rent.
                let spare = crate::trunk::earns_rent(passed);
                merge
                    .open(&sources, seg_from, hi, end.is_some(), spare)
                    .await?;
                // Each pivot's rent: the measured time its sources took to open, all an index
                // pivot gave, all but the dearest of the deepest that gave any (research/50 §14).
                crate::trunk::rents(passed, merge.open_ns(), charges);
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
            let (read_ns, write_ns) = self.store.page_costs();
            let pages = crate::trunk::PageCosts {
                read_ns,
                write_ns,
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
    /// A packing's grant: the memtable's bytes in extents, and one for the last partial one.
    fn packing_extents(&self) -> u64 {
        let run = self.store.run_bytes().max(1);
        let n = crate::util::div_ceil_usize(self.mem_limit, run)
            .unwrap_or(0)
            .saturating_add(1);
        u64::try_from(n).unwrap_or(u64::MAX)
    }

    /// A file kept as segments, cold (created, opened, no workers yet): a segment recovered past
    /// the admitted extents written whole again, and one segment's reserve admitted ahead, here
    /// and now. A segment is a packing's grant at first, the unit a fill grows by, and grows to
    /// the largest grant asked ([`Self::grow`]).
    fn grow_cold(&mut self) -> Result<(), Error> {
        let Some(seg) = self.store.segments() else {
            return Ok(());
        };
        self.segment_extents = self.packing_extents();
        self.store.set_growth_extents(self.segment_extents);
        if seg.pending.is_some() {
            self.store.grow_now(self.segment_extents)?;
        }
        if self.store.capacity() < self.segment_extents {
            self.store.grow_now(self.segment_extents)?;
        }
        Ok(())
    }

    /// Grows a file kept as segments ahead of its allocations (docs/research/48 §5): takes a
    /// growth back from its worker, waiting for it when `wait`, admits its segment and answers the
    /// asks for extents that waited for it; then, with fewer extents left than a segment and the
    /// lead, or than any grant waiting wants, books the next segment and hands it to a worker to
    /// add and write whole: a free one, or, when every seat is held, the worker whose ask for
    /// extents waits longest ([`Pool::send_growth`]). A segment is as large as the largest grant asked; the lead is the most
    /// the allocations took from the time a growth came due to its segment's admission, the wait
    /// for a free worker and any grant that waited included, so the next growth starts that far
    /// ahead. A larger segment would not help: it takes the longer to write. Without workers it
    /// grows in place, where the shard may wait. Returns whether it did anything.
    fn grow(&mut self, wait: bool) -> Result<bool, Error> {
        if self.store.segments().is_none() {
            return Ok(false);
        }
        if let Some(error) = &self.growth_failed {
            return Err(error.clone());
        }
        let mut did = false;
        if let Some((ticket, start, extents)) = self.growth {
            let pool = self.trunk.pool_mut().ok_or(Error::InvalidArgument {
                what: "a growth out with no workers",
            })?;
            if let Some(back) = pool.take(&mut self.store, Owner::Grow, wait)? {
                if back.ticket != ticket {
                    return Err(Error::InvalidArgument {
                        what: "a growth result for another growth",
                    });
                }
                // Its result is taken once: the failure stays, the growth no longer out.
                if let Some(error) = back
                    .physical_error
                    .or_else(|| back.result.as_ref().err().cloned())
                {
                    self.growth = None;
                    self.growth_failed = Some(error.clone());
                    return Err(error);
                }
                self.growth = None;
                self.store.growth_added();
                self.note_lead();
                self.store.admit(start, extents)?;
                did = true;
            }
        }
        let asked = self
            .trunk
            .take_capacity_wanted()
            .max(std::mem::take(&mut self.packing_wanted));
        let wanted = asked.max(self.trunk.pool_mut().map_or(0, |p| p.needs_wanted()));
        if let Some(pool) = self.trunk.pool_mut() {
            pool.answer_needs(&mut self.store)?;
        }
        if wanted > self.store.capacity() {
            self.growth_waited = self.growth_waited.max(wanted);
        }
        // What waits for room now: the asks declared since the last look, and the workers' asks
        // the admitted room could not answer. `growth_waited` keeps the most that waited since the
        // last booking, for the next segment's size and lead.
        let short = asked.max(self.trunk.pool_mut().map_or(0, |p| p.needs_wanted()))
            > self.store.capacity();
        if self.growth.is_some() || (!self.growth_due() && !short) {
            return Ok(did);
        }
        // Due ahead of need, nothing waiting for room, a growth leaves the last free seat to a
        // packing held for one: the packing fits the room admitted, the writer waits on it, and
        // the idle step hands it off next; the growth is booked at a later look.
        if !short
            && self.packing.as_ref().is_some_and(|p| p.sort.is_some())
            && self.trunk.pool_mut().is_some_and(|pool| pool.idle() <= 1)
        {
            return Ok(did);
        }
        // Due from now, if it was not already: the lead counts from the first look that found it.
        self.growth_due_at.get_or_insert(self.store.capacity());
        let waited = std::mem::take(&mut self.growth_waited);
        if waited > self.segment_extents {
            self.segment_extents = waited;
            self.store.set_growth_extents(self.segment_extents);
        }
        let Some(p) = self.store.begin_growth(self.segment_extents)? else {
            return Ok(did);
        };
        // Where the shard may wait it grows here, so no growth is out on a worker while its own
        // allocations need one ([`Store::grows_in_place`]); a runtime's shard hands it to one.
        if self.store.grows_in_place() {
            self.store.grow_segment(p.start, p.extents, !p.added)?;
            self.store.growth_added();
            self.note_lead();
            self.store.admit(p.start, p.extents)?;
            return Ok(true);
        }
        let Some(pool) = self.trunk.pool_mut() else {
            self.store.growth_abandoned();
            return Ok(did);
        };
        let job = Box::new(Job {
            work: Work::Grow(Growth {
                start: p.start,
                extents: p.extents,
                add: !p.added,
            }),
            grant: Vec::new(),
            file_end: self.store.end(),
            generation: self.store.generation(),
            keep: None,
        });
        let sent = pool.send_growth(&mut self.store, job);
        match sent {
            Ok(Ok(ticket)) => {
                self.growth = Some((ticket, p.start, p.extents));
                Ok(true)
            }
            Ok(Err(_)) => {
                pool.want_message();
                if self.store.grows_in_place() {
                    // Every worker is occupied and the shard may wait: grown here.
                    self.store.grow_segment(p.start, p.extents, !p.added)?;
                    self.store.growth_added();
                    self.note_lead();
                    self.store.admit(p.start, p.extents)?;
                    return Ok(true);
                }
                // The next look books it again.
                self.store.growth_abandoned();
                Ok(did)
            }
            Err((error, _)) => {
                self.store.growth_abandoned();
                Err(error)
            }
        }
    }

    /// Room found short at a runtime task's idle step: a packing's hand-off, a ready job's grant
    /// or a worker's ask wanted more extents than the file has admitted. The growth that brings
    /// them is booked now, where none is out and a worker is free ([`Self::grow`], which waits
    /// for a worker to free where none is), so it runs from the moment the room was wanted. A
    /// growth that landed meanwhile is progress, and the task passes again, its waiters taking
    /// the room; otherwise, while a growth is out, the task waits for a worker's message, that
    /// growth's result among them, rather than trying again at every pass. Called once the room's
    /// owners are held where a failure leaves them (`self.packing`, the trunk's tasks), from the
    /// step whose failure the range keeps as its fault.
    fn wait_for_room(&mut self) -> Result<(), Error> {
        let out = self.growth.map(|(ticket, ..)| ticket);
        self.grow(false)?;
        let landed = out.is_some() && self.growth.map(|(ticket, ..)| ticket) != out;
        if landed {
            // Its room may meet a wait declared before it, whichever message that wait named:
            // the task passes again, each waiter declaring afresh what it still waits for.
            self.room_came = true;
            self.trunk.clear_io_wait();
        } else if self.growth.is_some()
            && let Some(pool) = self.trunk.pool_mut()
        {
            pool.want_message();
        }
        Ok(())
    }

    /// Grows a file kept as segments until `extents` can be handed out, waiting for each growth:
    /// for the shard's own allocations where it may wait.
    fn grow_for(&mut self, extents: u64) -> Result<(), Error> {
        if self.store.segments().is_none() {
            return Ok(());
        }
        while self.store.capacity() < extents {
            self.packing_wanted = self.packing_wanted.max(extents);
            if !self.grow(true)? && self.growth.is_none() {
                return Err(Error::LimitExceeded {
                    what: "extents a store's file could be grown by",
                    limit: extents,
                });
            }
        }
        Ok(())
    }

    fn pace(&mut self, bytes: usize, paced: bool) -> Result<(), Error> {
        if !paced {
            self.grow(false)?;
        }
        let room = self.mem.room();
        // A memtable a worker will sort orders itself only for scans (research/40 §4); one the
        // shard packs sorts its own entries as it fills, in chunks of at most `order_bound`
        // entries paid by the puts that write them, so its packing owes the walk alone, and a
        // scan after any burst of puts sorts at most two chunks (HashMem::order_put).
        if self.order_paced || !self.mem.mirrored() {
            let t = self.timed.then(std::time::Instant::now);
            let moved = self.mem.order_put(1, self.order_bound, room);
            let ns = ns_since(t);
            let f = &mut self.flush_stats;
            f.order_ns = f.order_ns.saturating_add(ns);
            f.order_moves = f
                .order_moves
                .saturating_add(u64::try_from(moved).unwrap_or(u64::MAX));
        }
        if paced {
            // A range's task pays its turn's puts' shares at the turn, with the workers' results
            // ([`Self::take_results`]): one pass of the trunk's maintenance a turn, not a put
            // (research/52).
            self.turn_bytes = self.turn_bytes.saturating_add(bytes);
            return Ok(());
        }
        self.pay_shares(bytes, room, false)
    }

    /// Pays `bytes` of puts' shares of the work owed, `room` left in the memtable after them: the
    /// packing memtable's (or the workers' branches taken), the cache's forgetting and the
    /// trunk's maintenance, each a share of its debt by `bytes` over the room it is paced over.
    fn pay_shares(&mut self, bytes: usize, room: usize, paced: bool) -> Result<(), Error> {
        let left = self.pack_left();
        if left > 0 {
            // The memtable rotates when a put no longer fits, before its room reaches nothing:
            // packing is paced over the room as many bytes again leave, so it ends first.
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
        } else if self.trunk.maintaining() && self.trunk.has_pool() {
            // Maintenance's compactions are out on the workers: a step takes whatever results
            // have come and hands out what they let go, waiting for none.
            if paced {
                self.trunk.step_paced(&mut self.store, 1)?;
            } else {
                self.trunk.step(&mut self.store, 1)?;
            }
        }
        // A trunk step may have kept a compaction's pages in the cache: the kept memtables give
        // way at once. The index out on a worker is taken back at the next rotation, get or idle
        // step, not polled for by every put.
        if self.kept_crowded() {
            self.fit_kept(false);
        }
        Ok(())
    }

    /// The index over kept memtables, tended between operations: taken back if its worker is
    /// done, sent out if it lacks a list and a worker is free; a flag's look when neither.
    fn tend_kept(&mut self) -> Result<(), Error> {
        if self.kept_out {
            self.take_kept()?;
            // The index home may hold more than it did when it went: the kept memtables look
            // again at once.
            if self.kept_crowded() {
                self.fit_kept(false);
            }
        } else if !self.kept_lists.is_empty() {
            self.extend_kept()?;
        }
        Ok(())
    }

    /// Packs `w` keys' worth of the packing memtable: one waiting for a worker is handed to the
    /// first free; one the shard packs pays its order, then entries, then its filter pages at
    /// their worth in keys, its branch going to the trunk once whole. Workers' branches back are
    /// taken.
    fn pack_some(&mut self, w: u64, wait: bool) -> Result<(), Error> {
        if self.packing.as_ref().is_some_and(|p| p.sort.is_some()) {
            self.hand_off_sorted()?;
        }
        if w == 0 || self.packing.as_ref().is_none_or(|p| p.sort.is_some()) {
            // No share this put, or nothing the shard packs: a worker's branch is taken if it
            // is back, waited for when `wait`.
            return self.take_packs(wait);
        }
        let Some(p) = &mut self.packing else {
            return Ok(());
        };
        let t = self.timed.then(std::time::Instant::now);
        let mut w = w;
        // The memtable's order first: its sort and merges, a slice of the budget.
        if p.mem.debt() > 0 {
            let paid = p.mem.pay(usize::try_from(w).unwrap_or(usize::MAX));
            w = w.saturating_sub(u64::try_from(paid).unwrap_or(u64::MAX));
        }
        if p.mem.debt() == 0 && p.walk.is_none() {
            p.walk = Some(p.mem.walk_start());
        }
        if w == 0 {
            return Ok(());
        }
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

    /// The packing memtable waiting for a worker goes to one free now, frozen and read until its
    /// branch is back: whether one took it.
    fn hand_off_sorted(&mut self) -> Result<bool, Error> {
        let Some(mut p) = self.packing.take() else {
            return Ok(false);
        };
        let Some(pack) = p.sort.take() else {
            self.packing = Some(p);
            return Ok(false);
        };
        match self.sort_off(p.mem, pack, p.since)? {
            None => Ok(true),
            Some((mem, pack)) => {
                p.mem = mem;
                p.sort = Some(pack);
                self.packing = Some(p);
                Ok(false)
            }
        }
    }

    /// The packing still to do: the packing memtable's ([`Packing::left`]).
    fn pack_left(&self) -> u64 {
        self.packing.as_ref().map_or(0, Packing::left)
    }

    /// Takes the packing workers' branches that are back, waiting for one when `wait` and a
    /// memtable is frozen: each goes to its memtable, and those whose older memtables' are all
    /// in go to the trunk as pending, oldest first ([`Self::retire`]).
    fn take_packs(&mut self, wait: bool) -> Result<(), Error> {
        let mut wait = wait
            && self
                .frozen
                .iter()
                .any(|f| f.back.is_none() && f.error.is_none());
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
                        .any(|f| f.back.is_none() && f.error.is_none())
                {
                    pool.want_message();
                }
                return Ok(());
            };
            wait = false;
            let Some(f) = self.frozen.iter_mut().find(|f| f.ticket == back.ticket) else {
                return Err(Error::InvalidArgument {
                    what: "a packed branch for no frozen memtable",
                });
            };
            f.back = Some(back);
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
        let mut keys = None;
        let mut cuts = Vec::new();
        // The cache's room the packing kept its pages in: the trunk's to adopt with its branches,
        // given back on any failure.
        let kept = f.back.as_mut().and_then(|b| b.kept.take());
        let retired = (|| {
            if let Some(error) = f.error.clone() {
                return Err(error);
            }
            let back = f.back.as_mut().ok_or(Error::InvalidArgument {
                what: "a frozen input with no worker result",
            })?;
            if let Some(mut pack) = back.sorted.take() {
                f.sorting = 0;
                keys = Some(std::mem::take(&mut pack.keys));
                cuts = std::mem::take(&mut pack.cuts);
                self.keep_sort_pack(pack);
            }
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
            // A branch for each root pivot the memtable held keys of, each named by the pivot key
            // it begins at; one over every key when the packing had no cuts.
            if out.parts.is_empty() || (cuts.is_empty() && out.parts.len() != 1) {
                return Err(Error::InvalidArgument {
                    what: "a packing worker's result of other than its parts",
                });
            }
            let parts = std::mem::take(&mut out.parts);
            self.store.extend_end(out.end);
            Ok(parts)
        })();
        let parts = match retired {
            Ok(parts) => parts,
            Err(error) => {
                if let Some(kept) = kept {
                    self.store.unlend_pages(*kept);
                }
                f.error.get_or_insert_with(|| error.clone());
                self.terminal_error.get_or_insert_with(|| error.clone());
                self.frozen.push_front(f);
                return Err(error);
            }
        };
        let Frozen { mem, since, .. } = f;
        if cuts.is_empty() {
            if let Some(kept) = kept {
                self.store.unlend_pages(*kept);
            }
            for (_, branch) in parts {
                self.trunk.add(branch);
            }
        } else {
            let mut placed = Vec::with_capacity(parts.len());
            for (key, branch) in parts {
                let Ok(j) = cuts.binary_search(&key) else {
                    if let Some(kept) = kept {
                        self.store.unlend_pages(*kept);
                    }
                    return Err(Error::InvalidArgument {
                        what: "a packed part named by no pivot key it was cut at",
                    });
                };
                placed.push((j, branch));
            }
            self.trunk.add_parts(cuts, placed, kept);
        }
        self.pack_ns = u64::try_from(
            std::time::Instant::now()
                .saturating_duration_since(since)
                .as_nanos(),
        )
        .unwrap_or(u64::MAX);
        self.flush_stats.fed = self.flush_stats.fed.saturating_add(1);
        let t = self.timed.then(std::time::Instant::now);
        self.keep(mem, keys)?;
        let ns = ns_since(t);
        self.flush_stats.retire_ns = self.flush_stats.retire_ns.saturating_add(ns);
        self.flush_stats.flushes = self.flush_stats.flushes.saturating_add(1);
        Ok(())
    }

    /// A packed memtable, its branch in the trunk, with its key list when a worker packed it:
    /// kept readable for gets, newest last, the oldest given back while they hold more than
    /// [`Self::fit_kept`] allows, and its keys sent to the indexes over kept memtables. It is read
    /// through its index alone, so its entry buffers go to the next memtable a fill starts new.
    /// Without a memory budget, or without workers to build the indexes, none is kept, and none
    /// older stays: a get's probes would grow with the memtables kept (research/51 §7).
    fn keep(&mut self, mut mem: HashMem, keys: Option<crate::kept::KeyList>) -> Result<(), Error> {
        if self.memory.is_none() || !self.trunk.has_pool() {
            // A packed memtable not kept: the trunk holds records no kept one does.
            self.kept_whole = false;
            self.give_back_kept();
            if let Some(list) = keys {
                self.free_list(list);
            }
            self.recycle(mem);
            return Ok(());
        }
        let list = match keys {
            Some(list) => list,
            None => {
                // Packed on the shard: its list made here, from its index.
                let mut list = self.kept_free.pop().unwrap_or_default();
                if mem.key_list(&mut list).is_err() {
                    // An entry it cannot read is not kept: nor is any older.
                    self.kept_whole = false;
                    self.give_back_kept();
                    self.free_list(list);
                    self.recycle(mem);
                    return Ok(());
                }
                list
            }
        };
        self.kept_shed = Some(mem.shed());
        let number = self.kept_next;
        self.kept_next = number.wrapping_add(1);
        self.kept_bytes = self
            .kept_bytes
            .saturating_add(mem.memory())
            .saturating_add(list.bytes());
        let keys = u64::try_from(list.len()).unwrap_or(u64::MAX);
        self.kept.push_back(KeptMem { mem, number, keys });
        self.kept_lists.push_back((number, list));
        self.fit_kept(true);
        // The index out on a worker comes back here, a rotation's look, before the next goes.
        self.tend_kept()
    }

    /// Gives every kept memtable back, and the indexes over them: the trunk then holds records
    /// none does, if one was given back.
    fn give_back_kept(&mut self) {
        while let Some(k) = self.kept.pop_front() {
            self.kept_gone = self.kept_gone.saturating_add(k.keys);
            self.kept_whole = false;
            self.recycle(k.mem);
        }
        self.kept_bytes = 0;
        while let Some((_, list)) = self.kept_lists.pop_front() {
            self.free_list(list);
        }
        self.kept_index = None;
        self.kept_spare = None;
    }

    /// A key list no index needs: kept for the next packing's or list's, at most one a worker
    /// and the shard's own.
    fn free_list(&mut self, mut list: crate::kept::KeyList) {
        let most = self
            .trunk
            .pool_workers()
            .map_or(0, |(held, _)| held)
            .saturating_add(1);
        if self.kept_free.len() < most && list.bytes() > 0 {
            list.clear();
            self.kept_free.push(list);
        }
    }

    /// Sends the index that is home out to a free worker to take the key lists it lacks, oldest
    /// first: the spare, or a new one while there are fewer than two. One is out at a time.
    fn extend_kept(&mut self) -> Result<(), Error> {
        if self.kept_out || self.kept_lists.is_empty() || self.kept.is_empty() {
            return Ok(());
        }
        let newest = self.kept_lists.back().map(|(n, _)| *n);
        let lacks = |i: &crate::kept::KeptIndex| {
            newest.is_some_and(|n| i.covers().is_none_or(|c| crate::kept::older(c, n)))
        };
        // The spare when it lacks a list; with fewer than two indexes, a new one, which takes
        // every list home (none is freed before both indexes have it).
        let index = match self.kept_spare.take() {
            Some(spare) if lacks(&spare) => spare,
            Some(spare) => {
                self.kept_spare = Some(spare);
                return Ok(());
            }
            None => crate::kept::KeptIndex::new(),
        };
        // Within the pool's bound, as a packing is: the index is owed maintenance a get waits
        // on, not work the measured need for compactions sizes.
        let admitted = match self.trunk.pool_mut() {
            Some(pool) => pool.can_take(&mut self.store, true),
            None => Ok(false),
        };
        let Some(pool) = self
            .trunk
            .pool_mut()
            .filter(|_| matches!(admitted, Ok(true)))
        else {
            self.kept_spare = Some(index);
            return admitted.map(|_| ());
        };
        let oldest = self.kept.front().map_or(self.kept_next, |k| k.number);
        // Swept when the entries given back since its last sweep are more than half it holds.
        let stale = self.kept_gone.saturating_sub(index.swept_at());
        let sweep = stale.saturating_mul(2) > u64::try_from(index.len()).unwrap_or(u64::MAX);
        let bytes = index.bytes();
        let mut job = Box::new(crate::trunk::pool::IndexJob {
            index,
            lists: self.kept_lists.drain(..).collect(),
            oldest,
            sweep,
        });
        if sweep {
            job.index.set_swept_at(self.kept_gone);
        }
        let lists_bytes = job
            .lists
            .iter()
            .map(|(_, l)| l.bytes())
            .fold(0, usize::saturating_add);
        let job = Box::new(Job {
            work: Work::Index(job),
            grant: Vec::new(),
            file_end: self.store.end(),
            generation: self.store.generation(),
            keep: None,
        });
        match pool.send(job, Owner::Index, true) {
            Ok(Ok(_)) => {
                self.kept_out = true;
                self.kept_out_bytes = bytes.saturating_add(lists_bytes);
                self.kept_bytes = self.kept_bytes.saturating_sub(lists_bytes);
            }
            Ok(Err(job)) | Err((_, job)) => {
                // Not taken: everything back where it was.
                if let Work::Index(job) = job.work {
                    let crate::trunk::pool::IndexJob { index, lists, .. } = *job;
                    self.kept_lists = lists.into();
                    self.kept_spare = Some(index);
                }
            }
        }
        Ok(())
    }

    /// Takes the index back from its worker, if it is back: it serves gets when it covers newer
    /// memtables than the one serving, which becomes the spare; lists both have taken are freed,
    /// and the spare goes out again when lists remain that it lacks.
    fn take_kept(&mut self) -> Result<(), Error> {
        self.take_kept_waiting(false)
    }

    /// [`Self::take_kept`], waiting for the index when `wait` (where the shard may wait).
    fn take_kept_waiting(&mut self, wait: bool) -> Result<(), Error> {
        if !self.kept_out {
            return Ok(());
        }
        let Some(pool) = self.trunk.pool_mut() else {
            return Ok(());
        };
        let Some(back) = pool.take(&mut self.store, Owner::Index, wait)? else {
            return Ok(());
        };
        self.kept_out = false;
        self.kept_out_bytes = 0;
        back.result?;
        let Some(job) = back.indexed else {
            return Err(Error::InvalidArgument {
                what: "an index job's result with no index",
            });
        };
        let crate::trunk::pool::IndexJob { index, lists, .. } = *job;
        // The lists out with the index come before those kept since.
        let mut all: std::collections::VecDeque<_> = lists.into();
        all.extend(self.kept_lists.drain(..));
        let returned = all
            .iter()
            .map(|(_, l)| l.bytes())
            .fold(0, usize::saturating_add);
        self.kept_bytes = self.kept_bytes.saturating_add(returned);
        self.kept_lists = all;
        if self.kept.is_empty() {
            // Every memtable it covers was given back while it was out.
            self.give_back_kept();
            return Ok(());
        }
        let newer =
            |a: &crate::kept::KeptIndex, b: &crate::kept::KeptIndex| match (a.covers(), b.covers())
            {
                (Some(x), Some(y)) => crate::kept::older(y, x),
                (Some(_), None) => true,
                _ => false,
            };
        match self.kept_index.take() {
            Some(serving) if newer(&index, &serving) => {
                self.kept_index = Some(index);
                self.kept_spare = Some(serving);
            }
            Some(serving) => {
                self.kept_index = Some(serving);
                self.kept_spare = Some(index);
            }
            None => self.kept_index = Some(index),
        }
        self.prune_lists();
        self.extend_kept()
    }

    /// Frees the key lists both indexes have taken, and those of memtables given back.
    fn prune_lists(&mut self) {
        let oldest = self.kept.front().map(|k| k.number);
        let both = match (
            self.kept_index
                .as_ref()
                .and_then(crate::kept::KeptIndex::covers),
            self.kept_spare
                .as_ref()
                .and_then(crate::kept::KeptIndex::covers),
        ) {
            (Some(a), Some(b)) => Some(if crate::kept::older(a, b) { a } else { b }),
            _ => None,
        };
        while let Some(&(n, _)) = self.kept_lists.front() {
            let taken = both.is_some_and(|b| !crate::kept::older(b, n));
            let gone = oldest.is_none_or(|o| crate::kept::older(n, o));
            if !(taken || gone) {
                break;
            }
            if let Some((_, list)) = self.kept_lists.pop_front() {
                self.kept_bytes = self.kept_bytes.saturating_sub(list.bytes());
                self.free_list(list);
            }
        }
    }

    /// Gives kept memtables back, oldest first: while they hold more than the budget's idle
    /// memory ([`Self::kept_limit`]); one when the page cache was refused the machine's memory
    /// since the last look; and, with `machine` (a reading of the machine, made at rotations, not
    /// gets), while the next fill would take a new memtable and the machine has not twice its
    /// bytes free, the rule the page cache takes memory by (`Cache::machine_spares`). Without a
    /// memory budget none is kept.
    fn fit_kept(&mut self, machine: bool) {
        if machine {
            // Between two looks a get grows the page cache by at most a chunk and the record
            // cache by at most a page; a put's growth is looked at as its pacing ends.
            self.kept_step = self
                .store
                .cache_next_chunk_bytes()
                .saturating_add(self.store.page_size());
        }
        let refused = self.store.cache_refused();
        let mut short = refused > self.kept_refused;
        self.kept_refused = refused;
        let limit = self.kept_limit();
        let mut gave = false;
        while !self.kept.is_empty() {
            let over = self.kept_held() > limit;
            let machine_short = machine && self.spare.is_none() && !self.machine_spares();
            if !(over || short || machine_short) {
                break;
            }
            short = false;
            if let Some(k) = self.kept.pop_front() {
                self.kept_bytes = self.kept_bytes.saturating_sub(k.mem.memory());
                self.kept_gone = self.kept_gone.saturating_add(k.keys);
                self.recycle(k.mem);
                self.kept_whole = false;
                gave = true;
            }
        }
        if self.kept.is_empty() {
            // Nothing kept: the indexes home and the lists go too; one out on a worker is dropped
            // when it comes back.
            self.give_back_kept();
        } else if gave {
            self.prune_lists();
        }
        self.kept_seen = self.regions_held();
        self.kept_seen_held = self.kept_held();
    }

    /// What kept memtables, their key lists and their indexes hold, the index out on a worker
    /// counted as it went.
    fn kept_held(&self) -> usize {
        self.kept_bytes
            .saturating_add(
                self.kept_index
                    .as_ref()
                    .map_or(0, crate::kept::KeptIndex::bytes),
            )
            .saturating_add(
                self.kept_spare
                    .as_ref()
                    .map_or(0, crate::kept::KeptIndex::bytes),
            )
            .saturating_add(self.kept_out_bytes)
    }

    /// What the budget's regions hold together ([`Self::memory_held`]): a put's trunk step, a
    /// get and a tuning step grow them between rotations.
    fn regions_held(&self) -> usize {
        let (cache, write, records) = self.memory_held();
        cache.saturating_add(write).saturating_add(records)
    }

    /// Whether memtables are kept and, since their last look, a region or the kept memtables'
    /// indexes and lists took memory, or the page cache was refused the machine's: a few loads,
    /// made after each put, get and scan.
    fn kept_crowded(&self) -> bool {
        !self.kept.is_empty()
            && (self.regions_held() > self.kept_seen
                || self.kept_held() > self.kept_seen_held
                || self.store.cache_refused() > self.kept_refused)
    }

    /// The memory kept memtables may hold: the budget less what its regions hold and the most a
    /// get grows the caches by ([`Self::kept_step`]), so after any operation the regions and the
    /// kept memtables together hold no more than the budget; none without a budget.
    fn kept_limit(&self) -> usize {
        let Some(budget) = self.memory else {
            return 0;
        };
        let (cache, write, records) = self.memory_held();
        budget
            .saturating_sub(cache)
            .saturating_sub(write)
            .saturating_sub(records)
            .saturating_sub(self.kept_step)
    }

    /// Whether the machine has twice a memtable's bytes free, a new memtable's arena: true
    /// without a reading.
    fn machine_spares(&self) -> bool {
        self.store.machine_free().is_none_or(|free| {
            u64::try_from(self.mem_limit)
                .ok()
                .and_then(|b| b.checked_mul(2))
                .is_some_and(|need| free >= need)
        })
    }

    /// A memtable no longer read: cleared for the next fill when none is spare, else dropped,
    /// its memory back to the machine.
    fn recycle(&mut self, mut mem: HashMem) {
        if self.spare.is_none() {
            mem.clear();
            self.spare = Some(mem);
        }
    }

    /// The newest entry of `key` among the kept memtables: those the index a get probes does not
    /// cover, newest first, each through its own index, then the index, whose entry is checked:
    /// its memtable still kept, its key the one asked. Every probe's first group is read before
    /// any is probed, their misses overlapped.
    fn kept_get(&self, key: &[u8], hash: u64, value: &mut Vec<u8>) -> Result<KeptAnswer, Error> {
        let index = self.kept_index.as_ref();
        let covered = index.and_then(crate::kept::KeptIndex::covers);
        let uncovered = |k: &&KeptMem| covered.is_none_or(|c| crate::kept::older(c, k.number));
        let mut touched = index.map_or(0, |i| i.touch(hash));
        for k in self.kept.iter().rev().take_while(uncovered) {
            touched ^= k.mem.touch(hash);
        }
        std::hint::black_box(touched);
        for k in self.kept.iter().rev().take_while(uncovered) {
            if let Some(op) = k.mem.get_hashed(key, hash, value)? {
                return Ok(KeptAnswer::Found(op));
            }
        }
        let (Some(index), Some(oldest)) = (index, self.kept.front()) else {
            // No index: every kept memtable was probed above.
            return Ok(KeptAnswer::Absent);
        };
        let Some((number, offset)) = index.get(hash) else {
            return Ok(KeptAnswer::Absent);
        };
        if crate::kept::older(number, oldest.number) {
            // Its memtable was given back: the trunk holds the record.
            return Ok(KeptAnswer::Unknown);
        }
        let at = usize::try_from(number.wrapping_sub(oldest.number)).unwrap_or(usize::MAX);
        match self.kept.get(at) {
            Some(k) if k.number == number => Ok(k
                .mem
                .get_at(offset, key, value)?
                // Another key of its hash: the trunk answers.
                .map_or(KeptAnswer::Unknown, KeptAnswer::Found)),
            _ => Ok(KeptAnswer::Unknown),
        }
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
            // The frozen memtables pack in parallel on their workers: a wait is the slowest's.
            self.take_packs(true)?;
        }
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

    /// Whether maintenance is owed: a memtable packing, the trunk's work or a consolidation, the
    /// active memtable's runs to merge, freed pages the cache has yet to forget, or cache slots
    /// to reserve.
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
        self.growth.is_some()
            || self.growth_due()
            || self.packing.is_some()
            || !self.frozen.is_empty()
            || self.trunk.debt() > 0
            || !self.trunk.is_idle()
            || self.trunk.consolidation_owed()
            || (if paced {
                self.mem.debt() > 0
            } else {
                self.mem.untidy()
            })
            || self.tidy_paid()
            || self.store.forget_debt() > 0
            || self.store.cache_unreserved() > 0
            || self.kept_out
            || !self.kept_lists.is_empty()
            || self.quiet_unconsolidated()
    }

    /// The shard is quiet with a layout to consolidate that its writes have paid for: its
    /// memtable empty since a flush, no packing out or frozen, the trunk owing nothing, its
    /// layout not consolidated since branches last entered the root, and the writes since the
    /// last whole consolidation at least what one rewrites ([`Trunk::writes_cover_consolidation`]).
    /// A fill keeps its memtable full, so none is quiet while one runs.
    fn quiet_unconsolidated(&self) -> bool {
        self.mem.is_empty()
            && self.packing.is_none()
            && self.frozen.is_empty()
            && self.trunk.debt() == 0
            && self.trunk.is_idle()
            && self.trunk.unconsolidated()
            && self.trunk.writes_cover_consolidation()
    }

    /// Pays up to `keys` work units of maintenance owed, for the shard's idle time (SILK: the
    /// flush first, since a full memtable stops puts; then the trunk's work; then the
    /// consolidations seeks paid for; then the active memtable's order; then the cache's
    /// forgetting and its slots). Returns the work done, 0 once nothing is owed.
    pub fn idle_step(&mut self, keys: u64) -> Result<u64, Error> {
        self.idle_step_with(keys, false)
    }

    /// A range task's turn: what the workers sent back since its last turn is taken, a growth
    /// back or booked once due and packed branches retired, their needs answered, and the shares
    /// of the work owed that the turn's puts wrote paid, once a turn rather than at every put
    /// (research/52). Nothing while a checkpoint holds the trunk still.
    pub(crate) fn take_results(&mut self) -> Result<(), Error> {
        self.room_came = false;
        self.require_async_backend()?;
        if self.checkpoint_frozen() {
            return Ok(());
        }
        self.grow(false)?;
        let bytes = std::mem::take(&mut self.turn_bytes);
        if bytes > 0 {
            let room = self.mem.room();
            return self.pay_shares(bytes, room, true);
        }
        if !self.frozen.is_empty() {
            self.take_packs(false)?;
        }
        // A retired branch's pages may have gone to the cache: the kept memtables give way.
        if self.kept_crowded() {
            self.fit_kept(false);
        }
        Ok(())
    }

    /// A runtime task's idle slice: a required trunk read that has not landed leaves its
    /// work owed, so the task can await an attachment completion or another request.
    pub(crate) fn idle_paced_step(&mut self, keys: u64) -> Result<u64, Error> {
        self.require_async_backend()?;
        if self.checkpoint_frozen() {
            return Ok(0);
        }
        self.trunk.clear_io_wait();
        let done = self.idle_step_with(keys, true)?;
        // A hand-off, a grant or an ask left waiting for room: its growth is booked before the task
        // can wait, and the wait is for that growth's result.
        if self.packing_wanted > 0 || self.trunk.wants_room() {
            self.wait_for_room()?;
        }
        let f = &mut self.flush_stats;
        f.idle_slices = f.idle_slices.saturating_add(1);
        f.idle_units = f.idle_units.saturating_add(done);
        Ok(done)
    }

    pub(crate) fn waiting_for_io(&self) -> bool {
        !self.room_came
            && (self.trunk.waiting_for_worker()
                || ((self.stopping
                    || self.rotation.is_some()
                    || self.checkpoint.is_some()
                    || self.trunk.waiting_for_io())
                    && self.store.io_outstanding()))
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

    /// Whether a file kept as segments has fewer extents left than a segment and the lead, and no
    /// growth out.
    fn growth_due(&self) -> bool {
        self.growth.is_none()
            && self.store.segments().is_some()
            && self.store.capacity() < self.segment_extents.saturating_add(self.growth_lead)
    }

    /// A growth's segment is about to be admitted: what the allocations took since it came due,
    /// and the most any grant asked of the extents left while it was out, is the lead the next
    /// is booked ahead of, if it is the most yet.
    fn note_lead(&mut self) {
        if let Some(due_at) = self.growth_due_at.take() {
            let took = due_at
                .saturating_sub(self.store.capacity())
                .saturating_add(self.growth_waited);
            self.growth_lead = self.growth_lead.max(took);
        }
    }

    fn idle_step_with(&mut self, keys: u64, paced: bool) -> Result<u64, Error> {
        let done = self.idle_step_inner(keys, paced)?;
        // Idle work may have kept a compaction's pages or a page read: the kept memtables give
        // way before the step returns.
        if self.kept_crowded() {
            self.fit_kept(false);
        }
        Ok(done)
    }

    fn idle_step_inner(&mut self, keys: u64, paced: bool) -> Result<u64, Error> {
        // A file kept as segments grows first: the room every allocation after needs.
        if (self.growth.is_some() || self.growth_due()) && self.grow(!paced)? {
            return Ok(1);
        }
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
            self.take_packs(!paced)?;
            return Ok(1);
        }
        if self.trunk.debt() > 0 || !self.trunk.is_idle() {
            let t = self.timed.then(std::time::Instant::now);
            let used = if paced {
                self.trunk.step_paced(&mut self.store, keys)?
            } else {
                self.trunk.idle_step(&mut self.store, keys)?
            };
            if self.timed {
                self.note_trunk(ns_since(t));
            }
            return Ok(used.max(1));
        }
        if self.kept_out || !self.kept_lists.is_empty() {
            // The index over kept memtables: back from its worker, waited for where the shard may
            // wait, or sent out with the lists it lacks. With no worker free, the work below
            // goes on, its results freeing one.
            if self.kept_out {
                let out = self.kept_out;
                self.take_kept_waiting(!paced)?;
                if out != self.kept_out {
                    return Ok(1);
                }
            } else {
                self.extend_kept()?;
                if self.kept_out {
                    return Ok(1);
                }
            }
        }
        if self.quiet_unconsolidated() {
            // Nothing written since a flush emptied the memtable, nothing else owed, and the
            // writes since the last whole consolidation have paid for one: the whole trunk
            // consolidates now rather than pivot by pivot as seeks pay (research/53 §11).
            self.trunk.consolidate_all();
        }
        if self.trunk.consolidation_owed() {
            // Seeks paid for consolidating leaves: one consolidation, in bulk, flushes and settles
            // them.
            let t = self.timed.then(std::time::Instant::now);
            let used = self.trunk.consolidate_step(&mut self.store, keys, paced)?;
            if self.timed {
                self.note_trunk(ns_since(t));
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
        // One waiting for a worker goes to one free now; with none free, the shard packs it.
        if self.packing.as_ref().is_some_and(|p| p.sort.is_some()) && self.hand_off_sorted()? {
            return self.drain_frozen();
        }
        let Some(p) = self.packing.as_mut() else {
            return self.drain_frozen();
        };
        let t = self.timed.then(std::time::Instant::now);
        if let Some(pack) = p.sort.take() {
            p.mem.close();
            let entries = u64::try_from(p.mem.len()).unwrap_or(u64::MAX);
            // The memtable's bytes in extents, and one for the last partial one: a file kept as
            // segments grows first, waited for, as the shard's own allocations may.
            let run = self.store.run_bytes().max(1);
            let extents = crate::util::div_ceil_usize(p.mem.bytes(), run)
                .unwrap_or(0)
                .saturating_add(1);
            self.keep_sort_pack(pack);
            self.grow_for(u64::try_from(extents).unwrap_or(u64::MAX))?;
            let builder = Builder::new(&mut self.store, Keys::Exactly(entries))?;
            if let Some(p) = self.packing.as_mut() {
                p.builder = Some(builder);
            }
        }
        let Some(p) = self.packing.as_mut() else {
            return self.drain_frozen();
        };
        // The order work left, whole, then the walk from where packing left it.
        p.mem.pay(usize::MAX);
        if p.walk.is_none() {
            p.walk = Some(p.mem.walk_start());
        }
        // Built here: after every older memtable's branch, pending in age order.
        self.drain_frozen()?;
        let (mem, walk, mut builder) = match self.packing.take() {
            Some(Packing {
                mem,
                walk,
                builder: Some(builder),
                ..
            }) => (mem, walk, builder),
            Some(other) => {
                // No builder: kept, never dropped with its entries.
                self.packing = Some(other);
                return Err(Error::InvalidArgument {
                    what: "a packing finished with no builder",
                });
            }
            None => return Ok(()),
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
        self.keep(mem, None)?;
        let ns = ns_since(t);
        self.flush_stats.retire_ns = self.flush_stats.retire_ns.saturating_add(ns);
        self.pack_carry = 0;
        self.flush_stats.flushes = self.flush_stats.flushes.saturating_add(1);
        Ok(())
    }

    /// The memtable the next fill takes: the spare one, else a new one, given a mirror when
    /// workers will pack it ([`Self::give_mirror`]).
    fn fresh_mem(&mut self) -> Result<HashMem, Error> {
        let mut fresh = match self.spare.take() {
            Some(m) => m,
            // Sized for the entries the memtable it follows holds, with the entry buffers a kept
            // memtable shed: it fills without its index or buffers growing, as a cleared spare
            // refills.
            None => {
                let mut fresh = HashMem::new_for(self.mem_limit, self.mem.len())?;
                if let Some(bufs) = self.kept_shed.take() {
                    fresh.take_shed(bufs);
                }
                fresh
            }
        };
        self.give_mirror(&mut fresh);
        Ok(fresh)
    }

    /// Gives an empty memtable a copy of its arena to keep for the worker that will sort and
    /// pack it (research/40 §4), when the shard has workers: a spare packing's buffer, else a new
    /// one. Refused its room, the memtable fills without one and the shard packs it.
    fn give_mirror(&mut self, mem: &mut HashMem) {
        if mem.mirrored() || !self.trunk.has_pool() {
            return;
        }
        let buf = self.mirror_buffer();
        if let Err(buf) = mem.mirror_into(buf) {
            self.spare_mirror(buf);
        }
    }

    /// A spare packing's arena buffer, else a new one.
    fn mirror_buffer(&mut self) -> Vec<u8> {
        self.sort_spares
            .iter_mut()
            .find(|p| p.arena.capacity() > 0)
            .map(|p| std::mem::take(&mut p.arena))
            .unwrap_or_default()
    }

    /// Keeps an arena buffer a memtable did not take in a spare packing that has none.
    fn spare_mirror(&mut self, buf: Vec<u8>) {
        if let Some(p) = self
            .sort_spares
            .iter_mut()
            .find(|p| p.arena.capacity() == 0)
        {
            p.arena = buf;
        }
    }

    /// Keeps a sorted packing back from its worker for the next, emptied: no more spares than a
    /// worker's packing each, and the active memtable's.
    fn keep_sort_pack(&mut self, mut pack: SortPack) {
        let most = self
            .trunk
            .pool_workers()
            .map_or(0, |(held, _)| held)
            .saturating_add(1);
        if self.sort_spares.len() < most {
            pack.arena.clear();
            pack.entries = 0;
            self.sort_spares.push(pack);
        }
    }

    /// A full memtable goes to its packing: to a worker that sorts the copy of its arena it kept
    /// (or one made now, for a memtable filled before the shard had workers), frozen at once and
    /// read until its branch is back; with none free now, it waits as the packing memtable for
    /// the first free ([`Self::hand_off_sorted`]). Without workers, or with the copy's memory
    /// refused, the shard packs it, its order a debt paid a slice at a time. Scans since the last rotation have the next memtable ordered as it fills.
    fn pack_full(&mut self, mut full: HashMem) -> Result<(), Error> {
        // Rent paid on one memtable's runs buys no merge of the next's.
        self.tidy_rent_ns = 0;
        self.order_paced = self.scans != self.scans_at_rotation;
        self.scans_at_rotation = self.scans;
        let since = std::time::Instant::now();
        let arena = match full.take_mirror() {
            Some(arena) => Some(arena),
            // Filled before the shard had workers, or its copy's room refused as it began: its
            // arena copied now, once, so a worker still sorts it; the shard packs a memtable
            // only without workers, or with this copy's memory refused too.
            None if self.trunk.has_pool() => {
                let mut buf = self.mirror_buffer();
                match full.copy_arena(&mut buf) {
                    Ok(()) => Some(buf),
                    Err(_) => {
                        self.spare_mirror(buf);
                        None
                    }
                }
            }
            None => None,
        };
        if let Some(arena) = arena {
            let mut pack = self.sort_spares.pop().unwrap_or_default();
            pack.entries = u64::try_from(full.len()).unwrap_or(u64::MAX);
            pack.skip = full.skip();
            pack.arena = arena;
            if let Some((mem, pack)) = self.sort_off(full, pack, since)? {
                self.packing = Some(Packing {
                    mem,
                    sort: Some(pack),
                    walk: None,
                    builder: None,
                    packed: 0,
                    since,
                });
            }
            return Ok(());
        }
        full.close();
        self.packing = Some(self.packing_of(full)?);
        Ok(())
    }

    /// Hands `full` and its arena's copy in `pack` to a worker that sorts and packs it, the
    /// memtable frozen at once, read until its branch is back: None. Both back when no worker
    /// takes it now, or the file must grow first.
    fn sort_off(
        &mut self,
        full: HashMem,
        mut pack: SortPack,
        since: std::time::Instant,
    ) -> Result<Option<(HashMem, SortPack)>, Error> {
        // The packing ends a branch at each of the root's pivot keys, each branch then that
        // pivot's (research/57 §7), and each branch begins an extent.
        self.trunk.root_cuts(&mut pack.cuts);
        let run = self.store.run_bytes().max(1);
        let extents = crate::util::div_ceil_usize(full.bytes(), run)
            .unwrap_or(0)
            .saturating_add(1)
            .saturating_add(pack.cuts.len());
        let asked = u64::try_from(extents).unwrap_or(u64::MAX);
        // The seat first: taking what the workers sent answers their asks for extents, which
        // take from the extents counted next.
        let admitted = match self.trunk.pool_mut() {
            Some(pool) => pool.can_take(&mut self.store, true)?,
            None => false,
        };
        if self.store.segments().is_some() && self.store.capacity() < asked {
            self.packing_wanted = self.packing_wanted.max(asked);
            return Ok(Some((full, pack)));
        }
        let Some(pool) = self.trunk.pool_mut().filter(|_| admitted) else {
            if let Some(pool) = self.trunk.pool_mut() {
                // Every worker busy: a completion frees the one this packing needs.
                pool.want_message();
            }
            return Ok(Some((full, pack)));
        };
        let grant = self.store.grant(extents)?;
        let sorting = pack.memory();
        // A packing cut at the root's pivot keys may become their bundles' branches as written,
        // so it keeps its pages in the cache's room as a compaction does, at most its grant's.
        let keep = if pack.cuts.is_empty() {
            None
        } else {
            let pages = usize::try_from(self.store.extent_pages()).unwrap_or(usize::MAX);
            self.store.lend_pages(extents.saturating_mul(pages))
        };
        let job = Box::new(Job {
            work: Work::Sort(pack),
            grant: grant.clone(),
            file_end: self.store.end(),
            generation: self.store.generation(),
            keep,
        });
        let sent = match pool.send(job, Owner::Pack, true) {
            Ok(sent) => sent,
            Err((error, job)) => {
                let job = *job;
                if let Some(kept) = job.keep {
                    self.store.unlend_pages(*kept);
                }
                self.store.grant_back(&job.grant)?;
                return Err(error);
            }
        };
        let ticket = match sent {
            Ok(ticket) => ticket,
            Err(job) => {
                let mut job = *job;
                if let Some(kept) = job.keep.take() {
                    self.store.unlend_pages(*kept);
                }
                self.store.grant_back(&job.grant)?;
                let Job { work, .. } = job;
                let Work::Sort(pack) = work else {
                    return Err(Error::InvalidArgument {
                        what: "a sorted packing's job came back as other work",
                    });
                };
                return Ok(Some((full, pack)));
            }
        };
        self.frozen.push_back(Frozen {
            mem: full,
            ticket,
            grant,
            back: None,
            since,
            error: None,
            sorting,
        });
        let held = u64::try_from(self.frozen.len()).unwrap_or(u64::MAX);
        self.flush_stats.frozen_most = self.flush_stats.frozen_most.max(held);
        self.pack_carry = 0;
        Ok(None)
    }

    /// A full memtable as the packing one the shard packs itself: its builder now, its order a
    /// debt paid a slice at a time.
    fn packing_of(&mut self, mem: HashMem) -> Result<Packing, Error> {
        let builder = Builder::new(
            &mut self.store,
            Keys::Exactly(u64::try_from(mem.len()).unwrap_or(u64::MAX)),
        )?;
        Ok(Packing {
            mem,
            sort: None,
            walk: None,
            builder: Some(builder),
            packed: 0,
            since: std::time::Instant::now(),
        })
    }

    /// The same rotation as the blocking API, paid in the task's existing slice budget.
    /// Only old packing and maintenance state changes before it returns true; the caller's entry
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
            let finish_packing = self.packing.is_some();
            self.rotation = Some(Rotation {
                started: now,
                finish_packing,
                most: None,
                stalled: finish_packing,
            });
        }
        if self.rotation.as_ref().is_some_and(|r| r.finish_packing) {
            if self.packing.is_some() {
                // Still packing at the rotation that needs its place: one waiting for a worker
                // goes to the first free, results already in taken first to free theirs; one the
                // shard packs pays its slices. The measured overlap below decides what else is
                // waited for.
                self.take_packs(false)?;
                self.pack_some(keys.max(1), false)?;
                if self.packing.is_some() {
                    return Ok(false);
                }
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
            self.take_packs(false)?;
            if self.frozen.len() > most {
                return Ok(false);
            }
        }
        if !self.trunk.has_room() {
            if let Some(r) = self.rotation.as_mut() {
                r.stalled = true;
            }
            if self.trunk.packing_blocks_dispatch(&mut self.store)? {
                // Packings hold every seat: their results free them. Take any back before
                // waiting on maintenance's worker capacity.
                self.pack_some(keys.max(1), false)?;
                self.take_packs(false)?;
                if self.trunk.packing_blocks_dispatch(&mut self.store)? {
                    return Ok(false);
                }
            }
            if !self.trunk.make_room_paced(&mut self.store, keys.max(1))? {
                return Ok(false);
            }
        }
        let fresh = self.fresh_mem()?;
        let full = std::mem::replace(&mut self.mem, fresh);
        self.pack_full(full)?;
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

    /// Packs every memtable and finishes its maintenance before a range starts durability.
    /// A false result owns all unfinished work.
    pub(crate) fn flush_paced(&mut self, keys: u64) -> Result<bool, Error> {
        self.trunk.clear_io_wait();
        self.require_async_backend()?;
        // A job the file's admitted extents cannot hold waits for growth, which only this
        // drives while the flush steps the trunk.
        self.grow(false)?;
        if self.rotation.is_some() && !self.rotate_paced(keys)? {
            return Ok(false);
        }
        if self.packing.is_some() || !self.frozen.is_empty() {
            self.pack_some(keys.max(1), false)?;
            self.take_packs(false)?;
            return Ok(false);
        }
        if !self.mem.is_empty() {
            let fresh = self.fresh_mem()?;
            let full = std::mem::replace(&mut self.mem, fresh);
            self.pack_full(full)?;
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
    /// trunk's maintenance when its root has no room for more branches.
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
        if self.packing.is_some() && !self.hand_off_sorted()? {
            // No worker free for it, or one the shard packs: built here, its branch after the
            // older ones'.
            self.finish_packing()?;
            stalled = true;
        }
        // Whatever froze them (this rotation, or a put's share that fed one whole), no more stay
        // frozen than the measure says overlap: the oldest are waited for.
        let most = self.frozen_most();
        while self.frozen.len() > most {
            self.take_packs(true)?;
            stalled = true;
        }
        if !self.trunk.has_room() {
            if self.trunk.packing_blocks_dispatch(&mut self.store)? {
                // The root's compactions cannot admit a job until a packing seat is free: the
                // oldest packing's result frees one.
                self.take_packs(true)?;
            }
            self.trunk.make_room(&mut self.store)?;
            stalled = true;
        }
        if stalled {
            let ns = ns_since(t);
            let f = &mut self.flush_stats;
            f.stalls = f.stalls.saturating_add(1);
            f.stall_ns = f.stall_ns.saturating_add(ns);
            f.stall_max_ns = f.stall_max_ns.max(ns);
        }
        let fresh = self.fresh_mem()?;
        let full = std::mem::replace(&mut self.mem, fresh);
        self.pack_full(full)?;
        self.rotated = Some(std::time::Instant::now());
        Ok(())
    }

    /// Pays maintenance owed, for the shard's idle time: the packing memtable's whole, then up
    /// to `budget` work units of the trunk's (initial leaf moves, merged keys and filter pages),
    /// then the cache's freed pages with the budget left.
    /// Returns the work done, less than `budget` only once nothing is owed.
    pub fn maintain(&mut self, budget: u64) -> Result<u64, Error> {
        self.finish_packing()?;
        let t = self.timed.then(std::time::Instant::now);
        let mut used = self.trunk.step(&mut self.store, budget)?;
        if self.timed {
            self.note_trunk(ns_since(t));
        }
        // Then a consolidation of every pivot of several sources, each step at least one unit
        // while owed: a drain settles the layout so each seek opens one source, as bulk
        // preparation measured 3.6x the seeks of the layout it replaces (Codex's
        // mantle-bounded-consolidation-experiment-20261008).
        self.trunk.consolidate_all();
        while used < budget && (self.trunk.consolidation_owed() || self.trunk.maintaining()) {
            // A drain's whole budget stays whole, so each step waits for the workers'
            // compactions rather than return to be called again.
            let left = if budget == u64::MAX {
                u64::MAX
            } else {
                budget.saturating_sub(used)
            };
            let done = self.trunk.consolidate_step(&mut self.store, left, false)?;
            used = used.saturating_add(done.max(1));
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
            let fresh = self.fresh_mem()?;
            let full = std::mem::replace(&mut self.mem, fresh);
            self.pack_full(full)?;
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
        let mut flush = self.flush_stats;
        let as_u64 = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
        if let Some(r) = self.records.as_ref() {
            (flush.record_hits, flush.record_misses) = r.stats();
            flush.record_ghost_hits = r.ghost_hits();
            flush.record_held = as_u64(r.held_bytes());
            flush.record_bytes = as_u64(r.bytes());
        }
        flush.kept = as_u64(self.kept.len());
        flush.kept_bytes = as_u64(self.kept_held());
        flush.kept_hits = self.kept_hits;
        flush.kept_absent = self.kept_absent;
        let covered = self
            .kept_index
            .as_ref()
            .and_then(crate::kept::KeptIndex::covers);
        flush.kept_covered = as_u64(
            self.kept
                .iter()
                .filter(|k| covered.is_some_and(|c| !crate::kept::older(c, k.number)))
                .count(),
        );
        let mut trunk = self.trunk.stats();
        trunk.scans = self.scans;
        trunk.scan_segments = self.scan_segments;
        trunk.scan_sources = self.scan_listed;
        trunk.scan_skipped = self.scan_skipped;
        trunk.scan_opened = self.scan_opened;
        (flush, trunk, self.store.io_stats())
    }

    /// Shadow mode for the store's resident reads' kept answers, for measurement
    /// ([`Store::set_residency_shadow`]).
    pub fn set_residency_shadow(&mut self, on: bool) {
        self.store.set_residency_shadow(on);
    }

    /// The bytes the trunk's branches hold in memory.
    pub fn memory(&self) -> crate::trunk::Memory {
        self.trunk.memory()
    }

    /// Trunk sources scans have passed over by their range filters, and those they opened.
    pub fn scan_filtered(&self) -> (u64, u64) {
        (self.scan_skipped, self.scan_opened)
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
    //! one it waits for that worker; released, the branches enter the trunk oldest first; a
    //! worker's failure is kept, typed and fenced, never dropping what the memtables hold; and
    //! the store reopens at its last checkpoint.
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

    /// Drains every packing and the trunk's maintenance, so every worker is free and the next
    /// packing goes to the first seat; stalls that seat's writes, failing the first after the
    /// stall when `fail`; puts through the rotation that hands it the next memtable, and waits
    /// until it waits in a write. Returns that worker.
    fn stall_oldest(
        db: &mut ShardDb<Held>,
        oracle: &mut BTreeMap<Vec<u8>, Vec<u8>>,
        i: &mut u64,
        put: Put,
        gates: &'static Gates,
        fail: bool,
    ) -> usize {
        db.flush().unwrap();
        assert!(db.frozen.is_empty() && db.packing.is_none());
        let worker = 0;
        gates.arm(worker, fail);
        let rotations = db.flush_stats.rotations;
        while db.flush_stats.rotations == rotations {
            put(db, oracle, *i);
            *i += 1;
        }
        gates.until_waiting();
        assert!(
            db.frozen
                .front()
                .is_some_and(|f| f.ticket.worker == worker && !f.ready()),
            "the stalled packing is the oldest frozen"
        );
        worker
    }

    /// Puts through the next rotation: a free worker, when the host has one, takes the younger
    /// memtable and its result comes back behind the stalled one's, waited for; on a host whose
    /// workers are all busy, the stalled one's among them, it waits for a worker.
    fn younger_beside(
        db: &mut ShardDb<Held>,
        oracle: &mut BTreeMap<Vec<u8>, Vec<u8>>,
        i: &mut u64,
        put: Put,
    ) {
        let rotations = db.flush_stats.rotations;
        while db.flush_stats.rotations == rotations {
            put(db, oracle, *i);
            *i += 1;
        }
        if db.frozen.len() > 1 {
            db.take_packs(true).unwrap();
        }
        match db.frozen.get(1) {
            Some(younger) => assert!(
                younger.back.is_some(),
                "a younger packing's result did not come back behind the stalled one"
            ),
            None => {
                assert!(
                    db.packing.as_ref().is_some_and(|p| p.sort.is_some()),
                    "a younger packing neither came back nor waits for a worker"
                );
                assert!(
                    !db.trunk
                        .pool_mut()
                        .unwrap()
                        .can_take(&mut db.store, true)
                        .unwrap(),
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
        younger_beside(&mut db, &mut oracle, &mut i, put_or_delete);
        check(&mut db, &oracle);
        // Released: every frozen branch enters the trunk, oldest first.
        GATES_A.release();
        db.drain_frozen().unwrap();
        assert!(db.frozen.is_empty());
        assert!(GATES_A.waited());
        check(&mut db, &oracle);
        // Puts on, through `fanout` rotations, at the overlap the shard measures.
        let rotations = db.flush_stats.rotations;
        let more = u64::try_from(TRUNK.fanout).unwrap();
        while db.flush_stats.rotations < rotations + more {
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
        younger_beside(&mut db, &mut oracle, &mut i, put_or_delete);
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
}
