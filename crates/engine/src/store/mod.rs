//! The engine's own file (docs/design/engine-structure.md §3, §7; step E1): checksummed pages in
//! extents, a reference-counted allocator persisted with each checkpoint, and two alternating
//! superblock copies naming checkpoints whole.
//!
//! A checkpoint is made in the order §7 gives:
//! 1. the structure's new pages are written (by the caller, [`Store::write_page`]);
//! 2. the allocator map is written, copy-on-write, to extents of its own;
//! 3. the platform's full flush;
//! 4. the superblock copy for the new generation, naming the root, the map and the applied index;
//! 5. the full flush;
//! 6. the extents no longer named are freed for reuse.
//!
//! So the newest superblock copy that verifies always names a checkpoint whose every page is
//! durable, and recovery ([`Store::open`]) takes it. A write or flush that fails leaves the
//! durability of what was written unknown (hyper-block's `BlockFile::sync_data`): the store then
//! takes no more writes, and the caller reopens it from the file.

pub mod alloc;
pub mod cache;
pub mod page;
pub mod superblock;

use crate::error::{Error, Malformed};
use crate::fst::packed::MAX_WIDTH;
use crate::fst::surf::SurfBuilder;
use crate::fst::trie::TrieBuilder;
use alloc::Allocator;
use hyper_block::block::BlockFile;
use hyper_block::buf::{AlignedBuf, Alignment};
use hyper_block::issuer::{Answer, Attached, Attacher, Issuer, RetirementWatch, Transfers};
use hyper_rt::runtime::{OriginalAdoption, OriginalFence, OriginalLease};
use page::{HEADER, Kind};
use std::collections::VecDeque;
use superblock::Superblock;

/// The smallest page: 4 KiB, the largest logical block common devices use and the page every
/// file system the engine runs on maps (docs/design/raft-log.md §2's `B`).
pub const MIN_PAGE: usize = 4096;

/// A store's shape: fixed when the file is created, checked when it is opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// Bytes a page, a multiple of the file's alignment and at least [`MIN_PAGE`].
    pub page_size: usize,
    /// Pages an extent, at least 2 (extent 0 holds the two superblock copies).
    pub extent_pages: u32,
    /// The extents the file may hold: its bound (CLAUDE.md §2).
    pub max_extents: u64,
}

/// What [`Store::open`] recovered: the newest durable checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Recovered {
    /// The checkpoint's generation.
    pub generation: u64,
    /// The last Raft index its state includes: the log is replayed from the next.
    pub applied: u64,
    /// The structure's root page.
    pub root: Option<u64>,
}

/// The store's I/O since it was opened: what its callers' work cost the device.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IoStats {
    /// Read calls: one a page read alone, one a span ([`Store::read_page_ahead`]).
    pub reads: u64,
    /// Pages read, alone or in spans.
    pub pages_read: u64,
    /// Write calls: one a page written directly, one a run.
    pub writes: u64,
    /// Pages written, directly or in runs.
    pub pages_written: u64,
    /// Full flushes.
    pub syncs: u64,
    /// Nanoseconds inside the file's write calls, and its read calls: time the device, or the OS
    /// in front of it, held the caller.
    pub write_ns: u64,
    pub read_ns: u64,
    /// Runs handed to the device's issuer, and the times a write waited for a batch to be
    /// answered because all it may have out were out, with the nanoseconds waited.
    pub submitted: u64,
    pub write_waits: u64,
    pub write_wait_ns: u64,
    /// Point reads the page cache served, and those it did not.
    pub cache_hits: u64,
    pub cache_misses: u64,
    /// Scan pages the cache served, each a page a span did not read from the device.
    pub span_cache_hits: u64,
    /// A compaction's extents read ahead through the device's issuer ([`Store::prefetch`]), and
    /// the times a compaction step found its next page not yet landed and stopped for it
    /// ([`Store::ready`]).
    pub prefetches: u64,
    pub prefetch_waits: u64,
    /// The most queue steps one cache eviction took.
    pub cache_evict_steps_most: u64,
    /// Extent buffers taken for spans, runs and submissions, and those of them allocated fresh
    /// because the pool had none.
    pub buffers_taken: u64,
    pub buffers_fresh: u64,
    /// Extent buffers lent now: runs being filled, out or queued, and spans. The pool keeps no
    /// more than the most ever lent at once.
    pub buffers_out: u64,
    /// Runs whose answer the store has taken, landed or failed: `submitted` less these are out.
    pub runs_answered: u64,
    /// Nanoseconds queueing written pages (sealing, caching and handing runs on, waits
    /// included), and reading pages ahead for scans (from the cache or the file), timed only
    /// when the store is ([`Store::set_timed`]).
    pub queue_ns: u64,
    pub ahead_ns: u64,
    /// Nanoseconds builders spent sealing branches (their leaf index and range filter built
    /// and encoded, [`crate::branch::Builder::seal`]), in all and at the most one seal: timed
    /// only when the store is.
    pub seal_ns: u64,
    pub seal_most_ns: u64,
    /// Runs queued in write memory because every batch was out, where the writer would have
    /// waited, and the most queued at once.
    pub runs_queued: u64,
    pub runs_queued_most: u64,
    /// Pages read from runs still queued, without waiting for the device.
    pub queued_reads: u64,
    /// Measured always, for the memory tuner ([`crate::shard_db::ShardDb::set_memory`]):
    /// - point reads the cache missed whose page its ghost named (reads a larger cache would
    ///   have saved);
    /// - point reads the device served, and their nanoseconds;
    /// - nanoseconds writers waited because the write budget was spent.
    pub ghost_misses: u64,
    pub device_reads: u64,
    pub device_read_ns: u64,
    pub budget_wait_ns: u64,
}

/// Nanoseconds since `since`, none when timing is off (`Store::set_timed`).
fn elapsed_ns(since: Option<std::time::Instant>) -> u64 {
    since.map_or(0, |t| {
        u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX)
    })
}

/// Suffix bits a branch's range filter keeps a key unless set otherwise: measured, 1 M keys
/// (benches/surf.rs, 2026-10-07): 16-byte keys of random numbers let 0.03% of empty ranges
/// through at 8 bits (5.6% at none) for 18.8 bits a key; object names 3.6-4.4% at 8 bits
/// (38-44% at none, 0.2-0.6% at 16).
pub const RANGE_FILTER_BITS: u32 = 8;

/// A store over one file, owned by one thread (as every `BlockFile` is).
#[derive(Debug)]
pub struct Store<F: BlockFile> {
    /// Standalone cold owners retain their file; runtime owners transfer it before admission.
    file: Option<F>,
    /// The existing native retirement owner holds the original file through physical close.
    original: Option<OriginalLease>,
    config: Config,
    alloc: Allocator,
    /// Accepted worker jobs; nonempty borrowed runs cannot cross this boundary.
    job_ordinal: u64,
    /// The durable checkpoint.
    durable: Superblock,
    /// One page's aligned buffer, reused by every read and write.
    buf: AlignedBuf,
    /// A write or flush failed: the store takes no more.
    fenced: bool,
    /// The first cause of the fence survives later drains until the worker starts a new job.
    fence_error: Option<Error>,
    /// A paced terminal drain retains its first failure until every issued batch ends.
    drain_error: Option<Error>,
    drain_closed: bool,
    /// Terminal close survives a canceled borrowed wait.
    closing: Option<Closing>,
    /// One retained durability barrier; its map freezes allocator changes across waits.
    checkpoint: Option<Checkpoint>,
    /// A failed or abandoned barrier is terminal; a worker's ordinary job fence is separate.
    checkpoint_aborted: bool,
    checkpoint_payload: Vec<u8>,
    checkpoint_map: Vec<u64>,
    /// The file's end in bytes: past every page written, the furthest a span reads.
    end: u64,
    io: IoStats,
    /// The page cache for point reads, when the owner gives one ([`Store::set_cache`]).
    cache: Option<cache::Cache>,
    /// Suffix bits a branch's range filter keeps a key ([`Self::set_range_filter`]).
    range_filter_bits: u32,
    /// The device's issuer, when the owner attaches the store to one ([`Store::attach`]).
    writer: Option<Writer>,
    /// Runs of write memory the owner spares for runs waiting for the issuer, and the most
    /// queued at once since the tuner last asked ([`Store::take_queue_peak`]).
    write_budget_runs: usize,
    queue_peak: usize,
    /// Queued output buffers ever held, in bytes. Their returned buffers stay warm in pool,
    /// so reducing admission does not give this reservation back before the store retires.
    warm_write_bytes: usize,
    /// Extent buffers given back by spans, runs and answered writes, for the next to need one:
    /// a fresh one costs a page fault for each of its pages, milliseconds a compaction's
    /// cursors on a busy machine. It holds no more than were ever out at once.
    pool: Vec<AlignedBuf>,
    lent: usize,
    /// The buffer a point read's page is read into on a cache miss ([`Store::with_page`]).
    point: Vec<u8>,
    most_lent: usize,
    /// Empty span read lists, bounded by the most spans borrowed together. Pool slots
    /// are reserved while acquiring a span, never while canceling one.
    span_pending: Vec<PendingReads>,
    spans_lent: usize,
    spans_most_lent: usize,
    /// Page buffers lent to readers (a cursor's path, a point read), kept when given back: at
    /// most as many as were ever out at once.
    pages: Vec<Vec<u8>>,
    pages_lent: usize,
    pages_most_lent: usize,
    /// A branch builder's working lists (its extents, its pages' entry counts), kept when given
    /// back so they grow once, not with every branch: at most as many as were ever out at once.
    lists: Vec<Lists>,
    lists_lent: usize,
    lists_most_lent: usize,
    /// Whether its I/O is timed ([`Store::set_timed`]): a clock read a call, for diagnosis.
    timed: bool,
    /// Freed extents whose pages the cache still holds, forgotten a share at a time
    /// ([`Store::forget_some`]): at most the extents freed.
    forgetting: VecDeque<u64>,
    /// A worker's way to more extents once its grant is spent ([`Store::set_refill`]).
    refill: Option<Refill>,
}

/// A file returned after physical retirement, or its unchanged owner on refusal.
#[derive(Debug)]
pub enum IntoFile<O, F> {
    Finished { file: F, result: Result<(), Error> },
    Refused { owner: O, error: Error },
}

#[derive(Debug)]
struct Closing {
    phase: ClosePhase,
    error: Option<Error>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClosePhase {
    Drain,
    Abort,
    Retire,
    Original,
    Done,
}

/// The registered physical watch stays native-owned with the original file.
pub(crate) struct OriginalPhysical(RetirementWatch);

impl OriginalFence for OriginalPhysical {
    fn wait_blocking(&mut self) -> Result<(), hyper_rt::error::RtError> {
        self.0
            .wait_blocking()
            .map_err(|_| hyper_rt::error::RtError::BadConfig {
                what: "original file attachment retired abnormally",
            })
    }

    fn is_retired(&self) -> bool {
        self.0.is_retired()
    }
}

/// Asks the shard for `n` more extents and waits for them: a worker's, whose job outgrew its
/// grant.
pub struct Refill(pub Box<dyn FnMut(usize) -> Result<Vec<u64>, Error> + Send>);

impl std::fmt::Debug for Refill {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Refill")
    }
}

/// Runs handed to the device's issuer and not yet answered: each batch's number, the page
/// addresses it writes and the byte past them.
#[derive(Debug)]
struct Writer {
    attached: Attached,
    in_flight: VecDeque<(u64, u64, u64, u64)>,
    /// Full runs waiting for the issuer, oldest first, when every batch it may have out is
    /// out: a writer goes on instead of waiting for the device, up to `queue_most` runs, the
    /// write memory the owner spares the store ([`Store::set_write_budget`]). A put then waits
    /// for the device only once that memory is spent, as RocksDB's writes wait only once its
    /// memtables and level 0 have lagged their bound.
    queued: VecDeque<Queued>,
    queue_most: usize,
    /// Reads handed to the issuer ([`Store::prefetch`]) by batch number; the answers come
    /// with the writes' and are kept here until their span takes them; and reads whose span was
    /// given back first, whose buffers go back to the pool when they land.
    reads: Vec<u64>,
    parked: Vec<(u64, Result<Transfers, Error>)>,
    orphans: Vec<u64>,
    /// Batches' vectors back from the issuer with their answers, emptied, for the next batch:
    /// at most the batches attached for, since no more are ever out, so a warm submission
    /// allocates nothing.
    transfers: Vec<Transfers>,
    /// A compaction found no batch free for its read: the next one freed is kept for it, not
    /// handed to a queued run, so a write queue that never empties cannot starve its reads.
    /// Writes keep every other batch, so with two or more the reserve never stops them.
    read_wanted: bool,
    /// A numbered flush has no buffer loan. Any answer consumer keeps its result here.
    flush: Option<Flush>,
}

#[derive(Debug)]
struct Flush {
    number: u64,
    result: Option<Result<(), Error>>,
}

#[derive(Debug)]
struct Checkpoint {
    root: Option<u64>,
    applied: u64,
    generation: u64,
    refs: usize,
    map: Vec<u64>,
    payload: Vec<u8>,
    run: Run,
    phase: CheckpointPhase,
    error: Option<Error>,
}

#[derive(Debug)]
enum CheckpointPhase {
    Map(usize),
    DrainMap,
    Flush1(Option<u64>),
    EncodeSuperblock,
    Superblock,
    DrainSuperblock,
    Flush2(Option<u64>),
    Commit,
    Failed,
}

/// A full run waiting for the issuer: its buffer cut to its pages, its offset, its pages and
/// the file's end past it.
#[derive(Debug)]
struct Queued {
    buf: AlignedBuf,
    offset: u64,
    first: u64,
    end: u64,
    past: u64,
}

/// Node pages one writer queues at consecutive addresses within an extent, sealed into an
/// extent's buffer and written in one call ([`Store::queue_page`]): the buffer, its first page,
/// and the pages queued. Each writer (a branch's builder, the trunk's image) holds its own, so
/// writers that take turns do not write each other's runs out a page at a time. A run's pages
/// are read only once it is written: a branch is read once it is built, an image once saved.
#[derive(Debug)]
pub struct Run {
    buf: AlignedBuf,
    first: u64,
    pages: u32,
    /// The job that accepted its first page; ignored while this run is empty.
    job_ordinal: u64,
}

/// A branch builder's working lists, kept by the store between builders so they grow once: the
/// extents the branch holds, its tree pages' entry counts, and its leaves' separators with the
/// index built of them.
#[derive(Debug, Default)]
pub struct Lists {
    pub extents: Vec<u64>,
    pub counts: Vec<u16>,
    /// Each leaf's separator to its page number, a trie builder whose levels keep their
    /// buffers; and the encoded index built of them.
    pub separators: TrieBuilder,
    /// Each key, for the branch's range filter, and the filter encoded.
    pub keys: SurfBuilder,
    pub range: Vec<u8>,
    pub index: Vec<u8>,
    /// Each key's 32-bit maplet hash, sorted after the seal ([`crate::maplet`]), and the sort's
    /// scratch.
    pub hashes: Vec<u32>,
    pub hash_scratch: Vec<u32>,
    /// The builder's levels (empty between builders) and its spare pages; each level's first-key
    /// buffer; the last key and the last leaf's last key; and the payload a page
    /// is encoded into.
    pub(crate) levels: Vec<crate::branch::Page>,
    pub(crate) pages: Vec<crate::branch::Page>,
    pub(crate) firsts: Vec<Vec<u8>>,
    pub(crate) last: Vec<u8>,
    pub(crate) prev_last: Vec<u8>,
    pub(crate) payload: Vec<u8>,
}

impl Lists {
    fn clear(&mut self) {
        self.extents.clear();
        self.counts.clear();
        self.index.clear();
        self.range.clear();
        self.hashes.clear();
        for p in &mut self.pages {
            p.clear();
        }
        self.levels.clear();
        self.last.clear();
        self.prev_last.clear();
        self.payload.clear();
    }
}

impl Span {
    /// Bytes a page read verified in this span: a cursor borrows them until its next read.
    pub(crate) fn payload(&self, range: &std::ops::Range<usize>) -> Option<&[u8]> {
        self.buf.as_slice().get(range.clone())
    }

    /// Whether the span holds page `address`.
    fn holds(&self, address: u64) -> bool {
        address
            .checked_sub(self.first)
            .is_some_and(|i| i < u64::from(self.pages))
    }

    /// The read handed to the issuer for the span that covers `address`: its place in the
    /// span's reads.
    fn pending_for(&self, address: u64) -> Option<usize> {
        self.pending.iter().position(|&(_, first, pages, _)| {
            address
                .checked_sub(first)
                .is_some_and(|i| i < u64::from(pages))
        })
    }

    /// Extents the span reads ahead ([`Store::prefetch`]).
    pub fn ahead_extents(&self) -> usize {
        self.depth
    }

    /// Whether a read of `address` runs on from the last: it lands within what the next read,
    /// at double the size, would cover.
    fn runs_on(&self, address: u64) -> bool {
        let held_end = self.first.saturating_add(u64::from(self.pages));
        self.pages > 0
            && address >= held_end
            && address < held_end.saturating_add(u64::from(self.ahead).saturating_mul(2))
    }
}

type PendingReads = VecDeque<(u64, u64, u32, bool)>;

/// Pages a scan reads ahead ([`Store::read_page_ahead`]): an extent's buffer, the first page it
/// holds, and the pages it holds. A scan reads one finished branch's pages, which no write
/// changes while the branch is held, so the pages a span holds stay the file's.
#[derive(Debug)]
pub struct Span {
    buf: AlignedBuf,
    first: u64,
    pages: u32,
    /// Pages the next read takes, at most the extent's: a scan's reads start at `floor` and
    /// double while they run on from the last (RocksDB's auto-readahead, which starts once
    /// reads turn sequential and doubles to its bound), so a seek reads the page it wants and a
    /// long scan reads extents a call.
    ahead: u32,
    /// The least a read takes: one page for a scan that may stop at once, the extent for a
    /// compaction, which reads its inputs to their end.
    floor: u32,
    /// Reads handed to the device's issuer for this span, in address order: each one's batch
    /// number, first page, pages and whether its delay has widened the lookahead. Each late
    /// read adds one extent, up to the batches; polling it again is not a new observation of
    /// the device's latency over the time the compaction takes through an extent.
    pending: PendingReads,
    depth: usize,
    /// The next memory-ready payload, held across a builder's intervening cache admission or
    /// write submission. One pooled page; it does not replace the current leaf's bytes.
    prepared: Option<(u64, Vec<u8>)>,
}

fn io(op: &'static str, e: impl std::fmt::Display) -> Error {
    Error::Io {
        op,
        detail: e.to_string(),
    }
}

fn corrupt(why: Malformed) -> Error {
    Error::Corruption {
        what: "a store",
        why,
    }
}

fn blocking_allowed(what: &'static str) -> Result<(), Error> {
    if hyper_rt::registry::current_shard().is_some() {
        Err(Error::InvalidArgument { what })
    } else {
        Ok(())
    }
}

fn finish_context(cx: &std::task::Context<'_>) -> Result<(), Error> {
    if hyper_rt::futures::current_task()
        .is_some_and(|task| hyper_rt::waker::waker_for(task.0).will_wake(cx.waker()))
    {
        Ok(())
    } else {
        Err(Error::InvalidArgument {
            what: "an async store finish polled outside its runtime task",
        })
    }
}

fn checkpoint_context(cx: &std::task::Context<'_>) -> Result<(), Error> {
    if hyper_rt::futures::current_task()
        .is_some_and(|task| hyper_rt::waker::waker_for(task.0).will_wake(cx.waker()))
    {
        Ok(())
    } else {
        Err(Error::InvalidArgument {
            what: "an async checkpoint polled outside its runtime task",
        })
    }
}

fn page_context(cx: &std::task::Context<'_>) -> Result<(), Error> {
    if hyper_rt::futures::current_task()
        .is_some_and(|task| hyper_rt::waker::waker_for(task.0).will_wake(cx.waker()))
    {
        Ok(())
    } else {
        Err(Error::InvalidArgument {
            what: "an async page read polled outside its runtime task",
        })
    }
}

fn write_context(cx: &std::task::Context<'_>) -> Result<(), Error> {
    if hyper_rt::futures::current_task()
        .is_some_and(|task| hyper_rt::waker::waker_for(task.0).will_wake(cx.waker()))
    {
        Ok(())
    } else {
        Err(Error::InvalidArgument {
            what: "an async page write polled outside its runtime task",
        })
    }
}

/// Appends the payload of the node page `page` read from `address` to `out`, once it verifies.
fn node_payload(page: &[u8], address: u64, out: &mut Vec<u8>) -> Result<(), Error> {
    out.extend_from_slice(node_payload_ref(page, address)?);
    Ok(())
}

fn node_payload_ref(page: &[u8], address: u64) -> Result<&[u8], Error> {
    let header = page::verify(page, address)?;
    if header.kind != Kind::Node {
        return Err(corrupt(Malformed::UnknownTag(0)));
    }
    page::payload(page, header)
}

impl<F: BlockFile> Store<F> {
    fn check(file: &F, config: Config) -> Result<(), Error> {
        let align = file.alignment();
        if config.page_size < MIN_PAGE || !align.is_aligned(config.page_size) {
            return Err(Error::InvalidArgument {
                what: "a page size below 4 KiB or off the file's alignment",
            });
        }
        if config.extent_pages < 2 {
            return Err(Error::InvalidArgument {
                what: "an extent of fewer than 2 pages",
            });
        }
        Ok(())
    }

    fn page_buf(file: &F, config: Config) -> Result<AlignedBuf, Error> {
        let mut buf = AlignedBuf::zeroed(config.page_size, file.alignment())
            .map_err(|e| io("allocate a page buffer", e))?;
        buf.extend_zeros(config.page_size)
            .map_err(|e| io("allocate a page buffer", e))?;
        Ok(buf)
    }

    fn run_buf(alignment: Alignment, config: Config) -> Result<AlignedBuf, Error> {
        let bytes = config
            .page_size
            .checked_mul(
                usize::try_from(config.extent_pages).map_err(|_| corrupt(Malformed::TooLarge))?,
            )
            .ok_or(corrupt(Malformed::TooLarge))?;
        let mut run =
            AlignedBuf::zeroed(bytes, alignment).map_err(|e| io("allocate an extent buffer", e))?;
        run.extend_zeros(bytes)
            .map_err(|e| io("allocate an extent buffer", e))?;
        Ok(run)
    }

    /// Creates a store in `file`, which must be empty: generation 1, no root, applied 0, durable
    /// when this returns.
    pub fn create(file: F, config: Config) -> Result<Self, Error> {
        blocking_allowed("a synchronous store creation in a runtime task")?;
        Self::check(&file, config)?;
        if !file.is_empty().map_err(|e| io("stat a store", e))? {
            return Err(Error::InvalidArgument {
                what: "a store created over a file that is not empty",
            });
        }
        let buf = Self::page_buf(&file, config)?;
        let page_size = u32::try_from(config.page_size).map_err(|_| Error::InvalidArgument {
            what: "a page size past 4 GiB",
        })?;
        let mut store = Self {
            file: Some(file),
            original: None,
            config,
            alloc: Allocator::new(config.max_extents),
            durable: Superblock {
                page_size,
                extent_pages: config.extent_pages,
                generation: 0,
                applied: 0,
                root: None,
                extents: 1,
                map: Vec::new(),
            },
            buf,
            fenced: false,
            fence_error: None,
            job_ordinal: 0,
            drain_error: None,
            drain_closed: false,
            closing: None,
            checkpoint: None,
            checkpoint_aborted: false,
            checkpoint_payload: Vec::new(),
            checkpoint_map: Vec::new(),
            end: 0,
            io: IoStats::default(),
            cache: None,
            range_filter_bits: RANGE_FILTER_BITS,
            writer: None,
            write_budget_runs: 0,
            queue_peak: 0,
            warm_write_bytes: 0,
            pool: Vec::new(),
            lent: 0,
            point: Vec::new(),
            most_lent: 0,
            span_pending: Vec::new(),
            spans_lent: 0,
            spans_most_lent: 0,
            pages: Vec::new(),
            pages_lent: 0,
            pages_most_lent: 0,
            lists: Vec::new(),
            lists_lent: 0,
            lists_most_lent: 0,
            forgetting: VecDeque::new(),
            timed: false,
            refill: None,
        };
        store.checkpoint(None, 0)?;
        Ok(store)
    }

    /// A maintenance worker's store over its own handle on the shard's file: no superblock and no
    /// cache, and no extent until a job grants some ([`Self::begin_job`]). A compaction or a
    /// branch's build runs on it as on the shard's own, writing in place through the file, each
    /// write landed when it returns; the shard takes the result once every write has. One store
    /// serves a worker's every job, its buffers' pools kept warm.
    pub fn worker(file: F, config: Config) -> Result<Self, Error> {
        blocking_allowed("a synchronous worker store setup in a runtime task")?;
        Self::check(&file, config)?;
        let buf = Self::page_buf(&file, config)?;
        let page_size = u32::try_from(config.page_size).map_err(|_| Error::InvalidArgument {
            what: "a page size past 4 GiB",
        })?;
        Ok(Self {
            file: Some(file),
            original: None,
            config,
            alloc: Allocator::granted(),
            durable: Superblock {
                page_size,
                extent_pages: config.extent_pages,
                generation: 0,
                applied: 0,
                root: None,
                extents: 0,
                map: Vec::new(),
            },
            buf,
            fenced: false,
            fence_error: None,
            job_ordinal: 0,
            drain_error: None,
            drain_closed: false,
            closing: None,
            checkpoint: None,
            checkpoint_aborted: false,
            checkpoint_payload: Vec::new(),
            checkpoint_map: Vec::new(),
            end: 0,
            io: IoStats::default(),
            cache: None,
            range_filter_bits: RANGE_FILTER_BITS,
            writer: None,
            write_budget_runs: 0,
            queue_peak: 0,
            warm_write_bytes: 0,
            pool: Vec::new(),
            lent: 0,
            point: Vec::new(),
            most_lent: 0,
            span_pending: Vec::new(),
            spans_lent: 0,
            spans_most_lent: 0,
            pages: Vec::new(),
            pages_lent: 0,
            pages_most_lent: 0,
            lists: Vec::new(),
            lists_lent: 0,
            lists_most_lent: 0,
            forgetting: VecDeque::new(),
            timed: false,
            refill: None,
        })
    }

    /// A worker's next job: the extents `grant`ed it ([`Allocator::granted`]); reads up to `end`,
    /// the bytes the shard's store knew the file to hold when it granted the job; and its pages
    /// sealed as the shard's would be, for the checkpoint after `generation`.
    pub fn begin_job(&mut self, grant: &[u64], end: u64, generation: u64) -> Result<(), Error> {
        self.allocator_available()?;
        if self
            .writer
            .as_ref()
            .is_some_and(|writer| writer.attached.out() > 0 || !writer.queued.is_empty())
        {
            return Err(Error::InvalidArgument {
                what: "a worker job started before its earlier transfers drained",
            });
        }
        let job_ordinal = self
            .job_ordinal
            .checked_add(1)
            .ok_or(Error::LimitExceeded {
                what: "worker jobs of a store",
                limit: u64::MAX,
            })?;
        self.alloc.regrant(grant);
        self.job_ordinal = job_ordinal;
        self.end = end;
        self.durable.generation = generation;
        // A failed write fences a store, since what it holds durable is then unknown. A
        // worker's store holds nothing durable across jobs: a failed job's outputs are dropped
        // by the shard, which keeps its own fence. Each job starts unfenced, so one failure
        // does not fail every later job on the worker.
        self.fenced = false;
        self.fence_error = None;
        Ok(())
    }

    /// How a worker gets more extents once its grant is spent.
    pub fn set_refill(&mut self, refill: Refill) {
        self.refill = Some(refill);
    }

    /// A worker's granted extents it never wrote, for the shard to take back
    /// ([`Self::grant_back`]).
    pub fn unused_grant(&mut self) -> Vec<u64> {
        self.alloc.unused()
    }

    /// Opens the store in `file` at its newest durable checkpoint: the newer of the two
    /// superblock copies that verify, its map read back. A file whose copies both fail is corrupt.
    pub fn open(file: F, config: Config) -> Result<(Self, Recovered), Error> {
        blocking_allowed("a synchronous store recovery in a runtime task")?;
        Self::check(&file, config)?;
        let mut buf = Self::page_buf(&file, config)?;
        let mut best: Option<Superblock> = None;
        for slot in 0..2u64 {
            let Ok(sb) = Self::read_superblock(&file, config, &mut buf, slot) else {
                continue;
            };
            if sb.slot() == slot && best.as_ref().is_none_or(|b| sb.generation > b.generation) {
                best = Some(sb);
            }
        }
        let sb = best.ok_or(corrupt(Malformed::ChecksumMismatch))?;
        if usize::try_from(sb.page_size).ok() != Some(config.page_size)
            || sb.extent_pages != config.extent_pages
        {
            return Err(Error::InvalidArgument {
                what: "a store opened with another page or extent size",
            });
        }
        let refs = Self::read_map(&file, config, &mut buf, &sb)?;
        let alloc = Allocator::from_refs(refs, config.max_extents)?;
        let end = file.len().map_err(|e| io("stat a store", e))?;
        let recovered = Recovered {
            generation: sb.generation,
            applied: sb.applied,
            root: sb.root,
        };
        Ok((
            Self {
                file: Some(file),
                original: None,
                config,
                alloc,
                durable: sb,
                buf,
                fenced: false,
                fence_error: None,
                job_ordinal: 0,
                drain_error: None,
                drain_closed: false,
                closing: None,
                checkpoint: None,
                checkpoint_aborted: false,
                checkpoint_payload: Vec::new(),
                checkpoint_map: Vec::new(),
                end,
                io: IoStats::default(),
                cache: None,
                range_filter_bits: RANGE_FILTER_BITS,
                writer: None,
                write_budget_runs: 0,
                queue_peak: 0,
                warm_write_bytes: 0,
                pool: Vec::new(),
                lent: 0,
                point: Vec::new(),
                most_lent: 0,
                span_pending: Vec::new(),
                spans_lent: 0,
                spans_most_lent: 0,
                pages: Vec::new(),
                pages_lent: 0,
                pages_most_lent: 0,
                lists: Vec::new(),
                lists_lent: 0,
                lists_most_lent: 0,
                forgetting: VecDeque::new(),
                timed: false,
                refill: None,
            },
            recovered,
        ))
    }

    fn read_superblock(
        file: &F,
        config: Config,
        buf: &mut AlignedBuf,
        slot: u64,
    ) -> Result<Superblock, Error> {
        blocking_allowed("a synchronous superblock read in a runtime task")?;
        let offset = slot
            .checked_mul(u64::try_from(config.page_size).map_err(|_| corrupt(Malformed::TooLarge))?)
            .ok_or(corrupt(Malformed::TooLarge))?;
        file.read_exact_at(buf.as_mut_slice(), offset)
            .map_err(|e| io("read a superblock", e))?;
        let header = page::verify(buf.as_slice(), slot)?;
        if header.kind != Kind::Superblock {
            return Err(corrupt(Malformed::UnknownTag(0)));
        }
        let sb = Superblock::decode(page::payload(buf.as_slice(), header)?)?;
        if sb.generation != header.generation {
            return Err(corrupt(Malformed::CountMismatch));
        }
        Ok(sb)
    }

    fn read_map(
        file: &F,
        config: Config,
        buf: &mut AlignedBuf,
        sb: &Superblock,
    ) -> Result<Vec<u32>, Error> {
        blocking_allowed("a synchronous allocator map read in a runtime task")?;
        let count = usize::try_from(sb.extents).map_err(|_| corrupt(Malformed::TooLarge))?;
        if sb.extents > config.max_extents {
            return Err(corrupt(Malformed::TooLarge));
        }
        let mut refs = Vec::with_capacity(count);
        'pages: for &extent in &sb.map {
            for i in 0..u64::from(config.extent_pages) {
                if refs.len() >= count {
                    break 'pages;
                }
                let address = Self::address_in(config, extent, i)?;
                file.read_exact_at(buf.as_mut_slice(), Self::offset_in(config, address)?)
                    .map_err(|e| io("read the allocator map", e))?;
                let header = page::verify(buf.as_slice(), address)?;
                if header.kind != Kind::Map || header.generation != sb.generation {
                    return Err(corrupt(Malformed::CountMismatch));
                }
                for c in page::payload(buf.as_slice(), header)?.as_chunks::<4>().0 {
                    if refs.len() < count {
                        refs.push(u32::from_le_bytes(*c));
                    }
                }
            }
        }
        if refs.len() != count {
            return Err(corrupt(Malformed::CountMismatch));
        }
        Ok(refs)
    }

    fn address_in(config: Config, extent: u64, page: u64) -> Result<u64, Error> {
        extent
            .checked_mul(u64::from(config.extent_pages))
            .and_then(|a| a.checked_add(page))
            .ok_or(corrupt(Malformed::TooLarge))
    }

    fn offset_in(config: Config, address: u64) -> Result<u64, Error> {
        address
            .checked_mul(u64::try_from(config.page_size).map_err(|_| corrupt(Malformed::TooLarge))?)
            .ok_or(corrupt(Malformed::TooLarge))
    }

    /// The store's I/O since it was opened.
    pub fn io_stats(&self) -> IoStats {
        let (cache_hits, cache_misses) = self.cache.as_ref().map_or((0, 0), cache::Cache::stats);
        IoStats {
            cache_hits,
            cache_misses,
            cache_evict_steps_most: self
                .cache
                .as_ref()
                .map_or(0, cache::Cache::evict_steps_most),
            buffers_out: u64::try_from(self.lent).unwrap_or(u64::MAX),
            ..self.io
        }
    }

    /// Has the branches built from here keep `suffix_bits` of each key past its cut in their
    /// range filters (research/35 §2): more bits rule out more empty ranges, at a bit a key each.
    pub fn set_range_filter(&mut self, suffix_bits: u32) -> Result<(), Error> {
        if suffix_bits > MAX_WIDTH {
            return Err(Error::InvalidArgument {
                what: "a range filter suffix past 32 bits",
            });
        }
        self.range_filter_bits = suffix_bits;
        Ok(())
    }

    /// Suffix bits the branches built from here keep a key in their range filters.
    pub fn range_filter_bits(&self) -> u32 {
        self.range_filter_bits
    }

    /// Gives point reads a page cache of `pages` pages (none at 0), replacing any it had. Its
    /// memory is `pages` times the page size, taken now.
    pub fn set_cache(&mut self, pages: usize) {
        self.cache = (pages > 0).then(|| cache::Cache::new(pages, self.config.page_size));
    }

    /// Resizes the page cache to `pages` in place, keeping what it holds (`Cache::resize`); a
    /// store without one is given one.
    pub fn resize_cache(&mut self, pages: usize) {
        match self.cache.as_mut() {
            Some(c) if pages > 0 => c.resize(pages),
            _ => self.set_cache(pages),
        }
    }

    /// Up to `n` steps bringing the page cache down to its limit (`Cache::trim`); true while it
    /// is still above it.
    /// The page cache's slots still to reserve ([`cache::Cache::reserve`]).
    pub fn cache_unreserved(&self) -> usize {
        self.cache.as_ref().map_or(0, cache::Cache::unreserved)
    }

    /// Reserves up to `n` of the page cache's slots, in idle time: the pages made.
    pub fn reserve_cache(&mut self, n: usize) -> usize {
        self.cache.as_mut().map_or(0, |c| c.reserve(n))
    }

    pub fn trim_cache(&mut self, n: usize) -> bool {
        self.cache.as_mut().is_some_and(|c| c.trim(n))
    }

    /// The page cache's bytes in memory: its pages and the buffers it keeps for more.
    pub fn cache_bytes(&self) -> usize {
        self.cache.as_ref().map_or(0, cache::Cache::bytes)
    }

    /// The bytes the page cache's index and ghost take.
    pub fn cache_index_bytes(&self) -> usize {
        self.cache.as_ref().map_or(0, cache::Cache::index_bytes)
    }

    /// Pages and freed buffers the page cache holds above its limit.
    pub fn cache_over(&self) -> usize {
        self.cache.as_ref().map_or(0, cache::Cache::over)
    }

    /// The page cache's slots and its ghost's, none without a cache.
    pub fn cache_pages(&self) -> (usize, usize) {
        self.cache
            .as_ref()
            .map_or((0, 0), |c| (c.pages(), c.ghost_pages()))
    }

    /// The pages an extent holds.
    pub fn extent_pages(&self) -> u32 {
        self.config.extent_pages
    }

    /// The page size.
    pub fn page_size(&self) -> usize {
        self.config.page_size
    }

    /// The payload a page holds at most.
    pub fn page_capacity(&self) -> usize {
        page::capacity(self.config.page_size)
    }

    /// The address of page `page` (below the extent's pages) of `extent`.
    pub fn address(&self, extent: u64, page: u32) -> Result<u64, Error> {
        if page >= self.config.extent_pages {
            return Err(Error::InvalidArgument {
                what: "a page past its extent",
            });
        }
        Self::address_in(self.config, extent, u64::from(page))
    }

    /// The extent holding `address`.
    pub fn extent_of(&self, address: u64) -> u64 {
        address
            .checked_div(u64::from(self.config.extent_pages))
            .unwrap_or(0)
    }

    /// The store's shape: its page size, extent pages and most extents.
    pub fn config(&self) -> Config {
        self.config
    }

    /// The durable checkpoint's generation.
    pub fn generation(&self) -> u64 {
        self.durable.generation
    }

    /// An extent for new pages, held once.
    pub fn allocate_extent(&mut self) -> Result<u64, Error> {
        self.allocator_available()?;
        match self.alloc.allocate() {
            Err(Error::LimitExceeded { .. }) if self.refill.is_some() => {
                // The failed granted allocation moved no extent. A runtime task must
                // not enter the native worker's synchronous refill callback.
                blocking_allowed("a synchronous worker refill inside a runtime task")?;
                // A worker's grant is spent: as many again as it held, so the asks a job makes
                // grow geometrically and number at most the logarithm of its need (the doubling
                // of a dynamic table, Cormen et al., Introduction to Algorithms, 3rd ed., §17.4).
                let n = usize::try_from(self.alloc.limit())
                    .unwrap_or(usize::MAX)
                    .max(1);
                let more = match self.refill.as_mut() {
                    Some(Refill(ask)) => ask(n)?,
                    None => Vec::new(),
                };
                self.alloc.grant_more(&more);
                self.alloc.allocate()
            }
            other => other,
        }
    }

    /// `count` extents for a maintenance worker's job, each held once as the shard's own: the
    /// worker writes them through its own store ([`Self::worker`]), whose writes this store's
    /// cache never sees, so any page the cache still holds of them, from before they were freed,
    /// is forgotten now rather than in its turn ([`Self::forget_some`] passes over extents held
    /// again, since this store's own writes replace what it cached).
    pub fn grant(&mut self, count: usize) -> Result<Vec<u64>, Error> {
        self.allocator_available()?;
        let mut out = Vec::new();
        out.try_reserve_exact(count)
            .map_err(|_| Error::LimitExceeded {
                what: "extents granted a maintenance job",
                limit: u64::try_from(count).unwrap_or(u64::MAX),
            })?;
        if let Some(last) = self.alloc.reserve(count)? {
            let page = u64::from(self.config.extent_pages)
                .checked_sub(1)
                .ok_or(corrupt(Malformed::TooLarge))?;
            Self::address_in(self.config, last, page)?;
        }
        for _ in 0..count {
            out.push(self.alloc.allocate()?);
        }
        if let Some(c) = self.cache.as_mut() {
            let per = u64::from(self.config.extent_pages);
            for &extent in &out {
                let first = extent
                    .checked_mul(per)
                    .ok_or(corrupt(Malformed::TooLarge))?;
                for page in 0..per {
                    c.forget(first.saturating_add(page));
                }
            }
        }
        Ok(out)
    }

    /// Takes back extents granted a worker that it never wrote: free again at once, since no
    /// checkpoint ever named them.
    pub fn grant_back(&mut self, extents: &[u64]) -> Result<(), Error> {
        // Returning an unused grant is cleanup after finish began. The checkpoint's
        // retained and abandoned barriers, and held-once validation, still apply.
        self.allocator_idle()?;
        self.alloc.reserve_back(extents)?;
        for &extent in extents {
            self.alloc.give_back(extent)?;
        }
        Ok(())
    }

    /// The bytes the file is known to hold: what a read may reach.
    pub fn end(&self) -> u64 {
        self.end
    }

    /// The file holds at least `end` bytes: a worker's writes, landed, reached it.
    pub fn extend_end(&mut self, end: u64) {
        self.end = self.end.max(end);
    }

    /// One more reference to a held extent (a new node naming an existing branch).
    pub fn retain(&mut self, extent: u64) -> Result<(), Error> {
        self.allocator_available()?;
        self.alloc.retain(extent)
    }

    /// One reference fewer; an extent left with none is reused once a checkpoint that no
    /// longer names it is durable.
    pub fn release(&mut self, extent: u64) -> Result<(), Error> {
        self.checkpoint_idle()?;
        self.alloc.release(extent)?;
        // An extent no node names any more is read by no one: its pages leave the cache, so
        // the cache holds live pages, not the inputs of compactions done. Forgetting a
        // compaction's inputs at once cost milliseconds of one slice, so the extent waits its
        // turn ([`Self::forget_some`]).
        if !self.alloc.is_held(extent) && self.cache.is_some() {
            self.forgetting.push_back(extent);
        }
        Ok(())
    }

    /// The pages of freed extents the cache has yet to forget.
    pub fn forget_debt(&self) -> u64 {
        u64::try_from(self.forgetting.len())
            .unwrap_or(u64::MAX)
            .saturating_mul(u64::from(self.config.extent_pages))
    }

    /// Forgets up to `pages` pages of freed extents, an extent at a time; returns the pages
    /// forgotten. An extent held again by then is passed over: each page written there replaced
    /// what the cache held, and its pages not written are read by no one.
    pub fn forget_some(&mut self, pages: u64) -> u64 {
        let per = u64::from(self.config.extent_pages);
        let mut done = 0u64;
        while done < pages {
            let Some(extent) = self.forgetting.pop_front() else {
                break;
            };
            done = done.saturating_add(per);
            if self.alloc.is_held(extent) {
                continue;
            }
            if let Some(c) = self.cache.as_mut() {
                let first = extent.saturating_mul(per);
                for address in first..first.saturating_add(per) {
                    c.forget(address);
                }
            }
        }
        done
    }

    /// The extents holding the durable checkpoint's allocator map.
    pub fn map_extents(&self) -> &[u64] {
        &self.durable.map
    }

    /// The allocator's reference counts, extent by extent.
    pub fn refs(&self) -> &[u32] {
        self.alloc.refs()
    }

    /// Whether a failed write or flush has stopped further writes until the file is reopened.
    pub(crate) fn fenced(&self) -> bool {
        self.fenced
    }

    pub(crate) fn has_issuer(&self) -> bool {
        self.writer.is_some()
    }

    fn fence<T>(&mut self, result: Result<T, Error>) -> Result<T, Error> {
        if let Err(error) = &result {
            self.fence_error.get_or_insert_with(|| error.clone());
            self.fenced = true;
            // The cache took pages as they were queued; one whose write failed is not the
            // file's, so reads go to the file until the owner reopens it.
            self.cache = None;
        }
        result
    }

    fn put(
        &mut self,
        address: u64,
        kind: Kind,
        generation: u64,
        payload: &[u8],
    ) -> Result<(), Error> {
        blocking_allowed("a synchronous store page write in a runtime task")?;
        self.admission_open()?;
        if self.fenced {
            return Err(io(
                "write a store page",
                "the store was fenced by a failed write or flush",
            ));
        }
        if let Some(c) = self.cache.as_mut() {
            c.forget(address);
        }
        let buf = self.buf.as_mut_slice();
        buf.get_mut(HEADER..HEADER.saturating_add(payload.len()))
            .ok_or(Error::InvalidArgument {
                what: "a page payload longer than the page",
            })?
            .copy_from_slice(payload);
        page::seal(buf, address, kind, generation, payload.len())?;
        let offset = Self::offset_in(self.config, address)?;
        let past = Self::past(offset, self.buf.as_slice().len())?;
        self.io.writes = self.io.writes.saturating_add(1);
        self.io.pages_written = self.io.pages_written.saturating_add(1);
        let started = self.timed.then(std::time::Instant::now);
        let written = self
            .file
            .as_ref()
            .ok_or_else(Self::original_missing)?
            .write_all_at(self.buf.as_slice(), offset)
            .map_err(|e| io("write a store page", e));
        self.io.write_ns = self.io.write_ns.saturating_add(elapsed_ns(started));
        self.fence(written)?;
        self.end = self.end.max(past);
        Ok(())
    }

    /// The byte past `len` bytes at `offset`.
    fn past(offset: u64, len: usize) -> Result<u64, Error> {
        u64::try_from(len)
            .ok()
            .and_then(|l| offset.checked_add(l))
            .ok_or(corrupt(Malformed::TooLarge))
    }

    fn sync(&mut self) -> Result<(), Error> {
        blocking_allowed("a synchronous store flush in a runtime task")?;
        self.admission_open()?;
        // A flush makes durable only the writes completed when it is issued: every run in
        // flight is answered first, and a batch still out here is refused, not flushed past (a
        // batch counts as out until its answer is taken, so the check is exact).
        self.drain()?;
        if self.writer.as_ref().is_some_and(|w| w.attached.out() > 0) {
            return Err(io(
                "flush a store",
                "a run handed to the issuer is still out",
            ));
        }
        self.io.syncs = self.io.syncs.saturating_add(1);
        let synced = self
            .file
            .as_ref()
            .ok_or_else(Self::original_missing)?
            .sync_data()
            .map_err(|e| io("flush a store", e));
        self.fence(synced)
    }

    /// Times the store's write and read calls into [`IoStats`]: off by default, as a clock read
    /// each call costs the put and get paths.
    /// A clock read for a stage the store's owner times, when the store is timed.
    pub fn clock(&self) -> Option<std::time::Instant> {
        self.timed.then(std::time::Instant::now)
    }

    /// Counts a branch seal begun at `t` ([`Self::clock`]).
    pub fn note_seal(&mut self, t: Option<std::time::Instant>) {
        let ns = elapsed_ns(t);
        self.io.seal_ns = self.io.seal_ns.saturating_add(ns);
        self.io.seal_most_ns = self.io.seal_most_ns.max(ns);
    }

    pub fn set_timed(&mut self, on: bool) {
        self.timed = on;
    }

    /// An extent buffer from the pool, or a fresh one.
    fn take_buf(&mut self) -> Result<AlignedBuf, Error> {
        let buf = match self.pool.pop() {
            Some(b) => b,
            None => {
                self.io.buffers_fresh = self.io.buffers_fresh.saturating_add(1);
                Self::run_buf(self.buf.alignment(), self.config)?
            }
        };
        self.io.buffers_taken = self.io.buffers_taken.saturating_add(1);
        self.lent = self.lent.saturating_add(1);
        self.most_lent = self.most_lent.max(self.lent);
        Ok(buf)
    }

    /// Gives an extent buffer back to the pool, its length the extent's again.
    fn give_buf(&mut self, mut buf: AlignedBuf) {
        self.lent = self.lent.saturating_sub(1);
        let capacity = self
            .config
            .page_size
            .saturating_mul(usize::try_from(self.config.extent_pages).unwrap_or(usize::MAX));
        if buf.capacity() >= capacity
            && buf.set_len(capacity).is_ok()
            && self.pool.len() < self.most_lent
        {
            self.pool.push(buf);
        }
    }

    /// An empty page buffer for a reader, from the pool or fresh: given back with
    /// [`Store::give_page`], a read allocates nothing once the pool holds the most ever out.
    pub fn take_page(&mut self) -> Vec<u8> {
        self.pages_lent = self.pages_lent.saturating_add(1);
        self.pages_most_lent = self.pages_most_lent.max(self.pages_lent);
        match self.pages.pop() {
            Some(mut p) => {
                p.clear();
                p
            }
            None => Vec::with_capacity(self.config.page_size),
        }
    }

    /// Takes back a page buffer a reader is done with.
    pub fn give_page(&mut self, page: Vec<u8>) {
        self.pages_lent = self.pages_lent.saturating_sub(1);
        if self.pages.len() < self.pages_most_lent {
            self.pages.push(page);
        }
    }

    /// Empty working lists for a branch builder, from the pool or fresh.
    pub fn take_lists(&mut self) -> Lists {
        self.lists_lent = self.lists_lent.saturating_add(1);
        self.lists_most_lent = self.lists_most_lent.max(self.lists_lent);
        let mut lists = self.lists.pop().unwrap_or_default();
        lists.clear();
        lists
    }

    /// Takes back a builder's working lists.
    pub fn give_lists(&mut self, lists: Lists) {
        self.lists_lent = self.lists_lent.saturating_sub(1);
        if self.lists.len() < self.lists_most_lent {
            self.lists.push(lists);
        }
    }

    /// An empty run for a writer.
    pub fn run(&mut self) -> Result<Run, Error> {
        Ok(Run {
            buf: self.take_buf()?,
            first: 0,
            pages: 0,
            job_ordinal: 0,
        })
    }

    fn current_run_job(&self, run: &Run) -> Result<(), Error> {
        if run.pages > 0 && run.job_ordinal != self.job_ordinal {
            return Err(Error::InvalidArgument {
                what: "a nonempty page run from an earlier worker job",
            });
        }
        Ok(())
    }

    /// Takes back a run its writer is done with: its pages written, its buffer for the next.
    pub fn give_run(&mut self, run: Run) {
        self.give_buf(run.buf);
    }

    /// Queues a node page at `address` in `run` for the next checkpoint, as
    /// [`Self::write_page`] writes one: pages queued at consecutive addresses within one extent
    /// are written in one call, an extent's pages a system call where a page each was. A page
    /// not continuing the run writes the run out first ([`Self::write_run`]), and a full extent
    /// writes it.
    pub fn queue_page(&mut self, run: &mut Run, address: u64, payload: &[u8]) -> Result<(), Error> {
        blocking_allowed("a synchronous store page queue in a runtime task")?;
        self.admission_open()?;
        self.checkpoint_idle()?;
        if self.fenced {
            return Err(io(
                "queue a store page",
                "the store was fenced by a failed write or flush",
            ));
        }
        let started = self.timed.then(std::time::Instant::now);
        let queued = self.queue_page_timed(run, address, payload);
        self.io.queue_ns = self.io.queue_ns.saturating_add(elapsed_ns(started));
        queued
    }

    /// Accepts one node page without waiting for output credit. False leaves this page
    /// unaccepted; a full older run remains owned by its caller across the wait.
    pub(crate) fn queue_page_paced(
        &mut self,
        run: &mut Run,
        address: u64,
        payload: &[u8],
    ) -> Result<bool, Error> {
        self.checkpoint_idle()?;
        let generation = self
            .durable
            .generation
            .checked_add(1)
            .ok_or(corrupt(Malformed::TooLarge))?;
        self.queue_kind_paced(run, address, Kind::Node, generation, payload)
    }

    /// Accepts one page into the borrowed run, yielding for an older run's output credit.
    /// Cancellation before acceptance retains that older run. Once this page is accepted,
    /// the call returns without another wait; `write_run_async` dispatches the retained run.
    pub async fn queue_page_async(
        &mut self,
        run: &mut Run,
        address: u64,
        payload: &[u8],
    ) -> Result<(), Error> {
        loop {
            std::future::poll_fn(|cx| std::task::Poll::Ready(write_context(cx))).await?;
            if self.queue_page_paced(run, address, payload)? {
                return Ok(());
            }
            self.write_credit().await?;
        }
    }

    fn queue_kind_paced(
        &mut self,
        run: &mut Run,
        address: u64,
        kind: Kind,
        generation: u64,
        payload: &[u8],
    ) -> Result<bool, Error> {
        self.async_writer()?;
        let extent = self.extent_of(address);
        let held = if kind == Kind::Superblock {
            address == (generation & 1)
        } else {
            extent != 0 && self.alloc.is_held(extent)
        };
        if !held || payload.len() > self.page_capacity() {
            return Err(Error::InvalidArgument {
                what: "a paced page outside its held extent or page capacity",
            });
        }
        self.current_run_job(run)?;
        let continues = run.pages > 0
            && run.pages < self.config.extent_pages
            && run.first.checked_add(u64::from(run.pages)) == Some(address)
            && self.extent_of(run.first) == extent;
        if !continues {
            if !self.write_run_paced(run)? {
                return Ok(false);
            }
            run.first = address;
        }
        let at = usize::try_from(run.pages)
            .ok()
            .and_then(|n| n.checked_mul(self.config.page_size))
            .ok_or(corrupt(Malformed::TooLarge))?;
        let page = run
            .buf
            .as_mut_slice()
            .get_mut(
                at..at
                    .checked_add(self.config.page_size)
                    .ok_or(corrupt(Malformed::TooLarge))?,
            )
            .ok_or(corrupt(Malformed::TooLarge))?;
        let end = HEADER
            .checked_add(payload.len())
            .ok_or(corrupt(Malformed::TooLarge))?;
        page.get_mut(HEADER..end)
            .ok_or(corrupt(Malformed::TooLarge))?
            .copy_from_slice(payload);
        page::seal(page, address, kind, generation, payload.len())?;
        if let Some(cache) = self.cache.as_mut() {
            if kind == Kind::Node {
                cache.insert(address, payload);
            } else {
                cache.forget(address);
            }
        }
        if run.pages == 0 {
            run.job_ordinal = self.job_ordinal;
        }
        run.pages = run
            .pages
            .checked_add(1)
            .ok_or(corrupt(Malformed::TooLarge))?;
        // Dispatch is a separate acceptance boundary: this page already belongs to the
        // run even when it filled the extent and the attachment has no credit yet.
        Ok(true)
    }

    /// Dispatches an attached run without waiting. False preserves both its page count
    /// and buffer. A successful queue admission uses the existing whole-run budget.
    pub(crate) fn write_run_paced(&mut self, run: &mut Run) -> Result<bool, Error> {
        self.async_writer()?;
        if run.pages == 0 {
            return Ok(true);
        }
        self.current_run_job(run)?;
        if !self.alloc.is_held(self.extent_of(run.first)) {
            return Err(Error::InvalidArgument {
                what: "a page run written outside a held extent",
            });
        }
        self.reap()?;
        let run_bytes = self.write_reservation_for(None)?;
        let Some(writer) = self.writer.as_ref() else {
            return Err(io("dispatch paced pages", "no issuer attached"));
        };
        let reserve = usize::from(writer.read_wanted && writer.attached.batches() >= 2);
        let direct = writer.queued.is_empty()
            && writer.attached.out().saturating_add(reserve) < writer.attached.batches();
        if !direct && writer.queued.len() >= writer.queue_most {
            return Ok(false);
        }
        let warm = if direct {
            0
        } else {
            writer
                .queued
                .len()
                .checked_add(1)
                .and_then(|n| n.checked_mul(run_bytes))
                .ok_or(corrupt(Malformed::TooLarge))?
        };
        let bytes = usize::try_from(run.pages)
            .ok()
            .and_then(|n| n.checked_mul(self.config.page_size))
            .ok_or(corrupt(Malformed::TooLarge))?;
        let offset = Self::offset_in(self.config, run.first)?;
        let past = Self::past(offset, bytes)?;
        let pages = u64::from(run.pages);
        let fresh = self.take_buf()?;
        let mut buf = std::mem::replace(&mut run.buf, fresh);
        if let Err(error) = buf.set_len(bytes) {
            let spare = std::mem::replace(&mut run.buf, buf);
            self.give_buf(spare);
            return Err(io("cut paced pages to their run", error));
        }
        let queued = Queued {
            buf,
            offset,
            first: run.first,
            end: run
                .first
                .checked_add(pages)
                .ok_or(corrupt(Malformed::TooLarge))?,
            past,
        };
        run.pages = 0;
        self.io.writes = self.io.writes.saturating_add(1);
        self.io.pages_written = self.io.pages_written.saturating_add(pages);
        if direct {
            self.issue(queued)?;
        } else if let Some(writer) = self.writer.as_mut() {
            writer.queued.push_back(queued);
            self.warm_write_bytes = self.warm_write_bytes.max(warm);
            self.io.runs_queued = self.io.runs_queued.saturating_add(1);
            let n = writer.queued.len();
            self.io.runs_queued_most = self
                .io
                .runs_queued_most
                .max(u64::try_from(n).unwrap_or(u64::MAX));
            self.queue_peak = self.queue_peak.max(n);
        }
        Ok(true)
    }

    /// Dispatches the borrowed run through the issuer, yielding for actual output credit.
    /// A cancelled wait leaves its unsubmitted pages owned by the same run. Accepted
    /// transfers remain owned by the Store until completion, independently of this borrow.
    pub async fn write_run_async(&mut self, run: &mut Run) -> Result<(), Error> {
        loop {
            std::future::poll_fn(|cx| std::task::Poll::Ready(write_context(cx))).await?;
            self.checkpoint_idle()?;
            if self.write_run_paced(run)? {
                return Ok(());
            }
            self.write_credit().await?;
        }
    }

    async fn write_credit(&mut self) -> Result<(), Error> {
        let answered = {
            let mut completion = std::pin::pin!(self.wait_completion());
            std::future::poll_fn(|cx| {
                if let Err(error) = write_context(cx) {
                    return std::task::Poll::Ready(Err(error));
                }
                std::future::Future::poll(completion.as_mut(), cx)
            })
            .await?
        };
        if !answered {
            return Err(Error::InvalidArgument {
                what: "an async page write without an outstanding completion",
            });
        }
        Ok(())
    }

    fn queue_page_timed(
        &mut self,
        run: &mut Run,
        address: u64,
        payload: &[u8],
    ) -> Result<(), Error> {
        let extent = self.extent_of(address);
        if extent == 0 || !self.alloc.is_held(extent) {
            return Err(Error::InvalidArgument {
                what: "a page written outside a held extent",
            });
        }
        if payload.len() > self.page_capacity() {
            return Err(Error::InvalidArgument {
                what: "a page payload longer than the page",
            });
        }
        self.current_run_job(run)?;
        let continues = run.pages > 0
            && run.first.checked_add(u64::from(run.pages)) == Some(address)
            && self.extent_of(run.first) == extent;
        if !continues {
            self.write_run(run)?;
            run.first = address;
        }
        let size = self.config.page_size;
        let at = usize::try_from(run.pages)
            .ok()
            .and_then(|p| p.checked_mul(size))
            .ok_or(corrupt(Malformed::TooLarge))?;
        let page = run
            .buf
            .as_mut_slice()
            .get_mut(at..at.checked_add(size).ok_or(corrupt(Malformed::TooLarge))?)
            .ok_or(corrupt(Malformed::TooLarge))?;
        page.get_mut(HEADER..HEADER.saturating_add(payload.len()))
            .ok_or(Error::InvalidArgument {
                what: "a page payload longer than the page",
            })?
            .copy_from_slice(payload);
        let generation = self.durable.generation.saturating_add(1);
        page::seal(page, address, Kind::Node, generation, payload.len())?;
        // Only a validated, sealed page enters the write-through cache (research/34 §1).
        // A page just compacted can be read from memory while its run waits for the device.
        if let Some(c) = self.cache.as_mut() {
            c.insert(address, payload);
        }
        if run.pages == 0 {
            run.job_ordinal = self.job_ordinal;
        }
        run.pages = run.pages.saturating_add(1);
        // The run ends with its extent.
        if self
            .address_in_extent(address)
            .is_some_and(|i| i.saturating_add(1) == self.config.extent_pages)
        {
            self.write_run(run)?;
        }
        Ok(())
    }

    /// The page's index within its extent.
    fn address_in_extent(&self, address: u64) -> Option<u32> {
        u32::try_from(address.checked_rem(u64::from(self.config.extent_pages))?).ok()
    }

    /// Writes `run`'s queued pages out.
    pub fn write_run(&mut self, run: &mut Run) -> Result<(), Error> {
        blocking_allowed("a synchronous store run write in a runtime task")?;
        self.admission_open()?;
        self.checkpoint_idle()?;
        if run.pages == 0 {
            return Ok(());
        }
        if self.fenced {
            return Err(io(
                "write a store page",
                "the store was fenced by a failed write or flush",
            ));
        }
        self.current_run_job(run)?;
        if !self.alloc.is_held(self.extent_of(run.first)) {
            return Err(Error::InvalidArgument {
                what: "a page run written outside a held extent",
            });
        }
        let bytes = usize::try_from(run.pages)
            .ok()
            .and_then(|p| p.checked_mul(self.config.page_size))
            .ok_or(corrupt(Malformed::TooLarge))?;
        let offset = Self::offset_in(self.config, run.first)?;
        let past = Self::past(offset, bytes)?;
        self.io.writes = self.io.writes.saturating_add(1);
        self.io.pages_written = self.io.pages_written.saturating_add(u64::from(run.pages));
        let (first, pages) = (run.first, u64::from(run.pages));
        run.pages = 0;
        if self.writer.is_some() {
            return self.submit(run, first, pages, bytes, offset, past);
        }
        let started = self.timed.then(std::time::Instant::now);
        let written = run
            .buf
            .as_slice()
            .get(..bytes)
            .ok_or(corrupt(Malformed::TooLarge))
            .and_then(|b| {
                self.file
                    .as_ref()
                    .ok_or_else(Self::original_missing)?
                    .write_all_at(b, offset)
                    .map_err(|e| io("write a store page run", e))
            });
        self.io.write_ns = self.io.write_ns.saturating_add(elapsed_ns(started));
        self.fence(written)?;
        self.end = self.end.max(past);
        Ok(())
    }

    /// Hands `run`'s `bytes` at `offset` to the device's issuer and gives the run a spare
    /// buffer: the caller goes on while the device writes. With every batch it may have out
    /// already out, the run waits in write memory if the budget has room, else the caller waits
    /// for an answer, the bound on what is in flight and queued. The buffer is cut to the run's
    /// pages: runs complete in any order, and an extent's later run must not be overwritten by
    /// an earlier one's unused tail.
    fn submit(
        &mut self,
        run: &mut Run,
        first: u64,
        pages: u64,
        bytes: usize,
        offset: u64,
        past: u64,
    ) -> Result<(), Error> {
        let run_bytes = self.write_reservation_for(None)?;
        self.reap()?;
        let fresh = self.take_buf()?;
        let mut buf = std::mem::replace(&mut run.buf, fresh);
        buf.set_len(bytes)
            .map_err(|e| io("cut a run to its pages", e))?;
        let queued = Queued {
            buf,
            offset,
            first,
            end: first.saturating_add(pages),
            past,
        };
        let warm = self
            .writer
            .as_ref()
            .filter(|w| w.attached.out() >= w.attached.batches() || !w.queued.is_empty())
            .map(|w| {
                w.queued
                    .len()
                    .checked_add(1)
                    .and_then(|n| n.min(w.queue_most).checked_mul(run_bytes))
            });
        let warm = match warm {
            Some(Some(bytes)) => bytes,
            Some(None) => {
                self.give_buf(queued.buf);
                return Err(Error::LimitExceeded {
                    what: "queued write buffer bytes",
                    limit: u64::try_from(usize::MAX).unwrap_or(u64::MAX),
                });
            }
            None => 0,
        };
        let Some(w) = self.writer.as_mut() else {
            return Err(io("submit a store page run", "no issuer attached"));
        };
        let full = w.attached.out() >= w.attached.batches();
        if full && w.queued.len() < w.queue_most {
            w.queued.push_back(queued);
            self.warm_write_bytes = self.warm_write_bytes.max(warm);
            let n = u64::try_from(w.queued.len()).unwrap_or(u64::MAX);
            self.io.runs_queued = self.io.runs_queued.saturating_add(1);
            self.io.runs_queued_most = self.io.runs_queued_most.max(n);
            self.queue_peak = self
                .queue_peak
                .max(usize::try_from(n).unwrap_or(usize::MAX));
            return Ok(());
        }
        if full || !w.queued.is_empty() {
            // Runs go out in the order they were written: this one after those queued.
            w.queued.push_back(queued);
            self.warm_write_bytes = self.warm_write_bytes.max(warm);
            let mut t = None;
            while self
                .writer
                .as_ref()
                .is_some_and(|w| w.queued.len() > w.queue_most)
            {
                if !self.pump()? {
                    // Timed always, only when it waits: what the tuner prices write memory at.
                    t = t.or_else(|| Some(std::time::Instant::now()));
                    self.answer(true)?;
                    self.io.write_waits = self.io.write_waits.saturating_add(1);
                }
            }
            let ns = elapsed_ns(t);
            self.io.write_wait_ns = self.io.write_wait_ns.saturating_add(ns);
            self.io.budget_wait_ns = self.io.budget_wait_ns.saturating_add(ns);
            self.pump()?;
            return Ok(());
        }
        self.issue(queued)
    }

    /// Hands one run to the issuer, which must have room for it.
    fn issue(&mut self, run: Queued) -> Result<(), Error> {
        let started = self.timed.then(std::time::Instant::now);
        let Some(w) = self.writer.as_mut() else {
            return Err(io("submit a store page run", "no issuer attached"));
        };
        let mut batch = w.transfers.pop().unwrap_or_default();
        batch.push((run.buf, run.offset));
        let submitted = w
            .attached
            .submit(batch, false)
            .map_err(|e| io("submit a store page run", e));
        self.io.write_ns = self.io.write_ns.saturating_add(elapsed_ns(started));
        if submitted.is_err() {
            // A refused batch is dropped with the run's buffer in it: that loan ends here.
            self.lent = self.lent.saturating_sub(1);
        }
        let number = self.fence(submitted)?;
        if let Some(w) = self.writer.as_mut() {
            w.in_flight
                .push_back((number, run.first, run.end, run.past));
        }
        // The file's end moves when the run is answered: until then its bytes may not be there,
        // and a span reads no further than bytes written.
        self.io.submitted = self.io.submitted.saturating_add(1);
        Ok(())
    }

    /// Hands queued runs to the issuer while it has room; true if it handed any. A store a
    /// failed write fenced hands on none: it takes no more writes ([`Self::drain`] gives their
    /// buffers back).
    fn pump(&mut self) -> Result<bool, Error> {
        let mut any = false;
        loop {
            if self.fenced {
                return Ok(any);
            }
            let Some(w) = self.writer.as_mut() else {
                return Ok(any);
            };
            let reserve = usize::from(w.read_wanted && w.attached.batches() >= 2);
            if w.attached.out().saturating_add(reserve) >= w.attached.batches() {
                return Ok(any);
            }
            let Some(run) = w.queued.pop_front() else {
                return Ok(any);
            };
            self.issue(run)?;
            any = true;
        }
    }

    /// Takes one answer from the issuer, waiting for it when `wait`: its batch leaves the
    /// pages in flight, the file's end moves past it, and its buffer is kept for a run. A
    /// failed write fences the store. Returns whether an answer was taken.
    fn answer(&mut self, wait: bool) -> Result<bool, Error> {
        if wait {
            blocking_allowed("a synchronous issuer answer in a runtime task")?;
        }
        let Some(w) = self.writer.as_mut() else {
            return Ok(false);
        };
        let answered = if wait {
            w.attached.answer().map(Some)
        } else {
            w.attached.try_answer()
        };
        let answered = answered.map_err(|e| io("take a store page run's answer", e));
        self.accept_answer(answered)
    }

    /// Whether an issued read or write can still deliver an answer.
    pub(crate) fn io_outstanding(&self) -> bool {
        self.writer.as_ref().is_some_and(|w| w.attached.out() > 0)
    }

    /// Waits for one issued read or write through the attachment's reusable completion
    /// channel. Canceling this borrowed wait leaves its number, buffer and credit owned by
    /// the attachment; a later synchronous or asynchronous consumer takes the same answer.
    /// Returns false when no transfer is out. A read error is kept for its span; a write
    /// error fences the store, as with the synchronous answer path.
    pub async fn wait_completion(&mut self) -> Result<bool, Error> {
        let Some(w) = self.writer.as_mut().filter(|w| w.attached.out() > 0) else {
            return Ok(false);
        };
        let answered = match w.attached.answer_async().await {
            Ok(answered) => Ok(Some(answered)),
            Err(error) => {
                // A pending receive outside a runtime task was refused without consuming
                // any transfer. It is caller misuse, rather than a failed device/channel.
                let refused = matches!(
                    &error,
                    hyper_block::DiskError::Io { source, .. }
                        if source.kind() == std::io::ErrorKind::InvalidInput
                );
                let error = io("take a store page run's answer", error);
                if refused {
                    return Err(error);
                }
                Err(error)
            }
        };
        self.accept_answer(answered)
    }

    /// Accounts a numbered answer once, regardless of how its consumer waited for it.
    fn accept_answer(
        &mut self,
        answered: Result<Option<(u64, Answer)>, Error>,
    ) -> Result<bool, Error> {
        let result = self.fence(answered).and_then(|answer| match answer {
            Some(numbered) => self.route(numbered),
            None => Ok(false),
        });
        if let Err(error) = &result
            && let Some(checkpoint) = self.checkpoint.as_mut()
        {
            checkpoint.error.get_or_insert(error.clone());
            checkpoint.phase = CheckpointPhase::Failed;
        }
        result
    }

    /// Routes one answer: a read's to its span (or its buffers to the pool when the span is
    /// gone), a write's run out of flight and the file's end past it.
    fn route(&mut self, (number, answer): (u64, Answer)) -> Result<bool, Error> {
        if self
            .writer
            .as_ref()
            .is_some_and(|w| w.flush.as_ref().is_some_and(|f| f.number == number))
        {
            let result = match answer {
                Ok(batch) => {
                    self.give_transfers(batch);
                    Ok(())
                }
                Err(error) => self.fence(Err(io("flush a store", error))),
            };
            if let Some(flush) = self.writer.as_mut().and_then(|w| w.flush.as_mut()) {
                flush.result = Some(result.clone());
            }
            result?;
            return Ok(true);
        }
        // A read's answer: kept for its span, or its buffers pooled when the span is gone. A
        // failed read fails only the read that asked for it.
        let read = self.writer.as_mut().and_then(|w| {
            let i = w.reads.iter().position(|&n| n == number)?;
            w.reads.swap_remove(i);
            let orphan = w.orphans.iter().position(|&n| n == number);
            if let Some(j) = orphan {
                w.orphans.swap_remove(j);
            }
            Some(orphan.is_some())
        });
        if read.is_some() && answer.is_err() {
            // Each prefetch lends one buffer. The issuer drops it on a failed batch, so no
            // later claim or orphan release can return that loan to the pool.
            self.lent = self.lent.saturating_sub(1);
        }
        match read {
            Some(true) => {
                if let Ok(batch) = answer {
                    self.give_transfers(batch);
                }
                return Ok(true);
            }
            Some(false) => {
                if let Some(w) = self.writer.as_mut() {
                    w.parked.push((
                        number,
                        answer.map_err(|e| io("read a store page span ahead", e)),
                    ));
                }
                return Ok(true);
            }
            None => {}
        }
        let past = self.writer.as_mut().and_then(|w| {
            let at = w.in_flight.iter().position(|&(n, ..)| n == number)?;
            w.in_flight.remove(at).map(|(.., past)| past)
        });
        self.io.runs_answered = self.io.runs_answered.saturating_add(1);
        if answer.is_err() {
            // A write batch holds its run's one buffer ([`Self::issue`]), which the issuer drops
            // with a failed batch: that loan ends here.
            self.lent = self.lent.saturating_sub(1);
        }
        let batch = self.fence(answer.map_err(|e| io("write a store page run", e)))?;
        if let Some(past) = past {
            self.end = self.end.max(past);
        }
        self.give_transfers(batch);
        Ok(true)
    }

    /// Gives a batch's buffers back to the pool and keeps its emptied vector for the next
    /// batch, at most one a batch attached for.
    fn give_transfers(&mut self, mut batch: Transfers) {
        for (b, _) in batch.drain(..) {
            self.give_buf(b);
        }
        if let Some(w) = self.writer.as_mut()
            && w.transfers.len() < w.transfers.capacity()
        {
            w.transfers.push(batch);
        }
    }

    /// Takes every answer that has come, and hands queued runs on into the room they leave.
    fn reap(&mut self) -> Result<(), Error> {
        while self.answer(false)? {}
        self.pump()?;
        Ok(())
    }

    /// Whether every write to `extents` has landed, taking the answers that have come and never
    /// waiting: a maintenance worker reading them through its own handle then reads what was
    /// written ([`Self::worker`]).
    pub fn landed(&mut self, extents: &[u64]) -> Result<bool, Error> {
        self.reap()?;
        let per = u64::from(self.config.extent_pages);
        Ok(self.writer.as_ref().is_none_or(|w| {
            extents.iter().all(|&e| {
                let first = e.saturating_mul(per);
                let end = first.saturating_add(per);
                !(w.in_flight.iter().any(|&(_, a, b, _)| a < end && first < b)
                    || w.queued.iter().any(|q| q.first < end && first < q.end))
            })
        }))
    }

    /// [`Self::landed`], waiting for the answers it needs.
    pub fn settle_extents(&mut self, extents: &[u64]) -> Result<(), Error> {
        let per = u64::from(self.config.extent_pages);
        for &e in extents {
            let first = e.saturating_mul(per);
            self.settle(first, first.saturating_add(per))?;
        }
        Ok(())
    }

    /// Waits until no run in flight writes a page in `[first, end)`: a read of those pages then
    /// reads what was written.
    fn settle(&mut self, first: u64, end: u64) -> Result<(), Error> {
        if self.writer.as_ref().is_some_and(|writer| {
            writer
                .in_flight
                .iter()
                .any(|&(_, a, b, _)| a < end && first < b)
                || writer
                    .queued
                    .iter()
                    .any(|run| run.first < end && first < run.end)
        }) {
            blocking_allowed("a synchronous store extent wait in a runtime task")?;
        }
        self.reap()?;
        // A run queued counts as in flight: its pages are not in the file yet. Each answer
        // frees room the queue's oldest takes, so the loop ends within the queue and the batches.
        while self.writer.as_ref().is_some_and(|w| {
            w.in_flight.iter().any(|&(_, a, b, _)| a < end && first < b)
                || w.queued.iter().any(|q| q.first < end && first < q.end)
        }) {
            self.answer(true)?;
            self.pump()?;
        }
        Ok(())
    }

    /// Waits until nothing the store handed the device is out: every answer taken, each run
    /// landed or failed, and the first failure returned. A run still queued once a write has
    /// failed is never handed on, since a fenced store takes no more writes: its buffer goes back
    /// to the pool. A maintenance worker's job ends here, and the shard may write the job's
    /// extents again once it has the result, so nothing may still be in flight then.
    pub fn drain(&mut self) -> Result<(), Error> {
        if self
            .writer
            .as_ref()
            .is_some_and(|writer| !writer.queued.is_empty() || writer.attached.out() > 0)
        {
            blocking_allowed("a synchronous store drain in a runtime task")?;
        }
        let mut first = self.drain_error.take();
        // Each pass takes one answer or ends: the runs out, and those queued before it, bound it.
        loop {
            if let Err(error) = self.pump() {
                first.get_or_insert(error);
            }
            if self.fenced {
                while let Some(run) = self.writer.as_mut().and_then(|w| w.queued.pop_front()) {
                    self.give_buf(run.buf);
                }
            }
            let out = self.writer.as_ref().map_or(0, |w| w.attached.out());
            if out == 0 {
                break;
            }
            if let Err(error) = self.answer(true) {
                first.get_or_insert(error);
                // An answer that cannot come, the issuer stopped, leaves its batch counted out;
                // a stopped issuer joined its workers, so nothing of the store's is in flight.
                if self.writer.as_ref().map_or(0, |w| w.attached.out()) == out {
                    break;
                }
            }
        }
        if let Some(error) = self.fence_error.clone().or(first) {
            return Err(error);
        }
        if self.fenced && !self.checkpoint_aborted {
            return Err(io(
                "drain store page runs",
                "the store was fenced by a failed write or flush",
            ));
        }
        Ok(())
    }

    /// Advances terminal draining without waiting on a batch. A failed write fences new
    /// submissions, but every older issued batch is still received before its first failure
    /// is returned. False retains the remaining work; the caller awaits only when a batch
    /// is out, otherwise it resumes a bounded slice of queued work.
    pub(crate) fn drain_paced(&mut self) -> Result<bool, Error> {
        // Each pass consumes one admitted batch, or stops at the first not yet answered.
        let bound = self
            .writer
            .as_ref()
            .map_or(0, |w| w.attached.out().saturating_add(w.queued.len()));
        for _ in 0..=bound {
            if let Err(error) = self.pump() {
                self.drain_error.get_or_insert(error);
            }
            if self.fenced {
                while let Some(run) = self.writer.as_mut().and_then(|w| w.queued.pop_front()) {
                    self.give_buf(run.buf);
                }
            }
            let out = self.writer.as_ref().map_or(0, |w| w.attached.out());
            if out == 0 || self.drain_closed {
                break;
            }
            match self.answer(false) {
                Ok(true) => {}
                Ok(false) => return Ok(false),
                Err(error) => {
                    self.drain_error.get_or_insert(error);
                    // A failed batch consumes its answer. A closed issuer channel does
                    // not: the issuer's stop has already retired its in-flight transfers.
                    if self.writer.as_ref().map_or(0, |w| w.attached.out()) == out {
                        self.drain_closed = true;
                    }
                }
            }
        }
        let queued = self.writer.as_ref().is_some_and(|w| !w.queued.is_empty());
        if queued || (self.io_outstanding() && !self.drain_closed) {
            return Ok(false);
        }
        if let Some(error) = self.fence_error.clone().or(self.drain_error.take()) {
            return Err(error);
        }
        if self.fenced && !self.checkpoint_aborted {
            return Err(io(
                "drain store page runs",
                "the store was fenced by a failed write or flush",
            ));
        }
        Ok(true)
    }

    /// Retires the owner's device duplicates after terminal draining, yielding even when a
    /// different attachment's held write delays a worker from dropping this file.
    pub(crate) async fn retire_async(&mut self) -> Result<(), Error> {
        let Some(writer) = self.writer.as_mut() else {
            return Ok(());
        };
        writer
            .attached
            .retire_async()
            .await
            .map_err(|error| io("retire a store attachment", error))
    }

    /// Spares the store `bytes` of write memory for runs waiting for the device's issuer
    /// ([`Writer::queued`]): runs of a whole extent each. None by default, so a writer waits as
    /// soon as every batch is out.
    /// The most bytes queued in write memory at once since the last call: the write memory the
    /// cycle used, which the tuner never leaves write memory above.
    pub fn take_queue_peak(&mut self) -> usize {
        let now = self.writer.as_ref().map_or(0, |w| w.queued.len());
        let peak = std::mem::replace(&mut self.queue_peak, now).max(now);
        peak.saturating_mul(self.run_bytes())
    }

    /// The bytes a run takes: an extent's pages.
    pub fn run_bytes(&self) -> usize {
        self.config
            .page_size
            .saturating_mul(usize::try_from(self.config.extent_pages).unwrap_or(usize::MAX))
    }

    /// Fixed output roles in whole runs: one active run without an issuer; attached batches,
    /// the active replacement and one submit handoff with an issuer. Submit takes its spare
    /// before waiting for a full attachment, so the latter two can coexist. Input spans,
    /// other Builder working runs, point/page buffers, feed buffers and branch/memtable metadata
    /// are outside this scope, as are attachment/channel metadata and worker stacks.
    pub(crate) fn write_reservation_for(&self, batches: Option<usize>) -> Result<usize, Error> {
        let refused = || Error::LimitExceeded {
            what: "fixed write buffer bytes",
            limit: u64::try_from(usize::MAX).unwrap_or(u64::MAX),
        };
        let extents = usize::try_from(self.config.extent_pages).map_err(|_| refused())?;
        let run = self
            .config
            .page_size
            .checked_mul(extents)
            .ok_or_else(refused)?;
        let roles = match batches {
            Some(n) => n.checked_add(2).ok_or_else(refused)?,
            None => 1,
        };
        run.checked_mul(roles).ok_or_else(refused)
    }

    /// Output queue buffers retained warm after their run is answered. The fixed roles above
    /// already include the single transient submit handoff, which is not charged a second time.
    pub(crate) fn warm_write_bytes(&self) -> usize {
        self.warm_write_bytes
    }

    /// The write memory spared now, in bytes: whole runs.
    pub fn write_budget(&self) -> usize {
        self.write_budget_runs.saturating_mul(
            self.config
                .page_size
                .saturating_mul(usize::try_from(self.config.extent_pages).unwrap_or(usize::MAX)),
        )
    }

    pub fn set_write_budget(&mut self, bytes: usize) {
        let run = self
            .config
            .page_size
            .saturating_mul(usize::try_from(self.config.extent_pages).unwrap_or(usize::MAX))
            .max(1);
        self.write_budget_runs = bytes.checked_div(run).unwrap_or(0);
        if let Some(w) = self.writer.as_mut() {
            w.queue_most = self.write_budget_runs;
        }
    }

    /// Hands the store's runs to `issuer`, the device's, with up to `batches` out at once: a
    /// run is then written while its writer goes on, as the device's other volumes' writes are
    /// ([`Issuer::attach_deep`]). Its bound is the extent buffers the owner spares for them.
    pub fn attach(&mut self, issuer: &Issuer, batches: usize) -> Result<(), Error>
    where
        F: 'static,
    {
        self.attach_through(&issuer.attacher(), batches)
    }

    /// [`Self::attach`] through an owned way to the issuer: a maintenance worker's store, on
    /// the worker's own thread, which cannot borrow the issuer its shard was given.
    pub fn attach_through(&mut self, attacher: &Attacher, batches: usize) -> Result<(), Error>
    where
        F: 'static,
    {
        blocking_allowed("a synchronous store attachment in a runtime task")?;
        self.allocator_available()?;
        if self.spans_lent > 0
            || self.writer.as_ref().is_some_and(|writer| {
                !writer.reads.is_empty() || !writer.parked.is_empty() || !writer.orphans.is_empty()
            })
        {
            return Err(Error::InvalidArgument {
                what: "a store reattached while demand read owners remain",
            });
        }
        let mut orphans = Vec::new();
        orphans
            .try_reserve_exact(batches)
            .map_err(|error| io("reserve canceled read numbers", error))?;
        self.drain()?;
        let attached = attacher
            .attach_deep(
                self.file.as_ref().ok_or_else(Self::original_missing)?,
                batches,
            )
            .map_err(|e| io("attach a store to its device's issuer", e))?;
        self.writer = Some(Writer {
            attached,
            in_flight: VecDeque::with_capacity(batches),
            queued: VecDeque::new(),
            queue_most: self.write_budget_runs,
            reads: Vec::with_capacity(batches),
            parked: Vec::with_capacity(batches),
            orphans,
            transfers: Vec::with_capacity(batches),
            read_wanted: false,
            flush: None,
        });
        Ok(())
    }

    fn original_missing() -> Error {
        Error::InvalidArgument {
            what: "original store file owned by native retirement",
        }
    }

    pub(crate) fn prepare_original_retirement_watch(&mut self) -> Result<OriginalPhysical, Error> {
        blocking_allowed("original file retirement setup on a runtime")?;
        self.allocator_available()?;
        if self.file.is_none() || self.original.is_some() {
            return Err(Self::original_missing());
        }
        self.writer
            .as_mut()
            .ok_or(Error::InvalidArgument {
                what: "original file retirement without an attachment",
            })?
            .attached
            .prepare_retirement_watch()
            .map(OriginalPhysical)
            .map_err(|error| io("prepare original file physical retirement", error))
    }

    pub(crate) fn adopt_original<W>(
        &mut self,
        adoption: &mut OriginalAdoption<F, W>,
        physical: W,
    ) -> Result<(), Error>
    where
        F: Send + 'static,
        W: OriginalFence,
    {
        blocking_allowed("original file adoption on a runtime")?;
        self.allocator_available()?;
        if self.file.is_none() || self.original.is_some() {
            return Err(Self::original_missing());
        }
        match adoption.adopt(&mut self.file, physical) {
            Ok(original) => {
                self.original = Some(original);
                Ok(())
            }
            Err((physical, error)) => {
                // A cold refusal leaves the complete original in self.file.
                drop(physical);
                Err(io("adopt a store original file", error))
            }
        }
    }

    /// Writes a node page at `address`, in a held extent past the superblocks', for the next
    /// checkpoint. It is durable once that checkpoint is.
    pub fn write_page(&mut self, address: u64, payload: &[u8]) -> Result<(), Error> {
        self.checkpoint_idle()?;
        let extent = self.extent_of(address);
        if extent == 0 || !self.alloc.is_held(extent) {
            return Err(Error::InvalidArgument {
                what: "a page written outside a held extent",
            });
        }
        let generation = self.durable.generation.saturating_add(1);
        self.put(address, Kind::Node, generation, payload)?;
        if let Some(c) = self.cache.as_mut() {
            c.insert(address, payload);
        }
        Ok(())
    }

    /// Reads the node page at `address` and appends its payload to `out`.
    pub fn read_page(&mut self, address: u64, out: &mut Vec<u8>) -> Result<(), Error> {
        if let Some(c) = self.cache.as_mut()
            && c.get(address, out)
        {
            return Ok(());
        }
        self.read_page_uncached(address, out)
    }

    /// [`Self::read_page`] once the cache has missed: from a queued run, or the device, and
    /// cached.
    fn read_page_uncached(&mut self, address: u64, out: &mut Vec<u8>) -> Result<(), Error> {
        if self.queued_page(address, out)? {
            return Ok(());
        }
        blocking_allowed("a synchronous store page miss in a runtime task")?;
        if self.cache.as_ref().is_some_and(|c| c.in_ghost(address)) {
            self.io.ghost_misses = self.io.ghost_misses.saturating_add(1);
        }
        self.settle(address, address.saturating_add(1))?;
        let offset = Self::offset_in(self.config, address)?;
        self.io.reads = self.io.reads.saturating_add(1);
        self.io.pages_read = self.io.pages_read.saturating_add(1);
        // Timed always: a clock read beside a device read, what the tuner prices a miss at.
        let started = Some(std::time::Instant::now());
        let read = self
            .file
            .as_ref()
            .ok_or_else(Self::original_missing)?
            .read_exact_at(self.buf.as_mut_slice(), offset)
            .map_err(|e| io("read a store page", e));
        let ns = elapsed_ns(started);
        self.io.read_ns = self.io.read_ns.saturating_add(ns);
        self.io.device_reads = self.io.device_reads.saturating_add(1);
        self.io.device_read_ns = self.io.device_read_ns.saturating_add(ns);
        read?;
        let from = out.len();
        node_payload(self.buf.as_slice(), address, out)?;
        if let Some(c) = self.cache.as_mut()
            && let Some(payload) = out.get(from..)
        {
            c.insert(address, payload);
        }
        Ok(())
    }

    /// Decodes page `address` into `out` from a run still queued in write memory, if one holds
    /// it: its bytes are there, sealed, and waiting for the device would hold the read behind
    /// every run queued before it.
    fn queued_page(&mut self, address: u64, out: &mut Vec<u8>) -> Result<bool, Error> {
        let size = self.config.page_size;
        let Some(page) = self.writer.as_ref().and_then(|w| {
            let q = w
                .queued
                .iter()
                .find(|q| q.first <= address && address < q.end)?;
            let at = usize::try_from(address.checked_sub(q.first)?)
                .ok()?
                .checked_mul(size)?;
            q.buf.as_slice().get(at..at.checked_add(size)?)
        }) else {
            return Ok(false);
        };
        node_payload(page, address, out)?;
        self.io.queued_reads = self.io.queued_reads.saturating_add(1);
        Ok(true)
    }

    /// The first page at or past `address`, before `end`, that a queued run holds.
    fn first_queued(&self, address: u64, end: u64) -> Option<u64> {
        self.writer.as_ref().and_then(|w| {
            w.queued
                .iter()
                .filter(|q| q.first < end && address < q.end)
                .map(|q| q.first.max(address))
                .min()
        })
    }

    /// Lends a resident payload without issuing or waiting for a device read. A miss leaves
    /// the callback untouched, so an async reader can acquire its demand owners afterward.
    pub(crate) fn with_resident_page<R>(
        &mut self,
        address: u64,
        read: impl FnOnce(&[u8]) -> Result<R, Error>,
    ) -> Result<Option<R>, Error> {
        if self
            .cache
            .as_ref()
            .is_some_and(|cache| cache.contains(address))
            && let Some(payload) = self.cache.as_mut().and_then(|cache| cache.get_ref(address))
        {
            return read(payload).map(Some);
        }
        if self.fenced {
            return Ok(None);
        }
        let size = self.config.page_size;
        let Some(page) = self.writer.as_ref().and_then(|writer| {
            let run = writer
                .queued
                .iter()
                .find(|run| run.first <= address && address < run.end)?;
            let at = usize::try_from(address.checked_sub(run.first)?)
                .ok()?
                .checked_mul(size)?;
            run.buf.as_slice().get(at..at.checked_add(size)?)
        }) else {
            return Ok(None);
        };
        let payload = node_payload_ref(page, address)?;
        self.io.queued_reads = self.io.queued_reads.saturating_add(1);
        read(payload).map(Some)
    }

    /// Lends page `address`'s payload to `read`: in place from the cache when it holds the page,
    /// else read as [`Self::read_page`] reads it (and cached), into a buffer the store keeps.
    /// A point read copies only what it takes from the page, not the page.
    pub fn with_page<R>(
        &mut self,
        address: u64,
        read: impl FnOnce(&[u8]) -> Result<R, Error>,
    ) -> Result<R, Error> {
        if let Some(payload) = self.cache.as_mut().and_then(|c| c.get_ref(address)) {
            return read(payload);
        }
        let mut page = std::mem::take(&mut self.point);
        page.clear();
        // The cache was just asked: `read_page` asks again only to count, so read past it.
        let got = self.read_page_uncached(address, &mut page);
        let result = got.and_then(|()| read(&page));
        self.point = page;
        result
    }

    /// A span for a scan, which may stop after a page: its first read takes one page, and its
    /// reads double while the scan runs on.
    pub fn span(&mut self) -> Result<Span, Error> {
        self.span_with_floor(1)
    }

    /// A span for a compaction, which reads its inputs to their end: every read takes the rest
    /// of its extent.
    pub fn span_sequential(&mut self) -> Result<Span, Error> {
        self.span_with_floor(self.config.extent_pages)
    }

    fn span_with_floor(&mut self, floor: u32) -> Result<Span, Error> {
        let lent = self.spans_lent.checked_add(1).ok_or(Error::LimitExceeded {
            what: "borrowed span metadata",
            limit: u64::try_from(usize::MAX).unwrap_or(u64::MAX),
        })?;
        let most = self.spans_most_lent.max(lent);
        self.span_pending
            .try_reserve(most.saturating_sub(self.span_pending.len()))
            .map_err(|error| io("reserve span metadata returns", error))?;
        let pooled = self.span_pending.pop();
        let was_pooled = pooled.is_some();
        let mut pending = pooled.unwrap_or_default();
        let batches = self
            .writer
            .as_ref()
            .map_or(1, |writer| writer.attached.batches());
        if pending.capacity() < batches
            && let Err(error) = pending.try_reserve(batches.saturating_sub(pending.len()))
        {
            if was_pooled {
                self.span_pending.push(pending);
            }
            return Err(io("reserve a span's pending reads", error));
        }
        let buf = match self.take_buf() {
            Ok(buf) => buf,
            Err(error) => {
                if was_pooled {
                    self.span_pending.push(pending);
                }
                return Err(error);
            }
        };
        self.spans_lent = lent;
        self.spans_most_lent = most;
        Ok(Span {
            buf,
            first: 0,
            pages: 0,
            ahead: floor,
            floor,
            pending,
            depth: 1,
            prepared: None,
        })
    }

    /// Takes back a span its scan is done with.
    pub fn give_span(&mut self, mut span: Span) {
        if let Some((_, page)) = span.prepared.take() {
            self.give_page(page);
        }
        while let Some((number, ..)) = span.pending.pop_front() {
            self.release_read(number);
        }
        self.spans_lent = self.spans_lent.saturating_sub(1);
        if self.span_pending.len() < self.spans_most_lent {
            // span_with_floor reserved this slot before lending its metadata.
            self.span_pending.push(span.pending);
        }
        self.give_buf(span.buf);
    }

    /// Lets read `number` go unclaimed: its buffers back to the pool now if it has landed, else
    /// when it does.
    fn release_read(&mut self, number: u64) {
        let Some(w) = self.writer.as_mut() else {
            return;
        };
        match w.parked.iter().position(|&(n, _)| n == number) {
            Some(i) => {
                if let (_, Ok(batch)) = w.parked.swap_remove(i) {
                    self.give_transfers(batch);
                }
            }
            None => w.orphans.push(number),
        }
    }

    /// Reserve the configured attachment's pending-read roles before a batch is published.
    fn reserve_span_reads(&self, span: &mut Span) -> Result<(), Error> {
        let batches = self
            .writer
            .as_ref()
            .map_or(1, |writer| writer.attached.batches());
        if span.pending.capacity() < batches {
            span.pending
                .try_reserve(batches.saturating_sub(span.pending.len()))
                .map_err(|error| io("reserve a span's pending reads", error))?;
        }
        Ok(())
    }

    /// A demand page through the issuer, even for a one-page foreground span. False
    /// leaves `out` and the current span page unchanged; the owned read resumes after
    /// its numbered answer. Memory hits and every new device page keep Node validation.
    pub(crate) fn read_page_paced(
        &mut self,
        span: &mut Span,
        address: u64,
        out: &mut Vec<u8>,
    ) -> Result<bool, Error> {
        self.async_writer()?;
        self.reserve_span_reads(span)?;
        self.reap()?;
        let mut landed = false;
        if let Some(index) = span.pending_for(address) {
            let number = span
                .pending
                .get(index)
                .map(|&(number, ..)| number)
                .ok_or(corrupt(Malformed::Truncated))?;
            let answered = self.writer.as_mut().and_then(|writer| {
                let at = writer
                    .parked
                    .iter()
                    .position(|&(ready, _)| ready == number)?;
                Some(writer.parked.swap_remove(at).1)
            });
            let Some(answered) = answered else {
                return Ok(false);
            };
            for _ in 0..index {
                if let Some((number, ..)) = span.pending.pop_front() {
                    self.release_read(number);
                }
            }
            let (_, first, pages, _) = span
                .pending
                .pop_front()
                .ok_or(corrupt(Malformed::Truncated))?;
            let mut batch = answered?;
            if batch.len() != 1 {
                self.give_transfers(batch);
                return Err(corrupt(Malformed::CountMismatch));
            }
            let taken = batch.pop();
            if let Some(writer) = self.writer.as_mut()
                && writer.transfers.len() < writer.transfers.capacity()
            {
                writer.transfers.push(batch);
            }
            let (mut buf, _) = taken.ok_or(corrupt(Malformed::Truncated))?;
            if let Err(error) = buf.set_len(buf.capacity()) {
                self.give_buf(buf);
                return Err(io("size a paced demand page", error));
            }
            let old = std::mem::replace(&mut span.buf, buf);
            self.give_buf(old);
            span.first = first;
            span.pages = pages;
            landed = true;
        }
        if let Some(index) = address
            .checked_sub(span.first)
            .filter(|&index| index < u64::from(span.pages))
        {
            let at = usize::try_from(index)
                .ok()
                .and_then(|index| index.checked_mul(self.config.page_size))
                .ok_or(corrupt(Malformed::TooLarge))?;
            let end = at
                .checked_add(self.config.page_size)
                .ok_or(corrupt(Malformed::TooLarge))?;
            let page = span
                .buf
                .as_slice()
                .get(at..end)
                .ok_or(corrupt(Malformed::Truncated))?;
            let payload = node_payload_ref(page, address)?;
            if landed && let Some(cache) = self.cache.as_mut() {
                cache.insert(address, payload);
            }
            out.extend_from_slice(payload);
            return Ok(true);
        }
        if let Some((prepared, page)) = span.prepared.take() {
            if prepared == address {
                out.extend_from_slice(&page);
                self.give_page(page);
                return Ok(true);
            }
            self.give_page(page);
        }
        if self
            .cache
            .as_mut()
            .is_some_and(|cache| cache.get(address, out))
        {
            self.io.span_cache_hits = self.io.span_cache_hits.saturating_add(1);
            return Ok(true);
        }
        if self.queued_page(address, out)? {
            return Ok(true);
        }
        while let Some((number, ..)) = span.pending.pop_front() {
            self.release_read(number);
        }
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| io("read a paced demand page", "no issuer attached"))?;
        if writer
            .in_flight
            .iter()
            .any(|&(_, first, end, _)| first <= address && address < end)
            || writer.attached.out() >= writer.attached.batches()
        {
            writer.read_wanted = true;
            return Ok(false);
        }
        let offset = Self::offset_in(self.config, address)?;
        if Self::past(offset, self.config.page_size)? > self.end {
            return Err(corrupt(Malformed::Truncated));
        }
        let mut buf = self.take_buf()?;
        if let Err(error) = buf.set_len(self.config.page_size) {
            self.give_buf(buf);
            return Err(io("size a paced demand page", error));
        }
        let Some(writer) = self.writer.as_mut() else {
            self.give_buf(buf);
            return Err(io("read a paced demand page", "no issuer attached"));
        };
        let mut batch = writer.transfers.pop().unwrap_or_default();
        batch.push((buf, offset));
        let number = match writer.attached.submit_reads(batch) {
            Ok(number) => number,
            Err(error) => {
                self.lent = self.lent.saturating_sub(1);
                return Err(io("submit a paced demand page", error));
            }
        };
        writer.reads.push(number);
        writer.read_wanted = false;
        span.pending.push_back((number, address, 1, false));
        self.io.reads = self.io.reads.saturating_add(1);
        self.io.pages_read = self.io.pages_read.saturating_add(1);
        self.io.device_reads = self.io.device_reads.saturating_add(1);
        Ok(false)
    }

    /// Reads a demand page without a blocking fallback. The numbered read and current
    /// span remain owned across each borrowed completion wait; its caller owns cleanup.
    pub(crate) async fn read_page_async(
        &mut self,
        span: &mut Span,
        address: u64,
        out: &mut Vec<u8>,
    ) -> Result<(), Error> {
        loop {
            std::future::poll_fn(|cx| std::task::Poll::Ready(page_context(cx))).await?;
            if self.read_page_paced(span, address, out)? {
                return Ok(());
            }
            let answered = {
                let mut completion = std::pin::pin!(self.wait_completion());
                std::future::poll_fn(|cx| {
                    if let Err(error) = page_context(cx) {
                        return std::task::Poll::Ready(Err(error));
                    }
                    std::future::Future::poll(completion.as_mut(), cx)
                })
                .await?
            };
            if !answered {
                return Err(Error::InvalidArgument {
                    what: "an async demand page without an outstanding completion",
                });
            }
        }
    }

    /// Hands the read of `address` and the rest of its extent to the device's issuer for a
    /// compaction's `span`, without waiting: the read [`Self::read_page_ahead`] would make
    /// when the compaction reaches it, made while it works through the extent before. Only a
    /// compaction's span reads ahead, one extent at a time; nothing is handed over for pages a
    /// write still out or queued covers, and with no batch free the next one freed is kept for
    /// it.
    pub fn prefetch(&mut self, span: &mut Span, address: u64) -> Result<(), Error> {
        self.admission_open()?;
        if span.floor <= 1
            || span.pending.len() >= span.depth
            || span.holds(address)
            || span.pending_for(address).is_some()
            || (!self.fenced && span.prepared.as_ref().is_some_and(|(a, _)| *a == address))
            || self.cache.as_ref().is_some_and(|c| c.contains(address))
        {
            return Ok(());
        }
        let extent_pages = u64::from(self.config.extent_pages);
        let extent_end = self
            .extent_of(address)
            .checked_add(1)
            .and_then(|e| e.checked_mul(extent_pages))
            .ok_or(corrupt(Malformed::TooLarge))?;
        let size =
            u64::try_from(self.config.page_size).map_err(|_| corrupt(Malformed::TooLarge))?;
        let last = extent_end.min(self.end.checked_div(size).unwrap_or(0));
        let busy = self.first_queued(address, extent_end).is_some();
        self.reserve_span_reads(span)?;
        let Some(w) = self.writer.as_mut() else {
            return Ok(());
        };
        if busy
            || last <= address
            || w.in_flight
                .iter()
                .any(|&(_, a, b, _)| a < extent_end && address < b)
        {
            return Ok(());
        }
        if w.attached.out() >= w.attached.batches() {
            w.read_wanted = true;
            return Ok(());
        }
        let pages = u32::try_from(last.saturating_sub(address))
            .map_err(|_| corrupt(Malformed::TooLarge))?;
        let bytes = usize::try_from(pages)
            .ok()
            .and_then(|p| p.checked_mul(self.config.page_size))
            .ok_or(corrupt(Malformed::TooLarge))?;
        let offset = Self::offset_in(self.config, address)?;
        let mut buf = self.take_buf()?;
        buf.set_len(bytes)
            .map_err(|e| io("size a store page span read", e))?;
        let Some(w) = self.writer.as_mut() else {
            return Ok(());
        };
        let mut batch = w.transfers.pop().unwrap_or_default();
        batch.push((buf, offset));
        let number = match w.attached.submit_reads(batch) {
            Ok(number) => number,
            Err(error) => {
                // Refusal drops the one consumed buffer before any read number is recorded.
                self.lent = self.lent.saturating_sub(1);
                return Err(io("hand a store page span read to the issuer", error));
            }
        };
        w.reads.push(number);
        w.read_wanted = false;
        span.pending.push_back((number, address, pages, false));
        self.io.prefetches = self.io.prefetches.saturating_add(1);
        self.io.reads = self.io.reads.saturating_add(1);
        self.io.pages_read = self.io.pages_read.saturating_add(u64::from(pages));
        Ok(())
    }

    /// Whether a compaction's `span` can read `address` without waiting for the device: held,
    /// resident in memory with no pending target read, or its read ahead landed. Otherwise the
    /// read is handed to the issuer if it is not yet
    /// ([`Self::prefetch`]) and the answer is no: the compaction stops for now and goes on
    /// once it has landed, rather than hold the put that paces it behind the device. A scan's
    /// span, or a store with no issuer, always reads at once.
    pub fn ready(&mut self, span: &mut Span, address: u64) -> Result<bool, Error> {
        if hyper_rt::futures::current_task().is_some() {
            self.async_writer()?;
            self.reap()?;
            if span.floor <= 1 {
                let mut page = match span.prepared.take() {
                    Some((prepared, page)) if prepared == address => {
                        span.prepared = Some((prepared, page));
                        return Ok(true);
                    }
                    Some((_, mut page)) => {
                        page.clear();
                        page
                    }
                    None => self.take_page(),
                };
                let ready = self.read_page_paced(span, address, &mut page);
                match ready {
                    Ok(true) => {
                        span.prepared = Some((address, page));
                        return Ok(true);
                    }
                    result => {
                        self.give_page(page);
                        return result;
                    }
                }
            }
        }
        if span.floor <= 1 || self.writer.is_none() || span.holds(address) {
            return Ok(true);
        }
        let reaped = self.reap();
        if self.fenced
            && let Some((_, page)) = span.prepared.take()
        {
            self.give_page(page);
        }
        reaped?;
        // A pending target is claimed first. Otherwise keep a memory-ready payload: the
        // builder may evict its cache slot or submit its queued run before the cursor reads it.
        if span.pending_for(address).is_none() && !self.fenced {
            if span.prepared.as_ref().is_some_and(|(a, _)| *a == address) {
                return Ok(true);
            }
            if self.cache.as_ref().is_some_and(|c| c.contains(address))
                || self
                    .first_queued(address, address.saturating_add(1))
                    .is_some()
            {
                let mut page = match span.prepared.take() {
                    Some((_, mut page)) => {
                        page.clear();
                        page
                    }
                    None => self.take_page(),
                };
                let copied = if self
                    .cache
                    .as_ref()
                    .is_some_and(|c| c.peek(address, &mut page))
                {
                    self.io.span_cache_hits = self.io.span_cache_hits.saturating_add(1);
                    Ok(true)
                } else {
                    self.queued_page(address, &mut page)
                };
                match copied {
                    Ok(true) => {
                        span.prepared = Some((address, page));
                        return Ok(true);
                    }
                    result => {
                        self.give_page(page);
                        result?;
                    }
                }
            }
        }
        if span.pending_for(address).is_none() {
            // Reads ahead of somewhere else were mispredicted: let them go, and read this.
            while let Some((number, ..)) = span.pending.pop_front() {
                self.release_read(number);
            }
            self.prefetch(span, address)?;
        }
        let landed = span
            .pending_for(address)
            .and_then(|i| span.pending.get(i))
            .is_some_and(|&(n, ..)| {
                self.writer
                    .as_ref()
                    .is_some_and(|w| w.parked.iter().any(|&(p, _)| p == n))
            });
        let size =
            u64::try_from(self.config.page_size).map_err(|_| corrupt(Malformed::TooLarge))?;
        // Past the answered file end can still be a valid page whose write has not landed.
        // Without held memory, reading it would settle that write and block the paced step.
        let writing = self.writer.as_ref().is_some_and(|w| {
            w.in_flight
                .iter()
                .any(|&(_, first, end, _)| first <= address && address < end)
        });
        let past_end = address >= self.end.checked_div(size).unwrap_or(0) && !writing;
        if !landed && !past_end {
            self.io.prefetch_waits = self.io.prefetch_waits.saturating_add(1);
            if let Some((_, _, _, late)) = span
                .pending_for(address)
                .and_then(|i| span.pending.get_mut(i))
                && !*late
            {
                *late = true;
                let most = self.writer.as_ref().map_or(1, |w| w.attached.batches());
                span.depth = span.depth.saturating_add(1).min(most.max(1));
            }
        }
        Ok(landed || past_end)
    }

    /// The buffers of read `number`, waiting for its answer if it has not come.
    fn claim(&mut self, number: u64) -> Result<Transfers, Error> {
        loop {
            let Some(w) = self.writer.as_mut() else {
                return Err(io("claim a store page span read", "no issuer attached"));
            };
            if let Some(i) = w.parked.iter().position(|&(n, _)| n == number) {
                return w.parked.swap_remove(i).1;
            }
            if !w.reads.contains(&number) {
                return Err(io("claim a store page span read", "no such read is out"));
            }
            blocking_allowed("an unanswered span claimed synchronously in a runtime task")?;
            self.answer(true)?;
        }
    }

    /// [`Self::read_page`] for a scan, which reads pages in address order: a page `span` holds
    /// is taken from it, and any other is read with the rest of its extent, up to the file's
    /// end, in one call. A scan of a branch then reads an extent a call where it read a page,
    /// and every page is still verified as it is taken.
    pub fn read_page_ahead(
        &mut self,
        span: &mut Span,
        address: u64,
        out: &mut Vec<u8>,
    ) -> Result<(), Error> {
        let started = self.timed.then(std::time::Instant::now);
        let read = self
            .read_page_ahead_timed(span, address, out, false)
            .and_then(|range| {
                if let Some(range) = range {
                    out.extend_from_slice(
                        span.payload(&range).ok_or(corrupt(Malformed::Truncated))?,
                    );
                }
                Ok(())
            });
        self.io.ahead_ns = self.io.ahead_ns.saturating_add(elapsed_ns(started));
        read
    }

    /// Reads as [`Self::read_page_ahead`] reads, returning the verified payload's range in
    /// `span` when it lies there. A cache or queued page is appended to `out` instead. The span
    /// belongs to its cursor, so a disk read or a held page can be parsed without another copy.
    pub(crate) fn read_page_ahead_ref(
        &mut self,
        span: &mut Span,
        address: u64,
        out: &mut Vec<u8>,
    ) -> Result<Option<std::ops::Range<usize>>, Error> {
        let started = self.timed.then(std::time::Instant::now);
        let read = self.read_page_ahead_timed(span, address, out, true);
        self.io.ahead_ns = self.io.ahead_ns.saturating_add(elapsed_ns(started));
        read
    }

    fn read_page_ahead_timed(
        &mut self,
        span: &mut Span,
        address: u64,
        out: &mut Vec<u8>,
        transfer: bool,
    ) -> Result<Option<std::ops::Range<usize>>, Error> {
        let pending = span.pending_for(address);
        let memory = span
            .prepared
            .as_ref()
            .is_some_and(|(ready, _)| *ready == address)
            || span.holds(address)
            || self
                .cache
                .as_ref()
                .is_some_and(|cache| cache.contains(address))
            || self
                .first_queued(address, address.saturating_add(1))
                .is_some();
        if pending.is_none() && !memory {
            blocking_allowed("a cold span read synchronously in a runtime task")?;
        }
        if let Some(number) = pending
            .and_then(|index| span.pending.get(index))
            .map(|&(number, ..)| number)
            && !self
                .writer
                .as_ref()
                .is_some_and(|writer| writer.parked.iter().any(|&(ready, _)| ready == number))
        {
            // Refuse before removing the pending read or replacing its current page.
            blocking_allowed("an unanswered span read synchronously in a runtime task")?;
        }
        if let Some((prepared, mut page)) = span.prepared.take() {
            let copied = prepared == address && pending.is_none() && !self.fenced;
            if copied {
                if transfer && out.is_empty() {
                    std::mem::swap(out, &mut page);
                } else {
                    out.extend_from_slice(&page);
                }
            }
            self.give_page(page);
            if copied {
                return Ok(None);
            }
        }
        if let Some(i) = pending {
            // Read ahead: its extent becomes the span's, and reads ahead of extents passed go.
            for _ in 0..i {
                if let Some((number, ..)) = span.pending.pop_front() {
                    self.release_read(number);
                }
            }
            let (number, first, pages, _) = span
                .pending
                .pop_front()
                .ok_or(corrupt(Malformed::Truncated))?;
            // A read batch holds the one span; its emptied vector goes back for the next.
            let mut batch = self.claim(number)?;
            let taken = batch.pop();
            if let Some(w) = self.writer.as_mut()
                && w.transfers.len() < w.transfers.capacity()
            {
                w.transfers.push(batch);
            }
            let (mut buf, _) = taken.ok_or(corrupt(Malformed::Truncated))?;
            buf.set_len(buf.capacity())
                .map_err(|e| io("size a store page span read", e))?;
            let old = std::mem::replace(&mut span.buf, buf);
            self.give_buf(old);
            span.first = first;
            span.pages = pages;
        }
        let held = address
            .checked_sub(span.first)
            .filter(|&i| i < u64::from(span.pages));
        // A foreground scan's page that does not run on from its last read is the page a seek
        // asked for: demand, as a point read's is. Its look counts as an access and a miss
        // admits it, so short seeks repeated over a working set are served from memory as
        // RocksDB's block cache serves them. Pages read ahead, a run-on scan's pages and a
        // compaction's are only looked at: the look neither counts nor moves the page, so a long
        // scan admits one page a jump and promotes nothing it passes (S3-FIFO's scan resistance
        // holds).
        let demand = held.is_none() && span.floor == 1 && !span.runs_on(address);
        if held.is_none()
            && let Some(c) = self.cache.as_mut()
            && (if demand {
                c.get(address, out)
            } else {
                c.peek(address, out)
            })
        {
            self.io.span_cache_hits = self.io.span_cache_hits.saturating_add(1);
            return Ok(None);
        }
        if demand && self.cache.as_ref().is_some_and(|c| c.in_ghost(address)) {
            self.io.ghost_misses = self.io.ghost_misses.saturating_add(1);
        }
        if held.is_none() && self.queued_page(address, out)? {
            return Ok(None);
        }
        let index = match held {
            Some(i) => i,
            None => {
                self.fill(span, address)?;
                0
            }
        };
        let size = self.config.page_size;
        let at = usize::try_from(index)
            .ok()
            .and_then(|i| i.checked_mul(size))
            .ok_or(corrupt(Malformed::TooLarge))?;
        let page = span
            .buf
            .as_slice()
            .get(at..at.checked_add(size).ok_or(corrupt(Malformed::TooLarge))?)
            .ok_or(corrupt(Malformed::Truncated))?;
        let payload = node_payload_ref(page, address)?;
        if demand && let Some(c) = self.cache.as_mut() {
            c.insert(address, payload);
        }
        let from = at
            .checked_add(page::HEADER)
            .ok_or(corrupt(Malformed::TooLarge))?;
        let end = from
            .checked_add(payload.len())
            .ok_or(corrupt(Malformed::TooLarge))?;
        Ok(Some(from..end))
    }

    /// Reads `address` and the pages after it in its extent, up to the file's end, into `span`.
    fn fill(&mut self, span: &mut Span, address: u64) -> Result<(), Error> {
        blocking_allowed("a synchronous store span read in a runtime task")?;
        let extent_pages = u64::from(self.config.extent_pages);
        let extent_end = self
            .extent_of(address)
            .checked_add(1)
            .and_then(|e| e.checked_mul(extent_pages))
            .ok_or(corrupt(Malformed::TooLarge))?;
        // The read stops before the first page a queued run holds, which is read from the
        // queue when reached; pages a run in flight writes are read once it is answered.
        let extent_end = self.first_queued(address, extent_end).unwrap_or(extent_end);
        self.settle(address, extent_end)?;
        let size =
            u64::try_from(self.config.page_size).map_err(|_| corrupt(Malformed::TooLarge))?;
        let file_end = self
            .end
            .checked_div(size)
            .ok_or(corrupt(Malformed::TooLarge))?;
        let last = extent_end.min(file_end);
        if last <= address {
            return Err(corrupt(Malformed::Truncated));
        }
        // A read runs on from the last when it lands within what the next read, at double the
        // size, would cover (a branch's index page between two leaves is passed over unread):
        // it doubles, up to the extent. Any other starts again at the floor.
        let runs_on = span.runs_on(address);
        span.ahead = if runs_on {
            span.ahead.saturating_mul(2)
        } else {
            span.floor
        }
        .clamp(1, self.config.extent_pages);
        let pages = u32::try_from(last.saturating_sub(address))
            .map_err(|_| corrupt(Malformed::TooLarge))?
            .min(span.ahead);
        let bytes = usize::try_from(pages)
            .ok()
            .and_then(|p| p.checked_mul(self.config.page_size))
            .ok_or(corrupt(Malformed::TooLarge))?;
        let offset = Self::offset_in(self.config, address)?;
        span.pages = 0;
        self.io.reads = self.io.reads.saturating_add(1);
        self.io.pages_read = self.io.pages_read.saturating_add(u64::from(pages));
        let started = self.timed.then(std::time::Instant::now);
        let read = span
            .buf
            .as_mut_slice()
            .get_mut(..bytes)
            .ok_or(corrupt(Malformed::TooLarge))
            .and_then(|b| {
                self.file
                    .as_ref()
                    .ok_or_else(Self::original_missing)?
                    .read_exact_at(b, offset)
                    .map_err(|e| io("read a store page span", e))
            });
        self.io.read_ns = self.io.read_ns.saturating_add(elapsed_ns(started));
        read?;
        span.first = address;
        span.pages = pages;
        Ok(())
    }

    fn admission_open(&self) -> Result<(), Error> {
        if self.closing.is_some() {
            Err(Error::InvalidArgument {
                what: "new work after store finish began",
            })
        } else {
            Ok(())
        }
    }

    fn checkpoint_idle(&self) -> Result<(), Error> {
        if self.checkpoint.is_some() {
            Err(Error::InvalidArgument {
                what: "allocator or writes changed during a retained checkpoint",
            })
        } else {
            Ok(())
        }
    }

    fn allocator_available(&self) -> Result<(), Error> {
        self.admission_open()?;
        self.allocator_idle()
    }

    fn allocator_idle(&self) -> Result<(), Error> {
        self.checkpoint_idle()?;
        if self.checkpoint_aborted {
            return Err(Error::InvalidArgument {
                what: "allocator changed after terminal checkpoint abandonment",
            });
        }
        Ok(())
    }

    fn async_writer(&self) -> Result<(), Error> {
        self.admission_open()?;
        if self.fenced {
            return Err(io(
                "write paced store pages",
                "the store was fenced by a failed write or flush",
            ));
        }
        if self.writer.is_none() {
            return Err(Error::InvalidArgument {
                what: "paced store I/O without an attached issuer",
            });
        }
        Ok(())
    }

    fn prepare_checkpoint(&mut self, root: Option<u64>, applied: u64) -> Result<Checkpoint, Error> {
        self.async_writer()?;
        if self.alloc.refs().is_empty() {
            return Err(Error::InvalidArgument {
                what: "a checkpoint on a granted worker store",
            });
        }
        let generation = self
            .durable
            .generation
            .checked_add(1)
            .ok_or(corrupt(Malformed::TooLarge))?;
        let max_map = superblock::max_map_extents(self.page_capacity());
        let per_extent = (self.page_capacity() / 4)
            .checked_mul(
                usize::try_from(self.config.extent_pages)
                    .map_err(|_| corrupt(Malformed::TooLarge))?,
            )
            .ok_or(corrupt(Malformed::TooLarge))?;
        max_map
            .checked_mul(per_extent)
            .ok_or(corrupt(Malformed::TooLarge))?;
        // Validate every old reference before changing any. A duplicate would release the
        // same map extent twice rather than describe a valid allocator snapshot.
        for (at, &extent) in self.durable.map.iter().enumerate() {
            if extent == 0
                || !self.alloc.is_held(extent)
                || self
                    .durable
                    .map
                    .get(..at)
                    .is_some_and(|old| old.contains(&extent))
            {
                return Err(corrupt(Malformed::OutOfRange));
            }
        }
        let capacity = self.page_capacity();
        self.checkpoint_payload
            .try_reserve(capacity.saturating_sub(self.checkpoint_payload.len()))
            .map_err(|_| Error::LimitExceeded {
                what: "bytes of a checkpoint page",
                limit: u64::try_from(capacity).unwrap_or(u64::MAX),
            })?;
        self.checkpoint_map.clear();
        self.checkpoint_map
            .try_reserve(max_map)
            .map_err(|_| Error::LimitExceeded {
                what: "allocator map extents a superblock names",
                limit: u64::try_from(max_map).unwrap_or(u64::MAX),
            })?;
        let run = self.run()?;
        let mut map = std::mem::take(&mut self.checkpoint_map);
        while map
            .len()
            .checked_mul(per_extent)
            .ok_or(corrupt(Malformed::TooLarge))?
            < self.alloc.refs().len()
        {
            let allocated = if map.len() >= max_map {
                Err(Error::LimitExceeded {
                    what: "allocator map extents a superblock names",
                    limit: u64::try_from(max_map).unwrap_or(u64::MAX),
                })
            } else {
                self.alloc.allocate()
            };
            match allocated {
                Ok(extent) => map.push(extent),
                Err(mut error) => {
                    for &extent in &map {
                        if let Err(failed) = self.alloc.give_back(extent) {
                            error = failed;
                        }
                    }
                    self.checkpoint_map = map;
                    self.give_run(run);
                    return Err(error);
                }
            }
        }
        for &extent in &self.durable.map {
            self.alloc.release(extent)?;
        }
        let mut payload = std::mem::take(&mut self.checkpoint_payload);
        payload.clear();
        Ok(Checkpoint {
            root,
            applied,
            generation,
            refs: self.alloc.refs().len(),
            map,
            payload,
            run,
            phase: CheckpointPhase::Map(0),
            error: None,
        })
    }

    fn checkpoint_flush(&mut self, number: &mut Option<u64>) -> Result<bool, Error> {
        self.reap()?;
        let Some(writer) = self.writer.as_mut() else {
            return Err(io("flush a paced checkpoint", "no issuer attached"));
        };
        if let Some(number) = *number {
            let flush = writer
                .flush
                .as_mut()
                .filter(|flush| flush.number == number)
                .ok_or(corrupt(Malformed::CountMismatch))?;
            let Some(result) = flush.result.take() else {
                return Ok(false);
            };
            writer.flush = None;
            result?;
            return Ok(true);
        }
        if writer.attached.out() != 0 || !writer.queued.is_empty() {
            return Ok(false);
        }
        if writer.flush.is_some() {
            return Err(corrupt(Malformed::CountMismatch));
        }
        let batch = writer.transfers.pop().unwrap_or_default();
        let submitted = writer
            .attached
            .submit(batch, true)
            .map_err(|error| io("flush a paced checkpoint", error));
        let issued = self.fence(submitted)?;
        if let Some(writer) = self.writer.as_mut() {
            writer.flush = Some(Flush {
                number: issued,
                result: None,
            });
        }
        *number = Some(issued);
        self.io.syncs = self.io.syncs.saturating_add(1);
        Ok(false)
    }

    fn checkpoint_step(&mut self, checkpoint: &mut Checkpoint, budget: u64) -> Result<bool, Error> {
        for _ in 0..budget {
            match &mut checkpoint.phase {
                CheckpointPhase::Map(at) => {
                    if *at >= checkpoint.refs {
                        if !self.write_run_paced(&mut checkpoint.run)? {
                            return Ok(false);
                        }
                        checkpoint.phase = CheckpointPhase::DrainMap;
                        continue;
                    }
                    let per = self.page_capacity() / 4;
                    let end = at
                        .checked_add(per)
                        .ok_or(corrupt(Malformed::TooLarge))?
                        .min(checkpoint.refs);
                    checkpoint.payload.clear();
                    for count in self
                        .alloc
                        .refs()
                        .get(*at..end)
                        .ok_or(corrupt(Malformed::Truncated))?
                    {
                        checkpoint.payload.extend_from_slice(&count.to_le_bytes());
                    }
                    let page = at.checked_div(per).ok_or(corrupt(Malformed::TooLarge))?;
                    let extent_pages = usize::try_from(self.config.extent_pages)
                        .map_err(|_| corrupt(Malformed::TooLarge))?;
                    let extent = *checkpoint
                        .map
                        .get(
                            page.checked_div(extent_pages)
                                .ok_or(corrupt(Malformed::TooLarge))?,
                        )
                        .ok_or(corrupt(Malformed::Truncated))?;
                    let in_extent = u32::try_from(
                        page.checked_rem(extent_pages)
                            .ok_or(corrupt(Malformed::TooLarge))?,
                    )
                    .map_err(|_| corrupt(Malformed::TooLarge))?;
                    let address = self.address(extent, in_extent)?;
                    if !self.queue_kind_paced(
                        &mut checkpoint.run,
                        address,
                        Kind::Map,
                        checkpoint.generation,
                        &checkpoint.payload,
                    )? {
                        return Ok(false);
                    }
                    *at = end;
                }
                CheckpointPhase::DrainMap => {
                    if !self.drain_paced()? {
                        return Ok(false);
                    }
                    checkpoint.phase = CheckpointPhase::Flush1(None);
                }
                CheckpointPhase::Flush1(number) => {
                    if !self.checkpoint_flush(number)? {
                        return Ok(false);
                    }
                    checkpoint.phase = CheckpointPhase::EncodeSuperblock;
                }
                CheckpointPhase::EncodeSuperblock => {
                    let sb = Superblock {
                        page_size: self.durable.page_size,
                        extent_pages: self.config.extent_pages,
                        generation: checkpoint.generation,
                        applied: checkpoint.applied,
                        root: checkpoint.root,
                        extents: u64::try_from(checkpoint.refs)
                            .map_err(|_| corrupt(Malformed::TooLarge))?,
                        map: std::mem::take(&mut checkpoint.map),
                    };
                    let len = sb.encoded_len()?;
                    checkpoint.payload.resize(len, 0);
                    let encoded = sb.encode(&mut checkpoint.payload);
                    checkpoint.map = sb.map;
                    encoded?;
                    checkpoint.phase = CheckpointPhase::Superblock;
                }
                CheckpointPhase::Superblock => {
                    if !self.queue_kind_paced(
                        &mut checkpoint.run,
                        checkpoint.generation & 1,
                        Kind::Superblock,
                        checkpoint.generation,
                        &checkpoint.payload,
                    )? {
                        return Ok(false);
                    }
                    checkpoint.phase = CheckpointPhase::DrainSuperblock;
                }
                CheckpointPhase::DrainSuperblock => {
                    if !self.write_run_paced(&mut checkpoint.run)? || !self.drain_paced()? {
                        return Ok(false);
                    }
                    checkpoint.phase = CheckpointPhase::Flush2(None);
                }
                CheckpointPhase::Flush2(number) => {
                    if !self.checkpoint_flush(number)? {
                        return Ok(false);
                    }
                    checkpoint.phase = CheckpointPhase::Commit;
                }
                CheckpointPhase::Commit => {
                    let sb = Superblock {
                        page_size: self.durable.page_size,
                        extent_pages: self.config.extent_pages,
                        generation: checkpoint.generation,
                        applied: checkpoint.applied,
                        root: checkpoint.root,
                        extents: u64::try_from(checkpoint.refs)
                            .map_err(|_| corrupt(Malformed::TooLarge))?,
                        map: std::mem::take(&mut checkpoint.map),
                    };
                    let old = std::mem::replace(&mut self.durable, sb);
                    self.checkpoint_map = old.map;
                    self.alloc.durable();
                    return Ok(true);
                }
                CheckpointPhase::Failed => {
                    match self.drain_paced() {
                        Ok(false) => return Ok(false),
                        Ok(true) => {}
                        Err(error) => {
                            checkpoint.error.get_or_insert(error);
                        }
                    }
                    return Err(checkpoint
                        .error
                        .clone()
                        .unwrap_or_else(|| io("checkpoint a store", "checkpoint failed")));
                }
            }
        }
        Ok(false)
    }

    /// One counted slice of a retained attached checkpoint. False keeps the same map,
    /// generation, page cursor and numbered flush across the next completion wait.
    pub(crate) fn checkpoint_paced(
        &mut self,
        root: Option<u64>,
        applied: u64,
        budget: u64,
    ) -> Result<bool, Error> {
        self.admission_open()?;
        if self
            .checkpoint
            .as_ref()
            .is_some_and(|checkpoint| checkpoint.root != root || checkpoint.applied != applied)
        {
            return Err(Error::InvalidArgument {
                what: "a different checkpoint while a barrier is retained",
            });
        }
        let mut checkpoint = match self.checkpoint.take() {
            Some(checkpoint) => checkpoint,
            None => self.prepare_checkpoint(root, applied)?,
        };
        let mut result = self.checkpoint_step(&mut checkpoint, budget);
        if let Err(error) = &result
            && !matches!(checkpoint.phase, CheckpointPhase::Failed)
        {
            self.fence_error.get_or_insert_with(|| error.clone());
            self.fenced = true;
            self.cache = None;
            checkpoint.error.get_or_insert(error.clone());
            checkpoint.phase = CheckpointPhase::Failed;
            result = Ok(false);
        }
        if matches!(result, Ok(true) | Err(_)) {
            if let Err(error) = &result {
                self.checkpoint_aborted = true;
                self.drain_error.get_or_insert(error.clone());
            }
            self.give_run(checkpoint.run);
            self.checkpoint_payload = checkpoint.payload;
        } else {
            self.checkpoint = Some(checkpoint);
        }
        result
    }

    /// Terminal cleanup only: accepted writes end before the unfinished barrier's Run
    /// is returned. Its extents stay conservatively held or pending; no allocator reuse
    /// or durable generation is advanced. Reopen recovers only the durable file state.
    pub(crate) fn abort_checkpoint_paced(&mut self) -> Result<bool, Error> {
        if self.checkpoint.is_none() {
            return Ok(true);
        }
        let drained = self.drain_paced();
        if matches!(drained, Ok(false)) {
            return Ok(false);
        }
        let Some(checkpoint) = self.checkpoint.take() else {
            return Ok(true);
        };
        let result = checkpoint.error.map_or(drained, Err);
        self.give_run(checkpoint.run);
        self.checkpoint_payload = checkpoint.payload;
        if let Some(writer) = self.writer.as_mut() {
            writer.flush = None;
        }
        self.checkpoint_aborted = true;
        if let Err(error) = &result {
            self.fence_error.get_or_insert_with(|| error.clone());
            self.drain_error.get_or_insert(error.clone());
        }
        self.fenced = true;
        self.cache = None;
        result
    }

    /// Makes an attached checkpoint durable from a runtime task. Cancellation keeps the
    /// same barrier; a later call with the same root and index resumes it. Allocator and
    /// ordinary write methods refuse changes until it completes or fails after draining.
    pub async fn checkpoint_async(&mut self, root: Option<u64>, applied: u64) -> Result<(), Error> {
        loop {
            std::future::poll_fn(|cx| std::task::Poll::Ready(checkpoint_context(cx))).await?;
            if self.checkpoint_paced(root, applied, u64::from(self.config.extent_pages))? {
                return Ok(());
            }
            if self.io_outstanding() {
                let answered = {
                    let mut completion = std::pin::pin!(self.wait_completion());
                    std::future::poll_fn(|cx| {
                        if let Err(error) = checkpoint_context(cx) {
                            return std::task::Poll::Ready(Err(error));
                        }
                        std::future::Future::poll(completion.as_mut(), cx)
                    })
                    .await
                };
                if let Err(error) = answered {
                    if matches!(error, Error::InvalidArgument { .. }) {
                        return Err(error);
                    }
                    if let Some(checkpoint) = self.checkpoint.as_mut() {
                        checkpoint.error.get_or_insert(error);
                        checkpoint.phase = CheckpointPhase::Failed;
                    }
                }
            } else {
                // The remaining work is a counted CPU slice, never a repeated I/O probe.
                hyper_rt::futures::yield_now().await;
            }
        }
    }

    /// Makes a checkpoint naming `root` with state through Raft index `applied`, durable when
    /// this returns, in the order the module describes.
    pub fn checkpoint(&mut self, root: Option<u64>, applied: u64) -> Result<(), Error> {
        blocking_allowed("a synchronous store checkpoint in a runtime task")?;
        self.allocator_available()?;
        let generation = self
            .durable
            .generation
            .checked_add(1)
            .ok_or(corrupt(Malformed::TooLarge))?;
        // The new map's extents: enough pages for every extent's count, its own included, so
        // the extents are taken until the count they must hold stops growing.
        let per_page = self.page_capacity() / 4;
        let per_extent =
            per_page.saturating_mul(usize::try_from(self.config.extent_pages).unwrap_or(0));
        let max_map = superblock::max_map_extents(self.page_capacity());
        let mut map = Vec::new();
        while map.len().saturating_mul(per_extent) < self.alloc.refs().len() {
            if map.len() >= max_map {
                return Err(Error::LimitExceeded {
                    what: "allocator map extents a superblock names",
                    limit: u64::try_from(max_map).unwrap_or(u64::MAX),
                });
            }
            map.push(self.alloc.allocate()?);
        }
        // The old map is not named by this checkpoint: released now, so the counts persisted
        // show it free, and kept from reuse (pending) until this checkpoint is durable.
        for &extent in &self.durable.map {
            self.alloc.release(extent)?;
        }
        // The counts as this checkpoint names them, written page by page into the map extents.
        let refs: Vec<u32> = self.alloc.refs().to_vec();
        let mut chunks = refs.chunks(per_page);
        'extents: for &extent in &map {
            for i in 0..self.config.extent_pages {
                let Some(chunk) = chunks.next() else {
                    break 'extents;
                };
                let bytes: Vec<u8> = chunk.iter().flat_map(|c| c.to_le_bytes()).collect();
                let address = self.address(extent, i)?;
                self.put(address, Kind::Map, generation, &bytes)?;
            }
        }
        self.sync()?;
        let page_size = self.durable.page_size;
        let sb = Superblock {
            page_size,
            extent_pages: self.config.extent_pages,
            generation,
            applied,
            root,
            extents: u64::try_from(refs.len()).map_err(|_| corrupt(Malformed::TooLarge))?,
            map,
        };
        let mut payload = vec![0u8; sb.encoded_len()?];
        sb.encode(&mut payload)?;
        self.put(sb.slot(), Kind::Superblock, generation, &payload)?;
        self.sync()?;
        // Durable: every extent this checkpoint no longer names, the old map's included, is free.
        self.durable = sb;
        self.alloc.durable();
        Ok(())
    }

    /// Finishes accepted work without consuming the store. Cancellation retains its phase,
    /// first error and every buffer; only actual attachment retirement completes the finish.
    pub async fn finish_async(&mut self) -> Result<(), Error> {
        let mut finish = std::pin::pin!(self.finish_inner());
        std::future::poll_fn(|cx| {
            if let Err(error) = finish_context(cx) {
                return std::task::Poll::Ready(Err(error));
            }
            std::future::Future::poll(finish.as_mut(), cx)
        })
        .await
    }

    fn close_error(&mut self, error: Error) {
        if let Some(closing) = self.closing.as_mut() {
            closing.error.get_or_insert(error);
        }
    }

    async fn finish_inner(&mut self) -> Result<(), Error> {
        if self.closing.is_none() {
            self.closing = Some(Closing {
                phase: ClosePhase::Drain,
                error: self.fence_error.clone(),
            });
        }
        loop {
            let phase = self.closing.as_ref().map(|closing| closing.phase);
            match phase {
                Some(ClosePhase::Drain) | Some(ClosePhase::Abort) => {
                    let advanced = if phase == Some(ClosePhase::Drain) {
                        self.drain_paced()
                    } else {
                        self.abort_checkpoint_paced()
                    };
                    match advanced {
                        Ok(false) => {
                            if self.io_outstanding() {
                                if let Err(error) = self.wait_completion().await {
                                    self.close_error(error);
                                }
                            } else {
                                // Only a counted queued CPU slice remains; no readiness polling.
                                hyper_rt::futures::yield_now().await;
                            }
                        }
                        result => {
                            if let Err(error) = result {
                                self.close_error(error);
                            }
                            if let Some(closing) = self.closing.as_mut() {
                                closing.phase = if phase == Some(ClosePhase::Drain) {
                                    ClosePhase::Abort
                                } else {
                                    ClosePhase::Retire
                                };
                            }
                        }
                    }
                }
                Some(ClosePhase::Retire) => {
                    let retired = self.retire_async().await;
                    if self
                        .writer
                        .as_ref()
                        .is_some_and(|writer| !writer.attached.is_retired())
                    {
                        // Refusal retains the owner and phase; no file is exposed on this path.
                        return retired.and(Err(Error::InvalidArgument {
                            what: "store attachment retirement not completed",
                        }));
                    }
                    if let Err(error) = retired {
                        self.close_error(error);
                    }
                    self.writer = None;
                    if let Some(closing) = self.closing.as_mut() {
                        closing.phase = ClosePhase::Original;
                    }
                }
                Some(ClosePhase::Original) => {
                    if let Some(original) = self.original.as_mut() {
                        let closed = original.wait().await;
                        if !original.is_retired() {
                            // Context/admission refusal cannot establish physical file close.
                            return Err(closed.err().map_or_else(
                                || Error::InvalidArgument {
                                    what: "original file close not completed",
                                },
                                |error| io("retire a store original file", error),
                            ));
                        }
                        if let Err(error) = closed {
                            self.close_error(io("retire a store original file", error));
                        }
                    }
                    if let Some(closing) = self.closing.as_mut() {
                        closing.phase = ClosePhase::Done;
                    }
                }
                Some(ClosePhase::Done) => {
                    return self
                        .closing
                        .as_ref()
                        .and_then(|closing| closing.error.clone())
                        .map_or(Ok(()), Err);
                }
                None => {
                    return Err(Error::InvalidArgument {
                        what: "store finish lost its retained phase",
                    });
                }
            }
        }
    }

    pub(crate) fn finished(&self) -> bool {
        self.writer.is_none()
            && self.original.as_ref().is_none_or(OriginalLease::is_retired)
            && self
                .closing
                .as_ref()
                .is_some_and(|closing| closing.phase == ClosePhase::Done)
    }

    /// True only for a runtime owner whose original file was physically closed off-shard.
    pub(crate) fn original_retired(&self) -> bool {
        self.file.is_none()
            && self
                .original
                .as_ref()
                .is_some_and(OriginalLease::is_retired)
    }

    /// Checked extraction performs no I/O, receipt wait or native join.
    pub(crate) fn into_file_finished(mut self) -> IntoFile<Self, F> {
        if !self.finished() {
            return IntoFile::Refused {
                owner: self,
                error: Error::InvalidArgument {
                    what: "file extraction before store finish",
                },
            };
        }
        let Some(file) = self.file.take() else {
            return IntoFile::Refused {
                owner: self,
                error: Self::original_missing(),
            };
        };
        let result = self
            .closing
            .take()
            .and_then(|closing| closing.error)
            .map_or(Ok(()), Err);
        IntoFile::Finished { file, result }
    }

    /// A cold caller drains synchronously. An entered runtime receives its unchanged owner
    /// unless a borrowed async finish already established physical quiescence.
    pub fn into_file(mut self) -> IntoFile<Self, F> {
        if self.finished() {
            return self.into_file_finished();
        }
        if self.file.is_none() {
            return IntoFile::Refused {
                owner: self,
                error: Self::original_missing(),
            };
        }
        if hyper_rt::registry::current_shard().is_some() {
            return IntoFile::Refused {
                owner: self,
                error: Error::InvalidArgument {
                    what: "unfinished store consumed by a runtime",
                },
            };
        }
        let drained = self.drain();
        let mut first = self
            .closing
            .as_ref()
            .and_then(|closing| closing.error.clone())
            .or_else(|| drained.err());
        if let Some(writer) = self.writer.as_mut()
            && !writer.attached.is_retired()
            && let Err(error) = writer.attached.retire_blocking()
        {
            first.get_or_insert_with(|| io("retire a store attachment", error));
        }
        if self
            .writer
            .as_ref()
            .is_some_and(|writer| !writer.attached.is_retired())
        {
            let error = first.unwrap_or(Error::InvalidArgument {
                what: "file extraction before physical attachment retirement",
            });
            let closing = self.closing.get_or_insert(Closing {
                phase: ClosePhase::Retire,
                error: None,
            });
            closing.error.get_or_insert_with(|| error.clone());
            return IntoFile::Refused { owner: self, error };
        }
        self.writer = None;
        self.closing = Some(Closing {
            phase: ClosePhase::Done,
            error: first,
        });
        self.into_file_finished()
    }
}

#[cfg(test)]
pub(crate) fn cold_file<O, F>(outcome: IntoFile<O, F>) -> (F, Result<(), Error>) {
    match outcome {
        IntoFile::Finished { file, result } => (file, result),
        IntoFile::Refused { error, .. } => panic!("cold file extraction refused: {error:?}"),
    }
}

#[cfg(test)]
#[path = "../../tests/support/shared_sim.rs"]
mod checkpoint_sim;

#[cfg(test)]
mod checkpoint_tests {
    use super::checkpoint_sim::SharedSim;
    use super::*;
    use hyper_block::buf::Alignment;
    use hyper_block::sim::{Crash, Fault};
    use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

    #[test]
    fn failed_terminal_abort_preserves_error_and_refuses_allocator_admission() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config {
            page_size: 4096,
            extent_pages: 4,
            max_extents: 256,
        };
        let file = SharedSim::new(
            Alignment::new(config.page_size).unwrap(),
            Alignment::new(512).unwrap(),
            7,
        )
        .unwrap();
        let observed = file.try_clone().unwrap();
        let mut store = Store::create(file, config).unwrap();
        let old = store.allocate_extent().unwrap();
        let old_root = store.address(old, 0).unwrap();
        store.write_page(old_root, b"durable before abort").unwrap();
        store.checkpoint(Some(old_root), 1).unwrap();
        let fresh = store.allocate_extent().unwrap();
        let root = store.address(fresh, 0).unwrap();
        store.write_page(root, b"unfinished checkpoint").unwrap();
        store.release(old).unwrap();
        let issuer = Issuer::start_for(directory.path(), 1, 1).unwrap();
        store.attach(&issuer, 1).unwrap();
        observed.inject(Fault::WriteError).unwrap();
        let mut runtime = LocalRuntime::new(&RuntimeConfig {
            shards: 1,
            tasks_per_shard: 1,
            pin: false,
            cores: Vec::new(),
            timers_per_shard: 1,
            interests_per_shard: 2,
            ring_entries: 1,
            batch: 1,
            step_budget_ns: 1_000_000_000,
            timer_tick_ns: 100_000,
            spin_ns: 0,
            page_bytes: config.page_size,
            wake_tracking: None,
        })
        .unwrap();
        let (store, failed) = runtime
            .block_on(async move {
                let failed = loop {
                    assert!(!store.checkpoint_paced(Some(root), 2, 1).unwrap());
                    if store.io_outstanding() {
                        break store.wait_completion().await.unwrap_err();
                    }
                    hyper_rt::futures::yield_now().await;
                };
                assert!(matches!(failed, Error::Io { .. }));
                assert_eq!(store.abort_checkpoint_paced(), Err(failed.clone()));
                let refs = store.refs().to_vec();
                let generation = store.generation();
                assert!(matches!(
                    store.allocate_extent(),
                    Err(Error::InvalidArgument { .. })
                ));
                assert!(matches!(store.grant(1), Err(Error::InvalidArgument { .. })));
                assert!(matches!(
                    store.grant_back(&[]),
                    Err(Error::InvalidArgument { .. })
                ));
                assert!(matches!(
                    store.begin_job(&[], 0, 0),
                    Err(Error::InvalidArgument { .. })
                ));
                assert_eq!(store.refs(), refs);
                assert_eq!(store.generation(), generation);
                // Terminal Trunk cleanup may still release an unpublished extent; it never
                // makes that extent reusable before a durable checkpoint.
                store.release(fresh).unwrap();
                (store, failed)
            })
            .unwrap();
        assert_eq!(store.io_stats().buffers_out, 0);
        let (file, landed) = crate::store::cold_file(store.into_file());
        assert_eq!(landed, Err(failed));
        eprintln!(
            "terminal abort physical operations: {:?}",
            observed.stats().unwrap()
        );
        drop(issuer);
        file.crash(Crash::LoseAll).unwrap();
        file.clear_faults().unwrap();
        let (mut recovered, checkpoint) = Store::open(file, config).unwrap();
        assert_eq!(checkpoint.applied, 1);
        let mut value = Vec::new();
        recovered
            .read_page(checkpoint.root.unwrap(), &mut value)
            .unwrap();
        assert_eq!(value, b"durable before abort");
    }
}
