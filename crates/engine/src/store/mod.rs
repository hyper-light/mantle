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
use hyper_block::buf::AlignedBuf;
use hyper_block::issuer::{Attached, Issuer};
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
    file: F,
    config: Config,
    alloc: Allocator,
    /// The durable checkpoint.
    durable: Superblock,
    /// One page's aligned buffer, reused by every read and write.
    buf: AlignedBuf,
    /// A write or flush failed: the store takes no more.
    fenced: bool,
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
    /// Extent buffers given back by spans, runs and answered writes, for the next to need one:
    /// a fresh one costs a page fault for each of its pages, milliseconds a compaction's
    /// cursors on a busy machine. It holds no more than were ever out at once.
    pool: Vec<AlignedBuf>,
    lent: usize,
    /// The buffer a point read's page is read into on a cache miss ([`Store::with_page`]).
    point: Vec<u8>,
    most_lent: usize,
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
    parked: Vec<(u64, Result<Vec<AlignedBuf>, Error>)>,
    orphans: Vec<u64>,
    /// A compaction found no batch free for its read: the next one freed is kept for it, not
    /// handed to a queued run, so a write queue that never empties cannot starve its reads.
    /// Writes keep every other batch, so with two or more the reserve never stops them.
    read_wanted: bool,
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
}

impl Lists {
    fn clear(&mut self) {
        self.extents.clear();
        self.counts.clear();
        self.index.clear();
        self.range.clear();
        self.hashes.clear();
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
        self.pending.iter().position(|&(_, first, pages)| {
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
    /// number, first page and pages ([`Store::prefetch`]); and how many extents it reads ahead,
    /// one more each time its compaction finds a page not yet landed, up to the batches: the
    /// device's latency over the time the compaction takes through an extent, found as it runs.
    pending: VecDeque<(u64, u64, u32)>,
    depth: usize,
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

    fn run_buf(file: &F, config: Config) -> Result<AlignedBuf, Error> {
        let bytes = config
            .page_size
            .checked_mul(
                usize::try_from(config.extent_pages).map_err(|_| corrupt(Malformed::TooLarge))?,
            )
            .ok_or(corrupt(Malformed::TooLarge))?;
        let mut run = AlignedBuf::zeroed(bytes, file.alignment())
            .map_err(|e| io("allocate an extent buffer", e))?;
        run.extend_zeros(bytes)
            .map_err(|e| io("allocate an extent buffer", e))?;
        Ok(run)
    }

    /// Creates a store in `file`, which must be empty: generation 1, no root, applied 0, durable
    /// when this returns.
    pub fn create(file: F, config: Config) -> Result<Self, Error> {
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
            file,
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
            end: 0,
            io: IoStats::default(),
            cache: None,
            range_filter_bits: RANGE_FILTER_BITS,
            writer: None,
            write_budget_runs: 0,
            queue_peak: 0,
            pool: Vec::new(),
            lent: 0,
            point: Vec::new(),
            most_lent: 0,
            pages: Vec::new(),
            pages_lent: 0,
            pages_most_lent: 0,
            lists: Vec::new(),
            lists_lent: 0,
            lists_most_lent: 0,
            forgetting: VecDeque::new(),
            timed: false,
        };
        store.checkpoint(None, 0)?;
        Ok(store)
    }

    /// Opens the store in `file` at its newest durable checkpoint: the newer of the two
    /// superblock copies that verify, its map read back. A file whose copies both fail is corrupt.
    pub fn open(file: F, config: Config) -> Result<(Self, Recovered), Error> {
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
                file,
                config,
                alloc,
                durable: sb,
                buf,
                fenced: false,
                end,
                io: IoStats::default(),
                cache: None,
                range_filter_bits: RANGE_FILTER_BITS,
                writer: None,
                write_budget_runs: 0,
                queue_peak: 0,
                pool: Vec::new(),
                lent: 0,
                point: Vec::new(),
                most_lent: 0,
                pages: Vec::new(),
                pages_lent: 0,
                pages_most_lent: 0,
                lists: Vec::new(),
                lists_lent: 0,
                lists_most_lent: 0,
                forgetting: VecDeque::new(),
                timed: false,
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

    /// The durable checkpoint's generation.
    pub fn generation(&self) -> u64 {
        self.durable.generation
    }

    /// An extent for new pages, held once.
    pub fn allocate_extent(&mut self) -> Result<u64, Error> {
        self.alloc.allocate()
    }

    /// One more reference to a held extent (a new node naming an existing branch).
    pub fn retain(&mut self, extent: u64) -> Result<(), Error> {
        self.alloc.retain(extent)
    }

    /// One reference fewer; an extent left with none is reused once a checkpoint that no
    /// longer names it is durable.
    pub fn release(&mut self, extent: u64) -> Result<(), Error> {
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

    fn fence<T>(&mut self, result: Result<T, Error>) -> Result<T, Error> {
        if result.is_err() {
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
        let synced = self.file.sync_data().map_err(|e| io("flush a store", e));
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
                Self::run_buf(&self.file, self.config)?
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
        })
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
        let started = self.timed.then(std::time::Instant::now);
        let queued = self.queue_page_timed(run, address, payload);
        self.io.queue_ns = self.io.queue_ns.saturating_add(elapsed_ns(started));
        queued
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
        // The page enters the cache as it is written, as SplinterDB writes through its cache
        // (research/34 §1): a page just compacted is read without I/O, as the OS's cache serves
        // a buffered write. A page never read again leaves first (S3-FIFO's small queue).
        if let Some(c) = self.cache.as_mut() {
            c.insert(address, payload);
        }
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
        if run.pages == 0 {
            return Ok(());
        }
        if self.fenced {
            return Err(io(
                "write a store page",
                "the store was fenced by a failed write or flush",
            ));
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
        let Some(w) = self.writer.as_mut() else {
            return Err(io("submit a store page run", "no issuer attached"));
        };
        let full = w.attached.out() >= w.attached.batches();
        if full && w.queued.len() < w.queue_most {
            w.queued.push_back(queued);
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
        let submitted = w
            .attached
            .submit(vec![(run.buf, run.offset)], false)
            .map_err(|e| io("submit a store page run", e));
        self.io.write_ns = self.io.write_ns.saturating_add(elapsed_ns(started));
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

    /// Hands queued runs to the issuer while it has room; true if it handed any.
    fn pump(&mut self) -> Result<bool, Error> {
        let mut any = false;
        loop {
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
        let Some(w) = self.writer.as_mut() else {
            return Ok(false);
        };
        let answered = if wait {
            w.attached.answer().map(Some)
        } else {
            w.attached.try_answer()
        };
        let answered = answered.map_err(|e| io("take a store page run's answer", e));
        let Some(numbered) = self.fence(answered)? else {
            return Ok(false);
        };
        self.route(numbered)
    }

    /// Waits, as a hyper-rt task, for the device's next answer, takes it as [`Self::answer`]
    /// does and hands queued runs on into the room it leaves: the shard's thread runs its other
    /// tasks meanwhile. False with nothing out, nothing to wait for. Dropped before the answer
    /// comes, it leaves the answer queued for the next wait or take.
    pub async fn wait_answer(&mut self) -> Result<bool, Error> {
        let Some(w) = self.writer.as_mut() else {
            return Ok(false);
        };
        if w.attached.out() == 0 {
            return Ok(false);
        }
        let answered = w
            .attached
            .answer_async()
            .await
            .map_err(|e| io("take a store page run's answer", e));
        let numbered = self.fence(answered)?;
        self.route(numbered)?;
        self.pump()?;
        Ok(true)
    }

    /// Routes one answer: a read's to its span (or its buffers to the pool when the span is
    /// gone), a write's run out of flight and the file's end past it.
    fn route(
        &mut self,
        (number, answer): (u64, Result<Vec<AlignedBuf>, hyper_block::DiskError>),
    ) -> Result<bool, Error> {
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
        match read {
            Some(true) => {
                for b in answer.unwrap_or_default() {
                    self.give_buf(b);
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
        let buffers = self.fence(answer.map_err(|e| io("write a store page run", e)))?;
        if let Some(past) = past {
            self.end = self.end.max(past);
        }
        for b in buffers {
            self.give_buf(b);
        }
        Ok(true)
    }

    /// Takes every answer that has come, and hands queued runs on into the room they leave.
    fn reap(&mut self) -> Result<(), Error> {
        while self.answer(false)? {}
        self.pump()?;
        Ok(())
    }

    /// Waits until no run in flight writes a page in `[first, end)`: a read of those pages then
    /// reads what was written.
    fn settle(&mut self, first: u64, end: u64) -> Result<(), Error> {
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

    /// Waits for every run in flight: each has landed, or the store is fenced and the failure
    /// returned.
    pub fn drain(&mut self) -> Result<(), Error> {
        self.pump()?;
        while self
            .writer
            .as_ref()
            .is_some_and(|w| w.attached.out() > 0 || !w.queued.is_empty())
        {
            self.answer(true)?;
            self.pump()?;
        }
        Ok(())
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
        self.drain()?;
        let attached = issuer
            .attach_deep(&self.file, batches)
            .map_err(|e| io("attach a store to its device's issuer", e))?;
        self.writer = Some(Writer {
            attached,
            in_flight: VecDeque::with_capacity(batches),
            queued: VecDeque::new(),
            queue_most: self.write_budget_runs,
            reads: Vec::with_capacity(batches),
            parked: Vec::with_capacity(batches),
            orphans: Vec::new(),
            read_wanted: false,
        });
        Ok(())
    }

    /// Writes a node page at `address`, in a held extent past the superblocks', for the next
    /// checkpoint. It is durable once that checkpoint is.
    pub fn write_page(&mut self, address: u64, payload: &[u8]) -> Result<(), Error> {
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
        Ok(Span {
            buf: self.take_buf()?,
            first: 0,
            pages: 0,
            ahead: 1,
            floor: 1,
            pending: VecDeque::new(),
            depth: 1,
        })
    }

    /// A span for a compaction, which reads its inputs to their end: every read takes the rest
    /// of its extent.
    pub fn span_sequential(&mut self) -> Result<Span, Error> {
        let extent = self.config.extent_pages;
        Ok(Span {
            buf: self.take_buf()?,
            first: 0,
            pages: 0,
            ahead: extent,
            floor: extent,
            pending: VecDeque::new(),
            depth: 1,
        })
    }

    /// Takes back a span its scan is done with.
    pub fn give_span(&mut self, mut span: Span) {
        while let Some((number, ..)) = span.pending.pop_front() {
            self.release_read(number);
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
                if let (_, Ok(buffers)) = w.parked.swap_remove(i) {
                    for b in buffers {
                        self.give_buf(b);
                    }
                }
            }
            None => w.orphans.push(number),
        }
    }

    /// Hands the read of `address` and the rest of its extent to the device's issuer for a
    /// compaction's `span`, without waiting: the read [`Self::read_page_ahead`] would make
    /// when the compaction reaches it, made while it works through the extent before. Only a
    /// compaction's span reads ahead, one extent at a time; nothing is handed over for pages a
    /// write still out or queued covers, and with no batch free the next one freed is kept for
    /// it.
    pub fn prefetch(&mut self, span: &mut Span, address: u64) -> Result<(), Error> {
        if span.floor <= 1
            || span.pending.len() >= span.depth
            || span.holds(address)
            || span.pending_for(address).is_some()
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
        let number = w
            .attached
            .submit_reads(vec![(buf, offset)])
            .map_err(|e| io("hand a store page span read to the issuer", e))?;
        w.reads.push(number);
        w.read_wanted = false;
        span.pending.push_back((number, address, pages));
        self.io.prefetches = self.io.prefetches.saturating_add(1);
        self.io.reads = self.io.reads.saturating_add(1);
        self.io.pages_read = self.io.pages_read.saturating_add(u64::from(pages));
        Ok(())
    }

    /// Whether a compaction's `span` can read `address` without waiting for the device: held,
    /// or its read ahead landed. Otherwise the read is handed to the issuer if it is not yet
    /// ([`Self::prefetch`]) and the answer is no: the compaction stops for now and goes on
    /// once it has landed, rather than hold the put that paces it behind the device. A scan's
    /// span, or a store with no issuer, always reads at once.
    pub fn ready(&mut self, span: &mut Span, address: u64) -> Result<bool, Error> {
        if span.floor <= 1 || self.writer.is_none() || span.holds(address) {
            return Ok(true);
        }
        self.reap()?;
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
        let past_end = address >= self.end.checked_div(size).unwrap_or(0);
        if !landed && !past_end {
            self.io.prefetch_waits = self.io.prefetch_waits.saturating_add(1);
            let most = self.writer.as_ref().map_or(1, |w| w.attached.batches());
            span.depth = span.depth.saturating_add(1).min(most.max(1));
        }
        Ok(landed || past_end)
    }

    /// The buffers of read `number`, waiting for its answer if it has not come.
    fn claim(&mut self, number: u64) -> Result<Vec<AlignedBuf>, Error> {
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
            .read_page_ahead_timed(span, address, out)
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
        let read = self.read_page_ahead_timed(span, address, out);
        self.io.ahead_ns = self.io.ahead_ns.saturating_add(elapsed_ns(started));
        read
    }

    fn read_page_ahead_timed(
        &mut self,
        span: &mut Span,
        address: u64,
        out: &mut Vec<u8>,
    ) -> Result<Option<std::ops::Range<usize>>, Error> {
        if let Some(i) = span.pending_for(address) {
            // Read ahead: its extent becomes the span's, and reads ahead of extents passed go.
            for _ in 0..i {
                if let Some((number, ..)) = span.pending.pop_front() {
                    self.release_read(number);
                }
            }
            let (number, first, pages) = span
                .pending
                .pop_front()
                .ok_or(corrupt(Malformed::Truncated))?;
            let mut buf = self
                .claim(number)?
                .into_iter()
                .next()
                .ok_or(corrupt(Malformed::Truncated))?;
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
                    .read_exact_at(b, offset)
                    .map_err(|e| io("read a store page span", e))
            });
        self.io.read_ns = self.io.read_ns.saturating_add(elapsed_ns(started));
        read?;
        span.first = address;
        span.pages = pages;
        Ok(())
    }

    /// Makes a checkpoint naming `root` with state through Raft index `applied`, durable when
    /// this returns, in the order the module describes.
    pub fn checkpoint(&mut self, root: Option<u64>, applied: u64) -> Result<(), Error> {
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

    /// The store's file, its work done, and whether every run handed to the device's issuer
    /// landed: the file comes back either way, as a crash's recovery reopens it, and a run that
    /// failed is reported here, not lost (it fenced the store already).
    pub fn into_file(mut self) -> (F, Result<(), Error>) {
        let drained = self.drain();
        self.writer = None;
        (self.file, drained)
    }
}
