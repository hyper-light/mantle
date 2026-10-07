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
    /// The most queue steps one cache eviction took.
    pub cache_evict_steps_most: u64,
    /// Extent buffers taken for spans, runs and submissions, and those of them allocated fresh
    /// because the pool had none.
    pub buffers_taken: u64,
    pub buffers_fresh: u64,
}

/// Nanoseconds since `since`, none when timing is off (`Store::set_timed`).
fn elapsed_ns(since: Option<std::time::Instant>) -> u64 {
    since.map_or(0, |t| {
        u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX)
    })
}

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
    /// The device's issuer, when the owner attaches the store to one ([`Store::attach`]).
    writer: Option<Writer>,
    /// Extent buffers given back by spans, runs and answered writes, for the next to need one:
    /// a fresh one costs a page fault for each of its pages, milliseconds a compaction's
    /// cursors on a busy machine. It holds no more than were ever out at once.
    pool: Vec<AlignedBuf>,
    lent: usize,
    most_lent: usize,
    /// Page buffers lent to readers (a cursor's path, a point read), kept when given back: at
    /// most as many as were ever out at once.
    pages: Vec<Vec<u8>>,
    pages_lent: usize,
    pages_most_lent: usize,
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

/// Pages a scan reads ahead ([`Store::read_page_ahead`]): an extent's buffer, the first page it
/// holds, and the pages it holds. A scan reads one finished branch's pages, which no write
/// changes while the branch is held, so the pages a span holds stay the file's.
#[derive(Debug)]
pub struct Span {
    buf: AlignedBuf,
    first: u64,
    pages: u32,
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
    let header = page::verify(page, address)?;
    if header.kind != Kind::Node {
        return Err(corrupt(Malformed::UnknownTag(0)));
    }
    out.extend_from_slice(page::payload(page, header)?);
    Ok(())
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
            writer: None,
            pool: Vec::new(),
            lent: 0,
            most_lent: 0,
            pages: Vec::new(),
            pages_lent: 0,
            pages_most_lent: 0,
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
                writer: None,
                pool: Vec::new(),
                lent: 0,
                most_lent: 0,
                pages: Vec::new(),
                pages_lent: 0,
                pages_most_lent: 0,
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

    /// Gives point reads a page cache of `pages` pages (none at 0), replacing any it had. Its
    /// memory is `pages` times the page size, taken now.
    pub fn set_cache(&mut self, pages: usize) {
        self.cache = (pages > 0).then(|| cache::Cache::new(pages, self.config.page_size));
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
    /// already out, it first waits for one to be answered, the bound on what is in flight. The
    /// buffer is cut to the run's pages: runs complete in any order, and an extent's later run
    /// must not be overwritten by an earlier one's unused tail.
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
        let full = self
            .writer
            .as_ref()
            .is_some_and(|w| w.attached.out() >= w.attached.batches());
        if full {
            let t = self.timed.then(std::time::Instant::now);
            self.answer(true)?;
            self.io.write_waits = self.io.write_waits.saturating_add(1);
            self.io.write_wait_ns = self.io.write_wait_ns.saturating_add(elapsed_ns(t));
        }
        let fresh = self.take_buf()?;
        let mut buf = std::mem::replace(&mut run.buf, fresh);
        buf.set_len(bytes)
            .map_err(|e| io("cut a run to its pages", e))?;
        let started = self.timed.then(std::time::Instant::now);
        let Some(w) = self.writer.as_mut() else {
            return Err(io("submit a store page run", "no issuer attached"));
        };
        let submitted = w
            .attached
            .submit(vec![(buf, offset)], false)
            .map_err(|e| io("submit a store page run", e));
        self.io.write_ns = self.io.write_ns.saturating_add(elapsed_ns(started));
        let number = self.fence(submitted)?;
        if let Some(w) = self.writer.as_mut() {
            w.in_flight
                .push_back((number, first, first.saturating_add(pages), past));
        }
        // The file's end moves when the run is answered: until then its bytes may not be there,
        // and a span reads no further than bytes written.
        self.io.submitted = self.io.submitted.saturating_add(1);
        Ok(())
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
        let Some((number, answer)) = self.fence(answered)? else {
            return Ok(false);
        };
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

    /// Takes every answer that has come.
    fn reap(&mut self) -> Result<(), Error> {
        while self.answer(false)? {}
        Ok(())
    }

    /// Waits until no run in flight writes a page in `[first, end)`: a read of those pages then
    /// reads what was written.
    fn settle(&mut self, first: u64, end: u64) -> Result<(), Error> {
        self.reap()?;
        while self
            .writer
            .as_ref()
            .is_some_and(|w| w.in_flight.iter().any(|&(_, a, b, _)| a < end && first < b))
        {
            self.answer(true)?;
        }
        Ok(())
    }

    /// Waits for every run in flight: each has landed, or the store is fenced and the failure
    /// returned.
    pub fn drain(&mut self) -> Result<(), Error> {
        while self.writer.as_ref().is_some_and(|w| w.attached.out() > 0) {
            self.answer(true)?;
        }
        Ok(())
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
        self.settle(address, address.saturating_add(1))?;
        let offset = Self::offset_in(self.config, address)?;
        self.io.reads = self.io.reads.saturating_add(1);
        self.io.pages_read = self.io.pages_read.saturating_add(1);
        let started = self.timed.then(std::time::Instant::now);
        let read = self
            .file
            .read_exact_at(self.buf.as_mut_slice(), offset)
            .map_err(|e| io("read a store page", e));
        self.io.read_ns = self.io.read_ns.saturating_add(elapsed_ns(started));
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

    /// A span for a scan: an extent's buffer, holding no pages yet.
    pub fn span(&mut self) -> Result<Span, Error> {
        Ok(Span {
            buf: self.take_buf()?,
            first: 0,
            pages: 0,
        })
    }

    /// Takes back a span its scan is done with.
    pub fn give_span(&mut self, span: Span) {
        self.give_buf(span.buf);
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
        let held = address
            .checked_sub(span.first)
            .filter(|&i| i < u64::from(span.pages));
        // A page the cache holds is taken from it, not read again from the device: compaction
        // reads pages written moments before, which the cache took as they were written. The
        // look neither counts as a read nor moves the page, so a scan never promotes what it
        // passes (S3-FIFO's scan resistance holds).
        if held.is_none()
            && let Some(c) = self.cache.as_mut()
            && c.peek(address, out)
        {
            self.io.span_cache_hits = self.io.span_cache_hits.saturating_add(1);
            return Ok(());
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
        node_payload(page, address, out)
    }

    /// Reads `address` and the pages after it in its extent, up to the file's end, into `span`.
    fn fill(&mut self, span: &mut Span, address: u64) -> Result<(), Error> {
        let extent_pages = u64::from(self.config.extent_pages);
        let extent_end = self
            .extent_of(address)
            .checked_add(1)
            .and_then(|e| e.checked_mul(extent_pages))
            .ok_or(corrupt(Malformed::TooLarge))?;
        // Pages a run in flight writes are read once it is answered.
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
        let pages = u32::try_from(last.saturating_sub(address))
            .map_err(|_| corrupt(Malformed::TooLarge))?;
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
