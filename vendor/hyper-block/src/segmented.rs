//! A file kept as segments: the base file and, beside it, `<base>.1`, `<base>.2` and on, read and
//! written at offsets into their concatenation. A file created as segments keeps its base empty,
//! an anchor; a file that was one file keeps what it holds as its first segment.
//!
//! A file's own first writes hold up every other read and write of that file while they run, and
//! leave another file's untouched (mantle docs/research/48, measured on APFS; on Linux an
//! extending write takes the inode's lock exclusively, fs/ext4/file.c and fs/xfs/xfs_file.c). So a
//! file that grows does so by a new segment, created whole at its final length ([`SegmentedFile::add`]),
//! written by its owner while no other transfer uses it, and only then used: the segments in use
//! take overwrites alone. A segment never changes length once created, a transfer lies within one
//! segment (a caller lays its blocks out so none crosses a boundary), and the segments are at most
//! the bound given at open.
//!
//! **One open file a segment, whatever the handles.** [`SegmentedFile::create`] and
//! [`SegmentedFile::open`] give the file's *owner*, which opens the base and each segment beside it
//! once; a segment added through any handle is opened once, by that handle. Every other handle,
//! made by [`BlockFile::try_clone`], transfers through those same open files and opens none: a
//! descriptor a handle cost before, each opening every segment for itself, came to about 188 a
//! segment on a 32-core host, where a maintenance worker's handle and each of the issuer's
//! duplicates of it opened each new segment (mantle `docs/measurements/2026-11-06-targets.md`),
//! and a 100M fill ran out of descriptors at macOS's default limit. A transfer names its offset,
//! and positioned transfers on one open file from many threads take their offsets from their calls
//! and are atomic with respect to one another (`crate::file`), so a shared open file orders nothing
//! that each handle's own would not. A segment's writes and
//! flushes are counted on the segment, not the handle: a flush is the file's, whichever handle
//! wrote, so one through any handle covers every handle's writes. Before, each handle kept marks
//! of its own, and the device's issuer flushes a batch on one worker's handle after its writes ran
//! on any workers' handles, so a segment written only on other workers' handles was not flushed.
//! What a handle keeps of its own is its reads from memory.
//!
//! The open files sit in a process-static slot ([`FILES`]) read under counted pins (hyper-rt
//! `retire::Pinned`, the shard registry's protocol): a handle's transfer pins the slot for its
//! call, and the owner's drop retires the open files, which the last transfer running then closes,
//! on the file it began on. A slot carries the generation of the owner that holds it, so a handle
//! used after its owner closed the file, its slot free or another file's by then, is refused,
//! typed, and never transfers through another file's descriptors.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use hyper_rt::retire::Pinned;

use crate::DiskError;
use crate::atomic::AtomicUnit;
use crate::block::{BlockFile, Durable};
use crate::buf::{AlignedBuf, Alignment};
use crate::file::{CachingRequest, DeviceFile, sync_dir};
use crate::resident::{Epoch, Resident};
use crate::write_cache::DeviceCache;

/// One chunk for each bit of a slot's index: chunk `c` holds `2^c` slots, so the chunks hold every
/// index a `usize` can name.
const CHUNKS: usize = usize::BITS as usize;

/// Slots kept in chunks of 1, 2, 4 and on, each made when an index first reaches it: they hold
/// as many slots as were reached, never more than twice as many, and a made slot never moves, so
/// `&self` finds slots while another thread makes one (the bucket layout of Dechev, Pirkelbauer and
/// Stroustrup, "Lock-free dynamically resizable arrays", OPODIS 2006). Slots for a bound of 30 K
/// segments made up front cost a 10M fill's handles 4.9 GB of memory touched at setup (mantle
/// `docs/research/48-first-writes.md` §6).
#[derive(Debug)]
struct Chunked<T> {
    chunks: [OnceLock<Box<[T]>>; CHUNKS],
}

impl<T> Chunked<T> {
    const fn new() -> Self {
        Self {
            chunks: [const { OnceLock::new() }; CHUNKS],
        }
    }

    /// The chunk slot `index` is in, and its place there.
    fn locate(index: usize) -> Option<(usize, usize)> {
        let n = index.checked_add(1)?;
        let chunk = n.ilog2();
        let first = 1usize.checked_shl(chunk)?;
        Some((usize::try_from(chunk).ok()?, n.checked_sub(first)?))
    }

    /// Slot `index`, if its chunk was made.
    fn get(&self, index: usize) -> Option<&T> {
        let (chunk, at) = Self::locate(index)?;
        self.chunks.get(chunk)?.get()?.get(at)
    }

    fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        let (chunk, at) = Self::locate(index)?;
        self.chunks.get_mut(chunk)?.get_mut()?.get_mut(at)
    }

    /// Slot `index`, its chunk made of `empty` slots if none reached it before; none when the
    /// chunk cannot be allocated.
    fn make(&self, index: usize, empty: impl Fn() -> T) -> Option<&T> {
        let (chunk, at) = Self::locate(index)?;
        let cell = self.chunks.get(chunk)?;
        if cell.get().is_none() {
            let size = 1usize.checked_shl(u32::try_from(chunk).ok()?)?;
            let mut slots = Vec::new();
            slots.try_reserve_exact(size).ok()?;
            slots.extend((0..size).map(|_| empty()));
            // Made meanwhile by another thread: that chunk stays, and this one goes.
            let _ = cell.set(slots.into_boxed_slice());
        }
        cell.get()?.get(at)
    }

    /// The made slots, each with its index, in order.
    fn iter_mut(&mut self) -> impl Iterator<Item = (usize, &mut T)> {
        self.chunks
            .iter_mut()
            .enumerate()
            .filter_map(|(chunk, cell)| Some((chunk, cell.get_mut()?)))
            .flat_map(|(chunk, slots)| {
                let first = 1usize
                    .checked_shl(u32::try_from(chunk).unwrap_or(u32::MAX))
                    .and_then(|n| n.checked_sub(1));
                slots
                    .iter_mut()
                    .enumerate()
                    .filter_map(move |(at, slot)| Some((first?.checked_add(at)?, slot)))
            })
    }

    /// The made slots, each with its index, in order.
    fn iter(&self) -> impl Iterator<Item = (usize, &T)> {
        self.chunks
            .iter()
            .enumerate()
            .filter_map(|(chunk, cell)| Some((chunk, cell.get()?)))
            .flat_map(|(chunk, slots)| {
                let first = 1usize
                    .checked_shl(u32::try_from(chunk).unwrap_or(u32::MAX))
                    .and_then(|n| n.checked_sub(1));
                slots
                    .iter()
                    .enumerate()
                    .filter_map(move |(at, slot)| Some((first?.checked_add(at)?, slot)))
            })
    }
}

/// The process's segmented files, a slot each: made as files are opened and reused once a file is
/// closed, so there are as many as were ever open at once. Each holds at least its base's
/// descriptor, so the process's descriptor allowance bounds them.
static FILES: Chunked<Slot> = Chunked::new();

/// A segmented file's place in [`FILES`].
#[derive(Debug)]
struct Slot {
    /// Even while the slot is free, odd while an owner holds it: then the generation its
    /// handles carry, so a handle of a file closed since never names the slot's next file.
    state: AtomicU64,
    open: Pinned<Open>,
}

impl Slot {
    fn new() -> Self {
        Self {
            state: AtomicU64::new(0),
            open: Pinned::new(),
        }
    }

    /// Claims the slot if it is free: the generation it is held at.
    fn take(&self) -> Option<u64> {
        let state = self.state.load(Ordering::SeqCst);
        if !state.is_multiple_of(2) {
            return None;
        }
        let held = state.checked_add(1)?;
        self.state
            .compare_exchange(state, held, Ordering::SeqCst, Ordering::SeqCst)
            .ok()?;
        Some(held)
    }

    /// A free slot of [`FILES`], claimed: it and its generation. Each slot is looked at once, and
    /// one past those made is free, so the search ends within the files held now and one.
    fn claim(base: &Path) -> Result<(&'static Slot, u64), DiskError> {
        for index in 0..usize::MAX {
            let slot = FILES
                .make(index, Slot::new)
                .ok_or_else(|| beyond(base, "no memory for another segmented file's slot"))?;
            if let Some(generation) = slot.take() {
                return Ok((slot, generation));
            }
        }
        Err(beyond(base, "every segmented file slot is held"))
    }

    /// Frees the slot, held and never published or its files closed: the next claim may take it.
    fn free(&self) {
        self.state.fetch_add(1, Ordering::SeqCst);
    }
}

/// The end of a retirement, run by whoever finishes it last, the owner or a transfer's unpin:
/// every segment's file closed, then the slot freed.
fn closed(slot: &'static Slot, open: Box<Open>) {
    drop(open);
    slot.free();
}

/// What every handle of a segmented file shares: its segments' open files.
#[derive(Debug)]
struct Open {
    /// The generation its slot was claimed at: what each handle carries.
    generation: u64,
    base: PathBuf,
    caching: CachingRequest,
    align: Alignment,
    bound: usize,
    segments: Chunked<OnceLock<Segment>>,
    /// The segments filled, a prefix: a transfer finds its segment by a binary search over them,
    /// which touches a logarithm of the segments where a scan touched each (about 250
    /// instructions a get more than one file's at 10M keys and 20 segments, mantle
    /// `docs/research/48-first-writes.md` §6).
    filled: AtomicUsize,
    /// The segment one of these handles is adding, [`NONE_ADDED`] while none is: its file exists
    /// before it is whole, and discovery leaves it to the add, which places it whole.
    adding: AtomicUsize,
}

/// Format: [`Open::adding`]'s word while no handle adds a segment, an index no segment reaches.
const NONE_ADDED: usize = usize::MAX;

/// Every handle reads `Open` from its own thread and the last retirer closes it on its own: what
/// `Pinned` asks of what it holds.
const _: () = {
    const fn shared<T: Send + Sync>() {}
    shared::<Open>();
};

#[derive(Debug)]
struct Segment {
    /// Its first byte's offset in the concatenation.
    start: u64,
    len: u64,
    file: DeviceFile,
    /// Writes completed through any handle, counted as each returns, and the most of them a
    /// flush made durable, set as the flush returns: the segment needs a flush while the first is
    /// past the second. A flush is the file's, whichever handle wrote (fdatasync(2), "all modified
    /// in-core data of ... the file referred to by the file descriptor"; F_FULLFSYNC and
    /// FlushFileBuffers likewise), so a handle's flush covers every handle's writes, as
    /// [`BlockFile::sync_data`] promises, and one that finds the second at the first relies on a
    /// flush that has returned, never on one still running.
    written: AtomicU64,
    flushed: AtomicU64,
}

impl Segment {
    fn new(start: u64, len: u64, file: DeviceFile) -> Self {
        Self {
            start,
            len,
            file,
            written: AtomicU64::new(0),
            flushed: AtomicU64::new(0),
        }
    }

    /// A write through any handle returned: the next flush covers it.
    fn wrote(&self) {
        self.written.fetch_add(1, Ordering::Release);
    }

    /// Flushes the segment if a write returned since the last flush that returned: each write
    /// counted before this reads the count completed before the flush begins, so the flush covers
    /// it.
    fn flush(&self) -> Result<(), DiskError> {
        let written = self.written.load(Ordering::Acquire);
        if self.flushed.load(Ordering::Acquire) >= written {
            return Ok(());
        }
        self.file.sync_data()?;
        self.flushed.fetch_max(written, Ordering::AcqRel);
        Ok(())
    }

    /// Whether a write returned since the last flush that returned.
    #[cfg(test)]
    fn unflushed(&self) -> bool {
        self.flushed.load(Ordering::Acquire) < self.written.load(Ordering::Acquire)
    }
}

impl Open {
    /// Opens `base` and every segment beside it, at most `bound` in all.
    fn new(
        generation: u64,
        base: &Path,
        caching: CachingRequest,
        align: Alignment,
        bound: usize,
    ) -> Result<Self, DiskError> {
        let open = Self {
            generation,
            base: base.to_path_buf(),
            caching,
            align,
            bound,
            segments: Chunked::new(),
            filled: AtomicUsize::new(0),
            adding: AtomicUsize::new(NONE_ADDED),
        };
        let first = DeviceFile::open_shared(base, false, caching, align)?;
        let len = first.len()?;
        open.place(0, 0, len, first)?;
        // Every segment that exists now, so the end is the whole file's.
        open.discover_all()?;
        Ok(open)
    }

    /// Opens the segment after those filled, if one exists: whether it was found, by this call or
    /// by another handle's meanwhile. Another owner of the same files, opened apart, adds segments
    /// these open files do not hold; each is opened here when a transfer first reaches past the
    /// segments filled, and only then, as the segment is written whole before any transfer uses
    /// it (the module's note).
    ///
    /// A segment one of these handles is adding is left to that add: its file exists, created and
    /// not yet whole, from before it is whole until the add places it. The add names its index
    /// before it creates the file, so a look that finds the file then finds the index named.
    fn discover(&self) -> Result<bool, DiskError> {
        let index = self.filled.load(Ordering::SeqCst);
        if index >= self.bound || self.adding.load(Ordering::SeqCst) == index {
            return Ok(false);
        }
        let path = segment_path(&self.base, index);
        if !path.exists() || self.adding.load(Ordering::SeqCst) == index {
            return Ok(false);
        }
        let file = DeviceFile::open_shared(&path, false, self.caching, self.align)?;
        let len = file.len()?;
        match self.place(index, self.end(), len, file) {
            Ok(()) => Ok(true),
            // Filled meanwhile through another handle: the same segment, found once.
            Err(_) if self.segment(index).is_some() => Ok(true),
            Err(e) => Err(e),
        }
    }

    /// Opens every segment beside those filled: at most the bound's, one a pass.
    fn discover_all(&self) -> Result<(), DiskError> {
        for _ in 0..self.bound {
            if !self.discover()? {
                break;
            }
        }
        Ok(())
    }

    /// Fills segment `index`, then counts it filled; refused past the bound, when its chunk cannot
    /// be allocated, or when it was filled already.
    fn place(&self, index: usize, start: u64, len: u64, file: DeviceFile) -> Result<(), DiskError> {
        if index >= self.bound {
            return Err(beyond(&self.base, "segments past the file's bound"));
        }
        let cell = self
            .segments
            .make(index, OnceLock::new)
            .ok_or_else(|| beyond(&self.base, "no memory for another segment's slot"))?;
        if cell.set(Segment::new(start, len, file)).is_err() {
            return Err(beyond(&self.base, "a segment slot filled twice"));
        }
        let filled = index
            .checked_add(1)
            .ok_or_else(|| beyond(&self.base, "segments past usize"))?;
        self.filled.fetch_max(filled, Ordering::SeqCst);
        Ok(())
    }

    fn segment(&self, index: usize) -> Option<&Segment> {
        self.segments.get(index)?.get()
    }

    /// The end of the segments filled: the file's length.
    fn end(&self) -> u64 {
        self.filled
            .load(Ordering::SeqCst)
            .checked_sub(1)
            .and_then(|last| self.segment(last))
            .map_or(0, |s| s.start.saturating_add(s.len))
    }

    /// The segment holding `offset`, and its index.
    fn find(&self, offset: u64) -> Option<(usize, &Segment)> {
        let (mut lo, mut hi) = (0usize, self.filled.load(Ordering::SeqCst));
        while lo < hi {
            let mid = lo.checked_add(hi.checked_sub(lo)?.checked_div(2)?)?;
            let s = self.segment(mid)?;
            if offset < s.start {
                hi = mid;
            } else if offset >= s.start.saturating_add(s.len) {
                lo = mid.checked_add(1)?;
            } else {
                return Some((mid, s));
            }
        }
        None
    }

    /// The segment holding `[offset, offset + len)`, its index and the offset in it; a transfer
    /// across two segments, or past the last, is refused.
    /// Each pass finds the segment, refuses, or fills one more segment, so the passes are at most
    /// the bound.
    fn holding(&self, offset: u64, len: usize) -> Result<(usize, &Segment, u64), DiskError> {
        let end = u64::try_from(len)
            .ok()
            .and_then(|len| offset.checked_add(len))
            .ok_or_else(|| beyond(&self.base, "a transfer past u64"))?;
        for _ in 0..=self.bound {
            if let Some((index, s)) = self.find(offset) {
                if end > s.start.saturating_add(s.len) {
                    return Err(beyond(&self.base, "a transfer across two segments"));
                }
                return Ok((index, s, offset.saturating_sub(s.start)));
            }
            if offset < self.end() || !self.discover()? {
                break;
            }
        }
        Err(beyond(
            &self.base,
            "a transfer past the file's last segment",
        ))
    }

    /// Adds a segment of `len` bytes after the last, allocated whole and its name made durable,
    /// then opened for every handle: its index and the offset it starts at.
    /// One add at a time: another handle's add meanwhile is refused.
    fn add(&self, len: u64) -> Result<(usize, u64), DiskError> {
        self.discover_all()?;
        let index = self.filled.load(Ordering::SeqCst);
        if index >= self.bound {
            return Err(beyond(&self.base, "segments past the file's bound"));
        }
        if self
            .adding
            .compare_exchange(NONE_ADDED, index, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(beyond(
                &self.base,
                "a segment added meanwhile through another handle",
            ));
        }
        let added = self.add_at(index, len);
        self.adding.store(NONE_ADDED, Ordering::SeqCst);
        added
    }

    fn add_at(&self, index: usize, len: u64) -> Result<(usize, u64), DiskError> {
        let path = segment_path(&self.base, index);
        let file = DeviceFile::open_shared(&path, true, self.caching, self.align)?;
        if file.len()? != 0 {
            return Err(beyond(&path, "a new segment that already holds bytes"));
        }
        made_whole(&file, &path, len)?;
        let start = self.end();
        self.place(index, start, len, file)?;
        Ok((index, start))
    }
}

/// What one handle's own reads from memory of a segment keep ([`BlockFile::read_resident_at`]):
/// each handle maps and probes for itself, from the file system's answer the segment's open file
/// got.
#[derive(Debug)]
struct Mark {
    resident: Resident,
}

impl Mark {
    fn new(segment: &Segment, shadow: bool) -> Self {
        let mut resident = segment.file.resident_for_handle();
        if shadow {
            resident.set_shadow(true);
        }
        Self { resident }
    }
}

/// A base file and the segments beside it, as one file ([`BlockFile`]): the owner that opened
/// them, or a handle that transfers through the owner's open files ([`BlockFile::try_clone`]).
#[derive(Debug)]
pub struct SegmentedFile {
    slot: &'static Slot,
    generation: u64,
    /// Opened the segments ([`Self::create`], [`Self::open`]): its drop closes them.
    owner: bool,
    base: PathBuf,
    align: Alignment,
    bound: usize,
    /// The base's layout block, whether new space is zeroed and whether its completed writes are
    /// the device's, asked once, at open.
    layout: Alignment,
    fills: bool,
    reaches: bool,
    /// This handle's reads from memory of each segment it read so.
    marks: Chunked<OnceLock<Mark>>,
    /// Shadow mode for resident reads, given to each segment as this handle reaches it
    /// ([`BlockFile::set_residency_shadow`]).
    shadow: bool,
    /// The epoch every segment's kept answers hold in (`crate::resident`): the kernel's count it
    /// stands for is the machine's, so one serves them all, read once however many segments a
    /// file's reads reach.
    epoch: Epoch,
}

/// The path of segment `index`: the base file for 0, `<base>.<index>` after.
fn segment_path(base: &Path, index: usize) -> PathBuf {
    if index == 0 {
        return base.to_path_buf();
    }
    let mut name = base.as_os_str().to_owned();
    name.push(format!(".{index}"));
    PathBuf::from(name)
}

fn beyond(path: &Path, reason: &'static str) -> DiskError {
    DiskError::Unsupported {
        path: path.to_path_buf(),
        reason,
    }
}

/// A transfer through a handle whose owner has closed the file.
fn shut(path: &Path) -> DiskError {
    beyond(path, "a segmented file whose owner has closed it")
}

/// The alignment a write past the OS's cache needs of its buffer: on macOS the cluster layer
/// writes past the cache only from a buffer and at a file offset aligned to the VM page, and
/// copies through the cache otherwise (XNU `bsd/vfs/vfs_cluster.c`, `cluster_write_direct`; and
/// only a write of 16 KiB or more, `MIN_DIRECT_WRITE_SIZE`); elsewhere the file's own.
fn whole_align(align: Alignment) -> Result<Alignment, DiskError> {
    #[cfg(target_vendor = "apple")]
    {
        Ok(Alignment::new(rustix::param::page_size().max(align.get()))?)
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        Ok(align)
    }
}

/// Allocates `file`, at `path` and still empty, whole at `len` bytes, flushes it, and flushes its
/// directory so its name survives a crash (Pillai et al., OSDI 2014).
fn made_whole(file: &DeviceFile, path: &Path, len: u64) -> Result<(), DiskError> {
    if len > 0 {
        file.preallocate(len)?;
    }
    file.sync_data()?;
    let dir = match path.parent() {
        Some(d) if !d.as_os_str().is_empty() => d.to_path_buf(),
        _ => PathBuf::from("."),
    };
    sync_dir(&dir)
}

impl SegmentedFile {
    /// Creates `base`, empty, as the anchor of a file kept as segments, its name made durable:
    /// every byte the file comes to hold is in a segment [`Self::add`] makes after it, so the
    /// base, never written, holds no first write either. At most `bound` segments, the base
    /// among them. The file's owner ([`Self::open`]).
    pub fn create(
        base: &Path,
        caching: CachingRequest,
        align: Alignment,
        bound: usize,
    ) -> Result<Self, DiskError> {
        let first = DeviceFile::open(base, true, caching, align)?;
        if first.len()? != 0 {
            return Err(beyond(
                base,
                "a new segmented file whose base already holds bytes",
            ));
        }
        made_whole(&first, base, 0)?;
        drop(first);
        Self::open(base, caching, align, bound)
    }

    /// Opens `base` and every segment beside it, at most `bound` in all, as their owner: the one
    /// handle that opens them, whose drop closes them for every handle made from it.
    pub fn open(
        base: &Path,
        caching: CachingRequest,
        align: Alignment,
        bound: usize,
    ) -> Result<Self, DiskError> {
        if bound == 0 {
            return Err(beyond(base, "a segmented file bounded to no segments"));
        }
        let (slot, generation) = Slot::claim(base)?;
        Self::open_in(slot, generation, base, caching, align, bound)
    }

    /// [`Self::open`] in `slot`, claimed at `generation`: freed again if the files cannot be
    /// opened.
    fn open_in(
        slot: &'static Slot,
        generation: u64,
        base: &Path,
        caching: CachingRequest,
        align: Alignment,
        bound: usize,
    ) -> Result<Self, DiskError> {
        let open = match Open::new(generation, base, caching, align, bound) {
            Ok(open) => open,
            Err(e) => {
                slot.free();
                return Err(e);
            }
        };
        let Some(first) = open.segment(0) else {
            slot.free();
            return Err(beyond(base, "a segmented file with no base"));
        };
        let owner = Self {
            slot,
            generation,
            owner: true,
            base: base.to_path_buf(),
            align,
            bound,
            layout: first.file.layout_block(),
            fills: first.file.fills_new_space(),
            reaches: first.file.writes_reach_device(),
            marks: Chunked::new(),
            shadow: false,
            epoch: Epoch::new(),
        };
        slot.open.publish(Box::new(open));
        Ok(owner)
    }

    /// Runs `f` on the open files, pinned for the call; refused once their owner closed them.
    fn with<R>(&self, f: impl FnOnce(&Open) -> Result<R, DiskError>) -> Result<R, DiskError> {
        let (slot, generation) = (self.slot, self.generation);
        slot.open
            .read(
                |open| {
                    if open.generation == generation {
                        f(open)
                    } else {
                        Err(shut(&self.base))
                    }
                },
                |retired| closed(slot, retired),
            )
            .unwrap_or_else(|| Err(shut(&self.base)))
    }

    /// Adds a segment of `len` bytes after the last, allocated whole and its name made durable
    /// before it is returned: its index and the offset it starts at. Its blocks are allocated
    /// (`DeviceFile::preallocate`), not written; its owner writes them before any other transfer
    /// uses them. Opened once, for every handle of the file.
    pub fn add(&self, len: u64) -> Result<(usize, u64), DiskError> {
        self.with(|open| open.add(len))
    }

    /// The segments, each its start and length; none once the file's owner has closed it.
    pub fn layout(&self) -> Vec<(u64, u64)> {
        self.with(|open| {
            let filled = open.filled.load(Ordering::SeqCst);
            let mut layout = Vec::new();
            layout
                .try_reserve_exact(filled)
                .map_err(|_| beyond(&self.base, "no memory for a segment layout"))?;
            layout.extend(
                (0..filled)
                    .filter_map(|i| open.segment(i))
                    .map(|s| (s.start, s.len)),
            );
            Ok(layout)
        })
        .unwrap_or_default()
    }

    /// The bound on segments given at open.
    pub fn bound(&self) -> usize {
        self.bound
    }
}

impl Drop for SegmentedFile {
    fn drop(&mut self) {
        if self.owner {
            let slot = self.slot;
            slot.open.retire(|open| closed(slot, open));
        }
    }
}

impl BlockFile for SegmentedFile {
    fn alignment(&self) -> Alignment {
        self.align
    }

    fn layout_block(&self) -> Alignment {
        self.layout
    }

    /// The base's device's: every segment lies beside the base, in its directory, on its device.
    fn atomic_unit(&self) -> Result<AtomicUnit, DiskError> {
        self.with(|open| {
            open.segment(0)
                .ok_or_else(|| beyond(&self.base, "a segmented file with no base"))?
                .file
                .atomic_unit()
        })
    }

    /// The base's device's, as its atomic write unit is.
    fn write_cache(&self) -> Result<DeviceCache, DiskError> {
        self.with(|open| {
            open.segment(0)
                .ok_or_else(|| beyond(&self.base, "a segmented file with no base"))?
                .file
                .write_cache()
        })
    }

    fn len(&self) -> Result<u64, DiskError> {
        self.with(|open| {
            open.discover_all()?;
            Ok(open.end())
        })
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.with(|open| {
            let (_, s, at) = open.holding(offset, buf.len())?;
            s.file.read_exact_at(buf, at)
        })
    }

    fn read_resident_at(&mut self, buf: &mut [u8], offset: u64) -> Result<bool, DiskError> {
        let (slot, generation, shadow) = (self.slot, self.generation, self.shadow);
        let Self {
            base, marks, epoch, ..
        } = self;
        slot.open
            .read(
                |open| {
                    if open.generation != generation {
                        return Err(shut(base));
                    }
                    let (index, s, at) = open.holding(offset, buf.len())?;
                    let cell = marks
                        .make(index, OnceLock::new)
                        .ok_or_else(|| beyond(base, "no memory for another segment's mark"))?;
                    cell.get_or_init(|| Mark::new(s, shadow));
                    let mark = marks
                        .get_mut(index)
                        .and_then(OnceLock::get_mut)
                        .ok_or_else(|| beyond(base, "a segment's mark lost from its slot"))?;
                    s.file
                        .read_resident_through(&mut mark.resident, buf, at, epoch)
                },
                |retired| closed(slot, retired),
            )
            .unwrap_or_else(|| Err(shut(base)))
    }

    fn reads_resident(&self) -> bool {
        // Every segment through this handle: one that learned its file system refuses says so
        // for all.
        self.with(|open| {
            let filled = open.filled.load(Ordering::SeqCst);
            Ok(filled > 0
                && (0..filled).all(|i| {
                    open.segment(i).is_some_and(|s| {
                        s.file.reads_resident()
                            && self
                                .marks
                                .get(i)
                                .and_then(OnceLock::get)
                                .is_none_or(|m| m.resident.reads())
                    })
                }))
        })
        .unwrap_or(false)
    }

    fn residency(&self) -> crate::Residency {
        self.marks
            .iter()
            .filter_map(|(_, cell)| cell.get())
            .map(|m| m.resident.residency())
            .fold(self.epoch.residency(), crate::Residency::plus)
    }

    fn set_residency_shadow(&mut self, on: bool) {
        self.shadow = on;
        for (_, cell) in self.marks.iter_mut() {
            if let Some(mark) = cell.get_mut() {
                mark.resident.set_shadow(on);
            }
        }
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.with(|open| {
            let (_, s, at) = open.holding(offset, buf.len())?;
            s.file.write_all_at(buf, at)?;
            s.wrote();
            Ok(())
        })
    }

    /// Writes the copies through a handle of each segment they reach opened past the OS's cache
    /// where the platform has a way ([`CachingRequest::PreferDirect`]): the bytes only make the
    /// blocks written, and no read wants them held in memory, where they would take the place of
    /// pages reads do want. All of a segment's copies go in as few calls as the kernel takes
    /// ([`DeviceFile::write_copies_at`]). The segment's flush makes them durable.
    fn write_whole(&self, offset: u64, copies: u64, fill: &[u8]) -> Result<(), DiskError> {
        let piece = u64::try_from(fill.len()).map_err(|_| beyond(&self.base, "a fill past u64"))?;
        if piece == 0 {
            return Ok(());
        }
        let mut aligned = AlignedBuf::zeroed(fill.len(), whole_align(self.align)?)?;
        aligned.extend_from_slice(fill)?;
        let fill = aligned.as_slice();
        let mut at = offset;
        let mut left = copies;
        while left > 0 {
            let (index, start, len) = self.with(|open| {
                let (index, s, _) = open.holding(at, fill.len())?;
                Ok((index, s.start, s.len))
            })?;
            let direct = DeviceFile::open(
                &segment_path(&self.base, index),
                false,
                CachingRequest::PreferDirect,
                self.align,
            )?;
            // The copies this segment holds from `at` on.
            let here = start
                .saturating_add(len)
                .saturating_sub(at)
                .checked_div(piece)
                .unwrap_or(0)
                .min(left);
            if here == 0 {
                return Err(beyond(&self.base, "a copy across two segments"));
            }
            direct.write_copies_at(fill, at.saturating_sub(start), here)?;
            self.with(|open| {
                open.segment(index)
                    .ok_or_else(|| beyond(&self.base, "a written segment lost"))?
                    .wrote();
                Ok(())
            })?;
            at = piece
                .checked_mul(here)
                .and_then(|b| at.checked_add(b))
                .ok_or_else(|| beyond(&self.base, "a transfer past u64"))?;
            left = left.saturating_sub(here);
        }
        Ok(())
    }

    /// Flushes every segment written since its last flush, through this handle or any other: the
    /// base's flush alone would leave another segment's writes to a crash, and a flush of only
    /// what this handle wrote would leave the writes the device's issuer made on its other
    /// workers' handles to one.
    fn sync_data(&self) -> Result<(), DiskError> {
        self.with(|open| {
            let filled = open.filled.load(Ordering::SeqCst);
            for index in 0..filled {
                open.segment(index)
                    .ok_or_else(|| beyond(&self.base, "a segment lost from its slot"))?
                    .flush()?;
            }
            Ok(())
        })
    }

    fn fills_new_space(&self) -> bool {
        self.fills
    }

    /// The base's, as its device's atomic write unit and cache are: every segment lies beside it,
    /// in its directory, on its file system, opened with the caching the base was asked for.
    fn writes_reach_device(&self) -> bool {
        self.reaches
    }

    fn grows_by_segments(&self) -> bool {
        true
    }

    fn grow(&self, len: u64) -> Result<Option<u64>, DiskError> {
        self.add(len).map(|(_, start)| Some(start))
    }

    fn segment_end(&self, offset: u64) -> Result<Option<u64>, DiskError> {
        self.with(|open| {
            open.discover_all()?;
            Ok(open
                .find(offset)
                .map(|(_, s)| s.start.saturating_add(s.len)))
        })
    }

    fn write_durable_at(&self, buf: &[u8], offset: u64) -> Result<Durable, DiskError> {
        self.with(|open| {
            let (_, s, at) = open.holding(offset, buf.len())?;
            s.file.write_durable_at(buf, at)
        })
    }

    /// A handle of the same open files, taking no descriptor of its own: it transfers through the
    /// owner's, keeps its own record of what it wrote and read from memory, and is refused once
    /// the owner has closed them.
    fn try_clone(&self) -> Result<Self, DiskError> {
        self.with(|_| {
            Ok(Self {
                slot: self.slot,
                generation: self.generation,
                owner: false,
                base: self.base.clone(),
                align: self.align,
                bound: self.bound,
                layout: self.layout,
                fills: self.fills,
                reaches: self.reaches,
                marks: Chunked::new(),
                shadow: self.shadow,
                epoch: Epoch::new(),
            })
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::buf::AlignedBuf;

    const ALIGN: usize = 4096;

    /// A new segmented file whose first segment holds one page.
    fn create(base: &Path, bound: usize) -> SegmentedFile {
        let align = Alignment::new(ALIGN).unwrap();
        let file = SegmentedFile::create(base, CachingRequest::Buffered, align, bound).unwrap();
        assert_eq!(file.add(4096).unwrap(), (1, 0));
        file
    }

    fn open(base: &Path, bound: usize) -> SegmentedFile {
        let align = Alignment::new(ALIGN).unwrap();
        SegmentedFile::open(base, CachingRequest::Buffered, align, bound).unwrap()
    }

    fn page(fill: u8) -> AlignedBuf {
        let mut b = AlignedBuf::zeroed(ALIGN, Alignment::new(ALIGN).unwrap()).unwrap();
        b.extend_from_slice(&[fill; ALIGN]).unwrap();
        b
    }

    fn read(file: &SegmentedFile, offset: u64) -> u8 {
        let mut b = page(0);
        file.read_exact_at(b.as_mut_slice(), offset).unwrap();
        b.as_slice()[0]
    }

    /// Which of the file's segments were written since their last flush, through any handle.
    fn dirty(f: &SegmentedFile) -> Vec<bool> {
        f.with(|open| {
            let filled = open.filled.load(Ordering::SeqCst);
            Ok((0..filled)
                .map(|i| open.segment(i).is_some_and(Segment::unflushed))
                .collect())
        })
        .unwrap()
    }

    /// The descriptors of the open files `f` transfers through: one for each segment opened.
    fn opened(f: &SegmentedFile) -> usize {
        f.with(|open| Ok(open.filled.load(Ordering::SeqCst)))
            .unwrap()
    }

    /// Segments added after the base are read and written at offsets into the concatenation,
    /// each byte landing in the segment that holds it; a handle made before a segment was added
    /// transfers through it; reopened, the file has every segment and its whole length.
    #[test]
    fn segments_are_one_file_to_every_handle() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("store");
        let file = create(&base, 4);
        file.write_all_at(page(1).as_slice(), 0).unwrap();
        assert_eq!(file.len().unwrap(), 4096);
        let early = file.try_clone().unwrap();
        let (index, start) = file.add(3 * 4096).unwrap();
        assert_eq!((index, start), (2, 4096));
        let (index, start) = early.add(2 * 4096).unwrap();
        assert_eq!((index, start), (3, 4 * 4096));
        assert_eq!(file.len().unwrap(), 6 * 4096);
        file.write_all_at(page(2).as_slice(), 2 * 4096).unwrap();
        file.write_all_at(page(3).as_slice(), 5 * 4096).unwrap();
        file.sync_data().unwrap();
        // The handle made before either segment transfers through both.
        assert_eq!(read(&early, 2 * 4096), 2);
        assert_eq!(read(&early, 5 * 4096), 3);
        assert_eq!(
            early.layout(),
            vec![(0, 0), (0, 4096), (4096, 3 * 4096), (4 * 4096, 2 * 4096)]
        );
        // Each byte is in its segment's file, at its offset there; the base holds none.
        assert_eq!(std::fs::metadata(&base).unwrap().len(), 0);
        let two = std::fs::read(segment_path(&base, 2)).unwrap();
        assert_eq!((two.len(), two[4096]), (3 * 4096, 2));
        let three = std::fs::read(segment_path(&base, 3)).unwrap();
        assert_eq!((three.len(), three[4096]), (2 * 4096, 3));
        drop((file, early));
        let reopened = open(&base, 4);
        assert_eq!(reopened.len().unwrap(), 6 * 4096);
        assert_eq!((read(&reopened, 0), read(&reopened, 2 * 4096)), (1, 2));
    }

    /// A segmented file answers the atomic write unit of its base's device, as the OS states it
    /// for the base opened alone, through its owner and through a handle made from it, rather than
    /// the unit of a device that states none.
    #[test]
    fn a_segmented_file_reports_its_devices_atomic_unit() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("store");
        let file = create(&base, 4);
        let alone = DeviceFile::open(
            &base,
            false,
            CachingRequest::Buffered,
            Alignment::new(ALIGN).unwrap(),
        )
        .unwrap();
        let device = crate::atomic::atomic_unit(alone.std_file(), &base).unwrap();
        assert_eq!(file.atomic_unit().unwrap(), device);
        assert_eq!(file.try_clone().unwrap().atomic_unit().unwrap(), device);
    }

    /// A segmented file answers the write cache of its base's device, as the OS states it for the
    /// base opened alone, through its owner and through a handle made from it, rather than the
    /// default of a device that states none.
    #[test]
    fn a_segmented_file_reports_its_devices_write_cache() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("store");
        let file = create(&base, 4);
        let alone = DeviceFile::open(
            &base,
            false,
            CachingRequest::Buffered,
            Alignment::new(ALIGN).unwrap(),
        )
        .unwrap();
        let device = crate::write_cache::write_cache(alone.std_file(), &base).unwrap();
        assert_eq!(file.write_cache().unwrap(), device);
        assert_eq!(file.try_clone().unwrap().write_cache().unwrap(), device);
    }

    /// Do: add 37 segments to a file bounded to 1,000, writing a page into each, and reopen it.
    /// Expect: every page reads back from its segment, in order, through the owner that added
    /// them and through the reopened one; each made the six chunks that hold 38 segments (63
    /// slots), not slots for its bound.
    #[test]
    fn a_file_holds_slots_for_the_segments_it_has_not_for_its_bound() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("store");
        let file = create(&base, 1000);
        for k in 1..38u64 {
            let (index, start) = file.add(4096).unwrap();
            assert_eq!((index as u64, start), (k + 1, k * 4096));
        }
        for k in 0..38u64 {
            file.write_all_at(page(k as u8).as_slice(), k * 4096)
                .unwrap();
        }
        let made = |f: &SegmentedFile| {
            f.with(|open| {
                Ok(open
                    .segments
                    .chunks
                    .iter()
                    .filter(|c| c.get().is_some())
                    .count())
            })
            .unwrap()
        };
        let reopened = open(&base, 1000);
        for f in [&file, &reopened] {
            assert_eq!(f.layout().len(), 39);
            for k in 0..38u64 {
                assert_eq!(read(f, k * 4096), k as u8, "segment {k}");
            }
            assert_eq!(made(f), 6);
            assert_eq!(f.bound(), 1000);
        }
    }

    /// A transfer across two segments, or past the last, is refused, as is a segment past the
    /// bound; nothing of a refused write lands.
    #[test]
    fn transfers_across_or_past_segments_and_segments_past_the_bound_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("store");
        let file = create(&base, 3);
        file.write_all_at(page(1).as_slice(), 0).unwrap();
        file.add(4096).unwrap();
        let mut two = AlignedBuf::zeroed(2 * ALIGN, Alignment::new(ALIGN).unwrap()).unwrap();
        two.extend_from_slice(&[9; 2 * ALIGN]).unwrap();
        assert!(file.write_all_at(two.as_slice(), 0).is_err());
        assert!(file.write_all_at(page(9).as_slice(), 2 * 4096).is_err());
        assert!(file.add(4096).is_err());
        assert_eq!((read(&file, 0), read(&file, 4096)), (1, 0));
    }

    /// Do: write two segments through two handles, flush through one, then write one again and
    /// flush through the other. Expect: each flush covers every segment written since its last
    /// flush, whichever handle wrote it, and only those: a flush is the file's, as the device's
    /// issuer needs when a batch's writes ran on some workers' handles and its flush on another's.
    #[test]
    fn a_flush_covers_the_segments_written_since_the_last_through_any_handle() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("store");
        let file = create(&base, 4);
        let other = file.try_clone().unwrap();
        file.write_all_at(page(1).as_slice(), 0).unwrap();
        file.add(4096).unwrap();
        file.add(4096).unwrap();
        other.write_all_at(page(2).as_slice(), 2 * 4096).unwrap();
        assert_eq!(dirty(&file), vec![false, true, false, true]);
        file.sync_data().unwrap();
        assert_eq!(dirty(&other), vec![false, false, false, false]);
        other.write_all_at(page(3).as_slice(), 4096).unwrap();
        assert_eq!(dirty(&file), vec![false, false, true, false]);
        other.sync_data().unwrap();
        assert_eq!(dirty(&file), vec![false, false, false, false]);
    }

    /// Do: make a hundred handles of a file of three segments, through the owner and through one
    /// another, and transfer a page through each into each segment. Expect: the open files stay
    /// one a segment, the owner's, and every handle reads what every other wrote.
    #[test]
    fn handles_take_no_open_file_of_their_own() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("store");
        let file = create(&base, 4);
        file.add(4096).unwrap();
        file.add(4096).unwrap();
        assert_eq!(opened(&file), 4);
        let mut handles = vec![file.try_clone().unwrap()];
        for i in 1..100 {
            let next = handles[i - 1].try_clone().unwrap();
            handles.push(next);
        }
        for (i, h) in handles.iter().enumerate() {
            for segment in 0..3u64 {
                h.write_all_at(page(i as u8).as_slice(), segment * 4096)
                    .unwrap();
                assert_eq!(read(&file, segment * 4096), i as u8);
            }
            assert_eq!(opened(h), 4);
        }
        assert_eq!(opened(&file), 4);
    }

    /// Do: drop the owner of a file another handle transfers through, then open a second file in
    /// the slot the first freed. Expect: the handle's transfers, flushes and duplicates are
    /// refused, typed, once its owner is gone, and none lands in the second file, whose owner
    /// reads only its own bytes: the slot's generation moved.
    #[test]
    fn a_handle_is_refused_once_its_owner_closed_and_never_reaches_another_file() {
        let dir = tempfile::tempdir().unwrap();
        let align = Alignment::new(ALIGN).unwrap();
        // A slot of this test's own, so the second file takes the one the first freed.
        let slot: &'static Slot = Box::leak(Box::new(Slot::new()));
        let in_slot = |name: &str| {
            let base = dir.path().join(name);
            drop(create(&base, 4));
            let generation = slot.take().unwrap();
            SegmentedFile::open_in(slot, generation, &base, CachingRequest::Buffered, align, 4)
                .unwrap()
        };
        let first = in_slot("first");
        first.write_all_at(page(1).as_slice(), 0).unwrap();
        let handle = first.try_clone().unwrap();
        assert_eq!(read(&handle, 0), 1);
        drop(first);
        let shut = |e: DiskError| matches!(e, DiskError::Unsupported { reason, .. } if reason.contains("owner has closed"));
        assert!(shut(
            handle.write_all_at(page(2).as_slice(), 0).unwrap_err()
        ));
        assert!(shut(handle.len().unwrap_err()));
        assert!(shut(handle.try_clone().unwrap_err()));
        let second = in_slot("second");
        second.write_all_at(page(3).as_slice(), 0).unwrap();
        assert_ne!(second.generation, handle.generation);
        assert!(shut(
            handle.write_all_at(page(4).as_slice(), 0).unwrap_err()
        ));
        let mut b = page(0);
        assert!(shut(handle.read_exact_at(b.as_mut_slice(), 0).unwrap_err()));
        assert!(shut(handle.sync_data().unwrap_err()));
        assert_eq!(read(&second, 0), 3);
        drop(handle);
        assert_eq!(read(&second, 0), 3);
        drop(second);
        assert!(
            slot.state.load(Ordering::SeqCst).is_multiple_of(2),
            "freed once closed"
        );
    }

    /// Do: from as many threads as the host runs at once, and at least two, each with its own
    /// handle, write a distinct page into each of three segments and read every page back while
    /// the others write. Expect: each page holds what its writer wrote, through every handle:
    /// positioned transfers on one open file from many threads land at their own offsets.
    #[test]
    fn threads_transfer_through_one_open_file_at_once() {
        let threads = std::thread::available_parallelism().map_or(2, |n| n.get().max(2));
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("store");
        let pages = threads as u64;
        let file = create(&base, 5);
        file.add(pages * 4096 - 4096).unwrap();
        file.add(pages * 4096).unwrap();
        file.add(pages * 4096).unwrap();
        let handles: Vec<SegmentedFile> = (0..threads).map(|_| file.try_clone().unwrap()).collect();
        let segment_starts = [0u64, pages * 4096, 2 * pages * 4096];
        std::thread::scope(|s| {
            for (t, h) in handles.iter().enumerate() {
                s.spawn(move || {
                    for start in segment_starts {
                        let at = start + t as u64 * 4096;
                        h.write_all_at(page(t as u8 + 1).as_slice(), at).unwrap();
                        assert_eq!(read(h, at), t as u8 + 1);
                    }
                });
            }
        });
        for (t, h) in handles.iter().enumerate() {
            for start in segment_starts {
                for u in 0..threads {
                    assert_eq!(read(h, start + u as u64 * 4096), u as u8 + 1, "thread {t}");
                }
            }
        }
        assert_eq!(opened(&file), 5);
    }

    /// Do: open a file's segments twice, as two owners, and add a segment through one. Expect:
    /// the other reaches it on its first transfer past the segments it holds, through an open
    /// file of its own, as a file opened apart in another process would.
    #[test]
    fn a_second_owner_finds_a_segment_the_first_added() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("store");
        let one = create(&base, 4);
        let two = open(&base, 4);
        assert_eq!(opened(&two), 2);
        let (index, start) = one.add(4096).unwrap();
        assert_eq!((index, start), (2, 4096));
        one.write_all_at(page(5).as_slice(), 4096).unwrap();
        one.sync_data().unwrap();
        assert_eq!(read(&two, 4096), 5);
        assert_eq!((opened(&two), two.len().unwrap()), (3, 2 * 4096));
    }

    /// Do: as a handle's add has named its segment and created its file, not yet whole, ask
    /// another handle the file's length and add a segment through it; then let the first add
    /// through. Expect: the other handle's length is the file's before the add, its add is
    /// refused, and once the first add places the segment whole both handles hold it, at its
    /// length.
    #[test]
    fn a_segment_being_added_is_left_to_its_add() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("store");
        let file = create(&base, 4);
        let other = file.try_clone().unwrap();
        let index = file
            .with(|open| {
                let index = open.filled.load(Ordering::SeqCst);
                open.adding.store(index, Ordering::SeqCst);
                Ok(index)
            })
            .unwrap();
        let align = Alignment::new(ALIGN).unwrap();
        let path = segment_path(&base, index);
        drop(DeviceFile::open(&path, true, CachingRequest::Buffered, align).unwrap());
        assert_eq!((other.len().unwrap(), other.layout().len()), (4096, 2));
        assert!(other.add(4096).is_err());
        file.with(|open| {
            let added = open.add_at(index, 3 * 4096);
            open.adding.store(NONE_ADDED, Ordering::SeqCst);
            added
        })
        .unwrap();
        assert_eq!(other.len().unwrap(), 4 * 4096);
        assert_eq!(other.layout(), vec![(0, 0), (0, 4096), (4096, 3 * 4096)]);
    }

    /// Do: write two segments whole with copies of a 16 KiB piece (a VM page on every target),
    /// then overwrite one piece through the file. Expect: every piece holds the fill, each
    /// segment reached is flushed with the file's next flush, and where the OS granted a handle
    /// past its cache the pieces written whole are not in memory, while the piece overwritten
    /// through the file is where it reads from memory.
    #[test]
    fn a_segment_written_whole_holds_its_fill_and_takes_no_memory() {
        const PIECE: usize = 16384;
        let piece = |fill: u8| {
            let mut b = AlignedBuf::zeroed(PIECE, Alignment::new(PIECE).unwrap()).unwrap();
            b.extend_from_slice(&[fill; PIECE]).unwrap();
            b
        };
        let at = |i: u64| i * PIECE as u64;
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("store");
        let align = Alignment::new(ALIGN).unwrap();
        let file = SegmentedFile::create(&base, CachingRequest::Buffered, align, 4).unwrap();
        file.add(at(1)).unwrap();
        file.add(at(2)).unwrap();
        file.write_whole(0, 3, piece(7).as_slice()).unwrap();
        assert!(file.write_whole(0, 4, piece(7).as_slice()).is_err());
        assert_eq!(dirty(&file), vec![false, true, true]);
        // Whether this host's file system granted a handle past its cache, read before any read
        // of the pieces could bring them into memory.
        let direct = DeviceFile::open(
            &segment_path(&base, 2),
            false,
            CachingRequest::PreferDirect,
            align,
        )
        .unwrap()
        .caching()
            == crate::file::Caching::Direct;
        let mut fresh = open(&base, 4);
        let mut b = piece(0);
        if direct {
            for i in 0..3u64 {
                // On Linux the page cache is asked itself: a resident read of a page not in memory
                // starts its read-ahead, which takes the next piece of the same segment too, and
                // reads what that brought in by the time it looks (`resident::linux`).
                #[cfg(target_os = "linux")]
                let held = {
                    let (index, start) = fresh
                        .with(|open| {
                            let (index, s, _) = open.holding(at(i), PIECE)?;
                            Ok((index, s.start))
                        })
                        .unwrap();
                    let segment = std::fs::File::open(segment_path(&base, index)).unwrap();
                    crate::resident::cached(&segment, at(i) - start, PIECE).unwrap()
                };
                #[cfg(not(target_os = "linux"))]
                let held = fresh.read_resident_at(b.as_mut_slice(), at(i)).unwrap();
                assert!(!held, "piece {i} is in memory");
            }
        }
        fresh.write_all_at(piece(8).as_slice(), at(1)).unwrap();
        assert_eq!(
            fresh.read_resident_at(b.as_mut_slice(), at(1)).unwrap(),
            fresh.reads_resident()
        );
        assert_eq!(
            (read(&file, 0), read(&file, at(1)), read(&file, at(2))),
            (7, 8, 7)
        );
        file.sync_data().unwrap();
    }

    /// Do: write a page into each of two segments through the cache, hold the file's epoch, read
    /// each from memory twice, then move the epoch and read both again. Expect: each segment's
    /// first read probes and its second is made on the kept answer; one move of the file's epoch
    /// forgets both segments' answers; no read refreshes the held epoch.
    #[cfg(target_vendor = "apple")]
    #[test]
    fn one_epoch_holds_every_segments_answers() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("store");
        let mut file = create(&base, 4);
        file.add(4096).unwrap();
        file.write_all_at(page(1).as_slice(), 0).unwrap();
        file.write_all_at(page(2).as_slice(), 4096).unwrap();
        file.epoch.hold();
        let mut b = page(0);
        let mut both = |file: &mut SegmentedFile| {
            for (offset, fill) in [(0, 1), (4096, 2)] {
                assert!(file.read_resident_at(b.as_mut_slice(), offset).unwrap());
                assert_eq!(b.as_slice()[0], fill);
            }
            let r = file.residency();
            (r.probes, r.skips, r.epochs)
        };
        assert_eq!(both(&mut file), (2, 0, 0));
        assert_eq!(both(&mut file), (2, 2, 0));
        file.epoch.move_on();
        assert_eq!(both(&mut file), (4, 2, 0));
    }

    /// Do: create a segmented file asking for direct transfers and one buffered, add a segment
    /// to each, open each again and take a second handle. Expect: each answers whether its writes
    /// reach the device as its base, opened alone, does (`DeviceFile::writes_reach_device`), at
    /// create, after a segment is added, at open, and through every handle; a buffered one never.
    #[test]
    fn a_segmented_file_answers_whether_its_writes_reach_the_device_as_its_base_does() {
        let dir = tempfile::tempdir().unwrap();
        let align = Alignment::new(ALIGN).unwrap();
        for (name, caching) in [
            ("direct", CachingRequest::PreferDirect),
            ("buffered", CachingRequest::Buffered),
        ] {
            let base = dir.path().join(name);
            let file = SegmentedFile::create(&base, caching, align, 4).unwrap();
            let alone = DeviceFile::open(&base, false, caching, align)
                .unwrap()
                .writes_reach_device();
            if caching == CachingRequest::Buffered {
                assert!(!alone);
            }
            assert_eq!(file.writes_reach_device(), alone, "{name}");
            file.add(ALIGN as u64).unwrap();
            assert_eq!(file.writes_reach_device(), alone, "{name}");
            assert_eq!(file.try_clone().unwrap().writes_reach_device(), alone);
            drop(file);
            let file = SegmentedFile::open(&base, caching, align, 4).unwrap();
            assert_eq!(file.writes_reach_device(), alone, "{name}");
            assert_eq!(file.try_clone().unwrap().writes_reach_device(), alone);
        }
    }
}
