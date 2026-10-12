//! A file kept as segments: the base file and, beside it, `<base>.1`, `<base>.2` and on, read and
//! written at offsets into their concatenation. A file created as segments keeps its base empty,
//! an anchor; a file that was one file keeps what it holds as its first segment.
//!
//! A file's own first writes hold up every other read and write of that file while they run, and
//! leave another file's untouched (mantle docs/research/48, measured on APFS; on Linux an
//! extending write takes the inode's lock exclusively, fs/ext4/file.c and fs/xfs/xfs_file.c). So a
//! file that grows does so by a new segment, created whole at its final length ([`Self::add`]),
//! written by its owner while no other transfer uses it, and only then used: the segments in use
//! take overwrites alone.
//!
//! A segment never changes length once created, so every handle finds the layout from the
//! segments' names and lengths alone: a duplicate made before a segment was added opens it when a
//! transfer first reaches past the segments it knows, and nothing is shared between handles. A
//! transfer lies within one segment (a caller lays its blocks out so none crosses a boundary), and
//! the segments are at most the bound given at open.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::DiskError;
use crate::block::{BlockFile, Durable};
use crate::buf::{AlignedBuf, Alignment};
use crate::file::{CachingRequest, DeviceFile, sync_dir};

#[derive(Debug)]
struct Segment {
    /// Its first byte's offset in the concatenation.
    start: u64,
    len: u64,
    file: DeviceFile,
    /// Written since its last flush.
    dirty: AtomicBool,
}

/// A base file and the segments beside it, as one file ([`BlockFile`]).
#[derive(Debug)]
pub struct SegmentedFile {
    base: PathBuf,
    caching: CachingRequest,
    align: Alignment,
    /// The segments this handle knows, in order: a slot is filled once, when found or added.
    segments: Box<[OnceLock<Segment>]>,
    /// Shadow mode for resident reads, given to each segment as it is found
    /// ([`BlockFile::set_residency_shadow`]).
    shadow: bool,
    /// The epoch every segment's kept answers hold in (`crate::resident`): the kernel's count it
    /// stands for is the machine's, so one serves them all, read once however many segments a
    /// file's reads reach.
    epoch: crate::resident::Epoch,
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
    /// among them.
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

    /// Opens `base` and every segment beside it, at most `bound` in all.
    pub fn open(
        base: &Path,
        caching: CachingRequest,
        align: Alignment,
        bound: usize,
    ) -> Result<Self, DiskError> {
        if bound == 0 {
            return Err(beyond(base, "a segmented file bounded to no segments"));
        }
        let segments: Box<[OnceLock<Segment>]> = (0..bound).map(|_| OnceLock::new()).collect();
        let first = DeviceFile::open(base, false, caching, align)?;
        let len = first.len()?;
        let file = Self {
            base: base.to_path_buf(),
            caching,
            align,
            segments,
            shadow: false,
            epoch: crate::resident::Epoch::new(),
        };
        file.place(0, 0, len, first)?;
        // Every segment that exists now, so `len` is the whole file's.
        while file.find_next()?.is_some() {}
        Ok(file)
    }

    fn place(&self, index: usize, start: u64, len: u64, file: DeviceFile) -> Result<(), DiskError> {
        let slot = self
            .segments
            .get(index)
            .ok_or_else(|| beyond(&self.base, "segments past the file's bound"))?;
        slot.set(Segment {
            start,
            len,
            file,
            dirty: AtomicBool::new(false),
        })
        .map_err(|_| beyond(&self.base, "a segment slot filled twice"))
    }

    /// The segments this handle knows.
    fn known(&self) -> usize {
        self.segments
            .iter()
            .take_while(|s| s.get().is_some())
            .count()
    }

    /// The end of the segments this handle knows.
    fn known_end(&self) -> u64 {
        self.segments
            .iter()
            .map_while(OnceLock::get)
            .last()
            .map_or(0, |s| s.start.saturating_add(s.len))
    }

    /// Opens the segment after those this handle knows, if it exists: its index.
    fn find_next(&self) -> Result<Option<usize>, DiskError> {
        let index = self.known();
        if index >= self.segments.len() {
            return Ok(None);
        }
        let path = segment_path(&self.base, index);
        if !path.exists() {
            return Ok(None);
        }
        let mut file = DeviceFile::open(&path, false, self.caching, self.align)?;
        if self.shadow {
            file.set_residency_shadow(true);
        }
        let len = file.len()?;
        let start = self.known_end();
        match self.place(index, start, len, file) {
            Ok(()) => Ok(Some(index)),
            // Filled meanwhile through a shared reference: the same segment, found once.
            Err(_) if self.segments.get(index).is_some_and(|s| s.get().is_some()) => {
                Ok(Some(index))
            }
            Err(e) => Err(e),
        }
    }

    /// The segment holding `[offset, offset + len)` and its index, finding segments added since
    /// this handle last looked; a transfer across two segments, or past the last, is refused.
    fn segment(&self, offset: u64, len: usize) -> Result<(usize, &Segment), DiskError> {
        let end = offset
            .checked_add(u64::try_from(len).map_err(|_| beyond(&self.base, "a transfer past u64"))?)
            .ok_or_else(|| beyond(&self.base, "a transfer past u64"))?;
        loop {
            let at = self
                .segments
                .iter()
                .map_while(OnceLock::get)
                .enumerate()
                .find(|(_, s)| offset >= s.start && offset < s.start.saturating_add(s.len));
            if let Some((index, s)) = at {
                if end > s.start.saturating_add(s.len) {
                    return Err(beyond(&self.base, "a transfer across two segments"));
                }
                return Ok((index, s));
            }
            if offset < self.known_end() || self.find_next()?.is_none() {
                return Err(beyond(
                    &self.base,
                    "a transfer past the file's last segment",
                ));
            }
        }
    }

    /// Adds a segment of `len` bytes after the last, allocated whole and its name made durable
    /// before it is returned: its index and the offset it starts at. Its blocks are allocated
    /// (`DeviceFile::preallocate`), not written; its owner writes them before any other transfer
    /// uses them.
    pub fn add(&self, len: u64) -> Result<(usize, u64), DiskError> {
        while self.find_next()?.is_some() {}
        let index = self.known();
        if index >= self.segments.len() {
            return Err(beyond(&self.base, "segments past the file's bound"));
        }
        let path = segment_path(&self.base, index);
        let mut file = DeviceFile::open(&path, true, self.caching, self.align)?;
        if file.len()? != 0 {
            return Err(beyond(&path, "a new segment that already holds bytes"));
        }
        made_whole(&file, &path, len)?;
        if self.shadow {
            file.set_residency_shadow(true);
        }
        let start = self.known_end();
        self.place(index, start, len, file)?;
        Ok((index, start))
    }

    /// The segments, each its start and length, that this handle knows.
    pub fn layout(&self) -> Vec<(u64, u64)> {
        self.segments
            .iter()
            .map_while(OnceLock::get)
            .map(|s| (s.start, s.len))
            .collect()
    }

    /// The bound on segments given at open.
    pub fn bound(&self) -> usize {
        self.segments.len()
    }

    fn first(&self) -> Result<&Segment, DiskError> {
        self.segments
            .first()
            .and_then(OnceLock::get)
            .ok_or_else(|| beyond(&self.base, "a segmented file with no base"))
    }
}

impl BlockFile for SegmentedFile {
    fn alignment(&self) -> Alignment {
        self.align
    }

    fn layout_block(&self) -> Alignment {
        self.first().map_or(self.align, |s| s.file.layout_block())
    }

    fn len(&self) -> Result<u64, DiskError> {
        while self.find_next()?.is_some() {}
        Ok(self.known_end())
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        let (_, s) = self.segment(offset, buf.len())?;
        s.file.read_exact_at(buf, offset.saturating_sub(s.start))
    }

    fn read_resident_at(&mut self, buf: &mut [u8], offset: u64) -> Result<bool, DiskError> {
        let (index, start) = self.segment(offset, buf.len()).map(|(i, s)| (i, s.start))?;
        let s = self
            .segments
            .get_mut(index)
            .and_then(OnceLock::get_mut)
            .ok_or_else(|| beyond(&self.base, "a segment lost from its slot"))?;
        s.file
            .read_resident_with(buf, offset.saturating_sub(start), &mut self.epoch)
    }

    fn reads_resident(&self) -> bool {
        self.first().is_ok_and(|s| s.file.reads_resident())
    }

    fn residency(&self) -> crate::Residency {
        self.segments
            .iter()
            .map_while(OnceLock::get)
            .map(|s| s.file.residency())
            .fold(self.epoch.residency(), crate::Residency::plus)
    }

    fn set_residency_shadow(&mut self, on: bool) {
        self.shadow = on;
        for s in self.segments.iter_mut().filter_map(OnceLock::get_mut) {
            s.file.set_residency_shadow(on);
        }
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        let (_, s) = self.segment(offset, buf.len())?;
        s.dirty.store(true, Ordering::Release);
        s.file.write_all_at(buf, offset.saturating_sub(s.start))
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
            let (index, s) = self.segment(at, fill.len())?;
            let direct = DeviceFile::open(
                &segment_path(&self.base, index),
                false,
                CachingRequest::PreferDirect,
                self.align,
            )?;
            s.dirty.store(true, Ordering::Release);
            // The copies this segment holds from `at` on.
            let here = s
                .start
                .saturating_add(s.len)
                .saturating_sub(at)
                .checked_div(piece)
                .unwrap_or(0)
                .min(left);
            if here == 0 {
                return Err(beyond(&self.base, "a copy across two segments"));
            }
            direct.write_copies_at(fill, at.saturating_sub(s.start), here)?;
            at = piece
                .checked_mul(here)
                .and_then(|b| at.checked_add(b))
                .ok_or_else(|| beyond(&self.base, "a transfer past u64"))?;
            left = left.saturating_sub(here);
        }
        Ok(())
    }

    /// Flushes every segment written since its last flush: the base's flush alone would leave
    /// another segment's writes to a crash.
    fn sync_data(&self) -> Result<(), DiskError> {
        for s in self.segments.iter().map_while(OnceLock::get) {
            if s.dirty.swap(false, Ordering::AcqRel) {
                s.file.sync_data()?;
            }
        }
        Ok(())
    }

    fn fills_new_space(&self) -> bool {
        self.first().is_ok_and(|s| s.file.fills_new_space())
    }

    fn grows_by_segments(&self) -> bool {
        true
    }

    fn grow(&self, len: u64) -> Result<Option<u64>, DiskError> {
        self.add(len).map(|(_, start)| Some(start))
    }

    fn segment_end(&self, offset: u64) -> Result<Option<u64>, DiskError> {
        while self.find_next()?.is_some() {}
        Ok(self
            .segments
            .iter()
            .map_while(OnceLock::get)
            .find(|s| offset >= s.start && offset < s.start.saturating_add(s.len))
            .map(|s| s.start.saturating_add(s.len)))
    }

    fn write_durable_at(&self, buf: &[u8], offset: u64) -> Result<Durable, DiskError> {
        let (_, s) = self.segment(offset, buf.len())?;
        s.file.write_durable_at(buf, offset.saturating_sub(s.start))
    }

    fn try_clone(&self) -> Result<Self, DiskError> {
        let segments: Box<[OnceLock<Segment>]> =
            (0..self.segments.len()).map(|_| OnceLock::new()).collect();
        let copy = Self {
            base: self.base.clone(),
            caching: self.caching,
            align: self.align,
            segments,
            shadow: self.shadow,
            epoch: crate::resident::Epoch::new(),
        };
        for (i, s) in self.segments.iter().map_while(OnceLock::get).enumerate() {
            copy.place(i, s.start, s.len, s.file.try_clone()?)?;
        }
        Ok(copy)
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

    /// Segments added after the base are read and written at offsets into the concatenation,
    /// each byte landing in the segment that holds it; a duplicate made before a segment was
    /// added finds it on first use; reopened, the file has every segment and its whole length.
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
        let (index, start) = file.add(2 * 4096).unwrap();
        assert_eq!((index, start), (3, 4 * 4096));
        assert_eq!(file.len().unwrap(), 6 * 4096);
        file.write_all_at(page(2).as_slice(), 2 * 4096).unwrap();
        file.write_all_at(page(3).as_slice(), 5 * 4096).unwrap();
        file.sync_data().unwrap();
        // The duplicate made before either segment finds both.
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

    /// A flush covers each segment written since the last, and only those.
    #[test]
    fn a_flush_covers_the_segments_written_since_the_last() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("store");
        let file = create(&base, 4);
        file.write_all_at(page(1).as_slice(), 0).unwrap();
        file.add(4096).unwrap();
        file.add(4096).unwrap();
        file.write_all_at(page(2).as_slice(), 2 * 4096).unwrap();
        let dirty = |f: &SegmentedFile| -> Vec<bool> {
            f.segments
                .iter()
                .map_while(OnceLock::get)
                .map(|s| s.dirty.load(Ordering::Acquire))
                .collect()
        };
        assert_eq!(dirty(&file), vec![false, true, false, true]);
        file.sync_data().unwrap();
        assert_eq!(dirty(&file), vec![false, false, false, false]);
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
        let dirty: Vec<bool> = file
            .segments
            .iter()
            .map_while(OnceLock::get)
            .map(|s| s.dirty.load(Ordering::Acquire))
            .collect();
        assert_eq!(dirty, vec![false, true, true]);
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
            for i in 0..3 {
                assert!(!fresh.read_resident_at(b.as_mut_slice(), at(i)).unwrap());
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
}
