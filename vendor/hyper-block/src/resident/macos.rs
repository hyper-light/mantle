//! macOS: whether a range of a file is in the unified buffer cache, from `mincore(2)` over a
//! read-only shared mapping of the file, and a `pread(2)` of the range when every page of it is.
//!
//! Measured on an M5 Max under a load average of 76 to 98 (mantle
//! `docs/research/41-resident-reads.md`): the mapping reports the cache's residency exactly, for
//! pages it never touched and for pages written after the file grew past its end at mapping
//! time: every page of a file written through the cache, none of one written with `F_NOCACHE`,
//! and after one `pread(2)` that page alone. A probe on a kept mapping costs 2.0 to 2.7 µs and one
//! with its own `mmap(2)` and `munmap(2)` 10 to 12 µs, so the mapping is kept, and remapped twice
//! as long when a read reaches past it: the remaps a file's growth costs are logarithmic in its
//! length, and the address space held at most twice what was read (Cormen et al., Introduction
//! to Algorithms, 3rd ed., §17.4, the table that doubles).
//!
//! No byte is read through the mapping, which is only passed to `mincore(2)`: a page evicted, or
//! the file truncated, after the probe costs a `pread(2)` that waits or fails, never a fault in
//! this process (Crotty, Leis and Pavlo, CIDR 2022, on what reading through a mapping costs a
//! database).
//!
//! A probe's answer is kept, one bit an OS page, while the kernel reclaims no file-backed page:
//! its pageout scan counts every page it frees by the queue it left, and the sum of the three
//! file-backed counts is an epoch (XNU `vm_pageout.c`, `reclaim_page`; `vm_unix.c`,
//! `vm.pageout_freed_*`). The scan reclaims only once free memory falls to its target, which
//! memory cannot do faster than the machine can write it: so the epoch is read again before that
//! could happen, after (free − target) bytes at the fastest fill rate, or, at or below the
//! target, after the target's own bytes at that rate. A read whose pages were all found resident
//! in the epoch now is made without probing; a changed epoch forgets every answer. A superuser's
//! `vfs_purge`, which no counter sees, can leave answers stale until the epoch next moves: such a
//! read waits for the device on its caller's thread, as a page evicted between a probe and its
//! read does. Shadow mode probes anyway and counts the wrong answers (mantle
//! `docs/research/46-reclaim-epochs.md`).
#![allow(unsafe_code)]

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;

/// A handle's mapping for probes, the vector `mincore(2)` fills, and its probes' answers.
#[derive(Debug)]
pub(crate) struct Resident {
    window: Option<Window>,
    /// One byte for each page of the range probed: kept, so a probe allocates only to grow it.
    pages: Vec<libc::c_char>,
    /// The window could not be mapped, or a probe refused: the handle probes no more, and its
    /// reads all go to the thread that may wait.
    refused: bool,
    /// For each 64 OS pages of the file, the pages a probe found resident, and the generation it
    /// found them in: a word of another generation answers nothing.
    bits: Vec<u64>,
    gens: Vec<u32>,
    generation: u32,
    /// The kernel's count of file-backed pages reclaimed at the last refresh, which the
    /// generation stands for; none before the first, or when it cannot be read, and then every
    /// read probes.
    epoch: Option<u64>,
    /// When the epoch must be read again; none before the first read.
    due: Option<std::time::Instant>,
    /// The names could not be resolved: no answer is kept, and none asked for again.
    unanswerable: bool,
    /// The counters' and the free count's resolved names, and the free target in bytes, found
    /// at the first refresh; none when one cannot be, and then no answer is kept.
    mibs: Option<Mibs>,
    shadow: bool,
    counts: super::Residency,
}

#[derive(Clone, Copy, Debug)]
struct Mibs {
    reclaimed: [Mib; 3],
    free: Mib,
    target_bytes: u64,
}

/// The most integers a sysctl name has (`CTL_MAXNAME` in the macOS SDK's `<sys/sysctl.h>`),
/// which the `libc` crate leaves out for Apple targets.
const CTL_MAXNAME: usize = 12;

/// A sysctl name resolved to its numbers, for reads that skip the lookup by string.
#[derive(Clone, Copy, Debug)]
struct Mib {
    name: [libc::c_int; CTL_MAXNAME],
    len: usize,
}

/// The pageout scan's counts of file-backed pages freed: speculative, cleaned, and the rest.
const RECLAIMED: [&std::ffi::CStr; 3] = [
    c"vm.pageout_freed_speculative",
    c"vm.pageout_freed_cleaned",
    c"vm.pageout_freed_external",
];

/// The fastest any Mac fills memory, in bytes a nanosecond: the highest unified memory bandwidth
/// Apple publishes, 819 GB/s (Mac Studio with M3 Ultra, Apple technical specifications, 2025).
/// The fastest bound gives the shortest time free memory could take to reach the target, so a
/// slower machine reads the epoch early, never late.
const FILL_BYTES_PER_NS: u64 = 819;

impl Mib {
    fn resolve(name: &std::ffi::CStr) -> Option<Self> {
        let mut mib = Self {
            name: [0; CTL_MAXNAME],
            len: CTL_MAXNAME,
        };
        // SAFETY: `name` is NUL-terminated; `mib.name` holds `mib.len` integers, the capacity
        // sysctlnametomib(3) is told and writes at most, and it sets `mib.len` to the count used.
        let resolved =
            unsafe { libc::sysctlnametomib(name.as_ptr(), mib.name.as_mut_ptr(), &mut mib.len) };
        if resolved == 0 && mib.len <= mib.name.len() {
            Some(mib)
        } else {
            None
        }
    }

    fn read(&self) -> Option<u64> {
        self.read_as::<u64>()
    }

    /// An `int` sysctl, widened.
    fn read_int(&self) -> Option<u64> {
        self.read_as::<libc::c_uint>().map(u64::from)
    }

    fn read_as<T: Copy + Default>(&self) -> Option<T> {
        let mut value = T::default();
        let mut size = std::mem::size_of::<T>();
        let len = libc::c_uint::try_from(self.len).ok()?;
        // SAFETY: `self.name` holds `self.len` resolved integers, which sysctl(3) only reads
        // though its prototype takes them mutable; `value` is `size` writable bytes of a plain
        // integer type, the most it writes; no new value is given.
        let read = unsafe {
            libc::sysctl(
                self.name.as_ptr().cast_mut(),
                len,
                std::ptr::from_mut(&mut value).cast(),
                &mut size,
                std::ptr::null_mut(),
                0,
            )
        };
        if read == 0 && size == std::mem::size_of::<T>() {
            Some(value)
        } else {
            None
        }
    }
}

/// A read-only shared mapping of the file from its start, its address exposed so the handle
/// that holds it moves between threads: the address is only ever passed to `mincore(2)` and
/// `munmap(2)`.
#[derive(Debug)]
struct Window {
    addr: usize,
    len: usize,
}

impl Window {
    fn map(file: &File, len: usize) -> io::Result<Self> {
        // SAFETY: a new read-only shared mapping at an address the kernel chooses, of a
        // descriptor this borrow holds open for the call; no reference in the process names that
        // range before mmap(2) returns it, and none is made after: the mapping is only probed.
        let addr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if addr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            addr: addr.expose_provenance(),
            len,
        })
    }
}

impl Drop for Window {
    fn drop(&mut self) {
        // SAFETY: `addr` and `len` are a mapping this process made with mmap(2), unmapped only
        // here, and nothing refers into it: it was only passed to mincore(2). A failure leaves
        // the range mapped, which costs address space and nothing else.
        unsafe {
            libc::munmap(std::ptr::with_exposed_provenance_mut(self.addr), self.len);
        }
    }
}

impl Resident {
    /// A handle with no mapping yet: it maps one on its first read.
    pub(crate) fn new() -> Self {
        Self {
            window: None,
            pages: Vec::new(),
            refused: false,
            bits: Vec::new(),
            gens: Vec::new(),
            generation: 0,
            epoch: None,
            due: None,
            unanswerable: false,
            mibs: None,
            shadow: false,
            counts: super::Residency::default(),
        }
    }

    /// The names resolved and the free target read, once; none when any cannot be.
    fn resolve() -> Option<Mibs> {
        let reclaimed: Vec<Mib> = RECLAIMED
            .iter()
            .map(|n| Mib::resolve(n))
            .collect::<Option<_>>()?;
        let free = Mib::resolve(c"vm.page_free_count")?;
        let target = Mib::resolve(c"vm.vm_page_free_target")?.read_int()?;
        let page = u64::try_from(rustix::param::page_size()).ok()?;
        Some(Mibs {
            reclaimed: <[Mib; 3]>::try_from(reclaimed).ok()?,
            free,
            target_bytes: target.checked_mul(page)?,
        })
    }

    /// Reads the epoch and the free memory, and sets when to read them again: a changed epoch,
    /// or one that cannot be read, forgets every answer.
    fn refresh(&mut self, now: std::time::Instant) {
        if self.due.is_none() {
            self.mibs = Self::resolve();
        }
        let Some(mibs) = self.mibs else {
            self.epoch = None;
            self.unanswerable = true;
            return;
        };
        let read = mibs
            .reclaimed
            .iter()
            .try_fold(0u64, |sum, mib| mib.read().map(|v| sum.wrapping_add(v)));
        let page = u64::try_from(rustix::param::page_size()).unwrap_or(u64::MAX);
        let free = mibs.free.read_int().map(|f| f.saturating_mul(page));
        self.counts.epochs = self.counts.epochs.saturating_add(1);
        // The time free memory needs to reach the target at the fastest fill, or to spend the
        // target's own bytes once it is there.
        let room = free.map_or(0, |f| {
            f.saturating_sub(mibs.target_bytes).max(mibs.target_bytes)
        });
        self.due = Some(
            now.checked_add(std::time::Duration::from_nanos(room / FILL_BYTES_PER_NS))
                .unwrap_or(now),
        );
        if read.is_some() && read == self.epoch {
            return;
        }
        if self.epoch.is_some() {
            self.counts.changes = self.counts.changes.saturating_add(1);
        }
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            // A wrapped generation could match a word left from four billion epochs ago.
            self.bits.iter_mut().for_each(|b| *b = 0);
        }
        self.epoch = read;
    }

    /// What its reads from memory did.
    pub(crate) fn residency(&self) -> super::Residency {
        self.counts
    }

    /// Shadow mode: a read made on an answer probes anyway first, and counts the answer wrong
    /// when the probe disagrees. For measurement: it costs the probe the answer saves.
    pub(crate) fn set_shadow(&mut self, on: bool) {
        self.shadow = on;
    }

    /// Whether every OS page in [`first`, `end`) was found resident in this epoch.
    fn answered(&self, first: usize, end: usize) -> bool {
        (first..end).all(|p| {
            let (w, b) = (p / 64, p % 64);
            self.gens.get(w) == Some(&self.generation)
                && self.bits.get(w).is_some_and(|bits| bits & (1u64 << b) != 0)
        })
    }

    /// Keeps the answer that every OS page in [`first`, `end`) is resident; none when the words
    /// cannot grow, which only costs those pages a probe again.
    fn answer(&mut self, first: usize, end: usize) {
        let words = end.div_ceil(64);
        if words > self.bits.len() {
            let more = words.saturating_sub(self.bits.len());
            if self.bits.try_reserve(more).is_err() || self.gens.try_reserve(more).is_err() {
                return;
            }
            self.bits.resize(words, 0);
            self.gens.resize(words, 0);
        }
        for p in first..end {
            let (w, b) = (p / 64, p % 64);
            if let (Some(bits), Some(generation)) = (self.bits.get_mut(w), self.gens.get_mut(w)) {
                if *generation != self.generation {
                    *generation = self.generation;
                    *bits = 0;
                }
                *bits |= 1u64 << b;
            }
        }
    }

    /// Fills `buf` from `offset` if every page of it is in the unified buffer cache now: true when
    /// it did; false when a page is not, the file ends first, or the handle cannot probe.
    pub(crate) fn read(&mut self, file: &File, buf: &mut [u8], offset: u64) -> io::Result<bool> {
        if self.refused || buf.is_empty() {
            return Ok(false);
        }
        let page = rustix::param::page_size();
        // The whole pages the read touches, [first, end) of the file.
        let Some((first, end)) = usize::try_from(offset).ok().and_then(|start| {
            let first = start.checked_sub(start.checked_rem(page)?)?;
            let end = start
                .checked_add(buf.len())?
                .checked_next_multiple_of(page)?;
            Some((first, end))
        }) else {
            return Ok(false);
        };
        let (p0, p1) = (first / page, end / page);
        let now = std::time::Instant::now();
        if !self.unanswerable && self.due.is_none_or(|due| now >= due) {
            self.refresh(now);
        }
        if self.epoch.is_some() && self.answered(p0, p1) {
            if self.shadow && self.probe(file, first, end)? != Some(true) {
                self.counts.wrong = self.counts.wrong.saturating_add(1);
            }
            self.counts.skips = self.counts.skips.saturating_add(1);
            return Self::read_now(file, buf, offset);
        }
        match self.probe(file, first, end)? {
            Some(true) => {
                if self.epoch.is_some() {
                    self.answer(p0, p1);
                }
                Self::read_now(file, buf, offset)
            }
            _ => Ok(false),
        }
    }

    /// Asks `mincore(2)` whether every page of [`first`, `end`) of the file is resident: none
    /// when the handle cannot ask.
    fn probe(&mut self, file: &File, first: usize, end: usize) -> io::Result<Option<bool>> {
        let page = rustix::param::page_size();
        self.counts.probes = self.counts.probes.saturating_add(1);
        if self.window.as_ref().is_none_or(|w| w.len < end) {
            let len = self
                .window
                .as_ref()
                .and_then(|w| w.len.checked_mul(2))
                .map_or(end, |doubled| doubled.max(end));
            // The old window is unmapped as the new one replaces it.
            self.window = None;
            match Window::map(file, len) {
                Ok(window) => self.window = Some(window),
                Err(_) => {
                    self.refused = true;
                    return Ok(None);
                }
            }
        }
        let Some(window) = self.window.as_ref() else {
            return Ok(None);
        };
        let (Some(span), Some(at)) = (end.checked_sub(first), window.addr.checked_add(first))
        else {
            return Ok(None);
        };
        let count = span.checked_div(page).unwrap_or(0);
        self.pages.clear();
        if self.pages.try_reserve(count).is_err() {
            return Ok(None);
        }
        self.pages.resize(count, 0);
        // SAFETY: [at, at + span) lies inside the live mapping (`first < end <= window.len`), and
        // `pages` holds one byte for each of its `span / page` pages, which mincore(2) writes.
        let probed = unsafe {
            libc::mincore(
                std::ptr::with_exposed_provenance(at),
                span,
                self.pages.as_mut_ptr(),
            )
        };
        if probed != 0 {
            self.refused = true;
            return Ok(None);
        }
        Ok(Some(
            self.pages
                .iter()
                .all(|&p| i32::from(p) & libc::MINCORE_INCORE != 0),
        ))
    }

    /// Reads a range found, or answered, resident: copied from memory. A page evicted since
    /// makes this read wait for the device once, on this thread; its bytes are right either way.
    fn read_now(file: &File, buf: &mut [u8], offset: u64) -> io::Result<bool> {
        match file.read_exact_at(buf, offset) {
            Ok(()) => Ok(true),
            // The file ends inside the range: the read that may wait reports that end.
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Whether this handle reads nothing from memory: its window could not be mapped.
    #[cfg(test)]
    pub(crate) fn refused(&self) -> bool {
        self.refused
    }

    /// Holds the epoch where it is for a test, whatever the host's kernel reclaims meanwhile:
    /// answers are kept, and no refresh comes due.
    #[cfg(test)]
    pub(crate) fn hold_epoch(&mut self) {
        let now = std::time::Instant::now();
        self.mibs = Self::resolve();
        self.epoch = Some(self.epoch.unwrap_or(0));
        self.due = now.checked_add(std::time::Duration::from_secs(u64::from(u32::MAX)));
    }

    /// What a refresh does when the epoch moved: every answer forgotten.
    #[cfg(test)]
    pub(crate) fn move_epoch(&mut self) {
        self.epoch = self.epoch.map(|e| e.wrapping_add(1));
        self.generation = self.generation.wrapping_add(1);
    }

    /// A refresh against the kernel's counters as they are, with the epoch held one off them.
    #[cfg(test)]
    pub(crate) fn refresh_from_off_epoch(&mut self) -> (u64, u64) {
        let now = std::time::Instant::now();
        self.refresh(now);
        let (before, generation) = (self.counts.changes, self.generation);
        self.epoch = self.epoch.map(|e| e.wrapping_sub(1));
        self.refresh(now);
        assert_ne!(
            self.generation, generation,
            "a changed epoch is a new generation"
        );
        assert!(self.due.is_some_and(|due| due >= now));
        (before, self.counts.changes)
    }
}
