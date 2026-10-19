//! RocksDB's `port/mmap.{h,cc}`: memory mapped anonymously, zero-filled by the OS and made
//! resident only where it is written (`MemMapping::AllocateLazyZeroed`), here as the 8-byte words
//! the memtable's arena stores into (memory/arena.rs).
//!
//! The OS interface (CLAUDE.md §7): on Unix `mmap(MAP_PRIVATE | MAP_ANONYMOUS)` and `munmap`
//! (mmap(2): anonymous mappings are initialized to zero; the pages are allocated on first touch);
//! on Windows a page-file-backed section, `CreateFileMappingW(INVALID_HANDLE_VALUE, …,
//! PAGE_READWRITE | SEC_COMMIT)` and `MapViewOfFile`, as RocksDB's port does (the Win32 reference:
//! a section's pages are zero-initialized and backed by the page file, committed as charge, and
//! made resident when touched). RocksDB's `AllocateHuge` waits for the arena's huge-page option.
#![allow(unsafe_code)]

use std::ptr::NonNull;
use std::sync::atomic::AtomicU64;

use crate::error::Error;

/// The size of one word, the unit a mapping is counted in.
const WORD: usize = std::mem::size_of::<AtomicU64>();

/// Has the C library's allocator give a freed large block's memory back to the OS when it is
/// freed, as it does until its first such block is freed. glibc's `malloc` maps each block of at
/// least its mmap threshold, 128 KiB to begin with, and unmaps it when freed; but each mapped block
/// up to 32 MiB that is freed raises the threshold to its size, so later blocks of that size come
/// from its arenas and stay there once freed (mallopt(3), `M_MMAP_THRESHOLD`): 1.3-2.8 GiB held
/// at 100M, outside any account (docs/research/64 §6). Setting the threshold, here to its initial
/// value, turns the adjustment off (mallopt(3)). For a process's owner to call at its start,
/// before its threads allocate; elsewhere than glibc it does nothing.
pub fn return_freed_blocks() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        unsafe extern "C" {
            fn mallopt(param: i32, value: i32) -> i32;
        }
        /// glibc's `M_MMAP_THRESHOLD` parameter number (malloc.h).
        const M_MMAP_THRESHOLD: i32 = -3;
        /// glibc's initial mmap threshold, `DEFAULT_MMAP_THRESHOLD_MIN` (mallopt(3): 128*1024).
        const INITIAL_MMAP_THRESHOLD: i32 = 128 * 1024;
        // SAFETY: mallopt(3) takes a parameter number and a value and changes only the
        // allocator's settings; a refusal (0) leaves them as they were.
        unsafe {
            mallopt(M_MMAP_THRESHOLD, INITIAL_MMAP_THRESHOLD);
        }
    }
}

/// The size of the OS's memory pages, the least a mapping takes: below it a block is not mapped
/// (memory/arena.rs).
pub fn page_size() -> usize {
    #[cfg(unix)]
    {
        rustix::param::page_size()
    }
    #[cfg(windows)]
    {
        let mut info = windows_sys::Win32::System::SystemInformation::SYSTEM_INFO::default();
        // SAFETY: GetSystemInfo writes the SYSTEM_INFO it is given, which lives for the call.
        unsafe { windows_sys::Win32::System::SystemInformation::GetSystemInfo(&mut info) };
        usize::try_from(info.dwPageSize).unwrap_or(usize::MAX)
    }
}

/// `words` 8-byte words of anonymous memory, zero until written and resident only where written,
/// released when dropped.
pub struct LazyZeroed {
    /// The mapping's first word; `None` for a mapping of no words, which maps nothing.
    addr: Option<NonNull<AtomicU64>>,
    words: usize,
    /// The section the view maps, closed after the view is unmapped.
    #[cfg(windows)]
    section: windows_sys::Win32::Foundation::HANDLE,
}

// SAFETY: the mapping is owned by this value alone and every access to it is through `&AtomicU64`
// (`words`), whose loads and stores are atomic, so sharing or moving it across threads cannot
// race; it is unmapped only in `drop`, when no borrow of it is left.
unsafe impl Send for LazyZeroed {}
// SAFETY: as for `Send`: shared access is only through atomics.
unsafe impl Sync for LazyZeroed {}

impl std::fmt::Debug for LazyZeroed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyZeroed")
            .field("words", &self.words)
            .finish()
    }
}

impl LazyZeroed {
    /// Maps `words` zeroed words, or refuses with the OS's error.
    pub fn allocate(words: usize) -> Result<Self, Error> {
        let refused = |_| Error::LimitExceeded {
            what: "memtable arena block (the OS refused the mapping)",
            limit: u64::try_from(words.saturating_mul(WORD)).unwrap_or(u64::MAX),
        };
        let bytes = words.checked_mul(WORD).ok_or_else(|| refused(()))?;
        if bytes == 0 {
            return Ok(Self::empty());
        }
        Self::map(bytes, words).map_err(refused)
    }

    fn empty() -> Self {
        Self {
            addr: None,
            words: 0,
            #[cfg(windows)]
            section: std::ptr::null_mut(),
        }
    }

    #[cfg(unix)]
    fn map(bytes: usize, words: usize) -> Result<Self, ()> {
        use rustix::mm::{MapFlags, ProtFlags, mmap_anonymous};
        // SAFETY: a new private anonymous mapping at an address the kernel chooses touches no
        // memory this process already uses; `bytes` is nonzero.
        let addr = unsafe {
            mmap_anonymous(
                std::ptr::null_mut(),
                bytes,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::PRIVATE,
            )
        }
        .map_err(|_| ())?;
        let addr = NonNull::new(addr.cast::<AtomicU64>()).ok_or(())?;
        Ok(Self {
            addr: Some(addr),
            words,
        })
    }

    #[cfg(windows)]
    fn map(bytes: usize, words: usize) -> Result<Self, ()> {
        use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
        use windows_sys::Win32::System::Memory::{
            CreateFileMappingW, FILE_MAP_WRITE, MapViewOfFile, PAGE_READWRITE, SEC_COMMIT,
        };
        let size = u64::try_from(bytes).map_err(|_| ())?;
        let high = u32::try_from(size >> 32).map_err(|_| ())?;
        let low = u32::try_from(size & u64::from(u32::MAX)).map_err(|_| ())?;
        // SAFETY: a section backed by the page file (INVALID_HANDLE_VALUE), unnamed, with no
        // security attributes; the handle is closed in `drop` or below on failure.
        let section = unsafe {
            CreateFileMappingW(
                INVALID_HANDLE_VALUE,
                std::ptr::null(),
                PAGE_READWRITE | SEC_COMMIT,
                high,
                low,
                std::ptr::null(),
            )
        };
        if section.is_null() {
            return Err(());
        }
        // SAFETY: maps the whole section just created, writable; unmapped in `drop`.
        let view = unsafe { MapViewOfFile(section, FILE_MAP_WRITE, 0, 0, bytes) };
        let Some(addr) = NonNull::new(view.Value.cast::<AtomicU64>()) else {
            // SAFETY: closes the section this function opened, which nothing else holds.
            unsafe { CloseHandle(section) };
            return Err(());
        };
        Ok(Self {
            addr: Some(addr),
            words,
            section,
        })
    }

    /// The mapping's words.
    pub fn words(&self) -> &[AtomicU64] {
        match self.addr {
            // SAFETY: `addr` is the start of a live mapping of `words` words: page-aligned, so
            // aligned for `AtomicU64`, which has `u64`'s size and alignment; zero-filled by the
            // OS, and every bit pattern of a `u64` is a valid `AtomicU64`; unmapped only in
            // `drop`, which the borrow of `self` this slice holds keeps from running.
            Some(addr) => unsafe { std::slice::from_raw_parts(addr.as_ptr(), self.words) },
            None => &[],
        }
    }
}

impl Drop for LazyZeroed {
    fn drop(&mut self) {
        let Some(addr) = self.addr.take() else {
            return;
        };
        #[cfg(unix)]
        {
            let bytes = self.words.saturating_mul(WORD);
            // SAFETY: unmaps exactly the mapping `map` made, which no borrow outlives (`words`
            // borrows `self`). A failure leaves the pages mapped: nothing can act on it in a
            // drop, and RocksDB's port ignores it too [R port/mmap.cc:27-33].
            let _ = unsafe { rustix::mm::munmap(addr.as_ptr().cast(), bytes) };
        }
        #[cfg(windows)]
        {
            use windows_sys::Win32::Foundation::CloseHandle;
            use windows_sys::Win32::System::Memory::{MEMORY_MAPPED_VIEW_ADDRESS, UnmapViewOfFile};
            // SAFETY: unmaps the view `map` made and closes its section, neither held elsewhere;
            // failures are ignored as on Unix [R port/mmap.cc:19-25].
            unsafe {
                UnmapViewOfFile(MEMORY_MAPPED_VIEW_ADDRESS {
                    Value: addr.as_ptr().cast(),
                });
                CloseHandle(self.section);
            }
        }
    }
}

/// `len` bytes of anonymous memory, zero, faulted in for writing when made, which Linux backs with
/// transparent huge pages where it has them (`madvise(MADV_HUGEPAGE)`, madvise(2)). For a table
/// read at random: on 4 KiB pages its probes walk the page tables, which cost a 10M get the read a
/// one-read layout saves (research/63 §4), and a put its memtable index's probe (research/63 §10).
/// Released when dropped.
///
/// It is faulted in for writing at once (`MADV_POPULATE_WRITE`, Linux 5.14) because a first touch
/// that reads, as a table's probe before its insert does, maps the shared zero page, and the write
/// after it copies 4 KiB pages out of it: the kernel allocates no huge page on that path, and a
/// 10M table built so held none (research/63 §4). A kernel without it has each page written once
/// instead. Elsewhere a mapping as [`LazyZeroed`]'s, whose pages are the OS's size.
///
/// The populate goes a huge page at a time ([`huge_page_size`]). One `madvise` holds the
/// process's mmap lock for reading over its whole range (Linux v6.12 mm/madvise.c,
/// `madvise_populate`, which drops it only for a fault that retries), and a huge page's fault in
/// a `MADV_HUGEPAGE` range may reclaim and compact memory before it maps (the `defrag` modes of
/// Documentation/admin-guide/mm/transhuge.rst). A 100M table's gigabytes in one call held the lock
/// for up to a second, and every mapping change in the process waited behind it: the shard's next
/// memtable, whose allocation maps (box, 100M: rotations of 0.4-1.1 s).
pub struct HugeZeroed {
    addr: Option<NonNull<u8>>,
    len: usize,
    /// The bytes mapped. On Linux from a huge page's boundary ([`huge_page_size`]), so the kernel
    /// can back each huge page the bytes cover with one and a move keeps each whole
    /// ([`Self::resize`]), and to a whole number of huge pages where one is at most an eighth of
    /// the bytes ([`mapped_len`]). Elsewhere `len` in words.
    mapped: usize,
    #[cfg(windows)]
    section: windows_sys::Win32::Foundation::HANDLE,
}

// SAFETY: the mapping is owned by this value alone, read through `&self` and written through
// `&mut self` only, so moving it or sharing it across threads cannot race; it is unmapped only
// in `drop`, when no borrow of it is left.
unsafe impl Send for HugeZeroed {}
// SAFETY: as for `Send`: shared access is read-only.
unsafe impl Sync for HugeZeroed {}

impl std::fmt::Debug for HugeZeroed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HugeZeroed")
            .field("len", &self.len)
            .finish()
    }
}

impl HugeZeroed {
    /// A mapping of no bytes, which maps nothing.
    pub fn empty() -> Self {
        Self {
            addr: None,
            len: 0,
            mapped: 0,
            #[cfg(windows)]
            section: std::ptr::null_mut(),
        }
    }

    /// Maps `len` zeroed bytes, or refuses with the OS's error.
    #[cfg(target_os = "linux")]
    pub fn allocate(len: usize) -> Result<Self, Error> {
        if len == 0 {
            return Ok(Self::empty());
        }
        let refused = || Error::LimitExceeded {
            what: "a table on huge pages (the OS refused the mapping)",
            limit: u64::try_from(len).unwrap_or(u64::MAX),
        };
        let huge = huge_page_size().max(page_size()).max(1);
        let mapped = mapped_len(len).ok_or_else(refused)?;
        let rw = rustix::mm::ProtFlags::READ | rustix::mm::ProtFlags::WRITE;
        let addr =
            map_aligned(mapped, huge, rw, rustix::mm::MapFlags::empty()).map_err(|()| refused())?;
        let mut m = Self {
            addr: Some(addr),
            len,
            mapped,
        };
        m.fault_in(0);
        Ok(m)
    }

    /// Maps `len` zeroed bytes, or refuses with the OS's error.
    #[cfg(not(target_os = "linux"))]
    pub fn allocate(len: usize) -> Result<Self, Error> {
        let refused = |_| Error::LimitExceeded {
            what: "a table on huge pages (the OS refused the mapping)",
            limit: u64::try_from(len).unwrap_or(u64::MAX),
        };
        if len == 0 {
            return Ok(Self::empty());
        }
        let words = len.div_ceil(WORD);
        let mut lazy = LazyZeroed::map(words.checked_mul(WORD).ok_or_else(|| refused(()))?, words)
            .map_err(refused)?;
        // The mapping now belongs to this value: taken out of `lazy`, whose drop then unmaps
        // nothing.
        let addr = lazy.addr.take().map(NonNull::cast::<u8>);
        #[cfg(windows)]
        let section = std::mem::replace(&mut lazy.section, std::ptr::null_mut());
        drop(lazy);
        let mut m = Self {
            addr,
            len,
            mapped: words.saturating_mul(WORD),
            #[cfg(windows)]
            section,
        };
        m.fault_in(0);
        Ok(m)
    }

    /// The bytes it holds mapped, at least its length.
    pub fn mapped(&self) -> usize {
        self.mapped
    }

    /// Whether [`Self::resize`] keeps the mapping's pages where they are. On Linux they move
    /// with the mapping (`mremap(MREMAP_MAYMOVE)`, mremap(2)), and nothing is copied: a resize
    /// holds no second copy beside the first. Elsewhere a new mapping takes a copy.
    pub const RESIZES_IN_PLACE: bool = cfg!(target_os = "linux");

    /// Resized to `len` bytes: the first `len.min(self.len())` kept, zero after them, the new
    /// bytes faulted in as [`Self::allocate`]'s are. Refused, the mapping is as it was.
    pub fn resize(&mut self, len: usize) -> Result<(), Error> {
        if len == self.len {
            return Ok(());
        }
        if self.addr.is_none() || len == 0 {
            *self = Self::allocate(len)?;
            return Ok(());
        }
        self.remap(len)
    }

    /// [`Self::resize`] of a mapping on Linux: shrunk in place; grown in place where the address
    /// space after it is free, nothing moving; else moved onto a reservation from a huge page's
    /// boundary, its pages moving with it, nothing copied. The
    /// mapping starts on one too, and the kernel moves a huge page whole only between addresses
    /// both on its boundary, splitting it into base pages otherwise (Linux v6.12 mm/mremap.c,
    /// `move_page_tables`): moved where the kernel chose, a 10M table kept 75% of its bytes on
    /// huge pages and a 100M one's gets ran 0.81x (box, inplace 51e56b6, r391-r396, r471). Only
    /// the bytes past the old length are populated.
    #[cfg(target_os = "linux")]
    fn remap(&mut self, len: usize) -> Result<(), Error> {
        use rustix::mm::{MapFlags, MremapFlags, ProtFlags};
        let refused = || Error::LimitExceeded {
            what: "a kept record table (the OS refused to resize its mapping)",
            limit: u64::try_from(len).unwrap_or(u64::MAX),
        };
        let Some(addr) = self.addr else {
            return Err(refused());
        };
        let huge = huge_page_size().max(page_size()).max(1);
        let mapped = mapped_len(len).ok_or_else(refused)?;
        let (old, old_mapped) = (self.len, self.mapped);
        if mapped != old_mapped {
            // SAFETY: `addr` maps the `old_mapped` bytes this value owns, and `&mut self` leaves no
            // borrow of them. Without MREMAP_MAYMOVE the mapping stays where it is: shrunk, its
            // pages past `mapped` unmapped; grown, only into address space no mapping holds.
            // Refused, it is unchanged (mremap(2)).
            let in_place = unsafe {
                rustix::mm::mremap(
                    addr.as_ptr().cast(),
                    old_mapped,
                    mapped,
                    MremapFlags::empty(),
                )
            };
            if in_place.is_err() {
                if mapped < old_mapped {
                    return Err(refused());
                }
                let dst = map_aligned(mapped, huge, ProtFlags::empty(), MapFlags::NORESERVE)
                    .map_err(|()| refused())?;
                // SAFETY: as above for `addr`; `dst` starts `mapped` bytes this value just
                // reserved, which MREMAP_FIXED unmaps as the pages move there, and on a huge
                // page's boundary as `addr` is, so each huge page moves whole. Refused, the old
                // mapping is unchanged and the reservation is unmapped below.
                let moved = unsafe {
                    rustix::mm::mremap_fixed(
                        addr.as_ptr().cast(),
                        old_mapped,
                        mapped,
                        MremapFlags::MAYMOVE,
                        dst.as_ptr().cast(),
                    )
                };
                match moved.ok().and_then(|p| NonNull::new(p.cast::<u8>())) {
                    Some(p) => self.addr = Some(p),
                    None => {
                        // SAFETY: unmaps the reservation made above, which nothing else holds.
                        let _ = unsafe { rustix::mm::munmap(dst.as_ptr().cast(), mapped) };
                        return Err(refused());
                    }
                }
            }
            self.mapped = mapped;
        }
        self.len = len;
        if len > old {
            // The bytes past the old length that it had mapped may hold what a shrink left there:
            // zeroed, so every byte gained reads zero as fresh pages do.
            let end = old_mapped.min(len);
            if let Some(tail) = self.bytes_mut().get_mut(old..end) {
                tail.fill(0);
            }
            self.fault_in(old);
        }
        Ok(())
    }

    /// [`Self::resize`] elsewhere: a new mapping, the old bytes copied into it.
    #[cfg(not(target_os = "linux"))]
    fn remap(&mut self, len: usize) -> Result<(), Error> {
        let mut fresh = Self::allocate(len)?;
        let keep = len.min(self.len);
        if let (Some(dst), Some(src)) =
            (fresh.bytes_mut().get_mut(..keep), self.bytes().get(..keep))
        {
            dst.copy_from_slice(src);
        }
        *self = fresh;
        Ok(())
    }

    /// Asks Linux for huge pages and faults every page from byte `from` on in for writing, a huge
    /// page at a time (the type's docs).
    #[cfg(target_os = "linux")]
    fn fault_in(&mut self, from: usize) {
        let Some(addr) = self.addr else {
            return;
        };
        // SAFETY: advises the whole mapping this value owns; the advice changes how the kernel
        // backs it, never its contents. Refused, the pages are the base size.
        let _ = unsafe {
            rustix::mm::madvise(
                addr.as_ptr().cast(),
                self.mapped,
                rustix::mm::Advice::LinuxHugepage,
            )
        };
        let step = huge_page_size().max(page_size()).max(1);
        let page = page_size().max(1);
        let mut done = from.checked_div(page).unwrap_or(0).saturating_mul(page);
        while done < self.len {
            let n = step.min(self.len.saturating_sub(done));
            // SAFETY: `done` is below `len`, so the range starts inside the mapping this value
            // owns and its `n` bytes end inside it; `done` starts on a page and moves in steps of
            // whole pages, so the start is page aligned as madvise(2) asks. Populating for
            // writing leaves the bytes the mapping holds as they are.
            let populated = unsafe {
                rustix::mm::madvise(
                    addr.as_ptr().add(done).cast(),
                    n,
                    rustix::mm::Advice::LinuxPopulateWrite,
                )
            };
            if populated.is_err() {
                // A kernel before 5.14: each page left written once, a write fault each; the
                // bytes written are the ones each holds.
                if let Some(rest) = self.bytes_mut().get_mut(done..) {
                    for b in rest.iter_mut().step_by(page) {
                        *b = std::hint::black_box(*b);
                    }
                }
                return;
            }
            done = done.saturating_add(n);
        }
    }

    /// Elsewhere pages fault in as they are first touched, at the OS's page size.
    #[cfg(not(target_os = "linux"))]
    fn fault_in(&mut self, _from: usize) {}

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn bytes(&self) -> &[u8] {
        match self.addr {
            // SAFETY: `addr` starts a live mapping of at least `len` bytes, zero-filled by the
            // OS; unmapped only in `drop`, which this borrow of `self` keeps from running; no
            // `&mut` to it lives while `&self` does.
            Some(addr) => unsafe { std::slice::from_raw_parts(addr.as_ptr(), self.len) },
            None => &[],
        }
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        match self.addr {
            // SAFETY: as `bytes`, and `&mut self` makes this the mapping's only borrow.
            Some(addr) => unsafe { std::slice::from_raw_parts_mut(addr.as_ptr(), self.len) },
            None => &mut [],
        }
    }

    /// Its bytes as 64-bit words, a tail shorter than one left out.
    pub fn u64s(&self) -> &[u64] {
        match self.addr {
            // SAFETY: as `bytes`; a mapping starts on a page's boundary, so it is aligned for
            // u64, `len / 8` words lie within its `len` bytes, and every bit pattern is a u64.
            Some(addr) => unsafe {
                std::slice::from_raw_parts(addr.as_ptr().cast::<u64>(), self.len / 8)
            },
            None => &[],
        }
    }

    /// [`Self::u64s`], written.
    pub fn u64s_mut(&mut self) -> &mut [u64] {
        match self.addr {
            // SAFETY: as `u64s`, and `&mut self` makes this the mapping's only borrow.
            Some(addr) => unsafe {
                std::slice::from_raw_parts_mut(addr.as_ptr().cast::<u64>(), self.len / 8)
            },
            None => &mut [],
        }
    }

    /// Its bytes as 32-bit words, a tail shorter than one left out.
    pub fn u32s(&self) -> &[u32] {
        match self.addr {
            // SAFETY: as `u64s`, for u32: aligned, `len / 4` words within, any bit pattern.
            Some(addr) => unsafe {
                std::slice::from_raw_parts(addr.as_ptr().cast::<u32>(), self.len / 4)
            },
            None => &[],
        }
    }

    /// [`Self::u32s`], written.
    pub fn u32s_mut(&mut self) -> &mut [u32] {
        match self.addr {
            // SAFETY: as `u32s`, and `&mut self` makes this the mapping's only borrow.
            Some(addr) => unsafe {
                std::slice::from_raw_parts_mut(addr.as_ptr().cast::<u32>(), self.len / 4)
            },
            None => &mut [],
        }
    }
}

/// The size of a transparent huge page, the unit [`HugeZeroed`] populates in: the kernel's own
/// figure (`/sys/kernel/mm/transparent_hugepage/hpage_pmd_size`,
/// Documentation/admin-guide/mm/transhuge.rst), read once; where it cannot be read, what one
/// page-table page of 8-byte entries maps, which a PMD entry covers on the 64-bit architectures
/// Linux runs mantle on (x86_64 and aarch64: 2 MiB on 4 KiB pages, 32 MiB on 16 KiB, 512 MiB on
/// 64 KiB).
#[cfg(target_os = "linux")]
fn huge_page_size() -> usize {
    static SIZE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *SIZE.get_or_init(|| {
        std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/hpage_pmd_size")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or_else(|| {
                let page = page_size();
                page.saturating_mul(page.checked_div(8).unwrap_or(0))
            })
    })
}

/// The bytes a mapping of `len` takes on Linux: whole huge pages where one is at most an eighth of
/// `len`, else whole pages. A mapping that ends inside a huge page leaves that page's bytes on base
/// pages, and a table grown in place leaves one such page each growth: on the VM a 10M table's
/// gets ran 0.97x of tables built new, every round (`r5651..r5656`). Rounded to whole huge pages,
/// none is left, and an eighth stays under the third of its slots a table at its most, 2/3 full,
/// leaves empty. Where a huge page is more than an eighth, as 512 MiB ones on a 64 KiB-page kernel
/// are for all but the largest tables, the length is rounded to pages, not to a huge page the
/// table would leave mostly unused.
#[cfg(target_os = "linux")]
fn mapped_len(len: usize) -> Option<usize> {
    let (page, huge) = (page_size().max(1), huge_page_size().max(page_size()).max(1));
    let unit = if huge <= len.checked_div(8).unwrap_or(0) {
        huge
    } else {
        page
    };
    len.div_ceil(unit).checked_mul(unit)
}

/// `bytes` of private anonymous memory of protection `prot` mapped from an `align` boundary:
/// `bytes + align` mapped where the kernel chooses, then the part before the first boundary in
/// it and the part past `bytes` from there unmapped.
#[cfg(target_os = "linux")]
fn map_aligned(
    bytes: usize,
    align: usize,
    prot: rustix::mm::ProtFlags,
    flags: rustix::mm::MapFlags,
) -> Result<NonNull<u8>, ()> {
    let span = bytes.checked_add(align).ok_or(())?;
    // SAFETY: a new private anonymous mapping at an address the kernel chooses touches no memory
    // this process already uses; `span` is nonzero.
    let base = unsafe {
        rustix::mm::mmap_anonymous(
            std::ptr::null_mut(),
            span,
            prot,
            rustix::mm::MapFlags::PRIVATE | flags,
        )
    }
    .map_err(|_| ())?;
    let at = base.addr();
    let head = at
        .div_ceil(align.max(1))
        .checked_mul(align.max(1))
        .and_then(|start| start.checked_sub(at))
        .ok_or(())?;
    let tail = span.saturating_sub(head).saturating_sub(bytes);
    let start = base.cast::<u8>().wrapping_add(head);
    if head > 0 {
        // SAFETY: unmaps the first `head` bytes of the mapping just made, which nothing holds.
        let _ = unsafe { rustix::mm::munmap(base, head) };
    }
    if tail > 0 {
        // SAFETY: unmaps the mapping's last `tail` bytes, past the `bytes` from `start`, which
        // nothing holds.
        let _ = unsafe { rustix::mm::munmap(start.wrapping_add(bytes).cast(), tail) };
    }
    NonNull::new(start).ok_or(())
}

impl Drop for HugeZeroed {
    fn drop(&mut self) {
        let Some(addr) = self.addr.take() else {
            return;
        };
        #[cfg(target_os = "linux")]
        {
            // SAFETY: unmaps exactly the `mapped` bytes this value mapped, which no borrow
            // outlives (`bytes` borrows `self`). A failure leaves them mapped: nothing can act on
            // it in a drop.
            let _ = unsafe { rustix::mm::munmap(addr.as_ptr().cast(), self.mapped) };
        }
        #[cfg(not(target_os = "linux"))]
        {
            // Handed back to a `LazyZeroed` of the same words, whose drop unmaps it.
            drop(LazyZeroed {
                addr: Some(addr.cast::<AtomicU64>()),
                words: self.mapped.div_ceil(WORD),
                #[cfg(windows)]
                section: self.section,
            });
        }
    }
}

#[cfg(test)]
mod huge_tests {
    use super::*;

    #[test]
    fn a_huge_mapping_reads_zero_and_keeps_what_is_written() {
        let mut m = HugeZeroed::allocate(3 * 1024 * 1024 + 5).unwrap();
        assert_eq!(m.len(), 3 * 1024 * 1024 + 5);
        assert!(m.bytes().iter().all(|&b| b == 0));
        let n = m.len();
        m.bytes_mut()[n - 1] = 7;
        m.bytes_mut()[0] = 9;
        assert_eq!((m.bytes()[0], m.bytes()[n - 1]), (9, 7));
        assert!(HugeZeroed::allocate(0).unwrap().is_empty());
    }

    #[test]
    fn a_resized_mapping_keeps_its_bytes_and_reads_zero_past_them() {
        let byte = |i: usize| u8::try_from(i % 251).unwrap() + 1;
        let mut m = HugeZeroed::allocate(5 * 4096 + 3).unwrap();
        for (i, b) in m.bytes_mut().iter_mut().enumerate() {
            *b = byte(i);
        }
        m.resize(9 * 1024 * 1024 + 17).unwrap();
        assert_eq!(m.len(), 9 * 1024 * 1024 + 17);
        assert!(
            m.bytes()[..5 * 4096 + 3]
                .iter()
                .enumerate()
                .all(|(i, &b)| b == byte(i))
        );
        assert!(m.bytes()[5 * 4096 + 3..].iter().all(|&b| b == 0));
        // Shrunk into a page, then grown: the page's bytes past the shrunk length read zero.
        m.resize(4096 + 100).unwrap();
        assert!(m.bytes().iter().enumerate().all(|(i, &b)| b == byte(i)));
        m.resize(3 * 4096).unwrap();
        assert!(
            m.bytes()[..4096 + 100]
                .iter()
                .enumerate()
                .all(|(i, &b)| b == byte(i))
        );
        assert!(m.bytes()[4096 + 100..].iter().all(|&b| b == 0));
        m.resize(0).unwrap();
        assert!(m.is_empty());
        m.resize(10).unwrap();
        assert!(m.bytes().iter().all(|&b| b == 0));
    }

    /// The word views are the bytes in the machine's order, a tail shorter than a word left out,
    /// and what one view writes the others read.
    #[test]
    fn a_mapping_s_word_views_are_its_bytes() {
        let mut m = HugeZeroed::allocate(2 * 1024 * 1024 + 6).unwrap();
        assert_eq!(m.u64s().len(), (2 * 1024 * 1024 + 6) / 8);
        assert_eq!(m.u32s().len(), (2 * 1024 * 1024 + 6) / 4);
        assert!(m.u64s().iter().all(|&w| w == 0));
        let last = m.u64s().len() - 1;
        m.u64s_mut()[last] = 0x0102_0304_0506_0708;
        let at = last * 8;
        assert_eq!(
            m.bytes()[at..at + 8],
            0x0102_0304_0506_0708u64.to_ne_bytes()
        );
        m.u32s_mut()[1] = 0xdead_beef;
        assert_eq!(m.bytes()[4..8], 0xdead_beefu32.to_ne_bytes());
        assert!(HugeZeroed::empty().u64s().is_empty() && HugeZeroed::empty().u32s().is_empty());
    }
}
