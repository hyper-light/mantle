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
/// read at random over gigabytes: on 4 KiB pages its probes walk the page tables, which cost a 10M
/// get the read a one-read layout saves (research/63 §4). Released when dropped.
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
            #[cfg(windows)]
            section: std::ptr::null_mut(),
        }
    }

    /// Maps `len` zeroed bytes, or refuses with the OS's error.
    pub fn allocate(len: usize) -> Result<Self, Error> {
        let refused = |_| Error::LimitExceeded {
            what: "a kept record table (the OS refused the mapping)",
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
        let mut mapped = Self {
            addr,
            len,
            #[cfg(windows)]
            section,
        };
        mapped.fault_in();
        Ok(mapped)
    }

    /// Asks Linux for huge pages and faults every page in for writing, a huge page at a time (the
    /// type's docs).
    #[cfg(target_os = "linux")]
    fn fault_in(&mut self) {
        let Some(addr) = self.addr else {
            return;
        };
        // SAFETY: advises the whole mapping this value owns; the advice changes how the kernel
        // backs it, never its contents. Refused, the pages are the base size.
        let _ = unsafe {
            rustix::mm::madvise(
                addr.as_ptr().cast(),
                self.len,
                rustix::mm::Advice::LinuxHugepage,
            )
        };
        let step = huge_page_size().max(page_size()).max(1);
        let mut done = 0usize;
        while done < self.len {
            let n = step.min(self.len.saturating_sub(done));
            // SAFETY: `done` is below `len`, so the range starts inside the mapping this value
            // owns and its `n` bytes end inside it; `done` is a multiple of `step`, a whole number
            // of pages, so the start is page aligned as madvise(2) asks. Populating for writing
            // leaves the zeros the mapping already reads as.
            let populated = unsafe {
                rustix::mm::madvise(
                    addr.as_ptr().add(done).cast(),
                    n,
                    rustix::mm::Advice::LinuxPopulateWrite,
                )
            };
            if populated.is_err() {
                // A kernel before 5.14: each page left written once, a write fault each.
                let page = page_size().max(1);
                if let Some(rest) = self.bytes_mut().get_mut(done..) {
                    for b in rest.iter_mut().step_by(page) {
                        *b = 0;
                    }
                }
                return;
            }
            done = done.saturating_add(n);
        }
    }

    /// Elsewhere pages fault in as they are first touched, at the OS's page size.
    #[cfg(not(target_os = "linux"))]
    fn fault_in(&mut self) {}

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

impl Drop for HugeZeroed {
    fn drop(&mut self) {
        let Some(addr) = self.addr.take() else {
            return;
        };
        // Handed back to a `LazyZeroed` of the same words, whose drop unmaps it.
        drop(LazyZeroed {
            addr: Some(addr.cast::<AtomicU64>()),
            words: self.len.div_ceil(WORD),
            #[cfg(windows)]
            section: self.section,
        });
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
}
