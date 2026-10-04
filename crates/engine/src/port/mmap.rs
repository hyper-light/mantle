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
