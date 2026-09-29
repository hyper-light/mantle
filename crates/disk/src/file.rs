//! A file on a local device, opened for direct I/O where the platform and file system accept
//! it, with positional reads and writes and the platform's exact durability flush.
//!
//! Direct I/O per platform: Linux `O_DIRECT` (open(2)), macOS `F_NOCACHE` (fcntl(2)), Windows
//! `FILE_FLAG_NO_BUFFERING` (CreateFileW, "File Buffering"). A file system that refuses it is
//! opened buffered and says so in [`DeviceFile::caching`].
//!
//! Durability per platform is what `std::fs::File::sync_data` issues, verified in the std
//! source (library/std/src/sys/fs/{unix,windows}.rs): `fdatasync(2)` on Linux,
//! `fcntl(F_FULLFSYNC)` on macOS (plain fsync(2) there leaves data in the drive's volatile
//! cache, per Apple's fsync(2) man page), and `FlushFileBuffers` on Windows.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use crate::DiskError;
use crate::buf::Alignment;

/// Whether transfers bypass the OS page cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Caching {
    Direct,
    Buffered,
}

/// What the caller asks for when opening.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachingRequest {
    /// Direct I/O if the file system accepts it, otherwise buffered.
    PreferDirect,
    Buffered,
}

#[derive(Debug)]
pub struct DeviceFile {
    file: File,
    path: PathBuf,
    caching: Caching,
    align: Alignment,
}

impl DeviceFile {
    /// Opens `path` for reading and writing, creating it when `create` is set.
    ///
    /// `align` is the offset and length alignment direct transfers need on this file (the
    /// device's logical block size, or what statx(STATX_DIOALIGN) reports); it is enforced on
    /// every transfer while the file is direct.
    pub fn open(
        path: &Path,
        create: bool,
        request: CachingRequest,
        align: Alignment,
    ) -> Result<Self, DiskError> {
        let wrap = |op: &'static str| {
            let path = path.to_path_buf();
            move |source| DiskError::Io { op, path, source }
        };
        if request == CachingRequest::PreferDirect {
            match sys::open_direct(path, create) {
                Ok(file) => {
                    return Ok(Self {
                        file,
                        path: path.to_path_buf(),
                        caching: Caching::Direct,
                        align,
                    });
                }
                Err(e) if sys::refuses_direct(&e) => {}
                Err(e) => return Err(wrap("open")(e)),
            }
        }
        let file = options(create).open(path).map_err(wrap("open"))?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
            caching: Caching::Buffered,
            align: Alignment::BYTE,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn caching(&self) -> Caching {
        self.caching
    }

    /// The alignment every transfer's offset and length must meet (one byte when buffered).
    pub fn alignment(&self) -> Alignment {
        self.align
    }

    /// Writes all of `buf` at `offset`.
    pub fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.check_alignment(offset, buf.len())?;
        self.check_address(buf.as_ptr().addr(), offset, buf.len())?;
        let mut done = 0usize;
        while let Some(rest) = buf.get(done..).filter(|r| !r.is_empty()) {
            let at = offset
                .checked_add(u64::try_from(done).map_err(|_| self.overflow(offset))?)
                .ok_or_else(|| self.overflow(offset))?;
            match sys::write_at(&self.file, rest, at) {
                Ok(0) => {
                    return Err(self.io_error("write", io::Error::from(io::ErrorKind::WriteZero)));
                }
                Ok(n) => done = done.saturating_add(n),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(self.io_error("write", e)),
            }
        }
        Ok(())
    }

    /// Fills `buf` from `offset`; reaching the end of the file first is an error.
    pub fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        let filled = self.read_at(buf, offset)?;
        if filled < buf.len() {
            return Err(DiskError::ShortRead {
                path: self.path.clone(),
                offset,
                missing: buf.len().saturating_sub(filled),
            });
        }
        Ok(())
    }

    /// Reads into `buf` from `offset` until it is full or the file ends; returns the count.
    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize, DiskError> {
        self.check_alignment(offset, buf.len())?;
        self.check_address(buf.as_ptr().addr(), offset, buf.len())?;
        let mut done = 0usize;
        while let Some(rest) = buf.get_mut(done..).filter(|r| !r.is_empty()) {
            let at = offset
                .checked_add(u64::try_from(done).map_err(|_| self.overflow(offset))?)
                .ok_or_else(|| self.overflow(offset))?;
            match sys::read_at(&self.file, rest, at) {
                Ok(0) => break,
                Ok(n) => done = done.saturating_add(n),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(self.io_error("read", e)),
            }
        }
        Ok(done)
    }

    /// Makes every completed write durable, including the device's volatile cache.
    pub fn sync_data(&self) -> Result<(), DiskError> {
        self.file
            .sync_data()
            .map_err(|e| self.io_error("sync_data", e))
    }

    /// Reserves space so the file is `len` bytes long with its blocks allocated. Only for a
    /// file that is still empty (see `sys::preallocate`).
    pub fn preallocate(&self, len: u64) -> Result<(), DiskError> {
        sys::preallocate(&self.file, len).map_err(|e| self.io_error("preallocate", e))
    }

    pub fn len(&self) -> Result<u64, DiskError> {
        self.file
            .metadata()
            .map(|m| m.len())
            .map_err(|e| self.io_error("stat", e))
    }

    pub fn is_empty(&self) -> Result<bool, DiskError> {
        self.len().map(|len| len == 0)
    }

    pub fn std_file(&self) -> &File {
        &self.file
    }

    fn check_alignment(&self, offset: u64, len: usize) -> Result<(), DiskError> {
        if self.align.is_aligned_u64(offset) && self.align.is_aligned(len) {
            return Ok(());
        }
        Err(DiskError::Misaligned {
            offset,
            len,
            align: self.align.get(),
        })
    }

    /// Direct transfers move straight between the device and the buffer, so its address
    /// must be aligned as well (open(2) NOTES; Windows "File Buffering").
    fn check_address(&self, addr: usize, offset: u64, len: usize) -> Result<(), DiskError> {
        if self.align.is_aligned(addr) {
            return Ok(());
        }
        Err(DiskError::Misaligned {
            offset,
            len,
            align: self.align.get(),
        })
    }

    fn io_error(&self, op: &'static str, source: io::Error) -> DiskError {
        DiskError::Io {
            op,
            path: self.path.clone(),
            source,
        }
    }

    fn overflow(&self, offset: u64) -> DiskError {
        self.io_error(
            "offset",
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("transfer at {offset} passes the largest file offset"),
            ),
        )
    }
}

/// Flushes a directory so entries created or renamed in it survive a crash (Pillai et al.,
/// OSDI 2014: a new file's directory entry is durable only after its directory is synced).
pub fn sync_dir(dir: &Path) -> Result<(), DiskError> {
    sys::sync_dir(dir).map_err(|source| DiskError::Io {
        op: "sync_dir",
        path: dir.to_path_buf(),
        source,
    })
}

fn options(create: bool) -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(create);
    options
}

#[cfg(unix)]
mod sys {
    use std::fs::File;
    use std::io;
    use std::os::unix::fs::FileExt;
    use std::path::Path;

    pub fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
        file.write_at(buf, offset)
    }

    pub fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        file.read_at(buf, offset)
    }

    pub fn sync_dir(dir: &Path) -> io::Result<()> {
        File::open(dir)?.sync_all()
    }

    pub fn preallocate(file: &File, len: u64) -> io::Result<()> {
        // Linux: fallocate(2) mode 0 allocates and extends. macOS: rustix issues
        // F_PREALLOCATE from the physical end of file, then ftruncate(2) — correct only
        // for an empty file, which is the one use.
        rustix::fs::fallocate(file, rustix::fs::FallocateFlags::empty(), 0, len)
            .map_err(io::Error::from)
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn open_direct(path: &Path, create: bool) -> io::Result<File> {
        use std::os::unix::fs::OpenOptionsExt;
        let flag = i32::try_from(rustix::fs::OFlags::DIRECT.bits())
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        let mut options = super::options(create);
        options.custom_flags(flag);
        options.open(path)
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn refuses_direct(e: &io::Error) -> bool {
        // open(2): EINVAL "The filesystem does not support the O_DIRECT flag".
        e.raw_os_error() == Some(rustix::io::Errno::INVAL.raw_os_error())
    }

    #[cfg(target_vendor = "apple")]
    pub fn open_direct(path: &Path, create: bool) -> io::Result<File> {
        let file = super::options(create).open(path)?;
        rustix::fs::fcntl_nocache(&file, true)?;
        Ok(file)
    }

    #[cfg(target_vendor = "apple")]
    pub fn refuses_direct(e: &io::Error) -> bool {
        matches!(
            e.raw_os_error(),
            Some(code) if code == rustix::io::Errno::INVAL.raw_os_error()
                || code == rustix::io::Errno::NOTSUP.raw_os_error()
        )
    }

    #[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
    pub fn open_direct(_path: &Path, _create: bool) -> io::Result<File> {
        Err(io::Error::from(io::ErrorKind::Unsupported))
    }

    #[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
    pub fn refuses_direct(e: &io::Error) -> bool {
        e.kind() == io::ErrorKind::Unsupported
    }
}

#[cfg(windows)]
mod sys {
    use std::fs::File;
    use std::io;
    use std::os::windows::fs::{FileExt, OpenOptionsExt};
    use std::path::Path;

    use windows_sys::Win32::Foundation::ERROR_INVALID_PARAMETER;
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_NO_BUFFERING;

    pub fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
        file.seek_write(buf, offset)
    }

    pub fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        // std maps ReadFile's ERROR_HANDLE_EOF to Ok(0) (library/std/src/sys/pal/windows/handle.rs).
        file.seek_read(buf, offset)
    }

    pub fn sync_dir(dir: &Path) -> io::Result<()> {
        // A directory opens only with FILE_FLAG_BACKUP_SEMANTICS (CreateFileW); FlushFileBuffers
        // needs a handle with write access.
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(dir)?
            .sync_all()
    }

    pub fn preallocate(file: &File, len: u64) -> io::Result<()> {
        // SetEndOfFile allocates the clusters of a non-sparse NTFS file; the valid data
        // length stays at zero, so reads past what was written return zeros.
        file.set_len(len)
    }

    pub fn open_direct(path: &Path, create: bool) -> io::Result<File> {
        let mut options = super::options(create);
        options.custom_flags(FILE_FLAG_NO_BUFFERING);
        options.open(path)
    }

    pub fn refuses_direct(e: &io::Error) -> bool {
        e.raw_os_error() == i32::try_from(ERROR_INVALID_PARAMETER).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buf::AlignedBuf;

    fn align() -> Alignment {
        Alignment::new(4096).unwrap()
    }

    #[test]
    fn direct_round_trip_is_durable_and_exact() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("volume");
        let file = DeviceFile::open(&path, true, CachingRequest::PreferDirect, align()).unwrap();
        file.preallocate(1 << 20).unwrap();
        assert_eq!(file.len().unwrap(), 1 << 20);

        let mut out = AlignedBuf::zeroed(8192, align()).unwrap();
        let pattern: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
        out.extend_from_slice(&pattern).unwrap();
        file.write_all_at(out.as_slice(), 4096).unwrap();
        file.sync_data().unwrap();
        sync_dir(dir.path()).unwrap();

        let mut back = AlignedBuf::zeroed(8192, align()).unwrap();
        file.read_exact_at(back.as_mut_capacity(), 4096).unwrap();
        back.set_len(8192).unwrap();
        assert_eq!(back.as_slice(), pattern.as_slice());
    }

    #[test]
    fn direct_transfers_must_be_aligned() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("volume");
        let file = DeviceFile::open(&path, true, CachingRequest::PreferDirect, align()).unwrap();
        if file.caching() == Caching::Buffered {
            return;
        }
        let buf = AlignedBuf::zeroed(4096, align()).unwrap();
        let err = file.write_all_at(&buf.as_slice()[..0], 100).unwrap_err();
        assert!(matches!(err, DiskError::Misaligned { offset: 100, .. }));
        let mut small = [0u8; 100];
        let err = file.read_at(&mut small, 0).unwrap_err();
        assert!(matches!(err, DiskError::Misaligned { len: 100, .. }));
    }

    #[test]
    fn buffered_accepts_any_offset_and_reports_short_reads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        let file = DeviceFile::open(&path, true, CachingRequest::Buffered, align()).unwrap();
        assert_eq!(file.caching(), Caching::Buffered);
        file.write_all_at(b"hello", 3).unwrap();
        let mut buf = [0u8; 8];
        file.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"\0\0\0hello");
        let err = file.read_exact_at(&mut buf, 4).unwrap_err();
        assert!(matches!(err, DiskError::ShortRead { missing: 4, .. }));
    }

    #[test]
    fn opening_a_missing_file_without_create_fails_with_its_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent");
        let err =
            DeviceFile::open(&path, false, CachingRequest::PreferDirect, align()).unwrap_err();
        match err {
            DiskError::Io {
                op,
                path: p,
                source,
            } => {
                assert_eq!(op, "open");
                assert_eq!(p, path);
                assert_eq!(source.kind(), io::ErrorKind::NotFound);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
