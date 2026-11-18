//! The write cache of the device under a file: whether a write the device has completed can still
//! be lost to a power cut before a flush, which decides where the log may answer a frame at its
//! own flush (mantle docs/design/durability.md §7.7). A device with a volatile write-back cache
//! "signal[s] I/O completion to the operating system before data actually has hit the
//! non-volatile storage" (Linux Documentation/block/writeback_cache_control.rst); one with no
//! volatile cache does not. It is read from the OS, never assumed (mantle CLAUDE.md §5):
//!
//! - Linux: the `write_cache` attribute of the queue of the block device holding the file, found
//!   as [`crate::atomic::atomic_unit`] finds it: "write back" where the device has write back
//!   caching enabled, "write through" where not (Documentation/ABI/stable/sysfs-block), the
//!   kernel's view, which also decides whether it sends the device flushes at all
//!   (writeback_cache_control.rst: a queue without `BLK_FEAT_WRITE_CACHE` has its flushes
//!   completed before they reach the driver). Beside it `fua`, whether the driver takes Force
//!   Unit Access, a write that bypasses the volatile cache (the same ABI file).
//! - Windows: `IOCTL_STORAGE_QUERY_PROPERTY` for `StorageDeviceWriteCacheProperty`, whose
//!   `STORAGE_WRITE_CACHE_PROPERTY` states the cache's type (none, write-back, write-through),
//!   whether it is enabled, and `NVCacheEnabled`, "whether the device has a battery backup for
//!   the write cache" (STORAGE_WRITE_CACHE_PROPERTY, Microsoft Learn), asked of the file's volume
//!   with no access rights (`crate::node`).
//! - macOS: the block storage device's "WriteCacheState", true where its write cache is enabled
//!   (IOBlockStorageDevice.h), read from the I/O Registry without opening the device
//!   (`crate::atomic::macos`). Where the device publishes none, macOS's own rule stands: a drive's
//!   cache holds written data until F_FULLFSYNC "asks the drive to flush all buffered data to the
//!   permanent storage device" (fcntl(2)), so the cache is write-back.
//! - A device the OS does not describe: write-back, the conservative side. A completed write is
//!   durable only once flushed, which holds on every device; answering earlier is safe only where
//!   the OS says the cache is not volatile.
//!
//! A battery- or capacitor-backed cache counts as write-through here: what makes a completed
//! write losable is the cache's volatility, not its existence (writeback_cache_control.rst), and
//! a cache with a battery backup keeps what it holds across the power cut.

use std::fs::File;
#[cfg(any(target_os = "linux", target_os = "android", test))]
use std::io::Read;
use std::path::Path;

use crate::DiskError;

/// Whether a write the device has completed can be lost to a power cut before a flush.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteCache {
    /// No volatile cache: the device has none, writes through it, or has it disabled, so a
    /// completed write is on stable media.
    WriteThrough,
    /// A write-back cache the device states has a battery backup, which keeps a completed write
    /// across a power cut.
    PowerProtected,
    /// A volatile write-back cache: a completed write is lost to a power cut until it is flushed.
    WriteBack,
}

/// Where a [`DeviceCache`] was read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    /// Linux's sysfs queue attribute `write_cache`, with `fua`: whether the driver takes Force
    /// Unit Access writes, `None` on a kernel without the attribute.
    Sysfs {
        /// The queue's `fua`.
        fua: Option<bool>,
    },
    /// Windows' STORAGE_WRITE_CACHE_PROPERTY.
    StorageProperty,
    /// macOS's "WriteCacheState" in the I/O Registry.
    Registry,
    /// macOS published no state for the device, and fcntl(2)'s F_FULLFSYNC rule stands: write-back.
    FullFsync,
    /// The OS described no device: write-back, the conservative default.
    Default,
    /// A simulated device states its own cache, as its model holds it (`crate::sim::SimFile`).
    Simulated,
}

/// The device's write cache, and where it was read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceCache {
    cache: WriteCache,
    source: Source,
}

impl DeviceCache {
    /// The cache of a device the OS does not describe.
    pub const DEFAULT: Self = Self {
        cache: WriteCache::WriteBack,
        source: Source::Default,
    };

    /// The cache `source` states: a device a wrapper or a simulation describes for itself.
    pub const fn new(cache: WriteCache, source: Source) -> Self {
        Self { cache, source }
    }

    /// The cache.
    pub fn cache(self) -> WriteCache {
        self.cache
    }

    /// Where the cache was read.
    pub fn source(self) -> Source {
        self.source
    }

    /// Whether the OS stated the device's cache, rather than it being a rule for a device that
    /// states none; a simulated device states its own.
    pub fn stated(self) -> bool {
        matches!(
            self.source,
            Source::Sysfs { .. } | Source::StorageProperty | Source::Registry | Source::Simulated
        )
    }
}

/// The write cache of the device holding `file`, opened at `path`, and where it was read. An OS
/// call that fails on the open file is an error; a device the OS does not describe is
/// [`DeviceCache::DEFAULT`].
pub fn write_cache(file: &File, path: &Path) -> Result<DeviceCache, DiskError> {
    query(file, path)
}

#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
fn io_error(path: &Path, source: std::io::Error) -> DiskError {
    DiskError::Io {
        op: "write_cache",
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn query(file: &File, path: &Path) -> Result<DeviceCache, DiskError> {
    match crate::atomic::queue_of(file).map_err(|source| io_error(path, source))? {
        Some(queue) => from_queue(&queue),
        None => Ok(DeviceCache::DEFAULT),
    }
}

/// The cache the attributes of a Linux block device's sysfs `queue` directory state.
#[cfg(any(target_os = "linux", target_os = "android", test))]
fn from_queue(queue: &Path) -> Result<DeviceCache, DiskError> {
    let Some(cache) = cache_attribute(queue)? else {
        return Ok(DeviceCache::DEFAULT);
    };
    let fua = match crate::atomic::attribute(queue, "fua")? {
        None => None,
        Some(0) => Some(false),
        Some(1) => Some(true),
        Some(_) => {
            return Err(DiskError::Corrupt {
                path: queue.join("fua"),
                what: "a sysfs fua that is neither 0 nor 1",
            });
        }
    };
    Ok(DeviceCache {
        cache,
        source: Source::Sysfs { fua },
    })
}

/// The bytes read of `write_cache`: its longer value, "write through" and the newline sysfs ends
/// it with (block/blk-sysfs.c, `queue_wc_show`), fourteen bytes, and one more, which only a
/// longer value fills.
#[cfg(any(target_os = "linux", target_os = "android", test))]
const CACHE_READ: u64 = 15;

/// The queue's `write_cache`; `None` where the kernel has no such attribute (before Linux 4.7).
#[cfg(any(target_os = "linux", target_os = "android", test))]
fn cache_attribute(queue: &Path) -> Result<Option<WriteCache>, DiskError> {
    let path = queue.join("write_cache");
    let io = |source| DiskError::Io {
        op: "read a sysfs attribute",
        path: path.clone(),
        source,
    };
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io(e)),
    };
    let mut text = String::new();
    let read = file
        .take(CACHE_READ)
        .read_to_string(&mut text)
        .map_err(io)?;
    let whole = u64::try_from(read).is_ok_and(|read| read < CACHE_READ);
    match text.strip_suffix('\n').unwrap_or(&text) {
        "write back" if whole => Ok(Some(WriteCache::WriteBack)),
        "write through" if whole => Ok(Some(WriteCache::WriteThrough)),
        _ => Err(DiskError::Corrupt {
            path,
            what: "a sysfs write_cache that is neither \"write back\" nor \"write through\"",
        }),
    }
}

#[cfg(target_vendor = "apple")]
fn query(file: &File, path: &Path) -> Result<DeviceCache, DiskError> {
    let enabled =
        crate::atomic::macos::write_cache(file).map_err(|source| io_error(path, source))?;
    Ok(from_registry(enabled))
}

/// The cache a macOS block storage device's "WriteCacheState" states: enabled is write-back,
/// disabled write-through; none published leaves F_FULLFSYNC's rule, write-back.
#[cfg(any(target_vendor = "apple", test))]
fn from_registry(enabled: Option<bool>) -> DeviceCache {
    let (cache, source) = match enabled {
        Some(true) => (WriteCache::WriteBack, Source::Registry),
        Some(false) => (WriteCache::WriteThrough, Source::Registry),
        None => (WriteCache::WriteBack, Source::FullFsync),
    };
    DeviceCache { cache, source }
}

#[cfg(windows)]
fn query(file: &File, path: &Path) -> Result<DeviceCache, DiskError> {
    let node = crate::node::is_node(file, path).map_err(|source| DiskError::Io {
        op: "write_cache",
        path: path.to_path_buf(),
        source,
    })?;
    Ok(crate::node::write_cache_property(file, node)
        .map_or(DeviceCache::DEFAULT, |bytes| from_property(&bytes)))
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    windows
)))]
fn query(_file: &File, _path: &Path) -> Result<DeviceCache, DiskError> {
    Ok(DeviceCache::DEFAULT)
}

/// The byte offset of `STORAGE_WRITE_CACHE_PROPERTY::WriteCacheType`, past the two DWORDs
/// `Version` and `Size` (STORAGE_WRITE_CACHE_PROPERTY, winioctl.h, Microsoft Learn).
#[cfg(any(windows, test))]
const CACHE_TYPE_AT: usize = 8;
/// The byte offset of `WriteCacheEnabled`, the enumeration after `WriteCacheType`, four bytes.
#[cfg(any(windows, test))]
const CACHE_ENABLED_AT: usize = 12;
/// The byte offset of `NVCacheEnabled`: past four enumerations of four bytes from
/// `WriteCacheType`, then the BOOLEANs `FlushCacheSupported` and `UserDefinedPowerProtection`,
/// one byte each.
#[cfg(any(windows, test))]
const NV_CACHE_AT: usize = 26;
/// `WriteCacheTypeNone`: "The device does not have a write cache" (WRITE_CACHE_TYPE, winioctl.h).
#[cfg(any(windows, test))]
const TYPE_NONE: i32 = 1;
/// `WriteCacheTypeWriteBack`: "The device has a write-back cache" (WRITE_CACHE_TYPE).
#[cfg(any(windows, test))]
const TYPE_WRITE_BACK: i32 = 2;
/// `WriteCacheTypeWriteThrough`: "The device has a write-through cache" (WRITE_CACHE_TYPE).
#[cfg(any(windows, test))]
const TYPE_WRITE_THROUGH: i32 = 3;
/// `WriteCacheDisabled`: "The device's write cache is disabled" (WRITE_CACHE_ENABLE, winioctl.h).
#[cfg(any(windows, test))]
const CACHE_DISABLED: i32 = 1;
/// `TRUE` of a BOOLEAN (winnt.h).
#[cfg(any(windows, test))]
const TRUE: u8 = 1;

/// The cache a Windows device states in the bytes of a `STORAGE_WRITE_CACHE_PROPERTY`,
/// little-endian as every Windows target is: none or write-through, or a write-back cache that is
/// disabled, is write-through; an enabled write-back cache with a battery backup is
/// power-protected, and without one write-back. A type the system cannot report, or bytes too few
/// for the fields, describe nothing: [`DeviceCache::DEFAULT`].
#[cfg(any(windows, test))]
fn from_property(bytes: &[u8]) -> DeviceCache {
    let enum_at = |at: usize| {
        let field = bytes.get(at..)?.first_chunk()?;
        Some(i32::from_le_bytes(*field))
    };
    let (Some(kind), Some(enabled), Some(&battery)) = (
        enum_at(CACHE_TYPE_AT),
        enum_at(CACHE_ENABLED_AT),
        bytes.get(NV_CACHE_AT),
    ) else {
        return DeviceCache::DEFAULT;
    };
    let cache = match kind {
        TYPE_NONE | TYPE_WRITE_THROUGH => WriteCache::WriteThrough,
        TYPE_WRITE_BACK if enabled == CACHE_DISABLED => WriteCache::WriteThrough,
        TYPE_WRITE_BACK if battery == TRUE => WriteCache::PowerProtected,
        TYPE_WRITE_BACK => WriteCache::WriteBack,
        _ => return DeviceCache::DEFAULT,
    };
    DeviceCache {
        cache,
        source: Source::StorageProperty,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A sysfs queue directory holding `attributes`, each written as sysfs prints it.
    #[expect(
        clippy::disallowed_methods,
        reason = "the test writes its fixtures outside the device layer"
    )]
    fn queue(attributes: &[(&str, &str)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for (name, text) in attributes {
            std::fs::write(dir.path().join(name), text).unwrap();
        }
        dir
    }

    /// Do: read the queue of a device with write back caching and FUA (a consumer NVMe drive),
    /// and of one without a volatile cache (a drive with power-loss protection, or one whose cache
    /// is off). Expect: write-back and write-through, each stated by sysfs with its FUA.
    #[test]
    fn a_queue_states_write_back_or_write_through_with_its_fua() {
        let back = queue(&[("write_cache", "write back\n"), ("fua", "1\n")]);
        let read = from_queue(back.path()).unwrap();
        assert_eq!(read.cache(), WriteCache::WriteBack);
        assert_eq!(read.source(), Source::Sysfs { fua: Some(true) });
        assert!(read.stated());
        let through = queue(&[("write_cache", "write through\n"), ("fua", "0\n")]);
        let read = from_queue(through.path()).unwrap();
        assert_eq!(read.cache(), WriteCache::WriteThrough);
        assert_eq!(read.source(), Source::Sysfs { fua: Some(false) });
        assert!(read.stated());
    }

    /// Do: read a queue with `write_cache` and no `fua` (a kernel before 4.18), and one with
    /// neither (a kernel before 4.7). Expect: the cache without its FUA, then the default,
    /// write-back and not stated.
    #[test]
    fn a_queue_without_its_attributes_gives_what_it_states_then_the_default() {
        let no_fua = queue(&[("write_cache", "write through\n")]);
        let read = from_queue(no_fua.path()).unwrap();
        assert_eq!(read.cache(), WriteCache::WriteThrough);
        assert_eq!(read.source(), Source::Sysfs { fua: None });
        let nothing = queue(&[]);
        let read = from_queue(nothing.path()).unwrap();
        assert_eq!(read, DeviceCache::DEFAULT);
        assert_eq!(read.cache(), WriteCache::WriteBack);
        assert!(!read.stated());
    }

    /// Do: read `write_cache` values sysfs never prints (another word, empty, without its space,
    /// past the longest value) and a `fua` that is no flag. Expect: each a typed corruption,
    /// never a guess.
    #[test]
    fn a_write_cache_or_fua_sysfs_never_prints_is_corrupt() {
        for text in [
            "write around\n",
            "\n",
            "",
            "writeback\n",
            "write through\n\n",
            "write through and more\n",
        ] {
            let dir = queue(&[("write_cache", text)]);
            let read = from_queue(dir.path());
            assert!(
                matches!(read, Err(DiskError::Corrupt { .. })),
                "{text:?} read as {read:?}"
            );
        }
        let dir = queue(&[("write_cache", "write back\n"), ("fua", "2\n")]);
        assert!(matches!(
            from_queue(dir.path()),
            Err(DiskError::Corrupt { .. })
        ));
    }

    /// Do: decode the registry's "WriteCacheState" enabled, disabled and absent. Expect:
    /// write-back and write-through stated by the registry, then F_FULLFSYNC's rule, write-back,
    /// not stated.
    #[test]
    fn the_registrys_write_cache_state_or_its_absence_gives_the_cache() {
        let enabled = from_registry(Some(true));
        assert_eq!(
            (enabled.cache(), enabled.source(), enabled.stated()),
            (WriteCache::WriteBack, Source::Registry, true)
        );
        let disabled = from_registry(Some(false));
        assert_eq!(
            (disabled.cache(), disabled.source(), disabled.stated()),
            (WriteCache::WriteThrough, Source::Registry, true)
        );
        let none = from_registry(None);
        assert_eq!(
            (none.cache(), none.source(), none.stated()),
            (WriteCache::WriteBack, Source::FullFsync, false)
        );
    }

    /// The bytes of a STORAGE_WRITE_CACHE_PROPERTY: `Version` and `Size` (its 28 bytes, padded
    /// to its DWORDs), the four enumerations, then the three BOOLEANs.
    fn property(enums: [i32; 4], booleans: [u8; 3]) -> Vec<u8> {
        let mut bytes: Vec<u8> = [28i32, 28]
            .iter()
            .chain(enums.iter())
            .flat_map(|field| field.to_le_bytes())
            .collect();
        bytes.extend_from_slice(&booleans);
        bytes.push(0);
        bytes
    }

    /// Do: decode the property of a drive with no cache, one with a write-through cache, one with
    /// a write-back cache disabled, one enabled, and one enabled with a battery backup
    /// (`NVCacheEnabled`). Expect: write-through three times, then write-back, then
    /// power-protected, each stated.
    #[test]
    fn a_write_cache_property_gives_its_cache() {
        let cases = [
            (property([1, 0, 0, 0], [0, 0, 0]), WriteCache::WriteThrough),
            (property([3, 2, 1, 2], [1, 0, 0]), WriteCache::WriteThrough),
            (property([2, 1, 2, 2], [1, 0, 0]), WriteCache::WriteThrough),
            (property([2, 2, 2, 2], [1, 0, 0]), WriteCache::WriteBack),
            (
                property([2, 2, 1, 1], [1, 1, 1]),
                WriteCache::PowerProtected,
            ),
        ];
        for (bytes, cache) in cases {
            let read = from_property(&bytes);
            assert_eq!(read.cache(), cache, "{bytes:?}");
            assert_eq!(read.source(), Source::StorageProperty);
            assert!(read.stated());
        }
    }

    /// Do: decode a property whose type the system cannot report, whatever else it says, and
    /// bytes too few to hold `NVCacheEnabled`. Expect: the default, write-back, not stated; a
    /// battery byte other than TRUE states no backup.
    #[test]
    fn a_write_cache_property_that_reports_no_type_or_is_short_is_the_default() {
        for unknown in [
            property([0, 1, 0, 0], [0, 0, 0]),
            property([0, 2, 0, 0], [1, 0, 1]),
            property([4, 2, 0, 0], [1, 0, 1]),
        ] {
            assert_eq!(from_property(&unknown), DeviceCache::DEFAULT);
        }
        let whole = property([2, 2, 2, 2], [1, 1, 1]);
        assert_eq!(
            from_property(whole.get(..NV_CACHE_AT).unwrap()),
            DeviceCache::DEFAULT
        );
        let odd = property([2, 2, 2, 2], [1, 1, 2]);
        assert_eq!(from_property(&odd).cache(), WriteCache::WriteBack);
    }

    /// Do: compare the offsets and values the decoder reads with windows-sys'
    /// STORAGE_WRITE_CACHE_PROPERTY and its enumerations. Expect: the same, so the fixtures above
    /// are laid out as the OS lays the structure.
    #[cfg(windows)]
    #[test]
    fn the_decoders_offsets_and_values_are_the_structures() {
        use windows_sys::Win32::System::Ioctl::{
            STORAGE_WRITE_CACHE_PROPERTY, WriteCacheDisabled, WriteCacheTypeNone,
            WriteCacheTypeWriteBack, WriteCacheTypeWriteThrough,
        };
        assert_eq!(
            std::mem::offset_of!(STORAGE_WRITE_CACHE_PROPERTY, WriteCacheType),
            CACHE_TYPE_AT
        );
        assert_eq!(
            std::mem::offset_of!(STORAGE_WRITE_CACHE_PROPERTY, WriteCacheEnabled),
            CACHE_ENABLED_AT
        );
        assert_eq!(
            std::mem::offset_of!(STORAGE_WRITE_CACHE_PROPERTY, NVCacheEnabled),
            NV_CACHE_AT
        );
        assert_eq!(std::mem::size_of::<STORAGE_WRITE_CACHE_PROPERTY>(), 28);
        assert_eq!(
            (
                TYPE_NONE,
                TYPE_WRITE_BACK,
                TYPE_WRITE_THROUGH,
                CACHE_DISABLED
            ),
            (
                WriteCacheTypeNone,
                WriteCacheTypeWriteBack,
                WriteCacheTypeWriteThrough,
                WriteCacheDisabled
            )
        );
    }

    /// Do: ask for the write cache of a file on this machine's disk. Expect: an answer, and where
    /// it came from, which the test prints. Whether a machine's device states its cache is its
    /// own, so the source is printed, not required.
    #[test]
    #[expect(
        clippy::disallowed_methods,
        reason = "the test makes its file outside the device layer"
    )]
    fn a_file_on_this_machine_has_a_write_cache_and_says_where_it_came_from() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache");
        let file = File::create(&path).unwrap();
        let read = write_cache(&file, &path).unwrap();
        eprintln!(
            "{}: write cache {:?}, from {:?}, stated {}",
            path.display(),
            read.cache(),
            read.source(),
            read.stated()
        );
        if !read.stated() {
            assert_eq!(read.cache(), WriteCache::WriteBack);
        }
    }
}
