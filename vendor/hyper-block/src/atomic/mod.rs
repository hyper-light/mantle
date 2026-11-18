//! The atomic write unit of the device under a file: the size of write a crash leaves all old or
//! all new, which hyper-log lays a persist record's pieces in (mantle docs/design/durability.md
//! §7.2, format 5). It is read from the OS, never assumed (mantle CLAUDE.md §5):
//!
//! - Linux: the queue attributes of the block device holding the file
//!   (Documentation/ABI/stable/sysfs-block), found from the file's device number at
//!   `/sys/dev/block/MAJOR:MINOR` (Documentation/ABI/stable/sysfs-dev), a block device node's by
//!   its own number, and a partition's queue being its disk's. `atomic_write_unit_min_bytes`,
//!   "the smallest block which can be written atomically with an atomic write operation", where
//!   the device takes atomic writes (non-zero, Linux 6.11 on), with `atomic_write_unit_max_bytes`,
//!   the largest; else `physical_block_size`, "the smallest unit a physical storage device can
//!   write atomically"; else `logical_block_size`. An atomic write's unit is taken only where it
//!   is no larger than the physical block, the unit of a write that does not ask for atomicity,
//!   so the answer holds for either kind of write.
//! - macOS: the "Physical Block Size" the storage device publishes in the I/O Registry, read
//!   without opening the device (`macos`); a disk node opened directly answers
//!   DKIOCGETPHYSICALBLOCKSIZE (<sys/disk.h>).
//! - Windows: `FILE_STORAGE_INFO::PhysicalBytesPerSectorForAtomicity`, "bytes per sector for
//!   atomic writes", which GetFileInformationByHandleEx gives for any handle without privilege
//!   (FILE_STORAGE_INFO, Microsoft Learn). Windows itself sets it to the logical sector when the
//!   device reports none (FILE_FS_SECTOR_SIZE_INFORMATION, Microsoft Learn), the rule below.
//! - A device the OS does not describe: Linux's own rule, block/blk-settings.c
//!   `blk_validate_limits`, "Unless otherwise specified, default to 512 byte logical blocks and a
//!   physical block size equal to the logical block size". A device that states no physical block
//!   is taken at its logical block, and one that states neither at 512 bytes
//!   ([`Alignment::SECTOR`]). A smaller unit is the conservative side: a piece no larger than the
//!   true unit, at an offset it divides, lies within one unit, and is written whole.
//!
//! A write torn within a stated unit shows only across a power cut, so the measured check of
//! this answer (mantle CLAUDE.md §5) is the power-cut tests of the log that lays pieces in it.

#[cfg(target_vendor = "apple")]
mod macos;

use std::fs::File;
#[cfg(any(target_os = "linux", target_os = "android", test))]
use std::io::Read;
use std::path::Path;
#[cfg(any(target_os = "linux", target_os = "android"))]
use std::path::PathBuf;

use crate::DiskError;
use crate::buf::Alignment;

/// The device's atomic write unit, and where it was read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AtomicUnit {
    unit: Alignment,
    source: Source,
}

/// Where an [`AtomicUnit`] was read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    /// The OS stated the device's atomic writes: Linux's `atomic_write_unit_min_bytes` is the unit.
    AtomicWrite {
        /// The largest write the device takes atomically when asked to (`RWF_ATOMIC`), Linux's
        /// `atomic_write_unit_max_bytes`.
        max_bytes: u32,
    },
    /// The OS stated the device's physical block: Linux's `physical_block_size`, macOS's
    /// "Physical Block Size", Windows' `PhysicalBytesPerSectorForAtomicity`.
    PhysicalBlock,
    /// The OS stated only the device's logical block, and the unit is that: the physical block a
    /// device states none of (`blk_validate_limits`).
    LogicalBlock,
    /// The OS described no device: [`Alignment::SECTOR`], the logical block of a device that
    /// states none (`blk_validate_limits`).
    Default,
}

impl AtomicUnit {
    /// The unit of a device the OS does not describe.
    pub const DEFAULT: Self = Self {
        unit: Alignment::SECTOR,
        source: Source::Default,
    };

    /// A unit a device states as its physical block: a simulated device's sector, which its
    /// crashes tear writes at.
    #[cfg(any(test, feature = "sim"))]
    pub(crate) fn of_sector(unit: Alignment) -> Self {
        Self {
            unit,
            source: Source::PhysicalBlock,
        }
    }

    /// The unit: a write of it, at an offset it divides, lands whole or not at all.
    pub fn unit(self) -> Alignment {
        self.unit
    }

    /// Where the unit was read.
    pub fn source(self) -> Source {
        self.source
    }

    /// Whether the OS stated the unit itself, rather than it being the conservative default of a
    /// device that states no atomic or physical unit.
    pub fn stated(self) -> bool {
        matches!(
            self.source,
            Source::AtomicWrite { .. } | Source::PhysicalBlock
        )
    }
}

/// The atomic write unit of the device holding `file`, opened at `path`, and where it was read.
/// An OS call that fails on the open file is an error; a device the OS does not describe is
/// [`AtomicUnit::DEFAULT`].
pub fn atomic_unit(file: &File, path: &Path) -> Result<AtomicUnit, DiskError> {
    query(file, path)
}

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    windows
))]
fn io_error(path: &Path, source: std::io::Error) -> DiskError {
    DiskError::Io {
        op: "atomic_unit",
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn query(file: &File, path: &Path) -> Result<AtomicUnit, DiskError> {
    match queue_of(file).map_err(|source| io_error(path, source))? {
        Some(queue) => from_queue(&queue),
        None => Ok(AtomicUnit::DEFAULT),
    }
}

/// The sysfs queue directory of the block device holding `file`; `None` for a device sysfs does
/// not list, an anonymous one (major 0: tmpfs, overlayfs, FUSE, a btrfs subvolume).
#[cfg(any(target_os = "linux", target_os = "android"))]
fn queue_of(file: &File) -> std::io::Result<Option<PathBuf>> {
    use rustix::fs::{FileType, fstat, major, minor};
    let stat = fstat(file)?;
    // A block device node is the device itself; the file system holding the node, devtmpfs, is no
    // device's (stat(2): `st_rdev` is the device a special file represents).
    let number = if FileType::from_raw_mode(stat.st_mode) == FileType::BlockDevice {
        stat.st_rdev
    } else {
        stat.st_dev
    };
    let link = PathBuf::from(format!(
        "/sys/dev/block/{}:{}",
        major(number),
        minor(number)
    ));
    let device = match std::fs::canonicalize(&link) {
        Ok(device) => device,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    // A partition has a `partition` attribute and its directory sits in its disk's, whose queue
    // it is written through (sysfs-block, /sys/block/<disk>/<partition>/partition).
    if device.join("partition").exists() {
        return Ok(device.parent().map(|disk| disk.join("queue")));
    }
    Ok(Some(device.join("queue")))
}

/// The unit the attributes of a Linux block device's sysfs `queue` directory state.
#[cfg(any(target_os = "linux", target_os = "android", test))]
fn from_queue(queue: &Path) -> Result<AtomicUnit, DiskError> {
    let atomic_min = attribute(queue, "atomic_write_unit_min_bytes")?;
    let atomic_max = attribute(queue, "atomic_write_unit_max_bytes")?;
    let physical = attribute(queue, "physical_block_size")?;
    let logical = attribute(queue, "logical_block_size")?;
    // Zero states that the device takes no atomic writes (sysfs-block, atomic_write_max_bytes).
    let atomic = match (atomic_min, atomic_max) {
        (Some(min), Some(max)) if min > 0 && max >= min => Some((min, max)),
        _ => None,
    };
    Ok(choose(atomic, physical, logical))
}

/// The bytes read of a sysfs attribute: the longest one this reads, `u32::MAX` in decimal (ten
/// digits) and the newline sysfs ends an attribute with, and one more, which only a longer
/// attribute fills.
#[cfg(any(target_os = "linux", target_os = "android", test))]
const ATTRIBUTE_READ: u64 = 12;

/// The decimal a sysfs attribute of `queue` holds; `None` where the kernel has no such attribute
/// (the atomic-write limits came in Linux 6.11).
#[cfg(any(target_os = "linux", target_os = "android", test))]
fn attribute(queue: &Path, name: &str) -> Result<Option<u32>, DiskError> {
    let path = queue.join(name);
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
        .take(ATTRIBUTE_READ)
        .read_to_string(&mut text)
        .map_err(io)?;
    let whole = u64::try_from(read).is_ok_and(|read| read < ATTRIBUTE_READ);
    match parse(&text) {
        Some(value) if whole => Ok(Some(value)),
        _ => Err(DiskError::Corrupt {
            path,
            what: "a sysfs attribute that is no decimal of 32 bits",
        }),
    }
}

/// A sysfs attribute's decimal: digits, and the newline sysfs ends an attribute with.
#[cfg(any(target_os = "linux", target_os = "android", test))]
fn parse(text: &str) -> Option<u32> {
    let digits = text.strip_suffix('\n').unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

#[cfg(target_vendor = "apple")]
fn query(file: &File, path: &Path) -> Result<AtomicUnit, DiskError> {
    let io = |source| io_error(path, source);
    if crate::node::is_node(file, path).map_err(io)? {
        let physical = crate::node::physical_block(file).map_err(io)?;
        return Ok(choose(None, Some(physical), None));
    }
    let sizes = macos::sizes(file).map_err(io)?;
    Ok(choose(None, sizes.physical, sizes.logical))
}

#[cfg(windows)]
fn query(file: &File, path: &Path) -> Result<AtomicUnit, DiskError> {
    let info = crate::node::storage_info(file).map_err(|source| io_error(path, source))?;
    // The structure's fields in the order it declares them, as GetFileInformationByHandleEx fills
    // its bytes.
    let bytes: Vec<u8> = [
        info.LogicalBytesPerSector,
        info.PhysicalBytesPerSectorForAtomicity,
    ]
    .iter()
    .flat_map(|field| field.to_le_bytes())
    .collect();
    from_storage_info(&bytes).ok_or_else(|| DiskError::Corrupt {
        path: path.to_path_buf(),
        what: "a FILE_STORAGE_INFO too short for its sector sizes",
    })
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    windows
)))]
fn query(_file: &File, _path: &Path) -> Result<AtomicUnit, DiskError> {
    Ok(AtomicUnit::DEFAULT)
}

/// The byte offset of `FILE_STORAGE_INFO::LogicalBytesPerSector`, the structure's first ULONG
/// (FILE_STORAGE_INFO structure, winbase.h, Microsoft Learn).
#[cfg(any(windows, test))]
const LOGICAL_SECTOR_AT: usize = 0;
/// The byte offset of `FILE_STORAGE_INFO::PhysicalBytesPerSectorForAtomicity`, its second ULONG:
/// four bytes, the first's size, past the start.
#[cfg(any(windows, test))]
const ATOMICITY_AT: usize = 4;

/// The unit a Windows volume states in the bytes of a `FILE_STORAGE_INFO`, little-endian as every
/// Windows target is; `None` when they are too few to hold the two sector sizes.
#[cfg(any(windows, test))]
fn from_storage_info(info: &[u8]) -> Option<AtomicUnit> {
    let ulong_at = |at: usize| {
        let field = info.get(at..)?.first_chunk()?;
        Some(u32::from_le_bytes(*field))
    };
    let logical = ulong_at(LOGICAL_SECTOR_AT)?;
    let atomicity = ulong_at(ATOMICITY_AT)?;
    Some(choose(None, Some(atomicity), Some(logical)))
}

/// The unit from what the OS stated, the first of: an atomic write's unit no larger than the
/// physical block, the physical block, the logical block, and [`AtomicUnit::DEFAULT`]. A value
/// that is no power of two within the alignment bound states nothing.
#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    windows,
    test
))]
fn choose(atomic: Option<(u32, u32)>, physical: Option<u32>, logical: Option<u32>) -> AtomicUnit {
    let physical = physical.and_then(alignment);
    if let Some(unit) = atomic.and_then(|(min, max)| atomic_write(min, max, physical)) {
        return unit;
    }
    if let Some(unit) = physical {
        return AtomicUnit {
            unit,
            source: Source::PhysicalBlock,
        };
    }
    match logical.and_then(alignment) {
        Some(unit) => AtomicUnit {
            unit,
            source: Source::LogicalBlock,
        },
        None => AtomicUnit::DEFAULT,
    }
}

/// An atomic write's unit `min`, where it is no larger than the physical block.
#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    windows,
    test
))]
fn atomic_write(min: u32, max: u32, physical: Option<Alignment>) -> Option<AtomicUnit> {
    let unit = alignment(min)?;
    physical
        .is_none_or(|physical| unit.get() <= physical.get())
        .then_some(AtomicUnit {
            unit,
            source: Source::AtomicWrite { max_bytes: max },
        })
}

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    windows,
    test
))]
fn alignment(bytes: u32) -> Option<Alignment> {
    Alignment::new(usize::try_from(bytes).ok()?).ok()
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

    fn unit(bytes: usize, source: Source) -> AtomicUnit {
        AtomicUnit {
            unit: Alignment::new(bytes).unwrap(),
            source,
        }
    }

    /// Do: read the queue of an NVMe namespace that takes atomic writes up to 16 KiB, on Linux 6.11
    /// or later. Expect: the atomic write's unit, stated, with its largest.
    #[test]
    fn a_queue_with_atomic_writes_gives_their_unit_and_largest() {
        let dir = queue(&[
            ("atomic_write_unit_min_bytes", "4096\n"),
            ("atomic_write_unit_max_bytes", "16384\n"),
            ("physical_block_size", "4096\n"),
            ("logical_block_size", "4096\n"),
        ]);
        let read = from_queue(dir.path()).unwrap();
        assert_eq!(read, unit(4096, Source::AtomicWrite { max_bytes: 16384 }));
        assert!(read.stated());
    }

    /// Do: read the queue of a 512e disk (4 KiB sectors, 512-byte logical blocks) on a kernel
    /// before 6.11, which has no atomic-write attributes, and on one after, where they read zero.
    /// Expect: the physical block, both times.
    #[test]
    fn a_queue_without_atomic_writes_gives_the_physical_block() {
        let before = queue(&[
            ("physical_block_size", "4096\n"),
            ("logical_block_size", "512\n"),
        ]);
        let after = queue(&[
            ("atomic_write_unit_min_bytes", "0\n"),
            ("atomic_write_unit_max_bytes", "0\n"),
            ("physical_block_size", "4096\n"),
            ("logical_block_size", "512\n"),
        ]);
        for dir in [before, after] {
            let read = from_queue(dir.path()).unwrap();
            assert_eq!(read, unit(4096, Source::PhysicalBlock));
            assert!(read.stated());
        }
    }

    /// Do: read a queue whose atomic-write unit is larger than its physical block. Expect: the
    /// physical block, the unit a write that does not ask for atomicity lands whole in.
    #[test]
    fn an_atomic_unit_past_the_physical_block_gives_the_physical_block() {
        let dir = queue(&[
            ("atomic_write_unit_min_bytes", "8192\n"),
            ("atomic_write_unit_max_bytes", "65536\n"),
            ("physical_block_size", "4096\n"),
            ("logical_block_size", "512\n"),
        ]);
        assert_eq!(
            from_queue(dir.path()).unwrap(),
            unit(4096, Source::PhysicalBlock)
        );
    }

    /// Do: read a queue stating only its logical block, and one stating nothing. Expect: the
    /// logical block, then 512 bytes, each the default and not stated.
    #[test]
    fn a_queue_without_a_physical_block_gives_the_logical_block_then_the_sector() {
        let logical = queue(&[("logical_block_size", "4096\n")]);
        let read = from_queue(logical.path()).unwrap();
        assert_eq!(read, unit(4096, Source::LogicalBlock));
        assert!(!read.stated());
        let nothing = queue(&[]);
        let read = from_queue(nothing.path()).unwrap();
        assert_eq!(read, AtomicUnit::DEFAULT);
        assert_eq!(read.unit().get(), 512);
        assert!(!read.stated());
    }

    /// Do: read a physical block of zero, and one that is no power of two. Expect: neither is a
    /// unit, and the logical block stands.
    #[test]
    fn a_physical_block_no_unit_can_be_falls_to_the_logical_block() {
        for physical in ["0\n", "3000\n"] {
            let dir = queue(&[
                ("physical_block_size", physical),
                ("logical_block_size", "512\n"),
            ]);
            assert_eq!(
                from_queue(dir.path()).unwrap(),
                unit(512, Source::LogicalBlock)
            );
        }
    }

    /// Do: read attributes that are no decimal, empty, past 32 bits, or longer than any decimal of
    /// 32 bits. Expect: each a typed corruption, never a guess.
    #[test]
    fn an_attribute_that_is_no_decimal_is_corrupt() {
        for text in [
            "4k\n",
            "\n",
            "",
            "4294967296\n",
            "000000000004096\n",
            "-1\n",
        ] {
            let dir = queue(&[("physical_block_size", text)]);
            let read = from_queue(dir.path());
            assert!(
                matches!(read, Err(DiskError::Corrupt { .. })),
                "{text:?} read as {read:?}"
            );
        }
    }

    /// The bytes of a FILE_STORAGE_INFO, its seven ULONGs in order.
    fn storage_info(fields: [u32; 7]) -> Vec<u8> {
        fields.iter().flat_map(|f| f.to_le_bytes()).collect()
    }

    /// Do: decode the FILE_STORAGE_INFO of a 512e disk (logical 512, atomicity, performance and
    /// the file system's atomicity 4096, both alignment flags, offsets zero) and of a 4Kn disk.
    /// Expect: the atomicity sector, stated.
    #[test]
    fn a_storage_info_gives_its_atomicity_sector() {
        let e512 = storage_info([512, 4096, 4096, 4096, 3, 0, 0]);
        let read = from_storage_info(&e512).unwrap();
        assert_eq!(read, unit(4096, Source::PhysicalBlock));
        assert!(read.stated());
        let native = storage_info([4096, 4096, 4096, 4096, 3, 0, 0]);
        assert_eq!(
            from_storage_info(&native).unwrap(),
            unit(4096, Source::PhysicalBlock)
        );
    }

    /// Do: decode a FILE_STORAGE_INFO whose atomicity sector is zero, then one whose sizes are
    /// both zero, then bytes too few for the two sizes. Expect: the logical sector, then the
    /// default, then nothing.
    #[test]
    fn a_storage_info_without_an_atomicity_sector_falls_back_and_a_short_one_is_none() {
        let no_atomicity = storage_info([512, 0, 0, 0, 0, u32::MAX, u32::MAX]);
        assert_eq!(
            from_storage_info(&no_atomicity).unwrap(),
            unit(512, Source::LogicalBlock)
        );
        let nothing = storage_info([0; 7]);
        assert_eq!(from_storage_info(&nothing).unwrap(), AtomicUnit::DEFAULT);
        let all = storage_info([512, 4096, 4096, 4096, 3, 0, 0]);
        assert_eq!(from_storage_info(all.get(..7).unwrap()), None);
    }

    /// Do: compare the offsets the decoder reads with windows-sys' FILE_STORAGE_INFO. Expect:
    /// the same, so the fixtures above are laid out as the OS lays the structure.
    #[cfg(windows)]
    #[test]
    fn the_decoders_offsets_are_the_structures() {
        use windows_sys::Win32::Storage::FileSystem::FILE_STORAGE_INFO;
        assert_eq!(
            std::mem::offset_of!(FILE_STORAGE_INFO, LogicalBytesPerSector),
            LOGICAL_SECTOR_AT
        );
        assert_eq!(
            std::mem::offset_of!(FILE_STORAGE_INFO, PhysicalBytesPerSectorForAtomicity),
            ATOMICITY_AT
        );
    }

    /// Do: ask for the atomic write unit of a file on this machine's disk. Expect: a unit no
    /// smaller than a sector, and where it came from, which the test prints: on an Apple SSD the
    /// physical block its storage device publishes, 4096 (mantle research/02 §3.4). Whether a
    /// virtual machine's disk publishes one is its own, so the source is printed, not required.
    #[test]
    #[expect(
        clippy::disallowed_methods,
        reason = "the test makes its file outside the device layer"
    )]
    fn a_file_on_this_machine_has_a_unit_and_says_where_it_came_from() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("unit");
        let file = File::create(&path).unwrap();
        let read = atomic_unit(&file, &path).unwrap();
        eprintln!(
            "{}: atomic write unit {} bytes, from {:?}",
            path.display(),
            read.unit().get(),
            read.source()
        );
        assert!(read.unit().get() >= Alignment::SECTOR.get());
    }
}
