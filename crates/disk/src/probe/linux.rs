//! Linux: the block device under a path, from sysfs.
//!
//! A path's file system reports its device number (stat(2) `st_dev`); sysfs exposes that
//! device at `/sys/dev/block/MAJOR:MINOR` (Documentation/ABI/stable/sysfs-dev). A partition
//! has a `partition` attribute and lives under its disk's directory; a device-mapper or md
//! device lists its members under `slaves/`. Queue attributes are those of
//! Documentation/ABI/stable/sysfs-block. File system types are the statfs(2) `f_type` magic
//! numbers of include/uapi/linux/magic.h.

use std::path::{Path, PathBuf};

use crate::identity::{
    FileSystem, FileSystemKind, Identity, Interconnect, Medium, WriteCache, Zoned,
};

/// Composite devices nest (dm-crypt over LVM over md); the walk stops past this depth.
const MAX_DEPTH: usize = 8;
/// Members described per composite device.
const MAX_MEMBERS: usize = 64;

pub fn identify(path: &Path) -> Identity {
    let file_system = file_system(path);
    let mut identity = Identity::unknown(file_system.clone());
    match &file_system.kind {
        FileSystemKind::Tmpfs => {
            identity.medium = Medium::Memory;
            identity.interconnect = Interconnect::Memory;
            return identity;
        }
        kind if kind.is_network() => {
            identity.interconnect = Interconnect::Network;
            return identity;
        }
        _ => {}
    }
    let stat = match rustix::fs::stat(path) {
        Ok(stat) => stat,
        Err(e) => {
            identity.note(format!("stat {}", path.display()), e.to_string());
            return identity;
        }
    };
    let (major, minor) = (
        rustix::fs::major(stat.st_dev),
        rustix::fs::minor(stat.st_dev),
    );
    let node = PathBuf::from(format!("/sys/dev/block/{major}:{minor}"));
    let dir = match std::fs::canonicalize(&node) {
        Ok(dir) => dir,
        Err(e) => {
            // Major 0 is an anonymous device: overlayfs, FUSE, btrfs subvolumes. No queue.
            identity.note(format!("resolve {}", node.display()), e.to_string());
            return identity;
        }
    };
    let disk = if dir.join("partition").exists() {
        dir.parent().map(Path::to_path_buf).unwrap_or(dir)
    } else {
        dir
    };
    describe(&disk, &mut identity, 0);
    identity
}

/// Fills `identity` from the sysfs directory of a whole block device.
fn describe(disk: &Path, identity: &mut Identity, depth: usize) {
    let name = disk
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    identity.device = Some(name.clone());
    identity.size_bytes = read_u64(disk, "size", identity).and_then(|sectors| {
        // sysfs `size` counts 512-byte sectors whatever the logical block size.
        sectors.checked_mul(512)
    });
    identity.removable = read_u64(disk, "removable", identity).map(|v| v == 1);

    let queue = disk.join("queue");
    identity.logical_block = read_u32(&queue, "logical_block_size", identity);
    identity.physical_block = read_u32(&queue, "physical_block_size", identity);
    identity.optimal_io = read_u32(&queue, "optimal_io_size", identity).filter(|&v| v > 0);
    identity.max_transfer =
        read_u32(&queue, "max_sectors_kb", identity).and_then(|kib| kib.checked_mul(1024));
    identity.queue_depth = read_u32(&queue, "nr_requests", identity);
    identity.write_cache = match read_str(&queue, "write_cache", identity).as_deref() {
        Some("write back") => WriteCache::WriteBack,
        Some("write through") => WriteCache::WriteThrough,
        _ => WriteCache::Unknown,
    };
    identity.fua = read_u64(&queue, "fua", identity).map(|v| v == 1);
    identity.zoned = match read_str(&queue, "zoned", identity).as_deref() {
        Some("none") => Zoned::None,
        Some("host-aware") => Zoned::HostAware,
        Some("host-managed") => Zoned::HostManaged,
        _ => Zoned::Unknown,
    };
    let rotational = read_u64(&queue, "rotational", identity);
    let canonical = disk.to_string_lossy().into_owned();
    identity.interconnect = interconnect(&name, &canonical, disk, identity);
    identity.medium = match (&identity.interconnect, rotational) {
        (Interconnect::Memory, _) => Medium::Memory,
        (_, Some(1)) => Medium::Rotational,
        (_, Some(0)) => Medium::SolidState,
        _ => Medium::Unknown,
    };
    identity.model = read_str(&disk.join("device"), "model", identity);

    if identity.interconnect == Interconnect::Composite {
        members(disk, identity, depth);
    }
}

fn interconnect(name: &str, canonical: &str, disk: &Path, identity: &mut Identity) -> Interconnect {
    let prefixed = |p: &str| name.starts_with(p);
    if prefixed("dm-") || prefixed("md") {
        return Interconnect::Composite;
    }
    if prefixed("zram") || prefixed("ram") || prefixed("pmem") {
        return Interconnect::Memory;
    }
    if prefixed("nbd") || prefixed("rbd") {
        return Interconnect::Network;
    }
    if canonical.contains("/usb") {
        return Interconnect::Usb;
    }
    if prefixed("nvme") {
        // NVMe over fabrics reports its transport on the controller: pcie, tcp, rdma, fc.
        return match read_str(&disk.join("device"), "transport", identity).as_deref() {
            Some("tcp" | "rdma" | "fc") => Interconnect::Network,
            _ => Interconnect::Nvme,
        };
    }
    if prefixed("vd") || prefixed("xvd") || canonical.contains("/virtio") {
        return Interconnect::Virtual;
    }
    if prefixed("mmcblk") {
        return Interconnect::Mmc;
    }
    if prefixed("sd") {
        return Interconnect::Scsi;
    }
    if prefixed("loop") {
        return Interconnect::Other("loop".to_owned());
    }
    Interconnect::Unknown
}

fn members(disk: &Path, identity: &mut Identity, depth: usize) {
    if depth >= MAX_DEPTH {
        identity.note(
            "slaves",
            format!("nesting deeper than {MAX_DEPTH}; not followed"),
        );
        return;
    }
    let slaves = disk.join("slaves");
    let entries = match std::fs::read_dir(&slaves) {
        Ok(entries) => entries,
        Err(e) => {
            identity.note(format!("read_dir {}", slaves.display()), e.to_string());
            return;
        }
    };
    for entry in entries.flatten().take(MAX_MEMBERS) {
        let Ok(dir) = std::fs::canonicalize(entry.path()) else {
            continue;
        };
        let whole = if dir.join("partition").exists() {
            dir.parent().map(Path::to_path_buf).unwrap_or(dir)
        } else {
            dir
        };
        let mut member = Identity::unknown(identity.file_system.clone());
        describe(&whole, &mut member, depth.saturating_add(1));
        identity.members.push(member);
    }
    // A composite device is as slow as its slowest member: rotational if any member is.
    if identity
        .members
        .iter()
        .any(|m| m.medium == Medium::Rotational)
    {
        identity.medium = Medium::Rotational;
    }
}

fn file_system(path: &Path) -> FileSystem {
    let kind = match rustix::fs::statfs(path) {
        Ok(fs) => kind_of(i128::from(fs.f_type)),
        Err(_) => FileSystemKind::Unknown,
    };
    let (block_size, total_bytes, available_bytes) = match rustix::fs::statvfs(path) {
        Ok(vfs) => (
            Some(vfs.f_frsize),
            vfs.f_blocks.checked_mul(vfs.f_frsize),
            vfs.f_bavail.checked_mul(vfs.f_frsize),
        ),
        Err(_) => (None, None, None),
    };
    FileSystem {
        kind,
        block_size,
        total_bytes,
        available_bytes,
    }
}

/// statfs(2) `f_type` values, from include/uapi/linux/magic.h (ZFS's from the OpenZFS source,
/// ZFS_SUPER_MAGIC). `f_type` is signed on some architectures, so compare as i128.
fn kind_of(f_type: i128) -> FileSystemKind {
    match f_type {
        0xEF53 => FileSystemKind::Ext4,
        0x5846_5342 => FileSystemKind::Xfs,
        0x9123_683E => FileSystemKind::Btrfs,
        0x2FC1_2FC1 => FileSystemKind::Zfs,
        0xF2F5_2010 => FileSystemKind::F2fs,
        0xCA45_1A4E => FileSystemKind::Bcachefs,
        0x0102_1994 | 0x8584_58F6 => FileSystemKind::Tmpfs,
        0x794C_7630 => FileSystemKind::Overlay,
        0x6969 => FileSystemKind::Nfs,
        0xFF53_4D42 | 0xFE53_4D42 | 0x517B => FileSystemKind::Smb,
        0x6573_5546 => FileSystemKind::Fuse,
        0x00C3_6400 => FileSystemKind::Ceph,
        0x4D44 => FileSystemKind::Fat,
        0x2011_BAB0 => FileSystemKind::ExFat,
        0x7366_746E | 0x5346_544E => FileSystemKind::Ntfs,
        0x482B => FileSystemKind::Hfs,
        other => FileSystemKind::Other(format!("statfs f_type {other:#x}")),
    }
}

fn read_str(dir: &Path, attr: &str, identity: &mut Identity) -> Option<String> {
    let path = dir.join(attr);
    match std::fs::read_to_string(&path) {
        Ok(s) => Some(s.trim().to_owned()),
        Err(e) => {
            identity.note(format!("read {}", path.display()), e.to_string());
            None
        }
    }
}

fn read_u64(dir: &Path, attr: &str, identity: &mut Identity) -> Option<u64> {
    let text = read_str(dir, attr, identity)?;
    match text.parse() {
        Ok(v) => Some(v),
        Err(e) => {
            identity.note(
                format!("parse {}/{attr} {text:?}", dir.display()),
                e.to_string(),
            );
            None
        }
    }
}

fn read_u32(dir: &Path, attr: &str, identity: &mut Identity) -> Option<u32> {
    read_u64(dir, attr, identity).and_then(|v| u32::try_from(v).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_known_magic_numbers() {
        assert_eq!(kind_of(0xEF53), FileSystemKind::Ext4);
        assert_eq!(kind_of(0x0102_1994), FileSystemKind::Tmpfs);
        assert_eq!(kind_of(0x794C_7630), FileSystemKind::Overlay);
        assert!(matches!(kind_of(0x1234), FileSystemKind::Other(_)));
    }

    #[test]
    fn identifies_the_test_directory_without_failing() {
        let dir = tempfile::tempdir().unwrap();
        let id = identify(dir.path());
        eprintln!("{id:#?}");
        assert!(id.file_system.available_bytes.is_some());
    }
}
