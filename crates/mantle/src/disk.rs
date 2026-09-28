//! `mantle disk probe`: what the OS says about a device, and what the device does.

use std::fmt;
use std::io::Write;
use std::path::Path;

use mantle_disk::buf::Alignment;
use mantle_disk::calibrate::{self, Calibration, Plan};
use mantle_disk::file::Caching;
use mantle_disk::identity::{FileSystemKind, Identity, Interconnect, Medium, WriteCache, Zoned};

use crate::display;

#[derive(Debug)]
pub enum Error {
    Output(std::io::Error),
    Disk(mantle_disk::DiskError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Output(e) => write!(f, "writing output: {e}"),
            Self::Disk(e) => write!(f, "{e}"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Output(e)
    }
}

pub fn probe(out: &mut impl Write, path: &Path, measure: bool, verbose: bool) -> Result<(), Error> {
    let id = mantle_disk::probe::identify(path);
    writeln!(out, "{}", path.display())?;
    field(out, "disk", &disk(&id))?;
    field(out, "file system", &file_system(&id))?;
    field(out, "safety", safety(&id))?;
    if matches!(id.zoned, Zoned::HostAware | Zoned::HostManaged) {
        field(
            out,
            "zones",
            "the drive is zoned: mantle writes each zone front to back",
        )?;
    }
    if verbose && !id.notes.is_empty() {
        writeln!(out, "  the OS could not say:")?;
        for note in &id.notes {
            writeln!(out, "    {note}")?;
        }
    }
    if measure {
        measured(out, path, &id)?;
    }
    Ok(())
}

fn field(out: &mut impl Write, name: &str, value: &str) -> std::io::Result<()> {
    writeln!(out, "  {name:<13} {value}")
}

fn disk(id: &Identity) -> String {
    let name = id
        .model
        .clone()
        .or_else(|| id.device.clone())
        .unwrap_or_else(|| "a device the OS does not name".to_owned());
    let medium = match id.medium {
        Medium::SolidState => "flash",
        Medium::Rotational => "spinning disk",
        Medium::Memory => "memory",
        Medium::Unknown => "medium not reported",
    };
    let link = match &id.interconnect {
        Interconnect::Nvme => Some("NVMe".to_owned()),
        Interconnect::Scsi => Some("SATA/SAS".to_owned()),
        Interconnect::Usb => Some("USB".to_owned()),
        Interconnect::Virtual => Some("a virtual disk".to_owned()),
        Interconnect::Mmc => Some("SD card".to_owned()),
        Interconnect::AppleFabric => Some("internal".to_owned()),
        Interconnect::Network => Some("over the network".to_owned()),
        Interconnect::Composite => Some(format!("{} devices combined", id.members.len())),
        Interconnect::Memory => Some("RAM".to_owned()),
        Interconnect::Other(s) => Some(s.clone()),
        Interconnect::Unknown => None,
    };
    match link {
        Some(link) => format!("{name}, {link} {medium}"),
        None => format!("{name}, {medium}"),
    }
}

fn file_system(id: &Identity) -> String {
    let kind = match &id.file_system.kind {
        FileSystemKind::Apfs => "APFS".to_owned(),
        FileSystemKind::Hfs => "HFS+".to_owned(),
        FileSystemKind::Ext4 => "ext4".to_owned(),
        FileSystemKind::Xfs => "XFS".to_owned(),
        FileSystemKind::Btrfs => "Btrfs".to_owned(),
        FileSystemKind::Zfs => "ZFS".to_owned(),
        FileSystemKind::F2fs => "F2FS".to_owned(),
        FileSystemKind::Bcachefs => "bcachefs".to_owned(),
        FileSystemKind::Ntfs => "NTFS".to_owned(),
        FileSystemKind::Refs => "ReFS".to_owned(),
        FileSystemKind::Fat => "FAT".to_owned(),
        FileSystemKind::ExFat => "exFAT".to_owned(),
        FileSystemKind::Tmpfs => "tmpfs (memory: nothing survives a reboot)".to_owned(),
        FileSystemKind::Overlay => "overlayfs".to_owned(),
        FileSystemKind::Nfs => "NFS".to_owned(),
        FileSystemKind::Smb => "SMB".to_owned(),
        FileSystemKind::Fuse => "FUSE".to_owned(),
        FileSystemKind::Ceph => "CephFS".to_owned(),
        FileSystemKind::Other(s) => s.clone(),
        FileSystemKind::Unknown => "not reported".to_owned(),
    };
    match id.file_system.available_bytes {
        Some(free) => format!("{kind}, {} free", display::capacity(free)),
        None => kind,
    }
}

/// What mantle does to make a write safe on this device, and why.
fn safety(id: &Identity) -> &'static str {
    match id.write_cache {
        WriteCache::WriteBack => {
            "the drive caches writes, so every commit empties its cache before mantle answers"
        }
        WriteCache::WriteThrough => "the drive puts writes on stable media before it answers",
        WriteCache::Unknown => {
            "the drive does not say whether it caches writes, so every commit empties its cache"
        }
    }
}

fn measured(out: &mut impl Write, path: &Path, id: &Identity) -> Result<(), Error> {
    let align = [id.logical_block, id.physical_block]
        .into_iter()
        .flatten()
        .filter_map(|b| usize::try_from(b).ok())
        .filter_map(|b| Alignment::new(b).ok())
        .fold(
            Alignment::new(4096).unwrap_or(Alignment::BYTE),
            Alignment::max,
        );
    let plan = Plan::standard(align);
    writeln!(
        out,
        "measuring with a scratch file of up to {}, removed afterwards...",
        display::capacity(plan.span)
    )?;
    out.flush()?;
    let c = calibrate::calibrate(path, align, id.file_system.available_bytes, &plan)
        .map_err(Error::Disk)?;
    report(out, &c)?;
    Ok(())
}

fn report(out: &mut impl Write, c: &Calibration) -> std::io::Result<()> {
    writeln!(out, "measured in {:.0} s:", c.elapsed.as_secs_f64())?;
    if let Some(knee) = c.random_read_knee() {
        field(
            out,
            "reads",
            &format!(
                "fastest with {} small reads in flight, so mantle keeps up to {} in flight",
                knee.depth, knee.depth
            ),
        )?;
    }
    let best = |points: &[calibrate::Point]| {
        points
            .iter()
            .map(|p| p.bytes_per_sec)
            .fold(0.0f64, f64::max)
    };
    field(
        out,
        "throughput",
        &format!(
            "{} reading and {} writing large transfers",
            display::rate(best(&c.sequential_read)),
            display::rate(best(&c.sequential_write))
        ),
    )?;
    field(
        out,
        "commits",
        &format!(
            "about {} until a write is safe; writes that arrive together share one commit",
            display::nanos(c.durable_write.p50_ns)
        ),
    )?;
    if c.caching == Caching::Buffered {
        field(
            out,
            "caching",
            "the file system refused direct I/O, so reads and writes pass through its cache",
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probing_a_directory_prints_every_field() {
        let dir = tempfile::tempdir().unwrap();
        let mut out = Vec::new();
        probe(&mut out, dir.path(), false, true).unwrap();
        let text = String::from_utf8(out).unwrap();
        for name in ["disk", "file system", "safety"] {
            assert!(text.contains(name), "{name} missing from:\n{text}");
        }
    }
}
