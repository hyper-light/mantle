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
    field(out, "write cache", write_cache(&id))?;
    if matches!(id.zoned, Zoned::HostAware | Zoned::HostManaged) {
        field(
            out,
            "zones",
            "zoned drive; mantle writes each zone sequentially",
        )?;
    }
    if verbose && !id.notes.is_empty() {
        writeln!(out, "  not reported by the OS:")?;
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

/// Whether the device holds writes in a volatile cache, and what mantle does about it.
fn write_cache(id: &Identity) -> &'static str {
    match id.write_cache {
        WriteCache::WriteBack => "volatile; mantle flushes it on every commit",
        WriteCache::WriteThrough => "write-through; writes are on stable media when they complete",
        WriteCache::Unknown => "not reported; mantle flushes the drive cache on every commit",
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
        "measuring with a scratch file of up to {} (removed afterwards)",
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
    if let (Some(knee), Some(saturation)) = (c.random_read_knee(), c.random_read_saturation()) {
        field(
            out,
            "reads",
            &format!(
                "throughput stops growing at {} concurrent {} reads, which mantle holds at the \
                 device; {} get the most throughput for their wait",
                saturation.depth,
                display::size(c.small),
                knee.depth
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
            "{} read, {} write ({} transfers)",
            display::rate(best(&c.sequential_read)),
            display::rate(best(&c.sequential_write)),
            display::size(c.large)
        ),
    )?;
    field(
        out,
        "commits",
        &format!(
            "{} per durable write; concurrent writes share a flush",
            display::quantile(c.durable_write.p50_ns)
        ),
    )?;
    field(
        out,
        "batches",
        &format!(
            "{} when {} is made durable at a time",
            display::rate(c.durable_sequential.bytes_per_sec),
            display::size(c.durable_large)
        ),
    )?;
    field(out, "first writes", &first_writes(c))?;
    if c.caching == Caching::Buffered {
        field(
            out,
            "caching",
            "the file system refused direct I/O; reads and writes use the page cache",
        )?;
    }
    Ok(())
}

/// What a durable write into never-written space costs against one over written space, and
/// what the chunk store does about it.
pub(crate) fn first_writes(c: &Calibration) -> String {
    let ratio = if c.first_write.ops_per_sec > 0.0 {
        c.overwrite.ops_per_sec / c.first_write.ops_per_sec
    } else {
        f64::INFINITY
    };
    if c.first_write_penalty() {
        format!(
            "a durable write into new space takes {ratio:.1}x one over written space; \
             volumes here are written once at format"
        )
    } else {
        format!(
            "a durable write into new space takes {ratio:.2}x one over written space, \
             within the noise; volumes here are not pre-written"
        )
    }
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
        for name in ["disk", "file system", "write cache"] {
            assert!(text.contains(name), "{name} missing from:\n{text}");
        }
    }
}
