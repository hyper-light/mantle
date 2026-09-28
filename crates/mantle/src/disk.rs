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
    field(out, "device", &device(&id))?;
    field(out, "medium", &medium(&id))?;
    field(out, "blocks", &blocks(&id))?;
    field(out, "file system", &file_system(&id))?;
    field(out, "write cache", write_cache(&id))?;
    if matches!(id.zoned, Zoned::HostAware | Zoned::HostManaged) {
        field(
            out,
            "zoned",
            "yes: writes must stay sequential within each zone",
        )?;
    }
    if verbose && !id.notes.is_empty() {
        writeln!(out, "  not answered by the OS:")?;
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

fn device(id: &Identity) -> String {
    match (&id.device, &id.model) {
        (Some(d), Some(m)) => format!("{d}: {m}"),
        (Some(d), None) => d.clone(),
        (None, Some(m)) => m.clone(),
        (None, None) => "not reported".to_owned(),
    }
}

fn medium(id: &Identity) -> String {
    let medium = match id.medium {
        Medium::SolidState => "solid state",
        Medium::Rotational => "rotational",
        Medium::Memory => "memory",
        Medium::Unknown => "not reported",
    };
    let link = match &id.interconnect {
        Interconnect::Nvme => "NVMe".to_owned(),
        Interconnect::Scsi => "SATA/SAS/SCSI".to_owned(),
        Interconnect::Usb => "USB".to_owned(),
        Interconnect::Virtual => "virtual disk".to_owned(),
        Interconnect::Mmc => "SD/MMC".to_owned(),
        Interconnect::AppleFabric => "Apple Fabric".to_owned(),
        Interconnect::Network => "network".to_owned(),
        Interconnect::Composite => format!("composite of {} devices", id.members.len()),
        Interconnect::Memory => "RAM".to_owned(),
        Interconnect::Other(s) => s.clone(),
        Interconnect::Unknown => return medium.to_owned(),
    };
    format!("{medium}, {link}")
}

fn blocks(id: &Identity) -> String {
    let show = |b: Option<u32>| b.map_or("?".to_owned(), |b| format!("{b} B"));
    format!(
        "{} logical, {} physical",
        show(id.logical_block),
        show(id.physical_block)
    )
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
    match (id.file_system.available_bytes, id.file_system.total_bytes) {
        (Some(free), Some(total)) => format!(
            "{kind}, {} free of {}",
            display::capacity(free),
            display::capacity(total)
        ),
        _ => kind,
    }
}

fn write_cache(id: &Identity) -> &'static str {
    match id.write_cache {
        WriteCache::WriteBack => "volatile: a write is durable only once flushed",
        WriteCache::WriteThrough => "write-through: completed writes are on stable media",
        WriteCache::Unknown if cfg!(target_vendor = "apple") => {
            "not reported; flushes use F_FULLFSYNC, which empties any drive cache"
        }
        WriteCache::Unknown => "not reported; every flush asks the drive to empty it",
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
        "measuring with a scratch file of up to {} ...",
        display::capacity(plan.span)
    )?;
    out.flush()?;
    let c = calibrate::calibrate(path, align, id.file_system.available_bytes, &plan)
        .map_err(Error::Disk)?;
    report(out, &c)?;
    Ok(())
}

fn report(out: &mut impl Write, c: &Calibration) -> std::io::Result<()> {
    let caching = match c.caching {
        Caching::Direct => "direct I/O",
        Caching::Buffered => "buffered I/O (the file system refused direct I/O)",
    };
    writeln!(
        out,
        "measured in {:.1} s through {caching}, median of three rounds:",
        c.elapsed.as_secs_f64()
    )?;
    let at = |p: &calibrate::Point, rate: String| format!("{rate} at depth {}", p.depth);
    let reads: Vec<String> = c
        .random_read
        .iter()
        .map(|p| {
            format!(
                "{} ({})",
                at(p, display::per_sec(p.ops_per_sec)),
                display::nanos(p.p50_ns)
            )
        })
        .collect();
    measure_field(
        out,
        &format!("{} random reads", display::size(c.small)),
        &reads.join(", "),
    )?;
    if let Some(knee) = c.random_read_knee() {
        measure_field(
            out,
            "useful queue depth",
            &format!("{}: deeper queues add waiting, not throughput", knee.depth),
        )?;
    }
    let seq = |points: &[calibrate::Point]| {
        points
            .iter()
            .map(|p| at(p, display::rate(p.bytes_per_sec)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    measure_field(
        out,
        &format!("{} reads", display::size(c.large)),
        &seq(&c.sequential_read),
    )?;
    measure_field(
        out,
        &format!("{} writes", display::size(c.large)),
        &seq(&c.sequential_write),
    )?;
    measure_field(
        out,
        "durable write",
        &format!(
            "{} (p99 {}): at most {} flushes a second, so writes must share them",
            display::nanos(c.durable_write.p50_ns),
            display::nanos(c.durable_write.p99_ns),
            display::count(c.durable_write.ops_per_sec)
        ),
    )?;
    Ok(())
}

fn measure_field(out: &mut impl Write, name: &str, value: &str) -> std::io::Result<()> {
    writeln!(out, "  {name:<20} {value}")
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
        for name in ["device", "medium", "blocks", "file system", "write cache"] {
            assert!(text.contains(name), "{name} missing from:\n{text}");
        }
    }
}
