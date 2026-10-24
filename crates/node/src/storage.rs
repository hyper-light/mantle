//! The node's storage role (docs/design/node.md §1.1, §1.6, §5.2): the chunk volumes on its data
//! devices, each held under its device directory's `LOCK` and written through its device's issuer,
//! and the puts and gets of their chunks, whether a peer sent them on the transfer class or the
//! node asked them itself, in-process (the laptop's path, §5.5).
//!
//! Every wait is a ticket, so the shard that runs the node never waits on a device (§1.3): a put
//! answers through the volume's writer, which wakes the node's task once the chunk is durable; a
//! get is a batch at the device's issuer, taken as its answer comes (`mantle_chunk::Reader`).
//! Opening is the exception, and runs before the node's runtime does: an issuer's start and a
//! volume's recovery wait on the device, which hyper-block refuses on a shard.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::task::{Context, Poll, Waker};

use hyper_block::DiskError;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_transport::ExchangeId;
use mantle_chunk::{Answer, ChunkError, ChunkKey, Config, ReadId, Reader, Volume};

/// The directory a device's mantle files sit in, inside the device directory the configuration
/// names (node.md §1.6).
const MANTLE: &str = "mantle";
/// A directory's lock, held while the node runs: one owner per device (audit §14.1), and one
/// process a node directory.
const LOCK: &str = "LOCK";
/// The volume's file.
const VOLUME: &str = "volume";
/// A volume being laid out, renamed to [`VOLUME`] once whole.
const VOLUME_NEW: &str = "volume.new";

/// A chunk volume the node keeps, as its configuration states it.
#[derive(Debug, Clone)]
pub struct VolumeSettings {
    /// The device directory: the volume is `mantle/volume` in it, held under `mantle/LOCK`.
    pub dir: PathBuf,
    /// The volume's bytes, laid out at its first start.
    pub size: u64,
    pub config: Config,
    /// Transfers the device's issuer keeps in flight (`hyper_block::issuer::depth`): the measured
    /// depth, or one for a device not measured.
    pub depth: usize,
}

/// Why the node's storage could not be opened.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// A lock another process holds: a device directory's (`mantle/LOCK`) or the node
    /// directory's own (`LOCK`).
    #[error("{path}: another process holds this lock")]
    Locked { path: PathBuf },
    #[error("{op} {path}: {source}")]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("the device under {path}: {source}")]
    Device {
        path: PathBuf,
        #[source]
        source: DiskError,
    },
    #[error("the volume at {path}: {source}")]
    Volume {
        path: PathBuf,
        #[source]
        source: ChunkError,
    },
    #[error("the volumes at {first} and {second} are one volume, {volume:032x}")]
    Duplicate {
        volume: u128,
        first: PathBuf,
        second: PathBuf,
    },
}

/// What asked for a chunk's put or get, which its answer goes back to: a peer's exchange, or the
/// node itself, by the number it asked under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Origin {
    Exchange(ExchangeId),
    Asked(u64),
}

/// A put or a get answered.
#[derive(Debug)]
pub(crate) enum Done {
    Put(Origin, Result<(), ChunkError>),
    Get(Origin, Result<Vec<u8>, ChunkError>),
}

/// The node's volumes and the puts and gets out on them.
#[derive(Default)]
pub struct Storage {
    // Field order is drop order: the volumes, each its reader before it, close before their
    // devices' issuers stop.
    volumes: Vec<Held>,
    devices: Vec<Device>,
    /// Each put out, with the room it holds of what the node takes.
    puts: Vec<(Origin, Answer, u64)>,
    gets: Vec<(usize, ReadId, Origin)>,
    /// Bytes of the chunks peers sent being read or queued at a volume, and the most there may
    /// be: what the volumes' writer queues hold together, past which a volume refuses a put
    /// `Busy` anyway (`Volume::queue_bytes`).
    taken: u64,
    room: u64,
    /// The chunk exchanges the volumes take at once (`mantle_chunk::Config::transfers`).
    transfers: usize,
}

struct Held {
    id: u128,
    dir: PathBuf,
    reader: Reader,
    volume: Volume<DeviceFile>,
    _lock: File,
}

/// A device's issuer, and the name the OS gives the device, which its volumes share it by.
struct Device {
    name: Option<String>,
    issuer: Issuer,
}

impl Storage {
    /// A node with no storage role: it holds no volume, and refuses every chunk sent to it.
    pub fn none() -> Self {
        Self::default()
    }

    /// Opens each volume `settings` names, off any runtime shard: takes its device directory's
    /// lock, starts its device's issuer (one a device the OS names, shared by the device's
    /// volumes), and opens its volume, recovering it, or lays it out at its first start. A step
    /// that fails leaves what the steps before it opened to close as `Storage` drops.
    pub fn open(settings: &[VolumeSettings]) -> Result<Self, StorageError> {
        Self::open_locked(settings, Locks::default())
    }

    /// [`Storage::open`], each volume under the lock `locks` holds of its device directory, or one
    /// taken now where it holds none.
    pub fn open_locked(
        settings: &[VolumeSettings],
        mut locks: Locks,
    ) -> Result<Self, StorageError> {
        let mut storage = Self::none();
        for volume in settings {
            let lock = match locks.give(&volume.dir) {
                Some(lock) => lock,
                None => Locks::take_one(&volume.dir)?,
            };
            storage.open_one(volume, lock)?;
        }
        Ok(storage)
    }

    fn open_one(&mut self, settings: &VolumeSettings, lock: File) -> Result<(), StorageError> {
        let dir = mantle_dir(&settings.dir);
        let identity = mantle_disk::probe::identify(&settings.dir);
        let align = identity.transfer_alignment();
        let device = match identity.device {
            Some(name) => match self
                .devices
                .iter()
                .position(|d| d.name.as_deref() == Some(name.as_str()))
            {
                Some(at) => at,
                None => self.start_device(Some(name), &dir, settings.depth)?,
            },
            // A device the OS does not name shares its issuer with no other volume.
            None => self.start_device(None, &dir, settings.depth)?,
        };
        let issuer = &self
            .devices
            .get(device)
            .ok_or(StorageError::Device {
                path: dir.clone(),
                source: DiskError::Unsupported {
                    path: dir.clone(),
                    reason: "a device's issuer the node did not keep",
                },
            })?
            .issuer;
        let volume = open_volume(&dir, settings, issuer, align)?;
        let id = volume.volume_id();
        if let Some(held) = self.volumes.iter().find(|h| h.id == id) {
            return Err(StorageError::Duplicate {
                volume: id,
                first: held.dir.clone(),
                second: settings.dir.clone(),
            });
        }
        let reader = volume
            .reader(issuer)
            .map_err(|source| StorageError::Volume {
                path: dir.clone(),
                source,
            })?;
        self.room = self.room.saturating_add(volume.queue_bytes());
        self.transfers = self.transfers.saturating_add(settings.config.transfers());
        self.volumes.push(Held {
            id,
            dir: settings.dir.clone(),
            reader,
            volume,
            _lock: lock,
        });
        Ok(())
    }

    fn start_device(
        &mut self,
        name: Option<String>,
        dir: &Path,
        depth: usize,
    ) -> Result<usize, StorageError> {
        let issuer = Issuer::start(dir, depth).map_err(|source| StorageError::Device {
            path: dir.to_path_buf(),
            source,
        })?;
        self.devices.push(Device { name, issuer });
        Ok(self.devices.len().saturating_sub(1))
    }

    /// The volumes the node holds: each one's id and its device directory.
    pub fn volumes(&self) -> impl Iterator<Item = (u128, &Path)> {
        self.volumes.iter().map(|h| (h.id, h.dir.as_path()))
    }

    /// The volumes as the node enlists them in the volume table: each one's id and the bytes its
    /// segments hold.
    pub fn enlisting(&self) -> Vec<(u128, u64)> {
        self.volumes
            .iter()
            .map(|h| (h.id, h.volume.data_bytes()))
            .collect()
    }

    /// The slot of the volume `volume` names, if the node holds it.
    pub(crate) fn holds(&self, volume: u128) -> Option<usize> {
        self.volumes.iter().position(|h| h.id == volume)
    }

    /// The chunk exchanges the node's volumes take at once: each one's writer queue and its reads
    /// at the device and waiting, past which it refuses `Busy`.
    pub(crate) fn transfers(&self) -> usize {
        self.transfers
    }

    /// The largest chunk a record on any of the node's volumes holds, none without volumes.
    pub fn max_payload(&self) -> Option<u64> {
        self.volumes.iter().map(|h| h.volume.max_payload()).max()
    }

    /// Takes `len` bytes of room for a chunk a peer sends, before its body is read: refused when
    /// the chunks being taken already hold what the volumes' queues hold. A chunk larger than
    /// the room is taken alone, as a volume's writer queue takes a payload larger than its bound.
    pub(crate) fn take_room(&mut self, len: u64) -> bool {
        match self.taken.checked_add(len) {
            Some(taken) if taken <= self.room || self.taken == 0 => {
                self.taken = taken;
                true
            }
            _ => false,
        }
    }

    /// Gives back room a chunk held: answered, or its body never read whole.
    pub(crate) fn give_room(&mut self, len: u64) {
        self.taken = self.taken.saturating_sub(len);
    }

    /// Puts `bytes`, `key`'s whole chunk, on the volume in `slot`, checked against `crc32c`: its
    /// answer comes through [`Self::poll_done`], `waker` woken when it has, and the `room` it
    /// holds is given back then. A refusal before it is queued (`Busy`, `Full`, `TooLarge`,
    /// `Checksum`) is answered at once, its room still the caller's to give back.
    pub(crate) fn put(
        &mut self,
        slot: usize,
        key: ChunkKey,
        bytes: Vec<u8>,
        crc32c: u32,
        waker: &Waker,
        (origin, room): (Origin, u64),
    ) -> Result<(), ChunkError> {
        let held = self.volumes.get(slot).ok_or(ChunkError::Closed)?;
        let answer = held
            .volume
            .put_checked_waking(key, bytes, crc32c, waker.clone())?;
        self.puts.push((origin, answer, room));
        Ok(())
    }

    /// Gets bytes `[offset, offset + len)` of `key`'s chunk from the volume in `slot`: its answer
    /// comes through [`Self::poll_done`]. `NotFound`, `Range` and `Busy` are answered at once.
    pub(crate) fn get(
        &mut self,
        slot: usize,
        key: &ChunkKey,
        offset: u64,
        len: u64,
        origin: Origin,
    ) -> Result<(), ChunkError> {
        let held = self.volumes.get_mut(slot).ok_or(ChunkError::Closed)?;
        let id = held.reader.submit(&held.volume, key, offset, len)?;
        self.gets.push((slot, id, origin));
        Ok(())
    }

    /// Puts and gets out.
    pub(crate) fn out(&self) -> usize {
        self.puts.len().saturating_add(self.gets.len())
    }

    /// Moves the puts and gets answered since the last call into `done`, and registers the task
    /// `cx` names to be woken when a get still out is answered (a put wakes the waker it was
    /// given).
    pub(crate) fn poll_done(&mut self, cx: &mut Context<'_>, done: &mut Vec<Done>) {
        let mut at = 0;
        while let Some((origin, answer, room)) = self.puts.get(at) {
            match answer.poll() {
                Some(answered) => {
                    done.push(Done::Put(*origin, answered));
                    self.taken = self.taken.saturating_sub(*room);
                    self.puts.swap_remove(at);
                }
                None => at = at.saturating_add(1),
            }
        }
        for (slot, held) in self.volumes.iter_mut().enumerate() {
            let Held { reader, volume, .. } = held;
            while let Poll::Ready(Some((id, answered))) = reader.poll_next(volume, cx) {
                if let Some(at) = self
                    .gets
                    .iter()
                    .position(|(s, r, _)| *s == slot && *r == id)
                {
                    let (_, _, origin) = self.gets.swap_remove(at);
                    done.push(Done::Get(origin, answered));
                }
            }
        }
    }
}

/// A device directory's `mantle/`: its lock, its profile (`mantle_disk::profile`) and its volume.
pub fn mantle_dir(dir: &Path) -> PathBuf {
    dir.join(MANTLE)
}

/// A device directory's volume, once laid out.
pub fn volume_path(dir: &Path) -> PathBuf {
    mantle_dir(dir).join(VOLUME)
}

/// Device directories' locks, taken before anything reads or measures their devices (node.md §1.7
/// step 1): each one's `mantle/LOCK`, held while kept here and then by the volume opened under it
/// ([`Storage::open_locked`]).
#[derive(Default)]
pub struct Locks {
    held: Vec<(PathBuf, File)>,
}

/// The node directory's own lock (node.md §1.6), `<dir>/LOCK`: held while it lives, since the
/// directory's log and stores admit one process as a volume does.
pub struct NodeLock {
    _file: File,
}

impl Locks {
    /// The node directory `dir`'s lock: refused when another process holds it.
    pub fn node(dir: &Path) -> Result<NodeLock, StorageError> {
        lock(&dir.join(LOCK)).map(|file| NodeLock { _file: file })
    }

    /// Takes the lock of each of `dirs`, creating its `mantle/`: refused at the first another
    /// process holds.
    pub fn take<'a>(dirs: impl IntoIterator<Item = &'a Path>) -> Result<Self, StorageError> {
        let mut locks = Self::default();
        for dir in dirs {
            if locks.held.iter().any(|(held, _)| held == dir) {
                continue;
            }
            let lock = Self::take_one(dir)?;
            locks.held.push((dir.to_path_buf(), lock));
        }
        Ok(locks)
    }

    fn take_one(dir: &Path) -> Result<File, StorageError> {
        let mantle = mantle_dir(dir);
        std::fs::create_dir_all(&mantle).map_err(io("create", &mantle))?;
        lock(&mantle.join(LOCK))
    }

    /// `dir`'s lock, given up by these to the volume opened under it.
    fn give(&mut self, dir: &Path) -> Option<File> {
        let at = self.held.iter().position(|(held, _)| held == dir)?;
        Some(self.held.swap_remove(at).1)
    }
}

/// Takes `path`'s lock, creating the file: held while the file stays open.
fn lock(path: &Path) -> Result<File, StorageError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(io("open", path))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(StorageError::Locked {
            path: path.to_path_buf(),
        }),
        Err(TryLockError::Error(source)) => Err(StorageError::Io {
            op: "lock",
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// Opens the volume in `dir`, recovering it; or, at its first start, lays one out under another
/// name, closes it, renames it into place and flushes the directory (CLAUDE.md §6), so `volume` is
/// only ever a whole layout: a start cut part way through lays it out again.
fn open_volume(
    dir: &Path,
    settings: &VolumeSettings,
    issuer: &Issuer,
    align: hyper_block::buf::Alignment,
) -> Result<Volume<DeviceFile>, StorageError> {
    let path = dir.join(VOLUME);
    let failed = |source| StorageError::Volume {
        path: path.clone(),
        source,
    };
    let exists = path.try_exists().map_err(io("look for", &path))?;
    if !exists {
        let fresh = dir.join(VOLUME_NEW);
        match std::fs::remove_file(&fresh) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io("remove", &fresh)(e)),
        }
        let file = DeviceFile::open(&fresh, true, CachingRequest::PreferDirect, align)
            .map_err(device(&fresh))?;
        file.preallocate(settings.size).map_err(device(&fresh))?;
        let volume =
            Volume::format(issuer, file, settings.size, settings.config).map_err(failed)?;
        volume.close();
        hyper_block::file::rename_durable(&fresh, &path).map_err(device(&path))?;
    }
    let file = DeviceFile::open(&path, false, CachingRequest::PreferDirect, align)
        .map_err(device(&path))?;
    let (volume, _recovered) = Volume::open(issuer, file, settings.config).map_err(failed)?;
    Ok(volume)
}

fn io(op: &'static str, path: &Path) -> impl FnOnce(std::io::Error) -> StorageError {
    let path = path.to_path_buf();
    move |source| StorageError::Io { op, path, source }
}

fn device(path: &Path) -> impl FnOnce(DiskError) -> StorageError {
    let path = path.to_path_buf();
    move |source| StorageError::Device { path, source }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_take_room_up_to_the_queues_bytes_and_one_larger_goes_alone() {
        let mut storage = Storage::none();
        storage.room = 100;
        assert!(storage.take_room(60));
        assert!(
            !storage.take_room(50),
            "past the room while another holds some"
        );
        assert!(storage.take_room(40));
        storage.give_room(100);
        assert!(storage.take_room(500), "larger than the room, taken alone");
        assert!(!storage.take_room(1));
        storage.give_room(500);
        assert!(storage.take_room(1));
    }
}
