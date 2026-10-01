//! Test devices that several handles reach, as duplicated descriptors reach one file: a volume
//! reads through its own handle while the device's issuer writes through a duplicate of its
//! own (`BlockFile::try_clone`). hyper-block's files have one owner each, and its simulated
//! file has no second handle, so a test shares one device behind an `Arc`, the simulated file
//! behind a lock. Shared by the crate's unit tests (lib.rs) and its integration tests.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::sim::{Crash, Fault, SimFile, SimStats};

/// A handle to a device shared by every handle made from it.
pub struct Handle<T>(pub Arc<T>);

impl<T> Handle<T> {
    pub fn new(device: T) -> Self {
        Self(Arc::new(device))
    }
}

impl<T> Clone for Handle<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> std::ops::Deref for Handle<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T: BlockFile + Sync> BlockFile for Handle<T> {
    fn alignment(&self) -> Alignment {
        self.0.alignment()
    }

    fn len(&self) -> Result<u64, DiskError> {
        self.0.len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.0.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.0.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        self.0.sync_data()
    }

    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(self.clone())
    }
}

/// A simulated file with power-loss semantics that several threads reach, one operation at a
/// time.
pub struct Sim(Mutex<SimFile>);

/// A shared simulated device: what most tests run a volume on.
pub type SimDevice = Handle<Sim>;

impl Sim {
    pub fn new(file: SimFile) -> Self {
        Self(Mutex::new(file))
    }

    fn file(&self) -> MutexGuard<'_, SimFile> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn inject(&self, fault: Fault) -> Result<(), DiskError> {
        self.file().inject(fault)
    }

    pub fn clear_faults(&self) -> Result<(), DiskError> {
        self.file().clear_faults()
    }

    pub fn crash(&self, crash: Crash) -> Result<(), DiskError> {
        self.file().crash(crash)
    }

    pub fn stats(&self) -> Result<SimStats, DiskError> {
        self.file().stats()
    }

    pub fn durable_image(&self) -> Result<Vec<u8>, DiskError> {
        self.file().durable_image()
    }
}

impl BlockFile for Sim {
    fn alignment(&self) -> Alignment {
        self.file().alignment()
    }

    fn len(&self) -> Result<u64, DiskError> {
        self.file().len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.file().read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.file().write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        self.file().sync_data()
    }
}

/// A simulated device of 4 KiB blocks and 512-byte sectors whose crashes and faults replay
/// from `seed`.
pub fn sim(seed: u64) -> SimDevice {
    Handle::new(Sim::new(
        SimFile::new(
            Alignment::new(4096).unwrap(),
            Alignment::new(512).unwrap(),
            seed,
        )
        .unwrap(),
    ))
}
