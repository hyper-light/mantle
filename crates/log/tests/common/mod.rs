//! A device whose flushes a test lets through one at a time, shared by the log's tests.
#![allow(clippy::unwrap_used, clippy::arithmetic_side_effects)]

use std::sync::Arc;

use mantle_disk::buf::Alignment;
use mantle_disk::sim::SimFile;

/// A simulated file that lets its flushes through one at a time, so a test can step the writer
/// frame by frame and submit while it is held in a flush.
pub struct Stepped {
    file: Arc<SimFile>,
    /// Flushes that have arrived, and how many of them may complete.
    gate: std::sync::Mutex<(u64, u64)>,
    changed: std::sync::Condvar,
}

impl Stepped {
    pub fn new(file: Arc<SimFile>) -> Arc<Self> {
        Arc::new(Self {
            file,
            gate: std::sync::Mutex::new((0, u64::MAX)),
            changed: std::sync::Condvar::new(),
        })
    }

    /// Holds every flush from here on.
    pub fn hold(&self) {
        let mut gate = self.gate.lock().unwrap();
        gate.1 = gate.0;
    }

    /// Waits until the writer is held in a flush.
    pub fn held(&self) {
        let mut gate = self.gate.lock().unwrap();
        while gate.0 <= gate.1 {
            gate = self.changed.wait(gate).unwrap();
        }
    }

    /// Lets the held flush complete and waits until the writer is held in its next one.
    pub fn step(&self) {
        let mut gate = self.gate.lock().unwrap();
        gate.1 += 1;
        self.changed.notify_all();
        while gate.0 <= gate.1 {
            gate = self.changed.wait(gate).unwrap();
        }
    }

    pub fn release(&self) {
        self.gate.lock().unwrap().1 = u64::MAX;
        self.changed.notify_all();
    }
}

/// Releases the writer when dropped, as a failing test's unwinding drops it too, so a log
/// dropped after it never waits on a writer held in a flush.
pub struct Released(pub Arc<Stepped>);

impl Drop for Released {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl mantle_disk::block::BlockFile for Stepped {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }

    fn len(&self) -> Result<u64, mantle_disk::DiskError> {
        mantle_disk::block::BlockFile::len(&*self.file)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), mantle_disk::DiskError> {
        self.file.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), mantle_disk::DiskError> {
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), mantle_disk::DiskError> {
        let mut gate = self.gate.lock().unwrap();
        gate.0 += 1;
        let me = gate.0;
        self.changed.notify_all();
        while me > gate.1 {
            gate = self.changed.wait(gate).unwrap();
        }
        drop(gate);
        self.file.sync_data()
    }
}
