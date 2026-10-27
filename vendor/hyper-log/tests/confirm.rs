//! A frame's confirmation is made durable the cheaper of the two ways its file has measured
//! (`device::Device::confirm`): a write durable on its own, or a write and a flush of the cache.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods
)]

use std::time::Duration;

use hyper_block::DiskError;
use hyper_block::block::{BlockFile, Durable};
use hyper_block::buf::Alignment;
use hyper_block::sim::SimFile;
use hyper_log::{Config, Entries, Entry, Log, Update, Waits};

const ID: u128 = 0x636f_6e66_6972_6d73;
const BLOCK: usize = 4096;

/// A simulated file whose writes made durable on their own (a FUA write) take `durable` and whose
/// flushes of its cache take `flush`, as a drive's do; each made durable in the simulation.
struct Timed {
    file: SimFile,
    durable: Duration,
    flush: Duration,
}

impl BlockFile for Timed {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }

    fn len(&self) -> Result<u64, DiskError> {
        BlockFile::len(&self.file)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.file.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        std::thread::sleep(self.flush);
        self.file.sync_data()
    }

    fn write_durable_at(&self, buf: &[u8], offset: u64) -> Result<Durable, DiskError> {
        std::thread::sleep(self.durable);
        self.file.write_all_at(buf, offset)?;
        self.file.sync_data()?;
        Ok(Durable::Written)
    }
}

fn config() -> Config {
    Config {
        segment_bytes: 16 * BLOCK as u64,
        max_segments: 8,
        max_groups: 4,
        group_entries: 1 << 16,
        group_bytes: 1 << 24,
        group_cache: 1 << 10,
        queue_submissions: 256,
        waits: Waits::Measured,
    }
}

/// Twelve entries written one after another, each waited for, so every frame is confirmed on its
/// own: the log's statistics after.
fn confirmations(durable: Duration, flush: Duration) -> Box<hyper_log::LogStats> {
    let file = Timed {
        file: SimFile::new(
            Alignment::new(BLOCK).unwrap(),
            Alignment::new(512).unwrap(),
            7,
        )
        .unwrap(),
        durable,
        flush,
    };
    let log = Log::create(file, config(), ID).unwrap();
    for index in 1..=12u64 {
        log.write(
            1,
            Update {
                entries: Some(Entries {
                    first: index,
                    entries: vec![Entry {
                        term: 1,
                        bytes: index.to_le_bytes().to_vec(),
                    }],
                }),
                ..Update::default()
            },
        )
        .unwrap();
    }
    assert_eq!(log.view(1).unwrap().unwrap().last, 12);
    log.stats(None).unwrap()
}

/// Do: confirm frames on a drive whose durable write costs more than a write and a flush (a
/// consumer NVMe drive's FUA write). Expect: the first confirmation is a durable write, which
/// measures it; every later one is written and flushed.
#[test]
fn a_drive_whose_durable_write_costs_more_than_a_flush_confirms_by_flushing() {
    let stats = confirmations(Duration::from_millis(20), Duration::ZERO);
    assert_eq!(stats.durable_writes, 1, "{stats:?}");
    assert!(stats.flushed_confirmations >= 1, "{stats:?}");
}

/// Do: confirm frames on a drive whose flush costs more than its durable write. Expect: every
/// confirmation is a durable write, none flushed.
#[test]
fn a_drive_whose_flush_costs_more_than_its_durable_write_confirms_durable() {
    let stats = confirmations(Duration::ZERO, Duration::from_millis(20));
    assert_eq!(stats.flushed_confirmations, 0, "{stats:?}");
    assert!(stats.durable_writes >= 1, "{stats:?}");
}
