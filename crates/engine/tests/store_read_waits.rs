//! A span read's wait prices the kernel's page cache only on a maintenance worker's store, whose
//! compactions read their inputs through it, and only where the file can tell which pages the OS
//! holds (docs/research/65 §8): what a read the OS did not hold waited for the device. A file that
//! cannot tell, as the simulated device cannot, counts no wait whatever it reads, so a budget on it
//! leaves the kernel no share; and the shard's own store asks the OS nothing and times nothing for
//! it, so its scans pay neither a probe nor a clock read.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::sim::SimFile;
use hyper_block::{DiskError, Residency};
use mantle_engine::store::{Config, Store};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 32,
    max_extents: 4096,
};

/// A file that says it can tell which pages the OS holds and holds none: each read asked of it
/// from memory is counted and refused, so every span read waits on the file.
struct Telling {
    file: DeviceFile,
    probes: u64,
}

impl BlockFile for Telling {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }

    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.file.read_exact_at(buf, offset)
    }

    fn read_resident_at(&mut self, _buf: &mut [u8], _offset: u64) -> Result<bool, DiskError> {
        self.probes += 1;
        Ok(false)
    }

    fn reads_resident(&self) -> bool {
        true
    }

    fn residency(&self) -> Residency {
        Residency {
            probes: self.probes,
            ..Residency::default()
        }
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }
}

/// Writes two extents of pages through `store` and returns their addresses.
fn write_pages<F: BlockFile>(store: &mut Store<F>) -> Vec<u64> {
    let mut run = store.run().unwrap();
    let mut addresses = Vec::new();
    for _ in 0..2 {
        let extent = store.allocate_extent().unwrap();
        for n in 0..CONFIG.extent_pages {
            let a = store.address(extent, n).unwrap();
            store.queue_page(&mut run, a, &[7u8; 100]).unwrap();
            addresses.push(a);
        }
    }
    store.write_run(&mut run).unwrap();
    store.drain().unwrap();
    addresses
}

/// Reads `addresses` in order through one span of `store`, checking each page.
fn read_pages<F: BlockFile>(store: &mut Store<F>, addresses: &[u64]) {
    let mut span = store.span_sequential().unwrap();
    let mut out = Vec::new();
    for &a in addresses {
        out.clear();
        store.read_page_ahead(&mut span, a, &mut out).unwrap();
        assert_eq!(&out[..], &[7u8; 100][..], "page {a}");
    }
    store.give_span(span);
}

fn telling(path: &std::path::Path, create: bool) -> Telling {
    Telling {
        file: DeviceFile::open(
            path,
            create,
            CachingRequest::Buffered,
            Alignment::new(4096).unwrap(),
        )
        .unwrap(),
        probes: 0,
    }
}

#[test]
fn a_file_that_cannot_tell_what_the_os_holds_counts_no_read_wait() {
    let align = Alignment::new(4096).unwrap();
    let file = SimFile::new(align, Alignment::new(512).unwrap(), 5).unwrap();
    assert!(
        !file.reads_resident(),
        "the simulated device tells residency"
    );
    let mut store = Store::create(file, CONFIG).unwrap();
    let addresses = write_pages(&mut store);
    let before = store.io_stats();
    read_pages(&mut store, &addresses);
    let after = store.io_stats();
    assert!(after.reads > before.reads, "no read went to the file");
    assert_eq!(after.read_wait_ns, 0);
    assert_eq!(store.read_wait_ns(), 0);
}

#[test]
fn only_a_workers_store_asks_the_os_and_prices_its_reads_waits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut shard = Store::create(telling(&path, true), CONFIG).unwrap();
    let addresses = write_pages(&mut shard);
    let before = shard.io_stats();
    read_pages(&mut shard, &addresses);
    let after = shard.io_stats();
    let reads = after.reads - before.reads;
    assert!(reads > 0, "no read went to the file");
    // The shard's own store asked the OS nothing and counted no wait, though its file can tell.
    assert_eq!(after.residency.probes, 0);
    assert_eq!(after.read_wait_ns, 0);
    // A worker's store over the same file asks before each span read, and each one the OS did
    // not hold waited on the file.
    let mut worker = Store::worker(telling(&path, false), CONFIG).unwrap();
    worker
        .begin_job(&[], shard.end(), shard.generation())
        .unwrap();
    let before = worker.io_stats();
    read_pages(&mut worker, &addresses);
    let after = worker.io_stats();
    assert_eq!(after.reads - before.reads, reads);
    assert_eq!(after.residency.probes, reads);
    assert!(after.read_wait_ns > 0, "the worker's waits were not priced");
    assert_eq!(worker.read_wait_ns(), after.read_wait_ns);
}
