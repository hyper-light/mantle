//! The store's runs handed to the device's issuer (`Store::attach`), on a real file whose writes
//! are held at a gate until the test opens it: a run submitted returns before its write lands,
//! and a read of its pages waits for it and reads exactly what was written; a checkpoint
//! answers every run before its flush, so a page written before it reads back after.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use mantle_engine::store::{Config, Store};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 64,
};

/// What every duplicate of one gated file shares, leaked for the test's run.
#[derive(Default)]
struct Gate {
    open: AtomicBool,
    /// Writes that have entered, and those that have completed.
    entered: AtomicUsize,
    completed: AtomicUsize,
}

/// A file whose writes wait at the gate while it is shut.
struct Gated {
    file: DeviceFile,
    gate: &'static Gate,
}

impl BlockFile for Gated {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }

    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.file.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.gate.entered.fetch_add(1, Ordering::SeqCst);
        while !self.gate.open.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        let r = self.file.write_all_at(buf, offset);
        self.gate.completed.fetch_add(1, Ordering::SeqCst);
        r
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }

    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            gate: self.gate,
        })
    }
}

fn payload(address: u64) -> Vec<u8> {
    let mut v = format!("page {address} ").into_bytes();
    v.resize(64 + (address as usize * 31) % 900, address as u8);
    v
}

#[test]
fn a_submitted_run_is_read_once_it_lands() {
    let dir = tempfile::tempdir().unwrap();
    let gate: &'static Gate = Box::leak(Box::default());
    gate.open.store(true, Ordering::SeqCst);
    let file = Gated {
        file: DeviceFile::open(
            &dir.path().join("store"),
            true,
            CachingRequest::Buffered,
            Alignment::new(4096).unwrap(),
        )
        .unwrap(),
        gate,
    };
    let mut store = Store::create(file, CONFIG).unwrap();
    let issuer = Issuer::start(dir.path(), 2).unwrap();
    store.attach(&issuer, 2).unwrap();
    gate.open.store(false, Ordering::SeqCst);
    let before = gate.entered.load(Ordering::SeqCst);

    // Two extents' runs submitted while the device holds every write: each returns at once.
    let mut run = store.run().unwrap();
    let mut addresses = Vec::new();
    for _ in 0..2 {
        let extent = store.allocate_extent().unwrap();
        for n in 0..CONFIG.extent_pages {
            let a = store.address(extent, n).unwrap();
            store.queue_page(&mut run, a, &payload(a)).unwrap();
            addresses.push(a);
        }
    }
    assert_eq!(store.io_stats().submitted, 2);
    while gate.entered.load(Ordering::SeqCst) < before + 2 {
        std::thread::yield_now();
    }
    assert_eq!(gate.completed.load(Ordering::SeqCst), before);

    // A scan of both extents, the gate opened beside it: every page reads as written. Without
    // the wait the span finds its pages past the file's end, which moves only when a run is
    // answered, and fails (checked by removing `settle` from the span's fill).
    std::thread::scope(|s| {
        s.spawn(|| gate.open.store(true, Ordering::SeqCst));
        let mut span = store.span().unwrap();
        for &a in &addresses {
            let mut out = Vec::new();
            store.read_page_ahead(&mut span, a, &mut out).unwrap();
            assert_eq!(out, payload(a), "page {a}");
        }
    });

    // A run, then a checkpoint, then the store reopened: the page reads back.
    let extent = store.allocate_extent().unwrap();
    let a = store.address(extent, 0).unwrap();
    store.queue_page(&mut run, a, &payload(a)).unwrap();
    store.write_run(&mut run).unwrap();
    store.checkpoint(Some(a), 7).unwrap();
    let (file, landed) = store.into_file();
    landed.unwrap();
    let (mut store, recovered) = Store::open(file, CONFIG).unwrap();
    assert_eq!(recovered.applied, 7);
    let mut out = Vec::new();
    store.read_page(a, &mut out).unwrap();
    assert_eq!(out, payload(a));
}

#[test]
fn runs_past_the_batches_wait_in_write_memory_and_read_back_whole() {
    let dir = tempfile::tempdir().unwrap();
    let gate: &'static Gate = Box::leak(Box::default());
    gate.open.store(true, Ordering::SeqCst);
    let file = Gated {
        file: DeviceFile::open(
            &dir.path().join("store"),
            true,
            CachingRequest::Buffered,
            Alignment::new(4096).unwrap(),
        )
        .unwrap(),
        gate,
    };
    let mut store = Store::create(file, CONFIG).unwrap();
    let issuer = Issuer::start(dir.path(), 2).unwrap();
    store.attach(&issuer, 2).unwrap();
    // Write memory for two runs past the two batches out.
    let run_bytes = CONFIG.page_size * CONFIG.extent_pages as usize;
    store.set_write_budget(2 * run_bytes);
    gate.open.store(false, Ordering::SeqCst);

    // Four extents' runs while the device holds every write: two go out, two wait in write
    // memory, and each write returns at once. Had one waited for the device, the gate being
    // shut, the test would not get past this loop.
    let mut run = store.run().unwrap();
    let mut addresses = Vec::new();
    for _ in 0..4 {
        let extent = store.allocate_extent().unwrap();
        for n in 0..CONFIG.extent_pages {
            let a = store.address(extent, n).unwrap();
            store.queue_page(&mut run, a, &payload(a)).unwrap();
            addresses.push(a);
        }
    }
    let io = store.io_stats();
    assert_eq!(
        (io.submitted, io.runs_queued, io.write_waits),
        (2, 2, 0),
        "{io:?}"
    );

    // The queued extents' pages read with the gate still shut: from write memory, exactly as
    // written, without waiting for the device (a wait here would never end).
    let queued_pages = 2 * CONFIG.extent_pages as usize;
    let mut span = store.span().unwrap();
    for &a in addresses.iter().rev().take(queued_pages) {
        let mut out = Vec::new();
        store.read_page_ahead(&mut span, a, &mut out).unwrap();
        assert_eq!(out, payload(a), "queued page {a}");
        let mut point = Vec::new();
        store.read_page(a, &mut point).unwrap();
        assert_eq!(point, payload(a), "queued page {a} read alone");
    }
    store.give_span(span);
    assert_eq!(store.io_stats().queued_reads, 2 * queued_pages as u64);

    // A scan of all four, the gate opened beside it: a queued page is read from write memory,
    // one in flight once its run has landed, each exactly as written. Then every run is drained
    // before the gate shuts again, so none is left held at it while the store waits for room.
    std::thread::scope(|s| {
        s.spawn(|| gate.open.store(true, Ordering::SeqCst));
        let mut span = store.span().unwrap();
        for &a in addresses.iter().rev() {
            let mut out = Vec::new();
            store.read_page_ahead(&mut span, a, &mut out).unwrap();
            assert_eq!(out, payload(a), "page {a}");
        }
        store.give_span(span);
        store.drain().unwrap();
    });
    assert_eq!(store.io_stats().submitted, 4);

    // Queued again, then a checkpoint, which drains the queue before its flush, and a reopen.
    gate.open.store(false, Ordering::SeqCst);
    let mut later = Vec::new();
    for _ in 0..3 {
        let extent = store.allocate_extent().unwrap();
        for n in 0..CONFIG.extent_pages {
            let a = store.address(extent, n).unwrap();
            store.queue_page(&mut run, a, &payload(a)).unwrap();
            later.push(a);
        }
    }
    std::thread::scope(|s| {
        s.spawn(|| gate.open.store(true, Ordering::SeqCst));
        store.checkpoint(later.first().copied(), 9).unwrap();
    });
    let (file, landed) = store.into_file();
    landed.unwrap();
    let (mut store, recovered) = Store::open(file, CONFIG).unwrap();
    assert_eq!(recovered.applied, 9);
    for &a in addresses.iter().chain(&later) {
        let mut out = Vec::new();
        store.read_page(a, &mut out).unwrap();
        assert_eq!(out, payload(a), "page {a} after reopening");
    }
}
