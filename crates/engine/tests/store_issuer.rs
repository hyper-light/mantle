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

use std::future::{Future, poll_fn};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::Poll;

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_rt::combine::{Either, race2};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{Builder, Op};
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
    hold_reads: AtomicBool,
    reads_entered: AtomicUsize,
    reject_reads: AtomicBool,
    reject_writes: AtomicBool,
    reject_sync: AtomicBool,
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
        if self.gate.hold_reads.load(Ordering::SeqCst) {
            self.gate.reads_entered.fetch_add(1, Ordering::SeqCst);
            while self.gate.hold_reads.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
        }
        if self.gate.reject_reads.load(Ordering::SeqCst) {
            return Err(refusal("read rejected by test"));
        }
        self.file.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.gate.entered.fetch_add(1, Ordering::SeqCst);
        while !self.gate.open.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        if self.gate.reject_writes.load(Ordering::SeqCst) {
            return Err(refusal("write rejected by test"));
        }
        let r = self.file.write_all_at(buf, offset);
        self.gate.completed.fetch_add(1, Ordering::SeqCst);
        r
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        if self.gate.reject_sync.load(Ordering::SeqCst) {
            return Err(refusal("flush rejected by test"));
        }
        self.file.sync_data()
    }

    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            gate: self.gate,
        })
    }
}

fn refusal(op: &'static str) -> DiskError {
    DiskError::Io {
        op,
        path: std::path::PathBuf::new(),
        source: std::io::Error::other(op),
    }
}

fn native_written() -> (tempfile::TempDir, &'static Gate, Store<Gated>, u64) {
    let dir = tempfile::tempdir().unwrap();
    let gate: &'static Gate = Box::leak(Box::default());
    gate.open.store(true, Ordering::SeqCst);
    let file = Gated {
        file: DeviceFile::open(
            &dir.path().join("store"),
            true,
            CachingRequest::Buffered,
            Alignment::new(CONFIG.page_size).unwrap(),
        )
        .unwrap(),
        gate,
    };
    let mut store = Store::create(file, CONFIG).unwrap();
    let extent = store.allocate_extent().unwrap();
    let address = store.address(extent, 0).unwrap();
    store
        .write_page(address, b"last successful contents")
        .unwrap();
    store.checkpoint(Some(address), 1).unwrap();
    store.set_cache(CONFIG.extent_pages as usize);
    (dir, gate, store, address)
}

/// Releases device operations before the issuer is dropped, even if a readiness check fails.
struct OpenOnDrop(&'static Gate);

impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        self.0.open.store(true, Ordering::SeqCst);
        self.0.hold_reads.store(false, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy)]
enum Memory {
    Cached,
    Queued,
}

/// A page already available in memory, while the device cannot complete the next operation.
fn memory_ready(memory: Memory, pending: bool) {
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
    if matches!(memory, Memory::Cached) {
        store.set_cache(2 * CONFIG.extent_pages as usize);
    }
    let mut run = store.run().unwrap();
    let extent = store.allocate_extent().unwrap();
    let target = store.address(extent, 0).unwrap();
    for n in 0..CONFIG.extent_pages {
        let a = store.address(extent, n).unwrap();
        store.queue_page(&mut run, a, &payload(a)).unwrap();
    }
    store.drain().unwrap();
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    store.set_write_budget(CONFIG.page_size * CONFIG.extent_pages as usize);
    let _open = OpenOnDrop(gate);
    let mut span = store.span_sequential().unwrap();
    if pending {
        // Start the read while uncached, then make the same target bytes available in memory.
        store.set_cache(0);
        gate.hold_reads.store(true, Ordering::SeqCst);
        store.prefetch(&mut span, target).unwrap();
        while gate.reads_entered.load(Ordering::SeqCst) == 0 {
            std::thread::yield_now();
        }
        if matches!(memory, Memory::Cached) {
            store.set_cache(2 * CONFIG.extent_pages as usize);
            store
                .queue_page(&mut run, target, &payload(target))
                .unwrap();
        }
    } else {
        // The one batch and worker are occupied by a write elsewhere.
        gate.open.store(false, Ordering::SeqCst);
        let before = gate.entered.load(Ordering::SeqCst);
        let busy = store.allocate_extent().unwrap();
        let a = store.address(busy, 0).unwrap();
        store.queue_page(&mut run, a, &payload(a)).unwrap();
        store.write_run(&mut run).unwrap();
        while gate.entered.load(Ordering::SeqCst) == before {
            std::thread::yield_now();
        }
    }
    if matches!(memory, Memory::Queued) {
        // The same bytes, sealed in write memory, can be read before this run is submitted.
        store
            .queue_page(&mut run, target, &payload(target))
            .unwrap();
        store.write_run(&mut run).unwrap();
    }
    assert_eq!(store.ready(&mut span, target).unwrap(), !pending);
    if pending {
        // A pending target is still claimed first by the reader: do not label it ready early.
        gate.hold_reads.store(false, Ordering::SeqCst);
    }
    let mut out = Vec::new();
    store.read_page_ahead(&mut span, target, &mut out).unwrap();
    assert_eq!(out, payload(target));
    gate.open.store(true, Ordering::SeqCst);
    store.write_run(&mut run).unwrap();
    store.give_span(span);
    store.give_run(run);
    store.drain().unwrap();
}

#[test]
fn a_cached_page_is_ready_while_the_device_is_busy() {
    memory_ready(Memory::Cached, false);
}

#[test]
fn a_queued_page_is_ready_while_the_device_is_busy() {
    memory_ready(Memory::Queued, false);
}

#[test]
fn a_cached_page_with_a_pending_read_is_not_ready_early() {
    memory_ready(Memory::Cached, true);
}

#[test]
fn a_queued_page_with_a_pending_read_is_not_ready_early() {
    memory_ready(Memory::Queued, true);
}

/// The builder can evict a cache target or submit a queued target between a merge's
/// readiness check and its next read. The promised page remains readable without I/O.
fn ready_survives_write_progress(memory: Memory) {
    let dir = tempfile::tempdir().unwrap();
    let gate: &'static Gate = Box::leak(Box::default());
    gate.open.store(true, Ordering::SeqCst);
    let file = Gated {
        file: DeviceFile::open(
            &dir.path().join("store"),
            true,
            CachingRequest::Buffered,
            Alignment::new(CONFIG.page_size).unwrap(),
        )
        .unwrap(),
        gate,
    };
    let mut store = Store::create(file, CONFIG).unwrap();
    if matches!(memory, Memory::Cached) {
        store.set_cache(CONFIG.extent_pages as usize);
    }
    let mut run = store.run().unwrap();
    let extent = store.allocate_extent().unwrap();
    let target = store.address(extent, 0).unwrap();
    store
        .queue_page(&mut run, target, &payload(target))
        .unwrap();
    store.write_run(&mut run).unwrap();
    store.drain().unwrap();
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    store.set_write_budget(CONFIG.page_size * CONFIG.extent_pages as usize);
    let _open = OpenOnDrop(gate);
    gate.open.store(false, Ordering::SeqCst);
    let entered = gate.entered.load(Ordering::SeqCst);
    let busy = store.allocate_extent().unwrap();
    let address = store.address(busy, 0).unwrap();
    store
        .queue_page(&mut run, address, &payload(address))
        .unwrap();
    store.write_run(&mut run).unwrap();
    while gate.entered.load(Ordering::SeqCst) == entered {
        std::thread::yield_now();
    }
    if matches!(memory, Memory::Queued) {
        store
            .queue_page(&mut run, target, &payload(target))
            .unwrap();
        store.write_run(&mut run).unwrap();
    }
    let mut span = store.span_sequential().unwrap();
    assert!(store.ready(&mut span, target).unwrap());
    if matches!(memory, Memory::Cached) {
        // Filling the remaining run admits more pages than the configured cache holds.
        let next = store.allocate_extent().unwrap();
        for page in 0..CONFIG.extent_pages.saturating_sub(1) {
            let address = store.address(next, page).unwrap();
            store
                .queue_page(&mut run, address, &payload(address))
                .unwrap();
        }
    } else {
        // An answer lets the sealed queued target leave write memory.
        gate.open.store(true, Ordering::SeqCst);
        store.drain().unwrap();
    }
    gate.reject_reads.store(true, Ordering::SeqCst);
    let mut out = b"prefix ".to_vec();
    store.read_page_ahead(&mut span, target, &mut out).unwrap();
    let mut expected = b"prefix ".to_vec();
    expected.extend_from_slice(&payload(target));
    assert_eq!(out, expected);
    gate.open.store(true, Ordering::SeqCst);
    store.give_span(span);
    store.write_run(&mut run).unwrap();
    store.give_run(run);
    store.drain().unwrap();
}

#[test]
fn a_ready_cached_page_survives_write_admission_eviction() {
    ready_survives_write_progress(Memory::Cached);
}

#[test]
fn a_ready_queued_page_survives_write_submission() {
    ready_survives_write_progress(Memory::Queued);
}

#[test]
fn a_memory_ready_cursor_keeps_its_current_value_until_it_moves() {
    let dir = tempfile::tempdir().unwrap();
    let gate: &'static Gate = Box::leak(Box::default());
    gate.open.store(true, Ordering::SeqCst);
    let file = Gated {
        file: DeviceFile::open(
            &dir.path().join("store"),
            true,
            CachingRequest::Buffered,
            Alignment::new(CONFIG.page_size).unwrap(),
        )
        .unwrap(),
        gate,
    };
    let config = Config {
        extent_pages: 2,
        ..CONFIG
    };
    let mut store = Store::create(file, config).unwrap();
    let cache_pages = 2 * CONFIG.extent_pages as usize;
    store.set_cache(cache_pages);
    let mut builder = Builder::new(&mut store, Keys::Exactly(3)).unwrap();
    // Two values cannot fit in a page even before the node's header and entry table.
    let values = [
        vec![1; CONFIG.page_size / 2],
        vec![2; CONFIG.page_size / 2],
        vec![3; CONFIG.page_size / 2],
    ];
    for (key, value) in [b"a", b"b", b"c"].into_iter().zip(&values) {
        builder.add(&mut store, key, Op::Put, value).unwrap();
    }
    let branch = builder.finish(&mut store).unwrap();
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    store.set_write_budget(config.page_size * config.extent_pages as usize);
    let _open = OpenOnDrop(gate);
    gate.reject_reads.store(true, Ordering::SeqCst);
    let mut cursor = branch.seek_sequential(&mut store, b"a").unwrap();
    assert!(cursor.next_ready(&branch, &mut store).unwrap());
    assert_eq!(cursor.key(), b"a");
    assert_eq!(cursor.value(), values[0]);
    // The intervening builder work admits a cache's worth of other pages.
    let mut run = store.run().unwrap();
    for _ in 0..cache_pages / config.extent_pages as usize {
        let extent = store.allocate_extent().unwrap();
        for page in 0..config.extent_pages {
            let address = store.address(extent, page).unwrap();
            store
                .queue_page(&mut run, address, &payload(address))
                .unwrap();
        }
    }
    assert_eq!(cursor.key(), b"a");
    assert_eq!(cursor.value(), values[0]);
    cursor.next(&branch, &mut store).unwrap();
    assert_eq!(cursor.key(), b"b");
    assert_eq!(cursor.value(), values[1]);
    gate.reject_reads.store(false, Ordering::SeqCst);
    cursor.give_back(&mut store);
    store.give_run(run);
    store.drain().unwrap();
}

/// Readiness held bytes before submission; a later known write/flush failure discards them.
fn failed_prepared_page_is_not_served(write: bool) {
    let (dir, gate, mut store, address) = native_written();
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    let _open = OpenOnDrop(gate);
    let mut run = store.run().unwrap();
    store
        .queue_page(&mut run, address, b"failed prepared bytes")
        .unwrap();
    let mut span = store.span_sequential().unwrap();
    assert!(store.ready(&mut span, address).unwrap());
    gate.reject_writes.store(write, Ordering::SeqCst);
    gate.reject_sync.store(!write, Ordering::SeqCst);
    store.write_run(&mut run).unwrap();
    assert!(matches!(
        store.checkpoint(Some(address), 2),
        Err(mantle_engine::Error::Io { .. })
    ));
    gate.reject_reads.store(true, Ordering::SeqCst);
    assert!(matches!(
        store.read_page_ahead(&mut span, address, &mut Vec::new()),
        Err(mantle_engine::Error::Io { .. })
    ));
    store.give_span(span);
    store.give_run(run);
}

#[test]
fn a_prepared_page_is_discarded_after_a_failed_write() {
    failed_prepared_page_is_not_served(true);
}

#[test]
fn a_prepared_page_is_discarded_after_a_failed_flush() {
    failed_prepared_page_is_not_served(false);
}

#[test]
fn a_span_reposition_does_not_reuse_a_prepared_page_for_another_target() {
    let (dir, _, mut store, address) = native_written();
    let other_extent = store.allocate_extent().unwrap();
    let other = store.address(other_extent, 0).unwrap();
    store.write_page(other, b"another target").unwrap();
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    let mut run = store.run().unwrap();
    store
        .queue_page(&mut run, address, b"first target")
        .unwrap();
    let mut span = store.span_sequential().unwrap();
    assert!(store.ready(&mut span, address).unwrap());
    let mut out = b"prefix ".to_vec();
    store.read_page_ahead(&mut span, other, &mut out).unwrap();
    assert_eq!(out, b"prefix another target");
    store
        .queue_page(&mut run, address, b"updated target")
        .unwrap();
    assert!(store.ready(&mut span, address).unwrap());
    out.clear();
    store.read_page_ahead(&mut span, address, &mut out).unwrap();
    assert_eq!(out, b"updated target");
    store.give_span(span);
    store.write_run(&mut run).unwrap();
    store.give_run(run);
    store.drain().unwrap();
}

#[test]
fn a_stopped_issuer_refuses_a_prefetch_and_preserves_the_checkpoint() {
    let (dir, _, mut store, address) = native_written();
    store.set_cache(0);
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    drop(issuer);
    let mut span = store.span_sequential().unwrap();
    assert!(matches!(
        store.prefetch(&mut span, address),
        Err(mantle_engine::Error::Io {
            op: "hand a store page span read to the issuer",
            ..
        })
    ));
    store.give_span(span);
    let (file, _) = store.into_file();
    let (mut recovered, checkpoint) = Store::open(file, CONFIG).unwrap();
    assert_eq!(checkpoint.applied, 1);
    let mut out = Vec::new();
    recovered.read_page(address, &mut out).unwrap();
    assert_eq!(out, b"last successful contents");
}

fn payload(address: u64) -> Vec<u8> {
    let mut v = format!("page {address} ").into_bytes();
    v.resize(64 + (address as usize * 31) % 900, address as u8);
    v
}

fn delayed_read_lookahead(alternating: bool) {
    let (dir, gate, mut store, target) = native_written();
    store.set_cache(0);
    let mut run = store.run().unwrap();
    let mut future = Vec::new();
    for _ in 0..CONFIG.extent_pages {
        let extent = store.allocate_extent().unwrap();
        let address = store.address(extent, 0).unwrap();
        future.push(address);
        for page in 0..CONFIG.extent_pages {
            let address = store.address(extent, page).unwrap();
            store
                .queue_page(&mut run, address, &payload(address))
                .unwrap();
        }
    }
    store.write_run(&mut run).unwrap();
    store.drain().unwrap();
    let batches = CONFIG.extent_pages as usize;
    let issuer = Issuer::start_for(dir.path(), batches, batches).unwrap();
    store.attach(&issuer, batches).unwrap();
    let _open = OpenOnDrop(gate);
    gate.hold_reads.store(true, Ordering::SeqCst);
    let mut span = store.span_sequential().unwrap();
    store.prefetch(&mut span, target).unwrap();
    while gate.reads_entered.load(Ordering::SeqCst) == 0 {
        std::thread::yield_now();
    }
    // The same submitted read is late throughout these polls; a write elsewhere can finish.
    let before = gate.completed.load(Ordering::SeqCst);
    let extent = store.allocate_extent().unwrap();
    let written = store.address(extent, 0).unwrap();
    store
        .queue_page(&mut run, written, &payload(written))
        .unwrap();
    store.write_run(&mut run).unwrap();
    for _ in 0..batches * CONFIG.extent_pages as usize {
        assert!(!store.ready(&mut span, target).unwrap());
    }
    while gate.completed.load(Ordering::SeqCst) == before {
        std::thread::yield_now();
    }
    if alternating {
        store.prefetch(&mut span, future[0]).unwrap();
        while gate.reads_entered.load(Ordering::SeqCst) < 2 {
            std::thread::yield_now();
        }
        for _ in 0..batches {
            assert!(!store.ready(&mut span, target).unwrap());
            assert!(!store.ready(&mut span, future[0]).unwrap());
        }
    }
    for &address in &future {
        store.prefetch(&mut span, address).unwrap();
    }
    // Each distinct late read permits one extent; polling the same reads adds none.
    assert_eq!(store.io_stats().prefetches, if alternating { 3 } else { 2 });
    gate.hold_reads.store(false, Ordering::SeqCst);
    let mut out = Vec::new();
    store.read_page_ahead(&mut span, target, &mut out).unwrap();
    assert_eq!(out, b"last successful contents");
    for &address in &future {
        out.clear();
        store.read_page_ahead(&mut span, address, &mut out).unwrap();
        assert_eq!(out, payload(address));
    }
    store.give_span(span);
    // A required read still propagates its device failure, and the span can be returned.
    gate.reject_reads.store(true, Ordering::SeqCst);
    let mut failed = store.span_sequential().unwrap();
    store.prefetch(&mut failed, target).unwrap();
    assert!(matches!(
        store.read_page_ahead(&mut failed, target, &mut Vec::new()),
        Err(mantle_engine::Error::Io { .. })
    ));
    store.give_span(failed);
    gate.reject_reads.store(false, Ordering::SeqCst);
    store.give_run(run);
    store.drain().unwrap();
    out.clear();
    store.read_page(written, &mut out).unwrap();
    assert_eq!(out, payload(written));
}

#[test]
fn polling_one_delayed_read_does_not_expand_its_lookahead() {
    delayed_read_lookahead(false);
}

#[test]
fn alternating_polls_of_delayed_reads_do_not_expand_their_lookahead() {
    delayed_read_lookahead(true);
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

fn runtime() -> LocalRuntime {
    // The same one-shard fixture as hyper-rt's channel tests, with room for a waiter and
    // the independent task that opens the device gate.
    LocalRuntime::new(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: 64,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: CONFIG.page_size,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap()
}

/// A request wins the race with a held read without consuming its completion. A different
/// task can then open the gate while the Store waits, and the original span keeps its bytes.
#[test]
fn a_request_interrupts_a_completion_wait_without_losing_the_read() {
    let (dir, gate, mut store, address) = native_written();
    store.set_cache(0);
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    let open = OpenOnDrop(gate);
    gate.hold_reads.store(true, Ordering::SeqCst);
    let mut span = store.span_sequential().unwrap();
    store.prefetch(&mut span, address).unwrap();
    while gate.reads_entered.load(Ordering::SeqCst) == 0 {
        std::thread::yield_now();
    }
    let mut rt = runtime();
    let mut store = rt
        .block_on(async move {
            let _open = open;
            {
                let mut wait = std::pin::pin!(store.wait_completion());
                let waker = poll_fn(|cx| {
                    assert!(wait.as_mut().poll(cx).is_pending());
                    assert!(wait.as_mut().poll(cx).is_pending());
                    Poll::Ready(cx.waker().clone())
                })
                .await;
                waker.wake_by_ref();
                let (request, mut requests) = hyper_rt::sync::channel(1).unwrap();
                request.try_send(b"another request").unwrap();
                assert!(matches!(
                    race2(requests.recv(), wait.as_mut()).await,
                    Either::First(Ok(b"another request"))
                ));
            }
            assert!(!store.ready(&mut span, address).unwrap());
            hyper_rt::futures::spawn_detached(async move {
                gate.hold_reads.store(false, Ordering::SeqCst);
            })
            .unwrap();
            assert!(store.wait_completion().await.unwrap());
            assert!(store.ready(&mut span, address).unwrap());
            let mut out = b"prefix ".to_vec();
            store.read_page_ahead(&mut span, address, &mut out).unwrap();
            assert_eq!(out, b"prefix last successful contents");
            store.give_span(span);
            assert!(!store.wait_completion().await.unwrap());
            store
        })
        .unwrap();
    store.checkpoint(Some(address), 2).unwrap();
    let (file, landed) = store.into_file();
    landed.unwrap();
    let (mut recovered, checkpoint) = Store::open(file, CONFIG).unwrap();
    assert_eq!(checkpoint.applied, 2);
    let mut out = Vec::new();
    recovered.read_page(address, &mut out).unwrap();
    assert_eq!(out, b"last successful contents");
}

/// Returning a span after cancellation abandons its read, not its loan. The later answer
/// can be pooled, and the same Store continues serving fresh reads and writes.
#[test]
fn a_cancelled_completion_wait_can_return_its_span_before_the_read_lands() {
    let (dir, gate, mut store, address) = native_written();
    store.set_cache(0);
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    let open = OpenOnDrop(gate);
    gate.hold_reads.store(true, Ordering::SeqCst);
    let mut span = store.span_sequential().unwrap();
    store.prefetch(&mut span, address).unwrap();
    while gate.reads_entered.load(Ordering::SeqCst) == 0 {
        std::thread::yield_now();
    }
    let mut rt = runtime();
    let mut store = rt
        .block_on(async move {
            let _open = open;
            assert!(matches!(
                race2(store.wait_completion(), async {}).await,
                Either::Second(())
            ));
            store.give_span(span);
            hyper_rt::futures::spawn_detached(async move {
                gate.hold_reads.store(false, Ordering::SeqCst);
            })
            .unwrap();
            assert!(store.wait_completion().await.unwrap());
            assert!(!store.wait_completion().await.unwrap());
            let mut fresh = store.span_sequential().unwrap();
            while !store.ready(&mut fresh, address).unwrap() {
                assert!(store.wait_completion().await.unwrap());
            }
            let mut out = Vec::new();
            store
                .read_page_ahead(&mut fresh, address, &mut out)
                .unwrap();
            assert_eq!(out, b"last successful contents");
            store.give_span(fresh);
            store
        })
        .unwrap();
    store.write_page(address, b"continued service").unwrap();
    store.checkpoint(Some(address), 2).unwrap();
    let mut out = Vec::new();
    store.read_page(address, &mut out).unwrap();
    assert_eq!(out, b"continued service");
}

#[test]
fn a_failed_async_read_is_kept_for_its_span_and_does_not_fence_the_store() {
    let (dir, gate, mut store, address) = native_written();
    store.set_cache(0);
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    let open = OpenOnDrop(gate);
    gate.hold_reads.store(true, Ordering::SeqCst);
    gate.reject_reads.store(true, Ordering::SeqCst);
    let mut span = store.span_sequential().unwrap();
    store.prefetch(&mut span, address).unwrap();
    while gate.reads_entered.load(Ordering::SeqCst) == 0 {
        std::thread::yield_now();
    }
    let mut rt = runtime();
    let mut store = rt
        .block_on(async move {
            let _open = open;
            {
                let mut wait = std::pin::pin!(store.wait_completion());
                poll_fn(|cx| {
                    assert!(wait.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
            }
            hyper_rt::futures::spawn_detached(async move {
                gate.hold_reads.store(false, Ordering::SeqCst);
            })
            .unwrap();
            assert!(store.wait_completion().await.unwrap());
            assert!(matches!(
                store.read_page_ahead(&mut span, address, &mut Vec::new()),
                Err(mantle_engine::Error::Io {
                    op: "read a store page span ahead",
                    ..
                })
            ));
            store.give_span(span);
            gate.reject_reads.store(false, Ordering::SeqCst);
            let mut fresh = store.span_sequential().unwrap();
            while !store.ready(&mut fresh, address).unwrap() {
                assert!(store.wait_completion().await.unwrap());
            }
            let mut out = Vec::new();
            store
                .read_page_ahead(&mut fresh, address, &mut out)
                .unwrap();
            assert_eq!(out, b"last successful contents");
            store.give_span(fresh);
            store
        })
        .unwrap();
    store
        .write_page(address, b"after the repaired read")
        .unwrap();
    store.checkpoint(Some(address), 2).unwrap();
    let mut out = Vec::new();
    store.read_page(address, &mut out).unwrap();
    assert_eq!(out, b"after the repaired read");
}

/// Completion returns the single batch credit. Queued bytes remain readable while the
/// first write is held; subsequent work with no write budget still forwards and checkpoints.
#[test]
fn async_write_completions_advance_a_single_slot_queue_and_zero_budget_writes() {
    let (dir, gate, mut store, _) = native_written();
    store.set_cache(0);
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    store.set_write_budget(store.run_bytes());
    let open = OpenOnDrop(gate);
    let before = gate.entered.load(Ordering::SeqCst);
    gate.open.store(false, Ordering::SeqCst);
    let mut run = store.run().unwrap();
    let mut addresses = Vec::new();
    for _ in 0..2 {
        let extent = store.allocate_extent().unwrap();
        let address = store.address(extent, 0).unwrap();
        addresses.push(address);
        store
            .queue_page(&mut run, address, &payload(address))
            .unwrap();
        store.write_run(&mut run).unwrap();
    }
    while gate.entered.load(Ordering::SeqCst) == before {
        std::thread::yield_now();
    }
    let mut out = Vec::new();
    store.read_page(addresses[1], &mut out).unwrap();
    assert_eq!(out, payload(addresses[1]));
    let mut rt = runtime();
    let (mut store, run) = rt
        .block_on(async move {
            let _open = open;
            assert!(matches!(
                race2(store.wait_completion(), async {}).await,
                Either::Second(())
            ));
            hyper_rt::futures::spawn_detached(async move {
                gate.open.store(true, Ordering::SeqCst);
            })
            .unwrap();
            assert!(store.wait_completion().await.unwrap());
            for &address in &addresses {
                let mut span = store.span_sequential().unwrap();
                while !store.ready(&mut span, address).unwrap() {
                    assert!(store.wait_completion().await.unwrap());
                }
                out.clear();
                store.read_page_ahead(&mut span, address, &mut out).unwrap();
                assert_eq!(out, payload(address));
                store.give_span(span);
            }
            store.drain().unwrap();
            store.set_write_budget(0);
            let extent = store.allocate_extent().unwrap();
            let address = store.address(extent, 0).unwrap();
            store
                .queue_page(&mut run, address, &payload(address))
                .unwrap();
            store.write_run(&mut run).unwrap();
            assert!(store.wait_completion().await.unwrap());
            assert!(!store.wait_completion().await.unwrap());
            out.clear();
            store.read_page(address, &mut out).unwrap();
            assert_eq!(out, payload(address));
            store.checkpoint(Some(address), 2).unwrap();
            (store, run)
        })
        .unwrap();
    store.give_run(run);
    let (file, landed) = store.into_file();
    landed.unwrap();
    let (mut recovered, checkpoint) = Store::open(file, CONFIG).unwrap();
    assert_eq!(checkpoint.applied, 2);
    let address = checkpoint.root.unwrap();
    out = Vec::new();
    recovered.read_page(address, &mut out).unwrap();
    assert_eq!(out, payload(address));
}

#[test]
fn a_failed_async_write_fences_cached_bytes_and_preserves_the_previous_checkpoint() {
    let (dir, gate, mut store, address) = native_written();
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    let open = OpenOnDrop(gate);
    let before = gate.entered.load(Ordering::SeqCst);
    gate.open.store(false, Ordering::SeqCst);
    gate.reject_writes.store(true, Ordering::SeqCst);
    let mut run = store.run().unwrap();
    store
        .queue_page(&mut run, address, b"failed contents")
        .unwrap();
    store.write_run(&mut run).unwrap();
    while gate.entered.load(Ordering::SeqCst) == before {
        std::thread::yield_now();
    }
    let mut rt = runtime();
    let (mut store, run) = rt
        .block_on(async move {
            let _open = open;
            assert!(matches!(
                race2(store.wait_completion(), async {}).await,
                Either::Second(())
            ));
            hyper_rt::futures::spawn_detached(async move {
                gate.open.store(true, Ordering::SeqCst);
            })
            .unwrap();
            assert!(matches!(
                store.wait_completion().await,
                Err(mantle_engine::Error::Io {
                    op: "write a store page run",
                    ..
                })
            ));
            let mut span = store.span_sequential().unwrap();
            // Fencing discards admission/prepared bytes; durable reads may still succeed.
            store.ready(&mut span, address).unwrap();
            let mut out = Vec::new();
            store.read_page_ahead(&mut span, address, &mut out).unwrap();
            assert_eq!(out, b"last successful contents");
            out.clear();
            store.read_page(address, &mut out).unwrap();
            assert_eq!(out, b"last successful contents");
            assert!(matches!(
                store.write_page(address, b"another refused write"),
                Err(mantle_engine::Error::Io { .. })
            ));
            store.give_span(span);
            assert!(matches!(
                store.checkpoint(Some(address), 2),
                Err(mantle_engine::Error::Io { .. })
            ));
            (store, run)
        })
        .unwrap();
    store.give_run(run);
    gate.reject_writes.store(false, Ordering::SeqCst);
    let (file, landed) = store.into_file();
    assert!(landed.is_err());
    let (mut recovered, checkpoint) = Store::open(file, CONFIG).unwrap();
    assert_eq!(checkpoint.applied, 1);
    let mut out = Vec::new();
    recovered.read_page(address, &mut out).unwrap();
    assert_eq!(out, b"last successful contents");
}

#[test]
fn an_idle_or_stopped_issuer_has_no_completion_to_wait_for() {
    let (dir, _, mut store, address) = native_written();
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    drop(issuer);
    let mut rt = runtime();
    let mut store = rt
        .block_on(async move {
            assert!(!store.wait_completion().await.unwrap());
            let mut span = store.span_sequential().unwrap();
            store.set_cache(0);
            assert!(matches!(
                store.prefetch(&mut span, address),
                Err(mantle_engine::Error::Io { .. })
            ));
            store.give_span(span);
            assert!(!store.wait_completion().await.unwrap());
            store
        })
        .unwrap();
    let mut run = store.run().unwrap();
    store
        .queue_page(&mut run, address, b"refused write")
        .unwrap();
    assert!(matches!(
        store.write_run(&mut run),
        Err(mantle_engine::Error::Io { .. })
    ));
    store.give_run(run);
    let (file, _) = store.into_file();
    let (mut recovered, checkpoint) = Store::open(file, CONFIG).unwrap();
    assert_eq!(checkpoint.applied, 1);
    let mut out = Vec::new();
    recovered.read_page(address, &mut out).unwrap();
    assert_eq!(out, b"last successful contents");
}

#[test]
fn a_completion_wait_off_the_runtime_refuses_without_fencing_the_store() {
    let (dir, gate, mut store, address) = native_written();
    store.set_cache(0);
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    let _open = OpenOnDrop(gate);
    gate.hold_reads.store(true, Ordering::SeqCst);
    let mut span = store.span_sequential().unwrap();
    store.prefetch(&mut span, address).unwrap();
    while gate.reads_entered.load(Ordering::SeqCst) == 0 {
        std::thread::yield_now();
    }
    {
        let mut wait = std::pin::pin!(store.wait_completion());
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(matches!(
            wait.as_mut().poll(&mut cx),
            Poll::Ready(Err(mantle_engine::Error::Io { .. }))
        ));
    }
    gate.hold_reads.store(false, Ordering::SeqCst);
    store.drain().unwrap();
    let mut out = Vec::new();
    store.read_page_ahead(&mut span, address, &mut out).unwrap();
    assert_eq!(out, b"last successful contents");
    store.give_span(span);
    store
        .write_page(address, b"after the context refusal")
        .unwrap();
    store.checkpoint(Some(address), 2).unwrap();
    let (file, landed) = store.into_file();
    landed.unwrap();
    let (mut recovered, checkpoint) = Store::open(file, CONFIG).unwrap();
    assert_eq!(checkpoint.applied, 2);
    out.clear();
    recovered.read_page(address, &mut out).unwrap();
    assert_eq!(out, b"after the context refusal");
}
