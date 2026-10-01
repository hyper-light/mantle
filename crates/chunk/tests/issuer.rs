//! The chunk store on its device's issuer (docs/design/chunk-store.md §4): batches that span
//! many segments write their regions on the issuer's workers, never on threads of their own,
//! never more at once than the issuer's depth, and a region that fails fails its batch before
//! any flush.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

mod common;

use std::path::Path;
use std::sync::{Condvar, Mutex};
use std::task::Waker;

use common::device::{Handle, SimDevice};
use common::{config, data, issuer, key, sim};
use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::issuer::Issuer;
use hyper_block::sim::Crash;
use mantle_chunk::{ChunkError, Config, Limits, Volume};

/// A simulated file that watches its writes: how many are in flight at once, the threads the
/// process runs while each is, the most written between two flushes, and, when armed, fails
/// the write that makes `fail_at` since the last flush. While held, every write waits: a test
/// holds the device while it submits a round, so the writer's batches are made of what was
/// queued, never of how the scheduler happened to interleave the submissions.
struct Watch {
    file: SimDevice,
    seen: Mutex<Seen>,
    held: Mutex<bool>,
    released: Condvar,
}

#[derive(Default)]
struct Seen {
    in_flight: usize,
    most_in_flight: usize,
    most_threads: usize,
    since_flush: usize,
    most_since_flush: usize,
    fail_at: Option<usize>,
    failed: bool,
    flushed_after_failure: bool,
}

impl Watch {
    fn new(file: SimDevice) -> Handle<Self> {
        Handle::new(Self {
            file,
            seen: Mutex::new(Seen::default()),
            held: Mutex::new(false),
            released: Condvar::new(),
        })
    }
    fn hold(&self) {
        *self.held.lock().unwrap() = true;
    }
    fn release(&self) {
        *self.held.lock().unwrap() = false;
        self.released.notify_all();
    }
}

impl BlockFile for Watch {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }
    fn len(&self) -> Result<u64, DiskError> {
        BlockFile::len(&*self.file)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.file.read_exact_at(buf, offset)
    }
    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        let mut held = self.held.lock().unwrap();
        while *held {
            held = self.released.wait(held).unwrap();
        }
        drop(held);
        let mut seen = self.seen.lock().unwrap();
        seen.in_flight += 1;
        seen.most_in_flight = seen.most_in_flight.max(seen.in_flight);
        seen.since_flush += 1;
        seen.most_since_flush = seen.most_since_flush.max(seen.since_flush);
        let fail = !seen.failed && seen.fail_at == Some(seen.since_flush);
        seen.failed |= fail;
        drop(seen);
        // The process's threads while this write is in flight: a thread started for another
        // region of the batch would be running now.
        let threads = hyper_block::threads::count().unwrap();
        let result = if fail {
            Err(DiskError::Io {
                op: "write",
                path: "watch".into(),
                source: std::io::Error::other("a region the test fails"),
            })
        } else {
            self.file.write_all_at(buf, offset)
        };
        let mut seen = self.seen.lock().unwrap();
        seen.most_threads = seen.most_threads.max(threads);
        seen.in_flight -= 1;
        result
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        let mut seen = self.seen.lock().unwrap();
        seen.since_flush = 0;
        seen.flushed_after_failure |= seen.failed;
        drop(seen);
        self.file.sync_data()
    }
}

/// 64 KiB segments, each holding one 40,000-byte chunk's record: every put of a batch opens a
/// segment of its own, so a batch of 32 puts writes 32 regions and its frame.
fn small_segments() -> Config {
    Config {
        segment_size: 64 << 10,
        limits: Limits {
            batch_requests: 64,
            batch_bytes: 4 << 20,
            fragments_per_chunk: 64,
        },
        ..config()
    }
}

const CHUNK: usize = 40_000;
const SIZE: u64 = 32 << 20;

/// Submits `n` puts from `first` at once, as many clients would, with the device held until
/// every one is queued, and waits for every answer; a put refused when submitted answers with
/// its refusal.
fn put_together(
    v: &Volume<Handle<Watch>>,
    file: &Watch,
    first: u64,
    n: u64,
) -> Vec<Result<(), ChunkError>> {
    file.hold();
    let answers: Vec<_> = (first..first + n)
        .map(|k| v.put_waking(key(k), &data(k, CHUNK), Waker::noop().clone()))
        .collect();
    file.release();
    answers
        .into_iter()
        .map(|a| a.and_then(|a| a.wait()))
        .collect()
}

/// Batches spanning many more regions than the issuer's depth run as many threads, as the OS
/// counts them, as the volume idle: the issuer's workers carry every region, and no more of
/// them are in flight than its depth. Run in a process of its own, so that no other test's
/// threads are counted.
#[test]
fn batches_spanning_many_regions_start_no_threads() {
    const ALONE: &str = "MANTLE_TEST_THREADS_ALONE";
    if std::env::var_os(ALONE).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "batches_spanning_many_regions_start_no_threads",
                "--test-threads=1",
                "--nocapture",
            ])
            .env(ALONE, "1")
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    let issuer = Issuer::start(Path::new("test device"), 4).unwrap();
    let file = Watch::new(sim(1));
    let v = Volume::format(&issuer, file.clone(), SIZE, small_segments()).unwrap();
    let idle = hyper_block::threads::count().unwrap();
    // The harness's thread and this test's, the writer and the cleaner, the issuer and its
    // workers.
    assert!(idle >= 4 + 1 + issuer.depth(), "{idle} threads");
    for round in 0..3 {
        for result in put_together(&v, &file, round * 32, 32) {
            result.unwrap();
        }
    }
    let seen = file.seen.lock().unwrap();
    eprintln!(
        "{idle} threads idle, {} while writing; {} writes between flushes at most, {} in \
         flight at most at depth {}",
        seen.most_threads,
        seen.most_since_flush,
        seen.most_in_flight,
        issuer.depth()
    );
    assert!(
        seen.most_since_flush >= 4 * issuer.depth(),
        "no batch spanned many regions: {} writes",
        seen.most_since_flush
    );
    assert!(seen.most_in_flight <= issuer.depth());
    assert_eq!(seen.most_threads, idle);
    drop(seen);
    for k in 0..96 {
        assert_eq!(v.read(&key(k), 0, CHUNK as u64).unwrap(), data(k, CHUNK));
    }
    v.close();
    assert_eq!(hyper_block::threads::count().unwrap(), idle - 2);
}

/// A batch whose third write fails, a region of a batch spanning many: the batch's puts are
/// refused, the volume is fenced, no flush follows the failure, and every put acknowledged
/// before it is there after a crash.
#[test]
fn a_failed_region_fails_its_batch() {
    let file = Watch::new(sim(2));
    let v = Volume::format(issuer(), file.clone(), SIZE, small_segments()).unwrap();
    v.put(key(1000), &data(1000, CHUNK)).unwrap();
    file.seen.lock().unwrap().fail_at = Some(3);
    let results = put_together(&v, &file, 0, 32);
    assert!(
        results
            .iter()
            .any(|r| matches!(r, Err(ChunkError::Device(_)))),
        "{results:?}"
    );
    assert!(v.is_fenced());
    let seen = file.seen.lock().unwrap();
    assert!(seen.failed);
    assert!(!seen.flushed_after_failure);
    drop(seen);
    let acknowledged: Vec<u64> = (0..32).filter(|&k| results[k as usize].is_ok()).collect();
    drop(v);
    file.file.crash(Crash::LoseAll).unwrap();
    let (v, _) = Volume::open(issuer(), file.clone(), small_segments()).unwrap();
    for k in acknowledged.into_iter().chain([1000]) {
        assert_eq!(v.read(&key(k), 0, CHUNK as u64).unwrap(), data(k, CHUNK));
    }
}
