//! A measurement at the device's full reported depth runs no more threads than its pool, as the
//! operating system counts them, and a depth past the process's thread budget starts none
//! (docs/design/measurement.md §8; research/26 §8, tests 1, 4 and 5). One test, so that no
//! other test's threads share the process's count.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use hyper_block::DiskError;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::UNDESCRIBED_QUEUE_DEPTH;
use hyper_block::threads;
use mantle_disk::measure::{self, Job, Pattern};

#[test]
fn a_full_depth_measurement_stays_within_its_pool_and_past_the_budget_starts_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let id = mantle_disk::probe::identify(dir.path());
    let queue = id
        .queue_depth
        .and_then(|q| usize::try_from(q).ok())
        .filter(|&q| q > 0)
        .unwrap_or(UNDESCRIBED_QUEUE_DEPTH);
    let depth = queue.min(threads::left().unwrap());
    let file = DeviceFile::open(
        &dir.path().join("depth"),
        true,
        CachingRequest::PreferDirect,
        Alignment::new(4096).unwrap(),
    )
    .unwrap();
    file.preallocate(4 << 20).unwrap();
    let job = Job {
        pattern: Pattern::RandomRead,
        block: 4096,
        depth,
        base: 0,
        span: 4 << 20,
        budget: None,
        max_ops: u64::try_from(depth).unwrap() * 64,
        sync_each: false,
        seed: 3,
    };

    // A depth past what the budget has left draws nothing and starts no thread.
    let before = threads::count().unwrap();
    let past = threads::left().unwrap() + 1;
    let err = measure::with_pool(&file, past, |pool| {
        pool.run(&Job {
            depth: past,
            ..job.clone()
        })
    })
    .unwrap_err();
    assert!(matches!(err, DiskError::Threads { asked, .. } if asked == past));
    assert_eq!(
        threads::count().unwrap(),
        before,
        "a refused pool started threads"
    );

    // The OS's count, sampled throughout by one thread of the test's own, never passes the
    // pool's workers.
    let (stop, peak) = (AtomicBool::new(false), AtomicUsize::new(0));
    let result = std::thread::scope(|s| {
        s.spawn(|| {
            while !stop.load(Ordering::Acquire) {
                peak.fetch_max(threads::count().unwrap(), Ordering::Relaxed);
                std::thread::yield_now();
            }
        });
        let result = measure::run(&file, &job);
        stop.store(true, Ordering::Release);
        result
    })
    .unwrap();
    let peak = peak.into_inner();
    eprintln!(
        "depth {depth} (device queue {queue}): threads {before} before, peak {peak}; \
         achieved {:.1} mean, {} most",
        result.achieved, result.achieved_max
    );
    assert!(
        peak <= before + depth + 1,
        "{peak} threads for a pool of {depth} and a sampler, {before} before"
    );
    assert_eq!(result.ops, u64::try_from(depth).unwrap() * 64);
    assert_eq!(result.depth, depth);
    // The depth achieved is reported, never more than asked.
    assert!(result.achieved > 0.0 && result.achieved <= depth as f64);
    assert!((1..=depth).contains(&result.achieved_max));
}
