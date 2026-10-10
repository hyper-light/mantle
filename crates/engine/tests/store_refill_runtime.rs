//! Public worker refill refusal before callback entry, with exact cold grant reuse.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::disallowed_types
)]

#[path = "support/into_file.rs"]
mod file_outcome;
use file_outcome::finished_file;

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_rt::runtime::interests_for;
use hyper_rt::{Runtime, RuntimeConfig};
use mantle_engine::Error;
use mantle_engine::store::{Config, Refill, Store};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};

/// Existing Store-native page shape; the two granted data extents determine the oracle.
const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 256,
};
#[derive(Debug)]
enum Fact {
    Entered,
    Returned,
}

struct ReleaseOnDrop(mpsc::SyncSender<()>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        // Failure cleanup only: either a permit is already queued, or this opens the
        // callback before Runtime joins. No successful verdict is reached on unwind.
        let _ = self.0.try_send(());
    }
}

#[test]
fn runtime_refill_refuses_before_callback_then_the_same_grant_resumes_cold() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let file = || {
        DeviceFile::open(
            &path,
            false,
            CachingRequest::Buffered,
            Alignment::new(CONFIG.page_size).unwrap(),
        )
        .unwrap()
    };
    let mut owner = Store::create(
        DeviceFile::open(
            &path,
            true,
            CachingRequest::Buffered,
            Alignment::new(CONFIG.page_size).unwrap(),
        )
        .unwrap(),
        CONFIG,
    )
    .unwrap();
    let first = owner.allocate_extent().unwrap();
    let second = owner.allocate_extent().unwrap();
    let mut worker = Store::worker(file(), CONFIG).unwrap();
    worker
        .begin_job(&[first], owner.end(), owner.generation())
        .unwrap();
    assert_eq!(worker.allocate_extent().unwrap(), first);
    let invoked = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&invoked);
    // One callback-entry fact and one completed-call fact; no cadence or sleep controls release.
    let (facts, observed) = mpsc::sync_channel(2);
    let callback_fact = facts.clone();
    let (release, allowed) = mpsc::sync_channel(1);
    worker.set_refill(Refill(Box::new(move |_| {
        calls.fetch_add(1, Ordering::SeqCst);
        callback_fact.try_send(Fact::Entered).unwrap();
        allowed.recv().map_err(|error| Error::Io {
            op: "release native refill oracle",
            detail: error.to_string(),
        })?;
        Ok(vec![second])
    })));
    let roles = 1; // One actual runtime call; the release is the cold test controller.
    let rt = Runtime::start(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: roles,
        timers_per_shard: roles,
        interests_per_shard: interests_for(roles),
        ring_entries: roles,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: roles,
        pin: false,
        cores: Vec::new(),
        page_bytes: CONFIG.page_size,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap();
    // Declared after Runtime so any controller assertion opens the callback first.
    let release = ReleaseOnDrop(release);
    let (back, returned) = mpsc::sync_channel(1);
    rt.spawn_on(rt.shard_ids()[0], async move {
        let result = worker.allocate_extent();
        let callback_count = invoked.load(Ordering::SeqCst);
        facts.try_send(Fact::Returned).unwrap();
        back.try_send((worker, result, callback_count)).unwrap();
    })
    .unwrap();
    while let Fact::Entered = observed.recv().unwrap() {
        release.0.try_send(()).unwrap();
    }
    let (mut worker, result, callback_count) = returned.recv().unwrap();
    let refused = matches!(result, Err(Error::InvalidArgument { .. }));
    let extent = match result {
        Ok(extent) => extent, // Historical baseline still receives full cleanup before RED.
        Err(_) => {
            release.0.try_send(()).unwrap();
            worker.allocate_extent().unwrap()
        }
    };
    assert_eq!(extent, second);
    let address = worker.address(extent, 0).unwrap();
    let mut run = worker.run().unwrap();
    worker
        .queue_page(&mut run, address, b"same retained refill grant")
        .unwrap();
    worker.write_run(&mut run).unwrap();
    worker.give_run(run);
    worker.drain().unwrap();
    let mut value = Vec::new();
    worker.read_page(address, &mut value).unwrap();
    assert_eq!(value, b"same retained refill grant");
    drop(worker);
    rt.shutdown().unwrap();
    owner.release(first).unwrap();
    owner.release(second).unwrap();
    owner.checkpoint(None, 1).unwrap();
    let (_, landed) = finished_file(owner.into_file());
    landed.unwrap();
    let (reopened, recovered) = Store::open(file(), CONFIG).unwrap();
    assert_eq!(recovered.applied, 1);
    assert!(recovered.root.is_none());
    drop(reopened);
    assert!(
        refused,
        "the runtime allocation entered a blocking worker callback"
    );
    assert_eq!(
        callback_count, 0,
        "refusal changed callback/extent ownership before returning"
    );
}
