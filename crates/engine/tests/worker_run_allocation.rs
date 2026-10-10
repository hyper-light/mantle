//! A borrowed nonempty Run cannot write outside a worker's current grant after a job change.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]
use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_rt::RuntimeConfig;
use hyper_rt::runtime::{LocalRuntime, interests_for};
use mantle_engine::Error;
use mantle_engine::store::{Config, Store};
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 64,
};
struct File {
    file: DeviceFile,
    writes: Arc<AtomicUsize>,
}
impl BlockFile for File {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }
    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }
    fn read_exact_at(&self, buf: &mut [u8], at: u64) -> Result<(), DiskError> {
        self.file.read_exact_at(buf, at)
    }
    fn write_all_at(&self, buf: &[u8], at: u64) -> Result<(), DiskError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.file.write_all_at(buf, at)
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            writes: Arc::clone(&self.writes),
        })
    }
}
#[test]
fn a_run_from_a_previous_grant_is_refused_before_any_write_and_a_current_run_still_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("grant-run");
    let writes = Arc::new(AtomicUsize::new(0));
    let file = File {
        file: DeviceFile::open(
            &path,
            true,
            CachingRequest::Buffered,
            Alignment::new(CONFIG.page_size).unwrap(),
        )
        .unwrap(),
        writes: Arc::clone(&writes),
    };
    let worker_file = file.try_clone().unwrap();
    let mut owner = Store::create(file, CONFIG).unwrap();
    let old = owner.grant(1).unwrap();
    let next = owner.grant(1).unwrap();
    let mut worker = Store::worker(worker_file, CONFIG).unwrap();
    worker
        .begin_job(&old, owner.end(), owner.generation())
        .unwrap();
    let old_extent = worker.allocate_extent().unwrap();
    // The first page does not end its extent: it stays unsubmitted in the borrowed Run.
    let old_page = worker.address(old_extent, 0).unwrap();
    let mut old_run = worker.run().unwrap();
    worker
        .queue_page(&mut old_run, old_page, b"old unsubmitted page")
        .unwrap();
    let before = writes.load(Ordering::SeqCst);
    worker
        .begin_job(&next, owner.end(), owner.generation())
        .unwrap();
    let current = worker.allocate_extent().unwrap();
    assert!(next.contains(&current) && !old.contains(&current));
    let refused = worker.write_run(&mut old_run);
    assert!(
        matches!(refused, Err(Error::InvalidArgument { .. })),
        "{refused:?}"
    );
    assert_eq!(
        writes.load(Ordering::SeqCst),
        before,
        "no old-grant physical write"
    );
    worker.give_run(old_run);
    let current_page = worker.address(current, 0).unwrap();
    let payload = vec![7; CONFIG.page_size / 3];
    let mut fresh = worker.run().unwrap();
    worker
        .queue_page(&mut fresh, current_page, &payload)
        .unwrap();
    worker.write_run(&mut fresh).unwrap();
    worker.drain().unwrap();
    assert!(
        writes.load(Ordering::SeqCst) > before,
        "healthy path really writes"
    );
    worker.give_run(fresh);
    owner.extend_end(worker.end());
    let mut got = Vec::new();
    owner.read_page(current_page, &mut got).unwrap();
    assert_eq!(got, payload);
    owner.release(old_extent).unwrap();
}

#[test]
fn an_async_run_from_a_previous_grant_is_refused_on_its_actual_task_and_a_current_run_round_trips()
{
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("async-grant-run");
    let writes = Arc::new(AtomicUsize::new(0));
    let file = File {
        file: DeviceFile::open(
            &path,
            true,
            CachingRequest::Buffered,
            Alignment::new(CONFIG.page_size).unwrap(),
        )
        .unwrap(),
        writes: Arc::clone(&writes),
    };
    let worker_file = file.try_clone().unwrap();
    let mut owner = Store::create(file, CONFIG).unwrap();
    let old = owner.grant(1).unwrap();
    let next = owner.grant(1).unwrap();
    let mut worker = Store::worker(worker_file, CONFIG).unwrap();
    let issuer = Issuer::start_for(&path, 1, 1).unwrap();
    assert_eq!(issuer.depth(), 1, "one actual device worker suffices");
    worker.attach(&issuer, 1).unwrap();
    worker
        .begin_job(&old, owner.end(), owner.generation())
        .unwrap();
    let old_extent = worker.allocate_extent().unwrap();
    let old_page = worker.address(old_extent, 0).unwrap();
    let mut old_run = worker.run().unwrap();
    worker
        .queue_page(&mut old_run, old_page, b"old async unsubmitted page")
        .unwrap();
    worker
        .begin_job(&next, owner.end(), owner.generation())
        .unwrap();
    let current = worker.allocate_extent().unwrap();
    assert!(next.contains(&current) && !old.contains(&current));
    let page = worker.address(current, 0).unwrap();
    let payload = vec![11; CONFIG.page_size / 3];
    let before = writes.load(Ordering::SeqCst);
    let observed = Arc::clone(&writes);
    // One root task uses the existing native fixture's portable ring/timing; no timer is
    // armed and both output and completion use the one declared attachment credit.
    let mut runtime = LocalRuntime::new(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: 1,
        timers_per_shard: 0,
        interests_per_shard: interests_for(1),
        ring_entries: 64,
        batch: 1,
        step_budget_ns: 50_000,
        timer_tick_ns: 100_000,
        pin: false,
        cores: Vec::new(),
        page_bytes: CONFIG.page_size,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap();
    let mut worker = runtime
        .block_on(async move {
            let task = hyper_rt::futures::current_task().unwrap();
            {
                let mut stale = pin!(worker.write_run_async(&mut old_run));
                let refused = poll_fn(|cx| {
                    assert_eq!(hyper_rt::futures::current_task(), Some(task));
                    stale.as_mut().poll(cx)
                })
                .await;
                assert!(
                    matches!(refused, Err(Error::InvalidArgument { .. })),
                    "{refused:?}"
                );
            }
            assert_eq!(
                observed.load(Ordering::SeqCst),
                before,
                "no old-grant physical write"
            );
            worker.give_run(old_run);
            let mut fresh = worker.run().unwrap();
            worker
                .queue_page_async(&mut fresh, page, &payload)
                .await
                .unwrap();
            {
                let mut current = pin!(worker.write_run_async(&mut fresh));
                poll_fn(|cx| {
                    assert_eq!(hyper_rt::futures::current_task(), Some(task));
                    current.as_mut().poll(cx)
                })
                .await
                .unwrap();
            }
            assert!(worker.wait_completion().await.unwrap());
            assert!(
                observed.load(Ordering::SeqCst) > before,
                "healthy path really writes"
            );
            worker.give_run(fresh);
            worker
        })
        .unwrap();
    worker.drain().unwrap();
    owner.extend_end(worker.end());
    let mut got = Vec::new();
    owner.read_page(page, &mut got).unwrap();
    assert_eq!(got, vec![11; CONFIG.page_size / 3]);
    owner.release(old_extent).unwrap();
}
