//! A retained nonempty Run belongs to its accepted worker job, even when a later
//! job is granted exactly the same extent. An empty warm Run remains reusable.
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
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use mantle_engine::Error;
use mantle_engine::store::{Config, Store};
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// The existing public disjoint-grant oracle's native page/extent shape. The first
// of four pages remains borrowed rather than auto-dispatching a full extent.
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

fn open(path: &std::path::Path, writes: &Arc<AtomicUsize>) -> File {
    File {
        file: DeviceFile::open(
            path,
            true,
            CachingRequest::Buffered,
            Alignment::new(CONFIG.page_size).unwrap(),
        )
        .unwrap(),
        writes: Arc::clone(writes),
    }
}

#[test]
fn a_nonempty_run_cannot_cross_a_job_boundary_with_the_same_grant_but_an_empty_run_can() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("same-grant-run");
    let writes = Arc::new(AtomicUsize::new(0));
    let file = open(&path, &writes);
    let worker_file = file.try_clone().unwrap();
    let mut owner = Store::create(file, CONFIG).unwrap();
    let grant = owner.grant(1).unwrap();
    let mut worker = Store::worker(worker_file, CONFIG).unwrap();
    worker
        .begin_job(&grant, owner.end(), owner.generation())
        .unwrap();
    let old_extent = worker.allocate_extent().unwrap();
    let page = worker.address(old_extent, 0).unwrap();
    let second_page = worker.address(old_extent, 1).unwrap();
    let mut stale = worker.run().unwrap();
    let mut warm = worker.run().unwrap();
    worker
        .queue_page(&mut stale, page, b"old unsubmitted same-address page")
        .unwrap();
    let before = writes.load(Ordering::SeqCst);
    worker
        .begin_job(&grant, owner.end(), owner.generation())
        .unwrap();
    let current = worker.allocate_extent().unwrap();
    assert_eq!(current, old_extent, "same physical address is held again");
    let refused = worker.write_run(&mut stale);
    let no_stale_write = writes.load(Ordering::SeqCst) == before;
    worker.give_run(stale);

    // Complete healthy work and return every loan before the RED/GREEN verdict.
    worker
        .queue_page(&mut warm, page, b"accepted second job")
        .unwrap();
    worker.write_run(&mut warm).unwrap();
    worker.drain().unwrap();
    worker
        .begin_job(&grant, worker.end(), owner.generation())
        .unwrap();
    assert_eq!(worker.allocate_extent().unwrap(), old_extent);
    // This Run has actually written before the third job; empty reuse is valid.
    worker
        .queue_page(&mut warm, second_page, b"accepted third job")
        .unwrap();
    worker.write_run(&mut warm).unwrap();
    worker.drain().unwrap();
    worker.give_run(warm);
    owner.extend_end(worker.end());
    let mut got = Vec::new();
    owner.read_page(page, &mut got).unwrap();
    assert_eq!(got, b"accepted second job");
    got.clear();
    owner.read_page(second_page, &mut got).unwrap();
    assert_eq!(got, b"accepted third job");
    owner.release(old_extent).unwrap();
    assert!(
        matches!(refused, Err(Error::InvalidArgument { .. })),
        "nonempty prior-job Run was not typed-refused: {refused:?}"
    );
    assert!(no_stale_write, "old same-grant bytes reached the device");
}

#[test]
fn an_async_nonempty_run_cannot_cross_a_same_grant_job_but_empty_warm_reuse_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("async-same-grant-run");
    let writes = Arc::new(AtomicUsize::new(0));
    let file = open(&path, &writes);
    let worker_file = file.try_clone().unwrap();
    let mut owner = Store::create(file, CONFIG).unwrap();
    let grant = owner.grant(1).unwrap();
    let mut worker = Store::worker(worker_file, CONFIG).unwrap();
    let issuer = Issuer::start_for(&path, 1, 1).unwrap();
    assert_eq!(issuer.depth(), 1, "one actual device worker suffices");
    worker.attach(&issuer, 1).unwrap();
    worker
        .begin_job(&grant, owner.end(), owner.generation())
        .unwrap();
    let old_extent = worker.allocate_extent().unwrap();
    let page = worker.address(old_extent, 0).unwrap();
    let second_page = worker.address(old_extent, 1).unwrap();
    let mut stale = worker.run().unwrap();
    let mut warm = worker.run().unwrap();
    worker
        .queue_page(&mut stale, page, b"old async unsubmitted same-address page")
        .unwrap();
    let before = writes.load(Ordering::SeqCst);
    worker
        .begin_job(&grant, owner.end(), owner.generation())
        .unwrap();
    assert_eq!(worker.allocate_extent().unwrap(), old_extent);
    let observed = Arc::clone(&writes);
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
    let (mut worker, mut warm, refused, no_stale_write) = runtime
        .block_on(async move {
            let task = hyper_rt::futures::current_task().unwrap();
            let refused = {
                let mut write = pin!(worker.write_run_async(&mut stale));
                poll_fn(|cx| {
                    assert_eq!(hyper_rt::futures::current_task(), Some(task));
                    write.as_mut().poll(cx)
                })
                .await
            };
            // On the RED source, retire an erroneously accepted transfer before
            // checking device facts. Empty completion is an ordinary false result.
            while worker.wait_completion().await.unwrap() {}
            let no_stale_write = observed.load(Ordering::SeqCst) == before;
            worker.give_run(stale);
            worker
                .queue_page_async(&mut warm, page, b"accepted async second job")
                .await
                .unwrap();
            worker.write_run_async(&mut warm).await.unwrap();
            while worker.wait_completion().await.unwrap() {}
            (worker, warm, refused, no_stale_write)
        })
        .unwrap();
    worker
        .begin_job(&grant, worker.end(), owner.generation())
        .unwrap();
    assert_eq!(worker.allocate_extent().unwrap(), old_extent);
    let mut worker = runtime
        .block_on(async move {
            worker
                .queue_page_async(&mut warm, second_page, b"accepted async third job")
                .await
                .unwrap();
            worker.write_run_async(&mut warm).await.unwrap();
            while worker.wait_completion().await.unwrap() {}
            worker.give_run(warm);
            worker
        })
        .unwrap();
    worker.drain().unwrap();
    owner.extend_end(worker.end());
    let mut got = Vec::new();
    owner.read_page(page, &mut got).unwrap();
    assert_eq!(got, b"accepted async second job");
    got.clear();
    owner.read_page(second_page, &mut got).unwrap();
    assert_eq!(got, b"accepted async third job");
    owner.release(old_extent).unwrap();
    assert!(
        matches!(refused, Err(Error::InvalidArgument { .. })),
        "nonempty prior-job async Run was not typed-refused: {refused:?}"
    );
    assert!(
        no_stale_write,
        "old same-grant async bytes reached the device"
    );
}
