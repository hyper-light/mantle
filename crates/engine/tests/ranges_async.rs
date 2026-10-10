//! A Range and its client progress on the same native hyper-rt shard.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_rt::{Runtime, RuntimeConfig};
use mantle_engine::Error;
use mantle_engine::ranges::{Client, Ranges, RangesConfig};
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;

// The existing ranges.rs native fixture's page/tree shape and timing fields.
const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 16,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
const SLICE_NS: u64 = 50_000;

fn runtime(actors: usize) -> Runtime {
    Runtime::start(&RuntimeConfig {
        shards: 1,
        // Exactly one slot for each Range and the client task in these fixtures.
        tasks_per_shard: actors,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: SLICE_NS,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: STORE.page_size,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap()
}

fn ranges_config() -> RangesConfig {
    RangesConfig {
        clients: 1,
        slice_ns: SLICE_NS,
        spin_ns: 0,
        inline: false,
    }
}

fn native(path: &std::path::Path, issuer: &Issuer) -> ShardDb<DeviceFile> {
    let file = DeviceFile::open(
        path,
        true,
        CachingRequest::Buffered,
        Alignment::new(STORE.page_size).unwrap(),
    )
    .unwrap();
    let mut db = ShardDb::create(file, STORE, STORE.page_size, TRUNK).unwrap();
    db.attach(issuer, 1).unwrap();
    let path = path.to_path_buf();
    db.set_workers(move || {
        DeviceFile::open(
            &path,
            false,
            CachingRequest::Buffered,
            Alignment::new(STORE.page_size).map_err(|error| Error::Io {
                op: "align a native range worker",
                detail: error.to_string(),
            })?,
        )
        .map_err(|error| Error::Io {
            op: "open a native range worker",
            detail: error.to_string(),
        })
    })
    .unwrap();
    db
}

fn issuer(path: &std::path::Path, ranges: usize) -> Issuer {
    Issuer::start_for(
        path,
        1,
        mantle_engine::shard_db::issuer_batches(1, 1) * ranges,
    )
    .unwrap()
}

async fn canceled(future: impl Future<Output = Result<(), Error>>) {
    let mut future = std::pin::pin!(future);
    poll_fn(|cx| {
        // The colocated Range cannot run between this request's publication and this poll.
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    // Drops the borrowed wait while retaining its owner and one published request.
}

async fn scan(
    client: &mut Client<'_>,
    oracle: &BTreeMap<Vec<u8>, Vec<u8>>,
    from: &[u8],
    end: Option<&[u8]>,
    limit: usize,
) {
    let want: Vec<_> = oracle
        .iter()
        .filter(|(key, _)| key.as_slice() >= from && end.is_none_or(|end| key.as_slice() < end))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let mut at = from.to_vec();
    let mut out = Rows::new();
    let mut next = Vec::new();
    let mut got = Vec::new();
    for _ in 0..=want.len() {
        out.clear();
        let more = client
            .scan_async(&at, end, limit, &mut out, &mut next)
            .await
            .unwrap();
        assert!(out.len() <= limit);
        got.extend(
            out.iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec())),
        );
        if !more {
            assert_eq!(got, want);
            return;
        }
        assert!(next > at);
        at.clone_from(&next);
    }
    panic!("a page failed to advance within its live-row bound");
}

#[test]
fn same_shard_async_clients_preserve_versions_pages_and_durable_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let paths = [dir.path().join("first"), dir.path().join("second")];
    let issuer = issuer(dir.path(), paths.len());
    let mut runtime = runtime(paths.len() + 1);
    let shard = runtime.shard_ids()[0];
    let ranges = Ranges::start(
        &mut runtime,
        vec![
            (Vec::new(), native(&paths[0], &issuer)),
            (b"m".to_vec(), native(&paths[1], &issuer)),
        ],
        ranges_config(),
    )
    .unwrap();
    let (done, result) = std::sync::mpsc::sync_channel(1);
    runtime
        .spawn_on(shard, async move {
            assert_eq!(hyper_rt::futures::current_task().unwrap().shard(), shard);
            // Ranges is owned by this static task; Client borrows it inside the pinned future.
            let mut client = ranges.client().unwrap();
            let mut keys: Vec<_> = (0..TRUNK.leaf_entries * 2)
                .map(|i| {
                    format!(
                        "{}-shared-prefix-{i:04}",
                        if i % 2 == 0 { "a" } else { "n" }
                    )
                    .into_bytes()
                })
                .collect();
            keys.extend([Vec::new(), vec![0], vec![0, 1], vec![0xff], b"m".to_vec()]);
            let mut oracle = BTreeMap::new();
            let mut out = Vec::new();
            for version in 0..TRUNK.fanout {
                for (i, key) in keys.iter().enumerate() {
                    if (i + version) % 5 == 0 {
                        client.delete_async(key).await.unwrap();
                        oracle.remove(key);
                    } else {
                        let value = vec![(i + version) as u8; 100 + version];
                        client.put_async(key, &value).await.unwrap();
                        oracle.insert(key.clone(), value);
                    }
                    assert_eq!(
                        client.get_async(key, &mut out).await.unwrap(),
                        oracle.contains_key(key)
                    );
                    if let Some(value) = oracle.get(key) {
                        assert_eq!(&out, value);
                    }
                }
            }
            scan(&mut client, &oracle, b"", None, 3).await;
            scan(
                &mut client,
                &oracle,
                b"a-shared-prefix-0000\0",
                Some(b"n-shared-prefix-0100\0"),
                1,
            )
            .await;
            client.flush_async().await.unwrap();
            client.checkpoint_async(17).await.unwrap();
            assert_eq!(client.stats_async().await.unwrap().len(), 2);
            drop(client);
            ranges.stop_async().await.unwrap();
            done.send(oracle).unwrap();
        })
        .unwrap();
    let oracle = result.recv().unwrap();
    runtime.shutdown().unwrap();
    let mut recovered = BTreeMap::new();
    for path in &paths {
        let file = DeviceFile::open(
            path,
            false,
            CachingRequest::Buffered,
            Alignment::new(STORE.page_size).unwrap(),
        )
        .unwrap();
        let (mut db, applied) = ShardDb::open(file, STORE, STORE.page_size, TRUNK).unwrap();
        assert_eq!(applied, 17);
        db.check_references().unwrap();
        let mut rows = Rows::new();
        let mut next = Vec::new();
        assert!(
            !db.scan(b"", None, oracle.len() + 1, &mut rows, &mut next)
                .unwrap()
        );
        recovered.extend(
            rows.iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec())),
        );
    }
    assert_eq!(recovered, oracle);
}

#[test]
fn canceled_mutation_returns_its_error_before_new_work_and_retains_orphan_admission() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let issuer = issuer(dir.path(), 1);
    let mut runtime = runtime(2);
    let ranges = Ranges::start(
        &mut runtime,
        vec![(Vec::new(), native(&path, &issuer))],
        ranges_config(),
    )
    .unwrap();
    let (done, result) = std::sync::mpsc::sync_channel(1);
    runtime
        .spawn_on(runtime.shard_ids()[0], async move {
            let mut client = ranges.client().unwrap();
            // The format's u16 key bound is intrinsic; this refusal leaves the engine healthy.
            let too_long = vec![b'x'; usize::from(u16::MAX) + 1];
            canceled(client.put_async(&too_long, b"refused")).await;
            assert!(matches!(
                client.put_async(b"not-published", b"v").await,
                Err(Error::InvalidArgument { .. })
            ));
            let mut value = Vec::new();
            assert!(
                !client
                    .get_async(b"not-published", &mut value)
                    .await
                    .unwrap()
            );
            client.put_async(b"healthy", b"new").await.unwrap();
            assert!(client.get_async(b"healthy", &mut value).await.unwrap());
            assert_eq!(value, b"new");
            canceled(client.put_async(b"orphan", b"kept once")).await;
            drop(client);
            assert!(matches!(
                ranges.client(),
                Err(Error::LimitExceeded { limit: 1, .. })
            ));
            // Request publication queued the Range before this client's own yield, so the
            // request is retired by the colocated owner before this client runs again.
            hyper_rt::futures::yield_now().await;
            let mut replacement = ranges.client().unwrap();
            assert!(replacement.get_async(b"orphan", &mut value).await.unwrap());
            assert_eq!(value, b"kept once");
            replacement.checkpoint_async(3).await.unwrap();
            drop(replacement);
            ranges.stop_async().await.unwrap();
            done.send(()).unwrap();
        })
        .unwrap();
    result.recv().unwrap();
    runtime.shutdown().unwrap();
    let file = DeviceFile::open(
        &path,
        false,
        CachingRequest::Buffered,
        Alignment::new(STORE.page_size).unwrap(),
    )
    .unwrap();
    let (mut db, applied) = ShardDb::open(file, STORE, STORE.page_size, TRUNK).unwrap();
    assert_eq!(applied, 3);
    db.check_references().unwrap();
    let mut value = Vec::new();
    assert!(db.get(b"orphan", &mut value).unwrap());
    assert_eq!(value, b"kept once");
    assert!(!db.get(b"not-published", &mut value).unwrap());
}

#[derive(Default)]
struct SyncFacts {
    calls: AtomicUsize,
    reject: AtomicBool,
}

struct File {
    file: DeviceFile,
    facts: Arc<SyncFacts>,
}

impl BlockFile for File {
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
        if self.facts.reject.load(Ordering::SeqCst) {
            self.facts.calls.fetch_add(1, Ordering::SeqCst);
            return Err(DiskError::Io {
                op: "test checkpoint write refusal",
                path: std::path::PathBuf::new(),
                source: std::io::Error::other("test checkpoint write refusal"),
            });
        }
        self.file.write_all_at(buf, offset)
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        self.facts.calls.fetch_add(1, Ordering::SeqCst);
        if self.facts.reject.load(Ordering::SeqCst) {
            return Err(DiskError::Io {
                op: "test durability refusal",
                path: std::path::PathBuf::new(),
                source: std::io::Error::other("test durability refusal"),
            });
        }
        self.file.sync_data()
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            facts: Arc::clone(&self.facts),
        })
    }
}

fn tracked(path: &std::path::Path, facts: Arc<SyncFacts>) -> ShardDb<File> {
    let file = File {
        file: DeviceFile::open(
            path,
            true,
            CachingRequest::Buffered,
            Alignment::new(STORE.page_size).unwrap(),
        )
        .unwrap(),
        facts,
    };
    ShardDb::create(file, STORE, STORE.page_size, TRUNK).unwrap()
}

fn tracked_backend(
    db: &mut ShardDb<File>,
    issuer: &Issuer,
    path: &std::path::Path,
    facts: Arc<SyncFacts>,
) {
    db.attach(issuer, 1).unwrap();
    let path = path.to_path_buf();
    db.set_workers(move || {
        Ok(File {
            file: DeviceFile::open(
                &path,
                false,
                CachingRequest::Buffered,
                Alignment::new(STORE.page_size).unwrap(),
            )
            .unwrap(),
            facts: Arc::clone(&facts),
        })
    })
    .unwrap();
}

async fn foreign_repoll(
    future: impl Future<Output = Result<(), Error>>,
    ready: bool,
    observer: Option<&mut Client<'_>>,
    facts: &SyncFacts,
) {
    let before = facts.calls.load(Ordering::SeqCst);
    let mut future = std::pin::pin!(future);
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    if ready {
        // This ordered barrier cannot reply before the first checkpoint's reply is queued.
        // Device sync observations alone precede owner completion routing and are insufficient.
        let observed = observer.unwrap().flush_async().await;
        if facts.reject.load(Ordering::SeqCst) {
            assert!(matches!(observed, Err(Error::Io { .. })));
        } else {
            observed.unwrap();
        }
        assert!(facts.calls.load(Ordering::SeqCst) > before);
    }
    let mut foreign = Context::from_waker(Waker::noop());
    assert!(matches!(
        future.as_mut().poll(&mut foreign),
        Poll::Ready(Err(Error::InvalidArgument { .. }))
    ));
}

fn checkpoint_repoll(ready: bool) {
    let dir = tempfile::tempdir().unwrap();
    let paths = [dir.path().join("first"), dir.path().join("second")];
    let facts = Arc::new(SyncFacts::default());
    let mut first = tracked(&paths[0], Arc::clone(&facts));
    let second_facts = Arc::new(SyncFacts::default());
    let mut second = tracked(&paths[1], Arc::clone(&second_facts));
    first.put(b"a", b"first").unwrap();
    second.put(b"z", b"second").unwrap();
    let issuer = issuer(dir.path(), paths.len());
    tracked_backend(&mut first, &issuer, &paths[0], Arc::clone(&facts));
    tracked_backend(&mut second, &issuer, &paths[1], second_facts);
    let mut runtime = runtime(paths.len() + 1);
    let ranges = Ranges::start(
        &mut runtime,
        vec![(Vec::new(), first), (b"m".to_vec(), second)],
        RangesConfig {
            clients: if ready { 2 } else { 1 },
            ..ranges_config()
        },
    )
    .unwrap();
    let (done, result) = std::sync::mpsc::sync_channel(1);
    runtime
        .spawn_on(runtime.shard_ids()[0], async move {
            let mut client = ranges.client().unwrap();
            let mut observer = ready.then(|| ranges.client().unwrap());
            foreign_repoll(client.checkpoint_async(7), ready, observer.as_mut(), &facts).await;
            drop(observer);
            let mut value = Vec::new();
            // Reclaims the first answer and finishes the second checkpoint before this get.
            assert!(client.get_async(b"z", &mut value).await.unwrap());
            assert_eq!(value, b"second");
            drop(client);
            ranges.stop_async().await.unwrap();
            done.send(()).unwrap();
        })
        .unwrap();
    result.recv().unwrap();
    runtime.shutdown().unwrap();
    for (i, path) in paths.iter().enumerate() {
        let file = DeviceFile::open(
            path,
            false,
            CachingRequest::Buffered,
            Alignment::new(STORE.page_size).unwrap(),
        )
        .unwrap();
        let (mut db, applied) = ShardDb::open(file, STORE, STORE.page_size, TRUNK).unwrap();
        assert_eq!(
            applied, 7,
            "foreign polling must retain undispatched checkpoints"
        );
        db.check_references().unwrap();
        let (key, expected) = if i == 0 {
            (b"a".as_slice(), b"first".as_slice())
        } else {
            (b"z".as_slice(), b"second".as_slice())
        };
        let mut value = Vec::new();
        assert!(db.get(key, &mut value).unwrap());
        assert_eq!(value, expected);
        let mut rows = Rows::new();
        let mut next = Vec::new();
        assert!(!db.scan(b"", None, 2, &mut rows, &mut next).unwrap());
        assert_eq!(rows.iter().collect::<Vec<_>>(), vec![(key, expected)]);
    }
}

#[test]
fn a_foreign_repoll_of_a_pending_barrier_preserves_checkpoint_recovery() {
    checkpoint_repoll(false);
}

#[test]
fn a_foreign_repoll_of_a_queued_barrier_preserves_checkpoint_recovery() {
    checkpoint_repoll(true);
}

#[test]
fn a_foreign_ready_poll_cannot_consume_an_error_or_publish_replacement_work() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let facts = Arc::new(SyncFacts::default());
    let mut db = tracked(&path, Arc::clone(&facts));
    db.put(b"known", b"durable").unwrap();
    db.checkpoint(1).unwrap();
    db.put(b"uncheckpointed", b"tail").unwrap();
    let issuer = issuer(dir.path(), 1);
    tracked_backend(&mut db, &issuer, &path, Arc::clone(&facts));
    let mut runtime = runtime(2);
    let ranges = Ranges::start(
        &mut runtime,
        vec![(Vec::new(), db)],
        RangesConfig {
            clients: 2,
            ..ranges_config()
        },
    )
    .unwrap();
    let (done, result) = std::sync::mpsc::sync_channel(1);
    runtime
        .spawn_on(runtime.shard_ids()[0], async move {
            let mut client = ranges.client().unwrap();
            facts.reject.store(true, Ordering::SeqCst);
            let mut observer = ranges.client().unwrap();
            foreign_repoll(
                client.checkpoint_async(2),
                true,
                Some(&mut observer),
                &facts,
            )
            .await;
            drop(observer);
            let mut value = b"untouched".to_vec();
            assert!(matches!(
                client.get_async(b"known", &mut value).await,
                Err(Error::Io { .. })
            ));
            assert_eq!(
                value, b"untouched",
                "the next get must not publish after the prior error"
            );
            assert!(matches!(
                client.put_async(b"not-kept", b"v").await,
                Err(Error::Io { .. })
            ));
            drop(client);
            assert!(matches!(ranges.stop_async().await, Err(Error::Io { .. })));
            done.send(()).unwrap();
        })
        .unwrap();
    result.recv().unwrap();
    runtime.shutdown().unwrap();
    let file = DeviceFile::open(
        &path,
        false,
        CachingRequest::Buffered,
        Alignment::new(STORE.page_size).unwrap(),
    )
    .unwrap();
    let (mut db, applied) = ShardDb::open(file, STORE, STORE.page_size, TRUNK).unwrap();
    assert_eq!(applied, 1);
    db.check_references().unwrap();
    let mut value = Vec::new();
    assert!(db.get(b"known", &mut value).unwrap());
    assert_eq!(value, b"durable");
    assert!(!db.get(b"not-kept", &mut value).unwrap());
}

#[test]
fn terminal_clients_keep_admission_until_their_owner_and_request_retire() {
    let dir = tempfile::tempdir().unwrap();
    let facts = Arc::new(SyncFacts::default());
    let path = dir.path().join("store");
    let mut db = tracked(&path, Arc::clone(&facts));
    let issuer = issuer(dir.path(), 1);
    tracked_backend(&mut db, &issuer, &path, Arc::clone(&facts));
    let mut runtime = runtime(2);
    let ranges = Ranges::start(&mut runtime, vec![(Vec::new(), db)], ranges_config()).unwrap();
    let (done, result) = std::sync::mpsc::sync_channel(1);
    runtime
        .spawn_on(runtime.shard_ids()[0], async move {
            let mut client = ranges.client().unwrap();
            client.put_async(b"known", b"value").await.unwrap();
            client.checkpoint_async(1).await.unwrap();
            // Cancel the actual admitted actor through its public generational identity.
            let owner = ranges.task_ids().first().copied().unwrap();
            canceled(client.put_async(b"unanswered", b"v")).await;
            hyper_rt::futures::cancel(owner).unwrap();
            hyper_rt::futures::yield_now().await;
            let mut value = Vec::new();
            assert!(matches!(
                client.get_async(b"known", &mut value).await,
                Err(Error::Gone { .. })
            ));
            assert!(matches!(
                ranges.client(),
                Err(Error::LimitExceeded { limit: 1, .. })
            ));
            drop(client);
            let mut replacement = ranges.client().unwrap();
            assert!(matches!(
                replacement.get_async(b"known", &mut value).await,
                Err(Error::Gone { .. })
            ));
            drop(replacement);
            assert!(matches!(ranges.stop_async().await, Err(Error::Gone { .. })));
            done.send(()).unwrap();
        })
        .unwrap();
    result.recv().unwrap();
    runtime.shutdown().unwrap();
}

#[test]
fn an_off_runtime_async_call_is_refused_before_publication_and_reuses_its_buffers() {
    let dir = tempfile::tempdir().unwrap();
    let issuer = issuer(dir.path(), 1);
    let mut runtime = runtime(1);
    let ranges = Ranges::start(
        &mut runtime,
        vec![(Vec::new(), native(&dir.path().join("store"), &issuer))],
        ranges_config(),
    )
    .unwrap();
    let mut client = ranges.client().unwrap();
    assert!(matches!(
        ranges.client(),
        Err(Error::LimitExceeded { limit: 1, .. })
    ));
    {
        let mut put = std::pin::pin!(client.put_async(b"must-not-publish", b"v"));
        let mut foreign = Context::from_waker(Waker::noop());
        assert!(matches!(
            put.as_mut().poll(&mut foreign),
            Poll::Ready(Err(Error::InvalidArgument { .. }))
        ));
    }
    let mut value = Vec::new();
    assert!(!client.get(b"must-not-publish", &mut value).unwrap());
    client.put(b"healthy", b"reused").unwrap();
    assert!(client.get(b"healthy", &mut value).unwrap());
    assert_eq!(value, b"reused");
    drop(client);
    ranges.stop().unwrap();
    runtime.shutdown().unwrap();
}
