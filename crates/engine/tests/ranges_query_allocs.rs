//! Warm public async query counts, in a dedicated binary with only this test.
//! Each phase counts all process threads; setup, reporting and durability are outside it.
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
use hyper_measure::alloc::{self, Counting, Counts};
use hyper_rt::runtime::interests_for;
use hyper_rt::{Runtime, RuntimeConfig};
use mantle_engine::branch::Op;
use mantle_engine::memtable::hashed::HashMem;
use mantle_engine::ranges::{Client, Ranges, RangesConfig};
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::{ShardDb, issuer_batches};
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;
use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;

#[global_allocator]
static ALLOCATOR: Counting = Counting;
const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 16,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
// Existing branch/index and native Range query fixtures' complete dataset.
const N: usize = 2049;
// Existing unflushed-active Range fixture capacity; the public HashMem below proves fit.
const ACTIVE_BYTES: usize = 1 << 20;
// Existing native Range fixture's depth: a transfer may wait while another can complete.
const DEPTH: usize = 2;
// Existing public scan allocation fixture's ten-row pages.
const PAGE_ROWS: usize = 10;

struct Native {
    file: DeviceFile,
    reads: Arc<AtomicUsize>,
}
impl Native {
    fn require_device_thread(&self, op: &'static str) -> Result<(), DiskError> {
        if hyper_rt::futures::current_task().is_some() {
            return Err(DiskError::Io {
                op,
                path: Default::default(),
                source: std::io::Error::other("device operation on runtime owner"),
            });
        }
        Ok(())
    }
}
impl BlockFile for Native {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }
    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.require_device_thread("query oracle owner read")?;
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.file.read_exact_at(buf, offset)
    }
    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.require_device_thread("query oracle owner write")?;
        self.file.write_all_at(buf, offset)
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        self.require_device_thread("query oracle owner sync")?;
        self.file.sync_data()
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            reads: Arc::clone(&self.reads),
        })
    }
}
fn native(path: &Path, create: bool, reads: Arc<AtomicUsize>) -> Native {
    Native {
        file: DeviceFile::open(
            path,
            create,
            CachingRequest::Buffered,
            Alignment::new(STORE.page_size).unwrap(),
        )
        .unwrap(),
        reads,
    }
}
fn key(i: usize) -> Vec<u8> {
    match i {
        0 => Vec::new(),
        1 => vec![0, 1],
        _ => format!("shared-prefix/{i:08}").into_bytes(),
    }
}
struct Input {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}
struct Data {
    input: Vec<Input>,
    points: Vec<Input>,
    live: Vec<(Vec<u8>, Vec<u8>)>,
}
fn data() -> Data {
    let mut input = Vec::new();
    let mut expected = BTreeMap::new();
    for i in 0..N {
        let k = key(i);
        let len = [0, 1, 33, STORE.page_size / 16][i % 4];
        let value = vec![(i % 251) as u8; len];
        expected.insert(k.clone(), value.clone());
        input.push(Input {
            key: k,
            value: Some(value),
        });
    }
    for i in (0..N).step_by(7) {
        let k = key(i);
        expected.remove(&k);
        input.push(Input {
            key: k,
            value: None,
        });
    }
    for i in (0..N).step_by(11) {
        let k = key(i);
        let value = vec![(i % 239) as u8; STORE.page_size / 3];
        expected.insert(k.clone(), value.clone());
        input.push(Input {
            key: k,
            value: Some(value),
        });
    }
    let mut capacity = HashMem::new(ACTIVE_BYTES).unwrap();
    for op in &input {
        capacity
            .insert(
                &op.key,
                if op.value.is_some() {
                    Op::Put
                } else {
                    Op::Delete
                },
                op.value.as_deref().unwrap_or(&[]),
            )
            .unwrap();
    }
    let mut points = (0..N)
        .map(|i| {
            let k = key(i);
            let value = expected.get(&k).cloned();
            Input { key: k, value }
        })
        .collect::<Vec<_>>();
    points.push(Input {
        key: b"zz-absent".to_vec(),
        value: None,
    });
    Data {
        input,
        points,
        live: expected.into_iter().collect(),
    }
}
async fn points(client: &mut Client<'_>, data: &Data, value: &mut Vec<u8>) {
    for point in &data.points {
        assert_eq!(
            client.get_async(&point.key, value).await.unwrap(),
            point.value.is_some()
        );
        if let Some(want) = &point.value {
            assert_eq!(&*value, want);
        }
    }
}
async fn scan(
    client: &mut Client<'_>,
    expected: &[(Vec<u8>, Vec<u8>)],
    rows: &mut Rows,
    from: &mut Vec<u8>,
    next: &mut Vec<u8>,
    end: Option<&[u8]>,
) {
    from.clear();
    let wanted = expected
        .iter()
        .take_while(|(k, _)| end.is_none_or(|end| k.as_slice() < end))
        .count();
    let mut at = 0;
    // Each page either returns a live row or advances over one input's tombstone, then ends.
    for _ in 0..=N {
        rows.clear();
        let more = client
            .scan_async(from, end, PAGE_ROWS, rows, next)
            .await
            .unwrap();
        assert!(rows.len() <= PAGE_ROWS);
        for (k, value) in rows.iter() {
            let (want_key, want_value) = &expected[at];
            assert_eq!(k, want_key.as_slice());
            assert_eq!(value, want_value.as_slice());
            at += 1;
        }
        if !more {
            assert_eq!(at, wanted);
            return;
        }
        assert!(next.as_slice() > from.as_slice());
        std::mem::swap(from, next);
    }
    panic!("scan exceeded its input-derived page bound");
}
// Close the result sender before waking, including task panic/cancellation cleanup. The
// controller uses its existing park permit, not a first-time blocking channel context
// whose allocation could accidentally enter another thread's process count.
struct Return<T> {
    sender: Option<mpsc::SyncSender<T>>,
    controller: thread::Thread,
}
impl<T> Drop for Return<T> {
    fn drop(&mut self) {
        drop(self.sender.take());
        self.controller.unpark();
    }
}

struct CountOnDrop(bool);
impl Drop for CountOnDrop {
    fn drop(&mut self) {
        if self.0 {
            alloc::end_process();
        }
    }
}
async fn count(work: impl Future<Output = ()>) -> Counts {
    alloc::begin_process();
    let mut active = CountOnDrop(true);
    work.await;
    let counts = alloc::end_process();
    active.0 = false;
    counts
}

#[derive(Clone, Copy)]
enum Mode {
    Active,
    Resident,
    Device,
}
impl Mode {
    fn on_disk(self) -> bool {
        !matches!(self, Self::Active)
    }
    fn name(self) -> &'static str {
        match self {
            Self::Active => "unflushed-memory",
            Self::Resident => "resident-branch",
            Self::Device => "cache0-device",
        }
    }
}
fn case(mode: Mode) {
    let disk = mode.on_disk();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let reads = Arc::new(AtomicUsize::new(0));
    let data = data();
    let mem = if disk { STORE.page_size } else { ACTIVE_BYTES };
    let mut db =
        ShardDb::create(native(&path, true, Arc::clone(&reads)), STORE, mem, TRUNK).unwrap();
    for op in &data.input {
        match &op.value {
            Some(value) => db.put(&op.key, value).unwrap(),
            None => db.delete(&op.key).unwrap(),
        }
    }
    if disk {
        db.flush().unwrap();
    }
    while db.owed() {
        assert!(db.maintain(u64::MAX).unwrap() > 0);
    }
    if disk {
        db.checkpoint(17).unwrap();
    }
    let cache_pages = if matches!(mode, Mode::Resident) {
        // Every possible page of the actual checkpointed file fits. This derives capacity
        // from data, includes metadata conservatively, and does not assume a tree layout.
        let bytes = native(&path, false, Arc::clone(&reads)).len().unwrap();
        let page = u64::try_from(STORE.page_size).unwrap();
        usize::try_from(bytes.checked_add(page - 1).unwrap() / page).unwrap()
    } else {
        0
    };
    db.set_cache(cache_pages);
    let issuer = Issuer::start_for(dir.path(), DEPTH, issuer_batches(DEPTH, DEPTH)).unwrap();
    db.attach(&issuer, DEPTH).unwrap();
    let worker_path = path.clone();
    let worker_reads = Arc::clone(&reads);
    db.set_workers(move || Ok(native(&worker_path, false, Arc::clone(&worker_reads))))
        .unwrap();
    let mut runtime = Runtime::start(&RuntimeConfig {
        shards: 1,
        // Exactly one Range actor and one client task; no timer is scheduled by this test.
        tasks_per_shard: 2,
        timers_per_shard: 0,
        interests_per_shard: interests_for(2),
        // Retain the existing native Range fixture's portable driver/control-ring shape.
        ring_entries: 64,
        step_budget_ns: 50_000,
        timer_tick_ns: 100_000,
        batch: 2,
        pin: false,
        cores: Vec::new(),
        page_bytes: STORE.page_size,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap();
    let ranges = Ranges::start(
        &mut runtime,
        vec![(Vec::new(), db)],
        RangesConfig {
            clients: 1,
            slice_ns: 50_000,
            spin_ns: 0,
        },
    )
    .unwrap();
    let (done, result) = mpsc::sync_channel(1);
    let controller = thread::current();
    let task_reads = Arc::clone(&reads);
    runtime
        .spawn_on(runtime.shard_ids()[0], async move {
            let result = Return {
                sender: Some(done),
                controller,
            };
            let mut client = ranges.client().unwrap();
            let mut value = Vec::new();
            let mut rows = Rows::new();
            let mut from = Vec::new();
            let mut next = Vec::new();
            let end = key(N / 2);
            let before_warm = task_reads.load(Ordering::SeqCst);
            // Two complete passes warm both sides of the swapped value and reply-row buffers.
            for _ in 0..2 {
                points(&mut client, &data, &mut value).await;
                scan(
                    &mut client,
                    &data.live,
                    &mut rows,
                    &mut from,
                    &mut next,
                    None,
                )
                .await;
                scan(
                    &mut client,
                    &data.live,
                    &mut rows,
                    &mut from,
                    &mut next,
                    Some(&end),
                )
                .await;
            }
            // Empty active state on disk: finish any orphan lookahead before a count begins.
            // The memory case deliberately retains its unflushed active dataset.
            if disk {
                client.flush_async().await.unwrap();
            }
            let warm_reads = task_reads.load(Ordering::SeqCst) - before_warm;
            if matches!(mode, Mode::Active) {
                assert_eq!(warm_reads, 0);
            } else {
                assert!(
                    warm_reads > 0,
                    "the branch path was actually loaded from the device before measurement"
                );
            }
            let before = task_reads.load(Ordering::SeqCst);
            let point_counts = count(points(&mut client, &data, &mut value)).await;
            let point_reads = task_reads.load(Ordering::SeqCst) - before;
            let before = task_reads.load(Ordering::SeqCst);
            let scan_counts = count(async {
                scan(
                    &mut client,
                    &data.live,
                    &mut rows,
                    &mut from,
                    &mut next,
                    None,
                )
                .await;
                scan(
                    &mut client,
                    &data.live,
                    &mut rows,
                    &mut from,
                    &mut next,
                    Some(&end),
                )
                .await;
            })
            .await;
            if disk {
                client.flush_async().await.unwrap();
            }
            let scan_reads = task_reads.load(Ordering::SeqCst) - before;
            // Native-read incidence is an independent witness, not inferred from cache settings.
            if matches!(mode, Mode::Device) {
                assert!(point_reads > 0 && scan_reads > 0);
            } else {
                assert_eq!((point_reads, scan_reads), (0, 0));
            }
            client.checkpoint_async(17).await.unwrap();
            drop(client);
            ranges.stop_async().await.unwrap();
            result
                .sender
                .as_ref()
                .unwrap()
                .send((
                    point_counts,
                    scan_counts,
                    point_reads,
                    scan_reads,
                    warm_reads,
                    data.live,
                ))
                .unwrap();
        })
        .unwrap();
    let (points, scans, point_reads, scan_reads, warm_reads, expected) = loop {
        match result.try_recv() {
            Ok(result) => break result,
            Err(mpsc::TryRecvError::Empty) => thread::park(),
            Err(mpsc::TryRecvError::Disconnected) => panic!("query task ended without its result"),
        }
    };
    runtime.shutdown().unwrap();
    drop(issuer);
    eprintln!(
        "query allocation mode={} cache_pages={cache_pages} warm_reads={warm_reads} points={points:?} scans={scans:?} point_reads={point_reads} scan_reads={scan_reads}",
        mode.name()
    );
    // These are falsifiable proposed expectations, not a pre-run allocation claim.
    assert_eq!(
        (points.allocations, points.reallocations),
        (0, 0),
        "{points:?}"
    );
    assert_eq!(
        (scans.allocations, scans.reallocations),
        (0, 0),
        "{scans:?}"
    );
    let (mut reopened, index) =
        ShardDb::open(native(&path, false, reads), STORE, mem, TRUNK).unwrap();
    assert_eq!(index, 17);
    let mut rows = Rows::new();
    let mut next = Vec::new();
    assert!(
        !reopened
            .scan(b"", None, expected.len() + 1, &mut rows, &mut next)
            .unwrap()
    );
    assert_eq!(rows.len(), expected.len());
    for ((k, v), (want_k, want_v)) in rows.iter().zip(&expected) {
        assert_eq!(k, want_k.as_slice());
        assert_eq!(v, want_v.as_slice());
    }
    reopened.check_references().unwrap();
}

#[test]
fn warm_async_queries_measure_active_resident_and_device_paths_separately() {
    assert!(alloc::installed());
    case(Mode::Active);
    case(Mode::Resident);
    case(Mode::Device);
}
