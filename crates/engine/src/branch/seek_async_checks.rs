//! A seek through demand completion ([`RunCursor::seek_async`]) lands as the synchronous seek
//! does (tests/branch_seek_checks.rs): on the first entry at or past its key, past a leaf's last
//! entry onto the next leaf's first, with the page cache and without it; and a leaf whose first
//! entry carries an unknown operation under a valid page checksum fails every seek that reads it.
use super::*;
use crate::branch::filter::Keys;
use crate::store::Config;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 4096,
};
/// Rows, each at an even key, so every odd key is absent and falls between two rows.
const ROWS: u64 = 2049;
/// A page cache of this many pages holds every page of the branch.
const CACHED: usize = 1024;
/// An operation byte no entry carries ([`Op`] is 1 or 2).
const UNKNOWN: u8 = 0xEE;
/// The failure guard of every wait for a fact (pool_lifecycle.rs): a passing run never reaches it.
const WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// The entry a seek lands on: its key, operation and value, none past the last.
type Landed = Option<(Vec<u8>, Op, Vec<u8>)>;

fn key(k: u64) -> Vec<u8> {
    format!("key-{k:08}").into_bytes()
}

fn value(i: u64) -> Vec<u8> {
    vec![u8::try_from(i % 251).unwrap(); 100]
}

/// The rows of leaf page `page`, first to last, by the branch's own leaf index.
fn rows_of(branch: &Branch, page: u32) -> (u64, u64) {
    let rows: Vec<u64> = (0..ROWS)
        .filter(|&i| branch.leaf_of(&key(2 * i)).unwrap() == page)
        .collect();
    (*rows.first().unwrap(), *rows.last().unwrap())
}

/// What a seek through demand completion from key `k` lands on: its key, operation and value,
/// none past the branch's last entry, or its error.
async fn seek<F: BlockFile>(
    store: &mut Store<F>,
    branch: &Branch,
    k: u64,
) -> Result<Landed, Error> {
    let mut c = RunCursor::new(store)?;
    let landed = c.seek_async(branch, store, &key(k)).await;
    let got = landed.map(|()| {
        c.valid()
            .then(|| (c.key().to_vec(), c.op(), c.value().to_vec()))
    });
    c.give_back(store);
    got
}

fn row(i: u64) -> Landed {
    Some((key(2 * i), Op::Put, value(i)))
}

fn malformed(got: Result<Landed, Error>, what: &str) {
    assert!(
        matches!(
            got,
            Err(Error::Corruption {
                why: Malformed::UnknownTag(UNKNOWN),
                ..
            })
        ),
        "{what}: {got:?}, not the malformed first entry's corruption"
    );
}

/// Do: break the first entry of a leaf in the middle of a branch on a device file, through the
/// store's own page writes; then, through demand completion on the device's issuer, with the
/// cache off and holding every page, seek into that leaf, past it, from the gap before it, into
/// the leaves around it, and past a sound leaf's last entry. Expect: every seek that reads the
/// broken leaf fails with its first entry's corruption; every other lands on the first row at or
/// past its key, exactly.
#[test]
fn a_seek_through_demand_completion_lands_and_checks_as_the_synchronous_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let file = DeviceFile::open(
        &path,
        true,
        CachingRequest::Buffered,
        Alignment::new(CONFIG.page_size).unwrap(),
    )
    .unwrap();
    let mut store = Store::create(file, CONFIG).unwrap();
    store.set_cache(0);
    let mut builder = Builder::new(&mut store, Keys::Exactly(ROWS)).unwrap();
    for i in 0..ROWS {
        builder
            .add(&mut store, &key(2 * i), Op::Put, &value(i))
            .unwrap();
    }
    let branch = builder.finish(&mut store).unwrap();
    let page = branch.leaf_of(&key(ROWS)).unwrap();
    let (first, last) = rows_of(&branch, page);
    let (_, next_last) = rows_of(&branch, branch.leaf_of(&key(2 * (last + 1))).unwrap());
    assert!(
        first > 1 && last > first + 1 && next_last >= last + 3 && next_last + 2 < ROWS,
        "a leaf of several rows with leaves before it and two after it, the next of three rows"
    );
    let address = branch.page_address(&store, u64::from(page)).unwrap();
    let mut payload = Vec::new();
    store.read_page(address, &mut payload).unwrap();
    // The leaf's layout: kind, entries (2), prefix length (2), the prefix, then each entry's
    // offset (2); an entry is its suffix length (2), then its operation.
    let prefix = usize::from(u16::from_le_bytes([payload[3], payload[4]]));
    let table = 5 + prefix;
    let at = usize::from(u16::from_le_bytes([payload[table], payload[table + 1]]));
    assert_eq!(
        payload[at + 2],
        1,
        "the first entry is a put before it is broken"
    );
    payload[at + 2] = UNKNOWN;
    let mut run = store.run().unwrap();
    store.queue_page(&mut run, address, &payload).unwrap();
    store.write_run(&mut run).unwrap();
    store.give_run(run);
    store.checkpoint(None, 7).unwrap();
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    let config = RuntimeConfig {
        shards: 1,
        tasks_per_shard: 1,
        timers_per_shard: 1,
        interests_per_shard: 2,
        ring_entries: 1,
        batch: 1,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        spin_ns: 0,
        page_bytes: CONFIG.page_size,
        wake_tracking: None,
        latency_objective_ns: None,
        pin: false,
        cores: Vec::new(),
    };
    let mut runtime = LocalRuntime::new(&config).unwrap();
    let store = runtime
        .block_on(async move {
            let s = &mut store;
            for cache in [0, CACHED] {
                s.set_cache(cache);
                malformed(seek(s, &branch, 2 * first).await, "its first entry");
                malformed(seek(s, &branch, 2 * (first + 1)).await, "a later entry");
                malformed(
                    seek(s, &branch, 2 * last - 1).await,
                    "an absent key inside it",
                );
                malformed(seek(s, &branch, 2 * last + 1).await, "past its last entry");
                malformed(seek(s, &branch, 2 * first - 1).await, "the gap before it");
                assert_eq!(
                    seek(s, &branch, 2 * (first - 2)).await.unwrap(),
                    row(first - 2)
                );
                assert_eq!(
                    seek(s, &branch, 2 * (last + 2)).await.unwrap(),
                    row(last + 2)
                );
                assert_eq!(
                    seek(s, &branch, 2 * next_last + 1).await.unwrap(),
                    row(next_last + 1),
                    "past a sound leaf's last entry, onto the next leaf's first"
                );
                assert_eq!(seek(s, &branch, 2 * ROWS).await.unwrap(), None);
                // One cursor through several seeks: the second into the same leaf finds it
                // held and places on it again, its first entry checked again; then another
                // leaf, past the end, and into the broken leaf.
                let mut c = RunCursor::new(s).unwrap();
                for (k, want) in [
                    (2 * (last + 2), row(last + 2)),
                    (2 * (last + 3), row(last + 3)),
                    (2 * (first - 2), row(first - 2)),
                    (2 * ROWS, None),
                ] {
                    c.seek_async(&branch, s, &key(k)).await.unwrap();
                    let got = c
                        .valid()
                        .then(|| (c.key().to_vec(), c.op(), c.value().to_vec()));
                    assert_eq!(got, want, "a reused cursor's seek from key {k}");
                }
                let broken = c.seek_async(&branch, s, &key(2 * (first + 1))).await;
                malformed(
                    broken.map(|()| None),
                    "a reused cursor into the broken leaf",
                );
                c.give_back(s);
            }
            store.retire_async().await.unwrap();
            store
        })
        .unwrap();
    let (file, landed) = crate::store::cold_file(store.into_file());
    landed.unwrap();
    drop(file);
}

/// Leaf reads on the device held while the gate is shut, the first one held signaled: a seek's
/// demand read can be caught in flight.
struct Hold {
    open: std::sync::Mutex<bool>,
    changed: std::sync::Condvar,
    held: std::sync::atomic::AtomicUsize,
    entered: hyper_rt::sync::Sender<()>,
}

impl Hold {
    fn release(&self) {
        *self.open.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.changed.notify_all();
    }
}

/// Releases the hold when dropped: a failed assertion leaves no device read held.
struct ReleaseOnDrop(&'static Hold);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct Held {
    file: DeviceFile,
    hold: &'static Hold,
}

impl BlockFile for Held {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }
    fn len(&self) -> Result<u64, hyper_block::DiskError> {
        self.file.len()
    }
    fn read_exact_at(&self, out: &mut [u8], at: u64) -> Result<(), hyper_block::DiskError> {
        // A demand read belongs on the issuer's threads: one on the runtime's own is refused,
        // typed, rather than held there.
        if hyper_rt::futures::current_task().is_some() {
            return Err(hyper_block::DiskError::Io {
                op: "a demand page read on the runtime owner",
                path: std::path::PathBuf::new(),
                source: std::io::Error::other("a demand page read on the runtime owner"),
            });
        }
        let mut open = self.hold.open.lock().unwrap_or_else(|e| e.into_inner());
        if !*open {
            if self
                .hold
                .held
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                self.hold.entered.blocking_send(()).unwrap();
            }
            while !*open {
                open = self
                    .hold
                    .changed
                    .wait(open)
                    .unwrap_or_else(|e| e.into_inner());
            }
        }
        drop(open);
        self.file.read_exact_at(out, at)
    }
    fn write_all_at(&self, out: &[u8], at: u64) -> Result<(), hyper_block::DiskError> {
        self.file.write_all_at(out, at)
    }
    fn sync_data(&self) -> Result<(), hyper_block::DiskError> {
        self.file.sync_data()
    }
    fn try_clone(&self) -> Result<Self, hyper_block::DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            hold: self.hold,
        })
    }
}

/// Do: a branch on a device file read through the issuer, the cache off; a seek through demand
/// completion polled once while its leaf's read is held on the device, then canceled there, its
/// cursor given back; the read let go. Expect: the first poll is pending with the read held; after
/// the cancel a new seek to the same key lands on its row exactly, as do seeks past the leaf; the
/// store retires, and reopened cold reads every row.
#[test]
fn a_seek_canceled_in_its_held_leaf_read_gives_its_cursor_back_and_the_next_reads_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let (entered, mut heard) = hyper_rt::sync::channel(1).unwrap();
    let hold: &'static Hold = Box::leak(Box::new(Hold {
        open: std::sync::Mutex::new(true),
        changed: std::sync::Condvar::new(),
        held: std::sync::atomic::AtomicUsize::new(0),
        entered,
    }));
    let file = Held {
        file: DeviceFile::open(
            &path,
            true,
            CachingRequest::Buffered,
            Alignment::new(CONFIG.page_size).unwrap(),
        )
        .unwrap(),
        hold,
    };
    let mut store = Store::create(file, CONFIG).unwrap();
    store.set_cache(0);
    let mut builder = Builder::new(&mut store, Keys::Exactly(ROWS)).unwrap();
    for i in 0..ROWS {
        builder
            .add(&mut store, &key(2 * i), Op::Put, &value(i))
            .unwrap();
    }
    let branch = builder.finish(&mut store).unwrap();
    store.checkpoint(None, 7).unwrap();
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    let target = ROWS / 2;
    let (_, last) = rows_of(&branch, branch.leaf_of(&key(2 * target)).unwrap());
    let config = RuntimeConfig {
        shards: 1,
        tasks_per_shard: 1,
        timers_per_shard: 1,
        interests_per_shard: 2,
        ring_entries: 1,
        batch: 1,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        spin_ns: 0,
        page_bytes: CONFIG.page_size,
        wake_tracking: None,
        latency_objective_ns: None,
        pin: false,
        cores: Vec::new(),
    };
    let mut runtime = LocalRuntime::new(&config).unwrap();
    let _release = ReleaseOnDrop(hold);
    *hold.open.lock().unwrap() = false;
    // The failure guard, on the host: a passing run says it is done long before; past the guard,
    // or as a failing test unwinds, it releases the held read and tells the task, so a seek stuck
    // before its read, inside a blocked poll or in a later demand read ends, and the test fails on
    // its facts rather than hanging.
    let (expired, mut expiry) = hyper_rt::sync::channel::<()>(1).unwrap();
    let guard = Guard::start(hold, expired);
    let target_key = key(2 * target);
    let (store, branch) = runtime
        .block_on(async move {
            let s = &mut store;
            let mut c = RunCursor::new(s).unwrap();
            {
                let mut seek = std::pin::pin!(c.seek_async(&branch, s, &target_key));
                let first = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(std::future::Future::poll(seek.as_mut(), cx))
                })
                .await;
                assert!(
                    first.is_pending(),
                    "the seek did not wait for its leaf's read"
                );
                let entered = hyper_rt::combine::race2(heard.recv(), expiry.recv()).await;
                assert!(
                    matches!(entered, hyper_rt::combine::Either::First(Ok(()))),
                    "the seek's leaf read held: not seen within the failure guard"
                );
            }
            // Canceled in the held read: the cursor goes back, the read is let go.
            c.give_back(s);
            hold.release();
            // The rest of the scenario, every later demand read and the retirement among it,
            // still races the guard's expiry.
            let rest = async move {
                assert_eq!(
                    seek(&mut store, &branch, 2 * target).await.unwrap(),
                    row(target)
                );
                assert_eq!(
                    seek(&mut store, &branch, 2 * last + 1).await.unwrap(),
                    row(last + 1),
                    "past the leaf's last entry, onto the next leaf's first"
                );
                store.retire_async().await.unwrap();
                (store, branch)
            };
            match hyper_rt::combine::race2(rest, expiry.recv()).await {
                hyper_rt::combine::Either::First(done) => done,
                hyper_rt::combine::Either::Second(_) => {
                    panic!("the seeks after the cancel: not done within the failure guard")
                }
            }
        })
        .unwrap();
    assert!(!guard.finish(), "the failure guard expired");
    let (file, landed) = crate::store::cold_file(store.into_file());
    landed.unwrap();
    drop(file);
    drop(issuer);
    let reopened = Held {
        file: DeviceFile::open(
            &path,
            false,
            CachingRequest::Buffered,
            Alignment::new(CONFIG.page_size).unwrap(),
        )
        .unwrap(),
        hold,
    };
    let (mut store, _) = Store::open(reopened, CONFIG).unwrap();
    let mut out = Vec::new();
    for i in 0..ROWS {
        assert_eq!(
            branch.get(&mut store, &key(2 * i), &mut out).unwrap(),
            Some(Op::Put),
            "row {i}"
        );
        assert_eq!(out, value(i));
    }
}

/// The host-side failure guard: a thread that, past [`WAIT`] or once its owner goes, releases the
/// held read and tells the task. A passing run finishes it before the guard; a failing test's
/// unwind drops it, which releases and joins it there.
struct Guard {
    done: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<bool>>,
}

impl Guard {
    fn start(hold: &'static Hold, expired: hyper_rt::sync::Sender<()>) -> Self {
        let (done, done_rx) = std::sync::mpsc::channel::<()>();
        let thread = std::thread::spawn(move || {
            if done_rx.recv_timeout(WAIT).is_ok() {
                return false;
            }
            hold.release();
            drop(expired.blocking_send(()));
            true
        });
        Self {
            done: Some(done),
            thread: Some(thread),
        }
    }

    /// Ends the guard before its expiry: whether it had expired.
    fn finish(mut self) -> bool {
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
        self.thread.take().is_some_and(|t| t.join().unwrap_or(true))
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        // Unwinding: the sender's drop wakes the guard, which releases the hold; then it is joined.
        drop(self.done.take());
        if let Some(t) = self.thread.take() {
            drop(t.join());
        }
    }
}
