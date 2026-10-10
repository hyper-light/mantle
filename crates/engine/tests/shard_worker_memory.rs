//! Write memory reported by the shard covers real buffers its workers have handed the device.
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

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use mantle_engine::Error;
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::{ShardDb, issuer_batches};
use mantle_engine::store::Config;
use mantle_engine::trunk::{TrunkConfig, pool::Pool};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 1 << 16,
};
const MEM: usize = 4 * 1024;
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
// Two actual device write roles; the gate can hold both without requiring more worker threads.
const DEPTH: usize = 2;
// Existing public worker fixtures' failure watchdog, never an ordering delay.
const WAIT: Duration = Duration::from_secs(5);
// A small public workload capable of crossing more memtables than the machine's worker bound.
const KEYS: usize = 4096;
struct Gate {
    open: AtomicBool,
    expired: AtomicBool,
    held: AtomicUsize,
    reported_write: AtomicUsize,
    reported_total: AtomicUsize,
    observed_write: AtomicUsize,
    observed_total: AtomicUsize,
    observed_held: AtomicUsize,
    notice: Mutex<Option<std::sync::mpsc::SyncSender<Event>>>,
    threads: Mutex<Vec<std::thread::Thread>>,
}
impl Gate {
    fn leaked() -> &'static Self {
        Box::leak(Box::new(Self {
            open: AtomicBool::new(false),
            expired: AtomicBool::new(false),
            held: AtomicUsize::new(0),
            reported_write: AtomicUsize::new(0),
            reported_total: AtomicUsize::new(0),
            observed_write: AtomicUsize::new(0),
            observed_total: AtomicUsize::new(0),
            observed_held: AtomicUsize::new(0),
            notice: Mutex::new(None),
            threads: Mutex::new(Vec::new()),
        }))
    }
    fn release(&self) {
        self.open.store(true, Ordering::SeqCst);
        for thread in self.threads.lock().unwrap().iter() {
            thread.unpark();
        }
    }
}
enum Event {
    Held,
    Stop,
}
struct Open {
    gate: &'static Gate,
    stop: std::sync::mpsc::SyncSender<Event>,
    watch: Option<std::thread::JoinHandle<()>>,
}
impl Open {
    fn new(gate: &'static Gate) -> Self {
        let (stop, ended) = std::sync::mpsc::sync_channel(1);
        *gate.notice.lock().unwrap() = Some(stop.clone());
        let watch = std::thread::spawn(move || {
            if ended.recv_timeout(WAIT).is_err() {
                gate.expired.store(true, Ordering::SeqCst);
            }
            // Release only after the real held-write fact or cleanup; never after a delay.
            gate.release();
        });
        Self {
            gate,
            stop,
            watch: Some(watch),
        }
    }
}
impl Drop for Open {
    fn drop(&mut self) {
        self.gate.release();
        let _ = self.stop.try_send(Event::Stop);
        if let Some(watch) = self.watch.take() {
            watch.join().unwrap();
        }
    }
}
struct File {
    file: DeviceFile,
    gate: &'static Gate,
    worker: bool,
}
impl BlockFile for File {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }
    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }
    fn read_exact_at(&self, bytes: &mut [u8], at: u64) -> Result<(), DiskError> {
        self.file.read_exact_at(bytes, at)
    }
    fn write_all_at(&self, bytes: &[u8], at: u64) -> Result<(), DiskError> {
        if self.worker && !self.gate.open.load(Ordering::SeqCst) {
            // Register before the recheck: release cannot miss a thread about to park.
            self.gate
                .threads
                .lock()
                .unwrap()
                .push(std::thread::current());
            let held = self.gate.held.fetch_add(bytes.len(), Ordering::SeqCst) + bytes.len();
            if self
                .gate
                .observed_held
                .compare_exchange(0, held, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.gate.observed_write.store(
                    self.gate.reported_write.load(Ordering::SeqCst),
                    Ordering::SeqCst,
                );
                self.gate.observed_total.store(
                    self.gate.reported_total.load(Ordering::SeqCst),
                    Ordering::SeqCst,
                );
                if let Some(notice) = self.gate.notice.lock().unwrap().as_ref() {
                    let _ = notice.try_send(Event::Held);
                }
            }
            while !self.gate.open.load(Ordering::SeqCst) {
                std::thread::park();
            }
            let result = self.file.write_all_at(bytes, at);
            self.gate.held.fetch_sub(bytes.len(), Ordering::SeqCst);
            return result;
        }
        self.file.write_all_at(bytes, at)
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            gate: self.gate,
            worker: self.worker,
        })
    }
}
fn open(path: &Path, create: bool, gate: &'static Gate, worker: bool) -> Result<File, Error> {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(STORE.page_size).unwrap(),
    )
    .map(|file| File { file, gate, worker })
    .map_err(|e| Error::Io {
        op: "open a memory fixture file",
        detail: e.to_string(),
    })
}
fn key(i: usize) -> Vec<u8> {
    format!("key-{i:06}").into_bytes()
}
fn check(db: &mut ShardDb<File>, golden: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let mut value = Vec::new();
    for (key, expected) in golden {
        assert!(db.get(key, &mut value).unwrap());
        assert_eq!(&value, expected);
    }
    let mut rows = Rows::new();
    let mut next = Vec::new();
    let mut from = Vec::new();
    let mut got = Vec::new();
    loop {
        rows.clear();
        let more = db
            .scan(
                &from,
                None,
                usize::try_from(TRUNK.leaf_entries).unwrap(),
                &mut rows,
                &mut next,
            )
            .unwrap();
        got.extend(rows.iter().map(|(k, v)| (k.to_vec(), v.to_vec())));
        if !more {
            break;
        }
        from.clone_from(&next);
    }
    assert_eq!(
        got,
        golden
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>()
    );
    assert!(!db.get(&key(KEYS), &mut value).unwrap());
}
#[test]
fn held_worker_writes_are_in_the_public_memory_report() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let gate = Gate::leaked();
    let mut db =
        ShardDb::create(open(&path, true, gate, false).unwrap(), STORE, MEM, TRUNK).unwrap();
    let run = STORE.page_size * usize::try_from(STORE.extent_pages).unwrap();
    // Every affordable output role: batches plus active replacement and submit handoff.
    let per = (DEPTH + 2) * run;
    let budget = (Pool::cores() + 1) * per;
    // This syntax is shared by the historical infallible setter and proposed fallible setter.
    let _ = db.set_memory(budget);
    let issuer = Issuer::start_for(&path, DEPTH, issuer_batches(DEPTH, DEPTH)).unwrap();
    db.attach(&issuer, DEPTH).unwrap();
    let p = path.clone();
    db.set_workers(move || open(&p, false, gate, true)).unwrap();
    let release = Open::new(gate);
    let value = vec![7u8; 100];
    let mut golden = BTreeMap::new();
    for i in 0..KEYS {
        // Reservation is public before admitting a put that can hand a worker more writes.
        // The synchronous put may wait on its feed; the device records this report at the held
        // write, then the controller releases it so that wait can finish without a time guess.
        let reported = db.memory_held();
        gate.reported_write.store(reported.1, Ordering::SeqCst);
        gate.reported_total
            .store(reported.0 + reported.1 + reported.2, Ordering::SeqCst);
        let k = key(i);
        db.put(&k, &value).unwrap();
        golden.insert(k, value.clone());
        if gate.observed_held.load(Ordering::SeqCst) != 0 {
            break;
        }
        std::thread::yield_now();
    }
    let expired = gate.expired.load(Ordering::SeqCst);
    drop(release);
    // Refusal verdict is fixed before release. Complete and verify data before the RED assertion,
    // so a failed memory report still closes every worker, attachment and file cleanly.
    db.put(&key(0), b"newest").unwrap();
    golden.insert(key(0), b"newest".to_vec());
    db.delete(&key(1)).unwrap();
    golden.remove(&key(1));
    db.checkpoint(u64::try_from(golden.len()).unwrap()).unwrap();
    check(&mut db, &golden);
    db.check_references().unwrap();
    let applied = u64::try_from(golden.len()).unwrap();
    let (file, landed) = finished_file(db.into_file());
    landed.unwrap();
    drop(file);
    drop(issuer);
    let (mut db, found) =
        ShardDb::open(open(&path, false, gate, false).unwrap(), STORE, MEM, TRUNK).unwrap();
    assert_eq!(found, applied);
    check(&mut db, &golden);
    db.check_references().unwrap();
    let (file, landed) = finished_file(db.into_file());
    landed.unwrap();
    drop(file);
    let held = gate.observed_held.load(Ordering::SeqCst);
    let reported_write = gate.observed_write.load(Ordering::SeqCst);
    let reported_total = gate.observed_total.load(Ordering::SeqCst);
    assert!(
        !expired && held > 0,
        "a real worker write was held before observation"
    );
    assert!(
        reported_write >= held,
        "write memory {} excludes {} bytes held by actual worker writes",
        reported_write,
        held
    );
    assert!(
        reported_total <= budget,
        "accepted scoped budget: {reported_total} > {budget}"
    );
}

#[test]
fn refused_memory_changes_keep_configuration_and_data_usable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let gate = Gate::leaked();
    gate.release();
    let mut db =
        ShardDb::create(open(&path, true, gate, false).unwrap(), STORE, MEM, TRUNK).unwrap();
    let run = STORE.page_size * usize::try_from(STORE.extent_pages).unwrap();
    db.set_memory(run).unwrap();
    db.put(b"a", b"first").unwrap();
    let before = (
        db.memory_split(),
        db.memory_held(),
        db.write_budget(),
        db.workers(),
    );
    assert!(matches!(
        db.set_memory(run - 1),
        Err(Error::LimitExceeded { .. })
    ));
    assert!(matches!(
        db.set_write_budget(usize::MAX),
        Err(Error::LimitExceeded { .. })
    ));
    let issuer = Issuer::start_for(&path, DEPTH, issuer_batches(DEPTH, DEPTH)).unwrap();
    assert!(matches!(
        db.attach(&issuer, usize::MAX),
        Err(Error::LimitExceeded { .. })
    ));
    assert!(matches!(
        db.attach(&issuer, DEPTH),
        Err(Error::LimitExceeded { .. })
    ));
    let p = path.clone();
    assert!(matches!(
        db.set_workers(move || open(&p, false, gate, true)),
        Err(Error::LimitExceeded { .. })
    ));
    assert_eq!(
        (
            db.memory_split(),
            db.memory_held(),
            db.write_budget(),
            db.workers()
        ),
        before
    );
    db.put(b"a", b"newest").unwrap();
    db.put(b"b", b"gone").unwrap();
    db.delete(b"b").unwrap();
    db.checkpoint(3).unwrap();
    let golden = BTreeMap::from([(b"a".to_vec(), b"newest".to_vec())]);
    check(&mut db, &golden);
    db.check_references().unwrap();
    let (file, landed) = finished_file(db.into_file());
    landed.unwrap();
    drop(file);
    drop(issuer);
    let (mut db, applied) =
        ShardDb::open(open(&path, false, gate, false).unwrap(), STORE, MEM, TRUNK).unwrap();
    assert_eq!(applied, 3);
    check(&mut db, &golden);
    let (file, landed) = finished_file(db.into_file());
    landed.unwrap();
    drop(file);
}

#[test]
fn a_small_accepted_budget_bounds_workers_and_refuses_live_shrink() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let gate = Gate::leaked();
    gate.release();
    let mut db =
        ShardDb::create(open(&path, true, gate, false).unwrap(), STORE, MEM, TRUNK).unwrap();
    let run = STORE.page_size * usize::try_from(STORE.extent_pages).unwrap();
    // Enough for the owner and one worker's actual depth, active output and handoff roles.
    let budget = 2 * (DEPTH + 2) * run;
    db.set_memory(budget).unwrap();
    let issuer = Issuer::start_for(&path, DEPTH, issuer_batches(DEPTH, DEPTH)).unwrap();
    db.attach(&issuer, DEPTH).unwrap();
    let p = path.clone();
    db.set_workers(move || open(&p, false, gate, true)).unwrap();
    let reserved = db.memory_held().1;
    let before = (
        db.memory_split(),
        db.memory_held(),
        db.write_budget(),
        db.workers(),
    );
    assert!(matches!(
        db.set_memory(reserved - 1),
        Err(Error::LimitExceeded { .. })
    ));
    assert_eq!(
        (
            db.memory_split(),
            db.memory_held(),
            db.write_budget(),
            db.workers()
        ),
        before
    );
    let mut golden = BTreeMap::new();
    for i in 0..KEYS {
        let k = key(i);
        let value = i.to_le_bytes().to_vec();
        db.put(&k, &value).unwrap();
        golden.insert(k, value);
        if let Some((held, wanted)) = db.workers() {
            assert!(
                held <= 1 && wanted <= 1,
                "accepted worker budget: held={held}, wanted={wanted}"
            );
        }
    }
    db.put(&key(0), b"newest").unwrap();
    golden.insert(key(0), b"newest".to_vec());
    db.delete(&key(1)).unwrap();
    golden.remove(&key(1));
    db.checkpoint(u64::try_from(KEYS).unwrap()).unwrap();
    check(&mut db, &golden);
    db.check_references().unwrap();
    let before = db.memory_held().1;
    let unchanged = (
        db.memory_split(),
        db.memory_held(),
        db.write_budget(),
        db.workers(),
    );
    assert!(matches!(
        db.attach(&issuer, 1),
        Err(Error::InvalidArgument { .. })
    ));
    assert_eq!(
        (
            db.memory_split(),
            db.memory_held(),
            db.write_budget(),
            db.workers()
        ),
        unchanged
    );
    db.set_write_budget(0).unwrap();
    assert!(
        db.memory_held().1 >= before,
        "smaller depth and queue admission do not release retained output reservations"
    );
    let (file, landed) = finished_file(db.into_file());
    landed.unwrap();
    drop(file);
    drop(issuer);
    let (mut db, applied) =
        ShardDb::open(open(&path, false, gate, false).unwrap(), STORE, MEM, TRUNK).unwrap();
    assert_eq!(applied, u64::try_from(KEYS).unwrap());
    check(&mut db, &golden);
    db.check_references().unwrap();
    let (file, landed) = finished_file(db.into_file());
    landed.unwrap();
    drop(file);
}
/// Keeps the device held until the caller finishes checking the admitted queue. Its watchdog
/// opens only on failure, never as the ordering condition of a passing test.
struct HeldUntilDrop {
    gate: &'static Gate,
    entered: std::sync::mpsc::Receiver<Event>,
    stop: std::sync::mpsc::Sender<()>,
    watch: Option<std::thread::JoinHandle<()>>,
}
impl HeldUntilDrop {
    fn new(gate: &'static Gate) -> Self {
        let (notice, entered) = std::sync::mpsc::sync_channel(1);
        *gate.notice.lock().unwrap() = Some(notice);
        let (stop, ended) = std::sync::mpsc::channel();
        let watch = std::thread::spawn(move || {
            if ended.recv_timeout(WAIT).is_err() {
                gate.expired.store(true, Ordering::SeqCst);
                gate.release();
            }
        });
        Self {
            gate,
            entered,
            stop,
            watch: Some(watch),
        }
    }
    fn entered(&self) {
        assert!(matches!(self.entered.recv_timeout(WAIT), Ok(Event::Held)));
        assert!(self.gate.held.load(Ordering::SeqCst) > 0);
    }
}
impl Drop for HeldUntilDrop {
    fn drop(&mut self) {
        self.gate.release();
        let _ = self.stop.send(());
        if let Some(watch) = self.watch.take() {
            watch.join().unwrap();
        }
    }
}

#[test]
fn queued_output_stays_charged_after_cap_shrink_and_completion() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let gate = Gate::leaked();
    gate.release();
    let value = vec![7u8; 100];
    // Values alone fill this arena; their nonempty keys and record headers force a rotation.
    let mem = KEYS * value.len();
    let mut db =
        ShardDb::create(open(&path, true, gate, true).unwrap(), STORE, mem, TRUNK).unwrap();
    let issuer = Issuer::start_for(&path, DEPTH, DEPTH).unwrap();
    db.attach(&issuer, DEPTH).unwrap();
    let fixed = db.memory_held().1;
    let mut golden = BTreeMap::new();
    for i in 0..KEYS {
        let k = key(i);
        db.put(&k, &value).unwrap();
        golden.insert(k, value.clone());
        if db.stats().0.rotations != 0 {
            break;
        }
    }
    assert!(db.stats().0.rotations > 0);
    db.land().unwrap();
    // The queue has room for both memtables in this workload; this is fixture storage,
    // not a maintenance tuning threshold. No further rotation occurs before the witness.
    db.set_write_budget(2 * mem).unwrap();
    let held = HeldUntilDrop::new(gate);
    gate.open.store(false, Ordering::SeqCst);
    for _ in 0..KEYS {
        if db.stats().0.flushes != 0 {
            break;
        }
        db.idle_step(u64::try_from(mem).unwrap()).unwrap();
    }
    assert!(db.stats().0.flushes > 0);
    held.entered();
    assert!(
        db.stats().2.runs_queued > 0,
        "the public I/O report confirms queued output was admitted"
    );
    db.set_write_budget(0).unwrap();
    let retained = db.memory_held().1;
    assert!(
        retained > fixed,
        "admitted output is still owned after its queue cap shrinks"
    );
    let before = (db.memory_split(), db.memory_held(), db.write_budget());
    assert!(matches!(
        db.set_memory(retained - 1),
        Err(Error::LimitExceeded { .. })
    ));
    assert_eq!(
        (db.memory_split(), db.memory_held(), db.write_budget()),
        before
    );
    db.set_memory(retained).unwrap();
    db.set_write_budget(0).unwrap();
    drop(held);
    db.land().unwrap();
    assert!(!gate.expired.load(Ordering::SeqCst));
    assert_eq!(
        db.memory_held().1,
        retained,
        "completed output buffers remain warm and charged"
    );
    assert!(matches!(
        db.set_memory(retained - 1),
        Err(Error::LimitExceeded { .. })
    ));
    db.put(&key(0), b"newest").unwrap();
    golden.insert(key(0), b"newest".to_vec());
    db.delete(&key(1)).unwrap();
    golden.remove(&key(1));
    db.checkpoint(u64::try_from(KEYS).unwrap()).unwrap();
    check(&mut db, &golden);
    db.check_references().unwrap();
    let (file, landed) = finished_file(db.into_file());
    landed.unwrap();
    drop(file);
    drop(issuer);
    let (mut db, applied) =
        ShardDb::open(open(&path, false, gate, false).unwrap(), STORE, mem, TRUNK).unwrap();
    assert_eq!(applied, u64::try_from(KEYS).unwrap());
    check(&mut db, &golden);
    db.check_references().unwrap();
    let (file, landed) = finished_file(db.into_file());
    landed.unwrap();
    drop(file);
}
