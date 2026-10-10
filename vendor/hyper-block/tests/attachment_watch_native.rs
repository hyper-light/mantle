//! Independent physical-fence behavior. Source-only proposal; external guards are failure only.
#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_types,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation
)]

use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Condvar, Mutex};

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::{AlignedBuf, Alignment};
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};

/// The filesystem page/alignment used by the existing native issuer fixtures.
const PAGE: usize = 4096;

#[derive(Debug, PartialEq, Eq)]
enum Notice {
    WriteHeld,
    TlsHeld,
}

struct Status {
    alive: AtomicUsize,
    held: AtomicBool,
    original_dropped: AtomicBool,
    fail_write: AtomicBool,
    fail_read: AtomicBool,
    panic_duplicate_drop: AtomicBool,
    hold_tls: AtomicBool,
    tls_ended: AtomicBool,
    tls_open: Mutex<bool>,
    open: Mutex<bool>,
    changed: Condvar,
    entered: SyncSender<Notice>,
}

struct NativeTls(&'static Status);
impl Drop for NativeTls {
    fn drop(&mut self) {
        if self.0.hold_tls.load(Ordering::SeqCst) {
            self.0.entered.try_send(Notice::TlsHeld).unwrap();
            let mut open = self.0.tls_open.lock().unwrap();
            while !*open {
                open = self.0.changed.wait(open).unwrap();
            }
        }
        self.0.tls_ended.store(true, Ordering::SeqCst);
    }
}
thread_local! { static NATIVE_TLS: Cell<Option<NativeTls>> = const { Cell::new(None) }; }

struct Probe {
    file: DeviceFile,
    path: PathBuf,
    status: &'static Status,
    duplicate: bool,
}

struct Payload;
impl Drop for Payload {
    fn drop(&mut self) {
        panic!("watch probe panic payload Drop");
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        self.status.alive.fetch_sub(1, Ordering::SeqCst);
        if !self.duplicate {
            self.status.original_dropped.store(true, Ordering::SeqCst);
        } else if self
            .status
            .panic_duplicate_drop
            .swap(false, Ordering::SeqCst)
        {
            std::panic::panic_any(Payload);
        }
    }
}

impl BlockFile for Probe {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }
    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }
    fn read_exact_at(&self, bytes: &mut [u8], at: u64) -> Result<(), DiskError> {
        if self.status.fail_read.swap(false, Ordering::SeqCst) {
            return Err(failed(
                &self.path,
                "read watch probe",
                "repairable probe read",
            ));
        }
        self.file.read_exact_at(bytes, at)
    }
    fn write_all_at(&self, bytes: &[u8], at: u64) -> Result<(), DiskError> {
        NATIVE_TLS.with(|slot| {
            let native = slot.take().unwrap_or_else(|| NativeTls(self.status));
            slot.set(Some(native));
        });
        let mut open = self
            .status
            .open
            .lock()
            .map_err(|_| failed(&self.path, "hold watch probe", "write gate poisoned"))?;
        if !*open && !self.status.held.swap(true, Ordering::SeqCst) {
            self.status
                .entered
                .try_send(Notice::WriteHeld)
                .map_err(|_| {
                    failed(
                        &self.path,
                        "signal watch probe",
                        "write witness unavailable",
                    )
                })?;
        }
        while !*open {
            open = self
                .status
                .changed
                .wait(open)
                .map_err(|_| failed(&self.path, "hold watch probe", "write gate poisoned"))?;
        }
        drop(open);
        if self.status.fail_write.swap(false, Ordering::SeqCst) {
            return Err(failed(
                &self.path,
                "write watch probe",
                "actual probe write failure",
            ));
        }
        self.file.write_all_at(bytes, at)
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        let file = self.file.try_clone()?;
        self.status.alive.fetch_add(1, Ordering::SeqCst);
        Ok(Self {
            file,
            path: self.path.clone(),
            status: self.status,
            duplicate: true,
        })
    }
}

fn failed(path: &Path, op: &'static str, detail: &'static str) -> DiskError {
    DiskError::Io {
        op,
        path: path.to_path_buf(),
        source: std::io::Error::other(detail),
    }
}

fn setup(path: &Path, open: bool) -> (Probe, std::sync::mpsc::Receiver<Notice>) {
    let (entered, held) = sync_channel(1); // One selected actual write callback.
    let status = Box::leak(Box::new(Status {
        alive: AtomicUsize::new(1),
        held: AtomicBool::new(false),
        original_dropped: AtomicBool::new(false),
        fail_write: AtomicBool::new(false),
        fail_read: AtomicBool::new(false),
        panic_duplicate_drop: AtomicBool::new(false),
        hold_tls: AtomicBool::new(false),
        tls_ended: AtomicBool::new(false),
        tls_open: Mutex::new(false),
        open: Mutex::new(open),
        changed: Condvar::new(),
        entered,
    }));
    let file = DeviceFile::open(
        path,
        true,
        CachingRequest::Buffered,
        Alignment::new(PAGE).unwrap(),
    )
    .unwrap();
    (
        Probe {
            file,
            path: path.to_path_buf(),
            status,
            duplicate: false,
        },
        held,
    )
}

fn page(fill: u8) -> Vec<(AlignedBuf, u64)> {
    let mut buf = AlignedBuf::zeroed(PAGE, Alignment::new(PAGE).unwrap()).unwrap();
    buf.extend_from_slice(&[fill; PAGE]).unwrap();
    vec![(buf, 0)]
}

fn release(status: &Status) {
    *status
        .open
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = true;
    *status
        .tls_open
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = true;
    status.changed.notify_all();
}
struct OpenOnDrop(&'static Status);
impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        release(self.0);
    }
}

fn runtime() -> LocalRuntime {
    // One root task returns the exact owned attachment; no admission/identity probe.
    LocalRuntime::new(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: 1,
        timers_per_shard: 1,
        interests_per_shard: interests_for(1),
        ring_entries: 1,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 1,
        pin: false,
        cores: Vec::new(),
        page_bytes: PAGE,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap()
}

fn write_reason(result: Result<(), DiskError>) {
    assert!(
        matches!(result, Err(DiskError::Io { op: "write watch probe", source, .. })
        if source.to_string() == "actual probe write failure")
    );
}

fn assert_invalid_context<T>(result: Result<T, DiskError>) {
    assert!(
        matches!(result, Err(DiskError::Io { source, .. }) if source.kind() == std::io::ErrorKind::InvalidInput)
    );
}

#[test]
fn watch_registration_refuses_on_shard_before_changing_credits_and_is_once_only() {
    let dir = tempfile::tempdir().unwrap();
    let (probe, _) = setup(&dir.path().join("watch"), true);
    let issuer = Issuer::start(dir.path(), 1).unwrap();
    assert_eq!(issuer.depth(), 1);
    let attached = issuer.attach_deep(&probe, 1).unwrap();
    let (mut attached, result, out) = runtime()
        .block_on(async move {
            let mut attached = attached;
            let before = attached.out();
            let result = attached.prepare_retirement_watch();
            (attached, result, before)
        })
        .unwrap();
    assert_invalid_context(result);
    assert_eq!(attached.out(), out);
    let mut watch = attached.prepare_retirement_watch().unwrap();
    assert!(attached.prepare_retirement_watch().is_err());
    attached.write(page(0x5a), true).unwrap();
    attached.retire_blocking().unwrap();
    let (mut watch, refused, retired) = runtime()
        .block_on(async move {
            let refused = watch.wait_blocking();
            let retired = watch.is_retired();
            (watch, refused, retired)
        })
        .unwrap();
    assert_invalid_context(refused);
    assert!(!retired); // Ready terminal value is retained across a context refusal.
    watch.wait_blocking().unwrap();
    assert!(watch.is_retired());
    watch.wait_blocking().unwrap();
    assert_eq!(probe.status.alive.load(Ordering::SeqCst), 1);
    let mut bytes = vec![0; PAGE];
    probe.read_exact_at(&mut bytes, 0).unwrap();
    assert_eq!(bytes, vec![0x5a; PAGE]);
}

#[test]
fn watch_preserves_an_actual_write_failure_after_its_numbered_answer_was_consumed() {
    let dir = tempfile::tempdir().unwrap();
    let (probe, _) = setup(&dir.path().join("watch"), true);
    let issuer = Issuer::start(dir.path(), 1).unwrap();
    assert_eq!(issuer.depth(), 1);
    let mut attached = issuer.attach(&probe).unwrap();
    probe.status.fail_write.store(true, Ordering::SeqCst);
    let number = attached.submit(page(0x33), true).unwrap();
    let (answered, result) = attached.answer().unwrap();
    assert_eq!(answered, number);
    write_reason(result.map(|_| ()));
    assert_eq!(attached.out(), 0);
    // Registration after answer consumption cannot erase that attachment's physical error.
    let mut watch = attached.prepare_retirement_watch().unwrap();
    attached.retire_blocking().unwrap(); // Legacy Detached result is unchanged.
    write_reason(watch.wait_blocking());
    assert!(watch.is_retired());
    write_reason(watch.wait_blocking());
    assert_eq!(probe.status.alive.load(Ordering::SeqCst), 1);
}

#[test]
fn a_repaired_read_failure_does_not_poison_the_physical_fence() {
    let dir = tempfile::tempdir().unwrap();
    let (probe, _) = setup(&dir.path().join("watch"), true);
    probe.file.write_all_at(&[0x71; PAGE], 0).unwrap();
    let issuer = Issuer::start(dir.path(), 1).unwrap();
    assert_eq!(issuer.depth(), 1);
    let mut attached = issuer.attach(&probe).unwrap();
    let mut watch = attached.prepare_retirement_watch().unwrap();
    probe.status.fail_read.store(true, Ordering::SeqCst);
    attached.submit_reads(page(0)).unwrap();
    assert!(matches!(
        attached.answer().unwrap().1,
        Err(DiskError::Io {
            op: "read watch probe",
            ..
        })
    ));
    attached.submit_reads(page(0)).unwrap();
    let back = attached.answer().unwrap().1.unwrap();
    assert_eq!(back[0].0.as_slice(), &[0x71; PAGE]);
    attached.retire_blocking().unwrap();
    watch.wait_blocking().unwrap();
    assert!(watch.is_retired());
}

#[test]
fn an_entered_owner_drop_keeps_the_watch_and_original_alive_until_held_io_retires() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("watch");
    let (probe, held) = setup(&path, false);
    let status = probe.status;
    let issuer = Issuer::start(dir.path(), 1).unwrap();
    assert_eq!(issuer.depth(), 1);
    let mut attached = issuer.attach_deep(&probe, 2).unwrap(); // Two accepted numbered batches.
    let mut watch = attached.prepare_retirement_watch().unwrap();
    let _cleanup = OpenOnDrop(status); // Opens before Attached/Issuer Drop on setup failure.
    attached.submit(page(0x49), true).unwrap();
    let mut second = page(0x4a);
    second[0].1 = PAGE as u64;
    attached.submit(second, true).unwrap();
    assert_eq!(held.recv().unwrap(), Notice::WriteHeld); // Actual native write.
    assert!(status.held.load(Ordering::SeqCst));
    eprintln!("[WATCH_WRITE_HELD] two accepted batches before entered owner Drop");
    std::thread::scope(|scope| {
        let _release = OpenOnDrop(status); // Before spawn/admission or implicit scope joins.
        let waiter = scope.spawn(move || {
            let result = watch.wait_blocking();
            let retired = watch.is_retired();
            drop(probe); // Simulates the cold original-file fence consumer.
            (result, retired)
        });
        assert_eq!(
            runtime()
                .block_on(async move {
                    drop(attached);
                    7_u64
                })
                .unwrap(),
            7
        );
        assert!(!status.original_dropped.load(Ordering::SeqCst));
        assert!(!waiter.is_finished());
        release(status);
        let (result, retired) = waiter.join().unwrap();
        result.unwrap();
        assert!(retired);
    });
    drop(issuer);
    assert_eq!(status.alive.load(Ordering::SeqCst), 0);
    let file = DeviceFile::open(
        &path,
        false,
        CachingRequest::Buffered,
        Alignment::new(PAGE).unwrap(),
    )
    .unwrap();
    let mut bytes = vec![0; 2 * PAGE];
    file.read_exact_at(&mut bytes, 0).unwrap();
    assert_eq!(&bytes[..PAGE], &[0x49; PAGE]);
    assert_eq!(&bytes[PAGE..], &[0x4a; PAGE]);
}

#[test]
fn an_unexpected_worker_exit_preserves_prior_write_cause_through_held_native_tls() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("watch");
    let (probe, held) = setup(&path, true);
    let status = probe.status;
    let issuer = Issuer::start(dir.path(), 1).unwrap();
    assert_eq!(issuer.depth(), 1);
    let mut attached = issuer.attach(&probe).unwrap();
    attached.write(page(0x2f), true).unwrap(); // Positive durable output before the fault.
    status.fail_write.store(true, Ordering::SeqCst);
    let number = attached.submit(page(0x31), true).unwrap();
    let (answered, result) = attached.answer().unwrap();
    assert_eq!(answered, number);
    write_reason(result.map(|_| ()));
    let mut watch = attached.prepare_retirement_watch().unwrap();
    let _cleanup = OpenOnDrop(status);
    status.hold_tls.store(true, Ordering::SeqCst);
    status.panic_duplicate_drop.store(true, Ordering::SeqCst);
    std::thread::scope(|scope| {
        let _release = OpenOnDrop(status);
        let waiting = scope.spawn(move || {
            let result = watch.wait_blocking();
            let retired = watch.is_retired();
            drop(probe);
            (result, retired)
        });
        let retiring = scope.spawn(move || attached.retire_blocking());
        assert_eq!(held.recv().unwrap(), Notice::TlsHeld);
        eprintln!("[WATCH_TLS_HELD] unexpected worker exit after completed and failed writes");
        assert!(!status.tls_ended.load(Ordering::SeqCst));
        assert!(!status.original_dropped.load(Ordering::SeqCst));
        assert!(!waiting.is_finished());
        assert!(!retiring.is_finished());
        release(status);
        assert!(retiring.join().unwrap().is_err());
        let (result, retired) = waiting.join().unwrap();
        write_reason(result); // Earlier actual write, rather than generic lifecycle/Stop.
        assert!(retired);
        assert!(status.tls_ended.load(Ordering::SeqCst));
    });
    drop(issuer);
    assert_eq!(status.alive.load(Ordering::SeqCst), 0);
    let file = DeviceFile::open(
        &path,
        false,
        CachingRequest::Buffered,
        Alignment::new(PAGE).unwrap(),
    )
    .unwrap();
    let mut bytes = vec![0; PAGE];
    file.read_exact_at(&mut bytes, 0).unwrap();
    assert_eq!(bytes, vec![0x2f; PAGE]);
}

#[test]
fn issuer_shutdown_keeps_the_watch_pending_through_actual_native_tls() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("watch");
    let (probe, held) = setup(&path, true);
    let status = probe.status;
    let issuer = Issuer::start(dir.path(), 1).unwrap();
    assert_eq!(issuer.depth(), 1);
    let mut attached = issuer.attach(&probe).unwrap();
    let mut watch = attached.prepare_retirement_watch().unwrap();
    attached.write(page(0x66), true).unwrap();
    status.hold_tls.store(true, Ordering::SeqCst);
    let _cleanup = OpenOnDrop(status);
    std::thread::scope(|scope| {
        let _release = OpenOnDrop(status); // Before implicit joins on any assertion failure.
        let waiting = scope.spawn(move || {
            let result = watch.wait_blocking();
            let retired = watch.is_retired();
            drop(probe);
            (result, retired)
        });
        let stopping = scope.spawn(move || drop(issuer));
        assert_eq!(held.recv().unwrap(), Notice::TlsHeld);
        eprintln!("[WATCH_TLS_HELD] stopped issuer with completed write");
        assert!(!status.tls_ended.load(Ordering::SeqCst));
        assert!(!status.original_dropped.load(Ordering::SeqCst));
        assert!(!waiting.is_finished());
        release(status);
        stopping.join().unwrap();
        let (result, retired) = waiting.join().unwrap();
        assert!(
            matches!(result, Err(DiskError::Io { source, .. }) if source.kind() == std::io::ErrorKind::BrokenPipe)
        );
        assert!(retired);
        assert!(status.tls_ended.load(Ordering::SeqCst));
    });
    drop(attached);
    assert_eq!(status.alive.load(Ordering::SeqCst), 0);
    let file = DeviceFile::open(
        &path,
        false,
        CachingRequest::Buffered,
        Alignment::new(PAGE).unwrap(),
    )
    .unwrap();
    let mut bytes = vec![0; PAGE];
    file.read_exact_at(&mut bytes, 0).unwrap();
    assert_eq!(bytes, vec![0x66; PAGE]);
}
