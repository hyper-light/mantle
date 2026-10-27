//! The caller's code a log runs on its own threads, unwinding: a waker, a look at the file, the
//! file itself. Each runs inside an unwind boundary of its own (CLAUDE.md §1), so an unwind costs
//! only the call it was in: the log's device keeps working, every other caller is answered, and the
//! one whose code unwound is answered too, typed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_types,
    clippy::panic_in_result_fn
)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Wake, Waker};

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::sim::SimFile;
use hyper_log::{
    Class, Config, Entries, Entry, Fetched, HardState, Log, LogError, Page, Update, Waits,
};

use common::held;

const ID: u128 = 0x756e_7769_6e64;
const BLOCK: usize = 4096;

fn config() -> Config {
    Config {
        segment_bytes: 16 * BLOCK as u64,
        max_segments: 8,
        max_groups: 64,
        group_entries: 1 << 16,
        group_bytes: 1 << 24,
        group_cache: 1 << 10,
        queue_submissions: 256,
        waits: Waits::Measured,
    }
}

fn sim(seed: u64) -> SimFile {
    SimFile::new(
        Alignment::new(BLOCK).unwrap(),
        Alignment::new(512).unwrap(),
        seed,
    )
    .unwrap()
}

fn small(term: u64) -> Update {
    Update {
        hard_state: Some(HardState {
            term,
            vote: 1,
            commit: 0,
            held: None,
        }),
        ..Update::default()
    }
}

/// A waker that counts its wakes, then unwinds: a caller's code that panics.
struct Unwinding(AtomicU64);

impl Wake for Unwinding {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("a waker that unwinds");
    }
}

/// A waker that unwinds, woken by each of the log's threads that answers: the thread a frame's
/// I/O is done on (the I/O thread for a submission no one waits on, a blocking writer's own for
/// the frame it does) and the owner's (a fetch it answers from memory). Each caller is answered,
/// typed, its wake having unwound; the writer whose frame carried one is answered; and the device
/// works on, written, looked at and handed back at close.
#[test]
fn a_waker_that_unwinds_costs_only_its_wake() {
    let (device, holder) = held(sim(71));
    // Idle, a log under `Waits::Measured` fills its next slot ahead and flushes it, and that
    // flush may be the one held below in place of the frame before (`Owner::fill_ahead`).
    let cfg = Config {
        waits: Waits::Never,
        ..config()
    };
    let log = Log::create(device, cfg, ID).unwrap();
    let unwinding = Arc::new(Unwinding(AtomicU64::new(0)));
    let waker = Waker::from(unwinding.clone());
    let mut woken = 0;

    // A frame done by the I/O thread, its answers given there.
    let alone = log
        .submit_waking(1, Class::Normal, small(1), waker.clone())
        .unwrap();
    alone.wait().unwrap();
    woken += 1;

    // A frame carrying submissions whose wakers unwind and a blocking writer, which does the
    // frame's I/O and, no frame following, confirms it and gives its answers on its own thread:
    // the device is held in the frame before until all are taken, each of another group so that
    // one frame takes them all.
    std::thread::scope(|s| {
        holder.hold();
        let _released = holder.released();
        log.submit(2, small(1)).unwrap();
        holder.held();
        let carried: Vec<_> = (3..6)
            .map(|group| {
                log.submit_waking(group, Class::Normal, small(1), waker.clone())
                    .unwrap()
            })
            .collect();
        let writer = s.spawn(|| log.write(6, small(1)));
        holder.release();
        writer.join().unwrap().unwrap();
        for pending in carried {
            pending.wait().unwrap();
            woken += 1;
        }
    });

    // A fetch the owner answers from memory, waking on the owner's thread.
    log.write(
        7,
        Update {
            entries: Some(Entries {
                first: 1,
                entries: vec![Entry {
                    term: 1,
                    bytes: vec![5; 64],
                }],
            }),
            ..Update::default()
        },
    )
    .unwrap();
    let fetched = log
        .fetch_waking(
            7,
            1,
            2,
            Page::payload(u64::MAX),
            Fetched::default(),
            Some(waker.clone()),
        )
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(fetched.iter().count(), 1);
    woken += 1;

    log.write(8, small(1)).unwrap();
    assert!(log.with_file(|file| file.file().len().is_ok()).unwrap());
    let file = log.close().unwrap();
    assert!(file.into_inner().len().unwrap() > 0);
    // A caller's wait returns once its answer is in its port, and the thread that answered wakes
    // its waker after that (`Ticket::answer`): each wake has run once the threads that answer have
    // ended, the writer's in the scope above and the log's at its close.
    assert_eq!(
        unwinding.0.load(Ordering::SeqCst),
        woken,
        "each wake ran, once"
    );
}

/// A look at the file that unwinds is answered as a failed operation on the file, typed, and the
/// log goes on: written, looked at again, closed.
#[test]
fn a_look_that_unwinds_is_answered_and_the_log_goes_on() {
    let log = Log::create(sim(72), config(), ID).unwrap();
    log.write(1, small(1)).unwrap();
    let looked = log.with_file(|_| -> u64 { panic!("a look that unwinds") });
    match looked {
        Err(LogError::Disk(DiskError::Io { op, .. })) => {
            assert_eq!(op, "a look at the file unwound")
        }
        other => panic!("{other:?}"),
    }
    log.write(1, small(2)).unwrap();
    assert!(log.with_file(|file| file.len().is_ok()).unwrap());
    log.close().unwrap();
}

/// A panic payload whose drop unwinds too.
struct Bomb;

impl Drop for Bomb {
    fn drop(&mut self) {
        panic!("a payload that unwinds as it drops");
    }
}

/// A look whose unwind carries a payload that unwinds again as it drops is answered the same way:
/// the payload is dropped inside a boundary of its own, and the log goes on.
#[test]
fn a_look_whose_payload_unwinds_as_it_drops_is_answered_and_the_log_goes_on() {
    let log = Log::create(sim(74), config(), ID).unwrap();
    log.write(1, small(1)).unwrap();
    let looked = log.with_file(|_| -> u64 { std::panic::panic_any(Bomb) });
    match looked {
        Err(LogError::Disk(DiskError::Io { op, .. })) => {
            assert_eq!(op, "a look at the file unwound")
        }
        other => panic!("{other:?}"),
    }
    log.write(1, small(2)).unwrap();
    log.close().unwrap();
}

/// A file whose flush unwinds fails the write, typed (the log fences, as for any failed flush),
/// and the device comes back: a look still runs on it and close hands it back.
#[test]
fn a_file_that_unwinds_fails_its_write_and_keeps_the_device() {
    let log = Log::create(Unwinds::new(sim(73)), config(), ID).unwrap();
    log.write(1, small(1)).unwrap();
    log.with_file(|file| file.arm()).unwrap();
    assert!(matches!(
        log.write(1, small(2)),
        Err(LogError::Fenced | LogError::Disk(_))
    ));
    assert!(log.is_fenced());
    assert!(log.with_file(|file| file.unwound()).unwrap());
    log.close().unwrap();
}

/// A simulated file whose next flush unwinds once armed.
struct Unwinds {
    file: SimFile,
    armed: std::cell::Cell<bool>,
    unwound: std::cell::Cell<bool>,
}

impl Unwinds {
    fn new(file: SimFile) -> Self {
        Self {
            file,
            armed: std::cell::Cell::new(false),
            unwound: std::cell::Cell::new(false),
        }
    }

    fn arm(&self) {
        self.armed.set(true);
    }

    fn unwound(&self) -> bool {
        self.unwound.get()
    }
}

impl BlockFile for Unwinds {
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
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        if self.armed.replace(false) {
            self.unwound.set(true);
            panic!("a flush that unwinds");
        }
        self.file.sync_data()
    }
}
