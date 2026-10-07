//! The file grows only as its owner admits (`Growth`, `docs/durable.md` §6): a refusal reads as the
//! file's bound reached, never a fence; compaction lets writes resume within the slots held; a
//! write that fails to grow the file gives its admission back; a reopened log tells its owner what
//! its slots already take.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_types
)]

use std::sync::{Arc, Mutex};

use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::sim::{Fault, SimFile};
use hyper_log::{Config, Entries, Entry, Growth, Log, LogError, Start, Update, Waits, With};

const ID: u128 = 0x6772_6f77_7468;
const BLOCK: usize = 4096;
/// Bytes of a segment, and of the persist area before the first.
const SEGMENT: u64 = 16 * BLOCK as u64;
/// An entry's bytes: a few frames fill a segment.
const ENTRY: usize = 12 * 1024;

fn config() -> Config {
    Config {
        segment_bytes: SEGMENT,
        max_segments: 64,
        max_groups: 4,
        group_entries: 1 << 16,
        group_bytes: 1 << 26,
        group_cache: 1 << 10,
        queue_submissions: 16,
        waits: Waits::Never,
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

/// What a test's gate was told and asked, and what it admits: at most `budget` bytes held,
/// pending and committed together.
#[derive(Debug, Default)]
struct Ledger {
    budget: u64,
    told: Vec<u64>,
    held: u64,
    pending: u64,
    committed: u64,
    admits: u64,
    refusals: u64,
    releases: u64,
}

struct Budgeted(Arc<Mutex<Ledger>>);

impl Growth for Budgeted {
    fn held(&mut self, bytes: u64) {
        let mut l = self.0.lock().unwrap();
        l.told.push(bytes);
        l.held += bytes;
    }
    fn admit(&mut self, bytes: u64) -> bool {
        let mut l = self.0.lock().unwrap();
        if l.held + l.pending + l.committed + bytes > l.budget {
            l.refusals += 1;
            return false;
        }
        l.pending += bytes;
        l.admits += 1;
        true
    }
    fn commit(&mut self, bytes: u64) {
        let mut l = self.0.lock().unwrap();
        l.pending -= bytes;
        l.committed += bytes;
    }
    fn release(&mut self, bytes: u64) {
        let mut l = self.0.lock().unwrap();
        l.pending -= bytes;
        l.releases += 1;
    }
}

fn gated(budget: u64) -> (Arc<Mutex<Ledger>>, With) {
    let ledger = Arc::new(Mutex::new(Ledger {
        budget,
        ..Ledger::default()
    }));
    let with = With {
        growth: Some(Box::new(Budgeted(ledger.clone()))),
        ..With::default()
    };
    (ledger, with)
}

fn entry(index: u64) -> Update {
    Update {
        entries: Some(Entries {
            first: index,
            entries: vec![Entry {
                term: 1,
                bytes: vec![(index % 251) as u8; ENTRY],
            }],
        }),
        ..Update::default()
    }
}

/// Appends entries to group 1 from `next` until one is refused; the index refused and the error.
fn fill(log: &Log<SimFile>, next: &mut u64) -> LogError {
    for _ in 0..10_000 {
        match log.submit(1, entry(*next)).and_then(|p| p.wait()) {
            Ok(()) => *next += 1,
            Err(error) => return error,
        }
    }
    panic!("the log never refused a write");
}

/// A gate that admits the persist area and four slots: the fifth slot is refused, read as the
/// file's bound reached, `Full` and never `Fenced`; the refusal is counted; and the file takes no
/// more than was admitted.
#[test]
fn a_refused_slot_is_the_bound_reached_never_a_fence() {
    let budget = SEGMENT + 4 * SEGMENT;
    let (ledger, with) = gated(budget);
    let log = Log::create_with(sim(1), config(), ID, with).unwrap();
    let mut next = 1;
    assert!(matches!(fill(&log, &mut next), LogError::Full));
    assert!(log.stats(None).unwrap().growth_refused > 0);
    let len = log.with_file(|file| file.len()).unwrap().unwrap();
    {
        let l = ledger.lock().unwrap();
        assert!(l.refusals > 0);
        assert!(l.committed + l.pending <= budget);
        assert!(
            len <= l.committed + l.pending,
            "the file passed what was admitted"
        );
    }
    // The log still serves: what it holds reads back.
    assert_eq!(
        log.entries(1, 1, 2, u64::MAX).unwrap()[0].bytes.len(),
        ENTRY
    );
    drop(log.close().unwrap());
}

/// Fills `log` to its bound, lets go of all but the last entry, and counts the appends that fit
/// in the slots the compaction freed before the bound is met again.
fn resumed_after_compaction(log: &Log<SimFile>) -> u64 {
    let mut next = 1;
    assert!(matches!(fill(log, &mut next), LogError::Full));
    let last = next - 1;
    log.submit(
        1,
        Update {
            start: Some(Start {
                index: last,
                term: 1,
            }),
            ..Update::default()
        },
    )
    .unwrap()
    .wait()
    .unwrap();
    let before = next;
    assert!(matches!(fill(log, &mut next), LogError::Full));
    next - before
}

/// Once refused, a compaction frees the slots its entries held, and writes resume in them with no
/// admission more, exactly as many as under a file bounded by `max_segments` alone at the slots
/// the owner admitted: the refusal is that bound's path, not another.
#[test]
fn a_compaction_frees_slots_and_writes_resume_as_under_the_bound() {
    let (ledger, with) = gated(SEGMENT + 4 * SEGMENT);
    let gated = Log::create_with(sim(2), config(), ID, with).unwrap();
    let admits_at = |l: &Arc<Mutex<Ledger>>| l.lock().unwrap().admits;
    let resumed = resumed_after_compaction(&gated);
    let admits = admits_at(&ledger);
    assert!(resumed > 0, "no write resumed after the compaction");
    drop(gated.close().unwrap());
    // Four slots, as the gate admitted: the persist area and slot 0 at create, three more.
    let mut bounded = config();
    bounded.max_segments = 4;
    let plain = Log::create(sim(2), bounded, ID).unwrap();
    assert_eq!(resumed_after_compaction(&plain), resumed);
    drop(plain.close().unwrap());
    assert_eq!(admits_at(&ledger), admits, "the file grew again");
}

/// The write that grows the file into a new slot fails (another owner filled the volume): the log
/// fences, as before, and what it admitted and never made durable goes back first.
#[test]
fn a_failed_growth_gives_its_admission_back_and_fences() {
    let (ledger, with) = gated(u64::MAX / 2);
    let log = Log::create_with(sim(3), config(), ID, with).unwrap();
    let mut next = 1;
    // A few entries, then the volume ends where the next slot would begin.
    for _ in 0..3 {
        log.submit(1, entry(next)).unwrap().wait().unwrap();
        next += 1;
    }
    let held = {
        let l = ledger.lock().unwrap();
        l.held + l.committed
    };
    log.with_file(move |file| file.inject(Fault::Capacity { len: held }))
        .unwrap()
        .unwrap();
    assert!(matches!(fill(&log, &mut next), LogError::Fenced));
    {
        let l = ledger.lock().unwrap();
        assert_eq!(l.pending, 0, "an admission outlived the failed write");
        assert!(l.releases > 0);
        assert_eq!(l.held + l.committed, held, "a failed slot was held");
    }
    drop(log.close());
}

/// A log reopened tells its owner what its slots take, each a whole segment, before it admits
/// any: no less than the file's length, and less than a segment past it.
#[test]
fn a_reopened_log_tells_its_owner_what_its_slots_take() {
    let (_, with) = gated(u64::MAX / 2);
    let log = Log::create_with(sim(4), config(), ID, with).unwrap();
    let mut next = 1;
    for _ in 0..20 {
        log.submit(1, entry(next)).unwrap().wait().unwrap();
        next += 1;
    }
    let file = log.close().unwrap();
    let len = file.len().unwrap();
    let (ledger, with) = gated(u64::MAX / 2);
    let (log, _) = Log::open_with(file, config(), ID, with).unwrap();
    let told = ledger.lock().unwrap().told.clone();
    assert_eq!(told.len(), 1);
    let held = told[0];
    assert_eq!(held % SEGMENT, 0, "slots are whole segments");
    assert!(
        held >= len && held < len + SEGMENT,
        "held {held}, file {len}"
    );
    // And it goes on writing what is read back.
    log.submit(1, entry(next)).unwrap().wait().unwrap();
    assert_eq!(
        log.entries(1, next, next + 1, u64::MAX).unwrap()[0].bytes,
        vec![(next % 251) as u8; ENTRY]
    );
    drop(log.close().unwrap());
}

/// The admission of a new log's persist area and first slot is refused before anything is
/// written: `Full`, and the file stays empty.
#[test]
fn a_new_log_refused_its_first_slot_writes_nothing() {
    let (ledger, with) = gated(SEGMENT);
    match Log::create_with(sim(5), config(), ID, with) {
        Err(refused) => {
            assert!(matches!(refused.error, LogError::Full));
            assert_eq!(refused.file.unwrap().len().unwrap(), 0);
        }
        Ok(_) => panic!("a log was made past its admission"),
    }
    assert_eq!(ledger.lock().unwrap().refusals, 1);
}

/// Writes `count` entries from `next` and closes the log: the frames it wrote and its file.
fn written(log: Log<SimFile>, next: u64, count: u64) -> (u64, SimFile) {
    for index in next..next + count {
        log.submit(1, entry(index)).unwrap().wait().unwrap();
    }
    let frames = log.stats(None).unwrap().frames;
    (frames, log.close().unwrap())
}

/// A gate that admits everything changes nothing a log does, across a reopen too: the same
/// entries make the same frames and the same file as with no gate. A log reopened with a gate
/// decides its sweeps on the slots its owner admits ahead, never on the file's slots alone, which
/// would sweep segments it has room beside.
#[test]
fn a_gate_that_admits_everything_writes_as_no_gate() {
    let run = |gate: bool| {
        let with = || {
            if gate {
                gated(u64::MAX / 2).1
            } else {
                With::default()
            }
        };
        let log = Log::create_with(sim(6), config(), ID, with()).unwrap();
        let (_, file) = written(log, 1, 30);
        let (log, _) = Log::open_with(file, config(), ID, with()).unwrap();
        let (frames, file) = written(log, 31, 30);
        (frames, file.len().unwrap())
    };
    assert_eq!(run(true), run(false));
}
