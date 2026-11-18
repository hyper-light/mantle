//! A frame answered at its own flush (mantle docs/design/durability.md §7.1, §7.5, §7.7 step 4b):
//! the owner's decisions (`AtFlush`) say where, and today's answer at the flush nowhere. On a
//! simulated device that keeps every completed write (`SimFile::keeping`), with decision (B) taken
//! for it, a frame is answered at its flush with one flush and no confirmation, survives every crash
//! point by §7.6's laws, and its log's mark refuses the binaries before; a write-back device and a
//! one- or two-voter group's frame stay answered once confirmed, two durable operations a frame.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_types
)]

use hyper_block::DiskError;
use hyper_block::atomic::AtomicUnit;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{Caching, CachingRequest, DeviceFile};
use hyper_block::sim::{Crash, Fault, SimFile};
use hyper_block::write_cache::{DeviceCache, Source, WriteCache};
use hyper_log::{
    Answers, AtFlush, Config, Entries, Entry, Log, LogError, StatedCache, Tail, Update, Waits,
    With, WriteBackFrames,
};

const ID: u128 = 0x6d61_6e74_6c65_2d61_7466;
const BLOCK: usize = 4096;
const GROUP: u128 = 1;

fn config() -> Config {
    Config {
        segment_bytes: 16 * BLOCK as u64,
        max_segments: 8,
        max_groups: 64,
        group_entries: 1 << 16,
        group_bytes: 1 << 24,
        group_cache: 1 << 10,
        queue_submissions: 256,
        waits: Waits::Never,
    }
}

/// The owner's decision (B) taken, as the tests take it for the simulated device: a cache stated
/// to keep a completed write is trusted.
const TRUSTED: AtFlush = AtFlush {
    write_back: WriteBackFrames::Confirmed,
    stated: StatedCache::Trusted,
};

/// The owner's decision (A) taken: a write-back device answers at the flush.
const ACCEPTED: AtFlush = AtFlush {
    write_back: WriteBackFrames::AtFlush,
    stated: StatedCache::Unmeasured,
};

fn keeping(seed: u64) -> SimFile {
    SimFile::keeping(
        Alignment::new(BLOCK).unwrap(),
        Alignment::new(512).unwrap(),
        seed,
    )
    .unwrap()
}

fn write_back(seed: u64) -> SimFile {
    SimFile::new(
        Alignment::new(BLOCK).unwrap(),
        Alignment::new(512).unwrap(),
        seed,
    )
    .unwrap()
}

fn with(at_flush: AtFlush) -> With {
    With {
        at_flush,
        ..With::default()
    }
}

/// Entry `index` of term 1, its group's update allowing an answer at the flush where `at_flush`:
/// three voters or more, as the replica says (`Update::at_flush`).
fn update(index: u64, at_flush: bool) -> Update {
    Update {
        entries: Some(Entries {
            first: index,
            entries: vec![Entry {
                term: 1,
                bytes: index.to_le_bytes().to_vec(),
            }],
        }),
        at_flush,
        ..Update::default()
    }
}

/// The flushes and durable writes the device made, and the log's own count of its flushes,
/// durable writes and confirmations flushed.
fn costs(log: &Log<SimFile>) -> (u64, u64, u64, u64, u64) {
    let device = log.with_file(|f| f.stats().unwrap()).unwrap();
    let stats = log.stats(None).unwrap();
    (
        device.syncs,
        device.durable_writes,
        stats.flushes,
        stats.durable_writes,
        stats.flushed_confirmations,
    )
}

/// Writes entries `from..=to` one frame each and checks the durable operations each frame cost:
/// `flushes` and `durable` on the device, as the log counts them too.
fn each_costs(log: &Log<SimFile>, (from, to): (u64, u64), at_flush: bool, ops: u64) {
    for index in from..=to {
        let before = costs(log);
        log.write(GROUP, update(index, at_flush)).unwrap();
        let after = costs(log);
        let device = (after.0 - before.0) + (after.1 - before.1);
        let counted = (after.2 - before.2) + (after.3 - before.3);
        assert_eq!(
            device, ops,
            "write {index}: durable operations on the device"
        );
        assert_eq!(
            counted, ops,
            "write {index}: durable operations the log counts"
        );
    }
}

/// On a device that keeps every completed write, with decision (B) taken, a frame of three-voter
/// groups is answered at its flush: one flush, no confirmation, after an open as before it.
#[test]
fn a_frame_is_answered_at_its_flush_with_one_flush_where_the_owner_trusts_a_kept_write() {
    let log = Log::create_with(keeping(1), config(), ID, with(TRUSTED)).unwrap();
    assert_eq!(log.laid().answers, Answers::Kept);
    assert!(log.laid().cache.stated());
    assert!(
        log.laid().pieces.unwrap().at_flush,
        "the mark says so from the log's first flush"
    );
    each_costs(&log, (1, 4), true, 1);
    let stats = log.stats(None).unwrap();
    assert_eq!((stats.flushed_confirmations, stats.durable_writes), (0, 0));
    let (log, recovery) =
        Log::open_with(log.close().unwrap(), config(), ID, with(TRUSTED)).unwrap();
    assert_eq!(recovery.laid.answers, Answers::Kept);
    assert_eq!(log.view(GROUP).unwrap().unwrap().last, 4);
    each_costs(&log, (5, 6), true, 1);
}

/// Today's decisions answer at the flush nowhere: the same device answers once confirmed, two
/// durable operations a frame, and its mark says nothing.
#[test]
fn todays_decisions_answer_at_the_flush_nowhere() {
    assert_eq!(AtFlush::default(), AtFlush::DECIDED);
    assert_eq!(With::default().at_flush, AtFlush::DECIDED);
    let log = Log::create_with(keeping(2), config(), ID, with(AtFlush::DECIDED)).unwrap();
    assert_eq!(log.laid().answers, Answers::Unmeasured);
    assert!(!log.laid().pieces.unwrap().at_flush);
    each_costs(&log, (1, 4), true, 2);
}

/// A log opened under the decision sets its mark's bit, and flushes it, before its first frame;
/// a log of today's decisions opened later keeps the bit and answers once confirmed.
#[test]
fn the_open_marks_the_log_before_its_first_frame_answered_at_its_flush() {
    let log = Log::create_with(keeping(3), config(), ID, with(AtFlush::DECIDED)).unwrap();
    each_costs(&log, (1, 2), true, 2);
    let file = log.close().unwrap();
    let (log, recovery) = Log::open_with(file, config(), ID, with(TRUSTED)).unwrap();
    assert!(recovery.laid.pieces.unwrap().at_flush);
    assert_eq!(recovery.laid.answers, Answers::Kept);
    each_costs(&log, (3, 4), true, 1);
    let file = log.close().unwrap();
    // Durable before any frame: a crash that keeps nothing unflushed keeps the mark.
    file.crash(Crash::LoseAll).unwrap();
    let (log, recovery) = Log::open_with(file, config(), ID, with(AtFlush::DECIDED)).unwrap();
    assert!(recovery.laid.pieces.unwrap().at_flush, "the bit is kept");
    assert_eq!(recovery.laid.answers, Answers::Unmeasured);
    assert_eq!(log.view(GROUP).unwrap().unwrap().last, 4);
    each_costs(&log, (5, 6), true, 2);
}

/// A write-back device never answers at the flush under decision (B) alone, and its mark stays
/// clear; decision (A) is what would answer it there, accepting §7.5's fault (b).
#[test]
fn a_write_back_device_never_answers_at_its_flush_unless_decision_a_accepts_it() {
    let log = Log::create_with(write_back(4), config(), ID, with(TRUSTED)).unwrap();
    assert_eq!(log.laid().answers, Answers::WriteBack);
    assert!(!log.laid().pieces.unwrap().at_flush);
    each_costs(&log, (1, 4), true, 2);
    let log = Log::create_with(write_back(5), config(), ID, with(ACCEPTED)).unwrap();
    assert_eq!(log.laid().answers, Answers::Accepted);
    assert!(log.laid().pieces.unwrap().at_flush);
    each_costs(&log, (1, 4), true, 1);
}

/// A frame holding a group of one or two voters stays answered once confirmed wherever the log
/// answers at the flush: the others are no quorum without the member (§7.5).
#[test]
fn a_one_or_two_voter_group_is_never_answered_at_its_flush() {
    for (seed, policy) in [(6, TRUSTED), (7, ACCEPTED)] {
        let file = if policy == TRUSTED {
            keeping(seed)
        } else {
            write_back(seed)
        };
        let log = Log::create_with(file, config(), ID, with(policy)).unwrap();
        assert!(log.laid().answers.at_flush());
        each_costs(&log, (1, 3), false, 2);
        each_costs(&log, (4, 4), true, 1);
        each_costs(&log, (5, 5), false, 2);
    }
}

/// Every crash point of a frame answered at its flush, on the device that keeps completed writes:
/// the power cut at each of its writes and its flush, then every kind of crash. By §7.6's laws:
/// what was answered is held after the open; what was not was never answered; and no group is
/// marked, since a record written only after its frame's write completed proves the frame landed,
/// so no frame tears after its record.
#[test]
fn a_frame_answered_at_its_flush_survives_every_crash_point() {
    // The operations a frame answered at its flush makes: its writes and its flush.
    let log = Log::create_with(keeping(100), config(), ID, with(TRUSTED)).unwrap();
    each_costs(&log, (1, 3), true, 1);
    let before = log.with_file(|f| f.stats().unwrap()).unwrap();
    log.write(GROUP, update(4, true)).unwrap();
    let after = log.with_file(|f| f.stats().unwrap()).unwrap();
    let ops = (after.writes - before.writes) + (after.syncs - before.syncs);
    assert_eq!(ops, 3, "the frame's write, its record's and one flush");
    drop(log);
    let mut seed = 100;
    for cut in 0..=ops {
        for crash in [Crash::LoseAll, Crash::Random, Crash::KeepAll] {
            seed += 1;
            crash_at(seed, cut, cut >= ops, crash);
        }
    }
}

/// Three frames answered at their flush, then the fourth's power cut after `cut` of its writes
/// and flushes, and `crash`; `answers` says whether the cut comes after its flush.
fn crash_at(seed: u64, cut: u64, answers: bool, crash: Crash) {
    let log = Log::create_with(keeping(seed), config(), ID, with(TRUSTED)).unwrap();
    each_costs(&log, (1, 3), true, 1);
    log.with_file(move |f| f.inject(Fault::PowerCut { ops: cut }).unwrap())
        .unwrap();
    let answered = match log.write(GROUP, update(4, true)) {
        Ok(()) => true,
        Err(LogError::Fenced) => false,
        Err(e) => panic!("cut {cut}: {e:?}"),
    };
    assert_eq!(
        answered, answers,
        "cut {cut}: answered only once its flush ended"
    );
    log.with_file(move |f| f.crash(crash).unwrap()).unwrap();
    let file = log.close().unwrap();
    file.clear_faults().unwrap();
    let (log, recovery) = Log::open_with(file, config(), ID, with(TRUSTED)).unwrap();
    assert!(
        !matches!(recovery.tail, Tail::Undetermined { .. } | Tail::Lost { .. }),
        "cut {cut}, {crash:?}: no frame torn after its record ({:?})",
        recovery.tail
    );
    assert!(recovery.restored.is_empty() && recovery.damaged.is_empty());
    let view = log.view(GROUP).unwrap().unwrap();
    assert_eq!(view.uncertain, None, "cut {cut}, {crash:?}: nothing marked");
    let held = if answered { 4 } else { 3 };
    assert!(
        (held..=4).contains(&view.last),
        "cut {cut}, {crash:?}: answered entries held, and no more: {} of {held}",
        view.last
    );
    // The log goes on answering at the flush after the crash.
    each_costs(&log, (view.last + 1, view.last + 1), true, 1);
}

/// A real file whose device is stated to keep a completed write, as Linux's queue `write_cache`
/// "write through" states it: every other fact, whether its completed writes are the device's
/// among them (`BlockFile::writes_reach_device`), is the file's own.
#[derive(Debug)]
struct StatedThrough(DeviceFile);

impl BlockFile for StatedThrough {
    fn alignment(&self) -> Alignment {
        BlockFile::alignment(&self.0)
    }

    fn layout_block(&self) -> Alignment {
        BlockFile::layout_block(&self.0)
    }

    fn atomic_unit(&self) -> Result<AtomicUnit, DiskError> {
        BlockFile::atomic_unit(&self.0)
    }

    fn write_cache(&self) -> Result<DeviceCache, DiskError> {
        Ok(DeviceCache::new(
            WriteCache::WriteThrough,
            Source::Sysfs { fua: Some(true) },
        ))
    }

    fn writes_reach_device(&self) -> bool {
        BlockFile::writes_reach_device(&self.0)
    }

    fn len(&self) -> Result<u64, DiskError> {
        BlockFile::len(&self.0)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        BlockFile::read_exact_at(&self.0, buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        BlockFile::write_all_at(&self.0, buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        BlockFile::sync_data(&self.0)
    }

    fn fills_new_space(&self) -> bool {
        BlockFile::fills_new_space(&self.0)
    }
}

/// A real file asked for direct I/O, its device stated to write through, decision (B) taken: its
/// log answers at the flush (`Kept`) where the platform's direct path makes the file's completed
/// write the device's (`DeviceFile::writes_reach_device`: Linux's `O_DIRECT` on ext4 or XFS,
/// Windows' `FILE_FLAG_NO_BUFFERING`), its mark saying so from the log's first flush, and once
/// confirmed (`Buffered`) where it does not: macOS's `F_NOCACHE`, which promises no write the
/// device's, and every buffered file. Under today's decisions the same file answers once
/// confirmed, unmeasured, and its mark says nothing.
#[test]
fn a_direct_file_on_a_stated_write_through_cache_is_kept_where_its_writes_reach_the_device() {
    let dir = tempfile::tempdir().unwrap();
    for request in [CachingRequest::PreferDirect, CachingRequest::Buffered] {
        let file = real(dir.path(), request, "trusted");
        let reaches = as_its_platform_promises(&file);
        let kept = if reaches {
            Answers::Kept
        } else {
            Answers::Buffered
        };
        answered(StatedThrough(file), TRUSTED, kept);
        let file = real(dir.path(), request, "decided");
        answered(StatedThrough(file), AtFlush::DECIDED, Answers::Unmeasured);
    }
}

/// A new file in `dir`, opened as `request` asks.
fn real(dir: &std::path::Path, request: CachingRequest, name: &str) -> DeviceFile {
    let path = dir.join(format!("{request:?}-{name}"));
    DeviceFile::open(&path, true, request, Alignment::new(BLOCK).unwrap()).unwrap()
}

/// Whether `file`'s completed writes are the device's, checked against what its platform
/// promises of it as it was opened: never through a buffered handle or on macOS, and on Windows
/// wherever the handle is unbuffered.
fn as_its_platform_promises(file: &DeviceFile) -> bool {
    let reaches = file.writes_reach_device();
    if file.caching() == Caching::Buffered || cfg!(target_vendor = "apple") {
        assert!(
            !reaches,
            "{:?}: no completed write is the device's",
            file.caching()
        );
    }
    if cfg!(windows) {
        assert_eq!(reaches, file.caching() == Caching::Direct);
    }
    reaches
}

/// A log made on `file` under `policy` answers as `answers`, its mark saying whether it answers at
/// the flush from its first flush; after a frame and an open, it answers so still.
fn answered(file: StatedThrough, policy: AtFlush, answers: Answers) {
    let log = Log::create_with(file, config(), ID, with(policy)).unwrap();
    assert_eq!(log.laid().cache.cache(), WriteCache::WriteThrough);
    assert_eq!(log.laid().answers, answers);
    assert_eq!(log.laid().pieces.unwrap().at_flush, answers.at_flush());
    log.write(GROUP, update(1, true)).unwrap();
    let (log, recovery) = Log::open_with(log.close().unwrap(), config(), ID, with(policy)).unwrap();
    assert_eq!(recovery.laid.answers, answers);
    assert_eq!(log.view(GROUP).unwrap().unwrap().last, 1);
}
