//! The slot past the file's last is written with zeros ahead of its first frame, a piece at a time
//! while the log has nothing else to write, and flushed with its last piece (`docs/durable.md`
//! §6.3), so the frames in it are overwrites; a frame that opens a slot before it is filled writes
//! into it as into any unwritten space, and writes no zeros itself. Zeros past the last frame read
//! as no frame, as the file's end does; a power cut anywhere in a growing frame's writes or in a
//! fill leaves a log that opens with what was acknowledged and nothing else, and a fill that fails
//! fences nothing.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::sim::{Crash, Fault, SimFile};
use hyper_log::format::{FRAME_MAGIC, FrameHeader, SEGMENT_MAGIC, SegmentHeader};
use hyper_log::{Config, Entries, Entry, Log, LogError, LogStats, Update, Waits};

const ID: u128 = 0x7072_6561_6c6c_6f63;
const BLOCK: usize = 4096;
const SEGMENT: u64 = 16 * BLOCK as u64;
/// An entry a frame holds alone and two of which no segment does: every second frame opens a slot.
const ENTRY: usize = 36 * 1024;
/// A small entry: its frame and its persist record a block each.
const SMALL: usize = 1024;

/// A writer that forms each batch from what is queued: idle, it fills nothing ahead, so a test
/// that aims a fault at a frame's writes meets only the frame's.
fn config() -> Config {
    Config {
        segment_bytes: SEGMENT,
        max_segments: 16,
        max_groups: 4,
        group_entries: 1 << 16,
        group_bytes: 1 << 26,
        group_cache: 1 << 10,
        queue_submissions: 16,
        waits: Waits::Never,
    }
}

/// A writer that waits as a busy log does, and fills the next slot ahead while idle.
fn measured() -> Config {
    Config {
        waits: Waits::Measured,
        ..config()
    }
}

/// [`measured`] with slots of 64 blocks: a fill of 32 two-block pieces, long enough that a fault
/// injected once it begins meets it.
fn measured_long() -> Config {
    Config {
        segment_bytes: 4 * SEGMENT,
        ..measured()
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

fn entry(index: u64, size: usize) -> Update {
    Update {
        entries: Some(Entries {
            first: index,
            entries: vec![Entry {
                term: 1,
                bytes: vec![(index % 251) as u8; size],
            }],
        }),
        ..Update::default()
    }
}

fn len(log: &Log<SimFile>) -> u64 {
    log.with_file(|f| f.len()).unwrap().unwrap()
}

/// Waits until the log's statistics meet `met`, charged to the fill's own steps: a look at the file
/// is a job for the device, queued behind the one it has, so when a look returns the piece of a fill
/// the device was writing is written, and the log, idle again, has given it the next. A fill of a
/// slot therefore ends within as many looks as the slot has pieces, at most one a block, and the
/// wait fails only once two such fills' looks passed without `met`: it is charged to the pieces a
/// fill writes, never to the time a busy machine takes to write them.
fn until(log: &Log<SimFile>, what: &str, met: impl Fn(&LogStats) -> bool) -> Box<LogStats> {
    let looks = 2 * (4 * SEGMENT / BLOCK as u64) + 2;
    for _ in 0..looks {
        let stats = log.stats(None).unwrap();
        if met(&stats) {
            return stats;
        }
        log.with_file(|_| ()).unwrap();
    }
    let stats = log.stats(None).unwrap();
    assert!(
        met(&stats),
        "{what}: not met in {looks} of the device's turns: {stats:?}"
    );
    stats
}

/// Writes small entries from `index` until a frame opens the slot filled ahead and the fill of the
/// slot after it has begun: the index the next write takes. After each write the device is given a
/// look, so the log, idle once the write is answered, has its turn to begin a fill that is due.
fn open_the_filled_slot(log: &Log<SimFile>, mut index: u64) -> u64 {
    let pieces = log.stats(None).unwrap().fill_pieces;
    loop {
        log.write(1, entry(index, SMALL)).unwrap();
        index += 1;
        log.with_file(|_| ()).unwrap();
        if log.stats(None).unwrap().fill_pieces > pieces {
            return index;
        }
        assert!(index < 1_000, "no slot after the first was filled");
    }
}

/// The small entries before `upto` read back as written.
fn small_entries_read_back(log: &Log<SimFile>, upto: u64, what: &str) {
    for i in 1..upto {
        assert_eq!(
            log.entries(1, i, i + 1, u64::MAX).unwrap()[0].bytes,
            vec![(i % 251) as u8; SMALL],
            "{what}: entry {i}"
        );
    }
}

/// A block of zeros is no frame and no segment: neither magic has a zero byte, and each header is
/// read only where its magic is. So recovery reads a zeroed tail as the end, never as a damaged
/// frame.
#[test]
fn a_zeroed_block_is_no_header() {
    assert!(FRAME_MAGIC.iter().all(|&b| b != 0));
    assert!(SEGMENT_MAGIC.iter().all(|&b| b != 0));
    let zeros = vec![0u8; BLOCK];
    assert!(FrameHeader::decode(&zeros).is_none());
    assert!(SegmentHeader::decode(&zeros).is_none());
}

/// The file spans its first slot from its creation, does not grow while frames fill a slot, and a
/// frame that opens the next writes no zeros: the file grows by the frame's own blocks, not a
/// whole slot, and nothing is counted zeroed. A frame that opened a growing slot used to write the
/// slot's zeros under its own flush, and every frame behind it waited for them (focal's A/B
/// 39436443557: 10,000/s p99 24.4 ms with them, 3.7 ms without).
#[test]
fn a_frame_that_opens_a_slot_writes_no_zeros() {
    let log = Log::create(sim(1), config(), ID).unwrap();
    let created = len(&log);
    assert_eq!(
        created,
        2 * SEGMENT,
        "the persist area and the first slot, whole"
    );
    // Small entries fill the first slot without growing the file.
    let mut index = 1;
    while len(&log) == created {
        log.write(1, entry(index, SMALL)).unwrap();
        index += 1;
        assert!(index < 1_000, "the file never grew");
    }
    let grown = len(&log) - created;
    assert!(
        grown > 0 && grown < SEGMENT,
        "the opening frame's blocks alone: {grown}"
    );
    let stats = log.stats(None).unwrap();
    assert_eq!((stats.zeroed_bytes, stats.fills), (0, 0));
    // And reads back across both slots after a reopen.
    let file = log.close().unwrap();
    let (log, _) = Log::open(file, config(), ID).unwrap();
    for i in 1..index {
        assert_eq!(
            log.entries(1, i, i + 1, u64::MAX).unwrap()[0].bytes.len(),
            SMALL
        );
    }
    drop(log.close().unwrap());
}

/// An idle log writes the next slot whole with zeros ahead of its first frame, in pieces no larger
/// than its last frame's writes, and flushes it once, with its last piece: with no frame yet a
/// piece is a block, sixteen to a slot; after frames of a block and records of a block it is two,
/// or three after the frame that opened a slot, its segment's header beside it, so six to eight
/// pieces fill a slot. The frames that fill a slot filled ahead are overwrites and grow the file
/// by none of it; every entry reads back after a reopen.
#[test]
fn an_idle_log_fills_the_next_slot_ahead_in_pieces_no_larger_than_its_frames() {
    let log = Log::create(sim(2), measured(), ID).unwrap();
    let first = until(&log, "the first fill", |s| s.fills == 1);
    assert_eq!(len(&log), 3 * SEGMENT, "the next slot, whole");
    assert_eq!(first.zeroed_bytes, SEGMENT);
    assert_eq!(first.fill_pieces, SEGMENT / BLOCK as u64, "a block a piece");
    let index = open_the_filled_slot(&log, 1);
    let second = until(&log, "the second fill", |s| s.fills == 2);
    let pieces = second.fill_pieces - first.fill_pieces;
    assert!(
        (SEGMENT.div_ceil(3 * BLOCK as u64)..=SEGMENT / (2 * BLOCK as u64)).contains(&pieces),
        "{pieces} pieces: two or three blocks each, as the frames before them"
    );
    assert_eq!(second.zeroed_bytes, 2 * SEGMENT);
    assert_eq!(second.fill_failures, 0);
    // The persist area and three slots, each whole: the frames in the second grew the file by none
    // of it.
    assert_eq!(len(&log), 4 * SEGMENT);
    let file = log.close().unwrap();
    let (log, recovery) = Log::open(file, measured(), ID).unwrap();
    assert!(recovery.damaged.is_empty());
    small_entries_read_back(&log, index, "reopened");
    drop(log.close().unwrap());
}

/// Power is cut at each of a growing frame's operations (the frame's write, its record's, the
/// flush), under each way a crash keeps what was not flushed. The log opens with every entry it
/// acknowledged; the one it did not is gone or whole, as a write whose answer was lost may be,
/// never damaged; and it takes writes again.
#[test]
fn a_power_cut_anywhere_in_a_growing_frame_leaves_what_was_acknowledged() {
    for ops in 0..3 {
        for (seed, crash) in [Crash::Random, Crash::KeepAll, Crash::LoseAll]
            .into_iter()
            .enumerate()
        {
            let log = Log::create(sim(10 + seed as u64), config(), ID).unwrap();
            // The first entry fills most of slot 0; the second opens slot 1.
            log.write(1, entry(1, ENTRY)).unwrap();
            log.with_file(move |f| f.inject(Fault::PowerCut { ops }).unwrap())
                .unwrap();
            assert!(matches!(
                log.write(1, entry(2, ENTRY)),
                Err(LogError::Fenced)
            ));
            log.with_file(move |f| f.crash(crash).unwrap()).unwrap();
            let file = log.close().unwrap();
            file.clear_faults().unwrap();
            let (log, _) = Log::open(file, config(), ID).unwrap();
            let view = log.view(1).unwrap().unwrap();
            assert!(
                view.last == 1 || view.last == 2,
                "ops {ops}, {crash:?}: last {}",
                view.last
            );
            for (index, byte) in (1..=view.last).zip([1u8, 2]) {
                assert_eq!(
                    log.entries(1, index, index + 1, u64::MAX).unwrap()[0].bytes,
                    vec![byte; ENTRY],
                    "ops {ops}, {crash:?}: entry {index}"
                );
            }
            log.write(1, entry(2, ENTRY)).unwrap();
            log.write(1, entry(3, ENTRY)).unwrap();
            let file = log.close().unwrap();
            let (log, _) = Log::open(file, config(), ID).unwrap();
            assert_eq!(
                log.view(1).unwrap().unwrap().last,
                3,
                "ops {ops}, {crash:?}"
            );
            drop(log.close().unwrap());
        }
    }
}

/// Power is cut while the log fills a slot ahead, at whichever of its pieces or its flush the cut
/// meets, under each way a crash keeps what was not flushed: the log opens with every entry it
/// acknowledged and nothing damaged, its partly zeroed slot read as no frame, and takes writes
/// again, the slot written by its frames. A fill that ended before the cut was injected is tried
/// again with a log of its own: each cut meets a fill.
#[test]
fn a_power_cut_anywhere_in_a_fill_ahead_leaves_what_was_acknowledged() {
    for ops in 0..8u64 {
        for (kind, crash) in [Crash::Random, Crash::KeepAll, Crash::LoseAll]
            .into_iter()
            .enumerate()
        {
            let met = (0..16u64).any(|attempt| {
                cut_in_a_fill(ops, crash, 100 + ops * 64 + kind as u64 * 16 + attempt)
            });
            assert!(met, "ops {ops}, {crash:?}: no cut met a fill");
        }
    }
}

/// One log's power cut `ops` operations into the fill begun once a frame opens its first slot
/// filled ahead, then its crash, reopening and writes: whether the cut met the fill.
fn cut_in_a_fill(ops: u64, crash: Crash, seed: u64) -> bool {
    let what = format!("ops {ops}, {crash:?}, seed {seed}");
    let log = Log::create(sim(seed), measured_long(), ID).unwrap();
    until(&log, "the first fill", |s| s.fills == 1);
    let next = open_the_filled_slot(&log, 1);
    log.with_file(move |f| f.inject(Fault::PowerCut { ops }).unwrap())
        .unwrap();
    let ended = until(&log, "the fill the cut meets", |s| {
        s.fill_failures == 1 || s.fills == 2
    });
    log.with_file(move |f| f.crash(crash).unwrap()).unwrap();
    let file = log.close().unwrap();
    file.clear_faults().unwrap();
    let (log, recovery) = Log::open(file, measured_long(), ID).unwrap();
    assert!(recovery.damaged.is_empty(), "{what}");
    small_entries_read_back(&log, next, &what);
    for index in next..next + 3 {
        log.write(1, entry(index, ENTRY)).unwrap();
    }
    let file = log.close().unwrap();
    let (log, recovery) = Log::open(file, measured_long(), ID).unwrap();
    assert!(recovery.damaged.is_empty(), "{what}");
    assert_eq!(log.view(1).unwrap().unwrap().last, next + 2, "{what}");
    drop(log.close().unwrap());
    ended.fill_failures == 1
}

/// A fill whose write fails fences nothing: the frames acknowledged stand, the slot is not filled
/// ahead again, and the frames that open it write it as any unwritten space, read back after a
/// reopen. A fill that ended before the fault was injected is tried again with a log of its own,
/// its fault cleared before a frame meets it.
#[test]
fn a_failed_fill_fences_nothing_and_its_slot_is_written_by_its_frames() {
    for attempt in 0..16u64 {
        let log = Log::create(sim(3 + attempt), measured_long(), ID).unwrap();
        until(&log, "the first fill", |s| s.fills == 1);
        let next = open_the_filled_slot(&log, 1);
        // Nothing else is queued: the next write the device makes is the fill's.
        log.with_file(|f| f.inject(Fault::WriteError).unwrap())
            .unwrap();
        let ended = until(&log, "the fill the fault meets", |s| {
            s.fill_failures == 1 || s.fills == 2
        });
        if ended.fill_failures == 0 {
            log.with_file(|f| f.clear_faults().unwrap()).unwrap();
            drop(log.close().unwrap());
            continue;
        }
        assert_eq!(ended.fills, 1, "the slot is not filled");
        for index in next..next + 3 {
            log.write(1, entry(index, ENTRY)).unwrap();
        }
        assert_eq!(
            log.stats(None).unwrap().fill_failures,
            1,
            "not filled again"
        );
        let file = log.close().unwrap();
        let (log, recovery) = Log::open(file, measured_long(), ID).unwrap();
        assert!(recovery.damaged.is_empty());
        assert_eq!(log.view(1).unwrap().unwrap().last, next + 2);
        drop(log.close().unwrap());
        return;
    }
    panic!("no fault met a fill");
}
