//! A slot the file grows by is written whole with zeros before its first frame, under that frame's
//! flush (`docs/durable.md` §6.3), so every later frame in it is an overwrite. Zeros past the last
//! frame read as no frame, as the file's end does; a power cut anywhere in a growing frame's writes
//! leaves a log that opens with what was acknowledged and nothing else.
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
use hyper_log::{Config, Entries, Entry, Log, LogError, Update, Waits};

const ID: u128 = 0x7072_6561_6c6c_6f63;
const BLOCK: usize = 4096;
const SEGMENT: u64 = 16 * BLOCK as u64;
/// An entry a frame holds alone and two of which no segment does: every second frame opens a slot.
const ENTRY: usize = 36 * 1024;

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

/// The file spans its first slot from its creation, does not grow while frames fill a slot, and
/// grows by exactly a segment when a frame opens the next: frames inside a slot are overwrites.
#[test]
fn frames_inside_a_slot_overwrite_and_a_new_slot_grows_the_file_by_a_segment() {
    let log = Log::create(sim(1), config(), ID).unwrap();
    let len = |log: &Log<SimFile>| log.with_file(|f| f.len()).unwrap().unwrap();
    let created = len(&log);
    assert_eq!(
        created,
        2 * SEGMENT,
        "the persist area and the first slot, whole"
    );
    // Small entries fill the first slot without growing the file.
    let mut index = 1;
    while len(&log) == created {
        log.write(1, entry(index, 1024)).unwrap();
        index += 1;
        assert!(index < 1_000, "the file never grew");
        if len(&log) != created {
            break;
        }
    }
    assert_eq!(len(&log), created + SEGMENT, "a new slot, whole");
    // And reads back across both slots after a reopen.
    let file = log.close().unwrap();
    let (log, _) = Log::open(file, config(), ID).unwrap();
    for i in 1..index {
        assert_eq!(
            log.entries(1, i, i + 1, u64::MAX).unwrap()[0].bytes.len(),
            1024
        );
    }
    drop(log.close().unwrap());
}

/// Power is cut at each of a growing frame's operations (the zeros' write, the frame's, its
/// record's, the flush), under each way a crash keeps what was not flushed. The log opens with
/// every entry it acknowledged; the one it did not is gone or whole, as a write whose answer was
/// lost may be, never damaged; the slot's zeros read as no frame; and it takes writes again.
#[test]
fn a_power_cut_anywhere_in_a_growing_frame_leaves_what_was_acknowledged() {
    for ops in 0..4 {
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
