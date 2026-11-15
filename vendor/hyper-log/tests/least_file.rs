//! The least file, three segments, takes writes again once its group compacts, however its
//! oldest segment was left: the sweep weighs a tail against the slot it frees. Found by mantle
//! (2026-11-07): a leader whose log was compacted to one entry (start 286, last 287) was refused
//! `Full` for good, its file's tail one frame of one live record, which the sweep never moved.
//! These are hyper-raft branch `log-retention-counts-frames`' (`277dd17`), without its retention's
//! derivation.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use hyper_block::buf::Alignment;
use hyper_block::sim::SimFile;
use hyper_log::{Config, Entries, Entry, Facts, HardState, Log, LogError, Start, Update, Waits};

const ID: u128 = 0x7265_7465_6e74;
const BLOCK: usize = 4096;
/// An entry's payload: a few hundred bytes, mantle's small writes.
const SMALL: usize = 300;

fn sim(seed: u64) -> SimFile {
    SimFile::new(
        Alignment::new(BLOCK).unwrap(),
        Alignment::new(512).unwrap(),
        seed,
    )
    .unwrap()
}

/// A node's facts for `groups` groups, each retaining a mebibyte of payload, on a disk budget of
/// the payload the groups retain at once.
fn facts(groups: usize) -> Facts {
    let mut facts = Facts {
        align: Alignment::new(BLOCK).unwrap(),
        sealed: false,
        largest_entry: 64 << 10,
        disk_bytes: 1 << 34,
        max_groups: groups,
        cadence_entries: 1 << 20,
        cadence_bytes: 1 << 19,
        uncommitted_entries: 0,
        uncommitted_bytes: 0,
        cache_bytes: 1 << 26,
    };
    let unbounded = Config::derive(&facts).unwrap();
    let retained = unbounded.group_bytes * groups as u64;
    let segments = retained.div_ceil(unbounded.segment_bytes).max(3);
    facts.disk_bytes = (segments + 1) * unbounded.segment_bytes;
    facts
}

/// One entry of `size` bytes at `index`, as its own update.
fn one(index: u64, size: usize) -> Update {
    Update {
        entries: Some(Entries {
            first: index,
            entries: vec![Entry {
                term: 1,
                bytes: vec![index as u8; size],
            }],
        }),
        ..Update::default()
    }
}

/// Do: the least file, three segments (`Config::derive`'s floor), one group whose own bounds the
/// file never reaches, written one-entry updates with their hard state, as a replica writes them,
/// until the file refuses one `Full`; the group then compacts to its last entry, as an owner does
/// when its log is refused for room, and writes again; ten times, for entries of a few sizes.
/// Expect: after each compaction the next write is taken. Found by mantle (2026-11-07): a leader
/// whose log was compacted to one entry (start 286, last 287) was refused `Full` for good, its
/// file's tail one frame of one live record, which the sweep never moved.
#[test]
fn a_least_file_takes_writes_again_once_its_group_compacts() {
    for (seed, size) in [1usize, SMALL, 3_000].into_iter().enumerate() {
        let facts = facts(1);
        let mut config = Config::derive(&facts).unwrap();
        config.max_segments = 3;
        config.group_entries = u64::MAX;
        config.group_bytes = u64::MAX;
        config.waits = Waits::Never;
        let log = Log::create(sim(seed as u64 + 40), config, ID).unwrap();
        let mut next = 1u64;
        let write = |log: &Log<SimFile>, index: u64| {
            let mut update = one(index, size);
            update.hard_state = Some(HardState {
                term: 1,
                commit: index,
                ..HardState::default()
            });
            log.write(1, update)
        };
        for round in 0..10 {
            let taken = next;
            let refused = loop {
                match write(&log, next) {
                    Ok(()) => next += 1,
                    Err(error) => break error,
                }
            };
            assert!(
                matches!(refused, LogError::Full),
                "{size}-byte entries, round {round}: refused {refused:?}"
            );
            assert!(
                next > taken,
                "{size}-byte entries, round {round}: nothing taken since the last compaction"
            );
            let compact = Update {
                start: Some(Start {
                    index: next - 2,
                    term: 1,
                }),
                ..Update::default()
            };
            log.write(1, compact).unwrap();
            write(&log, next).unwrap_or_else(|refused| {
                panic!("{size}-byte entries, round {round}: compacted to its last entry, the next write refused {refused:?}")
            });
            next += 1;
        }
        log.close().unwrap();
    }
}

/// Do: the least file, three segments, one group whose own bounds the file never reaches. Its
/// first frame is its hard state alone, one block, and its next an entry too large for the rest of
/// that segment, which opens the next: the oldest segment is one block's frame holding one live
/// record. Then entries of a few hundred bytes, each a frame of its own; whenever the file refuses
/// one `Full`, the group compacts to its last entry, as an owner does when its log is refused for
/// room; twenty times. Expect: each compaction and the write after it are taken. Before: the
/// oldest segment, one live record in one block's frame, was never swept (its copy, one block, was
/// no smaller than its frames); once the newest segment filled with what the group then let go,
/// nothing freed the third, and every write, the compaction's too, was refused `Full` for good, as
/// mantle's leader was with its log compacted to one entry (start 286, last 287; mantle's
/// docs/measurements/2026-11-07-full-log-restart.md).
#[test]
fn a_least_file_whose_oldest_segment_holds_one_small_record_takes_writes_once_its_group_compacts() {
    let facts = facts(1);
    let mut config = Config::derive(&facts).unwrap();
    config.max_segments = 3;
    config.group_entries = u64::MAX;
    config.group_bytes = u64::MAX;
    config.waits = Waits::Never;
    let room = config.segment_bytes as usize - BLOCK;
    let log = Log::create(sim(60), config, ID).unwrap();
    let state = Update {
        hard_state: Some(HardState {
            term: 1,
            ..HardState::default()
        }),
        ..Update::default()
    };
    log.write(1, state).unwrap();
    // An entry whose frame fills a segment's room but for a block: past what the first segment
    // has left after its one-block frame.
    let large = room - 4 * 64 - BLOCK / 2;
    log.write(1, one(1, large)).unwrap();
    let mut next = 2u64;
    let mut compactions = 0;
    while compactions < 20 {
        match log.write(1, one(next, SMALL)) {
            Ok(()) => next += 1,
            Err(LogError::Full) => {
                let compact = Update {
                    start: Some(Start {
                        index: next - 2,
                        term: 1,
                    }),
                    ..Update::default()
                };
                log.write(1, compact).unwrap_or_else(|refused| {
                    panic!(
                        "compaction {compactions}, to entry {}: refused {refused:?}",
                        next - 2
                    )
                });
                log.write(1, one(next, SMALL)).unwrap_or_else(|refused| {
                    panic!("compaction {compactions}: the next write refused {refused:?}")
                });
                next += 1;
                compactions += 1;
            }
            Err(other) => panic!("entry {next}: refused {other:?}"),
        }
    }
    log.close().unwrap();
}
