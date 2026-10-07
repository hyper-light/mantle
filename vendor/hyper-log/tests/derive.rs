//! `Config::derive`: a configuration derived from a node's and its device's facts runs, holds the
//! stated largest entry in a frame of its own, and facts that cannot give one are refused, typed.
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
use hyper_log::{Config, Entries, Entry, Facts, Log, LogError, Unfit, Update, Waits};
use proptest::prelude::*;

const ID: u128 = 0x6465_7269_7665;
const BLOCK: usize = 4096;

fn sim(seed: u64) -> SimFile {
    SimFile::new(
        Alignment::new(BLOCK).unwrap(),
        Alignment::new(512).unwrap(),
        seed,
    )
    .unwrap()
}

/// A node's facts: `groups` groups, entries up to `largest`, and a disk budget of `disk`.
fn facts(largest: usize, groups: usize, disk: u64) -> Facts {
    Facts {
        align: Alignment::new(BLOCK).unwrap(),
        sealed: false,
        largest_entry: largest,
        disk_bytes: disk,
        max_groups: groups,
        cadence_entries: 1024,
        cadence_bytes: 1 << 24,
        uncommitted_entries: 256,
        uncommitted_bytes: 1 << 25,
        cache_bytes: 1 << 26,
    }
}

/// Opens a log on the derived configuration and writes one entry of the largest size alone:
/// it is answered durable and read back whole.
fn holds_the_largest(config: Config, largest: usize, seed: u64) {
    let log = Log::create(sim(seed), config, ID).unwrap();
    assert!(
        log.entry_room().unwrap() >= largest,
        "a frame of the derived log holds the stated largest entry"
    );
    let bytes = vec![0xa5u8; largest];
    let update = Update {
        entries: Some(Entries {
            first: 1,
            entries: vec![Entry {
                term: 1,
                bytes: bytes.clone(),
            }],
        }),
        ..Update::default()
    };
    log.submit(1, update).unwrap().wait().unwrap();
    let read = log.entries(1, 1, 2, u64::MAX).unwrap();
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].bytes, bytes);
    log.close().unwrap();
}

#[test]
fn a_derived_log_holds_the_largest_entry_in_one_frame() {
    // focal's case: an 8 MiB entry, four thousand groups, a 1 GiB budget.
    let largest = 8 << 20;
    let config = Config::derive(&facts(largest, 4096, 1 << 30)).unwrap();
    assert_eq!(config.segment_bytes % BLOCK as u64, 0);
    assert_eq!(config.max_groups, 4096);
    // The queue admits each group's writes alike: as many for 4,096 groups as for one, 4,096 times.
    let one = Config::derive(&facts(largest, 1, 1 << 30)).unwrap();
    assert!(one.queue_submissions >= 1);
    assert_eq!(config.queue_submissions, 4096 * one.queue_submissions);
    assert_eq!(config.group_entries, 2 * 1024 + 256);
    assert_eq!(config.group_bytes, 2 * (1 << 24) + (1 << 25));
    assert_eq!(config.group_cache, (1 << 26) / 4096);
    assert_eq!(config.waits, Waits::Measured);
    assert!(
        u64::from(config.max_segments) * config.segment_bytes + config.segment_bytes <= 1 << 30
    );
    holds_the_largest(config, largest, 1);
}

#[test]
fn the_segment_is_the_least_that_holds_the_largest_entry() {
    let config = Config::derive(&facts(1 << 20, 64, 1 << 28)).unwrap();
    // One block less would not hold it: the segment is the least multiple of the block that does.
    let mut smaller = config;
    smaller.segment_bytes -= BLOCK as u64;
    let log = Log::create(sim(2), smaller, ID).unwrap();
    assert!(log.entry_room().unwrap() < 1 << 20);
    log.close().unwrap();
}

/// What `facts` are refused with; a derivation that gives a configuration fails the test.
fn unfit(f: Facts) -> Unfit {
    match Config::derive(&f) {
        Err(LogError::Unfit(unfit)) => unfit,
        other => panic!("derived {other:?}"),
    }
}

#[test]
fn a_budget_under_the_least_file_is_refused_naming_both() {
    // The persist area and three segments.
    let config = Config::derive(&facts(1 << 20, 64, 1 << 28)).unwrap();
    let least = 4 * config.segment_bytes;
    assert_eq!(
        unfit(facts(1 << 20, 64, least - 1)),
        Unfit::Disk {
            needed: least,
            budget: least - 1
        }
    );
    assert!(Config::derive(&facts(1 << 20, 64, least)).is_ok());
}

#[test]
fn an_entry_no_segment_holds_is_refused() {
    assert!(matches!(
        unfit(facts(1 << 30, 64, u64::MAX)),
        Unfit::Entry { largest, .. } if largest == 1 << 30
    ));
}

#[test]
fn retention_under_one_entry_is_refused() {
    let mut small = facts(1 << 20, 64, 1 << 28);
    small.cadence_bytes = 1 << 10;
    small.uncommitted_bytes = 0;
    assert_eq!(
        unfit(small),
        Unfit::Retention {
            retained: 2 << 10,
            largest: 1 << 20
        }
    );
}

#[test]
fn a_zero_fact_is_refused() {
    assert!(matches!(unfit(facts(0, 64, 1 << 28)), Unfit::Zero(_)));
    assert!(matches!(unfit(facts(1 << 10, 0, 1 << 28)), Unfit::Zero(_)));
    // A cache that gives each group nothing.
    let mut no_cache = facts(1 << 10, 64, 1 << 28);
    no_cache.cache_bytes = 63;
    assert!(matches!(unfit(no_cache), Unfit::Zero(_)));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Across a spread of facts, a derivation either refuses, typed, or gives a configuration a
    /// log is created with, whose frame holds the largest entry, and whose file fits the budget.
    #[test]
    fn a_derived_configuration_runs_and_fits_its_facts(
        largest in 1usize..(4 << 20),
        groups in 1usize..8192,
        disk_segments in 0u64..64,
        cadence_entries in 1u64..100_000,
        cadence_bytes in 1u64..(1 << 28),
        uncommitted_entries in 0u64..10_000,
        uncommitted_bytes in 0u64..(1 << 28),
        cache_bytes in 1u64..(1 << 30),
        sealed in any::<bool>(),
        seed in any::<u64>(),
    ) {
        let mut f = facts(largest, groups, 0);
        f.sealed = sealed;
        f.cadence_entries = cadence_entries;
        f.cadence_bytes = cadence_bytes;
        f.uncommitted_entries = uncommitted_entries;
        f.uncommitted_bytes = uncommitted_bytes;
        f.cache_bytes = cache_bytes;
        // The budget in segments of the largest entry's frame, so both sides of the least file
        // are drawn.
        f.disk_bytes = disk_segments * ((largest as u64 + 2 * BLOCK as u64) / BLOCK as u64 + 1) * BLOCK as u64;
        match Config::derive(&f) {
            Ok(config) => {
                prop_assert!(config.segment_bytes % BLOCK as u64 == 0);
                prop_assert!(config.max_segments >= 3);
                prop_assert!(
                    (u64::from(config.max_segments) + 1) * config.segment_bytes <= f.disk_bytes
                );
                prop_assert!(config.group_bytes >= largest as u64);
                // Whatever a group's writes are, the queue admits them for every group alike. One
                // group's segment is no larger than these groups' (its persist slots are smaller),
                // so the same facts derive for it.
                let mut alone = f;
                alone.max_groups = 1;
                let one = Config::derive(&alone).unwrap();
                prop_assert!(one.queue_submissions >= 1);
                prop_assert_eq!(config.queue_submissions, groups * one.queue_submissions);
                if !sealed {
                    let log = Log::create(sim(seed), config, ID).unwrap();
                    prop_assert!(log.entry_room().unwrap() >= largest);
                    log.close().unwrap();
                }
            }
            Err(LogError::Unfit(_)) => {}
            Err(other) => prop_assert!(false, "refused untyped: {other:?}"),
        }
    }
}
