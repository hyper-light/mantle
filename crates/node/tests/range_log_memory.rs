//! What a range's members and their logs hold in memory together at the floor's settings
//! (`derive::cell`, `derive::log`), counted across every thread by hyper-measure's counting
//! allocator through the shapes that fill a member's holds (`support/members.rs`), each member's
//! group on a log of its own over a file: held to `derive::range_bytes` and `derive::log_bytes`.
//! A test binary of its own, since a count across threads counts every test that runs beside it.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

#[allow(dead_code)]
#[path = "support/members.rs"]
mod members;

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_log::Log;
use hyper_measure::alloc;
use mantle_meta::engine::Model;
use mantle_node::derive;
use mantle_range::Elections;

use members::{Group, READS, Real, first_range, range, shapes};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// Over the node's logs, the members and their logs together hold no more than their classes.
#[test]
fn a_ranges_members_and_their_logs_hold_no_more_than_their_classes() {
    let range = range(3, 0);
    let settings = range.settings;
    let align = Alignment::new(4096).unwrap();
    let config = derive::log(align, &settings, 1).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let engines: Vec<Model> = (0..3).map(|_| first_range()).collect();
    let mut g = Group::new(Vec::with_capacity(3), settings.max_entry_bytes);
    let mut logs = Vec::with_capacity(3);
    alloc::begin_process();
    for (id, engine) in (1..=3u64).zip(engines) {
        let file = DeviceFile::open(
            &dir.path().join(format!("log{id}")),
            true,
            CachingRequest::Buffered,
            align,
        )
        .unwrap();
        let log = Log::create(file, config, u128::from(id)).unwrap();
        let store = Real::new(mantle_range::claim(&log, 1, &range).unwrap());
        g.members.push(
            mantle_range::open_shell(id, store, engine, &range, id, Elections::Ticks).unwrap(),
        );
        logs.push(log);
    }
    g.members[0].campaign().unwrap();
    g.run();
    assert!(g.members[0].is_leader());
    let said = shapes(&mut g, settings.heartbeat_tick, alloc::read_process);
    let counts = alloc::end_process();
    let each = derive::range_bytes(&range, READS).unwrap() + derive::log_bytes(&config).unwrap();
    let most = 3 * each + u64::try_from(g.wire_most).unwrap();
    for line in &said {
        eprintln!("{line}");
    }
    eprintln!(
        "peak {} of {most}: {each} a member and its log",
        counts.peak
    );
    assert!(
        u64::try_from(counts.peak).unwrap() <= most,
        "the members and their logs reached {} bytes, past {most}",
        counts.peak
    );
    drop(g);
    drop(logs);
}
