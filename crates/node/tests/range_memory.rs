//! What a range's members hold in memory beside their log and their engine, hyper-raft's core and
//! hyper-durable's shell, counted by hyper-measure's counting allocator at the floor's settings
//! (`derive::cell`) over stores that hold nothing of their own, through the shapes that fill their
//! holds (`support/members.rs`); held to `derive::range_bytes`.
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

use hyper_measure::alloc;
use hyper_raft::progress::Progress;
use mantle_meta::engine::Model;
use mantle_node::derive;
use mantle_range::Elections;

use members::{Group, READS, ROOM, Synth, first_range, largest, least, range, shapes};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// Over a store that holds nothing of its own, the members' count is the core's and the shell's.
#[test]
fn a_ranges_members_hold_no_more_than_their_class() {
    let range = range(3, 0);
    let settings = range.settings;
    let max = usize::try_from(settings.max_entry_bytes).unwrap();
    let payloads: &'static [Vec<u8>] = Box::leak(
        vec![
            Vec::new(),
            least().encode().unwrap(),
            largest(max).encode().unwrap(),
        ]
        .into_boxed_slice(),
    );
    // What the harness holds is laid out before counting begins.
    let stores: Vec<Synth> = (0..3)
        .map(|_| Synth::new(payloads, derive::LOG_DEPTH, ROOM))
        .collect();
    let engines: Vec<Model> = (0..3).map(|_| first_range()).collect();
    let mut g = Group::new(Vec::with_capacity(3), settings.max_entry_bytes);
    alloc::begin();
    for (id, (store, engine)) in (1..=3u64).zip(stores.into_iter().zip(engines)) {
        g.members.push(
            mantle_range::open_shell(id, store, engine, &range, id, Elections::Ticks).unwrap(),
        );
    }
    g.members[0].campaign().unwrap();
    g.run();
    assert!(g.members[0].is_leader());
    let said = shapes(&mut g, settings.heartbeat_tick, alloc::read);
    let counts = alloc::end();
    let each = derive::range_bytes(&range, READS).unwrap();
    let most = 3 * each + u64::try_from(g.wire_most).unwrap();
    for line in &said {
        eprintln!("{line}");
    }
    eprintln!("peak {} of {most}: {each} a member", counts.peak);
    assert!(
        u64::try_from(counts.peak).unwrap() <= most,
        "the members reached {} bytes, past {most}",
        counts.peak
    );
}

/// A member's heap as it opens is `derive::MEMBER_BYTES` and, for each member its configuration
/// names, no more than the core counts a member at (`ProgressTracker::resident_bytes`), under
/// elections by suspicion, the node's.
#[test]
fn a_member_opens_on_its_measured_bytes() {
    let each = std::mem::size_of::<(u64, Progress)>()
        + std::mem::size_of::<(u64, bool)>()
        + 5 * std::mem::size_of::<u64>();
    let mut opened = Vec::new();
    for members in 1..=10u64 {
        let range = range(members, 0);
        let payloads: &'static [Vec<u8>] = Box::leak(vec![Vec::new()].into_boxed_slice());
        let (store, engine) = (Synth::new(payloads, derive::LOG_DEPTH, 16), first_range());
        alloc::begin();
        let shell =
            mantle_range::open_shell(1, store, engine, &range, 1, Elections::Suspicion).unwrap();
        let live = u64::try_from(alloc::end().live).unwrap();
        drop(shell);
        opened.push(live);
    }
    let slope = opened[1] - opened[0];
    for (members, live) in (1..=10u64).zip(&opened) {
        assert_eq!(*live, opened[0] + (members - 1) * slope, "{opened:?}");
    }
    assert!(slope <= each as u64, "{slope} a member past {each}");
    assert_eq!(opened[0] - slope, derive::MEMBER_BYTES, "{opened:?}");
}
