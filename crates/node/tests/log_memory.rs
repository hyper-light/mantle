//! What the node's log holds in memory at the floor's configuration (`derive::log`), counted
//! across its threads by hyper-measure's counting allocator: each group filled to its retention
//! with entries of the fewest bytes, whose bookkeeping is the most a group keeps.
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
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_log::{Entries, Entry, HardState, Log, Update};
use hyper_measure::alloc;
use mantle_node::derive;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// The ranges a node hosts at most, a layer each, and the log's id.
const RANGES: usize = 5;
const ID: u128 = 0x6c6f67;

#[test]
fn the_log_at_the_floor_holds_its_groups_retention() {
    let (settings, _) = derive::cell(3).unwrap();
    let align = Alignment::new(4096).unwrap();
    let config = derive::log(align, &settings, RANGES).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let file = DeviceFile::open(
        &dir.path().join("log"),
        true,
        CachingRequest::Buffered,
        align,
    )
    .unwrap();
    // A Ready's worth of entries of the fewest bytes, the most a group's update carries: each a
    // range's least entry in its durable shell.
    let (per_update, _) = mantle_range::ready_held(&settings).unwrap();
    let entry = hyper_durable::ENTRY_OVERHEAD + mantle_meta::wire::least_entry_bytes().unwrap();
    eprintln!("config {config:?}; {per_update} entries an update of {entry} bytes each");
    let most = derive::log_bytes(&config).unwrap();
    alloc::begin_process();
    let log = Log::create(file, config, ID).unwrap();
    let mut groups: Vec<_> = (1..=RANGES as u128 + 1)
        .map(|g| log.group(g).unwrap())
        .collect();
    for group in &mut groups {
        let mut next = 1u64;
        for _ in 0..3 {
            // The term's empty first entry, its shell alone, and the rest the least a range
            // proposes: the most entries a Ready's bytes make.
            let entries = (0..per_update)
                .map(|n| Entry {
                    term: 1,
                    bytes: vec![
                        0;
                        if n == 0 {
                            hyper_durable::ENTRY_OVERHEAD
                        } else {
                            entry
                        }
                    ],
                })
                .collect();
            // In the parts a frame takes, as the replica writes a Ready past one; each Ready
            // commits the ones before it, so the group holds two committed and one not, its
            // retention, as a range that has not yet checkpointed does.
            let update = Update {
                entries: Some(Entries {
                    first: next,
                    entries,
                }),
                hard_state: Some(HardState {
                    term: 1,
                    vote: 1,
                    commit: next - 1,
                }),
                ..Update::default()
            };
            for part in group.parts(update).unwrap() {
                group.write(part).unwrap();
            }
            next += per_update;
        }
        let live = alloc::read_process().live;
        eprintln!("group {} filled: live {live}", group.group());
    }
    let counts = alloc::end_process();
    let peak = u64::try_from(counts.peak).unwrap();
    eprintln!("peak {peak} of {most}; live {}", counts.live);
    assert!(peak <= most, "the log reached {peak} bytes, past {most}");
    drop(groups);
    drop(log);
}
