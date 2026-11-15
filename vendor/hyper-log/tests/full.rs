//! A log driven to `Full` by generated histories takes writes again once its groups compact
//! (mantle docs/design/raft-log.md §5: "once its groups compact, a log that answered `Full` takes
//! entries of any size up to a frame's room").
//!
//! Each history is its seed's: groups write updates of entries of any size, from none of their
//! bytes to a frame's room, often with a hard state, compact part of their logs and write hard
//! states alone, on files of the least three segments and a few more, segments of a few blocks and
//! of more. Whenever the log answers `Full`, every group compacts through its last entry, which
//! leaves the log nothing live but each group's start and hard state, and then what was refused is
//! written again. The compactions come as an owner's do, one of two ways:
//!
//! - each on its own, waited for before the next, and one refused asked again while another's was
//!   taken, the fact that may have made room; then the write refused, on its own;
//! - all in one frame with the write refused beside them, as a node's turn submits its ranges'
//!   compactions and their writes together (mantle's `Node::compact_if_stalled`); then the write
//!   on its own if that frame refused it.
//!
//! A compaction must be taken; and once all are, the write. A log that refuses either has wedged:
//! its groups hold nothing more to free, and it never takes a write again. mantle's node
//! simulation found such a log at three segments, a leader's range stalled for good (mantle
//! docs/measurements/2026-11-07-full-log-restart.md, "Still open: a file of three segments").
//!
//! The writer is told when to write (`Waits::Told`), so what each frame holds is the history's.
//! `HYPER_LOG_FULL_SEEDS` sets how many histories each shape runs, and `HYPER_LOG_FULL_SEED`
//! where they begin.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]

use hyper_block::buf::Alignment;
use hyper_block::sim::SimFile;
use hyper_log::{Config, Entries, Entry, HardState, Log, LogError, Start, Update, Waits};

const ID: u128 = 0x6d61_6e74_6c65_2d6c_6f67;
const BLOCK: usize = 4096;
/// The histories each shape runs when `HYPER_LOG_FULL_SEEDS` is not set.
const SEEDS: u64 = 8;
/// The steps of one history: enough to fill the largest file of the shapes below many times over
/// with its smallest frames, one block each (10 segments of 16 blocks).
const STEPS: u64 = 1_200;

/// A generator of the history's draws: SplitMix64 (Steele, Lea and Flood, "Fast Splittable
/// Pseudorandom Number Generators", OOPSLA 2014), its constants theirs.
struct Draws(u64);

impl Draws {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A draw in `0..n`, `n` at least one.
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// How the groups compact when the log answers `Full`.
#[derive(Clone, Copy, Debug)]
enum Compacting {
    /// Each on its own.
    Alone,
    /// All in one frame, beside the write refused.
    Together,
}

/// What the history knows of a group: where its log starts and its last entry.
#[derive(Clone, Copy, Default)]
struct Group {
    start: u64,
    last: u64,
}

fn sim(seed: u64) -> SimFile {
    SimFile::new(
        Alignment::new(BLOCK).unwrap(),
        Alignment::new(512).unwrap(),
        seed,
    )
    .unwrap()
}

/// Groups bound by nothing but the file, so the file is what refuses: `Full`, never `Backlog`.
fn config(blocks: u64, segments: u32, groups: usize) -> Config {
    Config {
        segment_bytes: blocks * BLOCK as u64,
        max_segments: segments,
        max_groups: groups,
        group_entries: 1 << 24,
        group_bytes: 1 << 40,
        group_cache: 1 << 12,
        queue_submissions: 256,
        waits: Waits::Told,
    }
}

/// A hard state that commits through `commit`.
fn hard(commit: u64) -> HardState {
    HardState {
        term: 1,
        vote: 1,
        commit,
        held: None,
    }
}

/// A compaction through `index`.
fn compaction(index: u64) -> Update {
    Update {
        start: Some(Start { index, term: 1 }),
        ..Update::default()
    }
}

/// Each of `updates` submitted, the log told to write them, and each one's answer.
fn written(log: &Log<SimFile>, updates: Vec<(u128, Update)>) -> Vec<Result<(), LogError>> {
    let pending: Vec<_> = updates
        .into_iter()
        .map(|(group, update)| log.submit(group, update))
        .collect();
    log.cut().unwrap().wait().unwrap();
    pending
        .into_iter()
        .map(|pending| pending.and_then(|pending| pending.wait()))
        .collect()
}

/// An entry's bytes: none, a few, a few hundred, a few KiB, or up to `most`, the most each entry of
/// the update may take for the update to fit a frame.
fn entry_len(draws: &mut Draws, most: usize) -> usize {
    let most = most as u64;
    let len = match draws.below(10) {
        0..=3 => draws.below(65),
        4..=6 => 65 + draws.below(960),
        7..=8 => 1_025 + draws.below(most.saturating_sub(1_024).max(1) / 4),
        _ => draws.below(most + 1),
    };
    len.min(most) as usize
}

/// The next step of a history for `group`: its update, and what the group holds once it is taken.
fn step(draws: &mut Draws, g: Group, room: usize) -> (Update, Group) {
    match draws.below(9) {
        0..=5 => {
            let count = 1 + draws.below(3) as usize;
            let most = room / count;
            let entries: Vec<Entry> = (0..count)
                .map(|_| Entry {
                    term: 1,
                    bytes: vec![b'x'; entry_len(draws, most)],
                })
                .collect();
            let last = g.last + count as u64;
            let commit = (draws.below(2) == 0).then(|| hard(g.last));
            (
                Update {
                    entries: Some(Entries {
                        first: g.last + 1,
                        entries,
                    }),
                    hard_state: commit,
                    ..Update::default()
                },
                Group { last, ..g },
            )
        }
        6..=7 if g.last > g.start => {
            let index = g.start + 1 + draws.below(g.last - g.start);
            (compaction(index), Group { start: index, ..g })
        }
        _ => (
            Update {
                hard_state: Some(hard(g.start + draws.below(g.last - g.start + 1))),
                ..Update::default()
            },
            g,
        ),
    }
}

/// One history of `seed` on a file of `segments` segments of `blocks` blocks, its `groups` groups
/// writing and compacting as `compacting` says: how often the log answered `Full`, or how it
/// wedged.
fn history(
    seed: u64,
    (blocks, segments, groups): (u64, u32, u128),
    compacting: Compacting,
) -> Result<u64, String> {
    let log = Log::create(sim(seed), config(blocks, segments, groups as usize + 1), ID).unwrap();
    let room = log.entry_room().unwrap();
    let mut draws =
        Draws(seed ^ (u64::from(segments) << 32) ^ (blocks << 40) ^ ((groups as u64) << 48));
    let mut known = vec![Group::default(); groups as usize + 1];
    let mut fulls = 0u64;
    let shape = format!(
        "seed {seed}, {segments} segments of {blocks} blocks, {groups} groups, compacting \
         {compacting:?}"
    );
    for at in 0..STEPS {
        let group = 1 + u128::from(draws.below(groups as u64));
        let (update, after) = step(&mut draws, known[group as usize], room);
        let answer = written(&log, vec![(group, update.clone())]).remove(0);
        match answer {
            Ok(()) => known[group as usize] = after,
            // An update past a frame's room: its entries' records take more than their bytes.
            Err(LogError::TooLarge(_)) => {}
            Err(LogError::Full) => {
                fulls += 1;
                let wedged = |what: String| format!("{shape}, step {at}, Full {fulls}: {what}");
                // The group's own compaction past what was refused stands for it.
                let refused = (update.start.is_none()).then_some((group, update));
                let refused = match compacting {
                    Compacting::Alone => alone(&log, &mut known).map_err(&wedged)?.and(refused),
                    Compacting::Together => together(&log, &mut known, refused).map_err(&wedged)?,
                };
                if let Some((group, update)) = refused {
                    match written(&log, vec![(group, update)]).remove(0) {
                        Ok(()) | Err(LogError::TooLarge(_)) => {}
                        Err(e) => {
                            return Err(wedged(format!(
                                "refused again after every group compacted: {e}"
                            )));
                        }
                    }
                }
                // Each group starts after its last entry now, what the write refused written past.
                if let Some(g) = known.get_mut(group as usize) {
                    g.last = g.last.max(after.last);
                }
            }
            Err(e) => panic!("{shape}, step {at}: {e}"),
        }
    }
    log.close().unwrap();
    Ok(fulls)
}

/// Every group compacts through its last entry, each on its own; one refused is asked again while
/// another's was taken. `Some(())` once all are taken, as `Option::and` reads it.
fn alone(log: &Log<SimFile>, known: &mut [Group]) -> Result<Option<()>, String> {
    loop {
        let mut compacted = false;
        let mut refused = None;
        for (group, held) in known.iter_mut().enumerate().skip(1) {
            if held.last == held.start {
                continue;
            }
            match written(log, vec![(group as u128, compaction(held.last))]).remove(0) {
                Ok(()) => {
                    held.start = held.last;
                    compacted = true;
                }
                Err(LogError::Full) => refused = Some((group, held.last)),
                Err(e) => panic!("a compaction: {e}"),
            }
        }
        match refused {
            None => return Ok(Some(())),
            Some((group, through)) if !compacted => {
                return Err(format!(
                    "group {group}'s compaction through {through} refused, and no other group's \
                     taken"
                ));
            }
            Some(_) => {}
        }
    }
}

/// Every group compacts through its last entry in one frame, `refused` beside them: what is left
/// to write on its own, `refused` if the frame refused it.
fn together(
    log: &Log<SimFile>,
    known: &mut [Group],
    refused: Option<(u128, Update)>,
) -> Result<Option<(u128, Update)>, String> {
    let compacting: Vec<usize> = (1..known.len())
        .filter(|&group| known[group].last > known[group].start)
        .collect();
    let mut updates: Vec<(u128, Update)> = compacting
        .iter()
        .map(|&group| (group as u128, compaction(known[group].last)))
        .collect();
    updates.extend(refused.clone());
    let answers = written(log, updates);
    for (&group, answer) in compacting.iter().zip(&answers) {
        match answer {
            Ok(()) => known[group].start = known[group].last,
            Err(e) => {
                return Err(format!(
                    "group {group}'s compaction through {}, beside the others' and the write \
                     refused, refused: {e}",
                    known[group].last
                ));
            }
        }
    }
    Ok(match answers.get(compacting.len()) {
        Some(Ok(())) | None => None,
        Some(Err(LogError::Full)) => refused,
        Some(Err(e)) => panic!("the write refused, beside the compactions: {e}"),
    })
}

/// A count from the environment, for longer soaks.
#[allow(
    clippy::disallowed_methods,
    reason = "a soak sets the seed count from the environment; the default is the gate's"
)]
fn count(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// The histories of every shape: hyper-log's least file of three segments and files of four, six
/// and ten; segments of four blocks, the least, of eight and of sixteen; one group, two and three;
/// each with its groups compacting on their own and together.
#[test]
fn a_log_driven_to_full_by_any_history_takes_writes_again_once_its_groups_compact() {
    let seeds = count("HYPER_LOG_FULL_SEEDS", SEEDS);
    let first = count("HYPER_LOG_FULL_SEED", 1);
    let mut wedged = Vec::new();
    let mut ran = 0u64;
    let mut filled = 0u64;
    for segments in [3u32, 4, 6, 10] {
        for blocks in [4u64, 8, 16] {
            for groups in 1..=3u128 {
                for compacting in [Compacting::Alone, Compacting::Together] {
                    for seed in first..first + seeds {
                        ran += 1;
                        match history(seed, (blocks, segments, groups), compacting) {
                            Ok(fulls) => filled += u64::from(fulls > 0),
                            Err(wedge) => wedged.push(wedge),
                        }
                    }
                }
            }
        }
    }
    eprintln!(
        "{ran} histories, {filled} of them filled their log and went on, {} wedged",
        wedged.len()
    );
    assert!(
        wedged.is_empty(),
        "{} of {ran} histories wedged:\n{}",
        wedged.len(),
        wedged.join("\n")
    );
    // Histories filled their logs, or the test proved nothing.
    assert!(filled > 0);
}
