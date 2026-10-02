//! A group's handle (`GroupLog`): it answers every read as the log does, from the group's state
//! as its answered writes left it; it asks the log only for what it does not hold; it holds its
//! group against everyone else's writes; and each of its writes wakes its own waker alone.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity,
    clippy::unwrap_in_result
)]

use hyper_block::buf::Alignment;
use hyper_block::sim::{Fault, SimFile};
use hyper_log::{
    Config, Entries, Entry, Fetched, GroupLog, HardState, Log, LogError, Proposal, Start, Update,
    Waits,
};
use proptest::prelude::*;

const ID: u128 = 0x0067_726f_7570;
const BLOCK: usize = 4096;

fn config(group_cache: u64) -> Config {
    Config {
        segment_bytes: 16 * BLOCK as u64,
        max_segments: 16,
        max_groups: 64,
        group_entries: 1 << 10,
        group_bytes: 1 << 20,
        group_cache,
        queue_submissions: 64,
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

/// `count` entries from `first` of `term`, each of `len` bytes tagged with its index.
fn entries(first: u64, count: u64, term: u64, len: usize) -> Entries {
    Entries {
        first,
        entries: (first..first + count)
            .map(|i| Entry {
                term,
                bytes: (0..len).map(|b| (i as usize + b) as u8).collect(),
            })
            .collect(),
    }
}

/// The kind of a generated write, drawn as a number by weight (`prop_oneof!` would box its
/// arms in `Arc`, which the lint wall refuses in tests too).
#[derive(Debug, Clone, Copy)]
struct Step {
    kind: u8,
    at: u64,
    count: u64,
    len: usize,
    hard: bool,
}

fn step() -> impl Strategy<Value = Step> {
    (0u8..100, 0u64..64, 0u64..5, 0usize..700, any::<bool>()).prop_map(
        |(kind, at, count, len, hard)| Step {
            kind,
            at,
            count,
            len,
            hard,
        },
    )
}

/// The update a step makes of the group as its handle says it stands: mostly appends, then
/// conflicting suffixes, compactions within and past the log, removals, proposals, invalid
/// gaps, and empty entries that only say where the log ends.
fn update(s: Step, start: Start, last: u64, term: &mut u64) -> Update {
    let held = last - start.index;
    let pick = |n: u64| start.index + 1 + s.at % n.max(1);
    let hard = s.hard.then_some(HardState {
        term: *term,
        vote: s.at % 3,
        commit: last.min(s.at),
    });
    let mut u = Update {
        hard_state: hard,
        ..Update::default()
    };
    match s.kind {
        0..=49 => u.entries = Some(entries(last + 1, s.count, *term, s.len)),
        50..=64 if held > 0 => {
            *term += 1;
            u.entries = Some(entries(pick(held), s.count + 1, *term, s.len));
        }
        65..=79 if held > 0 => {
            u.start = Some(Start {
                index: pick(held),
                term: *term,
            });
        }
        80..=84 => {
            u.start = Some(Start {
                index: last + 1 + s.at % 3,
                term: *term,
            });
        }
        85..=87 => {
            return Update {
                remove: true,
                ..Update::default()
            };
        }
        88..=91 => {
            u.proposals.push(Proposal {
                index: last + 2 + s.at % 3,
                term: *term,
                bytes: vec![7; s.len.min(64)],
            });
        }
        92..=95 => u.entries = Some(entries(last + 2, s.count + 1, *term, s.len)),
        _ => u.entries = Some(entries(last + 1, 0, *term, 0)),
    }
    u
}

/// Every read the handle answers equals the log's own answer to it: the view, the bounds, the
/// term of every index around the group's, and fetches of every range, by every byte bound.
fn same_reads(log: &Log<SimFile>, h: &GroupLog<SimFile>, g: u128) -> Result<(), TestCaseError> {
    let view = log.view(g).unwrap();
    prop_assert_eq!(&h.view().unwrap(), &view);
    let (start, last) = view
        .as_ref()
        .map_or((Start::default(), 0), |v| (v.start, v.last));
    prop_assert_eq!(h.bounds().unwrap(), (start, last));
    let low = start.index.saturating_sub(1);
    for i in low..=last + 1 {
        let ours = h.term(i).map_err(|e| e.to_string());
        let theirs = log.term(g, i).map_err(|e| e.to_string());
        prop_assert_eq!(ours, theirs, "term of {}", i);
    }
    for lo in low..=last + 1 {
        for hi in [lo, lo + 1, last + 1, last + 2] {
            for max in [0, 600, u64::MAX] {
                let ours = h.fetch(lo, hi, max, Fetched::new());
                let theirs = log.fetch(g, lo, hi, max, Fetched::new());
                prop_assert_eq!(
                    ours.map_err(|e| e.to_string()),
                    theirs.map_err(|e| e.to_string()),
                    "fetch [{}, {}) by {}",
                    lo,
                    hi,
                    max
                );
            }
        }
    }
    Ok(())
}

fn handle_reads_as_the_log_does(
    steps: &[Step],
    cache: u64,
    seed: u64,
) -> Result<(), TestCaseError> {
    let log = Log::create(sim(seed), config(cache), ID).unwrap();
    let g = 5u128;
    let mut h = log.group(g).unwrap();
    let mut term = 1u64;
    for &s in steps {
        let (start, last) = h.bounds().unwrap();
        let u = update(s, start, last, &mut term);
        match h.write(u) {
            Ok(())
            | Err(
                LogError::Invalid { .. }
                | LogError::Backlog(_)
                | LogError::Full
                | LogError::TooLarge(_),
            ) => {}
            Err(e) => return Err(TestCaseError::fail(format!("{e}"))),
        }
        same_reads(&log, &h, g)?;
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Whatever its group's writes, a handle answers every read as the log does, with a cache
    /// that holds nothing, a few entries, or all of them.
    #[test]
    fn a_handle_reads_as_the_log_does(
        steps in prop::collection::vec(step(), 1..40),
        cache in prop::sample::select(vec![0u64, 1_500, 1 << 20]),
        seed in any::<u64>(),
    ) {
        handle_reads_as_the_log_does(&steps, cache, seed)?;
    }
}

/// The log holds a group for its handle: its other writers are refused, a second handle is
/// refused, and once the handle is dropped the group is anyone's again.
#[test]
fn a_handle_holds_its_group() {
    let log = Log::create(sim(1), config(1 << 20), ID).unwrap();
    let h = log.group(1).unwrap();
    let u = || Update {
        entries: Some(entries(1, 1, 1, 8)),
        ..Update::default()
    };
    assert!(matches!(log.write(1, u()), Err(LogError::Claimed(1))));
    assert!(matches!(log.submit(1, u()), Err(LogError::Claimed(1))));
    assert!(matches!(log.group(1), Err(LogError::Claimed(1))));
    // Another group is not held.
    log.write(2, u()).unwrap();
    drop(h);
    log.write(1, u()).unwrap();
    let h = log.group(1).unwrap();
    assert_eq!(h.bounds().unwrap(), (Start::default(), 1));
}

/// A handle answers reads of what it holds itself, and asks the log only for entries older
/// than its cache; it takes over the entries the log held in memory when it was made.
#[test]
fn a_handle_asks_the_log_only_for_what_it_lacks() {
    let log = Log::create(sim(2), config(3 * 100), ID).unwrap();
    // Written before the handle: the log keeps the last three in memory, which the handle takes.
    log.write(
        1,
        Update {
            entries: Some(entries(1, 5, 1, 100)),
            ..Update::default()
        },
    )
    .unwrap();
    let mut h = log.group(1).unwrap();
    for i in 0..5 {
        h.term(i).unwrap();
    }
    h.bounds().unwrap();
    h.view().unwrap();
    let recent = h.fetch(3, 6, u64::MAX, Fetched::new()).unwrap();
    assert_eq!(recent.len(), 3);
    assert_eq!(h.asked(), 0, "the handle asked for what it held");
    // Older than the cache: from the file, through the log.
    let old = h.fetch(1, 6, u64::MAX, Fetched::new()).unwrap();
    assert_eq!(old.len(), 5);
    assert_eq!(h.asked(), 1);
    // Its own writes' entries come back to it: the newest three are its own.
    h.write(Update {
        entries: Some(entries(6, 3, 1, 100)),
        ..Update::default()
    })
    .unwrap();
    let mine = h.fetch(6, 9, u64::MAX, Fetched::new()).unwrap();
    assert_eq!(
        mine.get(2).unwrap().1,
        entries(8, 1, 1, 100).entries[0].bytes
    );
    assert_eq!(h.asked(), 1);
}

/// A write that fails leaves the handle reading from the log: it cannot know what the failed
/// frame left.
#[test]
fn a_failed_write_leaves_reads_to_the_log() {
    let log = Log::create(sim(3), config(1 << 20), ID).unwrap();
    let mut h = log.group(1).unwrap();
    h.write(Update {
        entries: Some(entries(1, 2, 1, 16)),
        ..Update::default()
    })
    .unwrap();
    log.with_file(|f| f.inject(Fault::WriteError).unwrap())
        .unwrap();
    let failed = h.write(Update {
        entries: Some(entries(3, 1, 1, 16)),
        ..Update::default()
    });
    assert!(matches!(failed, Err(LogError::Fenced)), "{failed:?}");
    let asked = h.asked();
    assert_eq!(h.view().unwrap(), log.view(1).unwrap());
    assert_eq!(h.asked(), asked + 1);
}

/// Each handle's write wakes its own waker alone, once, after its answer: 48 groups, each
/// writing through its handle with a counting waker, 12 rounds each.
#[test]
fn a_handles_write_wakes_only_its_writer() {
    const GROUPS: usize = 48;
    const ROUNDS: usize = 12;
    let log = Log::create(sim(4), config(1 << 20), ID).unwrap();
    let (ready, woken) = std::sync::mpsc::sync_channel(GROUPS);
    let wakers: Vec<_> = (0..GROUPS)
        .map(|g| hyper_measure::wake::waker(g, ready.clone()))
        .collect();
    drop(ready);
    let mut handles: Vec<GroupLog<SimFile>> =
        (0..GROUPS).map(|g| log.group(g as u128).unwrap()).collect();
    let submit = |h: &mut GroupLog<SimFile>, g: usize, round: usize| {
        let u = Update {
            entries: Some(entries(round as u64 + 1, 1, 1, 16)),
            ..Update::default()
        };
        h.submit_waking(u, wakers[g].0.clone()).unwrap();
    };
    for (g, h) in handles.iter_mut().enumerate() {
        submit(h, g, 0);
    }
    let mut answered = vec![0usize; GROUPS];
    let mut total = 0usize;
    while total < GROUPS * ROUNDS {
        let g = woken.recv().unwrap();
        handles[g].poll().expect("woken before its answer").unwrap();
        answered[g] += 1;
        total += 1;
        // A waker woken for another's answer would have been woken before its own came.
        assert_eq!(wakers[g].1.wakes() as usize, answered[g], "waker {g}");
        if answered[g] < ROUNDS {
            submit(&mut handles[g], g, answered[g]);
        }
    }
    for (g, h) in handles.iter().enumerate() {
        assert_eq!(h.bounds().unwrap().1, ROUNDS as u64, "group {g}");
    }
    drop(handles);
    drop(log);
    for (g, (_, slot)) in wakers.iter().enumerate() {
        assert_eq!(slot.wakes() as usize, ROUNDS, "waker {g}");
    }
    assert!(woken.try_recv().is_err(), "a wake with no answer");
}
