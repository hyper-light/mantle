//! The Raft log on real and simulated devices (docs/design/raft-log.md §8).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use mantle_disk::buf::Alignment;
use mantle_disk::sim::{Crash, Fault, SimFile};
use mantle_log::{Config, Entries, Entry, HardState, Log, LogError, Proposal, Start, Update, View};
use proptest::prelude::*;

const ID: u128 = 0x6d61_6e74_6c65_2d6c_6f67;
const BLOCK: usize = 4096;

fn config(segment_blocks: u64, max_segments: u32) -> Config {
    Config {
        segment_bytes: segment_blocks * BLOCK as u64,
        max_segments,
        max_groups: 64,
        group_entries: 1 << 16,
        group_bytes: 1 << 24,
        group_cache: 1 << 10,
        queue_submissions: 256,
        queue_bytes: 1 << 24,
    }
}

fn sim(seed: u64) -> Arc<SimFile> {
    Arc::new(
        SimFile::new(
            Alignment::new(BLOCK).unwrap(),
            Alignment::new(512).unwrap(),
            seed,
        )
        .unwrap(),
    )
}

fn entry(term: u64, tag: &str) -> Entry {
    Entry {
        term,
        bytes: Arc::from(tag.as_bytes()),
    }
}

/// Entries from `first`, one per term given, each tagged with its index.
fn entries(first: u64, terms: &[u64]) -> Entries {
    Entries {
        first,
        entries: (first..)
            .zip(terms)
            .map(|(i, &t)| entry(t, &format!("e{i}t{t}")))
            .collect(),
    }
}

/// What a group should hold, applied the way the log applies an update.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Model {
    start: Start,
    entries: Vec<(u64, Vec<u8>)>,
    hard: Option<HardState>,
    proposals: BTreeMap<u64, (u64, Vec<u8>)>,
}

impl Model {
    fn last(&self) -> u64 {
        self.start.index + self.entries.len() as u64
    }

    fn apply(&mut self, u: &Update) {
        if let Some(s) = u.start {
            let drop = (s.index - self.start.index).min(self.entries.len() as u64) as usize;
            self.entries.drain(..drop);
            self.start = s;
        }
        if let Some(e) = &u.entries {
            self.entries
                .truncate((e.first - self.start.index - 1) as usize);
            self.entries
                .extend(e.entries.iter().map(|x| (x.term, x.bytes.to_vec())));
        }
        if let Some(h) = u.hard_state {
            self.hard = Some(h);
        }
        let last = self.last();
        self.proposals.retain(|&i, _| i > last);
        for p in &u.proposals {
            self.proposals.insert(p.index, (p.term, p.bytes.to_vec()));
        }
    }
}

type Models = HashMap<u128, Model>;

fn apply(models: &mut Models, group: u128, u: &Update) {
    if u.remove {
        models.remove(&group);
    } else {
        models.entry(group).or_default().apply(u);
    }
}

/// Whether the log holds exactly `model` for `group`, `None` meaning nothing.
fn holds<F: mantle_disk::block::BlockFile>(
    log: &Log<F>,
    group: u128,
    model: Option<&Model>,
) -> bool {
    let view = log.view(group).unwrap();
    let (Some(view), Some(m)) = (view.as_ref(), model) else {
        return view.is_none() && model.is_none();
    };
    let proposals: BTreeMap<u64, (u64, Vec<u8>)> = view
        .proposals
        .iter()
        .map(|p| (p.index, (p.term, p.bytes.to_vec())))
        .collect();
    if (view.start, view.last, view.hard_state, &proposals)
        != (m.start, m.last(), m.hard, &m.proposals)
    {
        return false;
    }
    if m.last() == m.start.index {
        return true;
    }
    let got = log
        .entries(group, m.start.index + 1, m.last() + 1, u64::MAX)
        .unwrap();
    got.iter()
        .map(|e| (e.term, e.bytes.to_vec()))
        .collect::<Vec<_>>()
        == m.entries
}

/// The log holds exactly what the models say.
fn check<F: mantle_disk::block::BlockFile>(log: &Log<F>, models: &Models) {
    let mut groups = log.groups().unwrap();
    groups.sort_unstable();
    let mut expected: Vec<u128> = models.keys().copied().collect();
    expected.sort_unstable();
    assert_eq!(groups, expected);
    for (&group, m) in models {
        let view = log.view(group).unwrap().unwrap();
        let proposals: BTreeMap<u64, (u64, Vec<u8>)> = view
            .proposals
            .iter()
            .map(|p| (p.index, (p.term, p.bytes.to_vec())))
            .collect();
        assert_eq!(
            (view.start, view.last, view.hard_state, &proposals),
            (m.start, m.last(), m.hard, &m.proposals),
            "group {group}"
        );
        assert_eq!(log.term(group, m.start.index).unwrap(), m.start.term);
        if m.last() > m.start.index {
            let got = log
                .entries(group, m.start.index + 1, m.last() + 1, u64::MAX)
                .unwrap();
            let got: Vec<(u64, Vec<u8>)> = got.iter().map(|e| (e.term, e.bytes.to_vec())).collect();
            assert_eq!(got, m.entries, "group {group}");
        }
    }
}

fn hard(term: u64, commit: u64) -> HardState {
    HardState {
        term,
        vote: 1,
        commit,
    }
}

#[test]
fn updates_are_read_back_and_survive_reopening() {
    let file = sim(1);
    let mut models = Models::new();
    let log = Log::create(Arc::clone(&file), config(16, 8), ID).unwrap();
    let a = Update {
        entries: Some(entries(1, &[1, 1, 1, 2, 2])),
        hard_state: Some(hard(2, 3)),
        proposals: vec![Proposal {
            index: 7,
            term: 2,
            bytes: Arc::from(&b"fast"[..]),
        }],
        ..Update::default()
    };
    let b = Update {
        start: Some(Start { index: 10, term: 3 }),
        entries: Some(entries(11, &[3, 4])),
        hard_state: Some(hard(4, 11)),
        ..Update::default()
    };
    // Submitted together, both go in one frame.
    let pa = log.submit(1, a.clone()).unwrap();
    let pb = log.submit(2, b.clone()).unwrap();
    pa.wait().unwrap();
    pb.wait().unwrap();
    apply(&mut models, 1, &a);
    apply(&mut models, 2, &b);
    check(&log, &models);
    assert_eq!(log.term(2, 10).unwrap(), 3);
    assert!(matches!(
        log.entries(2, 9, 12, u64::MAX),
        Err(LogError::Compacted { first: 11, .. })
    ));
    // One entry at least, however small the byte budget.
    assert_eq!(log.entries(1, 1, 6, 0).unwrap().len(), 1);
    drop(log);
    let (log, recovery) = Log::open(Arc::clone(&file), config(16, 8), ID).unwrap();
    assert!(recovery.damaged.is_empty());
    check(&log, &models);
    assert_eq!(
        log.view(3).unwrap(),
        None::<View>,
        "a group never written holds nothing"
    );
}

#[test]
fn conflicts_replace_the_suffix_and_starts_drop_the_prefix() {
    let file = sim(2);
    let mut models = Models::new();
    let log = Log::create(Arc::clone(&file), config(16, 8), ID).unwrap();
    let run = |log: &Log<Arc<SimFile>>, models: &mut Models, u: Update| {
        log.write(7, u.clone()).unwrap();
        apply(models, 7, &u);
    };
    run(
        &log,
        &mut models,
        Update {
            entries: Some(entries(1, &[1; 10])),
            ..Update::default()
        },
    );
    // A new leader's entries replace 6..=10.
    run(
        &log,
        &mut models,
        Update {
            entries: Some(entries(6, &[2, 2, 2])),
            ..Update::default()
        },
    );
    run(
        &log,
        &mut models,
        Update {
            proposals: vec![Proposal {
                index: 10,
                term: 2,
                bytes: Arc::from(&b"p"[..]),
            }],
            ..Update::default()
        },
    );
    // The engine made 1..=4 durable.
    run(
        &log,
        &mut models,
        Update {
            start: Some(Start { index: 4, term: 1 }),
            ..Update::default()
        },
    );
    check(&log, &models);
    // The log reaching the proposal's index ends it.
    run(
        &log,
        &mut models,
        Update {
            entries: Some(entries(9, &[2, 2])),
            ..Update::default()
        },
    );
    assert!(log.view(7).unwrap().unwrap().proposals.is_empty());
    // A snapshot past the last entry leaves none.
    run(
        &log,
        &mut models,
        Update {
            start: Some(Start { index: 20, term: 3 }),
            ..Update::default()
        },
    );
    check(&log, &models);
    drop(log);
    let (log, _) = Log::open(Arc::clone(&file), config(16, 8), ID).unwrap();
    check(&log, &models);
}

#[test]
fn invalid_updates_are_refused_and_change_nothing() {
    let file = sim(3);
    let mut small = config(16, 8);
    small.max_groups = 2;
    small.group_entries = 4;
    let log = Log::create(Arc::clone(&file), small, ID).unwrap();
    log.write(
        1,
        Update {
            entries: Some(entries(1, &[1, 1])),
            ..Update::default()
        },
    )
    .unwrap();
    let refused = |u: Update| log.write(1, u).unwrap_err();
    assert!(matches!(
        refused(Update {
            entries: Some(entries(4, &[1])),
            ..Update::default()
        }),
        LogError::Invalid { .. }
    ));
    assert!(matches!(
        refused(Update {
            proposals: vec![Proposal {
                index: 2,
                term: 1,
                bytes: Arc::from(&b"x"[..]),
            }],
            ..Update::default()
        }),
        LogError::Invalid { .. }
    ));
    assert!(matches!(
        refused(Update {
            remove: true,
            hard_state: Some(hard(1, 1)),
            ..Update::default()
        }),
        LogError::Invalid { .. }
    ));
    assert!(matches!(
        refused(Update {
            entries: Some(entries(3, &[1, 1, 1])),
            ..Update::default()
        }),
        LogError::Backlog(1)
    ));
    log.write(
        1,
        Update {
            start: Some(Start { index: 1, term: 1 }),
            ..Update::default()
        },
    )
    .unwrap();
    assert!(matches!(
        refused(Update {
            start: Some(Start { index: 0, term: 0 }),
            ..Update::default()
        }),
        LogError::Invalid { .. }
    ));
    assert!(matches!(
        refused(Update {
            entries: Some(entries(1, &[1])),
            ..Update::default()
        }),
        LogError::Invalid { .. }
    ));
    log.write(2, Update::default()).unwrap();
    assert!(matches!(
        log.write(3, Update::default()),
        Err(LogError::TooManyGroups(2))
    ));
    let huge = Entries {
        first: 3,
        entries: vec![Entry {
            term: 1,
            bytes: Arc::from(vec![0u8; 16 * BLOCK]),
        }],
    };
    assert!(matches!(
        refused(Update {
            entries: Some(huge),
            ..Update::default()
        }),
        LogError::TooLarge(_)
    ));
    let view = log.view(1).unwrap().unwrap();
    assert_eq!((view.start.index, view.last), (1, 2));
}

#[test]
fn a_removed_group_leaves_nothing_behind() {
    let file = sim(4);
    let log = Log::create(Arc::clone(&file), config(16, 8), ID).unwrap();
    log.write(
        5,
        Update {
            entries: Some(entries(1, &[1, 1])),
            hard_state: Some(hard(1, 2)),
            ..Update::default()
        },
    )
    .unwrap();
    log.write(
        5,
        Update {
            remove: true,
            ..Update::default()
        },
    )
    .unwrap();
    assert_eq!(log.view(5).unwrap(), None);
    drop(log);
    let (log, _) = Log::open(Arc::clone(&file), config(16, 8), ID).unwrap();
    assert_eq!(log.view(5).unwrap(), None);
    assert!(log.groups().unwrap().is_empty());
}

/// Groups append and compact through segments much smaller than the history, some lagging
/// far behind, so the log reclaims its oldest segment again and again: every live record is
/// kept, before and after reopening.
#[test]
fn reclaiming_segments_keeps_every_live_record() {
    let file = sim(5);
    let cfg = config(8, 6);
    let mut models = Models::new();
    let log = Log::create(Arc::clone(&file), cfg, ID).unwrap();
    for round in 0..400u64 {
        for group in 0..6u128 {
            let m = models.entry(group).or_default().clone();
            let first = m.last() + 1;
            let mut u = Update {
                entries: Some(Entries {
                    first,
                    entries: vec![entry(
                        round / 50 + 1,
                        &"x".repeat(100 + group as usize * 37),
                    )],
                }),
                hard_state: Some(hard(round / 50 + 1, first)),
                ..Update::default()
            };
            // Group 0 never compacts while under 60 entries; the rest keep 5.
            let keep = if group == 0 { 60 } else { 5 };
            if m.last() > m.start.index + keep {
                let index = m.last() - keep;
                let term = m.entries[(index - m.start.index - 1) as usize].0;
                u.start = Some(Start { index, term });
            }
            log.write(group, u.clone()).unwrap();
            apply(&mut models, group, &u);
        }
    }
    check(&log, &models);
    drop(log);
    let (log, recovery) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
    assert!(recovery.damaged.is_empty());
    check(&log, &models);
    // The file never grew past its quota.
    let len = mantle_disk::block::BlockFile::len(&file).unwrap();
    assert!(len <= cfg.segment_bytes * u64::from(cfg.max_segments));
}

#[test]
fn many_submitters_share_flushes() {
    let file = sim(6);
    let log = Arc::new(Log::create(Arc::clone(&file), config(64, 8), ID).unwrap());
    let before = file.stats().unwrap().syncs;
    let threads: Vec<_> = (0..16u128)
        .map(|group| {
            let log = Arc::clone(&log);
            std::thread::spawn(move || {
                for i in 1..=20u64 {
                    loop {
                        let u = Update {
                            entries: Some(entries(i, &[1])),
                            ..Update::default()
                        };
                        match log.write(group, u) {
                            Ok(()) => break,
                            Err(LogError::Busy) => std::thread::yield_now(),
                            Err(e) => panic!("{e}"),
                        }
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let syncs = file.stats().unwrap().syncs - before;
    for group in 0..16u128 {
        assert_eq!(log.view(group).unwrap().unwrap().last, 20);
    }
    assert!(syncs <= 320, "{syncs} flushes for 320 updates");
}

#[test]
fn a_failed_flush_fences_the_log_and_loses_nothing_acknowledged() {
    let file = sim(7);
    let mut models = Models::new();
    let log = Log::create(Arc::clone(&file), config(16, 8), ID).unwrap();
    let u = Update {
        entries: Some(entries(1, &[1, 1])),
        ..Update::default()
    };
    log.write(1, u.clone()).unwrap();
    apply(&mut models, 1, &u);
    file.inject(Fault::SyncError).unwrap();
    let lost = Update {
        entries: Some(entries(3, &[1])),
        ..Update::default()
    };
    assert!(matches!(log.write(1, lost), Err(LogError::Fenced)));
    assert!(log.is_fenced());
    assert!(matches!(
        log.submit(1, Update::default()),
        Err(LogError::Fenced)
    ));
    drop(log);
    file.crash(Crash::LoseAll).unwrap();
    file.clear_faults().unwrap();
    let (log, _) = Log::open(Arc::clone(&file), config(16, 8), ID).unwrap();
    check(&log, &models);
}

#[test]
fn damage_to_an_acknowledged_frame_is_reported() {
    let file = sim(8);
    let log = Log::create(Arc::clone(&file), config(16, 8), ID).unwrap();
    for i in 1..=4u64 {
        log.write(
            1,
            Update {
                entries: Some(entries(i, &[1])),
                ..Update::default()
            },
        )
        .unwrap();
    }
    drop(log);
    // The second frame, just after the segment header and the empty first frame.
    let second = 2 * BLOCK as u64 + 70;
    file.inject(Fault::BitFlip {
        offset: second,
        bit: 3,
        stored: true,
    })
    .unwrap();
    assert!(matches!(
        Log::open(Arc::clone(&file), config(16, 8), ID),
        Err(LogError::Damaged(_))
    ));
}

#[test]
fn another_log_or_geometry_is_refused() {
    let file = sim(9);
    drop(Log::create(Arc::clone(&file), config(16, 8), ID).unwrap());
    assert!(matches!(
        Log::open(Arc::clone(&file), config(16, 8), ID + 1),
        Err(LogError::Foreign(_))
    ));
    assert!(matches!(
        Log::open(Arc::clone(&file), config(32, 8), ID),
        Err(LogError::Foreign(_))
    ));
    assert!(matches!(
        Log::create(Arc::clone(&file), config(16, 8), ID),
        Err(LogError::Foreign(_))
    ));
    assert!(matches!(
        Log::create(sim(10), config(3, 8), ID),
        Err(LogError::Config(_))
    ));
}

/// A step of a generated history.
#[derive(Debug, Clone)]
enum Step {
    Append {
        group: u128,
        terms: Vec<u64>,
        back: u64,
    },
    Compact {
        group: u128,
        keep: u64,
    },
    Snapshot {
        group: u128,
        ahead: u64,
    },
    Hard {
        group: u128,
        term: u64,
    },
    Propose {
        group: u128,
        ahead: u64,
    },
    Remove {
        group: u128,
    },
    /// Power is cut after `ops` more device writes and flushes, and the log reopened.
    Crash {
        ops: u64,
    },
}

fn step() -> impl Strategy<Value = Step> {
    let group = 0u128..4;
    prop_oneof![
        6 => (group.clone(), prop::collection::vec(1u64..4, 1..6), 0u64..3)
            .prop_map(|(group, terms, back)| Step::Append { group, terms, back }),
        2 => (group.clone(), 0u64..4).prop_map(|(group, keep)| Step::Compact { group, keep }),
        1 => (group.clone(), 0u64..4).prop_map(|(group, ahead)| Step::Snapshot { group, ahead }),
        2 => (group.clone(), 1u64..5).prop_map(|(group, term)| Step::Hard { group, term }),
        1 => (group.clone(), 1u64..3).prop_map(|(group, ahead)| Step::Propose { group, ahead }),
        1 => group.prop_map(|group| Step::Remove { group }),
        1 => (0u64..6).prop_map(|ops| Step::Crash { ops }),
    ]
}

/// The update a step makes of the group as the model holds it.
fn update(step: &Step, models: &Models) -> Option<(u128, Update)> {
    let m = |group: &u128| models.get(group).cloned().unwrap_or_default();
    match step {
        Step::Append { group, terms, back } => {
            let m = m(group);
            let first = (m.last() + 1).saturating_sub(*back).max(m.start.index + 1);
            Some((
                *group,
                Update {
                    entries: Some(entries(first, terms)),
                    ..Update::default()
                },
            ))
        }
        Step::Compact { group, keep } => {
            let m = m(group);
            let held = m.entries.len() as u64;
            (held > *keep).then(|| {
                let index = m.start.index + held - keep;
                let term = m.entries[(index - m.start.index - 1) as usize].0;
                (
                    *group,
                    Update {
                        start: Some(Start { index, term }),
                        ..Update::default()
                    },
                )
            })
        }
        Step::Snapshot { group, ahead } => {
            let m = m(group);
            let index = m.last() + ahead;
            let start = Start { index, term: 9 };
            // A snapshot discards what follows it too.
            Some((
                *group,
                Update {
                    start: Some(start),
                    entries: Some(Entries {
                        first: index + 1,
                        entries: Vec::new(),
                    }),
                    ..Update::default()
                },
            ))
        }
        Step::Hard { group, term } => Some((
            *group,
            Update {
                hard_state: Some(hard(*term, m(group).last())),
                ..Update::default()
            },
        )),
        Step::Propose { group, ahead } => Some((
            *group,
            Update {
                proposals: vec![Proposal {
                    index: m(group).last() + ahead,
                    term: 1,
                    bytes: Arc::from(&b"fast"[..]),
                }],
                ..Update::default()
            },
        )),
        Step::Remove { group } => models.contains_key(group).then(|| {
            (
                *group,
                Update {
                    remove: true,
                    ..Update::default()
                },
            )
        }),
        Step::Crash { .. } => None,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    /// Generated histories, cut by power loss at random points: after each reopen the log
    /// holds every acknowledged update, and the one in flight either wholly or not at all.
    #[test]
    fn every_acknowledged_update_survives_power_loss(
        steps in prop::collection::vec(step(), 1..60),
        seed in any::<u64>(),
        segment_blocks in 4u64..10,
        max_segments in 3u32..8,
    ) {
        let file = sim(seed);
        let cfg = config(segment_blocks, max_segments);
        let mut log = Log::create(Arc::clone(&file), cfg, ID).unwrap();
        let mut models = Models::new();
        let mut cut = false;
        for step in &steps {
            if let Step::Crash { ops } = step {
                file.inject(Fault::PowerCut { ops: *ops }).unwrap();
                cut = true;
                continue;
            }
            let Some((group, u)) = update(step, &models) else { continue };
            match log.write(group, u.clone()) {
                Ok(()) => apply(&mut models, group, &u),
                Err(LogError::Fenced) => {
                    prop_assert!(cut);
                    // Power is gone: crash, reopen, and see whether the update landed, which
                    // it may have wholly or not at all.
                    drop(log);
                    file.crash(Crash::Random).unwrap();
                    file.clear_faults().unwrap();
                    cut = false;
                    let (reopened, recovery) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
                    prop_assert!(recovery.damaged.is_empty());
                    let mut landed = models.clone();
                    apply(&mut landed, group, &u);
                    if holds(&reopened, group, landed.get(&group)) {
                        models = landed;
                    }
                    check(&reopened, &models);
                    log = reopened;
                }
                // Refused whole, changing nothing: the update breaks a rule, or every segment
                // holds live records the sweep of the tail cannot free.
                Err(
                    LogError::Invalid { .. }
                    | LogError::Backlog(_)
                    | LogError::TooManyGroups(_)
                    | LogError::Full,
                ) => {}
                Err(e) => return Err(TestCaseError::fail(format!("{e}"))),
            }
        }
        drop(log);
        file.clear_faults().unwrap();
        let (log, recovery) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
        prop_assert!(recovery.damaged.is_empty());
        check(&log, &models);
    }
}
