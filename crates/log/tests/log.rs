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
/// The persist area before segment 0 of the logs of `config(16, _)`: one segment's length.
const AREA: u64 = 16 * BLOCK as u64;

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
    /// The mark a restore left: entries the log may lack (raft-log.md §6).
    uncertain: Option<Start>,
}

impl Model {
    fn last(&self) -> u64 {
        self.start.index + self.entries.len() as u64
    }

    fn last_term(&self) -> u64 {
        self.entries.last().map_or(self.start.term, |e| e.0)
    }

    /// A mark ends once the log again reaches its index or holds an entry of a later term.
    fn settle(&mut self) {
        if let Some(mark) = self.uncertain
            && (self.last() >= mark.index || self.last_term() > mark.term)
        {
            self.uncertain = None;
        }
    }

    /// What recovery restores when the frame of `u` tore before its flush was confirmed but
    /// after its persist record landed: the torn tail, keeping only a later term or a vote
    /// given, at the group's own commit.
    fn kept(&self, u: &Update) -> Self {
        let mut m = self.clone();
        if let Some(h) = u.hard_state
            && m.hard
                .is_none_or(|c| h.term > c.term || (h.term == c.term && c.vote == 0))
        {
            m.hard = Some(HardState {
                commit: m.hard.map_or(0, |c| c.commit),
                ..h
            });
        }
        m
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
        self.settle();
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
    if (
        view.start,
        view.last,
        view.hard_state,
        &proposals,
        view.uncertain,
    ) != (m.start, m.last(), m.hard, &m.proposals, m.uncertain)
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
            (
                view.start,
                view.last,
                view.hard_state,
                &proposals,
                view.uncertain
            ),
            (m.start, m.last(), m.hard, &m.proposals, m.uncertain),
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

/// A replica's update waits for room rather than being refused: through a queue of one,
/// every waiting submitter gets through.
#[test]
fn waiting_submitters_are_never_refused() {
    let file = sim(11);
    let mut cfg = config(64, 8);
    cfg.queue_submissions = 1;
    let log = Arc::new(Log::create(Arc::clone(&file), cfg, ID).unwrap());
    let threads: Vec<_> = (0..8u128)
        .map(|group| {
            let log = Arc::clone(&log);
            std::thread::spawn(move || {
                for i in 1..=20u64 {
                    let u = Update {
                        entries: Some(entries(i, &[1])),
                        ..Update::default()
                    };
                    log.write_waiting(group, u).unwrap();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    for group in 0..8u128 {
        assert_eq!(log.view(group).unwrap().unwrap().last, 20);
    }
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
    assert!(matches!(log.write(1, lost.clone()), Err(LogError::Fenced)));
    assert!(log.is_fenced());
    assert!(matches!(
        log.submit(1, Update::default()),
        Err(LogError::Fenced)
    ));
    drop(log);
    file.crash(Crash::LoseAll).unwrap();
    file.clear_faults().unwrap();
    let (log, recovery) = Log::open(Arc::clone(&file), config(16, 8), ID).unwrap();
    // Whatever of the lost frame's persist record the failed flush left durable, the frame
    // was never confirmed: it is the torn tail, and it held no term or vote to keep.
    assert!(recovery.restored.is_empty());
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
    // The second frame, just after the segment header and the empty first frame, in segment
    // 0, which follows the persist area of one segment's length.
    let second = AREA + 2 * BLOCK as u64 + 70;
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

/// The last frame, flushed and acknowledged, then damaged at rest, whatever part of it no
/// longer reads: nothing after it proves it was flushed, but its persist record does. Its
/// group's term and vote come back as the frame left them, never older; the entries it wrote
/// are cut and marked as possibly lacking, so the replica takes no part in elections until
/// it holds them again; the restore is durable across reopening; and entries the leader
/// sends again end the mark (audit S01, the last frame).
#[test]
fn damage_to_the_last_acknowledged_frame_is_restored_from_its_persist_record() {
    for field in [0u64, 8, 40, 64, 70, 120] {
        let file = sim(30 + field);
        let cfg = config(16, 8);
        let log = Log::create(Arc::clone(&file), cfg, ID).unwrap();
        for i in 1..=3u64 {
            log.write(
                1,
                Update {
                    entries: Some(entries(i, &[1])),
                    hard_state: Some(HardState {
                        term: 1,
                        vote: 2,
                        commit: i - 1,
                    }),
                    ..Update::default()
                },
            )
            .unwrap();
        }
        // The last frame: the replica moved to term 2, voted for 3, and took entry 4.
        let voted = HardState {
            term: 2,
            vote: 3,
            commit: 3,
        };
        log.write(
            1,
            Update {
                entries: Some(entries(4, &[2])),
                hard_state: Some(voted),
                ..Update::default()
            },
        )
        .unwrap();
        drop(log);
        // Frames of one block each: the empty first frame, then one an update.
        file.inject(Fault::BitFlip {
            offset: AREA + 5 * BLOCK as u64 + field,
            bit: 2,
            stored: true,
        })
        .unwrap();
        for reopening in 0..2 {
            let (log, recovery) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
            let view = log.view(1).unwrap().unwrap();
            assert_eq!(
                view.hard_state,
                Some(voted),
                "byte {field}, reopening {reopening}"
            );
            assert_eq!(view.last, 3);
            assert_eq!(view.uncertain, Some(Start { index: 4, term: 2 }));
            assert_eq!(recovery.damaged, Vec::<u128>::new());
            if reopening == 0 {
                assert_eq!(recovery.restored, vec![1]);
            }
        }
        let (log, _) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
        log.write(
            1,
            Update {
                entries: Some(entries(4, &[2])),
                ..Update::default()
            },
        )
        .unwrap();
        assert_eq!(log.view(1).unwrap().unwrap().uncertain, None);
        drop(log);
        let (log, recovery) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
        assert_eq!(recovery.restored, Vec::<u128>::new());
        let view = log.view(1).unwrap().unwrap();
        assert_eq!(
            (view.last, view.uncertain, view.hard_state),
            (4, None, Some(voted))
        );
    }
}

/// A last frame that held proposals, which its persist record cannot restore, leaves its
/// group damaged: the log serves it to no one, since a replica opening it as new would vote
/// again in a term it voted in, and takes nothing for it but its removal, after which the
/// group may start over. Other groups of the frame are restored.
#[test]
fn a_lost_frame_with_proposals_leaves_its_group_damaged_until_removed() {
    let file = sim(41);
    let cfg = config(16, 8);
    let gated = Gated::new(Arc::clone(&file));
    let log = Log::create(Arc::clone(&gated), cfg, ID).unwrap();
    for group in [1, 2] {
        log.write(
            group,
            Update {
                entries: Some(entries(1, &[1])),
                ..Update::default()
            },
        )
        .unwrap();
    }
    // Both groups' next updates go in one frame: the writer is held in a flush meanwhile.
    let shut = gated.shut();
    let plug = log
        .submit(
            3,
            Update {
                entries: Some(entries(1, &[1])),
                ..Update::default()
            },
        )
        .unwrap();
    gated.held();
    let with_proposal = log
        .submit(
            1,
            Update {
                hard_state: Some(HardState {
                    term: 2,
                    vote: 1,
                    commit: 0,
                }),
                proposals: vec![Proposal {
                    index: 5,
                    term: 2,
                    bytes: Arc::from(&b"p"[..]),
                }],
                ..Update::default()
            },
        )
        .unwrap();
    let voted = HardState {
        term: 3,
        vote: 2,
        commit: 1,
    };
    let other = log
        .submit(
            2,
            Update {
                entries: Some(entries(2, &[3])),
                hard_state: Some(voted),
                ..Update::default()
            },
        )
        .unwrap();
    drop(shut);
    plug.wait().unwrap();
    with_proposal.wait().unwrap();
    other.wait().unwrap();
    drop(log);
    // That frame, the last, is damaged at rest.
    let image = file.durable_image().unwrap();
    let last = valid_frames(&image)
        .into_iter()
        .max_by_key(|f| f.2)
        .unwrap();
    file.inject(Fault::BitFlip {
        offset: last.0 + 70,
        bit: 4,
        stored: true,
    })
    .unwrap();
    let (log, recovery) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
    assert_eq!(recovery.damaged, vec![1]);
    assert_eq!(recovery.restored, vec![2]);
    assert!(matches!(log.view(1), Err(LogError::Damaged(_))));
    assert!(matches!(
        log.write(1, Update::default()),
        Err(LogError::Damaged(_))
    ));
    let view = log.view(2).unwrap().unwrap();
    assert_eq!(
        (view.hard_state, view.uncertain),
        (Some(voted), Some(Start { index: 2, term: 3 }))
    );
    log.write(
        1,
        Update {
            remove: true,
            ..Update::default()
        },
    )
    .unwrap();
    assert_eq!(log.view(1).unwrap(), None);
    log.write(
        1,
        Update {
            entries: Some(entries(1, &[4])),
            ..Update::default()
        },
    )
    .unwrap();
    assert_eq!(log.view(1).unwrap().unwrap().last, 1);
}

/// An uncertainty mark is a live piece of the log: reclaiming the segment it was written in
/// copies it, as it copies a hard state, and it survives reopening until it ends.
#[test]
fn an_uncertainty_mark_survives_reclaiming_its_segment() {
    let file = sim(42);
    let cfg = config(8, 6);
    let log = Log::create(Arc::clone(&file), cfg, ID).unwrap();
    log.write(
        1,
        Update {
            entries: Some(entries(1, &[1, 1])),
            ..Update::default()
        },
    )
    .unwrap();
    log.write(
        1,
        Update {
            entries: Some(entries(3, &[2])),
            hard_state: Some(HardState {
                term: 2,
                vote: 1,
                commit: 2,
            }),
            ..Update::default()
        },
    )
    .unwrap();
    drop(log);
    let image = file.durable_image().unwrap();
    let last = valid_frames(&image)
        .into_iter()
        .max_by_key(|f| f.2)
        .unwrap();
    file.inject(Fault::BitFlip {
        offset: last.0 + 70,
        bit: 4,
        stored: true,
    })
    .unwrap();
    let (log, recovery) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
    assert_eq!(recovery.restored, vec![1]);
    let mark = Some(Start { index: 3, term: 2 });
    assert_eq!(log.view(1).unwrap().unwrap().uncertain, mark);
    // Another group writes and compacts, lap after lap, until every segment has been
    // reclaimed at least once.
    let segments = u64::from(cfg.max_segments);
    let per_segment = cfg.segment_bytes / BLOCK as u64;
    for i in 1..=segments * per_segment * 3 {
        log.write(
            2,
            Update {
                start: (i > 1).then(|| Start {
                    index: i - 1,
                    term: 1,
                }),
                entries: Some(entries(i, &[1])),
                ..Update::default()
            },
        )
        .unwrap();
    }
    assert_eq!(log.view(1).unwrap().unwrap().uncertain, mark);
    drop(log);
    let (log, _) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
    assert_eq!(log.view(1).unwrap().unwrap().uncertain, mark);
}

/// A frame whose flush never completed may still have its persist record on the disk, whole,
/// beside a torn frame: nothing confirms the frame was flushed, so it is the torn tail and its
/// entries are gone, but the term and vote it held are kept, since keeping them is always
/// safe and forgetting a vote given could let the replica vote twice.
#[test]
fn an_unconfirmed_torn_frame_keeps_only_its_term_and_vote() {
    let file = sim(43);
    let cfg = config(16, 8);
    let log = Log::create(Arc::clone(&file), cfg, ID).unwrap();
    log.write(
        1,
        Update {
            entries: Some(entries(1, &[1])),
            hard_state: Some(HardState {
                term: 1,
                vote: 1,
                commit: 1,
            }),
            ..Update::default()
        },
    )
    .unwrap();
    // The next frame's writes, frame and persist record, reach the device; its flush fails,
    // and every written sector survives the crash.
    file.inject(Fault::PowerCut { ops: 2 }).unwrap();
    let voted = HardState {
        term: 4,
        vote: 3,
        commit: 2,
    };
    assert!(matches!(
        log.write(
            1,
            Update {
                entries: Some(entries(2, &[4])),
                hard_state: Some(voted),
                ..Update::default()
            },
        ),
        Err(LogError::Fenced)
    ));
    drop(log);
    file.crash(Crash::KeepAll).unwrap();
    file.clear_faults().unwrap();
    // The frame tears: its payload no longer reads.
    let image = file.durable_image().unwrap();
    let torn = valid_frames(&image)
        .into_iter()
        .max_by_key(|f| f.2)
        .unwrap();
    assert_eq!(torn.2, 2, "the unflushed frame's writes survived");
    file.inject(Fault::BitFlip {
        offset: torn.0 + 70,
        bit: 0,
        stored: true,
    })
    .unwrap();
    let (log, recovery) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
    assert_eq!((recovery.restored, recovery.damaged), (vec![1], vec![]));
    let view = log.view(1).unwrap().unwrap();
    assert_eq!(view.last, 1);
    assert_eq!(view.uncertain, None);
    assert_eq!(
        view.hard_state,
        Some(HardState {
            term: 4,
            vote: 3,
            commit: 1,
        })
    );
}

/// A frame torn by a crash before its persist record reached the disk was never
/// acknowledged: the log is cut before it, and nothing is restored or marked.
#[test]
fn a_torn_frame_without_its_persist_record_is_the_torn_tail() {
    let file = sim(40);
    let cfg = config(16, 8);
    let log = Log::create(Arc::clone(&file), cfg, ID).unwrap();
    log.write(
        1,
        Update {
            entries: Some(entries(1, &[1])),
            ..Update::default()
        },
    )
    .unwrap();
    file.inject(Fault::PowerCut { ops: 0 }).unwrap();
    assert!(matches!(
        log.write(
            1,
            Update {
                entries: Some(entries(2, &[1])),
                ..Update::default()
            },
        ),
        Err(LogError::Fenced)
    ));
    drop(log);
    file.crash(Crash::LoseAll).unwrap();
    file.clear_faults().unwrap();
    let (log, recovery) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
    assert_eq!(recovery.restored, Vec::<u128>::new());
    let view = log.view(1).unwrap().unwrap();
    assert_eq!((view.last, view.uncertain), (1, None));
}

/// Damage to any field of an acknowledged frame, header or payload, is reported when a later
/// frame proves the frame was flushed: a header whose magic, format or identity no longer
/// reads is damage, not the log's end (audit S01).
#[test]
fn damage_to_any_field_of_an_acknowledged_frame_is_reported() {
    // Offsets in the frame header: magic, format, padding, log, incarnation, nonce,
    // sequence, tail, payload length, record count, CRC; then the payload.
    for field in [0u64, 4, 5, 8, 24, 32, 40, 48, 56, 60, 64, 70] {
        let file = sim(10 + field);
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
        // The frame of the first update, after the segment header and the empty first frame.
        file.inject(Fault::BitFlip {
            offset: AREA + 2 * BLOCK as u64 + field,
            bit: 0,
            stored: true,
        })
        .unwrap();
        let opened = Log::open(Arc::clone(&file), config(16, 8), ID);
        assert!(
            matches!(opened, Err(LogError::Damaged(_))),
            "damage at byte {field} of a frame: {:?}",
            opened.map(|_| ())
        );
    }
}

/// Each segment header in the durable image, by slot: its offset and incarnation.
fn segment_headers(image: &[u8], segment: u64) -> Vec<(u64, u64)> {
    image
        .chunks(segment as usize)
        .enumerate()
        .filter_map(|(slot, s)| {
            let h = mantle_log::format::SegmentHeader::decode(s)?;
            Some((slot as u64 * segment, h.incarnation))
        })
        .collect()
}

/// Each valid frame in the durable image: its offset, incarnation and sequence.
fn valid_frames(image: &[u8]) -> Vec<(u64, u64, u64)> {
    let mut out = Vec::new();
    for (i, block) in image.chunks(BLOCK).enumerate() {
        let Some(h) = mantle_log::format::FrameHeader::decode(block) else {
            continue;
        };
        let start = i * BLOCK;
        let Some(frame) = h.frame_len().and_then(|len| image.get(start..start + len)) else {
            continue;
        };
        if h.log == ID && h.verifies(frame) {
            out.push((start as u64, h.incarnation, h.sequence));
        }
    }
    out
}

/// A log of one group whose `n` updates each took a frame of one block: segment 0 holds the
/// empty first frame and updates 1 to 14, and segment 1 the rest.
fn one_frame_each(seed: u64, n: u64) -> Arc<SimFile> {
    let file = sim(seed);
    let log = Log::create(Arc::clone(&file), config(16, 8), ID).unwrap();
    for i in 1..=n {
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
    let frames = valid_frames(&file.durable_image().unwrap());
    assert_eq!(frames.len() as u64, n + 1, "one frame an update");
    file
}

/// A newer segment whose header no longer reads, holding frames after its first, was flushed
/// with its header: its damage is reported, and the failed open writes nothing.
#[test]
fn a_damaged_header_of_the_newest_segment_is_reported() {
    let file = one_frame_each(20, 16);
    let segment = 16 * BLOCK as u64;
    file.inject(Fault::BitFlip {
        offset: AREA + segment,
        bit: 0,
        stored: true,
    })
    .unwrap();
    let before = file.durable_image().unwrap();
    assert!(matches!(
        Log::open(Arc::clone(&file), config(16, 8), ID),
        Err(LogError::Damaged(_))
    ));
    assert!(
        file.durable_image().unwrap() == before,
        "a failed open wrote"
    );
}

/// An opening whose header never became durable, with only its first frame, was never
/// acknowledged: the log is cut before it, and the next update goes on from there.
#[test]
fn an_opening_whose_header_never_became_durable_is_the_torn_tail() {
    let file = one_frame_each(21, 15);
    let segment = AREA + 16 * BLOCK as u64;
    let zeros = mantle_disk::buf::AlignedBuf::zeroed(BLOCK, Alignment::new(BLOCK).unwrap())
        .map(|mut b| {
            b.set_len(BLOCK).unwrap();
            b
        })
        .unwrap();
    mantle_disk::block::BlockFile::write_all_at(&*file, zeros.as_slice(), segment).unwrap();
    mantle_disk::block::BlockFile::sync_data(&*file).unwrap();
    let (log, _) = Log::open(Arc::clone(&file), config(16, 8), ID).unwrap();
    assert_eq!(log.view(1).unwrap().unwrap().last, 14);
    log.write(
        1,
        Update {
            entries: Some(entries(15, &[2])),
            ..Update::default()
        },
    )
    .unwrap();
    drop(log);
    let (log, recovery) = Log::open(Arc::clone(&file), config(16, 8), ID).unwrap();
    assert!(recovery.damaged.is_empty());
    assert_eq!(log.view(1).unwrap().unwrap().last, 15);
    assert_eq!(log.term(1, 15).unwrap(), 2);
}

/// Damage to the last frame of a segment the log went on past is reported: the next
/// segment's first frame proves it was flushed.
#[test]
fn damage_at_the_end_of_a_segment_the_log_went_past_is_reported() {
    for field in [0u64, 8, 40, 70] {
        let file = one_frame_each(22 + field, 16);
        // Update 14's frame, the last in segment 0.
        file.inject(Fault::BitFlip {
            offset: AREA + 15 * BLOCK as u64 + field,
            bit: 1,
            stored: true,
        })
        .unwrap();
        assert!(
            matches!(
                Log::open(Arc::clone(&file), config(16, 8), ID),
                Err(LogError::Damaged(_))
            ),
            "damage at byte {field}"
        );
    }
}

/// In a slot reused by a newer segment, the older segment's frames past the newer one's
/// last prove nothing about it: damage to the newer segment's last frame is its torn tail,
/// while damage to one followed by the newer segment's own frames is reported.
#[test]
fn stale_frames_in_a_reused_slot_prove_nothing() {
    let file = sim(30);
    let cfg = config(8, 6);
    let segment = cfg.segment_bytes;
    let log = Log::create(Arc::clone(&file), cfg, ID).unwrap();
    let mut first = 1u64;
    // Frames of the head's incarnation in a reused slot, stale frames after them, and at
    // least two of the head's own.
    let mut found = None;
    for round in 0..2_000u64 {
        let mut u = Update {
            entries: Some(entries(first, &[1])),
            ..Update::default()
        };
        if first > 3 {
            u.start = Some(Start {
                index: first - 3,
                term: 1,
            });
        }
        log.write(1, u).unwrap();
        first += 1;
        let image = file.durable_image().unwrap();
        let headers = segment_headers(&image, segment);
        let (head_at, head) = *headers.iter().max_by_key(|(_, inc)| *inc).unwrap();
        let frames = valid_frames(&image);
        let own: Vec<_> = frames
            .iter()
            .filter(|(at, inc, _)| *inc == head && *at >= head_at && *at < head_at + segment)
            .copied()
            .collect();
        let last_own = own.iter().map(|(at, ..)| *at).max();
        let stale_after = frames.iter().any(|(at, inc, _)| {
            *inc < head && *at >= head_at && *at < head_at + segment && Some(*at) > last_own
        });
        if own.len() >= 3 && stale_after && round > 50 {
            found = Some(own);
            break;
        }
    }
    drop(log);
    let own = found.expect("the head never came to a reused slot with stale frames past it");
    let (last_at, _, last_seq) = *own.iter().max_by_key(|(_, _, seq)| *seq).unwrap();
    let (inner_at, ..) = *own
        .iter()
        .filter(|(_, _, seq)| *seq < last_seq)
        .max_by_key(|(_, _, seq)| *seq)
        .unwrap();

    let torn = sim(31);
    let image = file.durable_image().unwrap();
    for (i, chunk) in image.chunks(BLOCK).enumerate() {
        let mut b =
            mantle_disk::buf::AlignedBuf::zeroed(BLOCK, Alignment::new(BLOCK).unwrap()).unwrap();
        b.extend_from_slice(chunk).unwrap();
        mantle_disk::block::BlockFile::write_all_at(&*torn, b.as_slice(), (i * BLOCK) as u64)
            .unwrap();
    }
    mantle_disk::block::BlockFile::sync_data(&*torn).unwrap();

    file.inject(Fault::BitFlip {
        offset: last_at,
        bit: 0,
        stored: true,
    })
    .unwrap();
    let (log, _) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
    assert_eq!(log.view(1).unwrap().unwrap().last, first - 2);
    drop(log);

    torn.inject(Fault::BitFlip {
        offset: inner_at,
        bit: 0,
        stored: true,
    })
    .unwrap();
    assert!(matches!(
        Log::open(Arc::clone(&torn), cfg, ID),
        Err(LogError::Damaged(_))
    ));
}

/// A simulated file whose flushes wait while its gate is shut, so a test can hold the writer
/// in a flush while it queues submissions behind it.
struct Gated {
    file: Arc<SimFile>,
    gate: std::sync::Mutex<(bool, u64)>,
    changed: std::sync::Condvar,
}

impl Gated {
    fn new(file: Arc<SimFile>) -> Arc<Self> {
        Arc::new(Self {
            file,
            gate: std::sync::Mutex::new((true, 0)),
            changed: std::sync::Condvar::new(),
        })
    }

    fn set(&self, open: bool) {
        self.gate.lock().unwrap().0 = open;
        self.changed.notify_all();
    }

    /// Shuts the gate until the guard is dropped, as a failing test's unwinding drops it too,
    /// so a log dropped after it never waits on a writer held at the gate.
    fn shut(self: &Arc<Self>) -> Shut {
        self.set(false);
        Shut(Arc::clone(self))
    }

    /// Waits until a flush is held at the shut gate.
    fn held(&self) {
        let mut gate = self.gate.lock().unwrap();
        while gate.1 == 0 {
            gate = self.changed.wait(gate).unwrap();
        }
    }
}

struct Shut(Arc<Gated>);

impl Drop for Shut {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

impl mantle_disk::block::BlockFile for Gated {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }

    fn len(&self) -> Result<u64, mantle_disk::DiskError> {
        mantle_disk::block::BlockFile::len(&*self.file)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), mantle_disk::DiskError> {
        self.file.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), mantle_disk::DiskError> {
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), mantle_disk::DiskError> {
        let mut gate = self.gate.lock().unwrap();
        gate.1 += 1;
        self.changed.notify_all();
        while !gate.0 {
            gate = self.changed.wait(gate).unwrap();
        }
        gate.1 -= 1;
        drop(gate);
        self.file.sync_data()
    }
}

/// A group's updates are made durable in the order it submitted them, even when the older
/// one waits for a frame with room and a newer one would fit the frame it missed (audit S02).
#[test]
fn a_groups_updates_keep_their_order_when_one_waits_for_room() {
    let gated = Gated::new(sim(40));
    let cfg = config(4, 8);
    let log = Log::create(Arc::clone(&gated), cfg, ID).unwrap();
    let state = |term, vote| HardState {
        term,
        vote,
        commit: 0,
    };
    let shut = gated.shut();
    let first = log
        .submit(
            3,
            Update {
                hard_state: Some(state(1, 3)),
                ..Update::default()
            },
        )
        .unwrap();
    gated.held();
    let big = log
        .submit(
            2,
            Update {
                entries: Some(Entries {
                    first: 1,
                    entries: vec![entry(1, &"b".repeat(10_000))],
                }),
                ..Update::default()
            },
        )
        .unwrap();
    let older = log
        .submit(
            1,
            Update {
                entries: Some(Entries {
                    first: 1,
                    entries: vec![entry(1, &"a".repeat(3_000))],
                }),
                hard_state: Some(state(1, 1)),
                ..Update::default()
            },
        )
        .unwrap();
    let newer = log
        .submit(
            1,
            Update {
                hard_state: Some(state(2, 2)),
                ..Update::default()
            },
        )
        .unwrap();
    drop(shut);
    for pending in [first, big, older, newer] {
        pending.wait().unwrap();
    }
    assert_eq!(log.view(1).unwrap().unwrap().hard_state, Some(state(2, 2)));
    drop(log);
    let (log, _) = Log::open(Arc::clone(&gated), cfg, ID).unwrap();
    assert_eq!(log.view(1).unwrap().unwrap().hard_state, Some(state(2, 2)));
}

/// The queue's bound holds everything the writer has not answered: submissions waiting in
/// the channel, updates held for a later frame and the frame being flushed. A refusal comes
/// at the bound, never after it (audit S03).
#[test]
fn the_queue_bounds_every_submission_not_yet_answered() {
    let gated = Gated::new(sim(41));
    let mut cfg = config(16, 8);
    cfg.queue_submissions = 2;
    let log = Log::create(Arc::clone(&gated), cfg, ID).unwrap();
    let small = |term| Update {
        hard_state: Some(hard(term, 0)),
        ..Update::default()
    };
    let shut = gated.shut();
    let mut pending = vec![log.submit(1, small(1)).unwrap()];
    gated.held();
    // One flush is held: the queue takes one more, from any group, and no third.
    pending.push(log.submit(2, small(1)).unwrap());
    assert!(matches!(log.submit(3, small(1)), Err(LogError::Busy)));
    assert!(matches!(log.submit(1, small(2)), Err(LogError::Busy)));
    drop(shut);
    for p in pending.drain(..) {
        p.wait().unwrap();
    }
    // A group holds two submissions at most: the one a frame takes and one for the next.
    let mut cfg = config(16, 8);
    cfg.queue_submissions = 8;
    let gated = Gated::new(sim(42));
    let log = Log::create(Arc::clone(&gated), cfg, ID).unwrap();
    let shut = gated.shut();
    let mut pending = vec![log.submit(1, small(1)).unwrap()];
    gated.held();
    pending.push(log.submit(1, small(2)).unwrap());
    assert!(matches!(log.submit(1, small(3)), Err(LogError::Busy)));
    pending.push(log.submit(2, small(1)).unwrap());
    drop(shut);
    for p in pending {
        p.wait().unwrap();
    }
    assert_eq!(log.view(1).unwrap().unwrap().hard_state, Some(hard(2, 0)));
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
                    // A frame whose flush power cut was never confirmed: never damaged.
                    prop_assert!(recovery.damaged.is_empty());
                    let mut landed = models.clone();
                    apply(&mut landed, group, &u);
                    if holds(&reopened, group, landed.get(&group)) {
                        models = landed;
                    } else if recovery.restored.contains(&group) {
                        // The frame tore after its persist record landed: the torn tail, but
                        // for the term and vote it held.
                        let kept = models.get(&group).cloned().unwrap_or_default().kept(&u);
                        prop_assert!(holds(&reopened, group, Some(&kept)));
                        models.insert(group, kept);
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// Rounds of updates submitted while the writer is held in a flush, two of a group at a
    /// time where the queue takes them and sized from a few bytes to most of a frame's room,
    /// so some wait for room behind others: each group's accepted updates become durable in
    /// the order submitted, as its state before and after reopening shows (audit S02).
    #[test]
    fn a_groups_queued_updates_become_durable_in_order(
        rounds in prop::collection::vec(
            prop::collection::vec((step(), 0usize..4), 1..12),
            1..6,
        ),
        seed in any::<u64>(),
        segment_blocks in 4u64..10,
    ) {
        let gated = Gated::new(sim(seed));
        let cfg = config(segment_blocks, 8);
        let log = Log::create(Arc::clone(&gated), cfg, ID).unwrap();
        let segment = cfg.segment_bytes as usize;
        let sizes = [16, segment / 4, segment / 2 - 200, segment - 2 * BLOCK];
        let mut models = Models::new();
        let plug = Update {
            hard_state: Some(hard(1, 0)),
            ..Update::default()
        };
        for round in &rounds {
            let shut = gated.shut();
            let plugged = log.submit(99, plug.clone()).unwrap();
            gated.held();
            let mut predicted = models.clone();
            let mut submitted = Vec::new();
            for (step, size) in round {
                let Some((group, mut u)) = update(step, &predicted) else { continue };
                if let Some(e) = &mut u.entries {
                    for x in &mut e.entries {
                        x.bytes = Arc::from(vec![b'x'; sizes[*size]]);
                    }
                }
                apply(&mut predicted, group, &u);
                let pending = match log.submit(group, u.clone()) {
                    Ok(pending) => pending,
                    // At the queue's bound, or the group's: refused whole, changing nothing.
                    Err(LogError::Busy) => continue,
                    Err(e) => return Err(TestCaseError::fail(format!("{e}"))),
                };
                submitted.push((group, u, pending));
            }
            drop(shut);
            plugged.wait().unwrap();
            apply(&mut models, 99, &plug);
            for (group, u, pending) in submitted {
                match pending.wait() {
                    Ok(()) => apply(&mut models, group, &u),
                    Err(
                        LogError::Invalid { .. }
                        | LogError::TooLarge(_)
                        | LogError::Full
                        | LogError::Backlog(_)
                        | LogError::TooManyGroups(_),
                    ) => {}
                    Err(e) => return Err(TestCaseError::fail(format!("{e}"))),
                }
            }
            check(&log, &models);
        }
        drop(log);
        let (log, recovery) = Log::open(Arc::clone(&gated), cfg, ID).unwrap();
        prop_assert!(recovery.damaged.is_empty());
        check(&log, &models);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// An update of any size, in parts: each part fits a frame, the hard state comes with or
    /// after the last entries, and written in order the parts leave the group as the whole
    /// update would (audit S04).
    #[test]
    fn an_update_in_parts_leaves_the_group_as_the_whole_would(
        held in 0u64..4,
        sizes in prop::collection::vec(0usize..6_000, 0..40),
        with_state in any::<bool>(),
        proposals in prop::collection::vec(0usize..3_000, 0..4),
        segment_blocks in 4u64..8,
        seed in any::<u64>(),
    ) {
        let file = sim(seed);
        let cfg = config(segment_blocks, 64);
        let log = Log::create(Arc::clone(&file), cfg, ID).unwrap();
        let mut models = Models::new();
        if held > 0 {
            let u = Update {
                entries: Some(entries(1, &vec![1; held as usize])),
                ..Update::default()
            };
            log.write(1, u.clone()).unwrap();
            apply(&mut models, 1, &u);
        }
        let first = held.saturating_sub(1).max(1);
        let last = first + sizes.len() as u64;
        let update = Update {
            entries: Some(Entries {
                first,
                entries: sizes
                    .iter()
                    .map(|&n| Entry {
                        term: 2,
                        bytes: Arc::from(vec![b'e'; n]),
                    })
                    .collect(),
            }),
            hard_state: with_state.then(|| hard(2, first)),
            proposals: proposals
                .iter()
                .enumerate()
                .map(|(i, &n)| Proposal {
                    index: last + 1 + i as u64,
                    term: 2,
                    bytes: Arc::from(vec![b'p'; n]),
                })
                .collect(),
            ..Update::default()
        };
        let parts = log.parts(1, update.clone()).unwrap();
        let with_hard = parts.iter().position(|p| p.hard_state.is_some());
        let last_entries = parts.iter().rposition(|p| p.entries.is_some());
        if let (Some(h), Some(e)) = (with_hard, last_entries) {
            prop_assert!(h >= e, "the hard state came before entries");
        }
        prop_assert_eq!(
            parts.iter().filter(|p| p.hard_state.is_some()).count(),
            usize::from(with_state)
        );
        for part in &parts {
            // A part that did not fit a frame would be refused `TooLarge`.
            log.write(1, part.clone()).unwrap();
        }
        apply(&mut models, 1, &update);
        check(&log, &models);
        drop(log);
        let (log, _) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
        check(&log, &models);
    }
}
