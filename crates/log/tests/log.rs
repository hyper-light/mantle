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

use mantle_disk::block::BlockFile;
use mantle_disk::buf::{AlignedBuf, Alignment};
use mantle_disk::sim::{Crash, Fault, SimFile};
use mantle_log::{
    Class, Config, Entries, Entry, HardState, Log, LogError, Proposal, Start, Update, View, Waits,
};
use proptest::prelude::*;

mod common;
use common::{Released, Stepped};

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
        waits: Waits::Measured,
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
    // The group stays damaged across restarts, and another group's writes between them,
    // until it is removed (audit S16).
    for reopening in 0..3 {
        let (log, recovery) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
        assert_eq!(recovery.damaged, vec![1], "reopening {reopening}");
        if reopening == 0 {
            assert_eq!(recovery.restored, vec![2]);
        }
        assert!(matches!(log.view(1), Err(LogError::Damaged(_))));
        assert!(matches!(
            log.write(1, Update::default()),
            Err(LogError::Damaged(_))
        ));
        let view = log.view(2).unwrap().unwrap();
        assert_eq!(view.hard_state, Some(voted));
        if reopening == 1 {
            log.write(
                2,
                Update {
                    entries: Some(entries(2, &[3])),
                    ..Update::default()
                },
            )
            .unwrap();
        }
    }
    let (log, _) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
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

/// A log whose last frame, acknowledged, held group 1's proposal and no longer reads: group
/// 1 is damaged when the log next opens.
fn damaged_group_one(seed: u64, cfg: Config) -> Arc<SimFile> {
    let file = sim(seed);
    let log = Log::create(Arc::clone(&file), cfg, ID).unwrap();
    log.write(
        1,
        Update {
            entries: Some(entries(1, &[1])),
            ..Update::default()
        },
    )
    .unwrap();
    log.write(
        1,
        Update {
            hard_state: Some(HardState {
                term: 2,
                vote: 1,
                commit: 0,
            }),
            proposals: vec![Proposal {
                index: 2,
                term: 2,
                bytes: Arc::from(&b"p"[..]),
            }],
            ..Update::default()
        },
    )
    .unwrap();
    drop(log);
    // The damage is written to the medium, so that it outlasts a test's clearing of faults.
    let image = file.durable_image().unwrap();
    let last = valid_frames(&image)
        .into_iter()
        .max_by_key(|f| f.2)
        .unwrap();
    let block = last.0 / BLOCK as u64 * BLOCK as u64;
    let mut buf = AlignedBuf::zeroed(BLOCK, Alignment::new(BLOCK).unwrap()).unwrap();
    buf.set_len(BLOCK).unwrap();
    file.read_exact_at(buf.as_mut_slice(), block).unwrap();
    buf.as_mut_slice()[(last.0 - block + 70) as usize] ^= 1 << 4;
    file.write_all_at(buf.as_slice(), block).unwrap();
    file.sync_data().unwrap();
    file
}

/// The fence on a damaged group is a live piece of the log: reclaiming the segment it was
/// written in copies it, and it holds across reopening (audit S16).
#[test]
fn a_damaged_groups_fence_survives_reclaiming_its_segment() {
    let cfg = config(8, 6);
    let file = damaged_group_one(46, cfg);
    let (log, recovery) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
    assert_eq!(recovery.damaged, vec![1]);
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
    assert!(matches!(log.view(1), Err(LogError::Damaged(_))));
    drop(log);
    let (log, recovery) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
    assert_eq!(recovery.damaged, vec![1]);
    assert!(matches!(log.view(1), Err(LogError::Damaged(_))));
}

/// Power fails at each write and flush of the open that finds a group damaged and fences
/// it: the open after finds the group damaged still, whatever of the first reached the disk.
#[test]
fn power_lost_while_fencing_a_damaged_group_leaves_it_damaged() {
    let cfg = config(16, 8);
    for ops in 0..8 {
        for seed in 0..4 {
            let file = damaged_group_one(47 + seed, cfg);
            file.inject(Fault::PowerCut { ops }).unwrap();
            drop(Log::open(Arc::clone(&file), cfg, ID));
            file.crash(Crash::Random).unwrap();
            file.clear_faults().unwrap();
            let (log, recovery) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
            assert_eq!(
                recovery.damaged,
                vec![1],
                "power cut after {ops} operations"
            );
            assert!(matches!(log.view(1), Err(LogError::Damaged(_))));
            drop(log);
            let (_, recovery) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
            assert_eq!(recovery.damaged, vec![1], "reopened after {ops} operations");
        }
    }
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

/// An update is answered only once a later durable record confirms its frame's flush
/// (audit S01, round three). Power fails after the frame, its persist record and its flush
/// reach the device, before the confirmation does, and the frame is damaged at rest: the
/// update was never acknowledged, so the frame is the torn tail, exactly. The same frame
/// confirmed and then damaged was acknowledged, and recovery restores it and marks the
/// entry it lost.
#[test]
fn an_update_is_answered_only_once_its_frame_is_confirmed() {
    for confirmed in [false, true] {
        let file = sim(45);
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
        let voted = HardState {
            term: 2,
            vote: 2,
            commit: 1,
        };
        let second = Update {
            entries: Some(entries(2, &[2])),
            hard_state: Some(voted),
            ..Update::default()
        };
        if confirmed {
            log.write(1, second).unwrap();
            file.crash(Crash::LoseAll).unwrap();
        } else {
            // The frame's write, its persist record's and the flush succeed; the confirmation's
            // write does not.
            file.inject(Fault::PowerCut { ops: 3 }).unwrap();
            assert!(matches!(log.write(1, second), Err(LogError::Fenced)));
            file.crash(Crash::KeepAll).unwrap();
        }
        drop(log);
        file.clear_faults().unwrap();
        let image = file.durable_image().unwrap();
        let last = valid_frames(&image)
            .into_iter()
            .max_by_key(|f| f.2)
            .unwrap();
        assert_eq!(last.2, 2, "the second frame was flushed");
        file.inject(Fault::BitFlip {
            offset: last.0 + 70,
            bit: 0,
            stored: true,
        })
        .unwrap();
        let (log, recovery) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
        assert_eq!((recovery.restored, recovery.damaged), (vec![1], vec![]));
        let view = log.view(1).unwrap().unwrap();
        assert_eq!(view.last, 1, "confirmed {confirmed}");
        assert_eq!(view.hard_state.map(|h| (h.term, h.vote)), Some((2, 2)));
        let mark = if confirmed {
            Some(Start { index: 2, term: 2 })
        } else {
            None
        };
        assert_eq!(view.uncertain, mark, "confirmed {confirmed}");
    }
}

/// A frame torn by a crash before its persist record reached the disk was never
/// acknowledged: the log is cut before it, and nothing is restored or marked. The frame's
/// write reaches the disk and its record's write fails, and the frame then tears, so recovery
/// finds a frame that no longer reads with no record of it.
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
    file.inject(Fault::PowerCut { ops: 1 }).unwrap();
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
    file.crash(Crash::KeepAll).unwrap();
    file.clear_faults().unwrap();
    let image = file.durable_image().unwrap();
    let torn = *valid_frames(&image)
        .iter()
        .find(|f| f.2 == 2)
        .expect("the frame's write reached the disk");
    // The frame of sequence 2 keeps its record in the first persist slot.
    assert!(
        mantle_log::format::Persist::decode(&image).is_none_or(|p| p.sequence != 2),
        "the frame's persist record reached the disk"
    );
    damage(&file, torn.0 + 70);
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

/// A simulated file that counts its reads and the looks at its length.
struct Counting {
    file: Arc<SimFile>,
    reads: std::sync::atomic::AtomicU64,
    lengths: std::sync::atomic::AtomicU64,
}

impl Counting {
    fn new(file: Arc<SimFile>) -> Arc<Self> {
        Arc::new(Self {
            file,
            reads: std::sync::atomic::AtomicU64::new(0),
            lengths: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Reads and looks at the length since the last call.
    fn take(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering::SeqCst;
        (self.reads.swap(0, SeqCst), self.lengths.swap(0, SeqCst))
    }
}

impl mantle_disk::block::BlockFile for Counting {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }

    fn len(&self) -> Result<u64, mantle_disk::DiskError> {
        self.lengths
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        mantle_disk::block::BlockFile::len(&*self.file)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), mantle_disk::DiskError> {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.file.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), mantle_disk::DiskError> {
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), mantle_disk::DiskError> {
        self.file.sync_data()
    }
}

/// Opening a log reads each segment it walks through a window of the segment, where it read
/// each frame's first block and then the frame, and looked at the file's length for each
/// (audit P07): an open replaying segments of one-block frames takes a few reads a segment
/// and one look at the length, and finds every update.
#[test]
fn opening_reads_each_segment_through_a_window() {
    let counting = Counting::new(sim(41));
    let settings = config(16, 8);
    let mut models = Models::new();
    let log = Log::create(Arc::clone(&counting), settings, ID).unwrap();
    for n in 1..=120u64 {
        let u = Update {
            entries: Some(entries(n, &[1])),
            ..Update::default()
        };
        apply(&mut models, 1, &u);
        log.write(1, u).unwrap();
    }
    drop(log);
    let len = mantle_disk::block::BlockFile::len(&*counting.file).unwrap();
    let segments = (len - AREA).div_ceil(settings.segment_bytes);
    counting.take();
    let (log, recovery) = Log::open(Arc::clone(&counting), settings, ID).unwrap();
    let (reads, lengths) = counting.take();
    // The writer reclaimed the oldest segments as it went; three at least hold one-block
    // frames that the open replays, a block after each segment's header.
    let per_segment = settings.segment_bytes / BLOCK as u64 - 1;
    assert!(
        recovery.frames >= 3 * per_segment,
        "{} frames",
        recovery.frames
    );
    assert!(segments >= 4, "{segments} segments");
    // Each segment's header, each segment walked twice (for the last frame, then in the
    // replay) and the two persist slots.
    assert!(
        reads <= 3 * segments + 2,
        "{reads} reads to open {segments} segments of {} frames",
        recovery.frames
    );
    assert!(lengths <= 2, "{lengths} looks at the length");
    check(&log, &models);
}

/// Entries no longer in memory are read from the file, and a run of them that lies together
/// in one read, each still verified as its group's entry of its term (audit P07).
#[test]
fn entries_not_in_memory_that_lie_together_are_read_at_once() {
    let counting = Counting::new(sim(42));
    let settings = Config {
        group_cache: 0,
        ..config(16, 8)
    };
    let log = Log::create(Arc::clone(&counting), settings, ID).unwrap();
    let terms = vec![1u64; 200];
    log.write(
        1,
        Update {
            entries: Some(entries(1, &terms)),
            ..Update::default()
        },
    )
    .unwrap();
    counting.take();
    let got = log.entries(1, 1, 201, u64::MAX).unwrap();
    let (reads, _) = counting.take();
    assert_eq!(got, entries(1, &terms).entries);
    assert_eq!(reads, 1, "{reads} reads for 200 entries written together");
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

/// An update of one entry whose records take `len` payload bytes.
fn sized(first: u64, len: usize) -> Update {
    let header = mantle_log::format::encoded_len(&mantle_log::format::Record::Entries {
        group: 0,
        first,
        entries: &[(1, &[])],
    })
    .unwrap();
    Update {
        entries: Some(Entries {
            first,
            entries: vec![Entry {
                term: 1,
                bytes: Arc::from(vec![b'x'; len - header]),
            }],
        }),
        ..Update::default()
    }
}

/// What a submission of `len` payload bytes holds of the queue's byte bound: its records and
/// its row in the frame's persist record.
fn charged(len: usize) -> u64 {
    (len + mantle_log::format::PERSIST_GROUP_LEN) as u64
}

/// The queue's byte bound is three frames of the largest charge, and holds every submission
/// not yet answered by the bytes its records take: one group's flood of frame-sized updates is
/// refused at its own two, another group still gets in, and the bound refuses past three
/// though the count bound has room (audit S03).
#[test]
fn the_queue_bounds_the_bytes_of_every_submission_not_yet_answered() {
    let stepped = Stepped::new(sim(43));
    let cfg = config_now(16, 64);
    let log = Log::create(Arc::clone(&stepped), cfg, ID).unwrap();
    let room = log.frame_room().unwrap();
    assert_eq!(log.queue_bytes(), 3 * charged(room));
    let full = |first| sized(first, room);
    stepped.hold();
    let _released = Released(Arc::clone(&stepped));
    let mut pending = vec![log.submit(1, full(1)).unwrap()];
    stepped.held();
    // One group floods: its second is taken, and its third refused at its own bound.
    pending.push(log.submit(1, full(2)).unwrap());
    assert!(matches!(log.submit(1, full(3)), Err(LogError::Busy)));
    // Another group still gets in, and the three fill the byte bound: a fourth is refused
    // with most of the count bound free.
    pending.push(log.submit(2, full(1)).unwrap());
    assert!(matches!(log.submit(3, full(1)), Err(LogError::Busy)));
    // A submission more than a frame holds is refused before it holds any room.
    assert!(matches!(
        log.submit(3, sized(1, room + 1)),
        Err(LogError::TooLarge(len)) if len == room + 1
    ));
    stepped.release();
    for p in pending {
        p.wait().unwrap();
    }
    assert_eq!(log.view(1).unwrap().unwrap().last, 2);
    assert_eq!(log.view(2).unwrap().unwrap().last, 1);
    // Answered, they give their room back: every group gets in again.
    log.write(3, full(1)).unwrap();
}

/// Tiny and empty updates are charged the bytes their records and persist rows take, so a
/// flood of them from many groups fills the byte bound as large ones do (audit S03).
#[test]
fn empty_entries_are_charged_their_records() {
    let stepped = Stepped::new(sim(44));
    let mut cfg = config_now(16, 64);
    cfg.queue_submissions = 4096;
    let log = Log::create(Arc::clone(&stepped), cfg, ID).unwrap();
    let empty = Update {
        entries: Some(Entries {
            first: 1,
            entries: vec![Entry {
                term: 1,
                bytes: Arc::from(Vec::new()),
            }],
        }),
        ..Update::default()
    };
    let len = mantle_log::format::encoded_len(&mantle_log::format::Record::Entries {
        group: 0,
        first: 1,
        entries: &[(1, &[])],
    })
    .unwrap();
    let cost = charged(len);
    let nothing = charged(0);
    let fit = log.queue_bytes() / cost;
    assert!(fit < cfg.queue_submissions as u64);
    stepped.hold();
    let _released = Released(Arc::clone(&stepped));
    let mut pending = vec![log.submit(0, empty.clone()).unwrap()];
    stepped.held();
    let mut group = 1u128;
    while let Ok(p) = log.submit(group, empty.clone()) {
        pending.push(p);
        group += 1;
    }
    assert_eq!(pending.len() as u64, fit, "admitted past the byte bound");
    assert!(matches!(
        log.submit(group, empty.clone()),
        Err(LogError::Busy)
    ));
    // An update of no records still costs its persist row: as many as the rest admits.
    let left = log.queue_bytes() - fit * cost;
    let mut more = 0;
    while let Ok(p) = log.submit(group, Update::default()) {
        pending.push(p);
        group += 1;
        more += 1;
    }
    assert_eq!(more, left / nothing);
    stepped.release();
    // Groups past the log's bound are refused once the writer takes them.
    for p in pending {
        assert!(matches!(p.wait(), Ok(()) | Err(LogError::TooManyGroups(_))));
    }
}

/// A frame takes first the updates the frame before passed over for room, ahead of those held
/// only because their group had one in it, so a passed-over update is written in the next
/// frame (docs/design/raft-log.md §3).
#[test]
fn an_update_passed_over_for_room_is_written_in_the_next_frame() {
    let stepped = Stepped::new(sim(45));
    let cfg = config_now(16, 64);
    let log = Log::create(Arc::clone(&stepped), cfg, ID).unwrap();
    let room = log.frame_room().unwrap();
    let (hot, cold) = (room * 45 / 100, room * 60 / 100);
    stepped.hold();
    let _released = Released(Arc::clone(&stepped));
    let mut pending = vec![log.submit(9, Update::default()).unwrap()];
    stepped.held();
    // The next frame takes 1's first, holds 1's second behind it, and passes 2 over.
    pending.push(log.submit(1, sized(1, hot)).unwrap());
    pending.push(log.submit(1, sized(2, hot)).unwrap());
    pending.push(log.submit(2, sized(1, cold)).unwrap());
    stepped.step();
    stepped.step();
    assert_eq!(log.view(1).unwrap().unwrap().last, 1);
    assert!(log.view(2).unwrap().is_none());
    stepped.step();
    assert!(log.view(2).unwrap().is_some(), "passed over twice");
    assert_eq!(log.view(1).unwrap().unwrap().last, 1);
    stepped.release();
    for p in pending {
        p.wait().unwrap();
    }
    assert_eq!(log.view(1).unwrap().unwrap().last, 2);
}

/// Hot groups keep every frame full, each with an update arriving for every frame; a cold
/// group's update passed over for room is still written in the next frame, and the hot ones
/// keep going (audit S03).
#[test]
fn hot_groups_do_not_starve_a_cold_one() {
    hot_beside_cold(46, Class::Normal, Class::Normal);
}

/// The same when the hot groups are latency-sensitive and the cold update background work:
/// passed over once, it goes before every class (docs/design/raft-log.md §3).
#[test]
fn latency_traffic_does_not_starve_background_work() {
    hot_beside_cold(50, Class::Latency, Class::Background);
}

fn hot_beside_cold(seed: u64, hot_class: Class, cold_class: Class) {
    let stepped = Stepped::new(sim(seed));
    let cfg = config_now(16, 64);
    let log = Log::create(Arc::clone(&stepped), cfg, ID).unwrap();
    let room = log.frame_room().unwrap();
    let (hot, cold) = (room * 55 / 100, room * 60 / 100);
    stepped.hold();
    let _released = Released(Arc::clone(&stepped));
    let mut pending = vec![log.submit(9, Update::default()).unwrap()];
    stepped.held();
    // A group has one update in a frame at most, so two stand in for a range whose next
    // update arrives for every frame.
    let mut next = [1u64, 1u64];
    let mut feed =
        |log: &Log<Arc<Stepped>>, frame: usize, pending: &mut Vec<mantle_log::Pending>| {
            let g = frame % 2;
            let update = sized(next[g], hot);
            pending.push(log.submit_in(g as u128 + 1, hot_class, update).unwrap());
            next[g] += 1;
        };
    feed(&log, 0, &mut pending);
    pending.push(log.submit_in(3, cold_class, sized(1, cold)).unwrap());
    let before = log.flushed().0;
    let mut written = None;
    for frame in 1..=12 {
        stepped.step();
        if written.is_none() && log.view(3).unwrap().is_some() {
            written = Some(log.flushed().0 - before);
        }
        feed(&log, frame, &mut pending);
    }
    // The held frame, the frame that passed the cold update over, and the next.
    assert_eq!(written, Some(3), "a cold update waited behind hot ones");
    stepped.release();
    for p in pending {
        p.wait().unwrap();
    }
    let (a, b) = (
        log.view(1).unwrap().unwrap().last,
        log.view(2).unwrap().unwrap().last,
    );
    assert_eq!((a, b), (next[0] - 1, next[1] - 1));
}

/// A frame with room for only some of the waiting updates takes them by class, and those it
/// passes over by class again in the next: latency before normal before background, whatever
/// the order they came in (docs/design/raft-log.md §3).
#[test]
fn a_full_frame_takes_updates_by_class() {
    let stepped = Stepped::new(sim(47));
    let log = Log::create(Arc::clone(&stepped), config_now(16, 64), ID).unwrap();
    let big = log.frame_room().unwrap() * 60 / 100;
    stepped.hold();
    let _released = Released(Arc::clone(&stepped));
    let mut pending = vec![log.submit(9, Update::default()).unwrap()];
    stepped.held();
    pending.push(log.submit_in(3, Class::Background, sized(1, big)).unwrap());
    pending.push(log.submit_in(2, Class::Normal, sized(1, big)).unwrap());
    pending.push(log.submit_in(1, Class::Latency, sized(1, big)).unwrap());
    let written = |log: &Log<Arc<Stepped>>| -> Vec<bool> {
        (1..=3).map(|g| log.view(g).unwrap().is_some()).collect()
    };
    stepped.step();
    stepped.step();
    assert_eq!(written(&log), [true, false, false]);
    stepped.step();
    assert_eq!(written(&log), [true, true, false]);
    stepped.step();
    assert_eq!(written(&log), [true, true, true]);
    stepped.release();
    for p in pending {
        p.wait().unwrap();
    }
}

/// A group's update of a more urgent class never goes before one of its own submitted
/// earlier: it takes the earlier one's place in the order (docs/design/raft-log.md §3).
#[test]
fn a_groups_urgent_update_waits_behind_its_own_earlier_one() {
    let stepped = Stepped::new(sim(48));
    let log = Log::create(Arc::clone(&stepped), config_now(16, 64), ID).unwrap();
    let big = log.frame_room().unwrap() * 60 / 100;
    stepped.hold();
    let _released = Released(Arc::clone(&stepped));
    let first = log.submit(9, Update::default()).unwrap();
    stepped.held();
    let older = log.submit_in(1, Class::Background, sized(1, big)).unwrap();
    let newer = log
        .submit_in(
            1,
            Class::Latency,
            Update {
                entries: Some(entries(2, &[1])),
                ..Update::default()
            },
        )
        .unwrap();
    stepped.release();
    for p in [first, older, newer] {
        p.wait().unwrap();
    }
    assert_eq!(log.view(1).unwrap().unwrap().last, 2);
}

/// Groups of one class share a full frame by start-time fair queueing in bytes: a group that
/// just had a large update written waits behind a group taken after it that has had none
/// (docs/design/raft-log.md §3).
#[test]
fn a_group_just_served_waits_behind_one_that_was_not() {
    let stepped = Stepped::new(sim(49));
    let log = Log::create(Arc::clone(&stepped), config_now(16, 64), ID).unwrap();
    let room = log.frame_room().unwrap();
    let (hot, cold) = (room * 60 / 100, room * 50 / 100);
    stepped.hold();
    let _released = Released(Arc::clone(&stepped));
    let mut pending = vec![log.submit(9, Update::default()).unwrap()];
    stepped.held();
    pending.push(log.submit(1, sized(1, hot)).unwrap());
    pending.push(log.submit(1, sized(2, hot)).unwrap());
    stepped.step();
    // The frame now flushing holds group 1's first; its second waits for the next.
    pending.push(log.submit(2, sized(1, cold)).unwrap());
    stepped.step();
    assert_eq!(log.view(1).unwrap().unwrap().last, 1);
    stepped.step();
    assert!(log.view(2).unwrap().is_some(), "taken after, written after");
    assert_eq!(log.view(1).unwrap().unwrap().last, 1);
    stepped.release();
    for p in pending {
        p.wait().unwrap();
    }
    assert_eq!(log.view(1).unwrap().unwrap().last, 2);
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
    // A segment one block past a buffer's bound, refused before the file is touched.
    let past = Config {
        segment_bytes: mantle_disk::buf::MAX_BUFFER as u64 + BLOCK as u64,
        ..config(16, 8)
    };
    let untouched = sim(11);
    assert!(matches!(
        Log::create(Arc::clone(&untouched), past, ID),
        Err(LogError::Config(_))
    ));
    assert!(untouched.durable_image().unwrap().is_empty());
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
                    // At the queue's bound, or the group's, or more than a frame holds: refused
                    // whole, changing nothing.
                    Err(LogError::Busy | LogError::TooLarge(_)) => continue,
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

/// The settings of `config` with a writer that forms each batch from what is queued, so a
/// test that holds the writer knows which submissions share a frame.
fn config_now(segment_blocks: u64, max_segments: u32) -> Config {
    Config {
        waits: Waits::Never,
        ..config(segment_blocks, max_segments)
    }
}

/// Flips a bit of the byte at `offset` on the medium itself, so that the damage outlasts a
/// test's clearing of faults.
fn damage(file: &SimFile, offset: u64) {
    let block = offset / BLOCK as u64 * BLOCK as u64;
    let mut buf = AlignedBuf::zeroed(BLOCK, Alignment::new(BLOCK).unwrap()).unwrap();
    buf.set_len(BLOCK).unwrap();
    file.read_exact_at(buf.as_mut_slice(), block).unwrap();
    buf.as_mut_slice()[(offset - block) as usize] ^= 1;
    file.write_all_at(buf.as_slice(), block).unwrap();
    file.sync_data().unwrap();
}

#[derive(Default)]
struct Gates {
    /// Flushes let through before the next is held; `None` lets every one through.
    permits: Option<u64>,
    /// Flushes held now.
    held: u64,
    /// Reads overlapping this range are held until released.
    trap: Option<(u64, u64)>,
    trapped: u64,
}

/// A simulated file that lets its flushes through a counted number at a time and holds reads
/// of a range, so a test can stop the writer at a chosen flush or read.
struct Gate {
    file: Arc<SimFile>,
    state: std::sync::Mutex<Gates>,
    changed: std::sync::Condvar,
}

impl Gate {
    fn new(file: Arc<SimFile>) -> Arc<Self> {
        Arc::new(Self {
            file,
            state: std::sync::Mutex::new(Gates::default()),
            changed: std::sync::Condvar::new(),
        })
    }

    fn permits(&self, permits: Option<u64>) {
        self.state.lock().unwrap().permits = permits;
        self.changed.notify_all();
    }

    /// Waits until a flush is held.
    fn flush_held(&self) {
        let mut s = self.state.lock().unwrap();
        while s.held == 0 {
            s = self.changed.wait(s).unwrap();
        }
    }

    fn trap(&self, from: u64, len: u64) {
        self.state.lock().unwrap().trap = Some((from, from + len));
    }

    /// Waits until a read is held.
    fn read_held(&self) {
        let mut s = self.state.lock().unwrap();
        while s.trapped == 0 {
            s = self.changed.wait(s).unwrap();
        }
    }

    fn release(&self) {
        self.state.lock().unwrap().trap = None;
        self.changed.notify_all();
    }
}

impl mantle_disk::block::BlockFile for Gate {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }

    fn len(&self) -> Result<u64, mantle_disk::DiskError> {
        mantle_disk::block::BlockFile::len(&*self.file)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), mantle_disk::DiskError> {
        let mut s = self.state.lock().unwrap();
        let end = offset + buf.len() as u64;
        if s.trap.is_some_and(|(a, b)| offset < b && a < end) {
            s.trapped += 1;
            self.changed.notify_all();
            while s.trap.is_some() {
                s = self.changed.wait(s).unwrap();
            }
        }
        drop(s);
        self.file.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), mantle_disk::DiskError> {
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), mantle_disk::DiskError> {
        let mut s = self.state.lock().unwrap();
        if s.permits == Some(0) {
            s.held += 1;
            self.changed.notify_all();
            while s.permits == Some(0) {
                s = self.changed.wait(s).unwrap();
            }
            s.held -= 1;
        }
        if let Some(p) = s.permits.as_mut() {
            *p -= 1;
        }
        drop(s);
        self.file.sync_data()
    }
}

/// A frame confirmed on its own is answered, and the confirmation must outlast the next
/// frame's persist record, which is written before that frame's flush and may tear: a record
/// of five groups or more spans two sectors. Power fails at the next frame's flush, the
/// confirmed frame is damaged at rest, and recovery must still know it was acknowledged: its
/// entry is held or marked, never dropped as a torn tail.
#[test]
fn a_confirmation_outlives_a_torn_record_of_the_next_frame() {
    let (mut reopened, mut torn) = (0, 0u32);
    // The second persist slot: a record of the log's most groups, padded to the block.
    let slot = mantle_log::format::persist_len(64)
        .unwrap()
        .next_multiple_of(BLOCK);
    for seed in 0..64u64 {
        let file = sim(seed);
        let gate = Gate::new(Arc::clone(&file));
        let cfg = config_now(16, 8);
        let log = Log::create(Arc::clone(&gate), cfg, ID).unwrap();
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
        // The second frame's flush passes; the flush of its confirmation is held.
        gate.permits(Some(1));
        let second = log
            .submit(
                1,
                Update {
                    entries: Some(entries(2, &[1])),
                    ..Update::default()
                },
            )
            .unwrap();
        gate.flush_held();
        let others: Vec<_> = (10..16u128)
            .map(|g| {
                log.submit(
                    g,
                    Update {
                        hard_state: Some(HardState {
                            term: 1,
                            vote: 0,
                            commit: 0,
                        }),
                        ..Update::default()
                    },
                )
                .unwrap()
            })
            .collect();
        // The confirmation's flush succeeds; the third frame's writes, its frame and its
        // record, reach the device, and its flush fails.
        file.inject(Fault::PowerCut { ops: 3 }).unwrap();
        gate.permits(None);
        second.wait().unwrap();
        for o in others {
            assert!(matches!(o.wait(), Err(LogError::Fenced)));
        }
        drop(log);
        file.crash(Crash::Random).unwrap();
        file.clear_faults().unwrap();
        let image = file.durable_image().unwrap();
        let frames = valid_frames(&image);
        // A third frame that survived whole proves the second was flushed: damage to it is
        // reported, which other tests cover.
        if frames.iter().any(|f| f.2 == 3) {
            continue;
        }
        // The third frame's record, in the second persist slot, where a confirmation written
        // into the next frame's slot would have been: torn means it no longer reads at all.
        let record = mantle_log::format::Persist::decode(&image[slot..]);
        let tore = record.is_none();
        let two = frames.iter().find(|f| f.2 == 2).unwrap();
        damage(&file, two.0 + 70);
        let (log, recovery) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
        let at = format!("seed {seed}, record {record:?}");
        // The second frame was answered, so it is restored: its entry cut and marked, the
        // hard state the first frame left, and nothing of the third frame, never answered.
        assert_eq!(
            (recovery.restored, recovery.damaged),
            (vec![1], vec![]),
            "{at}"
        );
        let view = log.view(1).unwrap().unwrap();
        assert_eq!(
            (view.last, view.uncertain, view.hard_state),
            (
                1,
                Some(Start { index: 2, term: 1 }),
                Some(HardState {
                    term: 1,
                    vote: 1,
                    commit: 1,
                })
            ),
            "{at}"
        );
        for g in 10..16u128 {
            assert!(log.view(g).unwrap().is_none(), "{at}: group {g}");
        }
        reopened += 1;
        torn += u32::from(tore);
    }
    assert!(
        torn > 0,
        "no seed tore the third frame's record ({reopened} lost the frame)"
    );
}

/// A commit that fails before its frame is written, here as its sweep of the tail reads the
/// tail, fences the log and answers every update it held: the last frame's, which no
/// confirmation will follow, and the batch's, whose room in the queue is given back.
#[test]
fn a_commit_that_fails_before_its_frame_answers_every_update() {
    let file = sim(7);
    let gate = Gate::new(Arc::clone(&file));
    let cfg = config_now(4, 3);
    let log = Log::create(Arc::clone(&gate), cfg, ID).unwrap();
    let state = |term| Update {
        hard_state: Some(HardState {
            term,
            vote: 1,
            commit: 0,
        }),
        ..Update::default()
    };
    // Group 2's entry stays live; group 1's hard states fill segment 0.
    log.write(
        2,
        Update {
            entries: Some(entries(1, &[1])),
            ..Update::default()
        },
    )
    .unwrap();
    log.write(1, state(1)).unwrap();
    // The next frame opens segment 1 and is held in its flush while another update queues.
    gate.permits(Some(0));
    let a = log.submit(1, state(2)).unwrap();
    gate.flush_held();
    let b = log.submit(1, state(3)).unwrap();
    // The commit after it sweeps segment 0, whose read is held, and fails.
    let segment_zero = cfg.segment_bytes;
    gate.trap(segment_zero, cfg.segment_bytes);
    gate.permits(None);
    gate.read_held();
    let d = log.submit(4, state(1)).unwrap();
    file.inject(Fault::ReadError {
        offset: segment_zero,
        len: cfg.segment_bytes,
    })
    .unwrap();
    gate.release();
    // Answered after the failed commit, by which time every answer it gives has gone out.
    assert!(matches!(d.wait(), Err(LogError::Fenced)));
    assert!(log.is_fenced());
    assert!(
        matches!(b.poll(), Some(Err(LogError::Fenced))),
        "{:?}",
        b.poll()
    );
    assert!(
        matches!(a.poll(), Some(Err(LogError::Fenced))),
        "{:?}",
        a.poll()
    );
}

/// Power fails at each write and flush of the open that restores an acknowledged last frame
/// that no longer reads. The restore's own frame and persist record may tear, and must not
/// take with them what recovery needs to restore the lost frame: whatever reached the disk,
/// the next open holds each group's acknowledged vote, marks its lost entry, and keeps the
/// group whose frame held a proposal damaged.
#[test]
fn power_lost_while_restoring_a_lost_frame_loses_nothing_acknowledged() {
    let cfg = config_now(16, 8);
    let groups = 10..16u128;
    for ops in 0..8 {
        for seed in 0..24u64 {
            let file = sim(1000 + seed);
            let gate = Gate::new(Arc::clone(&file));
            let log = Log::create(Arc::clone(&gate), cfg, ID).unwrap();
            for g in groups.clone() {
                let voted = HardState {
                    term: 1,
                    vote: 0,
                    commit: 0,
                };
                log.write(
                    g,
                    Update {
                        hard_state: Some(voted),
                        ..Update::default()
                    },
                )
                .unwrap();
            }
            // One frame, held in its flush, while the six groups queue: the next frame carries
            // all six, so its persist record spans sectors and can tear.
            gate.permits(Some(0));
            let plug = log
                .submit(
                    1,
                    Update {
                        hard_state: Some(hard(1, 0)),
                        ..Update::default()
                    },
                )
                .unwrap();
            gate.flush_held();
            let pending: Vec<_> = groups
                .clone()
                .map(|g| {
                    let proposals = if g == 15 {
                        vec![Proposal {
                            index: 5,
                            term: 2,
                            bytes: Arc::from(&b"p"[..]),
                        }]
                    } else {
                        Vec::new()
                    };
                    let u = Update {
                        entries: Some(entries(1, &[2])),
                        hard_state: Some(HardState {
                            term: 2,
                            vote: 2,
                            commit: 0,
                        }),
                        proposals,
                        ..Update::default()
                    };
                    log.submit(g, u).unwrap()
                })
                .collect();
            gate.permits(None);
            plug.wait().unwrap();
            for p in pending {
                p.wait().unwrap();
            }
            drop(log);
            let frames = valid_frames(&file.durable_image().unwrap());
            let last = frames.iter().max_by_key(|f| f.2).unwrap();
            assert_eq!(last.2, 8, "the six groups share the last frame");
            damage(&file, last.0 + 70);
            file.inject(Fault::PowerCut { ops }).unwrap();
            drop(Log::open(Arc::clone(&file), cfg, ID));
            file.crash(Crash::Random).unwrap();
            file.clear_faults().unwrap();
            let (log, recovery) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
            let at = format!("power cut after {ops} operations, seed {seed}");
            assert_eq!(recovery.damaged, vec![15], "{at}: {recovery:?}");
            for g in 10..15u128 {
                let view = log.view(g).unwrap().unwrap();
                assert_eq!(
                    view.hard_state.map(|h| (h.term, h.vote)),
                    Some((2, 2)),
                    "{at}: group {g}"
                );
                assert!(
                    view.last >= 1 || view.uncertain == Some(Start { index: 1, term: 2 }),
                    "{at}: group {g} lost its entry unmarked: {view:?}"
                );
            }
        }
    }
}

/// The tail of each valid frame in the durable image, by sequence.
fn frame_tails(image: &[u8]) -> BTreeMap<u64, (u64, u64)> {
    valid_frames(image)
        .into_iter()
        .map(|(at, _, seq)| {
            let h = mantle_log::format::FrameHeader::decode(&image[at as usize..]).unwrap();
            (seq, (at, h.tail))
        })
        .collect()
}

/// A frame that swept the tail, freeing its segment, is the last frame, filling its own
/// segment, and is confirmed. The next frame opens a segment in the freed slot, and power
/// fails as it is written, so any of its sectors may have reached the disk, over the freed
/// segment's header and first frames. Then the swept frame is damaged at rest. Recovery falls
/// back to the frame before it, whose tail is the freed segment: the pieces the sweep moved
/// are in that segment and in the lost frame, and nowhere else. Where the torn opening wrote
/// over the segment, they are lost, and the log must say so; where it did not, the log opens
/// with every acknowledged piece. It never opens without them and without a word.
#[test]
fn a_torn_opening_over_a_segment_swept_by_a_lost_frame_loses_nothing_unreported() {
    let cfg = config_now(4, 4);
    let segment = cfg.segment_bytes;
    let slot0 = segment;
    let (mut unframed, mut refused, mut intact, mut clobbered_open) = (0, 0, 0, 0);
    for ops in 1..=2u64 {
        for seed in 0..200u64 {
            let file = sim(5000 + seed);
            let log = Log::create(Arc::clone(&file), cfg, ID).unwrap();
            // Group 2's entry stays live in segment 0; group 1's hard states die as they go.
            log.write(
                2,
                Update {
                    entries: Some(entries(1, &[1])),
                    ..Update::default()
                },
            )
            .unwrap();
            for term in 1..=5 {
                log.write(
                    1,
                    Update {
                        hard_state: Some(hard(term, 0)),
                        ..Update::default()
                    },
                )
                .unwrap();
            }
            // Three segments hold frames and one may still be added: the next frame sweeps
            // segment 0, moving group 2's entry, and fills the rest of segment 2.
            log.write(
                1,
                Update {
                    entries: Some(Entries {
                        first: 1,
                        entries: vec![Entry {
                            term: 6,
                            bytes: Arc::from(vec![7u8; 5000]),
                        }],
                    }),
                    hard_state: Some(hard(6, 0)),
                    ..Update::default()
                },
            )
            .unwrap();
            let image = file.durable_image().unwrap();
            let tails = frame_tails(&image);
            let (&swept, &(swept_at, tail)) = tails.iter().next_back().unwrap();
            assert_eq!(swept, 7, "seed {seed}");
            assert_eq!(tails[&(swept - 1)].1, 1, "the frame before names segment 0");
            assert_eq!(tail, 2, "the sweep names segment 1 as the tail");
            assert_eq!(swept_at, slot0 + 2 * segment + 2 * BLOCK as u64);
            let before = image[slot0 as usize..(slot0 + segment) as usize].to_vec();
            // The next frame opens segment 0 again; power fails after `ops` of its writes. Its
            // sectors differ from what they overwrite, so any of them may tear.
            file.inject(Fault::PowerCut { ops }).unwrap();
            assert!(
                log.write(
                    1,
                    Update {
                        entries: Some(Entries {
                            first: 1,
                            entries: vec![Entry {
                                term: 7,
                                bytes: (0..3000u32).map(|i| (i % 251) as u8 + 1).collect(),
                            }],
                        }),
                        hard_state: Some(hard(7, 0)),
                        ..Update::default()
                    },
                )
                .is_err()
            );
            drop(log);
            file.crash(Crash::Random).unwrap();
            file.clear_faults().unwrap();
            let image = file.durable_image().unwrap();
            if frame_tails(&image).contains_key(&(swept + 1)) {
                // The opening survived whole and proves the swept frame was flushed: damage to
                // it is damage to an interior frame, which other tests cover.
                continue;
            }
            let untouched = image[slot0 as usize..(slot0 + segment) as usize] == before[..];
            damage(&file, swept_at + 70);
            let at = format!("power cut after {ops} writes, seed {seed}");
            let damaged = file.durable_image().unwrap();
            match Log::open(Arc::clone(&file), cfg, ID) {
                Err(LogError::Damaged(why)) => {
                    assert!(!untouched, "{at}: refused with segment 0 intact");
                    // Segment 0's header still reads, and its frames begin with the opening's.
                    unframed += u32::from(why == "a live segment holds no frame");
                    assert!(
                        file.durable_image().unwrap() == damaged,
                        "{at}: a failed open wrote"
                    );
                    refused += 1;
                }
                Err(e) => panic!("{at}: {e:?}"),
                Ok((log, recovery)) => {
                    let two = log.view(2).unwrap();
                    assert!(
                        recovery.damaged.contains(&2) || two.is_some_and(|v| v.last == 1),
                        "{at}: group 2's acknowledged entry is gone unreported: {recovery:?}"
                    );
                    if !recovery.damaged.contains(&2) {
                        assert_eq!(log.entries(2, 1, 2, u64::MAX).unwrap()[0].term, 1);
                    }
                    let one = log.view(1).unwrap().unwrap();
                    assert_eq!(one.hard_state.map(|h| h.term), Some(6), "{at}");
                    if untouched {
                        intact += 1;
                    } else {
                        clobbered_open += 1;
                    }
                }
            }
        }
    }
    assert!(
        unframed > 0 && refused > 0 && intact > 0,
        "{unframed} of {refused} refused with segment 0 frameless, {intact} intact, \
         {clobbered_open} opened over a torn opening"
    );
}

/// Damage where the tail segment's first frame begins, to its magic or its identity, ends
/// that segment's frames before the first. A later segment's frames follow, so the damage is
/// of acknowledged frames and is reported, never taken for a segment that held nothing:
/// group 2, whose only record lies in that segment, would otherwise vanish unreported.
#[test]
fn a_live_segment_whose_first_frame_no_longer_reads_is_reported() {
    for field in [0u64, 8, 24, 32] {
        let file = sim(60 + field);
        let log = Log::create(Arc::clone(&file), config(16, 8), ID).unwrap();
        log.write(
            2,
            Update {
                entries: Some(entries(1, &[1])),
                ..Update::default()
            },
        )
        .unwrap();
        for i in 1..=15u64 {
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
        assert!(
            frames.iter().any(|f| f.1 == 2),
            "the log went on to segment 1"
        );
        // The empty first frame of segment 0, after its header.
        file.inject(Fault::BitFlip {
            offset: AREA + BLOCK as u64 + field,
            bit: 0,
            stored: true,
        })
        .unwrap();
        let opened = Log::open(Arc::clone(&file), config(16, 8), ID);
        assert!(
            matches!(opened, Err(LogError::Damaged(_))),
            "damage at byte {field} of the first frame: {:?}",
            opened.map(|(_, r)| r)
        );
    }
}

/// The last frame, confirmed, then damaged at rest, where the next frame opened a segment
/// whose header reached the disk and whose frame did not. The restore at open finds free
/// segments short and sweeps the tail, the lost frame's own segment: the frame there after the
/// last that reads was never live in what recovery rebuilt, and the sweep must not take it for
/// a live frame damaged. The log opens and restores the lost frame.
#[test]
fn a_lost_last_frame_before_a_torn_opening_is_restored_while_its_segment_is_swept() {
    let cfg = config_now(4, 3);
    let segment = cfg.segment_bytes;
    let mut hit = 0;
    for seed in 0..64u64 {
        let file = sim(7000 + seed);
        let log = Log::create(Arc::clone(&file), cfg, ID).unwrap();
        // Segment 0 fills, segment 1 opens and fills, and segment 0, dead, is free again.
        for term in 1..=5 {
            log.write(
                1,
                Update {
                    hard_state: Some(hard(term, 0)),
                    ..Update::default()
                },
            )
            .unwrap();
        }
        let image = file.durable_image().unwrap();
        let tails = frame_tails(&image);
        let (&lost, &(lost_at, _)) = tails.iter().next_back().unwrap();
        assert_eq!(lost, 5, "seed {seed}");
        assert_eq!(
            lost_at,
            2 * segment + 3 * BLOCK as u64,
            "the last block of segment 1"
        );
        // The next frame opens segment 0 again, and power fails before its flush.
        file.inject(Fault::PowerCut { ops: 1 }).unwrap();
        assert!(
            log.write(
                1,
                Update {
                    hard_state: Some(hard(6, 0)),
                    ..Update::default()
                },
            )
            .is_err()
        );
        drop(log);
        file.crash(Crash::Random).unwrap();
        file.clear_faults().unwrap();
        let image = file.durable_image().unwrap();
        let opened = segment_headers(&image, segment).contains(&(segment, 3));
        if !opened || frame_tails(&image).contains_key(&(lost + 1)) {
            // No opening, or a whole one, which proves the lost frame was flushed.
            continue;
        }
        hit += 1;
        damage(&file, lost_at + 70);
        let at = format!("seed {seed}");
        let (log, recovery) =
            Log::open(Arc::clone(&file), cfg, ID).unwrap_or_else(|e| panic!("{at}: {e:?}"));
        assert_eq!(
            (recovery.restored, recovery.damaged),
            (vec![1], vec![]),
            "{at}"
        );
        assert_eq!(log.view(1).unwrap().unwrap().hard_state, Some(hard(5, 0)));
        drop(log);
        let (log, _) = Log::open(Arc::clone(&file), cfg, ID).unwrap();
        assert_eq!(log.view(1).unwrap().unwrap().hard_state, Some(hard(5, 0)));
    }
    assert!(
        hit > 0,
        "no seed left the opening's header without its frame"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    /// Generated histories cut by power loss, and then the last frame that still reads
    /// damaged at rest, whether it was acknowledged or not: after the reopen every group
    /// acknowledged before the cut, but the one in flight, is reported damaged or holds its
    /// start, a term and vote no older, and every entry it held or a mark that it may lack
    /// it. A refusal to open reports the damage too; nothing acknowledged goes unreported.
    #[test]
    fn damage_after_power_loss_never_loses_acknowledged_state_unreported(
        steps in prop::collection::vec(step(), 1..60),
        seed in any::<u64>(),
        segment_blocks in 4u64..10,
        max_segments in 3u32..8,
    ) {
        let file = sim(seed);
        let cfg = config_now(segment_blocks, max_segments);
        let log = Log::create(Arc::clone(&file), cfg, ID).unwrap();
        let mut models = Models::new();
        for step in &steps {
            if let Step::Crash { ops } = step {
                file.inject(Fault::PowerCut { ops: *ops }).unwrap();
                continue;
            }
            let Some((group, u)) = update(step, &models) else { continue };
            match log.write(group, u.clone()) {
                Ok(()) => apply(&mut models, group, &u),
                Err(LogError::Fenced) => {
                    drop(log);
                    file.crash(Crash::Random).unwrap();
                    file.clear_faults().unwrap();
                    let image = file.durable_image().unwrap();
                    let frames = valid_frames(&image);
                    let Some(last) = frames.iter().max_by_key(|f| f.2) else {
                        return Ok(());
                    };
                    file.inject(Fault::BitFlip {
                        offset: last.0 + 70,
                        bit: 0,
                        stored: true,
                    })
                    .unwrap();
                    let (reopened, recovery) = match Log::open(Arc::clone(&file), cfg, ID) {
                        Err(LogError::Damaged(_)) => return Ok(()),
                        Err(e) => return Err(TestCaseError::fail(format!("{e}"))),
                        Ok(r) => r,
                    };
                    for (&g, m) in &models {
                        if g == group || recovery.damaged.contains(&g) {
                            continue;
                        }
                        let view = reopened.view(g).unwrap();
                        prop_assert!(view.is_some(), "group {g} gone: {recovery:?}");
                        let view = view.unwrap();
                        prop_assert_eq!(view.start, m.start, "group {}", g);
                        if let Some(h) = m.hard {
                            let got = view.hard_state.unwrap();
                            prop_assert!(
                                got.term > h.term || (got.term == h.term && got.vote == h.vote),
                                "group {g}: {got:?} regressed from {h:?}"
                            );
                        }
                        for (i, (term, bytes)) in (m.start.index + 1..).zip(&m.entries) {
                            if i <= view.last {
                                let got = reopened.entries(g, i, i + 1, u64::MAX).unwrap();
                                prop_assert!(
                                    got[0].term == *term && *got[0].bytes == bytes[..],
                                    "group {g} entry {i} differs"
                                );
                            } else {
                                prop_assert!(
                                    view.uncertain.is_some_and(|mark| mark.index >= i),
                                    "group {g} entry {i} lost unmarked: {view:?} {recovery:?}"
                                );
                            }
                        }
                    }
                    return Ok(());
                }
                Err(
                    LogError::Invalid { .. }
                    | LogError::Backlog(_)
                    | LogError::TooManyGroups(_)
                    | LogError::Full,
                ) => {}
                Err(e) => return Err(TestCaseError::fail(format!("{e}"))),
            }
        }
    }
}
