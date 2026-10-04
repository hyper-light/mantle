//! The Raft log on real and simulated devices (docs/design/raft-log.md §8).
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

use std::collections::{BTreeMap, HashMap};

use hyper_block::block::BlockFile;
use hyper_block::buf::{AlignedBuf, Alignment};
use hyper_block::sim::{Crash, Fault, SimFile};
use hyper_log::{
    Class, Config, Entries, Entry, HardState, Log, LogError, LogStats, Proposal, Refused, Start,
    Update, View, Waits,
};
use proptest::prelude::*;

mod common;
use common::{Held, Holder, held, held_telling};

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

fn sim(seed: u64) -> SimFile {
    SimFile::new(
        Alignment::new(BLOCK).unwrap(),
        Alignment::new(512).unwrap(),
        seed,
    )
    .unwrap()
}

/// The log's file, once the log has answered everything and closed.
fn closed<F: BlockFile>(log: Log<F>) -> F {
    log.close().unwrap()
}

/// The file an open took, whether it opened or was refused.
fn opened_or_not(
    opened: Result<(Log<SimFile>, hyper_log::Recovery), hyper_log::Refused<SimFile>>,
) -> SimFile {
    match opened {
        Ok((log, _)) => closed(log),
        Err(refused) => refused.file.unwrap(),
    }
}

fn entry(term: u64, tag: &str) -> Entry {
    Entry {
        term,
        bytes: Vec::from(tag.as_bytes()),
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
fn holds<F: hyper_block::block::BlockFile>(
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
fn check<F: hyper_block::block::BlockFile>(log: &Log<F>, models: &Models) {
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
    let log = Log::create(file, config(16, 8), ID).unwrap();
    let a = Update {
        entries: Some(entries(1, &[1, 1, 1, 2, 2])),
        hard_state: Some(hard(2, 3)),
        proposals: vec![Proposal {
            index: 7,
            term: 2,
            bytes: Vec::from(&b"fast"[..]),
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
    let file = closed(log);
    let (log, recovery) = Log::open(file, config(16, 8), ID).unwrap();
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
    let log = Log::create(file, config(16, 8), ID).unwrap();
    let run = |log: &Log<SimFile>, models: &mut Models, u: Update| {
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
                bytes: Vec::from(&b"p"[..]),
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
    let file = closed(log);
    let (log, _) = Log::open(file, config(16, 8), ID).unwrap();
    check(&log, &models);
}

#[test]
fn invalid_updates_are_refused_and_change_nothing() {
    let file = sim(3);
    let mut small = config(16, 8);
    small.max_groups = 2;
    small.group_entries = 4;
    let log = Log::create(file, small, ID).unwrap();
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
                bytes: Vec::from(&b"x"[..]),
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
            bytes: vec![0u8; 16 * BLOCK],
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
    let log = Log::create(file, config(16, 8), ID).unwrap();
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
    let file = closed(log);
    let (log, _) = Log::open(file, config(16, 8), ID).unwrap();
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
    let log = Log::create(file, cfg, ID).unwrap();
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
    let file = closed(log);
    let (log, recovery) = Log::open(file, cfg, ID).unwrap();
    assert!(recovery.damaged.is_empty());
    check(&log, &models);
    // The file never grew past its quota.
    let len = hyper_block::block::BlockFile::len(&closed(log)).unwrap();
    assert!(len <= cfg.segment_bytes * u64::from(cfg.max_segments));
}

/// The statistics count every frame, flush, byte and wait the log made (`docs/durable.md`
/// §13.1), against the file's own counts: writes one at a time, each a frame of one small update,
/// its persist record and its confirmation, a block each, and two flushes.
#[test]
fn the_statistics_count_each_frame_flush_byte_and_wait() {
    let log = Log::create(sim(46), config(16, 8), ID).unwrap();
    let counts = |log: &Log<SimFile>| {
        log.with_file(|f| {
            let s = f.stats().unwrap();
            (s.writes, s.syncs)
        })
        .unwrap()
    };
    let before = log.stats(None).unwrap();
    let (writes, syncs) = counts(&log);
    let n = 5;
    for index in 1..=n {
        log.write(
            1,
            Update {
                entries: Some(entries(index, &[1])),
                ..Update::default()
            },
        )
        .unwrap();
    }
    let after = log.stats(None).unwrap();
    let (writes_after, syncs_after) = counts(&log);
    assert_eq!(after.frames - before.frames, n);
    assert_eq!(after.updates - before.updates, n);
    assert_eq!(after.flushes - before.flushes, syncs_after - syncs);
    assert_eq!(after.flushes - before.flushes, 2 * n);
    assert_eq!(
        after.bytes - before.bytes,
        (writes_after - writes) * BLOCK as u64
    );
    assert_eq!(after.flush.count() - before.flush.count(), 2 * n);
    assert_eq!(after.write.count() - before.write.count(), n);
    assert_eq!(after.commit_wait.count() - before.commit_wait.count(), n);
    // Each wait runs from its submission past its frame's writes and flush and its
    // confirmation's flush, one write at a time, so the waits hold every write and flush timed.
    let waited = after.commit_wait.sum_ns() - before.commit_wait.sum_ns();
    let worked = (after.write.sum_ns() - before.write.sum_ns())
        + (after.flush.sum_ns() - before.flush.sum_ns());
    assert!(
        waited >= worked,
        "{waited} ns waited, {worked} ns written and flushed"
    );
    assert_eq!(after.flushing_since, None);
    assert!(after.at >= before.at);
}

/// A flush that stalls delays no answer of the statistics, which show it in progress: the owner
/// answers them as they come, never waiting on I/O (`docs/durable.md` §13.1). A box handed back is
/// filled again.
#[test]
fn a_held_flush_shows_in_the_statistics_without_delaying_them() {
    let (device, gated) = held(sim(47));
    let log = Log::create(device, config(16, 8), ID).unwrap();
    let before = log.stats(None).unwrap();
    gated.hold();
    let shut = gated.released();
    let pending = log
        .submit(
            1,
            Update {
                entries: Some(entries(1, &[1])),
                ..Update::default()
            },
        )
        .unwrap();
    gated.held();
    let during = log.stats(None).unwrap();
    let since = during
        .flushing_since
        .expect("the frame's flush is in progress");
    assert!(before.at <= since && since <= during.at);
    assert_eq!(during.frames, before.frames, "the frame is not flushed yet");
    assert_eq!(during.flush.count(), before.flush.count());
    gated.release();
    pending.wait().unwrap();
    let kept: *const LogStats = &*during;
    let after = log.stats(Some(during)).unwrap();
    assert!(std::ptr::eq(kept, &*after), "the box handed back is filled");
    assert_eq!(after.flushing_since, None);
    assert_eq!(after.frames, before.frames + 1);
    assert_eq!(after.flushes, before.flushes + 2);
    assert_eq!(after.commit_wait.count(), before.commit_wait.count() + 1);
    drop(shut);
}

#[test]
fn many_submitters_share_flushes() {
    let file = sim(6);
    let log = Log::create(file, config(64, 8), ID).unwrap();
    let before = log.with_file(|f| f.stats().unwrap().syncs).unwrap();
    std::thread::scope(|s| {
        for group in 0..16u128 {
            let log = &log;
            s.spawn(move || {
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
            });
        }
    });
    let syncs = log.with_file(|f| f.stats().unwrap().syncs).unwrap() - before;
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
    let log = Log::create(file, cfg, ID).unwrap();
    std::thread::scope(|s| {
        for group in 0..8u128 {
            let log = &log;
            s.spawn(move || {
                for i in 1..=20u64 {
                    let u = Update {
                        entries: Some(entries(i, &[1])),
                        ..Update::default()
                    };
                    log.write_waiting(group, u).unwrap();
                }
            });
        }
    });
    for group in 0..8u128 {
        assert_eq!(log.view(group).unwrap().unwrap().last, 20);
    }
}

#[test]
fn a_failed_flush_fences_the_log_and_loses_nothing_acknowledged() {
    let file = sim(7);
    let mut models = Models::new();
    let log = Log::create(file, config(16, 8), ID).unwrap();
    let u = Update {
        entries: Some(entries(1, &[1, 1])),
        ..Update::default()
    };
    log.write(1, u.clone()).unwrap();
    apply(&mut models, 1, &u);
    log.with_file(|f| f.inject(Fault::SyncError).unwrap())
        .unwrap();
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
    let file = closed(log);
    file.crash(Crash::LoseAll).unwrap();
    file.clear_faults().unwrap();
    let (log, recovery) = Log::open(file, config(16, 8), ID).unwrap();
    // Whatever of the lost frame's persist record the failed flush left durable, the frame
    // was never confirmed: it is the torn tail, and it held no term or vote to keep.
    assert!(recovery.restored.is_empty());
    check(&log, &models);
}

#[test]
fn damage_to_an_acknowledged_frame_is_reported() {
    let file = sim(8);
    let log = Log::create(file, config(16, 8), ID).unwrap();
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
    let file = closed(log);
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
        Log::open(file, config(16, 8), ID),
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
        let log = Log::create(file, cfg, ID).unwrap();
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
        let mut file = closed(log);
        // Frames of one block each: the empty first frame, then one an update.
        file.inject(Fault::BitFlip {
            offset: AREA + 5 * BLOCK as u64 + field,
            bit: 2,
            stored: true,
        })
        .unwrap();
        for reopening in 0..2 {
            let (log, recovery) = Log::open(file, cfg, ID).unwrap();
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
            file = closed(log);
        }
        let (log, _) = Log::open(file, cfg, ID).unwrap();
        log.write(
            1,
            Update {
                entries: Some(entries(4, &[2])),
                ..Update::default()
            },
        )
        .unwrap();
        assert_eq!(log.view(1).unwrap().unwrap().uncertain, None);
        let file = closed(log);
        let (log, recovery) = Log::open(file, cfg, ID).unwrap();
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
    let (device, gated) = held(file);
    let log = Log::create(device, cfg, ID).unwrap();
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
    gated.hold();
    let shut = gated.released();
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
                    bytes: Vec::from(&b"p"[..]),
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
    let mut file = closed(log).into_inner();
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
        let (log, recovery) = Log::open(file, cfg, ID).unwrap();
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
        file = closed(log);
    }
    let (log, _) = Log::open(file, cfg, ID).unwrap();
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
fn damaged_group_one(seed: u64, cfg: Config) -> SimFile {
    let file = sim(seed);
    let log = Log::create(file, cfg, ID).unwrap();
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
                bytes: Vec::from(&b"p"[..]),
            }],
            ..Update::default()
        },
    )
    .unwrap();
    let file = closed(log);
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
    let (log, recovery) = Log::open(file, cfg, ID).unwrap();
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
    let file = closed(log);
    let (log, recovery) = Log::open(file, cfg, ID).unwrap();
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
            let file = opened_or_not(Log::try_open(file, cfg, ID));
            file.crash(Crash::Random).unwrap();
            file.clear_faults().unwrap();
            let (log, recovery) = Log::open(file, cfg, ID).unwrap();
            assert_eq!(
                recovery.damaged,
                vec![1],
                "power cut after {ops} operations"
            );
            assert!(matches!(log.view(1), Err(LogError::Damaged(_))));
            let file = closed(log);
            let (_, recovery) = Log::open(file, cfg, ID).unwrap();
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
    let log = Log::create(file, cfg, ID).unwrap();
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
    let file = closed(log);
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
    let (log, recovery) = Log::open(file, cfg, ID).unwrap();
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
    let file = closed(log);
    let (log, _) = Log::open(file, cfg, ID).unwrap();
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
    let log = Log::create(file, cfg, ID).unwrap();
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
    log.with_file(|f| f.inject(Fault::PowerCut { ops: 2 }).unwrap())
        .unwrap();
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
    let file = closed(log);
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
    let (log, recovery) = Log::open(file, cfg, ID).unwrap();
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
        let log = Log::create(file, cfg, ID).unwrap();
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
            log.with_file(|f| f.crash(Crash::LoseAll).unwrap()).unwrap();
        } else {
            // The frame's write, its persist record's and the flush succeed; the confirmation's
            // write does not.
            log.with_file(|f| f.inject(Fault::PowerCut { ops: 3 }).unwrap())
                .unwrap();
            assert!(matches!(log.write(1, second), Err(LogError::Fenced)));
            log.with_file(|f| f.crash(Crash::KeepAll).unwrap()).unwrap();
        }
        let file = closed(log);
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
        let (log, recovery) = Log::open(file, cfg, ID).unwrap();
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
    let log = Log::create(file, cfg, ID).unwrap();
    log.write(
        1,
        Update {
            entries: Some(entries(1, &[1])),
            ..Update::default()
        },
    )
    .unwrap();
    log.with_file(|f| f.inject(Fault::PowerCut { ops: 1 }).unwrap())
        .unwrap();
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
    let file = closed(log);
    file.crash(Crash::KeepAll).unwrap();
    file.clear_faults().unwrap();
    let image = file.durable_image().unwrap();
    let torn = *valid_frames(&image)
        .iter()
        .find(|f| f.2 == 2)
        .expect("the frame's write reached the disk");
    // The frame of sequence 2 keeps its record in the first persist slot.
    assert!(
        hyper_log::format::Persist::decode(&image).is_none_or(|p| p.sequence != 2),
        "the frame's persist record reached the disk"
    );
    damage(&file, torn.0 + 70);
    let (log, recovery) = Log::open(file, cfg, ID).unwrap();
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
        let log = Log::create(file, config(16, 8), ID).unwrap();
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
        let file = closed(log);
        // The frame of the first update, after the segment header and the empty first frame.
        file.inject(Fault::BitFlip {
            offset: AREA + 2 * BLOCK as u64 + field,
            bit: 0,
            stored: true,
        })
        .unwrap();
        let opened = Log::open(file, config(16, 8), ID);
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
            let h = hyper_log::format::SegmentHeader::decode(s)?;
            Some((slot as u64 * segment, h.incarnation))
        })
        .collect()
}

/// Each valid frame in the durable image: its offset, incarnation and sequence.
fn valid_frames(image: &[u8]) -> Vec<(u64, u64, u64)> {
    let mut out = Vec::new();
    for (i, block) in image.chunks(BLOCK).enumerate() {
        let Some(h) = hyper_log::format::FrameHeader::decode(block) else {
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
fn one_frame_each(seed: u64, n: u64) -> SimFile {
    let file = sim(seed);
    let log = Log::create(file, config(16, 8), ID).unwrap();
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
    let file = closed(log);
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
    let Err(Refused {
        error: LogError::Damaged(_),
        file: Some(file),
    }) = Log::try_open(file, config(16, 8), ID)
    else {
        panic!("the damaged header was not reported");
    };
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
    let zeros = hyper_block::buf::AlignedBuf::zeroed(BLOCK, Alignment::new(BLOCK).unwrap())
        .map(|mut b| {
            b.set_len(BLOCK).unwrap();
            b
        })
        .unwrap();
    hyper_block::block::BlockFile::write_all_at(&file, zeros.as_slice(), segment).unwrap();
    hyper_block::block::BlockFile::sync_data(&file).unwrap();
    let (log, _) = Log::open(file, config(16, 8), ID).unwrap();
    assert_eq!(log.view(1).unwrap().unwrap().last, 14);
    log.write(
        1,
        Update {
            entries: Some(entries(15, &[2])),
            ..Update::default()
        },
    )
    .unwrap();
    let file = closed(log);
    let (log, recovery) = Log::open(file, config(16, 8), ID).unwrap();
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
                Log::open(file, config(16, 8), ID),
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
    let log = Log::create(file, cfg, ID).unwrap();
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
        let image = log.with_file(|f| f.durable_image().unwrap()).unwrap();
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
    let file = closed(log);
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
            hyper_block::buf::AlignedBuf::zeroed(BLOCK, Alignment::new(BLOCK).unwrap()).unwrap();
        b.extend_from_slice(chunk).unwrap();
        hyper_block::block::BlockFile::write_all_at(&torn, b.as_slice(), (i * BLOCK) as u64)
            .unwrap();
    }
    hyper_block::block::BlockFile::sync_data(&torn).unwrap();

    file.inject(Fault::BitFlip {
        offset: last_at,
        bit: 0,
        stored: true,
    })
    .unwrap();
    let (log, _) = Log::open(file, cfg, ID).unwrap();
    assert_eq!(log.view(1).unwrap().unwrap().last, first - 2);
    drop(log);

    torn.inject(Fault::BitFlip {
        offset: inner_at,
        bit: 0,
        stored: true,
    })
    .unwrap();
    assert!(matches!(
        Log::open(torn, cfg, ID),
        Err(LogError::Damaged(_))
    ));
}

/// A simulated file that counts its reads and the looks at its length.
struct Counting {
    file: SimFile,
    reads: std::cell::Cell<u64>,
    lengths: std::cell::Cell<u64>,
}

impl Counting {
    fn new(file: SimFile) -> Self {
        Self {
            file,
            reads: std::cell::Cell::new(0),
            lengths: std::cell::Cell::new(0),
        }
    }

    /// Reads and looks at the length since the last call.
    fn take(&self) -> (u64, u64) {
        (self.reads.replace(0), self.lengths.replace(0))
    }
}

impl hyper_block::block::BlockFile for Counting {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }

    fn len(&self) -> Result<u64, hyper_block::DiskError> {
        self.lengths.set(self.lengths.get() + 1);
        hyper_block::block::BlockFile::len(&self.file)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), hyper_block::DiskError> {
        self.reads.set(self.reads.get() + 1);
        self.file.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), hyper_block::DiskError> {
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), hyper_block::DiskError> {
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
    let log = Log::create(counting, settings, ID).unwrap();
    for n in 1..=120u64 {
        let u = Update {
            entries: Some(entries(n, &[1])),
            ..Update::default()
        };
        apply(&mut models, 1, &u);
        log.write(1, u).unwrap();
    }
    let counting = closed(log);
    let len = hyper_block::block::BlockFile::len(&counting.file).unwrap();
    let segments = (len - AREA).div_ceil(settings.segment_bytes);
    counting.take();
    let (log, recovery) = Log::open(counting, settings, ID).unwrap();
    let (reads, lengths) = log.with_file(Counting::take).unwrap();
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
    let log = Log::create(counting, settings, ID).unwrap();
    let terms = vec![1u64; 200];
    log.write(
        1,
        Update {
            entries: Some(entries(1, &terms)),
            ..Update::default()
        },
    )
    .unwrap();
    log.with_file(Counting::take).unwrap();
    let got = log.entries(1, 1, 201, u64::MAX).unwrap();
    let (reads, _) = log.with_file(Counting::take).unwrap();
    assert_eq!(got, entries(1, &terms).entries);
    assert_eq!(reads, 1, "{reads} reads for 200 entries written together");
}

/// A group's updates are made durable in the order it submitted them, even when the older
/// one waits for a frame with room and a newer one would fit the frame it missed (audit S02).
#[test]
fn a_groups_updates_keep_their_order_when_one_waits_for_room() {
    let (device, gated) = held(sim(40));
    let cfg = config(4, 8);
    let log = Log::create(device, cfg, ID).unwrap();
    let state = |term, vote| HardState {
        term,
        vote,
        commit: 0,
    };
    gated.hold();
    let shut = gated.released();
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
    let device = closed(log);
    let (log, _) = Log::open(device, cfg, ID).unwrap();
    assert_eq!(log.view(1).unwrap().unwrap().hard_state, Some(state(2, 2)));
}

/// The queue's bound holds everything the writer has not answered: submissions waiting in
/// the channel, updates held for a later frame and the frame being flushed. A refusal comes
/// at the bound, never after it (audit S03).
#[test]
fn the_queue_bounds_every_submission_not_yet_answered() {
    let (device, gated) = held(sim(41));
    let mut cfg = config(16, 8);
    cfg.queue_submissions = 2;
    let log = Log::create(device, cfg, ID).unwrap();
    let small = |term| Update {
        hard_state: Some(hard(term, 0)),
        ..Update::default()
    };
    gated.hold();
    let shut = gated.released();
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
    let (device, gated) = held(sim(42));
    let log = Log::create(device, cfg, ID).unwrap();
    gated.hold();
    let shut = gated.released();
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
    let header = hyper_log::format::encoded_len(&hyper_log::format::Record::Entries {
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
                bytes: vec![b'x'; len - header],
            }],
        }),
        ..Update::default()
    }
}

/// What a submission of `len` payload bytes holds of the queue's byte bound: its records and
/// its row in the frame's persist record.
fn charged(len: usize) -> u64 {
    (len + hyper_log::format::PERSIST_GROUP_LEN) as u64
}

/// The queue's byte bound is three frames of the largest charge, and holds every submission
/// not yet answered by the bytes its records take: one group's flood of frame-sized updates is
/// refused at its own two, another group still gets in, and the bound refuses past three
/// though the count bound has room (audit S03).
#[test]
fn the_queue_bounds_the_bytes_of_every_submission_not_yet_answered() {
    let (device, stepped) = held(sim(43));
    let cfg = config_now(16, 64);
    let log = Log::create(device, cfg, ID).unwrap();
    let room = log.frame_room().unwrap();
    assert_eq!(log.queue_bytes(), 3 * charged(room));
    let full = |first| sized(first, room);
    stepped.hold();
    let _released = stepped.released();
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
    let (device, stepped) = held(sim(44));
    let mut cfg = config_now(16, 64);
    cfg.queue_submissions = 4096;
    let log = Log::create(device, cfg, ID).unwrap();
    let empty = Update {
        entries: Some(Entries {
            first: 1,
            entries: vec![Entry {
                term: 1,
                bytes: Vec::new(),
            }],
        }),
        ..Update::default()
    };
    let len = hyper_log::format::encoded_len(&hyper_log::format::Record::Entries {
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
    let _released = stepped.released();
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
    let (device, stepped) = held(sim(45));
    let cfg = config_now(16, 64);
    let log = Log::create(device, cfg, ID).unwrap();
    let room = log.frame_room().unwrap();
    let (hot, cold) = (room * 45 / 100, room * 60 / 100);
    stepped.hold();
    let _released = stepped.released();
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
    let (device, stepped) = held(sim(seed));
    let cfg = config_now(16, 64);
    let log = Log::create(device, cfg, ID).unwrap();
    let room = log.frame_room().unwrap();
    let (hot, cold) = (room * 55 / 100, room * 60 / 100);
    stepped.hold();
    let _released = stepped.released();
    let mut pending = vec![log.submit(9, Update::default()).unwrap()];
    stepped.held();
    // A group has one update in a frame at most, so two stand in for a range whose next
    // update arrives for every frame.
    let mut next = [1u64, 1u64];
    let mut feed = |log: &Log<Held>, frame: usize, pending: &mut Vec<hyper_log::Pending>| {
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
    let (device, stepped) = held(sim(47));
    let log = Log::create(device, config_now(16, 64), ID).unwrap();
    let big = log.frame_room().unwrap() * 60 / 100;
    stepped.hold();
    let _released = stepped.released();
    let mut pending = vec![log.submit(9, Update::default()).unwrap()];
    stepped.held();
    pending.push(log.submit_in(3, Class::Background, sized(1, big)).unwrap());
    pending.push(log.submit_in(2, Class::Normal, sized(1, big)).unwrap());
    pending.push(log.submit_in(1, Class::Latency, sized(1, big)).unwrap());
    let written = |log: &Log<Held>| -> Vec<bool> {
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
    let (device, stepped) = held(sim(48));
    let log = Log::create(device, config_now(16, 64), ID).unwrap();
    let big = log.frame_room().unwrap() * 60 / 100;
    stepped.hold();
    let _released = stepped.released();
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
    let (device, stepped) = held(sim(49));
    let log = Log::create(device, config_now(16, 64), ID).unwrap();
    let room = log.frame_room().unwrap();
    let (hot, cold) = (room * 60 / 100, room * 50 / 100);
    stepped.hold();
    let _released = stepped.released();
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

/// A log whose every segment holds live records answers `Full`, and never goes on sweeping
/// without making room: once groups compact, the next write goes in (docs/design/raft-log.md
/// §5). The writer's frames and reads are counted, so a writer that loops without answering
/// fails the test rather than hanging it: a sweep reads its segment, and writes a frame.
#[test]
fn a_log_whose_segments_are_all_live_answers_full() {
    for (seed, (blocks, segments, len)) in (60..).zip([
        (4, 4, 7 << 10),
        (4, 4, 1 << 10),
        (4, 8, 7 << 10),
        (16, 4, 1 << 10),
    ]) {
        let log = Log::create(Counting::new(sim(seed)), config(blocks, segments), ID).unwrap();
        // The answer, or `None` once the writer has gone past what one update takes: one
        // frame, which sweeps at most once, and a sweep reads its segment in two reads at most
        // (raft-log.md §6).
        let write = |update: Update| -> Option<Result<(), LogError>> {
            log.with_file(Counting::take).unwrap();
            let pending = log.submit(1, update).unwrap();
            let before = log.flushed().0;
            let mut reads = 0;
            loop {
                if let Some(answer) = pending.poll() {
                    return Some(answer);
                }
                reads += log.with_file(Counting::take).unwrap().0;
                if reads > 2 || log.flushed().0 > before + 2 {
                    return None;
                }
                std::thread::yield_now();
            }
        };
        let looping = format!(
            "{blocks}-block segments, {segments} of them, {len}-byte entries: \
             the writer loops without answering"
        );
        let mut index = 1;
        let full = loop {
            let update = Update {
                entries: Some(Entries {
                    first: index,
                    entries: vec![Entry {
                        term: 1,
                        bytes: vec![b'x'; len],
                    }],
                }),
                ..Update::default()
            };
            match write(update) {
                Some(Ok(())) => index += 1,
                Some(Err(e)) => break e,
                None => {
                    // Dropping the log would join a writer that never returns.
                    let _ = std::mem::ManuallyDrop::new(log);
                    panic!("{looping}");
                }
            }
        };
        assert!(matches!(full, LogError::Full), "{full}");
        assert!(index > 1, "nothing written before the log filled");
        // The group compacts everything it wrote, and the log makes room again.
        let compact = Update {
            start: Some(Start {
                index: index - 1,
                term: 1,
            }),
            ..Update::default()
        };
        let next = Update {
            entries: Some(entries(index, &[1])),
            ..Update::default()
        };
        for update in [compact, next] {
            match write(update) {
                Some(answer) => answer.unwrap(),
                None => {
                    let _ = std::mem::ManuallyDrop::new(log);
                    panic!("{looping}");
                }
            }
        }
        assert_eq!(log.view(1).unwrap().unwrap().last, index);
    }
}

/// While no segment is free, the head's room is kept for frames that make room: a full log
/// whose groups compact a little and then write again until refused still takes the
/// compaction that frees it (docs/design/raft-log.md §5).
#[test]
fn a_full_log_keeps_room_for_the_compaction_it_waits_for() {
    let log = Log::create(sim(70), config(4, 4), ID).unwrap();
    let one = |index: u64| Update {
        entries: Some(Entries {
            first: index,
            entries: vec![Entry {
                term: 1,
                bytes: vec![b'x'; 1 << 10],
            }],
        }),
        ..Update::default()
    };
    let compact = |index: u64| Update {
        start: Some(Start { index, term: 1 }),
        ..Update::default()
    };
    let mut index = 1;
    let fill = |index: &mut u64| loop {
        match log.write(1, one(*index)) {
            Ok(()) => *index += 1,
            Err(LogError::Full) => return,
            Err(e) => panic!("{e}"),
        }
    };
    fill(&mut index);
    // A compaction that frees no segment: the oldest entry only.
    log.write(1, compact(1)).unwrap();
    fill(&mut index);
    log.write(1, compact(index - 1)).unwrap();
    log.write(1, one(index)).unwrap();
    assert_eq!(log.view(1).unwrap().unwrap().last, index);
}

/// A reopened log sweeps the segments it recovered as the writer swept those it wrote: it
/// knows the bytes their frames take. Frames of one small entry each fill segments that a
/// sweep packs into one frame, and the log fills no sooner for having been reopened.
#[test]
fn a_reopened_log_sweeps_the_segments_it_recovered() {
    let file = sim(71);
    let settings = config(16, 8);
    let mut models = Models::new();
    let write = |log: &Log<SimFile>, models: &mut Models, n: u64| {
        let u = Update {
            entries: Some(entries(n, &[1])),
            ..Update::default()
        };
        apply(models, 1, &u);
        log.write(1, u).unwrap();
    };
    let log = Log::create(file, settings, ID).unwrap();
    for n in 1..=100u64 {
        write(&log, &mut models, n);
    }
    let file = closed(log);
    let (log, _) = Log::open(file, settings, ID).unwrap();
    for n in 101..=200u64 {
        write(&log, &mut models, n);
    }
    check(&log, &models);
}

/// A log that answered `Full` takes any frame that fits a segment once its groups compact:
/// a group's small live record in the oldest segment holds back the dead segments behind it
/// only until a sweep copies it, and that sweep must go in whatever room the log has
/// (docs/design/raft-log.md §5). Entries from a frame's room down to a quarter of it.
#[test]
fn a_full_log_takes_any_frame_once_its_groups_compact() {
    for segments in [4u32, 8] {
        for share in 1..=4usize {
            let settings = Config {
                max_groups: 16,
                ..config(4, segments)
            };
            let log = Log::create(sim(80), settings, ID).unwrap();
            let size = log.entry_room().unwrap() / share;
            let one = |first: u64, fill: u8| Update {
                entries: Some(Entries {
                    first,
                    entries: vec![Entry {
                        term: 1,
                        bytes: vec![fill; size],
                    }],
                }),
                ..Update::default()
            };
            log.write(
                1,
                Update {
                    hard_state: Some(HardState {
                        term: 1,
                        vote: 1,
                        commit: 0,
                    }),
                    ..Update::default()
                },
            )
            .unwrap();
            // Each round fills the log from group 7 until refused, compacts it all away, and
            // writes one entry of group 1, whose hard state stays live in the oldest segment;
            // then group 1 compacts that entry too.
            let (mut last, mut mine) = (0u64, 0u64);
            for round in 0..u64::from(segments) {
                let full = loop {
                    match log.write(7, one(last + 1, 7)) {
                        Ok(()) => last += 1,
                        Err(e) => break e,
                    }
                };
                assert!(matches!(full, LogError::Full), "{full}");
                let compact = |index: u64| Update {
                    start: Some(Start { index, term: 1 }),
                    ..Update::default()
                };
                log.write(7, compact(last)).unwrap();
                mine += 1;
                log.write(1, one(mine, 1)).unwrap_or_else(|e| {
                    panic!(
                        "{segments} segments, entries of 1/{share} of a frame, round {round}: \
                         {e} after compacting"
                    )
                });
                log.write(1, compact(mine)).unwrap();
            }
            assert_eq!(log.view(1).unwrap().unwrap().last, mine);
        }
    }
}

#[test]
fn another_log_or_geometry_is_refused() {
    let file = closed(Log::create(sim(9), config(16, 8), ID).unwrap());
    let Err(Refused {
        error: LogError::Foreign(_),
        file: Some(file),
    }) = Log::try_open(file, config(16, 8), ID + 1)
    else {
        panic!("another log's file was opened");
    };
    let Err(Refused {
        error: LogError::Foreign(_),
        file: Some(file),
    }) = Log::try_open(file, config(32, 8), ID)
    else {
        panic!("another geometry's file was opened");
    };
    assert!(matches!(
        Log::create(file, config(16, 8), ID),
        Err(LogError::Foreign(_))
    ));
    assert!(matches!(
        Log::create(sim(10), config(3, 8), ID),
        Err(LogError::Config(_))
    ));
    // A segment one block past a buffer's bound, refused before the file is touched.
    let past = Config {
        segment_bytes: hyper_block::buf::MAX_BUFFER as u64 + BLOCK as u64,
        ..config(16, 8)
    };
    let Err(Refused {
        error: LogError::Config(_),
        file: Some(untouched),
    }) = Log::try_create(sim(11), past, ID)
    else {
        panic!("a segment past a buffer's bound was taken");
    };
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

/// A step, of each kind by the weights mantle's `prop_oneof!` gave them: appends 6, compactions
/// 2, snapshots 1, hard states 2, proposals 1, removals 1 and crashes 1 in 14. The macro boxes
/// its arms in `Arc`s, which this repository denies in tests too, so the kind is drawn as a
/// number and every field beside it.
fn step() -> impl Strategy<Value = Step> {
    (
        0u32..14,
        0u128..4,
        (prop::collection::vec(1u64..4, 1..6), 0u64..3),
        (0u64..4, 0u64..4, 1u64..5, 1u64..3, 0u64..6),
    )
        .prop_map(
            |(kind, group, (terms, back), (keep, ahead, term, near, ops))| match kind {
                0..=5 => Step::Append { group, terms, back },
                6..=7 => Step::Compact { group, keep },
                8 => Step::Snapshot { group, ahead },
                9..=10 => Step::Hard { group, term },
                11 => Step::Propose { group, ahead: near },
                12 => Step::Remove { group },
                _ => Step::Crash { ops },
            },
        )
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
                    bytes: Vec::from(&b"fast"[..]),
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
        survives_power_loss(&steps, seed, segment_blocks, max_segments)?;
    }
}

/// Generated histories, cut by power loss at random points: after each reopen the log holds
/// every acknowledged update, and the one in flight either wholly or not at all.
fn survives_power_loss(
    steps: &[Step],
    seed: u64,
    segment_blocks: u64,
    max_segments: u32,
) -> Result<(), TestCaseError> {
    let file = sim(seed);
    let cfg = config(segment_blocks, max_segments);
    let mut log = Log::create(file, cfg, ID).unwrap();
    let mut models = Models::new();
    let mut cut = false;
    for step in steps {
        if let Step::Crash { ops } = step {
            let ops = *ops;
            log.with_file(move |f| f.inject(Fault::PowerCut { ops }).unwrap())
                .unwrap();
            cut = true;
            continue;
        }
        let Some((group, u)) = update(step, &models) else {
            continue;
        };
        match log.write(group, u.clone()) {
            Ok(()) => apply(&mut models, group, &u),
            Err(LogError::Fenced) => {
                prop_assert!(cut);
                // Power is gone: crash, reopen, and see whether the update landed, which
                // it may have wholly or not at all.
                let file = closed(log);
                file.crash(Crash::Random).unwrap();
                file.clear_faults().unwrap();
                cut = false;
                let (reopened, recovery) = Log::open(file, cfg, ID).unwrap();
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
    let file = closed(log);
    file.clear_faults().unwrap();
    let (log, recovery) = Log::open(file, cfg, ID).unwrap();
    prop_assert!(recovery.damaged.is_empty());
    check(&log, &models);
    Ok(())
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
        queued_in_order(&rounds, seed, segment_blocks)?;
    }
}

/// Rounds of updates submitted while the writer is held in a flush: each group's accepted
/// updates become durable in the order submitted.
fn queued_in_order(
    rounds: &[Vec<(Step, usize)>],
    seed: u64,
    segment_blocks: u64,
) -> Result<(), TestCaseError> {
    let (device, gated) = held_telling(sim(seed));
    let cfg = config(segment_blocks, 8);
    let log = Log::create(device, cfg, ID).unwrap();
    let segment = cfg.segment_bytes as usize;
    let sizes = [16, segment / 4, segment / 2 - 200, segment - 2 * BLOCK];
    let mut models = Models::new();
    let plug = Update {
        hard_state: Some(hard(1, 0)),
        ..Update::default()
    };
    for (at, round) in rounds.iter().enumerate() {
        gated.settle();
        gated.hold();
        let shut = gated.released();
        // The plug holds the writer in its flush while the round queues, unless the log, full of
        // what the groups keep, refuses it with no frame written: then the round goes unheld.
        let plugged = log
            .submit_waking(99, Class::Normal, plug.clone(), gated.waker(at))
            .unwrap();
        gated.held_or_told(at);
        let mut predicted = models.clone();
        let mut submitted = Vec::new();
        for (step, size) in round {
            let Some((group, mut u)) = update(step, &predicted) else {
                continue;
            };
            if let Some(e) = &mut u.entries {
                for x in &mut e.entries {
                    x.bytes = vec![b'x'; sizes[*size]];
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
        match plugged.wait() {
            Ok(()) => apply(&mut models, 99, &plug),
            Err(LogError::Full | LogError::Backlog(_)) => {}
            Err(e) => return Err(TestCaseError::fail(format!("plug: {e}"))),
        }
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
    let device = closed(log);
    let (log, recovery) = Log::open(device, cfg, ID).unwrap();
    prop_assert!(recovery.damaged.is_empty());
    check(&log, &models);
    Ok(())
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
        let log = Log::create(file, cfg, ID).unwrap();
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
                        bytes: vec![b'e'; n],
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
                    bytes: vec![b'p'; n],
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
        let file = closed(log);
        let (log, _) = Log::open(file, cfg, ID).unwrap();
        check(&log, &models);
    }
}

/// Lets `permits` flushes through and holds the next, or, with `None`, lets every flush
/// through.
fn permits(holder: &Holder, permits: Option<u64>) {
    match permits {
        Some(n) => {
            holder.hold();
            for _ in 0..n {
                holder.allow();
            }
        }
        None => holder.release(),
    }
}

/// The cases proptest found failing in mantle-log's history and shrank, kept in
/// `log.proptest-regressions` as seeds of its old strategy and written here as the inputs they
/// shrank to, so that they are run whatever strategy draws the steps. Cases recorded before a
/// property took `max_segments` run under every value it now draws.
#[test]
fn recorded_regressions_hold() {
    use Step::{Append, Compact, Crash, Hard, Propose, Snapshot};
    let one = |group| Append {
        group,
        terms: vec![1],
        back: 0,
    };
    let three = |group| Append {
        group,
        terms: vec![1, 1, 1],
        back: 0,
    };
    let snap = |group, ahead| Snapshot { group, ahead };
    let without_quota: [(Vec<Step>, u64, u64); 3] = [
        (
            vec![
                three(3),
                Compact { group: 3, keep: 1 },
                Crash { ops: 0 },
                one(0),
                one(0),
            ],
            0,
            4,
        ),
        (
            vec![
                one(0),
                Propose { group: 0, ahead: 2 },
                Append {
                    group: 0,
                    terms: vec![1, 1, 1],
                    back: 1,
                },
                Append {
                    group: 0,
                    terms: vec![1],
                    back: 2,
                },
                Crash { ops: 0 },
                one(0),
            ],
            0,
            5,
        ),
        (
            vec![
                snap(2, 0),
                one(0),
                one(1),
                snap(0, 0),
                one(0),
                one(0),
                Crash { ops: 1 },
                snap(0, 0),
                Crash { ops: 1 },
                snap(0, 1),
            ],
            9_027_821_557_123_358_245,
            4,
        ),
    ];
    for (steps, seed, blocks) in &without_quota {
        for max_segments in 3u32..8 {
            survives_power_loss(steps, *seed, *blocks, max_segments).unwrap();
            damage_never_unreported(steps, *seed, *blocks, max_segments).unwrap();
        }
    }
    let mut long_one = vec![one(0), one(3), one(0), three(2), Hard { group: 0, term: 1 }];
    long_one.extend(std::iter::repeat_with(|| one(0)).take(4));
    long_one.extend([
        Hard { group: 0, term: 1 },
        one(0),
        one(0),
        three(3),
        snap(0, 0),
        one(0),
        one(0),
        snap(0, 0),
        one(0),
        snap(0, 0),
        snap(0, 0),
    ]);
    long_one.extend(std::iter::repeat_with(|| one(0)).take(7));
    long_one.extend([Crash { ops: 1 }, one(0)]);
    let mut long_two = vec![
        one(0),
        one(0),
        Append {
            group: 2,
            terms: vec![1, 1, 1, 1],
            back: 0,
        },
        snap(0, 0),
        snap(0, 0),
        snap(0, 0),
        one(0),
        snap(0, 0),
        one(0),
        one(0),
        snap(0, 0),
        snap(1, 0),
        one(0),
        snap(0, 0),
        snap(0, 0),
    ];
    long_two.extend(std::iter::repeat_with(|| one(0)).take(6));
    long_two.extend([snap(0, 0), one(0), Crash { ops: 1 }, snap(0, 0)]);
    let with_quota: [(Vec<Step>, u64, u64, u32); 3] = [
        (
            vec![Crash { ops: 2 }, Propose { group: 0, ahead: 1 }],
            3_285_527_586_399_690_220,
            4,
            3,
        ),
        (long_one, 7_909_410_152_274_503_951, 5, 3),
        (long_two, 827_730_651_015_473_522, 4, 3),
    ];
    for (steps, seed, blocks, max_segments) in &with_quota {
        survives_power_loss(steps, *seed, *blocks, *max_segments).unwrap();
        damage_never_unreported(steps, *seed, *blocks, *max_segments).unwrap();
    }
    let rounds = vec![vec![
        (one(2), 1),
        (one(1), 3),
        (
            Append {
                group: 1,
                terms: vec![1],
                back: 1,
            },
            0,
        ),
    ]];
    queued_in_order(&rounds, 0, 4).unwrap();
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

/// A frame confirmed on its own is answered, and the confirmation must outlast the next
/// frame's persist record, which is written before that frame's flush and may tear: a record
/// of five groups or more spans two sectors. Power fails at the next frame's flush, the
/// confirmed frame is damaged at rest, and recovery must still know it was acknowledged: its
/// entry is held or marked, never dropped as a torn tail.
#[test]
fn a_confirmation_outlives_a_torn_record_of_the_next_frame() {
    let (mut reopened, mut torn) = (0, 0u32);
    // The second persist slot: a record of the log's most groups, padded to the block.
    let slot = hyper_log::format::persist_len(64)
        .unwrap()
        .next_multiple_of(BLOCK);
    for seed in 0..64u64 {
        let (device, gate) = held(sim(seed));
        let cfg = config_now(16, 8);
        let log = Log::create(device, cfg, ID).unwrap();
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
        permits(&gate, Some(1));
        let second = log
            .submit(
                1,
                Update {
                    entries: Some(entries(2, &[1])),
                    ..Update::default()
                },
            )
            .unwrap();
        gate.held();
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
        // The device is held in the confirmation's flush: the fault goes in by the hold's own
        // channel, before the flush is let through.
        gate.inject(Fault::PowerCut { ops: 3 });
        permits(&gate, None);
        second.wait().unwrap();
        for o in others {
            assert!(matches!(o.wait(), Err(LogError::Fenced)));
        }
        let file = closed(log).into_inner();
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
        let record = hyper_log::format::Persist::decode(&image[slot..]);
        let tore = record.is_none();
        let two = frames.iter().find(|f| f.2 == 2).unwrap();
        damage(&file, two.0 + 70);
        let (log, recovery) = Log::open(file, cfg, ID).unwrap();
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
    let (device, gate) = held(sim(7));
    let cfg = config_now(4, 3);
    let log = Log::create(device, cfg, ID).unwrap();
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
    permits(&gate, Some(0));
    let a = log.submit(1, state(2)).unwrap();
    gate.held();
    let b = log.submit(1, state(3)).unwrap();
    // The commit after it sweeps segment 0, whose read is held, and fails.
    let segment_zero = cfg.segment_bytes;
    gate.trap(segment_zero, cfg.segment_bytes);
    permits(&gate, None);
    gate.read_held();
    let d = log.submit(4, state(1)).unwrap();
    // The device is held in the read: the fault goes in by the hold's own channel, before
    // the read is let through.
    gate.inject(Fault::ReadError {
        offset: segment_zero,
        len: cfg.segment_bytes,
    });
    gate.untrap();
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
            let (device, gate) = held(sim(1000 + seed));
            let log = Log::create(device, cfg, ID).unwrap();
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
            permits(&gate, Some(0));
            let plug = log
                .submit(
                    1,
                    Update {
                        hard_state: Some(hard(1, 0)),
                        ..Update::default()
                    },
                )
                .unwrap();
            gate.held();
            let pending: Vec<_> = groups
                .clone()
                .map(|g| {
                    let proposals = if g == 15 {
                        vec![Proposal {
                            index: 5,
                            term: 2,
                            bytes: Vec::from(&b"p"[..]),
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
            permits(&gate, None);
            plug.wait().unwrap();
            for p in pending {
                p.wait().unwrap();
            }
            let file = closed(log).into_inner();
            let frames = valid_frames(&file.durable_image().unwrap());
            let last = frames.iter().max_by_key(|f| f.2).unwrap();
            assert_eq!(last.2, 8, "the six groups share the last frame");
            damage(&file, last.0 + 70);
            file.inject(Fault::PowerCut { ops }).unwrap();
            let file = opened_or_not(Log::try_open(file, cfg, ID));
            file.crash(Crash::Random).unwrap();
            file.clear_faults().unwrap();
            let (log, recovery) = Log::open(file, cfg, ID).unwrap();
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
            let h = hyper_log::format::FrameHeader::decode(&image[at as usize..]).unwrap();
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
            let log = Log::create(file, cfg, ID).unwrap();
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
                            bytes: vec![7u8; 5000],
                        }],
                    }),
                    hard_state: Some(hard(6, 0)),
                    ..Update::default()
                },
            )
            .unwrap();
            let image = log.with_file(|f| f.durable_image().unwrap()).unwrap();
            let tails = frame_tails(&image);
            let (&swept, &(swept_at, tail)) = tails.iter().next_back().unwrap();
            assert_eq!(swept, 7, "seed {seed}");
            assert_eq!(tails[&(swept - 1)].1, 1, "the frame before names segment 0");
            assert_eq!(tail, 2, "the sweep names segment 1 as the tail");
            assert_eq!(swept_at, slot0 + 2 * segment + 2 * BLOCK as u64);
            let before = image[slot0 as usize..(slot0 + segment) as usize].to_vec();
            // The next frame opens segment 0 again; power fails after `ops` of its writes. Its
            // sectors differ from what they overwrite, so any of them may tear.
            log.with_file(move |f| f.inject(Fault::PowerCut { ops }).unwrap())
                .unwrap();
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
            let file = closed(log);
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
            match Log::try_open(file, cfg, ID) {
                Err(Refused {
                    error: LogError::Damaged(why),
                    file: Some(file),
                }) => {
                    assert!(!untouched, "{at}: refused with segment 0 intact");
                    // Segment 0's header still reads, and its frames begin with the opening's.
                    unframed += u32::from(why == "a live segment holds no frame");
                    assert!(
                        file.durable_image().unwrap() == damaged,
                        "{at}: a failed open wrote"
                    );
                    refused += 1;
                }
                Err(refused) => panic!("{at}: {:?}", refused.error),
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
        let log = Log::create(file, config(16, 8), ID).unwrap();
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
        let file = closed(log);
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
        let opened = Log::open(file, config(16, 8), ID);
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
        let log = Log::create(file, cfg, ID).unwrap();
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
        let image = log.with_file(|f| f.durable_image().unwrap()).unwrap();
        let tails = frame_tails(&image);
        let (&lost, &(lost_at, _)) = tails.iter().next_back().unwrap();
        assert_eq!(lost, 5, "seed {seed}");
        assert_eq!(
            lost_at,
            2 * segment + 3 * BLOCK as u64,
            "the last block of segment 1"
        );
        // The next frame opens segment 0 again, and power fails before its flush.
        log.with_file(|f| f.inject(Fault::PowerCut { ops: 1 }).unwrap())
            .unwrap();
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
        let file = closed(log);
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
        let (log, recovery) = Log::open(file, cfg, ID).unwrap_or_else(|e| panic!("{at}: {e:?}"));
        assert_eq!(
            (recovery.restored, recovery.damaged),
            (vec![1], vec![]),
            "{at}"
        );
        assert_eq!(log.view(1).unwrap().unwrap().hard_state, Some(hard(5, 0)));
        let file = closed(log);
        let (log, _) = Log::open(file, cfg, ID).unwrap();
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
        damage_never_unreported(&steps, seed, segment_blocks, max_segments)?;
    }
}

/// Generated histories cut by power loss, and then the last frame that still reads damaged at
/// rest: nothing acknowledged goes unreported.
fn damage_never_unreported(
    steps: &[Step],
    seed: u64,
    segment_blocks: u64,
    max_segments: u32,
) -> Result<(), TestCaseError> {
    let file = sim(seed);
    let cfg = config_now(segment_blocks, max_segments);
    let log = Log::create(file, cfg, ID).unwrap();
    let mut models = Models::new();
    for step in steps {
        if let Step::Crash { ops } = step {
            let ops = *ops;
            log.with_file(move |f| f.inject(Fault::PowerCut { ops }).unwrap())
                .unwrap();
            continue;
        }
        let Some((group, u)) = update(step, &models) else {
            continue;
        };
        match log.write(group, u.clone()) {
            Ok(()) => apply(&mut models, group, &u),
            Err(LogError::Fenced) => {
                let file = closed(log);
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
                let (reopened, recovery) = match Log::open(file, cfg, ID) {
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
    Ok(())
}

/// One thread keeps many groups' submissions out at once through `submit_waking`: each
/// answer wakes its submitter's waker once, naming which submission it was, so the thread
/// learns of every answer without looking at the others (docs/design/measurement.md §10).
#[test]
fn a_waker_is_woken_once_for_each_answer() {
    const GROUPS: usize = 32;
    let file = sim(23);
    let log = Log::create(file, config(64, 8), ID).unwrap();
    let (ready, woken) = std::sync::mpsc::sync_channel(GROUPS);
    let mut slots = Vec::new();
    let mut out: Vec<Option<hyper_log::Pending>> = (0..GROUPS)
        .map(|g| {
            let (waker, slot) = hyper_measure::wake::waker(g, ready.clone());
            slots.push(slot);
            let u = Update {
                entries: Some(entries(1, &[1])),
                ..Update::default()
            };
            Some(
                log.submit_waking(g as u128, hyper_log::Class::Normal, u, waker)
                    .unwrap(),
            )
        })
        .collect();
    drop(ready);
    for _ in 0..GROUPS {
        let g = woken.recv().unwrap();
        let pending = out[g].take().expect("a waker woken twice");
        pending.poll().unwrap().unwrap();
    }
    for g in 0..GROUPS {
        assert_eq!(log.view(g as u128).unwrap().unwrap().last, 1);
    }
    // Every waker went with its submission, and none woke again, before or after the log
    // closed and dropped whatever it held.
    drop(log);
    assert!(woken.try_recv().is_err());
    for (g, slot) in slots.iter().enumerate() {
        assert_eq!(slot.wakes(), 1, "waker {g}");
    }
}

/// A waking submitter hears everything through its waker, its admission included: the call
/// returns once the submission is on its way, without waiting for the owner to admit it. With
/// the queue's one submission held in a flush, the waking submission waits in the log for room
/// while its caller goes on; once the flush is let through, it is admitted, written and
/// answered, and its waker woken once. The call once waited for its admission, a round trip to
/// the owner that woke the caller for every submission (`docs/benchmarks.md`, "Many small
/// appends"); here it would wait for room behind the held flush for good.
#[test]
fn a_waking_submitter_does_not_wait_for_its_admission() {
    let (device, holder) = held_telling(sim(52));
    let mut cfg = config(16, 8);
    cfg.queue_submissions = 1;
    let log = Log::create(device, cfg, ID).unwrap();
    holder.hold();
    let _released = holder.released();
    let first = log
        .submit(
            1,
            Update {
                entries: Some(entries(1, &[1])),
                ..Update::default()
            },
        )
        .unwrap();
    holder.held();
    let waking = log
        .submit_waking(
            2,
            Class::Normal,
            Update {
                entries: Some(entries(1, &[2])),
                ..Update::default()
            },
            holder.waker(0),
        )
        .unwrap();
    assert!(waking.poll().is_none(), "answered while its room is held");
    holder.release();
    first.wait().unwrap();
    assert!(!holder.held_or_told(0), "the waker is told");
    waking.poll().expect("woken before its answer").unwrap();
    assert_eq!(log.view(2).unwrap().unwrap().last, 1);
}

/// Many submitters, each with a waker, keep updates out across many frames: an answer wakes
/// its own submitter's waker and no other's, once (mantle note 26 §5.3, note 32 L-2).
#[test]
fn a_completion_wakes_only_its_submitter() {
    const GROUPS: usize = 48;
    const ROUNDS: usize = 12;
    let log = Log::create(sim(24), config(64, 8), ID).unwrap();
    let (ready, woken) = std::sync::mpsc::sync_channel(GROUPS);
    let wakers: Vec<_> = (0..GROUPS)
        .map(|g| hyper_measure::wake::waker(g, ready.clone()))
        .collect();
    drop(ready);
    let submit = |g: usize, round: usize| {
        let u = Update {
            entries: Some(entries(round as u64 + 1, &[1])),
            ..Update::default()
        };
        log.submit_waking(g as u128, Class::Normal, u, wakers[g].0.clone())
            .unwrap()
    };
    let mut out: Vec<Option<hyper_log::Pending>> =
        (0..GROUPS).map(|g| Some(submit(g, 0))).collect();
    let mut answered = vec![0usize; GROUPS];
    let mut total = 0usize;
    while total < GROUPS * ROUNDS {
        let g = woken.recv().unwrap();
        let pending = out[g].take().expect("woken with no submission out");
        pending.poll().expect("woken before its answer").unwrap();
        answered[g] += 1;
        total += 1;
        // A waker woken for another's answer would have been woken before its own came.
        assert_eq!(wakers[g].1.wakes() as usize, answered[g], "waker {g}");
        if answered[g] < ROUNDS {
            out[g] = Some(submit(g, answered[g]));
        }
    }
    drop(log);
    for (g, (_, slot)) in wakers.iter().enumerate() {
        assert_eq!(slot.wakes() as usize, ROUNDS, "waker {g}");
    }
    assert!(woken.try_recv().is_err(), "a wake with no answer");
}

/// A file that tells which thread flushed it.
struct Flushing {
    file: SimFile,
    flushed: std::sync::mpsc::SyncSender<std::thread::ThreadId>,
}

impl BlockFile for Flushing {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }

    fn len(&self) -> Result<u64, hyper_block::DiskError> {
        self.file.len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), hyper_block::DiskError> {
        self.file.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), hyper_block::DiskError> {
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), hyper_block::DiskError> {
        let _ = self.flushed.try_send(std::thread::current().id());
        self.file.sync_data()
    }
}

/// A blocking write's frame is written, flushed and confirmed on its caller's thread, as
/// mantle's writer did on its own; a submission whose caller does not wait on it is done by the
/// log's I/O thread.
#[test]
fn a_blocking_writer_flushes_its_own_frame() {
    let (flushed, flushes) = std::sync::mpsc::sync_channel(1 << 10);
    let file = Flushing {
        file: sim(49),
        flushed,
    };
    let log = Log::create(file, config(16, 8), ID).unwrap();
    while flushes.try_recv().is_ok() {}
    let me = std::thread::current().id();
    for term in 1..=4 {
        log.write(
            1,
            Update {
                hard_state: Some(hard(term, 0)),
                ..Update::default()
            },
        )
        .unwrap();
        let threads: Vec<_> = flushes.try_iter().collect();
        // The frame's flush, then its confirmation's.
        assert_eq!(threads, [me, me], "write {term}");
    }
    log.submit(
        1,
        Update {
            hard_state: Some(hard(5, 0)),
            ..Update::default()
        },
    )
    .unwrap()
    .wait()
    .unwrap();
    let threads: Vec<_> = flushes.try_iter().collect();
    assert_eq!(threads.len(), 2);
    assert!(threads.iter().all(|t| *t != me), "{threads:?}");
}

/// A frame that confirms itself comes back to the owner waking no one. A submission the log took
/// while that confirmation was written waits on the frame to come back, and the owner, having
/// asked the I/O thread to wake it then, writes and answers it.
#[test]
fn a_submission_taken_while_a_frame_confirms_itself_is_written() {
    let (device, holder) = held(sim(50));
    let log = Log::create(device, config(16, 8), ID).unwrap();
    let small = |term| Update {
        hard_state: Some(hard(term, 0)),
        ..Update::default()
    };
    std::thread::scope(|s| {
        holder.hold();
        let _released = holder.released();
        let writer = s.spawn(|| log.write(1, small(1)));
        // The frame's flush; once it is let through with nothing submitted, the device confirms
        // the frame on its own, and that flush is held.
        holder.held();
        holder.step();
        let taken = log.submit(2, small(1)).unwrap();
        holder.release();
        writer.join().unwrap().unwrap();
        taken.wait().unwrap();
    });
    assert_eq!(log.view(2).unwrap().unwrap().hard_state, Some(hard(1, 0)));
}

/// A caller hears its answer only once the room its update held in the queue is given back,
/// whether the log refused the update or wrote it, and whichever thread gave the answer: a caller
/// that submits again at once, into a queue with room for one, is never refused for the room its
/// own answered update held (focal `5219002` found its writer answering first).
#[test]
fn room_is_given_back_before_the_answer() {
    let mut cfg = config(16, 8);
    cfg.queue_submissions = 1;
    let log = Log::create(sim(51), cfg, ID).unwrap();
    for i in 1..=100u64 {
        // Written, and answered by this thread, which did the frame's I/O.
        log.write(
            1,
            Update {
                entries: Some(entries(i, &[1])),
                ..Update::default()
            },
        )
        .unwrap();
        // At once, with no room to wait for: written, and answered by the log's I/O thread.
        log.submit(
            1,
            Update {
                hard_state: Some(hard(1, i)),
                ..Update::default()
            },
        )
        .unwrap()
        .wait()
        .unwrap();
        // Refused as the frame is laid out, by the owner.
        let gap = Update {
            entries: Some(entries(i + 2, &[1])),
            ..Update::default()
        };
        assert!(matches!(log.write(1, gap), Err(LogError::Invalid { .. })));
        let gap = Update {
            entries: Some(entries(i + 2, &[1])),
            ..Update::default()
        };
        assert!(matches!(
            log.submit(1, gap).unwrap().wait(),
            Err(LogError::Invalid { .. })
        ));
    }
    assert_eq!(log.view(1).unwrap().unwrap().last, 100);
}

/// A handle's writes apply in the order sent, refusals included: a write sent before the handle
/// took the refusal of an earlier one is refused too, `Behind`, changing nothing; once the
/// handle has taken the refusals, what it sends is written. Without the rule, a hard state sent
/// behind entries refused for the group's bound was written: the group then stated a commit
/// past the entries it held (hyper-raft docs/durable.md §2.4, I7).
#[test]
fn a_write_sent_behind_a_refused_one_is_refused_too() {
    let mut small = config(16, 8);
    small.group_entries = 2;
    let log = Log::create(sim(44), small, ID).unwrap();
    let mut group = log.group(1).unwrap();
    group
        .write(Update {
            entries: Some(entries(1, &[1])),
            ..Update::default()
        })
        .unwrap();
    // Past the group's bound of two entries: refused for room.
    group
        .submit(Update {
            entries: Some(entries(2, &[1, 1])),
            ..Update::default()
        })
        .unwrap();
    // Sent before the refusal is taken, with a commit that names the refused entries.
    group
        .submit(Update {
            hard_state: Some(hard(1, 3)),
            ..Update::default()
        })
        .unwrap();
    assert!(matches!(group.wait(), Some(Err(LogError::Backlog(1)))));
    assert!(matches!(group.wait(), Some(Err(LogError::Behind(1)))));
    let view = group.view().unwrap().unwrap();
    assert_eq!((view.last, view.hard_state), (1, None));
    // Sent after both refusals were taken: written.
    group
        .write(Update {
            entries: Some(entries(2, &[1])),
            hard_state: Some(hard(1, 2)),
            ..Update::default()
        })
        .unwrap();
    let view = log.view(1).unwrap().unwrap();
    assert_eq!((view.last, view.hard_state), (2, Some(hard(1, 2))));
}
