//! The shell on hyper-log's group handle (`GroupStore`), the first store: a write larger than a
//! frame goes in parts and is durable whole; a write the log refuses for the group's retained
//! bound stalls the replica, the writes sent behind it are refused too (`LogError::Behind`), and
//! a compaction frees the room the refused writes are made again in, in order.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity
)]

mod support;

use std::sync::mpsc::{Receiver, sync_channel};
use std::task::Waker;

use hyper_block::sim::SimFile;
use hyper_durable::{GroupStore, LogStore, Output, Replica, Unbounded};
use hyper_log::{Config, Log, Waits};
use hyper_raft::proto::ConfState;
use support::Kv;
use support::cluster::settings;
use support::device::sim_file;

type Member = Replica<GroupStore<SimFile>, Kv, Unbounded>;

fn config(segment_blocks: u64, group_entries: u64) -> Config {
    Config {
        segment_bytes: segment_blocks * 4096,
        max_segments: 64,
        max_groups: 4,
        group_entries,
        group_bytes: 1 << 24,
        group_cache: 1 << 20,
        queue_submissions: 64,
        waits: Waits::Never,
    }
}

fn sole(log: &Log<SimFile>) -> (Member, Waker, Receiver<usize>) {
    let store = GroupStore::claim(log, 1).unwrap();
    let kv = Kv::new(
        ConfState {
            voters: vec![1],
            ..ConfState::default()
        },
        false,
    );
    let mut r = Replica::open(&settings(1, 3), store, kv, Unbounded).unwrap();
    let (tell, woken) = sync_channel(1024);
    let (waker, _) = hyper_measure::wake::waker(0, tell);
    r.campaign().unwrap();
    (r, waker, woken)
}

/// Drives until nothing is out and nothing more to do, waiting on the log's answers.
fn settle(r: &mut Member, waker: &Waker, woken: &Receiver<usize>) {
    let mut out = Output::default();
    loop {
        out.clear();
        let driven = r.drive(now(), waker, &mut out).unwrap();
        if driven.more {
            continue;
        }
        if r.in_flight() == 0 {
            return;
        }
        woken.recv().unwrap();
    }
}

#[test]
fn a_write_larger_than_a_frame_goes_in_parts_and_is_durable_whole() {
    let log = Log::create(sim_file(21), config(16, 1 << 12), 1).unwrap();
    let (mut r, waker, woken) = sole(&log);
    settle(&mut r, &waker, &woken);
    assert!(r.is_leader());
    let room = log.frame_room().unwrap();
    let entry = 4096;
    let count = 3 * room / entry + 1;
    for i in 0..count {
        r.propose(Vec::new(), vec![i as u8; entry]).unwrap();
    }
    let before = log.flushed().0;
    settle(&mut r, &waker, &woken);
    assert_eq!(r.machine().now.entries.len(), count + 1);
    assert!(
        log.flushed().0 - before >= 3,
        "one frame took more than a frame holds"
    );
    let view = r.core().store().log().view().unwrap();
    assert_eq!(view.last, count as u64 + 1);
    assert_eq!(view.hard_state.commit, count as u64 + 1);
}

#[test]
fn a_refusal_for_the_groups_bound_stalls_until_a_compaction_frees_it() {
    let bound = 16;
    let log = Log::create(sim_file(22), config(16, bound), 1).unwrap();
    let (mut r, waker, woken) = sole(&log);
    settle(&mut r, &waker, &woken);
    let mut stalled = 0;
    let mut out = Output::default();
    for i in 0..(3 * bound) {
        match r.propose(Vec::new(), i.to_le_bytes().to_vec()) {
            Ok(()) => {}
            Err(hyper_durable::ReplicaError::Stalled) => {}
            Err(e) => panic!("{e}"),
        }
        out.clear();
        r.drive(now(), &waker, &mut out).unwrap();
        settle(&mut r, &waker, &woken);
        if r.is_stalled() {
            stalled += 1;
            assert!(r.compact(2, now(), &waker).unwrap());
            settle(&mut r, &waker, &woken);
            assert!(!r.is_stalled(), "a compaction did not free the room");
        }
    }
    assert!(stalled > 0, "the group's bound never refused a write");
    let entries = &r.machine().now.entries;
    assert!(
        entries.windows(2).all(|w| w[1].0 == w[0].0 + 1),
        "entries applied out of order"
    );
    let view = r.core().store().log().view().unwrap();
    assert!(view.hard_state.commit <= view.last);
    assert_eq!(view.last, r.applied().index);
}

/// An owner with nothing else to do for its group waits for the log's answer to the oldest write
/// out, whole, however many parts it went in (`GroupStore::wait`), and its next drive takes it,
/// though the write's waker wakes no one; with nothing out there is nothing to wait for.
#[test]
fn an_owner_waits_for_its_oldest_write_whole_and_the_next_drive_takes_it() {
    let log = Log::create(sim_file(23), config(16, 1 << 12), 1).unwrap();
    let (mut r, waker, woken) = sole(&log);
    settle(&mut r, &waker, &woken);
    assert!(!r.log_mut().wait(), "nothing out");
    let room = log.frame_room().unwrap();
    let entry = 4096;
    let count = 2 * room / entry + 1;
    for i in 0..count {
        r.propose(Vec::new(), vec![i as u8; entry]).unwrap();
    }
    let mut out = Output::default();
    r.drive(now(), Waker::noop(), &mut out).unwrap();
    assert_eq!(r.in_flight(), 1);
    let before = log.flushed().0;
    assert!(r.log_mut().wait());
    assert!(log.flushed().0 - before >= 2, "the write went in parts");
    out.clear();
    r.drive(now(), Waker::noop(), &mut out).unwrap();
    let last = count as u64 + 1;
    assert_eq!(r.logged_commit(), last, "the drive took the write's answer");
    // What it committed is applied a page a drive, each drive one entry at least.
    for _ in 0..count {
        if r.applied().index == last {
            break;
        }
        out.clear();
        r.drive(now(), Waker::noop(), &mut out).unwrap();
    }
    assert_eq!(r.machine().now.entries.len(), count + 1);
    let view = r.core().store().log().view().unwrap();
    assert_eq!(view.hard_state.commit, last);
}

/// The owner's clock in nanoseconds, simulated: each reading a nanosecond after the one before, so
/// time only moves forward, as an owner's monotonic clock does, and every run reads the same times.
/// No test here waits on elapsed time; those that judge time state it outright.
fn now() -> u64 {
    thread_local! {
        static CLOCK: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }
    CLOCK.with(|clock| {
        let now = clock.get() + 1;
        clock.set(now);
        now
    })
}

/// Group commit above the log (focal's finding, 2026-10-05): N proposals made before one drive
/// take one write, so one frame and one flush. The shell never waits on a flush per proposal: what
/// the core holds not yet durable goes out as one write when the replica is driven.
#[test]
fn proposals_made_before_one_drive_take_one_frame() {
    let log = Log::create(sim_file(24), config(16, 1 << 12), 1).unwrap();
    let (mut r, waker, woken) = sole(&log);
    settle(&mut r, &waker, &woken);
    let n = 64;
    for i in 0..n {
        r.propose(Vec::new(), vec![i as u8; 64]).unwrap();
    }
    let (frames, updates) = log.flushed();
    settle(&mut r, &waker, &woken);
    let (frames_after, updates_after) = log.flushed();
    assert_eq!(
        frames_after - frames,
        1,
        "the proposals took more than one frame"
    );
    assert_eq!(
        updates_after - updates,
        1,
        "the proposals took more than one write"
    );
    assert_eq!(r.machine().now.entries.len(), n + 1);
}

/// Two groups on one log, driven one after the other: neither drive waits on the log, so both
/// writes are out before either is answered, for the log to take together. That they then share
/// one frame is the log's own grouping (hyper-log's `many_submitters_share_flushes`); here the bound
/// is exact: two groups' writes take at most two frames.
#[test]
fn two_groups_driven_in_turn_both_have_their_write_out_before_either_waits() {
    let log = Log::create(sim_file(25), config(16, 1 << 12), 1).unwrap();
    let mut members = Vec::new();
    for group in [1u128, 2] {
        let store = GroupStore::claim(&log, group).unwrap();
        let kv = Kv::new(
            ConfState {
                voters: vec![1],
                ..ConfState::default()
            },
            false,
        );
        let mut r = Replica::open(&settings(1, 3), store, kv, Unbounded).unwrap();
        let (tell, woken) = sync_channel(1024);
        let (waker, _) = hyper_measure::wake::waker(0, tell);
        r.campaign().unwrap();
        settle(&mut r, &waker, &woken);
        members.push((r, waker, woken));
    }
    for (r, _, _) in &mut members {
        for i in 0..16u8 {
            r.propose(Vec::new(), vec![i; 64]).unwrap();
        }
    }
    let (frames, _) = log.flushed();
    let mut out = Output::default();
    for (r, waker, _) in &mut members {
        out.clear();
        r.drive(now(), waker, &mut out).unwrap();
    }
    for (r, _, _) in &members {
        assert_eq!(r.in_flight(), 1, "a drive waited on the log");
    }
    for (r, waker, woken) in &mut members {
        settle(r, waker, woken);
    }
    let (frames_after, _) = log.flushed();
    assert!(
        frames_after - frames <= 2,
        "two groups' writes took {} frames",
        frames_after - frames
    );
}

/// A voter holds a fast proposal above its log, and the leader's append reaches that index
/// before the write goes out: the one write carries the entry and the holding at its index. The
/// holding is the voter's vote, kept until a release (`docs/raft.md` §3.5), so the log takes it
/// with the entry that reached it rather than refusing the write.
#[test]
fn a_holding_the_same_write_reaches_by_an_append_is_kept() {
    use hyper_raft::proto::{Entry, Message, MessageType};
    let log = Log::create(sim_file(23), config(16, 1 << 12), 1).unwrap();
    let store = GroupStore::claim(&log, 1).unwrap();
    let kv = Kv::new(
        ConfState {
            voters: vec![1, 2, 3],
            ..ConfState::default()
        },
        false,
    );
    let mut s = settings(1, 3);
    s.core.fast = true;
    let mut r: Member = Replica::open(&s, store, kv, Unbounded).unwrap();
    let (tell, woken) = sync_channel(1024);
    let (waker, _) = hyper_measure::wake::waker(0, tell);
    r.step(Message {
        msg_type: MessageType::MsgHeartbeat,
        from: 2,
        to: 1,
        term: 1,
        ..Message::default()
    })
    .unwrap();
    settle(&mut r, &waker, &woken);
    let last = r.core().raft.log().last_index().unwrap();
    let at = last + 1;
    r.step(Message {
        msg_type: hyper_raft::fast::FAST_PROPOSE,
        from: 3,
        to: 1,
        term: 1,
        entries: vec![Entry {
            index: at,
            term: 1,
            data: b"held".to_vec(),
            ..Entry::default()
        }],
        ..Message::default()
    })
    .unwrap();
    let log_term = r.core().raft.log().term(last).unwrap();
    r.step(Message {
        msg_type: MessageType::MsgAppend,
        from: 2,
        to: 1,
        term: 1,
        index: last,
        log_term,
        entries: vec![Entry {
            index: at,
            term: 1,
            data: b"appended".to_vec(),
            ..Entry::default()
        }],
        ..Message::default()
    })
    .unwrap();
    settle(&mut r, &waker, &woken);
    let view = r.core().store().log().view().unwrap();
    assert_eq!(view.last, at);
    let initial = hyper_raft::Storage::initial_state(r.core().store()).unwrap();
    assert!(
        initial.proposals.iter().any(|p| p.index == at),
        "the holding is kept beside the entry that reached it"
    );
}
