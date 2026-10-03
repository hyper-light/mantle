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
