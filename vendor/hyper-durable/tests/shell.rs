//! The shell's rules one at a time, and every bound of `docs/durable.md` §6 at its edge: the
//! writes out, the entries behind the fence, the reads, the snapshot reports kept while
//! stalled, the budget, the arena; what opening repairs (§4.3); the unwind boundary; and the
//! threads an owner's replicas cost, which do not grow with them.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity,
    clippy::unreachable,
    clippy::panic_in_result_fn
)]

mod support;

use std::task::Waker;

use hyper_durable::{
    Budget, Bytes, Cause, EntryRef, Fatal, Fault, LogStore, OpenError, Output, Owner, Point,
    RamStore, Replica, ReplicaError, Settings, StateMachine, StoreView, Unbounded, Write,
};
use hyper_raft::StorageError;
use hyper_raft::proto::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState, Entry,
    EntryType, HardState, Message, MessageType,
};
use support::cluster::settings;
use support::{Kv, SimStore};

fn voters(ids: &[u64]) -> ConfState {
    ConfState {
        voters: ids.to_vec(),
        ..ConfState::default()
    }
}

fn waker() -> &'static Waker {
    Waker::noop()
}

type Sim<B = Unbounded> = Replica<SimStore, Kv, B>;

/// A sole voter on a store of `depth`, elected: it commits alone.
fn sole<B: Budget>(depth: usize, budget: B, tune: impl FnOnce(&mut Settings)) -> Sim<B> {
    let mut s = settings(1, 7);
    tune(&mut s);
    let mut r = Replica::open(
        &s,
        SimStore::new(depth),
        Kv::new(voters(&[1]), false),
        budget,
    )
    .unwrap();
    r.campaign().unwrap();
    pump(&mut r);
    assert!(r.is_leader());
    r
}

/// Elects member 1 of three by hand: it campaigns, and the test answers its pre-votes and then
/// its votes as if from the other two.
fn elect_by_hand(r: &mut Sim) {
    r.campaign().unwrap();
    for asked in [MessageType::MsgRequestPreVote, MessageType::MsgRequestVote] {
        let answer = match asked {
            MessageType::MsgRequestPreVote => MessageType::MsgRequestPreVoteResponse,
            _ => MessageType::MsgRequestVoteResponse,
        };
        let out = pump(r);
        for m in out.messages {
            if m.msg_type == asked {
                r.step(Message {
                    msg_type: answer,
                    from: m.to,
                    to: 1,
                    term: m.term,
                    ..Message::default()
                })
                .unwrap();
            }
        }
    }
    pump(r);
    assert!(r.is_leader());
}

/// Drives `r` and makes every write durable until nothing is out and nothing more to do.
fn pump<B: Budget>(r: &mut Sim<B>) -> Output<(u64, Vec<u8>)> {
    let mut all = Output::default();
    let mut out = Output::default();
    for _ in 0..10_000 {
        out.clear();
        let driven = r.drive(now(), waker(), &mut out).unwrap();
        all.messages.append(&mut out.messages);
        all.answers.append(&mut out.answers);
        all.reads.append(&mut out.reads);
        let mut made = false;
        while r.log_mut().make_durable() {
            made = true;
        }
        if !made && !driven.more && driven.out == 0 && r.log_mut().unanswered() == 0 {
            return all;
        }
    }
    panic!("the replica never rested");
}

fn change(kind: ConfChangeType, node_id: u64) -> ConfChangeV2 {
    ConfChangeV2 {
        transition: ConfChangeTransition::Auto,
        changes: vec![ConfChangeSingle {
            change_type: kind,
            node_id,
        }],
        context: Vec::new(),
    }
}

/// The core takes `Ready`s ahead of their persistence up to the store's depth, and the shell
/// keeps at most one write of its own beside them; past them it takes none, and nothing
/// refuses: the proposals wait in the core.
#[test]
fn the_writes_out_never_pass_the_stores_depth() {
    for depth in [1, 2, 3] {
        let mut r = sole(depth, Unbounded, |_| {});
        let mut out = Output::default();
        let mut deepest = 0;
        for i in 0..64u64 {
            r.propose(Vec::new(), i.to_le_bytes().to_vec()).unwrap();
            out.clear();
            r.drive(now(), waker(), &mut out).unwrap();
            deepest = deepest.max(r.in_flight());
            assert!(r.core().in_flight() <= depth, "depth {depth}");
            assert!(r.in_flight() <= depth + 1, "depth {depth}");
        }
        assert_eq!(deepest, depth, "depth {depth}: the pipeline never filled");
        pump(&mut r);
        assert_eq!(r.machine().now.entries.len(), 65, "depth {depth}");
    }
}

/// A sole voter states the commit of its own entries in the write that holds them (focal F17's
/// `sole_commit`): once that write is durable the commit is logged, with no write of its own.
#[test]
fn a_sole_voter_logs_its_commit_in_the_write_of_its_entries() {
    let mut r = sole(3, Unbounded, |_| {});
    let submits = r.log_mut().events.submits;
    r.propose(Vec::new(), b"x".to_vec()).unwrap();
    pump(&mut r);
    let last = r.applied().index;
    assert_eq!(r.log_mut().disk.hard.commit, last);
    assert_eq!(
        r.log_mut().events.submits,
        submits + 1,
        "a write of its own for the commit"
    );
    // Many entries, one write each, and never a write of the commit alone (§4.1).
    let before = r.writes();
    for i in 0..32u64 {
        r.propose(Vec::new(), i.to_le_bytes().to_vec()).unwrap();
        pump(&mut r);
    }
    let after = r.writes();
    assert_eq!(after.readies - before.readies, 32);
    assert_eq!((after.fenced, after.quiet), (before.fenced, before.quiet));
}

/// A change waits behind the fence until a write states its commit; entries committed after it
/// wait with it, at most a page from each write out and the page of the `Ready` that gave it
/// (`docs/durable.md` §6); the replica takes no `Ready` meanwhile.
#[test]
fn a_change_waits_behind_the_fence_and_what_waits_is_bounded() {
    // A member that does not decide alone: three voters of whom two are elsewhere, driven by
    // hand with acknowledgements sent as if from them.
    let depth = 3;
    let page = 256u64;
    let mut s = settings(1, 9);
    s.core.max_committed_size_per_ready = page;
    let mut r: Sim = Replica::open(
        &s,
        SimStore::new(depth),
        Kv::new(voters(&[1, 2, 3]), false),
        Unbounded,
    )
    .unwrap();
    elect_by_hand(&mut r);
    let ack = |r: &mut Sim| {
        let index = r.core().raft.log().last_index().unwrap();
        let term = r.term();
        r.step(Message {
            msg_type: MessageType::MsgAppendResponse,
            from: 2,
            to: 1,
            term,
            index,
            ..Message::default()
        })
        .unwrap();
    };
    ack(&mut r);
    pump(&mut r);
    // The change, then entries behind it, all committed by member 2's acknowledgement while
    // the leader's own writes are out.
    r.change(Vec::new(), &change(ConfChangeType::AddLearnerNode, 4))
        .unwrap();
    for i in 0..40u64 {
        r.propose(Vec::new(), vec![i as u8; 32]).unwrap();
        let mut out = Output::default();
        r.drive(now(), waker(), &mut out).unwrap();
    }
    let mut held = None;
    for _ in 0..200 {
        ack(&mut r);
        let mut out = Output::default();
        r.drive(now(), waker(), &mut out).unwrap();
        if let Some(range) = r.behind_fence() {
            held = Some(range);
            let entry = 32 + hyper_raft::wire::ENTRY_FIXED_BYTES as u64;
            let bytes = (range.1 - range.0) * entry;
            assert!(
                bytes <= (depth as u64 + 1) * (page + entry),
                "{range:?}: {bytes} bytes behind the fence"
            );
            assert!(
                r.configuration().learners.is_empty(),
                "applied behind the fence"
            );
            assert!(!r.core().has_ready() || r.in_flight() > 0 || r.log_mut().pending() > 0);
        }
        r.log_mut().make_durable();
    }
    assert!(held.is_some(), "nothing waited behind the fence");
    pump(&mut r);
    assert_eq!(r.configuration().learners, vec![4]);
    assert!(r.durable_commit() >= r.applied().index);
}

/// A write refused for room stalls the replica: inputs are refused `Stalled`, its campaigns held
/// whatever its detectors say, snapshot reports kept (one a member, members only), and the
/// refused writes are made again, in order, once the log has room; nothing was lost.
#[test]
fn a_write_refused_for_room_stalls_the_replica_until_it_is_made_again() {
    let mut r = sole(3, Unbounded, |s| s.core.limits.pending_reads = 8);
    for i in 0..3u64 {
        r.propose(Vec::new(), i.to_le_bytes().to_vec()).unwrap();
        let mut out = Output::default();
        r.drive(now(), waker(), &mut out).unwrap();
    }
    r.log_mut().refuse = Some(Fault::Room("the group's retained bound"));
    r.log_mut().make_durable();
    r.log_mut().full = true;
    let mut out = Output::default();
    let driven = r.drive(now(), waker(), &mut out).unwrap();
    assert!(r.is_stalled());
    assert_eq!(
        driven.stalled,
        Some(Fault::Room("the group's retained bound"))
    );
    assert!(out.messages.is_empty() && out.answers.is_empty());
    assert_eq!(
        r.propose(Vec::new(), b"no".to_vec()),
        Err(ReplicaError::Stalled)
    );
    assert_eq!(r.step(Message::default()), Err(ReplicaError::Stalled));
    assert_eq!(r.read(b"r".to_vec()), Err(ReplicaError::Stalled));
    assert_eq!(r.campaign(), Err(ReplicaError::Stalled));
    let term = r.term();
    r.suspect(2).unwrap();
    for _ in 0..100 {
        let mut out = Output::default();
        let driven = r.drive(now(), waker(), &mut out).unwrap();
        assert_eq!(driven.wake, None, "a stalled member is due for nothing");
    }
    assert_eq!(r.term(), term, "a stalled member campaigned");
    for member in [1, 9, 1] {
        r.report_snapshot(member, true).unwrap();
    }
    r.log_mut().full = false;
    // Nothing is made again until room may have been freed: the owner says so.
    let mut out = Output::default();
    r.drive(now(), waker(), &mut out).unwrap();
    assert!(r.is_stalled());
    r.resume();
    pump(&mut r);
    assert!(!r.is_stalled());
    assert_eq!(r.machine().now.entries.len(), 4);
    r.propose(Vec::new(), b"after".to_vec()).unwrap();
    pump(&mut r);
    assert_eq!(r.machine().now.entries.len(), 5);
}

/// A write that held the fast track's proposals, refused for room, is made again with them once
/// the log has room: the core keeps the proposals of every write issued until its notice
/// (`RawNode::issued_proposals`). The replica was fenced here before, so a full log cost a fast
/// group its member.
#[test]
fn a_write_of_fast_proposals_refused_for_room_is_made_again_with_them() {
    let mut s = settings(1, 7);
    s.core.fast = true;
    let mut r: Sim = Replica::open(
        &s,
        SimStore::new(3),
        Kv::new(voters(&[1, 2, 3]), false),
        Unbounded,
    )
    .unwrap();
    // Member 2 leads term 1: a member proposes by the fast track only knowing a leader.
    r.step(Message {
        msg_type: MessageType::MsgHeartbeat,
        from: 2,
        to: 1,
        term: 1,
        ..Message::default()
    })
    .unwrap();
    pump(&mut r);
    let index = r.propose_fast(Vec::new(), b"held".to_vec()).unwrap();
    let mut out = Output::default();
    r.drive(now(), waker(), &mut out).unwrap();
    assert!(
        r.core().issued_proposals().any(|p| p.index == index),
        "the write out holds the proposal"
    );
    r.log_mut().refuse = Some(Fault::Room("the group's retained bound"));
    r.log_mut().make_durable();
    r.log_mut().full = true;
    out.clear();
    r.drive(now(), waker(), &mut out).unwrap();
    assert!(r.is_stalled());
    assert!(
        r.log_mut().disk.proposals.iter().all(|p| p.index != index),
        "the refused write left nothing"
    );
    r.log_mut().full = false;
    r.resume();
    pump(&mut r);
    assert_eq!(r.fenced(), None);
    assert!(!r.is_stalled());
    assert!(
        r.log_mut()
            .disk
            .proposals
            .iter()
            .any(|p| p.index == index && p.data == b"held"),
        "made again, the write holds the proposal"
    );
    assert_eq!(r.core().issued_proposals().count(), 0);
}

/// Reads past the core's bound are refused, counting those the replica holds for its apply.
#[test]
fn reads_past_the_bound_are_refused() {
    let mut r = sole(1, Unbounded, |s| s.core.limits.pending_reads = 4);
    for i in 0..4u8 {
        r.read(vec![i]).unwrap();
    }
    assert!(matches!(
        r.read(vec![9]),
        Err(ReplicaError::Refused(hyper_raft::Error::Capacity(_)))
    ));
    let out = pump(&mut r);
    assert_eq!(out.reads.len(), 4);
    r.read(vec![10]).unwrap();
}

/// The budget refuses an input it cannot hold, and nothing changes; what it holds follows the
/// replica's resident bytes.
#[test]
fn the_budget_refuses_what_it_cannot_hold_and_follows_what_is_held() {
    let mut r = sole(2, Bytes::new(1 << 20), |_| {});
    let held = r.charged();
    assert!(held > 0);
    let last = r.core().raft.log().last_index().unwrap();
    assert!(matches!(
        r.propose(Vec::new(), vec![0; 2 << 20]),
        Err(ReplicaError::Budget(_))
    ));
    assert_eq!(r.core().raft.log().last_index().unwrap(), last);
    r.propose(Vec::new(), vec![0; 1024]).unwrap();
    pump(&mut r);
    assert_eq!(r.machine().now.entries.len(), 2);
}

/// A state machine that unwinds on an entry.
struct Boom(Kv);
impl StateMachine for Boom {
    type Answer = (u64, Vec<u8>);
    fn apply(
        &mut self,
        entry: &EntryRef<'_>,
        answers: &mut Vec<Self::Answer>,
    ) -> Result<(), Fatal> {
        if entry.data == b"boom" {
            panic!("the application unwound");
        }
        self.0.apply(entry, answers)
    }
    fn apply_change(
        &mut self,
        at: Point,
        change: &ConfChangeV2,
        c: &ConfState,
    ) -> Result<(), Fatal> {
        self.0.apply_change(at, change, c)
    }
    fn durable(&self) -> Point {
        self.0.durable()
    }
    fn configuration(&self) -> &ConfState {
        self.0.configuration()
    }
    fn acts_at_start(&self, entry: &EntryRef<'_>) -> bool {
        self.0.acts_at_start(entry)
    }
    fn image(&mut self, into: &mut Vec<u8>) -> Result<(Point, ConfState), Fatal> {
        self.0.image(into)
    }
    fn install(&mut self, image: &[u8], at: Point, c: &ConfState) -> Result<(), Fatal> {
        self.0.install(image, at, c)
    }
    fn persist(&mut self) -> Result<(), Fatal> {
        self.0.persist()
    }
}

/// An unwind of the state machine fences the replica and is reported, never propagated.
#[test]
fn an_unwind_inside_a_call_fences_the_replica() {
    let mut r = Replica::open(
        &settings(1, 3),
        RamStore::new(),
        Boom(Kv::new(voters(&[1]), false)),
        Unbounded,
    )
    .unwrap();
    r.campaign().unwrap();
    let mut out = Output::default();
    for _ in 0..8 {
        r.drive(now(), waker(), &mut out).unwrap();
    }
    r.propose(Vec::new(), b"boom".to_vec()).unwrap();
    let mut fenced = None;
    for _ in 0..8 {
        if let Err(e) = r.drive(now(), waker(), &mut out) {
            fenced = Some(e);
            break;
        }
    }
    assert_eq!(fenced, Some(ReplicaError::Fenced(Cause::Unwound)));
    assert_eq!(r.suspect(2), Err(ReplicaError::Fenced(Cause::Unwound)));
}

fn entries(first: u64, terms: &[u64]) -> Vec<Entry> {
    terms
        .iter()
        .enumerate()
        .map(|(i, &term)| Entry {
            index: first + i as u64,
            term,
            data: vec![1],
            ..Entry::default()
        })
        .collect()
}

fn store_with(entries: &[Entry], commit: u64) -> RamStore {
    let mut store = RamStore::new();
    store
        .write_now(&Write {
            entries: Some(hyper_durable::Entries { first: 1, entries }),
            hard_state: Some(HardState {
                term: 2,
                vote: 1,
                commit,
            }),
            ..Write::default()
        })
        .unwrap();
    store
}

fn machine_at(point: Point) -> Kv {
    let mut kv = Kv::new(voters(&[1, 2, 3]), false);
    kv.now.applied = point;
    kv.durable.applied = point;
    kv
}

/// §4.3, case 1: a snapshot the state machine installed and the log never recorded: the log
/// starts there, holds nothing past it, and records it committed.
#[test]
fn an_install_the_log_never_recorded_is_finished_at_open() {
    let store = store_with(&entries(1, &[1, 1, 2, 2, 2]), 3);
    let r: Replica<RamStore, Kv> = Replica::open(
        &settings(1, 1),
        store,
        machine_at(Point { index: 8, term: 2 }),
        Unbounded,
    )
    .unwrap();
    let view = r.core().store().log().view().unwrap();
    assert_eq!((view.start, view.last), (Point { index: 8, term: 2 }, 8));
    assert_eq!(view.hard_state.commit, 8);
    assert_eq!(view.hard_state.term, 2, "the term and vote are kept");
}

/// §4.3, case 2: a state machine past the log's commit, within its entries: the commit rises to
/// it, and the entries stay.
#[test]
fn a_state_machine_past_the_logs_commit_raises_it() {
    let store = store_with(&entries(1, &[1, 1, 2, 2, 2]), 2);
    let r: Replica<RamStore, Kv> = Replica::open(
        &settings(1, 1),
        store,
        machine_at(Point { index: 4, term: 2 }),
        Unbounded,
    )
    .unwrap();
    let view = r.core().store().log().view().unwrap();
    assert_eq!(
        (view.start.index, view.last, view.hard_state.commit),
        (0, 5, 4)
    );
    assert_eq!(r.applied().index, 4);
}

/// §4.3, case 3: a state machine past the log's last entry (a lost last frame, or a leader that
/// applied before its own write was durable): the log starts at the state machine's point, whose
/// term the state machine reports.
#[test]
fn a_state_machine_past_the_logs_last_entry_moves_its_start() {
    let store = store_with(&entries(1, &[1, 1, 2]), 2);
    let r: Replica<RamStore, Kv> = Replica::open(
        &settings(1, 1),
        store,
        machine_at(Point { index: 5, term: 2 }),
        Unbounded,
    )
    .unwrap();
    let view = r.core().store().log().view().unwrap();
    assert_eq!(
        (view.start, view.last, view.hard_state.commit),
        (Point { index: 5, term: 2 }, 5, 5)
    );
}

/// I8: a log that starts past what the state machine holds durably does not open.
#[test]
fn a_log_that_starts_past_the_state_machine_does_not_open() {
    let mut store = store_with(&entries(1, &[1, 1, 2, 2, 2]), 5);
    store
        .write_now(&Write {
            start: Some(Point { index: 4, term: 2 }),
            ..Write::default()
        })
        .unwrap();
    let opened: Result<Replica<RamStore, Kv>, _> = Replica::open(
        &settings(1, 1),
        store,
        machine_at(Point { index: 2, term: 1 }),
        Unbounded,
    );
    assert!(matches!(
        opened,
        Err(OpenError::StartPastMachine {
            start: 4,
            machine: 2
        })
    ));
}

/// A store holding two entries of term 1, marked through 5 of term 1.
fn marked_store() -> SimStore {
    let mut store = SimStore::new(1);
    store
        .write_now(&Write {
            entries: Some(hyper_durable::Entries {
                first: 1,
                entries: &entries(1, &[1, 1]),
            }),
            hard_state: Some(HardState {
                term: 1,
                vote: 1,
                commit: 1,
            }),
            ..Write::default()
        })
        .unwrap();
    store.mark = Some(Point { index: 5, term: 1 });
    store
}

/// A marked member (its log may lack entries it acknowledged) of three voters refuses its vote
/// to a candidate behind its mark, and campaigns on its log (core step R-7): its requests name
/// its log's last entry, not the mark, and once its detectors suspect every peer its campaign is
/// timed.
#[test]
fn a_marked_member_campaigns_on_its_log_and_votes_by_its_mark() {
    let mut r: Sim = Replica::open(
        &settings(2, 1),
        marked_store(),
        Kv::new(voters(&[1, 2, 3]), false),
        Unbounded,
    )
    .unwrap();
    assert_eq!(r.mark(), Some(Point { index: 5, term: 1 }));
    r.step(Message {
        msg_type: MessageType::MsgRequestVote,
        from: 3,
        to: 2,
        term: 2,
        log_term: 1,
        index: 3,
        ..Message::default()
    })
    .unwrap();
    let out = pump(&mut r);
    assert!(
        out.messages
            .iter()
            .filter(|m| m.msg_type == MessageType::MsgRequestVoteResponse)
            .all(|m| m.reject),
        "{:?}",
        out.messages
    );
    assert_eq!(r.campaign(), Ok(()));
    let out = pump(&mut r);
    let asked: Vec<_> = out
        .messages
        .iter()
        .filter(|m| {
            matches!(
                m.msg_type,
                MessageType::MsgRequestVote | MessageType::MsgRequestPreVote
            )
        })
        .map(|m| (m.to, m.index, m.log_term))
        .collect();
    assert_eq!(asked, vec![(1, 2, 1), (3, 2, 1)]);
}

/// A marked member of two voters takes no part in elections: the other is no quorum without
/// it, so it refuses to campaign and sends no request for votes, though it opened knowing no
/// leader and its detectors suspect its peer: the core holds its campaigns, and it is due for
/// nothing.
#[test]
fn a_marked_member_of_two_takes_no_part_in_elections() {
    let mut r: Sim = Replica::open(
        &settings(2, 1),
        marked_store(),
        Kv::new(voters(&[1, 2]), false),
        Unbounded,
    )
    .unwrap();
    assert_eq!(r.campaign(), Err(ReplicaError::Marked));
    r.set_timing(hyper_raft::Timing {
        span: std::time::Duration::from_millis(1),
        round: std::time::Duration::from_millis(1),
    })
    .unwrap();
    r.suspect(1).unwrap();
    let start = now();
    let mut out = Output::default();
    for step in 0..100u64 {
        out.clear();
        let at = start + step * 1_000_000;
        let driven = r.drive(at, waker(), &mut out).unwrap();
        assert_eq!(driven.wake, None, "a marked member is due for nothing");
    }
    let out = pump(&mut r);
    assert!(
        !out.messages.iter().any(|m| matches!(
            m.msg_type,
            MessageType::MsgRequestVote | MessageType::MsgRequestPreVote
        )),
        "{:?}",
        out.messages
    );
}

/// The flushes that made a term or vote durable feed hyper-timing's fold.
#[test]
fn a_votes_flush_is_folded() {
    let r = sole(2, Unbounded, |_| {});
    assert!(r.flushes().flushes() >= 1);
    assert!(r.last_durable().is_some());
}

/// The owner drives each queued replica once a turn, for one `Ready`, and queues again one with
/// more: every replica moves each turn, whatever the others hold. The arena refuses past its
/// slots, and a handle to an emptied slot finds nothing.
#[test]
fn the_owner_gives_each_replica_one_ready_a_turn() {
    let wakers: Vec<Waker> = (0..3).map(|_| Waker::noop().clone()).collect();
    let mut owner: Owner<RamStore, Kv, Unbounded> = Owner::new(wakers);
    let mut handles = Vec::new();
    for _ in 0..3 {
        let mut r = Replica::open(
            &settings(1, 5),
            RamStore::new(),
            Kv::new(voters(&[1]), false),
            Unbounded,
        )
        .unwrap();
        r.campaign().unwrap();
        handles.push(owner.insert(r).map_err(|_| ()).unwrap());
    }
    let extra = Replica::open(
        &settings(1, 5),
        RamStore::new(),
        Kv::new(voters(&[1]), false),
        Unbounded,
    )
    .unwrap();
    assert!(owner.insert(extra).is_err());
    let mut out = Output::default();
    for _ in 0..50 {
        owner.turn(now(), &mut out, |_, d, _| {
            d.unwrap();
        });
        for &h in &handles {
            owner.schedule(h);
        }
    }
    // One replica is given far more to do: each turn still drives every one once.
    for i in 0..200u64 {
        owner
            .get_mut(handles[0])
            .unwrap()
            .propose(Vec::new(), i.to_le_bytes().to_vec())
            .unwrap();
    }
    owner
        .get_mut(handles[1])
        .unwrap()
        .propose(Vec::new(), b"one".to_vec())
        .unwrap();
    for &h in &handles {
        owner.schedule(h);
    }
    let mut driven = [0u32; 3];
    for _ in 0..4 {
        owner.turn(now(), &mut out, |h, d, _| {
            d.unwrap();
            driven[h.slot()] += 1;
        });
        // A RamStore's answer wakes the slot; the noop waker cannot, so the test does.
        for &h in &handles {
            owner.woken(h.slot());
        }
    }
    assert!(driven.iter().all(|&d| d == 4), "{driven:?}");
    let applied: Vec<u64> = handles
        .iter()
        .map(|&h| owner.get(h).unwrap().applied().index)
        .collect();
    assert!(
        applied[1] > 1,
        "the light replica waited behind the heavy one: {applied:?}"
    );
    let removed = owner.remove(handles[2]).unwrap();
    drop(removed);
    assert!(owner.get(handles[2]).is_none());
    let again = Replica::open(
        &settings(1, 5),
        RamStore::new(),
        Kv::new(voters(&[1]), false),
        Unbounded,
    )
    .unwrap();
    let h = owner.insert(again).map_err(|_| ()).unwrap();
    assert_eq!(h.slot(), handles[2].slot());
    assert!(
        owner.get(handles[2]).is_none(),
        "a stale handle found the slot's new replica"
    );
}

/// A leader of three, elected by hand, whose followers' answers are stepped in by the test.
fn led(depth: usize, ahead: bool) -> Sim {
    let mut s = settings(1, 9);
    s.core.apply_unpersisted = ahead;
    let mut r: Sim = Replica::open(
        &s,
        SimStore::new(depth),
        Kv::new(voters(&[1, 2, 3]), false),
        Unbounded,
    )
    .unwrap();
    elect_by_hand(&mut r);
    r
}

fn acknowledge(r: &mut Sim, from: u64) {
    let index = r.core().raft.log().last_index().unwrap();
    let term = r.term();
    r.step(Message {
        msg_type: MessageType::MsgAppendResponse,
        from,
        to: 1,
        term,
        index,
        ..Message::default()
    })
    .unwrap();
}

/// §4.2 (core step R-6, `Config::apply_unpersisted`): a leader whose followers hold an entry of
/// its term before its own disk does applies it on the commit and answers its caller then, its
/// disk the slowest of the quorum; without it, the answer waits for its own write.
#[test]
fn a_leader_applies_its_own_term_before_its_write_is_durable() {
    for ahead in [false, true] {
        let mut r = led(3, ahead);
        r.propose(Vec::new(), b"fast".to_vec()).unwrap();
        let mut out = Output::default();
        r.drive(now(), waker(), &mut out).unwrap();
        acknowledge(&mut r, 2);
        acknowledge(&mut r, 3);
        out.clear();
        r.drive(now(), waker(), &mut out).unwrap();
        let answered = out.answers.iter().any(|(_, data)| data == b"fast");
        let disk = r.log_mut().disk.last();
        assert_eq!(
            answered,
            ahead,
            "ahead {ahead}: applied {:?}, disk through {disk}",
            r.applied()
        );
        if ahead {
            assert!(r.applied().index > disk);
        }
        pump(&mut r);
        assert!(r.machine().now.entries.iter().any(|(_, _, d)| d == b"fast"));
    }
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

/// What an entry's data says it needs: `NEEDS` and then the eight bytes of the precondition.
const NEEDS: &[u8] = b"needs";

/// A store that holds a write until its owner meets what the write's entries need, as focal's
/// store holds the first entry that needs a successor decoder until the group's record of that
/// floor is durable (focal 27 §15.5, O2). An entry whose data starts with [`NEEDS`] names its
/// precondition in the next eight bytes. While it holds a write, every write submitted behind it
/// is refused behind it, as hyper-log refuses a handle's writes sent after a refused one.
struct Holding {
    inner: SimStore,
    met: Vec<u64>,
    holding: Option<u64>,
}

impl Holding {
    fn new(depth: usize) -> Self {
        Self {
            inner: SimStore::new(depth),
            met: Vec::new(),
            holding: None,
        }
    }
    /// The first precondition `write`'s entries need that the owner has not met.
    fn needs(&self, write: &Write<'_>) -> Option<u64> {
        write
            .entries
            .iter()
            .flat_map(|e| e.entries)
            .filter_map(|e| e.data.strip_prefix(NEEDS))
            .filter_map(|rest| rest.get(..8))
            .map(|id| u64::from_le_bytes(id.try_into().unwrap()))
            .find(|id| !self.met.contains(id))
    }
}

impl LogStore for Holding {
    type Hold = u64;

    fn held(&self) -> Option<&u64> {
        self.holding.as_ref()
    }

    fn release(&mut self, met: &u64) {
        self.met.push(*met);
        if self.holding == Some(*met) {
            self.holding = None;
        }
    }

    fn depth(&self) -> usize {
        self.inner.depth()
    }

    fn view(&self) -> Result<StoreView, Fault> {
        self.inner.view()
    }

    fn bounds(&self) -> Result<(Point, u64), StorageError> {
        self.inner.bounds()
    }

    fn term(&self, index: u64) -> Result<u64, StorageError> {
        self.inner.term(index)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: &mut Vec<Entry>,
    ) -> Result<(), StorageError> {
        self.inner.entries(low, high, max_bytes, into)
    }

    fn visit(
        &self,
        low: u64,
        high: u64,
        page: u64,
        visit: &mut dyn FnMut(EntryRef<'_>) -> bool,
    ) -> Result<(), StorageError> {
        self.inner.visit(low, high, page, visit)
    }

    fn proposals(&self, into: &mut Vec<Entry>) -> Result<(), StorageError> {
        self.inner.proposals(into)
    }

    fn room(&self) -> bool {
        self.inner.room()
    }

    fn submit(&mut self, write: &Write<'_>, waker: &Waker) -> Result<(), Fault> {
        if self.holding.is_some() {
            return Err(Fault::Behind);
        }
        if let Some(needs) = self.needs(write) {
            self.holding = Some(needs);
            return Err(Fault::Held);
        }
        self.inner.submit(write, waker)
    }

    fn poll(&mut self) -> Option<Result<(), Fault>> {
        self.inner.poll()
    }

    fn write_now(&mut self, write: &Write<'_>) -> Result<(), Fault> {
        self.inner.write_now(write)
    }
}

/// Drives `r` and makes every write durable until nothing is out and nothing more to do, or it
/// stalls.
fn pump_holding(r: &mut Replica<Holding, Kv>) -> Output<(u64, Vec<u8>)> {
    let mut all = Output::default();
    let mut out = Output::default();
    for _ in 0..10_000 {
        out.clear();
        let driven = r.drive(now(), waker(), &mut out).unwrap();
        all.messages.append(&mut out.messages);
        all.answers.append(&mut out.answers);
        let mut made = false;
        while r.log_mut().inner.make_durable() {
            made = true;
        }
        if !made && !driven.more && driven.out == 0 && r.log_mut().inner.unanswered() == 0 {
            return all;
        }
    }
    panic!("the replica never rested");
}

fn needing(id: u64) -> Vec<u8> {
    let mut data = NEEDS.to_vec();
    data.extend_from_slice(&id.to_le_bytes());
    data
}

/// A write the store holds for its owner stalls the replica whole, as a refusal for room does,
/// but names what it waits for in the store's own type: inputs are refused `Stalled`, nothing
/// that depends on the write is answered, and it is counted once. Once the owner meets the
/// precondition the write is made again, and what it held is committed and applied.
#[test]
fn a_write_the_store_holds_waits_whole_until_its_owner_meets_what_it_holds_for() {
    let mut r = Replica::open(
        &settings(1, 7),
        Holding::new(1),
        Kv::new(voters(&[1]), false),
        Unbounded,
    )
    .unwrap();
    r.campaign().unwrap();
    pump_holding(&mut r);
    assert!(r.is_leader());
    r.propose(Vec::new(), b"plain".to_vec()).unwrap();
    pump_holding(&mut r);
    let applied = r.machine().now.applied.index;
    r.propose(Vec::new(), needing(7)).unwrap();
    // The drive that submits the write learns of the hold at once from the store; the next takes
    // the refusal, in order with the writes before it, and stalls.
    let mut out = Output::default();
    r.drive(now(), waker(), &mut out).unwrap();
    assert_eq!(r.held(), Some(&7));
    let driven = r.drive(now(), waker(), &mut out).unwrap();
    assert!(r.is_stalled());
    assert_eq!(driven.stalled, Some(Fault::Held));
    assert!(out.answers.is_empty());
    assert_eq!(
        r.propose(Vec::new(), b"no".to_vec()),
        Err(ReplicaError::Stalled)
    );
    for _ in 0..100 {
        let mut out = Output::default();
        let driven = r.drive(now(), waker(), &mut out).unwrap();
        assert_eq!(driven.stalled, Some(Fault::Held), "held until it is met");
        assert!(out.answers.is_empty() && out.messages.is_empty());
    }
    assert_eq!(
        r.machine().now.applied.index,
        applied,
        "nothing held was applied"
    );
    assert_eq!(r.writes().held, 1, "a held write is counted once");
    r.release(&7);
    let out = pump_holding(&mut r);
    assert!(!r.is_stalled() && r.held().is_none());
    assert_eq!(r.machine().now.applied.index, applied + 1);
    assert_eq!(
        out.answers.len(),
        1,
        "the held entry was answered once it was durable"
    );
    assert_eq!(r.writes().held, 1);
}

/// A member that stops after its owner met the precondition and before the held write was made
/// again reopens with nothing of it: the write never changed the store, so the member holds
/// what it held before it, and goes on from there.
#[test]
fn a_member_stopped_between_release_and_the_write_made_again_reopens_without_it() {
    let mut r = Replica::open(
        &settings(1, 7),
        Holding::new(1),
        Kv::new(voters(&[1]), false),
        Unbounded,
    )
    .unwrap();
    r.campaign().unwrap();
    pump_holding(&mut r);
    r.propose(Vec::new(), b"plain".to_vec()).unwrap();
    pump_holding(&mut r);
    let before = r.log_mut().inner.disk.clone();
    r.propose(Vec::new(), needing(9)).unwrap();
    let mut out = Output::default();
    r.drive(now(), waker(), &mut out).unwrap();
    assert_eq!(r.held(), Some(&9));
    r.release(&9);
    // Stopped here: the store keeps what was durable, which the held write never touched.
    let disk = r.log_mut().inner.disk.clone();
    assert_eq!(disk, before, "a held write changed nothing");
    drop(r);
    let mut again = Holding::new(1);
    again.inner = SimStore::from_disk(disk, 1);
    let mut r = Replica::open(
        &settings(1, 7),
        again,
        Kv::new(voters(&[1]), false),
        Unbounded,
    )
    .unwrap();
    r.campaign().unwrap();
    pump_holding(&mut r);
    assert!(r.is_leader());
    // The owner meets the precondition again, as it does whenever it learns what a write needs.
    r.propose(Vec::new(), needing(9)).unwrap();
    let mut out = Output::default();
    r.drive(now(), waker(), &mut out).unwrap();
    // Released before the replica took the refusal: the release is not lost.
    r.release(&9);
    pump_holding(&mut r);
    assert!(!r.is_stalled());
}

/// On ticks the replica elects by the owner's ticks and takes no detector's word, no timing and
/// no deadline: the two ways never mix. A stalled replica is not ticked, and the ticks it missed
/// are not given again.
#[test]
fn a_replica_on_ticks_elects_by_its_owners_ticks_and_hears_no_detector() {
    let mut s = settings(1, 7);
    s.elections = hyper_raft::Elections::Ticks;
    let election = s.core.election_tick;
    let mut r: Sim = Replica::open(
        &s,
        SimStore::new(1),
        Kv::new(voters(&[1, 2, 3]), false),
        Unbounded,
    )
    .unwrap();
    assert!(matches!(r.suspect(2), Err(ReplicaError::Refused(_))));
    assert!(matches!(r.trust(2), Err(ReplicaError::Refused(_))));
    assert!(matches!(r.restarted(2), Err(ReplicaError::Refused(_))));
    assert!(matches!(
        r.set_timing(hyper_raft::Timing {
            span: std::time::Duration::from_millis(100),
            round: std::time::Duration::from_millis(10),
        }),
        Err(ReplicaError::Refused(_))
    ));
    assert_eq!(r.deadline(), None);
    r.set_randomized_election_timeout(election).unwrap();
    assert!(
        matches!(
            r.set_randomized_election_timeout(election * 2),
            Err(ReplicaError::Refused(_))
        ),
        "a timeout outside one to two election ticks"
    );
    // One tick short of its timeout it asks nothing; the tick that reaches it campaigns.
    for _ in 1..election {
        assert!(!r.tick().unwrap());
        assert!(pump(&mut r).messages.is_empty());
    }
    assert!(r.tick().unwrap());
    let asked = pump(&mut r).messages;
    let pre_votes: Vec<&Message> = asked
        .iter()
        .filter(|m| m.msg_type == MessageType::MsgRequestPreVote)
        .collect();
    assert_eq!(pre_votes.len(), 2);
    // Its pre-votes and then its votes answered as if by the other two: it leads.
    for m in pre_votes {
        r.step(Message {
            msg_type: MessageType::MsgRequestPreVoteResponse,
            from: m.to,
            to: 1,
            term: m.term,
            ..Message::default()
        })
        .unwrap();
    }
    let votes = pump(&mut r).messages;
    for m in votes
        .iter()
        .filter(|m| m.msg_type == MessageType::MsgRequestVote)
    {
        r.step(Message {
            msg_type: MessageType::MsgRequestVoteResponse,
            from: m.to,
            to: 1,
            term: m.term,
            ..Message::default()
        })
        .unwrap();
    }
    pump(&mut r);
    assert!(r.is_leader());
    // Stalled for room, it is not ticked: however many ticks pass, it beats no one.
    r.propose(Vec::new(), b"x".to_vec()).unwrap();
    let mut out = Output::default();
    r.drive(now(), waker(), &mut out).unwrap();
    r.log_mut().refuse = Some(Fault::Room("the group's retained bound"));
    r.log_mut().make_durable();
    r.log_mut().full = true;
    let mut out = Output::default();
    r.drive(now(), waker(), &mut out).unwrap();
    assert!(r.is_stalled());
    for _ in 0..election * 4 {
        assert!(!r.tick().unwrap(), "a stalled member takes no part");
        let mut out = Output::default();
        r.drive(now(), waker(), &mut out).unwrap();
        assert!(out.messages.is_empty(), "a stalled leader beats no one");
    }
    // Room again: the refused write is made again, and the ticks it missed are not given again.
    r.log_mut().full = false;
    r.resume();
    pump(&mut r);
    assert!(!r.is_stalled() && r.is_leader());
    // Between ticks, the owner may have the leader beat.
    r.beat().unwrap();
    let beats = pump(&mut r).messages;
    assert_eq!(
        beats
            .iter()
            .filter(|m| m.msg_type == MessageType::MsgHeartbeat)
            .count(),
        2
    );
}

/// By suspicion the replica takes no ticks.
#[test]
fn a_replica_by_suspicion_takes_no_ticks() {
    let mut r = sole(1, Unbounded, |_| {});
    assert!(matches!(r.tick(), Err(ReplicaError::Refused(_))));
}

/// The state machine is given each change as its entry stated it: an owner that reports a change
/// with what it carried (focal reports its context with the configurations before and after)
/// reads it from the change itself.
#[test]
fn a_machine_is_given_the_change_it_applies() {
    let mut r = sole(1, Unbounded, |_| {});
    let mut learner = change(ConfChangeType::AddLearnerNode, 2);
    learner.context = b"where 2 listens".to_vec();
    r.change(Vec::new(), &learner).unwrap();
    pump(&mut r);
    let (index, context) = r.machine().changes.last().cloned().unwrap();
    assert_eq!(context, b"where 2 listens");
    assert_eq!(r.machine().configuration().learners, vec![2]);
    assert_eq!(r.machine().now.applied.index, index);
}

/// What `r` sends learner 2 once the learner, which holds nothing, refuses `append`: the refusal
/// takes its leader back past the log's start.
fn refused(r: &mut Sim, append: &Message) -> Message {
    r.step(Message {
        msg_type: MessageType::MsgAppendResponse,
        from: 2,
        to: 1,
        term: append.term,
        index: append.index,
        reject: true,
        reject_hint: 0,
        ..Message::default()
    })
    .unwrap();
    pump(r)
        .messages
        .into_iter()
        .find(|m| m.to == 2 && m.msg_type == MessageType::MsgSnapshot)
        .expect("a snapshot for the learner")
}

/// A machine that keeps its owner's checkpoints images the latest, behind what it applied
/// (focal's): the snapshot a member behind the log's start is sent carries the configuration the
/// group held at the image's point, never one applied since (Ongaro and Ousterhout 2014, §7). A
/// member added after the image is served once its owner checkpoints past the addition, and then
/// holds what its leader holds, each change applied once.
#[test]
fn a_snapshot_carries_the_configuration_held_at_its_images_point() {
    let mut r = sole(1, Unbounded, |_| {});
    r.machine_mut().checkpoints = true;
    for data in [b"a", b"b", b"c"] {
        r.propose(Vec::new(), data.to_vec()).unwrap();
    }
    pump(&mut r);
    let imaged = r.applied();
    assert!(r.compact(0, now(), waker()).unwrap());
    pump(&mut r);
    r.change(Vec::new(), &change(ConfChangeType::AddLearnerNode, 2))
        .unwrap();
    // The leader probes the learner it added, which holds nothing.
    let probe = pump(&mut r)
        .messages
        .into_iter()
        .find(|m| m.to == 2 && m.msg_type == MessageType::MsgAppend)
        .expect("a probe of the learner");
    assert_eq!(r.configuration().learners, vec![2]);

    let sent = refused(&mut r, &probe);
    let metadata = sent.snapshot.unwrap().metadata.unwrap();
    assert_eq!(metadata.index, imaged.index);
    assert_eq!(
        metadata.conf_state,
        Some(voters(&[1])),
        "the configuration held at the image's point"
    );

    // The owner checkpoints past the addition, and the next snapshot names the learner.
    assert!(r.compact(0, now(), waker()).unwrap());
    pump(&mut r);
    let checkpointed = r.machine().durable.applied;
    r.report_snapshot(2, false).unwrap();
    r.report_unreachable(2).unwrap();
    r.propose(Vec::new(), b"again".to_vec()).unwrap();
    let sent = pump(&mut r)
        .messages
        .into_iter()
        .find(|m| m.to == 2 && m.msg_type == MessageType::MsgSnapshot)
        .expect("a snapshot for the learner");
    let metadata = sent.snapshot.as_ref().unwrap().metadata.clone().unwrap();
    assert_eq!(metadata.index, checkpointed.index);
    assert_eq!(metadata.conf_state.unwrap().learners, vec![2]);

    // The learner installs it, and is given the rest from the log.
    let mut learner: Sim = Replica::open(
        &settings(2, 7),
        SimStore::new(1),
        Kv::new(voters(&[1]), false),
        Unbounded,
    )
    .unwrap();
    let mut to_learner = vec![sent];
    r.report_snapshot(2, true).unwrap();
    for _ in 0..64 {
        for m in to_learner.drain(..) {
            learner.step(m).unwrap();
        }
        for m in pump(&mut learner).messages {
            if m.to == 1 {
                r.step(m).unwrap();
            }
        }
        to_learner.extend(pump(&mut r).messages.into_iter().filter(|m| m.to == 2));
        if to_learner.is_empty() && learner.applied() == r.applied() {
            break;
        }
    }
    assert_eq!(learner.applied(), r.applied());
    assert_eq!(learner.machine().now.entries, r.machine().now.entries);
    assert_eq!(learner.configuration(), r.configuration());
    assert!(
        learner.machine().changes.is_empty(),
        "the addition came with the image"
    );
}

/// The bytes the core counts for what `out` answered: each entry applied, as `EntryRef` counts it.
fn applied_bytes(out: &Output<(u64, Vec<u8>)>) -> u64 {
    out.answers
        .iter()
        .map(|(index, data)| {
            EntryRef {
                index: *index,
                term: 0,
                kind: EntryType::EntryNormal,
                context: &[],
                data,
            }
            .encoded_bytes()
        })
        .sum()
}

/// A drive applies one page of committed entries at most, the core's
/// `max_committed_size_per_ready` as the core counts entries, or one entry larger than it, as it
/// takes one `Ready` at most (§7's quantum): what is committed past the page waits for the next
/// drive, which the drive says is due (`more`), and waits for no commit fence. So an owner
/// bounds what one drive gives its state machine by a page and an entry: four writes answered in
/// one drive gave it a page each before.
#[test]
fn a_drive_applies_one_page_and_the_next_drive_the_next() {
    let mut r = sole(4, Unbounded, |_| {});
    let page = settings(1, 7).core.max_committed_size_per_ready;
    let mut out = Output::default();
    for _ in 0..3 {
        for _ in 0..5 {
            r.propose(Vec::new(), vec![7; 100]).unwrap();
        }
        out.clear();
        r.drive(now(), waker(), &mut out).unwrap();
    }
    r.propose(Vec::new(), vec![9; 3 * page as usize]).unwrap();
    out.clear();
    r.drive(now(), waker(), &mut out).unwrap();
    assert_eq!(
        r.in_flight(),
        4,
        "a write out at every place the depth allows"
    );
    let last = r.core().raft.log().last_index().unwrap();
    while r.log_mut().make_durable() {}
    let mut drives = 0;
    while r.applied().index < last {
        drives += 1;
        assert!(
            drives <= 1_000,
            "applied through {} of {last}",
            r.applied().index
        );
        out.clear();
        let driven = r.drive(now(), waker(), &mut out).unwrap();
        let bytes = applied_bytes(&out);
        assert!(
            bytes <= page || out.answers.len() == 1,
            "drive {drives} applied {bytes} bytes in {} entries",
            out.answers.len()
        );
        if r.applied().index < last {
            assert!(
                driven.more,
                "drive {drives}: what waits past the page is due"
            );
            assert_eq!(r.behind_fence(), None, "a page waits for no fence");
        }
        while r.log_mut().make_durable() {}
    }
    assert!(
        drives > 4,
        "{drives} drives: the pages were not one a drive"
    );
    assert_eq!(
        r.machine().now.entries.last().unwrap().2.len(),
        3 * page as usize
    );
}
