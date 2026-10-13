//! What a replica tells its store to keep (`LogStore::keep_after`, `docs/durable.md` §8): the
//! entries it has yet to apply, and on a leader the entries a replicating member has yet to
//! acknowledge of what was sent to it up to the apply point, so a resend reads them from the store's
//! memory, never through the log's owner. A member that stops answering pins nothing past what was
//! in flight to it: once the leader has applied past what it sent, or probes it, it holds nothing.
//! Held after every leader drive against the leader's own tracker, on a seeded group in hyper-sim's
//! world (`support::cluster`), one follower's answers lost.
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

use hyper_raft::progress::{Progress, ProgressState};
use support::cluster::{Cluster, Member, Op, Shape, store_of};

/// The mark the leader owes its store: its apply point, or below it the least of what a member in
/// `Replicate`, whose next index is past the apply point, has acknowledged.
fn owed(r: &Member) -> u64 {
    let applied = r.applied().index;
    let id = r.id();
    r.core()
        .raft
        .tracker()
        .iter()
        .filter(|&(member, progress)| {
            member != id
                && progress.state == ProgressState::Replicate
                && progress.next_index > applied
        })
        .fold(applied, |from, (_, progress)| from.min(progress.matched))
}

/// One member's turn, as the cluster's rounds take it: it drives, makes its writes durable, and
/// drives again.
fn turn(group: &mut Cluster, id: u64) {
    group.act(Op::Drive(id));
    while group.act(Op::Durable(id)) {}
    group.act(Op::Drive(id));
}

/// Delivers every message but `silent`'s, which are lost.
fn deliver_but(group: &mut Cluster, silent: u64) {
    group.deliver_all(|message| message.from != silent);
}

/// After a leader's turn: the mark it owes, its apply point and the silent member's progress, with
/// the store held to the highest mark owed so far (`given`, which it raises).
fn checked(r: &mut Member, silent: u64, given: &mut u64, round: usize) -> (u64, u64, Progress) {
    assert!(r.is_leader(), "round {round}: the leader leads");
    let now_owed = owed(r);
    *given = (*given).max(now_owed);
    let applied = r.applied().index;
    let silent_progress = r.core().raft.tracker().get(silent).unwrap().clone();
    assert_eq!(
        store_of(r).kept_after,
        Some(*given),
        "round {round}: the store keeps after the mark owed (applied {applied}, silent member {silent_progress:?})"
    );
    if now_owed < applied {
        // What the silent member holds the mark for was all sent to it and is in flight.
        assert!(silent_progress.next_index > applied, "round {round}");
        assert_eq!(now_owed, silent_progress.matched, "round {round}");
    }
    (now_owed, applied, silent_progress)
}

#[test]
fn a_leader_keeps_what_a_replicating_member_has_yet_to_acknowledge_and_no_more_than_was_sent_it() {
    let shape = Shape {
        members: 3,
        voters: 3,
        depth: 2,
        volatile: false,
        control: false,
        ahead: false,
    };
    let mut group = Cluster::new(shape, 7);
    assert!(group.settles(2_000), "the group elects and commits");
    let leader = group.leader().expect("a leader");
    let mut followers = (1..=shape.members).filter(|&id| id != leader);
    let answering = followers.next().unwrap();
    let silent = followers.next().unwrap();
    // The window is full after this many appends go unanswered, the leader then sends the silent
    // member nothing more, and the next entry the answering member acknowledges is applied past
    // what was sent it: a proposal a round, each applied the round after its acknowledgement.
    // The leader sends the silent member one append a round, one entry proposed a round, until its
    // window of `window` messages is full; from then on it sends it nothing, and the apply point, one
    // entry further each round (each acknowledged by the answering member the round before), passes
    // what was sent in as many rounds as it trailed it when the window filled.
    let window = group
        .replica(leader)
        .unwrap()
        .core()
        .raft
        .config()
        .max_inflight_msgs;
    // The highest mark the leader owed so far: the store takes no lower one.
    let mut given = 0u64;
    let mut held_below_apply = 0u64;
    let mut filled: Option<(usize, u64)> = None;
    let mut released = false;
    for round in 0.. {
        group.act(Op::Propose(leader));
        turn(&mut group, leader);
        let (now_owed, applied, silent_progress) =
            checked(group.replica(leader).unwrap(), silent, &mut given, round);
        if now_owed < applied {
            held_below_apply += 1;
        } else if held_below_apply > 0 {
            released = true;
            break;
        }
        match filled {
            None if silent_progress.inflights.full() => {
                filled = Some((round, silent_progress.next_index - applied));
            }
            None => assert!(
                round < window,
                "round {round}: the window fills within {window} rounds"
            ),
            Some((at, trailing)) => assert!(
                round - at < usize::try_from(trailing).unwrap(),
                "round {round}: the apply point passes what was sent within {trailing} rounds of the window filling at round {at}"
            ),
        }
        turn(&mut group, answering);
        turn(&mut group, silent);
        deliver_but(&mut group, silent);
    }
    assert!(
        held_below_apply > 0,
        "the silent member held the mark below the apply point while its entries were in flight"
    );
    assert!(released);
    // Its answers back, the silent member catches up, and the mark is the apply point again.
    assert!(group.settles(2_000), "the group settles whole");
    let r = group.replica(leader).unwrap();
    let applied = r.applied().index;
    assert_eq!(owed(r), applied);
    assert_eq!(store_of(r).kept_after, Some(applied));
}

#[test]
fn a_follower_keeps_what_it_has_yet_to_apply() {
    let shape = Shape {
        members: 3,
        voters: 3,
        depth: 2,
        volatile: false,
        control: false,
        ahead: false,
    };
    let mut group = Cluster::new(shape, 11);
    assert!(group.settles(2_000), "the group elects and commits");
    let leader = group.leader().expect("a leader");
    for id in (1..=shape.members).filter(|&id| id != leader) {
        let r = group.replica(id).unwrap();
        assert!(!r.is_leader());
        let applied = r.applied().index;
        assert_eq!(store_of(r).kept_after, Some(applied), "member {id}");
    }
}
