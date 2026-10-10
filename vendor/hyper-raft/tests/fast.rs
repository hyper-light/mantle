//! The fast track (27 §4): a proposal from a member that does not lead is
//! committed once a fast quorum holds it, without the round the classic
//! track takes; and whatever the schedule, what one leader committed by it
//! every later leader holds.
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cognitive_complexity,
    unreachable_pub
)]
mod support;

use hyper_raft::{
    fast::{FAST_PROPOSE, FAST_VOTE},
    proto::{
        ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, Entry, EntryType,
        Message, MessageType,
    },
};
use support::{Cluster, Mix, New, Op, Replica, Seeded, Settings};

fn add(total: &mut hyper_raft::FastStats, more: hyper_raft::FastStats) {
    total.proposed += more.proposed;
    total.displaced += more.displaced;
    total.held += more.held;
    total.taken += more.taken;
    total.committed += more.committed;
    total.recovered += more.recovered;
}
#[allow(
    clippy::disallowed_methods,
    reason = "a soak sets the seed count from the environment; the default is the gate's"
)]
fn count(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
/// Delivers what `carried` admits until nothing it admits is left; the
/// rest stays on the network.
fn carry(group: &mut Cluster<New>, carried: impl Fn(&Message) -> bool) {
    for _ in 0..10_000 {
        let Some(at) = group.net.iter().position(&carried) else {
            return;
        };
        group.act(&Op::Deliver {
            at,
            keep: false,
            lose: false,
        });
    }
    panic!("the network does not fall quiet");
}
fn quiet(group: &mut Cluster<New>) {
    carry(group, |_| true);
}
fn group(members: u64) -> Cluster<New> {
    let voters: Vec<u64> = (1..=members).collect();
    let mut group: Cluster<New> = Cluster::new(members, &voters, Settings::fast(), 17);
    group.act(&Op::Campaign(1));
    quiet(&mut group);
    assert_eq!(group.leaders_now(), vec![1]);
    // The leader committed in its term and applied it: the fast track is
    // open.
    let view = group.peek(1).unwrap().view();
    assert_eq!((view.commit, view.applied), (1, 1));
    group
}
fn commit(group: &Cluster<New>, member: u64) -> u64 {
    group.peek(member).unwrap().view().commit
}
fn applied(group: &Cluster<New>, member: u64) -> u64 {
    group.peek(member).unwrap().app().index
}
fn fast(group: &mut Cluster<New>, member: u64, data: &[u8]) -> bool {
    group.act(&Op::Fast(member, data.to_vec()))[0].accepted == Some(true)
}

#[test]
fn a_proposal_is_committed_once_a_fast_quorum_holds_it() {
    let mut group = group(5);
    assert!(fast(&mut group, 3, b"e"));
    assert_eq!(group.peek(3).unwrap().held(), vec![(2, b"e".to_vec())]);
    // The proposal reaches every voter; each holds it, and says so once it
    // is durable.
    carry(&mut group, |message| message.msg_type == FAST_PROPOSE);
    for member in [2, 4, 5] {
        assert_eq!(group.peek(member).unwrap().held(), vec![(2, b"e".to_vec())]);
    }
    // The leader took it into its log when it heard of it.
    assert_eq!(group.peek(1).unwrap().view().last_index, 2);
    assert_eq!(commit(&group, 1), 1);
    // Three votes and the leader are the fast quorum of five; what the
    // leader sent its members is still on its way.
    let mut votes: Vec<u64> = group
        .net
        .iter()
        .filter(|message| message.msg_type == FAST_VOTE)
        .map(|message| message.from)
        .collect();
    votes.sort_unstable();
    assert_eq!(votes, vec![2, 3, 4, 5]);
    group.act(&Op::Deliver {
        at: group
            .net
            .iter()
            .position(|m| m.msg_type == FAST_VOTE && m.from == 2)
            .unwrap(),
        keep: false,
        lose: false,
    });
    group.act(&Op::Deliver {
        at: group
            .net
            .iter()
            .position(|m| m.msg_type == FAST_VOTE && m.from == 3)
            .unwrap(),
        keep: false,
        lose: false,
    });
    assert_eq!(
        commit(&group, 1),
        1,
        "two votes and the leader are no fast quorum"
    );
    group.act(&Op::Deliver {
        at: group
            .net
            .iter()
            .position(|m| m.msg_type == FAST_VOTE && m.from == 4)
            .unwrap(),
        keep: false,
        lose: false,
    });
    assert_eq!(commit(&group, 1), 2);
    assert_eq!(applied(&group, 1), 2);
    // No member answered an append of the leader's yet.
    assert!(
        !group
            .net
            .iter()
            .any(|message| message.msg_type == MessageType::MsgAppendResponse)
    );
    // The members learn of the commit from the leader, and hold the entry
    // from it. What each holds by itself it keeps until the leader's next
    // message says a classic quorum holds the index: the leader learns that
    // from their answers, and sends no round of its own to say it.
    quiet(&mut group);
    for member in 1..=5 {
        assert_eq!(applied(&group, member), 2);
    }
    assert!(group.peek(1).unwrap().held().is_empty());
    for member in 2..=5 {
        assert_eq!(group.peek(member).unwrap().held(), vec![(2, b"e".to_vec())]);
    }
    group.act(&Op::Ping(1));
    quiet(&mut group);
    for member in 1..=5 {
        assert!(group.peek(member).unwrap().held().is_empty());
    }
    assert_eq!(group.chosen[&2].3, b"e");
}

#[test]
fn without_the_votes_the_classic_quorum_commits() {
    let mut group = group(5);
    assert!(fast(&mut group, 3, b"e"));
    carry(&mut group, |message| message.msg_type == FAST_PROPOSE);
    // Every vote is lost.
    group.net.retain(|message| message.msg_type != FAST_VOTE);
    quiet(&mut group);
    for member in 1..=5 {
        assert_eq!(applied(&group, member), 2);
    }
    assert_eq!(group.chosen[&2].3, b"e");
    // And when the leader does not hear the proposal, a vote tells it.
    assert!(fast(&mut group, 4, b"f"));
    group
        .net
        .retain(|message| !(message.msg_type == FAST_PROPOSE && message.to == 1));
    quiet(&mut group);
    for member in 1..=5 {
        assert_eq!(applied(&group, member), 3);
    }
    assert_eq!(group.chosen[&3].3, b"f");
}

#[test]
fn of_two_proposals_for_one_index_one_is_taken_and_the_other_said_to_its_proposer() {
    let mut group = group(5);
    assert!(fast(&mut group, 2, b"e"));
    assert!(fast(&mut group, 3, b"f"));
    // Members 4 and 5 hear the second first; the leader the first.
    carry(&mut group, |message| {
        message.msg_type == FAST_PROPOSE && message.from == 3 && message.to >= 4
    });
    carry(&mut group, |message| message.msg_type == FAST_PROPOSE);
    assert_eq!(group.peek(4).unwrap().held(), vec![(2, b"f".to_vec())]);
    assert_eq!(group.peek(1).unwrap().view().last_index, 2);
    let mut displaced = Vec::new();
    for _ in 0..10_000 {
        if group.net.is_empty() {
            break;
        }
        for report in group.act(&Op::Deliver {
            at: 0,
            keep: false,
            lose: false,
        }) {
            displaced.extend(
                report
                    .output
                    .displaced
                    .iter()
                    .map(|said| (report.member, said.3.clone())),
            );
        }
    }
    // Two hold the one and three the other: no fast quorum, and the
    // leader's entry is committed by the classic one.
    assert_eq!(group.chosen[&2].3, b"e");
    assert_eq!(displaced, vec![(3, b"f".to_vec())]);
    for member in 1..=5 {
        assert_eq!(applied(&group, member), 2);
        assert!(group.peek(member).unwrap().held().is_empty());
    }
    // Its proposer proposes it again, for the next index.
    assert!(fast(&mut group, 3, b"f"));
    quiet(&mut group);
    assert_eq!(group.chosen[&3].3, b"f");
}

#[test]
fn what_a_leader_committed_by_the_fast_quorum_the_next_leader_takes() {
    for lost in [1u64, 2, 3, 4, 5] {
        let mut group = group(5);
        assert!(fast(&mut group, 3, b"e"));
        carry(&mut group, |message| message.msg_type == FAST_PROPOSE);
        carry(&mut group, |message| message.msg_type == FAST_VOTE);
        assert_eq!(commit(&group, 1), 2, "the fast quorum committed");
        // The leader stops before any member heard from it, and one of
        // the others is away.
        group.net.clear();
        group.stop(1);
        if lost != 1 {
            group.stop(lost);
        }
        let candidate = (2..=5).find(|member| *member != lost).unwrap();
        for _ in 0..40 {
            for member in group.up() {
                group.act(&Op::Tick(member));
            }
            quiet(&mut group);
            if !group.leaders_now().is_empty() {
                break;
            }
        }
        let leader = *group.leaders_now().first().expect("the rest elect");
        assert!(leader >= candidate);
        quiet(&mut group);
        // What the group holds at the index is what was committed there.
        for member in group.up() {
            assert!(applied(&group, member) >= 2, "member {member}");
        }
        assert_eq!(group.chosen[&2].3, b"e");
        // Those that were away hold it when they are back.
        assert!(group.settles());
        for member in 1..=5 {
            assert!(applied(&group, member) >= 2);
        }
    }
}

#[test]
fn what_no_one_committed_a_later_leader_may_replace() {
    let mut group = group(5);
    // Two hold one entry and two another; the leader hears of neither.
    assert!(fast(&mut group, 2, b"e"));
    assert!(fast(&mut group, 4, b"f"));
    carry(&mut group, |message| {
        message.msg_type == FAST_PROPOSE
            && ((message.from == 2 && message.to == 3) || (message.from == 4 && message.to == 5))
    });
    group.net.clear();
    group.stop(1);
    for _ in 0..40 {
        for member in group.up() {
            group.act(&Op::Tick(member));
        }
        quiet(&mut group);
        if !group.leaders_now().is_empty() {
            break;
        }
    }
    assert_eq!(group.leaders_now().len(), 1);
    quiet(&mut group);
    // One of the two is at the index, in every log; the other was said to
    // its proposer.
    let taken = group.chosen[&2].3.clone();
    assert!(taken == b"e" || taken == b"f");
    for member in group.up() {
        assert!(applied(&group, member) >= 3);
        assert!(group.peek(member).unwrap().held().is_empty());
    }
}

#[test]
fn what_a_member_holds_it_holds_after_it_stopped() {
    let mut group = group(3);
    assert!(fast(&mut group, 2, b"e"));
    carry(&mut group, |message| {
        message.msg_type == FAST_PROPOSE && message.to == 3
    });
    group.net.clear();
    for member in [2, 3] {
        group.act(&Op::Restart(member));
        assert_eq!(group.peek(member).unwrap().held(), vec![(2, b"e".to_vec())]);
    }
    // The leader stops; whoever is elected takes what the two hold.
    group.stop(1);
    for _ in 0..40 {
        for member in group.up() {
            group.act(&Op::Tick(member));
        }
        quiet(&mut group);
        if !group.leaders_now().is_empty() {
            break;
        }
    }
    assert_eq!(group.leaders_now().len(), 1);
    quiet(&mut group);
    assert_eq!(group.chosen[&2].3, b"e");
}

#[test]
fn while_the_group_changes_the_fast_quorum_commits_nothing() {
    let mut group = group(5);
    let joint = ConfChangeV2 {
        transition: ConfChangeTransition::Explicit,
        changes: vec![ConfChangeSingle {
            change_type: ConfChangeType::RemoveNode,
            node_id: 5,
        }],
        context: vec![],
    };
    assert_eq!(group.act(&Op::Change(1, joint))[0].accepted, Some(true));
    quiet(&mut group);
    let before = commit(&group, 1);
    assert!(fast(&mut group, 3, b"e"));
    carry(&mut group, |message| message.msg_type == FAST_PROPOSE);
    carry(&mut group, |message| message.msg_type == FAST_VOTE);
    assert_eq!(
        commit(&group, 1),
        before,
        "a fast quorum committed in a joint configuration"
    );
    quiet(&mut group);
    assert_eq!(commit(&group, 1), before + 1);
    assert_eq!(group.chosen[&(before + 1)].3, b"e");
    // Out of it, the fast quorum is of the four that stay.
    assert_eq!(
        group.act(&Op::Change(1, ConfChangeV2::default()))[0].accepted,
        Some(true)
    );
    quiet(&mut group);
    let before = commit(&group, 1);
    assert!(fast(&mut group, 3, b"f"));
    carry(&mut group, |message| message.msg_type == FAST_PROPOSE);
    carry(&mut group, |message| message.msg_type == FAST_VOTE);
    assert_eq!(commit(&group, 1), before + 1);
}

#[test]
fn what_may_not_go_by_the_fast_track_is_refused() {
    let mut group = group(3);
    let proposal = |entry: Entry| Message {
        msg_type: FAST_PROPOSE,
        from: 3,
        to: 2,
        entries: vec![entry],
        ..Message::default()
    };
    let change = Entry {
        entry_type: EntryType::EntryConfChangeV2,
        index: 2,
        data: vec![1],
        ..Entry::default()
    };
    let empty = Entry {
        index: 2,
        ..Entry::default()
    };
    let nowhere = Entry {
        data: vec![1],
        ..Entry::default()
    };
    for entry in [change, empty, nowhere] {
        group.net.push(proposal(entry));
        let reports = group.act(&Op::Deliver {
            at: group.net.len() - 1,
            keep: false,
            lose: false,
        });
        assert_eq!(reports[0].accepted, Some(false));
        assert!(group.peek(2).unwrap().held().is_empty());
    }
    // Below the log, and beyond the window above what is committed.
    let window = group.peek(2).unwrap().raw.raft.config().limits.fast_window;
    for index in [1, 2 + window] {
        group.net.push(proposal(Entry {
            index,
            data: vec![1],
            ..Entry::default()
        }));
        group.act(&Op::Deliver {
            at: group.net.len() - 1,
            keep: false,
            lose: false,
        });
        assert!(group.peek(2).unwrap().held().is_empty());
    }
    // A group that has no fast track holds nothing and proposes nothing.
    let mut plain: Cluster<New> = Cluster::new(3, &[1, 2, 3], Settings::focal(), 1);
    plain.act(&Op::Campaign(1));
    quiet(&mut plain);
    assert!(!fast(&mut plain, 2, b"e"));
    plain.net.push(proposal(Entry {
        index: 2,
        data: vec![1],
        ..Entry::default()
    }));
    plain.act(&Op::Deliver {
        at: 0,
        keep: false,
        lose: false,
    });
    assert!(plain.peek(2).unwrap().held().is_empty() && plain.net.is_empty());
}

/// Groups with the fast track under schedules: the terms they led, the
/// entries they committed, and what the fast track did.
fn fast_schedules(settings: Settings) -> (usize, usize, hyper_raft::FastStats) {
    let seeds = count("HYPER_RAFT_SEEDS", 96);
    let steps = count("HYPER_RAFT_STEPS", 4_000);
    let first = count("HYPER_RAFT_SEED", 0);
    let mix = Mix {
        leader_leaves: true,
        fast: 60,
        ..Mix::everything()
    };
    let (mut terms, mut committed) = (0, 0);
    let mut did = hyper_raft::FastStats::default();
    for seed in first..first + seeds {
        let group = fast_schedule(settings, seed, steps, &mix, &mut did);
        terms += group.leaders.len();
        committed += group.chosen.len();
    }
    (terms, committed, did)
}

/// One group with the fast track under the schedule of `seed`, which is
/// safe and settles; what the fast track did is added to `did`.
fn fast_schedule(
    settings: Settings,
    seed: u64,
    steps: u64,
    mix: &Mix,
    did: &mut hyper_raft::FastStats,
) -> Cluster<New> {
    let mut group: Cluster<New> = Cluster::new(5, &[1, 2, 3, 4, 5], settings, seed);
    let mut rng = Seeded(seed);
    for _ in 0..steps {
        let op = group.choose(&mut rng, mix);
        // What a member did it forgets when it stops.
        if let Op::Restart(member) = op
            && let Some(node) = group.peek(member)
        {
            add(did, node.fast_stats());
        }
        group.act(&op);
    }
    assert!(group.settles(), "seed {seed}: the group did not settle");
    for member in group.up() {
        add(did, group.peek(member).unwrap().fast_stats());
    }
    group
}

/// The schedule that found an election committing a second entry at an
/// index that held a committed one (seed 9843 of 40,000 from seed 3,000,
/// on the core before focal's F43, F41 and F42 and R-3's R17, whose rules
/// it keeps here so that it runs as it was found). A leader of term 3 committed
/// index 32 by the fast quorum of members 1, 2 and 4, of which 2 and 4
/// held the entry beside their logs and had told the leader nothing of
/// their logs; member 3, whose log held an entry of an older term at 32,
/// was later elected by them, kept its own entry and committed it. A
/// member's held entry now counts for a fast commit only once the leader
/// knows its log holds an entry of the leader's term (`docs/raft.md`).
#[test]
fn an_election_never_commits_a_second_entry_at_a_committed_index() {
    let found = Settings {
        round_each: true,
        max_inflight_bytes: u64::MAX,
        bare_answers: true,
        refuse_ahead: true,
        ..Settings::fast()
    };
    let mix = Mix {
        leader_leaves: true,
        fast: 60,
        ..Mix::everything()
    };
    let mut did = hyper_raft::FastStats::default();
    let group = fast_schedule(found, 9843, 4_000, &mix, &mut did);
    // It went on past the index, and every member agreed on it.
    assert!(group.chosen.len() > 32, "{did:?}");
}

/// The schedule that found an election under a configuration a member had
/// not yet applied committing a second entry (seed 54104 of 40,000 from
/// seed 43,000, once the rule above was in). The leader of term 2, whose
/// configuration had demoted member 3 to a learner, committed index 11 by
/// the fast quorum of three of its four voters. Member 2, one of them, had
/// not committed the demotion and counted by the five voters before it: it
/// was elected by members 3 and 4, which held another entry at 11, and took
/// theirs. A fast quorum now counts only where it is a fast quorum of every
/// configuration a member that holds an entry of the term may count by
/// (`docs/raft.md`). A member now counts by the newest configuration its log
/// holds (`docs/raft.md` §3.4), so one that counts by the five holds no entry
/// of the change; the schedule is kept as it ran, and the rule's catch is
/// `tests/check.rs`'s.
#[test]
fn a_member_that_counts_by_the_configuration_before_commits_no_second_entry() {
    let mix = Mix {
        leader_leaves: true,
        fast: 60,
        ..Mix::everything()
    };
    let mut did = hyper_raft::FastStats::default();
    let group = fast_schedule(Settings::fast(), 54104, 4_000, &mix, &mut did);
    assert!(group.chosen.len() > 11, "{did:?}");
}

#[test]
fn a_group_with_the_fast_track_is_safe_and_settles() {
    let seeds = count("HYPER_RAFT_SEEDS", 96);
    let (terms, committed, did) = fast_schedules(Settings::fast());
    println!("{seeds} schedules led {terms} terms and committed {committed} entries: {did:?}");
    // Schedules that never elected, never committed and never took the fast
    // track say nothing of it: each mechanism must be reached; how often is
    // reported above, not judged against a picked count.
    assert!(terms > 0 && committed > 0);
    assert!(did.proposed > 0 && did.taken > 0, "{did:?}");
    assert!(did.committed > 0 && did.recovered > 0, "{did:?}");
    assert!(did.displaced > 0, "{did:?}");
}

/// The same schedules with every member given its `Ready`s in place
/// (`RawNode::ready_in_place`): the group decides exactly what it decided
/// with copies.
#[test]
fn a_group_given_its_readies_in_place_decides_the_same() {
    let copied = fast_schedules(Settings::fast());
    let in_place = fast_schedules(Settings {
        in_place: true,
        ..Settings::fast()
    });
    assert_eq!(copied, in_place);
}

/// Delivers what `carried` admits among the members `among`, until nothing it admits is left;
/// what any other member sends or is sent stays on the network, and is dropped at the end.
fn among(group: &mut Cluster<New>, members: &[u64], carried: impl Fn(&Message) -> bool) {
    carry(group, |message| {
        members.contains(&message.from) && members.contains(&message.to) && carried(message)
    });
}

fn held_at(group: &Cluster<New>, member: u64, index: u64) -> Option<Vec<u8>> {
    group
        .peek(member)?
        .held()
        .into_iter()
        .find(|(at, _)| *at == index)
        .map(|(_, data)| data)
}

/// The run hyper-check's swarm found at fast seed 41,345 (`docs/sim.md` §15.9), at the fewest
/// members it takes in the core (`docs/models/FastTrackScenario.tla` has it at four in the model,
/// where an election's tie may go either way; the core breaks a tie for the first voted, so a
/// fifth member makes the lost value the most held). Before the fix a later leader lost an entry
/// the fast track committed; with it the entry is kept (`docs/raft.md` §3.5).
///
/// Term 1: member 1 leads; member 3 proposes `a` and `b` for indexes 2 and 3 by the fast track,
/// and members 1, 2, 3 and 5 hold them, a fast quorum of four of five; the leader's appends of 2
/// and 3 reach member 2 alone, and the leader commits both by the fast quorum. Members 3, 4 and 5
/// still end at index 1.
///
/// Term 2: member 3 is elected by 4 and 5 and recovers `a` and `b` at 2 and 3 under term 2;
/// member 4 takes its log through index 2 only (one entry an append), and holds `c`, its own
/// proposal, at 3.
///
/// Term 3: member 4 is elected by 1 and 2, whose logs end at index 3 in term 1, older than its
/// own. What it recovers at 3 is the most held among them: before the fix, 1 held nothing (a
/// leader did not hold what it took) and 2 had let `b` go once its log reached 3, so member 4
/// took its own `c`; now 1 and 2 hold `b` until they know index 3 committed by a classic quorum,
/// two reports to one, and member 4 takes `b`.
#[test]
fn an_entry_the_fast_track_committed_outlives_a_log_cut_short_of_it() {
    // A campaign is its election (no pre-vote, no lease from check-quorum), and an append carries
    // one entry.
    let settings = Settings {
        max_size_per_msg: 1,
        pre_vote: false,
        check_quorum: false,
        ..Settings::fast()
    };
    let mut group: Cluster<New> = Cluster::new(5, &[1, 2, 3, 4, 5], settings, 17);
    group.act(&Op::Campaign(1));
    quiet(&mut group);
    assert_eq!(group.leaders_now(), vec![1]);
    assert_eq!(commit(&group, 1), 1);
    // Term 1.
    assert!(fast(&mut group, 3, b"a"));
    assert!(fast(&mut group, 3, b"b"));
    carry(&mut group, |message| {
        message.msg_type == FAST_PROPOSE && [1, 2, 5].contains(&message.to)
    });
    group.net.retain(|message| message.msg_type != FAST_PROPOSE);
    carry(&mut group, |message| {
        message.msg_type == FAST_VOTE
            || (message.msg_type == MessageType::MsgAppend && message.to == 2)
            || (message.msg_type == MessageType::MsgAppendResponse && message.from == 2)
    });
    assert_eq!(commit(&group, 1), 3, "the fast quorum committed 2 and 3");
    assert_eq!(group.chosen[&3].3, b"b");
    for member in [3, 4, 5] {
        assert_eq!(group.peek(member).unwrap().view().last_index, 1);
    }
    group.net.clear();
    // Term 2.
    group.act(&Op::Campaign(3));
    among(&mut group, &[3, 4, 5], |message| {
        !(message.to == 4
            && message.msg_type == MessageType::MsgAppend
            && message.entries.iter().any(|entry| entry.index >= 3))
    });
    assert_eq!(group.leaders_now(), vec![1, 3]);
    let four = group.peek(4).unwrap().view();
    assert_eq!((four.term, four.last_index), (2, 2));
    group.net.clear();
    assert!(fast(&mut group, 4, b"c"));
    assert_eq!(held_at(&group, 4, 3), Some(b"c".to_vec()));
    group.net.clear();
    // Term 3.
    group.act(&Op::Campaign(4));
    among(&mut group, &[1, 2, 4], |_| true);
    assert!(group.leaders_now().contains(&4));
    let taken = group
        .disk(4)
        .entries
        .iter()
        .find(|entry| entry.index == 3)
        .map(|entry| entry.data.clone());
    assert_eq!(taken, Some(b"b".to_vec()), "what member 4 took at index 3");
}

/// A member holds what it holds by itself after its log reaches the index, until a leader says it
/// knows the index committed by a classic quorum; one that says nothing of it (a member not yet
/// upgraded, `Message::classic` of `None`) has it released by no one. Storage is told to release
/// it in the same order, and a member that opens again knows what storage released.
#[test]
fn a_holding_outlives_its_log_reaching_it_until_a_classic_commit_is_known() {
    let mut group = group(3);
    assert!(fast(&mut group, 2, b"e"));
    carry(&mut group, |message| message.msg_type == FAST_PROPOSE);
    carry(&mut group, |message| message.msg_type == FAST_VOTE);
    assert_eq!(commit(&group, 1), 2, "the fast quorum committed");
    // The leader's appends reach the others saying nothing of the classic commit: their logs
    // reach index 2, and they hold `e` there still.
    for _ in 0..1_000 {
        for message in &mut group.net {
            message.classic = None;
        }
        let Some(at) = group.net.iter().position(|_| true) else {
            break;
        };
        group.act(&Op::Deliver {
            at,
            keep: false,
            lose: false,
        });
    }
    for member in [2, 3] {
        assert!(group.peek(member).unwrap().view().last_index >= 2);
        assert_eq!(
            held_at(&group, member, 2),
            Some(b"e".to_vec()),
            "member {member}"
        );
        // Index 1, the leader's first entry, it knew committed so before the proposal.
        assert_eq!(group.disk(member).released, 1);
    }
    // The leader's next round says what it knows: index 2 is in a classic quorum's logs.
    for _ in 0..30 {
        group.act(&Op::Tick(1));
    }
    quiet(&mut group);
    for member in 1..=3 {
        assert_eq!(held_at(&group, member, 2), None, "member {member}");
    }
    // Storage is told with the next write each member makes anyway.
    assert!(fast(&mut group, 2, b"f"));
    quiet(&mut group);
    for member in 1..=3 {
        assert!(group.disk(member).released >= 2, "member {member}");
        assert!(
            group
                .disk(member)
                .proposals
                .iter()
                .all(|held| held.index > 2)
        );
    }
    // Opened again, a member knows what it released, and holds nothing below it.
    group.act(&Op::Restart(2));
    assert_eq!(held_at(&group, 2, 2), None);
}

/// A fast quorum counts what members hold by themselves alone: a member whose log holds the
/// leader's entry and that holds nothing at its index is not counted, the leader included.
#[test]
fn a_fast_quorum_counts_holdings_and_not_logs() {
    let mut group = group(3);
    assert!(fast(&mut group, 2, b"e"));
    // Member 3 never hears of the proposal: it holds the entry only once the leader's append
    // reaches it, and the fast quorum of three is not made by the other two.
    carry(&mut group, |message| {
        message.msg_type == FAST_PROPOSE && message.to == 1
    });
    group.net.retain(|message| message.msg_type != FAST_PROPOSE);
    carry(&mut group, |message| {
        message.msg_type == FAST_VOTE
            || (message.msg_type == MessageType::MsgAppend && message.to == 3)
    });
    assert_eq!(
        commit(&group, 1),
        1,
        "no fast quorum holds the entry by itself"
    );
    assert_eq!(held_at(&group, 3, 2), None);
    // The classic quorum commits it once member 3's answer arrives.
    quiet(&mut group);
    assert_eq!(commit(&group, 1), 2);
    assert_eq!(group.chosen[&2].3, b"e");
}
