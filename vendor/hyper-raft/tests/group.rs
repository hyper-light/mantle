//! A group under schedules that lose, repeat and hold back its messages,
//! stop its members and change who they are. Whatever the schedule: no two
//! members commit different entries at one index, and no term has two
//! leaders. Once the network is whole and the members are up, the group
//! elects and commits.
//!
//! The group is of this core alone, and of both cores together, as a group
//! is while its nodes are replaced one by one.
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

use hyper_raft::proto::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, Message, MessageType,
};
use support::{Cluster, Either, Mix, New, Old, Op, Replica, Seeded, Settings};

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

fn scheduled<R: Replica>(group: &mut Cluster<R>, seed: u64, steps: u64, mix: &Mix) {
    let mut rng = Seeded(seed);
    for _ in 0..steps {
        let op = group.choose(&mut rng, mix);
        group.act(&op);
    }
}

/// Groups of this core under schedules; the terms they led and the reads
/// they answered.
fn schedules_of_this_core(settings: Settings) -> (usize, u64) {
    let seeds = count("HYPER_RAFT_SEEDS", 96);
    let steps = count("HYPER_RAFT_STEPS", 4_000);
    let first = count("HYPER_RAFT_SEED", 0);
    let mix = Mix {
        leader_leaves: true,
        bursts: true,
        windows: true,
        ..Mix::everything()
    };
    let (mut terms, mut answered) = (0, 0);
    for seed in first..first + seeds {
        let mut group: Cluster<New> = Cluster::new(5, &[1, 2, 3], settings, seed);
        group.stop_who_left = true;
        scheduled(&mut group, seed, steps, &mix);
        assert!(group.settles(), "seed {seed}: the group did not settle");
        assert_eq!(
            group.deposed, 0,
            "seed {seed}: a member led a group it left"
        );
        terms += group.leaders.len();
        answered += group.answered;
    }
    (terms, answered)
}

#[test]
fn a_group_of_this_core_is_safe_and_settles() {
    let seeds = count("HYPER_RAFT_SEEDS", 96);
    let (terms, answered) = schedules_of_this_core(Settings::focal());
    // Every read answered saw what was committed before it was asked
    // (`Cluster::report`), asked alone or several at a time.
    assert!(answered > 0, "{answered} reads were answered");
    println!("{seeds} schedules led {terms} terms and answered {answered} reads");
    assert!(terms > 0);
}

/// The same schedules with every member given its `Ready`s in place: the
/// group leads the same terms, and is safe and settles alike.
#[test]
fn a_group_given_its_readies_in_place_leads_the_same_terms() {
    let in_place = Settings {
        in_place: true,
        ..Settings::focal()
    };
    assert_eq!(
        schedules_of_this_core(Settings::focal()),
        schedules_of_this_core(in_place)
    );
}

#[test]
fn a_group_of_both_cores_is_safe_and_settles() {
    let seeds = count("HYPER_RAFT_SEEDS", 96);
    let steps = count("HYPER_RAFT_STEPS", 4_000);
    let first = count("HYPER_RAFT_SEED", 0);
    // The cores count a change of the configuration differently: hyper-raft by the newest in its
    // log, raft-rs by the one it applied, a rule that alone elects two leaders of a term
    // (`docs/raft.md` §3.4, `docs/models/Reconfig.tla`). The group keeps its configuration, and
    // the cores agree on everything else.
    let mix = Mix {
        bursts: true,
        windows: true,
        changes: false,
        ..Mix::everything()
    };
    let (mut old, mut new) = (0, 0);
    for seed in first..first + seeds {
        let mut group: Cluster<Either> = Cluster::new(5, &[1, 2, 3], Settings::focal(), seed);
        scheduled(&mut group, seed, steps, &mix);
        assert!(group.settles(), "seed {seed}: the group did not settle");
        for leader in group.leaders.values() {
            if leader % 2 == 1 {
                old += 1;
            } else {
                new += 1;
            }
        }
    }
    println!("{seeds} schedules: raft-rs led {old} terms and hyper-raft {new}");
    assert!(old > 0 && new > 0);
}

fn quiet<R: Replica>(group: &mut Cluster<R>) {
    for _ in 0..10_000 {
        if group.net.is_empty() {
            return;
        }
        group.act(&Op::Deliver {
            at: 0,
            keep: false,
            lose: false,
        });
    }
    panic!("the network does not fall quiet");
}
fn elect<R: Replica>(group: &mut Cluster<R>, id: u64) {
    group.act(&Op::Campaign(id));
    quiet(group);
    assert_eq!(group.leaders_now(), vec![id], "member {id} was not elected");
}
fn ticks<R: Replica>(group: &mut Cluster<R>, members: &[u64], count: usize) {
    for _ in 0..count {
        for member in members {
            group.act(&Op::Tick(*member));
        }
        quiet(group);
    }
}
/// Delivers, and takes from the network, the messages `which` picks.
fn deliver<R: Replica>(group: &mut Cluster<R>, which: impl Fn(&Message) -> bool) -> usize {
    let mut delivered = 0;
    while let Some(at) = group.net.iter().position(&which) {
        group.act(&Op::Deliver {
            at,
            keep: false,
            lose: false,
        });
        delivered += 1;
    }
    delivered
}

/// A round of heartbeats confirms the reads asked before it left, and no
/// read asked after. A leader sends a round for one read; the answers are
/// held on the network; the leader is cut off, another is elected and
/// commits; the old leader, which has heard nothing, is asked a second
/// read; and then the answers to the first round arrive. They prove that it
/// led when the first read was asked, and nothing about when the second
/// was: the first is answered and the second is not — answered at the old
/// leader's commit it would miss what the group committed before it was
/// asked (`Cluster::report` checks every answer against that).
#[test]
fn a_round_confirms_no_read_asked_after_it_left() {
    let mut group: Cluster<New> = Cluster::new(3, &[1, 2, 3], Settings::focal(), 7);
    elect(&mut group, 1);
    group.act(&Op::Propose(1, b"before".to_vec()));
    quiet(&mut group);
    group.act(&Op::Read(1, b"first".to_vec()));
    let heartbeat = MessageType::MsgHeartbeat;
    let answer = MessageType::MsgHeartbeatResponse;
    assert_eq!(
        deliver(&mut group, |message| message.msg_type == heartbeat),
        2
    );
    // The answers wait on the network while the leader is cut off.
    let held: Vec<Message> = group
        .net
        .iter()
        .filter(|message| {
            message.msg_type == answer
                && hyper_raft::read::ReadOnly::of_round(&message.context)
                    .is_some_and(|(context, _)| context == b"first")
        })
        .cloned()
        .collect();
    assert_eq!(held.len(), 2);
    group.net.clear();
    separate(&mut group, 1);
    for _ in 0..64 {
        ticks(&mut group, &[2, 3], 1);
        if group.leaders_now().iter().any(|leader| *leader != 1) {
            break;
        }
    }
    let leader = group
        .leaders_now()
        .into_iter()
        .find(|leader| *leader != 1)
        .expect("the two that remain elect one of them");
    let committed = group.chosen.len();
    group.act(&Op::Propose(leader, b"after".to_vec()));
    quiet(&mut group);
    assert!(group.chosen.len() > committed);
    // The old leader still believes it leads, and is asked again.
    assert!(group.leaders_now().contains(&1));
    group.act(&Op::Read(1, b"second".to_vec()));
    group.net.clear();
    group.act(&Op::Heal);
    group.net.extend(held);
    assert_eq!(deliver(&mut group, |message| message.msg_type == answer), 2);
    // The first read was answered; the second waits for a round of its own,
    // which the members that follow another leader will not answer.
    assert_eq!(group.answered, 1);
    quiet(&mut group);
    assert!(group.settles());
    assert_eq!(group.answered, 1);
}

/// A member that may not campaign refuses no one for priority. Two voters; the one of higher
/// priority leads and removes itself. The other holds the removal and has not heard it committed.
/// It counts by the newest configuration its log holds, in which it is the one voter: told to
/// campaign, it is elected by its own vote and leads alone, needing nothing of the member that
/// left. (By the configuration it had applied, it needed the first one's vote, which a priority
/// refused, and the group had no leader for good: found by a schedule of three thousand.)
#[test]
fn a_member_that_left_refuses_no_one_for_priority() {
    let mut group: Cluster<New> = Cluster::new(2, &[1, 2], Settings::focal(), 11);
    elect(&mut group, 2);
    group.act(&Op::Priority(2, 3));
    group.act(&Op::Propose(2, b"before".to_vec()));
    quiet(&mut group);
    group.act(&Op::Change(2, change(ConfChangeType::RemoveNode, 2)));
    // The removal reaches member 1 and its answer reaches member 2, which commits it, applies it
    // and follows, telling member 1 to campaign. The commit is lost.
    let append = MessageType::MsgAppend;
    let answer = MessageType::MsgAppendResponse;
    let campaign = MessageType::MsgTimeoutNow;
    let to = |kind: MessageType, member: u64| {
        move |message: &Message| message.msg_type == kind && message.to == member
    };
    assert!(deliver(&mut group, to(append, 1)) > 0);
    assert!(deliver(&mut group, to(answer, 2)) > 0);
    assert_eq!(deliver(&mut group, to(campaign, 1)), 1);
    group.net.clear();
    let left = group.peek(2).unwrap().view();
    let stayed = group.peek(1).unwrap().view();
    assert!(!left.promotable && stayed.promotable);
    // Member 1 is elected by its own vote, the one its newest configuration counts.
    assert_eq!(group.leaders_now(), vec![1]);
    assert!(group.settles());
    assert_eq!(group.leaders_now(), vec![1]);
}

fn change(kind: ConfChangeType, member: u64) -> ConfChangeV2 {
    ConfChangeV2 {
        changes: vec![ConfChangeSingle {
            change_type: kind,
            node_id: member,
        }],
        ..ConfChangeV2::default()
    }
}
fn separate<R: Replica>(group: &mut Cluster<R>, member: u64) {
    for other in group.ids() {
        if other != member {
            group.act(&Op::Block(member, other));
            group.act(&Op::Block(other, member));
        }
    }
}

#[test]
fn a_leader_a_change_leaves_no_voter_hands_the_group_over_and_follows() {
    for demoted in [false, true] {
        let mut group: Cluster<New> = Cluster::new(3, &[1, 2, 3], Settings::focal(), 7);
        elect(&mut group, 1);
        let term = group.peek(1).unwrap().view().term;
        let kind = if demoted {
            ConfChangeType::AddLearnerNode
        } else {
            ConfChangeType::RemoveNode
        };
        let reports = group.act(&Op::Change(1, change(kind, 1)));
        assert_eq!(reports[0].accepted, Some(true));
        // No tick passes: the group does not wait out an election timeout.
        quiet(&mut group);
        let view = group.peek(1).unwrap().view();
        assert_eq!(view.role, 0, "the member that left still leads");
        assert!(!view.promotable);
        let leaders = group.leaders_now();
        assert_eq!(leaders.len(), 1);
        let leader = leaders[0];
        assert!(leader == 2 || leader == 3);
        assert_eq!(group.peek(leader).unwrap().view().term, term + 1);
        let conf = group.disk(leader).conf.clone();
        assert_eq!(conf.voters, vec![2, 3]);
        assert_eq!(conf.learners, if demoted { vec![1] } else { vec![] });
        let reports = group.act(&Op::Propose(leader, b"after".to_vec()));
        assert_eq!(reports[0].accepted, Some(true));
        quiet(&mut group);
        let index = group.peek(leader).unwrap().view().last_index;
        for member in [2, 3] {
            assert_eq!(group.peek(member).unwrap().app().index, index);
        }
        // A learner is sent what is committed; one that is removed is not.
        let applied = group.peek(1).unwrap().app().index;
        assert_eq!(applied == index, demoted);
        // One that is no voter does not campaign, whoever asks.
        assert_eq!(group.act(&Op::Campaign(1))[0].accepted, Some(false));
        ticks(&mut group, &[1], 40);
        assert_eq!(group.peek(1).unwrap().view().role, 0);
        assert_eq!(group.leaders_now(), vec![leader]);
    }
}

#[test]
fn a_leader_leaves_a_joint_configuration_it_is_no_part_of_after() {
    let mut group: Cluster<New> = Cluster::new(5, &[1, 2, 3], Settings::focal(), 11);
    elect(&mut group, 1);
    let replace = ConfChangeV2 {
        transition: ConfChangeTransition::Auto,
        changes: vec![
            ConfChangeSingle {
                change_type: ConfChangeType::AddNode,
                node_id: 4,
            },
            ConfChangeSingle {
                change_type: ConfChangeType::AddNode,
                node_id: 5,
            },
            ConfChangeSingle {
                change_type: ConfChangeType::RemoveNode,
                node_id: 1,
            },
        ],
        context: vec![],
    };
    assert_eq!(group.act(&Op::Change(1, replace))[0].accepted, Some(true));
    quiet(&mut group);
    // It led the group through the joint configuration and out of it, and
    // then handed it over.
    let leaders = group.leaders_now();
    assert_eq!(leaders.len(), 1);
    assert_ne!(leaders[0], 1);
    let conf = group.disk(leaders[0]).conf.clone();
    assert_eq!(conf.voters, vec![2, 3, 4, 5]);
    assert!(conf.voters_outgoing.is_empty() && !conf.auto_leave);
    assert_eq!(
        group.act(&Op::Propose(leaders[0], b"after".to_vec()))[0].accepted,
        Some(true)
    );
    quiet(&mut group);
    let index = group.peek(leaders[0]).unwrap().view().last_index;
    for member in [2, 3, 4, 5] {
        assert_eq!(group.peek(member).unwrap().app().index, index);
    }
}

/// Two voters whose logs are equally long and end in different terms, the
/// third away. The one of higher priority holds the older log.
fn divided<R: Replica>(settings: Settings) -> Cluster<R> {
    let mut group: Cluster<R> = Cluster::new(3, &[1, 2, 3], settings, 3);
    elect(&mut group, 1);
    group.act(&Op::Propose(1, b"held by all".to_vec()));
    quiet(&mut group);
    separate(&mut group, 1);
    // An entry only the old leader holds.
    group.act(&Op::Propose(1, b"held by one".to_vec()));
    group.net.clear();
    // The others elect, and the new leader's first entry takes the index.
    ticks(&mut group, &[2, 3], 25);
    group.act(&Op::Campaign(3));
    quiet(&mut group);
    ticks(&mut group, &[2, 3], 25);
    let leader = *group
        .leaders_now()
        .iter()
        .find(|id| **id != 1)
        .expect("the two elect");
    let other = if leader == 2 { 3 } else { 2 };
    let (old, new) = (
        group.peek(1).unwrap().view(),
        group.peek(leader).unwrap().view(),
    );
    assert_eq!(old.last_index, new.last_index);
    assert!(new.term > old.term);
    // The follower goes away, and the leader stops and opens again.
    group.stop(other);
    group.act(&Op::Restart(leader));
    group.act(&Op::Heal);
    group.net.clear();
    // The old leader finds it leads no quorum.
    ticks(&mut group, &[1], 25);
    group.net.clear();
    assert!(group.leaders_now().is_empty());
    group.act(&Op::Priority(1, 3));
    group.act(&Op::Priority(leader, 1));
    group
}
fn elects<R: Replica>(group: &mut Cluster<R>) -> bool {
    let up = group.up();
    for _ in 0..400 {
        for member in &up {
            group.act(&Op::Tick(*member));
        }
        quiet(group);
        if !group.leaders_now().is_empty() {
            return true;
        }
    }
    false
}

#[test]
fn priority_yields_to_a_log_that_is_more_current() {
    // By length alone the two refuse each other for as long as the third
    // is away: the one for priority, the other for the log.
    let mut group: Cluster<New> = divided(Settings::shell());
    assert!(!elects(&mut group), "the rule of raft-rs elected");
    let mut group: Cluster<Old> = divided(Settings::shell());
    assert!(!elects(&mut group), "raft-rs elected");
    // A voter that could not be elected itself does not refuse.
    let mut group: Cluster<New> = divided(Settings::focal());
    assert!(elects(&mut group));
    let leader = group.leaders_now()[0];
    assert_ne!(leader, 1);
    assert_eq!(
        group.act(&Op::Propose(leader, b"led".to_vec()))[0].accepted,
        Some(true)
    );
    quiet(&mut group);
    assert_eq!(
        group.peek(1).unwrap().app(),
        group.peek(leader).unwrap().app()
    );
}

#[test]
fn priority_orders_an_election_and_never_judges_a_transfer() {
    let mut group: Cluster<New> = Cluster::new(3, &[1, 2, 3], Settings::focal(), 5);
    elect(&mut group, 1);
    group.act(&Op::Propose(1, b"x".to_vec()));
    quiet(&mut group);
    group.act(&Op::Priority(2, 3));
    group.act(&Op::Priority(3, 1));
    group.act(&Op::Priority(1, 1));
    // Without the leader's vote the one of higher priority decides.
    group.act(&Op::Block(1, 3));
    group.act(&Op::Block(3, 1));
    group.act(&Op::Transfer(1, 3));
    // The leader tells the member to campaign; it cannot reach it.
    assert!(group.leaders_now() == vec![1]);
    group.act(&Op::Heal);
    group.net.clear();
    group.act(&Op::Transfer(1, 2));
    group.act(&Op::Transfer(1, 3));
    let told: Vec<_> = group
        .net
        .iter()
        .filter(|message| message.msg_type == MessageType::MsgTimeoutNow)
        .map(|message| message.to)
        .collect();
    assert_eq!(told, vec![2, 3]);
    // Only the second is delivered, and then the old leader is cut off.
    group.net.retain(|message| message.to == 3);
    group.act(&Op::Deliver {
        at: 0,
        keep: false,
        lose: false,
    });
    separate(&mut group, 1);
    group.act(&Op::Block(1, 2));
    quiet(&mut group);
    assert!(
        group.leaders_now().contains(&3),
        "a member of higher priority refused the transfer"
    );
    // An election of its own it would have lost to priority.
    let mut group: Cluster<New> = Cluster::new(3, &[1, 2, 3], Settings::focal(), 5);
    elect(&mut group, 1);
    group.act(&Op::Priority(2, 3));
    group.act(&Op::Priority(3, 1));
    separate(&mut group, 1);
    ticks(&mut group, &[2, 3], 9);
    group.act(&Op::Campaign(3));
    quiet(&mut group);
    assert!(!group.leaders_now().contains(&3));
    assert!(elects(&mut group));
    assert!(group.leaders_now().contains(&2));
}

#[test]
fn a_member_that_has_no_term_refuses_no_one_for_priority() {
    // `raft-rs` unwinds here: the refusal would bear no term.
    let mut group: Cluster<New> = Cluster::new(3, &[1, 2, 3], Settings::focal(), 9);
    for member in [1, 2, 3] {
        group.act(&Op::Priority(member, member as i64));
    }
    elect(&mut group, 1);
    assert_eq!(group.peek(3).unwrap().view().leader, 1);
}

/// A member that moved its term past its leader's answers that leader's append or heartbeat with
/// its term, so the leader steps down (thesis Figure 3.1, "reply false if term < currentTerm"),
/// with check-quorum and pre-vote off too. raft-rs answers only under one of them and leaves the
/// rest to vote requests, which never reach a leader the member's configuration names no voter:
/// the swarm's fast seed 3,112 (`docs/sim.md` §15.9) found such a leader leading its old term for
/// ever, its group never converging. Here member 3 moves to term 3 while every message it sends is
/// lost; member 1's heartbeat of term 1 then reaches it.
#[test]
fn a_member_of_a_later_term_answers_a_leader_of_an_earlier_one() {
    let settings = Settings {
        check_quorum: false,
        pre_vote: false,
        ..Settings::focal()
    };
    let mut group: Cluster<New> = Cluster::new(3, &[1, 2, 3], settings, 0);
    let deliver_all = |group: &mut Cluster<New>,
                       lose: &dyn Fn(&hyper_raft::proto::Message) -> bool| {
        for _ in 0..1_000 {
            let Some(message) = group.net.first() else {
                return;
            };
            let lose = lose(message);
            group.act(&Op::Deliver {
                at: 0,
                keep: false,
                lose,
            });
        }
        panic!("the network does not fall quiet");
    };
    group.act(&Op::Campaign(1));
    deliver_all(&mut group, &|_| false);
    assert_eq!(group.leaders_now(), vec![1]);
    for _ in 0..2 {
        group.act(&Op::Campaign(3));
        deliver_all(&mut group, &|message| message.from == 3);
    }
    assert_eq!(group.peek(3).unwrap().view().term, 3);
    for _ in 0..group.settings.heartbeat_tick {
        group.act(&Op::Tick(1));
    }
    let at = group
        .net
        .iter()
        .position(|m| m.from == 1 && m.to == 3)
        .expect("member 1's heartbeat to member 3");
    let reports = group.act(&Op::Deliver {
        at,
        keep: false,
        lose: false,
    });
    let answered = reports.iter().any(|report| {
        report.member == 3
            && report.output.messages.iter().any(|m| {
                m.to == 1
                    && m.term == 3
                    && m.msg_type == hyper_raft::proto::MessageType::MsgAppendResponse
            })
    });
    assert!(answered, "member 3 answered nothing: {reports:?}");
    deliver_all(&mut group, &|_| false);
    assert!(
        !group.leaders_now().contains(&1),
        "member 1 still leads term 1"
    );
}

/// A member whose log holds a change of the voters that it has not committed counts by the
/// configuration before it. Here the group goes from five voters to `{2, 3, 5}` through a joint
/// configuration (members 1 and 4 removed) and then to `{2, 3}`. Member 1 takes the joint
/// configuration's two entries in its log with a commit of 1 (an append that arrived ahead of a
/// hole and a stale one that filled it). It campaigns by the five voters it applied, and members
/// 4 and 5, removed or about to be and with logs no longer than its own, elect it: three of five.
/// Member 3 leads the same term by `{2, 3}`.
#[test]
fn a_member_that_holds_a_change_it_has_not_committed_leads_no_term_another_leads() {
    let settings = Settings {
        pre_vote: false,
        check_quorum: false,
        max_inflight_bytes: u64::MAX,
        ..Settings::focal()
    };
    let mut group: Cluster<New> = Cluster::new(5, &[1, 2, 3, 4, 5], settings, 13);
    elect(&mut group, 3);
    let leave = ConfChangeV2 {
        transition: ConfChangeTransition::Auto,
        changes: vec![
            ConfChangeSingle {
                change_type: ConfChangeType::RemoveNode,
                node_id: 1,
            },
            ConfChangeSingle {
                change_type: ConfChangeType::RemoveNode,
                node_id: 4,
            },
        ],
        context: vec![],
    };
    assert_eq!(group.act(&Op::Change(3, leave))[0].accepted, Some(true));
    // The joint configuration's entry reaches 2, 4 and 5, and is committed by both halves; the
    // entry that leaves it follows, and is committed too. Member 1's appends wait.
    let to_one = |m: &Message| m.to == 1 || m.from == 1;
    for _ in 0..20 {
        deliver(&mut group, |m| !to_one(m));
        group.act(&Op::Tick(3));
    }
    deliver(&mut group, |m| !to_one(m));
    let three = group.disk(3).conf.clone();
    assert_eq!(three.voters, vec![2, 3, 5]);
    assert!(three.voters_outgoing.is_empty());
    // The append of the leaving entry arrives first and is kept ahead of the hole; the joint
    // entry's, sent with a commit of 1, fills the hole.
    let appends: Vec<usize> = group
        .net
        .iter()
        .enumerate()
        .filter(|(_, m)| m.to == 1 && m.msg_type == MessageType::MsgAppend)
        .map(|(at, _)| at)
        .collect();
    assert!(appends.len() >= 2, "{:?}", group.net);
    let later = *appends.last().unwrap();
    group.act(&Op::Deliver {
        at: later,
        keep: false,
        lose: false,
    });
    let first = group
        .net
        .iter()
        .position(|m| m.to == 1 && m.msg_type == MessageType::MsgAppend)
        .unwrap();
    group.act(&Op::Deliver {
        at: first,
        keep: false,
        lose: false,
    });
    group.net.retain(|m| !to_one(m));
    let one = group.peek(1).unwrap().view();
    assert_eq!(one.last_index, group.peek(4).unwrap().view().last_index);
    assert_eq!(one.commit, 1);
    // Then member 5 leaves: `{2, 3}` commits it alone.
    assert_eq!(
        group.act(&Op::Change(3, change(ConfChangeType::RemoveNode, 5)))[0].accepted,
        Some(true)
    );
    for _ in 0..20 {
        deliver(&mut group, |m| {
            [2, 3].contains(&m.to) && [2, 3].contains(&m.from)
        });
    }
    assert_eq!(group.disk(3).conf.voters, vec![2, 3]);
    group.net.clear();
    // Member 3 leads the next term by 2's vote.
    group.act(&Op::Campaign(3));
    deliver(&mut group, |m| {
        [2, 3].contains(&m.to) && [2, 3].contains(&m.from)
    });
    let term = group.peek(3).unwrap().view().term;
    assert_eq!(group.leaders_now(), vec![3]);
    group.net.clear();
    // Member 1 campaigns by the five voters it applied; 4 and 5 are asked.
    group.act(&Op::Campaign(1));
    deliver(&mut group, |m| {
        [1, 4, 5].contains(&m.to) && [1, 4, 5].contains(&m.from)
    });
    let one = group.peek(1).unwrap().view();
    assert!(
        !(one.role == 2 && one.term == term),
        "members 1 and 3 both lead term {term}"
    );
}
