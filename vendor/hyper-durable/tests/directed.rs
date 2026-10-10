//! The commit fence (`docs/durable.md` §4.1, I5) on hyper-log over simulated devices, cut at
//! every write and flush in turn: mantle's four directed cases (mantle `crates/range/tests/
//! sim.rs` at `1c179e8`: a founder removing its only peer, a leader and a follower of three
//! inside a removal, a sole voter adding a learner) and focal's two F17 cases (the founder; a
//! host that acted on a fence and must not start below it), re-expressed against this shell.
//!
//! Each run: member 1 is elected; the target's device cuts the power at its `ops`-th write or
//! flush after the action is asked; the target crashes keeping nothing it did not flush and
//! reopens. Then, alone, before hearing from anyone, it reaches from its own durable state every
//! configuration it applied and every fence it acted on; and the group goes on: a leader is
//! elected, the action is made, and an entry commits on every member.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity,
    clippy::unreachable
)]

mod support;

use hyper_block::sim::{Crash, Fault};
use hyper_durable::ReplicaError;
use hyper_raft::proto::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState,
};
use support::device::{Action, Case, Directed, PATIENT_ROUNDS, Takes, same_configuration};

fn simple_change(kind: ConfChangeType, node_id: u64) -> ConfChangeV2 {
    ConfChangeV2 {
        transition: ConfChangeTransition::Auto,
        changes: vec![ConfChangeSingle {
            change_type: kind,
            node_id,
        }],
        context: Vec::new(),
    }
}

const FENCE: &[u8] = b"fence:upgrade-7";

/// Whether the action is made everywhere: the configuration on every member and the removed
/// member stopped, or the fence applied by every member.
fn made(d: &mut Directed, case: &Case) -> bool {
    match &case.action {
        Action::Change { made, removes, .. } => {
            d.devices.iter().all(|dev| {
                dev.replica
                    .as_ref()
                    .is_some_and(|r| same_configuration(r.configuration(), made))
            }) && removes.is_none_or(|removed| d.devices.iter().all(|dev| dev.id != removed))
        }
        Action::Fence => d.devices.iter().all(|dev| {
            dev.replica.as_ref().is_some_and(|r| {
                r.machine()
                    .now
                    .entries
                    .iter()
                    .any(|(_, _, data)| data == FENCE)
            })
        }),
    }
}

/// Asks the leader for the action; a refusal (no leader yet, a change pending) is asked again.
fn ask(d: &mut Directed, case: &Case) {
    let Some(leader) = d.leader() else {
        return;
    };
    let asked = match &case.action {
        Action::Change { change, .. } => leader.change(Vec::new(), change),
        Action::Fence => leader.propose(Vec::new(), FENCE.to_vec()),
    };
    match asked {
        Ok(()) | Err(ReplicaError::Refused(_) | ReplicaError::Stalled) => {}
        Err(e) => panic!("the leader refused the action: {e}"),
    }
}

/// One directed run: true when the power was cut; false for `Some(ops)` past the last write or
/// flush the run makes.
fn directed(case: &Case, takes: Takes, ops: Option<u64>, seed: u64) -> bool {
    let mut d = Directed::new(case, seed);
    d.device(1).unwrap().r().campaign().unwrap();
    let mut elected = false;
    for _ in 0..PATIENT_ROUNDS {
        assert!(d.round(case, Takes::Waiting).is_none());
        if d.device(1).unwrap().r().is_leader() {
            elected = true;
            break;
        }
    }
    assert!(elected, "member 1 is never elected");
    d.measured();
    if let Some(ops) = ops {
        d.device(case.target)
            .unwrap()
            .inject(Fault::PowerCut { ops });
    }
    ask(&mut d, case);
    let mut cut = None;
    let mut quiet = 0;
    for _ in 0..PATIENT_ROUNDS {
        cut = d.round(case, takes);
        if cut.is_some() {
            break;
        }
        if made(&mut d, case) {
            quiet += 1;
            if quiet > 4 {
                break;
            }
        }
    }
    let at_cut = match (cut, ops) {
        (Some(conf), _) => conf,
        (None, Some(_)) => {
            assert!(
                made(&mut d, case),
                "{case:?} {takes:?} never made its action"
            );
            return false;
        }
        (None, None) => {
            assert!(
                made(&mut d, case),
                "{case:?} {takes:?} never made its action"
            );
            d.device(case.target).unwrap().r().configuration().clone()
        }
    };
    let now = d.now();
    d.device(case.target).unwrap().crash(Crash::LoseAll);
    d.restarted(case.target);
    let target = d.device(case.target).unwrap();
    // Alone, the target reaches what its own durable state says is committed.
    let mut ignored = Vec::new();
    let mut settled = false;
    for _ in 0..PATIENT_ROUNDS {
        let before = target.r().applied();
        assert!(target.settle(now, &mut ignored).is_none(), "a second cut");
        if target.r().applied() == before {
            settled = true;
            break;
        }
    }
    assert!(settled, "the target alone never stops applying");
    let mut found = Vec::new();
    let reopened: ConfState = target.r().configuration().clone();
    if let Action::Change { made, .. } = &case.action
        && same_configuration(&at_cut, made)
        && !same_configuration(&reopened, made)
    {
        found.push(format!(
            "member {} applied {at_cut:?} and acted on it, but its durable state reopens at \
             {reopened:?}",
            case.target
        ));
    }
    let acted = std::mem::take(&mut target.acted_before);
    let applied = &target.r().machine().now.entries;
    for index in acted {
        if !applied.iter().any(|(i, _, _)| *i == index) {
            found.push(format!(
                "member {} acted at start on {index}, but reopens below it",
                case.target
            ));
        }
    }
    // The group goes on.
    let mut live = false;
    let mut proposed = false;
    for round in 0..PATIENT_ROUNDS {
        assert!(d.round(case, takes).is_none(), "a second cut");
        if round % 20 == 0 && !made(&mut d, case) {
            ask(&mut d, case);
        }
        if !proposed && let Some(leader) = d.leader() {
            proposed = leader.propose(Vec::new(), b"after".to_vec()).is_ok();
        }
        let everywhere = d.devices.iter().all(|dev| {
            dev.replica.as_ref().is_some_and(|r| {
                r.machine()
                    .now
                    .entries
                    .iter()
                    .any(|(_, _, data)| data == b"after")
            })
        });
        if proposed && made(&mut d, case) && everywhere {
            live = true;
            break;
        }
    }
    if !live {
        found.push("the group never committed an entry on every member".to_string());
    }
    assert!(
        found.is_empty(),
        "{case:?}, {takes:?}, power cut at operation {ops:?}: {found:#?}"
    );
    true
}

/// Runs `case` with the power cut at each of the target's writes and flushes after the action
/// is asked, in turn, and once with no cut, both ways of taking readies.
fn every_cut(case: &Case) -> u64 {
    /// A run makes a few dozen writes and flushes; this bounds the enumeration far past them.
    const MAX_OPS: u64 = 1 << 12;
    let seed = 0x51;
    let mut cuts = 0;
    for takes in [Takes::Waiting, Takes::Overlapping] {
        let mut ops = 0;
        while directed(case, takes, Some(ops), seed) {
            ops += 1;
            assert!(ops < MAX_OPS, "{case:?} never ran out of operations");
        }
        assert!(ops > 0, "{case:?} {takes:?} cut the power nowhere");
        cuts += ops;
        directed(case, takes, None, seed);
    }
    println!("{case:?}: cut at {cuts} operations");
    cuts
}

fn removal(members: u64, target: u64, removes: u64) -> Case {
    Case {
        members,
        target,
        action: Action::Change {
            change: simple_change(ConfChangeType::RemoveNode, removes),
            made: ConfState {
                voters: (1..=members).filter(|&m| m != removes).collect(),
                ..ConfState::default()
            },
            removes: Some(removes),
        },
    }
}

/// focal F17 (`cli_network`) and mantle's first case: the founder of a group of two removes its
/// only peer, which the operator stops once the founder says the change is known; cut anywhere,
/// the founder must still elect itself.
#[test]
fn a_founder_that_removes_its_only_peer_elects_itself_after_a_cut_anywhere() {
    every_cut(&removal(2, 1, 2));
}

/// A leader of three removes a member; the leader's power is cut anywhere in the change.
#[test]
fn a_leader_cut_inside_a_change_reopens_with_what_it_applied() {
    every_cut(&removal(3, 1, 3));
}

/// A follower of three applies the removal of another; its power is cut anywhere in the change.
#[test]
fn a_follower_cut_inside_a_change_reopens_with_what_it_applied() {
    every_cut(&removal(3, 2, 3));
}

/// The sole voter of a group adds a learner, a change it commits alone (its own write states
/// `commit = last`); its power is cut anywhere in the change.
#[test]
fn a_sole_voter_cut_inside_a_change_reopens_with_what_it_applied() {
    every_cut(&Case {
        members: 1,
        target: 1,
        action: Action::Change {
            change: simple_change(ConfChangeType::AddLearnerNode, 2),
            made: ConfState {
                voters: vec![1],
                learners: vec![2],
                ..ConfState::default()
            },
            removes: None,
        },
    });
}

/// focal F17 (`cli_upgrade`): a member that acted on a fence, an entry it acts on at its next
/// start, never reopens below it, for a follower and for the leader, cut anywhere.
#[test]
fn a_host_that_honoured_a_fence_never_starts_below_it() {
    for target in [2, 1] {
        every_cut(&Case {
            members: 3,
            target,
            action: Action::Fence,
        });
    }
}
