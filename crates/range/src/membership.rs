//! Replacing a failed member of a range's group (docs/design/replica.md §6).
//!
//! A member that lost its persistent state cannot rejoin under its identity; it is replaced
//! by a member with a new one through a membership change (docs/research/06 §A1.2, Diss §3.8).
//! The new member joins as a learner, which votes on nothing, and catches up; then one joint
//! change makes it a voter and removes the failed member, and the group leaves the joint
//! configuration by itself (06 §A1.7; Diss §4.2.1, §4.3). The group's voters never drop below those it
//! had while the failed member is still counted, and at no point does a quorum depend on a
//! member that has not caught up.
//!
//! A replacement ends only once every voter of the final configuration has learned that it
//! committed ([`crate::Replica::configuration_known`]): a member counts by the newest
//! configuration its log states, and one whose log lacks the entry still counts the member
//! removed, so losing another member before then could leave no quorum to elect a leader.
//!
//! A [`Replacement`] is stateless: each call reads the configuration the leader has applied
//! and says what to propose next. A proposal a leader drops, or one lost with a leader, is
//! proposed again the next time, and one already applied is not.
//!
//! [`Toward`] steps a range's configuration toward the cell's members as its root range holds them
//! (docs/design/membership.md §6): each member joins as a learner, which votes on nothing, and is
//! made a voter once caught up (Raft dissertation §4.2.1); a member the target makes a learner is
//! demoted and one it names nowhere removed, one server at a time (dissertation §4.1; research/06
//! §A1.7). Adds come before removals, so the voters never fall below the smaller of the old and
//! new sets on the way. It is stateless too: the caller proposes a step only while no change is
//! pending (hyper-raft's `has_pending_conf`), and a new leader takes up the target from the
//! configuration it finds.

use hyper_raft::proto::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState,
};

/// Replacing `failed` with `joining`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Replacement {
    failed: u64,
    joining: u64,
}

/// What a replacement asks of the group's leader next.
#[derive(Debug, Clone, PartialEq)]
pub enum Next {
    /// Propose this change.
    Propose(ConfChangeV2),
    /// Nothing until the configuration changes: the joining member is catching up, or the
    /// group is leaving a joint configuration by itself.
    Wait,
    /// The joining member votes, the failed one is gone, and every voter knows it.
    Done,
}

impl Replacement {
    /// Replacing `failed` with `joining`; `None` if either is zero or they are the same
    /// member, since an identity is never reused.
    pub fn new(failed: u64, joining: u64) -> Option<Self> {
        if failed != 0 && joining != 0 && failed != joining {
            Some(Self { failed, joining })
        } else {
            None
        }
    }

    pub fn failed(&self) -> u64 {
        self.failed
    }

    pub fn joining(&self) -> u64 {
        self.joining
    }

    /// What to propose given the configuration the leader has applied, `conf`; whether the
    /// joining member has caught up with the leader ([`crate::Replica::caught_up`]); and
    /// whether every voter knows `conf` committed ([`crate::Replica::configuration_known`]).
    pub fn next(&self, conf: &ConfState, caught_up: bool, known: bool) -> Next {
        if !conf.voters_outgoing.is_empty() || !conf.learners_next.is_empty() {
            return Next::Wait;
        }
        let failed_member =
            conf.voters.contains(&self.failed) || conf.learners.contains(&self.failed);
        if conf.voters.contains(&self.joining) {
            if failed_member {
                Next::Propose(change(&[(ConfChangeType::RemoveNode, self.failed)]))
            } else if known {
                Next::Done
            } else {
                Next::Wait
            }
        } else if conf.learners.contains(&self.joining) {
            if !caught_up {
                Next::Wait
            } else if failed_member {
                Next::Propose(change(&[
                    (ConfChangeType::AddNode, self.joining),
                    (ConfChangeType::RemoveNode, self.failed),
                ]))
            } else {
                Next::Propose(change(&[(ConfChangeType::AddNode, self.joining)]))
            }
        } else {
            Next::Propose(change(&[(ConfChangeType::AddLearnerNode, self.joining)]))
        }
    }
}

/// A range's configuration as the cell's members would have it (docs/design/membership.md §6): the
/// members that vote, and those that only learn, each sorted, none in both.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Toward {
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
}

/// What stepping toward a target asks of the range's leader next.
#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    /// Propose this change.
    Propose(ConfChangeV2),
    /// Hand the range's lead to this voter, which the target keeps, first: the target drops the
    /// leader itself (dissertation §4.2.2 prefers a transfer).
    Transfer(u64),
    /// Nothing until the configuration changes: a joint configuration being left, or a learner
    /// catching up.
    Wait,
    /// The configuration is the target's.
    Done,
}

impl Toward {
    /// The next step from `conf`, the configuration the leader `leader` applied, toward this
    /// target; `caught_up` says whether a member holds everything the leader knows committed
    /// ([`crate::Replica::caught_up`]). One server at a time: a target member that is no member
    /// joins as a learner; a learner the target makes a voter is promoted once caught up; a
    /// learner the target names nowhere is removed; and, once every learner the target makes a
    /// voter votes, a voter the target makes a learner is demoted and one it names nowhere
    /// removed. The leader is never demoted or removed by its own change, so no change leaves the
    /// range without a voter: once nothing else is left, a leader the target drops hands the lead
    /// to a caught-up voter the target keeps, and waits where none is caught up.
    pub fn step(&self, conf: &ConfState, leader: u64, caught_up: impl Fn(u64) -> bool) -> Step {
        if !conf.voters_outgoing.is_empty() || !conf.learners_next.is_empty() {
            return Step::Wait;
        }
        let member = |id: &u64| conf.voters.contains(id) || conf.learners.contains(id);
        let named = |id: &u64| self.voters.contains(id) || self.learners.contains(id);
        if let Some(&joining) = self
            .voters
            .iter()
            .chain(&self.learners)
            .filter(|id| !member(id))
            .min()
        {
            return Step::Propose(change(&[(ConfChangeType::AddLearnerNode, joining)]));
        }
        let mut catching_up = false;
        for &learner in conf.learners.iter().filter(|id| self.voters.contains(id)) {
            if caught_up(learner) {
                return Step::Propose(change(&[(ConfChangeType::AddNode, learner)]));
            }
            catching_up = true;
        }
        // A learner the target names nowhere goes at any time: it counts toward no quorum.
        if let Some(&removed) = conf.learners.iter().filter(|id| !named(id)).min() {
            return Step::Propose(change(&[(ConfChangeType::RemoveNode, removed)]));
        }
        // A voter goes only once every member the target makes a voter votes.
        if catching_up {
            return Step::Wait;
        }
        if let Some(&demoted) = conf
            .voters
            .iter()
            .filter(|id| **id != leader && self.learners.contains(id))
            .min()
        {
            return Step::Propose(change(&[(ConfChangeType::AddLearnerNode, demoted)]));
        }
        if let Some(&removed) = conf
            .voters
            .iter()
            .filter(|id| **id != leader && !named(id))
            .min()
        {
            return Step::Propose(change(&[(ConfChangeType::RemoveNode, removed)]));
        }
        if self.voters.contains(&leader) {
            return Step::Done;
        }
        match conf
            .voters
            .iter()
            .copied()
            .filter(|id| *id != leader && self.voters.contains(id) && caught_up(*id))
            .min()
        {
            Some(heir) => Step::Transfer(heir),
            None => Step::Wait,
        }
    }
}

/// How many more failures a range of `voters` voters, `absent` of them away, survives: the live
/// voters past its quorum's majority, none where it has no quorum now (docs/design/membership.md
/// §5; Raft commits and elects by a majority of the voters, dissertation §3).
pub fn tolerance(voters: usize, absent: usize) -> usize {
    let quorum = (voters / 2).saturating_add(1);
    voters.saturating_sub(absent).saturating_sub(quorum)
}

/// Whether a range of `voters` voters drops its `absent` absent ones (docs/design/membership.md
/// §5): only where the range then survives more failures than it does keeping them.
pub fn drops_absent(voters: usize, absent: usize) -> bool {
    absent > 0
        && absent < voters
        && tolerance(voters.saturating_sub(absent), 0) > tolerance(voters, absent)
}

/// One entry of `changes`: a simple change for one, a joint change the group leaves by itself
/// for more.
fn change(changes: &[(ConfChangeType, u64)]) -> ConfChangeV2 {
    ConfChangeV2 {
        transition: ConfChangeTransition::Auto,
        changes: changes
            .iter()
            .map(|&(kind, node_id)| ConfChangeSingle {
                change_type: kind,
                node_id,
            })
            .collect(),
        context: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conf(voters: &[u64], learners: &[u64], outgoing: &[u64]) -> ConfState {
        ConfState {
            voters: voters.to_vec(),
            learners: learners.to_vec(),
            voters_outgoing: outgoing.to_vec(),
            learners_next: Vec::new(),
            auto_leave: !outgoing.is_empty(),
        }
    }

    fn kinds(next: &Next) -> Vec<(ConfChangeType, u64)> {
        match next {
            Next::Propose(c) => c
                .changes
                .iter()
                .map(|s| (s.change_type, s.node_id))
                .collect(),
            _ => Vec::new(),
        }
    }

    const ADD: ConfChangeType = ConfChangeType::AddNode;
    const LEARN: ConfChangeType = ConfChangeType::AddLearnerNode;
    const REMOVE: ConfChangeType = ConfChangeType::RemoveNode;

    #[test]
    fn a_replacement_learns_catches_up_swaps_and_ends() {
        let r = Replacement::new(3, 4).unwrap();
        // The joining member is added as a learner first, caught up or not.
        let start = conf(&[1, 2, 3], &[], &[]);
        assert_eq!(kinds(&r.next(&start, false, true)), [(LEARN, 4)]);
        // It waits as a learner until it has caught up.
        let learning = conf(&[1, 2, 3], &[4], &[]);
        assert_eq!(r.next(&learning, false, true), Next::Wait);
        // Then one joint change promotes it and removes the failed member.
        let swap = r.next(&learning, true, true);
        assert_eq!(kinds(&swap), [(ADD, 4), (REMOVE, 3)]);
        if let Next::Propose(c) = &swap {
            assert_eq!(c.transition, ConfChangeTransition::Auto);
        }
        // The group leaves the joint configuration by itself.
        assert_eq!(
            r.next(&conf(&[1, 2, 4], &[], &[1, 2, 3]), true, true),
            Next::Wait
        );
        // It ends once every voter knows the final configuration committed.
        let end = conf(&[1, 2, 4], &[], &[]);
        assert_eq!(r.next(&end, false, false), Next::Wait);
        assert_eq!(r.next(&end, false, true), Next::Done);
    }

    #[test]
    fn a_replacement_finishes_from_any_configuration_it_meets() {
        let r = Replacement::new(3, 4).unwrap();
        // The failed member already gone: the learner is promoted alone.
        assert_eq!(
            kinds(&r.next(&conf(&[1, 2], &[4], &[]), true, true)),
            [(ADD, 4)]
        );
        // Both voters: the failed one is removed alone.
        assert_eq!(
            kinds(&r.next(&conf(&[1, 2, 3, 4], &[], &[]), false, true)),
            [(REMOVE, 3)]
        );
        // The failed member only a learner is removed too.
        assert_eq!(
            kinds(&r.next(&conf(&[1, 2, 4], &[3], &[]), false, true)),
            [(REMOVE, 3)]
        );
    }

    fn toward(voters: &[u64], learners: &[u64]) -> Toward {
        Toward {
            voters: voters.to_vec(),
            learners: learners.to_vec(),
        }
    }

    fn step_kinds(step: &Step) -> Vec<(ConfChangeType, u64)> {
        match step {
            Step::Propose(c) => c
                .changes
                .iter()
                .map(|s| (s.change_type, s.node_id))
                .collect(),
            _ => Vec::new(),
        }
    }

    /// A member joins as a learner, waits while it catches up, and is promoted alone; each step
    /// one server.
    #[test]
    fn a_member_joins_as_a_learner_and_is_promoted_once_caught_up() {
        let target = toward(&[1, 2, 3, 4], &[]);
        let start = conf(&[1, 2, 3], &[], &[]);
        assert_eq!(step_kinds(&target.step(&start, 1, |_| true)), [(LEARN, 4)]);
        let learning = conf(&[1, 2, 3], &[4], &[]);
        assert_eq!(target.step(&learning, 1, |_| false), Step::Wait);
        assert_eq!(step_kinds(&target.step(&learning, 1, |_| true)), [(ADD, 4)]);
        assert_eq!(
            target.step(&conf(&[1, 2, 3, 4], &[], &[]), 1, |_| true),
            Step::Done
        );
    }

    /// Adds come first; then a member the target names nowhere is removed, one at a time; a
    /// leaving member the target keeps learning is demoted, not removed.
    #[test]
    fn removals_and_demotions_follow_every_add() {
        // 4 joins and 3 leaves: 4 first.
        let target = toward(&[1, 2, 4], &[]);
        let now = conf(&[1, 2, 3], &[], &[]);
        assert_eq!(step_kinds(&target.step(&now, 1, |_| true)), [(LEARN, 4)]);
        let both = conf(&[1, 2, 3, 4], &[], &[]);
        assert_eq!(step_kinds(&target.step(&both, 1, |_| true)), [(REMOVE, 3)]);
        // A member the target keeps as a learner is demoted.
        let learner = toward(&[1, 2], &[3]);
        assert_eq!(step_kinds(&learner.step(&now, 1, |_| true)), [(LEARN, 3)]);
        assert_eq!(
            learner.step(&conf(&[1, 2], &[3], &[]), 1, |_| true),
            Step::Done
        );
        // A learner the target names nowhere is removed.
        let gone = toward(&[1, 2], &[]);
        assert_eq!(
            step_kinds(&gone.step(&conf(&[1, 2], &[3], &[]), 1, |_| true)),
            [(REMOVE, 3)]
        );
    }

    /// The leader the target drops hands the lead to a caught-up voter the target keeps, and
    /// waits where none is; it never removes itself, nor the last voter.
    #[test]
    fn a_dropped_leader_hands_its_lead_first() {
        let target = toward(&[2, 3], &[]);
        let now = conf(&[1, 2, 3], &[], &[]);
        assert_eq!(target.step(&now, 1, |id| id == 3), Step::Transfer(3));
        assert_eq!(target.step(&now, 1, |_| false), Step::Wait);
        // Led by 2 now: 1 is removed.
        assert_eq!(step_kinds(&target.step(&now, 2, |_| true)), [(REMOVE, 1)]);
        // A target of no voter removes nothing that would leave the range without one.
        let none = toward(&[], &[]);
        let alone = conf(&[1], &[2], &[]);
        assert_eq!(step_kinds(&none.step(&alone, 1, |_| true)), [(REMOVE, 2)]);
        assert_eq!(none.step(&conf(&[1], &[], &[]), 1, |_| true), Step::Wait);
        // While a member the target makes a voter catches up, no voter goes.
        let swap = toward(&[1, 2, 4], &[]);
        let learning = conf(&[1, 2, 3], &[4], &[]);
        assert_eq!(swap.step(&learning, 1, |id| id != 4), Step::Wait);
        assert_eq!(step_kinds(&swap.step(&learning, 1, |_| true)), [(ADD, 4)]);
    }

    #[test]
    fn a_joint_configuration_is_left_before_any_step() {
        let target = toward(&[1, 2, 4], &[]);
        assert_eq!(
            target.step(&conf(&[1, 2, 4], &[], &[1, 2, 3]), 1, |_| true),
            Step::Wait
        );
    }

    /// membership.md §5's examples: three voters keep one away, five drop two, four drop one.
    #[test]
    fn absent_voters_are_dropped_only_where_the_range_then_survives_more() {
        assert_eq!(tolerance(3, 0), 1);
        assert_eq!(tolerance(3, 1), 0);
        assert_eq!(tolerance(5, 2), 0);
        assert_eq!(tolerance(3, 2), 0);
        assert!(!drops_absent(3, 1));
        assert!(!drops_absent(5, 1));
        assert!(drops_absent(5, 2));
        assert!(drops_absent(4, 1));
        assert!(!drops_absent(2, 1));
        assert!(!drops_absent(3, 0));
        assert!(!drops_absent(2, 2));
    }

    #[test]
    fn identities_are_never_reused() {
        assert_eq!(Replacement::new(3, 3), None);
        assert_eq!(Replacement::new(0, 4), None);
        assert_eq!(Replacement::new(3, 0), None);
    }
}
