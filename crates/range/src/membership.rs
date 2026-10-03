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
//! committed ([`crate::Replica::configuration_known`]): members apply a configuration when they
//! apply its entry, and one that has not still counts the member removed, so losing another
//! member before then could leave no quorum to elect a leader.
//!
//! A [`Replacement`] is stateless: each call reads the configuration the leader has applied
//! and says what to propose next. A proposal a leader drops, or one lost with a leader, is
//! proposed again the next time, and one already applied is not.

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

    #[test]
    fn identities_are_never_reused() {
        assert_eq!(Replacement::new(3, 3), None);
        assert_eq!(Replacement::new(0, 4), None);
        assert_eq!(Replacement::new(3, 0), None);
    }
}
