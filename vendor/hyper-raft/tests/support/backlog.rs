//! slates' backlog workload (mantle note 32 §2.13, R21; slates
//! `docs/bugs/2026-09-29-a-leaders-commit-rule-scanned-its-backlog.md`, its measurement tool
//! `a_proposal_costs_the_leader_the_same_at_any_backlog`): a leader of five whose followers
//! acknowledge nothing appends proposals, each taken and written out as an owner does
//! (`RawNode::ready`, `advance_append`), its messages dropped. slates' commit rule tried every index
//! from the log's end down to the commit and its configuration lookups scanned the log, so a
//! proposal cost its leader 129 µs at a backlog of 1,000 and 20 ms at 5,000.
use hyper_raft::proto::{ConfState, Message, MessageType};
use hyper_raft::{Config, RawNode, StateRole};

use super::Store;

/// A leader of `voters` elected by the others' votes, with nothing proposed yet.
pub struct Backlog {
    pub leader: RawNode<Store>,
}

impl Backlog {
    pub fn new(voters: u64) -> Self {
        let boot = ConfState {
            voters: (1..=voters).collect(),
            ..ConfState::default()
        };
        let config = Config {
            election_tick: 10,
            heartbeat_tick: 2,
            check_quorum: true,
            pre_vote: true,
            max_size_per_msg: 1 << 20,
            ..Config::new(1, super::limits(voters as usize, 1))
        };
        let leader = RawNode::new(&config, Store::new(boot)).unwrap();
        let mut backlog = Self { leader };
        backlog.leader.campaign().unwrap();
        backlog.flush();
        for (kind, term) in [
            (MessageType::MsgRequestPreVoteResponse, 1),
            (MessageType::MsgRequestVoteResponse, 1),
        ] {
            for voter in 2..=voters {
                let granted = Message {
                    msg_type: kind,
                    from: voter,
                    to: 1,
                    term,
                    ..Message::default()
                };
                // A vote that arrives after the election is decided is dropped by the core.
                let _ = backlog.leader.step(granted);
                backlog.flush();
            }
        }
        assert_eq!(backlog.leader.raft.state(), StateRole::Leader);
        backlog
    }
    /// Takes and writes out what there is, as an owner does; the messages are dropped.
    pub fn flush(&mut self) {
        while self.leader.has_ready() {
            let mut ready = self.leader.ready().unwrap();
            let entries = ready.take_entries();
            let hard = ready.hard_state().copied();
            // What the owner's storage holds is the owner's, not the core's.
            hyper_measure::alloc::aside();
            let disk = &mut self.leader.store_mut().0;
            disk.append(&entries);
            if let Some(hard) = hard {
                disk.hard_state = hard;
            }
            drop(entries);
            hyper_measure::alloc::back();
            let _ = ready.take_messages();
            let _ = ready.take_persisted_messages();
            let _ = self.leader.advance_append(ready).unwrap();
        }
    }
    /// One proposal stating `at`, taken and written out.
    pub fn propose(&mut self, at: u64) {
        self.leader
            .propose(Vec::new(), at.to_le_bytes().to_vec())
            .unwrap();
        self.flush();
    }
    /// The leader's last index less its commit: the backlog its followers have not acknowledged.
    pub fn backlog(&self) -> u64 {
        let log = self.leader.raft.log();
        log.last_index().unwrap() - log.committed()
    }
}
