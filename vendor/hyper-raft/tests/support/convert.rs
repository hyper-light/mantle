//! raft-rs's types and this core's, field by field. raft-rs runs on its own types inside its own
//! cluster; what it is told and what it says cross here, so the two cores are compared as values
//! (`docs/raft.md` §3.1). A change carried in an entry's data is re-encoded on the way, from
//! raft-rs's protocol buffers to this core's format and back, so the entries compare equal.
use hyper_raft::proto::{
    ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState,
    Entry, EntryType, HardState, Message, MessageType, Snapshot, SnapshotMetadata,
};
use hyper_raft::wire::Record;
use raft::prelude as old;
use raft::protocompat::PbMessageExt;

const KINDS: [(MessageType, old::MessageType); 19] = [
    (MessageType::MsgHup, old::MessageType::MsgHup),
    (MessageType::MsgBeat, old::MessageType::MsgBeat),
    (MessageType::MsgPropose, old::MessageType::MsgPropose),
    (MessageType::MsgAppend, old::MessageType::MsgAppend),
    (
        MessageType::MsgAppendResponse,
        old::MessageType::MsgAppendResponse,
    ),
    (
        MessageType::MsgRequestVote,
        old::MessageType::MsgRequestVote,
    ),
    (
        MessageType::MsgRequestVoteResponse,
        old::MessageType::MsgRequestVoteResponse,
    ),
    (MessageType::MsgSnapshot, old::MessageType::MsgSnapshot),
    (MessageType::MsgHeartbeat, old::MessageType::MsgHeartbeat),
    (
        MessageType::MsgHeartbeatResponse,
        old::MessageType::MsgHeartbeatResponse,
    ),
    (
        MessageType::MsgUnreachable,
        old::MessageType::MsgUnreachable,
    ),
    (MessageType::MsgSnapStatus, old::MessageType::MsgSnapStatus),
    (
        MessageType::MsgCheckQuorum,
        old::MessageType::MsgCheckQuorum,
    ),
    (
        MessageType::MsgTransferLeader,
        old::MessageType::MsgTransferLeader,
    ),
    (MessageType::MsgTimeoutNow, old::MessageType::MsgTimeoutNow),
    (MessageType::MsgReadIndex, old::MessageType::MsgReadIndex),
    (
        MessageType::MsgReadIndexResp,
        old::MessageType::MsgReadIndexResp,
    ),
    (
        MessageType::MsgRequestPreVote,
        old::MessageType::MsgRequestPreVote,
    ),
    (
        MessageType::MsgRequestPreVoteResponse,
        old::MessageType::MsgRequestPreVoteResponse,
    ),
];

fn kind_from(kind: i32) -> MessageType {
    KINDS
        .iter()
        .find(|(_, theirs)| *theirs as i32 == kind)
        .map(|(ours, _)| *ours)
        .expect("raft-rs sends only its own kinds")
}
fn kind_to(kind: MessageType) -> i32 {
    KINDS
        .iter()
        .find(|(ours, _)| *ours == kind)
        .map(|(_, theirs)| *theirs as i32)
        .expect("raft-rs is sent no fast-track message")
}

fn change_kind_from(kind: i32) -> ConfChangeType {
    match old::ConfChangeType::from_i32(kind).expect("a kind raft-rs names") {
        old::ConfChangeType::AddNode => ConfChangeType::AddNode,
        old::ConfChangeType::RemoveNode => ConfChangeType::RemoveNode,
        old::ConfChangeType::AddLearnerNode => ConfChangeType::AddLearnerNode,
    }
}
fn change_kind_to(kind: ConfChangeType) -> i32 {
    (match kind {
        ConfChangeType::AddNode => old::ConfChangeType::AddNode,
        ConfChangeType::RemoveNode => old::ConfChangeType::RemoveNode,
        ConfChangeType::AddLearnerNode => old::ConfChangeType::AddLearnerNode,
    }) as i32
}

pub fn conf_from(conf: old::ConfState) -> ConfState {
    ConfState {
        voters: conf.voters,
        learners: conf.learners,
        voters_outgoing: conf.voters_outgoing,
        learners_next: conf.learners_next,
        auto_leave: conf.auto_leave,
    }
}
pub fn conf_to(conf: &ConfState) -> old::ConfState {
    old::ConfState {
        voters: conf.voters.clone(),
        learners: conf.learners.clone(),
        voters_outgoing: conf.voters_outgoing.clone(),
        learners_next: conf.learners_next.clone(),
        auto_leave: conf.auto_leave,
    }
}

pub fn hard_from(hard: &old::HardState) -> HardState {
    HardState {
        term: hard.term,
        vote: hard.vote,
        commit: hard.commit,
    }
}
pub fn hard_to(hard: &HardState) -> old::HardState {
    old::HardState {
        term: hard.term,
        vote: hard.vote,
        commit: hard.commit,
    }
}

pub fn change_from(change: &old::ConfChangeV2) -> ConfChangeV2 {
    ConfChangeV2 {
        transition: match old::ConfChangeTransition::from_i32(change.transition)
            .expect("a transition raft-rs names")
        {
            old::ConfChangeTransition::Auto => ConfChangeTransition::Auto,
            old::ConfChangeTransition::Implicit => ConfChangeTransition::Implicit,
            old::ConfChangeTransition::Explicit => ConfChangeTransition::Explicit,
        },
        changes: change
            .changes
            .iter()
            .map(|single| ConfChangeSingle {
                change_type: change_kind_from(single.change_type),
                node_id: single.node_id,
            })
            .collect(),
        context: change.context.clone(),
    }
}
pub fn change_to(change: &ConfChangeV2) -> old::ConfChangeV2 {
    old::ConfChangeV2 {
        transition: (match change.transition {
            ConfChangeTransition::Auto => old::ConfChangeTransition::Auto,
            ConfChangeTransition::Implicit => old::ConfChangeTransition::Implicit,
            ConfChangeTransition::Explicit => old::ConfChangeTransition::Explicit,
        }) as i32,
        changes: change
            .changes
            .iter()
            .map(|single| old::ConfChangeSingle {
                change_type: change_kind_to(single.change_type),
                node_id: single.node_id,
            })
            .collect(),
        context: change.context.clone(),
    }
}
pub fn single_to(change: &ConfChange) -> old::ConfChange {
    old::ConfChange {
        change_type: change_kind_to(change.change_type),
        node_id: change.node_id,
        context: change.context.clone(),
        id: 0,
    }
}

pub fn entry_from(entry: &old::Entry) -> Entry {
    let kind = old::EntryType::from_i32(entry.entry_type).expect("a kind raft-rs names");
    let (entry_type, data) = match kind {
        old::EntryType::EntryNormal => (EntryType::EntryNormal, entry.data.clone()),
        old::EntryType::EntryConfChange => {
            let mut change = old::ConfChange::default();
            change
                .merge_from_bytes(&entry.data)
                .expect("raft-rs's change");
            let ours = ConfChange {
                change_type: change_kind_from(change.change_type),
                node_id: change.node_id,
                context: change.context,
            };
            (EntryType::EntryConfChange, ours.encode_to_vec())
        }
        old::EntryType::EntryConfChangeV2 if entry.data.is_empty() => {
            (EntryType::EntryConfChangeV2, Vec::new())
        }
        old::EntryType::EntryConfChangeV2 => {
            let mut change = old::ConfChangeV2::default();
            change
                .merge_from_bytes(&entry.data)
                .expect("raft-rs's change");
            (
                EntryType::EntryConfChangeV2,
                change_from(&change).encode_to_vec(),
            )
        }
    };
    Entry {
        entry_type,
        term: entry.term,
        index: entry.index,
        data,
        context: entry.context.clone(),
    }
}
pub fn entry_to(entry: &Entry) -> old::Entry {
    let (entry_type, data) = match entry.entry_type {
        EntryType::EntryNormal => (old::EntryType::EntryNormal, entry.data.clone()),
        EntryType::EntryConfChange => {
            let change = ConfChange::decode(&entry.data).expect("this core's change");
            (
                old::EntryType::EntryConfChange,
                single_to(&change).write_to_bytes().expect("encodes"),
            )
        }
        EntryType::EntryConfChangeV2 if entry.data.is_empty() => {
            (old::EntryType::EntryConfChangeV2, Vec::new())
        }
        EntryType::EntryConfChangeV2 => {
            let change = ConfChangeV2::decode(&entry.data).expect("this core's change");
            (
                old::EntryType::EntryConfChangeV2,
                change_to(&change).write_to_bytes().expect("encodes"),
            )
        }
    };
    old::Entry {
        entry_type: entry_type as i32,
        term: entry.term,
        index: entry.index,
        data,
        context: entry.context.clone(),
        sync_log: false,
    }
}

pub fn snapshot_from(snapshot: &old::Snapshot) -> Snapshot {
    Snapshot {
        data: snapshot.data.clone(),
        metadata: snapshot.metadata.as_ref().map(|metadata| SnapshotMetadata {
            conf_state: metadata.conf_state.clone().map(conf_from),
            index: metadata.index,
            term: metadata.term,
        }),
    }
}
pub fn snapshot_to(snapshot: &Snapshot) -> old::Snapshot {
    old::Snapshot {
        data: snapshot.data.clone(),
        metadata: snapshot
            .metadata
            .as_ref()
            .map(|metadata| old::SnapshotMetadata {
                conf_state: metadata.conf_state.as_ref().map(conf_to),
                index: metadata.index,
                term: metadata.term,
            }),
    }
}

/// raft-rs's message as this core says it: its priority is the newer field, or the deprecated
/// one when the newer is unset, as raft-rs reads it.
pub fn message_from(message: old::Message) -> Message {
    let priority = if message.priority != 0 {
        message.priority
    } else {
        i64::try_from(message.deprecated_priority).unwrap_or(i64::MAX)
    };
    Message {
        msg_type: kind_from(message.msg_type),
        to: message.to,
        from: message.from,
        term: message.term,
        log_term: message.log_term,
        index: message.index,
        entries: message.entries.iter().map(entry_from).collect(),
        commit: message.commit,
        commit_term: message.commit_term,
        snapshot: message
            .snapshot
            .as_ref()
            .map(|snapshot| Box::new(snapshot_from(snapshot))),
        request_snapshot: message.request_snapshot,
        reject: message.reject,
        // raft-rs has no member whose log lost what it acknowledged, and keeps nothing that
        // arrives ahead of a hole.
        lost: false,
        kept: false,
        reject_hint: message.reject_hint,
        context: message.context,
        priority,
        // raft-rs says nothing of a classic commit, and has no fast track whose holdings it
        // would release.
        classic: None,
    }
}
pub fn message_to(message: &Message) -> old::Message {
    assert!(!message.lost, "raft-rs has no lost refusal (core step R-5)");
    old::Message {
        msg_type: kind_to(message.msg_type),
        to: message.to,
        from: message.from,
        term: message.term,
        log_term: message.log_term,
        index: message.index,
        entries: message.entries.iter().map(entry_to).collect(),
        commit: message.commit,
        commit_term: message.commit_term,
        snapshot: message.snapshot.as_deref().map(snapshot_to),
        request_snapshot: message.request_snapshot,
        reject: message.reject,
        reject_hint: message.reject_hint,
        context: message.context.clone(),
        deprecated_priority: u64::try_from(message.priority).unwrap_or(0),
        priority: message.priority,
    }
}

/// The bytes raft-rs counts an entry at: its protocol-buffers length.
pub fn old_bytes(entry: &Entry) -> u64 {
    u64::from(entry_to(entry).compute_size())
}
