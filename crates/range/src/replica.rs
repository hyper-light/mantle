//! One member of one range's Raft group (docs/design/replica.md): focal-raft's core, its
//! group's view of the device's log, the range's engine and its layer's state machine.

use std::sync::Arc;

use focal_raft::proto::protocompat::PbMessageExt;
use focal_raft::proto::{
    self, ConfChange, ConfChangeV2, ConfState, Entry, EntryType, Message, Snapshot,
    SnapshotMetadata,
};
use focal_raft::{RawNode, StateRole};
use mantle_disk::block::BlockFile;
use mantle_log::{Entries, Log, Proposal, Start, Update};
use mantle_meta::apply::{Layer, apply_entry};
use mantle_meta::engine::{Engine, Write};
use mantle_meta::session::Rules;
use mantle_meta::wire::{self, Answer};

use crate::error::ReplicaError;
use crate::store::{self, LogStore};
use crate::{conf, image};

/// Readies one call to [`Replica::drive`] handles at most: the caller drives again for the
/// rest, so a burst of committed entries never holds the caller without end.
const DRIVE_BUDGET: usize = 64;

/// What every member of a range shares: its layer, the bounds of its sessions, the
/// configuration it starts from, and how its core runs.
#[derive(Debug, Clone, PartialEq)]
pub struct Range {
    pub layer: Layer,
    pub rules: Rules,
    /// The configuration of a range whose engine holds nothing yet.
    pub boot: ConfState,
    pub settings: Settings,
}

/// How the group's core runs, as the node sets every range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    /// Ticks without hearing a leader before a follower campaigns.
    pub election_tick: usize,
    /// Ticks between a leader's heartbeats.
    pub heartbeat_tick: usize,
    /// Bytes of entries one append carries at most.
    pub max_size_per_msg: u64,
    /// Appends in flight to one follower at most.
    pub max_inflight_msgs: usize,
    /// Bytes of proposals not yet committed a leader takes at most.
    pub max_uncommitted_size: u64,
    /// Bytes of committed entries one ready gives to apply at most.
    pub max_committed_size_per_ready: u64,
}

/// What one call to [`Replica::drive`] produced.
#[derive(Debug, Default)]
pub struct Drive {
    /// Messages to send, in order.
    pub messages: Vec<Message>,
    /// The normal entries applied.
    pub applied: Vec<Applied>,
    /// Reads a quorum confirmed: each read's context and the index the replica must have
    /// applied before it answers from its rows (06 §A1).
    pub reads: Vec<(u64, Vec<u8>)>,
}

/// A normal entry applied: its index, and each command's session, serial and answer, in
/// order, so a gateway finds the answer to its command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Applied {
    pub index: u64,
    pub answers: Vec<(u64, u64, Answer)>,
}

pub struct Replica<F: BlockFile + 'static, E: Engine> {
    node: RawNode<LogStore<F>>,
    group: u128,
    engine: E,
    layer: Layer,
    rules: Rules,
    applied: u64,
    /// The configuration as of `applied`.
    conf: ConfState,
}

impl<F: BlockFile + 'static, E: Engine> Replica<F, E> {
    /// Opens member `id` of `range` whose records the log keeps under `group`, at the state
    /// `engine` holds durably (docs/design/replica.md §4). `seed` draws its election
    /// timeouts.
    pub fn open(
        id: u64,
        group: u128,
        log: Arc<Log<F>>,
        engine: E,
        range: &Range,
        seed: u64,
    ) -> Result<Self, ReplicaError> {
        let (layer, rules, boot, settings) =
            (range.layer, range.rules, &range.boot, &range.settings);
        let conf = match engine.get(conf::ROW)? {
            Some(bytes) => conf::decode(&bytes)
                .ok_or_else(|| ReplicaError::Stopped("the configuration does not decode".into()))?,
            None => boot.clone(),
        };
        let applied = engine.applied();
        let config = focal_raft::Config {
            election_tick: settings.election_tick,
            heartbeat_tick: settings.heartbeat_tick,
            applied,
            max_size_per_msg: settings.max_size_per_msg,
            max_inflight_msgs: settings.max_inflight_msgs,
            max_uncommitted_size: settings.max_uncommitted_size,
            max_committed_size_per_ready: settings.max_committed_size_per_ready,
            // Pre-vote and check-quorum together keep a partitioned member from deposing a
            // healthy leader and a leader cut off from stepping down late (06 §A1).
            check_quorum: true,
            pre_vote: true,
            seed,
            ..focal_raft::Config::new(id)
        };
        let store = LogStore {
            log,
            group,
            conf: conf.clone(),
            snapshot: None,
        };
        let node = RawNode::new(&config, store)?;
        let mut replica = Self {
            node,
            group,
            engine,
            layer,
            rules,
            applied,
            conf,
        };
        // A log compacted before the restart can serve a lagging member only by snapshot.
        let compacted = replica
            .node
            .store()
            .log
            .view(group)?
            .is_some_and(|v| v.start.index > 0);
        if compacted {
            replica.prepare()?;
        }
        Ok(replica)
    }

    pub fn id(&self) -> u64 {
        self.node.raft.id()
    }

    pub fn is_leader(&self) -> bool {
        self.node.raft.state() == StateRole::Leader
    }

    /// The member this one believes leads, 0 for none.
    pub fn leader(&self) -> u64 {
        self.node.raft.leader_id()
    }

    pub fn term(&self) -> u64 {
        self.node.raft.term()
    }

    /// The last entry applied.
    pub fn applied(&self) -> u64 {
        self.applied
    }

    /// A line on the member's state for diagnosis: its term, leader, applied and committed
    /// indexes, log bounds and prepared snapshot, and its view of every member's progress.
    pub fn describe(&self) -> String {
        let view = self.node.store().log.view(self.group).ok().flatten();
        let snap = self
            .node
            .store()
            .snapshot
            .as_ref()
            .map(proto::snapshot_index);
        let progress: Vec<String> = self
            .node
            .raft
            .tracker()
            .iter()
            .map(|(id, p)| {
                format!(
                    "{id}:m{}n{}s{:?}ps{}",
                    p.matched, p.next_index, p.state, p.pending_snapshot
                )
            })
            .collect();
        format!(
            "id {} lead {} term {} applied {} commit {} log {:?} snap {:?} progress {:?}",
            self.id(),
            self.leader(),
            self.term(),
            self.applied,
            self.node.raft.log().committed(),
            view.map(|v| (v.start.index, v.last)),
            snap,
            progress
        )
    }

    pub fn engine(&self) -> &E {
        &self.engine
    }

    /// Takes back the engine, as a crash does with the process that held it.
    pub fn into_engine(self) -> E {
        self.engine
    }

    pub fn tick(&mut self) -> Result<(), ReplicaError> {
        self.node.tick()?;
        Ok(())
    }

    pub fn step(&mut self, message: Message) -> Result<(), ReplicaError> {
        self.node.step(message)?;
        Ok(())
    }

    /// Asks the group to confirm a read: once a quorum has, `drive` gives back the read with
    /// the index the rows must reach, and rows at or past it answer the read linearizably.
    /// Only a leader confirms reads; `context` names the read.
    pub fn read_index(&mut self, context: Vec<u8>) -> Result<(), ReplicaError> {
        self.node.read_index(context)?;
        Ok(())
    }

    /// Reports whether a snapshot this member sent to `to` arrived. Replication to a member
    /// pauses while its snapshot is out, so the transport reports every snapshot's fate,
    /// which a snapshot's own stream always learns (docs/design/replica.md §3).
    pub fn report_snapshot(&mut self, to: u64, arrived: bool) -> Result<(), ReplicaError> {
        let status = if arrived {
            focal_raft::SnapshotStatus::Finish
        } else {
            focal_raft::SnapshotStatus::Failure
        };
        self.node.report_snapshot(to, status)?;
        Ok(())
    }

    pub fn campaign(&mut self) -> Result<(), ReplicaError> {
        self.node.campaign()?;
        Ok(())
    }

    /// Proposes an entry of commands; only a leader takes it.
    pub fn propose(&mut self, entry: &wire::Entry) -> Result<(), ReplicaError> {
        self.node.propose(Vec::new(), entry.encode()?)?;
        Ok(())
    }

    pub fn propose_change(&mut self, change: &ConfChangeV2) -> Result<(), ReplicaError> {
        self.node.propose_conf_change(Vec::new(), change)?;
        Ok(())
    }

    /// Handles what the core asks for, in the order Raft's safety needs
    /// (docs/design/replica.md §3): the messages a leader may send at once, the log write,
    /// the messages that wait for it, the committed entries, and the core's advance.
    pub fn drive(&mut self) -> Result<Drive, ReplicaError> {
        let mut out = Drive::default();
        for _ in 0..DRIVE_BUDGET {
            if !self.node.has_ready() {
                break;
            }
            let mut ready = self.node.ready()?;
            let installed = match ready.snapshot() {
                Some(s) if !proto::snapshot_is_empty(s) => Some(self.install(s.clone())?),
                _ => None,
            };
            out.messages.extend(ready.take_messages());
            out.reads.extend(
                ready
                    .take_read_states()
                    .into_iter()
                    .map(|r| (r.index, r.request_ctx)),
            );
            if let Some(update) = update_of(&ready, installed)? {
                self.node.store().log.write_waiting(self.group, update)?;
            }
            out.messages.extend(ready.take_persisted_messages());
            let committed = ready.take_committed_entries();
            self.apply(committed, &mut out)?;
            let mut light = self.node.advance_append(ready)?;
            out.messages.extend(light.take_messages());
            let committed = light.take_committed_entries();
            self.apply(committed, &mut out)?;
            self.node.advance_apply_to(self.applied)?;
        }
        Ok(out)
    }

    fn apply(&mut self, entries: Vec<Entry>, out: &mut Drive) -> Result<(), ReplicaError> {
        for entry in entries {
            let index = entry.index;
            match EntryType::from_i32(entry.entry_type) {
                // A new leader's empty entry changes no row.
                Some(EntryType::EntryNormal) if entry.data.is_empty() => {
                    self.engine.apply(index, &[])?;
                }
                Some(EntryType::EntryNormal) => {
                    let batch = wire::Entry::decode(&entry.data).map_err(|_| {
                        ReplicaError::Stopped(format!("committed entry {index} does not decode"))
                    })?;
                    let answers =
                        apply_entry(&mut self.engine, index, &batch, self.layer, &self.rules)?;
                    let answers = batch
                        .commands
                        .iter()
                        .zip(answers)
                        .map(|(c, a)| (c.session, c.serial, a))
                        .collect();
                    out.applied.push(Applied { index, answers });
                }
                Some(EntryType::EntryConfChangeV2) => {
                    let mut change = ConfChangeV2::default();
                    change.merge_from_bytes(&entry.data).map_err(|_| {
                        ReplicaError::Stopped(format!("committed change {index} does not decode"))
                    })?;
                    let changed = self.node.apply_conf_change(&change);
                    self.changed(index, changed)?;
                }
                Some(EntryType::EntryConfChange) => {
                    let mut change = ConfChange::default();
                    change.merge_from_bytes(&entry.data).map_err(|_| {
                        ReplicaError::Stopped(format!("committed change {index} does not decode"))
                    })?;
                    let changed = self.node.apply_conf_change_v1(&change);
                    self.changed(index, changed)?;
                }
                None => {
                    return Err(ReplicaError::Stopped(format!(
                        "committed entry {index} has no known type"
                    )));
                }
            }
            self.applied = index;
        }
        Ok(())
    }

    /// Records the configuration a committed change made, in the batch of its entry. A change
    /// the core refused is refused alike by every member, and its entry changes no row.
    fn changed(
        &mut self,
        index: u64,
        changed: focal_raft::Result<ConfState>,
    ) -> Result<(), ReplicaError> {
        match changed {
            Ok(conf) => {
                let bytes = conf::encode(&conf).ok_or_else(|| {
                    ReplicaError::Stopped("a configuration past u32 members".into())
                })?;
                self.engine
                    .apply(index, &[Write::Put(conf::ROW.to_vec(), bytes)])?;
                self.conf = conf;
            }
            Err(e) if !e.is_fatal() => self.engine.apply(index, &[])?,
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    /// Makes the engine's applied state durable and lets the log free the entries before
    /// it, keeping `keep` entries behind for followers that lag (docs/design/replica.md §4).
    pub fn compact(&mut self, keep: u64) -> Result<(), ReplicaError> {
        self.engine.persist()?;
        // Keeping more entries than the engine has made durable keeps them all.
        let index = self.engine.durable().saturating_sub(keep);
        let log = &self.node.store().log;
        let Some(view) = log.view(self.group)? else {
            return Ok(());
        };
        if index <= view.start.index {
            return Ok(());
        }
        let term = log.term(self.group, index)?;
        log.write_waiting(
            self.group,
            Update {
                start: Some(Start { index, term }),
                ..Update::default()
            },
        )?;
        self.prepare()
    }

    /// Prepares the snapshot the log store serves: every row as of `applied`, which is at
    /// or past the log's start, with the configuration there.
    fn prepare(&mut self) -> Result<(), ReplicaError> {
        let index = self.applied;
        let term = self.node.store().log.term(self.group, index)?;
        let data = image::encode(&self.engine.image()?)
            .ok_or_else(|| ReplicaError::Stopped("a snapshot past u32 bytes a row".into()))?;
        let snapshot = Snapshot {
            data,
            metadata: Some(SnapshotMetadata {
                conf_state: Some(self.conf.clone()),
                index,
                term,
            }),
        };
        self.node.store_mut().snapshot = Some(snapshot);
        Ok(())
    }

    /// Installs a snapshot the leader sent: the engine takes its rows and makes them durable
    /// before the log records its new start, so the log never starts past what the engine
    /// holds. Returns the start.
    fn install(&mut self, snapshot: Snapshot) -> Result<Start, ReplicaError> {
        let metadata = snapshot
            .metadata
            .clone()
            .ok_or_else(|| ReplicaError::Stopped("a snapshot without metadata".into()))?;
        let rows = image::decode(&snapshot.data)
            .ok_or_else(|| ReplicaError::Stopped("a snapshot's rows do not decode".into()))?;
        self.engine.install(metadata.index, rows)?;
        self.engine.persist()?;
        self.applied = metadata.index;
        if let Some(conf) = metadata.conf_state {
            self.conf = conf.clone();
            self.node.store_mut().conf = conf;
        }
        self.node.store_mut().snapshot = Some(snapshot);
        Ok(Start {
            index: metadata.index,
            term: metadata.term,
        })
    }
}

/// What a ready asks to be made durable, as one update of the log: after a snapshot
/// installed at `installed`, the log starts there and holds nothing past it but the ready's
/// entries.
fn update_of(
    ready: &focal_raft::Ready,
    installed: Option<Start>,
) -> Result<Option<Update>, ReplicaError> {
    let encode = |e: &Entry| {
        store::encode_entry(e)
            .ok_or_else(|| ReplicaError::Stopped(format!("entry {} does not encode", e.index)))
    };
    let entries = match ready.entries() {
        [] => match installed {
            None => None,
            Some(start) => Some(Entries {
                first: start
                    .index
                    .checked_add(1)
                    .ok_or_else(|| ReplicaError::Stopped("an index past u64".into()))?,
                entries: Vec::new(),
            }),
        },
        all @ [first, ..] => Some(Entries {
            first: first.index,
            entries: all
                .iter()
                .map(|e| {
                    Ok(mantle_log::Entry {
                        term: e.term,
                        bytes: Arc::from(encode(e)?),
                    })
                })
                .collect::<Result<_, ReplicaError>>()?,
        }),
    };
    let proposals = ready
        .proposals()
        .iter()
        .map(|p| {
            Ok(Proposal {
                index: p.index,
                term: p.term,
                bytes: Arc::from(encode(p)?),
            })
        })
        .collect::<Result<Vec<_>, ReplicaError>>()?;
    let hard_state = ready.hard_state().map(store::hard_from_proto);
    if installed.is_none() && entries.is_none() && hard_state.is_none() && proposals.is_empty() {
        return Ok(None);
    }
    Ok(Some(Update {
        start: installed,
        entries,
        hard_state,
        proposals,
        remove: false,
    }))
}
