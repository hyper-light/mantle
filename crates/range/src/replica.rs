//! One member of one range's Raft group (docs/design/replica.md): focal-raft's core, its
//! group's view of the device's log, the range's engine and its layer's state machine.

use std::collections::VecDeque;
use std::sync::Arc;

use focal_raft::proto::protocompat::PbMessageExt;
use focal_raft::proto::{
    self, ConfChange, ConfChangeV2, ConfState, Entry, EntryType, Message, MessageType, Snapshot,
    SnapshotMetadata,
};
use focal_raft::{RawNode, StateRole};
use mantle_disk::block::BlockFile;
use mantle_log::{Entries, Log, LogError, Pending, Proposal, Start, Update};
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
    /// Bytes of one entry at most, the same on every member: every member's log holds such an
    /// entry in one frame, so a ready of any size can be written in parts
    /// ([`Log::parts`]), and the leader refuses a larger proposal.
    pub max_entry_bytes: u64,
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
    /// The log refused the ready being written for want of room, answering this: the ready
    /// waits, whole, in the replica, which takes no call but `drive` and `compact` until the
    /// node frees room and drives again (audit S04).
    pub stalled: Option<LogError>,
    /// From [`Replica::begin`]: a ready's update is on its way to being durable. The
    /// messages above may go now; the replica takes no call but `begin` and `drive` until the
    /// update is durable, which `drive` waits for.
    pub persisting: bool,
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
    /// An index at or past the entry that made `conf`: the entry itself once this member
    /// applies a change, and before that the point it opened at or installed.
    conf_index: u64,
    max_entry_bytes: u64,
    /// A ready the log has yet to take whole, which the core counts as out (07 §1.2).
    staged: Option<Staged>,
    /// The last entry this member acknowledged, when its log may lack it: a last frame no
    /// longer read, whose term and vote recovery restored and whose entries it marked
    /// (docs/design/raft-log.md §6). Until the log holds them again, or an entry of a later
    /// term, the member judges a candidate against this entry rather than its shorter log,
    /// as it would have with the entries, and does not campaign, since it cannot lead
    /// without them.
    uncertain: Option<Start>,
    /// Reads asked of the group, confirmed a round at a time.
    reads: Rounds,
    /// Ticks a round of reads may be out before it is taken as lost: an election timeout.
    round_ticks: u64,
}

/// Reads confirmed a round at a time (audit §5.5). One round is out at a time, and one quorum's
/// confirmation serves every read it carries. A read asked while a round is out waits for the
/// next: the round's index is the commit index when it began, which may predate a write the
/// read must see (06 §A1.5), so a read never joins a round begun before it. A round whose
/// confirmation never comes, its leader gone or its message lost, is dropped after an election
/// timeout's ticks, its reads with it, as a lost message's are, and their callers ask again.
#[derive(Debug, Default)]
struct Rounds {
    /// The round out: its number, the ticks it has been out, and the reads it carries.
    out: Option<(u64, u64, Vec<Vec<u8>>)>,
    /// Reads waiting for the next round, and their bytes.
    waiting: Vec<Vec<u8>>,
    waiting_bytes: u64,
    next: u64,
}

/// The context a round of reads is asked under: its number, under a tag no other context of
/// the replica's takes.
fn round_context(number: u64) -> Vec<u8> {
    let mut context = ROUND.to_vec();
    context.extend_from_slice(&number.to_be_bytes());
    context
}

fn round_of(context: &[u8]) -> Option<u64> {
    let number = context.strip_prefix(ROUND)?;
    Some(u64::from_be_bytes(number.try_into().ok()?))
}

const ROUND: &[u8] = b"mantle-read-round:";

/// A ready being made durable: its update's parts not yet durable, in order, each fitting a
/// frame, the log's handle for the first once it is submitted, and the log's answer once it
/// has been waited for.
struct Staged {
    ready: focal_raft::Ready,
    parts: VecDeque<Update>,
    pending: Option<Pending>,
    answered: Option<Result<(), LogError>>,
}

/// Where writing a ready's parts got to.
enum Persisted {
    Done(focal_raft::Ready),
    /// A part is submitted and not yet durable: the ready waits for the log's flush.
    Flushing(Staged),
    /// The log refused a part for want of room: the ready waits with the parts left.
    Waiting(Staged, LogError),
}

/// Whether a member's log can hold to the range's settings (audit S04). An entry of the
/// range's largest must fit one frame, so a ready of any size can be written in parts. And a
/// group compacted to its applied state retains at most one ready's entries: a leader's
/// uncommitted proposals, or the appends a follower has in flight, and one entry past either
/// bound, which the core admits alone. The group's bounds must hold such a ready, in bytes
/// and in entries of the fewest bytes, or a ready refused for room could wait for good.
fn check_settings<F: BlockFile + 'static>(
    settings: &Settings,
    log: &Log<F>,
) -> Result<(), ReplicaError> {
    let overhead = u64::try_from(store::ENTRY_OVERHEAD).unwrap_or(u64::MAX);
    let largest = settings.max_entry_bytes.checked_add(overhead);
    let room = u64::try_from(log.entry_room()?).unwrap_or(u64::MAX);
    if largest.is_none_or(|b| b > room) {
        return Err(ReplicaError::Config(
            "an entry of the range's largest does not fit one frame of this log",
        ));
    }
    let inflight = u64::try_from(settings.max_inflight_msgs)
        .ok()
        .and_then(|n| n.checked_mul(settings.max_size_per_msg));
    let ready = inflight
        .map(|f| f.max(settings.max_uncommitted_size))
        .and_then(|b| b.checked_add(settings.max_entry_bytes));
    let config = log.config();
    let fits = ready.is_some_and(|b| {
        b <= config.group_bytes
            && b.checked_div(overhead)
                .is_some_and(|entries| entries <= config.group_entries)
    });
    if !fits {
        return Err(ReplicaError::Config(
            "the log's bounds on a group hold less than one ready of the range's settings",
        ));
    }
    Ok(())
}

/// Whether the log refused a write for want of room another write frees: the group's own
/// retained entries, the log's segments, or its groups.
fn waits_for_room(e: &LogError) -> bool {
    matches!(
        e,
        LogError::Backlog(_) | LogError::Full | LogError::TooManyGroups(_) | LogError::Busy
    )
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
        check_settings(settings, &log)?;
        let applied = engine.applied();
        complete_install(&log, group, &engine)?;
        commit_applied(&log, group, applied)?;
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
        let uncertain = uncertain_mark(&store)?;
        let node = RawNode::new(&config, store)?;
        let mut replica = Self {
            node,
            group,
            engine,
            layer,
            rules,
            applied,
            conf,
            conf_index: applied,
            max_entry_bytes: settings.max_entry_bytes,
            staged: None,
            uncertain,
            reads: Rounds::default(),
            round_ticks: u64::try_from(settings.election_tick).unwrap_or(u64::MAX),
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

    /// The group's configuration as of the last entry applied.
    pub fn configuration(&self) -> &ConfState {
        &self.conf
    }

    /// Whether this member leads and every voter of its configuration has said it committed
    /// the entry that made it. Configurations take effect as members apply them, so until
    /// then a voter that did not learn of the commit still counts the members the change
    /// removed, and losing the leader could leave no quorum it can elect in; after, losing
    /// any one member leaves voters that elect under this configuration
    /// (docs/design/replica.md §6).
    pub fn configuration_known(&self) -> bool {
        let tracker = self.node.raft.tracker();
        let id = self.id();
        self.is_leader()
            && self.conf.voters.iter().all(|&voter| {
                voter == id
                    || tracker
                        .get(voter)
                        .is_some_and(|p| p.committed_index >= self.conf_index)
            })
    }

    /// Whether this member leads and `member` has confirmed holding every entry it knows
    /// committed: a member that could vote now without a quorum waiting on it to catch up
    /// (docs/design/replica.md §6). The leader's record of what a member holds rises only on
    /// the member's own answer.
    pub fn caught_up(&self, member: u64) -> bool {
        let committed = self.node.raft.log().committed();
        self.is_leader()
            && self
                .node
                .raft
                .tracker()
                .get(member)
                .is_some_and(|p| p.matched >= committed)
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

    /// A tick of the group's clock. A replica waiting for room takes no part in the group,
    /// and its timers resume when it does.
    pub fn tick(&mut self) -> Result<(), ReplicaError> {
        if self.staged.is_some() {
            return Ok(());
        }
        self.node.tick()?;
        if let Some((_, age, _)) = self.reads.out.as_mut() {
            *age = age.saturating_add(1);
            if *age >= self.round_ticks {
                self.reads.out = None;
            }
        }
        self.next_round()
    }

    /// Takes a message. While the member's log may lack entries it acknowledged, it judges a
    /// request for its vote against the last of them: a candidate behind it may lack an entry
    /// this member helped commit, and has no answer. Nor does an order to campaign, since the
    /// member cannot lead without the entries.
    pub fn step(&mut self, message: Message) -> Result<(), ReplicaError> {
        self.ready_for_calls()?;
        let kind = proto::message_type(&message);
        let requests_vote = matches!(
            kind,
            Some(MessageType::MsgRequestVote | MessageType::MsgRequestPreVote)
        );
        let transfers = matches!(kind, Some(MessageType::MsgTimeoutNow));
        if (requests_vote || transfers)
            && let Some(mark) = self.uncertainty()?
        {
            let behind = message.log_term < mark.term
                || (message.log_term == mark.term && message.index < mark.index);
            if transfers || behind {
                return Ok(());
            }
        }
        self.node.step(message)?;
        Ok(())
    }

    /// Whether the member's log may still lack entries it acknowledged.
    pub fn is_uncertain(&mut self) -> Result<bool, ReplicaError> {
        Ok(self.uncertainty()?.is_some())
    }

    /// The last entry the member acknowledged, while its log may lack it. The log ends the
    /// mark once it holds the entries again or one of a later term, so it is read afresh
    /// while it lasts.
    fn uncertainty(&mut self) -> Result<Option<Start>, ReplicaError> {
        if self.uncertain.is_some() {
            self.uncertain = uncertain_mark(self.node.store())?;
        }
        Ok(self.uncertain)
    }

    /// `Stalled` while a ready waits for room: the core takes no call until it is done.
    fn ready_for_calls(&self) -> Result<(), ReplicaError> {
        match self.staged {
            Some(_) => Err(ReplicaError::Stalled),
            None => Ok(()),
        }
    }

    /// Asks the group to confirm a read: once a quorum has, `drive` gives back the read with
    /// the index the rows must reach, and rows at or past it answer the read linearizably.
    /// `context` names the read. Reads are confirmed a round at a time, and a read waits for
    /// the round after any that is out (`Rounds`); the reads waiting hold at most an entry's
    /// bytes of contexts, past which a read is refused, to be asked again.
    pub fn read_index(&mut self, context: Vec<u8>) -> Result<(), ReplicaError> {
        self.ready_for_calls()?;
        let len = u64::try_from(context.len()).unwrap_or(u64::MAX);
        let waiting = self.reads.waiting_bytes.saturating_add(len);
        if waiting > self.max_entry_bytes {
            return Err(ReplicaError::ReadsWaiting {
                bytes: waiting,
                max: self.max_entry_bytes,
            });
        }
        self.reads.waiting.push(context);
        self.reads.waiting_bytes = waiting;
        self.next_round()
    }

    /// Asks for the next round of reads, carrying every read waiting, when none is out and the
    /// replica takes calls.
    fn next_round(&mut self) -> Result<(), ReplicaError> {
        if self.reads.out.is_some() || self.reads.waiting.is_empty() || self.staged.is_some() {
            return Ok(());
        }
        let number = self.reads.next;
        match self.node.read_index(round_context(number)) {
            Ok(()) => {}
            // The core cannot ask now, having no leader to ask: the reads wait, and a later
            // tick or drive asks.
            Err(e) if !e.is_fatal() => return Ok(()),
            Err(e) => return Err(e.into()),
        }
        self.reads.next = number.wrapping_add(1);
        let reads = std::mem::take(&mut self.reads.waiting);
        self.reads.waiting_bytes = 0;
        self.reads.out = Some((number, 0, reads));
        Ok(())
    }

    /// The reads a confirmation serves: every read of the round out that it names, each at the
    /// round's index. A confirmation of a round already dropped serves none; its reads were
    /// dropped with it.
    fn confirmed(&mut self, index: u64, context: &[u8], out: &mut Drive) {
        let Some(number) = round_of(context) else {
            return;
        };
        if self
            .reads
            .out
            .as_ref()
            .is_some_and(|(n, _, _)| *n == number)
            && let Some((_, _, reads)) = self.reads.out.take()
        {
            out.reads
                .extend(reads.into_iter().map(|read| (index, read)));
        }
    }

    /// Reports whether a snapshot this member sent to `to` arrived. Replication to a member
    /// pauses while its snapshot is out, so the transport reports every snapshot's fate,
    /// which a snapshot's own stream always learns (docs/design/replica.md §3).
    pub fn report_snapshot(&mut self, to: u64, arrived: bool) -> Result<(), ReplicaError> {
        self.ready_for_calls()?;
        let status = if arrived {
            focal_raft::SnapshotStatus::Finish
        } else {
            focal_raft::SnapshotStatus::Failure
        };
        self.node.report_snapshot(to, status)?;
        Ok(())
    }

    pub fn campaign(&mut self) -> Result<(), ReplicaError> {
        self.ready_for_calls()?;
        if self.is_uncertain()? {
            return Err(ReplicaError::Uncertain);
        }
        self.node.campaign()?;
        Ok(())
    }

    /// Proposes an entry of commands; only a leader takes it, and none larger than the
    /// range's bound.
    pub fn propose(&mut self, entry: &wire::Entry) -> Result<(), ReplicaError> {
        self.ready_for_calls()?;
        let data = entry.encode()?;
        self.bounded(data.len())?;
        self.node.propose(Vec::new(), data)?;
        Ok(())
    }

    pub fn propose_change(&mut self, change: &ConfChangeV2) -> Result<(), ReplicaError> {
        self.ready_for_calls()?;
        self.bounded(change.compute_size().try_into().unwrap_or(usize::MAX))?;
        self.node.propose_conf_change(Vec::new(), change)?;
        Ok(())
    }

    fn bounded(&self, len: usize) -> Result<(), ReplicaError> {
        if u64::try_from(len).map_or(true, |l| l > self.max_entry_bytes) {
            return Err(ReplicaError::EntryTooLarge {
                len,
                max: self.max_entry_bytes,
            });
        }
        Ok(())
    }

    /// Handles what the core asks for, in the order Raft's safety needs
    /// (docs/design/replica.md §3): the messages a leader may send at once, the log write,
    /// the messages that wait for it, the committed entries, and the core's advance.
    /// A ready the log refuses for want of room waits in the replica, whole, and the next
    /// drive writes it first (audit S04).
    pub fn drive(&mut self) -> Result<Drive, ReplicaError> {
        self.drive_ready(true)
    }

    /// Does what `drive` does without waiting on the log (docs/design/replica.md §3): takes
    /// the core's ready, gives out the messages a leader may send before its own write,
    /// submits the ready's update, and applies the entries already committed, then returns
    /// with `persisting` set while the update is not yet durable. The caller sends those
    /// messages while the log flushes, and the flush serves every group that submitted before
    /// it; `drive` or `begin` again finishes the ready once the update is durable, giving out
    /// the messages that waited for it, such as a follower's acknowledgement. One ready of a
    /// group is outstanding at a time (audit §5.1).
    pub fn begin(&mut self) -> Result<Drive, ReplicaError> {
        self.drive_ready(false)
    }

    /// Whether a ready's update is on its way to being durable.
    pub fn persisting(&self) -> bool {
        self.staged.as_ref().is_some_and(|s| s.pending.is_some())
    }

    /// Waits until the part of a ready's update now submitted is durable, or refused, and
    /// keeps the answer for the next `begin` or `drive`, which finish the ready: a node that
    /// has sent what `begin` gave out and has nothing else to do waits here for the flush.
    pub fn wait_persisted(&mut self) {
        if let Some(staged) = self.staged.as_mut()
            && staged.answered.is_none()
            && let Some(pending) = staged.pending.as_ref()
        {
            staged.answered = Some(pending.wait());
        }
    }

    fn drive_ready(&mut self, wait: bool) -> Result<Drive, ReplicaError> {
        let mut out = Drive::default();
        for _ in 0..DRIVE_BUDGET {
            let staged = match self.staged.take() {
                Some(staged) => staged,
                None if self.node.has_ready() => self.take_ready(&mut out)?,
                None => break,
            };
            match self.persist(staged, wait)? {
                Persisted::Done(ready) => self.finish(ready, &mut out)?,
                Persisted::Flushing(mut staged) => {
                    // As while the log refuses room: what is committed is durable already,
                    // and applies while the update flushes.
                    let committed = staged.ready.take_committed_entries();
                    self.apply(committed, &mut out)?;
                    self.staged = Some(staged);
                    out.persisting = true;
                    break;
                }
                Persisted::Waiting(mut staged, refusal) => {
                    // The core gives out only entries committed and durable, so these apply
                    // now, and the group can compact past them while the ready waits.
                    let committed = staged.ready.take_committed_entries();
                    self.apply(committed, &mut out)?;
                    self.staged = Some(staged);
                    out.stalled = Some(refusal);
                    break;
                }
            }
        }
        // The reads that waited on a round now confirmed go in the next.
        self.next_round()?;
        // A member whose log may lack entries it acknowledged cannot lead: its election
        // timer still runs, which keeps it from holding a lease on a leader that is gone,
        // but its campaigns ask no one.
        if self.uncertainty()?.is_some() {
            out.messages.retain(|m| !campaigns(m));
        }
        Ok(out)
    }

    /// Takes the core's next ready: installs its snapshot, gives out the messages a leader
    /// may send before its own write and the reads confirmed, and lays out its update.
    fn take_ready(&mut self, out: &mut Drive) -> Result<Staged, ReplicaError> {
        let mut ready = self.node.ready()?;
        let installed = match ready.snapshot() {
            Some(s) if !proto::snapshot_is_empty(s) => Some(self.install(s.clone())?),
            _ => None,
        };
        out.messages.extend(ready.take_messages());
        for read in ready.take_read_states() {
            self.confirmed(read.index, &read.request_ctx, out);
        }
        let parts = match update_of(&ready, installed)? {
            Some(update) => match self.node.store().log.parts(self.group, update) {
                Ok(parts) => parts.into(),
                Err(LogError::TooLarge(len)) => {
                    return Err(ReplicaError::Stopped(format!(
                        "a ready holds a record of {len} bytes, more than a frame of this log"
                    )));
                }
                Err(e) => return Err(e.into()),
            },
            None => VecDeque::new(),
        };
        Ok(Staged {
            ready,
            parts,
            pending: None,
            answered: None,
        })
    }

    /// Writes a ready's parts in order, each submitted once the one before it is durable;
    /// the ready back once every part is, staged while one flushes and `wait` is false, or
    /// staged when the log refuses one for want of room.
    fn persist(&mut self, mut staged: Staged, wait: bool) -> Result<Persisted, ReplicaError> {
        loop {
            if let Some(pending) = staged.pending.as_ref() {
                let answer = match staged.answered.take() {
                    Some(answer) => Some(answer),
                    None if wait => Some(pending.wait()),
                    None => pending.poll(),
                };
                match answer {
                    None => return Ok(Persisted::Flushing(staged)),
                    Some(Ok(())) => {
                        staged.pending = None;
                        staged.parts.pop_front();
                    }
                    Some(Err(e)) if waits_for_room(&e) => {
                        staged.pending = None;
                        return Ok(Persisted::Waiting(staged, e));
                    }
                    Some(Err(e)) => return Err(e.into()),
                }
            }
            let Some(part) = staged.parts.front() else {
                return Ok(Persisted::Done(staged.ready));
            };
            match self
                .node
                .store()
                .log
                .submit_waiting(self.group, part.clone())
            {
                Ok(pending) => staged.pending = Some(pending),
                Err(e) if waits_for_room(&e) => return Ok(Persisted::Waiting(staged, e)),
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Finishes a durable ready: the messages that waited for the write, the committed
    /// entries, and the core's advance.
    fn finish(
        &mut self,
        mut ready: focal_raft::Ready,
        out: &mut Drive,
    ) -> Result<(), ReplicaError> {
        out.messages.extend(ready.take_persisted_messages());
        let committed = ready.take_committed_entries();
        self.apply(committed, out)?;
        let mut light = self.node.advance_append(ready)?;
        out.messages.extend(light.take_messages());
        let committed = light.take_committed_entries();
        self.apply(committed, out)?;
        self.node.advance_apply_to(self.applied)?;
        Ok(())
    }

    fn apply(&mut self, entries: Vec<Entry>, out: &mut Drive) -> Result<(), ReplicaError> {
        for entry in entries {
            let index = entry.index;
            let mut changed = false;
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
                    let outcome = self.node.apply_conf_change(&change);
                    self.changed(index, outcome)?;
                    changed = true;
                }
                Some(EntryType::EntryConfChange) => {
                    let mut change = ConfChange::default();
                    change.merge_from_bytes(&entry.data).map_err(|_| {
                        ReplicaError::Stopped(format!("committed change {index} does not decode"))
                    })?;
                    let outcome = self.node.apply_conf_change_v1(&change);
                    self.changed(index, outcome)?;
                    changed = true;
                }
                None => {
                    return Err(ReplicaError::Stopped(format!(
                        "committed entry {index} has no known type"
                    )));
                }
            }
            self.applied = index;
            if changed && self.snapshot_misses_a_member() {
                self.prepare()?;
            }
        }
        Ok(())
    }

    /// Whether the snapshot prepared for lagging members leaves out a member of the
    /// configuration. A member refuses a snapshot that does not name it, so a member added
    /// after the snapshot was prepared could never be caught up by it; the snapshot is then
    /// prepared again, at the change that added the member (docs/design/replica.md §6).
    fn snapshot_misses_a_member(&self) -> bool {
        let Some(named) = self
            .node
            .store()
            .snapshot
            .as_ref()
            .and_then(|s| s.metadata.as_ref())
            .and_then(|m| m.conf_state.as_ref())
        else {
            return false;
        };
        let names = |c: &ConfState, id: &u64| {
            c.voters.contains(id)
                || c.learners.contains(id)
                || c.voters_outgoing.contains(id)
                || c.learners_next.contains(id)
        };
        let conf = &self.conf;
        conf.voters
            .iter()
            .chain(&conf.learners)
            .chain(&conf.voters_outgoing)
            .chain(&conf.learners_next)
            .any(|id| !names(named, id))
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
                self.conf_index = index;
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
        let mut rows = image::decode(&snapshot.data)
            .ok_or_else(|| ReplicaError::Stopped("a snapshot's rows do not decode".into()))?;
        // The engine keeps the snapshot's point beside its rows, so a restart can finish an
        // install whose log write failed after the engine took it.
        rows.retain(|(k, _)| k.as_slice() != conf::INSTALLED);
        rows.push((
            conf::INSTALLED.to_vec(),
            conf::encode_point(metadata.index, metadata.term),
        ));
        rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        self.engine.install(metadata.index, rows)?;
        self.engine.persist()?;
        self.applied = metadata.index;
        if let Some(conf) = metadata.conf_state {
            self.conf = conf.clone();
            self.conf_index = metadata.index;
            self.node.store_mut().conf = conf;
        }
        self.node.store_mut().snapshot = Some(snapshot);
        Ok(Start {
            index: metadata.index,
            term: metadata.term,
        })
    }
}

/// Finishes an install the log never recorded. The engine takes a snapshot and makes it
/// durable before the log records the snapshot's point; if that log write failed, the engine
/// stands at the snapshot while the log ends before it. The snapshot is committed state, and
/// nothing was acknowledged for the failed write, so the log is brought to it: it starts at
/// the snapshot's point, holds nothing past it, and knows it committed.
fn complete_install<F: BlockFile + 'static, E: Engine>(
    log: &Log<F>,
    group: u128,
    engine: &E,
) -> Result<(), ReplicaError> {
    let Some(bytes) = engine.get(conf::INSTALLED)? else {
        return Ok(());
    };
    let (index, term) = conf::decode_point(&bytes)
        .ok_or_else(|| ReplicaError::Stopped("the snapshot point does not decode".into()))?;
    let view = log.view(group)?;
    let recorded = match log.term(group, index) {
        Ok(t) => t == term,
        Err(mantle_log::LogError::Compacted { .. }) => true,
        Err(_) => false,
    };
    if recorded {
        return Ok(());
    }
    let hard = view.and_then(|v| v.hard_state).unwrap_or_default();
    let first = index
        .checked_add(1)
        .ok_or_else(|| ReplicaError::Stopped("an index past u64".into()))?;
    log.write_waiting(
        group,
        Update {
            start: Some(Start { index, term }),
            entries: Some(Entries {
                first,
                entries: Vec::new(),
            }),
            hard_state: Some(mantle_log::HardState {
                commit: hard.commit.max(index),
                ..hard
            }),
            proposals: Vec::new(),
            remove: false,
        },
    )?;
    Ok(())
}

/// Brings the log's durable commit up to the entries the engine applied. A member learns of
/// commits the core reports after its entries are durable, and applies them without writing
/// the commit to the log, so an engine that made its rows durable (at compaction, or on its
/// own) can open ahead of the commit the log kept, which the core refuses. Every entry the
/// engine applied was committed, and the log holds it, so its index is committed state the
/// log is told of, as for an install the log never recorded.
/// The uncertainty mark the group's log carries (`Replica::uncertain`).
fn uncertain_mark<F: BlockFile + 'static>(
    store: &LogStore<F>,
) -> Result<Option<Start>, ReplicaError> {
    Ok(store.log.view(store.group)?.and_then(|v| v.uncertain))
}

/// Whether `message` asks for votes for its sender's campaign.
fn campaigns(message: &Message) -> bool {
    matches!(
        proto::message_type(message),
        Some(MessageType::MsgRequestVote | MessageType::MsgRequestPreVote)
    )
}

fn commit_applied<F: BlockFile + 'static>(
    log: &Log<F>,
    group: u128,
    applied: u64,
) -> Result<(), ReplicaError> {
    let Some(view) = log.view(group)? else {
        return Ok(());
    };
    let hard = view.hard_state.unwrap_or_default();
    if applied <= hard.commit {
        return Ok(());
    }
    if applied > view.last {
        return Err(ReplicaError::Stopped(format!(
            "the engine applied {applied}, past the log's last entry {}",
            view.last
        )));
    }
    log.write_waiting(
        group,
        Update {
            hard_state: Some(mantle_log::HardState {
                commit: applied,
                ..hard
            }),
            ..Update::default()
        },
    )?;
    Ok(())
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
