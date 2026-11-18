//! A shard's ranges, driven by a task on that shard (node.md §2.9, step 2).
//!
//! The node's shard, the first, keeps the endpoint, the plane, the liveness stream and the table of
//! ranges by shard. A shard past it runs a task of its own that holds the replicas the table places
//! there in an owner of its own (`hyper_durable::Owner`), made with that task's waker, so a range's
//! log answers wake the shard that drives it, and keeps that shard's deadlines. The two talk over
//! two bounded lanes of hyper-rt's channel: into the shard, each replica moved there before its
//! first drive, Raft messages, proposals, reads, the stream's changes and measurements, and the
//! membership's targets and replacements ([`In`]); out of it, Raft messages, applied answers,
//! flushes, each range's peers, its expected election, what it shows after a drive that changed it
//! ([`Seen`], [`Configured`]), its reads served and its replacements done ([`Out`]). The node's
//! shard sends without waiting, a full lane refused, typed (`NodeError::LaneFull`); the shard waits
//! for room on its way out, its replicas' next turn held until the node's shard takes what they
//! gave. A shard whose task ended took its ranges with it, which nothing else holds, so the node
//! stops (`NodeError::ShardEnded`) and its next start recovers them from the log (node.md §1.7).

use std::collections::BTreeSet;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use hyper_durable::{Driven, Handle, Output, Owner, ReplicaError, Unbounded};
use hyper_liveness::{Change, Liveness, PeerId};
use hyper_raft::Timing;
use hyper_raft::proto::Message;
use hyper_rt::SlotHolder;
use hyper_rt::combine::{Either, race2};
use hyper_rt::sync::{ChannelReceiver, OneshotSender, Sender, SyncError, channel, oneshot};
use hyper_timing::{Ballot, ExchangeRtt, Trust};
use hyper_transport::Classes;
use mantle_meta::engine::Engine;
use mantle_meta::wire::Entry;
use mantle_protocol::bounds;
use mantle_protocol::classes::{self, NodeClasses};
use mantle_range::membership::{Next, Replacement, Step, Toward};
use mantle_range::{Applied, ConfState, LogStore, RangeMachine};

use super::NodeError;
use super::answers::Answers;
use crate::read::{Query, Read};

/// A range's replica, as the node's shard and a range's shard hold it.
pub(crate) type Replica<E, S> = hyper_durable::Replica<Answers<S>, RangeMachine<E>, Unbounded>;

/// What the node's shard hands a range's shard.
pub(crate) enum In<E: Engine, S: LogStore> {
    /// A range's replica and its group, moved before its first drive.
    Adopt(u128, Box<Replica<E, S>>),
    /// A Raft message for a range from `sender`, the peer the transport authenticated as its
    /// `from`: stepped if `sender` may act on the range.
    Step {
        group: u128,
        sender: PeerId,
        message: Message,
    },
    /// An entry proposed to a range, and the most bytes the range takes in one.
    Propose(u128, Entry, u64),
    /// A change the node's liveness stream reported (hyper-durable's `Owner::believe`).
    Believe(Change),
    /// What the stream believes now of a peer a range came to share its group with
    /// (`Replica::believe_of`).
    Belief(u128, Belief),
    /// What the stream measured, each range's timing derived from it (`Replica::measure`).
    Measure(Box<Measured>),
    /// A read of a range, its context `id`: asked of its replica (`Replica::read`), and once a
    /// quorum confirmed the range's leader and it applied through the read, `query` answered from
    /// its engine ([`Out::Served`]).
    Read { group: u128, id: u64, query: Query },
    /// A range waits for room in the node's log, or the node owes the stream a write the log
    /// refused for room: every range compacts ([`compact`]).
    Compact,
    /// Whether a snapshot a range sent member `to` arrived (`Replica::report_snapshot`).
    Reported { group: u128, to: u64, arrived: bool },
    /// A peer's connection went: each range that shares its group with it is told
    /// (`Replica::report_unreachable`), as the node's shard tells its own (`Node::unkey`).
    Unreachable(PeerId),
    /// The stop's hand-over of a range's lead ([`transfer`]), past the voter `failed` whose
    /// transfer its core gave up: the voter it goes to told back ([`Out::Handing`]).
    Transfer { group: u128, failed: Option<u64> },
    /// The configuration the cell's members would give a range, worked out by the node's shard
    /// from what the range last showed ([`Configured`]): stepped once ([`step_toward`]).
    Toward(u128, Toward),
    /// A member of a range lost for good, replaced (`Node::replace`): kept by the shard and stepped
    /// at each of its turns while the range leads here ([`step_replacement`]), until it is done
    /// ([`Out::Replaced`]). A later one supersedes it.
    Replace(u128, Replacement),
    /// Each range's engine as it stands, handed back through the sender, and the task ends: the
    /// node closes (`Node::close`).
    Release(OneshotSender<Vec<(u128, E)>>),
}

/// What a shard's ranges gave, for the node's shard.
pub(crate) enum Out {
    /// A Raft message a range gave, to send.
    Message(u128, Message),
    /// A command of an entry a range applied, and its answer.
    Applied(u128, Applied),
    /// A range's write the log made durable: when it was submitted and when its answer was taken
    /// (`Driven::flushed`), the flush a heartbeat proves.
    Flushed(u64, u64),
    /// The peers a range shares its group with now, sorted, each once (`Replica::peers`): the
    /// pairs the stream keeps for it.
    Pairs(u128, Vec<PeerId>),
    /// A range's expected election from what the stream measured, which its pairs are charged.
    Elected(u128, Duration),
    /// What a range shows after a drive that changed it, and when it was moved.
    Seen(u128, Seen),
    /// A read the node's shard asked of a range ([`In::Read`]), by its id, and how it was served.
    Served(u128, u64, Served),
    /// A round of compaction ([`In::Compact`]) that started at least one.
    Compacted,
    /// The voter a range's hand-over ([`In::Transfer`]) goes to; `None` where it began none: the
    /// range leads not here, or its core refused.
    Handing(u128, Option<u64>),
    /// A range's configuration and its learners' progress after a drive that changed them.
    Configured(u128, Configured),
    /// A replacement ([`In::Replace`]) that ended while the range led here.
    Replaced(u128, Replacement),
    /// Why the shard stopped: its task ends after it.
    Stopped(NodeError),
}

/// What the node's liveness stream measured that a range's timing derives from
/// (`Replica::measure`): the owner's granularity, the mean flush of the node's log writes, and the
/// echoed round trip to each peer the node keeps.
#[derive(Clone)]
pub(crate) struct Measured {
    granularity: Duration,
    flush_mean: Option<Duration>,
    paths: Vec<(PeerId, ExchangeRtt)>,
}

impl Measured {
    /// What `liveness` measured of `peers`; none before its granularity is measured, when no range
    /// has a timing to derive.
    pub(crate) fn of(liveness: &Liveness, peers: impl Iterator<Item = PeerId>) -> Option<Self> {
        let granularity = liveness.granularity()?;
        let paths = peers
            .filter_map(|peer| liveness.round_trip(peer).map(|path| (peer, *path)))
            .collect();
        Some(Self {
            granularity,
            flush_mean: liveness.flush_mean(),
            paths,
        })
    }

    /// The group's timing by hyper-timing's law over what was measured, given to the core when it
    /// moved, and the span its pairs are charged: hyper-durable's `Replica::measure`, over what the
    /// stream measured on the node's shard rather than the stream itself.
    fn time<E: Engine + 'static, S: LogStore>(
        &self,
        replica: &mut Replica<E, S>,
    ) -> Result<Option<Duration>, ReplicaError> {
        let id = replica.id();
        let voters = replica.core().raft.configuration().voters();
        let paths = voters
            .iter()
            .filter(|voter| **voter != id)
            .filter_map(|voter| {
                self.paths
                    .iter()
                    .find(|(peer, _)| peer == voter)
                    .map(|(_, path)| path)
            });
        let durable = replica
            .flushes()
            .mean()
            .or(self.flush_mean)
            .unwrap_or(Duration::ZERO);
        let Some(ballot) = Ballot::measure(paths, voters.len(), durable, self.granularity) else {
            return Ok(None);
        };
        let Some(span) = ballot.span(self.granularity) else {
            return Ok(None);
        };
        let timing = Timing::of(&ballot, &span);
        if replica.core().raft.timing() != Some(timing) {
            replica.set_timing(timing)?;
        }
        Ok(Some(span.election))
    }
}

/// What the node's liveness stream believes of a peer now: whether its disk is slow, its stop
/// announced, or it is suspected.
#[derive(Clone, Copy)]
pub(crate) struct Belief {
    peer: PeerId,
    disk_slow: bool,
    stopped: bool,
    suspected: bool,
}

impl Belief {
    /// What `liveness` believes of `peer` now.
    pub(crate) fn of(liveness: &Liveness, peer: PeerId) -> Self {
        Self {
            peer,
            disk_slow: liveness.disk_slow(peer) == Some(true),
            stopped: liveness.stopped(peer),
            suspected: liveness.trust(peer) == Some(Trust::Suspected),
        }
    }

    /// Tells `replica` the belief, as `Replica::believe_of` tells it the stream's.
    fn tell<E: Engine + 'static, S: LogStore>(
        self,
        replica: &mut Replica<E, S>,
    ) -> Result<(), ReplicaError> {
        if self.disk_slow {
            replica.disk_slow(self.peer)?;
        } else {
            replica.disk_caught_up(self.peer)?;
        }
        if self.stopped {
            replica.stopped(self.peer)
        } else if self.suspected {
            replica.suspect(self.peer)
        } else {
            replica.trust(self.peer)
        }
    }
}

/// A range another shard drives, as the node's shard keeps it: its shard, the peers the stream
/// keeps pairs with for it, its latest expected election, what it showed last, and the ends of
/// the snapshots it sent that its lane has yet to take ([`In::Reported`]): at most one a member, a
/// later end superseding an earlier, as a member's progress waits on one snapshot at a time.
pub(crate) struct Far {
    pub(crate) group: u128,
    pub(crate) shard: usize,
    pub(crate) peers: Vec<PeerId>,
    pub(crate) election: Option<Duration>,
    pub(crate) seen: Seen,
    pub(crate) reports: Vec<(u64, bool)>,
    /// Its configuration and learners' progress as it last showed them.
    pub(crate) configured: Configured,
    /// The replacement the node was told of it ([`In::Replace`]) until its shard reports it done,
    /// and whether its lane has yet to take it: one, a later superseding it.
    pub(crate) replacing: Option<(Replacement, bool)>,
}

/// What a range's member applied of its configuration, whether a change of it is pending, and,
/// while it leads, each learner's progress: what the membership's steps read of a range
/// (`Node::configuration`, `members.rs`). Read from the replica where the node's own task drives
/// it; where another shard does, as that shard reported it after the range's latest drive that
/// changed it ([`Out::Configured`]), as [`Seen`] is. Only the learners' progress: a removed member
/// of the root range learns until it says it holds the commit that removed it, and a learner the
/// target makes a voter is promoted once caught up (membership.md §4, §6), while a voter's progress
/// moves with every entry, which would report a range at each drive.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Configured {
    pub(crate) conf: ConfState,
    /// Whether a change is proposed and not yet applied (`Raft::has_pending_conf`).
    pub(crate) pending: bool,
    /// Each learner, the commit it last said its storage holds, and whether it holds everything the
    /// leader knows committed (`Replica::caught_up`); none while the member leads not.
    pub(crate) learners: Vec<(u64, u64, bool)>,
}

impl Configured {
    /// What `replica` shows now.
    pub(crate) fn of<E: Engine + 'static, S: LogStore>(replica: &Replica<E, S>) -> Self {
        Self {
            conf: replica.configuration().clone(),
            pending: replica.core().raft.has_pending_conf(),
            learners: learners(replica).collect(),
        }
    }

    /// Whether `replica` shows what this says, read without a copy, as a shard asks after each
    /// drive.
    fn shown<E: Engine + 'static, S: LogStore>(&self, replica: &Replica<E, S>) -> bool {
        self.conf == *replica.configuration()
            && self.pending == replica.core().raft.has_pending_conf()
            && self.learners.iter().copied().eq(learners(replica))
    }
}

/// Each learner of `replica`'s configuration, the commit it last said its storage holds, and
/// whether it is caught up, while `replica` leads.
fn learners<E: Engine + 'static, S: LogStore>(
    replica: &Replica<E, S>,
) -> impl Iterator<Item = (u64, u64, bool)> + '_ {
    let leads = replica.is_leader();
    let tracker = replica.core().raft.tracker();
    replica
        .configuration()
        .learners
        .iter()
        .filter(move |_| leads)
        .map(move |&id| {
            let committed = tracker.get(id).map_or(0, |p| p.committed_index);
            (id, committed, replica.caught_up(id))
        })
}

/// Steps the range at `handle` toward `target` (membership.md §6; `Toward::step`): a change
/// proposed, or its lead handed on, only while its member leads, no change is pending and no
/// hand-over is under way, one at a time. A change or a transfer the core refuses (its leader
/// stepping down, its log full) is asked again at a later turn.
pub(crate) fn step_toward<E: Engine + 'static, S: LogStore>(
    owner: &mut Owner<Answers<S>, RangeMachine<E>, Unbounded>,
    handle: Handle,
    target: &Toward,
) {
    let Some(shell) = owner.get_mut(handle) else {
        return;
    };
    let raft = &shell.core().raft;
    if !shell.is_leader() || raft.has_pending_conf() || raft.lead_transferee().is_some() {
        return;
    }
    match target.step(shell.configuration(), shell.id(), |member| {
        shell.caught_up(member)
    }) {
        Step::Propose(change) => {
            let _ = shell.change(Vec::new(), &change);
            owner.schedule(handle);
        }
        Step::Transfer(to) => {
            let _ = shell.transfer(to);
            owner.schedule(handle);
        }
        Step::Wait | Step::Done => {}
    }
}

/// Proposes what `replacement` names next of the range at `handle` while its member leads: a
/// change only while none is pending, since the core takes one at a time (a change it drops, with
/// a leader stepping down, is proposed again by whichever node leads next). Whether it is done:
/// the joining member votes, the failed one is gone, and every voter knows it.
pub(crate) fn step_replacement<E: Engine + 'static, S: LogStore>(
    owner: &mut Owner<Answers<S>, RangeMachine<E>, Unbounded>,
    handle: Handle,
    replacement: Replacement,
) -> bool {
    let Some(shell) = owner.get_mut(handle) else {
        return false;
    };
    if !shell.is_leader() {
        return false;
    }
    let next = replacement.next(
        shell.configuration(),
        shell.caught_up(replacement.joining()),
        shell.configuration_known(),
    );
    match next {
        Next::Propose(change) => {
            if !shell.core().raft.has_pending_conf() {
                // A change the core refuses (its leader stepping down, its log full) is proposed
                // again at a later turn.
                let _ = shell.change(Vec::new(), &change);
                owner.schedule(handle);
            }
            false
        }
        Next::Wait => false,
        Next::Done => true,
    }
}

/// What a range's member shows of itself: what the node's status, its leadership and its applied
/// entries read (`Node::status`, `Node::is_leader`). Read from the replica where the node's own
/// task drives it; where another shard does, as that shard reported it after the range's latest
/// drive that changed it, so the node's reads of it take no round trip (node.md §2.9).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Seen {
    pub(crate) term: u64,
    pub(crate) leader: u64,
    pub(crate) leading: bool,
    pub(crate) commit: u64,
    pub(crate) applied: u64,
    pub(crate) stalled: bool,
    pub(crate) in_flight: usize,
    pub(crate) writes: hyper_durable::Writes,
    pub(crate) fenced: bool,
    /// Whether the member is marked: its log may lack entries it acknowledged (`Replica::mark`).
    pub(crate) uncertain: bool,
    pub(crate) reads_held: usize,
    pub(crate) reads_confirming: usize,
    pub(crate) timing: Option<Timing>,
    /// Whether the member and the members its detectors trust are a quorum, or it campaigns not
    /// (`Raft::campaign_state`).
    pub(crate) trusted_quorum: bool,
    /// The voter the member hands its lead to, while it does (`Raft::lead_transferee`).
    pub(crate) transferee: Option<u64>,
}

impl Seen {
    /// What `replica` shows now.
    pub(crate) fn of<E: Engine + 'static, S: LogStore>(replica: &Replica<E, S>) -> Self {
        let raft = &replica.core().raft;
        Self {
            term: replica.term(),
            leader: replica.leader(),
            leading: replica.is_leader(),
            commit: replica.logged_commit(),
            applied: replica.applied().index,
            stalled: replica.is_stalled(),
            in_flight: replica.in_flight(),
            writes: replica.writes(),
            fenced: replica.fenced().is_some(),
            uncertain: replica.mark().is_some(),
            reads_held: raft.reads_held(),
            reads_confirming: raft.pending_read_count(),
            timing: raft.timing(),
            trusted_quorum: raft
                .campaign_state()
                .is_none_or(|state| state.trusted_quorum),
            transferee: raft.lead_transferee(),
        }
    }
}

/// How a range's shard served a read the node's shard asked of it.
pub(crate) enum Served {
    /// Its replica took no read: it does not lead, or the shard holds as many reads as it takes,
    /// or it stopped leading before a quorum confirmed the read, which is then never served.
    Refused,
    /// The range's engine failed the query; the range is fenced at its next write.
    Failed,
    /// The query's answer, read from the range's engine once the read was confirmed and the range
    /// applied through it.
    Read(Box<Read>),
}

/// Compacts each of `handles`' ranges in `owner` through what its engine made durable, freeing the
/// log's room, the compaction's write waking `waker`'s task: the node's own for the ranges its task
/// drives, a shard's for that shard's (node.md §6.2). A range with a write out is passed over: that
/// write may yet be refused for room, and hyper-log takes a compaction's start even then, as it
/// frees room, durable behind the refused write, which fences the range; its answer brings the next
/// try. A stalled range whose compaction had nothing to free makes its writes again at its next
/// drive (another range's compaction may have freed it). While a range is stalled every range is
/// driven again, as it always has been; else only those whose compaction started, or a node waiting
/// with nothing to compact would turn every range again at every turn. Whether any started.
pub(crate) fn compact<E: Engine + 'static, S: LogStore>(
    owner: &mut Owner<Answers<S>, RangeMachine<E>, Unbounded>,
    handles: impl Iterator<Item = Handle>,
    any_stalled: bool,
    now: u64,
    waker: &Waker,
) -> Result<bool, NodeError> {
    let mut made_room = false;
    for handle in handles {
        let Some(shell) = owner.get_mut(handle) else {
            continue;
        };
        let stalled = shell.is_stalled();
        if !stalled && shell.in_flight() > 0 {
            continue;
        }
        let compacted = shell
            .compact(0, now, waker)
            .map_err(mantle_range::ReplicaError::from)?;
        made_room |= compacted;
        if stalled && !compacted {
            shell.resume();
        }
        if any_stalled || compacted {
            owner.schedule(handle);
        }
    }
    Ok(made_room)
}

/// The node's shard's owner of the replicas its own task drives, and the handles it gave the
/// replicas that left it for another shard's (node.md §2.9), which their ranges' `Kept` still
/// hold. hyper-durable's handle is a slot and a generation that wraps at `u32` (`Owner::remove`),
/// so a slot emptied and filled again 2^32 times gives a handle equal to one it gave before: were
/// that one a far range's, the node would take the new replica for that range. Every insert here
/// passes [`Here::insert`], which gives none of those handles.
pub(crate) struct Here<E: Engine, S: LogStore> {
    owner: Owner<Answers<S>, RangeMachine<E>, Unbounded>,
    /// The handles of the replicas that left ([`Here::leave`]): one a range, as a range leaves
    /// once, before its first drive, so at most the ranges the node hosts.
    left: Vec<Handle>,
}

impl<E: Engine + 'static, S: LogStore> Here<E, S> {
    pub(crate) fn new(owner: Owner<Answers<S>, RangeMachine<E>, Unbounded>) -> Self {
        Self {
            owner,
            left: Vec::new(),
        }
    }

    /// Takes `replica` into a free slot, queued for its first drive, under a handle no far range
    /// holds: one equal to such a handle is taken out and put in again, which moves its slot's
    /// generation on. Each pass gives a handle no earlier pass of this insert gave, so of
    /// `left.len() + 1` passes one gives a handle none left with. Refused `Full` when no slot is
    /// free.
    pub(crate) fn insert(&mut self, replica: Replica<E, S>) -> Result<Handle, NodeError> {
        let mut handle = self.owner.insert(replica).map_err(|_| NodeError::Full)?;
        for _ in 0..self.left.len() {
            if !self.left.contains(&handle) {
                return Ok(handle);
            }
            let replica = self.owner.remove(handle).ok_or(NodeError::Bounds(
                "a handle just given names nothing".into(),
            ))?;
            handle = self.owner.insert(replica).map_err(|_| NodeError::Full)?;
        }
        if self.left.contains(&handle) {
            let _ = self.owner.remove(handle);
            return Err(NodeError::Aliased);
        }
        Ok(handle)
    }

    /// Gives back the replica at `handle`, which leaves for another shard's owner; its handle is
    /// one no insert gives again.
    pub(crate) fn leave(&mut self, handle: Handle) -> Option<Replica<E, S>> {
        let replica = self.owner.remove(handle)?;
        self.left.push(handle);
        Some(replica)
    }
}

impl<E: Engine, S: LogStore> std::ops::Deref for Here<E, S> {
    type Target = Owner<Answers<S>, RangeMachine<E>, Unbounded>;

    fn deref(&self) -> &Self::Target {
        &self.owner
    }
}

impl<E: Engine, S: LogStore> std::ops::DerefMut for Here<E, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.owner
    }
}

/// Refused, typed, when `replica` has writes out: each write records the waker of the task that
/// submitted it, so their answers would wake the shard it left. A replica moves before its first
/// drive, when it has made none.
pub(crate) fn movable<E: Engine + 'static, S: LogStore>(
    group: u128,
    replica: &Replica<E, S>,
) -> Result<(), NodeError> {
    if replica.in_flight() > 0 {
        return Err(NodeError::WritesOut(group));
    }
    Ok(())
}

/// Asks read `id` of `far`'s replica on its shard ([`In::Read`]) when the range leads as its shard
/// last showed it: whether it was asked. One whose lane has no room is not, as one its replica
/// would refuse, and its asker asks again (`Node::ask_read`, `Node::ask_read_out`); a shard with no
/// lanes, or one whose task ended, stops the node, typed.
pub(crate) fn ask_read<E: Engine + 'static, S: LogStore>(
    shards: &[Lanes<E, S>],
    far: &Far,
    id: u64,
    query: &Query,
) -> Result<bool, NodeError> {
    if !far.seen.leading {
        return Ok(false);
    }
    let lanes = shards
        .iter()
        .find(|lanes| lanes.shard() == far.shard)
        .ok_or(NodeError::ShardEnded(far.shard))?;
    match lanes.send(In::Read {
        group: far.group,
        id,
        query: query.clone(),
    }) {
        Ok(()) => Ok(true),
        Err(NodeError::LaneFull(_)) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Proposes `entry` to `far`'s range on its shard ([`In::Propose`]), at most `max` bytes. One whose
/// lane has no room is refused `LaneFull`, as one its replica would refuse: a peer's command is
/// answered `Refused` while the range leads here, which its asker asks again after its pause, and
/// the node's own pauses (`Node::propose_command`, `Node::propose_own`). A shard with no lanes, or
/// whose task ended, is refused `ShardEnded`.
pub(crate) fn propose<E: Engine + 'static, S: LogStore>(
    shards: &[Lanes<E, S>],
    far: &Far,
    entry: &Entry,
    max: u64,
) -> Result<(), NodeError> {
    shards
        .iter()
        .find(|lanes| lanes.shard() == far.shard)
        .ok_or(NodeError::ShardEnded(far.shard))?
        .send(In::Propose(far.group, entry.clone(), max))
}

/// Asks `far`'s shard to hand its lead over ([`In::Transfer`]), past the voter `failed`: whether it
/// was asked. One whose lane has no room is not, as one its core refuses: the range elects once
/// this node is gone (`node::stop`). A shard with no lanes, or whose task ended, stops the node.
pub(crate) fn ask_transfer<E: Engine + 'static, S: LogStore>(
    shards: &[Lanes<E, S>],
    far: &Far,
    failed: Option<u64>,
) -> Result<bool, NodeError> {
    let lanes = shards
        .iter()
        .find(|lanes| lanes.shard() == far.shard)
        .ok_or(NodeError::ShardEnded(far.shard))?;
    match lanes.send(In::Transfer {
        group: far.group,
        failed,
    }) {
        Ok(()) => Ok(true),
        Err(NodeError::LaneFull(_)) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Begins handing the lead of `replica`'s range, which it leads, to a voter whose log holds
/// everything committed, or, with none such, the voter whose log holds the most, which the core
/// brings up to its own before it hands over (Raft §3.10); once a transfer to `failed` was given
/// up, to another voter where there is one. The voter it goes to, or `None` where the core refuses,
/// after which the range elects as after a crash (`node::stop`).
pub(crate) fn transfer<E: Engine + 'static, S: LogStore>(
    replica: &mut Replica<E, S>,
    failed: Option<u64>,
) -> Option<u64> {
    let id = replica.id();
    let tracker = replica.core().raft.tracker();
    let to = replica
        .configuration()
        .voters
        .iter()
        .copied()
        .filter(|voter| *voter != id)
        .max_by_key(|voter| {
            (
                Some(*voter) != failed,
                replica.caught_up(*voter),
                tracker.get(*voter).map_or(0, |p| p.matched),
            )
        })?;
    replica.transfer(to).ok()?;
    Some(to)
}

/// The node's shard's ends of a range's shard's two lanes.
pub(crate) struct Lanes<E: Engine, S: LogStore> {
    shard: usize,
    capacity: usize,
    into: Sender<In<E, S>>,
    from: ChannelReceiver<Out>,
    /// What a look for the shard's output took and the node has yet to handle ([`Lanes::arrived`]).
    held: Option<Out>,
    /// The peers whose connection went that the shard is yet to be told of ([`Lanes::lost`]):
    /// each once, so at most the node's peers.
    lost: Vec<PeerId>,
}

impl<E, S> Lanes<E, S>
where
    E: Engine + Send + 'static,
    S: LogStore + Send + 'static,
{
    /// Starts shard `shard`'s task on the shard `holder` names, through its control queue (as the
    /// S3 listener's lanes start their takers, `listener::shards`), its owner holding `slots`
    /// replicas and each lane `capacity` values. Refused when a lane cannot be made or the shard
    /// takes no task now: gone, or its control queue full.
    pub(crate) fn start(
        holder: SlotHolder,
        shard: usize,
        slots: usize,
        capacity: usize,
    ) -> Result<Self, NodeError> {
        let (into, taken) = channel(capacity)?;
        let (given, from) = channel(capacity)?;
        hyper_rt::runtime::submit_to_holder(holder, run(taken, given, slots, capacity))?;
        Ok(Self {
            shard,
            capacity,
            into,
            from,
            held: None,
            lost: Vec::new(),
        })
    }
}

impl<E: Engine + 'static, S: LogStore> Lanes<E, S> {
    /// Each of the shard's ranges' engines as they stand, its task ended (`Node::close`): asked
    /// once its lane has room, which its task makes at its next turn. A shard that gives them not
    /// within `patience_ns` is refused, typed, its task ending as its lanes go.
    pub(crate) async fn release(&mut self, patience_ns: u64) -> Result<Vec<(u128, E)>, NodeError> {
        let shard = self.shard;
        let (reply, engines) = oneshot()?;
        let asked = pin!(async {
            self.into
                .send(In::Release(reply))
                .await
                .map_err(|_| NodeError::ShardEnded(shard))?;
            engines.await.map_err(|_| NodeError::ShardEnded(shard))
        });
        match race2(asked, pin!(hyper_rt::futures::sleep(patience_ns))).await {
            Either::First(engines) => engines,
            Either::Second(Ok(())) => Err(NodeError::ShardSilent(shard)),
            Either::Second(Err(error)) => Err(error.into()),
        }
    }

    /// The shard the lanes reach.
    pub(crate) fn shard(&self) -> usize {
        self.shard
    }

    /// The values each lane holds at most: the most the node takes from the shard in a turn.
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    /// Hands `sent` to the shard without waiting: refused `LaneFull` when its lane holds all it
    /// takes, and `ShardEnded` when its task is gone.
    pub(crate) fn send(&self, sent: In<E, S>) -> Result<(), NodeError> {
        self.into.try_send(sent).map_err(|refused| match refused {
            SyncError::Full(_) => NodeError::LaneFull(self.shard),
            SyncError::Closed(_) | SyncError::NotOnShardThread(_) => {
                NodeError::ShardEnded(self.shard)
            }
        })
    }

    /// Holds that `peer`'s connection went, for the shard to be told at the turn's take
    /// ([`Lanes::tell_lost`]), each peer once.
    pub(crate) fn lost(&mut self, peer: PeerId) {
        if !self.lost.contains(&peer) {
            self.lost.push(peer);
        }
    }

    /// Tells the shard of each peer whose connection went ([`In::Unreachable`]) while its lane has
    /// room, and holds the rest for the next turn's, as the ends of snapshots are held
    /// (`Node::report_far`): a range's leader told gives up the window it sent that peer ahead of
    /// its answers, which went with the connection, and probes it again from what it is known to
    /// hold (hyper-raft's `MsgUnreachable`, etcd raft's `ReportUnreachable`); dropped, the leader
    /// would wait on that window until the peer's answers after its next connection undo it. A
    /// held report costs one entry a peer. A shard whose task ended is refused `ShardEnded`, which
    /// stops the node.
    pub(crate) fn tell_lost(&mut self) -> Result<(), NodeError> {
        while let Some(&peer) = self.lost.first() {
            match self.send(In::Unreachable(peer)) {
                Ok(()) => {
                    self.lost.remove(0);
                }
                Err(NodeError::LaneFull(_)) => break,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Whether the shard gave something the node has yet to take, or ended. A look that finds
    /// neither asks the lane to wake the polling task when either comes, as a wait of the node's
    /// task begun after it ends at that wake.
    pub(crate) fn arrived(&mut self, context: &mut Context<'_>) -> bool {
        if self.held.is_some() {
            return true;
        }
        match pin!(self.from.recv()).poll(context) {
            Poll::Ready(Ok(out)) => {
                self.held = Some(out);
                true
            }
            Poll::Ready(Err(_)) => true,
            Poll::Pending => false,
        }
    }

    /// The next thing the shard gave, none when nothing waits; refused `ShardEnded` once its task
    /// is gone and all it gave is taken.
    pub(crate) fn take(&mut self) -> Result<Option<Out>, NodeError> {
        if let Some(out) = self.held.take() {
            return Ok(Some(out));
        }
        self.from
            .try_recv()
            .map_err(|_| NodeError::ShardEnded(self.shard))
    }
}

/// A range as its shard keeps it.
struct Kept {
    group: u128,
    handle: Handle,
    /// The replica's latest wake, held in [`Shard::deadlines`].
    wake: Option<u64>,
    /// The peers last told the node's shard ([`Out::Pairs`]).
    peers: Vec<PeerId>,
    /// What the range showed when last told the node's shard ([`Out::Seen`]).
    seen: Seen,
    /// Its configuration when last told the node's shard ([`Out::Configured`]).
    configured: Configured,
    /// The replacement the node's shard asked of it ([`In::Replace`]), until it is done.
    replacing: Option<Replacement>,
}

/// A read a range's replica took and has yet to serve: its range, its context and its query.
struct Reading {
    group: u128,
    id: u64,
    query: Query,
}

/// A shard's ranges in the owner of its task.
struct Shard<E: Engine, S: LogStore> {
    owner: Owner<Answers<S>, RangeMachine<E>, Unbounded>,
    ranges: Vec<Kept>,
    /// Each range's next wake and its place in `ranges`.
    deadlines: BTreeSet<(u64, usize)>,
    out: Output<Applied>,
    /// What the replicas gave this turn, for the node's shard; kept for its storage. At most the
    /// replicas' one `Ready` each and a few words of each, handed on before the next turn.
    gave: Vec<Out>,
    driven: Vec<(Handle, Driven)>,
    /// The reads the replicas took and have yet to serve, at most `reads_most`: a lane's capacity,
    /// the exchanges the transport admits at once, each of which asks at most one. The node's own
    /// reads ask here too; one past the bound is refused ([`Served::Refused`]) and asked again
    /// after its asker's pause.
    reading: Vec<Reading>,
    reads_most: usize,
    /// The reads' contexts the replicas gave back this turn, each with its range; kept for its
    /// storage, at most the reads taken.
    served: Vec<(u128, Vec<u8>)>,
    /// The task's waker, which a compaction's write wakes, and whether the node's shard asked a
    /// round of compaction ([`In::Compact`]) the shard has yet to make.
    waker: Waker,
    compact: bool,
    /// Where the node's shard asked its ranges' engines back ([`In::Release`]).
    release: Option<OneshotSender<Vec<(u128, E)>>>,
}

impl<E: Engine + 'static, S: LogStore> Shard<E, S> {
    fn at(&self, group: u128) -> Option<Handle> {
        self.ranges
            .iter()
            .find(|kept| kept.group == group)
            .map(|kept| kept.handle)
    }

    /// Takes what the node's shard handed over.
    fn take(&mut self, sent: In<E, S>) -> Result<(), NodeError> {
        match sent {
            In::Adopt(group, replica) => {
                let peers = sorted(replica.peers());
                let seen = Seen::of(&replica);
                let configured = Configured::of(&replica);
                let handle = self.owner.insert(*replica).map_err(|_| NodeError::Full)?;
                self.ranges.push(Kept {
                    group,
                    handle,
                    wake: None,
                    peers,
                    seen,
                    configured,
                    replacing: None,
                });
            }
            In::Step {
                group,
                sender,
                message,
            } => {
                let Some(handle) = self.at(group) else {
                    return Ok(());
                };
                if let Some(replica) = self.owner.get_mut(handle)
                    && may_act(replica, sender)
                {
                    // A replica waiting for room refuses a message, as the network may drop it.
                    let _ = replica.step(message);
                    self.owner.schedule(handle);
                }
            }
            In::Propose(group, entry, max) => {
                let Some(handle) = self.at(group) else {
                    return Ok(());
                };
                if let Some(replica) = self.owner.get_mut(handle) {
                    // A proposal the range refuses is lost as one its leader drops: its asker's
                    // patience ends it (node.md §2.9, step 4 answers each ask in a slot).
                    let _ = mantle_range::propose_entry(replica, &entry, max);
                    self.owner.schedule(handle);
                }
            }
            In::Believe(change) => {
                let peer = change.peer();
                for kept in &self.ranges {
                    let Some(replica) = self.owner.get_mut(kept.handle) else {
                        continue;
                    };
                    if !replica.peers().any(|other| other == peer) {
                        continue;
                    }
                    // A replica that refuses it is fenced, which its next drive reports.
                    let _ = match change {
                        Change::Suspected(_) => replica.suspect(peer),
                        Change::Trusted { .. } => replica.trust(peer),
                        Change::Restarted { .. } => replica.restarted(peer),
                        Change::Stopped { .. } => replica.stopped(peer),
                        Change::DiskSlow { .. } => replica.disk_slow(peer),
                        Change::DiskCaughtUp { .. } => replica.disk_caught_up(peer),
                        Change::ModeDiffers { .. } | Change::ModeAgrees { .. } => Ok(()),
                    };
                    self.owner.schedule(kept.handle);
                }
            }
            In::Belief(group, belief) => {
                let Some(handle) = self.at(group) else {
                    return Ok(());
                };
                if let Some(replica) = self.owner.get_mut(handle) {
                    let _ = belief.tell(replica);
                    self.owner.schedule(handle);
                }
            }
            In::Measure(measured) => {
                for kept in &self.ranges {
                    let Some(replica) = self.owner.get_mut(kept.handle) else {
                        continue;
                    };
                    let was = replica.core().raft.timing();
                    if let Ok(Some(election)) = measured.time(replica) {
                        self.gave.push(Out::Elected(kept.group, election));
                    }
                    // Its core draws a campaign's delay, or times a timer, only when driven.
                    if replica.core().raft.timing() != was {
                        self.owner.schedule(kept.handle);
                    }
                }
            }
            In::Read { group, id, query } => {
                let handle = self.at(group);
                let room = self.reading.len() < self.reads_most;
                let asked = handle
                    .filter(|_| room)
                    .and_then(|handle| self.owner.get_mut(handle))
                    .is_some_and(|replica| replica.read(id.to_le_bytes().to_vec()).is_ok());
                match handle {
                    Some(handle) if asked => {
                        self.owner.schedule(handle);
                        self.reading.push(Reading { group, id, query });
                    }
                    _ => self.gave.push(Out::Served(group, id, Served::Refused)),
                }
            }
            In::Compact => self.compact = true,
            In::Reported { group, to, arrived } => {
                let Some(handle) = self.at(group) else {
                    return Ok(());
                };
                if let Some(replica) = self.owner.get_mut(handle) {
                    // A replica that refuses it is fenced, which its next drive reports.
                    let _ = replica.report_snapshot(to, arrived);
                    self.owner.schedule(handle);
                }
            }
            In::Unreachable(peer) => {
                for kept in &self.ranges {
                    let Some(replica) = self.owner.get_mut(kept.handle) else {
                        continue;
                    };
                    if !replica.peers().any(|other| other == peer) {
                        continue;
                    }
                    // One a replica waiting for room refuses is lost, as the node's own is
                    // (`Node::unkey`): the peer's next reply, or its silence, moves its progress.
                    let _ = replica.report_unreachable(peer);
                    self.owner.schedule(kept.handle);
                }
            }
            In::Transfer { group, failed } => {
                let handle = self.at(group);
                let to = handle
                    .and_then(|handle| self.owner.get_mut(handle))
                    .filter(|replica| replica.is_leader())
                    .and_then(|replica| transfer(replica, failed));
                if let (Some(handle), Some(_)) = (handle, to) {
                    self.owner.schedule(handle);
                }
                self.gave.push(Out::Handing(group, to));
            }
            In::Toward(group, target) => {
                let replacing = self
                    .ranges
                    .iter()
                    .any(|kept| kept.group == group && kept.replacing.is_some());
                // No replacement and a step toward the members change one range at once.
                if let Some(handle) = self.at(group).filter(|_| !replacing) {
                    step_toward(&mut self.owner, handle, &target);
                }
            }
            In::Replace(group, replacement) => {
                if let Some(kept) = self.ranges.iter_mut().find(|kept| kept.group == group) {
                    kept.replacing = Some(replacement);
                    self.owner.schedule(kept.handle);
                }
            }
            In::Release(reply) => self.release = Some(reply),
        }
        Ok(())
    }

    /// Each range's engine as it stands, its replica gone from the owner, with whatever it had
    /// not made durable, as `Node::close` takes the node's own.
    fn engines(&mut self) -> Vec<(u128, E)> {
        let mut engines = Vec::with_capacity(self.ranges.len());
        for kept in self.ranges.drain(..) {
            if let Some(replica) = self.owner.remove(kept.handle) {
                engines.push((kept.group, replica.into_machine().into_engine()));
            }
        }
        engines
    }

    /// The round of compaction the node's shard asked, after a turn ([`compact`]): its ranges
    /// with the shard's waker, each driven again while one is stalled.
    fn compact(&mut self, now: u64) -> Result<(), NodeError> {
        if !std::mem::take(&mut self.compact) {
            return Ok(());
        }
        let any_stalled = self.ranges.iter().any(|kept| {
            self.owner
                .get(kept.handle)
                .is_some_and(|replica| replica.is_stalled())
        });
        let handles = self.ranges.iter().map(|kept| kept.handle);
        if compact(&mut self.owner, handles, any_stalled, now, &self.waker)? {
            self.gave.push(Out::Compacted);
        }
        Ok(())
    }

    /// Steps each replacement the shard keeps ([`step_replacement`]), each turn, and hands back
    /// those done.
    fn replace(&mut self) {
        for kept in &mut self.ranges {
            let Some(replacement) = kept.replacing else {
                continue;
            };
            if step_replacement(&mut self.owner, kept.handle, replacement) {
                kept.replacing = None;
                self.gave.push(Out::Replaced(kept.group, replacement));
            }
        }
    }

    /// Serves the reads the replicas gave back this turn, each confirmed and applied through: its
    /// query answered from its range's engine now, in at most what a reply's body carries past its
    /// head. Then refuses each read whose range no longer leads, which its core will not give back
    /// (node.md §5.1, as `Node::reply_commands` redirects the node's own).
    fn serve(&mut self) {
        let room = crate::read::reply_room(NodeClasses::frame_bound(classes::Class::Request));
        for (group, context) in self.served.drain(..) {
            let Ok(id) = <[u8; 8]>::try_from(context.as_slice()).map(u64::from_le_bytes) else {
                continue;
            };
            let Some(at) = self
                .reading
                .iter()
                .position(|reading| reading.id == id && reading.group == group)
            else {
                continue;
            };
            let reading = self.reading.swap_remove(at);
            let replica = self
                .ranges
                .iter()
                .find(|kept| kept.group == group)
                .and_then(|kept| self.owner.get_mut(kept.handle));
            let served = match replica.map(|replica| {
                reading
                    .query
                    .answer(replica.machine_mut().engine_mut(), room)
            }) {
                Some(Ok(read)) => Served::Read(Box::new(read)),
                _ => Served::Failed,
            };
            self.gave.push(Out::Served(group, id, served));
        }
        let mut at = 0;
        while let Some(reading) = self.reading.get(at) {
            let leads = self
                .ranges
                .iter()
                .find(|kept| kept.group == reading.group)
                .and_then(|kept| self.owner.get(kept.handle))
                .is_some_and(|replica| replica.is_leader());
            if leads {
                at = at.saturating_add(1);
                continue;
            }
            let reading = self.reading.swap_remove(at);
            self.gave
                .push(Out::Served(reading.group, reading.id, Served::Refused));
        }
    }

    /// Queues the replicas whose deadline came, and those with a write out, which a log's answer
    /// may have woken.
    fn schedule_due(&mut self, now: u64) {
        while let Some(&(at, place)) = self.deadlines.first() {
            if at > now {
                break;
            }
            self.deadlines.pop_first();
            if let Some(kept) = self.ranges.get_mut(place) {
                kept.wake = None;
                self.owner.schedule(kept.handle);
            }
        }
        for kept in &self.ranges {
            if self
                .owner
                .get(kept.handle)
                .is_some_and(|replica| replica.in_flight() > 0)
            {
                self.owner.schedule(kept.handle);
            }
        }
    }

    /// Whether a range's write was answered since its last drive.
    fn answered(&mut self) -> bool {
        let owner = &mut self.owner;
        self.ranges.iter().any(|kept| {
            owner
                .get_mut(kept.handle)
                .is_some_and(|replica| replica.in_flight() > 0 && replica.log_mut().answered())
        })
    }

    /// A turn of the owner: each queued replica driven once, what it gave kept for the node's
    /// shard, its next wake kept.
    fn drive(&mut self, now: u64) -> Result<(), NodeError> {
        let mut fenced = None;
        let Self {
            owner,
            ranges,
            out,
            gave,
            driven,
            served,
            ..
        } = self;
        owner.turn(now, out, |handle, outcome, out| {
            let group = ranges
                .iter()
                .find(|kept| kept.handle == handle)
                .map_or(0, |kept| kept.group);
            for message in out.messages.drain(..) {
                gave.push(Out::Message(group, message));
            }
            for answer in out.answers.drain(..) {
                gave.push(Out::Applied(group, answer));
            }
            for (context, _) in out.reads.drain(..) {
                served.push((group, context));
            }
            match outcome {
                Ok(d) => driven.push((handle, d)),
                Err(e) => {
                    fenced.get_or_insert(e);
                }
            }
        });
        for (handle, Driven { wake, flushed, .. }) in self.driven.drain(..) {
            if let Some((started, durable)) = flushed {
                self.gave.push(Out::Flushed(started, durable));
            }
            let Some(place) = self.ranges.iter().position(|kept| kept.handle == handle) else {
                continue;
            };
            let Some(kept) = self.ranges.get_mut(place) else {
                continue;
            };
            if let Some(old) = kept.wake.take() {
                self.deadlines.remove(&(old, place));
            }
            if let Some(at) = wake {
                kept.wake = Some(at);
                self.deadlines.insert((at, place));
            }
            // The peers the stream keeps pairs with for it, told again once its configuration
            // changed them.
            if let Some(replica) = self.owner.get(handle)
                && !(replica
                    .peers()
                    .all(|peer| kept.peers.binary_search(&peer).is_ok())
                    && kept
                        .peers
                        .iter()
                        .all(|peer| replica.peers().any(|other| other == *peer)))
            {
                kept.peers = sorted(replica.peers());
                self.gave.push(Out::Pairs(kept.group, kept.peers.clone()));
            }
            // What it shows, told again once a drive changed it.
            if let Some(replica) = self.owner.get(handle) {
                let seen = Seen::of(replica);
                if seen != kept.seen {
                    kept.seen = seen;
                    self.gave.push(Out::Seen(kept.group, seen));
                }
                if !kept.configured.shown(replica) {
                    kept.configured = Configured::of(replica);
                    self.gave
                        .push(Out::Configured(kept.group, kept.configured.clone()));
                }
            }
        }
        self.serve();
        match fenced {
            // A fenced range stops the node, as one on the node's shard does (`Node::drive`).
            Some(e) => Err(NodeError::Range(e.into())),
            None => Ok(()),
        }
    }

    /// When the shard's task wakes though nothing arrives: its earliest deadline, at most the idle
    /// timeout away, as every wait of the node's is bounded.
    fn sleep_ns(&self, now: u64) -> u64 {
        let idle = u64::try_from(bounds::idle_timeout().as_nanos()).unwrap_or(u64::MAX);
        self.deadlines
            .first()
            .map_or(idle, |(at, _)| at.saturating_sub(now).min(idle))
    }
}

/// `peers`, sorted, each once.
fn sorted(peers: impl Iterator<Item = PeerId>) -> Vec<PeerId> {
    let mut peers: Vec<PeerId> = peers.collect();
    peers.sort_unstable();
    peers.dedup();
    peers
}

/// Whether `sender` may act on `replica`'s range: a member of its newest configuration, or any
/// node while the replica is none (node.md §3.6; `Taking::checked`).
pub(crate) fn may_act<E: Engine + 'static, S: LogStore>(
    replica: &Replica<E, S>,
    sender: PeerId,
) -> bool {
    let id = replica.id();
    let configuration = replica.core().raft.configuration();
    !configuration.contains(id) || configuration.contains(sender)
}

/// A shard's task: takes what the node's shard hands it, at most a lane's capacity a turn, drives
/// its replicas, and hands back what they gave, waiting for room. Ends when the node's shard's end
/// of either lane is gone, or once it handed back why it stopped.
async fn run<E: Engine + 'static, S: LogStore>(
    mut taken: ChannelReceiver<In<E, S>>,
    given: Sender<Out>,
    slots: usize,
    capacity: usize,
) {
    let waker = poll_fn(|context| Poll::Ready(context.waker().clone())).await;
    let mut shard = Shard {
        owner: Owner::new(vec![waker.clone(); slots]),
        ranges: Vec::with_capacity(slots),
        deadlines: BTreeSet::new(),
        out: Output::default(),
        gave: Vec::new(),
        driven: Vec::new(),
        reading: Vec::with_capacity(capacity),
        reads_most: capacity,
        served: Vec::new(),
        waker,
        compact: false,
        release: None,
    };
    let mut arrived = None;
    loop {
        let mut stopped = arrived.take().and_then(|sent| shard.take(sent).err());
        for _ in 0..capacity {
            if stopped.is_some() {
                break;
            }
            match taken.try_recv() {
                Ok(Some(sent)) => stopped = shard.take(sent).err(),
                Ok(None) => break,
                Err(_) => return,
            }
        }
        // Asked its ranges' engines back, it gives them and ends: the node closes.
        if let Some(reply) = shard.release.take() {
            let _ = reply.send(shard.engines());
            return;
        }
        let now = hyper_rt::futures::now_ns();
        if stopped.is_none() {
            shard.schedule_due(now);
            stopped = shard
                .drive(now)
                .and_then(|()| shard.compact(now))
                .map(|()| shard.replace())
                .err();
        }
        for out in shard.gave.drain(..) {
            if given.send(out).await.is_err() {
                return;
            }
        }
        if let Some(error) = stopped {
            let _ = given.send(Out::Stopped(error)).await;
            return;
        }
        // With work still queued the task yields once, so the shard's other tasks run between
        // its turns, and turns again.
        if shard.owner.has_work() || shard.answered() {
            hyper_rt::futures::yield_now().await;
            continue;
        }
        let sleep_ns = shard.sleep_ns(now);
        let mut woken = false;
        let any_wake = pin!(poll_fn(move |_| {
            if woken {
                Poll::Ready(())
            } else {
                woken = true;
                Poll::Pending
            }
        }));
        let sleep = pin!(hyper_rt::futures::sleep(sleep_ns));
        match race2(taken.recv(), race2(sleep, any_wake)).await {
            Either::First(Ok(sent)) => arrived = Some(sent),
            Either::First(Err(_)) => return,
            Either::Second(Either::First(Err(error))) => {
                let _ = given.send(Out::Stopped(error.into())).await;
                return;
            }
            Either::Second(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use hyper_block::buf::Alignment;
    use hyper_block::file::{CachingRequest, DeviceFile};
    use hyper_log::Log;
    use hyper_rt::runtime::{Runtime, RuntimeConfig};
    use mantle_meta::apply::Layer;
    use mantle_meta::engine::Model;
    use mantle_meta::name;
    use mantle_meta::record::{Version, Versioning};
    use mantle_meta::wire::{Command, Sessioned};
    use mantle_range::{ConfState, Elections, GroupStore, Range};

    use super::*;

    const GROUP: u128 = 1;

    type Stored = GroupStore<DeviceFile>;

    /// A runtime of `shards` shards, each with `tasks` task slots (as the listener's lanes' tests
    /// start theirs).
    fn started(shards: u16, tasks: usize) -> Runtime {
        Runtime::start(&RuntimeConfig {
            shards,
            tasks_per_shard: tasks,
            timers_per_shard: 1,
            interests_per_shard: hyper_rt::runtime::interests_for(tasks),
            ring_entries: 1,
            step_budget_ns: 1_000_000_000,
            timer_tick_ns: 100_000,
            batch: 1,
            pin: false,
            cores: Vec::new(),
            page_bytes: usize::try_from(hyper_rt::machine::facts::Facts::query().page.base)
                .unwrap(),
            spin_ns: 0,
            wake_tracking: None,
            latency_objective_ns: None,
        })
        .unwrap()
    }

    /// A Name range of one member, node 1, at a cell's settings.
    fn one_member() -> Range {
        let (settings, rules) = crate::derive::cell(3).unwrap();
        Range {
            layer: Layer::Name,
            rules,
            boot: ConfState {
                voters: vec![1],
                ..ConfState::default()
            },
            settings,
            members: 0,
        }
    }

    /// A log of its own in `dir`, sized for one range.
    fn log_in(dir: &std::path::Path, range: &Range) -> Log<DeviceFile> {
        let align = Alignment::new(4096).unwrap();
        let config = crate::derive::log(align, &range.settings, 1).unwrap();
        let file =
            DeviceFile::open(&dir.join("log"), true, CachingRequest::Buffered, align).unwrap();
        Log::create(file, config, 1).unwrap()
    }

    /// Node 1's replica of `range` on `log`, campaigning: a sole voter, it leads once its vote is
    /// durable, at a drive past its first.
    fn campaigning(log: &Log<DeviceFile>, range: &Range) -> Replica<Model, Stored> {
        let mut engine = Model::default();
        engine.install(0, name::first(1).unwrap()).unwrap();
        engine.persist().unwrap();
        let store = Answers::new(mantle_range::claim(log, GROUP, range).unwrap());
        let mut replica =
            mantle_range::open_shell(1, store, engine, range, 1, Elections::Suspicion).unwrap();
        replica.campaign().unwrap();
        replica
    }

    /// An entry of one put.
    fn put() -> Entry {
        let command = Command::Name(Box::new(name::Command::Put(name::Put {
            bucket: "b".into(),
            incarnation: 1,
            key: "k".into(),
            versioning: Versioning::Enabled,
            preconditions: name::Preconditions::default(),
            at_ns: 0,
            ordered_ns: None,
            version: Version {
                marker: false,
                null: false,
                modified_ns: 0,
                etag: "e".into(),
                size: 1,
                checksum: None,
                file: Some(1),
                owner: "o".into(),
                headers: Vec::new(),
                retention: None,
                legal_hold: None,
                listing: None,
            },
            default: None,
            id: 0,
            deadline_ns: u64::MAX,
        })));
        Entry {
            at_ns: 0,
            commands: vec![Sessioned {
                session: 0,
                serial: 1,
                unanswered: 0,
                command,
            }],
        }
    }

    /// How long the test's thread waits on a fact the shards' lanes show before it fails: each step
    /// it waits through is a write made durable and a lane's hop, so ten seconds is far past it on
    /// a loaded host and fails only a wait that would never end (as `listener::shards`'s tests
    /// bound theirs).
    const PATIENCE: Duration = Duration::from_secs(10);

    /// Why the test's thread stopped waiting on a shard's lane for `what` without it.
    #[derive(Debug)]
    enum Waited {
        /// [`PATIENCE`] passed first.
        Patience(&'static str),
        /// The shard stopped, or its lane ended, first.
        Stopped(&'static str, NodeError),
    }

    impl std::fmt::Display for Waited {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Patience(what) => write!(f, "waited {PATIENCE:?} for {what}"),
                Self::Stopped(what, error) => write!(f, "the shard stopped before {what}: {error}"),
            }
        }
    }

    /// The next thing the shard gives, polled on the test's thread (hyper-rt's lanes have no timed
    /// wait) until `until`, waited for as `what`.
    fn taken(
        lanes: &mut Lanes<Model, Stored>,
        what: &'static str,
        until: std::time::Instant,
    ) -> Result<Out, Waited> {
        loop {
            match lanes.take() {
                Ok(Some(Out::Stopped(error))) | Err(error) => {
                    return Err(Waited::Stopped(what, error));
                }
                Ok(Some(out)) => return Ok(out),
                Ok(None) if std::time::Instant::now() >= until => {
                    return Err(Waited::Patience(what));
                }
                Ok(None) => std::thread::yield_now(),
            }
        }
    }

    /// A shard's ends of its lanes, held by a test.
    type Ends = (ChannelReceiver<In<Model, Stored>>, Sender<Out>);

    /// The lanes of a shard with no task: the test holds the shard's ends.
    fn unstarted(capacity: usize) -> (Lanes<Model, Stored>, Ends) {
        let (into, taken) = channel(capacity).unwrap();
        let (given, from) = channel(capacity).unwrap();
        let lanes = Lanes {
            shard: 1,
            capacity,
            into,
            from,
            held: None,
            lost: Vec::new(),
        };
        (lanes, (taken, given))
    }

    fn measured() -> In<Model, Stored> {
        In::Measure(Box::new(Measured {
            granularity: Duration::ZERO,
            flush_mean: None,
            paths: Vec::new(),
        }))
    }

    /// A one-member range moved to the second shard of two before its first drive is driven by
    /// that shard's task, its log's answers waking it there, and answers a proposal: the test's
    /// thread hands it over and reads what it gave, and nothing else holds an owner.
    #[test]
    fn a_range_on_the_second_shard_is_driven_there_and_answers_a_proposal() {
        let rt = started(2, 2);
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let replica = campaigning(&log, &range);
        movable(GROUP, &replica).unwrap();
        let holder = rt.holder_of(rt.shard_ids()[1]).unwrap();
        let mut lanes = Lanes::start(holder, 1, 1, 8).unwrap();
        lanes.send(In::Adopt(GROUP, Box::new(replica))).unwrap();
        // A proposal before the range leads is dropped, as a follower drops one: proposed again
        // at each write its shard made durable, the vote's and the leader's first entry's among
        // them, until one is applied.
        let (mut flushed, mut applied) = (0, None);
        let until = deadline();
        while applied.is_none() {
            match next(&mut lanes, "the proposal applied", until, |_| false) {
                Out::Applied(group, answer) => applied = Some((group, answer)),
                Out::Flushed(..) => {
                    flushed += 1;
                    lanes
                        .send(In::Propose(GROUP, put(), range.settings.max_entry_bytes))
                        .unwrap();
                }
                _ => {}
            }
        }
        let (group, answer) = applied.unwrap();
        assert_eq!(group, GROUP);
        assert_eq!((answer.session, answer.serial), (0, 1));
        assert!(answer.index > 0);
        assert!(flushed > 0, "its writes were made durable on its shard");
        drop(lanes);
        rt.shutdown().unwrap();
        drop(log);
    }

    /// A one-member range moved to the second shard of two before its first drive: its lanes,
    /// once its shard showed it leading, what it showed when it moved, and the messages it gave
    /// meanwhile.
    fn leading(
        rt: &Runtime,
        log: &Log<DeviceFile>,
        range: &Range,
    ) -> (Lanes<Model, Stored>, Seen, Vec<Message>) {
        let replica = campaigning(log, range);
        let moved = Seen::of(&replica);
        let holder = rt.holder_of(rt.shard_ids()[1]).unwrap();
        let mut lanes = Lanes::start(holder, 1, 1, 8).unwrap();
        lanes.send(In::Adopt(GROUP, Box::new(replica))).unwrap();
        let (mut messages, until) = (Vec::new(), deadline());
        loop {
            match next(&mut lanes, "the range shown leading", until, |_| false) {
                Out::Seen(group, seen) if seen.leading => {
                    assert_eq!(group, GROUP);
                    return (lanes, moved, messages);
                }
                Out::Message(_, message) => messages.push(message),
                _ => {}
            }
        }
    }

    /// When a wait begun now has waited [`PATIENCE`].
    fn deadline() -> std::time::Instant {
        std::time::Instant::now() + PATIENCE
    }

    /// What the shard gives next past what `skip` passes over, waited for as `what` until `until`.
    fn next(
        lanes: &mut Lanes<Model, Stored>,
        what: &'static str,
        until: std::time::Instant,
        mut skip: impl FnMut(&Out) -> bool,
    ) -> Out {
        loop {
            match taken(lanes, what, until) {
                Ok(out) if skip(&out) => {}
                Ok(out) => return out,
                Err(waited) => panic!("{waited}"),
            }
        }
    }

    /// What a far range shows reaches the node's shard after each drive that changed it: its
    /// leading, its term and leader, then the entry it applied, with no ask of the node's.
    #[test]
    fn a_far_ranges_status_is_seen_from_the_nodes_shard() {
        let rt = started(2, 2);
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let (mut lanes, moved, _) = leading(&rt, &log, &range);
        assert!(!moved.leading);
        lanes
            .send(In::Propose(GROUP, put(), range.settings.max_entry_bytes))
            .unwrap();
        let Out::Applied(_, answer) = next(&mut lanes, "its entry applied", deadline(), |out| {
            !matches!(out, Out::Applied(..))
        }) else {
            panic!("`next` gives only what it does not skip")
        };
        let Out::Seen(group, seen) = next(&mut lanes, "its entry shown", deadline(), |out| {
            !matches!(out, Out::Seen(..))
        }) else {
            panic!("`next` gives only what it does not skip")
        };
        assert_eq!(group, GROUP);
        assert!(seen.leading);
        assert_eq!(seen.leader, 1);
        assert!(seen.term >= moved.term && seen.term > 0);
        assert!(seen.applied >= answer.index);
        assert!(seen.commit >= seen.applied);
        assert!(!seen.fenced);
        drop(lanes);
        rt.shutdown().unwrap();
        drop(log);
    }

    /// A read of a far range is asked of its replica on its shard and served there from its
    /// engine, its answer handed back by its id; one of a range the shard holds not is refused.
    #[test]
    fn a_far_ranges_read_is_served_through_the_lanes() {
        let rt = started(2, 2);
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let (mut lanes, _, _) = leading(&rt, &log, &range);
        let query = Query::Current {
            bucket: "b".into(),
            key: "k".into(),
        };
        lanes
            .send(In::Read {
                group: GROUP,
                id: 7,
                query: query.clone(),
            })
            .unwrap();
        let Out::Served(group, id, served) =
            next(&mut lanes, "the read served", deadline(), |out| {
                !matches!(out, Out::Served(..))
            })
        else {
            panic!("`next` gives only what it does not skip")
        };
        assert_eq!((group, id), (GROUP, 7));
        // No bucket `b` was made: the range's engine says so.
        assert!(matches!(served, Served::Read(read) if *read == Read::NoBucket));
        lanes
            .send(In::Read {
                group: GROUP + 1,
                id: 8,
                query,
            })
            .unwrap();
        let Out::Served(group, id, served) =
            next(&mut lanes, "the read served", deadline(), |out| {
                !matches!(out, Out::Served(..))
            })
        else {
            panic!("`next` gives only what it does not skip")
        };
        assert_eq!((group, id), (GROUP + 1, 8));
        assert!(matches!(served, Served::Refused));
        drop(lanes);
        rt.shutdown().unwrap();
        drop(log);
    }

    /// A command proposed to a far range is answered to the command waiting for it, as the node's
    /// shard takes the range's applied answer (`Node::took`).
    #[test]
    fn a_far_ranges_answer_reaches_its_command() {
        let rt = started(2, 2);
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let (mut lanes, _, _) = leading(&rt, &log, &range);
        let mut commanded = vec![super::super::Commanded {
            origin: super::super::CommandOrigin::Own(0),
            group: GROUP,
            commands: vec![(0, 1, None)],
        }];
        lanes
            .send(In::Propose(GROUP, put(), range.settings.max_entry_bytes))
            .unwrap();
        let Out::Applied(group, applied) =
            next(&mut lanes, "its entry applied", deadline(), |out| {
                !matches!(out, Out::Applied(..))
            })
        else {
            panic!("`next` gives only what it does not skip")
        };
        super::super::answer_command(&mut commanded, group, &applied);
        assert!(commanded[0].commands[0].2.is_some());
        drop(lanes);
        rt.shutdown().unwrap();
        drop(log);
    }

    /// A round of compaction the node's shard asks runs on the far range's shard: the range,
    /// with an entry applied and no write out, starts one, and the compaction's write, answered,
    /// wakes that shard's task, which shows the range with it made and none out.
    #[test]
    fn compaction_runs_on_a_far_ranges_shard() {
        let rt = started(2, 2);
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let (mut lanes, _, _) = leading(&rt, &log, &range);
        lanes
            .send(In::Propose(GROUP, put(), range.settings.max_entry_bytes))
            .unwrap();
        let Out::Applied(_, answer) = next(&mut lanes, "its entry applied", deadline(), |out| {
            !matches!(out, Out::Applied(..))
        }) else {
            panic!("`next` gives only what it does not skip")
        };
        // Asked once the range shows its entry applied and no write out, and again each time it
        // shows none out after a write passed it over.
        let until = deadline();
        loop {
            match next(&mut lanes, "a compaction started", until, |_| false) {
                Out::Compacted => break,
                Out::Seen(_, seen) if seen.applied >= answer.index && seen.in_flight == 0 => {
                    lanes.send(In::Compact).unwrap();
                }
                _ => {}
            }
        }
        let until = deadline();
        loop {
            if let Out::Seen(_, seen) = next(&mut lanes, "the compaction made", until, |out| {
                !matches!(out, Out::Seen(..))
            }) && seen.writes.starts > 0
                && seen.in_flight == 0
            {
                break;
            }
        }
        drop(lanes);
        rt.shutdown().unwrap();
        drop(log);
    }

    /// A range of voter 1 and learner 2, with each member's replica moved to a shard of its own
    /// before its first drive: the leader's log compacted past the learner's next entry, so the
    /// learner is sent a snapshot, out of the leader's lane, which it installs from its own, the
    /// snapshot's end reported into the leader's; then the learner is sent entries again.
    #[test]
    fn a_far_ranges_snapshot_is_sent_and_installed_through_the_lanes() {
        use hyper_raft::proto::MessageType;

        let rt = started(2, 2);
        let (dir, learning_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let mut range = one_member();
        range.boot.learners = vec![2];
        let (log, learning_log) = (
            log_in(dir.path(), &range),
            log_in(learning_dir.path(), &range),
        );
        // The leader's messages to the learner, held until its log is compacted past them.
        let (mut leader, _, mut held) = leading(&rt, &log, &range);
        let mut take_held = |out: &Out| {
            if let Out::Message(_, message) = out {
                held.push(message.clone());
            }
            !matches!(out, Out::Applied(..))
        };
        leader
            .send(In::Propose(GROUP, put(), range.settings.max_entry_bytes))
            .unwrap();
        let Out::Applied(_, answer) =
            next(&mut leader, "its entry applied", deadline(), &mut take_held)
        else {
            panic!("`next` gives only what it does not skip")
        };
        let until = deadline();
        loop {
            match next(&mut leader, "a compaction started", until, |_| false) {
                Out::Compacted => break,
                Out::Message(_, message) => held.push(message),
                Out::Seen(_, seen) if seen.applied >= answer.index && seen.in_flight == 0 => {
                    leader.send(In::Compact).unwrap();
                }
                _ => {}
            }
        }
        // A compaction started is not one made: the log starts past what it compacted only once
        // the compaction's write is durable (hyper-durable's `Replica::durable`), and until then
        // the leader holds the learner's next entry and sends it in an append, which needs no
        // snapshot. The learner starts once the leader shows the write made and none out.
        let until = deadline();
        loop {
            assert!(
                std::time::Instant::now() < until,
                "waited for the leader's compaction made"
            );
            match leader.take().unwrap() {
                Some(Out::Message(_, message)) => held.push(message),
                Some(Out::Seen(_, seen)) if seen.writes.starts > 0 && seen.in_flight == 0 => break,
                Some(Out::Stopped(error)) => panic!("the leader's shard stopped: {error}"),
                Some(_) => {}
                None => std::thread::yield_now(),
            }
        }
        let learning = {
            let mut engine = Model::default();
            engine.install(0, name::first(1).unwrap()).unwrap();
            engine.persist().unwrap();
            let store = Answers::new(mantle_range::claim(&learning_log, GROUP, &range).unwrap());
            mantle_range::open_shell(2, store, engine, &range, 2, Elections::Suspicion).unwrap()
        };
        let holder = rt.holder_of(rt.shard_ids()[0]).unwrap();
        let mut learner = Lanes::start(holder, 0, 1, 8).unwrap();
        learner.send(In::Adopt(GROUP, Box::new(learning))).unwrap();
        for message in held.drain(..).filter(|message| message.to == 2) {
            learner
                .send(In::Step {
                    group: GROUP,
                    sender: 1,
                    message,
                })
                .unwrap();
        }
        // Each member's messages stepped by the other, the test's thread their network.
        let (mut sent, mut installed, mut appended) = (false, false, false);
        let until = deadline();
        while !(sent && installed && appended) {
            assert!(
                std::time::Instant::now() < until,
                "waited for the snapshot sent ({sent}), the learner past it ({installed}) and an \
                 append after it ({appended})"
            );
            if let Some(out) = leader.take().unwrap() {
                match out {
                    Out::Message(_, message) if message.to == 2 => {
                        if message.msg_type == MessageType::MsgSnapshot {
                            sent = true;
                            leader
                                .send(In::Reported {
                                    group: GROUP,
                                    to: 2,
                                    arrived: true,
                                })
                                .unwrap();
                        } else if sent && message.msg_type == MessageType::MsgAppend {
                            appended = true;
                        }
                        learner
                            .send(In::Step {
                                group: GROUP,
                                sender: 1,
                                message,
                            })
                            .unwrap();
                    }
                    Out::Stopped(error) => panic!("the leader's shard stopped: {error}"),
                    _ => {}
                }
            }
            if let Some(out) = learner.take().unwrap() {
                match out {
                    Out::Message(_, message) if message.to == 1 => leader
                        .send(In::Step {
                            group: GROUP,
                            sender: 2,
                            message,
                        })
                        .unwrap(),
                    Out::Seen(_, seen) if seen.applied >= answer.index => installed = true,
                    Out::Stopped(error) => panic!("the learner's shard stopped: {error}"),
                    _ => {}
                }
            }
            std::thread::yield_now();
        }
        assert!(sent && installed);
        drop((leader, learner));
        rt.shutdown().unwrap();
        drop((log, learning_log));
    }

    /// Node 1's view of a far range on shard 1 as it showed `seen`.
    fn far(seen: Seen) -> Far {
        Far {
            group: GROUP,
            shard: 1,
            peers: Vec::new(),
            election: None,
            seen,
            reports: Vec::new(),
            configured: Configured {
                conf: ConfState::default(),
                pending: false,
                learners: Vec::new(),
            },
            replacing: None,
        }
    }

    /// The node's own read of a far range it leads is asked of the range's shard and served back
    /// by its id (`Node::ask_read_out`), as a peer's is; one of a range shown not leading is not
    /// asked, and its asker asks again.
    #[test]
    fn the_nodes_own_read_of_a_far_range_it_leads_is_served_by_its_shard() {
        let rt = started(2, 2);
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let (lanes, moved, _) = leading(&rt, &log, &range);
        let mut shards = vec![lanes];
        let mut far = far(moved);
        let query = Query::Current {
            bucket: "b".into(),
            key: "k".into(),
        };
        assert!(!far.seen.leading);
        assert!(!ask_read(&shards, &far, 9, &query).unwrap());
        far.seen.leading = true;
        assert!(ask_read(&shards, &far, 9, &query).unwrap());
        let Out::Served(group, id, served) =
            next(&mut shards[0], "the read served", deadline(), |out| {
                !matches!(out, Out::Served(..))
            })
        else {
            panic!("`next` gives only what it does not skip")
        };
        assert_eq!((group, id), (GROUP, 9));
        assert!(matches!(served, Served::Read(read) if *read == Read::NoBucket));
        drop(shards);
        rt.shutdown().unwrap();
        drop(log);
    }

    /// A read asked of a far range's shard whose lane holds all it takes is not asked, which its
    /// asker takes as its replica's refusal, asked again after a pause; one of a shard the node has
    /// no lanes to stops the node, typed.
    #[test]
    fn a_full_lane_refuses_a_read_as_its_replica_would() {
        let (lanes, _ends) = unstarted(1);
        lanes.send(measured()).unwrap();
        let shards = vec![lanes];
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let mut far = far(Seen::of(&campaigning(&log, &range)));
        far.seen.leading = true;
        let query = Query::Current {
            bucket: "b".into(),
            key: "k".into(),
        };
        assert!(!ask_read(&shards, &far, 1, &query).unwrap());
        far.shard = 2;
        assert!(matches!(
            ask_read(&shards, &far, 2, &query),
            Err(NodeError::ShardEnded(2))
        ));
    }

    /// The stop's hand-over asked of a far range's shard whose lane holds all it takes is not
    /// asked, which the stop takes as one the range's core refused: the range elects once this
    /// node is gone; one of a shard the node has no lanes to stops the node, typed.
    #[test]
    fn a_full_lane_refuses_a_hand_over_as_its_core_would() {
        let (lanes, _ends) = unstarted(1);
        lanes.send(measured()).unwrap();
        let shards = vec![lanes];
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let mut far = far(Seen::of(&campaigning(&log, &range)));
        assert!(!ask_transfer(&shards, &far, None).unwrap());
        far.shard = 2;
        assert!(matches!(
            ask_transfer(&shards, &far, None),
            Err(NodeError::ShardEnded(2))
        ));
    }

    /// A range of voters 1 and 2, each member's replica moved to a shard of its own before its
    /// first drive: the stop's hand-over of its lead, asked of the leader's shard once it leads,
    /// goes to voter 2, as that shard tells, and voter 2 then leads, as the leader's shard shows.
    /// One of a range the shard holds not begins none.
    #[test]
    fn a_far_ranges_lead_is_handed_over_by_its_shard() {
        let rt = started(2, 2);
        let (dir, voting_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let mut range = one_member();
        range.boot.voters = vec![1, 2];
        let (log, voting_log) = (
            log_in(dir.path(), &range),
            log_in(voting_dir.path(), &range),
        );
        let voting = {
            let mut engine = Model::default();
            engine.install(0, name::first(1).unwrap()).unwrap();
            engine.persist().unwrap();
            let store = Answers::new(mantle_range::claim(&voting_log, GROUP, &range).unwrap());
            mantle_range::open_shell(2, store, engine, &range, 2, Elections::Suspicion).unwrap()
        };
        let mut leader = Lanes::start(rt.holder_of(rt.shard_ids()[1]).unwrap(), 1, 1, 8).unwrap();
        let mut voter = Lanes::start(rt.holder_of(rt.shard_ids()[0]).unwrap(), 0, 1, 8).unwrap();
        leader
            .send(In::Adopt(GROUP, Box::new(campaigning(&log, &range))))
            .unwrap();
        voter.send(In::Adopt(GROUP, Box::new(voting))).unwrap();
        // Each member's messages stepped by the other, the test's thread their network, which
        // drops one a full lane refuses, as a network may.
        let (mut asked, mut handing, mut handed) = (false, None, false);
        let until = deadline();
        while !handed {
            assert!(
                std::time::Instant::now() < until,
                "waited for voter 1 to lead ({asked}), its hand-over told ({handing:?}) and voter \
                 2 shown leading ({handed})"
            );
            if let Some(out) = leader.take().unwrap() {
                match out {
                    Out::Message(_, message) => {
                        let _ = voter.send(In::Step {
                            group: GROUP,
                            sender: 1,
                            message,
                        });
                    }
                    Out::Seen(_, seen) if seen.leading && !asked => {
                        leader
                            .send(In::Transfer {
                                group: GROUP,
                                failed: None,
                            })
                            .unwrap();
                        asked = true;
                    }
                    Out::Seen(_, seen) if handing.is_some() && seen.leader == 2 => handed = true,
                    Out::Handing(group, to) => {
                        assert_eq!((group, to), (GROUP, Some(2)));
                        handing = to;
                    }
                    Out::Stopped(error) => panic!("the leader's shard stopped: {error}"),
                    _ => {}
                }
            }
            if let Some(out) = voter.take().unwrap() {
                match out {
                    Out::Message(_, message) => {
                        let _ = leader.send(In::Step {
                            group: GROUP,
                            sender: 2,
                            message,
                        });
                    }
                    Out::Stopped(error) => panic!("the voter's shard stopped: {error}"),
                    _ => {}
                }
            }
            std::thread::yield_now();
        }
        leader
            .send(In::Transfer {
                group: GROUP + 1,
                failed: None,
            })
            .unwrap();
        let Out::Handing(group, to) = next(
            &mut leader,
            "a hand-over of a range not held",
            deadline(),
            |out| !matches!(out, Out::Handing(..)),
        ) else {
            panic!("`next` gives only what it does not skip")
        };
        assert_eq!((group, to), (GROUP + 1, None));
        drop((leader, voter));
        rt.shutdown().unwrap();
        drop((log, voting_log));
    }

    /// What a shard's close gave back: its ranges' engines, or why not.
    type Engines = Result<Vec<(u128, Model)>, NodeError>;

    /// What `lanes.release` gives, run as a task on shard 0, as the node's close runs it, and
    /// the lanes after it.
    fn released(
        rt: &Runtime,
        mut lanes: Lanes<Model, Stored>,
        patience_ns: u64,
    ) -> (Engines, Lanes<Model, Stored>) {
        let (given, taken) = std::sync::mpsc::sync_channel(1);
        hyper_rt::runtime::submit_to_holder(rt.holder_of(rt.shard_ids()[0]).unwrap(), async move {
            let engines = lanes.release(patience_ns).await;
            let _ = given.send((engines, lanes));
        })
        .unwrap();
        taken.recv_timeout(PATIENCE).unwrap()
    }

    /// The node's close takes a far range's engine back from its shard, whose task then ends; a
    /// shard whose task ended is refused as ended, and one that gives nothing back within the
    /// patience as silent, typed.
    #[test]
    fn a_far_ranges_engine_is_given_back_at_close_and_its_task_ends() {
        let rt = started(2, 2);
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let (lanes, _, _) = leading(&rt, &log, &range);
        let patience_ns = u64::try_from(PATIENCE.as_nanos()).unwrap();
        let (engines, mut lanes) = released(&rt, lanes, patience_ns);
        assert_eq!(
            engines
                .unwrap()
                .iter()
                .map(|(group, _)| *group)
                .collect::<Vec<_>>(),
            vec![GROUP]
        );
        // The shard's task ended: once what it gave is taken, its lane says so.
        let until = deadline();
        loop {
            match lanes.take() {
                Err(NodeError::ShardEnded(1)) => break,
                Err(error) => panic!("the lane ended otherwise: {error}"),
                Ok(_) => {
                    assert!(
                        std::time::Instant::now() < until,
                        "waited for the shard's task to end"
                    );
                    std::thread::yield_now();
                }
            }
        }

        let (lanes, ends) = unstarted(1);
        assert!(matches!(
            released(&rt, lanes, 1_000_000).0,
            Err(NodeError::ShardSilent(1))
        ));
        drop(ends);
        let (lanes, (taken, _given)) = unstarted(1);
        drop(taken);
        assert!(matches!(
            released(&rt, lanes, patience_ns).0,
            Err(NodeError::ShardEnded(1))
        ));
        rt.shutdown().unwrap();
        drop(log);
    }

    /// A replica with a write out is refused a move, typed: the write's answer would wake the
    /// task that drove it.
    #[test]
    fn a_replica_with_writes_out_is_refused_a_move() {
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let mut replica = campaigning(&log, &range);
        let mut out = Output::default();
        replica
            .drive(0, std::task::Waker::noop(), &mut out)
            .unwrap();
        assert!(replica.in_flight() > 0);
        assert!(matches!(
            movable(GROUP, &replica),
            Err(NodeError::WritesOut(GROUP))
        ));
    }

    /// A lane that holds all it takes refuses the next, typed, and one whose shard's end is gone
    /// refuses as the shard's end.
    #[test]
    fn a_full_lane_refuses_typed() {
        let (lanes, (taken, _given)) = unstarted(1);
        lanes.send(measured()).unwrap();
        assert!(matches!(
            lanes.send(measured()),
            Err(NodeError::LaneFull(1))
        ));
        drop(taken);
        assert!(matches!(
            lanes.send(measured()),
            Err(NodeError::ShardEnded(1))
        ));
    }

    /// A shard whose task ended stops the node, typed, once what it gave is taken: its lane's end
    /// gone, and a shard's task gone with its runtime.
    #[test]
    fn a_closed_lane_stops_the_node_typed() {
        let (mut lanes, (_taken, given)) = unstarted(2);
        assert!(given.try_send(Out::Flushed(1, 2)).is_ok());
        drop(given);
        assert!(matches!(lanes.take(), Ok(Some(Out::Flushed(1, 2)))));
        assert!(matches!(lanes.take(), Err(NodeError::ShardEnded(1))));

        let rt = started(2, 2);
        let holder = rt.holder_of(rt.shard_ids()[1]).unwrap();
        let mut lanes = Lanes::<Model, Stored>::start(holder, 1, 1, 8).unwrap();
        rt.shutdown().unwrap();
        assert!(matches!(lanes.take(), Err(NodeError::ShardEnded(1))));
        assert!(matches!(
            lanes.send(measured()),
            Err(NodeError::ShardEnded(1))
        ));
    }

    /// A peer's command and read of a far range this node leads, as its shard last showed it, are
    /// read as the range's own (`Node::take_request`): the command proposed over the shard's lane
    /// and its answer given to the command waiting for it, the read asked there and served back by
    /// its id. One of the range shown not leading is redirected to its leader, and one of a range
    /// the node keeps no member of is refused `NoRange`.
    #[test]
    fn a_peers_command_and_read_of_a_far_range_are_served_through_its_lanes() {
        use super::super::{Arriving, CommandOrigin, Commanded, answer_command, requested};
        let rt = started(2, 2);
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let (lanes, moved, _) = leading(&rt, &log, &range);
        let mut shards = vec![lanes];
        let mut far = far(moved);
        assert!(!far.seen.leading);
        assert!(requested(Some(far.seen), GROUP, Arriving::Command) == Arriving::Redirect(GROUP));
        assert!(
            requested(None, GROUP, Arriving::Read)
                == Arriving::Status(crate::command::Status::NoRange)
        );
        // As its shard showed it once it led (`leading`).
        far.seen.leading = true;

        assert!(requested(Some(far.seen), GROUP, Arriving::Command) == Arriving::Command(GROUP));
        propose(&shards, &far, &put(), range.settings.max_entry_bytes).unwrap();
        let mut commanded = vec![Commanded {
            origin: CommandOrigin::Own(0),
            group: GROUP,
            commands: vec![(0, 1, None)],
        }];
        let Out::Applied(group, applied) =
            next(&mut shards[0], "the command applied", deadline(), |out| {
                !matches!(out, Out::Applied(..))
            })
        else {
            panic!("`next` gives only what it does not skip")
        };
        answer_command(&mut commanded, group, &applied);
        assert!(commanded[0].commands[0].2.is_some(), "the command answered");

        assert!(requested(Some(far.seen), GROUP, Arriving::Read) == Arriving::Read(GROUP));
        let query = Query::Current {
            bucket: "b".into(),
            key: "k".into(),
        };
        assert!(ask_read(&shards, &far, 5, &query).unwrap());
        let Out::Served(group, id, served) =
            next(&mut shards[0], "the read served", deadline(), |out| {
                !matches!(out, Out::Served(..))
            })
        else {
            panic!("`next` gives only what it does not skip")
        };
        assert_eq!((group, id), (GROUP, 5));
        assert!(matches!(served, Served::Read(_)));
        drop(shards);
        rt.shutdown().unwrap();
        drop(log);
    }

    /// A proposal to a far range's shard whose lane holds all it takes is refused `LaneFull`, as
    /// one its replica refuses, which a peer's command hears as `Refused` and asks again after its
    /// pause; one to a shard the node has no lanes to stops the node, typed.
    #[test]
    fn a_full_lane_refuses_a_proposal_as_its_replica_would() {
        let (lanes, _ends) = unstarted(1);
        lanes.send(measured()).unwrap();
        let shards = vec![lanes];
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let mut far = far(Seen::of(&campaigning(&log, &range)));
        far.seen.leading = true;
        let max = range.settings.max_entry_bytes;
        assert!(matches!(
            propose(&shards, &far, &put(), max),
            Err(NodeError::LaneFull(1))
        ));
        far.shard = 2;
        assert!(matches!(
            propose(&shards, &far, &put(), max),
            Err(NodeError::ShardEnded(2))
        ));
    }

    /// A peer whose connection went is told to a far range's shard ([`In::Unreachable`]) once its
    /// lane has room, held while the lane holds all it takes, each peer once however often it was
    /// lost meanwhile; one held for a shard whose task ended stops the node, typed.
    #[test]
    fn a_lost_peer_is_told_to_the_shard_once_its_lane_has_room() {
        let (mut lanes, (mut taken, given)) = unstarted(1);
        lanes.send(measured()).unwrap();
        lanes.lost(2);
        lanes.lost(3);
        lanes.lost(2);
        lanes.tell_lost().unwrap();
        assert_eq!(lanes.lost, vec![2, 3], "held while the lane is full");
        assert!(matches!(taken.try_recv(), Ok(Some(In::Measure(_)))));
        lanes.tell_lost().unwrap();
        assert_eq!(lanes.lost, vec![3]);
        assert!(matches!(taken.try_recv(), Ok(Some(In::Unreachable(2)))));
        lanes.tell_lost().unwrap();
        assert!(lanes.lost.is_empty());
        assert!(matches!(taken.try_recv(), Ok(Some(In::Unreachable(3)))));
        assert!(matches!(taken.try_recv(), Ok(None)), "each peer told once");
        lanes.lost(4);
        drop((taken, given));
        assert!(matches!(lanes.tell_lost(), Err(NodeError::ShardEnded(1))));
    }

    /// A far range's shard told a peer's connection went goes on driving its ranges: one that
    /// shares no group with the peer is untouched, and answers its next proposal.
    #[test]
    fn a_far_ranges_shard_takes_a_lost_peer_and_goes_on() {
        let rt = started(2, 2);
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let (mut lanes, _, _) = leading(&rt, &log, &range);
        lanes.send(In::Unreachable(2)).unwrap();
        lanes
            .send(In::Propose(GROUP, put(), range.settings.max_entry_bytes))
            .unwrap();
        let Out::Applied(group, _) = next(&mut lanes, "its entry applied", deadline(), |out| {
            !matches!(out, Out::Applied(..))
        }) else {
            panic!("`next` gives only what it does not skip")
        };
        assert_eq!(group, GROUP);
        drop(lanes);
        rt.shutdown().unwrap();
        drop(log);
    }

    /// No insert into the node's shard's owner gives a handle a range that left it holds. A slot
    /// filled again 2^32 times would give one; here a second arena of one slot gives the handles
    /// the node's owner's slot gives in turn, the first two taken as held by ranges that left: the
    /// insert passes both for the slot's third. A replica that leaves holds its handle, which then
    /// names nothing, and the next insert gives another.
    #[test]
    fn no_insert_into_the_nodes_owner_gives_a_handle_a_far_range_holds() {
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let replica = campaigning(&log, &range);
        let waker = Waker::noop();
        let mut other = Owner::new(vec![waker.clone()]);
        let mut handles = Vec::new();
        let mut replica = Some(replica);
        for _ in 0..3 {
            let Ok(handle) = other.insert(replica.take().unwrap()) else {
                panic!("the second arena's one slot is free");
            };
            replica = other.remove(handle);
            handles.push(handle);
        }
        let replica = replica.unwrap();
        assert!(handles[0] != handles[1] && handles[1] != handles[2]);

        let mut here = Here::new(Owner::new(vec![waker.clone()]));
        here.left = vec![handles[0], handles[1]];
        let handle = here.insert(replica).unwrap();
        assert_eq!(handle, handles[2], "the slot's first handle none left with");
        assert!(here.get(handle).is_some());

        let replica = here.leave(handle).unwrap();
        assert!(here.left.contains(&handle));
        assert!(
            here.get(handle).is_none(),
            "a handle that left names nothing"
        );
        let again = here.insert(replica).unwrap();
        assert!(!here.left.contains(&again));
        assert!(here.get(again).is_some());
    }

    /// A far range's configuration reaches the node's shard after the drive that changed it: a
    /// learner the cell's target adds through the range's lane ([`In::Toward`]) is shown in the
    /// configuration it applied, with its progress as its leader holds it.
    #[test]
    fn a_far_ranges_configuration_change_is_seen() {
        let rt = started(2, 2);
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let (mut lanes, _, _) = leading(&rt, &log, &range);
        let target = Toward {
            voters: vec![1],
            learners: vec![2],
        };
        lanes.send(In::Toward(GROUP, target)).unwrap();
        let until = deadline();
        let shown = loop {
            if let Out::Configured(group, shown) =
                next(&mut lanes, "the learner shown added", until, |out| {
                    !matches!(out, Out::Configured(..))
                })
                && shown.conf.learners == [2]
            {
                assert_eq!(group, GROUP);
                break shown;
            }
        };
        assert_eq!(shown.conf.voters, [1]);
        assert!(!shown.pending, "the change it applied is pending no more");
        let progress: Vec<(u64, bool)> = shown
            .learners
            .iter()
            .map(|(id, _, caught_up)| (*id, *caught_up))
            .collect();
        assert_eq!(progress, [(2, false)], "a learner never heard from");
        drop(lanes);
        rt.shutdown().unwrap();
        drop(log);
    }

    /// Node 1's member of `range` on the second shard of `rt`, campaigning, and node 2's on the
    /// first, each on a log of its own: the lanes of each.
    fn two(
        rt: &Runtime,
        log: &Log<DeviceFile>,
        other_log: &Log<DeviceFile>,
        range: &Range,
    ) -> (Lanes<Model, Stored>, Lanes<Model, Stored>) {
        let other = {
            let mut engine = Model::default();
            engine.install(0, name::first(1).unwrap()).unwrap();
            engine.persist().unwrap();
            let store = Answers::new(mantle_range::claim(other_log, GROUP, range).unwrap());
            mantle_range::open_shell(2, store, engine, range, 2, Elections::Suspicion).unwrap()
        };
        let leader = Lanes::start(rt.holder_of(rt.shard_ids()[1]).unwrap(), 1, 1, 8).unwrap();
        let member = Lanes::start(rt.holder_of(rt.shard_ids()[0]).unwrap(), 0, 1, 8).unwrap();
        leader
            .send(In::Adopt(GROUP, Box::new(campaigning(log, range))))
            .unwrap();
        member.send(In::Adopt(GROUP, Box::new(other))).unwrap();
        (leader, member)
    }

    /// Carries each member's messages to the other, the test's thread their network, which drops
    /// one a full lane refuses, as a network may, and hands `seen` each other thing node 1's
    /// member gives, with its lanes, until it says what the test waits for, `what`, came.
    fn carried(
        leader: &mut Lanes<Model, Stored>,
        member: &mut Lanes<Model, Stored>,
        what: &'static str,
        mut seen: impl FnMut(Out, &Lanes<Model, Stored>) -> bool,
    ) {
        let until = deadline();
        loop {
            assert!(
                std::time::Instant::now() < until,
                "waited {PATIENCE:?} for {what}"
            );
            if let Some(out) = leader.take().unwrap() {
                match out {
                    Out::Message(_, message) => {
                        let _ = member.send(In::Step {
                            group: GROUP,
                            sender: 1,
                            message,
                        });
                    }
                    Out::Stopped(error) => panic!("node 1's shard stopped: {error}"),
                    out => {
                        if seen(out, leader) {
                            return;
                        }
                    }
                }
            }
            if let Some(out) = member.take().unwrap() {
                match out {
                    Out::Message(_, message) => {
                        let _ = leader.send(In::Step {
                            group: GROUP,
                            sender: 2,
                            message,
                        });
                    }
                    Out::Stopped(error) => panic!("node 2's shard stopped: {error}"),
                    _ => {}
                }
            }
            std::thread::yield_now();
        }
    }

    /// A voter is added to a far range and removed from it through its lane, each target worked
    /// out on the node's shard and handed over ([`In::Toward`]), and stepped on the range's from
    /// what its replica shows: node 2 learns, is made a voter once caught up, then is removed,
    /// each change seen in the configuration the range shows. The node works its target out at
    /// each turn; the test hands it again at each change node 1's member shows.
    #[test]
    fn a_voter_is_added_and_removed_through_a_far_ranges_lane() {
        let rt = started(2, 2);
        let (dir, other_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let mut range = one_member();
        range.boot.learners = vec![2];
        let (log, other_log) = (log_in(dir.path(), &range), log_in(other_dir.path(), &range));
        let (mut leader, mut member) = two(&rt, &log, &other_log, &range);
        let toward = |voters: &[u64]| {
            In::Toward(
                GROUP,
                Toward {
                    voters: voters.to_vec(),
                    learners: Vec::new(),
                },
            )
        };
        for (voters, what) in [
            (&[1, 2][..], "node 2 made a voter"),
            (&[1][..], "node 2 removed"),
        ] {
            let _ = leader.send(toward(voters));
            carried(&mut leader, &mut member, what, |out, leader| match out {
                Out::Configured(_, shown) if shown.conf.voters == voters => {
                    assert!(shown.conf.learners.is_empty());
                    true
                }
                Out::Configured(..) | Out::Seen(..) => {
                    let _ = leader.send(toward(voters));
                    false
                }
                _ => false,
            });
        }
        drop((leader, member));
        rt.shutdown().unwrap();
        drop((log, other_log));
    }

    /// A far range's replacement ([`In::Replace`]) is kept by its shard and stepped at each of its
    /// turns until it is done: node 2, under an identity the range never named, learns, catches up
    /// and is made a voter in place of node 3, which the range names no more, and once every
    /// voter knows it the shard hands the replacement back done ([`Out::Replaced`]). It was told
    /// before the range led, as every node may be told one.
    #[test]
    fn a_far_ranges_replacement_progresses_to_done() {
        let rt = started(2, 2);
        let (dir, other_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let range = one_member();
        let (log, other_log) = (log_in(dir.path(), &range), log_in(other_dir.path(), &range));
        let (mut leader, mut member) = two(&rt, &log, &other_log, &range);
        let replacement = Replacement::new(3, 2).unwrap();
        leader.send(In::Replace(GROUP, replacement)).unwrap();
        let (mut voters, mut proposed) = (Vec::new(), false);
        carried(
            &mut leader,
            &mut member,
            "the replacement done",
            |out, leader| {
                match out {
                    Out::Configured(_, shown) => {
                        // Node 2 says it holds the change once a message carries its commit to it:
                        // the next entry's append here, as the leader's next heartbeat would.
                        if shown.conf.voters == [1, 2] && !proposed {
                            let max = range.settings.max_entry_bytes;
                            leader.send(In::Propose(GROUP, put(), max)).unwrap();
                            proposed = true;
                        }
                        voters = shown.conf.voters;
                    }
                    Out::Replaced(group, done) => {
                        assert_eq!((group, done), (GROUP, replacement));
                        return true;
                    }
                    _ => {}
                }
                false
            },
        );
        assert_eq!(voters, [1, 2], "node 2 votes in its place");
        drop((leader, member));
        rt.shutdown().unwrap();
        drop((log, other_log));
    }
}
