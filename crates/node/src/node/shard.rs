//! A shard's ranges, driven by a task on that shard (node.md §2.9, step 2).
//!
//! The node's shard, the first, keeps the endpoint, the plane, the liveness stream and the table of
//! ranges by shard. A shard past it runs a task of its own that holds the replicas the table places
//! there in an owner of its own (`hyper_durable::Owner`), made with that task's waker, so a range's
//! log answers wake the shard that drives it, and keeps that shard's deadlines. The two talk over
//! two bounded lanes of hyper-rt's channel: into the shard, each replica moved there before its
//! first drive, Raft messages, proposals, reads, the stream's changes and measurements, the
//! membership's targets and replacements, and the asks for the root range's volume table and for
//! the Block range's rows repair walks by ([`In`]); out of it, Raft messages, applied answers,
//! flushes, each range's peers, its expected election, what it shows after a drive that changed it
//! ([`Seen`], [`Configured`]), the root range's rows, its reads served, its proposals refused, its
//! replacements done, and the volume table and Block rows asked ([`Out`]). The node's shard sends without waiting, a full lane refused, typed
//! (`NodeError::LaneFull`); the shard waits for room on its way out, its replicas' next turn held
//! until the node's shard takes what they gave. A shard whose task ended took its ranges with it,
//! which nothing else holds, so the node stops (`NodeError::ShardEnded`) and its next start
//! recovers them from the log (node.md §1.7).

use std::cell::Cell;
use std::collections::{BTreeSet, VecDeque};
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
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
use mantle_meta::block;
use mantle_meta::engine::Engine;
use mantle_meta::record::{BlockHeader, ChunkPlace};
use mantle_meta::root::{self, Fact};
use mantle_protocol::bounds;
use mantle_protocol::classes::{self, NodeClasses};
use mantle_range::membership::{Replacement, Step, Toward};
use mantle_range::{Applied, ConfState, LogStore, RangeMachine};

use super::answers::Answers;
use super::members::Rows;
use super::{NodeError, Woken};
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
    /// An entry's encoding proposed to a range, as it came, its commands' CRCs with it
    /// (replica.md §2), the most bytes the range takes in one, and its ask: one the range refuses
    /// is answered at once by its ask ([`Out::Refused`]), as the node's own replica's refusal is
    /// (node.md §2.9, step 4).
    Propose(u128, Vec<u8>, u64, u64),
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
    /// The root range's volume table, read from its engine as it applied it when the shard takes
    /// this ([`Out::Table`]): asked by the node once the range applied past the table it holds,
    /// one at a time ([`ask_table`]).
    Table(u128),
    /// A read of the Block range's rows that repair walks by, answered from its engine when the
    /// shard takes it ([`Out::Block`]): one at a time, as repair walks one block at a time
    /// ([`read_block`]).
    Block(u128, BlockRead),
    /// Each range's engine as it stands, handed back through the sender, and the task ends: the
    /// node closes (`Node::close`).
    Release(OneshotSender<Vec<(u128, E)>>),
}

/// Each kind of what the node's shard hands a range's shard ([`In`]), as the `counts` answer names
/// it (`Node::lane_counts`).
pub(crate) const KINDS: &[&str] = &[
    "adopt",
    "step",
    "propose",
    "believe",
    "belief",
    "measure",
    "read",
    "compact",
    "reported",
    "unreachable",
    "transfer",
    "toward",
    "replace",
    "table",
    "block",
    "release",
];

impl<E: Engine, S: LogStore> In<E, S> {
    /// Its kind's name, one of [`KINDS`]: a variant added fails to compile until it has one.
    fn kind(&self) -> &'static str {
        match self {
            Self::Adopt(..) => "adopt",
            Self::Step { .. } => "step",
            Self::Propose(..) => "propose",
            Self::Believe(_) => "believe",
            Self::Belief(..) => "belief",
            Self::Measure(_) => "measure",
            Self::Read { .. } => "read",
            Self::Compact => "compact",
            Self::Reported { .. } => "reported",
            Self::Unreachable(_) => "unreachable",
            Self::Transfer { .. } => "transfer",
            Self::Toward(..) => "toward",
            Self::Replace(..) => "replace",
            Self::Table(_) => "table",
            Self::Block(..) => "block",
            Self::Release(_) => "release",
        }
    }
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
    /// A proposal a range refused ([`In::Propose`]), by its ask, and why: its asker answered at
    /// once rather than at its patience's end.
    Refused(u128, u64, Refusal),
    /// A round of compaction ([`In::Compact`]) that started at least one.
    Compacted,
    /// The voter a range's hand-over ([`In::Transfer`]) goes to; `None` where it began none: the
    /// range leads not here, or its core refused.
    Handing(u128, Option<u64>),
    /// A range's configuration and its learners' progress after a drive that changed them.
    Configured(u128, Configured),
    /// A replacement ([`In::Replace`]) that ended while the range led here.
    Replaced(u128, Replacement),
    /// The root range's rows the membership reads, after a drive in which it applied an entry, and
    /// at its first.
    Rows(u128, Box<Rows>),
    /// The root range's volume table an [`In::Table`] asked, or why its engine failed to read it.
    Table(u128, Result<Box<Facts>, NodeError>),
    /// What an [`In::Block`] read of the Block range's rows found, or why its engine failed it.
    Block(u128, BlockRead, Result<BlockRows, NodeError>),
    /// Why the shard stopped: its task ends after it.
    Stopped(NodeError),
}

/// Why a range refused a proposal ([`Out::Refused`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The entry is past the most bytes the range takes in one (`ReplicaError::EntryTooLarge`):
    /// refused for what it is, never taken.
    TooLarge,
    /// The range takes no entry now: it leads not, its leader is handing its lead over, its
    /// uncommitted entries are at their bound, or the shard holds it not; asked again after a
    /// pause, or of the leader there is now.
    NotTaken,
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
    /// The root range's rows its shard last handed over ([`Out::Rows`]) that the node has yet to
    /// read: the latest, superseding those before.
    pub(crate) rows: Option<Box<Rows>>,
    /// Whether the root range's volume table is asked of its shard ([`In::Table`]), and the one it
    /// answered ([`Out::Table`]) that the node has yet to read.
    pub(crate) table_out: bool,
    pub(crate) table: Option<Result<Box<Facts>, NodeError>>,
    /// The read of the Block range's rows repair asked of its shard ([`In::Block`]), and its
    /// answer once given ([`Out::Block`]), until repair takes it or its turn ends.
    pub(crate) block: Option<(BlockRead, Option<Result<BlockRows, NodeError>>)>,
}

impl Far {
    /// The volume table its shard answered ([`Out::Table`]), for the node's next read: its ask is
    /// out no more.
    pub(crate) fn answered_table(&mut self, facts: Result<Box<Facts>, NodeError>) {
        self.table_out = false;
        self.table = Some(facts);
    }

    /// What its shard found for repair's `read` ([`Out::Block`]), kept while it is the read out.
    pub(crate) fn answered_block(&mut self, read: BlockRead, rows: Result<BlockRows, NodeError>) {
        if self.block.as_ref().is_some_and(|(asked, _)| *asked == read) {
            self.block = Some((read, Some(rows)));
        }
    }
}

/// The root range's volume table as its member applied it: each volume, its standing and its
/// node's presence (`root::facts`), at most the table's census of them, and the entry it had
/// applied when read.
pub(crate) struct Facts {
    pub(crate) applied: u64,
    pub(crate) facts: Vec<Fact>,
}

/// Every fact of the volume table in `engine`, the root range's: at most its census of volumes.
pub(crate) fn read_facts<E: Engine>(engine: &mut E) -> Result<Vec<Fact>, NodeError> {
    let meta = |e: mantle_meta::error::MetaError| NodeError::Bounds(e.to_string());
    let census = root::census(engine).map_err(meta)?;
    let most = usize::try_from(census.volumes).unwrap_or(usize::MAX);
    root::facts(engine, None, most).map_err(meta)
}

/// A read of the Block range's rows that repair walks by (`node::repair`), each of one block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BlockRead {
    /// The first block past `after` (`block::blocks`).
    Next { after: Option<u128> },
    /// The first block past `after` with a place on `volume`, and its chunk's index
    /// (`block::on_volume`).
    OnVolume { volume: u128, after: Option<u128> },
    /// A block's header and every place of its chunks (`block::places`).
    Places { block: u128 },
}

/// What a [`BlockRead`] found: at most one block, or one block's places.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BlockRows {
    Next(Option<u128>),
    OnVolume(Option<(u128, u16)>),
    Places(Option<(BlockHeader, Vec<ChunkPlace>)>),
}

impl BlockRead {
    /// What it finds in `engine`, the Block range's, as its member applied it: read alike on the
    /// node's shard and on the range's.
    pub(crate) fn read<E: Engine>(self, engine: &mut E) -> Result<BlockRows, NodeError> {
        let meta = |e: mantle_meta::error::MetaError| NodeError::Bounds(e.to_string());
        Ok(match self {
            Self::Next { after } => BlockRows::Next(
                block::blocks(engine, after, 1)
                    .map_err(meta)?
                    .first()
                    .copied(),
            ),
            Self::OnVolume { volume, after } => BlockRows::OnVolume(
                block::on_volume(engine, volume, after, 1)
                    .map_err(meta)?
                    .first()
                    .copied(),
            ),
            Self::Places { block } => {
                BlockRows::Places(block::places(engine, block).map_err(meta)?)
            }
        })
    }
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
    // A hand-over under way is waited out: the next leader takes the replacement up.
    if !shell.is_leader() || shell.core().raft.lead_transferee().is_some() {
        return false;
    }
    let step = replacement.step(
        shell.configuration(),
        shell.id(),
        |member| shell.caught_up(member),
        shell.configuration_known(),
    );
    match step {
        Step::Propose(change) => {
            if !shell.core().raft.has_pending_conf() {
                // A change the core refuses (its leader stepping down, its log full) is proposed
                // again at a later turn.
                let _ = shell.change(Vec::new(), &change);
                owner.schedule(handle);
            }
            false
        }
        // The leader is the member replaced: its lead goes first (`Replacement::step`), and a
        // transfer the core refuses or gives up is asked again at a later turn.
        Step::Transfer(to) => {
            let _ = shell.transfer(to);
            owner.schedule(handle);
            false
        }
        Step::Wait => false,
        Step::Done => true,
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
    /// The engine's checkpoints and its device's flushes since it opened (`Engine::checkpoints`).
    pub(crate) checkpoints: (u64, u64),
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
            checkpoints: replica.machine().engine().checkpoints(),
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

/// Whether the log may have answered `replica` since its last drive, which a turn drives it to
/// take: a write of its own out, or, leading, a page of a member's catch-up being read or waiting
/// its turn (hyper-durable's `Replica::catching_up`), whose answer wakes the task as a write's does
/// (node.md §2.2). The drive sends the members the pages read and opens the read of the next, so
/// each read starts as the one before it is answered. Queued for its writes alone, a leader whose
/// read was answered with none out was driven only by the next message into its range or its
/// next tick, and the page waited for it.
pub(crate) fn answer_due<E: Engine + 'static, S: LogStore>(replica: &Replica<E, S>) -> bool {
    replica.in_flight() > 0 || replica.catching_up()
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
            .map_err(|e| mantle_range::refused(shell.machine(), e))?;
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

/// `e`, what the replica at `handle` of `owner` answered its drive, as the node's error: a range
/// its machine fenced with the error the machine met and its engine's own
/// (`mantle_range::refused`).
pub(crate) fn refused<E: Engine + 'static, S: LogStore>(
    owner: &Owner<Answers<S>, RangeMachine<E>, Unbounded>,
    handle: Handle,
    e: ReplicaError,
) -> NodeError {
    NodeError::Range(match owner.get(handle) {
        Some(replica) => mantle_range::refused(replica.machine(), e),
        None => e.into(),
    })
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

/// Proposes the entry encoded as `data` to `far`'s range on its shard ([`In::Propose`]), at most `max` bytes, as ask
/// `ask`, which the range's refusal names ([`Out::Refused`]). One whose lane has no room is refused
/// `LaneFull`, as one its replica would refuse: a peer's command is answered `Refused` while the
/// range leads here, which its asker asks again after its pause, and the node's own pauses
/// (`Node::propose_command`, `Node::propose_own`). A shard with no lanes, or whose task ended, is
/// refused `ShardEnded`.
pub(crate) fn propose<E: Engine + 'static, S: LogStore>(
    shards: &[Lanes<E, S>],
    far: &Far,
    data: Vec<u8>,
    max: u64,
    ask: u64,
) -> Result<(), NodeError> {
    shards
        .iter()
        .find(|lanes| lanes.shard() == far.shard)
        .ok_or(NodeError::ShardEnded(far.shard))?
        .send(In::Propose(far.group, data, max, ask))
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

/// Asks `far`'s shard for the root range's volume table ([`In::Table`]) where it applied past
/// `held`, the entry the node's table was read at, and none is asked: one at a time, as the
/// table, every volume of the cell, may be large, so the shard sends it only when asked and the
/// node reads the table it holds meanwhile. One whose lane has no room is refused `LaneFull`, not
/// asked, and asked again at the node's next read; a shard with no lanes, or whose task ended, is
/// refused `ShardEnded`.
pub(crate) fn ask_table<E: Engine + 'static, S: LogStore>(
    shards: &[Lanes<E, S>],
    far: &mut Far,
    held: Option<u64>,
) -> Result<(), NodeError> {
    if far.table_out || far.table.is_some() || held == Some(far.seen.applied) {
        return Ok(());
    }
    shards
        .iter()
        .find(|lanes| lanes.shard() == far.shard)
        .ok_or(NodeError::ShardEnded(far.shard))?
        .send(In::Table(far.group))?;
    far.table_out = true;
    Ok(())
}

/// Repair's `read` of `far`'s rows, the Block range's: what its shard answered, taken once; none
/// until then, the read asked ([`In::Block`]) one at a time, so a read past one out waits for it
/// and an answer to another read is let go. One whose lane has no room is refused `LaneFull`, not
/// asked, and asked again at repair's next turn; a shard with no lanes, or whose task ended, is
/// refused `ShardEnded`.
pub(crate) fn read_block<E: Engine + 'static, S: LogStore>(
    shards: &[Lanes<E, S>],
    far: &mut Far,
    read: BlockRead,
) -> Result<Option<BlockRows>, NodeError> {
    match far.block.take() {
        Some((asked, Some(answer))) if asked == read => return answer.map(Some),
        Some((asked, None)) => {
            far.block = Some((asked, None));
            return Ok(None);
        }
        Some(_) | None => {}
    }
    shards
        .iter()
        .find(|lanes| lanes.shard() == far.shard)
        .ok_or(NodeError::ShardEnded(far.shard))?
        .send(In::Block(far.group, read))?;
    far.block = Some((read, None));
    Ok(None)
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
    /// The stream's words the shard is yet to be told of, in the order the stream gave them
    /// ([`Lanes::tell_stream`]): at most a lane's capacity.
    stream: VecDeque<In<E, S>>,
    /// The sends a full lane refused, each kind's in its place in [`KINDS`], and the stream's
    /// words held for want of room, each since the lanes were made (`Node::lane_counts`).
    full: [Cell<u64>; KINDS.len()],
    held_words: u64,
}

/// Submits `task` to the shard `holder` names, through its control queue, to run there in hyper-rt's
/// Critical class (`coop::Class`; hyper-raft docs/runtime.md §3.15), as the node's task runs on the
/// first (`process::run_on_first_shard`): a shard's task drives its ranges' Raft and apply, so woken
/// it goes ahead of the S3 connections the shard serves beside it, which stay Normal, the shares of
/// the shard's time staying equal. A submission takes the Normal class, so what is submitted is a
/// launcher that spawns `task` Critical on its shard and ends, holding a task slot of its own while
/// it spawns (`derive::shard_shape`). Refused when the shard takes no task now: gone, or its control
/// queue full.
fn submit_critical<F: Future<Output = ()> + Send + 'static>(
    holder: SlotHolder,
    task: F,
) -> Result<(), NodeError> {
    hyper_rt::runtime::submit_to_holder(holder, async move {
        // A shard that refuses it drops `task` with what it holds, a shard's task its lanes' ends,
        // which the node reads as the shard's end (`NodeError::ShardEnded`), as it reads a shard
        // that refuses the launcher.
        let _ = hyper_rt::futures::spawn_detached_as(hyper_rt::coop::Class::Critical, task);
    })?;
    Ok(())
}

impl<E, S> Lanes<E, S>
where
    E: Engine + Send + 'static,
    S: LogStore + Send + 'static,
{
    /// Starts shard `shard`'s task on the shard `holder` names, through its control queue (as the
    /// S3 listener's lanes start their takers, `listener::shards`), Critical there
    /// ([`submit_critical`]), its owner holding `slots` replicas and each lane `capacity` values.
    /// Refused when a lane cannot be made or the shard takes no task now: gone, or its control
    /// queue full.
    pub(crate) fn start(
        holder: SlotHolder,
        shard: usize,
        slots: usize,
        capacity: usize,
    ) -> Result<Self, NodeError> {
        let (into, taken) = channel(capacity)?;
        let (given, from) = channel(capacity)?;
        submit_critical(holder, run(taken, given, slots, capacity))?;
        Ok(Self {
            shard,
            capacity,
            into,
            from,
            held: None,
            lost: Vec::new(),
            stream: VecDeque::new(),
            full: std::array::from_fn(|_| Cell::new(0)),
            held_words: 0,
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
        let kind = sent.kind();
        self.into.try_send(sent).map_err(|refused| match refused {
            SyncError::Full(_) => {
                if let Some(count) = KINDS
                    .iter()
                    .position(|k| *k == kind)
                    .and_then(|at| self.full.get(at))
                {
                    count.set(count.get().saturating_add(1));
                }
                NodeError::LaneFull(self.shard)
            }
            SyncError::Closed(_) | SyncError::NotOnShardThread(_) => {
                NodeError::ShardEnded(self.shard)
            }
        })
    }

    /// Hands the shard a word of the stream, a change of it ([`In::Believe`]) or its belief of a
    /// peer a range came to share its group with ([`In::Belief`]), after those held: held while
    /// its lane is full, in order, and told at the next turn's take ([`Lanes::tell_held`]), as a
    /// lost peer is, since a range that missed it would believe what the stream no longer does. A
    /// lane full of Raft messages, which it drops, so holds a word for a turn rather than stopping
    /// the node. At most a lane's capacity is held: one past it is refused `LaneFull`, which stops
    /// the node; and `ShardEnded` when the shard's task is gone.
    pub(crate) fn tell_stream(&mut self, word: In<E, S>) -> Result<(), NodeError> {
        self.tell_held()?;
        let word = if self.stream.is_empty() {
            match self.into.try_send(word) {
                Ok(()) => return Ok(()),
                Err(SyncError::Full(word)) => word,
                Err(SyncError::Closed(_) | SyncError::NotOnShardThread(_)) => {
                    return Err(NodeError::ShardEnded(self.shard));
                }
            }
        } else {
            word
        };
        if self.stream.len() >= self.capacity {
            return Err(NodeError::LaneFull(self.shard));
        }
        self.stream.push_back(word);
        self.held_words = self.held_words.saturating_add(1);
        Ok(())
    }

    /// The sends a full lane refused, each kind's ([`KINDS`]), and the stream's words it held for
    /// want of room, since the lanes were made.
    pub(crate) fn full(&self) -> (impl Iterator<Item = (&'static str, u64)> + '_, u64) {
        (
            KINDS.iter().copied().zip(self.full.iter().map(Cell::get)),
            self.held_words,
        )
    }

    /// Tells the shard the stream's words held ([`Lanes::tell_stream`]) while its lane has room,
    /// in order, and holds the rest for the next turn's take.
    pub(crate) fn tell_held(&mut self) -> Result<(), NodeError> {
        while let Some(word) = self.stream.pop_front() {
            match self.into.try_send(word) {
                Ok(()) => {}
                Err(SyncError::Full(word)) => {
                    self.stream.push_front(word);
                    break;
                }
                Err(SyncError::Closed(_) | SyncError::NotOnShardThread(_)) => {
                    return Err(NodeError::ShardEnded(self.shard));
                }
            }
        }
        Ok(())
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
pub(super) struct Kept {
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
    /// Whether it is the root range, whose rows the shard hands over ([`Out::Rows`]), and the
    /// entry it had applied when it last did.
    root: bool,
    rows_at: Option<u64>,
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
    /// replicas' one `Ready` each and a few words of each, and a refusal for each proposal the
    /// turn took (at most a lane's capacity and the one that woke it), handed on before the next
    /// turn.
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
    /// The waker its ranges' logs answer through, a compaction's write and a member's catch-up
    /// read as well as a write: the task's, wrapped in a flag ([`Woken`]) the wait takes first.
    waker: Waker,
    woken: Arc<Woken>,
    /// Whether the node's shard asked a round of compaction ([`In::Compact`]) the shard has yet to
    /// make.
    compact: bool,
    /// Where the node's shard asked its ranges' engines back ([`In::Release`]).
    release: Option<OneshotSender<Vec<(u128, E)>>>,
    /// Its ranges' engines' maintenance in the time its turns leave (`node::upkeep`).
    upkeep: super::upkeep::Upkeep,
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
                let root = replica.machine().layer() == mantle_meta::apply::Layer::Root;
                let handle = self.owner.insert(*replica).map_err(|_| NodeError::Full)?;
                self.ranges.push(Kept {
                    group,
                    handle,
                    wake: None,
                    peers,
                    seen,
                    configured,
                    replacing: None,
                    root,
                    rows_at: None,
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
            In::Propose(group, data, max, ask) => {
                // A proposal the range refuses is answered by its ask at once, as the node's own
                // replica's refusal is, so its asker asks again after its pause rather than at its
                // patience's end (node.md §2.9, step 4).
                let proposed = self.at(group).and_then(|handle| {
                    let replica = self.owner.get_mut(handle)?;
                    let proposed = mantle_range::propose_encoded(replica, data, max);
                    self.owner.schedule(handle);
                    Some(proposed)
                });
                let refusal = match proposed {
                    Some(Ok(())) => None,
                    Some(Err(mantle_range::ReplicaError::EntryTooLarge { .. })) => {
                        Some(Refusal::TooLarge)
                    }
                    Some(Err(_)) | None => Some(Refusal::NotTaken),
                };
                if let Some(refusal) = refusal {
                    self.gave.push(Out::Refused(group, ask, refusal));
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
            In::Table(group) => {
                let replica = self.at(group).and_then(|handle| self.owner.get_mut(handle));
                let facts = match replica {
                    Some(replica) => {
                        let applied = replica.applied().index;
                        read_facts(replica.machine_mut().engine_mut())
                            .map(|facts| Box::new(Facts { applied, facts }))
                    }
                    None => Err(NodeError::Bounds("no root range here".into())),
                };
                self.gave.push(Out::Table(group, facts));
            }
            In::Block(group, read) => {
                let replica = self.at(group).and_then(|handle| self.owner.get_mut(handle));
                let rows = match replica {
                    Some(replica) => read.read(replica.machine_mut().engine_mut()),
                    None => Err(NodeError::Bounds("no Block range here".into())),
                };
                self.gave.push(Out::Block(group, read, rows));
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

    /// Queues the replicas whose deadline came, and those with a write out or a page of a member's
    /// catch-up being read, which a log's answer may have woken ([`answer_due`]).
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
            if self.owner.get(kept.handle).is_some_and(answer_due) {
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
                    fenced.get_or_insert((handle, e));
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
            // The root range's rows, read again once it applied an entry, as the node's shard
            // reads its own (`Node::read_members`); one its engine fails to read stops the node.
            if kept.root
                && let Some(replica) = self.owner.get_mut(handle)
            {
                let applied = replica.applied().index;
                if kept.rows_at != Some(applied) {
                    let rows = Rows::read(replica.machine_mut().engine_mut(), applied)?;
                    kept.rows_at = Some(applied);
                    self.gave.push(Out::Rows(kept.group, Box::new(rows)));
                }
            }
        }
        self.serve();
        match fenced {
            // A fenced range stops the node, as one on the node's shard does (`Node::drive`).
            Some((handle, e)) => Err(refused(&self.owner, handle, e)),
            None => Ok(()),
        }
    }

    /// A turn's maintenance of the shard's ranges' engines, once what the turn gave is handed on,
    /// after `own_ns` of the turn's own work (`node::upkeep`).
    fn upkeep(&mut self, own_ns: u64) -> Result<(), NodeError> {
        let Self {
            upkeep,
            ranges,
            owner,
            ..
        } = self;
        upkeep.turn(own_ns, ranges.len(), |at, units| {
            let Some(kept) = ranges.get(at) else {
                return Ok((0, false));
            };
            match owner.get_mut(kept.handle) {
                Some(replica) => {
                    super::upkeep::step(kept.group, replica.machine_mut().engine_mut(), units)
                }
                None => Ok((0, false)),
            }
        })?;
        Ok(())
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
    // Its replicas apply through the engine's synchronous calls, as the node's own shard's do: a
    // metadata shard, which may wait on the device (node.md §1.2, §2.9).
    mantle_engine::store::permit_blocking();
    let woken = Arc::new(Woken {
        task: poll_fn(|context| Poll::Ready(context.waker().clone())).await,
        woken: AtomicBool::new(false),
    });
    let waker = Waker::from(Arc::clone(&woken));
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
        woken,
        compact: false,
        release: None,
        upkeep: super::upkeep::Upkeep::default(),
    };
    let mut arrived = None;
    loop {
        let began = std::time::Instant::now();
        // The turn looks at every source of the task's wakes from here, so a wake that came before
        // is this turn's to answer, and one that comes after is kept for the wait.
        shard.woken.take();
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
        // The turn's own work, not its wait for the lane's room below.
        let own_ns = u64::try_from(began.elapsed().as_nanos()).unwrap_or(u64::MAX);
        for out in shard.gave.drain(..) {
            if given.send(out).await.is_err() {
                return;
            }
        }
        if let Some(error) = stopped {
            let _ = given.send(Out::Stopped(error)).await;
            return;
        }
        // Last, its ranges' engines' maintenance, a turn's budget of it.
        if let Err(error) = shard.upkeep(own_ns) {
            let _ = given.send(Out::Stopped(error)).await;
            return;
        }
        // With work still queued, an answer in, maintenance owed or a wake come since the turn
        // began, the task yields once, so the shard's other tasks run between its turns, and turns
        // again. A wake ends whichever await of the task it finds, and one that came while the
        // task waited for the lane's room above (a member's catch-up page read, which no look here
        // finds) ended that wait, not the one below, which slept to the range's next deadline with
        // the page in hand. Every source the owner and compaction answer through wakes through the
        // flag ([`Woken`]), so the wake is kept, and taken at the turn it brings.
        if shard.woken.take() || shard.owner.has_work() || shard.answered() || shard.upkeep.owed() {
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
    use mantle_meta::wire::{Command, Entry, Sessioned};
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
        let mut replica = following(log, range);
        replica.campaign().unwrap();
        replica
    }

    /// A one-member range's member that never campaigns: its elections follow its detectors
    /// (`Elections::Suspicion`), which nothing measures here, so it leads not.
    fn following(log: &Log<DeviceFile>, range: &Range) -> Replica<Model, Stored> {
        let mut engine = Model::default();
        engine.install(0, name::first(1).unwrap()).unwrap();
        engine.persist().unwrap();
        let store = Answers::new(mantle_range::claim(log, GROUP, range).unwrap());
        mantle_range::open_shell(1, store, engine, range, 1, Elections::Suspicion).unwrap()
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
            stream: VecDeque::new(),
            full: std::array::from_fn(|_| Cell::new(0)),
            held_words: 0,
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

    /// A shard's task is spawned Critical on the shard it is submitted to, so its ranges' Raft and
    /// apply go ahead of the S3 connections that shard serves; what it spawns stays Normal. The
    /// launcher and the task fit the two slots a shard past the first holds for them.
    #[test]
    fn a_range_shards_task_is_spawned_critical_and_what_it_spawns_normal() {
        use hyper_rt::coop::Class;
        let rt = started(2, 2);
        let (tell, heard) = std::sync::mpsc::sync_channel(1);
        submit_critical(rt.holder_of(rt.shard_ids()[1]).unwrap(), async move {
            let (told, spawned) = oneshot().unwrap();
            hyper_rt::futures::spawn_detached(async move {
                let _ = told.send(hyper_rt::coop::class());
            })
            .unwrap();
            let spawned = spawned.await.ok().flatten();
            let _ = tell.try_send((hyper_rt::coop::class(), spawned));
        })
        .unwrap();
        assert_eq!(
            heard.recv().unwrap(),
            (Some(Class::Critical), Some(Class::Normal))
        );
        rt.shutdown().unwrap();
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
        // A proposal before the range leads is refused, as a follower refuses one: proposed again
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
                        .send(In::Propose(
                            GROUP,
                            put().encode().unwrap(),
                            range.settings.max_entry_bytes,
                            0,
                        ))
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
            .send(In::Propose(
                GROUP,
                put().encode().unwrap(),
                range.settings.max_entry_bytes,
                0,
            ))
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
        let mut commanded = super::super::keyed::Waiting::default();
        commanded.push(super::super::Commanded {
            origin: super::super::CommandOrigin::Own(0),
            group: GROUP,
            ask: 0,
            commands: vec![(0, 1, None)],
        });
        lanes
            .send(In::Propose(
                GROUP,
                put().encode().unwrap(),
                range.settings.max_entry_bytes,
                0,
            ))
            .unwrap();
        let Out::Applied(group, applied) =
            next(&mut lanes, "its entry applied", deadline(), |out| {
                !matches!(out, Out::Applied(..))
            })
        else {
            panic!("`next` gives only what it does not skip")
        };
        super::super::answer_command(&mut commanded, group, &applied);
        assert!(commanded.get(0).unwrap().commands[0].2.is_some());
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
            .send(In::Propose(
                GROUP,
                put().encode().unwrap(),
                range.settings.max_entry_bytes,
                0,
            ))
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
            .send(In::Propose(
                GROUP,
                put().encode().unwrap(),
                range.settings.max_entry_bytes,
                0,
            ))
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
            rows: None,
            table_out: false,
            table: None,
            block: None,
        }
    }

    /// A member that leads not, moved to the second shard of two, its lanes and what it showed when
    /// it moved: nothing measures it, so it never campaigns.
    fn follower(
        rt: &Runtime,
        log: &Log<DeviceFile>,
        range: &Range,
    ) -> (Vec<Lanes<Model, Stored>>, Far) {
        let replica = following(log, range);
        let moved = Seen::of(&replica);
        let holder = rt.holder_of(rt.shard_ids()[1]).unwrap();
        let lanes = Lanes::start(holder, 1, 1, 8).unwrap();
        lanes.send(In::Adopt(GROUP, Box::new(replica))).unwrap();
        (vec![lanes], far(moved))
    }

    /// A proposal a far range refuses is answered by its ask at once (node.md §2.9, step 4): one
    /// to a member that leads not, which its replica refuses, and one past the most bytes the
    /// range takes in one, refused for what it is; the command waiting on each is taken by its
    /// ask alone, so the node answers its asker as its own replica's refusal answers it
    /// (`Node::refused_far`), and a refusal no command waits on answers nothing.
    #[test]
    fn a_refused_far_proposal_answers_its_asker_at_once() {
        use super::super::{CommandOrigin, Commanded, refused_command};
        let rt = started(2, 2);
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let (mut shards, far) = follower(&rt, &log, &range);
        assert!(!far.seen.leading);
        let mut commanded = super::super::keyed::Waiting::default();
        commanded.push(Commanded {
            origin: CommandOrigin::Own(3),
            group: GROUP,
            ask: 7,
            commands: vec![(0, 1, None)],
        });
        commanded.push(Commanded {
            origin: CommandOrigin::Own(4),
            group: GROUP,
            ask: 8,
            commands: vec![(0, 2, None)],
        });
        let max = range.settings.max_entry_bytes;
        propose(&shards, &far, put().encode().unwrap(), max, 7).unwrap();
        let refused = |shards: &mut Vec<Lanes<Model, Stored>>| {
            let Out::Refused(group, ask, refusal) =
                next(&mut shards[0], "the proposal refused", deadline(), |out| {
                    !matches!(out, Out::Refused(..))
                })
            else {
                panic!("`next` gives only what it does not skip")
            };
            (group, ask, refusal)
        };
        assert_eq!(refused(&mut shards), (GROUP, 7, Refusal::NotTaken));
        let taken = refused_command(&mut commanded, GROUP, 7).unwrap();
        assert!(taken.origin == CommandOrigin::Own(3));
        assert_eq!(commanded.len(), 1, "the other command waits on");
        propose(&shards, &far, put().encode().unwrap(), 1, 8).unwrap();
        assert_eq!(refused(&mut shards), (GROUP, 8, Refusal::TooLarge));
        assert!(refused_command(&mut commanded, GROUP + 1, 8).is_none());
        assert!(refused_command(&mut commanded, GROUP, 7).is_none());
        assert!(refused_command(&mut commanded, GROUP, 8).is_some());
        drop(shards);
        rt.shutdown().unwrap();
        drop(log);
    }

    /// A read a far range refuses is answered by its id at once: one asked of a member that leads
    /// not, as its shard last showed it leading, comes back refused in the turn that took it,
    /// which the node answers as `ask_read` answers a refusal (`Node::served_far`).
    #[test]
    fn a_refused_far_read_answers_its_asker_at_once() {
        let rt = started(2, 2);
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let (mut shards, mut far) = follower(&rt, &log, &range);
        // As its shard last showed it, before it stopped leading.
        far.seen.leading = true;
        let query = Query::Current {
            bucket: "b".into(),
            key: "k".into(),
        };
        assert!(ask_read(&shards, &far, 9, &query).unwrap());
        let Out::Served(group, id, served) =
            next(&mut shards[0], "the read refused", deadline(), |out| {
                !matches!(out, Out::Served(..))
            })
        else {
            panic!("`next` gives only what it does not skip")
        };
        assert_eq!((group, id), (GROUP, 9));
        assert!(matches!(served, Served::Refused));
        drop(shards);
        rt.shutdown().unwrap();
        drop(log);
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

    /// A model engine whose calls fail once `failing` is set, as `mantle_range::Stored`'s fail
    /// once its store refuses: each answers `Fenced`, the engine keeping its own error, the one a
    /// node of the node suite stopped on (2026-11-19), as its fault.
    struct Failing {
        rows: Model,
        failing: bool,
        fault: Option<mantle_engine::Error>,
    }

    impl Failing {
        fn fenced(&mut self) -> mantle_meta::engine::EngineError {
            self.fault
                .get_or_insert(mantle_engine::Error::LimitExceeded {
                    what: "extents of a store",
                    limit: 256,
                });
            mantle_meta::engine::EngineError::Fenced
        }
    }

    impl mantle_meta::engine::Rows for Failing {
        fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, mantle_meta::engine::EngineError> {
            if self.failing {
                return Err(self.fenced());
            }
            self.rows.get(key)
        }

        fn next(
            &mut self,
            from: &[u8],
            to: &[u8],
        ) -> Result<Option<mantle_meta::engine::Row>, mantle_meta::engine::EngineError> {
            if self.failing {
                return Err(self.fenced());
            }
            self.rows.next(from, to)
        }

        fn apply(
            &mut self,
            index: u64,
            writes: Vec<mantle_meta::engine::Write>,
        ) -> Result<(), mantle_meta::engine::EngineError> {
            if self.failing {
                return Err(self.fenced());
            }
            self.rows.apply(index, writes)
        }
    }

    impl Engine for Failing {
        fn applied(&self) -> u64 {
            self.rows.applied()
        }

        fn durable(&self) -> u64 {
            self.rows.durable()
        }

        fn persist(&mut self) -> Result<(), mantle_meta::engine::EngineError> {
            if self.failing {
                return Err(self.fenced());
            }
            self.rows.persist()
        }

        fn image(
            &mut self,
        ) -> Result<Vec<mantle_meta::engine::Row>, mantle_meta::engine::EngineError> {
            self.rows.image()
        }

        fn image_size(&self) -> Option<(u64, u64)> {
            self.rows.image_size()
        }

        fn install(
            &mut self,
            index: u64,
            rows: Vec<mantle_meta::engine::Row>,
        ) -> Result<(), mantle_meta::engine::EngineError> {
            self.rows.install(index, rows)
        }

        fn fault(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.fault
                .as_ref()
                .map(|e| e as &(dyn std::error::Error + 'static))
        }
    }

    /// What a range shard's turns did with an [`Upkept`] engine's maintenance: the turns it was
    /// asked at and the units done; the turns that found its job out; and, the job ended, the
    /// turns asked as it ended and the turn that took its result. The task that looked last is
    /// woken as the job ends, as a maintenance worker's message wakes its shard
    /// (`ShardDb::idle_step_now`).
    #[derive(Default)]
    struct Ledger {
        turns: std::sync::atomic::AtomicU64,
        units: std::sync::atomic::AtomicU64,
        looked: std::sync::atomic::AtomicU64,
        ended_at: std::sync::atomic::AtomicU64,
        taken_at: std::sync::atomic::AtomicU64,
        /// Whether the job ended, and the task to wake as it does: one lock, so a turn either
        /// takes the result or is woken for it.
        job: std::sync::Mutex<(bool, Option<Waker>)>,
    }

    impl Ledger {
        fn get(at: &std::sync::atomic::AtomicU64) -> u64 {
            at.load(std::sync::atomic::Ordering::SeqCst)
        }

        /// The job ends, as a worker's does: its owner's task woken.
        fn end_job(&self) {
            let mut job = self.job.lock().unwrap();
            self.ended_at
                .store(Self::get(&self.turns), std::sync::atomic::Ordering::SeqCst);
            job.0 = true;
            if let Some(waker) = job.1.take() {
                waker.wake();
            }
        }
    }

    /// A model engine whose maintenance is `inline` units done on its shard, a turn's budget at a
    /// time, then, when `out`, a job out on a worker that the test ends ([`Ledger::end_job`]).
    struct Upkept {
        rows: Model,
        inline: u64,
        out: bool,
        ledger: std::sync::Arc<Ledger>,
    }

    impl mantle_meta::engine::Rows for Upkept {
        fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, mantle_meta::engine::EngineError> {
            self.rows.get(key)
        }

        fn next(
            &mut self,
            from: &[u8],
            to: &[u8],
        ) -> Result<Option<mantle_meta::engine::Row>, mantle_meta::engine::EngineError> {
            self.rows.next(from, to)
        }

        fn apply(
            &mut self,
            index: u64,
            writes: Vec<mantle_meta::engine::Write>,
        ) -> Result<(), mantle_meta::engine::EngineError> {
            self.rows.apply(index, writes)
        }
    }

    impl Engine for Upkept {
        fn applied(&self) -> u64 {
            self.rows.applied()
        }

        fn durable(&self) -> u64 {
            self.rows.durable()
        }

        fn persist(&mut self) -> Result<(), mantle_meta::engine::EngineError> {
            self.rows.persist()
        }

        fn image(
            &mut self,
        ) -> Result<Vec<mantle_meta::engine::Row>, mantle_meta::engine::EngineError> {
            self.rows.image()
        }

        fn image_size(&self) -> Option<(u64, u64)> {
            self.rows.image_size()
        }

        fn install(
            &mut self,
            index: u64,
            rows: Vec<mantle_meta::engine::Row>,
        ) -> Result<(), mantle_meta::engine::EngineError> {
            self.rows.install(index, rows)
        }

        fn upkeep(&mut self, budget: u64) -> Result<(u64, bool), mantle_meta::engine::EngineError> {
            use std::sync::atomic::Ordering::SeqCst;
            let ledger = &self.ledger;
            let turn = ledger.turns.fetch_add(1, SeqCst) + 1;
            if self.inline > 0 {
                let did = budget.min(self.inline);
                self.inline -= did;
                ledger.units.fetch_add(did, SeqCst);
                return Ok((did, self.inline > 0 || self.out));
            }
            if !self.out {
                return Ok((0, false));
            }
            let mut job = ledger.job.lock().unwrap();
            if job.0 {
                self.out = false;
                ledger.taken_at.store(turn, SeqCst);
                ledger.units.fetch_add(1, SeqCst);
                return Ok((1, false));
            }
            // Nothing to do but the job: the task is left to be woken as it ends.
            let task = hyper_rt::futures::current_task().unwrap();
            job.1 = Some(hyper_rt::waker::waker_for(task.0));
            ledger.looked.fetch_add(1, SeqCst);
            Ok((0, true))
        }
    }

    /// A range shard turns at once while its range's engine owes maintenance it can do, and works
    /// through it with nothing else to turn it; with only a job out on a worker it waits, and the
    /// job's end wakes it, its result taken within a turn of the job's end.
    #[test]
    fn a_range_shard_turns_for_maintenance_it_can_do_and_wakes_for_its_workers() {
        let rt = started(2, 2);
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let mut rows = Model::default();
        rows.install(0, name::first(1).unwrap()).unwrap();
        rows.persist().unwrap();
        let ledger = std::sync::Arc::new(Ledger::default());
        const INLINE: u64 = 1_000;
        let engine = Upkept {
            rows,
            inline: INLINE,
            out: true,
            ledger: std::sync::Arc::clone(&ledger),
        };
        let store = Answers::new(mantle_range::claim(&log, GROUP, &range).unwrap());
        // A member that never campaigns (`following`): nothing but its maintenance turns it.
        let replica =
            mantle_range::open_shell(1, store, engine, &range, 1, Elections::Suspicion).unwrap();
        let holder = rt.holder_of(rt.shard_ids()[1]).unwrap();
        let mut lanes = Lanes::<Upkept, Stored>::start(holder, 1, 1, 8).unwrap();
        lanes.send(In::Adopt(GROUP, Box::new(replica))).unwrap();
        let mut until = |what: &str, done: &dyn Fn() -> bool| {
            let until = deadline();
            while !done() {
                match lanes.take() {
                    Ok(Some(Out::Stopped(error))) | Err(error) => {
                        panic!("{what}: the shard stopped: {error}")
                    }
                    Ok(_) => {}
                }
                assert!(
                    std::time::Instant::now() < until,
                    "{what}: past {PATIENCE:?}"
                );
                std::thread::yield_now();
            }
        };
        // The inline work, with nothing else to turn the shard: each turn did some.
        until("the inline maintenance done", &|| {
            Ledger::get(&ledger.looked) > 0
        });
        let inline_turns = Ledger::get(&ledger.turns) - Ledger::get(&ledger.looked);
        assert_eq!(Ledger::get(&ledger.units), INLINE);
        assert!(
            inline_turns <= INLINE,
            "{inline_turns} turns for {INLINE} units"
        );
        // The job out: the shard looked, and waits. Its end wakes the shard, whose next turn
        // takes the result.
        ledger.end_job();
        until("the job's result taken", &|| {
            Ledger::get(&ledger.taken_at) > 0
        });
        let (ended, taken) = (Ledger::get(&ledger.ended_at), Ledger::get(&ledger.taken_at));
        eprintln!(
            "inline turns {inline_turns}, looks {}, ended at turn {ended}, taken at {taken}",
            Ledger::get(&ledger.looked)
        );
        assert!(
            taken <= ended + 1,
            "taken at turn {taken}, ended at {ended}"
        );
        assert_eq!(Ledger::get(&ledger.units), INLINE + 1);
        drop(lanes);
        rt.shutdown().unwrap();
        drop(log);
    }

    /// What a [`Paging`] store's catch-up was asked: the drives that called it, and the waker the
    /// last one gave it, the shard's, which a page's read wakes as it is answered.
    #[derive(Default)]
    struct Pager {
        drives: std::sync::atomic::AtomicU64,
        waker: std::sync::Mutex<Option<Waker>>,
    }

    impl Pager {
        fn drives(&self) -> u64 {
            self.drives.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// A group's log held in memory (hyper-durable's `RamStore`) with a page of a member's
    /// catch-up being read at every drive, as a leader's store has while a lagging member's page
    /// is read from its log's file (`LogStore::catch_up`): each drive's call is counted and its
    /// waker kept ([`Pager`]), for the test to wake as the read is answered.
    struct Paging {
        store: hyper_durable::RamStore,
        pager: std::sync::Arc<Pager>,
    }

    impl LogStore for Paging {
        type Hold = <hyper_durable::RamStore as LogStore>::Hold;

        fn held(&self) -> Option<&Self::Hold> {
            self.store.held()
        }

        fn release(&mut self, met: &Self::Hold) {
            self.store.release(met);
        }

        fn depth(&self) -> usize {
            self.store.depth()
        }

        fn view(&self) -> Result<hyper_durable::StoreView, hyper_durable::Fault> {
            self.store.view()
        }

        fn bounds(&self) -> Result<(hyper_durable::Point, u64), hyper_raft::StorageError> {
            self.store.bounds()
        }

        fn term(&self, index: u64) -> Result<u64, hyper_raft::StorageError> {
            self.store.term(index)
        }

        fn entries(
            &self,
            low: u64,
            high: u64,
            max_bytes: u64,
            into: &mut Vec<hyper_raft::proto::Entry>,
        ) -> Result<(), hyper_raft::StorageError> {
            self.store.entries(low, high, max_bytes, into)
        }

        fn visit(
            &self,
            low: u64,
            high: u64,
            page: u64,
            visit: &mut dyn FnMut(hyper_durable::EntryRef<'_>) -> bool,
        ) -> Result<(), hyper_raft::StorageError> {
            self.store.visit(low, high, page, visit)
        }

        fn proposals(
            &self,
            into: &mut Vec<hyper_raft::proto::Entry>,
        ) -> Result<(), hyper_raft::StorageError> {
            self.store.proposals(into)
        }

        fn released(&self) -> Result<u64, hyper_raft::StorageError> {
            self.store.released()
        }

        fn room(&self) -> bool {
            self.store.room()
        }

        fn submit(
            &mut self,
            write: &hyper_durable::Write<'_>,
            waker: &Waker,
        ) -> Result<(), hyper_durable::Fault> {
            self.store.submit(write, waker)
        }

        fn poll(&mut self) -> Option<Result<(), hyper_durable::Fault>> {
            self.store.poll()
        }

        fn write_now(
            &mut self,
            write: &hyper_durable::Write<'_>,
        ) -> Result<(), hyper_durable::Fault> {
            self.store.write_now(write)
        }

        fn keep_after(&mut self, index: u64) {
            self.store.keep_after(index);
        }

        fn kept_bytes(&self) -> u64 {
            self.store.kept_bytes()
        }

        fn catch_up(&mut self, waker: &Waker, _: &[u64], _: &mut Vec<u64>) -> bool {
            self.pager
                .drives
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            *self.pager.waker.lock().unwrap() = Some(waker.clone());
            true
        }
    }

    /// A wake a range shard's task takes while it waits for room in its lane to the node's shard,
    /// a member's catch-up page read here, turns the task once its sends are done, at that instant,
    /// rather than at its range's next deadline; and turns it once. The task runs on hyper-rt's
    /// simulated runtime, whose clock moves only when the test moves it, and the test holds both
    /// lanes: two proposals its range refuses give two answers to a lane that holds one, so the
    /// task waits for room; the test wakes it there, runs it, then empties the lane.
    #[test]
    fn a_wake_a_range_shard_takes_while_it_waits_for_its_lanes_room_turns_it_once_sent() {
        let mut sim = hyper_rt::SimRuntime::new(
            &RuntimeConfig {
                shards: 1,
                tasks_per_shard: 2,
                timers_per_shard: 2,
                interests_per_shard: hyper_rt::runtime::interests_for(2),
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
            },
            1,
        )
        .unwrap();
        let shard = sim.shard_ids()[0];
        let (into, taken) = channel(4).unwrap();
        let (given, mut from) = channel(1).unwrap();
        sim.spawn_on(shard, run::<Model, Paging>(taken, given, 1, 4))
            .unwrap();
        let range = one_member();
        let pager = std::sync::Arc::new(Pager::default());
        let store = Answers::new(Paging {
            store: hyper_durable::RamStore::new(),
            pager: std::sync::Arc::clone(&pager),
        });
        let mut engine = Model::default();
        engine.install(0, name::first(1).unwrap()).unwrap();
        engine.persist().unwrap();
        // A sole voter, which leads at its first drives: the writes it makes are answered at once.
        let replica =
            mantle_range::open_shell(1, store, engine, &range, 1, Elections::Suspicion).unwrap();
        // What the node's shard takes from the lane, each taking letting the task send the next:
        // the refusals among it.
        let mut take = |sim: &mut hyper_rt::SimRuntime, now: u64| {
            let mut refusals = 0;
            while let Ok(Some(out)) = from.try_recv() {
                assert!(!matches!(out, Out::Stopped(_)), "the shard stopped");
                refusals += u32::from(matches!(out, Out::Refused(..)));
                sim.run_until(now);
            }
            refusals
        };
        let now = sim.now_ns();
        let sent = into.try_send(In::Adopt(GROUP, Box::new(replica)));
        assert!(sent.is_ok(), "the range handed to its shard");
        sim.run_until(now);
        take(&mut sim, now);
        let before = pager.drives();
        assert!(before > 0, "the shard drove its range");
        // Two proposals past the most bytes the range takes in one, which it refuses at once
        // with nothing written: two answers, and the lane holds one.
        for ask in [1, 2] {
            let sent = into.try_send(In::Propose(GROUP, put().encode().unwrap(), 1, ask));
            assert!(sent.is_ok(), "the proposal handed to the shard");
        }
        sim.run_until(now);
        let waiting = pager.drives();
        assert!(waiting > before, "the turn drove the range");
        // The page is read while the task waits for the lane's room: the wake ends that wait.
        let waker = pager.waker.lock().unwrap().clone().unwrap();
        waker.wake_by_ref();
        sim.run_until(now);
        assert_eq!(pager.drives(), waiting, "no turn while the lane is full");
        // The node's shard takes what its lane holds: the task sends the rest, then turns for the
        // page read, at the same instant, once.
        assert_eq!(
            take(&mut sim, now),
            2,
            "both refusals, one past the lane's room"
        );
        assert_eq!(
            pager.drives(),
            waiting + 1,
            "the task turned once for the page read while it waited for room"
        );
        assert_eq!(sim.now_ns(), now, "and no deadline passed");
    }

    /// A range whose engine fails stops the node with the engine's own error in its text, beside
    /// the step that failed and the error the step met. The node suite's nodes stopped saying
    /// only the step ("the engine failed to persist a snapshot"), the engine's error out only
    /// through a diagnostic print (2026-11-19). The sole voter leads at a drive past its first
    /// and applies its term's empty entry, which its engine refuses.
    #[test]
    fn a_range_whose_engine_fails_stops_the_node_with_the_engines_own_error() {
        let range = one_member();
        let mut rows = Model::default();
        rows.install(0, name::first(1).unwrap()).unwrap();
        rows.persist().unwrap();
        let engine = Failing {
            rows,
            failing: false,
            fault: None,
        };
        let store = Answers::new(hyper_durable::RamStore::new());
        let mut replica =
            mantle_range::open_shell(1, store, engine, &range, 1, Elections::Suspicion).unwrap();
        replica.machine_mut().engine_mut().failing = true;
        replica.campaign().unwrap();
        let mut owner = Owner::new(vec![Waker::noop().clone()]);
        let Ok(handle) = owner.insert(replica) else {
            panic!("the arena's one slot is free");
        };
        let mut out = Output::default();
        let mut fenced = None;
        // A vote's write, its answer, the leader's entry's write and its apply: a few turns, and
        // the bound fails only a range that never applies.
        for now in 0..64 {
            owner.schedule(handle);
            owner.turn(now, &mut out, |_, outcome, _| {
                if let Err(e) = outcome {
                    fenced.get_or_insert(e);
                }
            });
            if fenced.is_some() {
                break;
            }
        }
        let e = fenced.expect("the range's engine refused its entry");
        assert_eq!(
            refused(&owner, handle, e).to_string(),
            "a range: the replica is fenced: the state machine failed: the engine refused an \
             entry: the engine is fenced after a failed write: extents of a store would exceed \
             its bound of 256"
        );
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
        propose(
            &shards,
            &far,
            put().encode().unwrap(),
            range.settings.max_entry_bytes,
            0,
        )
        .unwrap();
        let mut commanded = super::super::keyed::Waiting::default();
        commanded.push(Commanded {
            origin: CommandOrigin::Own(0),
            group: GROUP,
            ask: 0,
            commands: vec![(0, 1, None)],
        });
        let Out::Applied(group, applied) =
            next(&mut shards[0], "the command applied", deadline(), |out| {
                !matches!(out, Out::Applied(..))
            })
        else {
            panic!("`next` gives only what it does not skip")
        };
        answer_command(&mut commanded, group, &applied);
        assert!(
            commanded.get(0).unwrap().commands[0].2.is_some(),
            "the command answered"
        );

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
            propose(&shards, &far, put().encode().unwrap(), max, 0),
            Err(NodeError::LaneFull(1))
        ));
        // Counted by its kind, and no other.
        let (full, held) = shards[0].full();
        let full: Vec<(&str, u64)> = full.filter(|(_, n)| *n > 0).collect();
        assert_eq!((full, held), (vec![("propose", 1)], 0));
        far.shard = 2;
        assert!(matches!(
            propose(&shards, &far, put().encode().unwrap(), max, 0),
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

    /// A word of the stream a far range's shard's lane has no room for, full of what the node
    /// handed it, is held and told at a later take, in the order the stream gave it; held past a
    /// lane's capacity it stops the node, typed, and so does one held for a shard whose task
    /// ended.
    #[test]
    fn a_stream_word_is_held_while_the_lane_is_full_and_told_in_order() {
        let (mut lanes, (mut taken, given)) = unstarted(1);
        let trusted = |at_ns| In::Believe(Change::Trusted { peer: 2, at_ns });
        let restarted = || In::Believe(Change::Restarted { peer: 3, at_ns: 2 });
        lanes.send(measured()).unwrap();
        lanes.tell_stream(trusted(1)).unwrap();
        assert_eq!(lanes.stream.len(), 1, "held while the lane is full");
        assert!(
            matches!(lanes.tell_stream(restarted()), Err(NodeError::LaneFull(1))),
            "past a lane's capacity held"
        );
        assert!(matches!(taken.try_recv(), Ok(Some(In::Measure(_)))));
        lanes.tell_held().unwrap();
        assert!(lanes.stream.is_empty());
        assert!(matches!(
            taken.try_recv(),
            Ok(Some(In::Believe(Change::Trusted { at_ns: 1, .. })))
        ));
        lanes.tell_stream(restarted()).unwrap();
        assert!(matches!(
            taken.try_recv(),
            Ok(Some(In::Believe(Change::Restarted { .. })))
        ));
        lanes.send(measured()).unwrap();
        lanes.tell_stream(trusted(3)).unwrap();
        drop((taken, given));
        assert!(matches!(lanes.tell_held(), Err(NodeError::ShardEnded(1))));
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
            .send(In::Propose(
                GROUP,
                put().encode().unwrap(),
                range.settings.max_entry_bytes,
                0,
            ))
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
                            leader
                                .send(In::Propose(GROUP, put().encode().unwrap(), max, 0))
                                .unwrap();
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

    /// A far root range's rows, which the membership reads, reach the node's shard at its first
    /// drive and again after each drive in which it applied an entry ([`Out::Rows`]), each as of
    /// the entry it applied; a range of another layer hands over none.
    #[test]
    fn a_far_root_ranges_rows_reach_the_nodes_shard_as_it_applies() {
        let rt = started(2, 2);
        let (dir, name_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let mut range = one_member();
        range.layer = Layer::Root;
        let log = log_in(dir.path(), &range);
        let replica = campaigning(&log, &range);
        let mut lanes = Lanes::start(rt.holder_of(rt.shard_ids()[1]).unwrap(), 1, 1, 8).unwrap();
        lanes.send(In::Adopt(GROUP, Box::new(replica))).unwrap();
        let (mut first, mut applied, until) = (None, 0, deadline());
        while first.is_none() || applied == 0 {
            match next(&mut lanes, "the rows of an entry applied", until, |_| false) {
                Out::Rows(group, rows) => {
                    assert_eq!(group, GROUP);
                    assert!(rows.members.is_empty(), "a cell made before grants");
                    first.get_or_insert(rows.applied);
                    applied = rows.applied;
                }
                Out::Seen(_, seen) if seen.applied > applied && first.is_some() => {
                    // The drive that applied it hands its rows over after what it shows.
                    let Out::Rows(_, rows) = next(&mut lanes, "its rows", until, |out| {
                        !matches!(out, Out::Rows(..))
                    }) else {
                        panic!("`next` gives only what it does not skip")
                    };
                    assert_eq!(rows.applied, seen.applied);
                    applied = rows.applied;
                }
                _ => {}
            }
        }
        assert_eq!(first, Some(0), "its first drive hands its rows over");
        drop(lanes);

        let name = one_member();
        let name_log = log_in(name_dir.path(), &name);
        let (mut lanes, _, _) = leading(&rt, &name_log, &name);
        lanes
            .send(In::Propose(
                GROUP,
                put().encode().unwrap(),
                name.settings.max_entry_bytes,
                0,
            ))
            .unwrap();
        let until = deadline();
        loop {
            match next(&mut lanes, "a name range's entry applied", until, |_| false) {
                Out::Rows(..) => panic!("a name range hands over no root rows"),
                Out::Applied(..) => break,
                _ => {}
            }
        }
        drop(lanes);
        rt.shutdown().unwrap();
        drop((log, name_log));
    }

    /// Node 1's replica of a one-member `range` on `log`, over `engine`, campaigning, moved to the
    /// second shard of `rt` before its first drive: its lanes once its shard shows it leading with
    /// its leader's first entry applied, after which it applies nothing unless proposed to, and
    /// what it shows then.
    fn far_leading(
        rt: &Runtime,
        log: &Log<DeviceFile>,
        range: &Range,
        mut engine: Model,
    ) -> (Lanes<Model, Stored>, Seen) {
        engine.persist().unwrap();
        let store = Answers::new(mantle_range::claim(log, GROUP, range).unwrap());
        let mut replica =
            mantle_range::open_shell(1, store, engine, range, 1, Elections::Suspicion).unwrap();
        replica.campaign().unwrap();
        let mut lanes = Lanes::start(rt.holder_of(rt.shard_ids()[1]).unwrap(), 1, 1, 8).unwrap();
        lanes.send(In::Adopt(GROUP, Box::new(replica))).unwrap();
        let until = deadline();
        loop {
            if let Out::Seen(_, seen) = next(&mut lanes, "the range leading", until, |_| false)
                && seen.leading
                && seen.applied > 0
            {
                return (lanes, seen);
            }
        }
    }

    /// An engine of the Name range's first rows, as `campaigning` makes one, and the rows `apply`
    /// writes through entries past it, all installed as of entry 0, where a replica opens.
    fn installed(apply: impl FnOnce(&mut Model)) -> Model {
        let mut written = Model::default();
        written.install(0, name::first(1).unwrap()).unwrap();
        apply(&mut written);
        let mut engine = Model::default();
        engine.install(0, written.image().unwrap()).unwrap();
        engine
    }

    /// The root range's word enlisting `node` in domain "d" with `volumes`.
    fn enlist(node: u64, volumes: Vec<(u128, u64)>) -> root::Command {
        root::Command::Enlist(root::Enlist {
            node,
            domain: "d".into(),
            volumes,
            max_nodes: 3,
            max_volumes: 3,
            at_ns: 0,
        })
    }

    /// What the shard answered of the table asked of it, the far range told as the node's shard
    /// tells it (`Node::took`), what it shows kept as the node keeps it.
    fn table_answered(lanes: &mut Lanes<Model, Stored>, far: &mut Far) -> Box<Facts> {
        let until = deadline();
        loop {
            match next(lanes, "the table answered", until, |_| false) {
                Out::Seen(_, seen) => far.seen = seen,
                Out::Table(group, facts) => {
                    assert_eq!(group, GROUP);
                    far.answered_table(facts);
                    assert!(!far.table_out, "its ask is out no more");
                    // The node reads it once (`Node::table`).
                    return far.table.take().unwrap().unwrap();
                }
                _ => {}
            }
        }
    }

    /// The host's volume table of a far root range is asked of the range's shard and answered
    /// as of the entry the range applied ([`In::Table`], [`Out::Table`]): every volume its engine
    /// holds. While the range shows it applied nothing past the table held, nothing is asked and
    /// the node reads the one it holds; once it applies an entry, which enlists a volume, the
    /// table is asked again, one ask at a time, and answered with that volume.
    #[test]
    fn a_far_root_ranges_volume_table_is_asked_once_it_applies_past_the_one_held() {
        let rt = started(2, 2);
        let dir = tempfile::tempdir().unwrap();
        let mut range = one_member();
        range.layer = Layer::Root;
        let log = log_in(dir.path(), &range);
        // Node 9 enlisted with volume 1, and a session registered for the entry proposed later.
        let mut session = 0;
        let engine = installed(|engine| {
            root::apply(engine, 1, &enlist(9, vec![(1, 1 << 30)])).unwrap();
            let register = Entry {
                at_ns: 0,
                commands: vec![Sessioned {
                    session: 0,
                    serial: 0,
                    unanswered: 0,
                    command: Command::Register {
                        owner: 1,
                        run: 1,
                        nth: 1,
                    },
                }],
            };
            let answers =
                mantle_meta::apply::apply_entry(engine, 2, &register, Layer::Root, &range.rules)
                    .unwrap();
            let [
                mantle_meta::wire::Answer::Registered {
                    session: registered,
                },
            ] = answers[..]
            else {
                panic!("{answers:?}")
            };
            session = registered;
        });
        let (mut lanes, seen) = far_leading(&rt, &log, &range, engine);
        let mut far = far(seen);
        ask_table(std::slice::from_ref(&lanes), &mut far, None).unwrap();
        assert!(far.table_out);
        // One at a time: asked again while out, nothing more is sent.
        ask_table(std::slice::from_ref(&lanes), &mut far, None).unwrap();
        let facts = table_answered(&mut lanes, &mut far);
        assert_eq!(
            facts.applied, far.seen.applied,
            "as of the entry it applied"
        );
        let volumes: Vec<(u128, u64)> = facts
            .facts
            .iter()
            .map(|f| (f.placed.volume, f.placed.node))
            .collect();
        assert_eq!(volumes, [(1, 9)]);

        // The range applied nothing since: the table held is read, nothing asked.
        let held = Some(facts.applied);
        ask_table(std::slice::from_ref(&lanes), &mut far, held).unwrap();
        assert!(!far.table_out, "the table held is the range's");

        let entry = Entry {
            at_ns: 0,
            commands: vec![Sessioned {
                session,
                serial: 1,
                unanswered: 0,
                command: Command::Root(enlist(10, vec![(2, 1 << 30)])),
            }],
        };
        lanes
            .send(In::Propose(
                GROUP,
                entry.encode().unwrap(),
                range.settings.max_entry_bytes,
                0,
            ))
            .unwrap();
        let until = deadline();
        while far.seen.applied <= facts.applied {
            match next(&mut lanes, "the enlistment applied", until, |_| false) {
                Out::Seen(_, seen) => far.seen = seen,
                Out::Applied(_, applied) => {
                    assert_eq!(
                        applied.answer,
                        mantle_meta::wire::Answer::Root(root::Outcome::Enlisted)
                    );
                }
                _ => {}
            }
        }
        ask_table(std::slice::from_ref(&lanes), &mut far, held).unwrap();
        assert!(far.table_out, "asked again once the range applied past it");
        let again = table_answered(&mut lanes, &mut far);
        assert_eq!(again.applied, far.seen.applied);
        let volumes: Vec<(u128, u64)> = again
            .facts
            .iter()
            .map(|f| (f.placed.volume, f.placed.node))
            .collect();
        assert_eq!(volumes, [(1, 9), (2, 10)]);
        drop(lanes);
        rt.shutdown().unwrap();
        drop(log);
    }

    /// Repair's three reads of a far Block range's rows, the next block, the next block on a
    /// volume, and a block's places, asked of its shard ([`In::Block`]) one at a time, answer as
    /// the range's engine answers them where the node's own task drives it: each taken once.
    #[test]
    fn repairs_reads_of_a_far_block_range_answer_as_its_engine_on_the_nodes_shard() {
        let rt = started(2, 2);
        let dir = tempfile::tempdir().unwrap();
        let mut range = one_member();
        range.layer = Layer::Block;
        let log = log_in(dir.path(), &range);
        let engine = installed(|engine| {
            for block in 1..=3u128 {
                let chunks = [7, 100 + block]
                    .into_iter()
                    .zip(0u16..)
                    .map(|(volume, index)| ChunkPlace {
                        volume,
                        key: mantle_chunk::ChunkKey {
                            block,
                            epoch: 1,
                            index,
                        },
                    })
                    .collect();
                let write = block::Command::Write {
                    block,
                    header: BlockHeader {
                        length: 3 << 20,
                        data: 1,
                        parity: 1,
                        chunk_len: 3 << 20,
                        crc32c: 1,
                    },
                    chunks,
                    file: 1,
                    handover_ns: u64::MAX / 2,
                    at_ns: 0,
                };
                let index = u64::try_from(block).unwrap();
                block::apply(engine, index, &write).unwrap();
            }
        });
        let mut here = engine.clone();
        let (mut lanes, seen) = far_leading(&rt, &log, &range, engine);
        let mut far = far(seen);
        let reads = [
            BlockRead::Next { after: None },
            BlockRead::Next { after: Some(2) },
            BlockRead::Next { after: Some(3) },
            BlockRead::OnVolume {
                volume: 7,
                after: None,
            },
            BlockRead::OnVolume {
                volume: 7,
                after: Some(1),
            },
            BlockRead::OnVolume {
                volume: 102,
                after: Some(2),
            },
            BlockRead::Places { block: 2 },
            BlockRead::Places { block: 4 },
        ];
        let mut found = 0;
        for read in reads {
            let shards = std::slice::from_ref(&lanes);
            assert_eq!(read_block(shards, &mut far, read).unwrap(), None, "asked");
            // One at a time: another read waits for it, unasked.
            let other = BlockRead::Places { block: 1 };
            assert_eq!(read_block(shards, &mut far, other).unwrap(), None);
            let until = deadline();
            loop {
                if let Out::Block(group, answered, rows) =
                    next(&mut lanes, "the read answered", until, |_| false)
                {
                    assert_eq!((group, answered), (GROUP, read), "the read out, alone");
                    far.answered_block(answered, rows);
                    break;
                }
            }
            let shards = std::slice::from_ref(&lanes);
            let rows = read_block(shards, &mut far, read).unwrap().unwrap();
            assert_eq!(rows, read.read(&mut here).unwrap(), "{read:?}");
            assert!(far.block.is_none(), "taken once");
            if !matches!(
                rows,
                BlockRows::Next(None) | BlockRows::OnVolume(None) | BlockRows::Places(None)
            ) {
                found += 1;
            }
        }
        assert_eq!(found, 5, "the reads that find a block find it");
        drop(lanes);
        rt.shutdown().unwrap();
        drop(log);
    }

    /// The volume table and each of repair's reads asked of a far range's shard whose lane holds
    /// all it takes are refused `LaneFull`, unasked: the node reads the table it holds and asks
    /// at its next read, and repair's walk waits and asks at its next turn. Of a shard the node
    /// has no lanes to, each is refused `ShardEnded`, which stops the node.
    #[test]
    fn a_full_lane_refuses_the_table_and_each_block_read_typed() {
        let (lanes, _ends) = unstarted(1);
        lanes.send(measured()).unwrap();
        let shards = vec![lanes];
        let dir = tempfile::tempdir().unwrap();
        let range = one_member();
        let log = log_in(dir.path(), &range);
        let mut far = far(Seen::of(&campaigning(&log, &range)));
        assert!(matches!(
            ask_table(&shards, &mut far, None),
            Err(NodeError::LaneFull(1))
        ));
        assert!(!far.table_out, "unasked");
        let reads = [
            BlockRead::Next { after: None },
            BlockRead::OnVolume {
                volume: 7,
                after: None,
            },
            BlockRead::Places { block: 1 },
        ];
        for read in reads {
            assert!(matches!(
                read_block(&shards, &mut far, read),
                Err(NodeError::LaneFull(1))
            ));
            assert!(far.block.is_none(), "unasked");
        }
        far.shard = 2;
        assert!(matches!(
            ask_table(&shards, &mut far, None),
            Err(NodeError::ShardEnded(2))
        ));
        for read in reads {
            assert!(matches!(
                read_block(&shards, &mut far, read),
                Err(NodeError::ShardEnded(2))
            ));
        }
    }

    /// What the node holds inline for each range is what derive counts (`node::slots_bytes`),
    /// counted by the counting allocator as the node and its shards past the first make it, at 0
    /// to 4,096 ranges placed over one shard and over three: the node's arena (`Owner::new`, a
    /// slot, a waker and two places a range), its table of its ranges (`node::Kept`) and the
    /// spread; the arenas and tables (`Kept`) of the shards past the first, which their ranges
    /// sum to as each is made at its own; and the node's entries for its far ranges (`Far`) and
    /// the handles its arena keeps of those that left it (`Here::left`), each pushed one at a
    /// time, an uninitialised entry of its size standing for each. The replica sits inline in its
    /// slot, so each arena holds each of its ranges' replicas at full size, emptied or not.
    #[test]
    fn a_ranges_slots_hold_what_derive_counts() {
        use std::mem::MaybeUninit;
        type Store = Answers<GroupStore<DeviceFile>>;
        type Machine = RangeMachine<mantle_range::Stored<DeviceFile>>;
        type Production = Replica<mantle_range::Stored<DeviceFile>, GroupStore<DeviceFile>>;
        let live = |counts: hyper_measure::alloc::Counts| u64::try_from(counts.live).unwrap();
        let over = |shards: usize| std::num::NonZeroUsize::new(shards).unwrap();
        let here = |shards: usize, n: usize| {
            let waker = Waker::noop().clone();
            hyper_measure::alloc::begin();
            let owner = Here::new(Owner::<Store, Machine, Unbounded>::new(vec![waker; n]));
            let ranges: Vec<super::super::Kept> = Vec::with_capacity(n);
            let spread = super::super::spread::Spread::over(over(shards), n);
            let counts = hyper_measure::alloc::end();
            drop((owner, ranges, spread));
            live(counts)
        };
        let far = |n: usize| {
            let waker = Waker::noop().clone();
            hyper_measure::alloc::begin();
            let owner = Owner::<Store, Machine, Unbounded>::new(vec![waker; n]);
            let ranges: Vec<Kept> = Vec::with_capacity(n);
            let mut far: Vec<MaybeUninit<Far>> = Vec::new();
            let mut left: Vec<MaybeUninit<Handle>> = Vec::new();
            for _ in 0..n {
                far.push(MaybeUninit::uninit());
                left.push(MaybeUninit::uninit());
            }
            let counts = hyper_measure::alloc::end();
            drop((owner, ranges, far, left));
            live(counts)
        };
        for n in [0, 1, 2, 3, 4, 5, 64, 1024, 4096] {
            let one = crate::node::slots_bytes(over(1), n).unwrap();
            assert_eq!(here(1, n), one, "{n}");
            let three = crate::node::slots_bytes(over(3), n).unwrap();
            assert_eq!(here(3, n) + far(n), three, "{n}");
        }
        eprintln!(
            "a range: {} on the node's shard, {} more past it; its slot {}, the replica {}, its \
             machine {}, the engine {}, its store {}; the node's entry {}, the shard's {}, the far \
             range's {}",
            here(1, 1) - here(1, 0),
            far(1),
            size_of::<(u32, Option<Production>, bool, Vec<PeerId>)>(),
            size_of::<Production>(),
            size_of::<Machine>(),
            size_of::<mantle_range::Stored<DeviceFile>>(),
            size_of::<GroupStore<DeviceFile>>(),
            size_of::<super::super::Kept>(),
            size_of::<Kept>(),
            size_of::<Far>(),
        );
    }
}
