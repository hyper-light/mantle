//! A shard's ranges, driven by a task on that shard (node.md §2.9, step 2).
//!
//! The node's shard, the first, keeps the endpoint, the plane, the liveness stream and the table of
//! ranges by shard. A shard past it runs a task of its own that holds the replicas the table places
//! there in an owner of its own (`hyper_durable::Owner`), made with that task's waker, so a range's
//! log answers wake the shard that drives it, and keeps that shard's deadlines. The two talk over
//! two bounded lanes of hyper-rt's channel: into the shard, each replica moved there before its
//! first drive, Raft messages, proposals, and the stream's changes and measurements ([`In`]); out
//! of it, Raft messages, applied answers, flushes, each range's peers and its expected election
//! ([`Out`]). The node's shard sends without waiting, a full lane refused, typed
//! (`NodeError::LaneFull`); the shard waits for room on its way out, its replicas' next turn held
//! until the node's shard takes what they gave. A shard whose task ended took its ranges with it,
//! which nothing else holds, so the node stops (`NodeError::ShardEnded`) and its next start
//! recovers them from the log (node.md §1.7).

use std::collections::BTreeSet;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper_durable::{Driven, Handle, Output, Owner, ReplicaError, Unbounded};
use hyper_liveness::{Change, Liveness, PeerId};
use hyper_raft::Timing;
use hyper_raft::proto::Message;
use hyper_rt::SlotHolder;
use hyper_rt::combine::{Either, race2};
use hyper_rt::sync::{ChannelReceiver, Sender, SyncError, channel};
use hyper_timing::{Ballot, ExchangeRtt, Trust};
use mantle_meta::engine::Engine;
use mantle_meta::wire::Entry;
use mantle_protocol::bounds;
use mantle_range::{Applied, LogStore, RangeMachine};

use super::NodeError;
use super::answers::Answers;

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
/// keeps pairs with for it, and its latest expected election.
pub(crate) struct Far {
    pub(crate) group: u128,
    pub(crate) shard: usize,
    pub(crate) peers: Vec<PeerId>,
    pub(crate) election: Option<Duration>,
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

/// The node's shard's ends of a range's shard's two lanes.
pub(crate) struct Lanes<E: Engine, S: LogStore> {
    shard: usize,
    capacity: usize,
    into: Sender<In<E, S>>,
    from: ChannelReceiver<Out>,
    /// What a look for the shard's output took and the node has yet to handle ([`Lanes::arrived`]).
    held: Option<Out>,
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
        })
    }
}

impl<E: Engine + 'static, S: LogStore> Lanes<E, S> {
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
                let handle = self.owner.insert(*replica).map_err(|_| NodeError::Full)?;
                self.ranges.push(Kept {
                    group,
                    handle,
                    wake: None,
                    peers,
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
        }
        Ok(())
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
        }
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
        owner: Owner::new(vec![waker; slots]),
        ranges: Vec::with_capacity(slots),
        deadlines: BTreeSet::new(),
        out: Output::default(),
        gave: Vec::new(),
        driven: Vec::new(),
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
        let now = hyper_rt::futures::now_ns();
        if stopped.is_none() {
            shard.schedule_due(now);
            stopped = shard.drive(now).err();
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
        while applied.is_none() {
            match lanes.from.blocking_recv().unwrap() {
                Out::Applied(group, answer) => applied = Some((group, answer)),
                Out::Flushed(..) => {
                    flushed += 1;
                    lanes
                        .send(In::Propose(GROUP, put(), range.settings.max_entry_bytes))
                        .unwrap();
                }
                Out::Stopped(error) => panic!("the shard stopped: {error}"),
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
}
