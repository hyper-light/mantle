//! A driving shard's task and its lanes (node.md §2.9 step 6e).
//!
//! A shard past the first that drives native requests runs a task of its own holding a [`Host`],
//! the jobs of the requests handed to it, started through that shard's control queue as the S3
//! listener's takers are, Normal as the S3 connections beside it. The node's task and it talk over
//! three bounded lanes of hyper-rt's channel.
//!
//! Into the shard ([`In`]): each request handed with its fields read, its operation and its door's
//! job half ([`Handover`]), at most the exchanges the transport admits at once (`parked_most`), one
//! past them refused for room, typed, and given back to run on the first shard; each volume table
//! the node reads, a settled copy, the latest superseding one the lane had no room for; that room
//! at a volume or in the transport was freed, held as a flag while the lane is full; and that a
//! piece of a chunk a job asked moved, dropped where the lane is full, its next piece telling it
//! again.
//!
//! Out of it ([`Out`]): each ask of its jobs, under a number of the shard's; each job's end, its
//! door's half riding it to the node's task, which holds the other half; and the put window's
//! samples. The shard waits for room on its way out, as a range's shard does.
//!
//! The answers, from the node's task to the shard, one an ask: the shard's host holds each kind of
//! ask to the node's bound for it (`command_asks`, `read_asks`, `chunk_asks`), refusing one past it
//! `NodeError::Asks`, which its job asks again at its next turn, and each ask holds its place from
//! its ask until its answer is taken, an abandoned one's until its answer comes and is dropped. So
//! the lane, made once and holding the three bounds' sum, is never full, an answer cannot be
//! refused, and an ask allocates nothing past its own value, where a `oneshot` an ask would make its
//! block and ring at each. The node's task holds an ask it has no room for, in order, rather than
//! answering it `Asks`, which the shard would ask again with nothing to wait on but the next
//! answer, and asks it once the node has room ([`Serve::room`]); one the node refuses otherwise is
//! answered its kind's refusal, which its job ends `Failed` on.
//!
//! The node's side is generic over [`Serve`], so a test serves a driving shard from its own thread
//! with a stub, and the node serves it by its own functions as it serves its own host.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use hyper_rt::SlotHolder;
use hyper_rt::combine::{Either, race2};
use hyper_rt::sync::{ChannelReceiver, Sender, SyncError, channel};
use hyper_transport::Refusal;
use mantle_meta::apply::Layer;

use super::host::{
    GatewaySettings, Handover, Host, JobAsk, Native, Outcome, Pending, Serve, Table,
};
use super::{ChunkOutcome, NodeError, Wall};
use crate::ask::{CommandOutcome, ReadOutcome};
use crate::command;

/// What the node's task hands a driving shard.
pub(crate) enum In {
    /// A native request with its fields read: its operation and its door's job half.
    Request(Box<Handover>),
    /// The volume table the node last read, a settled copy.
    Table(Box<Table>),
    /// Room at a volume or in the transport toward it was freed (`Serve::freed`).
    Freed,
    /// A piece of the chunk the shard's ask of this number asked moved (`Host::chunk_moved`).
    ChunkMoved(u64),
}

/// What a driving shard gives the node's task.
pub(crate) enum Out {
    /// A job's ask, under the number the shard gave it, which its answer comes back under.
    Ask(u64, JobAsk),
    /// A job ended: admitted at, charged, placed on, and its native door's half.
    Ended {
        admitted_ns: Option<u64>,
        charge: u64,
        driven: Option<usize>,
        half: Option<Box<Native>>,
    },
    /// A put's blocks recorded in this many nanoseconds (`Serve::committed`).
    Committed(u64),
    /// A put's body of this many bytes came in this many nanoseconds (`Serve::filled`).
    Filled(u64, u64),
    /// The shard's host failed, and its task ended: why.
    Stopped(NodeError),
}

/// What a driving shard's task is started with: the node's bound for each kind of ask, the
/// ranges' groups and their count (a range's layer and group do not change in a process), the
/// wall clock, the gateway's settings its jobs run by, shared with the node's, and the requests
/// its lane takes at once (`parked_most`).
pub(crate) struct Start {
    pub(crate) command_asks: usize,
    pub(crate) read_asks: usize,
    pub(crate) chunk_asks: usize,
    pub(crate) groups: Vec<(Layer, u128)>,
    pub(crate) ranges: usize,
    pub(crate) wall: Wall,
    pub(crate) gateway: Arc<GatewaySettings>,
    pub(crate) requests: usize,
}

impl Start {
    /// The asks the shard's host holds at once, every kind's: the answer lane's room.
    fn slots(&self) -> Result<usize, NodeError> {
        self.command_asks
            .checked_add(self.read_asks)
            .and_then(|asks| asks.checked_add(self.chunk_asks))
            .ok_or_else(|| NodeError::Bounds("a driving shard's asks pass a usize".into()))
    }
}

/// The node's task's ends of a driving shard's lanes.
pub(crate) struct Lanes {
    shard: usize,
    into: Sender<In>,
    answers: Sender<(u64, Outcome)>,
    from: ChannelReceiver<Out>,
    /// What a look for the shard's output took and the node has yet to handle ([`Lanes::arrived`]).
    held: Option<Out>,
    /// What the node's task takes from the shard in a turn at most: the lane's room.
    room: usize,
    /// The volume table the lane had no room for, the latest, told at the next turn.
    table: Option<Box<Table>>,
    /// Whether the shard is to be told room was freed, held while its lane was full.
    freed: bool,
    /// Asks the node had no room for, in order, each under the shard's number: at most the asks
    /// the shard's host holds.
    waiting: VecDeque<(u64, JobAsk)>,
    /// Asks the node holds for the shard, each under the shard's number and the node's: at most
    /// the asks the shard's host holds.
    asked: Vec<(u64, Pending)>,
}

impl Lanes {
    /// Starts a driving shard's task, the node's shard `shard` (numbered from its first), on the
    /// runtime shard `holder` names, through its control queue, as the S3 listener's lanes start
    /// their takers: a launcher that makes the task there, whose jobs' drivers need not cross, and
    /// spawns it Normal, as the S3 connections beside it run. Refused when a lane cannot be made
    /// or the shard takes no task now: gone, or its control queue full.
    pub(crate) fn start(holder: SlotHolder, shard: usize, start: Start) -> Result<Self, NodeError> {
        let (room, slots) = (start.requests, start.slots()?);
        let (into, taken) = channel(room)?;
        let (answers, answered) = channel(slots)?;
        let (given, from) = channel(room)?;
        hyper_rt::runtime::submit_to_holder(holder, async move {
            // A shard that refuses it drops the task with its lanes' ends, which the node reads as
            // the shard's end (`NodeError::ShardEnded`).
            let _ = hyper_rt::futures::spawn_detached_as(
                hyper_rt::coop::Class::Normal,
                run(taken, answered, given, start),
            );
        })?;
        Ok(Self {
            shard,
            into,
            answers,
            from,
            held: None,
            room,
            table: None,
            freed: false,
            waiting: VecDeque::with_capacity(slots),
            asked: Vec::with_capacity(slots),
        })
    }

    /// The node's shard the lanes reach.
    pub(crate) fn shard(&self) -> usize {
        self.shard
    }

    /// Hands the shard a request with its fields read, without waiting: none once sent, and the
    /// request given back where its lane holds all it takes, or its task is gone, to run on the
    /// first shard.
    pub(crate) fn hand(&self, request: Box<Handover>) -> Option<Box<Handover>> {
        let refused = self.into.try_send(In::Request(request)).err()?;
        let (SyncError::Full(sent) | SyncError::Closed(sent) | SyncError::NotOnShardThread(sent)) =
            refused;
        match sent {
            In::Request(request) => Some(request),
            // What was sent was a request: nothing else comes back.
            In::Table(_) | In::Freed | In::ChunkMoved(_) => None,
        }
    }

    /// Tells the shard the volume table the node read, a settled copy: held where its lane has no
    /// room, the latest superseding one held.
    pub(crate) fn tell_table(&mut self, table: Box<Table>) {
        self.table = Some(table);
        self.tell_held();
    }

    /// Tells the shard room was freed: held as a flag where its lane has no room.
    pub(crate) fn tell_freed(&mut self) {
        self.freed = true;
        self.tell_held();
    }

    /// Tells the shard a piece of the chunk the node's ask `ask` asked for it moved: dropped where
    /// its lane has no room, its next piece telling it again.
    pub(crate) fn tell_moved(&self, ask: u64) {
        if let Some((number, _)) = self
            .asked
            .iter()
            .find(|(_, pending)| *pending == Pending::Chunk(ask))
        {
            let _ = self.into.try_send(In::ChunkMoved(*number));
        }
    }

    /// Whether the shard gave something the node's task has yet to take, or ended. A look that
    /// finds neither asks the lane to wake the polling task when either comes, as a range's
    /// shard's lanes do (`shard::Lanes::arrived`).
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

    /// The table and the freed flag held, told while the lane has room.
    fn tell_held(&mut self) {
        if let Some(table) = self.table.take()
            && let Err(SyncError::Full(In::Table(table))) = self.into.try_send(In::Table(table))
        {
            self.table = Some(table);
        }
        if self.freed && self.into.try_send(In::Freed).is_ok() {
            self.freed = false;
        }
    }

    /// The node's task's turn of the lanes, served by `node`: what the shard gave taken, at most
    /// the lane's room, each ask asked of the node where it has room and held in order where not,
    /// each job's end told the node and its door's half given to `keep`, and each answer the
    /// node holds sent to the shard. A shard whose task ended is refused `ShardEnded`, which stops
    /// the node, as one whose ranges' task ended does.
    pub(crate) fn serve<N: Serve>(
        &mut self,
        node: &mut N,
        mut keep: impl FnMut(Box<Native>),
    ) -> Result<(), NodeError> {
        self.tell_held();
        for _ in 0..self.room {
            let out = match self.held.take() {
                Some(out) => out,
                None => match self.from.try_recv() {
                    Ok(Some(out)) => out,
                    Ok(None) => break,
                    Err(_) => return Err(NodeError::ShardEnded(self.shard)),
                },
            };
            self.take(node, out, &mut keep)?;
        }
        while let Some((_, ask)) = self.waiting.front() {
            if !node.room(ask) {
                break;
            }
            let Some((number, ask)) = self.waiting.pop_front() else {
                break;
            };
            self.ask(node, number, ask)?;
        }
        let mut at = 0;
        while let Some(&(number, pending)) = self.asked.get(at) {
            match node.outcome(pending) {
                Some(outcome) => {
                    self.asked.swap_remove(at);
                    self.answer(number, outcome)?;
                }
                None => at = at.saturating_add(1),
            }
        }
        Ok(())
    }

    /// One thing the shard gave.
    fn take<N: Serve>(
        &mut self,
        node: &mut N,
        out: Out,
        keep: &mut impl FnMut(Box<Native>),
    ) -> Result<(), NodeError> {
        match out {
            Out::Ask(number, ask) => {
                if self.waiting.is_empty() && node.room(&ask) {
                    self.ask(node, number, ask)?;
                } else {
                    self.waiting.push_back((number, ask));
                }
            }
            Out::Ended {
                admitted_ns,
                charge,
                driven,
                half,
            } => {
                if let Some(half) = node.job_ended(admitted_ns, charge, driven, [], half) {
                    keep(half);
                }
            }
            Out::Committed(ns) => node.committed(ns),
            Out::Filled(bytes, ns) => node.filled(bytes, ns),
            Out::Stopped(error) => return Err(error),
        }
        Ok(())
    }

    /// Asks the node what the shard's job asked, its kind's refusal answered at once where the
    /// node refuses it for another cause than room, which the job ends `Failed` on.
    fn ask<N: Serve>(&mut self, node: &mut N, number: u64, ask: JobAsk) -> Result<(), NodeError> {
        let refusal = refusal_of(&ask);
        match node.ask(ask) {
            Ok(pending) => {
                self.asked.push((number, pending));
                Ok(())
            }
            Err(_) => self.answer(number, refusal),
        }
    }

    /// Sends an answer, which its ask's place in the lane holds: a full lane is a broken bound,
    /// refused `LaneFull`, which stops the node.
    fn answer(&self, number: u64, outcome: Outcome) -> Result<(), NodeError> {
        self.answers
            .try_send((number, outcome))
            .map_err(|refused| match refused {
                SyncError::Full(_) => NodeError::LaneFull(self.shard),
                SyncError::Closed(_) | SyncError::NotOnShardThread(_) => {
                    NodeError::ShardEnded(self.shard)
                }
            })
    }
}

/// What an ask the node refuses for another cause than room is answered: its kind's refusal.
fn refusal_of(ask: &JobAsk) -> Outcome {
    match ask {
        JobAsk::Command(..) => Outcome::Command(CommandOutcome::Refused(command::Status::Refused)),
        JobAsk::Read(..) => Outcome::Read(ReadOutcome::Refused(command::Status::Refused)),
        JobAsk::Put { .. } | JobAsk::Get { .. } => {
            Outcome::Chunk(ChunkOutcome::Lost(Refusal::Closed))
        }
    }
}

/// A count of each kind of ask, each held to its bound: commands, reads, chunks.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Kinds {
    commands: usize,
    reads: usize,
    chunks: usize,
}

/// The kind of an ask.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Command,
    Read,
    Chunk,
}

impl Kind {
    fn of(ask: &JobAsk) -> Self {
        match ask {
            JobAsk::Command(..) => Self::Command,
            JobAsk::Read(..) => Self::Read,
            JobAsk::Put { .. } | JobAsk::Get { .. } => Self::Chunk,
        }
    }

    fn pending(pending: Pending) -> Self {
        match pending {
            Pending::Command(_) => Self::Command,
            Pending::Read(_) => Self::Read,
            Pending::Chunk(_) => Self::Chunk,
        }
    }
}

impl Kinds {
    /// The count of `kind`.
    fn of(&mut self, kind: Kind) -> &mut usize {
        match kind {
            Kind::Command => &mut self.commands,
            Kind::Read => &mut self.reads,
            Kind::Chunk => &mut self.chunks,
        }
    }

    /// The count of `kind`, read.
    fn get(self, kind: Kind) -> usize {
        match kind {
            Kind::Command => self.commands,
            Kind::Read => self.reads,
            Kind::Chunk => self.chunks,
        }
    }
}

/// A driving shard's host served over its lanes ([`Serve`]).
struct Served {
    /// What the host's step gave, sent after it, waiting for room: each a job's ask, end or
    /// sample, so at most the asks it holds and its jobs' ends and samples.
    gave: Vec<Out>,
    /// Answers come and not yet taken: at most the asks held.
    answers: Vec<(Pending, Outcome)>,
    /// Asks let go before their answers came, each dropped as it comes: at most the asks held.
    abandoned: Vec<Pending>,
    /// The asks held of each kind, and the node's bound for each.
    held: Kinds,
    bounds: Kinds,
    /// The number the next ask takes.
    next: u64,
    table: Option<Box<Table>>,
    freed: bool,
    groups: Vec<(Layer, u128)>,
    ranges: usize,
    wall: Wall,
}

impl Served {
    /// The answer to the ask of `number` came: kept for its job, or dropped where its job let go.
    fn came(&mut self, number: u64, outcome: Outcome) {
        let pending = match &outcome {
            Outcome::Command(_) => Pending::Command(number),
            Outcome::Read(_) => Pending::Read(number),
            Outcome::Chunk(_) => Pending::Chunk(number),
        };
        if let Some(at) = self.abandoned.iter().position(|p| *p == pending) {
            self.abandoned.swap_remove(at);
            self.let_go(pending);
            return;
        }
        self.answers.push((pending, outcome));
    }

    /// An ask's place given back: its answer taken, or dropped.
    fn let_go(&mut self, pending: Pending) {
        let held = self.held.of(Kind::pending(pending));
        *held = held.saturating_sub(1);
    }
}

impl Serve for Served {
    fn ask(&mut self, ask: JobAsk) -> Result<Pending, NodeError> {
        if !self.room(&ask) {
            return Err(NodeError::Asks);
        }
        let number = self.next;
        self.next = self.next.wrapping_add(1);
        let pending = match &ask {
            JobAsk::Command(..) => Pending::Command(number),
            JobAsk::Read(..) => Pending::Read(number),
            JobAsk::Put { .. } | JobAsk::Get { .. } => Pending::Chunk(number),
        };
        let held = self.held.of(Kind::pending(pending));
        *held = held.saturating_add(1);
        self.gave.push(Out::Ask(number, ask));
        Ok(pending)
    }

    fn outcome(&mut self, pending: Pending) -> Option<Outcome> {
        let at = self.answers.iter().position(|(p, _)| *p == pending)?;
        let (_, outcome) = self.answers.swap_remove(at);
        self.let_go(pending);
        Some(outcome)
    }

    fn answered(&self, pending: Pending) -> bool {
        self.answers.iter().any(|(p, _)| *p == pending)
    }

    fn abandon(&mut self, asks: impl IntoIterator<Item = Pending>) {
        for pending in asks {
            if let Some(at) = self.answers.iter().position(|(p, _)| *p == pending) {
                self.answers.swap_remove(at);
                self.let_go(pending);
            } else {
                self.abandoned.push(pending);
            }
        }
    }

    fn table(&mut self) -> Result<&Table, NodeError> {
        self.table
            .as_deref()
            .ok_or_else(|| NodeError::Bounds("no volume table handed to the shard yet".into()))
    }

    fn group_of(&self, layer: Layer) -> Option<u128> {
        self.groups
            .iter()
            .find(|(of, _)| *of == layer)
            .map(|(_, group)| *group)
    }

    fn ranges(&self) -> usize {
        self.ranges
    }

    fn command_room(&self) -> bool {
        self.held.commands < self.bounds.commands
    }

    fn command_asks(&self) -> usize {
        self.bounds.commands
    }

    fn room(&self, ask: &JobAsk) -> bool {
        let kind = Kind::of(ask);
        self.held.get(kind) < self.bounds.get(kind)
    }

    fn wall_ns(&self) -> u64 {
        self.wall.now_ns()
    }

    fn freed(&mut self) -> bool {
        std::mem::take(&mut self.freed)
    }

    /// The node's task counts what it moves itself; the shard's host counts its jobs' own.
    fn moved(&self) -> u64 {
        0
    }

    fn read_fields(&mut self, _: &mut super::host::Job) -> Result<bool, NodeError> {
        Err(NodeError::Bounds(
            "a request is handed to a driving shard with its fields read".into(),
        ))
    }

    /// The S3 door's jobs stay on the first shard: no sample comes here.
    fn took(&mut self, _: crate::door::Paced) {}

    fn committed(&mut self, ns: u64) {
        self.gave.push(Out::Committed(ns));
    }

    fn filled(&mut self, bytes: u64, ns: u64) {
        self.gave.push(Out::Filled(bytes, ns));
    }

    fn job_ended(
        &mut self,
        admitted_ns: Option<u64>,
        charge: u64,
        driven: Option<usize>,
        asks: impl IntoIterator<Item = Pending>,
        half: Option<Box<Native>>,
    ) -> Option<Box<Native>> {
        self.abandon(asks);
        self.gave.push(Out::Ended {
            admitted_ns,
            charge,
            driven,
            half,
        });
        None
    }
}

/// A driving shard's task: takes what the node's task hands it and the answers it sends, at most
/// each lane's room a turn, steps its host, and hands back what the host gave, waiting for room.
/// Waits for the first of a request, an answer and its host's own due (`Host::due`). Ends when the
/// node's task's end of a lane is gone, or once it handed back why its host stopped.
async fn run(
    mut taken: ChannelReceiver<In>,
    mut answers: ChannelReceiver<(u64, Outcome)>,
    given: Sender<Out>,
    start: Start,
) {
    let Ok(slots) = start.slots() else {
        return;
    };
    let Start {
        command_asks,
        read_asks,
        chunk_asks,
        groups,
        ranges,
        wall,
        gateway,
        requests,
    } = start;
    // A driving shard's host keeps no doors: each job's half rides its end to the node's task.
    let mut host = Host::new(0, Some(gateway));
    let mut served = Served {
        gave: Vec::with_capacity(slots),
        answers: Vec::with_capacity(slots),
        abandoned: Vec::with_capacity(slots),
        held: Kinds::default(),
        bounds: Kinds {
            commands: command_asks,
            reads: read_asks,
            chunks: chunk_asks,
        },
        next: 0,
        table: None,
        freed: false,
        groups,
        ranges,
        wall,
    };
    let (mut handed, mut answer) = (None, None);
    loop {
        let mut handed_in = handed.take();
        for _ in 0..=requests {
            let sent = match handed_in.take() {
                Some(sent) => sent,
                None => match taken.try_recv() {
                    Ok(Some(sent)) => sent,
                    Ok(None) => break,
                    Err(_) => return,
                },
            };
            match sent {
                In::Request(request) => host.take_handed(request, None),
                In::Table(table) => served.table = Some(table),
                In::Freed => served.freed = true,
                In::ChunkMoved(number) => host.chunk_moved(number),
            }
        }
        let mut answer_in = answer.take();
        for _ in 0..=slots {
            let (number, outcome) = match answer_in.take() {
                Some(came) => came,
                None => match answers.try_recv() {
                    Ok(Some(came)) => came,
                    Ok(None) => break,
                    Err(_) => return,
                },
            };
            served.came(number, outcome);
        }
        let stopped = host.step(&mut served).err();
        for out in served.gave.drain(..) {
            if given.send(out).await.is_err() {
                return;
            }
        }
        if let Some(error) = stopped {
            let _ = given.send(Out::Stopped(error)).await;
            return;
        }
        let now = hyper_rt::futures::now_ns();
        let due = host.due(&served);
        if due.is_some_and(|due| due <= now) {
            hyper_rt::futures::yield_now().await;
            continue;
        }
        let lanes = race2(taken.recv(), answers.recv());
        let woke = match due {
            Some(due) => {
                match race2(lanes, hyper_rt::futures::sleep(due.saturating_sub(now))).await {
                    Either::First(woke) => Some(woke),
                    Either::Second(_) => None,
                }
            }
            None => Some(lanes.await),
        };
        match woke {
            Some(Either::First(Ok(sent))) => handed = Some(sent),
            Some(Either::Second(Ok(came))) => answer = Some(came),
            Some(Either::First(Err(_)) | Either::Second(Err(_))) => return,
            None => {}
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use mantle_chunk::ChunkKey;

    use super::*;
    use crate::node::host::tests::Stub;

    /// The shard's ends of its lanes: what it is handed, the answers, and what it gives.
    pub(crate) type Ends = (
        ChannelReceiver<In>,
        ChannelReceiver<(u64, Outcome)>,
        Sender<Out>,
    );

    /// A driving shard's lanes whose task is not started, and the shard's ends of them.
    pub(crate) fn unstarted(requests: usize, slots: usize) -> (Lanes, Ends) {
        let (into, taken) = channel(requests).unwrap();
        let (answers, answered) = channel(slots).unwrap();
        let (given, from) = channel(requests).unwrap();
        let lanes = Lanes {
            shard: 1,
            into,
            answers,
            from,
            held: None,
            room: requests,
            table: None,
            freed: false,
            waiting: VecDeque::with_capacity(slots),
            asked: Vec::with_capacity(slots),
        };
        (lanes, (taken, answered, given))
    }

    /// The shard's side of its lanes, holding `bound` asks of each kind.
    fn served(bound: usize) -> Served {
        Served {
            gave: Vec::with_capacity(4 * bound),
            answers: Vec::with_capacity(3 * bound),
            abandoned: Vec::with_capacity(3 * bound),
            held: Kinds::default(),
            bounds: Kinds {
                commands: bound,
                reads: bound,
                chunks: bound,
            },
            next: 0,
            table: None,
            freed: false,
            groups: Vec::new(),
            ranges: 1,
            wall: Wall::From(0),
        }
    }

    /// A chunk's get, which holds nothing on the heap: an ask whose own value allocates nothing.
    fn chunk_get() -> JobAsk {
        JobAsk::Get {
            at: 1,
            volume: 1,
            key: ChunkKey {
                block: 1,
                epoch: 0,
                index: 0,
            },
            span: (0, 1),
            remaining_ns: 1,
        }
    }

    /// Each lane holds its stated bound: the shard's host holds each kind of ask to the node's
    /// bound, one past it refused `Asks`, so the answers lane is never full; an abandoned ask holds
    /// its place until its answer comes and is dropped; the node's task holds an ask it has no room
    /// for, in order, and asks it once it has; the volume table the lane had no room for is held,
    /// the latest superseding it, and room freed is held as a flag, each told once the lane has
    /// room; a chunk's move is dropped where the lane is full; and a shard whose task ended is
    /// refused `ShardEnded`.
    #[test]
    fn a_driving_shards_lanes_hold_their_bounds() {
        // The shard's host: two chunk asks at most, the third refused for room.
        let mut shard = served(2);
        let first = shard.ask(chunk_get()).unwrap();
        let second = shard.ask(chunk_get()).unwrap();
        assert!(matches!(shard.ask(chunk_get()), Err(NodeError::Asks)));
        assert!(shard.command_room(), "each kind is held to its own bound");
        // The first let go before its answer: its place held until the answer comes.
        shard.abandon([first]);
        assert!(!shard.room(&chunk_get()));
        shard.came(0, Outcome::Chunk(ChunkOutcome::Stored));
        assert!(shard.room(&chunk_get()), "its answer came and was dropped");
        assert!(shard.outcome(first).is_none());
        shard.came(1, Outcome::Chunk(ChunkOutcome::Stored));
        assert!(shard.answered(second));
        assert!(shard.outcome(second).is_some());
        assert_eq!(shard.held, Kinds::default());

        // The node's side: an ask it has no room for is held, in order, then asked.
        let (mut lanes, (taken, mut answered, given)) = unstarted(1, 2);
        let mut stub = Stub::chunks();
        stub.no_room = true;
        assert!(given.try_send(Out::Ask(7, chunk_get())).is_ok());
        lanes.serve(&mut stub, |_| {}).unwrap();
        assert_eq!(
            (stub.asked, lanes.waiting.len()),
            (0, 1),
            "held, not answered"
        );
        assert!(matches!(answered.try_recv(), Ok(None)));
        stub.no_room = false;
        lanes.serve(&mut stub, |_| {}).unwrap();
        assert_eq!((stub.asked, lanes.waiting.len()), (1, 0));
        assert!(matches!(
            answered.try_recv(),
            Ok(Some((7, Outcome::Chunk(_))))
        ));

        // Into the shard: a lane of one. The table held where it is full, the latest kept; the
        // freed flag held; a chunk's move dropped.
        assert!(lanes.into.try_send(In::Freed).is_ok());
        let table = || Box::new(Table::empty(3));
        lanes.tell_table(table());
        lanes.tell_table(Box::new(Table::empty(4)));
        lanes.tell_freed();
        lanes.asked.push((9, Pending::Chunk(5)));
        lanes.tell_moved(5);
        assert_eq!(lanes.table.as_ref().map(|t| t.applied()), Some(4));
        assert!(lanes.freed);
        let mut taken = taken;
        assert!(matches!(taken.try_recv(), Ok(Some(In::Freed))));
        assert!(matches!(taken.try_recv(), Ok(None)), "the move was dropped");
        lanes.serve(&mut stub, |_| {}).unwrap();
        assert!(matches!(taken.try_recv(), Ok(Some(In::Table(t))) if t.applied() == 4));
        lanes.serve(&mut stub, |_| {}).unwrap();
        assert!(matches!(taken.try_recv(), Ok(Some(In::Freed))));
        assert!(lanes.table.is_none() && !lanes.freed);

        // A shard whose task ended stops the node.
        drop(given);
        assert!(matches!(
            lanes.serve(&mut stub, |_| {}),
            Err(NodeError::ShardEnded(1))
        ));
        drop((taken, answered));
    }

    /// An ask and its answer cross the lanes asking the allocator for nothing past the ask's own
    /// value, in the steady state, on either side: the shard numbers it and holds its place, the
    /// node's task asks it and sends its answer into the lane made once, and the shard takes it.
    #[test]
    fn an_ask_over_the_lanes_allocates_nothing() {
        const ASKS: usize = 64;
        let (mut lanes, (_taken, mut answered, given)) = unstarted(2, 3);
        let mut shard = served(1);
        let mut stub = Stub::chunks();
        let mut ask = |shard: &mut Served, lanes: &mut Lanes, stub: &mut Stub| {
            let pending = shard.ask(chunk_get()).unwrap();
            for out in shard.gave.drain(..) {
                assert!(given.try_send(out).is_ok());
            }
            lanes.serve(stub, |_| {}).unwrap();
            let Ok(Some((number, outcome))) = answered.try_recv() else {
                panic!("the answer came");
            };
            shard.came(number, outcome);
            assert!(shard.outcome(pending).is_some());
        };
        ask(&mut shard, &mut lanes, &mut stub);
        hyper_measure::alloc::begin();
        for _ in 0..ASKS {
            ask(&mut shard, &mut lanes, &mut stub);
        }
        let counts = hyper_measure::alloc::end();
        assert_eq!((counts.allocations, counts.reallocations), (0, 0));
        assert_eq!(stub.asked, ASKS as u64 + 1);
    }
}
