//! One member of one group: the core, its group's log, its state machine and its budget, driven
//! by its owner (`docs/durable.md` §2–§7).
//!
//! The owner steps what arrives into the replica and calls [`Replica::drive`] when it stepped
//! something or when the log's answer woke it. Each drive takes the log's answers, applies what
//! the commit fence allows, and takes at most one `Ready` (the owner's quantum, §7), whose write
//! it submits with the owner's waker. Nothing is held between drives but the writes out, each
//! with what waits for it.
//!
//! The invariants of `docs/durable.md` §3, as this shell keeps them (the core keeps the rest):
//!
//! - **I1, I2.** What a `Ready` gives to send at once leaves at once; the core gives a leader's
//!   only while its term and vote are durable. Every other message leaves with the write it
//!   waits for ([`Out::messages`]), once that write and every earlier one is durable: answers are
//!   taken in submission order, and a write refused for room releases nothing until it is made
//!   again.
//! - **I3.** The core is told a write is durable only from its answer
//!   ([`RawNode::on_persist`]).
//! - **I4.** Only what the core gives to apply is applied; R-4 gives what is committed and
//!   durable here.
//! - **I5, the commit fence.** A change of configuration, and an entry the state machine acts on
//!   at its next start, is applied only once the durable commit `C_d` covers it: the greatest of
//!   the commit the durable writes stated and the state machine's durable index. Until then it
//!   waits with everything after it ([`Replica::behind_fence`]), the replica takes no `Ready`,
//!   and a write states the commit: one out already, or one of the hard state alone.
//! - **I6.** Answers are what the state machine returns as it applies.
//! - **I7.** Writes are submitted in order and their answers taken in order; every write states
//!   the commit the core holds when it is laid out, which names only entries it or an earlier
//!   write holds, and a sole voter states the last entry of its own term, which its own write
//!   commits. A stated commit past what the log holds once the write is durable fences the
//!   replica.
//! - **I8.** A snapshot is made durable by the state machine before the write that moves the
//!   log's start to it; a compaction never passes the state machine's durable index; opening
//!   finishes an install the log never recorded.
use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::task::Waker;
use std::time::Duration;

use hyper_liveness::{Liveness, PeerId};
use hyper_raft::progress::ProgressState;
use hyper_raft::proto::{
    self, ConfChange, ConfChangeV2, ConfState, Entry, EntryType, HardState, Message, MessageType,
    Snapshot, SnapshotMetadata,
};
use hyper_raft::wire::Record;
use hyper_raft::{
    Config, Elections, Lost, RawNode, SnapshotStatus, StateRole, StorageError, Timing,
};
use hyper_timing::{Ballot, Flushes, Span, Trust, inflight_window};

use crate::budget::{Budget, Unbounded};
use crate::compaction::Compaction;
use crate::held::Held;
use crate::machine::{Fatal, StateMachine};
use crate::patience::{self, Patience};
use crate::store::{Entries, EntryRef, Fault, Health, LogStore, Point, Write};

/// How a replica runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    /// The core's settings. Its `applied` is the state machine's durable index, its
    /// `limits.readies_in_flight` the store's depth and its `elections` this setting's, whatever
    /// is given here.
    pub core: Config,
    /// How the replica elects, fixed for as long as it runs: by suspicion (timing step L-2,
    /// `docs/timing.md` §2.3), with pre-vote and check-quorum on or the replica does not open; or
    /// on ticks, which the owner gives ([`Replica::tick`]) until it elects by suspicion
    /// (`docs/durable.md` §8). Stated by every owner: there is no default.
    pub elections: Elections,
    /// The owner's period: a commit no write has stated while the applied index ran past the
    /// durable commit for this long is written then, so a member that stops reopens with what it
    /// applied (`docs/durable.md` §4.1; focal F17's period).
    pub quiet: Duration,
    /// The group's disk mode ([`Disk`]): one setting, the same for every member of the group, from
    /// which the replica takes the core's `apply_unpersisted` and the law of its patience with its
    /// own disk (`docs/durable.md` §4.2), and its owner the liveness stream's mode
    /// ([`Replica::disk`]).
    pub disk: Disk,
}

/// How a group's members take their own disks (kiln's CP-T2.2, Focal's condition 1): one setting
/// for the group, stated by every member's owner from the group's definition, so its members never
/// differ in it, and a heartbeat says it, so a member of the other mode is refused, typed
/// (`hyper_liveness::Refusal::OtherMode`), and never misjudged.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Disk {
    /// Today's: each peer's heartbeats wait on its flush proof, and a leader applies only what a
    /// majority's disks hold with its own among them, as the core's settings say.
    #[default]
    Flushed,
    /// Each member judges its peers' processes apart from their disks
    /// (`hyper_liveness::Liveness::set_disk_apart`); a leader commits and applies on its followers'
    /// disks while its own lags (`Config::apply_unpersisted`), and hands the lead on once the
    /// backlog its disk doesn't hold would fill its budget's room within a hand-over, a voter there
    /// to take it: a patience by law, no time stated ([`Replica::disk_fill`], `docs/durable.md`
    /// §4.2).
    Apart,
}

impl Disk {
    /// Whether the mode judges each peer's process apart from its disk.
    pub const fn apart(self) -> bool {
        matches!(self, Self::Apart)
    }
}

/// Why a replica is fenced: it takes no call until its owner reopens it from what is durable.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Cause {
    /// The log failed a write: what the device holds is unknown.
    #[error("{0}")]
    Write(Fault),
    /// The core's own state no longer adds up.
    #[error("the core: {0}")]
    Core(hyper_raft::Error),
    /// The state machine failed.
    #[error("{0}")]
    Machine(Fatal),
    /// The core or the state machine unwound inside a call.
    #[error("a call unwound")]
    Unwound,
    /// The shell's own bookkeeping no longer adds up.
    #[error("the shell: {0}")]
    Invariant(&'static str),
}

/// What a call on a replica refuses.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ReplicaError {
    /// The core refused; nothing changed.
    #[error("refused: {0}")]
    Refused(hyper_raft::Error),
    /// A write waits for room in the log: the replica takes no part until its owner frees room
    /// and drives again (`docs/durable.md` §2.4). Nothing changed.
    #[error("stalled for room in the log")]
    Stalled,
    /// The member's log may lack entries it acknowledged: it does not campaign.
    #[error("the member's log may lack entries it acknowledged")]
    Marked,
    /// The owner's budget could not hold the input's bytes; nothing changed.
    #[error("the budget could not hold {0} bytes")]
    Budget(u64),
    /// The replica is fenced and must be reopened.
    #[error("fenced: {0}")]
    Fenced(Cause),
}

/// Why a replica did not open.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum OpenError {
    /// The log refused or failed.
    #[error("the log: {0}")]
    Log(Fault),
    /// The log could not be read.
    #[error("the log: {0}")]
    Storage(StorageError),
    /// The core would not open.
    #[error("the core: {0}")]
    Core(hyper_raft::Error),
    /// The state machine failed.
    #[error("{0}")]
    Machine(Fatal),
    /// The log starts past what the state machine holds durably (I8): entries the machine needs
    /// are gone.
    #[error("the log starts at {start} past the state machine's {machine}")]
    StartPastMachine {
        /// The log's start.
        start: u64,
        /// The state machine's durable index.
        machine: u64,
    },
}

/// What a drive gave out, in buffers the owner keeps and reuses.
#[derive(Debug)]
pub struct Output<A> {
    /// Messages to send, in order.
    pub messages: Vec<Message>,
    /// What the state machine answered as it applied, in order.
    pub answers: Vec<A>,
    /// Reads a quorum confirmed and the replica has applied far enough to serve: each read's
    /// context and the index it was confirmed at.
    pub reads: Vec<(Vec<u8>, u64)>,
    /// What this member proposed by the fast track and another entry took the index of, in
    /// order (`hyper_raft::Ready::displaced`): no member applies it, so its proposer proposes it
    /// again or answers that it was not taken. The core gives at most its bound on proposals in
    /// one `Ready` (`Limits::proposals`).
    pub displaced: Vec<Entry>,
}

impl<A> Default for Output<A> {
    fn default() -> Self {
        Self {
            messages: Vec::new(),
            answers: Vec::new(),
            reads: Vec::new(),
            displaced: Vec::new(),
        }
    }
}

impl<A> Output<A> {
    /// Empties the buffers, keeping their room.
    pub fn clear(&mut self) {
        self.messages.clear();
        self.answers.clear();
        self.reads.clear();
        self.displaced.clear();
    }
}

/// What a drive left.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Driven {
    /// Driving again now would do more without waiting for an answer from the log: the owner
    /// queues the replica for its next quantum.
    pub more: bool,
    /// Writes out.
    pub out: usize,
    /// A write waits for room: what the log said.
    pub stalled: Option<Fault>,
    /// When to drive the replica again though nothing arrives (`Replica::deadline`): a
    /// campaign's delay, a leader's beat while its group has work in flight, a transfer's end.
    /// None for a group with nothing in flight: it needs no wake at all.
    pub wake: Option<u64>,
    /// The latest write of the replica's that became durable in this drive: when it was
    /// submitted and when its answer was taken. The node's liveness stream takes it as the flush
    /// a heartbeat proves (`hyper_liveness::Liveness::on_durable`, `Write::Log`).
    pub flushed: Option<(u64, u64)>,
    /// When the oldest write of the replica's still out on the log was submitted, if one is
    /// ([`Replica::out_since`]): what the node's liveness stream judges its own disk's wait from in
    /// the disk mode (`hyper_liveness::Liveness::on_pending`, Focal's 5a).
    pub out_since: Option<u64>,
}

/// The writes a replica made, by what they were for: what an owner reads to see what its group
/// costs its log.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Writes {
    /// `Ready`s whose write held something.
    pub readies: u64,
    /// Of `readies`, those whose write carried entries, a snapshot or proposals; the rest held a
    /// hard state alone (a term, a vote, a promise, or a commit the fence needed). mantle:
    /// vendor/UPSTREAM.md.
    pub entries: u64,
    /// `Ready`s that held nothing to make durable: no write.
    pub empty: u64,
    /// Writes of the commit alone the fence asked for.
    pub fenced: u64,
    /// Writes of the commit alone after a quiet period.
    pub quiet: u64,
    /// Compactions' starts.
    pub starts: u64,
    /// Writes the store held for its owner ([`Fault::Held`]), each counted once as it was held.
    pub held: u64,
}

/// A write the replica made, oldest first, with what waits for it.
#[derive(Clone, Debug)]
struct Out {
    kind: Kind,
    state: State,
    /// Messages to send once this write and every earlier one is durable.
    messages: Vec<Message>,
    /// The last entry of the core's log when the write was laid out, its index and term: what its
    /// messages acknowledge stands on it. Refused, the write's messages leave with the write made
    /// again only while the core's log still holds that entry ([`Replica::make_again`]).
    tip: (u64, u64),
    /// The commit its hard state stated.
    commit: Option<u64>,
    /// Whether it changed the term or the vote: its time to durable is a vote's flush.
    vote: bool,
    /// Whether it moves the log's start: a snapshot's install or a compaction.
    start: bool,
    submitted: u64,
}

/// What a write is of.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// The `Ready` of this number.
    Ready(u64),
    /// The commit alone.
    Commit,
    /// A compaction's start, and the bytes its machine stated an image of its state there takes,
    /// if it states them ([`StateMachine::image_bytes`]): what the compaction rule weighs the log
    /// against once the start is durable ([`Compaction`]). No image is made for a compaction.
    Start(Option<u64>),
}

/// Where a write is.
#[derive(Clone, Debug, PartialEq, Eq)]
enum State {
    /// With the log.
    Submitted,
    /// It held nothing to make durable: it is durable once every write before it is.
    Empty,
    /// The log refused it as it was submitted.
    Refused(Fault),
}

/// Writes refused for room, waiting to be made again.
#[derive(Clone, Debug)]
struct Stall {
    fault: Fault,
    /// Writes that were out behind the refused one when its refusal was taken: each is refused
    /// too. A compaction's start submitted after is not, and may be durable.
    behind: usize,
    /// Room may have been freed since the refusal: the replica's own compaction is durable, or
    /// its owner said so ([`Replica::resume`]). Made again before, the writes would only be
    /// refused again, a refusal a drive.
    freed: bool,
    /// The last `Ready` refused: its notice says every refused one is durable.
    ready: Option<u64>,
    /// Each refused `Ready`'s messages, with the tip of the log they were made against
    /// ([`Out::tip`]): at most one a write out.
    messages: Vec<((u64, u64), Vec<Message>)>,
}

/// What committed entries given to apply wait for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Wait {
    /// The commit fence: the durable commit to cover the first.
    Commit,
    /// The next drive: they are past a drive's page.
    Page,
    /// Room in the state machine, which wakes the owner for the drive that applies them
    /// ([`StateMachine::apply`]).
    Room,
}

/// Where a walk of committed entries stopped.
enum Stop {
    /// Everything given was applied.
    Done,
    /// This entry waits for the commit fence.
    Fence(u64),
    /// This entry is past the drive's page: it waits for the next drive.
    Page(u64),
    /// The state machine has no room for this entry now: it waits for the drive its owner is
    /// woken for ([`StateMachine::apply`]).
    Room(u64),
    /// A change of configuration, applied by the core before the walk goes on.
    Change(Point, ConfChangeV2),
    /// The state machine or the entry failed.
    Failed(Cause),
}

/// One member of one group (module docs). A copy is a second member in the same state, for a
/// simulation that forks its world (`docs/sim.md` §3.7); its log, machine and budget must be values
/// to copy, so no owner of a device's handle has one.
#[derive(Clone)]
pub struct Replica<L: LogStore, M: StateMachine, B: Budget = Unbounded> {
    node: RawNode<Held<L>>,
    machine: M,
    budget: B,
    /// What the budget holds for this replica.
    charged: u64,
    depth: usize,
    quiet: Duration,
    writes: VecDeque<Out>,
    stall: Option<Stall>,
    /// The hard state of the last write submitted, or what the log held at open.
    issued: HardState,
    /// The commit the durable writes stated.
    logged_commit: u64,
    /// The last entry applied.
    applied: Point,
    /// The bytes of the applied entries the log holds past its start, as the core counts an
    /// entry's ([`EntryRef::encoded_bytes`]): what a compaction frees, and what the compaction
    /// rule weighs ([`Compaction`]).
    compactable: u64,
    /// What [`Replica::compactable`] counted of each entry, oldest first, the first that of the
    /// entry at `sizes_from`: the applied entries the log holds past its start. A start made
    /// durable takes out of the count what it let go of from here, never from the store, which
    /// holds an applied entry its cache let go only on its device. Read back through the log's
    /// owner, behind its writes, that count held focal's event bus 51 to 637 ms a page, on the
    /// thread that serves its heartbeats, and its followers were suspected 23 to 27 times a run
    /// (fedora, 2026-10-19: a compaction's start leaves the applied entries past the image's
    /// point, which is the part's window's floor, hundreds of megabytes behind its apply point).
    /// Each entry's count in four bytes, as the log keeps its entry's length (`hyper_log`'s slot):
    /// an entry is no larger than a message the transport carries.
    sizes: VecDeque<u32>,
    /// The index of the entry whose count `sizes` holds first.
    sizes_from: u64,
    /// The bytes of the image the log was last compacted to, as the machine states them
    /// ([`StateMachine::image_bytes`]), or of the one this member installed; at open, of what it
    /// opened with; none from a machine that states none.
    imaged: Option<u64>,
    /// The images made for members behind the log's start since the member opened
    /// ([`Replica::images_made`]).
    images_made: u64,
    /// The entries to keep of a compaction that waits on the state machine's checkpoint out
    /// ([`StateMachine::persist`] answered false): each drive asks the machine again, and the
    /// start moves at the one that finds it durable ([`Replica::persisting`]).
    persisting: Option<u64>,
    /// What the core was last told was applied.
    told_applied: u64,
    /// The index of the entry that made the configuration.
    conf_index: u64,
    /// Committed entries given to apply that wait: the first and last, for what `wait` says.
    fence: Option<(u64, u64)>,
    /// What the entries that wait wait for.
    wait: Wait,
    /// The bytes of entries a drive applies at most, as the core counts them
    /// (`EntryRef::encoded_bytes`): its committed page (`max_committed_size_per_ready`), or one
    /// entry larger than it.
    page: u64,
    /// The bytes of entries applied in this drive.
    drive_applied: u64,
    /// Reads confirmed, waiting to be applied far enough: index and context.
    reads: VecDeque<(u64, Vec<u8>)>,
    /// Snapshot reports that came while stalled: one a member.
    reports: Vec<(u64, bool)>,
    fenced: Option<Cause>,
    /// When the applied index first ran past the durable commit with nothing out.
    quiet_since: Option<u64>,
    flushes: Flushes,
    last_durable: Option<u64>,
    writes_made: Writes,
    /// The latest write made durable since the last drive ended ([`Driven::flushed`]).
    flushed: Option<(u64, u64)>,
    /// What the replica measures for its patience with its own disk in the disk mode; none in
    /// today's (`docs/durable.md` §4.2).
    patience: Option<Patience>,
    /// The owner's clock at the latest drive: what the patience's end is read at between drives.
    driven_at: u64,
    /// The group's disk mode, as the replica was opened with it ([`Settings::disk`]).
    disk: Disk,
    /// The hand-overs its patience with its disk began.
    handed_over: u64,
    /// The drives at which its patience ended with no voter to hand over to: none trusted whose disk
    /// isn't slow.
    heirless: u64,
    /// Since when, unbroken, a leader has had a voter to hand the lead to: one its detectors trust
    /// whose disk isn't judged slow; none, no such voter at its last drive.
    heir_since: Option<u64>,
    /// The voters whose disks the owner's detectors judge slow, apart from their processes
    /// (`hyper_liveness::Change::DiskSlow`): at most the group's voters.
    slow_disks: Vec<u64>,
    /// The catch-up's lists, kept between drives (`Replica::stream_catch_up`): the members' next
    /// indexes, the pages the store holds read, and the members waiting at one, or at a snapshot
    /// just made (`Replica::send_made`).
    lows: Vec<u64>,
    pages: Vec<u64>,
    waiting: Vec<u64>,
    /// A page of a member's catch-up is being read or waits its turn: its answer wakes the owner.
    catching_up: bool,
}

/// A value no other opening of a member states, with overwhelming likelihood: the keys std's
/// hasher draws from the operating system once a thread and advances at every draw, so that two
/// draws in one process differ and draws in two processes are independent
/// (`hyper_raft::Config::incarnation`). Never zero, which states none.
fn fresh_incarnation() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u8(0);
    hasher.finish().max(1)
}

/// The term of an entry the state machine applied, as a point.
fn point_of(entry: &EntryRef<'_>) -> Point {
    Point {
        index: entry.index,
        term: entry.term,
    }
}

/// The change a committed entry states, read where it lies; none for one that states none.
fn change_of(entry: &EntryRef<'_>) -> Result<ConfChangeV2, Cause> {
    let decoded = match entry.kind {
        EntryType::EntryNormal => return Err(Cause::Invariant("a normal entry read as a change")),
        EntryType::EntryConfChange if entry.data.is_empty() => {
            Ok(proto::joint(&ConfChange::default()))
        }
        EntryType::EntryConfChange => ConfChange::decode(entry.data).map(|c| proto::joint(&c)),
        EntryType::EntryConfChangeV2 if entry.data.is_empty() => Ok(ConfChangeV2::default()),
        EntryType::EntryConfChangeV2 => ConfChangeV2::decode(entry.data),
    };
    decoded.map_err(|_| Cause::Invariant("a committed change does not decode"))
}

/// Bytes as the budget counts them.
fn bytes(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

/// Whether `message` brings the core something it keeps: entries, a snapshot, or a context (a
/// read's, which the core holds until it is confirmed).
fn keeps_something(message: &Message) -> bool {
    !message.entries.is_empty() || message.snapshot.is_some() || !message.context.is_empty()
}

/// Whether `message` answers what this member asked, which it answers with nothing.
fn answers(message: &Message) -> bool {
    matches!(
        message.msg_type,
        MessageType::MsgAppendResponse
            | MessageType::MsgHeartbeatResponse
            | MessageType::MsgRequestVoteResponse
            | MessageType::MsgRequestPreVoteResponse
    )
}

/// Whether a write under a configuration of these halves may be answered at its frame's own flush
/// (`Write::at_flush`): in each half, the voters but any one are a quorum, `n - 1 > n / 2`, which
/// three voters or more meet and one or two do not (mantle docs/design/durability.md §7.5). The
/// outgoing half counts only in a joint configuration, where it is not empty.
fn others_a_quorum(voters: &[u64], outgoing: &[u64]) -> bool {
    let enough = |half: &[u64]| {
        half.len()
            .checked_sub(1)
            .is_some_and(|others| others > half.len() / 2)
    };
    enough(voters) && (outgoing.is_empty() || enough(outgoing))
}

/// Every member a configuration names.
fn names(configuration: &ConfState, member: u64) -> bool {
    [
        &configuration.voters,
        &configuration.learners,
        &configuration.voters_outgoing,
        &configuration.learners_next,
    ]
    .iter()
    .any(|ids| ids.contains(&member))
}

impl<L: LogStore, M: StateMachine, B: Budget> Replica<L, M, B> {
    /// Opens a member on what `log` and `machine` hold durably (`docs/durable.md` §4.3).
    pub fn open(settings: &Settings, mut log: L, machine: M, budget: B) -> Result<Self, OpenError> {
        let durable = machine.durable();
        let view = repair_at_open(&mut log, durable)?;
        // What the member has yet to apply it reads back from its store, which may keep it.
        log.keep_after(durable.index);
        let depth = log.depth();
        let mut config = settings.core.clone();
        config.applied = durable.index;
        // The group's disk mode: a leader applies on its followers' disks while its own lags.
        if settings.disk.apart() {
            config.apply_unpersisted = true;
        }
        // An incarnation no other opening of this member states (core step R-8), unless the
        // owner gave one.
        if config.incarnation == 0 {
            config.incarnation = fresh_incarnation();
        }
        config.limits.readies_in_flight = depth;
        config.elections = settings.elections;
        // The core keeps what the log may lack and acts on it (`docs/durable.md` §5): it judges
        // votes by it, and tells a leader that counts the entries that they are lost (R-5).
        config.lost = match view.health {
            Health::Marked(mark) => Some(Lost {
                index: mark.index,
                term: mark.term,
            }),
            Health::Whole => None,
        };
        // A walk of the log reads a page the size of what one `Ready` gives to apply.
        let page = config.max_committed_size_per_ready;
        let held = Held::new(log, machine.configuration().clone(), view.hard_state, page);
        let node = RawNode::new(&config, held).map_err(OpenError::Core)?;
        let mut writes = VecDeque::new();
        // Room for every write out: the core's depth and one write of the shell's own.
        writes
            .try_reserve_exact(depth.saturating_add(1))
            .map_err(|_| OpenError::Core(hyper_raft::Error::Memory))?;
        let mut replica = Self {
            node,
            machine,
            budget,
            charged: 0,
            depth,
            quiet: settings.quiet,
            writes,
            stall: None,
            issued: view.hard_state,
            logged_commit: view.hard_state.commit,
            applied: durable,
            compactable: 0,
            sizes: VecDeque::new(),
            sizes_from: durable.index.saturating_add(1),
            imaged: None,
            images_made: 0,
            persisting: None,
            told_applied: durable.index,
            conf_index: durable.index,
            fence: None,
            wait: Wait::Commit,
            page,
            drive_applied: 0,
            reads: VecDeque::new(),
            reports: Vec::new(),
            fenced: None,
            quiet_since: None,
            flushes: Flushes::new(),
            last_durable: None,
            writes_made: Writes::default(),
            flushed: None,
            patience: settings.disk.apart().then(Patience::default),
            driven_at: 0,
            disk: settings.disk,
            handed_over: 0,
            heirless: 0,
            heir_since: None,
            slow_disks: Vec::new(),
            lows: Vec::new(),
            pages: Vec::new(),
            waiting: Vec::new(),
            catching_up: false,
        };
        // The log is weighed against the image of what the member opened with, as its machine
        // states it. None is made here: a log compacted before the restart serves a lagging
        // member by an image made when the core asks for one ([`Replica::make_wanted`]).
        replica.imaged = replica.machine.image_bytes();
        replica
            .count_applied(view.start.index)
            .map_err(|e| OpenError::Core(hyper_raft::Error::Storage(e)))?;
        replica.settle();
        Ok(replica)
    }

    /// Plants `mutant` in the replica's core, or takes the one planted out: a defect a test build
    /// shows its checks catch (`hyper_raft::Mutant`; the `mutants` feature, which no consumer
    /// enables).
    #[cfg(feature = "mutants")]
    pub fn plant(&mut self, mutant: Option<hyper_raft::Mutant>) {
        self.node.plant(mutant);
    }

    /// This member.
    pub fn id(&self) -> u64 {
        self.node.raft.id()
    }

    /// The member's term.
    pub fn term(&self) -> u64 {
        self.node.raft.term()
    }

    /// The member it believes leads, zero for none.
    pub fn leader(&self) -> u64 {
        self.node.raft.leader_id()
    }

    /// Whether it leads.
    pub fn is_leader(&self) -> bool {
        self.node.raft.state() == StateRole::Leader
    }

    /// The last entry applied.
    pub fn applied(&self) -> Point {
        self.applied
    }

    /// The durable commit `C_d` (`docs/durable.md` §4.1): the greatest of the commit the durable
    /// writes stated and the state machine's durable index.
    pub fn durable_commit(&self) -> u64 {
        self.logged_commit.max(self.machine.durable().index)
    }

    /// The commit the member's durable writes stated.
    pub fn logged_commit(&self) -> u64 {
        self.logged_commit
    }

    /// Whether this member's writes may be answered at their frame's own flush (`Write::at_flush`):
    /// under both the configuration it counts by, the newest its log states, and the one applied,
    /// so that a change in the log and not applied answers no write the other would not.
    fn at_flush(&self) -> bool {
        [
            self.node.raft.configuration(),
            self.node.raft.applied_configuration(),
        ]
        .into_iter()
        .all(|c| others_a_quorum(c.voters(), c.outgoing()))
    }

    /// The configuration as of the last entry applied.
    pub fn configuration(&self) -> &ConfState {
        &self.node.store().configuration
    }

    /// Committed entries that wait for the commit fence: the first and the last.
    pub fn behind_fence(&self) -> Option<(u64, u64)> {
        self.fence.filter(|_| self.wait == Wait::Commit)
    }

    /// Whether committed entries wait for room in the state machine: its owner drives the
    /// replica again when the machine's work wakes it ([`StateMachine::apply`]).
    pub fn waits_for_room(&self) -> bool {
        self.fence.is_some() && self.wait == Wait::Room
    }

    /// Writes out.
    pub fn in_flight(&self) -> usize {
        self.writes.len()
    }

    /// Whether a page of a member's catch-up was being read, or waited its turn, as the last drive
    /// ended: its answer wakes the owner ([`LogStore::catch_up`]).
    pub fn catching_up(&self) -> bool {
        self.catching_up
    }

    /// Whether a write waits for room.
    pub fn is_stalled(&self) -> bool {
        self.stall.is_some()
    }

    /// Why the replica is fenced, if it is.
    pub fn fenced(&self) -> Option<&Cause> {
        self.fenced.as_ref()
    }

    /// The point through which the member's log may lack entries it acknowledged: the core's,
    /// which ends it once its durable log holds what it marks.
    pub fn mark(&self) -> Option<Point> {
        self.node.raft.lost().map(|lost| Point {
            index: lost.index,
            term: lost.term,
        })
    }

    /// The flushes of the writes that made a term or vote durable, submit to durable, on the
    /// owner's clock: what hyper-timing's ballot charges a vote (`docs/timing.md` §2.3).
    pub fn flushes(&self) -> &Flushes {
        &self.flushes
    }

    /// When the log last answered a write durable: the evidence a node heartbeats on
    /// (`docs/timing.md` §2.1, L-3).
    pub fn last_durable(&self) -> Option<u64> {
        self.last_durable
    }

    /// The core, to read.
    pub fn core(&self) -> &RawNode<Held<L>> {
        &self.node
    }

    /// The state machine, to read.
    pub fn machine(&self) -> &M {
        &self.machine
    }

    /// The group's log, for its owner's own use of it: its counts, or a device's fault
    /// injection in a test. A write submitted through it, past the shell, is not one the shell
    /// orders or answers.
    pub fn log_mut(&mut self) -> &mut L {
        &mut self.node.store_mut().log
    }

    /// The group's log, read: its counts, or what a test's device holds.
    pub fn log(&self) -> &L {
        &self.node.store().log
    }

    /// The state machine, for its owner's own use of it, between drives.
    pub fn machine_mut(&mut self) -> &mut M {
        &mut self.machine
    }

    /// The writes this replica made since it opened, by what they were for.
    pub fn writes(&self) -> Writes {
        self.writes_made
    }

    /// The owner's period from now on ([`Settings::quiet`]): for an owner whose period is a
    /// measure that moves, as an event-driven owner's is the interval its liveness stream sends
    /// at. A quiet period already begun is judged by the new one at the next drive.
    pub fn set_quiet(&mut self, quiet: Duration) {
        self.quiet = quiet;
    }

    /// The bytes the budget holds for this replica.
    pub fn charged(&self) -> u64 {
        self.charged
    }

    /// Takes back the state machine, as a crash does with the process that held it; the budget
    /// is given back what it held. The log's handle goes with the core: the core gives no store
    /// back (a `RawNode::into_store` is asked of the core, `docs/durable.md` §11), and a
    /// hyper-log group is claimed again from its log.
    pub fn into_machine(mut self) -> M {
        self.budget.release(self.charged);
        self.machine
    }

    /// Whether this member leads and every voter of its configuration has said its durable
    /// commit reaches the entry that made it: each follower's answers carry its durable commit
    /// (core step R-6), and the leader counts its own. Once true, any one member may be lost and
    /// the others still elect under this configuration (mantle's `configuration_known`).
    pub fn configuration_known(&self) -> bool {
        let tracker = self.node.raft.tracker();
        let id = self.id();
        self.is_leader()
            && self.configuration().voters.iter().all(|&voter| {
                if voter == id {
                    self.durable_commit() >= self.conf_index
                } else {
                    tracker
                        .get(voter)
                        .is_some_and(|p| p.committed_index >= self.conf_index)
                }
            })
    }

    /// Whether this member leads and `member` has confirmed holding everything it knows
    /// committed.
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

    /// Runs `call` inside the unwind boundary (focal's `guarded_in`): an unwind of the core or
    /// the state machine fences the replica, and is reported, never propagated.
    fn guarded<T>(
        &mut self,
        call: impl FnOnce(&mut Self) -> Result<T, ReplicaError>,
    ) -> Result<T, ReplicaError> {
        if let Some(cause) = &self.fenced {
            return Err(ReplicaError::Fenced(cause.clone()));
        }
        match catch_unwind(AssertUnwindSafe(|| call(&mut *self))) {
            Ok(outcome) => outcome,
            Err(payload) => {
                // The payload is the unwound code's own, so it is dropped inside a boundary too:
                // one whose drop unwinds again is left unfreed, one allocation, rather than unwind.
                if let Err(again) = catch_unwind(AssertUnwindSafe(move || drop(payload))) {
                    let _ = Box::leak(again);
                }
                Err(self.fence(Cause::Unwound))
            }
        }
    }

    /// Fences the replica: every write out is dropped with what waited for it, and every call
    /// after answers `Fenced` until the owner reopens it from what is durable.
    fn fence(&mut self, cause: Cause) -> ReplicaError {
        self.writes.clear();
        self.stall = None;
        self.fence = None;
        self.wait = Wait::Commit;
        self.persisting = None;
        self.reads.clear();
        self.reports.clear();
        self.fenced = Some(cause.clone());
        ReplicaError::Fenced(cause)
    }

    /// A refusal of the core is the caller's to hear; a fatal error fences.
    fn heard<T>(&mut self, outcome: hyper_raft::Result<T>) -> Result<T, ReplicaError> {
        match outcome {
            Ok(value) => Ok(value),
            Err(error) if error.is_fatal() => Err(self.fence(Cause::Core(error))),
            Err(error) => Err(ReplicaError::Refused(error)),
        }
    }

    /// What the replica asks of the core itself must not be refused: any error fences.
    fn must<T>(&mut self, outcome: hyper_raft::Result<T>) -> Result<T, ReplicaError> {
        outcome.map_err(|error| self.fence(Cause::Core(error)))
    }

    /// A failure of the state machine fences.
    fn machine_did<T>(&mut self, outcome: Result<T, Fatal>) -> Result<T, ReplicaError> {
        outcome.map_err(|fatal| self.fence(Cause::Machine(fatal)))
    }

    /// The replica takes an input: it is not fenced (checked by the boundary) and not stalled.
    fn takes(&self) -> Result<(), ReplicaError> {
        match self.stall {
            Some(_) => Err(ReplicaError::Stalled),
            None => Ok(()),
        }
    }

    /// Reserves an input's bytes before it changes anything. One the budget refuses is asked
    /// again once the core has given back the room it holds spare (`Raft::trim`): a backlog's
    /// slots, kept empty once it drained, held a member past its budget for good, refusing the
    /// entries it lacked (kiln's CP-T2.2, every disk held past the patience).
    fn reserve(&mut self, len: u64) -> Result<(), ReplicaError> {
        if B::BOUNDED {
            if !self.budget.reserve(len) {
                self.node.raft.trim();
                self.settle();
                if !self.budget.reserve(len) {
                    return Err(ReplicaError::Budget(len));
                }
            }
            self.charged = self.charged.saturating_add(len);
        }
        Ok(())
    }

    /// Whether the replica takes `message` with no room in its budget: an answer to what this
    /// member asked that brings the core nothing it keeps (no entries, no snapshot, no context),
    /// which it answers with nothing, and which what it changes the core's `Limits` bound. A request
    /// is reserved for: its answer waits behind the writes out, and a held disk piled the answers
    /// to requests taken with no room past the budget (777 KB of a member's 128 KiB, the harness's
    /// every disk held but the leader's).
    fn takes_without_room(&self, message: &Message) -> bool {
        !keeps_something(message) && answers(message)
    }

    /// Brings what the budget holds for this replica to what it holds: the core's resident
    /// bytes and the shell's.
    fn settle(&mut self) {
        if !B::BOUNDED {
            return;
        }
        let held = self.held_bytes();
        if held > self.charged {
            self.budget.charge(held.saturating_sub(self.charged));
        } else {
            self.budget.release(self.charged.saturating_sub(held));
        }
        self.charged = held;
    }

    /// The bytes the replica holds: the core's, and the messages, reads and reports the shell
    /// keeps, and its count of the applied entries the log holds.
    fn held_bytes(&self) -> u64 {
        let messages = |messages: &[Message]| {
            messages
                .iter()
                .fold(0usize, |sum, m| sum.saturating_add(proto::message_bytes(m)))
        };
        let writes = self.writes.iter().fold(0usize, |sum, out| {
            sum.saturating_add(messages(&out.messages))
                .saturating_add(std::mem::size_of::<Out>())
        });
        let stalled = self.stall.as_ref().map_or(0, |stall| {
            stall
                .messages
                .iter()
                .fold(0usize, |sum, (_, held)| sum.saturating_add(messages(held)))
        });
        let reads = self.reads.iter().fold(0usize, |sum, (_, context)| {
            sum.saturating_add(context.capacity())
                .saturating_add(std::mem::size_of::<(u64, Vec<u8>)>())
        });
        // What the store keeps for this replica's reads is this replica's to bound.
        let kept = usize::try_from(self.node.store().log.kept_bytes()).unwrap_or(usize::MAX);
        // The count of each applied entry the log holds, the room it has.
        let sizes = self
            .sizes
            .capacity()
            .saturating_mul(std::mem::size_of::<u32>());
        bytes(
            self.node
                .raft
                .resident_bytes()
                .saturating_add(writes)
                .saturating_add(stalled)
                .saturating_add(reads)
                .saturating_add(kept)
                .saturating_add(sizes),
        )
    }

    /// The owner's detectors suspect `member`'s node (timing step L-2, `docs/timing.md` §2.1):
    /// a follower that knows it for its leader campaigns after its delay, a leader they leave no
    /// quorum steps down. Taken while stalled or marked, as it changes nothing durable: the
    /// replica's campaigns are held meanwhile (`docs/durable.md` §8), and what it believes of its
    /// peers is current when they are let go. A fenced replica takes nothing; its owner tells
    /// the reopened one what its detectors believe.
    pub fn suspect(&mut self, member: u64) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            // The same detector decides whose held answers a leader counts (core step R-8).
            let held = r.node.suspect_held(member);
            r.heard(held)?;
            let told = r.node.suspect(member);
            r.heard(told)
        })
    }

    /// The owner's detectors trust `member`'s node again.
    pub fn trust(&mut self, member: u64) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            let held = r.node.trust_held(member);
            r.heard(held)?;
            let told = r.node.trust(member);
            r.heard(told)
        })
    }

    /// Judged apart from its disk, `member`'s disk is slow, as its heartbeats say
    /// (`hyper_liveness::Change::DiskSlow`, kiln's CP-T2.2): what the replica trusts of its process
    /// is unchanged, and while it is slow it is named heir to no hand-over of the leader's patience
    /// with its own disk (`docs/durable.md` §4.2). Kept for the members the configuration names and
    /// let go as one leaves it (Focal's 5b), so bounded by the configuration: a learner's verdict
    /// stands as it is made a voter.
    pub fn disk_slow(&mut self, member: u64) -> Result<(), ReplicaError> {
        if names(self.configuration(), member) && !self.slow_disks.contains(&member) {
            self.slow_disks.push(member);
        }
        Ok(())
    }

    /// `member`'s disk is no longer slow (`hyper_liveness::Change::DiskCaughtUp`).
    pub fn disk_caught_up(&mut self, member: u64) -> Result<(), ReplicaError> {
        self.slow_disks.retain(|slow| *slow != member);
        Ok(())
    }

    /// Whether the owner's detectors judge `member`'s disk slow ([`Replica::disk_slow`]).
    pub fn disk_is_slow(&self, member: u64) -> bool {
        self.slow_disks.contains(&member)
    }

    /// The owner's detectors saw `member`'s node start again, a new incarnation: trusted, and
    /// leading nothing it led before it stopped.
    pub fn restarted(&mut self, member: u64) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            let held = r.node.trust_held(member);
            r.heard(held)?;
            let told = r.node.restarted(member);
            r.heard(told)
        })
    }

    /// The owner's detectors took `member`'s node's announcement that its run ends: suspected at
    /// once, its held answers not counted (core step R-8), ordered to campaign never, and what it
    /// sends that could take the group's leader dropped, until its node is heard again
    /// (`hyper_raft::RawNode::stopped`).
    pub fn stopped(&mut self, member: u64) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            let held = r.node.suspect_held(member);
            r.heard(held)?;
            let told = r.node.stopped(member);
            r.heard(told)
        })
    }

    /// Stops this member's held answers ahead of a planned stop (core step R-8): its promise drops
    /// in a later write once its disk holds what it answered for, and the owner stops it once
    /// [`Replica::held_settled`], so that it opens unmarked.
    pub fn quiet_held(&mut self) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            let told = r.node.quiet_held();
            r.heard(told)
        })
    }

    /// Whether this member states no promise, durable or out: a quieted member may stop.
    pub fn held_settled(&self) -> bool {
        self.node.held_settled()
    }

    /// What the path to `member` carries in a round trip, as the node's transport measures it: its
    /// congestion window to the member's node (mantle note 32 R16). The core keeps in flight to the
    /// member twice that (`hyper_timing::inflight_window`), what the path carries over the two
    /// round trips a lost append takes to repair. Said whenever the transport's measure
    /// moves; false, and nothing changed, for a member the group does not name, or for a carriage
    /// so large that the window would have no bound where the core counts no messages
    /// (`RawNode::set_inflight_bytes`). Until it is said, a member is sent what the core's settings
    /// give (`Config::max_inflight_bytes`).
    pub fn set_carriage(&mut self, member: u64, carried: u64) -> Result<bool, ReplicaError> {
        self.guarded(|r| Ok(r.node.set_inflight_bytes(member, inflight_window(carried))))
    }

    /// Where catching up the learner `member` stands, for the owner to promote it once ready
    /// (`hyper_raft::CatchUp`, thesis §4.2.1, mantle note 32 R13): its rounds of replication are
    /// judged against an election, the time `Timing::election` says the group's takes. Promoting it
    /// is the owner's change to propose.
    pub fn catch_up(&mut self, member: u64) -> Result<hyper_raft::CatchUp, ReplicaError> {
        self.guarded(|r| {
            let said = r.node.catch_up(member);
            r.heard(said)
        })
    }

    /// What the owner's measurements give the group's elections: the span hyper-timing's law
    /// chose over the measured paths to its voters, and their round tail (`hyper_raft::Timing`,
    /// `Timing::of`). Given again whenever the law's ballot moves.
    pub fn set_timing(&mut self, timing: Timing) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            let set = r.node.set_timing(timing);
            r.heard(set)
        })
    }

    /// The other members of the group, each once: the node pairs the node's liveness stream keeps
    /// for this group (`hyper_liveness::Liveness::attach`).
    pub fn peers(&self) -> impl Iterator<Item = PeerId> + '_ {
        let id = self.id();
        self.node
            .raft
            .configuration()
            .members()
            .filter(move |member| *member != id)
    }

    /// Tells the core what the node's liveness stream believes of every other member now: on
    /// opening, and on reopening a fenced replica, before any change reaches it.
    pub fn believe_all(&mut self, liveness: &Liveness) -> Result<(), ReplicaError> {
        let peers: Vec<PeerId> = self.peers().collect();
        for peer in peers {
            self.believe_of(liveness, peer)?;
        }
        Ok(())
    }

    /// Tells the core what the node's liveness stream believes of `peer` now: its run's announced
    /// stop, a suspicion, or trust; and the replica whether the stream judges its disk slow, a
    /// verdict told while the replica shared no group with the peer having reached it as no change.
    pub fn believe_of(&mut self, liveness: &Liveness, peer: PeerId) -> Result<(), ReplicaError> {
        if liveness.disk_slow(peer) == Some(true) {
            self.disk_slow(peer)?;
        } else {
            self.disk_caught_up(peer)?;
        }
        if liveness.stopped(peer) {
            self.stopped(peer)
        } else if liveness.trust(peer) == Some(Trust::Suspected) {
            self.suspect(peer)
        } else {
            self.trust(peer)
        }
    }

    /// The group's timing by hyper-timing's law over what the node's liveness stream measured
    /// (`docs/timing.md` §2.3, §2.9): the ballot over the echoed round trips to the group's
    /// voters (`Liveness::round_trip`), the mean flush of this replica's vote writes or, before
    /// one, of the node's log writes (`Liveness::flush_mean`), and the node's timer granularity;
    /// given to the core when it moved. The span, for the leader's pair's `T_E`
    /// (`Liveness::set_election`); none before a quorum's paths and the granularity are measured,
    /// when the core draws no delay and so does not campaign (§3, item 10).
    pub fn measure(&mut self, liveness: &Liveness) -> Result<Option<Span>, ReplicaError> {
        let Some(granularity) = liveness.granularity() else {
            return Ok(None);
        };
        let id = self.id();
        let durable = self
            .flushes
            .mean()
            .or_else(|| liveness.flush_mean())
            .unwrap_or(Duration::ZERO);
        let configuration = self.node.raft.configuration();
        let voters = configuration.voters();
        if voters.iter().all(|voter| *voter == id) {
            self.measure_reach(liveness, granularity, durable)?;
            return Ok(None);
        }
        let paths = voters
            .iter()
            .filter(|voter| **voter != id)
            .filter_map(|voter| liveness.round_trip(*voter));
        let Some(ballot) = Ballot::measure(paths, voters.len(), durable, granularity) else {
            return Ok(None);
        };
        let Some(span) = ballot.span(granularity) else {
            return Ok(None);
        };
        let timing = Timing::of(&ballot, &span);
        if self.node.raft.timing() != Some(timing) {
            self.set_timing(timing)?;
        }
        Ok(Some(span))
    }

    /// A sole voter's reach, given to the core when it moved: the measured round trip to the
    /// slowest of the group's other members at its tail, and a flush, as a ballot's round tail is
    /// to its voters (`hyper_timing::Ballot::broadcast_tail`). A sole voter has no election to
    /// time and is given no timing, so it beats, leading, on this alone: the beat that recovers a
    /// learner's answer lost on its way, which its probe waits on (`hyper_raft::Raft::set_reach`).
    /// None before a path to one of them is measured.
    fn measure_reach(
        &mut self,
        liveness: &Liveness,
        granularity: Duration,
        durable: Duration,
    ) -> Result<(), ReplicaError> {
        let granularity = u64::try_from(granularity.as_nanos()).unwrap_or(u64::MAX);
        let tail = self
            .peers()
            .filter_map(|peer| liveness.round_trip(peer))
            .filter(|path| path.samples() > 0)
            .filter_map(|path| path.tail_ns(granularity))
            .max();
        let Some(tail) = tail else {
            return Ok(());
        };
        let reach = Duration::from_nanos(tail).saturating_add(durable);
        if self.node.raft.reach() != Some(reach) {
            self.guarded(|r| {
                let set = r.node.set_reach(reach);
                r.heard(set)
            })?;
        }
        Ok(())
    }

    /// When the owner is to drive the replica though nothing arrives (`Driven::wake`): the core's
    /// next timer, or the end of the quiet period at which a commit no write has stated is written
    /// (§4.1, `quiet_commit`), while the log has room for it; none when nothing is timed. Without
    /// the second, a group that went quiet with its applied index past its durable commit was
    /// driven by nothing again, and stayed there.
    pub fn deadline(&self) -> Option<u64> {
        let quiet = self.quiet_since.filter(|_| self.has_slot()).map(|since| {
            since.saturating_add(u64::try_from(self.quiet.as_nanos()).unwrap_or(u64::MAX))
        });
        [self.node.deadline(), quiet, self.disk_patience_point()]
            .into_iter()
            .flatten()
            .min()
    }

    /// Wakes the core at the owner's clock (timing step L-2): what it armed since the last
    /// drive is timed, and a campaign, a beat or a transfer's end that is due is done. Its
    /// campaigns are held while a write waits for room: a stalled member takes part in nothing.
    /// A marked one's are the core's to judge (§5).
    fn wake(&mut self, now: u64) -> Result<(), ReplicaError> {
        if self.node.raft.config().elections == Elections::Ticks {
            return Ok(());
        }
        let held = self.stall.is_some();
        let told = self.node.hold_campaigns(held);
        self.must(told)?;
        let woken = self.node.wake(now);
        self.must(woken).map(drop)
    }

    /// A message from the network, bound to its authenticated sender by the transport. One the
    /// replica can take with no room is taken whatever the budget's room (`takes_without_room`):
    /// what lets a full replica drain, a leader hearing the acknowledgements that commit and hand
    /// over, a follower its leader's commit. Reserved for as well, a leader whose stalled disk had
    /// filled its room heard none of them again, and held its lead and its room for good (kiln's
    /// CP-T2.2, the liveness harness's `judged_apart_no_hand_over_while_every_other_disk_is_slow`).
    pub fn step(&mut self, message: Message) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            r.takes()?;
            if !r.takes_without_room(&message) {
                r.reserve(bytes(proto::message_bytes(&message)))?;
            }
            let stepped = r.node.step(message);
            let outcome = r.heard(stepped);
            r.settle();
            outcome
        })
    }

    fn last_index(&mut self) -> Result<u64, ReplicaError> {
        let bounds = self.node.store().log.bounds();
        bounds
            .map(|(_, last)| last)
            .map_err(|error| self.fence(Cause::Core(hyper_raft::Error::Storage(error))))
    }

    /// Campaigns now; refused [`ReplicaError::Marked`] while the core finds that what the
    /// member's log may lack keeps it from leading (`docs/durable.md` §5).
    pub fn campaign(&mut self) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            r.takes()?;
            match r.node.campaign() {
                Err(hyper_raft::Error::Lost) => Err(ReplicaError::Marked),
                campaigned => r.heard(campaigned),
            }
        })
    }

    /// Proposes an entry of `data`, with `context` attached; only a leader takes it. The bytes
    /// are moved into the log, not copied.
    pub fn propose(&mut self, context: Vec<u8>, data: Vec<u8>) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            r.takes()?;
            r.reserve(bytes(data.len().saturating_add(context.len())))?;
            let proposed = r.node.propose(context, data);
            let outcome = r.heard(proposed);
            r.settle();
            outcome
        })
    }

    /// Proposes by the fast track (`hyper_raft::fast`); the index proposed for.
    pub fn propose_fast(&mut self, context: Vec<u8>, data: Vec<u8>) -> Result<u64, ReplicaError> {
        self.guarded(|r| {
            r.takes()?;
            r.reserve(bytes(data.len().saturating_add(context.len())))?;
            let proposed = r.node.propose_fast(context, data);
            let outcome = r.heard(proposed);
            r.settle();
            outcome
        })
    }

    /// Proposes a change of the configuration.
    pub fn change(&mut self, context: Vec<u8>, change: &ConfChangeV2) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            r.takes()?;
            r.reserve(bytes(change.encoded_len().saturating_add(context.len())))?;
            let proposed = r.node.propose_conf_change(context, change);
            let outcome = r.heard(proposed);
            r.settle();
            outcome
        })
    }

    /// Asks for a read confirmed by a quorum; once it is, and the replica has applied through its
    /// index, a drive gives it back in [`Output::reads`]. Refused past the core's bound on reads,
    /// which counts those the replica holds for its apply too.
    pub fn read(&mut self, context: Vec<u8>) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            r.takes()?;
            let held = r
                .reads
                .len()
                .saturating_add(r.node.raft.pending_read_count());
            if held >= r.node.raft.config().limits.pending_reads {
                return Err(ReplicaError::Refused(hyper_raft::Error::Capacity(
                    "reads waiting",
                )));
            }
            r.reserve(bytes(context.len()))?;
            let asked = r.node.read_index(context);
            let outcome = r.heard(asked);
            r.settle();
            outcome
        })
    }

    /// The owner freed room in the log (another group compacted or left, a queue drained): a
    /// replica stalled for room makes its refused writes again at its next drive.
    pub fn resume(&mut self) {
        if let Some(stall) = self.stall.as_mut() {
            stall.freed = true;
        }
    }

    /// What the store holds the replica's refused write for, while it does ([`Fault::Held`]):
    /// a precondition of the store's own, which its owner meets outside the log.
    pub fn held(&self) -> Option<&L::Hold> {
        self.node.store().log.held()
    }

    /// The owner met `met`, what the store held a write for: the store takes it, and the
    /// replica makes its refused writes again at its next drive.
    pub fn release(&mut self, met: &L::Hold) {
        self.node.store_mut().log.release(met);
        self.resume();
    }

    /// One tick of the owner's period, for a replica that elects on ticks (`docs/durable.md`
    /// §8): the core campaigns or beats as its counts say. True when it acted. A stalled
    /// replica is not ticked, for a member that cannot persist takes no part, and the ticks it
    /// missed are not given again. Refused by suspicion.
    pub fn tick(&mut self) -> Result<bool, ReplicaError> {
        self.guarded(|r| {
            if r.node.raft.config().elections != Elections::Ticks {
                return Err(ReplicaError::Refused(hyper_raft::Error::Settings(
                    "elections by suspicion take no ticks",
                )));
            }
            if r.stall.is_some() {
                return Ok(false);
            }
            let ticked = r.node.tick();
            r.heard(ticked)
        })
    }

    /// A leader sends its heartbeats now, between ticks: for an owner whose period is
    /// stretched, the heartbeats keep the cadence its followers expect. Any other role does
    /// nothing.
    pub fn beat(&mut self) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            r.takes()?;
            let beat = r.node.ping();
            r.heard(beat)
        })
    }

    /// The election timeout, in ticks, from the owner's own pace: within one to two of the
    /// core's `election_tick`, or refused.
    pub fn set_randomized_election_timeout(&mut self, ticks: usize) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            let set = r.node.raft.set_randomized_election_timeout(ticks);
            r.heard(set)
        })
    }

    /// Ticks of patience beyond the election timeout, for the stalls the owner has seen in
    /// itself.
    pub fn set_patience(&mut self, ticks: usize) {
        self.node.raft.set_patience(ticks);
    }

    /// Ticks this member, while it leads, waits beyond its election timeout before it asks
    /// whether a quorum heard it (`hyper_raft::Raft::set_quorum_patience`, `docs/raft.md` §3.6):
    /// what its owner measured of its voters' answers. Refused, nothing changed, where the
    /// election timeout and it pass what a tick counts.
    pub fn set_quorum_patience(&mut self, ticks: usize) -> Result<(), ReplicaError> {
        self.node
            .raft
            .set_quorum_patience(ticks)
            .map_err(ReplicaError::Refused)
    }

    /// What the member does with an append that arrives ahead of a hole
    /// (`hyper_raft::RawNode::set_ahead`): policy its owner sets once every peer can read a kept
    /// refusal, never part of what is durable.
    pub fn set_ahead(&mut self, ahead: hyper_raft::Ahead) {
        self.node.set_ahead(ahead);
    }

    /// The member's election priority (`hyper_raft::RawNode::set_priority`): policy its owner
    /// sets, never part of what is durable.
    pub fn set_priority(&mut self, priority: i64) {
        self.node.set_priority(priority);
    }

    /// While the member leads, `member` is sent no more than `bytes` of entries ahead of its
    /// answers (`hyper_raft::RawNode::set_inflight_bytes`): what its owner learned the path to
    /// it carries. False for a member the configuration does not name.
    pub fn set_inflight_bytes(&mut self, member: u64, bytes: u64) -> bool {
        self.node.set_inflight_bytes(member, bytes)
    }

    /// The owner's budget, for the owner to say what its next reservations are charged to.
    pub fn budget_mut(&mut self) -> &mut B {
        &mut self.budget
    }

    /// Reads a quorum confirmed that wait for the replica to apply through their index.
    pub fn reads_held(&self) -> usize {
        self.reads.len()
    }

    /// The time the room the replica's budget has left takes to fill at its backlog's growth, the
    /// room less what the backlog's next reallocation takes, as its patience with its own disk read
    /// them at its latest drive (kiln's CP-T2.2, `docs/durable.md` §4.2): what a leader in the disk
    /// mode hands the lead on within, a hand-over's time before it ends. None in today's mode, with
    /// a budget that refuses nothing ([`Budget::room`]), or while nothing grows.
    pub fn disk_fill(&self) -> Option<Duration> {
        self.fill_ns().map(Duration::from_nanos)
    }

    /// The time the replica's patience takes a hand-over to need: the tail of those it timed, from
    /// its order to the first commit of a later term it saw, or before any the election its timing
    /// law expects and its round (`hyper_raft::Timing`); none in today's mode.
    pub fn hand_over_time(&self) -> Option<Duration> {
        let patience = self.patience.as_ref()?;
        Some(Duration::from_nanos(
            patience.hand_over_ns(self.hand_over_prior_ns()),
        ))
    }

    /// The hand-overs the replica ordered and timed ([`Replica::hand_over_time`]).
    pub fn hand_overs_timed(&self) -> u64 {
        self.patience.as_ref().map_or(0, Patience::hand_overs_timed)
    }

    /// When the oldest write of the replica's still out on its log was submitted, if one is: what
    /// the node's liveness stream judges its own disk's wait from in the disk mode, a disk with
    /// nothing to do being no slow one (`hyper_liveness::Liveness::on_pending`, Focal's 5a).
    pub fn out_since(&self) -> Option<u64> {
        self.writes
            .iter()
            .find(|write| write.state == State::Submitted)
            .map(|write| write.submitted)
    }

    /// The group's disk mode the replica was opened with ([`Settings::disk`]): what its owner sets
    /// the node's liveness stream to (`hyper_liveness::Liveness::set_disk_apart`), and what
    /// [`crate::Owner::pairs`] holds every replica it pairs to.
    pub fn disk(&self) -> Disk {
        self.disk
    }

    /// The hand-overs the replica's patience with its disk began (`docs/durable.md` §4.2): one the
    /// core gives up (its transfer's timeout) is begun again while the patience stays ended.
    pub fn handed_over(&self) -> u64 {
        self.handed_over
    }

    /// The drives at which the replica's patience with its disk ended and no voter was there to
    /// hand over to: every other voter suspected, or its disk judged slow
    /// ([`Replica::disk_slow`]). The leader leads on, its writes committing where they can.
    pub fn heirless(&self) -> u64 {
        self.heirless
    }

    /// When a leader's patience with its own disk ends (`patience::point`), in the disk mode with a
    /// write of its own out, a budget that refuses ([`Budget::room`]) and a backlog that grows: once
    /// the room it has left would fill within a hand-over, and a hand-over's time after both its
    /// oldest write out and a voter there to hand over to, if one is.
    fn disk_patience_point(&self) -> Option<u64> {
        let patience = self.patience.as_ref()?;
        if !self.is_leader() {
            return None;
        }
        let oldest = self.writes.front()?;
        let fill = self.fill_ns()?;
        let hand_over = patience.hand_over_ns(self.hand_over_prior_ns());
        let anchor = oldest
            .submitted
            .max(self.heir_since.unwrap_or(oldest.submitted));
        Some(patience::point(self.driven_at, fill, hand_over, anchor))
    }

    /// The time the room left takes to fill at the backlog's growth, nanoseconds, in the disk mode
    /// as read at the latest drive: the entries the backlog takes before a proposal is refused
    /// (`patience::entries_to_fill`), at the growth's tail.
    fn fill_ns(&self) -> Option<u64> {
        let patience = self.patience.as_ref()?;
        let room = u64::try_from(self.budget.room()?).unwrap_or(u64::MAX);
        let growth = patience.growth(self.driven_at, self.log_end())?;
        let unstable = self.node.raft.log().unstable();
        let entries = unstable.entries().len();
        let slot = std::mem::size_of::<Entry>();
        // The slots by capacity: what the core counts resident, its entries' payloads and its
        // snapshot apart.
        let slots = unstable
            .resident_bytes()
            .saturating_sub(unstable.payload())
            .saturating_sub(unstable.snapshot_bytes())
            .checked_div(slot)
            .unwrap_or(0);
        let backlog = patience::Backlog {
            room,
            entries: bytes(entries),
            slots: bytes(slots),
            entry_room: bytes(
                self.node
                    .raft
                    .config()
                    .limits
                    .unstable_entries
                    .saturating_sub(entries),
            ),
            slot_bytes: bytes(slot),
            payload: patience.payload(),
        };
        patience::fill_ns(patience::entries_to_fill(&backlog), growth)
    }

    /// The index the core's log ends at: what the backlog's growth is counted in.
    fn log_end(&self) -> u64 {
        self.node.raft.log().last_index().unwrap_or(0)
    }

    /// The bytes an entry the core holds not yet durable takes for its payload, at their mean; none
    /// while it holds none.
    fn unstable_payload(&self) -> Option<u64> {
        let unstable = self.node.raft.log().unstable();
        bytes(unstable.payload()).checked_div(bytes(unstable.entries().len()))
    }

    /// Before a hand-over is timed, the time one is taken to need, nanoseconds: the election the
    /// group's timing law expects and its round; zero before the law measured them.
    fn hand_over_prior_ns(&self) -> u64 {
        self.node.raft.timing().map_or(0, |timing| {
            u64::try_from(timing.election.saturating_add(timing.round).as_nanos())
                .unwrap_or(u64::MAX)
        })
    }

    /// The voter a leader's patience would hand the lead to: of those its detectors trust whose
    /// disks aren't judged slow ([`Replica::disk_slow`]), the one that holds most of its log.
    fn disk_heir(&self) -> Option<u64> {
        let id = self.id();
        let tracker = self.node.raft.tracker();
        self.configuration()
            .voters
            .iter()
            .copied()
            .filter(|&voter| {
                voter != id && !self.node.raft.suspects(voter) && !self.disk_is_slow(voter)
            })
            .filter_map(|voter| tracker.get(voter).map(|p| (p.matched, voter)))
            .max()
            .map(|(_, voter)| voter)
    }

    /// A leader whose patience with its own disk ended (`disk_patience_point`) hands the lead to the
    /// voter that holds most of its log, of those its detectors trust whose disks aren't judged slow
    /// ([`Replica::disk_slow`]), unless a hand-over is under way. The heir campaigns on the core's
    /// word (`TimeoutNow`), which leaves without a write of this member's (`RawNode::light_now`)
    /// once the core has brought the heir's log up to the leader's (Raft's precondition, thesis
    /// §3.10). With no such voter, none is named: an heir whose disk is slow could neither hold the
    /// lead's writes nor win its campaign, whose vote waits on a disk. The patience counts a
    /// hand-over's time from when a voter could take the lead, so while every other disk is slow
    /// too, as when the members share one, it waits on its own as long as on theirs (Focal's
    /// condition 6): one voter's disk caught up a moment before its own is no cause to hand over.
    /// Counted from its write alone, doc 34's spread with the leader's disk let go last (1 and 3 ms
    /// after its followers') began a hand-over in 19 seeds of 200 under a publish a millisecond, and
    /// under one every 100 ms, whose heir catches up at once, spent one to three terms in 20 of 200.
    /// Each hand-over is timed to the first commit of a later term the member sees: what the next
    /// patience takes one to need.
    fn keep_disk_patience(&mut self, now: u64) -> Result<(), ReplicaError> {
        let log = self.node.raft.log();
        let committed = log.term(log.committed()).ok();
        let Some(patience) = self.patience.as_mut() else {
            self.heir_since = None;
            return Ok(());
        };
        if let Some(term) = committed {
            patience.committed(now, term);
        }
        if !self.is_leader() {
            self.heir_since = None;
            return Ok(());
        }
        let heir = self.disk_heir();
        self.heir_since = heir.and(self.heir_since.or(Some(now)));
        if self.disk_patience_point().is_none_or(|point| now < point)
            || self.node.raft.lead_transferee().is_some()
        {
            return Ok(());
        }
        let Some(heir) = heir else {
            self.heirless = self.heirless.saturating_add(1);
            return Ok(());
        };
        let term = self.term();
        let told = self.node.transfer_leader(heir);
        self.heard(told)?;
        self.handed_over = self.handed_over.saturating_add(1);
        if let Some(patience) = self.patience.as_mut() {
            patience.ordered(now, term);
        }
        Ok(())
    }

    /// Hands the lead to `to`.
    pub fn transfer(&mut self, to: u64) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            r.takes()?;
            let told = r.node.transfer_leader(to);
            r.heard(told)
        })
    }

    /// The last message to `member` did not arrive.
    pub fn report_unreachable(&mut self, member: u64) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            r.takes()?;
            let told = r.node.report_unreachable(member);
            r.heard(told)
        })
    }

    /// Whether a snapshot sent to `to` arrived. Replication to a member pauses until its
    /// snapshot's fate is known, so a report is never refused: one that comes while the replica
    /// is stalled is kept, the latest for each member of the configuration, and taken once it
    /// runs again (`docs/durable.md` §2.4).
    pub fn report_snapshot(&mut self, to: u64, arrived: bool) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            if r.stall.is_none() {
                return r.take_report(to, arrived);
            }
            if names(r.configuration(), to) {
                match r.reports.iter_mut().find(|(member, _)| *member == to) {
                    Some(report) => report.1 = arrived,
                    None => r.reports.push((to, arrived)),
                }
            }
            Ok(())
        })
    }

    fn take_report(&mut self, to: u64, arrived: bool) -> Result<(), ReplicaError> {
        let status = if arrived {
            SnapshotStatus::Finish
        } else {
            SnapshotStatus::Failure
        };
        let told = self.node.report_snapshot(to, status);
        self.heard(told)
    }

    /// Does what there is to do (`docs/durable.md` §2.2): takes the log's answers, applies what
    /// the fence allows, and takes at most one `Ready`, whose write goes out with `waker`. `now`
    /// is the owner's monotonic clock in nanoseconds, as the core's and hyper-liveness's are.
    /// Fills `out`, which the owner reuses.
    pub fn drive(
        &mut self,
        now: u64,
        waker: &Waker,
        out: &mut Output<M::Answer>,
    ) -> Result<Driven, ReplicaError> {
        self.guarded(|r| {
            r.drive_applied = 0;
            r.driven_at = now;
            r.take_answers(now, out)?;
            // A compaction waiting on the machine's checkpoint asks it again before the writes a
            // stall refused are made again, as an owner compacts a stalled group before it drives
            // it: the start moves once the checkpoint is durable, its write out with this drive's
            // waker and answered at the next drive, as a compaction's between drives is, and the
            // refused writes are made again only once it is answered (`make_again`).
            if let Some(keep) = r.persisting {
                r.compact_through(keep, now, waker)?;
            }
            if r.stall.is_some() {
                r.make_again(now, waker)?;
            }
            r.wake(now)?;
            if r.stall.is_some() {
                // What the fence lets go, and what the core committed meanwhile, is applied while
                // stalled as well: applying takes no write, and only what is applied is
                // compacted, which is what frees the log's room. Held until the stall cleared,
                // entries that waited for room in the machine and filled their group's retained
                // bound held back the compaction the refused write waited for, and it them
                // (mantle's leader-failure runs, replica.md §2).
                r.release_fence(out)?;
                r.take_committed(out)?;
            } else {
                r.take_reports()?;
                r.release_fence(out)?;
                r.take_ready(now, waker, out)?;
                r.keep_disk_patience(now)?;
                r.take_light(out)?;
                r.quiet_commit(now, waker)?;
                r.stream_catch_up(waker, out)?;
                r.make_wanted(out)?;
            }
            r.release_reads(out);
            r.keep();
            r.settle();
            let (end, payload) = (r.log_end(), r.unstable_payload());
            if let Some(patience) = r.patience.as_mut() {
                patience.settled(now, end, payload);
            }
            Ok(r.driven())
        })
    }

    fn driven(&mut self) -> Driven {
        let slot = self.has_slot();
        let paged = self.wait == Wait::Page && self.fence.is_some();
        // A page held is applied at the next drive, stalled or not (`drive`).
        let more = paged
            || (self.stall.is_none()
                && slot
                && (self.ready_due()
                    || self
                        .behind_fence()
                        .is_some_and(|(_, last)| !self.fence_covered(last))));
        Driven {
            more,
            out: self.writes.len(),
            stalled: self.stall.as_ref().map(|stall| stall.fault.clone()),
            wake: self.deadline(),
            flushed: self.flushed.take(),
            out_since: self.out_since(),
        }
    }

    /// Whether the replica may submit a write: the store has room, and the writes out are fewer
    /// than the core's depth and one of the shell's own.
    fn has_slot(&self) -> bool {
        self.writes.len() < self.depth.saturating_add(1) && self.node.store().log.room()
    }

    /// Takes the log's answers, in the order submitted. Only here: a write submitted in this
    /// drive is never looked at in it, so what a drive gives out never hangs on how soon a flush
    /// happened to end (mantle's determinism finding, replica.md §5).
    fn take_answers(&mut self, now: u64, out: &mut Output<M::Answer>) -> Result<(), ReplicaError> {
        loop {
            let Some(front) = self.writes.front() else {
                return Ok(());
            };
            let behind = self.stall.as_ref().is_some_and(|stall| stall.behind > 0);
            let answer = match &front.state {
                State::Empty if !behind => Ok(()),
                State::Empty => Err(Fault::Behind),
                State::Refused(fault) => Err(fault.clone()),
                State::Submitted => match self.node.store_mut().log.poll() {
                    Some(answer) => answer,
                    None => return Ok(()),
                },
            };
            let write = self
                .writes
                .pop_front()
                .ok_or(Cause::Invariant("an answer for no write"))
                .map_err(|cause| self.fence(cause))?;
            self.answered(write, answer, now, out)?;
        }
    }

    fn answered(
        &mut self,
        write: Out,
        answer: Result<(), Fault>,
        now: u64,
        out: &mut Output<M::Answer>,
    ) -> Result<(), ReplicaError> {
        let behind = match self.stall.as_mut() {
            Some(stall) if stall.behind > 0 => {
                stall.behind = stall.behind.saturating_sub(1);
                true
            }
            _ => false,
        };
        match answer {
            Ok(()) if behind => Err(self.fence(Cause::Invariant(
                "a write made durable behind a refused one",
            ))),
            Ok(()) => self.durable(write, now, out),
            Err(fault) if fault.changed_nothing() => {
                self.refused(write, fault);
                Ok(())
            }
            Err(fault) => Err(self.fence(Cause::Write(fault))),
        }
    }

    /// A write is durable, and every one before it: what waited for it leaves, the core hears,
    /// and what it gives to apply is applied.
    fn durable(
        &mut self,
        write: Out,
        now: u64,
        out: &mut Output<M::Answer>,
    ) -> Result<(), ReplicaError> {
        if write.state == State::Submitted {
            self.last_durable = Some(now);
            self.flushed = Some((write.submitted, now));
            // The wait it ended, a sample of the backlog's growth, measured on the log as the drive
            // began.
            let end = self.log_end();
            if let Some(patience) = self.patience.as_mut() {
                patience.on_durable(now, end);
            }
            if write.vote {
                // A fold that is full keeps its mean: the sample is one of more than it counts.
                let _ = self.flushes.on_flush(write.submitted, now);
            }
        }
        if let Some(commit) = write.commit {
            // I7: a commit a write states names only entries it or an earlier write holds.
            if commit > self.last_index()? {
                return Err(self.fence(Cause::Invariant(
                    "a write stated a commit past the entries the log holds",
                )));
            }
            self.logged_commit = self.logged_commit.max(commit);
        }
        let Kind::Ready(number) = write.kind else {
            if let Kind::Start(imaged) = write.kind {
                // The log starts past what it compacted only now: a start refused changed
                // nothing, and is the owner's to ask again (`Replica::make_again`).
                if imaged.is_some() {
                    self.imaged = imaged;
                }
                let start = self.node.store().log.bounds().map(|(start, _)| start.index);
                let start =
                    start.map_err(|e| self.fence(Cause::Core(hyper_raft::Error::Storage(e))))?;
                self.let_go_through(start);
                // A snapshot behind the start serves no member: it would need entries the log
                // no longer holds. Its bytes go now, not at the next member that falls behind.
                let held = &mut self.node.store_mut().snapshot;
                if held
                    .as_ref()
                    .is_some_and(|s| proto::snapshot_index(s) < start)
                {
                    *held = None;
                }
                if let Some(stall) = self.stall.as_mut() {
                    stall.freed = true;
                }
            }
            return self.tell_durable_commit();
        };
        self.emit(write.messages, out);
        let persisted = self.node.on_persist(number);
        let mut light = self.must(persisted)?;
        self.emit(light.take_messages(), out);
        self.tell_durable_commit()?;
        if let Some((first, last)) = light.committed_range() {
            self.apply_range(first, last, out)?;
        }
        self.tell_applied()
    }

    /// Sends each member a leader catches up from its store the pages the store has read for it
    /// (`LogStore::catch_up`, `docs/durable.md` §8): the store reads them off this thread, and the
    /// core, told meanwhile that the store does not hold them yet, sends them now, while the
    /// member's credit admits them (`Config::max_inflight_msgs`). Each round sends each member
    /// waiting at a page read one of them and asks the store again, which lets go the runs passed
    /// and opens the next as it does at every drive, so its window holds: one read out for the
    /// group, and for each member the run that holds its next index and the one after it. Sent one
    /// page a drive, a member had the rest of its credit only with its acknowledgement of that page,
    /// a round trip at every run read (mantle: vendor/UPSTREAM.md).
    fn stream_catch_up(
        &mut self,
        waker: &Waker,
        out: &mut Output<M::Answer>,
    ) -> Result<(), ReplicaError> {
        let id = self.id();
        let leads = self.is_leader();
        let mut lows = std::mem::take(&mut self.lows);
        let mut pages = std::mem::take(&mut self.pages);
        let mut waiting = std::mem::take(&mut self.waiting);
        // A member takes a page a round until its credit is spent (a probe's one, a window's
        // `max_inflight_msgs`), and the round after the last finds none to send.
        let rounds = self.node.raft.config().max_inflight_msgs.saturating_add(1);
        let mut sent = Ok(());
        let mut any = false;
        for round in 0..=rounds {
            lows.clear();
            pages.clear();
            waiting.clear();
            if leads {
                let others = self
                    .node
                    .raft
                    .tracker()
                    .iter()
                    .filter(|&(member, _)| member != id);
                lows.extend(others.map(|(_, progress)| progress.next_index));
            }
            self.catching_up = self.node.store_mut().log.catch_up(waker, &lows, &mut pages);
            let read = self
                .node
                .raft
                .tracker()
                .iter()
                .filter(|&(member, progress)| {
                    member != id && !progress.is_paused() && pages.contains(&progress.next_index)
                });
            waiting.extend(read.map(|(member, _)| member));
            if waiting.is_empty() || round == rounds {
                break;
            }
            sent = waiting
                .iter()
                .try_for_each(|&member| self.node.send_append(member));
            any = true;
            if sent.is_err() {
                break;
            }
        }
        (self.lows, self.pages, self.waiting) = (lows, pages, waiting);
        self.must(sent)?;
        if any {
            let messages = self.node.messages_now();
            self.emit(messages, out);
        }
        Ok(())
    }

    /// What the core may apply and send without a write, whatever the writes out
    /// (`RawNode::light_now`): a leader's appends when it proposes and what it commits when it
    /// commits, a follower's held answers when it takes the entries (core step R-8). Without it
    /// they wait for a write of the member's own to free a place for the next `Ready`.
    fn take_light(&mut self, out: &mut Output<M::Answer>) -> Result<(), ReplicaError> {
        let light = self.node.light_now(true);
        let mut light = self.must(light)?;
        self.emit(light.take_messages(), out);
        if let Some((first, last)) = light.committed_range() {
            self.apply_range(first, last, out)?;
        }
        self.tell_applied()
    }

    /// What the core committed, applied while the replica is stalled (`drive`): nothing is sent
    /// with it, as a stalled replica takes no part in the group until its writes are made again.
    fn take_committed(&mut self, out: &mut Output<M::Answer>) -> Result<(), ReplicaError> {
        let light = self.node.committed_now(true);
        let light = self.must(light)?;
        if let Some((first, last)) = light.committed_range() {
            self.apply_range(first, last, out)?;
        }
        self.tell_applied()
    }

    /// Tells the core the durable commit `C_d` where a write of the shell's own made it pass what
    /// the core read from `Ready`s' hard states (core step R-6, `RawNode::commit_durable`): a
    /// commit stated beyond its `Ready`'s (`commit = last`), a write of the commit alone, a
    /// compaction's, or the state machine's durable index. Answers then carry it.
    fn tell_durable_commit(&mut self) -> Result<(), ReplicaError> {
        let durable = self.durable_commit();
        if durable > self.node.durable_commit() {
            let told = self.node.commit_durable(durable);
            self.must(told)?;
        }
        Ok(())
    }

    /// A write was refused, changing nothing: it, and every write after it, waits to be made
    /// again once there is room.
    fn refused(&mut self, write: Out, fault: Fault) {
        // A store's hold its owner already met, before this refusal was taken (the owner learns
        // of a hold from the store as the write is submitted): the writes are made again at the
        // next drive, or the replica would wait for a release that has come.
        let released = fault == Fault::Held && self.node.store().log.held().is_none();
        if fault == Fault::Held {
            self.writes_made.held = self.writes_made.held.saturating_add(1);
        }
        let behind = self.writes.len();
        let stall = self.stall.get_or_insert_with(|| Stall {
            fault,
            behind,
            freed: released,
            ready: None,
            messages: Vec::new(),
        });
        if let Kind::Ready(number) = write.kind {
            stall.ready = Some(number);
            stall.messages.push((write.tip, write.messages));
        }
    }

    /// Makes the refused writes again, once every write out has been answered and the log has
    /// room: one write of everything they held, laid out from the core (R-4 keeps it until its
    /// notice), with the latest hard state and the messages of each refused write whose log the
    /// core still holds (`Out::tip`). Its notice is the last refused `Ready`'s, which covers all
    /// of them, and nothing past them: an entry no write was issued for waits for the `Ready`
    /// that issues it. Written here as well, the core would issue it again, and a compaction
    /// bounded by what the store holds could pass it before that write, which the store then
    /// refuses below its start (the shell's simulation, seed 9785 of its fourth shape: a leader
    /// applying ahead of its write applied what its followers took from that `Ready`, and
    /// compacted while the `Ready`'s write stalled). Nor while a compaction waits on the
    /// machine's checkpoint or for room ([`Replica::persisting`]): that compaction is what frees
    /// the room, and made before it the writes would only be refused again, and the compaction's
    /// start, sent behind them, refused with them.
    fn make_again(&mut self, now: u64, waker: &Waker) -> Result<(), ReplicaError> {
        let freed = self.stall.as_ref().is_some_and(|stall| stall.freed);
        if !freed
            || !self.writes.is_empty()
            || self.persisting.is_some()
            || !self.node.store().log.room()
        {
            return Ok(());
        }
        let Some(stall) = self.stall.take() else {
            return Ok(());
        };
        let Some(number) = stall.ready else {
            // Only the shell's own writes were refused: a commit is stated again when it is
            // needed, and a compaction is the owner's to ask again.
            return Ok(());
        };
        // A snapshot no `Ready` gave yet is not installed in the state machine: written here,
        // the log would start past it (I8), and a member stopped before that `Ready` would not
        // open. Its `Ready` installs it and writes it, and the entries after it, which follow
        // it in the core; the commit stated meanwhile names only what earlier writes hold.
        let unissued = self
            .node
            .raft
            .log()
            .unstable()
            .unissued_snapshot()
            .is_some();
        let stated = self.stated_commit()?;
        let hard = HardState {
            commit: if unissued {
                self.issued.commit
            } else {
                // I7: the core may have committed entries no write was issued for yet.
                stated.min(self.written_through()).max(self.issued.commit)
            },
            ..self.issued
        };
        // Copied: the core lends what it holds and the store's write takes the store mutably,
        // both inside the core. A refusal for room is rare, and this its one copy.
        let unstable = self.node.raft.log().unstable();
        let start = unstable.snapshot().filter(|_| !unissued).map(|s| Point {
            index: proto::snapshot_index(s),
            term: proto::snapshot_term(s),
        });
        let held: Vec<Entry> = if unissued {
            Vec::new()
        } else {
            unstable.issued_entries().to_vec()
        };
        // The fast track's proposals the refused writes held: the core keeps those of every
        // write issued until its notice, and every write out has been answered, so those it
        // keeps are the refused ones'. Fenced here before, a full log cost a fast group its
        // replica.
        let proposals: Vec<Entry> = self.node.issued_proposals().cloned().collect();
        // And what the core released through: written again with them, the release ends no
        // proposal they give.
        let released = Some(self.node.released()).filter(|through| *through > 0);
        let write = Write {
            start,
            entries: entries_of(&held, start),
            hard_state: Some(hard),
            proposals: &proposals,
            released,
            at_flush: self.at_flush(),
        };
        let submitted = self.node.store_mut().log.submit(&write, waker);
        let state = self.submitted_state(submitted)?;
        self.issued = hard;
        // The messages each refused write held, where the core's log still holds the entry it
        // ended at when that write was laid out: by Log Matching every entry before it is the
        // same, and what they acknowledge is what this write makes durable. Where a later
        // leader's append cut the log back past it since, they acknowledge entries this write
        // does not hold, and are dropped, as the network may drop any message (I2: the shell's
        // simulation, seed 3828 of its second shape, released an acknowledgement of 46 over a
        // log made durable to 41).
        let mut messages: Option<Vec<Message>> = None;
        for (tip, mut held) in stall.messages {
            if !self.holds(tip) {
                continue;
            }
            match messages.as_mut() {
                Some(all) => all.append(&mut held),
                None => messages = Some(held),
            }
        }
        let tip = self.tip()?;
        self.writes.push_back(Out {
            kind: Kind::Ready(number),
            state,
            messages: messages.unwrap_or_default(),
            tip,
            commit: Some(hard.commit),
            vote: true,
            start: start.is_some(),
            submitted: now,
        });
        Ok(())
    }

    /// The last entry of the core's log, its index and term ([`Out::tip`]).
    fn tip(&mut self) -> Result<(u64, u64), ReplicaError> {
        let log = self.node.raft.log();
        let tip = log
            .last_index()
            .and_then(|last| log.term(last).map(|term| (last, term)));
        self.must(tip)
    }

    /// Whether the core's log holds `tip`, an index and the term of the entry there.
    fn holds(&self, (index, term): (u64, u64)) -> bool {
        index == 0
            || self
                .node
                .raft
                .log()
                .term(index)
                .is_ok_and(|held| held == term)
    }

    /// What a submission's outcome makes of the write: with the log, refused as nothing, or a
    /// failure that fences.
    fn submitted_state(&mut self, submitted: Result<(), Fault>) -> Result<State, ReplicaError> {
        match submitted {
            Ok(()) => Ok(State::Submitted),
            Err(fault) if fault.changed_nothing() => Ok(State::Refused(fault)),
            Err(fault) => Err(self.fence(Cause::Write(fault))),
        }
    }

    /// The reports kept while stalled.
    fn take_reports(&mut self) -> Result<(), ReplicaError> {
        while let Some((to, arrived)) = self.reports.pop() {
            match self.take_report(to, arrived) {
                Ok(()) | Err(ReplicaError::Refused(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// The commit a write laid out now states (I7): the core's, and for a member that decides
    /// alone the last entry, which its own write commits once durable (focal F17's
    /// `sole_commit`; `docs/durable.md` §4.1).
    fn stated_commit(&self) -> Result<u64, ReplicaError> {
        let log = self.node.raft.log();
        if self.decides_alone() {
            return log
                .last_index()
                .map_err(|_| ReplicaError::Fenced(Cause::Invariant("the log's last entry")));
        }
        Ok(log.committed())
    }

    /// The last entry this write or an earlier one holds, for a write that holds no entries of
    /// its own: the entries before the first no write was issued for (I7). While a snapshot
    /// waits to be written, nothing past what earlier writes stated.
    fn written_through(&self) -> u64 {
        let unstable = self.node.raft.log().unstable();
        if unstable.unissued_snapshot().is_some() {
            return self.issued.commit;
        }
        unstable.issued().saturating_sub(1)
    }

    /// Whether this member alone decides what commits: it leads, it is the one voter of a
    /// configuration that is not joint, no change waits to be applied, and its last entry is of
    /// its term, so its own write commits everything it holds.
    fn decides_alone(&self) -> bool {
        let raft = &self.node.raft;
        let configuration = raft.configuration();
        raft.state() == StateRole::Leader
            && !configuration.is_joint()
            && configuration.voters() == [raft.id()]
            && !raft.has_pending_conf()
            && raft.log().last_term().is_ok_and(|term| term == raft.term())
    }

    /// Whether an entry waits behind the fence that no write out states the commit of: the next
    /// write states it (§4.1).
    fn fence_needs_commit(&self) -> bool {
        self.behind_fence()
            .is_some_and(|(_, last)| !self.fence_covered(last))
    }

    /// Whether a write out states a commit through `index`.
    fn fence_covered(&self, index: u64) -> bool {
        self.durable_commit() >= index
            || self
                .writes
                .iter()
                .any(|write| write.state == State::Submitted && write.commit >= Some(index))
    }

    /// Whether the core has a `Ready` the replica may take now.
    fn ready_due(&self) -> bool {
        self.node.has_ready() && self.node.in_flight() < self.depth && self.has_slot()
    }

    /// Takes the core's next `Ready`, if the replica may (one a quantum): gives out what leaves
    /// at once, installs its snapshot, submits its write and issues it, then applies what it
    /// gives to apply. While entries wait behind the fence the core gives nothing more to apply
    /// (`RawNode::pause_apply`, core step R-6), `Ready`s go on, and the next one's write states
    /// the commit; when none is due, a write of the commit alone does.
    fn take_ready(
        &mut self,
        now: u64,
        waker: &Waker,
        out: &mut Output<M::Answer>,
    ) -> Result<(), ReplicaError> {
        let due = self.ready_due();
        if let Some((_, last)) = self.behind_fence()
            && !due
            && !self.fence_covered(last)
            && self.has_slot()
        {
            self.writes_made.fenced = self.writes_made.fenced.saturating_add(1);
            return self.commit_write(now, waker);
        }
        if !due {
            return Ok(());
        }
        let readied = self.node.ready_in_place();
        let mut ready = self.must(readied)?;
        // A commit that moved alone rides a later write (§4.1), unless the fence needs `C_d`
        // for an entry no write out states: the core then vouches for no commit from this one,
        // and holds the answers among its messages, not yet taken, to the durable commit.
        if !self.fence_needs_commit() {
            let deferred = self.node.defer_commit(&mut ready);
            self.must(deferred)?;
        }
        self.emit(ready.take_messages(), out);
        // Held answers (core step R-8) promise memory alone: they leave at once, before this
        // `Ready`'s write and whatever it holds.
        self.emit(ready.take_held_messages(), out);
        for read in ready.take_read_states() {
            self.reads.push_back((read.index, read.request_ctx));
        }
        let number = ready.number();
        let messages = ready.take_persisted_messages();
        let range = ready.committed_range();
        let carries = {
            let persist = self.node.to_persist();
            !persist.entries.is_empty()
                || persist
                    .snapshot
                    .is_some_and(|s| !proto::snapshot_is_empty(s))
        } || !ready.proposals().is_empty();
        let (hard, vote) = self.hard_of(ready.hard_state(), carries)?;
        let installs = self
            .node
            .raft
            .log()
            .unstable()
            .unissued_snapshot()
            .is_some_and(|s| !proto::snapshot_is_empty(s));
        let state = self.write_ready(&ready, hard, waker)?;
        if state == State::Empty {
            self.writes_made.empty = self.writes_made.empty.saturating_add(1);
        } else {
            self.writes_made.readies = self.writes_made.readies.saturating_add(1);
            if carries {
                self.writes_made.entries = self.writes_made.entries.saturating_add(1);
            }
        }
        out.displaced.append(&mut ready.take_displaced());
        let issued = self.node.advance_issued(ready);
        self.must(issued)?;
        if let Some(hard) = hard {
            self.issued = hard;
        }
        let tip = self.tip()?;
        self.writes.push_back(Out {
            kind: Kind::Ready(number),
            state,
            messages,
            tip,
            commit: hard.map(|h| h.commit),
            vote,
            start: installs,
            submitted: now,
        });
        // What the `Ready` gave to apply comes before what its own notice gives.
        if let Some((first, last)) = range {
            self.apply_range(first, last, out)?;
        }
        self.tell_applied()?;
        self.take_answers_of_empty(now, out)
    }

    /// A write that held nothing goes no further than the queue: once every write before it is
    /// answered it is durable, which at the front it already is.
    fn take_answers_of_empty(
        &mut self,
        now: u64,
        out: &mut Output<M::Answer>,
    ) -> Result<(), ReplicaError> {
        while self
            .writes
            .front()
            .is_some_and(|write| write.state == State::Empty)
            && self.stall.is_none()
        {
            let write = self
                .writes
                .pop_front()
                .ok_or(Cause::Invariant("a write vanished"))
                .map_err(|cause| self.fence(cause))?;
            self.durable(write, now, out)?;
        }
        Ok(())
    }

    /// The hard state a `Ready`'s write states: its term and vote, and the commit of
    /// [`Replica::stated_commit`]; none when nothing it must state moved since the last write.
    /// With it, whether the term or vote moved: the write's flush is then a vote's.
    ///
    /// A commit costs no write of its own (§4.1, focal F17): it is stated by a write that
    /// `carries` something else (entries, a start or proposals), or when the commit fence needs
    /// `C_d` for an entry no write out states. A commit that moved alone is volatile (Ongaro's
    /// thesis, figure 3.1); etcd's `MustSync` likewise syncs only for entries, a term or a vote.
    /// It rides the next write that carries anything, and the quiet write states it for a member
    /// that goes quiet.
    fn hard_of(
        &self,
        given: Option<&HardState>,
        carries: bool,
    ) -> Result<(Option<HardState>, bool), ReplicaError> {
        let (term, vote, held_index, held_term) = given.map_or(
            (
                self.issued.term,
                self.issued.vote,
                self.issued.held_index,
                self.issued.held_term,
            ),
            |h| (h.term, h.vote, h.held_index, h.held_term),
        );
        let commit = self.stated_commit()?.max(self.issued.commit);
        // The promise (core step R-8) is written as a term or a vote is: a member answers for
        // memory only under one whose write is durable, and lowers it only once its disk holds
        // what it answered for.
        let moved = (term, vote, held_index, held_term)
            != (
                self.issued.term,
                self.issued.vote,
                self.issued.held_index,
                self.issued.held_term,
            );
        let states_commit = commit > self.issued.commit && (carries || self.fence_needs_commit());
        let hard = (moved || states_commit).then_some(HardState {
            term,
            vote,
            commit,
            held_index,
            held_term,
        });
        Ok((hard, moved))
    }

    /// Lays out and submits the write of the `Ready` taken: its snapshot installed in the state
    /// machine first (I8), then its start, entries, hard state and proposals.
    fn write_ready(
        &mut self,
        ready: &hyper_raft::Ready,
        hard: Option<HardState>,
        waker: &Waker,
    ) -> Result<State, ReplicaError> {
        let at_flush = self.at_flush();
        let Self { node, machine, .. } = self;
        let persist = node.to_persist();
        let start = match persist.snapshot {
            Some(snapshot) if !proto::snapshot_is_empty(snapshot) => {
                Some(install(persist.store, machine, snapshot))
            }
            _ => None,
        };
        let start = match start {
            Some(Ok(point)) => Some(point),
            Some(Err(cause)) => return Err(self.fence(cause)),
            None => None,
        };
        let write = Write {
            start,
            entries: entries_of(persist.entries, start),
            hard_state: hard,
            proposals: ready.proposals(),
            released: ready.released(),
            at_flush,
        };
        if write.is_empty() {
            return Ok(State::Empty);
        }
        let submitted = persist.store.log.submit(&write, waker);
        if let Some(point) = start {
            self.installed(point);
        }
        self.submitted_state(submitted)
    }

    /// A snapshot was installed at `point`: it replaces whatever page waited behind the fence,
    /// which it covers.
    fn installed(&mut self, point: Point) {
        self.applied = point;
        self.conf_index = point.index;
        // The log starts at the image's point, holding nothing applied past it.
        self.compactable = 0;
        self.sizes.clear();
        self.sizes_from = point.index.saturating_add(1);
        self.imaged = self
            .node
            .store()
            .snapshot
            .as_ref()
            .map(|s| bytes(s.data.len()));
        if self.fence.take().is_some() {
            self.wait = Wait::Commit;
            self.node.resume_apply();
        }
    }

    /// A write of the commit alone, with the term and vote of the last write: the fence waits
    /// for it (`docs/durable.md` §4.1). Every entry the core gave to apply is committed and
    /// durable here, so the commit names only what the log holds once the write is durable.
    fn commit_write(&mut self, now: u64, waker: &Waker) -> Result<(), ReplicaError> {
        let hard = HardState {
            commit: self
                .stated_commit()?
                .min(self.written_through())
                .max(self.issued.commit),
            ..self.issued
        };
        let write = Write {
            hard_state: Some(hard),
            at_flush: self.at_flush(),
            ..Write::default()
        };
        let submitted = self.node.store_mut().log.submit(&write, waker);
        let state = self.submitted_state(submitted)?;
        self.issued = hard;
        self.writes.push_back(Out {
            kind: Kind::Commit,
            state,
            messages: Vec::new(),
            tip: (0, 0),
            commit: Some(hard.commit),
            vote: false,
            start: false,
            submitted: now,
        });
        Ok(())
    }

    /// A commit no write has stated while the applied index ran past the durable commit for a
    /// whole owner period is written then, one such write at a time, waited for by no one, so a
    /// member that stops reopens with what it applied (focal F17's `settle_commit`).
    fn quiet_commit(&mut self, now: u64, waker: &Waker) -> Result<(), ReplicaError> {
        let behind = self.writes.is_empty()
            && self.behind_fence().is_none()
            && self.applied.index > self.durable_commit();
        if !behind {
            self.quiet_since = None;
            return Ok(());
        }
        let since = *self.quiet_since.get_or_insert(now);
        let quiet = u64::try_from(self.quiet.as_nanos()).unwrap_or(u64::MAX);
        if now.saturating_sub(since) >= quiet && self.has_slot() {
            self.quiet_since = None;
            self.writes_made.quiet = self.writes_made.quiet.saturating_add(1);
            self.commit_write(now, waker)?;
        }
        Ok(())
    }

    /// Committed entries given to apply: applied now, or held behind the fence after those that
    /// wait already.
    fn apply_range(
        &mut self,
        first: u64,
        last: u64,
        out: &mut Output<M::Answer>,
    ) -> Result<(), ReplicaError> {
        match self.fence {
            Some((held, through)) => {
                if through.checked_add(1) != Some(first) {
                    return Err(
                        self.fence(Cause::Invariant("committed entries given out of order"))
                    );
                }
                self.fence = Some((held, last));
                Ok(())
            }
            None => self.apply_from(first, last, out),
        }
    }

    /// What waits behind the fence is applied once the durable commit covers its first entry;
    /// what waits past a drive's page, at the next drive, a page of it; what waits for room in
    /// the state machine, at the next drive, which asks the machine again.
    fn release_fence(&mut self, out: &mut Output<M::Answer>) -> Result<(), ReplicaError> {
        let Some((first, last)) = self.fence else {
            return Ok(());
        };
        if self.wait == Wait::Commit && first > self.durable_commit() {
            return Ok(());
        }
        self.fence = None;
        self.wait = Wait::Commit;
        self.node.resume_apply();
        self.apply_from(first, last, out)?;
        self.tell_applied()
    }

    /// Applies `[first, last]` where the log holds it, in order, up to the first entry the fence
    /// holds; a change of configuration is applied by the core between walks.
    fn apply_from(
        &mut self,
        first: u64,
        last: u64,
        out: &mut Output<M::Answer>,
    ) -> Result<(), ReplicaError> {
        let mut next = first;
        while next <= last {
            match self.walk(next, last, out) {
                Stop::Done => break,
                Stop::Fence(index) => {
                    self.hold(index, last, Wait::Commit);
                    return Ok(());
                }
                Stop::Page(index) => {
                    self.hold(index, last, Wait::Page);
                    return Ok(());
                }
                // The entry stays committed and unapplied, and the core gives no more while it
                // waits (`pause_apply`): what waits is bounded as a page's wait is, by the core's
                // committed page and its log's room, and nothing past `last` is taken meanwhile.
                Stop::Room(index) => {
                    self.hold(index, last, Wait::Room);
                    return Ok(());
                }
                Stop::Change(point, change) => {
                    self.apply_change(point, &change)?;
                    next = point.index.saturating_add(1);
                }
                Stop::Failed(cause) => return Err(self.fence(cause)),
            }
        }
        if self.applied.index < last {
            return Err(self.fence(Cause::Invariant("the log held less than was committed")));
        }
        Ok(())
    }

    /// Holds `[first, last]` committed and unapplied for what `wait` says; the core gives no more
    /// to apply until it is released ([`Self::release_fence`]).
    fn hold(&mut self, first: u64, last: u64, wait: Wait) {
        self.fence = Some((first, last));
        self.wait = wait;
        self.node.pause_apply();
    }

    /// One walk of committed entries from `next` through `last`, applying each that needs
    /// nothing of the core and is not held by the fence: those the store holds read where it
    /// holds them, and a leader's own past it, given before its write is durable
    /// (`Config::apply_unpersisted`, core step R-6), where the core holds them.
    fn walk(&mut self, next: u64, last: u64, out: &mut Output<M::Answer>) -> Stop {
        let durable = self.durable_commit();
        let Self {
            node,
            machine,
            applied,
            compactable,
            sizes,
            sizes_from,
            page,
            drive_applied,
            ..
        } = self;
        let page = *page;
        let held = node.store();
        let unstable = node.raft.log().unstable().entries();
        let tail = unstable.first().map_or(u64::MAX, |e| e.index);
        let mut stop = Stop::Done;
        let mut step = |entry: EntryRef<'_>| -> bool {
            let fenced = entry.changes_configuration() || machine.acts_at_start(&entry);
            if fenced && entry.index > durable {
                stop = Stop::Fence(entry.index);
                return true;
            }
            // One page a drive, as one `Ready` a drive (§7's quantum), and at least one entry.
            let bytes = entry.encoded_bytes();
            if *drive_applied > 0 && drive_applied.saturating_add(bytes) > page {
                stop = Stop::Page(entry.index);
                return true;
            }
            let Ok(size) = u32::try_from(bytes) else {
                stop = Stop::Failed(Cause::Invariant("an entry past four gibibytes"));
                return true;
            };
            if sizes.try_reserve(1).is_err() {
                stop = Stop::Failed(Cause::Core(hyper_raft::Error::Memory));
                return true;
            }
            // Counted once the entry is applied, or failed, which fences the replica: it counts
            // afresh at open. An entry the machine has no room for is counted when it applies.
            let mut count = || {
                *drive_applied = drive_applied.saturating_add(bytes);
                if sizes.is_empty() {
                    *sizes_from = entry.index;
                }
                sizes.push_back(size);
                *compactable = compactable.saturating_add(bytes);
            };
            if entry.changes_configuration() {
                count();
                stop = match change_of(&entry) {
                    Ok(change) => Stop::Change(point_of(&entry), change),
                    Err(cause) => Stop::Failed(cause),
                };
                return true;
            }
            match machine.apply(&entry, &mut out.answers) {
                Ok(true) => {
                    count();
                    *applied = point_of(&entry);
                    false
                }
                Ok(false) => {
                    stop = Stop::Room(entry.index);
                    true
                }
                Err(fatal) => {
                    stop = Stop::Failed(Cause::Machine(fatal));
                    true
                }
            }
        };
        let end = last.saturating_add(1);
        let mut stopped = false;
        if next < tail {
            let walked = held
                .log
                .visit(next, end.min(tail), held.page, &mut |entry| {
                    stopped = step(entry);
                    stopped
                });
            if let Err(error) = walked {
                return Stop::Failed(Cause::Core(hyper_raft::Error::Storage(error)));
            }
        }
        if !stopped && end > tail {
            for entry in unstable.iter().filter(|e| e.index >= next && e.index < end) {
                if step(EntryRef::of(entry)) {
                    break;
                }
            }
        }
        stop
    }

    /// A committed change of configuration: the core applies it, and the state machine keeps
    /// the configuration it made. A change the core refuses is refused alike by every member,
    /// and leaves the configuration as it was.
    fn apply_change(&mut self, at: Point, change: &ConfChangeV2) -> Result<(), ReplicaError> {
        let applied = self.node.apply_conf_change(change);
        let configuration = match applied {
            Ok(configuration) => configuration,
            Err(error) if !error.is_fatal() => self.machine.configuration().clone(),
            Err(error) => return Err(self.fence(Cause::Core(error))),
        };
        let kept = self.machine.apply_change(at, change, &configuration);
        self.machine_did(kept)?;
        self.applied = at;
        self.conf_index = at.index;
        // A member that left keeps no verdict of its disk (Focal's 5b).
        self.slow_disks
            .retain(|member| names(&configuration, *member));
        self.node.store_mut().configuration = configuration;
        Ok(())
    }

    /// Tells the core how far the state machine applied.
    fn tell_applied(&mut self) -> Result<(), ReplicaError> {
        if self.applied.index > self.told_applied {
            let told = self.node.advance_apply_to(self.applied.index);
            self.must(told)?;
            self.told_applied = self.applied.index;
            // What the store kept for this replica to read back before applying it may go.
            self.keep();
        }
        Ok(())
    }

    /// Tells the store the index after which it keeps what this replica will read back
    /// ([`LogStore::keep_after`]): what it has yet to apply, and on a leader what a replicating
    /// member has yet to acknowledge of what was sent to it up to the apply point, which a resend
    /// reads (focal, 2026-10-12). A member in `Replicate` whose next index is past the apply point
    /// has all of that in flight, within the window the leader sends it ahead (`set_inflight_bytes`),
    /// so it holds no more than its window; one whose next index is not past it, or that probes or
    /// takes a snapshot, holds nothing, so a stalled member pins nothing past its window. A mark
    /// below one already given changes nothing, so a member that falls behind and catches up holds
    /// the mark again only from where it then stands.
    fn keep(&mut self) {
        let applied = self.applied.index;
        let from = if self.is_leader() {
            let id = self.id();
            self.node
                .raft
                .tracker()
                .iter()
                .filter(|&(member, progress)| {
                    member != id
                        && progress.state == ProgressState::Replicate
                        && progress.next_index > applied
                })
                .fold(applied, |from, (_, progress)| from.min(progress.matched))
        } else {
            applied
        };
        self.node.store_mut().log.keep_after(from);
    }

    /// Gives out `messages`. The emptied vector goes back to the core as its next queue
    /// (`RawNode::recycle_messages`), so the queue's room is not grown again for every ready taken
    /// ahead.
    fn emit(&mut self, mut messages: Vec<Message>, out: &mut Output<M::Answer>) {
        out.messages.append(&mut messages);
        self.node.recycle_messages(messages);
    }

    /// Reads confirmed and now applied far enough leave.
    fn release_reads(&mut self, out: &mut Output<M::Answer>) {
        let applied = self.applied.index;
        let mut at = 0;
        while let Some(&(index, _)) = self.reads.get(at) {
            if index > applied {
                at = at.saturating_add(1);
                continue;
            }
            if let Some((index, context)) = self.reads.swap_remove_back(at) {
                out.reads.push((context, index));
            }
        }
    }

    /// Makes the snapshot the core asked for, when none held reached what it needs
    /// ([`Held::wanted`]): the state machine's image, with the configuration the group held at
    /// its point. Not the configuration applied since: a member installing it applies the
    /// changes after the image's point from the log, each once. The core, told the snapshot was
    /// temporarily unavailable, is then asked to send it ([`Replica::send_made`]). An image is
    /// made only here, so only for a member behind the log's start, never for a compaction
    /// (raft-rs's `Storage::snapshot`, TiKV's `PeerStorage::snapshot`; Ongaro's thesis §5.2;
    /// mantle docs/research/91-snapshots-on-demand.md). None is made before the machine applied
    /// through what is needed: the core asks again, and one made now would not reach it. Not
    /// while stalled: a stalled member takes no part in the group, and the image waits to be
    /// made, of what the machine applied by then, at the drive that sends it.
    fn make_wanted(&mut self, out: &mut Output<M::Answer>) -> Result<(), ReplicaError> {
        let Some(wanted) = self.node.store().wanted.take() else {
            return Ok(());
        };
        if self.applied.index < wanted {
            return Ok(());
        }
        let mut data = Vec::new();
        let imaged = self.machine.image(&mut data);
        let (point, configuration) = self.machine_did(imaged)?;
        self.images_made = self.images_made.saturating_add(1);
        self.node.store_mut().snapshot = Some(Snapshot {
            data,
            metadata: Some(SnapshotMetadata {
                conf_state: Some(configuration),
                index: point.index,
                term: point.term,
            }),
        });
        // An image short of what was asked, a machine's last checkpoint behind what it applied,
        // waits for the core's next try: sent at once, the core would ask again at once, and
        // every drive would image again until the machine checkpoints past it.
        if point.index < wanted {
            return Ok(());
        }
        self.send_made(out)
    }

    /// Has the core send the snapshot just made (`RawNode::send_append`) to each member behind the
    /// log's start that it may send to now: one whose next entry the log no longer holds, or that
    /// asked for a snapshot, with no probe or snapshot of its own out. Sent in the drive that made
    /// it, as [`Replica::stream_catch_up`] sends a page once the store has read it and raft-rs's
    /// `on_entries_fetched` sends a member what storage had said was temporarily unavailable
    /// (mantle docs/research/91-snapshots-on-demand.md §2a). Left to the core's next try, it went
    /// at the member's next answer to a heartbeat: a round later, and never in a group no beat
    /// reached. The members are the core's, so as many as its configuration names.
    fn send_made(&mut self, out: &mut Output<M::Answer>) -> Result<(), ReplicaError> {
        if !self.is_leader() {
            return Ok(());
        }
        let start = self.node.store().log.bounds().map(|(start, _)| start.index);
        let start = start.map_err(|e| self.fence(Cause::Core(hyper_raft::Error::Storage(e))))?;
        let id = self.id();
        let mut waiting = std::mem::take(&mut self.waiting);
        waiting.clear();
        let behind = self
            .node
            .raft
            .tracker()
            .iter()
            .filter(|&(member, progress)| {
                member != id
                    && !progress.is_paused()
                    && (progress.next_index <= start || progress.pending_request_snapshot != 0)
            });
        waiting.extend(behind.map(|(member, _)| member));
        let sent = waiting
            .iter()
            .try_for_each(|&member| self.node.send_append(member));
        let any = !waiting.is_empty();
        self.waiting = waiting;
        self.must(sent)?;
        if any {
            let messages = self.node.messages_now();
            self.emit(messages, out);
        }
        Ok(())
    }

    /// The images this member made of its state machine for members behind its log's start since
    /// it opened ([`Replica::make_wanted`]): a compaction makes none.
    pub fn images_made(&self) -> u64 {
        self.images_made
    }

    /// Makes the state machine's applied state durable and lets the log free what is before it,
    /// keeping `keep` entries for members that lag; true when a compaction's start went out.
    /// The start never passes the state machine's durable index (I8) nor what the log holds
    /// durably (`docs/durable.md` §4.2). Allowed while stalled: a compaction is what frees room.
    /// A machine whose checkpoint is not durable yet says so ([`StateMachine::persist`]): no
    /// start moves now, and the compaction waits on the checkpoint, each drive asking the machine
    /// again and moving the start at the one that finds it durable ([`Replica::persisting`]), so
    /// the thread that drives the group waits on no flush. Asked meanwhile, the machine goes on
    /// with the checkpoint out and begins no other. A compaction the replica cannot write now (a
    /// start out, or no slot) waits so too, the machine asked only at a drive that can write it.
    pub fn compact(&mut self, keep: u64, now: u64, waker: &Waker) -> Result<bool, ReplicaError> {
        self.guarded(|r| r.compact_through(keep, now, waker))
    }

    /// Whether a compaction waits on the state machine's checkpoint ([`Replica::compact`]): its
    /// owner drives the replica again when the machine's work wakes it, and that drive moves the
    /// start once the checkpoint is durable.
    pub fn persisting(&self) -> bool {
        self.persisting.is_some()
    }

    /// [`Replica::compact`]'s work, and a drive's for a compaction that waits on the machine's
    /// checkpoint.
    fn compact_through(
        &mut self,
        keep: u64,
        now: u64,
        waker: &Waker,
    ) -> Result<bool, ReplicaError> {
        // A start a write out moves is not yet in the store's bounds: a compaction behind it
        // would move the start back. A snapshot's start refused for room is made again past
        // any compaction now: the state machine stands at it, so none passes it. Such a
        // compaction, or one the store has no room for (a write past the core's depth out, or a
        // refusal it holds that the replica has yet to take), waits for a drive that can write
        // it, its checkpoint left as it stands: asked of the machine now, a checkpoint found
        // durable here was let go with the compaction, and nothing asked it again. A stalled
        // replica's refused writes then waited for room no compaction would free, refusing
        // every message meanwhile, its owner asking again only once its ranges moved (mantle's
        // end-to-end runs through a leader's failure, replica.md §2).
        let moving = self.writes.iter().any(|w| w.start);
        if moving || !self.has_slot() {
            self.persisting = Some(keep);
            return Ok(false);
        }
        let persisted = self.machine.persist();
        if !self.machine_did(persisted)? {
            self.persisting = Some(keep);
            return Ok(false);
        }
        self.persisting = None;
        let durable = self.machine.durable().index;
        let bounds = self.node.store().log.bounds();
        let (start, last) =
            bounds.map_err(|e| self.fence(Cause::Core(hyper_raft::Error::Storage(e))))?;
        let index = durable.saturating_sub(keep).min(last);
        if index <= start.index {
            return Ok(false);
        }
        let term = self.node.store().log.term(index);
        let term = term.map_err(|e| self.fence(Cause::Core(hyper_raft::Error::Storage(e))))?;
        // No image: a member behind the new start is sent one made when the core asks for it
        // ([`Replica::make_wanted`]). Imaged here, every compaction read and encoded every
        // row on the thread that drives the group, whether or not any member was behind.
        let imaged = self.machine.image_bytes();
        // The log is compacted only through what it states committed, stated with it.
        let hard = (index > self.issued.commit).then_some(HardState {
            commit: index,
            ..self.issued
        });
        let write = Write {
            start: Some(Point { index, term }),
            hard_state: hard,
            at_flush: self.at_flush(),
            ..Write::default()
        };
        let submitted = self.node.store_mut().log.submit(&write, waker);
        let state = self.submitted_state(submitted)?;
        if let Some(hard) = hard {
            self.issued = hard;
        }
        self.writes_made.starts = self.writes_made.starts.saturating_add(1);
        self.writes.push_back(Out {
            kind: Kind::Start(imaged),
            state,
            messages: Vec::new(),
            tip: (0, 0),
            commit: hard.map(|h| h.commit),
            vote: false,
            start: true,
            submitted: now,
        });
        Ok(true)
    }

    /// Whether the log is due to be compacted by `rule` ([`Compaction`], Ongaro's thesis §5.1.2),
    /// for the owner to compact with [`Replica::compact`], keeping nothing: the applied entries it
    /// holds weighed against the image it was last compacted to, and while this member leads, a
    /// member that lacks what it applied waited for as long as the log holds no more than twice
    /// the threshold. A machine that keeps no image is never due.
    pub fn compaction_due(&self, rule: Compaction) -> bool {
        let Some(image) = self.imaged else {
            return false;
        };
        // A start out is a compaction or an install under way, and [`Replica::compact`] waits
        // for it; so is a compaction waiting on the machine's checkpoint.
        if self.persisting.is_some() || self.writes.iter().any(|w| w.start) {
            return false;
        }
        let raft = &self.node.raft;
        let id = raft.id();
        let applied = self.applied.index;
        let lagging = raft.state() == StateRole::Leader
            && raft
                .tracker()
                .iter()
                .any(|(member, progress)| member != id && progress.matched < applied);
        rule.due(self.compactable, image, lagging)
    }

    /// The bytes of the applied entries the log holds past its start, as the core counts an
    /// entry's: what [`Replica::compaction_due`] weighs. A compaction counts once its start is
    /// durable; an install, as it is written.
    pub fn compactable_bytes(&self) -> u64 {
        self.compactable
    }

    /// Counts the applied entries the log holds past `after` into [`Replica::compactable`], each
    /// into `sizes` as well, as the core counts an entry's: those the store holds, and a leader's
    /// own past them, applied before its write is durable (`Config::apply_unpersisted`), where the
    /// core holds them. An install not yet durable starts the log at its point, and what the
    /// store holds before it is not counted. At open alone: from then on each entry is counted as
    /// it is applied, and let go of as a start passes it ([`Replica::let_go_through`]).
    fn count_applied(&mut self, after: u64) -> Result<(), StorageError> {
        let end = self.applied.index.saturating_add(1);
        let unstable = self.node.raft.log().unstable();
        let after = unstable
            .snapshot()
            .map_or(after, |s| after.max(proto::snapshot_index(s)));
        let first = after.saturating_add(1);
        let held = self.node.store();
        let unstable = unstable.entries();
        let tail = unstable.first().map_or(u64::MAX, |e| e.index);
        let mut sizes = VecDeque::new();
        let mut bytes = 0u64;
        let mut refused = None;
        let mut count = |counted: u64| -> bool {
            let Ok(size) = u32::try_from(counted) else {
                refused = Some(StorageError::Other("an entry past four gibibytes"));
                return true;
            };
            if sizes.try_reserve(1).is_err() {
                refused = Some(StorageError::Other(
                    "no memory to count the applied entries",
                ));
                return true;
            }
            sizes.push_back(size);
            bytes = bytes.saturating_add(counted);
            false
        };
        if first < end.min(tail) {
            held.log
                .visit(first, end.min(tail), held.page, &mut |entry| {
                    count(entry.encoded_bytes())
                })?;
        }
        for entry in unstable
            .iter()
            .filter(|e| e.index >= first && e.index < end)
        {
            if count(EntryRef::of(entry).encoded_bytes()) {
                break;
            }
        }
        if let Some(refused) = refused {
            return Err(refused);
        }
        self.compactable = bytes;
        self.sizes = sizes;
        self.sizes_from = first;
        Ok(())
    }

    /// Takes the entries a start at `start` let go of out of [`Replica::compactable`], each by
    /// what it was counted, oldest first. What the count keeps of them shrinks by half once a
    /// quarter of its room is used (a dynamic table's contraction, Cormen, Leiserson, Rivest and
    /// Stein, *Introduction to Algorithms*, §17.4), so a log that held many entries once does not
    /// keep their room, and a log that grows and shrinks around one size does not reallocate on
    /// each turn.
    fn let_go_through(&mut self, start: u64) {
        while self.sizes_from <= start {
            let Some(size) = self.sizes.pop_front() else {
                break;
            };
            self.compactable = self.compactable.saturating_sub(u64::from(size));
            self.sizes_from = self.sizes_from.saturating_add(1);
        }
        self.sizes_from = self.sizes_from.max(start.saturating_add(1));
        // Moved into room of its own, as the core gives back a queue's room (`Outgoing::recycle`):
        // a reallocation in place has no refusal to return.
        let room = self.sizes.capacity();
        if self.sizes.len() <= room / 4 {
            let mut smaller = VecDeque::new();
            if smaller.try_reserve_exact(room / 2).is_ok() {
                smaller.extend(self.sizes.drain(..));
                self.sizes = smaller;
            }
        }
    }
}

/// The entries of a write: those given, from the first of them, or none past a snapshot's
/// start, which still says where the log ends.
fn entries_of(entries: &[Entry], start: Option<Point>) -> Option<Entries<'_>> {
    match (entries.first(), start) {
        (Some(first), _) => Some(Entries {
            first: first.index,
            entries,
        }),
        (None, Some(start)) => Some(Entries {
            first: start.index.saturating_add(1),
            entries,
        }),
        (None, None) => None,
    }
}

/// Installs `snapshot` in the state machine, durably, before the write that moves the log's
/// start to it (I8): its point.
fn install<L: LogStore, M: StateMachine>(
    held: &mut Held<L>,
    machine: &mut M,
    snapshot: &Snapshot,
) -> Result<Point, Cause> {
    let metadata = snapshot
        .metadata
        .as_ref()
        .ok_or(Cause::Invariant("a snapshot without metadata"))?;
    let point = Point {
        index: metadata.index,
        term: metadata.term,
    };
    let configuration = metadata.conf_state.clone().unwrap_or_default();
    machine
        .install(&snapshot.data, point, &configuration)
        .map_err(Cause::Machine)?;
    held.configuration = configuration;
    held.snapshot = Some(snapshot.clone());
    Ok(point)
}

/// What the log and the state machine hold when the member opens, made to agree before the core
/// reads them (`docs/durable.md` §4.3):
/// - the log may not start past the state machine (I8);
/// - a state machine at a point the log does not hold (a snapshot installed that the log never
///   recorded, or entries the log lost in its last frame) moves the log's start to that point,
///   with nothing past it, and the commit to it: the state machine reports its point's term, so
///   nothing is inferred from terms along the log;
/// - a state machine past the log's commit within its entries raises the commit to it.
fn repair_at_open<L: LogStore>(log: &mut L, durable: Point) -> Result<crate::StoreView, OpenError> {
    let view = log.view().map_err(OpenError::Log)?;
    if view.start.index > durable.index {
        return Err(OpenError::StartPastMachine {
            start: view.start.index,
            machine: durable.index,
        });
    }
    if durable.index == 0 {
        return Ok(view);
    }
    let holds = if durable.index == view.start.index {
        view.start.term == durable.term
    } else {
        durable.index <= view.last && log.term(durable.index).ok() == Some(durable.term)
    };
    let hard = view.hard_state;
    let write = if !holds {
        Write {
            start: Some(durable),
            entries: Some(Entries {
                first: durable.index.saturating_add(1),
                entries: &[],
            }),
            hard_state: Some(HardState {
                commit: hard.commit.max(durable.index),
                ..hard
            }),
            proposals: &[],
            released: None,
            // Written as the member opens, before its configuration is read: answered once
            // confirmed.
            at_flush: false,
        }
    } else if durable.index > hard.commit {
        Write {
            hard_state: Some(HardState {
                commit: durable.index,
                ..hard
            }),
            ..Write::default()
        }
    } else {
        return Ok(view);
    };
    log.write_now(&write).map_err(OpenError::Log)?;
    log.view().map_err(OpenError::Log)
}

#[cfg(test)]
mod tests {
    use super::others_a_quorum;

    /// A write is answered at its frame's flush only where, in each half of the configuration,
    /// the voters but any one are a quorum: three voters or more, never one or two, and in a
    /// joint configuration both halves (mantle durability.md §7.5).
    #[test]
    fn only_three_voters_or_more_in_each_half_answer_at_the_flush() {
        assert!(!others_a_quorum(&[], &[]));
        assert!(!others_a_quorum(&[1], &[]));
        assert!(!others_a_quorum(&[1, 2], &[]));
        assert!(others_a_quorum(&[1, 2, 3], &[]));
        assert!(others_a_quorum(&[1, 2, 3, 4], &[]));
        assert!(!others_a_quorum(&[1, 2, 3], &[1, 2]));
        assert!(!others_a_quorum(&[1, 2], &[1, 2, 3]));
        assert!(others_a_quorum(&[1, 2, 3], &[2, 3, 4, 5]));
    }
}
