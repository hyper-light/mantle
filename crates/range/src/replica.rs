//! One member of one range's Raft group (docs/design/replica.md): hyper-raft's durable shell
//! (`hyper_durable::Replica`, hyper-raft docs/durable.md) over the member's group of the device's
//! log (`GroupStore`) and the range's engine and layer (`RangeMachine`).
//!
//! The shell keeps the order Raft's safety needs (docs/durable.md §3): a leader's appends leave
//! before its own write, a follower's answers and every vote after the write that holds what they
//! say, a change of configuration applies only once the member's durable commit covers it (the
//! commit fence), and the member opens on what its log and engine hold durably. It takes the
//! core's readies ahead of their persistence to the log's depth, steps messages and ticks while
//! writes are out, repairs a marked member by its lost entries, and elects a marked member on its
//! log where the others can be a quorum. The range elects on its owner's ticks (docs/durable.md
//! §8) until its node carries the node-pair liveness stream.

use std::task::Waker;
use std::time::Duration;

use hyper_block::block::BlockFile;
use hyper_durable::{ClaimError, Driven, GroupStore, LogStore, OpenError, Unbounded};
use hyper_log::Log;
use hyper_raft::proto::{ConfChangeV2, ConfState, Entry, EntryType, Message};
use hyper_raft::wire::Record;
use hyper_raft::wire::{ENTRY_FIXED_BYTES, MESSAGE_RECORD_FIXED_BYTES};
use hyper_raft::{Config, Elections, Limits, Stated};
use mantle_meta::apply::Layer;
use mantle_meta::engine::Engine;
use mantle_meta::session::Rules;
use mantle_meta::wire;

use crate::error::ReplicaError;
use crate::machine::{Applied, RangeMachine};
use crate::membership::Replacement;

/// What a drive gives out, in buffers the owner keeps and reuses: messages to send, the answers
/// of the commands applied, and the reads confirmed and applied far enough to serve.
pub type Output = hyper_durable::Output<Applied>;

/// What every member of a range shares: its layer, the bounds of its sessions, the configuration
/// it starts from, and how its core runs.
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
    /// Bytes of committed entries one drive applies at most.
    pub max_committed_size_per_ready: u64,
    /// Bytes of one entry at most, the same on every member: every member's log holds such an
    /// entry in one frame, so a write of any size can be made in parts, and the leader refuses a
    /// larger proposal.
    pub max_entry_bytes: u64,
}

/// The shell's period for a commit no write has stated (`hyper_durable::Settings::quiet`): none.
/// The shell writes such a commit alone so that a member acting at its next start on applied
/// state reopens with it (docs/durable.md §4.1); no entry of a range is acted on at start
/// (`RangeMachine::acts_at_start`), the engine's durable index is part of the durable commit, and
/// a restart learns the rest from its group and its log.
const QUIET: Duration = Duration::MAX;

/// Whether a member's log can hold to the range's settings (audit S04). An entry of the range's
/// largest must fit one frame, so a write of any size can be made in parts. And a group compacted
/// to its applied state retains at most one write's entries: a leader's uncommitted proposals, or
/// the appends a follower has in flight, and one entry past either bound, which the core admits
/// alone. The group's bounds must hold so much, in bytes and in entries of the fewest bytes, or a
/// write refused for room could wait for good.
fn check_settings<F: BlockFile + 'static>(
    settings: &Settings,
    log: &Log<F>,
) -> Result<(), ReplicaError> {
    let overhead = u64::try_from(hyper_durable::ENTRY_OVERHEAD).unwrap_or(u64::MAX);
    let largest = settings.max_entry_bytes.checked_add(overhead);
    let room = u64::try_from(log.entry_room()?).unwrap_or(u64::MAX);
    if largest.is_none_or(|b| b > room) {
        return Err(ReplicaError::Config(
            "an entry of the range's largest does not fit one frame of this log",
        ));
    }
    let ready = one_ready(settings);
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

/// The bytes one ready of the range's settings holds: a leader's uncommitted proposals or the
/// appends a follower has in flight, and one entry past either bound, which the core admits alone.
fn one_ready(settings: &Settings) -> Option<u64> {
    let inflight = u64::try_from(settings.max_inflight_msgs)
        .ok()
        .and_then(|n| n.checked_mul(settings.max_size_per_msg));
    inflight
        .map(|f| f.max(settings.max_uncommitted_size))
        .and_then(|b| b.checked_add(settings.max_entry_bytes))
}

/// What the range states of its members to the core (`hyper_raft::Limits::derive`), each from the
/// range's own settings, so that no bound is chosen apart from them:
/// - the largest message is an append. The core takes entries while their encoding fits
///   `max_size_per_msg`, and one at least, so it carries either that or one entry of the range's
///   largest with its fixed bytes (a range's entries carry no context), inside the record's
///   fixed bytes;
/// - the most members a configuration names are the boot configuration's and the one learner a
///   replacement adds, the only change a range's membership makes ([`Replacement`]);
/// - each queue holds what one ready of the range's settings holds (the bound the log is held to,
///   [`check_settings`]), counted resident: the most entries a ready carries, each at least its
///   fixed bytes on the wire, at an entry's bytes in memory each, so that a member holds a
///   leader's message whole;
/// - one write is out, which the shell raises to the store's depth.
fn limits(range: &Range) -> Result<Limits, ReplicaError> {
    let s = &range.settings;
    let unfit = || ReplicaError::Config("the range's settings exceed what this machine addresses");
    let entry = usize::try_from(s.max_entry_bytes)
        .ok()
        .and_then(|bytes| bytes.checked_add(ENTRY_FIXED_BYTES))
        .ok_or_else(unfit)?;
    let page = usize::try_from(s.max_size_per_msg).map_err(|_| unfit())?;
    let message = page
        .max(entry)
        .checked_add(MESSAGE_RECORD_FIXED_BYTES)
        .ok_or_else(unfit)?;
    let members = range
        .boot
        .voters
        .len()
        .checked_add(range.boot.learners.len())
        .and_then(|n| n.checked_add(1))
        .ok_or_else(unfit)?;
    let memory = one_ready(s)
        .and_then(|bytes| usize::try_from(bytes).ok())
        .and_then(|bytes| bytes.checked_div(ENTRY_FIXED_BYTES))
        .and_then(|entries| entries.checked_mul(std::mem::size_of::<Entry>()))
        .ok_or_else(unfit)?;
    Ok(Limits::derive(Stated {
        message,
        members,
        memory,
        depth: 1,
    })?)
}

fn shell(e: hyper_durable::ReplicaError) -> ReplicaError {
    match e {
        hyper_durable::ReplicaError::Refused(e) => ReplicaError::Refused(e),
        hyper_durable::ReplicaError::Stalled => ReplicaError::Stalled,
        hyper_durable::ReplicaError::Marked => ReplicaError::Uncertain,
        // The range's replicas run under no budget (`Unbounded`), which refuses nothing.
        hyper_durable::ReplicaError::Budget(bytes) => {
            ReplicaError::Stopped(format!("a budget refused {bytes} bytes"))
        }
        hyper_durable::ReplicaError::Fenced(cause) => ReplicaError::Fenced(cause.to_string()),
    }
}

fn opened(e: OpenError) -> ReplicaError {
    match e {
        OpenError::Core(e) => e.into(),
        other => ReplicaError::Stopped(other.to_string()),
    }
}

/// The member's group of the device's log (hyper-log's group handle, `GroupStore`): claimed for
/// it once the range's settings are checked against the log. From here the member writes and
/// reads its group through it alone, and the log refuses the group's writes from anyone else. A
/// group the log found damaged is served to no one but its removal ([`remove`]): the member is
/// rebuilt under a new identity (docs/design/raft-log.md §6).
pub fn claim<F: BlockFile + 'static>(
    log: &Log<F>,
    group: u128,
    range: &Range,
) -> Result<GroupStore<F>, ReplicaError> {
    check_settings(&range.settings, log)?;
    GroupStore::claim(log, group).map_err(|e| match e {
        ClaimError::Damaged => ReplicaError::Damaged,
        ClaimError::Log(e) => ReplicaError::Log(e),
    })
}

/// Removes every record of a damaged group from `log`, before a member under a new identity opens
/// in its place ([`Replica::rebuild`]).
pub fn remove<F: BlockFile + 'static>(log: &Log<F>, group: u128) -> Result<(), ReplicaError> {
    Ok(GroupStore::remove(log, group)?)
}

pub struct Replica<L: LogStore, E: Engine> {
    shell: hyper_durable::Replica<L, RangeMachine<E>, Unbounded>,
    max_entry_bytes: u64,
}

impl<L: LogStore, E: Engine> Replica<L, E> {
    /// Opens member `id` of `range` over its group's store (a group of the device's log,
    /// [`claim`]), at the state `engine` holds durably (docs/design/replica.md §4): the shell
    /// finishes first what a crash left between the log and the engine. `seed` draws its
    /// election timeouts.
    pub fn open(
        id: u64,
        store: L,
        engine: E,
        range: &Range,
        seed: u64,
    ) -> Result<Self, ReplicaError> {
        let s = &range.settings;
        let machine = RangeMachine::open(engine, range.layer, range.rules, &range.boot)?;
        let settings = hyper_durable::Settings {
            core: Config {
                election_tick: s.election_tick,
                heartbeat_tick: s.heartbeat_tick,
                max_size_per_msg: s.max_size_per_msg,
                max_inflight_msgs: s.max_inflight_msgs,
                max_uncommitted_size: s.max_uncommitted_size,
                max_committed_size_per_ready: s.max_committed_size_per_ready,
                // Pre-vote and check-quorum together keep a partitioned member from deposing a
                // healthy leader and a leader cut off from stepping down late (06 §A1).
                check_quorum: true,
                pre_vote: true,
                seed,
                ..Config::new(id, limits(range)?)
            },
            elections: Elections::Ticks,
            quiet: QUIET,
        };
        let shell =
            hyper_durable::Replica::open(&settings, store, machine, Unbounded).map_err(opened)?;
        Ok(Self {
            shell,
            max_entry_bytes: s.max_entry_bytes,
        })
    }

    /// Rebuilds a member whose claim found it [`ReplicaError::Damaged`]: once the group's records,
    /// which the log serves to no one, are removed ([`remove`]) and the group claimed again, a
    /// member under the identity `joining` opens over `store` from `engine`, a new member's, and
    /// catches up from its peers. The group's leader runs the replacement returned, which adds
    /// `joining` as a learner and swaps it for `failed` once it has caught up
    /// (docs/design/replica.md §6).
    ///
    /// `failed` never opens again: it may have voted in terms its log no longer shows, and opened
    /// afresh it would vote in them again. So the node records `joining` as the group's member on
    /// this device before it removes the records, and a restart after the removal, finding the
    /// group empty, opens `joining` again by calling this again.
    pub fn rebuild(
        failed: u64,
        joining: u64,
        store: L,
        engine: E,
        range: &Range,
        seed: u64,
    ) -> Result<(Self, Replacement), ReplicaError> {
        let replacement = Replacement::new(failed, joining).ok_or(ReplicaError::Config(
            "a rebuilt member takes an identity never used, not its own or zero",
        ))?;
        let replica = Self::open(joining, store, engine, range, seed)?;
        Ok((replica, replacement))
    }

    pub fn id(&self) -> u64 {
        self.shell.id()
    }

    pub fn is_leader(&self) -> bool {
        self.shell.is_leader()
    }

    /// The member this one believes leads, 0 for none.
    pub fn leader(&self) -> u64 {
        self.shell.leader()
    }

    pub fn term(&self) -> u64 {
        self.shell.term()
    }

    /// The last entry applied.
    pub fn applied(&self) -> u64 {
        self.shell.applied().index
    }

    /// The last entry the member knows committed.
    pub fn committed(&self) -> u64 {
        self.shell.core().raft.log().committed()
    }

    /// The commit the member's durable state states: its log's, or its engine's durable index,
    /// whichever is greater. A restart opens knowing it committed.
    pub fn durable_commit(&self) -> u64 {
        self.shell.durable_commit()
    }

    /// The group's configuration as of the last entry applied.
    pub fn configuration(&self) -> &ConfState {
        self.shell.configuration()
    }

    /// Whether this member leads and every voter of its configuration has said its durable
    /// commit reaches the entry that made it (the core's answers carry the durable commit, R-6),
    /// the leader counting its own. A member counts by the newest configuration its log states
    /// (hyper-raft `docs/raft.md` §3.4), so a voter whose log lacks the change still counts the
    /// members it removed, and losing the leader could leave no quorum it can elect in; once
    /// every voter's durable commit reaches the change, its log holds it, and losing any one
    /// member leaves voters that elect under this configuration (docs/design/replica.md §6).
    pub fn configuration_known(&self) -> bool {
        self.shell.configuration_known()
    }

    /// Whether this member leads and `member` has confirmed holding every entry it knows
    /// committed: a member that could vote now without a quorum waiting on it to catch up.
    pub fn caught_up(&self, member: u64) -> bool {
        self.shell.caught_up(member)
    }

    /// Whether the member's log may lack entries it acknowledged (docs/design/replica.md §4).
    pub fn is_uncertain(&self) -> bool {
        self.shell.mark().is_some()
    }

    /// Whether the replica is fenced (`ReplicaError::Fenced`).
    pub fn is_fenced(&self) -> bool {
        self.shell.fenced().is_some()
    }

    /// Whether a write waits for room in the log (`ReplicaError::Stalled`).
    pub fn is_stalled(&self) -> bool {
        self.shell.is_stalled()
    }

    /// Writes out on the log, not yet answered.
    pub fn in_flight(&self) -> usize {
        self.shell.in_flight()
    }

    /// Whether the member knows a change of configuration committed whose commit its durable
    /// state does not state yet: a crash now reopens it without the change applied, whatever it
    /// said before (docs/design/replica.md §6). The window the real-process test kills a member in.
    pub fn change_unlogged(&self) -> Result<bool, ReplicaError> {
        let log = self.shell.core().raft.log();
        let (durable, committed) = (self.shell.durable_commit(), log.committed());
        if committed <= durable {
            return Ok(false);
        }
        let low = durable
            .checked_add(1)
            .ok_or_else(|| ReplicaError::Stopped("an index past u64".into()))?;
        let high = committed
            .checked_add(1)
            .ok_or_else(|| ReplicaError::Stopped("an index past u64".into()))?;
        Ok(log.any_entry(low, high, |entry: &Entry| {
            matches!(
                entry.entry_type,
                EntryType::EntryConfChange | EntryType::EntryConfChangeV2
            )
        })?)
    }

    /// A line on the member's state for diagnosis: its term, leader, applied, committed and
    /// durable indexes, log bounds, writes out, and its view of every member's progress.
    pub fn describe(&self) -> String {
        let core = self.shell.core();
        let progress: Vec<String> = core
            .raft
            .tracker()
            .iter()
            .map(|(id, p)| {
                format!(
                    "{id}:m{}n{}c{}s{:?}",
                    p.matched, p.next_index, p.committed_index, p.state
                )
            })
            .collect();
        format!(
            "id {} lead {} term {} applied {} commit {} durable {} out {} mark {:?} progress {:?}",
            self.id(),
            self.leader(),
            self.term(),
            self.applied(),
            self.committed(),
            self.durable_commit(),
            self.in_flight(),
            self.shell.mark(),
            progress
        )
    }

    pub fn engine(&self) -> &E {
        self.shell.machine().engine()
    }

    /// Takes back the engine, as a crash does with the process that held it.
    pub fn into_engine(self) -> E {
        self.shell.into_machine().into_engine()
    }

    /// A tick of the group's clock (docs/durable.md §8): the core campaigns or beats as its
    /// counts say. A replica waiting for room is not ticked, as it takes no part in the group,
    /// and the ticks it missed are not given again.
    pub fn tick(&mut self) -> Result<(), ReplicaError> {
        self.shell.tick().map(drop).map_err(shell)
    }

    /// Takes a message, bound to its sender by the transport. Taken while writes are out; a
    /// replica waiting for room refuses it with `Stalled`, as the network may drop it.
    pub fn step(&mut self, message: Message) -> Result<(), ReplicaError> {
        self.shell.step(message).map_err(shell)
    }

    /// Campaigns now; refused [`ReplicaError::Uncertain`] while its log may lack entries it
    /// acknowledged and the others are no quorum without it (core step R-7).
    pub fn campaign(&mut self) -> Result<(), ReplicaError> {
        self.shell.campaign().map_err(shell)
    }

    /// Proposes an entry of commands; only a leader takes it, and none larger than the range's
    /// bound.
    pub fn propose(&mut self, entry: &wire::Entry) -> Result<(), ReplicaError> {
        let data = entry.encode()?;
        self.bounded(data.len())?;
        self.shell.propose(Vec::new(), data).map_err(shell)
    }

    pub fn propose_change(&mut self, change: &ConfChangeV2) -> Result<(), ReplicaError> {
        self.bounded(change.encoded_len())?;
        self.shell.change(Vec::new(), change).map_err(shell)
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

    /// Asks the group to confirm a read: once a quorum has and the replica has applied through
    /// the index it was confirmed at, a drive gives it back with that index, and rows at or past
    /// it answer the read linearizably. `context` names the read. Refused past the core's bound
    /// on reads waiting; a read whose round is lost is asked again by the leader's heartbeats,
    /// and one a leader that steps down held is dropped, as a lost message is, for its caller to
    /// ask again.
    pub fn read_index(&mut self, context: Vec<u8>) -> Result<(), ReplicaError> {
        self.shell.read(context).map_err(shell)
    }

    /// Reports whether a snapshot this member sent to `to` arrived. Replication to a member
    /// pauses while its snapshot is out, so the transport reports every snapshot's fate; one
    /// that comes while the replica waits for room is kept, the latest for each member.
    pub fn report_snapshot(&mut self, to: u64, arrived: bool) -> Result<(), ReplicaError> {
        self.shell.report_snapshot(to, arrived).map_err(shell)
    }

    /// Does what there is to do (docs/design/replica.md §3): takes the log's answers, applies
    /// what the commit fence allows, one page at most, and takes at most one `Ready`, whose write
    /// goes out with `waker`, woken when the log answers it. `now` is the owner's monotonic clock
    /// in nanoseconds. Appends to `out`, which the owner empties and reuses. `Driven::more` says
    /// driving again now would do more without an answer from the log.
    pub fn drive(
        &mut self,
        now: u64,
        waker: &Waker,
        out: &mut Output,
    ) -> Result<Driven, ReplicaError> {
        self.shell.drive(now, waker, out).map_err(shell)
    }

    /// The owner freed room in the log (another group compacted or left): a replica waiting for
    /// room makes its refused writes again at its next drive.
    pub fn resume(&mut self) {
        self.shell.resume();
    }

    /// Makes the engine's applied state durable and lets the log free the entries before it,
    /// keeping `keep` entries behind for followers that lag (docs/design/replica.md §4); true
    /// when a compaction's start went out, with `waker`.
    pub fn compact(&mut self, keep: u64, now: u64, waker: &Waker) -> Result<bool, ReplicaError> {
        self.shell.compact(keep, now, waker).map_err(shell)
    }
}
