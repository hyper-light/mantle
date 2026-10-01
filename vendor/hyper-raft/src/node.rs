//! The member as its owner drives it: one [`Ready`] at a time says what to
//! persist, send and apply; [`RawNode::advance_append`] says it is
//! persisted, and [`RawNode::advance_apply_to`] how far it is applied.
//!
//! A follower's messages answer for what it holds, so they are sent only
//! after what the same `Ready` persists is durable
//! ([`Ready::persisted_messages`]). A leader's may go at once: what it
//! sends its members persist for themselves (Ongaro's thesis §10.2.1).
use crate::wire::Record;

use crate::{
    NodeId,
    error::{Error, Result},
    proto::{
        self, ConfChange, ConfChangeV2, ConfState, Entry, EntryType, HardState, Message,
        MessageType, Plan, Snapshot,
    },
    raft::{Config, Raft, SoftState, StateRole},
    read::ReadState,
    storage::Storage,
};

/// What became of a snapshot that was sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotStatus {
    /// It was sent: the leader waits for the member's answer and probes
    /// past it.
    Finish,
    /// It did not arrive: the leader waits a heartbeat and probes past what
    /// the member is known to hold.
    Failure,
}

/// What a member sends itself and the network never carries.
pub fn is_local(kind: MessageType) -> bool {
    matches!(
        kind,
        MessageType::MsgHup
            | MessageType::MsgBeat
            | MessageType::MsgUnreachable
            | MessageType::MsgSnapStatus
            | MessageType::MsgCheckQuorum
    )
}
fn is_answer(kind: MessageType) -> bool {
    matches!(
        kind,
        MessageType::MsgAppendResponse
            | MessageType::MsgRequestVoteResponse
            | MessageType::MsgHeartbeatResponse
            | MessageType::MsgUnreachable
            | MessageType::MsgRequestPreVoteResponse
    )
}

/// What follows once a [`Ready`] is persisted.
#[derive(Debug, Default, PartialEq)]
pub struct LightReady {
    commit_index: Option<u64>,
    committed_entries: Vec<Entry>,
    /// What to apply, where storage holds it, for a `Ready` given in place.
    committed_range: Option<(u64, u64)>,
    messages: Vec<Message>,
}
impl LightReady {
    /// The commit, when it moved. It need not be durable to be acted on.
    pub fn commit_index(&self) -> Option<u64> {
        self.commit_index
    }
    /// Committed and durable here: to apply.
    pub fn committed_entries(&self) -> &[Entry] {
        &self.committed_entries
    }
    /// Takes the entries to apply, leaving none.
    pub fn take_committed_entries(&mut self) -> Vec<Entry> {
        std::mem::take(&mut self.committed_entries)
    }
    /// Committed and durable here, to apply where storage holds them: the
    /// first and last index, for a `Ready` given in place
    /// ([`RawNode::ready_in_place`]), whose
    /// [`LightReady::committed_entries`] are none.
    pub fn committed_range(&self) -> Option<(u64, u64)> {
        self.committed_range
    }
    /// To send.
    pub fn messages(&self) -> &[Message] {
        &self.messages
    }
    /// Takes the messages to send, leaving none.
    pub fn take_messages(&mut self) -> Vec<Message> {
        std::mem::take(&mut self.messages)
    }
}

/// What the member asks of its owner at once: what to persist, send and
/// apply. One is out at a time, until [`RawNode::advance_append`].
#[derive(Debug, Default, PartialEq)]
pub struct Ready {
    number: u64,
    soft_state: Option<SoftState>,
    hard_state: Option<HardState>,
    read_states: Vec<ReadState>,
    entries: Vec<Entry>,
    proposals: Vec<Entry>,
    displaced: Vec<Entry>,
    snapshot: Option<Snapshot>,
    after_persisting: bool,
    light: LightReady,
    must_sync: bool,
}
impl Ready {
    /// Which `Ready` this is, counted from one since the member opened.
    pub fn number(&self) -> u64 {
        self.number
    }
    /// Who leads and what this member is, when either changed.
    pub fn soft_state(&self) -> Option<&SoftState> {
        self.soft_state.as_ref()
    }
    /// To persist, when it changed.
    pub fn hard_state(&self) -> Option<&HardState> {
        self.hard_state.as_ref()
    }
    /// Reads that may be served once their index is applied.
    pub fn read_states(&self) -> &[ReadState] {
        &self.read_states
    }
    /// Takes the reads, leaving none.
    pub fn take_read_states(&mut self) -> Vec<ReadState> {
        std::mem::take(&mut self.read_states)
    }
    /// To persist, replacing what storage holds from the first of them on.
    /// A `Ready` given in place gives none here; its entries are read with
    /// [`RawNode::to_persist`].
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }
    /// Takes the entries to persist, leaving none.
    pub fn take_entries(&mut self) -> Vec<Entry> {
        std::mem::take(&mut self.entries)
    }
    /// To persist beside the log: what this member approved by itself
    /// ([`crate::fast`]). What it holds it says only once this is durable,
    /// and storage gives it back when the member opens
    /// ([`crate::InitialState::proposals`]) until the log reaches its
    /// index.
    pub fn proposals(&self) -> &[Entry] {
        &self.proposals
    }
    /// What was proposed here by the fast track and another entry took the
    /// index of: its proposer proposes it again.
    pub fn displaced(&self) -> &[Entry] {
        &self.displaced
    }
    /// Takes the displaced entries, leaving none.
    pub fn take_displaced(&mut self) -> Vec<Entry> {
        std::mem::take(&mut self.displaced)
    }
    /// To persist before the entries: the log begins again after it. A
    /// `Ready` given in place gives none here; its snapshot is read with
    /// [`RawNode::to_persist`].
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }
    /// Committed and durable here: to apply.
    pub fn committed_entries(&self) -> &[Entry] {
        self.light.committed_entries()
    }
    /// Takes the entries to apply, leaving none.
    pub fn take_committed_entries(&mut self) -> Vec<Entry> {
        self.light.take_committed_entries()
    }
    /// Committed and durable here, to apply where storage holds them, for a
    /// `Ready` given in place ([`LightReady::committed_range`]).
    pub fn committed_range(&self) -> Option<(u64, u64)> {
        self.light.committed_range()
    }
    /// To send at once.
    pub fn messages(&self) -> &[Message] {
        if self.after_persisting {
            &[]
        } else {
            self.light.messages()
        }
    }
    /// Takes the messages to send at once, leaving none.
    pub fn take_messages(&mut self) -> Vec<Message> {
        if self.after_persisting {
            Vec::new()
        } else {
            self.light.take_messages()
        }
    }
    /// To send once what this `Ready` persists is durable.
    pub fn persisted_messages(&self) -> &[Message] {
        if self.after_persisting {
            self.light.messages()
        } else {
            &[]
        }
    }
    /// Takes the messages to send once this `Ready` is durable, leaving
    /// none.
    pub fn take_persisted_messages(&mut self) -> Vec<Message> {
        if self.after_persisting {
            self.light.take_messages()
        } else {
            Vec::new()
        }
    }
    /// False when nothing but the commit is to persist: that write need
    /// not be waited for.
    pub fn must_sync(&self) -> bool {
        self.must_sync
    }
}

/// What a `Ready` given in place asks to persist, where the member holds
/// it, beside the storage it goes to ([`RawNode::to_persist`]).
#[derive(Debug)]
pub struct ToPersist<'a, S> {
    /// The storage, to write to.
    pub store: &'a mut S,
    /// The snapshot to persist first, if there is one.
    pub snapshot: Option<&'a Snapshot>,
    /// The entries to persist, replacing what storage holds from the first
    /// of them on.
    pub entries: &'a [Entry],
}

/// What a `Ready` gave to persist, given up by the member once it is durable
/// ([`RawNode::advance_append_keeping`]).
#[derive(Debug, Default, PartialEq)]
pub struct Kept {
    /// The snapshot, if the `Ready` gave one.
    pub snapshot: Option<Snapshot>,
    /// The entries.
    pub entries: Vec<Entry>,
}

/// What a [`Ready`] gave to persist.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Given {
    number: u64,
    last_entry: Option<(u64, u64)>,
    snapshot: Option<u64>,
    /// Given in place: its entries are read where the member holds them,
    /// and what it gives to apply where storage holds it.
    in_place: bool,
}

/// A member as its owner drives it: operations in, one [`Ready`] at a time
/// out.
pub struct RawNode<S> {
    /// The state machine itself.
    pub raft: Raft<S>,
    previous_soft: SoftState,
    previous_hard: HardState,
    number: u64,
    /// The `Ready` that is out; one at a time.
    given: Option<Given>,
    /// What was given to apply reaches this index.
    commit_since: u64,
}

impl<S: Storage> RawNode<S> {
    /// The member `config` names, opened on what `store` holds.
    pub fn new(config: &Config, store: S) -> Result<Self> {
        let raft = Raft::new(config, store)?;
        Ok(Self {
            previous_soft: raft.soft_state(),
            previous_hard: raft.hard_state(),
            raft,
            number: 0,
            given: None,
            commit_since: config.applied,
        })
    }
    /// The storage the member reads.
    pub fn store(&self) -> &S {
        self.raft.store()
    }
    /// The storage the member reads, for its owner to write.
    pub fn store_mut(&mut self) -> &mut S {
        self.raft.store_mut()
    }
    /// Whether a `Ready` is out and not yet advanced.
    pub fn outstanding(&self) -> bool {
        self.given.is_some()
    }
    /// An operation on the member. The priority in force is settled before
    /// it and after, never within. While a `Ready` is out the member does
    /// not change.
    fn operate<T>(&mut self, operation: impl FnOnce(&mut Raft<S>) -> Result<T>) -> Result<T> {
        if self.given.is_some() {
            return Err(Error::Invariant("an operation while a ready is out"));
        }
        self.raft.settle_priority();
        let outcome = operation(&mut self.raft);
        self.raft.settle_priority();
        outcome
    }
    /// The priority this member's elections are judged by from the next
    /// operation on ([`Config::priority`]).
    pub fn set_priority(&mut self, priority: i64) {
        self.raft.set_priority(priority);
    }
    /// What the path to `member` carries before it answers
    /// ([`Raft::set_inflight_bytes`]).
    pub fn set_inflight_bytes(&mut self, member: u64, bytes: u64) -> bool {
        self.raft.set_inflight_bytes(member, bytes)
    }
    /// One tick of time has passed. True when the member acted on it: it
    /// campaigned, checked its quorum or sent heartbeats.
    pub fn tick(&mut self) -> Result<bool> {
        self.operate(Raft::tick)
    }
    /// Campaigns now, by pre-vote when the group runs it.
    pub fn campaign(&mut self) -> Result<()> {
        self.operate(|raft| raft.step(proto::message(0, MessageType::MsgHup)))
    }
    /// Proposes an entry stating `data`, which the leader appends.
    pub fn propose(&mut self, context: Vec<u8>, data: Vec<u8>) -> Result<()> {
        self.operate(|raft| {
            let mut message = proto::message(0, MessageType::MsgPropose);
            message.from = raft.id();
            message
                .entries
                .try_reserve_exact(1)
                .map_err(|_| Error::Capacity("a proposal"))?;
            message.entries.push(Entry {
                data,
                context,
                ..Entry::default()
            });
            raft.step(message)
        })
    }
    /// Proposes by the fast track ([`crate::fast`]); the index proposed
    /// for.
    pub fn propose_fast(&mut self, context: Vec<u8>, data: Vec<u8>) -> Result<u64> {
        self.operate(|raft| raft.propose_fast(context, data))
    }
    /// Proposes a change of the configuration; one that could not be read
    /// when applied is refused here.
    pub fn propose_conf_change(&mut self, context: Vec<u8>, change: &ConfChangeV2) -> Result<()> {
        // What could not be read when it is applied is not proposed.
        Plan::of(change)?;
        // The empty change, leaving the joint configuration, is written as no data, as the
        // leader writes its own leave: one change, one encoding.
        let data = if *change == ConfChangeV2::default() {
            Vec::new()
        } else {
            change.encode_to_vec()
        };
        self.operate(|raft| {
            let mut message = proto::message(0, MessageType::MsgPropose);
            message
                .entries
                .try_reserve_exact(1)
                .map_err(|_| Error::Capacity("a proposal"))?;
            message.entries.push(Entry {
                entry_type: EntryType::EntryConfChangeV2,
                data,
                context,
                ..Entry::default()
            });
            raft.step(message)
        })
    }
    /// A committed change is applied. One the application decides not to
    /// apply is not given here.
    pub fn apply_conf_change(&mut self, change: &ConfChangeV2) -> Result<ConfState> {
        let plan = Plan::of(change)?;
        self.raft.settle_priority();
        let outcome = self.raft.apply_conf_change(&plan);
        self.raft.settle_priority();
        outcome
    }
    /// A committed change in the older encoding is applied.
    pub fn apply_conf_change_v1(&mut self, change: &ConfChange) -> Result<ConfState> {
        let plan = Plan::of(&proto::joint(change))?;
        self.raft.settle_priority();
        let outcome = self.raft.apply_conf_change(&plan);
        self.raft.settle_priority();
        outcome
    }
    /// A message from the network.
    pub fn step(&mut self, message: Message) -> Result<()> {
        if message.msg_type == crate::fast::FAST_PROPOSE
            || message.msg_type == crate::fast::FAST_VOTE
        {
            return self.operate(|raft| raft.step(message));
        }
        let kind = message.msg_type;
        if is_local(kind) {
            return Err(Error::StepLocalMessage);
        }
        if is_answer(kind) && self.raft.tracker().get(message.from).is_none() {
            return Err(Error::StepPeerNotFound);
        }
        self.operate(|raft| raft.step(message))
    }
    /// A leader sends its heartbeats now.
    pub fn ping(&mut self) -> Result<()> {
        self.operate(Raft::ping)
    }
    /// What a member asks of itself on its owner's word. A refusal is no
    /// one's to hear.
    fn tell(&mut self, message: Message) -> Result<()> {
        match self.operate(|raft| raft.step(message)) {
            Err(error) if error.is_fatal() => Err(error),
            _ => Ok(()),
        }
    }
    /// The last message to `member` did not arrive.
    pub fn report_unreachable(&mut self, member: NodeId) -> Result<()> {
        let mut message = proto::message(0, MessageType::MsgUnreachable);
        message.from = member;
        self.tell(message)
    }
    /// What became of the snapshot sent to `member`.
    pub fn report_snapshot(&mut self, member: NodeId, status: SnapshotStatus) -> Result<()> {
        let mut message = proto::message(0, MessageType::MsgSnapStatus);
        message.from = member;
        message.reject = status == SnapshotStatus::Failure;
        self.tell(message)
    }
    /// Asks the leader for a snapshot that reaches this member's log.
    pub fn request_snapshot(&mut self) -> Result<()> {
        self.operate(Raft::request_snapshot)
    }
    /// `transferee` shall lead.
    pub fn transfer_leader(&mut self, transferee: NodeId) -> Result<()> {
        let mut message = proto::message(0, MessageType::MsgTransferLeader);
        message.from = transferee;
        self.tell(message)
    }
    /// Asks at which index a read may be served; the answer comes in a
    /// `Ready` with the same `context`.
    pub fn read_index(&mut self, context: Vec<u8>) -> Result<()> {
        let mut message = proto::message(0, MessageType::MsgReadIndex);
        message
            .entries
            .try_reserve_exact(1)
            .map_err(|_| Error::Capacity("a read"))?;
        message.entries.push(Entry {
            data: context,
            ..Entry::default()
        });
        match self.operate(|raft| raft.step(message)) {
            Err(error) if error.is_fatal() || matches!(error, Error::Capacity(_)) => Err(error),
            _ => Ok(()),
        }
    }

    fn light(&mut self, in_place: bool) -> Result<LightReady> {
        if in_place {
            return self.light_in_place();
        }
        let committed_entries = self
            .raft
            .log()
            .next_entries_since(self.commit_since, self.raft.committed_bytes_per_ready())?;
        self.raft.reduce_uncommitted(&committed_entries);
        if let Some(last) = committed_entries.last() {
            self.given_through(last.index)?;
        }
        Ok(LightReady {
            commit_index: None,
            committed_entries,
            committed_range: None,
            messages: self.raft.msgs.take(),
        })
    }
    /// As [`RawNode::light`], giving what to apply as the range storage
    /// holds it, copied nowhere.
    fn light_in_place(&mut self) -> Result<LightReady> {
        let range = self.raft.log().next_range_since(
            self.commit_since,
            self.raft.committed_bytes_per_ready(),
            self.raft.leader_tail(),
        )?;
        let committed_range = match range {
            Some(range) => {
                self.raft.reduce_uncommitted_bytes(range.data_above);
                self.given_through(range.last)?;
                Some((range.first, range.last))
            }
            None => None,
        };
        Ok(LightReady {
            commit_index: None,
            committed_entries: Vec::new(),
            committed_range,
            messages: self.raft.msgs.take(),
        })
    }
    /// Entries through `last` were given to apply.
    fn given_through(&mut self, last: u64) -> Result<()> {
        if self.commit_since >= last {
            return Err(Error::Invariant("entries given to apply twice"));
        }
        self.commit_since = last;
        Ok(())
    }
    /// Whether every counter the member trusts for what it holds says what
    /// a walk says ([`Raft::check_accounting`]).
    pub fn check_accounting(&self) -> Result<()> {
        self.raft.check_accounting()
    }
    /// Whether [`RawNode::ready`] has anything to give.
    pub fn has_ready(&self) -> bool {
        let raft = &self.raft;
        !raft.msgs.is_empty()
            || raft.reads_unasked()
            || raft.soft_state() != self.previous_soft
            || raft.hard_state() != self.previous_hard
            || !raft.read_states.is_empty()
            || !raft.displaced.is_empty()
            || raft.held.has_unstable()
            || !raft.log().unstable().entries().is_empty()
            || raft
                .snapshot()
                .is_some_and(|snapshot| !proto::snapshot_is_empty(snapshot))
            || raft
                .log()
                .has_next_entries_since(self.commit_since)
                .unwrap_or(false)
    }
    /// What there is to do. It is done, and advanced, before the member
    /// is given anything else.
    pub fn ready(&mut self) -> Result<Ready> {
        self.ready_given(false)
    }
    /// What there is to do, as [`RawNode::ready`] says it, with nothing
    /// copied that the owner can read where it is:
    /// - the entries to persist are read with [`RawNode::to_persist`] while
    ///   the `Ready` is out, and persisted before anything else is asked of
    ///   the member; [`Ready::entries`] are none;
    /// - what is committed is given as the range storage holds it
    ///   ([`Ready::committed_range`], [`LightReady::committed_range`]),
    ///   chosen by the rule [`Log::next_entries_since`] pages by, and
    ///   [`Ready::committed_entries`] are none.
    ///
    /// An owner that writes entries out (to a file, to a device) and applies
    /// what it holds copies nothing; the member's decisions are those of
    /// [`RawNode::ready`] exactly.
    ///
    /// [`Log::next_entries_since`]: crate::log::Log::next_entries_since
    pub fn ready_in_place(&mut self) -> Result<Ready> {
        self.ready_given(true)
    }
    /// The storage, and what the `Ready` that is out gives to persist,
    /// where the member holds it ([`RawNode::ready_in_place`]): the snapshot
    /// first, if there is one, then the entries.
    pub fn to_persist(&mut self) -> ToPersist<'_, S> {
        let log = &mut self.raft.log;
        ToPersist {
            store: &mut log.store,
            snapshot: log.unstable.snapshot.as_ref(),
            entries: log.unstable.entries.as_slice(),
        }
    }
    fn ready_given(&mut self, in_place: bool) -> Result<Ready> {
        if self.given.is_some() {
            return Err(Error::Invariant("a ready asked for while one is out"));
        }
        let number = self
            .number
            .checked_add(1)
            .ok_or(Error::Capacity("readies"))?;
        // One round for the reads asked since the last (`Raft::ask_reads`): it
        // leaves with this `Ready`. A round refused is asked for again by
        // the next `Ready`, and by the heartbeat the leader's clock sends.
        self.raft.ask_reads()?;
        let mut ready = Ready {
            number,
            ..Ready::default()
        };
        let mut given = Given {
            number,
            in_place,
            ..Given::default()
        };
        // Everything that can refuse does before the member is changed.
        if let Some(snapshot) = self.raft.snapshot() {
            let index = proto::snapshot_index(snapshot);
            if self.commit_since > index {
                return Err(Error::Invariant(
                    "a snapshot behind what was given to apply",
                ));
            }
            // Entries after a snapshot are not durable before it is, so
            // none is given to apply with it.
            if self.raft.log().has_next_entries_since(index)? {
                return Err(Error::Invariant(
                    "a snapshot with entries to apply after it",
                ));
            }
            if !in_place {
                ready.snapshot = Some(crate::log::copy_snapshot(snapshot)?);
            }
            given.snapshot = Some(index);
            ready.must_sync = true;
        }
        if !in_place {
            crate::log::copy_entries(self.raft.log().unstable().entries(), &mut ready.entries)?;
        }
        let last_entry = self
            .raft
            .log()
            .unstable()
            .entries()
            .last()
            .map(|last| (last.index, last.term));
        self.raft.unstable_proposals(&mut ready.proposals)?;

        if let Some(index) = given.snapshot {
            self.commit_since = index;
        }
        ready.light = self.light(in_place)?;
        self.number = number;
        let soft = self.raft.soft_state();
        if soft != self.previous_soft {
            ready.soft_state = Some(soft);
        }
        let hard = self.raft.hard_state();
        if hard != self.previous_hard {
            if hard.vote != self.previous_hard.vote || hard.term != self.previous_hard.term {
                ready.must_sync = true;
            }
            ready.hard_state = Some(hard);
        }
        // Taken, and not emptied: what the member holds when it rests is
        // what it held before.
        ready.read_states = std::mem::take(&mut self.raft.read_states);
        ready.displaced = self.raft.take_displaced();
        if !ready.proposals.is_empty() {
            ready.must_sync = true;
        }
        if last_entry.is_some() {
            ready.must_sync = true;
            given.last_entry = last_entry;
        }
        ready.after_persisting = self.raft.state() != StateRole::Leader;
        self.given = Some(given);
        Ok(ready)
    }
    /// What `ready` gave to persist is durable, and storage answers for it.
    pub fn advance_append(&mut self, ready: Ready) -> Result<LightReady> {
        self.advance_append_keeping(ready, |_, _| {})
    }
    /// As [`RawNode::advance_append`], handing what `ready` gave to persist
    /// to `keep`, with the storage, before the member reads storage for it:
    /// an owner that wrote it out where it was ([`RawNode::ready_in_place`])
    /// and keeps it in memory too keeps these very entries and this very
    /// snapshot, and copies none. `keep` must leave storage holding them, as
    /// `advance_append` requires; what else it does with them is the owner's.
    pub fn advance_append_keeping(
        &mut self,
        ready: Ready,
        keep: impl FnOnce(&mut S, Kept),
    ) -> Result<LightReady> {
        let given = self
            .given
            .filter(|given| given.number == ready.number)
            .ok_or(Error::Invariant("a ready advanced that is not the one out"))?;
        if let Some(soft) = ready.soft_state {
            self.previous_soft = soft;
        }
        if let Some(hard) = ready.hard_state {
            self.previous_hard = hard;
        }
        let mut kept = Kept::default();
        if let Some(index) = given.snapshot {
            kept.snapshot = Some(self.raft.log.take_stable_snapshot(index)?);
        }
        if let Some((index, term)) = given.last_entry {
            kept.entries = self.raft.log.take_stable_entries(index, term)?;
        }
        if kept.snapshot.is_some() || !kept.entries.is_empty() {
            keep(&mut self.raft.log.store, kept);
        }
        self.given = None;
        self.raft.settle_priority();
        if let Some(index) = given.snapshot {
            self.raft.on_persist_snapshot(index)?;
        }
        if let Some((index, term)) = given.last_entry {
            self.raft.on_persist_entries(index, term)?;
        }
        self.raft.on_persist_proposals(&ready.proposals)?;
        self.raft.settle_priority();
        // What is to send now is sent after what `ready` persisted, whoever
        // sends it: a leader that a change it applied made a follower has
        // its last messages here.
        let mut light = self.light(given.in_place)?;
        let hard = self.raft.hard_state();
        if hard.commit > self.previous_hard.commit {
            light.commit_index = Some(hard.commit);
            self.previous_hard.commit = hard.commit;
        }
        if hard != self.previous_hard {
            return Err(Error::Invariant(
                "a term or a vote moved while a ready was out",
            ));
        }
        Ok(light)
    }
    /// The application applied through `applied`.
    pub fn advance_apply_to(&mut self, applied: u64) -> Result<()> {
        self.raft.settle_priority();
        let outcome = self.raft.commit_apply(applied);
        self.raft.settle_priority();
        outcome
    }
    /// Persisted and applied, for an owner that does both at once.
    pub fn advance(&mut self, ready: Ready) -> Result<LightReady> {
        let applied = self.commit_since;
        let light = self.advance_append(ready)?;
        self.advance_apply_to(applied)?;
        Ok(light)
    }
    /// Through which index entries were given to apply.
    pub fn given_to_apply(&self) -> u64 {
        self.commit_since
    }
}
