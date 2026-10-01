//! Two cores on one schedule. `Old` is `raft-rs` as focal's shell drives
//! it; `New` is this crate. Both keep their log on a [`Store`] the harness
//! can crash and compact, and both say what they did in the same words
//! ([`Output`], [`View`]), so what they say is compared for equality.
#![allow(dead_code)]
use hyper_raft::proto::{
    CAMPAIGN_TRANSFER, ConfChange, ConfChangeV2, ConfState, Entry, EntryType, HardState, Message,
    MessageType, Snapshot, SnapshotMetadata,
};
use hyper_raft::wire::Record;

pub mod convert;

/// SplitMix64: the schedule of a run is its seed.
#[derive(Clone)]
pub struct Seeded(pub u64);
impl Seeded {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut drawn = self.0;
        drawn = (drawn ^ (drawn >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        drawn = (drawn ^ (drawn >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        drawn ^ (drawn >> 31)
    }
    pub fn below(&mut self, bound: u64) -> u64 {
        ((u128::from(self.next()) * u128::from(bound)) >> 64) as u64
    }
    pub fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
    pub fn pick<T: Copy>(&mut self, from: &[T]) -> Option<T> {
        if from.is_empty() {
            None
        } else {
            Some(from[self.below(from.len() as u64) as usize])
        }
    }
}

pub fn sorted(mut state: ConfState) -> ConfState {
    state.voters.sort_unstable();
    state.learners.sort_unstable();
    state.voters_outgoing.sort_unstable();
    state.learners_next.sort_unstable();
    state
}
pub fn votes(state: &ConfState, member: u64) -> bool {
    state.voters.contains(&member) || state.voters_outgoing.contains(&member)
}
pub fn members(state: &ConfState) -> Vec<u64> {
    let mut members: Vec<u64> = state
        .voters
        .iter()
        .chain(&state.voters_outgoing)
        .chain(&state.learners)
        .chain(&state.learners_next)
        .copied()
        .collect();
    members.sort_unstable();
    members.dedup();
    members
}

#[derive(Clone, Debug, Default)]
pub struct Disk {
    pub hard_state: HardState,
    /// The configuration as applied.
    pub conf: ConfState,
    /// The configuration the group began with.
    pub boot: ConfState,
    pub snapshot: Snapshot,
    pub entries: Vec<Entry>,
    /// What the member approved by itself.
    pub proposals: Vec<Entry>,
}
impl Disk {
    pub fn snapshot_index(&self) -> u64 {
        self.snapshot.metadata.as_ref().map_or(0, |m| m.index)
    }
    pub fn snapshot_term(&self) -> u64 {
        self.snapshot.metadata.as_ref().map_or(0, |m| m.term)
    }
    pub fn first_index(&self) -> u64 {
        self.snapshot_index() + 1
    }
    pub fn last_index(&self) -> u64 {
        self.snapshot_index() + self.entries.len() as u64
    }
    pub fn term(&self, index: u64) -> Option<u64> {
        if index == self.snapshot_index() {
            return Some(self.snapshot_term());
        }
        if index < self.snapshot_index() {
            return None;
        }
        self.entries
            .get((index - self.first_index()) as usize)
            .map(|entry| entry.term)
    }
    pub fn append(&mut self, entries: &[Entry]) {
        for entry in entries {
            if entry.index < self.first_index() {
                continue;
            }
            assert!(
                entry.index <= self.last_index() + 1,
                "a gap in what is persisted"
            );
            self.entries
                .truncate((entry.index - self.first_index()) as usize);
            self.entries.push(entry.clone());
        }
        let last = self.last_index();
        self.proposals.retain(|held| held.index > last);
    }
    /// Keeps the entries a member gave up once durable, as they are: what
    /// they replace is cut first, as `append` cuts it.
    pub fn keep(&mut self, entries: Vec<Entry>) {
        let Some(first) = entries.first() else {
            return;
        };
        assert!(
            first.index >= self.first_index() && first.index <= self.last_index() + 1,
            "a gap in what is kept"
        );
        self.entries
            .truncate((first.index - self.first_index()) as usize);
        self.entries.extend(entries);
        self.trim_proposals();
    }
    /// What the log reached is approved by itself no more.
    pub fn trim_proposals(&mut self) {
        let last = self.last_index();
        self.proposals.retain(|held| held.index > last);
    }
    pub fn install(&mut self, snapshot: &Snapshot) {
        self.install_owned(snapshot.clone());
    }
    /// Installs the snapshot itself.
    pub fn install_owned(&mut self, snapshot: Snapshot) {
        let metadata = snapshot.metadata.clone().unwrap_or_default();
        self.conf = sorted(metadata.conf_state.unwrap_or_default());
        self.hard_state.commit = self.hard_state.commit.max(metadata.index);
        self.entries.clear();
        self.snapshot = snapshot;
        self.proposals.retain(|held| held.index > metadata.index);
    }
    /// Everything through `index` becomes the snapshot.
    pub fn compact(&mut self, index: u64, data: Vec<u8>) {
        let term = self.term(index).expect("the compacted index is held");
        let keep = (index + 1 - self.first_index()) as usize;
        self.entries.drain(..keep);
        self.snapshot = Snapshot {
            data,
            metadata: Some(SnapshotMetadata {
                conf_state: Some(self.conf.clone()),
                index,
                term,
            }),
        };
    }
    /// What a member that opens finds applied: the snapshot's
    /// configuration, or the one the group began with.
    pub fn reopen(&mut self) {
        self.conf = match self.snapshot.metadata.as_ref() {
            Some(metadata) if metadata.index > 0 => {
                sorted(metadata.conf_state.clone().unwrap_or_default())
            }
            _ => self.boot.clone(),
        };
    }
    /// The page is chosen before it is copied, as the core's own storage
    /// chooses it: what is returned holds no spare room.
    /// Each core measures a page in its own bytes: `measure`.
    fn slice(&self, low: u64, high: u64, max_bytes: u64, measure: fn(&Entry) -> u64) -> Vec<Entry> {
        let first = self.first_index();
        let range = &self.entries[(low - first) as usize..(high - first) as usize];
        let mut bytes = 0u64;
        let mut kept = 0usize;
        for entry in range {
            bytes += measure(entry);
            if kept > 0 && bytes > max_bytes {
                break;
            }
            kept += 1;
        }
        let mut page = Vec::new();
        page.try_reserve_exact(kept).expect("a page");
        page.extend(range[..kept].iter().cloned());
        page
    }
}

/// One member's disk. The member's node owns it while it runs, and the
/// cluster holds it while the member is stopped (`Cluster::stop`): one
/// owner at a time, so nothing is shared.
#[derive(Clone, Default)]
pub struct Store(pub Disk);
impl Store {
    pub fn new(boot: ConfState) -> Self {
        let boot = sorted(boot);
        Self(Disk {
            conf: boot.clone(),
            boot,
            ..Disk::default()
        })
    }
    /// A copy that shares nothing, for the other core.
    pub fn twin(&self) -> Self {
        Self(self.0.clone())
    }
}
impl raft::Storage for Store {
    fn initial_state(&self) -> raft::Result<raft::RaftState> {
        let disk = &self.0;
        Ok(raft::RaftState {
            hard_state: convert::hard_to(&disk.hard_state),
            conf_state: convert::conf_to(&disk.conf),
        })
    }
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: raft::GetEntriesContext,
    ) -> raft::Result<Vec<raft::prelude::Entry>> {
        let disk = &self.0;
        if low < disk.first_index() {
            return Err(raft::StorageError::Compacted.into());
        }
        if low > high || high > disk.last_index() + 1 {
            return Err(raft::StorageError::Unavailable.into());
        }
        Ok(disk
            .slice(
                low,
                high,
                max_size.into().unwrap_or(u64::MAX),
                convert::old_bytes,
            )
            .iter()
            .map(convert::entry_to)
            .collect())
    }
    fn term(&self, index: u64) -> raft::Result<u64> {
        let disk = &self.0;
        if index < disk.snapshot_index() {
            return Err(raft::StorageError::Compacted.into());
        }
        disk.term(index)
            .ok_or_else(|| raft::StorageError::Unavailable.into())
    }
    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.0.first_index())
    }
    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.0.last_index())
    }
    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<raft::prelude::Snapshot> {
        let disk = &self.0;
        if disk.snapshot_index() == 0 || disk.snapshot_index() < request_index {
            return Err(raft::StorageError::SnapshotTemporarilyUnavailable.into());
        }
        Ok(convert::snapshot_to(&disk.snapshot))
    }
}
impl hyper_raft::Storage for Store {
    fn initial_state(&self) -> Result<hyper_raft::InitialState, hyper_raft::StorageError> {
        let disk = &self.0;
        Ok(hyper_raft::InitialState {
            hard_state: disk.hard_state,
            configuration: disk.conf.clone(),
            proposals: disk.proposals.clone(),
        })
    }
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: &mut Vec<Entry>,
    ) -> Result<(), hyper_raft::StorageError> {
        let disk = &self.0;
        if low < disk.first_index() {
            return Err(hyper_raft::StorageError::Compacted);
        }
        if low > high || high > disk.last_index() + 1 {
            return Err(hyper_raft::StorageError::Unavailable);
        }
        let page = disk.slice(low, high, max_bytes, hyper_raft::proto::encoded_bytes);
        into.try_reserve_exact(page.len())
            .map_err(|_| hyper_raft::StorageError::Unavailable)?;
        into.extend(page);
        Ok(())
    }
    fn any_entry(
        &self,
        low: u64,
        high: u64,
        predicate: &mut dyn FnMut(&Entry) -> bool,
    ) -> Result<bool, hyper_raft::StorageError> {
        let disk = &self.0;
        if low < disk.first_index() {
            return Err(hyper_raft::StorageError::Compacted);
        }
        if low > high || high > disk.last_index() + 1 {
            return Err(hyper_raft::StorageError::Unavailable);
        }
        let first = disk.first_index();
        Ok(
            disk.entries[(low - first) as usize..(high - first) as usize]
                .iter()
                .any(predicate),
        )
    }
    fn term(&self, index: u64) -> Result<u64, hyper_raft::StorageError> {
        let disk = &self.0;
        if index < disk.snapshot_index() {
            return Err(hyper_raft::StorageError::Compacted);
        }
        disk.term(index)
            .ok_or(hyper_raft::StorageError::Unavailable)
    }
    fn first_index(&self) -> Result<u64, hyper_raft::StorageError> {
        Ok(self.0.first_index())
    }
    fn last_index(&self) -> Result<u64, hyper_raft::StorageError> {
        Ok(self.0.last_index())
    }
    fn snapshot(&self, request_index: u64, _to: u64) -> Result<Snapshot, hyper_raft::StorageError> {
        let disk = &self.0;
        if disk.snapshot_index() == 0 || disk.snapshot_index() < request_index {
            return Err(hyper_raft::StorageError::SnapshotTemporarilyUnavailable);
        }
        Ok(disk.snapshot.clone())
    }
}

/// What the application is: a digest of what it applied, in order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct App {
    pub digest: u64,
    pub count: u64,
    pub index: u64,
}
impl App {
    pub fn apply(&mut self, entry: &Entry) {
        let mut digest = self.digest ^ 0xcbf2_9ce4_8422_2325;
        for byte in entry
            .index
            .to_le_bytes()
            .iter()
            .chain(&entry.term.to_le_bytes())
            .chain(&entry.data)
        {
            digest = (digest ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
        self.digest = digest;
        self.count += 1;
        self.index = entry.index;
    }
    pub fn encode(&self) -> Vec<u8> {
        [self.digest, self.count, self.index]
            .iter()
            .flat_map(|word| word.to_le_bytes())
            .collect()
    }
    pub fn decode(bytes: &[u8]) -> Self {
        let word = |at: usize| {
            u64::from_le_bytes(bytes[at * 8..at * 8 + 8].try_into().expect("eight bytes"))
        };
        Self {
            digest: word(0),
            count: word(1),
            index: word(2),
        }
    }
}

pub type Said = (u64, u64, EntryType, Vec<u8>);
fn said(entry: &Entry) -> Said {
    (
        entry.index,
        entry.term,
        entry.entry_type,
        entry.data.clone(),
    )
}

/// What a member did since it was last asked.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Output {
    pub hard_states: Vec<(u64, u64, u64)>,
    pub persisted: Vec<Said>,
    pub snapshots: Vec<(u64, u64)>,
    /// In order of whom they are for, and for each in the order sent.
    pub messages: Vec<Message>,
    pub committed: Vec<Said>,
    pub reads: Vec<(u64, Vec<u8>)>,
    pub confs: Vec<ConfState>,
    /// What was proposed here by the fast track and lost its index.
    pub displaced: Vec<Said>,
    /// A change that could not be applied, by the index of its entry.
    pub refused: Vec<u64>,
    /// A leader applied a change that leaves it no voter. The two cores
    /// differ from here by decision: this one steps down.
    pub leader_left: bool,
}

pub type Member = (u64, u64, u64, u8, bool, u64, u64, bool, usize, u64);

/// What a member is.
#[derive(Clone, Debug, PartialEq)]
pub struct View {
    pub term: u64,
    pub vote: u64,
    pub commit: u64,
    pub leader: u64,
    pub role: u8,
    pub last_index: u64,
    pub persisted: u64,
    pub applied: u64,
    pub timeout: usize,
    pub elapsed: usize,
    pub pending_conf: u64,
    pub transferee: Option<u64>,
    pub promotable: bool,
    pub uncommitted: usize,
    pub pending_reads: usize,
    pub app: App,
    pub members: Vec<Member>,
}

#[derive(Clone, Copy, Debug)]
pub struct Settings {
    pub election_tick: usize,
    pub heartbeat_tick: usize,
    pub max_size_per_msg: u64,
    pub max_inflight_msgs: usize,
    /// The bytes of entries a member is sent ahead of its answers;
    /// `raft-rs` has no such bound.
    pub max_inflight_bytes: u64,
    pub max_uncommitted_size: u64,
    pub max_committed_size_per_ready: u64,
    pub check_quorum: bool,
    pub pre_vote: bool,
    /// Whether priority yields to a longer log alone, as in `raft-rs`.
    pub by_length: bool,
    /// Whether a leader sends a round of heartbeats for each read as it is
    /// asked, as in `raft-rs`.
    pub round_each: bool,
    /// Whether a heartbeat's answer says nothing of the member's log and
    /// frees a full window's first message, as in `raft-rs`.
    pub bare_answers: bool,
    /// Whether the group has the fast track.
    pub fast: bool,
    /// Whether this core's members are driven by `RawNode::ready_in_place`,
    /// persisting their entries and applying what is committed where they
    /// are, rather than by `RawNode::ready`'s copies.
    pub in_place: bool,
}
impl Settings {
    /// As focal's shell sets a group.
    pub fn shell() -> Self {
        Self {
            election_tick: 10,
            heartbeat_tick: 2,
            max_size_per_msg: 4 * 1024 * 1024 + 1024,
            max_inflight_msgs: 128,
            max_inflight_bytes: u64::MAX,
            max_uncommitted_size: 32 * 1024 * 1024,
            max_committed_size_per_ready: 16 * 1024 * 1024,
            check_quorum: true,
            pre_vote: true,
            by_length: true,
            round_each: true,
            bare_answers: true,
            fast: false,
            in_place: false,
        }
    }
    /// As focal runs this core.
    pub fn focal() -> Self {
        Self {
            by_length: false,
            round_each: false,
            bare_answers: false,
            // A few of a schedule's entries: the window fills by its
            // bytes long before it fills by its places.
            max_inflight_bytes: 256,
            ..Self::shell()
        }
    }
    /// A group with the fast track.
    pub fn fast() -> Self {
        Self {
            fast: true,
            ..Self::focal()
        }
    }
}

pub trait Replica: Sized {
    fn open(id: u64, store: Store, settings: &Settings, seed: u64) -> Self;
    fn id(&self) -> u64;
    fn store(&self) -> &Store;
    fn store_mut(&mut self) -> &mut Store;
    fn tick(&mut self) -> bool;
    fn step(&mut self, message: Message) -> bool;
    fn propose(&mut self, data: Vec<u8>) -> bool;
    /// By the fast track; the index proposed for.
    fn propose_fast(&mut self, _data: Vec<u8>) -> Option<u64> {
        None
    }
    fn propose_change(&mut self, change: &ConfChangeV2) -> bool;
    fn campaign(&mut self) -> bool;
    fn ping(&mut self);
    fn transfer(&mut self, to: u64);
    fn read(&mut self, context: Vec<u8>);
    fn unreachable(&mut self, member: u64);
    fn snapshot_status(&mut self, member: u64, arrived: bool);
    fn set_priority(&mut self, priority: i64);
    /// What the path to `member` carries before it answers. `raft-rs` has
    /// no such bound and does nothing.
    fn set_window(&mut self, _member: u64, _bytes: u64) {}
    fn set_timeout(&mut self, ticks: usize);
    fn drain(&mut self) -> Output;
    fn view(&self) -> View;
    fn app(&self) -> App;
    /// Everything applied becomes the snapshot. False when there is
    /// nothing new to compact.
    fn compact(&mut self) -> bool {
        let app = self.app();
        let disk = &mut self.store_mut().0;
        if app.index <= disk.snapshot_index() || app.index > disk.last_index() {
            return false;
        }
        disk.compact(app.index, app.encode());
        true
    }
}

fn canonical(mut messages: Vec<Message>) -> Vec<Message> {
    messages.sort_by_key(|message| message.to);
    for message in &mut messages {
        if let Some(conf) = message
            .snapshot
            .as_mut()
            .and_then(|snapshot| snapshot.metadata.as_mut())
            .and_then(|metadata| metadata.conf_state.as_mut())
        {
            *conf = sorted(conf.clone());
        }
    }
    messages
}
fn change_of(entry: &Entry) -> Option<Result<ConfChangeV2, ConfChange>> {
    match entry.entry_type {
        EntryType::EntryNormal => None,
        EntryType::EntryConfChange if entry.data.is_empty() => Some(Err(ConfChange::default())),
        EntryType::EntryConfChange => ConfChange::decode(&entry.data).ok().map(Err),
        EntryType::EntryConfChangeV2 if entry.data.is_empty() => Some(Ok(ConfChangeV2::default())),
        EntryType::EntryConfChangeV2 => ConfChangeV2::decode(&entry.data).ok().map(Ok),
    }
}
fn role(state: u8) -> u8 {
    state
}

// ---------------------------------------------------------------------
// raft-rs, as focal's shell drives it.

pub struct Old {
    pub raw: raft::RawNode<Store>,
    priority: i64,
    app: App,
    /// What the changes raft-rs counts as uncommitted take in this core's format beyond what
    /// they take in raft-rs's, kept by raft-rs's own rule (`Old::note_lead`).
    change_bytes: isize,
    /// The term this member last led, and its log's last index as it took the lead: raft-rs
    /// counts what it appends past that index and forgets the count when it next leads.
    led: (u64, u64),
}
impl Old {
    fn settle(&mut self) {
        // As this core settles it: no term, or no voter, and the priority
        // judges nothing.
        let effective = if self.raw.raft.term == 0 || !self.raw.raft.promotable() {
            0
        } else {
            self.priority
        };
        if self.raw.raft.priority != effective {
            self.raw.set_priority(effective);
        }
    }
    fn operate<T>(&mut self, operation: impl FnOnce(&mut raft::RawNode<Store>) -> T) -> T {
        self.settle();
        let outcome = operation(&mut self.raw);
        self.settle();
        outcome
    }
    fn apply(&mut self, entries: Vec<Entry>, output: &mut Output) {
        for entry in entries {
            // raft-rs takes a committed entry past the tail off its count while it leads.
            if self.raw.raft.state == raft::StateRole::Leader && entry.index > self.led.1 {
                self.change_bytes -= extra(&entry);
            }
            output.committed.push(said(&entry));
            self.app.apply(&entry);
            let Some(change) = change_of(&entry) else {
                continue;
            };
            let led = self.raw.raft.state == raft::StateRole::Leader;
            let applied = match change {
                Ok(change) => self.raw.apply_conf_change(&convert::change_to(&change)),
                Err(change) => self.raw.apply_conf_change(&convert::single_to(&change)),
            };
            match applied {
                Ok(conf) => {
                    let conf = sorted(convert::conf_from(conf));
                    if led && !votes(&conf, self.raw.raft.id) {
                        output.leader_left = true;
                    }
                    self.raw.mut_store().0.conf = conf.clone();
                    output.confs.push(conf);
                }
                Err(_) => output.refused.push(entry.index),
            }
        }
    }
}
impl Replica for Old {
    fn open(id: u64, mut store: Store, settings: &Settings, _seed: u64) -> Self {
        store.0.reopen();
        let applied = store.0.snapshot_index();
        let app = if applied == 0 {
            App::default()
        } else {
            App::decode(&store.0.snapshot.data)
        };
        let config = raft::Config {
            id,
            election_tick: settings.election_tick,
            heartbeat_tick: settings.heartbeat_tick,
            applied,
            max_size_per_msg: settings.max_size_per_msg,
            max_inflight_msgs: settings.max_inflight_msgs,
            max_uncommitted_size: settings.max_uncommitted_size,
            max_committed_size_per_ready: settings.max_committed_size_per_ready,
            check_quorum: settings.check_quorum,
            pre_vote: settings.pre_vote,
            ..Default::default()
        };
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let raw = raft::RawNode::new(&config, store, &logger).expect("raft-rs opens");
        Self {
            raw,
            priority: 0,
            app,
            change_bytes: 0,
            led: (0, 0),
        }
    }
    fn id(&self) -> u64 {
        self.raw.raft.id
    }
    fn store(&self) -> &Store {
        self.raw.store()
    }
    fn store_mut(&mut self) -> &mut Store {
        self.raw.mut_store()
    }
    fn tick(&mut self) -> bool {
        self.operate(|raw| raw.tick())
    }
    fn step(&mut self, message: Message) -> bool {
        self.operate(|raw| {
            // Priority never judges the vote a transfer asks for.
            if message.msg_type == MessageType::MsgRequestVote
                && message.context.as_slice() == CAMPAIGN_TRANSFER
                && raw.raft.priority != 0
            {
                raw.set_priority(0);
            }
            raw.step(convert::message_to(&message)).is_ok()
        })
    }
    fn propose(&mut self, data: Vec<u8>) -> bool {
        self.operate(|raw| raw.propose(Vec::new(), data).is_ok())
    }
    fn propose_change(&mut self, change: &ConfChangeV2) -> bool {
        self.operate(|raw| {
            raw.propose_conf_change(Vec::new(), convert::change_to(change))
                .is_ok()
        })
    }
    fn campaign(&mut self) -> bool {
        // As the shell asks: `raft-rs` lets one that is no voter campaign,
        // and unwinds when it wins.
        if !self.raw.raft.promotable() {
            return false;
        }
        self.operate(|raw| raw.campaign().is_ok())
    }
    fn ping(&mut self) {
        self.operate(|raw| raw.ping());
    }
    fn transfer(&mut self, to: u64) {
        self.operate(|raw| raw.transfer_leader(to));
    }
    fn read(&mut self, context: Vec<u8>) {
        self.operate(|raw| raw.read_index(context));
    }
    fn unreachable(&mut self, member: u64) {
        self.operate(|raw| raw.report_unreachable(member));
    }
    fn snapshot_status(&mut self, member: u64, arrived: bool) {
        let status = if arrived {
            raft::SnapshotStatus::Finish
        } else {
            raft::SnapshotStatus::Failure
        };
        self.operate(|raw| raw.report_snapshot(member, status));
    }
    fn set_priority(&mut self, priority: i64) {
        self.priority = priority;
        self.settle();
    }
    fn set_timeout(&mut self, ticks: usize) {
        self.raw.raft.set_randomized_election_timeout(ticks);
    }
    fn drain(&mut self) -> Output {
        let mut output = Output::default();
        let mut messages = Vec::new();
        while self.raw.has_ready() {
            let mut ready = self.raw.ready();
            if !ready.snapshot().is_empty() {
                let snapshot = convert::snapshot_from(ready.snapshot());
                let metadata = snapshot.metadata.clone().unwrap_or_default();
                output.snapshots.push((metadata.index, metadata.term));
                self.app = App::decode(&snapshot.data);
                self.raw.mut_store().0.install(&snapshot);
            }
            let entries: Vec<Entry> = ready.entries().iter().map(convert::entry_from).collect();
            self.note_lead();
            if self.raw.raft.state == raft::StateRole::Leader {
                // What a leader persists in its term past the tail it appended itself, and
                // counted as it appended it.
                for entry in &entries {
                    if entry.term == self.led.0 && entry.index > self.led.1 {
                        self.change_bytes += extra(entry);
                    }
                }
            }
            output.persisted.extend(entries.iter().map(said));
            self.raw.mut_store().0.append(&entries);
            if let Some(hard) = ready.hs() {
                output.hard_states.push((hard.term, hard.vote, hard.commit));
                self.raw.mut_store().0.hard_state = convert::hard_from(hard);
            }
            messages.extend(ready.take_messages().into_iter().map(convert::message_from));
            messages.extend(
                ready
                    .take_persisted_messages()
                    .into_iter()
                    .map(convert::message_from),
            );
            output.reads.extend(
                ready
                    .take_read_states()
                    .into_iter()
                    .map(|read| (read.index, read.request_ctx)),
            );
            let committed = ready.take_committed_entries();
            self.apply(
                committed.iter().map(convert::entry_from).collect(),
                &mut output,
            );
            let mut light = self.raw.advance_append(ready);
            if let Some(commit) = light.commit_index() {
                let disk = &mut self.raw.mut_store().0;
                disk.hard_state.commit = commit;
                output
                    .hard_states
                    .push((disk.hard_state.term, disk.hard_state.vote, commit));
            }
            messages.extend(light.take_messages().into_iter().map(convert::message_from));
            let committed = light.take_committed_entries();
            self.apply(
                committed.iter().map(convert::entry_from).collect(),
                &mut output,
            );
            self.raw.advance_apply_to(self.app.index);
            self.settle();
        }
        output.messages = canonical(messages);
        output
    }
    fn view(&self) -> View {
        let raft = &self.raw.raft;
        let mut members: Vec<Member> = raft
            .prs()
            .iter()
            .map(|(member, progress)| {
                (
                    *member,
                    progress.matched,
                    progress.next_idx,
                    match progress.state {
                        raft::ProgressState::Probe => 0,
                        raft::ProgressState::Replicate => 1,
                        raft::ProgressState::Snapshot => 2,
                    },
                    progress.paused,
                    progress.pending_snapshot,
                    progress.pending_request_snapshot,
                    progress.recent_active,
                    progress.ins.count(),
                    progress.committed_index,
                )
            })
            .collect();
        members.sort_unstable();
        View {
            term: raft.term,
            vote: raft.vote,
            commit: raft.raft_log.committed,
            leader: raft.leader_id,
            role: role(match raft.state {
                raft::StateRole::Follower => 0,
                raft::StateRole::Candidate => 1,
                raft::StateRole::Leader => 2,
                raft::StateRole::PreCandidate => 3,
            }),
            last_index: raft.raft_log.last_index(),
            persisted: raft.raft_log.persisted,
            applied: raft.raft_log.applied,
            timeout: raft.randomized_election_timeout(),
            elapsed: raft.election_elapsed,
            pending_conf: raft.pending_conf_index,
            transferee: raft.lead_transferee,
            promotable: raft.promotable(),
            uncommitted: self.uncommitted(),
            pending_reads: raft.pending_read_count(),
            app: self.app,
            members,
        }
    }
    fn app(&self) -> App {
        self.app
    }
}

/// What a change entry's data takes in this core's format beyond raft-rs's.
fn extra(entry: &Entry) -> isize {
    if entry.entry_type == EntryType::EntryNormal {
        return 0;
    }
    entry.data.len() as isize - convert::entry_to(entry).data.len() as isize
}

impl Old {
    /// raft-rs's uncommitted bytes as this core counts them: each change raft-rs counted in its
    /// encoding is counted in this core's (`docs/raft.md` §3.1).
    fn uncommitted(&self) -> usize {
        // A count of nothing holds no change: raft-rs's floor at zero forgets them too.
        match self.raw.raft.uncommitted_size() {
            0 => 0,
            counted => (counted as isize + self.change_bytes) as usize,
        }
    }
    /// raft-rs's rule: a member that takes the lead forgets its count, and counts what it
    /// appends past its log's last index as it took the lead.
    fn note_lead(&mut self) {
        let raft = &self.raw.raft;
        if raft.state != raft::StateRole::Leader || raft.term == self.led.0 {
            return;
        }
        let mut tail = raft.raft_log.last_index();
        while tail > raft.raft_log.committed && raft.raft_log.term(tail).ok() == Some(raft.term) {
            tail -= 1;
        }
        self.led = (raft.term, tail);
        self.change_bytes = 0;
    }
}

// ---------------------------------------------------------------------
// hyper-raft.

pub struct New {
    pub raw: hyper_raft::RawNode<Store>,
    app: App,
    in_place: bool,
}
impl New {
    fn apply(&mut self, entries: Vec<Entry>, output: &mut Output) {
        for entry in entries {
            output.committed.push(said(&entry));
            self.app.apply(&entry);
            let Some(change) = change_of(&entry) else {
                continue;
            };
            let led = self.raw.raft.state() == hyper_raft::StateRole::Leader;
            let applied = match change {
                Ok(change) => self.raw.apply_conf_change(&change),
                Err(change) => self.raw.apply_conf_change_v1(&change),
            };
            match applied {
                Ok(conf) => {
                    if led && !votes(&conf, self.raw.raft.id()) {
                        output.leader_left = true;
                    }
                    self.raw.store_mut().0.conf = conf.clone();
                    output.confs.push(conf);
                }
                Err(error) => {
                    assert!(!error.is_fatal(), "applying a change: {error}");
                    output.refused.push(entry.index);
                }
            }
        }
    }
}
fn heard<T>(outcome: hyper_raft::Result<T>) -> Option<T> {
    match outcome {
        Ok(value) => Some(value),
        Err(error) => {
            assert!(!error.is_fatal(), "the member stopped: {error}");
            None
        }
    }
}
impl New {
    /// What a `Ready` gives to apply: its copies, or, given in place, the
    /// range read from storage.
    fn committed(&self, copies: Vec<Entry>, range: Option<(u64, u64)>) -> Vec<Entry> {
        let Some((first, last)) = range else {
            return copies;
        };
        assert!(
            copies.is_empty(),
            "a ready in place copied what it gives to apply"
        );
        assert!(
            self.in_place,
            "a range given to apply by a ready that copies"
        );
        let disk = &self.raw.store().0;
        (first..=last)
            .map(|index| disk.entries[(index - disk.first_index()) as usize].clone())
            .collect()
    }
    pub fn fast_stats(&self) -> hyper_raft::FastStats {
        self.raw.raft.fast_stats()
    }
    /// What the member holds approved by itself.
    pub fn held(&self) -> Vec<(u64, Vec<u8>)> {
        self.raw
            .raft
            .proposals()
            .map(|held| (held.index, held.data.clone()))
            .collect()
    }
}
impl Replica for New {
    fn open(id: u64, mut store: Store, settings: &Settings, seed: u64) -> Self {
        store.0.reopen();
        let applied = store.0.snapshot_index();
        let app = if applied == 0 {
            App::default()
        } else {
            App::decode(&store.0.snapshot.data)
        };
        let config = hyper_raft::Config {
            election_tick: settings.election_tick,
            heartbeat_tick: settings.heartbeat_tick,
            applied,
            max_size_per_msg: settings.max_size_per_msg,
            max_inflight_msgs: settings.max_inflight_msgs,
            max_inflight_bytes: settings.max_inflight_bytes,
            max_uncommitted_size: settings.max_uncommitted_size,
            max_committed_size_per_ready: settings.max_committed_size_per_ready,
            check_quorum: settings.check_quorum,
            pre_vote: settings.pre_vote,
            precedence: if settings.by_length {
                hyper_raft::Precedence::Length
            } else {
                hyper_raft::Precedence::Log
            },
            read_rounds: if settings.round_each {
                hyper_raft::ReadRounds::Each
            } else {
                hyper_raft::ReadRounds::Shared
            },
            heartbeat_answers: if settings.bare_answers {
                hyper_raft::HeartbeatAnswers::Bare
            } else {
                hyper_raft::HeartbeatAnswers::Position
            },
            fast: settings.fast,
            seed,
            ..hyper_raft::Config::new(id)
        };
        let raw = hyper_raft::RawNode::new(&config, store).expect("hyper-raft opens");
        Self {
            raw,
            app,
            in_place: settings.in_place,
        }
    }
    fn id(&self) -> u64 {
        self.raw.raft.id()
    }
    fn store(&self) -> &Store {
        self.raw.store()
    }
    fn store_mut(&mut self) -> &mut Store {
        self.raw.store_mut()
    }
    fn tick(&mut self) -> bool {
        heard(self.raw.tick()).unwrap_or(false)
    }
    fn step(&mut self, message: Message) -> bool {
        heard(self.raw.step(message)).is_some()
    }
    fn propose(&mut self, data: Vec<u8>) -> bool {
        heard(self.raw.propose(Vec::new(), data)).is_some()
    }
    fn propose_fast(&mut self, data: Vec<u8>) -> Option<u64> {
        heard(self.raw.propose_fast(Vec::new(), data))
    }
    fn propose_change(&mut self, change: &ConfChangeV2) -> bool {
        heard(self.raw.propose_conf_change(Vec::new(), change)).is_some()
    }
    fn campaign(&mut self) -> bool {
        heard(self.raw.campaign()).is_some()
    }
    fn ping(&mut self) {
        heard(self.raw.ping());
    }
    fn transfer(&mut self, to: u64) {
        heard(self.raw.transfer_leader(to));
    }
    fn read(&mut self, context: Vec<u8>) {
        heard(self.raw.read_index(context));
    }
    fn unreachable(&mut self, member: u64) {
        heard(self.raw.report_unreachable(member));
    }
    fn snapshot_status(&mut self, member: u64, arrived: bool) {
        let status = if arrived {
            hyper_raft::SnapshotStatus::Finish
        } else {
            hyper_raft::SnapshotStatus::Failure
        };
        heard(self.raw.report_snapshot(member, status));
    }
    fn set_priority(&mut self, priority: i64) {
        self.raw.set_priority(priority);
    }
    fn set_window(&mut self, member: u64, bytes: u64) {
        self.raw.set_inflight_bytes(member, bytes);
    }
    fn set_timeout(&mut self, ticks: usize) {
        self.raw
            .raft
            .set_randomized_election_timeout(ticks)
            .expect("a timeout within one to two");
    }
    fn drain(&mut self) -> Output {
        let mut output = Output::default();
        let mut messages = Vec::new();
        while self.raw.has_ready() {
            let mut ready = if self.in_place {
                self.raw.ready_in_place().expect("a ready")
            } else {
                self.raw.ready().expect("a ready")
            };
            if let Some(snapshot) = ready.snapshot() {
                let metadata = snapshot.metadata.clone().unwrap_or_default();
                output.snapshots.push((metadata.index, metadata.term));
                self.app = App::decode(&snapshot.data);
                self.raw.store_mut().0.install(snapshot);
            }
            if self.in_place {
                // Read where the member holds them, and persisted before
                // anything else is asked of it; the snapshot and the entries
                // themselves are kept when the ready advances.
                assert!(
                    ready.entries().is_empty() && ready.snapshot().is_none(),
                    "a ready in place copied what it persists"
                );
                let persist = self.raw.to_persist();
                if let Some(snapshot) = persist.snapshot {
                    let metadata = snapshot.metadata.clone().unwrap_or_default();
                    output.snapshots.push((metadata.index, metadata.term));
                    self.app = App::decode(&snapshot.data);
                }
                output.persisted.extend(persist.entries.iter().map(said));
                let disk = &mut persist.store.0;
                disk.proposals.extend(ready.proposals().iter().cloned());
                disk.trim_proposals();
            } else {
                output.persisted.extend(ready.entries().iter().map(said));
                let disk = &mut self.raw.store_mut().0;
                disk.proposals.extend(ready.proposals().iter().cloned());
                disk.append(ready.entries());
            }
            output.displaced.extend(ready.displaced().iter().map(said));
            if let Some(hard) = ready.hard_state() {
                output.hard_states.push((hard.term, hard.vote, hard.commit));
                self.raw.store_mut().0.hard_state = *hard;
            }
            for message in ready.messages().iter().chain(ready.persisted_messages()) {
                // A page is sized before it is copied: it holds no spare
                // room for the entries behind it.
                assert_eq!(
                    message.entries.capacity(),
                    message.entries.len(),
                    "member {}: a page with spare room",
                    self.raw.raft.id()
                );
            }
            messages.extend(ready.take_messages());
            messages.extend(ready.take_persisted_messages());
            output.reads.extend(
                ready
                    .take_read_states()
                    .into_iter()
                    .map(|read| (read.index, read.request_ctx)),
            );
            let committed = self.committed(ready.take_committed_entries(), ready.committed_range());
            self.apply(committed, &mut output);
            let mut light = if self.in_place {
                self.raw
                    .advance_append_keeping(ready, |store, kept| {
                        if let Some(snapshot) = kept.snapshot {
                            store.0.install_owned(snapshot);
                        }
                        store.0.keep(kept.entries);
                    })
                    .expect("advanced")
            } else {
                self.raw.advance_append(ready).expect("advanced")
            };
            if let Some(commit) = light.commit_index() {
                let disk = &mut self.raw.store_mut().0;
                disk.hard_state.commit = commit;
                output
                    .hard_states
                    .push((disk.hard_state.term, disk.hard_state.vote, commit));
            }
            messages.extend(light.take_messages());
            let committed = self.committed(light.take_committed_entries(), light.committed_range());
            self.apply(committed, &mut output);
            self.raw.advance_apply_to(self.app.index).expect("applied");
        }
        output.messages = canonical(messages);
        output
    }
    fn view(&self) -> View {
        // What the member counts is what it holds, after every operation.
        self.raw
            .check_accounting()
            .expect("the member's accounting adds up");
        let raft = &self.raw.raft;
        let members = raft
            .tracker()
            .iter()
            .map(|(member, progress)| {
                (
                    member,
                    progress.matched,
                    progress.next_index,
                    match progress.state {
                        hyper_raft::progress::ProgressState::Probe => 0,
                        hyper_raft::progress::ProgressState::Replicate => 1,
                        hyper_raft::progress::ProgressState::Snapshot => 2,
                    },
                    progress.paused,
                    progress.pending_snapshot,
                    progress.pending_request_snapshot,
                    progress.recent_active,
                    progress.inflights.count(),
                    progress.committed_index,
                )
            })
            .collect();
        View {
            term: raft.term(),
            vote: raft.vote(),
            commit: raft.log().committed(),
            leader: raft.leader_id(),
            role: role(match raft.state() {
                hyper_raft::StateRole::Follower => 0,
                hyper_raft::StateRole::Candidate => 1,
                hyper_raft::StateRole::Leader => 2,
                hyper_raft::StateRole::PreCandidate => 3,
            }),
            last_index: raft.log().last_index().expect("a last index"),
            persisted: raft.log().persisted(),
            applied: raft.log().applied(),
            timeout: raft.randomized_election_timeout(),
            elapsed: raft.election_elapsed(),
            pending_conf: raft.pending_conf_index(),
            transferee: raft.lead_transferee(),
            promotable: raft.promotable(),
            uncommitted: raft.uncommitted_bytes(),
            pending_reads: raft.pending_read_count(),
            app: self.app,
            members,
        }
    }
    fn app(&self) -> App {
        self.app
    }
}

/// A group of both cores: the odd members run `raft-rs` and the even ones
/// this crate, as a group does while its nodes are replaced one by one.
pub enum Either {
    Old(Box<Old>),
    New(Box<New>),
}
macro_rules! either {
    ($self:ident, $node:ident => $call:expr) => {
        match $self {
            Either::Old($node) => $call,
            Either::New($node) => $call,
        }
    };
}
impl Replica for Either {
    fn open(id: u64, store: Store, settings: &Settings, seed: u64) -> Self {
        if id % 2 == 1 {
            let mut old = Old::open(id, store, settings, seed);
            // `raft-rs` draws its timeouts from the thread; a run is its
            // seed.
            let span = settings.election_tick as u64;
            old.set_timeout(settings.election_tick + Seeded(seed).below(span) as usize);
            Self::Old(Box::new(old))
        } else {
            Self::New(Box::new(New::open(id, store, settings, seed)))
        }
    }
    fn id(&self) -> u64 {
        either!(self, node => node.id())
    }
    fn store(&self) -> &Store {
        either!(self, node => node.store())
    }
    fn store_mut(&mut self) -> &mut Store {
        either!(self, node => node.store_mut())
    }
    fn tick(&mut self) -> bool {
        either!(self, node => node.tick())
    }
    fn step(&mut self, message: Message) -> bool {
        either!(self, node => node.step(message))
    }
    fn propose(&mut self, data: Vec<u8>) -> bool {
        either!(self, node => node.propose(data))
    }
    fn propose_fast(&mut self, data: Vec<u8>) -> Option<u64> {
        either!(self, node => node.propose_fast(data))
    }
    fn propose_change(&mut self, change: &ConfChangeV2) -> bool {
        either!(self, node => node.propose_change(change))
    }
    fn campaign(&mut self) -> bool {
        either!(self, node => node.campaign())
    }
    fn ping(&mut self) {
        either!(self, node => node.ping())
    }
    fn transfer(&mut self, to: u64) {
        either!(self, node => node.transfer(to))
    }
    fn read(&mut self, context: Vec<u8>) {
        either!(self, node => node.read(context))
    }
    fn unreachable(&mut self, member: u64) {
        either!(self, node => node.unreachable(member))
    }
    fn snapshot_status(&mut self, member: u64, arrived: bool) {
        either!(self, node => node.snapshot_status(member, arrived))
    }
    fn set_priority(&mut self, priority: i64) {
        either!(self, node => node.set_priority(priority))
    }
    fn set_window(&mut self, member: u64, bytes: u64) {
        either!(self, node => node.set_window(member, bytes))
    }
    fn set_timeout(&mut self, ticks: usize) {
        either!(self, node => node.set_timeout(ticks))
    }
    fn drain(&mut self) -> Output {
        either!(self, node => node.drain())
    }
    fn view(&self) -> View {
        either!(self, node => node.view())
    }
    fn app(&self) -> App {
        either!(self, node => node.app())
    }
}

pub mod cluster;
#[allow(unused_imports)]
pub use cluster::{Cluster, Mix, Op, Report};
