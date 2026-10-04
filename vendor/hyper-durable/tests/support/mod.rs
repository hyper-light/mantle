//! What the shell's tests share: a store whose writes become durable when the schedule says
//! ([`SimStore`]), a state machine with a durable point that a crash returns to ([`Kv`]), and a
//! group of replicas over them driven step by step ([`Cluster`]), held to the invariants of
//! `docs/durable.md` §3 against each member's durable state at every step.
#![allow(
    dead_code,
    unreachable_pub,
    unused_imports,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity,
    clippy::panic_in_result_fn,
    clippy::unwrap_in_result
)]

pub mod cluster;
pub mod device;

use std::collections::VecDeque;
use std::task::Waker;

use hyper_durable::{
    EntryRef, Fatal, Fault, Health, LogStore, Point, StateMachine, StoreView, Write,
};
use hyper_raft::StorageError;
use hyper_raft::proto::{ConfChangeV2, ConfState, Entry, HardState};

/// A seeded generator: SplitMix64, so a schedule replays exactly from its seed.
#[derive(Clone, Debug)]
pub struct Seeded(pub u64);

impl Seeded {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
    pub fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// A group's log as a crash leaves it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Disk {
    pub start: Point,
    pub entries: Vec<Entry>,
    pub hard: HardState,
    pub proposals: Vec<Entry>,
}

impl Disk {
    pub fn last(&self) -> u64 {
        self.entries.last().map_or(self.start.index, |e| e.index)
    }
    pub fn term(&self, index: u64) -> Option<u64> {
        if index == self.start.index {
            return Some(self.start.term);
        }
        let at = index.checked_sub(self.start.index + 1)? as usize;
        self.entries.get(at).map(|e| e.term)
    }
    pub fn entry(&self, index: u64) -> Option<&Entry> {
        let at = index.checked_sub(self.start.index + 1)? as usize;
        self.entries.get(at)
    }
    /// Whether the log holds the entry `(index, term)`, or a start at or past it.
    pub fn holds(&self, index: u64, term: u64) -> bool {
        index <= self.start.index || self.term(index) == Some(term)
    }
    fn apply(&mut self, write: &Owned) -> Result<(), &'static str> {
        if let Some(start) = write.start {
            if start.index < self.start.index {
                return Err("the start moves back");
            }
            let drop = (start.index - self.start.index).min(self.entries.len() as u64) as usize;
            self.entries.drain(..drop);
            self.start = start;
            if self.last() < start.index {
                self.entries.clear();
            }
        }
        if let Some((first, entries)) = &write.entries {
            if *first <= self.start.index || *first > self.last() + 1 {
                return Err("entries that leave a gap or precede the start");
            }
            self.entries
                .truncate((*first - self.start.index - 1) as usize);
            self.entries.extend(entries.iter().cloned());
        }
        if let Some(hard) = write.hard {
            self.hard = hard;
        }
        let last = self.last();
        self.proposals.retain(|p| p.index > last);
        self.proposals
            .extend(write.proposals.iter().filter(|p| p.index > last).cloned());
        Ok(())
    }
}

/// A write as the store keeps it until it is durable.
#[derive(Clone, Debug, Default)]
struct Owned {
    start: Option<Point>,
    entries: Option<(u64, Vec<Entry>)>,
    hard: Option<HardState>,
    proposals: Vec<Entry>,
}

impl Owned {
    fn of(write: &Write<'_>) -> Self {
        Self {
            start: write.start,
            entries: write.entries.map(|e| (e.first, e.entries.to_vec())),
            hard: write.hard_state,
            proposals: write.proposals.to_vec(),
        }
    }
}

/// What a store did, counted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Events {
    pub submits: u64,
    pub durables: u64,
    pub answers: u64,
    pub refused: u64,
}

/// A store whose writes are durable only when the schedule says ([`SimStore::make_durable`]),
/// in the order submitted, and whose answers the replica takes later still. It reads as its
/// answered writes left it, as hyper-log's handle does. A refusal is whole and refuses every
/// write behind it, as hyper-log's `Behind` rule does.
#[derive(Debug)]
pub struct SimStore {
    /// What a crash keeps.
    pub disk: Disk,
    /// What the answered writes left: what the replica reads.
    answered: Disk,
    pending: VecDeque<(u64, Owned)>,
    done: VecDeque<(Owned, Result<(), Fault>)>,
    depth: usize,
    /// The next write made durable is refused or fails with this.
    pub refuse: Option<Fault>,
    pub events: Events,
    /// A mark the store reports until the log reaches past it.
    pub mark: Option<Point>,
    /// Refusals the replica has taken: each write is submitted in the epoch they make, and one
    /// submitted in an epoch at or before a refused write's is refused behind it, as hyper-log
    /// refuses a handle's writes sent before it took a refusal (`LogError::Behind`).
    epoch: u64,
    refused_epoch: Option<u64>,
    /// The log holds no room for another write until the owner frees some.
    pub full: bool,
}

impl SimStore {
    pub fn new(depth: usize) -> Self {
        Self::from_disk(Disk::default(), depth)
    }
    pub fn from_disk(disk: Disk, depth: usize) -> Self {
        Self {
            answered: disk.clone(),
            disk,
            pending: VecDeque::new(),
            done: VecDeque::new(),
            depth,
            refuse: None,
            events: Events::default(),
            mark: None,
            epoch: 0,
            refused_epoch: None,
            full: false,
        }
    }
    /// Writes submitted and not yet durable.
    pub fn pending(&self) -> usize {
        self.pending.len()
    }
    /// Writes durable whose answers were not taken.
    pub fn unanswered(&self) -> usize {
        self.done.len()
    }
    /// What the answered writes left: what the replica reads.
    pub fn answered(&self) -> &Disk {
        &self.answered
    }
    /// Whether a write that moves the log's start is out: submitted, or durable and unanswered.
    pub fn start_out(&self) -> bool {
        let pending = self.pending.iter().map(|(_, write)| write);
        let done = self.done.iter().map(|(write, _)| write);
        pending.chain(done).any(|write| write.start.is_some())
    }
    /// Makes the oldest pending write durable, or refuses it and every one behind it; false
    /// when none is pending.
    pub fn make_durable(&mut self) -> bool {
        let Some((epoch, write)) = self.pending.pop_front() else {
            return false;
        };
        match self.refused_epoch {
            Some(refused) if epoch <= refused => {
                self.done.push_back((write, Err(Fault::Behind)));
                return true;
            }
            Some(_) => self.refused_epoch = None,
            None => {}
        }
        match self.refuse.take() {
            Some(fault) => {
                self.events.refused += 1;
                let behind = fault.changed_nothing();
                if behind {
                    self.refused_epoch = Some(epoch);
                }
                self.done.push_back((write, Err(fault)));
                while let Some((_, later)) = self.pending.pop_front() {
                    let answer = if behind {
                        Err(Fault::Behind)
                    } else {
                        Err(Fault::Failed("the log is fenced"))
                    };
                    self.done.push_back((later, answer));
                }
            }
            None => {
                self.disk
                    .apply(&write)
                    .expect("a write the store cannot take");
                self.events.durables += 1;
                self.done.push_back((write, Ok(())));
            }
        }
        true
    }
}

impl LogStore for SimStore {
    type Hold = std::convert::Infallible;

    fn held(&self) -> Option<&Self::Hold> {
        None
    }

    fn release(&mut self, met: &Self::Hold) {
        match *met {}
    }

    fn depth(&self) -> usize {
        self.depth
    }
    fn view(&self) -> Result<StoreView, Fault> {
        let d = &self.answered;
        let health = match self.mark {
            Some(mark)
                if !(d.last() >= mark.index || d.term(d.last()).is_some_and(|t| t > mark.term)) =>
            {
                Health::Marked(mark)
            }
            _ => Health::Whole,
        };
        Ok(StoreView {
            start: d.start,
            last: d.last(),
            hard_state: d.hard,
            health,
        })
    }
    fn bounds(&self) -> Result<(Point, u64), StorageError> {
        Ok((self.answered.start, self.answered.last()))
    }
    fn term(&self, index: u64) -> Result<u64, StorageError> {
        if index < self.answered.start.index {
            return Err(StorageError::Compacted);
        }
        self.answered.term(index).ok_or(StorageError::Unavailable)
    }
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: &mut Vec<Entry>,
    ) -> Result<(), StorageError> {
        let mut total = 0u64;
        self.visit(low, high, u64::MAX, &mut |e| {
            total += e.encoded_bytes();
            if total > max_bytes && e.index > low {
                return true;
            }
            let mut copy = Entry::default();
            e.copy_into(&mut copy);
            into.push(copy);
            false
        })
    }
    fn visit(
        &self,
        low: u64,
        high: u64,
        _page: u64,
        visit: &mut dyn FnMut(EntryRef<'_>) -> bool,
    ) -> Result<(), StorageError> {
        let d = &self.answered;
        if low <= d.start.index {
            return Err(StorageError::Compacted);
        }
        if high > d.last() + 1 {
            return Err(StorageError::Unavailable);
        }
        for index in low..high {
            if visit(EntryRef::of(
                d.entry(index).ok_or(StorageError::Unavailable)?,
            )) {
                break;
            }
        }
        Ok(())
    }
    fn proposals(&self, into: &mut Vec<Entry>) -> Result<(), StorageError> {
        into.extend(self.answered.proposals.iter().cloned());
        Ok(())
    }
    fn room(&self) -> bool {
        !self.full && self.pending.len() + self.done.len() < self.depth + 1
    }
    fn submit(&mut self, write: &Write<'_>, _waker: &Waker) -> Result<(), Fault> {
        assert!(self.room(), "a write submitted past the store's room");
        self.events.submits += 1;
        self.pending.push_back((self.epoch, Owned::of(write)));
        Ok(())
    }
    fn poll(&mut self) -> Option<Result<(), Fault>> {
        let (write, answer) = self.done.pop_front()?;
        if answer.as_ref().is_err_and(Fault::changed_nothing) {
            self.epoch += 1;
        }
        if answer.is_ok() {
            self.answered.apply(&write).expect("an answered write");
        }
        self.events.answers += 1;
        Some(answer)
    }
    fn write_now(&mut self, write: &Write<'_>) -> Result<(), Fault> {
        let owned = Owned::of(write);
        self.disk.apply(&owned).map_err(Fault::Failed)?;
        self.answered.apply(&owned).map_err(Fault::Failed)?;
        Ok(())
    }
}

/// A state machine's state at a point.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KvState {
    pub applied: Point,
    /// Every entry applied, in order: index, term, data.
    pub entries: Vec<(u64, u64, Vec<u8>)>,
    pub configuration: ConfState,
}

/// A state machine that keeps every entry it applies, durable when persisted (or, when
/// `volatile`, never but by a snapshot's install, focal's case: it replays its log).
#[derive(Clone, Debug)]
pub struct Kv {
    pub now: KvState,
    pub durable: KvState,
    pub volatile: bool,
    /// Every entry of the group is acted on at start (focal's control groups).
    pub control: bool,
    /// Images what it persisted, not everything applied: an owner's checkpoint, focal's case.
    pub checkpoints: bool,
    /// Keeps only the last entry it applied: a state of one value, whose image does not grow
    /// with the log, as a register's group's does not.
    pub register: bool,
    /// Entries applied that a member acts on at its next start, and changes, since the harness
    /// last looked: what I5 holds against the durable commit.
    pub fenced_applied: Vec<u64>,
    /// Every entry acted on at start, ever, by index.
    pub acted: Vec<u64>,
    /// Each change applied since the member opened, by index, with the context its entry stated.
    pub changes: Vec<(u64, Vec<u8>)>,
}

/// What a fenced entry's data begins with: a member acts on it at its next start.
pub const ACTS: &[u8] = b"fence:";

impl Kv {
    pub fn new(configuration: ConfState, volatile: bool) -> Self {
        let state = KvState {
            configuration,
            ..KvState::default()
        };
        Self {
            now: state.clone(),
            durable: state,
            volatile,
            control: false,
            checkpoints: false,
            register: false,
            fenced_applied: Vec::new(),
            acted: Vec::new(),
            changes: Vec::new(),
        }
    }
    /// What a crash leaves.
    pub fn crashed(&self) -> Self {
        Self {
            now: self.durable.clone(),
            durable: self.durable.clone(),
            volatile: self.volatile,
            control: self.control,
            checkpoints: self.checkpoints,
            register: self.register,
            fenced_applied: Vec::new(),
            acted: Vec::new(),
            changes: Vec::new(),
        }
    }
}

fn put(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn take(bytes: &mut &[u8]) -> Option<u64> {
    let (head, rest) = bytes.split_at_checked(8)?;
    *bytes = rest;
    Some(u64::from_le_bytes(head.try_into().ok()?))
}
fn put_ids(out: &mut Vec<u8>, ids: &[u64]) {
    put(out, ids.len() as u64);
    for &id in ids {
        put(out, id);
    }
}
fn take_ids(bytes: &mut &[u8]) -> Option<Vec<u64>> {
    let n = take(bytes)?;
    (0..n).map(|_| take(bytes)).collect()
}

impl StateMachine for Kv {
    type Answer = (u64, Vec<u8>);
    fn apply(
        &mut self,
        entry: &EntryRef<'_>,
        answers: &mut Vec<Self::Answer>,
    ) -> Result<(), Fatal> {
        if entry.index != self.now.applied.index + 1 {
            return Err(Fatal("applied out of order"));
        }
        if self.acts_at_start(entry) {
            self.fenced_applied.push(entry.index);
            self.acted.push(entry.index);
        }
        if self.register {
            self.now.entries.clear();
        }
        self.now
            .entries
            .push((entry.index, entry.term, entry.data.to_vec()));
        self.now.applied = Point {
            index: entry.index,
            term: entry.term,
        };
        answers.push((entry.index, entry.data.to_vec()));
        Ok(())
    }
    fn apply_change(
        &mut self,
        at: Point,
        change: &ConfChangeV2,
        configuration: &ConfState,
    ) -> Result<(), Fatal> {
        if at.index != self.now.applied.index + 1 {
            return Err(Fatal("a change applied out of order"));
        }
        self.changes.push((at.index, change.context.clone()));
        self.fenced_applied.push(at.index);
        self.now.configuration = configuration.clone();
        self.now.applied = at;
        Ok(())
    }
    fn durable(&self) -> Point {
        self.durable.applied
    }
    fn configuration(&self) -> &ConfState {
        &self.now.configuration
    }
    fn acts_at_start(&self, entry: &EntryRef<'_>) -> bool {
        self.control || entry.data.starts_with(ACTS)
    }
    fn image(&mut self, into: &mut Vec<u8>) -> Result<(Point, ConfState), Fatal> {
        let state = if self.checkpoints {
            &self.durable
        } else {
            &self.now
        };
        put(into, state.entries.len() as u64);
        for (index, term, data) in &state.entries {
            put(into, *index);
            put(into, *term);
            put(into, data.len() as u64);
            into.extend_from_slice(data);
        }
        Ok((state.applied, state.configuration.clone()))
    }
    fn image_bytes(&self) -> Option<u64> {
        // As `image` writes it: the count, then each entry's index, term and length, and its data.
        let state = if self.checkpoints {
            &self.durable
        } else {
            &self.now
        };
        let entries = state.entries.iter();
        Some(entries.fold(8, |bytes, (_, _, data)| bytes + 24 + data.len() as u64))
    }
    fn install(&mut self, image: &[u8], at: Point, configuration: &ConfState) -> Result<(), Fatal> {
        let mut bytes = image;
        let mut entries = Vec::new();
        let count = take(&mut bytes).ok_or(Fatal("an image that does not read"))?;
        for _ in 0..count {
            let index = take(&mut bytes).ok_or(Fatal("an image that does not read"))?;
            let term = take(&mut bytes).ok_or(Fatal("an image that does not read"))?;
            let len = take(&mut bytes).ok_or(Fatal("an image that does not read"))? as usize;
            let (data, rest) = bytes.split_at_checked(len).ok_or(Fatal("short image"))?;
            bytes = rest;
            entries.push((index, term, data.to_vec()));
        }
        self.now = KvState {
            applied: at,
            entries,
            configuration: configuration.clone(),
        };
        self.durable = self.now.clone();
        Ok(())
    }
    fn persist(&mut self) -> Result<(), Fatal> {
        if !self.volatile {
            self.durable = self.now.clone();
        }
        Ok(())
    }
}

/// Encodes a configuration for a test's own records.
pub fn encode_conf(c: &ConfState) -> Vec<u8> {
    let mut out = Vec::new();
    put_ids(&mut out, &c.voters);
    put_ids(&mut out, &c.learners);
    out
}
pub fn decode_conf(mut bytes: &[u8]) -> Option<ConfState> {
    Some(ConfState {
        voters: take_ids(&mut bytes)?,
        learners: take_ids(&mut bytes)?,
        ..ConfState::default()
    })
}

/// Whether two configurations name the same members in the same roles.
pub fn same_configuration(a: &ConfState, b: &ConfState) -> bool {
    let sorted = |ids: &[u64]| {
        let mut ids = ids.to_vec();
        ids.sort_unstable();
        ids
    };
    sorted(&a.voters) == sorted(&b.voters)
        && sorted(&a.learners) == sorted(&b.learners)
        && sorted(&a.voters_outgoing) == sorted(&b.voters_outgoing)
        && sorted(&a.learners_next) == sorted(&b.learners_next)
}

/// A count from the environment, for longer soaks.
#[allow(
    clippy::disallowed_methods,
    reason = "a soak sets the seed count from the environment; the default is the gate's"
)]
pub fn count(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
