//! Three members of a range for counting what they hold (`tests/range_memory.rs`,
//! `tests/range_log_memory.rs`): stores that hold nothing of their own or the node's logs, a
//! network between them the test cuts, and the shapes that fill a member's holds: a leader cut off
//! with its proposals uncommitted and its writes out, followers taking appends with theirs out,
//! entries of the least and the largest bytes, reads waiting for a quorum, and a follower caught up
//! from the leader's log.

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, sync_channel};
use std::task::Waker;

use hyper_block::file::DeviceFile;
use hyper_durable::{EntryRef, Fault, GroupStore, Health, LogStore, Point, StoreView, Write};
use hyper_measure::alloc;
use hyper_raft::StorageError;
use hyper_raft::proto::{Entry, EntryType, HardState};
use hyper_raft::wire::ENTRY_FIXED_BYTES;
use mantle_meta::apply::Layer;
use mantle_meta::engine::{Engine, Model};
use mantle_meta::name;
use mantle_meta::record::{Version, Versioning};
use mantle_meta::wire;
use mantle_node::derive;
use mantle_range::{ConfState, Message, Output, Range, Shell};

/// A group's log as a store that keeps each entry's term and which of the test's payloads it
/// carries, and gives the payload back on every read: what it holds is fixed before counting
/// begins, so the count is the members' own. Writes are answered at once, or held until the test
/// lets them go, up to hyper-log's depth.
pub struct Synth {
    payloads: &'static [Vec<u8>],
    start: Point,
    log: Vec<(u64, u16)>,
    hard_state: HardState,
    depth: usize,
    hold: bool,
    held: usize,
    answers: usize,
}

impl Synth {
    pub fn new(payloads: &'static [Vec<u8>], depth: usize, room: usize) -> Self {
        Self {
            payloads,
            start: Point::default(),
            log: Vec::with_capacity(room),
            hard_state: HardState::default(),
            depth,
            hold: false,
            held: 0,
            answers: 0,
        }
    }

    fn last(&self) -> u64 {
        self.start.index + self.log.len() as u64
    }

    fn at(&self, index: u64) -> Option<(u64, u16)> {
        let offset = index.checked_sub(self.start.index + 1)?;
        self.log.get(usize::try_from(offset).ok()?).copied()
    }

    fn apply(&mut self, write: &Write<'_>) {
        if let Some(start) = write.start {
            let drop = (start.index.saturating_sub(self.start.index) as usize).min(self.log.len());
            self.log.drain(..drop);
            self.start = start;
        }
        if let Some(entries) = write.entries {
            let at = (entries.first - self.start.index - 1) as usize;
            self.log.truncate(at);
            for e in entries.entries {
                assert!(e.context.is_empty() && e.entry_type == EntryType::EntryNormal);
                let kind = self
                    .payloads
                    .iter()
                    .position(|p| *p == e.data)
                    .expect("an entry of a payload the test proposed");
                assert!(self.log.len() < self.log.capacity(), "the store's room");
                self.log.push((e.term, kind as u16));
            }
        }
        if let Some(hard_state) = write.hard_state {
            self.hard_state = hard_state;
        }
    }

    /// Answers every write held.
    fn let_go(&mut self) {
        self.answers += self.held;
        self.held = 0;
    }
}

impl LogStore for Synth {
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
        Ok(StoreView {
            start: self.start,
            last: self.last(),
            hard_state: self.hard_state,
            health: Health::Whole,
        })
    }

    fn bounds(&self) -> Result<(Point, u64), StorageError> {
        Ok((self.start, self.last()))
    }

    fn term(&self, index: u64) -> Result<u64, StorageError> {
        if index == self.start.index {
            return Ok(self.start.term);
        }
        if index < self.start.index {
            return Err(StorageError::Compacted);
        }
        self.at(index)
            .map(|(term, _)| term)
            .ok_or(StorageError::Unavailable)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: &mut Vec<Entry>,
    ) -> Result<(), StorageError> {
        if low <= self.start.index {
            return Err(StorageError::Compacted);
        }
        let mut bytes = 0u64;
        for index in low..high {
            let (term, kind) = self.at(index).ok_or(StorageError::Unavailable)?;
            let data = &self.payloads[usize::from(kind)];
            let size = (ENTRY_FIXED_BYTES + data.len()) as u64;
            if index > low && bytes + size > max_bytes {
                break;
            }
            bytes += size;
            into.push(Entry {
                entry_type: EntryType::EntryNormal,
                term,
                index,
                data: data.clone(),
                context: Vec::new(),
            });
        }
        Ok(())
    }

    fn visit(
        &self,
        low: u64,
        high: u64,
        _page: u64,
        visit: &mut dyn FnMut(EntryRef<'_>) -> bool,
    ) -> Result<(), StorageError> {
        if low <= self.start.index {
            return Err(StorageError::Compacted);
        }
        for index in low..high {
            let (term, kind) = self.at(index).ok_or(StorageError::Unavailable)?;
            let entry = EntryRef {
                index,
                term,
                kind: EntryType::EntryNormal,
                context: &[],
                data: &self.payloads[usize::from(kind)],
            };
            if visit(entry) {
                break;
            }
        }
        Ok(())
    }

    fn proposals(&self, _into: &mut Vec<Entry>) -> Result<(), StorageError> {
        Ok(())
    }

    fn released(&self) -> Result<u64, StorageError> {
        Ok(0)
    }

    fn room(&self) -> bool {
        self.held + self.answers < self.depth
    }

    fn submit(&mut self, write: &Write<'_>, _waker: &Waker) -> Result<(), Fault> {
        self.apply(write);
        if self.hold {
            self.held += 1;
        } else {
            self.answers += 1;
        }
        Ok(())
    }

    fn poll(&mut self) -> Option<Result<(), Fault>> {
        if self.answers == 0 {
            return None;
        }
        self.answers -= 1;
        Some(Ok(()))
    }

    fn write_now(&mut self, write: &Write<'_>) -> Result<(), Fault> {
        self.apply(write);
        Ok(())
    }

    fn keep_after(&mut self, _index: u64) {}

    fn kept_bytes(&self) -> u64 {
        0
    }
}

/// What the harness asks of a member's store beside a log store's own.
pub trait Held: LogStore {
    /// Whether an answer waits for the member's next drive.
    fn answered(&self) -> bool;
    /// Holds the answers from the member, or lets them go.
    fn hold(&mut self, hold: bool);
}

impl Held for Synth {
    fn answered(&self) -> bool {
        self.answers > 0
    }

    fn hold(&mut self, hold: bool) {
        self.hold = hold;
        if !hold {
            self.let_go();
        }
    }
}

/// The node's store, a group of hyper-log's log on a file, with every write made durable before
/// its submission returns, so what a drive gives out never hangs on when the log's threads
/// answered; its answers held from the member while the test says, as a device that holds its
/// writes.
pub struct Real {
    inner: GroupStore<DeviceFile>,
    answers: VecDeque<Result<(), Fault>>,
    waker: Waker,
    woken: Receiver<usize>,
    hold: bool,
}

impl Real {
    pub fn new(inner: GroupStore<DeviceFile>) -> Self {
        // One wake a part answered, taken before the next is asked.
        let (tell, woken) = sync_channel(1 << 10);
        let (waker, _) = hyper_measure::wake::waker(0, tell);
        Self {
            inner,
            answers: VecDeque::with_capacity(8),
            waker,
            woken,
            hold: false,
        }
    }
}

impl LogStore for Real {
    type Hold = std::convert::Infallible;

    fn held(&self) -> Option<&Self::Hold> {
        None
    }

    fn release(&mut self, met: &Self::Hold) {
        match *met {}
    }

    fn depth(&self) -> usize {
        self.inner.depth()
    }

    fn view(&self) -> Result<StoreView, Fault> {
        self.inner.view()
    }

    fn bounds(&self) -> Result<(Point, u64), StorageError> {
        self.inner.bounds()
    }

    fn term(&self, index: u64) -> Result<u64, StorageError> {
        self.inner.term(index)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: &mut Vec<Entry>,
    ) -> Result<(), StorageError> {
        self.inner.entries(low, high, max_bytes, into)
    }

    fn visit(
        &self,
        low: u64,
        high: u64,
        page: u64,
        visit: &mut dyn FnMut(EntryRef<'_>) -> bool,
    ) -> Result<(), StorageError> {
        self.inner.visit(low, high, page, visit)
    }

    fn proposals(&self, into: &mut Vec<Entry>) -> Result<(), StorageError> {
        self.inner.proposals(into)
    }

    fn released(&self) -> Result<u64, StorageError> {
        self.inner.released()
    }

    fn room(&self) -> bool {
        self.inner.room()
    }

    fn submit(&mut self, write: &Write<'_>, _waker: &Waker) -> Result<(), Fault> {
        self.inner.submit(write, &self.waker)?;
        loop {
            if let Some(answer) = self.inner.poll() {
                self.answers.push_back(answer);
                return Ok(());
            }
            self.woken
                .recv()
                .expect("the log answers every part it takes");
        }
    }

    fn poll(&mut self) -> Option<Result<(), Fault>> {
        if self.hold {
            return None;
        }
        self.answers.pop_front()
    }

    fn write_now(&mut self, write: &Write<'_>) -> Result<(), Fault> {
        self.inner.write_now(write)
    }

    fn keep_after(&mut self, index: u64) {
        self.inner.keep_after(index);
    }

    fn kept_bytes(&self) -> u64 {
        self.inner.kept_bytes()
    }
}

impl Held for Real {
    fn answered(&self) -> bool {
        !self.hold && !self.answers.is_empty()
    }

    fn hold(&mut self, hold: bool) {
        self.hold = hold;
    }
}

/// Entries a member's store holds at most in this test.
pub const ROOM: usize = 1 << 21;

/// The reads the leader is asked at once.
pub const READS: usize = 64;

/// The heap a message holds: its entries' slots and buffers, its context and a snapshot's bytes.
pub fn message_heap(m: &Message) -> usize {
    m.entries.capacity() * std::mem::size_of::<Entry>()
        + m.entries
            .iter()
            .map(|e| e.data.capacity() + e.context.capacity())
            .sum::<usize>()
        + m.context.capacity()
        + m.snapshot.as_ref().map_or(0, |s| s.data.capacity())
}

/// Three members of a range and the network between them, which carries every message in order
/// unless the test cuts a pair.
pub struct Group<L: Held> {
    pub members: Vec<Shell<L, Model>>,
    out: Output,
    wire: VecDeque<Message>,
    cut: [[bool; 3]; 3],
    /// The most the messages between the members held at once.
    pub wire_most: usize,
    pub max_entry_bytes: u64,
}

impl<L: Held> Group<L> {
    pub fn new(members: Vec<Shell<L, Model>>, max_entry_bytes: u64) -> Self {
        let mut out = Output::default();
        out.messages.reserve(1 << 12);
        out.answers.reserve(1 << 18);
        out.reads.reserve(1 << 18);
        Self {
            members,
            out,
            wire: VecDeque::with_capacity(1 << 12),
            cut: [[false; 3]; 3],
            wire_most: 0,
            max_entry_bytes,
        }
    }

    pub fn run(&mut self) {
        for _ in 0..100_000 {
            let mut moved = false;
            for m in &mut self.members {
                for _ in 0..1_000 {
                    let driven = m.drive(0, Waker::noop(), &mut self.out).unwrap();
                    for message in self.out.messages.drain(..) {
                        let (from, to) = (message.from as usize - 1, message.to as usize - 1);
                        if !self.cut[from][to] {
                            self.wire.push_back(message);
                        }
                    }
                    self.out.clear();
                    // A write answered at once is taken at the member's next drive.
                    if !driven.more && !m.log_mut().answered() {
                        break;
                    }
                    moved = true;
                }
            }
            let held: usize = self.wire.iter().map(message_heap).sum();
            self.wire_most = self.wire_most.max(held);
            while let Some(message) = self.wire.pop_front() {
                moved = true;
                let to = message.to as usize - 1;
                // A message the core refuses changes nothing, as the network may drop it.
                let _ = self.members[to].step(message);
            }
            if !moved {
                return;
            }
        }
        panic!("the group never settled");
    }

    pub fn cut_off(&mut self, member: usize, cut: bool) {
        for other in 0..3 {
            if other != member {
                self.cut[member][other] = cut;
                self.cut[other][member] = cut;
            }
        }
    }

    pub fn hold(&mut self, member: usize, hold: bool) {
        self.members[member].log_mut().hold(hold);
    }

    /// The leader's heartbeats.
    pub fn beat(&mut self, ticks: usize) {
        for _ in 0..ticks {
            self.members[0].tick().unwrap();
        }
        self.run();
    }

    /// Lets every member's writes go a round at a time, the followers' held again between, until
    /// every member applied the leader's log; then lets them all go.
    pub fn catch_up(&mut self, beat: usize) {
        for _ in 0..1_000_000 {
            let last = self.members[0].core().raft.log().last_index().unwrap();
            if self.members.iter().all(|m| m.applied().index >= last) {
                for m in 0..3 {
                    self.hold(m, false);
                }
                self.run();
                return;
            }
            for m in 0..3 {
                self.hold(m, false);
            }
            self.run();
            for m in 1..3 {
                self.hold(m, true);
            }
            self.beat(beat);
        }
        panic!("the group never caught up");
    }

    /// Proposes `entry` to the leader until it took `most`, or refuses one again once the group
    /// ran with what it took: the group runs only when the leader refuses, so a run's writes
    /// carry every entry taken since the last; how many it took.
    pub fn fill(&mut self, entry: &wire::Entry, most: usize) -> usize {
        let mut taken = 0;
        loop {
            let before = taken;
            while taken < most
                && mantle_range::propose_entry(&mut self.members[0], entry, self.max_entry_bytes)
                    .is_ok()
            {
                taken += 1;
            }
            self.run();
            if taken >= most || taken == before {
                return taken;
            }
        }
    }
}

/// The least entry a range proposes: no commands.
pub fn least() -> wire::Entry {
    wire::Entry {
        at_ns: 0,
        commands: Vec::new(),
    }
}

/// An entry of `bytes` encoded: one put whose key makes it so long, which a range refuses past
/// its bound on a key and so applies to nothing.
pub fn largest(bytes: usize) -> wire::Entry {
    let of = |key: usize| wire::Entry {
        at_ns: 0,
        commands: vec![wire::Sessioned {
            session: 0,
            serial: 1,
            unanswered: 0,
            command: put("k".repeat(key)),
        }],
    };
    let fixed = of(0).encode().unwrap().len();
    let entry = of(bytes - fixed);
    assert_eq!(entry.encode().unwrap().len(), bytes);
    entry
}

pub fn put(key: String) -> wire::Command {
    wire::Command::Name(Box::new(name::Command::Put(name::Put {
        bucket: "b".into(),
        incarnation: 1,
        key,
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
    })))
}

/// The engine of a cell's first Name range.
pub fn first_range() -> Model {
    let mut engine = Model::default();
    engine.install(0, name::first(1).unwrap()).unwrap();
    engine.persist().unwrap();
    engine
}

pub fn range(voters: u64, learners: u64) -> Range {
    let (settings, rules) = derive::cell(3).unwrap();
    Range {
        layer: Layer::Name,
        rules,
        boot: ConfState {
            voters: (1..=voters).collect(),
            learners: (voters + 1..=voters + learners).collect(),
            ..ConfState::default()
        },
        settings,
    }
}

/// Drives `g`, its leader elected, through the shapes that fill a member's holds, and says what
/// each left.
pub fn shapes<L: Held>(g: &mut Group<L>, beat: usize, read: fn() -> alloc::Counts) -> Vec<String> {
    let max = usize::try_from(g.max_entry_bytes).unwrap();
    let mut said = Vec::new();
    let mut say = |what: &str, g: &Group<L>| {
        let c = read();
        said.push(format!(
            "{what}: live {} peak {}; cores {:?}; wire {}",
            c.live,
            c.peak,
            g.members
                .iter()
                .map(|m| m.core().raft.resident_bytes())
                .collect::<Vec<_>>(),
            g.wire_most
        ));
    };
    let mut window = 0;
    for (name, entry) in [("least", least()), ("largest", largest(max))] {
        // The leader cut off, its writes out: proposals until it refuses one.
        g.cut_off(0, true);
        g.hold(0, true);
        let taken = g.fill(&entry, ROOM);
        assert!(taken > 0);
        window = window.max(taken);
        say(&format!("{name}: the leader holds {taken} uncommitted"), g);
        // Its writes let go and the network back, the followers' writes out as they take the
        // leader's appends.
        g.hold(0, false);
        g.hold(1, true);
        g.hold(2, true);
        g.cut_off(0, false);
        g.beat(beat);
        say(&format!("{name}: the followers hold appends"), g);
        g.catch_up(beat);
        say(&format!("{name}: caught up"), g);
    }
    // Reads waiting for a quorum the leader cannot reach.
    g.cut_off(0, true);
    for read in 0..READS {
        g.members[0]
            .read((read as u64).to_le_bytes().to_vec())
            .unwrap();
        g.run();
    }
    say(&format!("{READS} reads wait"), g);
    g.cut_off(0, false);
    g.beat(beat);
    g.catch_up(beat);
    say("the reads answered", g);
    // A follower cut off while the others commit, then caught up from the leader's log.
    g.cut_off(2, true);
    let behind = g.fill(&least(), 3 * window);
    say(&format!("a follower {behind} behind"), g);
    g.cut_off(2, false);
    g.hold(2, true);
    g.beat(beat);
    say("the follower takes appends from the log", g);
    g.catch_up(beat);
    say("caught up", g);
    said
}
