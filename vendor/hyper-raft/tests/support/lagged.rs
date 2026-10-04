//! A member of this core driven ahead of its persistence (core step R-4,
//! `docs/durable.md` §2.1): it takes a `Ready` and issues its write while
//! earlier writes are out, a write becomes durable on its disk when the
//! schedule says, and its owner hears of what became durable later still.
//! Each of those is a step of the schedule ([`Step`]), so a schedule
//! interleaves them with everything else, and a crash between any two loses
//! exactly what was not durable.
//!
//! The member is held to the invariants of `docs/durable.md` §3 the core
//! keeps, against its disk as it is at each step: what a write holds is
//! what the member held when it took the `Ready` (I7); a message waiting
//! for a write leaves only once the disk holds the term, vote and entries
//! it speaks for (I1, I2), and an answer's commit only once the disk states
//! it (R-6); a leader's own messages leave at once only while its term and
//! vote are durable (I1); what is given to apply is committed and durable,
//! or a leader's own and committed where it applies before its write is
//! durable (I4). What a leader counts and commits is held to its voters'
//! disks by the cluster (`Cluster::check_durable`, I3).
//!
//! Its owner keeps the commit fence as a shell does (`docs/durable.md` §4.1,
//! I5): it states a commit on its disk only as the core's `Ready`s give one,
//! never the commit a notice moved, so a member's commit runs ahead of its
//! disk's as a lazy shell's does; a change of configuration applies only
//! once the disk states a commit covering it, held until then with every
//! entry after it while the core is asked for no more (R-6's apply pause).
use std::collections::VecDeque;

use hyper_raft::proto::{Entry, HardState, Message, MessageType, Snapshot};

use super::{
    App, Disk, Led, New, Output, Replica, Settings, Step, Store, View, apply_to, canonical,
    cluster::holds, committed_of,
};

/// What the schedules reached, so that a test proves it ran what it claims.
#[derive(Clone, Copy, Debug, Default)]
pub struct Coverage {
    /// `Ready`s taken.
    pub taken: u64,
    /// Taken while an earlier write was out.
    pub behind: u64,
    /// Refused at the bound of writes out.
    pub refused: u64,
    /// Writes made durable.
    pub durable: u64,
    /// Notices the core was given.
    pub notices: u64,
    /// Notices of more than one write.
    pub several: u64,
    /// Notices whose messages waited for the next `Ready`.
    pub held_back: u64,
    /// Writes out when their member stopped: lost.
    pub lost: u64,
    /// Answers whose commit was held to the disk's.
    pub answers: u64,
    /// Entries a leader applied before its own write of them was durable.
    pub unpersisted: u64,
    /// Changes of configuration held behind the commit fence.
    pub fenced: u64,
    /// Writes of the hard state alone that the fence asked for.
    pub stated: u64,
    /// Entries a member took into its log from what it kept ahead of a
    /// hole (`Ahead::Kept`, R17), acknowledged with the write that held them.
    pub ahead: u64,
}
impl Coverage {
    pub fn add(&mut self, other: Self) {
        self.taken += other.taken;
        self.behind += other.behind;
        self.refused += other.refused;
        self.durable += other.durable;
        self.notices += other.notices;
        self.several += other.several;
        self.held_back += other.held_back;
        self.lost += other.lost;
        self.answers += other.answers;
        self.unpersisted += other.unpersisted;
        self.fenced += other.fenced;
        self.stated += other.stated;
        self.ahead += other.ahead;
    }
}

/// One write a member issued.
struct Write {
    number: u64,
    snapshot: Option<Snapshot>,
    entries: Vec<Entry>,
    proposals: Vec<Entry>,
    hard_state: Option<HardState>,
    /// What waits for this write to be durable.
    messages: Vec<Message>,
    /// What the member held when it took the `Ready`, from the last entry
    /// both committed and durable on: once this write is durable its disk
    /// holds the same (I7).
    from: u64,
    terms: Vec<u64>,
    vote: (u64, u64),
}

pub struct Lagged {
    pub node: New,
    depth: usize,
    /// Writes issued and not yet durable, oldest first.
    out: VecDeque<Write>,
    /// Writes durable whose owner has not heard of them yet.
    durable: VecDeque<Write>,
    output: Output,
    coverage: Coverage,
    /// Committed entries given to apply and held behind the commit fence: a
    /// change of configuration the disk's commit does not cover, and every
    /// entry after it.
    held: Vec<Entry>,
    apply_unpersisted: bool,
    /// Whether hyper-check's oracles judge the member's outputs in place of the checks here
    /// (`Settings::judged`).
    judged: bool,
}

/// R-6: an answer says of its commit only what the disk states, so a
/// leader that counts it counts what the member reopens with. True when the
/// message is such an answer.
fn check_commit(disk: &Disk, message: &Message, member: u64) -> bool {
    let kind = message.msg_type;
    if !matches!(
        kind,
        MessageType::MsgAppendResponse | MessageType::MsgHeartbeatResponse
    ) {
        return false;
    }
    assert!(
        message.commit <= disk.hard_state.commit,
        "member {member}: {kind:?} said commit {} with {} durable",
        message.commit,
        disk.hard_state.commit
    );
    true
}

/// A message is held to what the disk holds when it may leave (I1, I2,
/// and R-6's commit).
fn check_message(disk: &Disk, message: &Message, member: u64) -> bool {
    let answer = check_commit(disk, message, member);
    let kind = message.msg_type;
    // A pre-vote is asked for a term the member does not take.
    if matches!(
        kind,
        MessageType::MsgRequestPreVote | MessageType::MsgRequestPreVoteResponse
    ) {
        return answer;
    }
    let hard = disk.hard_state;
    assert!(
        hard.term >= message.term,
        "member {member}: {kind:?} of term {} left with term {} durable",
        message.term,
        hard.term
    );
    // A message of an older term than the disk's was superseded by what the
    // member did since: it is as late as the network may make any message
    // (thesis §3.3), and the disk holds a later promise.
    let current = hard.term == message.term;
    match kind {
        MessageType::MsgRequestVote if current => assert_eq!(
            (hard.term, hard.vote),
            (message.term, member),
            "member {member}: a vote asked for before its own was durable"
        ),
        MessageType::MsgRequestVoteResponse if current && !message.reject => assert_eq!(
            (hard.term, hard.vote),
            (message.term, message.to),
            "member {member}: a vote given before it was durable"
        ),
        MessageType::MsgAppendResponse if current && !message.reject => {
            assert!(
                message.index <= disk.last_index(),
                "member {member}: acknowledged {} with {} durable",
                message.index,
                disk.last_index()
            );
        }
        _ if current && kind == hyper_raft::fast::FAST_VOTE => {
            for entry in &message.entries {
                // Once the log reaches the index, what it holds there was the
                // leader's, and supersedes what the member held beside it.
                assert!(
                    holds(disk, entry) || disk.last_index() >= entry.index,
                    "member {member}: said it holds {} before its disk did",
                    entry.index
                );
            }
        }
        _ => {}
    }
    answer
}

impl Lagged {
    fn take(&mut self) -> bool {
        let raw = &mut self.node.raw;
        if !raw.has_ready() {
            return false;
        }
        if raw.in_flight() >= self.depth {
            self.coverage.refused += 1;
            return false;
        }
        let behind = raw.in_flight() > 0;
        let in_place = self.node.in_place;
        let mut ready = if in_place {
            raw.ready_in_place()
        } else {
            raw.ready()
        }
        .expect("a ready");
        let log = raw.raft.log();
        let from = log.committed().min(log.persisted());
        let last = log.last_index().unwrap();
        let terms = (from..=last)
            .map(|index| log.term(index).unwrap())
            .collect();
        let vote = (raw.raft.term(), raw.raft.vote());
        let (snapshot, entries) = if in_place {
            let persist = raw.to_persist();
            (persist.snapshot.cloned(), persist.entries.to_vec())
        } else {
            (ready.snapshot().cloned(), ready.entries().to_vec())
        };
        let id = raw.raft.id();
        let disk = &raw.store().0;
        for message in ready.messages().iter().filter(|_| !self.judged) {
            // I1: a leader's own messages leave at once only with its term
            // and vote durable.
            assert_eq!(
                (disk.hard_state.term, disk.hard_state.vote),
                vote,
                "member {id}: {:?} sent at once before its term was durable",
                message.msg_type
            );
            self.coverage.answers += u64::from(check_commit(disk, message, id));
        }
        for message in ready.messages().iter().chain(ready.persisted_messages()) {
            assert_eq!(
                message.entries.capacity(),
                message.entries.len(),
                "member {id}: a page with spare room"
            );
        }
        let write = Write {
            number: ready.number(),
            snapshot,
            entries,
            proposals: ready.proposals().to_vec(),
            hard_state: ready.hard_state().copied(),
            messages: ready.take_persisted_messages(),
            from,
            terms,
            vote,
        };
        self.output.messages.extend(ready.take_messages());
        self.output.reads.extend(
            ready
                .take_read_states()
                .into_iter()
                .map(|read| (read.index, read.request_ctx)),
        );
        self.output
            .displaced
            .extend(ready.displaced().iter().map(super::said));
        let committed = committed_of(
            &self.node.raw,
            in_place,
            ready.take_committed_entries(),
            ready.committed_range(),
        );
        self.apply(committed);
        self.node.raw.advance_issued(ready).expect("issued");
        self.node
            .raw
            .advance_apply_to(self.node.app.index)
            .expect("applied");
        self.out.push_back(write);
        self.coverage.taken += 1;
        self.coverage.behind += u64::from(behind);
        true
    }

    /// I4: what is given to apply is committed, and durable here, but for a
    /// leader's own entries where it applies before its write is durable
    /// (`Config::apply_unpersisted`, `docs/durable.md` §4.2), which a majority
    /// holds durably (`Cluster::check_durable`). Then the commit fence
    /// ([`Lagged::release`]).
    fn apply(&mut self, committed: Vec<Entry>) {
        let raw = &self.node.raw;
        let raft = &raw.raft;
        let leads = raft.state() == hyper_raft::StateRole::Leader;
        for entry in &committed {
            let durable = holds(&raw.store().0, entry);
            assert!(
                self.judged
                    || entry.index <= raft.log().committed()
                        && (durable
                            || (self.apply_unpersisted && leads && entry.term == raft.term())),
                "member {}: {} given to apply before it was committed and durable",
                raft.id(),
                entry.index
            );
            self.coverage.unpersisted += u64::from(!durable);
        }
        self.held.extend(committed);
        self.release();
    }

    /// I5, the commit fence (`docs/durable.md` §4.1) as a shell keeps it: an
    /// ordinary entry applies on the core's commit; a change of
    /// configuration only once the disk states a commit covering it, or a
    /// member that stops reopens under the configuration before it. Until
    /// then the change and every entry after it are held, and the core is
    /// asked for no more (R-6's apply pause); a write of the hard state
    /// alone states the commit as soon as one can ([`Lagged::state_commit`]).
    fn release(&mut self) {
        let mut through = 0;
        while let Some(entry) = self.held.get(through) {
            let index = entry.index;
            if super::change_of(entry).is_some()
                && index > self.node.raw.durable_commit()
                && !self.state_commit(index)
            {
                break;
            }
            through += 1;
        }
        let ready: Vec<Entry> = self.held.drain(..through).collect();
        apply_to(
            &mut self.node.raw,
            &mut self.node.app,
            ready,
            &mut self.output,
        );
        let raw = &mut self.node.raw;
        if self.held.is_empty() {
            raw.resume_apply();
        } else if !raw.apply_paused() {
            raw.pause_apply();
            self.coverage.fenced += 1;
        }
    }

    /// A write of the hard state alone, durable at once, stating the core's
    /// commit as far as the disk holds the core's log: true when it covers
    /// `index`. Only with no write out: a write out may still replace on the
    /// disk an entry the log holds again (etcd's ABA), and a write stated
    /// behind it would be durable after it. The core is told the commit it
    /// states.
    fn state_commit(&mut self, index: u64) -> bool {
        if !self.out.is_empty() {
            return false;
        }
        let raw = &mut self.node.raw;
        let log = raw.raft.log();
        let disk = &raw.store().0;
        let commit = log.committed().min(disk.last_index());
        if commit < index || disk.term(commit) != log.term(commit).ok() {
            return false;
        }
        let disk = &mut raw.store_mut().0;
        disk.hard_state.commit = disk.hard_state.commit.max(commit);
        raw.commit_durable(commit).expect("a commit the log holds");
        self.coverage.stated += 1;
        true
    }

    fn make_durable(&mut self) -> bool {
        let Some(write) = self.out.pop_front() else {
            return false;
        };
        let id = self.node.raw.raft.id();
        let disk = &mut self.node.raw.store_mut().0;
        if let Some(snapshot) = &write.snapshot {
            disk.install(snapshot);
        }
        disk.append(&write.entries);
        disk.proposals.extend(write.proposals.iter().cloned());
        disk.trim_proposals();
        if let Some(hard) = write.hard_state {
            // The commit a store records never goes back: a compaction since
            // the `Ready` was taken recorded a later one (`Disk::compact`).
            disk.hard_state = HardState {
                commit: hard.commit.max(disk.hard_state.commit),
                ..hard
            };
        }
        // I7: the disk holds what the member held when it took the `Ready`.
        assert_eq!(
            (disk.hard_state.term, disk.hard_state.vote),
            write.vote,
            "member {id}: write {} left another term or vote durable",
            write.number
        );
        let last = write.from + write.terms.len() as u64 - 1;
        assert_eq!(
            disk.last_index().max(disk.snapshot_index()),
            last,
            "member {id}: write {} left another log durable",
            write.number
        );
        for (at, term) in write.terms.iter().enumerate() {
            let index = write.from + at as u64;
            if index >= disk.snapshot_index() {
                assert_eq!(
                    disk.term(index),
                    Some(*term),
                    "member {id}: write {} left another entry durable at {index}",
                    write.number
                );
            }
        }
        for message in write.messages.iter().filter(|_| !self.judged) {
            self.coverage.answers += u64::from(check_message(disk, message, id));
        }
        self.durable.push_back(write);
        self.coverage.durable += 1;
        true
    }

    fn notify(&mut self) -> bool {
        let Some(number) = self.durable.back().map(|write| write.number) else {
            return false;
        };
        if self.durable.len() > 1 {
            self.coverage.several += 1;
        }
        for write in self.durable.drain(..) {
            self.output.messages.extend(write.messages);
            if let Some(snapshot) = write.snapshot {
                let metadata = snapshot.metadata.clone().unwrap_or_default();
                self.output.snapshots.push((metadata.index, metadata.term));
                self.node.app = App::decode(&snapshot.data);
                // The snapshot replaces what the fence held: every entry
                // it held is at or below the snapshot, which states it
                // (seed 730 of the narrow setting applied a held change
                // over the snapshot after it).
                self.held.retain(|entry| entry.index > metadata.index);
            }
        }
        let raw = &mut self.node.raw;
        let mut light = if self.node.in_place {
            raw.on_persist_keeping(number, |store, kept| {
                // What the member gives up is what its disk holds.
                if let Some(snapshot) = &kept.snapshot {
                    assert!(
                        store.0.snapshot_index()
                            >= snapshot
                                .metadata
                                .as_ref()
                                .map_or(0, |metadata| metadata.index)
                    );
                }
                for entry in &kept.entries {
                    assert!(holds(&store.0, entry), "kept {} not held", entry.index);
                }
            })
        } else {
            raw.on_persist(number)
        }
        .expect("persisted");
        let id = raw.raft.id();
        let leads = raw.raft.state() == hyper_raft::StateRole::Leader;
        let messages = light.take_messages();
        if messages.is_empty() && !raw.raft.messages().is_empty() {
            self.coverage.held_back += 1;
        }
        for message in messages.iter().filter(|_| !self.judged) {
            if leads {
                self.coverage.answers += u64::from(check_commit(&raw.store().0, message, id));
                // A pre-vote's answer names the term asked about, which no
                // member takes by it.
                assert!(
                    raw.store().0.hard_state.term >= message.term
                        || message.msg_type == MessageType::MsgRequestPreVoteResponse,
                    "member {id}: {:?} of term {} sent at once with term {} durable (term {} now)",
                    message.msg_type,
                    message.term,
                    raw.store().0.hard_state.term,
                    raw.raft.term()
                );
            } else {
                self.coverage.answers += u64::from(check_message(&raw.store().0, message, id));
            }
        }
        self.output.messages.extend(messages);
        let committed = committed_of(
            raw,
            self.node.in_place,
            light.take_committed_entries(),
            light.committed_range(),
        );
        self.apply(committed);
        self.node
            .raw
            .advance_apply_to(self.node.app.index)
            .expect("applied");
        self.coverage.notices += 1;
        true
    }
}

impl Replica for Lagged {
    const LAGGED: bool = true;
    fn open(id: u64, store: Store, settings: &Settings, seed: u64, members: usize) -> Self {
        Self {
            node: New::open(id, store, settings, seed, members),
            depth: settings.depth,
            out: VecDeque::new(),
            durable: VecDeque::new(),
            output: Output::default(),
            coverage: Coverage::default(),
            held: Vec::new(),
            apply_unpersisted: settings.apply_unpersisted,
            judged: settings.judged,
        }
    }
    fn id(&self) -> u64 {
        self.node.id()
    }
    fn store(&self) -> &Store {
        self.node.store()
    }
    fn store_mut(&mut self) -> &mut Store {
        self.node.store_mut()
    }
    fn tick(&mut self) -> bool {
        self.node.tick()
    }
    fn step(&mut self, message: Message) -> bool {
        let before = self.node.raw.raft.taken_ahead();
        let stepped = self.node.step(message);
        self.coverage.ahead += self.node.raw.raft.taken_ahead() - before;
        stepped
    }
    fn propose(&mut self, data: Vec<u8>) -> bool {
        self.node.propose(data)
    }
    fn propose_fast(&mut self, data: Vec<u8>) -> Option<u64> {
        self.node.propose_fast(data)
    }
    fn propose_change(&mut self, change: &hyper_raft::proto::ConfChangeV2) -> bool {
        self.node.propose_change(change)
    }
    fn campaign(&mut self) -> bool {
        self.node.campaign()
    }
    fn ping(&mut self) {
        self.node.ping();
    }
    fn transfer(&mut self, to: u64) {
        self.node.transfer(to);
    }
    fn read(&mut self, context: Vec<u8>) {
        self.node.read(context);
    }
    fn unreachable(&mut self, member: u64) {
        self.node.unreachable(member);
    }
    fn snapshot_status(&mut self, member: u64, arrived: bool) {
        self.node.snapshot_status(member, arrived);
    }
    fn set_priority(&mut self, priority: i64) {
        self.node.set_priority(priority);
    }
    fn set_window(&mut self, member: u64, bytes: u64) {
        self.node.set_window(member, bytes);
    }
    fn suspect(&mut self, member: u64) {
        self.node.suspect(member);
    }
    fn trust(&mut self, member: u64) {
        self.node.trust(member);
    }
    fn restarted(&mut self, member: u64) {
        self.node.restarted(member);
    }
    fn wake(&mut self, now: u64) -> bool {
        self.node.wake(now)
    }
    fn deadline(&self) -> Option<u64> {
        self.node.deadline()
    }
    fn plant(&mut self, mutant: Option<hyper_raft::Mutant>) {
        self.node.raw.plant(mutant);
    }
    fn set_timeout(&mut self, ticks: usize) {
        self.node.set_timeout(ticks);
    }
    fn drain(&mut self) -> Output {
        let mut output = std::mem::take(&mut self.output);
        output.messages = canonical(output.messages);
        output
    }
    fn persist(&mut self, step: Step) -> bool {
        match step {
            Step::Take => self.take(),
            Step::Durable => self.make_durable(),
            Step::Notify => self.notify(),
            Step::Flush => {
                let mut any = false;
                // Each round takes, writes or hears of something, and what
                // there is to take is bounded by what the member was given.
                for _ in 0..10_000 {
                    let mut moved = false;
                    while self.take() {
                        moved = true;
                    }
                    while self.make_durable() {
                        moved = true;
                    }
                    moved |= self.notify();
                    if !moved {
                        return any;
                    }
                    any = true;
                }
                panic!("member {}: its writes never settled", self.id());
            }
        }
    }
    fn busy(&self) -> bool {
        self.node.raw.has_ready()
            || !self.out.is_empty()
            || !self.durable.is_empty()
            || !self.held.is_empty()
    }
    fn coverage(&self) -> Coverage {
        Coverage {
            lost: self.out.len() as u64,
            ..self.coverage
        }
    }
    fn led(&self) -> Option<Led> {
        let raft = &self.node.raw.raft;
        if raft.state() != hyper_raft::StateRole::Leader {
            return None;
        }
        let log = raft.log();
        let entry_at = |index: u64| {
            if index == 0 || index < log.first_index().ok()? {
                return None;
            }
            log.slice(index, index + 1, u64::MAX)
                .ok()?
                .into_iter()
                .next()
        };
        Some(Led {
            term: raft.term(),
            committed: entry_at(log.committed()),
            own: raft
                .tracker()
                .get(raft.id())
                .and_then(|own| entry_at(own.matched)),
        })
    }
    /// Everything applied becomes the snapshot, once the disk states a
    /// commit covering it: what a restart replays is what was committed
    /// durably, and the log's start never passes it (I8). A leader that
    /// applied its own entries before they were durable here compacts none
    /// of them (`docs/durable.md` §4.2).
    fn compact(&mut self) -> bool {
        let index = self.node.app().index;
        let disk = &self.node.store().0;
        if index <= disk.snapshot_index() {
            return false;
        }
        if index > disk.hard_state.commit && !self.state_commit(index) {
            return false;
        }
        let data = self.node.app().encode();
        self.node.store_mut().0.compact(index, data);
        true
    }
    fn view(&self) -> View {
        self.node.view()
    }
    fn app(&self) -> App {
        self.node.app()
    }
}
