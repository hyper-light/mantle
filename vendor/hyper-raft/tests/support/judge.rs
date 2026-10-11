//! hyper-check's judge of a group (`docs/sim.md` §4, steps S-4 and S-5): every oracle of §4.1 fed
//! after every operation over the whole history, the clients' history recorded for both checkers
//! of linearizability, and the paths a run claims counted. `tests/check.rs` judges the schedule
//! tests' seeds by it, and `tests/strategies.rs` the strategies' runs.
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    unreachable_pub,
    missing_docs
)]

use std::collections::BTreeMap;

use hyper_check::coverage::Counters;
use hyper_check::liveness::Monitor;
use hyper_check::oracle::{
    Durability, DurableView, ElectionSafety, ExactlyOnce, FastAgreement, LeaderCompleteness,
    LogMatching, LogView, ReadSafety, Released, SameHistory, Says, StateMachineSafety, Terms,
    Violation,
};
use hyper_check::search::{Budget, MEMORY_CEILING};
use hyper_check::witness::{Consistency, Event, Initial, Outcome, Request};
use hyper_check::{Access, Agreement, Answer, Register, agree};
use hyper_measure::cost::{Costs, Tails, measure};
use hyper_measure::usage;
use hyper_raft::proto::{ConfState, Entry, Message, MessageType};
use hyper_raft::{Mutant, RawNode};

use super::cluster::Observer;
use super::{Cluster, Disk, Draws, Lagged, Mix, New, Op, Replica, Seeded, Step, Store};

/// What an entry states, as the oracles compare it: its kind and its data.
pub type Value = (u8, Vec<u8>);

pub fn value(entry_type: hyper_raft::proto::EntryType, data: &[u8]) -> Value {
    (entry_type as u8, data.to_vec())
}

/// A member of this core whose `RawNode` the judge reads.
pub trait Core: Replica {
    fn raw(&self) -> &RawNode<Store>;
}
impl Core for New {
    fn raw(&self) -> &RawNode<Store> {
        &self.raw
    }
}
impl Core for Lagged {
    fn raw(&self) -> &RawNode<Store> {
        &self.node.raw
    }
}

/// A member's disk as the durability oracle reads it: what a restart would open on, an entry lost
/// at rest after it was acknowledged held by its mark.
pub struct View<'a>(pub &'a Disk);

impl DurableView<Value> for View<'_> {
    fn term(&self) -> u64 {
        self.0.hard_state.term
    }
    fn vote(&self) -> u64 {
        self.0.hard_state.vote
    }
    fn commit(&self) -> u64 {
        self.0.hard_state.commit
    }
    fn start(&self) -> u64 {
        self.0.snapshot_index()
    }
    fn last(&self) -> u64 {
        self.0.last_index()
    }
    fn term_at(&self, index: u64) -> Option<u64> {
        self.0.term(index)
    }
    fn holds(&self, index: u64, held: &Value) -> bool {
        let disk = self.0;
        let same = |entry: &Entry| value(entry.entry_type, &entry.data) == *held;
        index <= disk.snapshot_index()
            || (index >= disk.first_index()
                && index <= disk.last_index()
                && same(&disk.entries[(index - disk.first_index()) as usize]))
            || disk
                .proposals
                .iter()
                .any(|entry| entry.index == index && same(entry))
            || disk.mark().is_some_and(|lost| lost.index >= index)
    }
}

/// A leader's log, as Leader Completeness reads it.
pub struct LogOf {
    pub start: u64,
    pub entries: BTreeMap<u64, (u64, Value)>,
}

impl LogView<Value> for LogOf {
    fn start(&self) -> u64 {
        self.start
    }
    fn entry(&self, index: u64) -> Option<(u64, &Value)> {
        self.entries.get(&index).map(|(term, value)| (*term, value))
    }
}

/// The entry at `index` of the member leading `term`, while one leads it and its log holds the index.
pub fn held_by_leader<R: Core>(group: &Cluster<R>, term: u64, index: u64) -> Option<Value> {
    let leader = group
        .leaders_now()
        .into_iter()
        .find(|id| group.peek(*id).is_some_and(|node| node.view().term == term))?;
    let log = group.peek(leader)?.raw().raft.log();
    let entry = log
        .slice(index, index + 1, u64::MAX)
        .ok()?
        .into_iter()
        .next()?;
    Some(value(entry.entry_type, &entry.data))
}

pub fn log_of(raw: &RawNode<Store>) -> LogOf {
    let log = raw.raft.log();
    let first = log.first_index().unwrap();
    let last = log.last_index().unwrap();
    let entries = log
        .slice(first, last + 1, u64::MAX)
        .unwrap()
        .into_iter()
        .map(|entry| {
            (
                entry.index,
                (entry.term, value(entry.entry_type, &entry.data)),
            )
        })
        .collect();
    LogOf {
        start: first - 1,
        entries,
    }
}

/// `message` as it leaves its sender.
pub fn released<'a>(message: &Message, held: &'a [(u64, Value)]) -> Released<'a, Value> {
    let kind = message.msg_type;
    let pre_vote = matches!(
        kind,
        MessageType::MsgRequestPreVote | MessageType::MsgRequestPreVoteResponse
    );
    let says = match kind {
        MessageType::MsgRequestVote => Says::VoteRequest {
            last_index: message.index,
            last_term: message.log_term,
        },
        MessageType::MsgRequestVoteResponse => Says::Vote {
            granted: !message.reject,
        },
        MessageType::MsgAppendResponse if !message.reject => Says::Acknowledges {
            index: message.index,
        },
        _ if kind == hyper_raft::fast::FAST_VOTE => Says::Holds { entries: held },
        _ => Says::Nothing,
    };
    let commit = matches!(
        kind,
        MessageType::MsgAppendResponse | MessageType::MsgHeartbeatResponse
    )
    .then_some(message.commit);
    Released {
        from: message.from,
        to: message.to,
        term: (!pre_vote).then_some(message.term),
        says,
        commit,
    }
}

/// A write a leader took.
#[derive(Clone, Debug)]
pub struct Write {
    pub object: u8,
    pub member: u64,
    pub incarnation: u64,
    pub index: u64,
    /// The term it was stamped with; none for the fast track's, which a successor may stamp again.
    pub term: Option<u64>,
    pub value: Value,
    pub call: u64,
    /// The attempt its client made again once its proposer restarted: the client asks, under the
    /// same request, what became of it, and is answered once its index is decided (a session's
    /// question, thesis §6.3, which takes effect at most once).
    pub retry: Option<u64>,
}

/// A read confirmed by a member at an index.
#[derive(Clone, Debug)]
pub struct Confirmed {
    pub call: u64,
    pub object: u8,
    pub member: u64,
    pub incarnation: u64,
    /// The member's term when it confirmed the read: its owner gives the read up with the term.
    pub term: u64,
    pub index: u64,
}

pub type HistoryEvent = Event<u8, u64, Access<u64>, Answer<u64>>;

/// Of each register, the clients that may have an operation open at once: mantle's range
/// simulation's three gateways (`GATEWAYS`), the bound `docs/sim.md` §4.3 gives the search, which is
/// linear in the history and exponential in the operations open at once (Lowe §4). A schedule that
/// asks more of a register while its clients are busy is not recorded, and the state machine applies
/// only the writes recorded.
pub const CLIENTS: usize = 3;

/// The paths a run claims, counted.
pub const PATHS: &[&str] = &[
    "terms led",
    "entries committed",
    "log entries held",
    "leaderships held to the committed entries",
    "messages held to their senders' devices",
    "leaders' commits held to their voters' devices",
    "entries applied held to their members' devices",
    "reads asked",
    "reads recorded",
    "read indexes answered",
    "reads served",
    "writes published",
    "writes answered committed",
    "writes failed, another entry at their index",
    "writes failed, their term ended past the commit",
    "operations left unknown by a restart",
    "operations unrecorded, every client busy",
    "fast votes cast",
    "indexes a fast quorum chose",
];

/// Everything the judge holds over one run.
#[derive(Clone)]
pub struct Judge {
    pub fast: bool,
    pub lagged: bool,
    pub election: ElectionSafety,
    pub matching: LogMatching<Value>,
    pub machine: StateMachineSafety<Value>,
    pub complete: LeaderCompleteness,
    pub agreement: FastAgreement<Value>,
    pub durable: Durability,
    pub reads: ReadSafety,
    pub once: ExactlyOnce,
    pub same: SameHistory,
    /// Reads confirmed and not yet served: each must be, or be given up as its owner gives it up (its
    /// member restarted, in a new term, or out of the configuration): P#'s monitor.
    pub serving: Monitor<u64>,
    pub counters: Counters,
    /// Per member, each index's term as its disk's log last held it, fed to Log Matching.
    pub fed: BTreeMap<u64, BTreeMap<u64, u64>>,
    /// Per member, the digest of the state machine it applied, at its applied index.
    pub digests: BTreeMap<u64, Option<(u64, u64)>>,
    /// Per leader, the term and commit last held to its voters.
    pub led: BTreeMap<u64, (u64, u64)>,
    pub writes: BTreeMap<u64, Write>,
    /// Writes committed, by index.
    pub by_index: BTreeMap<u64, u64>,
    /// Each register's writes as published, by index.
    pub registers: BTreeMap<u8, Vec<(u64, u64)>>,
    pub asked: BTreeMap<Vec<u8>, (u64, u8)>,
    pub confirmed: Vec<Confirmed>,
    pub events: Vec<HistoryEvent>,
    pub calls: u64,
    pub violation: Option<Violation>,
    /// Each recorded operation whose client waits for it, with its register.
    pub open: BTreeMap<u64, u8>,
    /// Each register's clients waiting.
    pub busy: BTreeMap<u8, usize>,
    /// Each recorded read not yet confirmed: its context, and the member asked, its incarnation
    /// and its term then (its owner gives the read up with the term).
    pub asking: BTreeMap<u64, (Vec<u8>, u64, u64, u64)>,
    /// Before the operation acting: its proposer's last index, and the configuration each leader
    /// counted commitment by.
    pub last: u64,
    pub conf: BTreeMap<u64, ConfState>,
}

/// Every table's bound: a run of `steps` operations takes at most a few entries, terms, reads and
/// writes an operation (a burst asks at most eight reads), and the liveness phase as many again.
pub fn bound(steps: u64) -> usize {
    (steps as usize + 20_000) * 16
}

impl Judge {
    pub fn new(fast: bool, lagged: bool, steps: u64) -> Self {
        let bound = bound(steps);
        Self {
            fast,
            lagged,
            election: ElectionSafety::new(bound),
            matching: LogMatching::new(
                if fast {
                    Terms::Ignored
                } else {
                    Terms::Compared
                },
                bound,
            ),
            machine: StateMachineSafety::new(
                if fast {
                    Terms::Ignored
                } else {
                    Terms::Compared
                },
                bound,
            ),
            complete: LeaderCompleteness::new(bound),
            agreement: FastAgreement::new(bound),
            durable: Durability::default(),
            reads: ReadSafety::new(bound),
            once: ExactlyOnce::new(bound),
            same: SameHistory::new(bound),
            serving: Monitor::new(bound),
            counters: Counters::new(PATHS),
            fed: BTreeMap::new(),
            digests: BTreeMap::new(),
            led: BTreeMap::new(),
            writes: BTreeMap::new(),
            by_index: BTreeMap::new(),
            registers: BTreeMap::new(),
            asked: BTreeMap::new(),
            confirmed: Vec::new(),
            events: Vec::new(),
            calls: 0,
            violation: None,
            last: 0,
            conf: BTreeMap::new(),
            open: BTreeMap::new(),
            busy: BTreeMap::new(),
            asking: BTreeMap::new(),
        }
    }

    pub fn call(&mut self) -> u64 {
        self.calls += 1;
        self.calls
    }

    /// A client of `object` takes up an operation, if one is free; the operation is recorded only
    /// then.
    pub fn hold(&mut self, object: u8) -> bool {
        let busy = self.busy.entry(object).or_insert(0);
        if *busy >= CLIENTS {
            self.counters
                .hit("operations unrecorded, every client busy");
            return false;
        }
        *busy += 1;
        true
    }

    /// The client of the operation `call` is answered for good, and free.
    pub fn release(&mut self, call: u64) {
        if let Some(object) = self.open.remove(&call)
            && let Some(busy) = self.busy.get_mut(&object)
        {
            *busy -= 1;
        }
    }

    /// Before `op` acts: reads are asked at the commit known now.
    pub fn ask<R: Core>(&mut self, group: &Cluster<R>, op: &Op) {
        let known = group
            .up()
            .into_iter()
            .filter_map(|id| group.peek(id).map(|node| node.view().commit))
            .max()
            .unwrap_or(0);
        self.reads.committed(known);
        self.agreement.committed(known);
        let contexts: Vec<Vec<u8>> = match op {
            Op::Read(_, context) => vec![context.clone()],
            Op::Reads(_, contexts) => contexts.clone(),
            _ => return,
        };
        let member = match op {
            Op::Read(member, _) | Op::Reads(member, _) => *member,
            _ => return,
        };
        // A member down is asked nothing, and one that knows no leader refuses the read at once.
        let view = group.peek(member).map(|node| node.view());
        let answerable = view
            .as_ref()
            .is_some_and(|view| view.role == 2 || view.leader != 0);
        for context in contexts {
            let read = u64::from_le_bytes(context[..8].try_into().unwrap());
            let object = (read % 2) as u8;
            note(&mut self.violation, self.reads.asked(read));
            self.counters.hit("reads asked");
            if !answerable || !self.hold(object) {
                continue;
            }
            let call = self.call();
            self.open.insert(call, object);
            let view = view.as_ref().unwrap();
            self.asking.insert(
                call,
                (
                    context.clone(),
                    member,
                    group.incarnations[&member],
                    view.term,
                ),
            );
            self.asked.insert(context, (call, object));
            self.events.push(Event::Invoke {
                call,
                request: Request::Read {
                    object,
                    input: Access::Read,
                    consistency: Consistency::Linearizable,
                },
            });
            self.counters.hit("reads recorded");
        }
    }

    /// A proposal taken: the write it is.
    pub fn proposed<R: Core>(
        &mut self,
        group: &Cluster<R>,
        member: u64,
        data: &[u8],
        last: u64,
        fast: bool,
    ) {
        let Some(node) = group.peek(member) else {
            return;
        };
        let view = node.view();
        let (index, term) = if fast {
            let held: Vec<u64> = node
                .raw()
                .raft
                .proposals()
                .filter(|held| held.data == data)
                .map(|held| held.index)
                .collect();
            match held.as_slice() {
                [index]
                    if !self
                        .writes
                        .values()
                        .any(|w| w.index == *index && w.value.1 == data) =>
                {
                    (*index, None)
                }
                _ => return,
            }
        } else {
            if view.role != 2 || view.last_index != last + 1 {
                return;
            }
            (view.last_index, Some(view.term))
        };
        let object = data.first().copied().unwrap_or(0) % 2;
        if !self.hold(object) {
            return;
        }
        let call = self.call();
        self.open.insert(call, object);
        self.writes.insert(
            call,
            Write {
                object,
                member,
                incarnation: group.incarnations[&member],
                index,
                term,
                value: value(hyper_raft::proto::EntryType::EntryNormal, data),
                call,
                retry: None,
            },
        );
        self.events.push(Event::Invoke {
            call,
            request: Request::Mutation {
                object,
                key: call,
                input: Access::Write(call),
            },
        });
    }

    /// The write a committed entry is, if the history recorded it.
    pub fn write_at(&self, index: u64, term: u64, held: &Value) -> Option<u64> {
        self.writes
            .values()
            .find(|write| {
                write.index == index
                    && write.value == *held
                    && write.term.is_none_or(|stamped| stamped == term)
                    && !self.by_index.values().any(|call| *call == write.call)
            })
            .map(|write| write.call)
    }

    /// An entry committed for the first time: a write of the history is published.
    pub fn publish(&mut self, index: u64, term: u64, held: &Value) {
        let Some(call) = self.write_at(index, term, held) else {
            return;
        };
        self.by_index.insert(index, call);
        let object = self.writes[&call].object;
        let register = self.registers.entry(object).or_default();
        register.push((index, call));
        let sequence = register.len() as u64;
        self.events.push(Event::Publish {
            object,
            sequence,
            key: call,
            input: Access::Write(call),
        });
        self.counters.hit("writes published");
    }

    /// `index` is decided: each write whose client asked again what became of it is answered.
    pub fn decide(&mut self, index: u64) {
        let asked: Vec<(u64, u64)> = self
            .writes
            .values()
            .filter(|write| write.index == index)
            .filter_map(|write| write.retry.map(|retry| (write.call, retry)))
            .collect();
        for (call, retry) in asked {
            self.writes.remove(&call);
            let outcome = if self.by_index.get(&index) == Some(&call) {
                note(&mut self.violation, self.once.acknowledged(call));
                self.counters.hit("writes answered committed");
                Outcome::Committed {
                    output: Answer::Written,
                }
            } else {
                self.counters
                    .hit("writes failed, another entry at their index");
                Outcome::Failed
            };
            self.events.push(Event::Complete {
                call: retry,
                outcome,
            });
            self.release(call);
        }
    }

    /// The group settled under a leader at `view`, which committed in its term: a write its
    /// client asked about again, of an older term and past that commit, can never be committed
    /// (a later leader's log holds an entry of a later term at every index past the leader's
    /// first of its term, thesis §3.6), and its client is told it failed.
    pub fn decided(&mut self, view: super::View) {
        let dead: Vec<(u64, u64)> = self
            .writes
            .values()
            .filter(|write| {
                write.index > view.commit && write.term.is_some_and(|term| term < view.term)
            })
            .filter_map(|write| write.retry.map(|retry| (write.call, retry)))
            .collect();
        for (call, retry) in dead {
            self.writes.remove(&call);
            self.counters
                .hit("writes failed, their term ended past the commit");
            self.events.push(Event::Complete {
                call: retry,
                outcome: Outcome::Failed,
            });
            self.release(call);
        }
    }

    /// `member` applied `index`: its own write there is answered.
    pub fn answer_writes(&mut self, member: u64, index: u64) {
        let mine: Vec<u64> = self
            .writes
            .values()
            .filter(|write| write.member == member && write.index == index)
            .map(|write| write.call)
            .collect();
        for call in mine {
            self.writes.remove(&call);
            let outcome = if self.by_index.get(&index) == Some(&call) {
                note(&mut self.violation, self.once.acknowledged(call));
                self.counters.hit("writes answered committed");
                Outcome::Committed {
                    output: Answer::Written,
                }
            } else {
                self.counters
                    .hit("writes failed, another entry at their index");
                Outcome::Failed
            };
            self.events.push(Event::Complete { call, outcome });
            self.release(call);
        }
    }

    pub fn applied(&mut self, member: u64, disk: &Disk, view: &super::View, said: &super::Said) {
        let (index, term, entry_type, data) = said;
        let held = value(*entry_type, data);
        let first = !self.machine.iter().any(|(at, ..)| at == *index);
        note(
            &mut self.violation,
            self.machine.committed(member, *index, *term, &held),
        );
        self.counters.hit("entries committed");
        self.reads.committed(*index);
        self.agreement.committed(*index);
        if first {
            self.publish(*index, *term, &held);
            self.decide(*index);
        }
        if let Some(call) = self.by_index.get(index).copied() {
            note(&mut self.violation, self.once.applied(member, *index, call));
        }
        if self.lagged {
            let own = view.role == 2 && *term == view.term;
            let outcome = self.durable.applied(
                member,
                &View(disk),
                *index,
                &held,
                view.commit.max(*index * u64::from(own)),
                own,
            );
            note(&mut self.violation, outcome);
            self.counters
                .hit("entries applied held to their members' devices");
        }
        let digest = self.digests.entry(member).or_insert(Some((0, 0)));
        if let Some((at, folded)) = digest
            && *index == *at + 1
        {
            let mut next = *folded ^ 0xcbf2_9ce4_8422_2325;
            for byte in index
                .to_le_bytes()
                .iter()
                .chain(&[*entry_type as u8])
                .chain(data)
            {
                next = (next ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
            }
            *digest = Some((*index, next));
            let outcome = self.same.reached(member, *index, next);
            note(&mut self.violation, outcome);
        }
        self.answer_writes(member, *index);
    }

    /// `member` installed a snapshot through `index`.
    pub fn installed(&mut self, member: u64, index: u64) {
        note(&mut self.violation, self.once.installed(member, index));
        self.digests
            .insert(member, self.same.at(index).map(|digest| (index, digest)));
        // Its writes at or below it are answered by what was committed there.
        let mine: Vec<u64> = self
            .writes
            .values()
            .filter(|write| write.member == member && write.index <= index)
            .map(|write| write.index)
            .collect();
        for at in mine {
            self.answer_writes(member, at);
        }
    }

    /// A member opened again: what its owner waited for it does not know.
    pub fn restarted<R: Core>(&mut self, group: &Cluster<R>) {
        let lost: Vec<u64> = self
            .writes
            .values()
            .filter(|write| write.retry.is_none())
            .filter(|write| {
                // Its proposer opened again, or no longer leads the term it took the write in:
                // its owner gives up the term's waits (as focal's does) and its client asks again.
                let deposed = write.term.is_some_and(|term| {
                    group
                        .peek(write.member)
                        .is_none_or(|node| node.view().term != term || node.view().role != 2)
                });
                group.incarnations[&write.member] != write.incarnation || deposed
            })
            .map(|write| write.call)
            .collect();
        for call in lost {
            self.unknown(call);
            let retry = self.call();
            let Some(write) = self.writes.get_mut(&call) else {
                continue;
            };
            write.member = 0;
            write.retry = Some(retry);
            let (object, index) = (write.object, write.index);
            self.events.push(Event::Invoke {
                call: retry,
                request: Request::Mutation {
                    object,
                    key: call,
                    input: Access::Write(call),
                },
            });
            if self.machine.at(index).is_some() {
                self.decide(index);
            }
        }
        let (gone, kept): (Vec<Confirmed>, Vec<Confirmed>) =
            self.confirmed.drain(..).partition(|read| {
                group.incarnations[&read.member] != read.incarnation
                    || group
                        .peek(read.member)
                        .is_none_or(|node| node.view().term != read.term)
            });
        self.confirmed = kept;
        for read in gone {
            self.serving.meet(&read.call);
            self.release(read.call);
            self.unknown(read.call);
        }
        let given_up: Vec<u64> = self
            .asking
            .iter()
            .filter(|(_, (_, member, incarnation, term))| {
                group.incarnations[member] != *incarnation
                    || group
                        .peek(*member)
                        .is_none_or(|node| node.view().term != *term)
            })
            .map(|(call, _)| *call)
            .collect();
        for call in given_up {
            if let Some((context, ..)) = self.asking.remove(&call) {
                self.asked.remove(&context);
            }
            self.release(call);
            self.unknown(call);
        }
        for (member, incarnation) in &group.incarnations {
            if let Some(disk_index) = group.peek(*member).map(|node| node.app().index)
                && self
                    .digests
                    .get(member)
                    .is_some_and(|digest| digest.is_some_and(|(at, _)| at > disk_index))
            {
                let _ = incarnation;
                let reopened = self.same.at(disk_index).map(|digest| (disk_index, digest));
                let start = if disk_index == 0 {
                    Some((0, 0))
                } else {
                    reopened
                };
                self.digests.insert(*member, start);
            }
        }
    }

    pub fn unknown(&mut self, call: u64) {
        self.counters.hit("operations left unknown by a restart");
        self.events.push(Event::Complete {
            call,
            outcome: Outcome::Unknown,
        });
    }

    /// Reads confirmed whose members applied through their index are answered.
    pub fn serve<R: Core>(&mut self, group: &Cluster<R>) {
        let mut waiting = Vec::new();
        for read in std::mem::take(&mut self.confirmed) {
            let applied = group.peek(read.member).map_or(0, |node| node.app().index);
            if applied < read.index {
                waiting.push(read);
                continue;
            }
            let register = self.registers.get(&read.object);
            let seen: Vec<&(u64, u64)> = register
                .map(|writes| writes.iter().filter(|(at, _)| *at <= applied).collect())
                .unwrap_or_default();
            let sequence = seen.len() as u64;
            let current = seen.last().map(|(_, call)| *call);
            self.serving.meet(&read.call);
            self.release(read.call);
            self.counters.hit("reads served");
            self.events.push(Event::Complete {
                call: read.call,
                outcome: Outcome::Read {
                    sequence,
                    output: Answer::Read(current),
                },
            });
        }
        self.confirmed = waiting;
    }

    /// Log Matching over what each member's disk holds now: each entry new or changed since.
    pub fn logs<R: Core>(&mut self, group: &Cluster<R>) {
        for id in group.ids() {
            let disk = group.disk(id);
            let fed = self.fed.entry(id).or_default();
            fed.retain(|index, _| *index > disk.snapshot_index());
            let mut fresh = Vec::new();
            for entry in &disk.entries {
                if fed.get(&entry.index) != Some(&entry.term) {
                    fed.insert(entry.index, entry.term);
                    let before = disk.term(entry.index - 1).unwrap_or(0);
                    fresh.push((
                        entry.index,
                        entry.term,
                        value(entry.entry_type, &entry.data),
                        before,
                    ));
                }
            }
            fed.retain(|index, _| *index <= disk.last_index());
            let commit = disk.hard_state.commit.max(disk.snapshot_index());
            for (index, term, held, before) in fresh {
                let before_committed = index.saturating_sub(1) <= commit;
                let outcome =
                    self.matching
                        .holds_at(id, index, term, &held, before, before_committed);
                note(&mut self.violation, outcome);
                self.counters.hit("log entries held");
            }
        }
    }

    /// I3 for each member that leads: its own count, and each commit it moved to.
    pub fn leaders<R: Core>(&mut self, group: &Cluster<R>, before: &BTreeMap<u64, ConfState>) {
        for id in group.up() {
            let Some(led) = group.peek(id).and_then(Replica::led) else {
                continue;
            };
            let disk = group.disk(id);
            if let Some(own) = &led.own {
                let outcome = self.durable.counted_self(
                    id,
                    &View(disk),
                    own.index,
                    &value(own.entry_type, &own.data),
                );
                note(&mut self.violation, outcome);
            }
            let Some(committed) = &led.committed else {
                continue;
            };
            let last = self.led.insert(id, (led.term, committed.index));
            if last.is_none_or(|(term, index)| term != led.term || index == committed.index) {
                continue;
            }
            let halves = |conf: &ConfState| vec![conf.voters.clone(), conf.voters_outgoing.clone()];
            let after = halves(&led.counted_by);
            let earlier = before.get(&id).map(halves).unwrap_or_else(|| after.clone());
            let after: Vec<&[u64]> = after.iter().map(Vec::as_slice).collect();
            let earlier: Vec<&[u64]> = earlier.iter().map(Vec::as_slice).collect();
            let views: BTreeMap<u64, View> = group
                .ids()
                .into_iter()
                .map(|id| (id, View(group.disk(id))))
                .collect();
            let outcome = self.durable.committed(
                id,
                committed.index,
                &value(committed.entry_type, &committed.data),
                &[&after, &earlier],
                |voter| views.get(&voter),
            );
            note(&mut self.violation, outcome);
            self.counters
                .hit("leaders' commits held to their voters' devices");
        }
    }

    /// One report: what a member did in an operation.
    pub fn report<R: Core>(&mut self, group: &Cluster<R>, report: &super::Report) {
        let member = report.member;
        let disk = group.disk(member).clone();
        for said in &report.output.committed {
            self.applied(member, &disk, &report.view, said);
        }
        for (index, _) in &report.output.snapshots {
            self.installed(member, *index);
        }
        for (index, context) in &report.output.reads {
            let read = u64::from_le_bytes(context[..8].try_into().unwrap());
            note(
                &mut self.violation,
                self.reads.answered(member, read, *index),
            );
            self.counters.hit("read indexes answered");
            if let Some((call, object)) = self.asked.remove(context) {
                self.asking.remove(&call);
                note(
                    &mut self.violation,
                    self.serving.raise(call, 0).map_err(|_| Violation::Full {
                        oracle: "the serving monitor",
                        bound: 0,
                    }),
                );
                self.confirmed.push(Confirmed {
                    call,
                    object,
                    member,
                    incarnation: group.incarnations[&member],
                    term: report.view.term,
                    index: *index,
                });
            }
        }
        for message in &report.output.messages {
            let held: Vec<(u64, Value)> = message
                .entries
                .iter()
                .map(|entry| (entry.index, value(entry.entry_type, &entry.data)))
                .collect();
            if message.msg_type == hyper_raft::fast::FAST_VOTE {
                for (index, cast) in &held {
                    let leaders = held_by_leader(group, message.term, *index);
                    let outcome =
                        self.agreement
                            .vote(message.term, *index, member, cast, leaders.as_ref());
                    note(&mut self.violation, outcome);
                    self.counters.hit("fast votes cast");
                }
            }
            let outcome = self
                .durable
                .released(&View(&disk), &released(message, &held));
            note(&mut self.violation, outcome);
            self.counters.hit("messages held to their senders' devices");
        }
        if report.view.role == 2 {
            note(
                &mut self.violation,
                self.election.leader(member, report.view.term),
            );
            if let Some(node) = group.peek(member)
                && node.view().role == 2
                && node.view().term == report.view.term
            {
                if self.fast {
                    let raft = &node.raw().raft;
                    let configuration = raft.configuration();
                    let outcome = self.agreement.term(
                        report.view.term,
                        configuration.voters(),
                        configuration.outgoing(),
                        |voters| hyper_raft::Quorum::Fast.of(voters),
                    );
                    note(&mut self.violation, outcome);
                }
                let before = self.complete.leaderships();
                let log = log_of(node.raw());
                let outcome = self
                    .complete
                    .leader(member, report.view.term, &log, &self.machine);
                note(&mut self.violation, outcome);
                if self.complete.leaderships() > before {
                    self.counters
                        .hit("leaderships held to the committed entries");
                }
            }
        }
    }

    /// `op` acted, giving `reports`.
    pub fn acted<R: Core>(&mut self, group: &Cluster<R>, op: &Op, reports: &[super::Report]) {
        // The group's commit, first reached at a leader: what it passed was committed in its term.
        if let Some((commit, term)) = group
            .up()
            .into_iter()
            .filter_map(|id| {
                group
                    .peek(id)
                    .map(|node| (node.view().commit, node.view().term))
            })
            .max_by_key(|(commit, term)| (*commit, std::cmp::Reverse(*term)))
        {
            note(
                &mut self.violation,
                self.complete.committed_in(commit, term),
            );
        }
        let last = self.last;
        match op {
            Op::Propose(member, data) => self.proposed(group, *member, data, last, false),
            Op::Fast(member, data) => self.proposed(group, *member, data, last, true),
            _ => {}
        }
        for report in reports {
            self.report(group, report);
        }
        if let Op::Persist(member, Step::Durable) = op {
            let outcome = self.durable.written(*member, &View(group.disk(*member)));
            note(&mut self.violation, outcome);
        }
        self.restarted(group);
        // A member opens on the snapshot its disk holds, one it took and was stopped before it
        // heard it was durable among them: what it applied is what that snapshot holds.
        if let Op::Restart(member) | Op::Corrupt(member, _) = op {
            let index = group.disk(*member).snapshot_index();
            if index > 0 {
                self.installed(*member, index);
            }
        }
        self.serve(group);
        self.logs(group);
        if self.lagged {
            let conf = std::mem::take(&mut self.conf);
            self.leaders(group, &conf);
        }
    }
}

/// `outcome` kept when it is the run's first violation.
pub fn note(first: &mut Option<Violation>, outcome: Result<(), Violation>) {
    if let Err(violation) = outcome
        && first.is_none()
    {
        *first = Some(violation);
    }
}

/// What one judged run gave.
pub struct Judged {
    pub violation: Option<Violation>,
    pub events: Vec<HistoryEvent>,
    pub counters: Counters,
    pub settled: bool,
    /// The serving monitor's obligations open when the run ended (a read confirmed and never
    /// served), if any.
    pub hot: Option<String>,
    /// What the driver checks beside the oracles and found broken: a step no run of the model
    /// takes (`hyper_check::conform`).
    pub departure: Option<String>,
    /// Operations acted, the liveness phase's included.
    pub steps: u64,
}

/// What chooses a judged run's operations, and watches each besides the judge.
pub trait Driver<R> {
    /// The next operation, or `None` when the schedule has ended.
    fn choose(&mut self, group: &mut Cluster<R>, mix: &Mix) -> Option<Op>;
    /// `op` acted: a departure from what the driver holds the run to, if any.
    fn acted(&mut self, _group: &Cluster<R>, _op: &Op) -> Option<String> {
        None
    }
}

/// The schedule of a seed, drawn by `Cluster::choose`.
pub struct Seed<D>(pub D);

impl<R: Replica, D: Draws> Driver<R> for Seed<D> {
    fn choose(&mut self, group: &mut Cluster<R>, mix: &Mix) -> Option<Op> {
        Some(group.choose(&mut self.0, mix))
    }
}

/// The judge and a driver watching one run together.
struct Both<'a, D> {
    judge: Judge,
    driver: &'a mut D,
    departure: Option<String>,
    steps: u64,
}

impl<R: Core, D: Driver<R>> Observer<R> for Both<'_, D> {
    fn before(&mut self, group: &Cluster<R>, op: &Op) {
        <Judge as Observer<R>>::before(&mut self.judge, group, op);
    }

    fn after(&mut self, group: &Cluster<R>, op: &Op, reports: &[super::Report]) {
        <Judge as Observer<R>>::after(&mut self.judge, group, op, reports);
        self.steps += 1;
        if self.departure.is_none() {
            self.departure = self.driver.acted(group, op);
        }
    }
}

/// `group` driven through the schedule of `seed` and its liveness phase (`Cluster::settles`),
/// judged after every operation; a run that breaks a property stops there.
pub fn judged<R: Core>(
    group: Cluster<R>,
    seed: u64,
    steps: u64,
    mix: &Mix,
    mutant: Option<Mutant>,
) -> Judged {
    drive(group, steps, mix, mutant, &mut Seed(Seeded(seed)))
}

/// `group` driven by `driver` for at most `steps` operations and then through its liveness phase,
/// judged after every operation and watched by the driver; a run that breaks a property, or
/// departs from what the driver holds it to, stops there.
pub fn drive<R: Core, D: Driver<R>>(
    mut group: Cluster<R>,
    steps: u64,
    mix: &Mix,
    mutant: Option<Mutant>,
    driver: &mut D,
) -> Judged {
    group.plant(mutant);
    let mut both = Both {
        judge: Judge::new(group.settings.fast, R::LAGGED, steps),
        driver,
        departure: None,
        steps: 0,
    };
    for _ in 0..steps {
        let Some(op) = both.driver.choose(&mut group, mix) else {
            break;
        };
        group.act_observed(&mut both, &op);
        if both.judge.violation.is_some() || both.departure.is_some() {
            return finish_both(both, group, false);
        }
    }
    let settled = group.settles_observed(&mut both);
    finish_both(both, group, settled)
}

fn finish_both<R: Core, D>(both: Both<'_, D>, group: Cluster<R>, settled: bool) -> Judged {
    let settled = settled && both.departure.is_none();
    let mut judged = finish(both.judge, group, settled);
    judged.departure = both.departure;
    judged.steps = both.steps;
    judged
}

impl<R: Core> Observer<R> for Judge {
    fn before(&mut self, group: &Cluster<R>, op: &Op) {
        self.ask(group, op);
        self.last = match op {
            Op::Propose(member, _) | Op::Fast(member, _) => {
                group.peek(*member).map_or(0, |node| node.view().last_index)
            }
            _ => 0,
        };
        // What each leader counts commitment by: the newest configuration its log states.
        self.conf = group
            .ids()
            .into_iter()
            .filter_map(|id| {
                group
                    .peek(id)
                    .and_then(Replica::led)
                    .map(|led| (id, led.counted_by))
            })
            .collect();
    }

    fn after(&mut self, group: &Cluster<R>, op: &Op, reports: &[super::Report]) {
        if self.violation.is_none() {
            self.acted(group, op, reports);
        }
    }
}

pub fn finish<R: Core>(mut judge: Judge, group: Cluster<R>, settled: bool) -> Judged {
    let mut hot = None;
    if settled && judge.violation.is_none() {
        // Once settled, every acknowledged write is on every member of the configuration, and every
        // read confirmed was served.
        let leader = group.leaders_now().into_iter().next().unwrap();
        judge.decided(group.peek(leader).unwrap().view());
        let members = super::members(&group.disk(leader).conf);
        note(&mut judge.violation, judge.once.settled(&members));
        // A member the configuration no longer names is sent nothing more, so a read it confirmed
        // above what it applied is never served: its owner gives it up, as with a new term
        // (seed 2,976 of the fast schedules, a follower removed by the entry its read waited for).
        let (gone, kept): (Vec<Confirmed>, Vec<Confirmed>) = std::mem::take(&mut judge.confirmed)
            .into_iter()
            .partition(|read| !members.contains(&read.member));
        judge.confirmed = kept;
        for read in gone {
            judge.serving.meet(&read.call);
            judge.release(read.call);
            judge.unknown(read.call);
        }
        hot = judge.serving.end().err().map(|hot| hot.to_string());
        judge
            .counters
            .add("terms led", judge.election.terms() as u64);
        judge.counters.add(
            "indexes a fast quorum chose",
            judge.agreement.choices() as u64,
        );
    }
    Judged {
        violation: judge.violation,
        events: judge.events,
        counters: judge.counters,
        settled,
        hot,
        departure: None,
        steps: 0,
    }
}

/// The two registers, from nothing.
pub fn initial() -> Vec<Initial<u8, Option<u64>>> {
    (0..2)
        .map(|object| Initial {
            object,
            sequence: 0,
            state: None,
        })
        .collect()
}

/// What the runs of one schedule test gave: their counters and the checkers' agreement.
#[derive(Default)]
pub struct Totals {
    pub histories: u64,
    pub operations: u64,
    pub configurations: u64,
    /// Each history's configurations searched.
    pub each: Vec<u64>,
    /// What each history's two checkers cost (`docs/tails.md` §1a).
    pub costs: Costs,
}

impl Totals {
    /// The tails of what a history cost, at the median, the 99th percentile (nearest rank) and the
    /// most, with the machine's load: the configurations searched, and the two checkers' CPU time,
    /// instructions, cycles, allocations and the most bytes held at once. The OS's counts are the
    /// process's: a run that reports them runs one test at a time (`--test-threads 1`).
    pub fn tails(&self) -> String {
        let mut each = self.each.clone();
        let configurations = Tails::of(&mut each).unwrap_or_default();
        format!(
            "configurations a history {configurations}; the checkers' cost a history at a load of {:.2}:\n{}",
            usage::load().unwrap_or(f64::NAN),
            self.costs.report()
        )
    }
}

pub fn agreed(seed: u64, events: &[HistoryEvent], totals: &mut Totals) {
    let (found, cost) = measure(|| {
        agree(
            &Register::<u64>::new(),
            &initial(),
            events,
            events.len(),
            &Budget::default(),
        )
    });
    match found {
        Ok(Agreement::Linearizable { searched, .. }) => {
            let configurations = searched
                .values()
                .map(|part| part.configurations)
                .sum::<u64>();
            totals.histories += 1;
            totals.operations += events.len() as u64;
            totals.configurations += configurations;
            totals.each.push(configurations);
            totals.costs.add(&cost);
            // The search's memory is bounded by the memo's ceiling (`docs/sim.md` §7): what a
            // history held at most, by the allocator's count, is held against it.
            assert!(
                u64::try_from(cost.counts.peak).unwrap_or(0) < MEMORY_CEILING as u64,
                "seed {seed}: the checkers held {} bytes",
                cost.counts.peak
            );
        }
        other => panic!("seed {seed}: the checkers did not both pass: {other:?}"),
    }
}

/// `group`'s messages delivered in order, those `allow` refuses lost, until none is left; each
/// operation judged.
pub fn route<R: Core>(
    group: &mut Cluster<R>,
    judge: &mut Judge,
    allow: impl Fn(&Cluster<R>, &Message) -> bool,
) {
    for _ in 0..10_000 {
        let Some(message) = group.net.first() else {
            return;
        };
        let lose = !allow(group, message);
        group.act_observed(
            judge,
            &Op::Deliver {
                at: 0,
                keep: false,
                lose,
            },
        );
    }
    panic!("the network does not fall quiet");
}

/// `id` campaigns, its messages and its voters' answers delivered among `among`, until it leads.
pub fn elect(group: &mut Cluster<New>, judge: &mut Judge, id: u64, among: &[u64]) {
    for _ in 0..4 {
        group.act_observed(judge, &Op::Campaign(id));
        route(group, judge, |_, m| {
            among.contains(&m.from)
                && among.contains(&m.to)
                && matches!(
                    m.msg_type,
                    MessageType::MsgRequestVote | MessageType::MsgRequestVoteResponse
                )
        });
        if group.leaders_now().contains(&id) {
            return;
        }
    }
    panic!("member {id} was not elected among {among:?}");
}

/// Rounds of `leader`'s heartbeat, each delivering what `allow` lets through: enough for its
/// followers to answer, be probed and take what they lack, one entry an append (a probe walks back
/// one index a round, and the play's logs are three entries long).
pub fn replicate(
    group: &mut Cluster<New>,
    judge: &mut Judge,
    leader: u64,
    allow: impl Fn(&Cluster<New>, &Message) -> bool + Copy,
) {
    for _ in 0..8 {
        for _ in 0..group.settings.heartbeat_tick {
            group.act_observed(judge, &Op::Tick(leader));
        }
        route(group, judge, allow);
    }
}

/// Whether `message` is between two of `members`.
pub fn between(members: &[u64], message: &Message) -> bool {
    members.contains(&message.from) && members.contains(&message.to)
}
