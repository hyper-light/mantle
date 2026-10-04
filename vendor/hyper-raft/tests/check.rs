//! This core's schedules judged by hyper-check (`docs/sim.md` §4, step S-4): the same seeds the
//! schedule tests run (`tests/group.rs`, `tests/fast.rs`, `tests/pipeline.rs`), with every oracle of
//! §4.1 fed after every operation over the whole history, the clients' history recorded and judged
//! by both linearizability checkers, which must agree, and the paths the runs claim held to their
//! floors (§4.4). Then the planted defects of §4.6 (`src/mutant.rs`), each run with the harness's
//! own checks set aside (`Settings::judged`), and caught by the oracle that names it.
//!
//! The clients' history. A proposal a leader takes is a write of a register (one of two, by its
//! first byte), its value its own number: called when proposed, published when its entry is first
//! committed by any member, answered committed when its proposer applies that entry and refused when
//! its proposer applies another at its index. A proposer that restarts, or stops leading the term it
//! took the write in, answers it unknown (its owner gives up the term's waits, as focal's does), and
//! the client asks again under the same request, a session's question (thesis §6.3): that attempt
//! is answered once the index is decided, committed or refused. A read is a
//! read of a register (by its number): called when asked, answered when the member that confirmed it
//! has applied through the index it confirmed, with the register's value there, and unknown when
//! that member restarts first. The state machine whose registers these are applies the writes the
//! history recorded and nothing else, so a proposal a follower forwarded, which a duplicated message
//! can append twice (its owner keeps no session, thesis §6.3), changes no register.
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    unreachable_pub,
    missing_docs
)]
mod support;

use std::collections::BTreeMap;

use hyper_check::coverage::{Counters, Floor, Measured, Per, hold};
use hyper_check::liveness::Monitor;
use hyper_check::oracle::{
    Durability, DurableView, ElectionSafety, ExactlyOnce, FastAgreement, LeaderCompleteness,
    LogMatching, LogView, ReadSafety, Released, Rule, SameHistory, Says, StateMachineSafety, Terms,
    Violation,
};
use hyper_check::search::{Budget, MEMORY_CEILING};
use hyper_check::witness::{Consistency, Event, Initial, Outcome, Request};
use hyper_check::{Access, Agreement, Answer, Register, agree};
use hyper_measure::alloc::Counting;
use hyper_measure::cost::{Costs, Tails, measure};
use hyper_measure::usage;
use hyper_raft::proto::{ConfState, Entry, Message, MessageType};
use hyper_raft::{Mutant, RawNode};
use support::cluster::Observer;
use support::{Cluster, Disk, Lagged, Mix, New, Op, Replica, Seeded, Settings, Step, Store};

#[global_allocator]
static ALLOCATOR: Counting = Counting;

#[allow(
    clippy::disallowed_methods,
    reason = "a soak sets the seed count from the environment; the default is the gate's"
)]
fn count(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// What an entry states, as the oracles compare it: its kind and its data.
type Value = (u8, Vec<u8>);

fn value(entry_type: hyper_raft::proto::EntryType, data: &[u8]) -> Value {
    (entry_type as u8, data.to_vec())
}

/// A member of this core whose `RawNode` the judge reads.
trait Core: Replica {
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
struct View<'a>(&'a Disk);

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
struct LogOf {
    start: u64,
    entries: BTreeMap<u64, (u64, Value)>,
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
fn held_by_leader<R: Core>(group: &Cluster<R>, term: u64, index: u64) -> Option<Value> {
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

fn log_of(raw: &RawNode<Store>) -> LogOf {
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
fn released<'a>(message: &Message, held: &'a [(u64, Value)]) -> Released<'a, Value> {
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
struct Write {
    object: u8,
    member: u64,
    incarnation: u64,
    index: u64,
    /// The term it was stamped with; none for the fast track's, which a successor may stamp again.
    term: Option<u64>,
    value: Value,
    call: u64,
    /// The attempt its client made again once its proposer restarted: the client asks, under the
    /// same request, what became of it, and is answered once its index is decided (a session's
    /// question, thesis §6.3, which takes effect at most once).
    retry: Option<u64>,
}

/// A read confirmed by a member at an index.
#[derive(Clone, Debug)]
struct Confirmed {
    call: u64,
    object: u8,
    member: u64,
    incarnation: u64,
    /// The member's term when it confirmed the read: its owner gives the read up with the term.
    term: u64,
    index: u64,
}

type HistoryEvent = Event<u8, u64, Access<u64>, Answer<u64>>;

/// Of each register, the clients that may have an operation open at once: mantle's range
/// simulation's three gateways (`GATEWAYS`), the bound `docs/sim.md` §4.3 gives the search, which is
/// linear in the history and exponential in the operations open at once (Lowe §4). A schedule that
/// asks more of a register while its clients are busy is not recorded, and the state machine applies
/// only the writes recorded.
const CLIENTS: usize = 3;

/// The paths a run claims, counted.
const PATHS: &[&str] = &[
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
struct Judge {
    fast: bool,
    lagged: bool,
    election: ElectionSafety,
    matching: LogMatching<Value>,
    machine: StateMachineSafety<Value>,
    complete: LeaderCompleteness,
    agreement: FastAgreement<Value>,
    durable: Durability,
    reads: ReadSafety,
    once: ExactlyOnce,
    same: SameHistory,
    /// Reads confirmed and not yet served: each must be, or be given up as its owner gives it up (its
    /// member restarted, in a new term, or out of the configuration): P#'s monitor.
    serving: Monitor<u64>,
    counters: Counters,
    /// Per member, each index's term as its disk's log last held it, fed to Log Matching.
    fed: BTreeMap<u64, BTreeMap<u64, u64>>,
    /// Per member, the digest of the state machine it applied, at its applied index.
    digests: BTreeMap<u64, Option<(u64, u64)>>,
    /// Per leader, the term and commit last held to its voters.
    led: BTreeMap<u64, (u64, u64)>,
    writes: BTreeMap<u64, Write>,
    /// Writes committed, by index.
    by_index: BTreeMap<u64, u64>,
    /// Each register's writes as published, by index.
    registers: BTreeMap<u8, Vec<(u64, u64)>>,
    asked: BTreeMap<Vec<u8>, (u64, u8)>,
    confirmed: Vec<Confirmed>,
    events: Vec<HistoryEvent>,
    calls: u64,
    violation: Option<Violation>,
    /// Each recorded operation whose client waits for it, with its register.
    open: BTreeMap<u64, u8>,
    /// Each register's clients waiting.
    busy: BTreeMap<u8, usize>,
    /// Each recorded read not yet confirmed: its context, and the member asked, its incarnation
    /// and its term then (its owner gives the read up with the term).
    asking: BTreeMap<u64, (Vec<u8>, u64, u64, u64)>,
    /// Before the operation acting: its proposer's last index, and each member's configuration.
    last: u64,
    conf: BTreeMap<u64, ConfState>,
}

/// Every table's bound: a run of `steps` operations takes at most a few entries, terms, reads and
/// writes an operation (a burst asks at most eight reads), and the liveness phase as many again.
fn bound(steps: u64) -> usize {
    (steps as usize + 20_000) * 16
}

impl Judge {
    fn new(fast: bool, lagged: bool, steps: u64) -> Self {
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

    fn call(&mut self) -> u64 {
        self.calls += 1;
        self.calls
    }

    /// A client of `object` takes up an operation, if one is free; the operation is recorded only
    /// then.
    fn hold(&mut self, object: u8) -> bool {
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
    fn release(&mut self, call: u64) {
        if let Some(object) = self.open.remove(&call)
            && let Some(busy) = self.busy.get_mut(&object)
        {
            *busy -= 1;
        }
    }

    /// Before `op` acts: reads are asked at the commit known now.
    fn ask<R: Core>(&mut self, group: &Cluster<R>, op: &Op) {
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
    fn proposed<R: Core>(
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
    fn write_at(&self, index: u64, term: u64, held: &Value) -> Option<u64> {
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
    fn publish(&mut self, index: u64, term: u64, held: &Value) {
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
    fn decide(&mut self, index: u64) {
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
    fn decided(&mut self, view: support::View) {
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
    fn answer_writes(&mut self, member: u64, index: u64) {
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

    fn applied(&mut self, member: u64, disk: &Disk, view: &support::View, said: &support::Said) {
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
    fn installed(&mut self, member: u64, index: u64) {
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
    fn restarted<R: Core>(&mut self, group: &Cluster<R>) {
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

    fn unknown(&mut self, call: u64) {
        self.counters.hit("operations left unknown by a restart");
        self.events.push(Event::Complete {
            call,
            outcome: Outcome::Unknown,
        });
    }

    /// Reads confirmed whose members applied through their index are answered.
    fn serve<R: Core>(&mut self, group: &Cluster<R>) {
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
    fn logs<R: Core>(&mut self, group: &Cluster<R>) {
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
            for (index, term, held, before) in fresh {
                let outcome = self.matching.holds(id, index, term, &held, before);
                note(&mut self.violation, outcome);
                self.counters.hit("log entries held");
            }
        }
    }

    /// I3 for each member that leads: its own count, and each commit it moved to.
    fn leaders<R: Core>(&mut self, group: &Cluster<R>, before: &BTreeMap<u64, ConfState>) {
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
            let after = halves(&disk.conf);
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
    fn report<R: Core>(&mut self, group: &Cluster<R>, report: &support::Report) {
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
    fn acted<R: Core>(&mut self, group: &Cluster<R>, op: &Op, reports: &[support::Report]) {
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
        self.serve(group);
        self.logs(group);
        if self.lagged {
            let conf = std::mem::take(&mut self.conf);
            self.leaders(group, &conf);
        }
    }
}

/// `outcome` kept when it is the run's first violation.
fn note(first: &mut Option<Violation>, outcome: Result<(), Violation>) {
    if let Err(violation) = outcome
        && first.is_none()
    {
        *first = Some(violation);
    }
}

/// What one judged run gave.
struct Judged {
    violation: Option<Violation>,
    events: Vec<HistoryEvent>,
    counters: Counters,
    settled: bool,
    /// The serving monitor's obligations open when the run ended (a read confirmed and never
    /// served), if any.
    hot: Option<String>,
}

/// `group` driven through the schedule of `seed` and its liveness phase (`Cluster::settles`),
/// judged after every operation; a run that breaks a property stops there.
fn judged<R: Core>(
    mut group: Cluster<R>,
    seed: u64,
    steps: u64,
    mix: &Mix,
    mutant: Option<Mutant>,
) -> Judged {
    let mut judge = Judge::new(group.settings.fast, R::LAGGED, steps);
    group.plant(mutant);
    let mut rng = Seeded(seed);
    for _ in 0..steps {
        let op = group.choose(&mut rng, mix);
        group.act_observed(&mut judge, &op);
        if judge.violation.is_some() {
            return finish(judge, group, false);
        }
    }
    let settled = group.settles_observed(&mut judge);
    finish(judge, group, settled)
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
        self.conf = group
            .ids()
            .into_iter()
            .map(|id| (id, group.disk(id).conf.clone()))
            .collect();
    }

    fn after(&mut self, group: &Cluster<R>, op: &Op, reports: &[support::Report]) {
        if self.violation.is_none() {
            self.acted(group, op, reports);
        }
    }
}

fn finish<R: Core>(mut judge: Judge, group: Cluster<R>, settled: bool) -> Judged {
    let mut hot = None;
    if settled && judge.violation.is_none() {
        // Once settled, every acknowledged write is on every member of the configuration, and every
        // read confirmed was served.
        let leader = group.leaders_now().into_iter().next().unwrap();
        judge.decided(group.peek(leader).unwrap().view());
        let members = support::members(&group.disk(leader).conf);
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
    }
}

/// The two registers, from nothing.
fn initial() -> Vec<Initial<u8, Option<u64>>> {
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
struct Totals {
    histories: u64,
    operations: u64,
    configurations: u64,
    /// Each history's configurations searched.
    each: Vec<u64>,
    /// What each history's two checkers cost (`docs/tails.md` §1a).
    costs: Costs,
}

impl Totals {
    /// The tails of what a history cost, at the median, the 99th percentile (nearest rank) and the
    /// most, with the machine's load: the configurations searched, and the two checkers' CPU time,
    /// instructions, cycles, allocations and the most bytes held at once. The OS's counts are the
    /// process's: a run that reports them runs one test at a time (`--test-threads 1`).
    fn tails(&self) -> String {
        let mut each = self.each.clone();
        let configurations = Tails::of(&mut each).unwrap_or_default();
        format!(
            "configurations a history {configurations}; the checkers' cost a history at a load of {:.2}:\n{}",
            usage::load().unwrap_or(f64::NAN),
            self.costs.report()
        )
    }
}

fn agreed(seed: u64, events: &[HistoryEvent], totals: &mut Totals) {
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

fn floors(name: &str, counters: &Counters, seeds: u64, floors: &[Floor]) {
    for (path, count) in counters.iter() {
        println!("{name}: {path}: {count}");
    }
    if let Err(fallen) = hold(floors, counters, seeds) {
        panic!("{name}: {fallen}");
    }
}

fn seed_floor(path: &'static str, count: u64, seeds: u64) -> Floor {
    Floor {
        path,
        per: Per::Seed,
        measured: Measured { count, seeds },
    }
}

fn campaign_floor(path: &'static str, count: u64, seeds: u64) -> Floor {
    Floor {
        path,
        per: Per::Campaign,
        measured: Measured { count, seeds },
    }
}

/// `tests/group.rs`'s schedules of this core, judged.
#[test]
fn the_group_schedules_keep_every_oracle_and_their_histories_are_linearizable() {
    let seeds = count("HYPER_RAFT_SEEDS", 96);
    let steps = count("HYPER_RAFT_STEPS", 4_000);
    let first = count("HYPER_RAFT_SEED", 0);
    let mix = Mix {
        leader_leaves: true,
        bursts: true,
        windows: true,
        ..Mix::everything()
    };
    let mut counters = Counters::new(PATHS);
    let mut totals = Totals::default();
    for seed in first..first + seeds {
        let mut group: Cluster<New> = Cluster::new(5, &[1, 2, 3], Settings::focal(), seed);
        group.stop_who_left = true;
        let run = judged(group, seed, steps, &mix, None);
        assert!(
            run.violation.is_none(),
            "seed {seed}: {}",
            run.violation.unwrap()
        );
        assert!(run.settled, "seed {seed}: the group did not settle");
        assert!(
            run.hot.is_none(),
            "seed {seed}: {}",
            run.hot.unwrap_or_default()
        );
        agreed(seed, &run.events, &mut totals);
        counters.merge(&run.counters);
    }
    println!(
        "{} histories of {} events agreed, {} configurations searched; {}",
        totals.histories,
        totals.operations,
        totals.configurations,
        totals.tails()
    );
    floors("group", &counters, seeds, &group_floors());
}

/// The floors of the group schedules, each set from its count over the default 96 seeds of 4,000
/// steps (2026-10-04, this test at its commit): more than one a seed where the count was, at least
/// one a campaign where it was rarer.
fn group_floors() -> Vec<Floor> {
    vec![
        seed_floor("terms led", 923, 96),
        seed_floor("entries committed", 88861, 96),
        seed_floor("log entries held", 59529, 96),
        seed_floor("leaderships held to the committed entries", 923, 96),
        seed_floor("messages held to their senders' devices", 161772, 96),
        seed_floor("reads asked", 23290, 96),
        seed_floor("reads recorded", 4371, 96),
        seed_floor("read indexes answered", 8596, 96),
        seed_floor("reads served", 2725, 96),
        seed_floor("writes published", 4270, 96),
        seed_floor("writes answered committed", 4270, 96),
        seed_floor("writes failed, another entry at their index", 167, 96),
        campaign_floor("writes failed, their term ended past the commit", 5, 96),
        seed_floor("operations left unknown by a restart", 2959, 96),
    ]
}

/// The group schedules on hostile networks: of a hundred deliveries, a quarter, a half and three
/// quarters lost, and as many repeated later (the quartiles of the range of rates), beside the
/// schedules' reordering, partitions and restarts. Every oracle holds, and both checkers agree.
#[test]
fn the_group_schedules_on_hostile_networks_keep_every_oracle_and_their_histories_are_linearizable()
{
    let seeds = count("HYPER_RAFT_SEEDS", 96);
    let steps = count("HYPER_RAFT_STEPS", 4_000);
    let first = count("HYPER_RAFT_SEED", 0);
    for rate in [25, 50, 75] {
        let mix = Mix {
            leader_leaves: true,
            bursts: true,
            windows: true,
            lose: rate,
            repeat: rate,
            ..Mix::everything()
        };
        let mut totals = Totals::default();
        for seed in first..first + seeds {
            let mut group: Cluster<New> = Cluster::new(5, &[1, 2, 3], Settings::focal(), seed);
            group.stop_who_left = true;
            let run = judged(group, seed, steps, &mix, None);
            assert!(
                run.violation.is_none(),
                "{rate}, seed {seed}: {}",
                run.violation.unwrap()
            );
            assert!(run.settled, "{rate}, seed {seed}: the group did not settle");
            assert!(
                run.hot.is_none(),
                "{rate}, seed {seed}: {}",
                run.hot.unwrap_or_default()
            );
            agreed(seed, &run.events, &mut totals);
        }
        println!(
            "{rate} in a hundred lost and repeated: {} histories of {} events agreed, {} configurations searched; {}",
            totals.histories,
            totals.operations,
            totals.configurations,
            totals.tails()
        );
    }
}

/// `tests/fast.rs`'s schedules of the fast track, judged.
#[test]
fn the_fast_schedules_keep_every_oracle_and_their_histories_are_linearizable() {
    let seeds = count("HYPER_RAFT_SEEDS", 96);
    let steps = count("HYPER_RAFT_STEPS", 4_000);
    let first = count("HYPER_RAFT_SEED", 0);
    let mix = Mix {
        leader_leaves: true,
        fast: 60,
        ..Mix::everything()
    };
    let mut counters = Counters::new(PATHS);
    let mut totals = Totals::default();
    for seed in first..first + seeds {
        let group: Cluster<New> = Cluster::new(5, &[1, 2, 3, 4, 5], Settings::fast(), seed);
        let run = judged(group, seed, steps, &mix, None);
        assert!(
            run.violation.is_none(),
            "seed {seed}: {}",
            run.violation.unwrap()
        );
        assert!(run.settled, "seed {seed}: the group did not settle");
        assert!(
            run.hot.is_none(),
            "seed {seed}: {}",
            run.hot.unwrap_or_default()
        );
        agreed(seed, &run.events, &mut totals);
        counters.merge(&run.counters);
    }
    println!(
        "{} histories of {} events agreed, {} configurations searched; {}",
        totals.histories,
        totals.operations,
        totals.configurations,
        totals.tails()
    );
    floors("fast", &counters, seeds, &fast_floors());
}

/// The floors of the fast schedules, from their counts over the default 96 seeds of 4,000 steps
/// (2026-10-04, this test at its commit).
fn fast_floors() -> Vec<Floor> {
    vec![
        seed_floor("terms led", 433, 96),
        seed_floor("entries committed", 31756, 96),
        seed_floor("log entries held", 30375, 96),
        seed_floor("leaderships held to the committed entries", 433, 96),
        seed_floor("messages held to their senders' devices", 238079, 96),
        seed_floor("reads asked", 7725, 96),
        seed_floor("reads recorded", 596, 96),
        seed_floor("read indexes answered", 1028, 96),
        seed_floor("reads served", 252, 96),
        seed_floor("writes published", 663, 96),
        seed_floor("writes answered committed", 663, 96),
        seed_floor("writes failed, another entry at their index", 769, 96),
        seed_floor("operations left unknown by a restart", 1290, 96),
        seed_floor("fast votes cast", 74848, 96),
        seed_floor("indexes a fast quorum chose", 639, 96),
    ]
}

/// `tests/pipeline.rs`'s schedules of members driven ahead of their persistence, judged: the
/// durability oracle holds every message to its sender's disk as the schedule leaves it.
#[test]
fn the_pipelined_schedules_keep_every_oracle_and_their_histories_are_linearizable() {
    let seeds = count("HYPER_RAFT_SEEDS", 24);
    let steps = count("HYPER_RAFT_STEPS", 2_000);
    let first = count("HYPER_RAFT_SEED", 0);
    let mut counters = Counters::new(PATHS);
    let mut totals = Totals::default();
    for (depth, apply_unpersisted) in [(2, false), (3, true)] {
        let settings = Settings {
            depth,
            in_place: true,
            apply_unpersisted,
            ..Settings::focal()
        };
        let mix = Mix {
            leader_leaves: true,
            bursts: true,
            windows: true,
            lag: 35,
            leader_durable: if apply_unpersisted { 10 } else { 100 },
            ..Mix::everything()
        };
        for seed in first..first + seeds {
            let mut group: Cluster<Lagged> = Cluster::new(5, &[1, 2, 3], settings, seed);
            group.stop_who_left = true;
            let run = judged(group, seed, steps, &mix, None);
            assert!(
                run.violation.is_none(),
                "seed {seed}: {}",
                run.violation.unwrap()
            );
            assert!(run.settled, "seed {seed}: the group did not settle");
            assert!(
                run.hot.is_none(),
                "seed {seed}: {}",
                run.hot.unwrap_or_default()
            );
            agreed(seed, &run.events, &mut totals);
            counters.merge(&run.counters);
        }
    }
    println!(
        "{} histories of {} events agreed, {} configurations searched; {}",
        totals.histories,
        totals.operations,
        totals.configurations,
        totals.tails()
    );
    floors("pipelined", &counters, 2 * seeds, &pipelined_floors());
}

/// The floors of the pipelined schedules, from their counts over the default 2 × 24 seeds of 2,000
/// steps (2026-10-04, this test at its commit).
fn pipelined_floors() -> Vec<Floor> {
    vec![
        seed_floor("terms led", 160, 48),
        seed_floor("entries committed", 6017, 48),
        seed_floor("log entries held", 5655, 48),
        seed_floor("leaderships held to the committed entries", 160, 48),
        seed_floor("messages held to their senders' devices", 16722, 48),
        seed_floor("leaders' commits held to their voters' devices", 477, 48),
        seed_floor("entries applied held to their members' devices", 6017, 48),
        seed_floor("reads asked", 3649, 48),
        seed_floor("reads recorded", 313, 48),
        seed_floor("read indexes answered", 730, 48),
        seed_floor("reads served", 159, 48),
        seed_floor("writes published", 346, 48),
        seed_floor("writes answered committed", 346, 48),
        seed_floor("writes failed, another entry at their index", 68, 48),
        campaign_floor("writes failed, their term ended past the commit", 29, 48),
        seed_floor("operations left unknown by a restart", 391, 48),
    ]
}

/// The first violation a planted defect brings, over the schedules of `seeds` from `first`: the
/// seed and what caught it.
fn caught<R: Core>(
    make: impl Fn(u64) -> Cluster<R>,
    first: u64,
    seeds: u64,
    steps: u64,
    mix: &Mix,
    mutant: Mutant,
) -> Option<(u64, Violation)> {
    // A campaign that looks for more seeds that catch it sets where to start and how many.
    let first = count("HYPER_RAFT_MUTANT_SEED", first);
    let seeds = count("HYPER_RAFT_MUTANT_SEEDS", seeds);
    for seed in first..first + seeds {
        let run = judged(make(seed), seed, steps, mix, Some(mutant));
        if let Some(violation) = run.violation {
            return Some((seed, violation));
        }
    }
    None
}

fn judged_settings(settings: Settings) -> Settings {
    Settings {
        judged: true,
        ..settings
    }
}

/// `group`'s messages delivered in order, those `allow` refuses lost, until none is left; each
/// operation judged.
fn route(
    group: &mut Cluster<New>,
    judge: &mut Judge,
    allow: impl Fn(&Cluster<New>, &Message) -> bool,
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
fn elect(group: &mut Cluster<New>, judge: &mut Judge, id: u64, among: &[u64]) {
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
fn replicate(
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
fn between(members: &[u64], message: &Message) -> bool {
    members.contains(&message.from) && members.contains(&message.to)
}

/// The thesis's Figure 3.7, played by three members: member 1 leads, and takes an entry at index 2
/// that reaches no one; member 3 leads by member 2 and takes index 2 itself, which reaches no one;
/// member 1 leads again by member 2, and gives member 2 its old entry at 2, one entry an append and
/// nothing of its own term, so a quorum holds the old entry; then member 3 leads again by member 2,
/// its log of a later last term than member 2's. The first violation the judge found.
fn figure_eight(mutant: Option<Mutant>) -> Option<Violation> {
    let settings = Settings {
        pre_vote: false,
        check_quorum: false,
        max_size_per_msg: 1,
        // A follower keeps nothing that arrives ahead of a hole, or member 2 would take in member
        // 1's entry of its term with its old one (R17's `Ahead::Kept`).
        refuse_ahead: true,
        judged: true,
        ..Settings::focal()
    };
    let all = [1, 2, 3];
    let mut group: Cluster<New> = Cluster::new(3, &all, settings, 0);
    group.plant(mutant);
    let mut judge = Judge::new(false, false, 1_000);
    elect(&mut group, &mut judge, 1, &all);
    replicate(&mut group, &mut judge, 1, |_, _| true);
    group.act_observed(&mut judge, &Op::Propose(1, b"of 1".to_vec()));
    route(&mut group, &mut judge, |_, _| false);
    group.stop(1);
    elect(&mut group, &mut judge, 3, &[2, 3]);
    group.act_observed(&mut judge, &Op::Propose(3, b"of 3".to_vec()));
    route(&mut group, &mut judge, |_, _| false);
    group.stop(3);
    group.act_observed(&mut judge, &Op::Restart(1));
    elect(&mut group, &mut judge, 1, &[1, 2]);
    // Between 1 and 2, every message but an append past index 2 to member 2 once it holds index
    // 2, which it would take: while it does not, it refuses one and is probed back to index 2.
    replicate(&mut group, &mut judge, 1, |group, m| {
        let past = m.entries.iter().any(|entry| entry.index > 2);
        between(&[1, 2], m) && !(past && group.disk(m.to).last_index() >= 2)
    });
    group.stop(1);
    group.act_observed(&mut judge, &Op::Restart(3));
    elect(&mut group, &mut judge, 3, &[2, 3]);
    route(&mut group, &mut judge, |_, m| m.from != 1 && m.to != 1);
    judge.violation
}

/// A leader that commits by counting an older term's replicas (thesis §3.6.2, Figure 3.7) commits
/// an entry a later leader replaces. Played as the figure plays it: with the rule taken out, Leader
/// Completeness catches the later leader lacking the entry committed; with it in, nothing is
/// committed that a later leader lacks. 2,096 random schedules of the group test (seeds 0 to 2,095)
/// never reached the figure's interleaving (2026-10-04), so it is played.
#[test]
fn a_commit_counted_from_an_older_term_is_caught() {
    assert_eq!(figure_eight(None), None);
    let violation = figure_eight(Some(Mutant::OlderTermCommit))
        .expect("the commit of an older term's entry went uncaught");
    println!("older-term commit: Figure 3.7: {violation}");
    assert!(
        matches!(
            violation,
            Violation::LeaderLacks { index: 2, .. } | Violation::TwoCommitted { index: 2, .. }
        ),
        "{violation}"
    );
}

/// A new leader that answers a read before it commits an entry of its term answers it below a
/// commit already made: read safety catches it.
#[test]
fn a_read_before_the_terms_first_commit_is_caught() {
    let mix = Mix {
        leader_leaves: true,
        bursts: true,
        windows: true,
        ..Mix::everything()
    };
    let make = |seed| {
        let mut group: Cluster<New> =
            Cluster::new(5, &[1, 2, 3], judged_settings(Settings::focal()), seed);
        group.stop_who_left = true;
        group
    };
    let (seed, violation) = caught(make, 0, 96, 4_000, &mix, Mutant::ReadBeforeFirstCommit)
        .expect("no schedule caught a read before the term's first commit");
    println!("read before the first commit: seed {seed}: {violation}");
    assert!(
        matches!(violation, Violation::StaleRead { .. }),
        "seed {seed}: {violation}"
    );
}

/// A vote sent before it is durable: the durability oracle catches it (I1).
#[test]
fn a_vote_sent_before_it_is_durable_is_caught() {
    let mix = Mix {
        leader_leaves: true,
        bursts: true,
        windows: true,
        lag: 35,
        ..Mix::everything()
    };
    let settings = judged_settings(Settings {
        depth: 2,
        in_place: true,
        ..Settings::focal()
    });
    let make = |seed| {
        let mut group: Cluster<Lagged> = Cluster::new(5, &[1, 2, 3], settings, seed);
        group.stop_who_left = true;
        group
    };
    let (seed, violation) = caught(make, 0, 24, 2_000, &mix, Mutant::VoteBeforeDurable)
        .expect("no schedule caught a vote sent before it was durable");
    println!("vote before durable: seed {seed}: {violation}");
    assert!(
        matches!(violation, Violation::Durability { rule: Rule::I1, .. }),
        "seed {seed}: {violation}"
    );
}

/// The fast track without its first rule (`docs/raft.md` §3): a member holding the entry beside its
/// log counted before its log holds an entry of the leader's term. Its original seed, 9843, no
/// longer reaches the defect on today's core (the reads' rounds and R17 moved its schedule); a
/// campaign from seed 0 found seed 47,818 first (2026-10-04, 200,000 seeds asked, on the core with
/// both fixes of `docs/sim.md` §14.5): a later leader lacks an entry the fast track committed. The
/// harness's own checks pass that run, which ends before a second entry is committed at the index;
/// the same seed without the defect keeps every oracle, so the catch is the defect's.
#[test]
fn the_fast_track_without_its_first_rule_is_caught() {
    let mix = Mix {
        leader_leaves: true,
        fast: 60,
        ..Mix::everything()
    };
    // The rules its original seed was found under (`tests/fast.rs`).
    let settings = Settings {
        round_each: true,
        max_inflight_bytes: u64::MAX,
        bare_answers: true,
        refuse_ahead: true,
        ..Settings::fast()
    };
    let make = |seed| Cluster::<New>::new(5, &[1, 2, 3, 4, 5], judged_settings(settings), seed);
    assert_eq!(
        judged(make(47_818), 47_818, 4_000, &mix, None).violation,
        None
    );
    let (seed, violation) = caught(make, 47_818, 1, 4_000, &mix, Mutant::FastBesideAnyTerm)
        .expect("seed 47,818 did not catch the fast track without its first rule");
    println!("fast track without its first rule: seed {seed}: {violation}");
    assert!(
        matches!(violation, Violation::LeaderLacks { .. }),
        "{violation}"
    );
}

/// The fast track without its second rule: a fast quorum counted of the configuration in force
/// alone. Its original seed, 54104, no longer reaches the defect; a campaign from seed 0 found seed
/// 121,040 first (2026-10-04, on the core with both fixes of `docs/sim.md` §14.5 and the final
/// oracles): a later leader lacks an entry the fast track committed. The same seed without the
/// defect keeps every oracle, so the catch is the defect's.
#[test]
fn the_fast_track_without_its_second_rule_is_caught() {
    let mix = Mix {
        leader_leaves: true,
        fast: 60,
        ..Mix::everything()
    };
    let settings = Settings::fast();
    let make = |seed| Cluster::<New>::new(5, &[1, 2, 3, 4, 5], judged_settings(settings), seed);
    assert_eq!(
        judged(make(121_040), 121_040, 4_000, &mix, None).violation,
        None
    );
    let (seed, violation) = caught(make, 121_040, 1, 4_000, &mix, Mutant::FastAnyConfiguration)
        .expect("seed 121,040 did not catch the fast track without its second rule");
    println!("fast track without its second rule: seed {seed}: {violation}");
    assert!(
        matches!(violation, Violation::LeaderLacks { .. }),
        "{violation}"
    );
}

/// A fast-track leader serves reads only once it commits the blank entry it began its term with
/// (`Raft::commit_to_current_term`). The judge found the cause at seed 15,761 of the fast schedules
/// (2026-10-04): index 9 committed by a fast quorum in term 1; member 4, elected in term 2 at a
/// commit of 2, recovered indexes 3 to 9 under its own term and appended its blank entry after them;
/// a read asked of it was served once index 5 committed, an entry of its term, and so answered
/// below the commit of 9 made before it was asked. Read safety catches the schedule if the rule is
/// an entry of the term rather than the term's blank entry.
#[test]
fn a_fast_leaders_reads_wait_for_the_entry_it_began_its_term_with() {
    let mix = Mix {
        leader_leaves: true,
        fast: 60,
        ..Mix::everything()
    };
    let group = Cluster::<New>::new(
        5,
        &[1, 2, 3, 4, 5],
        judged_settings(Settings::fast()),
        15_761,
    );
    let run = judged(group, 15_761, 4_000, &mix, None);
    assert_eq!(run.violation, None);
}
