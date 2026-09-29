//! Running a bucket's create or delete across its ranges (docs/design/metadata.md §2).
//!
//! Every transaction runs inside one range, so creating or deleting a bucket is a sequence of
//! range steps, each guarded by what the one before it wrote. A [`Coordinator`] holds where one
//! attempt is in that sequence and names its next [`Request`]: a command to the Bucket range,
//! or a read or a command to one of the Name ranges the bucket's keys fall in, in key order.
//! It takes the [`Answer`] and moves on. It does no I/O itself: the gateway, or a simulation,
//! sends each request and hands back what it answered.
//!
//! Each gate step names the gate's state as the coordinator read it and the attempt the
//! Bucket range issued, and a range refuses a step older than the attempt that last moved the
//! gate. So an attempt that finds a later one has taken over stops, and leaves the rest to it,
//! and a coordinator that stalls and resumes changes nothing a later attempt has done.
//!
//! Each step at a Name range is routed by a descriptor the attempt holds, and names its
//! generation; a range that has split since refuses it with its lineage
//! (docs/design/metadata.md §3). The attempt learns the descriptors and starts its phase
//! again over the ranges it now knows, as docs/models/RangeSplit.tla's attempts do: a create
//! opens every gate again, a delete closes and reads every range again, and a cleanup
//! condemns every gate again. When what it knows no longer covers the bucket's keys, it
//! reads the directory.

use std::cmp::Ordering;

use crate::bucket;
use crate::key;
use crate::name::{self, Collect, GateChange, Probe, Routed};
use crate::record::{Bucket, BucketState, Descriptor, Gate, GateState, Lineage};

/// A coordinator's next request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// A command to the Bucket range.
    Bucket(bucket::Command),
    /// A command to Name range `range`, boxed since a Name-range command is the largest
    /// request by far.
    Name {
        range: u64,
        command: Box<name::Command>,
    },
    /// A linearizable read of Name range `range`'s gate for the bucket ([`name::read_gate`]).
    Gate { range: u64, generation: u64 },
    /// A linearizable read of Name range `range` for a version or delete marker of the bucket,
    /// from `from` and passing at most `budget` rows ([`name::probe`]).
    Probe {
        range: u64,
        generation: u64,
        from: Option<Vec<u8>>,
        budget: usize,
    },
    /// A read of the Name ranges' descriptors the directory holds.
    Directory,
}

/// What a request answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Bucket(bucket::Outcome),
    Name(name::Outcome),
    Gate(Routed<Option<Gate>>),
    Probe(Routed<Probe>),
    Directory(Vec<Descriptor>),
}

/// What the request that started an attempt learns, once it can be told.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settled {
    /// Every gate opened and the bucket is active: CreateBucket succeeded.
    Created,
    /// No Name range held a version: DeleteBucket succeeded, and the attempt goes on to
    /// remove the bucket's uploads and gates.
    Deleted,
    /// A Name range held a version and the gates reopened: `409 BucketNotEmpty`.
    NotEmpty,
    /// A later attempt moved a row or gate first, and carries the bucket on.
    Superseded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CoordinatorError {
    #[error("an answer of another kind than the request")]
    Mismatch,
    #[error("an answer the coordinator's step cannot have")]
    Unexpected,
    #[error("descriptors that do not cover the bucket's keys")]
    NoRanges,
}

/// A Name range a step goes to: its place among the ranges the attempt knows, and the
/// descriptor it is routed by.
#[derive(Debug, Clone, PartialEq, Eq)]
struct At {
    index: usize,
    range: Descriptor,
}

/// Where an attempt is.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Phase {
    /// Reading a range's gate before opening it.
    ReadOpen(At),
    Open(At),
    Activate,
    /// Reading a range's gate before closing it.
    ReadClose(At),
    Close(At, Option<GateState>),
    Probe(At, Option<Vec<u8>>),
    Reopen(At),
    Restore,
    Finish,
    Condemn(At),
    Sweep(At),
    Drop(At),
    Forget,
    /// Reading the directory, then starting the phase again.
    Directory(Restart),
    Done,
}

/// The phase an attempt starts again when a range has moved on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Restart {
    Open,
    Close,
    Cleanup,
}

/// One attempt at creating or deleting a bucket.
#[derive(Debug, Clone)]
pub struct Coordinator {
    bucket: String,
    incarnation: u64,
    attempt: u64,
    /// The descriptors of the Name ranges the bucket's keys fall in, as far as the attempt
    /// knows, in key order.
    known: Vec<Descriptor>,
    /// Rows one read or one collection may pass or remove, which bounds an entry's size.
    budget: u32,
    phase: Phase,
    settled: Option<Settled>,
}

impl Coordinator {
    /// The attempt a Bucket-range command started: a create its `Create` answered with
    /// `Creating`, or a delete its `BeginDelete` or `Abandon` answered with `Deleting`, over
    /// the Name ranges `directory` describes. `None` for any other answer, which the request
    /// answers itself.
    pub fn start(
        bucket: &str,
        outcome: &bucket::Outcome,
        directory: &[Descriptor],
        budget: u32,
    ) -> Result<Option<Self>, CoordinatorError> {
        let (incarnation, attempt, restart) = match *outcome {
            bucket::Outcome::Creating {
                incarnation,
                attempt,
            } => (incarnation, attempt, Restart::Open),
            bucket::Outcome::Deleting {
                incarnation,
                attempt,
            } => (incarnation, attempt, Restart::Close),
            _ => return Ok(None),
        };
        Self::new(bucket, incarnation, attempt, directory, budget, restart).map(Some)
    }

    /// The collector's resumption of a deleted bucket's cleanup, from the Bucket range's row:
    /// condemn the gates, remove the uploads, drop the gates, forget the name. Every step is
    /// done again, and a gate the cleanup already dropped reads as condemned and swept, so it
    /// resumes from wherever it stopped. `None` unless the row is deleted.
    pub fn resume(
        bucket: &str,
        row: &Bucket,
        directory: &[Descriptor],
        budget: u32,
    ) -> Result<Option<Self>, CoordinatorError> {
        if row.state != BucketState::Deleted {
            return Ok(None);
        }
        Self::new(
            bucket,
            row.created_ns,
            row.attempt,
            directory,
            budget,
            Restart::Cleanup,
        )
        .map(Some)
    }

    fn new(
        bucket: &str,
        incarnation: u64,
        attempt: u64,
        directory: &[Descriptor],
        budget: u32,
        restart: Restart,
    ) -> Result<Self, CoordinatorError> {
        let mut c = Self {
            bucket: bucket.to_owned(),
            incarnation,
            attempt,
            known: Vec::new(),
            budget: budget.max(1),
            phase: Phase::Done,
            settled: None,
        };
        c.phase = c.learn_directory(directory, restart)?;
        Ok(c)
    }

    /// What the attempt's request learns, once the attempt has reached it.
    pub fn settled(&self) -> Option<Settled> {
        self.settled
    }

    /// The Bucket-range command that records the attempt's progress at `at_ns`: its driver
    /// sends it while the attempt works through the Name ranges, so the collector leaves the
    /// attempt alone (collector.rs).
    pub fn progress(&self, at_ns: u64) -> bucket::Command {
        bucket::Command::Progress {
            bucket: self.bucket.clone(),
            attempt: self.attempt,
            at_ns,
        }
    }

    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    /// The request to send next; `None` once the attempt is done.
    pub fn next(&self) -> Option<Request> {
        use GateState::{Closed, Condemned, Open};
        let bucket = self.bucket.clone();
        Some(match &self.phase {
            Phase::ReadOpen(at) | Phase::ReadClose(at) => Request::Gate {
                range: at.range.id,
                generation: at.range.generation,
            },
            Phase::Open(at) => self.gate(at, None, Some(Open)),
            Phase::Activate => Request::Bucket(bucket::Command::Activate {
                bucket,
                attempt: self.attempt,
            }),
            Phase::Close(at, from) => self.gate(at, *from, Some(Closed)),
            Phase::Probe(at, from) => Request::Probe {
                range: at.range.id,
                generation: at.range.generation,
                from: from.clone(),
                budget: usize::try_from(self.budget).unwrap_or(usize::MAX),
            },
            Phase::Reopen(at) => self.gate(at, Some(Closed), Some(Open)),
            Phase::Restore => Request::Bucket(bucket::Command::Restore {
                bucket,
                attempt: self.attempt,
            }),
            Phase::Finish => Request::Bucket(bucket::Command::Delete {
                bucket,
                attempt: self.attempt,
            }),
            Phase::Condemn(at) => self.gate(at, Some(Closed), Some(Condemned)),
            Phase::Sweep(at) => Request::Name {
                range: at.range.id,
                command: Box::new(name::Command::Collect(Collect {
                    bucket,
                    incarnation: self.incarnation,
                    budget: self.budget,
                    // The entry that carries it gives it its time (wire.rs).
                    at_ns: 0,
                    generation: at.range.generation,
                })),
            },
            Phase::Drop(at) => self.gate(at, Some(Condemned), None),
            Phase::Forget => Request::Bucket(bucket::Command::Forget {
                bucket,
                attempt: self.attempt,
            }),
            Phase::Directory(_) => Request::Directory,
            Phase::Done => return None,
        })
    }

    /// Takes the answer to the request [`next`](Self::next) named, and moves on. An answer
    /// that does not fit the request is refused, and the coordinator stays where it was.
    pub fn answer(&mut self, answer: Answer) -> Result<(), CoordinatorError> {
        self.phase = self.after(self.phase.clone(), answer)?;
        Ok(())
    }

    /// The phase after `phase` is answered with `answer`.
    fn after(&mut self, phase: Phase, answer: Answer) -> Result<Phase, CoordinatorError> {
        use name::Outcome::{Collected, Conflict, GateMoved, Moved};
        Ok(match (phase, answer) {
            (Phase::ReadOpen(at), Answer::Gate(Routed::Moved(lineage)))
            | (Phase::Open(at), Answer::Name(Moved(lineage))) => {
                self.relearn(&at, *lineage, Restart::Open)?
            }
            (Phase::ReadOpen(at), Answer::Gate(Routed::Here(gate))) => match gate {
                None => Phase::Open(at),
                // Opened by the attempt this one took over.
                Some(g) if g.incarnation == self.incarnation && g.state == GateState::Open => {
                    self.then(at, Phase::ReadOpen, Phase::Activate)
                }
                Some(_) => self.superseded(),
            },
            (Phase::Open(at), Answer::Name(outcome)) => {
                if outcome == GateMoved {
                    self.then(at, Phase::ReadOpen, Phase::Activate)
                } else {
                    self.superseded()
                }
            }
            (Phase::Activate, Answer::Bucket(outcome)) => {
                self.settle(if outcome == bucket::Outcome::Activated {
                    Settled::Created
                } else {
                    Settled::Superseded
                })
            }
            (Phase::ReadClose(at), Answer::Gate(Routed::Moved(lineage)))
            | (Phase::Close(at, _) | Phase::Reopen(at), Answer::Name(Moved(lineage)))
            | (Phase::Probe(at, _), Answer::Probe(Routed::Moved(lineage))) => {
                self.relearn(&at, *lineage, Restart::Close)?
            }
            (Phase::ReadClose(at), Answer::Gate(Routed::Here(gate))) => match gate {
                None => Phase::Close(at, None),
                Some(g) if g.incarnation == self.incarnation && g.state != GateState::Condemned => {
                    Phase::Close(at, Some(g.state))
                }
                Some(_) => self.superseded(),
            },
            (Phase::Close(at, _), Answer::Name(outcome)) => {
                if outcome == GateMoved {
                    let probe = Phase::Probe(self.first()?, None);
                    self.then(at, Phase::ReadClose, probe)
                } else {
                    self.superseded()
                }
            }
            (Phase::Probe(at, _), Answer::Probe(Routed::Here(probe))) => match probe {
                Probe::Found => Phase::Reopen(self.first()?),
                Probe::Clear => self.then(at, |a| Phase::Probe(a, None), Phase::Finish),
                Probe::Paused(from) => Phase::Probe(at, Some(from)),
            },
            (Phase::Reopen(at), Answer::Name(outcome)) => {
                if outcome == GateMoved {
                    self.then(at, Phase::Reopen, Phase::Restore)
                } else {
                    self.superseded()
                }
            }
            (Phase::Restore, Answer::Bucket(outcome)) => {
                self.settle(if outcome == bucket::Outcome::Restored {
                    Settled::NotEmpty
                } else {
                    Settled::Superseded
                })
            }
            (Phase::Finish, Answer::Bucket(outcome)) => {
                if outcome == bucket::Outcome::Deleted {
                    self.settled = Some(Settled::Deleted);
                    Phase::Condemn(self.first()?)
                } else {
                    self.superseded()
                }
            }
            (
                Phase::Condemn(at) | Phase::Sweep(at) | Phase::Drop(at),
                Answer::Name(Moved(lineage)),
            ) => self.relearn(&at, *lineage, Restart::Cleanup)?,
            (Phase::Condemn(at), Answer::Name(outcome)) => {
                if outcome == GateMoved {
                    let sweep = Phase::Sweep(self.first()?);
                    self.then(at, Phase::Condemn, sweep)
                } else {
                    self.superseded()
                }
            }
            (Phase::Sweep(at), Answer::Name(outcome)) => match outcome {
                Collected { done: false } => Phase::Sweep(at),
                Collected { done: true } => {
                    let drop = Phase::Drop(self.first()?);
                    self.then(at, Phase::Sweep, drop)
                }
                // The gate is another incarnation's: the name was forgotten and taken again.
                Conflict => self.superseded(),
                _ => return Err(CoordinatorError::Unexpected),
            },
            (Phase::Drop(at), Answer::Name(outcome)) => {
                if outcome == GateMoved {
                    self.then(at, Phase::Drop, Phase::Forget)
                } else {
                    self.superseded()
                }
            }
            (Phase::Forget, Answer::Bucket(_)) => Phase::Done,
            (Phase::Directory(restart), Answer::Directory(directory)) => {
                self.learn_directory(&directory, restart)?
            }
            (Phase::Done, _) => return Err(CoordinatorError::Unexpected),
            _ => return Err(CoordinatorError::Mismatch),
        })
    }

    /// The same step at the next range, or `last` after the last range.
    fn then(&self, at: At, same: impl FnOnce(At) -> Phase, last: Phase) -> Phase {
        let next = at.index.checked_add(1).and_then(|index| {
            let range = self.known.get(index)?.clone();
            Some(At { index, range })
        });
        match next {
            Some(next) => same(next),
            None => last,
        }
    }

    /// The step at the first range the attempt knows.
    fn first(&self) -> Result<At, CoordinatorError> {
        let range = self
            .known
            .first()
            .ok_or(CoordinatorError::NoRanges)?
            .clone();
        Ok(At { index: 0, range })
    }

    fn restart(&self, restart: Restart) -> Result<Phase, CoordinatorError> {
        let first = self.first()?;
        Ok(match restart {
            Restart::Open => Phase::ReadOpen(first),
            Restart::Close => Phase::ReadClose(first),
            Restart::Cleanup => Phase::Condemn(first),
        })
    }

    /// The range `at` names answered with its lineage: the attempt learns where its span went
    /// and starts the phase again, reading the directory first if what it knows no longer
    /// covers the bucket's keys.
    fn relearn(
        &mut self,
        at: &At,
        lineage: Lineage,
        restart: Restart,
    ) -> Result<Phase, CoordinatorError> {
        self.known.retain(|d| d.id != at.range.id);
        for d in [Some(lineage.now), lineage.child].into_iter().flatten() {
            self.learn(d);
        }
        if self.covers() {
            self.restart(restart)
        } else {
            Ok(Phase::Directory(restart))
        }
    }

    /// Takes the directory's descriptors as all the attempt knows, and starts `restart`.
    fn learn_directory(
        &mut self,
        directory: &[Descriptor],
        restart: Restart,
    ) -> Result<Phase, CoordinatorError> {
        self.known.clear();
        for d in directory {
            self.learn(d.clone());
        }
        if !self.covers() {
            return Err(CoordinatorError::NoRanges);
        }
        self.restart(restart)
    }

    /// Adds `d` to what the attempt knows if its span meets the bucket's keys and it is newer
    /// than what the attempt holds for its range.
    fn learn(&mut self, d: Descriptor) {
        let (first, past) = key::bucket_routes(&self.bucket);
        if !d.meets(&first, &past) {
            return;
        }
        match self.known.iter_mut().find(|k| k.id == d.id) {
            Some(held) if held.generation >= d.generation => {}
            Some(held) => *held = d,
            None => self.known.push(d),
        }
        self.known.sort_by(|a, b| a.lo.cmp(&b.lo));
    }

    /// Whether the spans the attempt knows cover every routing key of the bucket.
    fn covers(&self) -> bool {
        let (mut at, past) = key::bucket_routes(&self.bucket);
        // Each pass moves `at` to the end of a span that holds it, past where it was, so the
        // walk ends within a pass a span.
        for _ in 0..=self.known.len() {
            let reach = self
                .known
                .iter()
                .filter(|d| d.holds(&at))
                .map(|d| d.hi.as_deref())
                .max_by(|a, b| end_order(*a, *b));
            match reach {
                None => return false,
                Some(None) => return true,
                Some(Some(hi)) if hi >= past.as_slice() => return true,
                Some(Some(hi)) => at = hi.to_vec(),
            }
        }
        false
    }

    fn settle(&mut self, settled: Settled) -> Phase {
        self.settled = Some(settled);
        Phase::Done
    }

    /// A later attempt moved a row or gate first: this one stops, and if its request was not
    /// answered yet, it learns that.
    fn superseded(&mut self) -> Phase {
        self.settled.get_or_insert(Settled::Superseded);
        Phase::Done
    }

    fn gate(&self, at: &At, from: Option<GateState>, to: Option<GateState>) -> Request {
        Request::Name {
            range: at.range.id,
            command: Box::new(name::Command::Gate(GateChange {
                bucket: self.bucket.clone(),
                incarnation: self.incarnation,
                attempt: self.attempt,
                from,
                to,
                generation: at.range.generation,
            })),
        }
    }
}

/// Orders the ends of spans, `None` past every key.
fn end_order(a: Option<&[u8]>, b: Option<&[u8]>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(a), Some(b)) => a.cmp(b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate(incarnation: u64, attempt: u64, state: GateState) -> Routed<Option<Gate>> {
        Routed::Here(Some(Gate {
            incarnation,
            attempt,
            state,
        }))
    }

    fn range(id: u64, lo: Option<&str>, hi: Option<&str>, generation: u64) -> Descriptor {
        Descriptor {
            id,
            lo: lo.map(|k| key::route("b", k)).unwrap_or_default(),
            hi: hi.map(|k| key::route("b", k)),
            generation,
        }
    }

    fn whole() -> Vec<Descriptor> {
        vec![range(1, None, None, 1)]
    }

    fn moved(now: Descriptor, child: Option<Descriptor>) -> Box<Lineage> {
        Box::new(Lineage { now, child })
    }

    /// The range and generation a request is routed by.
    fn routed(request: Option<Request>) -> (u64, u64) {
        match request {
            Some(
                Request::Gate { range, generation }
                | Request::Probe {
                    range, generation, ..
                },
            ) => (range, generation),
            Some(Request::Name { range, command }) => match *command {
                name::Command::Gate(g) => (range, g.generation),
                name::Command::Collect(c) => (range, c.generation),
                other => panic!("a gate change or a collection, not {other:?}"),
            },
            other => panic!("a Name-range request, not {other:?}"),
        }
    }

    /// A create reads and opens each range's gate, then activates the bucket.
    #[test]
    fn a_create_opens_every_gate_then_activates() {
        let creating = bucket::Outcome::Creating {
            incarnation: 7,
            attempt: 7,
        };
        let directory = [range(2, Some("m"), None, 2), range(1, None, Some("m"), 2)];
        let mut c = Coordinator::start("b", &creating, &directory, 8)
            .unwrap()
            .unwrap();
        for id in [1, 2] {
            assert_eq!(
                c.next(),
                Some(Request::Gate {
                    range: id,
                    generation: 2
                })
            );
            c.answer(Answer::Gate(Routed::Here(None))).unwrap();
            let Some(Request::Name { range: r, command }) = c.next() else {
                panic!("a gate change");
            };
            assert_eq!(r, id);
            assert!(matches!(
                *command,
                name::Command::Gate(GateChange {
                    from: None,
                    to: Some(GateState::Open),
                    attempt: 7,
                    generation: 2,
                    ..
                })
            ));
            c.answer(Answer::Name(name::Outcome::GateMoved)).unwrap();
        }
        assert!(matches!(
            c.next(),
            Some(Request::Bucket(bucket::Command::Activate {
                attempt: 7,
                ..
            }))
        ));
        c.answer(Answer::Bucket(bucket::Outcome::Activated))
            .unwrap();
        assert_eq!(c.settled(), Some(Settled::Created));
        assert!(c.is_done() && c.next().is_none());
    }

    /// A delete that finds a version reopens the gates and answers BucketNotEmpty; one whose
    /// gate a later attempt moved stops there.
    #[test]
    fn a_delete_restores_a_bucket_that_holds_a_version() {
        let deleting = bucket::Outcome::Deleting {
            incarnation: 7,
            attempt: 9,
        };
        let mut c = Coordinator::start("b", &deleting, &whole(), 8)
            .unwrap()
            .unwrap();
        c.answer(Answer::Gate(gate(7, 7, GateState::Open))).unwrap();
        c.answer(Answer::Name(name::Outcome::GateMoved)).unwrap();
        assert!(matches!(c.next(), Some(Request::Probe { range: 1, .. })));
        c.answer(Answer::Probe(Routed::Here(Probe::Found))).unwrap();
        c.answer(Answer::Name(name::Outcome::GateMoved)).unwrap();
        c.answer(Answer::Bucket(bucket::Outcome::Restored)).unwrap();
        assert_eq!(c.settled(), Some(Settled::NotEmpty));

        let mut late = Coordinator::start("b", &deleting, &whole(), 8)
            .unwrap()
            .unwrap();
        late.answer(Answer::Gate(gate(7, 9, GateState::Closed)))
            .unwrap();
        late.answer(Answer::Name(name::Outcome::Conflict)).unwrap();
        assert_eq!(late.settled(), Some(Settled::Superseded));
        assert!(late.is_done());
    }

    /// A delete that finds nothing is settled once the Bucket range deletes the bucket, and
    /// goes on through the uploads and gates to forgetting the name.
    #[test]
    fn a_delete_of_an_empty_bucket_runs_to_forgetting_it() {
        let deleting = bucket::Outcome::Deleting {
            incarnation: 7,
            attempt: 9,
        };
        let mut c = Coordinator::start("b", &deleting, &whole(), 1)
            .unwrap()
            .unwrap();
        c.answer(Answer::Gate(Routed::Here(None))).unwrap();
        c.answer(Answer::Name(name::Outcome::GateMoved)).unwrap();
        c.answer(Answer::Probe(Routed::Here(Probe::Paused(vec![1]))))
            .unwrap();
        assert!(matches!(
            c.next(),
            Some(Request::Probe { from: Some(ref k), budget: 1, .. }) if k == &[1]
        ));
        c.answer(Answer::Probe(Routed::Here(Probe::Clear))).unwrap();
        c.answer(Answer::Bucket(bucket::Outcome::Deleted)).unwrap();
        assert_eq!(c.settled(), Some(Settled::Deleted));
        c.answer(Answer::Name(name::Outcome::GateMoved)).unwrap(); // condemned
        c.answer(Answer::Name(name::Outcome::Collected { done: false }))
            .unwrap();
        c.answer(Answer::Name(name::Outcome::Collected { done: true }))
            .unwrap();
        c.answer(Answer::Name(name::Outcome::GateMoved)).unwrap(); // dropped
        assert!(matches!(
            c.next(),
            Some(Request::Bucket(bucket::Command::Forget { attempt: 9, .. }))
        ));
        c.answer(Answer::Bucket(bucket::Outcome::Forgotten))
            .unwrap();
        assert!(c.is_done());
        assert_eq!(c.settled(), Some(Settled::Deleted));
    }

    /// A range that split since the attempt read the directory refuses its step with its
    /// lineage, and the attempt starts its phase again over both halves: a delete that had
    /// closed and was reading closes again, since the child may hold what the read missed.
    #[test]
    fn a_step_refused_by_a_split_range_starts_the_phase_again_over_its_halves() {
        let deleting = bucket::Outcome::Deleting {
            incarnation: 7,
            attempt: 9,
        };
        let mut c = Coordinator::start("b", &deleting, &whole(), 8)
            .unwrap()
            .unwrap();
        c.answer(Answer::Gate(gate(7, 7, GateState::Open))).unwrap();
        c.answer(Answer::Name(name::Outcome::GateMoved)).unwrap();
        assert_eq!(routed(c.next()), (1, 1));
        let lineage = moved(
            range(1, None, Some("m"), 2),
            Some(range(4, Some("m"), None, 2)),
        );
        c.answer(Answer::Probe(Routed::Moved(lineage))).unwrap();
        for id in [1, 4] {
            assert_eq!(routed(c.next()), (id, 2), "reads the gate");
            c.answer(Answer::Gate(gate(7, 9, GateState::Closed)))
                .unwrap();
            assert_eq!(routed(c.next()), (id, 2), "closes it");
            c.answer(Answer::Name(name::Outcome::GateMoved)).unwrap();
        }
        for id in [1, 4] {
            assert_eq!(routed(c.next()), (id, 2), "reads the range");
            c.answer(Answer::Probe(Routed::Here(Probe::Clear))).unwrap();
        }
        assert!(matches!(
            c.next(),
            Some(Request::Bucket(bucket::Command::Delete { .. }))
        ));
        // A split during the cleanup starts it again from condemning.
        c.answer(Answer::Bucket(bucket::Outcome::Deleted)).unwrap();
        c.answer(Answer::Name(name::Outcome::GateMoved)).unwrap();
        c.answer(Answer::Name(name::Outcome::GateMoved)).unwrap();
        assert_eq!(routed(c.next()), (1, 2), "collects");
        let lineage = moved(
            range(1, None, Some("f"), 3),
            Some(range(5, Some("f"), Some("m"), 3)),
        );
        c.answer(Answer::Name(name::Outcome::Moved(lineage)))
            .unwrap();
        let Some(Request::Name { range: 1, command }) = c.next() else {
            panic!("condemns again");
        };
        assert!(matches!(
            *command,
            name::Command::Gate(GateChange {
                from: Some(GateState::Closed),
                to: Some(GateState::Condemned),
                generation: 3,
                ..
            })
        ));
    }

    /// A lineage that leaves part of the bucket's keys unaccounted for, since the range split
    /// more than once, sends the attempt to the directory; a range whose span holds none of
    /// the bucket's keys is left out.
    #[test]
    fn an_attempt_that_no_longer_covers_the_bucket_reads_the_directory() {
        let creating = bucket::Outcome::Creating {
            incarnation: 7,
            attempt: 7,
        };
        let mut c = Coordinator::start("b", &creating, &whole(), 8)
            .unwrap()
            .unwrap();
        let lineage = moved(
            range(1, None, Some("f"), 3),
            Some(range(5, Some("f"), Some("m"), 3)),
        );
        c.answer(Answer::Gate(Routed::Moved(lineage))).unwrap();
        assert_eq!(c.next(), Some(Request::Directory));
        let below = Descriptor {
            id: 8,
            lo: Vec::new(),
            hi: Some(key::bucket_routes("b").0),
            generation: 4,
        };
        let directory = vec![
            Descriptor {
                lo: key::bucket_routes("b").0,
                ..range(1, None, Some("f"), 4)
            },
            below,
            range(5, Some("f"), Some("m"), 3),
            range(4, Some("m"), None, 2),
        ];
        // An answer of another kind is refused, and the attempt still reads the directory.
        assert_eq!(
            c.answer(Answer::Gate(Routed::Here(None))),
            Err(CoordinatorError::Mismatch)
        );
        c.answer(Answer::Directory(directory)).unwrap();
        for (id, generation) in [(1, 4), (5, 3), (4, 2)] {
            assert_eq!(routed(c.next()), (id, generation));
            c.answer(Answer::Gate(Routed::Here(None))).unwrap();
            c.answer(Answer::Name(name::Outcome::GateMoved)).unwrap();
        }
        assert!(matches!(
            c.next(),
            Some(Request::Bucket(bucket::Command::Activate { .. }))
        ));
        // A directory that leaves keys uncovered is refused.
        let mut d = Coordinator::start("b", &creating, &whole(), 8)
            .unwrap()
            .unwrap();
        d.answer(Answer::Gate(Routed::Moved(moved(
            range(1, None, Some("f"), 3),
            None,
        ))))
        .unwrap();
        assert_eq!(
            d.answer(Answer::Directory(vec![range(1, None, Some("f"), 3)])),
            Err(CoordinatorError::NoRanges)
        );
    }

    #[test]
    fn answers_that_do_not_fit_are_refused() {
        let creating = bucket::Outcome::Creating {
            incarnation: 1,
            attempt: 1,
        };
        let mut c = Coordinator::start("b", &creating, &whole(), 8)
            .unwrap()
            .unwrap();
        assert_eq!(
            c.answer(Answer::Probe(Routed::Here(Probe::Clear))),
            Err(CoordinatorError::Mismatch)
        );
        // Refused, the coordinator is where it was.
        assert_eq!(
            c.next(),
            Some(Request::Gate {
                range: 1,
                generation: 1
            })
        );
        assert_eq!(
            Coordinator::start("b", &creating, &[], 8).err(),
            Some(CoordinatorError::NoRanges)
        );
        assert!(
            Coordinator::start("b", &bucket::Outcome::AlreadyExists, &whole(), 8)
                .unwrap()
                .is_none()
        );
    }
}
