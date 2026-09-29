//! Running a bucket's create or delete across its ranges (docs/design/metadata.md §2).
//!
//! Every transaction runs inside one range, so creating or deleting a bucket is a sequence of
//! range steps, each guarded by what the one before it wrote. A [`Coordinator`] holds where one
//! attempt is in that sequence and names its next [`Request`]: a command to the Bucket range,
//! or a read or a command to one of the Name ranges the bucket's keys fall in, numbered in key
//! order. It takes the [`Answer`] and moves on. It does no I/O itself: the gateway, or a
//! simulation, sends each request and hands back what it answered.
//!
//! Each gate step names the gate's state as the coordinator read it and the attempt the
//! Bucket range issued, and a range refuses a step older than the attempt that last moved the
//! gate. So an attempt that finds a later one has taken over stops, and leaves the rest to it,
//! and a coordinator that stalls and resumes changes nothing a later attempt has done.

use crate::bucket;
use crate::name::{self, Collect, GateChange, Probe};
use crate::record::{Bucket, BucketState, Gate, GateState};

/// A coordinator's next request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// A command to the Bucket range.
    Bucket(bucket::Command),
    /// A command to Name range `range`, boxed since a Name-range command is the largest
    /// request by far.
    Name {
        range: usize,
        command: Box<name::Command>,
    },
    /// A linearizable read of Name range `range`'s gate for the bucket ([`name::gate`]).
    Gate { range: usize },
    /// A linearizable read of Name range `range` for a version or delete marker of the bucket,
    /// from `from` and passing at most `budget` rows ([`name::probe`]).
    Probe {
        range: usize,
        from: Option<Vec<u8>>,
        budget: usize,
    },
}

/// What a request answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Bucket(bucket::Outcome),
    Name(name::Outcome),
    Gate(Option<Gate>),
    Probe(Probe),
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
    #[error("a bucket whose keys fall in no Name range")]
    NoRanges,
}

/// Where an attempt is.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Phase {
    /// Reading a range's gate before opening it.
    ReadOpen(usize),
    Open(usize),
    Activate,
    /// Reading a range's gate before closing it.
    ReadClose(usize),
    Close(usize, Option<GateState>),
    Probe(usize, Option<Vec<u8>>),
    Reopen(usize),
    Restore,
    Finish,
    Condemn(usize),
    Sweep(usize),
    Drop(usize),
    Forget,
    Done,
}

/// One attempt at creating or deleting a bucket.
#[derive(Debug, Clone)]
pub struct Coordinator {
    bucket: String,
    incarnation: u64,
    attempt: u64,
    /// Name ranges the bucket's keys fall in.
    ranges: usize,
    /// Rows one read or one collection may pass or remove, which bounds an entry's size.
    budget: u32,
    phase: Phase,
    settled: Option<Settled>,
}

impl Coordinator {
    /// The attempt a Bucket-range command started: a create its `Create` answered with
    /// `Creating`, or a delete its `BeginDelete` or `Abandon` answered with `Deleting`. `None`
    /// for any other answer, which the request answers itself.
    pub fn start(
        bucket: &str,
        outcome: &bucket::Outcome,
        ranges: usize,
        budget: u32,
    ) -> Result<Option<Self>, CoordinatorError> {
        let (incarnation, attempt, phase) = match *outcome {
            bucket::Outcome::Creating {
                incarnation,
                attempt,
            } => (incarnation, attempt, Phase::ReadOpen(0)),
            bucket::Outcome::Deleting {
                incarnation,
                attempt,
            } => (incarnation, attempt, Phase::ReadClose(0)),
            _ => return Ok(None),
        };
        Self::new(bucket, incarnation, attempt, ranges, budget, phase).map(Some)
    }

    /// The collector's resumption of a deleted bucket's cleanup, from the Bucket range's row:
    /// condemn the gates, remove the uploads, drop the gates, forget the name. `None` unless
    /// the row is deleted.
    pub fn resume(
        bucket: &str,
        row: &Bucket,
        ranges: usize,
        budget: u32,
    ) -> Result<Option<Self>, CoordinatorError> {
        if row.state != BucketState::Deleted {
            return Ok(None);
        }
        Self::new(
            bucket,
            row.created_ns,
            row.attempt,
            ranges,
            budget,
            Phase::Condemn(0),
        )
        .map(Some)
    }

    fn new(
        bucket: &str,
        incarnation: u64,
        attempt: u64,
        ranges: usize,
        budget: u32,
        phase: Phase,
    ) -> Result<Self, CoordinatorError> {
        if ranges == 0 {
            return Err(CoordinatorError::NoRanges);
        }
        Ok(Self {
            bucket: bucket.to_owned(),
            incarnation,
            attempt,
            ranges,
            budget: budget.max(1),
            phase,
            settled: None,
        })
    }

    /// What the attempt's request learns, once the attempt has reached it.
    pub fn settled(&self) -> Option<Settled> {
        self.settled
    }

    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    /// The request to send next; `None` once the attempt is done.
    pub fn next(&self) -> Option<Request> {
        use GateState::{Closed, Condemned, Open};
        let bucket = self.bucket.clone();
        Some(match &self.phase {
            Phase::ReadOpen(range) | Phase::ReadClose(range) => Request::Gate { range: *range },
            Phase::Open(range) => self.gate(*range, None, Some(Open)),
            Phase::Activate => Request::Bucket(bucket::Command::Activate {
                bucket,
                attempt: self.attempt,
            }),
            Phase::Close(range, from) => self.gate(*range, *from, Some(Closed)),
            Phase::Probe(range, from) => Request::Probe {
                range: *range,
                from: from.clone(),
                budget: usize::try_from(self.budget).unwrap_or(usize::MAX),
            },
            Phase::Reopen(range) => self.gate(*range, Some(Closed), Some(Open)),
            Phase::Restore => Request::Bucket(bucket::Command::Restore {
                bucket,
                attempt: self.attempt,
            }),
            Phase::Finish => Request::Bucket(bucket::Command::Delete {
                bucket,
                attempt: self.attempt,
            }),
            Phase::Condemn(range) => self.gate(*range, Some(Closed), Some(Condemned)),
            Phase::Sweep(range) => Request::Name {
                range: *range,
                command: Box::new(name::Command::Collect(Collect {
                    bucket,
                    incarnation: self.incarnation,
                    budget: self.budget,
                })),
            },
            Phase::Drop(range) => self.gate(*range, Some(Condemned), None),
            Phase::Forget => Request::Bucket(bucket::Command::Forget {
                bucket,
                attempt: self.attempt,
            }),
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
        use name::Outcome::{Collected, Conflict, GateMoved};
        Ok(match (phase, answer) {
            (Phase::ReadOpen(range), Answer::Gate(gate)) => match gate {
                None => Phase::Open(range),
                // Opened by the attempt this one took over.
                Some(g) if g.incarnation == self.incarnation && g.state == GateState::Open => {
                    self.then(range, Phase::ReadOpen, Phase::Activate)
                }
                Some(_) => self.superseded(),
            },
            (Phase::Open(range), Answer::Name(outcome)) => {
                if outcome == GateMoved {
                    self.then(range, Phase::ReadOpen, Phase::Activate)
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
            (Phase::ReadClose(range), Answer::Gate(gate)) => match gate {
                None => Phase::Close(range, None),
                Some(g) if g.incarnation == self.incarnation && g.state != GateState::Condemned => {
                    Phase::Close(range, Some(g.state))
                }
                Some(_) => self.superseded(),
            },
            (Phase::Close(range, _), Answer::Name(outcome)) => {
                if outcome == GateMoved {
                    self.then(range, Phase::ReadClose, Phase::Probe(0, None))
                } else {
                    self.superseded()
                }
            }
            (Phase::Probe(range, _), Answer::Probe(probe)) => match probe {
                Probe::Found => Phase::Reopen(0),
                Probe::Clear => self.then(range, |r| Phase::Probe(r, None), Phase::Finish),
                Probe::Paused(at) => Phase::Probe(range, Some(at)),
            },
            (Phase::Reopen(range), Answer::Name(outcome)) => {
                if outcome == GateMoved {
                    self.then(range, Phase::Reopen, Phase::Restore)
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
                    Phase::Condemn(0)
                } else {
                    self.superseded()
                }
            }
            (Phase::Condemn(range), Answer::Name(outcome)) => {
                if outcome == GateMoved {
                    self.then(range, Phase::Condemn, Phase::Sweep(0))
                } else {
                    self.superseded()
                }
            }
            (Phase::Sweep(range), Answer::Name(outcome)) => match outcome {
                Collected { done: false } => Phase::Sweep(range),
                Collected { done: true } => self.then(range, Phase::Sweep, Phase::Drop(0)),
                // Another collector removed the gate first.
                Conflict => self.superseded(),
                _ => return Err(CoordinatorError::Unexpected),
            },
            (Phase::Drop(range), Answer::Name(outcome)) => {
                if outcome == GateMoved {
                    self.then(range, Phase::Drop, Phase::Forget)
                } else {
                    self.superseded()
                }
            }
            (Phase::Forget, Answer::Bucket(_)) => Phase::Done,
            (Phase::Done, _) => return Err(CoordinatorError::Unexpected),
            _ => return Err(CoordinatorError::Mismatch),
        })
    }

    /// The same step at the next range, or `last` after the last range.
    fn then(&self, range: usize, same: impl FnOnce(usize) -> Phase, last: Phase) -> Phase {
        match range.checked_add(1) {
            Some(next) if next < self.ranges => same(next),
            _ => last,
        }
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

    fn gate(&self, range: usize, from: Option<GateState>, to: Option<GateState>) -> Request {
        Request::Name {
            range,
            command: Box::new(name::Command::Gate(GateChange {
                bucket: self.bucket.clone(),
                incarnation: self.incarnation,
                attempt: self.attempt,
                from,
                to,
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate(incarnation: u64, attempt: u64, state: GateState) -> Option<Gate> {
        Some(Gate {
            incarnation,
            attempt,
            state,
        })
    }

    /// A create reads and opens each range's gate, then activates the bucket.
    #[test]
    fn a_create_opens_every_gate_then_activates() {
        let creating = bucket::Outcome::Creating {
            incarnation: 7,
            attempt: 7,
        };
        let mut c = Coordinator::start("b", &creating, 2, 8).unwrap().unwrap();
        for range in 0..2 {
            assert_eq!(c.next(), Some(Request::Gate { range }));
            c.answer(Answer::Gate(None)).unwrap();
            let Some(Request::Name { range: r, command }) = c.next() else {
                panic!("a gate change");
            };
            assert_eq!(r, range);
            assert!(matches!(
                *command,
                name::Command::Gate(GateChange {
                    from: None,
                    to: Some(GateState::Open),
                    attempt: 7,
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
        let mut c = Coordinator::start("b", &deleting, 1, 8).unwrap().unwrap();
        c.answer(Answer::Gate(gate(7, 7, GateState::Open))).unwrap();
        c.answer(Answer::Name(name::Outcome::GateMoved)).unwrap();
        assert!(matches!(c.next(), Some(Request::Probe { range: 0, .. })));
        c.answer(Answer::Probe(Probe::Found)).unwrap();
        c.answer(Answer::Name(name::Outcome::GateMoved)).unwrap();
        c.answer(Answer::Bucket(bucket::Outcome::Restored)).unwrap();
        assert_eq!(c.settled(), Some(Settled::NotEmpty));

        let mut late = Coordinator::start("b", &deleting, 1, 8).unwrap().unwrap();
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
        let mut c = Coordinator::start("b", &deleting, 1, 1).unwrap().unwrap();
        c.answer(Answer::Gate(None)).unwrap();
        c.answer(Answer::Name(name::Outcome::GateMoved)).unwrap();
        c.answer(Answer::Probe(Probe::Paused(vec![1]))).unwrap();
        assert!(matches!(
            c.next(),
            Some(Request::Probe { from: Some(ref k), budget: 1, .. }) if k == &[1]
        ));
        c.answer(Answer::Probe(Probe::Clear)).unwrap();
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

    #[test]
    fn answers_that_do_not_fit_are_refused() {
        let creating = bucket::Outcome::Creating {
            incarnation: 1,
            attempt: 1,
        };
        let mut c = Coordinator::start("b", &creating, 1, 8).unwrap().unwrap();
        assert_eq!(
            c.answer(Answer::Probe(Probe::Clear)),
            Err(CoordinatorError::Mismatch)
        );
        // Refused, the coordinator is where it was.
        assert_eq!(c.next(), Some(Request::Gate { range: 0 }));
        assert_eq!(
            Coordinator::start("b", &creating, 0, 8).err(),
            Some(CoordinatorError::NoRanges)
        );
        assert!(
            Coordinator::start("b", &bucket::Outcome::AlreadyExists, 1, 8)
                .unwrap()
                .is_none()
        );
    }
}
