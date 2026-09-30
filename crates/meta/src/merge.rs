//! Merging a Name range into the range just below it, across the two ranges' logs
//! (docs/design/metadata.md §3), as docs/models/RangeSplit.tla lays out.
//!
//! The driver freezes the higher range for a merge into the lower one at the generation it
//! read, then asks the lower range to decide. The lower range decides once, in its own log,
//! and moves its generation on either way, so a command for the merge that comes later,
//! however late, changes nothing. A taken merge ends the frozen range and is then resolved; a
//! refused one thaws it. The driver thaws only once the lower range shows the merge will never
//! be taken: its generation past the merge's and the merge not the one it holds, or the range
//! ended. A range holding a merge not yet resolved is not frozen, so it cannot end with the
//! merge unresolved and mislead that judgment.
//!
//! A [`Merger`] names its next [`Request`] and moves on with the [`Answer`], doing no I/O, as
//! the coordinator does. Every step can be repeated, and a driver that stops is replaced by
//! one resumed from either range's lineage. Once the lower range has let go of a merge it
//! took, its lineage no longer shows the merge, so a driver still judging it reads it as
//! never taken; but by then the frozen range has ended, and its thaw is refused.

use crate::name::{self, Abandon, End, Freeze, Merge, Resolve, Thaw};
use crate::record::{Descriptor, Lineage, Standing, Taken};

/// A merger's next request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// A command to Name range `range`.
    Name {
        range: u64,
        command: Box<name::Command>,
    },
    /// The lower range's decision ([`name::merge`]), which each of its replicas applies with
    /// its own copy of range `from`: sent once every replica of `from` has applied its
    /// freeze, so every copy is the frozen one.
    Merge {
        range: u64,
        from: u64,
        command: Merge,
    },
    /// A read of range `range`'s lineage.
    Lineage { range: u64 },
}

/// What a request answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Name(name::Outcome),
    Lineage(Lineage),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MergeError {
    #[error("an answer of another kind than the request")]
    Mismatch,
    /// An answer the protocol rules out, such as a frozen range thawed after its merge was
    /// taken: the merger stops rather than act on it.
    #[error("an answer the merger's step cannot have")]
    Unexpected,
    #[error("a lineage that names no merge to resume")]
    NotMerging,
}

/// Where a merge is.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Phase {
    /// Freezing the higher range, as the driver read it.
    Freeze(Descriptor),
    /// Asking the lower range to decide on the higher range, as it froze.
    Merge(Descriptor),
    /// Reading the lower range to learn whether it took the merge of the higher range, as it
    /// froze.
    Judge(Descriptor),
    /// Reading the higher range to learn whether it has ended, the lower range being as
    /// given.
    Upper(Descriptor),
    /// Ending the frozen range, naming the lower range as the merge left it.
    End(Descriptor),
    Resolve,
    Thaw,
    Done,
}

/// One merge of a range into `lower`, the range just below it.
#[derive(Debug, Clone)]
pub struct Merger {
    /// The lower range as the driver read it: its ID and generation name the merge.
    lower: Descriptor,
    /// The higher range's ID.
    upper: u64,
    /// Rows the merge may take, and their bytes, which bound its batch.
    max_rows: u64,
    max_bytes: u64,
    phase: Phase,
    merged: Option<bool>,
}

impl Merger {
    /// A merge of `upper` into `lower`, the range just below it, as the driver read both.
    pub fn new(lower: Descriptor, upper: Descriptor, max_rows: u64, max_bytes: u64) -> Self {
        Self {
            lower,
            upper: upper.id,
            max_rows,
            max_bytes,
            phase: Phase::Freeze(upper),
            merged: None,
        }
    }

    /// The merge a range's lineage shows left behind by a driver that stopped: a range frozen
    /// for one, whose merge is judged from the lower range, or a range holding one it took,
    /// whose frozen range is ended if it has not.
    pub fn resume(range: &Lineage, max_rows: u64, max_bytes: u64) -> Result<Self, MergeError> {
        match (range.standing, &range.into, range.taken) {
            (Standing::Frozen, Some(lower), _) => Ok(Self {
                lower: lower.clone(),
                upper: range.now.id,
                max_rows,
                max_bytes,
                phase: Phase::Judge(range.now.clone()),
                merged: None,
            }),
            (Standing::Serving, _, Some(Taken { from, generation })) => Ok(Self {
                lower: Descriptor {
                    generation,
                    ..range.now.clone()
                },
                upper: from,
                max_rows,
                max_bytes,
                phase: Phase::Upper(range.now.clone()),
                merged: Some(true),
            }),
            _ => Err(MergeError::NotMerging),
        }
    }

    /// Whether the merge was taken, once the merger knows.
    pub fn merged(&self) -> Option<bool> {
        self.merged
    }

    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    /// The request to send next; `None` once the merge is done.
    pub fn next(&self) -> Option<Request> {
        let name = |range: u64, command: name::Command| Request::Name {
            range,
            command: Box::new(command),
        };
        Some(match &self.phase {
            Phase::Freeze(upper) => name(
                self.upper,
                name::Command::Freeze(Freeze {
                    generation: upper.generation,
                    into: self.lower.clone(),
                }),
            ),
            Phase::Merge(frozen) => Request::Merge {
                range: self.lower.id,
                from: self.upper,
                command: Merge {
                    generation: self.lower.generation,
                    from: frozen.clone(),
                    max_rows: self.max_rows,
                    max_bytes: self.max_bytes,
                },
            },
            Phase::Judge(_) => Request::Lineage {
                range: self.lower.id,
            },
            Phase::Upper(_) => Request::Lineage { range: self.upper },
            Phase::End(into) => name(
                self.upper,
                name::Command::End(End {
                    generation: self.lower.generation,
                    into: into.clone(),
                }),
            ),
            Phase::Resolve => name(
                self.lower.id,
                name::Command::Resolve(Resolve {
                    from: self.upper,
                    generation: self.lower.generation,
                }),
            ),
            Phase::Thaw => name(
                self.upper,
                name::Command::Thaw(Thaw {
                    generation: self.lower.generation,
                }),
            ),
            Phase::Done => return None,
        })
    }

    /// Gives the merge up before the lower range decides it: the request that abandons it,
    /// in place of the decision, after which the merge is judged from the lower range's
    /// answer. `None` unless the merge awaits its decision.
    pub fn abandon(&mut self) -> Option<Request> {
        let Phase::Merge(frozen) = &self.phase else {
            return None;
        };
        self.phase = Phase::Judge(frozen.clone());
        Some(Request::Name {
            range: self.lower.id,
            command: Box::new(name::Command::Abandon(Abandon {
                generation: self.lower.generation,
            })),
        })
    }

    /// Takes the answer to the request [`next`](Self::next) or [`abandon`](Self::abandon)
    /// named, and moves on. An answer that does not fit the request is refused, and the
    /// merger stays where it was.
    pub fn answer(&mut self, answer: Answer) -> Result<(), MergeError> {
        use name::Outcome as O;
        let next = match (self.phase.clone(), answer) {
            (Phase::Freeze(_), Answer::Name(O::Frozen(frozen))) => Phase::Merge(frozen.now),
            // Refused, the range is not frozen and the merge never began.
            (Phase::Freeze(_), Answer::Name(O::Moved(_) | O::Conflict | O::Invalid)) => {
                self.merged = Some(false);
                Phase::Done
            }
            (Phase::Merge(_), Answer::Name(O::Merged(merged))) => {
                self.merged = Some(true);
                Phase::End(merged.now)
            }
            (Phase::Merge(_), Answer::Name(O::Refused(_))) => {
                self.merged = Some(false);
                Phase::Thaw
            }
            (Phase::Merge(frozen), Answer::Name(O::Moved(lower)))
            // An abandon's answer: moved on now, or the range was elsewhere already.
            | (Phase::Judge(frozen), Answer::Name(O::Refused(lower) | O::Moved(lower))) => {
                self.judge(frozen, &lower)?
            }
            (Phase::Judge(frozen), Answer::Lineage(lower)) => self.judge(frozen, &lower)?,
            (Phase::Upper(lower), Answer::Lineage(upper)) => match upper.standing {
                Standing::Frozen => Phase::End(lower),
                Standing::Ended => Phase::Resolve,
                // Thawed although its merge was taken.
                Standing::Serving => return Err(MergeError::Unexpected),
            },
            (Phase::End(_), Answer::Name(O::Ended)) => Phase::Resolve,
            // Resolved already, and the range frozen since for a merge of its own.
            (Phase::Resolve, Answer::Name(O::Resolved | O::Moved(_))) => Phase::Done,
            // Thawed now, or by another driver before.
            (Phase::Thaw, Answer::Name(O::Thawed(_) | O::Conflict)) => Phase::Done,
            // Ended: another driver saw the merge taken and resolved it, and the lower range,
            // letting go of it, no longer showed it. A thaw acts only on a range still frozen
            // for the merge, so none came of the judgment.
            (Phase::Thaw, Answer::Name(O::Moved(upper))) if upper.standing == Standing::Ended => {
                self.merged = Some(true);
                Phase::Done
            }
            (_, Answer::Name(_)) => return Err(MergeError::Unexpected),
            _ => return Err(MergeError::Mismatch),
        };
        self.phase = next;
        Ok(())
    }

    /// What the lower range's lineage says of the merge of `frozen`: taken, never to be taken,
    /// or not yet decided, in which case the decision is asked for again.
    fn judge(&mut self, frozen: Descriptor, lower: &Lineage) -> Result<Phase, MergeError> {
        let named = Taken {
            from: self.upper,
            generation: self.lower.generation,
        };
        if lower.taken == Some(named) {
            self.merged = Some(true);
            return Ok(Phase::End(lower.now.clone()));
        }
        if lower.standing == Standing::Ended || lower.now.generation > self.lower.generation {
            self.merged = Some(false);
            return Ok(Phase::Thaw);
        }
        if lower.now.generation < self.lower.generation {
            return Err(MergeError::Unexpected);
        }
        Ok(match lower.standing {
            // Read while frozen for a merge of its own, the lower range decides nothing until
            // that merge ends it or it thaws a generation on; either way this merge is then
            // never taken.
            Standing::Frozen => Phase::Judge(frozen),
            _ => Phase::Merge(frozen),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{Engine, Model};
    use crate::key;

    struct Range {
        engine: Model,
        index: u64,
    }

    /// Two ranges, split at "m" from a first one, and a merger's requests served by them.
    struct Cell {
        ranges: Vec<Range>,
    }

    impl Cell {
        fn new() -> Self {
            let mut first = Model::default();
            first.install(0, name::first(1).unwrap()).unwrap();
            let s = name::Split {
                generation: 1,
                at: key::route("b", "m"),
                child: 2,
            };
            let child = name::child(&first, &s).unwrap().unwrap();
            let mut engine = Model::default();
            engine.install(0, child.rows).unwrap();
            name::apply(&mut first, 1, &name::Command::Split(s)).unwrap();
            Self {
                ranges: vec![
                    Range {
                        engine: first,
                        index: 1,
                    },
                    Range { engine, index: 0 },
                ],
            }
        }

        fn at(&mut self, id: u64) -> &mut Range {
            &mut self.ranges[usize::try_from(id - 1).unwrap()]
        }

        fn lineage(&self, id: u64) -> Lineage {
            name::lineage(&self.ranges[usize::try_from(id - 1).unwrap()].engine).unwrap()
        }

        fn serve(&mut self, request: Request) -> Answer {
            match request {
                Request::Name { range, command } => {
                    let r = self.at(range);
                    r.index += 1;
                    Answer::Name(name::apply(&mut r.engine, r.index, &command).unwrap())
                }
                Request::Merge {
                    range,
                    from,
                    command,
                } => {
                    let [lower, upper] = &mut self.ranges[..] else {
                        panic!("two ranges");
                    };
                    let (lower, upper) = if range == 1 {
                        (lower, &*upper)
                    } else {
                        (upper, &*lower)
                    };
                    assert_eq!(from, if range == 1 { 2 } else { 1 });
                    lower.index += 1;
                    Answer::Name(
                        name::merge(&mut lower.engine, lower.index, &command, &upper.engine)
                            .unwrap(),
                    )
                }
                Request::Lineage { range } => Answer::Lineage(self.lineage(range)),
            }
        }

        fn drive(&mut self, m: &mut Merger, steps: usize) {
            for _ in 0..steps {
                let Some(request) = m.next() else {
                    return;
                };
                let answer = self.serve(request);
                m.answer(answer).unwrap();
            }
        }
    }

    #[test]
    fn a_merge_freezes_is_taken_ends_and_resolves() {
        let mut cell = Cell::new();
        let mut m = Merger::new(cell.lineage(1).now, cell.lineage(2).now, 64, u64::MAX);
        cell.drive(&mut m, 16);
        assert!(m.is_done());
        assert_eq!(m.merged(), Some(true));
        let lower = cell.lineage(1);
        assert_eq!((lower.now.hi, lower.taken), (None, None));
        assert_eq!(cell.lineage(2).standing, Standing::Ended);
    }

    /// A driver that stops anywhere is replaced by one resumed from either range's lineage,
    /// and the merge ends the same way.
    #[test]
    fn a_merge_resumes_from_either_range_after_any_step() {
        for stop in 0..5 {
            let mut cell = Cell::new();
            let mut m = Merger::new(cell.lineage(1).now, cell.lineage(2).now, 64, u64::MAX);
            cell.drive(&mut m, stop);
            let (upper, lower) = (cell.lineage(2), cell.lineage(1));
            let resumed = [
                Merger::resume(&upper, 64, u64::MAX),
                Merger::resume(&lower, 64, u64::MAX),
            ];
            let Some(mut r) = resumed.into_iter().find_map(Result::ok) else {
                // Not begun, or done: nothing left to resume.
                assert!(stop == 0 || m.is_done(), "stopped after {stop} steps");
                continue;
            };
            cell.drive(&mut r, 16);
            assert!(r.is_done(), "stopped after {stop} steps");
            assert_eq!(cell.lineage(2).standing, Standing::Ended);
            assert_eq!(cell.lineage(1).taken, None);
        }
    }

    /// Given up before its decision, the merge is abandoned, and the frozen range thaws; the
    /// decision that comes after takes nothing.
    #[test]
    fn an_abandoned_merge_thaws_and_stays_untaken() {
        let mut cell = Cell::new();
        let mut m = Merger::new(cell.lineage(1).now, cell.lineage(2).now, 64, u64::MAX);
        cell.drive(&mut m, 1);
        let Some(Request::Merge {
            range,
            from,
            command,
        }) = m.next()
        else {
            panic!("the decision");
        };
        let abandon = m.abandon().unwrap();
        let answer = cell.serve(abandon);
        m.answer(answer).unwrap();
        cell.drive(&mut m, 16);
        assert_eq!(m.merged(), Some(false));
        assert_eq!(cell.lineage(2).standing, Standing::Serving);
        let late = cell.serve(Request::Merge {
            range,
            from,
            command,
        });
        assert!(matches!(late, Answer::Name(name::Outcome::Moved(_))));
        assert_eq!(cell.lineage(1).now.hi, Some(key::route("b", "m")));
        assert_eq!(
            Merger::resume(&cell.lineage(2), 64, u64::MAX).err(),
            Some(MergeError::NotMerging)
        );
    }
}
