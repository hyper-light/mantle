//! The sweep for files a gateway made and never handed over (docs/design/metadata.md §2).
//!
//! Every file waits in its File range's queue of unsettled files from its write until the
//! sweep settles it, as HDFS allocates a block before it is written and RocksDB registers a
//! job's outputs while it runs (docs/research/22 §3.1, §7.3). Once a file's handover deadline
//! has passed, the sweep asks the Name range its key routes to whether it took the file. The
//! range answers from its own mark, or, finding none, releases the file into its queue there
//! and then: its time only moves forward, so a handover that comes after finds the deadline
//! passed and is refused, and the check and the handover are ordered by the range's log, as
//! Giza's no-op and a stalled put contend for one Paxos slot (22 §7.4). The sweep then settles
//! the file in the File range and removes the range's mark, in that order: a mark removed first
//! would read, to a sweep resumed after a stop, as a file never handed over.
//!
//! A [`Sweep`] names its next [`Request`] and moves on with the [`Answer`], doing no I/O, as
//! the coordinator and the reclaimer do. Every step can be repeated.

use crate::file::Unsettled;
use crate::name::Verdict;
use crate::record::Referrer;

/// A sweep's next request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// A read of at most `max` of the File range's unsettled files whose deadline has passed at
    /// its time ([`crate::file::unsettled`]).
    Due { max: usize },
    /// `name::Command::Check` of each file, sent to the Name range its referrer's key falls
    /// in, with the verdicts answered in the order asked.
    Check(Vec<Unsettled>),
    /// `file::Command::Settle` for these files.
    Settle(Vec<u128>),
    /// `name::Command::Unmark` for these files, each sent where its referrer's key falls.
    Unmark(Vec<(Referrer, u128)>),
}

/// What a request answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Due(Vec<Unsettled>),
    Checked(Vec<Verdict>),
    Settled,
    Unmarked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SweepError {
    #[error("an answer of another kind than the request")]
    Mismatch,
    #[error("a verdict for each file asked about, and no other")]
    Count,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Phase {
    Read,
    Check(Vec<Unsettled>),
    Settle(Vec<(Referrer, u128)>),
    Unmark(Vec<(Referrer, u128)>),
    Done,
}

/// One pass over a File range's unsettled files, a page at a time. A pass ends when a page is
/// empty, or holds only files whose Name range's time has not yet passed their deadline; the
/// driver runs another once the soonest of those comes due.
#[derive(Debug, Clone)]
pub struct Sweep {
    /// Files one page holds, which bounds each request's entry.
    page: usize,
    phase: Phase,
}

impl Sweep {
    pub fn new(page: usize) -> Self {
        Self {
            page: page.max(1),
            phase: Phase::Read,
        }
    }

    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    /// The request to send next; `None` once the pass is done.
    pub fn next(&self) -> Option<Request> {
        Some(match &self.phase {
            Phase::Read => Request::Due { max: self.page },
            Phase::Check(files) => Request::Check(files.clone()),
            Phase::Settle(decided) => Request::Settle(decided.iter().map(|(_, f)| *f).collect()),
            Phase::Unmark(decided) => Request::Unmark(decided.clone()),
            Phase::Done => return None,
        })
    }

    /// Takes the answer to the request [`next`](Self::next) named, and moves on. An answer that
    /// does not fit the request is refused, and the sweep stays where it was.
    pub fn answer(&mut self, answer: Answer) -> Result<(), SweepError> {
        let next = match (&self.phase, answer) {
            (Phase::Read, Answer::Due(files)) if files.is_empty() => Phase::Done,
            (Phase::Read, Answer::Due(files)) => Phase::Check(files),
            (Phase::Check(files), Answer::Checked(verdicts)) => {
                if verdicts.len() != files.len() {
                    return Err(SweepError::Count);
                }
                let decided: Vec<(Referrer, u128)> = files
                    .iter()
                    .zip(&verdicts)
                    .filter(|(_, v)| **v != Verdict::Young)
                    .map(|(u, _)| (u.referrer.clone(), u.file))
                    .collect();
                if decided.is_empty() {
                    Phase::Done
                } else {
                    Phase::Settle(decided)
                }
            }
            (Phase::Settle(decided), Answer::Settled) => Phase::Unmark(decided.clone()),
            (Phase::Unmark(_), Answer::Unmarked) => Phase::Read,
            _ => return Err(SweepError::Mismatch),
        };
        self.phase = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unsettled(file: u128) -> Unsettled {
        Unsettled {
            file,
            deadline_ns: 10,
            referrer: Referrer {
                bucket: "b".into(),
                incarnation: 1,
                key: format!("k{file}"),
            },
        }
    }

    #[test]
    fn a_page_is_checked_then_settled_then_unmarked() {
        let mut s = Sweep::new(2);
        assert_eq!(s.next(), Some(Request::Due { max: 2 }));
        s.answer(Answer::Due(vec![unsettled(1), unsettled(2)]))
            .unwrap();
        assert_eq!(
            s.next(),
            Some(Request::Check(vec![unsettled(1), unsettled(2)]))
        );
        assert_eq!(
            s.answer(Answer::Checked(vec![Verdict::Held])),
            Err(SweepError::Count)
        );
        s.answer(Answer::Checked(vec![Verdict::Young, Verdict::Released]))
            .unwrap();
        // Only the file decided is settled and unmarked; the young one waits.
        assert_eq!(s.next(), Some(Request::Settle(vec![2])));
        assert_eq!(s.answer(Answer::Unmarked), Err(SweepError::Mismatch));
        s.answer(Answer::Settled).unwrap();
        assert_eq!(
            s.next(),
            Some(Request::Unmark(vec![(unsettled(2).referrer, 2)]))
        );
        s.answer(Answer::Unmarked).unwrap();
        // The next page holds only the young file: the pass ends there.
        assert_eq!(s.next(), Some(Request::Due { max: 2 }));
        s.answer(Answer::Due(vec![unsettled(1)])).unwrap();
        s.answer(Answer::Checked(vec![Verdict::Young])).unwrap();
        assert!(s.is_done());
        assert_eq!(s.next(), None);
        let mut empty = Sweep::new(0);
        assert_eq!(empty.next(), Some(Request::Due { max: 1 }));
        empty.answer(Answer::Due(Vec::new())).unwrap();
        assert!(empty.is_done());
    }
}
