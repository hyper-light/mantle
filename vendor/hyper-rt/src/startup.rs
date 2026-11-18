//! How a thread a runtime starts (a shard's worker, the watchdog) tells its starter that it runs
//! (docs/runtime.md §3.17): a cell of the process-wide table (`crate::sync::cell`), its state word counting
//! the threads that said they run and its second word those that ended without saying so, the starter parked
//! on it until every thread it counts has answered (`crate::handoff::wait_as_thread`).
//!
//! **Nothing is left to free.** The hand-off allocates nothing: a cell is a slot of a table kept for the
//! process, and its last handle gives it back by setting a bit. So a caller that counts the bytes live around
//! `Runtime::start` counts no hand-off, whichever thread lets go last. Until 2026-11-18 the hand-off was a
//! `std::sync::mpsc::sync_channel`, freed by whichever end dropped last (std `sync/mpmc/counter.rs`,
//! `release`). A thread dropped its end only after its send had woken the starter, so on one CPU the starter
//! could run first and return with the channel still allocated: mantle's `crates/node/tests/shard_memory.rs`
//! counted 744 bytes too many on CI's one-CPU Linux leg, the 648-byte channel and the 96-byte waiter list its
//! blocked receiver grew. A second receive, waiting for the sender's disconnect, would only have narrowed the
//! window: std's release wakes the receiver as it disconnects, before it settles which end frees.
//!
//! **The wait is bounded by the threads.** Every counted thread answers once: its [`Answer`] says it runs, or
//! is dropped unsaid and says it went, by the thread as it ends or with the closure of a spawn the OS refused.
//! Each answer changes a word of the cell and then wakes, and the starter registers and then checks
//! (`crate::handoff`), so no answer is missed.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::RtError;
use crate::sync::cell::{Cell, CellRef, claim};

/// The starter's end: the cell its threads answer in.
#[derive(Debug)]
pub(crate) struct Answers {
    cell: CellRef,
}

/// A started thread's end, until it answers.
#[derive(Debug)]
pub(crate) struct Answer {
    /// `None` once the thread said it runs: the drop then says nothing.
    cell: Option<CellRef>,
}

/// Why a wait ended before every thread ran: one ended without saying it runs.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Went;

impl Answers {
    /// A hand-off no thread answers in yet; refused `Capacity` at the cells' bound.
    pub(crate) fn new() -> Result<Answers, RtError> {
        Ok(Answers { cell: claim(1)? })
    }

    /// The end of one more thread, moved to it before it is spawned.
    pub(crate) fn answer(&self) -> Answer {
        self.cell.retain();
        Answer {
            cell: Some(self.cell),
        }
    }

    /// Parks the calling thread until `threads` threads have said they run (`Ok`), or one has ended without
    /// saying so (`Went`), whichever comes first.
    pub(crate) fn wait(&self, threads: usize) -> Result<(), Went> {
        self.cell
            .wait_as_thread(|| answered(self.cell.cell(), threads))
            .unwrap_or(Err(Went))
    }
}

impl Drop for Answers {
    fn drop(&mut self) {
        self.cell.release();
    }
}

impl Answer {
    /// Says the thread runs, and lets go of the cell.
    pub(crate) fn ran(mut self) {
        if let Some(cell) = self.cell.take() {
            say(cell, |words| &words.state);
        }
    }
}

impl Drop for Answer {
    /// Unsaid: the thread ended, or was never spawned, without running; says it went.
    fn drop(&mut self) {
        if let Some(cell) = self.cell.take() {
            say(cell, |words| &words.aux);
        }
    }
}

/// What a wait on `threads` threads ends with, from the cell's `words`: `None` until it ends. A cell gone
/// while its starter holds a handle is never seen; were it, the wait ends `Went` rather than for good.
fn answered(words: Option<&Cell>, threads: usize) -> Option<Result<(), Went>> {
    let Some(words) = words else {
        return Some(Err(Went));
    };
    if words.aux.load(Ordering::Acquire) != 0 {
        return Some(Err(Went));
    }
    let ran = words.state.load(Ordering::Acquire);
    usize::try_from(ran)
        .ok()
        .is_none_or(|ran| ran >= threads)
        .then_some(Ok(()))
}

/// Counts one answer in the `word` of `cell`, wakes the starter, then lets go of the handle.
fn say(cell: CellRef, word: impl FnOnce(&Cell) -> &AtomicU64) {
    if let Some(words) = cell.cell() {
        word(words).fetch_add(1, Ordering::AcqRel);
    }
    cell.wake();
    cell.release();
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    /// A wait ends once every counted thread said it runs, whatever the order of their answers.
    #[test]
    fn a_wait_ends_once_every_thread_ran() {
        let answers = Answers::new().unwrap();
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let answer = answers.answer();
                std::thread::spawn(move || answer.ran())
            })
            .collect();
        assert_eq!(answers.wait(4), Ok(()));
        for thread in threads {
            thread.join().unwrap();
        }
    }

    /// A thread that ends without saying it runs, or is never spawned, ends the wait `Went`.
    #[test]
    fn an_answer_dropped_unsaid_ends_the_wait() {
        let answers = Answers::new().unwrap();
        let ran = answers.answer();
        let went = answers.answer();
        let thread = std::thread::spawn(move || drop(went));
        ran.ran();
        assert_eq!(answers.wait(2), Err(Went));
        thread.join().unwrap();
    }

    /// The cell goes back to the table once the starter and every thread have let go.
    #[test]
    fn the_cell_goes_back_once_every_end_let_go() {
        let answers = Answers::new().unwrap();
        let cell = answers.cell;
        let answer = answers.answer();
        drop(answers);
        assert!(cell.cell().is_some(), "the thread's end keeps it");
        answer.ran();
        assert!(cell.cell().is_none(), "the last end gave it back");
    }
}
