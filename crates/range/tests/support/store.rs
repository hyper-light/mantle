//! hyper-log's group handle with every write made durable, or refused, before its submission
//! returns: what the deterministic tests run their members on. The shell takes each answer at its
//! next drive, as from any store, so readies are still taken ahead of their answers; but what a
//! drive gives out never hangs on when the log's threads answered, and a seed runs the same every
//! time (docs/design/replica.md §5). The real-process test runs hyper-log's handle as it is.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, sync_channel};
use std::task::Waker;

use hyper_block::block::BlockFile;
use hyper_durable::{EntryRef, Fault, GroupStore, LogStore, Point, StoreView, Write};
use hyper_raft::StorageError;
use mantle_range::Entry;

pub struct Synchronous<F: BlockFile + 'static> {
    inner: GroupStore<F>,
    /// Answers taken from the log, not yet given to the shell.
    answers: VecDeque<Result<(), Fault>>,
    /// The store's own waker: the log wakes it once for every part it answers.
    waker: Waker,
    woken: Receiver<usize>,
    /// While set, the shell is given no answer: a device that holds its writes. The test that
    /// sets it leaks it for the store to read; nothing else is shared.
    hold: Option<&'static AtomicBool>,
}

impl<F: BlockFile + 'static> Synchronous<F> {
    /// The store over `inner`; with `hold`, one whose answers the shell is given only while it is
    /// clear.
    pub fn new(inner: GroupStore<F>, hold: Option<&'static AtomicBool>) -> Self {
        // One wake a part answered, taken before the next is asked: the channel holds the parts
        // of one write at most, which the log's frames bound.
        let (tell, woken) = sync_channel(1 << 10);
        let (waker, _) = hyper_measure::wake::waker(0, tell);
        Self {
            inner,
            answers: VecDeque::new(),
            waker,
            woken,
            hold,
        }
    }
}

impl<F: BlockFile + 'static> LogStore for Synchronous<F> {
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

    fn room(&self) -> bool {
        self.inner.room()
    }

    /// Submits `write` and waits for the log's answer, which the next `poll` gives. A wake that
    /// came for a part the poll before already found answered makes one poll more, no more.
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
        if self.hold.is_some_and(|hold| hold.load(Ordering::SeqCst)) {
            return None;
        }
        self.answers.pop_front()
    }

    fn write_now(&mut self, write: &Write<'_>) -> Result<(), Fault> {
        self.inner.write_now(write)
    }
}
