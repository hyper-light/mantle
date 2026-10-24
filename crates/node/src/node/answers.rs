//! A range's store as the node holds it (docs/design/node.md §2.2): the store its replica writes
//! through, and the log's answer the node looked for ahead of the replica's next drive.
//!
//! The log tells of an answer by waking the node's task (hyper-durable's `LogStore::submit`), and
//! a wake ends whichever await of the task it finds: a line the process says between turns spends
//! it as surely as the node's own wait does, and the wait that follows has nothing to wake it. So
//! before it waits the node looks where each answer lies ([`Answers::answered`]); one it finds is
//! kept here, and the replica's next drive takes it first ([`LogStore::poll`]), as it would have
//! taken it from the store. The node takes an answer only while it keeps none, so it keeps one at
//! most, and the answers reach the replica in the order the log gave them.

use std::task::Waker;

use hyper_durable::{EntryRef, Fault, LogStore, Point, StoreView, Write};
use hyper_raft::StorageError;
use hyper_raft::proto::Entry;

/// A range's store, and the answer taken from it ahead of the replica.
pub struct Answers<S> {
    store: S,
    taken: Option<Result<(), Fault>>,
}

impl<S: LogStore> Answers<S> {
    pub fn new(store: S) -> Self {
        Self { store, taken: None }
    }

    /// Whether an answer waits for the replica: one taken already, or the oldest write's, which is
    /// taken now if it has come.
    pub fn answered(&mut self) -> bool {
        if self.taken.is_none() {
            self.taken = self.store.poll();
        }
        self.taken.is_some()
    }
}

impl<S: LogStore> LogStore for Answers<S> {
    type Hold = S::Hold;

    fn held(&self) -> Option<&Self::Hold> {
        self.store.held()
    }

    fn release(&mut self, met: &Self::Hold) {
        self.store.release(met);
    }

    fn depth(&self) -> usize {
        self.store.depth()
    }

    fn view(&self) -> Result<StoreView, Fault> {
        self.store.view()
    }

    fn bounds(&self) -> Result<(Point, u64), StorageError> {
        self.store.bounds()
    }

    fn term(&self, index: u64) -> Result<u64, StorageError> {
        self.store.term(index)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: &mut Vec<Entry>,
    ) -> Result<(), StorageError> {
        self.store.entries(low, high, max_bytes, into)
    }

    fn visit(
        &self,
        low: u64,
        high: u64,
        page: u64,
        visit: &mut dyn FnMut(EntryRef<'_>) -> bool,
    ) -> Result<(), StorageError> {
        self.store.visit(low, high, page, visit)
    }

    fn proposals(&self, into: &mut Vec<Entry>) -> Result<(), StorageError> {
        self.store.proposals(into)
    }

    fn released(&self) -> Result<u64, StorageError> {
        self.store.released()
    }

    fn room(&self) -> bool {
        self.store.room()
    }

    fn submit(&mut self, write: &Write<'_>, waker: &Waker) -> Result<(), Fault> {
        self.store.submit(write, waker)
    }

    fn poll(&mut self) -> Option<Result<(), Fault>> {
        self.taken.take().or_else(|| self.store.poll())
    }

    fn write_now(&mut self, write: &Write<'_>) -> Result<(), Fault> {
        self.store.write_now(write)
    }

    fn keep_after(&mut self, index: u64) {
        self.store.keep_after(index);
    }

    fn kept_bytes(&self) -> u64 {
        self.store.kept_bytes()
    }
}
