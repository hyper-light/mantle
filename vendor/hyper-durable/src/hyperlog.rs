//! hyper-log's group handle as a [`LogStore`]: the first store (`docs/durable.md` §8). One log
//! per device serves every group on it, so one flush commits every group's writes; the handle
//! reads the group's state where the replica is, as its answered writes left it.
//!
//! A write is laid out as one update of the log, entries encoded as mantle's store encodes them
//! (the entry's kind, its context's length, its context and its data), and cut into parts that
//! each fit a frame (`GroupLog::parts`): entries first and the hard state last, so a crash
//! between parts leaves only entries no one acknowledged (`docs/durable.md` §2.3). The write is
//! answered once every part is. Parts beyond the handle's bound wait here and go out as answers
//! free room. A part refused, and every part sent behind it, changes nothing; parts not yet sent
//! then are never sent.
//!
//! A read is paged as the core pages: each entry counted at what it takes in a message, the bytes
//! the log stores of it and those the message adds (`IN_MESSAGE_BEYOND_STORED`), which the log
//! counts as it chooses the page, before anything is copied. The core takes a page its storage cut
//! as whole, so a page cut by the stored bytes alone ran past the core's bound on a message
//! (mantle, 2026-10-19).
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::task::Waker;

use hyper_block::block::BlockFile;
use hyper_log::{
    Fetched, Fetching, GroupLog, Log, LogError, LogOpener, Page, Proposal, Start, Update,
};
use hyper_raft::StorageError;
use hyper_raft::proto::{Entry, EntryType, HardState};

use crate::store::{Entries, EntryRef, Fault, Health, LogStore, Point, StoreView, Write};

/// Bytes an entry's encoding adds to its data and context: its kind and its context's length,
/// as mantle's store writes them (mantle `crates/range/src/store.rs`, `ENTRY_OVERHEAD`).
pub const ENTRY_OVERHEAD: usize = 1 + 4;

/// Bytes an entry takes in a message beyond those the log stores of it: its body there, what the
/// core counts an entry at (`hyper_raft::proto::encoded_bytes`), is its data, its context and
/// `hyper_raft::wire::ENTRY_FIXED_BYTES` (its kind, term, index and two lengths), and the log
/// stores its data, its context and [`ENTRY_OVERHEAD`], its term and index kept outside them.
const IN_MESSAGE_BEYOND_STORED: u64 = (hyper_raft::wire::ENTRY_FIXED_BYTES - ENTRY_OVERHEAD) as u64;

/// What a write's parts came to.
#[derive(Debug)]
struct Parts {
    total: usize,
    sent: usize,
    answered: usize,
    outcome: Result<(), Fault>,
}

impl Parts {
    /// A part's answer: the first refusal or failure is the write's.
    fn answer(&mut self, answer: Result<(), Fault>) {
        self.answered = self.answered.saturating_add(1);
        if self.outcome.is_ok() {
            self.outcome = answer;
        }
    }
}

/// A group of a hyper-log log, written and read through its handle.
pub struct GroupStore<F: BlockFile + 'static> {
    group: GroupLog<F>,
    /// The most bytes one read of the log gives: a segment's.
    page: u64,
    /// The reservation entries are fetched into, kept between reads while it holds no more
    /// than a page, as hyper-log keeps its own (`Log::entries`).
    fetched: Cell<Option<Fetched>>,
    /// Writes not yet answered, oldest first.
    writes: VecDeque<Parts>,
    /// Parts not yet sent, in order.
    queued: VecDeque<Update>,
    waker: Option<Waker>,
    /// The members a leader catches up from the log's file ([`LogStore::catch_up`]).
    catching: RefCell<CatchUp>,
    /// Whether the log reads a fetch that no caller waits on without being told to: every log but
    /// one under `Waits::Told`, whose caller decides when its device reads (`Log::serve`), and where
    /// the store reads where the core asks, as before.
    reads_untold: bool,
}

/// How a leader's store catches up the members whose next page its handle no longer holds in
/// memory ([`LogStore::catch_up`]). The core asks for a member's next page; the store opens a run of
/// the log at that index, the entries one read of the log gives, which waits its turn
/// ([`Step::Waiting`]), is read from the log's file off the replica's thread, one run of the group's
/// at a time ([`Step::Reading`]), and is held once read ([`Step::Read`]), the core told meanwhile
/// that the store does not hold the page yet. At the end of every drive the replica has the core
/// send their pages to the members whose next index a read run holds; each page the core takes
/// opens the run after it, so the next run is read while this one is sent, and kept until the
/// member reaches it. Any other run that holds no member's next index is let go.
#[derive(Default)]
struct CatchUp {
    /// The owner's waker, which a run's answer wakes: none until the replica first calls, and
    /// until then the store answers where the core asks.
    waker: Option<Waker>,
    /// The mark after which the replica reads entries back itself ([`LogStore::keep_after`]): an
    /// entry at or before it no read of the replica's own wants, only a member catching up.
    mark: u64,
    /// The runs, in the order asked: those holding a member's next index, the one after each of
    /// them, and the one being read.
    runs: VecDeque<Run>,
}

/// Entries of the log from `low`, as one read of the log gives them, and where its read stands.
struct Run {
    low: u64,
    step: Step,
}

/// Where a run's read stands.
enum Step {
    /// Asked for, behind the run being read.
    Waiting,
    /// Being read from the log's file: its answer wakes the owner.
    Reading(Fetching),
    /// Read: its entries from `low`, or why the log could not give them.
    Read(Result<Fetched, StorageError>),
}

impl Run {
    /// The index past the entries the run holds, read: one that failed holds its first index,
    /// whose reader is answered its failure; one not read yet holds nothing.
    fn end(&self) -> u64 {
        let held = match &self.step {
            Step::Read(Ok(fetched)) => u64::try_from(fetched.len()).unwrap_or(u64::MAX),
            Step::Read(Err(_)) => 1,
            Step::Waiting | Step::Reading(_) => 0,
        };
        self.low.saturating_add(held)
    }

    /// Whether the run, read, holds the entry at `index`.
    fn holds(&self, index: u64) -> bool {
        index >= self.low && index < self.end()
    }
}

impl CatchUp {
    /// Whether `r` is wanted still, the members' next indexes `lows`: being read, waiting for or
    /// holding a member's next index, or right after a run that holds one, read while that is sent.
    fn wanted(&self, r: &Run, lows: &[u64]) -> bool {
        let waits = matches!(r.step, Step::Waiting);
        matches!(r.step, Step::Reading(_))
            || lows
                .iter()
                .any(|&low| r.holds(low) || (waits && low == r.low))
            || self
                .runs
                .iter()
                .any(|before| before.end() == r.low && lows.iter().any(|&low| before.holds(low)))
    }

    /// Opens a run at `low` unless one begins there.
    fn open(&mut self, low: u64) {
        if !self.runs.iter().any(|r| r.low == low) {
            self.runs.push_back(Run {
                low,
                step: Step::Waiting,
            });
        }
    }

    /// Takes in the run being read, once its answer has come.
    fn take_in(&mut self) {
        for r in &mut self.runs {
            if let Step::Reading(fetching) = &r.step
                && let Some(answer) = fetching.poll()
            {
                r.step = Step::Read(answer.map_err(|e| storage(&e)));
            }
        }
    }

    /// Asks the log for the first run waiting, unless a run is being read: `bytes` of payload from
    /// its first index, at most. The owner's waker is woken once it is in.
    fn read_next<F: BlockFile + 'static>(
        &mut self,
        group: &GroupLog<F>,
        (fetched, bytes): (&Cell<Option<Fetched>>, u64),
    ) {
        let Some(waker) = self.waker.as_ref() else {
            return;
        };
        if self.runs.iter().any(|r| matches!(r.step, Step::Reading(_))) {
            return;
        }
        let Some(r) = self
            .runs
            .iter_mut()
            .find(|r| matches!(r.step, Step::Waiting))
        else {
            return;
        };
        let into = fetched.take().unwrap_or_default();
        let read = group.bounds().and_then(|(_, last)| {
            group.fetch_waking(
                r.low,
                last.saturating_add(1),
                Page::payload(bytes),
                into,
                waker.clone(),
            )
        });
        r.step = match read {
            Ok(fetching) => Step::Reading(fetching),
            Err(e) => Step::Read(Err(storage(&e))),
        };
    }
}

impl<F: BlockFile + 'static> std::fmt::Debug for GroupStore<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupStore")
            .field("group", &self.group)
            .field("writes", &self.writes.len())
            .field("queued", &self.queued.len())
            .finish_non_exhaustive()
    }
}

/// Why a group was not claimed.
#[derive(Debug, thiserror::Error)]
pub enum ClaimError {
    /// The log found the group's acknowledged records damaged: the member is rebuilt under a new
    /// identity (`docs/durable.md` §5), and the group's records removed ([`GroupStore::remove`]).
    #[error("the group's acknowledged records are damaged")]
    Damaged,
    /// The log refused.
    #[error("the log: {0}")]
    Log(LogError),
}

/// What claims and writes a log's groups: the [`Log`] itself, or a [`LogOpener`] an owner holds on
/// another thread (focal 27 §15.8), which claims as the log does.
pub trait LogGroups<F: BlockFile + 'static> {
    /// The handle through which `group` is written and read: [`Log::group`].
    fn group(&self, group: u128) -> Result<GroupLog<F>, LogError>;
    /// The parameters the log runs with.
    fn config(&self) -> hyper_log::Config;
    /// Submits `update` for `group` and waits until it is durable: [`Log::write_waiting`].
    fn write_waiting(&self, group: u128, update: Update) -> Result<(), LogError>;
}

impl<F: BlockFile + 'static> LogGroups<F> for Log<F> {
    fn group(&self, group: u128) -> Result<GroupLog<F>, LogError> {
        Log::group(self, group)
    }
    fn config(&self) -> hyper_log::Config {
        Log::config(self)
    }
    fn write_waiting(&self, group: u128, update: Update) -> Result<(), LogError> {
        Log::write_waiting(self, group, update)
    }
}

impl<F: BlockFile + 'static> LogGroups<F> for LogOpener<F> {
    fn group(&self, group: u128) -> Result<GroupLog<F>, LogError> {
        LogOpener::group(self, group)
    }
    fn config(&self) -> hyper_log::Config {
        LogOpener::config(self)
    }
    fn write_waiting(&self, group: u128, update: Update) -> Result<(), LogError> {
        LogOpener::write_waiting(self, group, update)
    }
}

impl<F: BlockFile + 'static> GroupStore<F> {
    /// Claims `group` of `log`: from here the group is written through this store alone.
    pub fn claim(log: &impl LogGroups<F>, group: u128) -> Result<Self, ClaimError> {
        match log.group(group) {
            Ok(handle) => Ok(Self::new(handle, log.config().segment_bytes)),
            Err(LogError::Damaged(_)) => Err(ClaimError::Damaged),
            Err(error) => Err(ClaimError::Log(error)),
        }
    }

    /// Removes every record of `group` from `log`: a damaged group's, before a member under a new
    /// identity opens in its place.
    pub fn remove(log: &impl LogGroups<F>, group: u128) -> Result<(), LogError> {
        log.write_waiting(
            group,
            Update {
                remove: true,
                ..Update::default()
            },
        )
    }

    /// A store over `group`'s handle, reading at most `page` bytes at once.
    pub fn new(group: GroupLog<F>, page: u64) -> Self {
        let reads_untold = group.reads_untold();
        Self {
            group,
            page,
            fetched: Cell::new(None),
            writes: VecDeque::new(),
            queued: VecDeque::new(),
            waker: None,
            catching: RefCell::new(CatchUp::default()),
            reads_untold,
        }
    }

    /// The handle.
    pub fn handle(&self) -> &GroupLog<F> {
        &self.group
    }

    /// Waits until the oldest write out is answered whole: every part of it the log took, and
    /// the parts behind them sent as answers free room. False when no part of it is with the
    /// log, so no answer is coming to wait for; its answer is then taken by the next poll. The
    /// wait is the log's own (`GroupLog::wait`), once for each part, and a part's refusal
    /// answers the write.
    pub fn wait(&mut self) -> bool {
        loop {
            self.send_queued();
            let Some(front) = self.writes.front_mut() else {
                return false;
            };
            if front.answered >= front.total {
                return true;
            }
            if front.answered >= front.sent {
                return false;
            }
            let Some(answer) = self.group.wait() else {
                return false;
            };
            let answer = answer.map_err(|e| fault(&e));
            let refused = answer.is_err();
            front.answer(answer);
            if refused {
                self.drop_queued();
            }
        }
    }

    /// The entries of `[low, high)`, as many as `max_bytes` of what they take in a message admit,
    /// and one at least: the core's page (`hyper_raft::Storage::entries`), chosen by the log.
    fn fetch(&self, low: u64, high: u64, max_bytes: u64) -> Result<Fetched, StorageError> {
        let into = self.fetched.take().unwrap_or_default();
        let page = Page {
            max_bytes,
            per_entry: IN_MESSAGE_BEYOND_STORED,
        };
        self.group
            .fetch(low, high, page, into)
            .map_err(|e| storage(&e))
    }

    /// Keeps `fetched` for the next read when it held no more than a page.
    fn keep(&self, fetched: Fetched) {
        let held = fetched.iter().fold(0u64, |sum, (_, bytes)| {
            sum.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
        });
        if held <= self.page {
            self.fetched.set(Some(fetched));
        }
    }

    /// Whether a read from `low` is a member's catch-up, read off the replica's thread
    /// ([`LogStore::catch_up`]): the replica has called with its waker, the log reads untold, `low`
    /// is at or before the mark, so no read of the replica's own wants it, and the handle does not
    /// hold it in memory.
    fn catches_up(&self, low: u64) -> bool {
        self.reads_untold
            && self
                .catching
                .try_borrow()
                .is_ok_and(|c| c.waker.is_some() && low <= c.mark)
            && !self.group.holds(low)
    }

    /// The entries of `[low, high)` within `max_bytes`, as [`LogStore::entries`] gives them, for a
    /// member catching up: from the read run that holds `low`, the run after it opened; until one
    /// does, `LogTemporarilyUnavailable`, a run at `low` asked or waiting.
    fn caught_up(
        &self,
        (low, high, max_bytes): (u64, u64, u64),
        into: &mut Vec<Entry>,
    ) -> Result<(), StorageError> {
        let mut c = self
            .catching
            .try_borrow_mut()
            .map_err(|_| StorageError::Other("a read of the store inside another"))?;
        c.take_in();
        let read = (&self.fetched, self.page);
        let Some(at) = c.runs.iter().position(|r| r.holds(low)) else {
            c.open(low);
            c.read_next(&self.group, read);
            return Err(StorageError::LogTemporarilyUnavailable);
        };
        let (first, fetched) = match c.runs.get(at).map(|r| (r.low, &r.step)) {
            Some((first, Step::Read(Ok(fetched)))) => (first, fetched),
            Some((_, Step::Read(Err(e)))) => {
                let e = *e;
                let _ = c.runs.remove(at);
                return Err(e);
            }
            _ => return Err(StorageError::LogTemporarilyUnavailable),
        };
        let end = first.saturating_add(u64::try_from(fetched.len()).unwrap_or(u64::MAX));
        let before = into.len();
        if self.decode_page(fetched, first, (low, high, max_bytes), into)? {
            // The next run, while this one is sent: what a member's next read from the file is.
            if end <= c.mark && !self.group.holds(end) {
                c.open(end);
            }
            c.read_next(&self.group, read);
            return Ok(());
        }
        // Read before the log's entries there were replaced: read again.
        into.truncate(before);
        let _ = c.runs.remove(at);
        c.open(low);
        c.read_next(&self.group, read);
        Err(StorageError::LogTemporarilyUnavailable)
    }

    /// Decodes into `into` the entries of `[low, high)` that `fetched`, read from `first`, holds:
    /// as many as `max_bytes` admit by the core's count, one at least, as [`LogStore::entries`]
    /// gives them. False when the last of them is not the entry the handle holds at its index now:
    /// the run was read before the log's entries there were replaced (a leader that stepped down,
    /// had its log cut and led again while it was read), and by Raft's log matching the last one
    /// agreeing is enough for every entry before it.
    fn decode_page(
        &self,
        fetched: &Fetched,
        first: u64,
        (low, high, max_bytes): (u64, u64, u64),
        into: &mut Vec<Entry>,
    ) -> Result<bool, StorageError> {
        let page = Page {
            max_bytes,
            per_entry: IN_MESSAGE_BEYOND_STORED,
        };
        let skip = usize::try_from(low.saturating_sub(first)).unwrap_or(usize::MAX);
        let mut total = 0u64;
        let mut last = None;
        for (index, (term, bytes)) in (low..high).zip((skip..).map_while(|i| fetched.get(i))) {
            total =
                total.saturating_add(page.count(u64::try_from(bytes.len()).unwrap_or(u64::MAX)));
            if last.is_some() && total > max_bytes {
                break;
            }
            let entry = decode_entry(index, term, bytes)
                .ok_or(StorageError::Other("an entry does not decode"))?;
            into.try_reserve(1)
                .map_err(|_| StorageError::Other("no memory for the entries asked"))?;
            let mut copy = Entry::default();
            entry.copy_into(&mut copy);
            into.push(copy);
            last = Some((index, term));
        }
        let (index, term) = last.ok_or(StorageError::Unavailable)?;
        Ok(self.group.term(index).is_ok_and(|held| held == term))
    }

    /// Sends the parts waiting while the handle has room.
    fn send_queued(&mut self) {
        while self.group.has_room() {
            let Some(part) = self.queued.pop_front() else {
                return;
            };
            let Some(owner) = self.writes.iter_mut().find(|w| w.sent < w.total) else {
                return;
            };
            owner.sent = owner.sent.saturating_add(1);
            let sent = match &self.waker {
                Some(waker) => self.group.submit_waking(part, waker.clone()),
                None => self.group.submit(part),
            };
            if let Err(error) = sent {
                // Never sent: answered here, and nothing behind it goes.
                owner.answer(Err(fault(&error)));
                self.drop_queued();
                return;
            }
        }
    }

    /// Parts not yet sent behind a refused one are never sent: each is answered `Behind`.
    fn drop_queued(&mut self) {
        while self.queued.pop_front().is_some() {
            if let Some(owner) = self.writes.iter_mut().find(|w| w.sent < w.total) {
                owner.sent = owner.sent.saturating_add(1);
                owner.answer(Err(Fault::Behind));
            }
        }
    }
}

/// The log's error as the core reads it.
fn storage(e: &LogError) -> StorageError {
    match e {
        LogError::Compacted { .. } => StorageError::Compacted,
        LogError::Unavailable { .. } => StorageError::Unavailable,
        // The log's queue of fetches was full: the core asks again when it next sends.
        LogError::Busy => StorageError::LogTemporarilyUnavailable,
        _ => StorageError::Other("the log failed"),
    }
}

/// The log's refusal or failure as a write's fault.
fn fault(e: &LogError) -> Fault {
    match e {
        LogError::Backlog(_) => Fault::Room("the group's retained bound"),
        LogError::Full => Fault::Room("the log's segments"),
        LogError::TooManyGroups(_) => Fault::Room("the log's groups"),
        LogError::Busy => Fault::Room("the log's queue"),
        LogError::Behind(_) => Fault::Behind,
        LogError::Fenced => Fault::Failed("the log is fenced"),
        LogError::Closed => Fault::Failed("the log is closed"),
        LogError::Disk(_) => Fault::Failed("the device failed"),
        LogError::TooLarge(_) => Fault::Failed("a record larger than a frame"),
        LogError::Damaged(_) => Fault::Failed("the group is damaged"),
        _ => Fault::Failed("the log refused a write the group cannot take"),
    }
}

/// An entry's bytes in the log: its kind, its context's length, its context and its data.
pub fn encode_entry(entry: &Entry) -> Option<Vec<u8>> {
    let context_len = u32::try_from(entry.context.len()).ok()?;
    let len = ENTRY_OVERHEAD
        .checked_add(entry.context.len())?
        .checked_add(entry.data.len())?;
    let mut out = Vec::with_capacity(len);
    out.push(entry.entry_type.byte());
    out.extend_from_slice(&context_len.to_le_bytes());
    out.extend_from_slice(&entry.context);
    out.extend_from_slice(&entry.data);
    Some(out)
}

/// The entry at `index` of `term` whose bytes are `bytes`, read where they lie.
pub fn decode_entry(index: u64, term: u64, bytes: &[u8]) -> Option<EntryRef<'_>> {
    let (&kind, rest) = bytes.split_first()?;
    let kind = EntryType::from_byte(kind)?;
    let (len, rest) = rest.split_at_checked(4)?;
    let len = usize::try_from(u32::from_le_bytes(len.try_into().ok()?)).ok()?;
    let (context, data) = rest.split_at_checked(len)?;
    Some(EntryRef {
        index,
        term,
        kind,
        context,
        data,
    })
}

fn hard_to_log(h: HardState) -> hyper_log::HardState {
    hyper_log::HardState {
        term: h.term,
        vote: h.vote,
        commit: h.commit,
        // The core's promise (core step R-8): the last entry its member may answer for in memory.
        held: (h.held_term != 0).then_some(hyper_log::Start {
            index: h.held_index,
            term: h.held_term,
        }),
    }
}

/// `write` as one update of the log: the one copy of what the core lent.
fn update_of(write: &Write<'_>) -> Result<Update, Fault> {
    let encoded = |e: &Entry| encode_entry(e).ok_or(Fault::Failed("an entry past what u32 counts"));
    let no_memory = |_| Fault::Failed("no memory for a write");
    // Each list is reserved at its exact length before it is filled: collected through a
    // `Result`, an iterator gives no lower bound, and the list grew by reallocation (traced at
    // 0.005 an entry on mantle's replica workload).
    let entries = match write.entries {
        Some(Entries { first, entries }) => {
            let mut encoded_entries = Vec::new();
            encoded_entries
                .try_reserve_exact(entries.len())
                .map_err(no_memory)?;
            for e in entries {
                encoded_entries.push(hyper_log::Entry {
                    term: e.term,
                    bytes: encoded(e)?,
                });
            }
            Some(hyper_log::Entries {
                first,
                entries: encoded_entries,
            })
        }
        None => None,
    };
    let mut proposals = Vec::new();
    proposals
        .try_reserve_exact(write.proposals.len())
        .map_err(no_memory)?;
    for p in write.proposals {
        proposals.push(Proposal {
            index: p.index,
            term: p.term,
            bytes: encoded(p)?,
        });
    }
    Ok(Update {
        start: write.start.map(|p| Start {
            index: p.index,
            term: p.term,
        }),
        entries,
        hard_state: write.hard_state.map(hard_to_log),
        proposals,
        released: write.released,
        remove: false,
    })
}

impl<F: BlockFile + 'static> LogStore for GroupStore<F> {
    type Hold = std::convert::Infallible;

    fn held(&self) -> Option<&Self::Hold> {
        None
    }

    fn release(&mut self, met: &Self::Hold) {
        match *met {}
    }

    fn depth(&self) -> usize {
        self.group.depth()
    }

    fn view(&self) -> Result<StoreView, Fault> {
        let Some(view) = self.group.view().map_err(|e| fault(&e))? else {
            return Ok(StoreView::default());
        };
        Ok(StoreView {
            start: Point {
                index: view.start.index,
                term: view.start.term,
            },
            last: view.last,
            hard_state: view
                .hard_state
                .map_or_else(HardState::default, |h| HardState {
                    term: h.term,
                    vote: h.vote,
                    commit: h.commit,
                    // A promise restored from a persist row is the last index there is, which the
                    // core takes as the last it counts (R-8).
                    held_index: h.held.map_or(0, |held| held.index),
                    held_term: h.held.map_or(0, |held| held.term),
                }),
            health: view.uncertain.map_or(Health::Whole, |mark| {
                Health::Marked(Point {
                    index: mark.index,
                    term: mark.term,
                })
            }),
        })
    }

    fn bounds(&self) -> Result<(Point, u64), StorageError> {
        let (start, last) = self.group.bounds().map_err(|e| storage(&e))?;
        Ok((
            Point {
                index: start.index,
                term: start.term,
            },
            last,
        ))
    }

    fn term(&self, index: u64) -> Result<u64, StorageError> {
        match self.group.term(index) {
            Ok(term) => Ok(term),
            // A group the log holds nothing of starts after index 0, of term 0.
            Err(LogError::Unavailable { .. }) if index == 0 => Ok(0),
            Err(e) => Err(storage(&e)),
        }
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: &mut Vec<Entry>,
    ) -> Result<(), StorageError> {
        if low >= high {
            return Ok(());
        }
        let fetched = self.fetch(low, high, max_bytes)?;
        into.try_reserve_exact(fetched.len())
            .map_err(|_| StorageError::Other("no memory for the entries asked"))?;
        let decoded = (low..)
            .zip(fetched.iter())
            .try_for_each(|(index, (term, bytes))| {
                let entry = decode_entry(index, term, bytes)
                    .ok_or(StorageError::Other("an entry does not decode"))?;
                let mut copy = Entry::default();
                entry.copy_into(&mut copy);
                into.push(copy);
                Ok(())
            });
        self.keep(fetched);
        decoded
    }

    fn entries_or_catch_up(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: &mut Vec<Entry>,
    ) -> Result<(), StorageError> {
        if low < high && self.catches_up(low) {
            return self.caught_up((low, high, max_bytes), into);
        }
        self.entries(low, high, max_bytes, into)
    }

    fn visit(
        &self,
        low: u64,
        high: u64,
        page: u64,
        visit: &mut dyn FnMut(EntryRef<'_>) -> bool,
    ) -> Result<(), StorageError> {
        let page = page.min(self.page);
        let mut next = low;
        while next < high {
            let fetched = self.fetch(next, high, page)?;
            if fetched.is_empty() {
                self.keep(fetched);
                return Err(StorageError::Unavailable);
            }
            let mut stopped = false;
            let mut failed = false;
            for (term, bytes) in fetched.iter() {
                let Some(entry) = decode_entry(next, term, bytes) else {
                    failed = true;
                    break;
                };
                if visit(entry) {
                    stopped = true;
                    break;
                }
                next = next.saturating_add(1);
            }
            self.keep(fetched);
            if failed {
                return Err(StorageError::Other("an entry does not decode"));
            }
            if stopped {
                return Ok(());
            }
        }
        Ok(())
    }

    fn proposals(&self, into: &mut Vec<Entry>) -> Result<(), StorageError> {
        let Some(view) = self.group.view().map_err(|e| storage(&e))? else {
            return Ok(());
        };
        for proposal in &view.proposals {
            let entry = decode_entry(proposal.index, proposal.term, &proposal.bytes)
                .ok_or(StorageError::Other("a proposal does not decode"))?;
            let mut copy = Entry::default();
            entry.copy_into(&mut copy);
            into.push(copy);
        }
        Ok(())
    }

    fn released(&self) -> Result<u64, StorageError> {
        let view = self.group.view().map_err(|e| storage(&e))?;
        Ok(view.map_or(0, |view| view.released))
    }

    fn room(&self) -> bool {
        self.queued.is_empty() && self.group.has_room()
    }

    fn submit(&mut self, write: &Write<'_>, waker: &Waker) -> Result<(), Fault> {
        let update = update_of(write)?;
        let parts = self.group.parts(update).map_err(|e| fault(&e))?;
        self.writes.push_back(Parts {
            total: parts.len(),
            sent: 0,
            answered: 0,
            outcome: Ok(()),
        });
        self.queued.extend(parts);
        match &self.waker {
            Some(held) if held.will_wake(waker) => {}
            _ => self.waker = Some(waker.clone()),
        }
        self.send_queued();
        Ok(())
    }

    fn poll(&mut self) -> Option<Result<(), Fault>> {
        loop {
            let front = self.writes.front_mut()?;
            if front.answered >= front.total {
                return self.writes.pop_front().map(|parts| parts.outcome);
            }
            if front.answered >= front.sent {
                self.send_queued();
                return None;
            }
            let Some(answer) = self.group.poll() else {
                self.send_queued();
                return None;
            };
            let answer = answer.map_err(|e| fault(&e));
            let refused = answer.is_err();
            front.answer(answer);
            if refused {
                self.drop_queued();
            }
        }
    }

    fn write_now(&mut self, write: &Write<'_>) -> Result<(), Fault> {
        let update = update_of(write)?;
        let parts = self.group.parts(update).map_err(|e| fault(&e))?;
        for part in parts {
            self.group.write(part).map_err(|e| fault(&e))?;
        }
        Ok(())
    }

    /// The handle keeps the entries the replica will read back, whatever the group's share of the
    /// log's cache (`GroupLog::keep_after`): a leader's entries are durable at its own write and
    /// committed at its followers' answers, and by then the core has let them go, so its apply read
    /// each back, past the handle's share (64 KiB a group in focal: 64 MiB over 1,024 groups),
    /// through the log's owner and to the device, queued behind its flushes. focal's session owner
    /// spent 57% of its time waiting on those reads at 5,000 writes a second (2026-10-12). A resend
    /// to a member that has yet to acknowledge them reads them back the same way. The handle took
    /// each entry's bytes back with its write's answer, so nothing is copied to keep them.
    fn keep_after(&mut self, index: u64) {
        self.group.keep_after(index);
        let catching = self.catching.get_mut();
        catching.mark = catching.mark.max(index);
    }

    fn kept_bytes(&self) -> u64 {
        self.group.kept_bytes()
    }

    /// A page of what a member the leader catches up lacks that the handle no longer holds is read
    /// from the log's file off the replica's thread, by hyper-log's fetcher, off the log's device,
    /// so no write of the leader's and no append to another member waits on it (`CatchUp`).
    fn catch_up(&mut self, waker: &Waker, lows: &[u64], read: &mut Vec<u64>) -> bool {
        let c = self.catching.get_mut();
        match &c.waker {
            Some(held) if held.will_wake(waker) => {}
            _ => c.waker = Some(waker.clone()),
        }
        c.take_in();
        let mut at = 0;
        while let Some(r) = c.runs.get(at) {
            if c.wanted(r, lows) {
                at = at.saturating_add(1);
                continue;
            }
            if let Some(Run {
                step: Step::Read(Ok(fetched)),
                ..
            }) = c.runs.remove(at)
                && self.fetched.get_mut().is_none()
            {
                *self.fetched.get_mut() = Some(fetched);
            }
        }
        c.read_next(&self.group, (&self.fetched, self.page));
        read.extend(lows.iter().filter(|&&low| {
            c.runs
                .iter()
                .any(|r| matches!(r.step, Step::Read(_)) && r.holds(low))
        }));
        c.runs
            .iter()
            .any(|r| matches!(r.step, Step::Waiting | Step::Reading(_)))
    }
}
