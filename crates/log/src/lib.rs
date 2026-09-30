//! The Raft log (docs/design/raft-log.md): one log per metadata device, shared by every range
//! replica on it, so that one flush commits every group's writes.
//!
//! A replica submits its `Ready`'s entries, hard state, start and proposals as one [`Update`];
//! the writer thread gathers every update that arrived while the last flush ran into one
//! frame, writes and flushes it, and only then publishes it to readers and answers. Reads
//! serve the view focal-raft's `Storage` needs: bounds, terms and entries.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]

mod error;
pub mod format;
mod recover;
mod state;
mod writer;

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::JoinHandle;

use mantle_disk::block::BlockFile;
use mantle_disk::buf::{Alignment, Pool};

pub use error::LogError;
pub use format::{HardState, Start};

use state::{Group, Live, Place};

/// The log's parameters. Each comes from the node: the segment size from the device's
/// geometry, the rest from the measured append rate and the replicas placed on the device
/// (docs/design/raft-log.md §4–§5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// Bytes of each segment: a multiple of the file's alignment, at least four blocks.
    pub segment_bytes: u64,
    /// Segments the file may grow to.
    pub max_segments: u32,
    /// Groups the log holds at most.
    pub max_groups: usize,
    /// Entries one group may retain.
    pub group_entries: u64,
    /// Payload bytes of entries one group may retain.
    pub group_bytes: u64,
    /// Payload bytes of recent entries one group keeps in memory.
    pub group_cache: u64,
    /// Submissions the queue admits, and their bytes: two batches' worth [research/11 §4].
    pub queue_submissions: usize,
    pub queue_bytes: u64,
}

/// Entries to write from `first` on, replacing any the group holds at or after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entries {
    pub first: u64,
    pub entries: Vec<Entry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub term: u64,
    pub bytes: Arc<[u8]>,
}

/// An entry the replica approved by itself on the fast track (07 §1.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    pub index: u64,
    pub term: u64,
    pub bytes: Arc<[u8]>,
}

/// What one replica makes durable at once: one `Ready`'s worth, applied in this order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Update {
    /// The log now starts after this: the engine made the entries before it durable, or a
    /// snapshot was installed there.
    pub start: Option<Start>,
    pub entries: Option<Entries>,
    pub hard_state: Option<HardState>,
    pub proposals: Vec<Proposal>,
    /// The replica left this device: every record of the group is dead. Nothing else may
    /// come with it.
    pub remove: bool,
}

/// A group's durable state, as focal-raft's `Storage` reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct View {
    pub start: Start,
    /// The last entry's index, the start's when none is held.
    pub last: u64,
    pub hard_state: Option<HardState>,
    pub proposals: Vec<Proposal>,
    /// Entries the log may lack, through this mark's index and of terms up to its term,
    /// that a frame no longer readable held (docs/design/raft-log.md §6). Until the log
    /// again reaches the index, or holds an entry of a later term, the replica takes no part
    /// in elections: it may have acknowledged what it no longer holds.
    pub uncertain: Option<Start>,
}

/// What opening found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Recovery {
    /// Frames replayed.
    pub frames: u64,
    /// Groups whose records do not run unbroken, or whose lost last frame held what its
    /// persist record cannot restore: the log serves none of them, and each recovers from
    /// its peers.
    pub damaged: Vec<u128>,
    /// Groups restored from the persist record of a last frame that no longer reads.
    pub restored: Vec<u128>,
}

/// An update's answer, once it is durable or refused.
#[derive(Debug)]
pub struct Pending {
    answer: Receiver<Result<(), LogError>>,
}

impl Pending {
    pub fn wait(self) -> Result<(), LogError> {
        self.answer.recv().map_err(|_| LogError::Closed)?
    }

    /// The answer if it has come.
    pub fn poll(&self) -> Option<Result<(), LogError>> {
        match self.answer.try_recv() {
            Ok(answer) => Some(answer),
            Err(std::sync::mpsc::TryRecvError::Empty) => None,
            Err(std::sync::mpsc::TryRecvError::Disconnected) => Some(Err(LogError::Closed)),
        }
    }
}

/// An update on its way to the writer.
struct Submission {
    group: u128,
    update: Update,
    /// An uncertainty mark written with the update: only by the restore at open.
    uncertain: Option<Start>,
    bytes: u64,
    reply: SyncSender<Result<(), LogError>>,
}

/// Where the next frame goes.
#[derive(Debug, Clone, Copy)]
struct Head {
    slot: u32,
    incarnation: u64,
    nonce: u64,
    /// File offset of the next frame.
    offset: u64,
}

/// Which segment slots are live, oldest first, and which are free.
#[derive(Debug, Default)]
struct Segments {
    /// Each slot's incarnation, 0 for one never used, and its nonce.
    incarnation: Vec<u64>,
    nonce: Vec<u64>,
    /// Live segments' slots, the tail first and the head last.
    live: VecDeque<u32>,
    /// Free slots, each with the sequence of the first frame that recorded a tail past it:
    /// reused only once that frame is durable (docs/design/raft-log.md §5).
    free: VecDeque<(u32, u64)>,
}

struct State {
    groups: HashMap<u128, Group>,
    /// Groups recovery found damaged: served to no one (`Recovery::damaged`).
    damaged: std::collections::HashSet<u128>,
    live: Live,
    segments: Segments,
    head: Head,
    next_sequence: u64,
    next_incarnation: u64,
    /// The sequence of the last frame flushed.
    durable: u64,
    /// The tail the last frame flushed names.
    durable_tail: u64,
}

impl State {
    fn tail_incarnation(&self) -> u64 {
        self.segments
            .live
            .front()
            .and_then(|&slot| self.segments.incarnation.get(usize::try_from(slot).ok()?))
            .copied()
            .unwrap_or(self.head.incarnation)
    }
}

/// The room submissions hold until the writer answers them: those waiting to be taken, those
/// held for a later frame, and those in the frame being written (audit S03).
#[derive(Debug, Default)]
struct Queue {
    submissions: usize,
    bytes: u64,
    /// Submissions not yet answered, by group, each at most [`GROUP_SUBMISSIONS`].
    groups: HashMap<u128, usize>,
}

/// Payload bytes one frame holds: a segment less its header block and the frame's header.
pub(crate) fn frame_room(config: &Config, align: Alignment) -> Result<usize, LogError> {
    let block = u64::try_from(align.get()).map_err(|_| LogError::Config("block"))?;
    config
        .segment_bytes
        .checked_sub(block)
        .and_then(|room| usize::try_from(room).ok())
        .and_then(|room| room.checked_sub(format::FRAME_HEADER_LEN))
        .ok_or(LogError::Config("a segment holds no frame"))
}

/// Submissions one group may have unanswered: the queue's two batches' worth
/// [research/11 §4] for a group, which has one update in each frame. A hot group then waits
/// for its own room and never takes the others'.
const GROUP_SUBMISSIONS: usize = 2;

struct Shared<F> {
    file: F,
    id: u128,
    config: Config,
    align: Alignment,
    /// Buffers for reading entries back.
    pool: Pool,
    state: RwLock<State>,
    queue: Mutex<Queue>,
    /// Signalled when the writer takes submissions off the queue, and when the log fences.
    room: Condvar,
    fenced: AtomicBool,
    /// Submissions sent to the writer so far, counted just before each is sent.
    submitted: AtomicU64,
    /// Frames written and flushed since the log opened, and the updates they carried.
    frames: AtomicU64,
    updates: AtomicU64,
}

impl<F: BlockFile> Shared<F> {
    fn read_state(&self) -> Result<std::sync::RwLockReadGuard<'_, State>, LogError> {
        self.state.read().map_err(|_| LogError::Fenced)
    }

    /// Gives back the room of a submission of `group` answered or never sent, and wakes the
    /// submitters waiting for room.
    fn release(&self, group: u128, bytes: u64) {
        if let Ok(mut queue) = self.queue.lock() {
            queue.submissions = queue.submissions.saturating_sub(1);
            queue.bytes = queue.bytes.saturating_sub(bytes);
            if let Some(own) = queue.groups.get_mut(&group) {
                *own = own.saturating_sub(1);
                if *own == 0 {
                    queue.groups.remove(&group);
                }
            }
        }
        self.room.notify_all();
    }
}

pub struct Log<F: BlockFile + 'static> {
    shared: Arc<Shared<F>>,
    sender: Option<SyncSender<Submission>>,
    writer: Option<JoinHandle<()>>,
}

impl<F: BlockFile + 'static> Log<F> {
    /// Formats a new log `id` in `file`, which must be empty.
    pub fn create(file: F, config: Config, id: u128) -> Result<Self, LogError> {
        let state = recover::create(&file, &config, id)?;
        Self::start(file, config, id, state)
    }

    /// Opens log `id` in `file` and recovers it (docs/design/raft-log.md §6). What a last
    /// frame that no longer reads held, as its persist record says, is written back before
    /// the log serves anyone.
    pub fn open(file: F, config: Config, id: u128) -> Result<(Self, Recovery), LogError> {
        let (state, recovery, restores) = recover::open(&file, &config, id)?;
        let log = Self::start(file, config, id, state)?;
        let mut pending = Vec::with_capacity(restores.len());
        for r in restores {
            pending.push(log.send(r.group, r.update, r.uncertain, true)?);
        }
        for p in pending {
            p.wait()?;
        }
        Ok((log, recovery))
    }

    fn start(file: F, config: Config, id: u128, state: State) -> Result<Self, LogError> {
        let align = file.alignment();
        let largest = usize::try_from(config.segment_bytes)
            .map_err(|_| LogError::Config("segment larger than memory"))?;
        let shared = Arc::new(Shared {
            file,
            id,
            config,
            align,
            pool: Pool::new(align, largest.saturating_mul(2), largest),
            state: RwLock::new(state),
            queue: Mutex::new(Queue::default()),
            room: Condvar::new(),
            fenced: AtomicBool::new(false),
            submitted: AtomicU64::new(0),
            frames: AtomicU64::new(0),
            updates: AtomicU64::new(0),
        });
        let capacity = config.queue_submissions.max(1);
        let (sender, receiver) = sync_channel(capacity);
        let writer = writer::Writer::new(Arc::clone(&shared), receiver)?;
        let handle = std::thread::Builder::new()
            .name("mantle-log".into())
            .spawn(move || writer.run())
            .map_err(|_| LogError::Closed)?;
        Ok(Self {
            shared,
            sender: Some(sender),
            writer: Some(handle),
        })
    }

    /// Submits `update` for `group`: refused at once when the queue is full, otherwise
    /// answered through the returned handle once it is durable.
    pub fn submit(&self, group: u128, update: Update) -> Result<Pending, LogError> {
        self.send(group, update, None, false)
    }

    /// Submits `update` for `group`, waiting for room in the queue rather than refusing. A
    /// replica cannot have a `Ready` refused, so the log holds it back instead
    /// (docs/design/replica.md §3). The writer frees room with every batch it takes, and a
    /// fence wakes every waiter, so the wait lasts no longer than the writer's progress.
    pub fn submit_waiting(&self, group: u128, update: Update) -> Result<Pending, LogError> {
        self.send(group, update, None, true)
    }

    fn send(
        &self,
        group: u128,
        update: Update,
        uncertain: Option<Start>,
        wait: bool,
    ) -> Result<Pending, LogError> {
        let bytes = update_bytes(&update);
        {
            let config = &self.shared.config;
            let mut queue = self.shared.queue.lock().map_err(|_| LogError::Fenced)?;
            loop {
                if self.shared.fenced.load(Ordering::Acquire) {
                    return Err(LogError::Fenced);
                }
                let submissions = queue.submissions.checked_add(1).ok_or(LogError::Busy)?;
                let total = queue.bytes.checked_add(bytes).ok_or(LogError::Busy)?;
                let own = queue.groups.get(&group).copied().unwrap_or(0);
                // A submission larger than the byte bound is taken into an empty queue.
                let room = submissions <= config.queue_submissions
                    && (queue.bytes == 0 || total <= config.queue_bytes)
                    && own < GROUP_SUBMISSIONS;
                if room {
                    queue.submissions = submissions;
                    queue.bytes = total;
                    queue.groups.insert(group, own.saturating_add(1));
                    break;
                }
                if !wait {
                    return Err(LogError::Busy);
                }
                queue = self.shared.room.wait(queue).map_err(|_| LogError::Fenced)?;
            }
        }
        let sender = self.sender.as_ref().ok_or(LogError::Closed)?;
        let (reply, answer) = sync_channel(1);
        self.shared.submitted.fetch_add(1, Ordering::AcqRel);
        let submission = Submission {
            group,
            update,
            uncertain,
            bytes,
            reply,
        };
        match sender.try_send(submission) {
            Ok(()) => Ok(Pending { answer }),
            Err(e) => {
                self.shared.release(group, bytes);
                Err(match e {
                    TrySendError::Full(_) => LogError::Busy,
                    TrySendError::Disconnected(_) => LogError::Closed,
                })
            }
        }
    }

    /// The parameters the log runs with.
    pub fn config(&self) -> Config {
        self.shared.config
    }

    /// Payload bytes one frame holds: an update no longer than this fits a frame of its own.
    pub fn frame_room(&self) -> Result<usize, LogError> {
        frame_room(&self.shared.config, self.shared.align)
    }

    /// The most bytes one entry may hold and still fit a frame alone.
    pub fn entry_room(&self) -> Result<usize, LogError> {
        let one = format::encoded_len(&format::Record::Entries {
            group: 0,
            first: 0,
            entries: &[(0, &[])],
        })
        .ok_or(LogError::Config("an entry's record"))?;
        self.frame_room()?
            .checked_sub(one)
            .ok_or(LogError::Config("a frame holds no entry"))
    }

    /// `update` for `group` in parts that each fit one frame, in the order they apply: its
    /// start and entries first, its hard state and proposals last, as etcd's raft persists a
    /// ready's entries before its hard state, together only when the store writes them
    /// atomically. An update that fits is one part. Written in order, the parts leave the
    /// group as the update would, and a crash between them leaves entries whose ready was
    /// never done, so never acknowledged. `TooLarge` when one entry or proposal alone is
    /// more than a frame holds.
    pub fn parts(&self, group: u128, update: Update) -> Result<Vec<Update>, LogError> {
        let room = self.frame_room()?;
        let len_of =
            |u: &Update| writer::update_len(group, u).ok_or(LogError::TooLarge(usize::MAX));
        if update.remove || len_of(&update)? <= room {
            return Ok(vec![update]);
        }
        let record =
            |r: &format::Record<'_>| format::encoded_len(r).ok_or(LogError::TooLarge(usize::MAX));
        let Update {
            start,
            entries,
            hard_state,
            proposals,
            ..
        } = update;
        let mut parts = Vec::new();
        let mut part = Update {
            start,
            ..Update::default()
        };
        let mut used = len_of(&part)?;
        if let Some(e) = entries {
            let none = record(&format::Record::Entries {
                group,
                first: e.first,
                entries: &[],
            })?;
            let mut first = e.first;
            let mut held: Vec<Entry> = Vec::new();
            let mut held_len = none;
            for (index, entry) in (e.first..).zip(e.entries) {
                let cost = record(&format::Record::Entries {
                    group,
                    first: index,
                    entries: &[(entry.term, &entry.bytes)],
                })?
                .saturating_sub(none);
                if used.saturating_add(held_len).saturating_add(cost) > room {
                    if held.is_empty() && used == 0 {
                        return Err(LogError::TooLarge(none.saturating_add(cost)));
                    }
                    if !held.is_empty() {
                        part.entries = Some(Entries {
                            first,
                            entries: std::mem::take(&mut held),
                        });
                    }
                    parts.push(std::mem::take(&mut part));
                    used = 0;
                    held_len = none;
                    first = index;
                    if none.saturating_add(cost) > room {
                        return Err(LogError::TooLarge(none.saturating_add(cost)));
                    }
                }
                held.push(entry);
                held_len = held_len.saturating_add(cost);
            }
            // Entries of none still say where the group's entries end.
            part.entries = Some(Entries {
                first,
                entries: held,
            });
            used = used.saturating_add(held_len);
        }
        if let Some(state) = hard_state {
            let cost = record(&format::Record::HardState { group, state })?;
            if used.saturating_add(cost) > room {
                parts.push(std::mem::take(&mut part));
                used = 0;
            }
            part.hard_state = Some(state);
            used = used.saturating_add(cost);
        }
        for p in proposals {
            let cost = record(&format::Record::Proposal {
                group,
                index: p.index,
                term: p.term,
                bytes: &p.bytes,
            })?;
            if cost > room {
                return Err(LogError::TooLarge(cost));
            }
            if used.saturating_add(cost) > room {
                parts.push(std::mem::take(&mut part));
                used = 0;
            }
            part.proposals.push(p);
            used = used.saturating_add(cost);
        }
        parts.push(part);
        Ok(parts)
    }

    /// Submits `update` and waits until it is durable; refused at once when the queue is
    /// full.
    pub fn write(&self, group: u128, update: Update) -> Result<(), LogError> {
        self.submit(group, update)?.wait()
    }

    /// Submits `update`, waiting for room, and waits until it is durable.
    pub fn write_waiting(&self, group: u128, update: Update) -> Result<(), LogError> {
        self.submit_waiting(group, update)?.wait()
    }

    /// The groups the log holds.
    pub fn groups(&self) -> Result<Vec<u128>, LogError> {
        Ok(self.shared.read_state()?.groups.keys().copied().collect())
    }

    /// A group's durable state; `None` for a group the log holds nothing of.
    pub fn view(&self, group: u128) -> Result<Option<View>, LogError> {
        let state = self.shared.read_state()?;
        if state.damaged.contains(&group) {
            return Err(LogError::Damaged(
                "the group's acknowledged records are damaged; it recovers from its peers",
            ));
        }
        let Some(g) = state.groups.get(&group) else {
            return Ok(None);
        };
        Ok(Some(View {
            start: g.start,
            last: g.last().ok_or(LogError::Damaged("an index past u64"))?,
            hard_state: g.hard.map(|(h, _)| h),
            proposals: g
                .proposals
                .iter()
                .map(|(&index, p)| Proposal {
                    index,
                    term: p.term,
                    bytes: Arc::clone(&p.bytes),
                })
                .collect(),
            uncertain: g.uncertain.map(|(mark, _)| mark),
        }))
    }

    /// The term of `index`, which may be the start's.
    pub fn term(&self, group: u128, index: u64) -> Result<u64, LogError> {
        let state = self.shared.read_state()?;
        let g = state
            .groups
            .get(&group)
            .ok_or(LogError::Unavailable { group, index })?;
        if index < g.start.index {
            return Err(LogError::Compacted {
                group,
                first: g.start.index,
            });
        }
        g.term(index).ok_or(LogError::Unavailable { group, index })
    }

    /// The entries of `[low, high)`, as many as `max_bytes` of payload admit and one at
    /// least, reading from the file those no longer in memory.
    pub fn entries(
        &self,
        group: u128,
        low: u64,
        high: u64,
        max_bytes: u64,
    ) -> Result<Vec<Entry>, LogError> {
        // What to read, taken under the lock; the file is read outside it.
        let mut wanted = Vec::new();
        {
            let state = self.shared.read_state()?;
            let g = state
                .groups
                .get(&group)
                .ok_or(LogError::Unavailable { group, index: low })?;
            let first = g.first().ok_or(LogError::Damaged("an index past u64"))?;
            if low < first {
                return Err(LogError::Compacted { group, first });
            }
            let mut total = 0u64;
            for index in low..high {
                let slot = g
                    .slot(index)
                    .ok_or(LogError::Unavailable { group, index })?;
                total = total.saturating_add(u64::from(slot.len));
                if !wanted.is_empty() && total > max_bytes {
                    break;
                }
                wanted.push((index, slot.term, slot.place, slot.len, slot.cached.clone()));
            }
        }
        // Entries no longer in memory are read from the file, those whose blocks touch in one
        // read: an update's entries lie together in its frame, and a replica catching up asks
        // for runs of them (audit P07).
        let mut out = Vec::with_capacity(wanted.len());
        let mut rest = wanted.as_slice();
        while let Some(((index, term, place, len, cached), after)) = rest.split_first() {
            if let Some(bytes) = cached {
                out.push(Entry {
                    term: *term,
                    bytes: Arc::clone(bytes),
                });
                rest = after;
                continue;
            }
            let (begin, mut end) = self.span(place, *len).ok_or(LogError::Corrupt {
                group,
                index: *index,
            })?;
            let mut run = 1usize;
            for (_, _, next, next_len, next_cached) in after {
                let Some((b, e)) = self.span(next, *next_len) else {
                    break;
                };
                if next_cached.is_some() || next.slot != place.slot || b < begin || b > end {
                    break;
                }
                end = end.max(e);
                run = run.saturating_add(1);
            }
            let (these, later) = rest.split_at(run.min(rest.len()));
            let bytes = self.read_span(begin, end)?;
            for (index, term, place, len, _) in these {
                let skip = usize::try_from(place.offset.saturating_sub(begin)).map_err(|_| {
                    LogError::Corrupt {
                        group,
                        index: *index,
                    }
                })?;
                let found = bytes
                    .as_slice()
                    .get(skip..)
                    .and_then(|at| format::entry_at(at, group, *index))
                    .filter(|(t, _)| t == term)
                    .map(|(_, b)| Arc::<[u8]>::from(b));
                let bytes = match found {
                    Some(bytes) => bytes,
                    // Moved since its place was taken: looked up again.
                    None => self.read_entry(group, *index, *term, *place, *len)?,
                };
                out.push(Entry { term: *term, bytes });
            }
            rest = later;
        }
        Ok(out)
    }

    /// The block-aligned span of the file holding an entry of `len` payload bytes at `place`.
    fn span(&self, place: &Place, len: u32) -> Option<(u64, u64)> {
        let align = self.shared.align;
        let end = place
            .offset
            .checked_add(format::ENTRY_HEADER_BYTES)?
            .checked_add(u64::from(len))
            .and_then(|e| align.up_u64(e))?;
        Some((align.down_u64(place.offset), end))
    }

    /// The file's bytes `[begin, end)`, block-aligned, in a buffer from the pool.
    fn read_span(&self, begin: u64, end: u64) -> Result<mantle_disk::buf::PoolBuf<'_>, LogError> {
        let size = usize::try_from(end.saturating_sub(begin))
            .map_err(|_| LogError::Damaged("a read past usize"))?;
        let mut buf = self
            .shared
            .pool
            .take(size)
            .map_err(|e| LogError::Disk(e.into()))?;
        buf.set_len(size).map_err(|e| LogError::Disk(e.into()))?;
        self.shared.file.read_exact_at(buf.as_mut_slice(), begin)?;
        Ok(buf)
    }

    /// Reads one entry from the file, verified as `group`'s entry `index` of `term`. An entry
    /// relocated since its place was taken is looked up again once.
    fn read_entry(
        &self,
        group: u128,
        index: u64,
        term: u64,
        place: Place,
        len: u32,
    ) -> Result<Arc<[u8]>, LogError> {
        let mut place = place;
        for _ in 0..2 {
            if let Some(bytes) = self.read_at(group, index, term, place, len)? {
                return Ok(bytes);
            }
            let state = self.shared.read_state()?;
            let now = state
                .groups
                .get(&group)
                .and_then(|g| g.slot(index))
                .map(|s| s.place);
            match now {
                Some(p) if p != place => place = p,
                Some(_) => return Err(LogError::Corrupt { group, index }),
                None => return Err(LogError::Unavailable { group, index }),
            }
        }
        Err(LogError::Corrupt { group, index })
    }

    fn read_at(
        &self,
        group: u128,
        index: u64,
        term: u64,
        place: Place,
        len: u32,
    ) -> Result<Option<Arc<[u8]>>, LogError> {
        let align = self.shared.align;
        let begin = align.down_u64(place.offset);
        let end = place
            .offset
            .checked_add(format::ENTRY_HEADER_BYTES)
            .and_then(|e| e.checked_add(u64::from(len)))
            .and_then(|e| align.up_u64(e))
            .ok_or(LogError::Corrupt { group, index })?;
        let span = usize::try_from(end.saturating_sub(begin))
            .map_err(|_| LogError::Corrupt { group, index })?;
        let mut buf = self
            .shared
            .pool
            .take(span)
            .map_err(|e| LogError::Disk(e.into()))?;
        buf.set_len(span).map_err(|e| LogError::Disk(e.into()))?;
        self.shared.file.read_exact_at(buf.as_mut_slice(), begin)?;
        let skip = usize::try_from(place.offset.saturating_sub(begin))
            .map_err(|_| LogError::Corrupt { group, index })?;
        let at = buf.as_slice().get(skip..).unwrap_or_default();
        Ok(format::entry_at(at, group, index)
            .filter(|(t, _)| *t == term)
            .map(|(_, bytes)| Arc::from(bytes)))
    }

    /// Frames written and flushed since the log opened, and the updates they carried: how
    /// many updates one flush commits.
    pub fn flushed(&self) -> (u64, u64) {
        (
            self.shared.frames.load(Ordering::Relaxed),
            self.shared.updates.load(Ordering::Relaxed),
        )
    }

    /// Whether a failed write or flush has fenced the log.
    pub fn is_fenced(&self) -> bool {
        self.shared.fenced.load(Ordering::Acquire)
    }
}

impl<F: BlockFile + 'static> Drop for Log<F> {
    fn drop(&mut self) {
        // Closing the channel stops the writer once it has answered what it took.
        self.sender = None;
        if let Some(handle) = self.writer.take() {
            let _ = handle.join();
        }
    }
}

/// The payload bytes an update carries, which the queue's byte bound counts.
fn update_bytes(update: &Update) -> u64 {
    let entries = update.entries.iter().flat_map(|e| &e.entries);
    let sizes = entries
        .map(|e| e.bytes.len())
        .chain(update.proposals.iter().map(|p| p.bytes.len()));
    sizes.fold(0u64, |sum, len| {
        sum.saturating_add(u64::try_from(len).unwrap_or(u64::MAX))
    })
}
