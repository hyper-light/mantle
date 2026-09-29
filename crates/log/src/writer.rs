//! The writer: one thread per log (docs/design/raft-log.md §3, §5).
//!
//! It runs the chunk store's group-commit loop (chunk-store.md §4): it takes every update
//! that arrived while the last flush ran, lays them into one frame, writes the frame, flushes
//! the file once, and only then publishes the updates to readers and answers them. One frame
//! is one batch, so a valid frame with a later sequence proves an earlier one was flushed,
//! which is what lets recovery tell a torn tail from damage (§6).
//!
//! Segments are freed oldest first, so the live ones are always a run of incarnations that
//! each frame's tail names. A tail with no live piece left is freed at once. When updates
//! are waiting and free segments are short, the writer sweeps the tail: the frame first
//! carries copies of every live piece of the tail, and names the next segment as the tail,
//! the cleaning of a log-structured file system applied to the end of a log [RO92]. A group's
//! live entries run unbroken from its start to its last, and the pieces of a record die
//! from its front by a start and from its back by a replacement, so the live part of any
//! record is one run and its copy is no larger than the record. The copies of a whole tail
//! therefore fit in one frame, and a sweep always completes in the frame that begins it.
//! A freed segment is reused only once a durable frame names a tail past it; the last free
//! segment is kept for a frame that does so, which is how the log never runs out of room to
//! free room.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;

use mantle_codec::Writer as Payload;
use mantle_disk::block::BlockFile;
use mantle_disk::buf::AlignedBuf;

use crate::format::{
    self, FRAME_HEADER_BYTES, FRAME_HEADER_LEN, FrameHeader, Owned, Placed, Record, SegmentHeader,
};
use crate::state::{
    self, Group, HARD_STATE_BYTES, PROPOSAL_EXTRA, Place, START_BYTES, Slot, entry_bytes,
};
use crate::{LogError, Shared, State, Submission, Update};

/// Where an update's pieces went in the payload.
#[derive(Debug, Default)]
struct Placement {
    start: Option<usize>,
    entries: Vec<(u64, usize)>,
    hard: Option<usize>,
    proposals: Vec<(u64, usize)>,
}

/// A live piece of the tail copied into the payload.
#[derive(Debug)]
enum Moved {
    Entry { group: u128, index: u64, at: usize },
    Hard { group: u128, at: usize },
    Start { group: u128, at: usize },
    Proposal { group: u128, index: u64, at: usize },
}

/// A sweep of the tail laid into the payload.
#[derive(Debug)]
struct Sweep {
    slot: u32,
    moved: Vec<Moved>,
    /// The incarnation of the segment after the tail, which the frame names as the tail.
    next_tail: u64,
}

pub(crate) struct Writer<F> {
    shared: Arc<Shared<F>>,
    receiver: Receiver<Submission>,
    /// Updates held for the next batch: a group's second update waits for its first.
    held: VecDeque<Submission>,
    /// Payload bytes one frame holds.
    capacity: usize,
}

impl<F: BlockFile> Writer<F> {
    pub fn new(shared: Arc<Shared<F>>, receiver: Receiver<Submission>) -> Result<Self, LogError> {
        let block = u64::try_from(shared.align.get()).map_err(|_| LogError::Config("block"))?;
        let capacity = shared
            .config
            .segment_bytes
            .checked_sub(block)
            .and_then(|room| usize::try_from(room).ok())
            .and_then(|room| room.checked_sub(FRAME_HEADER_LEN))
            .ok_or(LogError::Config("a segment holds no frame"))?;
        Ok(Self {
            shared,
            receiver,
            held: VecDeque::new(),
            capacity,
        })
    }

    pub fn run(mut self) {
        loop {
            let mut batch = std::mem::take(&mut self.held);
            if batch.is_empty() {
                match self.receiver.recv() {
                    Ok(s) => batch.push_back(self.taken(s)),
                    Err(_) => return,
                }
            }
            while let Ok(s) = self.receiver.try_recv() {
                batch.push_back(self.taken(s));
            }
            if self.shared.fenced.load(Ordering::Acquire) {
                for s in batch {
                    answer(&s, Err(LogError::Fenced));
                }
                // Answer what still comes until the log is dropped.
                while let Ok(s) = self.receiver.recv() {
                    answer(&self.taken(s), Err(LogError::Fenced));
                }
                return;
            }
            if self.commit(batch).is_err() {
                self.fence();
                for s in std::mem::take(&mut self.held) {
                    answer(&s, Err(LogError::Fenced));
                }
            }
        }
    }

    /// Gives back the queue room of a submission the writer took.
    fn taken(&self, s: Submission) -> Submission {
        if let Ok(mut queue) = self.shared.queue.lock() {
            queue.submissions = queue.submissions.saturating_sub(1);
            queue.bytes = queue.bytes.saturating_sub(s.bytes);
        }
        self.shared.room.notify_all();
        s
    }

    /// Fences the log: no submission is taken from here on, and every waiter wakes to hear
    /// it.
    fn fence(&self) {
        self.shared.fenced.store(true, Ordering::Release);
        // Taking the queue's lock orders the fence before any waiter's next look at it.
        drop(self.shared.queue.lock());
        self.shared.room.notify_all();
    }

    /// Writes one frame of a sweep and updates, then publishes and answers them. An error
    /// returned fences the log, and every update taken is answered `Fenced`; updates refused
    /// on their own are answered here.
    fn commit(&mut self, batch: VecDeque<Submission>) -> Result<(), LogError> {
        let mut payload = Payload::default();
        let mut records = 0u32;
        let sweep = if self.sweepable()? {
            Some(self.sweep(&mut payload, &mut records)?)
        } else {
            None
        };
        let mut taken: Vec<(Submission, Placement)> = Vec::new();
        {
            let state = self.shared.read_state()?;
            let mut seen = HashSet::new();
            let mut new_groups = 0usize;
            for s in batch {
                if !seen.insert(s.group) {
                    self.held.push_back(s);
                    continue;
                }
                let new = match validate(&state, &self.shared.config, &s, new_groups) {
                    Ok(new) => new,
                    Err(e) => {
                        answer(&s, Err(e));
                        continue;
                    }
                };
                let len = match update_len(s.group, &s.update) {
                    Some(len) if len <= self.capacity => len,
                    Some(len) => {
                        answer(&s, Err(LogError::TooLarge(len)));
                        continue;
                    }
                    None => {
                        answer(&s, Err(LogError::TooLarge(usize::MAX)));
                        continue;
                    }
                };
                if payload.len().saturating_add(len) > self.capacity {
                    seen.remove(&s.group);
                    self.held.push_back(s);
                    continue;
                }
                let placement = encode(&mut payload, &mut records, s.group, &s.update)
                    .ok_or(LogError::TooLarge(len))?;
                if new {
                    new_groups = new_groups.saturating_add(1);
                }
                taken.push((s, placement));
            }
        }
        if taken.is_empty() && sweep.is_none() {
            return Ok(());
        }
        let payload = payload.into_vec();
        let result = self.place_and_write(&payload, records, sweep, &taken);
        match result {
            Ok(true) => {
                for (s, _) in &taken {
                    answer(s, Ok(()));
                }
                Ok(())
            }
            Ok(false) => {
                // No segment can take the frame: every one holds live records the tail's
                // sweep cannot free. Groups must compact.
                for (s, _) in &taken {
                    answer(s, Err(LogError::Full));
                }
                Ok(())
            }
            Err(e) => {
                // Fenced before anyone hears of it, so no answer outruns the fence.
                self.fence();
                for (s, _) in &taken {
                    answer(s, Err(LogError::Fenced));
                }
                Err(e)
            }
        }
    }

    /// Places, writes and publishes the frame; `false` if no segment can take it.
    fn place_and_write(
        &mut self,
        payload: &[u8],
        records: u32,
        sweep: Option<Sweep>,
        taken: &[(Submission, Placement)],
    ) -> Result<bool, LogError> {
        let (tail, advances) = {
            let state = self.shared.read_state()?;
            let tail = match &sweep {
                Some(s) => s.next_tail,
                None => state.tail_incarnation(),
            };
            (tail, tail > state.durable_tail)
        };
        let Some(target) = self.target(payload.len(), advances)? else {
            return Ok(false);
        };
        self.write(&target, records, payload, tail)?;
        self.publish(&target, sweep, taken, tail)?;
        Ok(true)
    }

    /// Whether the tail should be swept now: free segments are short, a segment follows the
    /// tail, and some of the tail is dead.
    fn sweepable(&self) -> Result<bool, LogError> {
        let state = self.shared.read_state()?;
        let usable = usable_segments(&state, self.shared.config.max_segments);
        let Some(&tail) = state.segments.live.front() else {
            return Ok(false);
        };
        let live_bytes = state.live.of(tail).1;
        let room = u64::try_from(self.capacity).unwrap_or(u64::MAX);
        Ok(usable < 2 && state.segments.live.len() > 1 && live_bytes < room)
    }

    /// Lays copies of every live piece of the tail into the payload.
    fn sweep(&mut self, payload: &mut Payload, records: &mut u32) -> Result<Sweep, LogError> {
        let (slot, incarnation, nonce, next_tail) = {
            let state = self.shared.read_state()?;
            let slot = *state
                .segments
                .live
                .front()
                .ok_or(LogError::Damaged("no tail"))?;
            let next = state
                .segments
                .live
                .get(1)
                .map(|&s| incarnation_of(&state, s))
                .ok_or(LogError::Damaged("no segment after the tail"))?;
            (
                slot,
                incarnation_of(&state, slot),
                nonce_of(&state, slot),
                next,
            )
        };
        let mut offset = slot_start(&self.shared, slot)?
            .checked_add(block(&self.shared)?)
            .ok_or(LogError::Damaged("an offset past u64"))?;
        let mut moved = Vec::new();
        while let Some((header, bytes, padded)) =
            read_frame(&self.shared, slot, incarnation, nonce, offset)?
        {
            let body = bytes
                .as_slice()
                .get(FRAME_HEADER_LEN..header.frame_len().unwrap_or(0))
                .ok_or(LogError::Damaged("a frame shorter than its header says"))?;
            let decoded = format::records(body, header.records)
                .ok_or(LogError::Damaged("a verified frame does not decode"))?;
            let base = offset
                .checked_add(FRAME_HEADER_BYTES)
                .ok_or(LogError::Damaged("an offset past u64"))?;
            let copies = {
                let state = self.shared.read_state()?;
                live_copies(&state, slot, base, &decoded)
            };
            for copy in &copies {
                let placed = format::put(payload, &copy.record())
                    .ok_or(LogError::Damaged("a copy does not encode"))?;
                *records = records.saturating_add(1);
                copy.moved(placed, &mut moved);
            }
            if payload.len() > self.capacity {
                return Err(LogError::Damaged("a tail's live pieces outgrow a frame"));
            }
            offset = offset
                .checked_add(padded)
                .ok_or(LogError::Damaged("an offset past u64"))?;
        }
        Ok(Sweep {
            slot,
            moved,
            next_tail,
        })
    }

    /// Where the next frame of `payload_len` bytes goes: the head, or a segment opened for
    /// it. The last free segment is kept for a frame that names a later tail, whose
    /// durability frees one. `None` when no segment can take it.
    fn target(&self, payload_len: usize, advances: bool) -> Result<Option<Target>, LogError> {
        let shared = &self.shared;
        let block = block(shared)?;
        let frame_len = FRAME_HEADER_LEN
            .checked_add(payload_len)
            .and_then(|len| u64::try_from(len).ok())
            .and_then(|len| shared.align.up_u64(len))
            .ok_or(LogError::TooLarge(payload_len))?;
        let state = shared.read_state()?;
        let head = state.head;
        let head_end = slot_start(shared, head.slot)?
            .checked_add(shared.config.segment_bytes)
            .ok_or(LogError::Damaged("an offset past u64"))?;
        if head
            .offset
            .checked_add(frame_len)
            .is_some_and(|end| end <= head_end)
        {
            return Ok(Some(Target {
                slot: head.slot,
                incarnation: head.incarnation,
                nonce: head.nonce,
                offset: head.offset,
                frame_len,
                opens: false,
            }));
        }
        let usable = usable_segments(&state, shared.config.max_segments);
        if usable == 0 || (usable == 1 && !advances) {
            return Ok(None);
        }
        let reusable = state
            .segments
            .free
            .iter()
            .find(|(_, after)| *after <= state.durable)
            .map(|(slot, _)| *slot);
        let slot = match reusable {
            Some(slot) => slot,
            None => u32::try_from(state.segments.incarnation.len())
                .map_err(|_| LogError::Damaged("more slots than u32"))?,
        };
        let offset = slot_start(shared, slot)?
            .checked_add(block)
            .ok_or(LogError::Damaged("an offset past u64"))?;
        Ok(Some(Target {
            slot,
            incarnation: state.next_incarnation,
            nonce: crate::recover::random_nonce()?,
            offset,
            frame_len,
            opens: true,
        }))
    }

    /// Writes the frame, and the segment's header before it when it opens a segment, then
    /// flushes the file once.
    fn write(
        &self,
        target: &Target,
        records: u32,
        payload: &[u8],
        tail: u64,
    ) -> Result<(), LogError> {
        let shared = &self.shared;
        let sequence = shared.read_state()?.next_sequence;
        let frame = FrameHeader::frame(
            shared.id,
            target.incarnation,
            target.nonce,
            sequence,
            tail,
            records,
            payload,
        )
        .ok_or(LogError::TooLarge(payload.len()))?;
        let block = shared.align.get();
        let header_room = if target.opens { block } else { 0 };
        let frame_len =
            usize::try_from(target.frame_len).map_err(|_| LogError::TooLarge(payload.len()))?;
        let total = header_room
            .checked_add(frame_len)
            .ok_or(LogError::TooLarge(payload.len()))?;
        let mut buf =
            AlignedBuf::zeroed(total, shared.align).map_err(|e| LogError::Disk(e.into()))?;
        if target.opens {
            let header = SegmentHeader {
                log: shared.id,
                incarnation: target.incarnation,
                nonce: target.nonce,
                segment_bytes: shared.config.segment_bytes,
            };
            buf.extend_from_slice(&header.encode())
                .map_err(|e| LogError::Disk(e.into()))?;
            buf.extend_zeros(block.saturating_sub(buf.len()))
                .map_err(|e| LogError::Disk(e.into()))?;
        }
        buf.extend_from_slice(&frame)
            .map_err(|e| LogError::Disk(e.into()))?;
        let at = if target.opens {
            slot_start(shared, target.slot)?
        } else {
            target.offset
        };
        let bytes = buf.padded().map_err(|e| LogError::Disk(e.into()))?;
        shared.file.write_all_at(bytes, at)?;
        shared.file.sync_data()?;
        Ok(())
    }

    /// Makes the durable frame visible: the segment it opened, where swept pieces now are,
    /// every update's pieces, and the tails that died.
    fn publish(
        &mut self,
        target: &Target,
        sweep: Option<Sweep>,
        taken: &[(Submission, Placement)],
        tail: u64,
    ) -> Result<(), LogError> {
        let shared = Arc::clone(&self.shared);
        let config = shared.config;
        let base = target
            .offset
            .checked_add(FRAME_HEADER_BYTES)
            .ok_or(LogError::Damaged("an offset past u64"))?;
        let place = |at: usize| -> Result<Place, LogError> {
            Ok(Place {
                slot: target.slot,
                offset: u64::try_from(at)
                    .ok()
                    .and_then(|at| base.checked_add(at))
                    .ok_or(LogError::Damaged("an offset past u64"))?,
            })
        };
        let mut guard = shared.state.write().map_err(|_| LogError::Fenced)?;
        let state = &mut *guard;
        if target.opens {
            let slot = usize::try_from(target.slot).map_err(|_| LogError::Damaged("slot"))?;
            if state.segments.incarnation.len() <= slot {
                state.segments.incarnation.resize(slot.saturating_add(1), 0);
                state.segments.nonce.resize(slot.saturating_add(1), 0);
                state.live.grow(slot.saturating_add(1));
            }
            if let Some(entry) = state.segments.incarnation.get_mut(slot) {
                *entry = target.incarnation;
            }
            if let Some(entry) = state.segments.nonce.get_mut(slot) {
                *entry = target.nonce;
            }
            state.segments.free.retain(|(s, _)| *s != target.slot);
            state.segments.live.push_back(target.slot);
            state.next_incarnation = state
                .next_incarnation
                .checked_add(1)
                .ok_or(LogError::Damaged("incarnations past u64"))?;
        }
        let sequence = state.next_sequence;
        state.durable = sequence;
        state.durable_tail = tail;
        state.next_sequence = sequence
            .checked_add(1)
            .ok_or(LogError::Damaged("sequences past u64"))?;
        state.head = crate::Head {
            slot: target.slot,
            incarnation: target.incarnation,
            nonce: target.nonce,
            offset: target
                .offset
                .checked_add(target.frame_len)
                .ok_or(LogError::Damaged("an offset past u64"))?,
        };
        if let Some(sweep) = sweep {
            for m in &sweep.moved {
                move_piece(state, m, &place)?;
            }
            if state.segments.live.front() != Some(&sweep.slot) || state.live.of(sweep.slot).0 != 0
            {
                return Err(LogError::Damaged("a swept tail still holds a live piece"));
            }
            state.segments.live.pop_front();
            // This frame names the tail past it and is durable: the segment is free now.
            state.segments.free.push_back((sweep.slot, sequence));
        }
        for (s, placement) in taken {
            apply(state, &config, s.group, &s.update, placement, &place)?;
        }
        drop_dead_tails(state);
        Ok(())
    }
}

/// Where a frame goes.
#[derive(Debug, Clone, Copy)]
struct Target {
    slot: u32,
    incarnation: u64,
    nonce: u64,
    /// File offset of the frame.
    offset: u64,
    /// Bytes of the frame, padded.
    frame_len: u64,
    /// Whether the frame opens its segment, whose header it writes first.
    opens: bool,
}

fn answer(s: &Submission, result: Result<(), LogError>) {
    // A submitter that stopped waiting has nothing to be told.
    let _ = s.reply.try_send(result);
}

fn block<F>(shared: &Shared<F>) -> Result<u64, LogError> {
    u64::try_from(shared.align.get()).map_err(|_| LogError::Config("block"))
}

pub(crate) fn slot_start<F>(shared: &Shared<F>, slot: u32) -> Result<u64, LogError> {
    u64::from(slot)
        .checked_mul(shared.config.segment_bytes)
        .ok_or(LogError::Damaged("an offset past u64"))
}

fn incarnation_of(state: &State, slot: u32) -> u64 {
    usize::try_from(slot)
        .ok()
        .and_then(|s| state.segments.incarnation.get(s))
        .copied()
        .unwrap_or(0)
}

fn nonce_of(state: &State, slot: u32) -> u64 {
    usize::try_from(slot)
        .ok()
        .and_then(|s| state.segments.nonce.get(s))
        .copied()
        .unwrap_or(0)
}

/// Segments a frame could open now: free ones a durable frame has released, and those the
/// file may still grow by.
fn usable_segments(state: &State, max_segments: u32) -> u64 {
    let free = state
        .segments
        .free
        .iter()
        .filter(|(_, after)| *after <= state.durable)
        .count();
    let slots = state.segments.incarnation.len();
    let growable = usize::try_from(max_segments)
        .unwrap_or(usize::MAX)
        .saturating_sub(slots);
    u64::try_from(free.saturating_add(growable)).unwrap_or(u64::MAX)
}

/// Frees tail segments that hold no live piece. The next frame names the new tail, and a
/// freed segment is reused only once that frame is durable.
fn drop_dead_tails(state: &mut State) {
    while state.segments.live.len() > 1 {
        let Some(&tail) = state.segments.live.front() else {
            return;
        };
        if state.live.of(tail).0 != 0 {
            return;
        }
        state.segments.live.pop_front();
        state.segments.free.push_back((tail, state.next_sequence));
    }
}

/// Reads the frame of segment `slot`, incarnation `incarnation`, at `offset`: its header, its
/// bytes and its padded length, or `None` where the segment's frames end.
pub(crate) fn read_frame<F: BlockFile>(
    shared: &Shared<F>,
    slot: u32,
    incarnation: u64,
    nonce: u64,
    offset: u64,
) -> Result<Option<(FrameHeader, AlignedBuf, u64)>, LogError> {
    let end = slot_start(shared, slot)?
        .checked_add(shared.config.segment_bytes)
        .ok_or(LogError::Damaged("an offset past u64"))?;
    let segment = crate::recover::Segment {
        log: shared.id,
        incarnation,
        nonce,
    };
    match crate::recover::frame_at(&shared.file, segment, shared.align, offset, end)? {
        crate::recover::Found::Frame(header, bytes, padded) => Ok(Some((header, bytes, padded))),
        crate::recover::Found::End => Ok(None),
        crate::recover::Found::Invalid => Err(LogError::Damaged("a live frame does not verify")),
    }
}

/// A run of live entries being gathered: its first index and its entries' terms and bytes.
type Run<'a> = Option<(u64, Vec<(u64, &'a [u8])>)>;

/// A live piece of a tail frame, to be copied.
enum Copy<'a> {
    Entries {
        group: u128,
        first: u64,
        entries: Vec<(u64, &'a [u8])>,
    },
    Hard {
        group: u128,
        state: format::HardState,
    },
    Start {
        group: u128,
        start: format::Start,
    },
    Proposal {
        group: u128,
        index: u64,
        term: u64,
        bytes: &'a [u8],
    },
}

impl Copy<'_> {
    fn record(&self) -> Record<'_> {
        match self {
            Copy::Entries {
                group,
                first,
                entries,
            } => Record::Relocated {
                group: *group,
                first: *first,
                entries,
            },
            Copy::Hard { group, state } => Record::HardState {
                group: *group,
                state: *state,
            },
            Copy::Start { group, start } => Record::Start {
                group: *group,
                start: *start,
            },
            Copy::Proposal {
                group,
                index,
                term,
                bytes,
            } => Record::Proposal {
                group: *group,
                index: *index,
                term: *term,
                bytes,
            },
        }
    }

    fn moved(&self, placed: Placed, out: &mut Vec<Moved>) {
        match (self, placed) {
            (Copy::Entries { group, .. }, Placed::Entries(at)) => {
                out.extend(at.into_iter().map(|(index, at)| Moved::Entry {
                    group: *group,
                    index,
                    at,
                }));
            }
            (Copy::Hard { group, .. }, Placed::Record(at)) => {
                out.push(Moved::Hard { group: *group, at });
            }
            (Copy::Start { group, .. }, Placed::Record(at)) => {
                out.push(Moved::Start { group: *group, at });
            }
            (Copy::Proposal { group, index, .. }, Placed::Record(at)) => {
                out.push(Moved::Proposal {
                    group: *group,
                    index: *index,
                    at,
                });
            }
            _ => {}
        }
    }
}

/// The pieces of a tail frame the state still points at, as copies: runs of live entries,
/// and the hard states, starts and proposals still current. `base` is the file offset of the
/// frame's payload.
fn live_copies<'a>(state: &State, slot: u32, base: u64, records: &'a [Owned]) -> Vec<Copy<'a>> {
    let at = |offset: usize| Place {
        slot,
        offset: base.saturating_add(u64::try_from(offset).unwrap_or(u64::MAX)),
    };
    let mut out = Vec::new();
    for record in records {
        match record {
            Owned::Entries { group, entries, .. } | Owned::Relocated { group, entries, .. } => {
                let Some(g) = state.groups.get(group) else {
                    continue;
                };
                let mut run: Run<'_> = None;
                for e in entries {
                    let live = g.slot(e.index).is_some_and(|s| s.place == at(e.at));
                    match (&mut run, live) {
                        (Some((first, run_entries)), true)
                            if first.saturating_add(
                                u64::try_from(run_entries.len()).unwrap_or(u64::MAX),
                            ) == e.index =>
                        {
                            run_entries.push((e.term, &e.bytes));
                        }
                        (_, true) => {
                            if let Some((first, entries)) = run.take() {
                                out.push(Copy::Entries {
                                    group: *group,
                                    first,
                                    entries,
                                });
                            }
                            run = Some((e.index, vec![(e.term, &e.bytes)]));
                        }
                        (_, false) => {
                            if let Some((first, entries)) = run.take() {
                                out.push(Copy::Entries {
                                    group: *group,
                                    first,
                                    entries,
                                });
                            }
                        }
                    }
                }
                if let Some((first, entries)) = run {
                    out.push(Copy::Entries {
                        group: *group,
                        first,
                        entries,
                    });
                }
            }
            Owned::HardState {
                at: offset,
                group,
                state: hard,
            } => {
                let live = state
                    .groups
                    .get(group)
                    .and_then(|g| g.hard)
                    .is_some_and(|(_, p)| p == at(*offset));
                if live {
                    out.push(Copy::Hard {
                        group: *group,
                        state: *hard,
                    });
                }
            }
            Owned::Start {
                at: offset,
                group,
                start,
            } => {
                let live = state
                    .groups
                    .get(group)
                    .is_some_and(|g| g.start_at == Some(at(*offset)));
                if live {
                    out.push(Copy::Start {
                        group: *group,
                        start: *start,
                    });
                }
            }
            Owned::Proposal { group, proposal } => {
                let live = state
                    .groups
                    .get(group)
                    .and_then(|g| g.proposals.get(&proposal.index))
                    .is_some_and(|p| p.place == at(proposal.at));
                if live {
                    out.push(Copy::Proposal {
                        group: *group,
                        index: proposal.index,
                        term: proposal.term,
                        bytes: &proposal.bytes,
                    });
                }
            }
            Owned::Removed { .. } => {}
        }
    }
    out
}

/// Points the state at a relocated piece's new place.
fn move_piece(
    state: &mut State,
    moved: &Moved,
    place: &impl Fn(usize) -> Result<Place, LogError>,
) -> Result<(), LogError> {
    let (live, groups) = (&mut state.live, &mut state.groups);
    match *moved {
        Moved::Entry { group, index, at } => {
            if let Some(slot) = groups.get_mut(&group).and_then(|g| slot_mut(g, index)) {
                let bytes = entry_bytes(slot.len);
                live.kill(slot.place, bytes);
                slot.place = place(at)?;
                live.add(slot.place, bytes);
            }
        }
        Moved::Hard { group, at } => {
            if let Some((_, p)) = groups.get_mut(&group).and_then(|g| g.hard.as_mut()) {
                live.kill(*p, HARD_STATE_BYTES);
                *p = place(at)?;
                live.add(*p, HARD_STATE_BYTES);
            }
        }
        Moved::Start { group, at } => {
            if let Some(p) = groups.get_mut(&group).and_then(|g| g.start_at.as_mut()) {
                live.kill(*p, START_BYTES);
                *p = place(at)?;
                live.add(*p, START_BYTES);
            }
        }
        Moved::Proposal { group, index, at } => {
            if let Some(p) = groups
                .get_mut(&group)
                .and_then(|g| g.proposals.get_mut(&index))
            {
                let bytes = proposal_bytes(&p.bytes);
                live.kill(p.place, bytes);
                p.place = place(at)?;
                live.add(p.place, bytes);
            }
        }
    }
    Ok(())
}

fn slot_mut(g: &mut Group, index: u64) -> Option<&mut Slot> {
    let i = index.checked_sub(g.first()?)?;
    g.entries.get_mut(usize::try_from(i).ok()?)
}

fn proposal_bytes(bytes: &[u8]) -> u64 {
    entry_bytes(u32::try_from(bytes.len()).unwrap_or(u32::MAX)).saturating_add(PROPOSAL_EXTRA)
}

/// Whether `s` may be written as the group stands, and whether it makes a new group.
fn validate(
    state: &State,
    config: &crate::Config,
    s: &Submission,
    new_groups: usize,
) -> Result<bool, LogError> {
    let (group, update) = (s.group, &s.update);
    let invalid = |reason| LogError::Invalid { group, reason };
    let current = state.groups.get(&group);
    if update.remove {
        let alone = update.start.is_none()
            && update.entries.is_none()
            && update.hard_state.is_none()
            && update.proposals.is_empty();
        return if alone {
            Ok(false)
        } else {
            Err(invalid("a removal carries nothing else"))
        };
    }
    let new = current.is_none();
    if new && state.groups.len().saturating_add(new_groups) >= config.max_groups {
        return Err(LogError::TooManyGroups(config.max_groups));
    }
    let empty = Group::default();
    let g = current.unwrap_or(&empty);
    let mut start = g.start;
    let mut last = g.last().ok_or(invalid("an index past u64"))?;
    let mut count = u64::try_from(g.entries.len()).unwrap_or(u64::MAX);
    let mut bytes = g.bytes;
    if let Some(s) = update.start {
        if s.index < start.index {
            return Err(invalid("the start moves back"));
        }
        for index in start.index.saturating_add(1)..=s.index.min(last) {
            if let Some(slot) = g.slot(index) {
                count = count.saturating_sub(1);
                bytes = bytes.saturating_sub(u64::from(slot.len));
            }
        }
        if s.index > last {
            last = s.index;
        }
        start = s;
    }
    if let Some(e) = &update.entries {
        if e.first <= start.index {
            return Err(invalid("entries at or before the start"));
        }
        if e.first > last.saturating_add(1) {
            return Err(invalid("entries past the last leave a gap"));
        }
        for index in e.first..=last {
            if let Some(slot) = g.slot(index) {
                count = count.saturating_sub(1);
                bytes = bytes.saturating_sub(u64::from(slot.len));
            }
        }
        let added = u64::try_from(e.entries.len()).unwrap_or(u64::MAX);
        count = count.saturating_add(added);
        bytes = e.entries.iter().fold(bytes, |sum, entry| {
            sum.saturating_add(u64::try_from(entry.bytes.len()).unwrap_or(u64::MAX))
        });
        last = e
            .first
            .checked_add(added)
            .and_then(|end| end.checked_sub(1))
            .ok_or(invalid("an index past u64"))?;
    }
    if count > config.group_entries || bytes > config.group_bytes {
        return Err(LogError::Backlog(group));
    }
    if update.proposals.iter().any(|p| p.index <= last) {
        return Err(invalid("a proposal the log has reached"));
    }
    Ok(new)
}

/// Bytes an update's records take in a payload.
fn update_len(group: u128, update: &Update) -> Option<usize> {
    let mut len = 0usize;
    if update.remove {
        return format::encoded_len(&Record::Removed { group });
    }
    if let Some(start) = update.start {
        len = len.checked_add(format::encoded_len(&Record::Start { group, start })?)?;
    }
    if let Some(e) = &update.entries {
        let entries: Vec<(u64, &[u8])> = e.entries.iter().map(|x| (x.term, &*x.bytes)).collect();
        len = len.checked_add(format::encoded_len(&Record::Entries {
            group,
            first: e.first,
            entries: &entries,
        })?)?;
    }
    if let Some(state) = update.hard_state {
        len = len.checked_add(format::encoded_len(&Record::HardState { group, state })?)?;
    }
    for p in &update.proposals {
        len = len.checked_add(format::encoded_len(&Record::Proposal {
            group,
            index: p.index,
            term: p.term,
            bytes: &p.bytes,
        })?)?;
    }
    Some(len)
}

/// Lays an update's records into the payload: removal alone, or start, entries, hard state
/// and proposals, in the order they apply.
fn encode(
    payload: &mut Payload,
    records: &mut u32,
    group: u128,
    update: &Update,
) -> Option<Placement> {
    let mut placement = Placement::default();
    let mut put = |record: &Record<'_>| {
        *records = records.checked_add(1)?;
        format::put(payload, record)
    };
    if update.remove {
        put(&Record::Removed { group })?;
        return Some(placement);
    }
    if let Some(start) = update.start
        && let Placed::Record(at) = put(&Record::Start { group, start })?
    {
        placement.start = Some(at);
    }
    if let Some(e) = &update.entries {
        let entries: Vec<(u64, &[u8])> = e.entries.iter().map(|x| (x.term, &*x.bytes)).collect();
        if let Placed::Entries(at) = put(&Record::Entries {
            group,
            first: e.first,
            entries: &entries,
        })? {
            placement.entries = at;
        }
    }
    if let Some(state) = update.hard_state
        && let Placed::Record(at) = put(&Record::HardState { group, state })?
    {
        placement.hard = Some(at);
    }
    for p in &update.proposals {
        if let Placed::Record(at) = put(&Record::Proposal {
            group,
            index: p.index,
            term: p.term,
            bytes: &p.bytes,
        })? {
            placement.proposals.push((p.index, at));
        }
    }
    Some(placement)
}

/// Applies a durable update to the group's state.
fn apply(
    state: &mut State,
    config: &crate::Config,
    group: u128,
    update: &Update,
    placement: &Placement,
    place: &impl Fn(usize) -> Result<Place, LogError>,
) -> Result<(), LogError> {
    let live = &mut state.live;
    if update.remove {
        if let Some(g) = state.groups.remove(&group) {
            for (p, bytes) in g.pieces() {
                live.kill(p, bytes);
            }
        }
        return Ok(());
    }
    let g = state.groups.entry(group).or_insert_with(|| Group {
        cache_from: 1,
        ..Group::default()
    });
    if let (Some(start), Some(at)) = (update.start, placement.start) {
        if let Some(old) = g.start_at {
            live.kill(old, START_BYTES);
        }
        while g.start.index < start.index {
            let Some(slot) = g.entries.pop_front() else {
                break;
            };
            kill_slot(g, live, &slot);
            g.start.index = g.start.index.saturating_add(1);
        }
        g.start = start;
        let new_at = place(at)?;
        g.start_at = Some(new_at);
        live.add(new_at, START_BYTES);
        g.cache_from = g.cache_from.max(start.index.saturating_add(1));
    }
    if let Some(e) = &update.entries {
        while g.last().is_some_and(|last| last >= e.first) {
            let Some(slot) = g.entries.pop_back() else {
                break;
            };
            kill_slot(g, live, &slot);
        }
        for (entry, &(_, at)) in e.entries.iter().zip(&placement.entries) {
            let len =
                u32::try_from(entry.bytes.len()).map_err(|_| LogError::TooLarge(usize::MAX))?;
            let slot = Slot {
                term: entry.term,
                place: place(at)?,
                len,
                cached: Some(Arc::clone(&entry.bytes)),
            };
            live.add(slot.place, entry_bytes(len));
            g.bytes = g.bytes.saturating_add(u64::from(len));
            g.cached = g.cached.saturating_add(u64::from(len));
            g.entries.push_back(slot);
        }
        g.cache_from = g
            .cache_from
            .min(e.first)
            .max(g.start.index.saturating_add(1));
        evict(g, config.group_cache);
    }
    // What the log has reached, by entries or by its start, is no longer a proposal
    // (07 §1.4).
    let last = g.last().ok_or(LogError::Damaged("an index past u64"))?;
    let reached: Vec<u64> = g.proposals.range(..=last).map(|(&i, _)| i).collect();
    for index in reached {
        if let Some(p) = g.proposals.remove(&index) {
            live.kill(p.place, proposal_bytes(&p.bytes));
        }
    }
    if let (Some(hard), Some(at)) = (update.hard_state, placement.hard) {
        if let Some((_, old)) = g.hard {
            live.kill(old, HARD_STATE_BYTES);
        }
        let new_at = place(at)?;
        g.hard = Some((hard, new_at));
        live.add(new_at, HARD_STATE_BYTES);
    }
    for (p, &(index, at)) in update.proposals.iter().zip(&placement.proposals) {
        let new_at = place(at)?;
        let bytes = proposal_bytes(&p.bytes);
        let old = g.proposals.insert(
            index,
            state::Proposal {
                term: p.term,
                place: new_at,
                bytes: Arc::clone(&p.bytes),
            },
        );
        if let Some(old) = old {
            live.kill(old.place, proposal_bytes(&old.bytes));
        }
        live.add(new_at, bytes);
    }
    Ok(())
}

fn kill_slot(g: &mut Group, live: &mut state::Live, slot: &Slot) {
    live.kill(slot.place, entry_bytes(slot.len));
    g.bytes = g.bytes.saturating_sub(u64::from(slot.len));
    if slot.cached.is_some() {
        g.cached = g.cached.saturating_sub(u64::from(slot.len));
    }
}

/// Drops cached bytes from the oldest cached entry on until the group is within its budget.
fn evict(g: &mut Group, budget: u64) {
    while g.cached > budget {
        let index = g.cache_from;
        let Some(slot) = slot_mut(g, index) else {
            return;
        };
        if slot.cached.take().is_some() {
            let len = u64::from(slot.len);
            g.cached = g.cached.saturating_sub(len);
        }
        g.cache_from = index.saturating_add(1);
    }
}
