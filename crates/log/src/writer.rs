//! The writer: one thread per log (docs/design/raft-log.md §3, §5).
//!
//! It runs the chunk store's group-commit loop (chunk-store.md §4): it takes every update
//! that arrived while the last flush ran, lays them into one frame, writes the frame, flushes
//! the file once, and publishes the updates to readers. It answers them once a later durable
//! record confirms that flush: the next frame's persist record, or a confirmation written on
//! its own when no frame follows at once (raft-log.md §6). One frame is one batch, so a valid
//! frame with a later sequence proves an earlier one was flushed, which is what lets recovery
//! tell a torn tail from damage (§6).
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
//!
//! Replicas submit in a closed loop, each its next update once its last is durable. Formed at
//! once from whatever is queued, each batch would miss the replicas the last one answered,
//! and a few replicas would alternate between batches, every update waiting for two flushes
//! (docs/measurements/2026-09-28-raft-log-benchmark.md). The writer waits for them as long as
//! waiting is expected to lower total latency, as the chunk store's writer does
//! (`mantle_disk::commit`). Under `Waits::Never` it forms each batch from what is queued.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;

use mantle_codec::Writer as Payload;
use mantle_disk::block::BlockFile;
use mantle_disk::buf::AlignedBuf;
use mantle_disk::commit::Anticipation;

use crate::format::{
    self, FRAME_HEADER_BYTES, FRAME_HEADER_LEN, FrameHeader, Owned, Placed, Record, SegmentHeader,
};
use crate::state::{
    self, DAMAGED_BYTES, Group, HARD_STATE_BYTES, PROPOSAL_EXTRA, Place, START_BYTES, Slot,
    UNCERTAIN_BYTES, entry_bytes, resolves,
};
use crate::{Class, LogError, Shared, State, Submission, Update, Waits};

/// Where the writer's fair queue placed a submission (docs/design/raft-log.md §3).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Tags {
    /// Its place in the order the writer took submissions.
    seq: u64,
    /// Its start tag in start-time fair queueing: the larger of the virtual time when it was
    /// taken and its group's last finish tag, in charged bytes [SFQ96 §2 eq. 4].
    start: u128,
    /// The frame that first passed it over for room, if one has.
    passed: Option<u64>,
}

/// The order a frame considers a submission in: its tier (passed over, then each class), then
/// when it was passed over and its class, or its start tag, then when it was taken.
type Key = (u8, u128, u64);

fn key(s: &Submission) -> Key {
    let class = match s.class {
        Class::Latency => 1u8,
        Class::Normal => 2,
        Class::Background => 3,
    };
    match s.tags.passed {
        // Those one frame passed over go by class among themselves, all before any a later
        // frame passes over.
        Some(frame) => (0, u128::from(frame) << 2 | u128::from(class), s.tags.seq),
        None => (class, s.tags.start, s.tags.seq),
    }
}

/// Puts the backlog in the order a frame considers it (docs/design/raft-log.md §3). A group's
/// update never sorts before one of its own taken earlier: it takes the later key of the two,
/// as Tectonic gives traffic that borrows another TrafficGroup's resources the lower of their
/// classes [research/01 §1.11], so a group's updates stay in the order submitted.
fn order(batch: &mut VecDeque<Submission>) {
    let mut taken: Vec<Submission> = std::mem::take(batch).into();
    taken.sort_by_key(|s| s.tags.seq);
    let mut last: HashMap<u128, Key> = HashMap::new();
    let mut keyed: Vec<(Key, Submission)> = taken
        .into_iter()
        .map(|s| {
            let own = key(&s);
            let k = match last.get(&s.group) {
                Some(&before) if before > own => before,
                _ => own,
            };
            last.insert(s.group, k);
            (k, s)
        })
        .collect();
    // Stable: a group's updates of one key keep the order they were taken in.
    keyed.sort_by_key(|(k, _)| *k);
    batch.extend(keyed.into_iter().map(|(_, s)| s));
}

/// Where an update's pieces went in the payload.
#[derive(Debug, Default)]
struct Placement {
    start: Option<usize>,
    entries: Vec<(u64, usize)>,
    hard: Option<usize>,
    proposals: Vec<(u64, usize)>,
    uncertain: Option<usize>,
    damaged: Option<usize>,
}

/// A live piece of the tail copied into the payload.
#[derive(Debug)]
enum Moved {
    Entry { group: u128, index: u64, at: usize },
    Hard { group: u128, at: usize },
    Start { group: u128, at: usize },
    Proposal { group: u128, index: u64, at: usize },
    Uncertain { group: u128, at: usize },
    Damaged { group: u128, at: usize },
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
    /// Updates held for the next batch: those a frame passed over for room, and those whose
    /// group had an update in it.
    held: VecDeque<Submission>,
    /// Start-time fair queueing's virtual time, in charged bytes: the largest start tag laid
    /// into a frame, and the largest finish tag once the backlog empties [SFQ96 §2].
    virtual_time: u128,
    /// Each group's last finish tag, while it is ahead of the virtual time and the group is
    /// one the log holds or has an update waiting: at most `max_groups` and
    /// `queue_submissions` entries.
    finish: HashMap<u128, u128>,
    /// Frames laid out so far, which date a submission passed over.
    walks: u64,
    /// Frames written in a row that swept and carried no update while updates waited: at most
    /// `max_segments` before those waiting are refused `Full`.
    fruitless: u64,
    /// Payload bytes one frame holds.
    capacity: usize,
    anticipation: Anticipation,
    /// Submissions taken off the channel so far.
    received: u64,
    /// The last frame flushed, while nothing durable yet says its flush completed: the next
    /// frame's persist record will, or a confirmation written when no frame follows at once
    /// (docs/design/raft-log.md §3, §6). Its updates are answered only then.
    unconfirmed: Option<Unconfirmed>,
    /// The aligned buffer the last frame was laid out in, kept for the next: one frame's
    /// bytes at most, a segment and a block, which the log's settings hold within one I/O
    /// buffer.
    frame: Option<AlignedBuf>,
    /// The first batch is the restore of a lost frame at open, which goes in one frame or not
    /// at all (docs/design/raft-log.md §6).
    restoring: bool,
}

/// A frame flushed and not yet confirmed, and the updates it carried: at most one frame's.
struct Unconfirmed {
    sequence: u64,
    updates: Vec<Submission>,
}

impl<F: BlockFile> Writer<F> {
    /// A writer whose first batch is `restores`, the restore of a lost frame at open, if any.
    pub fn new(
        shared: Arc<Shared<F>>,
        receiver: Receiver<Submission>,
        restores: Vec<Submission>,
    ) -> Result<Self, LogError> {
        let capacity = crate::frame_room(&shared.config, shared.align)?;
        Ok(Self {
            shared,
            receiver,
            restoring: !restores.is_empty(),
            held: restores.into(),
            capacity,
            anticipation: Anticipation::new(),
            received: 0,
            virtual_time: 0,
            finish: HashMap::new(),
            walks: 0,
            fruitless: 0,
            unconfirmed: None,
            frame: None,
        })
    }

    pub fn run(mut self) {
        // Submissions the last confirmation answered, and those already sent when it did.
        let mut answered = 0u64;
        let mut backlog = 0u64;
        loop {
            let mut batch = std::mem::take(&mut self.held);
            let held = u64::try_from(batch.len()).unwrap_or(u64::MAX);
            while let Ok(s) = self.receiver.try_recv() {
                batch.push_back(self.taken(s));
            }
            if batch.is_empty() {
                if self.unconfirmed.is_some() {
                    // No frame would carry the confirmation: it goes on its own, at once.
                    answered = self.confirm();
                    backlog = self.backlog();
                    continue;
                }
                // The backlog is empty: the busy period ends at the largest finish tag
                // [SFQ96 §2], and no group's past service counts against it any longer.
                let last = self.finish.values().copied().max().unwrap_or(0);
                self.virtual_time = self.virtual_time.max(last);
                self.finish.clear();
                match self.receiver.recv() {
                    Ok(s) => batch.push_back(self.taken(s)),
                    Err(_) => return,
                }
            }
            self.gather(&mut batch, answered, backlog.saturating_add(held));
            if self.shared.fenced.load(Ordering::Acquire) {
                for s in batch {
                    self.answer(&s, Err(LogError::Fenced));
                }
                // Answer what still comes until the log is dropped.
                while let Ok(s) = self.receiver.recv() {
                    let s = self.taken(s);
                    self.answer(&s, Err(LogError::Fenced));
                }
                return;
            }
            answered = match self.commit(batch) {
                Ok(Some(confirmed)) => confirmed,
                // No frame was written, so none confirms the last: a confirmation does, so that
                // no answer waits on traffic that may only ever be refused.
                Ok(None) => self.confirm(),
                // The commit fenced the log and answered every update it held.
                Err(_) => 0,
            };
            self.forget();
            backlog = self.backlog();
        }
    }

    /// Drops the finish tags that no longer order anything: those the virtual time has passed,
    /// which a start tag would take the virtual time over, and those of groups the log no
    /// longer holds with nothing waiting, so the map is bounded by the log's groups and the
    /// queue's submissions.
    fn forget(&mut self) {
        let waiting: HashSet<u128> = self.held.iter().map(|s| s.group).collect();
        let Ok(state) = self.shared.read_state() else {
            self.finish.clear();
            return;
        };
        let now = self.virtual_time;
        self.finish.retain(|group, finish| {
            *finish > now && (state.groups.contains_key(group) || waiting.contains(group))
        });
    }

    /// Submissions sent before the answers just given went out: they are queued ahead of any
    /// the answered replicas send next.
    fn backlog(&self) -> u64 {
        self.shared
            .submitted
            .load(Ordering::Acquire)
            .saturating_sub(self.received)
    }

    /// Writes and flushes a confirmation that the last frame was flushed, and answers the
    /// frame's updates: the number answered. A failed write or flush fences the log, and they
    /// are answered `Fenced`.
    ///
    /// The confirmation is the frame's own persist record again, now saying the frame was
    /// flushed, written over the record in the frame's own slot. The next frame's record goes
    /// in the other slot before that frame's flush and may tear; a confirmation kept there
    /// would tear with it, and a frame answered and then damaged at rest would be taken for a
    /// torn one (docs/design/raft-log.md §6). The rewrite puts at risk only the record of a
    /// frame not yet answered, which recovery may drop as it drops a torn tail.
    fn confirm(&mut self) -> u64 {
        let Some(frame) = self.unconfirmed.take() else {
            return 0;
        };
        let written = if self.shared.fenced.load(Ordering::Acquire) {
            Err(LogError::Fenced)
        } else {
            let record = format::Persist {
                log: self.shared.id,
                sequence: frame.sequence,
                confirms: frame.sequence,
                groups: frame.updates.iter().map(persisted).collect(),
            };
            write_persist(&self.shared, &record, frame.sequence)
                .and_then(|()| self.shared.file.sync_data().map_err(LogError::from))
        };
        match written {
            Ok(()) => self.settle(frame, Ok(())),
            Err(_) => {
                self.fence();
                self.settle(frame, Err(()));
                0
            }
        }
    }

    /// Answers the updates of a frame whose confirmation is durable, `Ok`, or will never be,
    /// `Fenced`; the number answered.
    fn settle(&self, frame: Unconfirmed, result: Result<(), ()>) -> u64 {
        for s in &frame.updates {
            let answer = match result {
                Ok(()) => Ok(()),
                Err(()) => Err(LogError::Fenced),
            };
            self.answer(s, answer);
        }
        u64::try_from(frame.updates.len()).unwrap_or(u64::MAX)
    }

    /// Waits for the replicas the last confirmation answered while waiting is expected to
    /// lower total latency, and learns how many of them return (`mantle_disk::commit`). The
    /// first `before` submissions in the batch were sent before the answers.
    fn gather(&mut self, batch: &mut VecDeque<Submission>, answered: u64, before: u64) {
        if answered == 0 || self.shared.config.waits == Waits::Never {
            return;
        }
        let returned = |batch: &VecDeque<Submission>| {
            u64::try_from(batch.len())
                .unwrap_or(u64::MAX)
                .saturating_sub(before)
        };
        // A batch that holds a frame's worth already is not waited on: what came next would
        // go in a later frame whatever the wait.
        let full = u64::try_from(self.capacity).unwrap_or(u64::MAX);
        let mut gathered = batch
            .iter()
            .fold(0u64, |sum, s| sum.saturating_add(s.bytes));
        while returned(batch) < answered
            && batch.len() < self.shared.config.queue_submissions
            && gathered < full
        {
            let Some(step) = self.anticipation.wait(batch.len()) else {
                break;
            };
            match self.receiver.recv_timeout(step) {
                Ok(s) => {
                    gathered = gathered.saturating_add(s.bytes);
                    batch.push_back(self.taken(s));
                }
                Err(_) => break,
            }
        }
        self.anticipation.learn(answered, returned(batch));
    }

    /// Counts a submission the writer took. Its room in the queue is held until it is
    /// answered, so the queue's bound covers the updates held for a later frame and those in
    /// the frame being written as well as those waiting (audit S03).
    ///
    /// It is stamped for the fair queue: its start tag is the larger of the virtual time and
    /// its group's last finish tag, and its finish tag its start plus its charge [SFQ96 §2
    /// eqs. 4–5]. Every group weighs the same, so a group's share of a full frame's bytes is the
    /// others' (Theorem 1); the class orders tiers instead of weighting them.
    fn taken(&mut self, mut s: Submission) -> Submission {
        self.received = self.received.saturating_add(1);
        let start = self
            .finish
            .get(&s.group)
            .map_or(self.virtual_time, |&f| f.max(self.virtual_time));
        // Charged bytes over the log's life stay far below 2^128: saturation is unreachable,
        // and would only order the group last.
        self.finish
            .insert(s.group, start.saturating_add(u128::from(s.bytes)));
        s.tags = Tags {
            seq: self.received,
            start,
            passed: None,
        };
        s
    }

    /// Answers a submission, and gives back its room in the queue.
    fn answer(&self, s: &Submission, result: Result<(), LogError>) {
        // A submitter that stopped waiting has nothing to be told.
        let _ = s.reply.try_send(result);
        self.shared.release(s.group, s.bytes);
        s.wake();
    }

    /// Fences the log: no submission is taken from here on, and every waiter wakes to hear
    /// it.
    fn fence(&self) {
        self.shared.fenced.store(true, Ordering::Release);
        self.shared.room.fence();
    }

    /// Writes one frame of a sweep and updates and publishes it. Its persist record confirms
    /// the frame before, whose updates are answered, and the number answered is returned; its
    /// own updates wait for their confirmation. `None` if no frame was written. An error
    /// returned has fenced the log and answered `Fenced` every update the writer holds: those
    /// taken for the frame, those of the batch not reached, those held for a later frame, and
    /// those of the frame before, whose confirmation will never come. Wherever the commit
    /// fails, no submitter waits on an answer that cannot come, and every submission's room in
    /// the queue is given back. Updates refused on their own are answered as they are refused.
    fn commit(&mut self, batch: VecDeque<Submission>) -> Result<Option<u64>, LogError> {
        let mut batch = batch;
        let mut taken = Vec::new();
        let result = self.frame(&mut batch, &mut taken);
        if result.is_err() {
            // Fenced before anyone hears of it, so no answer outruns the fence.
            self.fence();
            let held = std::mem::take(&mut self.held);
            let untaken = batch.iter().chain(&held);
            for s in taken.iter().map(|(s, _)| s).chain(untaken) {
                self.answer(s, Err(LogError::Fenced));
            }
            if let Some(before) = self.unconfirmed.take() {
                self.settle(before, Err(()));
            }
        }
        result
    }

    /// Lays the batch into a frame after any sweep, then writes and publishes it and answers
    /// the frame before. Each update laid out moves from `batch` to `taken`, and leaves `taken`
    /// only once it is answered or awaits its confirmation, so that on an error `commit`
    /// answers every update this left.
    fn frame(
        &mut self,
        batch: &mut VecDeque<Submission>,
        taken: &mut Vec<(Submission, Placement)>,
    ) -> Result<Option<u64>, LogError> {
        let whole = std::mem::replace(&mut self.restoring, false);
        let mut payload = Payload::default();
        let mut records = 0u32;
        let sweep = if self.sweepable()? {
            Some(self.sweep(&mut payload, &mut records)?)
        } else {
            None
        };
        let mut refused = false;
        self.walks = self.walks.saturating_add(1);
        order(batch);
        {
            let state = self.shared.read_state()?;
            let mut seen = HashSet::new();
            let mut new_groups = 0usize;
            while let Some(mut s) = batch.pop_front() {
                if !seen.insert(s.group) {
                    self.held.push_back(s);
                    continue;
                }
                let checked =
                    validate(&state, &self.shared.config, &s, new_groups).and_then(|new| {
                        match submission_len(s.group, &s.update, s.marks) {
                            Some(len) if len <= self.capacity => Ok((new, len)),
                            Some(len) => Err(LogError::TooLarge(len)),
                            None => Err(LogError::TooLarge(usize::MAX)),
                        }
                    });
                let (new, len) = match checked {
                    Ok(checked) => checked,
                    Err(e) => {
                        self.answer(&s, Err(e));
                        refused = true;
                        continue;
                    }
                };
                if payload.len().saturating_add(len) > self.capacity {
                    // The group stays taken for this frame: its later updates wait behind
                    // this one, so its updates become durable in the order submitted. Passed
                    // over, it goes ahead of every class from the next frame on, behind only
                    // those passed over before it.
                    s.tags.passed.get_or_insert(self.walks);
                    self.held.push_back(s);
                    continue;
                }
                let Some(placement) =
                    encode(&mut payload, &mut records, s.group, &s.update, s.marks)
                else {
                    batch.push_front(s);
                    return Err(LogError::TooLarge(len));
                };
                self.virtual_time = self.virtual_time.max(s.tags.start);
                if new {
                    new_groups = new_groups.saturating_add(1);
                }
                taken.push((s, placement));
            }
        }
        if whole && (refused || !self.held.is_empty()) {
            // A restore in parts would overwrite the lost frame's persist record with the
            // first part's, and a crash before the last part was durable would leave the rest
            // restored nowhere: none of it is written, and the open fails (raft-log.md §6).
            let parts = taken.drain(..).map(|(s, _)| s);
            for s in parts.chain(std::mem::take(&mut self.held)) {
                self.answer(
                    &s,
                    Err(LogError::Damaged(
                        "a lost frame's restore does not fit one frame",
                    )),
                );
            }
            return Ok(None);
        }
        if taken.is_empty() && sweep.is_none() {
            return Ok(None);
        }
        let payload = payload.into_vec();
        match self.place_and_write(&payload, records, sweep, taken)? {
            Some(sequence) => {
                self.shared.frames.fetch_add(1, Ordering::Relaxed);
                let updates = u64::try_from(taken.len()).unwrap_or(u64::MAX);
                self.shared.updates.fetch_add(updates, Ordering::Relaxed);
                let confirmed = self
                    .unconfirmed
                    .take()
                    .map_or(0, |before| self.settle(before, Ok(())));
                // Frames that sweep and carry no update, in a row, while updates wait: each
                // copies a tail with a dead piece, and its copies are all live, so after a
                // sweep of every segment no dead piece is left to free. Past that many, the
                // log cannot make room for what waits (docs/design/raft-log.md §5).
                if taken.is_empty() {
                    self.fruitless = self.fruitless.saturating_add(1);
                    if self.fruitless >= u64::from(self.shared.config.max_segments) {
                        self.refuse_held();
                    }
                } else {
                    self.fruitless = 0;
                }
                self.unconfirmed = Some(Unconfirmed {
                    sequence,
                    updates: taken.drain(..).map(|(s, _)| s).collect(),
                });
                Ok(Some(confirmed))
            }
            None => {
                // No segment can take the frame: every one holds live records the tail's
                // sweep cannot free. Groups must compact. A frame that carried no update was
                // a sweep making room for those held: none can come, and they are refused
                // too, where before they were laid out again, and again, without an answer.
                let sweep_only = taken.is_empty();
                for (s, _) in taken.drain(..) {
                    self.answer(&s, Err(LogError::Full));
                }
                if sweep_only {
                    self.refuse_held();
                }
                Ok(None)
            }
        }
    }

    /// Answers every held update `Full`: no room can be made for them until groups compact.
    fn refuse_held(&mut self) {
        self.fruitless = 0;
        for s in std::mem::take(&mut self.held) {
            self.answer(&s, Err(LogError::Full));
        }
    }

    /// Places, writes and publishes the frame: its sequence, or `None` if no segment can take
    /// it.
    fn place_and_write(
        &mut self,
        payload: &[u8],
        records: u32,
        sweep: Option<Sweep>,
        taken: &[(Submission, Placement)],
    ) -> Result<Option<u64>, LogError> {
        let (tail, advances) = {
            let state = self.shared.read_state()?;
            let tail = match &sweep {
                Some(s) => s.next_tail,
                None => state.tail_incarnation(),
            };
            (tail, tail > state.durable_tail)
        };
        // A frame makes room if it names a later tail, whose durability frees a segment, or if
        // its every update only frees what its group held: a compaction, a removal, a fence.
        let makes_room =
            advances || (!taken.is_empty() && taken.iter().all(|(s, _)| frees(&s.update, s.marks)));
        let Some(target) = self.target(payload.len(), makes_room)? else {
            return Ok(None);
        };
        let started = std::time::Instant::now();
        let sequence = self.write(&target, records, payload, tail, taken)?;
        self.anticipation
            .served(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        self.publish(&target, sweep, taken, tail)?;
        Ok(Some(sequence))
    }

    /// Whether the tail should be swept now: free segments are short, a segment follows the
    /// tail, and sweeping it makes room: its copies fit one frame, and that frame takes less
    /// than the tail's frames do.
    fn sweepable(&self) -> Result<bool, LogError> {
        let state = self.shared.read_state()?;
        let usable = usable_segments(&state, self.shared.config.max_segments);
        let Some(&tail) = state.segments.live.front() else {
            return Ok(false);
        };
        if usable >= 2 || state.segments.live.len() < 2 {
            return Ok(false);
        }
        // The copies' bytes at most: every live piece, and a relocated record's header for
        // each, as if no two entries ran together.
        let (count, live_bytes) = state.live.of(tail);
        let run = format::encoded_len(&Record::Relocated {
            group: 0,
            first: 0,
            entries: &[],
        })
        .and_then(|len| u64::try_from(len).ok())
        .ok_or(LogError::Config("a relocated record's header"))?;
        let copies = count
            .checked_mul(run)
            .and_then(|h| h.checked_add(live_bytes));
        let room = u64::try_from(self.capacity).unwrap_or(u64::MAX);
        let Some(copies) = copies.filter(|&c| c <= room) else {
            return Ok(false);
        };
        let frame = u64::try_from(FRAME_HEADER_LEN)
            .ok()
            .and_then(|h| h.checked_add(copies))
            .and_then(|len| self.shared.align.up_u64(len))
            .ok_or(LogError::Damaged("a frame past u64"))?;
        // The sweep makes room if the segment after the tail holds nothing live, so that the
        // frame naming a later tail frees it too, or if its frame takes less than the
        // tail's frames. A tail copied into a segment of its own, as large as its own frames,
        // frees one segment and fills another: the writer once swept every segment in turn so,
        // while the updates waiting never fitted beside the copies. And a small live record
        // left in the oldest segment once held back every dead segment behind it: its copy was
        // as large as its frame, so it was never swept, and no frame larger than the head's
        // room went in again.
        // The segment after the tail, unless it is the head, which takes the next frame.
        let dead_behind = state.segments.live.len() > 2
            && state
                .segments
                .live
                .get(1)
                .is_some_and(|&slot| state.live.of(slot).0 == 0);
        Ok(dead_behind || frame < state.live.used(tail))
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
        let begin = slot_start(&self.shared, slot)?;
        let end = begin
            .checked_add(self.shared.config.segment_bytes)
            .ok_or(LogError::Damaged("an offset past u64"))?;
        let mut offset = begin
            .checked_add(block(&self.shared)?)
            .ok_or(LogError::Damaged("an offset past u64"))?;
        let segment = crate::recover::Segment {
            log: self.shared.id,
            incarnation,
            nonce,
        };
        // The tail is read through a window of a segment: nothing writes it while it is swept.
        let mut reader = crate::recover::Reader::new(
            &self.shared.file,
            self.shared.align,
            self.shared.config.segment_bytes,
        )?;
        let mut moved = Vec::new();
        loop {
            let (header, bytes, padded) = match reader.frame_at(segment, offset, end)? {
                crate::recover::Found::Frame(header, bytes, padded) => (header, bytes, padded),
                crate::recover::Found::End => break,
                crate::recover::Found::Invalid => {
                    return Err(LogError::Damaged("a live frame does not verify"));
                }
            };
            let body = bytes
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
    /// it. The last free segment is kept for a frame that makes room, and while no segment is
    /// free the head's room is too, so that the compaction or the tail's advance a full log
    /// waits for always has somewhere to go (docs/design/raft-log.md §5). `None` when no
    /// segment can take it.
    fn target(&self, payload_len: usize, makes_room: bool) -> Result<Option<Target>, LogError> {
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
        let usable = usable_segments(&state, shared.config.max_segments);
        if head
            .offset
            .checked_add(frame_len)
            .is_some_and(|end| end <= head_end)
            && (usable > 0 || makes_room)
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
        if usable == 0 || (usable == 1 && !makes_room) {
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

    /// Writes the frame, and the segment's header before it when it opens a segment, and the
    /// frame's persist record, then flushes the file once. Returns the frame's sequence.
    fn write(
        &mut self,
        target: &Target,
        records: u32,
        payload: &[u8],
        tail: u64,
        taken: &[(Submission, Placement)],
    ) -> Result<u64, LogError> {
        let shared = Arc::clone(&self.shared);
        let (sequence, flushed) = {
            let state = shared.read_state()?;
            (state.next_sequence, state.durable)
        };
        let frame = FrameHeader::header(
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
        // The last frame's buffer, reused while it is large enough: every byte up to the padded
        // end is written below, so nothing of the frame before survives into this one.
        let mut buf = match self.frame.take() {
            Some(buf) if buf.capacity() >= total => buf,
            _ => AlignedBuf::zeroed(total, shared.align).map_err(|e| LogError::Disk(e.into()))?,
        };
        buf.clear();
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
        buf.extend_from_slice(payload)
            .map_err(|e| LogError::Disk(e.into()))?;
        let at = if target.opens {
            slot_start(&shared, target.slot)?
        } else {
            target.offset
        };
        let written = buf
            .padded()
            .map_err(|e| LogError::Disk(e.into()))
            .and_then(|bytes| shared.file.write_all_at(bytes, at).map_err(LogError::from));
        self.frame = Some(buf);
        written?;
        // What the frame's flush makes durable, apart from the frame, so that recovery can
        // restore it should the frame be damaged since (§6); it confirms the frame before.
        let record = format::Persist {
            log: shared.id,
            sequence,
            confirms: flushed,
            groups: taken.iter().map(|(s, _)| persisted(s)).collect(),
        };
        write_persist(&shared, &record, sequence)?;
        shared.file.sync_data()?;
        Ok(sequence)
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
            state.live.open(target.slot);
            state.segments.free.retain(|(s, _)| *s != target.slot);
            state.segments.live.push_back(target.slot);
            state.next_incarnation = state
                .next_incarnation
                .checked_add(1)
                .ok_or(LogError::Damaged("incarnations past u64"))?;
        }
        state.live.wrote(target.slot, target.frame_len);
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
            apply(state, &config, s, placement, &place)?;
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

fn block<F>(shared: &Shared<F>) -> Result<u64, LogError> {
    u64::try_from(shared.align.get()).map_err(|_| LogError::Config("block"))
}

pub(crate) fn slot_start<F>(shared: &Shared<F>, slot: u32) -> Result<u64, LogError> {
    u64::from(slot)
        .checked_mul(shared.config.segment_bytes)
        .and_then(|s| s.checked_add(crate::recover::persist_area(&shared.config)))
        .ok_or(LogError::Damaged("an offset past u64"))
}

/// Writes `record` into the persist slot of the frame of `slot_of`.
fn write_persist<F: BlockFile>(
    shared: &Shared<F>,
    record: &format::Persist,
    slot_of: u64,
) -> Result<(), LogError> {
    let slot = crate::recover::persist_slot(&shared.config, shared.align)?;
    crate::recover::write_record(
        &shared.file,
        record,
        crate::recover::persist_at(slot, slot_of),
    )
}

/// What a submission's frame makes durable for its group, as its persist record says it.
fn persisted(s: &Submission) -> format::Persisted {
    let u = &s.update;
    format::Persisted {
        group: s.group,
        hard_state: u.hard_state,
        start: u.start,
        entries: u.entries.as_ref().map(|e| format::Written {
            first: e.first,
            count: u64::try_from(e.entries.len()).unwrap_or(u64::MAX),
            term: e.entries.last().map_or(0, |x| x.term),
        }),
        uncertain: s.marks.uncertain,
        removed: u.remove,
        proposals: !u.proposals.is_empty(),
        damaged: s.marks.damaged,
    }
}

/// Bytes a submission's records take in a payload: its update's and its marks'.
pub(crate) fn submission_len(group: u128, update: &Update, marks: crate::Marks) -> Option<usize> {
    if marks.damaged {
        return format::encoded_len(&Record::Damaged { group });
    }
    let len = update_len(group, update)?;
    match marks.uncertain {
        Some(mark) => len.checked_add(format::encoded_len(&Record::Uncertain { group, mark })?),
        None => Some(len),
    }
}

/// What a submission whose records take `len` payload bytes holds of the queue's byte bound
/// until it is answered: those bytes, record and entry headers included, and its group's row in
/// the frame's persist record. An empty entry costs its header and an update of no records its
/// row, so a flood of them fills the bound as surely as large ones do (audit S03).
pub(crate) fn charge(len: usize) -> Option<u64> {
    u64::try_from(len.checked_add(format::PERSIST_GROUP_LEN)?).ok()
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
    Uncertain {
        group: u128,
        mark: format::Start,
    },
    Damaged {
        group: u128,
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
            Copy::Uncertain { group, mark } => Record::Uncertain {
                group: *group,
                mark: *mark,
            },
            Copy::Damaged { group } => Record::Damaged { group: *group },
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
            (Copy::Uncertain { group, .. }, Placed::Record(at)) => {
                out.push(Moved::Uncertain { group: *group, at });
            }
            (Copy::Damaged { group }, Placed::Record(at)) => {
                out.push(Moved::Damaged { group: *group, at });
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
            Owned::Uncertain {
                at: offset,
                group,
                mark,
            } => {
                let live = state
                    .groups
                    .get(group)
                    .and_then(|g| g.uncertain)
                    .is_some_and(|(_, p)| p == at(*offset));
                if live {
                    out.push(Copy::Uncertain {
                        group: *group,
                        mark: *mark,
                    });
                }
            }
            Owned::Damaged { at: offset, group } => {
                if state.damaged.get(group) == Some(&Some(at(*offset))) {
                    out.push(Copy::Damaged { group: *group });
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
    let (live, groups, damaged) = (&mut state.live, &mut state.groups, &mut state.damaged);
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
        Moved::Uncertain { group, at } => {
            if let Some((_, p)) = groups.get_mut(&group).and_then(|g| g.uncertain.as_mut()) {
                live.kill(*p, UNCERTAIN_BYTES);
                *p = place(at)?;
                live.add(*p, UNCERTAIN_BYTES);
            }
        }
        Moved::Damaged { group, at } => {
            if let Some(Some(p)) = damaged.get_mut(&group) {
                live.kill(*p, DAMAGED_BYTES);
                *p = place(at)?;
                live.add(*p, DAMAGED_BYTES);
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

/// Whether an update only frees what its group holds: a removal, a damage fence, or a new start
/// that writes no entry or proposal, as a compaction or a snapshot's install does. Such a frame
/// may take the room the log keeps for making room (docs/design/raft-log.md §5).
fn frees(update: &Update, marks: crate::Marks) -> bool {
    if update.remove || marks.damaged {
        return true;
    }
    update.start.is_some()
        && update.proposals.is_empty()
        && marks.uncertain.is_none()
        && update.entries.as_ref().is_none_or(|e| e.entries.is_empty())
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
    // The fence recovery writes on a damaged group carries nothing else.
    if s.marks.damaged {
        return if *update == Update::default() {
            Ok(false)
        } else {
            Err(invalid("a damage mark carries nothing else"))
        };
    }
    // A damaged group takes nothing but its removal, which is how its replica leaves the
    // device to be rebuilt from its peers.
    if state.damaged.contains_key(&group) && !update.remove {
        return Err(LogError::Damaged(
            "the group's acknowledged records are damaged; it recovers from its peers",
        ));
    }
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
    // A fenced group holds its place among the groups until it is removed.
    let held = state.groups.len().saturating_add(state.damaged.len());
    if new && held.saturating_add(new_groups) >= config.max_groups {
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
pub(crate) fn update_len(group: u128, update: &Update) -> Option<usize> {
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
    marks: crate::Marks,
) -> Option<Placement> {
    let mut placement = Placement::default();
    let mut put = |record: &Record<'_>| {
        *records = records.checked_add(1)?;
        format::put(payload, record)
    };
    if marks.damaged {
        if let Placed::Record(at) = put(&Record::Damaged { group })? {
            placement.damaged = Some(at);
        }
        return Some(placement);
    }
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
    if let Some(mark) = marks.uncertain
        && let Placed::Record(at) = put(&Record::Uncertain { group, mark })?
    {
        placement.uncertain = Some(at);
    }
    Some(placement)
}

/// Applies a durable update to the group's state.
fn apply(
    state: &mut State,
    config: &crate::Config,
    s: &Submission,
    placement: &Placement,
    place: &impl Fn(usize) -> Result<Place, LogError>,
) -> Result<(), LogError> {
    let (group, update) = (s.group, &s.update);
    let live = &mut state.live;
    if s.marks.damaged {
        // Whatever the group held is gone; only the fence is live.
        if let Some(g) = state.groups.remove(&group) {
            for (p, bytes) in g.pieces() {
                live.kill(p, bytes);
            }
        }
        if let Some(at) = placement.damaged {
            let new_at = place(at)?;
            if let Some(Some(old)) = state.damaged.insert(group, Some(new_at)) {
                live.kill(old, DAMAGED_BYTES);
            }
            live.add(new_at, DAMAGED_BYTES);
        }
        return Ok(());
    }
    if update.remove {
        if let Some(Some(at)) = state.damaged.remove(&group) {
            live.kill(at, DAMAGED_BYTES);
        }
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
    // (07 §1.4), and no longer uncertain once it holds what the mark covers.
    let last = g.last().ok_or(LogError::Damaged("an index past u64"))?;
    if let Some((mark, at)) = g.uncertain
        && resolves(mark, last, g.last_term())
    {
        live.kill(at, UNCERTAIN_BYTES);
        g.uncertain = None;
    }
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
    if let (Some(mark), Some(at)) = (s.marks.uncertain, placement.uncertain) {
        if let Some((_, old)) = g.uncertain {
            live.kill(old, UNCERTAIN_BYTES);
        }
        let new_at = place(at)?;
        g.uncertain = Some((mark, new_at));
        live.add(new_at, UNCERTAIN_BYTES);
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
