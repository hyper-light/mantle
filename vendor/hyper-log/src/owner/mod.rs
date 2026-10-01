//! The log's owner: one thread that holds everything the log knows, its groups, its segments,
//! its queue's room and the writer's batch, and that alone reads or changes it (mantle note 32
//! §3.9). Callers reach it by message through a bounded inbox and hear back through their
//! tickets; the device thread hands back each job it was given as a message too. The owner
//! never waits on the device, so it answers a caller while a frame is being flushed, as readers
//! of mantle's lock did.
//!
//! The writer's loop is mantle-log's (`writer.rs`), run as steps between messages (`write.rs`):
//! at the top of the loop, the batch is what was held for this frame and what is queued, that is
//! what the owner has taken from its inbox and admitted since the last batch. Queries and fetches
//! are answered as they come (`read.rs`).
//!
//! The owner keeps the buffers and collections a frame needs from one frame to the next, so the
//! write path allocates nothing a frame once they have grown to the largest frame's needs.

mod read;
mod write;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TryRecvError};
use std::thread::JoinHandle;
use std::time::Instant;

use hyper_block::block::BlockFile;
use hyper_block::buf::AlignedBuf;
use hyper_block::commit::Anticipation;

use crate::codec::Writer as Payload;
use crate::device::{Completion, Job, Look};
use crate::format;
use crate::room::{Room, Take};
use crate::state::State;
use crate::ticket::{Answer, Ticket};
use crate::writer::{Key, Placement, Submission};
use crate::{LogError, Params};

use read::Fetch;
pub(crate) use read::Query;
use write::{Gather, Phase, Unconfirmed};

/// What reaches the owner.
pub(crate) enum Message<F> {
    /// A submission, admitted at once, held to wait for room when `wait`, or refused.
    Submit { submission: Submission, wait: bool },
    /// A question about the log's state, answered through its ticket.
    Query(Query, Ticket),
    /// A job the device has done.
    Done(Completion),
    /// A caller's look at the file, run on the device thread.
    Look(Look<F>),
    /// The log is closing: the owner answers what it holds and ends.
    Close,
}

/// The writer's schedule: what it holds back, its fair queue, its wait for returning
/// submitters, and what it has written.
struct Schedule {
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
    anticipation: Anticipation,
    /// Submissions taken into a batch so far.
    received: u64,
    /// Submissions the last confirmation answered, and those queued when it did.
    answered: u64,
    backlog: u64,
    /// The first batch is the restore of a lost frame at open, which goes in one frame or not
    /// at all (mantle docs/design/raft-log.md §6).
    restoring: bool,
    /// Frames written and flushed since the log opened, and the updates they carried.
    frames: u64,
    updates: u64,
}

/// What the write path keeps from one frame to the next so that it allocates nothing once
/// each has grown to the largest frame's needs: a frame's payload, its persist record, the
/// aligned buffers the device writes, and the batch's ordering scratch.
struct Buffers {
    payload: Payload,
    /// A persist record's groups and its encoding.
    persist: format::Persist,
    record_bytes: Payload,
    /// The aligned buffers the last frame and the last persist record were laid out in: one
    /// frame's bytes at most, a segment and a block.
    frame: Option<AlignedBuf>,
    record: Option<AlignedBuf>,
    /// The ordering's keys and each group's last key (`writer::order`).
    keyed: Vec<(Key, u64, Submission)>,
    last: HashMap<u128, Key>,
    /// Groups a frame has taken an update of.
    seen: HashSet<u128>,
    /// A batch's collection, kept between batches.
    batch: VecDeque<Submission>,
    /// Lists of a frame's updates and where they went, kept between frames: one for each frame
    /// the writer holds at once, the frame on the device and the frame awaiting its confirmation
    /// ([`SPARE_FRAMES`]).
    taken: Vec<Vec<(Submission, Placement)>>,
    /// Waiters room was just handed to.
    admitted: Vec<u64>,
}

/// Frames whose update lists the writer holds at once besides the one it lays out: the frame on
/// the device and the frame flushed and awaiting its confirmation (mantle
/// docs/design/raft-log.md §3).
const SPARE_FRAMES: usize = 2;

pub(crate) struct Owner<F> {
    p: Params,
    state: State,
    room: Room,
    /// Admitted submissions not yet taken into a batch, in the order admitted.
    intake: VecDeque<Submission>,
    /// Submissions waiting for room, by arrival number: at most the room's waiters.
    waiting: HashMap<u64, Submission>,
    phase: Phase,
    gather: Option<Gather>,
    /// The owner waits for a submission to make the next batch of.
    parked: bool,
    schedule: Schedule,
    /// The last frame flushed, while nothing durable yet says its flush completed: the next
    /// frame's persist record will, or a confirmation written when no frame follows at once
    /// (mantle docs/design/raft-log.md §3, §6). Its updates are answered only then.
    unconfirmed: Option<Unconfirmed>,
    buffers: Buffers,
    fenced: bool,
    device: SyncSender<Job<F>>,
    device_thread: Option<JoinHandle<Option<F>>>,
    /// Fetches waiting for the device, the first of them on it: one group's each at most for
    /// every group the log holds, past which a fetch is refused `Busy`.
    fetches: VecDeque<Fetch>,
    /// The lists of a fetch done, kept for the next.
    spare_reads: crate::device::Reads,
    /// Looks at the file waiting for the device, the first on it: one for each caller that
    /// waits in `Log::with_file`.
    looks: VecDeque<Look<F>>,
    looking: bool,
    closing: bool,
}

impl<F: BlockFile + 'static> Owner<F> {
    pub(crate) fn new(
        p: Params,
        state: State,
        room: Room,
        restores: Vec<Submission>,
        device: SyncSender<Job<F>>,
        device_thread: JoinHandle<Option<F>>,
    ) -> Self {
        Self {
            p,
            state,
            room,
            intake: VecDeque::new(),
            waiting: HashMap::new(),
            phase: Phase::Idle,
            gather: None,
            parked: false,
            schedule: Schedule::new(restores),
            unconfirmed: None,
            buffers: Buffers::new(),
            fenced: false,
            device,
            device_thread: Some(device_thread),
            fetches: VecDeque::new(),
            spare_reads: crate::device::Reads::none(),
            looks: VecDeque::new(),
            looking: false,
            closing: false,
        }
    }

    /// Runs until the log closes and nothing is left to answer; gives the file back.
    pub(crate) fn run(mut self, inbox: &Receiver<Message<F>>) -> Option<F> {
        self.step(inbox);
        while !self.finished() {
            match self.next(inbox) {
                Ok(Some(m)) => self.handle(m, inbox),
                Ok(None) => self.gathered(inbox),
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => break,
            }
        }
        self.stop()
    }

    /// The next message, or `None` once the writer's wait for returning submitters is over;
    /// an error once the inbox has closed.
    fn next(&self, inbox: &Receiver<Message<F>>) -> Result<Option<Message<F>>, RecvTimeoutError> {
        let Some(deadline) = self.gather.as_ref().map(|g| g.deadline) else {
            return inbox
                .recv()
                .map(Some)
                .map_err(|_| RecvTimeoutError::Disconnected);
        };
        let left = deadline.saturating_duration_since(Instant::now());
        match inbox.recv_timeout(left) {
            Ok(m) => Ok(Some(m)),
            Err(RecvTimeoutError::Timeout) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Whether the log has closed and the owner holds nothing more to answer.
    fn finished(&self) -> bool {
        self.closing
            && self.parked
            && self.intake.is_empty()
            && self.fetches.is_empty()
            && self.looks.is_empty()
            && !self.looking
    }

    /// Ends the device thread and gives back the file it held.
    fn stop(mut self) -> Option<F> {
        let thread = self.device_thread.take();
        drop(self);
        thread.and_then(|t| t.join().ok().flatten())
    }

    fn handle(&mut self, message: Message<F>, inbox: &Receiver<Message<F>>) {
        self.take(message, inbox);
        self.went_on(inbox);
    }

    /// Acts on one message.
    fn take(&mut self, message: Message<F>, inbox: &Receiver<Message<F>>) {
        match message {
            Message::Submit { submission, wait } => self.submit(submission, wait),
            Message::Query(query, ticket) => self.query(query, ticket),
            Message::Done(completion) => self.done(completion, inbox),
            Message::Look(look) => {
                self.looks.push_back(look);
                self.look();
            }
            Message::Close => self.closing = true,
        }
    }

    /// Handles every message already waiting, without waiting for more.
    fn drain(&mut self, inbox: &Receiver<Message<F>>) {
        loop {
            match inbox.try_recv() {
                Ok(message) => self.take(message, inbox),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return,
            }
        }
    }

    /// Admits a submission, holds it to wait for room, or refuses it.
    fn submit(&mut self, mut s: Submission, wait: bool) {
        if self.fenced {
            s.ticket.answer(Err(LogError::Fenced));
            return;
        }
        let mut admitted = std::mem::take(&mut self.buffers.admitted);
        match self.room.take(s.group, s.bytes, wait, &mut admitted) {
            Ok(Take::Admitted) => {
                s.ticket.admit();
                self.intake.push_back(s);
            }
            Ok(Take::Waiting(seq)) => {
                self.waiting.insert(seq, s);
            }
            Err(e) => s.ticket.answer(Err(e)),
        }
        self.let_in(&mut admitted);
        self.buffers.admitted = admitted;
    }

    /// Tells the waiters room was handed to, each through its own ticket, and queues them.
    fn let_in(&mut self, admitted: &mut Vec<u64>) {
        for seq in admitted.drain(..) {
            if let Some(s) = self.waiting.remove(&seq) {
                s.ticket.admit();
                self.intake.push_back(s);
            }
        }
    }

    /// Answers a submission, and gives back its room in the queue to the waiters it fits.
    fn answer(&mut self, mut s: Submission, result: Result<(), LogError>) {
        s.ticket.answer(result.map(|()| Answer::Durable));
        let mut admitted = std::mem::take(&mut self.buffers.admitted);
        self.room.release(s.group, s.bytes, &mut admitted);
        self.let_in(&mut admitted);
        self.buffers.admitted = admitted;
    }

    /// Fences the log: no submission is taken from here on, and every waiter hears it.
    fn fence(&mut self) {
        self.fenced = true;
        let mut fenced = Vec::new();
        self.room.fence(&mut fenced);
        for seq in fenced {
            if let Some(mut s) = self.waiting.remove(&seq) {
                s.ticket.answer(Err(LogError::Fenced));
            }
        }
    }

    fn done(&mut self, completion: Completion, inbox: &Receiver<Message<F>>) {
        match completion {
            Completion::Sweep(swept) => self.swept(swept, inbox),
            Completion::Frame {
                frame,
                record,
                result,
                took_ns,
            } => {
                self.buffers.frame = Some(frame);
                self.buffers.record = Some(record);
                self.written(result, took_ns, inbox);
            }
            Completion::Confirm { record, result } => {
                self.buffers.record = Some(record);
                self.confirmed(result, inbox);
            }
            Completion::Read(reads) => self.read(reads),
            Completion::Looked => {
                self.looking = false;
                self.look();
            }
        }
    }

    /// Hands the device the next caller's look at the file, one at a time.
    fn look(&mut self) {
        if self.looking {
            return;
        }
        let Some(look) = self.looks.pop_front() else {
            return;
        };
        // A device that ended drops the look, and its caller hears the log closed.
        self.looking = self.device.try_send(Job::Look(look)).is_ok();
    }
}

impl Schedule {
    fn new(restores: Vec<Submission>) -> Self {
        Self {
            restoring: !restores.is_empty(),
            held: restores.into(),
            virtual_time: 0,
            finish: HashMap::new(),
            walks: 0,
            fruitless: 0,
            anticipation: Anticipation::new(),
            received: 0,
            answered: 0,
            backlog: 0,
            frames: 0,
            updates: 0,
        }
    }
}

impl Buffers {
    fn new() -> Self {
        Self {
            payload: Payload::default(),
            persist: format::Persist::default(),
            record_bytes: Payload::default(),
            frame: None,
            record: None,
            keyed: Vec::new(),
            last: HashMap::new(),
            seen: HashSet::new(),
            batch: VecDeque::new(),
            taken: Vec::new(),
            admitted: Vec::new(),
        }
    }
}
