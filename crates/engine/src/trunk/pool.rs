//! Maintenance workers: a shard's packing of full memtables and its trunk's compactions run on
//! threads of their own, so the shard's thread keeps taking puts while they run, as RocksDB's
//! flushes and compactions run on background threads beside its writers
//! (`max_background_jobs`; SILK, Balmau et al., USENIX ATC 2019, schedules them so). A job is a
//! compaction's inputs and range, or a full memtable's entries to pack; a worker runs it on its
//! own store over its own handle on the shard's file ([`Store::worker`]), writing into extents the
//! shard granted it, and hands back the branches made. Nothing is shared between threads: the
//! shard keeps its memtables and trunk, a job owns its inputs' descriptors, and a packing's
//! entries reach the worker as buffers moved through a channel and moved back for reuse. The
//! trunk and the memtables change only on the shard's thread, when a result is taken, so a read
//! between never sees one half changed.
//!
//! How many workers: Little's law, L = λW (Little, Operations Research 9(3), 1961). Between one
//! cascade's start and the next, the shard's thread offers maintenance its memtables at the pace
//! puts fill them, and the workers spent `busy` nanoseconds on jobs; the shard itself ran for
//! `period` nanoseconds of that, its waits on the workers left out, since those measure too few
//! workers rather than demand. Keeping up takes `busy / period` workers on average, so the pool
//! keeps the ceiling of that measured ratio, at least one and at most the cores the OS reports
//! less the shard's own (`std::thread::available_parallelism`). A worker is started only when a
//! job is ready and none is free, so a pool never holds more threads than jobs were ever out at
//! once, and one above the measured need is stopped once free.

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
use std::thread::JoinHandle;
use std::time::Instant;

use crate::branch::filter::Keys;
use crate::branch::merge::Compaction;
use crate::branch::{Branch, Builder, Op};
use crate::error::Error;
use crate::store::{Config, Refill, Store};
use hyper_block::block::BlockFile;

/// A compaction for a worker: `inputs` newest first, the descriptors a merge reads (no filters),
/// merged over `[from, end)` into branches of at most `per` entries, tombstones dropped when
/// `drop_tombstones`.
#[derive(Debug)]
pub struct Task {
    pub inputs: Vec<Branch>,
    pub from: Vec<u8>,
    pub end: Option<Vec<u8>>,
    pub drop_tombstones: bool,
    pub per: u64,
}

/// A worker's work: a compaction, or a full memtable's entries to pack into one branch.
#[derive(Debug)]
pub enum Work {
    Compact(Task),
    Pack(Stream),
}

/// A packing's entries as the worker takes them: `entries` in all, in key order, in buffers
/// the shard fills from its memtable as it walks it ([`encode`]) and sends on `full`; each
/// buffer, emptied, goes back on the workers' one channel to the shard, so a shard waiting for
/// one also answers a worker's ask for extents ([`Pool::buffer`]). The shard's end of `full`
/// closing ends the entries.
#[derive(Debug)]
pub struct Stream {
    pub entries: u64,
    pub full: Receiver<Vec<u8>>,
}

/// The buffers a packing's entries reach its worker in: two, so the shard fills one while the
/// worker builds from the other (double buffering, the least that overlaps a producer with its
/// consumer), each the bytes of a run, the store's unit of write (`Store::run_bytes`).
pub const FEED_BUFFERS: usize = 2;

/// An entry's fixed bytes in a packing buffer: key length, value length, operation, hash.
const ENTRY_FIXED: usize = 4 + 4 + 1 + 4;

/// Appends an entry to a packing buffer: its key's and value's lengths, its operation and its
/// key's 32-bit hash, then the key and the value. Refused as too large past 4 GiB a field, and
/// when the buffer's memory cannot grow.
pub fn encode(buf: &mut Vec<u8>, key: &[u8], op: Op, value: &[u8], hash: u32) -> Result<(), Error> {
    let too_large = || Error::LimitExceeded {
        what: "bytes of a packed entry's key or value",
        limit: u64::from(u32::MAX),
    };
    let k = u32::try_from(key.len()).map_err(|_| too_large())?;
    let v = u32::try_from(value.len()).map_err(|_| too_large())?;
    let len = ENTRY_FIXED
        .saturating_add(key.len())
        .saturating_add(value.len());
    buf.try_reserve(len).map_err(|_| Error::LimitExceeded {
        what: "bytes of a packing buffer",
        limit: u64::try_from(buf.len().saturating_add(len)).unwrap_or(u64::MAX),
    })?;
    buf.extend_from_slice(&k.to_le_bytes());
    buf.extend_from_slice(&v.to_le_bytes());
    buf.push(op.byte());
    buf.extend_from_slice(&hash.to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    Ok(())
}

/// Each entry of a packing buffer, in order: `each(key, op, value, hash)`. A buffer not made of
/// whole entries is a typed corruption.
fn decode(
    buf: &[u8],
    mut each: impl FnMut(&[u8], Op, &[u8], u32) -> Result<(), Error>,
) -> Result<(), Error> {
    let bad = || Error::Corruption {
        what: "a packing buffer",
        why: crate::error::Malformed::OutOfRange,
    };
    let u32_at = |b: &[u8], at: usize| -> Result<u32, Error> {
        let bytes = b.get(at..at.saturating_add(4)).ok_or(bad())?;
        let a: [u8; 4] = bytes.try_into().map_err(|_| bad())?;
        Ok(u32::from_le_bytes(a))
    };
    let mut at = 0usize;
    while at < buf.len() {
        let k = usize::try_from(u32_at(buf, at)?).map_err(|_| bad())?;
        let v = usize::try_from(u32_at(buf, at.saturating_add(4))?).map_err(|_| bad())?;
        let op = buf
            .get(at.saturating_add(8))
            .copied()
            .and_then(Op::from_byte)
            .ok_or(bad())?;
        let hash = u32_at(buf, at.saturating_add(9))?;
        let key_at = at.saturating_add(ENTRY_FIXED);
        let value_at = key_at.checked_add(k).ok_or(bad())?;
        let end = value_at.checked_add(v).ok_or(bad())?;
        let key = buf.get(key_at..value_at).ok_or(bad())?;
        let value = buf.get(value_at..end).ok_or(bad())?;
        each(key, op, value, hash)?;
        at = end;
    }
    Ok(())
}

/// A job: its work, written into `grant`, with reads up to `file_end`, its pages sealed for the
/// checkpoint after `generation`.
#[derive(Debug)]
pub struct Job {
    pub work: Work,
    pub grant: Vec<u64>,
    pub file_end: u64,
    pub generation: u64,
}

/// A job's result: the branches made, each with its first key, in key order; the granted
/// extents it never wrote; the file's end past its writes; and the nanoseconds it ran.
#[derive(Debug)]
pub struct Output {
    pub parts: Vec<(Vec<u8>, Branch)>,
    pub unused: Vec<u64>,
    pub end: u64,
    pub ns: u64,
}

/// Who a job's result goes back to: the trunk's cascade, or the shard's packing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Owner {
    Trunk,
    Pack,
}

/// A job back from worker `worker`: its result; the extents granted it beyond its job's grant
/// (its top-ups), which are the owner's to release if it failed; and the packing buffers the
/// job gave back, the job's own, since the worker sends every one before its result.
#[derive(Debug)]
pub struct Back {
    pub worker: usize,
    pub result: Result<Output, Error>,
    pub topped: Vec<u64>,
    pub buffers: Vec<Vec<u8>>,
}

/// What a worker tells the shard: its grant is spent and it waits for `n` more extents, or its
/// job is done.
#[derive(Debug)]
enum Message {
    Need {
        worker: usize,
        n: usize,
    },
    Spent {
        worker: usize,
        buf: Vec<u8>,
    },
    Done {
        worker: usize,
        result: Result<Output, Error>,
    },
}

/// A worker's ends of its channels: its jobs, its grants' top-ups, and the shard's one channel
/// back, which holds at most a worker's packing buffers and one message more each (a worker
/// waits for its answer after an ask, and sends its result last).
#[derive(Debug)]
pub struct Seat {
    pub id: usize,
    jobs: Receiver<Box<Job>>,
    more: Receiver<Result<Vec<u64>, Error>>,
    back: SyncSender<Message>,
}

/// Starts a worker's thread on its seat: the shard's way to open its own file again.
pub type Spawn = Box<dyn FnMut(Seat) -> Result<JoinHandle<()>, Error> + Send>;

/// A worker's loop: each job run on one store, kept across jobs so its buffers stay warm, until
/// the shard drops its end of the job channel.
pub fn serve<F: BlockFile>(file: F, config: Config, seat: Seat) {
    let Seat {
        id,
        jobs,
        more,
        back,
    } = seat;
    let mut store = match Store::worker(file, config) {
        Ok(s) => s,
        Err(error) => {
            drop(back.send(Message::Done {
                worker: id,
                result: Err(error),
            }));
            return;
        }
    };
    let asks = back.clone();
    store.set_refill(Refill(Box::new(move |n| {
        asks.send(Message::Need { worker: id, n })
            .map_err(|_| gone("ask the shard for extents"))?;
        more.recv()
            .map_err(|_| gone("take extents from the shard"))?
    })));
    while let Ok(job) = jobs.recv() {
        let t = Instant::now();
        // A job's time less its waits for a packing's entries: the work it did, which the pool
        // sizes itself by, not the shard's pace.
        let result = run(&mut store, &job, &back, id).map(|(parts, waited)| Output {
            parts,
            unused: store.unused_grant(),
            end: store.end(),
            ns: ns(Instant::now().saturating_duration_since(t)).saturating_sub(waited),
        });
        // A packing's channels close before the result is sent, so every buffer still held is
        // back on the shard's side when it takes the result.
        drop(job);
        if back.send(Message::Done { worker: id, result }).is_err() {
            return;
        }
    }
}

/// The branches a job made, each with its first key, in key order.
type Parts = Vec<(Vec<u8>, Branch)>;

/// Runs `job`: the branches made, and the nanoseconds it waited for a packing's entries.
fn run<F: BlockFile>(
    store: &mut Store<F>,
    job: &Job,
    back: &SyncSender<Message>,
    id: usize,
) -> Result<(Parts, u64), Error> {
    store.begin_job(&job.grant, job.file_end, job.generation);
    let mut waited = 0u64;
    let parts = match &job.work {
        Work::Compact(t) => {
            let mut c = Compaction::new(
                store,
                &t.inputs,
                &t.from,
                t.end.as_deref(),
                t.drop_tombstones,
                t.per,
            )?;
            c.step(store, &t.inputs, u64::MAX)?;
            c.finish(store)?
        }
        Work::Pack(s) => {
            let mut b = Builder::new(store, Keys::Exactly(s.entries))?;
            loop {
                let t = Instant::now();
                let Ok(mut buf) = s.full.recv() else {
                    break;
                };
                waited = waited.saturating_add(ns(Instant::now().saturating_duration_since(t)));
                decode(&buf, |k, op, v, h| b.add_hashed(store, k, op, v, h))?;
                buf.clear();
                back.send(Message::Spent { worker: id, buf })
                    .map_err(|_| gone("give the shard a packing buffer back"))?;
            }
            vec![(Vec::new(), b.finish(store)?)]
        }
    };
    store.drain()?;
    Ok((parts, waited))
}

fn gone(what: &'static str) -> Error {
    Error::Io {
        op: what,
        detail: "the other end of a maintenance worker's channel is gone".into(),
    }
}

/// A worker as the shard holds it: its channels' sending ends, its thread, whose job is out on
/// it, and the extents topped up to that job.
#[derive(Debug)]
struct Worker {
    jobs: SyncSender<Box<Job>>,
    more: SyncSender<Result<Vec<u64>, Error>>,
    thread: Option<JoinHandle<()>>,
    owner: Option<Owner>,
    topped: Vec<u64>,
    /// Packing buffers it gave back, not yet taken: at most [`FEED_BUFFERS`].
    spent: Vec<Vec<u8>>,
}

/// The shard's workers.
pub struct Pool {
    spawn: Spawn,
    workers: Vec<Option<Worker>>,
    /// The workers' channel back, cloned into each seat; dropped last, so the channel ends once
    /// every worker has.
    back: Option<SyncSender<Message>>,
    messages: Receiver<Message>,
    /// Jobs back for an owner other than the one taking: at most a job a worker.
    undelivered: VecDeque<(Owner, Back)>,
    /// The cores the OS reports less the shard's own: the most workers.
    most: usize,
    /// The workers Little's law measured the need for.
    want: usize,
    /// Workers' nanoseconds on jobs, and the shard's own waits on them, since the cascade
    /// started at `since`.
    busy_ns: u64,
    waited_ns: u64,
    since: Option<Instant>,
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("workers", &self.workers)
            .field("most", &self.most)
            .field("want", &self.want)
            .finish_non_exhaustive()
    }
}

impl Pool {
    /// A pool of up to `most` workers, each started by `spawn`; none until a job needs one.
    pub fn new(spawn: Spawn, most: usize) -> Self {
        let most = most.max(1);
        let (back, messages) = sync_channel(most.saturating_mul(FEED_BUFFERS.saturating_add(1)));
        Self {
            spawn,
            workers: Vec::new(),
            back: Some(back),
            messages,
            undelivered: VecDeque::new(),
            most,
            want: 1,
            busy_ns: 0,
            waited_ns: 0,
            since: None,
        }
    }

    /// The cores the OS reports less the shard's own thread, at least one.
    pub fn cores() -> usize {
        std::thread::available_parallelism()
            .map_or(1, std::num::NonZero::get)
            .saturating_sub(1)
            .max(1)
    }

    /// The workers it holds now.
    pub fn workers(&self) -> usize {
        self.workers.iter().filter(|w| w.is_some()).count()
    }

    /// The workers the measured need asks for.
    pub fn want(&self) -> usize {
        self.want
    }

    /// A cascade starts: the last one's measure sets the workers wanted (the module's rule).
    /// With no shard time between two cascades (a drain), every core is wanted.
    pub fn cascade_started(&mut self) {
        let now = Instant::now();
        if let Some(since) = self.since {
            let wall = ns(now.saturating_duration_since(since));
            let period = wall.saturating_sub(self.waited_ns);
            self.want = if period == 0 {
                self.most
            } else {
                usize::try_from(crate::util::div_ceil(self.busy_ns, period).unwrap_or(0))
                    .unwrap_or(usize::MAX)
                    .max(1)
                    .min(self.most)
            };
        }
        self.since = Some(now);
        self.busy_ns = 0;
        self.waited_ns = 0;
        // Workers above the need stop once free.
        let mut keep = 0usize;
        for slot in &mut self.workers {
            if let Some(w) = slot {
                if keep < self.want || w.owner.is_some() {
                    keep = keep.saturating_add(1);
                } else {
                    // Its channels closed, its loop ends; the thread is not joined here, since
                    // it has no job and exits at once.
                    *slot = None;
                }
            }
        }
    }

    /// Whether a job handed now would go out: a worker is free, or another may start, within
    /// the measured need or, when `waiting` (the shard would otherwise wait on a job no worker
    /// takes, itself evidence the need is more), within the cores.
    pub fn can_take(&self, waiting: bool) -> bool {
        let bound = if waiting { self.most } else { self.want };
        self.workers() < bound || self.workers.iter().flatten().any(|w| w.owner.is_none())
    }

    /// Hands `job` to a free worker for `owner`, starting one if none is free within the bound
    /// [`Self::can_take`] states for `waiting`: the worker's number, or the job back when every
    /// worker is busy.
    pub fn send(
        &mut self,
        job: Box<Job>,
        owner: Owner,
        waiting: bool,
    ) -> Result<Result<usize, Box<Job>>, Error> {
        let free = self
            .workers
            .iter()
            .position(|w| w.as_ref().is_some_and(|w| w.owner.is_none()));
        let bound = if waiting { self.most } else { self.want };
        let at = match free {
            Some(at) => at,
            None if self.workers() < bound => self.start()?,
            None => return Ok(Err(job)),
        };
        let w = self
            .workers
            .get_mut(at)
            .and_then(Option::as_mut)
            .ok_or(gone("hand a worker its job"))?;
        w.jobs
            .send(job)
            .map_err(|_| gone("hand a worker its job"))?;
        w.owner = Some(owner);
        w.topped.clear();
        w.spent.clear();
        Ok(Ok(at))
    }

    fn start(&mut self) -> Result<usize, Error> {
        let at = self
            .workers
            .iter()
            .position(Option::is_none)
            .unwrap_or(self.workers.len());
        // Each channel holds what one worker can have waiting: one job, one top-up.
        let (jobs, job_rx) = sync_channel(1);
        let (more, more_rx) = sync_channel(1);
        let back = self.back.clone().ok_or(gone("start a worker"))?;
        let seat = Seat {
            id: at,
            jobs: job_rx,
            more: more_rx,
            back,
        };
        let thread = (self.spawn)(seat)?;
        let w = Worker {
            jobs,
            more,
            thread: Some(thread),
            owner: None,
            topped: Vec::new(),
            spent: Vec::new(),
        };
        match self.workers.get_mut(at) {
            Some(slot) => *slot = Some(w),
            None => self.workers.push(Some(w)),
        }
        Ok(at)
    }

    /// The next job back for `owner`, waiting for one when `wait` and one of its jobs is out,
    /// the wait counted against the shard's period. Meanwhile it answers every worker that asks
    /// for more extents from `store`, and keeps what comes for others for them.
    pub fn take<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        owner: Owner,
        wait: bool,
    ) -> Result<Option<Back>, Error> {
        loop {
            if let Some(at) = self.undelivered.iter().position(|(o, _)| *o == owner)
                && let Some((_, back)) = self.undelivered.swap_remove_back(at)
            {
                return Ok(Some(back));
            }
            if !self.next(store, wait && self.out_for(owner))? {
                return Ok(None);
            }
        }
    }

    /// A packing buffer worker `worker` gave back: waiting for one when `wait` and its job is
    /// still out. None once its job is back (taken with [`Self::take`]) or, without `wait`,
    /// when none has come.
    pub fn buffer<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        worker: usize,
        wait: bool,
    ) -> Result<Option<Vec<u8>>, Error> {
        loop {
            let w = self
                .workers
                .get_mut(worker)
                .and_then(Option::as_mut)
                .ok_or(gone("take a packing buffer"))?;
            if let Some(b) = w.spent.pop() {
                return Ok(Some(b));
            }
            let out = w.owner.is_some();
            if !self.next(store, wait && out)? {
                return Ok(None);
            }
        }
    }

    /// Takes one message from the workers, waiting for one when `wait`: an ask for extents is
    /// answered from `store`, a buffer kept for its worker's packing, a result kept for its
    /// owner. Whether one was taken.
    fn next<F: BlockFile>(&mut self, store: &mut Store<F>, wait: bool) -> Result<bool, Error> {
        let message = match self.messages.try_recv() {
            Ok(m) => m,
            Err(TryRecvError::Empty) if wait => {
                let t = Instant::now();
                let m = self
                    .messages
                    .recv()
                    .map_err(|_| gone("take a worker's message"))?;
                let waited = ns(Instant::now().saturating_duration_since(t));
                self.waited_ns = self.waited_ns.saturating_add(waited);
                m
            }
            Err(TryRecvError::Empty) => return Ok(false),
            Err(TryRecvError::Disconnected) => return Err(gone("take a worker's message")),
        };
        match message {
            Message::Need { worker, n } => {
                let more = store.grant(n);
                let w = self
                    .workers
                    .get_mut(worker)
                    .and_then(Option::as_mut)
                    .ok_or(gone("give a worker extents"))?;
                if let Ok(extents) = &more {
                    w.topped.extend_from_slice(extents);
                }
                w.more
                    .send(more)
                    .map_err(|_| gone("give a worker extents"))?;
            }
            Message::Spent { worker, buf } => {
                let w = self
                    .workers
                    .get_mut(worker)
                    .and_then(Option::as_mut)
                    .ok_or(gone("take a packing buffer"))?;
                if w.spent.len() < FEED_BUFFERS {
                    w.spent.push(buf);
                }
            }
            Message::Done { worker, result } => {
                let w = self
                    .workers
                    .get_mut(worker)
                    .and_then(Option::as_mut)
                    .ok_or(gone("take a worker's result"))?;
                let of = w.owner.take().ok_or(Error::InvalidArgument {
                    what: "a maintenance worker's result for no job",
                })?;
                let topped = std::mem::take(&mut w.topped);
                // The job's buffers, all back before its result on this channel: taken with it
                // now, before the worker can start another job whose buffers would mix in.
                let buffers = std::mem::take(&mut w.spent);
                if let Ok(out) = &result {
                    self.busy_ns = self.busy_ns.saturating_add(out.ns);
                }
                self.undelivered.push_back((
                    of,
                    Back {
                        worker,
                        result,
                        topped,
                        buffers,
                    },
                ));
            }
        }
        Ok(true)
    }

    /// Whether a job of `owner`'s is out, or back and not yet taken.
    pub fn out_for(&self, owner: Owner) -> bool {
        self.workers
            .iter()
            .flatten()
            .any(|w| w.owner == Some(owner))
            || self.undelivered.iter().any(|(o, _)| *o == owner)
    }
}

impl Drop for Pool {
    /// Every worker's channels close and its thread is joined: a job still out runs to its end
    /// first and a top-up it waits for is refused, so each worker ends; the messages they send
    /// meanwhile are taken until the last worker's end closes the channel back.
    fn drop(&mut self) {
        let threads: Vec<JoinHandle<()>> = self
            .workers
            .iter_mut()
            .filter_map(|w| w.take().and_then(|mut w| w.thread.take()))
            .collect();
        self.back = None;
        while self.messages.recv().is_ok() {}
        for t in threads {
            drop(t.join());
        }
    }
}

/// A duration's nanoseconds, saturating.
fn ns(d: std::time::Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}
