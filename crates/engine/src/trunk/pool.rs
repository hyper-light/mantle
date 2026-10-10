//! Maintenance workers: a shard's packing of full memtables and its trunk's compactions run on
//! threads of their own, so the shard's thread keeps taking puts while they run, as RocksDB's
//! flushes and compactions run on background threads beside its writers
//! (`max_background_jobs`; SILK, Balmau et al., USENIX ATC 2019, schedules them so). A job is a
//! compaction's inputs and range, or a full memtable's entries to pack; a worker runs it on its
//! own store over its own handle on the shard's file ([`Store::worker`]), writing into extents the
//! shard granted it, and hands back the branches made, named by the ticket the job's send
//! returned ([`Ticket`]): a worker freed while its result waits for its owner can take that
//! owner's next job, so the worker alone does not say which job a result answers. Nothing is
//! shared between threads: the shard keeps its memtables and trunk, a job owns its inputs'
//! descriptors, and a packing's entries reach the worker as buffers moved through a channel and
//! moved back for reuse. The trunk and the memtables change only on the shard's thread, when a
//! result is taken, so a read between never sees one half changed.
//!
//! How many workers: Little's law, L = λW (Little, Operations Research 9(3), 1961). Between one
//! cascade's start and the next, the shard's thread offers maintenance its memtables at the pace
//! puts fill them, and the workers spent `busy` nanoseconds on jobs; the shard itself ran for
//! `period` nanoseconds of that, its waits on the workers left out, since those measure too few
//! workers rather than demand. Keeping up takes `busy / period` workers on average, so the pool
//! admits the ceiling of that measured ratio as active jobs, at least one and at most the cores the OS reports
//! less the shard's own (`std::thread::available_parallelism`). A worker is started only when a
//! job is ready and none is free, so a pool never holds more threads than jobs were ever out at
//! once. Idle seats keep their warm stores and reservations until terminal retirement, so
//! reducing demand cannot overlap an old store's destruction with a replacement in its seat.

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::JoinHandle;
use std::time::Instant;

use hyper_block::issuer::Attacher;
use hyper_rt::sync::{ChannelReceiver, Sender};

use crate::branch::filter::Keys;
use crate::branch::merge::Compaction;
use crate::branch::{Branch, Builder, Op};
use crate::error::Error;
use crate::store::{Config, IntoFile, Refill, Store};
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

/// A job handed to a worker: the worker, whose buffers a packing's feed takes back, and the
/// job's number among every job the pool has handed out. A worker freed while its result waits
/// for its owner can take that owner's next job, so only the number tells two of an owner's jobs
/// apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ticket {
    pub worker: usize,
    pub job: u64,
}

/// A job back, as the ticket its send returned names it: its result; the extents granted it
/// beyond its job's grant (its top-ups), which are the owner's to release if it failed; and the
/// packing buffers the job gave back, the job's own, since the worker sends every one before
/// its result.
#[derive(Debug)]
pub struct Back {
    pub ticket: Ticket,
    pub result: Result<Output, Error>,
    /// An issued-write failure or worker unwind remains visible when input was abandoned.
    pub physical_error: Option<Error>,
    pub topped: Vec<u64>,
    pub buffers: Vec<Vec<u8>>,
}

/// What a worker tells the shard: its grant is spent and it waits for `n` more extents, or its
/// job is done.
#[derive(Debug)]
pub(crate) enum Message {
    /// Cold preparation acknowledges actual Store creation and issuer attachment.
    Ready {
        worker: usize,
        result: Result<(), Error>,
    },
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
        physical_error: Option<Error>,
    },
    /// Terminal native attachment result, after the owner's last Done was consumed.
    Retired {
        worker: usize,
        result: Result<(), Error>,
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
    back: Sender<Message>,
    /// The device's issuer and the batches the worker may have out on it, when the shard has
    /// one: its store's writes then go to the device while the worker builds on.
    attach: Option<(Attacher, usize)>,
    /// Only cold preparation reports startup before admitting a job.
    prepare: bool,
}

/// Starts a worker's thread on its seat: the shard's way to open its own file again.
pub type Spawn = Box<dyn FnMut(Seat) -> Result<JoinHandle<()>, Error> + Send>;

/// A worker's loop: each job run on one store, kept across jobs so its buffers stay warm, until
/// the shard drops its end of the job channel.
pub fn serve<F: BlockFile + 'static>(file: F, config: Config, seat: Seat) {
    let Seat {
        id,
        jobs,
        more,
        back,
        attach,
        prepare,
    } = seat;
    let worker = Store::worker(file, config).and_then(|mut store| {
        if let Some((attacher, batches)) = &attach {
            store.attach_through(attacher, *batches)?;
        }
        Ok(store)
    });
    let mut store = match worker {
        Ok(s) => s,
        Err(error) => {
            let message = if prepare {
                Message::Ready {
                    worker: id,
                    result: Err(error),
                }
            } else {
                Message::Done {
                    worker: id,
                    result: Err(error),
                    physical_error: None,
                }
            };
            drop(back.blocking_send(message));
            return;
        }
    };
    if prepare
        && back
            .blocking_send(Message::Ready {
                worker: id,
                result: Ok(()),
            })
            .is_err()
    {
        return;
    }
    let asks = back.clone();
    store.set_refill(Refill(Box::new(move |n| {
        asks.blocking_send(Message::Need { worker: id, n })
            .map_err(|_| gone("ask the shard for extents"))?;
        more.recv()
            .map_err(|_| gone("take extents from the shard"))?
    })));
    while let Ok(job) = jobs.recv() {
        let t = Instant::now();
        let (ran, unwound) = run_guarded(&mut store, &job, &back, id);
        // Whatever the job's end, every run it handed the device is answered before its result
        // goes back: the shard releases a failed job's extents once it has the result, and the
        // next job starts the allocator over, so none of this job's writes may land after. The
        // job's own failure is the one reported.
        let drained = store.drain();
        // An issued failure precedes the unwind. A worker unwind is non-abandonable too,
        // so terminal cleanup must retain it even when a partial input was canceled.
        let physical_error = drained.as_ref().err().cloned().or_else(|| {
            if unwound {
                ran.as_ref().err().cloned()
            } else {
                None
            }
        });
        // A job's time less its waits for a packing's entries: the work it did, which the pool
        // sizes itself by, not the shard's pace.
        let result = ran.and_then(|(parts, waited)| {
            drained.map(|()| Output {
                parts,
                unused: store.unused_grant(),
                end: store.end(),
                ns: ns(Instant::now().saturating_duration_since(t)).saturating_sub(waited),
            })
        });
        // A packing's channels close before the result is sent, so every buffer still held is
        // back on the shard's side when it takes the result.
        drop(job);
        let done = Message::Done {
            worker: id,
            result,
            physical_error,
        };
        if unwound {
            // Close admission before Done: its owner can immediately try this same seat.
            // Cursors/runs that unwound did not return every loan, so never reuse the Store.
            drop(jobs);
            drop(back.blocking_send(done));
            retire(store, &back, id);
            return;
        }
        if back.blocking_send(done).is_err() {
            return;
        }
    }
    retire(store, &back, id);
}

fn retire<F: BlockFile>(store: Store<F>, back: &Sender<Message>, id: usize) {
    // Raw Drop is unacknowledged cleanup. Explicit terminal retirement must report
    // the worker attachment's lifecycle error before its return sender reaches EOF.
    // Normal close has consumed Done; after unwind a bounded sender waits for its slot.
    match store.into_file() {
        IntoFile::Finished { file, result } => {
            drop(back.blocking_send(Message::Retired { worker: id, result }));
            // EOF and the native retirement receipt still follow this file's Drop/TLS.
            drop(file);
        }
        IntoFile::Refused { owner, error } => {
            drop(back.blocking_send(Message::Retired {
                worker: id,
                result: Err(error),
            }));
            // An exceptional refusal keeps the complete owner for cold cleanup; no early
            // physical receipt or actor acknowledgement follows this message alone.
            drop(owner);
        }
    }
}

/// The branches a job made, each with its first key, in key order.
type Parts = Vec<(Vec<u8>, Branch)>;

/// A guarded job keeps the Store, job/grants and accepted transfer ledger outside unwind.
/// Only this job's nested cursor/Builder stack unwinds; its Store is then drained and retired.
fn run_guarded<F: BlockFile>(
    store: &mut Store<F>,
    job: &Job,
    back: &Sender<Message>,
    id: usize,
) -> (Result<(Parts, u64), Error>, bool) {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(store, job, back, id))) {
        Ok(result) => (result, false),
        Err(payload) => {
            let reason = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("foreign callback unwound without a string payload");
            (
                Err(Error::Io {
                    op: "run a maintenance worker job",
                    detail: format!("maintenance worker job unwound: {reason}"),
                }),
                true,
            )
        }
    }
}

/// Runs `job`: the branches made, and the nanoseconds it waited for a packing's entries.
fn run<F: BlockFile>(
    store: &mut Store<F>,
    job: &Job,
    back: &Sender<Message>,
    id: usize,
) -> Result<(Parts, u64), Error> {
    store.begin_job(&job.grant, job.file_end, job.generation)?;
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
                back.blocking_send(Message::Spent { worker: id, buf })
                    .map_err(|_| gone("give the shard a packing buffer back"))?;
            }
            vec![(Vec::new(), b.finish(store)?)]
        }
    };
    Ok((parts, waited))
}

fn gone(what: &'static str) -> Error {
    Error::Io {
        op: what,
        detail: "the other end of a maintenance worker's channel is gone".into(),
    }
}

/// A worker as the shard holds it: its channels' sending ends, its thread, whose job is out on
/// it and that job's number, and the extents topped up to that job.
#[derive(Debug)]
struct Worker {
    jobs: Option<SyncSender<Box<Job>>>,
    more: Option<SyncSender<Result<Vec<u64>, Error>>>,
    thread: Option<JoinHandle<()>>,
    owner: Option<Owner>,
    job: u64,
    topped: Vec<u64>,
    /// Packing buffers it gave back, not yet taken: at most [`FEED_BUFFERS`].
    spent: Vec<Vec<u8>>,
}

/// The shard's workers.
pub struct Pool {
    spawn: Spawn,
    workers: Vec<Option<Worker>>,
    /// The workers' channel back, cloned into each seat; dropped last, so the channel ends once
    /// every worker has. A runtime's channel, so that a shard owned by a runtime task can await
    /// its workers' messages; plain threads send and take on it as on a std channel.
    back: Option<Sender<Message>>,
    messages: ChannelReceiver<Message>,
    /// Jobs back for an owner other than the one taking: at most every job the owners have out
    /// (one node's compactions, the memtables' packings), since a worker freed here can take its
    /// next job before its owner takes this one.
    undelivered: VecDeque<(Owner, Back)>,
    /// Jobs handed to workers since the pool began: the next one's number ([`Ticket`]).
    handed: u64,
    /// The cores the OS reports less the shard's own: the most workers.
    most: usize,
    /// The workers Little's law measured the need for.
    want: usize,
    /// Workers' nanoseconds on jobs, and the shard's own waits on them, since the cascade
    /// started at `since`.
    busy_ns: u64,
    waited_ns: u64,
    since: Option<Instant>,
    /// The device's issuer for the workers started from now on, and the batches each may have
    /// out ([`Self::set_attach`]).
    attach: Option<(Attacher, usize)>,
    /// A nonblocking take found work out but no message yet: a task can await the receiver.
    waiting: bool,
    /// Messages taken from the workers since the pool began. A feeding owner that takes one
    /// after it last found every feed starved feeds again before it waits
    /// ([`ShardDb::feed_from`](crate::shard_db::ShardDb)): the message may have brought a
    /// starved feed's buffer back. Wrapping: only its change is read.
    accepted: u64,
    /// Terminal jobs are abandoned, their refills refused, before the job channels close.
    closing: bool,
    /// Every worker dropped its store and return sender, including its device attachment.
    closed: bool,
    close_error: Option<Error>,
    /// Every charged seat acknowledged startup before any runtime job was admitted.
    prepared: bool,
    /// Handles adopted on the cold caller; runtime retirement sends no native owner.
    retirement: Option<hyper_rt::runtime::RetirementLease>,
}

/// A borrowed wait's share of the pool's measured period, including a request that interrupts
/// it. Dropping the future returns the measurement once without consuming any message.
struct WaitMeasure<'a> {
    waited: &'a mut u64,
    since: Instant,
}

impl Drop for WaitMeasure<'_> {
    fn drop(&mut self) {
        *self.waited = self.waited.saturating_add(ns(self.since.elapsed()));
    }
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
    /// Refused when the runtime has no synchronization cell left for its channel back.
    pub fn new(spawn: Spawn, most: usize) -> Result<Self, Error> {
        let most = most.max(1);
        let (back, messages) = hyper_rt::sync::channel(
            most.saturating_mul(FEED_BUFFERS.saturating_add(1)),
        )
        .map_err(|error| Error::Io {
            op: "make the maintenance workers' channel back",
            detail: error.to_string(),
        })?;
        Ok(Self {
            spawn,
            workers: Vec::new(),
            back: Some(back),
            messages,
            undelivered: VecDeque::new(),
            handed: 0,
            most,
            want: 1,
            busy_ns: 0,
            waited_ns: 0,
            since: None,
            attach: None,
            waiting: false,
            accepted: 0,
            closing: false,
            closed: false,
            close_error: None,
            prepared: false,
            retirement: None,
        })
    }

    /// Workers started from now on attach their stores to the device's issuer through
    /// `attacher`, each with up to `batches` out (`ShardDb::attach`).
    pub fn set_attach(&mut self, attacher: Attacher, batches: usize) {
        self.prepared = false;
        self.attach = Some((attacher, batches));
    }

    /// Preparation is cold: no actor task may open files, spawn threads or wait for startup.
    pub(crate) fn can_prepare(&self) -> Result<(), Error> {
        if self.closing || (!self.prepared && (!self.workers.is_empty() || self.attach.is_none())) {
            return Err(Error::InvalidArgument {
                what: "existing workers incompatible with runtime preparation",
            });
        }
        Ok(())
    }

    pub(crate) fn prepared(&self) -> bool {
        self.prepared && !self.closing && self.retirement.is_some()
    }

    /// Starts every seat already charged by capacity, then accepts its actual startup result.
    /// No job can run yet, so Ready never overlaps the feed/Need/Done channel roles.
    pub(crate) fn prepare(&mut self) -> Result<(), Error> {
        if hyper_rt::registry::current_shard().is_some() {
            return Err(Error::InvalidArgument {
                what: "maintenance worker preparation inside a runtime task",
            });
        }
        self.can_prepare()?;
        if self.prepared {
            return Ok(());
        }
        for _ in 0..self.most {
            let at = self.start(true)?;
            match self
                .messages
                .blocking_recv()
                .map_err(|_| gone("take worker startup"))?
            {
                Message::Ready { worker, result } if worker == at => result?,
                _ => return Err(gone("worker startup with no matching receipt")),
            }
        }
        self.prepared = true;
        Ok(())
    }

    pub(crate) fn adopt_retirement(
        &mut self,
        mut lease: hyper_rt::runtime::RetirementLease,
    ) -> Result<(), Error> {
        if hyper_rt::registry::with_current(|_| ()).is_some() {
            return Err(Error::InvalidArgument {
                what: "native worker adoption inside a runtime task",
            });
        }
        if !self.prepared || self.retirement.is_some() || lease.capacity() != self.most {
            return Err(Error::InvalidArgument {
                what: "native worker adoption outside its prepared capacity",
            });
        }
        let mut threads = Vec::new();
        threads
            .try_reserve_exact(self.most)
            .map_err(|_| Error::LimitExceeded {
                what: "native maintenance retirement handles",
                limit: u64::try_from(self.most).unwrap_or(u64::MAX),
            })?;
        for worker in self.workers.iter_mut().flatten() {
            if let Some(thread) = worker.thread.take() {
                threads.push(thread);
            }
        }
        let accepted = lease.adopt(&mut threads);
        if lease.adopted() {
            // Even a receipt failure after acceptance keeps the native owner in Runtime.
            self.retirement = Some(lease);
        } else {
            let mut returned = threads.into_iter();
            for worker in self.workers.iter_mut().flatten() {
                if worker.thread.is_none() {
                    worker.thread = returned.next();
                }
            }
        }
        accepted.map_err(|error| Error::Io {
            op: "adopt native maintenance threads",
            detail: error.to_string(),
        })
    }

    pub(crate) fn retirement_owned(&self) -> bool {
        self.retirement.is_some()
    }

    pub(crate) fn clear_wait(&mut self) {
        self.waiting = false;
    }

    pub(crate) fn waiting(&self) -> bool {
        self.waiting && (self.closing || self.workers.iter().flatten().any(|w| w.owner.is_some()))
    }

    pub(crate) fn want_message(&mut self) {
        self.waiting = true;
    }

    /// Canceling this borrowed receive leaves the message on the channel. The caller routes
    /// a received message with its store before returning to the task's next request.
    pub(crate) async fn receive(&mut self) -> Result<Option<Message>, Error> {
        if self.closed
            && let Some(retirement) = &mut self.retirement
        {
            if let Err(error) = retirement.wait().await {
                // A matching failed join still proves retirement. Route it through
                // close_paced so a prior physical failure remains the first error.
                if matches!(error, hyper_rt::RtError::NotOnShardThread)
                    || !matches!(retirement.try_result(), Ok(Some(Err(_))))
                {
                    return Err(Error::Io {
                        op: "retire native maintenance threads",
                        detail: error.to_string(),
                    });
                }
                self.close_error.get_or_insert(Error::Io {
                    op: "join native maintenance threads",
                    detail: error.to_string(),
                });
            }
            self.waiting = false;
            return Ok(None);
        }
        let _measure = WaitMeasure {
            waited: &mut self.waited_ns,
            since: Instant::now(),
        };
        match self.messages.recv().await {
            Ok(message) => Ok(Some(message)),
            Err(hyper_rt::sync::SyncError::Closed(())) if self.closing && self.back.is_none() => {
                self.closed = true;
                self.waiting = false;
                Ok(None)
            }
            Err(error) => Err(Error::Io {
                op: "take a worker's message",
                detail: format!("{error:?}"),
            }),
        }
    }

    /// Closing a partial feed makes its worker finish that input; no new extent grant is
    /// made after this point. Existing grants stay owned until every job is physically drained.
    pub(crate) fn begin_close(&mut self) {
        self.closing = true;
        for (_, back) in &mut self.undelivered {
            if let Some(error) = back.physical_error.take() {
                self.close_error.get_or_insert(error);
            }
        }
    }

    pub(crate) fn note_close_error(&mut self, error: Error) {
        self.close_error.get_or_insert(error);
    }

    /// Takes terminal results without applying their unfinished outputs, then closes the
    /// idle workers' channels. False awaits the same return receiver, including its final EOF.
    pub(crate) fn close_paced<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
    ) -> Result<bool, Error> {
        self.begin_close();
        loop {
            match self.next(store, false) {
                Ok(true) => {}
                Ok(false) => break,
                Err(error) => {
                    self.close_error.get_or_insert(error);
                    break;
                }
            }
        }
        if self.workers.iter().flatten().any(|w| w.owner.is_some()) {
            self.waiting = true;
            return Ok(false);
        }
        // Done is sent after the worker Store drained every transfer. Closing its job
        // receiver now ends the loop; EOF follows its Store/attachment destruction.
        for worker in self.workers.iter_mut().flatten() {
            worker.jobs = None;
            worker.more = None;
        }
        self.back = None;
        if !self.closed {
            match self.next(store, false) {
                Ok(_) => {}
                Err(error) => {
                    self.close_error.get_or_insert(error);
                }
            }
        }
        if self.closed {
            if let Some(retirement) = &mut self.retirement {
                retirement.request().map_err(|error| Error::Io {
                    op: "signal native maintenance retirement",
                    detail: error.to_string(),
                })?;
                match retirement.try_result().map_err(|error| Error::Io {
                    op: "take native maintenance retirement",
                    detail: error.to_string(),
                })? {
                    None => {
                        self.waiting = true;
                        return Ok(false);
                    }
                    Some(Err(error)) => {
                        self.close_error.get_or_insert(Error::Io {
                            op: "join native maintenance threads",
                            detail: error.to_string(),
                        });
                    }
                    Some(Ok(())) => {}
                }
            }
            return self.close_error.take().map_or(Ok(true), Err);
        }
        self.waiting = true;
        Ok(false)
    }

    /// The same terminal state for a plain thread: each unfinished pass takes one worker
    /// receipt, waiting for it. An error is reported only after the channel's terminal EOF.
    pub(crate) fn close_blocking<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
    ) -> Result<(), Error> {
        if hyper_rt::registry::with_current(|_| ()).is_some() {
            return Err(Error::InvalidArgument {
                what: "blocking maintenance retirement inside a runtime task",
            });
        }
        while !self.close_paced(store)? {
            if self.closed
                && let Some(retirement) = &mut self.retirement
            {
                if let Err(error) = retirement.wait_blocking() {
                    self.close_error.get_or_insert(Error::Io {
                        op: "retire native maintenance threads",
                        detail: error.to_string(),
                    });
                }
                continue;
            }
            if let Err(error) = self.next(store, true) {
                self.close_error.get_or_insert(error);
            }
        }
        Ok(())
    }

    /// The cores the OS reports less the shard's own thread, at least one.
    pub fn cores() -> usize {
        std::thread::available_parallelism()
            .map_or(1, std::num::NonZero::get)
            .saturating_sub(1)
            .max(1)
    }

    /// The admitted worker maximum, including workers that may start in a later cascade.
    pub(crate) fn capacity(&self) -> usize {
        self.most
    }

    /// Warm worker seats held, each charged until its terminal Store/attachment retirement.
    pub fn workers(&self) -> usize {
        self.workers.iter().filter(|w| w.is_some()).count()
    }

    /// The active workers the measured need asks for; idle warm seats may exceed it.
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
        // Idle seats stay warm and charged. Dropping one here could leave its Store alive
        // behind device detach while a replacement reused the same admitted seat.
    }

    /// Whether a job handed now would go out: a worker is free, or another may start, within
    /// the measured need or, when `waiting` (the shard would otherwise wait on a job no worker
    /// takes, itself evidence the need is more), within the cores.
    pub fn can_take(&self, waiting: bool) -> bool {
        if self.closing {
            return false;
        }
        let bound = if waiting { self.most } else { self.want };
        self.workers
            .iter()
            .flatten()
            .filter(|w| w.owner.is_some())
            .count()
            < bound
    }

    /// Hands `job` to a free worker for `owner`, starting one if none is free within the bound
    /// [`Self::can_take`] states for `waiting`: the job's ticket, which its result carries back,
    /// or the job back when every worker is busy. Every hard refusal returns its owned job too,
    /// before any grant is lost.
    pub fn send(
        &mut self,
        job: Box<Job>,
        owner: Owner,
        waiting: bool,
    ) -> Result<Result<Ticket, Box<Job>>, (Error, Box<Job>)> {
        if hyper_rt::registry::current_shard().is_some() && !self.prepared() {
            return Err((
                Error::InvalidArgument {
                    what: "runtime worker admission before preparation",
                },
                job,
            ));
        }
        if self.closing {
            return Err((gone("hand a closing worker its job"), job));
        }
        let Some(next) = self.handed.checked_add(1) else {
            return Err((
                Error::InvalidArgument {
                    what: "a maintenance job past the pool's job numbers",
                },
                job,
            ));
        };
        let bound = if waiting { self.most } else { self.want };
        if self
            .workers
            .iter()
            .flatten()
            .filter(|w| w.owner.is_some())
            .count()
            >= bound
        {
            return Ok(Err(job));
        }
        let free = self
            .workers
            .iter()
            .position(|w| w.as_ref().is_some_and(|w| w.owner.is_none()));
        let at = match free {
            Some(at) => at,
            None if self.workers() < self.most => match self.start(false) {
                Ok(at) => at,
                Err(error) => return Err((error, job)),
            },
            None => return Ok(Err(job)),
        };
        let Some(w) = self.workers.get_mut(at).and_then(Option::as_mut) else {
            return Err((gone("hand a worker its job"), job));
        };
        let Some(jobs) = w.jobs.as_ref() else {
            return Err((gone("hand a closing worker its job"), job));
        };
        match jobs.try_send(job) {
            Ok(()) => {}
            Err(std::sync::mpsc::TrySendError::Full(job)) => return Ok(Err(job)),
            Err(std::sync::mpsc::TrySendError::Disconnected(job)) => {
                return Err((gone("hand a worker its job"), job));
            }
        }
        w.owner = Some(owner);
        w.job = self.handed;
        w.topped.clear();
        w.spent.clear();
        let ticket = Ticket {
            worker: at,
            job: self.handed,
        };
        self.handed = next;
        Ok(Ok(ticket))
    }

    fn start(&mut self, prepare: bool) -> Result<usize, Error> {
        if hyper_rt::registry::current_shard().is_some() {
            return Err(Error::InvalidArgument {
                what: "maintenance worker startup inside a runtime task",
            });
        }
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
            attach: self.attach.clone(),
            prepare,
        };
        let thread = (self.spawn)(seat)?;
        let w = Worker {
            jobs: Some(jobs),
            more: Some(more),
            thread: Some(thread),
            owner: None,
            job: 0,
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
        blocking_wait_allowed(wait)?;
        loop {
            if let Some(at) = self.undelivered.iter().position(|(o, _)| *o == owner)
                && let Some((_, back)) = self.undelivered.swap_remove_back(at)
            {
                return Ok(Some(back));
            }
            if !self.next(store, wait && self.out_for(owner))? {
                // Compactions own their inputs. Packing can still need its owner to feed it,
                // so its caller marks a result wait only once the feed is closed.
                if owner == Owner::Trunk && self.out_for(owner) {
                    self.waiting = true;
                }
                return Ok(None);
            }
        }
    }

    /// A packing buffer the job `ticket` names gave back: waiting for one when `wait`. None once
    /// that job is back, its result held or taken, since its worker may then hold another
    /// job's buffers; or, without `wait`, when none has come.
    pub fn buffer<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        ticket: Ticket,
        wait: bool,
    ) -> Result<Option<Vec<u8>>, Error> {
        blocking_wait_allowed(wait)?;
        loop {
            let w = self
                .workers
                .get_mut(ticket.worker)
                .and_then(Option::as_mut)
                .ok_or(gone("take a packing buffer"))?;
            if w.owner.is_none() || w.job != ticket.job {
                return Ok(None);
            }
            if let Some(b) = w.spent.pop() {
                return Ok(Some(b));
            }
            if !self.next(store, wait)? {
                self.waiting = true;
                return Ok(None);
            }
        }
    }

    /// Takes one message from the workers, waiting for one when `wait`: an ask for extents is
    /// answered from `store`, a buffer kept for its worker's packing, a result kept for its
    /// owner. Whether one was taken.
    fn next<F: BlockFile>(&mut self, store: &mut Store<F>, wait: bool) -> Result<bool, Error> {
        let message = match self.messages.try_recv() {
            Ok(Some(m)) => m,
            Ok(None) if wait => {
                let t = Instant::now();
                let m = match self.messages.blocking_recv() {
                    Ok(message) => message,
                    Err(_) if self.closing && self.back.is_none() => {
                        self.closed = true;
                        self.waiting = false;
                        return Ok(false);
                    }
                    Err(_) => return Err(gone("take a worker's message")),
                };
                let waited = ns(Instant::now().saturating_duration_since(t));
                self.waited_ns = self.waited_ns.saturating_add(waited);
                m
            }
            Ok(None) => return Ok(false),
            Err(_) if self.closing && self.back.is_none() => {
                self.closed = true;
                self.waiting = false;
                return Ok(false);
            }
            Err(_) => return Err(gone("take a worker's message")),
        };
        self.accept(store, message)
    }

    pub(crate) fn accept<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        message: Message,
    ) -> Result<bool, Error> {
        self.waiting = false;
        self.accepted = self.accepted.wrapping_add(1);
        match message {
            Message::Ready { result, .. } => result?,
            Message::Retired { worker, result } => {
                if self.workers.get(worker).and_then(Option::as_ref).is_none() {
                    return Err(gone("take a worker's terminal result"));
                }
                if let Err(error) = result {
                    self.close_error.get_or_insert(error);
                }
            }
            Message::Need { worker, n } => {
                let more = if self.closing {
                    Err(gone("give a closing worker extents"))
                } else {
                    store.grant(n)
                };
                let w = self
                    .workers
                    .get_mut(worker)
                    .and_then(Option::as_mut)
                    .ok_or(gone("give a worker extents"))?;
                if let Ok(extents) = &more {
                    w.topped.extend_from_slice(extents);
                }
                // One Need waits for its one reply. A refused reply still belongs to
                // w.topped until Done proves physical quiescence; never wait in the owner.
                match w
                    .more
                    .as_ref()
                    .ok_or(gone("give a closing worker extents"))?
                    .try_send(more)
                {
                    Ok(()) => {}
                    Err(std::sync::mpsc::TrySendError::Full(_)) => {
                        return Err(Error::InvalidArgument {
                            what: "a maintenance worker's top-up still pending",
                        });
                    }
                    Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                        return Err(gone("give a worker extents"));
                    }
                }
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
            Message::Done {
                worker,
                result,
                physical_error,
            } => {
                // A canceled request can consume Done before closing starts. Physical
                // failures and worker unwinds stay terminal even after that Back is taken.
                if let Some(error) = &physical_error {
                    self.close_error.get_or_insert_with(|| error.clone());
                }
                let w = self
                    .workers
                    .get_mut(worker)
                    .and_then(Option::as_mut)
                    .ok_or(gone("take a worker's result"))?;
                let of = w.owner.take().ok_or(Error::InvalidArgument {
                    what: "a maintenance worker's result for no job",
                })?;
                let ticket = Ticket { worker, job: w.job };
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
                        ticket,
                        result,
                        physical_error,
                        topped,
                        buffers,
                    },
                ));
            }
        }
        Ok(true)
    }

    /// Waits for one message from the workers, answered or kept as [`Self::take`] does: false,
    /// with nothing taken, when no worker has a job out to send one. `open` names the workers
    /// whose packing feeds are still open. Refused while one of them has a buffer back in hand:
    /// a packing worker sends nothing while it waits for its feed, so the wait could end only by
    /// its caller feeding it with that buffer.
    pub fn wait_any<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        open: impl IntoIterator<Item = usize>,
    ) -> Result<bool, Error> {
        blocking_wait_allowed(true)?;
        if !self.workers.iter().flatten().any(|w| w.owner.is_some()) {
            return Ok(false);
        }
        if open.into_iter().any(|worker| {
            self.workers
                .get(worker)
                .and_then(Option::as_ref)
                .is_some_and(|w| w.owner.is_some() && !w.spent.is_empty())
        }) {
            return Err(Error::InvalidArgument {
                what: "a wait for a packing worker's message while its buffer is back in hand",
            });
        }
        self.next(store, true)
    }

    /// Messages taken from the workers since the pool began, wrapping: only its change is read.
    pub fn accepted(&self) -> u64 {
        self.accepted
    }

    /// The packing buffers worker `worker` gave back that are held here, not yet taken.
    pub fn spent(&self, worker: usize) -> usize {
        self.workers
            .get(worker)
            .and_then(Option::as_ref)
            .map_or(0, |w| w.spent.len())
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
        if let Some(retirement) = &mut self.retirement {
            // Cold adoption removed every handle from these seats. During exceptional
            // service Drop, closing both producer ends then dropping messages lets any
            // blocked worker publication fail; the independent owner still joins it.
            self.workers.clear();
            self.back = None;
            if hyper_rt::registry::with_current(|_| ()).is_none() {
                while self.messages.blocking_recv().is_ok() {}
                let _ = retirement.wait_blocking();
            } else {
                let _ = retirement.request();
            }
            return;
        }
        // Standalone cold Pools retain their explicit blocking ownership contract.
        let threads: Vec<JoinHandle<()>> = self
            .workers
            .iter_mut()
            .filter_map(|w| w.take().and_then(|mut w| w.thread.take()))
            .collect();
        self.back = None;
        while self.messages.blocking_recv().is_ok() {}
        for t in threads {
            drop(t.join());
        }
    }
}

/// Borrowed blocking APIs refuse before consuming a ready receipt or buffer. Runtime
/// callers keep the same job and await its existing receiver through the paced owner.
fn blocking_wait_allowed(wait: bool) -> Result<(), Error> {
    if wait && hyper_rt::registry::current_shard().is_some() {
        Err(Error::InvalidArgument {
            what: "a synchronous maintenance worker wait inside a runtime task",
        })
    } else {
        Ok(())
    }
}

/// A duration's nanoseconds, saturating.
fn ns(d: std::time::Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "retirement_first_error_native.rs"]
mod retirement_first_error_native;
