//! A node's ranges on hyper-rt's shards (docs/design/engine-structure.md §2, step E2): each
//! range's engine is owned by one task on one shard for its life, and nothing of it is shared.
//! Callers reach a range through its bounded request channel and get their answer back on their
//! own; the range's task serves every request waiting, a batch a wake (p²KVS, research/33 row
//! 4), and spends the time requests leave on its maintenance, a slice at a time (SILK).
//!
//! **Buffers travel.** A request carries its key, value and rows to the range and back, so a
//! client that reuses one allocates nothing per operation once its buffers have grown.
//!
//! **Bounds.** A client has one request out at a time, and a range's channel holds as many
//! requests as the clients the ranges admit (`Ranges::start`'s `clients`), so a send never finds
//! the channel full; a client past the bound is refused when it is made. A range's batch is at
//! most its channel's capacity, and a maintenance slice lasts about the caller's slice budget.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Poll, Waker};

use hyper_block::block::BlockFile;
use hyper_rt::combine::{Either, race2};
use hyper_rt::sync::{ChannelReceiver, Sender, SyncError, channel, channel_with};
use hyper_rt::{Runtime, TaskId};

use crate::error::Error;
use crate::rows::Rows;
use crate::shard_db::{FlushStats, ShardDb};
use crate::store::IoStats;
use crate::trunk::TrunkStats;

/// What a request asks of its range.
#[derive(Debug)]
enum Ask {
    Put,
    Delete,
    Get,
    /// A page of `[key, end)` (to the range's end when not `bounded`), at most `limit` rows.
    Scan {
        bounded: bool,
        limit: usize,
    },
    Checkpoint(u64),
    Flush,
    Stats,
    /// Answered once the range owes no maintenance, or has faulted (its fault the answer):
    /// a client's wait on a fact, with nothing sent meanwhile to interrupt the range's work.
    Settled,
    /// The range's last request: its writes landed, the engine dropped, the task ended.
    Stop,
}

/// A range's counters: its flushes, its trunk's maintenance, its store's I/O.
pub type RangeStats = (FlushStats, TrunkStats, IoStats);

/// One request and, on its way back, its answer.
#[derive(Debug)]
struct Request {
    ask: Ask,
    key: Vec<u8>,
    /// A put's value; a get's answer.
    value: Vec<u8>,
    /// A scan's end, when bounded.
    end: Vec<u8>,
    /// A scan's rows.
    rows: Rows,
    /// A scan's continuation, when `more`.
    next: Vec<u8>,
    more: bool,
    /// A get's: whether the key holds a value.
    found: bool,
    stats: Option<RangeStats>,
    result: Result<(), Error>,
    /// Where the answer goes. The range sends it back inside the answer, so the client's
    /// channel closes when a range drops a request unanswered.
    reply: Option<Sender<Box<Request>>>,
    /// Admission travels with the request, including after its client drops while waiting.
    _lease: Option<Arc<ClientLease>>,
}

#[derive(Debug)]
struct ClientLease(Arc<AtomicUsize>);

impl Drop for ClientLease {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Request {
    fn new() -> Self {
        Self {
            ask: Ask::Flush,
            key: Vec::new(),
            value: Vec::new(),
            end: Vec::new(),
            rows: Rows::new(),
            next: Vec::new(),
            more: false,
            found: false,
            stats: None,
            result: Ok(()),
            reply: None,
            _lease: None,
        }
    }
}

/// What a client on a range's own shard does with the range's engine while the range's task
/// lends it ([`Lend`]): an operation that finishes without waiting, or nothing, and the client
/// takes the request path.
trait Inline {
    /// Applies a put now (true), or leaves it unapplied when it would wait for room (false).
    fn put(&mut self, key: &[u8], value: &[u8], keys: u64) -> Result<bool, Error>;
    /// Applies a deletion now (true), or leaves it unapplied when it would wait (false).
    fn delete(&mut self, key: &[u8], keys: u64) -> Result<bool, Error>;
    /// A get answered now, or none when a page it needs is not in memory.
    fn get(&mut self, key: &[u8], value: &mut Vec<u8>) -> Result<Option<bool>, Error>;
    /// Whether the range owes maintenance: its task has work once it runs.
    fn owed(&self) -> bool;
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any>;
}

impl<F: BlockFile + 'static> Inline for ShardDb<F> {
    fn put(&mut self, key: &[u8], value: &[u8], keys: u64) -> Result<bool, Error> {
        self.put_paced(key, value, keys)
    }

    fn delete(&mut self, key: &[u8], keys: u64) -> Result<bool, Error> {
        self.delete_paced(key, keys)
    }

    fn get(&mut self, key: &[u8], value: &mut Vec<u8>) -> Result<Option<bool>, Error> {
        self.get_now(key, value)
    }

    fn owed(&self) -> bool {
        self.idle_owed()
    }

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}

/// A range's engine its task lends to its shard while it awaits with nothing of the engine
/// borrowed, the slice budget its puts pace their paid work by, and whether a client left it
/// maintenance owed: its task, woken when the client yields, takes the engine back to do it.
struct Lending {
    task: TaskId,
    db: Box<dyn Inline>,
    keys: u64,
    owed: bool,
}

thread_local! {
    /// The engines lent on this thread, at most one a range task its shard holds. The vector is
    /// moved out of its cell for each use and back (`Cell`): no reference into it outlives a call.
    static LENT: std::cell::Cell<Vec<Lending>> = const { std::cell::Cell::new(Vec::new()) };
}

/// A range's engine lent for one await of its task ([`Inline`]): an operation a client on the
/// same shard makes inline, as an event loop runs a call made on its own thread at once (trantor's
/// `EventLoop::runInLoop`), skips the request's round trip (docs/design/engine-structure.md,
/// "Same-shard clients"). Dropped unreclaimed, when the task's future is dropped mid-await, it
/// takes the engine back and drops it there, as the task's own local would have been.
struct Lend<F: BlockFile + 'static> {
    task: TaskId,
    held: bool,
    engine: std::marker::PhantomData<F>,
}

impl<F: BlockFile + 'static> Lend<F> {
    /// Lends `db` for `task`'s await; the engine back when it cannot be lent.
    fn new(task: TaskId, db: Box<ShardDb<F>>, keys: u64) -> Result<Self, Box<ShardDb<F>>> {
        let mut db = Some(db);
        // The engine leaves `db` only into the lending pushed beside it.
        let _ = LENT.try_with(|cell| {
            let mut lent = cell.take();
            if lent.try_reserve(1).is_ok()
                && let Some(db) = db.take()
            {
                lent.push(Lending {
                    task,
                    db,
                    keys,
                    owed: false,
                });
            }
            cell.set(lent);
        });
        match db {
            Some(db) => Err(db),
            None => Ok(Self {
                task,
                held: true,
                engine: std::marker::PhantomData,
            }),
        }
    }

    /// The engine back, and whether a client left it maintenance owed. None only if no lending
    /// of `task` is found, which nothing but a reclaim removes.
    fn reclaim(&mut self) -> Option<(Box<ShardDb<F>>, bool)> {
        if !std::mem::take(&mut self.held) {
            return None;
        }
        take_lent::<F>(self.task)
    }
}

impl<F: BlockFile + 'static> Drop for Lend<F> {
    fn drop(&mut self) {
        if self.held {
            drop(take_lent::<F>(self.task));
        }
    }
}

/// Removes `task`'s lending, its engine and whether it was left maintenance owed.
fn take_lent<F: BlockFile + 'static>(task: TaskId) -> Option<(Box<ShardDb<F>>, bool)> {
    let lending = LENT
        .try_with(|cell| {
            let mut lent = cell.take();
            let found = lent
                .iter()
                .position(|l| l.task == task)
                .map(|at| lent.swap_remove(at));
            cell.set(lent);
            found
        })
        .ok()
        .flatten()?;
    let owed = lending.owed;
    lending
        .db
        .into_any()
        .downcast::<ShardDb<F>>()
        .ok()
        .map(|db| (db, owed))
}

/// Whether `task`'s lent engine was left maintenance owed by a client.
fn lent_owed(task: TaskId) -> bool {
    LENT.try_with(|cell| {
        let lent = cell.take();
        let owed = lent.iter().any(|l| l.task == task && l.owed);
        cell.set(lent);
        owed
    })
    .unwrap_or(false)
}

/// Runs `op` on the engine `task` lends now, with its slice budget: none when it lends none.
/// The first operation to leave the engine maintenance owed marks the lending and wakes its task,
/// which runs once this client yields: one wake an episode of owed maintenance.
fn with_lent<R>(task: TaskId, op: impl FnOnce(&mut dyn Inline, u64) -> R) -> Option<R> {
    let (result, woke) = LENT
        .try_with(|cell| {
            let mut lent = cell.take();
            let done = lent.iter_mut().find(|l| l.task == task).map(|l| {
                let result = op(&mut *l.db, l.keys);
                let woke = !l.owed && l.db.owed();
                l.owed |= woke;
                (result, woke)
            });
            cell.set(lent);
            done
        })
        .ok()
        .flatten()?;
    if woke {
        hyper_rt::registry::wake(task.0);
    }
    Some(result)
}

/// What woke a lending range: a request, or a client that left its engine maintenance owed.
enum Woken {
    Request(Result<Box<Request>, SyncError<()>>),
    Owed,
}

/// A range's task: serves requests in batches, maintenance in between, until stopped or every
/// sender is gone.
async fn serve<F: BlockFile + 'static>(
    db: ShardDb<F>,
    mut requests: ChannelReceiver<Box<Request>>,
    batch: usize,
    mut range: Range,
) {
    // Boxed once, so a lend moves a pointer.
    let mut db = Box::new(db);
    let task = hyper_rt::futures::current_task();
    // Cancellation is latched by leaving this loop. The terminal cleanup below is
    // never raced again against the permanently ready cancellation level.
    let stop = 'serving: loop {
        match hyper_rt::futures::cancellation_requested() {
            Ok(false) => {}
            Ok(true) => break None,
            Err(_) => {
                range.fault.get_or_insert(Error::InvalidArgument {
                    what: "a range actor outside its admitted service task",
                });
                break None;
            }
        }
        if let Some(stop) = range.resume(&mut db) {
            break Some(stop);
        }
        let mut served = 0usize;
        while served < batch {
            match requests.try_recv() {
                Ok(Some(request)) => {
                    match range.answer(&mut db, request).await {
                        Answered::Continue => {}
                        Answered::Stop => break 'serving range.active.take(),
                        Answered::Cancelled => break 'serving None,
                    }
                    served = served.saturating_add(1);
                }
                Ok(None) => break,
                Err(_) => break 'serving None,
            }
        }
        if served == batch {
            hyper_rt::futures::yield_now().await;
            continue;
        }
        if range.fault.is_none() && (!range.pending.is_empty() || db.idle_owed()) {
            if db.idle_owed() {
                range.maintain(&mut db);
            }
            if range.fault.is_none() && db.waiting_for_io() {
                // Only borrowed waits are raced. Range retains every active/pending
                // Request, its buffers, reply and admission lease until physical finish.
                let ready = race2(
                    hyper_rt::futures::cancelled(),
                    race2(requests.recv(), db.wait_completion()),
                )
                .await;
                match ready {
                    Either::First(_) => break None,
                    Either::Second(Either::First(Ok(request))) => {
                        match range.answer(&mut db, request).await {
                            Answered::Continue => {}
                            Answered::Stop => break range.active.take(),
                            Answered::Cancelled => break None,
                        }
                    }
                    Either::Second(Either::First(Err(_))) => break None,
                    Either::Second(Either::Second(Ok(_))) => {}
                    Either::Second(Either::Second(Err(error))) => range.fault = Some(error),
                }
            } else {
                match task.filter(|_| range.lends()) {
                    Some(task) => match Lend::new(task, db, range.slice.keys()) {
                        Ok(mut lend) => {
                            hyper_rt::futures::yield_now().await;
                            let Some((back, _)) = lend.reclaim() else {
                                return;
                            };
                            db = back;
                        }
                        Err(back) => {
                            db = back;
                            hyper_rt::futures::yield_now().await;
                        }
                    },
                    None => hyper_rt::futures::yield_now().await,
                }
            }
            continue;
        }
        range.settle(&mut db);
        let woken = match task.filter(|_| range.lends()) {
            Some(task) => match Lend::new(task, db, range.slice.keys()) {
                Ok(mut lend) => {
                    let ready = {
                        let mut recv = std::pin::pin!(requests.recv());
                        let woken = std::future::poll_fn(|cx| {
                            if lent_owed(task) {
                                return Poll::Ready(Woken::Owed);
                            }
                            recv.as_mut().poll(cx).map(Woken::Request)
                        });
                        race2(hyper_rt::futures::cancelled(), woken).await
                    };
                    let Some((back, _)) = lend.reclaim() else {
                        return;
                    };
                    db = back;
                    ready
                }
                Err(back) => {
                    db = back;
                    race2(hyper_rt::futures::cancelled(), async {
                        Woken::Request(requests.recv().await)
                    })
                    .await
                }
            },
            None => {
                race2(hyper_rt::futures::cancelled(), async {
                    Woken::Request(requests.recv().await)
                })
                .await
            }
        };
        match woken {
            Either::First(_) => break None,
            Either::Second(Woken::Owed) => {}
            Either::Second(Woken::Request(Ok(request))) => {
                match range.answer(&mut db, request).await {
                    Answered::Continue => {}
                    Answered::Stop => break range.active.take(),
                    Answered::Cancelled => break None,
                }
            }
            Either::Second(Woken::Request(Err(_))) => break None,
        }
    };
    finish(*db, requests, stop, range).await;
}

/// An admitted cold bootstrap owns no Db until the caller observes its actual
/// admission. Cancellation does not drop the offer receiver: a late accepted Db
/// must arrive and be retired on this same service task, or the producer closes it.
async fn bootstrap<F: BlockFile + 'static>(
    mut offered: ChannelReceiver<ShardDb<F>>,
    requests: ChannelReceiver<Box<Request>>,
    batch: usize,
    range: Range,
) {
    if let Ok(db) = offered.recv().await {
        serve(db, requests, batch, range).await;
    }
}

/// Once closing, keep all unanswered ownership until worker, owner and file
/// retirement. Closing the owned receiver after retirement drops its bounded
/// queued requests in one terminal step, rather than accepting an endless new drain.
async fn finish<F: BlockFile>(
    mut db: ShardDb<F>,
    requests: ChannelReceiver<Box<Request>>,
    stop: Option<Box<Request>>,
    mut range: Range,
) {
    let fault = range.fault.take();
    let finished = db.finish_async().await;
    if !db.actor_finished() {
        // Prepared ownership returns to its existing reaper/issuer close protocol.
        // No file or Stop acknowledgement is exposed before physical quiescence.
        drop(db);
        drop(stop);
        drop(requests);
        drop(range);
        return;
    }
    // The cold reaper already closed the original; the actor holds only scalar receipts.
    drop(db);
    drop(requests);
    drop(range);
    if let Some(mut request) = stop {
        request.result = fault.map_or(finished, Err);
        Range::reply(request);
    }
}

enum Answered {
    Continue,
    Stop,
    Cancelled,
}

/// A range task's state beside its engine.
struct Range {
    /// The first maintenance failure: every request after it is answered with it, and
    /// maintenance stops, so a failed range never acknowledges a write it may not keep.
    fault: Option<Error>,
    slice: Slice,
    /// Settled requests waiting for the range to owe nothing: at most one a client, each
    /// client having one request out at a time.
    #[expect(
        clippy::vec_box,
        reason = "a request arrives and is answered boxed: unboxed here, each would be copied out and boxed anew"
    )]
    settling: Vec<Box<Request>>,
    /// The unapplied mutation and later mutations/barriers, in arrival order. Each
    /// client owns at most one request, so this and `settling` together cannot exceed
    /// the node's existing client bound. Capacity is reserved before the task starts.
    pending: VecDeque<Box<Request>>,
    clients: usize,
    /// Outside the borrowed query future: cancellation cannot drop its lease early.
    active: Option<Box<Request>>,
    /// Whether it lends its engine to clients on its shard ([`RangesConfig::inline`]).
    inline: bool,
}

enum Started {
    Pending(Box<Request>),
    Replied,
    Stop(Box<Request>),
}

impl Range {
    fn new(clients: usize, slice_ns: u64, inline: bool) -> Result<Self, Error> {
        let refused = || Error::LimitExceeded {
            what: "a range's retained requests",
            limit: u64::try_from(clients).unwrap_or(u64::MAX),
        };
        let mut pending = VecDeque::new();
        pending.try_reserve_exact(clients).map_err(|_| refused())?;
        let mut settling = Vec::new();
        settling.try_reserve_exact(clients).map_err(|_| refused())?;
        Ok(Self {
            fault: None,
            slice: Slice::new(slice_ns),
            settling,
            pending,
            clients,
            active: None,
            inline,
        })
    }

    /// Applies `request` and sends its answer, or retains a stop for terminal cleanup. The shard is
    /// told a client was served, so it spins out its wake window before it parks and the
    /// client's next request costs no kernel wake (hyper-rt `ShardContext::note_activity`).
    async fn answer<F: BlockFile>(
        &mut self,
        db: &mut ShardDb<F>,
        request: Box<Request>,
    ) -> Answered {
        hyper_rt::registry::with_current(|ctx| ctx.note_activity());
        if self.fault.is_none() && matches!(request.ask, Ask::Get | Ask::Scan { .. }) {
            self.active = Some(request);
            let ready = {
                let Some(request) = self.active.as_mut() else {
                    return Answered::Cancelled;
                };
                // Cancellation drops only this borrowed query and its span/page guards.
                // The Request itself remains in Range through finish_async.
                race2(hyper_rt::futures::cancelled(), Self::query(db, request)).await
            };
            match ready {
                Either::First(_) => return Answered::Cancelled,
                Either::Second(result) => {
                    if let Some(mut request) = self.active.take() {
                        request.result = result;
                        Self::reply(request);
                    }
                    return Answered::Continue;
                }
            }
        }
        if !self.pending.is_empty()
            && !matches!(request.ask, Ask::Get | Ask::Scan { .. } | Ask::Stats)
        {
            self.defer(request);
            return Answered::Continue;
        }
        match self.start(db, request) {
            Started::Pending(request) => {
                self.defer(request);
                Answered::Continue
            }
            Started::Replied => Answered::Continue,
            Started::Stop(request) => {
                self.active = Some(request);
                Answered::Stop
            }
        }
    }

    async fn query<F: BlockFile>(db: &mut ShardDb<F>, request: &mut Request) -> Result<(), Error> {
        match &request.ask {
            Ask::Get => db
                .get_async(&request.key, &mut request.value)
                .await
                .map(|found| request.found = found),
            Ask::Scan { bounded, limit } => {
                request.rows.clear();
                let end = bounded.then(|| request.end.as_slice());
                db.scan_async(
                    &request.key,
                    end,
                    *limit,
                    &mut request.rows,
                    &mut request.next,
                )
                .await
                .map(|more| request.more = more)
            }
            _ => Err(Error::InvalidArgument {
                what: "a nonquery in an owned range query",
            }),
        }
    }

    fn defer(&mut self, mut request: Box<Request>) {
        if self.pending.len() >= self.clients {
            request.result = Err(Error::LimitExceeded {
                what: "a range's retained requests",
                limit: u64::try_from(self.clients).unwrap_or(u64::MAX),
            });
            Self::reply(request);
        } else {
            self.pending.push_back(request);
        }
    }

    /// Retries only the oldest unapplied entry. A completed entry is removed before
    /// another request runs, so cancellation of a client's receive never replays it.
    fn resume<F: BlockFile>(&mut self, db: &mut ShardDb<F>) -> Option<Box<Request>> {
        for _ in 0..self.pending.len() {
            let Some(request) = self.pending.pop_front() else {
                break;
            };
            match self.start(db, request) {
                Started::Pending(request) => {
                    self.pending.push_front(request);
                    break;
                }
                Started::Replied => {}
                Started::Stop(request) => return Some(request),
            }
        }
        None
    }

    /// An unapplied request keeps its buffers, answer channel and admission lease. A stop
    /// keeps the same ownership even after a fault, until its writes and workers retire.
    fn start<F: BlockFile>(&mut self, db: &mut ShardDb<F>, mut request: Box<Request>) -> Started {
        if matches!(request.ask, Ask::Stop) {
            return Started::Stop(request);
        }
        if matches!(request.ask, Ask::Settled) && self.fault.is_none() && db.idle_owed() {
            // Answered once nothing is owed ([`Self::settle`]).
            if self.settling.len() >= self.clients {
                request.result = Err(Error::LimitExceeded {
                    what: "a range's settled requests",
                    limit: u64::try_from(self.clients).unwrap_or(u64::MAX),
                });
                Self::reply(request);
            } else {
                self.settling.push(request);
            }
            return Started::Replied;
        }
        let result = match &self.fault {
            Some(fault) => Err(fault.clone()),
            None => apply(db, &mut request, self.slice.keys()),
        };
        if matches!(request.ask, Ask::Checkpoint(_))
            && db.checkpoint_failed()
            && let Err(error) = &result
        {
            self.fault.get_or_insert_with(|| error.clone());
        }
        request.result = match result {
            Ok(false) => return Started::Pending(request),
            Ok(true) => Ok(()),
            Err(error) => Err(error),
        };
        Self::reply(request);
        Started::Replied
    }

    fn reply(mut request: Box<Request>) {
        if let Some(reply) = request.reply.take() {
            request.reply = Some(reply.clone());
            // A client gone has nobody to tell.
            drop(reply.try_send(request));
        }
    }

    /// Answers every settled request waiting, the range owing nothing or faulted: its fault, or
    /// done.
    fn settle<F: BlockFile>(&mut self, db: &mut ShardDb<F>) {
        if self.settling.is_empty() || (self.fault.is_none() && db.idle_owed()) {
            return;
        }
        for mut request in self.settling.drain(..) {
            request.result = self
                .fault
                .as_ref()
                .map_or(Ok(()), |fault| Err(fault.clone()));
            Self::reply(request);
        }
    }

    /// Whether the range lends its engine while it awaits: no fault, no mutation waiting to be
    /// applied (their order is kept), no request in hand.
    fn lends(&self) -> bool {
        self.inline && self.fault.is_none() && self.pending.is_empty() && self.active.is_none()
    }

    /// One slice of maintenance; a failure is kept as the range's fault. A slice that stopped
    /// for a read still in flight leaves [`ShardDb::waiting_for_io`] set for the task to await.
    fn maintain<F: BlockFile>(&mut self, db: &mut ShardDb<F>) {
        let t = std::time::Instant::now();
        let keys = self.slice.keys();
        match db.idle_paced_step(keys) {
            Ok(done) => {
                let ns = u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX);
                self.slice.record(done, ns);
            }
            Err(e) => self.fault = Some(e),
        }
    }
}

/// Applies a request to the engine, its answer into the request.
fn apply<F: BlockFile>(
    db: &mut ShardDb<F>,
    request: &mut Request,
    keys: u64,
) -> Result<bool, Error> {
    match &request.ask {
        Ask::Put => return db.put_paced(&request.key, &request.value, keys),
        Ask::Delete => return db.delete_paced(&request.key, keys),
        Ask::Get => Err(Error::InvalidArgument {
            what: "a runtime point lookup outside its owned async request",
        }),
        Ask::Scan { .. } => Err(Error::InvalidArgument {
            what: "a runtime scan outside its owned async request",
        }),
        Ask::Checkpoint(applied) => return db.checkpoint_paced(*applied, keys),
        Ask::Flush => return db.flush_paced(keys),
        Ask::Stats => {
            request.stats = Some(db.stats());
            Ok(())
        }
        Ask::Settled => Ok(()),
        Ask::Stop => db.land(),
    }
    .map(|()| true)
}

/// The keys a maintenance slice is given: the slice budget at the rate slices have run, so a
/// slice takes about the budget and a request arriving during one waits no longer (SILK's
/// short, preemptible compaction work). Until a slice has been timed, a slice is one key.
struct Slice {
    budget_ns: u64,
    keys: u64,
    ns: u64,
}

impl Slice {
    fn new(budget_ns: u64) -> Self {
        Self {
            budget_ns,
            keys: 0,
            ns: 0,
        }
    }

    fn keys(&self) -> u64 {
        let keys = u128::from(self.budget_ns)
            .saturating_mul(u128::from(self.keys))
            .checked_div(u128::from(self.ns))
            .unwrap_or(1);
        u64::try_from(keys).unwrap_or(u64::MAX).max(1)
    }

    fn record(&mut self, keys: u64, ns: u64) {
        self.keys = self.keys.saturating_add(keys);
        self.ns = self.ns.saturating_add(ns);
    }
}

/// How a node's ranges are run, each number from the runtime's calibration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RangesConfig {
    /// The clients at once, and so each range's channel.
    pub clients: usize,
    /// About how long a maintenance slice runs: the runtime's step budget, so a request waits
    /// on one no longer than on a park.
    pub slice_ns: u64,
    /// How long a client spins for its answer before it parks: the runtime's spin window, the
    /// measured cost of a wake (the 2-competitive spin-then-park rule; Karlin, Li, Manasse and
    /// Owicki, SOSP 1991). An answer that comes within it costs the client no wake.
    pub spin_ns: u64,
    /// Whether a client on a range's own shard makes an operation inline on the range's lent
    /// engine when it can finish without waiting (docs/design/engine-structure.md, "Same-shard
    /// clients"). Off, every operation crosses the range's channel, as one from another shard
    /// does: the request path's semantics alone, a canceled mutation published and kept.
    pub inline: bool,
}

/// A node's ranges, each on a shard of a runtime, and the clients that reach them.
#[derive(Debug)]
pub struct Ranges {
    /// Each range's lowest key, ascending; the first is empty, so every key has a range.
    starts: Vec<Vec<u8>>,
    senders: Vec<Sender<Box<Request>>>,
    tasks: Vec<TaskId>,
    /// Clients live now, at most `clients`.
    live: Arc<AtomicUsize>,
    clients: usize,
    spin_ns: u64,
    /// Whether clients on a range's shard make operations inline ([`RangesConfig::inline`]).
    inline: bool,
}

/// Runtime preflight retains every engine. Later cold startup errors follow cold rollback.
pub enum StartError<F: BlockFile> {
    Refused {
        ranges: Vec<(Vec<u8>, ShardDb<F>)>,
        error: Error,
    },
    Failed(Error),
}

impl<F: BlockFile> std::fmt::Debug for StartError<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused { ranges, error } => f
                .debug_struct("Refused")
                .field("ranges", &ranges.len())
                .field("error", error)
                .finish(),
            Self::Failed(error) => f.debug_tuple("Failed").field(error).finish(),
        }
    }
}

impl Ranges {
    /// Starts each range's engine on a shard of `runtime`, the ranges dealt to its shards in
    /// order. `ranges` gives each range's lowest key, ascending, the first empty.
    pub fn start<F: BlockFile + Send + 'static>(
        runtime: &mut Runtime,
        ranges: Vec<(Vec<u8>, ShardDb<F>)>,
        config: RangesConfig,
    ) -> Result<Self, StartError<F>> {
        if hyper_rt::registry::current_shard().is_some() {
            return Err(StartError::Refused {
                ranges,
                error: Error::InvalidArgument {
                    what: "range cold setup on a runtime",
                },
            });
        }
        Self::start_cold(runtime, ranges, config).map_err(StartError::Failed)
    }

    fn start_cold<F: BlockFile + Send + 'static>(
        runtime: &mut Runtime,
        mut ranges: Vec<(Vec<u8>, ShardDb<F>)>,
        config: RangesConfig,
    ) -> Result<Self, Error> {
        let RangesConfig {
            clients,
            slice_ns,
            spin_ns,
            inline,
        } = config;
        let ordered = ranges.first().is_some_and(|(k, _)| k.is_empty())
            && ranges.windows(2).all(|w| match w {
                [a, b] => a.0 < b.0,
                _ => true,
            });
        if !ordered {
            return Err(Error::InvalidArgument {
                what: "ranges not ascending from the empty key",
            });
        }
        // Refuse an incomplete setup before any engine can start serving on a shard.
        for (_, db) in &ranges {
            db.validate_async_setup()?;
        }
        let mut capacities = Vec::new();
        capacities
            .try_reserve_exact(ranges.len())
            .map_err(|_| Error::LimitExceeded {
                what: "range native retirement groups",
                limit: u64::try_from(ranges.len()).unwrap_or(u64::MAX),
            })?;
        capacities.extend(ranges.iter().map(|(_, db)| db.retirement_capacity()));
        // Spawn the empty retirement owner first. Native handles remain in their Pools
        // on every preparation refusal, and all adoptions precede bootstrap/Db transfer.
        let leases = runtime
            .prepare_retirement_with_original::<F, crate::store::OriginalPhysical>(&capacities)
            .map_err(|error| Error::Io {
                op: "prepare range native retirement",
                detail: error.to_string(),
            })?;
        // File opens, thread startup and attachment handshakes finish in cold setup.
        // No range is published until every engine's actual startup receipts arrived.
        for ((_, db), (lease, mut original)) in ranges.iter_mut().zip(leases) {
            db.prepare_async_backend()?;
            db.adopt_retirement(lease)?;
            let physical = db.prepare_original_retirement_watch()?;
            db.adopt_original(&mut original, physical)?;
        }
        let shards = runtime.shard_ids();
        let mut starts = Vec::with_capacity(ranges.len());
        let mut senders = Vec::with_capacity(ranges.len());
        let mut tasks = Vec::new();
        tasks
            .try_reserve_exact(ranges.len())
            .map_err(|_| Error::LimitExceeded {
                what: "range task identities",
                limit: u64::try_from(ranges.len()).unwrap_or(u64::MAX),
            })?;
        // Every bootstrap is admitted before any Db transfers. A failed admission
        // closes previous cold offers; their service tasks then have no live I/O to drop.
        let mut admitted = Vec::new();
        admitted
            .try_reserve_exact(ranges.len())
            .map_err(|_| Error::LimitExceeded {
                what: "range cold admission handoffs",
                limit: u64::try_from(ranges.len()).unwrap_or(u64::MAX),
            })?;
        for (i, (start, db)) in ranges.into_iter().enumerate() {
            let shard = shards
                .get(i.checked_rem(shards.len()).unwrap_or(0))
                .copied()
                .ok_or(Error::InvalidArgument {
                    what: "a runtime with no shards",
                })?;
            let (sender, receiver) =
                channel_with(clients, 1).map_err(|_| Error::LimitExceeded {
                    what: "a range's request channel",
                    limit: u64::try_from(clients).unwrap_or(u64::MAX),
                })?;
            let range = Range::new(clients, slice_ns, inline)?;
            let (offer, offered) = channel(1).map_err(|_| Error::LimitExceeded {
                what: "a range's single cold engine handoff",
                limit: 1,
            })?;
            let receipt = runtime
                .spawn_service_on_with_receipt(shard, bootstrap(offered, receiver, clients, range))
                .map_err(|_| Error::Gone {
                    what: "the shard a range was placed on",
                })?;
            let task = match receipt.wait_blocking() {
                Ok(hyper_rt::task::Admission::Admitted(task)) => task,
                Ok(hyper_rt::task::Admission::Refused(hyper_rt::RtError::TooManyTasks {
                    capacity,
                })) => {
                    return Err(Error::LimitExceeded {
                        what: "a range actor's task admission",
                        limit: u64::try_from(capacity).unwrap_or(u64::MAX),
                    });
                }
                Ok(hyper_rt::task::Admission::Refused(_))
                | Ok(hyper_rt::task::Admission::Terminated)
                | Err(_) => {
                    return Err(Error::Gone {
                        what: "the shard a range was placed on",
                    });
                }
            };
            admitted.push((start, sender, offer, db, task));
        }
        for (start, sender, offer, db, task) in admitted {
            // The empty cap-one lane has one producer and one engine. Refusal
            // returns/drops Db on this cold caller; it never silently drops on the shard.
            offer.try_send(db).map_err(|_| Error::Gone {
                what: "an admitted range's cold handoff",
            })?;
            drop(offer);
            starts.push(start);
            senders.push(sender);
            tasks.push(task);
        }
        Ok(Self {
            starts,
            senders,
            tasks,
            live: Arc::new(AtomicUsize::new(0)),
            clients,
            spin_ns,
            inline,
        })
    }

    /// The admitted actor identities, in range order, for placement inspection and
    /// cancellation. Each generational handle becomes stale once its actor retires.
    pub fn task_ids(&self) -> &[TaskId] {
        &self.tasks
    }

    /// A client of the ranges, refused past the bound `start` was given.
    pub fn client(&self) -> Result<Client<'_>, Error> {
        if self.live.fetch_add(1, Ordering::AcqRel) >= self.clients {
            self.live.fetch_sub(1, Ordering::AcqRel);
            return Err(Error::LimitExceeded {
                what: "the clients of a node's ranges",
                limit: u64::try_from(self.clients).unwrap_or(u64::MAX),
            });
        }
        let (reply, answers) = match channel(1) {
            Ok(pair) => pair,
            Err(_) => {
                self.live.fetch_sub(1, Ordering::AcqRel);
                return Err(Error::LimitExceeded {
                    what: "a client's answer channel",
                    limit: 1,
                });
            }
        };
        let lease = Arc::new(ClientLease(Arc::clone(&self.live)));
        Ok(Client {
            ranges: self,
            // One request a client, boxed once and reused: a request and its answer cross the
            // range's channel as a pointer, not as the 808 bytes of their buffers' headers and
            // counters (measured: moving them by value was a fifth of a get's time).
            request: Some(Box::new(Request {
                reply: Some(reply),
                _lease: Some(Arc::clone(&lease)),
                ..Request::new()
            })),
            answers,
            barrier: None,
            turn: Turn::default(),
            inlined: 0,
            _lease: lease,
        })
    }

    /// The ranges, each with its lowest key.
    pub fn len(&self) -> usize {
        self.starts.len()
    }

    /// Whether there are no ranges.
    pub fn is_empty(&self) -> bool {
        self.starts.is_empty()
    }

    /// The range that holds `key`.
    fn range_of(&self, key: &[u8]) -> usize {
        self.starts
            .partition_point(|s| s.as_slice() <= key)
            .saturating_sub(1)
    }

    /// Stops every range: its writes landed and its engine dropped, each range's result.
    pub fn stop(self) -> Result<(), Error> {
        let mut first = Ok(());
        {
            let mut client = self.client()?;
            for range in 0..self.starts.len() {
                let r = client.call(range, |r| r.ask = Ask::Stop).map(drop);
                if first.is_ok() {
                    first = r;
                }
            }
        }
        first
    }

    /// Stops every range from a hyper-rt task. A future owning these ranges closes their
    /// request channels if dropped; borrowed client waits can instead be canceled and resumed.
    pub async fn stop_async(self) -> Result<(), Error> {
        let mut client = self.client()?;
        client.barrier_async(BarrierAsk::Stop).await
    }
}

/// A caller's handle on the ranges: one request out at a time, its buffers reused.
#[derive(Debug)]
pub struct Client<'a> {
    ranges: &'a Ranges,
    /// The request and its buffers while none is out; none while a range owns it, or after
    /// that range dropped it unanswered. Canceling a borrowed async wait keeps it outstanding.
    request: Option<Box<Request>>,
    answers: ChannelReceiver<Box<Request>>,
    barrier: Option<Barrier>,
    /// Its turn on its shard for operations made inline ([`Self::turn`]), and those it made.
    turn: Turn,
    inlined: u64,
    /// The client and its orphaned request share admission until both have retired.
    _lease: Arc<ClientLease>,
}

/// A client's turn on its shard for operations made inline ([`Client::turn`]): those left in it,
/// and those made in it and when it began (the shard's clock).
#[derive(Debug, Default)]
struct Turn {
    left: u64,
    done: u64,
    began_ns: u64,
}

#[derive(Clone, Copy, Debug)]
enum BarrierAsk {
    Checkpoint(u64),
    Flush,
    Stop,
}

impl BarrierAsk {
    fn ask(self) -> Ask {
        match self {
            Self::Checkpoint(applied) => Ask::Checkpoint(applied),
            Self::Flush => Ask::Flush,
            Self::Stop => Ask::Stop,
        }
    }
}

#[derive(Debug)]
struct Barrier {
    ask: BarrierAsk,
    /// The next range to attempt; advances after send, including a stop send refusal,
    /// before waiting for any published answer.
    next: usize,
    first: Option<Error>,
}

/// The public task waker constructor preserves slot-only encoding on 32-bit targets.
/// No client affinity is stored: any current hyper-rt task may own the borrowed wait.
fn client_task(waker: &Waker) -> bool {
    hyper_rt::futures::current_task()
        .is_some_and(|task| hyper_rt::waker::waker_for(task.0).will_wake(waker))
}

async fn async_context() -> Result<(), Error> {
    std::future::poll_fn(|cx| {
        Poll::Ready(if client_task(cx.waker()) {
            Ok(())
        } else {
            Err(receive_error(SyncError::NotOnShardThread(())))
        })
    })
    .await
}

fn receive_error(error: SyncError<()>) -> Error {
    match error {
        SyncError::Closed(()) => Error::Gone {
            what: "a range's shard",
        },
        SyncError::Full(()) | SyncError::NotOnShardThread(()) => Error::InvalidArgument {
            what: "an asynchronous range client outside its hyper-rt task",
        },
    }
}

impl Client<'_> {
    /// The answer: spun for up to the spin window, then waited for parked; none when the range
    /// dropped the request unanswered.
    fn wait(&mut self) -> Option<Box<Request>> {
        let t = std::time::Instant::now();
        let window = u128::from(self.ranges.spin_ns);
        while t.elapsed().as_nanos() < window {
            match self.answers.try_recv() {
                Ok(Some(answer)) => return Some(answer),
                Ok(None) => std::hint::spin_loop(),
                Err(_) => return None,
            }
        }
        self.answers.blocking_recv().ok()
    }

    /// Publishes one request; refusal returns its buffers and admission to the client.
    fn send(&mut self, range: usize, fill: impl FnOnce(&mut Request)) -> Result<(), Error> {
        let sender = self
            .ranges
            .senders
            .get(range)
            .ok_or(Error::InvalidArgument {
                what: "a range the node does not have",
            })?;
        let mut request = self.request.take().ok_or(Error::Gone {
            what: "a range's shard",
        })?;
        fill(&mut request);
        request.result = Ok(());
        match sender.try_send(request) {
            Ok(()) => Ok(()),
            Err(SyncError::Full(request)) => {
                self.request = Some(request);
                Err(Error::LimitExceeded {
                    what: "a range's request channel",
                    limit: u64::try_from(self.ranges.clients).unwrap_or(u64::MAX),
                })
            }
            Err(SyncError::Closed(request) | SyncError::NotOnShardThread(request)) => {
                self.request = Some(request);
                Err(Error::Gone {
                    what: "a range's shard",
                })
            }
        }
    }

    fn accept(&mut self, answer: Box<Request>) -> Result<&mut Request, Error> {
        let request = self.request.insert(answer);
        request.result.clone()?;
        Ok(request)
    }

    fn prepare(&mut self) -> Result<(), Error> {
        if hyper_rt::registry::current_shard().is_some() {
            return Err(Error::InvalidArgument {
                what: "a synchronous range client call inside a runtime task",
            });
        }
        if self.barrier.is_some() {
            return Err(Error::InvalidArgument {
                what: "an asynchronous range barrier still to finish",
            });
        }
        if self.request.is_none() {
            let answer = self.wait().ok_or(Error::Gone {
                what: "a range's shard",
            })?;
            self.accept(answer)?;
        }
        Ok(())
    }

    /// Sends the request `fill` makes to `range` and waits for its answer.
    fn call(
        &mut self,
        range: usize,
        fill: impl FnOnce(&mut Request),
    ) -> Result<&mut Request, Error> {
        self.prepare()?;
        self.send(range, fill)?;
        let answer = self.wait().ok_or(Error::Gone {
            what: "a range's shard",
        })?;
        self.accept(answer)
    }

    /// Context is checked before every receive poll, including a queued answer. A refusal
    /// leaves ownership and barrier progress intact; it is distinct from a received error.
    async fn answer_async(&mut self) -> Result<Result<(), Error>, SyncError<()>> {
        let answer = {
            let mut recv = std::pin::pin!(self.answers.recv());
            std::future::poll_fn(|cx| {
                if client_task(cx.waker()) {
                    recv.as_mut().poll(cx)
                } else {
                    Poll::Ready(Err(SyncError::NotOnShardThread(())))
                }
            })
            .await?
        };
        Ok(self.accept(answer).map(drop))
    }

    /// Finishes the one published request and any canceled barrier's undispatched ranges.
    /// An unanswered request lost its buffers: it cannot be replaced or replayed.
    async fn finish_async(&mut self) -> Result<(), Error> {
        async_context().await?;
        loop {
            if self.request.is_none() {
                let answered = match self.answer_async().await {
                    Ok(result) => result,
                    Err(error @ SyncError::Closed(())) => {
                        self.barrier = None;
                        return Err(receive_error(error));
                    }
                    Err(error) => return Err(receive_error(error)),
                };
                if let Err(error) = answered {
                    if let Some(barrier) = self.barrier.as_mut()
                        && matches!(barrier.ask, BarrierAsk::Stop)
                    {
                        barrier.first.get_or_insert(error);
                        continue;
                    }
                    self.barrier = None;
                    return Err(error);
                }
            }
            let Some(barrier) = self.barrier.as_ref() else {
                return Ok(());
            };
            if barrier.next == self.ranges.len() {
                return self
                    .barrier
                    .take()
                    .and_then(|barrier| barrier.first)
                    .map_or(Ok(()), Err);
            }
            let range = barrier.next;
            let ask = barrier.ask;
            let next = range.checked_add(1).ok_or(Error::InvalidArgument {
                what: "a range barrier beyond the node's ranges",
            })?;
            // A canceled wait may be resumed by another executor; check at each publication.
            async_context().await?;
            let sent = self.send(range, |request| request.ask = ask.ask());
            if let Some(barrier) = self.barrier.as_mut() {
                barrier.next = next;
            }
            if let Err(error) = sent {
                if let Some(barrier) = self.barrier.as_mut()
                    && matches!(barrier.ask, BarrierAsk::Stop)
                {
                    barrier.first.get_or_insert(error);
                    continue;
                }
                self.barrier = None;
                return Err(error);
            }
        }
    }

    async fn call_async(
        &mut self,
        range: usize,
        fill: impl FnOnce(&mut Request),
    ) -> Result<&mut Request, Error> {
        self.finish_async().await?;
        async_context().await?;
        self.send(range, fill)?;
        self.answer_async().await.map_err(receive_error)??;
        self.request.as_deref_mut().ok_or(Error::Gone {
            what: "a range's shard",
        })
    }

    async fn barrier_async(&mut self, ask: BarrierAsk) -> Result<(), Error> {
        self.finish_async().await?;
        self.barrier = Some(Barrier {
            ask,
            next: 0,
            first: None,
        });
        self.finish_async().await
    }

    /// Records `key` holding `value`.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let range = self.ranges.range_of(key);
        self.call(range, |r| {
            r.ask = Ask::Put;
            r.key.clear();
            r.key.extend_from_slice(key);
            r.value.clear();
            r.value.extend_from_slice(value);
        })
        .map(drop)
    }

    /// Records `key` deleted.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        let range = self.ranges.range_of(key);
        self.call(range, |r| {
            r.ask = Ask::Delete;
            r.key.clear();
            r.key.extend_from_slice(key);
        })
        .map(drop)
    }

    /// The value `key` holds, into `value` (its buffer swapped with the request's, never
    /// copied); false when it holds none.
    pub fn get(&mut self, key: &[u8], value: &mut Vec<u8>) -> Result<bool, Error> {
        let range = self.ranges.range_of(key);
        let r = self.call(range, |r| {
            r.ask = Ask::Get;
            r.key.clear();
            r.key.extend_from_slice(key);
        })?;
        std::mem::swap(value, &mut r.value);
        Ok(r.found)
    }

    /// Records a value from a hyper-rt task, yielding for the same bounded reply channel.
    /// Canceling this borrowed wait leaves its request outstanding. The next async operation
    /// recovers its answer and reports its error before publishing another request.
    /// The task of `range` when this client may make an operation on its engine inline: the client
    /// runs on the range's shard, with no request or barrier of its own out.
    fn inline_task(&self, range: usize) -> Option<TaskId> {
        let task = *self.ranges.tasks.get(range)?;
        if self.ranges.inline
            && self.request.is_some()
            && self.barrier.is_none()
            && hyper_rt::registry::current_shard() == Some(task.shard().0)
        {
            Some(task)
        } else {
            None
        }
    }

    /// Takes this client's turn before an operation made inline. An inline operation never waits,
    /// so a client making them in a loop would hold its shard: after about the shard's quantum of
    /// them, at the rate measured over the turn just ended (two reads of the shard's clock a turn),
    /// it yields, and the shard's other tasks run, a range it left maintenance owed among them.
    async fn turn(&mut self) {
        if let Some(left) = self.turn.left.checked_sub(1) {
            self.turn.left = left;
            self.turn.done = self.turn.done.saturating_add(1);
            return;
        }
        let (now, quantum) =
            hyper_rt::registry::with_current(|ctx| (ctx.now_ns(), ctx.quantum_ns()))
                .unwrap_or((0, 0));
        let elapsed = now.saturating_sub(self.turn.began_ns);
        let per = u128::from(quantum)
            .saturating_mul(u128::from(self.turn.done))
            .checked_div(u128::from(elapsed))
            .unwrap_or(1);
        let per = u64::try_from(per).unwrap_or(u64::MAX).max(1);
        hyper_rt::futures::yield_now().await;
        self.turn.left = per.saturating_sub(1);
        self.turn.done = 1;
        self.turn.began_ns = hyper_rt::registry::with_current(|ctx| ctx.now_ns()).unwrap_or(0);
    }

    /// Makes `op` on `range`'s engine inline when the range lends it now: its answer, or none for
    /// the request path.
    async fn inline<R>(
        &mut self,
        range: usize,
        op: impl FnOnce(&mut dyn Inline, u64) -> R,
    ) -> Option<R> {
        let task = self.inline_task(range)?;
        self.turn().await;
        with_lent(task, op)
    }

    /// Operations this client completed inline on a range's lent engine, with no request.
    pub fn inlined(&self) -> u64 {
        self.inlined
    }

    fn completed_inline(&mut self) {
        self.inlined = self.inlined.saturating_add(1);
    }

    pub async fn put_async(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let range = self.ranges.range_of(key);
        if let Some(applied) = self
            .inline(range, |db, keys| db.put(key, value, keys))
            .await
            && applied?
        {
            self.completed_inline();
            return Ok(());
        }
        self.call_async(range, |r| {
            r.ask = Ask::Put;
            r.key.clear();
            r.key.extend_from_slice(key);
            r.value.clear();
            r.value.extend_from_slice(value);
        })
        .await
        .map(drop)
    }

    /// Records a deletion from a hyper-rt task; cancellation never republishes it.
    pub async fn delete_async(&mut self, key: &[u8]) -> Result<(), Error> {
        let range = self.ranges.range_of(key);
        if let Some(applied) = self.inline(range, |db, keys| db.delete(key, keys)).await
            && applied?
        {
            self.completed_inline();
            return Ok(());
        }
        self.call_async(range, |r| {
            r.ask = Ask::Delete;
            r.key.clear();
            r.key.extend_from_slice(key);
        })
        .await
        .map(drop)
    }

    /// A hyper-rt task's get, with the same swapped value buffer as `get`.
    pub async fn get_async(&mut self, key: &[u8], value: &mut Vec<u8>) -> Result<bool, Error> {
        let range = self.ranges.range_of(key);
        if let Some(found) = self.inline(range, |db, _| db.get(key, value)).await
            && let Some(found) = found?
        {
            self.completed_inline();
            return Ok(found);
        }
        let r = self
            .call_async(range, |r| {
                r.ask = Ask::Get;
                r.key.clear();
                r.key.extend_from_slice(key);
            })
            .await?;
        std::mem::swap(value, &mut r.value);
        Ok(r.found)
    }

    /// A page of `[from, end)` as `ShardDb::scan` gives one, across ranges: up to `limit` rows
    /// appended to `out` in key order; true when the page filled first, with the key to continue
    /// from in `next`. Nothing is allocated once the client's buffers, `out` and `next` have
    /// grown.
    pub fn scan(
        &mut self,
        from: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        out: &mut Rows,
        next: &mut Vec<u8>,
    ) -> Result<bool, Error> {
        self.prepare()?;
        let ranges = self.ranges;
        let mut range = ranges.range_of(from);
        let mut left = limit;
        if let Some(r) = self.request.as_mut() {
            r.key.clear();
            r.key.extend_from_slice(from);
        }
        loop {
            // The range's own end, or the scan's when it comes first; the last range asked is
            // the one whose end the scan's reaches.
            let bound = ranges.starts.get(range.saturating_add(1));
            let (stop, last): (Option<&[u8]>, bool) = match (bound, end) {
                (None, e) => (e, true),
                (Some(b), Some(e)) if e <= b.as_slice() => (Some(e), true),
                (Some(b), _) => (Some(b.as_slice()), false),
            };
            let r = self.call(range, |r| {
                r.end.clear();
                if let Some(stop) = stop {
                    r.end.extend_from_slice(stop);
                }
                r.ask = Ask::Scan {
                    bounded: stop.is_some(),
                    limit: left,
                };
            })?;
            left = left.saturating_sub(r.rows.len());
            out.append(&mut r.rows);
            if r.more {
                next.clear();
                next.extend_from_slice(&r.next);
                return Ok(true);
            }
            if last {
                return Ok(false);
            }
            range = range.saturating_add(1);
            let start = ranges.starts.get(range).map_or(&[][..], Vec::as_slice);
            if left == 0 {
                next.clear();
                next.extend_from_slice(start);
                return Ok(true);
            }
            r.key.clear();
            r.key.extend_from_slice(start);
        }
    }

    /// The same cross-range page as `scan`, yielding on its bounded replies. Canceling may
    /// leave a prefix appended to `out`; discard that incomplete page before another scan.
    /// The next operation recovers the outstanding reply without replaying the abandoned scan.
    pub async fn scan_async(
        &mut self,
        from: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        out: &mut Rows,
        next: &mut Vec<u8>,
    ) -> Result<bool, Error> {
        self.finish_async().await?;
        let ranges = self.ranges;
        let mut range = ranges.range_of(from);
        let mut left = limit;
        if let Some(r) = self.request.as_mut() {
            r.key.clear();
            r.key.extend_from_slice(from);
        }
        loop {
            // The range's own end, or the scan's when it comes first; the last range asked is
            // the one whose end the scan's reaches.
            let bound = ranges.starts.get(range.saturating_add(1));
            let (stop, last): (Option<&[u8]>, bool) = match (bound, end) {
                (None, e) => (e, true),
                (Some(b), Some(e)) if e <= b.as_slice() => (Some(e), true),
                (Some(b), _) => (Some(b.as_slice()), false),
            };
            let r = self
                .call_async(range, |r| {
                    r.end.clear();
                    if let Some(stop) = stop {
                        r.end.extend_from_slice(stop);
                    }
                    r.ask = Ask::Scan {
                        bounded: stop.is_some(),
                        limit: left,
                    };
                })
                .await?;
            left = left.saturating_sub(r.rows.len());
            out.append(&mut r.rows);
            if r.more {
                next.clear();
                next.extend_from_slice(&r.next);
                return Ok(true);
            }
            if last {
                return Ok(false);
            }
            range = range.saturating_add(1);
            let start = ranges.starts.get(range).map_or(&[][..], Vec::as_slice);
            if left == 0 {
                next.clear();
                next.extend_from_slice(start);
                return Ok(true);
            }
            r.key.clear();
            r.key.extend_from_slice(start);
        }
    }

    /// Makes every range's state through `applied` durable (`ShardDb::checkpoint`).
    pub fn checkpoint(&mut self, applied: u64) -> Result<(), Error> {
        for range in 0..self.ranges.len() {
            self.call(range, |r| r.ask = Ask::Checkpoint(applied))?;
        }
        Ok(())
    }

    /// Packs every range's memtables and runs its maintenance to the end.
    pub fn flush(&mut self) -> Result<(), Error> {
        for range in 0..self.ranges.len() {
            self.call(range, |r| r.ask = Ask::Flush)?;
        }
        Ok(())
    }

    /// Waits until every range owes no maintenance, range by range: a range that faulted
    /// answers with its fault. Nothing is sent to a range while it is awaited, so its
    /// maintenance runs uninterrupted.
    pub fn settled(&mut self) -> Result<(), Error> {
        for range in 0..self.ranges.len() {
            self.call(range, |r| r.ask = Ask::Settled)?;
        }
        Ok(())
    }

    /// Each range's counters, in range order.
    pub fn stats(&mut self) -> Result<Vec<RangeStats>, Error> {
        let mut all = Vec::with_capacity(self.ranges.len());
        for range in 0..self.ranges.len() {
            let r = self.call(range, |r| r.ask = Ask::Stats)?;
            if let Some(s) = r.stats.take() {
                all.push(s);
            }
        }
        Ok(all)
    }

    /// Makes every range durable from a hyper-rt task. A canceled barrier finishes its
    /// undispatched ranges before a later async operation; published requests are not replayed.
    pub async fn checkpoint_async(&mut self, applied: u64) -> Result<(), Error> {
        self.barrier_async(BarrierAsk::Checkpoint(applied)).await
    }

    /// Completes every range's full flush, with the same cancellation rule as checkpoint.
    pub async fn flush_async(&mut self) -> Result<(), Error> {
        self.barrier_async(BarrierAsk::Flush).await
    }

    /// Each range's counters from a hyper-rt task, in range order.
    pub async fn stats_async(&mut self) -> Result<Vec<RangeStats>, Error> {
        self.finish_async().await?;
        let mut all = Vec::with_capacity(self.ranges.len());
        for range in 0..self.ranges.len() {
            let r = self.call_async(range, |r| r.ask = Ask::Stats).await?;
            if let Some(stats) = r.stats.take() {
                all.push(stats);
            }
        }
        Ok(all)
    }
}
