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

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Poll, Waker};

use hyper_block::block::BlockFile;
use hyper_rt::Runtime;
use hyper_rt::combine::{Either, race2};
use hyper_rt::sync::{ChannelReceiver, Sender, SyncError, channel, channel_with};

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
    reply: Option<Sender<Request>>,
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

/// A range's task: serves requests in batches, maintenance in between, until stopped or every
/// sender is gone.
async fn serve<F: BlockFile>(
    mut db: ShardDb<F>,
    mut requests: ChannelReceiver<Request>,
    batch: usize,
    slice_ns: u64,
) {
    let mut range = Range {
        fault: None,
        slice: Slice::new(slice_ns),
    };
    loop {
        // Every request waiting, at most a batch: one wake serves them all.
        let mut served = 0usize;
        while served < batch {
            match requests.try_recv() {
                Ok(Some(request)) => {
                    if range.answer(&mut db, request) {
                        return finish(db, &mut requests);
                    }
                    served = served.saturating_add(1);
                }
                Ok(None) => break,
                Err(_) => return finish(db, &mut requests),
            }
        }
        if served == batch {
            // More may wait: the shard's other tasks first, then the next batch.
            hyper_rt::futures::yield_now().await;
            continue;
        }
        if range.fault.is_none() && db.idle_owed() {
            range.maintain(&mut db);
            if range.fault.is_none() && db.waiting_for_io() {
                // The same task owns both receivers. A request can interrupt this borrowed
                // completion wait without consuming its answer or returning its buffer.
                let ready = race2(requests.recv(), db.wait_completion()).await;
                match ready {
                    Either::First(Ok(request)) => {
                        if range.answer(&mut db, request) {
                            return finish(db, &mut requests);
                        }
                    }
                    Either::First(Err(_)) => return finish(db, &mut requests),
                    Either::Second(Ok(_)) => {}
                    Either::Second(Err(error)) => range.fault = Some(error),
                }
            } else {
                hyper_rt::futures::yield_now().await;
            }
            continue;
        }
        match requests.recv().await {
            Ok(request) => {
                if range.answer(&mut db, request) {
                    return finish(db, &mut requests);
                }
            }
            Err(_) => return finish(db, &mut requests),
        }
    }
}

/// The task's end: the engine's writes landed and the engine dropped. Requests still queued
/// are dropped unanswered, which closes their clients' channels: they are told the range is
/// gone.
fn finish<F: BlockFile>(db: ShardDb<F>, requests: &mut ChannelReceiver<Request>) {
    let (_, landed) = db.into_file();
    drop(landed);
    while let Ok(Some(request)) = requests.try_recv() {
        drop(request);
    }
}

/// A range task's state beside its engine.
struct Range {
    /// The first maintenance failure: every request after it is answered with it, and
    /// maintenance stops, so a failed range never acknowledges a write it may not keep.
    fault: Option<Error>,
    slice: Slice,
}

impl Range {
    /// Applies `request` and sends its answer; true when it was the range's stop. The shard is
    /// told a client was served, so it spins out its wake window before it parks and the
    /// client's next request costs no kernel wake (hyper-rt `ShardContext::note_activity`).
    fn answer<F: BlockFile>(&mut self, db: &mut ShardDb<F>, mut request: Request) -> bool {
        hyper_rt::registry::with_current(|ctx| ctx.note_activity());
        let stop = matches!(request.ask, Ask::Stop);
        request.result = match &self.fault {
            Some(fault) => Err(fault.clone()),
            None => apply(db, &mut request),
        };
        if let Some(reply) = request.reply.take() {
            request.reply = Some(reply.clone());
            // A client gone has nobody to tell.
            drop(reply.try_send(request));
        }
        stop
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
fn apply<F: BlockFile>(db: &mut ShardDb<F>, request: &mut Request) -> Result<(), Error> {
    match &request.ask {
        Ask::Put => db.put(&request.key, &request.value),
        Ask::Delete => db.delete(&request.key),
        Ask::Get => {
            request.found = db.get(&request.key, &mut request.value)?;
            Ok(())
        }
        Ask::Scan { bounded, limit } => {
            request.rows.clear();
            let end = if *bounded {
                Some(request.end.as_slice())
            } else {
                None
            };
            request.more = db.scan(
                &request.key,
                end,
                *limit,
                &mut request.rows,
                &mut request.next,
            )?;
            Ok(())
        }
        Ask::Checkpoint(applied) => db.checkpoint(*applied),
        Ask::Flush => db.flush(),
        Ask::Stats => {
            request.stats = Some(db.stats());
            Ok(())
        }
        Ask::Stop => db.land(),
    }
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
}

/// A node's ranges, each on a shard of a runtime, and the clients that reach them.
#[derive(Debug)]
pub struct Ranges {
    /// Each range's lowest key, ascending; the first is empty, so every key has a range.
    starts: Vec<Vec<u8>>,
    senders: Vec<Sender<Request>>,
    /// Clients live now, at most `clients`.
    live: Arc<AtomicUsize>,
    clients: usize,
    spin_ns: u64,
}

impl Ranges {
    /// Starts each range's engine on a shard of `runtime`, the ranges dealt to its shards in
    /// order. `ranges` gives each range's lowest key, ascending, the first empty.
    pub fn start<F: BlockFile + Send + 'static>(
        runtime: &Runtime,
        ranges: Vec<(Vec<u8>, ShardDb<F>)>,
        config: RangesConfig,
    ) -> Result<Self, Error> {
        let RangesConfig {
            clients,
            slice_ns,
            spin_ns,
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
        let shards = runtime.shard_ids();
        let mut starts = Vec::with_capacity(ranges.len());
        let mut senders = Vec::with_capacity(ranges.len());
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
            runtime
                .spawn_on(shard, serve(db, receiver, clients, slice_ns))
                .map_err(|_| Error::Gone {
                    what: "the shard a range was placed on",
                })?;
            starts.push(start);
            senders.push(sender);
        }
        Ok(Self {
            starts,
            senders,
            live: Arc::new(AtomicUsize::new(0)),
            clients,
            spin_ns,
        })
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
            request: Some(Request {
                reply: Some(reply),
                _lease: Some(Arc::clone(&lease)),
                ..Request::new()
            }),
            answers,
            barrier: None,
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
    request: Option<Request>,
    answers: ChannelReceiver<Request>,
    barrier: Option<Barrier>,
    /// The client and its orphaned request share admission until both have retired.
    _lease: Arc<ClientLease>,
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
    fn wait(&mut self) -> Option<Request> {
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

    fn accept(&mut self, answer: Request) -> Result<&mut Request, Error> {
        let request = self.request.insert(answer);
        request.result.clone()?;
        Ok(request)
    }

    fn prepare(&mut self) -> Result<(), Error> {
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
        self.request.as_mut().ok_or(Error::Gone {
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
    pub async fn put_async(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let range = self.ranges.range_of(key);
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
