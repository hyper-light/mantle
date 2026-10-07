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

use std::sync::atomic::{AtomicUsize, Ordering};

use hyper_block::block::BlockFile;
use hyper_rt::Runtime;
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
        if range.fault.is_none() && db.owed() {
            range.maintain(&mut db);
            hyper_rt::futures::yield_now().await;
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

    /// One slice of maintenance; a failure is kept as the range's fault.
    fn maintain<F: BlockFile>(&mut self, db: &mut ShardDb<F>) {
        let t = std::time::Instant::now();
        let keys = self.slice.keys();
        match db.idle_step(keys) {
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
    live: AtomicUsize,
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
            live: AtomicUsize::new(0),
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
        Ok(Client {
            ranges: self,
            request: Some(Request {
                reply: Some(reply),
                ..Request::new()
            }),
            answers,
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
}

/// A caller's handle on the ranges: one request out at a time, its buffers reused.
#[derive(Debug)]
pub struct Client<'a> {
    ranges: &'a Ranges,
    /// The request and its buffers while none is out; none after a range was lost with it.
    request: Option<Request>,
    answers: ChannelReceiver<Request>,
}

impl Drop for Client<'_> {
    fn drop(&mut self) {
        self.ranges.live.fetch_sub(1, Ordering::AcqRel);
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

    /// Sends the request `fill` makes to `range` and waits for its answer.
    fn call(
        &mut self,
        range: usize,
        fill: impl FnOnce(&mut Request),
    ) -> Result<&mut Request, Error> {
        let gone = Error::Gone {
            what: "a range's shard",
        };
        let mut request = self.request.take().ok_or(gone.clone())?;
        let sender = self
            .ranges
            .senders
            .get(range)
            .ok_or(Error::InvalidArgument {
                what: "a range the node does not have",
            })?;
        fill(&mut request);
        request.result = Ok(());
        match sender.try_send(request) {
            Ok(()) => {}
            Err(SyncError::Full(request)) => {
                self.request = Some(request);
                return Err(Error::LimitExceeded {
                    what: "a range's request channel",
                    limit: u64::try_from(self.ranges.clients).unwrap_or(u64::MAX),
                });
            }
            Err(SyncError::Closed(request) | SyncError::NotOnShardThread(request)) => {
                self.request = Some(request);
                return Err(gone);
            }
        }
        let answer = self.wait().ok_or(gone)?;
        let request = self.request.insert(answer);
        request.result.clone()?;
        Ok(request)
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
}
