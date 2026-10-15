//! Who gets a seat when a growth due ahead of need and a packing held for a seat both want one
//! ([`ShardDb::grow`]), on an engine prepared as a range's is, whose owner steps a test's task
//! takes one at a time, on a store grown by segments, its pool's two seats on a test's relays
//! ([`relay`]). Each step is answered with what the owner left: whether a packing is held for a
//! seat, a growth is out or due, what waits for room, and the seats free.
//!
//! The independent holder is the test's own job, an index of nothing sent to the pool and held
//! on its seat: no owner of the engine waits on it. Facts decide every verdict: the relays name
//! each job and result as it passes, and each step's answer what the owner left. Each wait for a
//! fact has a failure guard ([`WAIT`]) a passing run never reaches.
#![cfg(test)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::disallowed_types,
    clippy::cast_possible_truncation
)]
use super::*;
use crate::ranges::Ranges;
use crate::trunk::pool::relay::{self, Event, Kind, OpenOnDrop, Relays, Seen, WAIT};
use hyper_block::buf::Alignment;
use hyper_block::file::CachingRequest;
use hyper_block::issuer::Issuer;
use hyper_block::segmented::SegmentedFile;
use hyper_rt::sync::{ChannelReceiver, Sender};
use hyper_rt::{Runtime, RuntimeConfig};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 16,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 8,
    leaf_entries: 96,
};
/// A memtable of four pages: packings come every few dozen operations.
const MEM: usize = 4 * 4096;
const OPS: u64 = 2048;
/// Operations before the case begins, each until done: the shard has measured a fill and a
/// packing by then, so a memtable may stay frozen while the next fills.
const WARM: u64 = 256;
/// Segments a file may have: past what the workload makes.
const BOUND: usize = 1 << 12;
const SLICE_NS: u64 = 50_000;
/// The work units an owner's idle slice is given.
const KEYS: u64 = 64;
/// Owner turns a step may take before it is refused: past any a passing run takes.
const TURNS: usize = 1 << 16;

fn align() -> Alignment {
    Alignment::new(STORE.page_size).unwrap()
}

fn key(i: u64) -> Vec<u8> {
    format!("bucket-{}/object-{:05}", i % 5, i % 1500).into_bytes()
}

/// The `i`th operation: a put of a value of one of several lengths, or every seventh a delete.
fn operation(i: u64) -> (Vec<u8>, Option<Vec<u8>>) {
    let value = if i.is_multiple_of(7) {
        None
    } else {
        let len = [1, 33, 100, 700][i as usize % 4];
        Some(vec![(i % 251) as u8; len])
    };
    (key(i), value)
}

fn open(path: &Path) -> Result<SegmentedFile, Error> {
    SegmentedFile::open(path, CachingRequest::Buffered, align(), BOUND).map_err(|e| Error::Io {
        op: "open a worker's file",
        detail: e.to_string(),
    })
}

fn runtime() -> Runtime {
    Runtime::start(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: 4,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: SLICE_NS,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: STORE.page_size,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    })
    .unwrap()
}

/// A step of the owner's, each answered with what it left.
#[derive(Debug)]
enum Step {
    /// The writer's operation `i`, tried once: done, or waiting.
    Once(u64),
    /// The writer's operation `i` until done, the owner taking a range's turns between tries.
    Until(u64),
    /// One idle slice of maintenance, as a range's idle turn takes.
    Idle,
    /// The test's own job: an index of nothing, sent to the pool for a seat.
    Inject,
    /// The test's own job's result, taken back once it came.
    TakeInjected,
    /// Extents granted to no job, so the room admitted is what the held packing's hand-off
    /// asks: it fits, and a growth is due. Given back by [`Step::Unreserve`].
    Reserve,
    Unreserve,
    /// Flush, checkpoint at the operations applied, land and finish the engine.
    Finish(u64),
}

/// What the owner left after a step.
#[derive(Clone, Copy, Debug)]
struct Witness {
    done: bool,
    /// A packing held for a seat: the writer's next rotation waits on it.
    packing_held: bool,
    growth_out: bool,
    growth_due: bool,
    /// What waits for room: the packing's ask and the workers' asks.
    packing_wanted: u64,
    needs: u64,
    /// The room admitted, and what the held packing's hand-off asks of it.
    capacity: u64,
    asks: u64,
    frozen_most: usize,
}

/// The extents a hand-off of the held packing asks, as `ShardDb::sort_off` counts them: its
/// memtable's runs, one more, and one for each of the root's pivot keys it is cut at.
fn hand_off_asks(db: &mut ShardDb<SegmentedFile>) -> u64 {
    let Some(bytes) = db.packing.as_ref().map(|p| p.mem.bytes()) else {
        return 0;
    };
    let mut cuts = Vec::new();
    db.trunk.root_cuts(&mut cuts);
    let run = db.store.run_bytes().max(1);
    let extents = crate::util::div_ceil_usize(bytes, run)
        .unwrap()
        .saturating_add(1)
        .saturating_add(cuts.len());
    u64::try_from(extents).unwrap()
}

fn witness(db: &mut ShardDb<SegmentedFile>, done: bool) -> Witness {
    let needs = db.trunk.pool_mut().map_or(0, |p| p.needs_wanted());
    Witness {
        done,
        packing_held: db.packing.as_ref().is_some_and(|p| p.sort.is_some()),
        growth_out: db.growth.is_some(),
        growth_due: db.growth_due(),
        packing_wanted: db.packing_wanted,
        needs,
        capacity: db.store.capacity(),
        asks: hand_off_asks(db),
        frozen_most: db.frozen_most(),
    }
}

/// The writer's operation `i`, tried once.
fn once(db: &mut ShardDb<SegmentedFile>, i: u64) -> Result<bool, Error> {
    let (key, value) = operation(i);
    match &value {
        Some(v) => db.put_paced(&key, v, KEYS),
        None => db.delete_paced(&key, KEYS),
    }
}

/// One of a range's turns between tries: what the workers sent back taken, then a wait for a
/// worker's message or the device where that is what is owed, else an idle slice.
async fn turn(db: &mut ShardDb<SegmentedFile>) -> Result<(), Error> {
    db.take_results()?;
    if db.waiting_for_io() {
        db.wait_completion().await?;
    } else {
        db.idle_paced_step(KEYS)?;
        hyper_rt::futures::yield_now().await;
    }
    Ok(())
}

/// Tries `step` until it is done, a range's turn between tries, within [`TURNS`].
async fn until(
    db: &mut ShardDb<SegmentedFile>,
    mut step: impl FnMut(&mut ShardDb<SegmentedFile>) -> Result<bool, Error>,
) -> Result<bool, Error> {
    for _ in 0..TURNS {
        if step(db)? {
            return Ok(true);
        }
        turn(db).await?;
    }
    Err(Error::LimitExceeded {
        what: "a fixture step's turns",
        limit: u64::try_from(TURNS).unwrap_or(u64::MAX),
    })
}

async fn run(
    db: &mut ShardDb<SegmentedFile>,
    reserved: &mut Vec<u64>,
    step: Step,
) -> Result<bool, Error> {
    match step {
        Step::Reserve => {
            let spare = db.store.capacity().saturating_sub(hand_off_asks(db));
            reserved.extend(db.store.grant(usize::try_from(spare).unwrap())?);
            Ok(true)
        }
        Step::Unreserve => {
            db.store.grant_back(reserved)?;
            reserved.clear();
            Ok(true)
        }
        Step::Once(i) => once(db, i),
        Step::Until(i) => until(db, |db| once(db, i)).await,
        Step::Idle => db.idle_paced_step(KEYS).map(|_| true),
        Step::Inject => {
            let job = Box::new(Job {
                work: Work::Index(Box::default()),
                grant: Vec::new(),
                file_end: db.store.end(),
                generation: db.store.generation(),
                keep: None,
            });
            let pool = db.trunk.pool_mut().ok_or(Error::InvalidArgument {
                what: "a fixture with no pool",
            })?;
            match pool.send(job, Owner::Index, true) {
                Ok(sent) => Ok(sent.is_ok()),
                Err((error, _)) => Err(error),
            }
        }
        Step::TakeInjected => {
            for _ in 0..TURNS {
                let ShardDb { trunk, store, .. } = &mut *db;
                let pool = trunk.pool_mut().ok_or(Error::InvalidArgument {
                    what: "a fixture with no pool",
                })?;
                if let Some(back) = pool.take(store, Owner::Index, false)? {
                    back.result?;
                    return Ok(true);
                }
                match pool.receive().await? {
                    Some(message) => {
                        pool.accept(store, message)?;
                    }
                    None => {
                        return Err(Error::InvalidArgument {
                            what: "the workers' channel closed with the test's job out",
                        });
                    }
                }
            }
            Ok(false)
        }
        Step::Finish(applied) => {
            until(db, |db| db.flush_paced(KEYS)).await?;
            until(db, |db| db.checkpoint_paced(applied, KEYS)).await?;
            db.land()?;
            Ok(true)
        }
    }
}

/// The owner's task: each step run and answered, until the finish, which is answered once the
/// engine's workers and file have retired, or until a step fails.
async fn owner(
    mut db: ShardDb<SegmentedFile>,
    mut steps: ChannelReceiver<Step>,
    answers: mpsc::SyncSender<Result<Witness, Error>>,
) {
    let mut reserved = Vec::new();
    while let Ok(step) = steps.recv().await {
        let finish = matches!(step, Step::Finish(_));
        let answer = run(&mut db, &mut reserved, step)
            .await
            .map(|done| witness(&mut db, done));
        let failed = answer.is_err();
        if finish && !failed {
            let finished = db.finish_async().await;
            let retired = db.actor_finished();
            drop(db);
            let answer = match finished {
                Err(error) => Err(error),
                Ok(()) if retired => answer,
                Ok(()) => Err(Error::InvalidArgument {
                    what: "a finished fixture whose workers and file did not retire",
                }),
            };
            drop(answers.send(answer));
            return;
        }
        if answers.send(answer).is_err() || failed {
            break;
        }
    }
    drop(db.finish_async().await);
}

/// An engine on a new store grown by segments at `path`, its pool's two seats on `relays`, its
/// owner's steps taken by [`owner`] on the runtime.
struct Fixture {
    steps: Sender<Step>,
    answers: mpsc::Receiver<Result<Witness, Error>>,
    oracle: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    path: PathBuf,
    // Declared last, dropped last: the runtime outlives the steps' sender and the answers.
    _issuer: Issuer,
    runtime: Option<Runtime>,
}

impl Fixture {
    fn start(dir: &Path, relays: &'static Relays) -> Self {
        let path = dir.join("store");
        let issuer = Issuer::start_for(dir, 2, 2).unwrap();
        let mut runtime = runtime();
        let created =
            SegmentedFile::create(&path, CachingRequest::Buffered, align(), BOUND).unwrap();
        let mut db = ShardDb::create(created, STORE, MEM, TRUNK).unwrap();
        // The owner's write roles and two workers' (ranges_growth_asker.rs's recipe gives one).
        let run = STORE.page_size * usize::try_from(STORE.extent_pages).unwrap();
        db.set_memory(3 * (issuer.depth() + 2) * run).unwrap();
        db.attach(&issuer, issuer.depth()).unwrap();
        let worker = path.clone();
        db.set_workers(move || open(&worker)).unwrap();
        // The same pool with its two seats on the relays: no worker has started yet.
        let relayed = path.clone();
        let mut pool = Pool::new(relay::spawn(move || open(&relayed), STORE, relays), 2).unwrap();
        if let Some((attacher, batches)) = &db.worker_attach {
            pool.set_attach(attacher.clone(), *batches);
        }
        db.trunk.set_pool(pool);
        let (steps, received) = hyper_rt::sync::channel::<Step>(1).unwrap();
        let (answer, answers) = mpsc::sync_channel(1);
        Ranges::start_fixture(&mut runtime, db, move |db| owner(db, received, answer)).unwrap();
        Self {
            steps,
            answers,
            oracle: BTreeMap::new(),
            path,
            _issuer: issuer,
            runtime: Some(runtime),
        }
    }

    /// Runs `step` on the owner and returns what it left; a failed step fails the test.
    fn step(&mut self, step: Step) -> Witness {
        let what = format!("{step:?}");
        let op = match step {
            Step::Once(i) | Step::Until(i) => Some(i),
            _ => None,
        };
        self.steps.blocking_send(step).unwrap();
        let w = self
            .answers
            .recv_timeout(WAIT)
            .unwrap_or_else(|e| panic!("the owner's answer to {what}: not received ({e})"))
            .unwrap_or_else(|e| panic!("the owner's {what} failed: {e:?}"));
        if let Some(i) = op
            && w.done
        {
            let (key, value) = operation(i);
            self.oracle.insert(key, value);
        }
        w
    }

    /// The writer's operations `from..OPS`, flushed, checkpointed and finished; the runtime
    /// stopped; then the store reopened cold, every value and tombstone read back exactly, and
    /// its references whole.
    fn finish(mut self, from: u64) {
        for i in from..OPS {
            assert!(self.step(Step::Until(i)).done);
        }
        assert!(self.step(Step::Finish(OPS)).done);
        drop(self.runtime.take());
        let (mut db, applied) =
            ShardDb::open(open(&self.path).unwrap(), STORE, MEM, TRUNK).unwrap();
        assert_eq!(applied, OPS);
        let mut rows = Rows::new();
        let mut next = Vec::new();
        assert!(
            !db.scan(b"", None, usize::MAX, &mut rows, &mut next)
                .unwrap()
        );
        let got: Vec<(Vec<u8>, Vec<u8>)> =
            rows.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
        let want: Vec<(Vec<u8>, Vec<u8>)> = self
            .oracle
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k.clone(), v.clone())))
            .collect();
        assert_eq!(got, want);
        db.check_references().unwrap();
    }
}

/// The jobs handed `seat` after the `from`th event, in order.
fn jobs_after(seen: &Seen, from: usize, seat: usize) -> Vec<Kind> {
    seen.events
        .iter()
        .skip(from)
        .filter_map(|e| match *e {
            Event::Job { seat: s, kind } if s == seat => Some(kind),
            _ => None,
        })
        .collect()
}

/// Do: two seats, one held by the test's own job; the other's next result held; the writer's
/// operations tried once each until a packing is held for a seat, with a growth due ahead of
/// need and nothing waiting for room. Let the held result reach the shard, then take one idle
/// slice, with growths held as a seat takes them.
/// Expect: the freed seat's next job is the packing, and no growth is out; the writer's waiting
/// operation then completes, the test's job still held; let everything go, and every value and
/// tombstone reads back exactly after a checkpoint and a cold reopen.
#[test]
fn the_last_free_seat_goes_to_the_packing_held_for_one_not_a_growth_due_ahead_of_need() {
    let dir = tempfile::tempdir().unwrap();
    let relays = Relays::leak();
    let mut fx = Fixture::start(dir.path(), relays);
    // Declared after the fixture, it drops first: a failed assertion lets everything held go
    // before the runtime's teardown joins the workers.
    let _open = OpenOnDrop(relays);
    let mut i = 0;
    while i < WARM {
        assert!(fx.step(Step::Until(i)).done);
        i += 1;
    }
    // No growth out: idle slices until any growth out has landed.
    let mut w = fx.step(Step::Idle);
    for _ in 0..TURNS {
        if !w.growth_out {
            break;
        }
        w = fx.step(Step::Idle);
    }
    assert!(!w.growth_out, "a growth out did not land: {w:?}");
    assert!(
        w.frozen_most >= 1,
        "no frozen memtable allowed after warming: {w:?}"
    );
    // The independent holder: the test's own job, held on its seat.
    relays.hold_job(Kind::Index);
    assert!(fx.step(Step::Inject).done, "the test's job got no seat");
    let holder = relays
        .until("the test's job held on its seat", |s| {
            s.held_jobs.len() == 1
        })
        .held_jobs[0];
    let free = 1 - holder;
    // The free seat's next result is held: from then both seats are held.
    relays.hold_done(&[Kind::Sort, Kind::Compact, Kind::Grow]);
    let mut set = None;
    for _ in 0..OPS {
        let w = fx.step(Step::Once(i));
        if w.done {
            i += 1;
        }
        if w.packing_held && relays.seen().held_dones == [free] {
            set = Some(w);
            break;
        }
        assert!(
            w.done,
            "the writer waits before a packing was held for a seat with both seats held: {w:?}; \
             {:?}",
            relays.seen()
        );
    }
    set.expect("no packing held for a seat within the operations");
    // The room admitted is what the held packing's hand-off asks: it fits, a growth is due.
    let w = fx.step(Step::Reserve);
    assert!(
        w.packing_held
            && w.capacity == w.asks
            && w.growth_due
            && !w.growth_out
            && w.packing_wanted == 0
            && w.needs == 0,
        "the setup: a packing held for a seat that fits the room admitted, a growth due ahead \
         of need, nothing waiting for room: {w:?}"
    );
    // Growths are held as a seat takes them; the held result reaches the shard; one idle slice.
    relays.hold_job(Kind::Grow);
    let from = relays.seen().events.len();
    relays.release_done(free);
    relays.until("the held result delivered", |s| {
        s.events
            .iter()
            .skip(from)
            .any(|e| *e == Event::Delivered { seat: free })
    });
    let after = fx.step(Step::Idle);
    let seen = relays.until("the freed seat's next job", |s| {
        !jobs_after(s, from, free).is_empty()
    });
    assert_eq!(
        jobs_after(&seen, from, free)[0],
        Kind::Sort,
        "the freed seat went to a growth before the packing the writer waits on; {after:?}"
    );
    assert!(
        !after.growth_out && !after.packing_held,
        "after the slice: {after:?}"
    );
    // The writer's waiting operation completes, the test's job still held.
    assert!(fx.step(Step::Until(i)).done);
    i += 1;
    assert!(relays.seen().held_jobs.contains(&holder));
    // Everything goes: the room reserved, the test's job, then every hold; the writer finishes.
    assert!(fx.step(Step::Unreserve).done);
    relays.release_job(holder);
    assert!(fx.step(Step::TakeInjected).done);
    relays.open();
    fx.finish(i);
}
