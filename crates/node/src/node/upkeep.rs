//! The ranges' engines' maintenance in the time a shard's turn leaves (node.md §2.9, §6.1): after
//! a turn's own work, its ranges' engines that owe maintenance pay some of it, at most a turn's
//! budget in all (`Engine::upkeep`). A range's engine otherwise paid its packings, compactions and
//! consolidations only as its puts or its checkpoints did, so a range taking only gets after a
//! burst of puts read the busy trunk until its next write: about 18 sources a get against 2.6 once
//! its maintenance was done (read-amp's counting test at 49e6ac8f).
//!
//! Maintenance comes last in a turn, after its reads, writes and messages, and waits for nothing:
//! a turn's budget is about as long again as the turn's own work took ([`Upkeep::budget`]), so a
//! request that arrives during it waits no longer than that. While an engine owes work it can do
//! now the shard's wait is due at once, after a yield to its other tasks, so an idle shard works
//! through the debt; once none is owed its wait is what it was. An engine whose work waits on a
//! job out on a worker does none and says so, and the worker's message wakes the shard's task
//! (`ShardDb::idle_step_now`), which ends its wait: the shard neither turns to look for the result
//! nor sleeps past it.

use std::time::Instant;

use mantle_meta::engine::{Engine, EngineError};

use super::{Node, NodeError, NodeLog};

impl<E: Engine + 'static, L: NodeLog> Node<E, L> {
    /// A turn's maintenance of the ranges the node's own task drives, the turn's own work having
    /// begun at `began`; a range on another shard is that shard's to maintain (`Shard::upkeep`).
    pub(super) fn upkeep_ranges(&mut self, began: Instant) -> Result<(), NodeError> {
        let own_ns = u64::try_from(began.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let Self {
            upkeep,
            ranges,
            owner,
            ..
        } = self;
        upkeep.turn(own_ns, ranges.len(), |at, units| {
            let Some(kept) = ranges.get(at) else {
                return Ok((0, false));
            };
            step_range(owner, kept.handle, kept.group, units)
        })?;
        Ok(())
    }
}

/// Steps the engine of the range at `handle` in `owner` ([`step`]). Maintenance that did work
/// for a range whose entries wait for room in its engine, or whose compaction waits for its
/// engine's checkpoint, may have taken the worker's message that was to wake the shard for it,
/// and made the room (`ShardDb::room_now`) or moved the checkpoint on
/// (`ShardDb::checkpoint_now`): the range is queued, so the next turn drives it and comes at
/// once (`Owner::has_work`), and a drive that finds no room yet, or the checkpoint still out,
/// leaves the wake asked again.
pub(super) fn step_range<E: Engine + 'static, S: mantle_range::LogStore>(
    owner: &mut hyper_durable::Owner<
        super::answers::Answers<S>,
        mantle_range::RangeMachine<E>,
        hyper_durable::Unbounded,
    >,
    handle: hyper_durable::Handle,
    group: u128,
    units: u64,
) -> Result<(u64, bool), NodeError> {
    let Some(replica) = owner.get_mut(handle) else {
        return Ok((0, false));
    };
    let waits = replica.waits_for_room() || replica.persisting();
    let stepped = step(group, replica.machine_mut().engine_mut(), units)?;
    if waits && stepped.0 > 0 {
        owner.schedule(handle);
    }
    Ok(stepped)
}

/// A shard's maintenance of its ranges' engines, a turn's budget at a time.
#[derive(Debug, Default)]
pub(super) struct Upkeep {
    /// The units of maintenance its turns did, and the nanoseconds those turns took: the rate a
    /// turn's budget is measured at, on the running hardware.
    units: u64,
    ns: u64,
    /// The range a turn begins at, moved on one each turn, so each owed range is first in turn.
    start: usize,
    /// Whether an engine that did work at the last turn still owed some at its end, or the budget
    /// ran out before every range was asked: the wait is due at once while so.
    owed: bool,
    /// Whether an engine owed work at the last turn but could do none of it now, a job out on a
    /// worker among such: the worker's message wakes the task, and the node's wait asks it again
    /// before it sleeps (`Node::wait`).
    waiting: bool,
}

impl Upkeep {
    /// The units a turn's maintenance is given after `own_ns` of the turn's own work: as long
    /// again, at the rate turns have done it, at least one; one until a turn has been timed. So a
    /// request that arrives during it waits at most as long again as the turn it arrived in, and
    /// a shard with no request gives maintenance about half its time (SILK's short, preemptible
    /// compaction work in the time the clients leave, research/34 §5; the engine's runtime slices
    /// are sized the same way, `mantle_engine::ranges`). Learned: both times are measured.
    pub(super) fn budget(&self, own_ns: u64) -> u64 {
        let units = u128::from(own_ns)
            .saturating_mul(u128::from(self.units))
            .checked_div(u128::from(self.ns))
            .unwrap_or(1);
        u64::try_from(units).unwrap_or(u64::MAX).max(1)
    }

    /// Whether maintenance is owed: the shard's wait is due at once.
    pub(super) fn owed(&self) -> bool {
        self.owed
    }

    /// Whether an engine owed work at the last turn that it could do none of then.
    pub(super) fn waiting(&self) -> bool {
        self.waiting
    }

    /// One turn's maintenance over `count` ranges, after `own_ns` of the turn's own work
    /// ([`Self::budget`]): `step(at, units)` steps range `at`'s engine with at most `units` and
    /// answers the units it did and whether its engine still owes work, or the node's error for a
    /// range whose engine failed. Ranges are asked from the turn's start on, each given what the
    /// budget has left, until it is spent or each was asked once. The units done.
    pub(super) fn turn(
        &mut self,
        own_ns: u64,
        count: usize,
        mut step: impl FnMut(usize, u64) -> Result<(u64, bool), NodeError>,
    ) -> Result<u64, NodeError> {
        self.owed = false;
        self.waiting = false;
        if count == 0 {
            return Ok(0);
        }
        let budget = self.budget(own_ns);
        let mut left = budget;
        let mut done = 0u64;
        let started = Instant::now();
        let start = self.start.checked_rem(count).unwrap_or(0);
        self.start = start.checked_add(1).unwrap_or(0);
        for i in 0..count {
            if left == 0 {
                // The rest were not asked: one may owe, so the next turn comes at once.
                self.owed = true;
                break;
            }
            let at = start
                .checked_add(i)
                .and_then(|at| at.checked_rem(count))
                .unwrap_or(0);
            let (did, owes) = step(at, left)?;
            done = done.saturating_add(did);
            left = left.saturating_sub(did);
            // One that owes work but could do none now (its cache refused a slot, a job out on a
            // worker) is asked again at the next turn something else brings, a worker's message
            // among them: turned at once, the shard would spin on it.
            self.owed |= owes && did > 0;
            self.waiting |= owes && did == 0;
        }
        if done > 0 {
            let ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            self.units = self.units.saturating_add(done);
            self.ns = self.ns.saturating_add(ns);
        }
        Ok(done)
    }
}

/// Steps `engine`, range `group`'s, with at most `units` of maintenance; one that failed is the
/// node's error, fenced, with its engine's own error, as a fenced drive is reported
/// (`shard::refused`).
pub(super) fn step<E: Engine>(
    group: u128,
    engine: &mut E,
    units: u64,
) -> Result<(u64, bool), NodeError> {
    engine.upkeep(units).map_err(|e| failed(group, engine, e))
}

fn failed<E: Engine>(group: u128, engine: &E, e: EngineError) -> NodeError {
    let fault = engine.fault().map(|f| format!(": {f}")).unwrap_or_default();
    NodeError::Range(mantle_range::ReplicaError::Fenced(format!(
        "range {group:x}'s engine in its maintenance: {e}{fault}"
    )))
}

#[cfg(test)]
mod tests {
    use mantle_protocol::bounds;

    use super::*;

    /// A turn's own work, as the tests' turns state it: 10 µs.
    const OWN_NS: u64 = 10_000;

    /// An engine's maintenance as a count: `owed` units left, done at most `units` a step.
    #[derive(Default)]
    struct Owing {
        owed: u64,
        asked: Vec<u64>,
    }

    impl Owing {
        fn step(&mut self, units: u64) -> (u64, bool) {
            self.asked.push(units);
            let done = units.min(self.owed);
            self.owed -= done;
            (done, self.owed > 0)
        }
    }

    /// No turn gives its ranges more than its budget in all, each range is asked in turn, and the
    /// wait is due at once only while one owes.
    #[test]
    fn a_turn_gives_its_ranges_at_most_its_budget_and_owes_while_they_do() {
        let mut upkeep = Upkeep::default();
        let mut engines = [
            Owing {
                owed: 1_000,
                ..Owing::default()
            },
            Owing::default(),
            Owing {
                owed: 10,
                ..Owing::default()
            },
        ];
        let mut turns = 0;
        while engines.iter().any(|e| e.owed > 0) {
            let budget = upkeep.budget(OWN_NS);
            assert!(budget >= 1);
            let before: u64 = engines.iter().map(|e| e.owed).sum();
            let done = upkeep
                .turn(OWN_NS, engines.len(), |at, units| {
                    Ok(engines[at].step(units))
                })
                .unwrap();
            let after: u64 = engines.iter().map(|e| e.owed).sum();
            assert_eq!(done, before - after);
            assert!(done <= budget, "turn {turns} did {done} of {budget}");
            assert!(upkeep.owed() || after == 0);
            turns += 1;
            assert!(turns <= 1_010, "the debt outlasted a unit a turn");
        }
        // The last turn's engines owed nothing as it ended, unless the budget ran out first.
        let done = upkeep
            .turn(OWN_NS, engines.len(), |at, units| {
                Ok(engines[at].step(units))
            })
            .unwrap();
        assert_eq!(done, 0);
        assert!(!upkeep.owed());
        // Every range was first in turn.
        assert!(engines.iter().all(|e| !e.asked.is_empty()));
    }

    /// With nothing owed, a turn does nothing and its wait is not brought forward; an engine that
    /// keeps no maintenance (the trait's default, the model's) owes none.
    #[test]
    fn nothing_owed_does_nothing_and_brings_no_wait_forward() {
        let mut upkeep = Upkeep::default();
        let mut model = mantle_meta::engine::Model::default();
        for _ in 0..3 {
            let done = upkeep
                .turn(OWN_NS, 2, |at, units| {
                    step(u128::try_from(at).unwrap(), &mut model, units)
                })
                .unwrap();
            assert_eq!(done, 0);
            assert!(!upkeep.owed());
        }
        assert_eq!(upkeep.budget(OWN_NS), 1, "an untimed rate grew");
        assert!(!upkeep.waiting());
        // One that owes work but can do none now brings no wait forward either: it waits, as a
        // job out on a worker does, to be asked again before the node's wait sleeps.
        assert_eq!(upkeep.turn(OWN_NS, 1, |_, _| Ok((0, true))).unwrap(), 0);
        assert!(!upkeep.owed());
        assert!(upkeep.waiting());
        // With no ranges, none is asked.
        assert_eq!(upkeep.turn(OWN_NS, 0, |_, _| Ok((1, true))).unwrap(), 0);
        assert!(!upkeep.owed());
        assert!(!upkeep.waiting());
    }

    /// The rows of a PUT burst a metadata range's engine takes: 40,000, as read-amp's counting
    /// test took, within the 20,000 to 60,000 a cell's three ranges take for 20,000 small PUTs.
    const BURST: u64 = 40_000;
    /// The rows whose lookups are counted at each look: a get and a one-row seek each.
    const SAMPLE: u64 = 2_000;

    /// Row `i` of the burst, as a Name range's rows are sized: a version's key and its record,
    /// written in the order many clients' PUTs interleave (a stride prime to the burst).
    fn row(i: u64) -> (Vec<u8>, Vec<u8>) {
        let i = i * 7_919 % BURST;
        (
            format!("v/b/k{i:06}/{}", "0".repeat(24)).into_bytes(),
            vec![7; 120],
        )
    }

    /// What a range's lookups cost over `SAMPLE` rows, a row, in thousandths: the store's pages a
    /// get read, and the sources a one-row seek listed and opened.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Cost {
        get_pages: u64,
        seek_sources: u64,
        seek_opened: u64,
    }

    fn cost(e: &mut mantle_range::Stored<hyper_block::file::DeviceFile>) -> Cost {
        use mantle_meta::engine::Rows;
        let (io, trunk) = (e.io_stats(), e.trunk_stats());
        for i in (0..BURST).step_by(usize::try_from(BURST / SAMPLE).unwrap()) {
            let (k, v) = row(i);
            assert_eq!(e.get(&k).unwrap(), Some(v.clone()), "row {i}");
        }
        let got = e.io_stats();
        for i in (0..BURST).step_by(usize::try_from(BURST / SAMPLE).unwrap()) {
            let (k, _) = row(i);
            let mut end = k.clone();
            end.push(0);
            assert!(e.next(&k, &end).unwrap().is_some(), "row {i}");
        }
        let after = e.trunk_stats();
        Cost {
            get_pages: (got.pages_read - io.pages_read) * 1_000 / SAMPLE,
            seek_sources: (after.scan_sources - trunk.scan_sources) * 1_000 / SAMPLE,
            seek_opened: (after.scan_opened - trunk.scan_opened) * 1_000 / SAMPLE,
        }
    }

    /// What a burst then gets alone showed: the cost at once and at each doubling of the node's
    /// idle turns until none was owed, then after; the turns; the units done; the checkpoints and
    /// flushes, before and after; and the turns' waits on the workers.
    #[derive(Debug)]
    struct Caught {
        costs: Vec<(u64, Cost)>,
        turns: u64,
        units: u64,
        checkpoints: ((u64, u64), (u64, u64)),
        waits: Waits,
    }

    /// How a shard's turns met jobs out on its engine's workers: the turns whose step found
    /// nothing to do but a job out (`FlushStats::idle_waits`); of those, the ones after which the
    /// shard turned again at once to look (polled) and the ones after which it waited for a wake;
    /// the waits a wake ended and those that ran to the wait's bound; the turns a wake began whose
    /// step found nothing to take; and the turns after a wait that ran to its bound whose step
    /// found the result there, a wait that slept past it.
    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
    struct Waits {
        looked: u64,
        polled: u64,
        waited: u64,
        woken: u64,
        slept: u64,
        woken_idle: u64,
        slept_past: u64,
    }

    /// Waits as a node's or a range shard's turn waits with nothing due but its bound: until any
    /// wake of the task, or `bound_ns`. Whether a wake ended it.
    async fn wait_for_wake(bound_ns: u64) -> bool {
        use hyper_rt::combine::{Either, race2};
        use std::future::poll_fn;
        use std::pin::pin;
        use std::task::Poll;
        let mut woken = false;
        let any_wake = pin!(poll_fn(move |_| {
            if woken {
                Poll::Ready(())
            } else {
                woken = true;
                Poll::Pending
            }
        }));
        let sleep = pin!(hyper_rt::futures::sleep(bound_ns));
        match race2(sleep, any_wake).await {
            Either::First(slept) => {
                slept.unwrap();
                false
            }
            Either::Second(()) => true,
        }
    }

    /// A range's engine under the node's derived configuration (`derive::engine` of a cell's rules
    /// on a device of 4 KiB writes), opened as `process::engine` opens it with `workers`
    /// maintenance workers or none, on a shard that may wait: a burst applied and checkpointed,
    /// then a get each of the node's turns, each turn ending in its maintenance (`Upkeep::turn`),
    /// until none is owed. Between turns the shard does as the node's wait does: it turns again
    /// at once, after a yield, while maintenance is owed, and otherwise waits for a wake of its
    /// task, at most the node's idle timeout.
    fn burst_then_gets(workers: usize) -> Caught {
        use hyper_block::buf::Alignment;
        use hyper_block::file::{CachingRequest, DeviceFile};
        use hyper_rt::machine::facts::Facts;
        use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
        use mantle_meta::engine::{Rows, Write};
        std::thread::spawn(move || {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("range.store");
            mantle_engine::store::permit_blocking();
            let (_, rules) = crate::derive::cell(3).unwrap();
            let engine = crate::derive::engine(4096, 1 << 30, &rules).unwrap();
            let mut shard = LocalRuntime::new(&RuntimeConfig {
                shards: 1,
                tasks_per_shard: 1,
                timers_per_shard: 2,
                interests_per_shard: interests_for(1),
                ring_entries: 1,
                step_budget_ns: 1_000_000_000,
                timer_tick_ns: 100_000,
                batch: 1,
                pin: false,
                cores: Vec::new(),
                page_bytes: usize::try_from(Facts::query().page.base).unwrap(),
                spin_ns: 0,
                wake_tracking: None,
                latency_objective_ns: None,
            })
            .unwrap();
            shard
                .block_on(async move {
                    let open = move |path: &std::path::Path, create: bool| {
                        DeviceFile::open(
                            path,
                            create,
                            CachingRequest::Buffered,
                            Alignment::new(4096).unwrap(),
                        )
                    };
                    let mut e = mantle_range::Stored::create(
                        open(&path, true).unwrap(),
                        engine.store,
                        engine.memtable_bytes,
                        engine.trunk,
                    )
                    .unwrap();
                    if workers > 0 {
                        let again = path.clone();
                        e.set_workers(
                            move || {
                                open(&again, false).map_err(|e| mantle_engine::Error::Io {
                                    op: "open",
                                    detail: e.to_string(),
                                })
                            },
                            workers,
                            "",
                        )
                        .unwrap();
                    }
                    for i in 0..BURST {
                        let (k, v) = row(i);
                        e.apply(i + 1, vec![Write::Put(k, v)]).unwrap();
                    }
                    e.persist().unwrap();
                    let persisted = e.checkpoints();
                    let mut costs = vec![(0, cost(&mut e))];
                    let mut upkeep = Upkeep::default();
                    let (mut turns, mut units, mut next) = (0u64, 0u64, 1u64);
                    let mut waits = Waits::default();
                    let (mut woke, mut slept) = (false, false);
                    let bound = u64::try_from(bounds::idle_timeout().as_nanos()).unwrap();
                    loop {
                        // A turn's own work, a get, then its maintenance.
                        let began = Instant::now();
                        let (k, v) = row(turns % BURST);
                        assert_eq!(e.get(&k).unwrap(), Some(v));
                        let own_ns = u64::try_from(began.elapsed().as_nanos()).unwrap();
                        let budget = upkeep.budget(own_ns);
                        let looked = e.flush_stats().idle_waits;
                        let mut owes = false;
                        let done = upkeep
                            .turn(own_ns, 1, |_, units| {
                                let stepped = step(1, &mut e, units)?;
                                owes = stepped.1;
                                Ok(stepped)
                            })
                            .unwrap();
                        assert!(done <= budget, "turn {turns} did {done} past {budget}");
                        units += done;
                        turns += 1;
                        let looked = e.flush_stats().idle_waits - looked;
                        waits.looked += looked;
                        if woke && looked > 0 {
                            waits.woken_idle += 1;
                        }
                        if slept && looked == 0 {
                            waits.slept_past += 1;
                        }
                        if turns == next {
                            costs.push((turns, cost(&mut e)));
                            next *= 2;
                        }
                        assert!(
                            turns <= BURST,
                            "the burst's maintenance outlasted {BURST} turns"
                        );
                        if upkeep.owed() {
                            // The wait is due at once: a yield, then the next turn.
                            waits.polled += looked;
                            (woke, slept) = (false, false);
                            hyper_rt::futures::yield_now().await;
                            continue;
                        }
                        if !owes || looked == 0 {
                            // Nothing owed, or nothing it can do now and no job out to wait for.
                            break;
                        }
                        assert!(upkeep.waiting(), "turn {turns} looked and is not waiting");
                        waits.waited += 1;
                        woke = wait_for_wake(bound).await;
                        slept = !woke;
                        if woke {
                            waits.woken += 1;
                        } else {
                            waits.slept += 1;
                        }
                    }
                    costs.push((turns, cost(&mut e)));
                    let checkpoints = (persisted, e.checkpoints());
                    assert_eq!(e.durable(), BURST);
                    Caught {
                        costs,
                        turns,
                        units,
                        checkpoints,
                        waits,
                    }
                })
                .unwrap()
        })
        .join()
        .unwrap()
    }

    /// After a burst, a range taking only gets reads the busy trunk until its maintenance is paid:
    /// the node's idle turns pay it within as many turns as the burst had rows, inline or on its
    /// worker, none doing more than its budget; the lookups' cost falls to the maintained
    /// trunk's; and maintenance makes no checkpoint and flushes nothing. A turn that finds only
    /// a job out on the worker does nothing, and the shard neither turns again to look nor sleeps
    /// past the job's result: it waits, the worker's message wakes it, and the turn that wake
    /// begins takes what came.
    #[test]
    fn a_burst_s_maintenance_is_paid_in_the_node_s_idle_turns() {
        for workers in [0, crate::derive::ENGINE_WORKERS] {
            let caught = burst_then_gets(workers);
            eprintln!("workers {workers}: {caught:?}");
            let (busy, idle) = (caught.costs[0].1, caught.costs.last().unwrap().1);
            assert!(caught.turns > 1, "the burst left nothing owed: {caught:?}");
            let w = caught.waits;
            // Every turn but the last, and those that waited on the worker, did work.
            assert!(
                caught.units + 1 + w.waited >= caught.turns,
                "a turn did nothing: {caught:?}"
            );
            // No turn looked for a job out and turned again at once; each such turn waited, each
            // wait a wake ended began a turn that found something to take, and none that ran to
            // its bound (the job still out after the node's idle timeout) slept past a result.
            assert_eq!(w.polled, 0, "a turn polled a job out: {caught:?}");
            assert_eq!(w.looked, w.waited, "{caught:?}");
            assert_eq!(w.woken + w.slept, w.waited, "{caught:?}");
            assert_eq!(w.woken_idle, 0, "a wake found nothing: {caught:?}");
            assert_eq!(w.slept_past, 0, "a wait slept past a result: {caught:?}");
            if workers == 0 {
                assert_eq!(w, Waits::default(), "an inline engine waited: {caught:?}");
            }
            assert!(
                idle.seek_sources * 2 <= busy.seek_sources,
                "a seek's sources did not fall: {busy:?} to {idle:?}"
            );
            assert!(
                idle.get_pages <= busy.get_pages,
                "a get's pages grew: {busy:?} to {idle:?}"
            );
            assert_eq!(
                caught.checkpoints.0, caught.checkpoints.1,
                "maintenance checkpointed or flushed"
            );
        }
    }

    /// A gate a maintenance worker's writes wait at while it is shut: what holds a job out on the
    /// worker for as long as a test needs. Opened when dropped, so a failed test ends its worker.
    #[derive(Default)]
    struct Gate {
        open: std::sync::Mutex<bool>,
        opened: std::sync::Condvar,
        /// The calls that came to the gate, and the task to wake at the next: what tells a test
        /// that a job is out and held there, rather than a guess at how long it takes to get
        /// there.
        came: std::sync::atomic::AtomicUsize,
        tell: std::sync::Mutex<Option<std::task::Waker>>,
    }

    impl Gate {
        fn pass(&self) {
            self.came.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if let Some(waker) = self.tell.lock().unwrap().take() {
                waker.wake();
            }
            let mut open = self.open.lock().unwrap();
            while !*open {
                open = self.opened.wait(open).unwrap();
            }
        }

        fn open(&self) {
            *self.open.lock().unwrap() = true;
            self.opened.notify_all();
        }

        fn came(&self) -> usize {
            self.came.load(std::sync::atomic::Ordering::SeqCst)
        }

        /// The runtime task polling now is woken when a call next comes to the gate.
        fn tell_task(&self) {
            let task = hyper_rt::futures::current_task().unwrap();
            *self.tell.lock().unwrap() = Some(hyper_rt::waker::waker_for(task.0));
        }
    }

    struct Opens(std::sync::Arc<Gate>);

    impl Drop for Opens {
        fn drop(&mut self) {
            self.0.open();
        }
    }

    /// A store's file whose writes on a worker's handle pass the gate first, and whose flushes
    /// pass `held` first; `flushes` counts the flushes made through this handle. Its reads pass
    /// `read_gate` first, and `reads` counts the reads made through this handle.
    struct Gated {
        file: hyper_block::file::DeviceFile,
        gate: Option<std::sync::Arc<Gate>>,
        held: Option<std::sync::Arc<Gate>>,
        flushes: std::sync::Arc<std::sync::atomic::AtomicU64>,
        reads: std::sync::Arc<std::sync::atomic::AtomicU64>,
        read_gate: Option<std::sync::Arc<Gate>>,
    }

    impl hyper_block::block::BlockFile for Gated {
        fn alignment(&self) -> hyper_block::buf::Alignment {
            self.file.alignment()
        }
        fn layout_block(&self) -> hyper_block::buf::Alignment {
            self.file.layout_block()
        }
        fn writes_reach_device(&self) -> bool {
            self.file.writes_reach_device()
        }
        fn len(&self) -> Result<u64, hyper_block::DiskError> {
            self.file.len()
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), hyper_block::DiskError> {
            if let Some(gate) = &self.read_gate {
                gate.pass();
            }
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.file.read_exact_at(buf, offset)
        }
        fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), hyper_block::DiskError> {
            if let Some(gate) = &self.gate {
                gate.pass();
            }
            self.file.write_all_at(buf, offset)
        }
        fn sync_data(&self) -> Result<(), hyper_block::DiskError> {
            if let Some(held) = &self.held {
                held.pass();
            }
            self.flushes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.file.sync_data()
        }
        fn fills_new_space(&self) -> bool {
            self.file.fills_new_space()
        }
        fn grows_by_segments(&self) -> bool {
            self.file.grows_by_segments()
        }
        fn grow(&self, len: u64) -> Result<Option<u64>, hyper_block::DiskError> {
            self.file.grow(len)
        }
        fn segment_end(&self, offset: u64) -> Result<Option<u64>, hyper_block::DiskError> {
            self.file.segment_end(offset)
        }
    }

    /// Entry `i`'s rows: eight of 16-byte keys and 100-byte values, as a range's entry of several
    /// commands writes several rows.
    fn entry_rows(i: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..8u8)
            .map(|w| (format!("k{i:08}-{w:06}").into_bytes(), vec![w; 100]))
            .collect()
    }

    /// Entry `i`'s batch: its rows put.
    fn batch(i: u64) -> Vec<mantle_meta::engine::Write> {
        let put = |(key, value)| mantle_meta::engine::Write::Put(key, value);
        entry_rows(i).into_iter().map(put).collect()
    }

    /// Whether `e` holds none of entry `i`'s rows, or all of them with their values.
    fn whole(e: &mut mantle_range::Stored<Gated>, i: u64) -> Option<bool> {
        use mantle_meta::engine::Rows;
        let mut found = Vec::new();
        for (key, value) in entry_rows(i) {
            match e.get(&key).unwrap() {
                Some(got) => {
                    assert_eq!(got, value);
                    found.push(true);
                }
                None => found.push(false),
            }
        }
        if found.iter().all(|f| *f) {
            Some(true)
        } else if found.iter().all(|f| !*f) {
            Some(false)
        } else {
            None
        }
    }

    /// A range's engine whose memtable's rotation must wait for its maintenance worker, held at
    /// the gate with a packing out: the entry that needs the rotation is answered unapplied at
    /// once (`Engine::apply_now`), the shard's thread never waiting on the worker, and no read
    /// sees any of the entry's eight rows while every entry before it reads whole. With the gate open, the worker's message wakes the
    /// task (no timer: the wait's bound is the node's idle timeout, and a wait that reaches it
    /// fails the test as a lost wake), and the entry applied again at each wake applies whole.
    #[test]
    fn an_entry_waits_unapplied_for_the_workers_message_and_applies_whole_after_it() {
        use hyper_block::buf::Alignment;
        use hyper_block::file::{CachingRequest, DeviceFile};
        use hyper_rt::machine::facts::Facts;
        use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
        use mantle_meta::engine::Engine;
        std::thread::spawn(move || {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("range.store");
            mantle_engine::store::permit_blocking();
            let (_, rules) = crate::derive::cell(3).unwrap();
            let engine = crate::derive::engine(4096, 1 << 30, &rules).unwrap();
            // A memtable of about 16 entries, its rows counted at their largest (`entry_bytes`),
            // so that a few memtables fill before the held packing stops the next rotation.
            let row = mantle_engine::memtable::hashed::entry_bytes(16, 100).unwrap();
            let memtable = row * (8 * 16 + 3);
            let mut shard = LocalRuntime::new(&RuntimeConfig {
                shards: 1,
                tasks_per_shard: 1,
                timers_per_shard: 2,
                interests_per_shard: interests_for(1),
                ring_entries: 1,
                step_budget_ns: 1_000_000_000,
                timer_tick_ns: 100_000,
                batch: 1,
                pin: false,
                cores: Vec::new(),
                page_bytes: usize::try_from(Facts::query().page.base).unwrap(),
                spin_ns: 0,
                wake_tracking: None,
                latency_objective_ns: None,
            })
            .unwrap();
            shard
                .block_on(async move {
                    let open = move |path: &std::path::Path, create: bool| {
                        DeviceFile::open(
                            path,
                            create,
                            CachingRequest::Buffered,
                            Alignment::new(4096).unwrap(),
                        )
                    };
                    let file = Gated {
                        file: open(&path, true).unwrap(),
                        gate: None,
                        held: None,
                        flushes: std::sync::Arc::default(),
                        reads: std::sync::Arc::default(),
                        read_gate: None,
                    };
                    let mut e =
                        mantle_range::Stored::create(file, engine.store, memtable, engine.trunk)
                            .unwrap();
                    let gate = std::sync::Arc::new(Gate::default());
                    let opens = Opens(gate.clone());
                    let again = path.clone();
                    e.set_workers(
                        move || {
                            let file =
                                open(&again, false).map_err(|e| mantle_engine::Error::Io {
                                    op: "open",
                                    detail: e.to_string(),
                                })?;
                            Ok(Gated {
                                file,
                                gate: Some(gate.clone()),
                                held: None,
                                flushes: std::sync::Arc::default(),
                                reads: std::sync::Arc::default(),
                                read_gate: None,
                            })
                        },
                        1,
                        "",
                    )
                    .unwrap();
                    // Each memtable takes 16 entries; the worker's first packing never ends
                    // while the gate is shut, so a rotation soon waits on it.
                    let mut waited = None;
                    for i in 1..=16 * 64 {
                        if !e.apply_now(i, batch(i)).unwrap() {
                            waited = Some(i);
                            break;
                        }
                    }
                    let i = waited.expect("no rotation waited on the held worker");
                    assert_eq!(e.applied(), i - 1);
                    assert_eq!(whole(&mut e, i), Some(false), "entry {i} applied in part");
                    for before in 1..i {
                        assert_eq!(whole(&mut e, before), Some(true), "entry {before}");
                    }
                    // Asked again with the worker still held: refused again, still waiting.
                    assert!(!e.apply_now(i, batch(i)).unwrap());
                    assert_eq!(whole(&mut e, i), Some(false), "entry {i} applied in part");
                    drop(opens);
                    let bound = u64::try_from(bounds::idle_timeout().as_nanos()).unwrap();
                    let mut wakes = 0;
                    loop {
                        assert!(wait_for_wake(bound).await, "a wake was lost: entry {i}");
                        wakes += 1;
                        if e.apply_now(i, batch(i)).unwrap() {
                            break;
                        }
                        assert!(
                            wakes < 64,
                            "entry {i} waited past 64 of the worker's messages"
                        );
                    }
                    assert_eq!(e.applied(), i);
                    assert_eq!(whole(&mut e, i), Some(true), "entry {i}");
                    e.persist().unwrap();
                    for before in 1..=i {
                        assert_eq!(whole(&mut e, before), Some(true), "entry {before}");
                    }
                })
                .unwrap();
        })
        .join()
        .unwrap();
    }

    /// Runs `test` on a runtime shard that may wait, as a node's does, with a range's engine
    /// under the node's derived configuration in a new store at `path`, its one maintenance
    /// worker's flushes held at `held` and the shard's own handle's flushes counted in `here`.
    fn on_a_shard_with_a_held_flush<T>(
        test: impl FnOnce(
            mantle_range::Stored<Gated>,
            std::sync::Arc<Gate>,
            std::sync::Arc<std::sync::atomic::AtomicU64>,
            std::path::PathBuf,
        ) -> T
        + Send
        + 'static,
    ) where
        T: std::future::Future<Output = ()> + 'static,
    {
        use hyper_rt::machine::facts::Facts;
        use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
        std::thread::spawn(move || {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("range.store");
            mantle_engine::store::permit_blocking();
            let (_, rules) = crate::derive::cell(3).unwrap();
            let engine = crate::derive::engine(4096, 1 << 30, &rules).unwrap();
            // Room for 64 entries of eight rows at their largest: the checkpoint's memtable and
            // every entry applied while it is out.
            let row = mantle_engine::memtable::hashed::entry_bytes(16, 100).unwrap();
            let memtable = row * (8 * 64 + 3);
            let mut shard = LocalRuntime::new(&RuntimeConfig {
                shards: 1,
                tasks_per_shard: 1,
                timers_per_shard: 2,
                interests_per_shard: interests_for(1),
                ring_entries: 1,
                step_budget_ns: 1_000_000_000,
                timer_tick_ns: 100_000,
                batch: 1,
                pin: false,
                cores: Vec::new(),
                page_bytes: usize::try_from(Facts::query().page.base).unwrap(),
                spin_ns: 0,
                wake_tracking: None,
                latency_objective_ns: None,
            })
            .unwrap();
            shard
                .block_on(async move {
                    let held = std::sync::Arc::new(Gate::default());
                    let here = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
                    let file = Gated {
                        file: open_store(&path, true).unwrap(),
                        gate: None,
                        held: None,
                        flushes: here.clone(),
                        reads: std::sync::Arc::default(),
                        read_gate: None,
                    };
                    let mut e =
                        mantle_range::Stored::create(file, engine.store, memtable, engine.trunk)
                            .unwrap();
                    let (again, worker_held) = (path.clone(), held.clone());
                    e.set_workers(
                        move || {
                            let file = open_store(&again, false).map_err(|e| {
                                mantle_engine::Error::Io {
                                    op: "open",
                                    detail: e.to_string(),
                                }
                            })?;
                            Ok(Gated {
                                file,
                                gate: None,
                                held: Some(worker_held.clone()),
                                flushes: std::sync::Arc::default(),
                                reads: std::sync::Arc::default(),
                                read_gate: None,
                            })
                        },
                        1,
                        "",
                    )
                    .unwrap();
                    test(e, held, here, path).await;
                })
                .unwrap();
            drop(dir);
        })
        .join()
        .unwrap();
    }

    fn open_store(
        path: &std::path::Path,
        create: bool,
    ) -> Result<hyper_block::file::DeviceFile, hyper_block::DiskError> {
        hyper_block::file::DeviceFile::open(
            path,
            create,
            hyper_block::file::CachingRequest::Buffered,
            hyper_block::buf::Alignment::new(4096).unwrap(),
        )
    }

    /// Takes the checkpoint begun at `e`'s applied index on (`Engine::persist_now`), answered
    /// not durable at each call, until its flush is out on the worker and held at `held`: each
    /// wait is for a wake of the task, by the worker's message or by the flush coming to the
    /// gate, and a wait that reaches the node's idle timeout fails the test as a lost wake.
    async fn until_the_flush_is_held(e: &mut mantle_range::Stored<Gated>, held: &Gate) {
        use mantle_meta::engine::Engine;
        let bound = u64::try_from(bounds::idle_timeout().as_nanos()).unwrap();
        for _ in 0..64 {
            held.tell_task();
            assert!(!e.persist_now().unwrap(), "durable with its flush held");
            if held.came() > 0 {
                return;
            }
            assert!(
                wait_for_wake(bound).await,
                "a wake was lost before the flush went out"
            );
        }
        panic!("the checkpoint's flush never went out in 64 of the task's wakes");
    }

    /// A range's checkpoint begun without waiting (`Engine::persist_now`, `ShardDb::
    /// checkpoint_now`), its device flush held at a gate on the maintenance worker's handle: the
    /// shard's thread waits on neither, and while the flush is out entries apply and read whole,
    /// a second ask begins no other checkpoint, nothing is durable past the store's first
    /// checkpoint, and no flush runs on the shard's own handle. With the gate open, the worker's
    /// message wakes the task (no timer: the wait's bound is the node's idle timeout, and one
    /// that reaches it fails the test as a lost wake) and the checkpoint completes at the index
    /// applied when it began, not past it, both its flushes on the worker; the store opened
    /// again stands there.
    #[test]
    fn a_checkpoint_flushes_on_its_worker_and_entries_apply_while_it_is_out() {
        on_a_shard_with_a_held_flush(|mut e, held, here, path| async move {
            use mantle_meta::engine::Engine;
            use std::sync::atomic::Ordering::SeqCst;
            for i in 1..=8 {
                assert!(e.apply_now(i, batch(i)).unwrap(), "entry {i}");
            }
            // The new store's own first checkpoint flushed on the shard's handle, before any entry.
            let flushed = here.load(SeqCst);
            until_the_flush_is_held(&mut e, &held).await;
            for i in 9..=16 {
                assert!(
                    e.apply_now(i, batch(i)).unwrap(),
                    "entry {i} waited on the checkpoint"
                );
            }
            for i in 1..=16 {
                assert_eq!(whole(&mut e, i), Some(true), "entry {i}");
            }
            assert!(!e.persist_now().unwrap(), "durable with its flush held");
            assert_eq!(held.came(), 1, "a second checkpoint's flush went out");
            assert_eq!(e.durable(), 0);
            assert_eq!(
                here.load(SeqCst),
                flushed,
                "a flush ran on the shard's handle"
            );
            held.open();
            let bound = u64::try_from(bounds::idle_timeout().as_nanos()).unwrap();
            let mut wakes = 0;
            while !e.persist_now().unwrap() {
                assert!(
                    wait_for_wake(bound).await,
                    "a wake was lost: the checkpoint's flush"
                );
                wakes += 1;
                assert!(
                    wakes < 64,
                    "the checkpoint waited past 64 of the worker's messages"
                );
            }
            assert_eq!((e.applied(), e.durable()), (16, 8));
            assert_eq!(
                held.came(),
                2,
                "the checkpoint's two flushes, on its worker"
            );
            assert_eq!(
                here.load(SeqCst),
                flushed,
                "a flush ran on the shard's handle"
            );
            drop(e);
            let file = Gated {
                file: open_store(&path, false).unwrap(),
                gate: None,
                held: None,
                flushes: std::sync::Arc::default(),
                reads: std::sync::Arc::default(),
                read_gate: None,
            };
            let (_, rules) = crate::derive::cell(3).unwrap();
            let engine = crate::derive::engine(4096, 1 << 30, &rules).unwrap();
            let mut e =
                mantle_range::Stored::open(file, engine.store, 1 << 20, engine.trunk).unwrap();
            assert_eq!(
                e.applied(),
                8,
                "opened again past the checkpoint or before it"
            );
            for i in 1..=8 {
                assert_eq!(whole(&mut e, i), Some(true), "entry {i}");
            }
            assert_eq!(
                whole(&mut e, 9),
                Some(false),
                "entry 9, past the checkpoint"
            );
        });
    }

    /// A store whose checkpoint begun without waiting never completed, its first flush made by
    /// the worker and its superblock never written, as at a crash while the flush was out, opens
    /// again at the checkpoint before it: the store's first, before any entry, which the log
    /// still covers, the compaction never having passed it (docs/design/replica.md §4).
    #[test]
    fn a_checkpoint_that_never_completed_opens_at_the_one_before() {
        on_a_shard_with_a_held_flush(|mut e, held, _here, path| async move {
            use mantle_meta::engine::Engine;
            for i in 1..=8 {
                assert!(e.apply_now(i, batch(i)).unwrap(), "entry {i}");
            }
            until_the_flush_is_held(&mut e, &held).await;
            // The worker's flush ends, unasked by the shard, and the engine goes.
            held.open();
            drop(e);
            let file = Gated {
                file: open_store(&path, false).unwrap(),
                gate: None,
                held: None,
                flushes: std::sync::Arc::default(),
                reads: std::sync::Arc::default(),
                read_gate: None,
            };
            let (_, rules) = crate::derive::cell(3).unwrap();
            let engine = crate::derive::engine(4096, 1 << 30, &rules).unwrap();
            let mut e =
                mantle_range::Stored::open(file, engine.store, 1 << 20, engine.trunk).unwrap();
            assert_eq!((e.applied(), e.durable()), (0, 0));
            assert_eq!(
                whole(&mut e, 1),
                Some(false),
                "entry 1, past the checkpoint"
            );
        });
    }

    /// An entry the memtable has no room for while a checkpoint's flush is out waits for the
    /// checkpoint, unapplied and unseen, rather than rotate the memtable under the trunk and
    /// allocator the checkpoint names (`ShardDb::room_now`). With the gate open, the worker's
    /// messages wake the task; the entry's own asks take the checkpoint on to durable, then make
    /// its room, and it applies whole; the compaction's next ask finds the checkpoint durable at
    /// the index applied when it began.
    #[test]
    fn an_entry_with_no_room_while_a_checkpoint_is_out_waits_for_it() {
        on_a_shard_with_a_held_flush(|mut e, held, _here, _path| async move {
            use mantle_meta::engine::Engine;
            for i in 1..=8 {
                assert!(e.apply_now(i, batch(i)).unwrap(), "entry {i}");
            }
            until_the_flush_is_held(&mut e, &held).await;
            let mut waited = None;
            for i in 9..=8 * 64 {
                if !e.apply_now(i, batch(i)).unwrap() {
                    waited = Some(i);
                    break;
                }
            }
            let i = waited.expect("the memtable never filled while the checkpoint was out");
            assert_eq!(e.applied(), i - 1);
            assert_eq!(whole(&mut e, i), Some(false), "entry {i} applied in part");
            assert_eq!(held.came(), 1, "a second checkpoint's flush went out");
            held.open();
            let bound = u64::try_from(bounds::idle_timeout().as_nanos()).unwrap();
            let mut wakes = 0;
            loop {
                assert!(wait_for_wake(bound).await, "a wake was lost: entry {i}");
                wakes += 1;
                if e.apply_now(i, batch(i)).unwrap() {
                    break;
                }
                assert!(
                    wakes < 64,
                    "entry {i} waited past 64 of the worker's messages"
                );
            }
            assert_eq!(whole(&mut e, i), Some(true), "entry {i}");
            assert!(
                e.persist_now().unwrap(),
                "the checkpoint its entry waited for"
            );
            assert_eq!(e.durable(), 8);
        });
    }

    /// An entry larger than a whole memtable applied while the checkpoint a range's compaction
    /// begins without waiting still packs, as a three-node cell's entries of many 1-byte PUTs
    /// were: the checkpoint's first step swaps the memtable out to the worker, so the entry finds
    /// the memtable empty and is answered room at once (`ShardDb::room_now`); its writes fill
    /// the memtable, and the write that no longer fits rotates inline, finishing the checkpoint
    /// first, which packs the memtable those writes went into. The rotation is left an empty
    /// memtable and swaps none (`ShardDb::rotate`): swapped, its packing came back from the
    /// worker with no parts and fenced the engine ("a packing worker's result of other than its
    /// parts"). The entry and those after it apply whole, the checkpoint is durable at the index
    /// applied when it began, and the next takes every packing back.
    #[test]
    fn an_entry_larger_than_the_memtable_while_a_checkpoint_packs_applies_whole() {
        on_a_shard_with_a_held_flush(|mut e, held, _here, _path| async move {
            use mantle_meta::engine::{Engine, Rows, Write};
            for i in 1..=8 {
                assert!(e.apply_now(i, batch(i)).unwrap(), "entry {i}");
            }
            // Only the checkpoint's packing meets the entry, not its flushes.
            held.open();
            assert!(
                !e.persist_now().unwrap(),
                "durable before the memtable it packs came back"
            );
            // Past the memtable's room for 64 entries of eight rows.
            let rows: Vec<(Vec<u8>, Vec<u8>)> = (0..600u32)
                .map(|r| (format!("big-{r:012}").into_bytes(), vec![7; 100]))
                .collect();
            let writes = rows
                .iter()
                .map(|(k, v)| Write::Put(k.clone(), v.clone()))
                .collect();
            assert!(e.apply_now(9, writes).unwrap(), "entry 9, past a memtable");
            for i in 10..=16 {
                assert!(e.apply_now(i, batch(i)).unwrap(), "entry {i}");
            }
            assert!(e.persist_now().unwrap(), "the checkpoint entry 9 finished");
            assert_eq!(e.durable(), 8);
            e.persist().unwrap();
            assert_eq!((e.applied(), e.durable()), (16, 16));
            for (key, value) in &rows {
                assert_eq!(
                    e.get(key).unwrap().as_ref(),
                    Some(value),
                    "a row of entry 9"
                );
            }
            for i in (1..=8).chain(10..=16) {
                assert_eq!(whole(&mut e, i), Some(true), "entry {i}");
            }
        });
    }

    /// A range's engine opened again over rows only its store's file holds, its cache empty: an
    /// entry's read of a row (`Engine::get_now`, `Engine::next_now`, as the range's state
    /// machine reads them) is answered not in memory at once, the shard's own handle reading
    /// nothing, and the page it needs is read on the maintenance worker's handle, held at a gate.
    /// Asked again while the worker is held, it is refused again and no second load goes out.
    /// With the gate open, each worker's message wakes the task (no timer: the wait's bound is
    /// the node's idle timeout, and one that reaches it fails the test as a lost wake), the read
    /// made again takes the loaded page and the next missing one, if any, loads, until the row
    /// reads whole; the shard's handle read nothing throughout, the worker's one read a page
    /// loaded. The entry that read it then applies whole and lets the loaded pages go.
    #[test]
    fn an_entrys_reads_load_their_pages_on_the_worker_and_the_shard_reads_none() {
        use mantle_meta::engine::{Engine, EngineError, Rows};
        on_a_shard_with_a_held_flush(|mut e, _held, _here, path| async move {
            // Three memtables' entries, each packed by a checkpoint: a get reads the branch its
            // key's filter passes, and a scan merges every branch on its path.
            for i in 1..=3 * 64 {
                e.apply(i, batch(i)).unwrap();
                if i % 64 == 0 {
                    e.persist().unwrap();
                }
            }
            drop(e);
            let (_, rules) = crate::derive::cell(3).unwrap();
            let engine = crate::derive::engine(4096, 1 << 30, &rules).unwrap();
            let row = mantle_engine::memtable::hashed::entry_bytes(16, 100).unwrap();
            let shard_reads = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
            let file = Gated {
                file: open_store(&path, false).unwrap(),
                gate: None,
                held: None,
                flushes: std::sync::Arc::default(),
                reads: shard_reads.clone(),
                read_gate: None,
            };
            let mut e =
                mantle_range::Stored::open(file, engine.store, row * (8 * 64 + 3), engine.trunk)
                    .unwrap();
            let gate = std::sync::Arc::new(Gate::default());
            let opens = Opens(gate.clone());
            let worker_reads = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
            let (again, worker_gate, counted) = (path.clone(), gate.clone(), worker_reads.clone());
            e.set_workers(
                move || {
                    let file = open_store(&again, false).map_err(|e| mantle_engine::Error::Io {
                        op: "open",
                        detail: e.to_string(),
                    })?;
                    Ok(Gated {
                        file,
                        gate: None,
                        held: None,
                        flushes: std::sync::Arc::default(),
                        reads: counted.clone(),
                        read_gate: Some(worker_gate.clone()),
                    })
                },
                1,
                "",
            )
            .unwrap();
            let reads = |n: &std::sync::Arc<std::sync::atomic::AtomicU64>| {
                n.load(std::sync::atomic::Ordering::SeqCst)
            };
            let opened = reads(&shard_reads);
            let (key, value) = entry_rows(33).swap_remove(3);
            let bound = u64::try_from(bounds::idle_timeout().as_nanos()).unwrap();
            // The first read refused: its page goes out to the worker, held at the gate.
            gate.tell_task();
            assert_eq!(e.get_now(&key), Err(EngineError::Unloaded));
            assert_eq!(reads(&shard_reads), opened, "the shard read the device");
            assert_eq!(e.loads(), (1, 1));
            for _ in 0..64 {
                if gate.came() > 0 {
                    break;
                }
                assert!(
                    wait_for_wake(bound).await,
                    "the load never reached the worker"
                );
            }
            assert_eq!(gate.came(), 1);
            // Asked again with the worker held: refused, the same load out, none other sent.
            assert_eq!(e.get_now(&key), Err(EngineError::Unloaded));
            assert_eq!((gate.came(), e.loads()), (1, (1, 1)));
            drop(opens);
            let mut refusals = 1u64;
            let got = loop {
                assert!(
                    wait_for_wake(bound).await,
                    "a wake was lost: {refusals} refused"
                );
                match e.get_now(&key) {
                    Ok(got) => break got,
                    Err(EngineError::Unloaded) => refusals += 1,
                    Err(other) => panic!("{other}"),
                }
                assert!(
                    refusals < 64,
                    "the get waited past 64 of the worker's messages"
                );
            };
            assert_eq!(got, Some(value.clone()));
            assert_eq!(reads(&shard_reads), opened, "the shard read the device");
            // The one-row scan of the same row, as `name::current` reads a name: the pages held
            // serve it, and any other it needs loads as the get's did.
            let row = loop {
                match e.next_now(&key, b"l") {
                    Ok(row) => break row,
                    Err(EngineError::Unloaded) => refusals += 1,
                    Err(other) => panic!("{other}"),
                }
                assert!(
                    refusals < 64,
                    "the scan waited past 64 of the worker's messages"
                );
                assert!(
                    wait_for_wake(bound).await,
                    "a wake was lost: {refusals} refused"
                );
            };
            assert_eq!(row, Some((key.clone(), value)));
            assert_eq!(reads(&shard_reads), opened, "the shard read the device");
            // Each page loaded once, on the worker: as many reads there as pages held, a read
            // refused while its page was still out sending none.
            let (held, out) = e.loads();
            assert_eq!(out, 0);
            assert!(held >= 1 && u64::try_from(held).unwrap() <= refusals);
            assert_eq!(reads(&worker_reads), u64::try_from(held).unwrap());
            // The entry that read it applies whole and lets its pages go.
            assert!(e.apply_now(3 * 64 + 1, batch(3 * 64 + 1)).unwrap());
            assert_eq!(e.loads(), (0, 0));
            assert_eq!(whole(&mut e, 3 * 64 + 1), Some(true));
            // The read as an entry made it before, which waits on the device: a row another
            // entry wrote, cold, is read on the shard's own handle.
            let (cold, _) = entry_rows(1).swap_remove(0);
            assert!(e.get(&cold).unwrap().is_some());
            let before = reads(&shard_reads) - opened;
            println!(
                "shard reads: {before} by a waiting get, none by the ready reads; worker reads: \
                 {held} for {refusals} refused"
            );
            assert!(before > 0, "a cold get read nothing on the shard");
        });
    }

    /// Puts an empty object at `key` in bucket "b" of a Name range, as entry `index`, as a
    /// range's state machine applies a PUT.
    fn put_object(e: &mut mantle_range::Stored<Gated>, index: u64, key: &str) {
        use mantle_meta::name;
        let put = name::Command::Put(name::Put {
            bucket: "b".into(),
            incarnation: 1,
            key: key.into(),
            versioning: mantle_meta::record::Versioning::Unversioned,
            preconditions: name::Preconditions::default(),
            at_ns: index * 1_000_000_000,
            ordered_ns: None,
            version: mantle_meta::record::Version {
                marker: false,
                null: false,
                modified_ns: 0,
                etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
                size: 0,
                checksum: None,
                file: None,
                owner: "o".into(),
                headers: Vec::new(),
                retention: None,
                legal_hold: None,
                listing: None,
            },
            default: None,
            id: u128::from(index),
            deadline_ns: u64::MAX,
        });
        assert!(matches!(
            name::apply(e, index, &put).unwrap(),
            name::Outcome::Put { .. }
        ));
    }

    /// A Name range's engine opened again over rows only its store's file holds, its cache empty,
    /// the worker's reads held at a gate: a read its range confirmed and applied through, served
    /// as the node serves one (`Query::answer_confirmed`), whether a GET's read of a key's current
    /// version or a LIST's page, is answered waiting at once, the shard's own handle reading
    /// nothing, and its pages load on the worker. With the gate open, each worker's message wakes
    /// the task (no timer: a wait that reaches the node's idle timeout fails the test as a lost
    /// wake), and the reads made again answer what the same reads made waiting on the device
    /// answer, the shard's handle at no reads throughout. Answered, they let the loaded pages go.
    #[test]
    fn a_confirmed_reads_pages_load_on_the_worker_and_the_shard_reads_none() {
        use crate::read::{Query, Read};
        use mantle_meta::engine::Engine;
        use mantle_meta::name;
        on_a_shard_with_a_held_flush(|mut e, _held, _here, path| async move {
            e.install(0, name::first(1).unwrap()).unwrap();
            let open = name::Command::Gate(name::GateChange {
                bucket: "b".into(),
                incarnation: 1,
                attempt: 1,
                from: None,
                to: Some(mantle_meta::record::GateState::Open),
                generation: 1,
            });
            assert_eq!(
                name::apply(&mut e, 1, &open).unwrap(),
                name::Outcome::GateMoved
            );
            // Three checkpoints' objects, each packed: the get reads the branch its key's filter
            // passes, and the listing merges every branch on its path.
            let keys: Vec<String> = (0..3 * 64).map(|n| format!("k{n:04}")).collect();
            for (n, key) in (2u64..).zip(&keys) {
                put_object(&mut e, n, key);
                if n % 64 == 1 {
                    e.persist().unwrap();
                }
            }
            e.persist().unwrap();
            drop(e);
            let (_, rules) = crate::derive::cell(3).unwrap();
            let engine = crate::derive::engine(4096, 1 << 30, &rules).unwrap();
            let row = mantle_engine::memtable::hashed::entry_bytes(16, 100).unwrap();
            let shard_reads = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
            let file = Gated {
                file: open_store(&path, false).unwrap(),
                gate: None,
                held: None,
                flushes: std::sync::Arc::default(),
                reads: shard_reads.clone(),
                read_gate: None,
            };
            let mut e =
                mantle_range::Stored::open(file, engine.store, row * (8 * 64 + 3), engine.trunk)
                    .unwrap();
            let gate = std::sync::Arc::new(Gate::default());
            let opens = Opens(gate.clone());
            let worker_reads = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
            let (again, worker_gate, counted) = (path.clone(), gate.clone(), worker_reads.clone());
            e.set_workers(
                move || {
                    let file = open_store(&again, false).map_err(|e| mantle_engine::Error::Io {
                        op: "open",
                        detail: e.to_string(),
                    })?;
                    Ok(Gated {
                        file,
                        gate: None,
                        held: None,
                        flushes: std::sync::Arc::default(),
                        reads: counted.clone(),
                        read_gate: Some(worker_gate.clone()),
                    })
                },
                1,
                "",
            )
            .unwrap();
            let reads = |n: &std::sync::Arc<std::sync::atomic::AtomicU64>| {
                n.load(std::sync::atomic::Ordering::SeqCst)
            };
            let opened = reads(&shard_reads);
            let bound = u64::try_from(bounds::idle_timeout().as_nanos()).unwrap();
            let room = 1 << 16;
            let queries = [
                Query::Current {
                    bucket: "b".into(),
                    key: keys[100].clone(),
                },
                Query::List(mantle_protocol::object::List {
                    bucket: "b".into(),
                    prefix: String::new(),
                    delimiter: "/".into(),
                    start: None,
                    max_keys: 1000,
                    fetch_owner: true,
                }),
            ];
            // Each read's first pass, the worker held: answered waiting, its page out.
            gate.tell_task();
            let mut loads = [0usize; 2];
            for (query, loads) in queries.iter().zip(&mut loads) {
                assert_eq!(query.answer_confirmed(&mut e, room, loads, false), None);
            }
            assert_eq!(reads(&shard_reads), opened, "the shard read the device");
            assert_eq!(
                e.loads().1,
                1,
                "a second load went out for the page loading"
            );
            drop(opens);
            let mut answers: [Option<Read>; 2] = [None, None];
            let mut wakes = 0u64;
            while answers.iter().any(Option::is_none) {
                assert!(wait_for_wake(bound).await, "a wake was lost: {wakes} wakes");
                wakes += 1;
                for ((query, loads), answer) in queries.iter().zip(&mut loads).zip(&mut answers) {
                    if answer.is_some() {
                        continue;
                    }
                    if let Some(read) = query.answer_confirmed(&mut e, room, loads, false) {
                        *answer = Some(read.unwrap());
                    }
                }
                assert!(wakes < 256, "the reads waited past 256 of the task's wakes");
            }
            assert_eq!(reads(&shard_reads), opened, "the shard read the device");
            let worker = reads(&worker_reads);
            assert!(worker > 0, "the worker read nothing");
            // Answered, the pages they loaded went to the cache.
            assert_eq!(e.loads().0, 0);
            let [Some(get), Some(list)] = answers else {
                panic!("a read not answered");
            };
            let Read::Current(Some((_, version))) = &get else {
                panic!("{get:?}");
            };
            assert_eq!(version.size, 0);
            let Read::Keys(page) = &list else {
                panic!("{list:?}");
            };
            assert!(!page.truncated);
            assert_eq!(
                page.contents
                    .iter()
                    .map(|l| l.key.clone())
                    .collect::<Vec<_>>(),
                keys
            );
            // The same reads as the node made them before, waiting on the device, in the engine
            // opened cold again: they answer the same, read on the shard's own handle.
            drop(e);
            let cold_reads = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
            let file = Gated {
                file: open_store(&path, false).unwrap(),
                gate: None,
                held: None,
                flushes: std::sync::Arc::default(),
                reads: cold_reads.clone(),
                read_gate: None,
            };
            let engine = crate::derive::engine(4096, 1 << 30, &rules).unwrap();
            let mut cold =
                mantle_range::Stored::open(file, engine.store, row * (8 * 64 + 3), engine.trunk)
                    .unwrap();
            // A cache that holds every page, as a node lends its engines one: each page is read
            // once.
            cold.lend_cache(1 << 20);
            let mut before = [0u64; 2];
            for ((query, answer), before) in queries.iter().zip([get, list]).zip(&mut before) {
                let opened = reads(&cold_reads);
                assert_eq!(query.answer(&mut cold, room).unwrap(), answer);
                *before = reads(&cold_reads) - opened;
            }
            println!(
                "shard reads: {before:?} by the get and the list made waiting, none by them \
                 confirmed; worker reads: {worker} over {wakes} wakes, loads counted {loads:?}"
            );
            assert!(before[0] > 0, "the cold get read nothing on the shard");
        });
    }

    /// A failed engine stops the turn with the node's error, fenced.
    #[test]
    fn a_failed_maintenance_is_the_nodes_error() {
        let mut upkeep = Upkeep::default();
        let e = upkeep
            .turn(OWN_NS, 1, |_, _| {
                Err(failed(
                    7,
                    &mantle_meta::engine::Model::default(),
                    EngineError::Fenced,
                ))
            })
            .unwrap_err();
        assert!(
            matches!(&e, NodeError::Range(mantle_range::ReplicaError::Fenced(text)) if text.contains("range 7")),
            "{e:?}"
        );
    }
}
