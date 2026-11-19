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
            match owner.get_mut(kept.handle) {
                Some(replica) => step(kept.group, replica.machine_mut().engine_mut(), units),
                None => Ok((0, false)),
            }
        })?;
        Ok(())
    }
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
