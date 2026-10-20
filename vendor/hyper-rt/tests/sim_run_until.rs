//! A bounded run of the simulation (docs/runtime.md §11): `SimRuntime::run_until` takes every event due
//! at or before its deadline (a shard's timer, a datagram's arrival, the work each wakes) and none due
//! after it, and leaves the clock at the deadline. A world with a periodic timer always has a next
//! deadline, so `run_until_idle` never returns from it; these worlds' tasks run forever, and each is run
//! in slices. One test holds every slice to the closed form of what is due within it; the other holds a
//! run in slices to the same run taken in one, and to itself from the same seed.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cognitive_complexity,
    clippy::cast_possible_truncation
)]

use std::sync::mpsc::{Receiver, channel};

use hyper_rt::futures::{now_ns, sleep};
use hyper_rt::runtime::RuntimeConfig;
use hyper_rt::sim::{SimFabricStats, SimPath, SimRuntime};
use hyper_rt::udp::{Ipv4Addr, SocketAddrV4, UdpSocket};

/// Two shards whose wheels tick each microsecond.
fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 2,
        tasks_per_shard: 16,
        timers_per_shard: 16,
        interests_per_shard: 16,
        ring_entries: 16,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 1_000,
        batch: 16,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    }
}

/// Shape: the sender's period, 3 ms: a whole number of the wheel's 1 µs ticks, so each of its timers
/// fires at its deadline exactly, and no multiple of the slice.
const PERIOD_NS: u64 = 3_000_000;
/// Shape: the receiving shard's own period, 7 ms: a second timer, prime to the first and to the slice.
const OTHER_PERIOD_NS: u64 = 7_000_000;
/// Shape: the path's one-way delay, 5 ms: longer than a slice, so datagrams are in flight across slices'
/// ends.
const DELAY_NS: u64 = 5_000_000;
/// Shape: the jitter of the second test's path, ± 2.5 ms around its delay: wider than the sender's
/// period less the slice, so draws move arrivals across slices' ends.
const JITTER_NS: u64 = 2_500_000;
/// Shape: one slice, 2 ms.
const SLICE_NS: u64 = 2_000_000;
/// Shape: the slices run, 40 ms of virtual time: thirteen of the sender's periods, five of the other's.
const SLICES: u64 = 20;

/// What a task saw, and when, on the virtual clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Seen {
    at: u64,
    what: What,
    k: u64,
}

/// The kinds of event the world records.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum What {
    /// The sender's k-th period ended, and it sent datagram k.
    Sent,
    /// The receiving shard's own k-th period ended.
    Other,
    /// Datagram k arrived.
    Arrived,
}

/// Builds the world on `sim`: shard 0 receives, and keeps a period of its own; shard 1 sends a datagram
/// to shard 0 at the end of each of its periods. Every task runs forever, so the world always has a next
/// deadline. The receiver binds at time zero, the one slice before the sender is spawned.
fn world(sim: &mut SimRuntime) -> Receiver<Seen> {
    let shards = sim.shard_ids();
    let (seen, events) = channel();
    let (ports, port) = channel();
    let arrivals = seen.clone();
    sim.spawn_on(shards[0], async move {
        let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        ports.send(socket.local_addr().unwrap().port()).unwrap();
        let mut buffer = [0u8; 8];
        loop {
            let (length, _) = socket.recv_from(&mut buffer).await.unwrap();
            assert_eq!(length, 8, "a whole datagram arrived");
            let k = u64::from_le_bytes(buffer);
            arrivals
                .send(Seen {
                    at: now_ns(),
                    what: What::Arrived,
                    k,
                })
                .unwrap();
        }
    })
    .unwrap();
    let others = seen.clone();
    sim.spawn_on(shards[0], async move {
        let mut k = 0;
        loop {
            sleep(OTHER_PERIOD_NS).await.unwrap();
            k += 1;
            others
                .send(Seen {
                    at: now_ns(),
                    what: What::Other,
                    k,
                })
                .unwrap();
        }
    })
    .unwrap();
    sim.run_until(0);
    assert_eq!(sim.now_ns(), 0, "a run to time zero leaves the clock there");
    let port = port.try_recv().expect("the receiver bound at time zero");
    sim.spawn_on(shards[1], async move {
        let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let to = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
        let mut k = 0u64;
        loop {
            sleep(PERIOD_NS).await.unwrap();
            k += 1;
            seen.send(Seen {
                at: now_ns(),
                what: What::Sent,
                k,
            })
            .unwrap();
            assert_eq!(socket.send_to(&k.to_le_bytes(), to).unwrap(), 8);
        }
    })
    .unwrap();
    events
}

/// Everything due at or before `deadline` on the fixed path, at the time it was due, in time order.
fn due_by(deadline: u64) -> Vec<Seen> {
    let mut due = Vec::new();
    for k in 1..=deadline / PERIOD_NS {
        due.push(Seen {
            at: k * PERIOD_NS,
            what: What::Sent,
            k,
        });
        if k * PERIOD_NS + DELAY_NS <= deadline {
            due.push(Seen {
                at: k * PERIOD_NS + DELAY_NS,
                what: What::Arrived,
                k,
            });
        }
    }
    for k in 1..=deadline / OTHER_PERIOD_NS {
        due.push(Seen {
            at: k * OTHER_PERIOD_NS,
            what: What::Other,
            k,
        });
    }
    due.sort();
    due
}

/// Do: run a world whose every task runs forever (two periodic timers on two shards, and a datagram
/// sent on a 5 ms path at the end of every 3 ms) in slices of 2 ms. Expect: after each slice the clock is
/// at the slice's end, and the world has seen exactly what was due by it, each event at the time it was
/// due: every timer whose deadline has come and no other, every datagram whose arrival has come and no
/// other, those still in flight left for a later slice.
#[test]
fn a_bounded_run_takes_exactly_what_is_due_within_each_slice() {
    let mut sim = SimRuntime::new(&config(), 1).unwrap();
    sim.set_path(SimPath::in_order(DELAY_NS, 0));
    let events = world(&mut sim);
    let mut seen = Vec::new();
    for slice in 1..=SLICES {
        let deadline = slice * SLICE_NS;
        sim.run_until(deadline);
        assert_eq!(
            sim.now_ns(),
            deadline,
            "the clock is left at the slice's end"
        );
        seen.extend(events.try_iter());
        let mut sorted = seen.clone();
        sorted.sort();
        assert_eq!(
            sorted,
            due_by(deadline),
            "what slice {slice} took, to {deadline} ns"
        );
    }
    let stats = sim.stats();
    let arrived = seen
        .iter()
        .filter(|seen| seen.what == What::Arrived)
        .count() as u64;
    assert_eq!(stats.delivered, arrived, "{stats:?}");
}

/// What a run of the jittered world from `seed` saw in slices ending at each of `deadlines`: every
/// event, and the fabric's counts. Each slice's events fall within it.
fn sliced(seed: u64, deadlines: &[u64]) -> (Vec<Seen>, SimFabricStats) {
    let mut sim = SimRuntime::new(&config(), seed).unwrap();
    sim.set_path(SimPath::in_order(DELAY_NS, JITTER_NS));
    let events = world(&mut sim);
    let mut seen = Vec::new();
    let mut began = 0;
    for &deadline in deadlines {
        sim.run_until(deadline);
        assert_eq!(
            sim.now_ns(),
            deadline,
            "the clock is left at the slice's end"
        );
        for event in events.try_iter() {
            assert!(
                began < event.at && event.at <= deadline,
                "{event:?} taken in the slice ({began}, {deadline}]"
            );
            seen.push(event);
        }
        began = deadline;
    }
    (seen, sim.stats())
}

/// Do: run the world on a path of 5 ms ± 2.5 ms, its jitter drawn from the seed, for 40 ms: in one slice,
/// in slices of 2 ms, and in slices of 2 ms again; then from another seed. Expect: each slice takes only
/// events within it; the runs in slices see what the run in one slice saw, every event at the same time
/// and in the same order, with the same fabric counts; one seed gives one run; another seed's draws give
/// another.
#[test]
fn a_run_in_slices_is_the_run_in_one_and_one_seed_gives_one_run() {
    let end = SLICES * SLICE_NS;
    let slices: Vec<u64> = (1..=SLICES).map(|slice| slice * SLICE_NS).collect();
    let (whole, whole_stats) = sliced(7, &[end]);
    let (first, first_stats) = sliced(7, &slices);
    let (second, second_stats) = sliced(7, &slices);
    assert_eq!(first, whole, "the slices took what one slice took");
    assert_eq!(first_stats, whole_stats);
    assert_eq!(second, first, "one seed, one run");
    assert_eq!(second_stats, first_stats);
    let sent = whole.iter().filter(|seen| seen.what == What::Sent).count() as u64;
    let arrived = whole
        .iter()
        .filter(|seen| seen.what == What::Arrived)
        .count() as u64;
    assert_eq!(sent, end / PERIOD_NS);
    assert!(
        arrived < sent,
        "datagrams were in flight at the end: {arrived} of {sent}"
    );
    assert_eq!(whole_stats.delivered, arrived, "{whole_stats:?}");
    let (other, _) = sliced(8, &slices);
    assert_ne!(other, first, "another seed draws other delays");
}
