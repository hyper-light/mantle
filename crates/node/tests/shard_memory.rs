//! What the node's runtime holds beside its threads' stacks (`derive::runtime_bytes`, node.md
//! §1.4): each shard's task slots, timers, readiness waiters and control queue, hyper-rt sizing
//! every shard alike for the first's role, and the S3 listener's lane to each shard past the
//! first. Counted by hyper-measure's counting allocator: a runtime across the process's threads as
//! `Runtime::start` returns, each shard having built what it holds on its own thread before it
//! does, the count read with nothing else running; the lanes on the listener's thread around
//! `Listener::spread`, which makes each there. Each figure is the difference between counts, and
//! the floor's statement is checked against the whole runtime at every shape of a sweep of shards,
//! connections, peers and control entries.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::mpsc::sync_channel;
use std::task::Waker;

use hyper_measure::alloc;
use hyper_rt::coop::Class;
use hyper_rt::runtime::{Runtime, RuntimeConfig, WakeTracking};
use mantle_node::derive::{self, ShardShape};
use mantle_node::listener::{Listener, Settings};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// A process count sees every thread: the tests run one at a time.
static ALONE: Mutex<()> = Mutex::new(());

/// A runtime of `shards` shards of `shape` with `ring` control entries and a batch of as many
/// polls, the most the calibration gives one (hyper-rt `RuntimeConfig::calibrate_batch`), as the
/// node's runtime is made (`process::runtime`).
fn config(shards: u16, shape: &ShardShape, ring: usize) -> RuntimeConfig {
    RuntimeConfig {
        shards,
        tasks_per_shard: shape.tasks,
        timers_per_shard: shape.timers,
        interests_per_shard: shape.interests,
        ring_entries: ring,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: ring,
        pin: false,
        cores: Vec::new(),
        page_bytes: usize::try_from(hyper_rt::machine::facts::Facts::query().page.base).unwrap(),
        spin_ns: 0,
        wake_tracking: Some(WakeTracking {
            prior_ns: 10_000,
            shift: 4,
            idle_ratio: 1,
        }),
        latency_objective_ns: None,
    }
}

/// The bytes a runtime of `config` holds once started, past what the process held before.
fn started(config: &RuntimeConfig) -> u64 {
    alloc::begin_process();
    let rt = Runtime::start(config).unwrap();
    let live = alloc::end_process().live;
    rt.shutdown().unwrap();
    u64::try_from(live).unwrap()
}

/// The bytes the listener's thread holds for its lanes to the shards of `rt` past the first, the
/// listener serving `served` connections.
fn lanes(rt: &Runtime, served: u64) -> u64 {
    let first = rt.shard_ids()[0];
    let holders: Vec<_> = rt.shard_ids()[1..]
        .iter()
        .map(|shard| rt.holder_of(*shard).unwrap())
        .collect();
    let (told, heard) = sync_channel(1);
    rt.spawn_on(first, async move {
        let (_doorway, door) = mantle_node::door::doorway(1, Waker::noop().clone()).unwrap();
        let mut listener = Listener::bind(
            Settings {
                listen: SocketAddr::from(([127, 0, 0, 1], 0)),
                region: "us-east-1".into(),
                domains: Vec::new(),
                access: Vec::new(),
                tls: None,
                node: 1,
                connections: served,
            },
            door,
        )
        .unwrap();
        alloc::begin();
        listener.spread(&holders).unwrap();
        let live = alloc::end().live;
        told.try_send(live).unwrap();
    })
    .unwrap();
    u64::try_from(heard.recv().unwrap()).unwrap()
}

/// What a shard of `shape` makes as a task on it first takes `class`, past what a Normal task
/// spawned alike makes, counted on the shard's thread around the spawn: the queue's classes
/// (hyper-rt `queue::Kept`) and the class's lists, the node's task being Critical on the first
/// shard (`process::run_on_first_shard`).
fn classed(shape: &ShardShape, class: Class) -> u64 {
    let spawned = |class| {
        let rt = Runtime::start(&config(1, shape, 4)).unwrap();
        let (told, heard) = sync_channel(1);
        rt.spawn_on(rt.shard_ids()[0], async move {
            alloc::begin();
            hyper_rt::futures::spawn_detached_as(class, std::future::pending::<()>()).unwrap();
            let live = alloc::end().live;
            told.try_send(live).unwrap();
        })
        .unwrap();
        let live = heard.recv().unwrap();
        rt.shutdown().unwrap();
        live
    };
    u64::try_from(spawned(class) - spawned(Class::Normal)).unwrap()
}

/// Each of the runtime's figures is what the running code holds for it: what one more shard, task
/// slot, timer, waiter and control entry adds, and what a lane holds and adds a connection, each
/// counted apart from the rest, no more than `derive` states it.
#[test]
fn each_figure_of_the_runtime_holds_what_it_counts() {
    let _alone = ALONE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let base = ShardShape {
        tasks: 4,
        timers: 4,
        interests: 8,
    };
    let at = |shards: u16, shape: ShardShape, ring: usize| started(&config(shards, &shape, ring));
    // Once, for what a process makes at its first runtime and keeps.
    at(1, base, 4);
    let one = at(1, base, 4);
    let shard = at(3, base, 4) - one;
    assert_eq!(shard % 2, 0, "{shard}");
    let shard = shard / 2;
    assert_eq!(at(5, base, 4) - one, 4 * shard);
    let task = at(
        1,
        ShardShape {
            tasks: 1028,
            ..base
        },
        4,
    ) - one;
    let timer = at(
        1,
        ShardShape {
            timers: 1028,
            ..base
        },
        4,
    ) - one;
    // At powers of two the waiters' table is twice the waiters, 2,048 more slots past 1,024.
    let waiters = at(
        1,
        ShardShape {
            interests: 1024,
            ..base
        },
        4,
    ) - one;
    let slots = at(
        1,
        ShardShape {
            interests: 1025,
            ..base
        },
        4,
    ) - one
        - waiters;
    let ring = at(1, base, 1028) - one;
    eprintln!(
        "one shard {one}, each more {shard}; 1,024 tasks more {task}, timers {timer}, \
         1,016 waiters {waiters}, the 1,025th waiter and its slots {slots}, entries {ring}"
    );
    assert!(one - shard <= derive::RUNTIME_BYTES, "{one} {shard}");
    assert!(task <= 1024 * derive::SHARD_TASK_BYTES, "{task}");
    assert!(timer <= 1024 * derive::SHARD_TIMER_BYTES, "{timer}");
    assert!(ring <= 1024 * derive::SHARD_RING_BYTES, "{ring}");
    // 1,016 waiters past eight and their table's 2,032 slots past 16.
    assert!(
        waiters <= 1016 * derive::SHARD_INTEREST_BYTES + 2032 * derive::SHARD_INTEREST_SLOT_BYTES,
        "{waiters}"
    );
    assert!(
        slots <= derive::SHARD_INTEREST_BYTES + 2048 * derive::SHARD_INTEREST_SLOT_BYTES,
        "{slots}"
    );
    // The shard's fixed bytes, what remains of it at the figures above.
    let counted = 4 * derive::SHARD_TASK_BYTES
        + 4 * derive::SHARD_TIMER_BYTES
        + 8 * derive::SHARD_INTEREST_BYTES
        + 16 * derive::SHARD_INTEREST_SLOT_BYTES
        + 4 * derive::SHARD_RING_BYTES;
    assert!(shard <= derive::SHARD_BYTES + counted, "{shard}");

    // The node's task Critical on the first shard: the queue's classes and Critical's lists.
    let critical = classed(&base, Class::Critical);
    let critical_slots = classed(
        &ShardShape {
            tasks: 1028,
            ..base
        },
        Class::Critical,
    ) - critical;
    eprintln!("Critical at four tasks {critical}; 1,024 tasks more {critical_slots}");
    assert!(
        critical_slots <= 1024 * derive::CRITICAL_SLOT_BYTES,
        "{critical_slots}"
    );
    assert!(
        critical <= derive::CRITICAL_BYTES + 4 * derive::CRITICAL_SLOT_BYTES,
        "{critical}"
    );

    // A lane: its own and its connections', one for each the listener holds at once.
    let rt = Runtime::start(&config(3, &base, 4)).unwrap();
    let one_lane = |served| {
        let both = lanes(&rt, served);
        assert_eq!(both % 2, 0, "{both}");
        both / 2
    };
    let lane = one_lane(1);
    let connections = one_lane(1001) - lane;
    rt.shutdown().unwrap();
    eprintln!("a lane at one connection served {lane}; 1,000 more {connections}");
    assert!(
        connections <= 1000 * derive::LANE_CONNECTION_BYTES,
        "{connections}"
    );
    let places = mantle_node::listener::tasks(1);
    assert!(
        lane <= derive::LANE_BYTES + places * derive::LANE_CONNECTION_BYTES,
        "{lane}"
    );
}

/// The floor's runtime holds the node's runtime whole: built as the node builds it at a sweep of
/// shards, S3 connections, peers and control entries, its shards counted as `Runtime::start`
/// returns and the listener's lanes to the shards past the first as `Listener::spread` makes them,
/// none past `derive::runtime_bytes`. A floor that undercounts a shard, a lane or a connection
/// fails here at the shape it undercounts.
#[test]
fn the_floor_holds_the_nodes_runtime_at_every_shape() {
    let _alone = ALONE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // Once, for what a process makes at its first runtime and keeps.
    let warm = derive::shard_shape(Some(1), 2).unwrap();
    started(&config(1, &warm, 1));
    for shards in [1u16, 2, 4, 10] {
        for listening in [None, Some(1), Some(10), Some(256)] {
            for peers in [2, 8] {
                for ring in [1, 16, 1024] {
                    let shape = derive::shard_shape(listening, peers).unwrap();
                    let runtime = config(shards, &shape, ring);
                    // The node's task Critical on the first shard.
                    let mut held = started(&runtime) + classed(&shape, Class::Critical);
                    if let Some(served) = listening.filter(|_| shards > 1) {
                        let rt = Runtime::start(&runtime).unwrap();
                        held += lanes(&rt, served);
                        rt.shutdown().unwrap();
                    }
                    let stated =
                        derive::runtime_bytes(usize::from(shards), &shape, ring, listening)
                            .unwrap();
                    eprintln!(
                        "{shards} shards, {listening:?} served, {peers} peers, {ring} entries: \
                         held {held} of {stated}"
                    );
                    assert!(
                        held <= stated,
                        "{shards} shards, {listening:?} served, {peers} peers, {ring} entries: \
                         held {held}, past {stated}"
                    );
                }
            }
        }
    }
}
