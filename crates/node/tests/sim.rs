//! Deterministic simulation of a cell of nodes (docs/design/node.md §9.1, `support/cell.rs`):
//! three nodes or five (odd seeds three, even seeds five) and three clients.
//!
//! A run is its seed. The cell's paths are one of the named profiles of focal's path model (one
//! switch, one continent, across the planet: hyper-sim's `Path::LAN`, `REGIONAL`, `GEOGRAPHIC`).
//! Three clients put and read two keys through the range's leader. Half way through their
//! operations the leader or a follower, as the seed draws, is cut off from the other nodes (every
//! datagram between them lost, both ways), loses power (its device keeping what it made durable, its
//! engine what it persisted), or has its device fail a write (its log fenced, so it stops, as its
//! process would); three quarters through it is joined again or started again. Through that
//! window, on some seeds, every datagram on every path may be lost. After every run: each key's history
//! is linearizable (Wing and Gong's search with Lowe's memo, `crates/range/tests/support/linear.rs`);
//! every operation completed; every node applied as far as the others; and the seed run again gives
//! the same histories, the same ends and the same traffic, operation by operation.
//!
//! `MANTLE_NODE_SIM_SEEDS` sets how many runs, and `MANTLE_NODE_SIM_SEED` where they begin.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_block::sim::Fault as DeviceFault;
use hyper_rt::sim::SimPath;
use hyper_sim::rng::{Seeded, stream_seed};
use mantle_meta::wire::Answer;

#[allow(dead_code)]
#[path = "support/cell.rs"]
mod cell;
#[allow(dead_code)]
#[path = "support/history.rs"]
mod history;
#[path = "../../range/tests/support/linear.rs"]
mod linear;
#[path = "../../range/tests/support/store.rs"]
mod store;
use cell::{CLIENT, CLIENTS, GROUP, PROFILES, Sim, log_config};
use history::{Part, check_histories, registration};

/// The seeds run when `MANTLE_NODE_SIM_SEEDS` is not set: one cell of three and one of five, each
/// run twice.
const SEEDS: u64 = 2;

/// What befalls a node half way through the clients' operations.
#[derive(Clone, Copy, Debug)]
enum Fault {
    /// Cut off from the other nodes, and joined to them again three quarters through.
    Cut,
    /// Its power cut, and started again three quarters through.
    Crash,
    /// Its device fails a write, which fences its log; it stops on it, and is started again three
    /// quarters through.
    Disk,
}

/// One run of `seed`: its histories and every node's applied index at the end.
fn run(seed: u64) -> (Vec<(u64, Part)>, u64) {
    let mut cell = Sim::new(seed);
    let leader = cell.leader();
    cell.open_bucket();
    // Puts enough to pass the log group's room in all, so the history crosses compactions.
    let ops = (2 * (log_config().group_entries + 1)).div_ceil(CLIENTS);
    let stops = cell.start_workload(ops);
    cell.done_by(ops, 2);
    // What befalls whom, and whether datagrams are lost meanwhile: the run's draws.
    let mut draws = Seeded::new(stream_seed(seed, "faults", &[]));
    let fault = [Fault::Cut, Fault::Crash, Fault::Disk][usize::try_from(draws.below(3)).unwrap()];
    let followers: Vec<u64> = (1..=cell.nodes).filter(|id| *id != leader).collect();
    let struck = match draws.below(2) {
        0 => leader,
        _ => {
            followers
                [usize::try_from(draws.below(u64::try_from(followers.len()).unwrap())).unwrap()]
        }
    };
    let lossy = draws.below(2) == 1;
    eprintln!("seed {seed}: leader {leader}, {fault:?} on {struck} half way, lossy {lossy}");
    cell.lossy(lossy);
    match fault {
        Fault::Cut => {
            cell.isolate(struck, true);
            cell.done_by(ops, 3);
            cell.isolate(struck, false);
        }
        Fault::Crash => {
            cell.crash(struck);
            cell.done_by(ops, 3);
            cell.restart(struck);
        }
        Fault::Disk => {
            cell.fail_device(struck, DeviceFault::WriteError);
            cell.done_by(ops, 3);
            cell.restart(struck);
        }
    }
    cell.lossy(false);
    cell.done_by(ops, 4);
    drop(stops);
    cell.until("every client's part", |c| {
        if c.parts.len() == usize::try_from(CLIENTS).unwrap() {
            Some(())
        } else {
            None
        }
    });
    let parts: Vec<(u64, Part)> = std::mem::take(&mut cell.parts).into_iter().collect();
    check_histories(parts.iter().flat_map(|(_, part)| part.iter().cloned()));
    let applied = cell.converged();
    (parts, applied)
}

#[test]
fn a_cells_histories_through_cuts_power_cuts_and_device_faults_are_linearizable_and_a_seed_replays()
{
    let seeds = std::env::var("MANTLE_NODE_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(SEEDS);
    let first = std::env::var("MANTLE_NODE_SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1u64);
    for seed in first..first + seeds {
        let once = run(seed);
        let again = run(seed);
        assert_eq!(
            format!("{:?}", once),
            format!("{:?}", again),
            "seed {seed} ran otherwise the second time"
        );
    }
}

/// A registration whose answer takes longer than every exchange its client measured is applied
/// once. Asked again, a registration registers again (the Raft dissertation §6.3), so the client
/// cuts its exchange by no period and waits for the leader's answer (`mantle_node::client`); a
/// client that cut it and asked again would leave a session registered for nothing each time, and
/// a range holding its bound of sessions refuses every registration after.
#[test]
fn a_registration_slower_than_every_exchange_its_client_measured_is_applied_once() {
    // A cell of three, one switch apart.
    let mut cell = Sim::new(1);
    cell.leader();
    let order: Vec<u64> = (1..=cell.nodes).collect();
    let asking = cell.asking(CLIENT, &order);
    let (first, _) = cell.ask(&asking, GROUP, registration(1));
    assert!(
        matches!(first.as_deref(), Ok([Answer::Registered { .. }])),
        "the first registration answered {first:?}"
    );
    let before = cell.converged();
    // From now on the client's paths cross the planet, and the nodes' stay one switch apart: the
    // registration's answer takes longer than every exchange the client measured.
    let (_, one_way, jitter) = PROFILES[2];
    cell.slow_clients(SimPath::in_order(one_way, jitter));
    asking
        .try_send((GROUP, registration(2)))
        .map_err(|_| "the client's asks are full")
        .unwrap();
    let (second, asks) = cell.until("the second registration's answer", |c| {
        for (id, member) in &c.members {
            let applied = member.status.as_ref().map_or(0, |s| s.applied);
            assert!(
                applied <= before + 1,
                "node {id} applied to {applied}, past {before} and the one registration asked: \
                 the registration was asked again"
            );
        }
        c.asked.take()
    });
    assert!(
        matches!(second.as_deref(), Ok([Answer::Registered { .. }])),
        "the second registration answered {second:?}"
    );
    assert_eq!(asks.lost, 0, "an exchange was lost: {asks:?}");
    assert_eq!(cell.converged(), before + 1);
}
