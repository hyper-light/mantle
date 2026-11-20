//! Deterministic simulation of a cell of nodes (docs/design/node.md §9.1, `support/cell.rs`):
//! three nodes or five (odd seeds three, even seeds five), or one for a tear on a one-voter group,
//! or one voter or two and a spare for a change of the range's voters, and three clients.
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

use std::collections::BTreeMap;
use std::time::Duration;

use hyper_block::sim::Fault as DeviceFault;
use hyper_log::{Start, Tail};
use hyper_rt::sim::SimPath;
use hyper_sim::rng::{Seeded, stream_seed};
use mantle_meta::name;
use mantle_meta::wire::{Answer, Entry, Sessioned};
use mantle_node::ask::CommandOutcome;

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
use cell::{CLIENT, CLIENTS, GROUP, PROFILES, SETTINGS, Sim, log_config};
use history::{Part, check_histories, put, registration};
use hyper_log::Config as LogConfig;

/// The seeds run when `MANTLE_NODE_SIM_SEEDS` is not set: one cell of three and one of five, each
/// run twice.
const SEEDS: u64 = 2;

/// The seeds the power-cut tests run, a cut at any moment and a cut that tears a frame after its
/// record lands: each cell size on every profile of paths (`cell::PROFILES`, seeds 1 to 6, odd
/// seeds three nodes, even five), and the first two again.
const CUT_SEEDS: u64 = 8;

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

/// The seeds a test runs: `MANTLE_NODE_SIM_SEEDS` of them from `MANTLE_NODE_SIM_SEED` (1 where
/// unset) where the first is set, else `default`.
fn seeds(default: &[u64]) -> Vec<u64> {
    let count = std::env::var("MANTLE_NODE_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok());
    let first = std::env::var("MANTLE_NODE_SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1u64);
    match count {
        Some(count) => (first..first + count).collect(),
        None => default.to_vec(),
    }
}

/// A log of three segments, hyper-log's least, its groups bound only by what the file holds: the
/// clients' puts fill the file again and again, so the ranges' writes are refused for room
/// (`LogError::Full`), and a power cut finds it full of records its engine applied and did not yet
/// make durable. Here the log once answered `Full` for good after its groups compacted, a leader's
/// range held still with nothing left to wait for, on 41 of seeds 1-100
/// (docs/measurements/2026-11-14-log-full-wedge.md); the test ran four segments meanwhile.
fn tight_log() -> LogConfig {
    LogConfig {
        max_segments: 3,
        group_entries: 1 << 20,
        group_bytes: 1 << 30,
        ..log_config()
    }
}

/// A node whose log fills with live records goes on: a range's write the log refuses for room
/// waits for the ranges to compact, and a follower's power is cut at the turn its compaction
/// starts, its log out of room, and it is started again on what its device kept. No node stops,
/// the histories are linearizable and every node converges. A stream's write refused for room is
/// `a_liveness_write_the_log_refuses_for_room_is_owed_and_the_node_goes_on`.
#[test]
fn a_node_whose_log_fills_with_live_records_waits_for_its_ranges_to_compact_and_starts_again() {
    // Seed 10 fenced a node before a compaction waited for the writes out (the measurement), and
    // seeds 7, 10 and 11 wedged a leader's log at three segments (2026-11-14's).
    for seed in seeds(&[1, 2, 7, 10, 11]) {
        let mut cell = Sim::with(seed, SETTINGS, tight_log());
        let leader = cell.leader();
        cell.open_bucket();
        // Puts enough to fill the file many times over on every path: a slower path batches more
        // entries to a frame, so the file fills later.
        let ops = (8 * (log_config().group_entries + 1)).div_ceil(CLIENTS);
        let stops = cell.start_workload(ops);
        let struck = (1..=cell.nodes).find(|id| *id != leader).unwrap();
        // The follower's log out of room: a write refused, its compaction started at that turn
        // (`Writes::starts`) and not yet durable when its power is cut.
        let started = |c: &Sim| c.members[&struck].status.map_or(0, |s| s.writes.starts);
        cell.until("the follower's status", |c| {
            c.members[&struck].status.map(|_| ())
        });
        let before = started(&cell);
        cell.until("the follower's log out of room", |c| {
            if started(c) > before { Some(()) } else { None }
        });
        cell.crash(struck);
        cell.done_by(ops, 2);
        cell.restart(struck);
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
        cell.converged();
    }
}

/// A liveness write the log refuses for room is owed, not fatal (node.md §6.2): in the KIND cell on
/// 2026-11-07 a founder whose log was full when its host stopped started 22 times into "the node's
/// log: the log is full of live records". Here the leader's log and a follower's refuse their
/// stream's next writes whatever they hold, more than one so that an owed write asked again is
/// refused as well. No node stops (the cell fails on any it did not arm), each counts its
/// refusals, the histories are linearizable and every node converges.
#[test]
fn a_liveness_write_the_log_refuses_for_room_is_owed_and_the_node_goes_on() {
    const REFUSED: u64 = 3;
    for seed in [1u64, 2] {
        let mut cell = Sim::with(seed, SETTINGS, log_config());
        let leader = cell.leader();
        let follower = (1..=cell.nodes).find(|id| *id != leader).unwrap();
        cell.open_bucket();
        cell.refuse_stream_writes(leader, REFUSED);
        cell.refuse_stream_writes(follower, REFUSED);
        let ops = log_config().group_entries.div_ceil(CLIENTS);
        let stops = cell.start_workload(ops);
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
        cell.until("each armed node's refusals counted", |c| {
            let counted = [leader, follower].iter().all(|id| {
                c.members[id]
                    .status
                    .as_ref()
                    .is_some_and(|s| s.liveness_waits == REFUSED)
            });
            if counted { Some(()) } else { None }
        });
        cell.converged();
    }
}

/// A node idle when its log refused its liveness write for room proves itself again (node.md §2.4,
/// item 5). Its ranges neither write nor apply, so nothing the node waited on before moved, and its
/// stream, which asks no other proof while one is owed, held its heartbeats for as long as it
/// idled: the owed write is asked again a sending interval of the stream's after each refusal. Here
/// a follower's log refuses its stream's next writes whatever it holds, more than one so that a
/// write asked again is refused as well; the follower counts each, then takes its proof within the
/// patience it states, the cell idle throughout.
#[test]
fn an_idle_node_whose_liveness_write_the_log_refused_proves_itself_again() {
    const REFUSED: u64 = 2;
    for seed in seeds(&[1, 2]) {
        let mut cell = Sim::new(seed);
        let leader = cell.leader();
        let struck = (1..=cell.nodes).find(|id| *id != leader).unwrap();
        let applied = cell.converged();
        let status = |c: &Sim| c.members[&struck].status.unwrap();
        // Its patience is stated once its stream judges its peers and its range has timing.
        let stated = cell.until("the follower's patience stated", |c| status(c).patience);
        cell.refuse_stream_writes(struck, REFUSED);
        let (refused, writes) = cell.until("the follower's refusals counted", |c| {
            (status(c).liveness_waits == REFUSED).then(|| (c.now_ns(), status(c).liveness_writes))
        });
        let proved = cell.until("the follower's proof after its last refusal", |c| {
            (status(c).liveness_writes > writes).then(|| c.now_ns())
        });
        let (took, bound) = (proved - refused, u64::try_from(stated.as_nanos()).unwrap());
        eprintln!(
            "seed {seed}: the follower took its proof {took} ns after its last refusal, within the \
             patience of {bound} ns it stated"
        );
        assert!(
            took <= bound,
            "seed {seed}: the follower took its proof {took} ns after its last refusal, past the \
             patience of {bound} ns it stated"
        );
        assert_eq!(
            cell.converged(),
            applied,
            "seed {seed}: the cell applied while it idled"
        );
    }
}

#[test]
fn a_cells_histories_through_cuts_power_cuts_and_device_faults_are_linearizable_and_a_seed_replays()
{
    for seed in seeds(&(1..=SEEDS).collect::<Vec<_>>()) {
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
/// once. Asked again, a registration is applied again (the Raft dissertation §6.3): answered the
/// session it opened (`mantle_meta::session::register`), but an entry in the log each time, so the
/// client cuts its exchange by no period and waits for the leader's answer
/// (`mantle_node::client`).
#[test]
fn a_registration_slower_than_every_exchange_its_client_measured_is_applied_once() {
    // A cell of three, one switch apart.
    let mut cell = Sim::new(1);
    cell.leader();
    let order: Vec<u64> = (1..=cell.nodes).collect();
    let asking = cell.asking(CLIENT, &order);
    let (first, _) = cell.ask(&asking, GROUP, registration(CLIENT, 1, 1));
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
        .try_send((GROUP, registration(CLIENT, 1, 2)))
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

/// A client left idle between its commands for longer than its connections' idle timeout, as a
/// caller between its calls leaves one (it runs no task of its own): its nodes close their
/// connections to it without a word (RFC 9000 §10.1). Its next command goes on a connection dialed
/// anew, answered with no exchange lost, never first on the one its node closed (the serve suite's
/// S3 operations through every node, `Lost(Closed)`, on a Mac at load 46 to 140, 2026-11-12).
#[test]
fn a_client_idle_past_its_connections_idle_timeout_asks_its_next_command_on_one_dialed_anew() {
    let mut cell = Sim::new(1);
    cell.leader();
    let order: Vec<u64> = (1..=cell.nodes).collect();
    let asking = cell.asking(CLIENT, &order);
    let (first, asked) = cell.ask(&asking, GROUP, registration(CLIENT, 1, 1));
    assert!(
        matches!(first.as_deref(), Ok([Answer::Registered { .. }])),
        "the first registration answered {first:?}"
    );
    // Four idle timeouts pass on the simulation's clock with the client asking nothing.
    let idle = u64::try_from(mantle_protocol::bounds::idle_timeout().as_nanos()).unwrap();
    cell.run_for(4 * idle);
    let (second, asks) = cell.ask(&asking, GROUP, registration(CLIENT, 1, 2));
    assert!(
        matches!(second.as_deref(), Ok([Answer::Registered { .. }])),
        "the second registration answered {second:?}"
    );
    assert_eq!(asks.lost, asked.lost, "an exchange was lost: {asks:?}");
}

/// Runs until node `leader` names a leader other than itself.
fn stepped_down(cell: &mut Sim, leader: u64) {
    cell.until("the leader steps down", |c| {
        c.members[&leader]
            .status
            .filter(|s| s.leader != leader)
            .map(|_| ())
    });
}

/// Runs until the nodes but `deaf` name one leader other than `deaf`, in a term after `term`.
fn others_elect(cell: &mut Sim, deaf: u64, term: u64) {
    cell.until("the others elect one of themselves", |c| {
        let named: Vec<(u64, u64)> = c
            .members
            .iter()
            .filter(|(id, _)| **id != deaf)
            .map(|(_, m)| m.status.map(|s| (s.term, s.leader)))
            .collect::<Option<_>>()?;
        let first = *named.first()?;
        let elected =
            first.0 > term && ![0, deaf].contains(&first.1) && named.iter().all(|n| *n == first);
        if elected { Some(()) } else { None }
    });
}

/// A leader whose plane stops hearing its followers once it has judged them suspects them all and
/// steps down, with no voter it trusts to hand its term over to, while its followers, whose planes
/// hear it, trust it still (its node lives). Once it hears them again, the cell elects a leader.
#[test]
fn a_leader_that_hears_no_follower_for_a_while_steps_down_and_the_cell_elects_again() {
    let mut cell = Sim::new(1);
    let leader = cell.leader();
    cell.judged();
    cell.deafen_plane(leader, true);
    stepped_down(&mut cell, leader);
    cell.deafen_plane(leader, false);
    cell.leader();
}

/// As above, but the leader stays deaf: the others, which still trust it, elect one of themselves
/// in a later term, which the deaf node follows by its lanes.
#[test]
fn a_leader_that_hears_no_follower_steps_down_and_the_others_elect_one_of_themselves() {
    let mut cell = Sim::new(1);
    let leader = cell.leader();
    cell.judged();
    let term = cell.members[&leader].status.unwrap().term;
    cell.deafen_plane(leader, true);
    stepped_down(&mut cell, leader);
    others_elect(&mut cell, leader, term);
    cell.deafen_plane(leader, false);
    cell.leader();
}

/// A leader whose plane stops hearing its followers before it has judged them, with no evidence
/// of its own to judge them by, steps down all the same, and the others elect one of themselves.
#[test]
fn a_leader_deaf_before_it_judges_its_links_steps_down_and_the_others_elect_one_of_themselves() {
    let mut cell = Sim::new(1);
    let leader = cell.leader();
    let status = cell.members[&leader].status.unwrap();
    assert!(
        !status.judged,
        "the leader judged its links when it was elected, which this test is not about"
    );
    cell.deafen_plane(leader, true);
    stepped_down(&mut cell, leader);
    others_elect(&mut cell, leader, status.term);
    cell.deafen_plane(leader, false);
    cell.leader();
}

/// The leader of a cell of five voters loses power half way through the clients' operations, its
/// log full of their puts, and is started again three quarters through, as the end-to-end suite's
/// leader of five voters killed and started again is. The other four elect one of themselves in a
/// later term and the clients go on through it, each wait within the cell's quiet period; the node
/// started again comes back a follower and keeps no one from leading. Every history is
/// linearizable and every node, the one started again among them, applies as far as the others.
/// Seeds 2, 4 and 6: a cell of five on each profile of paths.
#[test]
fn a_leader_of_five_voters_whose_power_is_cut_under_load_is_succeeded_and_started_again_follows() {
    for seed in seeds(&[2, 4, 6]) {
        leader_restarted(seed);
    }
}

fn leader_restarted(seed: u64) {
    let mut cell = Sim::new(seed);
    assert_eq!(cell.nodes, 5, "seed {seed} makes no cell of five");
    let leader = cell.leader();
    cell.open_bucket();
    let ops = (2 * (log_config().group_entries + 1)).div_ceil(CLIENTS);
    let stops = cell.start_workload(ops);
    cell.done_by(ops, 2);
    let term = cell.members[&leader].status.unwrap().term;
    cell.crash(leader);
    others_elect(&mut cell, leader, term);
    cell.done_by(ops, 3);
    cell.restart(leader);
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
    cell.converged();
}

/// The commands an entry of puts carries, as a node's entry carries the commands its operations
/// wait on, as many as fit its bytes: four, which the simulated range's entry of 1 KiB holds.
const BATCH: u64 = 4;

/// A batched entry is answered only once it is durable on a quorum (CLAUDE.md §6): a client asks
/// an entry of [`BATCH`] puts, and while it is out every node's power is cut at once, its device
/// keeping only what it made durable, at a moment the seed draws, from as the entry is sent to past
/// its answer. Started again, the cell applies at least as far as it had when the client held its
/// answer, and the entry asked again is answered the same versions from its session, applied once;
/// where the cut came before the answer, the entry asked again is applied or answered, never lost
/// halfway (each of its puts answered alike). Were a node to answer before its log's flush, or the
/// leader before a quorum's, a cut at the answer would leave the cell short of it.
#[test]
fn a_batched_entry_is_answered_only_once_durable_and_a_power_cut_at_any_moment_keeps_it_whole() {
    for seed in 1..=CUT_SEEDS {
        let mut cell = Sim::new(seed);
        cell.leader();
        cell.open_bucket();
        let order: Vec<u64> = (1..=cell.nodes).collect();
        let asking = cell.asking(CLIENT, &order);
        let (registered, _) = cell.ask(&asking, GROUP, registration(CLIENT, 1, 1));
        let session = match registered.as_deref() {
            Ok([Answer::Registered { session }]) => *session,
            other => panic!("seed {seed}: the registration answered {other:?}"),
        };
        let entry = Entry {
            at_ns: 0,
            commands: (1..=BATCH)
                .map(|serial| Sessioned {
                    session,
                    serial,
                    unanswered: 1,
                    command: put(&format!("{serial}"), "e", u128::from(serial)),
                })
                .collect(),
        };
        asking
            .try_send((GROUP, entry.clone()))
            .map_err(|_| "the client's asks are full")
            .unwrap();
        // The cut's moment: up to a few of the LAN path's round trips past the send.
        let mut draws = Seeded::new(stream_seed(seed, "cut", &[]));
        cell.run_for(draws.below(4_000_000));
        let answered = cell.asked.take();
        let applied = cell
            .members
            .values()
            .filter_map(|m| m.status.as_ref().map(|s| s.applied))
            .max()
            .unwrap();
        for id in 1..=cell.nodes {
            cell.crash(id);
        }
        for id in 1..=cell.nodes {
            cell.restart(id);
        }
        cell.leader();
        // The nodes listen anew; a second client of the test's, which knows where, asks again. An
        // entry unanswered at the cut has no answer its first client holds.
        let early = answered.is_some();
        let first =
            answered.map(|(answers, _)| answers.unwrap_or_else(|why| panic!("seed {seed}: {why}")));
        drop(asking);
        let asking = cell.asking(CLIENT + 1, &order);
        if early {
            // The answered entry's index is what a new leader's first entry commits again: every
            // node applies through it, which a lost entry never lets it.
            cell.until("every node applies through the answered entry", |c| {
                let through = c
                    .members
                    .values()
                    .all(|m| m.status.as_ref().is_some_and(|s| s.applied >= applied));
                if through { Some(()) } else { None }
            });
        }
        // Asked again, the entry is applied once: answered from its session where it applied.
        let (again, _) = cell.ask(&asking, GROUP, entry);
        let again = again.unwrap_or_else(|why| panic!("seed {seed}: asked again: {why}"));
        assert_eq!(
            again.len(),
            usize::try_from(BATCH).unwrap(),
            "seed {seed}: {again:?}"
        );
        assert!(
            again
                .iter()
                .all(|a| matches!(a, Answer::Name(name::Outcome::Put { .. }))),
            "seed {seed}: asked again it answered {again:?}"
        );
        if let Some(first) = first {
            assert_eq!(
                first, again,
                "seed {seed}: an answered entry was applied again"
            );
        }
        assert!(
            cell.members.values().all(|m| m.status.is_some()),
            "seed {seed}: a node did not come back"
        );
    }
}

/// An entry of [`BATCH`] puts from `first`, its session's serials `first..first + BATCH`, the
/// session holding no answer the client has not read below `first`, each key tagged `tag`.
fn batch(session: u64, first: u64, tag: &str) -> Entry {
    Entry {
        at_ns: 0,
        commands: (first..first + BATCH)
            .map(|serial| Sessioned {
                session,
                serial,
                unanswered: first,
                command: put(&format!("{tag}{serial}"), "e", u128::from(serial)),
            })
            .collect(),
    }
}

/// Whether `answered` is a put's answer for every command of a [`BATCH`].
fn batch_answered(answered: &[Answer]) -> bool {
    answered.len() == usize::try_from(BATCH).unwrap()
        && answered
            .iter()
            .all(|a| matches!(a, Answer::Name(name::Outcome::Put { .. })))
}

/// An entry at the range's bound on an entry, and each within an entry's fixed bytes of it, is
/// sent, committed on every node and answered: the bound the nodes state on a Raft frame holds the
/// largest append the core sends, one entry of the range's largest with its fixed bytes, alone
/// (`mantle_range::largest_message`). The bound once held a page of entries or an entry's own
/// bytes and not its fixed bytes (`ENTRY_FIXED_BYTES`, 25): with the simulated range's page and
/// entry both 1 KiB, an entry of 1,000 to 1,024 bytes went alone in an append whose frame took
/// 1,149 to 1,173 bytes, past the bound of 1,148. The transport refused it before it left
/// (`Refusal::FrameBound`), the node dropped it as the network drops a message, and the leader
/// sent it again and again, the range's commit frozen before it for good. A node packs the commands
/// waiting into one entry while they fit the range's bound (`ask::Asking::another_entry`), so a
/// burst makes such entries, as one command of the range's largest does.
#[test]
fn an_entry_at_the_ranges_bound_is_sent_and_commits_on_every_node() {
    for seed in seeds(&[1, 2, 7]) {
        let mut cell = Sim::new(seed);
        cell.leader();
        cell.open_bucket();
        let order: Vec<u64> = (1..=cell.nodes).collect();
        let asking = cell.asking(CLIENT, &order);
        let (registered, _) = cell.ask(&asking, GROUP, registration(CLIENT, 1, 1));
        let session = match registered.as_deref() {
            Ok([Answer::Registered { session }]) => *session,
            other => panic!("seed {seed}: the registration answered {other:?}"),
        };
        let largest = usize::try_from(SETTINGS.max_entry_bytes).unwrap();
        let within = largest - hyper_raft::wire::ENTRY_FIXED_BYTES;
        for (serial, bytes) in (1..).zip(within..=largest) {
            asking
                .try_send((GROUP, sized(session, serial, bytes)))
                .map_err(|_| "the client's asks are full")
                .unwrap();
            let (answered, _) = cell.until("the entry's answer", |c| {
                for (id, member) in &c.members {
                    if let Some((hyper_transport::Refusal::FrameBound, frame, kind)) =
                        member.status.and_then(|s| s.sends.refused)
                    {
                        panic!(
                            "seed {seed}: node {id} refused a frame of {frame} bytes ({kind:?}) \
                             for its class's bound, the entry {bytes} bytes; commits {:?}",
                            c.members
                                .iter()
                                .map(|(id, m)| (*id, m.status.map(|s| s.commit)))
                                .collect::<Vec<_>>()
                        );
                    }
                }
                c.asked.take()
            });
            assert!(
                matches!(
                    answered.as_deref(),
                    Ok([Answer::Name(name::Outcome::Put { .. })])
                ),
                "seed {seed}: the entry of {bytes} bytes answered {answered:?}"
            );
        }
        // Every node applies as far as the leader that answered the last.
        cell.converged();
    }
}

/// An entry of one put whose encoding is `bytes` long, its key padded to make it so: its session's
/// serial `serial`, every answer before it read.
fn sized(session: u64, serial: u64, bytes: usize) -> Entry {
    let entry = |key: &str| Entry {
        at_ns: 0,
        commands: vec![Sessioned {
            session,
            serial,
            unanswered: serial,
            command: put(key, "e", u128::from(serial)),
        }],
    };
    let tag = format!("{serial}.");
    let short = entry(&tag).encode().unwrap().len();
    let sized = entry(&format!("{tag}{}", "k".repeat(bytes - short)));
    assert_eq!(sized.encode().unwrap().len(), bytes);
    sized
}

/// A batch is answered only after the flushes it needs (CLAUDE.md §6): the group commit of
/// research/71 step 9 and research/87 §1, an entry carrying every command that waited, answers
/// none of them before its own write is durable on a quorum, and on the leader that answers. A
/// held write is kept on its device and its answer given to no range, as a device whose flush has
/// not ended gives none (`Sim::hold_writes`). The cell first answers such an entry with nothing
/// held, in `took` on its clock; then a client asks one entry while:
///
/// - so many followers' flushes are held that the nodes left are one short of a quorum: every
///   node writes it, and for `took` past that it is unanswered; with one follower's let go a
///   quorum flushes it, and every command of it is answered;
/// - the leader's own flush is held, every follower's done: the followers' quorum commits it and
///   each follower applies it, and for `took` past that the leader, which answers from what it
///   applies and applies only what its own log holds durable (hyper-durable's I2 and R-4), does
///   not answer; its flush let go, it does.
///
/// Once every node has written the entry, answering it without a flush it needs takes what is
/// left of a round, a follower's answer, the leader's commit and its reply, so less than `took`:
/// a node that answered a batch before its log's flush, or a leader before a quorum's, fails here,
/// as a store that gives its answers while held fails at the first seed.
#[test]
fn a_batched_entry_is_answered_only_after_the_flushes_it_needs_and_never_while_they_are_held() {
    for seed in 1..=4u64 {
        let mut cell = Sim::new(seed);
        let leader = cell.leader();
        cell.open_bucket();
        let order: Vec<u64> = (1..=cell.nodes).collect();
        let asking = cell.asking(CLIENT, &order);
        let (registered, _) = cell.ask(&asking, GROUP, registration(CLIENT, 1, 1));
        let session = match registered.as_deref() {
            Ok([Answer::Registered { session }]) => *session,
            other => panic!("seed {seed}: the registration answered {other:?}"),
        };
        // The same entry with nothing held: what the cell takes to answer it.
        let began = cell.now_ns();
        let (control, _) = cell.ask(&asking, GROUP, batch(session, 1, "c"));
        let took = cell.now_ns() - began;
        let control = control.unwrap_or_else(|why| panic!("seed {seed}: {why}"));
        assert!(batch_answered(&control), "seed {seed}: {control:?}");

        // The followers' flushes held, the nodes left one short of a quorum.
        let followers: Vec<u64> = (1..=cell.nodes).filter(|id| *id != leader).collect();
        let quorum = cell.nodes / 2 + 1;
        let held: Vec<u64> = followers
            .iter()
            .copied()
            .take(usize::try_from(cell.nodes - quorum + 1).unwrap())
            .collect();
        for &id in &held {
            cell.hold_writes(id, true);
        }
        let short = format!(
            "seed {seed}: answered with {held:?}'s flushes held, {} of {} nodes left to flush it, \
             short of the quorum of {quorum}",
            cell.nodes - u64::try_from(held.len()).unwrap(),
            cell.nodes,
        );
        let readies = |c: &Sim, id: u64| {
            c.members[&id]
                .status
                .as_ref()
                .map_or(0, |s| s.writes.readies)
        };
        let wrote: Vec<(u64, u64)> = (1..=cell.nodes)
            .map(|id| (id, readies(&cell, id)))
            .collect();
        asking
            .try_send((GROUP, batch(session, 1 + BATCH, "q")))
            .map_err(|_| "the client's asks are full")
            .unwrap();
        // Every node writes the entry, the held ones' answers kept from them; then, for a whole
        // round of the unheld entry, past the follower's answer, the leader's commit and its reply
        // that answering without the flushes would have taken, nothing is answered.
        cell.until("every node writes the entry", |c| {
            assert!(c.asked.is_none(), "{short}");
            let written = wrote.iter().all(|&(id, was)| readies(c, id) > was);
            if written { Some(()) } else { None }
        });
        cell.run_for(took);
        assert!(cell.asked.take().is_none(), "{short}");
        cell.hold_writes(held[0], false);
        let (quorum_answered, _) =
            cell.until("the answer once a quorum flushed", |c| c.asked.take());
        let quorum_answered = quorum_answered.unwrap_or_else(|why| panic!("seed {seed}: {why}"));
        assert!(
            batch_answered(&quorum_answered),
            "seed {seed}: {quorum_answered:?}"
        );
        for &id in &held {
            cell.hold_writes(id, false);
        }

        // The leader's own flush held, every follower's done.
        let applied = |c: &Sim, id: u64| c.members[&id].status.as_ref().map_or(0, |s| s.applied);
        cell.hold_writes(leader, true);
        asking
            .try_send((GROUP, batch(session, 1 + 2 * BATCH, "l")))
            .map_err(|_| "the client's asks are full")
            .unwrap();
        // Past what the held leader applied: only the entry, which their quorum committed.
        cell.until("every follower applies past the held leader", |c| {
            assert!(
                c.asked.is_none(),
                "seed {seed}: the leader {leader} answered with its own flush held"
            );
            let led = applied(c, leader);
            let past = followers.iter().all(|&id| applied(c, id) > led);
            if past { Some(()) } else { None }
        });
        cell.run_for(took);
        assert!(
            cell.asked.take().is_none(),
            "seed {seed}: the leader {leader} answered with its own flush held"
        );
        cell.hold_writes(leader, false);
        let (leader_answered, _) =
            cell.until("the leader's answer once it flushed", |c| c.asked.take());
        let leader_answered = leader_answered.unwrap_or_else(|why| panic!("seed {seed}: {why}"));
        assert!(
            batch_answered(&leader_answered),
            "seed {seed}: {leader_answered:?}"
        );
    }
}

/// Where a power cut tears a frame of the range's entry after its persist record landed
/// (durability.md §7.6), the frame's write lost and its record's kept, its flush cut.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tear {
    /// On one member, drawn from the seed; the others go on.
    One,
    /// On a quorum of members, drawn from the seed; once the last of them stops, every other
    /// node's power is cut at once, as a cell whose nodes share one feed loses them.
    Quorum,
    /// On every member of a one-voter group: the node of a cell of one, as a laptop's is.
    OneVoter,
}

/// What one torn cut left of the range: whether a member's copy of the entry was marked, and
/// whether the group was left waiting, every marked copy with none whole to repair from and no
/// quorum lacking it (durability.md §7.3).
#[derive(Clone, Copy, Debug, Default)]
struct Left {
    marked: bool,
    waiting: bool,
}

/// One run of `seed` under `tear`, its laws checked: an answered entry is applied on every node;
/// every mark ends once a quorum of members is up and one copy reads whole or a quorum lacks the
/// entry; a group left waiting reports `Uncertain` on each marked member and serves nothing; a
/// one-voter group's torn frame says it was not answered at its flush, and the group never marks.
fn torn(seed: u64, tear: Tear) -> Left {
    let mut cell = if tear == Tear::OneVoter {
        Sim::lone(seed)
    } else {
        Sim::new(seed)
    };
    cell.leader();
    cell.open_bucket();
    let order: Vec<u64> = (1..=cell.nodes).collect();
    let asking = cell.asking(CLIENT, &order);
    let (registered, _) = cell.ask(&asking, GROUP, registration(CLIENT, 1, 1));
    let session = match registered.as_deref() {
        Ok([Answer::Registered { session }]) => *session,
        other => panic!("seed {seed}: the registration answered {other:?}"),
    };
    // Every entry answered so far, applied on every node.
    let answered = cell.converged();
    let quorum = cell.nodes / 2 + 1;
    let mut draws = Seeded::new(stream_seed(seed, "tear", &[]));
    let mut members: Vec<u64> = (1..=cell.nodes).collect();
    let struck: Vec<u64> = (0..match tear {
        Tear::One => 1,
        Tear::Quorum => quorum,
        Tear::OneVoter => cell.nodes,
    })
        .map(|_| {
            let at = draws.below(u64::try_from(members.len()).unwrap());
            members.remove(usize::try_from(at).unwrap())
        })
        .collect();
    for &id in &struck {
        cell.tear_next_entry(id);
    }
    asking
        .try_send((GROUP, batch(session, 1, "t")))
        .map_err(|_| "the client's asks are full")
        .unwrap();
    cell.until("every torn member stops", |c| {
        if struck.iter().all(|id| c.is_down(*id)) {
            Some(())
        } else {
            None
        }
    });
    if tear == Tear::Quorum {
        for &id in &members {
            cell.crash(id);
        }
    }
    for &id in &struck {
        cell.cut_stopped(id);
    }
    let down: Vec<u64> = (1..=cell.nodes).filter(|id| cell.is_down(*id)).collect();
    let reopened: Vec<(u64, cell::Reopened)> =
        down.iter().map(|&id| (id, cell.restart(id))).collect();
    for (id, r) in &reopened {
        eprintln!(
            "seed {seed}: {tear:?}: node {id} reopened {:?}, its range {:?}",
            r.tail, r.view
        );
    }
    if tear == Tear::OneVoter {
        // A one-voter group's frames are answered once confirmed (durability.md §7.5): the torn
        // frame's record says it was not answered at its flush, as `Write::at_flush` holds only
        // for three voters or more, and the open keeps the frame as the torn tail, unmarked.
        for (id, r) in &reopened {
            assert!(
                matches!(
                    r.tail,
                    Tail::Undetermined {
                        at_flush: false,
                        ..
                    }
                ) && r.view.as_ref().is_some_and(|v| v.uncertain.is_none()),
                "seed {seed}: node {id}, a one-voter group's member, reopened {:?}, its range {:?}",
                r.tail,
                r.view
            );
        }
    }
    let marks: Vec<(u64, Start)> = reopened
        .iter()
        .filter_map(|(id, r)| r.view.as_ref()?.uncertain.map(|m| (*id, m)))
        .collect();
    let Some(index) = marks.iter().map(|(_, m)| m.index).min() else {
        cell.leader();
        assert!(
            cell.converged() >= answered,
            "seed {seed}: an answered entry was lost"
        );
        assert!(
            cell.members
                .values()
                .all(|m| m.status.as_ref().is_some_and(|s| !s.uncertain)),
            "seed {seed}: {tear:?}: a member opened unmarked reports a mark"
        );
        return Left::default();
    };
    for (id, _) in &marks {
        assert!(
            struck.contains(id),
            "seed {seed}: node {id}, whose writes were not torn, was marked"
        );
    }
    // The unmarked members opened again, by what each holds of the marked entry: whole, or
    // lacking it. A member still running holds it whole or lacks it, and either way the group
    // resolves through it with the others like it: it is counted with those lacking it, the
    // lesser claim, since a quorum of them lacks it or one holds it.
    let unmarked = |id: &u64| marks.iter().all(|(m, _)| m != id);
    let running = (1..=cell.nodes)
        .filter(|id| unmarked(id) && reopened.iter().all(|(r, _)| r != id))
        .count();
    let whole = reopened
        .iter()
        .filter(|(id, r)| unmarked(id) && r.view.as_ref().is_some_and(|v| v.last >= index))
        .count();
    let lacking = reopened.iter().filter(|(id, _)| unmarked(id)).count() - whole + running;
    let waiting = whole == 0 && u64::try_from(lacking).unwrap() < quorum;
    eprintln!(
        "seed {seed}: {tear:?}: marked {:?}, whole copies {whole}, lacking {lacking}, quorum \
         {quorum}: waiting {waiting}",
        marks.iter().map(|(id, _)| *id).collect::<Vec<_>>()
    );
    if waiting {
        cell.until("each marked member reports Uncertain", |c| {
            let reported = marks
                .iter()
                .all(|(id, _)| c.members[id].status.as_ref().is_some_and(|s| s.uncertain));
            if reported { Some(()) } else { None }
        });
        // For the cell's quiet period, the longest any node states it may go without moving:
        // every mark stays, and nothing past what was answered is applied or answered.
        let quiet = cell.quiet();
        cell.run_for(quiet);
        for (id, member) in &cell.members {
            let status = member.status.as_ref();
            assert!(
                status.is_none_or(|s| s.applied <= answered),
                "seed {seed}: node {id} of a waiting group applied {status:?}"
            );
        }
        for (id, _) in &marks {
            assert!(
                cell.members[id]
                    .status
                    .as_ref()
                    .is_some_and(|s| s.uncertain),
                "seed {seed}: node {id}'s mark ended in a waiting group"
            );
        }
        assert!(
            cell.asked.take().is_none(),
            "seed {seed}: a waiting group answered"
        );
    } else {
        cell.until("every mark ends", |c| {
            let ended = c
                .members
                .values()
                .all(|m| m.status.as_ref().is_some_and(|s| !s.uncertain));
            if ended { Some(()) } else { None }
        });
        cell.leader();
        assert!(
            cell.converged() >= answered,
            "seed {seed}: an answered entry was lost"
        );
    }
    Left {
        marked: true,
        waiting,
    }
}

/// A power cut that tears a frame of the range's entry after its persist record landed, the
/// frame's write lost and its record's kept (durability.md §7.6), on one member, on a quorum of
/// members and on every member of a one-voter group, over the power-cut tests' seeds. Every frame
/// is still answered once confirmed, and the record of a range of three voters or more says its
/// frame was answered at its flush, so the open restores each torn member's frame as a lost one
/// and marks it; a one-voter group's record says it was not, and the group is never marked. The
/// laws: an answered entry is applied on every node; every mark ends once a quorum is up and one
/// copy reads whole or a quorum lacks the entry; a group left waiting reports `Uncertain` and
/// serves nothing. The groups marked and left waiting are counted, the evidence §7.5 decides step
/// 4 from.
#[test]
fn a_frame_torn_after_its_record_lands_is_marked_and_each_group_resolves_or_waits_reported() {
    let seeds = seeds(&(1..=CUT_SEEDS).collect::<Vec<_>>());
    let mut table = Vec::new();
    for tear in [Tear::One, Tear::Quorum, Tear::OneVoter] {
        let left: Vec<Left> = seeds.iter().map(|&seed| torn(seed, tear)).collect();
        let marked = left.iter().filter(|l| l.marked).count();
        let waiting = left.iter().filter(|l| l.waiting).count();
        table.push((tear, marked, marked - waiting, waiting));
    }
    eprintln!("| fault | seeds | groups marked | groups resolved | groups left waiting |");
    eprintln!("|---|---|---|---|---|");
    for (tear, marked, resolved, waiting) in &table {
        eprintln!(
            "| {tear:?} | {}-{} | {marked} | {resolved} | {waiting} |",
            seeds.first().unwrap(),
            seeds.last().unwrap()
        );
    }
    let one = table.iter().find(|row| row.0 == Tear::One).unwrap();
    assert_eq!(one.3, 0, "a tear on one member left a group waiting");
}

/// A range's voters change under the clients' load, each joining member a learner caught up and
/// then a voter (membership.md §3 step 4, replica.md §6): a cell of one voter grows to two, and a
/// cell of two replaces one of them, its leader on half the seeds. Through each change the range
/// is without a leader no longer than a round trip of the cell's slowest path and the step the
/// test looks at it by, no term has two leaders, the change ends, the histories are linearizable
/// and every member the change keeps applies as far as the others. A leader replaced by its own
/// change led a configuration without it until the change committed and then stopped, leaving
/// its range to an election (Raft dissertation §4.2.2): two elections and 3.2 ms without a leader
/// on one switch (seed 1, 16 one-way delays), 3 s across the planet (seed 4, 6). It hands its lead
/// on first now (`Replacement::step`; dissertation §3.10): none and 0.5 s.
#[test]
fn a_range_changes_its_voters_under_load_never_leaderless_past_a_round_trip() {
    for seed in seeds(&[1, 2, 3, 4]) {
        // One voter grows to two: the spare's promotion alone, the member it "replaces" one the
        // range never named.
        changed(seed, 1, |cell, _| (cell.nodes + 1, 2));
        // Two voters replace one with the spare: the leader on seeds 1 and 4 (and every fourth
        // past them), a follower on the others.
        changed(seed, 2, |_, leader| {
            let failed = if seed % 4 < 2 { leader } else { 3 - leader };
            (failed, 3)
        });
    }
}

/// A cell of `seed` of `voters` founding voters and a spare: once the clients did a quarter of
/// their operations, the range replaces the member `change` names with the one it names to join,
/// and the test watches the range's leaders until the clients are done.
fn changed(seed: u64, voters: u64, change: impl Fn(&Sim, u64) -> (u64, u64)) {
    let mut cell = Sim::with_spare(seed, voters);
    let leader = cell.until("a founder leads", |c| {
        let named = c.members[&1].status.map_or(0, |s| s.leader);
        let agreed = named != 0
            && (1..=voters).all(|id| c.members[&id].status.is_some_and(|s| s.leader == named));
        if agreed { Some(named) } else { None }
    });
    cell.open_bucket();
    let ops = (2 * (log_config().group_entries + 1)).div_ceil(CLIENTS);
    let stops = cell.start_workload(ops);
    cell.done_by(ops, 1);
    let (failed, joining) = change(&cell, leader);
    eprintln!("seed {seed}: {voters} voters, leader {leader}, {failed} replaced by {joining}");
    cell.replace(failed, joining);
    // What the test looks at the cell by: a round trip of its slowest path, and the step between
    // looks.
    let bound = 2 * cell.longest_ns() + cell.step_ns();
    let mut leaders: BTreeMap<u64, u64> = BTreeMap::new();
    let (mut unled_since, mut longest_unled) = (None::<u64>, 0u64);
    cell.until("the clients' operations through the change", |c| {
        let now = c.now_ns();
        let mut led = false;
        for (id, status) in c
            .members
            .iter()
            .filter_map(|(id, m)| Some((*id, m.status?)))
        {
            if status.leader == id {
                led = true;
                let named = *leaders.entry(status.term).or_insert(id);
                assert_eq!(
                    named, id,
                    "seed {seed}: two leaders in term {}",
                    status.term
                );
            }
        }
        match (led, unled_since) {
            (false, None) => unled_since = Some(now),
            (true, Some(since)) => {
                longest_unled = longest_unled.max(now - since);
                unled_since = None;
            }
            _ => {}
        }
        let done = c.progress.len() == usize::try_from(CLIENTS).unwrap()
            && c.progress.values().all(|done| *done >= ops)
            && c.replaced.contains(&(failed, joining));
        if done { Some(()) } else { None }
    });
    drop(stops);
    cell.until("every client's part", |c| {
        if c.parts.len() == usize::try_from(CLIENTS).unwrap() {
            Some(())
        } else {
            None
        }
    });
    eprintln!(
        "seed {seed}: {voters} voters, longest without a leader {longest_unled} ns, bound {bound} ns"
    );
    assert!(
        longest_unled <= bound,
        "seed {seed}: {voters} voters, {failed} replaced by {joining} with {leader} leading: \
         {longest_unled} ns without a leader, past {bound} ns"
    );
    let parts: Vec<(u64, Part)> = std::mem::take(&mut cell.parts).into_iter().collect();
    check_histories(parts.iter().flat_map(|(_, part)| part.iter().cloned()));
    // Every member the change keeps applies as far as the others; the one it removed is sent
    // nothing past its removal.
    cell.until("every member kept applies as far as the others", |c| {
        let mut applied = c
            .members
            .iter()
            .filter(|(id, _)| **id != failed)
            .map(|(_, m)| m.status.map(|s| s.applied));
        let first = applied.next()??;
        if first > 0 && applied.all(|a| a == Some(first)) {
            Some(())
        } else {
            None
        }
    });
}

/// A cell of one voter grows to two under the clients' load, each node asking a read of its own
/// every 20 ms as a gateway's request does (`Node::read`), and the new voter's device holds its
/// writes for 1 s from its promotion, as a new member's busy disk does. Every read asked after the
/// device answers again is answered, or ended past its range's budget, within the cell's quiet
/// period (`Sim::quiet`) of its asking (CLAUDE.md §2). Seen while looking for CI gates run
/// 44510117734's "2 nodes" SlowDown (`crates/mantle/tests/membership.rs`), which a cell grown to
/// two voters answered a GET: on seed 1 the new voter's reads, opened while the founder held them
/// for a quorum its held device could not give, filled its exchange table, and each open the table
/// refused kept its message's room in the transport's budget for good (hyper-transport's
/// `Core::open`, vendor/UPSTREAM.md), asked again at every turn while its member named the
/// leader; its budget full, every reply's head and lane frame after was refused `Budget`, and it
/// answered none of its own reads again, its clock without a leader never started while its member
/// trusted the leader.
#[test]
fn a_new_voters_own_reads_are_answered_after_its_device_held_its_writes_at_its_promotion() {
    for seed in seeds(&[1, 2]) {
        own_reads_after_a_held_promotion(seed);
    }
}

fn own_reads_after_a_held_promotion(seed: u64) {
    let mut cell = Sim::with_spare(seed, 1);
    cell.until("the founder leads", |c| {
        if c.members[&1].status.is_some_and(|s| s.leader == 1) {
            Some(())
        } else {
            None
        }
    });
    cell.open_bucket();
    let ops = (2 * (log_config().group_entries + 1)).div_ceil(CLIENTS);
    let stops = cell.start_workload(ops);
    cell.done_by(ops, 1);
    cell.replace(cell.nodes + 1, 2);
    let every = 20_000_000u64.max(2 * cell.longest_ns());
    let hold = 1_000_000_000u64;
    let (mut promoted, mut released, mut next) = (None::<u64>, None::<u64>, 0u64);
    cell.until("the new voter's device answers again", |c| {
        let now = c.now_ns();
        if promoted.is_none() && !c.replaced.is_empty() {
            promoted = Some(now);
            c.hold_writes(2, true);
        }
        if let Some(at) = promoted
            && now >= at + hold
        {
            c.hold_writes(2, false);
            released = Some(now);
            return Some(());
        }
        if now >= next {
            c.own_read(1, now);
            c.own_read(2, now);
            next = now + every;
        }
        None
    });
    let released = released.unwrap();
    let bound = cell.quiet();
    // Reads asked after the release, each node's, until every one of them is answered: those
    // still out, oldest first, and the answers taken so far.
    let mut out: std::collections::VecDeque<(u64, u64)> = std::collections::VecDeque::new();
    let mut answered: std::collections::HashSet<(u64, u64)> = std::collections::HashSet::new();
    let mut taken = 0;
    let end = released + 10 * bound;
    cell.until("every read asked after the release answered", |c| {
        let now = c.now_ns();
        if now >= next && now < end {
            for id in [1, 2] {
                c.own_read(id, now);
                out.push_back((id, now));
            }
            next = now + every;
        }
        for r in &c.own_reads[taken..] {
            answered.insert((r.0, r.1));
        }
        taken = c.own_reads.len();
        out.retain(|asked| !answered.contains(asked));
        if let Some((id, at)) = out.front() {
            assert!(
                now < at + bound,
                "seed {seed}: node {id}'s own read asked {} ns after the release unanswered past \
                 {bound} ns; node 1 answered {}, node 2 {}; statuses {:?}",
                at - released,
                c.own_reads.iter().filter(|r| r.0 == 1).count(),
                c.own_reads.iter().filter(|r| r.0 == 2).count(),
                c.members
                    .iter()
                    .map(|(id, m)| (*id, m.status))
                    .collect::<Vec<_>>()
            );
        }
        if now >= end && out.is_empty() {
            Some(())
        } else {
            None
        }
    });
    drop(stops);
}

/// A lone command's answer waits on its paths and its devices, never on a timer's period: a client
/// one switch from a cell of three asks the leader one command at a time, and once its connection
/// is up each is answered within its stages' own times (the client's path to the leader, the
/// leader's to a follower and back, which its own flush overlaps, and the leader's back to the
/// client, each at most the profile's longest one-way delay; the simulated device answers each
/// write as it is asked) plus a step of the test's clock at each end. A wait on a beat, a heartbeat,
/// a batching window or a node's next due time would take a timer's period past it (the beat is
/// `bounds::initial_pto`, about a second; the liveness stream's heartbeats are measured, many
/// round trips apart).
#[test]
fn a_lone_command_is_answered_within_its_paths_times_not_a_timers_period() {
    for seed in seeds(&[1, 7]) {
        // Odd seeds are cells of three; seeds 1 and 7 one switch apart (`cell::PROFILES`).
        let mut cell = Sim::new(seed);
        let leader = cell.leader();
        cell.judged();
        let mut order: Vec<u64> = (1..=cell.nodes).collect();
        order.sort_by_key(|id| *id != leader);
        let asking = cell.asking(CLIENT, &order);
        let bound = 4 * cell.longest_ns() + 2 * cell.step_ns();
        for serial in 1..=16 {
            let commits: BTreeMap<u64, u64> = cell
                .members
                .iter()
                .map(|(id, m)| (*id, m.status.map_or(0, |s| s.commit)))
                .collect();
            let at = cell.now_ns();
            asking
                .try_send((GROUP, registration(CLIENT, 1, serial)))
                .map_err(|_| "the client's asks are full")
                .unwrap();
            // When each node's commit first passed where it stood as the client asked.
            let mut committed: BTreeMap<u64, u64> = BTreeMap::new();
            let (answer, _) = cell.until("the client's answer", |c| {
                note(c, &commits, at, &mut committed);
                c.asked.take()
            });
            let answered = cell.now_ns() - at;
            assert!(
                matches!(answer.as_deref(), Ok([Answer::Registered { .. }])),
                "seed {seed}: registration {serial} answered {answer:?}"
            );
            cell.until("every node commits it", |c| {
                note(c, &commits, at, &mut committed);
                if committed.len() == commits.len() {
                    Some(())
                } else {
                    None
                }
            });
            eprintln!(
                "seed {seed}: command {serial}: the leader {leader} committed at {} ns, the client \
                 answered at {answered} ns, each node committed at {committed:?} ns",
                committed[&leader]
            );
            // The first command waits for its client's connection too.
            if serial > 1 {
                assert!(
                    answered <= bound,
                    "seed {seed}: command {serial} answered after {answered} ns, past its paths' \
                     {bound} ns"
                );
            }
        }
    }
}

/// Notes the time, from `at`, at which each node's commit first passed where `commits` has it.
fn note(cell: &Sim, commits: &BTreeMap<u64, u64>, at: u64, committed: &mut BTreeMap<u64, u64>) {
    for (id, member) in &cell.members {
        if member.status.is_some_and(|s| s.commit > commits[id]) {
            committed.entry(*id).or_insert(cell.now_ns() - at);
        }
    }
}

/// The commands each node of [`commands_refused_for_room_wait_for_the_room_not_a_pause`] holds of
/// its own at once.
const OWN_COMMANDS: u64 = 24;

/// A command a range's leader refuses for room waits for the room, never a backoff's pause: in a
/// cell of three one switch apart whose range takes one entry uncommitted at a time, the leader and
/// a follower each ask a burst of puts of their own past what they hold, every one by a deadline a
/// second and a half away. Those past what a node holds are refused at once, typed; the rest go out
/// as entries two at a time, the second refused for room while the first is uncommitted, on the
/// leader by its own member and on the follower by the leader. Each refused entry is asked again
/// once its range's member on the node shows the range move: on the leader the commit that frees
/// its room, on the follower that commit as the leader's next append carries it. So every command
/// is answered by its deadline; each wait for room ends on the range's move (none by a pause), the
/// longest within the leader's round trip to a follower and back, and on the follower one more
/// way. Asked again after a backoff's pause instead (half the initial probe timeout to all of it,
/// about 0.5 s to 1 s), the burst's entries took that each, and the deadline passed first.
#[test]
fn commands_refused_for_room_wait_for_the_room_not_a_pause() {
    for seed in seeds(&[1, 7]) {
        // One entry of two puts uncommitted at a time; the process's message bounds as every
        // other cell's (`message_bounds`).
        let settings = mantle_range::Settings {
            max_uncommitted_size: SETTINGS.max_size_per_msg,
            ..SETTINGS
        };
        let mut cell = Sim::asking_itself(seed, settings, usize::try_from(OWN_COMMANDS).unwrap());
        let leader = cell.leader();
        cell.judged();
        cell.open_bucket();
        let follower = (1..=cell.nodes).find(|id| *id != leader).unwrap();
        let at = cell.now_ns();
        let deadline = at + 1_500_000_000;
        let past = 4;
        for (node, first) in [(leader, 0), (follower, 1_000)] {
            cell.own_commands(node, first, OWN_COMMANDS + past, deadline);
        }
        let asked = 2 * (OWN_COMMANDS + past);
        cell.until("every own command ended", |c| {
            if c.own_commands.len() == usize::try_from(asked).unwrap() {
                Some(())
            } else {
                None
            }
        });
        let mut refused = 0;
        let mut last = 0;
        for (node, tag, outcome, taken) in &cell.own_commands {
            match outcome {
                Ok(CommandOutcome::Answered(Answer::Name(name::Outcome::Put { .. }))) => {}
                Err(why) => {
                    assert!(
                        why.contains("commands"),
                        "seed {seed}: node {node}'s command {tag} refused: {why}"
                    );
                    refused += 1;
                    continue;
                }
                Ok(outcome) => panic!("seed {seed}: node {node}'s command {tag} ended {outcome:?}"),
            }
            assert!(
                *taken <= deadline + cell.step_ns(),
                "seed {seed}: node {node}'s command {tag} answered at {taken} ns, past its \
                 deadline {deadline} ns"
            );
            last = last.max(taken - at);
        }
        assert_eq!(
            refused,
            2 * past,
            "seed {seed}: the commands past what a node holds"
        );
        for (node, bound) in [
            (leader, 2 * cell.longest_ns() + 2 * cell.step_ns()),
            (follower, 3 * cell.longest_ns() + 2 * cell.step_ns()),
        ] {
            let room = cell.members[&node].status.unwrap().room;
            eprintln!(
                "seed {seed}: node {node} ({}): {} waits for room, {} ended by a pause, the \
                 longest {:?}; the burst answered in {last} ns",
                if node == leader { "leader" } else { "follower" },
                room.ended,
                room.timed,
                room.longest
            );
            assert!(
                room.ended > 0,
                "seed {seed}: node {node} found its range with room"
            );
            assert_eq!(
                room.timed, 0,
                "seed {seed}: node {node}'s waits ended by a pause"
            );
            assert!(
                room.longest <= Duration::from_nanos(bound),
                "seed {seed}: node {node} waited {:?} for room, past {bound} ns",
                room.longest
            );
        }
    }
}

/// How far past its work each turn of the loaded founder runs, at most, drawn anew each turn
/// ([`Sim::load`]).
const FOUNDER_LOAD_NS: u64 = 20_000_000;
/// The loaded founder's heartbeats the joiner takes, each a judgment, while the test watches for a
/// suspicion of it.
const FOUNDER_HEARTBEATS: u64 = 100;

/// A cell of one voter grows to two under the clients' load, its founder loaded from before the
/// join, each of its turns running up to [`FOUNDER_LOAD_NS`] past its work, as on a host whose other
/// processes hold its CPU. The node that just joined, its detectors meeting
/// the founder at the join, suspects the live founder never; cut off from it, it suspects it within
/// the patience it states (`Status::patience`). Seen in CI gates run 44619550115 (macOS): at the "2 nodes" step of
/// `crates/mantle/tests/membership.rs`, the node that had just joined suspected the founder five
/// times while it served, and a read through it of a block only the founder held failed.
#[test]
fn a_node_that_just_joined_never_suspects_its_live_loaded_founder_and_suspects_it_cut_off() {
    for seed in seeds(&[1, 2]) {
        let suspicions = loaded_founder(seed, false);
        assert_eq!(
            suspicions, 0,
            "seed {seed}: the joiner suspected its live founder while it took \
             {FOUNDER_HEARTBEATS} of its heartbeats"
        );
    }
}

/// The same cell, its founder loaded only from after the join: the joiner's detector meets the
/// founder's stalls with nothing of the load in what judges the founder's link, and the times it
/// suspects the live founder are counted here, not refused. Cut off, the founder is suspected
/// within the patience the joiner states (`Status::patience`). On seed 1 (measured over
/// f41d8344) the link had its own configuration from the 312th heartbeat it took, a 5.3 ms margin
/// from latenesses that knew nothing of the load, kept until they double: it suspected the live
/// founder 60 times while the joiner took 100 of its heartbeats, none of them by the margin of the
/// node's evidence, which a premature suspicion lengthens at once (hyper-liveness
/// `Pair::premature_margin`), and the cut-off founder 41.2 ms after the cut, its patience 90.5 ms.
/// The live founder's suspicions are not yet bounded. Seeds 2 and 3, regional paths, reach the
/// join only after minutes of an unloaded founder's turns (`MANTLE_NODE_SIM_SEEDS`,
/// `MANTLE_NODE_SIM_SEED`).
#[test]
fn a_node_that_just_joined_meets_its_founder_loaded_after_the_join_and_suspects_it_cut_off() {
    for seed in seeds(&[1]) {
        loaded_founder(seed, true);
    }
}

/// Runs the cell of [`a_node_that_just_joined_never_suspects_its_live_loaded_founder_and_suspects_it_cut_off`],
/// its founder loaded from before the join or, `after_join`, from after it; checks that the joiner
/// suspects the cut-off founder within the patience it states, and gives the times it suspected
/// its live founder while it took [`FOUNDER_HEARTBEATS`] of its heartbeats.
fn loaded_founder(seed: u64, after_join: bool) -> u64 {
    let mut cell = Sim::with_spare(seed, 1);
    cell.until("the founder leads", |c| {
        if c.members[&1].status.is_some_and(|s| s.leader == 1) {
            Some(())
        } else {
            None
        }
    });
    cell.open_bucket();
    let ops = 2 * (log_config().group_entries + 1);
    let stops = cell.start_workload(ops);
    cell.done_by(ops, 1);
    if !after_join {
        cell.load(1, FOUNDER_LOAD_NS);
    }
    cell.replace(cell.nodes + 1, 2);
    cell.until("the spare joins", |c| {
        if c.replaced.is_empty() {
            None
        } else {
            Some(())
        }
    });
    if after_join {
        cell.load(1, FOUNDER_LOAD_NS);
    }
    // Judged at each of the founder's heartbeats the joiner takes: run until it took
    // `FOUNDER_HEARTBEATS` more of them, the founder alive throughout, each taken a judgment that
    // could have been a suspicion. They come at the interval the joiner's detector chose for the
    // founder's link and asked of its stream (hyper-liveness's `Pair::interval`), not at the
    // founder's floor: on seed 2 the joiner, its detector configured from 281 of the loaded
    // founder's heartbeats, asked 781 ms, its margin 449 ms and the group's unavailability 1.5%,
    // where at the 11 ms the founder sent at the best margin was 1.83 s and the unavailability 24%.
    // Each is given the patience the joiner states when it took the one before (`Status::patience`):
    // the longest its evidence of the founder may go unmoved, that interval and its margin, then
    // its own proof's wait, an election and a round. A founder whose heartbeats stopped coming fails
    // the test by it, as one the joiner never suspected. Loaded only from after the join, the
    // founder stalls past that patience while the joiner's link to it knows nothing of the load,
    // and is suspected at its first stall at least: each heartbeat is then given the cell's quiet
    // period, as `Sim::until` gives anything to come, and the suspicions are counted, not refused.
    let heartbeats = |c: &Sim| c.members[&2].status.map_or(0, |s| s.heartbeats);
    let patience = |c: &Sim| {
        if after_join {
            return c.quiet();
        }
        let stated = c.members[&2].status.and_then(|s| s.patience).unwrap();
        u64::try_from(stated.as_nanos()).unwrap()
    };
    let given_as = if after_join {
        "the cell's quiet period"
    } else {
        "the patience the joiner stated"
    };
    let (from, began) = (heartbeats(&cell), cell.now_ns());
    let (mut taken, mut moved, mut given) = (from, began, patience(&cell));
    let (mut longest, mut bound) = (0, given);
    while taken < from + FOUNDER_HEARTBEATS {
        cell.run_for(cell.step_ns());
        if heartbeats(&cell) > taken {
            if cell.now_ns() - moved > longest {
                (longest, bound) = (cell.now_ns() - moved, given);
            }
            (taken, moved, given) = (heartbeats(&cell), cell.now_ns(), patience(&cell));
            continue;
        }
        assert!(
            cell.now_ns() - moved <= given,
            "seed {seed}: the joiner took none of the loaded founder's heartbeats for {} ns, past \
             {given_as}, {given} ns, {} of {FOUNDER_HEARTBEATS} taken; suspicions {:?}",
            cell.now_ns() - moved,
            taken - from,
            cell.suspected
        );
    }
    eprintln!(
        "seed {seed}: the joiner took {FOUNDER_HEARTBEATS} of the loaded founder's heartbeats in {} \
         ns, at most {longest} ns apart, {given_as} then {bound} ns",
        cell.now_ns() - began
    );
    let suspected = |c: &Sim| c.suspected.get(&(2, 1)).copied().unwrap_or(0);
    let live = suspected(&cell);
    // The patience the joiner states at the cut: the longest its evidence of the founder may go
    // unmoved, its own proof's wait, then an election and a round (`Status::patience`).
    let stated = cell.members[&2].status.and_then(|s| s.patience).unwrap();
    let (cut, bound) = (cell.now_ns(), u64::try_from(stated.as_nanos()).unwrap());
    cell.isolate(1, true);
    let at = cell.until("the joiner suspects its cut-off founder", |c| {
        (suspected(c) > live).then(|| c.now_ns())
    });
    eprintln!(
        "seed {seed}: the joiner suspected its cut-off founder {} ns after the cut; patience \
         {bound} ns; suspicions of it live: {live}",
        at - cut
    );
    assert!(
        at - cut <= bound,
        "seed {seed}: the joiner suspected its cut-off founder {} ns after the cut, past the \
         patience of {bound} ns it stated; suspicions of it live: {live}",
        at - cut
    );
    drop(stops);
    live
}
