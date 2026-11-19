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

use hyper_block::sim::Fault as DeviceFault;
use hyper_log::{Start, Tail};
use hyper_rt::sim::SimPath;
use hyper_sim::rng::{Seeded, stream_seed};
use mantle_meta::name;
use mantle_meta::wire::{Answer, Entry, Sessioned};

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

/// REPRODUCTION of an open bug, failing, so ignored until it is fixed: a cell of one voter grows
/// to two under the clients' load, each node asking a read of its own every 20 ms as a gateway's
/// request does (`Node::read`), and the new voter's device holds its writes for 1 s from its
/// promotion, as a new member's busy disk does. Every read asked after the device answers again
/// must be answered, or ended past its range's budget, within the cell's quiet period
/// (`Sim::quiet`) of its asking (CLAUDE.md §2). On seed 1 the founder's reads are all answered,
/// but none of the new voter's is again: its reads wait paused at the longest backoff (its
/// status says `asking: 3.072s`) and are asked again for good, never answered and never ended,
/// its clock without a leader never started. Without the hold every read of both is answered.
/// Seen while looking for CI gates run 44510117734's "2 nodes" SlowDown
/// (`crates/mantle/tests/membership.rs`), which a cell grown to two voters answered a GET.
#[test]
#[ignore = "reproduction of an open bug: a new voter's own reads go unanswered after its device \
            held its writes at its promotion"]
fn a_new_voters_own_reads_are_answered_after_its_device_held_its_writes_at_its_promotion() {
    let seed = 1;
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
    // Reads asked after the release, each node's, until every one of them is answered.
    let mut asked: Vec<(u64, u64)> = Vec::new();
    let end = released + 10 * bound;
    cell.until("every read asked after the release answered", |c| {
        let now = c.now_ns();
        if now >= next && now < end {
            for id in [1, 2] {
                c.own_read(id, now);
                asked.push((id, now));
            }
            next = now + every;
        }
        for (id, at) in &asked {
            let answered = c.own_reads.iter().any(|r| r.0 == *id && r.1 == *at);
            assert!(
                answered || now < at + bound,
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
        if now >= end { Some(()) } else { None }
    });
    drop(stops);
}
