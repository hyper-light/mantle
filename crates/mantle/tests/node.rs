//! Real processes: a cell of `mantle node`s on loopback, three voters or five, each its own
//! process with its own directory, log file and engine store, replicating the cell's first Name
//! range over QUIC and the sealed datagram plane.
//!
//! - A client writes registrations to the range's leader; the leader's process is killed
//!   (SIGKILL) part way and started again on its directory, a new run of its liveness stream; the
//!   survivors elect, the client follows, and the node started again catches up. Asserted: every
//!   write the client was answered was answered once, and every node ends at the same applied
//!   index past every answered write.
//! - Three clients, each its own identity and session, put and read two keys while the leader is
//!   killed with SIGKILL half way and started again three quarters through, or cut off from the
//!   other nodes for a quarter by the test's relay (`crates/node/tests/support/relay.rs`), or, on
//!   three voters, has its plane deafened by the relay for a quarter (it hears no peer's plane
//!   while they hear its); each key's history is linearizable
//!   (`crates/node/tests/support/history.rs`), and every node ends where the others do. On cells
//!   of three voters and of five.
//! - A follower's engine store is damaged at rest while it is down, where both its superblocks
//!   lie: started again, it refuses the store with a typed corruption error rather than serve,
//!   and a spare under an identity never used replaces it (`replace` on the nodes' input, the
//!   leader's `replaced` line); then as many original voters are killed as leave a bare quorum,
//!   the spare in it. The clients' history through both is linearizable. On three voters and five.
//! - A cell hosting a range of each layer, the root range and the Bucket, Name, File and Block
//!   ranges, on every node, voters one to a node: each range elects and answers a session, a Name
//!   command is applied by the Name range and answered `WrongLayer` by the others, and the root
//!   range reads its volume table and refuses a read of another layer's rows; the node leading the most ranges is
//!   killed (SIGKILL), each range serves on, and the node started again catches up on each.
//! - A cell of every layer whose nodes keep volumes: each node enlists itself and its volume in the
//!   root range as it starts, the table a client reads names each volume's node and domain, and
//!   placement over it offers a volume of each node first for every block; started again, a node
//!   enlists again and the table holds what it held; started in another domain, a node is refused
//!   and ends saying why, and enlists again in its own.
//! - A cell of every layer whose nodes keep volumes and host objects: a bucket made, objects of a
//!   byte to 20 MiB put through each node in turn and read back, whole and in part, through
//!   another, each block's chunks on distinct nodes; with one node killed (SIGKILL) every object
//!   reads back through the others, and through it once it is started again.
//! - A storage cell, each node keeping a chunk volume: a gateway, a client in the gateway's role,
//!   puts chunks round the nodes over QUIC, one of each length on each, and one node is killed
//!   (SIGKILL) part way. Every put to a node running is stored, the killed node's until the kill
//!   and lost after; with it down, its peers' chunks read back whole and a put to it is lost; started
//!   again on its directory, it holds the same volume, every chunk acknowledged reads back whole, a
//!   lost put landed whole or not at all and is stored when retried, and each refusal is typed.
//!
//! The test waits on facts the processes report (`bound`, `status` lines) and the clients'
//! progress, and gives up once a quiet period passes with nothing moved: the longest any node
//! states it may go without moving (`Status::patience`) or a link to a peer it dials may, should
//! that peer fall silent (`Status::reconnect`), or any client gives an exchange
//! (`Client::patience`), never less than RFC 6298's initial retransmission timeout, one second (as
//! `crates/node/tests/cluster.rs` does). A node whose log has a flush out extends the watch, since
//! its range moves only as its writes become durable, until the flush has been out longer than the
//! longest any node's log has taken and the quiet period besides (its `log` line;
//! hyper-durable-e2e's rule, as the cluster's). A cell no majority of whose running nodes is free
//! of its own device holds still for its devices, as its law has it (`Status::held`: a node judges
//! no peer while its own proof is out, timing.md §2.8, and a range's quorum waits on its members'
//! devices), and the watch does not count that time, each such stretch up to the same bound, so a
//! cell held by a fault of its own still fails. On windows-2025 the runner's one disk held every
//! node's writes 5 to 7.5 s at once (CI run 39544711299): the nodes held still, and the watch took
//! the cell for a stopped one. A process's lines are taken only while that process runs: a node
//! started again is a new process, and a line its killed one left is dropped.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

#[allow(dead_code, unused_imports)]
#[path = "support/cell.rs"]
mod cell;
#[allow(dead_code)]
#[path = "../../node/tests/support/history.rs"]
mod history;
#[path = "../../range/tests/support/linear.rs"]
mod linear;
#[path = "../../node/tests/support/relay.rs"]
mod relay;
use cell::*;

/// The stated log group's bytes are the least that holds one ready of the stated range settings.
#[test]
fn the_stated_log_group_holds_one_ready_of_the_stated_range() {
    let settings = mantle_range::Settings {
        election_tick: 10,
        heartbeat_tick: 2,
        max_size_per_msg: 1024,
        max_inflight_msgs: 2,
        max_uncommitted_size: 2048,
        max_committed_size_per_ready: 1_048_576,
        max_entry_bytes: 1024,
    };
    let (entries, bytes) = mantle_range::ready_held(&settings).unwrap();
    assert_eq!(bytes, GROUP_BYTES);
    assert!(entries <= GROUP_ENTRIES);
}

#[test]
fn a_leader_killed_mid_write_loses_no_answered_write_and_catches_up_when_started_again() {
    let mut cell = Cell::new(NODES, Network::Direct);
    let leader = cell.leader(GROUP);
    let (settings, bound) = (cell.pki.client_settings(CLIENT), cell.bound_quic());
    let (writes, tell) = (2 * WRITES, cell.tell.clone());
    let thread = std::thread::spawn(move || writer(settings, bound, writes, tell));
    let written = |done: u64| {
        move |c: &mut Cell| {
            if c.progress.get(&CLIENT).is_some_and(|d| *d >= done) {
                Some(())
            } else {
                None
            }
        }
    };
    // Killed part way: the writes after go through the survivors.
    cell.until(written(WRITES));
    cell.kill(leader);
    cell.until(written(writes));
    let answered = thread.join().unwrap();
    cell.start(leader);
    cell.introduce();
    // What the survivors applied, which the node started again must apply.
    let last = (1..=NODES)
        .filter(|id| *id != leader)
        .map(|id| cell.applied(id, GROUP))
        .max()
        .unwrap();
    cell.until(|c| {
        if c.applied(leader, GROUP) >= last {
            Some(())
        } else {
            None
        }
    });
    // Every write was answered once, a registration's answer each.
    assert_eq!(answered.len() as u64, writes);
    for (serial, answers) in &answered {
        assert!(
            matches!(
                answers.as_slice(),
                [Answer::Registered { .. } | Answer::SessionsFull { .. }]
            ),
            "registration {serial} answered {answers:?}"
        );
    }
    cell.converged(GROUP);
}

/// What befalls the leader half way through the linearizable history.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    /// Its process is killed (SIGKILL), and started again three quarters through.
    Kill,
    /// It is cut off from the other nodes, and joined to them again three quarters through.
    Cut,
    /// Its plane hears the other nodes no more, while they hear it and every QUIC path passes, and
    /// hears them again three quarters through.
    Deaf,
}

fn linearizable(nodes: u64, fault: Fault) {
    let network = match fault {
        Fault::Kill => Network::Direct,
        Fault::Cut | Fault::Deaf => Network::Relayed,
    };
    let mut cell = Cell::new(nodes, network);
    let leader = cell.leader(GROUP);
    cell.open_bucket();
    // Puts enough to pass the log group's room in all, so the history crosses compactions.
    let ops = (2 * (GROUP_ENTRIES + 1)).div_ceil(CLIENTS);
    let (threads, stops) = cell.start_workload(Plan {
        group: GROUP,
        ops,
        phase: 0,
        clock: Clock::Host(Instant::now()),
    });
    cell.done_by(ops, 2);
    match fault {
        Fault::Kill => {
            cell.kill(leader);
            cell.done_by(ops, 3);
            cell.start(leader);
            cell.introduce();
            cell.tell_workload(leader);
        }
        Fault::Cut => {
            cell.isolate(leader, true);
            cell.done_by(ops, 3);
            cell.isolate(leader, false);
        }
        Fault::Deaf => {
            cell.deafen_plane(leader, true);
            cell.done_by(ops, 3);
            cell.deafen_plane(leader, false);
        }
    }
    cell.done_by(ops, 4);
    drop(stops);
    check_histories(threads.into_iter().flat_map(|t| t.join().unwrap()));
    // Every node, the one killed or cut off among them, ends where the others do.
    cell.converged(GROUP);
}

#[test]
fn puts_and_reads_through_a_leader_killed_and_started_again_are_linearizable() {
    linearizable(NODES, Fault::Kill);
}

#[test]
fn puts_and_reads_through_a_leader_cut_off_are_linearizable() {
    linearizable(NODES, Fault::Cut);
}

/// The leader's plane goes deaf: it suspects its followers, which hear it and trust it, and steps
/// down; the others elect one of themselves, which it follows by its lanes.
#[test]
fn puts_and_reads_through_a_leader_whose_plane_goes_deaf_are_linearizable() {
    linearizable(NODES, Fault::Deaf);
}

#[test]
fn puts_and_reads_through_a_leader_of_five_voters_killed_and_started_again_are_linearizable() {
    linearizable(MOST_NODES, Fault::Kill);
}

#[test]
fn puts_and_reads_through_a_leader_cut_off_from_five_voters_are_linearizable() {
    linearizable(MOST_NODES, Fault::Cut);
}

/// A follower's device fails at rest: while it is down its engine's store is overwritten where
/// both its superblocks lie. Started again, the node refuses the store with a typed corruption
/// error rather than serve from it (CLAUDE.md §6). It is lost for good, and a spare under an
/// identity never used replaces it (replica.md §6); then as many original voters are killed as
/// leave the range a bare quorum, so every quorum counts the spare. The history through both
/// phases is linearizable: no acknowledged write is lost.
fn damaged_and_replaced(voters: u64) {
    let mut cell = Cell::shaped(Shape {
        voters,
        spares: 1,
        network: Network::Direct,
        stored: false,
        ranges: NAME_RANGE,
        gateway: false,
        serving: false,
    });
    let spare = voters + 1;
    let leader = cell.leader(GROUP);
    cell.open_bucket();
    let ops = (2 * (GROUP_ENTRIES + 1)).div_ceil(CLIENTS);
    let start = Instant::now();
    let mut history = Vec::new();
    let (threads, stops) = cell.start_workload(Plan {
        group: GROUP,
        ops,
        phase: 0,
        clock: Clock::Host(start),
    });
    cell.done_by(ops, 1);
    let damaged = (1..=voters).find(|id| *id != leader).unwrap();
    cell.kill(damaged);
    cell.damage_store(damaged);
    let said = cell.start_damaged(damaged);
    assert!(said.contains("corruption"), "node {damaged} said: {said}");
    cell.start(spare);
    cell.introduce();
    cell.tell_workload(spare);
    cell.replace(damaged, spare);
    cell.until(|c| {
        if c.replaced.contains(&(GROUP, damaged, spare)) {
            Some(())
        } else {
            None
        }
    });
    cell.done_by(ops, 4);
    drop(stops);
    for thread in threads {
        history.extend(thread.join().unwrap());
    }
    // Of the voters now, a bare quorum is left, the spare in it.
    let originals: Vec<u64> = (1..=voters).filter(|id| *id != damaged).collect();
    for id in originals.iter().take(usize::try_from(voters / 2).unwrap()) {
        cell.kill(*id);
    }
    cell.progress.clear();
    let (threads, stops) = cell.start_workload(Plan {
        group: GROUP,
        ops,
        phase: 1,
        clock: Clock::Host(start),
    });
    cell.done_by(ops, 4);
    drop(stops);
    for thread in threads {
        history.extend(thread.join().unwrap());
    }
    check_histories(history.into_iter());
    cell.converged(GROUP);
}

#[test]
fn a_node_whose_store_is_damaged_refuses_it_and_is_replaced_with_no_acknowledged_write_lost() {
    damaged_and_replaced(NODES);
}

#[test]
fn a_node_of_five_voters_whose_store_is_damaged_is_replaced_with_no_acknowledged_write_lost() {
    damaged_and_replaced(MOST_NODES);
}

#[test]
fn chunks_a_node_acknowledged_read_back_whole_after_it_is_killed_mid_put_and_started_again() {
    const KILLED: u64 = 2;
    let mut cell = Cell::stored(NODES);
    let volumes: BTreeMap<u64, u128> = (1..=NODES).map(|id| (id, cell.volume(id))).collect();
    // A round puts one chunk on each node, the rounds in the lengths' order.
    let puts: Vec<(u64, ChunkKey, Vec<u8>)> = (0..CHUNK_LENS.len() as u64 * NODES)
        .map(|n| {
            let node = 1 + n % NODES;
            let len = CHUNK_LENS[usize::try_from(n / NODES).unwrap()];
            (node, chunk_key(n), chunk_bytes(n, len))
        })
        .collect();
    let put = |(node, key, bytes): &(u64, ChunkKey, Vec<u8>)| {
        Chunked::put(*node, volumes[node], *key, bytes.clone())
    };
    let whole = |(node, key, bytes): &(u64, ChunkKey, Vec<u8>)| {
        Chunked::Get(*node, volumes[node], *key, (0, bytes.len() as u64))
    };
    let thread = cell.start_gateway(puts.iter().map(put).collect());
    // Killed once the gateway has done half the rounds and the next round's first put: the killed
    // node's put of that round is out, or about to go, or done.
    let asked = puts.len() as u64 / 2 + 1;
    cell.gateway_done(asked);
    cell.kill(KILLED);
    cell.gateway_done(puts.len() as u64);
    let outcomes = thread.join().unwrap();
    // Every put to a node running is stored; the killed node's are stored until it is killed, the
    // ones done before the kill was asked among them, and lost after.
    let mut lost = Vec::new();
    for (n, ((node, ..), outcome)) in puts.iter().zip(&outcomes).enumerate() {
        match outcome {
            ChunkOutcome::Stored if *node != KILLED || lost.is_empty() => {}
            ChunkOutcome::Lost(_) if *node == KILLED && n as u64 >= asked => lost.push(n),
            other => panic!("put {n} to node {node}: {}", brief(other)),
        }
    }
    // Down, the killed node is not reached; its peers' chunks read back whole from each.
    let running: Vec<_> = puts.iter().filter(|(node, ..)| *node != KILLED).collect();
    let mut ops: Vec<Chunked> = running.iter().copied().map(whole).collect();
    let mut want: Vec<ChunkOutcome> = running
        .iter()
        .map(|(_, _, bytes)| ChunkOutcome::Read(bytes.clone()))
        .collect();
    ops.push(put(&(KILLED, chunk_key(1000), chunk_bytes(1000, 5000))));
    want.push(ChunkOutcome::Lost(Refusal::NotConnected));
    outcomes_are(&cell.through_gateway(ops), &want);
    // Started again on its directory, it holds the same volume; every chunk acknowledged reads
    // back whole, and a lost put landed whole or not at all.
    cell.start(KILLED);
    cell.introduce();
    assert_eq!(cell.volume(KILLED), volumes[&KILLED]);
    let got = cell.through_gateway(puts.iter().map(whole).collect());
    for (n, ((_, _, bytes), outcome)) in puts.iter().zip(&got).enumerate() {
        let read = *outcome == ChunkOutcome::Read(bytes.clone());
        let missing = *outcome == ChunkOutcome::Refused(Status::NotFound) && lost.contains(&n);
        assert!(read || missing, "get {n}: {}", brief(outcome));
    }
    // Each lost put retried is stored, landed or not: a retry's bytes are the first's
    // (chunk-store's audit B05). Then every chunk reads back whole, and in part across its
    // checksum blocks; and each refusal is the volume's or the node's own, typed.
    let mut ops: Vec<Chunked> = lost.iter().map(|n| put(&puts[*n])).collect();
    let mut want = vec![ChunkOutcome::Stored; lost.len()];
    ops.extend(puts.iter().map(whole));
    want.extend(
        puts.iter()
            .map(|(_, _, bytes)| ChunkOutcome::Read(bytes.clone())),
    );
    for (node, key, bytes) in puts.iter().filter(|(_, _, b)| b.len() == 1 << 20) {
        ops.push(Chunked::Get(*node, volumes[node], *key, (4095, 300_001)));
        want.push(ChunkOutcome::Read(bytes[4095..4095 + 300_001].to_vec()));
    }
    let (node, key, bytes) = &puts[0];
    let changed = mantle_crc::crc32c(bytes) ^ 1;
    ops.push(Chunked::Put(
        *node,
        volumes[node],
        chunk_key(1001),
        bytes.clone(),
        changed,
    ));
    want.push(ChunkOutcome::Refused(Status::Checksum));
    ops.push(Chunked::Get(*node, volumes[node], chunk_key(1001), (0, 1)));
    want.push(ChunkOutcome::Refused(Status::NotFound));
    ops.push(Chunked::Get(
        *node,
        volumes[node],
        *key,
        (bytes.len() as u64, 1),
    ));
    want.push(ChunkOutcome::Refused(Status::Range));
    ops.push(put(&(*node, chunk_key(1002), bytes.clone())));
    want.push(ChunkOutcome::Stored);
    ops.push(Chunked::put(
        *node,
        volumes[&KILLED],
        chunk_key(1003),
        bytes.clone(),
    ));
    want.push(ChunkOutcome::Refused(Status::NoVolume));
    outcomes_are(&cell.through_gateway(ops), &want);
}

#[test]
fn a_cell_hosts_a_range_of_each_layer_one_voter_a_node_and_each_serves_through_a_kill() {
    let mut cell = Cell::layered(NODES);
    for (group, _) in EVERY_LAYER {
        cell.leader(*group);
    }
    // A session with each range, and a Name command in each: the Name range applies it, every other
    // answers that it holds another layer's rows.
    let sessions = |answered: &[Vec<Answer>]| -> Vec<u64> {
        answered
            .iter()
            .map(|answers| match answers.as_slice() {
                [Answer::Registered { session }] => *session,
                other => panic!("a registration answered {other:?}"),
            })
            .collect()
    };
    let registered = cell.asked(
        (1..)
            .zip(EVERY_LAYER)
            .map(|(n, (group, _))| (*group, registration(CLIENT, 1, n)))
            .collect(),
    );
    let opened = cell.asked(
        EVERY_LAYER
            .iter()
            .zip(sessions(&registered))
            .map(|((group, _), session)| (*group, sessioned(session, 1, open_gate())))
            .collect(),
    );
    for ((group, layer), answers) in EVERY_LAYER.iter().zip(&opened) {
        let want = if *group == GROUP {
            Answer::Name(mantle_meta::name::Outcome::GateMoved)
        } else {
            Answer::WrongLayer
        };
        assert_eq!(answers, &vec![want], "the {layer} range");
    }
    // The root range's volume table, empty in a cell no node of which keeps a volume, and a read
    // of the Name layer's rows asked of it, refused.
    let read = cell.read(vec![
        (ROOT, Query::Volumes { after: None }),
        (
            ROOT,
            Query::Current {
                bucket: "b".into(),
                key: "k".into(),
            },
        ),
    ]);
    assert!(
        matches!(
            read.as_slice(),
            [
                Ok(Read::Volumes { volumes, more: false }),
                Err(ClientError::Status(CommandStatus::WrongLayer)),
            ] if volumes.is_empty()
        ),
        "{read:?}"
    );
    // The node leading the most ranges killed: each range has a quorum of its voters, one to a
    // node, and serves on; started again, the node catches up on each.
    let mut leaders: BTreeMap<u64, usize> = BTreeMap::new();
    for (group, _) in EVERY_LAYER {
        *leaders.entry(cell.leader(*group)).or_default() += 1;
    }
    let killed = leaders
        .iter()
        .max_by_key(|(_, led)| **led)
        .map(|(id, _)| *id)
        .unwrap();
    cell.kill(killed);
    let again = cell.asked(
        (10..)
            .zip(EVERY_LAYER)
            .map(|(n, (group, _))| (*group, registration(CLIENT, 1, n)))
            .collect(),
    );
    sessions(&again);
    cell.start(killed);
    cell.introduce();
    for (group, _) in EVERY_LAYER {
        cell.converged(*group);
    }
}

#[test]
fn each_node_enlists_its_volume_and_placement_offers_one_to_a_domain() {
    let mut cell = Cell::layered_stored(NODES);
    for id in 1..=NODES {
        cell.enlisted(id);
    }
    let volumes: BTreeMap<u64, u128> = (1..=NODES).map(|id| (id, cell.volume(id))).collect();
    let table = cell.table();
    assert_eq!(table.len(), volumes.len(), "{table:?}");
    for placed in &table {
        assert_eq!(volumes[&placed.node], placed.volume);
        assert_eq!(placed.domain, format!("n{}", placed.node));
        assert!(placed.bytes > 0);
    }
    // Every block is offered a volume of each node, each its own domain, before any other.
    let node_of: BTreeMap<u128, u64> = table.iter().map(|p| (p.volume, p.node)).collect();
    for n in 0..1_000u128 {
        let block = n.wrapping_mul(0x9E37_79B9_7F4A_7C15_F39C_C060_5CED_C834);
        let order = place::order(block, &table);
        let first: BTreeSet<u64> = order[..volumes.len()].iter().map(|v| node_of[v]).collect();
        assert_eq!(first.len(), volumes.len(), "block {block:x}: {order:?}");
    }
    // Started again, a node enlists again, and the table holds what it held.
    cell.kill(2);
    cell.start(2);
    cell.introduce();
    cell.enlisted(2);
    assert_eq!(cell.table(), table);
    // Started in another domain, a node is refused, and ends naming the range's answer; in its
    // own domain again, it enlists.
    cell.kill(3);
    cell.domains.insert(3, "elsewhere".into());
    let said = cell.start_refused(3);
    assert!(
        said.contains("refused the node's enlistment") && said.contains("Conflict"),
        "{said}"
    );
    cell.domains.remove(&3);
    cell.start(3);
    cell.introduce();
    cell.enlisted(3);
    assert_eq!(cell.table(), table);
}

#[test]
fn objects_round_trip_through_any_node_and_read_back_after_one_is_killed() {
    let mut cell = Cell::gateways(NODES);
    for id in 1..=NODES {
        cell.enlisted(id);
    }
    let made = cell.objects(vec![(1, Op::Create { bucket: "b".into() }, Vec::new())]);
    done(&made[0], "the bucket");
    // Lengths either side of a sealed segment's 64 KiB and across blocks of 8 MiB chunks.
    let lens = [0usize, 1, 65_535, 65_537, 3 << 20, 20 << 20];
    let objects: Vec<(String, Vec<u8>)> = (0u64..)
        .zip(lens)
        .map(|(n, len)| (format!("o{n}"), object_bytes(n, len)))
        .collect();
    let puts = objects
        .iter()
        .enumerate()
        .map(|(n, (key, data))| {
            let op = Op::Put {
                bucket: "b".into(),
                key: key.clone(),
                id: n as u128 + 1,
                length: data.len() as u64,
            };
            (1 + n as u64 % NODES, op, data.clone())
        })
        .collect();
    for (outcome, (key, _)) in cell.objects(puts).iter().zip(&objects) {
        done(outcome, &format!("the put of {key}"));
    }
    // Each read back whole through another node than its put's, and its middle third.
    let reads = |through: &dyn Fn(usize) -> u64| -> Vec<(u64, Op, Vec<u8>)> {
        let mut ops = Vec::new();
        for (n, (key, data)) in objects.iter().enumerate() {
            let get = |offset: u64, len: u64| Op::Get {
                bucket: "b".into(),
                key: key.clone(),
                offset,
                len,
            };
            ops.push((through(n), get(0, u64::MAX), Vec::new()));
            let third = data.len() as u64 / 3;
            if third > 0 {
                ops.push((through(n), get(third, third), Vec::new()));
            }
        }
        ops
    };
    let check = |got: Vec<ObjectOutcome>, how: &str| {
        let mut got = got.iter();
        for (key, data) in &objects {
            assert!(
                done(got.next().unwrap(), &format!("{key} {how}")) == *data,
                "{key} {how}: other bytes"
            );
            let third = data.len() / 3;
            if third > 0 {
                let part = done(got.next().unwrap(), &format!("{key}'s third {how}"));
                assert!(part == data[third..2 * third], "{key}'s third {how}");
            }
        }
    };
    let got = cell.objects(reads(&|n| 1 + (n as u64 + 1) % NODES));
    check(got, "through another node");
    // Each block's chunks on distinct nodes.
    let node_of: BTreeMap<u128, u64> = cell.table().iter().map(|p| (p.volume, p.node)).collect();
    for (key, data) in &objects {
        let current = cell.read(vec![(
            GROUP,
            Query::Current {
                bucket: "b".into(),
                key: key.clone(),
            },
        )]);
        let [Ok(Read::Current(Some((_, version))))] = current.as_slice() else {
            panic!("{key}: {current:?}");
        };
        let Some(file) = version.file else {
            assert!(data.is_empty(), "{key} has bytes and no file");
            continue;
        };
        let extents = cell.read(vec![(
            FILES,
            Query::Extents {
                file,
                offset: 0,
                max: 16,
            },
        )]);
        let [Ok(Read::Extents(extents))] = extents.as_slice() else {
            panic!("{key}'s extents: {extents:?}");
        };
        for (_, extent) in extents {
            let mantle_meta::record::Target::Block(block) = extent.target else {
                panic!("{key}: an extent of a file");
            };
            let read = cell.read(vec![(BLOCKS, Query::Block { block })]);
            let [Ok(Read::Block(Some((_, places))))] = read.as_slice() else {
                panic!("{key}'s block: {read:?}");
            };
            let nodes: BTreeSet<u64> = places.iter().map(|p| node_of[&p.volume]).collect();
            assert_eq!(
                nodes.len(),
                places.len(),
                "{key}'s block {block:x}: {places:?}"
            );
        }
    }
    // Each node in turn killed: every object reads back through the other two; started again,
    // it serves them too.
    for killed in 1..=NODES {
        cell.kill(killed);
        let others: Vec<u64> = (1..=NODES).filter(|id| *id != killed).collect();
        let got = cell.objects(reads(&|n| others[n % others.len()]));
        check(got, &format!("with node {killed} killed"));
        cell.start(killed);
        cell.introduce();
        cell.enlisted(killed);
        let got = cell.objects(reads(&|_| killed));
        check(got, &format!("through node {killed} started again"));
    }
}
