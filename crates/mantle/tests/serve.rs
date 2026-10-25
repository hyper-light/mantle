//! Real processes run as `mantle serve` from their operators' statements alone: a cell of three on
//! loopback, each node measuring what it needs of its host and its device, deriving every other
//! value (`mantle_node::derive::derive`) and saying each with its basis before it listens, then
//! serving as `mantle node` does. Asked: every key `mantle node`'s configuration names is said once
//! with its basis, each device measured at its node's first start; the cell enlists its volumes and
//! takes and returns objects; a node killed and started again takes its kept profile and its
//! stores' and volume's shapes, says the same values, and every object reads back through it.
//!
//! A binary of its own (crates/mantle/tests/support/cell.rs's harness, node.rs's watch): a process
//! states its message bounds once, and these nodes derive theirs, which its clients state the same.

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
#[allow(dead_code)]
#[path = "../../node/tests/support/relay.rs"]
mod relay;
use cell::*;
use std::process::{Command, Stdio};

/// The peers a node of a stated cell names: the other nodes, the clients and the gateway.
fn stated_peers() -> usize {
    usize::try_from(NODES - 1 + CLIENTS + 1 + 1).unwrap()
}

/// The value `bases` say for `key`, said once.
fn said<'a>(bases: &'a [Basis], key: &str) -> &'a Basis {
    let found: Vec<&Basis> = bases.iter().filter(|b| b.key == key).collect();
    assert_eq!(found.len(), 1, "{key}: {found:?}");
    found[0]
}

/// Every key `mantle node`'s configuration names said once by node `id`, its volume's as its first
/// volume's (`volume.1.`), and each line it repeats as often as its file states it, each value of
/// a kind a basis takes and with its reason.
fn every_key_said(id: u64, bases: &[Basis], peers: usize) {
    for key in mantle_node::process::KEYS {
        let key = key
            .strip_prefix("volume.")
            .map_or_else(|| (*key).to_owned(), |field| format!("volume.1.{field}"));
        let basis = said(bases, &key);
        assert!(
            ["stated", "measured", "derived", "cited", "bound"].contains(&basis.kind.as_str()),
            "node {id}: {basis:?}"
        );
        assert!(
            !basis.value.is_empty() && !basis.why.is_empty(),
            "node {id}: {basis:?}"
        );
    }
    for key in ["dir", "size", "depth", "read_bytes", "read_gap", "prewrite"] {
        said(bases, &format!("volume.1.{key}"));
    }
    // Its peers; the cell's five ranges; its root key.
    for (key, lines) in [
        ("peer", peers),
        ("range", EVERY_LAYER.len()),
        ("root_key", 1),
    ] {
        let count = bases.iter().filter(|b| b.key == key).count();
        assert_eq!(count, lines, "node {id}: {key}");
    }
}

#[test]
fn a_cell_served_from_stated_keys_alone_says_each_values_basis_and_serves_objects() {
    let mut cell = Cell::served(NODES);
    for id in 1..=NODES {
        cell.enlisted(id);
    }
    // Each node said every value it runs with, its device measured at this first start.
    let first = cell.bases.clone();
    assert_eq!(first.len(), usize::try_from(NODES).unwrap());
    for basis in &first[&1] {
        eprintln!("node 1: {} = {} ({})", basis.key, basis.value, basis.kind);
    }
    for (id, bases) in &first {
        every_key_said(*id, bases, stated_peers());
        let depth = said(bases, "volume.1.depth");
        assert!(depth.why.contains("measured now"), "node {id}: {depth:?}");
        assert_eq!(said(bases, "id").value, id.to_string());
        assert_eq!(
            said(bases, "volume.1.size").value,
            OBJECT_VOLUME_BYTES.to_string()
        );
        assert_eq!(
            said(bases, "rules.max_sessions").value,
            (NODES + CLIENTS + 2).to_string()
        );
    }

    let made = cell.objects(vec![(
        1,
        Op::Create {
            bucket: "b".into(),
            id: 1,
        },
        Vec::new(),
    )]);
    done(&made[0], "the bucket");
    let objects: Vec<(String, Vec<u8>)> = (0u64..)
        .zip([0usize, 1, 65_537, 3 << 20])
        .map(|(n, len)| (format!("o{n}"), object_bytes(n, len)))
        .collect();
    let puts = objects
        .iter()
        .enumerate()
        .map(|(n, (key, data))| {
            let op = put_op("b", key, n as u128 + 1, data.len() as u64);
            (1 + n as u64 % NODES, op, data.clone())
        })
        .collect();
    for (outcome, (key, _)) in cell.objects(puts).iter().zip(&objects) {
        done(outcome, &format!("the put of {key}"));
    }

    // Node 1 killed and started again: its profile kept, its stores' and volume's shapes theirs,
    // every value it runs with the same; every object reads back through it.
    cell.kill(1);
    cell.start(1);
    cell.introduce();
    cell.enlisted(1);
    let again = &cell.bases[&1];
    every_key_said(1, again, stated_peers());
    let depth = said(again, "volume.1.depth");
    assert!(depth.why.contains("kept"), "{depth:?}");
    assert!(
        said(again, "engine.page_size")
            .why
            .contains("stores were made")
    );
    assert!(
        said(again, "volume.1.segment_bytes")
            .why
            .contains("laid out")
    );
    let values = |bases: &[Basis]| {
        let mut by_key: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for basis in bases {
            by_key
                .entry(basis.key.clone())
                .or_default()
                .push(basis.value.clone());
        }
        by_key
    };
    assert_eq!(values(again), values(&first[&1]));
    let gets = objects
        .iter()
        .map(|(key, _)| (1, get_op("b", key, None), Vec::new()))
        .collect();
    for (outcome, (key, data)) in cell.objects(gets).iter().zip(&objects) {
        assert!(done(outcome, key) == *data, "{key}: other bytes");
    }
}

/// Puts objects of a byte to 3 MiB through `through` in turn, into a bucket made through the first,
/// and reads each back whole through `back`.
fn objects_round_trip(cell: &mut Cell, through: &[u64], back: &dyn Fn(usize) -> u64) {
    let made = cell.objects(vec![(
        through[0],
        Op::Create {
            bucket: "b".into(),
            id: 1,
        },
        Vec::new(),
    )]);
    done(&made[0], "the bucket");
    let objects: Vec<(String, Vec<u8>)> = (0u64..)
        .zip([0usize, 1, 65_537, 3 << 20])
        .map(|(n, len)| (format!("o{n}"), object_bytes(n, len)))
        .collect();
    let puts = objects
        .iter()
        .enumerate()
        .map(|(n, (key, data))| {
            let op = put_op("b", key, n as u128 + 1, data.len() as u64);
            (through[n % through.len()], op, data.clone())
        })
        .collect();
    for (outcome, (key, _)) in cell.objects(puts).iter().zip(&objects) {
        done(outcome, &format!("the put of {key}"));
    }
    let gets = objects
        .iter()
        .enumerate()
        .map(|(n, (key, _))| (back(n), get_op("b", key, None), Vec::new()))
        .collect();
    for (outcome, (key, data)) in cell.objects(gets).iter().zip(&objects) {
        assert!(done(outcome, key) == *data, "{key}: other bytes");
    }
}

/// `mantle init` makes a cell of one from one `--node`: served from what init wrote alone, the node
/// says every key's basis, takes the device profile init measured, enlists its volume, and takes
/// and returns objects.
#[test]
fn a_cell_of_one_made_by_init_serves_from_what_init_wrote() {
    let mut cell = Cell::made(|root| {
        let data = root.join("data");
        std::fs::create_dir(&data).unwrap();
        vec![
            "--node".into(),
            format!(
                "quic=127.0.0.1:0,plane=127.0.0.1:0,domain=laptop,volume={}:256M",
                data.display()
            ),
            "--max-nodes".into(),
            "1".into(),
            "--max-volumes".into(),
            "1".into(),
        ]
    });
    let id = *cell.dirs.keys().next().unwrap();
    cell.enlisted(id);
    let bases = cell.bases[&id].clone();
    // Its peers are the clients the test pinned.
    every_key_said(id, &bases, usize::try_from(CLIENTS + 1).unwrap());
    let depth = said(&bases, "volume.1.depth");
    assert!(depth.why.contains("kept"), "{depth:?}");
    assert_eq!(said(&bases, "voters").value, id.to_string());
    objects_round_trip(&mut cell, &[id], &|_| id);
}

/// Waits until every node of `others` names one leader of `group`, one of them: a stopped node's
/// range led on past its stop, by the voter it was handed to or one the others elected since, as
/// Raft lets them once the stopping node is gone.
fn led_on(cell: &mut Cell, group: u128, others: &[u64]) {
    cell.until(|c| {
        let named: Option<BTreeSet<u64>> = others
            .iter()
            .map(|other| match c.status.get(&(*other, group)) {
                Some(Line::Status { leader, .. }) => Some(*leader),
                _ => None,
            })
            .collect();
        named.filter(|named| named.len() == 1 && named.iter().all(|l| others.contains(l)))
    });
}

/// `mantle init`'s arguments for a cell of three under `root`: a machines file's two founders and a
/// `--node`'s, each with a volume of its own in a failure domain of its own.
fn three_founders(root: &Path) -> Vec<String> {
    let dirs: Vec<PathBuf> = (1..=3)
        .map(|n| {
            let dir = root.join(format!("d{n}"));
            std::fs::create_dir(&dir).unwrap();
            dir
        })
        .collect();
    let file = root.join("machines.conf");
    let mut text = String::from("max_nodes = 3\nmax_volumes = 3\n");
    for (n, dir) in dirs.iter().take(2).enumerate() {
        text.push_str(&format!(
            "[node]\nquic = 127.0.0.1:0\nplane = 127.0.0.1:0\ndomain = rack-{n}\nvolume = {} 256M\n",
            dir.display()
        ));
    }
    std::fs::write(&file, text).unwrap();
    vec![
        "--founders".into(),
        file.display().to_string(),
        "--node".into(),
        format!(
            "quic=127.0.0.1:0,plane=127.0.0.1:0,domain=rack-2,volume={}:256M",
            dirs[2].display()
        ),
    ]
}

/// The ranges node `id` leads, as it last said.
fn led(cell: &Cell, id: u64) -> Vec<u128> {
    cell.status
        .iter()
        .filter(|((node, _), line)| {
            *node == id && matches!(line, Line::Status { leader, .. } if *leader == id)
        })
        .map(|((_, group), _)| *group)
        .collect()
}

/// A node asked to stop (node.md §1.7: SIGTERM; a `stop` line on Windows, `Cell::stop`) hands each
/// range it leads to a member caught up with it, which then leads; says so of each as it begins
/// and once it is done, closes its connections and exits 0. The two others serve on:
/// every object acknowledged before the stop reads back whole through them, and the ranges, led by
/// them, take a bucket made through them. (A new object's chunks go to every node's volume, the
/// stopped one's too, which the volume table still holds.)
#[test]
fn a_node_asked_to_stop_hands_its_ranges_over_and_exits_well() {
    let mut cell = Cell::made(three_founders);
    let ids: Vec<u64> = cell.dirs.keys().copied().collect();
    for &id in &ids {
        cell.enlisted(id);
    }
    let made = cell.objects(vec![(
        ids[0],
        Op::Create {
            bucket: "b".into(),
            id: 1,
        },
        Vec::new(),
    )]);
    done(&made[0], "the bucket");
    let bytes = |n: usize| object_bytes(u64::try_from(n).unwrap(), 65_537);
    let put = |n: usize, through: u64| {
        let data = bytes(n);
        let op = put_op(
            "b",
            &format!("o{n}"),
            u128::try_from(n).unwrap() + 1,
            data.len() as u64,
        );
        (through, op, data)
    };
    let get = |n: usize, through: u64| {
        let op = get_op("b", &format!("o{n}"), None);
        (through, op, Vec::new())
    };
    let before: Vec<usize> = (0..3).collect();
    let puts = before.iter().map(|&n| put(n, ids[n % ids.len()])).collect();
    for (outcome, n) in cell.objects(puts).iter().zip(&before) {
        done(outcome, &format!("the put of o{n}"));
    }
    let stopping = *ids.iter().max_by_key(|id| led(&cell, **id).len()).unwrap();
    assert!(
        !led(&cell, stopping).is_empty(),
        "node {stopping} leads no range"
    );
    let exited = cell.stop(stopping);
    assert!(exited.success(), "node {stopping} exited {exited}");
    assert!(
        cell.stopped.contains(&stopping),
        "node {stopping} said no stop"
    );
    // The ranges it led as its stop began, as it said: each handed to another voter, then led by
    // one the others all name, the voter it was handed to or one they elected since.
    let ranges: BTreeSet<u128> = cell
        .handing
        .iter()
        .filter(|(node, ..)| *node == stopping)
        .map(|(_, group, _)| *group)
        .collect();
    assert!(
        !ranges.is_empty(),
        "node {stopping} began to hand no range over"
    );
    let others: Vec<u64> = ids.iter().copied().filter(|id| *id != stopping).collect();
    for group in &ranges {
        let to = cell
            .handed
            .iter()
            .find(|(node, handed, _)| *node == stopping && handed == group)
            .map(|(_, _, to)| *to)
            .unwrap_or_else(|| panic!("node {stopping} handed {group:x} to no one"));
        assert!(others.contains(&to), "{group:x} handed to {to}");
        led_on(&mut cell, *group, &others);
    }
    let gets = before
        .iter()
        .map(|&n| get(n, others[n % others.len()]))
        .collect();
    for (outcome, n) in cell.objects(gets).iter().zip(&before) {
        assert_eq!(done(outcome, &format!("o{n}")), bytes(*n), "o{n} read back");
    }
    let made = cell.objects(vec![(
        others[0],
        Op::Create {
            bucket: "c".into(),
            id: 1,
        },
        Vec::new(),
    )]);
    done(&made[0], "a bucket made after the stop");
}

/// A node's failure detectors start from the cell's history (node.md §2.4, item 7), as each
/// process says at its start: the cell's node time run and crashes, and its own part, which its
/// directory and the root range keep. A cell's first starts hold none; a node stopped in order and
/// started again counts no crash and the time it ran; one killed and started again counts one, and
/// the cell's history holds at least its own.
#[test]
fn a_node_started_again_starts_its_detectors_from_the_cells_history() {
    let mut cell = Cell::made(three_founders);
    let ids: Vec<u64> = cell.dirs.keys().copied().collect();
    for &id in &ids {
        let (whole, own) = cell.history(id);
        assert_eq!(own.crashes, 0, "node {id}'s first run");
        assert_eq!(own.exposure, Duration::ZERO, "node {id}'s first run");
        assert_eq!(whole.crashes, 0, "node {id}'s first run");
        cell.enlisted(id);
    }
    let (stopped, killed) = (ids[0], ids[1]);
    let exited = cell.stop(stopped);
    assert!(exited.success(), "node {stopped} exited {exited}");
    cell.start(stopped);
    let (whole, own) = cell.history(stopped);
    assert_eq!(own.crashes, 0, "node {stopped} stopped in order");
    assert!(own.exposure > Duration::ZERO, "node {stopped}'s time run");
    assert!(
        whole.exposure >= own.exposure && whole.crashes >= own.crashes,
        "the cell's history {whole:?} holds node {stopped}'s {own:?}"
    );
    cell.introduce();
    cell.kill(killed);
    cell.start(killed);
    let (whole, own) = cell.history(killed);
    assert_eq!(own.crashes, 1, "node {killed} killed once");
    assert!(
        whole.crashes >= own.crashes,
        "the cell's history {whole:?} holds node {killed}'s {own:?}"
    );
}

/// A cell of three made by `mantle init` rolls one node at a time, highest id first as a
/// StatefulSet rolls (Kubernetes' rolling update): each node stops in order and exits 0, every range
/// it led as its stop began (`handing`) handed to a voter whose own status then says it leads, and
/// while it is down every object
/// acknowledged before the roll reads back through the other two; it starts again from its own
/// directories, enlists with the volume it held, and every range applies as far on it as on the
/// others before the next stops. From the moment the other members name each handed range's new
/// leader until that, none of them says any range has no leader. After the last, every object
/// reads back through every node, and the cell takes a new one.
#[test]
fn a_cell_made_by_init_rolls_one_node_at_a_time_and_reads_every_object_back() {
    let mut cell = Cell::made(three_founders);
    let mut ids: Vec<u64> = cell.dirs.keys().copied().collect();
    for &id in &ids {
        cell.enlisted(id);
    }
    let volumes: BTreeMap<u64, u128> = ids.iter().map(|&id| (id, cell.volume(id))).collect();
    let made = cell.objects(vec![(
        ids[0],
        Op::Create {
            bucket: "b".into(),
            id: 1,
        },
        Vec::new(),
    )]);
    done(&made[0], "the bucket");
    let bytes = |n: usize| object_bytes(u64::try_from(n).unwrap(), 65_537);
    let put = |n: usize, through: u64| {
        let data = bytes(n);
        let op = put_op(
            "b",
            &format!("o{n}"),
            u128::try_from(n).unwrap() + 1,
            data.len() as u64,
        );
        (through, op, data)
    };
    let get = |n: usize, through: u64| {
        let op = get_op("b", &format!("o{n}"), None);
        (through, op, Vec::new())
    };
    let before: Vec<usize> = (0..ids.len()).collect();
    let puts = before.iter().map(|&n| put(n, ids[n])).collect();
    for (outcome, n) in cell.objects(puts).iter().zip(&before) {
        done(outcome, &format!("the put of o{n}"));
    }
    let groups: BTreeSet<u128> = cell.status.keys().map(|(_, group)| *group).collect();
    ids.sort_unstable_by(|a, b| b.cmp(a));
    for &stopping in &ids {
        let exited = cell.stop(stopping);
        assert!(exited.success(), "node {stopping} exited {exited}");
        assert!(
            cell.stopped.remove(&stopping),
            "node {stopping} said no stop"
        );
        // The ranges it led as its stop began, as it said: each handed to another voter.
        let ranges: BTreeSet<u128> = cell
            .handing
            .iter()
            .filter(|(node, ..)| *node == stopping)
            .map(|(_, group, _)| *group)
            .collect();
        let others: Vec<u64> = ids.iter().copied().filter(|id| *id != stopping).collect();
        for group in &ranges {
            let to = cell
                .handed
                .iter()
                .rev()
                .find(|(node, handed, _)| *node == stopping && handed == group)
                .map(|(_, _, to)| *to)
                .unwrap_or_else(|| panic!("node {stopping} handed {group:x} to no one"));
            assert!(others.contains(&to), "{group:x} handed to {to}");
        }
        // The watch begins once every remaining member names each range's leader, handed or not,
        // the moment the step's criterion starts from: a follower that lost sight of its leader
        // before the stop, as a starved one may without any election, names it again first (on
        // one CPU a member said no leader 330 ms before the stop and named it 105 ms after; the
        // stopped node had led none, so no handed range was waited on, and that stale status was
        // counted after the watch began).
        for group in &groups {
            led_on(&mut cell, *group, &others);
        }
        cell.leaderless.clear();
        let gets = before
            .iter()
            .flat_map(|&n| others.iter().map(move |&through| (n, through)))
            .collect::<Vec<_>>();
        let read = cell.objects(gets.iter().map(|&(n, through)| get(n, through)).collect());
        for (outcome, (n, through)) in read.iter().zip(&gets) {
            assert_eq!(
                done(outcome, &format!("o{n} through {through}")),
                bytes(*n),
                "o{n} read back through {through} with {stopping} stopped"
            );
        }
        // A founder stated at port 0 listens elsewhere once started again, and its peers, which
        // learn where on standard input, are told (node.md §1.7).
        cell.start(stopping);
        cell.introduce();
        cell.enlisted(stopping);
        assert_eq!(
            cell.volume(stopping),
            volumes[&stopping],
            "node {stopping} started again with another volume"
        );
        for group in &groups {
            cell.converged(*group);
        }
        let said: Vec<(u64, u128)> = cell
            .leaderless
            .iter()
            .copied()
            .filter(|(node, _)| *node != stopping)
            .collect();
        assert!(
            said.is_empty(),
            "with node {stopping} stopped and started again, nodes said ranges had no leader: \
             {said:x?}"
        );
    }
    let after = before.len();
    let added = cell.objects(vec![put(after, ids[0])]);
    done(&added[0], "a put after the roll");
    let all: Vec<(usize, u64)> = (0..=after)
        .flat_map(|n| ids.iter().map(move |&through| (n, through)))
        .collect();
    let read = cell.objects(all.iter().map(|&(n, through)| get(n, through)).collect());
    for (outcome, (n, through)) in read.iter().zip(&all) {
        assert_eq!(
            done(outcome, &format!("o{n} through {through}")),
            bytes(*n),
            "o{n} read back through {through} after the roll"
        );
    }
}

/// A second request to stop, as the node's stop begins, ends its process at once: not well, and
/// before its stop says it is done.
#[test]
fn a_second_request_to_stop_ends_the_process_at_once() {
    let mut cell = Cell::made(|root| {
        let data = root.join("data");
        std::fs::create_dir(&data).unwrap();
        vec![
            "--node".into(),
            format!(
                "quic=127.0.0.1:0,plane=127.0.0.1:0,domain=laptop,volume={}:256M",
                data.display()
            ),
            "--max-nodes".into(),
            "1".into(),
            "--max-volumes".into(),
            "1".into(),
        ]
    });
    let id = *cell.dirs.keys().next().unwrap();
    cell.enlisted(id);
    let exited = cell.stop_twice(id);
    assert!(!exited.success(), "node {id} exited {exited}");
    assert!(
        !cell.stopped.contains(&id),
        "node {id} said its stop was done"
    );
}

/// A start that fails names its step (node.md §1.7), and the node serving on its directory serves
/// on: a second process on that directory is refused at the locks, one whose key file is missing,
/// or whose TLS key or root key others than its owner may read, at its identity (read before any
/// lock; the key closed again, the start goes on to the locks), as is one that states another
/// cell's authority or pins a peer from another cell, one whose configuration does not parse or is
/// not there at the configuration, one on a directory of its own stating the serving node's volume
/// at the locks, naming the volume's, and one on a directory of its own whose plane's port, or
/// whose QUIC port, the serving node holds at the network. Each ends non-zero before it listens.
#[test]
fn a_start_that_fails_names_its_step_and_the_node_serving_serves_on() {
    let mut cell = Cell::made(|root| {
        let data = root.join("data");
        std::fs::create_dir(&data).unwrap();
        vec![
            "--node".into(),
            format!(
                "quic=127.0.0.1:0,plane=127.0.0.1:0,domain=laptop,volume={}:256M",
                data.display()
            ),
            "--max-nodes".into(),
            "1".into(),
            "--max-volumes".into(),
            "1".into(),
        ]
    });
    let id = *cell.dirs.keys().next().unwrap();
    cell.enlisted(id);
    let dir = cell.dirs[&id].clone();
    let root = dir.parent().unwrap().to_path_buf();
    let config = dir.join("config");
    let text = std::fs::read_to_string(&config).unwrap();
    let refused = |config: &Path, step: &str, what: &str| {
        let served = Command::new(NODE)
            .arg("serve")
            .arg(config)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        // A path the node names joins a stated relative path to its directory's, with either
        // separator on Windows.
        let said = String::from_utf8_lossy(&served.stderr).replace('\\', "/");
        assert!(!served.status.success(), "{step}: {said}");
        assert!(
            said.contains(&format!("the start's {step} step")) && said.contains(what),
            "{step}: {said}"
        );
        assert!(
            !String::from_utf8_lossy(&served.stdout)
                .lines()
                .any(|line| line.starts_with("bound ")),
            "{step}: it listened"
        );
    };
    refused(&config, "locks", "LOCK");
    for (key, named) in [
        (dir.join("identity").join("node.key"), "identity/node.key"),
        (dir.join("keys").join("1"), "keys/1"),
    ] {
        open_to_others(&key, true);
        refused(
            &config,
            "identity",
            &format!("{named}: is readable by others than its owner"),
        );
        open_to_others(&key, false);
        refused(&config, "locks", "LOCK");
    }
    // Another cell's authority, or a peer pinned from another cell, refuses the start at its
    // identity, naming the certificate the authority stated did not issue.
    let foreign = root.join("foreign");
    let foreign_data = root.join("foreign-data");
    std::fs::create_dir(&foreign_data).unwrap();
    let made = Command::new(NODE)
        .arg("init")
        .arg(&foreign)
        .args([
            "--node",
            &format!(
                "quic=127.0.0.1:0,plane=127.0.0.1:0,domain=laptop,volume={}:256M",
                foreign_data.display()
            ),
            "--max-nodes",
            "1",
            "--max-volumes",
            "1",
        ])
        .output()
        .unwrap();
    assert!(
        made.status.success(),
        "mantle init: {}",
        String::from_utf8_lossy(&made.stderr)
    );
    for (name, from) in [
        ("foreign-authority.der", "authority.der"),
        ("foreign-peer.der", "node.der"),
    ] {
        std::fs::copy(foreign.join("identity").join(from), dir.join(name)).unwrap();
    }
    let alien = dir.join("config-foreign-authority");
    std::fs::write(
        &alien,
        text.replace(
            "authority = identity/authority.der",
            "authority = foreign-authority.der",
        ),
    )
    .unwrap();
    refused(
        &alien,
        "identity",
        "identity/node.der: issued by another authority than the cell's",
    );
    let pinned = dir.join("config-foreign-peer");
    std::fs::write(
        &pinned,
        format!("{}\npeer = 9 node foreign-peer.der\n", text.trim_end()),
    )
    .unwrap();
    refused(
        &pinned,
        "identity",
        "foreign-peer.der: issued by another authority than the cell's",
    );
    let missing = dir.join("config-missing-key");
    std::fs::write(
        &missing,
        text.replace("key = identity/node.key", "key = identity/gone.key"),
    )
    .unwrap();
    refused(&missing, "identity", "gone.key");
    let bad = root.join("bad.conf");
    std::fs::write(&bad, "id = 1\nthis is no line\n").unwrap();
    refused(&bad, "configuration", "line 2");
    refused(&root.join("absent.conf"), "configuration", "absent.conf: ");
    // A directory of its own stating the serving node's volume: refused at the locks, naming the
    // volume's.
    let held = root.join("held");
    for sub in ["identity", "keys"] {
        copy_tree(&dir.join(sub), &held.join(sub));
    }
    std::fs::write(held.join("config"), &text).unwrap();
    refused(
        &held.join("config"),
        "locks",
        "data/mantle/LOCK: another process holds this lock",
    );
    // A directory of its own, its volume its own, stating `key` as `value`: the port the serving
    // node holds, refused at the network.
    let own = |name: &str, key: &str, value: String| {
        let other = root.join(name);
        for sub in ["identity", "keys"] {
            copy_tree(&dir.join(sub), &other.join(sub));
        }
        let data = other.join("data");
        std::fs::create_dir(&data).unwrap();
        let moved: Vec<String> = text
            .lines()
            .map(|line| {
                if line.starts_with(&format!("{key} = ")) {
                    format!("{key} = {value}")
                } else if line.starts_with("volume = ") {
                    let (_, size) = line.rsplit_once(' ').unwrap();
                    format!("volume = {} {size}", data.display())
                } else {
                    line.to_owned()
                }
            })
            .collect();
        std::fs::write(other.join("config"), moved.join("\n") + "\n").unwrap();
        other.join("config")
    };
    let (quic, plane) = cell.running[&id].bound.unwrap();
    refused(
        &own("plane-held", "plane", plane.to_string()),
        "network",
        "bind",
    );
    // The QUIC listener binds its port before it shares it (hyper-rt `Driver::bind`), so a port a
    // node holds is refused to another as a plane's is.
    refused(
        &own("quic-held", "quic", quic.to_string()),
        "network",
        "bind",
    );
    objects_round_trip(&mut cell, &[id], &|_| id);
}

/// Lets everyone read the key file at `path`, or takes that away again: its mode 0644 or 0600 on
/// Unix; on Windows an entry allowing Everyone (S-1-1-0) to read it added to its DACL, or removed.
fn open_to_others(path: &Path, open: bool) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = if open { 0o644 } else { 0o600 };
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }
    #[cfg(windows)]
    {
        let change: &[&str] = if open {
            &["/grant", "*S-1-1-0:R"]
        } else {
            &["/remove:g", "*S-1-1-0"]
        };
        let out = Command::new("icacls")
            .arg(path)
            .args(change)
            .output()
            .unwrap();
        assert!(out.status.success(), "icacls {change:?}: {out:?}");
    }
}

/// Copies the directory `from` and what it holds to `to`, each file's permissions with it (a key
/// file written again as its owner's alone, `cell::key_file`): each
/// directory copied in turn from a list of those found, none waiting on another.
fn copy_tree(from: &Path, to: &Path) {
    let mut dirs = vec![(from.to_path_buf(), to.to_path_buf())];
    while let Some((from, to)) = dirs.pop() {
        std::fs::create_dir_all(&to).unwrap();
        for entry in std::fs::read_dir(&from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                dirs.push((entry.path(), target));
            } else if mantle_node::secret::owner_only(&std::fs::File::open(entry.path()).unwrap())
                .unwrap()
            {
                // A key stays its owner's alone: Windows gives a copy its directory's entries.
                key_file(&target, &std::fs::read(entry.path()).unwrap());
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }
}

/// A peer named by a name that never resolves (the reserved `.invalid` domain, RFC 6761 §6.4) stops
/// `mantle serve` as it starts, before it listens, naming the peer and the name.
#[test]
fn a_peer_whose_name_does_not_resolve_stops_the_start_naming_it() {
    let root = tempfile::tempdir().unwrap();
    let out = root.path().join("cell");
    let data = root.path().join("data");
    std::fs::create_dir(&data).unwrap();
    let made = Command::new(NODE)
        .arg("init")
        .arg(&out)
        .arg("--node")
        .arg(format!(
            "quic=127.0.0.1:0,plane=127.0.0.1:0,domain=laptop,volume={}:256M",
            data.display()
        ))
        .args(["--max-nodes", "1", "--max-volumes", "1"])
        .output()
        .unwrap();
    assert!(
        made.status.success(),
        "{}",
        String::from_utf8_lossy(&made.stderr)
    );
    let config = out.join("config");
    let mut text = std::fs::read_to_string(&config).unwrap();
    text.push_str("peer = 77 node identity/node.der peer-77.invalid:7000 peer-77.invalid:7001\n");
    std::fs::write(&config, text).unwrap();
    let served = Command::new(NODE)
        .arg("serve")
        .arg(&config)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let said = String::from_utf8_lossy(&served.stderr);
    assert!(!served.status.success(), "{said}");
    assert!(
        said.contains("peer 77") && said.contains("peer-77.invalid:7000"),
        "{said}"
    );
    assert!(
        !String::from_utf8_lossy(&served.stdout)
            .lines()
            .any(|line| line.starts_with("bound ")),
        "it listened"
    );
}

/// `mantle init` makes a cell of three from a machines file's two founders and a `--node`'s, the
/// file's first: served each from what init wrote, each pinning the others, each measures its device
/// at its first start, every node enlists, and objects put through each read back through another.
/// The founders start highest id first, each configuration naming the peers already listening by
/// `localhost` and their ports, which each resolves as it starts: every dial a node makes is to an
/// address its configuration named, and only where a later peer listens comes on standard input.
#[test]
fn a_cell_of_three_made_by_init_from_a_file_and_a_flag_serves_objects() {
    let mut cell = Cell::made_named(three_founders);
    let ids: Vec<u64> = cell.dirs.keys().copied().collect();
    assert_eq!(ids.len(), 3);
    for &id in &ids {
        cell.enlisted(id);
    }
    for &id in &ids {
        let bases = cell.bases[&id].clone();
        every_key_said(id, &bases, 2 + usize::try_from(CLIENTS + 1).unwrap());
        let depth = said(&bases, "volume.1.depth");
        assert!(depth.why.contains("measured now"), "node {id}: {depth:?}");
        let voters: BTreeSet<u64> = said(&bases, "voters")
            .value
            .split(',')
            .map(|v| v.parse().unwrap())
            .collect();
        assert_eq!(voters, ids.iter().copied().collect());
    }
    let back = ids.clone();
    objects_round_trip(&mut cell, &ids, &|n| back[(n + 1) % back.len()]);
}

/// One operation asked of `node` by the writer client, and its reply and bytes.
fn ask(cell: &mut Cell, node: u64, op: Op, data: Vec<u8>) -> (Reply, Vec<u8>) {
    let got = cell.objects(vec![(node, op, data)]);
    replied(&got[0], "the operation")
}

/// The status alone of one operation asked of `node`.
fn status(cell: &mut Cell, node: u64, op: Op) -> ObjectStatus {
    ask(cell, node, op, Vec::new()).0.status
}

/// Headers of S3's preconditions and range, each named or not.
fn headers(
    if_match: Option<&str>,
    if_none_match: Option<&str>,
    if_modified_since: Option<&str>,
    if_unmodified_since: Option<&str>,
    range: Option<&str>,
) -> Headers {
    Headers {
        if_match: if_match.map(Into::into),
        if_none_match: if_none_match.map(Into::into),
        if_modified_since: if_modified_since.map(Into::into),
        if_unmodified_since: if_unmodified_since.map(Into::into),
        range: range.map(Into::into),
    }
}

fn put_with(bucket: &str, key: &str, id: u128, data: &[u8], headers: Headers) -> Op {
    Op::Put {
        bucket: bucket.into(),
        key: key.into(),
        id,
        length: data.len() as u64,
        headers,
        checksum: None,
        md5: None,
    }
}

fn get_with(bucket: &str, key: &str, headers: Headers) -> Op {
    Op::Get {
        bucket: bucket.into(),
        key: key.into(),
        headers,
    }
}

fn head_with(bucket: &str, key: &str, headers: Headers) -> Op {
    Op::Head {
        bucket: bucket.into(),
        key: key.into(),
        headers,
    }
}

fn delete_with(bucket: &str, key: &str, id: u128, if_match: Option<&str>) -> Op {
    Op::Delete {
        bucket: bucket.into(),
        key: key.into(),
        id,
        headers: headers(if_match, None, None, None, None),
    }
}

/// S3's ETag of a single PUT's bytes: their MD5, in hex (05 §5.1).
fn etag_of(data: &[u8]) -> String {
    let md5 = mantle_s3::checksum::checksum(mantle_s3::checksum::Algorithm::Md5, data).unwrap();
    mantle_s3::checksum::etag(&md5.bytes.try_into().unwrap())
}

/// The pages of ListObjectsV2 of `bucket`'s keys under `prefix`, each asked of the next node in
/// turn and resumed where the last said, and every key and common prefix they listed, in order.
fn listed(
    cell: &mut Cell,
    bucket: &str,
    prefix: &str,
    delimiter: &str,
    max_keys: u32,
) -> (usize, Vec<String>, Vec<String>) {
    let (mut pages, mut keys, mut prefixes) = (0, Vec::new(), Vec::new());
    let mut start = None;
    loop {
        let node = 1 + pages as u64 % NODES;
        let op = Op::List(List {
            bucket: bucket.into(),
            prefix: prefix.into(),
            delimiter: delimiter.into(),
            start: start.take(),
            max_keys,
            fetch_owner: true,
        });
        let (reply, _) = ask(cell, node, op, Vec::new());
        assert_eq!(reply.status, ObjectStatus::Done, "page {pages}");
        let Some(Listing::Keys(page)) = reply.listing else {
            panic!("page {pages}: {reply:?}");
        };
        assert!(page.contents.len() + page.common_prefixes.len() <= max_keys as usize);
        assert!(
            page.contents
                .iter()
                .all(|l| l.owner.as_deref() == Some(CLIENT.to_string().as_str())),
            "page {pages}: owners"
        );
        pages += 1;
        keys.extend(page.contents.into_iter().map(|l| l.key));
        prefixes.extend(page.common_prefixes);
        if !page.truncated {
            return (pages, keys, prefixes);
        }
        start = page.next;
        assert!(
            start.is_some(),
            "a truncated page says where the next starts"
        );
    }
}

/// Through each node of a cell of three `mantle serve` processes, S3's object and bucket
/// operations answer as S3 does for each case research/05 records: a conditional write judged as
/// it commits (05 §2.2), a conditional read in RFC 9110's order with its 304 and 412 (05 §2.4), a
/// checksum and a `Content-MD5` checked before anything commits (05 §3.2), ranges (05 §12), heads,
/// deletes and stacked delete markers in a versioned bucket (05 §7.3–§7.4), `404 NoSuchKey` and
/// `NoSuchBucket`, another principal's bucket refused, and a bucket deleted only once empty (05
/// §10.5). A listing of more keys than one page holds, paged by token through every node in turn,
/// lists every key exactly once.
#[test]
fn s3s_operations_answer_as_s3_does_through_every_node() {
    use mantle_meta::record::{self, Versioning};
    use mantle_s3::checksum::{self, Algorithm};
    use mantle_s3::list::Resume;

    let mut cell = Cell::served(NODES);
    for id in 1..=NODES {
        cell.enlisted(id);
    }
    let other = CLIENT + 1;
    let create = |bucket: &str| Op::Create {
        bucket: bucket.into(),
        id: 0,
    };
    let head_bucket = |bucket: &str| Op::HeadBucket {
        bucket: bucket.into(),
    };
    let buckets = || Op::ListBuckets {
        prefix: String::new(),
        after: None,
        max: 10,
    };
    let listed_buckets = |reply: Reply| match reply.listing {
        Some(Listing::Buckets(page)) => page.buckets.into_iter().map(|(n, _)| n).collect(),
        _ => panic!("{reply:?}"),
    };

    // Buckets: made once, headed and listed through every node; another principal's refused.
    assert_eq!(status(&mut cell, 1, create("b")), ObjectStatus::Done);
    assert_eq!(
        status(&mut cell, 2, create("b")),
        ObjectStatus::BucketAlreadyOwnedByYou
    );
    for node in 1..=NODES {
        assert_eq!(
            status(&mut cell, node, head_bucket("b")),
            ObjectStatus::Done
        );
        assert_eq!(
            status(&mut cell, node, head_bucket("nope")),
            ObjectStatus::NoSuchBucket
        );
        let names: Vec<String> = listed_buckets(ask(&mut cell, node, buckets(), Vec::new()).0);
        assert_eq!(names, ["b"]);
    }
    let theirs = cell.objects_as(
        other,
        vec![
            (1, create("b"), Vec::new()),
            (2, head_bucket("b"), Vec::new()),
            (
                3,
                Op::DeleteBucket {
                    bucket: "b".into(),
                    id: 1,
                },
                Vec::new(),
            ),
            (1, buckets(), Vec::new()),
        ],
    );
    let theirs: Vec<Reply> = theirs.iter().map(|o| replied(o, "theirs").0).collect();
    assert_eq!(theirs[0].status, ObjectStatus::BucketAlreadyExists);
    assert_eq!(theirs[1].status, ObjectStatus::AccessDenied);
    assert_eq!(theirs[2].status, ObjectStatus::AccessDenied);
    assert!(listed_buckets(theirs[3].clone()).is_empty());

    // Conditional writes, judged as each commits, through every node.
    let bytes = |n: u64, len: usize| object_bytes(n, len);
    let quoted = |etag: &str| format!("\"{etag}\"");
    for node in 1..=NODES {
        let key = format!("c{node}");
        let first = bytes(node, 1_000);
        let create_only = headers(None, Some("*"), None, None, None);
        let (made, _) = ask(
            &mut cell,
            node,
            put_with(
                "b",
                &key,
                u128::from(node) * 10,
                &first,
                create_only.clone(),
            ),
            first.clone(),
        );
        assert_eq!(made.status, ObjectStatus::Done);
        assert_eq!(made.etag, etag_of(&first));
        assert_eq!(made.version, "null");
        let again = put_with("b", &key, u128::from(node) * 10 + 1, &first, create_only);
        assert_eq!(
            ask(&mut cell, node, again, first.clone()).0.status,
            ObjectStatus::PreconditionFailed
        );
        let second = bytes(node + 10, 1_000);
        let wrong = headers(Some("\"0123\""), None, None, None, None);
        let swap = put_with("b", &key, u128::from(node) * 10 + 2, &second, wrong);
        assert_eq!(
            ask(&mut cell, node, swap, second.clone()).0.status,
            ObjectStatus::PreconditionFailed
        );
        let right = headers(Some(&quoted(&made.etag)), None, None, None, None);
        let swap = put_with("b", &key, u128::from(node) * 10 + 3, &second, right);
        let (swapped, _) = ask(&mut cell, node, swap, second.clone());
        assert_eq!(swapped.status, ObjectStatus::Done);
        assert_eq!(swapped.etag, etag_of(&second));
        let missing = headers(Some("*"), None, None, None, None);
        let absent = put_with(
            "b",
            &format!("m{node}"),
            u128::from(node) * 10 + 4,
            &second,
            missing,
        );
        assert_eq!(
            ask(&mut cell, node, absent, second.clone()).0.status,
            ObjectStatus::NoSuchKey
        );
        let (_, read) = ask(
            &mut cell,
            1 + node % NODES,
            get_op("b", &key, None),
            Vec::new(),
        );
        assert!(read == second, "{key}: the swapped bytes");
    }

    // A checksum and a Content-MD5, each checked before anything commits.
    let summed = bytes(70, 70_000);
    let crc = checksum::checksum(Algorithm::Crc32c, &summed).unwrap();
    let with_sum = |id: u128, sum: checksum::Checksum, md5: Option<[u8; 16]>| Op::Put {
        bucket: "b".into(),
        key: "sum".into(),
        id,
        length: summed.len() as u64,
        headers: Headers::default(),
        checksum: Some(sum),
        md5,
    };
    let (kept, _) = ask(
        &mut cell,
        2,
        with_sum(700, crc.clone(), None),
        summed.clone(),
    );
    assert_eq!(kept.status, ObjectStatus::Done);
    assert_eq!(
        kept.checksum,
        Some(record::Checksum {
            algorithm: Algorithm::Crc32c.code(),
            parts: 0,
            value: crc.bytes.clone(),
        })
    );
    let bad = checksum::Checksum {
        algorithm: Algorithm::Crc32c,
        bytes: vec![0; 4],
    };
    let other_bytes = bytes(71, 70_000);
    assert_eq!(
        ask(&mut cell, 3, with_sum(701, bad, None), other_bytes.clone())
            .0
            .status,
        ObjectStatus::BadDigest
    );
    let md5 = checksum::checksum(Algorithm::Md5, &summed).unwrap();
    let wrong_md5 = Some([7u8; 16]);
    let wrong_sum = checksum::checksum(Algorithm::Crc32c, &other_bytes).unwrap();
    assert_eq!(
        ask(
            &mut cell,
            1,
            with_sum(702, wrong_sum, wrong_md5),
            other_bytes
        )
        .0
        .status,
        ObjectStatus::BadDigest
    );
    let (_, read) = ask(&mut cell, 1, get_op("b", "sum", None), Vec::new());
    assert!(read == summed, "nothing a refused put sent committed");
    let right_md5 = Some(<[u8; 16]>::try_from(md5.bytes.as_slice()).unwrap());
    assert_eq!(
        ask(
            &mut cell,
            3,
            with_sum(703, crc.clone(), right_md5),
            summed.clone()
        )
        .0
        .status,
        ObjectStatus::Done
    );

    // Heads and conditional reads, in RFC 9110's order, through every node.
    let (current, _) = ask(
        &mut cell,
        1,
        head_with("b", "c1", Headers::default()),
        Vec::new(),
    );
    assert_eq!(current.status, ObjectStatus::Done);
    assert_eq!(current.size, 1_000);
    assert!(current.modified > 0 && current.version == "null" && !current.marker);
    let etag = quoted(&current.etag);
    let (past, future) = (
        "Sat, 01 Jan 1994 00:00:00 GMT",
        "Fri, 01 Jan 2100 00:00:00 GMT",
    );
    let cases = [
        (
            headers(None, Some(&etag), None, None, None),
            ObjectStatus::NotModified,
        ),
        (
            headers(Some("\"0123\""), None, None, None, None),
            ObjectStatus::PreconditionFailed,
        ),
        // AWS's first combination: If-Match true and If-Unmodified-Since false is 200 (05 §2.4).
        (
            headers(Some(&etag), None, None, Some(past), None),
            ObjectStatus::Done,
        ),
        // Its second: If-None-Match false and If-Modified-Since true is 304.
        (
            headers(None, Some(&etag), Some(past), None, None),
            ObjectStatus::NotModified,
        ),
        (
            headers(None, None, Some(future), None, None),
            ObjectStatus::NotModified,
        ),
        (
            headers(None, None, Some(past), None, None),
            ObjectStatus::Done,
        ),
        (
            headers(None, None, None, Some(past), None),
            ObjectStatus::PreconditionFailed,
        ),
        (
            headers(None, None, None, Some(future), None),
            ObjectStatus::Done,
        ),
        (
            headers(None, Some("\"0123\""), None, None, None),
            ObjectStatus::Done,
        ),
    ];
    for node in 1..=NODES {
        for (n, (h, expected)) in cases.iter().enumerate() {
            let (reply, read) = ask(&mut cell, node, get_with("b", "c1", h.clone()), Vec::new());
            assert_eq!(reply.status, *expected, "node {node}, case {n}");
            if *expected == ObjectStatus::NotModified {
                // A 304 carries the ETag (05 §2.4), and no bytes.
                assert_eq!(reply.etag, current.etag, "node {node}, case {n}");
                assert!(read.is_empty());
            }
            let (headed, _) = ask(&mut cell, node, head_with("b", "c1", h.clone()), Vec::new());
            assert_eq!(headed.status, *expected, "head: node {node}, case {n}");
        }
    }

    // Ranges, resolved against the version's size (05 §12).
    let ten: Vec<u8> = (0u8..10).collect();
    assert_eq!(
        ask(&mut cell, 3, put_op("b", "ten", 800, 10), ten.clone())
            .0
            .status,
        ObjectStatus::Done
    );
    assert_eq!(
        ask(&mut cell, 1, put_op("b", "empty", 801, 0), Vec::new())
            .0
            .status,
        ObjectStatus::Done
    );
    let ranged = |range: &str| headers(None, None, None, None, Some(range));
    for (node, (range, served)) in (1u64..).zip([
        ("bytes=3-6", Some((3u64, 6u64))),
        ("bytes=-4", Some((6, 9))),
        ("bytes=8-", Some((8, 9))),
        ("bytes=3-99", Some((3, 9))),
        ("bytes=a-b", None),
    ]) {
        let node = 1 + node % NODES;
        let (reply, read) = ask(
            &mut cell,
            node,
            get_with("b", "ten", ranged(range)),
            Vec::new(),
        );
        assert_eq!(reply.status, ObjectStatus::Done, "{range}");
        assert_eq!(reply.range, served, "{range}");
        assert_eq!(reply.size, 10, "{range}");
        let (first, last) = served.unwrap_or((0, 9));
        let (first, last) = (
            usize::try_from(first).unwrap(),
            usize::try_from(last).unwrap(),
        );
        assert_eq!(read, ten[first..=last], "{range}");
        let (headed, none) = ask(
            &mut cell,
            node,
            head_with("b", "ten", ranged(range)),
            Vec::new(),
        );
        assert_eq!((headed.status, headed.range), (ObjectStatus::Done, served));
        assert!(none.is_empty());
    }
    let (past_end, _) = ask(
        &mut cell,
        2,
        get_with("b", "ten", ranged("bytes=10-")),
        Vec::new(),
    );
    assert_eq!(
        (past_end.status, past_end.size),
        (ObjectStatus::InvalidRange, 10)
    );
    assert_eq!(
        ask(
            &mut cell,
            2,
            get_with("b", "empty", ranged("bytes=0-")),
            Vec::new()
        )
        .0
        .status,
        ObjectStatus::InvalidRange
    );
    let (whole, none) = ask(&mut cell, 2, get_op("b", "empty", None), Vec::new());
    assert_eq!((whole.status, whole.size), (ObjectStatus::Done, 0));
    assert!(none.is_empty());

    // A key, or a bucket, that is not there.
    for node in 1..=NODES {
        assert_eq!(
            status(&mut cell, node, get_op("b", "none", None)),
            ObjectStatus::NoSuchKey
        );
        assert_eq!(
            status(&mut cell, node, head_with("b", "none", Headers::default())),
            ObjectStatus::NoSuchKey
        );
        assert_eq!(
            status(&mut cell, node, get_op("nope", "k", None)),
            ObjectStatus::NoSuchBucket
        );
        assert_eq!(
            ask(&mut cell, node, put_op("nope", "k", 900, 1), vec![1])
                .0
                .status,
            ObjectStatus::NoSuchBucket
        );
        assert_eq!(
            status(&mut cell, node, delete_with("nope", "k", 901, None)),
            ObjectStatus::NoSuchBucket
        );
    }

    // Deletes in an unversioned bucket: conditional, then gone, then a delete of nothing done.
    let c1 = quoted(&current.etag);
    assert_eq!(
        status(&mut cell, 1, delete_with("b", "c1", 950, Some("\"0123\""))),
        ObjectStatus::PreconditionFailed
    );
    let (gone, _) = ask(
        &mut cell,
        2,
        delete_with("b", "c1", 951, Some(&c1)),
        Vec::new(),
    );
    assert_eq!((gone.status, gone.marker), (ObjectStatus::Done, false));
    assert_eq!(
        status(&mut cell, 3, get_op("b", "c1", None)),
        ObjectStatus::NoSuchKey
    );
    assert_eq!(
        status(&mut cell, 1, delete_with("b", "c1", 952, None)),
        ObjectStatus::Done
    );
    assert_eq!(
        status(&mut cell, 1, delete_with("b", "c1", 953, Some("*"))),
        ObjectStatus::NoSuchKey
    );

    // A versioned bucket: versions kept, a delete a marker, deletes stacking markers (05 §7.3).
    assert_eq!(status(&mut cell, 1, create("v")), ObjectStatus::Done);
    let versioning = |v| Op::PutVersioning {
        bucket: "v".into(),
        id: 0,
        versioning: v,
    };
    let state = |cell: &mut Cell| {
        ask(
            cell,
            2,
            Op::GetVersioning { bucket: "v".into() },
            Vec::new(),
        )
        .0
        .versioning
    };
    assert_eq!(state(&mut cell), Some(Versioning::Unversioned));
    assert_eq!(
        status(&mut cell, 3, versioning(Versioning::Enabled)),
        ObjectStatus::Done
    );
    assert_eq!(state(&mut cell), Some(Versioning::Enabled));
    let theirs = cell.objects_as(
        other,
        vec![(1, versioning(Versioning::Suspended), Vec::new())],
    );
    assert_eq!(
        replied(&theirs[0], "theirs").0.status,
        ObjectStatus::AccessDenied
    );
    let (one, _) = ask(&mut cell, 1, put_op("v", "k", 1_000, 3), vec![1, 2, 3]);
    let (two, _) = ask(&mut cell, 2, put_op("v", "k", 1_001, 2), vec![4, 5]);
    assert_eq!(
        (one.status, two.status),
        (ObjectStatus::Done, ObjectStatus::Done)
    );
    assert!(one.version != "null" && two.version != one.version);
    let (marked, _) = ask(&mut cell, 3, delete_with("v", "k", 1_002, None), Vec::new());
    assert_eq!((marked.status, marked.marker), (ObjectStatus::Done, true));
    assert!(
        ![one.version.as_str(), two.version.as_str(), "", "null"]
            .contains(&marked.version.as_str())
    );
    for node in 1..=NODES {
        let (read, _) = ask(&mut cell, node, get_op("v", "k", None), Vec::new());
        assert_eq!((read.status, read.marker), (ObjectStatus::NoSuchKey, true));
        assert_eq!(read.version, marked.version);
        let (headed, _) = ask(
            &mut cell,
            node,
            head_with("v", "k", Headers::default()),
            Vec::new(),
        );
        assert_eq!(
            (headed.status, headed.marker),
            (ObjectStatus::NoSuchKey, true)
        );
    }
    let (stacked, _) = ask(&mut cell, 1, delete_with("v", "k", 1_003, None), Vec::new());
    assert_eq!((stacked.status, stacked.marker), (ObjectStatus::Done, true));
    assert!(stacked.version != marked.version);
    let (_, keys, _) = listed(&mut cell, "v", "", "", 1000);
    assert!(
        keys.is_empty(),
        "a key whose current version is a marker lists nothing"
    );
    assert_eq!(
        status(
            &mut cell,
            2,
            Op::DeleteBucket {
                bucket: "v".into(),
                id: 1_004
            }
        ),
        ObjectStatus::BucketNotEmpty
    );
    assert_eq!(status(&mut cell, 3, head_bucket("v")), ObjectStatus::Done);

    // A listing of more keys than one page holds, paged through every node in turn.
    let many: Vec<String> = (0..1_050).map(|n| format!("l/{n:04}")).collect();
    let puts = many
        .iter()
        .enumerate()
        .map(|(n, key)| {
            (
                1 + n as u64 % NODES,
                put_op("b", key, 2_000 + n as u128, 0),
                Vec::new(),
            )
        })
        .collect();
    for (outcome, key) in cell.objects(puts).iter().zip(&many) {
        done(outcome, &format!("the put of {key}"));
    }
    let (pages, keys, prefixes) = listed(&mut cell, "b", "l/", "", 1000);
    assert_eq!((pages, prefixes.len()), (2, 0));
    assert_eq!(keys, many, "every key once, in order");
    let (pages, keys, _) = listed(&mut cell, "b", "l/", "", 97);
    assert_eq!(pages, 11);
    assert_eq!(keys, many, "every key once, in order, by 97s");
    let (_, keys, prefixes) = listed(&mut cell, "b", "", "/", 2);
    assert_eq!(prefixes, ["l/"]);
    assert_eq!(keys, ["c2", "c3", "empty", "sum", "ten"]);
    let after = Op::List(List {
        bucket: "b".into(),
        prefix: "l/".into(),
        delimiter: String::new(),
        start: Some(Resume::After("l/0999".into())),
        max_keys: 1000,
        fetch_owner: false,
    });
    let (reply, _) = ask(&mut cell, 2, after, Vec::new());
    let Some(Listing::Keys(page)) = reply.listing else {
        panic!("{reply:?}");
    };
    assert!(!page.truncated);
    assert_eq!(
        page.contents
            .iter()
            .map(|l| l.key.as_str())
            .collect::<Vec<_>>(),
        many[1_000..].iter().map(String::as_str).collect::<Vec<_>>()
    );
    assert!(
        page.contents
            .iter()
            .all(|l| l.owner.is_none() && l.size == 0)
    );
    let missing = Op::List(List {
        bucket: "nope".into(),
        prefix: String::new(),
        delimiter: String::new(),
        start: None,
        max_keys: 10,
        fetch_owner: false,
    });
    assert_eq!(status(&mut cell, 1, missing), ObjectStatus::NoSuchBucket);

    // A bucket is deleted only once empty, and its name can be made again.
    assert_eq!(status(&mut cell, 2, create("gone")), ObjectStatus::Done);
    assert_eq!(
        ask(&mut cell, 3, put_op("gone", "x", 3_000, 1), vec![9])
            .0
            .status,
        ObjectStatus::Done
    );
    let delete_gone = |id| Op::DeleteBucket {
        bucket: "gone".into(),
        id,
    };
    assert_eq!(
        status(&mut cell, 1, delete_gone(3_001)),
        ObjectStatus::BucketNotEmpty
    );
    assert_eq!(
        status(&mut cell, 1, head_bucket("gone")),
        ObjectStatus::Done
    );
    assert_eq!(
        status(&mut cell, 2, delete_with("gone", "x", 3_002, None)),
        ObjectStatus::Done
    );
    assert_eq!(status(&mut cell, 3, delete_gone(3_003)), ObjectStatus::Done);
    for node in 1..=NODES {
        assert_eq!(
            status(&mut cell, node, head_bucket("gone")),
            ObjectStatus::NoSuchBucket
        );
    }
    assert_eq!(
        status(&mut cell, 1, delete_gone(3_004)),
        ObjectStatus::NoSuchBucket
    );
    let names: Vec<String> = listed_buckets(ask(&mut cell, 2, buckets(), Vec::new()).0);
    assert_eq!(names, ["b", "v"]);
    assert_eq!(status(&mut cell, 3, create("gone")), ObjectStatus::Done);
    assert_eq!(
        status(&mut cell, 1, get_op("gone", "x", None)),
        ObjectStatus::NoSuchKey
    );
}
