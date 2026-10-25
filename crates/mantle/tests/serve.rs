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

    let made = cell.objects(vec![(1, Op::Create { bucket: "b".into() }, Vec::new())]);
    done(&made[0], "the bucket");
    let objects: Vec<(String, Vec<u8>)> = (0u64..)
        .zip([0usize, 1, 65_537, 3 << 20])
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
        .map(|(key, _)| {
            let get = Op::Get {
                bucket: "b".into(),
                key: key.clone(),
                offset: 0,
                len: u64::MAX,
            };
            (1, get, Vec::new())
        })
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
        Op::Create { bucket: "b".into() },
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
            let op = Op::Put {
                bucket: "b".into(),
                key: key.clone(),
                id: n as u128 + 1,
                length: data.len() as u64,
            };
            (through[n % through.len()], op, data.clone())
        })
        .collect();
    for (outcome, (key, _)) in cell.objects(puts).iter().zip(&objects) {
        done(outcome, &format!("the put of {key}"));
    }
    let gets = objects
        .iter()
        .enumerate()
        .map(|(n, (key, _))| {
            let get = Op::Get {
                bucket: "b".into(),
                key: key.clone(),
                offset: 0,
                len: u64::MAX,
            };
            (back(n), get, Vec::new())
        })
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
        Op::Create { bucket: "b".into() },
        Vec::new(),
    )]);
    done(&made[0], "the bucket");
    let bytes = |n: usize| object_bytes(u64::try_from(n).unwrap(), 65_537);
    let put = |n: usize, through: u64| {
        let data = bytes(n);
        let op = Op::Put {
            bucket: "b".into(),
            key: format!("o{n}"),
            id: u128::try_from(n).unwrap() + 1,
            length: data.len() as u64,
        };
        (through, op, data)
    };
    let get = |n: usize, through: u64| {
        let op = Op::Get {
            bucket: "b".into(),
            key: format!("o{n}"),
            offset: 0,
            len: u64::MAX,
        };
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
        Op::Create { bucket: "c".into() },
        Vec::new(),
    )]);
    done(&made[0], "a bucket made after the stop");
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
        Op::Create { bucket: "b".into() },
        Vec::new(),
    )]);
    done(&made[0], "the bucket");
    let bytes = |n: usize| object_bytes(u64::try_from(n).unwrap(), 65_537);
    let put = |n: usize, through: u64| {
        let data = bytes(n);
        let op = Op::Put {
            bucket: "b".into(),
            key: format!("o{n}"),
            id: u128::try_from(n).unwrap() + 1,
            length: data.len() as u64,
        };
        (through, op, data)
    };
    let get = |n: usize, through: u64| {
        let op = Op::Get {
            bucket: "b".into(),
            key: format!("o{n}"),
            offset: 0,
            len: u64::MAX,
        };
        (through, op, Vec::new())
    };
    let before: Vec<usize> = (0..ids.len()).collect();
    let puts = before.iter().map(|&n| put(n, ids[n])).collect();
    for (outcome, n) in cell.objects(puts).iter().zip(&before) {
        done(outcome, &format!("the put of o{n}"));
    }
    let groups: BTreeSet<u128> = cell.status.keys().map(|(_, group)| *group).collect();
    ids.sort_unstable_by(|a, b| b.cmp(a));
    cell.diag_mark(format!("MARK roll begins, objects put, ids {ids:?}"));
    for &stopping in &ids {
        cell.diag_mark(format!("MARK stop {stopping}"));
        let exited = cell.stop(stopping);
        cell.diag_mark(format!("MARK {stopping} exited {exited}"));
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
        cell.diag_mark(format!("MARK led on past {stopping}; leaderless cleared"));
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
        cell.diag_mark(format!("MARK read back; start {stopping}"));
        cell.start(stopping);
        cell.diag_mark(format!("MARK {stopping} started; introduce"));
        cell.introduce();
        cell.enlisted(stopping);
        cell.diag_mark(format!("MARK {stopping} enlisted"));
        assert_eq!(
            cell.volume(stopping),
            volumes[&stopping],
            "node {stopping} started again with another volume"
        );
        for group in &groups {
            cell.converged(*group);
        }
        cell.diag_mark(format!("MARK converged after {stopping}"));
        let said: Vec<(u64, u128)> = cell
            .leaderless
            .iter()
            .copied()
            .filter(|(node, _)| *node != stopping)
            .collect();
        if !said.is_empty() {
            eprintln!(
                "DIAG wall now {:.1}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs_f64() * 1000.0)
                    .unwrap_or(0.0)
            );
            let now = std::time::Instant::now();
            for (at, what) in &cell.diag_seen {
                eprintln!(
                    "DIAG heard {:>8.1} ms ago: {what}",
                    now.saturating_duration_since(*at).as_secs_f64() * 1000.0
                );
            }
        }
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
