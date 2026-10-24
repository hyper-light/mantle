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

/// A start that fails names its step (node.md §1.7), and the node serving on its directory serves
/// on: a second process on that directory is refused at the locks, one whose key file is missing at
/// its identity (read before any lock), one whose configuration does not parse at the
/// configuration, and one on a directory of its own whose plane's port the serving node holds at
/// the network. Each ends non-zero before it listens.
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
        let said = String::from_utf8_lossy(&served.stderr).into_owned();
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
    let other = root.join("other");
    for sub in ["identity", "keys"] {
        copy_tree(&dir.join(sub), &other.join(sub));
    }
    let data = other.join("data");
    std::fs::create_dir(&data).unwrap();
    let (_, plane) = cell.running[&id].bound.unwrap();
    let moved: Vec<String> = text
        .lines()
        .map(|line| {
            if line.starts_with("plane = ") {
                format!("plane = {plane}")
            } else if line.starts_with("volume = ") {
                let (_, size) = line.rsplit_once(' ').unwrap();
                format!("volume = {} {size}", data.display())
            } else {
                line.to_owned()
            }
        })
        .collect();
    std::fs::write(other.join("config"), moved.join("\n") + "\n").unwrap();
    refused(&other.join("config"), "network", "bind");
    objects_round_trip(&mut cell, &[id], &|_| id);
}

/// Copies the directory `from` and what it holds to `to`, each file's permissions with it: each
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
    let mut cell = Cell::made_named(|root| {
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
    });
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
