//! The cell's repair, rebalancing and multi-machine failures with real `mantle serve` processes on
//! real disks (docs/design/repair.md §7): cells of three and of five `mantle init` makes, objects put
//! through the S3 listener and the native door, and then volumes lost while objects are written and
//! at rest, a node held still (SIGSTOP, or on Windows each thread suspended) for a long stretch and
//! brought back, a node killed (SIGKILL) and stated gone, a node joining and a node leaving while
//! objects are written, a node leaving a cell narrower than its blocks and another joining, and
//! more volumes lost at once than a block's code spares. After each: every acknowledged object
//! reads back byte-exact through both doors and every node; every block's places, read from the
//! Block range and the volume table as a gateway reads them, each hold their chunk on disk, read
//! from its volume and checked against the block's CRC-32C; and what the cell cannot rebuild it
//! says, typed, never answering with bytes it does not hold. Every wait is on a fact a node says
//! (`repair`, `enlisted`, `leaving`, `standing`, `gone`, `link` lines), never a clock.
//!
//! A binary of its own (crates/mantle/tests/support/cell.rs's harness, support/s3.rs's client).

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
#[path = "support/hold.rs"]
mod hold;
#[path = "../../range/tests/support/linear.rs"]
mod linear;
#[allow(dead_code)]
#[path = "../../node/tests/support/relay.rs"]
mod relay;
#[allow(dead_code)]
#[path = "support/s3.rs"]
mod s3;

use cell::*;
use mantle_ec::Code;
use mantle_meta::record::{BlockHeader, ChunkPlace, Target};
use mantle_protocol::object::Status as ObjectStatus;
use s3::{Client as S3, Key};

/// Diagnostic: where the test body is.
#[track_caller]
fn step(cell: &Cell, what: &str) {
    eprintln!(
        "STEP {} at {} {what} running={:?}",
        std::thread::current().name().unwrap_or("?"),
        std::panic::Location::caller(),
        cell.running.keys().collect::<Vec<_>>()
    );
}

/// The ranges' groups `mantle init` drew, as a founder's configuration names them.
#[derive(Clone, Copy)]
struct Groups {
    root: u128,
    name: u128,
    file: u128,
    block: u128,
}

fn groups(cell: &Cell) -> Groups {
    let config = cell.made.values().next().unwrap();
    let text = std::fs::read_to_string(config).unwrap();
    let group = |layer: &str| {
        text.lines()
            .filter_map(|l| l.strip_prefix("range = "))
            .find_map(|l| {
                let (group, named) = l.split_once(' ')?;
                (named.trim() == layer).then(|| u128::from_str_radix(group, 16).unwrap())
            })
            .unwrap()
    };
    Groups {
        root: group("root"),
        name: group("name"),
        file: group("file"),
        block: group("block"),
    }
}

/// `mantle init`'s arguments for a cell of `nodes` founders under `root`, each in a domain of its
/// own with `volumes` volumes of 256 MiB, the harness's own, the table's bounds room for every
/// volume a node may start again with, and `lines` stated for the cell besides.
fn founders(root: &Path, nodes: usize, volumes: usize, lines: &str) -> Vec<String> {
    let file = root.join("machines.conf");
    let mut text = format!(
        "max_nodes = {nodes}\nmax_volumes = {}\n{lines}",
        nodes * volumes * 4
    );
    for n in 0..nodes {
        text.push_str(&format!(
            "[node]\nquic = 127.0.0.1:0\nplane = 127.0.0.1:0\ndomain = rack-{n}\n"
        ));
        for v in 0..volumes {
            let dir = root.join(format!("d{n}-{v}"));
            std::fs::create_dir(&dir).unwrap();
            text.push_str(&format!("volume = {} 256M\n", dir.display()));
        }
    }
    std::fs::write(&file, text).unwrap();
    vec!["--founders".into(), file.display().to_string()]
}

/// Each volume's directory node `id`'s configuration names.
fn volume_dirs(cell: &Cell, id: u64) -> Vec<PathBuf> {
    let config = &cell.made[&id];
    let text = std::fs::read_to_string(config).unwrap();
    text.lines()
        .filter_map(|l| l.strip_prefix("volume = "))
        .map(|l| {
            let (dir, _) = l.rsplit_once(' ').unwrap();
            let dir = PathBuf::from(dir);
            if dir.is_absolute() {
                dir
            } else {
                config.parent().unwrap().join(dir)
            }
        })
        .collect()
}

/// Node `id`'s volume in `dir` lost, as a disk replaced: its file gone, so the node, started again,
/// lays out a fresh one.
fn lose_volume(dir: &Path) {
    let volume = dir.join("mantle").join("volume");
    std::fs::remove_file(&volume).unwrap();
}

/// The repair as the node running it last said, once a node says it leads it.
fn leading(cell: &Cell) -> Option<Repairing> {
    cell.repairs.values().copied().find(|r| r.leading)
}

/// Waits until the node running the cell's repair says its last pass ended with nothing it could
/// do, the pass begun at the root range's index `mark` or later, and `done` holds of it: the facts
/// the test waits on.
#[track_caller]
fn settled_after(
    cell: &mut Cell,
    mark: u64,
    mut done: impl FnMut(&Repairing) -> bool,
) -> Repairing {
    cell.until(|c| {
        leading(c).filter(|r| r.settled && r.running == 0 && r.root_applied >= mark && done(r))
    })
}

/// [`settled_after`] at the root range's commit as the running nodes last said it: past every
/// statement the cell's nodes made to it so far, a lost volume's or a joining node's.
#[track_caller]
fn settled(cell: &mut Cell, done: impl FnMut(&Repairing) -> bool) -> Repairing {
    let mark = root_commit(cell);
    settled_after(cell, mark, done)
}

/// The root range's commit, the most any running node said.
fn root_commit(cell: &Cell) -> u64 {
    let root = groups(cell).root;
    cell.status
        .iter()
        .filter(|((_, group), _)| *group == root)
        .filter_map(|(_, line)| match line {
            Line::Status { commit, .. } => Some(*commit),
            _ => None,
        })
        .max()
        .unwrap_or(0)
}

/// The volume table as the root range holds it.
#[track_caller]
fn table(cell: &mut Cell, g: Groups) -> Vec<Placed> {
    let mut table = Vec::new();
    let mut after = None;
    loop {
        let read = cell.read(vec![(g.root, Query::Volumes { after })]);
        let [Ok(Read::Volumes { volumes, more })] = read.as_slice() else {
            panic!("the volume table read {read:?}");
        };
        after = volumes.last().map(|p| p.volume);
        table.extend(volumes.iter().cloned());
        if !more {
            return table;
        }
    }
}

/// The volumes the table gives node `id`.
#[track_caller]
fn volumes_of(cell: &mut Cell, g: Groups, id: u64) -> BTreeSet<u128> {
    table(cell, g)
        .iter()
        .filter(|p| p.node == id)
        .map(|p| p.volume)
        .collect()
}

/// A running node that leads no range, as the nodes last said: one whose stop no range waits out
/// a failover for, in a cell started in part, whose leaders' detectors have not judged every link.
fn follower(cell: &Cell) -> u64 {
    let leaders: BTreeSet<u64> = cell
        .status
        .values()
        .filter_map(|line| match line {
            Line::Status { leader, .. } => Some(*leader),
            _ => None,
        })
        .collect();
    *cell
        .running
        .keys()
        .find(|id| !leaders.contains(id))
        .expect("a running node that leads no range")
}

/// Node `id` asked to leave, waited on until it says it is leaving.
#[track_caller]
fn leave(cell: &mut Cell, id: u64) {
    {
        let input = &mut cell.running.get_mut(&id).unwrap().input;
        input.write_all(b"leave\n").unwrap();
        input.flush().unwrap();
    }
    cell.until(|c| {
        if c.leaving.contains(&id) {
            Some(())
        } else {
            None
        }
    });
}

/// Waits until the volume table names no volume of node `id`: each drained, written off and
/// forgotten.
#[track_caller]
fn forgotten(cell: &mut Cell, g: Groups, id: u64) {
    cell.until(|c| {
        if table(c, g).iter().any(|p| p.node == id) {
            None
        } else {
            Some(())
        }
    });
}

/// The blocks of `key`'s current version, each its header and places, through its file's
/// extents and, for an object of parts, each part's file's.
#[track_caller]
fn blocks_of(
    cell: &mut Cell,
    g: Groups,
    bucket: &str,
    key: &str,
) -> Vec<(u128, BlockHeader, Vec<ChunkPlace>)> {
    let current = cell.read(vec![(
        g.name,
        Query::Current {
            bucket: bucket.into(),
            key: key.into(),
        },
    )]);
    let [Ok(Read::Current(Some((_, version))))] = current.as_slice() else {
        panic!("{key}: {current:?}");
    };
    let mut files: Vec<u128> = version.file.into_iter().collect();
    let mut blocks = Vec::new();
    while let Some(file) = files.pop() {
        let mut offset = 0;
        loop {
            let extents = cell.read(vec![(
                g.file,
                Query::Extents {
                    file,
                    offset,
                    max: 16,
                },
            )]);
            let [Ok(Read::Extents(extents))] = extents.as_slice() else {
                panic!("{key}'s extents: {extents:?}");
            };
            let Some((last, extent)) = extents.last() else {
                break;
            };
            offset = last + extent.length;
            for (_, extent) in extents {
                match extent.target {
                    Target::File(part) => files.push(part),
                    Target::Block(block) => {
                        let read = cell.read(vec![(g.block, Query::Block { block })]);
                        let [Ok(Read::Block(Some((header, places))))] = read.as_slice() else {
                            panic!("{key}'s block: {read:?}");
                        };
                        blocks.push((block, *header, places.clone()));
                    }
                }
            }
        }
    }
    blocks
}

/// What a block's places hold on disk: each place's chunk read whole from its volume, which
/// verifies it, by a client the founders pin as a gateway. Chunks of a gone volume, or one the
/// table forgot, are none.
#[track_caller]
fn on_disk(
    cell: &mut Cell,
    places: &[ChunkPlace],
    len: u64,
    node_of: &BTreeMap<u128, u64>,
) -> Vec<Option<Vec<u8>>> {
    let gets: Vec<(u64, u128, ChunkKey)> = places
        .iter()
        .filter_map(|p| Some((*node_of.get(&p.volume)?, p.volume, p.key)))
        .collect();
    let (settings, bound) = (cell.pki.client_settings(CLIENT + 2), cell.bound_quic());
    let outcomes = std::thread::spawn(move || {
        client_runtime(settings.nodes.len() + 1)
            .block_on(async move {
                let mut client = Client::open(settings).await.unwrap();
                for (node, quic) in bound {
                    client.learn(node, quic).unwrap();
                }
                let mut read = Vec::with_capacity(gets.len());
                for (node, volume, key) in gets {
                    let got = client
                        .get_chunk(node, (volume, key), (0, len), u64::MAX)
                        .await;
                    read.push((
                        volume,
                        match got {
                            Ok(ChunkOutcome::Read(bytes)) => Some(bytes),
                            _ => None,
                        },
                    ));
                }
                read
            })
            .unwrap()
    })
    .join()
    .unwrap();
    let read: BTreeMap<u128, Option<Vec<u8>>> = outcomes.into_iter().collect();
    places
        .iter()
        .map(|p| read.get(&p.volume).cloned().flatten())
        .collect()
}

/// Whether `chunks`, a block's chunks by index as read, rebuild `header`'s block: a copy is the
/// block, and a code's data chunks decode to it; each checked against its CRC-32C.
fn rebuilds(header: &BlockHeader, chunks: &BTreeMap<u16, Vec<u8>>) -> bool {
    if header.data == 1 {
        return chunks
            .values()
            .any(|c| mantle_crc::crc32c(c) == header.crc32c);
    }
    let code = Code::new(header.data.into(), header.parity.into()).unwrap();
    let present: Vec<(usize, &[u8])> = chunks
        .iter()
        .take(usize::from(header.data))
        .map(|(&i, b)| (usize::from(i), b.as_slice()))
        .collect();
    if present.len() < usize::from(header.data) {
        return false;
    }
    let block = code
        .decode(&present, usize::try_from(header.length).unwrap())
        .unwrap();
    mantle_crc::crc32c(&block) == header.crc32c
}

/// Every block of every object in `keys` whole on disk: as many places as its width at least,
/// each chunk on a volume of its own with one place on each running node at most; each chunk's
/// places that read giving the chunk, and the chunks the block's own places hold rebuilding it.
/// Returns how many blocks and places it checked.
#[track_caller]
fn whole_on_disk(cell: &mut Cell, g: Groups, bucket: &str, keys: &[String]) -> (usize, usize) {
    let placed = table(cell, g);
    let node_of: BTreeMap<u128, u64> = placed.iter().map(|p| (p.volume, p.node)).collect();
    let (mut blocks, mut places_checked) = (0, 0);
    for key in keys {
        for (block, header, places) in blocks_of(cell, g, bucket, key) {
            let width = usize::from(header.data) + usize::from(header.parity);
            let distinct: BTreeSet<u128> = places.iter().map(|p| p.volume).collect();
            assert_eq!(
                distinct.len(),
                places.len(),
                "{key} {block:x}: two places on a volume"
            );
            let read = on_disk(cell, &places, header.chunk_len, &node_of);
            let mut chunks: BTreeMap<u16, Vec<u8>> = BTreeMap::new();
            let mut own_nodes = BTreeSet::new();
            for (at, (place, bytes)) in places.iter().zip(&read).enumerate() {
                let bytes = bytes.as_ref().unwrap_or_else(|| {
                    panic!(
                        "{key} {block:x}: place {at} {:x} reads nothing",
                        place.volume
                    )
                });
                assert_eq!(bytes.len() as u64, header.chunk_len);
                if at < width {
                    own_nodes.insert(node_of[&place.volume]);
                    chunks.insert(place.key.index, bytes.clone());
                }
                places_checked += 1;
            }
            assert_eq!(chunks.len(), width, "{key} {block:x}: {places:?}");
            assert!(
                rebuilds(&header, &chunks),
                "{key} {block:x} does not rebuild from its chunks"
            );
            // A copy is the block itself; a coded chunk's every place holds its bytes.
            for (place, bytes) in places.iter().zip(&read).skip(width) {
                assert_eq!(
                    bytes.as_ref(),
                    chunks.get(&place.key.index),
                    "{key} {block:x}: a kept place holds other bytes"
                );
            }
            let _ = own_nodes;
            blocks += 1;
        }
    }
    (blocks, places_checked)
}

/// The cell's objects as put: by key, their bytes.
type Objects = BTreeMap<String, Vec<u8>>;

/// The S3 client of each running node's listener.
fn listeners(cell: &Cell) -> BTreeMap<u64, S3> {
    let out = cell
        .made
        .values()
        .next()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let key = Key::of(&out);
    cell.running
        .keys()
        .filter_map(|id| {
            let (at, _) = cell.s3_bound.get(id)?;
            let (_, region, _) = &cell.s3[id];
            Some((*id, S3::new(*at, key.clone(), region)))
        })
        .collect()
}

/// Puts `n` objects of `sizes` in turn through the S3 listener of each running node and the
/// native door, half each, into `bucket`, named `prefix-i`; every one acknowledged.
#[track_caller]
fn put_objects(cell: &mut Cell, bucket: &str, prefix: &str, sizes: &[usize], seed: u64) -> Objects {
    let mut objects = Objects::new();
    let mut s3 = listeners(cell);
    let nodes: Vec<u64> = s3.keys().copied().collect();
    let mut native = Vec::new();
    for (i, &size) in sizes.iter().enumerate() {
        let key = format!("{prefix}-{i}");
        let data = object_bytes(seed * 1000 + i as u64, size);
        let node = nodes[i % nodes.len()];
        if i % 2 == 0 {
            let put =
                s3.get_mut(&node)
                    .unwrap()
                    .send("PUT", &format!("/{bucket}/{key}"), &[], &data);
            assert_eq!(put.status, 200, "{key}: {}", put.text());
        } else {
            native.push((
                node,
                put_op(
                    bucket,
                    &key,
                    (seed << 32 | i as u64).into(),
                    data.len() as u64,
                ),
                data.clone(),
            ));
        }
        objects.insert(key, data);
    }
    for (outcome, (_, op, _)) in cell.objects(native.clone()).iter().zip(&native) {
        let (reply, _) = replied(outcome, &format!("{op:?}"));
        assert_eq!(reply.status, ObjectStatus::Done, "{op:?}");
    }
    objects
}

/// Every object of `objects` read back whole through each running node's S3 listener and its
/// native door: byte-exact, or, where `lost` holds of its key, refused typed and never answered
/// with bytes.
#[track_caller]
fn read_back(cell: &mut Cell, bucket: &str, objects: &Objects, lost: &BTreeSet<String>) {
    let mut s3 = listeners(cell);
    let nodes: Vec<u64> = s3.keys().copied().collect();
    let mut gets = Vec::new();
    for (key, data) in objects {
        for &node in &nodes {
            let got = s3
                .get_mut(&node)
                .unwrap()
                .send("GET", &format!("/{bucket}/{key}"), &[], b"");
            if lost.contains(key) {
                assert!(
                    got.status == 500 || got.status == 503,
                    "{key} through {node}'s listener: {} {}",
                    got.status,
                    got.text()
                );
            } else {
                assert_eq!(
                    got.status,
                    200,
                    "{key} through {node}'s listener: {}",
                    got.text()
                );
                assert!(
                    got.body == *data,
                    "{key} through {node}'s listener: other bytes"
                );
            }
            gets.push((node, get_op(bucket, key, None), Vec::new(), key.clone()));
        }
    }
    let ops: Vec<(u64, Op, Vec<u8>)> = gets
        .iter()
        .map(|(n, op, d, _)| (*n, op.clone(), d.clone()))
        .collect();
    let outcomes = cell.objects(ops);
    for (outcome, (node, _, _, key)) in outcomes.iter().zip(&gets) {
        let (reply, bytes) = replied(outcome, key);
        if lost.contains(key) {
            assert_ne!(
                reply.status,
                ObjectStatus::Done,
                "{key} through {node}: lost, yet answered"
            );
        } else {
            assert_eq!(reply.status, ObjectStatus::Done, "{key} through {node}");
            assert!(
                bytes == objects[key],
                "{key} through {node}'s native door: other bytes"
            );
        }
    }
}

/// A cell made and started, every running founder enlisted and, where every founder runs, judging
/// its links, and a bucket. A range's leader measures its patience toward each voter, a founder not
/// yet started among them, so a cell started in part is ready once its running founders enlisted.
#[track_caller]
fn ready(cell: &mut Cell, bucket: &str) {
    let ids: Vec<u64> = cell.running.keys().copied().collect();
    for id in ids {
        cell.enlisted(id);
    }
    if cell.running.len() == cell.made.len() {
        cell.judging();
    }
    let mut s3 = listeners(cell);
    let made = s3
        .values_mut()
        .next()
        .unwrap()
        .send("PUT", &format!("/{bucket}"), &[], b"");
    assert_eq!(made.status, 200, "{}", made.text());
}

/// Object sizes across the layout: an empty one, under a segment, past one, past a block's chunk.
const SIZES: [usize; 6] = [0, 1, 70_001, 1 << 20, 3_000_017, (8 << 20) + 5];

/// A cell of three: a volume lost while objects are written, its node killed and started again
/// without it, and another lost at rest, its node stopped in order. Each is gone at its node's
/// enlistment, and its chunks are rebuilt at once onto the volume its node started with, from the
/// copies the others hold; every object reads back through both doors, and every block's places
/// hold their chunks on disk, none on a gone volume.
#[test]
fn three_nodes_rebuild_a_volume_lost_mid_write_and_one_lost_at_rest() {
    let mut cell = Cell::made(|root| founders(root, 3, 1, ""));
    let g = groups(&cell);
    ready(&mut cell, "b");
    let mut objects = put_objects(&mut cell, "b", "before", &SIZES, 1);
    let ids: Vec<u64> = cell.running.keys().copied().collect();

    // Mid-write: objects put through the first node while the third is killed and its volume
    // lost; each acknowledged is the cell's.
    let lost = ids[2];
    let dirs = volume_dirs(&cell, lost);
    let writer = {
        let (settings, bound) = (
            cell.pki.object_settings(object_client(CLIENT + 1)),
            cell.bound_quic(),
        );
        let first = ids[0];
        std::thread::spawn(move || tolerant_puts(settings, bound, first, "b", "during", 12, 2))
    };
    cell.kill(lost);
    for dir in &dirs {
        lose_volume(dir);
    }
    cell.start(lost);
    cell.introduce();
    cell.enlisted(lost);
    let acknowledged = writer.join().unwrap();
    objects.extend(acknowledged);
    let repaired = settled(&mut cell, |r| {
        r.waiting == 0 && r.short == 0 && r.rebuilt > 0
    });
    assert_eq!(
        repaired.unplaced + repaired.unreadable + repaired.corrupt,
        0,
        "{repaired:?}"
    );
    read_back(&mut cell, "b", &objects, &BTreeSet::new());
    let keys: Vec<String> = objects.keys().cloned().collect();
    let (blocks, _) = whole_on_disk(&mut cell, g, "b", &keys);
    assert!(blocks > 0);

    // At rest: the second node stopped in order and started again without its volume.
    let rested = ids[1];
    let rebuilt = repaired.rebuilt;
    assert!(cell.stop(rested).success());
    for dir in &volume_dirs(&cell, rested) {
        lose_volume(dir);
    }
    cell.start(rested);
    cell.introduce();
    cell.enlisted(rested);
    let repaired = settled(&mut cell, |r| {
        r.waiting == 0 && r.short == 0 && r.rebuilt > rebuilt
    });
    assert_eq!(
        repaired.unplaced + repaired.unreadable + repaired.corrupt,
        0,
        "{repaired:?}"
    );
    read_back(&mut cell, "b", &objects, &BTreeSet::new());
    whole_on_disk(&mut cell, g, "b", &keys);
    // Each lost volume, once nothing names it, the table forgets: one volume a node.
    cell.until(|c| {
        if table(c, g).len() == 3 {
            Some(())
        } else {
            None
        }
    });
}

/// A cell of five: two volumes lost at once are rebuilt, the blocks they left with the fewest
/// chunks to spare first, and every object reads back whole. Then more volumes lost at once than a
/// block's code spares: the cell rebuilds every block it can, says how many it lost for good, and
/// answers a read of a lost object with a typed refusal through both doors, never with bytes;
/// every other object reads back whole.
#[test]
fn five_nodes_survive_two_lost_volumes_and_refuse_what_more_losses_took() {
    let mut cell = Cell::made(|root| founders(root, 5, 1, ""));
    let g = groups(&cell);
    ready(&mut cell, "b");
    // Each object one block, so a lost one is refused before any byte of it goes.
    let sizes: Vec<usize> = (0..24).map(|i| SIZES[i % (SIZES.len() - 1)] + i).collect();
    let objects = put_objects(&mut cell, "b", "o", &sizes, 3);
    let ids: Vec<u64> = cell.running.keys().copied().collect();
    let keys: Vec<String> = objects.keys().cloned().collect();
    let (_, header, _) = blocks_of(&mut cell, g, "b", &keys[1])[0];
    let spare = usize::from(header.parity);

    // Two at once, within what a block spares where it spares two.
    let two: Vec<u64> = ids[3..5].to_vec();
    lose_at_once(&mut cell, &two);
    if spare >= 2 {
        let repaired = settled(&mut cell, |r| {
            r.waiting == 0 && r.short == 0 && r.rebuilt > 0
        });
        assert_eq!(repaired.lost, 0, "{repaired:?}");
        read_back(&mut cell, "b", &objects, &BTreeSet::new());
        whole_on_disk(&mut cell, g, "b", &keys);
    }

    // More than any block spares: some blocks lose every place of a chunk.
    let more: Vec<u64> = ids[..=spare].to_vec();
    lose_at_once(&mut cell, &more);
    let repaired = settled(&mut cell, |r| r.waiting == 0);
    // Which objects lost a block: each block of theirs whose chunks no volume left holds.
    let placed = table(&mut cell, g);
    let node_of: BTreeMap<u128, u64> = placed.iter().map(|p| (p.volume, p.node)).collect();
    let mut lost = BTreeSet::new();
    let mut lost_blocks = 0;
    for key in &keys {
        for (_, header, places) in blocks_of(&mut cell, g, "b", key) {
            let read = on_disk(&mut cell, &places, header.chunk_len, &node_of);
            let held: BTreeSet<u16> = places
                .iter()
                .zip(&read)
                .filter(|(_, b)| b.is_some())
                .map(|(p, _)| p.key.index)
                .collect();
            if held.len() < usize::from(header.data) {
                lost.insert(key.clone());
                lost_blocks += 1;
            }
        }
    }
    assert_eq!(
        repaired.lost,
        u64::try_from(lost_blocks).unwrap(),
        "{repaired:?}"
    );
    assert_eq!(
        repaired.short,
        u64::try_from(lost_blocks).unwrap(),
        "{repaired:?}"
    );
    read_back(&mut cell, "b", &objects, &lost);
    let kept: Vec<String> = keys
        .iter()
        .filter(|k| !lost.contains(*k))
        .cloned()
        .collect();
    whole_on_disk(&mut cell, g, "b", &kept);
}

/// Nodes `ids` killed, each volume of theirs lost, and started again at once.
#[track_caller]
fn lose_at_once(cell: &mut Cell, ids: &[u64]) {
    let dirs: Vec<(u64, Vec<PathBuf>)> =
        ids.iter().map(|id| (*id, volume_dirs(cell, *id))).collect();
    for &id in ids {
        cell.kill(id);
    }
    for (_, dirs) in &dirs {
        for dir in dirs {
            lose_volume(dir);
        }
    }
    for &id in ids {
        cell.start(id);
    }
    cell.introduce();
    for &id in ids {
        cell.enlisted(id);
    }
}

/// A node stopped (SIGSTOP) for as long as the cell takes to judge it away and plan every block,
/// as a laptop asleep: within the durability model's time for a repair, a day, nothing of it is
/// rebuilt but the one block that times a rebuild; objects written meanwhile are placed on the
/// nodes the cell sees, none on it; every object reads back; and once it wakes (SIGCONT) its
/// chunks count again, every block whole on disk.
#[test]
fn a_node_asleep_is_waited_for_and_writes_go_around_it() {
    let mut cell = Cell::made(|root| founders(root, 5, 1, ""));
    let g = groups(&cell);
    ready(&mut cell, "b");
    let mut objects = put_objects(&mut cell, "b", "before", &SIZES, 5);
    let ids: Vec<u64> = cell.running.keys().copied().collect();
    let asleep = ids[4];
    let volume = cell.volumes[&asleep];
    hold::still(cell.running[&asleep].child.id());
    // The cell judges it away: its blocks wait for it.
    let waiting = settled(&mut cell, |r| r.waiting > 0);
    assert_eq!(waiting.rebuilt, waiting.timing, "{waiting:?}");
    assert!(waiting.timing <= 1, "{waiting:?}");
    // Written while it sleeps: placed around it.
    let during = put_objects_on(&mut cell, &ids[..4], "b", "asleep", &SIZES, 6);
    for key in during.keys() {
        for (block, _, places) in blocks_of(&mut cell, g, "b", key) {
            assert!(
                places.iter().all(|p| p.volume != volume),
                "{key} {block:x} placed on the sleeping node's volume"
            );
        }
    }
    objects.extend(during);
    read_back_through(&mut cell, &ids[..4], "b", &objects);
    hold::go_on(cell.running[&asleep].child.id());
    // Back: its return recorded by every node, itself among them, as each applies the root range's
    // record of it (membership.md §5), so each reads through it and it through each again.
    cell.until(|c| {
        let back = ids.iter().all(|o| {
            c.standings
                .get(&(*o, asleep))
                .is_some_and(|(_, since)| *since == 0)
        });
        if back { Some(()) } else { None }
    });
    let woke = settled(&mut cell, |r| r.waiting == 0 && r.short == 0);
    assert_eq!(woke.rebuilt, woke.timing, "{woke:?}");
    read_back(&mut cell, "b", &objects, &BTreeSet::new());
    let keys: Vec<String> = objects.keys().cloned().collect();
    whole_on_disk(&mut cell, g, "b", &keys);
}

/// A node killed (SIGKILL) and stated gone, as a supervisor states a machine it knows dead
/// (membership.md §5): every volume of it gone with it, so its chunks are rebuilt on the others, no
/// place on its volumes is kept, and the table forgets them; started again on its volume, its
/// identity is refused at every node. A node that joins while objects are written is enlisted and
/// offered new blocks, every object reading back and every block whole. The cell's own judgment, a
/// member gone once absent past the durability model's repair time, is `members::gone_by`'s and
/// tested there: a repair time a test waits out is one a node of a host held that long passes while
/// it runs, judged gone as well.
#[test]
fn a_node_stated_gone_is_written_off_and_refused_and_a_joiner_takes_new_blocks() {
    // Two volumes a node, so a chunk of the gone node has a volume to be rebuilt on.
    let mut cell = Cell::made_partly(|root| founders(root, 5, 2, ""), 4);
    step(&cell, "made");
    let g = groups(&cell);
    ready(&mut cell, "b");
    step(&cell, "ready");
    let mut objects = put_objects(&mut cell, "b", "before", &SIZES, 7);
    step(&cell, "put before");
    let gone = follower(&cell);
    let others: Vec<u64> = cell
        .running
        .keys()
        .copied()
        .filter(|id| *id != gone)
        .collect();
    let theirs = volumes_of(&mut cell, g, gone);
    step(&cell, &format!("killing gone {gone}"));
    cell.kill(gone);
    // Stated gone on a member's standard input: its volumes written off with it and, once no block
    // names them, forgotten.
    cell.say_to(others[0], format!("gone {gone}\n").as_bytes());
    let said = cell.until(|c| c.gone.get(&gone).cloned());
    assert!(said.contains("Gone"), "gone {gone}: {said}");
    forgotten(&mut cell, g, gone);
    let rebuilt = settled(&mut cell, |r| {
        r.waiting == 0 && r.short == 0 && r.rebuilt > 0
    });
    assert_eq!(
        rebuilt.unplaced + rebuilt.unreadable + rebuilt.corrupt,
        0,
        "{rebuilt:?}"
    );
    let keys: Vec<String> = objects.keys().cloned().collect();
    for key in &keys {
        for (block, _, places) in blocks_of(&mut cell, g, "b", key) {
            assert!(
                places.iter().all(|p| !theirs.contains(&p.volume)),
                "{key} {block:x} keeps a place on the gone node's volume"
            );
        }
    }
    step(&cell, "read back after gone");
    read_back(&mut cell, "b", &objects, &BTreeSet::new());
    let (_, places) = whole_on_disk(&mut cell, g, "b", &keys);
    assert!(places > 0);
    step(&cell, "whole after gone; starting gone again");
    // Started again on its volume, it finds its identity refused: each link it keeps to a node
    // that runs ends refused.
    cell.start(gone);
    cell.introduce();
    cell.until(|c| {
        let refused = others.iter().all(|peer| {
            c.links
                .get(&(gone, *peer))
                .is_some_and(|link| link.ends_with("Identity"))
        });
        if refused { Some(()) } else { None }
    });
    step(&cell, "refused; stopping gone");
    let exited = cell.stop(gone);
    assert!(
        exited.success(),
        "the refused node stopped in order exited {exited}"
    );
    step(&cell, "gone stopped");
    let back = settled(&mut cell, |r| r.waiting == 0 && r.short == 0);

    // The fifth founder joins while objects are written: its volume new to the table.
    let joining = *cell
        .made
        .keys()
        .find(|id| !cell.running.contains_key(id) && **id != gone)
        .unwrap();
    let writer = {
        let (settings, bound) = (
            cell.pki.object_settings(object_client(CLIENT + 1)),
            cell.bound_quic(),
        );
        let first = others[0];
        std::thread::spawn(move || tolerant_puts(settings, bound, first, "b", "joining", 12, 8))
    };
    step(&cell, &format!("starting joiner {joining}"));
    cell.start(joining);
    cell.introduce();
    cell.enlisted(joining);
    step(&cell, "joiner enlisted; joining writer");
    objects.extend(writer.join().unwrap());
    step(&cell, "writer joined");
    assert!(!volumes_of(&mut cell, g, joining).is_empty());
    settled(&mut cell, |r| {
        r.waiting == 0 && r.short == 0 && r.root_applied > back.root_applied
    });
    step(&cell, "settled after join");
    objects.extend(put_objects(&mut cell, "b", "after", &SIZES, 9));
    step(&cell, "put after");
    read_back(&mut cell, "b", &objects, &BTreeSet::new());
    let keys: Vec<String> = objects.keys().cloned().collect();
    whole_on_disk(&mut cell, g, "b", &keys);
    step(&cell, "end");
}

/// A node that leaves while objects are written: its volumes take no new chunk, every chunk they
/// held moves to the others, and once they hold nothing the table writes them off and forgets them;
/// the node, refused once the cell removes it, then ends well, and every object reads back through
/// the rest, every block whole on disk. Two volumes a node: a block as wide as the cell keeps a
/// chunk to a volume on the four left.
#[test]
fn a_node_leaving_mid_write_moves_everything_off_and_is_forgotten() {
    let mut cell = Cell::made(|root| founders(root, 5, 2, ""));
    let g = groups(&cell);
    ready(&mut cell, "b");
    let mut objects = put_objects(&mut cell, "b", "before", &SIZES, 11);
    let ids: Vec<u64> = cell.running.keys().copied().collect();
    let leaving = ids[4];
    let writer = {
        let (settings, bound) = (
            cell.pki.object_settings(object_client(CLIENT + 1)),
            cell.bound_quic(),
        );
        let first = ids[0];
        std::thread::spawn(move || tolerant_puts(settings, bound, first, "b", "leaving", 12, 12))
    };
    leave(&mut cell, leaving);
    // Its end is the leave's last step (membership.md §4 step 5): once its volumes are forgotten,
    // the root range removes and delists it, its peers refuse it and it ends. Taken as expected
    // from here, and waited for once the table has forgotten its volumes.
    cell.refusing = Some(leaving);
    objects.extend(writer.join().unwrap());
    forgotten(&mut cell, g, leaving);
    let exited = cell.ended();
    assert!(exited.success(), "the node that left exited {exited}");
    let keys: Vec<String> = objects.keys().cloned().collect();
    read_back(&mut cell, "b", &objects, &BTreeSet::new());
    whole_on_disk(&mut cell, g, "b", &keys);
}

/// A cell of three nodes of two volumes each, a fourth founder not yet started: a node leaves, and
/// with two domains left that take chunks, each chunk of a block as wide as the cell it held moves
/// onto the other volume of a domain holding one of its block's already. Once the fourth joins, the
/// domain rule moves one of each such pair onto it, and every block's chunks are on distinct nodes.
/// The node that left ends well once the cell has removed it, before the fourth joins or after, as
/// its ranges' quorums let them drop its vote.
#[test]
fn chunks_a_leaving_node_doubled_on_a_domain_spread_onto_a_joiner() {
    let mut cell = Cell::made_partly(|root| founders(root, 4, 2, ""), 3);
    step(&cell, "made doubled");
    let g = groups(&cell);
    ready(&mut cell, "b");
    let mut objects = put_objects(&mut cell, "b", "before", &SIZES, 12);
    let leaving = follower(&cell);
    leave(&mut cell, leaving);
    // Its end is the leave's last step (membership.md §4 step 5): the ranges drop it, the root range
    // removes and delists it, its peers refuse it and it ends, whether or not the fourth founder has
    // joined by then (of the three voters left, the two running make a quorum). Taken as expected
    // from here, and waited for at the end.
    cell.refusing = Some(leaving);
    forgotten(&mut cell, g, leaving);
    let keys: Vec<String> = objects.keys().cloned().collect();
    // Each block as wide as the three domains it was written over now doubles one of the two left.
    let spread = |cell: &mut Cell| {
        let placed = table(cell, g);
        let node_of: BTreeMap<u128, u64> = placed.iter().map(|p| (p.volume, p.node)).collect();
        let (mut wide, mut doubled) = (0, 0);
        for key in &keys {
            for (_, header, places) in blocks_of(cell, g, "b", key) {
                let width = usize::from(header.data) + usize::from(header.parity);
                let nodes: BTreeSet<u64> =
                    places[..width].iter().map(|p| node_of[&p.volume]).collect();
                wide += usize::from(width == 3);
                doubled += usize::from(nodes.len() < width);
            }
        }
        (wide, doubled)
    };
    let (wide, doubled) = spread(&mut cell);
    assert!(wide > 0, "no block as wide as the three domains");
    assert_eq!(
        doubled, wide,
        "every block of three over two domains doubles one"
    );
    // The founder never started: the node that left, ended, runs no more either.
    let joining = *cell
        .made
        .keys()
        .find(|id| !cell.running.contains_key(id) && **id != leaving)
        .unwrap();
    step(&cell, &format!("doubled: starting joiner {joining} wide={wide}"));
    cell.start(joining);
    cell.introduce();
    cell.enlisted(joining);
    let mark = root_commit(&cell);
    step(&cell, &format!("doubled: joiner enlisted mark={mark}"));
    let at = settled_after(&mut cell, mark, |r| r.waiting == 0 && r.short == 0);
    step(&cell, &format!("doubled: settled {at:?} leading={:?}", cell.repairs.iter().filter(|(_, r)| r.leading).map(|(n, _)| *n).collect::<Vec<_>>()));
    let doubling = spread(&mut cell).1;
    step(&cell, &format!("doubled: doubling={doubling}"));
    if doubling != 0 {
        // Whether each node held the joiner present: a domain takes chunks only while its node is.
        let joiner: Vec<_> = cell
            .standings
            .iter()
            .filter(|((_, member), _)| *member == joining)
            .collect();
        panic!(
            "{doubling} blocks still double a domain, the pass begun at {mark} or later settled as \
             {at:?}; the joiner {joining} as each node holds it: {joiner:?}"
        );
    }
    objects.extend(put_objects(&mut cell, "b", "after", &SIZES, 13));
    read_back(&mut cell, "b", &objects, &BTreeSet::new());
    let keys: Vec<String> = objects.keys().cloned().collect();
    whole_on_disk(&mut cell, g, "b", &keys);
    let exited = cell.ended();
    assert!(exited.success(), "the node that left exited {exited}");
}

/// Puts through the S3 listeners of `nodes` alone.
fn put_objects_on(
    cell: &mut Cell,
    nodes: &[u64],
    bucket: &str,
    prefix: &str,
    sizes: &[usize],
    seed: u64,
) -> Objects {
    let mut s3 = listeners(cell);
    let mut objects = Objects::new();
    for (i, &size) in sizes.iter().enumerate() {
        let key = format!("{prefix}-{i}");
        let data = object_bytes(seed * 1000 + i as u64, size);
        let node = nodes[i % nodes.len()];
        let put = s3
            .get_mut(&node)
            .unwrap()
            .send("PUT", &format!("/{bucket}/{key}"), &[], &data);
        assert_eq!(put.status, 200, "{key} through {node}: {}", put.text());
        objects.insert(key, data);
    }
    objects
}

/// Reads through the S3 listeners of `nodes` alone.
fn read_back_through(cell: &mut Cell, nodes: &[u64], bucket: &str, objects: &Objects) {
    let mut s3 = listeners(cell);
    for (key, data) in objects {
        for node in nodes {
            let got = s3
                .get_mut(node)
                .unwrap()
                .send("GET", &format!("/{bucket}/{key}"), &[], b"");
            assert_eq!(got.status, 200, "{key} through {node}: {}", got.text());
            assert!(got.body == *data, "{key} through {node}: other bytes");
        }
    }
}

/// Puts `count` objects named `prefix-i` through node `node`'s native door with no deadline of
/// their own, each kept where the node acknowledged it and let go where it refused it or its answer
/// was lost: what the cell acknowledged, by key.
fn tolerant_puts(
    settings: ClientSettings,
    bound: Vec<(u64, SocketAddr)>,
    node: u64,
    bucket: &str,
    prefix: &str,
    count: u64,
    seed: u64,
) -> Objects {
    let bucket = bucket.to_owned();
    let prefix = prefix.to_owned();
    client_runtime(settings.nodes.len() + 1)
        .block_on(async move {
            let mut client = Client::open(settings).await.unwrap();
            for (id, quic) in bound {
                client.learn(id, quic).unwrap();
            }
            let mut acknowledged = Objects::new();
            for i in 0..count {
                let key = format!("{prefix}-{i}");
                let data = object_bytes(
                    seed * 1000 + i,
                    SIZES[usize::try_from(i).unwrap() % SIZES.len()],
                );
                let op = put_op(&bucket, &key, u128::from(seed << 32 | i), data.len() as u64);
                if let Ok(ObjectOutcome::Replied { reply, .. }) =
                    client.object(node, &op, &data, u64::MAX).await
                    && reply.status == ObjectStatus::Done
                {
                    acknowledged.insert(key, data);
                }
            }
            acknowledged
        })
        .unwrap()
}
