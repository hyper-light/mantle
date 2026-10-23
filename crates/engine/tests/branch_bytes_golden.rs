//! The bytes branches are written as, pinned: branches of four kinds of entry built in a store,
//! compactions merging and cutting them, then the store checkpointed, its durable image and each
//! branch's descriptor hashed and checked against the digests the builder and the merge wrote
//! before. A change to how pages are staged, encoded, queued or compared that writes a different
//! byte fails it; one that only does the same work faster passes.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

#[path = "support/into_file.rs"]
mod file_outcome;
use file_outcome::finished_file;

use hyper_block::buf::Alignment;
use hyper_block::sim::SimFile;
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::merge::Compaction;
use mantle_engine::branch::{Branch, Builder, Op};
use mantle_engine::store::{Config, Store};
use std::collections::BTreeMap;

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 32,
    max_extents: 4096,
};

type Entries = BTreeMap<Vec<u8>, (Op, Vec<u8>)>;

/// FNV-1a over 64 bits: a fixed digest whose value depends on every byte and its place.
fn fnv(bytes: &[u8], mut h: u64) -> u64 {
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

const FNV_START: u64 = 0xcbf2_9ce4_8422_2325;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn build(store: &mut Store<SimFile>, entries: &Entries) -> Branch {
    let mut b = Builder::new(store, Keys::Exactly(entries.len() as u64)).unwrap();
    for (k, (op, v)) in entries {
        b.add(store, k, *op, v).unwrap();
    }
    b.finish(store).unwrap()
}

/// The ranges bench's keys and values: sixteen-byte zero-padded numbers, 100-byte values, a
/// tombstone in every 29.
fn bench_like(rng: &mut Rng, n: u64, span: u64) -> Entries {
    let mut e = Entries::new();
    while (e.len() as u64) < n {
        let k = rng.next() % span;
        let op = if k.is_multiple_of(29) {
            Op::Delete
        } else {
            Op::Put
        };
        let value = if op == Op::Delete {
            Vec::new()
        } else {
            vec![b'a' + (k % 26) as u8; 100]
        };
        e.insert(format!("{k:016}").into_bytes(), (op, value));
    }
    e
}

/// Object-store-like keys sharing long prefixes, values of 0 to 299 bytes.
fn paths(rng: &mut Rng, n: usize) -> Entries {
    let mut e = Entries::new();
    while e.len() < n {
        let x = rng.next();
        let key = format!(
            "bucket-{}/dir{:03}/dir{:03}/object-{:08}",
            x % 3,
            (x >> 8) % 40,
            (x >> 16) % 40,
            (x >> 24) % 100_000
        );
        let value = vec![(x >> 40) as u8; ((x >> 48) % 300) as usize];
        e.insert(key.into_bytes(), (Op::Put, value));
    }
    e
}

/// Binary keys of 0 to 40 bytes, the empty key among them, tombstones with values.
fn binary(rng: &mut Rng, n: usize) -> Entries {
    let mut e = Entries::from([(Vec::new(), (Op::Put, Vec::new()))]);
    while e.len() < n {
        let x = rng.next();
        let len = (x % 41) as usize;
        let mut key = Vec::with_capacity(len);
        let mut y = rng.next();
        for i in 0..len {
            key.push(match y & 3 {
                0 => 0,
                1 => 255,
                _ => (y >> 8) as u8,
            });
            if i % 8 == 7 {
                y = rng.next();
            } else {
                y >>= 2;
            }
        }
        let op = if x.is_multiple_of(7) {
            Op::Delete
        } else {
            Op::Put
        };
        let value = vec![(x >> 33) as u8; ((x >> 40) % 64) as usize];
        e.insert(key, (op, value));
    }
    e
}

/// Half-page values: more leaves than an index page names, so the tree takes index levels.
fn half_pages(store: &Store<SimFile>, n: u32) -> Entries {
    (0..n)
        .map(|i| {
            let mut key = vec![0, 255];
            key.extend_from_slice(&i.to_be_bytes());
            let value = vec![(i % 251) as u8; store.page_capacity() / 2];
            (key, (Op::Put, value))
        })
        .collect()
}

fn compact(
    store: &mut Store<SimFile>,
    inputs: &[Branch],
    from: &[u8],
    end: Option<&[u8]>,
    drop_tombstones: bool,
) -> Vec<Branch> {
    let mut c = Compaction::new(store, inputs, from, end, drop_tombstones, u64::MAX).unwrap();
    c.step(store, inputs, u64::MAX).unwrap();
    c.finish(store)
        .unwrap()
        .into_iter()
        .map(|(_, b)| b)
        .collect()
}

/// Do: in one store, branches of the bench's entries (two, the newer overwriting and deleting
/// some of the older's keys), of path keys, of binary keys and of half-page values; then a
/// merge of the two bench branches whole, keeping tombstones, one of a range of them dropping
/// tombstones, one of a range of the path branch, and one of the binary and path branches whole;
/// then a checkpoint. Expect: the store's durable image and every branch's descriptor hash to the
/// digests recorded from the builder and merge as they were.
#[test]
fn built_and_compacted_branches_write_the_bytes_they_always_have() {
    let align = Alignment::new(4096).unwrap();
    let mut store = Store::create(
        SimFile::new(align, Alignment::new(512).unwrap(), 41).unwrap(),
        CONFIG,
    )
    .unwrap();
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    let older = bench_like(&mut rng, 20_000, 60_000);
    let newer = bench_like(&mut rng, 6_000, 60_000);
    let path = paths(&mut rng, 5_000);
    let bin = binary(&mut rng, 3_000);
    let half = half_pages(&store, 1_200);
    let mut branches = Vec::new();
    for entries in [&older, &newer, &path, &bin, &half] {
        branches.push(build(&mut store, entries));
    }
    let (b_older, b_newer, b_path, b_bin) = (
        branches[0].clone(),
        branches[1].clone(),
        branches[2].clone(),
        branches[3].clone(),
    );
    // Newest first, as a merge takes its inputs.
    let bench = [b_newer, b_older];
    branches.extend(compact(&mut store, &bench, b"", None, false));
    branches.extend(compact(
        &mut store,
        &bench,
        b"0000000000010000",
        Some(b"0000000000040000"),
        true,
    ));
    branches.extend(compact(
        &mut store,
        std::slice::from_ref(&b_path),
        b"bucket-1/",
        Some(b"bucket-2/dir020/"),
        false,
    ));
    branches.extend(compact(&mut store, &[b_bin, b_path], b"", None, false));
    let mut descriptors = FNV_START;
    let mut descriptor = Vec::new();
    for b in &branches {
        descriptor.clear();
        b.encode(&mut descriptor).unwrap();
        descriptors = fnv(&descriptor, descriptors);
    }
    let root = branches[0].root;
    store.checkpoint(Some(root), 1).unwrap();
    let (file, finished) = finished_file(store.into_file());
    finished.unwrap();
    let image = file.durable_image().unwrap();
    let digest = fnv(&image, FNV_START);
    assert_eq!(
        (branches.len(), image.len(), digest, descriptors),
        (BRANCHES, IMAGE_BYTES, IMAGE_DIGEST, DESCRIPTOR_DIGEST),
        "the branches written: (count, image bytes, image digest, descriptors' digest)"
    );
}

/// Recorded from engine-cap's builder and merge, the trunk image's format 7 (research/67): range
/// filters that state the keys they filter and their reach, the bytes every key shares and a depth
/// past them, an entry that long whole; leaf indexes whose entries are pages doubled (a group's
/// last leaf, its low bit set); tries whose sparse levels are one sequence each. Main 9ccd216's:
/// image digest 9_631_384_443_531_559_731, descriptors' 996_053_338_898_882_066.
const BRANCHES: usize = 9;
const IMAGE_BYTES: usize = 15_994_880;
const IMAGE_DIGEST: u64 = 9_348_659_477_259_526_053;
const DESCRIPTOR_DIGEST: u64 = 4_726_997_388_655_653_146;
