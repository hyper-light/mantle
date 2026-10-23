//! What a branch's two tries hold against the data they cover, at a range's floor (its page sized
//! by the range's longest key and largest row, mantle_meta's bounds): the range filter and the
//! leaf index of one branch of each shape, the densest rows, the shard benchmark's, object names,
//! paths through two random directories, and keys in pairs sharing all but a last byte (SuRF's
//! worst case) at 64 and at 2,132 bytes (research/67).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{self, Builder, Op};
use mantle_engine::store::{Config, Store};

/// The range's bounds on a row's key and value (mantle_meta's `key::largest_bytes` and
/// `wire::largest_row_bytes`), which size the floor's page.
const KEY: usize = 2_132;
const VALUE: usize = 18_699;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

fn pairs(n: u64, len: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            let mut k = (i / 2).to_be_bytes().to_vec();
            k.resize(len - 1, b'x');
            k.push(if i % 2 == 0 { b'a' } else { b'b' });
            k
        })
        .collect()
}

fn sorted(mut keys: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    keys.sort();
    keys.dedup();
    keys
}

/// One branch of `keys` with values of `value` bytes at the floor's page: its data's bytes
/// (tree pages), leaves, and the range filter's and leaf index's bytes held.
fn measure(name: &str, keys: &[Vec<u8>], value: usize) {
    let page = branch::least_page(4096, KEY, VALUE).unwrap();
    let config = Config {
        page_size: page,
        extent_pages: 2,
        max_extents: 1 << 20,
    };
    let dir = tempfile::tempdir().unwrap();
    let file = DeviceFile::open(
        &dir.path().join("store"),
        true,
        CachingRequest::Buffered,
        Alignment::new(4096).unwrap(),
    )
    .unwrap();
    let mut store = Store::create(file, config).unwrap();
    let value = vec![b'v'; value];
    let mut b = Builder::new(&mut store, Keys::Exactly(keys.len() as u64)).unwrap();
    // What every key shares: its first's with its last, as a packing states its memtable's.
    let shared = keys.first().zip(keys.last()).map_or(0, |(a, z)| {
        a.iter().zip(z).take_while(|(x, y)| x == y).count()
    });
    b.set_shared(shared).unwrap();
    for k in keys {
        b.add(&mut store, k, Op::Put, &value).unwrap();
    }
    let branch = b.finish(&mut store).unwrap();
    let pages = branch.counts.len();
    let leaves = branch.counts.iter().filter(|&&c| c > 0).count();
    let data = pages * page;
    let key_bytes: usize = keys.iter().map(Vec::len).sum();
    let (range, index) = (branch.range.held_bytes(), branch.index.held_bytes());
    // How far past the bytes every key shares its leaves' separators reach: each leaf's first key
    // cut one byte past what it shares with the leaf before's last.
    let shared = keys.first().zip(keys.last()).map_or(0, |(a, z)| {
        a.iter().zip(z).take_while(|(x, y)| x == y).count()
    });
    let mut at = 0usize;
    let mut deepest = 0usize;
    for &n in branch.counts.iter().filter(|&&c| c > 0) {
        if at > 0 {
            let (last, first) = (&keys[at - 1], &keys[at]);
            let lcp = last.iter().zip(first).take_while(|(x, y)| x == y).count();
            deepest = deepest.max((lcp + 1).min(first.len()).saturating_sub(shared));
        }
        at += usize::from(n);
    }
    // What the statement says the two tries take for these keys, its key the longest.
    let longest = keys.iter().map(Vec::len).max().unwrap();
    let stated = branch::tries_bytes(
        longest,
        keys.len() as u64,
        leaves as u64,
        store.range_filter_bits(),
    )
    .unwrap();
    assert!(
        (range + index) as u64 <= stated,
        "{name}: tries {} past {stated} stated",
        range + index
    );
    eprintln!(
        "{name}: keys {} ({:.0} B avg), page {page}, leaves {leaves}, data {data}: range filter {range} ({:.2}% of data, {:.1} B/key), leaf index {index} ({:.2}% of data, {:.0} B/leaf), separators to {deepest} past {shared} shared, stated {stated}",
        keys.len(),
        key_bytes as f64 / keys.len() as f64,
        range as f64 * 100.0 / data as f64,
        range as f64 / keys.len() as f64,
        index as f64 * 100.0 / data as f64,
        index as f64 / leaves.max(1) as f64,
    );
}

#[test]
fn a_branches_tries_against_its_data() {
    let mut r = Rng(7);
    let densest = sorted((0..100_000u64).map(|i| i.to_be_bytes().to_vec()).collect());
    measure("densest 8 B, no values", &densest, 0);
    let numbers = sorted(
        (0..100_000)
            .map(|_| {
                let mut k = r.next().to_be_bytes().to_vec();
                k.resize(16, b'0');
                k
            })
            .collect(),
    );
    measure("numbers 16 B, 100 B values", &numbers, 100);
    let objects = sorted(
        (0..100_000)
            .map(|_| {
                format!(
                    "tenant-{:04}/agent-{:06}/run-{:08}.json",
                    r.next() % 4_000,
                    r.next() % 100_000,
                    r.next() % 100_000_000
                )
                .into_bytes()
            })
            .collect(),
    );
    measure("object names, 100 B values", &objects, 100);
    let paths = sorted(
        (0..100_000)
            .map(|_| {
                format!(
                    "tenant-{:02}/{:016x}{:016x}/{:016x}{:016x}/part-{:05}.parquet",
                    r.next() % 64,
                    r.next() % 4_000,
                    0u64,
                    r.next() % 16,
                    0u64,
                    r.next() % 2_000
                )
                .into_bytes()
            })
            .collect(),
    );
    measure("uuid paths, 100 B values", &paths, 100);
    // A data lake's: tables, day and hour partitions, part files named by a uuid.
    let lake = sorted(
        (0..100_000)
            .map(|_| {
                let (day, hour) = (r.next() % 30, r.next() % 24);
                format!(
                    "warehouse/events_db/clicks/date=2026-10-{:02}/hour={hour:02}/part-{:05}-{:016x}{:016x}-c000.snappy.parquet",
                    day + 1,
                    r.next() % 200,
                    r.next(),
                    r.next()
                )
                .into_bytes()
            })
            .collect(),
    );
    measure("data-lake paths, 100 B values", &lake, 100);
    measure("pairs of 64 B, no values", &pairs(20_000, 64), 0);
    measure("pairs of 2,132 B, no values", &pairs(4_000, KEY), 0);
    measure(
        "pairs of 2,132 B, largest values",
        &pairs(1_000, KEY),
        VALUE,
    );
}
