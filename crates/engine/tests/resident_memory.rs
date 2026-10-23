//! What a built branch holds in memory while it lives, counted by hyper-measure's counting
//! allocator as the bytes its drop frees, against what `mantle_engine` states: for the branch
//! (`branch::resident_bytes`) and for each extent it holds (`shard_db::resident_per_extent`), at
//! a range's floor (its page sized by the range's longest key and largest row, mantle_meta's
//! bounds). One branch of each shape: the densest rows, numbers and object names with values, keys
//! in pairs sharing all but a last byte at 64 and at 2,132 bytes and at 16 bytes, those also parting
//! at every depth (the range filter's worst within its depth, research/67), and the densest rows as a
//! compaction's last branch builds them, its point filter made for eight times the keys it got
//! and for a bound whose halves stop at an odd count.
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
use hyper_measure::alloc;
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{self, Branch, Builder, Op};
use mantle_engine::shard_db;
use mantle_engine::store::{Config, Store};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

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

/// 16-byte keys in pairs, each pair's number in its first two bytes; a pair's two keys part at
/// their last byte, or, `everywhere`, one pair in eight at a byte from the third to the fifteenth
/// by its number: nodes of two labels at every depth.
fn pairs_parting(n: u64, everywhere: bool) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            let pair = i / 2;
            let mut k = vec![b'x'; 16];
            k[..2].copy_from_slice(&u16::try_from(pair).unwrap().to_be_bytes());
            let part = if everywhere && pair % 8 == 0 {
                2 + usize::try_from((pair / 8) % 14).unwrap()
            } else {
                15
            };
            k[part] = if i % 2 == 0 { b'a' } else { b'b' };
            k
        })
        .collect()
}

fn sorted(mut keys: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    keys.sort();
    keys.dedup();
    keys
}

/// The bytes a branch holds: what its drop frees, counted, and its record.
fn held(branch: Branch) -> u64 {
    alloc::begin();
    drop(branch);
    let freed = -alloc::end().live;
    u64::try_from(freed).unwrap() + std::mem::size_of::<Branch>() as u64
}

/// One branch of `keys` with values of `value` bytes at the floor's page, its point filter made
/// for `bound`: what it holds against what is stated for it and for its extents.
fn measure(name: &str, keys: &[Vec<u8>], value: usize, bound: Keys) {
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
    let bits = store.range_filter_bits();
    let value = vec![b'v'; value];
    let mut b = Builder::new(&mut store, bound).unwrap();
    // What every key shares: its first's with its last, as a packing states its memtable's.
    let shared = keys.first().zip(keys.last()).map_or(0, |(a, z)| {
        a.iter().zip(z).take_while(|(x, y)| x == y).count()
    });
    b.set_shared(shared).unwrap();
    for k in keys {
        b.add(&mut store, k, Op::Put, &value).unwrap();
    }
    let branch = b.finish(&mut store).unwrap();
    let pages = branch.counts.len() as u64;
    let leaves = branch.counts.iter().filter(|&&c| c > 0).count() as u64;
    let extents = branch.extents.len() as u64;
    let entries = branch.count;
    // What the trunk counts a branch holding ([`mantle_engine::trunk::Trunk::memory`]).
    let counted = (branch.filter.held_bytes()
        + branch.index.held_bytes()
        + branch.range.held_bytes()
        + branch.counts.capacity() * 2
        + branch.extents.capacity() * 8
        + std::mem::size_of::<Branch>()) as u64;
    let parts = (
        branch.filter.held_bytes(),
        branch.range.held_bytes(),
        branch.index.held_bytes(),
    );
    let held = held(branch);
    assert_eq!(held, counted, "{name}: freed and counted differ");
    // An extent holds at most its pages' entries at their least.
    let capacity = mantle_engine::store::page::capacity(page);
    let most = branch::most_entries(capacity).unwrap() * u64::from(config.extent_pages);
    assert!(
        entries <= most * extents,
        "{name}: {entries} entries in {extents} extents past {most} an extent"
    );
    let longest = keys.iter().map(Vec::len).max().unwrap();
    let stated =
        branch::resident_bytes(capacity, longest, entries, leaves, pages, extents, bits).unwrap();
    // The engine's statement: the extents it holds at the range's longest key, and at this
    // shape's own.
    let range_extent = shard_db::resident_per_extent(&config, KEY, bits).unwrap();
    let own_extent = shard_db::resident_per_extent(&config, longest, bits).unwrap();
    let data = pages * page as u64;
    eprintln!(
        "{name}: entries {entries}, pages {pages} ({leaves} leaves), extents {extents}, data {data}: held {held} ({:.2}% of data, {:.1} B/entry; filter {}, range {}, index {}), branch stated {stated} ({:.2}x held), per extent held {:.0}, stated {own_extent} at {longest} B keys ({:.2}x), {range_extent} at {KEY} B ({:.2}x)",
        held as f64 * 100.0 / data as f64,
        held as f64 / entries as f64,
        parts.0,
        parts.1,
        parts.2,
        stated as f64 / held as f64,
        held as f64 / extents as f64,
        (own_extent * extents) as f64 / held as f64,
        (range_extent * extents) as f64 / held as f64,
    );
    assert!(held <= stated, "{name}: held {held} past {stated} stated");
    assert!(
        held <= own_extent * extents,
        "{name}: held {held} past {own_extent} an extent of {extents}"
    );
    assert!(
        held <= range_extent * extents,
        "{name}: held {held} past {range_extent} an extent of {extents}"
    );
}

#[test]
fn a_branch_holds_no_more_than_is_stated_for_it_and_its_extents() {
    let mut r = Rng(7);
    let densest = sorted((0..100_000u64).map(|i| i.to_be_bytes().to_vec()).collect());
    let exactly = |keys: &[Vec<u8>]| Keys::Exactly(keys.len() as u64);
    measure("densest 8 B, no values", &densest, 0, exactly(&densest));
    let numbers = sorted(
        (0..100_000)
            .map(|_| {
                let mut k = r.next().to_be_bytes().to_vec();
                k.resize(16, b'0');
                k
            })
            .collect(),
    );
    measure(
        "numbers 16 B, 100 B values",
        &numbers,
        100,
        exactly(&numbers),
    );
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
    measure(
        "object names, 100 B values",
        &objects,
        100,
        exactly(&objects),
    );
    let pairs_64 = pairs(20_000, 64);
    measure("pairs of 64 B, no values", &pairs_64, 0, exactly(&pairs_64));
    let apart = sorted(pairs_parting(20_000, false));
    measure(
        "pairs of 16 B, a byte apart, no values",
        &apart,
        0,
        exactly(&apart),
    );
    let parting = sorted(pairs_parting(20_000, true));
    measure(
        "pairs of 16 B parting at every depth, no values",
        &parting,
        0,
        exactly(&parting),
    );
    let pairs_long = pairs(4_000, KEY);
    measure(
        "pairs of 2,132 B, no values",
        &pairs_long,
        0,
        exactly(&pairs_long),
    );
    let pairs_largest = pairs(1_000, KEY);
    measure(
        "pairs of 2,132 B, largest values",
        &pairs_largest,
        VALUE,
        exactly(&pairs_largest),
    );
    // A compaction's last branch: its filter made for the inputs' entries, which its merge cut
    // to an eighth.
    measure(
        "densest 8 B as a compaction's last branch, an eighth of its inputs",
        &densest,
        0,
        Keys::AtMost(8 * densest.len() as u64),
    );
    // 30,000 keys of a bound of 400,000: halves alone stopped at an odd 3,907 blocks, 66 bits a
    // key.
    let few = &densest[..30_000];
    measure(
        "densest 8 B, 30,000 of a bound of 400,000",
        few,
        0,
        Keys::AtMost(400_000),
    );
}
