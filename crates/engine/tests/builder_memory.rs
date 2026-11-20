//! What a branch builder holds from its making to its branch, counted by hyper-measure's counting
//! allocator against what `branch::builder_bytes` states: at the range's densest keys, its longest
//! sharing long prefixes and sharing none, its longest with its largest values, and keys in pairs
//! sharing all but their last byte, which give its tries the most labels keys of a length can;
//! and a memtable's packing against what `branch::builder_bytes_of` states for the memtable's
//! bytes (`branch::Built::of`, `memtable::hashed::most_entries`), at the mixes that fill a
//! memtable worst: its most entries, the shortest distinct keys, and the most labels its bytes
//! give, keys at each trie's depth, the longest, and pairs of the longest.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_measure::alloc;
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{self, Builder, Built, Op};
use mantle_engine::memtable::hashed::{self, HashMem};
use mantle_engine::store::{Config, Store};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// The range's bounds on a row's key and value (mantle_meta's `key::largest_bytes` and
/// `wire::largest_row_bytes`).
const KEY: usize = 2_132;
const VALUE: usize = 18_699;

/// Keys of `len` bytes in order: `common` bytes all share, then their number, then filler.
fn sharing(n: u64, len: usize, common: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            let mut k = vec![b'p'; common.min(len)];
            let count = i.to_be_bytes();
            let mut at = 0usize;
            while k.len() < len {
                k.push(count.get(at).copied().unwrap_or(b'z'));
                at += 1;
            }
            k
        })
        .collect()
}

/// Keys in pairs sharing all but their last byte, each pair parting from the one before at its
/// first bytes.
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

/// Builds a branch of `keys`, each with a value of `value` bytes, in a store of its own: the most
/// the build held from the builder's making to its branch, and what the statement says.
fn build(keys: &[Vec<u8>], value: usize) -> (u64, u64) {
    let longest = keys.iter().map(Vec::len).max().unwrap();
    let page = branch::least_page(4096, longest, value).unwrap();
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
    let entries = keys.len() as u64;
    alloc::begin();
    let mut b = Builder::new(&mut store, Keys::Exactly(entries)).unwrap();
    for k in keys {
        b.add(&mut store, k, Op::Put, &value).unwrap();
    }
    let branch = b.finish(&mut store).unwrap();
    let held = alloc::read().peak;
    alloc::end();
    drop(branch);
    let stated = branch::builder_bytes(
        mantle_engine::store::page::capacity(page),
        page * 2,
        longest,
        entries,
        store.range_filter_bits(),
    )
    .unwrap();
    (u64::try_from(held).unwrap(), stated)
}

#[test]
fn a_builder_holds_no_more_than_it_states() {
    let shapes: Vec<(&str, Vec<Vec<u8>>, usize)> = vec![
        ("densest, 100,000", sharing(100_000, 8, 0), 0),
        ("densest, 1,000,000", sharing(1_000_000, 8, 0), 0),
        (
            "longest sharing 2,124, 4,000",
            sharing(4_000, KEY, 2_124),
            0,
        ),
        ("longest sharing none, 4,000", sharing(4_000, KEY, 0), 0),
        (
            "longest with largest values, 1,000",
            sharing(1_000, KEY, 2_124),
            VALUE,
        ),
        ("pairs of the longest, 4,000", pairs(4_000, KEY), 0),
    ];
    for (name, keys, value) in &shapes {
        let (held, stated) = build(keys, *value);
        eprintln!("{name}: held {held} of {stated} stated");
        assert!(held <= stated, "{name}: held {held} past {stated}");
    }
}

/// The shortest key a range writes (mantle_meta's `key::least_bytes`).
const LEAST: usize = 2;

/// Keys of `len` bytes sharing nothing: their number, its low bytes first, then filler.
fn distinct(n: u64, len: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            let mut k: Vec<u8> = i.to_le_bytes().iter().copied().take(len).collect();
            k.resize(len, b'z');
            k
        })
        .collect()
}

/// The shortest distinct keys a range writes, as many as `n`: every key of two bytes, then of
/// three.
fn shortest(n: u64) -> Vec<Vec<u8>> {
    let two = n.min(1 << 16);
    let mut keys = distinct(two, LEAST);
    keys.extend(distinct(n - two, LEAST + 1));
    keys
}

/// The keys a memtable of `limit` bytes takes of `keys`, each with a value of `value` bytes,
/// until it refuses one: checked against the most it holds, its entries and its keys' bytes.
fn filled(limit: usize, keys: Vec<Vec<u8>>, value: usize) -> Vec<Vec<u8>> {
    let mut mem = HashMem::new(limit).unwrap();
    let v = vec![b'v'; value];
    let mut taken = Vec::new();
    for k in keys {
        if mem.insert(&k, Op::Put, &v).is_err() {
            break;
        }
        taken.push(k);
    }
    let most = hashed::most_entries(limit, LEAST).unwrap();
    assert!(taken.len() as u64 <= most, "{} past {most}", taken.len());
    let key_bytes: usize = taken.iter().map(Vec::len).sum();
    assert!(key_bytes <= limit);
    taken.sort();
    taken.dedup();
    taken
}

/// Packs a memtable's `keys`, each with a value of `value` bytes, into a branch of the range's
/// pages in a store of its own, one builder as a packing's part takes it: the most it held, and
/// what the statement says for a memtable of `limit` bytes at the range's longest key.
fn pack(keys: &[Vec<u8>], value: usize, limit: usize) -> (u64, u64) {
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
    alloc::begin();
    let mut b = Builder::new(&mut store, Keys::Exactly(keys.len() as u64)).unwrap();
    for k in keys {
        b.add(&mut store, k, Op::Put, &value).unwrap();
    }
    let branch = b.finish(&mut store).unwrap();
    let held = alloc::read().peak;
    alloc::end();
    drop(branch);
    let built = Built::of(
        KEY,
        hashed::most_entries(limit, LEAST).unwrap(),
        limit as u64,
    )
    .unwrap();
    let stated = branch::builder_bytes_of(
        mantle_engine::store::page::capacity(page),
        page * 2,
        KEY,
        built,
        store.range_filter_bits(),
    )
    .unwrap();
    (u64::try_from(held).unwrap(), stated)
}

/// A memtable's packing holds no more than its builder's statement for the memtable's bytes:
/// its most entries, each of the shortest key, and its keys' bytes, which bound each trie's
/// labels, at a memtable of an entry of the range's largest (561,885 bytes) and one of 4 MiB.
#[test]
fn a_memtables_packing_holds_no_more_than_its_bytes_state() {
    for limit in [561_885usize, 4 << 20] {
        let n = hashed::most_entries(limit, LEAST).unwrap();
        let mut shapes: Vec<(&str, Vec<Vec<u8>>, usize)> = vec![("shortest", shortest(n), 0)];
        if limit < 1 << 20 {
            let longest = (limit / KEY) as u64;
            shapes.extend([
                ("range filter's depth", distinct(n, 16), 0),
                ("leaf index's depth", distinct(n, 256), 0),
                ("longest sharing none", distinct(longest, KEY), 0),
                ("pairs of the longest", pairs(longest, KEY), 0),
                ("longest with largest values", distinct(longest, KEY), VALUE),
            ]);
        }
        for (name, keys, value) in shapes {
            let keys = filled(limit, keys, value);
            let (held, stated) = pack(&keys, value, limit);
            eprintln!(
                "{limit} bytes, {name}: {} entries, held {held} of {stated} stated",
                keys.len()
            );
            assert!(
                held <= stated,
                "{limit} bytes, {name}: held {held} past {stated}"
            );
        }
    }
}
