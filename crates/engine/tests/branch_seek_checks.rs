//! A seek reads the leaf its key falls in and copies out the one entry it lands on, and the
//! leaf's first entry is checked as loading it checks it wherever in the leaf, or past it, the
//! seek lands. A leaf whose first entry carries an unknown operation under a valid page checksum
//! fails every seek that reads it, with the page cache and without it; a seek into any other leaf
//! lands on the first entry at or past its key, and the walk after it reads every entry exactly.
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
use hyper_block::sim::SimFile;
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{Branch, Builder, Op};
use mantle_engine::store::{Config, Store};
use mantle_engine::{Error, Malformed};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 32,
    max_extents: 4096,
};
/// Rows, each at an even key, so every odd key is absent and falls between two rows.
const ROWS: u64 = 4000;
/// A page cache of this many pages holds every page of the branch.
const CACHED: usize = 1024;
/// An operation byte no entry carries ([`Op`] is 1 or 2).
const UNKNOWN: u8 = 0xEE;

/// The entries a seek lands on and walks: each key, operation and value.
type Walk = Vec<(Vec<u8>, Op, Vec<u8>)>;

fn key(k: u64) -> Vec<u8> {
    format!("key-{k:08}").into_bytes()
}

/// Row `i`'s value: 100 bytes of its own.
fn value(i: u64) -> Vec<u8> {
    (0..100u64)
        .map(|b| (i.wrapping_mul(31) ^ b) as u8)
        .collect()
}

fn store() -> Store<SimFile> {
    let align = Alignment::new(4096).unwrap();
    Store::create(
        SimFile::new(align, Alignment::new(512).unwrap(), 7).unwrap(),
        CONFIG,
    )
    .unwrap()
}

fn build(store: &mut Store<SimFile>) -> Branch {
    let mut b = Builder::new(store, Keys::Exactly(ROWS)).unwrap();
    for i in 0..ROWS {
        b.add(store, &key(2 * i), Op::Put, &value(i)).unwrap();
    }
    b.finish(store).unwrap()
}

/// The rows of leaf page `page`, first to last, by the branch's own leaf index.
fn rows_of(branch: &Branch, page: u32) -> (u64, u64) {
    let rows: Vec<u64> = (0..ROWS)
        .filter(|&i| branch.leaf_of(&key(2 * i)).unwrap() == page)
        .collect();
    (*rows.first().unwrap(), *rows.last().unwrap())
}

/// Rewrites leaf `page`'s first entry with an operation byte no entry carries, through the
/// store's own page writes, so the page's checksum is valid and only the entry is malformed.
fn break_first_entry(store: &mut Store<SimFile>, branch: &Branch, page: u32) {
    let address = branch.page_address(store, u64::from(page)).unwrap();
    let mut payload = Vec::new();
    store.read_page(address, &mut payload).unwrap();
    // The leaf's layout (branch/mod.rs): kind, entries (2), prefix length (2), the prefix, then
    // each entry's offset (2); an entry is its suffix length (2), then its operation.
    let prefix = usize::from(u16::from_le_bytes([payload[3], payload[4]]));
    let table = 5 + prefix;
    let first = usize::from(u16::from_le_bytes([payload[table], payload[table + 1]]));
    assert_eq!(
        payload[first + 2],
        1,
        "the first entry is a put before it is broken"
    );
    payload[first + 2] = UNKNOWN;
    let mut run = store.run().unwrap();
    store.queue_page(&mut run, address, &payload).unwrap();
    store.write_run(&mut run).unwrap();
    store.give_run(run);
    // A fresh cache, so no page read before the rewrite is served after it.
    store.set_cache(0);
    let mut back = Vec::new();
    store.read_page(address, &mut back).unwrap();
    assert_eq!(
        back, payload,
        "the rewritten leaf reads back as written, checksum valid"
    );
}

/// A seek from `from`: the entry it lands on and the `walk` entries after it, or its error.
fn seek(
    store: &mut Store<SimFile>,
    branch: &Branch,
    from: &[u8],
    walk: usize,
) -> Result<Walk, Error> {
    let mut c = branch.seek(store, from)?;
    let mut out = Vec::new();
    for _ in 0..=walk {
        if !c.valid() {
            break;
        }
        out.push((c.key().to_vec(), c.op(), c.value().to_vec()));
        if let Err(e) = c.next(branch, store) {
            c.give_back(store);
            return Err(e);
        }
    }
    c.give_back(store);
    Ok(out)
}

/// The rows a seek from key `k` and a walk of `walk` after it give: the first even key at or
/// past `k`, and on.
fn expected(k: u64, walk: usize) -> Walk {
    (k.div_ceil(2)..ROWS)
        .take(walk + 1)
        .map(|i| (key(2 * i), Op::Put, value(i)))
        .collect()
}

fn malformed(result: Result<Walk, Error>, what: &str) {
    match result {
        Err(Error::Corruption {
            why: Malformed::UnknownTag(UNKNOWN),
            ..
        }) => {}
        other => panic!("{what}: {other:?}, not the malformed first entry's corruption"),
    }
}

/// Do: seek from every key, present and absent, with the cache off and with it holding every
/// page. Expect: each lands on the first row at or past its key, past a leaf's last entry onto
/// the next leaf's first, and the walk after it reads every row exactly.
#[test]
fn seeks_land_on_the_first_entry_at_or_past_their_key_and_read_exactly() {
    let mut s = store();
    let branch = build(&mut s);
    assert!(branch.height > 1, "the branch spans several leaves");
    for cache in [0, CACHED] {
        s.set_cache(cache);
        for k in (0..2 * ROWS + 2).step_by(7) {
            assert_eq!(
                seek(&mut s, &branch, &key(k), 3).unwrap(),
                expected(k, 3),
                "a seek from key {k}, cache {cache}"
            );
        }
        // Past each leaf's last row: the odd key after it lands on the next leaf's first row.
        let mut page = branch.leaf_of(&key(0)).unwrap();
        loop {
            let (_, last) = rows_of(&branch, page);
            let k = 2 * last + 1;
            assert_eq!(seek(&mut s, &branch, &key(k), 1).unwrap(), expected(k, 1));
            if last + 1 >= ROWS {
                break;
            }
            page = branch.leaf_of(&key(2 * (last + 1))).unwrap();
        }
    }
}

/// Do: break the first entry of a leaf in the middle of the branch, then seek, with the cache off
/// and with it holding every page: to that entry, to a later entry of the leaf, to an absent key
/// inside it, past its last entry, and from the gap before it, which lands on it from the leaf
/// before. Expect: each fails with the malformed entry's corruption. Seeks into the next leaf and
/// the leaf before read exactly.
#[test]
fn a_leaf_whose_first_entry_is_malformed_fails_every_seek_that_reads_it() {
    let mut s = store();
    let branch = build(&mut s);
    let page = branch.leaf_of(&key(ROWS)).unwrap();
    let (first, last) = rows_of(&branch, page);
    assert!(
        first > 0 && last > first + 1 && last + 2 < ROWS,
        "a leaf of several rows, not the first or the last"
    );
    break_first_entry(&mut s, &branch, page);
    for cache in [0, CACHED] {
        s.set_cache(cache);
        malformed(seek(&mut s, &branch, &key(2 * first), 0), "its first entry");
        malformed(
            seek(&mut s, &branch, &key(2 * (first + 1)), 0),
            "a later entry",
        );
        malformed(
            seek(&mut s, &branch, &key(2 * last - 1), 0),
            "an absent key inside it",
        );
        malformed(
            seek(&mut s, &branch, &key(2 * last + 1), 0),
            "past its last entry",
        );
        malformed(
            seek(&mut s, &branch, &key(2 * first - 1), 0),
            "the gap before it",
        );
        let next = last + 2;
        assert_eq!(
            seek(&mut s, &branch, &key(2 * next), 2).unwrap(),
            expected(2 * next, 2),
            "the next leaf, cache {cache}"
        );
        assert_eq!(
            seek(&mut s, &branch, &key(2 * (first - 2)), 0).unwrap(),
            expected(2 * (first - 2), 0),
            "the leaf before, cache {cache}"
        );
    }
}
