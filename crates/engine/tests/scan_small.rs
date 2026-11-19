//! Small scans over a shard of many runs: a page of a few rows gives the rows a full merge gives,
//! with or without its continuation ([`ShardDb::scan`], [`ShardDb::scan_rows`]), and a one-row
//! lookup of a key's newest row (mantle-meta's `name::current`, a range's `Stored::next`) reads and
//! compares nothing past its row and allocates nothing once its buffers have grown.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use std::collections::BTreeMap;

use hyper_block::buf::Alignment;
use hyper_block::sim::SimFile;
use hyper_measure::alloc;
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::{Consolidation, TrunkConfig};
use proptest::prelude::*;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 16,
};

fn sim(seed: u64) -> SimFile {
    SimFile::new(
        Alignment::new(4096).unwrap(),
        Alignment::new(512).unwrap(),
        seed,
    )
    .unwrap()
}

/// A shard whose memtables of `mem` bytes pack into runs the trunk keeps as they are: seeks
/// consolidate none (`Consolidation::Never`), so a key's segment lists every run on its path.
fn shard(seed: u64, mem: usize) -> ShardDb<SimFile> {
    let mut db = ShardDb::create(
        sim(seed),
        STORE,
        mem,
        TrunkConfig {
            fanout: 4,
            leaf_entries: 512,
        },
    )
    .unwrap();
    db.set_consolidation(Consolidation::Never);
    db
}

/// Object `i`'s name rows' prefix, the names spread over the key space as a bucket's are.
fn object(i: u64) -> Vec<u8> {
    format!("bucket\0obj-{:08}\0", (i * 2_654_435_761) % 1_000_003).into_bytes()
}

/// Object `i`'s version row of order `order`, as mantle-meta's name rows: newest first.
fn version(i: u64, order: u64) -> Vec<u8> {
    let mut k = object(i);
    k.push(b'V');
    k.extend_from_slice(&order.to_be_bytes());
    k
}

/// The end of object `i`'s version rows: its first upload row.
fn upload(i: u64) -> Vec<u8> {
    let mut k = object(i);
    k.push(b'W');
    k
}

/// What a run of lookups did: rows found, sources listed, passed over and opened, pages read
/// from the device or the cache, keys the merges compared, and allocations.
#[derive(Debug, Default, PartialEq, Eq)]
struct Counts {
    rows: u64,
    listed: u64,
    skipped: u64,
    opened: u64,
    pages: u64,
    compared: u64,
    allocations: u64,
}

/// Each of `objects` looked up once, a one-row page of its version rows by `page`, counted.
fn look_up(
    db: &mut ShardDb<SimFile>,
    objects: &[u64],
    mut page: impl FnMut(&mut ShardDb<SimFile>, &[u8], &[u8], &mut Rows, &mut Vec<u8>),
) -> Counts {
    let ranges: Vec<(Vec<u8>, Vec<u8>)> = objects
        .iter()
        .map(|&i| (version(i, 0), upload(i)))
        .collect();
    let mut rows = Rows::new();
    let mut next = Vec::new();
    // Every buffer grown first: the same lookups once, uncounted.
    for (from, to) in &ranges {
        rows.clear();
        page(db, from, to, &mut rows, &mut next);
    }
    let (_, t0, io0) = db.stats();
    let mut found = 0u64;
    alloc::begin();
    for (from, to) in &ranges {
        rows.clear();
        page(db, from, to, &mut rows, &mut next);
        found += rows.len() as u64;
    }
    let allocated = alloc::end();
    let (_, t1, io1) = db.stats();
    Counts {
        rows: found,
        listed: t1.scan_sources - t0.scan_sources,
        skipped: t1.scan_skipped - t0.scan_skipped,
        opened: t1.scan_opened - t0.scan_opened,
        pages: (io1.reads - io0.reads) + (io1.span_cache_hits - io0.span_cache_hits),
        compared: t1.scan_compared - t0.scan_compared,
        allocations: allocated.allocations + allocated.reallocations,
    }
}

/// A PUT ladder's lookups: each object's newest version looked up (none yet), then written,
/// over memtables small enough that the trunk keeps many runs. With no page cache, as a node's
/// engine runs, every page a lookup reads is a device read.
#[test]
fn a_one_row_lookup_reads_and_compares_nothing_past_its_row() {
    let mut db = shard(7, 16 * 1024);
    let n = 20_000u64;
    let mut rows = Rows::new();
    for i in 0..n {
        rows.clear();
        db.scan_rows(&version(i, 0), Some(&upload(i)), 1, &mut rows)
            .unwrap();
        assert!(rows.is_empty());
        // Two versions: the newer, with the lesser order, is the one a lookup finds.
        db.put(&version(i, u64::MAX - 2 * i), b"old").unwrap();
        db.put(&version(i, u64::MAX - 2 * i - 1), b"new").unwrap();
    }
    let (height, _, _) = db.shape().unwrap();
    assert!(height > 1, "a trunk of one level");
    let objects: Vec<u64> = (0..2_000u64).map(|j| (j * 7_919) % n).collect();
    let paged = look_up(&mut db, &objects, |db, from, to, rows, next| {
        db.scan(from, Some(to), 1, rows, next).unwrap();
    });
    let alone = look_up(&mut db, &objects, |db, from, to, rows, _| {
        db.scan_rows(from, Some(to), 1, rows).unwrap();
    });
    eprintln!("with its continuation {paged:?}\nalone {alone:?}");
    // The same rows, every one a lookup's.
    assert_eq!((alone.rows, paged.rows), (objects.len() as u64, alone.rows));
    // The page alone stops on its row: it moves no source past it and compares no key to find
    // the next, and opens no segment past the row's for it, so it lists, opens and reads no
    // more than the page with a continuation and compares fewer keys.
    assert!(alone.listed <= paged.listed, "{alone:?} {paged:?}");
    assert!(alone.opened <= paged.opened, "{alone:?} {paged:?}");
    assert!(alone.pages <= paged.pages, "{alone:?} {paged:?}");
    assert!(alone.compared < paged.compared, "{alone:?} {paged:?}");
    // A lookup reads a page a source it opens, and compares a key or two a lookup: its source's
    // against the end, and a source's against another where two hold the range.
    assert_eq!(alone.pages, alone.opened, "{alone:?}");
    assert!(alone.compared <= 2 * alone.rows, "{alone:?}");
    // Neither allocates once its buffers have grown.
    assert_eq!((paged.allocations, alone.allocations), (0, 0));
}

type Oracle = BTreeMap<Vec<u8>, Vec<u8>>;

fn key_of(k: u16) -> Vec<u8> {
    format!("k{:02}/{k:05}", k % 7).into_bytes()
}

fn rows_of(rows: &Rows) -> Vec<(Vec<u8>, Vec<u8>)> {
    rows.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect()
}

/// Every small page of `[from, end)` against the full merge and the oracle: the rows a page of
/// `limit` gives are the full merge's first, with or without a continuation; its continuation
/// lies past its rows and at or before the next row; and pages followed from it give the full
/// merge whole.
fn check_small(
    db: &mut ShardDb<SimFile>,
    oracle: &Oracle,
    from: &[u8],
    end: Option<&[u8]>,
    limit: usize,
) -> Result<(), TestCaseError> {
    let mut rows = Rows::new();
    let mut next = Vec::new();
    let more = db
        .scan(from, end, usize::MAX, &mut rows, &mut next)
        .unwrap();
    prop_assert!(!more);
    let full = rows_of(&rows);
    let want: Vec<(Vec<u8>, Vec<u8>)> = oracle
        .range(from.to_vec()..)
        .take_while(|(k, _)| end.is_none_or(|e| k.as_slice() < e))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    prop_assert_eq!(&full, &want);
    rows.clear();
    let more = db.scan(from, end, limit, &mut rows, &mut next).unwrap();
    let page = rows_of(&rows);
    let head: Vec<_> = full.iter().take(limit).cloned().collect();
    prop_assert_eq!(&page, &head);
    if more {
        prop_assert_eq!(page.len(), limit);
        prop_assert!(
            page.last()
                .is_none_or(|(k, _)| next.as_slice() > k.as_slice())
        );
        prop_assert!(end.is_none_or(|e| next.as_slice() < e));
        if let Some((k, _)) = full.get(limit) {
            prop_assert!(next.as_slice() <= k.as_slice());
        }
    } else {
        prop_assert!(full.len() <= limit);
    }
    rows.clear();
    let filled = db.scan_rows(from, end, limit, &mut rows).unwrap();
    prop_assert_eq!(&rows_of(&rows), &head);
    prop_assert_eq!(filled, head.len() == limit);
    // Paged from the continuation to the range's end: the full merge.
    let mut paged = Vec::new();
    let mut at = from.to_vec();
    for _ in 0..=full.len().saturating_add(oracle.len()) {
        rows.clear();
        let more = db.scan(&at, end, limit, &mut rows, &mut next).unwrap();
        paged.extend(rows_of(&rows));
        if !more {
            break;
        }
        at.clone_from(&next);
    }
    prop_assert_eq!(&paged, &full);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 16, ..ProptestConfig::default() })]

    /// Random puts and deletes over memtables small enough that the trunk keeps many runs, a
    /// key's entries in several of them and in the memtables; small pages checked between
    /// batches, bounded and not, against the full merge.
    #[test]
    fn small_pages_give_the_full_merges_rows(
        batches in proptest::collection::vec(
            (
                proptest::collection::vec((0u16..1500, 0u8..10, any::<u8>()), 1..300),
                proptest::collection::vec((0u16..1500, 0u16..200, 1usize..5), 1..6),
            ),
            1..12,
        ),
        seed in any::<u64>(),
    ) {
        let mut db = shard(seed, 2 * 1024);
        let mut oracle = Oracle::new();
        for (writes, pages) in &batches {
            for &(k, kind, v) in writes {
                let key = key_of(k);
                if kind < 3 {
                    db.delete(&key).unwrap();
                    oracle.remove(&key);
                } else {
                    let value = vec![v; 1 + usize::from(k % 23)];
                    db.put(&key, &value).unwrap();
                    oracle.insert(key, value);
                }
            }
            for &(k, width, limit) in pages {
                let from = key_of(k);
                // A range of `width` keys of one prefix, a key's own (no width), or to the end.
                let end = match width {
                    0 => None,
                    1 => Some({
                        let mut e = from.clone();
                        e.push(0);
                        e
                    }),
                    w => Some(key_of(k.saturating_add(7 * w))),
                };
                check_small(&mut db, &oracle, &from, end.as_deref(), limit)?;
            }
        }
        let (_, trunk, _) = db.stats();
        prop_assert!(trunk.scans > 0);
    }
}
