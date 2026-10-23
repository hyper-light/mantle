//! A branch's run cursor (`Branch::seek`), the walk scans and compactions take: through the leaves
//! in page order, over the index pages between them, it reads every entry the tree holds at or past
//! any start key, in key order, and refuses a corrupt page.
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

use std::collections::BTreeMap;

use hyper_block::buf::Alignment;
use hyper_block::sim::{Fault, SimFile};
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{Branch, Builder, Op};
use mantle_engine::error::Error;
use mantle_engine::store::{Config, Store};
use proptest::prelude::*;

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 4096,
};

type Entries = BTreeMap<Vec<u8>, (Op, Vec<u8>)>;
type Read = (Vec<u8>, Op, Vec<u8>);

fn store(seed: u64) -> Store<SimFile> {
    let align = Alignment::new(4096).unwrap();
    Store::create(
        SimFile::new(align, Alignment::new(512).unwrap(), seed).unwrap(),
        CONFIG,
    )
    .unwrap()
}

fn build(s: &mut Store<SimFile>, entries: &Entries) -> Branch {
    let mut b = Builder::new(s, Keys::Exactly(entries.len() as u64)).unwrap();
    for (k, (op, v)) in entries {
        b.add(s, k, *op, v).unwrap();
    }
    b.finish(s).unwrap()
}

/// Every entry from `from` on, as the run cursor reads them.
fn walk(s: &mut Store<SimFile>, b: &Branch, from: &[u8]) -> Vec<Read> {
    let mut c = b.seek(s, from).unwrap();
    let mut out = Vec::new();
    while c.valid() {
        out.push((c.key().to_vec(), c.op(), c.value().to_vec()));
        c.next(b, s).unwrap();
        assert!(
            out.len() <= b.count as usize,
            "a walk past the branch's entries"
        );
    }
    c.give_back(s);
    out
}

fn entries() -> impl Strategy<Value = Entries> {
    prop::collection::btree_map(
        prop::collection::vec(any::<u8>(), 1..40),
        (
            prop_oneof![Just(Op::Put), Just(Op::Delete)],
            prop::collection::vec(any::<u8>(), 0..120),
        ),
        1..3_000,
    )
    .prop_map(|m| {
        m.into_iter()
            .map(|(k, (op, v))| (k, (op, if op == Op::Delete { Vec::new() } else { v })))
            .collect()
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    #[test]
    fn the_run_cursor_reads_what_the_tree_holds_from_any_key(
        e in entries(),
        from in prop::collection::vec(any::<u8>(), 0..40),
        pick in any::<prop::sample::Index>(),
    ) {
        let mut s = store(3);
        let b = build(&mut s, &e);
        let want: Vec<Read> = e
            .range(from.clone()..)
            .map(|(k, (op, v))| (k.clone(), *op, v.clone()))
            .collect();
        prop_assert_eq!(&walk(&mut s, &b, &from), &want);
        // The leaf index read back from the branch's pages is the one built, and it routes a
        // seek of every key to that key, and a seek between two keys to the second.
        let mut descriptor = Vec::new();
        b.encode(&mut descriptor).unwrap();
        let (back, _) = Branch::decode(&mut s, &descriptor).unwrap();
        prop_assert_eq!(&back.index, &b.index);
        let all = walk(&mut s, &b, b"");
        for (k, op, v) in &all {
            let c = b.seek(&mut s, k).unwrap();
            prop_assert!(c.valid());
            prop_assert_eq!((c.key(), c.op(), c.value()), (k.as_slice(), *op, v.as_slice()));
            c.give_back(&mut s);
        }
        let at = pick.index(all.len());
        let mut past = all[at].0.clone();
        past.push(0);
        prop_assert_eq!(&walk(&mut s, &b, &past)[..], &all[at + 1..]);
    }
}

#[test]
fn a_branch_of_many_leaves_has_index_pages_between_them() {
    // The case the page-order walk must pass over: a branch tall enough that index pages are
    // written among its leaves.
    let mut s = store(9);
    let e: Entries = (0u32..20_000)
        .map(|n| (n.to_be_bytes().to_vec(), (Op::Put, vec![7u8; 40])))
        .collect();
    let b = build(&mut s, &e);
    assert!(b.height >= 3, "height {}", b.height);
    let got = walk(&mut s, &b, b"");
    assert_eq!(got.len(), e.len());
    assert!(got.iter().zip(e.keys()).all(|(g, k)| &g.0 == k));
}

#[test]
fn seeks_alternating_between_branches_read_each_leafs_prefix_and_entry_ranges() {
    // Two branches whose keys share prefixes of different lengths, read through a cache of a
    // few pages: each seek's leaf is read again, its prefix and ranges its own.
    let mut s = store(13);
    s.set_cache(8);
    let entries = |prefix: &[u8]| -> Entries {
        (0u32..500)
            .map(|n| {
                let mut key = prefix.to_vec();
                key.extend_from_slice(&n.to_be_bytes());
                let op = if n % 5 == 0 { Op::Delete } else { Op::Put };
                let value = if op == Op::Put {
                    vec![(n % 256) as u8; (n % 120) as usize]
                } else {
                    Vec::new()
                };
                (key, (op, value))
            })
            .collect()
    };
    let first = build(&mut s, &entries(b"short/"));
    let second = build(&mut s, &entries(b"a-longer-common-prefix/"));
    let first_read = walk(&mut s, &first, b"");
    let second_read = walk(&mut s, &second, b"");
    for (branch, read) in [
        (&first, &first_read),
        (&second, &second_read),
        (&first, &first_read),
    ] {
        for at in [read.len() / 2, 0, read.len() - 1, 1] {
            let mut c = branch.seek(&mut s, &read[at].0).unwrap();
            assert_eq!((c.key().to_vec(), c.op(), c.value().to_vec()), read[at]);
            c.next(branch, &mut s).unwrap();
            match read.get(at + 1) {
                Some(entry) => {
                    assert!(c.valid());
                    assert_eq!(&(c.key().to_vec(), c.op(), c.value().to_vec()), entry);
                }
                None => assert!(!c.valid()),
            }
            c.give_back(&mut s);
        }
    }
}

/// A scan's cursor reads a leaf the cache holds where it lies (`Store::cache_slot_ahead`), not
/// copied into it; once the cache gives that slot to other pages between two steps, the next
/// step reads the leaf again, so the walk reads exactly the entries the branch holds.
#[test]
fn a_cursor_reads_its_leaf_again_once_the_cache_gives_its_slot_away() {
    let mut s = store(19);
    s.set_cache(4);
    let e: Entries = (0u32..3_000)
        .map(|n| {
            (
                n.to_be_bytes().to_vec(),
                (Op::Put, vec![(n % 256) as u8; 60]),
            )
        })
        .collect();
    let b = build(&mut s, &e);
    let want: Vec<Read> = e
        .iter()
        .map(|(k, (op, v))| (k.clone(), *op, v.clone()))
        .collect();
    let start = want.len() / 3;
    // A seek reads the start's leaf into the cache; the walk's cursor then reads it in place.
    let first = b.seek(&mut s, &want[start].0).unwrap();
    first.give_back(&mut s);
    let hits = s.io_stats().span_cache_hits;
    let mut c = b.seek(&mut s, &want[start].0).unwrap();
    assert!(
        s.io_stats().span_cache_hits > hits,
        "the leaf was not cached"
    );
    let mut got = Vec::new();
    let mut i = start;
    while c.valid() {
        got.push((c.key().to_vec(), c.op(), c.value().to_vec()));
        // Seeks elsewhere take the cache's few slots for their own leaves.
        for far in [
            i / 7,
            (i * 3) % want.len(),
            (i + want.len() / 2) % want.len(),
            0,
            want.len() - 1,
        ] {
            let other = b.seek(&mut s, &want[far].0).unwrap();
            other.give_back(&mut s);
        }
        c.next(&b, &mut s).unwrap();
        i += 1;
        assert!(got.len() <= want.len(), "a walk past the branch's entries");
    }
    c.give_back(&mut s);
    assert_eq!(got, want[start..]);
}

#[test]
fn a_seek_refuses_a_corrupt_leaf_and_a_valid_branch_still_reads() {
    let mut s = store(17);
    let entries: Entries = [(b"key".to_vec(), (Op::Put, b"value".to_vec()))]
        .into_iter()
        .collect();
    let good = build(&mut s, &entries);
    let broken = build(&mut s, &entries);
    let page = u64::from(broken.leaf_in(b"key").unwrap().unwrap());
    let address = broken.page_address(&s, page).unwrap();
    // A valid store-page checksum around an empty branch payload: corruption must also be
    // refused by the branch's reader, before any entry can be read through the cursor.
    s.write_page(address, b"").unwrap();
    assert!(matches!(
        broken.seek(&mut s, b""),
        Err(Error::Corruption { .. })
    ));
    let c = good.seek(&mut s, b"").unwrap();
    assert!(c.valid());
    assert_eq!(c.key(), b"key");
    assert_eq!(c.value(), b"value");
    c.give_back(&mut s);
}

#[test]
fn stepping_a_cursor_refuses_a_truncated_entry_in_a_valid_leaf_header() {
    let mut s = store(19);
    let entries: Entries = [
        (b"a".to_vec(), (Op::Put, b"first".to_vec())),
        (b"z".to_vec(), (Op::Put, b"last".to_vec())),
    ]
    .into_iter()
    .collect();
    let b = build(&mut s, &entries);
    let page = u64::from(b.leaf_in(b"a").unwrap().unwrap());
    let address = b.page_address(&s, page).unwrap();
    let mut payload = Vec::new();
    s.read_page(address, &mut payload).unwrap();
    // The page's final row loses its last byte, while its header and first row still decode.
    payload.pop().unwrap();
    s.write_page(address, &payload).unwrap();
    assert!(matches!(
        b.seek(&mut s, b"z"),
        Err(Error::Corruption { .. })
    ));
    let mut c = b.seek(&mut s, b"a").unwrap();
    assert_eq!(c.key(), b"a");
    assert_eq!(c.value(), b"first");
    assert!(matches!(c.next(&b, &mut s), Err(Error::Corruption { .. })));
    c.give_back(&mut s);
}

#[test]
fn a_cursor_refuses_a_page_flipped_on_the_medium() {
    let mut s = store(23);
    let entries: Entries = (0u32..500)
        .map(|n| (n.to_be_bytes().to_vec(), (Op::Put, vec![3u8; 100])))
        .collect();
    let b = build(&mut s, &entries);
    let page = u64::from(b.leaf_in(&250u32.to_be_bytes()).unwrap().unwrap());
    let address = b.page_address(&s, page).unwrap();
    s.checkpoint(None, 1).unwrap();
    let (file, finished) = finished_file(s.into_file());
    finished.unwrap();
    file.inject(Fault::BitFlip {
        offset: address * CONFIG.page_size as u64 + 100,
        bit: 5,
        stored: true,
    })
    .unwrap();
    let (mut s, _) = Store::open(file, CONFIG).unwrap();
    assert!(matches!(
        b.seek(&mut s, &250u32.to_be_bytes()),
        Err(Error::Corruption { .. })
    ));
    let c = b.seek(&mut s, &0u32.to_be_bytes()).unwrap();
    assert!(c.valid());
    assert_eq!(c.key(), 0u32.to_be_bytes());
    assert_eq!(c.value(), vec![3u8; 100]);
    c.give_back(&mut s);
}
