//! REMIX views (docs/design/engine-structure.md §5, E6) over random bundles: up to eight runs with
//! overlapping keys, puts and deletions, a view over a random range of them. Paged from any key,
//! the view reads exactly each key's newest version that holds a value, in key order, with the
//! continuation at the next key; and it holds every version in its range, in segments of at most
//! the segment width.
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
use mantle_engine::remix::{Build, Rebuild, SEGMENT, View};
use mantle_engine::rows::Rows;
use mantle_engine::store::{Config, Store};
use proptest::prelude::*;

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 14,
};

type Run = BTreeMap<Vec<u8>, (Op, Vec<u8>)>;

fn store() -> Store<SimFile> {
    let align = Alignment::new(4096).unwrap();
    Store::create(
        SimFile::new(align, Alignment::new(512).unwrap(), 11).unwrap(),
        CONFIG,
    )
    .unwrap()
}

fn build(s: &mut Store<SimFile>, run: &Run) -> Branch {
    let mut b = Builder::new(s, Keys::Exactly(run.len() as u64)).unwrap();
    for (k, (op, v)) in run {
        b.add(s, k, *op, v).unwrap();
    }
    b.finish(s).unwrap()
}

/// Keys from a small space, so runs overlap: two bytes of a few hundred values, and a tail.
fn key() -> impl Strategy<Value = Vec<u8>> {
    (0u16..600, prop::collection::vec(any::<u8>(), 0..6)).prop_map(|(n, tail)| {
        let mut k = n.to_be_bytes().to_vec();
        k.extend(tail);
        k
    })
}

fn run() -> impl Strategy<Value = Run> {
    prop::collection::btree_map(
        key(),
        (
            prop_oneof![3 => Just(Op::Put), 1 => Just(Op::Delete)],
            prop::collection::vec(any::<u8>(), 0..60),
        ),
        0..700,
    )
    .prop_map(|m| {
        m.into_iter()
            .map(|(k, (op, v))| (k, (op, if op == Op::Delete { Vec::new() } else { v })))
            .collect()
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn a_view_reads_each_keys_newest_live_version_in_order(
        runs in prop::collection::vec(run(), 1..9),
        lo in key(),
        hi in prop::option::of(key()),
        from in key(),
        limit in 1usize..50,
    ) {
        // Every run must hold an entry: a branch has at least one.
        let runs: Vec<Run> = runs.into_iter().filter(|r| !r.is_empty()).collect();
        prop_assume!(!runs.is_empty());
        let mut s = store();
        let branches: Vec<Branch> = runs.iter().map(|r| build(&mut s, r)).collect();
        let view = View::build(&mut s, &branches, &lo, hi.as_deref()).unwrap();

        let in_range = |k: &Vec<u8>| k >= &lo && hi.as_ref().is_none_or(|h| k < h);
        // The newest version of each key in range, runs newest first.
        let mut newest: BTreeMap<Vec<u8>, (Op, Vec<u8>)> = BTreeMap::new();
        let mut versions = 0usize;
        for r in &runs {
            for (k, e) in r.iter().filter(|(k, _)| in_range(k)) {
                versions += 1;
                newest.entry(k.clone()).or_insert_with(|| e.clone());
            }
        }
        prop_assert_eq!(view.entries(), versions);
        // Encoded and read back, the view is the same; cut short anywhere, it is refused.
        let mut bytes = Vec::new();
        view.encode(&mut bytes).unwrap();
        prop_assert_eq!(View::decode(&bytes).unwrap(), (view.clone(), bytes.len()));
        for cut in (0..bytes.len()).step_by(1 + bytes.len() / 50) {
            prop_assert!(View::decode(&bytes[..cut]).is_err());
        }
        // Built a slice at a time, as maintenance builds it, the view is the same, field for field.
        let mut job = Build::new(&mut s, &branches, &lo, hi.as_deref()).unwrap();
        let mut slices = 0u64;
        while !job.step(&mut s, &branches, 1 + (slices * 7919) % 40).unwrap().1 {
            slices += 1;
            prop_assert!(slices <= versions as u64 + 1, "a build that does not end");
        }
        prop_assert_eq!(&job.finish(&mut s), &view);
        // Rebuilt from the view of the older runs with the newest merged in, a slice at a time,
        // the view is the same again, field for field.
        if branches.len() >= 2 {
            let old = View::build(&mut s, &branches[1..], &lo, hi.as_deref()).unwrap();
            let mut job = Rebuild::new(&mut s, old, &branches, &lo, hi.as_deref()).unwrap();
            let mut slices = 0u64;
            while !job.step(&mut s, &branches, 1 + (slices * 104_729) % 33).unwrap().1 {
                slices += 1;
                prop_assert!(slices <= versions as u64 + 1, "a rebuild that does not end");
            }
            prop_assert_eq!(&job.finish(&mut s), &view);
        }
        prop_assert!(view.segments() * SEGMENT.max(runs.len()) >= versions);

        // Paged from `from`, the view reads the live keys at or past it.
        let want: Vec<(Vec<u8>, Vec<u8>)> = newest
            .iter()
            .filter(|(k, (op, _))| *op == Op::Put && k.as_slice() >= from.as_slice())
            .map(|(k, (_, v))| (k.clone(), v.clone()))
            .collect();
        let mut got = Vec::new();
        let mut at = from.clone();
        let mut page = Rows::new();
        let mut next = Vec::new();
        for pages in 0.. {
            prop_assert!(pages <= want.len() + 1, "a scan that does not end");
            page.clear();
            let more = view.scan(&mut s, &branches, &at, limit, &mut page, &mut next).unwrap();
            prop_assert!(page.len() <= limit);
            got.extend(page.iter().map(|(k, v)| (k.to_vec(), v.to_vec())));
            if !more {
                break;
            }
            prop_assert!(next > at);
            at.clone_from(&next);
        }
        prop_assert_eq!(got, want);
    }
}

#[test]
fn a_view_seeks_exactly_from_every_key_and_just_past_each() {
    // Eight overlapping runs, versions and deletions among them; every stored key, and each with
    // a byte after it, as a seek's start: each page boundary and segment edge is met.
    let mut s = store();
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    let mut runs: Vec<Run> = Vec::new();
    for _ in 0..8 {
        let mut r = Run::new();
        for _ in 0..600 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let k = ((x % 900) as u16).to_be_bytes().to_vec();
            let op = if x.is_multiple_of(5) {
                Op::Delete
            } else {
                Op::Put
            };
            let v = if op == Op::Delete {
                Vec::new()
            } else {
                vec![(x >> 8) as u8; (x % 90) as usize]
            };
            r.insert(k, (op, v));
        }
        runs.push(r);
    }
    let branches: Vec<Branch> = runs.iter().map(|r| build(&mut s, r)).collect();
    let view = View::build(&mut s, &branches, b"", None).unwrap();
    let mut newest: BTreeMap<Vec<u8>, (Op, Vec<u8>)> = BTreeMap::new();
    for r in &runs {
        for (k, e) in r {
            newest.entry(k.clone()).or_insert_with(|| e.clone());
        }
    }
    let mut starts: Vec<Vec<u8>> = newest.keys().cloned().collect();
    starts.extend(newest.keys().map(|k| {
        let mut k = k.clone();
        k.push(0);
        k
    }));
    starts.push(Vec::new());
    let mut page = Rows::new();
    let mut next = Vec::new();
    for from in &starts {
        page.clear();
        view.scan(&mut s, &branches, from, 3, &mut page, &mut next)
            .unwrap();
        let got: Vec<(Vec<u8>, Vec<u8>)> =
            page.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
        let want: Vec<(Vec<u8>, Vec<u8>)> = newest
            .range(from.clone()..)
            .filter(|(_, (op, _))| *op == Op::Put)
            .take(3)
            .map(|(k, (_, v))| (k.clone(), v.clone()))
            .collect();
        assert_eq!(got, want, "from {from:?}");
    }
}

#[test]
fn a_short_seek_does_not_read_a_fully_shadowed_run() {
    let mut s = store();
    let newest: Run = (0u16..64)
        .map(|n| (n.to_be_bytes().to_vec(), (Op::Put, b"new".to_vec())))
        .collect();
    let older: Run = newest
        .keys()
        .map(|k| (k.clone(), (Op::Put, b"old".to_vec())))
        .collect();
    let branches = vec![build(&mut s, &newest), build(&mut s, &older)];
    let view = View::build(&mut s, &branches, b"", None).unwrap();
    let from = 8u16.to_be_bytes();
    let old_page = u64::from(branches[1].leaf_of(&from).unwrap());
    let address = branches[1].page_address(&s, old_page).unwrap();
    s.checkpoint(None, 1).unwrap();
    let (file, finished) = finished_file(s.into_file());
    finished.unwrap();
    file.inject(Fault::ReadError {
        offset: address * CONFIG.page_size as u64,
        len: CONFIG.page_size as u64,
    })
    .unwrap();
    let (mut s, _) = Store::open(file, CONFIG).unwrap();
    let mut page = Rows::new();
    let mut next = Vec::new();
    assert!(
        view.scan(&mut s, &branches, &from, 3, &mut page, &mut next)
            .unwrap()
    );
    let got: Vec<_> = page.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
    let want: Vec<_> = newest
        .range(from.to_vec()..)
        .take(3)
        .map(|(k, (_, v))| (k.clone(), v.clone()))
        .collect();
    assert_eq!(got, want);
    assert_eq!(next, 11u16.to_be_bytes());
    // The medium failure remains armed: the old run still cannot be read directly.
    assert!(branches[1].run_at(&mut s, &from).is_err());
}

#[test]
fn a_reopened_view_pages_from_present_and_absent_keys_with_deletions() {
    let mut s = store();
    let runs: Vec<Run> = (0u16..8)
        .map(|r| {
            (0u16..128)
                .filter(|n| !n.is_multiple_of(r + 2))
                .map(|n| {
                    let op = if n % 7 == r % 7 { Op::Delete } else { Op::Put };
                    let value = if op == Op::Delete {
                        Vec::new()
                    } else {
                        vec![r as u8; 3 + usize::from((n * 37 + r * 53) % 900)]
                    };
                    ((n * 2).to_be_bytes().to_vec(), (op, value))
                })
                .collect()
        })
        .collect();
    let branches: Vec<_> = runs.iter().map(|r| build(&mut s, r)).collect();
    let lo = 34u16.to_be_bytes();
    let hi = 222u16.to_be_bytes();
    let view = View::build(&mut s, &branches, &lo, Some(&hi)).unwrap();
    let descriptors: Vec<_> = branches
        .iter()
        .map(|b| {
            let mut bytes = Vec::new();
            b.encode(&mut bytes).unwrap();
            bytes
        })
        .collect();
    let mut encoded_view = Vec::new();
    view.encode(&mut encoded_view).unwrap();
    s.checkpoint(None, 1).unwrap();
    let (file, finished) = finished_file(s.into_file());
    finished.unwrap();
    let (mut s, _) = Store::open(file, CONFIG).unwrap();
    let branches: Vec<_> = descriptors
        .iter()
        .map(|bytes| {
            let (branch, used) = Branch::decode(&mut s, bytes).unwrap();
            assert_eq!(used, bytes.len());
            branch
        })
        .collect();
    let (view, used) = View::decode(&encoded_view).unwrap();
    assert_eq!(used, encoded_view.len());
    let mut newest = Run::new();
    for run in &runs {
        for (key, entry) in run {
            newest.entry(key.clone()).or_insert_with(|| entry.clone());
        }
    }
    let mut starts: Vec<_> = (0u16..=256).map(|n| n.to_be_bytes().to_vec()).collect();
    starts.push(Vec::new());
    let mut page = Rows::new();
    let mut next = Vec::new();
    for from in starts {
        let want: Vec<_> = newest
            .iter()
            .filter(|(key, (op, _))| {
                *op == Op::Put
                    && key.as_slice() >= from.as_slice()
                    && key.as_slice() >= lo.as_slice()
                    && key.as_slice() < hi.as_slice()
            })
            .map(|(key, (_, value))| (key.clone(), value.clone()))
            .collect();
        for limit in [1, 3, 11] {
            let mut at = from.clone();
            let mut got = Vec::new();
            for pages in 0.. {
                assert!(pages <= want.len() + 1, "a scan that does not end");
                page.clear();
                let more = view
                    .scan(&mut s, &branches, &at, limit, &mut page, &mut next)
                    .unwrap();
                assert!(page.len() <= limit);
                got.extend(
                    page.iter()
                        .map(|(key, value)| (key.to_vec(), value.to_vec())),
                );
                if !more {
                    break;
                }
                assert!(next > at);
                at.clone_from(&next);
            }
            assert_eq!(got, want, "from {from:?}, limit {limit}");
        }
    }
}

#[test]
fn a_view_whose_least_key_is_the_empty_key_decodes_as_it_was_encoded() {
    // The empty key is the least of all, so a view's first anchor may be it: its encoding,
    // a first anchor of no bytes, decodes to the view it was (found as a checkpoint whose
    // view could not be read back, in tests/ranges_active_order.rs).
    let mut s = store();
    let mut a = Run::new();
    a.insert(Vec::new(), (Op::Put, b"empty".to_vec()));
    a.insert(b"a".to_vec(), (Op::Put, b"1".to_vec()));
    let mut b = Run::new();
    for n in 0u8..40 {
        b.insert(vec![b'b', n], (Op::Put, vec![n]));
    }
    let branches = vec![build(&mut s, &a), build(&mut s, &b)];
    let view = View::build(&mut s, &branches, b"", None).unwrap();
    let mut bytes = Vec::new();
    view.encode(&mut bytes).unwrap();
    let (decoded, used) = View::decode(&bytes).unwrap();
    assert_eq!(used, bytes.len());
    assert_eq!(decoded, view);
}
