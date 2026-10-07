//! A branch's run cursor (`Branch::run_at`, `Branch::run_from`), the walk a REMIX view's runs
//! take (docs/design/engine-structure.md §5, E6): through the leaves in page order, over the index
//! pages between them, it reads every entry the root-to-leaf cursor reads, in the same order, from
//! any start key; and placed again at any position it recorded, it reads the same rest.
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
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{Branch, Builder, Op};
use mantle_engine::store::{Config, Store};
use proptest::prelude::*;

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 4096,
};

type Entries = BTreeMap<Vec<u8>, (Op, Vec<u8>)>;
/// An entry read, and the position the cursor read it at.
type Read = ((Vec<u8>, Op, Vec<u8>), (u64, usize));

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

/// Every entry from `from` on, as the run cursor reads them, with each one's position.
fn walk(s: &mut Store<SimFile>, b: &Branch, from: &[u8]) -> Vec<Read> {
    let mut c = b.run_at(s, from).unwrap();
    let mut out = Vec::new();
    while c.valid() {
        out.push(((c.key().to_vec(), c.op(), c.value().to_vec()), c.position()));
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
    fn the_run_cursor_reads_what_the_tree_holds_from_any_key_and_any_position(
        e in entries(),
        from in prop::collection::vec(any::<u8>(), 0..40),
        pick in any::<prop::sample::Index>(),
        k in 0usize..600,
    ) {
        let mut s = store(3);
        let b = build(&mut s, &e);
        let want: Vec<(Vec<u8>, Op, Vec<u8>)> = e
            .range(from.clone()..)
            .map(|(k, (op, v))| (k.clone(), *op, v.clone()))
            .collect();
        let got = walk(&mut s, &b, &from);
        let read: Vec<_> = got.iter().map(|(entry, _)| entry.clone()).collect();
        prop_assert_eq!(&read, &want);
        // Placed at a recorded position, the cursor reads the same rest.
        if !got.is_empty() {
            let at = pick.index(got.len());
            let (page_no, index) = got[at].1;
            let mut c = b.run_from(&mut s, page_no, index).unwrap();
            let mut rest = Vec::new();
            while c.valid() {
                rest.push((c.key().to_vec(), c.op(), c.value().to_vec()));
                c.next(&b, &mut s).unwrap();
            }
            c.give_back(&mut s);
            prop_assert_eq!(&rest[..], &read[at..]);
            // `k` entries on, by the counts alone and by a cursor's one move: where `k` steps land.
            let want = got.get(at + k).map(|(_, p)| *p);
            prop_assert_eq!(b.position_after((page_no, index), k).unwrap(), want);
            let mut c = b.run_from(&mut s, page_no, index).unwrap();
            c.advance(&b, &mut s, k).unwrap();
            match got.get(at + k) {
                Some((entry, p)) => {
                    prop_assert!(c.valid());
                    prop_assert_eq!(c.position(), *p);
                    prop_assert_eq!(&(c.key().to_vec(), c.op(), c.value().to_vec()), entry);
                }
                None => prop_assert!(!c.valid()),
            }
            c.give_back(&mut s);
        }
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
    assert!(got.iter().zip(e.keys()).all(|((g, _), k)| &g.0 == k));
}

#[test]
fn position_arithmetic_lands_where_steps_do_from_every_position_by_every_count() {
    // Every position of a branch of many leaves and index pages, every count up to past several
    // leaves: each page's end is landed on exactly, from every entry before it.
    let mut s = store(5);
    let e: Entries = (0u32..20_000)
        .map(|n| {
            (
                n.to_be_bytes().to_vec(),
                (Op::Put, vec![9u8; 60 + (n % 50) as usize]),
            )
        })
        .collect();
    let b = build(&mut s, &e);
    assert!(b.height >= 3, "height {}", b.height);
    let positions: Vec<(u64, usize)> = walk(&mut s, &b, b"").into_iter().map(|(_, p)| p).collect();
    assert_eq!(positions.len(), e.len());
    for (j, &p) in positions.iter().enumerate() {
        for k in 0..300 {
            assert_eq!(
                b.position_after(p, k).unwrap(),
                positions.get(j + k).copied(),
                "from {p:?} by {k}"
            );
        }
    }
}
