//! A branch whose leaves' separators run past the leaf index's depth: the index holds each run
//! of them as one group of leaves, and a key within one finds its leaf through the index pages,
//! so every get and every seek stays exact (research/67).
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
use std::collections::BTreeMap;

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 1 << 16,
};

/// Keys in pairs of 600 bytes sharing all but a last byte, each pair parting from the one before
/// at its number, and a run of 4,000 sharing their first 500 bytes: separators past the depth
/// where a leaf ends inside a pair, and one group of many leaves over the run.
fn entries() -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut m = BTreeMap::new();
    for i in 0u64..600 {
        m.insert(pair(i / 2, if i % 2 == 0 { b'a' } else { b'b' }), {
            format!("pair-{i}").into_bytes()
        });
    }
    for i in 0u32..4_000 {
        m.insert(run(i), format!("run-{i}").into_bytes());
    }
    m
}

fn pair(n: u64, last: u8) -> Vec<u8> {
    let mut k = n.to_be_bytes().to_vec();
    k.resize(599, b'x');
    k.push(last);
    k
}

fn run(n: u32) -> Vec<u8> {
    let mut k = vec![0xff];
    k.resize(500, b'r');
    k.extend_from_slice(&n.to_be_bytes());
    k
}

fn build(store: &mut Store<SimFile>, entries: &BTreeMap<Vec<u8>, Vec<u8>>) -> Branch {
    let mut b = Builder::new(store, Keys::Exactly(entries.len() as u64)).unwrap();
    let (first, last) = (
        entries.keys().next().unwrap(),
        entries.keys().next_back().unwrap(),
    );
    b.set_shared(first.iter().zip(last).take_while(|(a, z)| a == z).count())
        .unwrap();
    for (k, v) in entries {
        b.add(store, k, Op::Put, v).unwrap();
    }
    b.finish(store).unwrap()
}

#[test]
fn keys_within_groups_of_leaves_are_found_through_the_index_pages() {
    let entries = entries();
    let mut store = Store::create(
        SimFile::new(
            Alignment::new(4096).unwrap(),
            Alignment::new(512).unwrap(),
            7,
        )
        .unwrap(),
        CONFIG,
    )
    .unwrap();
    let branch = build(&mut store, &entries);
    assert!(branch.height > 1, "index pages to read down");
    // Groups there are: some keys the leaf index alone does not place.
    let grouped = entries
        .keys()
        .filter(|k| branch.leaf_in(k).unwrap().is_none())
        .count();
    assert!(grouped > 4_000, "{grouped} keys within groups");
    let mut value = Vec::new();
    for (k, v) in &entries {
        assert_eq!(
            branch.get(&mut store, k, &mut value).unwrap(),
            Some(Op::Put),
            "{k:?}"
        );
        assert_eq!(&value, v);
    }
    // Keys never stored: a pair's third last byte, numbers past each run, and keys between.
    let mut absent: Vec<Vec<u8>> = (0..310).map(|n| pair(n, b'c')).collect();
    absent.extend((4_000..4_010).map(run));
    absent.extend((0..4_000).step_by(97).map(|n| {
        let mut k = run(n);
        k.push(0);
        k
    }));
    absent.push(vec![0xff]);
    absent.push(vec![0xff; 600]);
    for k in &absent {
        assert_eq!(
            branch.get(&mut store, k, &mut value).unwrap(),
            None,
            "{k:?}"
        );
    }
    // A seek from any of them, or from a key stored, walks exactly the entries at or after it.
    let froms = entries.keys().step_by(37).chain(&absent);
    for from in froms {
        let mut c = branch.seek(&mut store, from).unwrap();
        let mut got = Vec::new();
        while c.valid() && got.len() < 30 {
            got.push((c.key().to_vec(), c.value().to_vec()));
            c.next(&branch, &mut store).unwrap();
        }
        c.give_back(&mut store);
        let want: Vec<(Vec<u8>, Vec<u8>)> = entries
            .range(from.clone()..)
            .take(30)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(got, want, "seek from {from:?}");
    }
}
