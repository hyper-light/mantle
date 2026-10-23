//! What a trie builder holds, counted by hyper-measure's counting allocator, against what
//! `fst::trie::builder_bytes` states: a range filter's builder and a leaf index's, at the densest
//! keys, the longest sharing long prefixes, and keys in pairs sharing all but their last byte,
//! which give a range filter the most labels keys of their length can.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use hyper_measure::alloc;
use mantle_engine::fst::surf::SurfBuilder;
use mantle_engine::fst::trie::{self, TrieBuilder, Width};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// The suffix bits a range filter keeps of each key in the engine.
const SUFFIX: u32 = 8;

fn lcp(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

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

/// A range filter's builder over `keys`: its peak against the statement, the labels counted as
/// it cuts them (each key one byte past what it shares with either neighbour).
fn range_filter(keys: &[Vec<u8>]) -> (u64, u64) {
    let (mut labels, mut longest) = (0u64, 0usize);
    for (i, k) in keys.iter().enumerate() {
        let before = i.checked_sub(1).map_or(0, |j| lcp(&keys[j], k));
        let after = keys.get(i + 1).map_or(0, |n| lcp(k, n));
        let cut = before.max(after).saturating_add(1).min(k.len());
        labels += (cut - before.min(cut)) as u64;
        longest = longest.max(cut);
    }
    alloc::begin();
    let mut b = SurfBuilder::new(SUFFIX, mantle_engine::store::RANGE_FILTER_DEPTH).unwrap();
    let mut before: Option<&Vec<u8>> = None;
    for k in keys {
        b.push(before.map(Vec::as_slice), before.map_or(0, |p| lcp(p, k)))
            .unwrap();
        before = Some(k);
    }
    // The last key is cut as the filter is finished, which builds the filter itself too: the
    // builder's own peak is what it held before.
    let held = alloc::read().peak;
    alloc::end();
    drop(b);
    let n = keys.len() as u64;
    let stated = trie::builder_bytes(longest, labels, n, SUFFIX).unwrap();
    (u64::try_from(held).unwrap(), stated)
}

/// A leaf index's builder over `keys`, given whole ([`TrieBuilder::add`]).
fn leaf_index(keys: &[Vec<u8>]) -> (u64, u64) {
    let (mut labels, mut longest) = (0u64, 0usize);
    for (i, k) in keys.iter().enumerate() {
        let before = i.checked_sub(1).map_or(0, |j| lcp(&keys[j], k));
        labels += (k.len() - before) as u64;
        longest = longest.max(k.len());
    }
    alloc::begin();
    let mut t = TrieBuilder::new(Width::Fit).unwrap();
    for (i, k) in keys.iter().enumerate() {
        t.add(k, u32::try_from(i).unwrap()).unwrap();
    }
    let held = alloc::read().peak;
    alloc::end();
    drop(t);
    let n = keys.len() as u64;
    let stated = trie::builder_bytes(longest, labels, n, u32::BITS).unwrap();
    (u64::try_from(held).unwrap(), stated)
}

#[test]
fn a_trie_builder_holds_no_more_than_it_states() {
    let shapes: Vec<(&str, Vec<Vec<u8>>)> = vec![
        ("densest 1,000", sharing(1_000, 8, 0)),
        ("densest 100,000", sharing(100_000, 8, 0)),
        ("longest sharing 2,124, 1,000", sharing(1_000, 2_132, 2_124)),
        ("longest sharing 2,124, 4,000", sharing(4_000, 2_132, 2_124)),
        ("longest sharing none, 1,000", sharing(1_000, 2_132, 0)),
        ("pairs of 64 bytes, 1,000", pairs(1_000, 64)),
        ("pairs of 2,132 bytes, 1,000", pairs(1_000, 2_132)),
        ("pairs of 2,132 bytes, 4,000", pairs(4_000, 2_132)),
    ];
    for (name, keys) in &shapes {
        for (which, (held, stated)) in [
            ("range filter", range_filter(keys)),
            ("leaf index", leaf_index(keys)),
        ] {
            eprintln!("{name}: {which} held {held} of {stated} stated");
            assert!(held <= stated, "{name}: {which} held {held} past {stated}");
        }
    }
}

/// A finished trie holds exactly what [`trie::Trie::held_bytes`] counts: its builder dropped,
/// the heap left from the build is the trie's alone. A range filter and a leaf index over every
/// shape above.
#[test]
fn a_finished_trie_holds_what_it_counts() {
    let shapes: Vec<(&str, Vec<Vec<u8>>)> = vec![
        ("densest 100,000", sharing(100_000, 8, 0)),
        ("longest sharing 2,124", sharing(4_000, 2_132, 2_124)),
        ("longest sharing none", sharing(1_000, 2_132, 0)),
        ("pairs of 2,132 bytes", pairs(4_000, 2_132)),
    ];
    for (name, keys) in &shapes {
        alloc::begin();
        let mut b = SurfBuilder::new(SUFFIX, mantle_engine::store::RANGE_FILTER_DEPTH).unwrap();
        let mut before: Option<&Vec<u8>> = None;
        for k in keys {
            b.push(before.map(Vec::as_slice), before.map_or(0, |p| lcp(p, k)))
                .unwrap();
            before = Some(k);
        }
        let f = b.finish(before.map(Vec::as_slice)).unwrap();
        drop(b);
        let held = alloc::read().live;
        alloc::end();
        eprintln!(
            "{name}: range filter held {held}, counts {}",
            f.held_bytes()
        );
        assert_eq!(
            u64::try_from(held).unwrap(),
            f.held_bytes() as u64,
            "{name}"
        );
        drop(f);

        alloc::begin();
        let mut t = TrieBuilder::new(Width::Fit).unwrap();
        for (i, k) in keys.iter().enumerate() {
            t.add(k, u32::try_from(i).unwrap()).unwrap();
        }
        let trie = t.finish().unwrap();
        drop(t);
        let held = alloc::read().live;
        alloc::end();
        eprintln!(
            "{name}: leaf index held {held}, counts {}",
            trie.held_bytes()
        );
        assert_eq!(
            u64::try_from(held).unwrap(),
            trie.held_bytes() as u64,
            "{name}"
        );
    }
}
