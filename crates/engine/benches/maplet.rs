//! A bundle's maplet (crates/engine/src/maplet.rs) against the per-branch blocked Bloom filters it
//! replaces (branch/filter.rs), every page and filter in memory: the bytes a key each takes, and
//! the time a get spends routing a key through one bundle of `B` branches (probing each filter,
//! as a get does today, against one maplet lookup). Keys present and absent, xxh3-like hashes.
//! Each figure is the best of `ROUNDS` passes (a busy machine adds time, never removes it).
//! `cargo bench -p mantle-engine --bench maplet -- [KEYS_PER_BRANCH] [BRANCHES] [OPS]`.
#![allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::disallowed_macros
)]

use std::hint::black_box;
use std::time::Instant;

use mantle_engine::branch::filter::{Filter, Keys};
use mantle_engine::maplet::{self, Plan, Shape, Writer};

const ROUNDS: usize = 5;
const PAYLOAD: usize = 4076;

fn hashes(n: usize, seed: u64) -> Vec<u64> {
    let mut x = seed | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        })
        .collect()
}

fn best(ops: usize, mut f: impl FnMut() -> u64) -> f64 {
    let mut most = f64::MAX;
    for _ in 0..ROUNDS {
        let t = Instant::now();
        black_box(f());
        most = most.min(t.elapsed().as_nanos() as f64 / ops as f64);
    }
    most
}

fn main() {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| a != "--bench")
        .collect();
    let per: usize = args.first().map_or(580_000, |s| s.parse().unwrap());
    let branches: usize = args.get(1).map_or(8, |s| s.parse().unwrap());
    let ops: usize = args.get(2).map_or(2_000_000, |s| s.parse().unwrap());
    let keys: Vec<Vec<u64>> = (0..branches)
        .map(|b| hashes(per, 0x2545 + b as u64))
        .collect();
    // Filters, one a branch, as packing builds them.
    let filters: Vec<Filter> = keys
        .iter()
        .map(|ks| {
            let mut f = Filter::new(Keys::Exactly(ks.len() as u64));
            for &h in ks {
                f.insert(h);
            }
            f
        })
        .collect();
    let filter_bytes: usize = filters.iter().map(Filter::bytes).sum();
    // The bundle's maplet, its values the branches' ages, the store's width for a full bundle.
    let total = (per * branches) as u64;
    let hb = maplet::hash_bits(total);
    let vb = maplet::ceil_log2(branches as u64);
    let bare = Plan::new(hb, vb, total).unwrap();
    let _ = bare;
    let fp_of = |h: u64| h >> (64 - hb);
    let mut entries: Vec<(u64, u8)> = keys
        .iter()
        .enumerate()
        .flat_map(|(b, ks)| {
            ks.iter()
                .map(move |&h| (fp_of(h), u8::try_from(b).unwrap()))
        })
        .collect();
    entries.sort_unstable();
    let mut plan = Plan::new(hb, vb, total).unwrap();
    for &(fp, _) in &entries {
        plan.count(fp).unwrap();
    }
    let shape: Shape = plan.shape(PAYLOAD).unwrap();
    let mut pages: Vec<Vec<u8>> = Vec::new();
    let mut emit = |p: &[u8]| {
        pages.push(p.to_vec());
        Ok(())
    };
    let mut w = Writer::new(&plan, shape, PAYLOAD);
    for &(fp, v) in &entries {
        w.add(fp, v, &mut emit).unwrap();
    }
    w.close(&mut emit).unwrap();
    drop(entries);
    let maplet_bytes = pages.len() * PAYLOAD;
    // Queries: half present (a random branch's key), half absent.
    let mut x = 77u64;
    let present: Vec<u64> = (0..ops)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            keys[(x % branches as u64) as usize][((x >> 20) % per as u64) as usize]
        })
        .collect();
    let absent = hashes(ops, 0x9e37);
    // A get probes the bundle's filters newest first until one passes and the branch holds it;
    // here every passing filter's branch is taken to hold it (the first pass ends the probe).
    let probe_filters = |hs: &[u64]| -> u64 {
        hs.iter()
            .map(|&h| {
                filters
                    .iter()
                    .rev()
                    .position(|f| f.may_contain(h))
                    .map_or(0, |p| p as u64 + 1)
            })
            .sum()
    };
    let probe_maplet = |hs: &[u64]| -> u64 {
        hs.iter()
            .map(|&h| maplet::lookup(&pages[shape.page_of(h) as usize], &shape, h).unwrap())
            .fold(0, |a, m| a ^ m)
    };
    let fp_filters = absent
        .iter()
        .filter(|&&h| filters.iter().any(|f| f.may_contain(h)))
        .count();
    let fp_maplet = absent
        .iter()
        .filter(|&&h| maplet::lookup(&pages[shape.page_of(h) as usize], &shape, h).unwrap() != 0)
        .count();
    println!(
        "bundle of {branches} branches x {per} keys: shape hash {} buckets {} values {} pages {} ({} buckets a page)",
        shape.hash_bits,
        shape.bucket_bits,
        shape.value_bits,
        shape.pages,
        1u64 << shape.page_bucket_bits
    );
    println!(
        "bytes/key: filters {:.2}, maplet {:.2}",
        filter_bytes as f64 / total as f64,
        maplet_bytes as f64 / total as f64
    );
    println!(
        "absent keys passing: filters {:.3}%, maplet {:.3}%",
        fp_filters as f64 * 100.0 / ops as f64,
        fp_maplet as f64 * 100.0 / ops as f64
    );
    println!(
        "present ns/route: filters {:.1}, maplet {:.1}",
        best(ops, || probe_filters(&present)),
        best(ops, || probe_maplet(&present))
    );
    println!(
        "absent ns/route: filters {:.1}, maplet {:.1}",
        best(ops, || probe_filters(&absent)),
        best(ops, || probe_maplet(&absent))
    );
}
