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

use hyper_measure::usage;
use mantle_engine::branch::filter::{Filter, Keys};
use mantle_engine::maplet::{self, Builder, Shape};

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

/// The best of `ROUNDS` passes of `f`, as ns, instructions and cycles an operation: instructions
/// retired do not grow with a busy machine as time does (hyper-measure's usage counters).
fn best(ops: usize, mut f: impl FnMut() -> u64) -> (f64, f64, f64) {
    let mut most = (f64::MAX, f64::MAX, f64::MAX);
    for _ in 0..ROUNDS {
        let u0 = usage::this().ok();
        let t = Instant::now();
        black_box(f());
        let ns = t.elapsed().as_nanos() as f64 / ops as f64;
        let u1 = usage::this().ok();
        let per = |a: Option<u64>, b: Option<u64>| {
            a.zip(b)
                .map_or(f64::NAN, |(a, b)| b.saturating_sub(a) as f64 / ops as f64)
        };
        let ins = per(
            u0.as_ref().and_then(|u| u.instructions),
            u1.as_ref().and_then(|u| u.instructions),
        );
        let cyc = per(
            u0.as_ref().and_then(|u| u.cycles),
            u1.as_ref().and_then(|u| u.cycles),
        );
        most = (most.0.min(ns), most.1.min(ins), most.2.min(cyc));
    }
    most
}

fn show(name: &str, filters: (f64, f64, f64), maplet: (f64, f64, f64)) {
    println!(
        "{name}: filters {:.1} ns {:.0} ins {:.0} cyc | maplet {:.1} ns {:.0} ins {:.0} cyc",
        filters.0, filters.1, filters.2, maplet.0, maplet.1, maplet.2
    );
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
    // The bundle's maplet, its values the branches' ages, built from its keys' 32-bit hashes.
    let total = (per * branches) as u64;
    let vb = maplet::ceil_log2(branches as u64);
    let mut entries: Vec<(u32, u8)> = keys
        .iter()
        .enumerate()
        .flat_map(|(b, ks)| {
            ks.iter()
                .map(move |&h| (maplet::hash32(h), u8::try_from(b).unwrap()))
        })
        .collect();
    entries.sort_unstable();
    let mut pages: Vec<Vec<u8>> = Vec::new();
    let mut emit = |p: &[u8]| {
        pages.push(p.to_vec());
        Ok(())
    };
    let mut b = Builder::new(maplet::bucket_bits(total), vb, PAYLOAD).unwrap();
    for &(h, v) in &entries {
        b.add(h, v, &mut emit).unwrap();
    }
    let shape: Shape = b.close(&mut emit).unwrap();
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
            .map(|&h| {
                let p = shape.probe(h).unwrap();
                maplet::lookup(&pages[p.page as usize], &shape, &p).unwrap()
            })
            .fold(0, |a, m| a ^ m)
    };
    let fp_filters = absent
        .iter()
        .filter(|&&h| filters.iter().any(|f| f.may_contain(h)))
        .count();
    let fp_maplet = absent
        .iter()
        .filter(|&&h| { let p = shape.probe(h).unwrap(); maplet::lookup(&pages[p.page as usize], &shape, &p).unwrap() } != 0)
        .count();
    println!(
        "bundle of {branches} branches x {per} keys: values {} bits, buckets 2^{}, pages {}, load {:.3}",
        shape.value_bits,
        shape.bucket_bits,
        shape.pages,
        total as f64 / (1u64 << shape.bucket_bits) as f64
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
    show(
        "present route",
        best(ops, || probe_filters(&present)),
        best(ops, || probe_maplet(&present)),
    );
    show(
        "absent route",
        best(ops, || probe_filters(&absent)),
        best(ops, || probe_maplet(&absent)),
    );
}
