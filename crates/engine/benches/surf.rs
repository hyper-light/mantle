//! The range filter's cost and accuracy by suffix bits (docs/design/engine-structure.md §5, E6;
//! research/35 §2): a run of `KEYS` keys of two shapes, 16-byte keys of a random 64-bit number
//! (the shard benchmark's) and object names under a few thousand prefixes (an S3 listing's). For
//! each, ranges holding no key, as a bounded seek or a listing asks: the share let through (false
//! positives), bits a key, and nanoseconds a check at p50 and p99, with allocations a check.
//! `cargo bench -p mantle-engine --bench surf -- [KEYS] [QUERIES] [PASSES]`
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::disallowed_macros,
    clippy::disallowed_methods
)]

use std::collections::BTreeSet;
use std::time::Instant;

use hyper_measure::alloc;
use mantle_engine::fst::surf::SurfBuilder;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

fn number(n: u64) -> Vec<u8> {
    let mut k = vec![0u8; 16];
    k[..8].copy_from_slice(&n.to_be_bytes());
    k
}

fn object(r: &mut Rng) -> Vec<u8> {
    format!(
        "tenant-{:04}/agent-{:06}/run-{:08}.json",
        r.next() % 4_000,
        r.next() % 100_000,
        r.next() % 100_000_000
    )
    .into_bytes()
}

/// A key shape: its name and how a key of it is drawn.
type Shape<'a> = (&'a str, &'a dyn Fn(&mut Rng) -> Vec<u8>);

fn pct(lat: &mut [u64], q: f64) -> u64 {
    lat.sort_unstable();
    lat[((lat.len() as f64 * q) as usize).min(lat.len() - 1)]
}

/// The least key past every key starting with `p`: `p` with its last byte below 0xFF raised.
fn successor(p: &[u8]) -> Vec<u8> {
    let mut e = p.to_vec();
    while let Some(b) = e.pop() {
        if b < 0xff {
            e.push(b + 1);
            break;
        }
    }
    e
}

/// Ranges holding no key, of three kinds: a point absent from the run `[x, x\0)`; a range
/// from an absent key to a point strictly inside the gap before the next key; and a listing's
/// prefix no key starts with, `[p, successor(p))`, `p` the key cut after its second `/` (for
/// numbers, its first five bytes).
fn empty_ranges(
    set: &BTreeSet<Vec<u8>>,
    r: &mut Rng,
    make: &dyn Fn(&mut Rng) -> Vec<u8>,
    n: usize,
) -> [Vec<(Vec<u8>, Vec<u8>)>; 3] {
    let mut points = Vec::with_capacity(n);
    let mut gaps = Vec::with_capacity(n);
    let mut prefixes = Vec::with_capacity(n);
    let mut tries = 0usize;
    while (points.len() < n || gaps.len() < n || prefixes.len() < n) && tries < n * 1_000 {
        tries += 1;
        let x = make(r);
        if set.contains(&x) {
            continue;
        }
        if points.len() < n {
            let mut e = x.clone();
            e.push(0);
            points.push((x.clone(), e));
        }
        if gaps.len() < n
            && let Some(next) = set.range(x.clone()..).next()
        {
            // A point between `x` and the next key: their shared bytes, then the byte after
            // where they part, halfway, when there is room.
            let shared = x.iter().zip(next).take_while(|(a, b)| a == b).count();
            if let (Some(&a), Some(&b)) = (x.get(shared), next.get(shared))
                && b > a + 1
            {
                let mut e = next[..shared].to_vec();
                e.push(a + (b - a) / 2);
                gaps.push((x.clone(), e));
            }
        }
        if prefixes.len() < n {
            let cut = if x.contains(&b'/') {
                x.iter()
                    .enumerate()
                    .filter(|(_, c)| **c == b'/')
                    .nth(1)
                    .map_or(x.len(), |(i, _)| i + 1)
            } else {
                5
            };
            let p = x[..cut].to_vec();
            let end = successor(&p);
            if set.range(p.clone()..end.clone()).next().is_none() {
                prefixes.push((p, end));
            }
        }
    }
    [points, gaps, prefixes]
}

fn main() {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| a != "--bench")
        .collect();
    let keys: usize = args.first().map_or(1_000_000, |s| s.parse().unwrap());
    let queries: usize = args.get(1).map_or(200_000, |s| s.parse().unwrap());
    // Passes over each kind's ranges, for a profiler to sample a steady loop.
    let passes: usize = args.get(2).map_or(1, |s| s.parse().unwrap());
    let shapes: [Shape; 2] = [
        ("number16", &|r: &mut Rng| number(r.next())),
        ("object", &|r: &mut Rng| object(r)),
    ];
    for (name, make) in shapes {
        let mut r = Rng(7);
        let set: BTreeSet<Vec<u8>> = (0..keys).map(|_| make(&mut r)).collect();
        let kinds = empty_ranges(&set, &mut Rng(11), make, queries);
        let key_bytes: usize = set.iter().map(Vec::len).sum();
        for suffix in [0u32, 4, 8, 12, 16, 24, 32] {
            let mut b = SurfBuilder::new(suffix).unwrap();
            let t = Instant::now();
            for k in &set {
                b.add(k).unwrap();
            }
            let added = t.elapsed().as_secs_f64();
            let t = Instant::now();
            let f = b.finish().unwrap();
            let finish = t.elapsed().as_secs_f64();
            let build = added + finish;
            let mut scratch = Vec::with_capacity(64);
            let mut line = format!(
                "{name} keys {} ({:.1} B) suffix {suffix:2}: {:.2} bits/key, build {:.0} ns/key (finish {:.2} ms) |",
                set.len(),
                key_bytes as f64 / set.len() as f64,
                f.bytes() as f64 * 8.0 / set.len() as f64,
                build * 1e9 / set.len() as f64,
                finish * 1e3,
            );
            for (kind, ranges) in ["point", "gap", "prefix"].iter().zip(&kinds) {
                let mut lat = Vec::with_capacity(ranges.len());
                let mut through = 0usize;
                alloc::begin();
                for (from, end) in ranges.iter().cycle().take(ranges.len() * passes) {
                    let o = Instant::now();
                    let may = f.may_hold(from, Some(end), &mut scratch);
                    lat.push(o.elapsed().as_nanos() as u64);
                    through += usize::from(may);
                }
                let a = alloc::end();
                line += &format!(
                    " {kind} n {} fp {:.4} ns p50 {} p99 {} allocs {:.2} |",
                    ranges.len(),
                    through as f64 / (ranges.len() * passes).max(1) as f64,
                    pct(&mut lat, 0.5),
                    pct(&mut lat, 0.99),
                    a.allocations as f64 / (ranges.len() * passes).max(1) as f64
                );
            }
            println!("{line}");
        }
    }
}
