//! The range filter's cost and accuracy by suffix bits and by depth, the most bytes an entry runs
//! past the bytes every key shares (docs/design/engine-structure.md §5, E6;
//! research/35 §2, research/67): a run of `KEYS` keys of four shapes, 16-byte keys of a random
//! 64-bit number (the shard benchmark's), object names under a few thousand prefixes (an S3
//! listing's), paths through two random 32-hex directories (each new directory a long chain no
//! key before it shares), and a hundredth as many keys of 2,132 bytes in pairs sharing all but a
//! last byte (SuRF's worst case, the range's longest keys). For each, ranges holding no key, as a
//! bounded seek or a listing asks: the share let through (false positives), bits a key, and
//! nanoseconds a check at p50 and p99, with allocations a check.
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

fn hex(r: &mut Rng) -> String {
    format!("{:016x}{:016x}", r.next(), r.next())
}

fn uuid_path(r: &mut Rng) -> Vec<u8> {
    format!(
        "tenant-{:04}/{}/{}/part-{:05}.parquet",
        r.next() % 64,
        hex(&mut Rng(r.next() % 4_000)),
        hex(&mut Rng(r.next() % 16)),
        r.next() % 2_000
    )
    .into_bytes()
}

/// A key of 2,132 bytes in a pair sharing all but its last byte: its pair's number `n`, filler,
/// and `last`.
fn pair_key(n: u64, last: u8) -> Vec<u8> {
    let mut k = n.to_be_bytes().to_vec();
    k.resize(2_131, b'x');
    k.push(last);
    k
}

/// A pair's key drawn over twice the `pairs` stored, with `a`, `b` or `c` last: a stored
/// pair's `a` and `b`, the rest never stored.
fn pair(r: &mut Rng, pairs: u64) -> Vec<u8> {
    pair_key(r.next() % (pairs * 2), b'a' + (r.next() % 3) as u8)
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
    let pairs = (keys / 200).max(1) as u64;
    let shapes: [Shape; 4] = [
        ("number16", &|r: &mut Rng| number(r.next())),
        ("object", &|r: &mut Rng| object(r)),
        ("uuidpath", &|r: &mut Rng| uuid_path(r)),
        ("pairs2132", &|r: &mut Rng| pair(r, pairs)),
    ];
    for (name, make) in shapes {
        let mut r = Rng(7);
        // The pairs whole, each pair's two keys stored.
        let set: BTreeSet<Vec<u8>> = if name == "pairs2132" {
            (0..pairs)
                .flat_map(|n| [pair_key(n, b'a'), pair_key(n, b'b')])
                .collect()
        } else {
            (0..keys).map(|_| make(&mut r)).collect()
        };
        let kinds = empty_ranges(&set, &mut Rng(11), make, queries);
        let key_bytes: usize = set.iter().map(Vec::len).sum();
        let depths = [2usize, 4, 8, 16, 32, 64, 128, usize::MAX];
        for (suffix, depth) in [0u32, 8, 16]
            .into_iter()
            .flat_map(|s| depths.map(|d| (s, d)))
        {
            let shared = match (set.first(), set.last()) {
                (Some(a), Some(z)) => a.iter().zip(z).take_while(|(x, y)| x == y).count(),
                _ => 0,
            };
            let mut b = SurfBuilder::new(suffix, depth, shared).unwrap();
            let t = Instant::now();
            let mut before: Option<&Vec<u8>> = None;
            for k in &set {
                let shared =
                    before.map_or(0, |p| p.iter().zip(k).take_while(|(x, y)| x == y).count());
                b.push(before.map(Vec::as_slice), shared).unwrap();
                before = Some(k);
            }
            let added = t.elapsed().as_secs_f64();
            let t = Instant::now();
            let f = b.finish(before.map(Vec::as_slice)).unwrap();
            let finish = t.elapsed().as_secs_f64();
            let build = added + finish;
            let mut scratch = Vec::with_capacity(64);
            let mut line = format!(
                "{name} keys {} ({:.1} B) suffix {suffix:2} depth {}: entries {} {:.2} bits/key, build {:.0} ns/key (finish {:.2} ms) |",
                set.len(),
                key_bytes as f64 / set.len() as f64,
                if depth == usize::MAX {
                    "none".to_string()
                } else {
                    depth.to_string()
                },
                f.len(),
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
