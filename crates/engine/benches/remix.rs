//! A REMIX view against the merge it replaces (docs/design/engine-structure.md §5, E6): a leaf
//! bundle of `H` runs of 16-byte keys and 100-byte values drawn from one key space, so the runs
//! overlap as a leaf's do, newest first. Each case seeks a random key and reads the next `NEXTS`
//! live entries, through a REMIX view of the runs and through a merge of a cursor a run (what a
//! scan does today). Printed: nanoseconds a seek at p50, p99 and p99.9, allocations and
//! reallocations a seek, and the view's bytes a key.
//! `cargo bench -p mantle-engine --bench remix -- DIR [KEYS_PER_RUN] [SEEKS] [NEXTS]`
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

use std::path::PathBuf;
use std::time::Instant;

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_measure::alloc;
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::merge::Merge;
use mantle_engine::branch::{Branch, Builder, Op};
use mantle_engine::remix::View;
use mantle_engine::rows::Rows;
use mantle_engine::store::{Config, Store};

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

fn key(n: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&n.to_be_bytes());
    k
}

fn pct(lat: &mut [u64], q: f64) -> u64 {
    lat.sort_unstable();
    lat[((lat.len() as f64 * q) as usize).min(lat.len() - 1)]
}

fn main() {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| a != "--bench")
        .collect();
    let dir = PathBuf::from(args.first().expect("DIR"));
    let per_run: u64 = args.get(1).map_or(200_000, |s| s.parse().unwrap());
    let seeks: u64 = args.get(2).map_or(200_000, |s| s.parse().unwrap());
    let nexts: usize = args.get(3).map_or(10, |s| s.parse().unwrap());
    for runs in [2usize, 4, 8] {
        let path = dir.join("remix.store");
        let _ = std::fs::remove_file(&path);
        let align = Alignment::new(4096).unwrap();
        let file = DeviceFile::open(&path, true, CachingRequest::Buffered, align).unwrap();
        let mut s = Store::create(
            file,
            Config {
                page_size: 4096,
                extent_pages: 32,
                max_extents: 1 << 20,
            },
        )
        .unwrap();
        // Keys from a space twice a run's size: each run holds about half of it, so a key is in
        // about half the runs.
        let space = per_run * 2;
        let mut rng = Rng(7);
        let mut branches = Vec::new();
        for _ in 0..runs {
            let mut ks: Vec<u64> = (0..per_run).map(|_| rng.next() % space).collect();
            ks.sort_unstable();
            ks.dedup();
            let mut b = Builder::new(&mut s, Keys::Exactly(ks.len() as u64)).unwrap();
            for k in ks {
                b.add(&mut s, &key(k), Op::Put, &[b'v'; 100]).unwrap();
            }
            branches.push(b.finish(&mut s).unwrap());
        }
        let refs: Vec<&Branch> = branches.iter().collect();
        let t = Instant::now();
        let view = View::build(&mut s, &refs, b"", None).unwrap();
        let build_s = t.elapsed().as_secs_f64();
        let entries: u64 = branches.iter().map(|b| b.count).sum();
        let view_bytes = view.bytes();

        // The view: a seek and the next `nexts` live entries.
        let mut rows = Rows::new();
        let mut next = Vec::new();
        let mut lat = Vec::with_capacity(seeks as usize);
        let mut rng = Rng(11);
        alloc::begin();
        for _ in 0..seeks {
            let from = key(rng.next() % space);
            rows.clear();
            let o = Instant::now();
            view.scan(&mut s, &refs, &from, nexts, &mut rows, &mut next)
                .unwrap();
            lat.push(o.elapsed().as_nanos() as u64);
        }
        let a = alloc::end();
        println!(
            "runs {runs} view  seek+{nexts} ns p50 {} p99 {} p99.9 {} allocs/seek {:.2} reallocs/seek {:.2} | build {:.2}s {:.2} B/key",
            pct(&mut lat, 0.5),
            pct(&mut lat, 0.99),
            pct(&mut lat, 0.999),
            a.allocations as f64 / seeks as f64,
            a.reallocations as f64 / seeks as f64,
            build_s,
            view_bytes as f64 / entries as f64
        );

        // The merge: a cursor a run, the newest entry of each key, deletions passed over.
        lat.clear();
        let mut rng = Rng(11);
        alloc::begin();
        for _ in 0..seeks {
            let from = key(rng.next() % space);
            rows.clear();
            let o = Instant::now();
            let mut m = Merge::new(&mut s, refs.iter().copied(), &from, None).unwrap();
            let mut taken = 0;
            while taken < nexts {
                let Some((k, op, v)) = m.entry() else { break };
                if op == Op::Put {
                    rows.push(k, v);
                    taken += 1;
                }
                m.next(&mut s).unwrap();
            }
            m.give_back(&mut s);
            lat.push(o.elapsed().as_nanos() as u64);
        }
        let a = alloc::end();
        println!(
            "runs {runs} merge seek+{nexts} ns p50 {} p99 {} p99.9 {} allocs/seek {:.2} reallocs/seek {:.2}",
            pct(&mut lat, 0.5),
            pct(&mut lat, 0.99),
            pct(&mut lat, 0.999),
            a.allocations as f64 / seeks as f64,
            a.reallocations as f64 / seeks as f64
        );
        drop(s);
        std::fs::remove_file(&path).unwrap();
    }
}
