//! A range filter's build alone, for counting its instructions: sorted 16-byte keys, an 8-byte
//! number and eight zero bytes as `ranges` writes them, spaced alike at every count so two counts
//! differ only by the keys between them. Counted at two sizes (`perf stat -e instructions:u`, or
//! macOS's `/usr/bin/time -l`), the difference over the keys between is a key's (research/67 §7).
//!
//! `cargo bench -p mantle-engine --bench surf_build -- KEYS [MODE] [DEPTH]`: MODE `gen` makes the
//! keys and their shared bytes alone, `push` also pushes them, `all` (the default) also finishes
//! the filter; DEPTH is the filter's (`none` for none; [`RANGE_FILTER_DEPTH`] by default).
#![allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use mantle_engine::fst::surf::SurfBuilder;
use mantle_engine::store::{RANGE_FILTER_BITS, RANGE_FILTER_DEPTH};

fn main() {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| a != "--bench")
        .collect();
    let keys: u64 = args.first().map_or(1_000_000, |s| s.parse().unwrap());
    let mode = args.get(1).map_or("all", String::as_str);
    let depth = match args.get(2).map(String::as_str) {
        Some("none") => usize::MAX,
        Some(d) => d.parse().unwrap(),
        None => RANGE_FILTER_DEPTH,
    };
    let mut b = SurfBuilder::new(RANGE_FILTER_BITS, depth, 0).unwrap();
    let (mut before, mut key) = ([0u8; 16], [0u8; 16]);
    // The gaps' mean, one eight-millionth of the numbers: the same spacing at every count.
    let step = u64::MAX / 8_000_000;
    let (mut state, mut n, mut sink) = (7u64, 0u64, 0usize);
    for i in 0..keys {
        // splitmix64's step and mix.
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        n = n.wrapping_add(1 + z % (2 * step));
        key[..8].copy_from_slice(&n.to_be_bytes());
        let shared = if i == 0 {
            0
        } else {
            before.iter().zip(&key).take_while(|(a, b)| a == b).count()
        };
        if mode == "gen" {
            sink = sink.wrapping_add(shared);
        } else {
            let previous = if i > 0 { Some(&before[..]) } else { None };
            b.push(previous, shared).unwrap();
        }
        before = key;
    }
    if mode == "all" {
        let last = if keys > 0 { Some(&before[..]) } else { None };
        let f = b.finish(last).unwrap();
        sink = sink.wrapping_add(f.len());
    }
    println!("{sink}");
}
