//! What a reservation from the fixed budget costs as its spare buffers grow: `n` buffers of
//! capacities from 64 bytes up are given back to a budget that keeps `n`, then a reservation of
//! 1,000 bytes and its release are timed, 20,000 times after 2,000 warm-up rounds. A node keeps a
//! spare for each peer and client it may serve (mantle's `Fixed::new(.., peers + clients)`), and a
//! range leader's first shard spent 6.7% of itself reserving (mantle, 2026-11-19).
//! `cargo bench --manifest-path vendor/hyper-transport/Cargo.toml --bench budget`
//! (docs/benchmarks.md, "A reservation beside spare buffers").

#![allow(
    clippy::unwrap_used,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_precision_loss,
    missing_docs
)]

use std::hint::black_box;
use std::time::Instant;

use hyper_measure::alloc;
use hyper_transport::{Budget, Fixed, Lane};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

const ROUNDS: u64 = 20_000;
const WARM: u64 = 2_000;
const GRANT: u64 = 1_000;

#[allow(
    clippy::disallowed_methods,
    reason = "a benchmark measures real time on the host"
)]
fn main() {
    println!("| Spare buffers | Allocations | Time per reservation and release |");
    println!("|---|---|---|");
    for n in std::iter::successors(Some(1usize), |n| Some(n * 4)).take_while(|n| *n <= 16_384) {
        let mut budget = Fixed::new(1 << 40, n);
        let made: Vec<_> = (0..n as u64)
            .map(|at| Budget::<()>::reserve(&mut budget, 64 + at * 7 % 4_096, Lane::Class(())))
            .collect::<Result<_, _>>()
            .unwrap();
        for each in made {
            Budget::<()>::release(&mut budget, each);
        }
        let round = |budget: &mut Fixed| {
            let taken = Budget::<()>::reserve(budget, GRANT, Lane::Class(())).unwrap();
            Budget::<()>::release(budget, black_box(taken));
        };
        for _ in 0..WARM {
            round(&mut budget);
        }
        let began = Instant::now();
        alloc::begin();
        for _ in 0..ROUNDS {
            round(&mut budget);
        }
        let counts = alloc::end();
        let took = began.elapsed();
        println!(
            "| {n} | {:.2} | {:.1} ns |",
            counts.allocations as f64 / ROUNDS as f64,
            took.as_secs_f64() * 1e9 / ROUNDS as f64,
        );
    }
}
