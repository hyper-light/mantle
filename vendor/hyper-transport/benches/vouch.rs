//! What an owner's vouch costs (`Endpoint::working`) beside `n` other open exchanges on its
//! connection: the call a serving node makes many times a turn, for its parked requests and the
//! replies it writes, and about a fifth of its busiest thread when a progress pass followed each
//! one (mantle, 2026-11-19: `tally::Table::settle` 23% of a node's first shard serving native
//! GETs, 88% of it under this call). Node 2 is asked `n + 1` head-only requests, each holding a
//! stream, and answers none; timed are node 2's vouches, for each of them in turn, as a node
//! vouches for each parked request it holds. After the first, every vouch finds the note already
//! pending and the clock never moves, so each sweep is the same calls on the same state.
//! `cargo bench --manifest-path vendor/hyper-transport/Cargo.toml --bench vouch`
//! (docs/benchmarks.md, "A vouch beside open exchanges").

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    missing_docs
)]

#[path = "../tests/common/mod.rs"]
mod common;

use std::time::{Duration, Instant};

use common::*;
use hyper_transport::{Event, ExchangeId, Progress};

/// The connection's exchange limit, and its stream limit set equal so every request holds a
/// stream. Shape: `benches/progress.rs`'s.
const WIDE: usize = 1_024;
/// Vouches timed per row, after a warm-up of a tenth as many: whole sweeps of the exchanges.
const VOUCHES: usize = 1 << 20;

/// A connected pair whose connections take `WIDE` exchanges and streams.
#[allow(
    clippy::disallowed_methods,
    reason = "a benchmark measures real time on the host"
)]
fn transport_net() -> Net<Node<Mantle>, Node<Mantle>> {
    let pair = Pair::new();
    let now = Instant::now();
    let mut wide = limits();
    wide.exchanges = WIDE;
    wide.streams_per_connection = WIDE as u32;
    let book = || pair.book(Role::Node, Role::Node);
    let a = pair.node::<Mantle>(1, Role::Node, book(), wide, 1 << 30, now);
    let b = pair.node::<Mantle>(2, Role::Node, book(), wide, 1 << 30, now);
    let mut net = Net::new(now, a, b);
    let address = net.b_address;
    net.a.connect(now, 2, address).unwrap();
    net.until(10_000, |net| {
        let mut connected = false;
        while let Some(event) = net.a.poll_event() {
            connected |= matches!(event, Event::Connected { .. });
        }
        while net.b.poll_event().is_some() {}
        connected
    });
    net
}

/// `asks` head-only requests from node 1, open at node 2 and unanswered: node 2's exchanges.
fn served(net: &mut Net<Node<Mantle>, Node<Mantle>>, asks: usize) -> Vec<ExchangeId> {
    for seed in 0..asks {
        let progress = Progress::new(Duration::from_secs(3_600)).unwrap();
        let head = (seed as u32).to_le_bytes();
        net.a
            .open(net.now, 2, Kind::Get, &head, None, progress)
            .unwrap();
    }
    let mut served = Vec::with_capacity(asks);
    net.until(10_000, |net| {
        while net.a.poll_event().is_some() {}
        while let Some(event) = net.b.poll_event() {
            if let Event::Request { exchange, .. } = event {
                served.push(exchange);
            }
        }
        served.len() == asks
    });
    served
}

#[allow(
    clippy::disallowed_methods,
    reason = "a benchmark measures real time on the host"
)]
fn main() {
    println!("| Open beside it | Time per vouch |");
    println!("|---|---|");
    for beside in [1, 32, 256, WIDE - 1] {
        let mut net = transport_net();
        let served = served(&mut net, beside + 1);
        let sweeps = (VOUCHES / served.len()).max(1);
        let sweep = |net: &mut Net<Node<Mantle>, Node<Mantle>>| {
            for &exchange in &served {
                net.b.working(exchange);
            }
        };
        for _ in 0..sweeps.div_ceil(10) {
            sweep(&mut net);
        }
        let began = Instant::now();
        for _ in 0..sweeps {
            sweep(&mut net);
        }
        let took = began.elapsed();
        let vouches = (sweeps * served.len()) as f64;
        println!(
            "| {beside} | {:.1} ns |",
            took.as_secs_f64() * 1e9 / vouches
        );
    }
}
