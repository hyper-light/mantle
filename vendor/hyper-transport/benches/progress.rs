//! What one call on an exchange costs while its connection's other exchanges wait for credit: the
//! shape a range leader's busiest shard showed under 12,000 small operations a second (mantle,
//! 2026-11-19), where more than half the shard's time went to the progress pass that follows every
//! call. A bulk body spends the connection's credit, its peer never reading it, so the peer never
//! gives credit back; then `n` requests are opened beside it, each holding a stream (the peer's
//! stream credit is `WIDE`), their heads waiting for credit the bulk body took; and, for the second
//! table, every lane the limits allow is filled to its window with frames that wait for credit too.
//! The timed call is the bulk body's write, refused for credit (`write_body` taking nothing): it
//! changes nothing, so every round is the same call on the same state, and its cost is the progress
//! pass's over what waits.
//! `cargo bench --manifest-path vendor/hyper-transport/Cargo.toml --bench progress`
//! (docs/benchmarks.md, "A call beside exchanges waiting for credit").

#![allow(
    clippy::unwrap_in_result,
    clippy::type_complexity,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity,
    missing_docs
)]

#[path = "../tests/common/mod.rs"]
mod common;

use std::time::{Duration, Instant};

use common::*;
use hyper_measure::alloc;
use hyper_transport::{Event, ExchangeId, Progress, Refusal};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// The connection's exchange limit, and its stream limit set equal so every exchange holds a
/// stream and waits for connection credit alone. Shape: `benches/lookup.rs`'s widest connection.
const WIDE: usize = 1_024;
/// Calls timed per row, after the warm-up.
const ROUNDS: u64 = 2_000;
const WARM: u64 = 200;
/// The head each request carries.
const HEAD: usize = 16;
/// The body each request declares: it never writes it, so its head waits with it declared.
const BODY: u64 = 1_024;
/// The bulk body: past any connection window the peer's receive side starts with, so writing it
/// spends the connection's credit.
const BULK: u64 = 64 << 20;
/// Each lane frame's bytes.
const FRAME: usize = 64;

/// Requests waiting beside the bulk exchange: doubling from 1, then the limit itself.
fn waiting_counts() -> Vec<usize> {
    let limit = WIDE - 1;
    let mut counts: Vec<usize> = std::iter::successors(Some(1usize), |n| Some(n * 2))
        .take_while(|n| *n < limit)
        .collect();
    counts.push(limit);
    counts
}

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

/// A bulk exchange whose body is written until the connection's credit is spent; its peer never
/// reads it.
fn spend_credit(net: &mut Net<Node<Mantle>, Node<Mantle>>) -> ExchangeId {
    let progress = Progress::new(Duration::from_secs(3_600)).unwrap();
    let bulk = net
        .a
        .open(
            net.now,
            2,
            Kind::Snapshot,
            &[1u8; HEAD],
            Some(BULK),
            progress,
        )
        .unwrap();
    let piece = vec![5u8; 64 << 10];
    for _ in 0..1_000 {
        let mut took = 0;
        while let Ok(n) = net.a.write_body(bulk, &piece) {
            if n == 0 {
                break;
            }
            took += n;
        }
        net.exchange();
        while net.a.poll_event().is_some() {}
        while net.b.poll_event().is_some() {}
        if took == 0 && !net.exchange() {
            return bulk;
        }
    }
    panic!("the connection's credit was never spent");
}

/// `n` requests opened beside the bulk exchange, each holding a stream, their heads waiting for the
/// credit it took.
fn wait_for_credit(net: &mut Net<Node<Mantle>, Node<Mantle>>, n: usize) {
    let head = [3u8; HEAD];
    for _ in 0..n {
        let progress = Progress::new(Duration::from_secs(3_600)).unwrap();
        net.a
            .open(net.now, 2, Kind::Get, &head, Some(BODY), progress)
            .unwrap();
    }
    net.exchange();
    while net.a.poll_event().is_some() {}
    while net.b.poll_event().is_some() {}
}

/// Every lane the limits allow, filled to its window with frames that wait for credit.
fn fill_lanes(net: &mut Net<Node<Mantle>, Node<Mantle>>) -> usize {
    let frame = [7u8; FRAME];
    let mut queued = 0;
    for lane in 0..limits().lanes_per_peer {
        loop {
            match net.a.send_frame(2, lane, Kind::Get, &frame) {
                Ok(()) => queued += 1,
                Err(Refusal::LaneFull) => break,
                Err(refusal) => panic!("lane {lane} refused a frame: {refusal:?}"),
            }
        }
    }
    queued
}

#[allow(
    clippy::disallowed_methods,
    reason = "a benchmark measures real time on the host"
)]
fn table(lanes: bool) {
    let piece = [9u8; 1_024];
    println!("| Waiting beside it | Lane frames queued | Allocations | Bytes | Time per call |");
    println!("|---|---|---|---|---|");
    for n in waiting_counts() {
        let mut net = transport_net();
        let bulk = spend_credit(&mut net);
        wait_for_credit(&mut net, n);
        let frames = if lanes { fill_lanes(&mut net) } else { 0 };
        let call = |net: &mut Net<Node<Mantle>, Node<Mantle>>| {
            assert_eq!(
                net.a.write_body(bulk, &piece).unwrap(),
                0,
                "credit came back"
            );
        };
        for _ in 0..WARM {
            call(&mut net);
        }
        let began = Instant::now();
        alloc::begin();
        for _ in 0..ROUNDS {
            call(&mut net);
        }
        let counts = alloc::end();
        let took = began.elapsed();
        let rounds = ROUNDS as f64;
        println!(
            "| {n} | {frames} | {:.2} | {:.0} | {:.2} µs |",
            counts.allocations as f64 / rounds,
            counts.bytes as f64 / rounds,
            took.as_secs_f64() * 1e6 / rounds,
        );
    }
}

fn main() {
    table(false);
    println!();
    table(true);
}
