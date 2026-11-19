//! What a stream event costs as a connection's open exchanges grow: one exchange, request and
//! reply of a small body, timed and its allocations counted while `n` other exchanges stay open
//! and idle on the same connection, for `n` doubling from 1, then the limit itself: `WIDE` less the
//! timed exchange, the most the connection's exchange limit lets stay open beside it. The peer's
//! concurrent-stream credit is `WIDE` too, so credit never binds and every exchange holds a
//! stream: the bench times the scheduling core, not waiting for credit. Every Readable, Writable
//! and Stopped event of the timed exchange is matched to its exchange (`exchange_on`); a scan of
//! the connection's exchanges made that grow with `n`.
//! `cargo bench --manifest-path vendor/hyper-transport/Cargo.toml --bench lookup` (docs/benchmarks.md,
//! "Exchange lookup by stream").

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
use hyper_transport::{Event, ExchangeId, LEAST_PROGRESS, Progress};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// The connection's exchange limit under test, and its stream limit set equal so the peer's
/// stream credit never binds. Shape: the widest connection `benches/allocs.rs` measures
/// (`wide.exchanges`), so the two benches' rows compare.
const WIDE: usize = 1_024;
/// Exchanges timed per row, after the warm-up.
const ROUNDS: u64 = 2_000;
const WARM: u64 = 200;
/// The head each side sends.
const HEAD: usize = 16;
/// The timed exchange's body, each way. Derived: half the least datagram a QUIC path carries
/// (`LEAST_PROGRESS`, RFC 9000 §14's 1,200 bytes), so the head, the body and their framing go in
/// one datagram each way and the bench times per-event scheduling, not segmentation.
const BODY: u64 = LEAST_PROGRESS / 2;

/// Idle exchanges held open beside the timed one: doubling from 1, then the limit itself.
fn open_counts() -> Vec<usize> {
    let limit = WIDE - 1;
    let mut counts: Vec<usize> = std::iter::successors(Some(1usize), |n| Some(n * 2))
        .take_while(|n| *n < limit)
        .collect();
    counts.push(limit);
    counts
}

/// One request and its reply on hyper-transport, `body` bytes each way.
fn exchange(net: &mut Net<Node<Mantle>, Node<Mantle>>, body: Option<u64>, piece: &[u8]) {
    let progress = Progress::new(Duration::from_secs(10)).unwrap();
    let head = [7u8; HEAD];
    let id = net
        .a
        .open(net.now, 2, Kind::Get, &head, body, progress)
        .unwrap();
    let length = body.unwrap_or(0);
    let (mut written, mut served): (u64, Option<(ExchangeId, u64, u64, bool)>) = (0, None);
    let (mut read, mut answered, mut done) = (0u64, false, false);
    for _ in 0..100_000 {
        while written < length {
            let take = ((length - written) as usize).min(piece.len());
            match net.a.write_body(id, &piece[..take]).unwrap() {
                0 => break,
                took => written += took as u64,
            }
        }
        net.exchange();
        while let Some(event) = net.b.poll_event() {
            if let Event::Request { exchange, .. } = event {
                served = Some((exchange, 0, 0, false));
            }
        }
        if let Some((exchange, got, sent, replied)) = &mut served {
            while *got < length {
                let mut into = net.b.reserve(Class::Request, piece.len() as u64).unwrap();
                let n = net.b.read_body(*exchange, &mut into).unwrap();
                net.b.release(into);
                if n == 0 {
                    break;
                }
                *got += n as u64;
            }
            if *got == length && (body.is_none() || net.b.body_complete(*exchange)) && !*replied {
                net.b.reply(*exchange, &head, body).unwrap();
                *replied = true;
            }
            while *replied && *sent < length {
                let take = ((length - *sent) as usize).min(piece.len());
                match net.b.write_body(*exchange, &piece[..take]).unwrap() {
                    0 => break,
                    took => *sent += took as u64,
                }
            }
            if *replied && *sent == length {
                net.b.end(*exchange);
                served = None;
            }
        }
        net.exchange();
        while let Some(event) = net.a.poll_event() {
            match event {
                Event::Reply { exchange, .. } if exchange == id => answered = true,
                Event::Refused { exchange, .. } if exchange == id => {
                    panic!("the timed exchange was refused: {event:?}")
                }
                Event::Closed { .. } => panic!("the connection closed: {event:?}"),
                _ => {}
            }
        }
        if answered {
            while read < length {
                let mut into = net.a.reserve(Class::Request, piece.len() as u64).unwrap();
                let n = net.a.read_body(id, &mut into).unwrap();
                net.a.release(into);
                if n == 0 {
                    break;
                }
                read += n as u64;
            }
            if read == length && (body.is_none() || net.a.body_complete(id)) {
                net.a.end(id);
                done = true;
            }
        }
        if done && served.is_none() {
            net.exchange();
            // A removed exchange's seat is held until its last event is polled (the table's bound
            // holds the events of the removed too): at the limit the next open needs this one's, on
            // both sides.
            for node in [&mut net.a, &mut net.b] {
                while let Some(event) = node.poll_event() {
                    if let Event::Closed { .. } = event {
                        panic!("the connection closed: {event:?}");
                    }
                }
            }
            return;
        }
        if !net.exchange() {
            net.advance();
        }
    }
    panic!("the exchange never completed");
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

/// `n` exchanges opened from `a` and left open: their requests reach `b`, which never answers them.
fn hold_open(net: &mut Net<Node<Mantle>, Node<Mantle>>, n: usize) -> Vec<ExchangeId> {
    let head = [3u8; HEAD];
    let mut held = Vec::with_capacity(n);
    for _ in 0..n {
        let progress = Progress::new(Duration::from_secs(3_600)).unwrap();
        held.push(
            net.a
                .open(net.now, 2, Kind::Get, &head, Some(BODY), progress)
                .unwrap(),
        );
    }
    // Until the server has seen every held request, so none of them arrives while an exchange is timed (the server
    // takes the first request it sees as the timed one).
    let mut seen = 0;
    for _ in 0..1_000_000 {
        net.exchange();
        while net.a.poll_event().is_some() {}
        while let Some(event) = net.b.poll_event() {
            if matches!(event, Event::Request { .. }) {
                seen += 1;
            }
        }
        if seen == n {
            return held;
        }
        if !net.exchange() {
            net.advance();
        }
    }
    panic!("the server saw {seen} of the {n} held requests")
}

#[allow(
    clippy::disallowed_methods,
    reason = "a benchmark measures real time on the host"
)]
fn main() {
    let piece = vec![9u8; BODY as usize];
    println!("| Open beside it | Allocations | Bytes | Time per exchange |");
    println!("|---|---|---|---|");
    for n in open_counts() {
        let mut net = transport_net();
        let held = hold_open(&mut net, n);
        assert_eq!(held.len(), n);
        for _ in 0..WARM {
            exchange(&mut net, Some(BODY), &piece);
        }
        let began = Instant::now();
        alloc::begin();
        for _ in 0..ROUNDS {
            exchange(&mut net, Some(BODY), &piece);
        }
        let counts = alloc::end();
        let took = began.elapsed();
        let rounds = ROUNDS as f64;
        println!(
            "| {n} | {:.2} | {:.0} | {:.1} µs |",
            counts.allocations as f64 / rounds,
            counts.bytes as f64 / rounds,
            took.as_secs_f64() * 1e6 / rounds,
        );
    }
}
