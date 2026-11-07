//! What writing a body costs the writer, counted by hyper-measure's allocator: a body borrowed
//! (`Endpoint::write_body`) is copied into a buffer of QUIC's own as each piece is taken, one
//! allocation of its bytes a write; a body the owner holds as `Bytes`
//! (`Endpoint::write_body_bytes`) is handed to QUIC by reference, and its writes after the stream's
//! first allocate nothing (mantle docs/research/80, "The sites": 16.8 MB an operation on a native
//! PUT of 8 MiB's servers, the replicas' bodies copied as they were written).
//!
//! Two endpoints on the in-memory network of `tests/common`; an exchange's request carries a body of
//! `BODY` bytes in pieces of `PIECE`, written as QUIC takes them, and the server reads it as it
//! arrives. Only the writer's calls are counted.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity,
    missing_docs
)]

mod common;

use std::time::Duration;

use bytes::Bytes;
use common::*;
use hyper_measure::alloc::{self, Counts};
use hyper_transport::{Event, Progress};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// The request's body: sixteen of the writer's pieces.
const BODY: usize = 16 * WRITE;
/// What one write offers.
const WRITE: usize = 4 << 10;

#[derive(Clone, Copy, PartialEq, Eq)]
enum How {
    Borrowed,
    Owned,
}

fn net() -> Net<Node<Mantle>, Node<Mantle>> {
    let pair = Pair::new();
    let now = hyper_sim::Anchor::new().instant(0).unwrap();
    let book = || pair.book(Role::Node, Role::Node);
    let a = pair.node::<Mantle>(1, Role::Node, book(), limits(), 1 << 30, now);
    let b = pair.node::<Mantle>(2, Role::Node, book(), limits(), 1 << 30, now);
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

/// One exchange whose request carries `BODY` bytes written `how`; returns the counts of the
/// writer's calls, its first write apart, and how many writes took bytes.
fn exchange(net: &mut Net<Node<Mantle>, Node<Mantle>>, how: How) -> (Counts, Counts, u64) {
    let body = Bytes::from(vec![5u8; BODY]);
    let progress = Progress::new(Duration::from_secs(10)).unwrap();
    let id = net
        .a
        .open(
            net.now,
            2,
            Kind::Get,
            &[7u8; 16],
            Some(BODY as u64),
            progress,
        )
        .unwrap();
    let (mut first, mut rest) = (Counts::default(), Counts::default());
    let (mut written, mut writes, mut read) = (0usize, 0u64, 0usize);
    let mut served = None;
    for _ in 0..100_000 {
        while written < BODY {
            let take = (BODY - written).min(WRITE);
            let piece = body.slice(written..written + take);
            alloc::begin();
            let took = match how {
                How::Borrowed => net.a.write_body(id, &piece),
                How::Owned => net.a.write_body_bytes(id, &piece),
            }
            .unwrap();
            let counts = alloc::end();
            if took == 0 {
                break;
            }
            let into = if writes == 0 { &mut first } else { &mut rest };
            into.allocations += counts.allocations;
            into.reallocations += counts.reallocations;
            into.bytes += counts.bytes;
            written += took;
            writes += 1;
        }
        net.exchange();
        while let Some(event) = net.b.poll_event() {
            if let Event::Request { exchange, .. } = event {
                served = Some(exchange);
            }
        }
        if let Some(exchange) = served {
            while read < BODY {
                let mut into = net.b.reserve(Class::Request, PIECE as u64).unwrap();
                let n = net.b.read_body(exchange, &mut into).unwrap();
                net.b.release(into);
                if n == 0 {
                    break;
                }
                read += n;
            }
            if read == BODY && net.b.body_complete(exchange) {
                net.b.reply(exchange, &[7u8; 16], None).unwrap();
                net.b.end(exchange);
                net.until(10_000, |net| {
                    let mut answered = false;
                    while let Some(event) = net.a.poll_event() {
                        answered |= matches!(event, Event::Reply { .. });
                    }
                    answered
                });
                net.a.end(id);
                net.exchange();
                return (first, rest, writes);
            }
        }
        if !net.exchange() {
            net.advance();
        }
    }
    panic!("the exchange never completed: {written} written");
}

fn counted(how: How) -> (Counts, Counts, u64) {
    let mut net = net();
    exchange(&mut net, how);
    let counts = exchange(&mut net, how);
    assert!(
        alloc::installed(),
        "the counting allocator is not installed"
    );
    counts
}

/// An owned body's writes after the stream's first copy nothing: the pieces are QUIC's by
/// reference. What they allocate is the trailer's copy, written with the last piece, and the
/// stream's queue of pieces grown to hold those in flight (three doublings for seventeen).
#[test]
fn an_owned_body_is_written_without_copying() {
    let (first, rest, writes) = counted(How::Owned);
    eprintln!("owned: {writes} writes, the first {first:?}, the rest {rest:?}");
    assert_eq!(
        (first.allocations, first.reallocations, first.bytes),
        (0, 0, 0),
        "the first owned write"
    );
    assert_eq!(
        (rest.allocations, rest.reallocations),
        (1, 3),
        "{} owned writes after the first: {rest:?}",
        writes - 1
    );
}

/// A borrowed body's writes each copy what QUIC took, an allocation of its bytes, beside what an
/// owned body's allocate.
#[test]
fn a_borrowed_body_is_copied_as_it_is_written() {
    let (first, rest, writes) = counted(How::Borrowed);
    eprintln!("borrowed: {writes} writes, the first {first:?}, the rest {rest:?}");
    assert_eq!(
        (first.allocations, first.reallocations, first.bytes),
        (1, 0, WRITE as u64),
        "the first borrowed write"
    );
    assert_eq!(
        (rest.allocations, rest.reallocations),
        (writes - 1 + 1, 3),
        "{} borrowed writes after the first: {rest:?}",
        writes - 1
    );
}
