//! What an endpoint tells its peers of QUIC DATAGRAM frames (RFC 9221): nothing. No owner of
//! the transport sends or reads one, so an endpoint advertises no `max_datagram_frame_size`, and
//! hyper-quic holds no receive allowance for them on any of its connections. Each test reads the
//! parameter as the peer received it, from a bare hyper-quic connection on the other end, whose
//! `datagrams().max_size()` is `None` exactly when the peer sent no such parameter.

#![allow(
    clippy::unwrap_used,
    clippy::unwrap_in_result,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    missing_docs
)]

mod common;

use std::time::Instant;

use common::*;
use hyper_transport::Event;

const TURNS: usize = 10_000;
/// The test's endpoints' memory budget: room for a handshake, nothing more is sent.
const BUDGET: u64 = 64 << 20;

fn now() -> Instant {
    hyper_sim::Anchor::new().instant(0).unwrap()
}

/// The reading itself: a bare client and a bare server, both with hyper-quic's default transport,
/// each see the other advertise DATAGRAM frames. Without this, a `None` below could be the reading
/// failing rather than the parameter missing.
#[test]
fn bare_hyper_quic_advertises_datagrams_so_the_reading_sees_them() {
    let pair = Pair::new();
    let mut net = Net::new(now(), Bare::new(&pair, 1, false), Bare::new(&pair, 2, true));
    let (at, to) = (net.now, net.b_address);
    net.a.dial(at, to, 2);
    net.until(TURNS, |net| net.a.connected && net.b.connected);
    assert!(
        net.a.peer_datagrams().is_some(),
        "the bare server advertises DATAGRAM frames"
    );
    assert!(
        net.b.peer_datagrams().is_some(),
        "the bare client advertises DATAGRAM frames"
    );
}

/// A node that accepts a connection sends no `max_datagram_frame_size`.
#[test]
fn a_node_accepting_a_connection_advertises_no_datagrams() {
    let pair = Pair::new();
    let book = pair.book(Role::Node, Role::Node);
    let node = pair.node::<Mantle>(2, Role::Node, book, limits(), BUDGET, now());
    let mut net = Net::new(now(), Bare::new(&pair, 1, false), node);
    let (at, to) = (net.now, net.b_address);
    net.a.dial(at, to, 2);
    net.until(TURNS, |net| net.a.connected);
    assert_eq!(net.a.peer_datagrams(), None);
}

/// A node that dials sends no `max_datagram_frame_size`.
#[test]
fn a_node_dialing_a_connection_advertises_no_datagrams() {
    let pair = Pair::new();
    let book = pair.book(Role::Node, Role::Node);
    let node = pair.node::<Mantle>(1, Role::Node, book, limits(), BUDGET, now());
    let mut net = Net::new(now(), node, Bare::new(&pair, 2, true));
    let (at, to) = (net.now, net.b_address);
    net.a.connect(at, 2, to).unwrap();
    net.until(TURNS, |net| {
        while let Some(event) = net.a.poll_event() {
            assert!(
                !matches!(event, Event::Unreachable { .. }),
                "the dial failed: {event:?}"
            );
        }
        net.b.connected
    });
    assert_eq!(net.b.peer_datagrams(), None);
}
