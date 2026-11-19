//! A put's body is read into the piece the put's admission charged it for (node.md §2.8), never a
//! piece of the node's transport budget, which every connection's windows, receive chunks, heads
//! and frames share. Read through that budget, a body whose piece the budget refused was neither
//! read nor vouched for: the put waits on no ask of its own, so the host holds nothing of it, and
//! the budget freeing tells the node nothing. Its client, judging the exchange once a period, gave
//! it up: the node suite's 20 MiB put `Lost(Stalled)` among its six, about one run in ten on CI's
//! ubuntu-24.04-arm runners (2026-11-18).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::collections::VecDeque;
use std::time::Instant;

use hyper_transport::{Event, Progress, Reservation};
use mantle_node::node::Replied;
use mantle_protocol::bounds;
use mantle_protocol::classes::{self, Class, Kind};

#[path = "support/net.rs"]
mod net;
use net::{Net, certificates, stated};

/// Node 1 puts a body of many pieces to node 2 on the transfer class, the class a native put's
/// body comes on, judged as a client judges an object's exchange: once a period of the node's idle
/// timeout, the least a client's period is (`mantle_client`'s `period_ns`). Once node 2 has taken
/// the request, other traffic holds node 2's whole budget for good, and node 2 reads the body at
/// every turn as the host does. The body is read whole and the put is not given up.
#[test]
fn a_puts_body_is_read_while_other_traffic_holds_the_transports_whole_budget() {
    let least = bounds::least_transport(&stated(0)).unwrap();
    classes::state_bounds(stated(least).messages).unwrap();
    let mut net = Net::new(
        &certificates(),
        &stated(least),
        Instant::now(),
        (VecDeque::new(), Vec::new()),
    );
    net.connect();
    let body: Vec<u8> = (0..1u32 << 20).map(|n| (n % 251) as u8).collect();
    let head = mantle_node::chunk::Put {
        volume: 1,
        key: mantle_chunk::ChunkKey {
            block: 1,
            epoch: 0,
            index: 0,
        },
        crc32c: 0,
        remaining_ns: 0,
    }
    .encode();
    let period = bounds::idle_timeout();
    let put = net.nodes[0]
        .open(
            net.now,
            2,
            Kind::ChunkPut,
            &head,
            Some(body.len() as u64),
            Progress::new(period).unwrap(),
        )
        .unwrap();
    let began = net.now;
    let (mut written, mut taken, mut held) = (0, None, None);
    let (mut read, mut piece, mut refused) = (Vec::new(), Vec::new(), None);
    // Bounded by the clock: a put given up is given up within two periods of its last progress.
    while net.now.duration_since(began) < period * 8 {
        for event in net.events[0].drain(..) {
            if let Event::Refused {
                exchange, refusal, ..
            } = event
                && exchange == put
            {
                refused = Some(refusal);
            }
        }
        if refused.is_some() {
            break;
        }
        if written < body.len() {
            written += net.nodes[0].write_body(put, &body[written..]).unwrap();
        }
        net.pump();
        for event in net.events[1].drain(..) {
            if let Event::Request { exchange, .. } = event {
                taken = Some(exchange);
            }
        }
        let Some(exchange) = taken else {
            net.tick();
            continue;
        };
        if held.is_none() {
            let free = least - net.nodes[1].budget().used();
            held = Some(net.nodes[1].reserve(Class::Transfer, free).unwrap());
        }
        // Node 2's turn: the body read as far as it came, a piece at a time.
        loop {
            match mantle_node::node::read_piece(&mut net.nodes[1], exchange, &mut piece) {
                Ok(0) => break,
                Ok(_) => {
                    read.extend_from_slice(&piece);
                    piece.clear();
                }
                Err(refusal) => panic!("node 2's read of the put refused {refusal:?}"),
            }
        }
        if read.len() == body.len() {
            break;
        }
        net.tick();
    }
    assert_eq!(
        refused,
        None,
        "node 1 gave its put up {:?} after asking it, node 2 having read {} of its {} bytes",
        net.now.duration_since(began),
        read.len(),
        body.len()
    );
    assert!(
        read == body,
        "node 2 read {} of {} bytes",
        read.len(),
        body.len()
    );
    // The body's checksum, after its last byte, verified with the budget still held.
    let exchange = taken.unwrap();
    let _ = net.nodes[1].read_body(exchange, &mut Reservation::default());
    assert!(net.nodes[1].body_complete(exchange));
    net.nodes[1].release(held.unwrap());
}

/// Node 1 puts a small body to node 2, which reads it whole and then answers it once other traffic
/// holds every byte of node 2's budget: the reply's head fills the room node 2's endpoint took with
/// the request's head (hyper-transport's `Core::admit_prefix`), so it goes at its first offer and
/// node 1 has its reply. Taken from the budget as the reply was given, the head was refused for
/// room, as a finished job's was on CI's Linux runners, and a job ended at that refusal, as
/// `drain_job` ended it, left its exchange open with nothing more to come: node 1 gave the put up
/// `Stalled` a period later.
#[test]
fn a_reply_given_while_other_traffic_holds_the_whole_budget_goes_at_once() {
    let least = bounds::least_transport(&stated(0)).unwrap();
    classes::state_bounds(stated(least).messages).unwrap();
    let mut net = Net::new(
        &certificates(),
        &stated(least),
        Instant::now(),
        (VecDeque::new(), Vec::new()),
    );
    net.connect();
    let body = vec![7u8; 1 << 10];
    let head = mantle_node::chunk::Put {
        volume: 1,
        key: mantle_chunk::ChunkKey {
            block: 1,
            epoch: 0,
            index: 0,
        },
        crc32c: 0,
        remaining_ns: 0,
    }
    .encode();
    let period = bounds::idle_timeout();
    let put = net.nodes[0]
        .open(
            net.now,
            2,
            Kind::ChunkPut,
            &head,
            Some(body.len() as u64),
            Progress::new(period).unwrap(),
        )
        .unwrap();
    let began = net.now;
    let mut written = 0;
    let (mut taken, mut held, mut offered) = (None, None, None);
    let (mut piece, mut read) = (Vec::new(), 0);
    let (mut replied, mut refused) = (false, None);
    // Bounded by the clock: a put given up is given up within two periods of its last progress.
    while net.now.duration_since(began) < period * 4 {
        for event in net.events[0].drain(..) {
            match event {
                Event::Reply { exchange, .. } if exchange == put => replied = true,
                Event::Refused {
                    exchange, refusal, ..
                } if exchange == put => refused = Some(refusal),
                _ => {}
            }
        }
        if replied || refused.is_some() {
            break;
        }
        if written < body.len() {
            written += net.nodes[0].write_body(put, &body[written..]).unwrap();
        }
        net.pump();
        for event in net.events[1].drain(..) {
            if let Event::Request { exchange, .. } = event {
                taken = Some(exchange);
            }
        }
        let Some(exchange) = taken else {
            net.tick();
            continue;
        };
        // Node 2's turn: the body read whole into a piece of its own, then the reply offered once.
        while !net.nodes[1].body_complete(exchange) {
            match mantle_node::node::read_piece(&mut net.nodes[1], exchange, &mut piece) {
                Ok(0) => break,
                Ok(n) => {
                    read += n;
                    piece.clear();
                }
                Err(refusal) => panic!("node 2's read of the put refused {refusal:?}"),
            }
        }
        if net.nodes[1].body_complete(exchange) && offered.is_none() {
            let free = least - net.nodes[1].budget().used();
            held = Some(net.nodes[1].reserve(Class::Transfer, free).unwrap());
            assert_eq!(
                net.nodes[1].reserve(Class::Transfer, 1).err(),
                Some(hyper_transport::Refusal::Budget),
                "node 2's budget is full"
            );
            let reply =
                mantle_node::node::reply_head(&mut net.nodes[1], exchange, &[1, 2, 3], None);
            if reply == Replied::Sent {
                net.nodes[1].end(exchange);
            }
            offered = Some(reply);
        }
        net.pump();
        net.tick();
    }
    assert_eq!(read, body.len(), "node 2 read the put's body whole");
    assert_eq!(
        offered,
        Some(Replied::Sent),
        "node 2's reply went at its offer"
    );
    assert_eq!(
        refused,
        None,
        "node 1 gave its put up {:?} after asking it",
        net.now.duration_since(began)
    );
    assert!(replied, "node 1 had no reply");
    net.nodes[1].release(held.unwrap());
}
