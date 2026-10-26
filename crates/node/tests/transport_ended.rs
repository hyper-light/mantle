//! What a node's endpoint tells its owner of an exchange the transport ends after queueing its
//! reply (hyper-transport's endpoint: an exchange's events hold its slot, removed or not). A
//! leader that answers a command and then closes its connections, as an orderly stop does once its
//! ranges are handed over, can have both reach the asker before the asker's next turn, and the
//! asker's driver handles every datagram that arrived before it hands out an event
//! (hyper-rt-transport's turn): the reply is told after the connection's loss removed the
//! exchange, with no head, and the exchange's refusal behind it. The node and its client take
//! such a reply as that refusal says, asking again (`Node::on_event`, `Client`), never its missing
//! head as an empty one: read so, a node's statement of its history was refused `Malformed` and
//! the node ended, in the serve suite's roll on loaded CI runners (2026-10-25).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use hyper_transport::{Event, ExchangeId, Progress, Refusal, Reservation};
use mantle_node::command::{self, Status};
use mantle_protocol::bounds;
use mantle_protocol::classes::{self, Kind};

#[path = "support/net.rs"]
mod net;
use net::{Net, certificates, stated};

/// Two connected nodes, node 1 having asked node 2 a command and node 2 having read it whole, as
/// a node reads a command before it answers: the exchange on node 1's side and on node 2's.
fn asked() -> (Net, ExchangeId, ExchangeId) {
    let least = bounds::least_transport(&stated(0)).unwrap();
    classes::state_bounds(stated(least).messages).unwrap();
    let mut net = Net::new(
        &certificates(),
        &stated(least),
        Instant::now(),
        (VecDeque::new(), Vec::new()),
    );
    net.connect();
    let entry = [7u8; 64];
    let deadline = Progress::new(Duration::from_secs(3_600)).unwrap();
    let asked = net.nodes[0]
        .open(
            net.now,
            2,
            Kind::Command,
            &command::head(1),
            Some(entry.len() as u64),
            deadline,
        )
        .unwrap();
    let (mut written, mut request) = (0, None);
    for _ in 0..1_000 {
        if written < entry.len() {
            written += net.nodes[0].write_body(asked, &entry[written..]).unwrap();
        }
        net.pump();
        request = request.or_else(|| {
            net.events[1].iter().find_map(|event| match event {
                Event::Request {
                    exchange, class, ..
                } => Some((*exchange, *class)),
                _ => None,
            })
        });
        if let Some((taken, class)) = request {
            let mut piece = net.nodes[1].reserve(class, entry.len() as u64).unwrap();
            let _ = net.nodes[1].read_body(taken, &mut piece).unwrap();
            net.nodes[1].release(piece);
            // The body's checksum, after its last byte.
            let _ = net.nodes[1].read_body(taken, &mut Reservation::default());
            if net.nodes[1].body_complete(taken) {
                net.events[0].clear();
                net.events[1].clear();
                return (net, asked, taken);
            }
        }
        net.tick();
    }
    panic!("node 2 never read node 1's command whole");
}

/// Node 2 redirects the command on `taken`, as a leader that has handed its range over answers
/// one, and ends its side of the exchange.
fn redirect(net: &mut Net, taken: ExchangeId) -> [u8; command::REPLY_HEAD_BYTES] {
    let head = command::reply_head(Status::Redirect, 0, 0);
    net.nodes[1].reply(taken, &head, None).unwrap();
    net.nodes[1].end(taken);
    head
}

#[test]
fn a_reply_taken_before_its_connection_is_lost_holds_its_head() {
    let (mut net, asked, taken) = asked();
    let head = redirect(&mut net, taken);
    net.pump();
    assert!(
        net.events[0]
            .iter()
            .any(|event| matches!(event, Event::Reply { exchange, .. } if *exchange == asked)),
        "{:?}",
        net.events[0]
    );
    assert_eq!(net.nodes[0].head(asked), Some(&head[..]));
}

#[test]
fn a_reply_told_after_its_connection_was_lost_has_no_head_and_its_refusal_follows() {
    let (mut net, asked, taken) = asked();
    redirect(&mut net, taken);
    // The reply leaves node 2, and then node 2 closes its connection, before node 1 takes either.
    net.send(1);
    net.nodes[1].disconnect(net.now, 1);
    net.send(1);
    // Node 1's turn, in its driver's order: what arrived, then its sends, then its events, taken
    // one at a time with a reply's head read as it is taken, as the node reads it.
    net.deliver();
    net.send(0);
    let mut told = Vec::new();
    while let Some(event) = net.nodes[0].poll_event() {
        let head = match &event {
            Event::Reply { exchange, .. } => Some(net.nodes[0].head(*exchange).map(<[u8]>::to_vec)),
            _ => None,
        };
        told.push((event, head));
    }
    let of_asked: Vec<&(Event<_>, Option<Option<Vec<u8>>>)> = told
        .iter()
        .filter(|(event, _)| {
            matches!(event, Event::Reply { exchange, .. } | Event::Refused { exchange, .. }
                if *exchange == asked)
        })
        .collect();
    assert!(
        matches!(
            of_asked.as_slice(),
            [
                (Event::Reply { .. }, Some(None)),
                (
                    Event::Refused {
                        refusal: Refusal::Closed,
                        ..
                    },
                    None
                )
            ]
        ),
        "{told:?}"
    );
}
