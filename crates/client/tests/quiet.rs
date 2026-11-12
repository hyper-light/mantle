//! A client idle between its calls: it runs no task of its own, so its connection sends nothing
//! while it waits, and its node closes it at the idle timeout without a word (RFC 9000 §10.1). The
//! client's next request goes on a connection dialed anew rather than on the one its node closed.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::large_enum_variant
)]

use std::sync::mpsc::channel;

use mantle_client::{Client, Settings};
use mantle_protocol::object::{Op, Status};

#[allow(dead_code)]
mod support;
use support::{CLIENT, NODE, Script, credentials, runtime, scripted};

/// A bucket's head answered, then the client left undriven until its node's idle timeout closed
/// its connection, which the node tells: the next head is answered, on a connection of its own.
#[test]
fn a_request_after_the_node_closed_an_idle_connection_is_answered_on_a_new_one() {
    let (node, credentials) = credentials();
    let transport_bytes = mantle_client::least_transport(1).unwrap();
    let (tell, bound) = channel();
    let served = std::thread::spawn(move || {
        scripted(
            node,
            transport_bytes,
            (0, None),
            Script {
                outlive: 1,
                ..Script::default()
            },
            tell,
        )
    });
    let address = bound.recv().unwrap();
    runtime(mantle_client::sockets(1).unwrap())
        .block_on(async move {
            let mut client = Client::open(Settings {
                id: CLIENT,
                bind: "127.0.0.1:0".parse().unwrap(),
                credentials,
                nodes: vec![(NODE, address)],
                names: Vec::new(),
                transport_bytes,
            })
            .await
            .unwrap();
            let head = Op::HeadBucket { bucket: "b".into() };
            let first = client.call(&head, u64::MAX).await.unwrap();
            assert_eq!(first.status, Status::Done);
            // The shard blocked here drives nothing of the client's, as a caller between its calls
            // leaves it, until the node says its idle timeout closed the connection.
            assert_eq!(bound.recv().unwrap(), address);
            let second = client.call(&head, u64::MAX).await.unwrap();
            assert_eq!(second.status, Status::Done);
        })
        .unwrap();
    served.join().unwrap();
}
