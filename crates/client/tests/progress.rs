//! How long the client gives an exchange that moves nothing: never less than the node's own serve
//! period, the idle timeout a node judges its exchanges stalled by (`mantle_protocol::bounds`), so a
//! put whose node stops reading its body while its driver waits on durable writes is waited for,
//! however fast the exchanges the client measured before it.

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

use mantle_client::{Client, Settings, TRAILER};
use mantle_protocol::bounds;
use mantle_protocol::object::{Headers, Op, Status};

#[allow(dead_code)]
mod support;
use support::{CLIENT, Made, NODE, credentials, runtime, stub};

/// A bucket's head answered at once, then a put whose node leaves its body unread for a quarter of
/// its serve period: the put is answered, taking at least the pause.
#[test]
fn a_put_its_node_holds_within_its_serve_period_is_answered() {
    let (node, client) = credentials();
    let transport_bytes = mantle_client::least_transport(1).unwrap();
    let stated = mantle_client::stated(1, transport_bytes);
    let length = 16 * bounds::limits(&stated).unwrap().window_ceiling;
    let pause = bounds::idle_timeout() / 4;
    let (tell, bound) = channel();
    let served =
        std::thread::spawn(move || stub(node, transport_bytes, (length, Some(pause)), tell));
    let address = bound.recv().unwrap();
    let sockets = mantle_client::sockets(1).unwrap();
    let took = runtime(sockets)
        .block_on(async move {
            let mut client = Client::open(Settings {
                id: CLIENT,
                bind: "127.0.0.1:0".parse().unwrap(),
                credentials: client,
                nodes: vec![(NODE, address)],
                transport_bytes,
            })
            .await
            .unwrap();
            let head = Op::HeadBucket { bucket: "b".into() };
            let headed = client.call(&head, u64::MAX).await.unwrap();
            assert_eq!(headed.status, Status::Done);
            let put = Op::Put {
                bucket: "b".into(),
                key: "k".into(),
                id: mantle_client::identity().unwrap(),
                length,
                headers: Headers::default(),
                checksum: None,
                md5: None,
                trailing: Some(TRAILER),
            };
            let began = std::time::Instant::now();
            let made = client
                .put(&put, &mut Made { at: 0, length }, u64::MAX)
                .await
                .unwrap_or_else(|e| panic!("the put the node held: {e}"));
            assert_eq!(made.status, Status::Done);
            began.elapsed()
        })
        .unwrap();
    served.join().unwrap();
    assert!(
        took >= pause,
        "the node held the put {took:?}, not {pause:?}"
    );
}
