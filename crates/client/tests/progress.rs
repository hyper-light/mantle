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
use support::{
    CLIENT, Checked, Made, NODE, Script, credentials, credentials_of, runtime, scripted, stub,
};

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

/// A get its node reads whole and never answers, alive, as a founder just started again held its
/// first reads, is cut at the node's serve period though the client has measured nothing yet, and
/// asked again of the next node, which answers it whole. With no period while nothing was measured,
/// a client waited on such a node for good, two hours once
/// (docs/measurements/2026-10-28-transfer-scale.md); asked again of the same node, it waited there
/// again; and a get cut on a busy host, its node slow to begin its answer, ended the read. A peer's
/// time to answer begins once it has the request whole, and the exchange is cut at the end of the
/// first period that began so (hyper-transport `progress`): two periods after it opened at most.
/// The get's deadline is three: the periods' timers and the deadline's run on the one shard, in
/// order, so the cut comes first, where with no period the deadline ended the get; and its object
/// fits a datagram, so its answer from the next node takes no period of its own.
#[test]
fn a_get_its_node_never_answers_is_cut_and_asked_of_the_next_node() {
    const OTHER: u64 = 2;
    let (mut nodes, client) = credentials_of(&[NODE, OTHER]);
    let (answering, holding) = (nodes.pop().unwrap(), nodes.pop().unwrap());
    let transport_bytes = mantle_client::least_transport(2).unwrap();
    let length = 1_000;
    let (tell_holding, bound_holding) = channel();
    let held = std::thread::spawn(move || {
        let script = Script {
            hold_get: Some(1),
            ..Script::default()
        };
        scripted(
            holding,
            transport_bytes,
            (length, None),
            script,
            tell_holding,
        )
    });
    let (tell, bound) = channel();
    let other = std::thread::spawn(move || {
        scripted(
            answering,
            transport_bytes,
            (length, None),
            Script::default(),
            tell,
        )
    });
    let (holding_at, answering_at) = (bound_holding.recv().unwrap(), bound.recv().unwrap());
    let sockets = mantle_client::sockets(2).unwrap();
    let served = u64::try_from(bounds::idle_timeout().as_nanos()).unwrap();
    let got = runtime(sockets)
        .block_on(async move {
            let mut client = Client::open(Settings {
                id: CLIENT,
                bind: "127.0.0.1:0".parse().unwrap(),
                credentials: client,
                nodes: vec![(NODE, holding_at), (OTHER, answering_at)],
                transport_bytes,
            })
            .await
            .unwrap();
            let get = Op::Get {
                bucket: "b".into(),
                key: "k".into(),
                headers: Headers::default(),
            };
            let deadline = hyper_rt::futures::now_ns() + 3 * served;
            let mut checked = Checked { at: 0 };
            let got = client.get(&get, &mut checked, deadline).await;
            got.map(|reply| (reply.status, checked.at))
        })
        .unwrap();
    assert!(
        matches!(got, Ok((Status::Done, at)) if at == length),
        "the get: {got:?}"
    );
    // The holding node first: the other's thread ends only once its client connected and left.
    let held = held.join().unwrap();
    assert_eq!((held.gets, held.cut), (1, 1));
    assert_eq!(other.join().unwrap().gets, 1);
}
