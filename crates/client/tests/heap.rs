//! The client's heap while it puts and gets an object many windows long: counted by hyper-measure's
//! allocator on the client's thread alone, against the bound the client states
//! (`mantle_client::memory`), from before it opens to after it is dropped. A node of the native
//! protocol's own, built from `mantle_protocol` and hyper-transport on a thread of its own, answers
//! it: it holds a put's bytes to their trailer's CRC-64/NVME and keeps nothing of them, and answers a
//! get with bytes it makes as it sends them.
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

use hyper_measure::alloc;
use mantle_client::{Client, Settings, TRAILER};
use mantle_protocol::bounds;
use mantle_protocol::object::{Headers, Op, Status};

mod support;
use support::{CLIENT, Checked, Made, NODE, credentials, runtime, stub};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// The client's peak heap, from before it opens until it is dropped, while it puts an object of
/// `windows` of its windows from a source that makes it and gets it into a sink that keeps none of
/// it; and the bound it states.
fn peak_of(windows: u64) -> (u64, u64) {
    let (node, client) = credentials();
    let transport_bytes = mantle_client::least_transport(1).unwrap();
    let stated = mantle_client::stated(1, transport_bytes);
    let window = bounds::limits(&stated).unwrap().window_ceiling;
    let length = windows * window;
    let (tell, bound) = channel();
    let served = std::thread::spawn(move || stub(node, transport_bytes, (length, None), tell));
    let address = bound.recv().unwrap();
    let sockets = mantle_client::sockets(1).unwrap();
    let (peak, memory) = runtime(sockets)
        .block_on(async move {
            alloc::begin();
            let mut client = Client::open(Settings {
                id: CLIENT,
                bind: "127.0.0.1:0".parse().unwrap(),
                credentials: client,
                nodes: vec![(NODE, address)],
                transport_bytes,
            })
            .await
            .unwrap();
            let memory = client.memory();
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
            let made = client
                .put(&put, &mut Made { at: 0, length }, u64::MAX)
                .await
                .unwrap();
            assert_eq!(made.status, Status::Done);
            let get = Op::Get {
                bucket: "b".into(),
                key: "k".into(),
                headers: Headers::default(),
            };
            let mut checked = Checked { at: 0 };
            let got = client.get(&get, &mut checked, u64::MAX).await.unwrap();
            assert_eq!((got.status, checked.at), (Status::Done, length));
            drop(client);
            let counts = alloc::end();
            (counts.peak, memory)
        })
        .unwrap();
    served.join().unwrap();
    let peak = u64::try_from(peak).unwrap();
    eprintln!("an object of {length} bytes, {windows} windows: peak {peak} of the {memory} stated");
    (peak, memory)
}

/// A put and a get of an object many windows long hold the client within the memory it states,
/// and one sixteen times longer within the same.
#[test]
fn a_put_and_a_get_many_windows_long_hold_the_client_within_its_stated_memory() {
    for windows in [64, 1024] {
        let (peak, memory) = peak_of(windows);
        assert_eq!(peak.saturating_sub(memory), 0, "{peak} past {memory}");
    }
}

/// A put whose source ends before the length its op states is refused before its last byte, so
/// nothing commits, and the node is told the body was cut short.
#[test]
fn a_source_shorter_than_its_stated_length_cuts_the_put_short() {
    let (node, client) = credentials();
    let transport_bytes = mantle_client::least_transport(1).unwrap();
    let (tell, bound) = channel();
    let served = std::thread::spawn(move || stub(node, transport_bytes, (0, None), tell));
    let address = bound.recv().unwrap();
    let sockets = mantle_client::sockets(1).unwrap();
    runtime(sockets)
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
            let put = Op::Put {
                bucket: "b".into(),
                key: "k".into(),
                id: mantle_client::identity().unwrap(),
                length: 10,
                headers: Headers::default(),
                checksum: None,
                md5: None,
                trailing: Some(TRAILER),
            };
            let short: &[u8] = &[1, 2, 3];
            let refused = client.put(&put, &mut { short }, u64::MAX).await;
            assert!(
                matches!(
                    refused,
                    Err(mantle_client::ClientError::Length {
                        stated: 10,
                        read: 3
                    })
                ),
                "{refused:?}"
            );
        })
        .unwrap();
    served.join().unwrap();
}
