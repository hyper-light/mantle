//! A socket's receive queue holds the room the transport tells of it from what the kernel is
//! measured to charge (hyper-transport's `ReceiveCharge`, `docs/transport.md` §4i): a datagram is
//! sent alone and its charge read before and after it is read (the driver's singleton rule), the
//! room is told from that sample, and the room's worth of the same datagrams sent unread are all
//! there to read, none dropped. What an endpoint lets its peers send unread stays within that room,
//! so a reader the machine stops loses nothing.

// Test harness code: a panic here is a failed test.
#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cognitive_complexity,
    clippy::cast_possible_truncation
)]

use std::time::{Duration, Instant};

use hyper_rt::udp::{Ipv4Addr, SocketAddrV4, UdpSocket};
use hyper_transport::{Charged, Queue, ReceiveCharge, Unprompted};

/// Shape: the windows' demand the socket is asked for, which every platform's default ceiling
/// grants.
const DEMAND: u64 = 32 << 10;
/// Shape: a full QUIC packet on an Ethernet path.
const DATAGRAM: usize = 1_252;
/// Shape: how long datagrams sent on loopback may take to land, at most (the failure guard).
const LAND: Duration = Duration::from_secs(5);

/// Waits until `receiver`'s queue is charged anything, or the guard passes.
fn until_charged(receiver: &UdpSocket, began: Instant) -> hyper_rt::udp::QueueCharge {
    loop {
        let charge = receiver.queue_charge().unwrap().unwrap();
        if charge.charged > 0 {
            return charge;
        }
        assert!(began.elapsed() < LAND, "the datagram never landed");
        std::thread::yield_now();
    }
}

#[test]
fn a_socket_holds_the_room_told_from_its_measured_charge() {
    let receiver = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
    receiver
        .set_recv_buffer_bytes(hyper_transport::socket_buffer_ask(DEMAND) as usize)
        .unwrap();
    if receiver.queue_charge().unwrap().is_none() {
        // Windows keeps no account a process may read; its room is measured otherwise.
        return;
    }
    let target = receiver.local_addr().unwrap();
    let sender = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
    let datagram = vec![0u8; DATAGRAM];
    let mut buffer = vec![0u8; 2 * DATAGRAM];
    let began = Instant::now();

    // The sample: one datagram alone, its charge before it is read, nothing after.
    assert_eq!(
        sender.try_send_to(&datagram, target).unwrap(),
        Some(DATAGRAM)
    );
    let before = until_charged(&receiver, began);
    assert_eq!(
        receiver
            .try_recv_from(&mut buffer)
            .unwrap()
            .map(|(bytes, _)| bytes),
        Some(DATAGRAM)
    );
    assert_eq!(receiver.queue_charge().unwrap().unwrap().charged, 0);
    let mut charge = ReceiveCharge::new();
    charge.sample(Charged {
        charged: before.charged.max(before.message_floor),
        stream: DATAGRAM as u64,
        datagrams: 1,
        bytes: DATAGRAM as u64,
    });
    let queue = Queue {
        buffer: before.buffer,
        release_lag: before.release_lag,
    };
    let room = charge.room(queue, Unprompted::default()).unwrap();
    let count = room as usize / DATAGRAM;
    assert!(count > 1, "a room of {room} from {before:?}");

    // The room's worth, unread, then every one read back.
    let mut sent = 0;
    while sent < count {
        match sender.try_send_to(&datagram, target).unwrap() {
            Some(bytes) => {
                assert_eq!(bytes, DATAGRAM);
                sent += 1;
            }
            None => {
                assert!(began.elapsed() < LAND, "the sender stayed full");
                std::thread::yield_now();
            }
        }
    }
    let mut received = 0;
    while received < count {
        match receiver.try_recv_from(&mut buffer).unwrap() {
            Some((bytes, _)) => {
                assert_eq!(bytes, DATAGRAM);
                received += 1;
            }
            None => {
                assert!(
                    began.elapsed() < LAND,
                    "{received} of {count} landed: the queue dropped what its room holds"
                );
                std::thread::yield_now();
            }
        }
    }
    assert!(
        receiver.try_recv_from(&mut buffer).unwrap().is_none(),
        "no more than sent"
    );
    let after = receiver.queue_charge().unwrap().unwrap();
    assert_eq!(after.drops, before.drops, "the kernel dropped none");
    eprintln!(
        "a datagram of {DATAGRAM} bytes charged {} (floor {}) against {}, lag {}: a room of {room}, \
         {count} datagrams held",
        before.charged, before.message_floor, before.buffer, before.release_lag
    );
}
