//! The plane on real UDP sockets, between real processes: the usage its consumers make of it.
//!
//! `two_processes_exchange_sealed_messages` re-runs this test binary as the peer process (selected
//! by `HYPER_DATAGRAM_PEER`), so the two planes share no memory and talk only through the kernel's
//! sockets. The attack tests send from a third socket an honest peer never uses.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    missing_docs
)]

use std::net::UdpSocket;
use std::process::Command;
use std::time::Duration;

use hyper_datagram::{
    AdmitAll, Epoch, ExporterSecret, PeerId, Plane, PlaneLimits, Refusal, Role, SECRET_BYTES,
};

const LIMITS: PlaneLimits = PlaneLimits {
    max_peers: 8,
    epochs_per_peer: 2,
    window_limit: 1_024,
};
const PARENT: PeerId = 1;
const CHILD: PeerId = 2;
const EPOCH: Epoch = 1;
/// The longest a test waits for a datagram on loopback before it fails: far beyond any
/// loopback round trip, so a timeout means the datagram was lost or never sent.
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(10);
/// The messages the parent sends per datagram and the datagrams it sends.
const MESSAGES_PER_DATAGRAM: usize = 4;
const DATAGRAMS: usize = 50;

/// The secret both ends would export from their QUIC connection's TLS session.
fn secret() -> ExporterSecret {
    let mut bytes = [0u8; SECRET_BYTES];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::try_from(index).unwrap().wrapping_mul(37);
    }
    ExporterSecret::new(bytes)
}

fn bound() -> UdpSocket {
    UdpSocket::bind("127.0.0.1:0").unwrap()
}

/// The next datagram, waited for without taking it (`hyper_measure::wait::arrives`) and taken by a
/// receive that does not wait.
fn receive(socket: &UdpSocket) -> Vec<u8> {
    let mut buffer = vec![0u8; 65_536];
    assert!(
        hyper_measure::wait::arrives(socket, Some(RECEIVE_TIMEOUT), &mut buffer).unwrap(),
        "a datagram arrives"
    );
    socket.set_nonblocking(true).unwrap();
    let taken = socket.recv_from(&mut buffer);
    socket.set_nonblocking(false).unwrap();
    let (length, _) = taken.expect("the datagram peeked is taken");
    buffer.truncate(length);
    buffer
}

fn send_queued(plane: &mut Plane, socket: &UdpSocket, to: &str) {
    plane.flush(|_, datagram: Result<&[u8], Refusal>| {
        socket.send_to(datagram.unwrap(), to).unwrap();
    });
}

/// The peer process: echoes every message of every datagram back, sealed under its own key,
/// until it has echoed `DATAGRAMS` datagrams.
#[test]
#[allow(
    clippy::disallowed_methods,
    reason = "real processes on the host's clock, threads and environment (CLAUDE.md §1a, end to end)"
)]
fn peer_process() {
    let Ok(parent) = std::env::var("HYPER_DATAGRAM_PEER") else {
        return;
    };
    let socket = bound();
    let mut plane = Plane::new(CHILD, LIMITS).unwrap();
    plane
        .install_epoch(PARENT, EPOCH, &secret(), Role::Acceptor)
        .unwrap();
    plane.set_path(PARENT, 1_200).unwrap();
    // Announce the socket: an empty datagram the parent reads as the child's address.
    socket.send_to(&[], &parent).unwrap();
    for _ in 0..DATAGRAMS {
        let mut datagram = receive(&socket);
        let opened = plane.open(&mut datagram, &AdmitAll).unwrap();
        let echoes: Vec<Vec<u8>> = opened.messages().map(<[u8]>::to_vec).collect();
        for echo in echoes {
            plane.queue(PARENT, &echo).unwrap();
        }
        send_queued(&mut plane, &socket, &parent);
    }
}

#[test]
fn two_processes_exchange_sealed_messages() {
    let socket = bound();
    let address = socket.local_addr().unwrap().to_string();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "peer_process", "--nocapture"])
        .env("HYPER_DATAGRAM_PEER", &address)
        .spawn()
        .unwrap();
    let mut announce = [0u8; 1];
    let (_, child_address) = socket.recv_from(&mut announce).unwrap();
    let child_address = child_address.to_string();

    let mut plane = Plane::new(PARENT, LIMITS).unwrap();
    plane
        .install_epoch(CHILD, EPOCH, &secret(), Role::Initiator)
        .unwrap();
    plane.set_path(CHILD, 1_200).unwrap();
    for round in 0..DATAGRAMS {
        let sent: Vec<Vec<u8>> = (0..MESSAGES_PER_DATAGRAM)
            .map(|index| format!("round {round} message {index}").into_bytes())
            .collect();
        for message in &sent {
            plane.queue(CHILD, message).unwrap();
        }
        send_queued(&mut plane, &socket, &child_address);
        let mut reply = receive(&socket);
        let opened = plane.open(&mut reply, &AdmitAll).unwrap();
        assert_eq!(opened.sender, CHILD);
        let echoed: Vec<Vec<u8>> = opened.messages().map(<[u8]>::to_vec).collect();
        assert_eq!(echoed, sent, "round {round}");
    }
    assert!(child.wait().unwrap().success(), "the peer process failed");
}

#[test]
fn replays_and_forgeries_from_another_socket_are_refused_and_honest_traffic_continues() {
    let honest = bound();
    let receiver = bound();
    let attacker = bound();
    let to = receiver.local_addr().unwrap().to_string();

    let mut sender = Plane::new(PARENT, LIMITS).unwrap();
    sender
        .install_epoch(CHILD, EPOCH, &secret(), Role::Initiator)
        .unwrap();
    let mut plane = Plane::new(CHILD, LIMITS).unwrap();
    plane
        .install_epoch(PARENT, EPOCH, &secret(), Role::Acceptor)
        .unwrap();

    sender.queue(CHILD, b"commit index 41").unwrap();
    send_queued(&mut sender, &honest, &to);
    let first = receive(&receiver);
    let mut opening = first.clone();
    let opened = plane.open(&mut opening, &AdmitAll).unwrap();
    assert_eq!(opened.messages().next(), Some(&b"commit index 41"[..]));

    // The attacker captured the datagram and replays it.
    attacker.send_to(&first, &to).unwrap();
    let mut replayed = receive(&receiver);
    assert_eq!(
        plane.open(&mut replayed, &AdmitAll).map(|_| ()),
        Err(Refusal::Replay)
    );
    // It forges a later counter on the same body.
    let mut forged = first.clone();
    forged[13..21].copy_from_slice(&1_000u64.to_le_bytes());
    attacker.send_to(&forged, &to).unwrap();
    let mut forged = receive(&receiver);
    assert_eq!(
        plane.open(&mut forged, &AdmitAll).map(|_| ()),
        Err(Refusal::BadSeal)
    );

    // Honest traffic carries on unaffected by either.
    sender.queue(CHILD, b"commit index 42").unwrap();
    send_queued(&mut sender, &honest, &to);
    let mut next = receive(&receiver);
    let opened = plane.open(&mut next, &AdmitAll).unwrap();
    assert_eq!(opened.messages().next(), Some(&b"commit index 42"[..]));
}
