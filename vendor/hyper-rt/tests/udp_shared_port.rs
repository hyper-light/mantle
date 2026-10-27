//! A shared UDP port is its listener's alone (docs/runtime.md §5.1): `UdpSocket::bind_shared` binds a
//! port no other socket holds, port 0 included, and only the sockets that join it on purpose
//! (`UdpSocket::join_shared`) share it. Shared at its bind, a socket asking Linux for port 0 could be
//! given a port another shared socket of the user held, and the two split one port's datagrams: a
//! client of 256 then never heard its server (hyper-rt-transport's `connection_memory` measurement).

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]
#![cfg(unix)]

use std::io::ErrorKind;
use std::time::{Duration, Instant};

use hyper_rt::RtError;
use hyper_rt::udp::{Ipv4Addr, SocketAddr, UdpSocket};

/// Shape: the sockets bound at port 0. Past 1,000 a port shared among them is all but certain where
/// a shared bind may be given a held port (Linux's ephemeral range holds about 28,000 ports: the
/// chance none of 1,024 meets another is under one in ten million); fewer where the process may hold
/// fewer descriptors, which the first refusal says.
const SOCKETS: usize = 1024;
/// Shape: how long a datagram on loopback is given before the test fails rather than hangs.
const PATIENCE: Duration = Duration::from_secs(10);

fn loopback(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

/// The OS's error kind of a refused call, where it named one.
fn kind(error: &RtError) -> Option<ErrorKind> {
    match error {
        RtError::DriverRefused {
            code: Some(code), ..
        } => Some(std::io::Error::from_raw_os_error(*code).kind()),
        _ => None,
    }
}

/// Do: bind up to 1,024 sockets with `bind_shared` at port 0, all held at once. Expect: no two share
/// a port.
#[test]
fn every_shared_bind_at_port_zero_is_given_a_port_of_its_own() {
    let mut held = Vec::new();
    for _ in 0..SOCKETS {
        match UdpSocket::bind_shared(loopback(0)) {
            Ok(socket) => held.push(socket),
            // The process's descriptors are spent (`EMFILE` at the socket's making): what was
            // bound is the sample.
            Err(RtError::DriverRefused {
                call: "socket(DGRAM)",
                ..
            }) => break,
            Err(error) => panic!("{error}"),
        }
    }
    assert!(held.len() >= 64, "only {} sockets bound", held.len());
    let mut ports: Vec<u16> = held
        .iter()
        .map(|socket| socket.local_addr().unwrap().port())
        .collect();
    ports.sort_unstable();
    let shared: Vec<u16> = ports
        .windows(2)
        .filter(|pair| pair[0] == pair[1])
        .map(|pair| pair[0])
        .collect();
    assert!(
        shared.is_empty(),
        "of {} sockets bound at port 0, these ports were given twice: {shared:?}",
        held.len()
    );
}

/// Do: bind a port with `bind_shared`, then bind it again with `bind_shared`, and with a plain bind.
/// Expect: both refused `AddrInUse`: the port is the first socket's alone until a socket joins it.
#[test]
fn a_shared_port_is_refused_to_a_second_listener() {
    let first = UdpSocket::bind_shared(loopback(0)).unwrap();
    let port = first.local_addr().unwrap().port();
    let second = UdpSocket::bind_shared(loopback(port)).map(|_| ());
    assert_eq!(
        second.as_ref().map_err(kind),
        Err(Some(ErrorKind::AddrInUse)),
        "{second:?}"
    );
    let plain = UdpSocket::bind(loopback(port)).map(|_| ());
    assert_eq!(
        plain.as_ref().map_err(kind),
        Err(Some(ErrorKind::AddrInUse)),
        "{plain:?}"
    );
}

/// Do: bind a port with `bind_shared`; join it with `join_shared` and connect the joiner to a peer;
/// the peer and a stranger each send to the port. Expect: the joiner takes its peer's datagram and
/// the listener the stranger's.
#[test]
fn a_joined_port_hands_the_joiner_its_peers_datagrams() {
    let listener = UdpSocket::bind_shared(loopback(0)).unwrap();
    let port = loopback(listener.local_addr().unwrap().port());
    let peer = UdpSocket::bind(loopback(0)).unwrap();
    let stranger = UdpSocket::bind(loopback(0)).unwrap();
    let mut joiner = UdpSocket::join_shared(port).unwrap();
    joiner.connect(peer.local_addr().unwrap()).unwrap();
    peer.send_to(b"peer", port).unwrap();
    stranger.send_to(b"stranger", port).unwrap();
    let mut buf = [0u8; 16];
    let took = |socket: &UdpSocket, buf: &mut [u8]| -> Vec<u8> {
        let began = Instant::now();
        loop {
            if let Some((n, _)) = socket.try_recv_from(buf).unwrap() {
                return buf[..n].to_vec();
            }
            assert!(began.elapsed() < PATIENCE, "nothing arrived");
            std::thread::sleep(Duration::from_millis(1));
        }
    };
    assert_eq!(took(&joiner, &mut buf), b"peer");
    assert_eq!(took(&listener, &mut buf), b"stranger");
}
