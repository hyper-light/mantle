//! A shared UDP port (`UdpSocket::bind_shared`, `UdpSocket::join_shared`, docs/runtime.md §5.1): a
//! listener and the sockets it connects to its peers bind one port with `SO_REUSEPORT`, each connected
//! socket taking its own peer's datagrams. Unix only: Windows refuses a shared port.
#![cfg(unix)]
// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods
)]

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use hyper_rt::udp::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};

/// Shape: the shared sockets the first test holds at once, at most. Bound to port 0 with `SO_REUSEPORT`
/// set before the bind, 500 sockets over Linux's default ephemeral range (32,768 to 60,999: 28,232 ports)
/// were each handed a port another already held about 500² / (2 × 28,232) ≈ 4.4 times a run, so a run
/// found none about once in a hundred (e^-4.4).
const SHARED: usize = 500;
/// Shape: how long a datagram on loopback is waited for before the test calls it lost.
const LAND: Duration = Duration::from_secs(5);

/// Loopback, the port the kernel's to choose.
fn any() -> SocketAddr {
    SocketAddr::from(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
}

/// Half the descriptors this process may have open (the soft limit: 256 on macOS by default), so the
/// test holds what any host lets it and raises no limit.
fn half_the_descriptors() -> usize {
    let limit = rustix::process::getrlimit(rustix::process::Resource::Nofile);
    limit.current.map_or(usize::MAX, |soft| {
        usize::try_from(soft).unwrap_or(usize::MAX)
    }) / 2
}

/// The next datagram `socket` takes and who sent it, waited for at most [`LAND`].
fn received(socket: &UdpSocket) -> (Vec<u8>, SocketAddr) {
    let mut buffer = [0u8; 64];
    let began = Instant::now();
    loop {
        if let Some((length, from)) = socket.try_recv_from(&mut buffer).unwrap() {
            return (buffer[..length].to_vec(), from);
        }
        assert!(began.elapsed() < LAND, "nothing arrived in {LAND:?}");
        std::thread::yield_now();
    }
}

/// A shared socket the kernel gives a port takes one no other socket holds (found by mantle's process
/// tests: two nodes' listeners on 127.0.0.1:59152, the one that announced it never answering on it).
/// Linux's ephemeral search leaves out of the ports in use those held by a socket that shares its port
/// with the binder's user (`udp_lib_lport_inuse`, net/ipv4/udp.c), so a socket asking for port 0 with
/// `SO_REUSEPORT` already set could be handed a port another socket of the user held, in this process
/// or another; XNU's search takes only a port no socket holds (`in_pcbbind`). Do: hold [`SHARED`]
/// sockets bound shared to port 0, or half the descriptors the process may open where that is fewer.
/// Expect: every port distinct.
#[test]
fn shared_sockets_given_port_zero_each_take_a_port_no_other_holds() {
    let count = SHARED.min(half_the_descriptors());
    let mut held = Vec::with_capacity(count);
    let mut ports = BTreeSet::new();
    let mut repeated = Vec::new();
    for _ in 0..count {
        let socket = UdpSocket::bind_shared(any()).unwrap();
        let port = socket.local_addr().unwrap().port();
        if !ports.insert(port) {
            repeated.push(port);
        }
        held.push(socket);
    }
    assert!(
        repeated.is_empty(),
        "{} of {count} shared sockets were given a port another held: {repeated:?}",
        repeated.len()
    );
}

/// A socket that binds a listener's shared port and connects to a peer takes that peer's datagrams,
/// and the listener every other sender's: the listener shares its port once the kernel has chosen it,
/// the joining socket before it binds, and each kernel checks the held socket's flag as another binds
/// (Linux `udp_lib_lport_inuse`, forming their group then, `udp_reuseport_add_sock`; XNU
/// `in_pcbbind`). A connected socket matches its peer's datagrams by both addresses first.
#[test]
fn a_socket_joining_a_shared_port_takes_its_connected_peers_datagrams() {
    let listener = UdpSocket::bind_shared(any()).unwrap();
    let shared = listener.local_addr().unwrap();
    let peer = UdpSocket::bind(any()).unwrap();
    let stranger = UdpSocket::bind(any()).unwrap();
    let mut own = UdpSocket::join_shared(shared).unwrap();
    own.connect(peer.local_addr().unwrap()).unwrap();
    assert_eq!(
        own.local_addr().unwrap(),
        shared,
        "joined the listener's port"
    );
    assert_eq!(peer.send_to(b"from the peer", shared).unwrap(), 13);
    assert_eq!(stranger.send_to(b"from a stranger", shared).unwrap(), 15);
    assert_eq!(
        received(&own),
        (b"from the peer".to_vec(), peer.local_addr().unwrap())
    );
    assert_eq!(
        received(&listener),
        (b"from a stranger".to_vec(), stranger.local_addr().unwrap())
    );
}
