//! Two nodes' transport endpoints on one thread, joined by an in-memory network on a virtual clock
//! (`tests/transport_memory.rs`, `tests/transport_ended.rs`): the test moves every datagram and
//! takes every event itself, so what a node's endpoint has handled when its owner takes an event
//! is the test's to say.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::Instant;

use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer, Tls};
use hyper_transport::{Config, Endpoint, Event, Fixed};
use mantle_cert::{KeyPair, Serial, Validity};
use mantle_node::derive;
use mantle_node::directory::{Peer, Peers};
use mantle_protocol::bounds::{self, Stated};
use mantle_protocol::classes::{Bounds, NodeClasses, Role};
use mantle_protocol::node_name;

const AUTHORITY: &str = "mantle transport test";

pub type Node = Endpoint<NodeClasses, Fixed, Peers>;

/// A certificate and its key, both DER.
pub type Issued = (Vec<u8>, Vec<u8>);

/// Two nodes' certificates under one authority, each with its key.
pub fn certificates() -> (Vec<u8>, [Issued; 2]) {
    let validity = Validity {
        not_before: 0,
        not_after: 253_402_300_799,
    };
    let key = KeyPair::generate().unwrap();
    let authority =
        mantle_cert::authority(&key, AUTHORITY, validity, Serial::draw().unwrap()).unwrap();
    let issue = |id: u64| {
        let own = KeyPair::generate().unwrap();
        let certificate = mantle_cert::node(
            &key,
            AUTHORITY,
            own.public_key(),
            &node_name(id),
            (validity, Serial::draw().unwrap()),
        )
        .unwrap();
        (certificate, own.pkcs8().to_vec())
    };
    (authority, [issue(1), issue(2)])
}

/// Node `id`'s endpoint, which knows `other` as its one peer.
pub fn endpoint(
    id: u64,
    authority: &[u8],
    own: &Issued,
    other: (u64, &[u8]),
    stated: &Stated,
    now: Instant,
) -> Node {
    let credentials = Credentials {
        chain: vec![CertificateDer::from(own.0.clone())],
        key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(own.1.clone())),
        roots: vec![CertificateDer::from(authority.to_vec())],
    };
    let config = Config {
        tls: Tls::mutual(&credentials, true).unwrap(),
        role: Role::Node,
        limits: bounds::limits(stated).unwrap(),
        congestion: Default::default(),
    };
    let directory = Peers::new(
        vec![Peer {
            id: other.0,
            role: Role::Node,
            certificate: other.1.to_vec(),
        }],
        0,
    );
    Endpoint::new_seeded(
        config,
        Fixed::new(stated.transport_bytes, stated.peers.max(1)),
        directory,
        now,
        [u8::try_from(id).unwrap(); 32],
    )
    .unwrap()
}

/// What a node of a cell of three states of its transport, with one peer and `transport_bytes`.
pub fn stated(transport_bytes: u64) -> Stated {
    let (settings, _) = derive::cell(3).unwrap();
    let largest = settings.max_size_per_msg.max(settings.max_entry_bytes)
        + hyper_raft::wire::MESSAGE_RECORD_FIXED_BYTES as u64
        + mantle_node::frame::HEADER_BYTES as u64;
    Stated {
        peers: 1,
        ranges: 5,
        inflight: settings.max_inflight_msgs,
        transfers: 38,
        transport_bytes,
        messages: Bounds {
            replication: largest,
            request: settings.max_entry_bytes + mantle_node::command::HEAD_BYTES as u64,
            transfer: u64::from(u32::MAX),
            bulk: u64::from(u32::MAX) + largest,
        },
        max_head: mantle_node::node::MAX_HEAD,
    }
}

/// Two endpoints, nodes 1 and 2 at indexes 0 and 1, and the datagrams between them; time moves to
/// the next timer when nothing is in flight.
pub struct Net {
    pub now: Instant,
    pub nodes: [Node; 2],
    pub addresses: [SocketAddr; 2],
    pub wire: VecDeque<(usize, Vec<u8>)>,
    pub out: Vec<u8>,
    /// What each node's owner was told, not yet acted on.
    pub events: [VecDeque<Event<NodeClasses>>; 2],
}

impl Net {
    /// Nodes 1 and 2 of `stated` at `now`, under `certificates`' authority; the certificates, the
    /// wire and the transmit buffer are made by the caller, so a test counting what the endpoints
    /// hold makes them before it counts.
    pub fn new(
        (authority, certs): &(Vec<u8>, [Issued; 2]),
        stated: &Stated,
        now: Instant,
        (wire, out): (VecDeque<(usize, Vec<u8>)>, Vec<u8>),
    ) -> Self {
        let nodes = [
            endpoint(1, authority, &certs[0], (2, &certs[1].0), stated, now),
            endpoint(2, authority, &certs[1], (1, &certs[0].0), stated, now),
        ];
        Self {
            now,
            nodes,
            addresses: [
                "127.0.0.1:4001".parse().unwrap(),
                "127.0.0.1:4002".parse().unwrap(),
            ],
            wire,
            out,
            events: [VecDeque::new(), VecDeque::new()],
        }
    }

    /// Puts on the wire every datagram node `from` sends now, delivering none.
    pub fn send(&mut self, from: usize) {
        while let Some(t) = self.nodes[from].poll_transmit(self.now, &mut self.out) {
            let segment = t.segment_size.unwrap_or(t.size);
            for datagram in self.out[..t.size].chunks(segment) {
                self.wire.push_back((1 - from, datagram.to_vec()));
            }
            self.out.clear();
        }
    }

    /// Hands each datagram on the wire to the node it goes to, in the order they left.
    pub fn deliver(&mut self) {
        while let Some((to, datagram)) = self.wire.pop_front() {
            let from = self.addresses[1 - to];
            self.nodes[to].handle_datagram(self.now, from, None, &datagram);
        }
    }

    /// Takes every event each node's endpoint holds for its owner.
    pub fn take_events(&mut self) {
        for at in 0..2 {
            while let Some(event) = self.nodes[at].poll_event() {
                self.events[at].push_back(event);
            }
        }
    }

    /// Carries every datagram either node sends until neither sends more, then takes the events.
    pub fn pump(&mut self) {
        loop {
            self.send(0);
            self.send(1);
            if self.wire.is_empty() {
                break;
            }
            self.deliver();
        }
        self.take_events();
    }

    /// Moves the clock to the earlier node's next timer and fires it.
    pub fn tick(&mut self) {
        let next = self.nodes.iter_mut().filter_map(|n| n.poll_timeout()).min();
        if let Some(next) = next {
            self.now = self.now.max(next);
            for node in &mut self.nodes {
                node.handle_timeout(self.now);
            }
        }
        self.pump();
    }

    /// Node 1 dials node 2, and the handshake runs until both owners are told they are connected;
    /// the events of the handshake are cleared.
    pub fn connect(&mut self) {
        self.nodes[0]
            .connect(self.now, 2, self.addresses[1])
            .unwrap();
        for _ in 0..1_000 {
            self.pump();
            let up = self
                .events
                .iter()
                .all(|e| e.iter().any(|e| matches!(e, Event::Connected { .. })));
            if up {
                break;
            }
            self.tick();
        }
        self.events[0].clear();
        self.events[1].clear();
    }
}
