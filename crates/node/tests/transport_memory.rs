//! What two nodes' transports hold in memory beside the budget each states (`transport_bytes`):
//! their QUIC connections' and TLS sessions' state, what QUIC keeps to send, and hyper-transport's
//! own, counted by hyper-measure's counting allocator with both endpoints on one thread, joined by
//! an in-memory network on a virtual clock.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use hyper_measure::alloc;
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer, Tls};
use hyper_transport::{Config, Endpoint, Event, ExchangeId, Fixed, Progress};
use mantle_cert::{KeyPair, Serial, Validity};
use mantle_node::bounds::{self, Stated};
use mantle_node::classes::{self, Bounds, Kind, NodeClasses, Role};
use mantle_node::derive;
use mantle_node::directory::{Peer, Peers, node_name};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

const AUTHORITY: &str = "mantle transport test";

type Node = Endpoint<NodeClasses, Fixed, Peers>;

/// A certificate and its key, both DER.
type Issued = (Vec<u8>, Vec<u8>);

/// Two nodes' certificates under one authority, each with its key.
fn certificates() -> (Vec<u8>, [Issued; 2]) {
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

fn endpoint(
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
    let directory = Peers::new(vec![Peer {
        id: other.0,
        role: Role::Node,
        certificate: other.1.to_vec(),
    }]);
    Endpoint::new_seeded(
        config,
        Fixed::new(stated.transport_bytes, stated.peers.max(1)),
        directory,
        now,
        [u8::try_from(id).unwrap(); 32],
    )
    .unwrap()
}

/// Two endpoints and the datagrams between them, delivered in order the instant they leave; time
/// moves to the next timer when nothing is in flight.
struct Net {
    now: Instant,
    nodes: [Node; 2],
    addresses: [SocketAddr; 2],
    wire: VecDeque<(usize, Vec<u8>)>,
    out: Vec<u8>,
    /// What each node's owner was told, not yet acted on.
    events: [VecDeque<Event<NodeClasses>>; 2],
}

impl Net {
    /// Carries every datagram either node sends until neither sends more.
    fn pump(&mut self) {
        loop {
            for from in 0..2 {
                while let Some(t) = self.nodes[from].poll_transmit(self.now, &mut self.out) {
                    let segment = t.segment_size.unwrap_or(t.size);
                    for datagram in self.out[..t.size].chunks(segment) {
                        self.wire.push_back((1 - from, datagram.to_vec()));
                    }
                    self.out.clear();
                }
            }
            if self.wire.is_empty() {
                break;
            }
            while let Some((to, datagram)) = self.wire.pop_front() {
                let from = self.addresses[1 - to];
                self.nodes[to].handle_datagram(self.now, from, None, &datagram);
            }
        }
        for at in 0..2 {
            while let Some(event) = self.nodes[at].poll_event() {
                self.events[at].push_back(event);
            }
        }
    }

    /// Moves the clock to the earlier node's next timer and fires it.
    fn tick(&mut self) {
        let next = self.nodes.iter_mut().filter_map(|n| n.poll_timeout()).min();
        if let Some(next) = next {
            self.now = self.now.max(next);
            for node in &mut self.nodes {
                node.handle_timeout(self.now);
            }
        }
        self.pump();
    }
}

fn stated(transport_bytes: u64) -> Stated {
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
    }
}

/// Runs two nodes of `stated` through a handshake and chunk puts that take every byte of credit,
/// and says what each step left.
fn run(stated: &Stated) -> Vec<(String, i64, i64)> {
    let (authority, certs) = certificates();
    let start = Instant::now();
    let mut steps = Vec::new();
    let mut step = |what: &str| {
        let c = alloc::read();
        steps.push((what.to_owned(), c.live, c.peak));
    };
    let wire = VecDeque::with_capacity(1 << 16);
    let out = Vec::with_capacity(1 << 16);
    alloc::begin();
    let nodes = [
        endpoint(1, &authority, &certs[0], (2, &certs[1].0), stated, start),
        endpoint(2, &authority, &certs[1], (1, &certs[0].0), stated, start),
    ];
    let mut net = Net {
        now: start,
        nodes,
        addresses: [
            "127.0.0.1:4001".parse().unwrap(),
            "127.0.0.1:4002".parse().unwrap(),
        ],
        wire,
        out,
        events: [VecDeque::new(), VecDeque::new()],
    };
    step("opened");
    net.nodes[0].connect(net.now, 2, net.addresses[1]).unwrap();
    for _ in 0..1_000 {
        net.pump();
        let up = net
            .events
            .iter()
            .all(|e| e.iter().any(|e| matches!(e, Event::Connected { .. })));
        if up {
            break;
        }
        net.tick();
    }
    net.events[0].clear();
    net.events[1].clear();
    step("connected");
    let body = vec![7u8; 1 << 16];
    let mut exchanges: Vec<(ExchangeId, u64)> = Vec::new();
    let deadline = Progress::new(Duration::from_secs(3600)).unwrap();
    let head = [0u8; mantle_node::chunk::PUT_HEAD_BYTES];
    for _ in 0..stated.transfers {
        let chunk = mantle_gateway::layout::CHUNK;
        if let Ok(id) = net.nodes[0].open(net.now, 2, Kind::ChunkPut, &head, Some(chunk), deadline)
        {
            exchanges.push((id, 0));
        }
    }
    for _ in 0..10_000 {
        let mut wrote = 0usize;
        for (id, written) in &mut exchanges {
            let left = mantle_gateway::layout::CHUNK - *written;
            let take = (left as usize).min(body.len());
            if take > 0
                && let Ok(n) = net.nodes[0].write_body(*id, &body[..take])
            {
                *written += n as u64;
                wrote += n;
            }
        }
        net.pump();
        if wrote == 0 {
            let before = net.now;
            net.tick();
            if net.now == before {
                break;
            }
        }
    }
    let written: u64 = exchanges.iter().map(|(_, w)| w).sum();
    step(&format!(
        "{} puts, {written} bytes written, budgets {} {}",
        exchanges.len(),
        net.nodes[0].budget().used(),
        net.nodes[1].budget().used()
    ));
    alloc::end();
    steps
}

/// Two nodes' endpoints hold no more than their budgets and what `derive::transport_bytes_beside`
/// states besides, at the floor's budget and at eight times it, for one, two and four peers and
/// two counts of chunk exchanges.
#[test]
fn two_nodes_transports_hold_their_budget_and_what_it_does_not_charge() {
    let least = bounds::least_transport(&stated(0)).unwrap();
    classes::state_bounds(stated(least).messages).unwrap();
    for (peers, transfers) in [(1usize, 38usize), (2, 38), (4, 38), (1, 76)] {
        let at = |bytes| Stated {
            peers,
            transfers,
            ..stated(bytes)
        };
        let least = bounds::least_transport(&at(0)).unwrap();
        for times in [1u64, 8] {
            let stated = at(least * times);
            let beside = derive::transport_bytes_beside(&stated).unwrap();
            let steps = run(&stated);
            let peak = steps.iter().map(|(_, _, peak)| *peak).max().unwrap();
            let most = 2 * (stated.transport_bytes + beside);
            eprintln!("peers {peers} transfers {transfers} x{times}: peak {peak} of {most}");
            for (what, live, peak) in &steps {
                eprintln!("  {what}: live {live} peak {peak}");
            }
            assert!(u64::try_from(peak).unwrap() <= most, "{peak} past {most}");
        }
    }
}

/// The plane holds no more than `derive::plane_bytes` states, each peer's queue filled to its
/// largest datagram; and a link estimator's ring at its longest window is what
/// `derive::liveness_bytes` counts a link at.
#[test]
fn the_plane_and_a_link_hold_what_derive_states() {
    use hyper_datagram::{ExporterSecret, MAX_DATAGRAM_BYTES, Plane, PlaneLimits};
    for (peers, width) in [(1usize, 64usize), (4, 64), (1, 4096), (4, 4096)] {
        let limits = PlaneLimits {
            max_peers: peers,
            epochs_per_peer: 2,
            window_limit: width,
        };
        alloc::begin();
        let mut plane = Plane::new(1, limits).unwrap();
        for peer in 0..peers as u64 {
            for epoch in 1..=3u32 {
                plane
                    .install_epoch(
                        peer + 2,
                        epoch,
                        &ExporterSecret::new([epoch as u8; 32]),
                        hyper_datagram::Role::Initiator,
                    )
                    .unwrap();
            }
            plane.set_path(peer + 2, MAX_DATAGRAM_BYTES).unwrap();
            while plane.queue(peer + 2, &[1u8; 1000]).is_ok() {}
        }
        let mut sealed = 0usize;
        plane.flush(|_, d| sealed = sealed.max(d.map_or(0, |d| d.len())));
        let c = alloc::end();
        let most = derive::plane_bytes(&limits).unwrap();
        eprintln!(
            "plane peers {peers} width {width}: peak {} of {most}; sealed {sealed}",
            c.peak
        );
        assert!(u64::try_from(c.peak).unwrap() <= most);
        drop(plane);
    }
    // A link's ring at its longest window, a granularity as long as its interval.
    let millis = Duration::from_millis(1);
    alloc::begin();
    let link = hyper_timing::LinkEstimator::new(millis, millis, None).unwrap();
    let c = alloc::end();
    drop(link);
    let ring = derive::liveness_bytes(1).unwrap() / 3;
    assert_eq!(u64::try_from(c.peak).unwrap(), ring);
}
