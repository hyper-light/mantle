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
use std::time::{Duration, Instant};

use hyper_measure::alloc;
use hyper_transport::{ExchangeId, Progress};
use mantle_node::bounds::{self, Stated};
use mantle_node::classes::{self, Kind};
use mantle_node::derive;

#[path = "support/net.rs"]
mod net;
use net::{Net, certificates, stated};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// Runs two nodes of `stated` through a handshake and chunk puts that take every byte of credit,
/// and says what each step left.
fn run(stated: &Stated) -> Vec<(String, i64, i64)> {
    let certificates = certificates();
    let start = Instant::now();
    let mut steps = Vec::new();
    let mut step = |what: &str| {
        let c = alloc::read();
        steps.push((what.to_owned(), c.live, c.peak));
    };
    let wire = VecDeque::with_capacity(1 << 16);
    let out = Vec::with_capacity(1 << 16);
    alloc::begin();
    let mut net = Net::new(&certificates, stated, start, (wire, out));
    step("opened");
    net.connect();
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
