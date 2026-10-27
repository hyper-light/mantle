//! Dials past a listener's handshake places: a burst of them is answered whole in the turn its
//! datagrams arrive, the room for the endpoint's own responses (version negotiation, retries,
//! refusals, stateless resets) is taken as they come, never laid out at the bound, and a ClientHello
//! its client sent again before a retry reached it holds no place.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity,
    missing_docs
)]

mod common;

use common::*;
use hyper_measure::alloc;
use hyper_transport::tls::Tls;
use hyper_transport::{AdmissionLimits, Config, Endpoint, Event, Fixed, Limits, PeerId, Refusal};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// The listener the dialers dial.
const LISTENER: PeerId = 1;
/// The dialers, each its own node: as many as focal's enrollment test sends a sponsor at once.
const DIALERS: PeerId = 8;

/// The listener's bounds: one handshake at a time among four connections, as focal's enrollment
/// sponsor in its joiners' test (a quarter of its connections), and `max_responses`.
fn listener_limits(max_responses: usize) -> Limits {
    Limits {
        admission: AdmissionLimits {
            pending: 1,
            identities: 4,
            connections: 4,
            per_identity: 1,
        },
        max_responses,
        ..limits()
    }
}

/// The listener and `dialers` dialers, each knowing every other, made at `now`.
fn nodes(dialers: PeerId, max_responses: usize, now: std::time::Instant) -> Vec<Node<Mantle>> {
    nodes_with(dialers, listener_limits(max_responses), now)
}

/// [`nodes`], the listener's bounds `listener`.
fn nodes_with(dialers: PeerId, listener: Limits, now: std::time::Instant) -> Vec<Node<Mantle>> {
    let issued = Issued::new(dialers + 1);
    issued
        .issued
        .iter()
        .enumerate()
        .map(|(at, (certificate, key))| {
            let peer = at as PeerId + 1;
            let config = Config {
                tls: Tls::mutual(
                    &common::credentials(&issued.pki.root, certificate, key),
                    true,
                )
                .unwrap(),
                role: Role::Node,
                limits: if peer == LISTENER { listener } else { limits() },
                congestion: Default::default(),
            };
            Endpoint::new_seeded(
                config,
                Fixed::new(256 << 20, 64),
                issued.book(),
                now,
                seed_of(peer),
            )
            .unwrap()
        })
        .collect()
}

/// The listener and its dialers on hyper-sim's network.
fn burst(max_responses: usize) -> Mesh<Node<Mantle>> {
    let now = hyper_sim::Anchor::new().instant(0).unwrap();
    Mesh::new(
        nodes(DIALERS, max_responses, now),
        now,
        hyper_sim::Source::Seed(0),
    )
}

/// The dialers that heard how their dial ended: up, or refused.
fn answered(mesh: &mut Mesh<Node<Mantle>>) -> PeerId {
    let mut answered = 0;
    for dialer in LISTENER + 1..=LISTENER + DIALERS {
        let node = mesh.node(dialer);
        let mut heard = false;
        while let Some(event) = node.poll_event() {
            heard |= matches!(
                event,
                Event::Connected { peer: LISTENER, .. }
                    | Event::Unreachable { peer: LISTENER, .. }
                    | Event::Closed { peer: LISTENER, .. }
            );
        }
        answered += PeerId::from(heard);
    }
    answered
}

/// Do: eight dialers dial a listener with one handshake place at once, the network moving every
/// datagram with no time passing, so no probe timeout fires. Expect: every dialer hears how its
/// dial ended, and the listener dropped no response. Its responses each answer one datagram it
/// took since it last sent, so a listener whose bound is what one receive takes answers a whole
/// burst: with a bound of one, a dialer whose retry was dropped heard nothing until its probe
/// timeout, as focal's joiners did, which failed their joins.
#[test]
fn a_burst_of_dials_past_the_handshake_places_is_answered_before_any_probe_timeout() {
    // At most two datagrams a dialer's first flight: a hybrid key share's ClientHello.
    let mut mesh = burst(2 * DIALERS as usize);
    let now = mesh.now;
    for dialer in LISTENER + 1..=LISTENER + DIALERS {
        mesh.node(dialer)
            .connect(now, LISTENER, mesh_address(LISTENER))
            .unwrap();
    }
    mesh.exchange();
    assert_eq!(
        answered(&mut mesh),
        DIALERS,
        "a dialer heard nothing of its dial before any probe timeout"
    );
    assert_eq!(mesh.node(LISTENER).stats().responses_dropped, 0);
}

/// Do: make the same listener with a bound of one response and of 4,096, counting what each
/// allocates, after one made first. Expect: the same bytes: the room for responses is taken as
/// they come.
#[test]
fn a_listener_lays_out_no_room_for_responses_it_has_not_made() {
    let made = |max_responses: usize| {
        let issued = Issued::new(1);
        let (certificate, key) = &issued.issued[0];
        let now = hyper_sim::Anchor::new().instant(0).unwrap();
        let config = Config {
            tls: Tls::mutual(
                &common::credentials(&issued.pki.root, certificate, key),
                true,
            )
            .unwrap(),
            role: Role::Node,
            limits: listener_limits(max_responses),
            congestion: Default::default(),
        };
        let book = issued.book();
        alloc::begin();
        let node: Node<Mantle> = Endpoint::new_seeded(
            config,
            Fixed::new(256 << 20, 64),
            book,
            now,
            seed_of(LISTENER),
        )
        .unwrap();
        let counts = alloc::end();
        drop(node);
        counts.bytes
    };
    assert!(
        alloc::installed(),
        "the counting allocator is not installed"
    );
    // The first makes what a process makes once (the TLS provider's tables among them).
    made(1);
    assert_eq!(made(1), made(1 << 12));
}

/// Datagrams between nodes `1..` at their mesh addresses, moved by hand, no time passing: each node's
/// transmits are taken, and handed to their destination unless `hold` keeps them back.
fn route(
    nodes: &mut [Node<Mantle>],
    now: std::time::Instant,
    mut hold: impl FnMut(PeerId, PeerId, &[u8]) -> bool,
) {
    let mut out = Vec::new();
    for _ in 0..10_000 {
        let mut moved = false;
        for at in 0..nodes.len() {
            let from = at as PeerId + 1;
            while let Some(transmit) = nodes[at].transmit(now, &mut out) {
                let to = PeerId::from(transmit.destination.port() - 4432);
                let bytes = out[..transmit.size].to_vec();
                if !hold(from, to, &bytes) {
                    nodes[(to - 1) as usize].datagram(now, mesh_address(from), &bytes);
                }
                moved = true;
            }
        }
        if !moved {
            return;
        }
    }
    panic!("the nodes never went quiet");
}

/// Whether `node` heard its dial of `peer` end, and how: up, or refused with its reason.
fn outcome(node: &mut Node<Mantle>, peer: PeerId) -> Option<Result<(), Option<Refusal>>> {
    let mut heard = None;
    while let Some(event) = node.poll_event() {
        match event {
            Event::Connected { peer: up, .. } if up == peer => heard = Some(Ok(())),
            Event::Unreachable {
                peer: down,
                refusal,
            }
            | Event::Closed {
                peer: down,
                refusal,
                ..
            } if down == peer => heard = Some(Err(refusal)),
            _ => {}
        }
    }
    heard
}

/// Do: a listener with one handshake place, dialed by A and by B. A takes the place; B's
/// ClientHello is answered with a retry, which is held back while A's handshake completes and the
/// place comes free; then B's first datagram arrives again, as a client sends its ClientHello again
/// before a retry reaches it, and only then the retry, so B comes back with its token. No time
/// passes. Expect: B is up and the listener holds no handshake pending. Before the token named the
/// ID B gave up, the second copy took the place free and B's token was refused for it, the copy
/// holding the place until its idle timeout: eight joiners of focal's enrollment sponsor waited 10 s
/// behind such a copy (2026-10-19).
#[test]
fn a_clienthello_sent_again_before_its_retry_holds_no_place() {
    const A: PeerId = 2;
    const B: PeerId = 3;
    let now = hyper_sim::Anchor::new().instant(0).unwrap();
    let mut nodes = nodes(2, 16, now);
    let mut to_a: Vec<Vec<u8>> = Vec::new();
    let mut to_b: Vec<Vec<u8>> = Vec::new();
    // A's ClientHello takes the place; the listener's answers to A wait.
    nodes[(A - 1) as usize]
        .connect(now, LISTENER, mesh_address(LISTENER))
        .unwrap();
    route(&mut nodes, now, |_, to, bytes| {
        if to == A {
            to_a.push(bytes.to_vec());
            return true;
        }
        false
    });
    assert_eq!(nodes[0].stats().admission.pending, 1, "A holds the place");
    // B's ClientHello, its first datagram kept to arrive again, is answered with a retry, which
    // waits.
    nodes[(B - 1) as usize]
        .connect(now, LISTENER, mesh_address(LISTENER))
        .unwrap();
    let mut first_of_b: Option<Vec<u8>> = None;
    route(&mut nodes, now, |from, to, bytes| {
        if from == B && first_of_b.is_none() {
            first_of_b = Some(bytes.to_vec());
        }
        match to {
            A => to_a.push(bytes.to_vec()),
            B => to_b.push(bytes.to_vec()),
            _ => return false,
        }
        true
    });
    assert!(!to_b.is_empty(), "B was answered");
    // A's handshake completes, B's answers still waiting: the place comes free.
    for datagram in std::mem::take(&mut to_a) {
        nodes[(A - 1) as usize].datagram(now, mesh_address(LISTENER), &datagram);
    }
    route(&mut nodes, now, |_, to, bytes| {
        if to == B {
            to_b.push(bytes.to_vec());
            return true;
        }
        false
    });
    assert_eq!(
        outcome(&mut nodes[(A - 1) as usize], LISTENER),
        Some(Ok(())),
        "A is up"
    );
    assert_eq!(nodes[0].stats().admission.pending, 0, "A's place is free");
    // B's ClientHello again under the ID the retry answered, then the retry, and B's token.
    nodes[0].datagram(now, mesh_address(B), &first_of_b.unwrap());
    for datagram in to_b {
        nodes[(B - 1) as usize].datagram(now, mesh_address(LISTENER), &datagram);
    }
    route(&mut nodes, now, |_, _, _| false);
    assert_eq!(
        outcome(&mut nodes[(B - 1) as usize], LISTENER),
        Some(Ok(())),
        "B was refused the place its own copy took"
    );
    assert_eq!(nodes[0].stats().admission.pending, 0);
}

/// Do: a listener with one handshake place. A takes it; B, answered with a retry, comes back with
/// its token and is refused for the place, its token's ClientHello kept to arrive again, as a
/// client sends a copy a spacing behind its original (hyper-quic's handshake copies); A's handshake
/// completes, the place comes free, and B's copy arrives, after B heard its refusal; then C dials.
/// No time passes. Expect: the copy takes no place, and C is up. Admitted, the copy of the attempt
/// B gave up held the place until its idle timeout, and C was refused for it: an enrollment
/// joiner's copy held its sponsor's one place 10 s in one run of six under CPU contention
/// (focal, 2026-10-19).
#[test]
fn a_copy_of_a_refused_attempt_holds_no_place() {
    const A: PeerId = 2;
    const B: PeerId = 3;
    const C: PeerId = 4;
    let now = hyper_sim::Anchor::new().instant(0).unwrap();
    let mut nodes = nodes(3, 16, now);
    let mut to_a: Vec<Vec<u8>> = Vec::new();
    // A takes the place; the listener's answers to A wait.
    nodes[(A - 1) as usize]
        .connect(now, LISTENER, mesh_address(LISTENER))
        .unwrap();
    route(&mut nodes, now, |_, to, bytes| {
        if to == A {
            to_a.push(bytes.to_vec());
            return true;
        }
        false
    });
    assert_eq!(nodes[0].stats().admission.pending, 1, "A holds the place");
    // B is answered with a retry, comes back with its token and is refused; its first datagram
    // with the token is kept.
    nodes[(B - 1) as usize]
        .connect(now, LISTENER, mesh_address(LISTENER))
        .unwrap();
    let mut sent_by_b = 0;
    let mut token_of_b: Option<Vec<u8>> = None;
    route(&mut nodes, now, |from, to, bytes| {
        if from == B {
            sent_by_b += 1;
            // Its first flight is the ClientHello without a token; the one after the retry
            // carries it.
            if token_of_b.is_none() && sent_by_b > 2 {
                token_of_b = Some(bytes.to_vec());
            }
        }
        if to == A {
            to_a.push(bytes.to_vec());
            return true;
        }
        false
    });
    assert!(
        matches!(
            outcome(&mut nodes[(B - 1) as usize], LISTENER),
            Some(Err(Some(Refusal::Pending)))
        ),
        "B was refused for the place A holds"
    );
    // A's handshake completes, and the place comes free.
    for datagram in std::mem::take(&mut to_a) {
        nodes[(A - 1) as usize].datagram(now, mesh_address(LISTENER), &datagram);
    }
    route(&mut nodes, now, |_, _, _| false);
    assert_eq!(
        outcome(&mut nodes[(A - 1) as usize], LISTENER),
        Some(Ok(())),
        "A is up"
    );
    assert_eq!(nodes[0].stats().admission.pending, 0, "A's place is free");
    // B's copy of its token's ClientHello, after B gave the attempt up.
    nodes[0].datagram(now, mesh_address(B), &token_of_b.unwrap());
    route(&mut nodes, now, |_, _, _| false);
    assert_eq!(
        nodes[0].stats().admission.pending,
        0,
        "the copy of the attempt B gave up took the place"
    );
    nodes[(C - 1) as usize]
        .connect(now, LISTENER, mesh_address(LISTENER))
        .unwrap();
    route(&mut nodes, now, |_, _, _| false);
    assert_eq!(
        outcome(&mut nodes[(C - 1) as usize], LISTENER),
        Some(Ok(())),
        "C was refused the place the copy took"
    );
}

/// Whether `datagram` is a Retry: a long-header packet of type 3 (RFC 9000 §17.2.5), its fixed
/// bit not read, which a peer may grease (RFC 9287).
fn is_retry(datagram: &[u8]) -> bool {
    datagram
        .first()
        .is_some_and(|first| first & 0x80 != 0 && (first >> 4) & 0x3 == 0x3)
}

/// Do: a listener whose handshakes in progress may hold nothing before one from an unproven
/// address is answered with a Retry (`Limits::retry_bytes` zero), dialed by one node; then one
/// that allows them more than any handshake holds. Expect: the first answers the ClientHello with a
/// Retry, and the dialer comes back with its token and is up; the second admits it outright, no
/// Retry sent. The handshakes in progress are counted at what a connection is charged as it is
/// made, against what the owner allows them, as msquic counts its handshakes' memory against its
/// `RetryMemoryLimit`: a rule of half the places tracked a count, which an owner deriving its
/// places from its memory made tens of thousands.
#[test]
fn a_retry_comes_once_the_handshakes_hold_what_the_owner_allows_them() {
    const DIALER: PeerId = 2;
    for (retry_bytes, retried) in [(0u64, true), (u64::MAX, false)] {
        let now = hyper_sim::Anchor::new().instant(0).unwrap();
        let listener = Limits {
            retry_bytes: Some(retry_bytes),
            ..listener_limits(16)
        };
        let mut nodes = nodes_with(1, listener, now);
        nodes[(DIALER - 1) as usize]
            .connect(now, LISTENER, mesh_address(LISTENER))
            .unwrap();
        let mut retries = 0;
        route(&mut nodes, now, |from, _, bytes| {
            if from == LISTENER && is_retry(bytes) {
                retries += 1;
            }
            false
        });
        assert_eq!(
            outcome(&mut nodes[(DIALER - 1) as usize], LISTENER),
            Some(Ok(())),
            "the dialer is up (allowance {retry_bytes})"
        );
        assert_eq!(
            retries > 0,
            retried,
            "{retries} retries under an allowance of {retry_bytes}"
        );
        assert_eq!(nodes[0].stats().admission.pending, 0);
    }
}
