//! The allocations one handshake makes, client and server together, counted by a counting
//! allocator, against upstream rustls 0.23.45 (unmodified, built with this crate's one build path)
//! in the same process: the same shapes, the same certificates, the same counter.
//!
//! A handshake allocates the same number of blocks every time it runs, so the allocations are
//! held to upstream's exactly: no shape may make more than upstream's does. Reallocations and
//! bytes depend on the sizes the run draws (an ECDSA signature's DER length varies with its
//! integers), so they are printed for the record (VENDORED.md), and with Ed25519 credentials,
//! whose every size is fixed, they are held to upstream's too.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_types,
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    missing_docs
)]

use std::sync::atomic::{AtomicU64, Ordering};

use hyper_measure::alloc::{self, Counting, Counts};

#[global_allocator]
static GLOBAL: Counting = Counting;

/// The random bytes both implementations draw through their provider (client and server randoms,
/// session IDs, the order of a ClientHello's extensions): SplitMix64 from a state each run starts
/// at [`SEED`], so that both draw the same bytes in the same order. Key shares and signatures draw
/// from the cryptography library's own generator; their sizes are fixed for X25519, ML-KEM and
/// Ed25519.
static RANDOM: AtomicU64 = AtomicU64::new(SEED);
/// Where every run's random stream starts.
const SEED: u64 = 0x5eed;

fn reseed() {
    RANDOM.store(SEED, Ordering::Relaxed);
}

fn fill(buf: &mut [u8]) {
    for chunk in buf.chunks_mut(8) {
        // SplitMix64 (Steele, Lea and Flood, "Fast splittable pseudorandom number generators",
        // OOPSLA 2014).
        let mut z = RANDOM
            .fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed)
            .wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        chunk.copy_from_slice(&z.to_le_bytes()[..chunk.len()]);
    }
}

fn measure(f: impl FnOnce()) -> Counts {
    alloc::begin();
    f();
    alloc::end()
}

/// The credentials a shape runs with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pki {
    /// ECDSA P-256, as the counts recorded since the conformance (VENDORED.md §2, §3).
    EcdsaP256,
    /// Ed25519: every signature 64 bytes, so every size in a handshake is fixed.
    Ed25519,
}

impl Pki {
    fn file(self, name: &str) -> &'static [u8] {
        match (self, name) {
            (Self::EcdsaP256, "end.fullchain") => {
                include_bytes!("../test-ca/ecdsa-p256/end.fullchain")
            }
            (Self::EcdsaP256, "end.key") => include_bytes!("../test-ca/ecdsa-p256/end.key"),
            (Self::EcdsaP256, "ca.cert") => include_bytes!("../test-ca/ecdsa-p256/ca.cert"),
            (Self::EcdsaP256, "client.fullchain") => {
                include_bytes!("../test-ca/ecdsa-p256/client.fullchain")
            }
            (Self::EcdsaP256, "client.key") => include_bytes!("../test-ca/ecdsa-p256/client.key"),
            (Self::Ed25519, "end.fullchain") => include_bytes!("../test-ca/eddsa/end.fullchain"),
            (Self::Ed25519, "end.key") => include_bytes!("../test-ca/eddsa/end.key"),
            (Self::Ed25519, "ca.cert") => include_bytes!("../test-ca/eddsa/ca.cert"),
            (Self::Ed25519, "client.fullchain") => {
                include_bytes!("../test-ca/eddsa/client.fullchain")
            }
            (Self::Ed25519, "client.key") => include_bytes!("../test-ca/eddsa/client.key"),
            _ => unreachable!("no file {name}"),
        }
    }
}

/// A handshake shape: what the configurations are, what ran before, and which handshake counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    /// TLS 1.3, first contact.
    Tls13Full,
    /// TLS 1.3, resumed with a ticket from the full handshake.
    Tls13Resumed,
    /// TLS 1.3, the client's key share for a group the server does not take: a HelloRetryRequest.
    Tls13Retry,
    /// TLS 1.3, both sides authenticated.
    Tls13Mutual,
    /// TLS 1.2, first contact.
    Tls12Full,
    /// TLS 1.2, resumed by session ID from the server's session cache.
    Tls12Resumed,
    /// TLS 1.2 with a server that issues tickets, first contact.
    Tls12TicketFull,
    /// TLS 1.2, resumed with the ticket from the full handshake.
    Tls12TicketResumed,
    /// TLS 1.2, a resumption the server declines (it lost its cache): a full handshake follows.
    Tls12Declined,
    /// TLS 1.2, both sides authenticated.
    Tls12Mutual,
    /// A client of both versions holding a TLS 1.2 ticket meets a server now on TLS 1.3 that
    /// asks it to retry: the second ClientHello repeats the first's ticket (RFC 8446 §4.1.2).
    Tls12TicketThenTls13Retry,
}

const SHAPES: [Shape; 11] = [
    Shape::Tls13Full,
    Shape::Tls13Resumed,
    Shape::Tls13Retry,
    Shape::Tls13Mutual,
    Shape::Tls12Full,
    Shape::Tls12Resumed,
    Shape::Tls12TicketFull,
    Shape::Tls12TicketResumed,
    Shape::Tls12Declined,
    Shape::Tls12Mutual,
    Shape::Tls12TicketThenTls13Retry,
];

/// The configurations a shape's two sides use: the protocol versions, the key exchange groups (in
/// the order of preference; `None` is the provider's default list), tickets and client
/// authentication.
#[derive(Clone, Copy)]
struct Side {
    tls12: bool,
    tls13: bool,
    groups: Option<&'static [Group]>,
    tickets: bool,
    mutual: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Group {
    X25519,
    Secp384r1,
}

const BOTH: Side = Side {
    tls12: true,
    tls13: true,
    groups: None,
    tickets: false,
    mutual: false,
};
const TLS13: Side = Side {
    tls12: false,
    ..BOTH
};
const TLS12: Side = Side {
    tls13: false,
    ..BOTH
};

/// What a shape does: the client's configuration, the server configurations of the handshakes
/// before the counted one (each a fresh server), and the server of the counted one. `same_server`
/// keeps one server configuration for every handshake, as a resumption needs.
struct Plan {
    client: Side,
    before: &'static [Side],
    counted: Side,
    same_server: bool,
}

fn plan(shape: Shape) -> Plan {
    const RETRY_CLIENT: &[Group] = &[Group::Secp384r1, Group::X25519];
    const RETRY_SERVER: &[Group] = &[Group::X25519];
    const OLD_SERVER: &[Group] = &[Group::Secp384r1];
    match shape {
        Shape::Tls13Full => Plan {
            client: TLS13,
            before: &[],
            counted: TLS13,
            same_server: true,
        },
        Shape::Tls13Resumed => Plan {
            client: TLS13,
            before: &[TLS13],
            counted: TLS13,
            same_server: true,
        },
        Shape::Tls13Retry => Plan {
            client: Side {
                groups: Some(RETRY_CLIENT),
                ..TLS13
            },
            before: &[],
            counted: Side {
                groups: Some(RETRY_SERVER),
                ..TLS13
            },
            same_server: true,
        },
        Shape::Tls13Mutual => Plan {
            client: Side {
                mutual: true,
                ..TLS13
            },
            before: &[],
            counted: Side {
                mutual: true,
                ..TLS13
            },
            same_server: true,
        },
        Shape::Tls12Full => Plan {
            client: TLS12,
            before: &[],
            counted: TLS12,
            same_server: true,
        },
        Shape::Tls12Resumed => Plan {
            client: TLS12,
            before: &[TLS12],
            counted: TLS12,
            same_server: true,
        },
        Shape::Tls12TicketFull => Plan {
            client: TLS12,
            before: &[],
            counted: Side {
                tickets: true,
                ..TLS12
            },
            same_server: true,
        },
        Shape::Tls12TicketResumed => Plan {
            client: TLS12,
            before: &[Side {
                tickets: true,
                ..TLS12
            }],
            counted: Side {
                tickets: true,
                ..TLS12
            },
            same_server: true,
        },
        Shape::Tls12Declined => Plan {
            client: TLS12,
            before: &[TLS12],
            counted: TLS12,
            same_server: false,
        },
        Shape::Tls12Mutual => Plan {
            client: Side {
                mutual: true,
                ..TLS12
            },
            before: &[],
            counted: Side {
                mutual: true,
                ..TLS12
            },
            same_server: true,
        },
        Shape::Tls12TicketThenTls13Retry => Plan {
            client: Side {
                groups: Some(RETRY_CLIENT),
                ..BOTH
            },
            before: &[Side {
                groups: Some(OLD_SERVER),
                tickets: true,
                ..TLS12
            }],
            counted: Side {
                groups: Some(RETRY_SERVER),
                ..TLS13
            },
            same_server: false,
        },
    }
}

/// hyper-tls's side of every shape: configurations owned here and lent to each call.
mod hyper {
    use std::io;

    use hyper_tls::crypto::{aws_lc_rs, CryptoProvider, SupportedKxGroup};
    use hyper_tls::pki_types::pem::PemObject;
    use hyper_tls::pki_types::{CertificateDer, PrivateKeyDer};
    use hyper_tls::server::WebPkiClientVerifier;
    use hyper_tls::{
        ClientConfig, ClientConnection, HandshakeKind, RootCertStore, ServerConfig,
        ServerConnection,
    };

    use super::{measure, plan, reseed, Counts, Group, Pki, Shape, Side};

    #[derive(Debug)]
    struct Random;

    impl hyper_tls::crypto::SecureRandom for Random {
        fn fill(&self, buf: &mut [u8]) -> Result<(), hyper_tls::crypto::GetRandomFailed> {
            super::fill(buf);
            Ok(())
        }
    }

    fn provider(groups: Option<&'static [Group]>) -> &'static CryptoProvider {
        let mut provider = aws_lc_rs::default_provider();
        provider.secure_random = &Random;
        if let Some(groups) = groups {
            provider.kx_groups = groups
                .iter()
                .map(|group| -> &'static dyn SupportedKxGroup {
                    match group {
                        Group::X25519 => aws_lc_rs::kx_group::X25519,
                        Group::Secp384r1 => aws_lc_rs::kx_group::SECP384R1,
                    }
                })
                .collect();
        }
        // A configuration borrows its provider for 'static; a test leaks one per configuration.
        Box::leak(Box::new(provider))
    }

    fn versions(side: Side) -> Vec<&'static hyper_tls::SupportedProtocolVersion> {
        let mut versions = Vec::new();
        if side.tls13 {
            versions.push(&hyper_tls::version::TLS13);
        }
        if side.tls12 {
            versions.push(&hyper_tls::version::TLS12);
        }
        versions
    }

    fn roots(pki: Pki) -> RootCertStore {
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(pki.file("ca.cert")).unwrap())
            .unwrap();
        roots
    }

    fn chain(pki: Pki, name: &str) -> Vec<CertificateDer<'static>> {
        CertificateDer::pem_slice_iter(pki.file(name))
            .map(Result::unwrap)
            .collect()
    }

    pub(super) fn server(pki: Pki, side: Side) -> ServerConfig {
        let provider = provider(side.groups);
        let builder = ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&versions(side))
            .unwrap();
        let builder = if side.mutual {
            builder.with_client_cert_verifier(
                WebPkiClientVerifier::builder_with_provider(roots(pki), provider)
                    .build()
                    .unwrap(),
            )
        } else {
            builder.with_no_client_auth()
        };
        let key = PrivateKeyDer::from_pem_slice(pki.file("end.key")).unwrap();
        let mut config = builder
            .with_single_cert(chain(pki, "end.fullchain"), key)
            .unwrap();
        if side.tickets {
            config.ticketer = aws_lc_rs::Ticketer::new().unwrap();
        }
        config
    }

    pub(super) fn client(pki: Pki, side: Side) -> ClientConfig {
        let builder = ClientConfig::builder_with_provider(provider(side.groups))
            .with_protocol_versions(&versions(side))
            .unwrap()
            .with_root_certificates(roots(pki));
        if side.mutual {
            let key = PrivateKeyDer::from_pem_slice(pki.file("client.key")).unwrap();
            builder
                .with_client_auth_cert(chain(pki, "client.fullchain"), key)
                .unwrap()
        } else {
            builder.with_no_client_auth()
        }
    }

    /// Moves everything `from` has to send into `to`, through a stack buffer.
    fn transfer(
        from: &mut impl std::ops::DerefMut<
            Target = hyper_tls::ConnectionCommon<impl hyper_tls::SideData>,
        >,
        to: &mut impl std::ops::DerefMut<Target = hyper_tls::ConnectionCommon<impl hyper_tls::SideData>>,
    ) {
        let mut buf = [0u8; 65536];
        while from.wants_write() {
            let n = from.write_tls(&mut &mut buf[..]).unwrap();
            let mut offs = 0;
            while offs < n {
                offs += to.read_tls(&mut io::Cursor::new(&buf[offs..n])).unwrap();
            }
        }
    }

    /// One handshake to its end, and what kind the client saw.
    pub(super) fn handshake(client: &mut ClientConfig, server: &mut ServerConfig) -> HandshakeKind {
        let mut c = ClientConnection::new(client, "testserver.com".try_into().unwrap()).unwrap();
        let mut s = ServerConnection::new(server).unwrap();
        while c.is_handshaking() || s.is_handshaking() || c.wants_write() || s.wants_write() {
            transfer(&mut c, &mut s);
            s.process_new_packets(server).unwrap();
            transfer(&mut s, &mut c);
            c.process_new_packets(client).unwrap();
        }
        c.handshake_kind().unwrap()
    }

    pub(super) fn run(shape: Shape, pki: Pki) -> (Counts, HandshakeKind) {
        let plan = plan(shape);
        reseed();
        let mut client = client(pki, plan.client);
        let mut counted = server(pki, plan.counted);
        for before in plan.before {
            if plan.same_server {
                handshake(&mut client, &mut counted);
            } else {
                handshake(&mut client, &mut server(pki, *before));
            }
        }
        let mut kind = None;
        let counts = measure(|| kind = Some(handshake(&mut client, &mut counted)));
        (counts, kind.unwrap())
    }
}

/// Upstream rustls's side of every shape: configurations shared by `Arc`, as upstream has them.
mod upstream {
    use std::io;
    use std::sync::Arc;

    use upstream_rustls::crypto::{aws_lc_rs, CryptoProvider, SupportedKxGroup};
    use upstream_rustls::pki_types::pem::PemObject;
    use upstream_rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use upstream_rustls::server::WebPkiClientVerifier;
    use upstream_rustls::{
        ClientConfig, ClientConnection, HandshakeKind, RootCertStore, ServerConfig,
        ServerConnection,
    };

    use super::{measure, plan, reseed, Counts, Group, Pki, Shape, Side};

    #[derive(Debug)]
    struct Random;

    impl upstream_rustls::crypto::SecureRandom for Random {
        fn fill(&self, buf: &mut [u8]) -> Result<(), upstream_rustls::crypto::GetRandomFailed> {
            super::fill(buf);
            Ok(())
        }
    }

    fn provider(groups: Option<&'static [Group]>) -> Arc<CryptoProvider> {
        let mut provider = aws_lc_rs::default_provider();
        provider.secure_random = &Random;
        if let Some(groups) = groups {
            provider.kx_groups = groups
                .iter()
                .map(|group| -> &'static dyn SupportedKxGroup {
                    match group {
                        Group::X25519 => aws_lc_rs::kx_group::X25519,
                        Group::Secp384r1 => aws_lc_rs::kx_group::SECP384R1,
                    }
                })
                .collect();
        }
        Arc::new(provider)
    }

    fn versions(side: Side) -> Vec<&'static upstream_rustls::SupportedProtocolVersion> {
        let mut versions = Vec::new();
        if side.tls13 {
            versions.push(&upstream_rustls::version::TLS13);
        }
        if side.tls12 {
            versions.push(&upstream_rustls::version::TLS12);
        }
        versions
    }

    fn roots(pki: Pki) -> RootCertStore {
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(pki.file("ca.cert")).unwrap())
            .unwrap();
        roots
    }

    fn chain(pki: Pki, name: &str) -> Vec<CertificateDer<'static>> {
        CertificateDer::pem_slice_iter(pki.file(name))
            .map(Result::unwrap)
            .collect()
    }

    pub(super) fn server(pki: Pki, side: Side) -> Arc<ServerConfig> {
        let provider = provider(side.groups);
        let builder = ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&versions(side))
            .unwrap();
        let builder = if side.mutual {
            builder.with_client_cert_verifier(
                WebPkiClientVerifier::builder_with_provider(Arc::new(roots(pki)), provider)
                    .build()
                    .unwrap(),
            )
        } else {
            builder.with_no_client_auth()
        };
        let key = PrivateKeyDer::from_pem_slice(pki.file("end.key")).unwrap();
        let mut config = builder
            .with_single_cert(chain(pki, "end.fullchain"), key)
            .unwrap();
        if side.tickets {
            config.ticketer = aws_lc_rs::Ticketer::new().unwrap();
        }
        Arc::new(config)
    }

    pub(super) fn client(pki: Pki, side: Side) -> Arc<ClientConfig> {
        let builder = ClientConfig::builder_with_provider(provider(side.groups))
            .with_protocol_versions(&versions(side))
            .unwrap()
            .with_root_certificates(roots(pki));
        let config = if side.mutual {
            let key = PrivateKeyDer::from_pem_slice(pki.file("client.key")).unwrap();
            builder
                .with_client_auth_cert(chain(pki, "client.fullchain"), key)
                .unwrap()
        } else {
            builder.with_no_client_auth()
        };
        Arc::new(config)
    }

    /// Moves everything `from` has to send into `to`, through a stack buffer.
    fn transfer(
        from: &mut impl std::ops::DerefMut<
            Target = upstream_rustls::ConnectionCommon<impl upstream_rustls::SideData>,
        >,
        to: &mut impl std::ops::DerefMut<
            Target = upstream_rustls::ConnectionCommon<impl upstream_rustls::SideData>,
        >,
    ) {
        let mut buf = [0u8; 65536];
        while from.wants_write() {
            let n = from.write_tls(&mut &mut buf[..]).unwrap();
            let mut offs = 0;
            while offs < n {
                offs += to.read_tls(&mut io::Cursor::new(&buf[offs..n])).unwrap();
            }
        }
    }

    pub(super) fn handshake(
        client: &Arc<ClientConfig>,
        server: &Arc<ServerConfig>,
    ) -> HandshakeKind {
        let mut c =
            ClientConnection::new(client.clone(), "testserver.com".try_into().unwrap()).unwrap();
        let mut s = ServerConnection::new(server.clone()).unwrap();
        while c.is_handshaking() || s.is_handshaking() || c.wants_write() || s.wants_write() {
            transfer(&mut c, &mut s);
            s.process_new_packets().unwrap();
            transfer(&mut s, &mut c);
            c.process_new_packets().unwrap();
        }
        c.handshake_kind().unwrap()
    }

    pub(super) fn run(shape: Shape, pki: Pki) -> (Counts, HandshakeKind) {
        let plan = plan(shape);
        reseed();
        let client = client(pki, plan.client);
        let counted = server(pki, plan.counted);
        for before in plan.before {
            if plan.same_server {
                handshake(&client, &counted);
            } else {
                handshake(&client, &server(pki, *before));
            }
        }
        let mut kind = None;
        let counts = measure(|| kind = Some(handshake(&client, &counted)));
        (counts, kind.unwrap())
    }
}

/// The kind of handshake each shape must be, so that a count is of the shape it names.
fn expected_kind(shape: Shape) -> &'static str {
    match shape {
        Shape::Tls13Resumed | Shape::Tls12Resumed | Shape::Tls12TicketResumed => "Resumed",
        Shape::Tls13Retry | Shape::Tls12TicketThenTls13Retry => "FullWithHelloRetryRequest",
        _ => "Full",
    }
}

#[test]
fn allocations_per_handshake_against_upstream() {
    assert!(alloc::installed());
    // One-time initialisation (aws-lc, the process-default provider, each side's statics) is paid
    // outside the counts: one run of every shape on each side first.
    for pki in [Pki::EcdsaP256, Pki::Ed25519] {
        for shape in SHAPES {
            hyper::run(shape, pki);
            upstream::run(shape, pki);
        }
    }

    let mut over = Vec::new();
    for pki in [Pki::EcdsaP256, Pki::Ed25519] {
        for shape in SHAPES {
            let (ours, our_kind) = hyper::run(shape, pki);
            let (theirs, their_kind) = upstream::run(shape, pki);
            assert_eq!(format!("{our_kind:?}"), expected_kind(shape), "{shape:?}");
            assert_eq!(format!("{their_kind:?}"), expected_kind(shape), "{shape:?}");
            eprintln!(
                "{pki:?} {shape:?}: hyper-tls {} allocations, {} reallocations, {} B; \
                 upstream {} allocations, {} reallocations, {} B",
                ours.allocations,
                ours.reallocations,
                ours.bytes,
                theirs.allocations,
                theirs.reallocations,
                theirs.bytes
            );
            if ours.allocations > theirs.allocations {
                over.push(format!(
                    "{pki:?} {shape:?}: {} allocations against upstream's {}",
                    ours.allocations, theirs.allocations
                ));
            }
            if pki == Pki::Ed25519 {
                if ours.reallocations > theirs.reallocations {
                    over.push(format!(
                        "{pki:?} {shape:?}: {} reallocations against upstream's {}",
                        ours.reallocations, theirs.reallocations
                    ));
                }
                if ours.bytes > theirs.bytes {
                    over.push(format!(
                        "{pki:?} {shape:?}: {} bytes against upstream's {}",
                        ours.bytes, theirs.bytes
                    ));
                }
            }
        }
    }
    assert!(over.is_empty(), "more than upstream:\n{}", over.join("\n"));
}
