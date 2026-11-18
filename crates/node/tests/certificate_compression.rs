//! The certificates a node's handshakes send compressed (RFC 8879) decode with a Brotli decoder
//! written apart from the encoder that made them.
//!
//! A node's TLS keeps hyper-tls's compressors, Brotli first (`Tls::mutual`): its server side
//! compresses its certificate when a dialer offers Brotli, and its dialing side compresses its own
//! when the server's CertificateRequest asks, as every node's does. The gateway's range client
//! dials with the same client side (`mantle_node::client`), under a node-profile certificate named
//! `gateway-ID` (`mantle init`). The S3 door sends its certificate uncompressed
//! (`listener::tls_config`), and the native client neither compresses its own nor asks for a
//! node's (`mantle_client`). The encoder is the `brotli` crate, Dropbox's Rust port, at quality 11
//! and a window of the message's length (hyper-tls `compress.rs`, `window_bits`); the tree's other
//! Brotli decoder, `brotli-decompressor`, is that port's own.
//!
//! The decoder here is Google's reference decoder: google/brotli's C 1.2.1, the release with
//! CVE-2026-111497's fix (research/88 [2]), which `brotlic-sys` 0.2.5 builds and `brotlic` 0.8.4
//! binds (MIT, and MIT OR Apache-2.0 for the binding; a dev-dependency Ada allowed on 2026-11-18).
//! It runs as `BrotliDecoderCreateInstance` makes it, without the large-window extension, so a
//! stream it takes is RFC 7932 Brotli, the one format RFC 8879 §3 names.
//!
//! Each stream is taken from a real handshake between two nodes' transports: hyper-tls's own
//! `BROTLI_COMPRESSOR` is wrapped to keep what it was given and what it returned, which is what the
//! CompressedCertificate message carries (RFC 8879 §4), and nothing else of the handshake changes.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::cell::RefCell;
use std::collections::VecDeque;
use std::time::Instant;

use brotlic::BrotliDecoder;
use brotlic::decode::DecoderInfo;
use hyper_tls::CertificateCompressionAlgorithm;
use hyper_tls::compress::{BROTLI_COMPRESSOR, CertCompressor, CompressionFailed, CompressionLevel};
use hyper_transport::tls::Tls;
use mantle_cert::{KeyPair, Serial};
use mantle_protocol::bounds;
use mantle_protocol::node_name;

#[path = "support/net.rs"]
#[allow(dead_code)]
mod net;
use net::{Issued, Net};

/// What a handshake gave Brotli to compress and what Brotli returned, in the order compressed.
struct Sent {
    message: Vec<u8>,
    level: CompressionLevel,
    compressed: Vec<u8>,
}

thread_local! {
    /// What this thread's handshakes compressed: each test drives its endpoints on its own thread.
    static SENT: RefCell<Vec<Sent>> = const { RefCell::new(Vec::new()) };
}

/// hyper-tls's Brotli compressor, keeping what it compresses and returns.
#[derive(Debug)]
struct Kept;

impl CertCompressor for Kept {
    fn compress(
        &self,
        input: Vec<u8>,
        level: CompressionLevel,
    ) -> Result<Vec<u8>, CompressionFailed> {
        let compressed = BROTLI_COMPRESSOR.compress(input.clone(), level)?;
        SENT.with_borrow_mut(|sent| {
            sent.push(Sent {
                message: input,
                level,
                compressed: compressed.clone(),
            });
        });
        Ok(compressed)
    }

    fn algorithm(&self) -> CertificateCompressionAlgorithm {
        BROTLI_COMPRESSOR.algorithm()
    }
}

/// `tls` with its Brotli compressor, on both sides, kept: each side's compressors are otherwise
/// as they were, in their order, so a handshake chooses as a node's does.
fn keeping(mut tls: Tls) -> Tls {
    let mut sides = vec![&mut tls.client.cert_compressors];
    if let Some(server) = &mut tls.server {
        sides.push(&mut server.cert_compressors);
    }
    for compressors in sides {
        assert_eq!(
            compressors.first().map(|c| c.algorithm()),
            Some(CertificateCompressionAlgorithm::Brotli),
            "a node offers Brotli first"
        );
        compressors[0] = &Kept;
    }
    tls
}

/// `dialer` dials `server`, each its id and certificate under `authority`, and the handshake runs to
/// its end: what Brotli compressed on either side.
fn handshake(authority: &[u8], dialer: (u64, &Issued), server: (u64, &Issued)) -> Vec<Sent> {
    let stated = net::stated(bounds::least_transport(&net::stated(0)).unwrap());
    let now = Instant::now();
    let endpoint = |own: &Issued, (id, other): (u64, &Issued), seed| {
        let tls = keeping(net::tls(authority, own));
        net::endpoint_under(tls, (id, &other.0), &stated, now, seed)
    };
    let nodes = [
        endpoint(dialer.1, server, [1; 32]),
        endpoint(server.1, dialer, [2; 32]),
    ];
    let wire = VecDeque::with_capacity(1 << 16);
    let mut net = Net::of(nodes, now, (wire, Vec::with_capacity(1 << 16)));
    SENT.with_borrow_mut(Vec::clear);
    assert!(net.dial(server.0), "the handshake did not end");
    SENT.take()
}

/// The Certificate message a node sends of `certificate` (RFC 8446 §4.4.2): no request context,
/// and one entry, the certificate with no extensions.
fn message(certificate: &[u8]) -> Vec<u8> {
    let u24 = |len: usize| u32::try_from(len).unwrap().to_be_bytes()[1..].to_vec();
    [
        &[0][..],
        &u24(certificate.len() + 5),
        &u24(certificate.len()),
        certificate,
        &[0, 0],
    ]
    .concat()
}

/// The window a Brotli stream states in its first bits, WBITS (RFC 7932 §9.1); `None` for the
/// pattern RFC 7932 forbids, which the large-window extension takes.
fn wbits(stream: &[u8]) -> Option<u32> {
    let first = u32::from(*stream.first()?);
    if first & 1 == 0 {
        return Some(16);
    }
    match (first >> 1) & 0b111 {
        0 => match (first >> 4) & 0b111 {
            0 => Some(17),
            1 => None,
            n => Some(8 + n),
        },
        n => Some(17 + n),
    }
}

/// The least WBITS whose window, `(1 << WBITS) - 16` bytes (RFC 7932 §9.1), holds `len` bytes.
fn least_wbits(len: usize) -> u32 {
    (10..=24).find(|bits| (1usize << bits) - 16 >= len).unwrap()
}

/// `stream` through Google's decoder into exactly `len` bytes, the length a CompressedCertificate
/// states (RFC 8879 §4): it ends there, every byte of the stream read.
fn googles(stream: &[u8], len: usize) -> Vec<u8> {
    let mut decoder = BrotliDecoder::new();
    let mut decoded = vec![0; len];
    let result = decoder.decompress(stream, &mut decoded).unwrap();
    assert_eq!(
        (result.info, result.bytes_read, result.bytes_written),
        (DecoderInfo::Finished, stream.len(), len)
    );
    assert!(decoder.is_finished());
    decoded
}

/// What a side sent of `certificate`, its Certificate message compressed at the message's window,
/// decodes with Google's decoder to that message, the certificate's bytes within it.
fn decodes(what: &str, sent: &Sent, certificate: &[u8]) {
    let message = message(certificate);
    assert_eq!(sent.message, message, "{what}: the message compressed");
    assert_eq!(sent.level, CompressionLevel::Amortized, "{what}");
    let bits = wbits(&sent.compressed);
    assert_eq!(bits, Some(least_wbits(message.len())), "{what}: WBITS");
    let decoded = googles(&sent.compressed, message.len());
    eprintln!(
        "{what}: certificate {} bytes, message {}, sent {} at WBITS {}",
        certificate.len(),
        message.len(),
        sent.compressed.len(),
        bits.unwrap()
    );
    assert_eq!(decoded, message, "{what}");
    assert_eq!(&decoded[7..7 + certificate.len()], certificate, "{what}");
}

/// Each certificate a node or the gateway sends, compressed by a handshake, decodes with Google's
/// decoder: the shortest and the longest a node carries as `mantle init` issues them (granted,
/// under the cell's authority, ids and grants of one digit and of twenty), the gateway's at its
/// longest, and the transport tests' two the audit measured (`transport_memory.rs`: 326 and 365
/// bytes). The server compresses first, at the ClientHello; the dialer at the server's flight.
#[test]
fn every_certificate_a_handshake_compresses_decodes_with_googles_decoder() {
    let key = KeyPair::generate().unwrap();
    // `mantle init`'s name for a cell's authority, 28 characters whatever the cell's id.
    let name = format!("mantle cell {:016x}", u64::MAX);
    let authority = mantle_cert::authority(&key, &name, net::VALIDITY, Serial::draw().unwrap());
    let authority = authority.unwrap();
    let issue = |certificate: &dyn Fn(&KeyPair) -> Vec<u8>| -> Issued {
        let own = KeyPair::generate().unwrap();
        (certificate(&own), own.pkcs8().to_vec())
    };
    let granted = |id: u64, grant: u64| {
        issue(&|own: &KeyPair| {
            let (node, serial) = (node_name(id), Serial::draw().unwrap());
            let validity = (net::VALIDITY, serial);
            let names = (node.as_str(), grant);
            mantle_cert::granted_node(&key, &name, own.public_key(), names, validity).unwrap()
        })
    };
    let (shortest, longest) = (granted(1, 1), granted(u64::MAX, u64::MAX));
    let gateway = issue(&|own: &KeyPair| {
        let serial = Serial::draw().unwrap();
        let gateway = format!("gateway-{}", u64::MAX);
        let validity = (net::VALIDITY, serial);
        mantle_cert::node(&key, &name, own.public_key(), &gateway, validity).unwrap()
    });
    let (audited, audited_issued) = net::certificates_of([u64::MAX, 2]);
    let [audited_longest, audited_shortest] = &audited_issued;

    let runs = [
        (
            &authority,
            ("the shortest node's", 1, &shortest),
            ("the longest node's", u64::MAX, &longest),
        ),
        (
            &authority,
            ("the gateway's", u64::MAX, &gateway),
            ("the shortest node's", 1, &shortest),
        ),
        (
            &audited,
            ("the audit's 365-byte", u64::MAX, audited_longest),
            ("the audit's 326-byte", 2, audited_shortest),
        ),
    ];
    for (authority, dialer, server) in runs {
        let sent = handshake(authority, (dialer.1, dialer.2), (server.1, server.2));
        assert_eq!(sent.len(), 2, "each side compressed its own once");
        decodes(&format!("{}, served", server.0), &sent[0], &server.2.0);
        decodes(&format!("{}, dialed", dialer.0), &sent[1], &dialer.2.0);
    }
}
