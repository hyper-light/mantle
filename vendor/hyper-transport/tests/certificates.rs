//! A node reads a client's certificate before it has authenticated it, and every node's
//! CertificateRequest offers to take it compressed (RFC 8879): what a client sends there is the
//! first thing a stranger can make a node decode. A certificate that does not decompress closes
//! that one connection with the TLS alert bad_certificate (RFC 9001 §4.8: CRYPTO_ERROR 0x100 plus
//! the alert, RFC 8446 §6), and the node goes on serving: the next client's certificate, compressed
//! with Brotli, is decoded and the connection made.

#![allow(
    clippy::unwrap_used,
    clippy::unwrap_in_result,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    missing_docs
)]

mod common;

use common::*;
use hyper_quic::rustls::compress::{
    BROTLI_COMPRESSOR, CertCompressor, CompressionFailed, CompressionLevel,
};
use hyper_quic::rustls::{AlertDescription, CertificateCompressionAlgorithm};
use hyper_quic::{ConnectionError, TransportErrorCode};
use hyper_transport::Event;

const TURNS: usize = 10_000;
/// The test's node's memory budget: room for a handshake, nothing more is sent.
const BUDGET: u64 = 64 << 20;

/// Says it compresses with Brotli and sends its bytes whatever the certificate.
#[derive(Debug)]
struct Junk(&'static [u8]);

impl CertCompressor for Junk {
    fn compress(&self, _: Vec<u8>, _: CompressionLevel) -> Result<Vec<u8>, CompressionFailed> {
        Ok(self.0.to_vec())
    }

    fn algorithm(&self) -> CertificateCompressionAlgorithm {
        CertificateCompressionAlgorithm::Brotli
    }
}

/// One seed a client's endpoint draws from, apart from every other: a client dialing again after a
/// refusal is a new endpoint, whose connection IDs are not the refused one's.
fn seed(n: u8) -> [u8; 32] {
    [n; 32]
}

#[test]
fn a_certificate_that_does_not_decompress_is_refused_and_the_node_serves_on() {
    let now = hyper_sim::Anchor::new().instant(0).unwrap();
    let pair = Pair::new();
    let book = pair.book(Role::Node, Role::Node);
    let node = pair.node::<Mantle>(2, Role::Node, book, limits(), BUDGET, now);
    let hostile = |pair: &Pair, n: u8, junk: &'static [u8]| {
        Bare::with(pair, 1, false, seed(n), |client| {
            client.cert_compressors = vec![Box::leak(Box::new(Junk(junk)))];
        })
    };
    let mut net = Net::new(now, hostile(&pair, 1, &[0xff; 64]), node);
    let junks: [&'static [u8]; 3] = [&[0xff; 64], &[0x00; 1024], &[0x8b, 0x02, 0x80]];
    for (n, junk) in (1u8..).zip(junks) {
        net.a = hostile(&pair, n, junk);
        let (at, to) = (net.now, net.b_address);
        net.a.dial(at, to, 2);
        net.until(TURNS, |net| net.a.lost.is_some());
        let bad_certificate =
            TransportErrorCode::crypto(u8::from(AlertDescription::BadCertificate));
        match net.a.lost.take() {
            Some(ConnectionError::ConnectionClosed(close)) => {
                assert_eq!(close.error_code, bad_certificate, "{junk:?}");
            }
            other => panic!("{junk:?}: {other:?}"),
        }
        while let Some(event) = net.b.poll_event() {
            assert!(
                !matches!(event, Event::Connected { .. }),
                "{junk:?}: {event:?}"
            );
        }
    }

    net.a = Bare::with(&pair, 1, false, seed(0x80), |client| {
        client.cert_compressors = vec![BROTLI_COMPRESSOR];
    });
    let (at, to) = (net.now, net.b_address);
    net.a.dial(at, to, 2);
    let mut served = false;
    net.until(TURNS, |net| {
        while let Some(event) = net.b.poll_event() {
            served |= matches!(event, Event::Connected { peer: 1, .. });
        }
        assert!(net.a.lost.is_none(), "{:?}", net.a.lost);
        served
    });
    assert!(net.a.connected);
}
