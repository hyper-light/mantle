//! A connection's read buffer across records and idles, counted by a counting allocator on the
//! server's side of a TLS 1.3 and a TLS 1.2 connection: the first record at its largest grows the
//! deframer's buffer from one read to a record at its largest on the wire (`MAX_WIRE_SIZE`), a read
//! at a time and to exactly that capacity; then many more records, each drained and followed by a
//! read that finds the peer idle, reallocate it no more (VENDORED.md §13). Upstream let the buffer
//! go back to one read at every idle and grew it a read at a time for the next record, doubling its
//! capacity to 32 KiB on the way.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    missing_docs
)]

use std::io::{self, Read, Write};

use hyper_measure::alloc::{self, Counting, Counts};
use hyper_tls::crypto::aws_lc_rs::DEFAULT_PROVIDER;
use hyper_tls::pki_types::pem::PemObject;
use hyper_tls::pki_types::{CertificateDer, PrivateKeyDer};
use hyper_tls::{
    version, ClientConfig, ClientConnection, ConnectionCommon, RootCertStore, ServerConfig,
    ServerConnection, SideData, SupportedProtocolVersion,
};

#[global_allocator]
static GLOBAL: Counting = Counting;

/// The deframer's read (`prepare_read`'s `READ_SIZE`), and so its buffer after a handshake whose
/// records each arrive within one read.
const READ: usize = 4096;
/// A record at its largest on the wire: its 5-byte header, 2^14 bytes of plaintext (RFC 8446 §5.1)
/// and the most its protection adds, 2,048 bytes under TLS 1.2 (RFC 5246 §6.2.3), more than TLS
/// 1.3's 256 (RFC 8446 §5.2): the crate's `MAX_WIRE_SIZE`.
const MAX_WIRE_SIZE: usize = 5 + (1 << 14) + 2048;
/// A record's plaintext at its largest (RFC 8446 §5.1).
const PLAINTEXT: usize = 1 << 14;
/// The records read after the first.
const RECORDS: usize = 64;

/// A peer with nothing to send.
struct Idle;

impl Read for Idle {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }
}

fn configs(version: &'static SupportedProtocolVersion) -> (ClientConfig, ServerConfig) {
    let chain = CertificateDer::pem_slice_iter(include_bytes!("../test-ca/eddsa/end.fullchain"))
        .map(Result::unwrap)
        .collect();
    let key = PrivateKeyDer::from_pem_slice(include_bytes!("../test-ca/eddsa/end.key")).unwrap();
    let server = ServerConfig::builder_with_provider(&DEFAULT_PROVIDER)
        .with_protocol_versions(&[version])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .unwrap();
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(include_bytes!("../test-ca/eddsa/ca.cert")).unwrap())
        .unwrap();
    let client = ClientConfig::builder_with_provider(&DEFAULT_PROVIDER)
        .with_protocol_versions(&[version])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (client, server)
}

/// Moves everything `from` has to send into `to`.
fn transfer(
    from: &mut impl std::ops::DerefMut<Target = ConnectionCommon<impl SideData>>,
    to: &mut impl std::ops::DerefMut<Target = ConnectionCommon<impl SideData>>,
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

fn handshake(
    client: &mut ClientConfig,
    server: &mut ServerConfig,
) -> (ClientConnection, ServerConnection) {
    let mut c = ClientConnection::new(client, "testserver.com".try_into().unwrap()).unwrap();
    let mut s = ServerConnection::new(server).unwrap();
    while c.is_handshaking() || s.is_handshaking() || c.wants_write() || s.wants_write() {
        transfer(&mut c, &mut s);
        s.process_new_packets(server).unwrap();
        transfer(&mut s, &mut c);
        c.process_new_packets(client).unwrap();
    }
    (c, s)
}

/// What the server's side allocated taking one record of the largest plaintext: its reads of the
/// record, its processing and draining of it, and its read of the idle peer after.
struct Record {
    reads: Counts,
    process: Counts,
    idle: Counts,
}

fn one_record(
    c: &mut ClientConnection,
    s: &mut ServerConnection,
    server: &mut ServerConfig,
) -> Record {
    c.writer().write_all(&[0x5a; PLAINTEXT]).unwrap();
    let mut wire = [0u8; 2 * MAX_WIRE_SIZE];
    let mut n = 0;
    while c.wants_write() {
        n += c.write_tls(&mut &mut wire[n..]).unwrap();
    }

    alloc::begin();
    let mut offs = 0;
    while offs < n {
        offs += s.read_tls(&mut io::Cursor::new(&wire[offs..n])).unwrap();
    }
    let reads = alloc::end();

    alloc::begin();
    s.process_new_packets(server).unwrap();
    let mut plaintext = [0u8; PLAINTEXT];
    let mut got = 0;
    while got < PLAINTEXT {
        got += s.reader().read(&mut plaintext[got..]).unwrap();
    }
    assert_eq!(
        s.reader().read(&mut plaintext).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let process = alloc::end();
    assert!(plaintext.iter().all(|b| *b == 0x5a));

    alloc::begin();
    assert_eq!(
        s.read_tls(&mut Idle).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let idle = alloc::end();
    Record {
        reads,
        process,
        idle,
    }
}

/// The first record at its largest grows the buffer from one read to `MAX_WIRE_SIZE`, a read at a
/// time: 4,096 to 8,192, 12,288, 16,384 and 18,437 bytes, each its capacity exactly. The idle read
/// after it keeps it.
fn check_first(first: &Record, version: &SupportedProtocolVersion) {
    let steps = u64::try_from((MAX_WIRE_SIZE - READ).div_ceil(READ)).unwrap();
    let grown = i64::try_from(MAX_WIRE_SIZE - READ).unwrap();
    eprintln!(
        "{version:?} first record: reads {:?}, idle {:?}",
        first.reads, first.idle
    );
    assert_eq!(first.reads.allocations, 0, "{version:?}");
    assert_eq!(first.reads.reallocations, steps, "{version:?}");
    assert_eq!(first.reads.live, grown, "{version:?}");
    assert_eq!(
        first.idle,
        Counts::ZERO,
        "{version:?}: the idle read let the buffer go"
    );
}

/// A later record's reads and the idle read after it touch the buffer's memory not at all.
fn check_later(next: &Record, version: &SupportedProtocolVersion, record: usize) {
    assert_eq!(
        next.reads,
        Counts::ZERO,
        "{version:?} record {record}: its reads reallocated the buffer"
    );
    assert_eq!(
        next.idle,
        Counts::ZERO,
        "{version:?} record {record}: the idle read let the buffer go"
    );
    // Each record's plaintext is copied out of the buffer into a block of its own
    // (`CommonState::take_received_plaintext`), which draining it frees.
    assert_eq!(next.process.allocations, 1, "{version:?} record {record}");
    assert_eq!(next.process.reallocations, 0, "{version:?} record {record}");
    assert_eq!(next.process.live, 0, "{version:?} record {record}");
}

#[test]
fn a_connection_reading_records_and_idling_reallocates_its_read_buffer_only_to_grow_it_once() {
    assert!(alloc::installed());
    for version in [&version::TLS13, &version::TLS12] {
        let (mut client, mut server) = configs(version);
        let (mut c, mut s) = handshake(&mut client, &mut server);
        check_first(&one_record(&mut c, &mut s, &mut server), version);
        for record in 0..RECORDS {
            check_later(&one_record(&mut c, &mut s, &mut server), version, record);
        }
    }
}
