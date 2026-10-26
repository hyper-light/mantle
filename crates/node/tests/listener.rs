//! The S3 listener's TLS (docs/design/node.md §4.1; research/71): a listener bound on a real
//! socket with the endpoint's P-256 chain as `mantle init` makes it, reached by a stock client's
//! TLS (hyper-tls, rustls's webpki verifier against the endpoint authority) over TLS 1.3 and over
//! TLS 1.2. Asked: each handshake verifies the endpoint's certificate for the name the client
//! dials, takes HTTP/1.1 by ALPN where the client offers HTTP/2 beside it, and on TLS 1.2 an
//! ECDHE-ECDSA AES-GCM suite; and a request is read and answered over it, one signed with another
//! secret, which the listener refuses itself, `403 SignatureDoesNotMatch`, so no node stands
//! behind it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

#[allow(dead_code)]
#[path = "../../mantle/tests/support/s3.rs"]
mod s3;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::task::Waker;

use hyper_rt::combine::{Either, race2};
use hyper_rt::machine::facts::Facts;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_tls::pki_types::{CertificateDer, ServerName};
use hyper_tls::{
    ClientConfig, ClientConnection, ProtocolVersion, RootCertStore, StreamOwned,
    SupportedProtocolVersion, version,
};
use mantle_cert::{EndpointKey, EndpointName, Serial, Validity};
use mantle_node::door;
use mantle_node::listener::{CONNECTIONS, Listener, Settings};
use mantle_node::process::AccessKey;

/// The listener's shard: the root task and a task for each connection it holds, a socket for it
/// and each, read and written.
fn runtime() -> LocalRuntime {
    let connections = usize::try_from(CONNECTIONS).unwrap();
    LocalRuntime::new(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: connections + 1,
        timers_per_shard: 1,
        interests_per_shard: interests_for(connections + 1),
        ring_entries: 1,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 1,
        pin: false,
        cores: Vec::new(),
        page_bytes: usize::try_from(Facts::query().page.base).unwrap(),
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    })
    .unwrap()
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// What one handshake and request came to: the version and suite negotiated, the protocol ALPN
/// chose, and the response's status and body.
#[derive(Debug)]
struct Said {
    version: Option<ProtocolVersion>,
    suite: String,
    alpn: Option<Vec<u8>>,
    status: u16,
    body: String,
}

#[test]
fn the_listener_answers_over_tls_1_3_and_1_2_with_the_endpoint_chain() {
    let authority_key = EndpointKey::generate().unwrap();
    let validity = Validity {
        not_before: now() - 3_600,
        not_after: now() + 86_400,
    };
    let name = "mantle s3 endpoint authority";
    let authority =
        mantle_cert::endpoint_authority(&authority_key, name, validity, Serial::draw().unwrap())
            .unwrap();
    let key = EndpointKey::generate().unwrap();
    let leaf = mantle_cert::endpoint(
        &authority_key,
        name,
        key.public_key(),
        &[
            EndpointName::Dns("localhost"),
            EndpointName::Address("127.0.0.1".parse().unwrap()),
        ],
        (validity, Serial::draw().unwrap()),
    )
    .unwrap();
    let access = AccessKey {
        id: "AKIAIOSFODNN7EXAMPLE".into(),
        account: "123456789012".into(),
        secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
    };
    let pkcs8 = key.pkcs8().to_vec();
    let asked = access.clone();
    let said = runtime()
        .block_on(async move {
            // No node takes what the door holds: every request here is refused before it.
            let (_doorway, submitter) = door::doorway(1, Waker::noop().clone()).unwrap();
            let settings = Settings {
                listen: "127.0.0.1:0".parse().unwrap(),
                region: "us-east-1".into(),
                domains: Vec::new(),
                access: vec![access],
                tls: Some((leaf, pkcs8)),
                node: 1,
            };
            let listener = Listener::bind(settings, submitter).unwrap();
            let at = listener.local_addr().unwrap();
            let (told, mut heard) = hyper_rt::sync::channel::<Vec<Said>>(1).unwrap();
            let client = std::thread::spawn(move || {
                let said = [&version::TLS13, &version::TLS12]
                    .into_iter()
                    .map(|version| ask(at, &authority, version, &asked))
                    .collect();
                told.blocking_send(said).unwrap();
            });
            let said = match race2(listener.serve(), heard.recv()).await {
                Either::Second(Ok(said)) => said,
                Either::Second(Err(e)) => panic!("the client: {e:?}"),
                Either::First(e) => panic!("the listener ended: {e}"),
            };
            client.join().unwrap();
            said
        })
        .unwrap();
    let [tls13, tls12] = said.as_slice() else {
        panic!("{said:?}")
    };
    assert_eq!(tls13.version, Some(ProtocolVersion::TLSv1_3), "{tls13:?}");
    assert_eq!(tls12.version, Some(ProtocolVersion::TLSv1_2), "{tls12:?}");
    assert!(
        tls12.suite.contains("ECDHE_ECDSA") && tls12.suite.contains("GCM"),
        "{tls12:?}"
    );
    for said in [tls13, tls12] {
        assert_eq!(said.alpn.as_deref(), Some(&b"http/1.1"[..]), "{said:?}");
        assert_eq!(said.status, 403, "{said:?}");
        assert!(
            said.body.contains("<Code>SignatureDoesNotMatch</Code>"),
            "{said:?}"
        );
    }
}

/// Asks the listener at `at` over `version` of TLS, its certificate verified under `authority`
/// for `localhost`, a request signed with a secret other than `key`'s.
fn ask(
    at: SocketAddr,
    authority: &[u8],
    version: &'static SupportedProtocolVersion,
    key: &AccessKey,
) -> Said {
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from(authority.to_vec())).unwrap();
    let mut config =
        ClientConfig::builder_with_provider(&hyper_tls::crypto::aws_lc_rs::DEFAULT_PROVIDER)
            .with_protocol_versions(&[version])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let connection =
        ClientConnection::new(&mut config, ServerName::try_from("localhost").unwrap()).unwrap();
    let mut tls = StreamOwned::new(connection, config, TcpStream::connect(at).unwrap());
    let wrong = s3::Key {
        id: key.id.clone(),
        secret: format!("{}x", key.secret),
    };
    let head = s3::Client::new(at, wrong, "us-east-1").head(
        "GET",
        "/",
        &[],
        mantle_s3::sigv4::EMPTY_SHA256,
    );
    tls.write_all(head.as_bytes()).unwrap();
    let mut read = Vec::new();
    let mut buf = [0u8; 4096];
    let end = loop {
        if let Some(end) = read.windows(4).position(|w| w == b"\r\n\r\n") {
            break end + 4;
        }
        let n = tls.read(&mut buf).unwrap();
        assert!(n > 0, "the listener ended the response early");
        read.extend_from_slice(&buf[..n]);
    };
    let head = String::from_utf8(read[..end].to_vec()).unwrap();
    let length: usize = head
        .lines()
        .find_map(|l| {
            let (name, value) = l.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().unwrap())
        })
        .unwrap();
    while read.len() < end + length {
        let n = tls.read(&mut buf).unwrap();
        assert!(n > 0, "the listener ended the body early");
        read.extend_from_slice(&buf[..n]);
    }
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    Said {
        version: tls.conn.protocol_version(),
        suite: format!(
            "{:?}",
            tls.conn.negotiated_cipher_suite().map(|s| s.suite())
        ),
        alpn: tls.conn.alpn_protocol().map(<[u8]>::to_vec),
        status,
        body: String::from_utf8_lossy(&read[end..end + length]).into_owned(),
    }
}
