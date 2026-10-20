//! hyper-tls against OpenSSL with SecP384r1MLKEM1024 (draft-ietf-tls-ecdhe-mlkem), each side
//! offering that group alone, in both directions over TCP: the hybrid's layout (the P-384 share
//! first, the shared secret ECDH ‖ ML-KEM) is checked against an independent implementation.
//! OpenSSL 3.5 or later is needed, which CI's runners do not have, so the tests are run by hand and
//! their result recorded in VENDORED.md (`cargo test -p hyper-tls --test openssl_interop --
//! --ignored`).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods
)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::sync::LazyLock;

use hyper_tls::crypto::aws_lc_rs::{default_provider, kx_group};
use hyper_tls::crypto::CryptoProvider;
use hyper_tls::pki_types::pem::PemObject;
use hyper_tls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use hyper_tls::{
    ClientConfig, ClientConnection, NamedGroup, RootCertStore, ServerConfig, ServerConnection,
};

/// hyper-tls offering SecP384r1MLKEM1024 alone.
static PROVIDER: LazyLock<CryptoProvider> = LazyLock::new(|| CryptoProvider {
    kx_groups: vec![kx_group::SECP384R1MLKEM1024],
    ..default_provider()
});

const CA: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/test-ca/ecdsa-p384");

/// An OpenSSL that lists SecP384r1MLKEM1024 among its key encapsulations.
fn openssl() -> &'static str {
    [
        "/opt/homebrew/bin/openssl",
        "/usr/local/bin/openssl",
        "/usr/bin/openssl",
        "openssl",
    ]
    .into_iter()
    .find(|path| {
        Command::new(path)
            .args(["list", "-kem-algorithms"])
            .output()
            .is_ok_and(|o| String::from_utf8_lossy(&o.stdout).contains("SecP384r1MLKEM1024"))
    })
    .expect("no OpenSSL with SecP384r1MLKEM1024 (3.5 or later)")
}

fn file(name: &str) -> String {
    format!("{CA}/{name}")
}

#[test]
#[ignore = "needs OpenSSL 3.5 or later; run by hand, the result recorded in VENDORED.md"]
fn a_hyper_tls_client_agrees_secp384r1mlkem1024_with_openssl() {
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut server = Command::new(openssl())
        .args([
            "s_server",
            "-accept",
            &format!("127.0.0.1:{port}"),
            "-tls1_3",
            "-naccept",
            "1",
            "-www",
        ])
        .args(["-groups", "SecP384r1MLKEM1024"])
        .args([
            "-cert",
            &file("end.cert"),
            "-cert_chain",
            &file("end.chain"),
            "-key",
            &file("end.key"),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    // Waits on the fact: OpenSSL says ACCEPT once it listens.
    let mut lines = BufReader::new(server.stdout.take().unwrap());
    let mut line = String::new();
    while !line.starts_with("ACCEPT") {
        line.clear();
        assert!(
            lines.read_line(&mut line).unwrap() > 0,
            "s_server ended before listening"
        );
    }
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_file(file("ca.cert")).unwrap())
        .unwrap();
    let mut config = ClientConfig::builder_with_provider(&PROVIDER)
        .with_protocol_versions(&[&hyper_tls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let mut conn =
        ClientConnection::new(&mut config, ServerName::try_from("testserver.com").unwrap())
            .unwrap();
    let mut socket = TcpStream::connect(("127.0.0.1", port)).unwrap();
    while conn.is_handshaking() {
        conn.complete_io(&mut socket, &mut config).unwrap();
    }
    assert_eq!(
        conn.negotiated_key_exchange_group().map(|g| g.name()),
        Some(NamedGroup::secp384r1MLKEM1024)
    );
    conn.writer().write_all(b"GET / HTTP/1.0\r\n\r\n").unwrap();
    conn.complete_io(&mut socket, &mut config).unwrap();
    let mut page = Vec::new();
    while !String::from_utf8_lossy(&page).contains("HTTP/1.0 200") {
        conn.complete_io(&mut socket, &mut config).unwrap();
        let mut chunk = [0u8; 4096];
        match conn.reader().read(&mut chunk) {
            Ok(n) if n > 0 => page.extend_from_slice(&chunk[..n]),
            _ => {}
        }
    }
    let _ = server.kill();
    let _ = server.wait();
}

#[test]
#[ignore = "needs OpenSSL 3.5 or later; run by hand, the result recorded in VENDORED.md"]
fn openssl_agrees_secp384r1mlkem1024_with_a_hyper_tls_server() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let client = Command::new(openssl())
        .args([
            "s_client",
            "-connect",
            &format!("127.0.0.1:{port}"),
            "-tls1_3",
            "-brief",
        ])
        .args(["-groups", "SecP384r1MLKEM1024"])
        .args([
            "-CAfile",
            &file("ca.cert"),
            "-servername",
            "testserver.com",
            "-verify_return_error",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(file("end.fullchain"))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let key = PrivateKeyDer::from_pem_file(file("end.key")).unwrap();
    let mut config = ServerConfig::builder_with_provider(&PROVIDER)
        .with_protocol_versions(&[&hyper_tls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .unwrap();
    let (mut socket, _) = listener.accept().unwrap();
    let mut conn = ServerConnection::new(&config).unwrap();
    while conn.is_handshaking() {
        conn.complete_io(&mut socket, &mut config).unwrap();
    }
    assert_eq!(
        conn.negotiated_key_exchange_group().map(|g| g.name()),
        Some(NamedGroup::secp384r1MLKEM1024)
    );
    conn.send_close_notify();
    let _ = conn.complete_io(&mut socket, &mut config);
    drop(socket);
    let out = client.wait_with_output().unwrap();
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(said.contains("SecP384r1MLKEM1024"), "OpenSSL said: {said}");
    assert!(said.contains("Verification: OK"), "OpenSSL said: {said}");
}
