//! The S3 listener of real `mantle serve` processes (docs/design/node.md §4): a cell of three made
//! by `mantle init` on loopback, each node's listener serving plain HTTP/1.1, every request signed
//! with the operator's profile key. Asked: buckets, objects put and read whole and by range, heads,
//! deletes, ListObjectsV2, conditional writes and multipart uploads through every node's listener,
//! each read byte-identical to what was put; and what S3 refuses, refused as S3 refuses it: a
//! request whose body's length cannot be trusted answered 400 and its connection closed, a request
//! refused before `100 Continue` sending no body, a connection past the budget closed as it comes,
//! and a bad signature `403 SignatureDoesNotMatch`.
//!
//! A binary of its own (crates/mantle/tests/support/cell.rs's harness, support/s3.rs's client).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

#[allow(dead_code, unused_imports)]
#[path = "support/cell.rs"]
mod cell;
#[allow(dead_code)]
#[path = "../../node/tests/support/history.rs"]
mod history;
#[path = "../../range/tests/support/linear.rs"]
mod linear;
#[allow(dead_code)]
#[path = "../../node/tests/support/relay.rs"]
mod relay;
#[allow(dead_code)]
#[path = "support/s3.rs"]
mod s3;

use std::io::{Read, Write};
use std::net::TcpStream;

use cell::*;
use mantle_s3::checksum::{self, Algorithm};
use s3::{Client, Key};

/// A started cell of three, each node enlisted, and a client of each node's listener.
fn started() -> (Cell, Vec<u64>, Vec<Client>) {
    let mut cell = Cell::made(three_founders);
    let ids: Vec<u64> = cell.dirs.keys().copied().collect();
    for &id in &ids {
        cell.enlisted(id);
    }
    let out = cell.dirs[&ids[0]].parent().unwrap().to_path_buf();
    let key = Key::of(&out);
    let clients = ids
        .iter()
        .map(|id| {
            let (at, _) = cell.s3_bound[id];
            let (_, region, _) = &cell.s3[id];
            Client::new(at, key.clone(), region)
        })
        .collect();
    (cell, ids, clients)
}

/// An object's bytes: `len` of them, each a function of `n` and its place.
fn bytes(n: u64, len: usize) -> Vec<u8> {
    object_bytes(n, len)
}

/// S3's ETag of a single PUT: the hex MD5 of its bytes, quoted (05 §5.1).
fn etag(data: &[u8]) -> String {
    let md5 = checksum::checksum(Algorithm::Md5, data).unwrap();
    let hex: String = md5.bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!("\"{hex}\"")
}

fn md5(data: &[u8]) -> [u8; 16] {
    checksum::checksum(Algorithm::Md5, data)
        .unwrap()
        .bytes
        .try_into()
        .unwrap()
}

#[test]
fn a_cell_of_three_serves_signed_s3_requests_through_every_nodes_listener() {
    let (mut cell, ids, mut clients) = started();
    let n = clients.len();
    // A bucket made through one listener is the cell's: another lists it, a third heads it.
    let made = clients[0].send("PUT", "/photos", &[], b"");
    assert_eq!(made.status, 200, "{}", made.text());
    let listed = clients[1].send("GET", "/", &[], b"");
    assert_eq!(listed.status, 200, "{}", listed.text());
    assert_eq!(listed.element("Name").as_deref(), Some("photos"));
    assert_eq!(clients[2].send("HEAD", "/photos", &[], b"").status, 200);
    let again = clients[1].send("PUT", "/photos", &[], b"");
    assert_eq!(
        (again.status, again.element("Code").as_deref()),
        (409, Some("BucketAlreadyOwnedByYou")),
        "{}",
        again.text()
    );

    // Objects put through one listener alone: the listener hands each to its own node's host
    // in-process, and their chunks go to the other two nodes over QUIC's transfer class, each of
    // which holds exactly what it received and wrote none itself.
    let alone: Vec<Vec<u8>> = (0..2u64)
        .map(|i| bytes(50 + i, 1_000_003 + usize::try_from(i).unwrap()))
        .collect();
    for (i, data) in alone.iter().enumerate() {
        let target = format!("/photos/alone-{i}");
        let put = clients[0].send("PUT", &target, &[], data);
        assert_eq!(put.status, 200, "{}", put.text());
        assert!(clients[1].send("GET", &target, &[], b"").body == *data);
    }
    for &id in &ids[1..] {
        let (received, written, held) = cell.chunk_bytes(id);
        assert_eq!(
            written, 0,
            "node {id} put chunks itself, though its listener took no put"
        );
        assert!(received > 0, "node {id} received no chunk over QUIC");
        assert_eq!(
            received, held,
            "node {id}: {received} bytes received over QUIC, {held} held"
        );
    }

    // As many puts at once as the listener takes connections beside the one the first client
    // holds, past the operations the host runs at once (`gateway.objects`), as the AWS CLI's
    // transfers send their parts: each waits at the door until the host has room, and every one
    // commits.
    let (at, budget) = cell.s3_bound[&ids[0]];
    let at_once: Vec<(String, Vec<u8>)> = (0..budget - 1)
        .map(|i| {
            (
                format!("at-once/{i}"),
                bytes(70 + i, 200_001 + usize::try_from(i).unwrap()),
            )
        })
        .collect();
    let threads: Vec<_> = at_once
        .iter()
        .cloned()
        .map(|(key, data)| {
            let (key_pair, region) = (clients[0].key.clone(), clients[0].region.clone());
            std::thread::spawn(move || {
                let mut client = Client::new(at, key_pair, &region);
                let put = client.send("PUT", &format!("/photos/{key}"), &[], &data);
                (key, put.status, put.text())
            })
        })
        .collect();
    for thread in threads {
        let (key, status, text) = thread.join().unwrap();
        assert_eq!(status, 200, "{key}: {text}");
    }
    for (key, data) in &at_once {
        assert!(
            clients[2]
                .send("GET", &format!("/photos/{key}"), &[], b"")
                .body
                == *data
        );
    }

    // Objects put through each listener read back whole and by range through the others.
    let objects: Vec<(String, Vec<u8>)> = (0..n)
        .map(|i| {
            (
                format!("o{i}/{}", "k ü+".repeat(i + 1)),
                bytes(i as u64, 300_001 + i),
            )
        })
        .collect();
    for (i, (key, data)) in objects.iter().enumerate() {
        let target = format!(
            "/photos/{}",
            mantle_s3::sigv4::uri_encode(key.as_bytes(), false)
        );
        let put = clients[i].send("PUT", &target, &[], data);
        assert_eq!(put.status, 200, "{}", put.text());
        assert_eq!(put.header("etag"), Some(etag(data).as_str()));
        let reader = &mut clients[(i + 1) % n];
        let got = reader.send("GET", &target, &[], b"");
        assert_eq!(got.status, 200, "{}", got.text());
        assert!(
            got.body == *data,
            "{key} read back through another listener"
        );
        assert_eq!(got.header("etag"), Some(etag(data).as_str()));
        let ranged = clients[(i + 2) % n].send("GET", &target, &[("range", "bytes=100-4195")], b"");
        assert_eq!(ranged.status, 206, "{}", ranged.text());
        assert!(ranged.body == data[100..=4195], "{key}'s range");
        assert_eq!(
            ranged.header("content-range"),
            Some(format!("bytes 100-4195/{}", data.len()).as_str())
        );
        let head = clients[(i + 2) % n].send("HEAD", &target, &[], b"");
        assert_eq!(head.status, 200);
        assert_eq!(
            head.header("content-length"),
            Some(data.len().to_string().as_str())
        );
    }

    // ListObjectsV2 lists every key through every listener.
    for client in &mut clients {
        let page = client.send("GET", "/photos?list-type=2", &[], b"");
        assert_eq!(page.status, 200, "{}", page.text());
        assert_eq!(
            page.element("KeyCount"),
            Some((alone.len() + at_once.len() + n).to_string())
        );
        for (key, _) in &objects {
            assert!(page.text().contains(&format!("<Key>{key}</Key>")), "{key}");
        }
    }

    // Conditional writes: a key made only where none is, and replaced only at the ETag it has.
    let target = "/photos/conditional";
    let first = bytes(100, 1_000);
    let made = clients[0].send("PUT", target, &[("if-none-match", "*")], &first);
    assert_eq!(made.status, 200, "{}", made.text());
    let refused = clients[1].send("PUT", target, &[("if-none-match", "*")], &bytes(101, 10));
    assert_eq!(
        (refused.status, refused.element("Code").as_deref()),
        (412, Some("PreconditionFailed")),
        "{}",
        refused.text()
    );
    let wrong = clients[2].send("PUT", target, &[("if-match", "\"0123\"")], &bytes(102, 10));
    assert_eq!(wrong.status, 412, "{}", wrong.text());
    let second = bytes(103, 2_000);
    let replaced = clients[2].send("PUT", target, &[("if-match", &etag(&first))], &second);
    assert_eq!(replaced.status, 200, "{}", replaced.text());
    assert!(clients[0].send("GET", target, &[], b"").body == second);

    // A delete through one listener is every listener's.
    let deleted = clients[1].send("DELETE", target, &[], b"");
    assert_eq!(deleted.status, 204, "{}", deleted.text());
    for client in &mut clients {
        let gone = client.send("GET", target, &[], b"");
        assert_eq!(
            (gone.status, gone.element("Code").as_deref()),
            (404, Some("NoSuchKey")),
            "{}",
            gone.text()
        );
    }

    // A multipart upload whose parts go through different listeners, completed through a third.
    let target = "/photos/big";
    let created = clients[0].send("POST", &format!("{target}?uploads"), &[], b"");
    assert_eq!(created.status, 200, "{}", created.text());
    let upload = created.element("UploadId").unwrap();
    let parts = [bytes(200, 5 << 20), bytes(201, 777_777)];
    let mut tags = Vec::new();
    for (number, data) in parts.iter().enumerate() {
        let part = format!("{target}?partNumber={}&uploadId={upload}", number + 1);
        let sent = clients[number % n].send("PUT", &part, &[], data);
        assert_eq!(sent.status, 200, "{}", sent.text());
        assert_eq!(sent.header("etag"), Some(etag(data).as_str()));
        tags.push(sent.header("etag").unwrap().to_owned());
    }
    let listed = clients[1].send("GET", &format!("{target}?uploadId={upload}"), &[], b"");
    assert_eq!(listed.status, 200, "{}", listed.text());
    assert_eq!(listed.text().matches("<Part>").count(), 2);
    let document: String = tags
        .iter()
        .enumerate()
        .map(|(i, tag)| {
            format!(
                "<Part><PartNumber>{}</PartNumber><ETag>{tag}</ETag></Part>",
                i + 1
            )
        })
        .collect();
    let document = format!("<CompleteMultipartUpload>{document}</CompleteMultipartUpload>");
    let completed = clients[2].send(
        "POST",
        &format!("{target}?uploadId={upload}"),
        &[],
        document.as_bytes(),
    );
    assert_eq!(completed.status, 200, "{}", completed.text());
    let whole: Vec<u8> = parts.concat();
    let expected = mantle_s3::checksum::multipart_etag(&[md5(&parts[0]), md5(&parts[1])]).unwrap();
    assert_eq!(
        completed.element("ETag").map(|e| e.replace("&quot;", "\"")),
        Some(format!("\"{expected}\""))
    );
    for client in &mut clients {
        let got = client.send("GET", target, &[], b"");
        assert_eq!(got.status, 200);
        assert!(
            got.body == whole,
            "the completed object through {}",
            client.at
        );
        let tail = client.send("GET", target, &[("range", "bytes=-1000")], b"");
        assert_eq!(tail.status, 206);
        assert!(tail.body == whole[whole.len() - 1000..]);
    }

    // The listeners handed every operation over in-process; the chunks went to the nodes that
    // hold them over QUIC's transfer class, each byte received held once: on every node, what its
    // volumes hold is what its peers sent it and what it put on them itself.
    let mut received = 0;
    for &id in &ids {
        let (from_peers, own, held) = cell.chunk_bytes(id);
        assert_eq!(
            from_peers + own,
            held,
            "node {id}: {from_peers} bytes received over QUIC, {own} put in-process, {held} held"
        );
        received += from_peers;
    }
    assert!(received > 0, "no chunk went to a peer over QUIC");
}

#[test]
fn the_listener_refuses_what_s3_refuses_and_closes_what_it_cannot_trust() {
    let (cell, ids, mut clients) = started();

    // Past the connection budget a connection is closed as it comes. On a listener no client has
    // reached yet, each connection within the budget is served (a request answered on it), and
    // the next, asked the same, is closed with nothing said.
    let (at, budget) = cell.s3_bound[&ids[0]];
    let mut held = Vec::new();
    for _ in 0..budget {
        let mut client = Client::new(at, clients[0].key.clone(), &clients[0].region);
        assert_eq!(client.send("GET", "/", &[], b"").status, 200);
        held.push(client);
    }
    let past = Client::new(at, clients[0].key.clone(), &clients[0].region);
    let head = past.head("GET", "/", &[], mantle_s3::sigv4::EMPTY_SHA256);
    let mut stream = TcpStream::connect(at).unwrap();
    // A write to a connection the listener already closed may be refused: what matters is that
    // nothing comes back.
    let _ = stream.write_all(head.as_bytes());
    let mut said = Vec::new();
    let _ = stream.read_to_end(&mut said);
    assert!(
        said.is_empty(),
        "a connection past the budget of {budget} was served: {}",
        String::from_utf8_lossy(&said)
    );
    drop(held);

    // The rest through the other listeners, whose connections the budget above did not count.
    let made = clients[1].send("PUT", "/b", &[], b"");
    assert_eq!(made.status, 200, "{}", made.text());

    // Transfer-Encoding beside Content-Length: 400, and the connection closed (RFC 9112 §6.1).
    let at = clients[2].at;
    let mut smuggled = TcpStream::connect(at).unwrap();
    smuggled
        .write_all(
            format!(
                "PUT /b/k HTTP/1.1\r\nhost: {at}\r\ncontent-length: 5\r\ntransfer-encoding: chunked\r\n\r\n0\r\n\r\n"
            )
            .as_bytes(),
        )
        .unwrap();
    let mut said = Vec::new();
    smuggled.read_to_end(&mut said).unwrap();
    let said = String::from_utf8_lossy(&said);
    assert!(said.starts_with("HTTP/1.1 400 "), "{said}");
    assert!(
        said.to_ascii_lowercase().contains("connection: close"),
        "{said}"
    );

    // A put refused before `100 Continue`: its final status comes in its place, and no octet of its
    // body is sent (RFC 9110 §10.1.1). Refused by the signature, and by the host: no such bucket.
    let data = bytes(7, 64 << 10);
    let payload = mantle_s3::sigv4::hex(&mantle_s3::crypto::sha256(&data).unwrap());
    let length = data.len().to_string();
    let expect = [
        ("content-length", length.as_str()),
        ("expect", "100-continue"),
    ];
    for (client, target, wrong, code, status) in [
        (2usize, "/b/refused", true, "SignatureDoesNotMatch", 403u16),
        (1, "/none/k", false, "NoSuchBucket", 404),
    ] {
        let mut asker = Client::new(
            clients[client].at,
            clients[client].key.clone(),
            &clients[client].region,
        );
        if wrong {
            asker.key.secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into();
        }
        let head = asker.head("PUT", target, &expect, &payload);
        asker.stream().write_all(head.as_bytes()).unwrap();
        let answer = asker.read_response(false);
        assert_eq!(
            (answer.status, answer.element("Code").as_deref()),
            (status, Some(code)),
            "{target}: {}",
            answer.text()
        );
        assert!(
            answer.closes(),
            "{target}: a refusal with its body unread closes"
        );
        assert!(
            asker.closed(),
            "{target}: the listener closed the connection"
        );
    }

    // A bad signature on a request without a body: 403 SignatureDoesNotMatch, the connection kept.
    let mut forger = Client::new(clients[1].at, clients[1].key.clone(), &clients[1].region);
    forger.key.secret.push('x');
    let forged = forger.send("GET", "/b?list-type=2", &[], b"");
    assert_eq!(
        (forged.status, forged.element("Code").as_deref()),
        (403, Some("SignatureDoesNotMatch")),
        "{}",
        forged.text()
    );
    assert!(!forged.closes());
}
