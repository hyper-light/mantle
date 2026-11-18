//! The S3 listener of real `mantle serve` processes (docs/design/node.md §4): a cell of three made
//! by `mantle init` on loopback, each node's listener serving plain HTTP/1.1, every request signed
//! with the operator's profile key. Asked: buckets, objects put and read whole and by range, heads,
//! deletes, ListObjectsV2, conditional writes and multipart uploads through every node's listener,
//! each read byte-identical to what was put; and what S3 refuses, refused as S3 refuses it: a
//! request whose body's length cannot be trusted answered 400 and its connection closed, a request
//! refused before `100 Continue` sending no body, a connection past the budget answered `503
//! SlowDown` and closed, and a bad signature `403 SignatureDoesNotMatch`.
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
#[allow(dead_code)]
#[path = "support/s3_watched.rs"]
mod s3_watched;

use std::io::{Read, Write};

use cell::*;
use mantle_s3::checksum::{self, Algorithm};
use s3::{Client, Key};

/// A started cell of three, each node enlisted, and a client of each node's listener.
fn started() -> (Cell, Vec<u64>, Vec<Client>) {
    // Each founder at four gibibytes, so its listener serves as many connections as a test holds
    // (`the_listener_refuses_what_s3_refuses_and_closes_what_it_cannot_trust`).
    let mut cell = Cell::made(|root| founders_within(root, Some("4G")));
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
    let made = clients[0].request(&mut cell, "PUT", "/photos", &[], b"");
    assert_eq!(made.status, 200, "{}", made.text());
    let listed = clients[1].request(&mut cell, "GET", "/", &[], b"");
    assert_eq!(listed.status, 200, "{}", listed.text());
    assert_eq!(listed.element("Name").as_deref(), Some("photos"));
    assert_eq!(
        clients[2]
            .request(&mut cell, "HEAD", "/photos", &[], b"")
            .status,
        200
    );
    let again = clients[1].request(&mut cell, "PUT", "/photos", &[], b"");
    assert_eq!(
        (again.status, again.element("Code").as_deref()),
        (409, Some("BucketAlreadyOwnedByYou")),
        "{}",
        again.text()
    );

    // Objects put through one listener alone: the listener hands each to its own node's host
    // in-process, and their chunks go to the other two nodes over QUIC's transfer class, each of
    // which holds exactly what it received and kept and wrote none itself.
    let alone: Vec<Vec<u8>> = (0..2u64)
        .map(|i| bytes(50 + i, 1_000_003 + usize::try_from(i).unwrap()))
        .collect();
    for (i, data) in alone.iter().enumerate() {
        let target = format!("/photos/alone-{i}");
        let put = clients[0].request(&mut cell, "PUT", &target, &[], data);
        assert_eq!(put.status, 200, "{}", put.text());
        assert!(clients[1].request(&mut cell, "GET", &target, &[], b"").body == *data);
    }
    for &id in &ids[1..] {
        let (received, written, held, unkept) = cell.chunk_bytes(id);
        assert_eq!(
            written, 0,
            "node {id} put chunks itself, though its listener took no put"
        );
        assert!(received > 0, "node {id} received no chunk over QUIC");
        assert_eq!(
            received,
            held + unkept,
            "node {id}: {received} bytes received over QUIC, {held} held, {unkept} kept by none"
        );
    }

    // As many puts at once as a stock client's transfers send parts, the AWS CLI's ten, within the
    // connections the listener takes beside the one the first client holds: any past the
    // operations the host runs now waits at the door until it has room, and every one commits.
    let (at, budget) = cell.s3_bound[&ids[0]];
    let at_once: Vec<(String, Vec<u8>)> = (0..mantle_node::listener::REFUSING.min(budget - 1))
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
            let mut client = Client::new(at, clients[0].key.clone(), &clients[0].region);
            let what = format!(
                "PUT /photos/{key} at once through node {}'s listener",
                ids[0]
            );
            let thread = std::thread::spawn(move || {
                let put = client.send("PUT", &format!("/photos/{key}"), &[], &data);
                (key, put.status, put.text())
            });
            (what, thread)
        })
        .collect();
    for (what, thread) in threads {
        let (key, status, text) = cell.watched(&what, thread);
        assert_eq!(status, 200, "{key}: {text}");
    }
    for (key, data) in &at_once {
        assert!(
            clients[2]
                .request(&mut cell, "GET", &format!("/photos/{key}"), &[], b"")
                .body
                == *data
        );
    }

    // A body framed `aws-chunked` with its checksum in a trailer, as the AWS CLI's CRT transfers and
    // the SDKs' streaming uploads send one (05 §1.9): the object commits with the trailer's
    // CRC-32 and reads back whole; a trailer whose value is another's is refused before anything
    // commits.
    let trailed = bytes(90, 150_001);
    let crc = checksum::checksum(Algorithm::Crc32, &trailed)
        .unwrap()
        .to_base64();
    let wrong = checksum::checksum(Algorithm::Crc32, b"another body")
        .unwrap()
        .to_base64();
    let decoded = trailed.len().to_string();
    let chunked = [
        ("content-encoding", "aws-chunked"),
        ("x-amz-decoded-content-length", decoded.as_str()),
        ("x-amz-trailer", "x-amz-checksum-crc32"),
    ];
    let streaming = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
    let body = s3::aws_chunked(&trailed, 64 << 10, ("x-amz-checksum-crc32", &crc));
    let put = clients[1].request_as(
        &mut cell,
        "PUT",
        "/photos/trailed",
        &chunked,
        &body,
        streaming,
    );
    assert_eq!(put.status, 200, "{}", put.text());
    assert_eq!(put.header("x-amz-checksum-crc32"), Some(crc.as_str()));
    assert!(
        clients[2]
            .request(&mut cell, "GET", "/photos/trailed", &[], b"")
            .body
            == trailed
    );
    let body = s3::aws_chunked(&trailed, 64 << 10, ("x-amz-checksum-crc32", &wrong));
    let refused = clients[1].request_as(
        &mut cell,
        "PUT",
        "/photos/mistrailed",
        &chunked,
        &body,
        streaming,
    );
    assert_eq!(
        (refused.status, refused.element("Code").as_deref()),
        (400, Some("BadDigest")),
        "{}",
        refused.text()
    );
    assert_eq!(
        clients[2]
            .request(&mut cell, "HEAD", "/photos/mistrailed", &[], b"")
            .status,
        404
    );

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
        let put = clients[i].request(&mut cell, "PUT", &target, &[], data);
        assert_eq!(put.status, 200, "{}", put.text());
        assert_eq!(put.header("etag"), Some(etag(data).as_str()));
        let reader = &mut clients[(i + 1) % n];
        let got = reader.request(&mut cell, "GET", &target, &[], b"");
        assert_eq!(got.status, 200, "{}", got.text());
        assert!(
            got.body == *data,
            "{key} read back through another listener"
        );
        assert_eq!(got.header("etag"), Some(etag(data).as_str()));
        let ranged = clients[(i + 2) % n].request(
            &mut cell,
            "GET",
            &target,
            &[("range", "bytes=100-4195")],
            b"",
        );
        assert_eq!(ranged.status, 206, "{}", ranged.text());
        assert!(ranged.body == data[100..=4195], "{key}'s range");
        assert_eq!(
            ranged.header("content-range"),
            Some(format!("bytes 100-4195/{}", data.len()).as_str())
        );
        let head = clients[(i + 2) % n].request(&mut cell, "HEAD", &target, &[], b"");
        assert_eq!(head.status, 200);
        assert_eq!(
            head.header("content-length"),
            Some(data.len().to_string().as_str())
        );
    }

    // ListObjectsV2 lists every key through every listener.
    for client in &mut clients {
        let page = client.request(&mut cell, "GET", "/photos?list-type=2", &[], b"");
        assert_eq!(page.status, 200, "{}", page.text());
        assert_eq!(
            page.element("KeyCount"),
            Some((alone.len() + at_once.len() + 1 + n).to_string())
        );
        for (key, _) in &objects {
            assert!(page.text().contains(&format!("<Key>{key}</Key>")), "{key}");
        }
    }

    // Conditional writes: a key made only where none is, and replaced only at the ETag it has.
    let target = "/photos/conditional";
    let first = bytes(100, 1_000);
    let made = clients[0].request(&mut cell, "PUT", target, &[("if-none-match", "*")], &first);
    assert_eq!(made.status, 200, "{}", made.text());
    let refused = clients[1].request(
        &mut cell,
        "PUT",
        target,
        &[("if-none-match", "*")],
        &bytes(101, 10),
    );
    assert_eq!(
        (refused.status, refused.element("Code").as_deref()),
        (412, Some("PreconditionFailed")),
        "{}",
        refused.text()
    );
    let wrong = clients[2].request(
        &mut cell,
        "PUT",
        target,
        &[("if-match", "\"0123\"")],
        &bytes(102, 10),
    );
    assert_eq!(wrong.status, 412, "{}", wrong.text());
    let second = bytes(103, 2_000);
    let replaced = clients[2].request(
        &mut cell,
        "PUT",
        target,
        &[("if-match", &etag(&first))],
        &second,
    );
    assert_eq!(replaced.status, 200, "{}", replaced.text());
    assert!(clients[0].request(&mut cell, "GET", target, &[], b"").body == second);

    // A delete through one listener is every listener's.
    let deleted = clients[1].request(&mut cell, "DELETE", target, &[], b"");
    assert_eq!(deleted.status, 204, "{}", deleted.text());
    for client in &mut clients {
        let gone = client.request(&mut cell, "GET", target, &[], b"");
        assert_eq!(
            (gone.status, gone.element("Code").as_deref()),
            (404, Some("NoSuchKey")),
            "{}",
            gone.text()
        );
    }

    // A multipart upload whose parts go through different listeners, completed through a third.
    let target = "/photos/big";
    let created = clients[0].request(&mut cell, "POST", &format!("{target}?uploads"), &[], b"");
    assert_eq!(created.status, 200, "{}", created.text());
    let upload = created.element("UploadId").unwrap();
    let parts = [bytes(200, 5 << 20), bytes(201, 777_777)];
    let mut tags = Vec::new();
    for (number, data) in parts.iter().enumerate() {
        let part = format!("{target}?partNumber={}&uploadId={upload}", number + 1);
        let sent = clients[number % n].request(&mut cell, "PUT", &part, &[], data);
        assert_eq!(sent.status, 200, "{}", sent.text());
        assert_eq!(sent.header("etag"), Some(etag(data).as_str()));
        tags.push(sent.header("etag").unwrap().to_owned());
    }
    let listed = clients[1].request(
        &mut cell,
        "GET",
        &format!("{target}?uploadId={upload}"),
        &[],
        b"",
    );
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
    let completed = clients[2].request(
        &mut cell,
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
        let got = client.request(&mut cell, "GET", target, &[], b"");
        assert_eq!(got.status, 200);
        assert!(
            got.body == whole,
            "the completed object through {}",
            client.at
        );
        let tail = client.request(&mut cell, "GET", target, &[("range", "bytes=-1000")], b"");
        assert_eq!(tail.status, 206);
        assert!(tail.body == whole[whole.len() - 1000..]);
    }

    // The listeners handed every operation over in-process; the chunks went to the nodes that
    // hold them over QUIC's transfer class, each byte received held once or kept by none: on every
    // node, what its volumes hold and what none kept is what its peers sent it and what it put on
    // them itself.
    let mut received = 0;
    for &id in &ids {
        let (from_peers, own, held, unkept) = cell.chunk_bytes(id);
        assert_eq!(
            from_peers + own,
            held + unkept,
            "node {id}: {from_peers} bytes received over QUIC, {own} put in-process, {held} held, \
             {unkept} kept by none"
        );
        received += from_peers;
    }
    assert!(received > 0, "no chunk went to a peer over QUIC");
}

#[test]
fn the_listener_refuses_what_s3_refuses_and_closes_what_it_cannot_trust() {
    let (mut cell, ids, mut clients) = started();

    // Past the connection budget a connection is answered `503 SlowDown` with `Connection:
    // close`, never closed with no answer, which the AWS CLI takes for a part done (research/72).
    // On a listener no client has reached yet, each connection within the budget is served (a
    // request answered on it), and the next, asked the same, is answered refused.
    let (at, budget) = cell.s3_bound[&ids[0]];
    let mut held = Vec::new();
    for _ in 0..budget {
        let mut client = Client::new(at, clients[0].key.clone(), &clients[0].region);
        assert_eq!(client.request(&mut cell, "GET", "/", &[], b"").status, 200);
        held.push(client);
    }
    let mut past = Client::new(at, clients[0].key.clone(), &clients[0].region);
    let refused = past.request(&mut cell, "GET", "/", &[], b"");
    assert_eq!(
        refused.status,
        503,
        "a connection past the budget of {budget}: {}",
        refused.text()
    );
    assert_eq!(
        refused.element("Code").as_deref(),
        Some("SlowDown"),
        "{}",
        refused.text()
    );
    assert!(refused.closes(), "{}", refused.text());
    drop(held);

    // The rest through the other listeners, whose connections the budget above did not count.
    let made = clients[1].request(&mut cell, "PUT", "/b", &[], b"");
    assert_eq!(made.status, 200, "{}", made.text());

    // Transfer-Encoding beside Content-Length: 400, and the connection closed (RFC 9112 §6.1).
    let mut smuggler = Client::new(clients[2].at, clients[2].key.clone(), &clients[2].region);
    let said = smuggler.watched(
        &mut cell,
        "PUT /b/k with Transfer-Encoding beside Content-Length, read to its close",
        |smuggler| {
            let at = smuggler.at;
            let smuggled = smuggler.stream();
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
            said
        },
    );
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
        let what = format!("PUT {target} refused before 100 Continue, read to its close");
        let (answer, closed) = asker.watched(&mut cell, &what, move |asker| {
            asker.stream().write_all(head.as_bytes()).unwrap();
            let answer = asker.read_response(false);
            (answer, asker.closed())
        });
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
        assert!(closed, "{target}: the listener closed the connection");
    }

    // A bad signature on a request without a body: 403 SignatureDoesNotMatch, the connection kept.
    let mut forger = Client::new(clients[1].at, clients[1].key.clone(), &clients[1].region);
    forger.key.secret.push('x');
    let forged = forger.request(&mut cell, "GET", "/b?list-type=2", &[], b"");
    assert_eq!(
        (forged.status, forged.element("Code").as_deref()),
        (403, Some("SignatureDoesNotMatch")),
        "{}",
        forged.text()
    );
    assert!(!forged.closes());
}

/// The XML text `element` holds, its entities read.
fn unescaped(text: Option<String>) -> Option<String> {
    text.map(|t| {
        t.replace("&quot;", "\"")
            .replace("&apos;", "'")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&amp;", "&")
    })
}

/// Every `<element>` of a document, in order.
fn elements(text: &str, element: &str) -> Vec<String> {
    let (open, close) = (format!("<{element}>"), format!("</{element}>"));
    let mut found = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find(&open) {
        let after = rest.get(start + open.len()..).unwrap();
        let end = after.find(&close).unwrap();
        found.push(after.get(..end).unwrap().to_owned());
        rest = after.get(end + close.len()..).unwrap();
    }
    found
}

/// A DeleteObjects document of `objects`, each its key and version, and its `Content-MD5`.
fn delete_document(objects: &[(&str, Option<&str>)], quiet: bool) -> (String, String) {
    let listed: String = objects
        .iter()
        .map(|(key, version)| match version {
            Some(v) => format!("<Object><Key>{key}</Key><VersionId>{v}</VersionId></Object>"),
            None => format!("<Object><Key>{key}</Key></Object>"),
        })
        .collect();
    let quiet = if quiet { "<Quiet>true</Quiet>" } else { "" };
    let document = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{listed}{quiet}</Delete>"
    );
    let md5 = checksum::checksum(Algorithm::Md5, document.as_bytes())
        .unwrap()
        .to_base64();
    (document, md5)
}

/// What research/89 builds, through a cell of three's listeners, each answered as S3 answers it:
/// an object's content headers and user-defined metadata kept at PutObject and
/// CreateMultipartUpload and returned on GET and HEAD, overridden by a read's `response-*`, held to
/// S3's 2 KB; versions read, headed and removed by ID and listed; CopyObject within and across
/// buckets, of a version, under each directive and the source's conditions, onto itself; part
/// copies of a range and whole; DeleteObjects of keys present, missing and versioned, quiet, and
/// refused without its integrity header. Every read back byte-identical.
#[test]
fn metadata_versions_copies_and_batch_deletes_answer_as_s3_does() {
    let (mut cell, _ids, mut clients) = started();
    let n = clients.len();
    assert_eq!(
        clients[0]
            .request(&mut cell, "PUT", "/meta", &[], b"")
            .status,
        200
    );
    assert_eq!(
        clients[1]
            .request(&mut cell, "PUT", "/other", &[], b"")
            .status,
        200
    );

    // Metadata kept at PutObject, returned as S3 returns it: names lowercase, a name sent twice
    // joined, a value past US-ASCII in RFC 2047's form, an empty one kept (research/89 §1).
    let data = bytes(300, 70_001);
    let sent = [
        (
            "content-type",
            "application/vnd.oci.image.layer.v1.tar+gzip",
        ),
        ("content-encoding", "gzip"),
        (
            "content-disposition",
            "attachment; filename=\"layer.tar.gz\"",
        ),
        ("content-language", "en-US"),
        ("cache-control", "max-age=3600, immutable"),
        ("expires", "Wed, 01 Dec 2094 16:00:00 GMT"),
        ("x-amz-meta-Digest", "sha256:0123"),
        ("x-amz-meta-tag", "first"),
        ("X-Amz-Meta-Tag", "second"),
        ("x-amz-meta-empty", ""),
        ("x-amz-meta-nonascii", "ÄMÄZÕÑ S3"),
    ];
    let put = clients[0].request(&mut cell, "PUT", "/meta/layer", &sent, &data);
    assert_eq!(put.status, 200, "{}", put.text());
    let expected = [
        (
            "content-type",
            "application/vnd.oci.image.layer.v1.tar+gzip",
        ),
        ("content-encoding", "gzip"),
        (
            "content-disposition",
            "attachment; filename=\"layer.tar.gz\"",
        ),
        ("content-language", "en-US"),
        ("cache-control", "max-age=3600, immutable"),
        ("expires", "Wed, 01 Dec 2094 16:00:00 GMT"),
        ("x-amz-meta-digest", "sha256:0123"),
        ("x-amz-meta-tag", "first,second"),
        ("x-amz-meta-empty", ""),
        (
            "x-amz-meta-nonascii",
            "=?UTF-8?B?w4PChE3Dg8KEWsODwpXDg8KRIFMz?=",
        ),
    ];
    for (i, method) in ["GET", "HEAD"].into_iter().enumerate() {
        let read = clients[(i + 1) % n].request(&mut cell, method, "/meta/layer", &[], b"");
        assert_eq!(read.status, 200, "{method}: {}", read.text());
        for (name, value) in expected {
            assert_eq!(read.header(name), Some(value), "{method} {name}");
        }
        if method == "GET" {
            assert!(read.body == data, "the object read back");
        }
    }
    // A read's overrides, on a successful read alone (research/89 §1.5).
    let overridden = clients[2].request(&mut cell,
        "GET",
        "/meta/layer?response-content-type=text%2Fplain&response-cache-control=no-store&response-content-disposition=inline",
        &[],
        b"",
    );
    assert_eq!(overridden.status, 200, "{}", overridden.text());
    assert_eq!(overridden.header("content-type"), Some("text/plain"));
    assert_eq!(overridden.header("cache-control"), Some("no-store"));
    assert_eq!(overridden.header("content-disposition"), Some("inline"));
    assert_eq!(overridden.header("content-encoding"), Some("gzip"));
    // An object put with none is served as S3's binary/octet-stream; a later PUT's metadata
    // replaces an earlier one's.
    let plain = clients[0].request(
        &mut cell,
        "PUT",
        "/meta/plain",
        &[("x-amz-meta-a", "1")],
        b"abc",
    );
    assert_eq!(plain.status, 200);
    let replaced = clients[1].request(&mut cell, "PUT", "/meta/plain", &[], b"abcd");
    assert_eq!(replaced.status, 200);
    let head = clients[2].request(&mut cell, "HEAD", "/meta/plain", &[], b"");
    assert_eq!(head.header("content-type"), Some("binary/octet-stream"));
    assert_eq!(head.header("x-amz-meta-a"), None);
    // S3's 2 KB of user-defined metadata, a key counted past its prefix: at it kept, past it
    // refused, nothing written.
    let at_bound = "v".repeat(2048 - 1);
    let fits = clients[0].request(
        &mut cell,
        "PUT",
        "/meta/bound",
        &[("x-amz-meta-k", &at_bound)],
        b"x",
    );
    assert_eq!(fits.status, 200, "{}", fits.text());
    let past = "v".repeat(2048);
    let refused = clients[0].request(
        &mut cell,
        "PUT",
        "/meta/past",
        &[("x-amz-meta-k", &past)],
        b"x",
    );
    assert_eq!(
        (refused.status, refused.element("Code").as_deref()),
        (400, Some("MetadataTooLarge")),
        "{}",
        refused.text()
    );
    assert_eq!(
        clients[1]
            .request(&mut cell, "HEAD", "/meta/past", &[], b"")
            .status,
        404
    );
    // A streamed body's `aws-chunked` is no coding of the object (research/05 §1.8).
    let streamed = bytes(301, 90_001);
    let crc = checksum::checksum(Algorithm::Crc32, &streamed)
        .unwrap()
        .to_base64();
    let decoded = streamed.len().to_string();
    let chunked = [
        ("content-encoding", "aws-chunked,gzip"),
        ("x-amz-decoded-content-length", decoded.as_str()),
        ("x-amz-trailer", "x-amz-checksum-crc32"),
    ];
    let body = s3::aws_chunked(&streamed, 64 << 10, ("x-amz-checksum-crc32", &crc));
    let put = clients[1].request_as(
        &mut cell,
        "PUT",
        "/meta/streamed",
        &chunked,
        &body,
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
    );
    assert_eq!(put.status, 200, "{}", put.text());
    let got = clients[2].request(&mut cell, "GET", "/meta/streamed", &[], b"");
    assert_eq!(got.header("content-encoding"), Some("gzip"));
    assert!(got.body == streamed);
    // A multipart upload's metadata, named at its creation, is its completed object's.
    let created = clients[0].request(
        &mut cell,
        "POST",
        "/meta/parts?uploads",
        &[
            ("content-type", "text/csv"),
            ("x-amz-meta-origin", "upload"),
        ],
        b"",
    );
    assert_eq!(created.status, 200, "{}", created.text());
    let upload = created.element("UploadId").unwrap();
    let part = bytes(302, 1_234);
    let sent = clients[1].request(
        &mut cell,
        "PUT",
        &format!("/meta/parts?partNumber=1&uploadId={upload}"),
        &[],
        &part,
    );
    assert_eq!(sent.status, 200, "{}", sent.text());
    let document = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        sent.header("etag").unwrap()
    );
    let completed = clients[2].request(
        &mut cell,
        "POST",
        &format!("/meta/parts?uploadId={upload}"),
        &[],
        document.as_bytes(),
    );
    assert_eq!(completed.status, 200, "{}", completed.text());
    let head = clients[0].request(&mut cell, "HEAD", "/meta/parts", &[], b"");
    assert_eq!(head.header("content-type"), Some("text/csv"));
    assert_eq!(head.header("x-amz-meta-origin"), Some("upload"));

    // Tags (research/89 §3.8): a version's tag set is the empty one, which stock clients ask of
    // every source they copy in parts; a missing key's is 404; a write naming tags is refused, not
    // dropped, nothing written, and one naming the empty set is taken.
    let tags = clients[1].request(&mut cell, "GET", "/meta/layer?tagging", &[], b"");
    assert_eq!(tags.status, 200, "{}", tags.text());
    assert_eq!(elements(&tags.text(), "TagSet"), [""], "{}", tags.text());
    let missing = clients[2].request(&mut cell, "GET", "/meta/none?tagging", &[], b"");
    assert_eq!(
        (missing.status, missing.element("Code").as_deref()),
        (404, Some("NoSuchKey")),
        "{}",
        missing.text()
    );
    let tagged = clients[0].request(
        &mut cell,
        "PUT",
        "/meta/tagged",
        &[("x-amz-tagging", "k=v")],
        b"x",
    );
    assert_eq!(
        (tagged.status, tagged.element("Code").as_deref()),
        (501, Some("NotImplemented")),
        "{}",
        tagged.text()
    );
    assert_eq!(
        clients[1]
            .request(&mut cell, "HEAD", "/meta/tagged", &[], b"")
            .status,
        404
    );
    let created = clients[2].request(
        &mut cell,
        "POST",
        "/meta/tagged?uploads",
        &[("x-amz-tagging", "k=v")],
        b"",
    );
    assert_eq!(created.status, 501, "{}", created.text());
    let untagged = clients[0].request(
        &mut cell,
        "PUT",
        "/meta/untagged",
        &[("x-amz-tagging", "")],
        b"x",
    );
    assert_eq!(untagged.status, 200, "{}", untagged.text());

    // Versions (research/89 §2): each read and headed by its ID, one no version has 404
    // NoSuchVersion, one mantle never makes 400 InvalidArgument; a delete marker named 405 with
    // its Last-Modified; a version removed by its ID; every version listed.
    let enable = b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
    let versioned = clients[0].request(&mut cell, "PUT", "/meta?versioning", &[], enable);
    assert_eq!(versioned.status, 200, "{}", versioned.text());
    let first = bytes(310, 5_001);
    let second = bytes(311, 6_002);
    let v1 = clients[0].request(
        &mut cell,
        "PUT",
        "/meta/v",
        &[("x-amz-meta-n", "1")],
        &first,
    );
    let v2 = clients[1].request(
        &mut cell,
        "PUT",
        "/meta/v",
        &[("x-amz-meta-n", "2")],
        &second,
    );
    let (v1, v2) = (
        v1.header("x-amz-version-id").unwrap().to_owned(),
        v2.header("x-amz-version-id").unwrap().to_owned(),
    );
    assert_ne!(v1, v2);
    let old = clients[2].request(
        &mut cell,
        "GET",
        &format!("/meta/v?versionId={v1}"),
        &[],
        b"",
    );
    assert_eq!(old.status, 200, "{}", old.text());
    assert!(old.body == first, "the first version read by its ID");
    assert_eq!(old.header("x-amz-version-id"), Some(v1.as_str()));
    assert_eq!(old.header("x-amz-meta-n"), Some("1"));
    let current = clients[0].request(&mut cell, "GET", "/meta/v", &[], b"");
    assert!(current.body == second);
    let headed = clients[1].request(
        &mut cell,
        "HEAD",
        &format!("/meta/v?versionId={v2}"),
        &[],
        b"",
    );
    assert_eq!(headed.status, 200);
    assert_eq!(headed.header("x-amz-meta-n"), Some("2"));
    let tags = clients[2].request(
        &mut cell,
        "GET",
        &format!("/meta/v?tagging&versionId={v1}"),
        &[],
        b"",
    );
    assert_eq!(tags.status, 200, "{}", tags.text());
    assert_eq!(tags.header("x-amz-version-id"), Some(v1.as_str()));
    let absent = clients[2].request(
        &mut cell,
        "GET",
        "/meta/v?versionId=0000000000001",
        &[],
        b"",
    );
    assert_eq!(
        (absent.status, absent.element("Code").as_deref()),
        (404, Some("NoSuchVersion")),
        "{}",
        absent.text()
    );
    let invalid = clients[0].request(&mut cell, "GET", "/meta/v?versionId=not-one", &[], b"");
    assert_eq!(
        (invalid.status, invalid.element("Code").as_deref()),
        (400, Some("InvalidArgument")),
        "{}",
        invalid.text()
    );
    let marked = clients[1].request(&mut cell, "DELETE", "/meta/v", &[], b"");
    assert_eq!(marked.status, 204);
    assert_eq!(marked.header("x-amz-delete-marker"), Some("true"));
    let marker = marked.header("x-amz-version-id").unwrap().to_owned();
    let gone = clients[2].request(&mut cell, "GET", "/meta/v", &[], b"");
    assert_eq!(gone.status, 404);
    assert_eq!(gone.header("x-amz-delete-marker"), Some("true"));
    let named = clients[0].request(
        &mut cell,
        "GET",
        &format!("/meta/v?versionId={marker}"),
        &[],
        b"",
    );
    assert_eq!(
        (named.status, named.element("Code").as_deref()),
        (405, Some("MethodNotAllowed")),
        "{}",
        named.text()
    );
    assert_eq!(named.header("x-amz-delete-marker"), Some("true"));
    assert!(named.header("last-modified").is_some());
    let listed = clients[1].request(&mut cell, "GET", "/meta?versions&prefix=v", &[], b"");
    assert_eq!(listed.status, 200, "{}", listed.text());
    let ids = elements(&listed.text(), "VersionId");
    assert_eq!(
        ids,
        [marker.clone(), v2.clone(), v1.clone()],
        "{}",
        listed.text()
    );
    assert_eq!(listed.text().matches("<DeleteMarker>").count(), 1);
    assert_eq!(
        elements(&listed.text(), "IsLatest"),
        ["true", "false", "false"]
    );
    // The delete marker removed by its ID undeletes the key; a version removed by its ID goes.
    let unmarked = clients[2].request(
        &mut cell,
        "DELETE",
        &format!("/meta/v?versionId={marker}"),
        &[],
        b"",
    );
    assert_eq!(unmarked.status, 204);
    assert_eq!(unmarked.header("x-amz-delete-marker"), Some("true"));
    assert_eq!(unmarked.header("x-amz-version-id"), Some(marker.as_str()));
    assert!(
        clients[0]
            .request(&mut cell, "GET", "/meta/v", &[], b"")
            .body
            == second
    );
    let removed = clients[0].request(
        &mut cell,
        "DELETE",
        &format!("/meta/v?versionId={v1}"),
        &[],
        b"",
    );
    assert_eq!(removed.status, 204);
    assert_eq!(removed.header("x-amz-version-id"), Some(v1.as_str()));
    let lost = clients[1].request(
        &mut cell,
        "GET",
        &format!("/meta/v?versionId={v1}"),
        &[],
        b"",
    );
    assert_eq!(lost.status, 404, "{}", lost.text());

    // CopyObject (research/89 §3): within the bucket and across, its metadata the source's under
    // COPY and the request's under REPLACE, of a named version, held to the source's conditions;
    // the copy's ETag its bytes' MD5, every byte the source's.
    let source = bytes(320, 3 << 20);
    let made = clients[0].request(
        &mut cell,
        "PUT",
        "/meta/src",
        &[("content-type", "image/png"), ("x-amz-meta-from", "source")],
        &source,
    );
    assert_eq!(made.status, 200);
    let source_version = made.header("x-amz-version-id").unwrap().to_owned();
    let copied = clients[1].request(
        &mut cell,
        "PUT",
        "/other/copy",
        &[("x-amz-copy-source", "/meta/src")],
        b"",
    );
    assert_eq!(copied.status, 200, "{}", copied.text());
    assert_eq!(
        unescaped(copied.element("ETag")),
        Some(etag(&source)),
        "{}",
        copied.text()
    );
    assert!(copied.element("LastModified").is_some());
    assert_eq!(
        copied.header("x-amz-copy-source-version-id"),
        Some(source_version.as_str())
    );
    let read = clients[2].request(&mut cell, "GET", "/other/copy", &[], b"");
    assert!(read.body == source, "a copy across buckets read back");
    assert_eq!(read.header("content-type"), Some("image/png"));
    assert_eq!(read.header("x-amz-meta-from"), Some("source"));
    assert_eq!(read.header("etag"), Some(etag(&source).as_str()));
    // Its LastModified is the version's own.
    let stated = mantle_s3::time::parse_iso8601(&copied.element("LastModified").unwrap())
        .and_then(|(seconds, _)| mantle_s3::time::http_date(seconds));
    assert_eq!(read.header("last-modified"), stated.as_deref());
    let replaced = clients[0].request(
        &mut cell,
        "PUT",
        "/meta/replaced",
        &[
            ("x-amz-copy-source", "meta/src"),
            ("x-amz-metadata-directive", "REPLACE"),
            ("content-type", "text/plain"),
            ("x-amz-meta-new", "yes"),
        ],
        b"",
    );
    assert_eq!(replaced.status, 200, "{}", replaced.text());
    let head = clients[1].request(&mut cell, "HEAD", "/meta/replaced", &[], b"");
    assert_eq!(head.header("content-type"), Some("text/plain"));
    assert_eq!(head.header("x-amz-meta-new"), Some("yes"));
    assert_eq!(head.header("x-amz-meta-from"), None);
    // A version copied by its ID.
    let restored = clients[2].request(
        &mut cell,
        "PUT",
        "/other/restored",
        &[("x-amz-copy-source", &format!("meta/v?versionId={v2}"))],
        b"",
    );
    assert_eq!(restored.status, 200, "{}", restored.text());
    assert_eq!(
        restored.header("x-amz-copy-source-version-id"),
        Some(v2.as_str())
    );
    assert!(
        clients[0]
            .request(&mut cell, "GET", "/other/restored", &[], b"")
            .body
            == second
    );
    // The source's conditions: a matching if-match copies, a mismatched one and a matching
    // if-none-match fail 412, an if-modified-since after the source fails 412 (research/89 §3.3).
    let tag = etag(&source);
    let held = clients[1].request(
        &mut cell,
        "PUT",
        "/other/held",
        &[
            ("x-amz-copy-source", "meta/src"),
            ("x-amz-copy-source-if-match", &tag),
            (
                "x-amz-copy-source-if-unmodified-since",
                "Sat, 01 Jan 1994 00:00:00 GMT",
            ),
        ],
        b"",
    );
    assert_eq!(held.status, 200, "{}", held.text());
    for condition in [
        ("x-amz-copy-source-if-match", "\"0123\""),
        ("x-amz-copy-source-if-none-match", tag.as_str()),
        (
            "x-amz-copy-source-if-modified-since",
            "Fri, 01 Jan 2100 00:00:00 GMT",
        ),
    ] {
        let failed = clients[2].request(
            &mut cell,
            "PUT",
            "/other/failed",
            &[("x-amz-copy-source", "meta/src"), condition],
            b"",
        );
        assert_eq!(
            (failed.status, failed.element("Code").as_deref()),
            (412, Some("PreconditionFailed")),
            "{condition:?}: {}",
            failed.text()
        );
    }
    assert_eq!(
        clients[0]
            .request(&mut cell, "HEAD", "/other/failed", &[], b"")
            .status,
        404
    );
    // Onto itself: refused under COPY, taken under REPLACE, which changes its metadata.
    let itself = clients[0].request(
        &mut cell,
        "PUT",
        "/meta/src",
        &[("x-amz-copy-source", "meta/src")],
        b"",
    );
    assert_eq!(
        (itself.status, itself.element("Code").as_deref()),
        (400, Some("InvalidRequest")),
        "{}",
        itself.text()
    );
    let renamed = clients[1].request(
        &mut cell,
        "PUT",
        "/meta/src",
        &[
            ("x-amz-copy-source", "meta/src"),
            ("x-amz-metadata-directive", "REPLACE"),
            ("x-amz-meta-from", "itself"),
        ],
        b"",
    );
    assert_eq!(renamed.status, 200, "{}", renamed.text());
    let head = clients[2].request(&mut cell, "HEAD", "/meta/src", &[], b"");
    assert_eq!(head.header("x-amz-meta-from"), Some("itself"));
    assert_eq!(head.header("etag"), Some(tag.as_str()));
    // What names no source: a missing key, a missing bucket, a source not encoded as one.
    for (copy_source, status, code) in [
        ("meta/none", 404, "NoSuchKey"),
        ("nobucket/k", 404, "NoSuchBucket"),
        ("meta", 400, "InvalidArgument"),
        ("meta/src?versionId=0000000000001", 404, "NoSuchVersion"),
    ] {
        let failed = clients[0].request(
            &mut cell,
            "PUT",
            "/other/none",
            &[("x-amz-copy-source", copy_source)],
            b"",
        );
        assert_eq!(
            (failed.status, failed.element("Code").as_deref()),
            (status, Some(code)),
            "{copy_source}: {}",
            failed.text()
        );
    }
    let directive = clients[1].request(
        &mut cell,
        "PUT",
        "/other/none",
        &[
            ("x-amz-copy-source", "meta/src"),
            ("x-amz-metadata-directive", "MOVE"),
        ],
        b"",
    );
    assert_eq!(directive.status, 400, "{}", directive.text());

    // UploadPartCopy (research/89 §4): a range of a source past 5 MiB and a whole source as two
    // parts, completed and read back as their bytes; a malformed range, one past the source, and
    // a range of a source of 5 MiB or less refused.
    let big = bytes(330, (5 << 20) + 12_345);
    assert_eq!(
        clients[0]
            .request(&mut cell, "PUT", "/meta/big", &[], &big)
            .status,
        200
    );
    let created = clients[1].request(&mut cell, "POST", "/other/assembled?uploads", &[], b"");
    let upload = created.element("UploadId").unwrap();
    let ranged = clients[2].request(
        &mut cell,
        "PUT",
        &format!("/other/assembled?partNumber=1&uploadId={upload}"),
        &[
            ("x-amz-copy-source", "meta/big"),
            ("x-amz-copy-source-range", "bytes=0-5242879"),
        ],
        b"",
    );
    assert_eq!(ranged.status, 200, "{}", ranged.text());
    assert_eq!(
        unescaped(ranged.element("ETag")),
        Some(etag(&big[..5 << 20]))
    );
    assert!(ranged.element("LastModified").is_some());
    let whole = clients[0].request(
        &mut cell,
        "PUT",
        &format!("/other/assembled?partNumber=2&uploadId={upload}"),
        &[("x-amz-copy-source", "meta/plain")],
        b"",
    );
    assert_eq!(whole.status, 200, "{}", whole.text());
    let parts = clients[1].request(
        &mut cell,
        "GET",
        &format!("/other/assembled?uploadId={upload}"),
        &[],
        b"",
    );
    assert!(
        !parts.text().contains("1970-01-01"),
        "a part's time is its upload's: {}",
        parts.text()
    );
    for (range, status, code) in [
        ("0-2", 400, "InvalidArgument"),
        ("bytes=0-bar", 400, "InvalidArgument"),
        ("bytes=0-99999999", 416, "InvalidRange"),
    ] {
        let failed = clients[2].request(
            &mut cell,
            "PUT",
            &format!("/other/assembled?partNumber=3&uploadId={upload}"),
            &[
                ("x-amz-copy-source", "meta/big"),
                ("x-amz-copy-source-range", range),
            ],
            b"",
        );
        assert_eq!(
            (failed.status, failed.element("Code").as_deref()),
            (status, Some(code)),
            "{range}: {}",
            failed.text()
        );
    }
    let small_range = clients[0].request(
        &mut cell,
        "PUT",
        &format!("/other/assembled?partNumber=3&uploadId={upload}"),
        &[
            ("x-amz-copy-source", "meta/plain"),
            ("x-amz-copy-source-range", "bytes=0-1"),
        ],
        b"",
    );
    assert_eq!(
        (small_range.status, small_range.element("Code").as_deref()),
        (400, Some("InvalidRequest")),
        "{}",
        small_range.text()
    );
    let document = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        unescaped(ranged.element("ETag")).unwrap(),
        unescaped(whole.element("ETag")).unwrap()
    );
    let completed = clients[1].request(
        &mut cell,
        "POST",
        &format!("/other/assembled?uploadId={upload}"),
        &[],
        document.as_bytes(),
    );
    assert_eq!(completed.status, 200, "{}", completed.text());
    let assembled = clients[2].request(&mut cell, "GET", "/other/assembled", &[], b"");
    let mut expected = big[..5 << 20].to_vec();
    expected.extend_from_slice(b"abcd");
    assert!(assembled.body == expected, "the part copies read back");

    // DeleteObjects (research/89 §5): keys present and missing each reported deleted; a version
    // removed by its ID; quiet reporting failures alone; a document without its integrity header
    // refused, and one whose MD5 is another's; 1,001 keys refused.
    for key in ["d/1", "d/2", "d/3"] {
        assert_eq!(
            clients[0]
                .request(&mut cell, "PUT", &format!("/other/{key}"), &[], b"x")
                .status,
            200
        );
    }
    let (document, md5) =
        delete_document(&[("d/1", None), ("d/2", None), ("d/missing", None)], false);
    let deleted = clients[1].request(
        &mut cell,
        "POST",
        "/other?delete",
        &[("content-md5", &md5)],
        document.as_bytes(),
    );
    assert_eq!(deleted.status, 200, "{}", deleted.text());
    assert_eq!(
        elements(&deleted.text(), "Key"),
        ["d/1", "d/2", "d/missing"],
        "{}",
        deleted.text()
    );
    assert!(!deleted.text().contains("<Error>"));
    assert_eq!(
        clients[2]
            .request(&mut cell, "HEAD", "/other/d/1", &[], b"")
            .status,
        404
    );
    // Under versioning a simple delete makes a marker, which the result names; a version named
    // goes; quiet says nothing of either.
    let made = clients[0].request(&mut cell, "PUT", "/meta/m", &[], b"one");
    let m1 = made.header("x-amz-version-id").unwrap().to_owned();
    let (document, md5) = delete_document(&[("m", None)], false);
    let marked = clients[1].request(
        &mut cell,
        "POST",
        "/meta?delete",
        &[("content-md5", &md5)],
        document.as_bytes(),
    );
    assert_eq!(
        marked.element("DeleteMarker").as_deref(),
        Some("true"),
        "{}",
        marked.text()
    );
    assert!(marked.element("DeleteMarkerVersionId").is_some());
    let (document, md5) = delete_document(&[("m", Some(&m1)), ("d/never", None)], true);
    let quiet = clients[2].request(
        &mut cell,
        "POST",
        "/meta?delete",
        &[("content-md5", &md5)],
        document.as_bytes(),
    );
    assert_eq!(quiet.status, 200, "{}", quiet.text());
    assert!(!quiet.text().contains("<Deleted>"), "{}", quiet.text());
    assert!(!quiet.text().contains("<Error>"), "{}", quiet.text());
    let lost = clients[0].request(
        &mut cell,
        "GET",
        &format!("/meta/m?versionId={m1}"),
        &[],
        b"",
    );
    assert_eq!(lost.status, 404);
    // A version ID mantle never makes fails its key alone, quiet or not.
    let (document, md5) = delete_document(&[("d/3", None), ("d/3", Some("bad"))], true);
    let mixed = clients[1].request(
        &mut cell,
        "POST",
        "/other?delete",
        &[("content-md5", &md5)],
        document.as_bytes(),
    );
    assert_eq!(mixed.status, 200, "{}", mixed.text());
    assert_eq!(
        elements(&mixed.text(), "Code"),
        ["InvalidArgument"],
        "{}",
        mixed.text()
    );
    assert_eq!(
        clients[2]
            .request(&mut cell, "HEAD", "/other/d/3", &[], b"")
            .status,
        404
    );
    // A checksum header in Content-MD5's place serves; neither is refused; one of another body
    // is BadDigest.
    let (document, _) = delete_document(&[("d/2", None)], false);
    let crc = checksum::checksum(Algorithm::Crc32, document.as_bytes())
        .unwrap()
        .to_base64();
    let by_crc = clients[0].request(
        &mut cell,
        "POST",
        "/other?delete",
        &[("x-amz-checksum-crc32", &crc)],
        document.as_bytes(),
    );
    assert_eq!(by_crc.status, 200, "{}", by_crc.text());
    let bare = clients[1].request(&mut cell, "POST", "/other?delete", &[], document.as_bytes());
    assert_eq!(
        (bare.status, bare.element("Code").as_deref()),
        (400, Some("InvalidRequest")),
        "{}",
        bare.text()
    );
    let other_md5 = checksum::checksum(Algorithm::Md5, b"another document")
        .unwrap()
        .to_base64();
    let wrong = clients[2].request(
        &mut cell,
        "POST",
        "/other?delete",
        &[("content-md5", &other_md5)],
        document.as_bytes(),
    );
    assert_eq!(
        (wrong.status, wrong.element("Code").as_deref()),
        (400, Some("BadDigest")),
        "{}",
        wrong.text()
    );
    let keys: Vec<String> = (0..1001).map(|i| format!("k{i}")).collect();
    let listed: Vec<(&str, Option<&str>)> = keys.iter().map(|k| (k.as_str(), None)).collect();
    let (document, md5) = delete_document(&listed, true);
    let crowded = clients[0].request(
        &mut cell,
        "POST",
        "/other?delete",
        &[("content-md5", &md5)],
        document.as_bytes(),
    );
    assert_eq!(crowded.status, 400, "{}", crowded.text());
}
