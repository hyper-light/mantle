//! What the codec allocates, counted exactly: nothing on a request's way through. A head is parsed
//! where it lies, its field lines read from it as they are asked for, whatever shape it has and
//! whether it is read, waited on or refused; a body is decoded as spans of its input.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_measure::alloc;
use mantle_http::write::{self, Length};
use mantle_http::{Body, Framing, MAX_HEAD, Version, parse};
use proptest::prelude::*;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// The allocations and reallocations `f` makes on this thread.
fn allocations<T>(f: impl FnOnce() -> T) -> (T, u64) {
    alloc::begin();
    let out = f();
    (out, alloc::end().calls())
}

/// A PutObject as the AWS CLI signs it, its body chunked.
const PUT: &[u8] = b"PUT /bucket/some/key.bin HTTP/1.1\r\n\
Host: 127.0.0.1:9000\r\n\
Accept-Encoding: identity\r\n\
Content-Type: application/octet-stream\r\n\
User-Agent: aws-cli/2.27.0 md/awscrt#0.26.1 ua/2.1 os/macos#25.4.0 md/arch#arm64 lang/python#3.13.3\r\n\
Expect: 100-continue\r\n\
X-Amz-Date: 20261025T120000Z\r\n\
X-Amz-Content-SHA256: STREAMING-UNSIGNED-PAYLOAD-TRAILER\r\n\
X-Amz-Trailer: x-amz-checksum-crc64nvme\r\n\
X-Amz-Decoded-Content-Length: 26\r\n\
Content-Encoding: aws-chunked\r\n\
amz-sdk-invocation-id: 0c5dd5a3-6a8e-4c7b-9b8f-6d2a8a7a1f3c\r\n\
amz-sdk-request: attempt=1; max=3\r\n\
Authorization: AWS4-HMAC-SHA256 Credential=AKIAEXAMPLE/20261025/us-east-1/s3/aws4_request, SignedHeaders=content-encoding;content-type;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-trailer, Signature=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\r\n\
Transfer-Encoding: chunked\r\n\
\r\n\
1a\r\nabcdefghijklmnopqrstuvwxyz\r\n0\r\n\r\n";

/// Every head shape of the codec's rule tests (tests/rules.rs): read, refused, absolute-form,
/// HTTP/1.0, pipelined, folded, with controls, CRs and obs-text, and the stock clients' own.
const SHAPES: &[&[u8]] = &[
    PUT,
    b"PUT /b/k?x=1 HTTP/1.1\r\nHost: b.s3\r\nContent-Length: 5\r\nX-Amz-Meta-A:  v w \r\n\r\nhello",
    b"PUT / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\n\r\n",
    b"PUT / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n",
    b"PUT / HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\n",
    b"PUT / HTTP/1.1\r\nHost: h\r\nContent-Length: 5, 5\r\n\r\n",
    b"GET / HTTP/1.1\r\nHost: h\r\n\r\n",
    b"\r\n\r\nGET / HTTP/1.1\r\nHost: h\r\n\r\n",
    b"GET / HTTP/1.1\nHost: h\n\n",
    b"GET / HTTP/1.1\r\nHost : h\r\n\r\n",
    b"GET / HTTP/1.1\r\nHost: h\r\n folded\r\n\r\n",
    b"GET / HTTP/1.1\r\n Host: h\r\n\r\n",
    b"GET / HTTP/1.1\r\nHost: h\rX\r\n\r\n",
    b"GET / HTTP/1.1\r\nHost: h\r\nX: a\x00b\r\n\r\n",
    b"GET / HTTP/1.1\r\nHost: h\r\nX: a\x7fb\r\n\r\n",
    b"GET  / HTTP/1.1\r\nHost: h\r\n\r\n",
    b"GET / x HTTP/1.1\r\nHost: h\r\n\r\n",
    b"GET /\x80 HTTP/1.1\r\nHost: h\r\n\r\n",
    b"GET / HTTP/1.1 \r\nHost: h\r\n\r\n",
    b"GET / HTTP/1.1\r\n: v\r\nHost: h\r\n\r\n",
    b"GET / HTTP/1.1\r\nNo colon\r\nHost: h\r\n\r\n",
    b"GET / HTTP/1.1\r\nHost: h\r\nX: \xe9\r\n\r\n",
    b"GET / HTTP/2.0\r\nHost: h\r\n\r\n",
    b"GET / http/1.1\r\nHost: h\r\n\r\n",
    b"GET / HTTP/1.10\r\nHost: h\r\n\r\n",
    b"GET / HTTP/1.0\r\n\r\n",
    b"GET / HTTP/1.1\r\n\r\n",
    b"GET / HTTP/1.1\r\nHost: a\r\nHost: a\r\n\r\n",
    b"GET / HTTP/1.1\r\nHost: a b\r\n\r\n",
    b"GET / HTTP/1.0\r\nHost: a\r\nhost: b\r\n\r\n",
    b"GET http://b.s3.example:9000/k?versions HTTP/1.1\r\nHost: ignored\r\n\r\n",
    b"OPTIONS * HTTP/1.1\r\nHost: h\r\n\r\n",
    b"GET / HTTP/1.1\r\nHost:\r\n\r\n",
    b"PUT / HTTP/1.1\r\nHost: h\r\nExpect: 100-continue\r\nContent-Length: 1\r\n\r\nx",
    b"PUT / HTTP/1.1\r\nHost: h\r\nExpect: 100-continue, x\r\n\r\n",
    b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n",
    b"GET / HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n",
    b"PUT /a HTTP/1.1\r\nHost: h\r\nContent-Length: 3\r\n\r\nabcPUT /b HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nde\r\n0\r\n\r\n",
    b"PUT / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n",
    b"GET / HTTP/1.1\r\nHost: h",
];

/// A head of exactly [`MAX_HEAD`] octets holding as many fields as fit, and one octet more.
fn largest() -> [Vec<u8>; 2] {
    let exact = |size: usize| {
        let mut head = b"GET / HTTP/1.1\r\nHost: h\r\n".to_vec();
        while head.len() + "a: b\r\n".len() + "X: \r\n\r\n".len() <= size {
            head.extend_from_slice(b"a: b\r\n");
        }
        head.extend_from_slice(b"X: ");
        head.extend(std::iter::repeat_n(b'p', size - head.len() - 4));
        head.extend_from_slice(b"\r\n\r\n");
        assert_eq!(head.len(), size);
        head
    };
    [exact(MAX_HEAD), exact(MAX_HEAD + 1)]
}

/// What `parse` allocates reading `bytes`, whatever it answers.
fn parsed(bytes: &[u8]) -> u64 {
    allocations(|| {
        let head = parse(bytes);
        if let Ok(Some(head)) = &head {
            // Every field read, and one looked up, as a request's routing reads them.
            let read = head.fields(bytes).count();
            let _ = head.field(bytes, "authorization");
            return read;
        }
        0
    })
    .1
}

#[test]
fn every_head_shape_is_parsed_with_no_allocation() {
    for shape in SHAPES {
        for end in 0..=shape.len() {
            assert_eq!(
                parsed(&shape[..end]),
                0,
                "{:?}",
                String::from_utf8_lossy(&shape[..end])
            );
        }
    }
    for head in largest() {
        assert_eq!(parsed(&head), 0);
    }
    let [at, _] = largest();
    assert!(parse(&at).unwrap().unwrap().fields(&at).count() > MAX_HEAD / 8);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// A head of any shape, each of the suite's mutated where and as any octet may be, is parsed
    /// with no allocation.
    #[test]
    fn a_mutated_head_is_parsed_with_no_allocation(
        shape in 0..SHAPES.len(),
        edits in proptest::collection::vec((any::<usize>(), any::<u8>(), 0u8..3), 1..8),
    ) {
        let mut bytes = SHAPES[shape].to_vec();
        for (at, octet, how) in edits {
            let at = at % (bytes.len() + 1);
            match how {
                0 => bytes.insert(at, octet),
                1 if at < bytes.len() => bytes[at] = octet,
                _ if at < bytes.len() => {
                    bytes.remove(at);
                }
                _ => {}
            }
        }
        prop_assert_eq!(parsed(&bytes), 0);
    }
}

#[test]
fn a_body_decodes_with_no_allocation() {
    let head = parse(PUT).unwrap().unwrap();
    assert_eq!(head.fields(PUT).count(), 14);
    assert_eq!(head.framing(), Framing::Chunked);
    let rest = &PUT[head.len()..];
    let (taken, made) = allocations(|| {
        let mut body = Body::new(head.framing());
        let mut at = 0;
        let mut taken = 0;
        loop {
            let step = body.decode(&rest[at..]).unwrap();
            taken += step.data.len();
            at += step.consumed;
            if step.end {
                return taken;
            }
        }
    });
    assert_eq!((taken, made), (26, 0));
}

#[test]
fn a_response_head_is_written_with_no_allocation_of_its_own() {
    let mut out = Vec::with_capacity(512);
    let (_, made) = allocations(|| {
        write::head(
            &mut out,
            Version::Http11,
            200,
            "OK",
            &[
                ("ETag", b"\"d41d8cd98f00b204e9800998ecf8427e\""),
                ("Last-Modified", b"Sun, 25 Oct 2026 12:00:00 GMT"),
                ("x-amz-request-id", b"0123456789ABCDEF"),
            ],
            Length::Known(1 << 20),
        )
        .unwrap();
        write::chunk(&mut out, 1 << 20);
        write::last(&mut out, &[]).unwrap();
    });
    assert_eq!(made, 0);
}
