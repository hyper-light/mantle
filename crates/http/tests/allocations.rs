//! What the codec allocates, counted exactly: a head's field list once, at its size, and nothing
//! else on a request's way through: no growth, no copy of the head or of a body's octets.
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
use mantle_http::{Body, Framing, Version, parse};

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

#[test]
fn a_head_allocates_its_field_list_once_and_a_body_nothing() {
    let (head, made) = allocations(|| parse(PUT).unwrap().unwrap());
    assert_eq!(made, 1, "the field list, made once at its size");
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
    // A head with no field allocates nothing at all.
    let (_, made) = allocations(|| parse(b"GET / HTTP/1.0\r\n\r\n").unwrap().unwrap());
    assert_eq!(made, 0);
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
