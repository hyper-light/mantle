//! What the S3 door's own work allocates per request, counted exactly: a signature checked with
//! its canonical request hashed as it is formed and its string to sign in one buffer at its
//! length; an aws-chunked body decoded in place, each chunk's signature checked with no buffer of
//! its own; a listing's document one buffer at its length, nothing a key.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_measure::alloc;
use mantle_s3::chunked::Decoder;
use mantle_s3::crypto;
use mantle_s3::list::{Page, Paging, Request as ListRequest};
use mantle_s3::response::{self, Entry, ListV2};
use mantle_s3::sigv4::{self, Request};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// The allocations and reallocations `f` makes on this thread.
fn counted<T>(f: impl FnOnce() -> T) -> (T, (u64, u64)) {
    alloc::begin();
    let out = f();
    let counts = alloc::end();
    (out, (counts.allocations, counts.reallocations))
}

const KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const WHEN: &str = "20130524T000000Z";

fn now() -> i64 {
    mantle_s3::time::parse_amz_date(WHEN).unwrap()
}

fn authorization(signed: &str, signature: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential={KEY}/20130524/us-east-1/s3/aws4_request,SignedHeaders={signed},Signature={signature}"
    )
}

/// AWS's GetObject example (S3 developer guide, "Signature Calculation: Transfer Payload in a
/// Single Chunk"), checked: its key derived into one buffer, its string to sign into another, and
/// the access key it names; nothing grows.
#[test]
fn a_signature_is_checked_in_three_buffers() {
    let auth = authorization(
        "host;range;x-amz-content-sha256;x-amz-date",
        "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
    );
    let headers = [
        ("Host", "examplebucket.s3.amazonaws.com"),
        ("Range", "bytes=0-9"),
        ("x-amz-content-sha256", sigv4::EMPTY_SHA256),
        ("x-amz-date", WHEN),
        ("Authorization", auth.as_str()),
    ];
    let request = Request {
        method: "GET",
        path: "/test.txt",
        query: "",
        headers: &headers,
    };
    let secret = |key: &str| (key == KEY).then_some(SECRET);
    let (verified, made) = counted(|| sigv4::verify(&request, "us-east-1", now(), secret));
    assert_eq!(verified.unwrap().access_key, KEY);
    assert_eq!(made, (3, 0));
}

/// AWS's chunked upload example (S3 developer guide, "Signature Calculation: Transfer Payload in
/// Multiple Chunks"): its 66,560 bytes decoded in place, each of the three chunks' signatures
/// checked, with no allocation at all.
#[test]
fn an_aws_chunked_body_is_decoded_in_place_with_no_allocation() {
    let seed = sigv4::unhex("4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9")
        .unwrap();
    let chain = sigv4::client_chain(SECRET, WHEN, "20130524", "us-east-1", seed).unwrap();
    let mut body = Vec::new();
    for (len, signature) in [
        (
            65536,
            "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648",
        ),
        (
            1024,
            "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497",
        ),
        (
            0,
            "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9",
        ),
    ] {
        body.extend_from_slice(format!("{len:x};chunk-signature={signature}\r\n").as_bytes());
        body.extend(std::iter::repeat_n(b'a', len));
        body.extend_from_slice(b"\r\n");
    }
    let mut decoder = Decoder::signed(chain, None, 66560);
    let (payload, made) = counted(|| {
        let mut at = 0;
        let mut payload = 0;
        while at < body.len() {
            let (taken, run) = decoder.step(&body[at..]).unwrap();
            assert!(body[at..][run.clone()].iter().all(|b| *b == b'a'));
            payload += run.len();
            at += taken;
        }
        payload
    });
    assert_eq!(payload, 66560);
    assert_eq!(made, (0, 0));
    assert_eq!(decoder.finish(), Ok(None));
    // A chunk whose bytes changed fails its signature where it ends.
    let mut changed = body.clone();
    changed[200] = b'b';
    let seed = sigv4::unhex("4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9")
        .unwrap();
    let chain = sigv4::client_chain(SECRET, WHEN, "20130524", "us-east-1", seed).unwrap();
    let mut decoder = Decoder::signed(chain, None, 66560);
    let mut at = 0;
    let refused = loop {
        match decoder.step(&changed[at..]) {
            Ok((taken, _)) => at += taken,
            Err(e) => break e,
        }
    };
    assert_eq!(refused.code(), ("SignatureDoesNotMatch", 403));
    let _ = crypto::sha256(b"").unwrap();
}

/// A ListObjectsV2 document of a page: one buffer at its length, and one list of the page's
/// times, at any count of keys.
#[test]
fn a_listings_document_allocates_nothing_a_key() {
    let page = |keys: usize| Page {
        contents: (0..keys)
            .map(|i| {
                (
                    format!("key-{i:04}"),
                    Entry {
                        modified: 1_727_179_200 + i as i64,
                        etag: "6805f2cfc46c0f04559748bb039d69ae".into(),
                        size: i as u64,
                        owner: "owner".into(),
                        checksum: None,
                    },
                )
            })
            .collect(),
        common_prefixes: Vec::new(),
        truncated: true,
        next: None,
    };
    let list = ListV2 {
        bucket: "bucket",
        request: ListRequest {
            prefix: "",
            delimiter: None,
            start: None,
            max_keys: 1000,
            paging: Paging::Token,
        },
        continuation_token: None,
        start_after: None,
        url: false,
        fetch_owner: true,
    };
    for keys in [1, 10, 1000] {
        let page = page(keys);
        let (document, made) = counted(|| response::list_objects_v2(&list, &page).unwrap());
        assert_eq!(made, (2, 0), "{keys} keys");
        assert_eq!(document.capacity(), document.len());
    }
}
