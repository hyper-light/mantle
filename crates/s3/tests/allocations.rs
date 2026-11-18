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
    let secret = |key: &str| if key == KEY { Some(SECRET) } else { None };
    let (verified, made) = counted(|| sigv4::verify(&request, "us-east-1", now(), secret));
    assert_eq!(verified.unwrap().access_key, KEY);
    assert_eq!(made, (3, 0));
}

/// AWS's chunked upload example (S3 developer guide, "Signature Calculation: Transfer Payload in
/// Multiple Chunks"): its 66,560 bytes decoded in place, each of the three chunks' signatures
/// checked, with no allocation at all.
#[test]
fn an_aws_chunked_body_is_decoded_in_place_with_no_allocation() {
    let seed =
        sigv4::unhex("4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9").unwrap();
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
    let seed =
        sigv4::unhex("4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9").unwrap();
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
                        modified: 1_727_179_200 + i64::try_from(i).unwrap(),
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

/// `checksum::etag` as it was before Lean doors step 7: each hex digit pushed onto a string that
/// grows.
fn reference_etag(md5: &[u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::new();
    for &b in md5 {
        out.push(char::from(HEX[usize::from(b >> 4)]));
        out.push(char::from(HEX[usize::from(b & 15)]));
    }
    out
}

/// An object's ETag is one string made at its length, the MD5's 32 hex characters, as the encoder
/// before Lean doors step 7 wrote them.
#[test]
fn an_etag_is_one_string_at_its_length() {
    for md5 in [
        [0u8; 16],
        [0xa5; 16],
        std::array::from_fn(|i| u8::try_from(i).unwrap() * 17),
    ] {
        let (expected, before) = counted(|| reference_etag(&md5));
        let (etag, made) = counted(|| mantle_s3::checksum::etag(&md5));
        println!("etag {expected}: before {before:?}, now {made:?}");
        assert_eq!((made, etag.capacity()), ((1, 0), etag.len()), "{expected}");
        assert_eq!(etag, expected);
    }
}

/// A listing's continuation token is its tag and key in one buffer at their length, encoded into
/// one string at its length, the token the encoder before Lean doors step 7 wrote: no key, a key,
/// and a key at S3's 1,024-byte bound.
#[test]
fn a_continuation_token_is_two_buffers_at_their_lengths() {
    use base64::Engine as _;
    use mantle_s3::list::{Resume, continuation_start, continuation_token};
    let resumes = [
        Resume::After(String::new()),
        Resume::Past("photos/2026/".into()),
        Resume::After("k".repeat(mantle_s3::route::MAX_KEY)),
    ];
    for resume in resumes {
        let (tag, key) = match &resume {
            Resume::After(key) => (b'a', key),
            Resume::Past(key) => (b'p', key),
        };
        // `continuation_token` as it was before Lean doors step 7: its bytes in a buffer that grew.
        let (expected, before) = counted(|| {
            let mut bytes = Vec::new();
            bytes.push(tag);
            bytes.extend_from_slice(key.as_bytes());
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
        });
        let (token, made) = counted(|| continuation_token(&resume));
        println!(
            "token of {} bytes: before {before:?}, now {made:?}",
            key.len()
        );
        assert_eq!(
            (made, token.capacity()),
            ((2, 0), token.len()),
            "{resume:?}"
        );
        assert_eq!(token, expected);
        assert_eq!(continuation_start(&token), Some(resume));
    }
}
