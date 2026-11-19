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
/// Single Chunk"), checked: its key derived and its string to sign written on the stack; the one
/// buffer is the access key it names, copied into its `Verified`; nothing grows.
#[test]
fn a_signature_is_checked_with_one_buffer_its_access_keys() {
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
    assert_eq!(made, (1, 0));
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

/// `metadata::of_request`'s reading of a request's fields as it was before Lean doors step 6: each
/// field's name lowercased into a string of its own to ask whether a version keeps it.
fn reference_metadata<'a>(
    headers: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Vec<(String, String)> {
    use mantle_s3::metadata::{CONTENT, USER};
    let mut pairs: Vec<(String, String)> = Vec::new();
    for (name, value) in headers {
        let lower = name.to_ascii_lowercase();
        if !(CONTENT.contains(&lower.as_str()) || lower.starts_with(USER)) {
            continue;
        }
        match pairs.iter_mut().find(|(n, _)| *n == lower) {
            Some((_, joined)) => {
                joined.push(',');
                joined.push_str(value);
            }
            None => pairs.push((lower, value.to_owned())),
        }
    }
    pairs
}

/// A request's metadata copies only the fields a version keeps, each judged where it lies: its
/// content type and its user-defined metadata at S3's 2 KB (`metadata::MAX_USER`), each name
/// lowercased once and each value copied once, into one list made at their count. The same
/// request with as many fields no version keeps as S3's 8 KB of headers carries at their shortest
/// (`X:` and CRLF, 4 octets; `route::MAX_HEADERS`) allocates no more.
#[test]
fn a_requests_metadata_copies_only_the_fields_a_version_keeps() {
    use mantle_s3::metadata::{self, MAX_USER};
    use mantle_s3::route::MAX_HEADERS;
    let key = "author";
    let value = "v".repeat(MAX_USER - key.len());
    let name = format!("X-Amz-Meta-{key}");
    let few = vec![
        ("Host", "examplebucket.s3.amazonaws.com"),
        ("Content-Type", "text/plain"),
        (name.as_str(), value.as_str()),
        ("x-amz-content-sha256", sigv4::EMPTY_SHA256),
    ];
    let shortest = "X:\r\n".len();
    let mut many = few.clone();
    many.extend(std::iter::repeat_n(("X", ""), MAX_HEADERS / shortest));
    let (kept, few_made) = counted(|| metadata::of_request(few.iter().copied()));
    assert_eq!(kept.unwrap().len(), 2);
    let (kept, many_made) = counted(|| metadata::of_request(many.iter().copied()));
    assert_eq!(kept.unwrap().len(), 2);
    let (_, few_before) = counted(|| reference_metadata(few.iter().copied()));
    let (_, many_before) = counted(|| reference_metadata(many.iter().copied()));
    println!(
        "metadata of {} fields: before {few_before:?}, now {few_made:?}; of {} fields: before \
         {many_before:?}, now {many_made:?}",
        few.len(),
        many.len()
    );
    // The list, and each kept field's name and value.
    assert_eq!(few_made, (1 + 2 * 2, 0));
    assert_eq!(many_made, few_made);
}

/// `sigv4::uri_encode` as it was before Lean doors step 7: each character pushed onto a string that
/// grows.
fn reference_uri_encode(bytes: &[u8], slash: bool) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::new();
    for &b in bytes {
        let keep = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~');
        if keep || (b == b'/' && !slash) {
            out.push(char::from(b));
        } else {
            out.push('%');
            out.push(char::from(HEX[usize::from(b >> 4)]));
            out.push(char::from(HEX[usize::from(b & 15)]));
        }
    }
    out
}

/// S3's `UriEncode` of a key, a listing's `encoding-type=url` and a completion's `Location`, is
/// one string made at its length, as the encoder before Lean doors step 7 wrote it: a key that
/// needs none escaped, one with spaces, slashes and UTF-8, and keys at S3's 1,024-byte bound kept
/// whole and escaped whole, each with `/` kept and escaped.
#[test]
fn a_uri_encoded_text_is_one_string_at_its_length() {
    let longest = mantle_s3::route::MAX_KEY;
    let keys: [Vec<u8>; 5] = [
        b"photos".to_vec(),
        "photos/2026/a b+c~d.é".as_bytes().to_vec(),
        "k".repeat(longest).into_bytes(),
        " ".repeat(longest).into_bytes(),
        (0..=u8::MAX).cycle().take(longest).collect(),
    ];
    for key in &keys {
        for slash in [false, true] {
            let (expected, before) = counted(|| reference_uri_encode(key, slash));
            let (encoded, made) = counted(|| sigv4::uri_encode(key, slash));
            println!(
                "uri_encode of {} bytes, slash {slash}: before {before:?}, now {made:?}",
                key.len()
            );
            assert_eq!(
                (made, encoded.capacity()),
                ((1, 0), encoded.len()),
                "{} bytes, slash {slash}",
                key.len()
            );
            assert_eq!(encoded, expected);
        }
    }
}

/// An access key's or a principal's id is one string at its length: its characters written on the
/// stack, each drawn byte's base32 character as before Lean doors step 7, and copied once.
#[test]
fn a_drawn_id_is_one_string_at_its_length() {
    use mantle_s3::account::{KEY_ID_CHARS, draw_id, valid_key_id};
    let mut drawn = std::collections::BTreeSet::new();
    for _ in 0..8 {
        let (id, made) = counted(|| draw_id().unwrap());
        println!("draw_id: now {made:?}");
        assert_eq!(
            (made, id.len(), id.capacity()),
            ((1, 0), KEY_ID_CHARS, KEY_ID_CHARS)
        );
        assert!(valid_key_id(&id), "{id}");
        assert!(drawn.insert(id));
    }
}

/// `checksum::multipart_etag` as it was before: its hex in a string of its own, then `format!`.
fn reference_multipart_etag(parts: &[[u8; 16]]) -> String {
    let mut h = crypto::Digest::new(&crypto::MD5).unwrap();
    for part in parts {
        h.update(part).unwrap();
    }
    let digest: [u8; 16] = h.finish_array().unwrap();
    format!("{}-{}", reference_etag(&digest), parts.len())
}

/// A multipart object's ETag is one string made at its length, the parts' MD5's 32 hex digits, a
/// dash and the count, as the encoder before wrote them: one part, ten and S3's most, 10,000.
#[test]
fn a_multipart_etag_is_one_string_at_its_length() {
    for count in [1usize, 10, 10_000] {
        let parts: Vec<[u8; 16]> = (0..count)
            .map(|n| std::array::from_fn(|i| u8::try_from((n + i) % 251).unwrap()))
            .collect();
        let (expected, before) = counted(|| reference_multipart_etag(&parts));
        let (etag, made) = counted(|| mantle_s3::checksum::multipart_etag(&parts).unwrap());
        println!("multipart etag of {count}: before {before:?}, now {made:?}");
        assert_eq!(made.1, 0, "{count}: no reallocation");
        assert_eq!(etag.capacity(), etag.len(), "{count}");
        assert_eq!(etag, expected);
    }
}

/// An object of parts' composite checksum is one string made at its length, base64 of the parts'
/// checksum, a dash and the count, as the encoder before wrote it.
#[test]
fn a_composite_checksum_is_one_string_at_its_length() {
    use mantle_s3::checksum::{self, Algorithm, Checksum};
    for algorithm in [
        Algorithm::Crc32,
        Algorithm::Crc32c,
        Algorithm::Sha1,
        Algorithm::Sha256,
    ] {
        for count in [1usize, 10, 10_000] {
            let parts: Vec<Checksum> = (0..count)
                .map(|n| {
                    let mut h = checksum::Hasher::new(algorithm).unwrap();
                    h.update(&n.to_le_bytes()).unwrap();
                    h.finish().unwrap()
                })
                .collect();
            let reference = {
                let mut h = checksum::Hasher::new(algorithm).unwrap();
                for part in &parts {
                    h.update(&part.bytes).unwrap();
                }
                format!("{}-{}", h.finish().unwrap().to_base64(), parts.len())
            };
            let (value, made) = counted(|| checksum::composite(&parts).unwrap().unwrap());
            println!("composite {algorithm:?} of {count}: now {made:?}");
            assert_eq!(made.1, 0, "{algorithm:?} {count}: no reallocation");
            assert_eq!(value.capacity(), value.len(), "{algorithm:?} {count}");
            assert_eq!(value, reference);
        }
    }
}
