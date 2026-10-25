//! Generated and mutated requests (docs/research/70): a request written from a model reads back as
//! that model, whole or in any pieces; any octets, a valid request mutated or none at all, are read
//! or refused with a typed error, never a panic, nothing read past them and nothing held past the
//! head's bound. Each head is read as `httparse`, hyper's parser, reads it, the oracle the owner
//! allowed for these tests alone (2026-10-25), or refused: where the codec reads a head the oracle
//! reads the same one, and where the codec waits for more the oracle has read no head.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use mantle_http::{Body, Framing, MAX_HEAD, Refusal, Version, parse};
use proptest::prelude::*;

/// A head as the oracle reads it: its length, method, target, minor version and fields.
type Read = (usize, Vec<u8>, Vec<u8>, u8, Fields);

/// The head `bytes` begins with as `httparse` reads it: none while it reads no whole head, or its
/// refusal.
fn oracle(bytes: &[u8]) -> Option<Result<Read, httparse::Error>> {
    // Room for every field a head within the bound can hold, so the oracle never runs out.
    let mut fields = vec![httparse::EMPTY_HEADER; MAX_HEAD];
    let mut request = httparse::Request::new(&mut fields);
    match request.parse(bytes) {
        Ok(httparse::Status::Complete(len)) => Some(Ok((
            len,
            request.method?.as_bytes().to_vec(),
            request.path?.as_bytes().to_vec(),
            request.version?,
            request
                .headers
                .iter()
                .map(|h| (h.name.as_bytes().to_vec(), h.value.to_vec()))
                .collect(),
        ))),
        Ok(httparse::Status::Partial) => None,
        Err(e) => Some(Err(e)),
    }
}

/// Whether the codec agrees with the oracle on `bytes`: a head it reads is the one the oracle
/// reads; while it waits for more, the oracle has read no head; a refusal of its own is typed.
fn agrees(bytes: &[u8]) -> Result<(), String> {
    match parse(bytes) {
        Ok(Some(head)) => {
            let version = match head.version() {
                Version::Http10 => 0,
                Version::Http11 => 1,
            };
            let read = (
                head.len(),
                head.method(bytes).to_vec(),
                head.target(bytes).to_vec(),
                version,
                head.fields(bytes)
                    .map(|(n, v)| (n.to_vec(), v.to_vec()))
                    .collect(),
            );
            match oracle(bytes) {
                Some(Ok(theirs)) if theirs == read => Ok(()),
                other => Err(format!("the codec read {read:?}, the oracle {other:?}")),
            }
        }
        Ok(None) => match oracle(bytes) {
            Some(Ok(theirs)) => Err(format!("the codec waits, the oracle read {theirs:?}")),
            _ => Ok(()),
        },
        Err(_) => Ok(()),
    }
}

/// How a model's body is chunked: the lengths it is cut into, each chunk with an extension or not,
/// and its trailers.
type Chunks = (Vec<usize>, Vec<bool>, Vec<(String, String)>);

/// Fields, each its name and value.
type Fields = Vec<(Vec<u8>, Vec<u8>)>;

/// A request as a model: its method, target, fields and body, framed by length or by chunks.
#[derive(Debug, Clone)]
struct Model {
    method: String,
    target: String,
    fields: Vec<(String, String)>,
    body: Vec<u8>,
    /// Chunked: the chunks' lengths the body is cut into, each with an extension or not, and the
    /// trailers; else framed by Content-Length.
    chunked: Option<Chunks>,
}

impl Model {
    fn write(&self) -> Vec<u8> {
        let mut out = format!(
            "{} {} HTTP/1.1\r\nHost: h.example\r\n",
            self.method, self.target
        )
        .into_bytes();
        for (name, value) in &self.fields {
            out.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
        }
        match &self.chunked {
            None => {
                out.extend_from_slice(
                    format!("Content-Length: {}\r\n\r\n", self.body.len()).as_bytes(),
                );
                out.extend_from_slice(&self.body);
            }
            Some((cuts, extended, trailers)) => {
                out.extend_from_slice(b"Transfer-Encoding: chunked\r\n\r\n");
                let mut at = 0;
                for (len, ext) in cuts.iter().zip(extended) {
                    let len = (*len).min(self.body.len() - at);
                    if len == 0 {
                        continue;
                    }
                    let ext = if *ext { ";e=\"v\"" } else { "" };
                    out.extend_from_slice(format!("{len:X}{ext}\r\n").as_bytes());
                    out.extend_from_slice(&self.body[at..at + len]);
                    out.extend_from_slice(b"\r\n");
                    at += len;
                }
                if at < self.body.len() {
                    out.extend_from_slice(format!("{:x}\r\n", self.body.len() - at).as_bytes());
                    out.extend_from_slice(&self.body[at..]);
                    out.extend_from_slice(b"\r\n");
                }
                out.extend_from_slice(b"0\r\n");
                for (name, value) in trailers {
                    out.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
                }
                out.extend_from_slice(b"\r\n");
            }
        }
        out
    }
}

fn token() -> impl Strategy<Value = String> {
    "[A-Za-z0-9!#$%&'*+.^_`|~-]{1,12}"
}

/// A field name the codec gives no meaning of its own.
fn plain_name() -> impl Strategy<Value = String> {
    token().prop_filter("a name of its own", |n| {
        ![
            "host",
            "content-length",
            "transfer-encoding",
            "expect",
            "connection",
        ]
        .contains(&n.to_ascii_lowercase().as_str())
    })
}

/// A field value bare of whitespace at its ends: VCHAR, with SP and HTAB within.
fn value() -> impl Strategy<Value = String> {
    "([!-~]([ \t!-~]{0,30}[!-~])?)?"
}

fn model() -> impl Strategy<Value = Model> {
    (
        "[A-Z]{1,7}",
        "/[A-Za-z0-9/._~%?=&-]{0,60}",
        proptest::collection::vec((plain_name(), value()), 0..12),
        proptest::collection::vec(any::<u8>(), 0..2048),
        proptest::option::of((
            proptest::collection::vec(1usize..600, 0..8),
            proptest::collection::vec(any::<bool>(), 8),
            proptest::collection::vec((plain_name(), value()), 0..4),
        )),
    )
        .prop_map(|(method, target, fields, body, chunked)| Model {
            method,
            target,
            fields,
            body,
            chunked,
        })
}

/// What a decoder gives of `input` fed in the pieces `cuts` marks: the body's octets, the octets
/// taken, the trailers, and whether the body ended; or its refusal.
fn decode(
    framing: Framing,
    input: &[u8],
    cuts: &[usize],
) -> Result<(Vec<u8>, usize, Fields, bool), Refusal> {
    let mut body = Body::new(framing);
    let (mut data, mut at) = (Vec::new(), 0usize);
    let mut ends: Vec<usize> = cuts.iter().map(|c| c % (input.len() + 1)).collect();
    ends.push(input.len());
    ends.sort_unstable();
    for end in ends {
        // Each read's octets are given until the decoder takes no more of them.
        loop {
            let piece = &input[at..end.max(at)];
            let step = body.decode(piece)?;
            assert!(step.consumed <= piece.len(), "taken past its input");
            assert!(step.data.end <= step.consumed && step.data.start <= step.data.end);
            data.extend_from_slice(&piece[step.data.clone()]);
            at += step.consumed;
            if step.end {
                let trailers: Fields = body
                    .trailers()
                    .map(|(n, v)| (n.to_vec(), v.to_vec()))
                    .collect();
                let held: usize = trailers.iter().map(|(n, v)| n.len() + v.len()).sum();
                assert!(held <= MAX_HEAD);
                return Ok((data, at, trailers, true));
            }
            if step.consumed == 0 {
                break;
            }
        }
    }
    Ok((data, at, Vec::new(), false))
}

/// Each property's cases: 512, or as many as proptest's `PROPTEST_CASES` asks for a deeper search,
/// which a fixed count would override.
fn config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|c| c.parse().ok())
        .unwrap_or(512);
    ProptestConfig::with_cases(cases)
}

proptest! {
    #![proptest_config(config())]

    /// A request written from a model reads back as the model, its head whole or growing a piece at
    /// a time and its body in any pieces.
    #[test]
    fn a_written_request_reads_back(model in model(), cuts in proptest::collection::vec(any::<usize>(), 0..6)) {
        let bytes = model.write();
        prop_assume!(bytes.len() <= MAX_HEAD + model.body.len() + 4096);
        let head = parse(&bytes).unwrap().expect("a whole head");
        prop_assert_eq!(agrees(&bytes), Ok(()));
        prop_assert_eq!(head.method(&bytes), model.method.as_bytes());
        prop_assert_eq!(head.target(&bytes), model.target.as_bytes());
        let fields: Fields = head
            .fields(&bytes)
            .filter(|(n, _)| !n.eq_ignore_ascii_case(b"host")
                && !n.eq_ignore_ascii_case(b"content-length")
                && !n.eq_ignore_ascii_case(b"transfer-encoding"))
            .map(|(n, v)| (n.to_vec(), v.to_vec()))
            .collect();
        let expected: Fields = model
            .fields
            .iter()
            .map(|(n, v)| (n.as_bytes().to_vec(), v.as_bytes().to_vec()))
            .collect();
        prop_assert_eq!(fields, expected);
        for end in cuts.iter().map(|c| c % head.len()) {
            prop_assert_eq!(parse(&bytes[..end]), Ok(None));
        }
        let (data, used, trailers, ended) = decode(head.framing(), &bytes[head.len()..], &cuts).unwrap();
        prop_assert!(ended);
        prop_assert_eq!(head.len() + used, bytes.len());
        prop_assert_eq!(data, model.body.clone());
        let expected: Fields = model
            .chunked
            .map(|(_, _, t)| t.into_iter().map(|(n, v)| (n.into_bytes(), v.into_bytes())).collect())
            .unwrap_or_default();
        prop_assert_eq!(trailers, expected);
    }

    /// A valid request mutated is read or refused, never a panic, nothing read past it.
    #[test]
    fn a_mutated_request_is_read_or_refused(
        model in model(),
        edits in proptest::collection::vec((any::<usize>(), any::<u8>(), 0u8..3), 1..8),
        cuts in proptest::collection::vec(any::<usize>(), 0..4),
    ) {
        let mut bytes = model.write();
        for (at, b, kind) in edits {
            let at = at % (bytes.len() + 1);
            match kind {
                0 if at < bytes.len() => bytes[at] = b,
                1 => bytes.insert(at, b),
                _ if at < bytes.len() => {
                    bytes.remove(at);
                }
                _ => {}
            }
        }
        prop_assert_eq!(agrees(&bytes), Ok(()));
        if let Ok(Some(head)) = parse(&bytes) {
            prop_assert!(head.len() <= bytes.len().min(MAX_HEAD));
            let _ = decode(head.framing(), &bytes[head.len()..], &cuts);
        }
    }

    /// Any octets at all, up to twice the head's bound, are read or refused, never a panic.
    #[test]
    fn any_octets_are_read_or_refused(bytes in proptest::collection::vec(any::<u8>(), 0..(2 * MAX_HEAD))) {
        prop_assert_eq!(agrees(&bytes), Ok(()));
        match parse(&bytes) {
            Ok(Some(head)) => {
                prop_assert!(head.len() <= bytes.len().min(MAX_HEAD));
                let _ = decode(head.framing(), &bytes[head.len()..], &[]);
            }
            Ok(None) => prop_assert!(bytes.len() < MAX_HEAD),
            Err(refusal) => prop_assert!(matches!(refusal.status(), 400 | 414 | 417 | 501 | 505)),
        }
        let _ = decode(Framing::Chunked, &bytes, &[1, 7, 4096]);
    }
}
