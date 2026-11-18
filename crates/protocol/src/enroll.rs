//! A written-off node's request for a new identity and its answer (docs/design/membership.md
//! §5.1 step 3): the one exchange of kind `Enroll` it opens on its connection under the
//! enrollment protocol ([`crate::classes::ENROLL`]), and the one a member opens to the root
//! range's leader to forward it. A request is the exchange's body, its head empty, and its answer
//! the reply's body, its head empty: each body's CRC-32C is checked by the transport (CLAUDE.md
//! §6).
//!
//! | Message | Bytes |
//! |---|---|
//! | `Regrant` | the tag, u8 0; the public key asked for (32) |
//! | `Forward` | the tag, u8 1; the old id, u64; the old certificate's fingerprint (32) and the key it certifies (32), as the member's handshake with the node proved them; the public key asked for (32) |
//! | `Granted` | the tag, u8 0; the certificate and the issuer's, each a u32 length then its bytes; the members (a u32 count), each its id, u64, and its QUIC and plane places, each a u32 length then `HOST:PORT`, at most [`MAX_REACH`] bytes |
//! | `Refused` | the tag, u8 1; the refusal's code, u8 ([`Refusal`]) |
//!
//! Numbers are little-endian. Each message is written into one buffer made at its length
//! (`mantle_codec::encode`).

use mantle_codec::{Reader, Writer};

/// The bytes of a node's public key: an Ed25519 key's 32 (RFC 8032 §5.1.5).
pub const KEY: usize = 32;

/// The bytes of a certificate's fingerprint: a SHA-256 (FIPS 180-4).
pub const FINGERPRINT: usize = 32;

/// The longest place a member is dialed at, `HOST:PORT`, in bytes: a host of at most 255 octets
/// (RFC 1035 §2.3.4) in brackets where it is an IPv6 address (RFC 3986 §3.2.2), a colon, and a
/// port of at most five digits (a `u16`), as the root range bounds it (`mantle_meta::root::
/// MAX_REACH`).
pub const MAX_REACH: usize = 255 + 8;

/// The longest request: a forward's tag, old id, fingerprint, old key and key. A body longer is
/// refused before it is read.
pub const MAX_REQUEST: usize = 1 + 8 + FINGERPRINT + KEY + KEY;

const REGRANT: u8 = 0;
const FORWARD: u8 = 1;
const GRANTED: u8 = 0;
const REFUSED: u8 = 1;

/// The bytes of a member in an answer beside its places: its id and each place's length.
const MEMBER_BYTES: usize = 8 + 4 + 4;

/// What a written-off node asks, or what a member forwards for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request {
    /// A written-off node's own request, on its connection under its old certificate: a new
    /// identity for the key it has just made. Who asks is what its handshake proved, never
    /// anything the request names.
    Regrant { key: [u8; KEY] },
    /// A member's forward of a written-off node's request to the root range's leader, naming the
    /// old id, fingerprint and key its handshake with the node proved.
    Forward {
        old: u64,
        fingerprint: [u8; FINGERPRINT],
        old_key: [u8; KEY],
        key: [u8; KEY],
    },
}

/// Why a request was refused (membership.md §5.1 step 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    /// The cell's setting is `rejoin off`.
    #[error("the cell re-grants none: rejoin off")]
    Off,
    /// The member keeps no issuer.
    #[error("the member keeps no issuer")]
    Unissued,
    /// The ledger holds no entry for the old identity: banned and dropped, evicted, left in
    /// order, or never written off.
    #[error("the ledger holds no entry for this identity")]
    Unknown,
    /// A person banned the old identity.
    #[error("the identity is banned")]
    Banned,
    /// The old identity was re-granted to another key.
    #[error("the identity was re-granted to another key")]
    Regranted,
    /// The key asked for is the old certificate's.
    #[error("the key asked for is the old certificate's")]
    SameKey,
    /// The table has no room for one more node.
    #[error("the cell holds as many nodes as it states")]
    Full,
    /// The old member is not yet removed: its volumes may still hold a chunk no other copy holds.
    #[error("the old member is not yet removed")]
    NotRemoved,
    /// The issuer's grant the leader took was spent or passed before its re-grant applied.
    #[error("the issuer's grant was spent or passed")]
    Stale,
    /// The id the leader drew names a node the table or the ledger holds.
    #[error("the id drawn is taken")]
    Conflict,
    /// The root range did not answer within the member's patience, or no member led it.
    #[error("the root range did not answer in time")]
    Unanswered,
    /// The node the request was forwarded to does not lead the root range.
    #[error("the node asked does not lead the root range")]
    Unled,
}

const REFUSALS: [Refusal; 12] = [
    Refusal::Off,
    Refusal::Unissued,
    Refusal::Unknown,
    Refusal::Banned,
    Refusal::Regranted,
    Refusal::SameKey,
    Refusal::Full,
    Refusal::NotRemoved,
    Refusal::Stale,
    Refusal::Conflict,
    Refusal::Unanswered,
    Refusal::Unled,
];

impl Refusal {
    /// Its code on the wire.
    pub fn code(self) -> u8 {
        match self {
            Self::Off => 0,
            Self::Unissued => 1,
            Self::Unknown => 2,
            Self::Banned => 3,
            Self::Regranted => 4,
            Self::SameKey => 5,
            Self::Full => 6,
            Self::NotRemoved => 7,
            Self::Stale => 8,
            Self::Conflict => 9,
            Self::Unanswered => 10,
            Self::Unled => 11,
        }
    }

    /// The refusal `code` names, none for another.
    pub fn of(code: u8) -> Option<Self> {
        REFUSALS.into_iter().find(|r| r.code() == code)
    }
}

/// A member of the cell as an answer names it: its id and where it is dialed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub id: u64,
    /// Its QUIC endpoint's and its plane's places, `HOST:PORT`, at most [`MAX_REACH`] bytes each.
    pub quic: String,
    pub plane: String,
}

/// A new identity: the certificate the issuer signed, the issuer's own for its chain, and the
/// members the root range holds, which the node dials as its seeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Granted {
    pub certificate: Vec<u8>,
    pub issuer: Vec<u8>,
    pub members: Vec<Member>,
}

/// What a request is answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Granted(Granted),
    Refused(Refusal),
}

/// Why a message could not be written or read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EnrollError {
    #[error("a request's or an answer's bytes do not decode")]
    Malformed,
    #[error("a field of {0} bytes past what its length holds")]
    TooLarge(usize),
}

impl Request {
    /// Its bytes, the exchange's body, at their length.
    pub fn encode(&self) -> Vec<u8> {
        let written = mantle_codec::encode(0, |w| {
            match self {
                Self::Regrant { key } => {
                    w.u8(REGRANT);
                    w.bytes(key);
                }
                Self::Forward {
                    old,
                    fingerprint,
                    old_key,
                    key,
                } => {
                    w.u8(FORWARD);
                    w.u64(*old);
                    w.bytes(fingerprint);
                    w.bytes(old_key);
                    w.bytes(key);
                }
            }
            Ok::<_, EnrollError>(())
        });
        written.map(Writer::into_vec).unwrap_or_default()
    }

    /// The request `bytes` hold, every byte of them.
    pub fn decode(bytes: &[u8]) -> Result<Self, EnrollError> {
        let mut r = Reader::new(bytes);
        let request = match r.u8() {
            Some(REGRANT) => Self::Regrant {
                key: array(&mut r)?,
            },
            Some(FORWARD) => Self::Forward {
                old: r.u64().ok_or(EnrollError::Malformed)?,
                fingerprint: array(&mut r)?,
                old_key: array(&mut r)?,
                key: array(&mut r)?,
            },
            _ => return Err(EnrollError::Malformed),
        };
        if r.remaining() != 0 {
            return Err(EnrollError::Malformed);
        }
        Ok(request)
    }

    /// The key it asks for.
    pub fn key(&self) -> &[u8; KEY] {
        match self {
            Self::Regrant { key } | Self::Forward { key, .. } => key,
        }
    }
}

impl Answer {
    /// Its bytes, a reply's body, at their length: refused where a place is longer than
    /// [`MAX_REACH`] or a field longer than its u32 length holds.
    pub fn encode(&self) -> Result<Vec<u8>, EnrollError> {
        let written = mantle_codec::encode(0, |w| {
            match self {
                Self::Granted(g) => {
                    w.u8(GRANTED);
                    put_bytes(w, &g.certificate, usize::MAX)?;
                    put_bytes(w, &g.issuer, usize::MAX)?;
                    put_count(w, g.members.len())?;
                    for m in &g.members {
                        w.u64(m.id);
                        put_bytes(w, m.quic.as_bytes(), MAX_REACH)?;
                        put_bytes(w, m.plane.as_bytes(), MAX_REACH)?;
                    }
                }
                Self::Refused(refusal) => {
                    w.u8(REFUSED);
                    w.u8(refusal.code());
                }
            }
            Ok(())
        })?;
        Ok(written.into_vec())
    }

    /// The answer `bytes` hold, every byte of them.
    pub fn decode(bytes: &[u8]) -> Result<Self, EnrollError> {
        let mut r = Reader::new(bytes);
        let answer = match r.u8() {
            Some(GRANTED) => {
                let certificate = take_bytes(&mut r, usize::MAX)?.to_vec();
                let issuer = take_bytes(&mut r, usize::MAX)?.to_vec();
                let count = usize::try_from(r.u32().ok_or(EnrollError::Malformed)?)
                    .map_err(|_| EnrollError::Malformed)?;
                // A count past what the bytes left can hold is refused before anything is
                // reserved for it.
                if count > r.remaining().checked_div(MEMBER_BYTES).unwrap_or(0) {
                    return Err(EnrollError::Malformed);
                }
                let mut members = Vec::with_capacity(count);
                for _ in 0..count {
                    let id = r.u64().ok_or(EnrollError::Malformed)?;
                    let quic = take_text(&mut r)?;
                    let plane = take_text(&mut r)?;
                    members.push(Member { id, quic, plane });
                }
                Self::Granted(Granted {
                    certificate,
                    issuer,
                    members,
                })
            }
            Some(REFUSED) => {
                Self::Refused(r.u8().and_then(Refusal::of).ok_or(EnrollError::Malformed)?)
            }
            _ => return Err(EnrollError::Malformed),
        };
        if r.remaining() != 0 {
            return Err(EnrollError::Malformed);
        }
        Ok(answer)
    }
}

fn array<const N: usize>(r: &mut Reader<'_>) -> Result<[u8; N], EnrollError> {
    r.take(N)
        .and_then(|b| b.try_into().ok())
        .ok_or(EnrollError::Malformed)
}

fn put_count(w: &mut Writer, count: usize) -> Result<(), EnrollError> {
    w.u32(u32::try_from(count).map_err(|_| EnrollError::TooLarge(count))?);
    Ok(())
}

fn put_bytes(w: &mut Writer, bytes: &[u8], most: usize) -> Result<(), EnrollError> {
    if bytes.len() > most {
        return Err(EnrollError::TooLarge(bytes.len()));
    }
    put_count(w, bytes.len())?;
    w.bytes(bytes);
    Ok(())
}

fn take_bytes<'a>(r: &mut Reader<'a>, most: usize) -> Result<&'a [u8], EnrollError> {
    let len = usize::try_from(r.u32().ok_or(EnrollError::Malformed)?)
        .map_err(|_| EnrollError::Malformed)?;
    if len > most {
        return Err(EnrollError::Malformed);
    }
    r.take(len).ok_or(EnrollError::Malformed)
}

fn take_text(r: &mut Reader<'_>) -> Result<String, EnrollError> {
    let bytes = take_bytes(r, MAX_REACH)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| EnrollError::Malformed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn granted() -> Answer {
        Answer::Granted(Granted {
            certificate: vec![0x30, 0x82],
            issuer: vec![0x30],
            members: vec![Member {
                id: 7,
                quic: "h:1".into(),
                plane: "h:2".into(),
            }],
        })
    }

    fn every_answer() -> Vec<Answer> {
        let mut answers = vec![granted()];
        answers.extend(REFUSALS.into_iter().map(Answer::Refused));
        answers
    }

    fn every_request() -> [Request; 2] {
        [
            Request::Regrant { key: [2; KEY] },
            Request::Forward {
                old: 4,
                fingerprint: [1; FINGERPRINT],
                old_key: [3; KEY],
                key: [2; KEY],
            },
        ]
    }

    /// The wire as the module's table states it, byte for byte.
    #[test]
    fn golden_vectors() {
        let mut regrant = vec![0];
        regrant.extend([2; KEY]);
        assert_eq!(Request::Regrant { key: [2; KEY] }.encode(), regrant);
        let mut forward = vec![1, 4, 0, 0, 0, 0, 0, 0, 0];
        forward.extend([1; FINGERPRINT]);
        forward.extend([3; KEY]);
        forward.extend([2; KEY]);
        assert_eq!(every_request()[1].encode(), forward);
        assert_eq!(forward.len(), MAX_REQUEST);
        let granted_bytes = [
            0, // granted
            2, 0, 0, 0, 0x30, 0x82, // the certificate
            1, 0, 0, 0, 0x30, // the issuer's
            1, 0, 0, 0, // one member
            7, 0, 0, 0, 0, 0, 0, 0, // its id
            3, 0, 0, 0, b'h', b':', b'1', // its QUIC place
            3, 0, 0, 0, b'h', b':', b'2', // its plane's
        ];
        assert_eq!(granted().encode().unwrap(), granted_bytes);
        for (refusal, code) in REFUSALS.into_iter().zip(0u8..) {
            assert_eq!(Answer::Refused(refusal).encode().unwrap(), [1, code]);
        }
    }

    #[test]
    fn every_request_and_answer_reads_back() {
        for request in every_request() {
            assert_eq!(Request::decode(&request.encode()), Ok(request));
        }
        for answer in every_answer() {
            assert_eq!(Answer::decode(&answer.encode().unwrap()), Ok(answer));
        }
        assert_eq!(Refusal::of(12), None);
    }

    /// One buffer each, made at its length: a request and every answer are written with one
    /// allocation and no reallocation.
    #[test]
    fn every_request_and_answer_is_written_at_its_length() {
        use hyper_measure::alloc;
        for request in every_request() {
            alloc::begin();
            let encoded = request.encode();
            let c = alloc::end();
            assert!(!encoded.is_empty());
            assert_eq!((c.allocations, c.reallocations), (1, 0), "{request:?}");
        }
        for answer in every_answer() {
            alloc::begin();
            let encoded = answer.encode();
            let c = alloc::end();
            assert!(encoded.is_ok(), "{answer:?}");
            assert_eq!((c.allocations, c.reallocations), (1, 0), "{answer:?}");
        }
    }

    /// A cut message, one with bytes past its end, an unknown tag or code, a count past what
    /// the bytes hold and a place past [`MAX_REACH`] are refused.
    #[test]
    fn a_malformed_message_is_refused() {
        for request in every_request() {
            let bytes = request.encode();
            assert!(Request::decode(&bytes[..bytes.len() - 1]).is_err());
            let mut longer = bytes.clone();
            longer.push(0);
            assert!(Request::decode(&longer).is_err());
        }
        assert!(Request::decode(&[2]).is_err());
        assert!(Request::decode(&[]).is_err());
        let bytes = granted().encode().unwrap();
        assert!(Answer::decode(&bytes[..bytes.len() - 1]).is_err());
        assert!(Answer::decode(&[1, 12]).is_err());
        assert!(Answer::decode(&[2, 0]).is_err());
        // A count of members no bytes follow.
        let counted = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff];
        assert!(Answer::decode(&counted).is_err());
        let far = Answer::Granted(Granted {
            certificate: Vec::new(),
            issuer: Vec::new(),
            members: vec![Member {
                id: 1,
                quic: "h".repeat(MAX_REACH + 1),
                plane: String::new(),
            }],
        });
        assert_eq!(far.encode(), Err(EnrollError::TooLarge(MAX_REACH + 1)));
    }
}
