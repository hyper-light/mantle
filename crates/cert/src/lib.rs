//! X.509 certificates for a cell's authority and its nodes (docs/design/node.md §1.5, §3.6): every
//! connection between nodes is mutual TLS 1.3, each side presenting a certificate its cell's
//! authority signed, verified against that authority (rustls's WebPKI verifiers).
//!
//! The certificates are RFC 5280 v3 in DER (ITU-T X.690), their keys and signatures Ed25519 (RFC
//! 8410) from AWS-LC, which the node's TLS provider verifies. An authority is self-signed and may
//! sign certificates and nothing else (`keyUsage` keyCertSign and cRLSign, `basicConstraints`
//! cA, both critical: RFC 5280 §4.2.1.3, §4.2.1.9). A node's certificate names it by one DNS name
//! in its `subjectAltName`, which a dial verifies against the name the peer is known by
//! (RFC 5280 §4.2.1.6, RFC 6125); it is no authority, signs only (`digitalSignature`), and may
//! serve either end of a connection (`extKeyUsage` serverAuth and clientAuth, §4.2.1.12). A serial
//! is 16 octets, 126 bits of them the issuer's draw ([`Serial`]): positive and within the 20
//! octets RFC 5280 §4.1.2.2 allows, past the 64 random bits the CA/Browser Forum's Baseline
//! Requirements §7.1 ask, and of one length whatever its bits. A key pair and a serial are drawn
//! from the operating system's random source, or derived by the caller (a simulation, whose run
//! is its seed and whose handshakes compress the certificates they carry, RFC 8879: the same
//! bytes, the same sizes). A certificate is read back only for a start's check that its cell's
//! authority issued it ([`issued_by`], RFC 5280 §6.1.3 (a)), so a directory holding another cell's
//! certificates refuses to start rather than failing each handshake; the TLS stack verifies what
//! peers present.

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]

mod der;
mod time;

use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use aws_lc_rs::signature::{ED25519, Ed25519KeyPair, KeyPair as _, UnparsedPublicKey};

use der::{nested, oid, tlv};

/// What an issue refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CertError {
    /// The operating system's random source refused bytes for a key or a serial.
    #[error("no random bytes from the operating system")]
    Random,
    /// A key that is not an Ed25519 key in PKCS#8, or one AWS-LC refused to make.
    #[error("not an Ed25519 key in PKCS#8")]
    Key,
    /// A validity whose end is not after its start, or a time RFC 5280 cannot state (before 1950,
    /// after 9999).
    #[error("a validity RFC 5280 cannot state")]
    Time,
    /// A name that is empty or not a DNS name a certificate may carry.
    #[error("a name a certificate cannot carry")]
    Name,
    /// A length or value DER cannot hold on this machine.
    #[error("a value DER cannot encode")]
    Encoding,
    /// Bytes that are not a certificate of this profile in DER: an Ed25519 v3 certificate whose
    /// times are RFC 5280's forms.
    #[error("not a certificate of this profile")]
    Malformed,
    /// A certificate whose issuer is not the authority's subject (RFC 5280 §6.1.3 (a)(4)).
    #[error("issued by another authority than the cell's")]
    Issuer,
    /// A certificate whose signature does not verify under the authority's key (RFC 5280 §6.1.3
    /// (a)(1)).
    #[error("not signed by the cell's authority's key")]
    Signature,
    /// A certificate whose validity does not hold the time it is checked at (RFC 5280 §6.1.3
    /// (a)(2)).
    #[error("outside its validity by this machine's clock")]
    Validity,
}

/// An Ed25519 key pair, kept with its PKCS#8 encoding (RFC 5958), the form a node stores its key in
/// and its TLS stack loads.
pub struct KeyPair {
    key: Ed25519KeyPair,
    pkcs8: Vec<u8>,
}

impl std::fmt::Debug for KeyPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyPair")
            .field("public", &self.public_key())
            .finish_non_exhaustive()
    }
}

impl KeyPair {
    /// A new key pair from AWS-LC, seeded from the operating system's random source.
    pub fn generate() -> Result<Self, CertError> {
        let document =
            Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).map_err(|_| CertError::Random)?;
        Self::from_pkcs8(document.as_ref())
    }

    /// The key pair whose private key is `seed` (RFC 8032 §5.1.5: an Ed25519 private key is 32
    /// octets, the seed its public key and every signature derive from): one its caller derived.
    pub fn from_seed(seed: &[u8; 32]) -> Result<Self, CertError> {
        let key = Ed25519KeyPair::from_seed_unchecked(seed).map_err(|_| CertError::Key)?;
        let document = key.to_pkcs8().map_err(|_| CertError::Key)?;
        Self::from_pkcs8(document.as_ref())
    }

    /// The key pair a PKCS#8 document holds.
    pub fn from_pkcs8(pkcs8: &[u8]) -> Result<Self, CertError> {
        let key = Ed25519KeyPair::from_pkcs8(pkcs8).map_err(|_| CertError::Key)?;
        Ok(Self {
            key,
            pkcs8: pkcs8.to_vec(),
        })
    }

    /// The PKCS#8 document of the private key.
    pub fn pkcs8(&self) -> &[u8] {
        &self.pkcs8
    }

    /// The 32-byte public key (RFC 8032 §5.1.5).
    pub fn public_key(&self) -> &[u8] {
        self.key.public_key().as_ref()
    }
}

/// The span a certificate is valid over, in seconds since 1970-01-01T00:00:00Z, both ends
/// included (RFC 5280 §4.1.2.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Validity {
    pub not_before: u64,
    pub not_after: u64,
}

/// A certificate's serial: 16 octets, its first octet's top bit cleared, so the integer is
/// positive with no octet before it, and the next bit set, so none of its octets is a leading zero
/// dropped (X.690 §8.3.2): every serial encodes as 16 content octets, the other 126 bits a draw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Serial([u8; 16]);

impl Serial {
    /// A serial of 126 bits from the operating system's random source.
    pub fn draw() -> Result<Self, CertError> {
        let mut random = [0u8; 16];
        SystemRandom::new()
            .fill(&mut random)
            .map_err(|_| CertError::Random)?;
        Ok(Self::from_octets(random))
    }

    /// The serial of `random`, 16 octets its caller drew or derived: all but its first octet's two
    /// top bits.
    pub fn from_octets(mut random: [u8; 16]) -> Self {
        if let Some(first) = random.first_mut() {
            *first = (*first & 0x7f) | 0x40;
        }
        Self(random)
    }
}

/// A self-signed authority's certificate: `name` its subject and issuer, `key` its own.
pub fn authority(
    key: &KeyPair,
    name: &str,
    validity: Validity,
    serial: Serial,
) -> Result<Vec<u8>, CertError> {
    check_name(name)?;
    issue(
        key,
        name,
        &Subject {
            name,
            public_key: key.public_key(),
        },
        (validity, serial),
        Role::Authority,
    )
}

/// A node's certificate: issued by the authority holding `authority_key` and named
/// `authority_name` (its certificate's subject), for the node whose public key is `public_key`,
/// known to its peers by the DNS name `dns_name`.
pub fn node(
    authority_key: &KeyPair,
    authority_name: &str,
    public_key: &[u8],
    dns_name: &str,
    (validity, serial): (Validity, Serial),
) -> Result<Vec<u8>, CertError> {
    check_name(authority_name)?;
    check_dns_name(dns_name)?;
    if public_key.len() != 32 {
        return Err(CertError::Key);
    }
    issue(
        authority_key,
        authority_name,
        &Subject {
            name: dns_name,
            public_key,
        },
        (validity, serial),
        Role::Node { dns_name },
    )
}

/// A name for a subject's common name: non-empty, and short enough for X.520's `ub-common-name`
/// of 64 characters (RFC 5280 Appendix A.1).
fn check_name(name: &str) -> Result<(), CertError> {
    if name.is_empty() || name.chars().count() > 64 {
        return Err(CertError::Name);
    }
    Ok(())
}

/// A DNS name a `subjectAltName` may carry (RFC 5280 §4.2.1.6, RFC 1034 §3.5's preferred
/// syntax): labels of letters, digits and hyphens, each 1 to 63 characters, not starting or ending
/// with a hyphen, at most 253 characters in all.
fn check_dns_name(name: &str) -> Result<(), CertError> {
    if name.is_empty() || name.len() > 253 {
        return Err(CertError::Name);
    }
    for label in name.split('.') {
        let bytes = label.as_bytes();
        let ok = !bytes.is_empty()
            && bytes.len() <= 63
            && bytes
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || *b == b'-')
            && bytes.first() != Some(&b'-')
            && bytes.last() != Some(&b'-');
        if !ok {
            return Err(CertError::Name);
        }
    }
    Ok(())
}

struct Subject<'a> {
    name: &'a str,
    public_key: &'a [u8],
}

enum Role<'a> {
    Authority,
    Node { dns_name: &'a str },
}

/// The certificate RFC 5280 §4.1 lays out: the TBS certificate, the signature algorithm, and the
/// issuer's signature over the TBS certificate's DER.
fn issue(
    issuer_key: &KeyPair,
    issuer_name: &str,
    subject: &Subject<'_>,
    (validity, serial): (Validity, Serial),
    role: Role<'_>,
) -> Result<Vec<u8>, CertError> {
    if validity.not_after <= validity.not_before {
        return Err(CertError::Time);
    }
    let tbs = tbs_certificate(&serial.0, issuer_name, subject, validity, &role)?;
    let signature = issuer_key.key.sign(&tbs);
    let mut out = Vec::new();
    nested(&mut out, der::SEQUENCE, |c| {
        c.extend_from_slice(&tbs);
        ed25519_algorithm(c)?;
        der::bits(c, 0, signature.as_ref())
    })?;
    Ok(out)
}

fn tbs_certificate(
    serial: &[u8],
    issuer_name: &str,
    subject: &Subject<'_>,
    validity: Validity,
    role: &Role<'_>,
) -> Result<Vec<u8>, CertError> {
    let mut out = Vec::new();
    nested(&mut out, der::SEQUENCE, |t| {
        // version: v3 (2), RFC 5280 §4.1.2.1, since the certificate carries extensions.
        nested(t, der::EXPLICIT_0, |v| der::unsigned(v, &[2]))?;
        der::unsigned(t, serial)?;
        ed25519_algorithm(t)?;
        name(t, issuer_name)?;
        nested(t, der::SEQUENCE, |v| {
            time::encode(v, validity.not_before)?;
            time::encode(v, validity.not_after)
        })?;
        name(t, subject.name)?;
        nested(t, der::SEQUENCE, |k| {
            ed25519_algorithm(k)?;
            der::bits(k, 0, subject.public_key)
        })?;
        nested(t, der::EXPLICIT_3, |x| {
            nested(x, der::SEQUENCE, |list| extensions(list, role))
        })
    })?;
    Ok(out)
}

/// id-Ed25519 with its parameters absent, as RFC 8410 §3 requires.
fn ed25519_algorithm(out: &mut Vec<u8>) -> Result<(), CertError> {
    nested(out, der::SEQUENCE, |a| tlv(a, der::OID, oid::ED25519))
}

/// A Name of one relative distinguished name, its common name as a UTF8String (RFC 5280
/// §4.1.2.4 for new certificates). The issuer's is encoded the same way as its own certificate's
/// subject, so a verifier's byte comparison of the two holds.
fn name(out: &mut Vec<u8>, common_name: &str) -> Result<(), CertError> {
    nested(out, der::SEQUENCE, |n| {
        nested(n, der::SET, |rdn| {
            nested(rdn, der::SEQUENCE, |atv| {
                tlv(atv, der::OID, oid::COMMON_NAME)?;
                tlv(atv, der::UTF8_STRING, common_name.as_bytes())
            })
        })
    })
}

/// One extension: its identifier, `critical` only where true (DER omits a default), and its
/// value's DER inside an OCTET STRING (RFC 5280 §4.1).
fn extension(
    out: &mut Vec<u8>,
    id: &[u8],
    critical: bool,
    value: impl FnOnce(&mut Vec<u8>) -> Result<(), CertError>,
) -> Result<(), CertError> {
    nested(out, der::SEQUENCE, |e| {
        tlv(e, der::OID, id)?;
        if critical {
            tlv(e, der::BOOLEAN, &[0xff])?;
        }
        let mut inner = Vec::new();
        value(&mut inner)?;
        tlv(e, der::OCTET_STRING, &inner)
    })
}

/// The `keyUsage` bits (RFC 5280 §4.2.1.3) as a named-bit BIT STRING: trailing zero bits dropped
/// and counted as unused (X.690 §11.2.2).
const DIGITAL_SIGNATURE: (u8, u8) = (7, 0x80);
const KEY_CERT_SIGN_AND_CRL_SIGN: (u8, u8) = (1, 0x06);

fn extensions(out: &mut Vec<u8>, role: &Role<'_>) -> Result<(), CertError> {
    match role {
        Role::Authority => {
            extension(out, oid::BASIC_CONSTRAINTS, true, |v| {
                nested(v, der::SEQUENCE, |b| tlv(b, der::BOOLEAN, &[0xff]))
            })?;
            extension(out, oid::KEY_USAGE, true, |v| {
                der::bits(
                    v,
                    KEY_CERT_SIGN_AND_CRL_SIGN.0,
                    &[KEY_CERT_SIGN_AND_CRL_SIGN.1],
                )
            })
        }
        Role::Node { dns_name } => {
            // cA FALSE is the default, which DER leaves out: an empty sequence.
            extension(out, oid::BASIC_CONSTRAINTS, true, |v| {
                tlv(v, der::SEQUENCE, &[])
            })?;
            extension(out, oid::KEY_USAGE, true, |v| {
                der::bits(v, DIGITAL_SIGNATURE.0, &[DIGITAL_SIGNATURE.1])
            })?;
            extension(out, oid::EXT_KEY_USAGE, false, |v| {
                nested(v, der::SEQUENCE, |k| {
                    tlv(k, der::OID, oid::SERVER_AUTH)?;
                    tlv(k, der::OID, oid::CLIENT_AUTH)
                })
            })?;
            extension(out, oid::SUBJECT_ALT_NAME, false, |v| {
                nested(v, der::SEQUENCE, |names| {
                    tlv(names, der::DNS_NAME, dns_name.as_bytes())
                })
            })
        }
    }
}

/// What a check of a certificate's issuer reads from it (RFC 5280 §4.1).
struct Certificate<'a> {
    /// The TBS certificate's DER, which its issuer signed.
    tbs: &'a [u8],
    /// The issuer's and the subject's Name, each its DER whole.
    issuer: &'a [u8],
    subject: &'a [u8],
    validity: Validity,
    /// The subject's Ed25519 public key, and the issuer's Ed25519 signature over `tbs`.
    public_key: &'a [u8],
    signature: &'a [u8],
}

/// The certificate in `der`, as this profile writes one: v3, Ed25519 throughout (RFC 8410), its
/// times in RFC 5280's forms; the extensions after its key are not read, the TLS stack checking a
/// peer's at its handshake. Anything else, and anything after the certificate, is refused.
fn read(der: &[u8]) -> Result<Certificate<'_>, CertError> {
    let mut input = der;
    let mut certificate = der::expect(&mut input, der::SEQUENCE)?.content;
    if !input.is_empty() {
        return Err(CertError::Malformed);
    }
    let tbs = der::expect(&mut certificate, der::SEQUENCE)?;
    ed25519_identifier(&mut certificate)?;
    // An Ed25519 signature is 64 octets and a public key 32 (RFC 8032 §5.1.6, §5.1.5).
    let signature = whole_octets(der::expect(&mut certificate, der::BIT_STRING)?.content, 64)?;
    if !certificate.is_empty() {
        return Err(CertError::Malformed);
    }
    let mut fields = tbs.content;
    // v3 (2), RFC 5280 §4.1.2.1.
    if der::expect(&mut fields, der::EXPLICIT_0)?.content != [der::INTEGER, 1, 2] {
        return Err(CertError::Malformed);
    }
    der::expect(&mut fields, der::INTEGER)?;
    ed25519_identifier(&mut fields)?;
    let issuer = der::expect(&mut fields, der::SEQUENCE)?.whole;
    let mut span = der::expect(&mut fields, der::SEQUENCE)?.content;
    let not_before = time::decode(&der::next(&mut span)?)?;
    let not_after = time::decode(&der::next(&mut span)?)?;
    if !span.is_empty() {
        return Err(CertError::Malformed);
    }
    let subject = der::expect(&mut fields, der::SEQUENCE)?.whole;
    let mut key = der::expect(&mut fields, der::SEQUENCE)?.content;
    ed25519_identifier(&mut key)?;
    let public_key = whole_octets(der::expect(&mut key, der::BIT_STRING)?.content, 32)?;
    if !key.is_empty() {
        return Err(CertError::Malformed);
    }
    Ok(Certificate {
        tbs: tbs.whole,
        issuer,
        subject,
        validity: Validity {
            not_before,
            not_after,
        },
        public_key,
        signature,
    })
}

/// id-Ed25519's AlgorithmIdentifier at the front of `input`, its parameters absent (RFC 8410 §3).
fn ed25519_identifier(input: &mut &[u8]) -> Result<(), CertError> {
    let mut algorithm = der::expect(input, der::SEQUENCE)?.content;
    if der::expect(&mut algorithm, der::OID)?.content != oid::ED25519 || !algorithm.is_empty() {
        return Err(CertError::Malformed);
    }
    Ok(())
}

/// A BIT STRING's content holding `octets` whole octets, no bit unused (X.690 §8.6.2).
fn whole_octets(content: &[u8], octets: usize) -> Result<&[u8], CertError> {
    match content.split_first() {
        Some((0, bits)) if bits.len() == octets => Ok(bits),
        _ => Err(CertError::Malformed),
    }
}

/// Whether the authority whose certificate is `authority` issued `certificate`, and it is valid
/// `now` seconds after the Unix epoch: RFC 5280 §6.1.3 (a)'s checks of a path's one certificate,
/// the authority its trust anchor (§6.1.1 (d)), whose subject and key are the working issuer name
/// and key: the certificate's issuer is the authority's subject (a)(4), its signature verifies
/// under the authority's key (a)(1), and its validity holds `now` (a)(2). Names compare byte for
/// byte, as this profile encodes an issuer's name the same as its certificate's subject.
pub fn issued_by(certificate: &[u8], authority: &[u8], now: u64) -> Result<(), CertError> {
    let issued = read(certificate)?;
    let anchor = read(authority)?;
    if issued.issuer != anchor.subject {
        return Err(CertError::Issuer);
    }
    UnparsedPublicKey::new(&ED25519, anchor.public_key)
        .verify(issued.tbs, issued.signature)
        .map_err(|_| CertError::Signature)?;
    if now < issued.validity.not_before || now > issued.validity.not_after {
        return Err(CertError::Validity);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper_tls::RootCertStore;
    use hyper_tls::client::WebPkiServerVerifier;
    use hyper_tls::client::danger::ServerCertVerifier;
    use hyper_tls::pki_types::{CertificateDer, ServerName, UnixTime};
    use hyper_tls::server::WebPkiClientVerifier;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn day_around_now() -> Validity {
        Validity {
            not_before: now() - 3_600,
            not_after: now() + 86_400,
        }
    }

    /// A serial encodes as 16 content octets whatever its random bits: the draws that made one
    /// longer (a top bit set, which takes a sign octet) or shorter (a leading zero, dropped).
    #[test]
    fn every_serial_encodes_in_sixteen_octets() {
        let mut leading_zero = [0x5au8; 16];
        leading_zero[0] = 0;
        for random in [[0u8; 16], [0xff; 16], [0x80; 16], leading_zero] {
            let mut out = Vec::new();
            der::unsigned(&mut out, &Serial::from_octets(random).0).unwrap();
            // The INTEGER's tag, its length and its content.
            assert_eq!(out.len(), 2 + 16, "{random:?}");
        }
    }

    fn roots(authority: &[u8]) -> RootCertStore {
        let mut roots = RootCertStore::empty();
        roots.add(CertificateDer::from(authority.to_vec())).unwrap();
        roots
    }

    fn as_server(authority: &[u8], node: &[u8], name: &str) -> Result<(), hyper_tls::Error> {
        let provider = hyper_tls::crypto::aws_lc_rs::default_provider();
        let verifier = WebPkiServerVerifier::builder_with_provider(roots(authority), &provider)
            .build()
            .unwrap();
        verifier
            .verify_server_cert(
                &CertificateDer::from(node.to_vec()),
                &[],
                &ServerName::try_from(name.to_string()).unwrap(),
                &[],
                UnixTime::now(),
            )
            .map(|_| ())
    }

    fn as_client(authority: &[u8], node: &[u8]) -> Result<(), hyper_tls::Error> {
        let provider = hyper_tls::crypto::aws_lc_rs::default_provider();
        let verifier = WebPkiClientVerifier::builder_with_provider(roots(authority), &provider)
            .build()
            .unwrap();
        verifier
            .verify_client_cert(&CertificateDer::from(node.to_vec()), &[], UnixTime::now())
            .map(|_| ())
    }

    /// A cell's authority and two of its nodes: each node's certificate verifies under the
    /// authority at both ends of a connection, by its own name and by no other.
    #[test]
    fn a_node_certificate_verifies_under_its_authority_as_server_and_client() {
        let authority_key = KeyPair::generate().unwrap();
        let ca = authority(
            &authority_key,
            "mantle cell authority",
            day_around_now(),
            Serial::draw().unwrap(),
        )
        .unwrap();
        for id in [1u64, 2] {
            let node_key = KeyPair::generate().unwrap();
            let name = format!("node-{id}");
            let cert = node(
                &authority_key,
                "mantle cell authority",
                node_key.public_key(),
                &name,
                (day_around_now(), Serial::draw().unwrap()),
            )
            .unwrap();
            as_server(&ca, &cert, &name).unwrap();
            as_client(&ca, &cert).unwrap();
            assert!(
                as_server(&ca, &cert, "node-99").is_err(),
                "verified under another node's name"
            );
        }
    }

    #[test]
    fn a_certificate_from_another_authority_is_refused() {
        let ours = KeyPair::generate().unwrap();
        let theirs = KeyPair::generate().unwrap();
        let ca = authority(&ours, "cell", day_around_now(), Serial::draw().unwrap()).unwrap();
        let node_key = KeyPair::generate().unwrap();
        let foreign = node(
            &theirs,
            "cell",
            node_key.public_key(),
            "node-1",
            (day_around_now(), Serial::draw().unwrap()),
        )
        .unwrap();
        assert!(as_server(&ca, &foreign, "node-1").is_err());
        assert!(as_client(&ca, &foreign).is_err());
        // The start's check refuses it too: the same name, another key.
        assert_eq!(issued_by(&foreign, &ca, now()), Err(CertError::Signature));
    }

    #[test]
    fn an_expired_certificate_or_one_not_yet_valid_is_refused() {
        let authority_key = KeyPair::generate().unwrap();
        let ca = authority(
            &authority_key,
            "cell",
            day_around_now(),
            Serial::draw().unwrap(),
        )
        .unwrap();
        let node_key = KeyPair::generate().unwrap();
        for validity in [
            Validity {
                not_before: now() - 7_200,
                not_after: now() - 3_600,
            },
            Validity {
                not_before: now() + 3_600,
                not_after: now() + 7_200,
            },
        ] {
            let cert = node(
                &authority_key,
                "cell",
                node_key.public_key(),
                "node-1",
                (validity, Serial::draw().unwrap()),
            )
            .unwrap();
            assert!(as_server(&ca, &cert, "node-1").is_err(), "{validity:?}");
            assert_eq!(
                issued_by(&cert, &ca, now()),
                Err(CertError::Validity),
                "{validity:?}"
            );
        }
    }

    /// A start's check of its certificate holds for the authority that issued it, at each end of its
    /// validity, and for that authority's own certificate; it refuses another authority by name or,
    /// of the same name, by its key, a time outside the validity, each octet of the signature
    /// flipped and each octet before it flipped, a byte past the certificate, and every truncation
    /// of either certificate, each typed and none panicking.
    #[test]
    fn a_certificate_is_its_authoritys_alone_and_every_other_byte_is_refused() {
        let ours = KeyPair::generate().unwrap();
        let span = day_around_now();
        let ca = authority(&ours, "mantle cell 1", span, Serial::draw().unwrap()).unwrap();
        let node_key = KeyPair::generate().unwrap();
        let cert = node(
            &ours,
            "mantle cell 1",
            node_key.public_key(),
            "node-1",
            (span, Serial::draw().unwrap()),
        )
        .unwrap();
        issued_by(&cert, &ca, now()).unwrap();
        issued_by(&cert, &ca, span.not_before).unwrap();
        issued_by(&cert, &ca, span.not_after).unwrap();
        issued_by(&ca, &ca, now()).unwrap();
        let theirs = KeyPair::generate().unwrap();
        let named = authority(&theirs, "mantle cell 2", span, Serial::draw().unwrap()).unwrap();
        assert_eq!(issued_by(&cert, &named, now()), Err(CertError::Issuer));
        let same = authority(&theirs, "mantle cell 1", span, Serial::draw().unwrap()).unwrap();
        assert_eq!(issued_by(&cert, &same, now()), Err(CertError::Signature));
        assert_eq!(
            issued_by(&cert, &ca, span.not_before - 1),
            Err(CertError::Validity)
        );
        assert_eq!(
            issued_by(&cert, &ca, span.not_after + 1),
            Err(CertError::Validity)
        );
        let signed = cert.len() - 64;
        for at in 0..cert.len() {
            let mut flipped = cert.clone();
            flipped[at] ^= 0x01;
            let checked = issued_by(&flipped, &ca, now());
            if at >= signed {
                assert_eq!(checked, Err(CertError::Signature), "signature octet {at}");
            } else {
                assert!(checked.is_err(), "octet {at} flipped");
            }
        }
        let mut longer = cert.clone();
        longer.push(0);
        assert_eq!(issued_by(&longer, &ca, now()), Err(CertError::Malformed));
        for len in 0..cert.len() {
            assert_eq!(
                issued_by(&cert[..len], &ca, now()),
                Err(CertError::Malformed),
                "{len} of the certificate"
            );
        }
        for len in 0..ca.len() {
            assert_eq!(
                issued_by(&cert, &ca[..len], now()),
                Err(CertError::Malformed),
                "{len} of the authority"
            );
        }
    }

    #[test]
    fn what_cannot_be_stated_is_refused() {
        let key = KeyPair::generate().unwrap();
        let backwards = Validity {
            not_before: now(),
            not_after: now(),
        };
        assert_eq!(
            authority(&key, "cell", backwards, Serial::draw().unwrap()),
            Err(CertError::Time)
        );
        assert_eq!(
            authority(&key, "", day_around_now(), Serial::draw().unwrap()),
            Err(CertError::Name)
        );
        let long = "x".repeat(64);
        for bad in ["", "-node", "node-", "no de", "a..b", long.as_str()] {
            assert_eq!(
                node(
                    &key,
                    "cell",
                    key.public_key(),
                    bad,
                    (day_around_now(), Serial::draw().unwrap())
                ),
                Err(CertError::Name),
                "{bad:?}"
            );
        }
        assert_eq!(
            node(
                &key,
                "cell",
                &[0; 31],
                "node-1",
                (day_around_now(), Serial::draw().unwrap())
            ),
            Err(CertError::Key)
        );
    }

    /// A key pair from a seed, and a certificate issued with a serial its caller gave, are the same
    /// bytes every time: what a simulation's run, its seed, rests on. The certificate verifies.
    #[test]
    fn a_seed_and_a_given_serial_issue_the_same_certificate() {
        let seed = [7u8; 32];
        let (a, b) = (
            KeyPair::from_seed(&seed).unwrap(),
            KeyPair::from_seed(&seed).unwrap(),
        );
        assert_eq!(a.pkcs8(), b.pkcs8());
        let issued = (day_around_now(), Serial::from_octets([9; 16]));
        let ca = authority(&a, "cell", issued.0, issued.1).unwrap();
        assert_eq!(ca, authority(&b, "cell", issued.0, issued.1).unwrap());
        let node_key = KeyPair::from_seed(&[8; 32]).unwrap();
        let first = node(&a, "cell", node_key.public_key(), "node-1", issued).unwrap();
        assert_eq!(
            first,
            node(&b, "cell", node_key.public_key(), "node-1", issued).unwrap()
        );
        as_server(&ca, &first, "node-1").unwrap();
    }

    #[test]
    fn a_key_survives_its_pkcs8_document() {
        let key = KeyPair::generate().unwrap();
        let again = KeyPair::from_pkcs8(key.pkcs8()).unwrap();
        assert_eq!(key.public_key(), again.public_key());
        assert!(KeyPair::from_pkcs8(&[0x30, 0x00]).is_err());
    }
}
