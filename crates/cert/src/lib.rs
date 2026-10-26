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
//! serve either end of a connection (`extKeyUsage` serverAuth and clientAuth, §4.2.1.12). A client
//! of the native protocol's certificate names it by its subject's common name and the account it
//! acts for by its subject's organization (RFC 5280 §4.1.2.6), and serves a connection's client end
//! alone (`extKeyUsage` clientAuth): a TLS server verifies a client's certificate for clientAuth and
//! a client a server's for serverAuth (RFC 5280 §4.2.1.12), so a client's never serves as a node's.
//! [`client_names`] reads those names back from a certificate of exactly that profile. A serial
//! is 16 octets, 126 bits of them the issuer's draw ([`Serial`]): positive and within the 20
//! octets RFC 5280 §4.1.2.2 allows, past the 64 random bits the CA/Browser Forum's Baseline
//! Requirements §7.1 ask, and of one length whatever its bits. A key pair and a serial are drawn
//! from the operating system's random source, or derived by the caller (a simulation, whose run
//! is its seed and whose handshakes compress the certificates they carry, RFC 8879: the same
//! bytes, the same sizes). A certificate is read back for a start's check that its cell's
//! authority issued it ([`issued_by`], RFC 5280 §6.1.3 (a)), so a directory holding another cell's
//! certificates refuses to start rather than failing each handshake, and for a client's names; the
//! TLS stack verifies what peers present.
//!
//! The S3 endpoint's certificates are stock clients' to verify, through their platforms' TLS
//! stacks, and are ECDSA P-256 with SHA-256 throughout (RFC 5480, RFC 5758 §3.2): the one scheme
//! every TLS 1.3 implementation must verify (RFC 8446 §9.1), where the AWS CLI's transfer client
//! verifies no Ed25519 certificate on Linux, macOS or Windows (research/71). Their authority is
//! the endpoint's own, beside the cell's, so no Ed25519 signature is in their chain. They carry
//! what a verifier holding to RFC 5280 asks (Python's default since 3.13): each its key's
//! identifier and an issued one its issuer's (§4.2.1.1, §4.2.1.2, RFC 7093 §2's first method). An
//! endpoint's certificate names every DNS name and address its clients dial, serves the server's
//! end alone (`extKeyUsage` serverAuth), and is valid for at most 825 days, past which Apple's
//! platforms refuse a server's certificate ([`MAX_ENDPOINT_VALIDITY`]). [`pem`] writes a
//! certificate as a client's CA bundle takes it (RFC 7468).

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

use std::net::IpAddr;

use aws_lc_rs::digest::{SHA256, digest};
use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use aws_lc_rs::signature::{
    ECDSA_P256_SHA256_ASN1_SIGNING, ED25519, EcdsaKeyPair, Ed25519KeyPair, KeyPair as _, Signature,
    UnparsedPublicKey,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;

use der::{nested, oid, tlv};

/// What an issue refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CertError {
    /// The operating system's random source refused bytes for a key or a serial.
    #[error("no random bytes from the operating system")]
    Random,
    /// A key that is not one of its kind in PKCS#8, Ed25519 for the cell and P-256 for the S3
    /// endpoint, or one AWS-LC refused to make or to sign with.
    #[error("not a key of its kind in PKCS#8")]
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

/// A P-256 key pair of the S3 endpoint's, signing with ECDSA and SHA-256, kept with its PKCS#8
/// encoding (RFC 5958), the form the endpoint's TLS stack loads.
pub struct EndpointKey {
    key: EcdsaKeyPair,
    pkcs8: Vec<u8>,
}

impl std::fmt::Debug for EndpointKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EndpointKey")
            .field("public", &self.public_key())
            .finish_non_exhaustive()
    }
}

impl EndpointKey {
    /// A new key pair from AWS-LC, drawn from the operating system's random source.
    pub fn generate() -> Result<Self, CertError> {
        let document =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &SystemRandom::new())
                .map_err(|_| CertError::Random)?;
        Self::from_pkcs8(document.as_ref())
    }

    /// The key pair a PKCS#8 document holds.
    pub fn from_pkcs8(pkcs8: &[u8]) -> Result<Self, CertError> {
        let key = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, pkcs8)
            .map_err(|_| CertError::Key)?;
        Ok(Self {
            key,
            pkcs8: pkcs8.to_vec(),
        })
    }

    /// The PKCS#8 document of the private key.
    pub fn pkcs8(&self) -> &[u8] {
        &self.pkcs8
    }

    /// The public key, its uncompressed point: 0x04, then its x and y coordinates, 32 octets each
    /// (SEC 1 §2.3.3; RFC 5480 §2.2).
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
        Signer::Ed25519(key),
        name,
        &Subject {
            name,
            key: SubjectKey::Ed25519(key.public_key()),
            organization: None,
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
        Signer::Ed25519(authority_key),
        authority_name,
        &Subject {
            name: dns_name,
            key: SubjectKey::Ed25519(public_key),
            organization: None,
        },
        (validity, serial),
        Role::Node { dns_name },
    )
}

/// A client's certificate: issued by the authority holding `authority_key` and named
/// `authority_name`, for the client whose public key is `public_key`, named `name` (its subject's
/// common name) and acting for `account` (its subject's organization), each within X.520's 64
/// characters (RFC 5280 Appendix A.1, `ub-common-name` and `ub-organization-name`).
pub fn client(
    authority_key: &KeyPair,
    authority_name: &str,
    public_key: &[u8],
    (name, account): (&str, &str),
    (validity, serial): (Validity, Serial),
) -> Result<Vec<u8>, CertError> {
    check_name(authority_name)?;
    check_name(name)?;
    check_name(account)?;
    if public_key.len() != 32 {
        return Err(CertError::Key);
    }
    issue(
        Signer::Ed25519(authority_key),
        authority_name,
        &Subject {
            name,
            key: SubjectKey::Ed25519(public_key),
            organization: Some(account),
        },
        (validity, serial),
        Role::Client,
    )
}

/// What a client's certificate names ([`client`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientNames<'a> {
    /// Its subject's common name.
    pub name: &'a str,
    /// The account it acts for, its subject's organization.
    pub account: &'a str,
}

/// The names a client's certificate gives, where `certificate` is one of exactly this profile's:
/// its subject an organization and a common name, and its extensions a client's alone (no CA, a
/// signer of handshakes, `extKeyUsage` clientAuth and nothing else), so a node's certificate, which
/// may serve a connection's server end, names no client. Who issued it is not checked here: the TLS
/// stack verified the chain at the handshake that presented it.
pub fn client_names(certificate: &[u8]) -> Result<ClientNames<'_>, CertError> {
    let read = read(certificate)?;
    let mut profile = Vec::new();
    extensions(&mut profile, &Role::Client)?;
    if read.extensions != profile.as_slice() {
        return Err(CertError::Malformed);
    }
    let mut subject = der::expect(&mut &*read.subject, der::SEQUENCE)?.content;
    let account = attribute(&mut subject, oid::ORGANIZATION_NAME)?;
    let name = attribute(&mut subject, oid::COMMON_NAME)?;
    if !subject.is_empty() {
        return Err(CertError::Malformed);
    }
    Ok(ClientNames { name, account })
}

/// A relative distinguished name of one attribute at the front of `rdns`, the attribute `id` as a
/// UTF8String (RFC 5280 §4.1.2.4), its value.
fn attribute<'a>(rdns: &mut &'a [u8], id: &[u8]) -> Result<&'a str, CertError> {
    let mut rdn = der::expect(rdns, der::SET)?.content;
    let mut atv = der::expect(&mut rdn, der::SEQUENCE)?.content;
    if !rdn.is_empty() || der::expect(&mut atv, der::OID)?.content != id {
        return Err(CertError::Malformed);
    }
    let value = der::expect(&mut atv, der::UTF8_STRING)?.content;
    if !atv.is_empty() {
        return Err(CertError::Malformed);
    }
    std::str::from_utf8(value).map_err(|_| CertError::Malformed)
}

/// The longest an S3 endpoint's certificate is valid, in seconds: 825 days of 86,400, past which
/// Apple's platforms refuse a TLS server's certificate ("TLS server certificates must have a
/// validity period of 825 days or fewer", support.apple.com/103769; research/71).
pub const MAX_ENDPOINT_VALIDITY: u64 = 71_280_000;

/// A name an S3 endpoint's clients dial it by, as its certificate's `subjectAltName` states it
/// (RFC 5280 §4.2.1.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointName<'a> {
    Dns(&'a str),
    Address(IpAddr),
}

/// The S3 endpoint's self-signed authority: `name` its subject and issuer, `key` its own, with its
/// key's identifier (RFC 5280 §4.2.1.2).
pub fn endpoint_authority(
    key: &EndpointKey,
    name: &str,
    validity: Validity,
    serial: Serial,
) -> Result<Vec<u8>, CertError> {
    check_name(name)?;
    let public_key = point(key.public_key())?;
    issue(
        Signer::P256(key),
        name,
        &Subject {
            name,
            key: SubjectKey::P256(public_key),
            organization: None,
        },
        (validity, serial),
        Role::EndpointAuthority {
            key_id: key_id(public_key),
        },
    )
}

/// An S3 endpoint's certificate: issued by the endpoint's authority holding `authority_key` and
/// named `authority_name`, for the endpoint whose public key is `public_key`, dialed by each of
/// `names`, the first its subject's common name; valid for at most [`MAX_ENDPOINT_VALIDITY`].
pub fn endpoint(
    authority_key: &EndpointKey,
    authority_name: &str,
    public_key: &[u8],
    names: &[EndpointName<'_>],
    (validity, serial): (Validity, Serial),
) -> Result<Vec<u8>, CertError> {
    check_name(authority_name)?;
    let public_key = point(public_key)?;
    for name in names {
        if let EndpointName::Dns(dns) = name {
            check_dns_name(dns)?;
        }
    }
    if validity
        .not_after
        .checked_sub(validity.not_before)
        .is_some_and(|span| span > MAX_ENDPOINT_VALIDITY)
    {
        return Err(CertError::Time);
    }
    let address;
    let common = match names.first().ok_or(CertError::Name)? {
        EndpointName::Dns(dns) => *dns,
        EndpointName::Address(ip) => {
            address = ip.to_string();
            address.as_str()
        }
    };
    check_name(common)?;
    issue(
        Signer::P256(authority_key),
        authority_name,
        &Subject {
            name: common,
            key: SubjectKey::P256(public_key),
            organization: None,
        },
        (validity, serial),
        Role::Endpoint {
            names,
            key_id: key_id(public_key),
            authority_key_id: key_id(point(authority_key.public_key())?),
        },
    )
}

/// `der`, a certificate, as RFC 7468 §5.1 writes one: between the CERTIFICATE label's lines, its
/// base64 in lines of exactly 64 characters but the last (§2), each line ended by a line feed;
/// made once at its size.
pub fn pem(der: &[u8]) -> String {
    const BEGIN: &str = "-----BEGIN CERTIFICATE-----\n";
    const END: &str = "-----END CERTIFICATE-----\n";
    let text = BASE64.encode(der);
    let size = text
        .len()
        .checked_add(text.len().div_ceil(PEM_LINE))
        .and_then(|n| n.checked_add(BEGIN.len()))
        .and_then(|n| n.checked_add(END.len()));
    // A size past usize is past any text there is room for: the string grows instead.
    let mut out = String::with_capacity(size.unwrap_or(0));
    out.push_str(BEGIN);
    // Base64's alphabet is ASCII, so each 64 octets of it are 64 characters.
    for line in text.as_bytes().chunks(PEM_LINE) {
        out.push_str(std::str::from_utf8(line).unwrap_or_default());
        out.push('\n');
    }
    out.push_str(END);
    out
}

/// The characters of a PEM line but the last (RFC 7468 §2).
const PEM_LINE: usize = 64;

/// A P-256 public key as its uncompressed point: 65 octets, the first 0x04 (SEC 1 §2.3.3; RFC 5480
/// §2.2).
fn point(key: &[u8]) -> Result<&[u8], CertError> {
    match key.first() {
        Some(0x04) if key.len() == 65 => Ok(key),
        _ => Err(CertError::Key),
    }
}

/// The octets of a key's identifier: 160 bits (RFC 7093 §2).
const KEY_ID_BYTES: usize = 20;

/// A key's identifier: the leftmost 160 bits of the SHA-256 of its subjectPublicKey's bits, the
/// BIT STRING's value without its tag, length and unused-bits octet (RFC 7093 §2, its first method).
fn key_id(public_key: &[u8]) -> [u8; KEY_ID_BYTES] {
    let hash = digest(&SHA256, public_key);
    let mut id = [0u8; KEY_ID_BYTES];
    if let Some(left) = hash.as_ref().get(..KEY_ID_BYTES) {
        id.copy_from_slice(left);
    }
    id
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
/// with a hyphen, at most 253 characters in all; [`CertError::Name`] otherwise.
pub fn check_dns_name(name: &str) -> Result<(), CertError> {
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
    key: SubjectKey<'a>,
    /// The organization a client's subject names before its common name.
    organization: Option<&'a str>,
}

/// A subject's public key, as its certificate states it (RFC 5280 §4.1.2.7).
#[derive(Clone, Copy)]
enum SubjectKey<'a> {
    /// Ed25519's 32 octets (RFC 8410 §4).
    Ed25519(&'a [u8]),
    /// A P-256 key's uncompressed point (RFC 5480 §2.2).
    P256(&'a [u8]),
}

/// Who signs a certificate: the cell's authority, with Ed25519 (RFC 8410), or the S3 endpoint's,
/// with ECDSA P-256 and SHA-256 (RFC 5758 §3.2).
#[derive(Clone, Copy)]
enum Signer<'a> {
    Ed25519(&'a KeyPair),
    P256(&'a EndpointKey),
}

impl Signer<'_> {
    /// The signature's AlgorithmIdentifier, the same in the certificate and its TBS certificate
    /// (RFC 5280 §4.1.1.2, §4.1.2.3), its parameters absent for both (RFC 8410 §3, RFC 5758 §3.2).
    fn algorithm(&self, out: &mut Vec<u8>) -> Result<(), CertError> {
        match self {
            Self::Ed25519(_) => ed25519_algorithm(out),
            Self::P256(_) => nested(out, der::SEQUENCE, |a| {
                tlv(a, der::OID, oid::ECDSA_WITH_SHA256)
            }),
        }
    }

    /// The signature over `tbs`: Ed25519's 64 octets, or ECDSA's as its DER `Ecdsa-Sig-Value`
    /// (RFC 3279 §2.2.3), which a certificate carries (RFC 5758 §3.2).
    fn sign(&self, tbs: &[u8]) -> Result<Signature, CertError> {
        match self {
            Self::Ed25519(key) => Ok(key.key.sign(tbs)),
            Self::P256(key) => key
                .key
                .sign(&SystemRandom::new(), tbs)
                .map_err(|_| CertError::Key),
        }
    }
}

/// What a certificate is for, which its extensions state.
enum Role<'a> {
    Authority,
    Node {
        dns_name: &'a str,
    },
    /// A client of the native protocol: a connection's client end alone.
    Client,
    /// The S3 endpoint's authority: an authority's extensions, and its key's identifier.
    EndpointAuthority {
        key_id: [u8; KEY_ID_BYTES],
    },
    /// An S3 endpoint: a server's certificate for the names its clients dial, with its key's
    /// identifier and its issuer's.
    Endpoint {
        names: &'a [EndpointName<'a>],
        key_id: [u8; KEY_ID_BYTES],
        authority_key_id: [u8; KEY_ID_BYTES],
    },
}

/// The certificate RFC 5280 §4.1 lays out: the TBS certificate, the signature algorithm, and the
/// issuer's signature over the TBS certificate's DER.
fn issue(
    signer: Signer<'_>,
    issuer_name: &str,
    subject: &Subject<'_>,
    (validity, serial): (Validity, Serial),
    role: Role<'_>,
) -> Result<Vec<u8>, CertError> {
    if validity.not_after <= validity.not_before {
        return Err(CertError::Time);
    }
    let tbs = tbs_certificate(&serial.0, (signer, issuer_name), subject, validity, &role)?;
    let signature = signer.sign(&tbs)?;
    let mut out = Vec::new();
    nested(&mut out, der::SEQUENCE, |c| {
        c.extend_from_slice(&tbs);
        signer.algorithm(c)?;
        der::bits(c, 0, signature.as_ref())
    })?;
    Ok(out)
}

fn tbs_certificate(
    serial: &[u8],
    (signer, issuer_name): (Signer<'_>, &str),
    subject: &Subject<'_>,
    validity: Validity,
    role: &Role<'_>,
) -> Result<Vec<u8>, CertError> {
    let mut out = Vec::new();
    nested(&mut out, der::SEQUENCE, |t| {
        // version: v3 (2), RFC 5280 §4.1.2.1, since the certificate carries extensions.
        nested(t, der::EXPLICIT_0, |v| der::unsigned(v, &[2]))?;
        der::unsigned(t, serial)?;
        signer.algorithm(t)?;
        name(t, issuer_name)?;
        nested(t, der::SEQUENCE, |v| {
            time::encode(v, validity.not_before)?;
            time::encode(v, validity.not_after)
        })?;
        match subject.organization {
            Some(organization) => client_name(t, organization, subject.name)?,
            None => name(t, subject.name)?,
        }
        nested(t, der::SEQUENCE, |k| match subject.key {
            SubjectKey::Ed25519(key) => {
                ed25519_algorithm(k)?;
                der::bits(k, 0, key)
            }
            // id-ecPublicKey on the named curve P-256 (RFC 5480 §2.1.1).
            SubjectKey::P256(key) => {
                nested(k, der::SEQUENCE, |a| {
                    tlv(a, der::OID, oid::EC_PUBLIC_KEY)?;
                    tlv(a, der::OID, oid::PRIME256V1)
                })?;
                der::bits(k, 0, key)
            }
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

/// A client's subject: its organization, then its common name, each a relative distinguished
/// name of one attribute as a UTF8String (RFC 5280 §4.1.2.4, §4.1.2.6).
fn client_name(out: &mut Vec<u8>, organization: &str, common_name: &str) -> Result<(), CertError> {
    nested(out, der::SEQUENCE, |n| {
        for (id, value) in [
            (oid::ORGANIZATION_NAME, organization),
            (oid::COMMON_NAME, common_name),
        ] {
            nested(n, der::SET, |rdn| {
                nested(rdn, der::SEQUENCE, |atv| {
                    tlv(atv, der::OID, id)?;
                    tlv(atv, der::UTF8_STRING, value.as_bytes())
                })
            })?;
        }
        Ok(())
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
        Role::Authority => authority_extensions(out),
        Role::Client => leaf_extensions(out, &[oid::CLIENT_AUTH]),
        Role::Node { dns_name } => {
            leaf_extensions(out, &[oid::SERVER_AUTH, oid::CLIENT_AUTH])?;
            extension(out, oid::SUBJECT_ALT_NAME, false, |v| {
                nested(v, der::SEQUENCE, |names| {
                    tlv(names, der::DNS_NAME, dns_name.as_bytes())
                })
            })
        }
        Role::EndpointAuthority { key_id } => {
            authority_extensions(out)?;
            subject_key_id(out, key_id)
        }
        Role::Endpoint {
            names,
            key_id,
            authority_key_id,
        } => {
            leaf_extensions(out, &[oid::SERVER_AUTH])?;
            extension(out, oid::SUBJECT_ALT_NAME, false, |v| {
                nested(v, der::SEQUENCE, |list| {
                    for name in *names {
                        match name {
                            EndpointName::Dns(dns) => tlv(list, der::DNS_NAME, dns.as_bytes())?,
                            // An iPAddress is the address's octets in network order, four for
                            // IPv4 and sixteen for IPv6 (RFC 5280 §4.2.1.6).
                            EndpointName::Address(IpAddr::V4(ip)) => {
                                tlv(list, der::IP_ADDRESS, &ip.octets())?;
                            }
                            EndpointName::Address(IpAddr::V6(ip)) => {
                                tlv(list, der::IP_ADDRESS, &ip.octets())?;
                            }
                        }
                    }
                    Ok(())
                })
            })?;
            subject_key_id(out, key_id)?;
            // AuthorityKeyIdentifier's keyIdentifier alone (RFC 5280 §4.2.1.1).
            extension(out, oid::AUTHORITY_KEY_IDENTIFIER, false, |v| {
                nested(v, der::SEQUENCE, |a| {
                    tlv(a, der::KEY_IDENTIFIER, authority_key_id)
                })
            })
        }
    }
}

/// An authority's: a CA, and a signer of certificates and their revocation lists alone (RFC 5280
/// §4.2.1.9, §4.2.1.3), both critical.
fn authority_extensions(out: &mut Vec<u8>) -> Result<(), CertError> {
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

/// A leaf's: no CA, a signer of handshakes alone, both critical, and the ends of a connection it
/// may serve (RFC 5280 §4.2.1.9, §4.2.1.3, §4.2.1.12).
fn leaf_extensions(out: &mut Vec<u8>, ends: &[&[u8]]) -> Result<(), CertError> {
    // cA FALSE is the default, which DER leaves out: an empty sequence.
    extension(out, oid::BASIC_CONSTRAINTS, true, |v| {
        tlv(v, der::SEQUENCE, &[])
    })?;
    extension(out, oid::KEY_USAGE, true, |v| {
        der::bits(v, DIGITAL_SIGNATURE.0, &[DIGITAL_SIGNATURE.1])
    })?;
    extension(out, oid::EXT_KEY_USAGE, false, |v| {
        nested(v, der::SEQUENCE, |k| {
            for end in ends {
                tlv(k, der::OID, end)?;
            }
            Ok(())
        })
    })
}

/// SubjectKeyIdentifier: the subject's key's identifier, an OCTET STRING (RFC 5280 §4.2.1.2).
fn subject_key_id(out: &mut Vec<u8>, key_id: &[u8; KEY_ID_BYTES]) -> Result<(), CertError> {
    extension(out, oid::SUBJECT_KEY_IDENTIFIER, false, |v| {
        tlv(v, der::OCTET_STRING, key_id)
    })
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
    /// Its extensions, the content of their SEQUENCE.
    extensions: &'a [u8],
}

/// The certificate in `der`, as this profile writes one: v3, Ed25519 throughout (RFC 8410), its
/// times in RFC 5280's forms, its extensions last, which the TLS stack checks of a peer's at its
/// handshake. Anything else, and anything after the certificate, is refused.
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
    let mut explicit = der::expect(&mut fields, der::EXPLICIT_3)?.content;
    let extensions = der::expect(&mut explicit, der::SEQUENCE)?.content;
    if !explicit.is_empty() || !fields.is_empty() {
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
        extensions,
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

    /// A client's certificate verifies under its authority at a connection's client end and at no
    /// server's, names the client and its account, and is the only kind that does: a node's,
    /// which may serve, names no client, and one another authority issued fails the handshake.
    #[test]
    fn a_client_certificate_serves_a_client_end_alone_and_names_its_client() {
        let authority_key = KeyPair::generate().unwrap();
        let ca = authority(
            &authority_key,
            "mantle cell authority",
            day_around_now(),
            Serial::draw().unwrap(),
        )
        .unwrap();
        let client_key = KeyPair::generate().unwrap();
        let issued = client(
            &authority_key,
            "mantle cell authority",
            client_key.public_key(),
            ("client-7", "123456789012"),
            (day_around_now(), Serial::draw().unwrap()),
        )
        .unwrap();
        as_client(&ca, &issued).unwrap();
        assert!(as_server(&ca, &issued, "client-7").is_err());
        assert_eq!(
            client_names(&issued),
            Ok(ClientNames {
                name: "client-7",
                account: "123456789012",
            })
        );
        assert_eq!(issued_by(&issued, &ca, now()), Ok(()));
        let node_key = KeyPair::generate().unwrap();
        let node_cert = node(
            &authority_key,
            "mantle cell authority",
            node_key.public_key(),
            "node-7",
            (day_around_now(), Serial::draw().unwrap()),
        )
        .unwrap();
        assert_eq!(client_names(&node_cert), Err(CertError::Malformed));
        assert_eq!(client_names(&ca), Err(CertError::Malformed));
        let theirs = KeyPair::generate().unwrap();
        let foreign = client(
            &theirs,
            "mantle cell authority",
            client_key.public_key(),
            ("client-7", "123456789012"),
            (day_around_now(), Serial::draw().unwrap()),
        )
        .unwrap();
        assert!(as_client(&ca, &foreign).is_err());
        assert_eq!(issued_by(&foreign, &ca, now()), Err(CertError::Signature));
        // Names past X.520's bounds, and an empty account, are refused.
        let named = |name: &str, account: &str| {
            client(
                &authority_key,
                "mantle cell authority",
                client_key.public_key(),
                (name, account),
                (day_around_now(), Serial::draw().unwrap()),
            )
        };
        assert_eq!(named(&"c".repeat(65), "1"), Err(CertError::Name));
        assert_eq!(named("client-7", ""), Err(CertError::Name));
        assert!(named(&"c".repeat(64), &"1".repeat(64)).is_ok());
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

    /// The cell's certificates from fixed keys, serial and validity are the bytes they were before
    /// the S3 endpoint's chain shared their issue (their SHA-256 and length, from that code):
    /// what a node's identity, a simulation's run and a cell made before rest on.
    #[test]
    fn the_cells_certificates_are_the_bytes_they_were() {
        let a = KeyPair::from_seed(&[7u8; 32]).unwrap();
        let n = KeyPair::from_seed(&[8u8; 32]).unwrap();
        let issued = (
            Validity {
                not_before: 1_700_000_000,
                not_after: 253_402_300_799,
            },
            Serial::from_octets([9; 16]),
        );
        let name = "mantle cell 0123456789abcdef";
        let hex = |b: &[u8]| -> String {
            digest(&SHA256, b)
                .as_ref()
                .iter()
                .map(|x| format!("{x:02x}"))
                .collect()
        };
        let ca = authority(&a, name, issued.0, issued.1).unwrap();
        assert_eq!(
            (ca.len(), hex(&ca).as_str()),
            (
                308,
                "53f9b8b166368af218fcca5b145afe75b58b08dc6c33b28df714f21528c4d664"
            )
        );
        let cert = node(&a, name, n.public_key(), "node-1", issued).unwrap();
        assert_eq!(
            (cert.len(), hex(&cert).as_str()),
            (
                333,
                "0b9f28dd71be3dd6c1b23a2570d4611409e8414d266888ea35954effc645bb4c"
            )
        );
    }

    #[test]
    fn a_key_survives_its_pkcs8_document() {
        let key = KeyPair::generate().unwrap();
        let again = KeyPair::from_pkcs8(key.pkcs8()).unwrap();
        assert_eq!(key.public_key(), again.public_key());
        assert!(KeyPair::from_pkcs8(&[0x30, 0x00]).is_err());
        let key = EndpointKey::generate().unwrap();
        let again = EndpointKey::from_pkcs8(key.pkcs8()).unwrap();
        assert_eq!(key.public_key(), again.public_key());
        assert_eq!((key.public_key().len(), key.public_key()[0]), (65, 0x04));
        assert!(EndpointKey::from_pkcs8(KeyPair::generate().unwrap().pkcs8()).is_err());
    }

    /// The S3 endpoint's authority, and an endpoint's certificate for `names`.
    fn endpoint_chain(names: &[EndpointName<'_>]) -> (EndpointKey, Vec<u8>, EndpointKey, Vec<u8>) {
        let authority_key = EndpointKey::generate().unwrap();
        let ca = endpoint_authority(
            &authority_key,
            "mantle s3 authority",
            day_around_now(),
            Serial::draw().unwrap(),
        )
        .unwrap();
        let own = EndpointKey::generate().unwrap();
        let cert = endpoint(
            &authority_key,
            "mantle s3 authority",
            own.public_key(),
            names,
            (day_around_now(), Serial::draw().unwrap()),
        )
        .unwrap();
        (authority_key, ca, own, cert)
    }

    /// An endpoint's certificate verifies under the endpoint's authority for each DNS name and
    /// address it states, and for no other, and as a server's alone.
    #[test]
    fn an_endpoint_certificate_verifies_for_each_name_it_states_and_no_other() {
        let names = [
            EndpointName::Dns("s3.example"),
            EndpointName::Dns("localhost"),
            EndpointName::Address("127.0.0.1".parse().unwrap()),
            EndpointName::Address("::1".parse().unwrap()),
        ];
        let (_, ca, _, cert) = endpoint_chain(&names);
        for name in ["s3.example", "localhost", "127.0.0.1", "::1"] {
            as_server(&ca, &cert, name).unwrap_or_else(|e| panic!("{name}: {e:?}"));
        }
        for name in ["other.example", "10.0.0.1", "::2"] {
            assert!(as_server(&ca, &cert, name).is_err(), "verified for {name}");
        }
        assert!(as_client(&ca, &cert).is_err(), "a client's certificate");
        // Another endpoint authority's certificate does not verify under this one.
        let (_, other, _, _) = endpoint_chain(&names);
        assert!(as_server(&other, &cert, "s3.example").is_err());
    }

    /// An extension: its identifier, whether it is critical, and its value's DER.
    type Extension = (Vec<u8>, bool, Vec<u8>);

    /// Each extension of `certificate`, its identifier and whether it is critical, and the
    /// certificate's signature algorithm, in the TBS certificate and outside it.
    fn extensions_of(certificate: &[u8]) -> (Vec<Extension>, Vec<u8>, Vec<u8>) {
        let mut input = certificate;
        let mut outer = der::expect(&mut input, der::SEQUENCE).unwrap().content;
        let tbs = der::expect(&mut outer, der::SEQUENCE).unwrap().content;
        let outside = der::expect(&mut outer, der::SEQUENCE)
            .unwrap()
            .content
            .to_vec();
        let mut fields = tbs;
        der::expect(&mut fields, der::EXPLICIT_0).unwrap();
        der::expect(&mut fields, der::INTEGER).unwrap();
        let inside = der::expect(&mut fields, der::SEQUENCE)
            .unwrap()
            .content
            .to_vec();
        for _ in 0..4 {
            // The issuer, the validity, the subject and its key.
            der::expect(&mut fields, der::SEQUENCE).unwrap();
        }
        let mut list = der::expect(&mut fields, der::EXPLICIT_3).unwrap().content;
        let mut items = der::expect(&mut list, der::SEQUENCE).unwrap().content;
        let mut found = Vec::new();
        while !items.is_empty() {
            let mut e = der::expect(&mut items, der::SEQUENCE).unwrap().content;
            let id = der::expect(&mut e, der::OID).unwrap().content.to_vec();
            let critical = e.first() == Some(&der::BOOLEAN);
            if critical {
                der::expect(&mut e, der::BOOLEAN).unwrap();
            }
            let value = der::expect(&mut e, der::OCTET_STRING)
                .unwrap()
                .content
                .to_vec();
            found.push((id, critical, value));
        }
        (found, inside, outside)
    }

    /// The endpoint's chain carries what a verifier holding to RFC 5280 asks: ECDSA with SHA-256
    /// throughout; the authority a CA, its key's identifier stated; the endpoint no CA, a server
    /// alone, its key's identifier and its authority's stated, the latter the authority's own.
    #[test]
    fn an_endpoint_chain_carries_what_a_strict_verifier_asks() {
        let (authority_key, ca, own, cert) = endpoint_chain(&[EndpointName::Dns("s3.example")]);
        let ecdsa = {
            let mut a = Vec::new();
            tlv(&mut a, der::OID, oid::ECDSA_WITH_SHA256).unwrap();
            a
        };
        let (authority, inside, outside) = extensions_of(&ca);
        assert_eq!((&inside, &outside), (&ecdsa, &ecdsa));
        let ids: Vec<(&[u8], bool)> = authority.iter().map(|(i, c, _)| (&i[..], *c)).collect();
        assert_eq!(
            ids,
            [
                (oid::BASIC_CONSTRAINTS, true),
                (oid::KEY_USAGE, true),
                (oid::SUBJECT_KEY_IDENTIFIER, false)
            ]
        );
        let mut authority_id = Vec::new();
        tlv(
            &mut authority_id,
            der::OCTET_STRING,
            &key_id(authority_key.public_key()),
        )
        .unwrap();
        assert_eq!(authority[2].2, authority_id);
        let (leaf, inside, outside) = extensions_of(&cert);
        assert_eq!((&inside, &outside), (&ecdsa, &ecdsa));
        let ids: Vec<(&[u8], bool)> = leaf.iter().map(|(i, c, _)| (&i[..], *c)).collect();
        assert_eq!(
            ids,
            [
                (oid::BASIC_CONSTRAINTS, true),
                (oid::KEY_USAGE, true),
                (oid::EXT_KEY_USAGE, false),
                (oid::SUBJECT_ALT_NAME, false),
                (oid::SUBJECT_KEY_IDENTIFIER, false),
                (oid::AUTHORITY_KEY_IDENTIFIER, false)
            ]
        );
        let mut leaf_id = Vec::new();
        tlv(&mut leaf_id, der::OCTET_STRING, &key_id(own.public_key())).unwrap();
        assert_eq!(leaf[4].2, leaf_id);
        let mut issuer_id = Vec::new();
        nested(&mut issuer_id, der::SEQUENCE, |a| {
            tlv(a, der::KEY_IDENTIFIER, &key_id(authority_key.public_key()))
        })
        .unwrap();
        assert_eq!(leaf[5].2, issuer_id);
    }

    /// An endpoint's certificate is valid for 825 days at most, and names something.
    #[test]
    fn an_endpoint_certificate_past_825_days_or_naming_nothing_is_refused() {
        let authority_key = EndpointKey::generate().unwrap();
        let own = EndpointKey::generate().unwrap();
        let names = [EndpointName::Dns("s3.example")];
        let issue = |not_after: u64, names: &[EndpointName<'_>]| {
            endpoint(
                &authority_key,
                "mantle s3 authority",
                own.public_key(),
                names,
                (
                    Validity {
                        not_before: 1_000,
                        not_after,
                    },
                    Serial::draw().unwrap(),
                ),
            )
        };
        assert!(issue(1_000 + MAX_ENDPOINT_VALIDITY, &names).is_ok());
        assert_eq!(
            issue(1_001 + MAX_ENDPOINT_VALIDITY, &names),
            Err(CertError::Time)
        );
        assert_eq!(issue(2_000, &[]), Err(CertError::Name));
        assert_eq!(
            issue(2_000, &[EndpointName::Dns("-bad.example")]),
            Err(CertError::Name)
        );
        assert_eq!(
            endpoint(
                &authority_key,
                "mantle s3 authority",
                &own.public_key()[1..],
                &names,
                (day_around_now(), Serial::draw().unwrap()),
            ),
            Err(CertError::Key)
        );
    }

    /// A PEM document is its certificate's base64 in lines of 64 characters, the last no longer,
    /// between the CERTIFICATE label's lines (RFC 7468).
    #[test]
    fn a_pem_document_is_its_certificate_in_lines_of_64() {
        let (_, ca, _, _) = endpoint_chain(&[EndpointName::Dns("s3.example")]);
        let text = pem(&ca);
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.first(), Some(&"-----BEGIN CERTIFICATE-----"));
        assert_eq!(lines.last(), Some(&"-----END CERTIFICATE-----"));
        let body = &lines[1..lines.len() - 1];
        let (last, full) = body.split_last().unwrap();
        assert!(full.iter().all(|l| l.len() == 64) && !last.is_empty() && last.len() <= 64);
        assert_eq!(BASE64.decode(body.concat()).unwrap(), ca);
        assert!(text.ends_with('\n') && text.len() == text.capacity());
    }
}
