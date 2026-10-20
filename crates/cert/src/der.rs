//! DER (ITU-T X.690 §10) of the few types a certificate holds: each value a tag, its length in the
//! shortest form, and its content.

use crate::CertError;

pub(crate) const BOOLEAN: u8 = 0x01;
pub(crate) const INTEGER: u8 = 0x02;
pub(crate) const BIT_STRING: u8 = 0x03;
pub(crate) const OCTET_STRING: u8 = 0x04;
pub(crate) const OID: u8 = 0x06;
pub(crate) const UTF8_STRING: u8 = 0x0c;
pub(crate) const UTC_TIME: u8 = 0x17;
pub(crate) const GENERALIZED_TIME: u8 = 0x18;
pub(crate) const SEQUENCE: u8 = 0x30;
pub(crate) const SET: u8 = 0x31;
/// `[0] EXPLICIT`, constructed: a certificate's version (RFC 5280 §4.1).
pub(crate) const EXPLICIT_0: u8 = 0xa0;
/// `[3] EXPLICIT`, constructed: a certificate's extensions (RFC 5280 §4.1).
pub(crate) const EXPLICIT_3: u8 = 0xa3;
/// `[2] IMPLICIT IA5String`: a `GeneralName`'s `dNSName` (RFC 5280 §4.2.1.6).
pub(crate) const DNS_NAME: u8 = 0x82;

/// Object identifiers, each its content octets (X.690 §8.19), checked against its arcs in the
/// tests.
pub(crate) mod oid {
    /// id-Ed25519, 1.3.101.112 (RFC 8410 §3).
    pub(crate) const ED25519: &[u8] = &[0x2b, 0x65, 0x70];
    /// id-at-commonName, 2.5.4.3 (RFC 5280 Appendix A.1).
    pub(crate) const COMMON_NAME: &[u8] = &[0x55, 0x04, 0x03];
    /// id-ce-basicConstraints, 2.5.29.19 (RFC 5280 §4.2.1.9).
    pub(crate) const BASIC_CONSTRAINTS: &[u8] = &[0x55, 0x1d, 0x13];
    /// id-ce-keyUsage, 2.5.29.15 (RFC 5280 §4.2.1.3).
    pub(crate) const KEY_USAGE: &[u8] = &[0x55, 0x1d, 0x0f];
    /// id-ce-extKeyUsage, 2.5.29.37 (RFC 5280 §4.2.1.12).
    pub(crate) const EXT_KEY_USAGE: &[u8] = &[0x55, 0x1d, 0x25];
    /// id-ce-subjectAltName, 2.5.29.17 (RFC 5280 §4.2.1.6).
    pub(crate) const SUBJECT_ALT_NAME: &[u8] = &[0x55, 0x1d, 0x11];
    /// id-kp-serverAuth, 1.3.6.1.5.5.7.3.1 (RFC 5280 §4.2.1.12).
    pub(crate) const SERVER_AUTH: &[u8] = &[0x2b, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x01];
    /// id-kp-clientAuth, 1.3.6.1.5.5.7.3.2 (RFC 5280 §4.2.1.12).
    pub(crate) const CLIENT_AUTH: &[u8] = &[0x2b, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x02];
}

/// Appends `tag`, the length of `value` in DER's shortest form, and `value`.
pub(crate) fn tlv(out: &mut Vec<u8>, tag: u8, value: &[u8]) -> Result<(), CertError> {
    out.push(tag);
    length(out, value.len())?;
    out.extend_from_slice(value);
    Ok(())
}

/// Appends a constructed `tag` whose content `body` writes.
pub(crate) fn nested(
    out: &mut Vec<u8>,
    tag: u8,
    body: impl FnOnce(&mut Vec<u8>) -> Result<(), CertError>,
) -> Result<(), CertError> {
    let mut inner = Vec::new();
    body(&mut inner)?;
    tlv(out, tag, &inner)
}

/// A length in DER's form (X.690 §10.1, §8.1.3): one octet below 128, else an octet stating how
/// many big-endian octets follow and those octets, none of them a leading zero.
fn length(out: &mut Vec<u8>, len: usize) -> Result<(), CertError> {
    if len < 0x80 {
        out.push(u8::try_from(len).map_err(|_| CertError::Encoding)?);
        return Ok(());
    }
    let octets = len.to_be_bytes();
    let first = octets
        .iter()
        .position(|&b| b != 0)
        .ok_or(CertError::Encoding)?;
    let significant = octets.get(first..).ok_or(CertError::Encoding)?;
    // A usize has at most eight octets, well inside the 126 X.690 §8.1.3.5 allows.
    out.push(0x80 | u8::try_from(significant.len()).map_err(|_| CertError::Encoding)?);
    out.extend_from_slice(significant);
    Ok(())
}

/// A non-negative INTEGER from its big-endian magnitude: no leading zero octets but one where the
/// first content bit would otherwise be set (X.690 §8.3.2), so it reads as positive.
pub(crate) fn unsigned(out: &mut Vec<u8>, magnitude: &[u8]) -> Result<(), CertError> {
    let digits = match magnitude.iter().position(|&b| b != 0) {
        Some(first) => magnitude.get(first..).ok_or(CertError::Encoding)?,
        None => &[0u8][..],
    };
    let mut content = Vec::new();
    if digits.first().is_some_and(|&b| b & 0x80 != 0) {
        content.push(0);
    }
    content.extend_from_slice(digits);
    tlv(out, INTEGER, &content)
}

/// A BIT STRING of whole octets, or of `unused` bits fewer in its last octet (X.690 §8.6.2).
pub(crate) fn bits(out: &mut Vec<u8>, unused: u8, octets: &[u8]) -> Result<(), CertError> {
    let mut content = Vec::new();
    content.push(unused);
    content.extend_from_slice(octets);
    tlv(out, BIT_STRING, &content)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An object identifier's content octets from its arcs (X.690 §8.19): the first two as
    /// `40 × first + second`, each arc base 128, high bit set on all but its last octet.
    fn encode_arcs(arcs: &[u64]) -> Vec<u8> {
        let mut values = vec![40 * arcs[0] + arcs[1]];
        values.extend_from_slice(&arcs[2..]);
        let mut out = Vec::new();
        for v in values {
            let mut groups = vec![(v & 0x7f) as u8];
            let mut rest = v >> 7;
            while rest > 0 {
                groups.push(((rest & 0x7f) as u8) | 0x80);
                rest >>= 7;
            }
            groups.reverse();
            out.extend_from_slice(&groups);
        }
        out
    }

    #[test]
    fn each_identifier_is_its_arcs_encoded() {
        let table: [(&[u8], &[u64]); 8] = [
            (oid::ED25519, &[1, 3, 101, 112]),
            (oid::COMMON_NAME, &[2, 5, 4, 3]),
            (oid::BASIC_CONSTRAINTS, &[2, 5, 29, 19]),
            (oid::KEY_USAGE, &[2, 5, 29, 15]),
            (oid::EXT_KEY_USAGE, &[2, 5, 29, 37]),
            (oid::SUBJECT_ALT_NAME, &[2, 5, 29, 17]),
            (oid::SERVER_AUTH, &[1, 3, 6, 1, 5, 5, 7, 3, 1]),
            (oid::CLIENT_AUTH, &[1, 3, 6, 1, 5, 5, 7, 3, 2]),
        ];
        for (encoded, arcs) in table {
            assert_eq!(encoded, encode_arcs(arcs).as_slice(), "{arcs:?}");
        }
    }

    #[test]
    fn a_length_takes_the_shortest_form() {
        let mut out = Vec::new();
        tlv(&mut out, OCTET_STRING, &[7; 127]).unwrap();
        assert_eq!(&out[..2], &[OCTET_STRING, 0x7f]);
        out.clear();
        tlv(&mut out, OCTET_STRING, &[7; 128]).unwrap();
        assert_eq!(&out[..3], &[OCTET_STRING, 0x81, 0x80]);
        out.clear();
        tlv(&mut out, OCTET_STRING, &[7; 256]).unwrap();
        assert_eq!(&out[..4], &[OCTET_STRING, 0x82, 0x01, 0x00]);
    }

    #[test]
    fn an_integer_reads_positive_and_has_no_leading_zero() {
        let cases: [(&[u8], &[u8]); 5] = [
            (&[0], &[INTEGER, 1, 0]),
            (&[0, 0, 5], &[INTEGER, 1, 5]),
            (&[0x7f], &[INTEGER, 1, 0x7f]),
            (&[0x80], &[INTEGER, 2, 0, 0x80]),
            (&[0, 0xff, 1], &[INTEGER, 3, 0, 0xff, 1]),
        ];
        for (magnitude, der) in cases {
            let mut out = Vec::new();
            unsigned(&mut out, magnitude).unwrap();
            assert_eq!(out, der, "{magnitude:?}");
        }
    }
}
