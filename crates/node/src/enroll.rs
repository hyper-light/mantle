//! The cell's issuer on a member, and the re-grant the root range's leader signs with it
//! (docs/design/membership.md §5.1 step 3).
//!
//! Every member keeps the issuer's certificate and key beside the authority's certificate
//! (`crate::process::ISSUER`), since every member votes in the root range and may lead it. A
//! written-off node asking on the enrollment protocol is admitted under its old certificate,
//! whose fingerprint and key its handshake proved (`crate::directory::Enrolling`), and judged
//! first by the member it reached ([`judge`]): refused `Unissued` where that member keeps no
//! issuer, and `SameKey` where the key it asks for is the one its old certificate certifies, so a
//! re-grant always moves it to a key it did not present before. The root range's leader signs
//! ([`sign`]) a certificate of `mantle_cert::granted_node`'s profile for the new key, under a new
//! id and the issuer's next grant and the issuer's own name, and proposes the `Regrant` that
//! names it; the directory admits that certificate only once the root range has committed it
//! (rule 4).

use mantle_cert::{CertError, KeyPair, Serial, Validity};
use mantle_meta::record::PUBLIC_KEY;
use mantle_meta::root::Regrant;

use crate::directory::Enrolling;

/// The cell's issuer as a member keeps it: its certificate, its key, and the name it signs as,
/// read from its certificate (`mantle_cert::issuer_name`) rather than spelled from the
/// authority's.
#[derive(Debug)]
pub struct Issuer {
    certificate: Vec<u8>,
    key: KeyPair,
    name: String,
}

impl Issuer {
    /// The issuer whose certificate is `certificate` and key `key`: refused unless the
    /// certificate is of the issuer's profile and certifies `key`.
    pub fn new(certificate: Vec<u8>, key: KeyPair) -> Result<Self, CertError> {
        let name = mantle_cert::issuer_name(&certificate)?.to_owned();
        if mantle_cert::public_key(&certificate)? != key.public_key() {
            return Err(CertError::Key);
        }
        Ok(Self {
            certificate,
            key,
            name,
        })
    }

    /// Its certificate, which an answer carries for the new certificate's chain.
    pub fn certificate(&self) -> &[u8] {
        &self.certificate
    }
}

/// Why a written-off node's request is refused before the root range is asked (membership.md
/// §5.1 step 3); the root range's own refusals are `mantle_meta::root::Outcome`'s.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Refused {
    /// The member keeps no issuer: a cell made before it, whose authority's holder has yet to
    /// make one (`mantle init issuer`).
    #[error("this member keeps no issuer")]
    Unissued,
    /// The key asked for is the one the old certificate certifies.
    #[error("the key asked for is the old certificate's")]
    SameKey,
}

/// Why the leader made no re-grant.
#[derive(Debug, thiserror::Error)]
pub enum EnrollError {
    #[error("refused: {0}")]
    Refused(#[from] Refused),
    #[error("the issuer could not sign: {0}")]
    Certificate(#[from] CertError),
}

/// Whether a member keeping `issuer` asks the root range to re-grant `enrolling`, the written-off
/// node its handshake proved, to `key`.
pub fn judge(
    issuer: Option<&Issuer>,
    enrolling: &Enrolling,
    key: &[u8; PUBLIC_KEY],
) -> Result<(), Refused> {
    if issuer.is_none() {
        return Err(Refused::Unissued);
    }
    if enrolling.key == *key {
        return Err(Refused::SameKey);
    }
    Ok(())
}

/// The re-grant the root range's leader proposes for `enrolling`, the written-off node a member's
/// handshake proved, asking for `key`: a certificate the issuer signs for `key` as node `node`,
/// the id the leader drew, under `grant`, the issuer's window's next, valid from `now` seconds
/// after the Unix epoch with no expiry (`mantle_cert::NO_EXPIRY`), its fingerprint the SHA-256 of
/// that certificate, in a cell of at most `max_nodes` nodes. Refused as [`judge`] refuses.
pub fn sign(
    issuer: Option<&Issuer>,
    (enrolling, key): (&Enrolling, &[u8; PUBLIC_KEY]),
    (node, grant): (u64, u64),
    (max_nodes, now): (u32, u64),
) -> Result<Regrant, EnrollError> {
    let issuer = issuer.ok_or(Refused::Unissued)?;
    judge(Some(issuer), enrolling, key)?;
    let certificate = mantle_cert::granted_node(
        &issuer.key,
        &issuer.name,
        key,
        (&mantle_protocol::node_name(node), grant),
        (
            Validity {
                not_before: now,
                not_after: mantle_cert::NO_EXPIRY,
            },
            Serial::draw()?,
        ),
    )?;
    Ok(Regrant {
        old: enrolling.old,
        node,
        grant,
        fingerprint: mantle_cert::fingerprint(&certificate),
        key: *key,
        certificate,
        max_nodes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mantle_cert::IssuedBy;

    fn validity() -> (Validity, Serial) {
        (
            Validity {
                not_before: 0,
                not_after: mantle_cert::NO_EXPIRY,
            },
            Serial::draw().unwrap(),
        )
    }

    /// A cell's authority and its issuer, named apart as `mantle init` names them.
    fn cell() -> (Vec<u8>, Issuer) {
        let authority_key = KeyPair::generate().unwrap();
        let authority =
            mantle_cert::authority(&authority_key, "cell", validity().0, validity().1).unwrap();
        let issuer_key = KeyPair::generate().unwrap();
        let certificate = mantle_cert::issuer(
            &authority_key,
            "cell",
            issuer_key.public_key(),
            "cell issuer",
            validity(),
        )
        .unwrap();
        (authority, Issuer::new(certificate, issuer_key).unwrap())
    }

    fn enrolling(old: u64) -> (Enrolling, KeyPair) {
        let key = KeyPair::generate().unwrap();
        let enrolling = Enrolling {
            old,
            fingerprint: [7; 32],
            key: key.public_key().try_into().unwrap(),
        };
        (enrolling, key)
    }

    /// membership.md §5.1 step 3: the leader's re-grant names the old node, the new id and the
    /// issuer's grant, and a certificate for the new key that the issuer signed under its own
    /// name, which a node's start takes as the issuer's (`mantle_cert::node_issued`) and whose
    /// SHA-256 is the re-grant's fingerprint.
    #[test]
    fn the_leaders_regrant_signs_a_certificate_the_issuer_issued() {
        let (authority, issuer) = cell();
        let (asking, _) = enrolling(4);
        let next = KeyPair::generate().unwrap();
        let key: [u8; PUBLIC_KEY] = next.public_key().try_into().unwrap();
        let now = 1_000;
        let regrant = sign(Some(&issuer), (&asking, &key), (9, 3), (5, now)).unwrap();
        assert_eq!(
            (regrant.old, regrant.node, regrant.grant, regrant.max_nodes),
            (4, 9, 3, 5)
        );
        assert_eq!(regrant.key, key);
        assert_eq!(
            regrant.fingerprint,
            mantle_cert::fingerprint(&regrant.certificate)
        );
        assert_eq!(
            mantle_cert::node_issued(
                &regrant.certificate,
                Some(issuer.certificate()),
                &authority,
                now
            ),
            Ok(IssuedBy::Issuer)
        );
        let names = mantle_cert::node_names(&regrant.certificate).unwrap();
        assert_eq!((names.name, names.grant), ("node-9", Some(3)));
        assert_eq!(
            mantle_cert::public_key(&regrant.certificate).unwrap(),
            next.public_key()
        );
        // Not the authority's: a directory told it takes the certificate as the issuer's.
        assert!(!mantle_cert::issuer_is(&regrant.certificate, &authority).unwrap());
    }

    /// membership.md §5.1 step 3's refusals a member makes itself: `Unissued` where it keeps no
    /// issuer, `SameKey` where the key asked for is the old certificate's.
    #[test]
    fn same_key_and_unissued_are_refused() {
        let (_, issuer) = cell();
        let (asking, old_key) = enrolling(4);
        let same: [u8; PUBLIC_KEY] = old_key.public_key().try_into().unwrap();
        let fresh: [u8; PUBLIC_KEY] = KeyPair::generate()
            .unwrap()
            .public_key()
            .try_into()
            .unwrap();
        assert_eq!(judge(Some(&issuer), &asking, &same), Err(Refused::SameKey));
        assert_eq!(judge(None, &asking, &fresh), Err(Refused::Unissued));
        assert_eq!(judge(Some(&issuer), &asking, &fresh), Ok(()));
        assert!(matches!(
            sign(Some(&issuer), (&asking, &same), (9, 3), (5, 0)),
            Err(EnrollError::Refused(Refused::SameKey))
        ));
        assert!(matches!(
            sign(None, (&asking, &fresh), (9, 3), (5, 0)),
            Err(EnrollError::Refused(Refused::Unissued))
        ));
    }

    /// An issuer whose certificate does not certify its key, or is no issuer's, is refused.
    #[test]
    fn an_issuer_is_its_certificates_key() {
        let (authority, issuer) = cell();
        let other = KeyPair::generate().unwrap();
        assert!(matches!(
            Issuer::new(issuer.certificate().to_vec(), other),
            Err(CertError::Key)
        ));
        assert!(matches!(
            Issuer::new(authority, KeyPair::generate().unwrap()),
            Err(CertError::Malformed)
        ));
    }
}
