//! The wire profiles a node and its peers speak, and how a connection's two sides agree on one
//! (docs/research/91; node.md §1.7).
//!
//! A profile fixes the formats of every message that travels point to point on a connection: the
//! native protocol's requests and replies ([`crate::object`]) and the reads a node asks of a range's
//! leader (mantle-node's `read`). Its two sides agree on it at the handshake, by ALPN (RFC 7301),
//! which QUIC requires an application protocol to be agreed by (RFC 9001 §8.1): each side offers
//! the profiles it speaks, newest first, and a server takes the first of its own that the client
//! offers (RFC 7301 §3.2, "the most highly preferred protocol that it supports and that is also
//! advertised by the client"), so a pair speaks the newest profile both know, as RFC 9368's
//! compatible version negotiation has a QUIC pair use the newest version both support. TLS 1.3
//! carries the offer in the ClientHello and the choice in its EncryptedExtensions, under the
//! Finished message's MAC over the whole handshake (RFC 8446 §2, §4.2), so no one between the two
//! can talk them down to an older profile, the downgrade RFC 9368 §4 closes for QUIC's versions.
//!
//! Every mantle before profiles offered the transport's own protocol name alone
//! (`hyper_transport::tls::ALPN`); that name is profile [`Profile::One`], so a binary that speaks
//! profiles meets one that does not on the formats the older knows. What a cell writes in formats
//! older members cannot read waits on its upgrade fence (node.md §1.7), not on any connection.

/// A wire profile, oldest first: each one's formats are the previous one's and what it adds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Profile {
    /// The formats of every mantle before profiles: the native protocol's operations through a
    /// native upload's Finish (kinds 0 to 20) with their fields as they were, and the reads of
    /// kinds 0 to 12.
    One,
    /// research/89's: a version's metadata on PutObject, CreateMultipartUpload, Open and every
    /// reply that states a version; a version named on GetObject, HeadObject and DeleteObject;
    /// CopyObject, UploadPartCopy, DeleteObjects and ListObjectVersions (kinds 21 to 24) and their
    /// statuses and listings; and the reads of a named version, of a page of versions and of a
    /// write's mark (kinds 13 to 15).
    Two,
}

/// Every profile, newest first: the order a side offers them in.
const NEWEST_FIRST: [Profile; 2] = [Profile::Two, Profile::One];

/// The environment variable an operator names a lower profile in, for a staged rollout or a
/// rehearsal: a node that announces less holds its cell's upgrade fence below its binary's newest
/// profile, and keeps the binary it ran before able to take its place (node.md §1.7).
pub const ANNOUNCED: &str = "MANTLE_WIRE";

/// Why the profile an operator named cannot be announced.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProfileError {
    #[error("{ANNOUNCED}={0}: not a profile this binary speaks, 1 to {newest}", newest = Profile::NEWEST.number())]
    Unknown(String),
}

impl Profile {
    /// The newest profile this binary speaks.
    pub const NEWEST: Profile = Profile::Two;

    /// The profile's number, as an operator names it.
    pub const fn number(self) -> u8 {
        match self {
            Self::One => 1,
            Self::Two => 2,
        }
    }

    /// The application protocol name a handshake agrees on it by. Profile one's is the
    /// transport's own, the one every mantle before profiles offered.
    pub fn protocol(self) -> &'static [u8] {
        match self {
            Self::One => hyper_transport::tls::ALPN,
            Self::Two => b"mantle/2",
        }
    }

    /// The profile numbered `number`; none for one this binary does not speak.
    pub fn numbered(number: u8) -> Option<Self> {
        NEWEST_FIRST.into_iter().find(|p| p.number() == number)
    }

    /// The profile `protocol` names; none for one this binary does not speak.
    pub fn of(protocol: &[u8]) -> Option<Self> {
        NEWEST_FIRST.into_iter().find(|p| p.protocol() == protocol)
    }

    /// The protocols a side announcing `most` offers, newest first: its own preference, and a
    /// server's order of choice.
    pub fn offered(most: Profile) -> Vec<Vec<u8>> {
        NEWEST_FIRST
            .into_iter()
            .filter(|p| *p <= most)
            .map(|p| p.protocol().to_vec())
            .collect()
    }

    /// The profile this process announces: the newest it speaks, or the lower one its operator
    /// names in [`ANNOUNCED`]; a name past the newest, or not a profile, is refused rather than
    /// taken as another.
    pub fn announced() -> Result<Self, ProfileError> {
        match std::env::var(ANNOUNCED) {
            Err(_) => Ok(Self::NEWEST),
            Ok(named) => NEWEST_FIRST
                .into_iter()
                .find(|p| named.trim() == p.number().to_string())
                .ok_or(ProfileError::Unknown(named)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_profile_is_named_by_its_protocol_and_offered_newest_first() {
        for profile in NEWEST_FIRST {
            assert_eq!(Profile::of(profile.protocol()), Some(profile));
        }
        assert_eq!(Profile::of(b"hyper-transport/1"), Some(Profile::One));
        assert_eq!(Profile::of(b"mantle/3"), None);
        assert_eq!(Profile::numbered(2), Some(Profile::Two));
        assert_eq!(Profile::numbered(3), None);
        assert_eq!(
            Profile::offered(Profile::Two),
            [b"mantle/2".to_vec(), b"hyper-transport/1".to_vec()]
        );
        assert_eq!(
            Profile::offered(Profile::One),
            [b"hyper-transport/1".to_vec()]
        );
        assert!(Profile::One < Profile::Two);
    }
}
