//! Accounts as requests name them (docs/research/17 §2.1, §6): a 12-digit account ID, which a
//! bucket policy names a principal by, and which `x-amz-expected-bucket-owner` and
//! `x-amz-source-expected-bucket-owner` hold to make a request fail unless its bucket's owner is
//! the account the client expects.
//!
//! An account's principals, as IAM has its users (docs/research/91): each named as IAM names a
//! user and holding access keys as IAM's users hold theirs, an id and a secret drawn from the OS
//! (research/84), the same draw a cell's first key takes at `mantle init`.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use zeroize::Zeroizing;

/// An owner's name at most: a canonical user ID's 64 hexadecimal digits, the longest form S3
/// names an owner in (research/13, the ACL grantee); an account ID is 12 digits (17 §6).
pub const MAX_OWNER: usize = 64;

/// The principals an account holds at most: IAM's "Users per account", 5,000, not adjustable
/// (research/91 §2).
pub const MAX_PRINCIPALS: u32 = 5_000;

/// The access keys a principal holds at most: IAM's "Access keys per user", 2, not adjustable;
/// two, so that "While the first access key is still active, create a second access key" and
/// no request is refused while clients move to it (research/91 §2, §4).
pub const MAX_KEYS: usize = 2;

/// A principal's name at most, in characters: IAM's "User name", 64 (research/91 §2).
pub const MAX_NAME: usize = 64;

/// An access key's id, in characters of RFC 4648's base32 alphabet: as long as AWS's own
/// (`AKIAIOSFODNN7EXAMPLE`), within IAM's 16 to 128 word characters, with no `/`, which SigV4's
/// credential scope separates its parts by (research/84). A principal's unique ID is drawn the
/// same way, as long as a key's.
pub const KEY_ID_CHARS: usize = 20;

/// An access key's secret: 30 octets from the OS, 240 bits, as base64's 40 characters with no
/// padding, as long as AWS's (`wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`; research/84).
pub const SECRET_BYTES: usize = 30;

/// A secret's characters: base64 takes three octets to four characters (RFC 4648 §4).
pub const SECRET_CHARS: usize = SECRET_BYTES / 3 * 4;

/// RFC 4648 §6's base32 alphabet: 32 characters, so a random octet's low five bits pick one
/// uniformly.
pub const BASE32: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/// A secret held in memory: wiped when dropped.
pub type Secret = Zeroizing<String>;

/// The OS's random source failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the operating system's random source failed")]
pub struct RandomError;

/// Whether `name` is a principal's name as IAM takes a user's (research/91 §2): 1 to 64
/// characters, "alphanumeric, including the following common characters: plus (+), equals (=),
/// comma (,), period (.), at (@), underscore (_), and hyphen (-)".
pub fn valid_name(name: &str) -> bool {
    (1..=MAX_NAME).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"+=,.@_-".contains(&b))
}

/// Whether `id` is in the form [`draw_id`] draws: [`KEY_ID_CHARS`] characters of [`BASE32`]. A key
/// a request claims in any other form is no principal's.
pub fn valid_key_id(id: &str) -> bool {
    id.len() == KEY_ID_CHARS && id.bytes().all(|b| BASE32.contains(&b))
}

/// An id of [`KEY_ID_CHARS`] base32 characters from the OS: an access key's, or a principal's
/// unique ID.
pub fn draw_id() -> Result<String, RandomError> {
    let mut drawn = [0u8; KEY_ID_CHARS];
    getrandom::fill(&mut drawn).map_err(|_| RandomError)?;
    // Each byte drawn becomes its character in place: the id is written on the stack and copied
    // once into a string made at its length.
    for b in &mut drawn {
        *b = BASE32.get(usize::from(*b & 0x1f)).copied().unwrap_or(b'A');
    }
    Ok(String::from_utf8_lossy(&drawn).into_owned())
}

/// A secret of [`SECRET_BYTES`] octets from the OS, as its [`SECRET_CHARS`] characters of base64;
/// both wiped when dropped.
pub fn draw_secret() -> Result<Secret, RandomError> {
    let mut drawn = Zeroizing::new([0u8; SECRET_BYTES]);
    getrandom::fill(drawn.as_mut_slice()).map_err(|_| RandomError)?;
    Ok(Zeroizing::new(BASE64.encode(drawn.as_slice())))
}

/// Whether `id` is an account ID: twelve decimal digits, the form AWS gives one and a policy
/// names one in (17 §6).
pub fn valid_id(id: &str) -> bool {
    id.len() == 12 && id.bytes().all(|b| b.is_ascii_digit())
}

/// Why a request's expected bucket owner refuses it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OwnerError {
    /// The header does not hold an account ID: S3 answered `0000`, `0000000000020`, `abcd` and
    /// `invalid` so (17 §2.1).
    #[error("The value of the expected bucket owner parameter must be an AWS Account ID... [{0}]")]
    Invalid(String),
    /// The bucket's owner is another account: "the request fails with the HTTP status code `403
    /// Forbidden` (access denied)".
    #[error("Access Denied")]
    Mismatch,
}

impl OwnerError {
    /// The S3 error code and status, as S3 answered (17 §2.1).
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            Self::Invalid(_) => ("InvalidBucketOwnerAWSAccountID", 400),
            Self::Mismatch => ("AccessDenied", 403),
        }
    }
}

/// Whether a request may go on against a bucket owned by the account `owner`, given its
/// `x-amz-expected-bucket-owner`, or for a copy's source its
/// `x-amz-source-expected-bucket-owner`, if it sent one. A value that is no account ID is
/// refused before it is compared.
pub fn expected_owner(header: Option<&str>, owner: &str) -> Result<(), OwnerError> {
    let Some(expected) = header else {
        return Ok(());
    };
    if !valid_id(expected) {
        return Err(OwnerError::Invalid(expected.to_owned()));
    }
    if expected == owner {
        Ok(())
    } else {
        Err(OwnerError::Mismatch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// IAM's user names (research/91 §2): 1 to 64 characters of its alphabet.
    #[test]
    fn a_principals_name_is_an_iam_users() {
        for name in [
            "runner-7",
            "Bob",
            "a",
            "ci+kiln=1,x.y@z_w-v",
            &"n".repeat(MAX_NAME),
        ] {
            assert!(valid_name(name), "{name}");
        }
        for name in ["", "a b", "a/b", "é", "a:b", "*", &"n".repeat(MAX_NAME + 1)] {
            assert!(!valid_name(name), "{name}");
        }
    }

    /// An id is twenty base32 characters, a secret forty of base64 from thirty octets, each drawn
    /// anew.
    #[test]
    fn ids_and_secrets_are_drawn_in_their_forms() {
        let (a, b) = (draw_id().unwrap(), draw_id().unwrap());
        assert_eq!(a.len(), KEY_ID_CHARS);
        assert!(a.bytes().all(|c| BASE32.contains(&c)), "{a}");
        assert!(valid_key_id(&a));
        assert_ne!(a, b);
        for other in [
            "AKIAIOSFODNN7EXAMPLE1",
            "AKIAIOSFODNN7EXAMPL",
            "akiaiosfodnn7example",
            "AKIAIOSFODNN7EXAMPL1",
        ] {
            assert!(!valid_key_id(other), "{other}");
        }
        let secret = draw_secret().unwrap();
        assert_eq!(secret.len(), SECRET_CHARS);
        assert_eq!(SECRET_CHARS, 40);
        assert_eq!(
            BASE64.decode(secret.as_bytes()).unwrap().len(),
            SECRET_BYTES
        );
        assert_ne!(*secret, *draw_secret().unwrap());
    }

    /// LocalStack's recordings of S3 (17 §2.1).
    #[test]
    fn expected_owners_are_answered_as_s3_answered() {
        let owner = "111122223333";
        assert_eq!(expected_owner(None, owner), Ok(()));
        assert_eq!(expected_owner(Some(owner), owner), Ok(()));
        let other = expected_owner(Some("000000000002"), owner).unwrap_err();
        assert_eq!(other.code(), ("AccessDenied", 403));
        assert_eq!(other.to_string(), "Access Denied");
        for invalid in [
            "0000",
            "0000000000020",
            "abcd",
            "aa000000000$",
            "invalid",
            "",
        ] {
            let error = expected_owner(Some(invalid), owner).unwrap_err();
            assert_eq!(
                error.code(),
                ("InvalidBucketOwnerAWSAccountID", 400),
                "{invalid}"
            );
            assert_eq!(
                error.to_string(),
                format!(
                    "The value of the expected bucket owner parameter must be an AWS Account ID... [{invalid}]"
                )
            );
        }
    }
}
