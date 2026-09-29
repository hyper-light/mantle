//! Accounts as requests name them (docs/research/17 §2.1, §6): a 12-digit account ID, which a
//! bucket policy names a principal by, and which `x-amz-expected-bucket-owner` and
//! `x-amz-source-expected-bucket-owner` hold to make a request fail unless its bucket's owner is
//! the account the client expects.

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
