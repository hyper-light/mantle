//! The cell's S3 profile as `mantle init` writes it (OUT/s3) and `mantle init client` gives a
//! client (CLIENT/s3): AWS's shared `config` and `credentials` files, read as the AWS CLI reads
//! them (AWS SDKs and Tools Reference Guide, "The shared config and credentials files";
//! docs/research/86 [2]), and the endpoint's authority as PEM. mantle's own S3 client signs with
//! this key, for this region, at this endpoint (src/door.rs).
//!
//! A file is sections, `[NAME]` in the credentials file and `[profile NAME]` in the config, each of
//! `key = value` lines; a line that begins with whitespace sets a key of the nested section the
//! line before it opened (`s3 =`), and `#` or `;` opens a comment line. Only the `mantle` profile is
//! read.

use std::path::{Path, PathBuf};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;

/// The profile's name, in both files (`mantle init`).
pub const PROFILE: &str = "mantle";

/// What a client signs with and asks: the cell's access key, its region, its endpoint where the
/// config names one, and the endpoint's authority, DER.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Profile {
    pub id: String,
    pub secret: String,
    pub region: String,
    /// `endpoint_url`: `http://HOST:PORT` on a loopback address, `https://HOST:PORT` elsewhere.
    pub endpoint: Option<String>,
    pub authority: Vec<u8>,
}

/// Why a profile did not read.
#[derive(Debug)]
pub struct ProfileError(pub PathBuf, pub String);

impl std::fmt::Display for ProfileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.0.display(), self.1)
    }
}

impl Profile {
    /// The profile in `dir`: its `credentials`, its `authority.pem`, and its `config` where there
    /// is one, whose region is S3's default, us-east-1, where none is stated (as `mantle init`'s).
    pub fn read(dir: &Path) -> Result<Self, ProfileError> {
        let read = |name: &str| {
            let path = dir.join(name);
            match std::fs::read_to_string(&path) {
                Ok(text) => Ok(Some((path, text))),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(ProfileError(path, e.to_string())),
            }
        };
        let missing = |name: &str| ProfileError(dir.join(name), "no such file".into());
        let (path, text) = read("credentials")?.ok_or_else(|| missing("credentials"))?;
        let keys = section(&text, PROFILE);
        let key = |name: &str| {
            setting(&keys, name)
                .ok_or_else(|| ProfileError(path.clone(), format!("no {name} in [{PROFILE}]")))
        };
        let (id, secret) = (key("aws_access_key_id")?, key("aws_secret_access_key")?);
        let (mut region, mut endpoint) = (None, None);
        if let Some((_, text)) = read("config")? {
            let settings = section(&text, &format!("profile {PROFILE}"));
            region = setting(&settings, "region");
            endpoint = setting(&settings, "endpoint_url");
        }
        let (path, pem) = read("authority.pem")?.ok_or_else(|| missing("authority.pem"))?;
        let authority = der(&pem)
            .ok_or_else(|| ProfileError(path, "not one PEM CERTIFICATE (RFC 7468 §5)".into()))?;
        Ok(Self {
            id,
            secret,
            region: region.unwrap_or_else(|| crate::init::DEFAULT_REGION.to_owned()),
            endpoint,
            authority,
        })
    }
}

/// The `key = value` settings of the section `name`, top-level ones only, in order.
fn section(text: &str, name: &str) -> Vec<(String, String)> {
    let mut settings = Vec::new();
    let mut inside = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with(';') {
            continue;
        }
        if let Some(header) = trimmed.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            inside = header.trim() == name;
            continue;
        }
        if !inside || line.starts_with(char::is_whitespace) {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            settings.push((key.trim().to_owned(), value.trim().to_owned()));
        }
    }
    settings
}

/// The last value `key` takes, as a later line overrides an earlier one; none where it is empty.
fn setting(settings: &[(String, String)], key: &str) -> Option<String> {
    settings
        .iter()
        .rev()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
        .filter(|v| !v.is_empty())
}

/// The DER of the one `CERTIFICATE` a PEM file holds: the base64 between its encapsulation
/// boundaries, white space ignored (RFC 7468 §2, §5).
fn der(pem: &str) -> Option<Vec<u8>> {
    let begin = "-----BEGIN CERTIFICATE-----";
    let end = "-----END CERTIFICATE-----";
    let start = pem.find(begin)?.checked_add(begin.len())?;
    let rest = pem.get(start..)?;
    let body = rest.get(..rest.find(end)?)?;
    if rest.get(rest.find(end)?..)?.matches(begin).next().is_some() {
        return None;
    }
    let text: String = body.chars().filter(|c| !c.is_whitespace()).collect();
    BASE64.decode(text).ok().filter(|d| !d.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The files `mantle init` writes read back as written: the key, the region, the endpoint and
    /// the authority's DER, a nested section's keys and other profiles passed.
    #[test]
    fn the_files_init_writes_read_back() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("credentials"),
            "[other]\naws_access_key_id = X\n[mantle]\naws_access_key_id = AKID\n\
             aws_secret_access_key = s/e+cret=\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("config"),
            "# made by mantle init\n[profile mantle]\nregion = eu-west-1\n\
             endpoint_url = http://127.0.0.1:9000\ns3 =\n  addressing_style = path\n",
        )
        .unwrap();
        let der = vec![0x30, 0x82, 0x01, 0x0a, 0xff];
        std::fs::write(dir.path().join("authority.pem"), mantle_cert::pem(&der)).unwrap();
        assert_eq!(
            Profile::read(dir.path()).unwrap(),
            Profile {
                id: "AKID".into(),
                secret: "s/e+cret=".into(),
                region: "eu-west-1".into(),
                endpoint: Some("http://127.0.0.1:9000".into()),
                authority: der,
            }
        );
        // No config: S3's default region, no endpoint.
        std::fs::remove_file(dir.path().join("config")).unwrap();
        let read = Profile::read(dir.path()).unwrap();
        assert_eq!((read.region.as_str(), read.endpoint), ("us-east-1", None));
        // A PEM of two certificates, or of none, is refused.
        let two = format!("{}{}", mantle_cert::pem(&[1]), mantle_cert::pem(&[2]));
        std::fs::write(dir.path().join("authority.pem"), two).unwrap();
        assert!(Profile::read(dir.path()).is_err());
        std::fs::write(dir.path().join("authority.pem"), "nothing").unwrap();
        assert!(Profile::read(dir.path()).is_err());
    }
}
