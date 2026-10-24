//! Key files: created readable by their owner alone from the moment they exist, and read only from
//! a file no one but its owner may read (node.md §1.7; encryption.md §2), by the rule hyper-seal's
//! key files keep (vendor/hyper-seal `file.rs`): on Unix no permission bit for the file's group or
//! others, as OpenSSH (`sshkey_perm_ok`) and PostgreSQL refuse a private key; on Windows no allow
//! entry in the file's DACL for any principal but its owner and LocalSystem. `mantle init` writes
//! keys with [`create`], `mantle serve` reads them with [`read`]. The check reads the open file, the
//! one then read, so a file put in its place between the two is never read unchecked.

use std::fs::File;
use std::io::Read as _;
use std::path::Path;

#[cfg(windows)]
mod windows;

/// What reading a key file refuses.
#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("is readable by others than its owner")]
    Exposed,
}

/// A new file at `path`, readable and writable by its owner alone from the moment it exists:
/// refused where a file is there already.
#[cfg(unix)]
pub fn create(path: &Path) -> Result<File, std::io::Error> {
    use std::os::unix::fs::OpenOptionsExt as _;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

/// A new file at `path`, its DACL granting its owner and LocalSystem alone, inherited from nothing:
/// refused where a file is there already.
#[cfg(windows)]
pub fn create(path: &Path) -> Result<File, std::io::Error> {
    windows::create(path)
}

/// The bytes of the key file at `path`, read through the handle its permissions were read from.
pub fn read(path: &Path) -> Result<Vec<u8>, SecretError> {
    let mut file = File::open(path)?;
    if !owner_only(&file)? {
        return Err(SecretError::Exposed);
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

/// Whether no one but `file`'s owner may read it: no permission bit for its group or others.
#[cfg(unix)]
pub fn owner_only(file: &File) -> Result<bool, std::io::Error> {
    use std::os::unix::fs::PermissionsExt as _;
    Ok(file.metadata()?.permissions().mode() & 0o077 == 0)
}

/// Whether no one but `file`'s owner may read it: each entry of its DACL that allows anything
/// allows its owner or LocalSystem.
#[cfg(windows)]
pub fn owner_only(file: &File) -> Result<bool, std::io::Error> {
    windows::owner_only(file)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A key file in a directory of the test's own, holding `bytes`.
    fn key(bytes: &[u8]) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.key");
        std::fs::write(&path, bytes).unwrap();
        (dir, path)
    }

    /// A key at 0600 is read whole; one any group or other permission bit opens is refused,
    /// whichever bit it is.
    #[cfg(unix)]
    #[test]
    fn a_key_others_may_read_is_refused_and_its_owners_alone_is_read() {
        use std::os::unix::fs::PermissionsExt as _;
        let (_dir, path) = key(&[7u8; 48]);
        let mode = |mode: u32| {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        };
        mode(0o600);
        assert_eq!(read(&path).unwrap(), vec![7u8; 48]);
        mode(0o400);
        assert_eq!(read(&path).unwrap(), vec![7u8; 48]);
        for open in [0o644, 0o640, 0o604, 0o620, 0o602, 0o610, 0o601] {
            mode(open);
            assert!(
                matches!(read(&path), Err(SecretError::Exposed)),
                "{open:o} refused"
            );
        }
    }

    /// On Windows a key its owner and LocalSystem alone may access is read whole; one whose DACL
    /// allows Everyone, the Users group or the Administrators group (none of them the file's owner
    /// here) is refused, and read again once that entry is gone. The test's account is made the
    /// file's owner and its DACL cut from the directory's entries, which differ from host to host.
    #[cfg(windows)]
    #[test]
    fn a_key_others_may_read_is_refused_and_its_owners_alone_is_read() {
        let (_dir, path) = key(&[7u8; 48]);
        let icacls = |args: &[&str]| {
            let out = std::process::Command::new("icacls")
                .arg(&path)
                .args(args)
                .output()
                .unwrap();
            assert!(out.status.success(), "icacls {args:?}: {out:?}");
        };
        let me = format!("*{}", user_sid());
        icacls(&["/setowner", &me]);
        icacls(&[
            "/inheritance:r",
            "/grant:r",
            &format!("{me}:F"),
            "*S-1-5-18:F",
        ]);
        assert_eq!(read(&path).unwrap(), vec![7u8; 48]);
        for other in ["*S-1-1-0", "*S-1-5-32-545", "*S-1-5-32-544"] {
            icacls(&["/grant", &format!("{other}:R")]);
            assert!(
                matches!(read(&path), Err(SecretError::Exposed)),
                "{other} refused"
            );
            icacls(&["/remove:g", other]);
            assert_eq!(read(&path).unwrap(), vec![7u8; 48]);
        }
    }

    /// The SID of the account the test runs as, the second field of `whoami /user /fo csv /nh`.
    #[cfg(windows)]
    fn user_sid() -> String {
        let out = std::process::Command::new("whoami")
            .args(["/user", "/fo", "csv", "/nh"])
            .output()
            .unwrap();
        assert!(out.status.success(), "whoami: {out:?}");
        let line = String::from_utf8(out.stdout).unwrap();
        let (_, sid) = line.trim().rsplit_once(',').unwrap();
        sid.trim_matches('"').to_owned()
    }

    /// A key file that is not there is refused as the system's error, not as exposed.
    #[test]
    fn a_missing_key_is_the_systems_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing = read(&dir.path().join("gone.key"));
        assert!(
            matches!(missing, Err(SecretError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound)
        );
    }
}
