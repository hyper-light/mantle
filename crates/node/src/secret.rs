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
    #[cfg(unix)]
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

    /// On Windows a key made as `mantle init` makes one ([`create`]: its DACL its owner's and
    /// LocalSystem's alone, protected) is read whole; one whose DACL also allows Everyone, the
    /// Users group or Authenticated Users to read it, principals never its owner, is refused, and
    /// read again once that entry is gone. (A file `icacls` cuts to its owner's entries is no such
    /// file on an elevated runner: `/inheritance:r` beside `/grant:r` left explicit copies of the
    /// inherited entries, the Administrators group's among them, which the check rightly refused.)
    #[cfg(windows)]
    #[test]
    fn a_key_others_may_read_is_refused_and_its_owners_alone_is_read() {
        use std::io::Write as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.key");
        create(&path).unwrap().write_all(&[7u8; 48]).unwrap();
        let icacls = |args: &[&str]| {
            let out = std::process::Command::new("icacls")
                .arg(&path)
                .args(args)
                .output()
                .unwrap();
            assert!(out.status.success(), "icacls {args:?}: {out:?}");
        };
        let owners_alone = |when: &str| {
            let read = read(&path);
            assert!(
                matches!(&read, Ok(bytes) if *bytes == [7u8; 48]),
                "{when}: {read:?}, the file's security {}",
                sddl(&path)
            );
        };
        owners_alone("its owner's and LocalSystem's alone");
        for other in ["*S-1-1-0", "*S-1-5-32-545", "*S-1-5-11"] {
            icacls(&["/grant", &format!("{other}:R")]);
            let read = read(&path);
            assert!(
                matches!(read, Err(SecretError::Exposed)),
                "{other} allowed to read: {read:?}, the file's security {}",
                sddl(&path)
            );
            icacls(&["/remove:g", other]);
            owners_alone(&format!("{other} removed"));
        }
    }

    /// The file's security descriptor as SDDL, its owner and every DACL entry, said when a verdict
    /// is not the one expected.
    #[cfg(windows)]
    fn sddl(path: &std::path::Path) -> String {
        let out = std::process::Command::new("powershell")
            .args(["-NoProfile", "-Command"])
            .arg(format!("(Get-Acl -LiteralPath '{}').Sddl", path.display()))
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_owned()
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
