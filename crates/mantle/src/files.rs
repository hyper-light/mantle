//! Files `mantle init` keeps, each written whole and durable under a temporary name, then renamed over
//! its place and its directory flushed (CLAUDE.md §6). Keys (encryption.md §2: the root key "32
//! bytes created at initialization with owner-only permissions"; the node's and the authority's
//! private keys) are never readable by another principal, not even for a moment: on Unix they are
//! created with mode 0600; on Windows with a protected DACL that grants their owner and LocalSystem
//! alone and inherits nothing from their directory, the two principals hyper-seal's check of a key
//! file admits (vendor/hyper-seal `file_windows.rs`).

use std::fs::File;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use hyper_block::DiskError;

#[cfg(windows)]
mod windows;

fn io(op: &'static str, path: &Path) -> impl FnOnce(std::io::Error) -> DiskError {
    let path = path.to_path_buf();
    move |source| DiskError::Io { op, path, source }
}

/// `bytes` kept at `path`, whole or not at all, and durable when this returns. A file already at
/// `path` is replaced.
pub fn write(path: &Path, bytes: &[u8]) -> Result<(), DiskError> {
    keep(path, bytes, false)
}

/// `bytes` kept at `path` as [`write`] keeps them, readable by its owner alone; refused, the file
/// removed, where its file system does not keep it so (one that ignores permission bits, as FAT
/// and some network file systems do).
pub fn write_secret(path: &Path, bytes: &[u8]) -> Result<(), DiskError> {
    keep(path, bytes, true)?;
    if owner_only(path)? {
        return Ok(());
    }
    std::fs::remove_file(path).map_err(io("remove", path))?;
    Err(DiskError::Io {
        op: "keep to its owner",
        path: path.to_path_buf(),
        source: std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "the file system lets others read a file created for its owner alone",
        ),
    })
}

/// A directory made at `path` and each of its parents missing, each entry durable: every directory
/// made flushed with its parent (CLAUDE.md §6).
pub fn create_dir(path: &Path) -> Result<(), DiskError> {
    let mut missing = Vec::new();
    let mut at = Some(path);
    while let Some(dir) = at.filter(|dir| !dir.as_os_str().is_empty() && !dir.exists()) {
        missing.push(dir.to_path_buf());
        at = dir.parent();
    }
    for dir in missing.iter().rev() {
        std::fs::create_dir(dir).map_err(io("create", dir))?;
        hyper_block::file::sync_dir(&parent(dir))?;
    }
    Ok(())
}

/// The directory `path` is in: the working directory for a bare name.
fn parent(path: &Path) -> PathBuf {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

fn keep(path: &Path, bytes: &[u8], secret: bool) -> Result<(), DiskError> {
    let mut name = path.as_os_str().to_owned();
    name.push(".new");
    let temporary = PathBuf::from(name);
    // A temporary name a crash left behind is no one's: created anew, owner-only.
    match std::fs::remove_file(&temporary) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(io("remove", &temporary)(e)),
    }
    {
        let mut file = if secret {
            create(&temporary)?
        } else {
            File::create_new(&temporary).map_err(io("create", &temporary))?
        };
        file.write_all(bytes).map_err(io("write", &temporary))?;
        file.sync_data().map_err(io("sync_data", &temporary))?;
    }
    std::fs::rename(&temporary, path).map_err(io("rename", path))?;
    hyper_block::file::sync_dir(&parent(path))
}

/// A new file at `path`, readable and writable by its owner alone from the moment it exists:
/// refused where a file is there already.
#[cfg(unix)]
fn create(path: &Path) -> Result<File, DiskError> {
    use std::os::unix::fs::OpenOptionsExt as _;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(io("create", path))
}

#[cfg(windows)]
fn create(path: &Path) -> Result<File, DiskError> {
    windows::create(path).map_err(io("create", path))
}

/// Whether the file at `path` is readable by its owner alone: no permission bit for its group or
/// others on Unix; on Windows a protected DACL whose every entry grants its owner or LocalSystem.
#[cfg(unix)]
pub fn owner_only(path: &Path) -> Result<bool, DiskError> {
    use std::os::unix::fs::PermissionsExt as _;
    let mode = std::fs::metadata(path)
        .map_err(io("stat", path))?
        .permissions()
        .mode();
    Ok(mode & 0o077 == 0)
}

#[cfg(windows)]
pub fn owner_only(path: &Path) -> Result<bool, DiskError> {
    windows::owner_only(path).map_err(io("read the security of", path))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A key is written whole, readable by its owner alone, and a second write replaces it the
    /// same; a temporary name a crash left behind is replaced, not followed; a directory is made
    /// with its missing parents.
    #[test]
    fn a_key_is_written_whole_and_readable_by_its_owner_alone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("root.key");
        write_secret(&path, &[7u8; 32]).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), vec![7u8; 32]);
        assert!(owner_only(&path).unwrap());
        std::fs::write(dir.path().join("root.key.new"), b"left by a crash").unwrap();
        write_secret(&path, &[9u8; 32]).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), vec![9u8; 32]);
        assert!(owner_only(&path).unwrap());
        assert!(!dir.path().join("root.key.new").exists());
        let deep = dir.path().join("a").join("b").join("c");
        create_dir(&deep).unwrap();
        assert!(deep.is_dir());
        write(&deep.join("config"), b"id = 1\n").unwrap();
        assert_eq!(std::fs::read(deep.join("config")).unwrap(), b"id = 1\n");
        // A file made as any other is not owner-only where the directory lets others read.
        let open = dir.path().join("open");
        std::fs::write(&open, b"x").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(!owner_only(&open).unwrap());
        }
    }
}
