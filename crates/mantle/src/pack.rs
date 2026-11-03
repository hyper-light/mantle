//! `mantle pack DIR FILE` and `mantle install FILE TO`: a directory carried whole as one file, its
//! keys marked, and installed again with each key readable by its owner alone (research/78). A
//! founder's directory `mantle init` made reaches its pod as one Kubernetes Secret's value: a
//! Secret's keys name flat files, while a founder's directory holds directories and files whose
//! names its node ids choose, and a projected file takes the volume's mode, 0644 unless stated,
//! while `mantle serve` reads a key only from a file no one but its owner may read.
//!
//! A pack is the bytes `MNTLPACK` and its version, then each file under the directory: its path,
//! its names joined by `/`, whether it is a key (a file its owner alone may read where it was
//! packed), and its bytes, each length before what it counts; and last the CRC-32C of everything
//! before it (CLAUDE.md §6). It holds at most a Secret's bytes.
//!
//! An install writes each file whole and durable (`files`), a key owner-only, and the directory's
//! `config` last, so a directory holding a configuration holds every file the pack carried, and an
//! install a crash cut short is made whole by the next. A directory holding a configuration
//! already is kept as it is: a node's state lives beside it.

use std::collections::BTreeSet;
use std::fmt;
use std::io::{Read as _, Write};
use std::path::{Path, PathBuf};

use crate::files;

/// The pack's first bytes.
const MAGIC: [u8; 8] = *b"MNTLPACK";
/// The pack's version.
const VERSION: u16 = 1;
/// The configuration's name, which an install writes last and finds a directory installed by.
const CONFIG: &str = "config";
/// The most bytes a pack holds: a Kubernetes Secret's ("Individual Secrets are limited to 1MiB in
/// size", research/78 [10]).
pub const MAX_PACK: usize = 1 << 20;
/// A file's place in a pack before its path and bytes: the path's length, the key mark and the
/// bytes' length.
const HEAD: usize = 2 + 1 + 4;
/// The CRC-32C after the files.
const TRAILER: usize = 4;
/// What a file's place and the pack's checksum add to its path and bytes.
const OVERHEAD: usize = HEAD + TRAILER;

#[derive(Debug)]
pub enum Error {
    Output(std::io::Error),
    /// A file or directory that does not read.
    Read(PathBuf, std::io::Error),
    /// A name not UTF-8, or one a path within a directory cannot be made of.
    Name(PathBuf),
    /// Neither a file nor a directory: a link, a device, a socket.
    Kind(PathBuf),
    /// Past the most bytes a pack holds.
    TooLarge,
    /// The pack's bytes are not the ones its CRC-32C was made over.
    Corrupt,
    /// The pack does not read as one: why.
    Malformed(&'static str),
    Disk(hyper_block::DiskError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Output(e) => write!(f, "writing output: {e}"),
            Self::Read(path, e) => write!(f, "{}: {e}", path.display()),
            Self::Name(path) => write!(
                f,
                "{}: a name a pack's path cannot hold (UTF-8, and no `/`, `\\` or `:`)",
                path.display()
            ),
            Self::Kind(path) => write!(f, "{}: neither a file nor a directory", path.display()),
            Self::TooLarge => write!(f, "past a pack's {MAX_PACK} bytes, a Secret's most"),
            Self::Corrupt => write!(f, "the pack's checksum does not match its bytes"),
            Self::Malformed(why) => write!(f, "not a pack: {why}"),
            Self::Disk(e) => write!(f, "{e}"),
        }
    }
}

impl From<hyper_block::DiskError> for Error {
    fn from(e: hyper_block::DiskError) -> Self {
        Self::Disk(e)
    }
}

/// A file a pack carries: its path, whether it is a key, and its bytes.
struct Packed<'a> {
    path: &'a str,
    key: bool,
    bytes: &'a [u8],
}

/// Packs every file under `dir` into `file`, written whole and durable, saying on `said` what it
/// packed. Directories are read from a list, not by a call's recursion, each in its names' order,
/// so a directory packs to the same bytes each time.
pub fn pack(said: &mut impl Write, dir: &Path, file: &Path) -> Result<(), Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&MAGIC);
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    let mut files = 0u64;
    let mut keys = 0u64;
    let mut open = vec![(dir.to_path_buf(), String::new())];
    while let Some((at, under)) = open.pop() {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(&at).map_err(|e| Error::Read(at.clone(), e))? {
            let entry = entry.map_err(|e| Error::Read(at.clone(), e))?;
            // Each name is a byte of the pack at least.
            if names.len() >= MAX_PACK {
                return Err(Error::TooLarge);
            }
            names.push(entry.file_name());
        }
        names.sort();
        // Read last first off the list: pushed in reverse, the directories go in names' order.
        let mut directories = Vec::new();
        for name in names {
            let path = at.join(&name);
            let name = name
                .to_str()
                .filter(|name| fits(name))
                .ok_or_else(|| Error::Name(path.clone()))?;
            let inner = if under.is_empty() {
                name.to_owned()
            } else {
                format!("{under}/{name}")
            };
            let kind = std::fs::symlink_metadata(&path)
                .map_err(|e| Error::Read(path.clone(), e))?
                .file_type();
            if kind.is_dir() {
                directories.push((path, inner));
            } else if kind.is_file() {
                let key = files::owner_only(&path)?;
                let room = MAX_PACK
                    .checked_sub(bytes.len())
                    .and_then(|room| room.checked_sub(OVERHEAD))
                    .and_then(|room| room.checked_sub(inner.len()))
                    .ok_or(Error::TooLarge)?;
                let mut body = Vec::new();
                std::fs::File::open(&path)
                    .and_then(|f| {
                        f.take(u64::try_from(room).unwrap_or(u64::MAX).saturating_add(1))
                            .read_to_end(&mut body)
                    })
                    .map_err(|e| Error::Read(path.clone(), e))?;
                if body.len() > room {
                    return Err(Error::TooLarge);
                }
                put(
                    &mut bytes,
                    &Packed {
                        path: &inner,
                        key,
                        bytes: &body,
                    },
                )?;
                files = files.saturating_add(1);
                keys = keys.saturating_add(u64::from(key));
            } else {
                return Err(Error::Kind(path));
            }
        }
        open.extend(directories.into_iter().rev());
    }
    let crc = mantle_crc::crc32c(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    if bytes.len() > MAX_PACK {
        return Err(Error::TooLarge);
    }
    files::write(file, &bytes)?;
    writeln!(
        said,
        "packed {} into {}: {files} files, {keys} keys, {} bytes",
        dir.display(),
        file.display(),
        bytes.len()
    )
    .map_err(Error::Output)
}

/// Installs the pack at `from` into the directory `to`, saying on `said` what it did.
pub fn install(said: &mut impl Write, from: &Path, to: &Path) -> Result<(), Error> {
    let config = to.join(CONFIG);
    if config
        .try_exists()
        .map_err(|e| Error::Read(config.clone(), e))?
    {
        writeln!(said, "{}: installed already, kept", to.display()).map_err(Error::Output)?;
        return Ok(());
    }
    let mut bytes = Vec::new();
    std::fs::File::open(from)
        .and_then(|f| {
            f.take(
                u64::try_from(MAX_PACK)
                    .unwrap_or(u64::MAX)
                    .saturating_add(1),
            )
            .read_to_end(&mut bytes)
        })
        .map_err(|e| Error::Read(from.to_path_buf(), e))?;
    if bytes.len() > MAX_PACK {
        return Err(Error::TooLarge);
    }
    let packed = read(&bytes)?;
    files::create_dir(to)?;
    let (last, rest): (Vec<&Packed<'_>>, Vec<&Packed<'_>>) =
        packed.iter().partition(|file| file.path == CONFIG);
    for file in rest.into_iter().chain(last) {
        let at = file
            .path
            .split('/')
            .fold(to.to_path_buf(), |at, name| at.join(name));
        if let Some(parent) = at.parent() {
            files::create_dir(parent)?;
        }
        if file.key {
            files::write_secret(&at, file.bytes)?;
        } else {
            files::write(&at, file.bytes)?;
        }
    }
    writeln!(
        said,
        "installed {} into {}: {} files",
        from.display(),
        to.display(),
        packed.len()
    )
    .map_err(Error::Output)
}

/// Whether `name` can be one name of a pack's path: not empty, not `.` or `..`, and holding no
/// separator of any platform's paths, nor a NUL.
fn fits(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\\', ':', '\0'])
}

/// Appends `file` to `bytes`.
fn put(bytes: &mut Vec<u8>, file: &Packed<'_>) -> Result<(), Error> {
    let path = u16::try_from(file.path.len()).map_err(|_| Error::TooLarge)?;
    let length = u32::try_from(file.bytes.len()).map_err(|_| Error::TooLarge)?;
    bytes.extend_from_slice(&path.to_le_bytes());
    bytes.extend_from_slice(file.path.as_bytes());
    bytes.push(u8::from(file.key));
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(file.bytes);
    Ok(())
}

/// The files the pack `bytes` carries, its checksum checked first; refused where a path is not
/// one within a directory or names a file twice.
fn read(bytes: &[u8]) -> Result<Vec<Packed<'_>>, Error> {
    let body_len = bytes
        .len()
        .checked_sub(TRAILER)
        .ok_or(Error::Malformed("shorter than its checksum"))?;
    let (body, crc) = bytes.split_at(body_len);
    if mantle_crc::crc32c(body).to_le_bytes().as_slice() != crc {
        return Err(Error::Corrupt);
    }
    let (magic, rest) = body
        .split_at_checked(MAGIC.len())
        .ok_or(Error::Malformed("no magic"))?;
    if magic != MAGIC {
        return Err(Error::Malformed("no magic"));
    }
    let (version, mut rest) = take::<2>(rest)?;
    if u16::from_le_bytes(version) != VERSION {
        return Err(Error::Malformed("a version this build does not read"));
    }
    let mut packed = Vec::new();
    let mut seen = BTreeSet::new();
    while !rest.is_empty() {
        let (length, after) = take::<2>(rest)?;
        let (path, after) = after
            .split_at_checked(usize::from(u16::from_le_bytes(length)))
            .ok_or(Error::Malformed("a path past the end"))?;
        let path = std::str::from_utf8(path).map_err(|_| Error::Malformed("a path not UTF-8"))?;
        if !path.split('/').all(fits) {
            return Err(Error::Malformed("a path not one within a directory"));
        }
        if !seen.insert(path) {
            return Err(Error::Malformed("a path twice"));
        }
        let (&mark, after) = after
            .split_first()
            .ok_or(Error::Malformed("a file without its key mark"))?;
        let key = match mark {
            0 => false,
            1 => true,
            _ => return Err(Error::Malformed("a key mark neither 0 nor 1")),
        };
        let (length, after) = take::<4>(after)?;
        let length = usize::try_from(u32::from_le_bytes(length))
            .map_err(|_| Error::Malformed("a file past the end"))?;
        let (file, after) = after
            .split_at_checked(length)
            .ok_or(Error::Malformed("a file past the end"))?;
        packed.push(Packed {
            path,
            key,
            bytes: file,
        });
        rest = after;
    }
    Ok(packed)
}

/// The first `N` bytes of `bytes` and the rest.
fn take<const N: usize>(bytes: &[u8]) -> Result<([u8; N], &[u8]), Error> {
    let (head, rest) = bytes
        .split_first_chunk::<N>()
        .ok_or(Error::Malformed("ends inside a length"))?;
    Ok((*head, rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A founder `mantle init` made packs and installs with every file as it was, the keys
    /// owner-only; a second install keeps what the first made; a pack with a byte changed, or a
    /// path that climbs out of its directory, installs nothing.
    #[test]
    fn a_founder_packs_and_installs_whole_with_its_keys_owner_only_once() {
        let made = tempfile::tempdir().unwrap();
        let founders = (0..3u16)
            .map(|n| crate::init::Founder {
                quic: std::net::SocketAddr::from(([127, 0, 0, 1], 7000 + 2 * n)),
                plane: std::net::SocketAddr::from(([127, 0, 0, 1], 7001 + 2 * n)),
                s3: None,
                names: Vec::new(),
                domain: None,
                volumes: Vec::new(),
                memory: None,
                reach: None,
            })
            .collect();
        let cell = crate::init::Cell {
            founders,
            ..crate::init::Cell::default()
        };
        crate::init::init(&mut Vec::new(), made.path(), &cell).unwrap();
        crate::init::pin_gateway(&mut Vec::new(), &made.path().join("gateway"), made.path())
            .unwrap();
        let from = std::fs::read_dir(made.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("node-")
            })
            .unwrap();
        let work = tempfile::tempdir().unwrap();
        let file = work.path().join("founder");
        let mut said = Vec::new();
        pack(&mut said, &from, &file).unwrap();
        pack(&mut Vec::new(), &from, &work.path().join("again")).unwrap();
        assert_eq!(
            std::fs::read(&file).unwrap(),
            std::fs::read(work.path().join("again")).unwrap()
        );

        let to = work.path().join("node");
        install(&mut said, &file, &to).unwrap();
        for packed in walk(&from) {
            let name = packed.strip_prefix(&from).unwrap();
            assert_eq!(
                std::fs::read(to.join(name)).unwrap(),
                std::fs::read(&packed).unwrap()
            );
            assert_eq!(
                files::owner_only(&to.join(name)).unwrap(),
                files::owner_only(&packed).unwrap(),
                "{}",
                name.display()
            );
        }
        let stated = mantle_node::process::read_stated(&to.join(CONFIG)).unwrap();
        let s3 = stated.s3.as_ref().unwrap();
        let mut keys = vec![&stated.key, &s3.key];
        keys.extend(stated.keys.iter().map(|(_, file)| file));
        keys.extend(s3.access.iter().map(|(_, file)| file));
        for key in keys {
            assert!(key.starts_with(&to));
            assert!(files::owner_only(key).unwrap(), "{}", key.display());
        }
        assert!(stated.peers.iter().any(|peer| peer.certificate.exists()
            && peer.certificate.to_string_lossy().contains("gateway-")));
        std::fs::write(to.join("run"), b"kept").unwrap();
        install(&mut said, &file, &to).unwrap();
        assert_eq!(std::fs::read(to.join("run")).unwrap(), b"kept");
        assert!(
            String::from_utf8(said)
                .unwrap()
                .contains("installed already, kept")
        );

        let mut bytes = std::fs::read(&file).unwrap();
        let middle = bytes.len() / 2;
        bytes[middle] ^= 0x01;
        std::fs::write(&file, &bytes).unwrap();
        let other = work.path().join("other");
        assert!(matches!(
            install(&mut Vec::new(), &file, &other),
            Err(Error::Corrupt)
        ));
        assert!(!other.join(CONFIG).exists());

        let mut climbing = MAGIC.to_vec();
        climbing.extend_from_slice(&VERSION.to_le_bytes());
        put(
            &mut climbing,
            &Packed {
                path: "../config",
                key: false,
                bytes: b"id = 1\n",
            },
        )
        .unwrap();
        let crc = mantle_crc::crc32c(&climbing);
        climbing.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(&file, &climbing).unwrap();
        assert!(matches!(
            install(&mut Vec::new(), &file, &other),
            Err(Error::Malformed(_))
        ));
        assert!(!work.path().join("config").exists());
    }

    /// Every file under `dir`.
    fn walk(dir: &Path) -> Vec<PathBuf> {
        let mut found = Vec::new();
        let mut open = vec![dir.to_path_buf()];
        while let Some(at) = open.pop() {
            for entry in std::fs::read_dir(&at).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    open.push(path);
                } else {
                    found.push(path);
                }
            }
        }
        found
    }
}
