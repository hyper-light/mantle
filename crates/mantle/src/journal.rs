//! The command line's journal of its open native uploads (docs/design/gateway.md §6), kept through
//! the client library's [`Watch`](mantle_client::Watch): a record of each upload a copy opened,
//! written and flushed before its first run is sent and removed once the upload is answered, so a
//! copy run again after a crash or a kill finds its upload and resumes it from the node's offset,
//! with no committed run sent again.
//!
//! A record is the destination, the source, the operation's identity and the upload's ID: the
//! source a file's path, length, modification time and, on Unix, its device and inode, or standard
//! input. A file whose length, time or identity changed is a new copy, never a resume, and its old
//! upload is aborted; one changed where those checks cannot see is caught at the Finish, whose
//! full-object checksum covers every byte as the source holds it now (`mantle_client::Client::
//! upload`). A record is a file of its own under the client's directory, named by the SHA-256 of
//! its destination and its source's name, written under a temporary name, flushed (`sync_data`,
//! the platform's full flush), renamed into place and its directory flushed (CLAUDE.md §6). It
//! ends in the CRC-32C of what precedes it, checked on every read: a mismatch is a typed error,
//! never a resume.
//!
//! **Its bound.** The journal holds at most [`MAX_OPEN`] records, one per upload the client left
//! open, as many as one page of ListMultipartUploads lists, so `mantle multipart uploads` shows
//! every upload the journal names; a copy past them is refused until one is finished or aborted.

use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use mantle_codec::{Reader, Writer};
use mantle_protocol::object::MAX_PAGE_PARTS;

/// The most open uploads the journal records: S3's 1,000 uploads a page of ListMultipartUploads
/// lists (05 §4.7), so one listing shows every upload the journal names.
pub const MAX_OPEN: usize = MAX_PAGE_PARTS;

/// What begins every record, and its format.
const MAGIC: &[u8; 4] = b"MNJ1";

/// Why the journal did not do what it was asked.
#[derive(Debug)]
pub enum JournalError {
    Io(PathBuf, std::io::Error),
    /// A record whose checksum or fields do not hold.
    Corrupt(PathBuf),
    /// As many uploads are open as the journal records.
    Full,
}

impl std::fmt::Display for JournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(path, e) => write!(f, "{}: {e}", path.display()),
            Self::Corrupt(path) => {
                write!(f, "{}: a journal record that does not hold", path.display())
            }
            Self::Full => write!(
                f,
                "the journal records {MAX_OPEN} open uploads: finish or abort one first"
            ),
        }
    }
}

/// Where a copy's bytes come from, as a record names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Origin {
    /// A file: its path, length, modification time (nanoseconds since the Unix epoch) and, on
    /// Unix, its device and inode.
    File {
        path: PathBuf,
        length: u64,
        modified_ns: u64,
        identity: Option<(u64, u64)>,
    },
    /// Standard input, which has no identity to check.
    Standard,
}

impl Origin {
    /// The file at `path` as its metadata describes it now.
    pub fn file(path: &Path) -> Result<Self, JournalError> {
        let fail = |e| JournalError::Io(path.to_path_buf(), e);
        let canonical = fs::canonicalize(path).map_err(fail)?;
        let meta = fs::metadata(&canonical).map_err(fail)?;
        let modified_ns = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .and_then(|d| u64::try_from(d.as_nanos()).ok())
            .unwrap_or(0);
        Ok(Self::File {
            path: canonical,
            length: meta.len(),
            modified_ns,
            identity: identity(&meta),
        })
    }

    /// The name a record is found by: the path, or `-`.
    fn name(&self) -> String {
        match self {
            Self::File { path, .. } => path.to_string_lossy().into_owned(),
            Self::Standard => "-".to_owned(),
        }
    }
}

/// A file's device and inode, which a rename or a replaced file changes.
#[cfg(unix)]
fn identity(meta: &fs::Metadata) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt as _;
    Some((meta.dev(), meta.ino()))
}

/// Windows names a file by its volume and file index only through an interface the standard
/// library does not yet make stable; its length and time stand alone there.
#[cfg(not(unix))]
fn identity(_: &fs::Metadata) -> Option<(u64, u64)> {
    None
}

/// An open upload as the journal records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub bucket: String,
    pub key: String,
    pub origin: Origin,
    /// The operation's identity, which every attempt of it carries.
    pub id: u128,
    pub upload: String,
}

impl Record {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::default();
        w.bytes(MAGIC);
        text(&mut w, &self.bucket);
        text(&mut w, &self.key);
        match &self.origin {
            Origin::Standard => w.u8(0),
            Origin::File {
                path,
                length,
                modified_ns,
                identity,
            } => {
                w.u8(1);
                text(&mut w, &path.to_string_lossy());
                w.u64(*length);
                w.u64(*modified_ns);
                match identity {
                    None => w.u8(0),
                    Some((device, inode)) => {
                        w.u8(1);
                        w.u64(*device);
                        w.u64(*inode);
                    }
                }
            }
        }
        w.u128(self.id);
        text(&mut w, &self.upload);
        let crc = mantle_crc::crc32c(w.as_slice());
        w.u32(crc);
        w.into_vec()
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let body = bytes.len().checked_sub(4)?;
        let (fields, crc) = bytes.split_at_checked(body)?;
        if mantle_crc::crc32c(fields).to_le_bytes() != *crc {
            return None;
        }
        let mut r = Reader::new(fields);
        if r.take(MAGIC.len())? != MAGIC {
            return None;
        }
        let bucket = take_text(&mut r)?;
        let key = take_text(&mut r)?;
        let origin = match r.u8()? {
            0 => Origin::Standard,
            1 => Origin::File {
                path: PathBuf::from(take_text(&mut r)?),
                length: r.u64()?,
                modified_ns: r.u64()?,
                identity: match r.u8()? {
                    0 => None,
                    1 => Some((r.u64()?, r.u64()?)),
                    _ => return None,
                },
            },
            _ => return None,
        };
        let id = r.u128()?;
        let upload = take_text(&mut r)?;
        if r.remaining() != 0 {
            return None;
        }
        Some(Self {
            bucket,
            key,
            origin,
            id,
            upload,
        })
    }
}

fn text(w: &mut Writer, text: &str) {
    // A bucket, a key, a path and an upload's ID are each far within a u32.
    w.u32(u32::try_from(text.len()).unwrap_or(u32::MAX));
    w.bytes(text.as_bytes());
}

fn take_text(r: &mut Reader<'_>) -> Option<String> {
    let len = usize::try_from(r.u32()?).ok()?;
    String::from_utf8(r.take(len)?.to_vec()).ok()
}

/// The journal under a client's directory.
pub struct Journal {
    dir: PathBuf,
}

impl Journal {
    /// The journal of the client whose directory is `client`, made if it is not there.
    pub fn open(client: &Path) -> Result<Self, JournalError> {
        let dir = client.join("journal");
        if !dir.is_dir() {
            fs::create_dir_all(&dir).map_err(|e| JournalError::Io(dir.clone(), e))?;
            sync_dir(client)?;
        }
        Ok(Self { dir })
    }

    /// The record of an upload to `bucket`'s `key` from `origin`'s source, if the journal holds
    /// one: found by the source's name, whatever its length, time or identity now are, which the
    /// caller compares.
    pub fn find(
        &self,
        bucket: &str,
        key: &str,
        origin: &Origin,
    ) -> Result<Option<Record>, JournalError> {
        let path = self.path(bucket, key, origin)?;
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(JournalError::Io(path, e)),
        };
        let record = Record::decode(&bytes).ok_or_else(|| JournalError::Corrupt(path.clone()))?;
        if record.bucket != bucket || record.key != key || record.origin.name() != origin.name() {
            return Err(JournalError::Corrupt(path));
        }
        Ok(Some(record))
    }

    /// Keeps `record`, flushed and its directory flushed, before the upload's first run is sent;
    /// refused where the journal holds [`MAX_OPEN`] other records.
    pub fn keep(&self, record: &Record) -> Result<(), JournalError> {
        let path = self.path(&record.bucket, &record.key, &record.origin)?;
        if !path.exists() && self.count()? >= MAX_OPEN {
            return Err(JournalError::Full);
        }
        let mut name = path.as_os_str().to_owned();
        name.push(".tmp");
        let temporary = PathBuf::from(name);
        let fail = |at: &Path, e| JournalError::Io(at.to_path_buf(), e);
        let mut file = File::create(&temporary).map_err(|e| fail(&temporary, e))?;
        file.write_all(&record.encode())
            .map_err(|e| fail(&temporary, e))?;
        file.sync_data().map_err(|e| fail(&temporary, e))?;
        drop(file);
        fs::rename(&temporary, &path).map_err(|e| fail(&path, e))?;
        sync_dir(&self.dir)
    }

    /// Removes `record`'s file, its upload answered or abandoned, and flushes its directory.
    pub fn forget(&self, record: &Record) -> Result<(), JournalError> {
        let path = self.path(&record.bucket, &record.key, &record.origin)?;
        match fs::remove_file(&path) {
            Ok(()) => sync_dir(&self.dir),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(JournalError::Io(path, e)),
        }
    }

    /// The records the journal holds: its files but those half written.
    fn count(&self) -> Result<usize, JournalError> {
        let entries = fs::read_dir(&self.dir).map_err(|e| JournalError::Io(self.dir.clone(), e))?;
        let mut count = 0usize;
        for entry in entries {
            let entry = entry.map_err(|e| JournalError::Io(self.dir.clone(), e))?;
            if entry.path().extension().is_none() {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    /// A record's file: the hex SHA-256 of its destination and its source's name.
    fn path(&self, bucket: &str, key: &str, origin: &Origin) -> Result<PathBuf, JournalError> {
        let mut named = Vec::new();
        for part in [bucket, key, &origin.name()] {
            named.extend_from_slice(&u64::try_from(part.len()).unwrap_or(u64::MAX).to_le_bytes());
            named.extend_from_slice(part.as_bytes());
        }
        let digest = mantle_s3::crypto::sha256(&named)
            .map_err(|_| JournalError::Corrupt(self.dir.clone()))?;
        let name: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        Ok(self.dir.join(name))
    }
}

fn sync_dir(dir: &Path) -> Result<(), JournalError> {
    hyper_block::file::sync_dir(dir)
        .map_err(|e| JournalError::Io(dir.to_path_buf(), std::io::Error::other(e.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(origin: Origin) -> Record {
        Record {
            bucket: "b".into(),
            key: "a\nkey".into(),
            origin,
            id: 0x1234,
            upload: "0123456789ABC".into(),
        }
    }

    /// A record kept is found again by its destination and source, whatever the file's length
    /// is now; forgotten, it is gone; a byte changed in it is a corrupt record, never a resume.
    #[test]
    fn a_record_kept_is_found_and_checked() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::open(dir.path()).unwrap();
        let source = dir.path().join("source");
        fs::write(&source, b"bytes").unwrap();
        let origin = Origin::file(&source).unwrap();
        let kept = record(origin.clone());
        journal.keep(&kept).unwrap();
        assert_eq!(
            journal.find("b", "a\nkey", &origin).unwrap(),
            Some(kept.clone())
        );
        assert_eq!(journal.find("b", "other", &origin).unwrap(), None);
        let standard = record(Origin::Standard);
        journal.keep(&standard).unwrap();
        assert_eq!(
            journal.find("b", "a\nkey", &Origin::Standard).unwrap(),
            Some(standard.clone())
        );
        // The file changed: still found by its name, its length now another.
        fs::write(&source, b"longer bytes").unwrap();
        let changed = Origin::file(&source).unwrap();
        let found = journal.find("b", "a\nkey", &changed).unwrap().unwrap();
        assert_ne!(found.origin, changed);
        journal.forget(&kept).unwrap();
        assert_eq!(journal.find("b", "a\nkey", &origin).unwrap(), None);
        let path = journal.path("b", "a\nkey", &Origin::Standard).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        bytes[6] ^= 1;
        fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            journal.find("b", "a\nkey", &Origin::Standard),
            Err(JournalError::Corrupt(_))
        ));
    }

    /// Past its bound the journal refuses a new record, and keeps taking the ones it holds.
    #[test]
    fn the_journal_holds_its_bound() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::open(dir.path()).unwrap();
        // The bound's records written as `keep` writes them, without a flush each.
        for n in 0..MAX_OPEN {
            let mut r = record(Origin::Standard);
            r.key = format!("k{n}");
            let path = journal.path(&r.bucket, &r.key, &r.origin).unwrap();
            fs::write(path, r.encode()).unwrap();
        }
        let mut past = record(Origin::Standard);
        past.key = "past".into();
        assert!(matches!(journal.keep(&past), Err(JournalError::Full)));
        let mut again = record(Origin::Standard);
        again.key = "k0".into();
        journal.keep(&again).unwrap();
    }
}
