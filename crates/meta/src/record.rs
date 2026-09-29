//! Row values: a format byte, the fields, and a CRC-32C of everything before it, verified
//! before a field is read (CLAUDE.md rule 6). A value whose checksum fails is `Corrupt`.

use mantle_codec::{Reader, Writer};

/// Values written by this code.
const FORMAT: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RecordError {
    #[error("a {0} row failed its checksum or does not decode")]
    Corrupt(&'static str),
    /// A field longer than a length field holds; the protocol's own limits keep every field
    /// far below it.
    #[error("a field of {0} bytes is too long for a row")]
    TooLarge(usize),
}

/// A version of an object: its bytes and what S3 returns about them, or a delete marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    /// A delete marker rather than an object.
    pub marker: bool,
    /// The key's null version (05 §7.1).
    pub null: bool,
    /// When it was written, nanoseconds since the Unix epoch: its `Last-Modified`.
    pub modified_ns: u64,
    /// The ETag without its quotes.
    pub etag: String,
    pub size: u64,
    pub checksum: Option<Checksum>,
    /// The file holding the bytes; none for a delete marker or an empty object.
    pub file: Option<u128>,
    pub owner: String,
    /// Content headers and user metadata, as the object was written with them.
    pub headers: Vec<(String, String)>,
    /// Its Object Lock retention, if one was placed (18 §1.2).
    pub retention: Option<Retention>,
    /// Its legal hold: `None` if none was ever placed, which GetObjectLegalHold answers
    /// `NoSuchObjectLockConfiguration`, else on or off (18 §5).
    pub legal_hold: Option<bool>,
}

/// A retention's mode (18 §2.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionMode {
    /// Changed, shortened or removed only under `s3:BypassGovernanceRetention`.
    Governance,
    /// Never shortened, changed or removed while it lasts.
    Compliance,
}

/// An Object Lock retention: its mode, and the instant it lasts until, Unix milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    pub mode: RetentionMode,
    pub until_ms: i64,
}

/// A bucket's default retention: a mode and a period, days or years (18 §1.1, §2.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DefaultRetention {
    pub mode: RetentionMode,
    pub period: Period,
}

/// A default retention's period, kept in the unit it was set in, so the configuration reads
/// back as set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Period {
    Days(u32),
    Years(u32),
}

/// Milliseconds in a day.
const DAY_MS: i64 = 86_400_000;

impl DefaultRetention {
    /// The retention a version created at `created_ms` takes: "adding the specified duration
    /// to the object version's creation timestamp", a year counted as 365 days, as S3 counts
    /// one for a retention duration (18 §2.4, §2.6). A period reaching past the last instant an
    /// `i64` holds lasts until that instant, as no later one can be compared with it.
    pub fn from(&self, created_ms: i64) -> Retention {
        let days = match self.period {
            Period::Days(days) => i64::from(days),
            Period::Years(years) => i64::from(years).saturating_mul(365),
        };
        Retention {
            mode: self.mode,
            until_ms: created_ms.saturating_add(days.saturating_mul(DAY_MS)),
        }
    }
}

/// An object's checksum as S3 returns it (05 §3.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checksum {
    /// The algorithm's code, as the protocol layer numbers them.
    pub algorithm: u8,
    /// Parts combined for a composite value; zero for a full-object value.
    pub parts: u16,
    pub value: Vec<u8>,
}

/// A multipart upload in progress (05 §4.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Upload {
    /// When it was created: it also orders the version its completion makes (05 §4.4).
    pub initiated_ns: u64,
    pub owner: String,
    pub headers: Vec<(String, String)>,
    /// The checksum algorithm and whether values combine as full-object or composite.
    pub checksum: Option<(u8, bool)>,
    /// The retention and legal hold CreateMultipartUpload's headers named, which the
    /// completed version takes (18 §1.3).
    pub retention: Option<Retention>,
    pub legal_hold: Option<bool>,
}

/// A part of an upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Part {
    pub etag: String,
    pub size: u64,
    pub checksum: Option<Vec<u8>>,
    pub file: u128,
    pub modified_ns: u64,
}

/// A bucket's versioning state (05 §7.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Versioning {
    Unversioned,
    Enabled,
    Suspended,
}

/// Where a bucket is in being created or deleted (docs/design/metadata.md §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketState {
    /// Its gates are being opened.
    Creating,
    Active,
    /// Its gates are closed while the Name ranges are read for versions.
    Deleting,
    /// Gone to requests; the collector is removing its uploads and gates.
    Deleted,
}

/// A bucket's row in the Bucket layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bucket {
    pub owner: String,
    /// When the Bucket range created it, which is also its incarnation.
    pub created_ns: u64,
    /// The location constraint it was created with; empty for the default.
    pub location: String,
    pub versioning: Versioning,
    pub state: BucketState,
    /// The create or delete attempt that last moved it: the Bucket range's time when the
    /// attempt began. A step of an older attempt is refused.
    pub attempt: u64,
    /// The Bucket range's time when that attempt last showed progress: when it began, or its
    /// latest `Progress` step. The collector takes over a create or delete that has gone its
    /// patience without progress (docs/design/metadata.md §2).
    pub progress_ns: u64,
    /// Its Object Lock configuration, once Object Lock is on, which is for good (18 §2.1).
    pub lock: Option<Lock>,
}

/// A bucket's Object Lock configuration: Object Lock is on, and new versions may take a
/// default retention (18 §1.1).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Lock {
    pub default: Option<DefaultRetention>,
}

/// Whether a Name range admits a bucket's writes (docs/design/metadata.md §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateState {
    Open,
    /// Closed while the range is read for versions, and reopened if the bucket stays.
    Closed,
    /// The bucket is deleted: its rows are the collector's.
    Condemned,
}

/// A bucket's gate in a Name range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gate {
    pub incarnation: u64,
    /// The attempt that last moved it.
    pub attempt: u64,
    pub state: GateState,
}

/// An owner's row: its buckets, counting those being created or deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Owner {
    pub buckets: u32,
}

/// A reverse row: one of an owner's active buckets, as ListBuckets shows it (05 §10.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owned {
    pub created_ns: u64,
    pub location: String,
}

/// A gateway's session with a range (docs/design/replica.md §1).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Session {
    /// The time of the entry that last used the session.
    pub last_ns: u64,
    /// Serials below this were answered and are forgotten.
    pub low: u64,
    /// The answers kept, in serial order, each encoded.
    pub answers: Vec<(u64, Vec<u8>)>,
}

/// A file: its length, how many extents hold it, and the handover it was made for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    pub length: u64,
    pub extents: u32,
    /// The File range's time when the file's rows committed.
    pub made_ns: u64,
    /// The latest time, in the clock of the Name range it is handed to, at which that range
    /// takes the file; past it the file is refused and released (docs/design/metadata.md §2).
    pub deadline_ns: u64,
    pub referrer: Referrer,
}

/// The Name-range write a file was made for: the object key whose version or part it is to
/// become. It routes the sweep's question about the file to the range that holds the key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Referrer {
    pub bucket: String,
    pub incarnation: u64,
    pub key: String,
}

/// Bytes of a file, up to the end its row's key names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Extent {
    pub length: u64,
    pub target: Target,
}

/// What holds an extent's bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    Block(u128),
    /// Another file: a completed upload's part.
    File(u128),
}

/// A block: its bytes, and how they are split into chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHeader {
    pub length: u64,
    /// Data chunks, `k`, and parity chunks, `m`: Reed-Solomon `(k, m)`, and replication to
    /// `m + 1` copies when `k` is one.
    pub data: u8,
    pub parity: u8,
    /// Bytes of each chunk.
    pub chunk_len: u64,
    /// CRC-32C of the block's bytes, checked after they are rebuilt from chunks.
    pub crc32c: u32,
}

/// A Name range's span and generation, as requests route by it (docs/design/metadata.md §3).
/// The span runs from routing key `lo` (`key::route`), empty for the first range, up to `hi`,
/// or to the end of the key space when `hi` is `None`. The generation rises with every split,
/// so a request routed by an older descriptor is told where the span went.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Descriptor {
    pub id: u64,
    pub lo: Vec<u8>,
    pub hi: Option<Vec<u8>>,
    pub generation: u64,
}

/// What a Name range knows of where its span went: its descriptor now and the child its last
/// split made, as it was made. A range answers a request routed by a descriptor it no longer
/// matches with its lineage, and never with data (docs/design/metadata.md §3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lineage {
    pub now: Descriptor,
    pub child: Option<Descriptor>,
}

/// The object key a released file was held under: the version, part or handover that took
/// it. It routes the removal of the file's mark to the range that holds the key, wherever
/// splits and merges have moved it by then (docs/design/metadata.md §2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Holder {
    pub bucket: String,
    pub key: String,
}

/// Where a block came from: the file it was made for, which alone may name it, and the Block
/// range's time it was written and by which that file's write must name it
/// (docs/design/metadata.md §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockOrigin {
    pub file: u128,
    pub made_ns: u64,
    pub deadline_ns: u64,
}

/// What the range a file or block was handed to answers the sweep of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Taken: something references it, or the range released it.
    Held,
    /// Never taken, and past its deadline: no write can take it now, and it is released.
    Released,
    /// Not taken yet, and its deadline has not passed at the range's time.
    Young,
}

/// Where one chunk of a block lives: a chunk store volume and the key it holds it under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkPlace {
    pub volume: u128,
    pub key: mantle_chunk::ChunkKey,
}

/// A reverse row: which chunk of its block the volume in its key holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reverse {
    pub index: u16,
}

impl FileHeader {
    pub fn encode(&self) -> Result<Vec<u8>, RecordError> {
        let mut w = start();
        w.u64(self.length);
        w.u32(self.extents);
        w.u64(self.made_ns);
        w.u64(self.deadline_ns);
        self.referrer.put(&mut w)?;
        Ok(finish(w))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "file")?;
        let header = (|| {
            Some(Self {
                length: r.u64()?,
                extents: r.u32()?,
                made_ns: r.u64()?,
                deadline_ns: r.u64()?,
                referrer: Referrer::take(&mut r)?,
            })
        })();
        decoded(header, &r, "file")
    }
}

impl Referrer {
    pub fn encode(&self) -> Result<Vec<u8>, RecordError> {
        let mut w = start();
        self.put(&mut w)?;
        Ok(finish(w))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "referrer")?;
        let referrer = Self::take(&mut r);
        decoded(referrer, &r, "referrer")
    }

    pub(crate) fn put(&self, w: &mut Writer) -> Result<(), RecordError> {
        put_str(w, &self.bucket)?;
        w.u64(self.incarnation);
        put_str(w, &self.key)
    }

    pub(crate) fn take(r: &mut Reader<'_>) -> Option<Self> {
        Some(Self {
            bucket: take_str(r)?,
            incarnation: r.u64()?,
            key: take_str(r)?,
        })
    }
}

impl Extent {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = start();
        w.u64(self.length);
        match self.target {
            Target::Block(id) => {
                w.u8(0);
                w.u128(id);
            }
            Target::File(id) => {
                w.u8(1);
                w.u128(id);
            }
        }
        finish(w)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "extent")?;
        let extent = (|| {
            let length = r.u64()?;
            let target = match r.u8()? {
                0 => Target::Block(r.u128()?),
                1 => Target::File(r.u128()?),
                _ => return None,
            };
            Some(Self { length, target })
        })();
        decoded(extent, &r, "extent")
    }
}

impl BlockHeader {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = start();
        w.u64(self.length);
        w.u8(self.data);
        w.u8(self.parity);
        w.u64(self.chunk_len);
        w.u32(self.crc32c);
        finish(w)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "block")?;
        let header = (|| {
            Some(Self {
                length: r.u64()?,
                data: r.u8()?,
                parity: r.u8()?,
                chunk_len: r.u64()?,
                crc32c: r.u32()?,
            })
        })();
        decoded(header, &r, "block")
    }
}

impl Descriptor {
    /// Whether the span holds routing key `route`.
    pub fn holds(&self, route: &[u8]) -> bool {
        self.lo.as_slice() <= route && self.hi.as_deref().is_none_or(|hi| route < hi)
    }

    /// Whether the span holds any routing key in `[from, past)`: a bucket's, from
    /// `key::bucket_routes`.
    pub fn meets(&self, from: &[u8], past: &[u8]) -> bool {
        self.lo.as_slice() < past && self.hi.as_deref().is_none_or(|hi| from < hi)
    }
}

pub(crate) fn put_descriptor(w: &mut Writer, d: &Descriptor) -> Result<(), RecordError> {
    w.u64(d.id);
    put_bytes(w, &d.lo)?;
    match &d.hi {
        None => w.u8(0),
        Some(hi) => {
            w.u8(1);
            put_bytes(w, hi)?;
        }
    }
    w.u64(d.generation);
    Ok(())
}

pub(crate) fn take_descriptor(r: &mut Reader<'_>) -> Option<Descriptor> {
    let id = r.u64()?;
    let lo = take_bytes(r)?;
    let hi = match r.u8()? {
        0 => None,
        1 => Some(take_bytes(r)?),
        _ => return None,
    };
    Some(Descriptor {
        id,
        lo,
        hi,
        generation: r.u64()?,
    })
}

pub(crate) fn put_lineage(w: &mut Writer, l: &Lineage) -> Result<(), RecordError> {
    put_descriptor(w, &l.now)?;
    match &l.child {
        None => w.u8(0),
        Some(child) => {
            w.u8(1);
            put_descriptor(w, child)?;
        }
    }
    Ok(())
}

pub(crate) fn take_lineage(r: &mut Reader<'_>) -> Option<Lineage> {
    let now = take_descriptor(r)?;
    let child = match r.u8()? {
        0 => None,
        1 => Some(take_descriptor(r)?),
        _ => return None,
    };
    Some(Lineage { now, child })
}

impl Lineage {
    pub fn encode(&self) -> Result<Vec<u8>, RecordError> {
        let mut w = start();
        put_lineage(&mut w, self)?;
        Ok(finish(w))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "lineage")?;
        let lineage = take_lineage(&mut r);
        decoded(lineage, &r, "lineage")
    }
}

impl Holder {
    pub fn encode(&self) -> Result<Vec<u8>, RecordError> {
        let mut w = start();
        put_str(&mut w, &self.bucket)?;
        put_str(&mut w, &self.key)?;
        Ok(finish(w))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "holder")?;
        let holder = (|| {
            Some(Self {
                bucket: take_str(&mut r)?,
                key: take_str(&mut r)?,
            })
        })();
        decoded(holder, &r, "holder")
    }
}

impl BlockOrigin {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = start();
        w.u128(self.file);
        w.u64(self.made_ns);
        w.u64(self.deadline_ns);
        finish(w)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "origin")?;
        let origin = (|| {
            Some(Self {
                file: r.u128()?,
                made_ns: r.u64()?,
                deadline_ns: r.u64()?,
            })
        })();
        decoded(origin, &r, "origin")
    }
}

impl ChunkPlace {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = start();
        w.u128(self.volume);
        self.key.encode(&mut w);
        finish(w)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "chunk")?;
        let place = (|| {
            Some(Self {
                volume: r.u128()?,
                key: mantle_chunk::ChunkKey::decode(&mut r)?,
            })
        })();
        decoded(place, &r, "chunk")
    }
}

impl Versioning {
    pub(crate) fn code(self) -> u8 {
        match self {
            Self::Unversioned => 0,
            Self::Enabled => 1,
            Self::Suspended => 2,
        }
    }

    pub(crate) fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Unversioned),
            1 => Some(Self::Enabled),
            2 => Some(Self::Suspended),
            _ => None,
        }
    }
}

impl BucketState {
    fn code(self) -> u8 {
        match self {
            Self::Creating => 0,
            Self::Active => 1,
            Self::Deleting => 2,
            Self::Deleted => 3,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Creating),
            1 => Some(Self::Active),
            2 => Some(Self::Deleting),
            3 => Some(Self::Deleted),
            _ => None,
        }
    }
}

impl Bucket {
    pub fn encode(&self) -> Result<Vec<u8>, RecordError> {
        let mut w = start();
        put_str(&mut w, &self.owner)?;
        w.u64(self.created_ns);
        put_str(&mut w, &self.location)?;
        w.u8(self.versioning.code());
        w.u8(self.state.code());
        w.u64(self.attempt);
        w.u64(self.progress_ns);
        match self.lock {
            None => w.u8(0),
            Some(Lock { default: None }) => w.u8(1),
            Some(Lock {
                default: Some(default),
            }) => {
                w.u8(2);
                put_default(&mut w, default);
            }
        }
        Ok(finish(w))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "bucket")?;
        decoded(
            (|| {
                Some(Self {
                    owner: take_str(&mut r)?,
                    created_ns: r.u64()?,
                    location: take_str(&mut r)?,
                    versioning: Versioning::from_code(r.u8()?)?,
                    state: BucketState::from_code(r.u8()?)?,
                    attempt: r.u64()?,
                    progress_ns: r.u64()?,
                    lock: match r.u8()? {
                        0 => None,
                        1 => Some(Lock::default()),
                        2 => Some(Lock {
                            default: Some(take_default(&mut r)?),
                        }),
                        _ => return None,
                    },
                })
            })(),
            &r,
            "bucket",
        )
    }
}

impl Gate {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = start();
        w.u64(self.incarnation);
        w.u64(self.attempt);
        w.u8(match self.state {
            GateState::Open => 0,
            GateState::Closed => 1,
            GateState::Condemned => 2,
        });
        finish(w)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "gate")?;
        let gate = (|| {
            let incarnation = r.u64()?;
            let attempt = r.u64()?;
            let state = match r.u8()? {
                0 => GateState::Open,
                1 => GateState::Closed,
                2 => GateState::Condemned,
                _ => return None,
            };
            Some(Self {
                incarnation,
                attempt,
                state,
            })
        })();
        decoded(gate, &r, "gate")
    }
}

impl Owner {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = start();
        w.u32(self.buckets);
        finish(w)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "owner")?;
        let owner = r.u32().map(|buckets| Self { buckets });
        decoded(owner, &r, "owner")
    }
}

impl Owned {
    pub fn encode(&self) -> Result<Vec<u8>, RecordError> {
        let mut w = start();
        w.u64(self.created_ns);
        put_str(&mut w, &self.location)?;
        Ok(finish(w))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "owned")?;
        decoded(
            (|| {
                Some(Self {
                    created_ns: r.u64()?,
                    location: take_str(&mut r)?,
                })
            })(),
            &r,
            "owned",
        )
    }
}

impl Session {
    pub fn encode(&self) -> Result<Vec<u8>, RecordError> {
        let mut w = start();
        w.u64(self.last_ns);
        w.u64(self.low);
        put_len(&mut w, self.answers.len())?;
        for (serial, answer) in &self.answers {
            w.u64(*serial);
            put_bytes(&mut w, answer)?;
        }
        Ok(finish(w))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "session")?;
        let session = (|| {
            let last_ns = r.u64()?;
            let low = r.u64()?;
            let count = usize::try_from(r.u32()?).ok()?;
            // An answer takes twelve bytes at least.
            if count > r.remaining() / 12 {
                return None;
            }
            let mut answers = Vec::with_capacity(count);
            for _ in 0..count {
                answers.push((r.u64()?, take_bytes(&mut r)?));
            }
            Some(Self {
                last_ns,
                low,
                answers,
            })
        })();
        decoded(session, &r, "session")
    }
}

impl Reverse {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = start();
        w.u16(self.index);
        finish(w)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "reverse")?;
        let reverse = r.u16().map(|index| Self { index });
        decoded(reverse, &r, "reverse")
    }
}

impl Version {
    pub fn encode(&self) -> Result<Vec<u8>, RecordError> {
        let mut w = start();
        w.u8(u8::from(self.marker)
            | (u8::from(self.null) << 1)
            | lock_flags(self.retention, self.legal_hold) << 2);
        w.u64(self.modified_ns);
        put_str(&mut w, &self.etag)?;
        w.u64(self.size);
        match &self.checksum {
            None => w.u8(0),
            Some(c) => {
                w.u8(1);
                w.u8(c.algorithm);
                w.u16(c.parts);
                put_bytes(&mut w, &c.value)?;
            }
        }
        put_file(&mut w, self.file);
        put_str(&mut w, &self.owner)?;
        put_pairs(&mut w, &self.headers)?;
        put_retention(&mut w, self.retention);
        Ok(finish(w))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "version")?;
        decoded(
            (|| {
                let flags = r.u8()?;
                let (has_retention, legal_hold) = take_lock_flags(flags >> 2)?;
                Some(Self {
                    marker: flags & 1 != 0,
                    null: flags & 2 != 0,
                    modified_ns: r.u64()?,
                    etag: take_str(&mut r)?,
                    size: r.u64()?,
                    checksum: match r.u8()? {
                        0 => None,
                        1 => Some(Checksum {
                            algorithm: r.u8()?,
                            parts: r.u16()?,
                            value: take_bytes(&mut r)?,
                        }),
                        _ => return None,
                    },
                    file: take_file(&mut r)?,
                    owner: take_str(&mut r)?,
                    headers: take_pairs(&mut r)?,
                    retention: take_retention(&mut r, has_retention)?,
                    legal_hold,
                })
            })(),
            &r,
            "version",
        )
    }
}

impl Upload {
    pub fn encode(&self) -> Result<Vec<u8>, RecordError> {
        let mut w = start();
        w.u64(self.initiated_ns);
        put_str(&mut w, &self.owner)?;
        put_pairs(&mut w, &self.headers)?;
        let lock = lock_flags(self.retention, self.legal_hold) << 2;
        match self.checksum {
            None => w.u8(lock),
            Some((algorithm, full)) => {
                w.u8(1 | (u8::from(full) << 1) | lock);
                w.u8(algorithm);
            }
        }
        put_retention(&mut w, self.retention);
        Ok(finish(w))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "upload")?;
        decoded(
            (|| {
                let initiated_ns = r.u64()?;
                let owner = take_str(&mut r)?;
                let headers = take_pairs(&mut r)?;
                let flags = r.u8()?;
                let (has_retention, legal_hold) = take_lock_flags(flags >> 2)?;
                let checksum = match flags & 3 {
                    0 => None,
                    flag @ (1 | 3) => Some((r.u8()?, flag == 3)),
                    _ => return None,
                };
                Some(Self {
                    initiated_ns,
                    owner,
                    headers,
                    checksum,
                    retention: take_retention(&mut r, has_retention)?,
                    legal_hold,
                })
            })(),
            &r,
            "upload",
        )
    }
}

impl Part {
    pub fn encode(&self) -> Result<Vec<u8>, RecordError> {
        let mut w = start();
        put_str(&mut w, &self.etag)?;
        w.u64(self.size);
        match &self.checksum {
            None => w.u8(0),
            Some(value) => {
                w.u8(1);
                put_bytes(&mut w, value)?;
            }
        }
        w.u128(self.file);
        w.u64(self.modified_ns);
        Ok(finish(w))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "part")?;
        decoded(
            (|| {
                Some(Self {
                    etag: take_str(&mut r)?,
                    size: r.u64()?,
                    checksum: match r.u8()? {
                        0 => None,
                        1 => Some(take_bytes(&mut r)?),
                        _ => return None,
                    },
                    file: r.u128()?,
                    modified_ns: r.u64()?,
                })
            })(),
            &r,
            "part",
        )
    }
}

/// A row that holds one number: a range's clock, a key's null-version pointer.
pub fn encode_number(n: u64) -> Vec<u8> {
    let mut w = start();
    w.u64(n);
    finish(w)
}

pub fn decode_number(bytes: &[u8], what: &'static str) -> Result<u64, RecordError> {
    let mut r = open(bytes, what)?;
    let n = r.u64();
    decoded(n, &r, what)
}

fn start() -> Writer {
    let mut w = Writer::default();
    w.u8(FORMAT);
    w
}

/// Appends the CRC-32C of everything written.
fn finish(mut w: Writer) -> Vec<u8> {
    let crc = mantle_crc::crc32c(w.as_slice());
    w.u32(crc);
    w.into_vec()
}

/// A reader over a value's fields once its checksum and format are verified.
fn open<'a>(bytes: &'a [u8], what: &'static str) -> Result<Reader<'a>, RecordError> {
    let body_len = bytes
        .len()
        .checked_sub(4)
        .ok_or(RecordError::Corrupt(what))?;
    let (body, crc) = bytes.split_at(body_len);
    let crc = u32::from_le_bytes(crc.try_into().map_err(|_| RecordError::Corrupt(what))?);
    if mantle_crc::crc32c(body) != crc {
        return Err(RecordError::Corrupt(what));
    }
    let mut r = Reader::new(body);
    if r.u8() != Some(FORMAT) {
        return Err(RecordError::Corrupt(what));
    }
    Ok(r)
}

/// A decoded value, if its fields used every byte.
fn decoded<T>(value: Option<T>, r: &Reader<'_>, what: &'static str) -> Result<T, RecordError> {
    value
        .filter(|_| r.remaining() == 0)
        .ok_or(RecordError::Corrupt(what))
}

/// A lock's flags: bit 0 a retention follows, bit 1 a legal hold was placed, bit 2 it is on.
fn lock_flags(retention: Option<Retention>, legal_hold: Option<bool>) -> u8 {
    u8::from(retention.is_some())
        | match legal_hold {
            None => 0,
            Some(false) => 2,
            Some(true) => 6,
        }
}

/// Whether a retention follows, and the legal hold, from flags `lock_flags` wrote: any other
/// value is corrupt, so every row has one encoding.
fn take_lock_flags(flags: u8) -> Option<(bool, Option<bool>)> {
    let legal_hold = match flags >> 1 {
        0 => None,
        1 => Some(false),
        3 => Some(true),
        _ => return None,
    };
    Some((flags & 1 != 0, legal_hold))
}

fn mode_code(mode: RetentionMode) -> u8 {
    match mode {
        RetentionMode::Governance => 0,
        RetentionMode::Compliance => 1,
    }
}

fn take_mode(r: &mut Reader<'_>) -> Option<RetentionMode> {
    match r.u8()? {
        0 => Some(RetentionMode::Governance),
        1 => Some(RetentionMode::Compliance),
        _ => None,
    }
}

pub(crate) fn put_retention(w: &mut Writer, retention: Option<Retention>) {
    if let Some(retention) = retention {
        w.u8(mode_code(retention.mode));
        w.u64(retention.until_ms.cast_unsigned());
    }
}

pub(crate) fn take_retention(r: &mut Reader<'_>, present: bool) -> Option<Option<Retention>> {
    if !present {
        return Some(None);
    }
    Some(Some(Retention {
        mode: take_mode(r)?,
        until_ms: r.u64()?.cast_signed(),
    }))
}

pub(crate) fn put_default(w: &mut Writer, default: DefaultRetention) {
    w.u8(mode_code(default.mode));
    match default.period {
        Period::Days(days) => {
            w.u8(0);
            w.u32(days);
        }
        Period::Years(years) => {
            w.u8(1);
            w.u32(years);
        }
    }
}

pub(crate) fn take_default(r: &mut Reader<'_>) -> Option<DefaultRetention> {
    let mode = take_mode(r)?;
    let period = match r.u8()? {
        0 => Period::Days(r.u32()?),
        1 => Period::Years(r.u32()?),
        _ => return None,
    };
    Some(DefaultRetention { mode, period })
}

pub(crate) fn put_len(w: &mut Writer, len: usize) -> Result<(), RecordError> {
    w.u32(u32::try_from(len).map_err(|_| RecordError::TooLarge(len))?);
    Ok(())
}

pub(crate) fn put_bytes(w: &mut Writer, b: &[u8]) -> Result<(), RecordError> {
    put_len(w, b.len())?;
    w.bytes(b);
    Ok(())
}

pub(crate) fn put_str(w: &mut Writer, s: &str) -> Result<(), RecordError> {
    put_bytes(w, s.as_bytes())
}

pub(crate) fn put_file(w: &mut Writer, file: Option<u128>) {
    match file {
        None => w.u8(0),
        Some(id) => {
            w.u8(1);
            w.u128(id);
        }
    }
}

pub(crate) fn put_pairs(w: &mut Writer, pairs: &[(String, String)]) -> Result<(), RecordError> {
    put_len(w, pairs.len())?;
    for (name, value) in pairs {
        put_str(w, name)?;
        put_str(w, value)?;
    }
    Ok(())
}

pub(crate) fn take_bytes(r: &mut Reader<'_>) -> Option<Vec<u8>> {
    let len = usize::try_from(r.u32()?).ok()?;
    Some(r.take(len)?.to_vec())
}

pub(crate) fn take_str(r: &mut Reader<'_>) -> Option<String> {
    String::from_utf8(take_bytes(r)?).ok()
}

pub(crate) fn take_file(r: &mut Reader<'_>) -> Option<Option<u128>> {
    match r.u8()? {
        0 => Some(None),
        1 => Some(Some(r.u128()?)),
        _ => None,
    }
}

pub(crate) fn take_pairs(r: &mut Reader<'_>) -> Option<Vec<(String, String)>> {
    let count = usize::try_from(r.u32()?).ok()?;
    // Each pair takes at least eight bytes, so a count the value cannot hold is corrupt
    // before anything is allocated for it.
    if count > r.remaining() / 8 {
        return None;
    }
    let mut pairs = Vec::with_capacity(count);
    for _ in 0..count {
        pairs.push((take_str(r)?, take_str(r)?));
    }
    Some(pairs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version() -> Version {
        Version {
            marker: false,
            null: true,
            modified_ns: 1_727_179_200_000_000_000,
            etag: "6805f2cfc46c0f04559748bb039d69ae".into(),
            size: 11,
            checksum: Some(Checksum {
                algorithm: 1,
                parts: 3,
                value: vec![1, 2, 3, 4],
            }),
            file: Some(0x1234_5678_9abc_def0_1234_5678_9abc_def0),
            owner: "owner".into(),
            headers: vec![
                ("content-type".into(), "text/plain".into()),
                ("x-amz-meta-a".into(), "é".into()),
            ],
            retention: None,
            legal_hold: None,
        }
    }

    #[test]
    fn rows_round_trip() {
        let v = version();
        assert_eq!(Version::decode(&v.encode().unwrap()), Ok(v));
        let marker = Version {
            marker: true,
            checksum: None,
            file: None,
            headers: vec![],
            ..version()
        };
        assert_eq!(Version::decode(&marker.encode().unwrap()), Ok(marker));
        let u = Upload {
            initiated_ns: 5,
            owner: "o".into(),
            headers: vec![("content-type".into(), "a/b".into())],
            checksum: Some((2, true)),
            retention: None,
            legal_hold: None,
        };
        assert_eq!(Upload::decode(&u.encode().unwrap()), Ok(u));
        let p = Part {
            etag: "e".into(),
            size: 5 << 20,
            checksum: None,
            file: 7,
            modified_ns: 9,
        };
        assert_eq!(Part::decode(&p.encode().unwrap()), Ok(p));
    }

    /// Every lock a version, an upload and a bucket can hold reads back as written, and every
    /// flag value no lock writes is corrupt, so each row has one encoding.
    #[test]
    fn locks_round_trip() {
        let retentions = [
            None,
            Some(Retention {
                mode: RetentionMode::Governance,
                until_ms: 1_893_456_000_000,
            }),
            Some(Retention {
                mode: RetentionMode::Compliance,
                until_ms: i64::MIN,
            }),
        ];
        for retention in retentions {
            for legal_hold in [None, Some(false), Some(true)] {
                let v = Version {
                    retention,
                    legal_hold,
                    ..version()
                };
                assert_eq!(Version::decode(&v.encode().unwrap()), Ok(v));
                for checksum in [None, Some((1, false)), Some((4, true))] {
                    let u = Upload {
                        initiated_ns: 1,
                        owner: "o".into(),
                        headers: Vec::new(),
                        checksum,
                        retention,
                        legal_hold,
                    };
                    assert_eq!(Upload::decode(&u.encode().unwrap()), Ok(u));
                }
            }
        }
        for lock in [
            None,
            Some(Lock::default()),
            Some(Lock {
                default: Some(DefaultRetention {
                    mode: RetentionMode::Compliance,
                    period: Period::Days(36_500),
                }),
            }),
            Some(Lock {
                default: Some(DefaultRetention {
                    mode: RetentionMode::Governance,
                    period: Period::Years(100),
                }),
            }),
        ] {
            let b = Bucket {
                owner: "o".into(),
                created_ns: 1,
                location: String::new(),
                versioning: Versioning::Enabled,
                state: BucketState::Active,
                attempt: 1,
                lock,
                progress_ns: 0,
            };
            assert_eq!(Bucket::decode(&b.encode().unwrap()), Ok(b));
        }
        // A legal hold on that was never placed, and flags past the last.
        for flags in [4u8 << 2, 1 << 5, 1 << 7] {
            let mut bytes = Version {
                checksum: None,
                ..version()
            }
            .encode()
            .unwrap();
            let body = bytes.len() - 4;
            bytes[1] |= flags;
            let crc = mantle_crc::crc32c(&bytes[..body]);
            bytes[body..].copy_from_slice(&crc.to_le_bytes());
            assert_eq!(
                Version::decode(&bytes),
                Err(RecordError::Corrupt("version"))
            );
        }
    }

    /// A default runs from the version's creation, a year as 365 days, and past the last
    /// instant lasts until it.
    #[test]
    fn a_default_retention_dates_from_creation() {
        let default = |period| DefaultRetention {
            mode: RetentionMode::Governance,
            period,
        };
        assert_eq!(default(Period::Days(1)).from(5).until_ms, 5 + DAY_MS);
        assert_eq!(default(Period::Years(2)).from(0).until_ms, 2 * 365 * DAY_MS);
        assert_eq!(
            default(Period::Years(u32::MAX)).from(i64::MAX - 1).until_ms,
            i64::MAX
        );
    }

    #[test]
    fn file_and_block_rows_round_trip() {
        let h = FileHeader {
            length: 1 << 40,
            extents: 10_000,
            made_ns: 7,
            deadline_ns: u64::MAX,
            referrer: Referrer {
                bucket: "b".into(),
                incarnation: 3,
                key: "a/\u{0}é".into(),
            },
        };
        assert_eq!(FileHeader::decode(&h.encode().unwrap()), Ok(h.clone()));
        assert_eq!(
            Referrer::decode(&h.referrer.encode().unwrap()),
            Ok(h.referrer)
        );
        for target in [Target::Block(3), Target::File(u128::MAX)] {
            let e = Extent {
                length: 5 << 30,
                target,
            };
            assert_eq!(Extent::decode(&e.encode()), Ok(e));
        }
        let b = BlockHeader {
            length: 72 << 20,
            data: 9,
            parity: 6,
            chunk_len: 8 << 20,
            crc32c: 0xDEAD_BEEF,
        };
        assert_eq!(BlockHeader::decode(&b.encode()), Ok(b));
        let c = ChunkPlace {
            volume: 42,
            key: mantle_chunk::ChunkKey {
                block: 7,
                epoch: 1,
                index: 14,
            },
        };
        assert_eq!(ChunkPlace::decode(&c.encode()), Ok(c));
        assert!(ChunkPlace::decode(&b.encode()).is_err());
        let r = Reverse { index: 14 };
        assert_eq!(Reverse::decode(&r.encode()), Ok(r));
    }

    #[test]
    fn bucket_rows_round_trip() {
        for (versioning, state) in [
            (Versioning::Unversioned, BucketState::Creating),
            (Versioning::Enabled, BucketState::Active),
            (Versioning::Suspended, BucketState::Deleting),
            (Versioning::Enabled, BucketState::Deleted),
        ] {
            let b = Bucket {
                owner: "o".into(),
                created_ns: 7,
                location: "eu-west-1".into(),
                versioning,
                state,
                attempt: 8,
                lock: None,
                progress_ns: 0,
            };
            assert_eq!(Bucket::decode(&b.encode().unwrap()), Ok(b));
        }
        for state in [GateState::Open, GateState::Closed, GateState::Condemned] {
            let g = Gate {
                incarnation: u64::MAX,
                attempt: 3,
                state,
            };
            assert_eq!(Gate::decode(&g.encode()), Ok(g));
        }
        let o = Owner { buckets: 10_000 };
        assert_eq!(Owner::decode(&o.encode()), Ok(o));
        let owned = Owned {
            created_ns: 9,
            location: String::new(),
        };
        assert_eq!(Owned::decode(&owned.encode().unwrap()), Ok(owned));
        assert_eq!(decode_number(&encode_number(u64::MAX), "n"), Ok(u64::MAX));
        assert!(decode_number(&o.encode(), "n").is_err());
        let mut bad = Gate {
            incarnation: 1,
            attempt: 1,
            state: GateState::Open,
        }
        .encode();
        // A state code past the last one, with its checksum made good again.
        let body = bad.len() - 4;
        bad[body - 1] = 3;
        let crc = mantle_crc::crc32c(&bad[..body]);
        bad[body..].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(Gate::decode(&bad), Err(RecordError::Corrupt("gate")));
    }

    #[test]
    fn lineages_round_trip_and_spans_hold_their_keys() {
        let parent = Descriptor {
            id: 1,
            lo: Vec::new(),
            hi: Some(vec![b'm', 0, 0]),
            generation: 2,
        };
        let child = Descriptor {
            id: u64::MAX,
            lo: vec![b'm', 0, 0],
            hi: None,
            generation: 2,
        };
        for l in [
            Lineage {
                now: parent.clone(),
                child: Some(child.clone()),
            },
            Lineage {
                now: child.clone(),
                child: None,
            },
        ] {
            assert_eq!(Lineage::decode(&l.encode().unwrap()), Ok(l));
        }
        assert!(parent.holds(b"") && parent.holds(b"l") && !parent.holds(&[b'm', 0, 0]));
        assert!(child.holds(&[b'm', 0, 0]) && child.holds(&[0xFE]));
        // A bucket's routes meet the spans that hold any of them.
        assert!(parent.meets(b"a", b"b") && !child.meets(b"a", b"b"));
        assert!(parent.meets(&[b'm', 0], &[b'm', 0xFF]) && child.meets(&[b'm', 0], &[b'm', 0xFF]));
    }

    /// Every flipped bit and every truncation is refused, never misread.
    #[test]
    fn damage_is_corrupt() {
        let bytes = version().encode().unwrap();
        for i in 0..bytes.len() * 8 {
            let mut damaged = bytes.clone();
            damaged[i / 8] ^= 1 << (i % 8);
            assert_eq!(
                Version::decode(&damaged),
                Err(RecordError::Corrupt("version"))
            );
        }
        for len in 0..bytes.len() {
            assert!(Version::decode(&bytes[..len]).is_err());
        }
        assert!(Part::decode(&bytes).is_err());
    }
}
