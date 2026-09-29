//! The Name layer's state machine (docs/design/metadata.md §2): S3's writes to one object key
//! as commands applied at a log index, and its reads.
//!
//! Applying a command reads the key's rows, decides, and writes one batch with the entry's
//! index, so every replica that applies the same log holds the same rows. A write's time is
//! the range's clock's (clock.rs), so versions of a key never share an order. A write to a
//! bucket's objects passes only through the bucket's open gate for the incarnation it names
//! (docs/design/metadata.md §2).

use crate::clock;
use crate::engine::{Engine, Write};
use crate::error::MetaError;
use crate::key::{self, NULL_VERSION, NameRow};
use crate::record::{self, Checksum, Gate, GateState, Part, Upload, Version};

pub use crate::record::Versioning;

/// "Part size: 5 MiB to 5 GiB. There is no minimum size limit on the last part" (05 §4.1).
pub const MIN_PART: u64 = 5 << 20;

/// Entity tags a precondition names, bare of quotes, or `*`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Match {
    Any,
    Tags(Vec<String>),
}

impl Match {
    fn names(&self, etag: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Tags(tags) => tags.iter().any(|t| t == etag),
        }
    }
}

/// A write's preconditions on the key's current version, judged as it commits (05 §2.2).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Preconditions {
    pub if_match: Option<Match>,
    pub if_none_match: Option<Match>,
}

/// A version a request names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Named {
    Null,
    Order(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Put(Put),
    Delete(Delete),
    CreateUpload(CreateUpload),
    PutPart(PutPart),
    Complete(Complete),
    Abort(Abort),
    Gate(GateChange),
    Collect(Collect),
}

impl Command {
    /// The bucket and incarnation a write to objects names; `None` for the gates' commands.
    fn write_to(&self) -> Option<(&str, u64)> {
        match self {
            Self::Put(c) => Some((&c.bucket, c.incarnation)),
            Self::Delete(c) => Some((&c.bucket, c.incarnation)),
            Self::CreateUpload(c) => Some((&c.bucket, c.incarnation)),
            Self::PutPart(c) => Some((&c.bucket, c.incarnation)),
            Self::Complete(c) => Some((&c.bucket, c.incarnation)),
            Self::Abort(c) => Some((&c.bucket, c.incarnation)),
            Self::Gate(_) | Self::Collect(_) => None,
        }
    }
}

/// A new version of `key`: PutObject, CopyObject's destination, or a completed upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Put {
    pub bucket: String,
    /// The bucket's incarnation the write was made under.
    pub incarnation: u64,
    pub key: String,
    pub versioning: Versioning,
    pub preconditions: Preconditions,
    pub at_ns: u64,
    /// Orders the version at this time instead of the commit's: a completed upload's
    /// initiation, since the upload that "started most recently" is current (05 §4.4). S3
    /// does not document a completed upload's `Last-Modified`; the version's is the time
    /// that orders it.
    pub ordered_ns: Option<u64>,
    pub version: Version,
}

/// CreateMultipartUpload (05 §4.2). The upload's ID is its initiation time in the version-ID
/// alphabet: a key's uploads then sort in initiation order, as ListMultipartUploads lists
/// them and as paging by `upload-id-marker` needs (05 §4.7), and the version that completes
/// it sits at that time's order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateUpload {
    pub bucket: String,
    /// The bucket's incarnation the write was made under.
    pub incarnation: u64,
    pub key: String,
    pub at_ns: u64,
    /// The upload's owner, headers and checksum; its initiation time is the commit's.
    pub upload: Upload,
}

/// UploadPart and UploadPartCopy: the part's row, replacing any of the same number
/// (05 §4.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutPart {
    pub bucket: String,
    /// The bucket's incarnation the write was made under.
    pub incarnation: u64,
    pub key: String,
    pub upload: String,
    pub number: u16,
    pub part: Part,
}

/// CompleteMultipartUpload (05 §4.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Complete {
    pub bucket: String,
    /// The bucket's incarnation the write was made under.
    pub incarnation: u64,
    pub key: String,
    pub upload: String,
    pub versioning: Versioning,
    pub preconditions: Preconditions,
    pub at_ns: u64,
    /// The parts the request lists, in its order, each with the ETag it sent and the file the
    /// gateway read for it.
    pub parts: Vec<Listed>,
    /// The object as the gateway combined it from those parts: its ETag, size and checksum,
    /// and the file of their extents.
    pub etag: String,
    pub size: u64,
    pub checksum: Option<Checksum>,
    pub file: Option<u128>,
}

/// A part a CompleteMultipartUpload lists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    pub number: u16,
    /// Bare of quotes.
    pub etag: String,
    pub file: u128,
}

/// AbortMultipartUpload (05 §4.8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Abort {
    pub bucket: String,
    /// The bucket's incarnation the write was made under.
    pub incarnation: u64,
    pub key: String,
    pub upload: String,
}

/// Moves a bucket's gate a step in its creation or deletion (docs/design/metadata.md §2), if
/// the gate is where the coordinator read it and no later attempt has moved it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateChange {
    pub bucket: String,
    pub incarnation: u64,
    /// The create or delete attempt making the change, as the Bucket range issued it.
    pub attempt: u64,
    /// The gate's state now; `None` for no gate.
    pub from: Option<GateState>,
    /// Its next state; `None` removes the gate once the range holds no row of the bucket.
    pub to: Option<GateState>,
}

/// Removes rows of a condemned bucket, its uploads and their parts, as the collector does
/// after a delete (docs/design/metadata.md §2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Collect {
    pub bucket: String,
    pub incarnation: u64,
    /// Rows this entry may remove, which bounds its size.
    pub budget: u32,
}

/// DeleteObject (05 §7.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delete {
    pub bucket: String,
    /// The bucket's incarnation the write was made under.
    pub incarnation: u64,
    pub key: String,
    pub versioning: Versioning,
    pub named: Option<Named>,
    pub if_match: Option<Match>,
    pub at_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// A version was written; its ID.
    Put { version: String },
    /// A delete was done: whether the version it made or removed is a delete marker, and
    /// that version's ID. An unversioned delete names none.
    Deleted {
        marker: bool,
        version: Option<String>,
    },
    /// `412 PreconditionFailed`.
    PreconditionFailed,
    /// `404 NoSuchKey`: `If-Match` with no current version (05 §2.2).
    NoSuchKey,
    /// An upload was created; its ID.
    Created { upload: String },
    /// A part's row was written.
    PartWritten,
    /// An upload and its parts were removed.
    Aborted,
    /// `404 NoSuchUpload`.
    NoSuchUpload,
    /// `400 InvalidPart`: a listed part was never uploaded, or its ETag differs.
    InvalidPart,
    /// `400 InvalidPartOrder`: parts not listed in ascending order of number.
    InvalidPartOrder,
    /// `400 EntityTooSmall`: a part other than the last is under `MIN_PART`.
    EntityTooSmall,
    /// A listed part now holds another file than the gateway read: it read the parts again
    /// and retries, since the object it built would name the old part's bytes.
    Stale,
    /// `404 NoSuchBucket`: the range holds no open gate for the write's incarnation.
    NoSuchBucket,
    /// A gate moved, or a retry found it moved.
    GateMoved,
    /// The gate is not where the step expects: the coordinator reads the bucket again.
    Conflict,
    /// A gate move that creating and deleting a bucket never make.
    Invalid,
    /// The range holds rows of the bucket that the step needs gone.
    NotEmpty,
    /// Rows of a condemned bucket were removed; `done` once none is left.
    Collected { done: bool },
}

/// Applies `command` as log entry `index`. A refused command still advances the index.
pub fn apply<E: Engine>(
    engine: &mut E,
    index: u64,
    command: &Command,
) -> Result<Outcome, MetaError> {
    let admitted = match command.write_to() {
        Some((bucket, incarnation)) => admits(engine, bucket, incarnation)?,
        None => true,
    };
    let (outcome, writes) = match command {
        _ if !admitted => (Outcome::NoSuchBucket, Vec::new()),
        Command::Put(p) => put(engine, p)?,
        Command::Delete(d) => delete(engine, d)?,
        Command::CreateUpload(c) => create_upload(engine, c)?,
        Command::PutPart(p) => put_part(engine, p)?,
        Command::Complete(c) => complete(engine, c)?,
        Command::Abort(a) => abort(engine, a)?,
        Command::Gate(g) => move_gate(engine, g)?,
        Command::Collect(c) => collect(engine, c)?,
    };
    engine.apply(index, &writes)?;
    Ok(outcome)
}

/// The range's gate floor: no attempt older than it may place a gate where there is none. A
/// gate's removal raises it to the removing attempt, so a coordinator left behind by a later
/// attempt cannot open a gate for a bucket that is gone, and the range keeps no row for it.
const FLOOR: &[u8] = &[key::LOCAL, b'f'];

/// Whether the range admits a write to `bucket` made under `incarnation`.
fn admits<E: Engine>(engine: &E, bucket: &str, incarnation: u64) -> Result<bool, MetaError> {
    Ok(gate(engine, bucket)?
        .is_some_and(|g| g.incarnation == incarnation && g.state == GateState::Open))
}

fn move_gate<E: Engine>(engine: &E, g: &GateChange) -> Result<(Outcome, Vec<Write>), MetaError> {
    use GateState::{Closed, Condemned, Open};
    // The steps creating and deleting a bucket take. A delete that takes over from an
    // attempt left behind closes each gate again, and one that abandons an unfinished create
    // places closed gates where none was opened.
    let step = matches!(
        (g.from, g.to),
        (None, Some(Open | Closed))
            | (Some(Open), Some(Closed))
            | (Some(Closed), Some(Open | Closed | Condemned))
            | (Some(Condemned), None)
    );
    if !step {
        return Ok((Outcome::Invalid, Vec::new()));
    }
    let now = gate(engine, &g.bucket)?;
    let moved = g.to.map(|state| Gate {
        incarnation: g.incarnation,
        attempt: g.attempt,
        state,
    });
    if now == moved {
        // A retry finds the gate where it moved it.
        return Ok((Outcome::GateMoved, Vec::new()));
    }
    let row = key::gate(&g.bucket);
    match now {
        Some(gate) => {
            if gate.attempt > g.attempt
                || gate.incarnation != g.incarnation
                || Some(gate.state) != g.from
            {
                return Ok((Outcome::Conflict, Vec::new()));
            }
        }
        None => {
            if g.from.is_some() || g.attempt < floor(engine)? {
                return Ok((Outcome::Conflict, Vec::new()));
            }
        }
    }
    let Some(next) = moved else {
        let (from, to) = key::bucket_span(&g.bucket);
        if engine.next(&from, &to)?.is_some() {
            return Ok((Outcome::NotEmpty, Vec::new()));
        }
        let raised = floor(engine)?.max(g.attempt);
        return Ok((
            Outcome::GateMoved,
            vec![
                Write::Delete(row),
                Write::Put(FLOOR.to_vec(), record::encode_number(raised)),
            ],
        ));
    };
    Ok((Outcome::GateMoved, vec![Write::Put(row, next.encode())]))
}

fn floor<E: Engine>(engine: &E) -> Result<u64, MetaError> {
    match engine.get(FLOOR)? {
        None => Ok(0),
        Some(bytes) => Ok(record::decode_number(&bytes, "floor")?),
    }
}

fn collect<E: Engine>(engine: &E, c: &Collect) -> Result<(Outcome, Vec<Write>), MetaError> {
    let condemned = gate(engine, &c.bucket)?
        .is_some_and(|g| g.incarnation == c.incarnation && g.state == GateState::Condemned);
    if !condemned {
        return Ok((Outcome::Conflict, Vec::new()));
    }
    let (mut from, to) = key::bucket_span(&c.bucket);
    let mut writes = Vec::new();
    for _ in 0..c.budget {
        let Some((k, _)) = engine.next(&from, &to)? else {
            return Ok((Outcome::Collected { done: true }, writes));
        };
        match key::decode_name(&k) {
            Some((_, _, NameRow::Upload(_) | NameRow::Part(..))) => {}
            // A version under a condemned gate: the delete never read this range (§2), and
            // the collector removes no object.
            Some(_) => return Ok((Outcome::NotEmpty, Vec::new())),
            None => return Err(MetaError::Corrupt),
        }
        from = after(&k);
        writes.push(Write::Delete(k));
    }
    let done = engine.next(&from, &to)?.is_none();
    Ok((Outcome::Collected { done }, writes))
}

fn put<E: Engine>(engine: &E, p: &Put) -> Result<(Outcome, Vec<Write>), MetaError> {
    let (bucket, key, versioning) = (p.bucket.as_str(), p.key.as_str(), p.versioning);
    let (preconditions, at_ns, ordered_ns, version) =
        (&p.preconditions, p.at_ns, p.ordered_ns, &p.version);
    // A delete marker is no current version to a write (05 §2.2).
    let current = current(engine, bucket, key)?.filter(|(_, v)| !v.marker);
    if let Some(none_match) = &preconditions.if_none_match
        && current
            .as_ref()
            .is_some_and(|(_, v)| none_match.names(&v.etag))
    {
        return Ok((Outcome::PreconditionFailed, Vec::new()));
    }
    if let Some(if_match) = &preconditions.if_match {
        match &current {
            None => return Ok((Outcome::NoSuchKey, Vec::new())),
            Some((_, v)) if !if_match.names(&v.etag) => {
                return Ok((Outcome::PreconditionFailed, Vec::new()));
            }
            Some(_) => {}
        }
    }
    let (time, clock) = clock::tick(engine, at_ns)?;
    let mut writes = vec![clock];
    let when = ordered_ns.unwrap_or(time);
    let order = !when;
    let null = versioning != Versioning::Enabled;
    if null {
        writes.extend(remove_null(engine, bucket, key)?.1);
        writes.push(Write::Put(
            key::name(bucket, key, &NameRow::Null),
            record::encode_number(order),
        ));
    }
    let written = Version {
        marker: false,
        null,
        modified_ns: when,
        ..version.clone()
    };
    writes.push(Write::Put(
        key::name(bucket, key, &NameRow::Version(order)),
        written.encode()?,
    ));
    Ok((
        Outcome::Put {
            version: id(null, order),
        },
        writes,
    ))
}

fn delete<E: Engine>(engine: &E, d: &Delete) -> Result<(Outcome, Vec<Write>), MetaError> {
    let (bucket, key, versioning) = (d.bucket.as_str(), d.key.as_str(), d.versioning);
    let (named, if_match, at_ns) = (d.named, d.if_match.as_ref(), d.at_ns);
    if let Some(if_match) = if_match {
        // A conditional delete judges the version it removes, or the current one (05 §2.3).
        let target = match named {
            None => current(engine, bucket, key)?,
            Some(named) => version(engine, bucket, key, named)?,
        };
        match target {
            None => return Ok((Outcome::NoSuchKey, Vec::new())),
            Some((_, v)) if v.marker || !if_match.names(&v.etag) => {
                return Ok((Outcome::PreconditionFailed, Vec::new()));
            }
            Some(_) => {}
        }
    }
    match named {
        Some(Named::Null) => {
            let (removed, writes) = remove_null(engine, bucket, key)?;
            Ok((
                Outcome::Deleted {
                    marker: removed.is_some_and(|v| v.marker),
                    version: Some(NULL_VERSION.to_owned()),
                },
                writes,
            ))
        }
        Some(Named::Order(order)) => {
            let mut writes = Vec::new();
            let removed = match version(engine, bucket, key, Named::Order(order))? {
                None => None,
                Some((_, v)) => {
                    writes.push(Write::Delete(key::name(
                        bucket,
                        key,
                        &NameRow::Version(order),
                    )));
                    if v.null {
                        writes.push(Write::Delete(key::name(bucket, key, &NameRow::Null)));
                    }
                    Some(v)
                }
            };
            Ok((
                Outcome::Deleted {
                    marker: removed.as_ref().is_some_and(|v| v.marker),
                    version: Some(id(removed.is_some_and(|v| v.null), order)),
                },
                writes,
            ))
        }
        None => match versioning {
            Versioning::Unversioned => {
                let (_, writes) = remove_null(engine, bucket, key)?;
                Ok((
                    Outcome::Deleted {
                        marker: false,
                        version: None,
                    },
                    writes,
                ))
            }
            Versioning::Enabled | Versioning::Suspended => {
                // Enabled stacks a new marker; suspended replaces the null version with a null
                // marker (05 §7.3).
                let null = versioning == Versioning::Suspended;
                let (time, clock) = clock::tick(engine, at_ns)?;
                let mut writes = vec![clock];
                let order = !time;
                if null {
                    writes.extend(remove_null(engine, bucket, key)?.1);
                    writes.push(Write::Put(
                        key::name(bucket, key, &NameRow::Null),
                        record::encode_number(order),
                    ));
                }
                let marker = Version {
                    marker: true,
                    null,
                    modified_ns: time,
                    etag: String::new(),
                    size: 0,
                    checksum: None,
                    file: None,
                    owner: String::new(),
                    headers: Vec::new(),
                };
                writes.push(Write::Put(
                    key::name(bucket, key, &NameRow::Version(order)),
                    marker.encode()?,
                ));
                Ok((
                    Outcome::Deleted {
                        marker: true,
                        version: Some(id(null, order)),
                    },
                    writes,
                ))
            }
        },
    }
}

fn create_upload<E: Engine>(
    engine: &E,
    c: &CreateUpload,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    let (time, clock) = clock::tick(engine, c.at_ns)?;
    let mut writes = vec![clock];
    let upload = key::version_id(time);
    let row = Upload {
        initiated_ns: time,
        ..c.upload.clone()
    };
    writes.push(Write::Put(
        key::name(
            &c.bucket,
            &c.key,
            &NameRow::Upload(upload.clone().into_bytes()),
        ),
        row.encode()?,
    ));
    Ok((Outcome::Created { upload }, writes))
}

fn put_part<E: Engine>(engine: &E, p: &PutPart) -> Result<(Outcome, Vec<Write>), MetaError> {
    if upload(engine, &p.bucket, &p.key, &p.upload)?.is_none() {
        return Ok((Outcome::NoSuchUpload, Vec::new()));
    }
    let row = key::name(
        &p.bucket,
        &p.key,
        &NameRow::Part(p.upload.clone().into_bytes(), p.number),
    );
    Ok((
        Outcome::PartWritten,
        vec![Write::Put(row, p.part.encode()?)],
    ))
}

fn complete<E: Engine>(engine: &E, c: &Complete) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some(upload_row) = upload(engine, &c.bucket, &c.key, &c.upload)? else {
        // A retry of a complete that committed finds the version it made (05 §4.4).
        let made = match key::parse_version_id(&c.upload) {
            Some(initiated) => version(engine, &c.bucket, &c.key, Named::Order(!initiated))?,
            None => None,
        };
        return Ok(match made {
            Some((order, v)) if !v.marker && v.etag == c.etag => (
                Outcome::Put {
                    version: id(v.null, order),
                },
                Vec::new(),
            ),
            _ => (Outcome::NoSuchUpload, Vec::new()),
        });
    };
    if c.parts.is_empty()
        || !c
            .parts
            .windows(2)
            .all(|w| matches!(w, [a, b] if a.number < b.number))
    {
        return Ok((Outcome::InvalidPartOrder, Vec::new()));
    }
    let last = c.parts.len().saturating_sub(1);
    for (i, listed) in c.parts.iter().enumerate() {
        let row = key::name(
            &c.bucket,
            &c.key,
            &NameRow::Part(c.upload.clone().into_bytes(), listed.number),
        );
        let Some(part) = engine.get(&row)?.map(|b| Part::decode(&b)).transpose()? else {
            return Ok((Outcome::InvalidPart, Vec::new()));
        };
        if part.etag != listed.etag {
            return Ok((Outcome::InvalidPart, Vec::new()));
        }
        if part.file != listed.file {
            return Ok((Outcome::Stale, Vec::new()));
        }
        if i < last && part.size < MIN_PART {
            return Ok((Outcome::EntityTooSmall, Vec::new()));
        }
    }
    let (outcome, mut writes) = put(
        engine,
        &Put {
            bucket: c.bucket.clone(),
            incarnation: c.incarnation,
            key: c.key.clone(),
            versioning: c.versioning,
            preconditions: c.preconditions.clone(),
            at_ns: c.at_ns,
            ordered_ns: Some(upload_row.initiated_ns),
            version: Version {
                marker: false,
                null: false,
                modified_ns: 0,
                etag: c.etag.clone(),
                size: c.size,
                checksum: c.checksum.clone(),
                file: c.file,
                owner: upload_row.owner,
                headers: upload_row.headers,
            },
        },
    )?;
    if matches!(outcome, Outcome::Put { .. }) {
        // Parts not listed are discarded with the upload (05 §4.4).
        writes.extend(remove_upload(engine, &c.bucket, &c.key, &c.upload)?);
    }
    Ok((outcome, writes))
}

fn abort<E: Engine>(engine: &E, a: &Abort) -> Result<(Outcome, Vec<Write>), MetaError> {
    if upload(engine, &a.bucket, &a.key, &a.upload)?.is_none() {
        return Ok((Outcome::NoSuchUpload, Vec::new()));
    }
    Ok((
        Outcome::Aborted,
        remove_upload(engine, &a.bucket, &a.key, &a.upload)?,
    ))
}

/// Writes that remove an upload's row and every one of its parts: at most 10,000 (05 §4.1).
fn remove_upload<E: Engine>(
    engine: &E,
    bucket: &str,
    key: &str,
    upload: &str,
) -> Result<Vec<Write>, MetaError> {
    let id = upload.as_bytes().to_vec();
    let mut writes = vec![Write::Delete(key::name(
        bucket,
        key,
        &NameRow::Upload(id.clone()),
    ))];
    let mut from = key::name(bucket, key, &NameRow::Part(id.clone(), 0));
    let mut to = key::name(bucket, key, &NameRow::Part(id, u16::MAX));
    to.push(0);
    for _ in 0..=u16::MAX {
        let Some((k, _)) = engine.next(&from, &to)? else {
            break;
        };
        from = after(&k);
        writes.push(Write::Delete(k));
    }
    Ok(writes)
}

/// An upload in progress.
pub fn upload<E: Engine>(
    engine: &E,
    bucket: &str,
    key: &str,
    upload: &str,
) -> Result<Option<Upload>, MetaError> {
    let row = key::name(bucket, key, &NameRow::Upload(upload.as_bytes().to_vec()));
    Ok(engine.get(&row)?.map(|b| Upload::decode(&b)).transpose()?)
}

/// An upload's parts numbered after `after`, in order, at most `max` of them: ListParts'
/// page (05 §4.6).
pub fn parts<E: Engine>(
    engine: &E,
    bucket: &str,
    key: &str,
    upload: &str,
    after_part: u16,
    max: usize,
) -> Result<Vec<(u16, Part)>, MetaError> {
    let id = upload.as_bytes().to_vec();
    let Some(first) = after_part.checked_add(1) else {
        return Ok(Vec::new());
    };
    let mut from = key::name(bucket, key, &NameRow::Part(id.clone(), first));
    let mut to = key::name(bucket, key, &NameRow::Part(id, u16::MAX));
    to.push(0);
    let mut out = Vec::new();
    while out.len() < max {
        let Some((k, v)) = engine.next(&from, &to)? else {
            break;
        };
        let Some((_, _, NameRow::Part(_, number))) = key::decode_name(&k) else {
            return Err(MetaError::Corrupt);
        };
        out.push((number, Part::decode(&v)?));
        from = after(&k);
    }
    Ok(out)
}

/// The key's current version: its newest, a delete marker or not.
pub fn current<E: Engine>(
    engine: &E,
    bucket: &str,
    key: &str,
) -> Result<Option<(u64, Version)>, MetaError> {
    let from = key::name(bucket, key, &NameRow::Version(0));
    let to = key::name(bucket, key, &NameRow::Upload(Vec::new()));
    match engine.next(&from, &to)? {
        None => Ok(None),
        Some((k, v)) => match key::decode_name(&k) {
            Some((_, _, NameRow::Version(order))) => Ok(Some((order, Version::decode(&v)?))),
            _ => Err(MetaError::Corrupt),
        },
    }
}

/// The version a request names.
pub fn version<E: Engine>(
    engine: &E,
    bucket: &str,
    key: &str,
    named: Named,
) -> Result<Option<(u64, Version)>, MetaError> {
    let order = match named {
        Named::Order(order) => order,
        Named::Null => match null_order(engine, bucket, key)? {
            Some(order) => order,
            None => return Ok(None),
        },
    };
    match engine.get(&key::name(bucket, key, &NameRow::Version(order)))? {
        None => Ok(None),
        Some(v) => Ok(Some((order, Version::decode(&v)?))),
    }
}

/// The order of the key's null version.
fn null_order<E: Engine>(engine: &E, bucket: &str, key: &str) -> Result<Option<u64>, MetaError> {
    match engine.get(&key::name(bucket, key, &NameRow::Null))? {
        None => Ok(None),
        Some(bytes) => Ok(Some(record::decode_number(&bytes, "null")?)),
    }
}

/// Writes that remove the key's null version and its pointer, and the version removed.
fn remove_null<E: Engine>(
    engine: &E,
    bucket: &str,
    key: &str,
) -> Result<(Option<Version>, Vec<Write>), MetaError> {
    match version(engine, bucket, key, Named::Null)? {
        None => Ok((None, Vec::new())),
        Some((order, v)) => Ok((
            Some(v),
            vec![
                Write::Delete(key::name(bucket, key, &NameRow::Version(order))),
                Write::Delete(key::name(bucket, key, &NameRow::Null)),
            ],
        )),
    }
}

/// A version's ID: "null" for the null version, else its order's.
fn id(null: bool, order: u64) -> String {
    if null {
        NULL_VERSION.to_owned()
    } else {
        key::version_id(order)
    }
}

/// What a listing step found (docs/design/s3-protocol.md §3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scan {
    /// The first key at or after the position whose current version is an object.
    Found {
        key: String,
        order: u64,
        version: Version,
    },
    /// No such key is left in the bucket.
    End,
    /// The budget of rows ran out first: the next step starts at this object-key position.
    Paused(Vec<u8>),
}

/// The first key at or after `from`, in object-key byte order, whose current version is an
/// object rather than a delete marker, reading at most `budget` rows: a bucket whose keys are
/// mostly delete markers pauses the scan rather than running it unbounded.
pub fn next_current<E: Engine>(
    engine: &E,
    bucket: &str,
    from: &[u8],
    budget: usize,
) -> Result<Scan, MetaError> {
    let mut position = key::position(bucket, from);
    // Where the next step starts, in object-key space.
    let mut resume = from.to_vec();
    let (_, end) = key::bucket_span(bucket);
    for _ in 0..budget {
        let Some((k, v)) = engine.next(&position, &end)? else {
            return Ok(Scan::End);
        };
        let Some((_, object, row)) = key::decode_name(&k) else {
            return Err(MetaError::Corrupt);
        };
        match row {
            // The null pointer sorts before the key's versions.
            NameRow::Null => {
                position = after(&k);
                resume = object.into_bytes();
            }
            NameRow::Version(order) => {
                let version = Version::decode(&v)?;
                if !version.marker {
                    return Ok(Scan::Found {
                        key: object,
                        order,
                        version,
                    });
                }
                position = beyond_rows(bucket, &object);
                resume = after(object.as_bytes());
            }
            // Uploads with no version before them: the key has no current version.
            NameRow::Upload(_) | NameRow::Part(..) => {
                position = beyond_rows(bucket, &object);
                resume = after(object.as_bytes());
            }
        }
    }
    Ok(Scan::Paused(resume))
}

/// The range's gate for `bucket`.
pub fn gate<E: Engine>(engine: &E, bucket: &str) -> Result<Option<Gate>, MetaError> {
    Ok(engine
        .get(&key::gate(bucket))?
        .map(|b| Gate::decode(&b))
        .transpose()?)
}

/// What a read for a bucket's versions found (docs/design/metadata.md §2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Probe {
    /// A version or a delete marker.
    Found,
    /// Neither, anywhere in the range.
    Clear,
    /// The budget of rows ran out first: the next read starts at this key.
    Paused(Vec<u8>),
}

/// Whether the range holds a version or delete marker of `bucket`, reading at most `budget`
/// rows from `from`, a paused read's key, or from the bucket's first row. A key's versions
/// sort before its uploads, so one row answers for each key.
pub fn probe<E: Engine>(
    engine: &E,
    bucket: &str,
    from: Option<&[u8]>,
    budget: usize,
) -> Result<Probe, MetaError> {
    let (first, to) = key::bucket_span(bucket);
    let mut position = match from {
        Some(k) if k > first.as_slice() => k.to_vec(),
        _ => first,
    };
    for _ in 0..budget {
        let Some((k, _)) = engine.next(&position, &to)? else {
            return Ok(Probe::Clear);
        };
        match key::decode_name(&k) {
            Some((_, _, NameRow::Null | NameRow::Version(_))) => return Ok(Probe::Found),
            Some((_, object, NameRow::Upload(_) | NameRow::Part(..))) => {
                position = beyond_rows(bucket, &object);
            }
            None => return Err(MetaError::Corrupt),
        }
    }
    Ok(Probe::Paused(position))
}

/// The smallest key after `k`.
fn after(k: &[u8]) -> Vec<u8> {
    let mut next = k.to_vec();
    next.push(0);
    next
}

/// A position after every row of `object`: its prefix and a byte past every row kind.
fn beyond_rows(bucket: &str, object: &str) -> Vec<u8> {
    let mut next = key::object(bucket, object);
    next.push(u8::MAX);
    next
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Model;

    struct Range {
        engine: Model,
        index: u64,
        clock: u64,
    }

    impl Range {
        /// A range holding bucket "b", incarnation 1, open.
        fn new() -> Self {
            let mut r = Self {
                engine: Model::default(),
                index: 0,
                clock: 1_000,
            };
            assert_eq!(r.gate(None, Some(GateState::Open)), Outcome::GateMoved);
            r
        }

        fn gate(&mut self, from: Option<GateState>, to: Option<GateState>) -> Outcome {
            self.gate_at(1, from, to)
        }

        fn gate_at(
            &mut self,
            attempt: u64,
            from: Option<GateState>,
            to: Option<GateState>,
        ) -> Outcome {
            self.run(Command::Gate(GateChange {
                bucket: "b".into(),
                incarnation: 1,
                attempt,
                from,
                to,
            }))
        }

        fn run(&mut self, command: Command) -> Outcome {
            self.index += 1;
            apply(&mut self.engine, self.index, &command).unwrap()
        }

        fn put(&mut self, key: &str, etag: &str, versioning: Versioning) -> Outcome {
            self.put_if(key, etag, versioning, Preconditions::default())
        }

        fn put_if(
            &mut self,
            key: &str,
            etag: &str,
            versioning: Versioning,
            p: Preconditions,
        ) -> Outcome {
            self.clock += 10;
            self.run(Command::Put(Put {
                bucket: "b".into(),
                incarnation: 1,
                key: key.into(),
                versioning,
                preconditions: p,
                at_ns: self.clock,
                ordered_ns: None,
                version: object(etag),
            }))
        }

        fn delete(&mut self, key: &str, versioning: Versioning, named: Option<Named>) -> Outcome {
            self.clock += 10;
            self.run(Command::Delete(Delete {
                bucket: "b".into(),
                incarnation: 1,
                key: key.into(),
                versioning,
                named,
                if_match: None,
                at_ns: self.clock,
            }))
        }

        /// Every version of `key`, newest first: (ID, delete marker, ETag).
        fn versions(&self, key: &str) -> Vec<(String, bool, String)> {
            let mut out = Vec::new();
            let mut from = key::name("b", key, &NameRow::Version(0));
            let to = key::name("b", key, &NameRow::Upload(Vec::new()));
            while let Some((k, v)) = self.engine.next(&from, &to).unwrap() {
                let Some((_, _, NameRow::Version(order))) = key::decode_name(&k) else {
                    panic!("not a version row")
                };
                let v = Version::decode(&v).unwrap();
                out.push((id(v.null, order), v.marker, v.etag));
                from = after(&k);
            }
            out
        }
    }

    fn object(etag: &str) -> Version {
        Version {
            marker: false,
            null: false,
            modified_ns: 0,
            etag: etag.into(),
            size: 1,
            checksum: None,
            file: Some(1),
            owner: "o".into(),
            headers: Vec::new(),
        }
    }

    fn put_id(o: &Outcome) -> String {
        match o {
            Outcome::Put { version } => version.clone(),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn an_unversioned_key_has_one_null_version() {
        let mut r = Range::new();
        assert_eq!(put_id(&r.put("k", "a", Versioning::Unversioned)), "null");
        r.put("k", "b", Versioning::Unversioned);
        assert_eq!(r.versions("k"), [("null".into(), false, "b".into())]);
        assert_eq!(
            r.delete("k", Versioning::Unversioned, None),
            Outcome::Deleted {
                marker: false,
                version: None
            }
        );
        assert!(r.versions("k").is_empty());
        assert_eq!(current(&r.engine, "b", "k").unwrap(), None);
    }

    /// Enabled: every put is a version, deletes stack markers, and deleting a marker by its
    /// ID undeletes (05 §7.2–§7.3; s3-tests test_versioning_stack_delete_merkers).
    #[test]
    fn enabled_versioning_keeps_every_version_and_stacks_markers() {
        let mut r = Range::new();
        let v1 = put_id(&r.put("k", "a", Versioning::Enabled));
        let v2 = put_id(&r.put("k", "b", Versioning::Enabled));
        assert!(v2 < v1, "newer versions sort first");
        for _ in 0..3 {
            let Outcome::Deleted {
                marker: true,
                version: Some(_),
            } = r.delete("k", Versioning::Enabled, None)
            else {
                panic!("no marker")
            };
        }
        let versions = r.versions("k");
        assert_eq!(versions.len(), 5);
        assert!(versions[..3].iter().all(|(_, marker, _)| *marker));
        assert!(current(&r.engine, "b", "k").unwrap().unwrap().1.marker);
        // Removing the markers by ID brings "b" back as current.
        for (marker_id, _, _) in versions[..3].iter().cloned() {
            let order = key::parse_version_id(&marker_id).unwrap();
            assert_eq!(
                r.delete("k", Versioning::Enabled, Some(Named::Order(order))),
                Outcome::Deleted {
                    marker: true,
                    version: Some(marker_id)
                }
            );
        }
        assert_eq!(current(&r.engine, "b", "k").unwrap().unwrap().1.etag, "b");
        // A version removed by ID is gone; removing it again is still a success.
        let order = key::parse_version_id(&v1).unwrap();
        r.delete("k", Versioning::Enabled, Some(Named::Order(order)));
        assert_eq!(r.versions("k"), [(v2.clone(), false, "b".into())]);
        assert_eq!(
            r.delete("k", Versioning::Enabled, Some(Named::Order(order))),
            Outcome::Deleted {
                marker: false,
                version: Some(v1)
            }
        );
    }

    /// Suspended: puts and deletes replace the null version and keep the others (05 §7.2;
    /// s3-tests test_versioning_obj_plain_null_version_overwrite_suspended).
    #[test]
    fn suspended_versioning_replaces_only_the_null_version() {
        let mut r = Range::new();
        r.put("k", "pre", Versioning::Unversioned);
        let v = put_id(&r.put("k", "on", Versioning::Enabled));
        assert_eq!(r.versions("k").len(), 2);
        assert_eq!(put_id(&r.put("k", "off", Versioning::Suspended)), "null");
        assert_eq!(
            r.versions("k"),
            [
                ("null".into(), false, "off".into()),
                (v.clone(), false, "on".into())
            ]
        );
        assert_eq!(
            r.delete("k", Versioning::Suspended, None),
            Outcome::Deleted {
                marker: true,
                version: Some("null".into())
            }
        );
        assert_eq!(
            r.versions("k"),
            [
                ("null".into(), true, String::new()),
                (v, false, "on".into())
            ]
        );
        // The plain s3-tests case: a pre-versioning object overwritten while suspended leaves
        // exactly the null version.
        let mut r = Range::new();
        r.put("p", "pre", Versioning::Unversioned);
        r.put("p", "over", Versioning::Suspended);
        assert_eq!(r.versions("p"), [("null".into(), false, "over".into())]);
        assert_eq!(
            r.delete("p", Versioning::Suspended, Some(Named::Null)),
            Outcome::Deleted {
                marker: false,
                version: Some("null".into())
            }
        );
        assert!(r.versions("p").is_empty());
    }

    /// The conditional-write matrix of s3-tests (05 §2.2), judged at commit.
    #[test]
    fn preconditions_are_judged_against_the_current_version() {
        let mut r = Range::new();
        let none = |m: Match| Preconditions {
            if_none_match: Some(m),
            ..Preconditions::default()
        };
        let some = |m: Match| Preconditions {
            if_match: Some(m),
            ..Preconditions::default()
        };
        let v = Versioning::Enabled;
        assert_eq!(r.put_if("k", "a", v, some(Match::Any)), Outcome::NoSuchKey);
        assert!(matches!(
            r.put_if("k", "a", v, none(Match::Any)),
            Outcome::Put { .. }
        ));
        assert_eq!(
            r.put_if("k", "b", v, none(Match::Any)),
            Outcome::PreconditionFailed
        );
        assert_eq!(
            r.put_if("k", "b", v, none(Match::Tags(vec!["a".into()]))),
            Outcome::PreconditionFailed
        );
        assert_eq!(
            r.put_if("k", "b", v, some(Match::Tags(vec!["x".into()]))),
            Outcome::PreconditionFailed
        );
        assert!(matches!(
            r.put_if("k", "b", v, some(Match::Tags(vec!["x".into(), "a".into()]))),
            Outcome::Put { .. }
        ));
        // A delete marker is no current version to a write.
        r.delete("k", v, None);
        assert!(matches!(
            r.put_if("k", "c", v, none(Match::Any)),
            Outcome::Put { .. }
        ));
        // A refused write still advances the index.
        let index = r.engine.applied();
        r.put_if("k", "d", v, none(Match::Any));
        assert_eq!(r.engine.applied(), index + 1);
    }

    #[test]
    fn a_completed_upload_is_ordered_by_its_initiation() {
        let mut r = Range::new();
        let early = r.clock + 1;
        r.put("k", "later", Versioning::Enabled);
        r.index += 1;
        let outcome = apply(
            &mut r.engine,
            r.index,
            &Command::Put(Put {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
                versioning: Versioning::Enabled,
                preconditions: Preconditions::default(),
                at_ns: r.clock + 100,
                ordered_ns: Some(early),
                version: object("upload"),
            }),
        )
        .unwrap();
        let versions = r.versions("k");
        assert_eq!(versions[0].2, "later");
        assert_eq!(versions[1], (put_id(&outcome), false, "upload".into()));
    }

    #[test]
    fn listing_skips_markers_and_pauses_within_its_budget() {
        let mut r = Range::new();
        for k in ["a", "b", "c", "d"] {
            r.put(k, k, Versioning::Enabled);
        }
        r.delete("b", Versioning::Enabled, None);
        r.delete("c", Versioning::Enabled, None);
        let found = |s: Scan| match s {
            Scan::Found { key, .. } => key,
            other => panic!("{other:?}"),
        };
        assert_eq!(found(next_current(&r.engine, "b", b"", 10).unwrap()), "a");
        assert_eq!(
            found(next_current(&r.engine, "b", b"a\0", 10).unwrap()),
            "d"
        );
        // Two markers read, the budget spent: the next step starts just after "c".
        assert_eq!(
            next_current(&r.engine, "b", b"a\0", 2).unwrap(),
            Scan::Paused(b"c\0".to_vec())
        );
        assert_eq!(found(next_current(&r.engine, "b", b"c\0", 1).unwrap()), "d");
        assert_eq!(next_current(&r.engine, "b", b"e", 10).unwrap(), Scan::End);
        assert_eq!(
            next_current(&r.engine, "other", b"", 10).unwrap(),
            Scan::End
        );
    }

    impl Range {
        fn create(&mut self, key: &str) -> String {
            self.clock += 10;
            match self.run(Command::CreateUpload(CreateUpload {
                bucket: "b".into(),
                incarnation: 1,
                key: key.into(),
                at_ns: self.clock,
                upload: Upload {
                    initiated_ns: 0,
                    owner: "o".into(),
                    headers: vec![("content-type".into(), "a/b".into())],
                    checksum: None,
                },
            })) {
                Outcome::Created { upload } => upload,
                other => panic!("{other:?}"),
            }
        }

        fn part(&mut self, key: &str, upload: &str, number: u16, size: u64, file: u128) -> Outcome {
            self.run(Command::PutPart(PutPart {
                bucket: "b".into(),
                incarnation: 1,
                key: key.into(),
                upload: upload.into(),
                number,
                part: Part {
                    etag: format!("e{number}"),
                    size,
                    checksum: None,
                    file,
                    modified_ns: 0,
                },
            }))
        }

        fn complete(&mut self, key: &str, upload: &str, parts: &[(u16, &str, u128)]) -> Outcome {
            self.clock += 10;
            self.run(Command::Complete(Complete {
                bucket: "b".into(),
                incarnation: 1,
                key: key.into(),
                upload: upload.into(),
                versioning: Versioning::Enabled,
                preconditions: Preconditions::default(),
                at_ns: self.clock,
                parts: parts
                    .iter()
                    .map(|&(number, etag, file)| Listed {
                        number,
                        etag: etag.into(),
                        file,
                    })
                    .collect(),
                etag: "whole-2".into(),
                size: 11 << 20,
                checksum: None,
                file: Some(99),
            }))
        }
    }

    #[test]
    fn a_completed_upload_becomes_a_version_and_its_parts_go() {
        let mut r = Range::new();
        let first = r.create("k");
        let second = r.create("k");
        assert!(first < second, "uploads sort in initiation order");
        assert_eq!(r.part("k", &first, 1, MIN_PART, 11), Outcome::PartWritten);
        assert_eq!(r.part("k", &first, 2, 1 << 20, 12), Outcome::PartWritten);
        assert_eq!(r.part("k", &first, 3, 1, 13), Outcome::PartWritten);
        // A part uploaded again replaces the first.
        r.part("k", &first, 2, 6 << 20, 22);
        let pages = parts(&r.engine, "b", "k", &first, 0, 2).unwrap();
        assert_eq!(pages.iter().map(|(n, _)| *n).collect::<Vec<_>>(), [1, 2]);
        assert_eq!(pages[1].1.file, 22);
        assert_eq!(parts(&r.engine, "b", "k", &first, 2, 10).unwrap().len(), 1);

        let done = r.complete("k", &first, &[(1, "e1", 11), (2, "e2", 22)]);
        let Outcome::Put { version } = done.clone() else {
            panic!("{done:?}")
        };
        // Ordered at its initiation, with the upload's metadata.
        let order = key::parse_version_id(&version).unwrap();
        assert_eq!(order, !key::parse_version_id(&first).unwrap());
        let (_, v) = current(&r.engine, "b", "k").unwrap().unwrap();
        assert_eq!(
            (v.etag.as_str(), v.file, v.headers.len()),
            ("whole-2", Some(99), 1)
        );
        // The upload and every part, listed or not, are gone; the other upload remains.
        assert_eq!(upload(&r.engine, "b", "k", &first).unwrap(), None);
        assert!(
            parts(&r.engine, "b", "k", &first, 0, 10)
                .unwrap()
                .is_empty()
        );
        assert!(upload(&r.engine, "b", "k", &second).unwrap().is_some());
        // A retried complete answers as the first did (05 §4.4).
        assert_eq!(
            r.complete("k", &first, &[(1, "e1", 11), (2, "e2", 22)]),
            done
        );
        assert_eq!(r.part("k", &first, 4, 1, 14), Outcome::NoSuchUpload);
    }

    #[test]
    fn a_complete_checks_every_listed_part() {
        let mut r = Range::new();
        let u = r.create("k");
        r.part("k", &u, 1, MIN_PART - 1, 11);
        r.part("k", &u, 2, MIN_PART, 12);
        r.part("k", &u, 3, 1, 13);
        assert_eq!(r.complete("k", &u, &[]), Outcome::InvalidPartOrder);
        assert_eq!(
            r.complete("k", &u, &[(2, "e2", 12), (1, "e1", 11)]),
            Outcome::InvalidPartOrder
        );
        assert_eq!(
            r.complete("k", &u, &[(2, "e2", 12), (2, "e2", 12)]),
            Outcome::InvalidPartOrder
        );
        assert_eq!(
            r.complete("k", &u, &[(2, "e2", 12), (9, "e9", 19)]),
            Outcome::InvalidPart
        );
        assert_eq!(
            r.complete("k", &u, &[(2, "zz", 12), (3, "e3", 13)]),
            Outcome::InvalidPart
        );
        assert_eq!(
            r.complete("k", &u, &[(2, "e2", 77), (3, "e3", 13)]),
            Outcome::Stale
        );
        assert_eq!(
            r.complete("k", &u, &[(1, "e1", 11), (3, "e3", 13)]),
            Outcome::EntityTooSmall
        );
        // The last part may be any size; nothing refused removed the upload.
        assert!(matches!(
            r.complete("k", &u, &[(2, "e2", 12), (3, "e3", 13)]),
            Outcome::Put { .. }
        ));
        assert_eq!(
            r.complete("k", "0000000000000", &[(1, "e1", 1)]),
            Outcome::NoSuchUpload
        );
    }

    #[test]
    fn an_abort_removes_the_upload_and_its_parts() {
        let mut r = Range::new();
        let u = r.create("k");
        for n in 1..=5 {
            r.part("k", &u, n, 1, u128::from(n));
        }
        assert_eq!(
            r.run(Command::Abort(Abort {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
                upload: u.clone(),
            })),
            Outcome::Aborted
        );
        assert_eq!(upload(&r.engine, "b", "k", &u).unwrap(), None);
        assert!(parts(&r.engine, "b", "k", &u, 0, 10).unwrap().is_empty());
        assert_eq!(
            r.run(Command::Abort(Abort {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
                upload: u,
            })),
            Outcome::NoSuchUpload
        );
        // Uploads alone give a key no current version, and a listing passes over it.
        let other = r.create("m");
        r.part("m", &other, 1, 1, 1);
        assert_eq!(current(&r.engine, "b", "m").unwrap(), None);
        assert_eq!(next_current(&r.engine, "b", b"", 10).unwrap(), Scan::End);
    }

    /// Replaying the log from the durable point after a crash rebuilds the same rows.
    #[test]
    fn replay_after_a_crash_reaches_the_same_rows() {
        let open = Command::Gate(GateChange {
            bucket: "b".into(),
            incarnation: 1,
            attempt: 1,
            from: None,
            to: Some(GateState::Open),
        });
        let commands: Vec<Command> = std::iter::once(open)
            .chain((0..20u64).map(|i| {
                let versioning = [Versioning::Enabled, Versioning::Suspended][(i % 2) as usize];
                if i % 3 == 0 {
                    Command::Delete(Delete {
                        bucket: "b".into(),
                        incarnation: 1,
                        key: format!("k{}", i % 4),
                        versioning,
                        named: None,
                        if_match: None,
                        at_ns: 100 + i,
                    })
                } else {
                    Command::Put(Put {
                        bucket: "b".into(),
                        incarnation: 1,
                        key: format!("k{}", i % 4),
                        versioning,
                        preconditions: Preconditions::default(),
                        at_ns: 100 + i,
                        ordered_ns: None,
                        version: object(&format!("e{i}")),
                    })
                }
            }))
            .collect();
        let mut whole = Model::default();
        for (i, c) in commands.iter().enumerate() {
            apply(&mut whole, i as u64 + 1, c).unwrap();
        }
        let mut crashed = Model::default();
        for (i, c) in commands.iter().enumerate() {
            apply(&mut crashed, i as u64 + 1, c).unwrap();
            if i == 7 {
                crashed.persist().unwrap();
            }
        }
        crashed.crash();
        for (i, c) in commands.iter().enumerate().skip(crashed.applied() as usize) {
            apply(&mut crashed, i as u64 + 1, c).unwrap();
        }
        let all = |m: &Model| {
            let mut rows = Vec::new();
            let mut from = Vec::new();
            while let Some((k, v)) = m.next(&from, &[0xFF]).unwrap() {
                from = after(&k);
                rows.push((k, v));
            }
            rows
        };
        assert_eq!(all(&crashed), all(&whole));
    }

    use GateState::{Closed, Condemned, Open};

    #[test]
    fn writes_pass_only_an_open_gate_of_their_incarnation() {
        let mut r = Range::new();
        assert_eq!(put_id(&r.put("k", "a", Versioning::Unversioned)), "null");
        let mut stale = Put {
            bucket: "b".into(),
            incarnation: 2,
            key: "k".into(),
            versioning: Versioning::Unversioned,
            preconditions: Preconditions::default(),
            at_ns: 5_000,
            ordered_ns: None,
            version: object("x"),
        };
        assert_eq!(r.run(Command::Put(stale.clone())), Outcome::NoSuchBucket);
        stale.bucket = "c".into();
        stale.incarnation = 1;
        assert_eq!(r.run(Command::Put(stale)), Outcome::NoSuchBucket);
        assert_eq!(r.gate(Some(Open), Some(Closed)), Outcome::GateMoved);
        assert_eq!(
            r.put("k", "b", Versioning::Unversioned),
            Outcome::NoSuchBucket
        );
        assert_eq!(
            r.delete("k", Versioning::Unversioned, None),
            Outcome::NoSuchBucket
        );
        assert_eq!(r.gate(Some(Closed), Some(Open)), Outcome::GateMoved);
        assert_eq!(put_id(&r.put("k", "c", Versioning::Unversioned)), "null");
        assert_eq!(r.versions("k"), [("null".into(), false, "c".into())]);
    }

    /// A later attempt takes a gate over; the attempt it left behind can no longer move it.
    #[test]
    fn gates_move_by_the_steps_of_the_latest_attempt() {
        let mut r = Range::new();
        assert_eq!(r.gate_at(5, Some(Open), Some(Closed)), Outcome::GateMoved);
        assert_eq!(r.gate_at(5, Some(Open), Some(Closed)), Outcome::GateMoved);
        assert_eq!(r.gate_at(3, Some(Closed), Some(Open)), Outcome::Conflict);
        assert_eq!(r.gate_at(6, Some(Closed), Some(Closed)), Outcome::GateMoved);
        assert_eq!(
            r.gate_at(5, Some(Closed), Some(Condemned)),
            Outcome::Conflict
        );
        assert_eq!(r.gate_at(6, Some(Open), Some(Closed)), Outcome::GateMoved);
        assert_eq!(r.gate_at(6, Some(Open), Some(Condemned)), Outcome::Invalid);
        assert_eq!(r.gate_at(6, None, Some(Condemned)), Outcome::Invalid);
        assert_eq!(r.gate_at(6, Some(Open), None), Outcome::Invalid);
        assert_eq!(
            gate(&r.engine, "b").unwrap(),
            Some(Gate {
                incarnation: 1,
                attempt: 6,
                state: Closed
            })
        );
        // Another incarnation's step is not this gate's.
        let other = Command::Gate(GateChange {
            bucket: "b".into(),
            incarnation: 2,
            attempt: 9,
            from: Some(Closed),
            to: Some(Open),
        });
        assert_eq!(r.run(other), Outcome::Conflict);
    }

    /// A deleted bucket's uploads are collected, then its gate removed, and the floor keeps
    /// an attempt left behind from placing a gate again.
    #[test]
    fn a_condemned_bucket_is_collected_and_its_gate_removed() {
        let mut r = Range::new();
        let upload = r.create("k");
        for number in 1..=3 {
            assert_eq!(r.part("k", &upload, number, 1, 1), Outcome::PartWritten);
        }
        r.create("m");
        assert_eq!(r.gate_at(4, Some(Open), Some(Closed)), Outcome::GateMoved);
        assert_eq!(probe(&r.engine, "b", None, 10).unwrap(), Probe::Clear);
        let collect = |budget| {
            Command::Collect(Collect {
                bucket: "b".into(),
                incarnation: 1,
                budget,
            })
        };
        assert_eq!(r.run(collect(10)), Outcome::Conflict, "not yet condemned");
        assert_eq!(
            r.gate_at(4, Some(Closed), Some(Condemned)),
            Outcome::GateMoved
        );
        assert_eq!(r.gate_at(4, Some(Condemned), None), Outcome::NotEmpty);
        assert_eq!(r.run(collect(3)), Outcome::Collected { done: false });
        assert_eq!(r.run(collect(2)), Outcome::Collected { done: true });
        assert_eq!(r.run(collect(2)), Outcome::Collected { done: true });
        let (from, to) = key::bucket_span("b");
        assert_eq!(r.engine.next(&from, &to).unwrap(), None);
        assert_eq!(r.gate_at(4, Some(Condemned), None), Outcome::GateMoved);
        assert_eq!(r.gate_at(4, Some(Condemned), None), Outcome::GateMoved);
        assert_eq!(gate(&r.engine, "b").unwrap(), None);
        assert_eq!(r.gate_at(3, None, Some(Open)), Outcome::Conflict);
        assert_eq!(r.gate_at(4, None, Some(Open)), Outcome::GateMoved);
    }

    #[test]
    fn a_condemned_bucket_with_a_version_is_never_collected() {
        let mut r = Range::new();
        r.put("k", "a", Versioning::Enabled);
        r.gate(Some(Open), Some(Closed));
        r.gate(Some(Closed), Some(Condemned));
        let collect = Command::Collect(Collect {
            bucket: "b".into(),
            incarnation: 1,
            budget: 10,
        });
        assert_eq!(r.run(collect), Outcome::NotEmpty);
        assert_eq!(r.versions("k").len(), 1);
    }

    /// The probe finds versions and delete markers, passes over uploads one key at a time,
    /// and pauses within its budget.
    #[test]
    fn a_probe_finds_any_version_or_marker_and_pauses() {
        let mut r = Range::new();
        for key in ["a", "b", "c"] {
            let upload = r.create(key);
            r.part(key, &upload, 1, 1, 1);
        }
        assert_eq!(probe(&r.engine, "b", None, 10).unwrap(), Probe::Clear);
        r.delete("d", Versioning::Enabled, None);
        let Probe::Paused(at) = probe(&r.engine, "b", None, 2).unwrap() else {
            panic!("the budget ran out")
        };
        assert_eq!(
            probe(&r.engine, "b", Some(&at), 1).unwrap(),
            Probe::Paused(beyond_rows("b", "c"))
        );
        assert_eq!(probe(&r.engine, "b", Some(&at), 2).unwrap(), Probe::Found);
        // A position before the bucket starts at its first row.
        assert_eq!(probe(&r.engine, "b", Some(&[]), 4).unwrap(), Probe::Found);
        assert_eq!(probe(&r.engine, "c", None, 4).unwrap(), Probe::Clear);
    }
}
