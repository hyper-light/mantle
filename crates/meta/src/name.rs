//! The Name layer's state machine (docs/design/metadata.md §2): S3's writes to one object key
//! as commands applied at a log index, and its reads.
//!
//! Applying a command reads the key's rows, decides, and writes one batch with the entry's
//! index, so every replica that applies the same log holds the same rows. A write's time is
//! the range's clock's (clock.rs), so versions of a key never share an order. A write to a
//! bucket's objects passes only through the bucket's open gate for the incarnation it names
//! (docs/design/metadata.md §2).

use crate::clock;
use crate::engine::{Rows, Write};
use crate::error::MetaError;
use crate::key::{self, NULL_VERSION, NameRow};
use crate::record::{
    self, Checksum, DefaultRetention, Gate, GateState, Part, Retention, RetentionMode, Upload,
    Version,
};

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
    Retain(Retain),
    Hold(Hold),
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
            Self::Retain(c) => Some((&c.bucket, c.incarnation)),
            Self::Hold(c) => Some((&c.bucket, c.incarnation)),
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
    /// The version, with the retention and legal hold its request named, if any.
    pub version: Version,
    /// The bucket's default retention, which a version whose request named no retention takes
    /// from its creation (18 §2.4).
    pub default: Option<DefaultRetention>,
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
    /// The bucket's default retention, which the version takes when the upload named none.
    pub default: Option<DefaultRetention>,
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
    /// `x-amz-bypass-governance-retention: true` from a requester allowed
    /// `s3:BypassGovernanceRetention` (18 §2.2).
    pub bypass: bool,
}

/// PutObjectRetention (18 §1.2): places, extends, shortens or removes a version's retention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Retain {
    pub bucket: String,
    /// The bucket's incarnation the write was made under.
    pub incarnation: u64,
    pub key: String,
    /// The version; `None` for the current one.
    pub named: Option<Named>,
    /// The retention the version is to have; `None`, an empty `Retention`, removes it.
    pub retention: Option<Retention>,
    /// As DeleteObject's (18 §2.2).
    pub bypass: bool,
    pub at_ns: u64,
}

/// PutObjectLegalHold (18 §1.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hold {
    pub bucket: String,
    /// The bucket's incarnation the write was made under.
    pub incarnation: u64,
    pub key: String,
    /// The version; `None` for the current one.
    pub named: Option<Named>,
    pub on: bool,
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
    /// A version's retention was set or removed.
    Retained,
    /// A version's legal hold was set.
    Held,
    /// `403 AccessDenied`, "Access Denied because object protected by object lock." (18 §5):
    /// the write would remove, overwrite or weaken a lock that holds.
    Locked,
    /// `404 NoSuchVersion`: the version named does not exist.
    NoSuchVersion,
    /// The version is a delete marker, which holds no lock (18 §2.5): `405 MethodNotAllowed`
    /// when a request names it (18 §5).
    DeleteMarker,
}

/// Applies `command` as log entry `index`. A refused command still advances the index.
pub fn apply<E: Rows>(engine: &mut E, index: u64, command: &Command) -> Result<Outcome, MetaError> {
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
        Command::Retain(r) => retain(engine, r)?,
        Command::Hold(h) => hold(engine, h)?,
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
fn admits<E: Rows>(engine: &E, bucket: &str, incarnation: u64) -> Result<bool, MetaError> {
    Ok(gate(engine, bucket)?
        .is_some_and(|g| g.incarnation == incarnation && g.state == GateState::Open))
}

fn move_gate<E: Rows>(engine: &E, g: &GateChange) -> Result<(Outcome, Vec<Write>), MetaError> {
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

fn floor<E: Rows>(engine: &E) -> Result<u64, MetaError> {
    match engine.get(FLOOR)? {
        None => Ok(0),
        Some(bytes) => Ok(record::decode_number(&bytes, "floor")?),
    }
}

fn collect<E: Rows>(engine: &E, c: &Collect) -> Result<(Outcome, Vec<Write>), MetaError> {
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

fn put<E: Rows>(engine: &E, p: &Put) -> Result<(Outcome, Vec<Write>), MetaError> {
    let (bucket, key, versioning) = (p.bucket.as_str(), p.key.as_str(), p.versioning);
    let (preconditions, at_ns, ordered_ns, version) =
        (&p.preconditions, p.at_ns, p.ordered_ns, &p.version);
    let null = versioning != Versioning::Enabled;
    // A write that replaces the null version may not replace a locked one: "a protected object
    // version can't be overwritten" (18 §2.2). Object Lock keeps versioning enabled, so only a
    // write made under a stale view of the bucket reaches here.
    if null
        && let Some((_, v)) = self::version(engine, bucket, key, Named::Null)?
        && protected(&v, clock::now(engine, at_ns)?, false)?
    {
        return Ok((Outcome::Locked, Vec::new()));
    }
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
    if null {
        writes.extend(remove_null(engine, bucket, key)?.1);
        writes.push(Write::Put(
            key::name(bucket, key, &NameRow::Null),
            record::encode_number(order),
        ));
    }
    // "the object version's individual Object Lock settings override any bucket property
    // retention settings" (18 §2.4).
    let retention = match (version.retention, p.default) {
        (Some(retention), _) => Some(retention),
        (None, Some(default)) => Some(default.from(millis(when)?)),
        (None, None) => None,
    };
    let written = Version {
        marker: false,
        null,
        modified_ns: when,
        retention,
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

fn delete<E: Rows>(engine: &E, d: &Delete) -> Result<(Outcome, Vec<Write>), MetaError> {
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
    // A version is removed when one is named, or when an unversioned or suspended bucket's
    // delete replaces the null version; a locked one is refused (18 §2.5). A marker stacked
    // on a locked version is not: "Retention periods and legal holds don't prevent ... delete
    // markers to be added on top of the object" (18 §2.1).
    let removes = match named {
        Some(named) => Some(named),
        None if versioning != Versioning::Enabled => Some(Named::Null),
        None => None,
    };
    if let Some(removes) = removes
        && let Some((_, v)) = self::version(engine, bucket, key, removes)?
        && protected(&v, clock::now(engine, at_ns)?, d.bypass)?
    {
        return Ok((Outcome::Locked, Vec::new()));
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
                    retention: None,
                    legal_hold: None,
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

fn create_upload<E: Rows>(
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

fn put_part<E: Rows>(engine: &E, p: &PutPart) -> Result<(Outcome, Vec<Write>), MetaError> {
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

fn complete<E: Rows>(engine: &E, c: &Complete) -> Result<(Outcome, Vec<Write>), MetaError> {
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
                retention: upload_row.retention,
                legal_hold: upload_row.legal_hold,
            },
            default: c.default,
        },
    )?;
    if matches!(outcome, Outcome::Put { .. }) {
        // Parts not listed are discarded with the upload (05 §4.4).
        writes.extend(remove_upload(engine, &c.bucket, &c.key, &c.upload)?);
    }
    Ok((outcome, writes))
}

fn retain<E: Rows>(engine: &E, r: &Retain) -> Result<(Outcome, Vec<Write>), MetaError> {
    let (order, version) = match target(engine, &r.bucket, &r.key, r.named)? {
        Ok(found) => found,
        Err(outcome) => return Ok((outcome, Vec::new())),
    };
    let now = millis(clock::now(engine, r.at_ns)?)?;
    if !may_retain(version.retention, r.retention, now, r.bypass) {
        return Ok((Outcome::Locked, Vec::new()));
    }
    let written = Version {
        retention: r.retention,
        ..version
    };
    Ok((
        Outcome::Retained,
        vec![Write::Put(
            key::name(&r.bucket, &r.key, &NameRow::Version(order)),
            written.encode()?,
        )],
    ))
}

/// "Legal holds can be freely placed and removed by any user who has the
/// `s3:PutObjectLegalHold` permission" (18 §2.3).
fn hold<E: Rows>(engine: &E, h: &Hold) -> Result<(Outcome, Vec<Write>), MetaError> {
    let (order, version) = match target(engine, &h.bucket, &h.key, h.named)? {
        Ok(found) => found,
        Err(outcome) => return Ok((outcome, Vec::new())),
    };
    let written = Version {
        legal_hold: Some(h.on),
        ..version
    };
    Ok((
        Outcome::Held,
        vec![Write::Put(
            key::name(&h.bucket, &h.key, &NameRow::Version(order)),
            written.encode()?,
        )],
    ))
}

/// The version a lock's change names, or the current one: an object's, never a delete
/// marker's.
fn target<E: Rows>(
    engine: &E,
    bucket: &str,
    key: &str,
    named: Option<Named>,
) -> Result<Result<(u64, Version), Outcome>, MetaError> {
    let found = match named {
        None => current(engine, bucket, key)?,
        Some(named) => version(engine, bucket, key, named)?,
    };
    Ok(match found {
        None if named.is_some() => Err(Outcome::NoSuchVersion),
        None => Err(Outcome::NoSuchKey),
        Some((_, v)) if v.marker => Err(Outcome::DeleteMarker),
        Some(found) => Ok(found),
    })
}

/// Whether a version's retention may become `next` at `now`, Unix milliseconds (18 §2.2).
/// Once no retention holds, any may be placed. One that holds may be extended in its mode by
/// anyone who may place one; otherwise a GOVERNANCE retention changes only under bypass, and
/// a COMPLIANCE one "can't be changed, and its retention period can't be shortened".
fn may_retain(held: Option<Retention>, next: Option<Retention>, now: i64, bypass: bool) -> bool {
    let Some(held) = held.filter(|r| r.until_ms > now) else {
        return true;
    };
    match next {
        Some(next) if next.mode == held.mode && next.until_ms >= held.until_ms => true,
        _ => held.mode == RetentionMode::Governance && bypass,
    }
}

/// Whether `version` is protected at `now_ns` from removal or overwrite: under a legal hold,
/// which bypass does not lift, or while a retention holds, a GOVERNANCE one unless the
/// request bypasses it (18 §2.2, §2.3).
fn protected(version: &Version, now_ns: u64, bypass: bool) -> Result<bool, MetaError> {
    let now = millis(now_ns)?;
    Ok(version.legal_hold == Some(true)
        || version
            .retention
            .is_some_and(|r| r.until_ms > now && (r.mode == RetentionMode::Compliance || !bypass)))
}

/// Nanoseconds since the Unix epoch as milliseconds, the unit S3 dates a retention in.
fn millis(ns: u64) -> Result<i64, MetaError> {
    ns.checked_div(1_000_000)
        .and_then(|ms| i64::try_from(ms).ok())
        .ok_or(MetaError::Corrupt)
}

fn abort<E: Rows>(engine: &E, a: &Abort) -> Result<(Outcome, Vec<Write>), MetaError> {
    if upload(engine, &a.bucket, &a.key, &a.upload)?.is_none() {
        return Ok((Outcome::NoSuchUpload, Vec::new()));
    }
    Ok((
        Outcome::Aborted,
        remove_upload(engine, &a.bucket, &a.key, &a.upload)?,
    ))
}

/// Writes that remove an upload's row and every one of its parts: at most 10,000 (05 §4.1).
fn remove_upload<E: Rows>(
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
pub fn upload<E: Rows>(
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
pub fn parts<E: Rows>(
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
pub fn current<E: Rows>(
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
pub fn version<E: Rows>(
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
fn null_order<E: Rows>(engine: &E, bucket: &str, key: &str) -> Result<Option<u64>, MetaError> {
    match engine.get(&key::name(bucket, key, &NameRow::Null))? {
        None => Ok(None),
        Some(bytes) => Ok(Some(record::decode_number(&bytes, "null")?)),
    }
}

/// Writes that remove the key's null version and its pointer, and the version removed.
fn remove_null<E: Rows>(
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

/// What a listing's scan found (docs/design/s3-protocol.md §3). A scan passes at most a
/// budget of keys that hold nothing it lists, counting each one off, and pauses between keys,
/// so the key it names on pausing is the one to resume after.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scan<T> {
    Found(T),
    /// Nothing the scan lists is left before its end.
    End,
    /// The budget ran out: this is the last key passed.
    Paused(String),
}

/// A key's current version, as ListObjects lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Current {
    pub key: String,
    pub order: u64,
    pub version: Version,
}

/// The first key at or after `from` and before `to`, in object-key byte order, whose current
/// version is an object rather than a delete marker (05 §6.4). A delete marker's key and a key
/// holding only uploads are passed; each costs at most two rows, its null pointer and its
/// newest version or first upload.
pub fn next_current<E: Rows>(
    engine: &E,
    bucket: &str,
    from: &[u8],
    to: &[u8],
    budget: &mut usize,
) -> Result<Scan<Current>, MetaError> {
    let mut position = key::position(bucket, from);
    let end = key::position(bucket, to);
    loop {
        let Some((k, v)) = engine.next(&position, &end)? else {
            return Ok(Scan::End);
        };
        let Some((_, object, row)) = key::decode_name(&k) else {
            return Err(MetaError::Corrupt);
        };
        match row {
            // The null pointer sorts before the key's versions.
            NameRow::Null => position = after(&k),
            NameRow::Version(order) => {
                let version = Version::decode(&v)?;
                if !version.marker {
                    return Ok(Scan::Found(Current {
                        key: object,
                        order,
                        version,
                    }));
                }
                position = beyond_rows(bucket, &object);
                if pass(budget) {
                    return Ok(Scan::Paused(object));
                }
            }
            // Uploads with no version before them: the key has no current version.
            NameRow::Upload(_) | NameRow::Part(..) => {
                position = beyond_rows(bucket, &object);
                if pass(budget) {
                    return Ok(Scan::Paused(object));
                }
            }
        }
    }
}

/// A version or delete marker, as ListObjectVersions lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Versioned {
    pub key: String,
    pub order: u64,
    pub version: Version,
    /// The key's newest version: the first a scan meets on entering the key.
    pub latest: bool,
}

impl Versioned {
    /// The ID S3 shows for it: `null` for the null version (05 §7.1).
    pub fn version_id(&self) -> String {
        id(self.version.null, self.order)
    }
}

/// Where a scan of versions starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionFrom<'a> {
    /// The first version of the first key at or after these object-key bytes.
    Key(&'a [u8]),
    /// The version after the one `version_id` names, `null` included (05 §7.1). An ID that
    /// names no version of the key starts at the key's first, so no version is skipped.
    After { key: &'a str, version_id: &'a str },
}

/// The first version at or after `from` whose key is before `to`: keys in byte order, each
/// key's versions and delete markers newest first (05 §6.4). A key holding only uploads is
/// passed.
pub fn next_version<E: Rows>(
    engine: &E,
    bucket: &str,
    from: VersionFrom<'_>,
    to: &[u8],
    budget: &mut usize,
) -> Result<Scan<Versioned>, MetaError> {
    let (mut position, resumed) = match from {
        VersionFrom::Key(from) => (key::position(bucket, from), None),
        VersionFrom::After { key, version_id } => {
            let order = if version_id == NULL_VERSION {
                null_order(engine, bucket, key)?
            } else {
                key::parse_version_id(version_id)
            };
            match order {
                Some(order) => (
                    after(&key::name(bucket, key, &NameRow::Version(order))),
                    Some(key),
                ),
                None => (key::object(bucket, key), None),
            }
        }
    };
    let end = key::position(bucket, to);
    loop {
        let Some((k, v)) = engine.next(&position, &end)? else {
            return Ok(Scan::End);
        };
        let Some((_, object, row)) = key::decode_name(&k) else {
            return Err(MetaError::Corrupt);
        };
        match row {
            NameRow::Null => position = after(&k),
            NameRow::Version(order) => {
                let latest = resumed != Some(object.as_str());
                return Ok(Scan::Found(Versioned {
                    key: object,
                    order,
                    version: Version::decode(&v)?,
                    latest,
                }));
            }
            // Past the key's versions: its uploads list nothing here.
            NameRow::Upload(_) | NameRow::Part(..) => {
                position = beyond_rows(bucket, &object);
                if pass(budget) {
                    return Ok(Scan::Paused(object));
                }
            }
        }
    }
}

/// A multipart upload in progress, as ListMultipartUploads lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Uploaded {
    pub key: String,
    pub id: String,
    pub upload: Upload,
}

/// Where a scan of uploads starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadFrom<'a> {
    /// The first upload of the first key at or after these object-key bytes.
    Key(&'a [u8]),
    /// The upload after `id` of `key`.
    After { key: &'a str, id: &'a str },
}

/// The first upload at or after `from` whose key is before `to`: keys in byte order, each
/// key's uploads by ID, which is the order they were created in (05 §4.7). Parts are passed
/// with the seek past their upload, and a key's versions with one seek to its uploads; a key
/// holding only versions is passed.
pub fn next_upload<E: Rows>(
    engine: &E,
    bucket: &str,
    from: UploadFrom<'_>,
    to: &[u8],
    budget: &mut usize,
) -> Result<Scan<Uploaded>, MetaError> {
    let mut position = match from {
        UploadFrom::Key(from) => key::position(bucket, from),
        UploadFrom::After { key, id } => {
            let mut beyond = key::name(bucket, key, &NameRow::Upload(id.as_bytes().to_vec()));
            beyond.push(u8::MAX);
            beyond
        }
    };
    let end = key::position(bucket, to);
    // A key whose uploads were sought and not yet found.
    let mut sought: Option<String> = None;
    loop {
        let Some((k, v)) = engine.next(&position, &end)? else {
            return Ok(Scan::End);
        };
        let Some((_, object, row)) = key::decode_name(&k) else {
            return Err(MetaError::Corrupt);
        };
        if let Some(passed) = sought.take()
            && passed != object
            && pass(budget)
        {
            return Ok(Scan::Paused(passed));
        }
        match row {
            NameRow::Upload(id) => {
                return Ok(Scan::Found(Uploaded {
                    key: object,
                    id: String::from_utf8(id).map_err(|_| MetaError::Corrupt)?,
                    upload: Upload::decode(&v)?,
                }));
            }
            NameRow::Null | NameRow::Version(_) => {
                position = key::name(bucket, &object, &NameRow::Upload(Vec::new()));
                sought = Some(object);
            }
            // A part sorts after its upload, which a scan either lists or passes whole.
            NameRow::Part(..) => return Err(MetaError::Corrupt),
        }
    }
}

/// Counts a key passed off the budget; true once it is spent.
fn pass(budget: &mut usize) -> bool {
    *budget = budget.saturating_sub(1);
    *budget == 0
}

/// The range's gate for `bucket`.
pub fn gate<E: Rows>(engine: &E, bucket: &str) -> Result<Option<Gate>, MetaError> {
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
pub fn probe<E: Rows>(
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
    use crate::engine::{Engine, Model};

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
                default: None,
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
                bypass: false,
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
            retention: None,
            legal_hold: None,
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
                default: None,
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
        let scan = |bucket: &str, from: &[u8], to: &[u8], budget: usize| {
            let mut budget = budget;
            next_current(&r.engine, bucket, from, to, &mut budget).unwrap()
        };
        let found = |s: Scan<Current>| match s {
            Scan::Found(current) => current.key,
            other => panic!("{other:?}"),
        };
        let end = [u8::MAX];
        assert_eq!(found(scan("b", b"", &end, 10)), "a");
        assert_eq!(found(scan("b", b"a\0", &end, 10)), "d");
        // Two delete markers passed, the budget spent: the scan names the last key passed.
        assert_eq!(scan("b", b"a\0", &end, 2), Scan::Paused("c".into()));
        assert_eq!(found(scan("b", b"c\0", &end, 1)), "d");
        assert_eq!(scan("b", b"e", &end, 10), Scan::End);
        assert_eq!(scan("other", b"", &end, 10), Scan::End);
        // The scan stops at its end: "d" is not before "d".
        assert_eq!(scan("b", b"a\0", b"d", 10), Scan::End);
        // A null version's pointer is read with its key, not counted as a key passed.
        r.put("e", "e", Versioning::Unversioned);
        r.delete("e", Versioning::Suspended, None);
        let mut budget = 1;
        assert_eq!(
            next_current(&r.engine, "b", b"d\0", &end, &mut budget).unwrap(),
            Scan::Paused("e".into())
        );
    }

    /// Versions list by key, newest first, the first a scan meets on entering a key marked
    /// latest. A scan resumes after a version by its ID, `null` included, starts a key over
    /// when the ID names none of its versions, and passes keys holding only uploads.
    #[test]
    fn versions_list_newest_first_and_resume_by_id() {
        let mut r = Range::new();
        r.put("a", "a1", Versioning::Unversioned);
        r.put("a", "a2", Versioning::Enabled);
        r.delete("a", Versioning::Enabled, None);
        r.create("b");
        r.put("c", "c1", Versioning::Enabled);
        let end = [u8::MAX];
        let scan = |from: VersionFrom<'_>, budget: usize| {
            let mut budget = budget;
            next_version(&r.engine, "b", from, &end, &mut budget).unwrap()
        };
        let found = |s: Scan<Versioned>| match s {
            Scan::Found(v) => (
                v.key.clone(),
                id(v.version.null, v.order),
                v.version.marker,
                v.version.etag,
                v.latest,
            ),
            other => panic!("{other:?}"),
        };
        let mut seen = Vec::new();
        let mut step = scan(VersionFrom::Key(b""), 10);
        while let Scan::Found(v) = &step {
            let (key, version_id) = (v.key.clone(), id(v.version.null, v.order));
            seen.push(found(step.clone()));
            step = scan(
                VersionFrom::After {
                    key: &key,
                    version_id: &version_id,
                },
                10,
            );
        }
        assert_eq!(step, Scan::End);
        let marker = seen[0].1.clone();
        assert_eq!(
            seen,
            [
                ("a".into(), marker.clone(), true, String::new(), true),
                ("a".into(), seen[1].1.clone(), false, "a2".into(), false),
                ("a".into(), "null".into(), false, "a1".into(), false),
                ("c".into(), seen[3].1.clone(), false, "c1".into(), true),
            ]
        );
        // "b" holds only an upload: a budget of one passes it and pauses there.
        assert_eq!(
            scan(
                VersionFrom::After {
                    key: "a",
                    version_id: "null"
                },
                1
            ),
            Scan::Paused("b".into())
        );
        // An ID that names no version starts the key over, so nothing is skipped.
        assert_eq!(
            found(scan(
                VersionFrom::After {
                    key: "a",
                    version_id: "bogus"
                },
                10
            ))
            .1,
            marker
        );
        r.delete("a", Versioning::Enabled, Some(Named::Null));
        let mut budget = 10;
        let after_null = VersionFrom::After {
            key: "a",
            version_id: "null",
        };
        let step = next_version(&r.engine, "b", after_null, &end, &mut budget).unwrap();
        assert_eq!(found(step).1, marker);
    }

    /// Uploads list by key and ID, which is the order they were created in; their parts and
    /// their keys' versions are passed with a seek each, and a key holding only versions is
    /// passed within the budget.
    #[test]
    fn uploads_list_in_creation_order_and_pass_keys_without_them() {
        let mut r = Range::new();
        r.put("a", "a1", Versioning::Enabled);
        let u1 = r.create("b");
        r.part("b", &u1, 1, 1, 1);
        r.part("b", &u1, 2, 1, 2);
        let u2 = r.create("b");
        r.put("c", "c1", Versioning::Enabled);
        let u3 = r.create("c");
        r.put("d", "d1", Versioning::Enabled);
        assert!(u1 < u2);
        let end = [u8::MAX];
        let scan = |from: UploadFrom<'_>, budget: usize| {
            let mut budget = budget;
            next_upload(&r.engine, "b", from, &end, &mut budget).unwrap()
        };
        let mut seen = Vec::new();
        let mut step = scan(UploadFrom::Key(b""), 10);
        while let Scan::Found(u) = &step {
            seen.push((u.key.clone(), u.id.clone()));
            let (key, id) = (u.key.clone(), u.id.clone());
            step = scan(UploadFrom::After { key: &key, id: &id }, 10);
        }
        assert_eq!(step, Scan::End);
        assert_eq!(
            seen,
            [
                ("b".to_string(), u1.clone()),
                ("b".into(), u2.clone()),
                ("c".into(), u3.clone())
            ]
        );
        // "a" holds only versions: a budget of one passes it and pauses there.
        assert_eq!(scan(UploadFrom::Key(b""), 1), Scan::Paused("a".into()));
        // "d", the last key, holds only versions: the scan reaches the end rather than pausing.
        assert_eq!(scan(UploadFrom::After { key: "c", id: &u3 }, 1), Scan::End);
        // The end bounds the scan: nothing at or after "c".
        let mut budget = 10;
        assert_eq!(
            next_upload(
                &r.engine,
                "b",
                UploadFrom::After { key: "b", id: &u2 },
                b"c",
                &mut budget
            )
            .unwrap(),
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
                    retention: None,
                    legal_hold: None,
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
                default: None,
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
        let mut budget = 10;
        assert_eq!(
            next_current(&r.engine, "b", b"", &[u8::MAX], &mut budget).unwrap(),
            Scan::End
        );
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
                        bypass: false,
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
                        default: None,
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
            default: None,
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

    const MS: u64 = 1_000_000;

    fn governance(until_ms: i64) -> Option<Retention> {
        Some(Retention {
            mode: RetentionMode::Governance,
            until_ms,
        })
    }

    fn compliance(until_ms: i64) -> Option<Retention> {
        Some(Retention {
            mode: RetentionMode::Compliance,
            until_ms,
        })
    }

    impl Range {
        /// A version of `key` written at millisecond `at_ms` with the lock its request names,
        /// and the bucket's default retention.
        fn put_locked(
            &mut self,
            key: &str,
            at_ms: u64,
            retention: Option<Retention>,
            legal_hold: Option<bool>,
            default: Option<DefaultRetention>,
        ) -> String {
            self.clock = at_ms * MS;
            put_id(&self.run(Command::Put(Put {
                bucket: "b".into(),
                incarnation: 1,
                key: key.into(),
                versioning: Versioning::Enabled,
                preconditions: Preconditions::default(),
                at_ns: self.clock,
                ordered_ns: None,
                version: Version {
                    retention,
                    legal_hold,
                    ..object("e")
                },
                default,
            })))
        }

        fn remove(&mut self, key: &str, id: &str, at_ms: u64, bypass: bool) -> Outcome {
            self.run(Command::Delete(Delete {
                bucket: "b".into(),
                incarnation: 1,
                key: key.into(),
                versioning: Versioning::Enabled,
                named: Some(Named::Order(key::parse_version_id(id).unwrap())),
                if_match: None,
                at_ns: at_ms * MS,
                bypass,
            }))
        }

        fn retain(
            &mut self,
            key: &str,
            named: Option<Named>,
            retention: Option<Retention>,
            at_ms: u64,
            bypass: bool,
        ) -> Outcome {
            self.run(Command::Retain(Retain {
                bucket: "b".into(),
                incarnation: 1,
                key: key.into(),
                named,
                retention,
                bypass,
                at_ns: at_ms * MS,
            }))
        }

        fn hold(&mut self, key: &str, named: Option<Named>, on: bool) -> Outcome {
            self.run(Command::Hold(Hold {
                bucket: "b".into(),
                incarnation: 1,
                key: key.into(),
                named,
                on,
            }))
        }

        fn lock_of(&self, key: &str, id: &str) -> (Option<Retention>, Option<bool>) {
            let order = key::parse_version_id(id).unwrap();
            let (_, v) = version(&self.engine, "b", key, Named::Order(order))
                .unwrap()
                .unwrap();
            (v.retention, v.legal_hold)
        }
    }

    /// "If you issued a permanent DELETE request ... Amazon S3 returns an Access Denied"; a
    /// simple one stacks a marker (18 §2.5). GOVERNANCE yields to bypass, COMPLIANCE to
    /// nothing but time, and a legal hold to nothing but its removal (18 §2.2, §2.3).
    #[test]
    fn a_locked_version_is_not_deleted_until_its_lock_lifts() {
        let mut r = Range::new();
        let g = r.put_locked("k", 10, governance(1_000), None, None);
        assert_eq!(r.remove("k", &g, 20, false), Outcome::Locked);
        // A simple delete stacks a marker over it; the version stays.
        assert!(matches!(
            r.delete("k", Versioning::Enabled, None),
            Outcome::Deleted { marker: true, .. }
        ));
        assert_eq!(r.versions("k").len(), 2);
        assert!(matches!(
            r.remove("k", &g, 30, true),
            Outcome::Deleted { .. }
        ));
        assert_eq!(r.versions("k").len(), 1);

        let c = r.put_locked("k", 40, compliance(1_000), None, None);
        assert_eq!(r.remove("k", &c, 50, false), Outcome::Locked);
        assert_eq!(r.remove("k", &c, 50, true), Outcome::Locked);
        // It holds while its date is ahead, as a date placed must be (lock::ahead), and lifts
        // at that instant.
        assert_eq!(r.remove("k", &c, 999, false), Outcome::Locked);
        assert!(matches!(
            r.remove("k", &c, 1_000, false),
            Outcome::Deleted { .. }
        ));

        let h = r.put_locked("k", 1_100, None, Some(true), None);
        assert_eq!(r.remove("k", &h, 1_200, true), Outcome::Locked);
        assert_eq!(r.hold("k", None, false), Outcome::Held);
        assert_eq!(r.lock_of("k", &h), (None, Some(false)));
        assert!(matches!(
            r.remove("k", &h, 1_200, false),
            Outcome::Deleted { .. }
        ));
    }

    /// A retention may be extended by anyone; shortened, removed or moved to COMPLIANCE only
    /// under bypass in GOVERNANCE, and never in COMPLIANCE; and anything once it has lapsed (18
    /// §2.2, §5).
    #[test]
    fn a_retention_changes_as_its_mode_allows() {
        let mut r = Range::new();
        let v = r.put_locked("k", 10, governance(1_000), None, None);
        let named = Some(Named::Order(key::parse_version_id(&v).unwrap()));
        let retain = |r: &mut Range, retention, bypass| r.retain("k", named, retention, 20, bypass);
        assert_eq!(retain(&mut r, governance(2_000), false), Outcome::Retained);
        assert_eq!(retain(&mut r, governance(2_000), false), Outcome::Retained);
        assert_eq!(retain(&mut r, governance(1_500), false), Outcome::Locked);
        assert_eq!(retain(&mut r, None, false), Outcome::Locked);
        assert_eq!(retain(&mut r, compliance(3_000), false), Outcome::Locked);
        assert_eq!(r.lock_of("k", &v).0, governance(2_000));
        assert_eq!(retain(&mut r, governance(1_500), true), Outcome::Retained);
        assert_eq!(retain(&mut r, None, true), Outcome::Retained);
        assert_eq!(r.lock_of("k", &v).0, None);
        assert_eq!(retain(&mut r, compliance(3_000), false), Outcome::Retained);
        assert_eq!(retain(&mut r, compliance(4_000), false), Outcome::Retained);
        for (retention, bypass) in [
            (compliance(3_500), true),
            (governance(5_000), true),
            (None, true),
        ] {
            assert_eq!(retain(&mut r, retention, bypass), Outcome::Locked);
        }
        assert_eq!(r.lock_of("k", &v).0, compliance(4_000));
        // At its date, a COMPLIANCE retention no longer binds.
        assert_eq!(
            r.retain("k", named, governance(5_000), 4_000, false),
            Outcome::Retained
        );
        // The current version, a marker, a version or key that is not there.
        assert_eq!(
            r.retain("k", None, governance(6_000), 4_002, false),
            Outcome::Retained
        );
        r.delete("k", Versioning::Enabled, None);
        assert_eq!(
            r.retain("k", None, governance(7_000), 4_003, false),
            Outcome::DeleteMarker
        );
        assert_eq!(r.hold("k", None, true), Outcome::DeleteMarker);
        assert_eq!(
            r.retain("k", Some(Named::Order(7)), None, 4_004, false),
            Outcome::NoSuchVersion
        );
        assert_eq!(r.hold("gone", None, true), Outcome::NoSuchKey);
    }

    /// A version takes the bucket's default retention from its creation unless its request
    /// named one (18 §2.4), and a completed upload the lock its creation named.
    #[test]
    fn a_default_retention_runs_from_the_version_s_creation() {
        use crate::record::Period;
        let mut r = Range::new();
        let default = Some(DefaultRetention {
            mode: RetentionMode::Compliance,
            period: Period::Days(2),
        });
        let v = r.put_locked("k", 5_000, None, Some(true), default);
        assert_eq!(
            r.lock_of("k", &v),
            (compliance(5_000 + 2 * 86_400_000), Some(true))
        );
        let named = r.put_locked("k", 6_000, governance(9_000), None, default);
        assert_eq!(r.lock_of("k", &named), (governance(9_000), None));
        let years = Some(DefaultRetention {
            mode: RetentionMode::Governance,
            period: Period::Years(1),
        });
        let y = r.put_locked("k", 7_000, None, None, years);
        assert_eq!(r.lock_of("k", &y).0, governance(7_000 + 365 * 86_400_000));

        // An upload's lock, placed at its creation, goes to the version it completes.
        r.clock = 8_000 * MS;
        let Outcome::Created { upload } = r.run(Command::CreateUpload(CreateUpload {
            bucket: "b".into(),
            incarnation: 1,
            key: "m".into(),
            at_ns: r.clock,
            upload: Upload {
                initiated_ns: 0,
                owner: "o".into(),
                headers: Vec::new(),
                checksum: None,
                retention: compliance(50_000),
                legal_hold: Some(true),
            },
        })) else {
            panic!("no upload")
        };
        r.part("m", &upload, 1, 1, 7);
        let Outcome::Put { version: m } = r.complete("m", &upload, &[(1, "e1", 7)]) else {
            panic!("not completed")
        };
        assert_eq!(r.lock_of("m", &m), (compliance(50_000), Some(true)));
    }

    /// A write that would replace a locked null version is refused, as could only come from a
    /// gateway that read the bucket before Object Lock kept its versioning enabled.
    #[test]
    fn a_locked_null_version_is_not_replaced() {
        let mut r = Range::new();
        r.put("k", "a", Versioning::Unversioned);
        assert_eq!(
            r.retain("k", Some(Named::Null), governance(1_000), 0, false),
            Outcome::Retained
        );
        assert_eq!(r.put("k", "b", Versioning::Suspended), Outcome::Locked);
        assert_eq!(r.delete("k", Versioning::Suspended, None), Outcome::Locked);
        assert_eq!(
            r.delete("k", Versioning::Enabled, Some(Named::Null)),
            Outcome::Locked
        );
        assert_eq!(r.versions("k"), [("null".into(), false, "a".into())]);
        // Versioning enabled writes over it as a new version.
        assert!(matches!(
            r.put("k", "c", Versioning::Enabled),
            Outcome::Put { .. }
        ));
    }
}
