//! The Name layer's state machine (docs/design/metadata.md §2): S3's writes to one object key
//! as commands applied at a log index, and its reads.
//!
//! Applying a command reads the key's rows, decides, and writes one batch with the entry's
//! index, so every replica that applies the same log holds the same rows. A write's time is
//! the range's clock's (clock.rs), so versions of a key never share an order. A write to a
//! bucket's objects passes only through the bucket's open gate for the incarnation it names
//! (docs/design/metadata.md §2).
//!
//! A range holds the object keys of one span, which its lineage records with a generation
//! that every split and merge raises. A command for a key outside the span, or a
//! coordinator's step routed by another generation, is not the range's: it answers with its
//! lineage, takes nothing, and the sender routes the command again. A range frozen for a
//! merge, or ended by one, takes nothing but the merge's own steps (docs/design/metadata.md
//! §3).

use crate::clock;
use crate::engine::{Row, Rows, Write};
use crate::error::MetaError;
use crate::key::{self, NULL_VERSION, NameRow};
use crate::record::{
    self, Checksum, DefaultRetention, Descriptor, Gate, GateState, Holder, Lineage, Part,
    Retention, RetentionMode, Standing, Taken, Upload, Version,
};

pub use crate::record::Verdict;

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
    Reclaim(Reclaim),
    Check(Check),
    Unmark(Unmark),
    Disown(Disown),
    Split(Split),
    Freeze(Freeze),
    /// Applied by [`merge`], which reads the frozen range's rows; [`apply`] refuses it.
    Merge(Merge),
    Abandon(Abandon),
    End(End),
    Thaw(Thaw),
    Resolve(Resolve),
}

impl Command {
    /// Whether a range whose descriptor is `now` must refuse the command as routed by a
    /// descriptor it no longer matches: a write to an object key its span does not hold, the
    /// sweep's for a file of such a key, or a step routed by another generation. The range's
    /// own queue of released files goes with no key.
    fn moved(&self, now: &Descriptor) -> bool {
        let away = |bucket: &str, key: &str| !now.holds(&key::route(bucket, key));
        match self {
            Self::Put(c) => away(&c.bucket, &c.key),
            Self::Delete(c) => away(&c.bucket, &c.key),
            Self::CreateUpload(c) => away(&c.bucket, &c.key),
            Self::PutPart(c) => away(&c.bucket, &c.key),
            Self::Complete(c) => away(&c.bucket, &c.key),
            Self::Abort(c) => away(&c.bucket, &c.key),
            Self::Retain(c) => away(&c.bucket, &c.key),
            Self::Hold(c) => away(&c.bucket, &c.key),
            Self::Check(c) => c.files.iter().any(|f| away(&f.bucket, &f.key)),
            Self::Unmark(u) => u.files.iter().any(|m| away(&m.bucket, &m.key)),
            Self::Disown(d) => away(&d.bucket, &d.key),
            Self::Gate(g) => g.generation != now.generation,
            Self::Collect(c) => c.generation != now.generation,
            Self::Split(s) => s.generation != now.generation,
            Self::Freeze(f) => f.generation != now.generation,
            Self::Merge(m) => m.generation != now.generation,
            Self::Abandon(a) => a.generation != now.generation,
            Self::Reclaim(_) | Self::Resolve(_) | Self::End(_) | Self::Thaw(_) => false,
        }
    }

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
            Self::Gate(_)
            | Self::Collect(_)
            | Self::Reclaim(_)
            | Self::Check(_)
            | Self::Unmark(_)
            | Self::Disown(_)
            | Self::Split(_)
            | Self::Freeze(_)
            | Self::Merge(_)
            | Self::Abandon(_)
            | Self::End(_)
            | Self::Thaw(_)
            | Self::Resolve(_) => None,
        }
    }

    /// The file a write hands the range, which a version or part will reference if the write
    /// goes ahead, with the key it is for, the time of the entry that carries it, and the
    /// file's handover deadline.
    fn carries(&self) -> Option<Carried<'_>> {
        let (bucket, key, file, at_ns, deadline_ns) = match self {
            Self::Put(c) => (&c.bucket, &c.key, c.version.file?, c.at_ns, c.deadline_ns),
            Self::PutPart(c) => (&c.bucket, &c.key, c.part.file, c.at_ns, c.deadline_ns),
            Self::Complete(c) => (&c.bucket, &c.key, c.file?, c.at_ns, c.deadline_ns),
            _ => return None,
        };
        Some(Carried {
            bucket,
            key,
            file,
            at_ns,
            deadline_ns,
        })
    }
}

/// A file a write carries.
struct Carried<'a> {
    bucket: &'a str,
    key: &'a str,
    file: u128,
    at_ns: u64,
    deadline_ns: u64,
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
    /// The version's file's handover deadline, as the File range answered its write.
    pub deadline_ns: u64,
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
    pub at_ns: u64,
    /// The part's file's handover deadline, as the File range answered its write.
    pub deadline_ns: u64,
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
    /// The object file's handover deadline, as the File range answered its write.
    pub deadline_ns: u64,
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
    pub at_ns: u64,
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
    /// The generation of the descriptor the coordinator routed it by.
    pub generation: u64,
}

/// Removes rows of a condemned bucket, its uploads and their parts, as the collector does
/// after a delete (docs/design/metadata.md §2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Collect {
    pub bucket: String,
    pub incarnation: u64,
    /// Rows this entry may remove, which bounds its size.
    pub budget: u32,
    pub at_ns: u64,
    /// The generation of the descriptor the coordinator routed it by.
    pub generation: u64,
}

/// Splits the range at `at` (docs/design/metadata.md §3): the range keeps the object keys
/// before it, and a new range, `child`, takes the rest with their rows and marks, the gate of
/// every bucket whose keys it can hold, the gate floor and the clock. Both take the next
/// generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Split {
    /// The generation of the descriptor the splitter read.
    pub generation: u64,
    /// The first routing key the child holds (`key::route`), inside the span and past its
    /// first key.
    pub at: Vec<u8>,
    /// The child's ID, which no range has had.
    pub child: u64,
}

/// A split's child as the parent makes it ([`child`]): its descriptor, the rows it starts
/// with beside those it takes, and the key ranges whose rows it takes from the parent as they
/// stand before the split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Child {
    pub descriptor: Descriptor,
    /// Its lineage, and the parent's gate floor and clock.
    pub rows: Vec<Row>,
    /// Its keys' rows and marks, and its buckets' gates.
    pub spans: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Freezes the range for a merge into `into`, the range just below it, as the merge's driver
/// read both (docs/design/metadata.md §3). The range takes no step but the merge's own until
/// the merge ends it or it thaws, and its generation rises, so a step routed by what it was is
/// refused after it thaws. A range holding a merge not yet resolved is not frozen: it could
/// otherwise end with the merge unresolved, and a driver would read the merge as never taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Freeze {
    /// The generation of the descriptor the driver read.
    pub generation: u64,
    /// The range just below, whose ID and generation name the merge.
    pub into: Descriptor,
}

/// The lower range's decision on the merge its driver read it for at `generation`: it takes
/// `from`, the frozen range as it froze, keeping its own gates, if it holds no merge not yet
/// resolved, `from` begins where it ends, and `from` holds at most `max_rows` rows; otherwise
/// it refuses. Either way its generation moves on, so the merge is decided once, and a command
/// for it that comes later, however late, is routed by a generation the range no longer has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Merge {
    pub generation: u64,
    pub from: Descriptor,
    /// Rows the merge may take, which bounds its entry.
    pub max_rows: u64,
}

/// A driver abandons a merge not yet decided: the lower range, at the generation the merge
/// names, moves its generation on, so the merge is never taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Abandon {
    pub generation: u64,
}

/// The frozen range ends: the merge named by the range below and its `generation` was taken,
/// and `into` is that range as the merge left it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct End {
    pub generation: u64,
    pub into: Descriptor,
}

/// The frozen range serves again: the merge named by the range below and its `generation`
/// will never be taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Thaw {
    pub generation: u64,
}

/// The lower range lets go of the merge it took of range `from` at `generation`, once `from`
/// has ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolve {
    pub from: u64,
    pub generation: u64,
}

/// A coordinator's read, routed by the generation of the descriptor it holds: the answer, or
/// the range's lineage once the range has moved past that generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Routed<T> {
    Here(T),
    Moved(Box<Lineage>),
}

/// The collector reclaimed a released file, its blocks and chunks: its row in the queue goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reclaim {
    pub released_ns: u64,
    pub file: u128,
}

/// The sweep asks whether the range took each file, with its handover deadline: a file the
/// range marked was handed over, and one it did not, once past its deadline, is released
/// here and now, so no handover can take it after (docs/design/metadata.md §2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Check {
    pub files: Vec<Checked>,
    pub at_ns: u64,
}

/// A file the sweep asks about: the key it was made for, and its handover deadline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checked {
    pub bucket: String,
    pub key: String,
    pub file: u128,
    pub deadline_ns: u64,
}

/// These files are reclaimed: their marks go. Only the collector removes a mark, for a
/// file it took apart, which nothing references again; a mark the sweep removed on settling
/// a file would read, to a sweep delayed past it, as a file never handed over (audit B01).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unmark {
    pub files: Vec<Marked>,
}

/// The collector, reclaiming released composite file `owner`, gives back a part file it
/// names: released here if `owner` adopted it when its completion committed, and left
/// otherwise, since the upload or another composite holds it (audit B02).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Disown {
    pub bucket: String,
    pub key: String,
    pub file: u128,
    pub owner: u128,
    pub at_ns: u64,
}

/// A file's mark: the key it is under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Marked {
    pub bucket: String,
    pub key: String,
    pub file: u128,
}

/// A file in the range's queue of released files: when it was released, and the key it was
/// held under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Released {
    pub released_ns: u64,
    pub file: u128,
    pub holder: Holder,
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
    /// A released file's row went.
    Reclaimed,
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
    /// The write's file came past its handover deadline: the range released it, and the
    /// gateway writes the file again (`500 InternalError`, retried).
    Expired,
    /// The sweep's files, each with the range's verdict, in the order it asked.
    Checked(Vec<Verdict>),
    /// The marks of settled files went.
    Unmarked,
    /// A part file given back: `released` if the composite adopted it and it is released now.
    Disowned { released: bool },
    /// The command was routed by a descriptor the range no longer matches: it took nothing,
    /// and this is where its span went.
    Moved(Box<Lineage>),
    /// The range split: its lineage now, naming the child.
    Split(Box<Lineage>),
    /// The range froze for a merge: its lineage now.
    Frozen(Box<Lineage>),
    /// The range took the merge: its lineage now.
    Merged(Box<Lineage>),
    /// The range refused the merge, or abandoned it, and moved on: its lineage now.
    Refused(Box<Lineage>),
    /// The frozen range ended.
    Ended,
    /// The frozen range serves again: its lineage now.
    Thawed(Box<Lineage>),
    /// The range holds no merge from that range at that generation any more.
    Resolved,
}

/// Applies `command` as log entry `index`. A refused command still advances the index.
pub fn apply<E: Rows>(engine: &mut E, index: u64, command: &Command) -> Result<Outcome, MetaError> {
    let lineage = lineage(engine)?;
    // A frozen or ended range takes only the merge's own steps. A command that is not the
    // range's takes nothing, not even a file it carries, which its sender hands on to the
    // range that holds the key.
    let settled = match (lineage.standing, command) {
        (Standing::Frozen, Command::Thaw(t)) => Some(thaw(&lineage, t)?),
        (Standing::Frozen | Standing::Ended, Command::End(e)) => Some(end(&lineage, e)?),
        (Standing::Serving, Command::Thaw(_) | Command::End(_)) => {
            Some((Outcome::Conflict, Vec::new()))
        }
        (Standing::Serving, Command::Merge(_)) => Some((Outcome::Invalid, Vec::new())),
        (Standing::Serving, _) if !command.moved(&lineage.now) => None,
        _ => Some((Outcome::Moved(Box::new(lineage.clone())), Vec::new())),
    };
    if let Some((outcome, writes)) = settled {
        engine.apply(index, &writes)?;
        return Ok(outcome);
    }
    let admitted = match command.write_to() {
        Some((bucket, incarnation)) => admits(engine, bucket, incarnation)?,
        None => true,
    };
    let expired = match command.carries() {
        Some(c) => clock::now(engine, c.at_ns)? > c.deadline_ns,
        None => false,
    };
    let (outcome, writes) = match command {
        _ if !admitted => (Outcome::NoSuchBucket, Vec::new()),
        _ if expired => (Outcome::Expired, Vec::new()),
        Command::Put(p) => put(engine, p)?,
        Command::Delete(d) => delete(engine, d)?,
        Command::CreateUpload(c) => create_upload(engine, c)?,
        Command::PutPart(p) => put_part(engine, p)?,
        Command::Complete(c) => complete(engine, c)?,
        Command::Abort(a) => abort(engine, a)?,
        Command::Retain(r) => retain(engine, r)?,
        Command::Hold(h) => hold(engine, h)?,
        Command::Gate(g) => move_gate(engine, &lineage.now, g)?,
        Command::Collect(c) => collect(engine, c)?,
        Command::Reclaim(r) => (
            Outcome::Reclaimed,
            vec![Write::Delete(key::released(r.released_ns, r.file))],
        ),
        Command::Check(c) => check(engine, c)?,
        Command::Unmark(u) => (
            Outcome::Unmarked,
            u.files
                .iter()
                .map(|m| Write::Delete(key::mark(&m.bucket, &m.key, m.file)))
                .collect(),
        ),
        Command::Disown(d) => disown(engine, d)?,
        Command::Split(s) => split(engine, s)?,
        Command::Freeze(f) => freeze(&lineage, f)?,
        Command::Abandon(_) => refuse(lineage.clone())?,
        Command::Resolve(r) => resolve(&lineage, r)?,
        // Settled above.
        Command::Merge(_) | Command::End(_) | Command::Thaw(_) => (Outcome::Invalid, Vec::new()),
    };
    // A file a write carries is referenced if the write goes ahead, and released if it is
    // refused: the gateway wrote it for this write alone. Either way the range has taken it,
    // and one that came within its deadline is marked for the sweep, which settles a file
    // only once its deadline has passed, so it comes by after and removes the mark. One past
    // its deadline may come after the sweep settled the file, and a mark would stay; if the
    // sweep comes by after, it releases the file a second time, which the reclaimer finds
    // gone (docs/design/metadata.md §2).
    let mut writes = writes;
    if let Some(c) = command.carries() {
        if !matches!(outcome, Outcome::Put { .. } | Outcome::PartWritten) {
            let now = clock::now(engine, c.at_ns)?;
            writes.extend(release(now, c.bucket, c.key, c.file)?);
        }
        if !expired {
            let mark = key::mark(c.bucket, c.key, c.file);
            writes.push(Write::Put(mark, Vec::new()));
        }
    }
    engine.apply(index, &writes)?;
    Ok(outcome)
}

/// The range's lineage: its descriptor, and the child of its last split.
const LINEAGE: &[u8] = &[key::LOCAL, key::marker::LINEAGE];

/// The range's lineage. Every Name range has one from its making: the first by [`first`],
/// every other by the split that made it.
pub fn lineage<E: Rows>(engine: &E) -> Result<Lineage, MetaError> {
    let bytes = engine.get(LINEAGE)?.ok_or(MetaError::Corrupt)?;
    Ok(Lineage::decode(&bytes)?)
}

/// The rows of a cell's first Name range, `id`: every object key, at generation 1.
pub fn first(id: u64) -> Result<Vec<Row>, MetaError> {
    let lineage = serving(Descriptor {
        id,
        lo: Vec::new(),
        hi: None,
        generation: 1,
    });
    Ok(vec![(LINEAGE.to_vec(), lineage.encode()?)])
}

/// A serving range's lineage, new: no child, no merge.
fn serving(now: Descriptor) -> Lineage {
    Lineage {
        now,
        child: None,
        standing: Standing::Serving,
        into: None,
        taken: None,
    }
}

/// `lineage` a generation on.
fn onward(mut lineage: Lineage) -> Result<Lineage, MetaError> {
    lineage.now.generation = lineage
        .now
        .generation
        .checked_add(1)
        .ok_or(MetaError::Corrupt)?;
    Ok(lineage)
}

fn freeze(lineage: &Lineage, f: &Freeze) -> Result<(Outcome, Vec<Write>), MetaError> {
    if lineage.taken.is_some() {
        return Ok((Outcome::Conflict, Vec::new()));
    }
    if f.into.hi.as_ref() != Some(&lineage.now.lo) || f.into.id == lineage.now.id {
        return Ok((Outcome::Invalid, Vec::new()));
    }
    let mut frozen = onward(lineage.clone())?;
    frozen.standing = Standing::Frozen;
    frozen.into = Some(f.into.clone());
    let row = Write::Put(LINEAGE.to_vec(), frozen.encode()?);
    Ok((Outcome::Frozen(Box::new(frozen)), vec![row]))
}

/// The range refuses a merge, or abandons one, moving its generation on.
fn refuse(lineage: Lineage) -> Result<(Outcome, Vec<Write>), MetaError> {
    let refused = onward(lineage)?;
    let row = Write::Put(LINEAGE.to_vec(), refused.encode()?);
    Ok((Outcome::Refused(Box::new(refused)), vec![row]))
}

/// Whether a frozen range is frozen for the merge the range below names with `generation`.
fn frozen_for(lineage: &Lineage, generation: u64) -> bool {
    lineage.standing == Standing::Frozen
        && lineage
            .into
            .as_ref()
            .is_some_and(|d| d.generation == generation)
}

fn thaw(lineage: &Lineage, t: &Thaw) -> Result<(Outcome, Vec<Write>), MetaError> {
    if !frozen_for(lineage, t.generation) {
        return Ok((Outcome::Conflict, Vec::new()));
    }
    let mut thawed = onward(lineage.clone())?;
    thawed.standing = Standing::Serving;
    thawed.into = None;
    let row = Write::Put(LINEAGE.to_vec(), thawed.encode()?);
    Ok((Outcome::Thawed(Box::new(thawed)), vec![row]))
}

/// The frozen range ends; an end repeated once it has is answered the same. Its rows stay
/// for the replicas of the range that took them, which each read their own copy, until the
/// replica group ends.
fn end(lineage: &Lineage, e: &End) -> Result<(Outcome, Vec<Write>), MetaError> {
    let into = lineage.into.as_ref().map(|d| d.id);
    match lineage.standing {
        Standing::Ended if into == Some(e.into.id) => Ok((Outcome::Ended, Vec::new())),
        Standing::Frozen if frozen_for(lineage, e.generation) && into == Some(e.into.id) => {
            let ended = Lineage {
                standing: Standing::Ended,
                into: Some(e.into.clone()),
                ..lineage.clone()
            };
            let row = Write::Put(LINEAGE.to_vec(), ended.encode()?);
            Ok((Outcome::Ended, vec![row]))
        }
        _ => Ok((Outcome::Conflict, Vec::new())),
    }
}

fn resolve(lineage: &Lineage, r: &Resolve) -> Result<(Outcome, Vec<Write>), MetaError> {
    let held = Taken {
        from: r.from,
        generation: r.generation,
    };
    if lineage.taken != Some(held) {
        return Ok((Outcome::Resolved, Vec::new()));
    }
    let resolved = Lineage {
        taken: None,
        ..lineage.clone()
    };
    let row = Write::Put(LINEAGE.to_vec(), resolved.encode()?);
    Ok((Outcome::Resolved, vec![row]))
}

/// Applies merge `m`'s decision as log entry `index` of the lower range, reading `from`, this
/// replica's copy of the frozen range (docs/design/metadata.md §3). Every replica of the
/// frozen range applied its freeze before the merge was proposed, and a frozen range takes no
/// step, so every replica of this range reads the same rows. The range takes the frozen
/// range's rows and marks, its queue of released files, and the gates of the buckets it holds
/// none of; the later of the two clocks and the higher of the two floors; and none of its
/// sessions.
pub fn merge<E: Rows, F: Rows>(
    engine: &mut E,
    index: u64,
    m: &Merge,
    from: &F,
) -> Result<Outcome, MetaError> {
    let lineage = lineage(engine)?;
    if lineage.standing != Standing::Serving || lineage.now.generation != m.generation {
        engine.apply(index, &[])?;
        return Ok(Outcome::Moved(Box::new(lineage)));
    }
    let adjacent = lineage.now.hi.as_ref() == Some(&m.from.lo) && m.from.id != lineage.now.id;
    let taken = if lineage.taken.is_none() && adjacent {
        taking(engine, &lineage, m, from)?
    } else {
        None
    };
    let (outcome, writes) = match taken {
        Some(taken) => taken,
        None => refuse(lineage)?,
    };
    engine.apply(index, &writes)?;
    Ok(outcome)
}

/// The rows by which the range takes the frozen range `from`; `None` if `from` holds more rows
/// than the merge may take.
fn taking<E: Rows, F: Rows>(
    engine: &E,
    lineage: &Lineage,
    m: &Merge,
    from: &F,
) -> Result<Option<(Outcome, Vec<Write>)>, MetaError> {
    let theirs = self::lineage(from)?;
    let named = theirs
        .into
        .as_ref()
        .is_some_and(|d| d.id == lineage.now.id && d.generation == m.generation);
    if theirs.standing != Standing::Frozen || !named || theirs.now != m.from {
        return Err(MetaError::Unfrozen);
    }
    let mut writes = Vec::new();
    let mut spans = key::name_spans(&m.from.lo, m.from.hi.as_deref()).to_vec();
    spans.push(key::released_all());
    let (gates, gates_end) = (vec![key::LOCAL, key::marker::GATE], key::GATES_END.to_vec());
    spans.push((gates, gates_end));
    let mut rows: u64 = 0;
    for (mut at, to) in spans {
        while let Some((k, v)) = from.next(&at, &to)? {
            rows = rows.checked_add(1).ok_or(MetaError::Corrupt)?;
            if rows > m.max_rows {
                return Ok(None);
            }
            at = after(&k);
            // The range keeps its own gate of a bucket both hold keys of.
            if key::decode_gate(&k).is_some() && engine.get(&k)?.is_some() {
                continue;
            }
            writes.push(Write::Put(k, v));
        }
    }
    for local in [FLOOR, clock::ROW] {
        let ours = engine
            .get(local)?
            .map(|b| record::decode_number(&b, "local"))
            .transpose()?;
        let theirs = from
            .get(local)?
            .map(|b| record::decode_number(&b, "local"))
            .transpose()?;
        if let Some(later) = ours.max(theirs) {
            writes.push(Write::Put(local.to_vec(), record::encode_number(later)));
        }
    }
    let generation = lineage
        .now
        .generation
        .max(m.from.generation)
        .checked_add(1)
        .ok_or(MetaError::Corrupt)?;
    let merged = Lineage {
        now: Descriptor {
            id: lineage.now.id,
            lo: lineage.now.lo.clone(),
            hi: m.from.hi.clone(),
            generation,
        },
        // A child the range takes back is no longer where any of its span went.
        child: lineage.child.clone().filter(|c| c.id != m.from.id),
        standing: Standing::Serving,
        into: None,
        taken: Some(Taken {
            from: m.from.id,
            generation: m.generation,
        }),
    };
    writes.push(Write::Put(LINEAGE.to_vec(), merged.encode()?));
    Ok(Some((Outcome::Merged(Box::new(merged)), writes)))
}

/// A split the range takes: its lineage after, the child's descriptor, and where the gates
/// the child takes begin and those the range keeps end.
struct Cut {
    parent: Lineage,
    child: Descriptor,
    child_gates: Vec<u8>,
    kept_gates_past: Vec<u8>,
}

/// The split `s` does, or the outcome that refuses it.
fn cut<E: Rows>(engine: &E, s: &Split) -> Result<Result<Cut, Outcome>, MetaError> {
    let lineage = lineage(engine)?;
    let now = &lineage.now;
    if lineage.standing != Standing::Serving || s.generation != now.generation {
        return Ok(Err(Outcome::Moved(Box::new(lineage))));
    }
    let Some((bucket, key)) = key::decode_route(&s.at) else {
        return Ok(Err(Outcome::Invalid));
    };
    if s.at <= now.lo || now.hi.as_ref().is_some_and(|hi| s.at >= *hi) || s.child == now.id {
        return Ok(Err(Outcome::Invalid));
    }
    let generation = now.generation.checked_add(1).ok_or(MetaError::Corrupt)?;
    let child = Descriptor {
        id: s.child,
        lo: s.at.clone(),
        hi: now.hi.clone(),
        generation,
    };
    let parent = Lineage {
        now: Descriptor {
            id: now.id,
            lo: now.lo.clone(),
            hi: Some(s.at.clone()),
            generation,
        },
        child: Some(child.clone()),
        ..lineage.clone()
    };
    // The child can hold keys of every bucket from the one `at` falls in; the range keeps the
    // gates of those whose keys begin before `at`, the one `at` falls in unless `at` is its
    // first routing key.
    let child_gates = key::gate(&bucket);
    let kept_gates_past = if key.is_empty() {
        child_gates.clone()
    } else {
        after(&child_gates)
    };
    Ok(Ok(Cut {
        parent,
        child,
        child_gates,
        kept_gates_past,
    }))
}

fn split<E: Rows>(engine: &E, s: &Split) -> Result<(Outcome, Vec<Write>), MetaError> {
    let cut = match cut(engine, s)? {
        Ok(cut) => cut,
        Err(outcome) => return Ok((outcome, Vec::new())),
    };
    let mut writes = vec![Write::Put(LINEAGE.to_vec(), cut.parent.encode()?)];
    for (from, to) in key::name_spans(&cut.child.lo, cut.child.hi.as_deref()) {
        writes.push(Write::Clear(from, to));
    }
    writes.push(Write::Clear(cut.kept_gates_past, key::GATES_END.to_vec()));
    Ok((Outcome::Split(Box::new(cut.parent)), writes))
}

/// The child split `s` makes, read from the range before it applies the split; `None` if the
/// range would refuse it. The child starts with its lineage and the range's gate floor and
/// clock, so no attempt the range refused may place a gate there and no time it gave repeats,
/// and takes the rows of its keys, their marks and its buckets' gates as they stand. It holds
/// no session and none of the range's queue of released files, which the range keeps.
pub fn child<E: Rows>(engine: &E, s: &Split) -> Result<Option<Child>, MetaError> {
    let Ok(cut) = cut(engine, s)? else {
        return Ok(None);
    };
    let lineage = serving(cut.child.clone());
    let mut rows = vec![(LINEAGE.to_vec(), lineage.encode()?)];
    for local in [FLOOR, clock::ROW] {
        if let Some(value) = engine.get(local)? {
            rows.push((local.to_vec(), value));
        }
    }
    let mut spans = key::name_spans(&cut.child.lo, cut.child.hi.as_deref()).to_vec();
    spans.push((cut.child_gates, key::GATES_END.to_vec()));
    Ok(Some(Child {
        descriptor: cut.child,
        rows,
        spans,
    }))
}

/// The rows that release `file`, held under `key`, at the range's time `now_ns`: nothing
/// references it any more, and the collector reclaims it once the grace period has passed. A
/// mark the sweep left behind, stopping between settling the file and unmarking it, goes with
/// it (docs/design/metadata.md §2).
fn release(now_ns: u64, bucket: &str, key: &str, file: u128) -> Result<[Write; 2], MetaError> {
    let holder = Holder {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
    };
    Ok([
        Write::Put(key::released(now_ns, file), holder.encode()?),
        Write::Delete(key::mark(bucket, key, file)),
    ])
}

/// The range's verdict on each file the sweep asks about, releasing and marking every file
/// never handed over whose deadline has passed at the range's time. The check records that
/// time as a write's, so every later entry reads a time at least as late, even one a leader
/// with a slower clock proposed, and a handover that comes after finds the deadline passed.
fn check<E: Rows>(engine: &E, c: &Check) -> Result<(Outcome, Vec<Write>), MetaError> {
    let (now, clock) = clock::tick(engine, c.at_ns)?;
    let mut verdicts = Vec::with_capacity(c.files.len());
    let mut writes = vec![clock];
    for f in &c.files {
        let mark = key::mark(&f.bucket, &f.key, f.file);
        verdicts.push(if engine.get(&mark)?.is_some() {
            Verdict::Held
        } else if now > f.deadline_ns {
            let [queued, _] = release(now, &f.bucket, &f.key, f.file)?;
            writes.push(queued);
            writes.push(Write::Put(mark, Vec::new()));
            Verdict::Released
        } else {
            Verdict::Young
        });
    }
    Ok((Outcome::Checked(verdicts), writes))
}

/// The files the range released before `before_ns`, oldest first, at most `max`: the
/// collector's work.
pub fn released<E: Rows>(
    engine: &E,
    before_ns: u64,
    max: usize,
) -> Result<Vec<Released>, MetaError> {
    let (mut from, to) = key::released_before(before_ns);
    let mut out = Vec::new();
    while out.len() < max {
        let Some((k, v)) = engine.next(&from, &to)? else {
            break;
        };
        let (released_ns, file) = key::decode_released(&k).ok_or(MetaError::Corrupt)?;
        out.push(Released {
            released_ns,
            file,
            holder: Holder::decode(&v)?,
        });
        from = after(&k);
    }
    Ok(out)
}

/// The range's gate floor: no attempt older than it may place a gate where there is none. A
/// gate's removal raises it to the removing attempt, so a coordinator left behind by a later
/// attempt cannot open a gate for a bucket that is gone, and the range keeps no row for it.
const FLOOR: &[u8] = &[key::LOCAL, key::marker::FLOOR];

/// Whether the range admits a write to `bucket` made under `incarnation`.
fn admits<E: Rows>(engine: &E, bucket: &str, incarnation: u64) -> Result<bool, MetaError> {
    Ok(gate(engine, bucket)?
        .is_some_and(|g| g.incarnation == incarnation && g.state == GateState::Open))
}

fn move_gate<E: Rows>(
    engine: &E,
    now: &Descriptor,
    g: &GateChange,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    use GateState::{Closed, Condemned, Open};
    // A range keeps gates only for the buckets whose keys it can hold, which a split divides
    // by its span.
    let (first, past) = key::bucket_routes(&g.bucket);
    if !now.meets(&first, &past) {
        return Ok((Outcome::Invalid, Vec::new()));
    }
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
            let floor = floor(engine)?;
            // A deleted bucket's cleanup resumed after it dropped this gate: the floor shows
            // this attempt, or a later one, removed it, and the step it repeats is done.
            if (g.from, g.to) == (Some(Closed), Some(Condemned)) && g.attempt <= floor {
                return Ok((Outcome::GateMoved, Vec::new()));
            }
            if g.from.is_some() || g.attempt < floor {
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
    let (mut from, to) = key::bucket_span(&c.bucket);
    match gate(engine, &c.bucket)? {
        Some(g) if g.incarnation == c.incarnation && g.state == GateState::Condemned => {}
        // A gate is dropped only once the range holds no row of its bucket: a cleanup that
        // resumes after the drop has nothing left to collect here.
        None if engine.next(&from, &to)?.is_none() => {
            return Ok((Outcome::Collected { done: true }, Vec::new()));
        }
        _ => return Ok((Outcome::Conflict, Vec::new())),
    }
    let now = clock::now(engine, c.at_ns)?;
    let mut writes = Vec::new();
    for _ in 0..c.budget {
        let Some((k, v)) = engine.next(&from, &to)? else {
            return Ok((Outcome::Collected { done: true }, writes));
        };
        match key::decode_name(&k) {
            Some((_, _, NameRow::Upload(_))) => {}
            Some((_, object, NameRow::Part(..))) => {
                writes.extend(release(now, &c.bucket, &object, Part::decode(&v)?.file)?);
            }
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
        writes.extend(remove_null(engine, bucket, key, time)?.1);
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
    let now = clock::now(engine, at_ns)?;
    match named {
        Some(Named::Null) => {
            let (removed, writes) = remove_null(engine, bucket, key, now)?;
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
                    if let Some(file) = v.file {
                        writes.extend(release(now, bucket, key, file)?);
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
                let (_, writes) = remove_null(engine, bucket, key, now)?;
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
                    writes.extend(remove_null(engine, bucket, key, time)?.1);
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
    let mut writes = Vec::with_capacity(2);
    // A part uploaded again replaces the first (05 §4.3), whose file nothing then references.
    if let Some(replaced) = engine.get(&row)?.map(|b| Part::decode(&b)).transpose()?
        && replaced.file != p.part.file
    {
        let now = clock::now(engine, p.at_ns)?;
        writes.extend(release(now, &p.bucket, &p.key, replaced.file)?);
    }
    writes.push(Write::Put(row, p.part.encode()?));
    Ok((Outcome::PartWritten, writes))
}

fn complete<E: Rows>(engine: &E, c: &Complete) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some(upload_row) = upload(engine, &c.bucket, &c.key, &c.upload)? else {
        // A retry of a complete that committed finds the version it made (05 §4.4).
        let made = match key::parse_version_id(&c.upload) {
            Some(initiated) => version(engine, &c.bucket, &c.key, Named::Order(!initiated))?,
            None => None,
        };
        return Ok(match made {
            Some((order, v)) if !v.marker && v.etag == c.etag => {
                // A retry the gateway made a file for again: the version holds the first.
                let mut writes = Vec::new();
                if let Some(file) = c.file
                    && v.file != c.file
                {
                    let now = clock::now(engine, c.at_ns)?;
                    writes.extend(release(now, &c.bucket, &c.key, file)?);
                }
                (
                    Outcome::Put {
                        version: id(v.null, order),
                    },
                    writes,
                )
            }
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
            deadline_ns: c.deadline_ns,
        },
    )?;
    if matches!(outcome, Outcome::Put { .. }) {
        // Parts not listed are discarded with the upload (05 §4.4); those listed are now the
        // object's file's extents, adopted by it: their marks name it, so reclaiming it gives
        // them back, and reclaiming any other composite of them does not (audit B02). With no
        // file to adopt them, nothing references them.
        let listed: Vec<u16> = c.parts.iter().map(|p| p.number).collect();
        let now = clock::now(engine, c.at_ns)?;
        writes.extend(remove_upload(
            engine, &c.bucket, &c.key, &c.upload, &listed, now,
        )?);
        for part in &c.parts {
            match c.file {
                Some(by) => writes.push(Write::Put(
                    key::mark(&c.bucket, &c.key, part.file),
                    crate::record::Adopted { by }.encode(),
                )),
                None => writes.extend(release(now, &c.bucket, &c.key, part.file)?),
            }
        }
    }
    Ok((outcome, writes))
}

/// Releases part file `d.file` if composite `d.owner` adopted it: its mark names `d.owner`.
fn disown<E: Rows>(engine: &E, d: &Disown) -> Result<(Outcome, Vec<Write>), MetaError> {
    let mark = key::mark(&d.bucket, &d.key, d.file);
    let adopted = match engine.get(&mark)? {
        Some(value) => crate::record::Adopted::of(&value)?,
        None => None,
    };
    if adopted.is_none_or(|a| a.by != d.owner) {
        return Ok((Outcome::Disowned { released: false }, Vec::new()));
    }
    let (now, clock) = clock::tick(engine, d.at_ns)?;
    let mut writes = vec![clock];
    writes.extend(release(now, &d.bucket, &d.key, d.file)?);
    Ok((Outcome::Disowned { released: true }, writes))
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
    let now = clock::now(engine, a.at_ns)?;
    Ok((
        Outcome::Aborted,
        remove_upload(engine, &a.bucket, &a.key, &a.upload, &[], now)?,
    ))
}

/// Writes that remove an upload's row and every one of its parts, at most 10,000 (05 §4.1),
/// releasing at `now_ns` the files of the parts not `kept`.
fn remove_upload<E: Rows>(
    engine: &E,
    bucket: &str,
    key: &str,
    upload: &str,
    kept: &[u16],
    now_ns: u64,
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
        let Some((k, v)) = engine.next(&from, &to)? else {
            break;
        };
        let Some((_, _, NameRow::Part(_, number))) = key::decode_name(&k) else {
            return Err(MetaError::Corrupt);
        };
        // `kept` is in ascending order, as a completion's parts must be: a search, not a
        // scan, keeps a completion of 10,000 parts linear in them (audit P02).
        if kept.binary_search(&number).is_err() {
            writes.extend(release(now_ns, bucket, key, Part::decode(&v)?.file)?);
        }
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

/// Writes that remove the key's null version and its pointer, releasing its file at `now_ns`,
/// and the version removed.
fn remove_null<E: Rows>(
    engine: &E,
    bucket: &str,
    key: &str,
    now_ns: u64,
) -> Result<(Option<Version>, Vec<Write>), MetaError> {
    match version(engine, bucket, key, Named::Null)? {
        None => Ok((None, Vec::new())),
        Some((order, v)) => {
            let mut writes = vec![
                Write::Delete(key::name(bucket, key, &NameRow::Version(order))),
                Write::Delete(key::name(bucket, key, &NameRow::Null)),
            ];
            if let Some(file) = v.file {
                writes.extend(release(now_ns, bucket, key, file)?);
            }
            Ok((Some(v), writes))
        }
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
    /// The version after the one `version_id` names, `null` included (05 §7.1). An ID carries
    /// its version's place in the key's order, so the scan resumes after that place even when
    /// no version is there, as when it was deleted between pages: nothing is listed twice or
    /// skipped (audit B07; what S3 answers such a marker is not recorded, 05 §6.4). A `null`
    /// naming no version, or an ID that does not parse, carries no place and starts at the
    /// key's first.
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

/// The range's gate for `bucket`, read by a coordinator routed by `generation`.
pub fn read_gate<E: Rows>(
    engine: &E,
    bucket: &str,
    generation: u64,
) -> Result<Routed<Option<Gate>>, MetaError> {
    routed(engine, generation, || gate(engine, bucket))
}

/// Whether the range holds a version or delete marker of `bucket`, read by a coordinator
/// routed by `generation`: at most `budget` rows from `from`, a paused read's key, or from the
/// bucket's first row. A key's versions sort before its uploads, so one row answers for each
/// key.
pub fn probe<E: Rows>(
    engine: &E,
    bucket: &str,
    generation: u64,
    from: Option<&[u8]>,
    budget: usize,
) -> Result<Routed<Probe>, MetaError> {
    routed(engine, generation, || {
        probe_rows(engine, bucket, from, budget)
    })
}

/// `read`'s answer if the range is at `generation`, or its lineage.
fn routed<E: Rows, T>(
    engine: &E,
    generation: u64,
    read: impl FnOnce() -> Result<T, MetaError>,
) -> Result<Routed<T>, MetaError> {
    let lineage = lineage(engine)?;
    if lineage.standing != Standing::Serving || lineage.now.generation != generation {
        return Ok(Routed::Moved(Box::new(lineage)));
    }
    Ok(Routed::Here(read()?))
}

fn probe_rows<E: Rows>(
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

    /// A cell's first Name range, holding every key.
    fn seeded() -> Model {
        let mut m = Model::default();
        m.install(0, first(1).unwrap()).unwrap();
        m.persist().unwrap();
        m
    }

    struct Range {
        engine: Model,
        index: u64,
        clock: u64,
    }

    impl Range {
        /// A range holding bucket "b", incarnation 1, open.
        fn new() -> Self {
            let mut r = Self {
                engine: seeded(),
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
                generation: 1,
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
                deadline_ns: u64::MAX,
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
                deadline_ns: u64::MAX,
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
        // A version deleted between pages still marks its place: the scan goes on after it.
        let gone = seen[1].1.clone();
        let order = key::parse_version_id(&gone).unwrap();
        r.delete("a", Versioning::Enabled, Some(Named::Order(order)));
        let mut budget = 10;
        let after_gone = VersionFrom::After {
            key: "a",
            version_id: &gone,
        };
        let step = next_version(&r.engine, "b", after_gone, &end, &mut budget).unwrap();
        assert_eq!(found(step).1, "null");
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
                at_ns: 0,
                deadline_ns: u64::MAX,
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
                deadline_ns: u64::MAX,
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
                at_ns: 0,
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
                at_ns: 0,
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
            generation: 1,
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
                        deadline_ns: u64::MAX,
                    })
                }
            }))
            .collect();
        let mut whole = seeded();
        for (i, c) in commands.iter().enumerate() {
            apply(&mut whole, i as u64 + 1, c).unwrap();
        }
        let mut crashed = seeded();
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
            deadline_ns: u64::MAX,
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
            generation: 1,
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
            assert_eq!(
                r.part("k", &upload, number, 1, 10 + u128::from(number)),
                Outcome::PartWritten
            );
        }
        r.create("m");
        assert_eq!(r.gate_at(4, Some(Open), Some(Closed)), Outcome::GateMoved);
        assert_eq!(
            probe(&r.engine, "b", 1, None, 10).unwrap(),
            Routed::Here(Probe::Clear)
        );
        let collect = |budget| {
            Command::Collect(Collect {
                bucket: "b".into(),
                incarnation: 1,
                budget,
                at_ns: 0,
                generation: 1,
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
        // The parts' files are released for the collector to reclaim.
        let mut files: Vec<u128> = released(&r.engine, u64::MAX, 10)
            .unwrap()
            .into_iter()
            .map(|r| r.file)
            .collect();
        files.sort_unstable();
        assert_eq!(files, [11, 12, 13]);
        assert_eq!(r.gate_at(4, Some(Condemned), None), Outcome::GateMoved);
        assert_eq!(r.gate_at(4, Some(Condemned), None), Outcome::GateMoved);
        assert_eq!(gate(&r.engine, "b").unwrap(), None);
        // The cleanup resumed from its start after the drop: condemning and collecting are
        // done already. An attempt past the floor never had this gate to condemn.
        assert_eq!(
            r.gate_at(4, Some(Closed), Some(Condemned)),
            Outcome::GateMoved
        );
        assert_eq!(r.run(collect(2)), Outcome::Collected { done: true });
        assert_eq!(
            r.gate_at(5, Some(Closed), Some(Condemned)),
            Outcome::Conflict
        );
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
            at_ns: 0,
            generation: 1,
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
        assert_eq!(
            probe(&r.engine, "b", 1, None, 10).unwrap(),
            Routed::Here(Probe::Clear)
        );
        r.delete("d", Versioning::Enabled, None);
        let Routed::Here(Probe::Paused(at)) = probe(&r.engine, "b", 1, None, 2).unwrap() else {
            panic!("the budget ran out")
        };
        assert_eq!(
            probe(&r.engine, "b", 1, Some(&at), 1).unwrap(),
            Routed::Here(Probe::Paused(beyond_rows("b", "c")))
        );
        assert_eq!(
            probe(&r.engine, "b", 1, Some(&at), 2).unwrap(),
            Routed::Here(Probe::Found)
        );
        // A position before the bucket starts at its first row.
        assert_eq!(
            probe(&r.engine, "b", 1, Some(&[]), 4).unwrap(),
            Routed::Here(Probe::Found)
        );
        assert_eq!(
            probe(&r.engine, "c", 1, None, 4).unwrap(),
            Routed::Here(Probe::Clear)
        );
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
                deadline_ns: u64::MAX,
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

    /// One step of a history the release property drives.
    #[derive(Debug, Clone)]
    enum Step {
        Put {
            key: u8,
            versioning: u8,
            empty: bool,
        },
        Delete {
            key: u8,
            versioning: u8,
            named: Option<u8>,
        },
        Create {
            key: u8,
        },
        Part {
            key: u8,
            upload: u8,
            number: u16,
        },
        Complete {
            key: u8,
            upload: u8,
            parts: Vec<u16>,
            stale: bool,
        },
        Abort {
            key: u8,
            upload: u8,
        },
    }

    fn step() -> impl proptest::strategy::Strategy<Value = Step> {
        use proptest::prelude::*;
        prop_oneof![
            (0u8..3, 0u8..3, any::<bool>()).prop_map(|(key, versioning, empty)| Step::Put {
                key,
                versioning,
                empty
            }),
            (0u8..3, 0u8..3, proptest::option::of(0u8..8)).prop_map(|(key, versioning, named)| {
                Step::Delete {
                    key,
                    versioning,
                    named,
                }
            }),
            (0u8..3).prop_map(|key| Step::Create { key }),
            (0u8..3, 0u8..3, 1u16..4).prop_map(|(key, upload, number)| Step::Part {
                key,
                upload,
                number
            }),
            (
                0u8..3,
                0u8..3,
                proptest::collection::vec(1u16..4, 0..4),
                any::<bool>()
            )
                .prop_map(|(key, upload, parts, stale)| Step::Complete {
                    key,
                    upload,
                    parts,
                    stale
                }),
            (0u8..3, 0u8..3).prop_map(|(key, upload)| Step::Abort { key, upload }),
        ]
    }

    /// Every file of a version or a part in the range, and every released one.
    fn files_held(engine: &Model) -> (Vec<u128>, Vec<u128>, Vec<u128>) {
        let (mut versions, mut parts) = (Vec::new(), Vec::new());
        let (mut from, to) = key::bucket_span("b");
        while let Some((k, v)) = engine.next(&from, &to).unwrap() {
            match key::decode_name(&k).unwrap() {
                (_, _, NameRow::Version(_)) => versions.extend(Version::decode(&v).unwrap().file),
                (_, _, NameRow::Part(..)) => parts.push(Part::decode(&v).unwrap().file),
                _ => {}
            }
            from = after(&k);
        }
        let released = released(engine, u64::MAX, usize::MAX)
            .unwrap()
            .into_iter()
            .map(|r| r.file)
            .collect();
        (versions, parts, released)
    }

    proptest::proptest! {
        /// Every file a write hands the range is held in exactly one place after every step:
        /// by a version, by a part, as an extent of a completed object's file that is held or
        /// released, or in the released queue. No removal loses a file, and none is released
        /// while something still references it (docs/design/metadata.md §2).
        #[test]
        fn every_file_is_referenced_or_released_exactly_once(
            steps in proptest::collection::vec(step(), 1..60),
        ) {
            use std::collections::{BTreeMap, BTreeSet};
            let mut r = Range::new();
            let mut next_file = 1000u128;
            let mut carried: BTreeSet<u128> = BTreeSet::new();
            // A completed object's file and the part files it took as extents.
            let mut adopted: BTreeMap<u128, Vec<u128>> = BTreeMap::new();
            let mut uploads: BTreeMap<(u8, u8), String> = BTreeMap::new();
            let versionings = [Versioning::Enabled, Versioning::Suspended, Versioning::Unversioned];
            for s in steps {
                r.clock += 10;
                match s {
                    Step::Put { key, versioning, empty } => {
                        let file = (!empty).then(|| { next_file += 1; next_file });
                        carried.extend(file);
                        r.run(Command::Put(Put {
                            bucket: "b".into(),
                            incarnation: 1,
                            key: format!("k{key}"),
                            versioning: versionings[usize::from(versioning)],
                            preconditions: Preconditions::default(),
                            at_ns: r.clock,
                            ordered_ns: None,
                            version: Version { file, ..object("e") },
                            default: None,
                            deadline_ns: u64::MAX,
                        }));
                    }
                    Step::Delete { key, versioning, named } => {
                        let named = named.map(|n| match n {
                            0 => Named::Null,
                            n => {
                                let versions = r.versions(&format!("k{key}"));
                                versions
                                    .get(usize::from(n) % versions.len().max(1))
                                    .and_then(|(id, _, _)| key::parse_version_id(id))
                                    .map_or(Named::Order(u64::from(n)), Named::Order)
                            }
                        });
                        r.run(Command::Delete(Delete {
                            bucket: "b".into(),
                            incarnation: 1,
                            key: format!("k{key}"),
                            versioning: versionings[usize::from(versioning)],
                            named,
                            if_match: None,
                            at_ns: r.clock,
                            bypass: false,
                        }));
                    }
                    Step::Create { key } => {
                        let upload = r.create(&format!("k{key}"));
                        let slot = uploads.keys().filter(|(k, _)| *k == key).count();
                        uploads.insert((key, u8::try_from(slot % 3).unwrap()), upload);
                    }
                    Step::Part { key, upload, number } => {
                        let Some(id) = uploads.get(&(key, upload)).cloned() else { continue };
                        next_file += 1;
                        carried.insert(next_file);
                        r.run(Command::PutPart(PutPart {
                            bucket: "b".into(),
                            incarnation: 1,
                            key: format!("k{key}"),
                            upload: id,
                            number,
                            part: Part {
                                etag: format!("e{number}"),
                                size: MIN_PART,
                                checksum: None,
                                file: next_file,
                                modified_ns: 0,
                            },
                            at_ns: r.clock,
                            deadline_ns: u64::MAX,
                        }));
                    }
                    Step::Complete { key, upload, mut parts, stale } => {
                        let Some(id) = uploads.get(&(key, upload)).cloned() else { continue };
                        parts.sort_unstable();
                        parts.dedup();
                        let held = super::parts(&r.engine, "b", &format!("k{key}"), &id, 0, 10).unwrap();
                        let listed: Vec<Listed> = parts
                            .iter()
                            .map(|&number| Listed {
                                number,
                                etag: format!("e{number}"),
                                file: held
                                    .iter()
                                    .find(|(n, _)| *n == number)
                                    .map_or(0, |(_, p)| p.file + u128::from(stale)),
                            })
                            .collect();
                        next_file += 1;
                        let file = next_file;
                        carried.insert(file);
                        let outcome = r.run(Command::Complete(Complete {
                            bucket: "b".into(),
                            incarnation: 1,
                            key: format!("k{key}"),
                            upload: id,
                            versioning: Versioning::Enabled,
                            preconditions: Preconditions::default(),
                            at_ns: r.clock,
                            parts: listed.clone(),
                            etag: "whole".into(),
                            size: 1,
                            checksum: None,
                            file: Some(file),
                            default: None,
                            deadline_ns: u64::MAX,
                        }));
                        if matches!(outcome, Outcome::Put { .. })
                            && versions_hold(&r.engine, file)
                        {
                            adopted.insert(file, listed.iter().map(|l| l.file).collect());
                        }
                    }
                    Step::Abort { key, upload } => {
                        let Some(id) = uploads.get(&(key, upload)).cloned() else { continue };
                        r.run(Command::Abort(Abort {
                            bucket: "b".into(),
                            incarnation: 1,
                            key: format!("k{key}"),
                            upload: id,
                            at_ns: r.clock,
                        }));
                    }
                }
                let (versions, part_files, released) = files_held(&r.engine);
                let children: Vec<u128> = adopted
                    .iter()
                    .filter(|(root, _)| versions.contains(root) || released.contains(root))
                    .flat_map(|(_, children)| children.iter().copied())
                    .collect();
                for file in &carried {
                    let places = [&versions, &part_files, &released, &children]
                        .iter()
                        .map(|held| held.iter().filter(|f| *f == file).count())
                        .sum::<usize>();
                    proptest::prop_assert_eq!(places, 1, "file {} after {:?}", file, r.versions("k0"));
                }
            }
        }
    }

    fn versions_hold(engine: &Model, file: u128) -> bool {
        files_held(engine).0.contains(&file)
    }

    /// The collector reads the released queue oldest first, up to a time, and a reclaimed
    /// file's row goes.
    #[test]
    fn released_files_are_read_oldest_first_and_reclaimed() {
        let mut r = Range::new();
        for (key, at) in [("a", 30u64), ("b", 10), ("c", 20)] {
            r.clock = at * MS;
            r.put(key, "e", Versioning::Unversioned);
        }
        for (key, at) in [("a", 300u64), ("b", 100), ("c", 200)] {
            r.clock = at * MS;
            r.delete(key, Versioning::Unversioned, None);
        }
        let queue = released(&r.engine, u64::MAX, 10).unwrap();
        let times: Vec<u64> = queue.iter().map(|r| r.released_ns / MS).collect();
        assert_eq!(times, [100, 200, 300]);
        // Each row names the key its file was held under.
        let keys: Vec<&str> = queue.iter().map(|r| r.holder.key.as_str()).collect();
        assert_eq!(keys, ["b", "c", "a"]);
        assert_eq!(released(&r.engine, 200 * MS, 10).unwrap().len(), 1);
        assert_eq!(released(&r.engine, u64::MAX, 2).unwrap(), queue[..2]);
        let (released_ns, file) = (queue[0].released_ns, queue[0].file);
        assert_eq!(
            r.run(Command::Reclaim(Reclaim { released_ns, file })),
            Outcome::Reclaimed
        );
        assert_eq!(released(&r.engine, u64::MAX, 10).unwrap(), queue[1..]);
    }

    /// A handover past its file's deadline is refused, and its file released and marked. The
    /// sweep's check then finds every file the range took held, releases and marks a file never
    /// handed over once its deadline has passed, and leaves one alone until then.
    #[test]
    fn a_late_handover_is_refused_and_the_sweep_releases_what_was_never_handed_over() {
        use Verdict::{Held, Released, Young};
        let mut r = Range::new();
        let put = |r: &mut Range, key: &str, file: u128, deadline_ns: u64| {
            r.clock += 10;
            r.run(Command::Put(Put {
                bucket: "b".into(),
                incarnation: 1,
                key: key.into(),
                versioning: Versioning::Unversioned,
                preconditions: Preconditions::default(),
                at_ns: r.clock,
                ordered_ns: None,
                version: Version {
                    file: Some(file),
                    ..object("e")
                },
                default: None,
                deadline_ns,
            }))
        };
        let marked = |r: &Range, k: &str, file| {
            let mark = key::mark("b", k, file);
            r.engine.get(&mark).unwrap().is_some()
        };
        let queue = |r: &Range| -> Vec<u128> {
            released(&r.engine, u64::MAX, 10)
                .unwrap()
                .into_iter()
                .map(|r| r.file)
                .collect()
        };
        let asked = |k: &str, file, deadline_ns| Checked {
            bucket: "b".into(),
            key: k.into(),
            file,
            deadline_ns,
        };
        let in_time = r.clock + 10;
        assert!(matches!(put(&mut r, "a", 5, in_time), Outcome::Put { .. }));
        // A handover past the deadline may come after the sweep settled the file, so it
        // releases the file unmarked.
        assert_eq!(put(&mut r, "b", 6, in_time), Outcome::Expired);
        assert_eq!(queue(&r), [6]);
        assert!(marked(&r, "a", 5) && !marked(&r, "b", 6));
        // 5 was taken; 6 was released when it came late, which the sweep cannot tell, so it
        // releases it again, which the reclaimer finds gone; 7 was never handed over and is
        // past its deadline; 8 was never handed over and is not yet.
        r.clock += 10;
        let check = |r: &mut Range, files: Vec<Checked>| {
            let at_ns = r.clock;
            r.run(Command::Check(Check { files, at_ns }))
        };
        assert_eq!(
            check(
                &mut r,
                vec![
                    asked("a", 5, 0),
                    asked("b", 6, 0),
                    asked("c", 7, 0),
                    asked("d", 8, u64::MAX)
                ]
            ),
            Outcome::Checked(vec![Held, Released, Released, Young])
        );
        assert_eq!(queue(&r), [6, 6, 7]);
        // A sweep that stopped before settling asks again, and 7 is not released twice.
        assert_eq!(
            check(&mut r, vec![asked("c", 7, 0)]),
            Outcome::Checked(vec![Held])
        );
        // A handover of 7 after its release finds its deadline passed.
        assert_eq!(put(&mut r, "c", 7, 0), Outcome::Expired);
        // Once settled, the marks go; a mark left behind goes when its file is released.
        let marks = |files: &[(&str, u128)]| {
            Command::Unmark(Unmark {
                files: files
                    .iter()
                    .map(|&(k, file)| Marked {
                        bucket: "b".into(),
                        key: k.into(),
                        file,
                    })
                    .collect(),
            })
        };
        assert_eq!(
            r.run(marks(&[("b", 6), ("c", 7), ("d", 8)])),
            Outcome::Unmarked
        );
        assert!(!marked(&r, "b", 6) && !marked(&r, "c", 7) && marked(&r, "a", 5));
        assert!(matches!(
            r.delete("a", Versioning::Unversioned, None),
            Outcome::Deleted { .. }
        ));
        assert!(!marked(&r, "a", 5));
        // A sweep that released 9 and stopped before unmarking it: the reclaimer removes the
        // mark, routed by the key the queue row names, before the row.
        assert_eq!(
            check(&mut r, vec![asked("e", 9, 0)]),
            Outcome::Checked(vec![Released])
        );
        assert!(marked(&r, "e", 9));
        let row = released(&r.engine, u64::MAX, 100)
            .unwrap()
            .into_iter()
            .find(|r| r.file == 9)
            .unwrap();
        assert_eq!(row.holder.key, "e");
        assert_eq!(r.run(marks(&[("e", 9)])), Outcome::Unmarked);
        assert!(!marked(&r, "e", 9));
    }

    /// A leader whose clock runs behind proposes a handover at a time earlier than the check
    /// that released its file. The check recorded its time, so the range reads the later one and
    /// refuses the handover; judged at the proposal's time, the handover would take a file the
    /// collector is to reclaim.
    #[test]
    fn a_handover_proposed_behind_the_check_that_released_its_file_is_refused() {
        let mut r = Range::new();
        let deadline_ns = r.clock + 50;
        let released_at = deadline_ns + 1;
        let check = Command::Check(Check {
            files: vec![Checked {
                bucket: "b".into(),
                key: "a".into(),
                file: 4,
                deadline_ns,
            }],
            at_ns: released_at,
        });
        assert_eq!(r.run(check), Outcome::Checked(vec![Verdict::Released]));
        let put = Command::Put(Put {
            bucket: "b".into(),
            incarnation: 1,
            key: "a".into(),
            versioning: Versioning::Unversioned,
            preconditions: Preconditions::default(),
            at_ns: deadline_ns - 10,
            ordered_ns: None,
            version: Version {
                file: Some(4),
                ..object("e")
            },
            default: None,
            deadline_ns,
        });
        assert_eq!(r.run(put), Outcome::Expired);
        assert_eq!(current(&r.engine, "b", "a").unwrap(), None);
    }

    impl Range {
        /// Splits the range at `at` into itself and a child with ID `id`, made as a replica
        /// makes it: the rows `child` names, and the parent's rows in its spans as they stand.
        fn split(&mut self, at: Vec<u8>, id: u64) -> (Outcome, Option<Range>) {
            let generation = lineage(&self.engine).unwrap().now.generation;
            let s = Split {
                generation,
                at,
                child: id,
            };
            let made = child(&self.engine, &s).unwrap().map(|c| {
                let mut rows = c.rows;
                for (k, v) in self.engine.image().unwrap() {
                    if c.spans.iter().any(|(from, to)| *from <= k && k < *to) {
                        rows.push((k, v));
                    }
                }
                let mut engine = Model::default();
                engine.install(0, rows).unwrap();
                Range {
                    engine,
                    index: 0,
                    clock: self.clock,
                }
            });
            (self.run(Command::Split(s)), made)
        }

        fn rows(&self) -> Vec<Vec<u8>> {
            self.engine
                .image()
                .unwrap()
                .into_iter()
                .map(|(k, _)| k)
                .collect()
        }
    }

    fn descriptor(id: u64, lo: Option<&str>, hi: Option<&str>, generation: u64) -> Descriptor {
        Descriptor {
            id,
            lo: lo.map(|k| key::route("b", k)).unwrap_or_default(),
            hi: hi.map(|k| key::route("b", k)),
            generation,
        }
    }

    /// A split leaves the range the keys before its point and gives the child the rest, with
    /// their rows and marks, the gates of the buckets it can hold, the floor and the clock.
    /// Each side refuses what is the other's with its lineage, taking nothing.
    #[test]
    fn a_split_divides_the_keys_and_each_side_refuses_the_others() {
        use GateState::{Closed, Condemned, Open};
        let mut r = Range::new();
        r.put("a", "1", Versioning::Enabled);
        r.put("m", "2", Versioning::Enabled);
        let upload = r.create("t");
        r.part("t", &upload, 1, 1, 9);
        // Bucket "c", after "b", comes and goes, raising the floor to its attempt.
        let other = |from, to| {
            Command::Gate(GateChange {
                bucket: "c".into(),
                incarnation: 5,
                attempt: 5,
                from,
                to,
                generation: 1,
            })
        };
        for (from, to) in [
            (None, Some(Open)),
            (Some(Open), Some(Closed)),
            (Some(Closed), Some(Condemned)),
            (Some(Condemned), None),
        ] {
            assert_eq!(r.run(other(from, to)), Outcome::GateMoved);
        }
        let (outcome, made) = r.split(key::route("b", "m"), 2);
        let parent = Lineage {
            now: descriptor(1, None, Some("m"), 2),
            child: Some(descriptor(2, Some("m"), None, 2)),
            standing: Standing::Serving,
            into: None,
            taken: None,
        };
        assert_eq!(outcome, Outcome::Split(Box::new(parent.clone())));
        let mut c = made.unwrap();
        assert_eq!(lineage(&r.engine).unwrap(), parent);
        assert_eq!(
            lineage(&c.engine).unwrap(),
            serving(descriptor(2, Some("m"), None, 2))
        );
        // Every row and mark of a key is on the side that holds the key; the bucket both hold
        // keys of has its gate on both sides; the floor and the clock went with the child.
        let routes = |r: &Range| -> Vec<(String, String)> {
            r.rows()
                .iter()
                .filter_map(|k| {
                    let (bucket, key, _) = key::decode_name(k)
                        .or_else(|| key::decode_mark(k).map(|(b, k, _)| (b, k, NameRow::Null)))?;
                    Some((bucket, key))
                })
                .collect()
        };
        assert!(routes(&r).iter().all(|(_, k)| k == "a"));
        assert!(routes(&c).iter().any(|(_, k)| k == "m"));
        assert!(routes(&c).iter().any(|(_, k)| k == "t"));
        assert!(routes(&c).iter().all(|(_, k)| k != "a"));
        assert!(r.engine.get(&key::mark("b", "a", 1)).unwrap().is_some());
        assert!(c.engine.get(&key::mark("b", "m", 1)).unwrap().is_some());
        assert_eq!(gate(&r.engine, "b").unwrap(), gate(&c.engine, "b").unwrap());
        assert_eq!(floor(&c.engine).unwrap(), 5);
        assert_eq!(
            clock::now(&c.engine, 0).unwrap(),
            clock::now(&r.engine, 0).unwrap()
        );
        // The queue of released files stays with the parent.
        assert!(released(&c.engine, u64::MAX, 8).unwrap().is_empty());
        // Refused, a write carrying a file takes nothing: no version, release or mark.
        let queued = released(&r.engine, u64::MAX, 8).unwrap().len();
        let moved = |r: &Range| Outcome::Moved(Box::new(lineage(&r.engine).unwrap()));
        assert_eq!(r.put("t", "3", Versioning::Enabled), moved(&r));
        assert_eq!(released(&r.engine, u64::MAX, 8).unwrap().len(), queued);
        assert!(r.engine.get(&key::mark("b", "t", 1)).unwrap().is_none());
        assert_eq!(c.put("a", "3", Versioning::Enabled), moved(&c));
        assert!(matches!(
            c.put("z", "3", Versioning::Enabled),
            Outcome::Put { .. }
        ));
        // The sweep's commands name keys, and go where the keys are.
        let check = Command::Check(Check {
            files: vec![Checked {
                bucket: "b".into(),
                key: "t".into(),
                file: 9,
                deadline_ns: 0,
            }],
            at_ns: 0,
        });
        assert_eq!(r.run(check.clone()), moved(&r));
        assert_eq!(c.run(check), Outcome::Checked(vec![Verdict::Held]));
        // A coordinator's reads and steps routed by the old generation are told where the span
        // went, and those routed by the new one are answered.
        assert_eq!(
            read_gate(&r.engine, "b", 1).unwrap(),
            Routed::Moved(Box::new(parent))
        );
        assert!(matches!(
            read_gate(&c.engine, "b", 2).unwrap(),
            Routed::Here(Some(Gate { state: Open, .. }))
        ));
        assert!(matches!(
            probe(&c.engine, "b", 1, None, 8).unwrap(),
            Routed::Moved(_)
        ));
        assert_eq!(r.gate(Some(Open), Some(Closed)), moved(&r));
        let collect = |generation| {
            Command::Collect(Collect {
                bucket: "b".into(),
                incarnation: 1,
                budget: 8,
                at_ns: 0,
                generation,
            })
        };
        assert_eq!(c.run(collect(1)), moved(&c));
        assert_eq!(c.run(collect(2)), Outcome::Conflict, "not condemned");
    }

    /// The clock goes with the child. A check that released a file never handed over recorded
    /// its time; a handover a lagging leader proposes behind that time, reaching the child that
    /// holds the key now, is refused as the parent would have refused it. A child starting its
    /// clock afresh would take a file the collector is to reclaim.
    #[test]
    fn a_split_carries_the_time_a_check_recorded() {
        let mut r = Range::new();
        let deadline_ns = r.clock + 50;
        let check = Command::Check(Check {
            files: vec![Checked {
                bucket: "b".into(),
                key: "t".into(),
                file: 4,
                deadline_ns,
            }],
            at_ns: deadline_ns + 1,
        });
        assert_eq!(r.run(check), Outcome::Checked(vec![Verdict::Released]));
        let (_, made) = r.split(key::route("b", "m"), 2);
        let mut c = made.unwrap();
        let put = Command::Put(Put {
            bucket: "b".into(),
            incarnation: 1,
            key: "t".into(),
            versioning: Versioning::Unversioned,
            preconditions: Preconditions::default(),
            at_ns: deadline_ns - 10,
            ordered_ns: None,
            version: Version {
                file: Some(4),
                ..object("e")
            },
            default: None,
            deadline_ns,
        });
        assert_eq!(c.run(put), Outcome::Expired);
        assert_eq!(current(&c.engine, "b", "t").unwrap(), None);
    }

    /// A merge across the two ranges' logs: the higher range freezes and takes nothing but the
    /// merge's own steps; the lower range takes its rows, marks, queue and the gates it holds
    /// none of, the later clock and the higher floor; the frozen range ends, pointing at the
    /// lower; and the lower lets go of the merge.
    #[test]
    fn a_merge_joins_the_higher_range_to_the_lower() {
        use GateState::Open;
        let mut r = Range::new();
        r.put("a", "1", Versioning::Enabled);
        r.put("t", "2", Versioning::Enabled);
        let (_, made) = r.split(key::route("b", "m"), 2);
        let mut c = made.unwrap();
        // The child alone holds bucket "c", and releases a file there.
        c.run(Command::Gate(GateChange {
            bucket: "c".into(),
            incarnation: 3,
            attempt: 3,
            from: None,
            to: Some(Open),
            generation: 2,
        }));
        c.clock = r.clock + 1_000;
        let put_c = |c: &mut Range, key: &str| {
            c.clock += 10;
            c.run(Command::Put(Put {
                bucket: "c".into(),
                incarnation: 3,
                key: key.into(),
                versioning: Versioning::Unversioned,
                preconditions: Preconditions::default(),
                at_ns: c.clock,
                ordered_ns: None,
                version: Version {
                    file: Some(7),
                    ..object("e")
                },
                default: None,
                deadline_ns: u64::MAX,
            }))
        };
        put_c(&mut c, "k");
        put_c(&mut c, "k");
        assert_eq!(released(&c.engine, u64::MAX, 8).unwrap().len(), 1);
        let lower = lineage(&r.engine).unwrap().now;
        let upper = lineage(&c.engine).unwrap().now;
        let freeze = Command::Freeze(Freeze {
            generation: upper.generation,
            into: lower.clone(),
        });
        let Outcome::Frozen(frozen) = c.run(freeze) else {
            panic!("frozen");
        };
        // Frozen, it takes nothing: not a write, a coordinator's read, or a split.
        let moved = |r: &Range| Outcome::Moved(Box::new(lineage(&r.engine).unwrap()));
        assert_eq!(c.put("t", "3", Versioning::Enabled), moved(&c));
        assert!(matches!(
            read_gate(&c.engine, "b", frozen.now.generation).unwrap(),
            Routed::Moved(_)
        ));
        assert_eq!(c.split(key::route("b", "x"), 9).0, moved(&c));
        assert_eq!(
            c.run(Command::Thaw(Thaw {
                generation: lower.generation + 1
            })),
            Outcome::Conflict,
            "a thaw for another merge"
        );
        let m = Merge {
            generation: lower.generation,
            from: frozen.now.clone(),
            max_rows: 64,
        };
        // Only `merge`, with the frozen range's rows, takes it.
        assert_eq!(r.run(Command::Merge(m.clone())), Outcome::Invalid);
        r.index += 1;
        let Outcome::Merged(merged) = merge(&mut r.engine, r.index, &m, &c.engine).unwrap() else {
            panic!("merged");
        };
        assert_eq!(
            (merged.now.lo.clone(), merged.now.hi.clone()),
            (Vec::new(), None)
        );
        assert!(merged.now.generation > lower.generation.max(frozen.now.generation));
        assert_eq!(
            merged.taken,
            Some(Taken {
                from: 2,
                generation: lower.generation
            })
        );
        // Everything of the child is in the lower range now.
        assert!(current(&r.engine, "b", "t").unwrap().is_some());
        assert!(current(&r.engine, "c", "k").unwrap().is_some());
        assert!(r.engine.get(&key::mark("b", "t", 1)).unwrap().is_some());
        assert!(gate(&r.engine, "c").unwrap().is_some());
        assert_eq!(released(&r.engine, u64::MAX, 8).unwrap().len(), 1);
        assert!(clock::now(&r.engine, 0).unwrap() >= clock::now(&c.engine, 0).unwrap());
        // A command for the merge that comes again is routed by a generation the range no
        // longer has, and changes nothing.
        r.index += 1;
        assert_eq!(
            merge(&mut r.engine, r.index, &m, &c.engine).unwrap(),
            moved(&r)
        );
        // Holding the merge, the range may not be frozen for another.
        let hold = Command::Freeze(Freeze {
            generation: merged.now.generation,
            into: descriptor(0, None, Some(""), 1),
        });
        assert_eq!(r.run(hold), Outcome::Conflict);
        // The frozen range ends, and answers every request with where its span went; the end
        // repeated is answered the same.
        let end = Command::End(End {
            generation: lower.generation,
            into: merged.now.clone(),
        });
        assert_eq!(c.run(end.clone()), Outcome::Ended);
        assert_eq!(c.run(end), Outcome::Ended);
        let Outcome::Moved(gone) = c.put("t", "3", Versioning::Enabled) else {
            panic!("moved");
        };
        assert_eq!(gone.standing, Standing::Ended);
        assert_eq!(gone.into, Some(merged.now.clone()));
        let resolve = Command::Resolve(Resolve {
            from: 2,
            generation: lower.generation,
        });
        assert_eq!(r.run(resolve.clone()), Outcome::Resolved);
        assert_eq!(lineage(&r.engine).unwrap().taken, None);
        assert_eq!(r.run(resolve), Outcome::Resolved);
        assert!(matches!(
            r.put("t", "3", Versioning::Enabled),
            Outcome::Put { .. }
        ));
    }

    /// A merge keeps the later clock. A check in the frozen range released a file never handed
    /// over and recorded its time; the lower range's clock is behind it. A handover a lagging
    /// leader proposes behind the check, reaching the range that took the key, is refused.
    #[test]
    fn a_merge_keeps_the_time_a_check_recorded() {
        let mut r = Range::new();
        let (_, made) = r.split(key::route("b", "m"), 2);
        let mut c = made.unwrap();
        let deadline_ns = r.clock + 50;
        let check = Command::Check(Check {
            files: vec![Checked {
                bucket: "b".into(),
                key: "t".into(),
                file: 4,
                deadline_ns,
            }],
            at_ns: deadline_ns + 1_000,
        });
        assert_eq!(c.run(check), Outcome::Checked(vec![Verdict::Released]));
        let lower = lineage(&r.engine).unwrap().now;
        let generation = lineage(&c.engine).unwrap().now.generation;
        let Outcome::Frozen(frozen) = c.run(Command::Freeze(Freeze {
            generation,
            into: lower.clone(),
        })) else {
            panic!("frozen");
        };
        let m = Merge {
            generation: lower.generation,
            from: frozen.now,
            max_rows: 64,
        };
        r.index += 1;
        assert!(matches!(
            merge(&mut r.engine, r.index, &m, &c.engine).unwrap(),
            Outcome::Merged(_)
        ));
        let put = Command::Put(Put {
            bucket: "b".into(),
            incarnation: 1,
            key: "t".into(),
            versioning: Versioning::Unversioned,
            preconditions: Preconditions::default(),
            at_ns: deadline_ns - 10,
            ordered_ns: None,
            version: Version {
                file: Some(4),
                ..object("e")
            },
            default: None,
            deadline_ns,
        });
        assert_eq!(r.run(put), Outcome::Expired);
    }

    /// The lower range decides a merge once. A refusal, or an abandon, moves its generation
    /// on, so the frozen range may thaw: a command for the merge that comes after finds the
    /// range at another generation and changes nothing.
    #[test]
    fn a_refused_merge_is_never_taken_after() {
        let mut r = Range::new();
        let (_, made) = r.split(key::route("b", "m"), 2);
        let mut c = made.unwrap();
        let lower = lineage(&r.engine).unwrap().now;
        let freeze = |c: &mut Range, into: &Descriptor| {
            let generation = lineage(&c.engine).unwrap().now.generation;
            let Outcome::Frozen(frozen) = c.run(Command::Freeze(Freeze {
                generation,
                into: into.clone(),
            })) else {
                panic!("frozen");
            };
            frozen.now
        };
        let from = freeze(&mut c, &lower);
        let abandon = Command::Abandon(Abandon {
            generation: lower.generation,
        });
        let Outcome::Refused(refused) = r.run(abandon) else {
            panic!("refused");
        };
        assert!(refused.now.generation > lower.generation && refused.taken.is_none());
        let m = Merge {
            generation: lower.generation,
            from,
            max_rows: 64,
        };
        let thaw = Command::Thaw(Thaw {
            generation: lower.generation,
        });
        assert!(matches!(c.run(thaw), Outcome::Thawed(_)));
        // The merge's decision, arriving late, finds the range moved on and takes nothing.
        r.index += 1;
        assert!(matches!(
            merge(&mut r.engine, r.index, &m, &c.engine).unwrap(),
            Outcome::Moved(_)
        ));
        assert_eq!(
            lineage(&r.engine).unwrap().now.hi,
            Some(key::route("b", "m"))
        );
        // A merge whose frozen range holds more rows than it may take is refused.
        c.put("t", "1", Versioning::Enabled);
        let lower = lineage(&r.engine).unwrap().now;
        let from = freeze(&mut c, &lower);
        let m = Merge {
            generation: lower.generation,
            from,
            max_rows: 1,
        };
        r.index += 1;
        assert!(matches!(
            merge(&mut r.engine, r.index, &m, &c.engine).unwrap(),
            Outcome::Refused(_)
        ));
        // A replica whose copy of the frozen range is not frozen for the merge stops rather
        // than take other rows than its peers.
        let lower = lineage(&r.engine).unwrap().now;
        let m = Merge {
            generation: lower.generation,
            from: lineage(&c.engine).unwrap().now,
            max_rows: 64,
        };
        r.index += 1;
        assert_eq!(
            merge(&mut r.engine, r.index, &m, &c.engine),
            Err(MetaError::Unfrozen)
        );
    }

    /// A split at a bucket's first routing key leaves the range none of its keys, and the
    /// range gives up the bucket's gate; a split the range cannot take is refused.
    #[test]
    fn a_split_at_a_buckets_start_moves_its_gate_whole() {
        let mut r = Range::new();
        let (outcome, made) = r.split(key::bucket_routes("b").0, 2);
        assert!(matches!(outcome, Outcome::Split(_)));
        let c = made.unwrap();
        assert_eq!(gate(&r.engine, "b").unwrap(), None);
        assert!(gate(&c.engine, "b").unwrap().is_some());
        // A range may hold a gate only for a bucket whose keys its span can hold.
        let open = Command::Gate(GateChange {
            bucket: "b".into(),
            incarnation: 1,
            attempt: 1,
            from: None,
            to: Some(GateState::Open),
            generation: 2,
        });
        assert_eq!(r.run(open), Outcome::Invalid);
        let refused = |r: &mut Range, at: Vec<u8>, id: u64, generation: u64| {
            let s = Split {
                generation,
                at,
                child: id,
            };
            assert_eq!(child(&r.engine, &s).unwrap(), None);
            r.run(Command::Split(s))
        };
        let lo = lineage(&r.engine).unwrap().now.hi.unwrap();
        // At or past the span's end, at its start, not a key's route, or the range's own ID.
        assert_eq!(refused(&mut r, lo.clone(), 3, 2), Outcome::Invalid);
        assert_eq!(refused(&mut r, Vec::new(), 3, 2), Outcome::Invalid);
        assert_eq!(refused(&mut r, vec![b'a'], 3, 2), Outcome::Invalid);
        assert_eq!(
            refused(&mut r, key::route("a", "k"), 1, 2),
            Outcome::Invalid
        );
        assert!(matches!(
            refused(&mut r, key::route("a", "k"), 3, 1),
            Outcome::Moved(_)
        ));
    }

    /// Every range's engine also holds the rows its replica keeps about itself: the group's
    /// configuration, which once began with the queue's marker and read as a corrupt queue
    /// row, and the last snapshot installed.
    #[test]
    fn the_queue_reads_past_the_replicas_own_rows() {
        let mut r = Range::new();
        let own: Vec<Write> = [key::marker::CONFIGURATION, key::marker::INSTALLED]
            .iter()
            .map(|&m| Write::Put(vec![key::LOCAL, m], vec![1, 2, 3]))
            .collect();
        r.index += 1;
        r.engine.apply(r.index, &own).unwrap();
        r.clock = 10 * MS;
        r.put("a", "e", Versioning::Unversioned);
        r.clock = 20 * MS;
        r.delete("a", Versioning::Unversioned, None);
        let queue = released(&r.engine, u64::MAX, 10).unwrap();
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].released_ns / MS, 20);
    }
}
