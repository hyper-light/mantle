//! The gateway host (docs/design/node.md §4.2, §5): for each object exchange a client or gateway
//! asks of the node (`mantle_protocol::object`), the node runs the bucket's coordinator
//! (`mantle_meta::coordinator`), the gateway's drivers (`mantle_gateway::put`, `get`) or the
//! ranges' commands and reads an operation needs, and serves each one's requests itself: a
//! placement from its own replica of the volume table (`crate::place`), a chunk on its own volume
//! in-process or on a peer's over the transfer class, and a range's command or read of the range's
//! leader (`Node::command`, `Node::read`).
//!
//! An operation is typed (`object::Op`) and answers with a typed reply (`object::Reply`): the
//! native protocol's encoding is taken where a request's fields are read and given where its
//! reply's head and fields are written, so another door hands the host the same operation. Its
//! preconditions and range are judged by `mantle_s3::conditional` and `mantle_s3::range`, a
//! write's at the Name range's commit (05 §2).
//!
//! A put's body is read from its exchange only as fast as the driver takes it, a piece at a time,
//! and a get's bytes are taken from its driver only as fast as the exchange takes them, so what an
//! operation holds is its driver's window of blocks and one piece. The operations the host runs at
//! once are bounded (`GatewaySettings::objects`); one past them is answered `SlowDown`.

use std::collections::VecDeque;

use bytes::Bytes;
use hyper_rt::compute;
use hyper_rt::slow::Label;
use hyper_rt::sync::{ChannelReceiver, OneshotReceiver, Sender, SyncError};
use hyper_transport::{ExchangeId, PeerId, Refusal};
use mantle_ec::durability::{self, Choice, Rates};
use mantle_gateway::complete::{self, CompleteError, Completion};
use mantle_gateway::get::{self, Get, GetError, Keyring};
use mantle_gateway::layout::Layout;
use mantle_gateway::put::{self, Body, Commit, Expected, Holding, Keys, Put, PutError, Random};
use mantle_meta::apply::Layer;
use mantle_meta::coordinator::{self, Coordinator, Settled};
use mantle_meta::key::{NULL_VERSION, parse_version_id};
use mantle_meta::name::{self, Match, Named, Preconditions};
use mantle_meta::principal;
use mantle_meta::record::{
    self, Bucket, BucketState, Descriptor, How, LISTING, Part, RunMd5, Runs, Upload, Version,
    Versioning, WrappedKey, Wrapper,
};
use mantle_meta::root::{self, Placed};
use mantle_meta::wire::{Answer, Command};
use mantle_meta::{bucket, engine::Engine};
use mantle_s3::checksum::{self, Algorithm};
use mantle_s3::conditional::{self, Current};
use mantle_s3::copy::Directive;
use mantle_s3::range::{self, CopyRangeError, Span};
use mantle_s3::seal::{DataKey, WrappingKey};

use super::{ChunkAsk, ChunkOutcome, Node, NodeError, NodeLog, PIECE, shard, take_done};
use crate::ask::{CommandAsk, CommandOutcome, ReadAsk, ReadOutcome, Wait};
use crate::chunk;
use crate::door::{self, Piece};
use crate::labels;
use crate::place;
use crate::read::{Query, Read};
use mantle_protocol::bounds;
use mantle_protocol::classes::Class;
use mantle_protocol::object::{
    self, Deleted, Head, Headers, Issued, List, Listing, MAX_PAGE_PARTS, Metadata, ObjectError, Op,
    PartListed, Parts, Principals, Progress, Reply, Status, Stored, Target,
};

/// What the gateway host is given (docs/design/node.md §4.6, §5).
pub struct GatewaySettings {
    /// The root key's generations, the last the current one, which new files' data keys are
    /// wrapped under (encryption.md §2): a file is opened under the generation its header names.
    pub keys: Vec<(u32, WrappingKey)>,
    /// How long a block or file is held for the write above it (gateway.md §2).
    pub handover_ns: u64,
    /// The blocks a put holds going down, and a get holds being read, at once (gateway.md §2–§3).
    pub window: usize,
    /// The most object operations the host runs at once, as many as its operations' bytes hold
    /// at the least one holds: within it the host runs as many as it has learned its knee is
    /// (`crate::admit`), and one past them is answered `SlowDown`.
    pub objects: usize,
    /// The bytes the host's operations hold at once, each charged what it holds at its own size
    /// (`charges`): one whose bytes would pass them waits, or is answered `SlowDown`.
    pub objects_bytes: u64,
    pub charges: crate::derive::Charges,
    /// The clients of the native protocol it admits at once, each by the certificate the cell's
    /// authority issued it for the client role (node.md §3.6). One past them is refused as the
    /// transport refuses an identity past its bound (`Refusal::Identities`), which the client asks
    /// again after.
    pub clients: usize,
    /// The buckets an owner may have (S3's 10,000, research/05 §10.4).
    pub bucket_quota: u32,
    /// The rows a bucket's coordinator passes in one read or collection (`coordinator::Bounds`).
    pub scan_rows: u32,
    /// The durability a block's scheme meets: the chunk loss and repair rates the model takes,
    /// and the annual loss probability allowed (durability.md; `mantle_ec::durability::choose`).
    pub rates: Rates,
    pub target: f64,
}

/// A native object request parked until the host has room for it (`Node::take_parked`), its body
/// unread: its exchange and asker, its head as decoded, what it will be charged, its client's
/// deadline, and when it is answered `SlowDown` if still parked.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Parked {
    exchange: ExchangeId,
    peer: PeerId,
    head: Option<Head>,
    charge: u64,
    window: usize,
    deadline_ns: Option<u64>,
    until_ns: u64,
}

/// What the host waits on for an operation: a command, a read, or a chunk it asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Pending {
    Command(CommandAsk),
    Read(ReadAsk),
    Chunk(ChunkAsk),
}

/// An object operation the host runs: the door it came in by, its asker, and where it is.
pub(crate) struct Job {
    door: Door,
    /// Who asks: a native asker's peer id, or the account an S3 request's access key acts for.
    principal: String,
    work: Work,
    /// What the operation waits on, each with the driver's request it answers, if a driver's.
    pending: Vec<(Pending, Option<u64>)>,
    /// When, on the shard's clock, its client stops waiting, where the client said (research/77
    /// D2).
    deadline_ns: Option<u64>,
    /// When its work last moved: begun, an ask of its own answered, a piece of a chunk it reads
    /// or sends.
    moved_ns: u64,
    /// When the host admitted it, on the shard's clock; none for one answered at once, refused or
    /// malformed, which the admission limit does not count (`crate::admit`).
    admitted_ns: Option<u64>,
    /// The bytes it was charged as it was admitted, what it holds at its own size
    /// (`derive::Charges`); none for one not admitted.
    charge: u64,
    /// The blocks a put of it holds going down at once, as the node learned them when it was
    /// admitted (`crate::window`) and charged for them.
    window: usize,
    /// Its operation's label (`crate::labels::host`), once its operation is known: none while a
    /// native request's fields are read, or for one answered without its operation.
    label: Option<&'static Label>,
}

impl Job {
    /// When, on the shard's clock, the operation's client stops waiting: its own deadline, or the
    /// stock S3 clients' patience past the last time its work moved, whichever comes first
    /// (research/77 D2).
    fn bound_ns(&self) -> u64 {
        let patience = u64::try_from(bounds::STOCK_PATIENCE.as_nanos()).unwrap_or(u64::MAX);
        let quiet = self.moved_ns.saturating_add(patience);
        self.deadline_ns
            .map_or(quiet, |deadline| deadline.min(quiet))
    }

    /// Whether its work is held, by a peer or a device: an ask of its own out, or a chunk waiting
    /// for room at a volume or in the transport toward it.
    fn held(&self) -> bool {
        !self.pending.is_empty()
            || match &self.work {
                Work::Put(putting) => putting.put.as_ref().is_some_and(Put::waits_for_room),
                Work::Copy(copying) => copying.put.as_ref().is_some_and(Put::waits_for_room),
                _ => false,
            }
    }

    /// What to do while its work is held: vouch within its patience, answer a refusal past it,
    /// or stop vouching if its reply already began. A get's driver is made with its reply head
    /// (`current_read`); before that, it can still be answered `SlowDown`.
    fn bounded(&self, now: u64) -> Bounded {
        if now < self.bound_ns() {
            Bounded::Held
        } else if matches!(&self.work, Work::Get(getting) if getting.get.is_some() && getting.held.is_none())
        {
            Bounded::Unvouched
        } else {
            Bounded::SlowDown
        }
    }

    /// Its next bound or working beat, while its own work is held. A get whose reply began
    /// and whose patience passed is vouched for no more and needs no beat; a get still reading
    /// its version must wake at the bound even through the local S3 door.
    fn due_ns(&self, now: u64, beat: u64) -> Option<u64> {
        if self.bounded(now) == Bounded::Unvouched {
            return None;
        }
        let bound = self.bound_ns();
        Some(match self.door {
            Door::Exchange(_) => bound.min(now.saturating_add(beat)),
            Door::Local(_) => bound,
        })
    }

    /// Whether the operation still works toward its reply, rather than giving one or reading the
    /// fields that say what it is.
    fn working(&self) -> bool {
        !matches!(
            self.work,
            Work::Fields(..) | Work::Draining(_) | Work::Replying(..) | Work::Answering(_)
        )
    }
}

/// What the host keeps of one native put or get beside its charge, by design: its job, and the
/// work it boxes, a put's or a get's whichever is larger, its driver inline in it
/// (`derive::native`).
pub(crate) fn record_bytes() -> Option<usize> {
    size_of::<Job>().checked_add(size_of::<Putting>().max(size_of::<Getting>()))
}

/// What a held job's bound makes of it at a turn (research/77 D2).
#[derive(Debug, PartialEq, Eq)]
enum Bounded {
    /// Within its client's patience: its client is told its work is held.
    Held,
    /// Past it, no reply begun: ended `503 SlowDown`, letting go of what it asked.
    SlowDown,
    /// Past it, its reply begun: told nothing more, its chunk reads ended by their transport.
    Unvouched,
}

/// Where a job's operation came in: its body is read and its reply written there.
enum Door {
    /// A native exchange, the operation's fields at its body's start (`mantle_protocol::object`).
    Exchange(ExchangeId),
    /// The S3 listener's, in-process (`crate::door`).
    Local(Box<Local>),
}

/// A local door's ends: the request's body as the listener sends it, and the reply as the host
/// writes it.
struct Local {
    body: ChannelReceiver<Piece>,
    reply: Sender<door::Answer>,
    /// The body's end once it came, with the checksum its trailer carried.
    ended: Option<Option<checksum::Checksum>>,
    /// Answers the reply's channel had no room for, sent before any other, in order. A stream's
    /// next piece is taken only once these are sent, so they are at most one piece of bytes and
    /// the few answers around it.
    pending: VecDeque<door::Answer>,
    /// Whether the listener was told the put wants its body.
    ready: bool,
}

/// What became of an answer sent on a local door.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sent {
    Taken,
    /// The channel is full: the answer is held, and sent first at a later turn.
    Held,
    /// The connection is gone.
    Gone,
}

impl Local {
    /// Sends what waits, in order: whether the door takes more now.
    fn flush(&mut self) -> Sent {
        while let Some(answer) = self.pending.pop_front() {
            match self.reply.try_send(answer) {
                Ok(()) => {}
                Err(SyncError::Full(answer)) => {
                    self.pending.push_front(answer);
                    return Sent::Held;
                }
                Err(SyncError::Closed(_) | SyncError::NotOnShardThread(_)) => return Sent::Gone,
            }
        }
        Sent::Taken
    }

    /// Sends `answer` after what waits, or holds it behind them.
    fn offer(&mut self, answer: door::Answer) -> Sent {
        self.pending.push_back(answer);
        self.flush()
    }
}

enum Work {
    /// The request's fields being read from its body's start: the head, the fields so far.
    Fields(Head, Vec<u8>),
    /// A request refused or done: what is left of its body read and dropped, then the reply, so
    /// the exchange ends whole (`Arriving::Redirect`'s reason). Boxed: a reply is many times any
    /// other job's state.
    Draining(Box<Reply>),
    /// A reply's head sent: the fields that open its body, and how much of them is written.
    Replying(Vec<u8>, usize),
    /// A local operation's last answers going out, in order: its reply and its end, or its end.
    Answering(VecDeque<door::Answer>),
    /// CreateBucket or DeleteBucket, through the bucket's coordinator.
    Bucket(Box<Coordinating>),
    Put(Box<Putting>),
    /// GetObject, or HeadObject.
    Get(Box<Getting>),
    /// DeleteObject, PutBucketVersioning, CreateMultipartUpload, a native upload's Open or
    /// AbortMultipartUpload: the bucket's row read, then one command made from it.
    Command(Box<Commanding>),
    /// CompleteMultipartUpload, or a native upload's Finish, run by the `Completion` driver.
    Complete(Box<Completing>),
    /// An operation one read answers.
    Read(Box<Single>),
    /// DeleteObjects: its bucket's row read, then each object's delete (research/89 §5).
    Deletes(Box<Deleting>),
    /// CopyObject or UploadPartCopy: the source read by a `Get`, written by a `Put` (research/89
    /// §3–§4).
    Copy(Box<Copying>),
    /// An operator's request on the account's principals and their keys.
    Admin(Box<Administering>),
}

/// The operations an op holds at once once taken: a copy two, its get and its put (research/89
/// §3.7); any other one.
pub(super) fn weight(op: &Op) -> usize {
    match op {
        Op::Copy(_) | Op::CopyPart(_) => crate::derive::COPY_WEIGHT,
        _ => 1,
    }
}

/// The version a request's `versionId` names: `null`, or one of mantle's 13-character IDs
/// (`mantle_meta::key::version_id`); `InvalidVersion` for any other text, S3's `400
/// InvalidArgument`, "Invalid version id specified" (research/89 §2.1).
fn named(version: Option<&str>) -> Result<Option<Named>, Status> {
    match version {
        None => Ok(None),
        Some(NULL_VERSION) => Ok(Some(Named::Null)),
        Some(id) => parse_version_id(id)
            .map(|order| Some(Named::Order(order)))
            .ok_or(Status::InvalidVersion),
    }
}

/// A DeleteObjects: its objects, each one's delete asked in order, at most the commands an
/// operation is given out at once, and each one's result in its place (research/89 §5).
pub(crate) struct Deleting {
    bucket: String,
    objects: Vec<Target>,
    quiet: bool,
    /// The bucket's incarnation and versioning, once its row is read.
    row: Option<(u64, Versioning)>,
    /// The first object not yet asked.
    next: usize,
    /// Deletes asked and not yet answered.
    out: usize,
    results: Vec<Option<Deleted>>,
}

impl Deleting {
    /// The objects at `denied`, which its principal's policy does not let it delete, each
    /// answered `AccessDenied` in its place, as S3 answers them, its delete never asked
    /// (research/91 §6).
    fn deny(&mut self, denied: &[usize]) {
        for &at in denied {
            let (Some(target), Some(slot)) = (self.objects.get(at), self.results.get_mut(at))
            else {
                continue;
            };
            *slot = Some(Deleted {
                key: target.key.clone(),
                version: target.version.clone(),
                status: Status::AccessDenied,
                marker: None,
            });
        }
    }
}

/// Where a copy is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyStage {
    /// The destination bucket's row to read.
    Bucket,
    /// An UploadPartCopy's upload row to read, for its checksum's algorithm.
    Upload,
    /// The source's version to read.
    Source,
    /// The source read by a `Get` and written by a `Put`.
    Run,
    /// CopyObject's version committed as its key's null version: its mark read for the time the
    /// range gave it (research/89 §3.6).
    Mark,
}

/// A CopyObject or an UploadPartCopy (research/89 §3–§4).
pub(crate) struct Copying {
    bucket: String,
    key: String,
    /// UploadPartCopy's upload and part number; CopyObject's none.
    part: Option<(String, u16)>,
    id: u128,
    source: (String, String, Option<Named>),
    /// `x-amz-copy-source-if-*`, and a part's `x-amz-copy-source-range`.
    conditions: Headers,
    /// The destination's `If-Match` and `If-None-Match`.
    preconditions: Preconditions,
    directive: Directive,
    metadata: Metadata,
    checksum: Option<Algorithm>,
    stage: CopyStage,
    /// The destination bucket's row, and an UploadPartCopy's upload's.
    row: Option<Bucket>,
    upload: Option<Upload>,
    get: Option<Get>,
    put: Option<Put>,
    /// What the put checks at its end: the source's own MD5 and checksum, where it copies the
    /// whole source and the source keeps them.
    expected: Expected,
    /// Plaintext the get gave and the put has not yet taken, and how much of it the put took.
    carry: Option<(Bytes, usize)>,
    /// The put's body ended.
    ended: bool,
    get_waiting: VecDeque<(u64, get::Request)>,
    put_waiting: VecDeque<(u64, put::Request)>,
    /// The source version's ID.
    source_version: String,
    /// The put's file, whose mark names the version a null version's copy made.
    file: u128,
    /// The put's outcome, while the version's time is read.
    stored: Option<put::Stored>,
}

/// Which of a copy's drivers a request of its job is for: its ask's request ID carries it in its
/// lowest bit, the driver's own ID above it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Driver {
    Get,
    Put,
}

/// The request ID a copy's ask is kept under: the driver's ID shifted past a bit naming the
/// driver; none past what a u64 holds, a driver's IDs counting its requests from zero.
fn tagged(driver: Driver, id: u64) -> Option<u64> {
    let bit = match driver {
        Driver::Get => 0,
        Driver::Put => 1,
    };
    id.checked_mul(2)?.checked_add(bit)
}

fn untagged(tag: u64) -> (Driver, u64) {
    let driver = if tag & 1 == 0 {
        Driver::Get
    } else {
        Driver::Put
    };
    (driver, tag >> 1)
}

/// An operator's request on the account's principals and their keys (docs/design/principals.md):
/// one command of the root range, or one read of it, asked once and replied from its answer. Its
/// asker is the account's root, as every client of the native protocol is (node.md §3.6).
pub(crate) struct Administering {
    asking: Admin,
    asked: bool,
    /// A key made: its id and its secret, held until the root range answers and replied once.
    issued: Option<(String, mantle_s3::account::Secret)>,
}

/// What an operator's request asks of the root range.
enum Admin {
    Command(Box<root::Command>),
    /// A page of principals, or with `one` the principal asked, whose absence is `NoSuchEntity`.
    Read {
        query: Query,
        one: bool,
    },
    /// A key for `principal`: its id and secret drawn and the secret wrapped under the root key's
    /// current generation at its first turn, then asked as a command.
    Key {
        principal: String,
        rotating: bool,
    },
}

/// A principal's key a connection of the S3 listener asked for (`door::KeyAsk`): read from the
/// root range by ReadIndex, a read that began after the request came, so a key revoked by a
/// command committed before it is refused at every node; its secret unwrapped under the root
/// key, and answered on its own channel (docs/design/principals.md).
pub(crate) struct Lookup {
    id: String,
    reply: Sender<door::Signed>,
    /// The read asked, once the node had room for it.
    ask: Option<ReadAsk>,
}

/// A bucket's create or delete, run by its coordinator.
struct Coordinating {
    bucket: String,
    /// A DeleteBucket rather than a CreateBucket.
    delete: bool,
    /// A delete's bucket row read and its owner the asker: S3 lets no other principal delete a
    /// bucket unless a policy grants it (05 §10.5).
    owned: bool,
    coordinator: Option<Coordinator>,
}

struct Putting {
    bucket: String,
    key: String,
    /// An UploadPart's upload and number; a PutObject's none.
    part: Option<(String, u16)>,
    id: u128,
    length: u64,
    preconditions: Preconditions,
    /// What the body's end is checked against: its `Content-MD5` and checksum (05 §3.2).
    expected: Expected,
    /// The algorithm of the checksum a trailer brings with the body's end: aws-chunked's through
    /// the listener's door, or the value past a native put's bytes (`object::Op::Put`).
    trailing: Option<checksum::Algorithm>,
    /// A native put's trailer read so far, at most its algorithm's width.
    trailer: Vec<u8>,
    /// The driver, once the bucket's row was read.
    put: Option<Put>,
    /// Body bytes read and not yet taken by the driver, and the body's bytes taken so far.
    carry: Vec<u8>,
    taken: u64,
    /// The driver's requests not yet asked: the node held as many asks as it keeps.
    waiting: VecDeque<(u64, put::Request)>,
    /// A native upload's run (an Append): its upload's row read after its bucket's.
    run: Option<Appending>,
    /// A PutObject's content headers and user-defined metadata, which its version keeps
    /// (research/89 §1).
    metadata: Metadata,
}

/// A native upload's run as its Append names it, and its upload's runs once read: where the run
/// begins, whether it ends the object, and the bucket's incarnation its commit is made under.
struct Appending {
    offset: u64,
    last: bool,
    incarnation: Option<u64>,
    runs: Option<Runs>,
}

struct Getting {
    bucket: String,
    key: String,
    headers: Headers,
    /// The version `versionId` names; none for the key's current one (research/89 §2.1).
    version: Option<Named>,
    /// A HeadObject: the version's facts, and no bytes.
    head: bool,
    get: Option<Get>,
    /// The reply, held until the driver has the range's first bytes or its end, so a block that
    /// cannot be read before any byte goes is answered with a typed refusal rather than a reply
    /// begun and cut (docs/design/repair.md §6); none once sent.
    held: Option<Box<Reply>>,
    /// Plaintext taken from the driver and not yet written to the exchange.
    out: Option<(Bytes, usize)>,
    /// Bytes of the reply's body written so far, and the body's length.
    written: u64,
    total: u64,
    waiting: VecDeque<(u64, get::Request)>,
}

/// An operation whose bucket's row is read first, for the incarnation, versioning and owner its
/// one command is made from.
struct Commanding {
    bucket: String,
    then: Then,
    /// The command asked.
    asked: bool,
}

/// The command a bucket's row is read for.
enum Then {
    /// DeleteObject: the Name range's delete (05 §7.3), of the version `versionId` names where
    /// it names one (research/89 §2.2).
    Delete {
        key: String,
        if_match: Option<Match>,
        named: Option<Named>,
    },
    /// PutBucketVersioning, by the bucket's owner (05 §7.1).
    Versioning(Versioning),
    /// CreateMultipartUpload, with the checksum its parts carry (05 §4.2) and the metadata its
    /// completed version keeps (research/89 §1).
    CreateUpload {
        key: String,
        checksum: Option<(Algorithm, bool)>,
        metadata: Metadata,
    },
    /// A native upload's Open (gateway.md §2.1): its object's length where stated, its
    /// full-object checksum's algorithm, the length of its runs (`name::run_length`) and its
    /// object's metadata.
    Open {
        key: String,
        length: Option<u64>,
        checksum: Algorithm,
        run: u64,
        metadata: Metadata,
    },
    /// AbortMultipartUpload (05 §4.8).
    Abort { key: String, upload: String },
}

/// A CompleteMultipartUpload: its bucket's row read first, for the incarnation, versioning and
/// default retention its Complete names; then its driver.
struct Completing {
    bucket: String,
    key: String,
    upload: String,
    /// The parts the request lists, each its number and ETag.
    listed: Vec<(u16, String)>,
    /// A native upload's Finish: the length and full-object checksum its client sent, which its
    /// runs must combine to.
    native: Option<(u64, record::Checksum)>,
    preconditions: Preconditions,
    completion: Option<Completion>,
    /// The driver's requests not yet asked: the node held as many asks as it keeps.
    waiting: VecDeque<(u64, complete::Request)>,
}

/// An operation one read answers.
enum Single {
    /// HeadBucket: the bucket's row.
    HeadBucket(String),
    /// ListObjectsV2: a page of keys.
    List(List),
    /// ListBuckets: a page of the asker's buckets.
    Buckets {
        prefix: String,
        after: Option<String>,
        max: u32,
    },
    /// GetBucketVersioning: the bucket's row.
    Versioning(String),
    /// ListParts: a page of an upload's parts.
    Parts {
        bucket: String,
        key: String,
        upload: String,
        after: u16,
        max: u32,
    },
    /// ListMultipartUploads: a page of the bucket's uploads, its query made from the request.
    Uploads(Query),
    /// A native upload's Offset: its row, its progress.
    Offset {
        bucket: String,
        key: String,
        upload: String,
    },
    /// ListObjectVersions: a page of the bucket's versions, its query made from the request.
    Versions(Query),
}

/// The volume table as this node's member of the root range held it after an entry it applied,
/// read again once per entry the member applies, the only thing that changes it; and the scheme
/// new blocks take over it, chosen again only when the table's failure domains changed, the one
/// thing of the table the choice reads (the gateway's rates and target are the process's own).
pub(crate) struct Table {
    applied: u64,
    /// In the order of their ids (`root::volumes`).
    placed: Vec<Placed>,
    /// The same volumes, each with its standing and its node's presence (`root::facts`): what
    /// placement and repair judge by (docs/design/repair.md §2).
    pub(super) facts: Vec<root::Fact>,
    /// The volumes that take chunks now, which the scheme is chosen over.
    standing: Vec<Placed>,
    scheming: Scheming,
}

impl Table {
    /// The table of `facts`, read as of entry `applied`, after `was`: its scheme carried where the
    /// domains of the volumes that take chunks now are as they were, and asked again where they
    /// changed (`Scheming::settle`, which `Node::table` calls at each look).
    fn of(was: Option<Table>, applied: u64, facts: Vec<root::Fact>) -> Self {
        let placed: Vec<Placed> = facts.iter().map(|f| f.placed.clone()).collect();
        let standing: Vec<Placed> = facts
            .iter()
            .filter(|f| f.takes())
            .map(|f| f.placed.clone())
            .collect();
        let same = was
            .as_ref()
            .is_some_and(|t| same_domains(&t.standing, &standing));
        let scheming = Scheming::after(was.map(|t| t.scheming), same, applied);
        Self {
            applied,
            placed,
            facts,
            standing,
            scheming,
        }
    }

    /// The node that holds `volume`, if the table holds it.
    pub(super) fn node_of(&self, volume: u128) -> Option<PeerId> {
        let at = self
            .placed
            .binary_search_by_key(&volume, |p| p.volume)
            .ok()?;
        self.placed.get(at).map(|p| p.node)
    }

    /// The root range's applied index the table was read at.
    pub(super) fn applied(&self) -> u64 {
        self.applied
    }

    /// The scheme new blocks take now: none where no candidate fits the domains, and refused
    /// where the durability model's choice for them unwound on the compute pool, until the
    /// domains change and it is asked again.
    pub(super) fn scheme(&self) -> Result<Option<durability::Scheme>, NodeError> {
        self.scheming.now.map_err(|Unwound| {
            NodeError::Runtime("the durability model's choice unwound on the compute pool".into())
        })
    }
}

/// The durability model's choice for a table's domains, and the scheme new blocks take meanwhile
/// (docs/design/node.md §5.1). The model runs on hyper-rt's compute pool, off the shard, which it
/// held for 2,448.5 allocations each time it ran there (research/71); the shard reads the answer
/// once it landed. Until then new blocks take the scheme chosen before wherever it still fits the
/// domains, no wider than they are: the model prices a scheme by the scheme and the gateway's
/// rates alone, the domains only ruling out the wider candidates (durability.md §4), so a block
/// placed under it, one chunk to each domain (`place::offer`), is exactly as durable as the blocks
/// before the change: within the target, or as near it as the cell came. Where it no longer fits,
/// or none was chosen yet, a block placed under it would put two of its chunks in one domain,
/// which the model never priced: placements wait for the choice, made at once on the shard.
struct Scheming {
    /// The scheme new blocks take now.
    now: Result<Option<durability::Scheme>, Unwound>,
    /// The generation of the table's domains: the applied index of the reading at which they last
    /// changed. A choice is asked for one and its answer taken only for the same.
    domains_at: u64,
    /// Whether `now` was chosen for the domains of `domains_at`.
    settled: bool,
    /// The choice running on the compute pool, at most one: a table whose domains changed again
    /// while it ran asks again once its stale answer lands, as a per-node task is discarded if its
    /// node changed meanwhile (CLAUDE.md §10).
    choosing: Option<Choosing>,
}

/// The durability model's choice for a table's domains unwound on the compute pool: its job's
/// receiver saw the sender gone without a value (hyper-rt `compute::run`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Unwound;

/// A choice running on the compute pool: the generation of the domains it was asked for, and its
/// answer.
struct Choosing {
    asked_at: u64,
    answer: OneshotReceiver<Option<durability::Scheme>>,
}

impl Scheming {
    /// The scheming of a table read at `applied`, after `was`'s: carried whole where the domains
    /// are the `same`, and otherwise a new generation, the scheme chosen before kept until
    /// `settle` judges whether it fits, and a choice still running kept to be judged stale.
    fn after(was: Option<Scheming>, same: bool, applied: u64) -> Self {
        match was {
            Some(was) if same => was,
            Some(was) => Self {
                now: was.now,
                domains_at: applied,
                settled: false,
                choosing: was.choosing,
            },
            None => Self {
                now: Ok(None),
                domains_at: applied,
                settled: false,
                choosing: None,
            },
        }
    }

    /// Takes the running choice's answer if it landed, and asks the model where the scheme is not
    /// yet chosen for the domains of the volumes that take chunks now, `standing`: on the compute
    /// pool while the scheme held fits them, and on the shard at once where it does not, or where
    /// the pool refuses the job (none in this process, one at its bound, or one stopping: the
    /// choice is never left undone, and runs where it ran before the pool).
    fn settle(&mut self, gateway: Option<&GatewaySettings>, standing: &[Placed]) {
        self.take();
        let asked = self
            .choosing
            .as_ref()
            .is_some_and(|c| c.asked_at == self.domains_at);
        if self.settled || asked {
            return;
        }
        let Some(gateway) = gateway else {
            return;
        };
        let ask = Ask::of(gateway, standing);
        let fits = matches!(self.now, Ok(Some(scheme)) if scheme.width() <= ask.domains);
        if fits && self.choosing.is_some() {
            return;
        }
        if fits {
            match compute::run(move || ask.choose()) {
                Ok(answer) => {
                    self.choosing = Some(Choosing {
                        asked_at: self.domains_at,
                        answer,
                    });
                    return;
                }
                Err(refused) => {
                    let job = refused.job;
                    self.now = Ok(job());
                }
            }
        } else {
            self.now = Ok(ask.choose());
        }
        self.settled = true;
    }

    /// The running choice's answer, if it landed: taken where it was asked for the domains the
    /// table has, and dropped where they changed since.
    fn take(&mut self) {
        let Some(choosing) = self.choosing.as_mut() else {
            return;
        };
        let landed = match choosing.answer.try_recv() {
            Ok(None) => return,
            Ok(Some(scheme)) => Ok(scheme),
            Err(_) => Err(Unwound),
        };
        let current = choosing.asked_at == self.domains_at;
        self.choosing = None;
        if current {
            self.now = landed;
            self.settled = true;
        }
    }
}

/// What the durability model's choice reads, owned, so it runs on the compute pool: the
/// candidates, the count of the domains that take chunks now, and the gateway's rates and target.
struct Ask {
    candidates: Vec<durability::Scheme>,
    domains: usize,
    rates: Rates,
    target: f64,
}

impl Ask {
    /// The choice over `standing`: a domain counts while it holds a volume that takes chunks now,
    /// active and its node seen.
    fn of(gateway: &GatewaySettings, standing: &[Placed]) -> Self {
        let mut domains: Vec<&str> = standing.iter().map(|p| p.domain.as_str()).collect();
        domains.sort_unstable();
        domains.dedup();
        Self {
            candidates: durability::candidates(),
            domains: domains.len(),
            rates: gateway.rates.clone(),
            target: gateway.target,
        }
    }

    /// The scheme a new block takes: the durability model's choice among the schemes that fit the
    /// cell's failure domains, or the closest to the target where none meets it, as a cell short
    /// of domains places on those there are (node.md §1.5). A block written while machines sleep
    /// takes the scheme the machines seen meet the target with, a chunk to each domain, rather
    /// than waiting for them or doubling chunks on the domains there are (docs/design/repair.md
    /// §5).
    fn choose(self) -> Option<durability::Scheme> {
        match durability::choose(&self.candidates, self.domains, &self.rates, self.target) {
            Ok(Some(Choice::Meets { scheme, .. } | Choice::Short { scheme, .. })) => Some(scheme),
            _ => None,
        }
    }
}

/// Whether two readings of the volume table hold the same volumes in the same failure domains.
fn same_domains(was: &[Placed], now: &[Placed]) -> bool {
    was.len() == now.len()
        && was
            .iter()
            .zip(now)
            .all(|(a, b)| a.volume == b.volume && a.domain == b.domain)
}

/// The data keys of the files a get opens, unwrapped under the root key's generation each names.
struct RootKeys {
    keys: Vec<(u32, [u8; 32])>,
}

impl Keyring for RootKeys {
    fn unwrap(&self, key: &WrappedKey) -> Result<DataKey, GetError> {
        let Wrapper::Root(generation) = key.by else {
            return Err(GetError::Key);
        };
        let (_, bytes) = self
            .keys
            .iter()
            .find(|(g, _)| *g == generation)
            .ok_or(GetError::Key)?;
        DataKey::unwrap(&key.bytes, &WrappingKey::new(bytes)).map_err(|_| GetError::Key)
    }
}

/// The tags an `If-Match` (`strong`) or `If-None-Match` header names, as the Name range judges
/// them at commit (`mantle_s3::conditional::named`).
fn matched(header: Option<&str>, strong: bool) -> Option<Match> {
    header.map(|list| match conditional::named(list, strong) {
        None => Match::Any,
        Some(tags) => Match::Tags(tags),
    })
}

/// The status a Name range's refusal of a write is answered with (node.md §4.4).
fn name_status(outcome: &name::Outcome) -> Status {
    match outcome {
        name::Outcome::NoSuchBucket => Status::NoSuchBucket,
        name::Outcome::NoSuchKey => Status::NoSuchKey,
        name::Outcome::PreconditionFailed => Status::PreconditionFailed,
        name::Outcome::Locked => Status::AccessDenied,
        name::Outcome::NoSuchUpload => Status::NoSuchUpload,
        name::Outcome::InvalidPart => Status::InvalidPart,
        name::Outcome::InvalidPartOrder => Status::InvalidPartOrder,
        name::Outcome::EntityTooSmall => Status::EntityTooSmall,
        // A part's number or size outside S3's, which the Name range refuses too.
        name::Outcome::Invalid => Status::InvalidArgument,
        name::Outcome::Offset { .. } => Status::Offset,
        _ => Status::Failed,
    }
}

/// What one object's delete came to (research/89 §5.2): done, with the delete marker it made or
/// removed, or the refusal its Name range's answer earns.
fn deleted(target: &Target, outcome: name::Outcome) -> Deleted {
    let (status, marker) = match outcome {
        // "When a multi-object Delete request results in Amazon S3 either adding a delete marker
        // or removing a delete marker, the response returns" the marker's version ID.
        name::Outcome::Deleted { marker, version } => {
            (Status::Done, if marker { version } else { None })
        }
        refused => (name_status(&refused), None),
    };
    Deleted {
        key: target.key.clone(),
        version: target.version.clone(),
        status,
        marker,
    }
}

/// The status a completion's failure is answered with (05 §4.4).
fn complete_status(error: &CompleteError) -> Status {
    match error {
        CompleteError::NoSuchUpload => Status::NoSuchUpload,
        // Composite checksums need parts numbered from 1 without a gap: S3 answers 500 for the
        // gap, and mantle the part it cannot combine (05 §3.3).
        CompleteError::InvalidPart | CompleteError::NotConsecutive => Status::InvalidPart,
        CompleteError::InvalidPartOrder => Status::InvalidPartOrder,
        CompleteError::EntityTooSmall => Status::EntityTooSmall,
        CompleteError::BadDigest => Status::BadDigest,
        CompleteError::Unended => Status::IncompleteBody,
        CompleteError::Name(outcome) => name_status(outcome),
        // A listed part uploaded again while the completion ran: `500`, which clients retry.
        _ => Status::Failed,
    }
}

/// The reply a put's failure is answered with (node.md §4.4): a block no volume offered took,
/// `SlowDown` for that cause.
fn put_failed(error: &PutError) -> Reply {
    Reply::status(match error {
        PutError::IncompleteBody => Status::IncompleteBody,
        PutError::EntityTooLarge { .. } => Status::EntityTooLarge,
        PutError::BadDigest | PutError::BadChecksum => Status::BadDigest,
        PutError::Unplaced(_) => return Reply::slowed(door::Slowed::Unplaced),
        PutError::Full(_) => Status::InsufficientStorage,
        PutError::InvalidPartNumber(_) => Status::InvalidArgument,
        PutError::Name(outcome) => name_status(outcome),
        _ => Status::Failed,
    })
}

/// A put's checksum as its reply carries it: a full-object value (05 §3.3).
fn kept(c: &checksum::Checksum) -> Stored {
    Stored {
        algorithm: c.algorithm.code(),
        parts: 0,
        value: c.bytes.clone(),
    }
}

/// A part's row, its ETag, size, checksum, file and deadline the put's driver fills.
fn unfilled_part() -> Part {
    Part {
        etag: String::new(),
        size: 0,
        checksum: None,
        file: 0,
        modified_ns: 0,
        deadline_ns: 0,
    }
}

/// A native upload's progress as its runs stand, with a run's checksum in its frame.
fn progress(runs: &Runs, checksum: Option<Vec<u8>>) -> Progress {
    Progress {
        offset: runs.offset,
        runs: runs.count,
        run: runs.run,
        ended: runs.ended,
        checksum,
    }
}

/// A run's frame, once it committed: the offset after it, its number and its checksum.
fn run_frame(appending: &Appending, stored: &put::Stored) -> Option<Progress> {
    let runs = appending.runs?;
    Some(Progress {
        offset: appending.offset.checked_add(stored.size)?,
        runs: runs.count.checked_add(1)?,
        run: runs.run,
        ended: appending.last,
        checksum: stored.checksum.as_ref().map(|c| c.bytes.clone()),
    })
}

/// A version's checksum as a reply carries it.
fn shown(c: &record::Checksum) -> Stored {
    Stored {
        algorithm: c.algorithm,
        parts: c.parts,
        value: c.value.clone(),
    }
}

/// What an operation that writes `written` bytes, or none, holds at its own size, and the blocks a
/// put of it holds going down at once: a write's window as `learned` measured the node's puts
/// (`crate::window`), at least the floor's (`GatewaySettings::window`), charged for those blocks
/// (`derive::Charges::write`); every other operation the most, at the floor's window. None on a
/// node that hosts no objects.
/// What an op is charged once taken, and its window ([`sized`]): a copy its get's and its put's at
/// once (`weight`), each at the most an operation holds, as its source's size is known only once
/// its row is read; any other op as the bytes it writes say.
pub(super) fn charged(
    gateway: Option<&GatewaySettings>,
    learned: &crate::window::Window,
    op: &Op,
) -> (u64, usize) {
    let (charge, window) = sized(gateway, learned, op.written());
    let charge = u64::try_from(weight(op))
        .ok()
        .and_then(|times| charge.checked_mul(times))
        // Past what any memory holds: never admitted.
        .unwrap_or(u64::MAX);
    (charge, window)
}

pub(super) fn sized(
    gateway: Option<&GatewaySettings>,
    learned: &crate::window::Window,
    written: Option<u64>,
) -> (u64, usize) {
    let Some(gateway) = gateway else {
        return (0, 0);
    };
    match written {
        Some(length) => {
            let learned = learned
                .blocks(length, gateway.charges.largest_block())
                .max(gateway.window);
            let window = fitting(gateway, length, learned);
            (gateway.charges.write(window, length), window)
        }
        None => (gateway.charges.of(None), gateway.window),
    }
}

/// The widest window from the floor's to the one learned whose write of `length` bytes the
/// operations' bytes hold (`gateway.objects_bytes`), the floor's where none past it does: a write
/// charged past them would never be admitted, and the door, which takes its requests in the order
/// they came, would hold every request behind it (CLAUDE.md §2). The floor's always fits, as the
/// operations' bytes are at least the most one operation is charged (`derive::most_charged`). A
/// write's charge grows with the blocks it holds going down (`derive::Charges::write`), so the
/// widest is found by halving the range.
fn fitting(gateway: &GatewaySettings, length: u64, learned: usize) -> usize {
    let (mut low, mut high) = (gateway.window, learned.max(gateway.window));
    while low < high {
        let mid = low.saturating_add(high.saturating_sub(low).div_ceil(2));
        if gateway.charges.write(mid, length) <= gateway.objects_bytes {
            low = mid;
        } else {
            high = mid.saturating_sub(1);
        }
    }
    low
}

/// A bucket's versioning as GetBucketVersioning answers it: none for a bucket never versioned.
fn answered(versioning: Versioning) -> Option<mantle_s3::body::Versioning> {
    match versioning {
        Versioning::Unversioned => None,
        Versioning::Enabled => Some(mantle_s3::body::Versioning::Enabled),
        Versioning::Suspended => Some(mantle_s3::body::Versioning::Suspended),
    }
}

/// The reply that states a version's facts, `status` its answer: its metadata among them
/// (research/89 §1).
fn facts(status: Status, order: u64, version: &Version) -> Reply {
    Reply {
        status,
        size: version.size,
        version: name::shown_id(version.null, order),
        etag: version.etag.clone(),
        modified: object::seconds(version.modified_ns),
        checksum: version.checksum.as_ref().map(shown),
        marker: version.marker,
        metadata: version.headers.clone(),
        ..Reply::status(status)
    }
}

impl<E: Engine + 'static, L: NodeLog> Node<E, L> {
    /// An object's request arrived on `exchange` from `peer`, its body `body` bytes: a job that
    /// reads its fields first; parked, its body unread, where the host has no room for it now or
    /// requests parked before it wait (`take_parked`), as the S3 listener's door holds a request
    /// (node.md §4.5); or the reply it gets at once.
    pub(super) fn take_object(&mut self, exchange: ExchangeId, peer: PeerId, body: Option<u64>) {
        let head = Head::decode(
            self.driver.endpoint().head(exchange).unwrap_or_default(),
            body,
        );
        let now = hyper_rt::futures::now_ns();
        let deadline_ns = head
            .as_ref()
            .ok()
            .and_then(Head::remaining)
            .map(|remaining| {
                now.saturating_add(u64::try_from(remaining.as_nanos()).unwrap_or(u64::MAX))
            });
        // What it holds at its own size, its body's length known from its head.
        let (charge, window) = match &head {
            Ok(head) => sized(self.gateway.as_ref(), &self.put_window, head.written()),
            Err(_) => (0, 0),
        };
        let parks = head.is_ok() && self.gateway.is_some() && !self.stopping();
        // Asked even behind parked requests: one the limit turns away tells it its clients ask
        // more than it admits.
        let room = parks && self.admits(charge);
        if parks && (!room || !self.parked.is_empty()) && self.parked.len() < self.parked_most {
            let patience = u64::try_from(bounds::STOCK_PATIENCE.as_nanos()).unwrap_or(u64::MAX);
            self.parked.push_back(Parked {
                exchange,
                peer,
                head: head.ok(),
                charge,
                window,
                deadline_ns,
                until_ns: deadline_ns.unwrap_or_else(|| now.saturating_add(patience)),
            });
            return;
        }
        // Not parked and no room: a stopping node's, or one whose parked requests are as many as
        // it keeps.
        let slowed = if room {
            None
        } else if self.stopping() {
            Some(door::Slowed::Stopping)
        } else {
            Some(door::Slowed::Queued)
        };
        self.start_object(
            exchange,
            peer,
            head,
            (deadline_ns, now, charge, window),
            slowed,
        );
    }

    /// The parked requests taken in the order they came while the host has room for the first
    /// (`crate::admit`); one whose exchange ended is let go, and one whose client stops waiting
    /// (its deadline, or a stock client's patience) or whose node stops is answered `SlowDown`.
    /// Those still parked are held by the host, as a job whose asks are out is: each tells its
    /// asker so (research/77 D1, D2).
    pub(super) fn take_parked(&mut self) {
        let now = hyper_rt::futures::now_ns();
        while let Some(&first) = self.parked.front() {
            if self.driver.endpoint().head(first.exchange).is_none() {
                self.parked.pop_front();
                continue;
            }
            let over = now >= first.until_ns || self.stopping();
            if !over && !self.admits(first.charge) {
                let Self { parked, driver, .. } = self;
                for waiting in parked.iter() {
                    driver.endpoint().working(waiting.exchange);
                }
                return;
            }
            self.parked.pop_front();
            let head = first.head.ok_or(ObjectError::Fields(0));
            let slowed = if !over {
                None
            } else if self.stopping() {
                Some(door::Slowed::Stopping)
            } else {
                Some(door::Slowed::Waited)
            };
            self.start_object(
                first.exchange,
                first.peer,
                head,
                (first.deadline_ns, now, first.charge, first.window),
                slowed,
            );
        }
    }

    /// When the node's next turn is due for its parked requests: at once where the host has room
    /// for the first, else at the first's client's patience or the next beat it is vouched at, a
    /// probe timeout, as a held job's (`Job::due_ns`).
    pub(super) fn parked_due(&self, now: u64) -> Option<u64> {
        let first = self.parked.front()?;
        let room = self
            .admission
            .as_ref()
            .is_some_and(|a| a.admits(first.charge).is_ok());
        let beat = u64::try_from(bounds::initial_pto().as_nanos()).unwrap_or(u64::MAX);
        Some(if room {
            now
        } else {
            first.until_ns.min(now.saturating_add(beat))
        })
    }

    /// The job of an object's request on `exchange` from `peer`, at `now`, its client's deadline
    /// and its charge as its head says: one that reads its fields first where the host admits it
    /// (no `slowed`), else the reply it gets at once, `SlowDown` for `slowed`'s cause where it is
    /// a request the host would run.
    fn start_object(
        &mut self,
        exchange: ExchangeId,
        peer: PeerId,
        head: Result<Head, ObjectError>,
        (deadline_ns, now, charge, window): (Option<u64>, u64, u64, usize),
        slowed: Option<door::Slowed>,
    ) {
        // A client's operations act for the account its certificate names; one whose
        // connections have all gone since asking is no longer known, and nothing is taken.
        let Some(principal) = self.driver.endpoint().directory().principal(peer) else {
            self.driver
                .endpoint()
                .refuse_exchange(exchange, Refusal::Identity);
            return;
        };
        let mut admitted_ns = None;
        let work = match (head, self.gateway.as_ref(), slowed) {
            (_, None, _) => Work::Draining(Box::new(Reply::status(Status::Failed))),
            (Err(_), Some(_), _) => Work::Draining(Box::new(Reply::status(Status::Malformed))),
            // A node stopping begins no operation (node.md §1.7), and one with no room for it
            // answers it at once: its client asks another, or again.
            (Ok(_), Some(_), Some(cause)) => Work::Draining(Box::new(Reply::slowed(cause))),
            (Ok(head), Some(_), None) => {
                admitted_ns = Some(now);
                let fields = usize::try_from(head.fields()).unwrap_or(0);
                Work::Fields(head, Vec::with_capacity(fields))
            }
        };
        let charge = if admitted_ns.is_some() { charge } else { 0 };
        if admitted_ns.is_some()
            && let Some(admission) = self.admission.as_mut()
        {
            admission.admitted(charge);
        }
        self.jobs.push(Job {
            door: Door::Exchange(exchange),
            principal,
            work,
            pending: Vec::new(),
            deadline_ns,
            moved_ns: now,
            admitted_ns,
            charge,
            window,
            label: None,
        });
    }

    /// An operation the S3 listener handed over (`crate::door`): a job of its work, or the reply
    /// it gets at once, as a native request's.
    pub(super) fn take_local(&mut self, request: door::Request) {
        let door::Request {
            op,
            principal,
            denied,
            body,
            reply,
        } = request;
        let now = hyper_rt::futures::now_ns();
        let mut admitted_ns = None;
        let (charge, window) = charged(self.gateway.as_ref(), &self.put_window, &op);
        let label = Some(labels::host(&op));
        // The door holds a request while the host has no room (`take_door`): one taken here past
        // the limit is a stopping node's.
        let room = self.gateway.is_some() && !self.stopping() && self.admits(charge);
        let work = match self.gateway.as_ref() {
            None => Work::Draining(Box::new(Reply::status(Status::Failed))),
            // Taken past the limit, the door holds no request while the host has no room: a
            // stopping node's (`take_door`).
            Some(_) if !room => Work::Draining(Box::new(Reply::slowed(door::Slowed::Stopping))),
            Some(_) => {
                admitted_ns = Some(now);
                let block = self.open_block(&op);
                let mut made = work(op, Vec::new(), block);
                if let Work::Deletes(deleting) = &mut made {
                    deleting.deny(&denied);
                }
                made
            }
        };
        let charge = if admitted_ns.is_some() { charge } else { 0 };
        if admitted_ns.is_some()
            && let Some(admission) = self.admission.as_mut()
        {
            admission.admitted(charge);
        }
        self.jobs.push(Job {
            door: Door::Local(Box::new(Local {
                body,
                reply,
                ended: None,
                pending: VecDeque::new(),
                ready: false,
            })),
            principal,
            work,
            pending: Vec::new(),
            // S3 states no patience: its stock clients' is the bound (research/77 D2).
            deadline_ns: None,
            moved_ns: now,
            admitted_ns,
            charge,
            window,
            label,
        });
    }

    /// A key the S3 listener asked for (`door::KeyAsk`): looked up at the next turn, or refused at
    /// once by a node that hosts no objects (`Failed`), a stopping one, or one holding as many
    /// lookups as its door holds requests (`SlowDown`): a connection asks one at a time.
    pub(super) fn take_lookup(&mut self, ask: door::KeyAsk) {
        let door::KeyAsk { id, reply } = ask;
        let refused = if self.gateway.is_none() {
            Some(door::Signed::Refused(Status::Failed))
        } else if self.stopping() {
            Some(door::Signed::Slowed(door::Slowed::Stopping))
        } else if self.lookups.len() >= self.door_capacity {
            Some(door::Signed::Slowed(door::Slowed::Lookups))
        } else {
            None
        };
        if let Some(refused) = refused {
            // A connection gone takes no answer.
            let _ = reply.try_send(refused);
            return;
        }
        self.lookups.push(Lookup {
            id,
            reply,
            ask: None,
        });
    }

    /// The lookups at a turn: each not yet asked read of the root range while the node has room
    /// for a read, and each answered given its answer.
    fn step_lookups(&mut self) {
        let mut at = 0;
        while let Some(lookup) = self.lookups.get(at) {
            let answer = match lookup.ask {
                Some(ask) => take_done(&mut self.reads_done, ask).map(|o| self.signed(o)),
                None => match self.group_of(Layer::Root) {
                    None => Some(door::Signed::Refused(Status::Failed)),
                    Some(group) => {
                        let query = Query::Signer {
                            id: lookup.id.clone(),
                        };
                        match self.read(group, query, u64::MAX, Wait::Request) {
                            Ok(ask) => {
                                if let Some(lookup) = self.lookups.get_mut(at) {
                                    lookup.ask = Some(ask);
                                }
                                None
                            }
                            // Asked again at the next turn a read ends.
                            Err(NodeError::Asks) => None,
                            Err(_) => Some(door::Signed::Refused(Status::Failed)),
                        }
                    }
                },
            };
            match answer {
                Some(answer) => {
                    let lookup = self.lookups.swap_remove(at);
                    let _ = lookup.reply.try_send(answer);
                }
                None => at = at.saturating_add(1),
            }
        }
    }

    /// A lookup's answer from its read: the key's principal, its secret unwrapped under the root
    /// key's generation its row names; none for a key the cell does not hold; refused `SlowDown`
    /// for a root range without a leader that can serve it within its budget (node.md §5.4), and
    /// `Failed` for a row that does not open, which is the range's corruption.
    fn signed(&self, outcome: ReadOutcome) -> door::Signed {
        let failed = door::Signed::Refused(Status::Failed);
        let read = match outcome {
            ReadOutcome::Read(read) => *read,
            ReadOutcome::Deadline => return door::Signed::Slowed(door::Slowed::KeyLeaderless),
            ReadOutcome::Refused(_) => return failed,
        };
        let signer = match read {
            Read::Signer(None) => return door::Signed::Unknown,
            Read::Signer(Some(signer)) => signer,
            _ => return failed,
        };
        let (row, held) = *signer;
        let key = self.gateway.as_ref().and_then(|g| {
            g.keys
                .iter()
                .find(|(generation, _)| *generation == row.generation)
        });
        let Some((_, key)) = key else {
            return failed;
        };
        match mantle_s3::seal::unwrap_secret(&row.wrapped, key) {
            Ok(secret) => door::Signed::Principal(Box::new(door::Signer {
                secret,
                name: held.name,
                id: held.id,
                policy: held.policy,
            })),
            Err(_) => failed,
        }
    }

    /// An operator's request at its turn: its one command or read of the root range asked, once;
    /// a key's id and secret drawn and the secret wrapped under the root key's current generation
    /// first, once, so a turn without room asks the same key again.
    fn step_admin(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(Job {
            work: Work::Admin(admin),
            pending,
            ..
        }) = self.jobs.get(at)
        else {
            return Ok(true);
        };
        if !pending.is_empty() || admin.asked {
            return Ok(false);
        }
        if let Admin::Key {
            principal,
            rotating,
        } = &admin.asking
        {
            let made = self.issue(principal, *rotating);
            let Some(Job {
                work: Work::Admin(admin),
                ..
            }) = self.jobs.get_mut(at)
            else {
                return Ok(true);
            };
            match made {
                Ok((command, issued)) => {
                    admin.asking = Admin::Command(Box::new(command));
                    admin.issued = Some(issued);
                }
                Err(status) => {
                    self.fail_job(at, status)?;
                    return self.drain_job(at);
                }
            }
        }
        let Some(group) = self.group_of(Layer::Root) else {
            self.fail_job(at, Status::Failed)?;
            return self.drain_job(at);
        };
        let asking = match self.jobs.get(at) {
            Some(Job {
                work: Work::Admin(admin),
                ..
            }) => match &admin.asking {
                Admin::Command(command) => Ok(Command::Root(command.as_ref().clone())),
                Admin::Read { query, .. } => Err(query.clone()),
                Admin::Key { .. } => return Ok(false),
            },
            _ => return Ok(true),
        };
        let asked = match asking {
            Ok(command) => self
                .command(group, command, u64::MAX, Wait::Request)
                .map(Pending::Command),
            Err(query) => self
                .read(group, query, u64::MAX, Wait::Request)
                .map(Pending::Read),
        };
        match asked {
            Ok(pending) => {
                if let Some(Job {
                    work: Work::Admin(admin),
                    pending: held,
                    ..
                }) = self.jobs.get_mut(at)
                {
                    held.push((pending, None));
                    admin.asked = true;
                }
                Ok(false)
            }
            // Asked again at the next turn, as every ask the node has no room for is.
            Err(NodeError::Asks) => Ok(false),
            Err(_) => {
                self.fail_job(at, Status::Failed)?;
                self.drain_job(at)
            }
        }
    }

    /// A key for `principal`: its id and secret drawn from the OS, and the command that makes it,
    /// its secret wrapped under the root key's current generation, the last the node holds
    /// (encryption.md §2); with the key, to reply once the root range made it.
    fn issue(
        &self,
        principal: &str,
        rotating: bool,
    ) -> Result<(root::Command, (String, mantle_s3::account::Secret)), Status> {
        let (generation, key) = self
            .gateway
            .as_ref()
            .and_then(|g| g.keys.last())
            .ok_or(Status::Failed)?;
        let id = mantle_s3::account::draw_id().map_err(|_| Status::Failed)?;
        let secret = mantle_s3::account::draw_secret().map_err(|_| Status::Failed)?;
        let wrapped = mantle_s3::seal::wrap_secret(&secret, key).map_err(|_| Status::Failed)?;
        let command = root::Command::Principal(principal::Command::Key(principal::NewKey {
            principal: principal.to_owned(),
            id: id.clone(),
            generation: *generation,
            wrapped,
            rotating,
            at_ns: 0,
        }));
        Ok((command, (id, secret)))
    }

    /// An operator's request answered by the root range: its reply, IAM's error codes for its
    /// refusals (research/91 §2), a key made with its id and secret, and a page of principals.
    fn admin_answered(&mut self, at: usize, outcome: Outcome) -> Result<(), NodeError> {
        let one = matches!(
            self.jobs.get(at),
            Some(Job {
                work: Work::Admin(admin),
                ..
            }) if matches!(admin.asking, Admin::Read { one: true, .. })
        );
        let reply = match outcome {
            Outcome::Command(CommandOutcome::Answered(Answer::Root(root::Outcome::Principal(
                made,
            )))) => match made {
                principal::Outcome::Made
                | principal::Outcome::Set
                | principal::Outcome::Removed => Reply::status(Status::Done),
                principal::Outcome::KeyMade { replacing } => {
                    let issued = match self.jobs.get_mut(at) {
                        Some(Job {
                            work: Work::Admin(admin),
                            ..
                        }) => admin.issued.take(),
                        _ => None,
                    };
                    match issued {
                        Some((id, secret)) => Reply {
                            listing: Some(Listing::Issued(Box::new(Issued {
                                id,
                                secret,
                                replacing,
                            }))),
                            ..Reply::status(Status::Done)
                        },
                        None => Reply::status(Status::Failed),
                    }
                }
                principal::Outcome::Taken => Reply::status(Status::EntityAlreadyExists),
                principal::Outcome::Full => Reply::status(Status::LimitExceeded),
                principal::Outcome::Unknown => Reply::status(Status::NoSuchEntity),
                principal::Outcome::Held => Reply::status(Status::DeleteConflict),
                principal::Outcome::Unrotatable => Reply::status(Status::NothingToRotate),
                principal::Outcome::Invalid => Reply::status(Status::InvalidArgument),
            },
            Outcome::Read(ReadOutcome::Read(read)) => match *read {
                Read::Principals(page) if one && page.principals.is_empty() => {
                    Reply::status(Status::NoSuchEntity)
                }
                Read::Principals(page) => Reply {
                    listing: Some(Listing::Principals(page)),
                    ..Reply::status(Status::Done)
                },
                _ => Reply::status(Status::Failed),
            },
            _ => Reply::status(Status::Failed),
        };
        self.reply_job(at, reply)
    }

    /// A job's fields read from its body's start, a piece at a time; once whole, the work they
    /// name, the bytes read past them the start of a put's body.
    fn read_fields(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        // A local operation comes typed, never as fields to read.
        let Door::Exchange(exchange) = job.door else {
            return Ok(true);
        };
        let Work::Fields(head, fields) = &mut job.work else {
            return Ok(false);
        };
        let wanted = usize::try_from(head.fields()).unwrap_or(usize::MAX);
        let endpoint = self.driver.endpoint();
        while fields.len() < wanted {
            let Ok(mut piece) = endpoint.reserve(Class::Transfer, PIECE) else {
                return Ok(false);
            };
            let got = endpoint.read_body(exchange, &mut piece);
            fields.extend_from_slice(piece.bytes());
            self.moved = self
                .moved
                .saturating_add(u64::try_from(piece.bytes().len()).unwrap_or(u64::MAX));
            endpoint.release(piece);
            match got {
                Ok(0) if fields.len() < wanted => return Ok(false),
                Ok(_) => {}
                // Its asker is gone.
                Err(_) => return Ok(true),
            }
        }
        let carry = fields.split_off(wanted.min(fields.len()));
        let op = head.op(fields);
        let admitted = job.charge;
        let block = op.as_ref().ok().and_then(|op| self.open_block(op));
        // A copy holds a get's window and a put's at once (`charged`), this job admitted at its
        // head's charge, its get's: one whose put the operations' memory does not hold beside the
        // rest is answered as one past the host's room is.
        let more = op
            .as_ref()
            .ok()
            .filter(|op| weight(op) > 1)
            .map_or(0, |op| {
                charged(self.gateway.as_ref(), &self.put_window, op)
                    .0
                    .saturating_sub(admitted)
            });
        let crowded = more > 0
            && self
                .admission
                .as_ref()
                .is_some_and(|admission| !admission.holds(more));
        if let Some(job) = self.jobs.get_mut(at) {
            job.work = match op {
                Err(_) => Work::Draining(Box::new(Reply::status(Status::Malformed))),
                Ok(_) if crowded => Work::Draining(Box::new(Reply::slowed(door::Slowed::Crowded))),
                Ok(op) => {
                    if more > 0
                        && let Some(admission) = self.admission.as_mut()
                    {
                        admission.took(more);
                        job.charge = job.charge.saturating_add(more);
                    }
                    job.label = Some(labels::host(&op));
                    work(op, carry, block)
                }
            };
        }
        Ok(false)
    }

    /// For a native upload's Open, the plaintext bytes of a whole block of the layout new blocks
    /// take, the unit its runs are cut in (`name::run_length`); none for any other operation, or
    /// where the volume table names no scheme yet.
    fn open_block(&mut self, op: &Op) -> Option<u64> {
        if !matches!(op, Op::Open { .. }) {
            return None;
        }
        let scheme = self.table().ok()?.scheme().ok()??;
        let segments = Layout::new(scheme).ok()?.segments_per_block();
        segments.checked_mul(u64::try_from(mantle_s3::seal::SEGMENT).ok()?)
    }

    /// The group of the range this node hosts of `layer`: the cell's one range of it.
    pub(super) fn group_of(&self, layer: Layer) -> Option<u128> {
        self.ranges
            .iter()
            .find(|k| k.layer == layer)
            .map(|k| k.group)
    }

    /// The volume table as this node's member of the root range holds it now: read from the
    /// range's rows again only once the member applied an entry since the last read. Where another
    /// shard drives the root range, the table its shard last answered, asked again once the range
    /// shows it applied past it ([`shard::ask_table`]), the one held read meanwhile: the node read
    /// one before the range moved (`Node::spread_to`). At each look the scheme new blocks take is
    /// settled as far as it can be without waiting (`Scheming::settle`): an answer the compute pool
    /// gave taken, and a choice asked where the domains changed.
    pub(super) fn table(&mut self) -> Result<&Table, NodeError> {
        let (handle, group) = self
            .ranges
            .iter()
            .find(|k| k.layer == Layer::Root)
            .map(|k| (k.handle, k.group))
            .ok_or(NodeError::Bounds("no root range here".into()))?;
        if let Some(far) = self.far.iter_mut().find(|far| far.group == group) {
            if let Some(answered) = far.table.take() {
                let shard::Facts { applied, facts } = *answered?;
                self.table = Some(Table::of(self.table.take(), applied, facts));
            }
            let held = self.table.as_ref().map(|t| t.applied);
            match shard::ask_table(&self.shards, far, held) {
                Ok(()) | Err(NodeError::LaneFull(_)) => {}
                Err(e) => return Err(e),
            }
        } else {
            let shell = self
                .owner
                .get_mut(handle)
                .ok_or(NodeError::Bounds("no root range here".into()))?;
            let applied = shell.applied().index;
            if self.table.as_ref().is_none_or(|t| t.applied != applied) {
                let facts = shard::read_facts(shell.machine_mut().engine_mut())?;
                self.table = Some(Table::of(self.table.take(), applied, facts));
            }
        }
        let table = self
            .table
            .as_mut()
            .ok_or(NodeError::Bounds("no volume table".into()))?;
        table
            .scheming
            .settle(self.gateway.as_ref(), &table.standing);
        Ok(table)
    }

    /// The host at a turn: each operation's answers taken, its next requests asked, its body read
    /// and its bytes written as far as they go, and each one done answered.
    ///
    /// The node's task is labelled, as the turn ends, with the operation whose step held it
    /// longest, none where it stepped none (`crate::labels`): a long poll of the task is counted
    /// under the operation that cost it most.
    pub(super) fn step_host(&mut self) -> Result<(), NodeError> {
        self.drop_abandoned();
        self.step_lookups();
        if self.jobs.is_empty() {
            labels::set(None);
            return Ok(());
        }
        // Every operation's answers taken before any asks again, so each asks with the room
        // that every answer taken this turn gave back.
        // A chunk answered otherwise than for want of room frees room at its volume and in the
        // transport toward it, as a volume's put answered does at the volume: each chunk refused
        // for want of room is asked again then (research/77 D2), and not at its own refusal,
        // which would ask it again each turn while the room stays taken.
        let freed = std::mem::take(&mut self.room_freed)
            || self.chunks_done.iter().any(|(_, outcome)| {
                !matches!(
                    outcome,
                    ChunkOutcome::Refused(chunk::Status::Busy)
                        | ChunkOutcome::Lost(Refusal::Budget)
                )
            });
        for at in 0..self.jobs.len() {
            self.take_answers(at)?;
        }
        if freed {
            self.offer_room()?;
        }
        self.bound_jobs()?;
        // The step that held the turn longest, and its operation's label.
        let mut slowest: Option<(u64, &'static Label)> = None;
        let mut at = 0;
        while at < self.jobs.len() {
            // Bytes of its own body read or of its reply written are its work moving, as its
            // asks answered are.
            let before = self.moved;
            let label = self.jobs.get(at).and_then(|job| job.label);
            let began = hyper_rt::futures::now_ns();
            let over = self.step_job(at)?;
            let held = hyper_rt::futures::now_ns().saturating_sub(began);
            if let Some(label) = label
                && slowest.is_none_or(|(most, _)| held >= most)
            {
                slowest = Some((held, label));
            }
            if self.moved != before
                && let Some(job) = self.jobs.get_mut(at)
            {
                job.moved_ns = hyper_rt::futures::now_ns();
            }
            if over {
                // What it still waits on is let go: an ended get's chunks still out, say.
                let job = self.jobs.swap_remove(at);
                self.ended(job.admitted_ns, job.charge);
                self.abandoned
                    .extend(job.pending.into_iter().map(|(pending, _)| pending));
            } else {
                at = at.saturating_add(1);
            }
        }
        labels::set(slowest.map(|(_, label)| label));
        Ok(())
    }

    /// Asks again each chunk a put's driver holds for want of room, its volume's or the
    /// transport's; a driver that fails doing so ends its job as its failed answers do.
    fn offer_room(&mut self) -> Result<(), NodeError> {
        for at in 0..self.jobs.len() {
            let put = match self.jobs.get_mut(at).map(|job| &mut job.work) {
                Some(Work::Put(putting)) => putting.put.as_mut(),
                Some(Work::Copy(copying)) => copying.put.as_mut(),
                _ => None,
            };
            let Some(put) = put else {
                continue;
            };
            // A driver that fails keeps its error as its outcome, which its job answers.
            if put.waits_for_room() && put.room().is_err() && put.outcome().is_none() {
                self.fail_job(at, Status::Failed)?;
            }
        }
        Ok(())
    }

    /// Each operation whose work is held, by an ask of its own out (a peer's transfer its
    /// transport keeps, or a write at this node's device) or a chunk waiting for room, held to its
    /// client's patience (research/77 D2): one past it is answered `SlowDown` and lets go of what
    /// it asked, its client gone or going; one within it tells its asker its work is held. A get
    /// replied before its body: its asks, chunk reads, are given up by its transport once their
    /// peers say nothing, and it ends unfinished as a get does. An operation waiting on its client,
    /// for its body or room to write its reply, is the transport's to judge.
    fn bound_jobs(&mut self) -> Result<(), NodeError> {
        let now = hyper_rt::futures::now_ns();
        for at in 0..self.jobs.len() {
            let Some(job) = self.jobs.get(at) else {
                continue;
            };
            if !job.working() || !job.held() {
                continue;
            }
            match job.bounded(now) {
                Bounded::SlowDown => self.slow_job(at, door::Slowed::Patience)?,
                Bounded::Held => {
                    if let Door::Exchange(exchange) = job.door {
                        self.driver.endpoint().working(exchange);
                    }
                }
                Bounded::Unvouched => {}
            }
        }
        Ok(())
    }

    /// One operation's turn; whether it is over.
    fn step_job(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get(at) else {
            return Ok(true);
        };
        match job.work {
            Work::Fields(..) => self.read_fields(at),
            Work::Draining(_) => self.drain_job(at),
            Work::Replying(..) => self.write_fields(at),
            Work::Answering(_) => self.answer_local(at),
            Work::Bucket(_) => self.step_bucket(at),
            Work::Put(_) => self.step_put(at),
            Work::Get(_) => self.step_get(at),
            Work::Command(_) => self.step_command(at),
            Work::Complete(_) => self.step_complete(at),
            Work::Read(_) => self.step_single(at),
            Work::Deletes(_) => self.step_deletes(at),
            Work::Copy(_) => self.step_copy(at),
            Work::Admin(_) => self.step_admin(at),
        }
    }

    /// The outcomes of what job `at` asked, taken from the node's answers and given to it.
    fn take_answers(&mut self, at: usize) -> Result<(), NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(());
        };
        if job.pending.is_empty() {
            return Ok(());
        }
        let mut answered: Vec<(Option<u64>, Outcome)> = Vec::new();
        job.pending.retain(|(pending, request)| {
            let outcome = match pending {
                Pending::Command(ask) => {
                    take_done(&mut self.commands_done, *ask).map(Outcome::Command)
                }
                Pending::Read(ask) => take_done(&mut self.reads_done, *ask).map(Outcome::Read),
                Pending::Chunk(ask) => take_done(&mut self.chunks_done, *ask).map(Outcome::Chunk),
            };
            match outcome {
                Some(outcome) => {
                    answered.push((*request, outcome));
                    false
                }
                None => true,
            }
        });
        for (request, outcome) in answered {
            self.answer_job(at, request, outcome)?;
        }
        Ok(())
    }

    /// Gives job `at` an outcome of what it asked.
    fn answer_job(
        &mut self,
        at: usize,
        request: Option<u64>,
        outcome: Outcome,
    ) -> Result<(), NodeError> {
        // A range step that waited its range's budget without a leader that can serve it (node.md
        // §5.4), the only end a job's steps have: the request is answered `SlowDown`, a cause that
        // passes, which clients retry.
        let leaderless = matches!(
            outcome,
            Outcome::Read(ReadOutcome::Deadline) | Outcome::Command(CommandOutcome::Deadline)
        );
        if leaderless && self.jobs.get(at).is_some_and(Job::working) {
            return self.slow_job(at, door::Slowed::Leaderless);
        }
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(());
        };
        // One of its own asks answered: the job's work moved.
        job.moved_ns = hyper_rt::futures::now_ns();
        let principal = job.principal.clone();
        match &mut job.work {
            Work::Fields(..) | Work::Draining(_) | Work::Replying(..) | Work::Answering(_) => {}
            // A native upload opened: its ID, and its progress, every run but the last as long
            // as the plan made it.
            Work::Command(commanding) if matches!(commanding.then, Then::Open { .. }) => {
                let Then::Open { run, .. } = commanding.then else {
                    return Ok(());
                };
                return match outcome {
                    Outcome::Read(ReadOutcome::Read(read)) => self.command_read(at, *read),
                    Outcome::Command(CommandOutcome::Answered(Answer::Name(
                        name::Outcome::Created { upload },
                    ))) => {
                        let reply = Reply {
                            upload,
                            progress: Some(Box::new(Progress {
                                offset: 0,
                                runs: 0,
                                run,
                                ended: false,
                                checksum: None,
                            })),
                            ..Reply::status(Status::Done)
                        };
                        self.reply_job(at, reply)
                    }
                    Outcome::Command(CommandOutcome::Answered(Answer::Name(refused))) => {
                        self.fail_job(at, name_status(&refused))
                    }
                    _ => self.fail_job(at, Status::Failed),
                };
            }
            Work::Bucket(coordinating) => {
                let answer = match outcome {
                    Outcome::Read(ReadOutcome::Read(read)) if !coordinating.owned => {
                        let status = match *read {
                            Read::Bucket(Some(row)) if row.owner == principal => None,
                            Read::Bucket(Some(_)) => Some(Status::AccessDenied),
                            Read::Bucket(None) => Some(Status::NoSuchBucket),
                            _ => Some(Status::Failed),
                        };
                        coordinating.owned = status.is_none();
                        return match status {
                            Some(status) => self.fail_job(at, status),
                            None => Ok(()),
                        };
                    }
                    Outcome::Command(CommandOutcome::Answered(Answer::Bucket(o))) => {
                        if coordinating.coordinator.is_none() {
                            return self.begun(at, o);
                        }
                        coordinator::Answer::Bucket(o)
                    }
                    Outcome::Command(CommandOutcome::Answered(Answer::Name(o))) => {
                        coordinator::Answer::Name(o)
                    }
                    Outcome::Read(ReadOutcome::Read(read)) => match *read {
                        Read::Gate(gate) => coordinator::Answer::Gate(gate),
                        Read::Probe(probe) => coordinator::Answer::Probe(probe),
                        _ => return self.fail_job(at, Status::Failed),
                    },
                    _ => return self.fail_job(at, Status::Failed),
                };
                let Some(c) = coordinating.coordinator.as_mut() else {
                    return self.fail_job(at, Status::Failed);
                };
                if c.answer(answer).is_err() {
                    return self.fail_job(at, Status::Failed);
                }
            }
            Work::Put(putting) => match (request, outcome) {
                (None, Outcome::Read(ReadOutcome::Read(read))) => {
                    return self.bucket_read(at, *read);
                }
                (None, _) => return self.fail_job(at, Status::Failed),
                (Some(id), outcome) => {
                    // An answer the driver refuses ends it with that outcome, which its next
                    // turn replies: a precondition or a lock the Name range judged, say.
                    if let Some(put) = putting.put.as_mut() {
                        let _ = put.answer(id, put_answer(outcome));
                    }
                }
            },
            Work::Get(getting) => match (request, outcome) {
                (None, Outcome::Read(ReadOutcome::Read(read))) => {
                    return self.current_read(at, *read);
                }
                (None, _) => return self.fail_job(at, Status::Failed),
                (Some(id), outcome) => {
                    // An answer the driver refuses ends it with that outcome, which its next
                    // turn ends the exchange on.
                    if let Some(get) = getting.get.as_mut() {
                        let _ = get.answer(id, get_answer(outcome));
                    }
                }
            },
            Work::Command(_) => {
                return match outcome {
                    Outcome::Read(ReadOutcome::Read(read)) => self.command_read(at, *read),
                    Outcome::Command(CommandOutcome::Answered(Answer::Name(o))) => {
                        let reply = match o {
                            name::Outcome::Deleted { marker, version } => Reply {
                                version: version.unwrap_or_default(),
                                marker,
                                ..Reply::status(Status::Done)
                            },
                            name::Outcome::Created { upload } => Reply {
                                upload,
                                ..Reply::status(Status::Done)
                            },
                            name::Outcome::Aborted => Reply::status(Status::Done),
                            refused => Reply::status(name_status(&refused)),
                        };
                        self.reply_job(at, reply)
                    }
                    Outcome::Command(CommandOutcome::Answered(Answer::Bucket(o))) => {
                        let status = match o {
                            bucket::Outcome::Versioned => Status::Done,
                            bucket::Outcome::VersioningLocked => Status::InvalidBucketState,
                            bucket::Outcome::NoSuchBucket => Status::NoSuchBucket,
                            bucket::Outcome::OperationAborted => Status::OperationAborted,
                            _ => Status::Failed,
                        };
                        self.fail_job(at, status)
                    }
                    _ => self.fail_job(at, Status::Failed),
                };
            }
            Work::Complete(completing) => match (request, outcome) {
                (None, Outcome::Read(ReadOutcome::Read(read))) => {
                    return self.complete_read(at, *read);
                }
                (None, _) => return self.fail_job(at, Status::Failed),
                (Some(id), outcome) => {
                    // A read or command refused, or answered with another kind: `500`, which
                    // clients retry; never the 404 an upload not found would be.
                    let Some(answer) = complete_answer(outcome) else {
                        return self.fail_job(at, Status::Failed);
                    };
                    // An answer the driver refuses ends it with that outcome, which its next
                    // turn replies.
                    if let Some(completion) = completing.completion.as_mut() {
                        let _ = completion.answer(id, answer);
                    }
                }
            },
            Work::Read(_) => {
                return match outcome {
                    Outcome::Read(ReadOutcome::Read(read)) => self.single_read(at, *read),
                    _ => self.fail_job(at, Status::Failed),
                };
            }
            Work::Deletes(deleting) => match (request, outcome) {
                (None, Outcome::Read(ReadOutcome::Read(read))) => {
                    return self.deletes_read(at, *read);
                }
                (None, _) => return self.fail_job(at, Status::Failed),
                (Some(index), outcome) => {
                    let index = usize::try_from(index).unwrap_or(usize::MAX);
                    deleting.out = deleting.out.saturating_sub(1);
                    let (Some(target), Some(slot)) =
                        (deleting.objects.get(index), deleting.results.get_mut(index))
                    else {
                        return self.fail_job(at, Status::Failed);
                    };
                    *slot = Some(match outcome {
                        Outcome::Command(CommandOutcome::Answered(Answer::Name(o))) => {
                            deleted(target, o)
                        }
                        // A command its range answered with another kind: S3's `500`, which
                        // its client asks again.
                        _ => Deleted {
                            key: target.key.clone(),
                            version: target.version.clone(),
                            status: Status::Failed,
                            marker: None,
                        },
                    });
                }
            },
            Work::Copy(copying) => match (request, outcome) {
                (None, Outcome::Read(ReadOutcome::Read(read))) => {
                    return self.copy_read(at, *read);
                }
                (None, _) => return self.fail_job(at, Status::Failed),
                (Some(tag), outcome) => match untagged(tag) {
                    // An answer the driver refuses ends it with that outcome, which its next
                    // turn replies.
                    (Driver::Get, id) => {
                        if let Some(get) = copying.get.as_mut() {
                            let _ = get.answer(id, get_answer(outcome));
                        }
                    }
                    (Driver::Put, id) => {
                        if let Some(put) = copying.put.as_mut() {
                            let _ = put.answer(id, put_answer(outcome));
                        }
                    }
                },
            },
            Work::Admin(_) => return self.admin_answered(at, outcome),
        }
        Ok(())
    }

    /// A bucket's create or delete answered by the Bucket range's first command: its
    /// coordinator, or the reply it gets.
    fn begun(&mut self, at: usize, outcome: bucket::Outcome) -> Result<(), NodeError> {
        let status = match outcome {
            bucket::Outcome::Creating { .. } | bucket::Outcome::Deleting { .. } => None,
            bucket::Outcome::AlreadyOwnedByYou => Some(Status::BucketAlreadyOwnedByYou),
            bucket::Outcome::AlreadyExists => Some(Status::BucketAlreadyExists),
            bucket::Outcome::TooManyBuckets => Some(Status::TooManyBuckets),
            bucket::Outcome::OperationAborted => Some(Status::OperationAborted),
            bucket::Outcome::NoSuchBucket => Some(Status::NoSuchBucket),
            _ => Some(Status::Failed),
        };
        if let Some(status) = status {
            return self.fail_job(at, status);
        }
        let Some(gateway) = self.gateway.as_ref() else {
            return self.fail_job(at, Status::Failed);
        };
        let bounds = coordinator::Bounds {
            budget: gateway.scan_rows,
            ranges: self.ranges.len(),
        };
        let Some(Job {
            work: Work::Bucket(coordinating),
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        match Coordinator::start(&coordinating.bucket, &outcome, &directory(), bounds) {
            Ok(Some(c)) => {
                coordinating.coordinator = Some(c);
                Ok(())
            }
            _ => self.fail_job(at, Status::Failed),
        }
    }

    /// A bucket's create or delete at a turn: a delete's bucket row read first, its owner checked;
    /// then its first command; then its coordinator's next request, one at a time; once the
    /// coordinator is done, its answer.
    fn step_bucket(&mut self, at: usize) -> Result<bool, NodeError> {
        let quota = self.gateway.as_ref().map_or(0, |g| g.bucket_quota);
        let Some(job) = self.jobs.get(at) else {
            return Ok(true);
        };
        if !job.pending.is_empty() {
            return Ok(false);
        }
        let Work::Bucket(coordinating) = &job.work else {
            return Ok(true);
        };
        let bucket = coordinating.bucket.clone();
        let next = match &coordinating.coordinator {
            None if coordinating.delete && !coordinating.owned => Next::Owner,
            None if coordinating.delete => {
                Next::Request(coordinator::Request::Bucket(bucket::Command::BeginDelete {
                    bucket: bucket.clone(),
                    at_ns: 0,
                }))
            }
            None => Next::Request(coordinator::Request::Bucket(bucket::Command::Create(
                bucket::Create {
                    bucket: bucket.clone(),
                    owner: job.principal.clone(),
                    location: String::new(),
                    at_ns: 0,
                    quota,
                    lock: false,
                },
            ))),
            Some(c) if c.is_done() => Next::Done(match (c.settled(), coordinating.delete) {
                (Some(Settled::Created), false) | (Some(Settled::Deleted), true) => Status::Done,
                (Some(Settled::NotEmpty), true) => Status::BucketNotEmpty,
                _ => Status::OperationAborted,
            }),
            Some(c) => match c.next() {
                Some(request) => Next::Request(request),
                None => Next::Done(Status::Failed),
            },
        };
        let (Some(bucket_group), Some(name_group)) =
            (self.group_of(Layer::Bucket), self.group_of(Layer::Name))
        else {
            self.fail_job(at, Status::Failed)?;
            return self.drain_job(at);
        };
        let asked = match next {
            Next::Done(status) => {
                self.fail_job(at, status)?;
                return self.drain_job(at);
            }
            Next::Owner => self
                .read(
                    bucket_group,
                    Query::Bucket { bucket },
                    u64::MAX,
                    Wait::Request,
                )
                .map(Pending::Read),
            Next::Request(coordinator::Request::Bucket(command)) => self
                .command(
                    bucket_group,
                    Command::Bucket(command),
                    u64::MAX,
                    Wait::Request,
                )
                .map(Pending::Command),
            Next::Request(coordinator::Request::Name { command, .. }) => self
                .command(name_group, Command::Name(command), u64::MAX, Wait::Request)
                .map(Pending::Command),
            Next::Request(coordinator::Request::Gate { generation, .. }) => self
                .read(
                    name_group,
                    Query::Gate { bucket, generation },
                    u64::MAX,
                    Wait::Request,
                )
                .map(Pending::Read),
            Next::Request(coordinator::Request::Probe {
                generation,
                from,
                budget,
                ..
            }) => {
                let query = Query::Probe {
                    bucket,
                    generation,
                    from,
                    budget: u32::try_from(budget).unwrap_or(u32::MAX),
                };
                self.read(name_group, query, u64::MAX, Wait::Request)
                    .map(Pending::Read)
            }
            // The cell's one Name range is the directory.
            Next::Request(coordinator::Request::Directory) => {
                if let Some(Job {
                    work: Work::Bucket(coordinating),
                    ..
                }) = self.jobs.get_mut(at)
                    && let Some(c) = coordinating.coordinator.as_mut()
                    && c.answer(coordinator::Answer::Directory(directory()))
                        .is_err()
                {
                    self.fail_job(at, Status::Failed)?;
                }
                return Ok(false);
            }
        };
        self.asked(at, asked, None)
    }

    /// What job `at` asked, kept to wait on; the asking's failure ends the job. An ask the node
    /// has no room for is asked again at the next turn.
    fn asked(
        &mut self,
        at: usize,
        asked: Result<Pending, NodeError>,
        request: Option<u64>,
    ) -> Result<bool, NodeError> {
        match asked {
            Ok(pending) => {
                if let Some(job) = self.jobs.get_mut(at) {
                    job.pending.push((pending, request));
                }
                Ok(false)
            }
            Err(NodeError::Asks) => Ok(false),
            Err(_) => {
                self.fail_job(at, Status::Failed)?;
                self.drain_job(at)
            }
        }
    }

    /// A put's bucket row, read: its driver made over it, or the reply `NoSuchBucket`. A native
    /// upload's run asks its upload's row next, which it is checked against (`run_read`).
    fn bucket_read(&mut self, at: usize, read: Read) -> Result<(), NodeError> {
        let read = match read {
            Read::Upload(upload) => return self.run_read(at, upload),
            read => read,
        };
        let Read::Bucket(Some(row)) = read else {
            return self.fail_job(at, Status::NoSuchBucket);
        };
        if row.state != BucketState::Active {
            return self.fail_job(at, Status::NoSuchBucket);
        }
        if let Some(Job {
            work: Work::Put(putting),
            ..
        }) = self.jobs.get_mut(at)
            && let Some(appending) = putting.run.as_mut()
        {
            appending.incarnation = Some(row.created_ns);
            let query = Query::Upload {
                bucket: putting.bucket.clone(),
                key: putting.key.clone(),
                upload: putting
                    .part
                    .as_ref()
                    .map(|(upload, _)| upload.clone())
                    .unwrap_or_default(),
            };
            let Some(group) = self.group_of(Layer::Name) else {
                return self.fail_job(at, Status::Failed);
            };
            let asked = self
                .read(group, query, u64::MAX, Wait::Request)
                .map(Pending::Read);
            return self.asked(at, asked, None).map(|_| ());
        }
        let default = row.lock.and_then(|lock| lock.default);
        let Some(Job {
            work: Work::Put(putting),
            principal,
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        let commit = match &putting.part {
            // The part's ETag, size, checksum, file and deadline are the driver's to fill.
            Some((upload, number)) => Commit::Part(name::PutPart {
                bucket: putting.bucket.clone(),
                incarnation: row.created_ns,
                key: putting.key.clone(),
                upload: upload.clone(),
                number: *number,
                part: unfilled_part(),
                at_ns: 0,
                run: None,
            }),
            None => Commit::Object(name::Put {
                bucket: putting.bucket.clone(),
                incarnation: row.created_ns,
                key: putting.key.clone(),
                versioning: row.versioning,
                preconditions: std::mem::take(&mut putting.preconditions),
                at_ns: 0,
                ordered_ns: None,
                version: Version {
                    marker: false,
                    null: false,
                    modified_ns: 0,
                    etag: String::new(),
                    size: 0,
                    checksum: None,
                    file: None,
                    owner: principal.clone(),
                    headers: std::mem::take(&mut putting.metadata),
                    retention: None,
                    legal_hold: None,
                    listing: None,
                },
                default,
                id: putting.id,
                deadline_ns: 0,
            }),
        };
        self.start_put(at, commit)
    }

    /// A native upload's run, its upload's row read (gateway.md §2.1): begun at the offset the
    /// row holds, as long as the plan's runs or, the last, no longer, numbered the next; or the
    /// reply it earns, one at any other offset or after the last run `Offset`, naming the
    /// upload's.
    fn run_read(&mut self, at: usize, upload: Option<Upload>) -> Result<(), NodeError> {
        let Some(Job {
            work: Work::Put(putting),
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        let Some(upload) = upload else {
            return self.fail_job(at, Status::NoSuchUpload);
        };
        // An upload CreateMultipartUpload made takes its client's parts, never runs.
        let (Some(runs), Some(appending)) = (upload.runs, putting.run.as_mut()) else {
            return self.fail_job(at, Status::InvalidRequest);
        };
        appending.runs = Some(runs);
        if runs.ended || appending.offset != runs.offset {
            let reply = Reply {
                progress: Some(Box::new(progress(&runs, None))),
                ..Reply::status(Status::Offset)
            };
            return self.reply_job(at, reply);
        }
        let (offset, last) = (appending.offset, appending.last);
        let Some(incarnation) = appending.incarnation else {
            return self.fail_job(at, Status::Failed);
        };
        // The run's checksum is in the algorithm its object is held to at its end.
        if putting.trailing.map(Algorithm::code) != upload.checksum.map(|(code, _)| code) {
            return self.fail_job(at, Status::InvalidRequest);
        }
        let end = offset.checked_add(putting.length);
        let planned = match end {
            None => false,
            Some(end) if last => {
                putting.length <= runs.run && runs.length.is_none_or(|length| end == length)
            }
            Some(end) => {
                putting.length == runs.run && runs.length.is_none_or(|length| end < length)
            }
        };
        let number = runs
            .count
            .checked_add(1)
            .filter(|n| *n <= name::MAX_PART_NUMBER);
        let (true, Some(number), Some((upload_id, numbered))) =
            (planned, number, putting.part.as_mut())
        else {
            return self.fail_job(at, Status::InvalidArgument);
        };
        *numbered = number;
        let commit = Commit::Run {
            part: name::PutPart {
                bucket: putting.bucket.clone(),
                incarnation,
                key: putting.key.clone(),
                upload: upload_id.clone(),
                number,
                part: unfilled_part(),
                at_ns: 0,
                run: Some(name::Run {
                    offset,
                    last,
                    md5: runs.md5,
                }),
            },
            // The run's own MD5, its part's ETag, for the multipart ETag of an object that may
            // pass one PUT's bytes.
            own: runs
                .length
                .is_none_or(|length| length > mantle_s3::body::MAX_UPLOAD),
        };
        self.start_put(at, commit)
    }

    /// A put's driver, made over `commit`: its file's ID and data key drawn, under the root key's
    /// current generation, and its blocks laid out by the scheme new blocks take; or the reply
    /// its failure earns.
    fn start_put(&mut self, at: usize, commit: Commit) -> Result<(), NodeError> {
        let Ok(Some(scheme)) = self.table().and_then(Table::scheme) else {
            return self.fail_job(at, Status::Failed);
        };
        let now = self.wall.now_ns();
        let Some(gateway) = self.gateway.as_ref() else {
            return self.fail_job(at, Status::Failed);
        };
        let Some((generation, root)) = gateway.keys.last() else {
            return self.fail_job(at, Status::Failed);
        };
        let holding = Holding {
            handover_ns: gateway.handover_ns,
            window: self.jobs.get(at).map_or(gateway.window, |job| job.window),
        };
        let made = (|| {
            let data = DataKey::generate().ok()?;
            let wrapped = WrappedKey {
                by: Wrapper::Root(*generation),
                bytes: data.wrap(root).ok()?,
            };
            let mut file = [0u8; 16];
            getrandom::fill(&mut file).ok()?;
            let keys = Keys {
                file: u128::from_be_bytes(file),
                data,
                wrapped,
            };
            Some((keys, Layout::new(scheme).ok()?))
        })();
        let Some((keys, layout)) = made else {
            return self.fail_job(at, Status::Failed);
        };
        let Some(Job {
            work: Work::Put(putting),
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        let body = Body {
            length: putting.length,
            checksum: putting
                .expected
                .checksum
                .as_ref()
                .map(|c| c.algorithm)
                .or(putting.trailing),
            content_md5: putting.expected.md5.is_some(),
        };
        match Put::new(commit, body, keys, layout, holding, Box::new(Random), now) {
            Ok(put) => {
                putting.put = Some(put);
                Ok(())
            }
            Err(error) => self.reply_job(at, put_failed(&error)),
        }
    }

    /// A put's turn: its bucket's row read first; then its body read as far as the driver takes
    /// it, its driver's time moved and its requests asked; once its outcome is known, its answer.
    fn step_put(&mut self, at: usize) -> Result<bool, NodeError> {
        let wall = self.wall.now_ns();
        let state = {
            let Some(Job {
                work: Work::Put(putting),
                pending,
                ..
            }) = self.jobs.get_mut(at)
            else {
                return Ok(true);
            };
            match putting.put.as_mut() {
                None if pending.is_empty() => Step::Ask(putting.bucket.clone()),
                None => Step::Wait,
                Some(put) => {
                    let run = putting.run.as_ref();
                    let over = put.outcome().map(|outcome| match outcome {
                        Ok(stored) => Reply {
                            version: stored.version.clone().unwrap_or_default(),
                            etag: stored.etag.clone(),
                            size: stored.size,
                            checksum: stored.checksum.as_ref().map(kept),
                            // A run's reply is its progress frame.
                            progress: run.and_then(|r| run_frame(r, stored)).map(Box::new),
                            ..Reply::status(Status::Done)
                        },
                        // Another run committed at its offset first: the offset it left.
                        Err(PutError::Name(name::Outcome::Offset { offset, ended })) => Reply {
                            progress: run.and_then(|r| r.runs).map(|runs| {
                                Box::new(Progress {
                                    offset: *offset,
                                    ended: *ended,
                                    ..progress(&runs, None)
                                })
                            }),
                            ..Reply::status(Status::Offset)
                        },
                        Err(error) => put_failed(error),
                    });
                    // What its blocks took to be recorded and its body to come, for the windows the
                    // node's next puts hold (`crate::window`).
                    if let Some(ns) = put.take_committed() {
                        self.put_window.committed(ns);
                    }
                    if let Some((bytes, ns)) = put.take_filled() {
                        self.put_window.filled(bytes, ns);
                    }
                    match over {
                        Some(reply) => Step::Over(Box::new(reply)),
                        None if put.tick(wall).is_err() => {
                            Step::Over(Box::new(Reply::status(Status::Failed)))
                        }
                        None => Step::Go,
                    }
                }
            }
        };
        match state {
            Step::Wait => return Ok(false),
            Step::Over(reply) => {
                if let Some(job) = self.jobs.get_mut(at) {
                    job.work = Work::Draining(reply);
                }
                return self.drain_job(at);
            }
            Step::Ask(bucket) => {
                let Some(group) = self.group_of(Layer::Bucket) else {
                    self.fail_job(at, Status::Failed)?;
                    return self.drain_job(at);
                };
                let asked = self
                    .read(group, Query::Bucket { bucket }, u64::MAX, Wait::Request)
                    .map(Pending::Read);
                return self.asked(at, asked, None);
            }
            Step::Go => {}
        }
        self.feed_put(at)?;
        // The driver's requests, each asked of what serves it.
        loop {
            let Some(job) = self.jobs.get_mut(at) else {
                return Ok(true);
            };
            let Work::Put(putting) = &mut job.work else {
                return self.moved_on(at);
            };
            let next = match putting.waiting.pop_front() {
                Some(request) => Some(request),
                None => putting.put.as_mut().and_then(Put::poll),
            };
            let Some((id, request)) = next else {
                break;
            };
            // Not asked: the node holds as many asks as it keeps. It waits, in its place.
            if let Some(request) = self.ask_put(at, id, request)? {
                if let Some(Job {
                    work: Work::Put(putting),
                    ..
                }) = self.jobs.get_mut(at)
                {
                    putting.waiting.push_front((id, request));
                }
                break;
            }
        }
        Ok(false)
    }

    /// Reads a put's body into its driver as far as the driver takes it: what it left of the
    /// last piece first, then a piece at a time; at the body's end, the body ended, its digests
    /// checked against what the request said before anything commits.
    fn feed_put(&mut self, at: usize) -> Result<(), NodeError> {
        if self
            .jobs
            .get(at)
            .is_some_and(|j| matches!(j.door, Door::Local(_)))
        {
            return self.feed_local_put(at);
        }
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(());
        };
        let Door::Exchange(exchange) = job.door else {
            return Ok(());
        };
        let Work::Put(putting) = &mut job.work else {
            return Ok(());
        };
        let Some(put) = putting.put.as_mut() else {
            return Ok(());
        };
        let endpoint = self.driver.endpoint();
        // A trailer's value follows the object's bytes: the driver is fed those alone.
        let width = putting.trailing.map_or(0, |a| a.width());
        loop {
            let left = usize::try_from(putting.length.saturating_sub(putting.taken))
                .unwrap_or(usize::MAX)
                .min(putting.carry.len());
            if left > 0 {
                if !put.wants_body() {
                    return Ok(());
                }
                let took = match putting.carry.get(..left).map(|bytes| put.feed(bytes)) {
                    Some(Ok(took)) => took,
                    _ => return self.fail_job(at, Status::Failed),
                };
                putting.taken = putting
                    .taken
                    .saturating_add(u64::try_from(took).unwrap_or(u64::MAX));
                putting.carry.drain(..took.min(putting.carry.len()));
                if took < left {
                    return Ok(());
                }
            }
            if putting.taken >= putting.length && !putting.carry.is_empty() {
                // Past the object's bytes, the trailer's; the transport holds the body to the
                // length its head stated, the trailer's width within it.
                if putting.trailer.len().saturating_add(putting.carry.len()) > width {
                    return self.fail_job(at, Status::Malformed);
                }
                putting.trailer.append(&mut putting.carry);
            }
            if putting.taken >= putting.length && putting.trailer.len() == width {
                if endpoint.body_complete(exchange) || (putting.length == 0 && width == 0) {
                    if let Some(algorithm) = putting.trailing {
                        putting.expected.checksum = Some(checksum::Checksum {
                            algorithm,
                            bytes: std::mem::take(&mut putting.trailer),
                        });
                    }
                    if put.end(&putting.expected).is_err() && put.outcome().is_none() {
                        return self.fail_job(at, Status::Failed);
                    }
                    // Ended once: nothing more is read, and the outcome answers it.
                    putting.length = u64::MAX;
                } else {
                    // The body's checksum, after its last byte.
                    let mut empty = hyper_transport::Reservation::default();
                    let _ = endpoint.read_body(exchange, &mut empty);
                }
                return Ok(());
            }
            if putting.taken < putting.length && !put.wants_body() {
                return Ok(());
            }
            let Ok(mut piece) = endpoint.reserve(Class::Transfer, PIECE) else {
                return Ok(());
            };
            let got = endpoint.read_body(exchange, &mut piece);
            putting.carry.extend_from_slice(piece.bytes());
            self.moved = self
                .moved
                .saturating_add(u64::try_from(piece.bytes().len()).unwrap_or(u64::MAX));
            endpoint.release(piece);
            match got {
                Ok(0) if putting.carry.is_empty() => return Ok(()),
                Ok(_) => {}
                Err(_) => return self.fail_job(at, Status::IncompleteBody),
            }
        }
    }

    /// A put's body from a local door (`crate::door`): the listener told once the driver first
    /// wants a byte, so a client that waits for `100 Continue` sends none before every check
    /// that could refuse it passed; each piece fed to the driver as it wants it, only what it did
    /// not take kept; and the body's end taken only once the listener checked its signature and
    /// its digest, its trailer's checksum then the one the end is checked against.
    fn feed_local_put(&mut self, at: usize) -> Result<(), NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(());
        };
        let (Door::Local(local), Work::Put(putting)) = (&mut job.door, &mut job.work) else {
            return Ok(());
        };
        let Some(put) = putting.put.as_mut() else {
            return Ok(());
        };
        if !local.ready && put.wants_body() {
            local.ready = true;
            if local.offer(door::Answer::Ready) == Sent::Gone {
                return self.fail_job(at, Status::IncompleteBody);
            }
        }
        loop {
            if !putting.carry.is_empty() {
                if !put.wants_body() {
                    return Ok(());
                }
                let took = match put.feed(&putting.carry) {
                    Ok(took) => took,
                    Err(_) => return self.fail_job(at, Status::Failed),
                };
                putting.taken = putting
                    .taken
                    .saturating_add(u64::try_from(took).unwrap_or(u64::MAX));
                putting.carry.drain(..took.min(putting.carry.len()));
                if !putting.carry.is_empty() {
                    return Ok(());
                }
            }
            if let Some(trailer) = local.ended.take() {
                // The listener framed the body at its stated length: an end before or past it is
                // a body that does not hold.
                if putting.taken != putting.length {
                    return self.fail_job(at, Status::IncompleteBody);
                }
                if let Some(checksum) = trailer {
                    putting.expected.checksum = Some(checksum);
                }
                if put.end(&putting.expected).is_err() && put.outcome().is_none() {
                    return self.fail_job(at, Status::Failed);
                }
                // Ended once: nothing more is read, and the outcome answers it.
                putting.length = u64::MAX;
                return Ok(());
            }
            if putting.length == u64::MAX || !put.wants_body() {
                return Ok(());
            }
            match local.body.try_recv() {
                Ok(Some(Piece::Bytes(bytes))) => {
                    let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                    self.moved = self.moved.saturating_add(len);
                    let took = match put.feed(&bytes) {
                        Ok(took) => took,
                        Err(_) => return self.fail_job(at, Status::Failed),
                    };
                    putting.taken = putting
                        .taken
                        .saturating_add(u64::try_from(took).unwrap_or(u64::MAX));
                    if let Some(rest) = bytes.get(took..) {
                        putting.carry.extend_from_slice(rest);
                    }
                }
                Ok(Some(Piece::End(trailer))) => local.ended = Some(trailer),
                Ok(Some(Piece::Refused(status))) => return self.fail_job(at, status),
                Ok(None) => return Ok(()),
                // The connection is gone with the body unfinished.
                Err(_) => return self.fail_job(at, Status::IncompleteBody),
            }
        }
    }

    /// Asks a put's driver's request of what serves it; the request back when the node holds as
    /// many asks as it keeps.
    fn ask_put(
        &mut self,
        at: usize,
        id: u64,
        request: put::Request,
    ) -> Result<Option<put::Request>, NodeError> {
        let Some(kept) = self.kept_id(at, Driver::Put, id) else {
            self.fail_job(at, Status::Failed)?;
            return Ok(None);
        };
        // A command goes to its range as the driver made it, moved, once the node has room
        // for it; without room it waits whole for the next turn.
        let request = match request {
            put::Request::Block(command) if self.command_room() => {
                return self.ask_command(at, kept, Layer::Block, Command::Block(command));
            }
            put::Request::File(command) if self.command_room() => {
                return self.ask_command(at, kept, Layer::File, Command::File(command));
            }
            put::Request::Name(command) if self.command_room() => {
                return self.ask_command(at, kept, Layer::Name, Command::Name(command));
            }
            request => request,
        };
        let asked = match &request {
            put::Request::Place { block, .. } => {
                // Only volumes that take chunks now, their nodes seen, weighted by measured
                // availability (docs/design/repair.md §5).
                let now = self.wall.now_ns();
                let Ok(volumes) = self
                    .table()
                    .map(|t| place::offer(*block, &t.facts, &[], false, now))
                else {
                    self.fail_job(at, Status::Failed)?;
                    return Ok(None);
                };
                // Refused, the driver ends with that outcome, which its next turn replies.
                if let Some(put) = self.put_of(at) {
                    let _ = put.answer(id, put::Answer::Volumes(volumes));
                }
                return Ok(None);
            }
            put::Request::Chunk {
                volume,
                key,
                bytes,
                crc,
            } => {
                let Ok(node) = self.table().map(|t| t.node_of(*volume)) else {
                    self.fail_job(at, Status::Failed)?;
                    return Ok(None);
                };
                let Some(node) = node else {
                    // A volume the table no longer holds refuses it: the next is asked.
                    if let Some(put) = self.put_of(at) {
                        let _ = put.answer(id, put::Answer::Refused { full: false });
                    }
                    return Ok(None);
                };
                let remaining = self.remaining_of(at);
                // The replicas' one copy of the bytes, shared (a count, not a copy): each
                // replica's put writes or sends from it.
                self.put_chunk(node, *volume, *key, bytes.clone(), (*crc, remaining))
                    .map(Pending::Chunk)
            }
            put::Request::Block(_) | put::Request::File(_) | put::Request::Name(_) => {
                Err(NodeError::Asks)
            }
        };
        match asked {
            Ok(pending) => {
                if let Some(job) = self.jobs.get_mut(at) {
                    job.pending.push((pending, Some(kept)));
                }
                Ok(None)
            }
            Err(NodeError::Asks) => Ok(Some(request)),
            Err(_) => {
                self.fail_job(at, Status::Failed)?;
                Ok(None)
            }
        }
    }

    /// Asks `command` of `layer`'s range for request `id` of job `at`, the node having room for
    /// it; a refusal fails the job.
    fn ask_command<R>(
        &mut self,
        at: usize,
        id: u64,
        layer: Layer,
        command: Command,
    ) -> Result<Option<R>, NodeError> {
        let asked = self
            .group_of(layer)
            .ok_or(NodeError::Bounds(
                "no range of the command's layer here".into(),
            ))
            .and_then(|g| self.command(g, command, u64::MAX, Wait::Request));
        match asked {
            Ok(ask) => {
                if let Some(job) = self.jobs.get_mut(at) {
                    job.pending.push((Pending::Command(ask), Some(id)));
                }
            }
            Err(_) => self.fail_job(at, Status::Failed)?,
        }
        Ok(None)
    }

    /// A get's or head's current version, read: its preconditions judged and its range resolved
    /// against it (05 §2.4, §12); then a head's facts replied, or a get's driver made over the
    /// range and the reply's head sent.
    fn current_read(&mut self, at: usize, read: Read) -> Result<(), NodeError> {
        let (order, version) = match read {
            Read::NoBucket => return self.fail_job(at, Status::NoSuchBucket),
            Read::Current(None) => return self.fail_job(at, Status::NoSuchKey),
            Read::Current(Some(found)) => found,
            Read::Version(None) => return self.fail_job(at, Status::NoSuchVersion),
            // A named version that is a delete marker: "the response returns a `405 Method Not
            // Allowed` error and the `Last-Modified: timestamp` response header" (research/89
            // §2.1), the marker's facts with it.
            Read::Version(Some((order, marker))) if marker.marker => {
                let reply = facts(Status::MethodNotAllowed, order, &marker);
                return self.reply_job(at, reply);
            }
            Read::Version(Some(found)) => found,
            _ => return self.fail_job(at, Status::Failed),
        };
        let Some(gateway) = self.gateway.as_ref() else {
            return self.fail_job(at, Status::Failed);
        };
        let window = gateway.window;
        let keyring = RootKeys {
            keys: gateway
                .keys
                .iter()
                .map(|(g, key)| (*g, *key.bytes()))
                .collect(),
        };
        let Some(Job {
            work: Work::Get(getting),
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        let mut reply = facts(Status::Done, order, &version);
        let current = if version.marker {
            Current::DeleteMarker
        } else {
            Current::Present {
                etag: &version.etag,
                modified: i64::try_from(reply.modified).unwrap_or(i64::MAX),
            }
        };
        reply.status = match conditional::read(&getting.headers.conditions(), &current) {
            conditional::Outcome::Proceed => Status::Done,
            // A current delete marker is no object: 404 with `x-amz-delete-marker` (05 §7.4).
            conditional::Outcome::NotFound => Status::NoSuchKey,
            conditional::Outcome::NotModified => Status::NotModified,
            conditional::Outcome::Failed => Status::PreconditionFailed,
        };
        if reply.status != Status::Done {
            if reply.status == Status::PreconditionFailed {
                reply = Reply::status(Status::PreconditionFailed);
            }
            return self.reply_job(at, reply);
        }
        let size = version.size;
        let served = match range::span(getting.headers.range.as_deref(), size) {
            Span::Whole => 0..size,
            Span::Part { first, last } => {
                reply.range = Some((first, last));
                first..last.saturating_add(1)
            }
            Span::Unsatisfiable => {
                let refused = Reply {
                    size,
                    ..Reply::status(Status::InvalidRange)
                };
                return self.reply_job(at, refused);
            }
        };
        if getting.head {
            return self.reply_job(at, reply);
        }
        let length = served.end.saturating_sub(served.start);
        // One GET returns at most S3's single GET; an object past it is read in ranges (05 §4.1).
        if length > mantle_s3::body::MAX_GET {
            return self.fail_job(at, Status::EntityTooLarge);
        }
        match Get::new(version.file, size, served, Box::new(keyring), window) {
            Ok(get) => getting.get = Some(get),
            Err(_) => return self.fail_job(at, Status::Failed),
        }
        // The reply waits for the range's first bytes (`write_get`).
        getting.total = length;
        getting.held = Some(Box::new(reply));
        Ok(())
    }

    /// Sends a get's held reply once its driver has the range's first bytes or its end: its head,
    /// and natively its fields opening the body, ahead of the range's bytes. A driver that failed
    /// before is answered with the refusal its failure is (`get_status`), no byte sent. Whether the
    /// job is over.
    fn send_held(&mut self, at: usize) -> Result<Option<bool>, NodeError> {
        let Some(Job {
            work: Work::Get(getting),
            door,
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(Some(true));
        };
        let Some(get) = getting.get.as_ref() else {
            return Ok(Some(false));
        };
        if getting.held.is_none() {
            return Ok(None);
        }
        if let Some(Err(e)) = get.outcome() {
            let failed = get_failed(e);
            getting.held = None;
            self.reply_job(at, failed)?;
            return Ok(Some(false));
        }
        if !get.ready() && get.outcome().is_none() {
            return Ok(Some(false));
        }
        let Some(reply) = getting.held.take() else {
            return Ok(None);
        };
        let length = getting.total;
        let exchange = match door {
            Door::Exchange(exchange) => *exchange,
            // The reply goes first, its bytes after it as the driver gives them.
            Door::Local(local) => {
                if local.offer(door::Answer::Head(reply)) == Sent::Gone {
                    self.fail_job(at, Status::Failed)?;
                    return Ok(Some(false));
                }
                return Ok(None);
            }
        };
        let Ok((head, fields)) = reply.encode() else {
            self.fail_job(at, Status::Failed)?;
            return Ok(Some(false));
        };
        // The fields open the body, ahead of the range's bytes.
        let total = u64::try_from(fields.len())
            .unwrap_or(u64::MAX)
            .saturating_add(length);
        getting.total = total;
        getting.out = Some((Bytes::from(fields), 0));
        match self.driver.endpoint().reply(exchange, &head, Some(total)) {
            Ok(()) => Ok(None),
            // Its asker is gone: the job ends with its exchange.
            Err(_) => {
                self.fail_job(at, Status::Failed)?;
                Ok(Some(false))
            }
        }
    }

    /// A get's or head's turn: its current version read first; then a get's driver's requests
    /// asked and its bytes written as far as the exchange takes them; once every byte is written,
    /// its end.
    fn step_get(&mut self, at: usize) -> Result<bool, NodeError> {
        let name = self.group_of(Layer::Name);
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        let Work::Get(getting) = &mut job.work else {
            return Ok(true);
        };
        if getting.get.is_none() {
            if !job.pending.is_empty() {
                return Ok(false);
            }
            let query = match getting.version {
                None => Query::Current {
                    bucket: getting.bucket.clone(),
                    key: getting.key.clone(),
                },
                Some(named) => Query::Version {
                    bucket: getting.bucket.clone(),
                    key: getting.key.clone(),
                    named,
                },
            };
            let Some(group) = name else {
                self.fail_job(at, Status::Failed)?;
                return self.drain_job(at);
            };
            let asked = self
                .read(group, query, u64::MAX, Wait::Request)
                .map(Pending::Read);
            return self.asked(at, asked, None);
        }
        if self.write_get(at)? {
            return Ok(true);
        }
        loop {
            let Some(job) = self.jobs.get_mut(at) else {
                return Ok(true);
            };
            let Work::Get(getting) = &mut job.work else {
                return self.moved_on(at);
            };
            let next = match getting.waiting.pop_front() {
                Some(request) => Some(request),
                None => getting.get.as_mut().and_then(Get::poll),
            };
            let Some((id, request)) = next else {
                break;
            };
            if let Some(request) = self.ask_get(at, id, request)? {
                if let Some(Job {
                    work: Work::Get(getting),
                    ..
                }) = self.jobs.get_mut(at)
                {
                    getting.waiting.push_front((id, request));
                }
                break;
            }
        }
        Ok(false)
    }

    /// Writes a get's bytes as far as its exchange takes them, taking more from the driver as the
    /// last is written; whether the get is over: every byte written, or its driver or exchange
    /// failed, which ends the exchange unfinished.
    fn write_get(&mut self, at: usize) -> Result<bool, NodeError> {
        if let Some(over) = self.send_held(at)? {
            return Ok(over);
        }
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        let Work::Get(getting) = &mut job.work else {
            return Ok(true);
        };
        let Some(get) = getting.get.as_mut() else {
            return Ok(false);
        };
        let exchange = match &mut job.door {
            Door::Exchange(exchange) => *exchange,
            Door::Local(local) => {
                // A piece is taken from the driver only once the last is sent, so the channel holds
                // what the client has yet to read and the driver the rest of its window.
                loop {
                    match local.flush() {
                        Sent::Taken => {}
                        Sent::Held => return Ok(false),
                        Sent::Gone => return Ok(true),
                    }
                    let Some(bytes) = get.take() else {
                        break;
                    };
                    let took = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                    getting.written = getting.written.saturating_add(took);
                    self.moved = self.moved.saturating_add(took);
                    if local.offer(door::Answer::Bytes(bytes)) == Sent::Gone {
                        return Ok(true);
                    }
                }
                let last = match get.outcome() {
                    Some(Ok(())) if getting.written >= getting.total => door::Answer::End,
                    // The driver failed past the reply's head: the response ends unfinished.
                    Some(Err(_)) => door::Answer::Failed,
                    _ => return Ok(false),
                };
                if local.offer(last) == Sent::Held {
                    // The last answer waits for room: the job sends it before it ends.
                    job.work = Work::Answering(VecDeque::new());
                    return Ok(false);
                }
                return Ok(true);
            }
        };
        let endpoint = self.driver.endpoint();
        loop {
            if getting.out.is_none() {
                getting.out = get.take().map(|bytes| (bytes, 0));
            }
            let Some((bytes, written)) = getting.out.as_mut() else {
                break;
            };
            let Some(rest) = bytes.get(*written..) else {
                getting.out = None;
                continue;
            };
            if rest.is_empty() {
                getting.out = None;
                continue;
            }
            match endpoint.write_body(exchange, rest) {
                Ok(0) => return Ok(false),
                Ok(took) => {
                    let took_bytes = u64::try_from(took).unwrap_or(u64::MAX);
                    *written = written.saturating_add(took);
                    getting.written = getting.written.saturating_add(took_bytes);
                    self.moved = self.moved.saturating_add(took_bytes);
                }
                Err(_) => {
                    endpoint.refuse_exchange(exchange, Refusal::Closed);
                    return Ok(true);
                }
            }
        }
        match get.outcome() {
            Some(Ok(())) if getting.written >= getting.total => {
                endpoint.end(exchange);
                Ok(true)
            }
            // The driver failed past the reply's head: the exchange ends unfinished, which its
            // asker hears as the exchange refused, not as bytes it could take for the object's.
            Some(Err(_)) => {
                endpoint.refuse_exchange(exchange, Refusal::Closed);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Asks a get's driver's request of what serves it; the request back when the node holds as
    /// many asks as it keeps.
    fn ask_get(
        &mut self,
        at: usize,
        id: u64,
        request: get::Request,
    ) -> Result<Option<get::Request>, NodeError> {
        let asked = match &request {
            get::Request::Header { file } => self
                .group_of(Layer::File)
                .ok_or(NodeError::Bounds("no File range here".into()))
                .and_then(|g| self.read(g, Query::Header { file: *file }, u64::MAX, Wait::Request))
                .map(Pending::Read),
            get::Request::Extents { file, offset, max } => {
                let max = u32::try_from(*max).unwrap_or(u32::MAX);
                self.group_of(Layer::File)
                    .ok_or(NodeError::Bounds("no File range here".into()))
                    .and_then(|g| {
                        let query = Query::Extents {
                            file: *file,
                            offset: *offset,
                            max,
                        };
                        self.read(g, query, u64::MAX, Wait::Request)
                    })
                    .map(Pending::Read)
            }
            get::Request::Block { block } => self
                .group_of(Layer::Block)
                .ok_or(NodeError::Bounds("no Block range here".into()))
                .and_then(|g| self.read(g, Query::Block { block: *block }, u64::MAX, Wait::Request))
                .map(Pending::Read),
            get::Request::Chunk {
                volume,
                key,
                offset,
                len,
            } => {
                let Ok(node) = self.table().map(|t| t.node_of(*volume)) else {
                    self.fail_job(at, Status::Failed)?;
                    return Ok(None);
                };
                let Some(node) = node else {
                    // A volume the table no longer holds: another copy is read, or the driver
                    // ends with its outcome.
                    if let Some(get) = self.get_of(at) {
                        let _ = get.answer(id, get::Answer::Unreadable);
                    }
                    return Ok(None);
                };
                let remaining = self.remaining_of(at);
                self.get_chunk(node, *volume, *key, (*offset, *len), remaining)
                    .map(Pending::Chunk)
            }
        };
        let Some(kept) = self.kept_id(at, Driver::Get, id) else {
            self.fail_job(at, Status::Failed)?;
            return Ok(None);
        };
        match asked {
            Ok(pending) => {
                if let Some(job) = self.jobs.get_mut(at) {
                    job.pending.push((pending, Some(kept)));
                }
                Ok(None)
            }
            Err(NodeError::Asks) => Ok(Some(request)),
            Err(_) => {
                self.fail_job(at, Status::Failed)?;
                Ok(None)
            }
        }
    }

    /// The request ID job `at` keeps its driver's request `id` under: the ID itself, or, for a
    /// copy, the ID tagged with its driver (`tagged`); none past what a u64 holds.
    fn kept_id(&self, at: usize, driver: Driver, id: u64) -> Option<u64> {
        match self.jobs.get(at).map(|job| &job.work) {
            Some(Work::Copy(_)) => tagged(driver, id),
            _ => Some(id),
        }
    }

    /// Job `at`'s get driver: its get's, or its copy's.
    fn get_of(&mut self, at: usize) -> Option<&mut Get> {
        match self.jobs.get_mut(at).map(|job| &mut job.work) {
            Some(Work::Get(getting)) => getting.get.as_mut(),
            Some(Work::Copy(copying)) => copying.get.as_mut(),
            _ => None,
        }
    }

    /// Job `at`'s put driver: its put's, or its copy's.
    fn put_of(&mut self, at: usize) -> Option<&mut Put> {
        match self.jobs.get_mut(at).map(|job| &mut job.work) {
            Some(Work::Put(putting)) => putting.put.as_mut(),
            Some(Work::Copy(copying)) => copying.put.as_mut(),
            _ => None,
        }
    }

    /// A commanding operation's turn: its bucket's row read first, then its command; the
    /// command's answer replies (`answer_job`).
    fn step_command(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(Job {
            work: Work::Command(commanding),
            pending,
            ..
        }) = self.jobs.get(at)
        else {
            return Ok(true);
        };
        if !pending.is_empty() || commanding.asked {
            return Ok(false);
        }
        let Some(group) = self.group_of(Layer::Bucket) else {
            self.fail_job(at, Status::Failed)?;
            return self.drain_job(at);
        };
        let query = Query::Bucket {
            bucket: commanding.bucket.clone(),
        };
        let asked = self
            .read(group, query, u64::MAX, Wait::Request)
            .map(Pending::Read);
        self.asked(at, asked, None)
    }

    /// A commanding operation's bucket row, read: its command asked, a delete's under the
    /// bucket's incarnation and versioning (05 §7.3), a versioning change by the bucket's owner;
    /// or the reply `NoSuchBucket` or `AccessDenied`.
    fn command_read(&mut self, at: usize, read: Read) -> Result<(), NodeError> {
        let Read::Bucket(Some(row)) = read else {
            return self.fail_job(at, Status::NoSuchBucket);
        };
        if row.state != BucketState::Active {
            return self.fail_job(at, Status::NoSuchBucket);
        }
        let (Some(bucket_group), Some(name_group)) =
            (self.group_of(Layer::Bucket), self.group_of(Layer::Name))
        else {
            return self.fail_job(at, Status::Failed);
        };
        let Some(Job {
            work: Work::Command(commanding),
            principal,
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        let (group, command) = match &mut commanding.then {
            Then::Delete {
                key,
                if_match,
                named,
            } => (
                name_group,
                Command::Name(Box::new(name::Command::Delete(name::Delete {
                    bucket: commanding.bucket.clone(),
                    incarnation: row.created_ns,
                    key: key.clone(),
                    versioning: row.versioning,
                    named: *named,
                    if_match: if_match.take(),
                    at_ns: 0,
                    bypass: false,
                    owner: principal.clone(),
                }))),
            ),
            // S3 lets no other principal change a bucket's versioning unless a policy grants it.
            Then::Versioning(_) if row.owner != *principal => {
                return self.fail_job(at, Status::AccessDenied);
            }
            Then::Versioning(versioning) => (
                bucket_group,
                Command::Bucket(bucket::Command::Version {
                    bucket: commanding.bucket.clone(),
                    incarnation: row.created_ns,
                    versioning: *versioning,
                }),
            ),
            Then::CreateUpload {
                key,
                checksum,
                metadata,
            } => (
                name_group,
                Command::Name(Box::new(name::Command::CreateUpload(name::CreateUpload {
                    bucket: commanding.bucket.clone(),
                    incarnation: row.created_ns,
                    key: key.clone(),
                    at_ns: 0,
                    upload: Upload {
                        initiated_ns: 0,
                        owner: principal.clone(),
                        headers: std::mem::take(metadata),
                        checksum: checksum.map(|(algorithm, full)| (algorithm.code(), full)),
                        retention: None,
                        legal_hold: None,
                        runs: None,
                    },
                }))),
            ),
            Then::Open {
                key,
                length,
                checksum,
                run,
                metadata,
            } => {
                // The MD5 across the runs is kept while the object may be one PUT's bytes, its
                // ETag then (gateway.md §2.1), begun from MD5's initial state.
                let md5 = if length.is_none_or(|l| l <= mantle_s3::body::MAX_UPLOAD) {
                    let begun = mantle_s3::crypto::Digest::new(&mantle_s3::crypto::MD5)
                        .and_then(|d| d.state())
                        .ok()
                        .and_then(|state| <[u8; 16]>::try_from(state.chaining()).ok());
                    match begun {
                        Some(chaining) => Some(RunMd5::Running(chaining)),
                        None => return self.fail_job(at, Status::Failed),
                    }
                } else {
                    None
                };
                (
                    name_group,
                    Command::Name(Box::new(name::Command::CreateUpload(name::CreateUpload {
                        bucket: commanding.bucket.clone(),
                        incarnation: row.created_ns,
                        key: key.clone(),
                        at_ns: 0,
                        upload: Upload {
                            initiated_ns: 0,
                            owner: principal.clone(),
                            headers: std::mem::take(metadata),
                            checksum: Some((checksum.code(), true)),
                            retention: None,
                            legal_hold: None,
                            runs: Some(Runs {
                                length: *length,
                                run: *run,
                                offset: 0,
                                count: 0,
                                ended: false,
                                md5,
                            }),
                        },
                    }))),
                )
            }
            Then::Abort { key, upload } => (
                name_group,
                Command::Name(Box::new(name::Command::Abort(name::Abort {
                    bucket: commanding.bucket.clone(),
                    incarnation: row.created_ns,
                    key: key.clone(),
                    upload: upload.clone(),
                    at_ns: 0,
                }))),
            ),
        };
        commanding.asked = true;
        let asked = self
            .command(group, command, u64::MAX, Wait::Request)
            .map(Pending::Command);
        self.asked(at, asked, None).map(|_| ())
    }

    /// A completion's turn: its bucket's row read first; then its driver's requests asked; once
    /// its outcome is known, its answer.
    fn step_complete(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(Job {
            work: Work::Complete(completing),
            pending,
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(true);
        };
        let Some(completion) = completing.completion.as_mut() else {
            if !pending.is_empty() {
                return Ok(false);
            }
            let query = Query::Bucket {
                bucket: completing.bucket.clone(),
            };
            let Some(group) = self.group_of(Layer::Bucket) else {
                self.fail_job(at, Status::Failed)?;
                return self.drain_job(at);
            };
            let asked = self
                .read(group, query, u64::MAX, Wait::Request)
                .map(Pending::Read);
            return self.asked(at, asked, None);
        };
        if let Some(outcome) = completion.outcome() {
            let reply = match outcome {
                Ok(done) => Reply {
                    version: done.version.clone(),
                    etag: done.etag.clone(),
                    size: done.size,
                    checksum: done.checksum.as_ref().map(shown),
                    ..Reply::status(Status::Done)
                },
                Err(error) => Reply::status(complete_status(error)),
            };
            self.reply_job(at, reply)?;
            return self.drain_job(at);
        }
        loop {
            let Some(job) = self.jobs.get_mut(at) else {
                return Ok(true);
            };
            let Work::Complete(completing) = &mut job.work else {
                return self.moved_on(at);
            };
            let next = match completing.waiting.pop_front() {
                Some(request) => Some(request),
                None => completing.completion.as_mut().and_then(Completion::poll),
            };
            let Some((id, request)) = next else {
                break;
            };
            if let Some(request) = self.ask_complete(at, id, request)? {
                if let Some(Job {
                    work: Work::Complete(completing),
                    ..
                }) = self.jobs.get_mut(at)
                {
                    completing.waiting.push_front((id, request));
                }
                break;
            }
        }
        Ok(false)
    }

    /// A completion's bucket row, read: its driver made over it, or the reply `NoSuchBucket` or
    /// the refusal its parts' list earns before anything is asked (05 §4.4).
    fn complete_read(&mut self, at: usize, read: Read) -> Result<(), NodeError> {
        let Read::Bucket(Some(row)) = read else {
            return self.fail_job(at, Status::NoSuchBucket);
        };
        if row.state != BucketState::Active {
            return self.fail_job(at, Status::NoSuchBucket);
        }
        let Some(handover_ns) = self.gateway.as_ref().map(|g| g.handover_ns) else {
            return self.fail_job(at, Status::Failed);
        };
        let now = self.wall.now_ns();
        let mut file = [0u8; 16];
        if getrandom::fill(&mut file).is_err() {
            return self.fail_job(at, Status::Failed);
        }
        let Some(Job {
            work: Work::Complete(completing),
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        // What the Complete says of the object, its parts, ETag, size, checksum, file, listing,
        // ID and deadline, the driver derives from the upload's rows.
        let complete = name::Complete {
            bucket: completing.bucket.clone(),
            incarnation: row.created_ns,
            key: completing.key.clone(),
            upload: completing.upload.clone(),
            versioning: row.versioning,
            preconditions: std::mem::take(&mut completing.preconditions),
            at_ns: 0,
            parts: Vec::new(),
            etag: String::new(),
            size: 0,
            checksum: None,
            file: None,
            listing: [0; LISTING],
            default: row.lock.and_then(|lock| lock.default),
            id: 0,
            deadline_ns: 0,
            native: false,
        };
        let listed = std::mem::take(&mut completing.listed);
        let made = match completing.native.take() {
            // A native upload's Finish: every run, held to what its client sent.
            Some(sent) => {
                Completion::native(complete, sent, u128::from_be_bytes(file), handover_ns, now)
            }
            None => Completion::new(
                complete,
                listed,
                u128::from_be_bytes(file),
                handover_ns,
                now,
            ),
        };
        match made {
            Ok(completion) => {
                completing.completion = Some(completion);
                Ok(())
            }
            Err(error) => {
                let status = complete_status(&error);
                self.fail_job(at, status)
            }
        }
    }

    /// Asks a completion's driver's request of what serves it; the request back when the node
    /// holds as many asks as it keeps.
    fn ask_complete(
        &mut self,
        at: usize,
        id: u64,
        request: complete::Request,
    ) -> Result<Option<complete::Request>, NodeError> {
        let Some(Job {
            work: Work::Complete(completing),
            ..
        }) = self.jobs.get(at)
        else {
            return Ok(None);
        };
        let (bucket, key, upload) = (
            completing.bucket.clone(),
            completing.key.clone(),
            completing.upload.clone(),
        );
        // A completion's commands, its list of parts among them, go to their ranges moved, as
        // a put's do (`ask_put`).
        let request = match request {
            complete::Request::File(command) if self.command_room() => {
                return self.ask_command(at, id, Layer::File, Command::File(command));
            }
            complete::Request::Name(command) if self.command_room() => {
                return self.ask_command(at, id, Layer::Name, Command::Name(command));
            }
            request => request,
        };
        let asked = match &request {
            complete::Request::Upload => self
                .group_of(Layer::Name)
                .ok_or(NodeError::Bounds("no Name range here".into()))
                .and_then(|g| {
                    let query = Query::Upload {
                        bucket,
                        key,
                        upload,
                    };
                    self.read(g, query, u64::MAX, Wait::Request)
                })
                .map(Pending::Read),
            complete::Request::Parts { after, max } => self
                .group_of(Layer::Name)
                .ok_or(NodeError::Bounds("no Name range here".into()))
                .and_then(|g| {
                    let query = Query::Parts {
                        bucket,
                        key,
                        upload,
                        after: *after,
                        max: u32::try_from(*max).unwrap_or(u32::MAX),
                    };
                    self.read(g, query, u64::MAX, Wait::Request)
                })
                .map(Pending::Read),
            complete::Request::File(_) | complete::Request::Name(_) => Err(NodeError::Asks),
        };
        match asked {
            Ok(pending) => {
                if let Some(job) = self.jobs.get_mut(at) {
                    job.pending.push((pending, Some(id)));
                }
                Ok(None)
            }
            Err(NodeError::Asks) => Ok(Some(request)),
            Err(_) => {
                self.fail_job(at, Status::Failed)?;
                Ok(None)
            }
        }
    }

    /// A DeleteObjects' turn (research/89 §5): its bucket's row read first; then each object's
    /// delete asked in the order named, at most an operation's share of the node's commands out
    /// at once (`crate::derive::asks`); once every one is answered, the results, the failures
    /// alone where it is quiet.
    fn step_deletes(&mut self, at: usize) -> Result<bool, NodeError> {
        let objects = self.gateway.as_ref().map_or(1, |g| g.objects).max(1);
        // An operation's share of the commands the node holds, past its own enlistment's one.
        let share = self
            .command_asks
            .saturating_sub(1)
            .checked_div(objects)
            .unwrap_or(1)
            .max(1);
        let Some(Job {
            work: Work::Deletes(deleting),
            pending,
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(true);
        };
        let Some((incarnation, versioning)) = deleting.row else {
            if !pending.is_empty() {
                return Ok(false);
            }
            let query = Query::Bucket {
                bucket: deleting.bucket.clone(),
            };
            let Some(group) = self.group_of(Layer::Bucket) else {
                self.fail_job(at, Status::Failed)?;
                return self.drain_job(at);
            };
            let asked = self
                .read(group, query, u64::MAX, Wait::Request)
                .map(Pending::Read);
            return self.asked(at, asked, None);
        };
        let Some(group) = self.group_of(Layer::Name) else {
            self.fail_job(at, Status::Failed)?;
            return self.drain_job(at);
        };
        let principal = self.jobs.get(at).map(|j| j.principal.clone());
        loop {
            let Some(Job {
                work: Work::Deletes(deleting),
                ..
            }) = self.jobs.get_mut(at)
            else {
                return Ok(true);
            };
            if deleting.out >= share {
                break;
            }
            let index = deleting.next;
            // An object answered already, one its principal may not delete, asks nothing.
            if deleting.results.get(index).is_some_and(Option::is_some) {
                deleting.next = index.saturating_add(1);
                continue;
            }
            let Some(target) = deleting.objects.get(index) else {
                break;
            };
            // A version ID no version is named by is answered here, its delete not asked.
            let named = match named(target.version.as_deref()) {
                Ok(named) => named,
                Err(status) => {
                    let result = Deleted {
                        key: target.key.clone(),
                        version: target.version.clone(),
                        status,
                        marker: None,
                    };
                    if let Some(slot) = deleting.results.get_mut(index) {
                        *slot = Some(result);
                    }
                    deleting.next = index.saturating_add(1);
                    continue;
                }
            };
            // The ETag a delete is conditioned on, as `If-Match` conditions a DeleteObject's
            // (research/89 §5.1).
            let if_match =
                target
                    .etag
                    .as_deref()
                    .map(|etag| match conditional::named(etag, true) {
                        None => Match::Any,
                        Some(tags) => Match::Tags(tags),
                    });
            let command = Command::Name(Box::new(name::Command::Delete(name::Delete {
                bucket: deleting.bucket.clone(),
                incarnation,
                key: target.key.clone(),
                versioning,
                named,
                if_match,
                at_ns: 0,
                bypass: false,
                owner: principal.clone().unwrap_or_default(),
            })));
            match self.command(group, command, u64::MAX, Wait::Request) {
                Ok(ask) => {
                    if let Some(Job {
                        work: Work::Deletes(deleting),
                        pending,
                        ..
                    }) = self.jobs.get_mut(at)
                    {
                        pending.push((
                            Pending::Command(ask),
                            Some(u64::try_from(index).unwrap_or(u64::MAX)),
                        ));
                        deleting.out = deleting.out.saturating_add(1);
                        deleting.next = index.saturating_add(1);
                    }
                }
                // The node holds as many commands as it keeps: the rest are asked at a later turn.
                Err(NodeError::Asks) => break,
                Err(_) => {
                    self.fail_job(at, Status::Failed)?;
                    return self.drain_job(at);
                }
            }
        }
        let Some(Job {
            work: Work::Deletes(deleting),
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(true);
        };
        if deleting.out > 0 || deleting.results.iter().any(Option::is_none) {
            return Ok(false);
        }
        let quiet = deleting.quiet;
        let results = std::mem::take(&mut deleting.results)
            .into_iter()
            .flatten()
            // "In quiet mode the response includes only keys where the delete operation
            // encountered an error" (research/89 §5.2).
            .filter(|d| !quiet || d.status != Status::Done)
            .collect();
        let reply = Reply {
            listing: Some(Listing::Deleted(results)),
            ..Reply::status(Status::Done)
        };
        self.reply_job(at, reply)?;
        self.drain_job(at)
    }

    /// A DeleteObjects' bucket row, read: the incarnation and versioning its deletes are made
    /// under, or the reply `NoSuchBucket`.
    fn deletes_read(&mut self, at: usize, read: Read) -> Result<(), NodeError> {
        let Read::Bucket(Some(row)) = read else {
            return self.fail_job(at, Status::NoSuchBucket);
        };
        if row.state != BucketState::Active {
            return self.fail_job(at, Status::NoSuchBucket);
        }
        if let Some(Job {
            work: Work::Deletes(deleting),
            ..
        }) = self.jobs.get_mut(at)
        {
            deleting.row = Some((row.created_ns, row.versioning));
        }
        Ok(())
    }

    /// A copy's turn (research/89 §3–§4): its destination bucket's row read first, an
    /// UploadPartCopy's upload's row next, then the source's version; then its drivers, the
    /// get's plaintext fed to the put as the put takes it, each one's requests asked; once the
    /// put commits, its reply, a null version's time read from its mark first.
    fn step_copy(&mut self, at: usize) -> Result<bool, NodeError> {
        let wall = self.wall.now_ns();
        let Some(Job {
            work: Work::Copy(copying),
            pending,
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(true);
        };
        let read = match copying.stage {
            CopyStage::Bucket => Some((
                Layer::Bucket,
                Query::Bucket {
                    bucket: copying.bucket.clone(),
                },
            )),
            CopyStage::Upload => Some((
                Layer::Name,
                Query::Upload {
                    bucket: copying.bucket.clone(),
                    key: copying.key.clone(),
                    upload: copying
                        .part
                        .as_ref()
                        .map(|(upload, _)| upload.clone())
                        .unwrap_or_default(),
                },
            )),
            CopyStage::Source => {
                let (bucket, key, named) = copying.source.clone();
                Some((
                    Layer::Name,
                    match named {
                        None => Query::Current { bucket, key },
                        Some(named) => Query::Version { bucket, key, named },
                    },
                ))
            }
            CopyStage::Mark => Some((
                Layer::Name,
                Query::Mark {
                    bucket: copying.bucket.clone(),
                    key: copying.key.clone(),
                    id: copying.file,
                },
            )),
            CopyStage::Run => None,
        };
        if let Some((layer, query)) = read {
            if !pending.is_empty() {
                return Ok(false);
            }
            let Some(group) = self.group_of(layer) else {
                self.fail_job(at, Status::Failed)?;
                return self.drain_job(at);
            };
            let asked = self
                .read(group, query, u64::MAX, Wait::Request)
                .map(Pending::Read);
            return self.asked(at, asked, None);
        }
        // The put's outcome ends the copy: its reply, or its version's time to read first.
        let over = match (
            copying.put.as_ref().and_then(Put::outcome),
            copying.get.as_ref().and_then(Get::outcome),
        ) {
            (Some(Ok(stored)), _) => Some(Ok(stored.clone())),
            // The digests a copy is held to are its source's own, never a client's: bytes that
            // miss them are the store's failure, not the request's `400 BadDigest`.
            (Some(Err(PutError::BadDigest | PutError::BadChecksum)), _) => {
                Some(Err(Reply::status(Status::Failed)))
            }
            (Some(Err(error)), _) => Some(Err(put_failed(error))),
            (None, Some(Err(error))) => Some(Err(get_failed(error))),
            _ => None,
        };
        match over {
            Some(Err(failed)) => {
                self.reply_job(at, failed)?;
                return self.drain_job(at);
            }
            Some(Ok(stored)) => {
                let made = stored.version.clone();
                let reply = match (copying.part.is_some(), made.as_deref()) {
                    // A part's row records the time its put committed it.
                    (true, _) => stored
                        .modified_ns
                        .map(|ns| copied(&stored, ns, &copying.source_version)),
                    // A version's order is its time inverted (`mantle_meta::name::put`).
                    (false, Some(id)) if id != NULL_VERSION => parse_version_id(id)
                        .map(|order| copied(&stored, !order, &copying.source_version)),
                    // A null version's ID names no order: its mark does.
                    (false, _) => {
                        copying.stored = Some(stored);
                        copying.stage = CopyStage::Mark;
                        return Ok(false);
                    }
                };
                let reply = reply.unwrap_or_else(|| Reply::status(Status::Failed));
                self.reply_job(at, reply)?;
                return self.drain_job(at);
            }
            None => {}
        }
        if let Some(put) = copying.put.as_mut()
            && put.tick(wall).is_err()
            && put.outcome().is_none()
        {
            self.fail_job(at, Status::Failed)?;
            return self.drain_job(at);
        }
        self.feed_copy(at);
        // Each driver's requests, each asked of what serves it.
        for driver in [Driver::Get, Driver::Put] {
            loop {
                let Some(job) = self.jobs.get_mut(at) else {
                    return Ok(true);
                };
                let Work::Copy(copying) = &mut job.work else {
                    return self.moved_on(at);
                };
                match driver {
                    Driver::Get => {
                        let next = match copying.get_waiting.pop_front() {
                            Some(request) => Some(request),
                            None => copying.get.as_mut().and_then(Get::poll),
                        };
                        let Some((id, request)) = next else {
                            break;
                        };
                        if let Some(request) = self.ask_get(at, id, request)? {
                            if let Some(Job {
                                work: Work::Copy(copying),
                                ..
                            }) = self.jobs.get_mut(at)
                            {
                                copying.get_waiting.push_front((id, request));
                            }
                            break;
                        }
                    }
                    Driver::Put => {
                        let next = match copying.put_waiting.pop_front() {
                            Some(request) => Some(request),
                            None => copying.put.as_mut().and_then(Put::poll),
                        };
                        let Some((id, request)) = next else {
                            break;
                        };
                        if let Some(request) = self.ask_put(at, id, request)? {
                            if let Some(Job {
                                work: Work::Copy(copying),
                                ..
                            }) = self.jobs.get_mut(at)
                            {
                                copying.put_waiting.push_front((id, request));
                            }
                            break;
                        }
                    }
                }
            }
        }
        Ok(false)
    }

    /// A copy's plaintext, from its get to its put as far as the put takes it; once the get has
    /// given every byte, the put's body ended, held to the source's own MD5 and checksum where the
    /// copy is of the whole source (research/89 §3.4). A driver that fails keeps its error as its
    /// outcome, which the copy's next turn replies.
    fn feed_copy(&mut self, at: usize) {
        let Some(Job {
            work: Work::Copy(copying),
            ..
        }) = self.jobs.get_mut(at)
        else {
            return;
        };
        let (Some(get), Some(put)) = (copying.get.as_mut(), copying.put.as_mut()) else {
            return;
        };
        loop {
            if copying.carry.is_none() {
                copying.carry = get.take().map(|bytes| (bytes, 0));
            }
            let Some((bytes, took)) = copying.carry.as_mut() else {
                break;
            };
            let rest = bytes.get(*took..).unwrap_or_default();
            if rest.is_empty() {
                copying.carry = None;
                continue;
            }
            if !put.wants_body() {
                return;
            }
            // A feed or an end that fails ends the put with its error, which its outcome holds
            // and the copy's step answers (`Put::guard`).
            match put.feed(rest) {
                Ok(0) | Err(_) => return,
                Ok(n) => *took = took.saturating_add(n),
            }
        }
        if !copying.ended && matches!(get.outcome(), Some(Ok(()))) {
            copying.ended = true;
            let _ = put.end(&copying.expected);
        }
    }

    /// What a copy read: its destination bucket's row, an UploadPartCopy's upload's, its
    /// source's version, or a null version's mark.
    fn copy_read(&mut self, at: usize, read: Read) -> Result<(), NodeError> {
        let Some(Job {
            work: Work::Copy(copying),
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        match (copying.stage, read) {
            (CopyStage::Bucket, Read::Bucket(Some(row))) if row.state == BucketState::Active => {
                copying.stage = if copying.part.is_some() {
                    CopyStage::Upload
                } else {
                    CopyStage::Source
                };
                copying.row = Some(row);
                Ok(())
            }
            (CopyStage::Bucket, _) | (CopyStage::Upload, Read::NoBucket) => {
                self.fail_job(at, Status::NoSuchBucket)
            }
            (CopyStage::Upload, Read::Upload(Some(upload))) if upload.runs.is_none() => {
                copying.upload = Some(upload);
                copying.stage = CopyStage::Source;
                Ok(())
            }
            // A native upload's parts are its runs, which no copy makes.
            (CopyStage::Upload, Read::Upload(Some(_))) => self.fail_job(at, Status::InvalidRequest),
            (CopyStage::Upload, Read::Upload(None)) => self.fail_job(at, Status::NoSuchUpload),
            (CopyStage::Source, read) => self.copy_source_read(at, read),
            (
                CopyStage::Mark,
                Read::Mark(Some(record::Mark {
                    how: How::Version { order, .. },
                    ..
                })),
            ) => {
                let reply = match &copying.stored {
                    Some(stored) => copied(stored, !order, &copying.source_version),
                    None => Reply::status(Status::Failed),
                };
                self.reply_job(at, reply)
            }
            _ => self.fail_job(at, Status::Failed),
        }
    }

    /// A copy's source version, read: judged by its copy-source conditions (research/89 §3.3),
    /// what may be copied and how much, then the copy's drivers made, a get of the source's span
    /// and a put of the destination's object or part, its metadata the source's or the request's
    /// by the directive (research/89 §3.2).
    fn copy_source_read(&mut self, at: usize, read: Read) -> Result<(), NodeError> {
        let (order, version) = match read {
            Read::NoBucket => return self.fail_job(at, Status::NoSuchBucket),
            Read::Current(None) => return self.fail_job(at, Status::NoSuchKey),
            // "If the current version is a delete marker, Amazon S3 behaves as if the object was
            // deleted" (research/89 §3.1).
            Read::Current(Some((_, current))) if current.marker => {
                return self.fail_job(at, Status::NoSuchKey);
            }
            Read::Current(Some(found)) => found,
            Read::Version(None) => return self.fail_job(at, Status::NoSuchVersion),
            Read::Version(Some((_, named))) if named.marker => {
                return self.fail_job(at, Status::CopyMarker);
            }
            Read::Version(Some(found)) => found,
            _ => return self.fail_job(at, Status::Failed),
        };
        let now = self.wall.now_ns();
        let Ok(Some(scheme)) = self.table().and_then(Table::scheme) else {
            return self.fail_job(at, Status::Failed);
        };
        let Some(gateway) = self.gateway.as_ref() else {
            return self.fail_job(at, Status::Failed);
        };
        let Some((generation, root)) = gateway.keys.last() else {
            return self.fail_job(at, Status::Failed);
        };
        let holding = Holding {
            handover_ns: gateway.handover_ns,
            window: gateway.window,
        };
        let window = gateway.window;
        let keyring = RootKeys {
            keys: gateway
                .keys
                .iter()
                .map(|(g, key)| (*g, *key.bytes()))
                .collect(),
        };
        let made = (|| {
            let data = DataKey::generate().ok()?;
            let wrapped = WrappedKey {
                by: Wrapper::Root(*generation),
                bytes: data.wrap(root).ok()?,
            };
            let mut file = [0u8; 16];
            getrandom::fill(&mut file).ok()?;
            let keys = Keys {
                file: u128::from_be_bytes(file),
                data,
                wrapped,
            };
            Some((keys, Layout::new(scheme).ok()?))
        })();
        let Some((keys, layout)) = made else {
            return self.fail_job(at, Status::Failed);
        };
        let Some(Job {
            work: Work::Copy(copying),
            principal,
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        let modified = i64::try_from(object::seconds(version.modified_ns)).unwrap_or(i64::MAX);
        let current = Current::Present {
            etag: &version.etag,
            modified,
        };
        match conditional::copy_source(&copying.conditions.conditions(), &current) {
            conditional::Outcome::Proceed => {}
            conditional::Outcome::NotFound => return self.fail_job(at, Status::NoSuchKey),
            conditional::Outcome::Failed | conditional::Outcome::NotModified => {
                return self.fail_job(at, Status::PreconditionFailed);
            }
        }
        let size = version.size;
        let span = match &copying.part {
            None => {
                // "This copy request is illegal because it is trying to copy an object to itself
                // without changing the object's metadata" (research/89 §3.5).
                let (source_bucket, source_key, named) = &copying.source;
                if *source_bucket == copying.bucket
                    && *source_key == copying.key
                    && named.is_none()
                    && copying.directive == Directive::Copy
                {
                    return self.fail_job(at, Status::CopyToItself);
                }
                if size > mantle_s3::body::MAX_UPLOAD {
                    return self.fail_job(at, Status::CopySourceTooLarge);
                }
                0..size
            }
            Some(_) => match copying.conditions.range.as_deref() {
                None if size > name::MAX_PART => {
                    return self.fail_job(at, Status::CopySourceTooLarge);
                }
                None => 0..size,
                Some(text) => match range::copy_range(text, size) {
                    Err(CopyRangeError::Malformed) => {
                        return self.fail_job(at, Status::InvalidCopyRange);
                    }
                    Err(CopyRangeError::Outside) => return self.fail_job(at, Status::InvalidRange),
                    // "You can copy a range only if the source object is greater than 5 MB"
                    // (research/89 §4.1).
                    Ok(_) if size <= mantle_s3::body::MIN_PART => {
                        return self.fail_job(at, Status::CopyRangeSource);
                    }
                    Ok((first, last)) => {
                        let end = last.saturating_add(1);
                        if end.saturating_sub(first) > name::MAX_PART {
                            return self.fail_job(at, Status::EntityTooLarge);
                        }
                        first..end
                    }
                },
            },
        };
        let whole = span.start == 0 && span.end == size;
        let length = span.end.saturating_sub(span.start);
        // The checksum the copy keeps: a part's its upload's; an object's the request's, else the
        // source's algorithm, else CRC-64/NVME, as S3 gives a copy a full-object value (research/89
        // §3.4).
        let source_algorithm = version
            .checksum
            .as_ref()
            .and_then(|c| Algorithm::from_code(c.algorithm));
        let algorithm = match (&copying.part, &copying.upload) {
            (Some(_), upload) => upload
                .as_ref()
                .and_then(|u| u.checksum)
                .and_then(|(code, _)| Algorithm::from_code(code)),
            (None, _) => Some(
                copying
                    .checksum
                    .or(source_algorithm)
                    .unwrap_or(Algorithm::Crc64Nvme),
            ),
        };
        // A copy of the whole source is held to the source's own digests: its ETag where that is
        // its bytes' MD5, and its checksum where that is a full-object value of the algorithm
        // the copy keeps.
        copying.expected = Expected {
            md5: if whole {
                md5_of_etag(&version.etag)
            } else {
                None
            },
            checksum: match (&version.checksum, algorithm) {
                (Some(c), Some(algorithm))
                    if whole
                        && c.parts == 0
                        && Algorithm::from_code(c.algorithm) == Some(algorithm) =>
                {
                    Some(checksum::Checksum {
                        algorithm,
                        bytes: c.value.clone(),
                    })
                }
                _ => None,
            },
        };
        let Some(row) = copying.row.as_ref() else {
            return self.fail_job(at, Status::Failed);
        };
        let commit = match &copying.part {
            Some((upload, number)) => Commit::Part(name::PutPart {
                bucket: copying.bucket.clone(),
                incarnation: row.created_ns,
                key: copying.key.clone(),
                upload: upload.clone(),
                number: *number,
                part: unfilled_part(),
                at_ns: 0,
                run: None,
            }),
            None => Commit::Object(name::Put {
                bucket: copying.bucket.clone(),
                incarnation: row.created_ns,
                key: copying.key.clone(),
                versioning: row.versioning,
                preconditions: std::mem::take(&mut copying.preconditions),
                at_ns: 0,
                ordered_ns: None,
                version: Version {
                    marker: false,
                    null: false,
                    modified_ns: 0,
                    etag: String::new(),
                    size: 0,
                    checksum: None,
                    file: None,
                    owner: principal.clone(),
                    headers: match copying.directive {
                        Directive::Copy => version.headers.clone(),
                        Directive::Replace => std::mem::take(&mut copying.metadata),
                    },
                    retention: None,
                    legal_hold: None,
                    listing: None,
                },
                default: row.lock.and_then(|lock| lock.default),
                id: copying.id,
                deadline_ns: 0,
            }),
        };
        let body = Body {
            length,
            checksum: algorithm,
            content_md5: copying.expected.md5.is_some(),
        };
        copying.source_version = name::shown_id(version.null, order);
        copying.file = keys.file;
        let get = Get::new(version.file, size, span, Box::new(keyring), window);
        let put = Put::new(commit, body, keys, layout, holding, Box::new(Random), now);
        match (get, put) {
            (Ok(get), Ok(put)) => {
                copying.get = Some(get);
                copying.put = Some(put);
                copying.stage = CopyStage::Run;
                Ok(())
            }
            (_, Err(error)) => self.reply_job(at, put_failed(&error)),
            (Err(_), _) => self.fail_job(at, Status::Failed),
        }
    }

    /// An operation one read answers, at its turn: its read asked once.
    fn step_single(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get(at) else {
            return Ok(true);
        };
        if !job.pending.is_empty() {
            return Ok(false);
        }
        let Work::Read(single) = &job.work else {
            return Ok(true);
        };
        let (layer, query) = match single.as_ref() {
            Single::HeadBucket(bucket) => (
                Layer::Bucket,
                Query::Bucket {
                    bucket: bucket.clone(),
                },
            ),
            Single::List(list) => (Layer::Name, Query::List(list.clone())),
            Single::Buckets { prefix, after, max } => (
                Layer::Bucket,
                Query::Owned {
                    owner: job.principal.clone(),
                    prefix: prefix.clone(),
                    after: after.clone(),
                    max: *max,
                },
            ),
            Single::Versioning(bucket) => (
                Layer::Bucket,
                Query::Bucket {
                    bucket: bucket.clone(),
                },
            ),
            Single::Parts {
                bucket,
                key,
                upload,
                after,
                max,
            } => (
                Layer::Name,
                Query::Parts {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    upload: upload.clone(),
                    after: *after,
                    max: *max,
                },
            ),
            Single::Uploads(query) | Single::Versions(query) => (Layer::Name, query.clone()),
            Single::Offset {
                bucket,
                key,
                upload,
            } => (
                Layer::Name,
                Query::Upload {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    upload: upload.clone(),
                },
            ),
        };
        let Some(group) = self.group_of(layer) else {
            self.fail_job(at, Status::Failed)?;
            return self.drain_job(at);
        };
        let asked = self
            .read(group, query, u64::MAX, Wait::Request)
            .map(Pending::Read);
        self.asked(at, asked, None)
    }

    /// The answer of an operation one read answers: its reply.
    fn single_read(&mut self, at: usize, read: Read) -> Result<(), NodeError> {
        let Some(job) = self.jobs.get(at) else {
            return Ok(());
        };
        let reply = match (&job.work, read) {
            (Work::Read(single), Read::Bucket(row)) => match row {
                Some(row) if row.state != BucketState::Active => {
                    Reply::status(Status::NoSuchBucket)
                }
                // S3 answers another principal's bucket 403 unless a policy grants it.
                Some(row) if row.owner != job.principal => Reply::status(Status::AccessDenied),
                Some(row) => match **single {
                    Single::Versioning(_) => Reply {
                        versioning: answered(row.versioning),
                        ..Reply::status(Status::Done)
                    },
                    _ => Reply::status(Status::Done),
                },
                None => Reply::status(Status::NoSuchBucket),
            },
            (_, Read::NoBucket) => Reply::status(Status::NoSuchBucket),
            (_, Read::Keys(page)) => Reply {
                listing: Some(Listing::Keys(page)),
                ..Reply::status(Status::Done)
            },
            (_, Read::Owned(page)) => Reply {
                listing: Some(Listing::Buckets(page)),
                ..Reply::status(Status::Done)
            },
            (_, Read::Parts(None)) => Reply::status(Status::NoSuchUpload),
            (_, Read::Parts(Some((parts, more)))) => {
                let next = if more {
                    parts.last().map(|(number, _)| *number)
                } else {
                    None
                };
                let parts = parts
                    .into_iter()
                    .map(|(number, part)| PartListed {
                        number,
                        etag: part.etag,
                        size: part.size,
                        modified: object::seconds(part.modified_ns),
                        checksum: part.checksum,
                    })
                    .collect();
                Reply {
                    listing: Some(Listing::Parts(Parts {
                        parts,
                        truncated: more,
                        next,
                    })),
                    ..Reply::status(Status::Done)
                }
            }
            (_, Read::Uploads(page)) => Reply {
                listing: Some(Listing::Uploads(page)),
                ..Reply::status(Status::Done)
            },
            (_, Read::Versions(page)) => Reply {
                listing: Some(Listing::Versions(page)),
                ..Reply::status(Status::Done)
            },
            // A native upload's progress, which a resuming client appends from; an upload
            // CreateMultipartUpload made has none.
            (_, Read::Upload(Some(upload))) => match upload.runs {
                Some(runs) => Reply {
                    progress: Some(Box::new(progress(&runs, None))),
                    ..Reply::status(Status::Done)
                },
                None => Reply::status(Status::InvalidRequest),
            },
            (_, Read::Upload(None)) => Reply::status(Status::NoSuchUpload),
            _ => Reply::status(Status::Failed),
        };
        self.reply_job(at, reply)
    }

    /// Ends job `at` with `reply`: what it waits on is let go, and its body, if any is left, read
    /// to its end and dropped before the reply goes.
    fn reply_job(&mut self, at: usize, reply: Reply) -> Result<(), NodeError> {
        if let Some(job) = self.jobs.get_mut(at) {
            job.work = Work::Draining(Box::new(reply));
            self.abandoned
                .extend(job.pending.drain(..).map(|(pending, _)| pending));
        }
        Ok(())
    }

    /// A job a callee moved on within its turn, to draining a refusal (`reply_job`) or to answering
    /// its last answers while its door's channel has no room for them (`write_get`): that work's turn
    /// now, so a refusal's reply goes at once, and a job ends only once its last answer went;
    /// whether it is over. A step that found its job no longer its own kind and ended it dropped
    /// that reply, and a get's end its channel held: the listener then closed the connection under
    /// a response whose every byte it had sent.
    fn moved_on(&mut self, at: usize) -> Result<bool, NodeError> {
        match self.jobs.get(at).map(|job| &job.work) {
            None => Ok(true),
            Some(Work::Draining(_)) => self.drain_job(at),
            Some(Work::Answering(_)) => self.answer_local(at),
            Some(_) => Ok(false),
        }
    }

    /// Ends job `at` with `status` alone.
    fn fail_job(&mut self, at: usize, status: Status) -> Result<(), NodeError> {
        self.reply_job(at, Reply::status(status))
    }

    /// Ends job `at` `SlowDown` for `cause`, which its reply names on either door.
    fn slow_job(&mut self, at: usize, cause: door::Slowed) -> Result<(), NodeError> {
        self.reply_job(at, Reply::slowed(cause))
    }

    /// The outcomes of asks whose job ended before they came, dropped as they come: an outcome no
    /// job takes would hold one of the node's asks for good.
    fn drop_abandoned(&mut self) {
        if self.abandoned.is_empty() {
            return;
        }
        let Self {
            abandoned,
            commands_done,
            reads_done,
            chunks_done,
            ..
        } = self;
        abandoned.retain(|pending| {
            let gone = match pending {
                Pending::Command(ask) => take_done(commands_done, *ask).is_some(),
                Pending::Read(ask) => take_done(reads_done, *ask).is_some(),
                Pending::Chunk(ask) => take_done(chunks_done, *ask).is_some(),
            };
            !gone
        });
    }

    /// A draining job's turn: its body read to its end and dropped, then its reply; whether it is
    /// over.
    fn drain_job(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        let exchange = match job.door {
            Door::Exchange(exchange) => exchange,
            // What is left of the body is the listener's to read or drop (node.md §4.5): the reply
            // and its end go now.
            Door::Local(_) => {
                let Work::Draining(reply) =
                    std::mem::replace(&mut job.work, Work::Answering(VecDeque::new()))
                else {
                    return Ok(false);
                };
                // A reply slowed for a cause says it (`door::Slowed`); any other, its head.
                let head = match reply.slowed {
                    Some(cause) => door::Answer::Slowed(cause),
                    None => door::Answer::Head(reply),
                };
                job.work = Work::Answering(VecDeque::from([head, door::Answer::End]));
                return self.answer_local(at);
            }
        };
        let Work::Draining(reply) = &job.work else {
            return Ok(false);
        };
        let endpoint = self.driver.endpoint();
        loop {
            let Ok(mut piece) = endpoint.reserve(Class::Transfer, PIECE) else {
                return Ok(false);
            };
            let got = endpoint.read_body(exchange, &mut piece);
            self.moved = self
                .moved
                .saturating_add(u64::try_from(piece.bytes().len()).unwrap_or(u64::MAX));
            endpoint.release(piece);
            match got {
                Ok(0) => break,
                Ok(_) => {}
                // Its asker is gone: nothing to answer.
                Err(_) => return Ok(true),
            }
        }
        if !endpoint.body_complete(exchange) && endpoint.head(exchange).is_some() {
            let mut empty = hyper_transport::Reservation::default();
            let _ = endpoint.read_body(exchange, &mut empty);
            if !endpoint.body_complete(exchange) {
                return Ok(false);
            }
        }
        let Ok((head, fields)) = reply.encode() else {
            return Ok(true);
        };
        let slowed = reply.slowed;
        let length = u64::try_from(fields.len()).unwrap_or(u64::MAX);
        if endpoint.reply(exchange, &head, Some(length)).is_err() {
            return Ok(true);
        }
        // A native request answered `SlowDown`, counted for its cause as it goes: the node says
        // the counts in its lines, as the listener counts the S3 door's.
        if let Some(count) = slowed.and_then(|cause| self.slowed.get_mut(cause.index())) {
            *count = count.saturating_add(1);
        }
        if let Some(job) = self.jobs.get_mut(at) {
            job.work = Work::Replying(fields, 0);
        }
        self.write_fields(at)
    }

    /// A reply's fields written as far as its exchange takes them; once all are, the exchange
    /// ends.
    fn write_fields(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        let Door::Exchange(exchange) = job.door else {
            return Ok(true);
        };
        let Work::Replying(fields, written) = &mut job.work else {
            return Ok(false);
        };
        let endpoint = self.driver.endpoint();
        while let Some(rest) = fields.get(*written..)
            && !rest.is_empty()
        {
            match endpoint.write_body(exchange, rest) {
                Ok(0) => return Ok(false),
                Ok(took) => {
                    *written = written.saturating_add(took);
                    self.moved = self
                        .moved
                        .saturating_add(u64::try_from(took).unwrap_or(u64::MAX));
                }
                Err(_) => return Ok(true),
            }
        }
        endpoint.end(exchange);
        Ok(true)
    }

    /// A local job's last answers sent as far as its channel takes them; whether it is over: every
    /// one sent, or the connection gone.
    fn answer_local(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        let (Door::Local(local), Work::Answering(answers)) = (&mut job.door, &mut job.work) else {
            return Ok(true);
        };
        local.pending.extend(answers.drain(..));
        Ok(local.flush() != Sent::Held)
    }

    /// The jobs whose exchange is `exchange`, for an event on it.
    pub(super) fn job_on(&self, exchange: ExchangeId) -> Option<usize> {
        self.jobs
            .iter()
            .position(|j| matches!(j.door, Door::Exchange(e) if e == exchange))
    }

    /// An exchange of a job ended unanswered: the job ends, what it waits on let go.
    pub(super) fn job_lost(&mut self, exchange: ExchangeId) {
        let Self {
            jobs,
            abandoned,
            admission,
            ..
        } = self;
        let now = hyper_rt::futures::now_ns();
        self.parked.retain(|p| p.exchange != exchange);
        jobs.retain_mut(|j| {
            if !matches!(j.door, Door::Exchange(e) if e == exchange) {
                return true;
            }
            abandoned.extend(j.pending.drain(..).map(|(pending, _)| pending));
            if let (Some(at), Some(admission)) = (j.admitted_ns, admission.as_mut()) {
                admission.completed(now.saturating_sub(at), now, j.charge);
            }
            false
        });
    }

    /// The operations the host admits at once now, as it learned its knee (`crate::admit`): none
    /// for a node that hosts no objects.
    pub fn admitting(&self) -> Option<usize> {
        self.admission.as_ref().map(crate::admit::Admission::limit)
    }

    /// Whether the host admits one more operation holding `charge` bytes beside those it runs
    /// (`crate::admit`). One the learned limit turns away tells the limit its clients ask more
    /// than it admits; one whose bytes the operations' memory does not hold says nothing of it.
    pub(super) fn admits(&mut self, charge: u64) -> bool {
        let Some(admission) = self.admission.as_mut() else {
            return true;
        };
        match admission.admits(charge) {
            Ok(()) => true,
            Err(crate::admit::Held::Limit) => {
                admission.turned_away();
                false
            }
            Err(crate::admit::Held::Bytes) => false,
        }
    }

    /// An operation the host admitted at `admitted_ns`, charged `charge` bytes, ended: the limit
    /// learns its latency, and its bytes are free.
    fn ended(&mut self, admitted_ns: Option<u64>, charge: u64) {
        let now = hyper_rt::futures::now_ns();
        if let (Some(at), Some(admission)) = (admitted_ns, self.admission.as_mut()) {
            admission.completed(now.saturating_sub(at), now, charge);
        }
        self.keep_pool();
    }

    /// The native clients' stream pool sized to the limit the admission learned, where it moved:
    /// the exchanges they may open past those they keep are the operations the host runs
    /// (Breakwater's credit, hyper-transport's `pool`).
    pub(super) fn keep_pool(&mut self) {
        let Some(limit) = self.admission.as_ref().map(crate::admit::Admission::limit) else {
            return;
        };
        if self.pooled == 0 || self.pooled == limit {
            return;
        }
        let streams = u32::try_from(limit).unwrap_or(u32::MAX);
        if self.driver.endpoint().set_stream_pool(streams).is_ok() {
            self.pooled = limit;
        }
    }

    /// When the host next needs a turn: at once when what an operation, or one ended, asked is
    /// answered, as one answered after the host's step in a turn (a read this node's own member
    /// answers) waits on no event; else a put's renewal coming due.
    /// What job `at`'s client will still wait, in nanoseconds, for an ask of the job's own
    /// (research/77 D2): at least one, as a time that has passed is none to wait on.
    fn remaining_of(&self, at: usize) -> u64 {
        let now = hyper_rt::futures::now_ns();
        self.jobs
            .get(at)
            .map_or(1, |job| job.bound_ns().saturating_sub(now).max(1))
    }

    pub(super) fn host_due(&self) -> Option<u64> {
        let shard = hyper_rt::futures::now_ns();
        let answered = |pending: &Pending| match pending {
            Pending::Command(ask) => self.commands_done.iter().any(|(a, _)| a == ask),
            Pending::Read(ask) => self.reads_done.iter().any(|(a, _)| a == ask),
            Pending::Chunk(ask) => self.chunks_done.iter().any(|(a, _)| a == ask),
        };
        let mut pending = self
            .jobs
            .iter()
            .flat_map(|j| j.pending.iter().map(|(p, _)| p))
            .chain(&self.abandoned);
        let looked_up = self
            .lookups
            .iter()
            .filter_map(|l| l.ask)
            .any(|ask| self.reads_done.iter().any(|(a, _)| *a == ask));
        if looked_up || pending.any(answered) {
            return Some(shard);
        }
        let wall = self.wall.now_ns();
        let renewal = self
            .jobs
            .iter()
            .filter_map(|j| match &j.work {
                Work::Put(putting) => putting.put.as_ref()?.due_ns(),
                Work::Copy(copying) => copying.put.as_ref()?.due_ns(),
                _ => None,
            })
            .min()
            .map(|due| shard.saturating_add(due.saturating_sub(wall)));
        // Each operation at work is ended at its client's patience, and one whose asks are held
        // says so each beat, a probe timeout, the transport's notes' beat of a third of the serve
        // period (research/77 D2).
        let beat = u64::try_from(bounds::initial_pto().as_nanos()).unwrap_or(u64::MAX);
        let bound = self
            .jobs
            .iter()
            .filter(|j| j.working() && j.held())
            .filter_map(|j| j.due_ns(shard, beat))
            .min();
        [renewal, bound].into_iter().flatten().min()
    }
}

/// The cell's first Name range, which spans every key at its first generation (node.md §1.5): the
/// directory a bucket's coordinator routes by.
fn directory() -> Vec<Descriptor> {
    vec![Descriptor {
        id: 1,
        lo: Vec::new(),
        hi: None,
        generation: 1,
    }]
}

/// The work an operation's request names, the bytes read past its fields the start of a put's
/// body; a native upload's Open cuts its runs in `block`'s whole blocks (`Host::open_block`).
fn work(op: Op, carry: Vec<u8>, block: Option<u64>) -> Work {
    // Metadata past S3's 2 KB, or that no HTTP field could carry, is refused before anything is
    // written (research/89 §1.4): judged once, its refusal taken in its place below.
    let metadata = match &op {
        Op::Put { metadata, .. }
        | Op::CreateUpload { metadata, .. }
        | Op::Open { metadata, .. } => mantle_s3::metadata::check(metadata).err(),
        Op::Copy(copy) => mantle_s3::metadata::check(&copy.metadata).err(),
        _ => None,
    };
    match op {
        Op::Create { bucket, .. } => Work::Bucket(Box::new(Coordinating {
            bucket,
            delete: false,
            owned: true,
            coordinator: None,
        })),
        Op::DeleteBucket { bucket, .. } => Work::Bucket(Box::new(Coordinating {
            bucket,
            delete: true,
            owned: false,
            coordinator: None,
        })),
        Op::Put { length, .. } if length > mantle_s3::body::MAX_UPLOAD => {
            Work::Draining(Box::new(Reply::status(Status::EntityTooLarge)))
        }
        // A body states one checksum at most, in its fields or its trailer (05 §3.2).
        Op::Put {
            checksum: Some(_),
            trailing: Some(_),
            ..
        }
        | Op::UploadPart {
            checksum: Some(_),
            trailing: Some(_),
            ..
        } => Work::Draining(Box::new(Reply::status(Status::InvalidRequest))),
        Op::Put { .. } | Op::CreateUpload { .. } | Op::Open { .. } | Op::Copy(_)
            if metadata.is_some() =>
        {
            Work::Draining(Box::new(Reply::status(metadata_status(metadata.as_ref()))))
        }
        Op::Put {
            bucket,
            key,
            id,
            length,
            headers,
            checksum,
            md5,
            trailing,
            metadata,
        } => Work::Put(Box::new(Putting {
            bucket,
            key,
            part: None,
            id,
            length,
            preconditions: Preconditions {
                if_match: matched(headers.if_match.as_deref(), true),
                if_none_match: matched(headers.if_none_match.as_deref(), false),
            },
            expected: Expected { md5, checksum },
            trailing,
            trailer: Vec::new(),
            put: None,
            taken: 0,
            carry,
            waiting: VecDeque::new(),
            run: None,
            metadata,
        })),
        Op::Get {
            bucket,
            key,
            headers,
            version,
        } => getting(bucket, key, headers, version.as_deref(), false),
        Op::Head {
            bucket,
            key,
            headers,
            version,
        } => getting(bucket, key, headers, version.as_deref(), true),
        Op::Delete {
            bucket,
            key,
            headers,
            version,
            ..
        } => match named(version.as_deref()) {
            Err(status) => Work::Draining(Box::new(Reply::status(status))),
            Ok(named) => Work::Command(Box::new(Commanding {
                bucket,
                then: Then::Delete {
                    key,
                    if_match: matched(headers.if_match.as_deref(), true),
                    named,
                },
                asked: false,
            })),
        },
        Op::PutVersioning {
            bucket, versioning, ..
        } => Work::Command(Box::new(Commanding {
            bucket,
            then: Then::Versioning(match versioning {
                mantle_s3::body::Versioning::Enabled => Versioning::Enabled,
                mantle_s3::body::Versioning::Suspended => Versioning::Suspended,
            }),
            asked: false,
        })),
        Op::GetVersioning { bucket } => Work::Read(Box::new(Single::Versioning(bucket))),
        // A checksum type its algorithm does not have: a full-object hash or a composite
        // CRC-64/NVME (05 §3.3), refused before anything is written.
        Op::CreateUpload {
            checksum: Some((algorithm, full)),
            ..
        } if !(if full {
            algorithm.full_object()
        } else {
            algorithm.composite()
        }) =>
        {
            Work::Draining(Box::new(Reply::status(Status::InvalidRequest)))
        }
        Op::CreateUpload {
            bucket,
            key,
            checksum,
            metadata,
            ..
        } => Work::Command(Box::new(Commanding {
            bucket,
            then: Then::CreateUpload {
                key,
                checksum,
                metadata,
            },
            asked: false,
        })),
        Op::UploadPart { length, .. } if length > name::MAX_PART => {
            Work::Draining(Box::new(Reply::status(Status::EntityTooLarge)))
        }
        Op::UploadPart {
            bucket,
            key,
            upload,
            number,
            id,
            length,
            checksum,
            md5,
            trailing,
        } => Work::Put(Box::new(Putting {
            bucket,
            key,
            part: Some((upload, number)),
            id,
            length,
            preconditions: Preconditions::default(),
            expected: Expected { md5, checksum },
            trailing,
            trailer: Vec::new(),
            put: None,
            taken: 0,
            carry,
            waiting: VecDeque::new(),
            run: None,
            metadata: Vec::new(),
        })),
        Op::Complete {
            bucket,
            key,
            upload,
            headers,
            parts,
            ..
        } => Work::Complete(Box::new(Completing {
            bucket,
            key,
            upload,
            listed: parts,
            native: None,
            preconditions: Preconditions {
                if_match: matched(headers.if_match.as_deref(), true),
                if_none_match: matched(headers.if_none_match.as_deref(), false),
            },
            completion: None,
            waiting: VecDeque::new(),
        })),
        Op::Abort {
            bucket,
            key,
            upload,
            ..
        } => Work::Command(Box::new(Commanding {
            bucket,
            then: Then::Abort { key, upload },
            asked: false,
        })),
        // A native upload's object is held to a CRC, which combines from its runs' (05 §3.3).
        Op::Open { checksum, .. } if !checksum.full_object() => {
            Work::Draining(Box::new(Reply::status(Status::InvalidRequest)))
        }
        Op::Open {
            bucket,
            key,
            length,
            buffer,
            checksum,
            metadata,
            ..
        } => {
            // No scheme to lay its blocks out by: nothing can be put.
            let Some(block) = block else {
                return Work::Draining(Box::new(Reply::status(Status::Failed)));
            };
            match name::run_length(length, buffer, block) {
                Some(run) => Work::Command(Box::new(Commanding {
                    bucket,
                    then: Then::Open {
                        key,
                        length,
                        checksum,
                        run,
                        metadata,
                    },
                    asked: false,
                })),
                // No run fits: an object past what 10,000 runs of S3's largest part hold, or a
                // buffer below its least part or too small for the length's runs.
                None if length.is_some_and(|l| {
                    l > name::MAX_PART.saturating_mul(u64::from(name::MAX_PART_NUMBER))
                }) =>
                {
                    Work::Draining(Box::new(Reply::status(Status::EntityTooLarge)))
                }
                None => Work::Draining(Box::new(Reply::status(Status::InvalidArgument))),
            }
        }
        // A run's checksum is its upload's, in a trailer, which the run is checked by.
        Op::Append { trailing: None, .. } => {
            Work::Draining(Box::new(Reply::status(Status::InvalidRequest)))
        }
        Op::Append { length, .. } if length > name::MAX_PART => {
            Work::Draining(Box::new(Reply::status(Status::EntityTooLarge)))
        }
        Op::Append {
            bucket,
            key,
            upload,
            id,
            offset,
            length,
            last,
            trailing,
        } => Work::Put(Box::new(Putting {
            bucket,
            key,
            // Numbered once its upload's row is read.
            part: Some((upload, 0)),
            id,
            length,
            preconditions: Preconditions::default(),
            expected: Expected::default(),
            trailing,
            trailer: Vec::new(),
            put: None,
            taken: 0,
            carry,
            waiting: VecDeque::new(),
            run: Some(Appending {
                offset,
                last,
                incarnation: None,
                runs: None,
            }),
            metadata: Vec::new(),
        })),
        Op::Offset {
            bucket,
            key,
            upload,
        } => Work::Read(Box::new(Single::Offset {
            bucket,
            key,
            upload,
        })),
        Op::Finish {
            bucket,
            key,
            upload,
            headers,
            length,
            checksum,
            ..
        } => Work::Complete(Box::new(Completing {
            bucket,
            key,
            upload,
            listed: Vec::new(),
            native: Some((
                length,
                record::Checksum {
                    algorithm: checksum.algorithm.code(),
                    parts: 0,
                    value: checksum.bytes,
                },
            )),
            preconditions: Preconditions {
                if_match: matched(headers.if_match.as_deref(), true),
                if_none_match: matched(headers.if_none_match.as_deref(), false),
            },
            completion: None,
            waiting: VecDeque::new(),
        })),
        // No part asked: an empty page, the upload not read (05 §4.6).
        Op::ListParts { max: 0, .. } => Work::Draining(Box::new(Reply {
            listing: Some(Listing::Parts(Parts::default())),
            ..Reply::status(Status::Done)
        })),
        Op::ListParts {
            bucket,
            key,
            upload,
            after,
            max,
        } => Work::Read(Box::new(Single::Parts {
            bucket,
            key,
            upload,
            after,
            max: max.min(u32::try_from(MAX_PAGE_PARTS).unwrap_or(u32::MAX)),
        })),
        Op::ListUploads {
            bucket,
            prefix,
            delimiter,
            key_marker,
            id_marker,
            max,
        } => Work::Read(Box::new(Single::Uploads(Query::Uploads {
            bucket,
            prefix,
            delimiter,
            key_marker,
            id_marker,
            max,
        }))),
        Op::List(list) => Work::Read(Box::new(Single::List(list))),
        Op::HeadBucket { bucket } => Work::Read(Box::new(Single::HeadBucket(bucket))),
        Op::ListBuckets { prefix, after, max } => {
            Work::Read(Box::new(Single::Buckets { prefix, after, max }))
        }
        // No version asked: an empty page, the range not read (research/89 §2.3).
        Op::ListVersions(list) if list.max == 0 => Work::Draining(Box::new(Reply {
            listing: Some(Listing::Versions(object::Versions::default())),
            ..Reply::status(Status::Done)
        })),
        Op::ListVersions(mut list) => {
            list.max = list
                .max
                .min(u32::try_from(mantle_s3::list::MAX_KEYS).unwrap_or(u32::MAX));
            Work::Read(Box::new(Single::Versions(Query::Versions(list))))
        }
        Op::DeleteObjects {
            bucket,
            objects,
            quiet,
            ..
        } => {
            let results = vec![None; objects.len()];
            Work::Deletes(Box::new(Deleting {
                bucket,
                objects,
                quiet,
                row: None,
                next: 0,
                out: 0,
                results,
            }))
        }
        Op::Copy(copy) => {
            let object::Copy {
                bucket,
                key,
                id,
                source,
                conditions,
                headers,
                directive,
                metadata,
                checksum,
            } = *copy;
            let preconditions = Preconditions {
                if_match: matched(headers.if_match.as_deref(), true),
                if_none_match: matched(headers.if_none_match.as_deref(), false),
            };
            copying(
                (bucket, key, None, id),
                source,
                conditions,
                (preconditions, directive, metadata, checksum),
            )
        }
        Op::CopyPart(part) => {
            let object::CopyPart {
                bucket,
                key,
                upload,
                number,
                id,
                source,
                conditions,
            } = *part;
            if !(1..=name::MAX_PART_NUMBER).contains(&number) {
                return Work::Draining(Box::new(Reply::status(Status::InvalidArgument)));
            }
            copying(
                (bucket, key, Some((upload, number)), id),
                source,
                conditions,
                (Preconditions::default(), Directive::Copy, Vec::new(), None),
            )
        }
        Op::CreatePrincipal { name, policy } => administer(|| {
            let compact = checked_policy(&name, &policy)?;
            let id = mantle_s3::account::draw_id().map_err(|_| Status::Failed)?;
            Ok(Admin::Command(Box::new(root::Command::Principal(
                principal::Command::Create(principal::NewPrincipal {
                    name,
                    id,
                    policy: compact,
                    at_ns: 0,
                }),
            ))))
        }),
        Op::PutPolicy { name, policy } => administer(|| {
            let compact = checked_policy(&name, &policy)?;
            Ok(Admin::Command(Box::new(root::Command::Principal(
                principal::Command::Policy {
                    name,
                    policy: compact,
                },
            ))))
        }),
        Op::DeletePrincipal { name } => administer(|| {
            Ok(Admin::Command(Box::new(root::Command::Principal(
                principal::Command::Delete { name },
            ))))
        }),
        // No principal asked: an empty page, nothing read.
        Op::ListPrincipals { max: 0, .. } => Work::Draining(Box::new(Reply {
            listing: Some(Listing::Principals(Principals::default())),
            ..Reply::status(Status::Done)
        })),
        Op::ListPrincipals { after, max } => administer(|| {
            let most = u32::try_from(object::MAX_PAGE_PRINCIPALS).unwrap_or(u32::MAX);
            Ok(Admin::Read {
                query: Query::Principals {
                    after,
                    max: max.min(most),
                },
                one: false,
            })
        }),
        Op::GetPrincipal { name } => administer(|| {
            Ok(Admin::Read {
                query: Query::Principal { name },
                one: true,
            })
        }),
        Op::CreateKey {
            principal,
            rotating,
        } => administer(|| {
            Ok(Admin::Key {
                principal,
                rotating,
            })
        }),
        Op::RevokeKey { id } => administer(|| {
            Ok(Admin::Command(Box::new(root::Command::Principal(
                principal::Command::Revoke { id },
            ))))
        }),
    }
}

/// The refusal `mantle_s3::metadata::check`'s error earns: past S3's 2 KB `MetadataTooLarge`, any
/// other `InvalidArgument`.
fn metadata_status(error: Option<&mantle_s3::metadata::MetadataError>) -> Status {
    match error {
        Some(mantle_s3::metadata::MetadataError::TooLarge) => Status::MetadataTooLarge,
        _ => Status::InvalidArgument,
    }
}

/// A copy's job, its source's version parsed; `InvalidVersion` for one no version is named by.
fn copying(
    (bucket, key, part, id): (String, String, Option<(String, u16)>, u128),
    source: mantle_s3::copy::Source,
    conditions: Headers,
    (preconditions, directive, metadata, checksum): (
        Preconditions,
        Directive,
        Metadata,
        Option<Algorithm>,
    ),
) -> Work {
    let named = match named(source.version.as_deref()) {
        Ok(named) => named,
        Err(status) => return Work::Draining(Box::new(Reply::status(status))),
    };
    Work::Copy(Box::new(Copying {
        bucket,
        key,
        part,
        id,
        source: (source.bucket, source.key, named),
        conditions,
        preconditions,
        directive,
        metadata,
        checksum,
        stage: CopyStage::Bucket,
        row: None,
        upload: None,
        get: None,
        put: None,
        expected: Expected::default(),
        carry: None,
        ended: false,
        get_waiting: VecDeque::new(),
        put_waiting: VecDeque::new(),
        source_version: String::new(),
        file: 0,
        stored: None,
    }))
}

/// An operator's request as work: what it asks, or the reply that refuses it at once.
fn administer(asking: impl FnOnce() -> Result<Admin, Status>) -> Work {
    match asking() {
        Ok(asking) => Work::Admin(Box::new(Administering {
            asking,
            asked: false,
            issued: None,
        })),
        Err(status) => Work::Draining(Box::new(Reply::status(status))),
    }
}

/// A principal's name and identity policy as the root range keeps them: the name in IAM's form,
/// `InvalidArgument` otherwise, and the policy checked as IAM checks a user's, its compact form,
/// `MalformedPolicy` otherwise (`mantle_s3::policy::identity`; research/91 §2, §3).
fn checked_policy(name: &str, policy: &str) -> Result<String, Status> {
    if !mantle_s3::account::valid_name(name) {
        return Err(Status::InvalidArgument);
    }
    mantle_s3::policy::identity(policy.as_bytes())
        .map(|(_, compact)| compact)
        .map_err(|_| Status::MalformedPolicy)
}

fn getting(
    bucket: String,
    key: String,
    headers: Headers,
    version: Option<&str>,
    head: bool,
) -> Work {
    let version = match named(version) {
        Ok(version) => version,
        Err(status) => return Work::Draining(Box::new(Reply::status(status))),
    };
    Work::Get(Box::new(Getting {
        bucket,
        key,
        headers,
        version,
        head,
        get: None,
        held: None,
        out: None,
        written: 0,
        total: 0,
        waiting: VecDeque::new(),
    }))
}

/// What a bucket's create or delete asks next.
enum Next {
    /// A delete's bucket row, whose owner it checks.
    Owner,
    /// Its first command, or its coordinator's next request.
    Request(coordinator::Request),
    /// Nothing: it is over, with this answer.
    Done(Status),
}

/// Where a put is at a turn.
enum Step {
    /// Its bucket's row to ask for.
    Ask(String),
    /// Waiting on what it asked.
    Wait,
    /// Going: its body and requests to move.
    Go,
    /// Over, with its reply.
    Over(Box<Reply>),
}

/// The reply a copy earns once its put committed (research/89 §3.6): the new version's or part's
/// ETag, checksum and time, the destination's version ID and the source's.
fn copied(stored: &put::Stored, modified_ns: u64, source_version: &str) -> Reply {
    Reply {
        version: stored.version.clone().unwrap_or_default(),
        etag: stored.etag.clone(),
        size: stored.size,
        modified: object::seconds(modified_ns),
        checksum: stored.checksum.as_ref().map(kept),
        source_version: source_version.to_owned(),
        ..Reply::status(Status::Done)
    }
}

/// The MD5 an ETag is, where it is one: 32 hexadecimal digits, a single PUT's of plaintext
/// under SSE-S3 (research/05 §5.1); a multipart object's has its parts' count after it.
fn md5_of_etag(etag: &str) -> Option<[u8; 16]> {
    let digits = etag.as_bytes();
    if digits.len() != 32 {
        return None;
    }
    let mut md5 = [0u8; 16];
    for (byte, pair) in md5.iter_mut().zip(digits.as_chunks::<2>().0) {
        let hex = std::str::from_utf8(pair).ok()?;
        *byte = u8::from_str_radix(hex, 16).ok()?;
    }
    Some(md5)
}

/// The reply a get's failure is answered with: too few of a block's chunks read is a cause that
/// passes, `503 SlowDown`, which clients ask again; any other `500`.
fn get_failed(error: &GetError) -> Reply {
    match error {
        GetError::Unreadable(_) => Reply::slowed(door::Slowed::Unreadable),
        _ => Reply::status(Status::Failed),
    }
}

/// What a job's ask came to.
enum Outcome {
    Command(CommandOutcome),
    Read(ReadOutcome),
    Chunk(ChunkOutcome),
}

/// The job of `jobs` waiting on chunk ask `ask`, its work moved now: a piece of the chunk it
/// reads came, or one of the chunk it sends went (research/77 D2).
pub(super) fn chunk_moved(jobs: &mut [Job], ask: u64) {
    let now = hyper_rt::futures::now_ns();
    if let Some(job) = jobs.iter_mut().find(|job| {
        job.pending
            .iter()
            .any(|(pending, _)| matches!(pending, Pending::Chunk(asked) if *asked == ask))
    }) {
        job.moved_ns = now;
    }
}

/// A put's driver's answer for what its request came to.
fn put_answer(outcome: Outcome) -> put::Answer {
    match outcome {
        Outcome::Chunk(ChunkOutcome::Stored) => put::Answer::Stored,
        Outcome::Chunk(ChunkOutcome::Refused(chunk::Status::Full)) => {
            put::Answer::Refused { full: true }
        }
        // No room now, at the volume's writer queue or in the transport toward it: asked again
        // once room frees, within the client's patience (research/77 D2).
        Outcome::Chunk(
            ChunkOutcome::Refused(chunk::Status::Busy) | ChunkOutcome::Lost(Refusal::Budget),
        ) => put::Answer::Busy,
        Outcome::Chunk(_) => put::Answer::Refused { full: false },
        Outcome::Command(CommandOutcome::Answered(Answer::Block(o))) => put::Answer::Block(o),
        Outcome::Command(CommandOutcome::Answered(Answer::File(o))) => put::Answer::File(o),
        Outcome::Command(CommandOutcome::Answered(Answer::Name(o))) => put::Answer::Name(o),
        // Any other: the driver takes it as the wrong kind and fails the put.
        _ => put::Answer::Refused { full: false },
    }
}

/// A completion's driver's answer for what its request came to; none for a read or command
/// refused or answered with another kind.
fn complete_answer(outcome: Outcome) -> Option<complete::Answer> {
    match outcome {
        Outcome::Read(ReadOutcome::Read(read)) => match *read {
            Read::Upload(upload) => Some(complete::Answer::Upload(upload)),
            Read::Parts(Some((parts, _))) => Some(complete::Answer::Parts(parts)),
            // The upload gone while the completion read it: its parts are none, and the
            // driver finds the listed parts missing.
            Read::Parts(None) => Some(complete::Answer::Parts(Vec::new())),
            _ => None,
        },
        Outcome::Command(CommandOutcome::Answered(Answer::File(o))) => {
            Some(complete::Answer::File(o))
        }
        Outcome::Command(CommandOutcome::Answered(Answer::Name(o))) => {
            Some(complete::Answer::Name(o))
        }
        _ => None,
    }
}

/// A get's driver's answer for what its request came to.
fn get_answer(outcome: Outcome) -> get::Answer {
    match outcome {
        Outcome::Chunk(ChunkOutcome::Read(bytes)) => get::Answer::Chunk(Bytes::from(bytes)),
        Outcome::Chunk(_) => get::Answer::Unreadable,
        Outcome::Read(ReadOutcome::Read(read)) => match *read {
            Read::Header(header) => get::Answer::Header(header),
            Read::Extents(extents) => get::Answer::Extents(extents),
            Read::Block(block) => get::Answer::Block(block),
            _ => get::Answer::Unreadable,
        },
        _ => get::Answer::Unreadable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A gateway of the rates and target `mantle init`'s cells state (crates/mantle/tests/support).
    fn scheming_gateway() -> GatewaySettings {
        GatewaySettings {
            keys: Vec::new(),
            handover_ns: 1,
            window: 1,
            objects: 1,
            objects_bytes: 1,
            charges: crate::derive::Charges::new(1, &[durability::Scheme::Copies(3)]).unwrap(),
            clients: 1,
            bucket_quota: 1,
            scan_rows: 1,
            rates: Rates {
                chunk: 0.000_001_552_511_415_525_114_2,
                domains: None,
                bursts: Vec::new(),
                repair: 0.041_666_666_666_666_664,
            },
            target: 1e-11,
        }
    }

    /// A volume in each of `domains`, in order.
    fn placed_in(domains: &[&str]) -> Vec<Placed> {
        domains
            .iter()
            .zip(1u128..)
            .map(|(domain, volume)| Placed {
                volume,
                node: u64::try_from(volume).unwrap(),
                domain: (*domain).to_owned(),
                bytes: 1 << 30,
            })
            .collect()
    }

    /// The scheming of a table read at `applied` over `standing` after `was`, settled as far as
    /// it goes without waiting, as `Node::table` reads it.
    fn read(
        was: Option<Scheming>,
        before: &[Placed],
        standing: &[Placed],
        applied: u64,
        gateway: &GatewaySettings,
    ) -> Scheming {
        let mut scheming = Scheming::after(was, same_domains(before, standing), applied);
        scheming.settle(Some(gateway), standing);
        scheming
    }

    /// The process's compute pool, started once for this test binary with one worker, so a test
    /// holding that worker holds the pool, and room for a job of each test at once; and the
    /// tests that use it one at a time, as each may hold its worker or fill its room.
    fn pool() -> std::sync::MutexGuard<'static, ()> {
        static STARTED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
        static ONE_AT_A_TIME: std::sync::Mutex<()> = std::sync::Mutex::new(());
        STARTED.get_or_init(|| {
            compute::start(compute::Config {
                workers: 1,
                queued: 64,
                piece_ns: 1,
            })
            .unwrap();
        });
        ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Holds the pool's one worker in a job until the returned sender is dropped: returns once
    /// the worker is in it.
    fn hold_the_worker() -> std::sync::mpsc::SyncSender<()> {
        let (release, held) = std::sync::mpsc::sync_channel::<()>(0);
        let (entered, inside) = std::sync::mpsc::sync_channel::<()>(1);
        let job = compute::run(move || {
            entered.send(()).unwrap();
            let _ = held.recv();
        });
        assert!(job.is_ok());
        inside.recv().unwrap();
        release
    }

    /// The volume table read again after an entry that left its volumes and their domains as
    /// they were takes the scheme chosen before, asking nothing of the allocator; one whose
    /// domains changed is chosen again (research/71, step 5: the choice ran at each entry the
    /// root range applied, 2,448.5 allocations each time).
    #[test]
    fn the_scheme_is_chosen_again_only_when_the_tables_domains_change() {
        let gateway = scheming_gateway();
        let first = placed_in(&["a", "b", "c"]);
        let chosen = read(None, &[], &first, 1, &gateway);
        assert!(chosen.settled && chosen.choosing.is_none());
        let scheme = chosen.now;
        // Three wide: two domains do not fit it, so a table of two chooses again at once.
        assert!(
            matches!(scheme, Ok(Some(s)) if s.width() == 3),
            "{scheme:?}"
        );
        let same = placed_in(&["a", "b", "c"]);
        hyper_measure::alloc::begin();
        let carried = read(Some(chosen), &first, &same, 2, &gateway);
        let counts = hyper_measure::alloc::end();
        assert_eq!(carried.now, scheme);
        assert_eq!((counts.allocations, counts.reallocations), (0, 0));
        // One domain fewer: chosen again, over two domains.
        let fewer = placed_in(&["a", "b", "b"]);
        hyper_measure::alloc::begin();
        let again = read(Some(carried), &same, &fewer, 3, &gateway);
        let counts = hyper_measure::alloc::end();
        assert!(again.settled && again.choosing.is_none());
        assert_eq!(again.now, Ok(Ask::of(&gateway, &fewer).choose()));
        assert_eq!((counts.allocations, counts.reallocations), (1_833, 3));
    }

    /// The scheme the model chooses on the compute pool is the one it chooses inline, at every
    /// count of domains a candidate fits, and over the tables above; a table whose domains grew
    /// asks the pool, its scheme held meanwhile, and takes the pool's answer once it landed.
    #[test]
    fn the_scheme_chosen_on_the_pool_is_the_one_chosen_inline() {
        let _pool = pool();
        let gateway = scheming_gateway();
        let names = [
            "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o",
        ];
        for count in 1..=names.len() {
            let standing = placed_in(&names[..count]);
            let inline = Ask::of(&gateway, &standing).choose();
            let ask = Ask::of(&gateway, &standing);
            let mut answer = compute::run(move || ask.choose()).unwrap();
            assert_eq!(answer.blocking_recv().unwrap(), inline, "{count} domains");
        }
        // Two domains, then three: the scheme of two fits three, so the choice goes to the pool.
        let two = placed_in(&["a", "b"]);
        let three = placed_in(&["a", "b", "c"]);
        let first = read(None, &[], &two, 1, &gateway);
        let held = first.now;
        let mut grown = read(Some(first), &two, &three, 2, &gateway);
        assert!(!grown.settled);
        assert_eq!(grown.now, held, "the scheme held until the answer lands");
        let mut choosing = grown.choosing.take().unwrap();
        assert_eq!(choosing.asked_at, 2);
        let landed = choosing.answer.blocking_recv().unwrap();
        assert_eq!(landed, Ask::of(&gateway, &three).choose());
        // Its answer, as it lands, is taken for the domains it was asked for.
        let (sender, answer) = hyper_rt::sync::oneshot().unwrap();
        sender.send(landed).unwrap();
        grown.choosing = Some(Choosing {
            asked_at: 2,
            answer,
        });
        grown.settle(Some(&gateway), &three);
        assert!(grown.settled && grown.choosing.is_none());
        assert_eq!(grown.now, Ok(landed));
    }

    /// An answer asked for domains the table has since left is dropped, the scheme held kept,
    /// and the choice asked again for the domains the table has; a choice that unwound is a
    /// typed refusal of the scheme until the domains change.
    #[test]
    fn a_stale_answer_is_dropped_and_the_choice_asked_again() {
        let _pool = pool();
        let gateway = scheming_gateway();
        let two = placed_in(&["a", "b"]);
        let three = placed_in(&["a", "b", "c"]);
        let four = placed_in(&["a", "b", "c", "d"]);
        let first = read(None, &[], &two, 1, &gateway);
        let held = first.now;
        let release = hold_the_worker();
        // Grown to three at 2, the choice waiting behind the held worker; grown to four at 3
        // before it ran: the one choice still runs, now stale.
        let grown = read(Some(first), &two, &three, 2, &gateway);
        let mut again = read(Some(grown), &three, &four, 3, &gateway);
        assert_eq!(again.choosing.as_ref().map(|c| c.asked_at), Some(2));
        assert_eq!(again.now, held);
        // Its answer for three domains lands: not taken, and a choice asked for four.
        drop(release);
        let mut stale = again.choosing.take().unwrap();
        let answer = stale.answer.blocking_recv().unwrap();
        let (sender, landed) = hyper_rt::sync::oneshot().unwrap();
        sender.send(answer).unwrap();
        again.choosing = Some(Choosing {
            asked_at: 2,
            answer: landed,
        });
        again.settle(Some(&gateway), &four);
        assert_eq!(again.now, held, "the stale answer dropped");
        assert!(!again.settled);
        let mut fresh = again.choosing.take().unwrap();
        assert_eq!(fresh.asked_at, 3);
        assert_eq!(
            fresh.answer.blocking_recv().unwrap(),
            Ask::of(&gateway, &four).choose()
        );
        // A choice for the current domains whose job unwound: its sender gone without a value.
        let (sender, unwound) = hyper_rt::sync::oneshot::<Option<durability::Scheme>>().unwrap();
        drop(sender);
        again.choosing = Some(Choosing {
            asked_at: 3,
            answer: unwound,
        });
        again.settle(Some(&gateway), &four);
        assert!(again.settled && again.choosing.is_none());
        assert_eq!(again.now, Err(Unwound));
        let table = Table {
            applied: 3,
            placed: four.clone(),
            facts: Vec::new(),
            standing: four,
            scheming: again,
        };
        assert!(matches!(table.scheme(), Err(NodeError::Runtime(_))));
    }

    /// A choice the pool refuses, at its bound here, is made at once on the shard, as it was
    /// before the pool: the table never waits on a choice nobody runs.
    #[test]
    fn a_refused_choice_is_made_at_once_on_the_shard() {
        let _pool = pool();
        let gateway = scheming_gateway();
        let two = placed_in(&["a", "b"]);
        let three = placed_in(&["a", "b", "c"]);
        let first = read(None, &[], &two, 1, &gateway);
        let release = hold_the_worker();
        // The pool's room filled behind its held worker, a job at a time until one is refused.
        let mut filled = Vec::new();
        for _ in 0..64 {
            match compute::run(|| ()) {
                Ok(receiver) => filled.push(receiver),
                Err(refused) => {
                    assert!(matches!(refused.error, hyper_rt::RtError::Capacity { .. }));
                    break;
                }
            }
        }
        let grown = read(Some(first), &two, &three, 2, &gateway);
        drop(release);
        assert!(grown.settled && grown.choosing.is_none());
        assert_eq!(grown.now, Ok(Ask::of(&gateway, &three).choose()));
    }

    /// A reading whose domains changed under a scheme that still fits them returns before the
    /// model runs: with the pool's worker held, the choice cannot have run, yet the reading is
    /// done, its scheme the one held; and the shard did a fraction of the model's work,
    /// allocating less than the model alone does inline.
    #[test]
    fn the_shard_is_not_held_while_the_model_chooses() {
        let _pool = pool();
        let gateway = scheming_gateway();
        let two = placed_in(&["a", "b"]);
        let three = placed_in(&["a", "b", "c"]);
        let first = read(None, &[], &two, 1, &gateway);
        let held = first.now;
        hyper_measure::alloc::begin();
        let inline = Ask::of(&gateway, &three).choose();
        let model = hyper_measure::alloc::end();
        let release = hold_the_worker();
        hyper_measure::alloc::begin();
        let grown = read(Some(first), &two, &three, 2, &gateway);
        let shard = hyper_measure::alloc::end();
        assert_eq!(grown.now, held);
        assert!(!grown.settled);
        let mut choosing = grown.choosing.unwrap();
        assert!(choosing.answer.try_recv().unwrap().is_none(), "not run yet");
        assert!(
            shard.allocations < model.allocations,
            "the shard's {shard:?} against the model's {model:?}"
        );
        drop(release);
        assert_eq!(choosing.answer.blocking_recv().unwrap(), inline);
    }

    /// A write is charged at the widest window from the floor's to the one learned whose charge
    /// the operations' bytes hold: one learned past them is held to what they hold, so the write
    /// is admitted alone and never waits at the door for good; with room, the learned one stands.
    #[test]
    fn a_writes_window_is_held_to_what_the_operations_bytes_hold() {
        let charges = || crate::derive::Charges::new(1, &[durability::Scheme::Copies(3)]).unwrap();
        let length = 64u64 << 20;
        let learned = usize::try_from(length.div_ceil(charges().largest_block())).unwrap();
        let mut gateway = GatewaySettings {
            keys: Vec::new(),
            handover_ns: 1,
            window: 1,
            objects: 1,
            objects_bytes: crate::derive::most_charged(&charges()),
            charges: charges(),
            clients: 1,
            bucket_quota: 1,
            scan_rows: 1,
            rates: Rates {
                chunk: 0.000_001_552_511_415_525_114_2,
                domains: None,
                bursts: Vec::new(),
                repair: 0.041_666_666_666_666_664,
            },
            target: 1e-11,
        };
        let charge = |window| charges().write(window, length);
        assert!(charge(learned) > gateway.objects_bytes);
        let window = fitting(&gateway, length, learned);
        assert!((1..learned).contains(&window), "{window} of {learned}");
        assert!(charge(window) <= gateway.objects_bytes);
        assert!(charge(window + 1) > gateway.objects_bytes);
        gateway.objects_bytes = charge(learned);
        assert_eq!(fitting(&gateway, length, learned), learned);
    }

    /// A get blocked before reading its version must still be refused at its own deadline;
    /// once its reply began, expiry must only stop its notes, preserving the one reply. Neither
    /// its pending read nor notes from a peer extend its local bound (research/77 D2).
    #[test]
    fn a_held_get_stops_vouching_at_its_bound_before_or_after_its_reply() {
        let patience = u64::try_from(bounds::STOCK_PATIENCE.as_nanos()).unwrap();
        let (_body, body) = hyper_rt::sync::channel(1).unwrap();
        let (reply, _answers) = hyper_rt::sync::channel(1).unwrap();
        let mut job = Job {
            door: Door::Local(Box::new(Local {
                body,
                reply,
                ended: None,
                pending: VecDeque::new(),
                ready: false,
            })),
            principal: "client".into(),
            work: getting("b".into(), "k".into(), Headers::default(), None, false),
            pending: vec![(Pending::Read(1), None)],
            deadline_ns: Some(10),
            moved_ns: 1,
            admitted_ns: Some(1),
            charge: 0,
            window: 1,
            label: None,
        };
        assert!(job.held());
        assert_eq!(job.bounded(9), Bounded::Held);
        assert_eq!(job.bounded(10), Bounded::SlowDown, "no reply has begun");
        assert_eq!(job.due_ns(9, 2), Some(10), "wake the S3 job at its bound");

        let Work::Get(getting) = &mut job.work else {
            panic!("the job is a get");
        };
        getting.get =
            Some(Get::new(Some(1), 1, 0..1, Box::new(RootKeys { keys: Vec::new() }), 1).unwrap());
        assert_eq!(job.bounded(9), Bounded::Held);
        assert_eq!(job.bounded(10), Bounded::Unvouched, "its reply has begun");
        assert_eq!(job.bounded(u64::MAX), Bounded::Unvouched);
        assert_eq!(job.due_ns(9, 2), Some(10));
        assert_eq!(
            job.due_ns(10, 2),
            None,
            "an expired reply needs no vouch beat"
        );

        job.deadline_ns = None;
        assert_eq!(job.bounded(patience), Bounded::Held);
        assert_eq!(job.bounded(patience + 1), Bounded::Unvouched);
        // Actual bytes or an answered ask advance the idle bound; a note does not.
        job.moved_ns = patience;
        assert_eq!(job.bounded(patience + 1), Bounded::Held);
        job.deadline_ns = Some(patience + 1);
        assert_eq!(job.bounded(patience + 1), Bounded::Unvouched);
    }

    fn refused(work: &Work) -> Option<Status> {
        match work {
            Work::Draining(reply) => Some(reply.status),
            _ => None,
        }
    }

    /// What a request's head alone refuses, before anything is read or written: a part declared
    /// past S3's 5 GiB, an object past a single PUT's, and a checksum type its algorithm does not
    /// have (05 §3.3, §4.1).
    #[test]
    fn a_request_past_s3s_bounds_is_refused_before_anything_is_read() {
        let part = |length| Op::UploadPart {
            bucket: "b".into(),
            key: "k".into(),
            upload: "0123456789ABC".into(),
            number: 1,
            id: 1,
            length,
            checksum: None,
            md5: None,
            trailing: None,
        };
        assert_eq!(
            refused(&work(part(name::MAX_PART + 1), Vec::new(), None)),
            Some(Status::EntityTooLarge)
        );
        assert_eq!(refused(&work(part(name::MAX_PART), Vec::new(), None)), None);
        let put = Op::Put {
            bucket: "b".into(),
            key: "k".into(),
            id: 1,
            length: mantle_s3::body::MAX_UPLOAD + 1,
            headers: Headers::default(),
            checksum: None,
            md5: None,
            trailing: None,
            metadata: Vec::new(),
        };
        assert_eq!(
            refused(&work(put, Vec::new(), None)),
            Some(Status::EntityTooLarge)
        );
        // A checksum in the fields and a trailer's both.
        let twice = Op::Put {
            bucket: "b".into(),
            key: "k".into(),
            id: 1,
            length: 1,
            headers: Headers::default(),
            checksum: Some(checksum::Checksum {
                algorithm: checksum::Algorithm::Crc32,
                bytes: vec![0; 4],
            }),
            md5: None,
            trailing: Some(checksum::Algorithm::Crc32),
            metadata: Vec::new(),
        };
        assert_eq!(
            refused(&work(twice, Vec::new(), None)),
            Some(Status::InvalidRequest)
        );
        let create = |algorithm, full| Op::CreateUpload {
            bucket: "b".into(),
            key: "k".into(),
            id: 1,
            checksum: Some((algorithm, full)),
            metadata: Vec::new(),
        };
        for (algorithm, full, ok) in [
            (Algorithm::Crc64Nvme, true, true),
            (Algorithm::Crc64Nvme, false, false),
            (Algorithm::Sha256, true, false),
            (Algorithm::Sha256, false, true),
            (Algorithm::Crc32c, true, true),
            (Algorithm::Crc32c, false, true),
        ] {
            let status = refused(&work(create(algorithm, full), Vec::new(), None));
            assert_eq!(
                status,
                if ok {
                    None
                } else {
                    Some(Status::InvalidRequest)
                },
                "{algorithm:?} full {full}"
            );
        }
    }

    /// What research/89 adds is refused from its request alone, before anything is read: a
    /// version ID mantle never makes (InvalidArgument), metadata past S3's 2 KB
    /// (MetadataTooLarge) or that no HTTP field carries, a part copy's number past 10,000; and a
    /// copy counts as two of the host's operations.
    #[test]
    fn what_a_version_metadata_or_a_copy_names_is_refused_before_anything_is_read() {
        let get = |version: &str| Op::Get {
            bucket: "b".into(),
            key: "k".into(),
            headers: Headers::default(),
            version: Some(version.into()),
        };
        for named in ["null", "0123456789ABC"] {
            assert_eq!(
                refused(&work(get(named), Vec::new(), None)),
                None,
                "{named}"
            );
        }
        for invalid in ["", "nul", "0123456789AB", "0123456789ABCD", "0123456789ABU"] {
            assert_eq!(
                refused(&work(get(invalid), Vec::new(), None)),
                Some(Status::InvalidVersion),
                "{invalid:?}"
            );
        }
        let delete = Op::Delete {
            bucket: "b".into(),
            key: "k".into(),
            id: 1,
            headers: Headers::default(),
            version: Some("not one".into()),
        };
        assert_eq!(
            refused(&work(delete, Vec::new(), None)),
            Some(Status::InvalidVersion)
        );
        let put = |metadata: Metadata| Op::Put {
            bucket: "b".into(),
            key: "k".into(),
            id: 1,
            length: 1,
            headers: Headers::default(),
            checksum: None,
            md5: None,
            trailing: None,
            metadata,
        };
        let at = vec![(
            "x-amz-meta-k".to_owned(),
            "v".repeat(mantle_s3::metadata::MAX_USER - 1),
        )];
        assert_eq!(refused(&work(put(at), Vec::new(), None)), None);
        let past = vec![(
            "x-amz-meta-k".to_owned(),
            "v".repeat(mantle_s3::metadata::MAX_USER),
        )];
        assert_eq!(
            refused(&work(put(past), Vec::new(), None)),
            Some(Status::MetadataTooLarge)
        );
        let control = vec![("x-amz-meta-k".to_owned(), "a\nb".to_owned())];
        assert_eq!(
            refused(&work(put(control), Vec::new(), None)),
            Some(Status::InvalidArgument)
        );
        let source = mantle_s3::copy::Source {
            bucket: "b".into(),
            key: "k".into(),
            version: None,
        };
        let part = Op::CopyPart(Box::new(object::CopyPart {
            bucket: "b".into(),
            key: "k".into(),
            upload: "0123456789ABC".into(),
            number: name::MAX_PART_NUMBER + 1,
            id: 1,
            source: source.clone(),
            conditions: Headers::default(),
        }));
        assert_eq!(
            refused(&work(part, Vec::new(), None)),
            Some(Status::InvalidArgument)
        );
        let copy = Op::Copy(Box::new(object::Copy {
            bucket: "b".into(),
            key: "k2".into(),
            id: 1,
            source: mantle_s3::copy::Source {
                version: Some("bad".into()),
                ..source
            },
            conditions: Headers::default(),
            headers: Headers::default(),
            directive: Directive::Copy,
            metadata: Vec::new(),
            checksum: None,
        }));
        assert_eq!(weight(&copy), 2);
        assert_eq!(
            refused(&work(copy, Vec::new(), None)),
            Some(Status::InvalidVersion)
        );
        assert_eq!(weight(&get("null")), 1);
    }
}
