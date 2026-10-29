//! `mantle bench transfer`: objects through a fresh cell of three, each read back through another
//! node and checked exactly (plan step 9; research/76). Each object's bytes are made from the run's
//! seed and its name (`generate`), so a read is checked by making them again and no copy of an
//! object exists outside the cell. A case is a set of objects on a cell of its own, made under the
//! run's directory and stopped when the case ends; what it held stays there.
//!
//! What a case keeps does not grow with its objects: fixed-size histograms of latency, the counts
//! of what each object came to, and a bit an object for whether it was acknowledged.

pub mod cell;
pub mod generate;
pub mod large;
pub mod native;
pub mod place;
pub mod stock;

use std::future::Future;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Instant;

use hyper_rt::runtime::LocalRuntime;
use mantle_client::{Client, Settings};
use mantle_disk::histogram::Histogram;
use mantle_protocol::classes::Bounds;
use mantle_protocol::object::{Op, Status};

use cell::Cell;
use generate::{GenError, Generator, Object};
use large::Way;
use native::Came;

/// S3's line for a small object: objects under 128 KiB are not monitored by S3 Intelligent-Tiering
/// and are billed as 128 KiB in the infrequent-access classes (research/76).
pub const SMALL_LINE: u64 = 128 << 10;

/// A segment of the layout, the 64 KiB a checkpoint falls on (gateway.md §1).
const SEGMENT: u64 = mantle_s3::seal::SEGMENT as u64;

/// The layout's boundaries every small set holds, whatever its draw: an empty object, a byte, and a
/// segment and a byte either side of it.
const BOUNDARIES: [u64; 5] = [0, 1, SEGMENT - 1, SEGMENT, SEGMENT + 1];

/// The bucket a case's objects go to.
const BUCKET: &str = "transfer";

/// What a run is asked to do.
pub struct Options {
    /// The run's directory: each case's cell is made in a directory of its own under it.
    pub dir: PathBuf,
    /// The seed every object's bytes and the small sets' sizes come from.
    pub seed: u64,
    /// The small sets' sizes, a case each.
    pub small: Vec<u64>,
    /// The large objects' sizes, a case each.
    pub large: Vec<u64>,
    /// The objects of a large case; none puts them until the cell refuses one as out of space
    /// through each door its ways use.
    pub count: Option<u64>,
    /// The ways a large case puts its objects, in turn; none, all four (`large::WAYS`).
    pub ways: Vec<Way>,
    /// Each founder's volume.
    pub volume: Volume,
}

/// Each founder's volume: stated bytes, or the run's file system's free space at each case's start
/// less a reserve its operator keeps free, split among the founders (plan step 9: a capacity case
/// fills what the machine can give).
#[derive(Clone, Copy, Debug)]
pub enum Volume {
    Bytes(u64),
    Free { reserve: u64 },
}

impl Volume {
    /// A founder's volume for the case in `dir`, and how it was come to.
    fn bytes(self, dir: &Path) -> Result<(u64, String), TransferError> {
        match self {
            Self::Bytes(bytes) => Ok((bytes, "stated".to_owned())),
            Self::Free { reserve } => {
                let free = mantle_disk::probe::identify(dir)
                    .file_system
                    .available_bytes
                    .ok_or_else(|| {
                        TransferError::Io(std::io::Error::other(
                            "the run's file system says no free space",
                        ))
                    })?;
                let bytes = free
                    .checked_sub(reserve)
                    .and_then(|room| room.checked_div(u64::try_from(cell::FOUNDERS).ok()?))
                    .filter(|bytes| *bytes > 0)
                    .ok_or_else(|| {
                        TransferError::Io(std::io::Error::other(format!(
                            "{free} bytes free, no more than the {reserve} kept free"
                        )))
                    })?;
                Ok((
                    bytes,
                    format!("{free} free less {reserve} kept, a third each"),
                ))
            }
        }
    }
}

#[derive(Debug)]
pub enum TransferError {
    Io(std::io::Error),
    Cell(cell::CellError),
    Init(crate::init::Error),
    Client(crate::client::Error),
    Native(mantle_client::ClientError),
    Stock(stock::StockError),
    Generate(GenError),
}

impl std::fmt::Display for TransferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "{e}"),
            Self::Cell(e) => write!(f, "the cell: {e}"),
            Self::Init(e) => write!(f, "the case's client: {e}"),
            Self::Client(e) => write!(f, "the case's client: {e}"),
            Self::Native(e) => write!(f, "the native door: {e}"),
            Self::Stock(e) => write!(f, "the HTTP door: {e}"),
            Self::Generate(e) => write!(f, "the generator: {e}"),
        }
    }
}

impl From<std::io::Error> for TransferError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<cell::CellError> for TransferError {
    fn from(e: cell::CellError) -> Self {
        Self::Cell(e)
    }
}

impl From<mantle_client::ClientError> for TransferError {
    fn from(e: mantle_client::ClientError) -> Self {
        Self::Native(e)
    }
}

impl From<GenError> for TransferError {
    fn from(e: GenError) -> Self {
        Self::Generate(e)
    }
}

/// What a case's objects put through one door came to, read back through both.
pub struct Tally {
    pub objects: u64,
    pub bytes: u64,
    /// Puts answered other than done or out of space.
    pub refused: u64,
    /// The statuses those puts were answered with, each with its count: an entry a status, so no
    /// more entries than there are statuses.
    pub refusals: Vec<(Status, u64)>,
    /// Puts refused as out of space: what a filling case puts until.
    pub full: u64,
    /// Requests asked again, the same, after their answers were lost: an exchange the client gave
    /// up on before the node answered (gateway.md §2.1).
    pub again: u64,
    /// Reads back, through either door, with a byte or a length not the generator's.
    pub wrong: u64,
    /// Reads back, through either door, of an acknowledged object not answered with it.
    pub missing: u64,
    pub put: Histogram,
    /// Reads back through the native door and through the S3 listener.
    pub native: Histogram,
    pub http: Histogram,
    /// The nanoseconds the acknowledged puts and the reads through each door took in all.
    pub put_ns: u64,
    pub native_ns: u64,
    pub http_ns: u64,
}

impl Tally {
    fn new() -> Self {
        Self {
            objects: 0,
            bytes: 0,
            refused: 0,
            refusals: Vec::new(),
            full: 0,
            again: 0,
            wrong: 0,
            missing: 0,
            put: Histogram::new(),
            native: Histogram::new(),
            http: Histogram::new(),
            put_ns: 0,
            native_ns: 0,
            http_ns: 0,
        }
    }

    /// Counts a put of `length` bytes that came to `came` after `took` nanoseconds: whether it was
    /// acknowledged.
    fn put_came(&mut self, length: u64, came: Came, took: u64) -> bool {
        self.put.record(took);
        self.objects = self.objects.saturating_add(1);
        if came == Came::Done {
            self.bytes = self.bytes.saturating_add(length);
            self.put_ns = self.put_ns.saturating_add(took);
            true
        } else {
            self.refused = self.refused.saturating_add(1);
            if let Came::Refused(status) = came {
                match self.refusals.iter_mut().find(|(s, _)| *s == status) {
                    Some((_, count)) => *count = count.saturating_add(1),
                    None => self.refusals.push((status, 1)),
                }
            }
            false
        }
    }

    /// Counts `again` requests asked again after their answers were lost.
    fn asked_again(&mut self, again: u32) {
        self.again = self.again.saturating_add(u64::from(again));
    }

    /// Counts a read back through the native door.
    fn native_came(&mut self, came: Came, took: u64) {
        self.native.record(took);
        self.native_ns = self.native_ns.saturating_add(took);
        self.read(came);
    }

    /// Counts a read back through the S3 listener.
    fn http_came(&mut self, came: Came, took: u64) {
        self.http.record(took);
        self.http_ns = self.http_ns.saturating_add(took);
        self.read(came);
    }

    /// Every object put was read back through both doors, every byte the generator's.
    pub fn exact(&self) -> bool {
        self.refused == 0 && self.wrong == 0 && self.missing == 0
    }

    fn read(&mut self, came: Came) {
        match came {
            Came::Done => {}
            Came::Wrong(_) => self.wrong = self.wrong.saturating_add(1),
            Came::Refused(_) => self.missing = self.missing.saturating_add(1),
        }
    }
}

/// The native status an S3 error code names, for a refusal through the S3 listener.
pub fn status_of_code(code: &str) -> Status {
    match code {
        // A HEAD's 404 carries no body to name its code: the AWS CLI's copy of an object to
        // standard output heads it first.
        "NoSuchKey" | "404" => Status::NoSuchKey,
        "NoSuchBucket" => Status::NoSuchBucket,
        "SlowDown" => Status::SlowDown,
        "InsufficientStorage" => Status::InsufficientStorage,
        _ => Status::Failed,
    }
}

/// The native door of a case: its client's directory, made by `mantle init client`, the cell's
/// nodes, and the runtime its clients run on, a client a phase.
struct Natives {
    dir: PathBuf,
    nodes: Vec<(u64, SocketAddr)>,
    rt: LocalRuntime,
}

impl Natives {
    /// The case's native door, its runtime's interests the sockets of a client of the cell or of
    /// its gateway's reader, whichever holds more: one of them runs at a time.
    fn new(cell: &Cell, dir: &Path, messages: Bounds) -> Result<Self, TransferError> {
        let client = dir.join("client");
        crate::init::client(&mut Vec::new(), &client, cell.out()).map_err(TransferError::Init)?;
        let nodes = cell.quic();
        let sockets = mantle_client::sockets(nodes.len())?.max(cell.gateway().sockets(messages)?);
        let rt = crate::client::runtime_of(&client, 1, sockets).map_err(TransferError::Client)?;
        Ok(Self {
            dir: client,
            nodes,
            rt,
        })
    }

    /// A client's settings asking node `first` first, then the rest in turn, its credentials read
    /// from the client's directory: a key is never copied.
    fn settings(&self, first: usize) -> Result<Settings, TransferError> {
        let configured = crate::client::configured(&self.dir).map_err(TransferError::Client)?;
        let mut nodes = self.nodes.clone();
        let first = first.min(nodes.len());
        nodes.rotate_left(first);
        Ok(Settings {
            id: configured.id,
            bind: SocketAddr::from(([127, 0, 0, 1], 0)),
            credentials: configured.credentials,
            transport_bytes: mantle_client::least_transport(nodes.len())?,
            nodes,
        })
    }

    /// Learns where the cell's nodes listen now: a node started again chose new ports.
    fn renew(&mut self, cell: &Cell) {
        self.nodes = cell.quic();
    }

    /// Reads every object of a case acknowledged in `oracle`, `count` of them named by `objects`
    /// and made from `seed`, natively through node `first` first: how many were read, wrong,
    /// missing and asked again.
    fn read_all(
        &mut self,
        first: usize,
        (objects, oracle, count, seed): (Objects, &Oracle, u64, u64),
    ) -> Result<(u64, u64, u64, u64), TransferError> {
        let settings = self.settings(first)?;
        let acknowledged = oracle.clone();
        self.block(async move {
            let generator = Generator::new(seed)?;
            let sizes = generator.object("sizes")?;
            let mut client = Client::open(settings).await?;
            let (mut read, mut wrong, mut missing, mut asked) = (0u64, 0u64, 0u64, 0u64);
            for i in (0..count).filter(|i| acknowledged.has(*i)) {
                let (key, length) = objects.of(i, &sizes)?;
                let object = generator.object(&key)?;
                let (came, again) =
                    native::get(&mut client, (BUCKET, &key), &object, length).await?;
                match came {
                    Came::Done => {}
                    Came::Wrong(_) => wrong = wrong.saturating_add(1),
                    Came::Refused(_) => missing = missing.saturating_add(1),
                }
                read = read.saturating_add(1);
                asked = asked.saturating_add(u64::from(again));
            }
            Ok((read, wrong, missing, asked))
        })
    }

    /// Runs a phase on the case's runtime: what it came to.
    fn block<T: 'static>(
        &mut self,
        phase: impl Future<Output = Result<T, TransferError>> + 'static,
    ) -> Result<T, TransferError> {
        flatten(self.rt.block_on(phase))
    }

    /// Makes the case's bucket through the first node.
    fn create(&mut self, bucket: &'static str) -> Result<(), TransferError> {
        let settings = self.settings(0)?;
        self.block(async move {
            let mut client = Client::open(settings).await?;
            let made = client
                .call(
                    &Op::Create {
                        bucket: bucket.into(),
                        id: mantle_client::identity()?,
                    },
                    u64::MAX,
                )
                .await?;
            if made.status == Status::Done {
                Ok(())
            } else {
                Err(TransferError::Native(mantle_client::ClientError::Reply(
                    "the case's bucket was not made",
                )))
            }
        })
    }
}

/// Which of a set's objects were put and acknowledged: a bit each, the set's one record of them.
#[derive(Clone)]
struct Oracle(Vec<u64>);

impl Oracle {
    fn new(count: u64) -> Result<Self, GenError> {
        let whole = count.checked_div(64).ok_or(GenError::Offset)?;
        let part = u64::from(count.checked_rem(64).ok_or(GenError::Offset)? != 0);
        let words = whole.checked_add(part).ok_or(GenError::Offset)?;
        Ok(Self(vec![
            0;
            usize::try_from(words)
                .map_err(|_| GenError::Offset)?
        ]))
    }

    fn word(i: u64) -> Option<(usize, u64)> {
        let word = usize::try_from(i.checked_div(64)?).ok()?;
        let bit = 1u64.checked_shl(u32::try_from(i.checked_rem(64)?).ok()?)?;
        Some((word, bit))
    }

    fn set(&mut self, i: u64) {
        if let Some((word, bit)) = Self::word(i)
            && let Some(slot) = self.0.get_mut(word)
        {
            *slot |= bit;
        }
    }

    fn has(&self, i: u64) -> bool {
        Self::word(i).is_some_and(|(word, bit)| self.0.get(word).is_some_and(|w| w & bit != 0))
    }
}

/// Small object `i`'s size: the layout's boundaries first, then each drawn below S3's small-object
/// line from the run's keystream of sizes.
fn size(sizes: &Object<'_>, i: u64) -> Result<u64, GenError> {
    if let Some(boundary) = usize::try_from(i).ok().and_then(|i| BOUNDARIES.get(i)) {
        return Ok(*boundary);
    }
    let mut word = [0u8; 8];
    sizes.fill(i.checked_mul(8).ok_or(GenError::Offset)?, &mut word)?;
    u64::from_le_bytes(word)
        .checked_rem(SMALL_LINE)
        .ok_or(GenError::Offset)
}

/// Runs `options`' cases, a row each door to `out`: whether every check of every case was exact.
pub fn transfer(out: &mut impl Write, options: &Options) -> Result<bool, TransferError> {
    // The harness asks a cell's ranges as its gateway and its objects as its client, in one
    // process: it states the cell's message bounds, which the client's object messages fit.
    let messages = mantle_node::derive::messages()
        .map_err(|e| TransferError::Io(std::io::Error::other(e.to_string())))?;
    mantle_protocol::classes::state_bounds(messages)
        .map_err(|e| TransferError::Io(std::io::Error::other(e.to_string())))?;
    if !options.small.is_empty() {
        writeln!(
            out,
            "  {:<18} {:>8} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>5} {:>7} {:>6} {:>7}",
            "case",
            "objects",
            "bytes",
            "put p50",
            "put p99",
            "native p50",
            "native p99",
            "http p50",
            "http p99",
            "again",
            "refused",
            "wrong",
            "missing"
        )?;
    }
    std::fs::create_dir_all(&options.dir)?;
    let mut exact = true;
    for &count in &options.small {
        // A case's directory is its own: one an earlier run left is refused, not reused.
        let dir = options.dir.join(format!("small-{count}"));
        std::fs::create_dir(&dir)?;
        let case = format!("small {count}");
        let (volume_bytes, why) = options.volume.bytes(&dir)?;
        writeln!(out, "  {case} volumes: {volume_bytes} bytes each ({why})")?;
        let mut cell = Cell::make(&dir, volume_bytes)?;
        let mut natives = Natives::new(&cell, &dir, messages)?;
        let (tallies, oracle) = small(&cell, &mut natives, &dir, options.seed, count)?;
        exact &= placed(
            out,
            (&cell, &mut natives, messages),
            &case,
            (Objects::Small, &oracle, count, options.seed),
        )?;
        exact &= accounted(out, &mut cell, &case, volume_bytes)?;
        exact &= killed(
            out,
            (&mut cell, &mut natives),
            &case,
            (Objects::Small, &oracle, count, options.seed),
        )?;
        exact &= controls(
            out,
            (&cell, &mut natives, messages),
            &case,
            (Objects::Small, &oracle, count, options.seed),
        )?;
        drop(natives);
        drop(cell);
        for (door, tally) in ["native", "http"].iter().zip(&tallies) {
            writeln!(
                out,
                "  {:<18} {:>8} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>5} {:>7} {:>6} {:>7}",
                format!("small {count} {door}"),
                tally.objects,
                tally.bytes,
                micros(tally.put.p50()),
                micros(tally.put.p99()),
                micros(tally.native.p50()),
                micros(tally.native.p99()),
                micros(tally.http.p50()),
                micros(tally.http.p99()),
                tally.again,
                tally.refused,
                tally.wrong,
                tally.missing,
            )?;
            if !tally.refusals.is_empty() {
                let said: Vec<String> = tally
                    .refusals
                    .iter()
                    .map(|(status, n)| format!("{status:?} {n}"))
                    .collect();
                writeln!(
                    out,
                    "  small {count} {door} refused with: {}",
                    said.join(", ")
                )?;
            }
            exact &= tally.exact();
        }
    }
    if !options.large.is_empty() {
        writeln!(
            out,
            "  {:<26} {:>8} {:>14} {:>12} {:>12} {:>12} {:>5} {:>5} {:>7} {:>6} {:>7}",
            "case",
            "objects",
            "bytes",
            "put",
            "native read",
            "http read",
            "again",
            "full",
            "refused",
            "wrong",
            "missing"
        )?;
    }
    for &length in &options.large {
        let dir = options.dir.join(format!("large-{length}"));
        std::fs::create_dir(&dir)?;
        let case = format!("large {length}");
        let (volume_bytes, why) = options.volume.bytes(&dir)?;
        writeln!(out, "  {case} volumes: {volume_bytes} bytes each ({why})")?;
        let mut cell = Cell::make(&dir, volume_bytes)?;
        let mut natives = Natives::new(&cell, &dir, messages)?;
        let ways: &[Way] = if options.ways.is_empty() {
            &large::WAYS
        } else {
            &options.ways
        };
        let (done, oracle, budget) = large::large(
            &cell,
            &mut natives,
            &dir,
            (options.seed, length, options.count, ways),
            volume_bytes,
        )?;
        exact &= placed(
            out,
            (&cell, &mut natives, messages),
            &case,
            (Objects::Large(length), &oracle, budget, options.seed),
        )?;
        exact &= accounted(out, &mut cell, &case, volume_bytes)?;
        exact &= killed(
            out,
            (&mut cell, &mut natives),
            &case,
            (Objects::Large(length), &oracle, budget, options.seed),
        )?;
        exact &= controls(
            out,
            (&cell, &mut natives, messages),
            &case,
            (Objects::Large(length), &oracle, budget, options.seed),
        )?;
        drop(natives);
        drop(cell);
        for (way, tally) in large::WAYS
            .iter()
            .zip(&done.tallies)
            .filter(|(way, _)| ways.contains(way))
        {
            writeln!(
                out,
                "  {:<26} {:>8} {:>14} {:>12} {:>12} {:>12} {:>5} {:>5} {:>7} {:>6} {:>7}",
                format!("large {length} {}", way.name()),
                tally.objects,
                tally.bytes,
                rate(tally.bytes, tally.put_ns),
                rate(tally.bytes, tally.native_ns),
                rate(tally.bytes, tally.http_ns),
                tally.again,
                tally.full,
                tally.refused,
                tally.wrong,
                tally.missing,
            )?;
        }
        let refused = |door: usize| match (done.doors.get(door), done.full.get(door)) {
            (Some(true), Some(Some(i))) => format!("l{i}"),
            (Some(true), _) => "none".to_owned(),
            _ => "not used".to_owned(),
        };
        writeln!(
            out,
            "  large {length} out of space: native {}, http {}; refused objects unreadable and \
             unlisted, acknowledged ones listed: {}",
            refused(0),
            refused(1),
            if done.unlisted { "yes" } else { "no" },
        )?;
        let otherwise: Vec<String> = done
            .refused
            .iter()
            .map(|(i, status)| format!("l{i} {status:?}"))
            .collect();
        writeln!(
            out,
            "  large {length} refused otherwise: {}",
            if otherwise.is_empty() {
                "none".to_owned()
            } else {
                otherwise.join(", ")
            },
        )?;
        exact &= done.exact(options.count.is_none());
    }
    Ok(exact)
}

/// What names a case's objects, so each is made again from its place in the case rather than kept:
/// a small set's key and drawn size, a large case's key and its one length.
#[derive(Clone, Copy)]
enum Objects {
    Small,
    Large(u64),
}

impl Objects {
    /// Object `i`'s key and length, a small set's size drawn from `sizes`.
    fn of(self, i: u64, sizes: &Object<'_>) -> Result<(String, u64), GenError> {
        Ok(match self {
            Self::Small => (format!("s{i}"), size(sizes, i)?),
            Self::Large(length) => (large::key(i), length),
        })
    }
}

/// Where the blocks of case `case`'s objects acknowledged in `oracle`, `count` of them named by
/// `objects` and made from `seed`, sit, read as the cell's gateway on the case's runtime, a line to
/// `out`: whether every block's chunks are on distinct nodes.
fn placed(
    out: &mut impl Write,
    (cell, natives, messages): (&Cell, &mut Natives, Bounds),
    case: &str,
    (objects, oracle, count, seed): (Objects, &Oracle, u64, u64),
) -> Result<bool, TransferError> {
    let settings = cell.gateway().settings(messages)?;
    let bound = cell.quic();
    let groups = cell.gateway().groups;
    let page = mantle_node::read::extents_page(mantle_node::read::reply_room(messages.request));
    let acknowledged = oracle.clone();
    let placement = natives.block(async move {
        let mut reader =
            place::Reader::open(settings, (bound, cell::FOUNDERS), (groups, page)).await?;
        let generator = Generator::new(seed)?;
        let sizes = generator.object("sizes")?;
        let mut placement = place::Placement::default();
        for i in (0..count).filter(|i| acknowledged.has(*i)) {
            let (key, length) = objects.of(i, &sizes)?;
            let (blocks, shared) = reader.object(BUCKET, (&key, length)).await?;
            placement.objects = placement.objects.saturating_add(1);
            placement.blocks = placement.blocks.saturating_add(blocks);
            placement.shared = placement.shared.saturating_add(shared);
        }
        Ok(placement)
    })?;
    writeln!(
        out,
        "  {case} placement: {} blocks of {} objects, each block's chunks on distinct nodes: {}",
        placement.blocks,
        placement.objects,
        if placement.exact() { "yes" } else { "no" },
    )?;
    Ok(placement.exact())
}

/// Each founder of case `case`'s cell killed in turn (SIGKILL), every object the case's `oracle`
/// acknowledged read back natively through the other two, and the founder started again on its
/// directory and every object read back through it, a line to `out`: whether every read was exact.
/// After the chunk bytes are accounted: a node started again counts them from nothing.
fn killed(
    out: &mut impl Write,
    (cell, natives): (&mut Cell, &mut Natives),
    case: &str,
    case_objects: (Objects, &Oracle, u64, u64),
) -> Result<bool, TransferError> {
    let (mut read, mut wrong, mut missing, mut again) = (0u64, 0u64, 0u64, 0u64);
    let mut count = |(r, w, m, a): (u64, u64, u64, u64)| {
        read = read.saturating_add(r);
        wrong = wrong.saturating_add(w);
        missing = missing.saturating_add(m);
        again = again.saturating_add(a);
    };
    for at in 0..cell::FOUNDERS {
        cell.kill(at)?;
        // Read once every range the killed founder led has another leader, a running one.
        cell.led()?;
        let other = at
            .checked_add(1)
            .and_then(|n| n.checked_rem(cell::FOUNDERS))
            .unwrap_or(0);
        count(natives.read_all(other, case_objects)?);
        cell.restart(at)?;
        natives.renew(cell);
        count(natives.read_all(at, case_objects)?);
    }
    writeln!(
        out,
        "  {case} kills: each founder killed in turn and started again, {read} reads through the \
         others and through it, {again} asked again, {wrong} wrong, {missing} missing",
    )?;
    Ok(wrong == 0 && missing == 0)
}

/// The key the missing-object control reads, which no case puts.
const NEVER_PUT: &str = "control-never-put";

/// The case's controls, each a fault its checks must catch, a line to `out`: the first
/// acknowledged object of a byte or more read back with its middle byte inverted on its way to the
/// check, which must find that byte; that object's first block judged with one chunk moved onto
/// another's node, which the placement check must count; and a key no case puts, which must read
/// as missing. Whether each was caught: a control the checks miss leaves the case inexact. A case
/// that acknowledged no such object, a large one whose first object no volume could hold, has no
/// bytes or placement to judge: the key never put is read all the same.
fn controls(
    out: &mut impl Write,
    (cell, natives, messages): (&Cell, &mut Natives, Bounds),
    case: &str,
    (objects, oracle, count, seed): (Objects, &Oracle, u64, u64),
) -> Result<bool, TransferError> {
    let generator = Generator::new(seed)?;
    let sizes = generator.object("sizes")?;
    let mut first = None;
    for i in (0..count).filter(|i| oracle.has(*i)) {
        let (key, length) = objects.of(i, &sizes)?;
        if length > 0 {
            first = Some((key, length));
            break;
        }
    }
    let settings = natives.settings(0)?;
    let Some((key, length)) = first else {
        let missing = natives.block(async move {
            let generator = Generator::new(seed)?;
            let never = generator.object(NEVER_PUT)?;
            let mut client = Client::open(settings).await?;
            let (missing, _) = native::get(&mut client, (BUCKET, NEVER_PUT), &never, 0).await?;
            Ok(missing)
        })?;
        let found_missing = missing == Came::Refused(Status::NoSuchKey);
        writeln!(
            out,
            "  {case} controls: no acknowledged object of a byte or more, no byte or chunk to \
             judge; a key never put read {}",
            if found_missing {
                "missing, caught"
            } else {
                "NOT missing"
            },
        )?;
        return Ok(found_missing);
    };
    let flip = length.checked_div(2).unwrap_or(0);
    let reader = cell.gateway().settings(messages)?;
    let bound = cell.quic();
    let groups = cell.gateway().groups;
    let page = mantle_node::read::extents_page(mantle_node::read::reply_room(messages.request));
    let read_key = key.clone();
    let (flipped, missing, moved) = natives.block(async move {
        let generator = Generator::new(seed)?;
        let object = generator.object(&read_key)?;
        let mut client = Client::open(settings).await?;
        let flipped = native::get_flipped(&mut client, (BUCKET, &read_key), &object, flip).await?;
        let never = generator.object(NEVER_PUT)?;
        let (missing, _) = native::get(&mut client, (BUCKET, NEVER_PUT), &never, 0).await?;
        let mut reader =
            place::Reader::open(reader, (bound, cell::FOUNDERS), (groups, page)).await?;
        let moved = reader.moved(BUCKET, &read_key).await?;
        Ok((flipped, missing, moved))
    })?;
    let found_flip = flipped == Came::Wrong(flip);
    let found_missing = missing == Came::Refused(Status::NoSuchKey);
    // An object whose blocks hold one chunk each has none to move: no control to judge.
    let found_moved = moved.unwrap_or(true);
    let said = |caught: bool| if caught { "caught" } else { "NOT caught" };
    writeln!(
        out,
        "  {case} controls: {key}'s byte {flip} inverted {}; a chunk of its first block moved \
         onto another's node {}; a key never put read {}",
        said(found_flip),
        match moved {
            Some(true) => "caught",
            Some(false) => "NOT caught",
            None => "not judged, a block of one chunk",
        },
        if found_missing {
            "missing, caught"
        } else {
            "NOT missing"
        },
    )?;
    Ok(found_flip && found_missing && found_moved)
}

/// Each node's chunk bytes once case `case` is done, a line to `out`: what each holds of its
/// `volume_bytes` and the room its volumes have left, in fragments and free segments (a large
/// case's refusal names which ran out), and whether what its peers sent it over QUIC and what it
/// put on its own volumes is exactly what its volumes hold and what none kept (a full volume's
/// refusals, a body ended before it was whole), every byte once: nothing removes a chunk yet.
fn accounted(
    out: &mut impl Write,
    cell: &mut Cell,
    case: &str,
    volume_bytes: u64,
) -> Result<bool, TransferError> {
    let mut held = Vec::with_capacity(cell::FOUNDERS);
    let mut exact = true;
    for at in 0..cell::FOUNDERS {
        let chunks = cell.chunks(at)?;
        exact &=
            chunks.received.checked_add(chunks.written) == chunks.held.checked_add(chunks.unkept);
        held.push(format!(
            "{} (+{} kept by none; {} of {} fragments, {} free)",
            chunks.held, chunks.unkept, chunks.fragments, chunks.budget, chunks.free
        ));
    }
    writeln!(
        out,
        "  {case} chunks held a node: {} of {volume_bytes}; each what it received over QUIC and \
         put in-process: {}",
        held.join(", "),
        if exact { "yes" } else { "no" },
    )?;
    Ok(exact)
}

/// `bytes` moved in `ns` nanoseconds, in MiB a second.
fn rate(bytes: u64, ns: u64) -> String {
    let per_second = u128::from(bytes)
        .checked_mul(1_000_000_000)
        .and_then(|b| b.checked_div(u128::from(ns)))
        .map_or(0, |b| b >> 20);
    format!("{per_second} MiB/s")
}

fn micros(ns: u64) -> String {
    format!("{} us", ns.checked_div(1_000).unwrap_or(0))
}

fn elapsed(began: Instant) -> u64 {
    u64::try_from(began.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// Which door object `i` of a set is put through: the native one for an even `i`, the S3
/// listener for an odd one; its tally's place.
fn door(i: u64) -> usize {
    usize::from(i.checked_rem(2) == Some(1))
}

/// The small set of `count` objects through `cell`'s two doors, in four phases so that neither
/// door waits on the other: the native door's objects put by a client of the first node; the S3
/// listener's put by boto3 through the third node's listener; then every acknowledged object read
/// back by a native client that asks the second node first; and then each again through the third
/// node's listener. Every object is so read through another node than it was put through.
fn small(
    cell: &Cell,
    natives: &mut Natives,
    dir: &Path,
    seed: u64,
    count: u64,
) -> Result<([Tally; 2], Oracle), TransferError> {
    let listeners = cell.s3();
    natives.create(BUCKET)?;

    // The native door's puts.
    let writer = natives.settings(0)?;
    let (mut tallies, mut oracle) = natives.block(async move {
        let generator = Generator::new(seed)?;
        let sizes = generator.object("sizes")?;
        let mut tallies = [Tally::new(), Tally::new()];
        let mut oracle = Oracle::new(count)?;
        let mut writer = Client::open(writer).await?;
        for i in (0..count).filter(|i| door(*i) == 0) {
            let length = size(&sizes, i)?;
            let name = format!("s{i}");
            let object = generator.object(&name)?;
            let began = Instant::now();
            let (came, again) = native::put(&mut writer, (BUCKET, &name), &object, length).await?;
            put_came(
                &mut tallies,
                &mut oracle,
                (i, length),
                (came, elapsed(began)),
            );
            if let Some(tally) = tallies.get_mut(door(i)) {
                tally.asked_again(again);
            }
        }
        Ok((tallies, oracle))
    })?;
    let generator = Generator::new(seed)?;
    let sizes = generator.object("sizes")?;

    // The S3 listener's puts.
    let (endpoint, region) =
        listeners
            .get(2)
            .cloned()
            .ok_or(TransferError::Cell(cell::CellError::Init(
                "no third listener".into(),
            )))?;
    let mut boto = stock::Boto::start(dir, cell.out(), (endpoint, &region), BUCKET)
        .map_err(TransferError::Stock)?;
    for i in (0..count).filter(|i| door(*i) == 1) {
        let length = size(&sizes, i)?;
        let name = format!("s{i}");
        let object = generator.object(&name)?;
        let began = Instant::now();
        let came = boto
            .put(&name, &object, length)
            .map_err(TransferError::Stock)?;
        put_came(
            &mut tallies,
            &mut oracle,
            (i, length),
            (came, elapsed(began)),
        );
    }

    // Every acknowledged object read back natively through the second node.
    let reader = natives.settings(1)?;
    let (mut tallies, oracle) = natives.block(async move {
        let generator = Generator::new(seed)?;
        let sizes = generator.object("sizes")?;
        let mut reader = Client::open(reader).await?;
        for i in (0..count).filter(|i| oracle.has(*i)) {
            let length = size(&sizes, i)?;
            let name = format!("s{i}");
            let object = generator.object(&name)?;
            let began = Instant::now();
            let (came, again) = native::get(&mut reader, (BUCKET, &name), &object, length).await?;
            if let Some(tally) = tallies.get_mut(door(i)) {
                tally.asked_again(again);
                tally.native_came(came, elapsed(began));
            }
        }
        Ok((tallies, oracle))
    })?;

    // And through the third node's listener.
    for i in (0..count).filter(|i| oracle.has(*i)) {
        let length = size(&sizes, i)?;
        let name = format!("s{i}");
        let object = generator.object(&name)?;
        let began = Instant::now();
        let came = boto
            .get(&name, &object, length)
            .map_err(TransferError::Stock)?;
        if let Some(tally) = tallies.get_mut(door(i)) {
            tally.http_came(came, elapsed(began));
        }
    }
    Ok((tallies, oracle))
}

/// Counts a put of object `i`, `length` bytes, that came to `came` after `took` nanoseconds.
fn put_came(
    tallies: &mut [Tally; 2],
    oracle: &mut Oracle,
    (i, length): (u64, u64),
    (came, took): (Came, u64),
) {
    if let Some(tally) = tallies.get_mut(door(i))
        && tally.put_came(length, came, took)
    {
        oracle.set(i);
    }
}

/// A phase's outcome, its runtime's error and its own as one.
fn flatten<T>(
    ran: Result<Result<T, TransferError>, hyper_rt::error::RtError>,
) -> Result<T, TransferError> {
    match ran {
        Ok(done) => done,
        Err(e) => Err(TransferError::Io(std::io::Error::other(format!("{e:?}")))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The controls: an acknowledged object read back as no such key counts missing, and one read
    /// back with a byte not the generator's counts wrong; either leaves the case inexact, as a put
    /// refused does.
    #[test]
    fn a_missing_object_or_a_wrong_byte_leaves_the_case_inexact() {
        let mut missing = Tally::new();
        assert!(missing.exact());
        missing.native_came(Came::Refused(Status::NoSuchKey), 1);
        assert_eq!(missing.missing, 1);
        assert!(!missing.exact());
        let mut wrong = Tally::new();
        wrong.http_came(Came::Wrong(7), 1);
        assert_eq!(wrong.wrong, 1);
        assert!(!wrong.exact());
        let mut refused = Tally::new();
        assert!(!refused.put_came(1, Came::Refused(Status::SlowDown), 1));
        assert!(!refused.exact());
    }

    /// Object `i`'s bit is the oracle's alone: set ones are held, the rest are not, past the
    /// last word too.
    #[test]
    fn the_oracle_holds_what_was_acknowledged() {
        let mut oracle = Oracle::new(130).unwrap();
        for i in [0, 63, 64, 129] {
            oracle.set(i);
        }
        let held: Vec<u64> = (0..140).filter(|i| oracle.has(*i)).collect();
        assert_eq!(held, [0, 63, 64, 129]);
    }
}
