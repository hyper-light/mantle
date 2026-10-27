//! `mantle bench transfer`: objects through a fresh cell of three, each read back through another
//! node and checked exactly (plan step 9; research/76). Each object's bytes are made from the run's
//! seed and its name (`generate`), so a read is checked by making them again and no copy of an
//! object exists outside the cell. A case is a set of objects on a cell of its own, made under the
//! run's directory and stopped when the case ends; what it held stays there.
//!
//! What a case keeps does not grow with its objects: two fixed-size histograms of latency and the
//! counts of what each object came to.

pub mod cell;
pub mod generate;
pub mod native;
pub mod stock;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

use mantle_client::{Client, Settings};
use mantle_disk::histogram::Histogram;
use mantle_protocol::object::{Op, Status};

use cell::Cell;
use generate::{GenError, Generator, Object};
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
    /// Each founder's volume.
    pub volume_bytes: u64,
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
    /// Puts answered other than done.
    pub refused: u64,
    /// Reads back, through either door, with a byte or a length not the generator's.
    pub wrong: u64,
    /// Reads back, through either door, of an acknowledged object not answered with it.
    pub missing: u64,
    pub put: Histogram,
    /// Reads back through the native door and through the S3 listener.
    pub native: Histogram,
    pub http: Histogram,
}

impl Tally {
    fn new() -> Self {
        Self {
            objects: 0,
            bytes: 0,
            refused: 0,
            wrong: 0,
            missing: 0,
            put: Histogram::new(),
            native: Histogram::new(),
            http: Histogram::new(),
        }
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
        "NoSuchKey" => Status::NoSuchKey,
        "NoSuchBucket" => Status::NoSuchBucket,
        "SlowDown" => Status::SlowDown,
        "InsufficientStorage" => Status::InsufficientStorage,
        _ => Status::Failed,
    }
}

/// Which of a set's objects were put and acknowledged: a bit each, the set's one record of them.
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
    writeln!(
        out,
        "  {:<18} {:>8} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>7} {:>6} {:>7}",
        "case",
        "objects",
        "bytes",
        "put p50",
        "put p99",
        "native p50",
        "native p99",
        "http p50",
        "http p99",
        "refused",
        "wrong",
        "missing"
    )?;
    std::fs::create_dir_all(&options.dir)?;
    let mut exact = true;
    for &count in &options.small {
        // A case's directory is its own: one an earlier run left is refused, not reused.
        let dir = options.dir.join(format!("small-{count}"));
        std::fs::create_dir(&dir)?;
        let cell = Cell::make(&dir, options.volume_bytes)?;
        let tallies = small(&cell, &dir, options.seed, count)?;
        drop(cell);
        for (door, tally) in ["native", "http"].iter().zip(&tallies) {
            writeln!(
                out,
                "  {:<18} {:>8} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>7} {:>6} {:>7}",
                format!("small {count} {door}"),
                tally.objects,
                tally.bytes,
                micros(tally.put.p50()),
                micros(tally.put.p99()),
                micros(tally.native.p50()),
                micros(tally.native.p99()),
                micros(tally.http.p50()),
                micros(tally.http.p99()),
                tally.refused,
                tally.wrong,
                tally.missing,
            )?;
            exact &= tally.exact();
        }
    }
    Ok(exact)
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
fn small(cell: &Cell, dir: &Path, seed: u64, count: u64) -> Result<[Tally; 2], TransferError> {
    let client_dir = dir.join("client");
    crate::init::client(&mut Vec::new(), &client_dir, cell.out()).map_err(TransferError::Init)?;
    let nodes = cell.quic();
    let listeners = cell.s3();
    let mut rt =
        crate::client::runtime(&client_dir, nodes.len(), 1).map_err(TransferError::Client)?;
    // Each client its own credentials, read from the client's directory: a key is never copied.
    let settings = |nodes: Vec<(u64, std::net::SocketAddr)>| -> Result<Settings, TransferError> {
        let configured = crate::client::configured(&client_dir).map_err(TransferError::Client)?;
        Ok(Settings {
            id: configured.id,
            bind: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            credentials: configured.credentials,
            transport_bytes: mantle_client::least_transport(nodes.len())?,
            nodes,
        })
    };
    let mut others = nodes.clone();
    others.rotate_left(1);

    // The native door's puts.
    let writer = settings(nodes)?;
    let put = rt.block_on(async move {
        let generator = Generator::new(seed)?;
        let sizes = generator.object("sizes")?;
        let mut tallies = [Tally::new(), Tally::new()];
        let mut oracle = Oracle::new(count)?;
        let mut writer = Client::open(writer).await?;
        let made = writer
            .call(
                &Op::Create {
                    bucket: BUCKET.into(),
                    id: mantle_client::identity()?,
                },
                u64::MAX,
            )
            .await?;
        if made.status != Status::Done {
            return Err(TransferError::Native(mantle_client::ClientError::Reply(
                "the case's bucket was not made",
            )));
        }
        for i in (0..count).filter(|i| door(*i) == 0) {
            let length = size(&sizes, i)?;
            let name = format!("s{i}");
            let object = generator.object(&name)?;
            let began = Instant::now();
            let came = native::put(&mut writer, (BUCKET, &name), &object, length).await?;
            put_came(
                &mut tallies,
                &mut oracle,
                (i, length),
                (came, elapsed(began)),
            );
        }
        Ok::<_, TransferError>((tallies, oracle))
    });
    let (mut tallies, mut oracle) = flatten(put)?;
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
    let reader = settings(others)?;
    let read = rt.block_on(async move {
        let generator = Generator::new(seed)?;
        let sizes = generator.object("sizes")?;
        let mut reader = Client::open(reader).await?;
        for i in (0..count).filter(|i| oracle.has(*i)) {
            let length = size(&sizes, i)?;
            let name = format!("s{i}");
            let object = generator.object(&name)?;
            let began = Instant::now();
            let came = native::get(&mut reader, (BUCKET, &name), &object, length).await?;
            if let Some(tally) = tallies.get_mut(door(i)) {
                tally.native.record(elapsed(began));
                tally.read(came);
            }
        }
        Ok::<_, TransferError>((tallies, oracle))
    });
    let (mut tallies, oracle) = flatten(read)?;

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
            tally.http.record(elapsed(began));
            tally.read(came);
        }
    }
    Ok(tallies)
}

/// Counts a put of object `i`, `length` bytes, that came to `came` after `took` nanoseconds.
fn put_came(
    tallies: &mut [Tally; 2],
    oracle: &mut Oracle,
    (i, length): (u64, u64),
    (came, took): (Came, u64),
) {
    let Some(tally) = tallies.get_mut(door(i)) else {
        return;
    };
    tally.put.record(took);
    tally.objects = tally.objects.saturating_add(1);
    if came == Came::Done {
        tally.bytes = tally.bytes.saturating_add(length);
        oracle.set(i);
    } else {
        tally.refused = tally.refused.saturating_add(1);
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
