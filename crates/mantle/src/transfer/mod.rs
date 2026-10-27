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

/// What a case's objects came to.
pub struct Tally {
    pub objects: u64,
    pub bytes: u64,
    /// Puts answered other than done.
    pub refused: u64,
    /// Objects read back with a byte or a length not the generator's.
    pub wrong: u64,
    /// Objects acknowledged and not read back.
    pub missing: u64,
    pub put: Histogram,
    pub get: Histogram,
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
            get: Histogram::new(),
        }
    }

    /// Every object put was read back, every byte the generator's.
    pub fn exact(&self) -> bool {
        self.refused == 0 && self.wrong == 0 && self.missing == 0
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

/// Runs `options`' cases, a row each to `out`: whether every check of every case was exact.
pub fn transfer(out: &mut impl Write, options: &Options) -> Result<bool, TransferError> {
    writeln!(
        out,
        "  {:<12} {:>8} {:>12} {:>10} {:>10} {:>10} {:>10} {:>7} {:>6} {:>7}",
        "case",
        "objects",
        "bytes",
        "put p50",
        "put p99",
        "get p50",
        "get p99",
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
        let tally = small(&cell, &dir, options.seed, count)?;
        drop(cell);
        writeln!(
            out,
            "  {:<12} {:>8} {:>12} {:>10} {:>10} {:>10} {:>10} {:>7} {:>6} {:>7}",
            format!("small {count}"),
            tally.objects,
            tally.bytes,
            micros(tally.put.p50()),
            micros(tally.put.p99()),
            micros(tally.get.p50()),
            micros(tally.get.p99()),
            tally.refused,
            tally.wrong,
            tally.missing,
        )?;
        exact &= tally.exact();
    }
    Ok(exact)
}

fn micros(ns: u64) -> String {
    format!("{} us", ns.checked_div(1_000).unwrap_or(0))
}

fn elapsed(began: Instant) -> u64 {
    u64::try_from(began.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// The small set of `count` objects through the native door of `cell`: each put, then read back
/// through a client that asks another node first.
fn small(cell: &Cell, dir: &Path, seed: u64, count: u64) -> Result<Tally, TransferError> {
    let client_dir = dir.join("client");
    crate::init::client(&mut Vec::new(), &client_dir, cell.out()).map_err(TransferError::Init)?;
    let nodes = cell.quic();
    let mut rt =
        crate::client::runtime(&client_dir, nodes.len(), 2).map_err(TransferError::Client)?;
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
    let (writer, reader) = (settings(nodes)?, settings(others)?);
    let ran = rt.block_on(async move {
        let mut writer = Client::open(writer).await?;
        let mut reader = Client::open(reader).await?;
        let made = writer
            .call(
                &Op::Create {
                    bucket: BUCKET.into(),
                    id: mantle_client::identity()?,
                },
                u64::MAX,
            )
            .await?;
        let mut tally = Tally::new();
        if made.status != Status::Done {
            tally.refused = tally.refused.saturating_add(1);
            return Ok::<_, TransferError>(tally);
        }
        let generator = Generator::new(seed)?;
        let sizes = generator.object("sizes")?;
        for i in 0..count {
            let length = size(&sizes, i)?;
            let name = format!("s{i}");
            let object = generator.object(&name)?;
            let began = Instant::now();
            let put = native::put(&mut writer, (BUCKET, &name), &object, length).await?;
            tally.put.record(elapsed(began));
            tally.objects = tally.objects.saturating_add(1);
            if put != Came::Done {
                tally.refused = tally.refused.saturating_add(1);
                continue;
            }
            tally.bytes = tally.bytes.saturating_add(length);
            let began = Instant::now();
            let got = native::get(&mut reader, (BUCKET, &name), &object, length).await?;
            tally.get.record(elapsed(began));
            match got {
                Came::Done => {}
                Came::Wrong(_) => tally.wrong = tally.wrong.saturating_add(1),
                Came::Refused(_) => tally.missing = tally.missing.saturating_add(1),
            }
        }
        Ok(tally)
    });
    match ran {
        Ok(Ok(tally)) => Ok(tally),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(TransferError::Io(std::io::Error::other(e.to_string()))),
    }
}
