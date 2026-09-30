//! `mantle bench gateway`: the object path's PUT and GET on one core, against a cell in memory
//! (docs/design/gateway.md).
//!
//! The cell answers every request at once: volumes are maps of chunks, and the Block, File and
//! Name ranges apply their commands to the model engine. What is timed is then the path's own
//! work: sealing, hashing, coding, copying and its state machines, the cost per byte a
//! gateway's core pays whatever the network and devices add. A PUT's body carries a
//! CRC-64/NVME, as the SDKs send one by default. A GET reads the whole object, and again with
//! the volume of its first chunk failing every read, so that each block it held is decoded.
//!
//! Beside each rate, the round trips a PUT or GET waits through, a PUT's with one full block going
//! down at a time and with every block of the object at once ("all out"): every request it has out
//! answered together is one. With the cell's answers each taking a latency `T` rather than
//! none, an object of `S` bytes takes at least its round trips times `T`, whatever the path's
//! speed per byte, which is what bounds a latency-limited transfer (audit §16.3).

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::time::{Duration, Instant};

use bytes::Bytes;
use mantle_chunk::ChunkKey;
use mantle_disk::measure::SplitMix64;
use mantle_ec::durability::Scheme;
use mantle_gateway::get::{self, Get, GetError, Keyring};
use mantle_gateway::layout::{Layout, LayoutError};
use mantle_gateway::put::{
    Answer, Body, Commit, Expected, Holding, Ids, Keys, Put, PutError, Request,
};
use mantle_meta::engine::Model;
use mantle_meta::error::MetaError;
use mantle_meta::name::{self, GateChange, Preconditions};
use mantle_meta::record::{GateState, Version, Versioning, WrappedKey, Wrapper};
use mantle_meta::{block, file};
use mantle_s3::checksum::Algorithm;
use mantle_s3::seal::{DataKey, SealError, WrappingKey};

use crate::display;

const BUCKET: &str = "bench";

/// A handover no step of a benchmark comes near: the cell's clock moves one tick a command.
const HANDOVER: u64 = 1 << 40;

#[derive(Debug)]
pub enum Error {
    Output(std::io::Error),
    Put(PutError),
    Get(GetError),
    Meta(MetaError),
    Seal(SealError),
    Layout(LayoutError),
    /// The path answered otherwise than the benchmark set it up to.
    Unexpected(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Output(e) => write!(f, "writing output: {e}"),
            Self::Put(e) => write!(f, "PUT: {e}"),
            Self::Get(e) => write!(f, "GET: {e}"),
            Self::Meta(e) => write!(f, "applying: {e}"),
            Self::Seal(e) => write!(f, "sealing: {e}"),
            Self::Layout(e) => write!(f, "layout: {e}"),
            Self::Unexpected(what) => write!(f, "unexpected: {what}"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Output(e)
    }
}

impl From<PutError> for Error {
    fn from(e: PutError) -> Self {
        Self::Put(e)
    }
}

impl From<GetError> for Error {
    fn from(e: GetError) -> Self {
        Self::Get(e)
    }
}

impl From<MetaError> for Error {
    fn from(e: MetaError) -> Self {
        Self::Meta(e)
    }
}

impl From<SealError> for Error {
    fn from(e: SealError) -> Self {
        Self::Seal(e)
    }
}

impl From<LayoutError> for Error {
    fn from(e: LayoutError) -> Self {
        Self::Layout(e)
    }
}

/// The schemes measured: three copies, and RS(6,3), the code for 9–11 failure domains
/// (docs/research/04 §0).
pub fn schemes() -> Result<Vec<Scheme>, Error> {
    let code = mantle_ec::Code::new(6, 3).map_err(|e| Error::Unexpected(e.to_string()))?;
    Ok(vec![Scheme::Copies(3), Scheme::Rs(code)])
}

/// A cell in memory: volumes of chunks, and the Block, File and Name ranges on the model
/// engine.
struct Cell {
    volumes: BTreeMap<u128, HashMap<ChunkKey, Bytes>>,
    /// A volume every read of which fails.
    failing: Option<u128>,
    blocks: Model,
    files: Model,
    names: Model,
    index: u64,
}

impl Cell {
    fn new(volumes: u128) -> Result<Self, Error> {
        let mut names = Model::default();
        mantle_meta::engine::Engine::install(&mut names, 0, name::first(1)?)
            .map_err(|e| Error::Unexpected(format!("installing the Name range: {e}")))?;
        let mut cell = Self {
            volumes: (1..=volumes).map(|v| (v, HashMap::new())).collect(),
            failing: None,
            blocks: Model::default(),
            files: Model::default(),
            names,
            index: 0,
        };
        let open = name::Command::Gate(GateChange {
            bucket: BUCKET.into(),
            incarnation: 1,
            attempt: 1,
            from: None,
            to: Some(GateState::Open),
            generation: 1,
        });
        match cell.name(open)? {
            name::Outcome::GateMoved => Ok(cell),
            other => Err(Error::Unexpected(format!("opening the bucket: {other:?}"))),
        }
    }

    fn tick(&mut self) -> Result<u64, Error> {
        self.index = self
            .index
            .checked_add(1)
            .ok_or(Error::Unexpected("the log's index ran out".into()))?;
        Ok(self.index)
    }

    fn name(&mut self, mut command: name::Command) -> Result<name::Outcome, Error> {
        let index = self.tick()?;
        if let name::Command::Put(p) = &mut command {
            p.at_ns = index;
        }
        Ok(name::apply(&mut self.names, index, &command)?)
    }

    fn serve_put(&mut self, request: Request) -> Result<Answer, Error> {
        Ok(match request {
            Request::Place { width, .. } => Answer::Volumes(
                self.volumes
                    .keys()
                    .copied()
                    .take(width.saturating_add(1))
                    .collect(),
            ),
            Request::Chunk {
                volume, key, bytes, ..
            } => match self.volumes.get_mut(&volume) {
                Some(chunks) => {
                    chunks.insert(key, bytes);
                    Answer::Stored
                }
                None => Answer::Refused,
            },
            Request::Block(mut c) => {
                let index = self.tick()?;
                if let block::Command::Write { at_ns, .. } | block::Command::Renew { at_ns, .. } =
                    &mut c
                {
                    *at_ns = index;
                }
                Answer::Block(block::apply(&mut self.blocks, index, &c)?)
            }
            Request::File(mut c) => {
                let index = self.tick()?;
                if let file::Command::Write { at_ns, .. } = &mut c {
                    *at_ns = index;
                }
                Answer::File(file::apply(&mut self.files, index, &c)?)
            }
            Request::Name(c) => Answer::Name(self.name(*c)?),
        })
    }

    fn serve_get(&mut self, request: get::Request) -> Result<get::Answer, Error> {
        Ok(match request {
            get::Request::Header { file } => get::Answer::Header(file::header(&self.files, file)?),
            get::Request::Extents { file, offset, max } => {
                get::Answer::Extents(file::extents(&self.files, file, offset, max)?)
            }
            get::Request::Block { block } => get::Answer::Block(block::read(&self.blocks, block)?),
            get::Request::Chunk {
                volume,
                key,
                offset,
                len,
            } => {
                let bytes = self
                    .volumes
                    .get(&volume)
                    .filter(|_| self.failing != Some(volume))
                    .and_then(|chunks| chunks.get(&key));
                let range = usize::try_from(offset).ok().zip(
                    offset
                        .checked_add(len)
                        .and_then(|e| usize::try_from(e).ok()),
                );
                match (bytes, range) {
                    (Some(b), Some((from, to))) if to <= b.len() => {
                        get::Answer::Chunk(b.slice(from..to))
                    }
                    _ => get::Answer::Unreadable,
                }
            }
        })
    }

    /// Drops every chunk and row an earlier PUT left, but the bucket's gate.
    fn empty(&mut self) {
        for chunks in self.volumes.values_mut() {
            chunks.clear();
        }
        self.blocks = Model::default();
        self.files = Model::default();
    }
}

/// Block IDs counted up, never reused within a run.
struct Counter(u128);

impl Ids for Counter {
    fn block(&mut self) -> Result<u128, PutError> {
        self.0 = self.0.checked_add(1).ok_or(PutError::Overflow)?;
        Ok(self.0)
    }
}

struct Root(WrappingKey);

impl Keyring for Root {
    fn unwrap(&self, key: &WrappedKey) -> Result<DataKey, GetError> {
        DataKey::unwrap(&key.bytes, &self.0).map_err(|_| GetError::Key)
    }
}

fn commit() -> Commit {
    Commit::Object(name::Put {
        bucket: BUCKET.into(),
        incarnation: 1,
        key: "object".into(),
        versioning: Versioning::Unversioned,
        preconditions: Preconditions::default(),
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
            owner: "bench".into(),
            headers: Vec::new(),
            retention: None,
            legal_hold: None,
            listing: None,
        },
        default: None,
        deadline_ns: 0,
    })
}

/// PUTs `bytes` as file `file`; the round trips it waited through.
fn put(
    cell: &mut Cell,
    bytes: &[u8],
    layout: Layout,
    file: u128,
    ids: &mut u128,
    wrapping: &WrappingKey,
    window: usize,
) -> Result<u64, Error> {
    let data = DataKey::generate()?;
    let keys = Keys {
        file,
        wrapped: WrappedKey {
            by: Wrapper::Root(1),
            bytes: data.wrap(wrapping)?,
        },
        data,
    };
    let length =
        u64::try_from(bytes.len()).map_err(|_| Error::Unexpected("a body past u64".into()))?;
    let body = Body {
        length,
        checksum: Some(Algorithm::Crc64Nvme),
        content_md5: false,
    };
    let first = *ids;
    *ids = ids
        .checked_add(1 << 32)
        .ok_or(Error::Unexpected("block IDs ran out".into()))?;
    let mut put = Put::new(
        commit(),
        body,
        keys,
        layout,
        Holding {
            handover_ns: HANDOVER,
            window,
        },
        Box::new(Counter(first)),
        0,
    )?;
    let (mut at, mut ended, mut rounds) = (0usize, false, 0u64);
    loop {
        while at < bytes.len() && put.wants_body() {
            let took = put.feed(bytes.get(at..).unwrap_or_default())?;
            at = at.checked_add(took).ok_or(PutError::Overflow)?;
        }
        if at == bytes.len() && !ended {
            put.end(&Expected::default())?;
            ended = true;
        }
        let mut asked = Vec::new();
        while let Some(request) = put.poll() {
            asked.push(request);
        }
        if asked.is_empty() {
            return match put.outcome() {
                Some(Ok(_)) => Ok(rounds),
                Some(Err(e)) => Err(e.clone().into()),
                None => Err(Error::Unexpected("a PUT waits on nothing".into())),
            };
        }
        rounds = rounds.saturating_add(1);
        for (id, request) in asked {
            let answer = cell.serve_put(request)?;
            put.answer(id, answer)?;
        }
    }
}

/// GETs the whole of file `file` of `size` bytes; the bytes read and the round trips.
fn get(
    cell: &mut Cell,
    file: u128,
    size: u64,
    wrapping: &WrappingKey,
    window: usize,
) -> Result<(u64, u64), Error> {
    let root = Root(WrappingKey::new(wrapping.bytes()));
    let mut get = Get::new(Some(file), size, 0..size, Box::new(root), window)?;
    let (mut read, mut rounds) = (0u64, 0u64);
    loop {
        while let Some(bytes) = get.take() {
            let len = u64::try_from(bytes.len()).map_err(|_| GetError::Overflow)?;
            read = read.checked_add(len).ok_or(GetError::Overflow)?;
        }
        if let Some(outcome) = get.outcome() {
            return match outcome {
                Ok(()) => Ok((read, rounds)),
                Err(e) => Err(e.clone().into()),
            };
        }
        let mut asked = Vec::new();
        while let Some(request) = get.poll() {
            asked.push(request);
        }
        if asked.is_empty() {
            return Err(Error::Unexpected("a GET waits on nothing".into()));
        }
        rounds = rounds.saturating_add(1);
        for (id, request) in asked {
            let answer = cell.serve_get(request)?;
            get.answer(id, answer)?;
        }
    }
}

/// Plaintext bytes a second over runs of `f` repeated for `step`, and what the last run gave.
fn rate<T>(
    bytes: usize,
    step: Duration,
    mut f: impl FnMut() -> Result<T, Error>,
) -> Result<(f64, T), Error> {
    let started = Instant::now();
    let mut runs = 0u64;
    let mut last = f()?;
    runs = runs.saturating_add(1);
    while started.elapsed() < step {
        last = f()?;
        runs = runs.saturating_add(1);
    }
    let secs = started.elapsed().as_secs_f64();
    // u64 -> f64 rounds above 2^53, far beyond any count a bounded run produces.
    Ok((runs as f64 * bytes as f64 / secs, last))
}

pub fn gateway(
    out: &mut impl Write,
    schemes: &[Scheme],
    sizes: &[usize],
    step: Duration,
) -> Result<(), Error> {
    writeln!(
        out,
        "  {:<10} {:>8} {:>11} {:>7} {:>8} {:>11} {:>7} {:>8} {:>11}",
        "scheme", "object", "PUT", "rounds", "all out", "GET", "rounds", "all out", "GET 1 lost"
    )?;
    let wrapping = WrappingKey::generate()?;
    for &scheme in schemes {
        let layout = Layout::new(scheme)?;
        let volumes = u128::try_from(scheme.width())
            .map_err(|_| Error::Unexpected("a scheme past u128".into()))?
            .saturating_add(1);
        for &size in sizes {
            let mut bytes = vec![0u8; size];
            SplitMix64::new(u64::try_from(size).unwrap_or(0)).fill(&mut bytes);
            let length =
                u64::try_from(size).map_err(|_| Error::Unexpected("a size past u64".into()))?;
            let mut cell = Cell::new(volumes)?;
            let mut ids = 0u128;
            let mut file = 1u128;
            let (put_rate, put_rounds) = rate(size, step, || {
                cell.empty();
                file = file.saturating_add(1);
                put(&mut cell, &bytes, layout, file, &mut ids, &wrapping, 1)
            })?;
            cell.empty();
            let stored = u128::MAX;
            put(&mut cell, &bytes, layout, stored, &mut ids, &wrapping, 1)?;
            // Every block of the object in flight at once: the fewest round trips a window
            // can bring the PUT to.
            let blocks = usize::try_from(layout.blocks(length))
                .map_err(|_| Error::Unexpected("blocks past usize".into()))?;
            cell.empty();
            let wide_rounds = put(
                &mut cell,
                &bytes,
                layout,
                stored,
                &mut ids,
                &wrapping,
                blocks.max(1),
            )?;
            cell.empty();
            put(&mut cell, &bytes, layout, stored, &mut ids, &wrapping, 1)?;
            // Two blocks held, one read while one waits, and every block of the object.
            let (get_rate, (read, get_rounds)) =
                rate(size, step, || get(&mut cell, stored, length, &wrapping, 2))?;
            let (_, wide_get_rounds) = get(&mut cell, stored, length, &wrapping, blocks.max(1))?;
            if read != length {
                return Err(Error::Unexpected(format!(
                    "a GET read {read} of {length} bytes"
                )));
            }
            cell.failing = cell.volumes.keys().next().copied();
            let (lost_rate, _) = rate(size, step, || get(&mut cell, stored, length, &wrapping, 2))?;
            writeln!(
                out,
                "  {:<10} {:>8} {:>11} {:>7} {:>8} {:>11} {:>7} {:>8} {:>11}",
                match scheme {
                    Scheme::Copies(n) => format!("{n} copies"),
                    Scheme::Rs(code) => format!("RS({},{})", code.data(), code.parity()),
                },
                display::size(size),
                display::rate(put_rate),
                put_rounds,
                wide_rounds,
                display::rate(get_rate),
                get_rounds,
                wide_get_rounds,
                display::rate(lost_rate),
            )?;
            out.flush()?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_scheme_and_size_is_reported() {
        let mut out = Vec::new();
        gateway(
            &mut out,
            &schemes().unwrap(),
            &[100_000],
            Duration::from_millis(1),
        )
        .unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(
            text.contains("3 copies") && text.contains("RS(6,3)"),
            "{text}"
        );
    }
}
