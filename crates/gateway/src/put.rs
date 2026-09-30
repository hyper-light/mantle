//! Writing an object or a part (docs/design/gateway.md §2): the body sealed as it streams and
//! cut into blocks of whole segments; each block coded into chunks written at once to distinct
//! volumes, then recorded in the Block range and renewed there while the body streams on; the
//! file written in the File range once every block is; and the version or part committed in
//! the Name range, the write's linearization point.
//!
//! A [`Put`] names each request and moves on with its answer, doing no I/O, as the bucket
//! coordinator does. Its requests go out many at once, each with an id its answer names, and
//! the caller tells it the time. It holds at most two blocks: one filling while the one before
//! it is written, the body waiting while both are full.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use bytes::Bytes;
use mantle_chunk::ChunkKey;
use mantle_ec::EcError;
use mantle_ec::durability::Scheme;
use mantle_meta::record::{self, BlockHeader, ChunkPlace, Extent, Referrer, Target, WrappedKey};
use mantle_meta::{block, file, name};
use mantle_s3::body::MAX_UPLOAD;
use mantle_s3::checksum::{self, Algorithm, Hasher};
use mantle_s3::crypto::CryptoError;
use mantle_s3::seal::{self, DataKey, SealError, Segments};

use crate::layout::{Layout, LayoutError};

/// A block's deadline is renewed once this fraction of the handover time has passed since the
/// gateway asked for the write or renewal that set it: a quarter, as Centrifuge's owners renew
/// 60 s leases every 15 s, so three renewals in a row must be lost before one lapses
/// (docs/research/09 §7.2.2).
const RENEWALS: u64 = 4;

/// A request's name among those in flight.
pub type Id = u64;

/// A PUT's request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// Volumes for block `block`'s `width` chunks of `chunk_len` bytes each, all different, in
    /// the order to use them: one a chunk, and any more to take a chunk a volume refuses.
    Place {
        block: u128,
        width: usize,
        chunk_len: u64,
    },
    /// A chunk to its volume, with its CRC-32C, which the storage node checks before it
    /// writes.
    Chunk {
        volume: u128,
        key: ChunkKey,
        bytes: Bytes,
        crc: u32,
    },
    /// A command to the Block range that holds the block.
    Block(block::Command),
    /// A command to the File range that holds the file.
    File(file::Command),
    /// A command to the Name range that holds the key, which the sender routes.
    Name(Box<name::Command>),
}

/// What a request answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Volumes(Vec<u128>),
    /// The chunk is durable on its volume.
    Stored,
    /// The volume refused the chunk, or did not answer in time.
    Refused,
    Block(block::Outcome),
    File(file::Outcome),
    Name(name::Outcome),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PutError {
    #[error("the body ended before the length the request declared")]
    IncompleteBody,
    #[error("the body runs past the length the request declared")]
    TooLong,
    #[error("the body's MD5 is not the Content-MD5 the request sent")]
    BadDigest,
    /// A `Content-MD5` the PUT was not told of when it was made, which it has not taken.
    #[error("a Content-MD5 the PUT was not told of when it was made")]
    UndeclaredDigest,
    #[error("the body's checksum is not the one the request sent")]
    BadChecksum,
    /// No volume the placement offered took one of the block's chunks: `503 SlowDown`.
    #[error("no volume took a chunk of block {0:032x}")]
    Unplaced(u128),
    /// The sweep released a block before its file was written: a renewal came too late.
    #[error("block {0:032x} was released before its file was written")]
    Released(u128),
    #[error("the Block range answered {0:?}")]
    Block(block::Outcome),
    #[error("the File range answered {0:?}")]
    File(file::Outcome),
    /// The Name range refused the write: a precondition, the bucket, a lock, a deadline.
    #[error("the Name range answered {0:?}")]
    Name(name::Outcome),
    #[error("an answer of another kind than its request")]
    Mismatch,
    #[error("an answer to no request in flight: {0}")]
    Unknown(Id),
    #[error("the PUT is over")]
    Over,
    #[error("the operating system's random source failed")]
    Random,
    #[error("a length past what the PUT can address")]
    Overflow,
    /// A body longer than one request may carry: `400 EntityTooLarge`, before anything is
    /// written (audit B08).
    #[error("a body of {length} bytes; one request carries {max} at most")]
    EntityTooLarge { length: u64, max: u64 },

    #[error(transparent)]
    Layout(#[from] LayoutError),
    #[error(transparent)]
    Seal(#[from] SealError),
    #[error(transparent)]
    Crypto(#[from] CryptoError),
    #[error(transparent)]
    Code(#[from] EcError),
}

/// The Name write a PUT ends in, which the PUT fills with its file, size, ETag, checksum and
/// deadline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Commit {
    /// PutObject: a new version of the key. An empty body is the version alone, with no file.
    Object(name::Put),
    /// UploadPart: the part's row. A part names a file, so an empty part has one, holding one
    /// empty segment, sealed.
    Part(name::PutPart),
}

/// The body a PUT takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Body {
    /// Its length, as the request declares it.
    pub length: u64,
    /// The checksum the object or part keeps, if the request or its upload names one.
    pub checksum: Option<Algorithm>,
    /// Whether the request sent `Content-MD5`, a header and so known before the body: the
    /// plaintext's MD5 is then taken to check it even where the ETag is not that MD5.
    pub content_md5: bool,
}

/// What the request said the body would be, checked at its end.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Expected {
    /// `Content-MD5`.
    pub md5: Option<[u8; 16]>,
    /// `x-amz-checksum-*`, from a header or a trailer.
    pub checksum: Option<checksum::Checksum>,
}

/// A file's identity and key: a file ID never used before, and its data key, wrapped as the
/// file's header keeps it.
pub struct Keys {
    pub file: u128,
    pub data: DataKey,
    pub wrapped: WrappedKey,
}

/// Where a PUT draws its blocks' IDs, each random and never used before.
pub trait Ids {
    fn block(&mut self) -> Result<u128, PutError>;
}

/// Block IDs from the operating system's random source.
pub struct Random;

impl Ids for Random {
    fn block(&mut self) -> Result<u128, PutError> {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).map_err(|_| PutError::Random)?;
        Ok(u128::from_be_bytes(bytes))
    }
}

/// What a PUT committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    /// The version's ID; `None` for a part.
    pub version: Option<String>,
    /// Bare of quotes.
    pub etag: String,
    pub size: u64,
    pub checksum: Option<checksum::Checksum>,
}

/// The 16 bytes of a finished MD5.
fn md5_of(hasher: Hasher) -> Result<[u8; 16], PutError> {
    hasher
        .finish()?
        .bytes
        .as_slice()
        .try_into()
        .map_err(|_| PutError::Crypto(CryptoError))
}

/// A block on its way down: coded, placed, written, then recorded.
struct Going {
    /// Its place in the file.
    index: u64,
    id: u128,
    len: u64,
    header: BlockHeader,
    chunks: Vec<(Bytes, u32)>,
    /// The volumes placement offered, each once, and the first not yet given a chunk.
    volumes: Vec<u128>,
    next: usize,
    /// Each chunk's volume, and whether the chunk is durable there.
    at: Vec<Option<(u128, bool)>>,
}

/// A block the Block range recorded.
struct Recorded {
    id: u128,
    len: u64,
    /// Its deadline in the Block range's time, and when, in the caller's, the PUT asked for
    /// the write or renewal that set it: the renewal timer starts there, before the range
    /// stamps the deadline.
    deadline_ns: u64,
    asked_ns: u64,
    renewing: bool,
}

/// What a request in flight asked.
#[derive(Debug, Clone, Copy)]
enum Asked {
    Place,
    Chunk(usize),
    Block { asked_ns: u64 },
    Renew { index: u64, asked_ns: u64 },
    File,
    Name,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Taking the body; blocks go down as they fill.
    Body,
    /// The body ended; its blocks are still going down.
    Ended,
    /// Every block is recorded: the renewals due then are the last, and the file is written
    /// once they answer.
    Closing,
    /// The file's write is asked.
    File,
    /// The Name write is asked.
    Name,
}

pub struct Put {
    commit: Commit,
    body: Body,
    layout: Layout,
    file: u128,
    wrapped: WrappedKey,
    sealer: Segments,
    handover_ns: u64,
    ids: Box<dyn Ids + Send>,
    /// The caller's time, which only moves forward.
    now_ns: u64,
    /// Plaintext bytes taken, the segment being filled, and the segments sealed.
    taken: u64,
    segment: Vec<u8>,
    sealed: u64,
    /// The block being filled, sealed, and its place in the file.
    filling: Vec<u8>,
    filling_index: u64,
    /// A full block waiting for the one before it to be recorded.
    waiting: Option<Vec<u8>>,
    going: Option<Going>,
    /// Blocks recorded, by their place in the file.
    recorded: BTreeMap<u64, Recorded>,
    /// When each recorded block not being renewed is next due for renewal, and its place:
    /// a turn takes the due ones off the front, touching no other block (audit B08).
    due: BTreeSet<(u64, u64)>,
    /// The plaintext's MD5: the ETag under SSE-S3, and the check of a `Content-MD5`.
    md5: Option<Hasher>,
    /// The stored bytes' MD5, each segment as sealed: the ETag under SSE-C (encryption.md §4;
    /// audit B11).
    sealed_md5: Option<Hasher>,
    sum: Option<Hasher>,
    etag: Option<String>,
    checked: Option<checksum::Checksum>,
    ready: VecDeque<(Id, Request)>,
    asked: BTreeMap<Id, Asked>,
    next_id: Id,
    phase: Phase,
    outcome: Option<Result<Stored, PutError>>,
}

impl Put {
    /// A PUT of `body` ending in `commit`, sealed under `keys`, its blocks laid out by `layout`
    /// and held for its file's write `handover_ns` at a time, at the caller's time `now_ns`.
    pub fn new(
        commit: Commit,
        body: Body,
        keys: Keys,
        layout: Layout,
        handover_ns: u64,
        ids: Box<dyn Ids + Send>,
        now_ns: u64,
    ) -> Result<Self, PutError> {
        // What the body may cost is known from its declared length, and is refused whole
        // before a chunk is written (audit B08). Within it, every layout's blocks fit a
        // file's extents, so the File range's write never refuses a body after every chunk
        // was written, and the blocks retained for renewal are bounded
        // (`the_largest_upload_fits_a_file_under_every_layout`).
        if body.length > MAX_UPLOAD {
            return Err(PutError::EntityTooLarge {
                length: body.length,
                max: MAX_UPLOAD,
            });
        }
        let sealer = Segments::new(&keys.data, keys.file)?;
        // An SSE-S3 object's ETag is the MD5 of its plaintext, as S3's is; an SSE-C object's
        // is not (research/20 §5.1), and mantle's is the MD5 of its ciphertext
        // (encryption.md §4). The key's wrapping names which the object is.
        let customer = matches!(keys.wrapped.by, record::Wrapper::Customer);
        let md5 = if customer && !body.content_md5 {
            None
        } else {
            Some(Hasher::new(Algorithm::Md5)?)
        };
        let sealed_md5 = if customer {
            Some(Hasher::new(Algorithm::Md5)?)
        } else {
            None
        };
        let sum = body.checksum.map(Hasher::new).transpose()?;
        let sealed_segment = seal::SEGMENT
            .checked_add(seal::TAG)
            .ok_or(PutError::Overflow)?;
        Ok(Self {
            commit,
            body,
            layout,
            file: keys.file,
            wrapped: keys.wrapped,
            sealer,
            handover_ns,
            ids,
            now_ns,
            taken: 0,
            segment: Vec::with_capacity(sealed_segment),
            sealed: 0,
            filling: Vec::new(),
            filling_index: 0,
            waiting: None,
            going: None,
            recorded: BTreeMap::new(),
            due: BTreeSet::new(),
            md5,
            sealed_md5,
            sum,
            etag: None,
            checked: None,
            ready: VecDeque::new(),
            asked: BTreeMap::new(),
            next_id: 0,
            phase: Phase::Body,
            outcome: None,
        })
    }

    /// Takes plaintext bytes of the body, in order, and answers how many it took: fewer than
    /// offered while it holds two blocks, the rest to be offered again once an answer frees
    /// one ([`wants_body`](Self::wants_body)).
    pub fn feed(&mut self, bytes: &[u8]) -> Result<usize, PutError> {
        self.guard(|put| put.take(bytes))
    }

    /// Whether the PUT takes more of the body now.
    pub fn wants_body(&self) -> bool {
        self.outcome.is_none() && self.phase == Phase::Body && self.waiting.is_none()
    }

    /// The body ended, with what the request said it would be.
    pub fn end(&mut self, expected: &Expected) -> Result<(), PutError> {
        self.guard(|put| put.finish(expected))
    }

    /// The caller's time now: renewals that have come due are asked for.
    pub fn tick(&mut self, now_ns: u64) -> Result<(), PutError> {
        self.guard(|put| {
            put.now_ns = put.now_ns.max(now_ns);
            if matches!(put.phase, Phase::Body | Phase::Ended) {
                put.renew_due()?;
            }
            Ok(())
        })
    }

    /// When, in the caller's time, the next renewal comes due, for the caller to tick then;
    /// `None` while none waits.
    pub fn due_ns(&self) -> Option<u64> {
        if self.outcome.is_some() || !matches!(self.phase, Phase::Body | Phase::Ended) {
            return None;
        }
        self.due.first().map(|&(at, _)| at)
    }

    /// The next request to send, each once; `None` while every request is in flight or the
    /// PUT is over.
    pub fn poll(&mut self) -> Option<(Id, Request)> {
        if self.outcome.is_some() {
            return None;
        }
        self.ready.pop_front()
    }

    /// Takes the answer to request `id`, and moves on.
    pub fn answer(&mut self, id: Id, answer: Answer) -> Result<(), PutError> {
        self.guard(|put| put.answered(id, answer))
    }

    /// How the PUT ended, once it has.
    pub fn outcome(&self) -> Option<&Result<Stored, PutError>> {
        self.outcome.as_ref()
    }

    /// Runs a step; one that fails ends the PUT with its error, and what the PUT wrote is left
    /// to the sweeps (docs/design/gateway.md §2).
    fn guard<T>(
        &mut self,
        step: impl FnOnce(&mut Self) -> Result<T, PutError>,
    ) -> Result<T, PutError> {
        if self.outcome.is_some() {
            return Err(PutError::Over);
        }
        let result = step(self);
        if let Err(e) = &result {
            self.outcome = Some(Err(e.clone()));
            self.ready.clear();
            self.asked.clear();
        }
        result
    }

    fn take(&mut self, mut bytes: &[u8]) -> Result<usize, PutError> {
        if self.phase != Phase::Body {
            return Err(PutError::Over);
        }
        let mut took = 0usize;
        while !bytes.is_empty() {
            if self.taken == self.body.length {
                return Err(PutError::TooLong);
            }
            if self.waiting.is_some() {
                break;
            }
            let left = self
                .body
                .length
                .checked_sub(self.taken)
                .ok_or(PutError::Overflow)?;
            let room = seal::SEGMENT
                .checked_sub(self.segment.len())
                .ok_or(PutError::Overflow)?;
            let n = bytes
                .len()
                .min(room)
                .min(usize::try_from(left).unwrap_or(usize::MAX));
            let (now, rest) = bytes.split_at_checked(n).ok_or(PutError::Overflow)?;
            self.segment.extend_from_slice(now);
            for h in [self.md5.as_mut(), self.sum.as_mut()].into_iter().flatten() {
                h.update(now)?;
            }
            let n64 = u64::try_from(n).map_err(|_| PutError::Overflow)?;
            self.taken = self.taken.checked_add(n64).ok_or(PutError::Overflow)?;
            took = took.checked_add(n).ok_or(PutError::Overflow)?;
            bytes = rest;
            if self.segment.len() == seal::SEGMENT || self.taken == self.body.length {
                self.seal_segment()?;
            }
        }
        Ok(took)
    }

    /// Seals the segment filled into its block, and sends the block down once it holds its
    /// segments.
    fn seal_segment(&mut self) -> Result<(), PutError> {
        let segments = seal::segments(self.body.length);
        let next = self.sealed.checked_add(1).ok_or(PutError::Overflow)?;
        if self.filling.is_empty() {
            let len = self
                .layout
                .block_len(self.body.length, self.filling_index)?;
            self.filling
                .reserve_exact(usize::try_from(len).map_err(|_| PutError::Overflow)?);
        }
        self.sealer
            .seal(self.sealed, next == segments, &mut self.segment)?;
        if let Some(h) = self.sealed_md5.as_mut() {
            h.update(&self.segment)?;
        }
        self.filling.extend_from_slice(&self.segment);
        self.segment.clear();
        self.sealed = next;
        let holds = self
            .layout
            .block_segments(self.body.length, self.filling_index)?;
        if self.sealed == holds.end {
            let full = std::mem::take(&mut self.filling);
            if self.going.is_some() {
                self.waiting = Some(full);
            } else {
                self.go(full)?;
            }
        }
        Ok(())
    }

    fn finish(&mut self, expected: &Expected) -> Result<(), PutError> {
        if self.phase != Phase::Body {
            return Err(PutError::Over);
        }
        if self.taken != self.body.length {
            return Err(PutError::IncompleteBody);
        }
        let plain = self.md5.take().map(md5_of).transpose()?;
        if let Some(sent) = expected.md5
            && plain.ok_or(PutError::UndeclaredDigest)? != sent
        {
            return Err(PutError::BadDigest);
        }
        let checked = self.sum.take().map(Hasher::finish).transpose()?;
        if let Some(sent) = &expected.checksum
            && checked.as_ref() != Some(sent)
        {
            return Err(PutError::BadChecksum);
        }
        self.checked = checked;
        self.phase = Phase::Ended;
        // An empty part is one empty segment, sealed now, before its ETag, which may be the
        // MD5 of what is sealed.
        let empty_part = self.body.length == 0 && matches!(self.commit, Commit::Part(_));
        if empty_part {
            self.seal_segment()?;
        }
        let etag = match self.sealed_md5.take() {
            Some(sealed) => md5_of(sealed)?,
            None => plain.ok_or(PutError::Over)?,
        };
        self.etag = Some(checksum::etag(&etag));
        if self.body.length == 0 && !empty_part {
            return self.commit(None);
        }
        self.file_if_written()
    }

    /// Codes a full block and asks where its chunks go.
    fn go(&mut self, bytes: Vec<u8>) -> Result<(), PutError> {
        let index = self.filling_index;
        self.filling_index = index.checked_add(1).ok_or(PutError::Overflow)?;
        let id = self.ids.block()?;
        let len = u64::try_from(bytes.len()).map_err(|_| PutError::Overflow)?;
        let crc32c = mantle_crc::crc32c(&bytes);
        let chunk_len = self.layout.chunk_len(len)?;
        let (data, chunks): (usize, Vec<(Bytes, u32)>) = match self.layout.scheme() {
            // Each copy is the block itself, and its checksum the block's.
            Scheme::Copies(n) => {
                let one = Bytes::from(bytes);
                (1, (0..n).map(|_| (one.clone(), crc32c)).collect())
            }
            // The data chunks are slices of the block, which they share; only the parity, and
            // a last data chunk the block does not fill, which is padded, are new bytes (audit
            // P08).
            Scheme::Rs(code) => {
                let parity = code.parity_of(&bytes)?;
                let block = Bytes::from(bytes);
                let c = code.chunk_len(block.len())?;
                let mut chunks = Vec::with_capacity(code.width());
                for i in 0..code.data() {
                    let span = code.data_span(block.len(), i)?;
                    let chunk = if span.len() == c {
                        block.slice(span)
                    } else {
                        let mut padded = Vec::with_capacity(c);
                        padded.extend_from_slice(block.get(span).unwrap_or_default());
                        padded.resize(c, 0);
                        Bytes::from(padded)
                    };
                    chunks.push(chunk);
                }
                chunks.extend(parity.into_iter().map(Bytes::from));
                let chunks = chunks
                    .into_iter()
                    .map(|chunk| {
                        let crc = mantle_crc::crc32c(&chunk);
                        (chunk, crc)
                    })
                    .collect();
                (code.data(), chunks)
            }
        };
        let width = chunks.len();
        let parity = width.checked_sub(data).ok_or(PutError::Overflow)?;
        let header = BlockHeader {
            length: len,
            data: u8::try_from(data).map_err(|_| PutError::Overflow)?,
            parity: u8::try_from(parity).map_err(|_| PutError::Overflow)?,
            chunk_len,
            crc32c,
        };
        self.going = Some(Going {
            index,
            id,
            len,
            header,
            chunks,
            volumes: Vec::new(),
            next: 0,
            at: vec![None; width],
        });
        self.ask(
            Asked::Place,
            Request::Place {
                block: id,
                width,
                chunk_len,
            },
        );
        Ok(())
    }

    fn ask(&mut self, asked: Asked, request: Request) {
        let id = self.next_id;
        self.next_id = id.wrapping_add(1);
        self.asked.insert(id, asked);
        self.ready.push_back((id, request));
    }

    fn answered(&mut self, id: Id, answer: Answer) -> Result<(), PutError> {
        let asked = self.asked.remove(&id).ok_or(PutError::Unknown(id))?;
        match (asked, answer) {
            (Asked::Place, Answer::Volumes(volumes)) => self.placed(volumes),
            (Asked::Chunk(i), Answer::Stored) => self.stored(i),
            (Asked::Chunk(i), Answer::Refused) => self.refused(i),
            (Asked::Block { asked_ns }, Answer::Block(block::Outcome::Written { deadline_ns })) => {
                self.recorded_block(deadline_ns, asked_ns)
            }
            (Asked::Block { .. }, Answer::Block(outcome)) => Err(PutError::Block(outcome)),
            (
                Asked::Renew { index, asked_ns },
                Answer::Block(block::Outcome::Written { deadline_ns }),
            ) => {
                let r = self.recorded.get_mut(&index).ok_or(PutError::Mismatch)?;
                r.deadline_ns = deadline_ns;
                r.asked_ns = asked_ns;
                r.renewing = false;
                let next = self.next_renewal(asked_ns)?;
                self.due.insert((next, index));
                self.file_when_renewed()
            }
            (
                Asked::Renew { index, .. },
                Answer::Block(block::Outcome::Expired | block::Outcome::NoSuchBlock),
            ) => {
                let r = self.recorded.get(&index).ok_or(PutError::Mismatch)?;
                Err(PutError::Released(r.id))
            }
            (Asked::Renew { .. }, Answer::Block(outcome)) => Err(PutError::Block(outcome)),
            (Asked::File, Answer::File(file::Outcome::Written { deadline_ns })) => {
                self.commit(Some(deadline_ns))
            }
            (Asked::File, Answer::File(outcome)) => Err(PutError::File(outcome)),
            (Asked::Name, Answer::Name(outcome)) => self.done(outcome),
            _ => Err(PutError::Mismatch),
        }
    }

    fn placed(&mut self, offered: Vec<u128>) -> Result<(), PutError> {
        let going = self.going.as_mut().ok_or(PutError::Mismatch)?;
        // Each volume once, in the order offered.
        let mut seen = BTreeSet::new();
        let volumes: Vec<u128> = offered.into_iter().filter(|v| seen.insert(*v)).collect();
        let width = going.chunks.len();
        if volumes.len() < width {
            return Err(PutError::Unplaced(going.id));
        }
        let mut sends = Vec::with_capacity(width);
        for (i, ((bytes, crc), &volume)) in going.chunks.iter().zip(&volumes).enumerate() {
            sends.push((i, chunk(going.id, i, volume, bytes, *crc)?));
        }
        for (slot, &volume) in going.at.iter_mut().zip(&volumes) {
            *slot = Some((volume, false));
        }
        going.next = width;
        going.volumes = volumes;
        for (i, request) in sends {
            self.ask(Asked::Chunk(i), request);
        }
        Ok(())
    }

    fn stored(&mut self, i: usize) -> Result<(), PutError> {
        let going = self.going.as_mut().ok_or(PutError::Mismatch)?;
        let slot = going.at.get_mut(i).ok_or(PutError::Mismatch)?;
        let (volume, _) = slot.ok_or(PutError::Mismatch)?;
        *slot = Some((volume, true));
        if !going.at.iter().all(|a| matches!(a, Some((_, true)))) {
            return Ok(());
        }
        let chunks = going
            .at
            .iter()
            .enumerate()
            .map(|(index, a)| {
                let (volume, _) = a.ok_or(PutError::Mismatch)?;
                Ok(ChunkPlace {
                    volume,
                    key: chunk_key(going.id, index)?,
                })
            })
            .collect::<Result<Vec<_>, PutError>>()?;
        let command = block::Command::Write {
            block: going.id,
            header: going.header,
            chunks,
            file: self.file,
            handover_ns: self.handover_ns,
            // The entry that carries a command gives it its time.
            at_ns: 0,
        };
        let asked_ns = self.now_ns;
        self.ask(Asked::Block { asked_ns }, Request::Block(command));
        Ok(())
    }

    /// Sends a refused chunk to the next volume the placement offered, which holds none of the
    /// block's chunks.
    fn refused(&mut self, i: usize) -> Result<(), PutError> {
        let going = self.going.as_mut().ok_or(PutError::Mismatch)?;
        let Some(&volume) = going.volumes.get(going.next) else {
            return Err(PutError::Unplaced(going.id));
        };
        going.next = going.next.checked_add(1).ok_or(PutError::Overflow)?;
        let (bytes, crc) = going.chunks.get(i).ok_or(PutError::Mismatch)?;
        let request = chunk(going.id, i, volume, bytes, *crc)?;
        *going.at.get_mut(i).ok_or(PutError::Mismatch)? = Some((volume, false));
        self.ask(Asked::Chunk(i), request);
        Ok(())
    }

    fn recorded_block(&mut self, deadline_ns: u64, asked_ns: u64) -> Result<(), PutError> {
        let going = self.going.take().ok_or(PutError::Mismatch)?;
        self.recorded.insert(
            going.index,
            Recorded {
                id: going.id,
                len: going.len,
                deadline_ns,
                asked_ns,
                renewing: false,
            },
        );
        let next = self.next_renewal(asked_ns)?;
        self.due.insert((next, going.index));
        if let Some(full) = self.waiting.take() {
            self.go(full)?;
        }
        self.file_if_written()
    }

    /// When a block whose write or renewal was asked at `asked_ns` is due for renewal: a
    /// quarter of the handover later. A renewal due past the end of the clock never comes.
    fn next_renewal(&self, asked_ns: u64) -> Result<u64, PutError> {
        let quarter = self
            .handover_ns
            .checked_div(RENEWALS)
            .ok_or(PutError::Overflow)?;
        Ok(asked_ns.saturating_add(quarter))
    }

    /// Asks to renew each recorded block whose renewal has come due, earliest first. Only
    /// the due blocks are touched, and at most every recorded block is due at once, which
    /// admission bounds (audit B08).
    fn renew_due(&mut self) -> Result<(), PutError> {
        let now = self.now_ns;
        while let Some(&(at, index)) = self.due.first() {
            if at > now {
                break;
            }
            self.due.pop_first();
            let r = self.recorded.get_mut(&index).ok_or(PutError::Mismatch)?;
            r.renewing = true;
            let renew = block::Command::Renew {
                block: r.id,
                file: self.file,
                handover_ns: self.handover_ns,
                at_ns: 0,
            };
            self.ask(
                Asked::Renew {
                    index,
                    asked_ns: now,
                },
                Request::Block(renew),
            );
        }
        Ok(())
    }

    /// Once the body has ended and every block is recorded, renews the blocks due one last
    /// time, so the file is asked for with each block renewed as recently as at any time
    /// before. The renewals are not repeated while they answer: one that takes longer than a
    /// quarter of the handover would find the next due already.
    fn file_if_written(&mut self) -> Result<(), PutError> {
        let blocks = self.layout.blocks(self.body.length);
        let all = u64::try_from(self.recorded.len()).map_err(|_| PutError::Overflow)?;
        if self.phase != Phase::Ended || all != blocks {
            return Ok(());
        }
        self.phase = Phase::Closing;
        self.renew_due()?;
        self.file_when_renewed()
    }

    /// Writes the file once the last renewals have answered.
    fn file_when_renewed(&mut self) -> Result<(), PutError> {
        if self.phase != Phase::Closing || self.recorded.values().any(|r| r.renewing) {
            return Ok(());
        }
        let extents = self
            .recorded
            .values()
            .map(|r| Extent {
                length: r.len,
                target: Target::Block(r.id),
            })
            .collect();
        let blocks_deadline_ns = self
            .recorded
            .values()
            .map(|r| r.deadline_ns)
            .min()
            .ok_or(PutError::Overflow)?;
        let referrer = match &self.commit {
            Commit::Object(p) => Referrer {
                bucket: p.bucket.clone(),
                incarnation: p.incarnation,
                key: p.key.clone(),
            },
            Commit::Part(p) => Referrer {
                bucket: p.bucket.clone(),
                incarnation: p.incarnation,
                key: p.key.clone(),
            },
        };
        let command = file::Command::Write {
            file: self.file,
            extents,
            referrer,
            key: Some(self.wrapped),
            handover_ns: self.handover_ns,
            blocks_deadline_ns,
            at_ns: 0,
        };
        self.phase = Phase::File;
        self.ask(Asked::File, Request::File(command));
        Ok(())
    }

    /// Commits the version or part, with the file's handover deadline, or with no file for an
    /// empty object.
    fn commit(&mut self, deadline_ns: Option<u64>) -> Result<(), PutError> {
        let etag = self.etag.clone().ok_or(PutError::Over)?;
        let command = match (&self.commit, deadline_ns) {
            (Commit::Object(p), deadline_ns) => {
                let mut p = p.clone();
                p.version.file = deadline_ns.map(|_| self.file);
                p.version.size = self.body.length;
                p.version.etag = etag;
                p.version.checksum = self.checked.as_ref().map(|c| record::Checksum {
                    algorithm: c.algorithm.code(),
                    parts: 0,
                    value: c.bytes.clone(),
                });
                p.deadline_ns = deadline_ns.unwrap_or(p.deadline_ns);
                name::Command::Put(p)
            }
            (Commit::Part(p), Some(deadline_ns)) => {
                let mut p = p.clone();
                p.part.file = self.file;
                p.part.size = self.body.length;
                p.part.etag = etag;
                p.part.checksum = self.checked.as_ref().map(|c| c.bytes.clone());
                p.deadline_ns = deadline_ns;
                name::Command::PutPart(p)
            }
            // A part always has a file.
            (Commit::Part(_), None) => return Err(PutError::Mismatch),
        };
        self.phase = Phase::Name;
        self.ask(Asked::Name, Request::Name(Box::new(command)));
        Ok(())
    }

    fn done(&mut self, outcome: name::Outcome) -> Result<(), PutError> {
        let version = match (&self.commit, outcome) {
            (Commit::Object(_), name::Outcome::Put { version }) => Some(version),
            (Commit::Part(_), name::Outcome::PartWritten) => None,
            (_, outcome) => return Err(PutError::Name(outcome)),
        };
        self.outcome = Some(Ok(Stored {
            version,
            etag: self.etag.clone().ok_or(PutError::Over)?,
            size: self.body.length,
            checksum: self.checked.clone(),
        }));
        Ok(())
    }
}

/// The key of chunk `index` of block `block`, in the block's first layout.
fn chunk_key(block: u128, index: usize) -> Result<ChunkKey, PutError> {
    Ok(ChunkKey {
        block,
        epoch: 1,
        index: u16::try_from(index).map_err(|_| PutError::Overflow)?,
    })
}

fn chunk(
    block: u128,
    index: usize,
    volume: u128,
    bytes: &Bytes,
    crc: u32,
) -> Result<Request, PutError> {
    Ok(Request::Chunk {
        volume,
        key: chunk_key(block, index)?,
        bytes: bytes.clone(),
        crc,
    })
}
