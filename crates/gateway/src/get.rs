//! Reading an object or a range of it (docs/design/gateway.md §3): the extents holding the
//! wanted bytes found a page at a time from the one holding the first, each block's row read
//! for its chunks' places, only the chunk bytes holding the wanted segments read, a chunk that
//! fails replaced by another copy or by decoding its block from any `data` of its chunks, one
//! refused for want of room asked of its place again once room frees, and each segment opened
//! under its file's key.
//!
//! A [`Get`] names each request and moves on with its answer, doing no I/O, as a PUT does. It
//! starts from the version the Name range read, whose preconditions, lock and range the caller
//! has judged, and gives out the range's plaintext in order. It holds at most as many blocks,
//! being read or waiting to be taken, as its caller admits: with two, one is read while the one
//! before it waits for the caller; with more, the reads of several blocks overlap, whose round
//! trips otherwise add up (audit §16.3, §16.7).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroU64;
use std::ops::Range;

use bytes::Bytes;
use mantle_chunk::ChunkKey;
use mantle_ec::durability::Scheme;
use mantle_ec::{Code, EcError};
use mantle_meta::record::{BlockHeader, ChunkPlace, Extent, FileHeader, Target, WrappedKey};
use mantle_s3::seal::{self, DataKey, SealError, Segments};

use crate::layout::{Layout, SEALED};

/// A request's name among those in flight.
pub type Id = u64;

/// A GET's request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// A file's header, from the File range that holds it.
    Header { file: u128 },
    /// The extents of `file` from the one holding byte `offset`, in order, at most `max`, each
    /// with the offset of its first byte: stored bytes of a file of blocks, plaintext of a file
    /// of parts.
    Extents { file: u128, offset: u64, max: usize },
    /// A block's header and its chunks' places, from the Block range that holds it.
    Block { block: u128 },
    /// Bytes `[offset, offset + len)` of a chunk, from its volume, which verifies them.
    Chunk {
        volume: u128,
        key: ChunkKey,
        offset: u64,
        len: u64,
    },
}

/// What a request answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Header(Option<FileHeader>),
    /// The extents asked for, each with the offset of its first byte; fewer past the end.
    Extents(Vec<(u64, Extent)>),
    Block(Option<(BlockHeader, Vec<ChunkPlace>)>),
    Chunk(Bytes),
    /// This place cannot give the bytes: its volume failed, is fenced or closing, or does not
    /// hold them whole and verified, or its peer is gone. Another place that holds them is read.
    Unreadable,
    /// No room for the read now, at the volume's reads or in the transport toward it: the place
    /// still holds the bytes, and is asked again once room frees ([`Get::room`], research/77 D2).
    Busy,
    /// No volume serves the read as it was asked: the node may not send it, or sent it so that
    /// every place refuses it alike. The GET cannot go on.
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GetError {
    /// A row the object names is gone, or disagrees with the rows that name it: its metadata
    /// is damaged (`500 InternalError`).
    #[error("the object's rows disagree: {0}")]
    Inconsistent(&'static str),
    /// Fewer of a block's chunks could be read than rebuild it (`503 SlowDown`, retried).
    #[error("block {0:032x}: too few of its chunks could be read")]
    Unreadable(u128),
    /// A chunk read of the block no volume serves as it was asked ([`Answer::Failed`]): reading
    /// another place would be refused alike, so the GET ends at once (`500 InternalError`).
    #[error("no volume serves a chunk read of block {0:032x} as it was asked")]
    Unsendable(u128),
    /// Its client's patience passed while a chunk read waited for room
    /// ([`Get::out_of_patience`]): `503 SlowDown` for patience, which clients ask again.
    #[error("no room for its chunk reads came within its client's patience")]
    Patience,
    /// A block rebuilt from its chunks does not match its CRC-32C, or a segment its tag.
    #[error("block {0:032x} did not verify")]
    Corrupt(u128),
    #[error("a range outside the object")]
    Range,
    /// A GET admitted no block at once.
    #[error("a GET needs room for one block")]
    Window,
    #[error("an answer of another kind than its request")]
    Mismatch,
    #[error("an answer to no request in flight: {0}")]
    Unknown(Id),
    #[error("the GET is over")]
    Over,
    #[error("a length past what the GET can address")]
    Overflow,
    /// The file's data key could not be unwrapped.
    #[error("the file's key could not be unwrapped")]
    Key,
    #[error(transparent)]
    Seal(#[from] SealError),
    #[error(transparent)]
    Code(#[from] EcError),
}

/// Unwraps a file's data key: under the node's root key of the generation that wrapped it, or
/// under the customer's key the request carries (docs/design/encryption.md §2).
pub trait Keyring {
    fn unwrap(&self, key: &WrappedKey) -> Result<DataKey, GetError>;
}

/// A file of blocks being read: plaintext `[from, to)` of it, sealed in segments under its key.
struct Plain {
    file: u128,
    segments: Segments,
    /// Its stored bytes, and the segments its plaintext is sealed in.
    stored: u64,
    count: u64,
    from: u64,
    to: u64,
    /// The next segment no block begun holds, and the one past the range's last.
    next: u64,
    end: u64,
    /// The first segment of the next block to give out.
    emit: u64,
    /// Extents a page gave, not yet begun.
    extents: VecDeque<(u64, Extent)>,
}

/// A part of a chunk read for a block, and what it gave.
struct Span {
    /// Where in the block's places it is read from now, and the bytes of the chunk read: a
    /// copy's, any of the block's places, a code's data chunk's, its own place or one kept
    /// beside it (docs/design/repair.md §3).
    chunk: usize,
    range: Range<u64>,
    got: Option<Bytes>,
    /// The places tried for this span.
    tried: BTreeSet<usize>,
}

/// A block being read.
struct Reading {
    block: u128,
    /// Its stored bytes' offsets in its file.
    stored: Range<u64>,
    /// The file's segments it holds that the range wants.
    segments: Range<u64>,
    /// Their stored bytes, as offsets in the block.
    want: Range<u64>,
    row: Option<(BlockHeader, Vec<ChunkPlace>)>,
    spans: Vec<Span>,
    /// Once a span of a coded block fails: the whole chunks read to decode it, held apart so a
    /// block read whole by its spans, the common case, holds none of it.
    whole: Option<Box<Whole>>,
    /// Its requests in flight: it finishes only once none is, so no answer outlives it.
    inflight: usize,
    /// When, in the caller's time, its row was asked; none where the caller told no time.
    begun_ns: Option<u64>,
}

/// The whole chunks of a coded block read to decode it, by index: those read, those asked, and
/// those none of whose places read; and every place tried, whose chunk is asked of another of its
/// places next.
#[derive(Default)]
struct Whole {
    got: BTreeMap<usize, Bytes>,
    asked: BTreeSet<usize>,
    failed: BTreeSet<usize>,
    tried: BTreeSet<usize>,
}

/// What a request in flight asked. Blocks are named by the first segment they hold.
#[derive(Debug, Clone, Copy)]
enum Asked {
    /// The header of the object's file.
    Top,
    /// The header of the part holding the object's plaintext from `start`, `len` bytes long.
    Part {
        file: u128,
        start: u64,
        len: u64,
    },
    /// The extent of the object's file of parts holding plaintext byte `at`.
    PartExtent,
    /// A page of the extents of the file being read.
    Extents,
    Block(u64),
    Span(u64, usize),
    /// A whole chunk of a block, read from the place at this position of its places.
    Whole(u64, usize),
}

/// The most chunk gets a get holds out at once with `window` blocks being read under `scheme`:
/// for a coded block, its data chunks' spans at once and, a span failing, whole chunks for as
/// many as the block still needs while the other spans are out, twice its data chunks less one,
/// more than its width where its parity is less than its data less one; for copies, one copy at
/// a time.
pub fn most_chunks(window: usize, scheme: Scheme) -> Option<usize> {
    let per_block = match scheme {
        Scheme::Copies(_) => 1,
        Scheme::Rs(code) => code.data().checked_mul(2)?.checked_sub(1)?,
    };
    window.checked_mul(per_block)
}

/// The most bytes a get holds with a window of `window` blocks under `layout`, its blocks the
/// largest. A block being read holds the spans it read, the plaintext's, and, once a span
/// failed, the whole chunks read in their place: a code's data chunks, a copy's one other copy.
/// The block finishing holds them through its decoding, a code's restoring the block from its
/// chunks (`Code::decode_bytes`) and a copy's copying the copy, then the stored bytes it wants
/// cut from the block, and the plaintext it opens into from them a segment at a time, beside
/// the others the window holds.
pub fn most_bytes(window: usize, layout: &Layout) -> Option<u64> {
    let block = layout.largest_block()?;
    let others = u64::try_from(window.checked_sub(1)?).ok()?;
    let (whole, decoding) = match layout.scheme() {
        Scheme::Copies(_) => (block, block.checked_mul(2)?),
        Scheme::Rs(code) => {
            let len = usize::try_from(block).ok()?;
            let chunk = u64::try_from(code.chunk_len(len).ok()?).ok()?;
            (
                chunk.checked_mul(u64::try_from(code.data()).ok()?)?,
                code.decode_bytes(len)?.max(block.checked_mul(2)?),
            )
        }
    };
    let read = block.checked_add(whole)?;
    let decoded = read.checked_add(decoding)?;
    let opened = read
        .checked_add(block.checked_mul(2)?)?
        .checked_add(SEALED)?;
    read.checked_mul(others)?.checked_add(decoded.max(opened))
}

pub struct Get {
    file: Option<u128>,
    size: u64,
    keyring: Box<dyn Keyring + Send>,
    /// Blocks held at most, being read or waiting to be taken.
    window: usize,
    /// The object's plaintext wanted, and the first byte of it not yet handed to a file.
    to: u64,
    at: u64,
    /// The object's file of parts, once its header says it is one.
    parts: Option<u128>,
    plain: Option<Plain>,
    /// A header or extent request in flight, which what comes next waits on.
    meta: bool,
    reading: BTreeMap<u64, Reading>,
    /// Blocks read, waiting their turn to be given out, by first segment: the segment past
    /// them, and their plaintext.
    read: BTreeMap<u64, (u64, Bytes)>,
    /// Plaintext ready for the caller, a block's at a time.
    out: VecDeque<Bytes>,
    ready: VecDeque<(Id, Request)>,
    asked: BTreeMap<Id, Asked>,
    /// The chunk reads answered [`Answer::Busy`] whose bytes their block still wants, each waiting
    /// for room at the place it asked: at most the chunk reads the window holds out at once
    /// ([`most_chunks`]), as each was one of them.
    roomless: Vec<Asked>,
    next_id: Id,
    outcome: Option<Result<(), GetError>>,
    /// The caller's time, as it last told it ([`Get::tick`]); none before it first does.
    now_ns: Option<u64>,
    /// How long, in the caller's time, the last block read took from its row asked to its
    /// plaintext opened, until the caller takes it ([`Get::take_read`]).
    read_ns: Option<u64>,
}

impl Get {
    /// A GET of plaintext `range` of an object of `size` bytes held in `file`, none for an
    /// empty object, whose keys `keyring` unwraps, holding at most `window` blocks at once.
    pub fn new(
        file: Option<u128>,
        size: u64,
        range: Range<u64>,
        keyring: Box<dyn Keyring + Send>,
        window: usize,
    ) -> Result<Self, GetError> {
        if range.start > range.end || range.end > size {
            return Err(GetError::Range);
        }
        if window == 0 {
            return Err(GetError::Window);
        }
        let mut get = Self {
            file,
            size,
            keyring,
            window,
            to: range.end,
            at: range.start,
            parts: None,
            plain: None,
            meta: false,
            reading: BTreeMap::new(),
            read: BTreeMap::new(),
            out: VecDeque::new(),
            ready: VecDeque::new(),
            asked: BTreeMap::new(),
            roomless: Vec::new(),
            next_id: 0,
            outcome: None,
            now_ns: None,
            read_ns: None,
        };
        get.guard(Self::advance)?;
        Ok(get)
    }

    /// The caller's time moved to `now_ns`: blocks begun and read from here are timed by it.
    pub fn tick(&mut self, now_ns: u64) {
        self.now_ns = Some(self.now_ns.map_or(now_ns, |was| was.max(now_ns)));
    }

    /// How long the last block read took, in the caller's time, from its row asked to its
    /// plaintext opened; none where none was read since the caller last took it, or where the
    /// caller told no time. The caller sizes the blocks a get holds being read by it.
    pub fn take_read(&mut self) -> Option<u64> {
        self.read_ns.take()
    }

    /// The next request to send, each once; `None` while every request is in flight or the
    /// GET is over.
    pub fn poll(&mut self) -> Option<(Id, Request)> {
        if matches!(self.outcome, Some(Err(_))) {
            return None;
        }
        self.ready.pop_front()
    }

    /// Takes the answer to request `id`, and moves on.
    pub fn answer(&mut self, id: Id, answer: Answer) -> Result<(), GetError> {
        self.guard(|get| {
            let asked = get.asked.remove(&id).ok_or(GetError::Unknown(id))?;
            get.answered(asked, answer)?;
            get.advance()
        })
    }

    /// Room may have freed where its chunk reads were refused for want of it: each is asked
    /// again of the place it asked, which still holds its bytes (research/77 D2). The caller's
    /// patience ends a GET whose room never comes ([`Get::out_of_patience`]).
    pub fn room(&mut self) -> Result<(), GetError> {
        self.guard(|get| {
            for asked in std::mem::take(&mut get.roomless) {
                match asked {
                    Asked::Span(key, i) => get.ask_span(key, i)?,
                    Asked::Whole(key, at) => get.ask_whole(key, at)?,
                    Asked::Top
                    | Asked::Part { .. }
                    | Asked::PartExtent
                    | Asked::Extents
                    | Asked::Block(_) => return Err(GetError::Mismatch),
                }
            }
            Ok(())
        })
    }

    /// Whether a chunk read of it waits for room ([`Get::room`]).
    pub fn waits_for_room(&self) -> bool {
        !self.roomless.is_empty()
    }

    /// Its client's patience passed while a chunk read of it waited for room, which no transport
    /// ends: the GET ends [`GetError::Patience`] and lets go of what it holds.
    pub fn out_of_patience(&mut self) {
        if self.outcome.is_none() {
            self.fail(GetError::Patience);
        }
    }

    /// The next plaintext of the range, in order; `None` while none is ready.
    /// Whether plaintext waits to be taken: a block read and opened, the range's next.
    pub fn ready(&self) -> bool {
        !self.out.is_empty()
    }

    pub fn take(&mut self) -> Option<Bytes> {
        let bytes = self.out.pop_front()?;
        // The room it held may let another block be read.
        if let Err(e) = self.advance() {
            self.fail(e);
        }
        Some(bytes)
    }

    /// How the GET ended, once it has: `Ok` once every byte of the range was given out.
    pub fn outcome(&self) -> Option<&Result<(), GetError>> {
        match &self.outcome {
            Some(Ok(())) if !self.out.is_empty() => None,
            outcome => outcome.as_ref(),
        }
    }

    fn guard<T>(
        &mut self,
        step: impl FnOnce(&mut Self) -> Result<T, GetError>,
    ) -> Result<T, GetError> {
        if self.outcome.is_some() {
            return Err(GetError::Over);
        }
        let result = step(self);
        if let Err(e) = &result {
            self.fail(e.clone());
        }
        result
    }

    fn fail(&mut self, e: GetError) {
        self.outcome = Some(Err(e));
        self.ready.clear();
        self.asked.clear();
        self.roomless.clear();
        self.out.clear();
        self.read.clear();
        self.reading.clear();
    }

    fn ask(&mut self, request: Request, asked: Asked) -> Result<(), GetError> {
        let id = self.next_id;
        self.next_id = id.checked_add(1).ok_or(GetError::Overflow)?;
        match asked {
            Asked::Block(key) | Asked::Span(key, _) | Asked::Whole(key, _) => {
                let reading = self.reading.get_mut(&key).ok_or(GetError::Mismatch)?;
                reading.inflight = reading.inflight.checked_add(1).ok_or(GetError::Overflow)?;
            }
            Asked::Top | Asked::Part { .. } | Asked::PartExtent | Asked::Extents => {
                self.meta = true;
            }
        }
        self.asked.insert(id, asked);
        self.ready.push_back((id, request));
        Ok(())
    }

    /// Blocks held: being read, read and waiting their turn, or ready for the caller.
    fn held(&self) -> usize {
        self.reading
            .len()
            .saturating_add(self.read.len())
            .saturating_add(self.out.len())
    }

    /// Asks for what comes next: blocks begun from the extents in hand while the window has
    /// room, a page of extents when none is in hand, and the next part or the end once a file
    /// is read.
    fn advance(&mut self) -> Result<(), GetError> {
        if self.outcome.is_some() || self.meta {
            return Ok(());
        }
        if self.plain.is_some() {
            loop {
                let held = self.held();
                let Some(plain) = self.plain.as_mut() else {
                    return Err(GetError::Mismatch);
                };
                if plain.next >= plain.end || held >= self.window {
                    break;
                }
                let Some((start, extent)) = plain.extents.pop_front() else {
                    break;
                };
                self.begin(start, extent)?;
            }
            let room = self.window.saturating_sub(self.held());
            let plain = self.plain.as_mut().ok_or(GetError::Mismatch)?;
            if plain.next < plain.end && plain.extents.is_empty() && room > 0 {
                let offset = plain.next.checked_mul(SEALED).ok_or(GetError::Overflow)?;
                let file = plain.file;
                // No more than the blocks the window has room for, nor than the range holds.
                let segments =
                    usize::try_from(plain.end.saturating_sub(plain.next)).unwrap_or(usize::MAX);
                let max = room.min(segments);
                return self.ask(Request::Extents { file, offset, max }, Asked::Extents);
            }
            if plain.next < plain.end || !self.reading.is_empty() || !self.read.is_empty() {
                return Ok(());
            }
            self.plain = None;
        }
        if self.at == self.to {
            self.outcome = Some(Ok(()));
            return Ok(());
        }
        match (self.file, self.parts) {
            (None, _) => Err(GetError::Range),
            (Some(file), None) => self.ask(Request::Header { file }, Asked::Top),
            (Some(_), Some(parts)) => self.ask(
                Request::Extents {
                    file: parts,
                    offset: self.at,
                    max: 1,
                },
                Asked::PartExtent,
            ),
        }
    }

    fn answered(&mut self, asked: Asked, answer: Answer) -> Result<(), GetError> {
        if let Asked::Block(key) | Asked::Span(key, _) | Asked::Whole(key, _) = asked {
            let reading = self.reading.get_mut(&key).ok_or(GetError::Mismatch)?;
            reading.inflight = reading.inflight.checked_sub(1).ok_or(GetError::Mismatch)?;
        } else {
            self.meta = false;
        }
        match (asked, answer) {
            (Asked::Top, Answer::Header(header)) => self.top(header),
            (Asked::Part { file, start, len }, Answer::Header(header)) => {
                self.part(file, start, len, header)
            }
            (Asked::PartExtent, Answer::Extents(extents)) => self.part_extent(extents),
            (Asked::Extents, Answer::Extents(extents)) => self.page(extents),
            (Asked::Block(key), Answer::Block(row)) => self.block_row(key, row),
            (Asked::Span(key, i), Answer::Chunk(bytes)) => self.span(key, i, Some(bytes)),
            (Asked::Span(key, i), Answer::Unreadable) => self.span(key, i, None),
            (Asked::Whole(key, c), Answer::Chunk(bytes)) => self.whole(key, c, Some(bytes)),
            (Asked::Whole(key, c), Answer::Unreadable) => self.whole(key, c, None),
            (Asked::Span(key, _) | Asked::Whole(key, _), Answer::Busy) => {
                self.wait_for_room(key, asked)
            }
            (Asked::Span(key, _) | Asked::Whole(key, _), Answer::Failed) => {
                let reading = self.reading.get(&key).ok_or(GetError::Mismatch)?;
                Err(GetError::Unsendable(reading.block))
            }
            _ => Err(GetError::Mismatch),
        }
    }

    /// A chunk read of block `key` refused for want of room: no copy is lost and no other place
    /// is read, as the place still holds its bytes; it waits to be asked of the same place again
    /// ([`Get::room`]), a whole chunk still counted as asked. A span of a block now decoded from
    /// whole chunks is wanted no more, and the block finishes if that read was the last it
    /// waited on.
    fn wait_for_room(&mut self, key: u64, asked: Asked) -> Result<(), GetError> {
        let reading = self.reading.get(&key).ok_or(GetError::Mismatch)?;
        if !(matches!(asked, Asked::Span(..)) && reading.whole.is_some()) {
            self.roomless.push(asked);
        }
        self.finish(key)
    }

    /// The object's file: of blocks, sealed under its own key, or of parts, each a file of
    /// blocks under its own.
    fn top(&mut self, header: Option<FileHeader>) -> Result<(), GetError> {
        let header = header.ok_or(GetError::Inconsistent("the version's file is gone"))?;
        let file = self.file.ok_or(GetError::Range)?;
        match header.key {
            Some(_) => {
                let (from, to) = (self.at, self.to);
                self.open(file, &header, self.size, from..to)?;
                self.at = to;
                Ok(())
            }
            // A file of parts is addressed by plaintext: its extents are its parts' plaintext
            // lengths (docs/design/gateway.md §1).
            None if header.length == self.size => {
                self.parts = Some(file);
                Ok(())
            }
            None => Err(GetError::Inconsistent(
                "a file of parts not the version's size",
            )),
        }
    }

    /// The part of a file of parts that holds the object's plaintext byte `at`.
    fn part_extent(&mut self, extents: Vec<(u64, Extent)>) -> Result<(), GetError> {
        let (start, extent) = extents.into_iter().next().ok_or(GetError::Inconsistent(
            "a file of parts ends before its size",
        ))?;
        let Target::File(file) = extent.target else {
            return Err(GetError::Inconsistent("a file of parts names a block"));
        };
        let end = start.checked_add(extent.length).ok_or(GetError::Overflow)?;
        if start > self.at || end <= self.at {
            return Err(GetError::Inconsistent(
                "an extent that does not hold its byte",
            ));
        }
        self.ask(
            Request::Header { file },
            Asked::Part {
                file,
                start,
                len: extent.length,
            },
        )
    }

    /// A part's header: its plaintext from the object's `at` to the range's end or the part's.
    fn part(
        &mut self,
        file: u128,
        start: u64,
        len: u64,
        header: Option<FileHeader>,
    ) -> Result<(), GetError> {
        let header = header.ok_or(GetError::Inconsistent("a part's file is gone"))?;
        if header.key.is_none() {
            return Err(GetError::Inconsistent("a part that is a file of parts"));
        }
        let end = start
            .checked_add(len)
            .ok_or(GetError::Overflow)?
            .min(self.to);
        let from = self.at.checked_sub(start).ok_or(GetError::Overflow)?;
        let to = end.checked_sub(start).ok_or(GetError::Overflow)?;
        self.open(file, &header, len, from..to)?;
        self.at = end;
        Ok(())
    }

    /// Begins reading plaintext `range` of `file`, a file of blocks of `plain` plaintext bytes.
    fn open(
        &mut self,
        file: u128,
        header: &FileHeader,
        plain: u64,
        range: Range<u64>,
    ) -> Result<(), GetError> {
        let key = header
            .key
            .as_ref()
            .ok_or(GetError::Inconsistent("a file of blocks without a key"))?;
        if seal::plain_len(header.length) != Some(plain) {
            return Err(GetError::Inconsistent(
                "a file's stored length does not seal its plaintext",
            ));
        }
        let data = self.keyring.unwrap(key)?;
        let segment = u64::try_from(seal::SEGMENT).map_err(|_| GetError::Overflow)?;
        let next = range.start.checked_div(segment).ok_or(GetError::Overflow)?;
        let end = crate::div_ceil(
            range.end,
            NonZeroU64::new(segment).ok_or(GetError::Overflow)?,
        );
        self.plain = Some(Plain {
            file,
            segments: Segments::new(&data, file)?,
            stored: header.length,
            count: seal::segments(plain),
            from: range.start,
            to: range.end,
            next,
            end,
            emit: next,
            extents: VecDeque::new(),
        });
        Ok(())
    }

    /// A page of extents, from the one holding the next wanted segment: each must follow the
    /// one before it, and those past the range are left.
    fn page(&mut self, extents: Vec<(u64, Extent)>) -> Result<(), GetError> {
        let plain = self.plain.as_mut().ok_or(GetError::Mismatch)?;
        if extents.is_empty() {
            return Err(GetError::Inconsistent("a file ends before its length"));
        }
        let mut at = plain.next.checked_mul(SEALED).ok_or(GetError::Overflow)?;
        let wanted_end = plain.end.checked_mul(SEALED).ok_or(GetError::Overflow)?;
        for (i, (start, extent)) in extents.into_iter().enumerate() {
            let end = start.checked_add(extent.length).ok_or(GetError::Overflow)?;
            // The first holds the next wanted byte; each after begins where the one before
            // ends.
            let placed = if i == 0 {
                start <= at && at < end
            } else {
                start == at
            };
            if !placed {
                return Err(GetError::Inconsistent(
                    "extents that do not follow each other",
                ));
            }
            if start >= wanted_end {
                break;
            }
            plain.extents.push_back((start, extent));
            at = end;
        }
        Ok(())
    }

    /// Begins reading the block of extent `(start, extent)`: the segments of it the range
    /// wants.
    fn begin(&mut self, start: u64, extent: Extent) -> Result<(), GetError> {
        let plain = self.plain.as_mut().ok_or(GetError::Mismatch)?;
        let Target::Block(block) = extent.target else {
            return Err(GetError::Inconsistent("a file of blocks names a file"));
        };
        let end = start.checked_add(extent.length).ok_or(GetError::Overflow)?;
        let at = plain.next.checked_mul(SEALED).ok_or(GetError::Overflow)?;
        // A block holds whole segments: it starts at one, and ends at one or at the file's end.
        let whole = |offset: u64| offset.checked_rem(SEALED) == Some(0);
        if start > at || end <= at || !whole(start) || !(whole(end) || end == plain.stored) {
            return Err(GetError::Inconsistent(
                "a block that does not hold whole segments",
            ));
        }
        let holds_end = crate::div_ceil(end, NonZeroU64::new(SEALED).ok_or(GetError::Overflow)?);
        let last = plain.end.min(holds_end);
        let segments = plain.next..last;
        let stored_end = last
            .checked_mul(SEALED)
            .ok_or(GetError::Overflow)?
            .min(plain.stored);
        let want = at.checked_sub(start).ok_or(GetError::Overflow)?
            ..stored_end.checked_sub(start).ok_or(GetError::Overflow)?;
        let key = plain.next;
        plain.next = last;
        self.reading.insert(
            key,
            Reading {
                block,
                stored: start..end,
                segments,
                want,
                row: None,
                spans: Vec::new(),
                whole: None,
                inflight: 0,
                begun_ns: self.now_ns,
            },
        );
        self.ask(Request::Block { block }, Asked::Block(key))
    }

    /// The block's row: reads the chunk bytes that hold the wanted range.
    fn block_row(
        &mut self,
        key: u64,
        row: Option<(BlockHeader, Vec<ChunkPlace>)>,
    ) -> Result<(), GetError> {
        let reading = self.reading.get_mut(&key).ok_or(GetError::Mismatch)?;
        let (header, places) = row.ok_or(GetError::Inconsistent("a file's block is gone"))?;
        let len = reading
            .stored
            .end
            .checked_sub(reading.stored.start)
            .ok_or(GetError::Overflow)?;
        let width = usize::from(header.data)
            .checked_add(usize::from(header.parity))
            .ok_or(GetError::Overflow)?;
        if header.length != len || places.len() < width || header.data == 0 {
            return Err(GetError::Inconsistent(
                "a block row that is not its extent's",
            ));
        }
        // Each place past the chunks' own keeps one of them, under that chunk's key.
        let kept_fit = places.iter().skip(width).all(|kept| {
            places
                .get(usize::from(kept.key.index))
                .is_some_and(|own| usize::from(kept.key.index) < width && own.key == kept.key)
        });
        if !kept_fit {
            return Err(GetError::Inconsistent(
                "a kept place of no chunk of its block",
            ));
        }
        let mut spans = Vec::new();
        if header.data == 1 {
            if header.chunk_len != len {
                return Err(GetError::Inconsistent("a copy not its block's length"));
            }
            spans.push(Span {
                chunk: 0,
                range: reading.want.clone(),
                got: None,
                tried: BTreeSet::from([0]),
            });
        } else {
            let code = Code::new(header.data.into(), header.parity.into())?;
            let block_len = usize::try_from(len).map_err(|_| GetError::Overflow)?;
            if u64::try_from(code.chunk_len(block_len)?).ok() != Some(header.chunk_len) {
                return Err(GetError::Inconsistent("chunks not their code's length"));
            }
            for i in 0..code.data() {
                let span = code.data_span(block_len, i)?;
                let lo = u64::try_from(span.start).map_err(|_| GetError::Overflow)?;
                let hi = u64::try_from(span.end).map_err(|_| GetError::Overflow)?;
                let (from, to) = (reading.want.start.max(lo), reading.want.end.min(hi));
                if from < to {
                    spans.push(Span {
                        chunk: i,
                        range: from.checked_sub(lo).ok_or(GetError::Overflow)?
                            ..to.checked_sub(lo).ok_or(GetError::Overflow)?,
                        got: None,
                        tried: BTreeSet::from([i]),
                    });
                }
            }
        }
        let count = spans.len();
        reading.spans = spans;
        reading.row = Some((header, places));
        for i in 0..count {
            self.ask_span(key, i)?;
        }
        self.finish(key)
    }

    /// Asks for span `i` of block `key` from the chunk it now names.
    fn ask_span(&mut self, key: u64, i: usize) -> Result<(), GetError> {
        let reading = self.reading.get(&key).ok_or(GetError::Mismatch)?;
        let span = reading.spans.get(i).ok_or(GetError::Mismatch)?;
        let (_, places) = reading.row.as_ref().ok_or(GetError::Mismatch)?;
        let place = places.get(span.chunk).ok_or(GetError::Overflow)?;
        let len = span
            .range
            .end
            .checked_sub(span.range.start)
            .ok_or(GetError::Overflow)?;
        let request = Request::Chunk {
            volume: place.volume,
            key: place.key,
            offset: span.range.start,
            len,
        };
        self.ask(request, Asked::Span(key, i))
    }

    /// Span `i` of block `key` answered: kept, or, when it failed, asked of another place that
    /// holds its bytes, a copy's any other, a code's chunk's kept one, or the block decoded from
    /// whole chunks instead.
    fn span(&mut self, key: u64, i: usize, got: Option<Bytes>) -> Result<(), GetError> {
        let reading = self.reading.get_mut(&key).ok_or(GetError::Mismatch)?;
        let block = reading.block;
        let (header, places) = reading.row.as_ref().ok_or(GetError::Mismatch)?;
        let data = header.data;
        let span = reading.spans.get_mut(i).ok_or(GetError::Mismatch)?;
        match got {
            Some(bytes) => {
                let expected = span
                    .range
                    .end
                    .checked_sub(span.range.start)
                    .ok_or(GetError::Overflow)?;
                if u64::try_from(bytes.len()).ok() != Some(expected) {
                    return Err(GetError::Inconsistent("a chunk read of another length"));
                }
                span.got = Some(bytes);
            }
            None => {
                let index = places
                    .get(span.chunk)
                    .map(|p| p.key.index)
                    .ok_or(GetError::Overflow)?;
                // A copy is the whole block wherever it lies; a coded chunk is its own bytes, in
                // its own place or one kept beside it.
                let next = (0..places.len()).find(|&c| {
                    !span.tried.contains(&c)
                        && (data == 1 || places.get(c).is_some_and(|p| p.key.index == index))
                });
                match next {
                    Some(next) => {
                        span.tried.insert(next);
                        span.chunk = next;
                        self.ask_span(key, i)?;
                    }
                    None if data == 1 => return Err(GetError::Unreadable(block)),
                    None => {
                        let tried = span.tried.clone();
                        let whole = reading.whole.get_or_insert_with(Box::default);
                        whole.tried.extend(tried);
                        // Decoded from whole chunks now, the block wants no span's bytes: none
                        // of its spans waits for room.
                        self.roomless
                            .retain(|asked| !matches!(asked, Asked::Span(k, _) if *k == key));
                        self.ask_wholes(key)?;
                    }
                }
            }
        }
        self.finish(key)
    }

    /// Asks for as many whole chunks of block `key`, not yet read, asked or failed, as decoding
    /// it still needs, data chunks first, each of the first of its places not yet tried.
    fn ask_wholes(&mut self, key: u64) -> Result<(), GetError> {
        let reading = self.reading.get_mut(&key).ok_or(GetError::Mismatch)?;
        let block = reading.block;
        let (header, places) = reading.row.as_ref().ok_or(GetError::Mismatch)?;
        let needed = usize::from(header.data);
        let width = needed
            .checked_add(usize::from(header.parity))
            .ok_or(GetError::Overflow)?;
        let whole = reading.whole.as_mut().ok_or(GetError::Mismatch)?;
        // A chunk none of whose places is left untried cannot be read.
        for c in 0..width {
            let untried = (0..places.len()).any(|p| {
                !whole.tried.contains(&p)
                    && places
                        .get(p)
                        .is_some_and(|pl| usize::from(pl.key.index) == c)
            });
            if !untried && !whole.got.contains_key(&c) && !whole.asked.contains(&c) {
                whole.failed.insert(c);
            }
        }
        let have = whole
            .got
            .len()
            .checked_add(whole.asked.len())
            .ok_or(GetError::Overflow)?;
        let short = needed.saturating_sub(have);
        let next: Vec<usize> = (0..width)
            .filter(|c| {
                !whole.got.contains_key(c) && !whole.asked.contains(c) && !whole.failed.contains(c)
            })
            .take(short)
            .collect();
        // Every chunk not yet tried is asked for when that is still short of the code's data
        // chunks: no read to come can make up the rest.
        if have.checked_add(next.len()).ok_or(GetError::Overflow)? < needed {
            return Err(GetError::Unreadable(block));
        }
        // A reservation the allocator refuses is an error, not an abort.
        let mut ats = Vec::new();
        ats.try_reserve_exact(next.len())
            .map_err(|_| GetError::Overflow)?;
        for c in next {
            let at = (0..places.len())
                .find(|&p| {
                    !whole.tried.contains(&p)
                        && places
                            .get(p)
                            .is_some_and(|pl| usize::from(pl.key.index) == c)
                })
                .ok_or(GetError::Overflow)?;
            whole.asked.insert(c);
            whole.tried.insert(at);
            ats.push(at);
        }
        for at in ats {
            self.ask_whole(key, at)?;
        }
        Ok(())
    }

    /// Asks for the whole chunk of block `key` its place at `at` holds.
    fn ask_whole(&mut self, key: u64, at: usize) -> Result<(), GetError> {
        let reading = self.reading.get(&key).ok_or(GetError::Mismatch)?;
        let (header, places) = reading.row.as_ref().ok_or(GetError::Mismatch)?;
        let place = places.get(at).ok_or(GetError::Overflow)?;
        let request = Request::Chunk {
            volume: place.volume,
            key: place.key,
            offset: 0,
            len: header.chunk_len,
        };
        self.ask(request, Asked::Whole(key, at))
    }

    /// A whole chunk read to decode block `key`, from the place at `at`, answered.
    fn whole(&mut self, key: u64, at: usize, got: Option<Bytes>) -> Result<(), GetError> {
        let reading = self.reading.get_mut(&key).ok_or(GetError::Mismatch)?;
        let (chunk_len, index) = match reading.row.as_ref() {
            Some((h, places)) => (
                h.chunk_len,
                places
                    .get(at)
                    .map(|p| usize::from(p.key.index))
                    .ok_or(GetError::Mismatch)?,
            ),
            None => return Err(GetError::Mismatch),
        };
        let whole = reading.whole.as_mut().ok_or(GetError::Mismatch)?;
        whole.asked.remove(&index);
        match got {
            Some(bytes) if u64::try_from(bytes.len()).ok() == Some(chunk_len) => {
                whole.got.insert(index, bytes);
            }
            Some(_) => return Err(GetError::Inconsistent("a chunk of another length")),
            // Asked again of another of its places, or another chunk asked in its stead.
            None => self.ask_wholes(key)?,
        }
        self.finish(key)
    }

    /// Once block `key`'s wanted bytes are all in hand and none of its reads is in flight,
    /// opens its segments, and gives out, in order, the range's plaintext of every block read
    /// whose turn has come.
    fn finish(&mut self, key: u64) -> Result<(), GetError> {
        let reading = self.reading.get(&key).ok_or(GetError::Mismatch)?;
        let Some((header, _)) = reading.row.as_ref() else {
            return Ok(());
        };
        if reading.inflight > 0 {
            return Ok(());
        }
        let block = reading.block;
        let want_len = usize::try_from(
            reading
                .want
                .end
                .checked_sub(reading.want.start)
                .ok_or(GetError::Overflow)?,
        )
        .map_err(|_| GetError::Overflow)?;
        let sealed: Vec<u8> = match &reading.whole {
            Some(whole) => {
                if whole.got.len() < usize::from(header.data) {
                    return Ok(());
                }
                let code = Code::new(header.data.into(), header.parity.into())?;
                let present: Vec<(usize, &[u8])> =
                    whole.got.iter().map(|(&c, b)| (c, b.as_ref())).collect();
                let len = usize::try_from(header.length).map_err(|_| GetError::Overflow)?;
                let decoded = code.decode(&present, len)?;
                if mantle_crc::crc32c(&decoded) != header.crc32c {
                    return Err(GetError::Corrupt(block));
                }
                let from = usize::try_from(reading.want.start).map_err(|_| GetError::Overflow)?;
                let to = from.checked_add(want_len).ok_or(GetError::Overflow)?;
                decoded.get(from..to).ok_or(GetError::Overflow)?.to_vec()
            }
            None => {
                if reading.spans.iter().any(|s| s.got.is_none()) {
                    return Ok(());
                }
                let mut sealed = Vec::new();
                sealed
                    .try_reserve_exact(want_len)
                    .map_err(|_| GetError::Overflow)?;
                for span in &reading.spans {
                    sealed.extend_from_slice(span.got.as_deref().unwrap_or_default());
                }
                sealed
            }
        };
        let reading = self.reading.remove(&key).ok_or(GetError::Mismatch)?;
        if let (Some(begun), Some(now)) = (reading.begun_ns, self.now_ns) {
            self.read_ns = Some(now.saturating_sub(begun));
        }
        let plain = self.plain.as_mut().ok_or(GetError::Mismatch)?;
        let segment = u64::try_from(seal::SEGMENT).map_err(|_| GetError::Overflow)?;
        let last = plain.count.checked_sub(1).ok_or(GetError::Overflow)?;
        // Each segment opened where it lies in `sealed`, its plaintext laid after the range's
        // part of the one before, over the tags (`Segments::open_within`): the range's plaintext
        // ends up as one run of the buffer the block was gathered into, handed out as a slice of
        // it, where each segment was copied out to be opened and its plaintext copied again.
        let mut sealed = sealed;
        let (mut at, mut end) = (0usize, 0usize);
        for s in reading.segments.clone() {
            let len = if s == last {
                plain
                    .stored
                    .checked_sub(s.checked_mul(SEALED).ok_or(GetError::Overflow)?)
                    .ok_or(GetError::Overflow)?
            } else {
                SEALED
            };
            let len = usize::try_from(len).map_err(|_| GetError::Overflow)?;
            let to = at.checked_add(len).ok_or(GetError::Overflow)?;
            if to > sealed.len() {
                return Err(GetError::Inconsistent("a block shorter than its segments"));
            }
            let within = sealed.get_mut(end..to).ok_or(GetError::Overflow)?;
            let from = at.checked_sub(end).ok_or(GetError::Overflow)?;
            let opened = plain
                .segments
                .open_within(s, s == last, within, from)
                .map_err(|_| GetError::Corrupt(block))?;
            at = to;
            // The segment's plaintext, cut to the range: a part that begins past the segment's
            // start (the range's first, mid-segment) is turned to the front of what it opened.
            let first = s.checked_mul(segment).ok_or(GetError::Overflow)?;
            let take_from = plain.from.saturating_sub(first);
            let take_to = plain.to.checked_sub(first).ok_or(GetError::Overflow)?;
            let opened_len = u64::try_from(opened).map_err(|_| GetError::Overflow)?;
            let (lo, hi) = (take_from.min(opened_len), take_to.min(opened_len));
            let lo = usize::try_from(lo).map_err(|_| GetError::Overflow)?;
            let hi = usize::try_from(hi).map_err(|_| GetError::Overflow)?;
            let cut = hi.checked_sub(lo).ok_or(GetError::Overflow)?;
            if lo > 0 {
                let part_end = end.checked_add(hi).ok_or(GetError::Overflow)?;
                let part = sealed.get_mut(end..part_end).ok_or(GetError::Overflow)?;
                to_front(part, lo).ok_or(GetError::Overflow)?;
            }
            end = end.checked_add(cut).ok_or(GetError::Overflow)?;
        }
        sealed.truncate(end);
        let out = Bytes::from(sealed);
        self.read
            .insert(reading.segments.start, (reading.segments.end, out));
        // Blocks read out of order wait for the ones before them.
        while let Some((end, bytes)) = self.read.remove(&plain.emit) {
            plain.emit = end;
            if !bytes.is_empty() {
                self.out.push_back(bytes);
            }
        }
        Ok(())
    }
}

/// Moves `bytes[from..]` to the front of `bytes`, a byte at a time from the first, each read before
/// the write that could overlap it: at most a 64 KiB segment's bytes.
fn to_front(bytes: &mut [u8], from: usize) -> Option<()> {
    let len = bytes.len().checked_sub(from)?;
    for at in 0..len {
        let byte = *bytes.get(from.checked_add(at)?)?;
        *bytes.get_mut(at)? = byte;
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use mantle_meta::record::{Referrer, Wrapper};
    use mantle_s3::seal::WrappingKey;

    use super::*;

    const FILE: u128 = 7;
    const BLOCK: u128 = 9;

    /// The key every file of these tests is wrapped under.
    struct Root(WrappingKey);

    impl Keyring for Root {
        fn unwrap(&self, key: &WrappedKey) -> Result<DataKey, GetError> {
            DataKey::unwrap(&key.bytes, &self.0).map_err(|_| GetError::Key)
        }
    }

    /// An object of `plain` plaintext in one block of one segment, its chunks under `scheme` on
    /// volumes 1, 2 and 3, and a GET of all of it holding one block, asked as far as its chunk
    /// reads: the GET, its chunks' bytes by place, and the chunk reads it asked.
    fn reading(scheme: Scheme, plain: &[u8]) -> (Get, Vec<Vec<u8>>, Vec<(Id, Request)>) {
        let wrapping = WrappingKey::generate().unwrap();
        let data = DataKey::generate().unwrap();
        let mut sealed = plain.to_vec();
        Segments::new(&data, FILE)
            .unwrap()
            .seal(0, true, &mut sealed)
            .unwrap();
        let length = sealed.len() as u64;
        let (header, chunks) = match scheme {
            Scheme::Copies(n) => {
                let header = BlockHeader {
                    length,
                    data: 1,
                    parity: (n - 1) as u8,
                    chunk_len: length,
                    crc32c: mantle_crc::crc32c(&sealed),
                };
                (header, vec![sealed.clone(); n])
            }
            Scheme::Rs(code) => {
                let header = BlockHeader {
                    length,
                    data: code.data() as u8,
                    parity: code.parity() as u8,
                    chunk_len: code.chunk_len(sealed.len()).unwrap() as u64,
                    crc32c: mantle_crc::crc32c(&sealed),
                };
                (header, code.encode(&sealed).unwrap())
            }
        };
        let places = (0..chunks.len())
            .map(|i| ChunkPlace {
                volume: i as u128 + 1,
                key: ChunkKey {
                    block: BLOCK,
                    epoch: 1,
                    index: if header.data == 1 { 0 } else { i as u16 },
                },
            })
            .collect();
        let key = WrappedKey {
            by: Wrapper::Root(1),
            bytes: data.wrap(&wrapping).unwrap(),
        };
        let size = plain.len() as u64;
        let mut get = Get::new(Some(FILE), size, 0..size, Box::new(Root(wrapping)), 1).unwrap();
        let file = FileHeader {
            length,
            extents: 1,
            made_ns: 0,
            deadline_ns: 0,
            referrer: Referrer {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
            },
            key: Some(key),
        };
        let extent = Extent {
            length,
            target: Target::Block(BLOCK),
        };
        let answers = [
            Answer::Header(Some(file)),
            Answer::Extents(vec![(0, extent)]),
            Answer::Block(Some((header, places))),
        ];
        for answer in answers {
            let (id, _) = get.poll().unwrap();
            get.answer(id, answer).unwrap();
        }
        let asked = std::iter::from_fn(|| get.poll()).collect();
        (get, chunks, asked)
    }

    /// The bytes a chunk read asks of `chunks`, its places' bytes.
    fn served(chunks: &[Vec<u8>], request: &Request) -> Answer {
        let Request::Chunk {
            volume,
            offset,
            len,
            ..
        } = *request
        else {
            panic!("{request:?}");
        };
        let chunk = &chunks[volume as usize - 1];
        Answer::Chunk(Bytes::copy_from_slice(
            &chunk[offset as usize..(offset + len) as usize],
        ))
    }

    /// Answers each read `asked` from `chunks`, and each the GET asks after, until it ends: the
    /// plaintext it gave out.
    fn read_out(get: &mut Get, chunks: &[Vec<u8>], asked: Vec<(Id, Request)>) -> Vec<u8> {
        let mut asked = VecDeque::from(asked);
        let mut out = Vec::new();
        while let Some((id, request)) = asked.pop_front() {
            get.answer(id, served(chunks, &request)).unwrap();
            asked.extend(std::iter::from_fn(|| get.poll()));
            while let Some(bytes) = get.take() {
                out.extend_from_slice(&bytes);
            }
        }
        assert_eq!(get.outcome(), Some(&Ok(())));
        out
    }

    fn schemes() -> [Scheme; 2] {
        [Scheme::Copies(3), Scheme::Rs(Code::new(2, 1).unwrap())]
    }

    /// A chunk read refused for want of room, at the volume's reads or in the transport toward
    /// it, is not a copy lost: the GET neither reads another place nor ends, but waits, and once
    /// room frees asks the same place for the same bytes again, its plaintext then exact. It read
    /// another place for every refusal, room's among them, and a cell of three whose copies were
    /// all refused for room ended `Unreadable`, `503 SlowDown`.
    #[test]
    fn a_chunk_read_refused_for_room_waits_and_asks_the_same_place_again() {
        let plain = b"the plaintext of a small object, read back exactly".to_vec();
        for scheme in schemes() {
            let (mut get, chunks, mut asked) = reading(scheme, &plain);
            let (mut id, request) = asked.remove(0);
            // Refused for room at each turn of room, then read.
            for _ in 0..3 {
                get.answer(id, Answer::Busy).unwrap();
                assert!(get.waits_for_room(), "{scheme:?}");
                assert!(get.outcome().is_none(), "{scheme:?}");
                assert!(get.poll().is_none(), "{scheme:?} read another place");
                get.room().unwrap();
                assert!(!get.waits_for_room(), "{scheme:?}");
                let (again, same) = get.poll().unwrap();
                assert_eq!(same, request, "{scheme:?}");
                id = again;
            }
            asked.insert(0, (id, request));
            assert!(read_out(&mut get, &chunks, asked) == plain, "{scheme:?}");
        }
    }

    /// A coded block's whole chunk, read once a span's every place failed, refused for room waits
    /// still counted as asked, and is asked of the same place again: the block is decoded exactly,
    /// and no other chunk is read in its stead.
    #[test]
    fn a_whole_chunk_refused_for_room_is_asked_of_its_place_again() {
        let plain = b"a coded block decoded from its whole chunks".to_vec();
        let (mut get, chunks, asked) = reading(Scheme::Rs(Code::new(2, 1).unwrap()), &plain);
        let (span, _) = &asked[0];
        get.answer(*span, Answer::Unreadable).unwrap();
        let wholes: Vec<_> = std::iter::from_fn(|| get.poll()).collect();
        assert_eq!(wholes.len(), 2, "{wholes:?}");
        let (id, request) = wholes[0].clone();
        get.answer(id, Answer::Busy).unwrap();
        assert!(get.waits_for_room());
        assert!(get.poll().is_none(), "another chunk read in its stead");
        get.room().unwrap();
        let again = get.poll().unwrap();
        assert_eq!(again.1, request);
        let rest = asked[1..]
            .iter()
            .cloned()
            .chain([again, wholes[1].clone()])
            .collect();
        assert!(read_out(&mut get, &chunks, rest) == plain);
    }

    /// A place that cannot give the bytes (its volume fenced, closing or not there, the chunk not
    /// found or corrupt, its peer gone) still sends the read to another place; with too few places
    /// readable for such causes the GET ends `Unreadable`, however often room was refused
    /// meanwhile.
    #[test]
    fn a_place_that_cannot_be_read_moves_on_and_too_few_end_unreadable() {
        for scheme in schemes() {
            let (mut get, _, asked) = reading(scheme, b"held at three places");
            // The first span's reads, each refused for room and then found unreadable; the rest
            // left out.
            let mut queue = VecDeque::from([asked[0].clone()]);
            let mut volumes = BTreeSet::new();
            let ended = loop {
                let (id, request) = queue.pop_front().expect("the GET asks on");
                let Request::Chunk { volume, .. } = request else {
                    panic!("{request:?}");
                };
                get.answer(id, Answer::Busy).unwrap();
                assert!(
                    get.poll().is_none(),
                    "{scheme:?}: read another place for room"
                );
                get.room().unwrap();
                let (again, same) = get.poll().unwrap();
                assert_eq!(same, request, "{scheme:?}");
                volumes.insert(volume);
                if let Err(e) = get.answer(again, Answer::Unreadable) {
                    break e;
                }
                queue.extend(std::iter::from_fn(|| get.poll()));
            };
            assert_eq!(ended, GetError::Unreadable(BLOCK), "{scheme:?}");
            assert_eq!(get.outcome(), Some(&Err(GetError::Unreadable(BLOCK))));
            // A copy's every place; a code's first data chunk's and the whole chunk read in its
            // place, after which too few are left to decode it.
            let read = match scheme {
                Scheme::Copies(_) => 3,
                Scheme::Rs(_) => 2,
            };
            assert_eq!(volumes.len(), read, "{scheme:?}: {volumes:?}");
        }
    }

    /// A read no volume serves as it was asked ends the GET at once, `Unsendable`: another place
    /// would refuse it alike.
    #[test]
    fn a_read_no_volume_serves_as_asked_ends_the_get() {
        for scheme in schemes() {
            let (mut get, _, asked) = reading(scheme, b"asked as no volume serves it");
            let failed = get.answer(asked[0].0, Answer::Failed);
            assert_eq!(failed, Err(GetError::Unsendable(BLOCK)), "{scheme:?}");
            assert!(get.poll().is_none(), "{scheme:?}");
        }
    }

    /// A GET whose read waits for room past its client's patience ends `Patience`, holding
    /// nothing, and asks nothing more.
    #[test]
    fn a_get_waiting_for_room_past_its_clients_patience_ends_patience() {
        let (mut get, _, asked) = reading(Scheme::Copies(3), b"never given room");
        get.answer(asked[0].0, Answer::Busy).unwrap();
        get.out_of_patience();
        assert_eq!(get.outcome(), Some(&Err(GetError::Patience)));
        assert!(!get.waits_for_room());
        assert!(get.reading.is_empty() && get.poll().is_none());
        assert_eq!(get.room(), Err(GetError::Over));
    }
}
