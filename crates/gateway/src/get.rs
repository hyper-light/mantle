//! Reading an object or a range of it (docs/design/gateway.md §3): the extents holding the
//! wanted bytes found a page at a time from the one holding the first, each block's row read
//! for its chunks' places, only the chunk bytes holding the wanted segments read, a chunk that
//! fails replaced by another copy or by decoding its block from any `data` of its chunks, and
//! each segment opened under its file's key.
//!
//! A [`Get`] names each request and moves on with its answer, doing no I/O, as a PUT does. It
//! starts from the version the Name range read, whose preconditions, lock and range the caller
//! has judged, and gives out the range's plaintext in order. It holds at most as many blocks,
//! being read or waiting to be taken, as its caller admits: with two, one is read while the one
//! before it waits for the caller; with more, the reads of several blocks overlap, whose round
//! trips otherwise add up (audit §16.3, §16.7).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Range;

use bytes::Bytes;
use mantle_chunk::ChunkKey;
use mantle_ec::{Code, EcError};
use mantle_meta::record::{BlockHeader, ChunkPlace, Extent, FileHeader, Target, WrappedKey};
use mantle_s3::seal::{self, DataKey, SealError, Segments};

use crate::layout::SEALED;

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
    /// The volume did not give the bytes: it failed, did not answer in time, or found them
    /// corrupt.
    Unreadable,
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
    /// The chunk's index, and the bytes of it read.
    chunk: usize,
    range: Range<u64>,
    got: Option<Bytes>,
    /// For copies: the chunks tried for this span.
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
    /// Once a span of a coded block fails: the whole chunks read to decode it.
    whole: Option<Whole>,
    /// Its requests in flight: it finishes only once none is, so no answer outlives it.
    inflight: usize,
}

#[derive(Default)]
struct Whole {
    got: BTreeMap<usize, Bytes>,
    asked: BTreeSet<usize>,
    failed: BTreeSet<usize>,
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
    Whole(u64, usize),
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
    next_id: Id,
    outcome: Option<Result<(), GetError>>,
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
            next_id: 0,
            outcome: None,
        };
        get.guard(Self::advance)?;
        Ok(get)
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

    /// The next plaintext of the range, in order; `None` while none is ready.
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
            _ => Err(GetError::Mismatch),
        }
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
        let end = range.end.div_ceil(segment);
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
        let holds_end = end.div_ceil(SEALED);
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
        if header.length != len || places.len() != width || header.data == 0 {
            return Err(GetError::Inconsistent(
                "a block row that is not its extent's",
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

    /// Span `i` of block `key` answered: kept, or, when it failed, asked of another copy, or
    /// the block decoded from whole chunks instead.
    fn span(&mut self, key: u64, i: usize, got: Option<Bytes>) -> Result<(), GetError> {
        let reading = self.reading.get_mut(&key).ok_or(GetError::Mismatch)?;
        let block = reading.block;
        let (header, _) = reading.row.as_ref().ok_or(GetError::Mismatch)?;
        let (data, width) = (
            header.data,
            usize::from(header.data)
                .checked_add(usize::from(header.parity))
                .ok_or(GetError::Overflow)?,
        );
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
            None if data == 1 => {
                // Another copy, one not tried for this span.
                let next = (0..width)
                    .find(|c| !span.tried.contains(c))
                    .ok_or(GetError::Unreadable(block))?;
                span.tried.insert(next);
                span.chunk = next;
                self.ask_span(key, i)?;
            }
            None => {
                let chunk = span.chunk;
                reading
                    .whole
                    .get_or_insert_with(Whole::default)
                    .failed
                    .insert(chunk);
                self.ask_wholes(key)?;
            }
        }
        self.finish(key)
    }

    /// Asks for as many whole chunks of block `key`, not yet asked or failed, as decoding it
    /// still needs, data chunks first.
    fn ask_wholes(&mut self, key: u64) -> Result<(), GetError> {
        let reading = self.reading.get_mut(&key).ok_or(GetError::Mismatch)?;
        let block = reading.block;
        let (header, places) = reading.row.as_ref().ok_or(GetError::Mismatch)?;
        let needed = usize::from(header.data);
        let chunk_len = header.chunk_len;
        let whole = reading.whole.as_mut().ok_or(GetError::Mismatch)?;
        let have = whole
            .got
            .len()
            .checked_add(whole.asked.len())
            .ok_or(GetError::Overflow)?;
        let short = needed.saturating_sub(have);
        let next: Vec<usize> = (0..places.len())
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
        let mut requests = Vec::with_capacity(next.len());
        for c in next {
            let place = places.get(c).ok_or(GetError::Overflow)?;
            whole.asked.insert(c);
            requests.push((
                Request::Chunk {
                    volume: place.volume,
                    key: place.key,
                    offset: 0,
                    len: chunk_len,
                },
                Asked::Whole(key, c),
            ));
        }
        for (request, asked) in requests {
            self.ask(request, asked)?;
        }
        Ok(())
    }

    /// A whole chunk read to decode block `key` answered.
    fn whole(&mut self, key: u64, c: usize, got: Option<Bytes>) -> Result<(), GetError> {
        let reading = self.reading.get_mut(&key).ok_or(GetError::Mismatch)?;
        let chunk_len = reading.row.as_ref().map_or(0, |(h, _)| h.chunk_len);
        let whole = reading.whole.as_mut().ok_or(GetError::Mismatch)?;
        whole.asked.remove(&c);
        match got {
            Some(bytes) if u64::try_from(bytes.len()).ok() == Some(chunk_len) => {
                whole.got.insert(c, bytes);
            }
            Some(_) => return Err(GetError::Inconsistent("a chunk of another length")),
            None => {
                whole.failed.insert(c);
                self.ask_wholes(key)?;
            }
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
                let mut sealed = Vec::with_capacity(want_len);
                for span in &reading.spans {
                    sealed.extend_from_slice(span.got.as_deref().unwrap_or_default());
                }
                sealed
            }
        };
        let reading = self.reading.remove(&key).ok_or(GetError::Mismatch)?;
        let plain = self.plain.as_mut().ok_or(GetError::Mismatch)?;
        let segment = u64::try_from(seal::SEGMENT).map_err(|_| GetError::Overflow)?;
        let last = plain.count.checked_sub(1).ok_or(GetError::Overflow)?;
        let mut out = Vec::with_capacity(want_len);
        let mut rest = sealed.as_slice();
        for s in reading.segments.clone() {
            let len = if s == last {
                plain
                    .stored
                    .checked_sub(s.checked_mul(SEALED).ok_or(GetError::Overflow)?)
                    .ok_or(GetError::Overflow)?
            } else {
                SEALED
            };
            let (this, after) = rest
                .split_at_checked(usize::try_from(len).map_err(|_| GetError::Overflow)?)
                .ok_or(GetError::Inconsistent("a block shorter than its segments"))?;
            rest = after;
            let mut bytes = this.to_vec();
            plain
                .segments
                .open(s, s == last, &mut bytes)
                .map_err(|_| GetError::Corrupt(block))?;
            // The segment's plaintext, cut to the range.
            let first = s.checked_mul(segment).ok_or(GetError::Overflow)?;
            let take_from = plain.from.saturating_sub(first);
            let take_to = plain.to.checked_sub(first).ok_or(GetError::Overflow)?;
            let bytes_len = u64::try_from(bytes.len()).map_err(|_| GetError::Overflow)?;
            let (lo, hi) = (take_from.min(bytes_len), take_to.min(bytes_len));
            let lo = usize::try_from(lo).map_err(|_| GetError::Overflow)?;
            let hi = usize::try_from(hi).map_err(|_| GetError::Overflow)?;
            out.extend_from_slice(bytes.get(lo..hi).ok_or(GetError::Overflow)?);
        }
        self.read.insert(
            reading.segments.start,
            (reading.segments.end, Bytes::from(out)),
        );
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
