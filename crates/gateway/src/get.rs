//! Reading an object or a range of it (docs/design/gateway.md §3): the extent holding each
//! wanted byte found by one seek, each block's row read for its chunks' places, only the chunk
//! bytes holding the wanted segments read, a chunk that fails replaced by another copy or by
//! decoding its block from any `data` of its chunks, and each segment opened under its file's
//! key.
//!
//! A [`Get`] names each request and moves on with its answer, doing no I/O, as a PUT does. It
//! starts from the version the Name range read, whose preconditions, lock and range the caller
//! has judged, and gives out the range's plaintext in order. It holds at most two blocks'
//! plaintext: one read while the one before it waits for the caller to take it, the fewest
//! that overlap reading with sending, as a PUT's two blocks overlap receiving with writing.

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

/// Blocks' plaintext a GET holds at most: one being read and one waiting to be taken.
const HELD: usize = 2;

/// A GET's request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// A file's header, from the File range that holds it.
    Header { file: u128 },
    /// The extent of `file` holding byte `offset`, with the offset of its first byte: a
    /// stored byte of a file of blocks, a plaintext byte of a file of parts.
    Extent { file: u128, offset: u64 },
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
    /// The extent and the offset of its first byte; `None` past the file's end.
    Extent(Option<(u64, Extent)>),
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
    /// The next segment to read, and the one past the range's last.
    next: u64,
    end: u64,
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
    /// Once a span of a coded block fails: the whole chunks read to decode it, by index,
    /// those asked and not yet answered, and those that failed.
    whole: Option<Whole>,
}

#[derive(Default)]
struct Whole {
    got: BTreeMap<usize, Bytes>,
    asked: BTreeSet<usize>,
    failed: BTreeSet<usize>,
}

/// What a request in flight asked.
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
    /// The extent of the file being read holding its stored byte `at`.
    BlockExtent,
    Block,
    Span(usize),
    Whole(usize),
}

pub struct Get {
    file: Option<u128>,
    size: u64,
    keyring: Box<dyn Keyring + Send>,
    /// The object's plaintext wanted, and the first byte of it not yet handed to a file.
    to: u64,
    at: u64,
    /// The object's file of parts, once its header says it is one.
    parts: Option<u128>,
    plain: Option<Plain>,
    reading: Option<Reading>,
    /// Plaintext ready for the caller, a block's at a time.
    out: VecDeque<Bytes>,
    ready: VecDeque<(Id, Request)>,
    asked: BTreeMap<Id, Asked>,
    next_id: Id,
    outcome: Option<Result<(), GetError>>,
}

impl Get {
    /// A GET of plaintext `range` of an object of `size` bytes held in `file`, none for an
    /// empty object, whose keys `keyring` unwraps.
    pub fn new(
        file: Option<u128>,
        size: u64,
        range: Range<u64>,
        keyring: Box<dyn Keyring + Send>,
    ) -> Result<Self, GetError> {
        if range.start > range.end || range.end > size {
            return Err(GetError::Range);
        }
        let mut get = Self {
            file,
            size,
            keyring,
            to: range.end,
            at: range.start,
            parts: None,
            plain: None,
            reading: None,
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
        // The room it held may let the next block be read.
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
    }

    fn ask(&mut self, request: Request, asked: Asked) -> Result<(), GetError> {
        let id = self.next_id;
        self.next_id = id.checked_add(1).ok_or(GetError::Overflow)?;
        self.asked.insert(id, asked);
        self.ready.push_back((id, request));
        Ok(())
    }

    /// Asks for what comes next, when nothing it waits on is in flight.
    fn advance(&mut self) -> Result<(), GetError> {
        if self.outcome.is_some() || self.reading.is_some() || !self.asked.is_empty() {
            return Ok(());
        }
        if let Some(plain) = &self.plain {
            if plain.next < plain.end {
                if self.out.len() >= HELD {
                    return Ok(());
                }
                let offset = plain.next.checked_mul(SEALED).ok_or(GetError::Overflow)?;
                let file = plain.file;
                return self.ask(Request::Extent { file, offset }, Asked::BlockExtent);
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
                Request::Extent {
                    file: parts,
                    offset: self.at,
                },
                Asked::PartExtent,
            ),
        }
    }

    fn answered(&mut self, asked: Asked, answer: Answer) -> Result<(), GetError> {
        match (asked, answer) {
            (Asked::Top, Answer::Header(header)) => self.top(header),
            (Asked::Part { file, start, len }, Answer::Header(header)) => {
                self.part(file, start, len, header)
            }
            (Asked::PartExtent, Answer::Extent(extent)) => self.part_extent(extent),
            (Asked::BlockExtent, Answer::Extent(extent)) => self.block_extent(extent),
            (Asked::Block, Answer::Block(row)) => self.block_row(row),
            (Asked::Span(i), Answer::Chunk(bytes)) => self.span(i, Some(bytes)),
            (Asked::Span(i), Answer::Unreadable) => self.span(i, None),
            (Asked::Whole(i), Answer::Chunk(bytes)) => self.whole(i, Some(bytes)),
            (Asked::Whole(i), Answer::Unreadable) => self.whole(i, None),
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
    fn part_extent(&mut self, extent: Option<(u64, Extent)>) -> Result<(), GetError> {
        let (start, extent) = extent.ok_or(GetError::Inconsistent(
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
        });
        Ok(())
    }

    /// The block holding the next wanted segment: the segments of it the range wants.
    fn block_extent(&mut self, extent: Option<(u64, Extent)>) -> Result<(), GetError> {
        let plain = self.plain.as_ref().ok_or(GetError::Mismatch)?;
        let (start, extent) =
            extent.ok_or(GetError::Inconsistent("a file ends before its length"))?;
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
        self.reading = Some(Reading {
            block,
            stored: start..end,
            segments,
            want,
            row: None,
            spans: Vec::new(),
            whole: None,
        });
        self.ask(Request::Block { block }, Asked::Block)
    }

    /// The block's row: reads the chunk bytes that hold the wanted range.
    fn block_row(&mut self, row: Option<(BlockHeader, Vec<ChunkPlace>)>) -> Result<(), GetError> {
        let reading = self.reading.as_mut().ok_or(GetError::Mismatch)?;
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
        reading.spans = spans;
        reading.row = Some((header, places));
        for i in 0..self.span_count() {
            self.ask_span(i)?;
        }
        self.finish_block()
    }

    fn span_count(&self) -> usize {
        self.reading.as_ref().map_or(0, |r| r.spans.len())
    }

    /// Asks for span `i` from the chunk it now names.
    fn ask_span(&mut self, i: usize) -> Result<(), GetError> {
        let reading = self.reading.as_ref().ok_or(GetError::Mismatch)?;
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
        self.ask(request, Asked::Span(i))
    }

    /// Span `i` answered: kept, or, when it failed, asked of another copy, or the block
    /// decoded from whole chunks instead.
    fn span(&mut self, i: usize, got: Option<Bytes>) -> Result<(), GetError> {
        let reading = self.reading.as_mut().ok_or(GetError::Mismatch)?;
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
                self.ask_span(i)?;
            }
            None => {
                if reading.whole.is_none() {
                    reading.whole = Some(Whole::default());
                }
                let chunk = span.chunk;
                if let Some(whole) = reading.whole.as_mut() {
                    whole.failed.insert(chunk);
                }
                self.ask_wholes()?;
            }
        }
        self.finish_block()
    }

    /// Asks for as many whole chunks, not yet asked or failed, as decoding the block still
    /// needs, data chunks first.
    fn ask_wholes(&mut self) -> Result<(), GetError> {
        let reading = self.reading.as_mut().ok_or(GetError::Mismatch)?;
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
                Asked::Whole(c),
            ));
        }
        for (request, asked) in requests {
            self.ask(request, asked)?;
        }
        Ok(())
    }

    /// A whole chunk read to decode the block answered.
    fn whole(&mut self, c: usize, got: Option<Bytes>) -> Result<(), GetError> {
        let reading = self.reading.as_mut().ok_or(GetError::Mismatch)?;
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
                self.ask_wholes()?;
            }
        }
        self.finish_block()
    }

    /// Once the block's wanted bytes are all in hand, and none of its reads is in flight, opens
    /// their segments and gives out the range's plaintext of them. A read still in flight is
    /// waited for, so that no answer outlives the block it was for.
    fn finish_block(&mut self) -> Result<(), GetError> {
        if !self.asked.is_empty() {
            return Ok(());
        }
        let Some(reading) = self.reading.as_ref() else {
            return Ok(());
        };
        let Some((header, _)) = reading.row.as_ref() else {
            return Ok(());
        };
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
        let reading = self.reading.take().ok_or(GetError::Mismatch)?;
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
        plain.next = reading.segments.end;
        if !out.is_empty() {
            self.out.push_back(Bytes::from(out));
        }
        Ok(())
    }
}
