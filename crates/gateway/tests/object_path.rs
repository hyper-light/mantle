#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

//! The gateway's object path against a cell in memory: volumes that check each chunk's CRC-32C
//! before they keep it, and a Block, a File and a Name range that apply commands as their logs
//! would, each entry stamped with a clock the test moves. The gateway's time is the same clock.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Range;

use bytes::Bytes;
use mantle_chunk::ChunkKey;
use mantle_ec::Code;
use mantle_ec::durability::Scheme;
use mantle_gateway::complete::{self, CompleteError, Completed, Completion};
use mantle_gateway::get::{self, Get, GetError, Keyring};
use mantle_gateway::layout::{Layout, SEALED};
use mantle_gateway::put::{
    Answer, Body, Commit, Expected, Holding, Ids, Keys, Put, PutError, Request, Stored,
};
use mantle_meta::engine::{Engine, Model};
use mantle_meta::name::{self, CreateUpload, GateChange, Preconditions, PutPart};
use mantle_meta::record::{
    Extent, GateState, Part, Referrer, Target, Upload, Version, Versioning, WrappedKey, Wrapper,
};
use mantle_meta::{block, file};
use mantle_s3::checksum::{self, Algorithm};
use mantle_s3::seal::{self, DataKey, Segments, WrappingKey};
use proptest::prelude::*;

const BUCKET: &str = "b";
/// How long a block or file is held for the write above it, in the cell's clock.
const HANDOVER: u64 = 1_000;

struct Cell {
    volumes: BTreeMap<u128, BTreeMap<ChunkKey, Bytes>>,
    /// Volumes that refuse every chunk.
    refusing: BTreeSet<u128>,
    blocks: Model,
    block_index: u64,
    files: Model,
    file_index: u64,
    names: Model,
    name_index: u64,
    clock: u64,
    /// The requests served, by kind: what a test asks of the path's traffic.
    renewals: usize,
    /// Volumes whose reads fail.
    unreadable: BTreeSet<u128>,
    /// A GET's requests served, by kind.
    gets: BTreeMap<&'static str, usize>,
}

impl Cell {
    /// A cell of `volumes` volumes and a Name range holding bucket `b`, open.
    fn new(volumes: u128) -> Self {
        let mut names = Model::default();
        names.install(0, name::first(1).unwrap()).unwrap();
        let mut cell = Self {
            volumes: (1..=volumes).map(|v| (v, BTreeMap::new())).collect(),
            refusing: BTreeSet::new(),
            blocks: Model::default(),
            block_index: 0,
            files: Model::default(),
            file_index: 0,
            names,
            name_index: 0,
            clock: 1_000_000,
            renewals: 0,
            unreadable: BTreeSet::new(),
            gets: BTreeMap::new(),
        };
        let open = name::Command::Gate(GateChange {
            bucket: BUCKET.into(),
            incarnation: 1,
            attempt: 1,
            from: None,
            to: Some(GateState::Open),
            generation: 1,
        });
        assert_eq!(cell.name(open), name::Outcome::GateMoved);
        cell
    }

    fn name(&mut self, mut command: name::Command) -> name::Outcome {
        match &mut command {
            name::Command::Put(p) => p.at_ns = self.clock,
            name::Command::PutPart(p) => p.at_ns = self.clock,
            name::Command::CreateUpload(c) => c.at_ns = self.clock,
            _ => {}
        }
        self.name_index += 1;
        name::apply(&mut self.names, self.name_index, &command).unwrap()
    }

    fn block(&mut self, mut command: block::Command) -> block::Outcome {
        match &mut command {
            block::Command::Write { at_ns, .. } | block::Command::Renew { at_ns, .. } => {
                *at_ns = self.clock;
            }
            _ => {}
        }
        if matches!(command, block::Command::Renew { .. }) {
            self.renewals += 1;
        }
        self.block_index += 1;
        block::apply(&mut self.blocks, self.block_index, &command).unwrap()
    }

    fn file(&mut self, mut command: file::Command) -> file::Outcome {
        if let file::Command::Write { at_ns, .. } = &mut command {
            *at_ns = self.clock;
        }
        self.file_index += 1;
        file::apply(&mut self.files, self.file_index, &command).unwrap()
    }

    /// What the cell answers a PUT's request.
    fn serve(&mut self, request: Request) -> Answer {
        match request {
            Request::Place { block, .. } => {
                // Every volume, from one the block picks, so blocks spread.
                let ids: Vec<u128> = self.volumes.keys().copied().collect();
                let start = (block % ids.len() as u128) as usize;
                Answer::Volumes(ids[start..].iter().chain(&ids[..start]).copied().collect())
            }
            Request::Chunk {
                volume,
                key,
                bytes,
                crc,
            } => {
                if self.refusing.contains(&volume) || mantle_crc::crc32c(&bytes) != crc {
                    return Answer::Refused;
                }
                self.volumes.get_mut(&volume).unwrap().insert(key, bytes);
                Answer::Stored
            }
            Request::Block(c) => Answer::Block(self.block(c)),
            Request::File(c) => Answer::File(self.file(c)),
            Request::Name(c) => Answer::Name(self.name(*c)),
        }
    }

    /// What the cell answers a GET's request: rows read as a range reads them, and chunk
    /// bytes from volumes that fail those in `unreadable`.
    fn serve_get(&mut self, request: get::Request) -> get::Answer {
        let kind = match &request {
            get::Request::Header { .. } => "header",
            get::Request::Extents { .. } => "extent",
            get::Request::Block { .. } => "block",
            get::Request::Chunk { .. } => "chunk",
        };
        *self.gets.entry(kind).or_default() += 1;
        match request {
            get::Request::Header { file } => {
                get::Answer::Header(file::header(&self.files, file).unwrap())
            }
            get::Request::Extents { file, offset, max } => {
                get::Answer::Extents(file::extents(&self.files, file, offset, max).unwrap())
            }
            get::Request::Block { block } => {
                get::Answer::Block(block::read(&self.blocks, block).unwrap())
            }
            get::Request::Chunk {
                volume,
                key,
                offset,
                len,
            } => {
                if self.unreadable.contains(&volume) {
                    return get::Answer::Unreadable;
                }
                match self.volumes.get(&volume).and_then(|v| v.get(&key)) {
                    Some(b) if offset + len <= b.len() as u64 => {
                        get::Answer::Chunk(b.slice(offset as usize..(offset + len) as usize))
                    }
                    _ => get::Answer::Unreadable,
                }
            }
        }
    }

    fn create_upload(&mut self, key: &str) -> String {
        self.create_upload_with(key, None)
    }

    /// An upload whose parts carry checksums in `checksum`'s algorithm, combined full-object
    /// or composite.
    fn create_upload_with(&mut self, key: &str, checksum: Option<(u8, bool)>) -> String {
        let create = name::Command::CreateUpload(CreateUpload {
            bucket: BUCKET.into(),
            incarnation: 1,
            key: key.into(),
            at_ns: 0,
            upload: Upload {
                initiated_ns: 0,
                owner: "o".into(),
                headers: Vec::new(),
                checksum,
                retention: None,
                legal_hold: None,
            },
        });
        match self.name(create) {
            name::Outcome::Created { upload } => upload,
            other => panic!("{other:?}"),
        }
    }

    /// What the cell answers a completion's request for `key`'s upload `upload`.
    fn serve_complete(
        &mut self,
        key: &str,
        upload: &str,
        request: complete::Request,
    ) -> complete::Answer {
        match request {
            complete::Request::Upload => {
                complete::Answer::Upload(name::upload(&self.names, BUCKET, key, upload).unwrap())
            }
            complete::Request::Parts { after, max } => complete::Answer::Parts(
                name::parts(&self.names, BUCKET, key, upload, after, max).unwrap(),
            ),
            complete::Request::File(c) => complete::Answer::File(self.file(c)),
            complete::Request::Name(c) => complete::Answer::Name(self.name(*c)),
        }
    }

    /// The plaintext of `file`, read from its rows and chunks and opened under its key.
    fn read_file(&self, f: u128, size: u64, wrapping: &WrappingKey) -> Vec<u8> {
        let header = file::header(&self.files, f).unwrap().unwrap();
        let wrapped = header.key.unwrap();
        let data = DataKey::unwrap(&wrapped.bytes, wrapping).unwrap();
        let segments = Segments::new(&data, f).unwrap();
        let sealed = self.read_sealed(f, size);
        let count = seal::segments(size);
        let mut plain = Vec::new();
        for (i, s) in sealed.chunks(SEALED as usize).enumerate() {
            let mut s = s.to_vec();
            segments
                .open(i as u64, i as u64 + 1 == count, &mut s)
                .unwrap();
            plain.extend(s);
        }
        plain
    }

    /// The bytes `file` stores, its segments as sealed, read from its rows and chunks: each
    /// block rebuilt from its data chunks alone and checked against its CRC-32C.
    fn read_sealed(&self, f: u128, size: u64) -> Vec<u8> {
        let mut sealed = Vec::new();
        for (_, extent) in file::extents(&self.files, f, 0, 10_000).unwrap() {
            let Target::Block(b) = extent.target else {
                panic!("a PUT's file names blocks");
            };
            let (h, places) = block::read(&self.blocks, b).unwrap().unwrap();
            let chunk = |i: usize| -> &[u8] { &self.volumes[&places[i].volume][&places[i].key] };
            let bytes = if h.data == 1 {
                chunk(0).to_vec()
            } else {
                let code = Code::new(h.data.into(), h.parity.into()).unwrap();
                let present: Vec<(usize, &[u8])> =
                    (0..usize::from(h.data)).map(|i| (i, chunk(i))).collect();
                code.decode(&present, h.length as usize).unwrap()
            };
            assert_eq!(bytes.len() as u64, extent.length);
            assert_eq!(mantle_crc::crc32c(&bytes), h.crc32c);
            let mut volumes: Vec<u128> = places.iter().map(|p| p.volume).collect();
            volumes.sort_unstable();
            volumes.dedup();
            assert_eq!(
                volumes.len(),
                places.len(),
                "two chunks of a block on one volume"
            );
            sealed.extend(bytes);
        }
        assert_eq!(sealed.len() as u64, seal::sealed_len(size).unwrap());
        sealed
    }

    /// The current version of `key`.
    fn current(&self, key: &str) -> Version {
        name::current(&self.names, BUCKET, key).unwrap().unwrap().1
    }
}

/// Block IDs counted from a start.
/// Held a handover at a time, `window` full blocks going down at once.
fn holding(window: usize) -> Holding {
    Holding {
        handover_ns: HANDOVER,
        window,
    }
}

struct Counter(u128);

impl Ids for Counter {
    fn block(&mut self) -> Result<u128, PutError> {
        self.0 += 1;
        Ok(self.0)
    }
}

/// The node's root key, which unwraps every file's key in these tests.
struct Root(WrappingKey);

impl Keyring for Root {
    fn unwrap(&self, key: &WrappedKey) -> Result<DataKey, GetError> {
        DataKey::unwrap(&key.bytes, &self.0).map_err(|_| GetError::Key)
    }
}

/// Reads plaintext `range` of an object of `size` bytes in `file` from `cell`, answering the
/// GET's requests first in, first out, or last in, first out; and the most blocks of
/// plaintext the GET held at once.
fn read(
    cell: &mut Cell,
    file: Option<u128>,
    size: u64,
    range: Range<u64>,
    wrapping: &WrappingKey,
    lifo: bool,
) -> Result<Vec<u8>, GetError> {
    read_in(cell, file, size, range, wrapping, lifo, 2)
}

/// Reads as `read` does, holding at most `window` blocks.
fn read_in(
    cell: &mut Cell,
    file: Option<u128>,
    size: u64,
    range: Range<u64>,
    wrapping: &WrappingKey,
    lifo: bool,
    window: usize,
) -> Result<Vec<u8>, GetError> {
    let root = Root(WrappingKey::new(wrapping.bytes()));
    let mut get = Get::new(file, size, range, Box::new(root), window)?;
    let mut out = Vec::new();
    let mut pending = VecDeque::new();
    loop {
        while let Some(bytes) = get.take() {
            out.extend_from_slice(&bytes);
        }
        if let Some(outcome) = get.outcome() {
            return outcome.clone().map(|()| out);
        }
        while let Some(request) = get.poll() {
            pending.push_back(request);
        }
        let next = if lifo {
            pending.pop_back()
        } else {
            pending.pop_front()
        };
        let (id, request) = next.expect("a GET waits on nothing");
        let answer = cell.serve_get(request);
        get.answer(id, answer)?;
    }
}

/// Puts an object of `bytes` in `scheme` as `file` and returns the cell and its wrapping key.
fn stored(scheme: Scheme, bytes: &[u8], file: u128, volumes: u128) -> (Cell, WrappingKey) {
    let mut cell = Cell::new(volumes);
    let wrapping = WrappingKey::generate().unwrap();
    let layout = Layout::new(scheme).unwrap();
    let len = bytes.len() as u64;
    let mut put = new_put(&cell, object("k"), len, layout, keys(file, &wrapping));
    run(
        &mut cell,
        &mut put,
        bytes,
        &FAST,
        &Expected::default(),
        |_, _| {},
    )
    .unwrap();
    (cell, wrapping)
}

fn keys(file: u128, wrapping: &WrappingKey) -> Keys {
    let data = DataKey::generate().unwrap();
    let bytes = data.wrap(wrapping).unwrap();
    Keys {
        file,
        data,
        wrapped: WrappedKey {
            by: Wrapper::Root(1),
            bytes,
        },
    }
}

/// Keys of an SSE-C file: its data key wrapped under the key the customer sends.
fn customer_keys(file: u128, customer: &WrappingKey) -> Keys {
    let data = DataKey::generate().unwrap();
    let bytes = data.wrap(customer).unwrap();
    Keys {
        file,
        data,
        wrapped: WrappedKey {
            by: Wrapper::Customer,
            bytes,
        },
    }
}

fn object(key: &str) -> Commit {
    Commit::Object(name::Put {
        bucket: BUCKET.into(),
        incarnation: 1,
        key: key.into(),
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
            owner: "o".into(),
            headers: Vec::new(),
            retention: None,
            legal_hold: None,
            listing: None,
        },
        default: None,
        id: 0,
        deadline_ns: 0,
    })
}

fn part(key: &str, upload: &str, number: u16) -> Commit {
    Commit::Part(PutPart {
        bucket: BUCKET.into(),
        incarnation: 1,
        key: key.into(),
        upload: upload.into(),
        number,
        part: Part {
            etag: String::new(),
            size: 0,
            checksum: None,
            file: 0,
            modified_ns: 0,
        },
        at_ns: 0,
        deadline_ns: 0,
    })
}

/// A body of `len` bytes that differ from one segment to the next.
fn body(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed) ^ (i >> 16) as u8)
        .collect()
}

fn md5(bytes: &[u8]) -> [u8; 16] {
    checksum::checksum(Algorithm::Md5, bytes)
        .unwrap()
        .bytes
        .try_into()
        .unwrap()
}

/// How a test drives a PUT: the body offered `piece` bytes at a time, `wait` passing before
/// each piece, the client's pace, and `step` before each answer, the cell's.
struct Drive {
    piece: usize,
    wait: u64,
    step: u64,
}

/// Runs `put` to its end against `cell`, a piece of the body and an answer a turn, as the
/// client and the cell go on at once, each request answered in the order it was sent;
/// `before` sees each request first, and may act on the cell.
fn run(
    cell: &mut Cell,
    put: &mut Put,
    bytes: &[u8],
    drive: &Drive,
    expected: &Expected,
    mut before: impl FnMut(&mut Cell, &Request),
) -> Result<Stored, PutError> {
    let mut at = 0;
    let mut ended = false;
    for _ in 0..1_000_000 {
        if let Some(outcome) = put.outcome() {
            return outcome.clone();
        }
        let mut went = false;
        if at < bytes.len() && put.wants_body() {
            cell.clock += drive.wait;
            put.tick(cell.clock)?;
            let end = (at + drive.piece).min(bytes.len());
            at += put.feed(&bytes[at..end])?;
            went = true;
        } else if at == bytes.len() && !ended {
            put.end(expected)?;
            ended = true;
            went = true;
        }
        if let Some((id, request)) = put.poll() {
            before(cell, &request);
            cell.clock += drive.step;
            put.tick(cell.clock)?;
            let answer = cell.serve(request);
            put.answer(id, answer)?;
            went = true;
        }
        if !went {
            if let Some(outcome) = put.outcome() {
                return outcome.clone();
            }
            // Nothing to send: time passes to the next renewal.
            let due = put.due_ns().expect("a PUT waits on nothing");
            cell.clock = cell.clock.max(due);
            put.tick(cell.clock)?;
        }
    }
    panic!("the PUT never ended");
}

fn new_put(cell: &Cell, commit: Commit, length: u64, layout: Layout, keys: Keys) -> Put {
    new_put_in(cell, commit, length, layout, keys, 1)
}

/// A PUT with `window` full blocks going down at once.
fn new_put_in(
    cell: &Cell,
    commit: Commit,
    length: u64,
    layout: Layout,
    keys: Keys,
    window: usize,
) -> Put {
    let body = Body {
        length,
        checksum: Some(Algorithm::Crc64Nvme),
        content_md5: true,
    };
    Put::new(
        commit,
        body,
        keys,
        layout,
        holding(window),
        Box::new(Counter(1_000)),
        cell.clock,
    )
    .unwrap()
}

/// A body longer than one request carries is refused when the PUT is made, before a chunk,
/// a block or a file is asked for; the longest one is taken (audit B08).
#[test]
fn a_body_past_one_request_is_refused_before_anything_is_asked() {
    let cell = Cell::new(3);
    let wrapping = WrappingKey::generate().unwrap();
    let make = |length| {
        Put::new(
            object("k"),
            Body {
                length,
                checksum: None,
                content_md5: false,
            },
            keys(1, &wrapping),
            Layout::new(Scheme::Copies(3)).unwrap(),
            holding(1),
            Box::new(Counter(1_000)),
            cell.clock,
        )
    };
    let max = mantle_s3::body::MAX_UPLOAD;
    assert!(matches!(
        make(max + 1),
        Err(PutError::EntityTooLarge { length, max: m }) if length == max + 1 && m == max
    ));
    let mut largest = make(max).unwrap();
    assert!(largest.wants_body());
    assert!(largest.poll().is_none());
}

/// A part numbered outside 1 to 10,000 is refused before anything is asked or written, and the
/// numbers at either end are taken (audit §7.1).
#[test]
fn a_part_numbered_outside_s3s_range_is_refused_before_anything_is_asked() {
    let cell = Cell::new(3);
    let wrapping = WrappingKey::generate().unwrap();
    let make = |number| {
        Put::new(
            part("k", "u", number),
            Body {
                length: 1,
                checksum: None,
                content_md5: false,
            },
            keys(1, &wrapping),
            Layout::new(Scheme::Copies(3)).unwrap(),
            holding(1),
            Box::new(Counter(1_000)),
            cell.clock,
        )
    };
    for number in [0, mantle_s3::body::MAX_PARTS + 1] {
        assert!(matches!(make(number), Err(PutError::InvalidPartNumber(n)) if n == number));
    }
    for number in [1, mantle_s3::body::MAX_PARTS] {
        assert!(make(number).is_ok());
    }
}

const FAST: Drive = Drive {
    piece: 100_000,
    wait: 0,
    step: 1,
};

/// A body of three blocks, stored as copies, lands whole: each block's copies on distinct
/// volumes, the file naming the blocks in order, the version naming the file with the body's
/// ETag and checksum, and the plaintext back from the chunks under the file's key.
#[test]
fn an_object_is_written_bottom_up_and_committed() {
    let mut cell = Cell::new(5);
    let wrapping = WrappingKey::generate().unwrap();
    let layout = Layout::new(Scheme::Copies(3)).unwrap();
    let len = (2 * layout.segments_per_block() * seal::SEGMENT as u64 + 12_345) as usize;
    let bytes = body(len, 7);
    let mut put = new_put(&cell, object("k"), len as u64, layout, keys(77, &wrapping));
    let sent = checksum::checksum(Algorithm::Crc64Nvme, &bytes).unwrap();
    let expected = Expected {
        md5: Some(md5(&bytes)),
        checksum: Some(sent.clone()),
    };
    let stored = run(&mut cell, &mut put, &bytes, &FAST, &expected, |_, _| {}).unwrap();
    assert_eq!(stored.etag, checksum::etag(&md5(&bytes)));
    assert_eq!(stored.checksum, Some(sent.clone()));
    let v = cell.current("k");
    assert_eq!(
        (v.file, v.size, v.etag.as_str()),
        (Some(77), len as u64, stored.etag.as_str())
    );
    let kept = v.checksum.unwrap();
    assert_eq!(
        Algorithm::from_code(kept.algorithm),
        Some(Algorithm::Crc64Nvme)
    );
    assert_eq!(kept.value, sent.bytes);
    assert_eq!(file::extents(&cell.files, 77, 0, 100).unwrap().len(), 3);
    assert!(cell.read_file(77, len as u64, &wrapping) == bytes);
}

/// An SSE-C object's ETag is the MD5 of what it stores, its segments as sealed, and not of its
/// plaintext, while the request's `Content-MD5` is still checked against the plaintext; an
/// SSE-S3 object's is the MD5 of its plaintext, as S3's is (encryption.md §4; audit B11).
#[test]
fn an_sse_c_objects_etag_is_the_md5_of_its_ciphertext() {
    let mut cell = Cell::new(5);
    let customer = WrappingKey::generate().unwrap();
    let layout = Layout::new(Scheme::Copies(3)).unwrap();
    let len = (layout.segments_per_block() * seal::SEGMENT as u64 + 12_345) as usize;
    let bytes = body(len, 11);
    let expected = Expected {
        md5: Some(md5(&bytes)),
        checksum: None,
    };
    let mut put = new_put(
        &cell,
        object("c"),
        len as u64,
        layout,
        customer_keys(91, &customer),
    );
    let stored = run(&mut cell, &mut put, &bytes, &FAST, &expected, |_, _| {}).unwrap();
    let sealed = cell.read_sealed(91, len as u64);
    assert_eq!(stored.etag, checksum::etag(&md5(&sealed)));
    assert_ne!(stored.etag, checksum::etag(&md5(&bytes)));
    assert_eq!(cell.current("c").etag, stored.etag);
    assert!(cell.read_file(91, len as u64, &customer) == bytes);

    // A Content-MD5 the plaintext does not match is refused, as under SSE-S3.
    let wrong = Expected {
        md5: Some(md5(b"other")),
        checksum: None,
    };
    let mut put = new_put(
        &cell,
        object("d"),
        len as u64,
        layout,
        customer_keys(92, &customer),
    );
    assert_eq!(
        run(&mut cell, &mut put, &bytes, &FAST, &wrong, |_, _| {}),
        Err(PutError::BadDigest)
    );
    // A Content-MD5 the PUT was not told of when it was made has not been taken, and is not
    // taken on trust.
    let mut put = Put::new(
        object("e"),
        Body {
            length: len as u64,
            checksum: None,
            content_md5: false,
        },
        customer_keys(93, &customer),
        layout,
        holding(1),
        Box::new(Counter(3_000)),
        cell.clock,
    )
    .unwrap();
    assert_eq!(
        run(&mut cell, &mut put, &bytes, &FAST, &expected, |_, _| {}),
        Err(PutError::UndeclaredDigest)
    );
}

/// An empty SSE-C part's file holds one empty segment, sealed, and its ETag is the MD5 of that
/// segment as stored.
#[test]
fn an_empty_sse_c_parts_etag_covers_its_sealed_segment() {
    let mut cell = Cell::new(3);
    let customer = WrappingKey::generate().unwrap();
    let layout = Layout::new(Scheme::Copies(3)).unwrap();
    let upload = cell.create_upload("m");
    let mut put = new_put(
        &cell,
        part("m", &upload, 1),
        0,
        layout,
        customer_keys(94, &customer),
    );
    let stored = run(
        &mut cell,
        &mut put,
        &[],
        &FAST,
        &Expected::default(),
        |_, _| {},
    )
    .unwrap();
    let sealed = cell.read_sealed(94, 0);
    assert_eq!(sealed.len(), seal::TAG);
    assert_eq!(stored.etag, checksum::etag(&md5(&sealed)));
}

/// A body coded RS(2,1) is kept in data chunks that hold the block in order and a parity
/// chunk; any two of the three rebuild it.
#[test]
fn a_coded_object_is_rebuilt_from_its_data_chunks() {
    let mut cell = Cell::new(4);
    let wrapping = WrappingKey::generate().unwrap();
    let layout = Layout::new(Scheme::Rs(Code::new(2, 1).unwrap())).unwrap();
    let bytes = body(1_000_000, 3);
    let mut put = new_put(&cell, object("k"), 1_000_000, layout, keys(5, &wrapping));
    run(
        &mut cell,
        &mut put,
        &bytes,
        &FAST,
        &Expected::default(),
        |_, _| {},
    )
    .unwrap();
    assert!(cell.read_file(5, 1_000_000, &wrapping) == bytes);
    let (_, e) = file::extents(&cell.files, 5, 0, 10).unwrap()[0];
    let Target::Block(b) = e.target else { panic!() };
    let (h, places) = block::read(&cell.blocks, b).unwrap().unwrap();
    assert_eq!((h.data, h.parity, places.len()), (2, 1, 3));
    let code = Code::new(2, 1).unwrap();
    let chunk = |i: usize| -> &[u8] { &cell.volumes[&places[i].volume][&places[i].key] };
    let rebuilt = code
        .decode(&[(1, chunk(1)), (2, chunk(2))], h.length as usize)
        .unwrap();
    assert_eq!(mantle_crc::crc32c(&rebuilt), h.crc32c);
}

/// A volume that refuses a chunk is passed over for the next one offered that holds none of
/// the block's chunks; with none left, the PUT fails, `503 SlowDown`, and commits nothing.
#[test]
fn a_refused_chunk_goes_to_another_volume() {
    let mut cell = Cell::new(5);
    let wrapping = WrappingKey::generate().unwrap();
    let layout = Layout::new(Scheme::Copies(3)).unwrap();
    cell.refusing = BTreeSet::from([2, 3]);
    let bytes = body(70_000, 1);
    let mut put = new_put(&cell, object("k"), 70_000, layout, keys(9, &wrapping));
    run(
        &mut cell,
        &mut put,
        &bytes,
        &FAST,
        &Expected::default(),
        |_, _| {},
    )
    .unwrap();
    assert!(cell.read_file(9, 70_000, &wrapping) == bytes);
    for v in [2, 3] {
        assert!(cell.volumes[&v].is_empty());
    }

    cell.refusing = BTreeSet::from([1, 2, 3]);
    let mut put = new_put(&cell, object("j"), 70_000, layout, keys(10, &wrapping));
    let failed = run(
        &mut cell,
        &mut put,
        &bytes,
        &FAST,
        &Expected::default(),
        |_, _| {},
    );
    assert!(matches!(failed, Err(PutError::Unplaced(_))), "{failed:?}");
    assert!(name::current(&cell.names, BUCKET, "j").unwrap().is_none());
    assert!(file::header(&cell.files, 10).unwrap().is_none());
}

/// A body that streams in over many handovers keeps its blocks: the PUT renews each block it
/// recorded as a quarter of the handover passes, and the file names them in time.
#[test]
fn a_slow_body_renews_its_blocks_until_its_file_names_them() {
    let mut cell = Cell::new(4);
    let wrapping = WrappingKey::generate().unwrap();
    let layout = Layout::new(Scheme::Copies(2)).unwrap();
    let len = (2 * layout.segments_per_block() * seal::SEGMENT as u64 + 1) as usize;
    let bytes = body(len, 9);
    let mut put = new_put(&cell, object("k"), len as u64, layout, keys(11, &wrapping));
    let slow = Drive {
        piece: 1 << 20,
        wait: HANDOVER / 2,
        step: HANDOVER / 20,
    };
    let start = cell.clock;
    run(
        &mut cell,
        &mut put,
        &bytes,
        &slow,
        &Expected::default(),
        |_, _| {},
    )
    .unwrap();
    assert!(
        cell.clock - start > 4 * HANDOVER,
        "the body came faster than a handover"
    );
    assert!(cell.renewals > 0);
    assert!(cell.read_file(11, len as u64, &wrapping) == bytes);
}

/// A block the sweep released while its body still streamed in fails the PUT at its next
/// renewal, and the file is never written.
#[test]
fn a_released_block_fails_the_put() {
    let mut cell = Cell::new(4);
    let wrapping = WrappingKey::generate().unwrap();
    let layout = Layout::new(Scheme::Copies(2)).unwrap();
    let len = (2 * layout.segments_per_block() * seal::SEGMENT as u64) as usize;
    let bytes = body(len, 2);
    let mut put = new_put(&cell, object("k"), len as u64, layout, keys(12, &wrapping));
    let slow = Drive {
        piece: 1 << 20,
        wait: HANDOVER / 2,
        step: HANDOVER / 20,
    };
    let mut released = None;
    let failed = run(
        &mut cell,
        &mut put,
        &bytes,
        &slow,
        &Expected::default(),
        |cell, r| {
            // The sweep releases the first block just before its first renewal.
            if let Request::Block(block::Command::Renew { block, .. }) = r
                && released.is_none()
            {
                let origin = block::origin(&cell.blocks, *block).unwrap().unwrap();
                let release = block::Command::Release {
                    block: *block,
                    deadline_ns: origin.deadline_ns,
                };
                assert_eq!(cell.block(release), block::Outcome::Released);
                released = Some(*block);
            }
        },
    );
    assert_eq!(failed, Err(PutError::Released(released.unwrap())));
    assert!(file::header(&cell.files, 12).unwrap().is_none());
}

/// An empty object is its version alone, with no file; an empty part has a file holding one
/// empty segment, sealed, as a part names a file.
#[test]
fn an_empty_object_has_no_file_and_an_empty_part_has_one() {
    let mut cell = Cell::new(3);
    let wrapping = WrappingKey::generate().unwrap();
    let layout = Layout::new(Scheme::Copies(3)).unwrap();
    let mut put = new_put(&cell, object("e"), 0, layout, keys(20, &wrapping));
    let stored = run(
        &mut cell,
        &mut put,
        &[],
        &FAST,
        &Expected::default(),
        |_, r| {
            assert!(matches!(r, Request::Name(_)), "{r:?}");
        },
    )
    .unwrap();
    assert_eq!(stored.etag, "d41d8cd98f00b204e9800998ecf8427e");
    let v = cell.current("e");
    assert_eq!((v.file, v.size), (None, 0));

    let upload = cell.create_upload("m");
    let mut put = new_put(&cell, part("m", &upload, 1), 0, layout, keys(21, &wrapping));
    let stored = run(
        &mut cell,
        &mut put,
        &[],
        &FAST,
        &Expected::default(),
        |_, _| {},
    )
    .unwrap();
    assert_eq!(stored.version, None);
    let parts = name::parts(&cell.names, BUCKET, "m", &upload, 0, 10).unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!((parts[0].1.file, parts[0].1.size), (21, 0));
    assert!(cell.read_file(21, 0, &wrapping).is_empty());
}

/// An empty object's write carries no file, so the PUT names it by the file ID it drew and
/// holds it to a handover from its time. Delivered again, as a gateway re-sends a write whose
/// session expired (docs/design/gateway.md §2), it is answered with the version the first
/// made, and makes no second one.
#[test]
fn an_empty_object_sent_again_makes_one_version() {
    let mut cell = Cell::new(3);
    let wrapping = WrappingKey::generate().unwrap();
    let layout = Layout::new(Scheme::Copies(3)).unwrap();
    let Commit::Object(mut versioned) = object("e") else {
        panic!("not an object")
    };
    versioned.versioning = Versioning::Enabled;
    let asked_at = cell.clock;
    let mut put = new_put(
        &cell,
        Commit::Object(versioned),
        0,
        layout,
        keys(20, &wrapping),
    );
    let mut sent = None;
    let stored = run(
        &mut cell,
        &mut put,
        &[],
        &FAST,
        &Expected::default(),
        |_, r| {
            if let Request::Name(command) = r {
                sent = Some(command.clone());
            }
        },
    )
    .unwrap();
    let Some(sent) = sent else {
        panic!("no Name write")
    };
    let name::Command::Put(p) = sent.as_ref() else {
        panic!("{sent:?}")
    };
    assert_eq!((p.id, p.version.file), (20, None));
    assert!(p.deadline_ns >= asked_at + HANDOVER && p.deadline_ns <= cell.clock + HANDOVER);
    let again = cell.name(*sent.clone());
    assert_eq!(
        again,
        name::Outcome::Put {
            version: stored.version.clone().unwrap()
        }
    );
    let (order, _) = name::current(&cell.names, BUCKET, "e").unwrap().unwrap();
    assert_eq!(
        Some(mantle_meta::key::version_id(order)),
        stored.version,
        "the copy made a version of its own"
    );
}

/// The body is held to its declared length and to the digests the request sent; a PUT that
/// failed takes nothing more.
#[test]
fn the_body_is_held_to_its_length_and_digests() {
    let mut cell = Cell::new(3);
    let wrapping = WrappingKey::generate().unwrap();
    let layout = Layout::new(Scheme::Copies(3)).unwrap();
    let bytes = body(1_000, 4);

    let mut put = new_put(&cell, object("k"), 999, layout, keys(30, &wrapping));
    assert_eq!(put.feed(&bytes), Err(PutError::TooLong));
    assert_eq!(put.feed(&bytes[..1]), Err(PutError::Over));

    let mut put = new_put(&cell, object("k"), 1_001, layout, keys(31, &wrapping));
    assert_eq!(put.feed(&bytes), Ok(1_000));
    assert_eq!(put.end(&Expected::default()), Err(PutError::IncompleteBody));

    let mut wrong = md5(&bytes);
    wrong[0] ^= 1;
    let bad = Expected {
        md5: Some(wrong),
        checksum: None,
    };
    let mut put = new_put(&cell, object("k"), 1_000, layout, keys(32, &wrapping));
    let failed = run(&mut cell, &mut put, &bytes, &FAST, &bad, |_, _| {});
    assert_eq!(failed, Err(PutError::BadDigest));

    let mut other = checksum::checksum(Algorithm::Crc64Nvme, &bytes).unwrap();
    other.bytes[0] ^= 1;
    let bad = Expected {
        md5: None,
        checksum: Some(other),
    };
    let mut put = new_put(&cell, object("k"), 1_000, layout, keys(33, &wrapping));
    let failed = run(&mut cell, &mut put, &bytes, &FAST, &bad, |_, _| {});
    assert_eq!(failed, Err(PutError::BadChecksum));
    assert!(name::current(&cell.names, BUCKET, "k").unwrap().is_none());
}

/// A PUT holds at most two blocks: the body waits while one block is being written and the
/// next is full, and goes on once the first is recorded.
#[test]
fn the_body_waits_while_two_blocks_are_held() {
    let cell = Cell::new(3);
    let wrapping = WrappingKey::generate().unwrap();
    let layout = Layout::new(Scheme::Copies(3)).unwrap();
    let block = (layout.segments_per_block() * seal::SEGMENT as u64) as usize;
    let bytes = body(3 * block, 5);
    let mut put = new_put(
        &cell,
        object("k"),
        bytes.len() as u64,
        layout,
        keys(40, &wrapping),
    );
    assert_eq!(put.feed(&bytes), Ok(2 * block));
    assert!(!put.wants_body());
    assert_eq!(put.feed(&bytes[2 * block..]), Ok(0));
}

/// A scheme and a body: any scheme with a body of up to a few segments, or copies with a body
/// of two blocks, 127 segments each, much of the second still to come once the first is
/// recorded.
fn case() -> impl Strategy<Value = (Scheme, usize)> {
    let scheme = prop_oneof![
        (1usize..4).prop_map(Scheme::Copies),
        Just(Scheme::Rs(Code::new(2, 1).unwrap())),
        Just(Scheme::Rs(Code::new(4, 2).unwrap())),
    ];
    let small = prop_oneof![0usize..200_000, Just(65_536), Just(131_072)];
    prop_oneof![
        4 => (scheme, small),
        1 => ((1usize..4).prop_map(Scheme::Copies), 10_800_000usize..16_000_000),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Whatever the body's length, however slowly the client sends it, and whichever volumes
    /// refuse, a PUT to a cell that answers well within a handover commits a version whose
    /// bytes read back whole, unless too few volumes take its chunks, when it commits nothing.
    #[test]
    fn a_put_commits_its_body_whole_or_nothing(
        (scheme, len) in case(),
        pieces in 1usize..64,
        wait in 0..=HANDOVER,
        step in 0..=HANDOVER / 20,
        refusing in prop::collection::btree_set(1u128..7, 0..4),
        seed in any::<u8>(),
        window in 1usize..=4,
    ) {
        let mut cell = Cell::new(6);
        cell.refusing = refusing;
        let wrapping = WrappingKey::generate().unwrap();
        let layout = Layout::new(scheme).unwrap();
        let bytes = body(len, seed);
        let mut put =
            new_put_in(&cell, object("k"), len as u64, layout, keys(50, &wrapping), window);
        let drive = Drive { piece: len.div_ceil(pieces).max(1), wait, step };
        let usable = 6 - cell.refusing.len();
        match run(&mut cell, &mut put, &bytes, &drive, &Expected::default(), |_, _| {}) {
            Ok(stored) => {
                let v = cell.current("k");
                prop_assert_eq!(v.size, len as u64);
                prop_assert_eq!(v.etag, stored.etag);
                if len > 0 {
                    prop_assert!(cell.read_file(50, len as u64, &wrapping) == bytes);
                }
                prop_assert!(len == 0 || usable >= scheme.width());
            }
            Err(e) => {
                prop_assert!(matches!(e, PutError::Unplaced(_)), "{:?}", e);
                prop_assert!(usable < scheme.width());
                prop_assert!(name::current(&cell.names, BUCKET, "k").unwrap().is_none());
            }
        }
    }

    /// A cell that answers slower than a handover allows may fail a PUT, and the PUT then
    /// commits nothing: a version is only ever one whose bytes read back whole.
    #[test]
    fn a_put_to_a_slow_cell_commits_whole_or_nothing(
        len in prop_oneof![0usize..100_000, 8_323_072usize..8_400_000],
        pieces in 1usize..16,
        wait in 0..=HANDOVER,
        step in 0..=2 * HANDOVER,
        seed in any::<u8>(),
        window in 1usize..=4,
    ) {
        let mut cell = Cell::new(3);
        let wrapping = WrappingKey::generate().unwrap();
        let layout = Layout::new(Scheme::Copies(2)).unwrap();
        let bytes = body(len, seed);
        let mut put =
            new_put_in(&cell, object("k"), len as u64, layout, keys(60, &wrapping), window);
        let drive = Drive { piece: len.div_ceil(pieces).max(1), wait, step };
        match run(&mut cell, &mut put, &bytes, &drive, &Expected::default(), |_, _| {}) {
            Ok(_) if len > 0 => {
                prop_assert!(cell.read_file(60, len as u64, &wrapping) == bytes);
            }
            Ok(_) => {}
            Err(e) => {
                prop_assert!(
                    matches!(
                        e,
                        PutError::File(file::Outcome::Expired)
                            | PutError::Name(name::Outcome::Expired)
                            | PutError::Released(_)
                    ),
                    "{:?}",
                    e
                );
                prop_assert!(name::current(&cell.names, BUCKET, "k").unwrap().is_none());
            }
        }
    }
}

/// An object reads back whole, and in ranges that start and end anywhere: on a segment's
/// boundary or across one, across a block's, at the first byte and the last, under copies and
/// codes, with the reads answered in either order.
#[test]
fn an_object_reads_back_in_any_range() {
    let segment = seal::SEGMENT as u64;
    for (scheme, len) in [
        (Scheme::Copies(1), 9_000_000usize),
        (Scheme::Copies(3), 300_000),
        (Scheme::Rs(Code::new(2, 1).unwrap()), 17_000_000),
    ] {
        let bytes = body(len, 7);
        let (mut cell, wrapping) = stored(scheme, &bytes, 70, 4);
        let len = len as u64;
        let layout = Layout::new(scheme).unwrap();
        let block = layout.segments_per_block() * segment;
        let ranges = [
            0..len,
            0..1,
            len - 1..len,
            segment - 1..segment + 1,
            segment..2 * segment,
            (block - 10).min(len - 20)..(block + 10).min(len),
            len / 3..len / 2,
        ];
        for range in ranges {
            for lifo in [false, true] {
                let got = read(&mut cell, Some(70), len, range.clone(), &wrapping, lifo).unwrap();
                assert!(
                    got == bytes[range.start as usize..range.end as usize],
                    "{scheme:?} {range:?}"
                );
            }
        }
    }
}

/// A range that holds no byte, of any object, and an empty object's, need no request; a range
/// past the object's end is refused.
#[test]
fn an_empty_range_asks_nothing() {
    let wrapping = WrappingKey::generate().unwrap();
    let mut cell = Cell::new(1);
    assert_eq!(
        read(&mut cell, None, 0, 0..0, &wrapping, false),
        Ok(Vec::new())
    );
    assert_eq!(
        read(&mut cell, Some(9), 100, 50..50, &wrapping, false),
        Ok(Vec::new())
    );
    assert!(cell.gets.is_empty());
    assert_eq!(
        read(&mut cell, Some(9), 100, 50..101, &wrapping, false),
        Err(GetError::Range)
    );
}

/// A chunk that cannot be read is read from another copy, or its block decoded from any data
/// chunks; with more lost than the scheme tolerates, the GET fails and says which block.
#[test]
fn a_chunk_that_cannot_be_read_is_read_elsewhere() {
    let len = 300_000usize;
    for scheme in [Scheme::Copies(3), Scheme::Rs(Code::new(2, 1).unwrap())] {
        let bytes = body(len, 9);
        let (mut cell, wrapping) = stored(scheme, &bytes, 80, 3);
        let (_, e) = file::extents(&cell.files, 80, 0, 1).unwrap()[0];
        let Target::Block(b) = e.target else { panic!() };
        let (_, places) = block::read(&cell.blocks, b).unwrap().unwrap();
        cell.unreadable.insert(places[0].volume);
        let got = read(
            &mut cell,
            Some(80),
            len as u64,
            1000..250_000,
            &wrapping,
            false,
        )
        .unwrap();
        assert!(got == bytes[1000..250_000], "{scheme:?}");
        let tolerated = scheme.width() - scheme.needed();
        for p in &places[1..=tolerated] {
            cell.unreadable.insert(p.volume);
        }
        assert_eq!(
            read(&mut cell, Some(80), len as u64, 0..10, &wrapping, false),
            Err(GetError::Unreadable(b)),
            "{scheme:?}"
        );
    }
}

/// Bytes a volume hands back that are not the block's fail the GET: a segment's tag when read
/// directly, the block's CRC-32C when decoded from its chunks.
#[test]
fn bytes_that_are_not_the_blocks_fail_the_get() {
    let len = 300_000usize;
    let bytes = body(len, 11);
    let (mut cell, wrapping) = stored(Scheme::Rs(Code::new(2, 1).unwrap()), &bytes, 90, 3);
    let (_, e) = file::extents(&cell.files, 90, 0, 1).unwrap()[0];
    let Target::Block(b) = e.target else { panic!() };
    let (_, places) = block::read(&cell.blocks, b).unwrap().unwrap();
    // The second data chunk's first byte, as its volume keeps it, flipped.
    let tamper = |cell: &mut Cell| {
        let chunk = cell.volumes.get_mut(&places[1].volume).unwrap();
        let mut bad = chunk[&places[1].key].to_vec();
        bad[0] ^= 1;
        chunk.insert(places[1].key, Bytes::from(bad));
    };
    tamper(&mut cell);
    let span = |cell: &mut Cell| read(cell, Some(90), len as u64, 0..len as u64, &wrapping, false);
    assert_eq!(span(&mut cell), Err(GetError::Corrupt(b)));
    // With the first data chunk unreadable, the block is decoded from the tampered one.
    cell.unreadable.insert(places[0].volume);
    assert_eq!(span(&mut cell), Err(GetError::Corrupt(b)));
}

/// An object of parts reads by its parts' plaintext: a part of 5 MiB and one byte and a last of
/// one byte, the case where the stored lengths, one tag longer each, would misplace the last
/// byte (audit §16.7). A read of the last byte asks only for the last part's block.
#[test]
fn an_object_of_parts_reads_by_its_parts_plaintext() {
    let mut cell = Cell::new(3);
    let wrapping = WrappingKey::generate().unwrap();
    let upload = cell.create_upload("k");
    let layout = Layout::new(Scheme::Copies(2)).unwrap();
    let first = body((5 << 20) + 1, 13);
    let last = body(1, 17);
    for (number, (file, bytes)) in [(100u128, &first), (101, &last)].into_iter().enumerate() {
        let len = bytes.len() as u64;
        let commit = part("k", &upload, number as u16 + 1);
        let body = Body {
            length: len,
            checksum: None,
            content_md5: false,
        };
        // Each part draws its blocks' IDs from a range of its own.
        let ids = Box::new(Counter(file * 1_000));
        let mut put = Put::new(
            commit,
            body,
            keys(file, &wrapping),
            layout,
            holding(1),
            ids,
            cell.clock,
        )
        .unwrap();
        run(
            &mut cell,
            &mut put,
            bytes,
            &FAST,
            &Expected::default(),
            |_, _| {},
        )
        .unwrap();
    }
    let size = (first.len() + last.len()) as u64;
    let write = file::Command::Write {
        file: 102,
        extents: vec![
            Extent {
                length: first.len() as u64,
                target: Target::File(100),
            },
            Extent {
                length: last.len() as u64,
                target: Target::File(101),
            },
        ],
        referrer: Referrer {
            bucket: BUCKET.into(),
            incarnation: 1,
            key: "k".into(),
        },
        key: None,
        handover_ns: HANDOVER,
        blocks_deadline_ns: u64::MAX,
        at_ns: 0,
    };
    assert!(matches!(cell.file(write), file::Outcome::Written { .. }));
    let whole: Vec<u8> = first.iter().chain(&last).copied().collect();
    for range in [
        0..size,
        size - 1..size,
        size - 3..size,
        5 << 20..(5 << 20) + 1,
    ] {
        let got = read(&mut cell, Some(102), size, range.clone(), &wrapping, false).unwrap();
        assert!(
            got == whole[range.start as usize..range.end as usize],
            "{range:?}"
        );
    }
    cell.gets.clear();
    read(&mut cell, Some(102), size, size - 1..size, &wrapping, false).unwrap();
    assert_eq!(cell.gets["block"], 1);
}

/// A GET holds at most two blocks' plaintext: while the caller takes none, it asks for no more
/// once two are ready, and asks again once one is taken.
#[test]
fn a_get_holds_two_blocks_at_most() {
    let len = 3 * 127 * seal::SEGMENT;
    let bytes = body(len, 19);
    let (mut cell, wrapping) = stored(Scheme::Copies(1), &bytes, 110, 2);
    let root = Root(WrappingKey::new(wrapping.bytes()));
    let mut get = Get::new(Some(110), len as u64, 0..len as u64, Box::new(root), 2).unwrap();
    let mut blocks = 0;
    let serve = |get: &mut Get, cell: &mut Cell| {
        let mut asked = false;
        while let Some((id, request)) = get.poll() {
            let answer = cell.serve_get(request);
            get.answer(id, answer).unwrap();
            asked = true;
        }
        asked
    };
    while serve(&mut get, &mut cell) {}
    assert_eq!(cell.gets["block"], 2, "two blocks read before any is taken");
    let mut out = Vec::new();
    while let Some(b) = get.take() {
        out.extend_from_slice(&b);
        blocks += 1;
        while serve(&mut get, &mut cell) {}
    }
    assert_eq!(blocks, 3);
    assert!(out == bytes);
    assert_eq!(get.outcome(), Some(&Ok(())));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Whatever the object's length and scheme, any range of it reads back as its bytes, with
    /// the reads answered in either order.
    #[test]
    fn any_range_reads_back(
        (scheme, len) in case(),
        a in any::<prop::sample::Index>(),
        b in any::<prop::sample::Index>(),
        lifo in any::<bool>(),
        seed in any::<u8>(),
        window in 1usize..=4,
    ) {
        let bytes = body(len, seed);
        let (mut cell, wrapping) = stored(scheme, &bytes, 120, 6);
        let (x, y) = (a.index(len + 1), b.index(len + 1));
        let range = x.min(y) as u64..x.max(y) as u64;
        let file = if len > 0 { Some(120) } else { None };
        let got =
            read_in(&mut cell, file, len as u64, range.clone(), &wrapping, lifo, window).unwrap();
        prop_assert!(got == bytes[range.start as usize..range.end as usize]);
    }
}

/// Uploads `bodies` as parts 1, 2, … of `key`'s upload `upload`, each in its own file from
/// `first_file` on, and returns each part's ETag.
fn upload_parts(
    cell: &mut Cell,
    key: &str,
    upload: &str,
    bodies: &[Vec<u8>],
    first_file: u128,
    checksum: Option<Algorithm>,
    wrapping: &WrappingKey,
) -> Vec<String> {
    let layout = Layout::new(Scheme::Copies(2)).unwrap();
    let mut etags = Vec::new();
    for (i, bytes) in bodies.iter().enumerate() {
        let file = first_file + i as u128;
        let body = Body {
            length: bytes.len() as u64,
            checksum,
            content_md5: false,
        };
        let mut put = Put::new(
            part(key, upload, i as u16 + 1),
            body,
            keys(file, wrapping),
            layout,
            holding(1),
            Box::new(Counter(file * 1_000)),
            cell.clock,
        )
        .unwrap();
        let stored = run(
            cell,
            &mut put,
            bytes,
            &FAST,
            &Expected::default(),
            |_, _| {},
        )
        .unwrap();
        etags.push(stored.etag);
    }
    etags
}

/// Completes `key`'s upload `upload` with the parts `listed` into file `file`.
fn complete_upload(
    cell: &mut Cell,
    key: &str,
    upload: &str,
    listed: Vec<(u16, String)>,
    file: u128,
) -> Result<Completed, CompleteError> {
    let template = name::Complete {
        bucket: BUCKET.into(),
        incarnation: 1,
        key: key.into(),
        upload: upload.into(),
        versioning: Versioning::Unversioned,
        preconditions: Preconditions::default(),
        at_ns: 0,
        parts: Vec::new(),
        etag: String::new(),
        size: 0,
        checksum: None,
        file: None,
        default: None,
        id: 0,
        deadline_ns: 0,
        listing: [0; mantle_meta::record::LISTING],
    };
    let mut completion = Completion::new(template, listed, file, HANDOVER, cell.clock)?;
    loop {
        if let Some(outcome) = completion.outcome() {
            return outcome.clone();
        }
        let (id, request) = completion.poll().expect("a completion waits on nothing");
        let answer = cell.serve_complete(key, upload, request);
        completion.answer(id, answer)?;
    }
}

/// An upload of three parts, each with its CRC-64/NVME, completes into an object whose size,
/// ETag and checksum are its parts' combined, as S3 combines them, and whose bytes read back
/// as the parts' in order, whole and across a part's end.
#[test]
fn a_completed_upload_is_its_parts() {
    let mut cell = Cell::new(3);
    let wrapping = WrappingKey::generate().unwrap();
    let full = Some((Algorithm::Crc64Nvme.code(), true));
    let upload = cell.create_upload_with("k", full);
    let bodies = [body((5 << 20) + 7, 1), body(5 << 20, 2), body(100, 3)];
    let etags = upload_parts(
        &mut cell,
        "k",
        &upload,
        &bodies,
        200,
        Some(Algorithm::Crc64Nvme),
        &wrapping,
    );
    let listed: Vec<(u16, String)> = (1..=3).zip(etags.iter().cloned()).collect();
    let done = complete_upload(&mut cell, "k", &upload, listed.clone(), 300).unwrap();
    let whole: Vec<u8> = bodies.concat();
    let digests: Vec<[u8; 16]> = bodies.iter().map(|b| md5(b)).collect();
    assert_eq!(done.size, whole.len() as u64);
    assert_eq!(done.etag, checksum::multipart_etag(&digests).unwrap());
    let crc = checksum::checksum(Algorithm::Crc64Nvme, &whole).unwrap();
    assert_eq!(
        done.checksum.as_ref().map(|c| c.value.clone()),
        Some(crc.bytes)
    );
    let v = cell.current("k");
    assert_eq!(
        (v.size, v.etag.clone(), v.file),
        (done.size, done.etag.clone(), Some(300))
    );
    let size = whole.len() as u64;
    let first = bodies[0].len() as u64;
    for range in [0..size, first - 5..first + 5, size - 100..size] {
        let got = read(&mut cell, v.file, size, range.clone(), &wrapping, false).unwrap();
        assert!(
            got == whole[range.start as usize..range.end as usize],
            "{range:?}"
        );
    }
    // A retry once the upload is gone finds the version it made, and reports the object as
    // the first completion did.
    let again = complete_upload(&mut cell, "k", &upload, listed, 301).unwrap();
    assert_eq!(again, done);
}

/// A retry once the upload is gone is matched as strictly as the first completion: the parts
/// it lists, their numbers and their ETags as sent, must be those the object was made from,
/// or the upload is no more (`NoSuchUpload`, 05 §4.4). Before, a retry was matched by the
/// multipart ETag alone, which names neither the parts' numbers nor their ETags' case.
#[test]
fn a_retried_completion_is_matched_as_strictly_as_the_first() {
    let mut cell = Cell::new(3);
    let wrapping = WrappingKey::generate().unwrap();
    let upload = cell.create_upload_with("k", Some((Algorithm::Crc64Nvme.code(), true)));
    let bodies = [body(5 << 20, 1), body(100, 3)];
    let etags = upload_parts(
        &mut cell,
        "k",
        &upload,
        &bodies,
        200,
        Some(Algorithm::Crc64Nvme),
        &wrapping,
    );
    let listed: Vec<(u16, String)> = (1..=2).zip(etags.iter().cloned()).collect();
    let done = complete_upload(&mut cell, "k", &upload, listed.clone(), 300).unwrap();
    assert_eq!(done.size, (5 << 20) + 100);
    assert!(done.checksum.is_some());
    // The first completion refuses these as parts the upload does not have.
    for other in [
        vec![(3, etags[0].to_uppercase()), (7, etags[1].clone())],
        vec![(1, etags[0].to_uppercase()), (2, etags[1].clone())],
        vec![(1, etags[0].clone()), (3, etags[1].clone())],
        vec![(2, etags[0].clone()), (3, etags[1].clone())],
    ] {
        assert_eq!(
            complete_upload(&mut cell, "k", &upload, other.clone(), 301),
            Err(CompleteError::NoSuchUpload),
            "{other:?}"
        );
    }
    assert_eq!(
        complete_upload(&mut cell, "k", &upload, listed, 302),
        Ok(done)
    );
}

/// A composite checksum is the hash of the parts' values, with the number of parts.
#[test]
fn a_composite_checksum_hashes_the_parts_values() {
    let mut cell = Cell::new(3);
    let wrapping = WrappingKey::generate().unwrap();
    let composite = Some((Algorithm::Sha256.code(), false));
    let upload = cell.create_upload_with("k", composite);
    let bodies = [body(5 << 20, 4), body(10, 5)];
    let etags = upload_parts(
        &mut cell,
        "k",
        &upload,
        &bodies,
        400,
        Some(Algorithm::Sha256),
        &wrapping,
    );
    let done = complete_upload(&mut cell, "k", &upload, (1..=2).zip(etags).collect(), 500).unwrap();
    let values: Vec<u8> = bodies
        .iter()
        .flat_map(|b| checksum::checksum(Algorithm::Sha256, b).unwrap().bytes)
        .collect();
    let expected = checksum::checksum(Algorithm::Sha256, &values).unwrap();
    let got = done.checksum.unwrap();
    assert_eq!((got.parts, got.value), (2, expected.bytes));
}

/// What a completion refuses: parts out of order before anything is asked, a part never
/// uploaded or with another ETag, a part other than the last under 5 MiB; parts not listed
/// are left out; and an empty last part adds nothing to the object's file.
#[test]
fn a_completion_is_held_to_its_parts() {
    let mut cell = Cell::new(3);
    let wrapping = WrappingKey::generate().unwrap();
    let upload = cell.create_upload("k");
    let bodies = [
        body(5 << 20, 6),
        body(1000, 7),
        body(5 << 20, 8),
        Vec::new(),
    ];
    let etags = upload_parts(&mut cell, "k", &upload, &bodies, 600, None, &wrapping);
    let listed = |numbers: &[u16]| -> Vec<(u16, String)> {
        numbers
            .iter()
            .map(|&n| (n, etags[usize::from(n) - 1].clone()))
            .collect()
    };
    assert!(matches!(
        Completion::new(
            name::Complete {
                parts: Vec::new(),
                ..template_complete("k", &upload)
            },
            listed(&[2, 1]),
            700,
            HANDOVER,
            cell.clock,
        ),
        Err(CompleteError::InvalidPartOrder)
    ));
    // An upload ID the Name range never made, and part numbers S3 never gives, are refused
    // before anything is asked: what goes on is within the largest entry a range takes.
    for (upload, parts) in [
        ("x".repeat(5000), listed(&[1])),
        (upload.clone(), vec![(0, etags[0].clone())]),
        (upload.clone(), vec![(10_001, etags[0].clone())]),
    ] {
        let refused = Completion::new(
            name::Complete {
                upload: upload.clone(),
                ..template_complete("k", &upload)
            },
            parts,
            700,
            HANDOVER,
            cell.clock,
        );
        assert!(
            matches!(
                refused,
                Err(CompleteError::NoSuchUpload | CompleteError::InvalidPart)
            ),
            "{:?}",
            refused.err()
        );
    }
    let mut wrong = listed(&[1, 3]);
    wrong[1].1 = "0".repeat(32);
    assert_eq!(
        complete_upload(&mut cell, "k", &upload, wrong, 700),
        Err(CompleteError::InvalidPart)
    );
    assert_eq!(
        complete_upload(
            &mut cell,
            "k",
            &upload,
            vec![(1, etags[0].clone()), (5, "0".repeat(32))],
            701
        ),
        Err(CompleteError::InvalidPart)
    );
    assert_eq!(
        complete_upload(&mut cell, "k", &upload, listed(&[2, 3]), 702),
        Err(CompleteError::EntityTooSmall)
    );
    // Parts 1 and 3, part 2 left out, and the empty part 4 last.
    let done = complete_upload(&mut cell, "k", &upload, listed(&[1, 3, 4]), 703).unwrap();
    assert_eq!(done.size, 10 << 20);
    let extents = file::extents(&cell.files, 703, 0, 10).unwrap();
    assert_eq!(extents.len(), 2);
    let whole: Vec<u8> = [bodies[0].clone(), bodies[2].clone()].concat();
    let got = read(
        &mut cell,
        Some(703),
        done.size,
        0..done.size,
        &wrapping,
        false,
    )
    .unwrap();
    assert!(got == whole);
}

fn template_complete(key: &str, upload: &str) -> name::Complete {
    name::Complete {
        bucket: BUCKET.into(),
        incarnation: 1,
        key: key.into(),
        upload: upload.into(),
        versioning: Versioning::Unversioned,
        preconditions: Preconditions::default(),
        at_ns: 0,
        parts: Vec::new(),
        etag: String::new(),
        size: 0,
        checksum: None,
        file: None,
        default: None,
        id: 0,
        deadline_ns: 0,
        listing: [0; mantle_meta::record::LISTING],
    }
}

/// An upload with composite checksums completes only from parts numbered 1, 2, 3, … (05
/// §3.3), and one whose row names a combination its algorithm lacks is refused.
#[test]
fn composite_checksums_need_consecutive_parts_and_a_combining_algorithm() {
    let mut cell = Cell::new(3);
    let wrapping = WrappingKey::generate().unwrap();
    let composite = Some((Algorithm::Sha256.code(), false));
    let upload = cell.create_upload_with("k", composite);
    let bodies = [body(5 << 20, 21), body(5 << 20, 22), body(10, 23)];
    let etags = upload_parts(
        &mut cell,
        "k",
        &upload,
        &bodies,
        800,
        Some(Algorithm::Sha256),
        &wrapping,
    );
    let gap = vec![(1, etags[0].clone()), (3, etags[2].clone())];
    assert_eq!(
        complete_upload(&mut cell, "k", &upload, gap, 900),
        Err(CompleteError::NotConsecutive)
    );
    let from_two = vec![(2, etags[1].clone()), (3, etags[2].clone())];
    assert_eq!(
        complete_upload(&mut cell, "k", &upload, from_two, 901),
        Err(CompleteError::NotConsecutive)
    );
    assert!(complete_upload(&mut cell, "k", &upload, (1..=3).zip(etags).collect(), 902).is_ok());
    // CRC-64/NVME has no composite form.
    let wrong = cell.create_upload_with("j", Some((Algorithm::Crc64Nvme.code(), false)));
    let etags = upload_parts(
        &mut cell,
        "j",
        &wrong,
        &[body(10, 24)],
        910,
        Some(Algorithm::Crc64Nvme),
        &wrapping,
    );
    assert_eq!(
        complete_upload(&mut cell, "j", &wrong, vec![(1, etags[0].clone())], 920),
        Err(CompleteError::ChecksumType)
    );
}

/// A PUT sends down at most as many full blocks at once as its window admits, and a wider
/// window overlaps their writes: a body of four blocks, arriving at once, takes fewer round
/// trips with every block in flight than with one, and reads back the same (audit §16.3).
#[test]
fn blocks_go_down_together_as_the_window_admits() {
    let len = 4 * 127 * seal::SEGMENT;
    let bytes = body(len, 29);
    let wrapping = WrappingKey::generate().unwrap();
    let layout = Layout::new(Scheme::Copies(1)).unwrap();
    let mut rounds = Vec::new();
    for window in [1usize, 2, 4] {
        let mut cell = Cell::new(2);
        let mut put = new_put_in(
            &cell,
            object("k"),
            len as u64,
            layout,
            keys(130, &wrapping),
            window,
        );
        let (mut at, mut ended, mut n) = (0usize, false, 0u64);
        loop {
            while at < len && put.wants_body() {
                at += put.feed(&bytes[at..]).unwrap();
            }
            if at == len && !ended {
                put.end(&Expected::default()).unwrap();
                ended = true;
            }
            let mut asked = Vec::new();
            while let Some(request) = put.poll() {
                asked.push(request);
            }
            if asked.is_empty() {
                assert!(matches!(put.outcome(), Some(Ok(_))), "{:?}", put.outcome());
                break;
            }
            // The blocks whose chunks are out at once.
            let going: BTreeSet<u128> = asked
                .iter()
                .filter_map(|(_, r)| match r {
                    Request::Chunk { key, .. } => Some(key.block),
                    _ => None,
                })
                .collect();
            assert!(
                going.len() <= window,
                "{} blocks out with a window of {window}",
                going.len()
            );
            n += 1;
            for (id, request) in asked {
                let answer = cell.serve(request);
                put.answer(id, answer).unwrap();
            }
        }
        assert!(
            cell.read_file(130, len as u64, &wrapping) == bytes,
            "window {window}"
        );
        rounds.push(n);
    }
    assert!(rounds[0] > rounds[1] && rounds[1] > rounds[2], "{rounds:?}");
}

/// A GET holds as many blocks as its window: with the caller taking nothing, it reads that
/// many and no more; and a wider window reads an object of four blocks in fewer round trips,
/// its blocks read out of order given out in order (audit §16.3).
#[test]
fn a_get_reads_as_many_blocks_at_once_as_its_window() {
    let len = 4 * 127 * seal::SEGMENT;
    let bytes = body(len, 31);
    let (mut cell, wrapping) = stored(Scheme::Copies(1), &bytes, 140, 2);
    let mut rounds = Vec::new();
    for window in [1usize, 2, 4] {
        let root = Root(WrappingKey::new(wrapping.bytes()));
        let mut get =
            Get::new(Some(140), len as u64, 0..len as u64, Box::new(root), window).unwrap();
        cell.gets.clear();
        let (mut n, mut out) = (0u64, Vec::new());
        let mut first = true;
        loop {
            let mut asked = Vec::new();
            while let Some(r) = get.poll() {
                asked.push(r);
            }
            if asked.is_empty() {
                if first {
                    // Nothing taken yet: as many blocks read as the window holds.
                    assert_eq!(cell.gets["block"], window.min(4), "window {window}");
                    first = false;
                }
                match get.take() {
                    Some(b) => out.extend_from_slice(&b),
                    None => break,
                }
                continue;
            }
            n += 1;
            // Chunk reads answered latest block first: blocks finish out of order.
            asked.sort_by_key(|(_, r)| match r {
                get::Request::Chunk { key, .. } => std::cmp::Reverse(key.block),
                _ => std::cmp::Reverse(0),
            });
            for (id, request) in asked {
                let answer = cell.serve_get(request);
                get.answer(id, answer).unwrap();
            }
        }
        while let Some(b) = get.take() {
            out.extend_from_slice(&b);
        }
        assert_eq!(get.outcome(), Some(&Ok(())));
        assert!(out == bytes, "window {window}");
        rounds.push(n);
    }
    assert!(rounds[0] > rounds[1] && rounds[1] > rounds[2], "{rounds:?}");
}
