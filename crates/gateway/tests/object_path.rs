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

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use mantle_chunk::ChunkKey;
use mantle_ec::Code;
use mantle_ec::durability::Scheme;
use mantle_gateway::layout::{Layout, SEALED};
use mantle_gateway::put::{
    Answer, Body, Commit, Expected, Ids, Keys, Put, PutError, Request, Stored,
};
use mantle_meta::engine::{Engine, Model};
use mantle_meta::name::{self, CreateUpload, GateChange, Preconditions, PutPart};
use mantle_meta::record::{
    GateState, Part, Target, Upload, Version, Versioning, WrappedKey, Wrapper,
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

    fn create_upload(&mut self, key: &str) -> String {
        let create = name::Command::CreateUpload(CreateUpload {
            bucket: BUCKET.into(),
            incarnation: 1,
            key: key.into(),
            at_ns: 0,
            upload: Upload {
                initiated_ns: 0,
                owner: "o".into(),
                headers: Vec::new(),
                checksum: None,
                retention: None,
                legal_hold: None,
            },
        });
        match self.name(create) {
            name::Outcome::Created { upload } => upload,
            other => panic!("{other:?}"),
        }
    }

    /// The plaintext of `file`, read from its rows and chunks and opened under its key: each
    /// block rebuilt from its data chunks alone and checked against its CRC-32C.
    fn read_file(&self, f: u128, size: u64, wrapping: &WrappingKey) -> Vec<u8> {
        let header = file::header(&self.files, f).unwrap().unwrap();
        let wrapped = header.key.unwrap();
        let data = DataKey::unwrap(&wrapped.bytes, wrapping).unwrap();
        let segments = Segments::new(&data, f).unwrap();
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

    /// The current version of `key`.
    fn current(&self, key: &str) -> Version {
        name::current(&self.names, BUCKET, key).unwrap().unwrap().1
    }
}

/// Block IDs counted from a start.
struct Counter(u128);

impl Ids for Counter {
    fn block(&mut self) -> Result<u128, PutError> {
        self.0 += 1;
        Ok(self.0)
    }
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
        },
        default: None,
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
    let body = Body {
        length,
        checksum: Some(Algorithm::Crc64Nvme),
    };
    Put::new(
        commit,
        body,
        keys,
        layout,
        HANDOVER,
        Box::new(Counter(1_000)),
        cell.clock,
    )
    .unwrap()
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
    ) {
        let mut cell = Cell::new(6);
        cell.refusing = refusing;
        let wrapping = WrappingKey::generate().unwrap();
        let layout = Layout::new(scheme).unwrap();
        let bytes = body(len, seed);
        let mut put = new_put(&cell, object("k"), len as u64, layout, keys(50, &wrapping));
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
    ) {
        let mut cell = Cell::new(3);
        let wrapping = WrappingKey::generate().unwrap();
        let layout = Layout::new(Scheme::Copies(2)).unwrap();
        let bytes = body(len, seed);
        let mut put = new_put(&cell, object("k"), len as u64, layout, keys(60, &wrapping));
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
