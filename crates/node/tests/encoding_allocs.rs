//! What the node's messages on the request path allocate while they are encoded, counted exactly
//! by hyper-measure's counting allocator (Lean doors step 7: a message whose length is known first
//! is made at it, so no buffer grows): a read's query and its answer, a command's reply and a
//! chunk's request head. Each kind, empty, with one item, with many and at the longest its bound
//! allows, is written with no reallocation, in a buffer whose capacity is its length, and byte for
//! byte as the encoders before step 7 wrote it (`reference_query`, `reference_read` and
//! `reference_answers`, those encoders kept here as they were).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_measure::alloc;
use mantle_codec::Writer;
use mantle_meta::name::{Named, Probe, Routed};
use mantle_meta::record::{
    AccessKeyRow, BlockHeader, Bucket, BucketState, Checksum, ChunkPlace, Descriptor, Extent,
    FileHeader, Gate, GateState, How, Lineage, Mark, Part, Principal, Referrer, Standing, Target,
    Upload, Version, Versioning,
};
use mantle_meta::root::{MAX_DOMAIN, Placed};
use mantle_meta::wire::{Answer as Applied, MAX_BUCKET, MAX_COMMANDS, MAX_KEY, MAX_OWNER};
use mantle_node::read::{self, Query, Read};
use mantle_node::{chunk, command};
use mantle_protocol::object::{
    self, Buckets, List, ListVersions, Listed, MAX_BUCKETS, MAX_CHECKSUM, MAX_PAGE_PARTS,
    MAX_PAGE_PRINCIPALS, MAX_TAG, Page, PrincipalListed, Principals, UploadListed, Uploads,
    VersionListed, Versions,
};
use mantle_s3::account::{KEY_ID_CHARS, MAX_NAME};
use mantle_s3::checksum::Algorithm;
use mantle_s3::list::Resume;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// The allocations and reallocations `f` makes on this thread.
fn counted<T>(f: impl FnOnce() -> T) -> (T, (u64, u64)) {
    alloc::begin();
    let out = f();
    let counts = alloc::end();
    (out, (counts.allocations, counts.reallocations))
}

/// The most S3 parts an upload holds (`mantle_s3::body::MAX_PARTS`).
const MOST_PARTS: u16 = 10_000;

/// A reply's room past its head on the request class, where every read is answered, at the bound
/// every node of a cell states.
fn room() -> usize {
    read::reply_room(mantle_node::derive::messages().unwrap().request)
}

fn text(c: char, len: usize) -> String {
    std::iter::repeat_n(c, len).collect()
}

/// A probe's resume position at its longest (read.rs `MAX_POSITION`).
fn longest_position() -> Vec<u8> {
    vec![0xff; 1 + 2 * MAX_BUCKET + 1 + 2 * MAX_KEY + 1 + 1]
}

fn descriptor(generation: u64) -> Descriptor {
    Descriptor {
        id: 9,
        lo: b"lo".to_vec(),
        hi: Some(text('h', MAX_KEY).into_bytes()),
        generation,
    }
}

fn lineage() -> Lineage {
    Lineage {
        now: descriptor(4),
        child: Some(descriptor(5)),
        standing: Standing::Frozen,
        into: Some(descriptor(6)),
        taken: None,
    }
}

fn version() -> Version {
    Version {
        marker: false,
        null: false,
        modified_ns: 7,
        etag: text('e', MAX_TAG),
        size: 1 << 40,
        checksum: Some(Checksum {
            algorithm: 2,
            parts: 3,
            value: vec![5; MAX_CHECKSUM],
        }),
        file: Some(11),
        owner: text('o', MAX_OWNER),
        headers: (0..8)
            .map(|i| (format!("x-amz-meta-{i}"), text('v', 64)))
            .collect(),
        retention: None,
        legal_hold: Some(true),
        listing: Some([3; 32]),
    }
}

fn part(number: u16) -> (u16, Part) {
    (
        number,
        Part {
            etag: text('p', MAX_TAG),
            size: 5 << 20,
            checksum: Some(vec![1; MAX_CHECKSUM]),
            file: u128::from(number),
            modified_ns: 8,
            deadline_ns: 9,
        },
    )
}

fn extent(i: u64) -> (u64, Extent) {
    let target = if i.is_multiple_of(2) {
        Target::Block(u128::from(i))
    } else {
        Target::File(u128::from(i))
    };
    (
        i << 23,
        Extent {
            length: 8 << 20,
            target,
        },
    )
}

fn place(index: u16) -> ChunkPlace {
    ChunkPlace {
        volume: 42,
        key: mantle_chunk::ChunkKey {
            block: 3,
            epoch: 1,
            index,
        },
    }
}

fn block_header() -> BlockHeader {
    BlockHeader {
        length: 8 << 20,
        data: u8::MAX,
        parity: u8::MAX,
        chunk_len: 1 << 16,
        crc32c: 4,
    }
}

fn placed(i: u64) -> Placed {
    Placed {
        volume: u128::from(i),
        node: i,
        domain: text('d', MAX_DOMAIN),
        bytes: 1 << 40,
    }
}

fn listed(i: usize) -> Listed {
    Listed {
        key: format!("{i:0>width$}", width = MAX_KEY),
        etag: text('e', MAX_TAG),
        size: 9,
        modified: 10,
        checksum: Some((Algorithm::Sha512, 10_000)),
        owner: Some(text('o', MAX_OWNER)),
    }
}

fn version_listed(i: usize) -> VersionListed {
    VersionListed {
        key: format!("{i:0>width$}", width = MAX_KEY),
        id: text('i', MAX_TAG),
        latest: i == 0,
        marker: i % 2 == 1,
        etag: text('e', MAX_TAG),
        size: 9,
        modified: 10,
        checksum: Some((Algorithm::Crc64Nvme, 1)),
        owner: text('o', MAX_OWNER),
    }
}

fn upload_listed(i: usize) -> UploadListed {
    UploadListed {
        key: format!("{i:0>width$}", width = MAX_KEY),
        id: text('u', MAX_TAG),
        initiated: 3,
        owner: text('o', MAX_OWNER),
    }
}

fn principal_listed(i: usize) -> PrincipalListed {
    PrincipalListed {
        name: format!("{i:0>width$}", width = MAX_NAME),
        id: text('A', KEY_ID_CHARS),
        created: 4,
        policy: (i == 0).then(|| text('{', 512)),
        keys: (0..mantle_s3::account::MAX_KEYS)
            .map(|k| {
                (
                    text(char::from(b'K' + u8::try_from(k).unwrap()), KEY_ID_CHARS),
                    5,
                )
            })
            .collect(),
    }
}

fn list(start: Option<Resume>) -> List {
    List {
        bucket: text('b', MAX_BUCKET),
        prefix: text('p', MAX_KEY),
        delimiter: "/".into(),
        start,
        max_keys: 1000,
        fetch_owner: true,
    }
}

fn list_versions(markers: bool) -> ListVersions {
    ListVersions {
        bucket: "b".into(),
        prefix: String::new(),
        delimiter: String::new(),
        key_marker: markers.then(|| text('k', MAX_KEY)),
        id_marker: markers.then(|| text('i', MAX_TAG)),
        max: 1000,
    }
}

/// Every query, each kind empty where it can be, short, and at its longest.
fn queries() -> Vec<(&'static str, Query)> {
    let (bucket, key) = (text('b', MAX_BUCKET), text('k', MAX_KEY));
    vec![
        (
            "current empty",
            Query::Current {
                bucket: String::new(),
                key: String::new(),
            },
        ),
        (
            "current longest",
            Query::Current {
                bucket: bucket.clone(),
                key: key.clone(),
            },
        ),
        ("volumes first", Query::Volumes { after: None }),
        (
            "volumes after",
            Query::Volumes {
                after: Some(u128::MAX),
            },
        ),
        ("header", Query::Header { file: 7 }),
        (
            "extents",
            Query::Extents {
                file: 7,
                offset: 1 << 30,
                max: read::extents_page(room()),
            },
        ),
        ("block", Query::Block { block: 3 }),
        ("bucket short", Query::Bucket { bucket: "b".into() }),
        (
            "bucket longest",
            Query::Bucket {
                bucket: bucket.clone(),
            },
        ),
        (
            "gate",
            Query::Gate {
                bucket: bucket.clone(),
                generation: 4,
            },
        ),
        ("list from the first", Query::List(list(None))),
        (
            "list after",
            Query::List(list(Some(Resume::After(key.clone())))),
        ),
        (
            "list past",
            Query::List(list(Some(Resume::Past(key.clone())))),
        ),
        (
            "owned first",
            Query::Owned {
                owner: "o".into(),
                prefix: String::new(),
                after: None,
                max: 1000,
            },
        ),
        (
            "owned longest",
            Query::Owned {
                owner: text('o', MAX_OWNER),
                prefix: bucket.clone(),
                after: Some(bucket.clone()),
                max: 1000,
            },
        ),
        (
            "probe first",
            Query::Probe {
                bucket: "b".into(),
                generation: 1,
                from: None,
                budget: 100,
            },
        ),
        (
            "probe longest",
            Query::Probe {
                bucket: bucket.clone(),
                generation: 1,
                from: Some(longest_position()),
                budget: 100,
            },
        ),
        (
            "upload",
            Query::Upload {
                bucket: bucket.clone(),
                key: key.clone(),
                upload: text('u', MAX_TAG),
            },
        ),
        (
            "parts",
            Query::Parts {
                bucket: bucket.clone(),
                key: key.clone(),
                upload: text('u', MAX_TAG),
                after: 9,
                max: u32::from(MOST_PARTS),
            },
        ),
        (
            "uploads no markers",
            Query::Uploads {
                bucket: "b".into(),
                prefix: String::new(),
                delimiter: String::new(),
                key_marker: None,
                id_marker: None,
                max: 1000,
            },
        ),
        (
            "uploads longest",
            Query::Uploads {
                bucket: bucket.clone(),
                prefix: key.clone(),
                delimiter: "/".into(),
                key_marker: Some(key.clone()),
                id_marker: Some(text('u', MAX_TAG)),
                max: 1000,
            },
        ),
        (
            "version null",
            Query::Version {
                bucket: "b".into(),
                key: "k".into(),
                named: Named::Null,
            },
        ),
        (
            "version order",
            Query::Version {
                bucket: bucket.clone(),
                key: key.clone(),
                named: Named::Order(u64::MAX),
            },
        ),
        ("versions no markers", Query::Versions(list_versions(false))),
        ("versions markers", Query::Versions(list_versions(true))),
        (
            "mark",
            Query::Mark {
                bucket: bucket.clone(),
                key,
                id: u128::MAX,
            },
        ),
        (
            "signer",
            Query::Signer {
                id: text('A', KEY_ID_CHARS),
            },
        ),
        (
            "principal",
            Query::Principal {
                name: text('n', MAX_NAME),
            },
        ),
        (
            "principals first",
            Query::Principals {
                after: None,
                max: 1000,
            },
        ),
        (
            "principals after",
            Query::Principals {
                after: Some(text('n', MAX_NAME)),
                max: 1000,
            },
        ),
    ]
}

/// One answer: its kind, the query it answers, the answer, and the allocations its encoding
/// makes: one for the answer, and one more for each record it carries whole that its own encoder
/// seals first (`Read::encode`).
type Answer = (&'static str, Query, Read, u64);

/// Every answer, each kind empty, with one item, with many and at the longest its bound allows.
fn answers() -> Vec<Answer> {
    let current = Query::Current {
        bucket: "b".into(),
        key: "k".into(),
    };
    let named = Query::Version {
        bucket: "b".into(),
        key: "k".into(),
        named: Named::Order(3),
    };
    let volumes = Query::Volumes { after: None };
    let header = Query::Header { file: 7 };
    let extents = Query::Extents {
        file: 7,
        offset: 0,
        max: read::extents_page(room()),
    };
    let block = Query::Block { block: 3 };
    let bucket = Query::Bucket { bucket: "b".into() };
    let gate = Query::Gate {
        bucket: "b".into(),
        generation: 4,
    };
    let keys = Query::List(list(None));
    let owned = Query::Owned {
        owner: "o".into(),
        prefix: String::new(),
        after: None,
        max: 1000,
    };
    let probe = Query::Probe {
        bucket: "b".into(),
        generation: 1,
        from: None,
        budget: 100,
    };
    let upload = Query::Upload {
        bucket: "b".into(),
        key: "k".into(),
        upload: "u".into(),
    };
    let parts = Query::Parts {
        bucket: "b".into(),
        key: "k".into(),
        upload: "u".into(),
        after: 0,
        max: u32::from(MOST_PARTS),
    };
    let uploads = Query::Uploads {
        bucket: "b".into(),
        prefix: String::new(),
        delimiter: String::new(),
        key_marker: None,
        id_marker: None,
        max: 1000,
    };
    let versions = Query::Versions(list_versions(false));
    let mark = Query::Mark {
        bucket: "b".into(),
        key: "k".into(),
        id: 5,
    };
    let signer = Query::Signer {
        id: text('A', KEY_ID_CHARS),
    };
    let principals = Query::Principals {
        after: None,
        max: 1000,
    };
    // A page of volumes holds as many as a reply carries at the longest domain (read.rs
    // `PLACED_BYTES`), behind whether more follow and the count.
    let most_volumes = (room() - (1 + 4)) / (16 + 8 + 8 + 4 + MAX_DOMAIN);
    let most_extents = u64::from(read::extents_page(room()));
    // A block's own places are its data and parity chunks, each kept once beside them.
    let most_places = 2 * (usize::from(u8::MAX) + usize::from(u8::MAX));
    let signed = Box::new((
        AccessKeyRow {
            principal: text('n', MAX_NAME),
            generation: 2,
            wrapped: [7; mantle_s3::seal::WRAPPED_SECRET],
            created_ns: 3,
        },
        Principal {
            name: text('n', MAX_NAME),
            id: text('A', KEY_ID_CHARS),
            created_ns: 4,
            policy: text('{', 512),
            keys: vec![text('A', KEY_ID_CHARS), text('B', KEY_ID_CHARS)],
        },
    ));
    vec![
        ("current none", current.clone(), Read::Current(None), 1),
        (
            "current",
            current.clone(),
            Read::Current(Some((9, version()))),
            2,
        ),
        ("current no bucket", current, Read::NoBucket, 1),
        (
            "volumes empty",
            volumes.clone(),
            Read::Volumes {
                volumes: Vec::new(),
                more: false,
            },
            1,
        ),
        (
            "volumes one",
            volumes.clone(),
            Read::Volumes {
                volumes: vec![placed(1)],
                more: true,
            },
            1,
        ),
        (
            "volumes most",
            volumes,
            Read::Volumes {
                volumes: (0..u64::try_from(most_volumes).unwrap())
                    .map(placed)
                    .collect(),
                more: true,
            },
            1,
        ),
        ("header none", header.clone(), Read::Header(None), 1),
        (
            "header",
            header,
            Read::Header(Some(FileHeader {
                length: 1 << 40,
                extents: 3,
                made_ns: 7,
                deadline_ns: 9,
                referrer: Referrer {
                    bucket: text('b', MAX_BUCKET),
                    incarnation: 3,
                    key: text('k', MAX_KEY),
                },
                key: None,
            })),
            2,
        ),
        (
            "extents empty",
            extents.clone(),
            Read::Extents(Vec::new()),
            1,
        ),
        (
            "extents one",
            extents.clone(),
            Read::Extents(vec![extent(1)]),
            1,
        ),
        (
            "extents most",
            extents,
            Read::Extents((0..most_extents).map(extent).collect()),
            1,
        ),
        ("block none", block.clone(), Read::Block(None), 1),
        (
            "block one",
            block.clone(),
            Read::Block(Some((block_header(), vec![place(0)]))),
            1,
        ),
        (
            "block most",
            block,
            Read::Block(Some((
                block_header(),
                (0..u16::try_from(most_places).unwrap())
                    .map(place)
                    .collect(),
            ))),
            1,
        ),
        ("bucket none", bucket.clone(), Read::Bucket(None), 1),
        (
            "bucket",
            bucket,
            Read::Bucket(Some(Bucket {
                owner: text('o', MAX_OWNER),
                created_ns: 1,
                location: "us-east-1".into(),
                versioning: Versioning::Enabled,
                state: BucketState::Active,
                attempt: 2,
                progress_ns: 3,
                lock: None,
            })),
            2,
        ),
        ("gate none", gate.clone(), Read::Gate(Routed::Here(None)), 1),
        (
            "gate",
            gate.clone(),
            Read::Gate(Routed::Here(Some(Gate {
                incarnation: 1,
                attempt: 2,
                state: GateState::Closed,
            }))),
            2,
        ),
        (
            "gate moved",
            gate,
            Read::Gate(Routed::Moved(Box::new(lineage()))),
            2,
        ),
        ("keys no bucket", keys.clone(), Read::NoBucket, 1),
        ("keys empty", keys.clone(), Read::Keys(Page::default()), 1),
        (
            "keys one",
            keys.clone(),
            Read::Keys(Page {
                contents: vec![listed(0)],
                common_prefixes: vec![text('p', MAX_KEY)],
                truncated: true,
                next: Some(Resume::After(text('k', MAX_KEY))),
            }),
            1,
        ),
        (
            "keys most",
            keys,
            Read::Keys(Page {
                contents: (0..mantle_s3::list::MAX_KEYS).map(listed).collect(),
                common_prefixes: Vec::new(),
                truncated: true,
                next: Some(Resume::Past(text('k', MAX_KEY))),
            }),
            1,
        ),
        (
            "owned empty",
            owned.clone(),
            Read::Owned(Buckets {
                buckets: Vec::new(),
                next: None,
            }),
            1,
        ),
        (
            "owned one",
            owned.clone(),
            Read::Owned(Buckets {
                buckets: vec![(text('b', MAX_BUCKET), 1)],
                next: Some(text('b', MAX_BUCKET)),
            }),
            1,
        ),
        (
            "owned most",
            owned,
            Read::Owned(Buckets {
                buckets: (0..MAX_BUCKETS)
                    .map(|i| (format!("{i:0>width$}", width = MAX_BUCKET), 1))
                    .collect(),
                next: None,
            }),
            1,
        ),
        (
            "probe found",
            probe.clone(),
            Read::Probe(Routed::Here(Probe::Found)),
            1,
        ),
        (
            "probe clear",
            probe.clone(),
            Read::Probe(Routed::Here(Probe::Clear)),
            1,
        ),
        (
            "probe paused",
            probe.clone(),
            Read::Probe(Routed::Here(Probe::Paused(longest_position()))),
            1,
        ),
        (
            "probe moved",
            probe,
            Read::Probe(Routed::Moved(Box::new(lineage()))),
            2,
        ),
        ("upload none", upload.clone(), Read::Upload(None), 1),
        (
            "upload",
            upload,
            Read::Upload(Some(Upload {
                initiated_ns: 1,
                owner: text('o', MAX_OWNER),
                headers: vec![("content-type".into(), "text/plain".into())],
                checksum: Some((2, true)),
                retention: None,
                legal_hold: Some(false),
                runs: None,
            })),
            2,
        ),
        ("parts none", parts.clone(), Read::Parts(None), 1),
        (
            "parts empty",
            parts.clone(),
            Read::Parts(Some((Vec::new(), false))),
            1,
        ),
        (
            "parts one",
            parts.clone(),
            Read::Parts(Some((vec![part(1)], true))),
            1,
        ),
        (
            "parts most",
            parts,
            Read::Parts(Some(((1..=MOST_PARTS).map(part).collect(), false))),
            1,
        ),
        (
            "uploads empty",
            uploads.clone(),
            Read::Uploads(Uploads::default()),
            1,
        ),
        (
            "uploads one",
            uploads.clone(),
            Read::Uploads(Uploads {
                uploads: vec![upload_listed(0)],
                common_prefixes: vec!["p/".into()],
                truncated: true,
                next: Some((text('k', MAX_KEY), text('u', MAX_TAG))),
            }),
            1,
        ),
        (
            "uploads most",
            uploads,
            Read::Uploads(Uploads {
                uploads: (0..MAX_PAGE_PARTS).map(upload_listed).collect(),
                common_prefixes: Vec::new(),
                truncated: false,
                next: None,
            }),
            1,
        ),
        ("version none", named.clone(), Read::Version(None), 1),
        (
            "version",
            named.clone(),
            Read::Version(Some((3, version()))),
            2,
        ),
        ("version no bucket", named, Read::NoBucket, 1),
        (
            "versions empty",
            versions.clone(),
            Read::Versions(Versions::default()),
            1,
        ),
        (
            "versions one",
            versions.clone(),
            Read::Versions(Versions {
                versions: vec![version_listed(0)],
                common_prefixes: vec!["p/".into()],
                truncated: true,
                next: Some((text('k', MAX_KEY), text('i', MAX_TAG))),
            }),
            1,
        ),
        (
            "versions most",
            versions,
            Read::Versions(Versions {
                versions: (0..mantle_s3::list::MAX_KEYS).map(version_listed).collect(),
                common_prefixes: Vec::new(),
                truncated: false,
                next: None,
            }),
            1,
        ),
        ("mark none", mark.clone(), Read::Mark(None), 1),
        (
            "mark",
            mark,
            Read::Mark(Some(Mark {
                how: How::Version {
                    order: 3,
                    null: true,
                },
                deadline_ns: 9,
            })),
            2,
        ),
        ("signer none", signer.clone(), Read::Signer(None), 1),
        ("signer", signer, Read::Signer(Some(signed)), 3),
        (
            "principals empty",
            principals.clone(),
            Read::Principals(Principals::default()),
            1,
        ),
        (
            "principals one",
            principals.clone(),
            Read::Principals(Principals {
                principals: vec![principal_listed(0)],
                next: Some(text('n', MAX_NAME)),
            }),
            1,
        ),
        (
            "principals most",
            principals,
            Read::Principals(Principals {
                principals: (0..MAX_PAGE_PRINCIPALS).map(principal_listed).collect(),
                next: None,
            }),
            1,
        ),
    ]
}

/// Every query is one buffer made at its length, the bytes the encoder before step 7 wrote.
#[test]
fn every_query_is_written_at_its_length() {
    let mut wrong = Vec::new();
    for (what, query) in queries() {
        let expected = reference_query(&query);
        let (bytes, made) = counted(|| query.encode().unwrap());
        println!("query {what}: {made:?}, {} bytes", bytes.len());
        if made != (1, 0) || bytes.capacity() != bytes.len() {
            wrong.push(format!(
                "{what}: (allocations, reallocations) {made:?}, capacity {} for {} bytes",
                bytes.capacity(),
                bytes.len()
            ));
        }
        assert_eq!(bytes, expected, "{what}");
        assert_eq!(Query::decode(&bytes), Ok(query), "{what}");
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// Every answer is one buffer made at its length, beside one for each record it carries whole,
/// the bytes the encoder before step 7 wrote.
#[test]
fn every_answer_is_written_at_its_length() {
    let mut wrong = Vec::new();
    for (what, query, answer, allocations) in answers() {
        let expected = reference_read(&answer);
        let (bytes, made) = counted(|| answer.encode().unwrap());
        println!("answer {what}: {made:?}, {} bytes", bytes.len());
        if made != (allocations, 0) || bytes.capacity() != bytes.len() {
            wrong.push(format!(
                "{what}: (allocations, reallocations) {made:?}, capacity {} for {} bytes",
                bytes.capacity(),
                bytes.len()
            ));
        }
        assert_eq!(bytes, expected, "{what}");
        assert_eq!(Read::decode(&query, &bytes), Ok(answer), "{what}");
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// The answers a command's reply carries, one of each kind that holds no outcome of a layer's.
fn applied() -> Vec<Applied> {
    vec![
        Applied::Registered { session: 9 },
        Applied::SessionExpired,
        Applied::SessionsFull { until_ns: 5 },
        Applied::Repeated,
        Applied::Superseded,
        Applied::RowTooLarge,
        Applied::AnswersFull,
        Applied::WrongLayer,
    ]
}

/// A command's reply is one buffer made at its length, its answers written in place behind their
/// lengths, the bytes the encoder before step 7 wrote: none, one, each kind, and an entry's most.
#[test]
fn every_command_reply_is_written_at_its_length() {
    let most: Vec<Applied> = applied().into_iter().cycle().take(MAX_COMMANDS).collect();
    let replies = [
        ("none", Vec::new()),
        ("one", vec![Applied::Registered { session: 9 }]),
        ("each kind", applied()),
        ("most", most),
    ];
    let mut wrong = Vec::new();
    for (what, answers) in replies {
        let expected = reference_answers(&answers);
        let (bytes, made) = counted(|| command::encode_answers(&answers).unwrap());
        println!("reply {what}: {made:?}, {} bytes", bytes.len());
        let ok = made == (1, 0) || (answers.is_empty() && made == (0, 0));
        if !ok || bytes.capacity() != bytes.len() {
            wrong.push(format!(
                "{what}: (allocations, reallocations) {made:?}, capacity {} for {} bytes",
                bytes.capacity(),
                bytes.len()
            ));
        }
        assert_eq!(bytes, expected, "{what}");
        assert_eq!(command::decode_answers(&bytes), Ok(answers), "{what}");
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// A chunk's PUT and GET heads are each one buffer made at its length.
#[test]
fn a_chunks_heads_are_written_at_their_length() {
    let key = mantle_chunk::ChunkKey {
        block: u128::MAX,
        epoch: u32::MAX,
        index: u16::MAX,
    };
    let put = chunk::Put {
        volume: u128::MAX,
        key,
        crc32c: 7,
        remaining_ns: u64::MAX,
    };
    let get = chunk::Get {
        volume: u128::MAX,
        key,
        offset: 1 << 20,
        len: 1 << 20,
        remaining_ns: u64::MAX,
    };
    let (bytes, made) = counted(|| put.encode());
    assert_eq!((made, bytes.capacity()), ((1, 0), bytes.len()), "put");
    let (bytes, made) = counted(|| get.encode());
    assert_eq!((made, bytes.capacity()), ((1, 0), bytes.len()), "get");
}

/// `command::encode_answers` as it was before step 7: each answer sealed in its own buffer and
/// copied, behind its length, into a buffer that grows.
fn reference_answers(answers: &[Applied]) -> Vec<u8> {
    let mut out = Vec::new();
    for answer in answers {
        let encoded = answer.encode().unwrap();
        out.extend_from_slice(&u32::try_from(encoded.len()).unwrap().to_le_bytes());
        out.extend_from_slice(&encoded);
    }
    out
}

fn put_text(w: &mut Writer, text: &str) {
    w.u32(u32::try_from(text.len()).unwrap());
    w.bytes(text.as_bytes());
}

fn put_record(w: &mut Writer, record: &[u8]) {
    w.u32(u32::try_from(record.len()).unwrap());
    w.bytes(record);
}

/// `Query::encode` as it was before step 7, in a writer that grows.
fn reference_query(query: &Query) -> Vec<u8> {
    let mut w = Writer::default();
    match query {
        Query::Current { bucket, key } => {
            w.u8(0);
            put_text(&mut w, bucket);
            put_text(&mut w, key);
        }
        Query::Volumes { after } => {
            w.u8(1);
            match after {
                None => w.u8(0),
                Some(after) => {
                    w.u8(1);
                    w.u128(*after);
                }
            }
        }
        Query::Header { file } => {
            w.u8(2);
            w.u128(*file);
        }
        Query::Extents { file, offset, max } => {
            w.u8(3);
            w.u128(*file);
            w.u64(*offset);
            w.u32(*max);
        }
        Query::Block { block } => {
            w.u8(4);
            w.u128(*block);
        }
        Query::Bucket { bucket } => {
            w.u8(5);
            put_text(&mut w, bucket);
        }
        Query::Gate { bucket, generation } => {
            w.u8(6);
            put_text(&mut w, bucket);
            w.u64(*generation);
        }
        Query::List(list) => {
            w.u8(7);
            object::put_list(&mut w, list).unwrap();
        }
        Query::Owned {
            owner,
            prefix,
            after,
            max,
        } => {
            w.u8(8);
            put_text(&mut w, owner);
            put_text(&mut w, prefix);
            match after {
                None => w.u8(0),
                Some(after) => {
                    w.u8(1);
                    put_text(&mut w, after);
                }
            }
            w.u32(*max);
        }
        Query::Probe {
            bucket,
            generation,
            from,
            budget,
        } => {
            w.u8(9);
            put_text(&mut w, bucket);
            w.u64(*generation);
            match from {
                None => w.u8(0),
                Some(from) => {
                    w.u8(1);
                    put_record(&mut w, from);
                }
            }
            w.u32(*budget);
        }
        Query::Upload {
            bucket,
            key,
            upload,
        } => {
            w.u8(10);
            put_text(&mut w, bucket);
            put_text(&mut w, key);
            put_text(&mut w, upload);
        }
        Query::Parts {
            bucket,
            key,
            upload,
            after,
            max,
        } => {
            w.u8(11);
            put_text(&mut w, bucket);
            put_text(&mut w, key);
            put_text(&mut w, upload);
            w.u16(*after);
            w.u32(*max);
        }
        Query::Uploads {
            bucket,
            prefix,
            delimiter,
            key_marker,
            id_marker,
            max,
        } => {
            w.u8(12);
            put_text(&mut w, bucket);
            put_text(&mut w, prefix);
            put_text(&mut w, delimiter);
            for marker in [key_marker, id_marker] {
                match marker {
                    None => w.u8(0),
                    Some(marker) => {
                        w.u8(1);
                        put_text(&mut w, marker);
                    }
                }
            }
            w.u32(*max);
        }
        Query::Version { bucket, key, named } => {
            w.u8(13);
            put_text(&mut w, bucket);
            put_text(&mut w, key);
            match named {
                Named::Null => w.u8(0),
                Named::Order(order) => {
                    w.u8(1);
                    w.u64(*order);
                }
            }
        }
        Query::Versions(list) => {
            w.u8(14);
            object::put_list_versions(&mut w, list).unwrap();
        }
        Query::Mark { bucket, key, id } => {
            w.u8(15);
            put_text(&mut w, bucket);
            put_text(&mut w, key);
            w.u128(*id);
        }
        Query::Signer { id } => {
            w.u8(16);
            put_text(&mut w, id);
        }
        Query::Principal { name } => {
            w.u8(17);
            put_text(&mut w, name);
        }
        Query::Principals { after, max } => {
            w.u8(18);
            match after {
                None => w.u8(0),
                Some(after) => {
                    w.u8(1);
                    put_text(&mut w, after);
                }
            }
            w.u32(*max);
        }
    }
    w.into_vec()
}

/// `Read::encode` as it was before step 7, each record sealed in its own buffer and copied into a
/// writer that grows.
fn reference_read(read: &Read) -> Vec<u8> {
    let mut w = Writer::default();
    match read {
        Read::Current(None) => w.u8(0),
        Read::Current(Some((order, version))) | Read::Version(Some((order, version))) => {
            w.u8(1);
            w.u64(*order);
            w.bytes(&version.encode().unwrap());
        }
        Read::Volumes { volumes, more } => {
            w.u8(u8::from(*more));
            w.u32(u32::try_from(volumes.len()).unwrap());
            for p in volumes {
                w.u128(p.volume);
                w.u64(p.node);
                w.u64(p.bytes);
                put_text(&mut w, &p.domain);
            }
        }
        Read::Header(None) | Read::Block(None) | Read::Bucket(None) => w.u8(0),
        Read::Gate(Routed::Here(None)) => w.u8(0),
        Read::NoBucket => w.u8(2),
        Read::Keys(page) => {
            w.u8(1);
            object::put_page(&mut w, page).unwrap();
        }
        Read::Owned(page) => object::put_buckets(&mut w, page).unwrap(),
        Read::Probe(Routed::Here(Probe::Found)) => w.u8(0),
        Read::Probe(Routed::Here(Probe::Clear)) => w.u8(1),
        Read::Probe(Routed::Here(Probe::Paused(from))) => {
            w.u8(2);
            put_record(&mut w, from);
        }
        Read::Probe(Routed::Moved(lineage)) => {
            w.u8(3);
            w.bytes(&lineage.encode().unwrap());
        }
        Read::Upload(None) | Read::Parts(None) | Read::Version(None) | Read::Mark(None) => w.u8(0),
        Read::Versions(page) => {
            w.u8(1);
            object::put_versions(&mut w, page).unwrap();
        }
        Read::Mark(Some(mark)) => {
            w.u8(1);
            w.bytes(&mark.encode());
        }
        Read::Upload(Some(upload)) => {
            w.u8(1);
            w.bytes(&upload.encode().unwrap());
        }
        Read::Parts(Some((parts, more))) => {
            w.u8(1);
            w.u8(u8::from(*more));
            w.u32(u32::try_from(parts.len()).unwrap());
            for (number, part) in parts {
                w.u16(*number);
                put_record(&mut w, &part.encode().unwrap());
            }
        }
        Read::Uploads(page) => {
            w.u8(1);
            object::put_uploads(&mut w, page).unwrap();
        }
        Read::Bucket(Some(row)) => {
            w.u8(1);
            w.bytes(&row.encode().unwrap());
        }
        Read::Gate(Routed::Here(Some(gate))) => {
            w.u8(1);
            w.bytes(&gate.encode());
        }
        Read::Gate(Routed::Moved(lineage)) => {
            w.u8(2);
            w.bytes(&lineage.encode().unwrap());
        }
        Read::Header(Some(header)) => {
            w.u8(1);
            w.bytes(&header.encode().unwrap());
        }
        Read::Extents(extents) => {
            w.u32(u32::try_from(extents.len()).unwrap());
            for (offset, extent) in extents {
                w.u64(*offset);
                put_record(&mut w, &extent.encode());
            }
        }
        Read::Block(Some((header, places))) => {
            w.u8(1);
            put_record(&mut w, &header.encode());
            w.u32(u32::try_from(places.len()).unwrap());
            for place in places {
                put_record(&mut w, &place.encode());
            }
        }
        Read::Signer(None) => w.u8(0),
        Read::Signer(Some(signer)) => {
            let (row, held) = signer.as_ref();
            w.u8(1);
            put_record(&mut w, &row.encode().unwrap());
            put_record(&mut w, &held.encode().unwrap());
        }
        Read::Principals(page) => object::put_principals(&mut w, page).unwrap(),
    }
    w.into_vec()
}
