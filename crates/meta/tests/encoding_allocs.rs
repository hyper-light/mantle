//! What a metadata message, a row's key and an entry's writes allocate, counted exactly: each
//! message and each key one buffer, made at its length before a byte of it is written, so none
//! grows; an entry's overlay takes its rows by move, so none is copied.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_measure::alloc;
use mantle_meta::engine::{Model, Rows, Write};
use mantle_meta::key::{self, NameRow};
use mantle_meta::overlay::Overlay;
use mantle_meta::record::{
    BlockHeader, ChunkPlace, Extent, FileHeader, Referrer, Target, WrappedKey, Wrapper,
};
use mantle_meta::wire::{Answer, Command, Entry, Sessioned};
use mantle_meta::{block, file};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// The allocations and reallocations `f` makes on this thread.
fn counted<T>(f: impl FnOnce() -> T) -> (T, (u64, u64)) {
    alloc::begin();
    let out = f();
    let counts = alloc::end();
    (out, (counts.allocations, counts.reallocations))
}

fn referrer() -> Referrer {
    Referrer {
        bucket: "bucket".into(),
        incarnation: 3,
        key: "some/key\u{0}with a zero".into(),
    }
}

/// A put's File and Block commands, as the gateway asks them: a file's extents and referrer,
/// and a block's header and chunks, each a record within the entry.
fn put_entry() -> Entry {
    let write = file::Command::Write {
        file: 7,
        extents: (0..3)
            .map(|i| Extent {
                length: 8 << 20,
                target: Target::Block(i),
            })
            .collect(),
        referrer: referrer(),
        key: Some(WrappedKey {
            by: Wrapper::Root(1),
            bytes: [7; 40],
        }),
        handover_ns: 5,
        blocks_deadline_ns: 9,
        at_ns: 1,
    };
    let block = block::Command::Write {
        block: 11,
        header: BlockHeader {
            length: 8 << 20,
            data: 1,
            parity: 2,
            chunk_len: 8 << 20,
            crc32c: 4,
        },
        chunks: (0..3)
            .map(|index| ChunkPlace {
                volume: 42,
                key: mantle_chunk::ChunkKey {
                    block: 11,
                    epoch: 1,
                    index,
                },
            })
            .collect(),
        file: 7,
        handover_ns: 5,
        at_ns: 1,
    };
    Entry {
        at_ns: 1,
        commands: [Command::File(write), Command::Block(block)]
            .into_iter()
            .enumerate()
            .map(|(i, command)| Sessioned {
                session: 1,
                serial: i as u64,
                unanswered: 0,
                command,
            })
            .collect(),
    }
}

/// An entry, its records within it, is one buffer at its length: its nested records are written
/// in place, their lengths counted first, with no buffer of their own.
#[test]
fn an_entry_is_one_buffer_made_at_its_length() {
    let entry = put_entry();
    let (bytes, made) = counted(|| entry.encode().unwrap());
    assert_eq!(made, (1, 0));
    assert_eq!(bytes.capacity(), bytes.len());
    assert_eq!(Entry::decode(&bytes).unwrap(), entry);
}

/// A row's value and an answer are each one buffer at its length.
#[test]
fn a_row_and_an_answer_are_each_one_buffer() {
    let header = FileHeader {
        length: 1 << 40,
        extents: 3,
        made_ns: 7,
        deadline_ns: 9,
        referrer: referrer(),
        key: None,
    };
    let (bytes, made) = counted(|| header.encode().unwrap());
    assert_eq!((made, bytes.capacity()), ((1, 0), bytes.len()));
    let (bytes, made) = counted(|| {
        Extent {
            length: 5,
            target: Target::File(3),
        }
        .encode()
    });
    assert_eq!((made, bytes.capacity()), ((1, 0), bytes.len()));
    let (bytes, made) = counted(|| Answer::Registered { session: 7 }.encode().unwrap());
    assert_eq!((made, bytes.capacity()), ((1, 0), bytes.len()));
}

/// Each row's key is one buffer at its length, a key's zeros escaped within it; a key read
/// back is one buffer for the object key it names, its bucket passed over.
#[test]
fn a_key_is_one_buffer_made_at_its_length() {
    let (bucket, object) = ("bucket", "a/b\u{0}c/\u{0}\u{0}d");
    let part = NameRow::Part(b"up\0load".to_vec(), 3);
    let keys: Vec<(&str, Box<dyn Fn() -> Vec<u8>>)> = vec![
        ("name null", Box::new(|| key::name(bucket, object, &NameRow::Null))),
        (
            "name version",
            Box::new(|| key::name(bucket, object, &NameRow::Version(9))),
        ),
        (
            "name part",
            Box::new(|| key::name(bucket, object, &part)),
        ),
        ("object", Box::new(|| key::object(bucket, object))),
        ("objects", Box::new(|| key::objects(bucket))),
        ("position", Box::new(|| key::position(bucket, object.as_bytes()))),
        ("mark", Box::new(|| key::mark(bucket, object, 5))),
        ("gate", Box::new(|| key::gate(bucket))),
        ("route", Box::new(|| key::route(bucket, object))),
        ("file header", Box::new(|| key::file_header(5))),
        ("file extent", Box::new(|| key::file_extent(5, 9))),
        ("block chunk", Box::new(|| key::block_chunk(5, 2))),
        ("reverse", Box::new(|| key::reverse(5, 6))),
        ("owned", Box::new(|| key::owned("owner", bucket))),
        ("released", Box::new(|| key::released(1, 2))),
        ("unsettled", Box::new(|| key::unsettled(1, 2))),
    ];
    for (what, make) in &keys {
        let (bytes, made) = counted(make);
        assert_eq!(made, (1, 0), "{what}");
        assert_eq!(bytes.capacity(), bytes.len(), "{what}");
    }
    for (what, make) in [
        ("bucket span", Box::new(|| key::bucket_span(bucket)) as Box<dyn Fn() -> _>),
        ("marks span", Box::new(|| key::marks_span(bucket))),
        ("bucket routes", Box::new(|| key::bucket_routes(bucket))),
    ] {
        let (_, made) = counted(make);
        assert_eq!(made, (2, 0), "{what}");
    }
    let row = key::name(bucket, object, &NameRow::Version(9));
    let (read, made) = counted(|| key::decode_object_row(&row));
    assert_eq!(read, Some((object.to_owned(), NameRow::Version(9))));
    assert_eq!(made, (1, 0));
}

/// An entry's overlay keeps the rows it is given, moved: writing three rows allocates the one
/// node its map holds them in, and no copy of any key or value.
#[test]
fn an_overlay_keeps_its_rows_by_move() {
    let mut engine = Model::default();
    let writes: Vec<Write> = (0..3u8)
        .map(|i| Write::Put(vec![b'k', i], vec![b'v'; 64]))
        .chain([Write::Delete(vec![b'd'])])
        .collect();
    let mut overlay = Overlay::new(&mut engine, (usize::MAX, usize::MAX));
    let (applied, made) = counted(|| overlay.apply(1, writes));
    applied.unwrap();
    assert_eq!(made, (1, 0));
    assert_eq!(overlay.into_writes().len(), 4);
}
