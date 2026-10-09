//! Native branch bytes and values remain exact when leaf entries are appended in parts.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use std::collections::BTreeMap;

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{Branch, Builder, Op};
use mantle_engine::store::{Config, Store};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 4096,
};

fn store(dir: &tempfile::TempDir) -> Store<DeviceFile> {
    let file = DeviceFile::open(
        &dir.path().join("store"),
        true,
        CachingRequest::Buffered,
        Alignment::new(4096).unwrap(),
    )
    .unwrap();
    Store::create(file, CONFIG).unwrap()
}

#[test]
fn one_native_leaf_keeps_its_documented_payload_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(&dir);
    let mut builder = Builder::new(&mut s, Keys::Exactly(1)).unwrap();
    builder.add(&mut s, b"", Op::Put, &[0, 255]).unwrap();
    let branch = builder.finish(&mut s).unwrap();
    let mut payload = Vec::new();
    s.read_page(branch.root, &mut payload).unwrap();
    // Kind, entry count, empty prefix, first offset, empty suffix's head, suffix length,
    // Put, value length, then the two value bytes (branch/mod.rs's page format).
    assert_eq!(
        payload,
        [1, 1, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 1, 2, 0, 0, 255]
    );
}

#[test]
fn native_shared_prefix_entries_cross_pages_and_reopen_with_exact_values() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(&dir);
    let mut entries = BTreeMap::from([(Vec::new(), (Op::Put, Vec::new()))]);
    // Shared prefixes consume little encoded space, while every entry keeps its full key.
    // The long values produce enough leaves to exercise child/count records in index pages.
    for n in 0u32..2048 {
        let mut key = vec![b'p'; 900];
        key.extend_from_slice(&n.to_be_bytes());
        key.extend_from_slice(&[0, 255]);
        let op = if n % 7 == 0 { Op::Delete } else { Op::Put };
        let value = if n % 11 == 0 {
            Vec::new()
        } else if op == Op::Delete {
            vec![0, 255]
        } else {
            let mut value = vec![n as u8; s.page_capacity() / 2];
            value[0] = 0;
            value[1] = 255;
            value
        };
        entries.insert(key, (op, value));
    }
    let mut builder = Builder::new(&mut s, Keys::Exactly(entries.len() as u64)).unwrap();
    for (key, (op, value)) in &entries {
        builder.add(&mut s, key, *op, value).unwrap();
    }
    let branch = builder.finish(&mut s).unwrap();
    let mut descriptor = Vec::new();
    branch.encode(&mut descriptor).unwrap();
    s.checkpoint(Some(branch.root), 1).unwrap();
    let (file, landed) = s.into_file();
    landed.unwrap();
    let (mut s, recovered) = Store::open(file, CONFIG).unwrap();
    assert_eq!(recovered.root, Some(branch.root));
    let (branch, used) = Branch::decode(&mut s, &descriptor).unwrap();
    assert_eq!(used, descriptor.len());
    let mut cursor = branch.run_at(&mut s, b"").unwrap();
    for (key, (op, value)) in &entries {
        assert!(cursor.valid());
        assert_eq!(cursor.key(), key);
        assert_eq!(cursor.op(), *op);
        assert_eq!(cursor.value(), value);
        cursor.next(&branch, &mut s).unwrap();
    }
    assert!(!cursor.valid());
    cursor.give_back(&mut s);
    let mut value = Vec::new();
    for (key, (op, expected)) in &entries {
        value.clear();
        assert_eq!(branch.get(&mut s, key, &mut value).unwrap(), Some(*op));
        assert_eq!(&value, expected);
    }
}
