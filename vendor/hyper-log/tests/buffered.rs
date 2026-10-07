//! A log on a file the system will not open for direct I/O: opened buffered, its alignment is one
//! byte, and the log is laid out in the device's write unit instead (`BlockFile::layout_block`).
//! Before, `Log::create` refused every such file ("a segment header past its block"), so no log
//! could live on tmpfs or on most FUSE file systems.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_log::{Config, Entries, Entry, Facts, Log, Update};

const ID: u128 = 0x6275_6666_6572_6564;

fn buffered(path: &std::path::Path) -> DeviceFile {
    DeviceFile::open(
        path,
        true,
        CachingRequest::Buffered,
        Alignment::new(4096).unwrap(),
    )
    .unwrap()
}

#[test]
fn a_log_on_a_buffered_file_is_written_closed_reopened_and_read_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log");
    let file = buffered(&path);
    assert_eq!(file.alignment(), Alignment::BYTE);
    // Derived for the block the file is laid out in.
    let config = Config::derive(&Facts {
        align: file.layout_block(),
        sealed: false,
        largest_entry: 64 << 10,
        disk_bytes: 64 << 20,
        max_groups: 4,
        cadence_entries: 256,
        cadence_bytes: 16 << 20,
        uncommitted_entries: 64,
        uncommitted_bytes: 1 << 20,
        cache_bytes: 1 << 20,
    })
    .unwrap();
    let log = Log::create(file, config, ID).unwrap();
    let entries: Vec<Entry> = (1..=20u8)
        .map(|n| Entry {
            term: 1,
            bytes: vec![n; usize::from(n) * 1000],
        })
        .collect();
    // An entry an update, each within a frame: the log's frames are padded to the layout block.
    for (first, entry) in (1u64..).zip(&entries) {
        log.submit(
            7,
            Update {
                entries: Some(Entries {
                    first,
                    entries: vec![entry.clone()],
                }),
                ..Update::default()
            },
        )
        .unwrap()
        .wait()
        .unwrap();
    }
    drop(log.close().unwrap());
    let (log, _) = Log::open(buffered(&path), config, ID).unwrap();
    let read = log.entries(7, 1, 21, u64::MAX).unwrap();
    assert_eq!(read.len(), entries.len());
    for (read, written) in read.iter().zip(&entries) {
        assert_eq!(read.bytes, written.bytes);
    }
    drop(log.close().unwrap());
}
