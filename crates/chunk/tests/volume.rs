#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

mod common;

use std::sync::Arc;

use common::{SIZE, config, data, key, sim};
use mantle_chunk::{ChunkError, Volume};
use mantle_disk::buf::Alignment;
use mantle_disk::file::{CachingRequest, DeviceFile};

#[test]
fn chunks_read_back_exactly_on_the_simulated_device() {
    let v = Volume::format(sim(1), SIZE, config()).unwrap();
    for n in 0..50u64 {
        v.put(key(n), &data(n, (n as usize * 997) % 20_000))
            .unwrap();
    }
    for n in 0..50u64 {
        let len = (n as usize * 997) % 20_000;
        assert_eq!(
            v.read(&key(n), 0, len as u64).unwrap(),
            data(n, len),
            "chunk {n}"
        );
        let stat = v.stat(&key(n)).unwrap().unwrap();
        assert_eq!(stat.len, len as u64);
        assert!(stat.sealed);
    }
    // Ranges that start and end inside checksum blocks.
    let whole = data(7, 7 * 997);
    assert_eq!(v.read(&key(7), 1000, 3000).unwrap(), whole[1000..4000]);
    assert_eq!(v.read(&key(7), 6000, 979).unwrap(), whole[6000..6979]);
    assert!(matches!(
        v.read(&key(7), 6000, 980),
        Err(ChunkError::Range { .. })
    ));
    assert!(matches!(
        v.read(&key(999), 0, 1),
        Err(ChunkError::NotFound(_))
    ));
}

#[test]
fn chunks_read_back_exactly_on_a_real_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("volume");
    let file = DeviceFile::open(
        &path,
        true,
        CachingRequest::PreferDirect,
        Alignment::new(4096).unwrap(),
    )
    .unwrap();
    file.preallocate(SIZE).unwrap();
    let v = Volume::format(file, SIZE, config()).unwrap();
    for n in 0..20u64 {
        v.put(key(n), &data(n, 10_000 + n as usize)).unwrap();
    }
    v.delete(key(3)).unwrap();
    drop(v);
    let file = DeviceFile::open(
        &path,
        false,
        CachingRequest::PreferDirect,
        Alignment::new(4096).unwrap(),
    )
    .unwrap();
    let (v, report) = Volume::open(file, config()).unwrap();
    assert!(report.frames >= 21);
    for n in 0..20u64 {
        if n == 3 {
            assert!(v.stat(&key(n)).unwrap().is_none());
            continue;
        }
        assert_eq!(
            v.read(&key(n), 0, 10_000 + n).unwrap(),
            data(n, 10_000 + n as usize)
        );
    }
}

#[test]
fn appends_grow_a_chunk_until_it_is_sealed() {
    let v = Volume::format(sim(2), SIZE, config()).unwrap();
    let k = key(1);
    let whole = data(1, 9000);
    v.append(k, 0, &whole[..3000], false).unwrap();
    v.append(k, 3000, &whole[3000..5000], false).unwrap();
    assert_eq!(v.stat(&k).unwrap().unwrap().len, 5000);
    assert!(!v.stat(&k).unwrap().unwrap().sealed);
    v.append(k, 5000, &whole[5000..], true).unwrap();
    let stat = v.stat(&k).unwrap().unwrap();
    assert_eq!((stat.len, stat.sealed, stat.fragments), (9000, true, 3));
    // A read across fragment boundaries.
    assert_eq!(v.read(&k, 2500, 4000).unwrap(), whole[2500..6500]);
    assert!(matches!(
        v.append(k, 9000, b"more", false),
        Err(ChunkError::Sealed(_))
    ));
}

#[test]
fn invalid_writes_are_refused_and_retries_succeed() {
    let v = Volume::format(sim(3), SIZE, config()).unwrap();
    let k = key(1);
    assert!(matches!(
        v.append(k, 10, b"x", false),
        Err(ChunkError::Gap { end: 0, .. })
    ));
    v.append(k, 0, b"hello", false).unwrap();
    assert!(matches!(
        v.append(k, 9, b"x", false),
        Err(ChunkError::Gap { end: 5, .. })
    ));
    // An exact repeat of a fragment already written is a retry, and succeeds.
    v.append(k, 0, b"hello", false).unwrap();
    assert!(matches!(
        v.append(k, 0, b"jello", false),
        Err(ChunkError::Conflict { .. })
    ));
    v.append(k, 5, b" world", true).unwrap();
    v.append(k, 5, b" world", true).unwrap();
    assert_eq!(v.read(&k, 0, 11).unwrap(), b"hello world");

    let p = key(2);
    v.put(p, b"payload").unwrap();
    v.put(p, b"payload").unwrap();
    assert!(matches!(v.put(p, b"another"), Err(ChunkError::Exists(_))));
    // Deleting twice, or deleting nothing, succeeds.
    v.delete(p).unwrap();
    v.delete(p).unwrap();
    v.delete(key(77)).unwrap();
    assert!(v.stat(&p).unwrap().is_none());
    // The key can be written again once deleted.
    v.put(p, b"again").unwrap();
    assert_eq!(v.read(&p, 0, 5).unwrap(), b"again");
}

#[test]
fn appending_nothing_writes_nothing_and_survives_checkpoints() {
    let file = sim(8);
    let v = Volume::format(Arc::clone(&file), SIZE, config()).unwrap();
    let k = key(1);
    v.append(k, 0, b"", false).unwrap();
    assert!(
        v.stat(&k).unwrap().is_none(),
        "an empty append created a chunk"
    );
    v.append(k, 0, b"abc", false).unwrap();
    v.append(k, 3, b"", false).unwrap();
    v.append(k, 3, b"def", false).unwrap();
    v.append(k, 6, b"", true).unwrap();
    let stat = v.stat(&k).unwrap().unwrap();
    assert_eq!((stat.len, stat.sealed, stat.fragments), (6, true, 3));
    // Enough further writes to force checkpoints, then reopen from one.
    for n in 10..700u64 {
        v.put(key(n), &data(n, 100)).unwrap();
    }
    drop(v);
    let (v, _) = Volume::open(Arc::clone(&file), config()).unwrap();
    assert_eq!(v.read(&k, 0, 6).unwrap(), b"abcdef");
    assert!(v.stat(&k).unwrap().unwrap().sealed);
}

#[test]
fn a_chunk_larger_than_a_segment_is_refused() {
    let v = Volume::format(sim(4), SIZE, config()).unwrap();
    assert!(matches!(
        v.put(key(1), &vec![1u8; 300 << 10]),
        Err(ChunkError::TooLarge { .. })
    ));
}

#[test]
fn reopening_after_many_checkpoints_and_log_wraps_restores_everything() {
    let file = sim(5);
    let v = Volume::format(Arc::clone(&file), SIZE, config()).unwrap();
    // Each put is its own batch and frame; over a thousand frames forces checkpoints and
    // takes the log around its region several times.
    for n in 0..1200u64 {
        v.put(key(n), &data(n, 64 + (n as usize % 300))).unwrap();
        if n % 3 == 0 {
            v.delete(key(n)).unwrap();
        }
    }
    drop(v);
    let (v, report) = Volume::open(Arc::clone(&file), config()).unwrap();
    assert!(
        report.frames < 1200,
        "recovery replayed {} frames: no checkpoint was used",
        report.frames
    );
    for n in 0..1200u64 {
        let expected = if n % 3 == 0 {
            None
        } else {
            Some(data(n, 64 + (n as usize % 300)))
        };
        let got = v
            .stat(&key(n))
            .unwrap()
            .map(|s| v.read(&key(n), 0, s.len).unwrap());
        assert_eq!(got, expected, "chunk {n}");
    }
}

#[test]
fn deleted_space_is_reused_and_a_full_volume_refuses_writes() {
    let v = Volume::format(sim(6), SIZE, config()).unwrap();
    let segments = v.usage().unwrap().segments;
    // Fill the volume with 64 KiB chunks until it refuses.
    let mut written = Vec::new();
    for n in 0..10_000u64 {
        match v.put(key(n), &data(n, 64 << 10)) {
            Ok(()) => written.push(n),
            Err(ChunkError::Full) => break,
            Err(e) => panic!("unexpected {e}"),
        }
    }
    assert!(
        written.len() > (segments as usize) * 2,
        "only {} chunks fit",
        written.len()
    );
    assert!(matches!(
        v.put(key(99_999), &data(1, 64 << 10)),
        Err(ChunkError::Full)
    ));
    // Delete everything: every sealed segment empties and is freed for reuse.
    for &n in &written {
        v.delete(key(n)).unwrap();
    }
    for n in 0..written.len() as u64 {
        v.put(key(50_000 + n), &data(n, 64 << 10)).unwrap();
    }
    let usage = v.usage().unwrap();
    assert!(usage.live_bytes > 0);
    for n in 0..written.len() as u64 {
        assert_eq!(
            v.read(&key(50_000 + n), 0, 64 << 10).unwrap(),
            data(n, 64 << 10)
        );
    }
}

#[test]
fn concurrent_writers_share_group_commits() {
    let file = sim(7);
    let v = Arc::new(Volume::format(Arc::clone(&file), SIZE, config()).unwrap());
    let before = file.stats().unwrap().syncs;
    std::thread::scope(|s| {
        for t in 0..8u64 {
            let v = Arc::clone(&v);
            s.spawn(move || {
                for i in 0..50u64 {
                    let n = t * 1000 + i;
                    v.put(key(n), &data(n, 2000)).unwrap();
                }
            });
        }
    });
    let flushes = file.stats().unwrap().syncs - before;
    assert!(
        flushes < 400,
        "{flushes} flushes for 400 puts: writes are not sharing them"
    );
    for t in 0..8u64 {
        for i in 0..50u64 {
            let n = t * 1000 + i;
            assert_eq!(v.read(&key(n), 0, 2000).unwrap(), data(n, 2000));
        }
    }
}
