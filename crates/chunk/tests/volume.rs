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
use std::sync::atomic::{AtomicU64, Ordering};

use common::{SIZE, config, data, key, put_retrying, sim};
use mantle_chunk::layout::{Geometry, batch_frame_bytes, checkpoint_bytes, largest_frame};
use mantle_chunk::{ChunkError, Config, Reads, Volume};
use mantle_disk::buf::Alignment;
use mantle_disk::file::{CachingRequest, DeviceFile};
use mantle_disk::sim::{Fault, SimFile};

/// A volume formatted to pre-write has every byte of its extent written at format, before
/// its superblocks; one formatted without has only its superblocks and log (docs/design/
/// chunk-store.md §2). The pre-written volume opens and works as any other.
#[test]
fn a_prewritten_volume_writes_its_whole_extent_once() {
    let plain = sim(1);
    let v = Volume::format(Arc::clone(&plain), SIZE, config()).unwrap();
    let end = v.data_span().end;
    v.close();
    assert!((plain.durable_image().unwrap().len() as u64) < end);

    let written = sim(2);
    let prewrite = Config {
        prewrite: true,
        ..config()
    };
    let v = Volume::format(Arc::clone(&written), SIZE, prewrite).unwrap();
    assert_eq!(v.data_span().end, end);
    v.close();
    assert_eq!(written.durable_image().unwrap().len() as u64, end);
    // One transfer per batch's worth of bytes, then the two superblocks.
    let batch = config().limits.batch_bytes as u64;
    let writes = written.stats().unwrap().writes;
    assert!(writes >= end.div_ceil(batch) + 2, "{writes} writes");

    let (v, _) = Volume::open(Arc::clone(&written), config()).unwrap();
    v.put(key(1), &data(1, 10_000)).unwrap();
    assert_eq!(v.read(&key(1), 0, 10_000).unwrap(), data(1, 10_000));
    v.close();
}

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

/// A volume on a raw device, the device's whole capacity, reopens with its chunks: the
/// node's length is the device's, so recovery finds the superblocks, and its flush reaches
/// the device (audit §6.2).
fn a_volume_on_a_raw_device_reopens(node: &std::path::Path) {
    let open = || {
        DeviceFile::open(
            node,
            false,
            CachingRequest::PreferDirect,
            Alignment::new(4096).unwrap(),
        )
        .unwrap()
    };
    let file = open();
    let size = file.len().unwrap();
    assert!(size >= 16 << 20, "{size}");
    let v = Volume::format(file, size, config()).unwrap();
    for n in 0..20u64 {
        v.put(key(n), &data(n, 10_000 + n as usize)).unwrap();
    }
    v.delete(key(3)).unwrap();
    v.close();
    let (v, _) = Volume::open(open(), config()).unwrap();
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

/// macOS: an attached disk image's node, which the attaching user owns.
#[cfg(target_vendor = "apple")]
#[test]
fn a_volume_on_a_raw_device_reopens_with_its_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let image = mantle_disk::image::DiskImage::attach(dir.path(), 16).unwrap();
    a_volume_on_a_raw_device_reopens(image.node());
}

/// Linux: a block device named by `MANTLE_TEST_BLOCK_DEVICE`, whose contents the test
/// destroys; a loop device over a scratch file serves (`losetup --find --show FILE`, as
/// root). Opening a block device needs a privilege a test run does not have by default.
#[cfg(target_os = "linux")]
#[test]
#[ignore = "needs MANTLE_TEST_BLOCK_DEVICE, a block device it may overwrite"]
fn a_volume_on_a_linux_block_device_reopens_with_its_chunks() {
    let device = std::env::var_os("MANTLE_TEST_BLOCK_DEVICE").unwrap();
    a_volume_on_a_raw_device_reopens(std::path::Path::new(&device));
}

/// A simulated file that counts the bytes read from it.
struct Counting {
    file: Arc<SimFile>,
    read: AtomicU64,
}

impl mantle_disk::block::BlockFile for Counting {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }
    fn len(&self) -> Result<u64, mantle_disk::DiskError> {
        mantle_disk::block::BlockFile::len(&*self.file)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), mantle_disk::DiskError> {
        self.read.fetch_add(buf.len() as u64, Ordering::SeqCst);
        self.file.read_exact_at(buf, offset)
    }
    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), mantle_disk::DiskError> {
        self.file.write_all_at(buf, offset)
    }
    fn sync_data(&self) -> Result<(), mantle_disk::DiskError> {
        self.file.sync_data()
    }
}

/// A 4 KiB range at the end of an 8 MiB chunk reads the record's header and checksum table
/// and the one 64 KiB checksum block that holds it, not the 8 MiB before it (audit P01);
/// where the device's measured gap covers the payload before a range, one read takes all of
/// it instead. Either way the range is verified: damage to its checksum block is found, and
/// damage to a block it does not read is not its concern.
#[test]
fn a_range_read_reads_its_checksum_blocks_and_not_the_payload_before_them() {
    let config = |gap| Config {
        segment_size: 16 << 20,
        checksum_shift: 16,
        max_fragments: 64,
        compact: true,
        scrub_period: None,
        limits: mantle_chunk::Limits {
            batch_requests: 16,
            batch_bytes: 32 << 20,
            fragments_per_chunk: 4,
        },
        prewrite: false,
        reads: Reads::measured(1, 0, gap),
    };
    let sim = sim(21);
    let counting = Arc::new(Counting {
        file: Arc::clone(&sim),
        read: AtomicU64::new(0),
    });
    let whole = 8u64 << 20;
    let bytes = data(5, whole as usize);
    let v = Volume::format(Arc::clone(&counting), 64 << 20, config(0)).unwrap();
    v.put(key(1), &bytes).unwrap();
    let tail = whole - 4096;
    let read = |v: &Volume<Arc<Counting>>| {
        counting.read.store(0, Ordering::SeqCst);
        let got = v.read(&key(1), tail, 4096);
        (got, counting.read.load(Ordering::SeqCst))
    };
    let (got, apart) = read(&v);
    assert_eq!(got.unwrap(), &bytes[tail as usize..]);
    assert!(apart <= 72 << 10, "{apart} bytes read for 4 KiB");
    v.close();
    // A gap as long as the payload: the one read of old.
    let (v, _) = Volume::open(Arc::clone(&counting), config(whole)).unwrap();
    let (got, together) = read(&v);
    assert_eq!(got.unwrap(), &bytes[tail as usize..]);
    assert!(together >= whole, "{together} bytes read in one");
    v.close();
    // The record is the first in the first segment, after the segment's header block.
    let (v, _) = Volume::open(Arc::clone(&counting), config(0)).unwrap();
    let payload = v.data_span().start
        + 4096
        + mantle_chunk::record::prefix_len(whole as u32, 16).unwrap() as u64;
    sim.inject(Fault::BitFlip {
        offset: payload,
        bit: 0,
        stored: false,
    })
    .unwrap();
    assert_eq!(read(&v).0.unwrap(), &bytes[tail as usize..]);
    sim.clear_faults().unwrap();
    sim.inject(Fault::BitFlip {
        offset: payload + whole - 1,
        bit: 0,
        stored: false,
    })
    .unwrap();
    assert!(matches!(read(&v).0, Err(ChunkError::Corrupt { .. })));
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

/// A checkpoint comes only when the log could not otherwise hold the next batch and another
/// checkpoint, so consecutive checkpoints of size `C` are at least `L − 2C − b − 2w` of log
/// apart, `b` a batch frame and `w` a wrap before each checkpoint. The trigger it replaces, a
/// third of the log counting the checkpoint itself, came ever sooner as the index filled: 16
/// checkpoints here where this allows 12 (docs/research/11 §9.1).
#[test]
fn checkpoints_come_only_when_the_log_needs_the_room() {
    let config = Config {
        max_fragments: 5000,
        ..config()
    };
    let size = 64 << 20;
    let geometry = Geometry::plan(size, Alignment::new(4096).unwrap(), &config).unwrap();
    let v = Volume::format(sim(9), size, config).unwrap();
    let chunks = 4500u64;
    std::thread::scope(|s| {
        for t in 0..8 {
            let v = &v;
            s.spawn(move || {
                for n in (t..chunks).step_by(8) {
                    v.put(key(n), &data(n, 16)).unwrap();
                }
            });
        }
    });
    let before = v.usage().unwrap().checkpoints;
    // Replacing chunks keeps the index its size; each delete and put is its own batch and a
    // one-block frame.
    let frames = 1200u64;
    for n in 0..frames / 2 {
        v.delete(key(n)).unwrap();
        v.put(key(n), &data(n + 7, 16)).unwrap();
    }
    let checkpoints = v.usage().unwrap().checkpoints - before;
    let block = geometry.block;
    let checkpoint = checkpoint_bytes(chunks + 1, u64::from(geometry.segments), block).unwrap();
    let requests = config.limits.batch_requests;
    let batch = batch_frame_bytes(requests, requests, block).unwrap();
    let wrap = largest_frame(&config, u64::from(geometry.segments), block).unwrap();
    let room = geometry.log_size - 2 * checkpoint - batch - 2 * wrap;
    let bound = (frames * block).div_ceil(room) + 1;
    assert!(
        (1..=bound).contains(&checkpoints),
        "{checkpoints} checkpoints in {frames} frames; the rule allows 1 to {bound}"
    );
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
        match put_retrying(&v, key(n), &data(n, 64 << 10)) {
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
        put_retrying(&v, key(50_000 + n), &data(n, 64 << 10)).unwrap();
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

#[test]
fn cleaning_reclaims_partly_dead_segments_and_keeps_every_live_byte() {
    let file = sim(9);
    let v = Volume::format(Arc::clone(&file), SIZE, config()).unwrap();
    // Small chunks, many per segment, until the volume is full.
    let mut written = Vec::new();
    for n in 0..100_000u64 {
        match put_retrying(&v, key(n), &data(n, 3000)) {
            Ok(()) => written.push(n),
            Err(ChunkError::Full) => break,
            Err(e) => panic!("unexpected {e}"),
        }
    }
    // Delete every other chunk: each sealed segment is about half dead, and deleting frees
    // none of them. Cleaning, by the background cleaner and asked for here, does.
    let before = v.usage().unwrap().free;
    for &n in written.iter().filter(|n| *n % 2 == 0) {
        v.delete(key(n)).unwrap();
    }
    let report = v.clean(8).unwrap();
    let after = v.usage().unwrap().free;

    assert!(report.corrupt == 0, "{report:?}");
    assert!(
        after > before,
        "free segments {before} -> {after} after {report:?}"
    );
    // Freed space takes new writes.
    for n in 0..20u64 {
        put_retrying(&v, key(900_000 + n), &data(n, 3000)).unwrap();
    }
    let check = |v: &Volume<Arc<mantle_disk::sim::SimFile>>| {
        for &n in &written {
            let got = v
                .stat(&key(n))
                .unwrap()
                .map(|s| v.read(&key(n), 0, s.len).unwrap());
            let want = if n % 2 == 0 {
                None
            } else {
                Some(data(n, 3000))
            };
            assert_eq!(got, want, "chunk {n}");
        }
    };
    check(&v);
    drop(v);
    let (v, _) = Volume::open(Arc::clone(&file), config()).unwrap();
    check(&v);
}

#[test]
fn cleaning_leaves_a_corrupt_fragment_in_place() {
    let file = sim(10);
    let v = Volume::format(Arc::clone(&file), SIZE, config()).unwrap();
    for n in 0..300u64 {
        v.put(key(n), &data(n, 3000)).unwrap();
    }
    for n in (0..300u64).filter(|n| n % 3 != 0) {
        v.delete(key(n)).unwrap();
    }
    // Damage one surviving chunk's payload on the device.
    let image = file.durable_image().unwrap();
    let needle = &data(3, 3000)[1000..1064];
    let at = image
        .windows(needle.len())
        .position(|w| w == needle)
        .unwrap() as u64;
    file.inject(mantle_disk::sim::Fault::BitFlip {
        offset: at,
        bit: 4,
        stored: true,
    })
    .unwrap();
    let report = v.clean(64).unwrap();
    assert_eq!(report.corrupt, 1, "{report:?}");
    assert!(matches!(
        v.read(&key(3), 0, 3000),
        Err(ChunkError::Corrupt { .. })
    ));
    for n in (0..300u64).filter(|n| n % 3 == 0 && *n != 3) {
        assert_eq!(
            v.read(&key(n), 0, 3000).unwrap(),
            data(n, 3000),
            "chunk {n}"
        );
    }
}

/// A freed segment keeps the records it held until new ones overwrite them. Whatever the
/// segment is reopened as after a restart, those stale records must never pass for new ones,
/// or the roll-forward at the next open would bring deleted chunks back.
#[test]
fn deleted_chunks_stay_deleted_when_their_segment_is_reused_after_a_restart() {
    let file = sim(11);
    let v = Volume::format(Arc::clone(&file), SIZE, config()).unwrap();
    // Two chunks fill the first segment; the third opens the second segment, and three
    // small ones follow it there, each in a batch of its own.
    v.put(key(1), &data(1, 1000)).unwrap();
    v.put(key(2), &data(2, 200 << 10)).unwrap();
    v.put(key(3), &data(3, 200 << 10)).unwrap();
    for n in 4..7u64 {
        v.put(key(n), &data(n, 1000)).unwrap();
    }
    let before: u64 = v.segments().unwrap().iter().map(|s| s.1).max().unwrap();
    // Deleting everything in the second segment frees it; the checkpoint is written while it
    // is free.
    for n in 3..7u64 {
        v.delete(key(n)).unwrap();
    }
    v.checkpoint().unwrap();
    drop(v);

    // After a restart, the next segment opened is written up to exactly where the deleted
    // small chunks' records begin.
    let (v, _) = Volume::open(Arc::clone(&file), config()).unwrap();
    v.put(key(7), &data(7, 200 << 10)).unwrap();
    let after: u64 = v
        .segments()
        .unwrap()
        .iter()
        .filter(|s| s.0 == mantle_chunk::frame::SegmentState::Open)
        .map(|s| s.1)
        .max()
        .unwrap();
    assert!(
        after > before,
        "a segment was opened with incarnation {after}, not above the {before} already used"
    );
    drop(v);

    let (v, report) = Volume::open(Arc::clone(&file), config()).unwrap();
    for n in 3..7u64 {
        assert_eq!(
            v.stat(&key(n)).unwrap(),
            None,
            "deleted chunk {n} came back"
        );
    }
    assert_eq!(report.rolled_forward, 0, "{report:?}");
    assert_eq!(v.read(&key(1), 0, 1000).unwrap(), data(1, 1000));
    assert_eq!(v.read(&key(7), 0, 200 << 10).unwrap(), data(7, 200 << 10));
}

/// A sender's CRC-32C is checked when the bytes arrive: bytes that changed on the way are
/// refused and nothing is written.
#[test]
fn bytes_that_do_not_match_their_senders_checksum_are_refused() {
    let v = Volume::format(sim(12), SIZE, config()).unwrap();
    let bytes = data(1, 10_000);
    let crc = mantle_crc::crc32c(&bytes);
    assert!(matches!(
        v.put_checked(key(1), &bytes, crc ^ 1),
        Err(ChunkError::Checksum { .. })
    ));
    assert_eq!(v.stat(&key(1)).unwrap(), None);
    v.put_checked(key(1), &bytes, crc).unwrap();
    assert_eq!(v.read(&key(1), 0, 10_000).unwrap(), bytes);

    let more = data(2, 5000);
    assert!(matches!(
        v.append_checked(key(2), 0, &more, false, 0),
        Err(ChunkError::Checksum { .. })
    ));
    assert_eq!(v.stat(&key(2)).unwrap(), None);
    v.append_checked(key(2), 0, &more, false, mantle_crc::crc32c(&more))
        .unwrap();
    assert_eq!(v.read(&key(2), 0, 5000).unwrap(), more);
}

/// Reads past the device's depth wait their turn, and those past the reads let wait are
/// refused with `Busy`: every read let through returns the chunk's bytes, every refusal is
/// counted, and no more than the depth are at the device at once.
#[test]
fn reads_past_the_depth_wait_or_are_refused() {
    let config = Config {
        reads: Reads {
            depth: 2,
            waiting: 1,
            bytes: u64::MAX,
            gap: 0,
        },
        ..config()
    };
    let v = Volume::format(sim(11), SIZE, config).unwrap();
    for n in 0..8 {
        v.put(key(n), &data(n, 3000)).unwrap();
    }
    let (read, refused) = (AtomicU64::new(0), AtomicU64::new(0));
    std::thread::scope(|s| {
        for t in 0..8u64 {
            let (v, read, refused) = (&v, &read, &refused);
            s.spawn(move || {
                for i in 0..200u64 {
                    let n = (t + i) % 8;
                    match v.read(&key(n), 0, 3000) {
                        Ok(bytes) => {
                            assert_eq!(bytes, data(n, 3000));
                            read.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(ChunkError::Busy) => {
                            refused.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => panic!("{e}"),
                    }
                }
            });
        }
    });
    let stats = v.read_stats().unwrap();
    assert!(stats.most_at_device <= 2, "{stats:?}");
    let refused = refused.into_inner();
    assert_eq!(stats.refused, refused);
    assert_eq!(read.into_inner() + refused, 8 * 200);
}

/// Where `bytes` first lie in the file's durable image.
fn find(file: &mantle_disk::sim::SimFile, bytes: &[u8]) -> u64 {
    let image = file.durable_image().unwrap();
    image
        .windows(bytes.len())
        .position(|w| w == bytes)
        .expect("the bytes are on the device") as u64
}

/// An acknowledged put whose bytes read wrong while the volume recovers is kept and
/// reported, never dropped: once the fault clears, it reads back whole (audit S05).
#[test]
fn a_bad_read_at_recovery_never_drops_an_acknowledged_chunk() {
    let file = sim(40);
    let v = Volume::format(Arc::clone(&file), SIZE, config()).unwrap();
    let bytes = data(40, 9_000);
    v.put(key(1), &bytes).unwrap();
    v.close();
    let at = find(&file, &bytes[..64]) + 100;
    file.inject(mantle_disk::sim::Fault::BitFlip {
        offset: at,
        bit: 2,
        stored: false,
    })
    .unwrap();
    let (v, report) = Volume::open(Arc::clone(&file), config()).unwrap();
    assert_eq!(report.damaged, vec![key(1)]);
    assert_eq!(v.stat(&key(1)).unwrap().unwrap().len, 9_000);
    assert!(
        v.read(&key(1), 0, 9_000).is_err(),
        "a read that does not verify"
    );
    file.clear_faults().unwrap();
    assert_eq!(v.read(&key(1), 0, 9_000).unwrap(), bytes);
    v.close();
    let (v, report) = Volume::open(Arc::clone(&file), config()).unwrap();
    assert!(report.damaged.is_empty());
    assert_eq!(v.read(&key(1), 0, 9_000).unwrap(), bytes);
}

/// An acknowledged put damaged on the device is kept, reads of it answer that it does not
/// verify, and a retry of the same bytes writes it again (audits S05, B05).
#[test]
fn a_damaged_chunk_is_reported_and_a_retry_writes_it_again() {
    let file = sim(41);
    let v = Volume::format(Arc::clone(&file), SIZE, config()).unwrap();
    let bytes = data(41, 9_000);
    v.put(key(1), &bytes).unwrap();
    v.close();
    let at = find(&file, &bytes[..64]) + 100;
    file.inject(mantle_disk::sim::Fault::BitFlip {
        offset: at,
        bit: 5,
        stored: true,
    })
    .unwrap();
    let (v, report) = Volume::open(Arc::clone(&file), config()).unwrap();
    assert_eq!(report.damaged, vec![key(1)]);
    assert!(v.read(&key(1), 0, 9_000).is_err());
    v.put(key(1), &bytes).unwrap();
    assert_eq!(v.read(&key(1), 0, 9_000).unwrap(), bytes);
    v.close();
    let (v, _) = Volume::open(Arc::clone(&file), config()).unwrap();
    assert_eq!(v.read(&key(1), 0, 9_000).unwrap(), bytes);
}

/// Two payloads of one length and one CRC-32C are different chunks: the second is refused,
/// and the first reads back (audit B05).
#[test]
fn a_crc_collision_is_not_a_retry() {
    let a = [0x95, 0xfb, 0xdf, 0x74, 0xc4, 0x93, 0x1b, 0xa0];
    let b = [0xf0, 0x80, 0xc8, 0x66, 0x4e, 0x34, 0xce, 0x4a];
    assert_eq!(mantle_crc::crc32c(&a), mantle_crc::crc32c(&b));
    let v = Volume::format(sim(42), SIZE, config()).unwrap();
    v.put(key(1), &a).unwrap();
    assert!(matches!(v.put(key(1), &b), Err(ChunkError::Exists(_))));
    v.put(key(1), &a).unwrap();
    assert_eq!(v.read(&key(1), 0, 8).unwrap(), a);
    // The same within one batch: submitted at once, whichever lands first, the other is
    // refused, and the one read back is whole.
    for n in 0..20u64 {
        let k = key(100 + n);
        let (ra, rb) = std::thread::scope(|s| {
            let ha = s.spawn(|| v.put(k, &a));
            let hb = s.spawn(|| v.put(k, &b));
            (ha.join().unwrap(), hb.join().unwrap())
        });
        let got = v.read(&k, 0, 8).unwrap();
        match (ra, rb) {
            (Ok(()), Err(ChunkError::Exists(_))) => assert_eq!(got, a),
            (Err(ChunkError::Exists(_)), Ok(())) => assert_eq!(got, b),
            other => panic!("{other:?}"),
        }
    }
}

/// Settings no volume can run with are refused at format and at open, before any I/O: a read
/// depth of none would park every read for good (audit S08).
#[test]
fn settings_no_volume_can_run_with_are_refused_before_any_io() {
    let broken = [
        Config {
            reads: Reads {
                depth: 0,
                waiting: 1,
                bytes: u64::MAX,
                gap: 0,
            },
            ..config()
        },
        Config {
            limits: mantle_chunk::Limits {
                batch_requests: 0,
                ..config().limits
            },
            ..config()
        },
        Config {
            limits: mantle_chunk::Limits {
                batch_bytes: 0,
                ..config().limits
            },
            ..config()
        },
        Config {
            limits: mantle_chunk::Limits {
                fragments_per_chunk: 0,
                ..config().limits
            },
            ..config()
        },
        Config {
            max_fragments: 0,
            ..config()
        },
        Config {
            checksum_shift: 30,
            ..config()
        },
    ];
    for (i, c) in broken.into_iter().enumerate() {
        let file = sim(60 + i as u64);
        assert!(matches!(
            Volume::format(Arc::clone(&file), SIZE, c),
            Err(ChunkError::Config(_))
        ));
        assert!(matches!(
            Volume::open(Arc::clone(&file), c),
            Err(ChunkError::Config(_))
        ));
        let stats = file.stats().unwrap();
        assert_eq!(
            (stats.reads, stats.writes, stats.syncs),
            (0, 0, 0),
            "setting {i}"
        );
    }
}

/// Concurrent reads hold their buffers to the volume's read bytes, a read larger than them
/// going alone: the most the reads buffered at once is within the bytes or one large read's
/// (audit S07).
#[test]
fn reads_hold_their_buffers_to_the_read_bytes() {
    let budget = 64 << 10;
    let config = Config {
        reads: Reads {
            depth: 4,
            waiting: 64,
            bytes: budget,
            gap: 0,
        },
        ..config()
    };
    let v = Volume::format(sim(70), SIZE, config).unwrap();
    for n in 0..8 {
        v.put(key(n), &data(n, 3_000)).unwrap();
    }
    v.put(key(100), &data(100, 200_000)).unwrap();
    std::thread::scope(|s| {
        for t in 0..8u64 {
            let v = &v;
            s.spawn(move || {
                for i in 0..200u64 {
                    let (k, len) = if (t + i) % 23 == 0 {
                        (key(100), 200_000)
                    } else {
                        (key((t + i) % 8), 3_000)
                    };
                    match v.read(&k, 0, len) {
                        Ok(bytes) => assert_eq!(bytes.len() as u64, len),
                        Err(ChunkError::Busy) => std::thread::yield_now(),
                        Err(e) => panic!("{e}"),
                    }
                }
            });
        }
    });
    let stats = v.read_stats().unwrap();
    // The large read's buffer: its header and table, and its payload.
    assert!(stats.most_bytes <= 220_000, "{stats:?}");
    assert!(stats.most_bytes > 0);
}
