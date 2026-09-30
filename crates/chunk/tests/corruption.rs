//! Damage to data already on the device: bit rot, read errors, a damaged superblock, damage
//! inside the index log, and write and flush failures (Ganesan et al., FAST 2017, §3: one
//! fault in one place at a time).
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
use mantle_disk::block::BlockFile;
use mantle_disk::sim::{Crash, Fault, SimFile};

/// Where `needle` sits in what the device durably holds.
fn find(file: &SimFile, needle: &[u8]) -> u64 {
    let image = file.durable_image().unwrap();
    let at = image
        .windows(needle.len())
        .position(|w| w == needle)
        .expect("payload not found on the device");
    at as u64
}

#[test]
fn a_flipped_payload_bit_is_reported_and_never_returned() {
    let file = sim(20);
    let v = Volume::format(Arc::clone(&file), SIZE, config()).unwrap();
    let (a, b) = (data(1, 20_000), data(2, 20_000));
    v.put(key(1), &a).unwrap();
    v.put(key(2), &b).unwrap();
    let at = find(&file, &a[10_000..10_064]);
    file.inject(Fault::BitFlip {
        offset: at + 5,
        bit: 2,
        stored: true,
    })
    .unwrap();
    match v.read(&key(1), 0, 20_000) {
        Err(ChunkError::Corrupt { .. }) => {}
        other => panic!("expected Corrupt, got {other:?}"),
    }
    // A range that avoids the damaged checksum block still reads, verified.
    assert_eq!(v.read(&key(1), 0, 4096).unwrap(), a[..4096]);
    assert_eq!(v.read(&key(2), 0, 20_000).unwrap(), b);
}

#[test]
fn a_flipped_header_bit_is_reported() {
    let file = sim(21);
    let v = Volume::format(Arc::clone(&file), SIZE, config()).unwrap();
    let a = data(3, 5000);
    v.put(key(3), &a).unwrap();
    // The record header sits just before the payload: 96 bytes and a 2-entry checksum table.
    let payload = find(&file, &a[..64]);
    file.inject(Fault::BitFlip {
        offset: payload - 60,
        bit: 0,
        stored: true,
    })
    .unwrap();
    assert!(matches!(
        v.read(&key(3), 0, 5000),
        Err(ChunkError::Corrupt { .. })
    ));
}

#[test]
fn a_read_error_is_reported_as_corruption() {
    let file = sim(22);
    let v = Volume::format(Arc::clone(&file), SIZE, config()).unwrap();
    let a = data(4, 9000);
    v.put(key(4), &a).unwrap();
    let at = find(&file, &a[..64]);
    file.inject(Fault::ReadError { offset: at, len: 1 })
        .unwrap();
    assert!(matches!(
        v.read(&key(4), 0, 9000),
        Err(ChunkError::Corrupt { .. })
    ));
    file.clear_faults().unwrap();
    assert_eq!(v.read(&key(4), 0, 9000).unwrap(), a);
}

#[test]
fn a_damaged_superblock_falls_back_to_its_twin() {
    let file = sim(23);
    let v = Volume::format(Arc::clone(&file), SIZE, config()).unwrap();
    v.put(key(5), &data(5, 3000)).unwrap();
    drop(v);
    // Superblock B (the newer copy after format) lives at 64 KiB in a compact volume.
    file.inject(Fault::BitFlip {
        offset: (64 << 10) + 40,
        bit: 1,
        stored: true,
    })
    .unwrap();
    let (v, _) = Volume::open(Arc::clone(&file), config()).unwrap();
    assert_eq!(v.read(&key(5), 0, 3000).unwrap(), data(5, 3000));
    drop(v);
    // With both copies damaged, the volume is refused rather than guessed at.
    file.inject(Fault::BitFlip {
        offset: 40,
        bit: 1,
        stored: true,
    })
    .unwrap();
    assert!(matches!(
        Volume::open(Arc::clone(&file), config()),
        Err(ChunkError::Format(_))
    ));
}

#[test]
fn damage_inside_the_log_refuses_to_open_but_a_torn_tail_does_not() {
    let file = sim(24);
    let v = Volume::format(Arc::clone(&file), SIZE, config()).unwrap();
    for n in 0..10u64 {
        v.put(key(n), &data(n, 500)).unwrap();
    }
    drop(v);
    // Each small batch is one 4 KiB frame; the log starts at 128 KiB in a compact volume.
    // Damage the third frame: acknowledged frames follow it, so this is not a crash.
    let log = 128u64 << 10;
    file.inject(Fault::BitFlip {
        offset: log + 2 * 4096 + 60,
        bit: 3,
        stored: true,
    })
    .unwrap();
    assert!(matches!(
        Volume::open(Arc::clone(&file), config()),
        Err(ChunkError::CorruptLog { .. })
    ));
    file.clear_faults().unwrap();

    // Damage only the last frame: indistinguishable from a torn write, so the volume opens
    // without that batch.
    file.inject(Fault::BitFlip {
        offset: log + 9 * 4096 + 60,
        bit: 3,
        stored: true,
    })
    .unwrap();
    let (v, _) = Volume::open(Arc::clone(&file), config()).unwrap();
    for n in 0..9u64 {
        assert_eq!(v.read(&key(n), 0, 500).unwrap(), data(n, 500));
    }
}

#[test]
fn a_failed_write_or_flush_fences_the_volume_and_loses_nothing_acknowledged() {
    for fault in [Fault::WriteError, Fault::SyncError] {
        let file = sim(25);
        let v = Volume::format(Arc::clone(&file), SIZE, config()).unwrap();
        v.put(key(1), &data(1, 4000)).unwrap();
        file.inject(fault.clone()).unwrap();
        assert!(
            matches!(v.put(key(2), &data(2, 4000)), Err(ChunkError::Device(_))),
            "{fault:?}"
        );
        assert!(v.is_fenced());
        assert!(matches!(
            v.put(key(3), &data(3, 4000)),
            Err(ChunkError::Fenced)
        ));
        // Reads of acknowledged data still work while fenced.
        assert_eq!(v.read(&key(1), 0, 4000).unwrap(), data(1, 4000));
        drop(v);
        file.crash(Crash::Random).unwrap();
        file.clear_faults().unwrap();
        let (v, _) = Volume::open(Arc::clone(&file), config()).unwrap();
        assert_eq!(v.read(&key(1), 0, 4000).unwrap(), data(1, 4000));
        assert!(v.stat(&key(3)).unwrap().is_none());
        let _ = file.len();
    }
}

#[test]
fn scrubbing_finds_damage_before_a_read_does() {
    let file = sim(26);
    let v = Volume::format(Arc::clone(&file), SIZE, config()).unwrap();
    for n in 0..40u64 {
        v.put(key(n), &data(n, 6000)).unwrap();
    }
    assert_eq!(v.scrub().unwrap(), 0);
    assert!(!v.at_risk());
    let at = find(&file, &data(7, 6000)[2000..2064]);
    file.inject(Fault::BitFlip {
        offset: at,
        bit: 6,
        stored: true,
    })
    .unwrap();
    assert_eq!(v.scrub().unwrap(), 1);
    assert_eq!(v.damaged().unwrap(), vec![key(7)]);
    assert!(v.at_risk());
    // Once repaired (here: rewritten under a new key and the damaged one deleted), the chunk
    // is forgotten.
    v.delete(key(7)).unwrap();
    v.repaired(&key(7)).unwrap();
    assert!(v.damaged().unwrap().is_empty());
    assert_eq!(v.scrub().unwrap(), 0);
}

/// A record whose header is damaged cannot say whose it is; the scrubber names its chunk
/// from the index's record places, and a read error at a live record is damage too.
#[test]
fn scrubbing_names_the_chunk_of_a_damaged_header_and_of_a_read_error() {
    let file = sim(28);
    let v = Volume::format(Arc::clone(&file), SIZE, config()).unwrap();
    for n in 0..40u64 {
        v.put(key(n), &data(n, 5000)).unwrap();
    }
    // The record header sits just before the payload: 96 bytes and a 2-entry checksum table.
    let payload = find(&file, &data(9, 5000)[..64]);
    file.inject(Fault::BitFlip {
        offset: payload - 60,
        bit: 0,
        stored: true,
    })
    .unwrap();
    let unreadable = find(&file, &data(21, 5000)[..64]);
    file.inject(Fault::ReadError {
        offset: unreadable,
        len: 1,
    })
    .unwrap();
    assert_eq!(v.scrub().unwrap(), 2);
    assert_eq!(v.damaged().unwrap(), vec![key(9), key(21)]);
}

#[test]
fn the_background_scrubber_finds_damage_on_its_own() {
    let file = sim(27);
    let mut cfg = config();
    cfg.scrub_period = Some(std::time::Duration::from_millis(200));
    let v = Volume::format(Arc::clone(&file), SIZE, cfg).unwrap();
    for n in 0..40u64 {
        v.put(key(n), &data(n, 6000)).unwrap();
    }
    let at = find(&file, &data(11, 6000)[100..164]);
    file.inject(Fault::BitFlip {
        offset: at,
        bit: 1,
        stored: true,
    })
    .unwrap();
    // The fact this test needs is the scrubber's finding; it has a whole period's worth of
    // passes (many) to make it, and the bound only stops a hang.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while v.damaged().unwrap().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the scrubber never found the damage"
        );
        std::thread::yield_now();
    }
    assert_eq!(v.damaged().unwrap(), vec![key(11)]);
}

/// A delete is answered only once a later frame confirms its batch's, so its frame is never
/// the log's last: damage to it is damage inside the log, refused, and damage to the last
/// frame, the confirmation, loses nothing answered. Before, a delete answered in the last
/// batch was lost with that batch's frame damaged, and the chunk it removed came back
/// (audit S15).
#[test]
fn a_delete_answered_is_never_in_the_last_frame() {
    let log = 128u64 << 10;
    // Ten one-frame batches of puts, the delete's frame, then its confirmation's.
    for (frame, opens) in [(10u64, false), (11, true)] {
        let file = sim(25 + frame);
        let v = Volume::format(Arc::clone(&file), SIZE, config()).unwrap();
        for n in 0..10u64 {
            v.put(key(n), &data(n, 500)).unwrap();
        }
        v.delete(key(3)).unwrap();
        drop(v);
        file.inject(Fault::BitFlip {
            offset: log + frame * 4096 + 60,
            bit: 3,
            stored: true,
        })
        .unwrap();
        match Volume::open(Arc::clone(&file), config()) {
            Ok((v, _)) => {
                assert!(opens, "damage to the delete's frame was not refused");
                assert!(
                    v.stat(&key(3)).unwrap().is_none(),
                    "a deleted chunk came back"
                );
            }
            Err(e) => {
                assert!(!opens, "{e}");
                assert!(matches!(e, ChunkError::CorruptLog { .. }), "{e}");
            }
        }
    }
}
