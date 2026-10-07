//! A sealed log (hyper-raft docs/seal.md §5) on the simulated device: what it is given it reads back
//! across reopening, through sweeps and continuations; its file holds none of the plaintext; it
//! opens only with its own keys; and framing changed with its CRC recomputed is tampering.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity
)]

use hyper_block::block::BlockFile;
use hyper_block::buf::{AlignedBuf, Alignment};
use hyper_block::sim::SimFile;
use hyper_log::format::{FRAME_HEADER_LEN, FrameHeader};
use hyper_log::{
    Config, Entries, Entry, HardState, Log, LogError, Proposal, Sealing, Start, Update, Waits,
};
use hyper_seal::Secret32;
use hyper_seal::keys::{KeyId, WrappingKey};

const ID: u128 = 0x7365_616c_6564_2d6c_6f67;
const BLOCK: usize = 4096;
/// The persist area before segment 0: one segment of `config(16, _)`.
const AREA: u64 = 16 * BLOCK as u64;
/// What every entry and proposal carries, which the file must never hold in the clear.
const MARK: &[u8] = b"PLAINTEXT-MARK-4f1c";

fn config(segment_blocks: u64, max_segments: u32) -> Config {
    Config {
        segment_bytes: segment_blocks * BLOCK as u64,
        max_segments,
        max_groups: 64,
        group_entries: 1 << 16,
        group_bytes: 1 << 24,
        // Nothing cached: every entry the tests read comes back from the file and is opened.
        group_cache: 1,
        queue_submissions: 256,
        waits: Waits::Measured,
    }
}

fn sim(seed: u64) -> SimFile {
    SimFile::new(
        Alignment::new(BLOCK).unwrap(),
        Alignment::new(512).unwrap(),
        seed,
    )
    .unwrap()
}

/// The keys of a log: from fixed bytes, so a test opens again with the same ones.
fn keys(parent: u8, auth: u8) -> Sealing {
    let _ = hyper_seal::lock_keys(256);
    Sealing {
        parent: WrappingKey::new(
            KeyId([parent; 16]),
            0,
            Secret32::from_bytes(&[parent; 32]).unwrap(),
        ),
        auth: Secret32::from_bytes(&[auth; 32]).unwrap(),
    }
}

fn entry(term: u64, index: u64) -> Entry {
    let mut bytes = MARK.to_vec();
    bytes.extend_from_slice(format!("-{index}").as_bytes());
    Entry { term, bytes }
}

fn entries(first: u64, terms: &[u64]) -> Entries {
    Entries {
        first,
        entries: terms
            .iter()
            .zip(first..)
            .map(|(&t, i)| entry(t, i))
            .collect(),
    }
}

/// The log's entries of `group` from `first` through `last`, as term and bytes.
fn read(log: &Log<SimFile>, group: u128, first: u64, last: u64) -> Vec<(u64, Vec<u8>)> {
    log.entries(group, first, last + 1, u64::MAX)
        .unwrap()
        .iter()
        .map(|e| (e.term, e.bytes.to_vec()))
        .collect()
}

fn expected(first: u64, terms: &[u64]) -> Vec<(u64, Vec<u8>)> {
    entries(first, terms)
        .entries
        .into_iter()
        .map(|e| (e.term, e.bytes))
        .collect()
}

/// The whole file's bytes.
fn bytes_of(file: &SimFile) -> Vec<u8> {
    let len = file.len().unwrap() as usize;
    let mut buf = AlignedBuf::zeroed(len, file.alignment()).unwrap();
    buf.set_len(len).unwrap();
    file.read_exact_at(buf.as_mut_slice(), 0).unwrap();
    buf.as_slice().to_vec()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn a_sealed_log_reads_back_what_it_was_given_and_holds_none_of_it_in_the_clear() {
    let log = Log::create_sealed(sim(1), config(16, 8), ID, keys(1, 2)).unwrap();
    let a = Update {
        entries: Some(entries(1, &[1, 1, 2])),
        hard_state: Some(HardState {
            term: 2,
            vote: 1,
            commit: 2,
        }),
        proposals: vec![Proposal {
            index: 7,
            term: 2,
            bytes: [MARK, b"-proposal"].concat(),
        }],
        ..Update::default()
    };
    log.write(1, a).unwrap();
    assert_eq!(read(&log, 1, 1, 3), expected(1, &[1, 1, 2]));
    let file = log.close().unwrap();
    assert!(
        !contains(&bytes_of(&file), MARK),
        "the file holds plaintext"
    );

    let (log, recovery) = Log::open_sealed(file, config(16, 8), ID, keys(1, 2)).unwrap();
    assert!(recovery.damaged.is_empty());
    assert_eq!(read(&log, 1, 1, 3), expected(1, &[1, 1, 2]));
    let view = log.view(1).unwrap().unwrap();
    assert_eq!(
        view.hard_state,
        Some(HardState {
            term: 2,
            vote: 1,
            commit: 2
        })
    );
    assert_eq!(view.proposals.len(), 1);
    assert_eq!(view.proposals[0].bytes, [MARK, b"-proposal"].concat());
    log.close().unwrap();
}

#[test]
fn a_sealed_log_opens_only_with_its_own_keys() {
    let log = Log::create_sealed(sim(2), config(16, 8), ID, keys(1, 2)).unwrap();
    log.write(
        1,
        Update {
            entries: Some(entries(1, &[1, 1])),
            hard_state: Some(HardState {
                term: 1,
                vote: 1,
                commit: 1,
            }),
            ..Update::default()
        },
    )
    .unwrap();
    let file = log.close().unwrap();
    // Another parent key: the sessions' keys do not unwrap.
    let refused = Log::try_open_sealed(file, config(16, 8), ID, keys(9, 2))
        .err()
        .unwrap();
    assert!(
        matches!(refused.error, LogError::Seal(_) | LogError::Tampered(_)),
        "{:?}",
        refused.error
    );
    // Another authentication key: the framing fails its MACs.
    let refused = Log::try_open_sealed(refused.file.unwrap(), config(16, 8), ID, keys(1, 9))
        .err()
        .unwrap();
    assert!(
        matches!(refused.error, LogError::Tampered(_)),
        "{:?}",
        refused.error
    );
    // No keys at all: an unsealed open finds no frame of its own kind.
    let refused = Log::try_open(refused.file.unwrap(), config(16, 8), ID)
        .err()
        .unwrap();
    assert!(
        matches!(refused.error, LogError::Foreign(_) | LogError::Damaged(_)),
        "{:?}",
        refused.error
    );
    // And its own keys still open it.
    let (log, _) = Log::open_sealed(refused.file.unwrap(), config(16, 8), ID, keys(1, 2)).unwrap();
    assert_eq!(read(&log, 1, 1, 2), expected(1, &[1, 1]));
    log.close().unwrap();
}

#[test]
fn an_unsealed_log_does_not_open_sealed() {
    let log = Log::create(sim(3), config(16, 8), ID).unwrap();
    log.write(
        1,
        Update {
            entries: Some(entries(1, &[1])),
            ..Update::default()
        },
    )
    .unwrap();
    let file = log.close().unwrap();
    let refused = Log::try_open_sealed(file, config(16, 8), ID, keys(1, 2))
        .err()
        .unwrap();
    assert!(
        matches!(refused.error, LogError::Tampered(_)),
        "{:?}",
        refused.error
    );
}

/// The frame of the first write, its hard state changed and its CRC recomputed, as someone who can
/// write the device would: the open reports tampering, never a torn tail.
#[test]
fn a_frame_changed_with_its_crc_recomputed_is_tampering() {
    let log = Log::create_sealed(sim(4), config(16, 8), ID, keys(1, 2)).unwrap();
    log.write(
        1,
        Update {
            hard_state: Some(HardState {
                term: 5,
                vote: 2,
                commit: 0,
            }),
            ..Update::default()
        },
    )
    .unwrap();
    log.write(
        1,
        Update {
            hard_state: Some(HardState {
                term: 6,
                vote: 3,
                commit: 0,
            }),
            ..Update::default()
        },
    )
    .unwrap();
    let file = log.close().unwrap();
    let bytes = bytes_of(&file);
    // Segment 0's frames: the empty one the log was created with, then one a write.
    let first = AREA as usize + BLOCK;
    let empty = FrameHeader::decode(&bytes[first..]).unwrap();
    let at = first
        + hyper_block::buf::Alignment::new(BLOCK)
            .unwrap()
            .up(empty.frame_len().unwrap())
            .unwrap();
    let header = FrameHeader::decode(&bytes[at..]).unwrap();
    assert!(header.sealed && header.records > 0);
    let payload = at + FRAME_HEADER_LEN;
    let end = payload + header.payload_len as usize;
    // The hard state record: kind 3, group, term, vote, commit. Its vote becomes 7.
    let record = (payload..end)
        .find(|&i| bytes[i] == 3 && bytes[i + 1..i + 17] == 1u128.to_le_bytes())
        .unwrap();
    let mut frame = bytes[at..end].to_vec();
    frame[record - at + 17 + 8] = 7;
    let mut crc = hyper_log::codec::Crc32c::new();
    crc.update(&frame[..FRAME_HEADER_LEN - 4]);
    crc.update(&frame[FRAME_HEADER_LEN..]);
    frame[FRAME_HEADER_LEN - 4..FRAME_HEADER_LEN].copy_from_slice(&crc.finish().to_le_bytes());
    let mut write = AlignedBuf::zeroed(BLOCK * 2, file.alignment()).unwrap();
    write.set_len(BLOCK * 2).unwrap();
    write.as_mut_slice()[..BLOCK * 2].copy_from_slice(&bytes[at..at + BLOCK * 2]);
    write.as_mut_slice()[..frame.len()].copy_from_slice(&frame);
    file.write_all_at(write.as_slice(), at as u64).unwrap();
    let refused = Log::try_open_sealed(file, config(16, 8), ID, keys(1, 2))
        .err()
        .unwrap();
    assert!(
        matches!(refused.error, LogError::Tampered(_)),
        "{:?}",
        refused.error
    );
}

/// Writes through reopenings: every open continues the head segment with a session of its own,
/// whose key record begins its first frame, and every record of every session reads back.
#[test]
fn every_session_of_a_continued_segment_reads_back() {
    let mut file = sim(5);
    let mut last = 0u64;
    let mut terms = Vec::new();
    for round in 0..5u64 {
        let log = if round == 0 {
            Log::create_sealed(file, config(16, 8), ID, keys(1, 2)).unwrap()
        } else {
            Log::open_sealed(file, config(16, 8), ID, keys(1, 2))
                .unwrap()
                .0
        };
        for _ in 0..3 {
            let first = last + 1;
            log.write(
                1,
                Update {
                    entries: Some(entries(first, &[round + 1, round + 1])),
                    hard_state: Some(HardState {
                        term: round + 1,
                        vote: 1,
                        commit: first,
                    }),
                    proposals: (0..2)
                        .map(|i| Proposal {
                            index: first + 10 + i,
                            term: round + 1,
                            bytes: [MARK, format!("-p{}", first + 10 + i).as_bytes()].concat(),
                        })
                        .collect(),
                    ..Update::default()
                },
            )
            .unwrap();
            last = first + 1;
            terms.extend([round + 1, round + 1]);
        }
        assert_eq!(read(&log, 1, 1, last), expected(1, &terms));
        file = log.close().unwrap();
    }
    let (log, recovery) = Log::open_sealed(file, config(16, 8), ID, keys(1, 2)).unwrap();
    assert!(recovery.damaged.is_empty());
    assert_eq!(read(&log, 1, 1, last), expected(1, &terms));
    for p in log.view(1).unwrap().unwrap().proposals {
        assert_eq!(
            p.bytes,
            [MARK, format!("-p{}", p.index).as_bytes()].concat()
        );
    }
    assert!(!contains(&bytes_of(&log.close().unwrap()), MARK));
}

/// Enough writes that the log reclaims segments: the sweep opens what it copies and seals the copy
/// under its own session, and every live record reads back, before and after reopening.
#[test]
fn sweeps_reseal_what_they_copy() {
    let cfg = config(8, 6);
    let log = Log::create_sealed(sim(6), cfg, ID, keys(1, 2)).unwrap();
    let mut last = [0u64; 3];
    let mut start = [0u64; 3];
    for round in 0..300u64 {
        for group in 0..3u128 {
            let g = group as usize;
            let first = last[g] + 1;
            let term = round / 50 + 1;
            let mut u = Update {
                entries: Some(entries(first, &[term, term, term])),
                hard_state: Some(HardState {
                    term: round / 50 + 1,
                    vote: 1,
                    commit: first,
                }),
                ..Update::default()
            };
            // Group 0 keeps 120 entries live, so the tail it holds is swept; the rest keep 12.
            let keep = if group == 0 { 120 } else { 12 };
            if last[g] > start[g] + keep {
                let index = last[g] - keep;
                // Entry `index` was written in the round of `(index - 1) / 3`.
                u.start = Some(Start {
                    index,
                    term: (index - 1) / 3 / 50 + 1,
                });
                start[g] = index;
            }
            log.write(group, u).unwrap();
            last[g] = first + 2;
        }
    }
    let check = |log: &Log<SimFile>| {
        for group in 0..3u128 {
            let g = group as usize;
            let got = read(log, group, start[g] + 1, last[g]);
            assert_eq!(got.len() as u64, last[g] - start[g], "group {group}");
            for (i, (_, bytes)) in got.iter().enumerate() {
                assert!(bytes.starts_with(MARK));
                assert!(bytes.ends_with(format!("-{}", start[g] + 1 + i as u64).as_bytes()));
            }
        }
    };
    check(&log);
    // More was written than the file holds, so segments were reclaimed; group 0's live entries
    // lie in every tail reclaimed, so each reclaim swept them.
    let written = log.stats(None).unwrap().bytes;
    assert!(
        written > cfg.segment_bytes * u64::from(cfg.max_segments),
        "{written}"
    );
    let file = log.close().unwrap();
    assert!(!contains(&bytes_of(&file), MARK));
    let (log, recovery) = Log::open_sealed(file, cfg, ID, keys(1, 2)).unwrap();
    assert!(recovery.damaged.is_empty());
    check(&log);
    log.close().unwrap();
}

/// A sealed entry's bytes changed, its CRC and its frame's CRC recomputed: the frame's MAC leaves
/// sealed bytes to their own tags but covers each one's CRC field, so a change that keeps the CRC
/// true changes what the MAC covers, and the open reports tampering. (One that leaves the CRC as it
/// was fails the CRC, and the bytes' tag besides.)
#[test]
fn a_sealed_entry_changed_with_its_crcs_recomputed_is_tampering() {
    let log = Log::create_sealed(sim(7), config(16, 8), ID, keys(1, 2)).unwrap();
    log.write(
        1,
        Update {
            entries: Some(entries(1, &[1, 1])),
            hard_state: Some(HardState {
                term: 1,
                vote: 1,
                commit: 2,
            }),
            ..Update::default()
        },
    )
    .unwrap();
    let file = log.close().unwrap();
    let bytes = bytes_of(&file);
    let first = AREA as usize + BLOCK;
    let empty = FrameHeader::decode(&bytes[first..]).unwrap();
    let at = first
        + Alignment::new(BLOCK)
            .unwrap()
            .up(empty.frame_len().unwrap())
            .unwrap();
    let header = FrameHeader::decode(&bytes[at..]).unwrap();
    let payload = at + FRAME_HEADER_LEN;
    let end = payload + header.payload_len as usize;
    let mut frame = bytes[at..end].to_vec();
    let mut first_entry = None;
    hyper_log::format::sealables(&frame[FRAME_HEADER_LEN..], header.records, |s| {
        first_entry.get_or_insert(s);
        Some(())
    })
    .unwrap();
    let s = first_entry.unwrap();
    let base = FRAME_HEADER_LEN;
    frame[base + s.bytes_at] ^= 0x01;
    let stored = &frame[base + s.bytes_at..base + s.bytes_at + s.stored];
    let crc = hyper_log::format::entry_crc(s.group, s.index, s.term, stored);
    frame[base + s.crc_at..base + s.crc_at + 4].copy_from_slice(&crc.to_le_bytes());
    let mut fcrc = hyper_log::codec::Crc32c::new();
    fcrc.update(&frame[..FRAME_HEADER_LEN - 4]);
    fcrc.update(&frame[FRAME_HEADER_LEN..]);
    frame[FRAME_HEADER_LEN - 4..FRAME_HEADER_LEN].copy_from_slice(&fcrc.finish().to_le_bytes());
    let span = Alignment::new(BLOCK).unwrap().up(frame.len()).unwrap();
    let mut write = AlignedBuf::zeroed(span, file.alignment()).unwrap();
    write.set_len(span).unwrap();
    write.as_mut_slice().copy_from_slice(&bytes[at..at + span]);
    write.as_mut_slice()[..frame.len()].copy_from_slice(&frame);
    file.write_all_at(write.as_slice(), at as u64).unwrap();
    let refused = Log::try_open_sealed(file, config(16, 8), ID, keys(1, 2))
        .err()
        .unwrap();
    assert!(
        matches!(refused.error, LogError::Tampered(_)),
        "{:?}",
        refused.error
    );
}
