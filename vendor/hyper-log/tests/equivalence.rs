//! Recorded-seed equivalence (mantle note 32 §5.2, L-1 and L-2): a seeded history of groups
//! appending, conflicting, compacting, voting, proposing and leaving, written through the log
//! onto the simulated device, with crashes, torn writes and power cuts between, leaves the same
//! bytes on the device and gets the same answers as mantle-log at mantle `147f035` did.
//!
//! The run is deterministic: every frame's batch is fixed by holding the device in a flush while
//! the batch's updates are submitted (`common::held`), the writer never waits on the clock
//! (`Waits::Never`), and every crash and fault draws from the simulated device's seed. Only the
//! segment nonces are drawn from the operating system (`raft-log.md` §2), so the image is compared
//! with each nonce replaced by its segment's incarnation and the checksums over it recomputed
//! (`canonical`); a checksum that did not hold before still does not.
//!
//! `EXPECTED` holds, per seed, the FNV-1a hash of mantle-log's transcript and of its canonical
//! image, from the same harness run against mantle `147f035` (`docs/benchmarks.md`, "Equivalence").
//! Set `HYPER_LOG_EQUIVALENCE_OUT` to a directory to write each seed's transcript and image there
//! for a byte-for-byte comparison.
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

use std::fmt::Write as _;

use hyper_block::buf::Alignment;
use hyper_block::sim::{Crash, Fault, SimFile};
use hyper_log::{
    Class, Config, Entries, Entry, HardState, Log, LogError, Proposal, Start, Update, Waits,
};

mod common;
use common::Holder;

const ID: u128 = 0x0065_7175_6976_616c_656e_6365;
const BLOCK: usize = 4096;
const SEGMENT: u64 = 16 * BLOCK as u64;
const GROUPS: u64 = 6;
const ROUNDS: u64 = 40;
const CYCLES: u64 = 4;
const SEEDS: u64 = 24;

/// Per seed: the FNV-1a hash of mantle-log's transcript and of its canonical device image.
const EXPECTED: [(u64, u64); SEEDS as usize] = [
    (0x12f05584153022fc, 0x759707004b2ce737),
    (0x11d3590bb351107b, 0x9863d519ddb4dbf3),
    (0x3a31c38249f29860, 0xc7a3b16b036a3ddb),
    (0x54947c86e0c64242, 0x478622165a384376),
    (0xc6b27861398a612b, 0x8b76098b50409bfb),
    (0x579cadf12058c822, 0x5848b9a2afb932f4),
    (0x28a0bbf44f29951e, 0x538e34e808983a1a),
    (0xeaa030fd9e6eb748, 0xc90f973d44d74bae),
    (0xd8007f06fe729490, 0xf163634f41ffc2ec),
    (0x89d5b83a536939e3, 0x152f986e73091584),
    (0xd07ab78d6a5d447e, 0xdcca445a21be49a7),
    (0xeabd09731e5bdf72, 0x67c44bf6cfffb2d1),
    (0x58051351877a5da0, 0x8fa5f1055597aaec),
    (0x81645771868b2863, 0xebb0c41ff34f953f),
    (0xce2bfbe3ce76f424, 0x13f7ddcc13b4d52f),
    (0x460a8a8618443eae, 0x1c840d8edde9543f),
    (0x3f6f27a1fd16c522, 0x9ae6fec0a8aa721b),
    (0xa804737f7012498c, 0xdcee6709addd1040),
    (0x5b8472099c706197, 0xaadc96c52dd39a68),
    (0x606378acce93c30c, 0x927a2c3cd6bd9d07),
    (0x521b0c25ee8cdf10, 0xd351328ebf23d82f),
    (0xc2701645484f32dc, 0x78ddb7097a481f91),
    (0x49ff14894efdb495, 0xb99d0b57f90af507),
    (0x45a4f7e958215bfd, 0xa0f9d9e7a498928c),
];

fn config() -> Config {
    Config {
        segment_bytes: SEGMENT,
        max_segments: 8,
        max_groups: 8,
        group_entries: 24,
        group_bytes: 16 << 10,
        group_cache: 4 << 10,
        queue_submissions: 64,
        waits: Waits::Never,
    }
}

/// SplitMix64, the workload's own generator (Steele, Lea and Flood, OOPSLA 2014).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

fn fnv(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325u64, |h, &b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// What a group holds, as its view says, for choosing a valid next update.
#[derive(Debug, Clone, Copy, Default)]
struct Held {
    start: u64,
    last: u64,
    term: u64,
}

fn held(log: &Log<common::Held>, group: u128) -> Held {
    match log.view(group) {
        Ok(Some(v)) => Held {
            start: v.start.index,
            last: v.last,
            term: v.hard_state.map_or(0, |h| h.term),
        },
        _ => Held::default(),
    }
}

fn bytes(rng: &mut Rng, round: u64) -> Vec<u8> {
    let len = rng.below(1400) as usize;
    (0..len).map(|i| (round as usize + i) as u8).collect()
}

/// The next update of a group, mostly valid: appends, conflicting suffixes, compactions, hard
/// states, proposals and now and then a removal.
fn update(rng: &mut Rng, h: Held, round: u64) -> Update {
    let term = round / 8 + 1;
    if rng.chance(3) {
        return Update {
            remove: true,
            ..Update::default()
        };
    }
    let mut u = Update::default();
    if h.last > h.start && rng.chance(25) {
        let index = h.start + 1 + rng.below(h.last - h.start);
        u.start = Some(Start { index, term });
        return u;
    }
    let first = if h.last > h.start && rng.chance(15) {
        h.start + 1 + rng.below(h.last - h.start)
    } else if rng.chance(3) {
        // A gap past the last entry, which the log refuses.
        h.last + 2
    } else {
        h.last + 1
    };
    let count = rng.below(4);
    u.entries = Some(Entries {
        first,
        entries: (0..count)
            .map(|_| Entry {
                term,
                bytes: bytes(rng, round),
            })
            .collect(),
    });
    let last = first + count - 1;
    if rng.chance(50) {
        u.hard_state = Some(HardState {
            term: term.max(h.term),
            vote: rng.below(3),
            commit: last.min(h.last),
        });
    }
    if rng.chance(10) {
        u.proposals.push(Proposal {
            index: last.max(first) + 2,
            term,
            bytes: bytes(rng, round),
        });
    }
    u
}

fn answer(r: Result<(), LogError>) -> &'static str {
    match r {
        Ok(()) => "ok",
        Err(LogError::Fenced) => "fenced",
        Err(LogError::Closed) => "closed",
        Err(LogError::Busy) => "busy",
        Err(LogError::Full) => "full",
        Err(LogError::TooLarge(_)) => "too-large",
        Err(LogError::TooManyGroups(_)) => "too-many-groups",
        Err(LogError::Backlog(_)) => "backlog",
        Err(LogError::Invalid { .. }) => "invalid",
        Err(LogError::Unavailable { .. }) => "unavailable",
        Err(LogError::Compacted { .. }) => "compacted",
        Err(LogError::Corrupt { .. }) => "corrupt",
        Err(LogError::Damaged(_)) => "damaged",
        Err(LogError::Foreign(_)) => "foreign",
        Err(LogError::Config(_)) => "config",
        Err(LogError::Claimed(_)) => "claimed",
        Err(LogError::Disk(_)) => "disk",
    }
}

fn views(log: &Log<common::Held>, out: &mut String) {
    for g in 1..=GROUPS {
        match log.view(u128::from(g)) {
            Ok(Some(v)) => {
                let _ = write!(
                    out,
                    " g{g}:{}/{}/{}/{:?}/{}/{:?}",
                    v.start.index,
                    v.start.term,
                    v.last,
                    v.hard_state.map(|h| (h.term, h.vote, h.commit)),
                    v.proposals.len(),
                    v.uncertain.map(|m| (m.index, m.term))
                );
                if v.last > v.start.index {
                    let read = log.entries(u128::from(g), v.start.index + 1, v.last + 1, u64::MAX);
                    let digest = read.map(|es| {
                        es.iter()
                            .fold(0u64, |h, e| h ^ fnv(&e.bytes).rotate_left(e.term as u32))
                    });
                    let _ = write!(out, "={digest:?}");
                }
            }
            Ok(None) => out.push_str(&format!(" g{g}:-")),
            Err(e) => out.push_str(&format!(" g{g}:{}", answer(Err(e)))),
        }
    }
    out.push('\n');
}

/// The group whose hard state plugs each round's first frame: an update the log always takes
/// while it has room, so its flush is the one the round holds.
const PLUG: u64 = GROUPS + 1;

/// One round: a plug update is written alone and held in its flush while the round's updates
/// are submitted, so they make the next frame together. A plug the log refuses (it is full)
/// holds nothing, and the round's updates are then written one at a time. The round waits for
/// the one or the other, each a fact the holder hears of: the plug's flush held, or the plug's
/// answer, through its waker; and it begins with nothing heard from the round before.
fn round(log: &Log<common::Held>, stepped: &Holder, rng: &mut Rng, n: u64, out: &mut String) {
    let mut groups: Vec<u64> = (1..=GROUPS).collect();
    for i in (1..groups.len()).rev() {
        groups.swap(i, rng.below(i as u64 + 1) as usize);
    }
    let k = 1 + rng.below(GROUPS) as usize;
    stepped.settle();
    stepped.hold();
    let _released = stepped.released();
    let plug = Update {
        hard_state: Some(HardState {
            term: n,
            vote: 0,
            commit: 0,
        }),
        ..Update::default()
    };
    let tag = usize::try_from(n).unwrap();
    let plug = log
        .submit_waking(u128::from(PLUG), Class::Normal, plug, stepped.waker(tag))
        .unwrap();
    let mut plug_answer = None;
    if !stepped.held_or_told(tag) {
        plug_answer = Some(answer(plug.poll().unwrap()));
        stepped.release();
    }
    let _ = write!(out, "r{n}");
    let mut pending = Vec::new();
    for &g in &groups[..k] {
        let u = update(rng, held(log, u128::from(g)), n);
        if plug_answer.is_some() {
            let a = answer(log.write(u128::from(g), u));
            let _ = write!(out, " {g}:{a}");
            continue;
        }
        match log.submit(u128::from(g), u) {
            Ok(p) => pending.push((g, Some(p), "")),
            Err(e) => pending.push((g, None, answer(Err(e)))),
        }
    }
    stepped.release();
    let plug_answer = plug_answer.unwrap_or_else(|| answer(plug.wait()));
    let _ = write!(out, " plug:{plug_answer}");
    for (g, p, refused) in pending {
        let a = p.map_or(refused, |p| answer(p.wait()));
        let _ = write!(out, " {g}:{a}");
    }
    let (frames, updates) = log.flushed();
    let _ = write!(out, " flushed {frames}/{updates};");
    views(log, out);
}

/// A round under an armed power cut: one update at a time, since a failed write is never
/// flushed and a held flush would wait for it forever.
fn faulty_round(log: &Log<common::Held>, rng: &mut Rng, n: u64, out: &mut String) {
    let _ = write!(out, "f{n}");
    for _ in 0..1 + rng.below(3) {
        let g = 1 + rng.below(GROUPS);
        let u = update(rng, held(log, u128::from(g)), n);
        let a = answer(log.write(u128::from(g), u));
        let _ = write!(out, " {g}:{a}");
    }
    out.push('\n');
}

/// The image with every segment's and frame's nonce replaced by its incarnation, and each
/// checksum over a nonce recomputed: kept valid where it held, kept failing where it did not.
fn canonical(mut image: Vec<u8>) -> Vec<u8> {
    let mut at = SEGMENT as usize;
    while at + BLOCK <= image.len() {
        let block = &image[at..];
        if block.starts_with(b"MNLS") && block[4] == 3 {
            let valid = hyper_log::format::SegmentHeader::decode(block).is_some();
            let incarnation = u64::from_le_bytes(block[24..32].try_into().unwrap());
            image[at + 32..at + 40].copy_from_slice(&incarnation.to_le_bytes());
            let crc = crc32c::crc32c(&image[at..at + 48]) ^ (u32::from(!valid) * u32::MAX);
            image[at + 48..at + 52].copy_from_slice(&crc.to_le_bytes());
        } else if block.starts_with(b"MNLF") && block[4] == 3 {
            let header = hyper_log::format::FrameHeader::decode(block).unwrap();
            let len = header.frame_len().unwrap();
            let end = (at + len).min(image.len());
            let valid = header.verifies(&image[at..end]);
            image[at + 32..at + 40].copy_from_slice(&header.incarnation.to_le_bytes());
            let mut crc = crc32c::crc32c(&image[at..at + 64]);
            crc = crc32c::crc32c_append(crc, &image[(at + 68).min(end)..end]);
            crc ^= u32::from(!valid) * u32::MAX;
            image[at + 64..at + 68].copy_from_slice(&crc.to_le_bytes());
        }
        at += BLOCK;
    }
    image
}

/// The offset of the valid frame with the highest sequence in `image`.
fn last_frame(image: &[u8]) -> Option<u64> {
    let mut best: Option<(u64, u64)> = None;
    let mut at = SEGMENT as usize;
    while at + BLOCK <= image.len() {
        if let Some(h) = hyper_log::format::FrameHeader::decode(&image[at..]) {
            let end = at + h.frame_len()?;
            if end <= image.len()
                && h.verifies(&image[at..end])
                && best.is_none_or(|(s, _)| h.sequence > s)
            {
                best = Some((h.sequence, at as u64));
            }
        }
        at += BLOCK;
    }
    best.map(|(_, at)| at)
}

/// One seed's run: its transcript and its canonical image.
fn run(seed: u64) -> (String, Vec<u8>) {
    let mut file = SimFile::new(
        Alignment::new(BLOCK).unwrap(),
        Alignment::new(512).unwrap(),
        seed,
    )
    .unwrap();
    let mut rng = Rng(seed);
    let mut out = String::new();
    let mut n = 0u64;
    for cycle in 0..CYCLES {
        let (device, stepped) = common::held_telling(file);
        let log = if cycle == 0 {
            Log::create(device, config(), ID).unwrap()
        } else {
            match Log::try_open(device, config(), ID) {
                Ok((log, r)) => {
                    let _ = writeln!(
                        out,
                        "open {} frames, damaged {:?}, restored {:?}",
                        r.frames, r.damaged, r.restored
                    );
                    log
                }
                Err(refused) => {
                    let _ = writeln!(out, "open refused: {}", answer(Err(refused.error)));
                    file = refused.file.unwrap().into_inner();
                    break;
                }
            }
        };
        views(&log, &mut out);
        let cut = (cycle % 2 == 1).then(|| rng.below(ROUNDS));
        for r in 0..ROUNDS {
            n += 1;
            if cut == Some(r) {
                let ops = rng.below(12);
                log.with_file(move |d| d.file().inject(Fault::PowerCut { ops }).unwrap())
                    .unwrap();
            }
            if cut.is_some_and(|c| r >= c) {
                faulty_round(&log, &mut rng, n, &mut out);
            } else {
                round(&log, &stepped, &mut rng, n, &mut out);
            }
        }
        file = log.close().unwrap().into_inner();
        file.crash(Crash::Random).unwrap();
        file.clear_faults().unwrap();
        let image = file.durable_image().unwrap();
        let _ = writeln!(out, "crash {}", fnv(&canonical(image.clone())));
        // Every third cycle ends with the last frame damaged at rest, which the next open
        // restores from its persist record, marks or fences (raft-log.md §6).
        if cycle % 3 == 2
            && let Some(at) = last_frame(&image)
        {
            let offset = at + 68 + rng.below(64);
            file.inject(Fault::BitFlip {
                offset,
                bit: rng.below(8) as u8,
                stored: true,
            })
            .unwrap();
            let _ = writeln!(out, "flip {offset}");
        }
    }
    (out, canonical(file.durable_image().unwrap()))
}

#[expect(
    clippy::disallowed_methods,
    reason = "a test writes the transcripts and images it was asked for, where it was asked"
)]
fn save(dir: &std::path::Path, seed: u64, transcript: &str, image: &[u8]) {
    std::fs::write(dir.join(format!("{seed}.transcript")), transcript).unwrap();
    std::fs::write(dir.join(format!("{seed}.image")), image).unwrap();
}

#[test]
fn every_seed_leaves_mantle_logs_bytes_and_answers() {
    let dir = std::env::var_os("HYPER_LOG_EQUIVALENCE_OUT").map(std::path::PathBuf::from);
    let mut got = Vec::new();
    for seed in 0..SEEDS {
        let (transcript, image) = run(seed);
        if let Some(dir) = &dir {
            save(dir, seed, &transcript, &image);
        }
        got.push((fnv(transcript.as_bytes()), fnv(&image)));
    }
    let rows: String = got
        .iter()
        .map(|(t, i)| format!("    ({t:#018x}, {i:#018x}),\n"))
        .collect();
    assert_eq!(got.as_slice(), EXPECTED.as_slice(), "\n{rows}");
}
