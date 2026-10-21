//! A volume's reads for an owner on a runtime task (`mantle_chunk::Reader`): each read at the
//! device as one of its issuer's batches and verified as `Volume::read` verifies one, held to the
//! volume's measured depth, bytes and waiting, and refused with `Busy` past them
//! (docs/design/node.md §1.3, chunk-store.md §7).
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

use std::future::poll_fn;

use common::device::SimDevice;
use common::{SIZE, config, data, issuer, key, sim};
use hyper_block::sim::Fault;
use hyper_rt::machine::facts::Facts;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use mantle_chunk::{ChunkError, Config, ReadAnswer, Reader, Reads, Volume};

/// One shard with one task: the reader's owner.
fn runtime() -> LocalRuntime {
    LocalRuntime::new(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: 1,
        timers_per_shard: 1,
        interests_per_shard: interests_for(1),
        ring_entries: 1,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 1,
        pin: false,
        cores: Vec::new(),
        page_bytes: usize::try_from(Facts::query().page.base).unwrap(),
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    })
    .unwrap()
}

/// `config()` holding reads to `reads`.
fn reading(reads: Reads) -> Config {
    Config { reads, ..config() }
}

/// Every answer the reader has out or waiting, in the order they come, taken on `rt` by a task
/// that owns the reader and its volume while it waits and gives them back.
fn drain(
    rt: &mut LocalRuntime,
    reader: &mut Option<Reader>,
    volume: &mut Option<Volume<SimDevice>>,
) -> Vec<ReadAnswer> {
    let (mut r, v) = (reader.take().unwrap(), volume.take().unwrap());
    let (r, v, answers) = rt
        .block_on(async move {
            let mut answers = Vec::new();
            while let Some(answer) = poll_fn(|cx| r.poll_next(&v, cx)).await {
                answers.push(answer);
            }
            (r, v, answers)
        })
        .unwrap();
    *reader = Some(r);
    *volume = Some(v);
    answers
}

/// Where `needle` sits in what the device durably holds.
fn find(file: &SimDevice, needle: &[u8]) -> u64 {
    let image = file.durable_image().unwrap();
    image
        .windows(needle.len())
        .position(|w| w == needle)
        .expect("payload not found on the device") as u64
}

#[test]
fn reads_at_the_device_return_what_a_thread_reads() {
    // Four reads at the device, a megabyte of buffers, and a gap of one checksum block, so
    // ranges far into a record read its header apart from its blocks.
    let v = Volume::format(
        issuer(),
        sim(40),
        SIZE,
        reading(Reads::measured(4, 1 << 20, 4096)),
    )
    .unwrap();
    // Whole chunks of many sizes, and one of three fragments appended in turn.
    let mut chunks = Vec::new();
    for n in 1..=12u64 {
        let len = 1 + (n as usize * 7919) % 60_000;
        v.put(key(n), &data(n, len)).unwrap();
        chunks.push((key(n), data(n, len)));
    }
    let parts = [data(100, 9000), data(101, 13_000), data(102, 4001)];
    let mut whole = Vec::new();
    for (i, part) in parts.iter().enumerate() {
        v.append(key(100), whole.len() as u64, part, i == parts.len() - 1)
            .unwrap();
        whole.extend_from_slice(part);
    }
    chunks.push((key(100), whole));
    let mut rt = runtime();
    let mut reader = Some(v.reader(issuer()).unwrap());
    let mut v = Some(v);
    // Ranges drawn from a fixed sequence: whole chunks, prefixes, suffixes and middles, every
    // one compared with what `Volume::read` gives for it.
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    let mut asked = Vec::new();
    for _ in 0..200 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let (k, bytes) = &chunks[(x % chunks.len() as u64) as usize];
        let len = bytes.len() as u64;
        let from = (x >> 8) % len;
        let to = from + 1 + (x >> 24) % (len - from);
        let (r, vol) = (reader.as_mut().unwrap(), v.as_ref().unwrap());
        let id = r.submit(vol, k, from, to - from).unwrap();
        asked.push((id, bytes[from as usize..to as usize].to_vec()));
        assert_eq!(
            vol.read(k, from, to - from).unwrap(),
            asked.last().unwrap().1
        );
        if r.waiting() > 0 {
            for (id, got) in drain(&mut rt, &mut reader, &mut v) {
                let want = &asked.iter().find(|(a, _)| *a == id).unwrap().1;
                assert_eq!(&got.unwrap(), want);
            }
        }
    }
    for (id, got) in drain(&mut rt, &mut reader, &mut v) {
        let want = &asked.iter().find(|(a, _)| *a == id).unwrap().1;
        assert_eq!(&got.unwrap(), want);
    }
    let stats = reader.unwrap().stats();
    assert!(stats.most_at_device <= 4, "{stats:?}");
    assert_eq!(stats.refused, 0);
}

#[test]
fn damage_reads_corrupt_and_its_bytes_are_never_returned() {
    let file = sim(41);
    // Three reads at once: an unmeasured device's one read and one waiting would refuse the third.
    let v = Volume::format(
        issuer(),
        file.clone(),
        SIZE,
        reading(Reads::measured(3, 1 << 20, 0)),
    )
    .unwrap();
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
    let mut rt = runtime();
    let mut r = v.reader(issuer()).unwrap();
    let damaged = r.submit(&v, &key(1), 0, 20_000).unwrap();
    let clear = r.submit(&v, &key(1), 0, 4096).unwrap();
    let other = r.submit(&v, &key(2), 0, 20_000).unwrap();
    let (mut reader, mut v) = (Some(r), Some(v));
    for (id, got) in drain(&mut rt, &mut reader, &mut v) {
        match (id, got) {
            (id, Err(ChunkError::Corrupt { .. })) if id == damaged => {}
            (id, Ok(bytes)) if id == clear => assert_eq!(bytes, a[..4096]),
            (id, Ok(bytes)) if id == other => assert_eq!(bytes, b),
            (id, got) => panic!("read {id} answered {got:?}"),
        }
    }
}

#[test]
fn a_read_error_is_corruption() {
    let file = sim(42);
    let v = Volume::format(issuer(), file.clone(), SIZE, config()).unwrap();
    let a = data(4, 9000);
    v.put(key(4), &a).unwrap();
    let at = find(&file, &a[..64]);
    file.inject(Fault::ReadError { offset: at, len: 1 })
        .unwrap();
    let mut rt = runtime();
    let mut r = v.reader(issuer()).unwrap();
    r.submit(&v, &key(4), 0, 9000).unwrap();
    let (mut reader, mut v) = (Some(r), Some(v));
    let answers = drain(&mut rt, &mut reader, &mut v);
    assert!(
        matches!(answers.as_slice(), [(_, Err(ChunkError::Corrupt { .. }))]),
        "{answers:?}"
    );
    file.clear_faults().unwrap();
    reader
        .as_mut()
        .unwrap()
        .submit(v.as_ref().unwrap(), &key(4), 0, 9000)
        .unwrap();
    let answers = drain(&mut rt, &mut reader, &mut v);
    assert!(matches!(answers.as_slice(), [(_, Ok(bytes))] if *bytes == a));
}

#[test]
fn reads_past_the_depth_wait_in_order_and_past_the_waiting_are_busy() {
    // One read at the device and two waiting: a fourth is refused before it takes anything.
    let v = Volume::format(
        issuer(),
        sim(43),
        SIZE,
        reading(Reads {
            depth: 1,
            waiting: 2,
            bytes: 1 << 20,
            gap: 0,
        }),
    )
    .unwrap();
    for n in 1..=4 {
        v.put(key(n), &data(n, 5000)).unwrap();
    }
    let mut rt = runtime();
    let mut r = v.reader(issuer()).unwrap();
    let ids: Vec<_> = (1..=3)
        .map(|n| r.submit(&v, &key(n), 0, 5000).unwrap())
        .collect();
    assert_eq!((r.out(), r.waiting()), (1, 2));
    assert!(matches!(
        r.submit(&v, &key(4), 0, 5000),
        Err(ChunkError::Busy)
    ));
    let (mut reader, mut v) = (Some(r), Some(v));
    let answers = drain(&mut rt, &mut reader, &mut v);
    let reader = reader.unwrap();
    let order: Vec<_> = answers.iter().map(|(id, _)| *id).collect();
    assert_eq!(order, ids, "the waiting reads went in the order they came");
    for (n, (_, got)) in (1..=3).zip(answers) {
        assert_eq!(got.unwrap(), data(n, 5000));
    }
    let stats = reader.stats();
    assert_eq!((stats.most_at_device, stats.refused), (1, 1));
}

#[test]
fn a_missing_chunk_and_a_range_past_its_end_are_answered_at_once() {
    let v = Volume::format(issuer(), sim(44), SIZE, config()).unwrap();
    v.put(key(1), &data(1, 3000)).unwrap();
    let mut reader = v.reader(issuer()).unwrap();
    assert!(matches!(
        reader.submit(&v, &key(9), 0, 1),
        Err(ChunkError::NotFound(k)) if k == key(9)
    ));
    assert!(matches!(
        reader.submit(&v, &key(1), 2999, 2),
        Err(ChunkError::Range { .. })
    ));
    assert_eq!((reader.out(), reader.waiting()), (0, 0));
}

#[test]
fn a_waiting_read_whose_chunk_was_deleted_is_answered_not_found() {
    let v = Volume::format(
        issuer(),
        sim(45),
        SIZE,
        reading(Reads {
            depth: 1,
            waiting: 1,
            bytes: 1 << 20,
            gap: 0,
        }),
    )
    .unwrap();
    v.put(key(1), &data(1, 5000)).unwrap();
    v.put(key(2), &data(2, 5000)).unwrap();
    let mut rt = runtime();
    let mut r = v.reader(issuer()).unwrap();
    let first = r.submit(&v, &key(1), 0, 5000).unwrap();
    let second = r.submit(&v, &key(2), 0, 5000).unwrap();
    v.delete(key(2)).unwrap();
    let (mut reader, mut v) = (Some(r), Some(v));
    let answers = drain(&mut rt, &mut reader, &mut v);
    assert!(
        matches!(answers.as_slice(), [(a, Ok(_)), (b, Err(ChunkError::NotFound(_)))] if *a == first && *b == second),
        "{answers:?}"
    );
}

#[test]
fn a_chunk_handed_over_is_checked_before_it_is_written_and_answered_once_durable() {
    let v = Volume::format(issuer(), sim(46), SIZE, config()).unwrap();
    let bytes = data(1, 7000);
    let crc = mantle_crc::crc32c(&bytes);
    // Changed on the way: refused before anything reaches the writer.
    assert!(matches!(
        v.put_checked_waking(key(1), bytes.clone(), crc ^ 1, std::task::Waker::noop().clone()),
        Err(ChunkError::Checksum { expected, actual, .. }) if expected == crc ^ 1 && actual == crc
    ));
    assert!(matches!(
        v.read(&key(1), 0, 1),
        Err(ChunkError::NotFound(_))
    ));
    let answer = v
        .put_checked_waking(key(1), bytes.clone(), crc, std::task::Waker::noop().clone())
        .unwrap();
    answer.wait().unwrap();
    assert_eq!(v.read(&key(1), 0, 7000).unwrap(), bytes);
}
