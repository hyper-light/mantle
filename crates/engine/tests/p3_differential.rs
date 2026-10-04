//! The P3 differential (docs/design/engine.md §8): the port's log writer and reader against
//! RocksDB 11.8.1's own, through fixtures `tests/golden/p3/p3_gen.py` made by running the oracle
//! (`tests/golden/p3/p3_oracle.cc`, RocksDB's unmodified db/log_writer.cc and db/log_reader.cc)
//! outside the repository, as the golden programs are.
//!
//! The port writes every script; each uncompressed WAL's FNV-1a digest must be the oracle's, so
//! the writers wrote the same bytes. Each WAL, and every mutation `mutations` names of it, is then
//! read in each recovery mode, plain and fragment-buffered, with the predecessor checks where the
//! WAL tracks them, and each transcript's digest must be the oracle's. A compressed WAL is read
//! from the oracle's own bytes (`tests/golden/p3/wal/`): the port's ZSTD encoder writes other bytes
//! for the same content, whose frames the reference reads (`tests/zstd_test.rs`). Where the oracle's
//! reader can never return (`hang`: a fixed point its shim detects exactly, engine.md §8), the port's
//! returns.
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
use std::fmt::Write as _;
use std::path::Path;

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use mantle_engine::db::log_format::{PredecessorWalInfo, WalRecoveryMode};
use mantle_engine::db::log_reader::{
    DropReason, FragmentBufferedReader, Reader, Reporter, WalVerification,
};
use mantle_engine::db::log_writer::{Writer, WriterOptions};
use mantle_engine::file::{BlockWritableFile, ReadFailure, SequentialFile, WritableFileWriter};
use mantle_engine::util::compression::CompressionType;
use mantle_engine::util::crc32c;

const GOLDEN: &str = include_str!("golden/p3/p3.txt");
const BLOCK: usize = 32768;

fn golden(path: &str) -> String {
    std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/golden/p3")
            .join(path),
    )
    .unwrap()
}

fn golden_bytes(path: &str) -> Vec<u8> {
    std::fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/golden/p3")
            .join(path),
    )
    .unwrap()
}

fn fnv(data: &[u8]) -> String {
    let mut h: u64 = 0xCBF2_9CE4_8422_2325;
    for &b in data {
        h = (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01B3);
    }
    format!("{h:016x}")
}

fn unhex(h: &str) -> Vec<u8> {
    h.as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

/// Runs `script` against the port's writer into `out`, as p3_oracle's `write` does.
fn write(script: &str, out: &Path) {
    let mut w: Option<Writer<BlockWritableFile<DeviceFile>>> = None;
    for line in script.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        let Some(&op) = f.first() else { continue };
        let result = match op {
            "options" => {
                if f.get(5).is_none_or(|r| *r == "0") {
                    let _ = std::fs::remove_file(out);
                }
                let mut o = WriterOptions::new(f[1].parse().unwrap(), f[2] != "0");
                o.compression_type = CompressionType::from_u32(f[3].parse().unwrap()).unwrap();
                o.track_and_verify_wals = f[4] != "0";
                let file =
                    DeviceFile::open(out, true, CachingRequest::Buffered, Alignment::BYTE).unwrap();
                let file = BlockWritableFile::new(file, 0).unwrap();
                w = Some(Writer::new(WritableFileWriter::new(file), o).unwrap());
                Ok(())
            }
            "compression_record" => w.as_mut().unwrap().add_compression_type_record(),
            "record_rep" => {
                let n: usize = f[1].parse().unwrap();
                let rec: Vec<u8> = unhex(f[2]).into_iter().cycle().take(n).collect();
                w.as_mut().unwrap().add_record(&rec, 0)
            }
            "ts" => w
                .as_mut()
                .unwrap()
                .maybe_add_user_defined_timestamp_size_record(&[(
                    f[1].parse().unwrap(),
                    f[2].parse().unwrap(),
                )]),
            "pred" => {
                w.as_mut()
                    .unwrap()
                    .maybe_add_predecessor_wal_info(Some(&PredecessorWalInfo {
                        log_number: f[1].parse().unwrap(),
                        size_bytes: f[2].parse().unwrap(),
                        last_seqno_recorded: f[3].parse().unwrap(),
                    }))
            }
            "flush" => w.as_mut().unwrap().write_buffer(),
            other => panic!("unknown script op {other}"),
        };
        result.unwrap_or_else(|e| panic!("{line}: {e}"));
    }
    w.unwrap().close().unwrap();
}

/// A WAL held in memory.
struct Bytes {
    data: Vec<u8>,
    pos: usize,
}

impl SequentialFile for Bytes {
    fn read(&mut self, scratch: &mut [u8]) -> Result<usize, ReadFailure> {
        let n = scratch.len().min(self.data.len() - self.pos);
        scratch[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
    fn file_name(&self) -> &str {
        "p3.log"
    }
}

/// p3_oracle's reporter, owning the transcript: the reader reports drops into it, and each record
/// read is added through the reader's reporter (`GetReporter`), so drops and records keep their
/// reading order.
#[derive(Default)]
struct Transcript(String);

impl Transcript {
    fn record(&mut self, offset: u64, record: &[u8]) {
        writeln!(
            self.0,
            "record {offset} {} {:08x}",
            record.len(),
            crc32c::value(record)
        )
        .unwrap();
    }
}

impl Reporter for Transcript {
    fn corruption(&mut self, bytes: usize, reason: &DropReason, log_number: Option<u64>) {
        match log_number {
            Some(n) => writeln!(self.0, "drop {bytes} {reason} log={n}").unwrap(),
            None => writeln!(self.0, "drop {bytes} {reason}").unwrap(),
        }
    }
    fn old_log_record(&mut self, bytes: usize) {
        writeln!(self.0, "old {bytes}").unwrap();
    }
}

/// p3_oracle's `read`: the transcript of reading `data` as log `log` in `mode`.
fn read(data: Vec<u8>, log: u64, mode: u8, retry: bool, track: &[u64]) -> String {
    let mode = WalRecoveryMode::from_u8(mode).unwrap();
    let file = Bytes { data, pos: 0 };
    let mut record = Vec::new();
    let (mut out, ts, eof) = if retry {
        let mut r = FragmentBufferedReader::new(file, Transcript::default(), true, log);
        while r.read_record(&mut record, mode) {
            let offset = r.last_record_offset();
            r.reporter_mut().record(offset, &record);
        }
        let ts = r.recorded_timestamp_size().clone();
        let eof = r.is_eof();
        (std::mem::take(&mut r.reporter_mut().0), ts, eof)
    } else {
        let mut verification = WalVerification::default();
        if let [min_keep, obs_log, size, seq] = *track {
            verification.track_and_verify_wals = true;
            verification.min_wal_number_to_keep = min_keep;
            if obs_log != 0 {
                verification.observed_predecessor_wal_info = Some(PredecessorWalInfo {
                    log_number: obs_log,
                    size_bytes: size,
                    last_seqno_recorded: seq,
                });
            }
        }
        let mut r = Reader::with_verification(file, Transcript::default(), true, log, verification);
        while r.read_record(&mut record, mode) {
            let offset = r.last_record_offset();
            r.reporter_mut().record(offset, &record);
        }
        let ts = r.recorded_timestamp_size().clone();
        let eof = r.is_eof();
        (std::mem::take(&mut r.reporter_mut().0), ts, eof)
    };
    for (cf, sz) in ts.into_iter().collect::<BTreeMap<_, _>>() {
        writeln!(out, "ts {cf} {sz}").unwrap();
    }
    writeln!(out, "end eof={}", u8::from(eof)).unwrap();
    out
}

/// The mutations p3_gen.py names, in its order.
fn mutations(n: usize) -> Vec<(String, &'static str, usize, usize)> {
    let mut out = vec![("whole".to_owned(), "none", 0, 0)];
    let mut b = 0usize;
    while b < n {
        // One byte before the boundary, at it, one after, and past a legacy header.
        let around = [b.checked_sub(1), Some(b), Some(b + 1), Some(b + 7)];
        for at in around.into_iter().flatten() {
            if at > 0 && at < n {
                out.push((format!("cut{at}"), "cut", at, 0));
            }
        }
        b += BLOCK;
    }
    for i in 0..16usize {
        let at = if n == 0 {
            0
        } else {
            ((i as u64 * 2_654_435_761) % n as u64) as usize
        };
        out.push((format!("flip{at}.{}", i % 8), "flip", at, i % 8));
    }
    for k in 0..4usize {
        let at = k * BLOCK;
        if at < n {
            out.push((format!("zero{at}"), "zero", at, 11));
        }
    }
    out
}

fn mutate(data: &[u8], kind: &str, at: usize, extra: usize) -> Vec<u8> {
    match kind {
        "cut" => data[..at].to_vec(),
        "flip" => {
            let mut b = data.to_vec();
            b[at] ^= 1 << extra;
            b
        }
        "zero" => {
            let mut b = data.to_vec();
            let end = (at + extra).min(b.len());
            b[at..end].fill(0);
            b
        }
        _ => data.to_vec(),
    }
}

#[test]
fn the_ports_log_writes_and_reads_as_rocksdbs() {
    let dir = tempfile::tempdir().unwrap();
    // name: (log, tracks predecessors, compressed)
    let configs: BTreeMap<&str, (u64, bool, bool)> = [
        ("legacy", (9, false, false)),
        ("recycle", (9, false, false)),
        ("track", (9, true, false)),
        ("track_recycle", (9, true, false)),
        ("biglog", ((1u64 << 32) | 9, false, false)),
        ("zstd", (9, false, true)),
        ("zstd_recycle", (9, false, true)),
        ("reused", (9, false, false)),
    ]
    .into_iter()
    .collect();
    let mut wals: BTreeMap<&str, Vec<u8>> = BTreeMap::new();
    for (&name, &(_, _, compressed)) in &configs {
        let out = dir.path().join(format!("{name}.log"));
        if name == "reused" {
            write(&golden("scripts/reused.1.txt"), &out);
            write(&golden("scripts/reused.2.txt"), &out);
        } else {
            write(&golden(&format!("scripts/{name}.txt")), &out);
        }
        let ours = std::fs::read(&out).unwrap();
        let data = if compressed {
            golden_bytes(&format!("wal/{name}.log"))
        } else {
            ours
        };
        wals.insert(name, data);
    }
    let mut writes = 0;
    let mut reads = 0;
    let mut hangs = 0;
    for line in GOLDEN.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        match f[0] {
            "write" => {
                assert_eq!(
                    fnv(&wals[f[1]]),
                    f[2],
                    "the port's {} WAL differs from RocksDB's",
                    f[1]
                );
                writes += 1;
            }
            "read" => {
                let (name, mutation, variant, mode, retry, digest) =
                    (f[1], f[2], f[3], f[4], f[5], f[6]);
                let (log, tracks, _) = configs[name];
                let data = &wals[name];
                let (_, kind, at, extra) = mutations(data.len())
                    .into_iter()
                    .find(|m| m.0 == mutation)
                    .unwrap_or_else(|| panic!("{name}: no mutation {mutation}"));
                let track: Vec<u64> = match (tracks, variant) {
                    (false, _) => vec![],
                    (true, "-") => vec![0, 41, 123_456, 777],
                    (true, "mismatch") => vec![0, 41, 123_456, 778],
                    (true, "missing") => vec![40, 0, 0, 0],
                    _ => panic!("{line}"),
                };
                let transcript = read(
                    mutate(data, kind, at, extra),
                    log,
                    mode.parse().unwrap(),
                    retry == "1",
                    &track,
                );
                if digest == "hang" {
                    hangs += 1;
                } else {
                    assert_eq!(fnv(transcript.as_bytes()), digest, "{line}\n{transcript}");
                }
                reads += 1;
            }
            other => panic!("unknown fixture line {other}"),
        }
    }
    eprintln!(
        "{writes} WALs written alike, {reads} reads alike ({hangs} the oracle's never ended, the port's did)"
    );
    assert!(
        writes >= 6 && reads > 1000,
        "{writes} writes, {reads} reads"
    );
}
