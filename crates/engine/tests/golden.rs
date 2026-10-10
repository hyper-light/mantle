//! The port against RocksDB 11.8.1 itself: every P1 function's output over every input length
//! 0..=4096, several seeds, and 4096 random draws, compared with the vectors RocksDB's own code
//! printed (`tests/golden/p1_gen.cc`, output `tests/golden/p1.txt`; docs/research/24 §3.1 P1).
//!
//! The file stores, per record, the FNV-1a 64 digest of each window of 32 consecutive outputs,
//! so a mismatch names the record and the lengths or draws it lies in. The inputs come from
//! SplitMix64 on both sides, so nothing but the functions differs.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

mod common;

use std::collections::BTreeMap;

use common::SplitMix64;
use mantle_engine::table::format::{
    ChecksumType, checksum_modifier_for_context, compute_builtin_checksum,
    compute_builtin_checksum_with_last_byte,
};
use mantle_engine::util::coding::{
    get_varint32_ptr, get_varint64_ptr, put_varint32, put_varint64, put_varsignedint64,
    varint_length,
};
use mantle_engine::util::crc32c;
use mantle_engine::util::fastrange::{fast_range32, fast_range64};
use mantle_engine::util::file_checksum_helper::FileChecksumGenCrc32c;
use mantle_engine::util::hash::{
    bijective_hash2x64_with_seed, bijective_unhash2x64_with_seed, bloom_hash, hash, hash2x64,
    hash2x64_with_seed, hash64, hash64_with_seed,
};
use mantle_engine::util::math::BitMath;
use mantle_engine::util::prefix_varint::{
    get_prefix_varint32_ptr, get_prefix_varint64_ptr, put_prefix_varint32, put_prefix_varint64,
};
use mantle_engine::util::xxhash::{xxh3_64bits, xxh3_64bits_with_seed, xxh32, xxh64};
use mantle_engine::{Error, Malformed};

const GOLDEN: &str = include_str!("golden/p1.txt");
const MAX_LEN: usize = 4096;
const DRAWS: usize = 4096;
const WINDOW: usize = 32;
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0100_0000_01b3;

/// Digests of windows of outputs, as `Record` in p1_gen.cc.
struct Record {
    count: usize,
    d: u64,
    digests: Vec<u64>,
}

impl Record {
    fn new() -> Self {
        Self {
            count: 0,
            d: FNV_OFFSET,
            digests: Vec::new(),
        }
    }
    fn bytes(&mut self, b: &[u8]) {
        for &x in b {
            self.d ^= u64::from(x);
            self.d = self.d.wrapping_mul(FNV_PRIME);
        }
    }
    fn u8(&mut self, v: u8) {
        self.bytes(&[v]);
    }
    fn u32(&mut self, v: u32) {
        self.bytes(&v.to_le_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.bytes(&v.to_le_bytes());
    }
    fn end_item(&mut self) {
        self.count += 1;
        if self.count.is_multiple_of(WINDOW) {
            self.flush();
        }
    }
    fn flush(&mut self) {
        self.digests.push(self.d);
        self.d = FNV_OFFSET;
    }
    fn finish(mut self) -> (usize, Vec<u64>) {
        if !self.count.is_multiple_of(WINDOW) {
            self.flush();
        }
        (self.count, self.digests)
    }
}

type Records = BTreeMap<String, (usize, Vec<u64>)>;

/// One record over every length: `f` feeds the output for `buf[..n]`.
fn by_length(out: &mut Records, name: &str, mut f: impl FnMut(&mut Record, usize)) {
    let mut rec = Record::new();
    for n in 0..=MAX_LEN {
        f(&mut rec, n);
        rec.end_item();
    }
    out.insert(name.to_string(), rec.finish());
}

/// One record over `DRAWS` draws from SplitMix64 seeded `seed`.
fn by_draw(
    out: &mut Records,
    name: &str,
    seed: u64,
    mut f: impl FnMut(&mut Record, &mut SplitMix64, usize),
) {
    let mut rec = Record::new();
    let mut r = SplitMix64(seed);
    for i in 0..DRAWS {
        f(&mut rec, &mut r, i);
        rec.end_item();
    }
    out.insert(name.to_string(), rec.finish());
}

fn compute() -> Records {
    let buf = SplitMix64(0x524F_434B_5344_4231).bytes(MAX_LEN + 64);
    let high: Vec<u8> = buf.iter().map(|b| b | 0x80).collect();
    let mut out = Records::new();

    for seed in [0u32, 0xbc9f_1d34, 397, 0xdead_beef, 0x8000_0000] {
        by_length(&mut out, &format!("hash32_{seed:08x}"), |rec, n| {
            rec.u32(hash(&buf[..n], seed));
        });
    }
    by_length(&mut out, "hash32_high_bloom", |rec, n| {
        rec.u32(bloom_hash(&high[..n]))
    });
    by_length(&mut out, "hash64_unseeded", |rec, n| {
        rec.u64(hash64(&buf[..n]))
    });
    for seed in [1u64, 0x9e37_79b9_7f4a_7c15, u64::MAX, 0x0123_4567_89ab_cdef] {
        by_length(&mut out, &format!("hash64_{seed:016x}"), |rec, n| {
            rec.u64(hash64_with_seed(&buf[..n], seed));
        });
    }
    by_length(&mut out, "hash64_high_unseeded", |rec, n| {
        rec.u64(hash64(&high[..n]))
    });
    by_length(&mut out, "hash2x64_unseeded", |rec, n| {
        let (hi, lo) = hash2x64(&buf[..n]);
        rec.u64(hi);
        rec.u64(lo);
    });
    for seed in [1u64, 0xfedc_ba98_7654_3210] {
        by_length(&mut out, &format!("hash2x64_{seed:016x}"), |rec, n| {
            let (hi, lo) = hash2x64_with_seed(&buf[..n], seed);
            rec.u64(hi);
            rec.u64(lo);
        });
    }
    for seed in [0u32, 0x9747_b28c] {
        by_length(&mut out, &format!("xxh32_{seed:08x}"), |rec, n| {
            rec.u32(xxh32(&buf[..n], seed));
        });
    }
    for seed in [0u64, 0x9747_b28c_9747_b28c] {
        by_length(&mut out, &format!("xxh64_{seed:016x}"), |rec, n| {
            rec.u64(xxh64(&buf[..n], seed));
        });
    }
    by_length(&mut out, "xxh3_64_unseeded", |rec, n| {
        rec.u64(xxh3_64bits(&buf[..n]))
    });
    by_length(&mut out, "xxh3_64_0123456789abcdef", |rec, n| {
        rec.u64(xxh3_64bits_with_seed(&buf[..n], 0x0123_4567_89ab_cdef));
    });
    by_length(&mut out, "crc32c_value", |rec, n| {
        rec.u32(crc32c::value(&buf[..n]))
    });
    by_length(&mut out, "crc32c_mask", |rec, n| {
        rec.u32(crc32c::mask(crc32c::value(&buf[..n])));
    });
    by_length(&mut out, "crc32c_extend", |rec, n| {
        let a = n / 3;
        rec.u32(crc32c::extend(crc32c::value(&buf[..a]), &buf[a..n]));
    });
    by_length(&mut out, "crc32c_combine", |rec, n| {
        let a = n / 3;
        rec.u32(crc32c::crc32c_combine(
            crc32c::value(&buf[..a]),
            crc32c::value(&buf[a..n]),
            n - a,
        ));
    });
    for t in 0u8..=4 {
        let kind = ChecksumType::from_byte(t).unwrap();
        by_length(&mut out, &format!("builtin_checksum_{t}"), |rec, n| {
            rec.u32(compute_builtin_checksum(kind, &buf[..n]));
        });
        by_length(
            &mut out,
            &format!("builtin_checksum_last_byte_{t}"),
            |rec, n| {
                rec.u32(compute_builtin_checksum_with_last_byte(
                    kind,
                    &buf[..n],
                    buf[n],
                ));
            },
        );
    }
    by_length(&mut out, "file_checksum_crc32c", |rec, n| {
        let mut generator = FileChecksumGenCrc32c::new();
        let a = n / 2;
        generator.update(&buf[..a]);
        generator.update(&buf[a..n]);
        rec.bytes(&generator.finalize());
    });

    by_draw(&mut out, "fastrange32", 1, |rec, r, i| {
        let h = r.next() as u32;
        let mut range = r.next() as u32;
        if i % 4 == 0 {
            range &= 0xff;
        }
        rec.u32(fast_range32(h, range));
    });
    by_draw(&mut out, "fastrange64", 2, |rec, r, i| {
        let h = r.next();
        let mut range = r.next();
        if i % 4 == 0 {
            range >>= 40;
        }
        rec.u64(fast_range64(h, range as usize) as u64);
    });
    by_draw(&mut out, "bijective_hash2x64", 3, |rec, r, i| {
        let (in_hi, in_lo) = (r.next(), r.next());
        let mut seed = r.next();
        if i % 2 == 0 {
            seed = 0;
        }
        let (hi, lo) = bijective_hash2x64_with_seed(in_hi, in_lo, seed);
        let (uhi, ulo) = bijective_unhash2x64_with_seed(in_hi, in_lo, seed);
        for v in [hi, lo, uhi, ulo] {
            rec.u64(v);
        }
    });
    by_draw(&mut out, "context_modifier", 4, |rec, r, i| {
        let mut base = r.next() as u32;
        let mut offset = r.next();
        if i % 8 == 0 {
            base = 0;
        }
        if i % 3 == 0 {
            offset >>= 20;
        }
        rec.u32(checksum_modifier_for_context(base, offset));
    });
    by_draw(&mut out, "crc32c_mask_unmask", 5, |rec, r, _| {
        let v = r.next() as u32;
        rec.u32(crc32c::mask(v));
        rec.u32(crc32c::unmask(v));
    });
    {
        let (mut v64, mut sv64, mut p32, mut p64) =
            (Record::new(), Record::new(), Record::new(), Record::new());
        let mut r = SplitMix64(6);
        for _ in 0..DRAWS {
            let mut v = r.next();
            v >>= r.next() % 64;
            let mut s = Vec::new();
            put_varint64(&mut s, v);
            put_varint32(&mut s, v as u32);
            v64.u8(varint_length(v) as u8);
            v64.bytes(&s);
            v64.end_item();
            s.clear();
            put_varsignedint64(&mut s, v as i64);
            put_varsignedint64(&mut s, -((v >> 1) as i64));
            sv64.bytes(&s);
            sv64.end_item();
            s.clear();
            put_prefix_varint32(&mut s, v as u32);
            p32.bytes(&s);
            p32.end_item();
            s.clear();
            put_prefix_varint64(&mut s, v);
            p64.bytes(&s);
            p64.end_item();
        }
        out.insert("varint_encode".into(), v64.finish());
        out.insert("varsignedint_encode".into(), sv64.finish());
        out.insert("prefix_varint32_encode".into(), p32.finish());
        out.insert("prefix_varint64_encode".into(), p64.finish());
    }
    {
        let (mut d32, mut d64, mut q32, mut q64) =
            (Record::new(), Record::new(), Record::new(), Record::new());
        let mut r = SplitMix64(7);
        let overflow = |e: &Error| {
            matches!(
                e,
                Error::Corruption {
                    why: Malformed::VarintOverflow,
                    ..
                }
            )
        };
        for _ in 0..DRAWS {
            let k = 1 + (r.next() % 11) as usize;
            let mut input = Vec::new();
            input.extend_from_slice(&r.next().to_le_bytes());
            input.extend_from_slice(&r.next().to_le_bytes());
            let input = &input[..k];
            match get_varint32_ptr(input) {
                Ok((v, n)) => {
                    d32.u8(1);
                    d32.u32(v);
                    d32.u8(n as u8);
                }
                Err(e) if overflow(&e) => d32.u8(2),
                Err(_) => d32.u8(0),
            }
            d32.end_item();
            match get_varint64_ptr(input) {
                Ok((v, n)) => {
                    d64.u8(1);
                    d64.u64(v);
                    d64.u8(n as u8);
                }
                Err(e) if overflow(&e) => d64.u8(2),
                Err(_) => d64.u8(0),
            }
            d64.end_item();
            match get_prefix_varint32_ptr(input) {
                Ok((v, n)) => {
                    q32.u8(1);
                    q32.u32(v);
                    q32.u8(n as u8);
                }
                Err(_) => q32.u8(0),
            }
            q32.end_item();
            match get_prefix_varint64_ptr(input) {
                Ok((v, n)) => {
                    q64.u8(1);
                    q64.u64(v);
                    q64.u8(n as u8);
                }
                Err(_) => q64.u8(0),
            }
            q64.end_item();
        }
        out.insert("varint32_decode".into(), d32.finish());
        out.insert("varint64_decode".into(), d64.finish());
        out.insert("prefix_varint32_decode".into(), q32.finish());
        out.insert("prefix_varint64_decode".into(), q64.finish());
    }
    by_draw(&mut out, "downward_involution", 8, |rec, r, _| {
        let v = r.next();
        rec.u64(v.downward_involution());
        rec.u32((v as u32).downward_involution());
    });
    out
}

fn golden() -> Records {
    GOLDEN
        .lines()
        .map(|line| {
            let mut fields = line.split_whitespace();
            let name = fields.next().unwrap().to_string();
            let count = fields.next().unwrap().parse().unwrap();
            let digests = fields
                .map(|d| u64::from_str_radix(d, 16).unwrap())
                .collect();
            (name, (count, digests))
        })
        .collect()
}

#[test]
fn port_matches_rocksdb_golden_vectors() {
    let want = golden();
    let got = compute();
    assert_eq!(
        want.keys().collect::<Vec<_>>(),
        got.keys().collect::<Vec<_>>(),
        "the port computes exactly the records the golden program printed"
    );
    let mut mismatches = Vec::new();
    for (name, (count, digests)) in &want {
        let (got_count, got_digests) = &got[name];
        assert_eq!(count, got_count, "{name}: item count");
        if let Some(w) = digests.iter().zip(got_digests).position(|(a, b)| a != b) {
            mismatches.push(format!(
                "{name}: items {}..{} differ",
                w * WINDOW,
                ((w + 1) * WINDOW).min(*count)
            ));
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}
