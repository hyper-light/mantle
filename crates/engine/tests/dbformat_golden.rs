//! The port's internal keys and user comparators against RocksDB 11.8.1 itself: 8,192 random
//! draws each of both comparators' `FindShortestSeparator`, `FindShortSuccessor` and
//! `IsSameLengthImmediateSuccessor`, `InternalKeyComparator`'s `Compare` and `CompareKeySeq`,
//! the index builder's internal-key separator and successor, `LookupKey` and
//! `ParseInternalKey`, compared with the digests RocksDB's own code printed
//! (`tests/golden/p2_dbformat_gen.cc`, output `tests/golden/p2_dbformat.txt`).
//!
//! The separators and successors become index-block keys (P4), so a wrong answer there writes
//! an index RocksDB reads differently. Inputs come from SplitMix64 over a small alphabet on both
//! sides, so common prefixes, 0x00 and 0xFF runs and ties are frequent.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

#[allow(dead_code)]
mod common;

use std::cmp::Ordering;
use std::collections::BTreeMap;

use common::SplitMix64;
use mantle_engine::db::dbformat::{
    InternalKeyComparator, LookupKey, MAX_SEQUENCE_NUMBER, ParsedInternalKey, ValueType,
    append_internal_key, find_short_internal_key_successor, find_shortest_internal_key_separator,
    parse_internal_key,
};
use mantle_engine::util::comparator::Comparator;

const GOLDEN: &str = include_str!("golden/p2_dbformat.txt");
const DRAWS: usize = 8192;
const WINDOW: usize = 32;
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0100_0000_01b3;

const ALPHABET: [u8; 8] = [0x00, 0x01, b'A', b'B', 0x7f, 0x80, 0xfe, 0xff];
const TYPES: [ValueType; 8] = [
    ValueType::Deletion,
    ValueType::Value,
    ValueType::Merge,
    ValueType::SingleDeletion,
    ValueType::RangeDeletion,
    ValueType::BlobIndex,
    ValueType::WideColumnEntity,
    ValueType::ValuePreferredSeqno,
];

/// Digests of windows of outputs, as `Record` in p2_dbformat_gen.cc.
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
    fn u64(&mut self, v: u64) {
        self.bytes(&v.to_le_bytes());
    }
    fn str(&mut self, s: &[u8]) {
        self.u64(s.len() as u64);
        self.bytes(s);
    }
    fn sign(&mut self, c: Ordering) {
        self.u8(match c {
            Ordering::Less => 0,
            Ordering::Equal => 1,
            Ordering::Greater => 2,
        });
    }
    fn end_item(&mut self) {
        self.count += 1;
        if self.count.is_multiple_of(WINDOW) {
            self.digests.push(self.d);
            self.d = FNV_OFFSET;
        }
    }
    fn finish(mut self) -> (usize, Vec<u64>) {
        if !self.count.is_multiple_of(WINDOW) {
            self.digests.push(self.d);
        }
        (self.count, self.digests)
    }
}

type Records = BTreeMap<String, (usize, Vec<u64>)>;

fn rand_key(r: &mut SplitMix64, max_len: u64) -> Vec<u8> {
    let n = r.next() % (max_len + 1);
    (0..n).map(|_| ALPHABET[(r.next() % 8) as usize]).collect()
}

fn rand_pair(r: &mut SplitMix64) -> (Vec<u8>, Vec<u8>) {
    let a = rand_key(r, 12);
    let p = (r.next() % (a.len() as u64 + 1)) as usize;
    let mut b = a[..p].to_vec();
    b.extend(rand_key(r, 6));
    (a, b)
}

fn ikey(u: &[u8], seq: u64, t: ValueType) -> Vec<u8> {
    let mut s = Vec::new();
    append_internal_key(&mut s, &ParsedInternalKey::new(u, seq, t)).unwrap();
    s
}

fn rand_ikeys(r: &mut SplitMix64) -> (Vec<u8>, Vec<u8>) {
    let (a, mut b) = rand_pair(r);
    if r.next().is_multiple_of(2) {
        b.clone_from(&a);
    }
    let sa = r.next() % 4;
    let sb = r.next() % 4;
    let ta = TYPES[(r.next() % 8) as usize];
    let tb = TYPES[(r.next() % 8) as usize];
    (ikey(&a, sa, ta), ikey(&b, sb, tb))
}

fn record(
    out: &mut Records,
    name: &str,
    seed: u64,
    mut f: impl FnMut(&mut Record, &mut SplitMix64),
) {
    let mut rec = Record::new();
    let mut r = SplitMix64(seed);
    for _ in 0..DRAWS {
        f(&mut rec, &mut r);
        rec.end_item();
    }
    out.insert(name.to_string(), rec.finish());
}

fn compute() -> Records {
    let mut out = Records::new();
    let comparators = [
        ("bytewise", Comparator::Bytewise),
        ("reverse", Comparator::ReverseBytewise),
    ];
    for (i, (name, c)) in comparators.iter().enumerate() {
        let i = i as u64;
        record(&mut out, &format!("{name}_separator"), 1 + i, |rec, r| {
            let (a, b) = rand_pair(r);
            let mut s = a;
            c.find_shortest_separator(&mut s, &b);
            rec.str(&s);
        });
        record(&mut out, &format!("{name}_successor"), 3 + i, |rec, r| {
            let mut s = rand_key(r, 12);
            c.find_short_successor(&mut s);
            rec.str(&s);
        });
        record(
            &mut out,
            &format!("{name}_same_length_successor"),
            5 + i,
            |rec, r| {
                let (a, b) = if r.next().is_multiple_of(2) {
                    let head = rand_key(r, 6);
                    let x = (r.next() % 0xff) as u8;
                    let k = (r.next() % 4) as usize;
                    let mut a = head.clone();
                    a.push(x);
                    a.extend(std::iter::repeat_n(0xffu8, k));
                    let mut b = head;
                    b.push(x + 1);
                    b.extend(std::iter::repeat_n(0u8, k));
                    if k > 0 && r.next().is_multiple_of(3) {
                        let last = b.len() - 1;
                        b[last] = (r.next() % 256) as u8;
                    }
                    (a, b)
                } else {
                    let n = r.next() % 6;
                    let mut a = Vec::new();
                    let mut b = Vec::new();
                    for _ in 0..n {
                        a.push(ALPHABET[(r.next() % 8) as usize]);
                        b.push(ALPHABET[(r.next() % 8) as usize]);
                    }
                    (a, b)
                };
                rec.u8(u8::from(c.is_same_length_immediate_successor(&a, &b)));
            },
        );
        let icmp = InternalKeyComparator::new(*c);
        record(&mut out, &format!("icmp_{name}"), 7 + i, |rec, r| {
            let (a, b) = rand_ikeys(r);
            rec.sign(icmp.compare(&a, &b));
            rec.sign(icmp.compare_key_seq(&a, &b));
        });
        record(
            &mut out,
            &format!("internal_separator_{name}"),
            9 + i,
            |rec, r| {
                let (a, b) = rand_ikeys(r);
                rec.str(&find_shortest_internal_key_separator(*c, &a, &b).unwrap());
                rec.str(&find_short_internal_key_successor(*c, &a).unwrap());
            },
        );
    }
    record(&mut out, "lookup_key", 11, |rec, r| {
        let u = rand_key(r, 300);
        let seq = r.next() & MAX_SEQUENCE_NUMBER;
        let lk = LookupKey::new(&u, seq).unwrap();
        rec.str(lk.memtable_key());
        rec.str(lk.internal_key());
        rec.str(lk.user_key());
    });
    record(&mut out, "parse_internal_key", 12, |rec, r| {
        let n = (r.next() % 17) as usize;
        let mut s: Vec<u8> = (0..n).map(|_| (r.next() & 0xff) as u8).collect();
        if n >= 8 && r.next().is_multiple_of(2) {
            s[n - 8] = (r.next() % 0x20) as u8;
        }
        match parse_internal_key(&s) {
            Ok(p) => {
                rec.u8(1);
                rec.str(p.user_key);
                rec.u64(p.sequence);
                rec.u8(p.value_type.as_u8());
            }
            Err(_) => rec.u8(0),
        }
    });
    out
}

fn parse_golden() -> Records {
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
fn dbformat_matches_rocksdb() {
    let golden = parse_golden();
    let ours = compute();
    assert_eq!(
        golden.keys().collect::<Vec<_>>(),
        ours.keys().collect::<Vec<_>>(),
        "record names"
    );
    let mut failures = Vec::new();
    for (name, (count, digests)) in &golden {
        let (our_count, our_digests) = &ours[name];
        assert_eq!(count, our_count, "{name}: count");
        for (w, (g, o)) in digests.iter().zip(our_digests).enumerate() {
            if g != o {
                failures.push(format!(
                    "{name}: draws {}..{} differ",
                    w * WINDOW,
                    (w + 1) * WINDOW
                ));
                break;
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
