//! The port's footers against RocksDB 11.8.1's own (`tests/golden/p4_footer_gen.cc`): each of the
//! 512 footers is built with the port from the same inputs and must equal RocksDB's bytes, and
//! each of RocksDB's three decodes of it (as written, with one bit flipped, at an offset one off)
//! must end the same way in the port: ok, corruption or not supported.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use mantle_engine::table::format::{BlockHandle, ChecksumType, Footer, build_footer};
use mantle_engine::{Error, Malformed};

fn status(decoded: Result<Footer, Error>) -> char {
    match decoded {
        // A ten-byte varint whose last byte carries bits past 64, which RocksDB never writes and
        // its `GetVarint64` reads by dropping them; the port refuses it (docs/research/24 §1.1).
        // A varint past ten bytes both refuse.
        Err(Error::Corruption {
            what: "block handle",
            why: Malformed::VarintOverflow,
        }) => 'V',
        Ok(_) => 'O',
        Err(Error::Corruption { .. }) => 'C',
        Err(Error::Unsupported { .. }) => 'N',
        Err(_) => 'X',
    }
}

fn unhex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn footers_match_rocksdb_byte_for_byte_and_decode_alike() {
    let golden = include_str!("golden/p4_footer.txt");
    let (mut lines, mut divergent) = (0, 0);
    for line in golden.lines() {
        let f: Vec<&str> = line.split(' ').collect();
        let n = |i: usize| f[i].parse::<u64>().unwrap();
        let fv: u32 = f[0].parse().unwrap();
        let t = ChecksumType::from_byte(f[1].parse().unwrap()).unwrap();
        let footer_offset = n(2);
        let meta = BlockHandle::new(n(3), n(4));
        let index = BlockHandle::new(n(5), n(6));
        let bcc: u32 = f[7].parse().unwrap();
        let expected = unhex(f[8]);
        let built = build_footer(fv, footer_offset, t, meta, index, bcc).unwrap();
        assert_eq!(built.as_slice(), expected.as_slice(), "{line}");
        let char_of = |s: &str| s.chars().last().unwrap();
        assert_eq!(
            status(Footer::decode_from(&expected, footer_offset, None)),
            char_of(f[9]),
            "as written: {line}"
        );
        let (bit, flip) = f[10].split_once(':').unwrap();
        let bit: usize = bit.parse().unwrap();
        let mut flipped = expected.clone();
        flipped[bit / 8] ^= 1 << (bit % 8);
        let port = status(Footer::decode_from(&flipped, footer_offset, None));
        let rocksdb = char_of(flip);
        // The only divergence allowed: a varint RocksDB accepts by dropping bits.
        if port == 'V' {
            assert_eq!(rocksdb, 'O', "bit {bit}: {line}");
            divergent += 1;
        } else {
            assert_eq!(port, rocksdb, "bit {bit}: {line}");
        }
        assert_eq!(
            status(Footer::decode_from(&expected, footer_offset - 1, None)),
            char_of(f[11]),
            "offset - 1: {line}"
        );
        lines += 1;
    }
    assert_eq!(lines, 512);
    // The flips that land on a varint's continuation bit in a pre-6 footer's handles.
    eprintln!(
        "{divergent} flips refused for a varint carrying bits past 64, which RocksDB reads by dropping them"
    );
}
