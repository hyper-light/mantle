//! The port's blocks against RocksDB 11.8.1's own `BlockBuilder` (`tests/golden/p4_block_gen.cc`):
//! each of the 256 blocks is built with the port from the same configuration and entries, by
//! `add` or `add_with_last_key` as RocksDB built it, and must equal RocksDB's bytes, its uniform
//! flag, and its two size estimates.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use mantle_engine::table::block_based::block_builder::{BlockBuilder, BlockBuilderOptions};
use mantle_engine::table::block_based::data_block_footer::DataBlockIndexType;

fn unhex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn blocks_match_rocksdb_byte_for_byte() {
    let golden = include_str!("golden/p4_block.txt");
    let (mut blocks, mut uniform, mut hashed) = (0, 0, 0);
    for line in golden.lines() {
        let f: Vec<&str> = line.split(' ').collect();
        let flag = |i: usize| f[i] == "1";
        let options = BlockBuilderOptions {
            restart_interval: f[0].parse().unwrap(),
            use_delta_encoding: flag(1),
            use_value_delta_encoding: flag(2),
            index_type: if flag(3) {
                DataBlockIndexType::BinaryAndHash
            } else {
                DataBlockIndexType::BinarySearch
            },
            data_block_hash_table_util_ratio: f[4].parse().unwrap(),
            use_separated_kv_storage: flag(5),
            uniform_cv_threshold: Some(f[6].parse().unwrap()),
            is_user_key: flag(7),
            capacity: 0,
        };
        let with_last_key = flag(8);
        let count: usize = f[9].parse().unwrap();
        let mut b = BlockBuilder::new(options).unwrap();
        let mut last = Vec::new();
        for entry in &f[10..10 + count] {
            let mut parts = entry.split(':');
            let key = unhex(parts.next().unwrap());
            let value = unhex(parts.next().unwrap());
            let delta = unhex(parts.next().unwrap());
            let delta = if options.use_value_delta_encoding {
                Some(delta.as_slice())
            } else {
                None
            };
            if with_last_key {
                b.add_with_last_key(&key, &value, &last, delta, false)
                    .unwrap();
            } else {
                b.add(&key, &value, delta, false).unwrap();
            }
            last = key;
        }
        let rest = &f[10 + count..];
        let (probe_key, probe_value) = rest[0].split_once(':').unwrap();
        let after: usize = rest[1].parse().unwrap();
        let current: usize = rest[2].parse().unwrap();
        assert_eq!(
            b.estimate_size_after_kv(&unhex(probe_key), &unhex(probe_value)),
            after,
            "estimate after, block {blocks}"
        );
        assert_eq!(b.current_size_estimate(), current, "current estimate");
        let block = b.finish().unwrap().to_vec();
        assert_eq!(
            b.is_uniform(),
            rest[3] == "1",
            "uniform flag of block {blocks}"
        );
        assert_eq!(block, unhex(rest[4]), "bytes of block {blocks}");
        uniform += usize::from(b.is_uniform());
        hashed += usize::from(options.index_type == DataBlockIndexType::BinaryAndHash);
        blocks += 1;
    }
    assert_eq!(blocks, 256);
    eprintln!("{blocks} blocks: {uniform} uniform, {hashed} with a hash index asked for");
}
