//! The port's block reader against RocksDB 11.8.1's own iterators (`tests/golden/p4_iter_gen.cc`):
//! each of the 192 blocks RocksDB wrote is read by the port with the same options, its per-entry
//! checksums must equal RocksDB's, and each of its 64 moves must land where RocksDB's did, with the
//! same key, value and status.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use mantle_engine::db::dbformat::DISABLE_GLOBAL_SEQUENCE_NUMBER;
use mantle_engine::table::block_based::block::{
    Block, BlockIter, BlockSearchType, IndexIterOptions,
};
use mantle_engine::util::comparator::Comparator;

fn unhex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02X}")).collect()
}

fn result(it: &BlockIter<'_>, index: bool) -> String {
    if it.status().is_err() {
        return "E".into();
    }
    if !it.valid() {
        return "I".into();
    }
    let value = if index {
        let v = it.index_value().unwrap();
        format!(
            "{},{},{}",
            v.handle.offset,
            v.handle.size,
            hex(v.first_internal_key)
        )
    } else {
        hex(it.value())
    };
    format!("V{}/{}", hex(it.key()), value)
}

#[test]
fn block_reads_match_rocksdb_move_for_move() {
    let golden = include_str!("golden/p4_iter.txt");
    let (mut blocks, mut moves) = (0, 0);
    for line in golden.lines() {
        let f: Vec<&str> = line.split(' ').collect();
        let kind = f[0];
        let cmp = if f[1] == "b" {
            Comparator::Bytewise
        } else {
            Comparator::ReverseBytewise
        };
        let interval: u32 = f[2].parse().unwrap();
        let width: u8 = f[3].parse().unwrap();
        let global_seqno = if f[4] == "-" {
            DISABLE_GLOBAL_SEQUENCE_NUMBER
        } else {
            f[4].parse().unwrap()
        };
        let search = match f[5] {
            "i" => BlockSearchType::Interpolation,
            "a" => BlockSearchType::Auto,
            _ => BlockSearchType::Binary,
        };
        let have_first_key = f[6] == "1";
        let value_is_full = f[7] == "1";
        let key_includes_seq = f[8] == "1";
        let mut block = Block::new(unhex(f[9]), interval);
        match kind {
            "d" => block.initialize_data_block_protection_info(width, cmp),
            "i" => block.initialize_index_block_protection_info(
                width,
                cmp,
                value_is_full,
                have_first_key,
            ),
            _ => block.initialize_meta_index_block_protection_info(width),
        }
        .unwrap();
        let sums = if f[10] == "-" {
            Vec::new()
        } else {
            unhex(f[10])
        };
        assert_eq!(
            block.kv_checksum(),
            sums.as_slice(),
            "checksums of block {blocks}"
        );
        let mut it = match kind {
            "d" => block.new_data_iterator(cmp, global_seqno),
            "i" => block.new_index_iterator(
                cmp,
                global_seqno,
                IndexIterOptions {
                    have_first_key,
                    key_includes_seq,
                    value_is_full,
                    search,
                    prefix_index: None,
                },
            ),
            _ => block.new_meta_iterator(),
        };
        for (m, step) in f[11..].iter().enumerate() {
            let (op, want) = step.split_once('=').unwrap();
            let (op, target) = op.split_once(':').unwrap();
            let target = unhex(if target == "-" { "" } else { target });
            let mut prefix = "";
            match op {
                "F" => it.seek_to_first(),
                "L" => it.seek_to_last(),
                "N" => it.next(),
                "P" => it.prev(),
                "S" => it.seek(&target),
                "R" => it.seek_for_prev(&target),
                _ => prefix = if it.seek_for_get(&target) { "T" } else { "F" },
            }
            let got = format!("{prefix}{}", result(&it, kind == "i"));
            assert_eq!(got, want, "block {blocks} ({line:.40}) move {m} {op}");
            moves += 1;
        }
        blocks += 1;
    }
    assert_eq!(blocks, 192);
    assert_eq!(moves, 192 * 64);
}
