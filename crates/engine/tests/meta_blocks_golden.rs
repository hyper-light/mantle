//! The port's meta blocks against RocksDB 11.8.1's (`tests/golden/p4_meta_gen.cc`): each of the
//! 128 property blocks the port builds from the same properties must equal RocksDB's byte for
//! byte, and the port must parse every one of RocksDB's (malformed numbers included) to the
//! properties RocksDB parsed; each of the 64 meta-index blocks must equal RocksDB's and answer
//! every lookup as RocksDB's did.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::collections::BTreeMap;

use mantle_engine::table::block_based::block::Block;
use mantle_engine::table::format::BlockHandle;
use mantle_engine::table::meta_blocks::{
    MetaIndexBuilder, PropertyBlockBuilder, find_meta_block, parse_properties_block,
};
use mantle_engine::table::table_properties::TableProperties;

fn unhex(s: &str) -> Vec<u8> {
    if s == "-" {
        return Vec::new();
    }
    s.as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn hex(b: &[u8]) -> String {
    if b.is_empty() {
        return "-".into();
    }
    b.iter().map(|x| format!("{x:02X}")).collect()
}

/// The numbers, in the generator's `NUMBERS` order.
fn numbers(p: &mut TableProperties) -> [&mut u64; 36] {
    [
        &mut p.orig_file_number,
        &mut p.data_size,
        &mut p.index_size,
        &mut p.index_partitions,
        &mut p.top_level_index_size,
        &mut p.index_key_is_user_key,
        &mut p.index_value_is_delta_encoded,
        &mut p.udi_is_primary_index,
        &mut p.filter_size,
        &mut p.raw_key_size,
        &mut p.raw_value_size,
        &mut p.num_data_blocks,
        &mut p.num_data_blocks_compression_rejected,
        &mut p.num_data_blocks_compression_bypassed,
        &mut p.num_uniform_blocks,
        &mut p.num_entries,
        &mut p.num_filter_entries,
        &mut p.num_deletions,
        &mut p.num_merge_operands,
        &mut p.num_range_deletions,
        &mut p.format_version,
        &mut p.fixed_key_len,
        &mut p.column_family_id,
        &mut p.creation_time,
        &mut p.oldest_key_time,
        &mut p.newest_key_time,
        &mut p.file_creation_time,
        &mut p.slow_compression_estimated_data_size,
        &mut p.fast_compression_estimated_data_size,
        &mut p.tail_start_offset,
        &mut p.user_defined_timestamps_persisted,
        &mut p.key_largest_seqno,
        &mut p.key_smallest_seqno,
        &mut p.data_block_restart_interval,
        &mut p.index_block_restart_interval,
        &mut p.separate_key_value_in_data_block,
    ]
}

/// The strings, in the generator's `STRINGS` order.
fn strings(p: &mut TableProperties) -> [&mut Vec<u8>; 12] {
    [
        &mut p.db_id,
        &mut p.db_session_id,
        &mut p.db_host_id,
        &mut p.column_family_name,
        &mut p.filter_policy_name,
        &mut p.comparator_name,
        &mut p.merge_operator_name,
        &mut p.prefix_extractor_name,
        &mut p.property_collectors_names,
        &mut p.compression_name,
        &mut p.compression_options,
        &mut p.seqno_to_time_mapping,
    ]
}

fn user(field: &str) -> BTreeMap<Vec<u8>, Vec<u8>> {
    if field == "-" {
        return BTreeMap::new();
    }
    field
        .split(';')
        .filter(|e| !e.is_empty())
        .map(|e| {
            let (k, v) = e.split_once('=').unwrap();
            (unhex(k), unhex(v))
        })
        .collect()
}

fn properties(nums: &str, strs: &str) -> TableProperties {
    let mut p = TableProperties::default();
    for (field, n) in numbers(&mut p).into_iter().zip(nums.split(',')) {
        *field = n.parse().unwrap();
    }
    for (field, s) in strings(&mut p).into_iter().zip(strs.split(',')) {
        *field = unhex(s);
    }
    p
}

fn render(p: &mut TableProperties) -> (String, String) {
    let n: Vec<String> = numbers(p).iter().map(|v| v.to_string()).collect();
    let s: Vec<String> = strings(p).iter().map(|v| hex(v)).collect();
    (n.join(","), s.join(","))
}

#[test]
fn property_blocks_match_rocksdb() {
    let (mut blocks, mut malformed_seen) = (0, 0);
    for line in include_str!("golden/p4_meta.txt")
        .lines()
        .filter(|l| l.starts_with("P "))
    {
        let f: Vec<&str> = line.split(' ').collect();
        let offset: u64 = f[1].parse().unwrap();
        let input = properties(f[2], f[3]);
        let collected = user(f[4]);
        let malformed = unhex(f[5]);
        let theirs = unhex(f[6]);
        if malformed.is_empty() {
            let mut b = PropertyBlockBuilder::new();
            b.add_table_property(&input).unwrap();
            b.add_user_collected(&collected).unwrap();
            assert_eq!(hex(&b.finish().unwrap()), f[6], "property block {blocks}");
        }
        let mut parsed = parse_properties_block(&Block::new(theirs, 1), offset).unwrap();
        let (nums, strs) = render(&mut parsed);
        assert_eq!(nums, f[7], "parsed numbers of block {blocks}");
        assert_eq!(strs, f[8], "parsed strings of block {blocks}");
        assert_eq!(
            parsed.user_collected_properties,
            user(f[9]),
            "user properties {blocks}"
        );
        assert_eq!(
            parsed.external_sst_file_global_seqno_offset.to_string(),
            f[10],
            "global seqno offset {blocks}"
        );
        if malformed.is_empty() {
            assert!(parsed.malformed.is_empty());
        } else {
            assert_eq!(parsed.malformed, vec![malformed]);
            malformed_seen += 1;
        }
        blocks += 1;
    }
    assert_eq!(blocks, 128);
    assert_eq!(malformed_seen, 16);
}

#[test]
fn meta_index_blocks_match_rocksdb() {
    let mut blocks = 0;
    for line in include_str!("golden/p4_meta.txt")
        .lines()
        .filter(|l| l.starts_with("M "))
    {
        let f: Vec<&str> = line.split(' ').collect();
        let mut b = MetaIndexBuilder::new();
        for e in f[1].split(';').filter(|e| !e.is_empty() && *e != "-") {
            let (name, handle) = e.split_once('=').unwrap();
            let (o, s) = handle.split_once(',').unwrap();
            b.add(
                &unhex(name),
                BlockHandle::new(o.parse().unwrap(), s.parse().unwrap()),
            )
            .unwrap();
        }
        assert_eq!(hex(&b.finish().unwrap()), f[2], "meta-index block {blocks}");
        let block = Block::new(unhex(f[2]), 1);
        for e in f[3].split(';').filter(|e| !e.is_empty()) {
            let (name, want) = e.split_once('=').unwrap();
            let got = match find_meta_block(&block, &unhex(name)).unwrap() {
                Some(h) => format!("{},{}", h.offset, h.size),
                None => "-".into(),
            };
            assert_eq!(got, want, "lookup in meta-index block {blocks}");
        }
        blocks += 1;
    }
    assert_eq!(blocks, 64);
}

#[test]
fn a_name_added_twice_is_refused() {
    let mut p = PropertyBlockBuilder::new();
    p.add(b"x", b"1").unwrap();
    assert!(p.add(b"x", b"2").is_err());
    let mut m = MetaIndexBuilder::new();
    m.add(b"x", BlockHandle::new(1, 2)).unwrap();
    assert!(m.add(b"x", BlockHandle::new(3, 4)).is_err());
}
