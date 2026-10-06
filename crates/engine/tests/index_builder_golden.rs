//! The port's index builders against RocksDB 11.8.1's own (`tests/golden/p4_index_gen.cc`): each
//! of the 240 tables is indexed by the port from the same blocks, and every separator, size
//! estimate and partition cut, every index block and partition, the hash index's prefix blocks,
//! and the index's size, uniform count and key form must equal RocksDB's.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use mantle_engine::table::block_based::index_builder::{
    Finished, IndexBuilder, IndexBuilderOptions, IndexShorteningMode, IndexType,
};
use mantle_engine::table::format::BlockHandle;
use mantle_engine::util::comparator::Comparator;
use mantle_engine::util::slice_transform::SliceTransform;

fn unhex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02X}")).collect()
}

struct Block {
    keys: Vec<Vec<u8>>,
    handle: BlockHandle,
    skip: bool,
    separator: String,
    estimate: u64,
    cut: Option<bool>,
}

fn parse_block(token: &str) -> Block {
    let (left, right) = token.get(1..).unwrap().split_once('=').unwrap();
    let l: Vec<&str> = left.split('/').collect();
    let r: Vec<&str> = right.split('/').collect();
    Block {
        keys: l[0].split(',').map(unhex).collect(),
        handle: BlockHandle {
            offset: l[1].parse().unwrap(),
            size: l[2].parse().unwrap(),
        },
        skip: l[3] == "1",
        separator: r[0].to_owned(),
        estimate: r[1].parse().unwrap(),
        cut: r.get(2).map(|c| *c == "1"),
    }
}

#[test]
fn indexes_match_rocksdb_byte_for_byte() {
    let golden = include_str!("golden/p4_index.txt");
    let (mut tables, mut partitions) = (0, 0);
    for line in golden.lines() {
        let f: Vec<&str> = line.split(' ').collect();
        let index_type = match f[0] {
            "0" => IndexType::BinarySearch,
            "1" => IndexType::HashSearch,
            "2" => IndexType::TwoLevelIndexSearch,
            _ => IndexType::BinarySearchWithFirstKey,
        };
        let comparator = if f[1] == "b" {
            Comparator::Bytewise
        } else {
            Comparator::ReverseBytewise
        };
        let format_version: u32 = f[2].parse().unwrap();
        let (kind, len) = f[6].split_once('.').unwrap();
        let len: usize = len.parse().unwrap();
        let prefix = match kind {
            "0" => SliceTransform::Fixed(len),
            "1" => SliceTransform::Capped(len),
            _ => SliceTransform::Noop,
        };
        let options = IndexBuilderOptions {
            comparator,
            index_block_restart_interval: f[4].parse().unwrap(),
            format_version,
            use_value_delta_encoding: format_version >= 4,
            index_shortening: match f[3] {
                "0" => IndexShorteningMode::NoShortening,
                "1" => IndexShorteningMode::ShortenSeparators,
                _ => IndexShorteningMode::ShortenSeparatorsAndSuccessor,
            },
            uniform_cv_threshold: if f[5] == "-1.0" {
                None
            } else {
                Some(f[5].parse().unwrap())
            },
            metadata_block_size: f[7].parse().unwrap(),
            block_size_deviation: f[8].parse().unwrap(),
        };
        let mut b = IndexBuilder::new(index_type, &options, Some(prefix)).unwrap();
        let blocks: Vec<Block> = f[9..]
            .iter()
            .filter(|t| t.starts_with('B'))
            .map(|t| parse_block(t))
            .collect();
        let mut scratch = Vec::new();
        let mut offset = 0;
        for (i, block) in blocks.iter().enumerate() {
            for k in &block.keys {
                b.on_key_added(k).unwrap();
            }
            let next = blocks.get(i + 1).map(|n| n.keys[0].as_slice());
            let last = block.keys.last().unwrap();
            let sep = b
                .add_index_entry(last, next, block.handle, &mut scratch, block.skip)
                .unwrap();
            assert_eq!(
                hex(sep),
                block.separator,
                "table {tables} block {i} separator"
            );
            assert_eq!(
                b.current_index_size_estimate(),
                block.estimate,
                "table {tables} block {i} estimate"
            );
            if let (Some(want), IndexBuilder::Partitioned(p)) = (block.cut, &mut b) {
                assert_eq!(
                    p.should_cut_filter_block(),
                    want,
                    "table {tables} block {i} cut"
                );
            }
            offset = block.handle.offset + block.handle.size + 5;
        }
        let mut last_partition = BlockHandle::new(u64::MAX, u64::MAX);
        let mut metas = Vec::new();
        for token in f[9..].iter().filter(|t| t.starts_with('F')) {
            let (state, want) = token.get(1..).unwrap().split_once(':').unwrap();
            let got = b.finish(last_partition).unwrap();
            let blocks = match (&got, state) {
                (Finished::Partition(blocks), "p") => {
                    partitions += 1;
                    blocks
                }
                (Finished::Done(blocks), "d") => blocks,
                _ => panic!("table {tables}: finish returned {got:?}, RocksDB {state}"),
            };
            assert_eq!(hex(&blocks.index_block), want, "table {tables} index block");
            last_partition = BlockHandle::new(offset, blocks.index_block.len() as u64);
            offset += blocks.index_block.len() as u64 + 5;
            metas = blocks.meta_blocks.clone();
        }
        metas.sort();
        let want_metas: Vec<(String, String)> = f[9..]
            .iter()
            .filter(|t| t.starts_with('M'))
            .map(|t| {
                let (n, h) = t.get(1..).unwrap().split_once(':').unwrap();
                (n.to_owned(), h.to_owned())
            })
            .collect();
        let got_metas: Vec<(String, String)> = metas
            .iter()
            .map(|(n, b)| ((*n).to_owned(), hex(b)))
            .collect();
        assert_eq!(got_metas, want_metas, "table {tables} meta blocks");
        let s: Vec<&str> = f.last().unwrap().get(1..).unwrap().split('/').collect();
        assert_eq!(
            b.index_size().to_string(),
            s[0],
            "table {tables} index size"
        );
        assert_eq!(
            b.num_uniform_index_blocks().to_string(),
            s[1],
            "table {tables} uniform"
        );
        assert_eq!(
            b.separator_is_key_plus_seq(),
            s[2] == "1",
            "table {tables} key form"
        );
        tables += 1;
    }
    assert_eq!(tables, 240);
    eprintln!("240 tables, {partitions} index partitions");
}
