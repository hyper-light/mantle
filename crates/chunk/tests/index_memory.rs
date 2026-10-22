//! What the chunk index holds in memory as it takes fragments, counted exactly by hyper-measure's
//! counting allocator: the bytes asked for, at their peak, which a table's growth reaches while
//! its old and new buckets are both held (`index::most_bytes`).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use hyper_measure::alloc;
use mantle_chunk::ChunkKey;
use mantle_chunk::frame::PutRecord;
use mantle_chunk::index::{self, Index};
use mantle_chunk::record::FLAG_FINAL;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// Fragments the measurement takes: through twenty doublings of the index's table, each of which
/// repeats the last's proportions.
const FRAGMENTS: u64 = 1 << 20;

/// A whole chunk of one fragment, as the node puts them: the `n`th, each in a place of its own.
fn put(n: u64) -> PutRecord {
    PutRecord {
        key: ChunkKey {
            block: u128::from(n),
            epoch: 0,
            index: 0,
        },
        segment: (n >> 10) as u32,
        incarnation: 1,
        offset: ((n & 1023) << 12) as u32,
        record_len: 4096,
        chunk_offset: 0,
        payload_len: 1,
        payload_crc: 0,
        sequence: n,
        time_ns: n,
        flags: FLAG_FINAL,
    }
}

#[test]
fn the_index_asks_at_most_its_stated_bytes_at_every_count() {
    let mut index = Index::default();
    alloc::begin();
    let mut first = 0;
    let mut tight = false;
    for n in 1..=FRAGMENTS {
        let record = put(n);
        index.insert(record.key, &record, 1).unwrap();
        let peak = u64::try_from(alloc::read().peak).unwrap();
        let most = index::most_bytes(n).unwrap();
        assert!(
            peak <= most,
            "{n} fragments reached {peak} bytes, past {most}"
        );
        if n == 1 {
            first = peak;
        }
        tight |= peak > (index::FRAGMENT_BYTES - 1) * n + index::INDEX_BYTES;
    }
    alloc::end();
    // Neither bound gives a byte to spare: one fragment asks exactly the two together where the
    // table's control bytes come in SSE2's groups of 16, x86_64's, and 8 fewer in the groups of 8
    // elsewhere (NEON's, or a machine word's); and a fragment past a growth asks more than one
    // byte a fragment less allows.
    let groups = if cfg!(target_arch = "x86_64") { 0 } else { 8 };
    assert_eq!(first + groups, index::most_bytes(1).unwrap());
    assert!(tight);
    // And a budget of the bound holds as many fragments as it was stated for.
    assert_eq!(
        index::most_fragments(index::most_bytes(FRAGMENTS).unwrap()),
        FRAGMENTS
    );
}
