//! What coding a block holds at its peak, counted exactly by hyper-measure's counting allocator,
//! within what `Code::encode_bytes` and `Code::decode_bytes` state for every code mantle stores.
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
use mantle_ec::{CODES, Code};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

#[test]
fn coding_holds_at_most_its_stated_bytes() {
    // The library's tables are statics it builds at its first use, once a process, the
    // encoder's and the decoder's (`TABLE_BYTES`, tests/tables_memory.rs).
    let warm = Code::new(2, 1).unwrap();
    let chunks = warm.encode(&[1u8; 128]).unwrap();
    warm.decode(&[(1, &chunks[1][..]), (2, &chunks[2][..])], 128)
        .unwrap();
    for (data, parity) in CODES {
        let code = Code::new(data, parity).unwrap();
        // A block its data chunks do not divide, so its last data chunk is padded, of chunks no
        // whole number of 64-byte lines.
        let len = data * 100_003 + 7;
        let block: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        alloc::begin();
        let parities = code.parity_of(&block).unwrap();
        let encoded = u64::try_from(alloc::end().peak).unwrap();
        assert_eq!(parities.len(), parity);
        let most = code.encode_bytes(len).unwrap();
        assert!(
            encoded <= most,
            "RS({data},{parity}) coded at {encoded} bytes, past {most}"
        );
        // A data chunk lost: the decoder restores it from the rest.
        let all = code.encode(&block).unwrap();
        let present: Vec<(usize, &[u8])> = all
            .iter()
            .enumerate()
            .skip(1)
            .take(data)
            .map(|(i, chunk)| (i, chunk.as_slice()))
            .collect();
        alloc::begin();
        let decoded = code.decode(&present, len).unwrap();
        let restored = u64::try_from(alloc::end().peak).unwrap();
        assert_eq!(decoded, block);
        let most = code.decode_bytes(len).unwrap();
        assert!(
            restored <= most,
            "RS({data},{parity}) restored at {restored} bytes, past {most}"
        );
    }
}
