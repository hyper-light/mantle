//! The engine's own LZ4 (`codec::lz4`) against the reference library (lz4 1.10), the oracle
//! (docs/design/engine.md §5): every block the reference compressed of the corpus, plainly and
//! after a dictionary (`tests/golden/lz4`, made by `lz4_oracle.cc`), decodes to its input; the
//! port's own blocks decode to their input within the format's bound; and blocks corrupted at
//! every byte and cut at every length fail typed and never panic.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

#[path = "support/corpus.rs"]
mod corpus;

use std::path::Path;

use corpus::{Rng, corpus, fnv};
use mantle_engine::codec::lz4::{compress, compress_bound, decompress};

fn golden() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/lz4"))
}

/// The oracle's dictionary: 16 KiB of the corpus's text from a stream of its own (seed 11).
fn dictionary() -> Vec<u8> {
    let mut rng = Rng(11);
    let words: Vec<&[u8]> = vec![
        b"the ",
        b"quick ",
        b"brown ",
        b"fox ",
        b"jumps ",
        b"over ",
        b"lazy ",
        b"dog ",
        b"mantle ",
        b"engine ",
        b"range ",
        b"replica ",
        b"\n",
        b"{\"key\": ",
        b"}, ",
    ];
    let mut text = Vec::new();
    while text.len() < 16384 {
        text.extend_from_slice(words[rng.below(15) as usize]);
    }
    text.truncate(16384);
    text
}

#[test]
fn the_corpus_is_the_oracles() {
    let manifest = std::fs::read_to_string(golden().join("corpus.txt")).unwrap();
    for (line, (name, input)) in manifest.lines().zip(corpus()) {
        let fields: Vec<&str> = line.split(' ').collect();
        assert_eq!(fields[0], name);
        assert_eq!(
            u64::from_str_radix(fields[2], 16).unwrap(),
            fnv(&input),
            "{name}"
        );
    }
}

#[test]
fn every_block_of_the_reference_decodes_to_its_input() {
    let dict = dictionary();
    for (name, input) in corpus() {
        let plain = std::fs::read(golden().join(format!("{name}.lz4"))).unwrap();
        assert!(
            decompress(&plain, &[], input.len()).unwrap() == input,
            "{name}"
        );
        let after = std::fs::read(golden().join(format!("{name}.lz4d"))).unwrap();
        assert!(
            decompress(&after, &dict, input.len()).unwrap() == input,
            "{name} after the dictionary"
        );
        // The block decodes to exactly its size: a size one short is refused.
        if !input.is_empty() {
            assert!(decompress(&plain, &[], input.len() - 1).is_err(), "{name}");
        }
    }
}

#[test]
fn the_ports_blocks_decode_to_their_input_within_the_bound() {
    let dict = dictionary();
    let (mut ours, mut theirs) = (0usize, 0usize);
    for (name, input) in corpus() {
        for (d, ext) in [(&[][..], "lz4"), (&dict[..], "lz4d")] {
            let block = compress(&input, d).unwrap();
            assert!(
                block.len() <= compress_bound(input.len()).unwrap(),
                "{name}"
            );
            assert!(
                decompress(&block, d, input.len()).unwrap() == input,
                "{name} {ext}"
            );
            ours += block.len();
            theirs += std::fs::read(golden().join(format!("{name}.{ext}")))
                .unwrap()
                .len();
        }
    }
    eprintln!(
        "bytes over the corpus, plain and after the dictionary: port {ours}, reference {theirs}"
    );
}

#[test]
fn corrupted_blocks_fail_typed_and_never_panic() {
    let dict = dictionary();
    for (name, input) in corpus()
        .into_iter()
        .filter(|(_, input)| input.len() <= 4096)
    {
        let block = std::fs::read(golden().join(format!("{name}.lz4d"))).unwrap();
        for at in 0..block.len() {
            for bit in 0..8 {
                let mut bad = block.clone();
                bad[at] ^= 1 << bit;
                if let Ok(out) = decompress(&bad, &dict, input.len()) {
                    assert_eq!(out.len(), input.len(), "{name} byte {at} bit {bit}");
                }
            }
        }
        for cut in 0..block.len() {
            let _ = decompress(&block[..cut], &dict, input.len());
        }
    }
}

/// Writes the port's blocks where the oracle's verify mode reads them; run once by hand
/// (`lz4_oracle verify <dir>`), the result recorded in engine.md.
#[test]
#[ignore = "writes files for the reference's one-time check of the port's blocks"]
fn write_the_ports_blocks() {
    let dict = dictionary();
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("lz4-port");
    std::fs::create_dir_all(&dir).unwrap();
    for (name, input) in corpus() {
        std::fs::write(
            dir.join(format!("{name}.port")),
            compress(&input, &[]).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.join(format!("{name}.portd")),
            compress(&input, &dict).unwrap(),
        )
        .unwrap();
    }
}
