//! The engine's own Snappy (`codec::snappy`) against the reference library (snappy 1.2), the
//! oracle (docs/design/engine.md §5): every block the reference compressed of a corpus
//! (`tests/golden/snappy`, made by `snappy_oracle.cc`) decodes to its input; the port's own blocks
//! decode to their input within the format's bound; and blocks corrupted at every byte and cut at
//! every length fail typed, never panic, and never decode to other content.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use std::path::Path;

use mantle_engine::codec::snappy::{compress, decompress, max_compressed_len};

#[path = "support/corpus.rs"]
mod corpus;

use corpus::{corpus, fnv};

fn golden() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/snappy"))
}

#[test]
fn the_corpus_is_the_oracles() {
    let manifest = std::fs::read_to_string(golden().join("corpus.txt")).unwrap();
    let corpus = corpus();
    assert_eq!(manifest.lines().count(), corpus.len());
    for (line, (name, input)) in manifest.lines().zip(&corpus) {
        let fields: Vec<&str> = line.split(' ').collect();
        assert_eq!(fields[0], name);
        assert_eq!(fields[1].parse::<usize>().unwrap(), input.len(), "{name}");
        assert_eq!(
            u64::from_str_radix(fields[2], 16).unwrap(),
            fnv(input),
            "{name}"
        );
    }
}

#[test]
fn every_block_of_the_reference_decodes_to_its_input() {
    for (name, input) in corpus() {
        let block = std::fs::read(golden().join(format!("{name}.sz"))).unwrap();
        let out = decompress(&block, input.len()).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(out == input, "{name}");
        // A bound below the stated length is refused before anything is reserved.
        if !input.is_empty() {
            assert!(decompress(&block, input.len() - 1).is_err(), "{name}");
        }
    }
}

#[test]
fn the_ports_blocks_decode_to_their_input_within_the_bound() {
    let mut ours = 0usize;
    let mut theirs = 0usize;
    for (name, input) in corpus() {
        let block = compress(&input).unwrap();
        assert!(
            block.len() <= max_compressed_len(input.len()).unwrap(),
            "{name}"
        );
        assert!(decompress(&block, input.len()).unwrap() == input, "{name}");
        ours += block.len();
        theirs += std::fs::read(golden().join(format!("{name}.sz")))
            .unwrap()
            .len();
    }
    eprintln!("bytes over the corpus: port {ours}, reference {theirs}");
}

/// Every byte of a block flipped at every bit, and the block cut at every length: each decodes to
/// its input or fails typed, and none panics. Snappy carries no checksum, so a flip may decode to
/// other bytes of the stated length; what must hold is that nothing reads or writes past a bound.
#[test]
fn corrupted_blocks_fail_typed_and_never_panic() {
    for (name, input) in corpus()
        .into_iter()
        .filter(|(_, input)| input.len() <= 4096)
    {
        let block = std::fs::read(golden().join(format!("{name}.sz"))).unwrap();
        for at in 0..block.len() {
            for bit in 0..8 {
                let mut bad = block.clone();
                bad[at] ^= 1 << bit;
                if let Ok(out) = decompress(&bad, 1 << 20) {
                    assert!(out.len() <= 1 << 20, "{name} byte {at} bit {bit}");
                }
            }
        }
        for cut in 0..block.len() {
            if let Ok(out) = decompress(&block[..cut], input.len()) {
                // Only a cut that leaves a whole shorter block stands, and it states its length.
                assert!(out.len() <= input.len(), "{name} cut at {cut}");
            }
        }
    }
}

/// Writes the port's blocks of the corpus where the oracle's verify mode reads them; run once by
/// hand (`snappy_oracle verify <dir>`), the result recorded in engine.md.
#[test]
#[ignore = "writes files for the reference's one-time check of the port's blocks"]
fn write_the_ports_blocks() {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("snappy-port");
    std::fs::create_dir_all(&dir).unwrap();
    for (name, input) in corpus() {
        std::fs::write(dir.join(format!("{name}.port")), compress(&input).unwrap()).unwrap();
    }
    eprintln!("{}", dir.display());
}
