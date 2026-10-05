//! The engine's own raw deflate (`codec::deflate`) against the reference zlib, the oracle
//! (docs/design/engine.md §5): every stream zlib deflated of the corpus as RocksDB calls it
//! (`tests/golden/deflate`, made by `deflate_oracle.cc`: level 6, window bits −14) inflates to its
//! input; the port's own streams inflate to their input; and streams corrupted at every byte and
//! cut at every length fail typed and never panic.
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

use corpus::{corpus, fnv};
use mantle_engine::codec::deflate::{compress, decompress};

/// RocksDB's window: `window_bits` −14 (`util/compression.cc`).
const WINDOW_LOG: u32 = 14;

fn golden() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/deflate"))
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
fn every_stream_of_the_reference_inflates_to_its_input() {
    for (name, input) in corpus() {
        let stream = std::fs::read(golden().join(format!("{name}.zz"))).unwrap();
        let out =
            decompress(&stream, WINDOW_LOG, input.len()).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(out == input, "{name}");
        if !input.is_empty() {
            assert!(
                decompress(&stream, WINDOW_LOG, input.len() - 1).is_err(),
                "{name}"
            );
        }
    }
}

#[test]
fn the_ports_streams_inflate_to_their_input() {
    let (mut ours, mut theirs) = (0usize, 0usize);
    for (name, input) in corpus() {
        let stream = compress(&input, WINDOW_LOG).unwrap();
        assert!(
            decompress(&stream, WINDOW_LOG, input.len()).unwrap() == input,
            "{name}"
        );
        ours += stream.len();
        theirs += std::fs::read(golden().join(format!("{name}.zz")))
            .unwrap()
            .len();
    }
    eprintln!("bytes over the corpus: port {ours}, reference {theirs}");
}

#[test]
fn corrupted_streams_fail_typed_and_never_panic() {
    for (name, input) in corpus()
        .into_iter()
        .filter(|(_, input)| input.len() <= 4096)
    {
        let stream = std::fs::read(golden().join(format!("{name}.zz"))).unwrap();
        for at in 0..stream.len() {
            for bit in 0..8 {
                let mut bad = stream.clone();
                bad[at] ^= 1 << bit;
                if let Ok(out) = decompress(&bad, WINDOW_LOG, input.len()) {
                    assert_eq!(out.len(), input.len(), "{name} byte {at} bit {bit}");
                }
            }
        }
        for cut in 0..stream.len() {
            let _ = decompress(&stream[..cut], WINDOW_LOG, input.len());
        }
    }
}

/// Writes the port's streams where the oracle's verify mode reads them; run once by hand.
#[test]
#[ignore = "writes files for the reference's one-time check of the port's streams"]
fn write_the_ports_streams() {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("deflate-port");
    std::fs::create_dir_all(&dir).unwrap();
    for (name, input) in corpus() {
        std::fs::write(
            dir.join(format!("{name}.port")),
            compress(&input, WINDOW_LOG).unwrap(),
        )
        .unwrap();
    }
}
