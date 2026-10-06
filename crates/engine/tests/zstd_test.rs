//! The engine's own Zstandard decoder (`codec::zstd`) against the reference C library, the
//! oracle (docs/design/engine.md §5): frames the reference compresses at every level, with and
//! without checksums and content sizes, with trained and raw dictionaries, decoded by the port fed
//! in pieces of every size; and frames corrupted at every kind of place, which must fail with a
//! typed error or decode, never panic.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use std::io::Write as _;

use mantle_engine::codec::zstd::{Compressor, Decoder, Dictionary, Level, compress};
use mantle_engine::util::xxhash::{Xxh64, xxh64};

/// The window bound the decoder is given: the reference's default decoding limit, 2^27 bytes
/// (`ZSTD_WINDOWLOG_LIMIT_DEFAULT`), above every frame these tests make.
const MAX_WINDOW: usize = 1 << 27;

/// A SplitMix64 stream.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Inputs of every character: text with repeats at many distances, random bytes, one byte
/// repeated, a short alphabet, and the empty input; lengths across the 128 KiB block size.
fn corpus() -> Vec<Vec<u8>> {
    let mut rng = Rng(7);
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
    let mut out = vec![Vec::new()];
    for &len in &[
        1usize, 2, 3, 7, 16, 100, 1000, 4096, 65_536, 131_072, 131_073, 400_000,
    ] {
        let text: Vec<u8> = (0..)
            .flat_map(|_| words[rng.below(words.len() as u64) as usize])
            .copied()
            .take(len)
            .collect();
        out.push(text);
        out.push((0..len).map(|_| rng.next() as u8).collect());
        out.push(vec![0x5A; len]);
        out.push((0..len).map(|_| b"ACGT"[rng.below(4) as usize]).collect());
    }
    out
}

/// Decodes `frame` with `decoder`, feeding input in pieces of `in_step` and draining into an
/// output of `out_step`, until the input is taken and nothing is pending. `Err` carries the
/// decoder's error, or says the input ended inside a frame.
fn decode_with(
    decoder: &mut Decoder,
    frame: &[u8],
    in_step: usize,
    out_step: usize,
) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut buf = vec![0u8; out_step];
    let mut at = 0;
    loop {
        let end = (at + in_step).min(frame.len());
        let p = decoder
            .decompress(&frame[at..end], &mut buf)
            .map_err(|e| e.to_string())?;
        out.extend_from_slice(&buf[..p.produced]);
        at += p.consumed;
        if at == frame.len() && p.produced < out_step {
            // Everything given was taken and the output was not filled: nothing is pending, and
            // the input must have ended at a frame's end.
            return if p.frame_done {
                Ok(out)
            } else {
                Err("input ended inside a frame".to_owned())
            };
        }
        assert!(
            p.consumed > 0 || p.produced > 0,
            "no progress at {at} of {}",
            frame.len()
        );
    }
}

fn decode(
    frame: &[u8],
    dict: Option<&Dictionary>,
    in_step: usize,
    out_step: usize,
) -> Result<Vec<u8>, String> {
    let mut d = Decoder::new(MAX_WINDOW, dict.cloned());
    let streamed = decode_with(&mut d, frame, in_step, out_step);
    // Whatever the stream decodes, the one-shot path decodes the same, with the same decoder.
    if let Ok(content) = &streamed {
        assert_eq!(
            decode_all(&mut d, frame).as_ref(),
            Ok(content),
            "one-shot decode differs"
        );
    }
    streamed
}

/// Decodes `frame`, whole, with [`Decoder::decompress_all`] onto a buffer holding bytes before.
fn decode_all(decoder: &mut Decoder, frame: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = b"before".to_vec();
    decoder
        .decompress_all(frame, &mut out, 1 << 30)
        .map_err(|e| e.to_string())?;
    assert_eq!(&out[..6], b"before");
    Ok(out.split_off(6))
}

fn reference(data: &[u8], level: i32, checksum: bool, size: bool) -> Vec<u8> {
    let mut enc = zstd::stream::Encoder::new(Vec::new(), level).unwrap();
    enc.include_checksum(checksum).unwrap();
    enc.include_contentsize(size).unwrap();
    if size {
        enc.set_pledged_src_size(Some(data.len() as u64)).unwrap();
    }
    enc.write_all(data).unwrap();
    enc.finish().unwrap()
}

#[test]
fn decodes_every_level_of_the_reference() {
    let corpus = corpus();
    for level in [-7, -1, 1, 2, 3, 5, 9, 15, 19] {
        for data in &corpus {
            for (checksum, size) in [(true, true), (false, false), (true, false)] {
                let frame = reference(data, level, checksum, size);
                let got = decode(&frame, None, frame.len().max(1), 1 << 17).unwrap();
                assert_eq!(got.len(), data.len(), "level {level} len {}", data.len());
                assert!(got == *data, "level {level} len {} differs", data.len());
            }
        }
    }
}

/// A sequence whose extra bits pass 31 takes the sequence decoder's second refill, which the
/// three states read after it need when their tables are large. Twelve blocks of random bytes
/// come first; the next block opens with 70,000 of them again (an offset of about 2^20.6, 20
/// extra bits, and a match past 65,539, 16), and 2,000 short sequences follow it in the same
/// block, so the block's tables are its own, not the predefined ones.
#[test]
fn a_sequence_with_more_than_31_extra_bits_decodes() {
    let mut rng = Rng(11);
    let first: Vec<u8> = (0..12 * 131_072).map(|_| rng.next() as u8).collect();
    let mut data = first.clone();
    data.extend_from_slice(&first[..70_000]);
    for k in 0..2_000u64 {
        data.extend((0..1 + rng.below(4)).map(|_| rng.next() as u8));
        let at = rng.below(first.len() as u64 - 64) as usize;
        data.extend_from_slice(&first[at..at + 6 + (k % 13) as usize]);
    }
    for level in [3, 9] {
        let frame = reference(&data, level, true, true);
        let got = decode(&frame, None, frame.len(), 1 << 17).unwrap();
        assert!(got == data, "level {level}");
    }
}

/// A match longer than its offset repeats the bytes it is still writing. Random patterns of
/// periods on each side of the decoder's copy sizes (its 16-byte pieces, and the 64 bytes past
/// which a copy is one `memcpy`) repeated, so the matches overlap at every offset that crosses
/// those boundaries.
#[test]
fn overlapping_matches_of_every_period_decode() {
    let mut rng = Rng(13);
    for period in [1usize, 2, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100] {
        let pattern: Vec<u8> = (0..period).map(|_| rng.next() as u8).collect();
        let data: Vec<u8> = pattern.iter().copied().cycle().take(5_000).collect();
        for level in [1, 3, 19] {
            let frame = reference(&data, level, true, true);
            let got = decode(&frame, None, frame.len(), 1 << 17).unwrap();
            assert!(got == data, "period {period} level {level}");
        }
    }
}

#[test]
fn decodes_in_pieces_of_every_size() {
    let corpus = corpus();
    for data in corpus.iter().filter(|d| d.len() <= 131_073) {
        let frame = reference(data, 3, true, false);
        for (in_step, out_step) in [
            (1, 1),
            (1, 7),
            (3, 4096),
            (13, 1),
            (4096, 32_768),
            (frame.len().max(1), 1),
        ] {
            let got = decode(&frame, None, in_step, out_step).unwrap();
            assert!(
                got == *data,
                "len {} in {in_step} out {out_step}",
                data.len()
            );
        }
    }
}

#[test]
fn decodes_concatenated_and_skippable_frames() {
    let a = b"first frame, first frame, first frame".to_vec();
    let b = vec![9u8; 70_000];
    let mut stream = reference(&a, 3, true, true);
    // A skippable frame between: magic 0x184D2A53, size 5, five bytes.
    stream.extend_from_slice(&0x184D_2A53u32.to_le_bytes());
    stream.extend_from_slice(&5u32.to_le_bytes());
    stream.extend_from_slice(b"skip!");
    stream.extend_from_slice(&reference(&b, 7, false, true));
    let mut want = a.clone();
    want.extend_from_slice(&b);
    for (i, o) in [(1, 1), (5, 300), (stream.len(), 1 << 17)] {
        assert!(decode(&stream, None, i, o).unwrap() == want);
    }
}

#[test]
fn decodes_with_trained_and_raw_dictionaries() {
    let mut rng = Rng(11);
    let samples: Vec<Vec<u8>> = (0..2_000)
        .map(|i| {
            format!(
                "{{\"id\": {i}, \"bucket\": \"logs-{}\", \"region\": \"us-east-{}\", \"size\": {}}}",
                rng.below(20),
                rng.below(3),
                rng.below(1 << 20)
            )
            .into_bytes()
        })
        .collect();
    let trained = zstd::dict::from_samples(&samples, 16 * 1024).unwrap();
    let raw: Vec<u8> = samples[..200].concat();
    for dict_bytes in [&trained, &raw] {
        let ours = Dictionary::new(dict_bytes).unwrap();
        for sample in samples.iter().step_by(97) {
            for level in [1, 3, 9, 19] {
                let mut c = zstd::bulk::Compressor::with_dictionary(level, dict_bytes).unwrap();
                c.include_checksum(true).unwrap();
                let frame = c.compress(sample).unwrap();
                let got = decode(&frame, Some(&ours), 3, 17).unwrap();
                assert!(got == *sample, "level {level}");
            }
        }
    }
}

#[test]
fn a_frame_naming_another_dictionary_is_refused() {
    let samples: Vec<Vec<u8>> = (0..500)
        .map(|i| format!("sample {i} of a dictionary").into_bytes())
        .collect();
    let trained = zstd::dict::from_samples(&samples, 4096).unwrap();
    let mut c = zstd::bulk::Compressor::with_dictionary(3, &trained).unwrap();
    let frame = c.compress(&samples[3]).unwrap();
    // No dictionary at all: refused.
    assert!(decode(&frame, None, frame.len(), 4096).is_err());
}

#[test]
fn a_window_above_the_bound_is_refused_before_it_is_used() {
    let frame = reference(&vec![1u8; 1 << 20], 19, false, false);
    let mut d = Decoder::new(1 << 10, None);
    let err = d.decompress(&frame, &mut [0u8; 64]).unwrap_err();
    assert!(
        matches!(err, mantle_engine::Error::LimitExceeded { .. }),
        "{err}"
    );
}

/// Corrupted frames: every byte flipped in turn in a small frame, truncated at every length, and
/// random overwrites in larger ones; each must decode to something or fail typed, never panic,
/// and a frame with a checksum must not decode to the wrong content.
#[test]
fn corrupt_frames_fail_typed_and_never_panic() {
    let mut rng = Rng(23);
    let small = b"corruption, corruption, corruption! and some more text to compress here".to_vec();
    let frame = reference(&small, 5, true, true);
    for at in 0..frame.len() {
        for bit in 0..8 {
            let mut f = frame.clone();
            f[at] ^= 1 << bit;
            if let Ok(out) = decode(&f, None, f.len(), 4096) {
                // A flip the format cannot see must still have produced the right content, or
                // the checksum would have refused it.
                assert!(
                    out == small,
                    "byte {at} bit {bit} decoded to different content"
                );
            }
            if let Ok(out) = decode_all(&mut Decoder::new(MAX_WINDOW, None), &f) {
                assert!(
                    out == small,
                    "byte {at} bit {bit} decoded whole to different content"
                );
            }
        }
    }
    for cut in 0..frame.len() {
        let mut d = Decoder::new(MAX_WINDOW, None);
        let mut buf = vec![0u8; 4096];
        let _ = d.decompress(&frame[..cut], &mut buf);
        // No input is no frames; any other cut ends inside the frame.
        assert!(
            cut == 0 || decode_all(&mut d, &frame[..cut]).is_err(),
            "a frame cut at {cut} decoded whole"
        );
    }
    for data in corpus()
        .iter()
        .filter(|d| d.len() >= 1000 && d.len() <= 131_073)
    {
        let frame = reference(data, 3, true, false);
        for _ in 0..200 {
            let mut f = frame.clone();
            for _ in 0..1 + rng.below(4) {
                let at = rng.below(f.len() as u64) as usize;
                f[at] = rng.next() as u8;
            }
            if let Ok(out) = decode(
                &f,
                None,
                1 + rng.below(5000) as usize,
                1 + rng.below(70_000) as usize,
            ) {
                assert!(
                    out == *data,
                    "a corrupted frame decoded to different content"
                );
            }
            if let Ok(out) = decode_all(&mut Decoder::new(MAX_WINDOW, None), &f) {
                assert!(
                    out == *data,
                    "a corrupted frame decoded whole to different content"
                );
            }
        }
    }
}

#[test]
fn streamed_xxh64_matches_one_shot() {
    let mut rng = Rng(5);
    let data: Vec<u8> = (0..10_000).map(|_| rng.next() as u8).collect();
    for step in [1usize, 3, 31, 32, 33, 64, 999, 10_000] {
        let mut h = Xxh64::new(0);
        for chunk in data.chunks(step) {
            h.update(chunk);
        }
        assert_eq!(h.digest(), xxh64(&data, 0), "step {step}");
    }
}

/// The port's frames decode to their input through the port's decoder and the reference's, at
/// every level, with and without checksums; their sizes beside the reference's at the same level
/// are printed (the ratio is measured, not asserted: the encoder is judged against the reference
/// by the benchmark, under the tails rule).
#[test]
fn the_ports_frames_decode_everywhere() {
    let corpus = corpus();
    let mut ours_total = 0usize;
    let mut theirs_total = 0usize;
    for level in [1, 3, 9] {
        for data in &corpus {
            for checksum in [true, false] {
                let frame = compress(data, Level::new(level), checksum).unwrap();
                let back = decode(&frame, None, frame.len().max(1), 1 << 17).unwrap();
                assert!(back == *data, "port→port level {level} len {}", data.len());
                let reference = zstd::stream::decode_all(&frame[..]).unwrap();
                assert!(
                    reference == *data,
                    "port→reference level {level} len {}",
                    data.len()
                );
                if checksum {
                    ours_total += frame.len();
                    theirs_total += zstd::bulk::compress(data, level).unwrap().len();
                }
            }
        }
    }
    eprintln!(
        "bytes over the corpus at levels 1, 3, 9: port {ours_total}, reference {theirs_total}"
    );
}

/// One compressor reused frame after frame, as a table builder keeps one: its tables hold the
/// earlier frames' positions, which no later frame may match. Every frame must be the one a new
/// compressor writes, and decode, by the port and by the reference, to its own input.
#[test]
fn a_reused_compressor_writes_every_frame_alone() {
    let corpus = corpus();
    let mut compressor = Compressor::new().unwrap();
    for level in [1, 3, 5, 9] {
        for data in corpus.iter().chain(corpus.iter().rev()) {
            let mut frame = Vec::new();
            compressor
                .compress_into(data, Level::new(level), false, &mut frame)
                .unwrap();
            assert!(
                frame == compress(data, Level::new(level), false).unwrap(),
                "history changed a frame: level {level} len {}",
                data.len()
            );
            let back = decode(&frame, None, frame.len().max(1), 1 << 17).unwrap();
            assert!(back == *data, "port level {level} len {}", data.len());
            let reference = zstd::stream::decode_all(&frame[..]).unwrap();
            assert!(
                reference == *data,
                "reference level {level} len {}",
                data.len()
            );
        }
    }
}
